use duckdb::params;
use log::error;
use log::info;
use parquet::basic::ZstdLevel;
use rayon::prelude::*;
use simd_json;
use solana_transaction_status::TransactionStatusMeta;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Mutex;
use {
    crate::{
        error::{LedgerToolError, Result},
        ledger_utils::get_program_ids,
    },
    arrow::{
        array::{ArrayRef, BooleanArray, Int64Array, LargeStringArray, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    chrono::{Local, TimeZone},
    parquet::{
        arrow::arrow_writer::ArrowWriter, basic::Compression, file::properties::WriterProperties,
    },
    serde::ser::{Impossible, SerializeSeq, SerializeStruct, Serializer},
    serde_derive::{Deserialize, Serialize},
    solana_account_decoder::{encode_ui_account, UiAccountData, UiAccountEncoding},
    solana_accounts_db::accounts_index::ScanConfig,
    solana_cli_output::{
        display::writeln_transaction, CliAccount, CliAccountNewConfig, OutputFormat, QuietDisplay,
        VerboseDisplay,
    },
    solana_ledger::{
        blockstore::{Blockstore, BlockstoreError},
        blockstore_meta::{DuplicateSlotProof, ErasureMeta},
        shred::{Shred, ShredType},
    },
    solana_runtime::bank::{Bank, TotalAccountsStats},
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount},
        clock::{Slot, UnixTimestamp},
        hash::Hash,
        native_token::lamports_to_sol,
        pubkey::Pubkey,
        transaction::VersionedTransaction,
    },
    solana_transaction_status::{
        BlockEncodingOptions, ConfirmedBlock, Encodable, EncodedConfirmedBlock,
        EncodedTransactionWithStatusMeta, EntrySummary, Rewards, TransactionDetails,
        UiTransactionEncoding, VersionedConfirmedBlock, VersionedConfirmedBlockWithEntries,
        VersionedTransactionWithStatusMeta,
    },
    std::{
        cell::RefCell,
        collections::HashMap,
        collections::VecDeque,
        fmt::{self, Display, Formatter},
        io::{stdout, Write},
        rc::Rc,
        sync::Arc,
        time::Instant,
    },
};
#[derive(Serialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct SlotInfo {
    pub total: usize,
    pub first: Option<u64>,
    pub last: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_after_last_root: Option<usize>,
}

#[derive(Serialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct SlotBounds<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub all_slots: Option<&'a Vec<u64>>,
    pub slots: SlotInfo,
    pub roots: SlotInfo,
}

impl VerboseDisplay for SlotBounds<'_> {}
impl QuietDisplay for SlotBounds<'_> {}

impl Display for SlotBounds<'_> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        if self.slots.total > 0 {
            let first = self.slots.first.unwrap();
            let last = self.slots.last.unwrap();

            if first != last {
                writeln!(
                    f,
                    "Ledger has data for {:?} slots {:?} to {:?}",
                    self.slots.total, first, last
                )?;

                if let Some(all_slots) = self.all_slots {
                    writeln!(f, "Non-empty slots: {all_slots:?}")?;
                }
            } else {
                writeln!(f, "Ledger has data for slot {first:?}")?;
            }

            if self.roots.total > 0 {
                let first_rooted = self.roots.first.unwrap_or_default();
                let last_rooted = self.roots.last.unwrap_or_default();
                let num_after_last_root = self.roots.num_after_last_root.unwrap_or_default();
                writeln!(
                    f,
                    "  with {:?} rooted slots from {:?} to {:?}",
                    self.roots.total, first_rooted, last_rooted
                )?;

                writeln!(f, "  and {num_after_last_root:?} slots past the last root")?;
            } else {
                writeln!(f, "  with no rooted slots")?;
            }
        } else {
            writeln!(f, "Ledger is empty")?;
        }

        Ok(())
    }
}

#[derive(Serialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct SlotBankHash {
    pub slot: Slot,
    pub hash: String,
}

impl VerboseDisplay for SlotBankHash {}
impl QuietDisplay for SlotBankHash {}

impl Display for SlotBankHash {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        writeln!(f, "Bank hash for slot {}: {}", self.slot, self.hash)
    }
}

fn writeln_entry(f: &mut dyn fmt::Write, i: usize, entry: &CliEntry, prefix: &str) -> fmt::Result {
    writeln!(
        f,
        "{prefix}Entry {} - num_hashes: {}, hash: {}, transactions: {}, starting_transaction_index {}",
        i, entry.num_hashes, entry.hash, entry.num_transactions, entry.starting_transaction_index,
    )
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliEntries {
    pub entries: Vec<CliEntry>,
    #[serde(skip_serializing)]
    pub slot: Slot,
}

impl QuietDisplay for CliEntries {}
impl VerboseDisplay for CliEntries {}

impl fmt::Display for CliEntries {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Slot {}", self.slot)?;
        for (i, entry) in self.entries.iter().enumerate() {
            writeln_entry(f, i, entry, "  ")?;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliEntry {
    num_hashes: u64,
    hash: String,
    num_transactions: u64,
    starting_transaction_index: usize,
}

impl From<EntrySummary> for CliEntry {
    fn from(entry_summary: EntrySummary) -> Self {
        Self {
            num_hashes: entry_summary.num_hashes,
            hash: entry_summary.hash.to_string(),
            num_transactions: entry_summary.num_transactions,
            starting_transaction_index: entry_summary.starting_transaction_index,
        }
    }
}

impl From<&CliPopulatedEntry> for CliEntry {
    fn from(populated_entry: &CliPopulatedEntry) -> Self {
        Self {
            num_hashes: populated_entry.num_hashes,
            hash: populated_entry.hash.clone(),
            num_transactions: populated_entry.num_transactions,
            starting_transaction_index: populated_entry.starting_transaction_index,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliPopulatedEntry {
    num_hashes: u64,
    hash: String,
    num_transactions: u64,
    starting_transaction_index: usize,
    transactions: Vec<EncodedTransactionWithStatusMeta>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliBlockWithEntries {
    #[serde(flatten)]
    pub encoded_confirmed_block: EncodedConfirmedBlockWithEntries,
    #[serde(skip_serializing)]
    pub slot: Slot,
}

impl QuietDisplay for CliBlockWithEntries {}
impl VerboseDisplay for CliBlockWithEntries {}

impl fmt::Display for CliBlockWithEntries {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Slot: {}", self.slot)?;
        writeln!(
            f,
            "Parent Slot: {}",
            self.encoded_confirmed_block.parent_slot
        )?;
        writeln!(f, "Blockhash: {}", self.encoded_confirmed_block.blockhash)?;
        writeln!(
            f,
            "Previous Blockhash: {}",
            self.encoded_confirmed_block.previous_blockhash
        )?;
        if let Some(block_time) = self.encoded_confirmed_block.block_time {
            writeln!(
                f,
                "Block Time: {:?}",
                Local.timestamp_opt(block_time, 0).unwrap()
            )?;
        }
        if let Some(block_height) = self.encoded_confirmed_block.block_height {
            writeln!(f, "Block Height: {block_height:?}")?;
        }
        if !self.encoded_confirmed_block.rewards.is_empty() {
            let mut rewards = self.encoded_confirmed_block.rewards.clone();
            rewards.sort_by(|a, b| a.pubkey.cmp(&b.pubkey));
            let mut total_rewards = 0;
            writeln!(f, "Rewards:")?;
            writeln!(
                f,
                "  {:<44}  {:^15}  {:<15}  {:<20}  {:>14}  {:>10}",
                "Address", "Type", "Amount", "New Balance", "Percent Change", "Commission"
            )?;
            for reward in rewards {
                let sign = if reward.lamports < 0 { "-" } else { "" };

                total_rewards += reward.lamports;
                #[allow(clippy::format_in_format_args)]
                writeln!(
                    f,
                    "  {:<44}  {:^15}  {:>15}  {}  {}",
                    reward.pubkey,
                    if let Some(reward_type) = reward.reward_type {
                        format!("{reward_type}")
                    } else {
                        "-".to_string()
                    },
                    format!(
                        "{}◎{:<14.9}",
                        sign,
                        lamports_to_sol(reward.lamports.unsigned_abs())
                    ),
                    if reward.post_balance == 0 {
                        "          -                 -".to_string()
                    } else {
                        format!(
                            "◎{:<19.9}  {:>13.9}%",
                            lamports_to_sol(reward.post_balance),
                            (reward.lamports.abs() as f64
                                / (reward.post_balance as f64 - reward.lamports as f64))
                                * 100.0
                        )
                    },
                    reward
                        .commission
                        .map(|commission| format!("{commission:>9}%"))
                        .unwrap_or_else(|| "    -".to_string())
                )?;
            }

            let sign = if total_rewards < 0 { "-" } else { "" };
            writeln!(
                f,
                "Total Rewards: {}◎{:<12.9}",
                sign,
                lamports_to_sol(total_rewards.unsigned_abs())
            )?;
        }
        for (index, entry) in self.encoded_confirmed_block.entries.iter().enumerate() {
            writeln_entry(f, index, &entry.into(), "")?;
            for (index, transaction_with_meta) in entry.transactions.iter().enumerate() {
                writeln!(f, "  Transaction {index}:")?;
                writeln_transaction(
                    f,
                    &transaction_with_meta.transaction.decode().unwrap(),
                    transaction_with_meta.meta.as_ref(),
                    "    ",
                    None,
                    None,
                )?;
            }
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliDuplicateSlotProof {
    shred1: CliDuplicateShred,
    shred2: CliDuplicateShred,
    erasure_consistency: Option<bool>,
}

impl QuietDisplay for CliDuplicateSlotProof {}

impl VerboseDisplay for CliDuplicateSlotProof {
    fn write_str(&self, w: &mut dyn std::fmt::Write) -> std::fmt::Result {
        write!(w, "    Shred1 ")?;
        VerboseDisplay::write_str(&self.shred1, w)?;
        write!(w, "    Shred2 ")?;
        VerboseDisplay::write_str(&self.shred2, w)?;
        if let Some(erasure_consistency) = self.erasure_consistency {
            writeln!(w, "    Erasure consistency {}", erasure_consistency)?;
        }
        Ok(())
    }
}

impl fmt::Display for CliDuplicateSlotProof {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "    Shred1 {}", self.shred1)?;
        write!(f, "    Shred2 {}", self.shred2)?;
        if let Some(erasure_consistency) = self.erasure_consistency {
            writeln!(f, "    Erasure consistency {}", erasure_consistency)?;
        }
        Ok(())
    }
}

impl From<DuplicateSlotProof> for CliDuplicateSlotProof {
    fn from(proof: DuplicateSlotProof) -> Self {
        let shred1 = Shred::new_from_serialized_shred(proof.shred1).unwrap();
        let shred2 = Shred::new_from_serialized_shred(proof.shred2).unwrap();
        let erasure_consistency = (shred1.shred_type() == ShredType::Code
            && shred2.shred_type() == ShredType::Code)
            .then(|| ErasureMeta::check_erasure_consistency(&shred1, &shred2));

        Self {
            shred1: CliDuplicateShred::from(shred1),
            shred2: CliDuplicateShred::from(shred2),
            erasure_consistency,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliDuplicateShred {
    fec_set_index: u32,
    index: u32,
    shred_type: ShredType,
    version: u16,
    merkle_root: Option<Hash>,
    chained_merkle_root: Option<Hash>,
    last_in_slot: bool,
    payload: Vec<u8>,
}

impl CliDuplicateShred {
    fn write_common(&self, w: &mut dyn std::fmt::Write) -> std::fmt::Result {
        writeln!(
            w,
            "fec_set_index {}, index {}, shred_type {:?}\n       \
             version {}, merkle_root {:?}, chained_merkle_root {:?}, last_in_slot {}",
            self.fec_set_index,
            self.index,
            self.shred_type,
            self.version,
            self.merkle_root,
            self.chained_merkle_root,
            self.last_in_slot,
        )
    }
}

impl QuietDisplay for CliDuplicateShred {}

impl VerboseDisplay for CliDuplicateShred {
    fn write_str(&self, w: &mut dyn std::fmt::Write) -> std::fmt::Result {
        self.write_common(w)?;
        writeln!(w, "       payload: {:?}", self.payload)
    }
}

impl fmt::Display for CliDuplicateShred {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.write_common(f)
    }
}

impl From<Shred> for CliDuplicateShred {
    fn from(shred: Shred) -> Self {
        Self {
            fec_set_index: shred.fec_set_index(),
            index: shred.index(),
            shred_type: shred.shred_type(),
            version: shred.version(),
            merkle_root: shred.merkle_root().ok(),
            chained_merkle_root: shred.chained_merkle_root().ok(),
            last_in_slot: shred.last_in_slot(),
            payload: shred.payload().clone(),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EncodedConfirmedBlockWithEntries {
    pub previous_blockhash: String,
    pub blockhash: String,
    pub parent_slot: Slot,
    pub entries: Vec<CliPopulatedEntry>,
    pub rewards: Rewards,
    pub block_time: Option<UnixTimestamp>,
    pub block_height: Option<u64>,
}

impl EncodedConfirmedBlockWithEntries {
    pub fn try_from(
        block: EncodedConfirmedBlock,
        entries_iterator: impl IntoIterator<Item = EntrySummary>,
    ) -> Result<Self> {
        let mut entries = vec![];
        for (i, entry) in entries_iterator.into_iter().enumerate() {
            let ending_transaction_index = entry
                .starting_transaction_index
                .saturating_add(entry.num_transactions as usize);
            let transactions = block
                .transactions
                .get(entry.starting_transaction_index..ending_transaction_index)
                .ok_or(LedgerToolError::Generic(format!(
                    "Mismatched entry data and transactions: entry {:?}",
                    i
                )))?;
            entries.push(CliPopulatedEntry {
                num_hashes: entry.num_hashes,
                hash: entry.hash.to_string(),
                num_transactions: entry.num_transactions,
                starting_transaction_index: entry.starting_transaction_index,
                transactions: transactions.to_vec(),
            });
        }
        Ok(Self {
            previous_blockhash: block.previous_blockhash,
            blockhash: block.blockhash,
            parent_slot: block.parent_slot,
            entries,
            rewards: block.rewards,
            block_time: block.block_time,
            block_height: block.block_height,
        })
    }
}

pub(crate) fn encode_confirmed_block(
    confirmed_block: ConfirmedBlock,
) -> Result<EncodedConfirmedBlock> {
    let encoded_block = confirmed_block.encode_with_options(
        UiTransactionEncoding::Base64,
        BlockEncodingOptions {
            transaction_details: TransactionDetails::Full,
            show_rewards: true,
            max_supported_transaction_version: Some(0),
        },
    )?;

    let encoded_block: EncodedConfirmedBlock = encoded_block.into();
    Ok(encoded_block)
}

pub enum BlockContents {
    VersionedConfirmedBlock(VersionedConfirmedBlock),
    BlockWithoutMetadata(BlockWithoutMetadata),
}

// A VersionedConfirmedBlock analogue for use when the transaction metadata
// fields are unavailable. Also supports non-full blocks
pub struct BlockWithoutMetadata {
    pub blockhash: String,
    pub parent_slot: Slot,
    pub transactions: Vec<VersionedTransaction>,
}

impl BlockContents {
    pub fn transactions(&self) -> Box<dyn Iterator<Item = &VersionedTransaction> + '_> {
        match self {
            BlockContents::VersionedConfirmedBlock(block) => Box::new(
                block
                    .transactions
                    .iter()
                    .map(|VersionedTransactionWithStatusMeta { transaction, .. }| transaction),
            ),
            BlockContents::BlockWithoutMetadata(block) => Box::new(block.transactions.iter()),
        }
    }
}

impl TryFrom<BlockContents> for EncodedConfirmedBlock {
    type Error = LedgerToolError;

    fn try_from(block_contents: BlockContents) -> Result<Self> {
        match block_contents {
            BlockContents::VersionedConfirmedBlock(block) => {
                encode_confirmed_block(ConfirmedBlock::from(block))
            }
            BlockContents::BlockWithoutMetadata(block) => Ok(EncodedConfirmedBlock {
                previous_blockhash: Hash::default().to_string(),
                blockhash: block.blockhash,
                parent_slot: block.parent_slot,
                transactions: block
                    .transactions
                    .into_iter()
                    .map(|tx| EncodedTransactionWithStatusMeta {
                        transaction: tx.encode(UiTransactionEncoding::Json),
                        meta: None,
                        version: None,
                    })
                    .collect(),
                rewards: Rewards::default(),
                block_time: None,
                block_height: None,
                num_partitions: None,
            }),
        }
    }
}

pub async fn output_slot_wrapper(
    blockstore: Arc<Blockstore>,
    slot: Slot,
    output_dir: PathBuf,
) -> Result<()> {
    let lowest_slot = blockstore.lowest_slot();
    let end_slot = blockstore.highest_slot().unwrap().unwrap();
    //write_blocks_to_parquet(blockstore, lowest_slot, end_slot, output_dir);
    write_blocks_to_duckdb(blockstore, lowest_slot, end_slot, output_dir)?;
    Ok(())
}

pub fn get_block(
    blockstore: &Blockstore,
    slot: Slot,
    allow_dead_slots: bool,
    output_format: &OutputFormat,
    verbose_level: u64,
    all_program_ids: &mut HashMap<Pubkey, u64>,
) -> Option<BlockContents> {
    let is_root = blockstore.is_root(slot);
    let is_dead = blockstore.is_dead(slot);

    let Some(meta) = blockstore.meta(slot).unwrap() else {
        return None;
    };

    let (block_contents, entries) = match blockstore.get_complete_block_with_entries(
        slot,
        /*require_previous_blockhash:*/ false,
        /*populate_entries:*/ true,
        allow_dead_slots,
    ) {
        Ok(VersionedConfirmedBlockWithEntries { block, entries }) => {
            (BlockContents::VersionedConfirmedBlock(block), entries)
        }
        Err(err) => {
            // Transaction metadata could be missing, try to fetch just the
            // entries and leave the metadata fields empty
            let maybe_entries = blockstore.get_slot_entries(slot, /*shred_start_index:*/ 0);

            let entries = match maybe_entries {
                Ok(e) => e,
                Err(_) => return None,
            };

            let blockhash = entries
                .last()
                .filter(|_| meta.is_full())
                .map(|entry| entry.hash)
                .unwrap_or(Hash::default());
            let parent_slot = meta.parent_slot.unwrap_or(0);

            let mut entry_summaries = Vec::with_capacity(entries.len());
            let mut starting_transaction_index = 0;
            let transactions = entries
                .into_iter()
                .flat_map(|entry| {
                    entry_summaries.push(EntrySummary {
                        num_hashes: entry.num_hashes,
                        hash: entry.hash,
                        num_transactions: entry.transactions.len() as u64,
                        starting_transaction_index,
                    });
                    starting_transaction_index += entry.transactions.len();

                    entry.transactions
                })
                .collect();

            let block = BlockWithoutMetadata {
                blockhash: blockhash.to_string(),
                parent_slot,
                transactions,
            };
            (BlockContents::BlockWithoutMetadata(block), entry_summaries)
        }
    };

    Some(block_contents)
}

struct ProcessingStats {
    start_time: Instant,
    last_batch_time: Instant,
    total_blocks_processed: u64,
    total_slots_processed: u64,
    recent_rates: VecDeque<f64>,
    window_size: usize,
    total_slots: u64,
}

impl ProcessingStats {
    fn new(window_size: usize, start_slot: u64, end_slot: u64) -> Self {
        Self {
            start_time: Instant::now(),
            last_batch_time: Instant::now(),
            total_blocks_processed: 0,
            total_slots_processed: 0,
            recent_rates: VecDeque::with_capacity(window_size),
            window_size,
            total_slots: end_slot - start_slot + 1,
        }
    }

    fn update(&mut self, slots_in_batch: u64, blocks_in_batch: u64) {
        let now = Instant::now();
        let batch_duration = now.duration_since(self.last_batch_time);
        let rate = blocks_in_batch as f64 / batch_duration.as_secs_f64();

        self.total_slots_processed += slots_in_batch;
        self.total_blocks_processed += blocks_in_batch;

        // Calculate progress based on slots processed
        let progress = (self.total_slots_processed as f64 / self.total_slots as f64) * 100.0;

        // Calculate time remaining based on slots left and current rate
        let slots_remaining = self.total_slots - self.total_slots_processed;
        let current_rate =
            self.total_slots_processed as f64 / now.duration_since(self.start_time).as_secs_f64();
        let estimated_remaining_secs = slots_remaining as f64 / current_rate;

        // Convert to hours, minutes, seconds
        let hours = (estimated_remaining_secs / 3600.0) as u64;
        let minutes = ((estimated_remaining_secs % 3600.0) / 60.0) as u64;
        let seconds = (estimated_remaining_secs % 60.0) as u64;

        println!(
            "Batch Stats:\n\
             - Blocks: {}\n\
             - Current Rate: {:.2} blocks/sec\n\
             - Overall Rate: {:.2} slots/sec\n\
             - Total Blocks: {}\n\
             - Total Slots: {}/{}\n\
             - Elapsed Time: {:.2}s\n\
             - Estimated Time Remaining: {}h {}m {}s\n\
             - Progress: {:.2}%",
            blocks_in_batch,
            rate,
            current_rate,
            self.total_blocks_processed,
            self.total_slots_processed,
            self.total_slots,
            now.duration_since(self.start_time).as_secs_f64(),
            hours,
            minutes,
            seconds,
            progress
        );

        self.last_batch_time = now;
    }

    fn final_stats(&self) {
        let total_duration = self.last_batch_time.duration_since(self.start_time);
        let overall_rate = self.total_blocks_processed as f64 / total_duration.as_secs_f64();

        println!(
            "\nFinal Statistics:\n\
             - Total Slots Processed: {}\n\
             - Total Blocks Found: {}\n\
             - Overall Rate: {:.2} blocks/sec\n\
             - Total Time: {:.2}s",
            self.total_slots_processed,
            self.total_blocks_processed,
            overall_rate,
            total_duration.as_secs_f64(),
        );
    }
}

// Helper function to process block contents
fn process_block_contents(
    slot: Slot,
    block_contents: BlockContents,
) -> (Slot, String, i64, u64, u64, String, String) {
    match block_contents {
        BlockContents::VersionedConfirmedBlock(block) => (
            slot,
            block.blockhash.to_string(),
            block.block_time.unwrap_or_default(),
            block.block_height.unwrap_or_default(),
            block.parent_slot,
            serde_json::to_string(&block.transactions).unwrap_or_default(),
            serde_json::to_string(&block.rewards).unwrap(),
        ),
        BlockContents::BlockWithoutMetadata(block) => {
            // Convert to same format as VersionedConfirmedBlock
            let transactions: Vec<VersionedTransactionWithStatusMeta> = block
                .transactions
                .into_iter()
                .map(|tx| VersionedTransactionWithStatusMeta {
                    transaction: tx,
                    meta: TransactionStatusMeta::default(),
                })
                .collect();

            (
                slot,
                block.blockhash.clone(),
                0,
                0,
                block.parent_slot,
                serde_json::to_string(&transactions).unwrap_or_default(),
                "[]".to_string(),
            )
        }
    }
}

pub fn output_slot(
    blockstore: &Blockstore,
    slot: Slot,
    allow_dead_slots: bool,
    output_format: &OutputFormat,
    verbose_level: u64,
    all_program_ids: &mut HashMap<Pubkey, u64>,
) -> Result<()> {
    let is_root = blockstore.is_root(slot);
    let is_dead = blockstore.is_dead(slot);
    if *output_format == OutputFormat::Display && verbose_level <= 1 {
        if is_root && is_dead {
            eprintln!("Slot {slot} is marked as both a root and dead, this shouldn't be possible");
        }
        println!(
            "Slot {slot}{}",
            if is_root {
                " (root)"
            } else if is_dead {
                " (dead)"
            } else {
                ""
            }
        );
    }

    if is_dead && !allow_dead_slots {
        return Err(LedgerToolError::from(BlockstoreError::DeadSlot));
    }

    let Some(meta) = blockstore.meta(slot)? else {
        return Ok(());
    };
    let (block_contents, entries) = match blockstore.get_complete_block_with_entries(
        slot,
        /*require_previous_blockhash:*/ false,
        /*populate_entries:*/ true,
        allow_dead_slots,
    ) {
        Ok(VersionedConfirmedBlockWithEntries { block, entries }) => {
            (BlockContents::VersionedConfirmedBlock(block), entries)
        }
        Err(_) => {
            // Transaction metadata could be missing, try to fetch just the
            // entries and leave the metadata fields empty
            let entries = blockstore.get_slot_entries(slot, /*shred_start_index:*/ 0)?;

            let blockhash = entries
                .last()
                .filter(|_| meta.is_full())
                .map(|entry| entry.hash)
                .unwrap_or(Hash::default());
            let parent_slot = meta.parent_slot.unwrap_or(0);

            let mut entry_summaries = Vec::with_capacity(entries.len());
            let mut starting_transaction_index = 0;
            let transactions = entries
                .into_iter()
                .flat_map(|entry| {
                    entry_summaries.push(EntrySummary {
                        num_hashes: entry.num_hashes,
                        hash: entry.hash,
                        num_transactions: entry.transactions.len() as u64,
                        starting_transaction_index,
                    });
                    starting_transaction_index += entry.transactions.len();

                    entry.transactions
                })
                .collect();

            let block = BlockWithoutMetadata {
                blockhash: blockhash.to_string(),
                parent_slot,
                transactions,
            };
            (BlockContents::BlockWithoutMetadata(block), entry_summaries)
        }
    };

    if verbose_level == 0 {
        if *output_format == OutputFormat::Display {
            // Given that Blockstore::get_complete_block_with_entries() returned Ok(_), we know
            // that we have a full block so meta.consumed is the number of shreds in the block
            println!(
                "  num_shreds: {}, parent_slot: {:?}, next_slots: {:?}, num_entries: {}, \
                 is_full: {}",
                meta.consumed,
                meta.parent_slot,
                meta.next_slots,
                entries.len(),
                meta.is_full(),
            );
        }
    } else if verbose_level == 1 {
        if *output_format == OutputFormat::Display {
            println!("  {meta:?} is_full: {}", meta.is_full());

            let mut num_hashes = 0;
            for entry in entries.iter() {
                num_hashes += entry.num_hashes;
            }
            let blockhash = entries
                .last()
                .filter(|_| meta.is_full())
                .map(|entry| entry.hash)
                .unwrap_or(Hash::default());

            let mut num_transactions = 0;
            let mut program_ids = HashMap::new();

            for transaction in block_contents.transactions() {
                num_transactions += 1;
                for program_id in get_program_ids(transaction) {
                    *program_ids.entry(*program_id).or_insert(0) += 1;
                }
            }
            println!(
                "  Transactions: {num_transactions}, hashes: {num_hashes}, block_hash: {blockhash}",
            );
            for (pubkey, count) in program_ids.iter() {
                *all_program_ids.entry(*pubkey).or_insert(0) += count;
            }
            println!("  Programs:");
            output_sorted_program_ids(program_ids);
        }
    } else {
        let encoded_block = EncodedConfirmedBlock::try_from(block_contents)?;
        let cli_block = CliBlockWithEntries {
            encoded_confirmed_block: EncodedConfirmedBlockWithEntries::try_from(
                encoded_block,
                entries,
            )?,
            slot,
        };

        println!("{}", output_format.formatted_string(&cli_block));
    }

    Ok(())
}

pub fn output_ledger(
    blockstore: Blockstore,
    starting_slot: Slot,
    ending_slot: Slot,
    allow_dead_slots: bool,
    output_format: OutputFormat,
    num_slots: Option<Slot>,
    verbose_level: u64,
    only_rooted: bool,
) -> Result<()> {
    let slot_iterator = blockstore.slot_meta_iterator(starting_slot)?;

    if output_format == OutputFormat::Json {
        stdout().write_all(b"{\"ledger\":[\n")?;
    }

    let num_slots = num_slots.unwrap_or(Slot::MAX);
    let mut num_printed = 0;
    let mut all_program_ids = HashMap::new();
    for (slot, _slot_meta) in slot_iterator {
        if only_rooted && !blockstore.is_root(slot) {
            continue;
        }
        if slot > ending_slot {
            break;
        }

        if let Err(err) = output_slot(
            &blockstore,
            slot,
            allow_dead_slots,
            &output_format,
            verbose_level,
            &mut all_program_ids,
        ) {
            eprintln!("{err}");
        }
        num_printed += 1;
        if num_printed >= num_slots as usize {
            break;
        }
    }

    if output_format == OutputFormat::Json {
        stdout().write_all(b"\n]}\n")?;
    } else {
        println!("Summary of Programs:");
        output_sorted_program_ids(all_program_ids);
    }
    Ok(())
}

pub fn output_sorted_program_ids(program_ids: HashMap<Pubkey, u64>) {
    let mut program_ids_array: Vec<_> = program_ids.into_iter().collect();
    // Sort descending by count of program id
    program_ids_array.sort_by(|a, b| b.1.cmp(&a.1));
    for (program_id, count) in program_ids_array.iter() {
        println!("{:<44}: {}", program_id.to_string(), count);
    }
}

/// A type to facilitate streaming account information to an output destination
///
/// This type scans every account, so streaming is preferred over the simpler
/// approach of accumulating all the accounts into a Vec and printing or
/// serializing the Vec directly.
pub struct AccountsOutputStreamer {
    account_scanner: AccountsScanner,
    total_accounts_stats: Rc<RefCell<TotalAccountsStats>>,
    output_format: OutputFormat,
}

pub enum AccountsOutputMode {
    All,
    Individual(Vec<Pubkey>),
    Program(Pubkey),
}

pub struct AccountsOutputConfig {
    pub mode: AccountsOutputMode,
    pub include_sysvars: bool,
    pub include_account_contents: bool,
    pub include_account_data: bool,
    pub account_data_encoding: UiAccountEncoding,
}

impl AccountsOutputStreamer {
    pub fn new(bank: Arc<Bank>, output_format: OutputFormat, config: AccountsOutputConfig) -> Self {
        let total_accounts_stats = Rc::new(RefCell::new(TotalAccountsStats::default()));
        let account_scanner = AccountsScanner {
            bank,
            total_accounts_stats: total_accounts_stats.clone(),
            config,
        };
        Self {
            account_scanner,
            total_accounts_stats,
            output_format,
        }
    }

    pub fn output(&self) -> std::result::Result<(), String> {
        match self.output_format {
            OutputFormat::Json | OutputFormat::JsonCompact => {
                let mut serializer = serde_json::Serializer::new(stdout());
                let mut struct_serializer = serializer
                    .serialize_struct("accountInfo", 2)
                    .map_err(|err| format!("unable to start serialization: {err}"))?;
                struct_serializer
                    .serialize_field("accounts", &self.account_scanner)
                    .map_err(|err| format!("unable to serialize accounts scanner: {err}"))?;
                struct_serializer
                    .serialize_field("summary", &*self.total_accounts_stats.borrow())
                    .map_err(|err| format!("unable to serialize accounts summary: {err}"))?;
                SerializeStruct::end(struct_serializer)
                    .map_err(|err| format!("unable to end serialization: {err}"))?;
                // The serializer doesn't give us a trailing newline so do it ourselves
                println!();
                Ok(())
            }
            _ => {
                // The compiler needs a placeholder type to satisfy the generic
                // SerializeSeq trait on AccountScanner::output(). The type
                // doesn't really matter since we're passing None, so just use
                // serde::ser::Impossible as it already implements SerializeSeq
                self.account_scanner
                    .output::<Impossible<(), serde_json::Error>>(&mut None);
                println!("\n{:#?}", self.total_accounts_stats.borrow());
                Ok(())
            }
        }
    }
}

struct AccountsScanner {
    bank: Arc<Bank>,
    total_accounts_stats: Rc<RefCell<TotalAccountsStats>>,
    config: AccountsOutputConfig,
}

impl AccountsScanner {
    /// Returns true if this account should be included in the output
    fn should_process_account(&self, account: &AccountSharedData) -> bool {
        solana_accounts_db::accounts::Accounts::is_loadable(account.lamports())
            && (self.config.include_sysvars || !solana_sdk::sysvar::check_id(account.owner()))
    }

    fn maybe_output_account<S>(
        &self,
        seq_serializer: &mut Option<S>,
        pubkey: &Pubkey,
        account: &AccountSharedData,
        slot: Option<Slot>,
        cli_account_new_config: &CliAccountNewConfig,
    ) where
        S: SerializeSeq,
    {
        if self.config.include_account_contents {
            if let Some(serializer) = seq_serializer {
                let cli_account =
                    CliAccount::new_with_config(pubkey, account, cli_account_new_config);
                serializer.serialize_element(&cli_account).unwrap();
            } else {
                output_account(
                    pubkey,
                    account,
                    slot,
                    self.config.include_account_data,
                    self.config.account_data_encoding,
                );
            }
        }
    }

    pub fn output<S>(&self, seq_serializer: &mut Option<S>)
    where
        S: SerializeSeq,
    {
        let mut total_accounts_stats = self.total_accounts_stats.borrow_mut();
        let rent_collector = self.bank.rent_collector();

        let cli_account_new_config = CliAccountNewConfig {
            data_encoding: self.config.account_data_encoding,
            ..CliAccountNewConfig::default()
        };

        let scan_func = |account_tuple: Option<(&Pubkey, AccountSharedData, Slot)>| {
            if let Some((pubkey, account, slot)) =
                account_tuple.filter(|(_, account, _)| self.should_process_account(account))
            {
                total_accounts_stats.accumulate_account(pubkey, &account, rent_collector);
                self.maybe_output_account(
                    seq_serializer,
                    pubkey,
                    &account,
                    Some(slot),
                    &cli_account_new_config,
                );
            }
        };

        match &self.config.mode {
            AccountsOutputMode::All => {
                self.bank.scan_all_accounts(scan_func, true).unwrap();
            }
            AccountsOutputMode::Individual(pubkeys) => pubkeys.iter().for_each(|pubkey| {
                if let Some((account, slot)) = self
                    .bank
                    .get_account_modified_slot_with_fixed_root(pubkey)
                    .filter(|(account, _)| self.should_process_account(account))
                {
                    total_accounts_stats.accumulate_account(pubkey, &account, rent_collector);
                    self.maybe_output_account(
                        seq_serializer,
                        pubkey,
                        &account,
                        Some(slot),
                        &cli_account_new_config,
                    );
                }
            }),
            AccountsOutputMode::Program(program_pubkey) => self
                .bank
                .get_program_accounts(program_pubkey, &ScanConfig::new(false))
                .unwrap()
                .iter()
                .filter(|(_, account)| self.should_process_account(account))
                .for_each(|(pubkey, account)| {
                    total_accounts_stats.accumulate_account(pubkey, account, rent_collector);
                    self.maybe_output_account(
                        seq_serializer,
                        pubkey,
                        account,
                        None,
                        &cli_account_new_config,
                    );
                }),
        }
    }
}

impl serde::Serialize for AccountsScanner {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq_serializer = Some(serializer.serialize_seq(None)?);
        self.output(&mut seq_serializer);
        seq_serializer.unwrap().end()
    }
}

pub fn output_account(
    pubkey: &Pubkey,
    account: &AccountSharedData,
    modified_slot: Option<Slot>,
    print_account_data: bool,
    encoding: UiAccountEncoding,
) {
    println!("{pubkey}:");
    println!("  balance: {} SOL", lamports_to_sol(account.lamports()));
    println!("  owner: '{}'", account.owner());
    println!("  executable: {}", account.executable());
    if let Some(slot) = modified_slot {
        println!("  slot: {slot}");
    }
    println!("  rent_epoch: {}", account.rent_epoch());
    println!("  data_len: {}", account.data().len());
    if print_account_data {
        let account_data = encode_ui_account(pubkey, account, encoding, None, None).data;
        match account_data {
            UiAccountData::Binary(data, data_encoding) => {
                println!("  data: '{data}'");
                println!(
                    "  encoding: {}",
                    serde_json::to_string(&data_encoding).unwrap()
                );
            }
            UiAccountData::Json(account_data) => {
                println!(
                    "  data: '{}'",
                    serde_json::to_string(&account_data).unwrap()
                );
                println!("  encoding: \"jsonParsed\"");
            }
            UiAccountData::LegacyBinary(_) => {}
        };
    }
}

pub fn write_blocks_to_parquet(
    blockstore: Arc<Blockstore>,
    start_slot: Slot,
    end_slot: Slot,
    output_dir: PathBuf,
) -> Result<()> {
    info!("Starting export for slots {} to {}", start_slot, end_slot);
    let output_file = output_dir.join(format!("blocks_{}_to_{}.parquet", start_slot, end_slot));
    let (tx, rx) = crossbeam_channel::bounded(64 * 4); // 256 slots, 4 batches per reader

    // Spawn reader threads
    let num_readers = 64;
    let slots_per_reader = (end_slot - start_slot + 1) / num_readers as u64;

    let reader_handles: Vec<_> = (0..num_readers)
        .map(|i| {
            let tx = tx.clone();
            let blockstore = blockstore.clone();
            let reader_start = start_slot + (i as u64 * slots_per_reader);
            let reader_end = if i == num_readers - 1 {
                end_slot
            } else {
                reader_start + slots_per_reader - 1
            };

            std::thread::spawn(move || -> Result<()> {
                for slot in reader_start..=reader_end {
                    let mut buffer = Vec::new();
                    match process_slot(&blockstore, slot, &mut buffer) {
                        Ok(_) => {
                            if !buffer.is_empty() {
                                if let Err(e) = tx.send(buffer) {
                                    error!("Thread {} failed to send slot {}: {}", i, slot, e);
                                    return Err(LedgerToolError::Generic(format!(
                                        "Channel send error: {}",
                                        e
                                    )));
                                }
                            }
                        }
                        Err(e) => {
                            error!("Thread {} error processing slot {}: {}", i, slot, e);
                            continue;
                        }
                    }
                }
                Ok(())
            })
        })
        .collect();

    // Writer thread handles serialization
    let writer_handle = std::thread::spawn(move || -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("slot", DataType::UInt64, false),
            Field::new("block_status", DataType::Utf8, false),
            Field::new("blockhash", DataType::Utf8, false),
            Field::new("parent_slot", DataType::UInt64, false),
            Field::new("transactions", DataType::LargeUtf8, false),
            Field::new("rewards", DataType::LargeUtf8, false),
            Field::new("num_partitions", DataType::Utf8, true),
            Field::new("block_time", DataType::Utf8, true),
            Field::new("block_height", DataType::Utf8, true),
            Field::new("shred_count", DataType::Utf8, false),
            Field::new("is_full", DataType::Boolean, false),
            Field::new("parent_exists", DataType::Boolean, false),
            Field::new("previous_blockhash", DataType::Utf8, false),
        ]));

        let file = File::create(&output_file).map_err(|e| {
            error!("Failed to create output file: {}", e);
            LedgerToolError::Generic(format!("File creation error: {}", e))
        })?;

        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(1).unwrap())) // ZSTD level 3
            .set_write_batch_size(1024 * 1024) // 1MB batch size
            .build();

        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).map_err(|e| {
            error!("Failed to create Arrow writer: {}", e);
            LedgerToolError::Generic(format!("Arrow writer creation error: {}", e))
        })?;

        let mut batch_size = 100; // Increased batch size
        let mut slots = Vec::with_capacity(batch_size);
        let mut statuses = Vec::with_capacity(batch_size);
        let mut blockhashes = Vec::with_capacity(batch_size);
        let mut parent_slots = Vec::with_capacity(batch_size);
        let mut transactions = Vec::with_capacity(batch_size);
        let mut rewards = Vec::with_capacity(batch_size);
        let mut num_partitions = Vec::with_capacity(batch_size);
        let mut block_times = Vec::with_capacity(batch_size);
        let mut block_heights = Vec::with_capacity(batch_size);
        let mut shred_counts = Vec::with_capacity(batch_size);
        let mut is_fulls = Vec::with_capacity(batch_size);
        let mut parent_exists = Vec::with_capacity(batch_size);
        let mut previous_blockhashes = Vec::with_capacity(batch_size);

        let mut total_records = 0;
        let start_time = Instant::now();
        let mut last_progress = Instant::now();

        let write_batch = |writer: &mut ArrowWriter<File>,
                           slots: Vec<u64>,
                           statuses: Vec<String>,
                           blockhashes: Vec<String>,
                           parent_slots: Vec<u64>,
                           transactions: Vec<String>,
                           rewards: Vec<String>,
                           num_partitions: Vec<String>,
                           block_times: Vec<String>,
                           block_heights: Vec<String>,
                           shred_counts: Vec<String>,
                           is_fulls: Vec<bool>,
                           parent_exists: Vec<bool>,
                           previous_blockhashes: Vec<String>|
         -> Result<()> {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt64Array::from(slots)),
                    Arc::new(StringArray::from(statuses)),
                    Arc::new(StringArray::from(blockhashes)),
                    Arc::new(UInt64Array::from(parent_slots)),
                    Arc::new(LargeStringArray::from(transactions)),
                    Arc::new(LargeStringArray::from(rewards)),
                    Arc::new(StringArray::from(num_partitions)),
                    Arc::new(StringArray::from(block_times)),
                    Arc::new(StringArray::from(block_heights)),
                    Arc::new(StringArray::from(shred_counts)),
                    Arc::new(BooleanArray::from(is_fulls)),
                    Arc::new(BooleanArray::from(parent_exists)),
                    Arc::new(StringArray::from(previous_blockhashes)),
                ],
            )
            .map_err(|e| {
                error!("Failed to create record batch: {}", e);
                LedgerToolError::Generic(format!("Record batch creation error: {}", e))
            })?;

            writer.write(&batch).map_err(|e| {
                error!("Failed to write batch: {}", e);
                LedgerToolError::Generic(format!("Batch write error: {}", e))
            })?;

            // Flush every batch
            writer.flush().map_err(|e| {
                error!("Failed to flush writer: {}", e);
                LedgerToolError::Generic(format!("Flush error: {}", e))
            })?;

            Ok(())
        };

        loop {
            match rx.recv() {
                Ok(records) => {
                    for record in records {
                        slots.push(record[0].parse::<u64>().unwrap_or(0));
                        statuses.push(record[1].to_string());
                        blockhashes.push(record[2].to_string());
                        parent_slots.push(record[3].parse::<u64>().unwrap_or(0));
                        transactions.push(record[4].to_string());
                        rewards.push(record[5].to_string());
                        num_partitions.push(record[6].to_string());
                        block_times.push(record[7].to_string());
                        block_heights.push(record[8].to_string());
                        shred_counts.push(record[9].to_string());
                        is_fulls.push(record[10].parse::<bool>().unwrap_or(false));
                        parent_exists.push(record[11].parse::<bool>().unwrap_or(false));
                        previous_blockhashes.push(record[12].to_string());

                        total_records += 1;

                        // Print progress every 5 seconds
                        if last_progress.elapsed().as_secs() >= 5 {
                            let elapsed = start_time.elapsed();
                            let rate = total_records as f64 / elapsed.as_secs_f64();
                            info!(
                                "Processed {} records ({:.2} records/sec)",
                                total_records, rate
                            );
                            last_progress = Instant::now();
                        }

                        if slots.len() >= batch_size {
                            write_batch(
                                &mut writer,
                                std::mem::take(&mut slots),
                                std::mem::take(&mut statuses),
                                std::mem::take(&mut blockhashes),
                                std::mem::take(&mut parent_slots),
                                std::mem::take(&mut transactions),
                                std::mem::take(&mut rewards),
                                std::mem::take(&mut num_partitions),
                                std::mem::take(&mut block_times),
                                std::mem::take(&mut block_heights),
                                std::mem::take(&mut shred_counts),
                                std::mem::take(&mut is_fulls),
                                std::mem::take(&mut parent_exists),
                                std::mem::take(&mut previous_blockhashes),
                            )?;
                        }
                    }
                }
                Err(_) => {
                    // Write any remaining records
                    if !slots.is_empty() {
                        write_batch(
                            &mut writer,
                            slots,
                            statuses,
                            blockhashes,
                            parent_slots,
                            transactions,
                            rewards,
                            num_partitions,
                            block_times,
                            block_heights,
                            shred_counts,
                            is_fulls,
                            parent_exists,
                            previous_blockhashes,
                        )?;
                    }

                    // Close the writer
                    writer.close().map_err(|e| {
                        error!("Failed to close writer: {}", e);
                        LedgerToolError::Generic(format!("Writer close error: {}", e))
                    })?;

                    let elapsed = start_time.elapsed();
                    let rate = total_records as f64 / elapsed.as_secs_f64();
                    info!(
                        "Completed processing {} records in {:.2} seconds ({:.2} records/sec)",
                        total_records,
                        elapsed.as_secs_f64(),
                        rate
                    );
                    break;
                }
            }
        }
        Ok(())
    });

    // Then wait for readers
    for handle in reader_handles {
        handle.join().unwrap()?;
    }

    // Wait for writer
    writer_handle.join().unwrap()?;

    Ok(())
}

pub fn write_blocks_to_duckdb(
    blockstore: Arc<Blockstore>,
    start_slot: Slot,
    end_slot: Slot,
    output_dir: PathBuf,
) -> Result<()> {
    info!(
        "Starting optimized DuckDB export for slots {} to {}",
        start_slot, end_slot
    );
    let output_file = output_dir.join(format!("blocks_{}_to_{}.db", start_slot, end_slot));

    let config = duckdb::Config::default();
    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| LedgerToolError::Generic(format!("DuckDB connection error: {}", e)))?;

    // Performance settings
    conn.execute_batch(
        "
        PRAGMA memory_limit='64GB';
        PRAGMA temp_directory='temp';
        PRAGMA synchronous='OFF';
        PRAGMA force_compression='UNCOMPRESSED';
        PRAGMA enable_progress_bar=false;
        PRAGMA enable_object_cache=false;
        
        DROP TABLE IF EXISTS blocks;
        
        CREATE TABLE blocks (
            slot BIGINT,
            block_status VARCHAR,
            blockhash VARCHAR,
            parent_slot BIGINT,
            transactions TEXT,
            rewards TEXT,
            num_partitions VARCHAR,
            block_time VARCHAR,
            block_height VARCHAR,
            shred_count VARCHAR,
            is_full BOOLEAN,
            parent_exists BOOLEAN,
            previous_blockhash VARCHAR
        ) WITH (storage_mode='UNCOMPRESSED');",
    )
    .map_err(|e| LedgerToolError::Generic(format!("Failed to initialize database: {}", e)))?;

    let (tx, rx) = crossbeam_channel::bounded(64 * 2);

    // Spawn reader threads
    let num_readers = 64;
    let slots_per_reader = (end_slot - start_slot + 1) / num_readers as u64;

    let reader_handles: Vec<_> = (0..num_readers)
        .map(|i| {
            let tx = tx.clone();
            let blockstore = blockstore.clone();
            let reader_start = start_slot + (i as u64 * slots_per_reader);
            let reader_end = if i == num_readers - 1 {
                end_slot
            } else {
                reader_start + slots_per_reader - 1
            };

            std::thread::spawn(move || -> Result<()> {
                for slot in reader_start..=reader_end {
                    // Just get the block and send it through
                    if let Some(block_contents) = get_block(
                        &blockstore,
                        slot,
                        true,
                        &OutputFormat::DisplayVerbose,
                        1000,
                        &mut HashMap::new(),
                    ) {
                        if let Err(e) = tx.send((slot, block_contents)) {
                            error!("Thread {} failed to send slot {}: {}", i, slot, e);
                            return Err(LedgerToolError::Generic(format!(
                                "Channel send error: {}",
                                e
                            )));
                        }
                    }
                }
                Ok(())
            })
        })
        .collect();

    let mut total_records = 0;
    let start_time = Instant::now();
    let mut last_progress = Instant::now();
    let mut batch = Vec::with_capacity(100);

    // Create appender
    let mut appender = conn
        .appender("blocks")
        .map_err(|e| LedgerToolError::Generic(format!("Failed to create appender: {}", e)))?;

    while let Ok((slot, block_contents)) = rx.recv() {
        match block_contents {
            BlockContents::VersionedConfirmedBlock(block) => {
                process_confirmed_block(slot, block, &mut appender)?;
            }
            BlockContents::BlockWithoutMetadata(block) => {
                process_unconfirmed_block(slot, block, &mut appender)?;
            }
        };
    }

    // Flush and finish the appender
    appender
        .flush()
        .map_err(|e| LedgerToolError::Generic(format!("Failed to flush appender: {}", e)))?;

    let total_time = start_time.elapsed();
    info!(
        "Completed processing {} records in {:.2} seconds ({:.2} records/sec)",
        total_records,
        total_time.as_secs_f64(),
        total_records as f64 / total_time.as_secs_f64()
    );

    Ok(())
}

fn process_slot(
    blockstore: &Blockstore,
    slot: Slot,
    local_buffer: &mut Vec<Vec<String>>,
) -> Result<()> {
    match blockstore.meta(slot) {
        Ok(Some(meta)) => {
            if let Some(block_contents) = get_block(
                blockstore,
                slot,
                true,
                &OutputFormat::DisplayVerbose,
                1000,
                &mut HashMap::new(),
            ) {
                match block_contents {
                    BlockContents::VersionedConfirmedBlock(block) => {
                        process_confirmed_block(slot, block, meta, local_buffer)
                    }
                    BlockContents::BlockWithoutMetadata(block) => {
                        process_unconfirmed_block(slot, block, meta, local_buffer)
                    }
                }
            } else {
                process_partial_block(slot, meta, local_buffer);
                Ok(())
            }
        }
        Ok(None) => Ok(()),
        Err(e) => {
            error!("Error fetching metadata for slot {}: {}", slot, e);
            Err(LedgerToolError::Blockstore(e))
        }
    }
}

fn process_confirmed_block(
    slot: Slot,
    block: VersionedConfirmedBlock,
    meta: solana_ledger::blockstore_meta::SlotMeta,
    local_buffer: &mut Vec<Vec<String>>,
) -> Result<()> {
    local_buffer.push(vec![
        slot.to_string(),
        "confirmed".to_string(),
        block.blockhash.to_string(),
        block.parent_slot.to_string(),
        simd_json::to_string(&block.transactions).map_err(|e| {
            error!("Failed to serialize transactions for slot {}: {}", slot, e);
            LedgerToolError::SerdeJson(e)
        })?,
        simd_json::to_string(&block.rewards).map_err(|e| {
            error!("Failed to serialize rewards for slot {}: {}", slot, e);
            LedgerToolError::SerdeJson(e)
        })?,
        block
            .num_partitions
            .map_or("".to_string(), |n| n.to_string()),
        block.block_time.map_or("".to_string(), |t| t.to_string()),
        block.block_height.map_or("".to_string(), |h| h.to_string()),
        meta.consumed.to_string(),
        meta.is_full().to_string(),
        meta.parent_slot
            .map_or("false".to_string(), |_| "true".to_string()),
        block.previous_blockhash.to_string(),
    ]);
    Ok(())
}

fn process_unconfirmed_block(
    slot: Slot,
    block: BlockWithoutMetadata,
    meta: solana_ledger::blockstore_meta::SlotMeta,
    local_buffer: &mut Vec<Vec<String>>,
) -> Result<()> {
    local_buffer.push(vec![
        slot.to_string(),
        "unconfirmed".to_string(),
        block.blockhash.clone(),
        block.parent_slot.to_string(),
        simd_json::to_string(&block.transactions).map_err(|e| {
            error!("Failed to serialize transactions for slot {}: {}", slot, e);
            LedgerToolError::SerdeJson(e)
        })?,
        "[]".to_string(),
        "".to_string(),
        "".to_string(),
        "".to_string(),
        meta.consumed.to_string(),
        meta.is_full().to_string(),
        meta.parent_slot
            .map_or("false".to_string(), |_| "true".to_string()),
        "".to_string(),
    ]);
    Ok(())
}

fn process_partial_block(
    slot: Slot,
    meta: solana_ledger::blockstore_meta::SlotMeta,
    local_buffer: &mut Vec<Vec<String>>,
) {
    local_buffer.push(vec![
        slot.to_string(),
        "partial".to_string(),
        "".to_string(),
        meta.parent_slot.map_or("".to_string(), |p| p.to_string()),
        "[]".to_string(),
        "[]".to_string(),
        "".to_string(),
        "".to_string(),
        "".to_string(),
        "".to_string(),
        meta.consumed.to_string(),
        meta.is_full().to_string(),
        meta.parent_slot
            .map_or("false".to_string(), |_| "true".to_string()),
    ]);
}

// async fn write_transaction(
//     &mut self,
//     context: &WriteContext,
//     transaction_infos: &Vec<TransactionInfo>,
// ) -> anyhow::Result<()> {
//     let timestamp = context.timestamp.duration_since(std::time::UNIX_EPOCH)?;

//     let p = self.db_pool.get()?;
//     let mut tran_appender = p
//         .appender_to_db("transaction_info", "hstore")
//         .with_context(|| "Cannot construct appender for transaction_info / hstore")?;
//     let mut acct_appender = p
//         .appender_to_db("account_to_transaction", "hstore")
//         .with_context(|| "Cannot construct appender for account_to_transaction / hstore")?;

//     for transaction_info in transaction_infos {
//         tran_appender.append_row(params![
//             &transaction_info.block_slot,
//             &transaction_info.signature,
//             &transaction_info.is_vote,
//             &transaction_info.index,
//             &(transaction_info.message_type as i16),
//             &transaction_info.message.to_string(),
//             &timestamp,
//         ])?;

//         for account_key in &transaction_info.account_keys {
//             acct_appender.append_row(params![
//                 &account_key,
//                 &transaction_info.signature,
//                 &transaction_info.block_slot,
//                 &timestamp
//             ])?
//         }
//     }

//     acct_appender.flush()?;
//     tran_appender.flush()?;
//     Ok(())
// }

// async fn write_block(&mut self, context: &WriteContext, block: &Block) -> anyhow::Result<()> {
//     let rewards = block.rewards.as_ref();
//     let rewards = &(rewards.map(|r| r.to_string()).unwrap_or("".to_string()));
//     let timestamp = context.timestamp.duration_since(std::time::UNIX_EPOCH)?;
//     self.db_pool.get()?.execute("insert into hstore.block (slot, blockhash, blocktime, blockheight, parentslot, rewards, commitment, timestamp, update_timestamp) \
//         values ($1::bigint, $2::text, $3::timestamp, $4::bigint, $5::bigint, $6::json, $7::smallint, $8::timestamp, $9::timestamp)",
//                                     params![
//                                         &block.block_slot,
//                                         &block.blockhash,
//                                         &block.block_time,
//                                         &block.block_height,
//                                         &block.parent_slot,
//                                         rewards,
//                                         &(block.commitment_level as i16),
//                                         &timestamp,
//                                         &timestamp,
//                                    ])?;
//     Ok(())
// }

// async fn persist() {
//     let _ = writer.write_transaction(&context, &transactions).await?;
//     let _ = writer.write_block(&context, &block).await?;
// }
