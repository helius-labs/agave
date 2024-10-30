use {
    crate::{
        error::{LedgerToolError, Result},
        ledger_utils::get_program_ids,
    },
    chrono::{Local, TimeZone},
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
        time::{Duration, Instant},
    },
};

use arrow::array::BinaryArray;
use arrow::array::Datum;
use arrow::array::Int16Array;
use arrow::array::TimestampMillisecondArray;
use arrow::array::UInt16Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::TimeUnit;
use arrow::{
    array::{ArrayRef, BooleanArray, StringArray, UInt64Array},
    record_batch::RecordBatch,
};
use chrono::Utc;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use downcast::Any;
use futures::executor::block_on;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::basic::Compression;
use rayon::prelude::*;
use std::fs::File;
use serde_json::Value;
use datafusion::dataframe::DataFrameWriteOptions;
use std::collections::HashSet;
use arrow::array::AsArray;
use arrow::compute::filter_record_batch;
use arrow::array::StringBuilder;
use arrow::array::Array;
use std::path::Path;
use datafusion::logical_expr::{create_udf, Volatility};
use arrow::compute::take;

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
        "{prefix}Entry {} - num_hashes: {}, hash: {}, transactions: {}, starting_transaction_index: {}",
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

fn encode_versioned_transactions(block: BlockWithoutMetadata) -> EncodedConfirmedBlock {
    let transactions = block
        .transactions
        .into_iter()
        .map(|transaction| EncodedTransactionWithStatusMeta {
            transaction: transaction.encode(UiTransactionEncoding::Base64),
            meta: None,
            version: None,
        })
        .collect();

    EncodedConfirmedBlock {
        previous_blockhash: Hash::default().to_string(),
        blockhash: block.blockhash,
        parent_slot: block.parent_slot,
        transactions,
        rewards: Rewards::default(),
        num_partitions: None,
        block_time: None,
        block_height: None,
    }
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
            BlockContents::BlockWithoutMetadata(block) => Ok(encode_versioned_transactions(block)),
        }
    }
}

pub fn output_slot_wrapper(blockstore: &Blockstore, slot: Slot) -> Result<()> {


    write_slots_to_parquet(
        blockstore,
        blockstore.lowest_slot(),
        blockstore.lowest_slot(),
    );

    Ok(())
}

pub fn output_slot_2(
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
    total_slots_processed: u64,
    total_records_written: u64,
    recent_rates: VecDeque<f64>,
    window_size: usize,
    total_slots: u64,
}

impl ProcessingStats {
    fn new(window_size: usize, start_slot: u64, end_slot: u64) -> Self {
        Self {
            start_time: Instant::now(),
            last_batch_time: Instant::now(),
            total_slots_processed: 0,
            total_records_written: 0,
            recent_rates: VecDeque::with_capacity(window_size),
            window_size,
            total_slots: end_slot - start_slot,
        }
    }

    fn update(&mut self, slots_in_batch: u64, records_in_batch: u64) {
        let now = Instant::now();
        let batch_duration = now.duration_since(self.last_batch_time);
        let rate = records_in_batch as f64 / batch_duration.as_secs_f64();

        self.total_slots_processed += slots_in_batch;
        self.total_records_written += records_in_batch;

        // Using known total records for estimation
        let total_expected_records = 230_000_000;
        let remaining_records = total_expected_records - self.total_records_written;

        // Calculate time remaining based on current rate
        let current_rate =
            self.total_records_written as f64 / now.duration_since(self.start_time).as_secs_f64();
        let estimated_remaining_secs = remaining_records as f64 / current_rate;

        // Convert to hours, minutes, seconds
        let hours = (estimated_remaining_secs / 3600.0) as u64;
        let minutes = ((estimated_remaining_secs % 3600.0) / 60.0) as u64;
        let seconds = (estimated_remaining_secs % 60.0) as u64;

        println!(
            "Batch Stats:\n\
             - Records: {}\n\
             - Current Rate: {:.2} records/sec\n\
             - Overall Rate: {:.2} records/sec\n\
             - Total Records: {}\n\
             - Elapsed Time: {:.2}s\n\
             - Estimated Time Remaining: {}h {}m {}s\n\
             - Progress: {:.2}%",
            records_in_batch,
            rate,
            current_rate,
            self.total_records_written,
            now.duration_since(self.start_time).as_secs_f64(),
            hours,
            minutes,
            seconds,
            (self.total_slots_processed as f64 / self.total_slots as f64) * 100.0
        );

        self.last_batch_time = now;
    }

    fn final_stats(&self) {
        let total_duration = self.last_batch_time.duration_since(self.start_time);
        let overall_rate = self.total_records_written as f64 / total_duration.as_secs_f64();

        println!(
            "\nFinal Statistics:\n\
             - Total Slots Processed: {}\n\
             - Total Records Written: {}\n\
             - Overall Rate: {:.2} records/sec\n\
             - Total Time: {:.2}s",
            self.total_slots_processed,
            self.total_records_written,
            overall_rate,
            total_duration.as_secs_f64(),
        );
    }
}

fn write_slots_to_parquet(blockstore: &Blockstore, start_slot: Slot, end_slot: Slot) {
    println!("Processing slots {} to {}", start_slot, end_slot);

    let mut stats = ProcessingStats::new(5, start_slot, end_slot);
    let batch_size = 100;

    // Create DataFusion context
    let ctx = SessionContext::new();

    // Define schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("address", DataType::Binary, false),
        Field::new("signature", DataType::Binary, false),
        Field::new("slot", DataType::UInt64, false),
        Field::new("index", DataType::UInt16, false),
        Field::new("rewards", DataType::Utf8, true), // Using Utf8 for JSON string
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));

    // Create memory table to accumulate batches
    block_on(async {
        let mem_table = MemTable::try_new(schema.clone(), vec![vec![]]).unwrap();
        ctx.register_table("blocks", Arc::new(mem_table));

        let mut batches = Vec::new();

        // Process in batches
        for batch_start in (start_slot..=end_slot).step_by(batch_size) {
            let batch_end = std::cmp::min(batch_start + batch_size as u64, end_slot);
            println!("\nProcessing batch {} to {}", batch_start, batch_end);

            let slot_data: Vec<_> = (batch_start..=batch_end)
                .into_par_iter()
                .filter_map(|slot| {
                    let result = output_slot_2(
                        blockstore,
                        slot,
                        true,
                        &OutputFormat::DisplayVerbose,
                        1000,
                        &mut HashMap::new(),
                    );
                    if slot % 1000 == 0 {
                        println!("Completed slot {} - found data: {}", slot, result.is_some());
                    }
                    result.map(|block_contents| process_block_contents(slot, block_contents))
                })
                .collect();

            if slot_data.is_empty() {
                continue;
            }

            // Process data in chunks
            for chunk in slot_data.chunks(100) {
                let now = Utc::now().timestamp_millis();
                let mut all_rows = Vec::new();

                for (slot, hash, time, height, parent, transactions, reward) in chunk {
                    for transaction in transactions {
                        all_rows.push((
                            *slot,
                            hash.clone(),
                            *time,
                            *height,
                            *parent,
                            transaction.clone(),
                            reward.clone(),
                            2i16,
                            now,
                            now,
                        ));
                    }
                }

                let lengy = all_rows.len().clone();
                if !all_rows.is_empty() {
                    let batch = create_record_batch(&schema, all_rows);
                    println!("\nNew batch created:");
                    println!("  Row count: {}", batch.num_rows());
                    println!("  Column count: {}", batch.num_columns());
                    if batch.num_rows() > 0 {
                        println!("  First row sample:");
                        for i in 0..batch.num_columns() {
                            println!("    Column {}: {:?}", i, batch.column(i).get());
                        }
                    }

                    // Also print the actual rows we're trying to add
                    println!("  Rows being added: {}", lengy);

                    batches.push(batch);
                }
                // After processing each batch of slots:
                println!("\nBatch validation:");
                println!("  Total batches in memory: {}", batches.len());
                if let Some(last_batch) = batches.last() {
                    println!("  Current batch size: {}", last_batch.num_rows());
                    println!(
                        "  Memory size of batches: {} bytes",
                        batches
                            .iter()
                            .map(|b| b.get_array_memory_size())
                            .sum::<usize>()
                    );
                }
            }

            stats.update(
                batch_size as u64,
                slot_data
                    .iter()
                    .map(|(_, _, _, _, _, txs, _)| txs.len() as u64)
                    .sum(),
            );
        }

        // Create output directories
        let output_dir = "/root/parquet_output";
        let table_dir = format!("{}/address_activity", output_dir);
        std::fs::create_dir_all(&table_dir).unwrap();

        println!("Original batch has {} rows", batches[0].num_rows());

        // Group rows by address
        let mut rows_by_address: HashMap<String, Vec<usize>> = HashMap::new();
        
        // Process all rows in the batch
        let total_rows = batches[0].num_rows();
        for row_idx in 0..total_rows {
            let addr_bytes = batches[0].column(0).as_binary::<i32>().value(row_idx);
            let addr = bs58::encode(addr_bytes).into_string();
            rows_by_address.entry(addr).or_default().push(row_idx);
        }

        println!("\nFound {} unique addresses", rows_by_address.len());
        
        // Write each group to its own file
        for (addr, row_indices) in rows_by_address {
            let output_file = format!("{}/address={}/data.parquet", table_dir, addr);
            std::fs::create_dir_all(Path::new(&output_file).parent().unwrap()).unwrap();
            
            // Create boolean mask for filtering
            let mut mask = vec![false; total_rows];
            for &idx in &row_indices {
                mask[idx] = true;
            }
            let bool_array = BooleanArray::from(mask);
            
            // Create batch with all rows for this address
            let filtered_batch = filter_record_batch(&batches[0], &bool_array).unwrap();
            
            let file = File::create(&output_file).unwrap();
            let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
            writer.write(&filtered_batch).unwrap();
            writer.close().unwrap();
        }

        println!("Data written to {}", table_dir);

        stats.final_stats();
    });
}

// Helper function to process block contents
fn process_block_contents(
    slot: Slot,
    block_contents: BlockContents,
) -> (Slot, String, i64, u64, u64, Vec<Vec<u8>>, String) {
    match block_contents {
        BlockContents::VersionedConfirmedBlock(block) => (
            slot,
            block.blockhash.to_string(),
            block.block_time.map(|t| t as i64).unwrap_or_default(),
            block.block_height.unwrap_or_default(),
            block.parent_slot,
            block
                .transactions
                .into_iter()
                .map(|tx| serde_json::to_vec(&tx).unwrap_or_default())
                .collect(),
            serde_json::to_string(&block.rewards).unwrap(),
        ),
        BlockContents::BlockWithoutMetadata(block) => (
            slot,
            block.blockhash.clone(),
            0,
            0,
            block.parent_slot,
            block
                .transactions
                .into_iter()
                .map(|tx| serde_json::to_vec(&tx).unwrap_or_default())
                .collect(),
            "[]".to_string(),
        ),
    }
}

// Helper function to create record batch with new schema
fn create_record_batch(
    schema: &Schema,
    rows: Vec<(u64, String, i64, u64, u64, Vec<u8>, String, i16, i64, i64)>,
) -> RecordBatch {
    let mut addresses = Vec::new();
    let mut signatures = Vec::new();
    let mut slots = Vec::new();
    let mut indices = Vec::new();
    let mut rewards = Vec::new();
    let mut timestamps = Vec::new();

    println!("Processing {} rows", rows.len());

    for (
        idx,
        (slot, _hash, timestamp, _height, _parent, tx_data, reward, _commitment, _ts, _update_ts),
    ) in rows.into_iter().enumerate()
    {
        let tx_json = String::from_utf8_lossy(&tx_data);

        match serde_json::from_str::<serde_json::Value>(&tx_json) {
            Ok(tx) => {
                // Safely extract signatures
                let signatures_array = match tx.get("signatures").and_then(|s| s.as_array()) {
                    Some(arr) => arr,
                    None => {
                        // Minimal error output
                        println!("Row {}: Invalid signature format", idx);
                        continue;
                    }
                };

                if signatures_array.is_empty() {
                    continue;
                }

                let signature_bytes = match signatures_array.get(0) {
                    Some(sig) => {
                        if let Some(arr) = sig.as_array() {
                            // If it's a single-element array [1], get the second signature
                            if arr.len() == 1 {
                                match signatures_array.get(1) {
                                    Some(real_sig) => {
                                        if let Some(bytes) = real_sig.as_array()
                                            .and_then(|arr| arr.iter()
                                                .map(|v| v.as_u64().map(|n| n as u8))
                                                .collect::<Option<Vec<u8>>>())
                                        {
                                            if bytes.len() == 32 || bytes.len() == 64 {
                                                if idx % 1000 == 0 {
                                                    println!("Processing vote transaction signature at row {}", idx);
                                                }
                                                bytes
                                            } else {
                                                println!("\nDEBUG - Invalid vote signature at row {}:", idx);
                                                println!("Second signature length: {}", bytes.len());
                                                println!("Raw second signature: {:?}", real_sig);
                                                continue
                                            }
                                        } else {
                                            println!("\nDEBUG - Vote signature parse failure at row {}:", idx);
                                            println!("Raw second signature: {:?}", real_sig);
                                            continue
                                        }
                                    }
                                    None => continue
                                }
                            }
                            // Regular non-vote transaction signature
                            else {
                                if let Some(bytes) = arr.iter()
                                    .map(|v| v.as_u64().map(|n| n as u8))
                                    .collect::<Option<Vec<u8>>>() 
                                {
                                    if bytes.len() == 32 || bytes.len() == 64 {
                                        bytes
                                    } else {
                                        println!("\nDEBUG - Invalid regular signature at row {}:", idx);
                                        println!("Signature length: {}", bytes.len());
                                        println!("Raw signature array: {:?}", arr);
                                        println!("Full transaction: {}", serde_json::to_string_pretty(&tx).unwrap());
                                        continue
                                    }
                                } else {
                                    println!("\nDEBUG - Regular signature parse failure at row {}:", idx);
                                    println!("Raw signature array: {:?}", arr);
                                    println!("Full transaction: {}", serde_json::to_string_pretty(&tx).unwrap());
                                    continue
                                }
                            }
                        }
                        else if let Some(sig_str) = sig.as_str() {
                            match bs58::decode(sig_str).into_vec() {
                                Ok(bytes) if bytes.len() == 32 || bytes.len() == 64 => bytes,
                                _ => {
                                    println!("\nDEBUG - Base58 signature failure at row {}:", idx);
                                    println!("Original string: {:?}", sig_str);
                                    continue
                                }
                            }
                        }
                        else {
                            println!("\nDEBUG - Unknown signature format at row {}:", idx);
                            println!("Raw value: {:?}", sig);
                            println!("Full transaction: {}", serde_json::to_string_pretty(&tx).unwrap());
                            continue
                        }
                    }
                    None => continue
                };

                // Extract account keys
                let account_keys = match tx
                    .get("message")
                    .and_then(|m| m.as_array())
                    .and_then(|arr| arr.get(0))
                    .and_then(|m| m.get("accountKeys"))
                    .and_then(|k| k.as_array())
                {
                    Some(arr) => arr,
                    None => continue,
                };

                for (i, key_array) in account_keys.iter().enumerate() {
                    if let Some(key_bytes) = key_array.as_array().and_then(|arr| {
                        arr.iter()
                            .map(|v| v.as_u64().map(|n| n as u8))
                            .collect::<Option<Vec<u8>>>()
                    }) {
                        addresses.push(key_bytes);
                        signatures.push(signature_bytes.clone());
                        slots.push(slot);
                        indices.push(i as u16);
                        rewards.push(Some(reward.clone()));
                        timestamps.push(timestamp);
                    }
                }
            }
            Err(_) => continue,
        }
    }

    println!("Creating batch with {} addresses", addresses.len());

    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(BinaryArray::from_iter_values(addresses)),
            Arc::new(BinaryArray::from_iter_values(signatures)),
            Arc::new(UInt64Array::from_iter_values(slots)),
            Arc::new(UInt16Array::from_iter_values(indices)),
            Arc::new(StringArray::from_iter(rewards)),
            Arc::new(TimestampMillisecondArray::from_iter_values(timestamps)),
        ],
    )
    .unwrap()
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

fn extract_signature(signatures_array: &Vec<Value>, idx: usize) -> Option<Vec<u8>> {
    // Skip empty arrays silently
    if signatures_array.is_empty() {
        return None;
    }
    
    let first_sig = signatures_array.get(0)?;
    
    // Handle vote transaction format (array with length 1)
    if let Some(arr) = first_sig.as_array() {
        if arr.len() == 1 {
            // For vote transactions, use the second signature
            let second_sig = signatures_array.get(1)?;
            return second_sig.as_array()
                .and_then(|arr| arr.iter()
                    .map(|v| v.as_u64().map(|n| n as u8))
                    .collect::<Option<Vec<u8>>>())
                .filter(|bytes| bytes.len() == 32 || bytes.len() == 64);
        }
        
        // For regular transactions, use the first signature
        return arr.iter()
            .map(|v| v.as_u64().map(|n| n as u8))
            .collect::<Option<Vec<u8>>>()
            .filter(|bytes| bytes.len() == 32 || bytes.len() == 64);
    }
    
    // Handle base58 encoded signatures
    if let Some(sig_str) = first_sig.as_str() {
        return bs58::decode(sig_str)
            .into_vec()
            .ok()
            .filter(|bytes| bytes.len() == 32 || bytes.len() == 64);
    }
    
    None
}
