use crate::blockstore::Blockstore;
use solana_sdk::clock::Slot;
use solana_transaction_status::VersionedConfirmedBlockWithEntries;
use std::{
    fs::File,
    io::BufWriter,
    result::Result,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Clone)]
pub struct ConfirmedBlockUploadConfig {
    pub force_reupload: bool,
    pub max_num_slots_to_check: usize,
    pub num_blocks_to_upload_in_parallel: usize,
    pub block_read_ahead_depth: usize,
}

impl Default for ConfirmedBlockUploadConfig {
    fn default() -> Self {
        let num_blocks_to_upload_in_parallel = num_cpus::get() / 2;
        ConfirmedBlockUploadConfig {
            force_reupload: false,
            max_num_slots_to_check: num_blocks_to_upload_in_parallel * 4,
            num_blocks_to_upload_in_parallel,
            block_read_ahead_depth: num_blocks_to_upload_in_parallel * 2,
        }
    }
}

struct BlockstoreLoadStats {
    num_blocks_read: usize,
    elapsed: Duration,
}

// Add this temporary debug function

pub async fn upload_confirmed_blocks(
    blockstore: Arc<Blockstore>,
    starting_slot: Slot,
) -> Result<Slot, Box<dyn std::error::Error>> {
    panic!();
}

fn process_block_to_csv(
    slot: Slot,
    block: VersionedConfirmedBlockWithEntries,
    writer: Arc<Mutex<csv::Writer<BufWriter<File>>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let timestamp = block.block.block_time.unwrap_or(0);

    for transaction in block.block.transactions {
        let mut writer = writer.lock().unwrap();
        writer.write_record(&[
            transaction.transaction.signatures[0].to_string(),
            transaction.transaction.message.static_account_keys()[0].to_string(),
            slot.to_string(),
            timestamp.to_string(),
        ])?;
    }

    Ok(())
}
