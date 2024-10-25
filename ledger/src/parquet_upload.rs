use crate::blockstore_meta::SlotMeta;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::ConfirmedBlock;
use std::collections::HashMap;
use {
    crate::blockstore::Blockstore,
    crossbeam_channel::{bounded, unbounded},
    log::*,
    solana_measure::measure::Measure,
    solana_sdk::clock::Slot,
    std::{
        cmp::{max, min},
        collections::HashSet,
        result::Result,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    },
};

#[derive(Clone)]
pub struct ConfirmedBlockUploadConfig {
    pub force_reupload: bool,
    pub max_num_slots_to_check: usize,
    pub num_blocks_to_upload_in_parallel: usize,
    pub block_read_ahead_depth: usize, // should always be >= `num_blocks_to_upload_in_parallel`
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

pub async fn upload_confirmed_blocks(
    blockstore: Arc<Blockstore>,
    starting_slot: Slot,
    ending_slot: Slot,
    config: ConfirmedBlockUploadConfig,
    exit: Arc<AtomicBool>,
) -> Result<Slot, Box<dyn std::error::Error>> {
    let allow_dead_slots = true; // You might want to make this configurable
    let verbose_level = 1; // Adjust as needed

    Ok(starting_slot)
}
