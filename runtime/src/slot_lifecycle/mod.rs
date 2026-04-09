//! Per-slot lifecycle instrumentation for optimistic confirmation latency.
//!
//! Configured via `SLOT_LIFECYCLE_CLICKHOUSE=url=http://host:port,db=default,user=default,password=...`
//! If unset, all instrumentation is a no-op.

mod bank_lifecycle;
mod fast_pubkey;
mod sink;
mod tsc;
mod vote_tracker;

pub use bank_lifecycle::SlotLifecycle;
pub use vote_tracker::VoteLifecycleTracker;

use sink::{EpochStakeRow, TableRow};

/// Record the full epoch stake map to ClickHouse. Called at startup and epoch boundaries.
pub fn record_epoch_stakes(
    epoch: u64,
    vote_accounts: &solana_vote::vote_account::VoteAccounts,
    total_epoch_stake: u64,
) {
    for (vote_pubkey, vote_account) in vote_accounts.iter() {
        let stake = vote_accounts.get_delegated_stake(vote_pubkey);
        sink::record(TableRow::EpochStake(EpochStakeRow {
            epoch,
            vote_pubkey: vote_pubkey.to_string(),
            node_pubkey: vote_account.node_pubkey().to_string(),
            stake,
            total_epoch_stake,
        }));
    }
    log::info!(
        "Recorded epoch stake map: epoch={epoch}, validators={}, total_stake={total_epoch_stake}",
        vote_accounts.len(),
    );
}
