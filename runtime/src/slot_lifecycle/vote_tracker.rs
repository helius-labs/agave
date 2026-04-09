//! `VoteLifecycleTracker` — owned by `process_votes_loop` thread.
//! Tracks per-slot vote accumulation and records individual votes.

use {
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    std::collections::HashMap,
    super::{
        fast_pubkey::PubkeyEncodingCache,
        sink::{self, VoteLifecycleRow, SlotVoteRow, TableRow},
        tsc::{hostname, now_datetime, rdtsc, tsc_delta_us, tsc_mhz, wallclock_us},
    },
};

const STAKE_THRESHOLDS: [f64; 4] = [0.10, 0.25, 0.50, 2.0 / 3.0];

struct VoteTimeline {
    epoch: u64,
    first_vote_tsc: u64,
    first_vote_wallclock_us: u64,
    thresholds_tsc: [u64; 4],
    gossip_vote_count: u32,
    replay_vote_count: u32,
    confirmed_by_gossip: bool,
    logged: bool,
}

impl VoteTimeline {
    fn new(tsc: u64, wallclock: u64, epoch: u64) -> Self {
        Self {
            epoch,
            first_vote_tsc: tsc,
            first_vote_wallclock_us: wallclock,
            thresholds_tsc: [0; 4],
            gossip_vote_count: 0,
            replay_vote_count: 0,
            confirmed_by_gossip: false,
            logged: false,
        }
    }
}

pub struct VoteLifecycleTracker {
    slots: HashMap<Slot, VoteTimeline>,
    pubkey_encoding_cache: PubkeyEncodingCache,
}

impl VoteLifecycleTracker {
    pub fn new() -> Self {
        let mhz = tsc_mhz();
        log::info!("VoteLifecycleTracker: TSC calibrated at {mhz:.1} MHz");
        Self {
            slots: HashMap::new(),
            pubkey_encoding_cache: PubkeyEncodingCache::new(),
        }
    }

    pub fn record_vote(
        &mut self,
        slot: Slot,
        epoch: u64,
        vote_pubkey: &Pubkey,
        stake: u64,
        cumulative_stake: u64,
        total_stake: u64,
        is_gossip: bool,
    ) {
        // Record individual vote
        let vote_pubkey_str = self.pubkey_encoding_cache.encode(vote_pubkey);
        sink::record(TableRow::Vote(SlotVoteRow {
            timestamp: now_datetime(),
            host: hostname(),
            slot,
            epoch,
            vote_pubkey: vote_pubkey_str,
            stake,
            is_gossip,
        }));

        // Track accumulation
        let now = rdtsc();
        let wc = wallclock_us();
        let timeline = self.slots.entry(slot).or_insert_with(|| VoteTimeline::new(now, wc, epoch));

        if is_gossip {
            timeline.gossip_vote_count += 1;
        } else {
            timeline.replay_vote_count += 1;
        }

        let stake_pct = cumulative_stake as f64 / total_stake as f64;
        let mut should_log = false;

        for (i, threshold) in STAKE_THRESHOLDS.iter().enumerate() {
            if timeline.thresholds_tsc[i] == 0 && stake_pct >= *threshold {
                timeline.thresholds_tsc[i] = now;
                if i == 3 && !timeline.logged {
                    timeline.confirmed_by_gossip = is_gossip;
                    timeline.logged = true;
                    should_log = true;
                }
            }
        }

        if should_log {
            self.log_vote_lifecycle(slot);
        }
    }

    fn log_vote_lifecycle(&self, slot: Slot) {
        let Some(t) = self.slots.get(&slot) else { return };
        let base = t.first_vote_tsc;

        sink::record(TableRow::VoteLifecycle(VoteLifecycleRow {
            timestamp: now_datetime(),
            host: hostname(),
            slot,
            epoch: t.epoch,
            first_vote_wallclock_us: t.first_vote_wallclock_us,
            first_vote_to_10pct_us: tsc_delta_us(t.thresholds_tsc[0], base),
            first_vote_to_25pct_us: tsc_delta_us(t.thresholds_tsc[1], base),
            first_vote_to_50pct_us: tsc_delta_us(t.thresholds_tsc[2], base),
            first_vote_to_66pct_us: tsc_delta_us(t.thresholds_tsc[3], base),
            gossip_votes: t.gossip_vote_count,
            replay_votes: t.replay_vote_count,
            total_votes: t.gossip_vote_count + t.replay_vote_count,
            confirmed_by_gossip: t.confirmed_by_gossip,
        }));
    }

    pub fn prune(&mut self, root: Slot) {
        self.slots.retain(|slot, _| *slot > root);
    }
}
