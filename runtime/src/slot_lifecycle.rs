//! Per-slot lifecycle instrumentation for optimistic confirmation latency.
//!
//! Two components:
//!
//! 1. `SlotLifecycle` — lives on `Bank`, stamped by any thread via atomics.
//!    Records: createdBank, frozen, optimisticConfirmation, root.
//!
//! 2. `VoteLifecycleTracker` — owned by `process_votes_loop` thread.
//!    Records: per-slot vote accumulation thresholds (10%, 25%, 50%, 66.7%),
//!    vote source breakdown (gossip vs replay).
//!
//! Both emit structured log lines with rdtsc-derived microsecond timings.
//! Correlate by slot number and wallclock anchors in ClickHouse.
//!
//! # Safety
//! Uses `std::arch::x86_64::_rdtsc()` — safe: read-only, no side effects,
//! monotonic on modern x86_64 (invariant TSC).

use {
    solana_clock::Slot,
    std::{
        collections::HashMap,
        sync::{
            OnceLock,
            atomic::{AtomicU64, Ordering},
        },
        time::{Duration, Instant, SystemTime},
    },
};

// --- TSC utilities ---

/// Read CPU timestamp counter. ~1-2ns overhead.
#[inline(always)]
pub fn rdtsc() -> u64 {
    // SAFETY: _rdtsc is always safe to call on x86_64.
    #[cfg(target_arch = "x86_64")]
    unsafe { std::arch::x86_64::_rdtsc() }
    #[cfg(not(target_arch = "x86_64"))]
    { 0 }
}

fn calibrate_tsc_mhz() -> f64 {
    let start_tsc = rdtsc();
    let start = Instant::now();
    std::thread::sleep(Duration::from_millis(10));
    let elapsed_us = start.elapsed().as_micros() as f64;
    let elapsed_tsc = rdtsc().wrapping_sub(start_tsc) as f64;
    elapsed_tsc / elapsed_us
}

static TSC_MHZ: OnceLock<f64> = OnceLock::new();

pub fn tsc_mhz() -> f64 {
    *TSC_MHZ.get_or_init(calibrate_tsc_mhz)
}

#[inline(always)]
fn cycles_to_us(cycles: u64) -> i64 {
    (cycles as f64 / tsc_mhz()) as i64
}

#[inline]
fn tsc_delta_us(end: u64, start: u64) -> i64 {
    if end == 0 || start == 0 { -1 } else { cycles_to_us(end.wrapping_sub(start)) }
}

fn wallclock_us() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

// =============================================================================
// SlotLifecycle — lives on Bank, stamped by any thread via atomics
// =============================================================================

/// Per-slot lifecycle timestamps. Lives on `Bank`.
/// All fields are atomic for lock-free cross-thread stamping.
/// 0 means "not yet recorded".
#[derive(Debug, Default)]
pub struct SlotLifecycle {
    pub created_bank_tsc: AtomicU64,
    pub created_bank_wallclock_us: AtomicU64,
    pub frozen_tsc: AtomicU64,
    pub optimistic_confirmation_tsc: AtomicU64,
    pub root_tsc: AtomicU64,
}

impl SlotLifecycle {
    #[inline]
    pub fn stamp_created_bank(&self) {
        self.created_bank_tsc.store(rdtsc(), Ordering::Relaxed);
        self.created_bank_wallclock_us.store(wallclock_us(), Ordering::Relaxed);
    }

    #[inline]
    pub fn stamp_frozen(&self) {
        self.frozen_tsc.store(rdtsc(), Ordering::Relaxed);
    }

    #[inline]
    pub fn stamp_optimistic_confirmation(&self) {
        self.optimistic_confirmation_tsc.store(rdtsc(), Ordering::Relaxed);
    }

    #[inline]
    pub fn stamp_root(&self) {
        self.root_tsc.store(rdtsc(), Ordering::Relaxed);
    }

    pub fn log_lifecycle(&self, slot: Slot) {
        let created = self.created_bank_tsc.load(Ordering::Relaxed);
        let frozen = self.frozen_tsc.load(Ordering::Relaxed);
        let confirmed = self.optimistic_confirmation_tsc.load(Ordering::Relaxed);
        let root = self.root_tsc.load(Ordering::Relaxed);

        log::info!(
            "slot_bank_lifecycle slot={} \
             created_bank_wallclock_us={} \
             created_to_frozen_us={} \
             created_to_confirmed_us={} \
             created_to_root_us={} \
             frozen_to_confirmed_us={} \
             confirmed_to_root_us={}",
            slot,
            self.created_bank_wallclock_us.load(Ordering::Relaxed),
            tsc_delta_us(frozen, created),
            tsc_delta_us(confirmed, created),
            tsc_delta_us(root, created),
            tsc_delta_us(confirmed, frozen),
            tsc_delta_us(root, confirmed),
        );
    }
}

// =============================================================================
// VoteLifecycleTracker — owned by process_votes_loop, tracks vote accumulation
// =============================================================================

const STAKE_THRESHOLDS: [f64; 4] = [0.10, 0.25, 0.50, 2.0 / 3.0];

struct VoteTimeline {
    first_vote_tsc: u64,
    first_vote_wallclock_us: u64,
    thresholds_tsc: [u64; 4],
    gossip_vote_count: u32,
    replay_vote_count: u32,
    confirmed_by_gossip: bool,
    logged: bool,
}

impl VoteTimeline {
    fn new(tsc: u64, wallclock: u64) -> Self {
        Self {
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
}

impl VoteLifecycleTracker {
    pub fn new() -> Self {
        let mhz = tsc_mhz();
        log::info!("VoteLifecycleTracker: TSC calibrated at {mhz:.1} MHz");
        Self { slots: HashMap::new() }
    }

    pub fn record_vote(
        &mut self,
        slot: Slot,
        cumulative_stake: u64,
        total_stake: u64,
        is_gossip: bool,
    ) {
        let now = rdtsc();
        let wc = wallclock_us();
        let timeline = self.slots.entry(slot).or_insert_with(|| VoteTimeline::new(now, wc));

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

        log::info!(
            "slot_vote_lifecycle slot={} \
             first_vote_wallclock_us={} \
             first_vote_to_10pct_us={} \
             first_vote_to_25pct_us={} \
             first_vote_to_50pct_us={} \
             first_vote_to_66pct_us={} \
             gossip_votes={} \
             replay_votes={} \
             total_votes={} \
             confirmed_by_gossip={}",
            slot,
            t.first_vote_wallclock_us,
            tsc_delta_us(t.thresholds_tsc[0], base),
            tsc_delta_us(t.thresholds_tsc[1], base),
            tsc_delta_us(t.thresholds_tsc[2], base),
            tsc_delta_us(t.thresholds_tsc[3], base),
            t.gossip_vote_count,
            t.replay_vote_count,
            t.gossip_vote_count + t.replay_vote_count,
            t.confirmed_by_gossip,
        );
    }

    pub fn prune(&mut self, root: Slot) {
        self.slots.retain(|slot, _| *slot > root);
    }
}
