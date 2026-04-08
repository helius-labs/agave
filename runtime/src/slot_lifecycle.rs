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
//! Data flows to a dedicated ClickHouse `slot_lifecycle` table via a background
//! thread using `reqwest::blocking` (JSONEachRow format). Configured via env var:
//!   `SLOT_LIFECYCLE_CLICKHOUSE=url=http://host:port,db=default,user=default,password=...`
//!
//! # Safety
//! Uses `std::arch::x86_64::_rdtsc()` — safe: read-only, no side effects,
//! monotonic on modern x86_64 (invariant TSC).

use {
    serde::Serialize,
    solana_clock::Slot,
    std::{
        collections::HashMap,
        sync::{
            OnceLock,
            atomic::{AtomicU64, Ordering},
            mpsc,
        },
        time::{Duration, Instant, SystemTime},
    },
};

// --- TSC utilities ---

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

fn hostname() -> &'static str {
    static HOSTNAME: OnceLock<String> = OnceLock::new();
    HOSTNAME.get_or_init(|| {
        gethostname::gethostname()
            .to_string_lossy()
            .into_owned()
    })
}

// =============================================================================
// ClickHouse sink — reqwest::blocking, JSONEachRow format
// =============================================================================

const TABLE: &str = "slot_lifecycle";
const FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const FLUSH_BATCH_SIZE: usize = 1024;
const CHANNEL_SIZE: usize = 100_000;

/// Row written to the `slot_lifecycle` ClickHouse table.
#[derive(Debug, Serialize)]
struct SlotLifecycleRow {
    host: &'static str,
    slot: u64,
    event_type: &'static str,
    created_bank_wallclock_us: u64,
    first_vote_wallclock_us: u64,
    created_to_frozen_us: i64,
    created_to_confirmed_us: i64,
    created_to_root_us: i64,
    frozen_to_confirmed_us: i64,
    confirmed_to_root_us: i64,
    first_vote_to_10pct_us: i64,
    first_vote_to_25pct_us: i64,
    first_vote_to_50pct_us: i64,
    first_vote_to_66pct_us: i64,
    gossip_votes: u32,
    replay_votes: u32,
    total_votes: u32,
    confirmed_by_gossip: bool,
}

struct SinkConfig {
    /// Full INSERT URL: `http://host:port/?database=db&user=u&password=p&query=INSERT+INTO+...`
    insert_url: String,
}

static SINK_TX: OnceLock<Option<mpsc::SyncSender<SlotLifecycleRow>>> = OnceLock::new();

fn init_sink() -> Option<mpsc::SyncSender<SlotLifecycleRow>> {
    let config_str = std::env::var("SLOT_LIFECYCLE_CLICKHOUSE").ok()?;
    let mut url = String::new();
    let mut db = "default".to_string();
    let mut user = "default".to_string();
    let mut password = String::new();

    for pair in config_str.split(',') {
        let mut parts = pair.splitn(2, '=');
        let key = parts.next()?;
        let val = parts.next().unwrap_or("");
        match key {
            "url" => url = val.to_string(),
            "db" => db = val.to_string(),
            "user" => user = val.to_string(),
            "password" => password = val.to_string(),
            _ => {}
        }
    }

    if url.is_empty() {
        return None;
    }

    let insert_url = format!(
        "{url}/?database={db}&user={user}&password={password}\
         &query=INSERT%20INTO%20{TABLE}%20FORMAT%20JSONEachRow"
    );

    log::info!("SlotLifecycle ClickHouse sink: url={url} db={db}");

    let config = SinkConfig { insert_url };
    let (tx, rx) = mpsc::sync_channel::<SlotLifecycleRow>(CHANNEL_SIZE);

    std::thread::Builder::new()
        .name("slotLifecycleCH".to_string())
        .spawn(move || flush_loop(config, rx))
        .expect("failed to spawn slot lifecycle CH thread");

    Some(tx)
}

fn flush_loop(config: SinkConfig, rx: mpsc::Receiver<SlotLifecycleRow>) {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("failed to build reqwest client");

    let mut batch: Vec<SlotLifecycleRow> = Vec::with_capacity(FLUSH_BATCH_SIZE);
    let mut last_flush = Instant::now();

    loop {
        match rx.try_recv() {
            Ok(row) => {
                batch.push(row);
                if batch.len() >= FLUSH_BATCH_SIZE {
                    flush(&client, &config, &mut batch);
                    last_flush = Instant::now();
                }
            }
            Err(mpsc::TryRecvError::Empty) => {
                if last_flush.elapsed() >= FLUSH_INTERVAL && !batch.is_empty() {
                    flush(&client, &config, &mut batch);
                    last_flush = Instant::now();
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                flush(&client, &config, &mut batch);
                log::info!("SlotLifecycle CH sink exiting");
                break;
            }
        }
    }
}

fn flush(client: &reqwest::blocking::Client, config: &SinkConfig, batch: &mut Vec<SlotLifecycleRow>) {
    if batch.is_empty() {
        return;
    }

    let mut body = String::new();
    for row in batch.iter() {
        if let Ok(json) = serde_json::to_string(row) {
            body.push_str(&json);
            body.push('\n');
        }
    }
    let count = batch.len();
    batch.clear();

    match client.post(&config.insert_url).body(body).send() {
        Ok(resp) if resp.status().is_success() => {}
        Ok(resp) => {
            let status = resp.status();
            let text = resp.text().unwrap_or_default();
            log::warn!("SlotLifecycle CH flush failed ({count} rows): {status} {text}");
        }
        Err(e) => {
            log::warn!("SlotLifecycle CH flush failed ({count} rows): {e}");
        }
    }
}

/// Send a row to the ClickHouse sink (if configured). Non-blocking.
fn ch_record(row: SlotLifecycleRow) {
    let tx = SINK_TX.get_or_init(|| init_sink());
    if let Some(tx) = tx {
        let _ = tx.try_send(row);
    }
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

        ch_record(SlotLifecycleRow {
            host: hostname(),
            slot,
            event_type: "bank_lifecycle",
            created_bank_wallclock_us: self.created_bank_wallclock_us.load(Ordering::Relaxed),
            created_to_frozen_us: tsc_delta_us(frozen, created),
            created_to_confirmed_us: tsc_delta_us(confirmed, created),
            created_to_root_us: tsc_delta_us(root, created),
            frozen_to_confirmed_us: tsc_delta_us(confirmed, frozen),
            confirmed_to_root_us: tsc_delta_us(root, confirmed),
            first_vote_wallclock_us: 0,
            first_vote_to_10pct_us: -1,
            first_vote_to_25pct_us: -1,
            first_vote_to_50pct_us: -1,
            first_vote_to_66pct_us: -1,
            gossip_votes: 0,
            replay_votes: 0,
            total_votes: 0,
            confirmed_by_gossip: false,
        });
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

        ch_record(SlotLifecycleRow {
            host: hostname(),
            slot,
            event_type: "vote_lifecycle",
            first_vote_wallclock_us: t.first_vote_wallclock_us,
            first_vote_to_10pct_us: tsc_delta_us(t.thresholds_tsc[0], base),
            first_vote_to_25pct_us: tsc_delta_us(t.thresholds_tsc[1], base),
            first_vote_to_50pct_us: tsc_delta_us(t.thresholds_tsc[2], base),
            first_vote_to_66pct_us: tsc_delta_us(t.thresholds_tsc[3], base),
            gossip_votes: t.gossip_vote_count,
            replay_votes: t.replay_vote_count,
            total_votes: t.gossip_vote_count + t.replay_vote_count,
            confirmed_by_gossip: t.confirmed_by_gossip,
            created_bank_wallclock_us: 0,
            created_to_frozen_us: -1,
            created_to_confirmed_us: -1,
            created_to_root_us: -1,
            frozen_to_confirmed_us: -1,
            confirmed_to_root_us: -1,
        });
    }

    pub fn prune(&mut self, root: Slot) {
        self.slots.retain(|slot, _| *slot > root);
    }
}
