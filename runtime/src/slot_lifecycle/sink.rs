//! ClickHouse sink — single OS thread, multiplexed tables.
//!
//! Matches the monorepo clickhouse-sink pattern: one `TableRow` enum, one channel,
//! one batcher that dispatches to the correct table. Dedicated `current_thread`
//! tokio runtime with 1 worker thread.

use {
    clickhouse::{Client, Row},
    serde::Serialize,
    std::{
        sync::{Arc, OnceLock, mpsc},
        time::{Duration, Instant},
    },
    time::OffsetDateTime,
};

// --- Row types ---

/// Row for the `slot_bank_lifecycle` table. Emitted on root.
#[derive(Debug, Serialize, Row)]
pub struct BankLifecycleRow {
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    pub timestamp: OffsetDateTime,
    pub host: &'static str,
    pub slot: u64,
    pub epoch: u64,
    pub created_bank_wallclock_us: u64,
    pub created_to_frozen_us: i64,
    pub created_to_confirmed_us: i64,
    pub created_to_root_us: i64,
    pub frozen_to_confirmed_us: i64,
    pub confirmed_to_root_us: i64,
}

/// Row for the `slot_vote_lifecycle` table. Emitted on 2/3 confirmation.
#[derive(Debug, Serialize, Row)]
pub struct VoteLifecycleRow {
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    pub timestamp: OffsetDateTime,
    pub host: &'static str,
    pub slot: u64,
    pub epoch: u64,
    pub first_vote_wallclock_us: u64,
    pub first_vote_to_10pct_us: i64,
    pub first_vote_to_25pct_us: i64,
    pub first_vote_to_50pct_us: i64,
    pub first_vote_to_66pct_us: i64,
    pub gossip_votes: u32,
    pub replay_votes: u32,
    pub total_votes: u32,
    pub confirmed_by_gossip: bool,
}

/// Row for the `slot_votes` table. One row per vote per slot.
#[derive(Debug, Serialize, Row)]
pub struct SlotVoteRow {
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    pub timestamp: OffsetDateTime,
    pub host: &'static str,
    pub slot: u64,
    pub epoch: u64,
    pub vote_pubkey: Arc<str>,
    pub stake: u64,
    pub is_gossip: bool,
}

/// Row for the `epoch_stake_map` table. One row per validator per epoch.
/// Duplicates from multiple validators merge via ReplacingMergeTree.
#[derive(Debug, Serialize, Row)]
pub struct EpochStakeRow {
    pub epoch: u64,
    pub vote_pubkey: String,
    pub node_pubkey: String,
    pub stake: u64,
    pub total_epoch_stake: u64,
}

/// Row for the `gossip_peer_stats` table. Emitted every ~2s from gossip stats.
#[derive(Debug, Serialize, Row)]
pub struct GossipPeerRow {
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    pub timestamp: OffsetDateTime,
    pub host: &'static str,
    pub num_nodes: u64,
    pub num_nodes_staked: u64,
    pub num_pubkeys: u64,
    pub table_size: u64,
    pub purged_values_size: u64,
    pub tvu_peers: u64,
    pub get_votes_count: u64,
}

const FLUSH_BATCH_SIZE: usize = 10_000;
const FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const CHANNEL_SIZE: usize = 1_000_000;

/// Multiplexed row enum — dispatched to the correct table in the flush loop.
#[allow(dead_code)] // GossipPeer constructed in solana-gossip crate
pub enum TableRow {
    BankLifecycle(BankLifecycleRow),
    VoteLifecycle(VoteLifecycleRow),
    Vote(SlotVoteRow),
    EpochStake(EpochStakeRow),
    GossipPeer(GossipPeerRow),
}

// --- Table batcher ---

struct TableBatcher {
    bank_lifecycle_batch: Vec<BankLifecycleRow>,
    vote_lifecycle_batch: Vec<VoteLifecycleRow>,
    vote_batch: Vec<SlotVoteRow>,
    epoch_stake_batch: Vec<EpochStakeRow>,
    gossip_peer_batch: Vec<GossipPeerRow>,
    batch_size: usize,
    flush_interval: Duration,
    last_flush: Instant,
}

impl TableBatcher {
    fn new(batch_size: usize, flush_interval: Duration) -> Self {
        Self {
            bank_lifecycle_batch: Vec::with_capacity(batch_size),
            vote_lifecycle_batch: Vec::with_capacity(batch_size),
            vote_batch: Vec::with_capacity(batch_size),
            epoch_stake_batch: Vec::with_capacity(2048),
            gossip_peer_batch: Vec::with_capacity(64),
            batch_size,
            flush_interval,
            last_flush: Instant::now(),
        }
    }

    fn add_row(&mut self, row: TableRow) {
        match row {
            TableRow::BankLifecycle(r) => self.bank_lifecycle_batch.push(r),
            TableRow::VoteLifecycle(r) => self.vote_lifecycle_batch.push(r),
            TableRow::Vote(r) => self.vote_batch.push(r),
            TableRow::EpochStake(r) => self.epoch_stake_batch.push(r),
            TableRow::GossipPeer(r) => self.gossip_peer_batch.push(r),
        }
    }

    fn should_flush(&self) -> bool {
        self.bank_lifecycle_batch.len() >= self.batch_size
            || self.vote_lifecycle_batch.len() >= self.batch_size
            || self.vote_batch.len() >= self.batch_size
            || !self.epoch_stake_batch.is_empty()
            || (self.last_flush.elapsed() >= self.flush_interval
                && (!self.bank_lifecycle_batch.is_empty()
                    || !self.vote_lifecycle_batch.is_empty()
                    || !self.vote_batch.is_empty()
                    || !self.gossip_peer_batch.is_empty()))
    }

    async fn maybe_flush(&mut self, client: &Client) {
        if self.should_flush() {
            self.flush(client).await;
        }
    }

    async fn flush(&mut self, client: &Client) {
        if !self.bank_lifecycle_batch.is_empty() {
            flush_rows(client, "slot_bank_lifecycle", &mut self.bank_lifecycle_batch).await;
        }
        if !self.vote_lifecycle_batch.is_empty() {
            flush_rows(client, "slot_vote_lifecycle", &mut self.vote_lifecycle_batch).await;
        }
        if !self.vote_batch.is_empty() {
            flush_rows(client, "slot_votes", &mut self.vote_batch).await;
        }
        if !self.epoch_stake_batch.is_empty() {
            flush_rows(client, "epoch_stake_map", &mut self.epoch_stake_batch).await;
        }
        if !self.gossip_peer_batch.is_empty() {
            flush_rows(client, "gossip_peer_stats", &mut self.gossip_peer_batch).await;
        }
        self.last_flush = Instant::now();
    }
}

async fn flush_rows<T: Row + Serialize>(client: &Client, table: &str, batch: &mut Vec<T>) {
    let count = batch.len();
    let mut insert = match client.insert(table) {
        Ok(i) => i,
        Err(e) => {
            log::warn!("CH {table}: insert failed, dropping {count} rows: {e}");
            batch.clear();
            return;
        }
    };
    for row in batch.drain(..) {
        if let Err(e) = insert.write(&row).await {
            log::warn!("CH {table}: write failed: {e}");
            return;
        }
    }
    if let Err(e) = insert.end().await {
        log::warn!("CH {table}: flush failed: {e}");
    }
}

// --- Sink thread ---

struct Sink {
    tx: mpsc::SyncSender<TableRow>,
}

static SINK: OnceLock<Option<Sink>> = OnceLock::new();

fn get_sink() -> &'static Option<Sink> {
    SINK.get_or_init(|| {
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

        log::info!("SlotLifecycle ClickHouse: url={url} db={db}");

        let client = Client::default()
            .with_url(&url)
            .with_user(&user)
            .with_password(&password)
            .with_database(&db);

        let (tx, rx) = mpsc::sync_channel::<TableRow>(CHANNEL_SIZE);

        std::thread::Builder::new()
            .name("slotLifecycleCH".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("tokio runtime for slot lifecycle CH");
                rt.block_on(async {
                    let mut batcher = TableBatcher::new(FLUSH_BATCH_SIZE, FLUSH_INTERVAL);
                    loop {
                        batcher.maybe_flush(&client).await;

                        match rx.try_recv() {
                            Ok(row) => batcher.add_row(row),
                            Err(mpsc::TryRecvError::Empty) => {
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                            Err(mpsc::TryRecvError::Disconnected) => {
                                batcher.flush(&client).await;
                                log::info!("SlotLifecycle CH sink exiting");
                                break;
                            }
                        }
                    }
                });
            })
            .expect("spawn slot lifecycle CH thread");

        Some(Sink { tx })
    })
}

pub fn record(row: TableRow) {
    if let Some(sink) = get_sink() {
        let _ = sink.tx.try_send(row);
    }
}
