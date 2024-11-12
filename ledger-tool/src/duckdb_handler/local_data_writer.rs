use anyhow::{anyhow, Context};
use crossbeam_channel::{Receiver, Sender};
use duckdb::types::{FromSql, Type, ValueRef};
use duckdb::{params, Appender, DuckdbConnectionManager};
use r2d2::Pool;
use solana_sdk::commitment_config::CommitmentLevel;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

static BLOCK_THREAD_DONE: AtomicBool = AtomicBool::new(false);
static TX_THREAD_DONE: AtomicBool = AtomicBool::new(false);
static ACCOUNT_THREAD_DONE: AtomicBool = AtomicBool::new(false);

#[derive(Clone)]
pub struct Block {
    pub block_slot: u64,
    pub blockhash: String,
    pub block_time: Option<Duration>,
    pub block_height: u64,
    pub parent_slot: u64,
    pub rewards: Option<String>,
    pub commitment_level: CommitmentLevel,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i16)]
pub enum MessageType {
    Legacy = 0,
    Versioned = 1,
}

impl FromSql for MessageType {
    fn column_result(value: ValueRef<'_>) -> duckdb::types::FromSqlResult<Self> {
        match value.data_type() {
            Type::TinyInt | Type::SmallInt | Type::BigInt => {
                let bytes = value.as_blob()?;
                if bytes.len() < 2 {
                    return Err(duckdb::types::FromSqlError::InvalidType);
                }
                let int_value = i16::from_le_bytes([bytes[0], bytes[1]]);
                match int_value {
                    0 => Ok(MessageType::Legacy),
                    1 => Ok(MessageType::Versioned),
                    _ => Err(duckdb::types::FromSqlError::InvalidType),
                }
            }
            _ => Err(duckdb::types::FromSqlError::InvalidType),
        }
    }
}

pub struct TransactionInfo {
    pub block_slot: u64,
    pub signature: String,
    pub is_vote: bool,
    pub index: u64,
    pub message_type: MessageType,
    pub message: String,
    pub account_keys: Vec<String>,
}

pub struct HistoricalTransactionInfo {
    pub block_slot: u64,
    pub block_hash: String,
    pub block_time: SystemTime,
    pub block_height: u64,
    pub parent_slot: u64,
    pub rewards: Option<String>,
    pub commitment: CommitmentLevel,
    pub timestamp: SystemTime,
    pub update_timestamp: SystemTime,
    pub signature: String,
    pub is_vote: bool,
    pub address: String,
}

pub struct WriteContext {
    pub block_slot: u64,
    pub timestamp: SystemTime,
}

#[derive(Clone)]
pub struct LocalWriter {
    db_pool: Pool<DuckdbConnectionManager>,
    config: WriterConfig,
}

// Create a custom error type
#[derive(thiserror::Error, Debug)]
pub enum WriterError {
    #[error("Database error: {0}")]
    Database(#[from] duckdb::Error),
    #[error("Pool error: {0}")]
    Pool(#[from] r2d2::Error),
    #[error("Channel error: {0}")]
    Channel(String),
    #[error("Invalid block range: end {end} < start {start}")]
    InvalidBlockRange { start: u64, end: u64 },
}

#[derive(Clone)]
pub struct WriterConfig {
    pub tx_batch_size: usize,
    pub account_batch_size: usize,
    pub account_channel_size: usize,
    pub vacuum_after_truncate: bool,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            tx_batch_size: 1000,
            account_batch_size: 1000,
            account_channel_size: 1_000_000,
            vacuum_after_truncate: true,
        }
    }
}

impl LocalWriter {
    pub fn new(
        db_pool: Pool<DuckdbConnectionManager>,
        block_rx: Receiver<Block>,
        tx_rx: Receiver<TransactionInfo>,
    ) -> Result<Self, WriterError> {
        Self::new_with_config(db_pool, block_rx, tx_rx, WriterConfig::default())
    }

    pub fn new_with_config(
        db_pool: Pool<DuckdbConnectionManager>,
        block_rx: Receiver<Block>,
        tx_rx: Receiver<TransactionInfo>,
        config: WriterConfig,
    ) -> Result<Self, WriterError> {
        let writer = LocalWriter {
            db_pool: db_pool.clone(),
            config: config.clone(),
        };

        Self::spawn_block_processor(db_pool.clone(), block_rx, config.tx_batch_size);

        // Create channel with Duration type
        let (account_tx, account_rx) = crossbeam_channel::bounded::<(String, String, u64, Duration)>(
            config.account_channel_size,
        );

        Self::spawn_transaction_processor(db_pool.clone(), tx_rx, account_tx, config.tx_batch_size);
        Self::spawn_account_processor(db_pool, account_rx, config.account_batch_size);

        Ok(writer)
    }

    fn spawn_block_processor(
        pool: Pool<DuckdbConnectionManager>,
        block_rx: Receiver<Block>,
        batch_size: usize,
    ) {
        tokio::spawn(async move {
            let conn = pool.get().expect("Failed to get connection for blocks");
            let mut appender = conn
                .appender_to_db("block", "hstore")
                .expect("Failed to create block appender");
            let mut block_count = 0;

            while let Ok(block) = block_rx.recv() {
                Self::write_block_to_appender(&mut appender, &block);
                block_count += 1;

                if block_count >= batch_size {
                    if let Err(e) = appender.flush() {
                        tracing::error!("Failed to flush block appender: {}", e);
                    }
                    block_count = 0;
                }
            }

            if block_count > 0 {
                if let Err(e) = appender.flush() {
                    tracing::error!("Failed to flush block appender: {}", e);
                }
            }
            BLOCK_THREAD_DONE.store(true, Ordering::Release);
        });
    }

    fn write_block_to_appender(appender: &mut Appender, block: &Block) {
        let timestamp = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards");

        let rewards = block
            .rewards
            .as_ref()
            .map(|r| r.to_string())
            .unwrap_or_default();

        if let Err(e) = appender.append_row(params![
            &block.block_slot,
            &block.blockhash,
            &block.block_time,
            &block.block_height,
            &block.parent_slot,
            rewards,
            &(block.commitment_level as i16),
            &timestamp,
            &timestamp,
        ]) {
            tracing::error!("Failed to append block {}: {}", block.block_slot, e);
        }
    }

    fn spawn_transaction_processor(
        pool: Pool<DuckdbConnectionManager>,
        tx_rx: Receiver<TransactionInfo>,
        account_tx: Sender<(String, String, u64, Duration)>, // Changed to Duration
        batch_size: usize,
    ) {
        tokio::spawn(async move {
            let conn = pool
                .get()
                .expect("Failed to get connection for transactions");
            let mut tx_appender = conn
                .appender_to_db("transaction_info", "hstore")
                .expect("Failed to create transaction appender");
            let mut tx_count = 0;

            while let Ok(tx) = tx_rx.recv() {
                let timestamp = SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("Time went backwards");

                if let Err(e) = tx_appender.append_row(params![
                    &tx.block_slot,
                    &tx.signature,
                    &tx.is_vote,
                    &tx.index,
                    &(tx.message_type as i16),
                    &tx.message,
                    &timestamp,
                ]) {
                    tracing::error!("Failed to append transaction {}: {}", tx.signature, e);
                }

                // Send account mappings
                for account_key in &tx.account_keys {
                    if let Err(e) = account_tx.send((
                        account_key.clone(),
                        tx.signature.clone(),
                        tx.block_slot,
                        timestamp,
                    )) {
                        tracing::error!("Failed to send account mapping: {}", e);
                    }
                }

                tx_count += 1;

                if tx_count >= batch_size {
                    if let Err(e) = tx_appender.flush() {
                        tracing::error!("Failed to flush transaction appender: {}", e);
                    }
                    tx_count = 0;
                }
            }

            if tx_count > 0 {
                if let Err(e) = tx_appender.flush() {
                    tracing::error!("Failed to flush transaction appender: {}", e);
                }
            }
            drop(account_tx);
            TX_THREAD_DONE.store(true, Ordering::Release);
        });
    }

    fn write_transaction_to_appender(
        tx_appender: &mut Appender,
        tx: &TransactionInfo,
        account_tx: &Sender<(String, String, u64, f64)>,
    ) {
        let timestamp = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_secs_f64();

        if let Err(e) = tx_appender.append_row(params![
            &tx.block_slot,
            &tx.signature,
            &tx.is_vote,
            &tx.index,
            &(tx.message_type as i16),
            &tx.message,
            &timestamp,
        ]) {
            tracing::error!("Failed to append transaction {}: {}", tx.signature, e);
        }

        for account_key in &tx.account_keys {
            if let Err(e) = account_tx.send((
                account_key.clone(),
                tx.signature.clone(),
                tx.block_slot,
                timestamp,
            )) {
                tracing::error!("Failed to send account mapping: {}", e);
            }
        }
    }

    fn spawn_account_processor(
        pool: Pool<DuckdbConnectionManager>,
        account_rx: Receiver<(String, String, u64, Duration)>, // Changed to Duration
        batch_size: usize,
    ) {
        tokio::spawn(async move {
            let conn = pool.get().expect("Failed to get connection for accounts");
            let mut appender = conn
                .appender_to_db("account_to_transaction", "hstore")
                .expect("Failed to create account appender");
            let mut batch_count = 0;

            while let Ok((address, signature, slot, timestamp)) = account_rx.recv() {
                if let Err(e) =
                    appender.append_row(params![&address, &signature, &slot, &timestamp])
                {
                    tracing::error!("Failed to append account {}: {}", address, e);
                }
                batch_count += 1;

                if batch_count >= batch_size {
                    if let Err(e) = appender.flush() {
                        tracing::error!("Failed to flush account appender: {}", e);
                    }
                    batch_count = 0;
                }
            }

            if batch_count > 0 {
                if let Err(e) = appender.flush() {
                    tracing::error!("Failed to flush account appender: {}", e);
                }
            }
            ACCOUNT_THREAD_DONE.store(true, Ordering::Release);
        });
    }

    pub async fn _update_commitment(
        &mut self,
        context: &WriteContext,
        commitment_level: CommitmentLevel,
    ) -> anyhow::Result<()> {
        let timestamp = context.timestamp.duration_since(std::time::UNIX_EPOCH)?;
        self.db_pool.get()?.execute(
            r"update hstore.block
        set commitment = $1::bigint,
        update_timestamp = $2::timestamp
        where slot = $3::bigint",
            params![&(commitment_level as i16), &timestamp, &context.block_slot],
        )?;
        Ok(())
    }

    pub async fn copy_data_to_file(
        &mut self,
        start_block: u64,
        end_block: u64,
        path: &str,
    ) -> Result<usize, WriterError> {
        if start_block > end_block {
            return Err(WriterError::InvalidBlockRange {
                start: start_block,
                end: end_block,
            });
        }
        // Wait for processing to complete
        while !BLOCK_THREAD_DONE.load(Ordering::Acquire)
            || !TX_THREAD_DONE.load(Ordering::Acquire)
            || !ACCOUNT_THREAD_DONE.load(Ordering::Acquire)
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tracing::info!("Processing complete, starting copy...");
        let start_time = std::time::Instant::now();

        let connection = self.db_pool.get()?;
        // First set the PRAGMAs
        connection.execute_batch(
            r"
        PRAGMA enable_progress_bar=true;
        PRAGMA progress_bar_time=1000;
        ",
        )?;
        let sql = format!(
            r"COPY (
        select b.*, t.signature, t.is_vote, t.message,
            hash(signature) % 8 as sig_hash,
            cast(round(b.slot/10000)*10000 as ubigint) as round_slot
        from hstore.block b
            inner join hstore.transaction_info t on b.slot = t.slot
            where b.slot < $1::bigint and b.slot >= $2::bigint and b.commitment = $3::smallint
            order by b.slot asc, t.index asc)
        TO '{path}'
        (FORMAT 'parquet', COMPRESSION 'snappy', ROW_GROUP_SIZE 100_000, partition_by (round_slot, sig_hash), OVERWRITE TRUE);"
        );

        let rows_copied = connection.execute(
            sql.as_str(),
            params![end_block, start_block, &(CommitmentLevel::Finalized as i16)],
        )?;

        let elapsed = start_time.elapsed();
        tracing::info!(
            "Copy operation copied {} rows in {:.2}s ({:.2} rows/s)",
            rows_copied,
            elapsed.as_secs_f32(),
            rows_copied as f32 / elapsed.as_secs_f32()
        );

        Ok(rows_copied)
    }

    pub async fn truncate_data(&mut self, block: u64) -> anyhow::Result<(usize, usize, usize)> {
        let blocks_deleted = {
            let con = self.db_pool.get()?;
            con.execute(
                "delete from hstore.block where slot < $1::bigint and commitment = $2::smallint",
                params![&block, &(CommitmentLevel::Finalized as i16)],
            )?
        };

        let trans_deleted = {
            let con = self.db_pool.get()?;
            con.execute(
                "delete from hstore.transaction_info where slot not in (select slot from hstore.block) and slot < $1::bigint",
                params![&block])?
        };
        let accounts_deleted = {
            let con = self.db_pool.get()?;
            con.execute(
                "delete from hstore.account_to_transaction where slot not in (select slot from hstore.block) and slot < $1::bigint",
                params![&block])?
        };

        {
            let con = self.db_pool.get()?;
            con.execute_batch(
                "VACUUM ANALYZE hstore.account_to_transaction;
                VACUUM ANALYZE hstore.transaction_info",
            )?;
        };
        Ok((blocks_deleted, trans_deleted, accounts_deleted))
    }
}
