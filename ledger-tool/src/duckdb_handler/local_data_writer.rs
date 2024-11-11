use anyhow::{anyhow, Context};
use crossbeam_channel::{Receiver, Sender};
use duckdb::{params, Appender, DuckdbConnectionManager};
use r2d2::Pool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

static BLOCK_THREAD_DONE: AtomicBool = AtomicBool::new(false);
static TX_THREAD_DONE: AtomicBool = AtomicBool::new(false);
static ACCOUNT_THREAD_DONE: AtomicBool = AtomicBool::new(false);

const TX_BATCH_SIZE: usize = 1000;
const ACCOUNT_BATCH_SIZE: usize = 1000;
const ACCOUNT_CHANNEL_SIZE: usize = 1_000_000;

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

pub trait LocalWriter {
    fn write_transaction(
        &mut self,
        context: &WriteContext,
        transaction_infos: &Vec<TransactionInfo>,
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;
    fn write_block(
        &mut self,
        context: &WriteContext,
        block: &Block,
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;
    fn update_commitment(
        &mut self,
        context: &WriteContext,
        commitment_level: CommitmentLevel,
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;
    fn copy_data_to_file(
        &mut self,
        start_block: u64,
        end_block: u64,
        path: &str,
    ) -> impl std::future::Future<Output = anyhow::Result<usize>> + Send;
    fn truncate_data(
        &mut self,
        block: u64,
    ) -> impl std::future::Future<Output = anyhow::Result<(usize, usize, usize)>> + Send;
}

#[derive(Clone)]
pub struct LocalWriterImpl {
    db_pool: Pool<DuckdbConnectionManager>,
}

impl LocalWriterImpl {
    pub fn new(
        db_pool: Pool<DuckdbConnectionManager>,
        block_rx: Receiver<Block>,
        tx_rx: Receiver<TransactionInfo>,
    ) -> Result<Self, anyhow::Error> {
        let writer = LocalWriterImpl {
            db_pool: db_pool.clone(),
        };

        Self::spawn_block_processor(db_pool.clone(), block_rx);

        let (account_tx, account_rx) = crossbeam_channel::bounded(ACCOUNT_CHANNEL_SIZE);
        Self::spawn_transaction_processor(db_pool.clone(), tx_rx, account_tx);
        Self::spawn_account_processor(db_pool, account_rx);

        Ok(writer)
    }

    fn spawn_block_processor(pool: Pool<DuckdbConnectionManager>, mut block_rx: Receiver<Block>) {
        tokio::spawn(async move {
            let conn = pool.get().expect("Failed to get connection for blocks");
            let mut appender = conn
                .appender_to_db("block", "hstore")
                .expect("Failed to create block appender");
            let mut block_count = 0;

            while let Ok(block) = block_rx.recv() {
                Self::write_block_to_appender(&mut appender, &block);
                block_count += 1;

                if block_count >= TX_BATCH_SIZE {
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
            .unwrap_or(Duration::from_secs(0));

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
            &rewards,
            &(block.commitment_level as i16),
            &timestamp,
            &timestamp,
        ]) {
            tracing::error!("Failed to append block {}: {}", block.block_slot, e);
        }
    }

    fn spawn_transaction_processor(
        pool: Pool<DuckdbConnectionManager>,
        mut tx_rx: Receiver<TransactionInfo>,
        account_tx: Sender<(String, String, u64, SystemTime)>,
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
                Self::write_transaction(&mut tx_appender, &tx, &account_tx);
                tx_count += 1;

                if tx_count >= TX_BATCH_SIZE {
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

    fn write_transaction(
        tx_appender: &mut Appender,
        tx: &TransactionInfo,
        account_tx: &Sender<(String, String, u64, SystemTime)>,
    ) {
        let timestamp = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0));

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
        mut account_rx: Receiver<(String, String, u64, SystemTime)>,
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

                if batch_count >= ACCOUNT_BATCH_SIZE {
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
}

impl LocalWriter for LocalWriterImpl {
    async fn write_transaction(
        &mut self,
        context: &WriteContext,
        transaction_infos: &Vec<TransactionInfo>,
    ) -> anyhow::Result<()> {
        let timestamp = context.timestamp.duration_since(std::time::UNIX_EPOCH)?;

        let p = self.db_pool.get()?;
        let mut tran_appender = p
            .appender_to_db("transaction_info", "hstore")
            .with_context(|| "Cannot construct appender for transaction_info / hstore")?;
        let mut acct_appender = p
            .appender_to_db("account_to_transaction", "hstore")
            .with_context(|| "Cannot construct appender for account_to_transaction / hstore")?;

        for transaction_info in transaction_infos {
            tran_appender.append_row(params![
                &transaction_info.block_slot,
                &transaction_info.signature,
                &transaction_info.is_vote,
                &transaction_info.index,
                &(transaction_info.message_type as i16),
                &transaction_info.message.to_string(),
                &timestamp,
            ])?;

            for account_key in &transaction_info.account_keys {
                acct_appender.append_row(params![
                    &account_key,
                    &transaction_info.signature,
                    &transaction_info.block_slot,
                    &timestamp
                ])?
            }
        }

        acct_appender.flush()?;
        tran_appender.flush()?;
        Ok(())
    }

    async fn write_block(&mut self, context: &WriteContext, block: &Block) -> anyhow::Result<()> {
        let rewards = block.rewards.as_ref();
        let rewards = &(rewards.map(|r| r.to_string()).unwrap_or("".to_string()));
        let timestamp = context.timestamp.duration_since(std::time::UNIX_EPOCH)?;
        self.db_pool.get()?.execute("insert into hstore.block (slot, blockhash, blocktime, blockheight, parentslot, rewards, commitment, timestamp, update_timestamp) \
        values ($1::bigint, $2::text, $3::timestamp, $4::bigint, $5::bigint, $6::json, $7::smallint, $8::timestamp, $9::timestamp)",
                                    params![
                                        &block.block_slot,
                                        &block.blockhash,
                                        &block.block_time,
                                        &block.block_height,
                                        &block.parent_slot,
                                        rewards,
                                        &(block.commitment_level as i16),
                                        &timestamp,
                                        &timestamp,
                                   ])?;
        Ok(())
    }

    async fn update_commitment(
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

    async fn copy_data_to_file(
        &mut self,
        start_block: u64,
        end_block: u64,
        path: &str,
    ) -> anyhow::Result<usize> {
        // Wait for processing to complete
        while !BLOCK_THREAD_DONE.load(Ordering::Acquire)
            || !TX_THREAD_DONE.load(Ordering::Acquire)
            || !ACCOUNT_THREAD_DONE.load(Ordering::Acquire)
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tracing::info!("Processing complete, starting copy...");
        let start_time = std::time::Instant::now();

        if start_block > end_block {
            return Err(anyhow!("End block {end_block} < start_block {start_block}"));
        }
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

    async fn truncate_data(&mut self, block: u64) -> anyhow::Result<(usize, usize, usize)> {
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
