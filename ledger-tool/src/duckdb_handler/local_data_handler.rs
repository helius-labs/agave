use super::local_data_writer::Block;
use super::local_data_writer::LocalWriterImpl;
use super::local_data_writer::TransactionInfo;
use anyhow::Context;
use anyhow::Result;
use crossbeam_channel::Receiver;
use duckdb::{params, DuckdbConnectionManager, OptionalExt};
use r2d2::Pool;
use solana_sdk::commitment_config::CommitmentLevel;
use std::time::Duration;
use tracing::info;
#[derive(Clone)]
pub struct LocalStore {
    pub db_pool: Pool<DuckdbConnectionManager>,
}

impl LocalStore {
    pub fn new(db_file: &Option<String>) -> anyhow::Result<Self> {
        let db_pool = get_connection(db_file)?;
        Ok(Self { db_pool })
    }

    pub fn init(&self) -> anyhow::Result<&Self> {
        self.db_pool.get()?.execute_batch(r"
        create schema if not exists hstore;
        PRAGMA temp_directory='/tmp/duckdb_spillover';
        PRAGMA memory_limit='256GB';

        DROP TABLE IF EXISTS hstore.block;
        DROP TABLE IF EXISTS hstore.transaction_info;
        DROP TABLE IF EXISTS hstore.account_to_transaction;

        CREATE TABLE if not exists hstore.block(
            slot             ubigint           not null,
            blockHash        text              not null,
            blockTime        timestamp         not null,
            blockHeight      ubigint           not null,
            parentSlot       ubigint           not null,
            rewards          json,
            commitment       usmallint         not null, -- 1: Processed, 2: confirmed, 3: finalized
            timestamp        timestamp         not null,
            update_timestamp timestamp         not null, -- commitment changes later, keeping track
         );

         CREATE TABLE if not exists hstore.transaction_info
        (
            slot             ubigint     not null,
            signature        text        not null,
            is_vote          BOOL        not null,
            index            ubigint     not null, -- position in the block
            message_type     usmallint   not null, -- 0: legacy, 1: versioned message
            message          json        not null,
            timestamp        timestamp   not null,
        );
        /*
        CREATE INDEX if not exists idx_transaction_info_slot on hstore.transaction_info (slot);
        */
        CREATE TABLE if not exists hstore.account_to_transaction
        (
            address   text        not null,
            signature text        not null,
            slot      ubigint     not null,
            timestamp timestamp   not null,
        );
        /*
        CREATE INDEX if not exists idx_account_to_transaction_addr on hstore.account_to_transaction (address);
        CREATE INDEX if not exists idx_account_to_transaction_addr on hstore.account_to_transaction (signature);
        */
        ")?;
        Ok(self)
    }

    pub fn get_writer(
        &self,
        block_rx: Receiver<Block>,
        tx_rx: Receiver<TransactionInfo>,
    ) -> anyhow::Result<LocalWriterImpl> {
        LocalWriterImpl::new(self.db_pool.clone(), block_rx, tx_rx)
    }

    pub fn get_oldest_and_newest_slot(&self) -> anyhow::Result<(u64, u64)> {
        self.db_pool
            .get()?
            .query_row(
                "select min(slot) as oldest_slot, max(slot) as newest_slot from hstore.block",
                params![],
                |row| {
                    let oldest_slot: u64 = row.get("oldest_slot")?;
                    let newest_slot: u64 = row.get("newest_slot")?;
                    Ok((oldest_slot, newest_slot))
                },
            )
            .with_context(|| "No oldest block")
    }

    pub fn get_block_by_slot_local(&self, slot: u64) -> Result<Option<Block>> {
        let conn = self.db_pool.get()?;
        let mut stmt = conn.prepare("SELECT * FROM hstore.block WHERE slot = ?")?;
        let block = stmt
            .query_row(params![slot], |row| {
                Ok(Block {
                    block_slot: row.get(0)?,
                    blockhash: row.get(1)?,
                    block_time: row
                        .get::<_, Option<i64>>(2)?
                        .map(|t| Duration::from_secs(t as u64)),
                    block_height: row.get(3)?,
                    parent_slot: row.get(4)?,
                    rewards: serde_json::from_str(&row.get::<_, String>(5)?).unwrap(),
                    commitment_level: CommitmentLevel::Confirmed,
                })
            })
            .optional()?;

        Ok(block)
    }

    pub fn get_transaction_by_signature_local(
        &self,
        signature: &str,
    ) -> Result<Option<TransactionInfo>> {
        let conn = self.db_pool.get()?;
        let mut stmt = conn.prepare("SELECT * FROM hstore.transaction_info WHERE signature = ?")?;
        let transaction = stmt
            .query_row(params![signature], |row| {
                Ok(TransactionInfo {
                    block_slot: row.get(0)?,
                    signature: row.get(1)?,
                    is_vote: row.get(2)?,
                    index: row.get(3)?,
                    message_type: row.get(4)?,
                    message: serde_json::from_str(&row.get::<_, String>(5)?).unwrap(),
                    account_keys: Vec::new(), // You might need to fetch this separately
                })
            })
            .optional()?;

        Ok(transaction)
    }

    pub fn get_transactions_for_address_local(
        &self,
        address: &str,
        limit: usize,
    ) -> Result<Vec<TransactionInfo>> {
        let conn = self.db_pool.get()?;
        let mut stmt = conn.prepare(
            "SELECT t.* FROM hstore.account_to_transaction a
             JOIN hstore.transaction_info t ON a.signature = t.signature
             WHERE a.address = ? LIMIT ?",
        )?;
        let transactions = stmt.query_map(params![address, limit as u32], |row| {
            Ok(TransactionInfo {
                block_slot: row.get(0)?,
                signature: row.get(1)?,
                is_vote: row.get(2)?,
                index: row.get(3)?,
                message_type: row.get(4)?,
                message: serde_json::from_str(&row.get::<_, String>(5)?).unwrap(),
                account_keys: Vec::new(), // You might need to fetch this separately
            })
        })?;

        transactions
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
}

fn get_connection(file: &Option<String>) -> anyhow::Result<Pool<DuckdbConnectionManager>> {
    // let manager = file
    //     .as_ref()
    //     .map(DuckdbConnectionManager::file)
    //     .unwrap_or(DuckdbConnectionManager::memory())?;

    let manager = DuckdbConnectionManager::memory()?;

    info!("Starting DuckDB connection {:?}", file);
    let pool = Pool::new(manager)?;
    Ok(pool)
}
