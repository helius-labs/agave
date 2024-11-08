use {duckdb::Error as DuckDBError, solana_ledger::blockstore::BlockstoreError, thiserror::Error};

pub type Result<T> = std::result::Result<T, LedgerToolError>;

#[derive(Error, Debug)]
pub enum LedgerToolError {
    #[error("{0}")]
    Blockstore(#[from] BlockstoreError),

    #[error("{0}")]
    SerdeJson(#[from] simd_json::Error),

    #[error("{0}")]
    TransactionEncode(#[from] solana_transaction_status::EncodeError),

    #[error("{0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Generic(String),

    #[error("{0}")]
    BadArgument(String),
    #[error("{0}")]
    Custom(String),
    #[error("JSON error: {0}")]
    Json(String), // Generic JSON error that can come from either source

    #[error("DuckDB error: {0}")]
    DuckDB(#[from] DuckDBError),
}

impl From<serde_json::Error> for LedgerToolError {
    fn from(e: serde_json::Error) -> Self {
        LedgerToolError::Json(e.to_string())
    }
}
