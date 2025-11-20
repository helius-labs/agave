use crate::table_batcher::serialize_as_json_string;
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TxnMonitorCustomerTxnLogData {
    pub signature: String,
    pub slot: u64,
    pub landed: bool,
    pub transaction_err: String,
    pub api_key: String,
    pub project_id: String,
}

#[derive(Debug, Serialize, Deserialize, Row, Clone)]
pub struct TxnMonitorCustomerTxnLogs {
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    pub timestamp: OffsetDateTime,
    #[serde(serialize_with = "serialize_as_json_string")]
    pub data: TxnMonitorCustomerTxnLogData,
    pub signature: String,
}
