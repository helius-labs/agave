use crate::table_batcher::serialize_as_json_string;
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TxnMonitorTipLogData {
    pub slot: u64,
    pub transaction: String,
    pub signature: String,
    pub transaction_err: String,
    pub tip_amount: u64,
    pub provider: String,
}

#[derive(Debug, Serialize, Deserialize, Row, Clone)]
pub struct TxnMonitorTipLogs {
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    pub timestamp: OffsetDateTime,
    #[serde(serialize_with = "serialize_as_json_string")]
    pub data: TxnMonitorTipLogData,
    pub signature: String,
}
