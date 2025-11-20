use crate::table_batcher::serialize_as_json_string;
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TxnSenderBundleLogData {
    pub signature: String,
    pub total_cu: u64,
    pub total_tip: u64,
    pub total_txns: u64,
}

#[derive(Debug, Serialize, Deserialize, Row, Clone)]
pub struct TxnSenderBundleLogs {
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    pub timestamp: OffsetDateTime,
    #[serde(serialize_with = "serialize_as_json_string")]
    pub data: TxnSenderBundleLogData,
    pub signature: String,
}
