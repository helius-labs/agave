use crate::table_batcher::serialize_as_json_string;
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DroppedData {
    pub signature: String,
    pub slot: u64,
    pub leader: String,
    pub identity: String,
    pub datacenter: String,
    pub staked: bool,
    pub ip_address: String,
    pub api_key: String,
    pub tip_amount: u64,
    pub txn_source: String,
    pub txn_strategy: String,
    pub dropped: bool,
    pub priority: u64,
    pub send_error: String,
    pub end_to_end_latency_ns: u128,
    pub is_retry: bool,
    pub time_to_start_sending_ns: u128,
    pub request_id: String,
    pub qos_priority: String,
    pub max_retries: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LandedData {
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    pub received_datetime: OffsetDateTime,
    pub signature: String,
    pub slot: u64,
    pub slot_sent: u64,
    pub slot_delta: i64,
    pub datacenter: String,
    pub staked: bool,
    pub leader: String,
    pub identity: String,
    pub ip_address: String,
    pub api_key: String,
    pub tip_amount: u64,
    pub open_uni_ns: u128,
    pub write_all_ns: u128,
    pub end_to_end_latency_ns: u128,
    pub dropped: bool,
    pub txn_source: String,
    pub txn_strategy: String,
    pub priority: u64,
    pub is_current_leader: bool,
    pub is_retry: bool,
    pub time_to_start_sending_ns: u128,
    pub request_id: String,
    pub qos_priority: String,
    pub max_retries: usize,
}

#[derive(Debug, Serialize, Deserialize, Row, Clone)]
pub struct TxnSenderLogsDropped {
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    pub timestamp: OffsetDateTime,
    #[serde(serialize_with = "serialize_as_json_string")]
    pub data: DroppedData,
    pub signature: String,
}

#[derive(Debug, Serialize, Deserialize, Row, Clone)]
pub struct TxnSenderLogsLanded {
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    pub timestamp: OffsetDateTime,
    #[serde(serialize_with = "serialize_as_json_string")]
    pub data: LandedData,
    pub signature: String,
}
