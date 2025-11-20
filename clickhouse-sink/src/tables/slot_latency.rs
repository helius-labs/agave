use crate::table_batcher::serialize_as_json_string;
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DroppedData {
    pub leader_sent: String,
    pub slot_sent: u64,
    pub slot_received: u64,
    pub leader_received: String,
    pub slot_dropped: u64,
    pub leader_dropped: String,
    pub dropped_after_ms: i128,
    pub priority: u64,
    pub max_retries: u64,
    pub account_keys: Vec<String>,
    pub is_relayed: bool,
    pub api_key: String,
    pub client_ip: String,
    pub tip_amount: u64,
    pub txn_strategy: String,
    pub request_id: String,
    pub qos_priority: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LandedData {
    pub leader_sent: String,
    pub slot_sent: u64,
    pub slot_received: u64,
    pub slot_landed: u64,
    pub latency_ns: i128,
    pub first_byte_to_landed_ns: i128,
    pub leaders_landed: String,
    pub position_in_block: u64,
    pub priority: u64,
    pub max_retries: u64,
    pub account_keys: Vec<String>,
    pub is_relayed: bool,
    pub api_key: String,
    pub client_ip: String,
    pub tip_amount: u64,
    pub txn_strategy: String,
    pub failed: bool,
    pub is_recent_slot: bool,
    pub request_id: String,
    pub qos_priority: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum SlotLatencyData {
    Landed(LandedData),
    Dropped(DroppedData),
}

#[derive(Debug, Serialize, Deserialize, Row, Clone)]
pub struct SlotLatency {
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    pub timestamp: OffsetDateTime,
    #[serde(serialize_with = "serialize_as_json_string")]
    pub data: SlotLatencyData,
    pub signature: String,
}
