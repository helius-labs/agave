use crate::table_batcher::serialize_as_json_string;
use clickhouse::Row;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Row, Clone)]
pub struct AgaveEvent {
    pub event_type: String,
    pub slot: u64,
    pub timestamp_us: u64,
    #[serde(serialize_with = "serialize_as_json_string")]
    pub metadata: serde_json::Value,
}
