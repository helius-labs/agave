// ClickHouse table schema:
// CREATE TABLE agave_events (
//     event_type LowCardinality(String),
//     slot UInt64,
//     timestamp_us UInt64,
//     timestamp DateTime64(3) DEFAULT toDateTime64(timestamp_us / 1000000, 3),
//     metadata String
// ) ENGINE = MergeTree()
// ORDER BY (event_type, slot, timestamp_us)
// TTL timestamp + INTERVAL 1 DAY;

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
