/*
CREATE TABLE IF NOT EXISTS laserstream_worker_messages (
    timestamp DateTime64(3),
    host LowCardinality(String),
    stage LowCardinality(String),
    kind LowCardinality(String),
    size_bytes UInt32,
    slot UInt64,
    latency_us UInt32,
    sample_key UInt32 DEFAULT xxHash32(timestamp)
) ENGINE = MergeTree()
PARTITION BY (toStartOfHour(timestamp), host)
ORDER BY (stage, sample_key, timestamp)
SAMPLE BY sample_key
TTL timestamp + INTERVAL 1 DAY
SETTINGS index_granularity = 8192;
*/

use clickhouse::Row;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Serialize, Deserialize, Row, Clone)]
pub struct LaserstreamWorkerMessages {
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    pub timestamp: OffsetDateTime,
    pub host: String,
    pub stage: String,
    pub latency_us: u32,
    pub kind: String,
    pub size_bytes: u32,
    pub slot: u64,
}

impl LaserstreamWorkerMessages {
    pub fn new(
        host: impl Into<String>,
        stage: impl Into<String>,
        latency_us: u32,
        kind: impl Into<String>,
        size_bytes: u32,
        slot: u64,
    ) -> Self {
        Self {
            timestamp: OffsetDateTime::now_utc(),
            host: host.into(),
            stage: stage.into(),
            latency_us,
            kind: kind.into(),
            size_bytes,
            slot,
        }
    }
}
