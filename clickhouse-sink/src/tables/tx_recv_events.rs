/*
CREATE TABLE IF NOT EXISTS tx_recv_events (
    signature String CODEC(ZSTD(3)),
    slot UInt64 CODEC(T64, ZSTD(3)),
    rx_time_micros DateTime64(6) CODEC(DoubleDelta),
    source LowCardinality(String),
    region LowCardinality(String)
)
ENGINE = MergeTree()
PARTITION BY (region, toStartOfHour(rx_time_micros))
ORDER BY (rx_time_micros, signature, source)
PRIMARY KEY (rx_time_micros)
TTL rx_time_micros + INTERVAL 1 DAY
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1,
    merge_with_ttl_timeout = 86400;
 */

use clickhouse::Row;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Serialize, Deserialize, Row, Clone)]
pub struct TxRecvEvent {
    pub signature: String,
    pub slot: u64,
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    pub rx_time_micros: OffsetDateTime,
    pub source: String,
    pub region: String,
}
