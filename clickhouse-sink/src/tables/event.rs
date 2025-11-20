use clickhouse::Row;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Serialize, Deserialize, Row, Clone)]
pub struct EventData {
    // TODO: Fill in
    pub placeholder: String,
}

#[derive(Debug, Serialize, Deserialize, Row, Clone)]
pub struct Event {
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    pub timestamp: OffsetDateTime,
    pub data: EventData,
}
