use clickhouse::Row;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::table_batcher::serialize_as_json_string;

/// The Data table is used to store any table that only has a timestamp and a JSON data field.
/// The table name should be set in the table_name field of the serde_json::Value data field.
#[derive(Debug, Serialize, Deserialize, Row, Clone)]
pub struct Data {
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    pub timestamp: OffsetDateTime,
    #[serde(serialize_with = "serialize_as_json_string")]
    pub data: serde_json::Value,
}
