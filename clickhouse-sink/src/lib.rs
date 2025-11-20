use clickhouse::Client;

pub mod config;
pub mod sink;
pub mod table_batcher;
pub mod tables;

pub use config::ClickhouseConfig;

pub async fn init(config: ClickhouseConfig) -> anyhow::Result<()> {
    let client = Client::default()
        .with_url(&config.url)
        .with_user(&config.user)
        .with_password(&config.password)
        .with_database(&config.database)
        .with_validation(false)
        .with_option("allow_experimental_json_type", "1")
        .with_option("input_format_binary_read_json_as_string", "1");

    sink::Sink::init(client, config).await;

    tracing::info!("ClickHouse sink initialized");
    Ok(())
}

// #[cfg(test)]
// mod tests {
//     use std::time::Duration;

//     use time::OffsetDateTime;

//     use crate::{
//         sink::record,
//         table_batcher::TableRow,
//         tables::txn_sender_logs::{DroppedData, TxnSenderLogsDropped, TxnSenderLogsLanded},
//     };

//     use super::*;

//     #[tokio::test]
//     async fn test_record() {
//         let config = ClickhouseConfig {
//             url: "http://64.130.59.126:8123".to_string(),
//             user: "default".to_string(),
//             password: "clickhouse-logs".to_string(),
//             database: "clickhouse_logs".to_string(),
//             env: Some("prod".to_string()),
//             host: Some("atlas-rpc-ams-1".to_string()),
//         };
//         init(config, 0, Duration::from_millis(1)).await.unwrap();
//         tokio::spawn(async move {
//             for i in 0..10 {
//                 record(TableRow::TxnSenderLogsDropped(TxnSenderLogsDropped {
//                     timestamp: OffsetDateTime::now_utc(),
//                     data: DroppedData {
//                         signature: "test".to_string(),
//                         slot: 1,
//                         leader: "test".to_string(),
//                         datacenter: "ams".to_string(),
//                         staked: false,
//                         ip_address: "127.0.0.1".to_string(),
//                         api_key: "test".to_string(),
//                         tip_amount: 1,
//                         txn_source: "test".to_string(),
//                         txn_strategy: "test".to_string(),
//                         dropped: true,
//                         priority: 1,
//                         send_error: "test".to_string(),
//                         end_to_end_latency_ns: 1,
//                     },
//                     signature: "test".to_string(),
//                 }));
//             }
//         });
//         tokio::time::sleep(std::time::Duration::from_secs(10)).await;
//     }
// }
