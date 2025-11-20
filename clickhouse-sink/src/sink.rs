use std::{
    sync::mpsc::{self, TryRecvError},
    time::Duration,
};

use clickhouse::Client;
use once_cell::sync::OnceCell;
use tokio::time::sleep;
use tracing::{error, info};

use crate::{
    table_batcher::{TableBatcher, TableRow},
    ClickhouseConfig,
};

const DEFAULT_SINK_CHANNEL_SIZE: usize = 10_000_000;

static SINK: OnceCell<Sink> = OnceCell::new();
pub static CONFIG: OnceCell<ClickhouseConfig> = OnceCell::new();

/// Record an event with JSON data and specific timestamp
pub fn record(table_row: TableRow) {
    let Some(sink) = SINK.get() else {
        return;
    };
    sink.send(table_row);
}

/// Generic sink for any Flushable type
#[derive(Debug)]
pub struct Sink {
    table_row_tx: mpsc::SyncSender<TableRow>,
}

impl Sink {
    /// Create a new sink for a specific flushable type
    pub async fn init(client: Client, config: ClickhouseConfig) {
        let (table_row_tx, table_row_rx) = mpsc::sync_channel(DEFAULT_SINK_CHANNEL_SIZE);

        SINK.set(Self { table_row_tx })
            .expect("Sink already initialized");
        CONFIG
            .set(config.clone())
            .expect("Event context already initialized");

        tokio::spawn(async move {
            info!("Starting flush thread");
            let mut table_batcher =
                TableBatcher::new(config.flush_batch_size, config.flush_interval);
            loop {
                table_batcher.maybe_flush(&client).await;
                let table_row = match table_row_rx.try_recv() {
                    Ok(table_row) => table_row,
                    Err(TryRecvError::Empty) => {
                        sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                    Err(TryRecvError::Disconnected) => {
                        info!("Exiting clickhouse sink");
                        break;
                    }
                };
                table_batcher.add_table_row(table_row);
            }
        });
    }

    /// Send a flushable item to the sink
    pub fn send(&self, item: TableRow) {
        if let Err(e) = self.table_row_tx.try_send(item) {
            error!("Failed to send table row to sink: {}", e);
        }
    }
}
