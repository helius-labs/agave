use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use clickhouse::{insert::Insert, Client, Row};
use serde::{Serialize, Serializer};
use tracing::debug;

use crate::{
    sink::CONFIG,
    tables::{
        agave_events::AgaveEvent,
        data::Data,
        event::Event,
        laserstream_worker_messages::LaserstreamWorkerMessages,
        slot_latency::SlotLatency,
        tx_recv_events::TxRecvEvent,
        txn_monitor_customer_tip_logs::TxnMonitorCustomerTxnLogs,
        txn_monitor_tip_logs::TxnMonitorTipLogs,
        txn_sender_bundle_logs::TxnSenderBundleLogs,
        txn_sender_logs::{TxnSenderLogsDropped, TxnSenderLogsLanded},
        validator_metadata::ValidatorMetadata,
    },
};

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum TableRow {
    AgaveEvents(AgaveEvent),
    Event(Event),
    SlotLatency(SlotLatency),
    Data(Data),
    TxRecvEvent(TxRecvEvent),
    TxnSenderLogsLanded(TxnSenderLogsLanded),
    TxnSenderLogsDropped(TxnSenderLogsDropped),
    TxnMonitorTipLogs(TxnMonitorTipLogs),
    TxnSenderBundleLogs(TxnSenderBundleLogs),
    TxnMonitorCustomerTxnLogs(TxnMonitorCustomerTxnLogs),
    ValidatorMetadata(ValidatorMetadata),
    LaserstreamWorkerMessages(LaserstreamWorkerMessages),
}

struct TableBatch<T: Row + Serialize> {
    batch: Vec<T>,
    last_flush: Instant,
}

impl<T: Row + Serialize> TableBatch<T> {
    fn new() -> Self {
        Self {
            batch: Vec::new(),
            last_flush: Instant::now(),
        }
    }
}

pub struct TableBatcher {
    agave_events: TableBatch<AgaveEvent>,
    events: TableBatch<Event>,
    slot_latency: TableBatch<SlotLatency>,
    tx_recv_events: TableBatch<TxRecvEvent>,
    txn_sender_logs_landed: TableBatch<TxnSenderLogsLanded>,
    txn_sender_logs_dropped: TableBatch<TxnSenderLogsDropped>,
    txn_monitor_tip_logs: TableBatch<TxnMonitorTipLogs>,
    txn_sender_bundle_logs: TableBatch<TxnSenderBundleLogs>,
    txn_monitor_customer_txn_logs: TableBatch<TxnMonitorCustomerTxnLogs>,
    validator_metadata: TableBatch<ValidatorMetadata>,
    laserstream_worker_messages: TableBatch<LaserstreamWorkerMessages>,
    data: HashMap<String, TableBatch<Data>>,
    desired_batch_size: usize,
    flush_interval: Duration,
}

impl TableBatcher {
    pub fn new(desired_batch_size: usize, flush_interval: Duration) -> Self {
        Self {
            agave_events: TableBatch::new(),
            events: TableBatch::new(),
            slot_latency: TableBatch::new(),
            tx_recv_events: TableBatch::new(),
            txn_sender_logs_landed: TableBatch::new(),
            txn_sender_logs_dropped: TableBatch::new(),
            txn_monitor_tip_logs: TableBatch::new(),
            txn_sender_bundle_logs: TableBatch::new(),
            txn_monitor_customer_txn_logs: TableBatch::new(),
            validator_metadata: TableBatch::new(),
            laserstream_worker_messages: TableBatch::new(),
            data: HashMap::new(),
            desired_batch_size,
            flush_interval,
        }
    }

    pub fn add_table_row(&mut self, table_row: TableRow) {
        match table_row {
            TableRow::AgaveEvents(agave_event) => {
                self.add_agave_event(agave_event);
            }
            TableRow::Event(event) => {
                self.add_event(event);
            }
            TableRow::SlotLatency(slot_latency) => self.add_slot_latency(slot_latency),
            TableRow::TxRecvEvent(tx_recv_event) => self.add_tx_recv_event(tx_recv_event),
            TableRow::TxnSenderLogsLanded(txn_sender_logs) => {
                self.add_txn_sender_logs_landed(txn_sender_logs)
            }
            TableRow::TxnSenderLogsDropped(txn_sender_logs) => {
                self.add_txn_sender_logs_dropped(txn_sender_logs)
            }
            TableRow::TxnMonitorTipLogs(txn_monitor_tip_logs) => {
                self.add_txn_monitor_tip_logs(txn_monitor_tip_logs)
            }
            TableRow::TxnSenderBundleLogs(txn_sender_bundle_logs) => {
                self.add_txn_sender_bundle_logs(txn_sender_bundle_logs)
            }
            TableRow::TxnMonitorCustomerTxnLogs(txn_monitor_customer_txn_logs) => {
                self.add_txn_monitor_customer_txn_logs(txn_monitor_customer_txn_logs)
            }
            TableRow::ValidatorMetadata(validator_metadata) => {
                self.add_validator_metadata(validator_metadata)
            }
            TableRow::LaserstreamWorkerMessages(laserstream_worker_messages) => {
                self.add_laserstream_worker_messages(laserstream_worker_messages)
            }
            TableRow::Data(data) => {
                self.add_data(data);
            }
        };
    }

    pub async fn maybe_flush(&mut self, client: &Client) {
        if self.agave_events.batch.len() >= self.desired_batch_size
            || self.agave_events.last_flush.elapsed() >= self.flush_interval
        {
            flush_rows::<AgaveEvent>(&client, std::mem::take(&mut self.agave_events.batch), "agave_events").await;
            self.agave_events.last_flush = Instant::now();
        }

        if self.events.batch.len() >= self.desired_batch_size
            || self.events.last_flush.elapsed() >= self.flush_interval
        {
            flush_rows::<Event>(&client, std::mem::take(&mut self.events.batch), "events").await;
            self.events.last_flush = Instant::now();
        }

        if self.slot_latency.batch.len() >= self.desired_batch_size
            || self.slot_latency.last_flush.elapsed() >= self.flush_interval
        {
            flush_rows::<SlotLatency>(
                &client,
                std::mem::take(&mut self.slot_latency.batch),
                "slot_latency",
            )
            .await;
            self.slot_latency.last_flush = Instant::now();
        }

        if self.tx_recv_events.batch.len() >= self.desired_batch_size
            || self.tx_recv_events.last_flush.elapsed() >= self.flush_interval
        {
            flush_rows::<TxRecvEvent>(
                &client,
                std::mem::take(&mut self.tx_recv_events.batch),
                "tx_recv_events",
            )
            .await;
            self.tx_recv_events.last_flush = Instant::now();
        }

        if self.txn_sender_logs_landed.batch.len() >= self.desired_batch_size
            || self.txn_sender_logs_landed.last_flush.elapsed() >= self.flush_interval
        {
            flush_rows::<TxnSenderLogsLanded>(
                &client,
                std::mem::take(&mut self.txn_sender_logs_landed.batch),
                "txn_sender_logs",
            )
            .await;
            self.txn_sender_logs_landed.last_flush = Instant::now();
        }

        if self.txn_sender_logs_dropped.batch.len() >= self.desired_batch_size
            || self.txn_sender_logs_dropped.last_flush.elapsed() >= self.flush_interval
        {
            flush_rows::<TxnSenderLogsDropped>(
                &client,
                std::mem::take(&mut self.txn_sender_logs_dropped.batch),
                "txn_sender_logs",
            )
            .await;
            self.txn_sender_logs_dropped.last_flush = Instant::now();
        }
        if self.txn_monitor_tip_logs.batch.len() >= self.desired_batch_size
            || self.txn_monitor_tip_logs.last_flush.elapsed() >= self.flush_interval
        {
            flush_rows::<TxnMonitorTipLogs>(
                &client,
                std::mem::take(&mut self.txn_monitor_tip_logs.batch),
                "txn_monitor_tip_logs",
            )
            .await;
            self.txn_monitor_tip_logs.last_flush = Instant::now();
        }
        if self.txn_sender_bundle_logs.batch.len() >= self.desired_batch_size
            || self.txn_sender_bundle_logs.last_flush.elapsed() >= self.flush_interval
        {
            flush_rows::<TxnSenderBundleLogs>(
                &client,
                std::mem::take(&mut self.txn_sender_bundle_logs.batch),
                "txn_sender_bundle_logs",
            )
            .await;
            self.txn_sender_bundle_logs.last_flush = Instant::now();
        }

        if self.txn_monitor_customer_txn_logs.batch.len() >= self.desired_batch_size
            || self.txn_monitor_customer_txn_logs.last_flush.elapsed() >= self.flush_interval
        {
            flush_rows::<TxnMonitorCustomerTxnLogs>(
                &client,
                std::mem::take(&mut self.txn_monitor_customer_txn_logs.batch),
                "txn_monitor_customer_txn_logs",
            )
            .await;
            self.txn_monitor_customer_txn_logs.last_flush = Instant::now();
        }
        if self.validator_metadata.batch.len() >= self.desired_batch_size
            || self.validator_metadata.last_flush.elapsed() >= self.flush_interval
        {
            flush_rows::<ValidatorMetadata>(
                &client,
                std::mem::take(&mut self.validator_metadata.batch),
                "validator_metadata",
            )
            .await;
            self.validator_metadata.last_flush = Instant::now();
        }
        if self.laserstream_worker_messages.batch.len() >= self.desired_batch_size
            || self.laserstream_worker_messages.last_flush.elapsed() >= self.flush_interval
        {
            flush_rows::<LaserstreamWorkerMessages>(
                &client,
                std::mem::take(&mut self.laserstream_worker_messages.batch),
                "laserstream_worker_messages",
            )
            .await;
            self.laserstream_worker_messages.last_flush = Instant::now();
        }
        for (table_name, batch) in self.data.iter_mut() {
            if batch.batch.len() >= self.desired_batch_size
                || batch.last_flush.elapsed() >= self.flush_interval
            {
                flush_rows::<Data>(&client, std::mem::take(&mut batch.batch), table_name).await;
                batch.last_flush = Instant::now();
            }
        }
    }

    fn add_agave_event(&mut self, agave_event: AgaveEvent) {
        self.agave_events.batch.push(agave_event);
    }

    fn add_event(&mut self, event: Event) {
        self.events.batch.push(event);
    }

    fn add_slot_latency(&mut self, slot_latency: SlotLatency) {
        self.slot_latency.batch.push(slot_latency);
    }

    fn add_txn_sender_logs_landed(&mut self, txn_sender_logs: TxnSenderLogsLanded) {
        self.txn_sender_logs_landed.batch.push(txn_sender_logs);
    }

    fn add_txn_sender_logs_dropped(&mut self, txn_sender_logs: TxnSenderLogsDropped) {
        self.txn_sender_logs_dropped.batch.push(txn_sender_logs);
    }

    fn add_txn_monitor_tip_logs(&mut self, txn_monitor_tip_logs: TxnMonitorTipLogs) {
        self.txn_monitor_tip_logs.batch.push(txn_monitor_tip_logs);
    }

    fn add_txn_sender_bundle_logs(&mut self, txn_sender_bundle_logs: TxnSenderBundleLogs) {
        self.txn_sender_bundle_logs
            .batch
            .push(txn_sender_bundle_logs);
    }

    fn add_txn_monitor_customer_txn_logs(
        &mut self,
        txn_monitor_customer_txn_logs: TxnMonitorCustomerTxnLogs,
    ) {
        self.txn_monitor_customer_txn_logs
            .batch
            .push(txn_monitor_customer_txn_logs);
    }

    fn add_validator_metadata(&mut self, validator_metadata: ValidatorMetadata) {
        self.validator_metadata.batch.push(validator_metadata);
    }

    fn add_laserstream_worker_messages(
        &mut self,
        laserstream_worker_messages: LaserstreamWorkerMessages,
    ) {
        self.laserstream_worker_messages
            .batch
            .push(laserstream_worker_messages);
    }

    fn add_tx_recv_event(&mut self, tx_recv_event: TxRecvEvent) {
        self.tx_recv_events.batch.push(tx_recv_event);
    }

    fn add_data(&mut self, data: Data) {
        let Some(table_name) = data.data["table_name"].as_str() else {
            tracing::error!("No table name found in data field, this is required to be set so that clickhouse-sink knows which table to write to.");
            return;
        };
        let batch = self
            .data
            .entry(table_name.to_string())
            .or_insert(TableBatch::new());
        batch.batch.push(data);
    }
}

// Custom serializer function for data field
pub fn serialize_as_json_string<S, T>(data: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: Serialize,
{
    let mut serialized = serde_json::to_value(data).map_err(serde::ser::Error::custom)?;
    if let Some(tags) = CONFIG.get().map(|c| c.get_tags()) {
        for (key, value) in tags {
            serialized[key] = serde_json::Value::String(value);
        }
    }
    let json_string = serde_json::to_string(&serialized).map_err(serde::ser::Error::custom)?;
    serializer.serialize_str(&json_string)
}

async fn flush_rows<T>(client: &Client, mut batch: Vec<T::Value<'_>>, table_name_str: &str)
where
    T: Row + Serialize,
    for<'a> <T as Row>::Value<'a>: Serialize,
{
    if batch.is_empty() {
        return;
    }
    debug!(
        "Flushing {} rows to table '{}'",
        batch.len(),
        table_name_str
    );

    let batch_size = batch.len();
    let mut insert: Insert<T> = match client.insert(table_name_str).await {
        Ok(i) => i,
        Err(e) => {
            tracing::error!(
                "Failed to create insert for table '{}', dropping {} rows: {}",
                table_name_str,
                batch_size,
                e
            );
            return;
        }
    };

    for row in batch.drain(..) {
        let row_insert = insert.write(&row).await;
        if let Err(e) = row_insert {
            tracing::error!(
                "Failed to write row to table '{}', dropping remaining rows: {}",
                table_name_str,
                e
            );
        }
    }

    if let Err(e) = insert.end().await {
        tracing::error!(
            "Failed to finalize insert for table '{}', batch may be lost: {}",
            table_name_str,
            e
        );
        return;
    }
}
