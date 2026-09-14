//! External scheduler worker that simulates ordered transaction bundles without committing
//! state. Requests arrive over a shared MPMC queue and every worker in the pool is
//! interchangeable.

use {
    super::{
        bundle_simulation::{SimulationOutcome, simulate_bundle},
        consume_worker::active_leader_state,
        transaction_scheduler::external_translation::translate_transaction_batch,
    },
    agave_scheduler_bindings::{
        MAX_TRANSACTIONS_PER_MESSAGE, PackToSimulationWorkerMessage, SimulationResponseRegion,
        SimulationWorkerToPackMessage, processed_codes, simulation_message_flags,
        worker_message_types::{SimulationResponse, not_included_reasons},
    },
    agave_scheduling_utils::{
        responses_region::simulation_responses_from_iter, transaction_ptr::TransactionPtrBatch,
    },
    solana_measure::measure_us,
    solana_metrics::datapoint_info,
    solana_poh::poh_recorder::SharedLeaderState,
    solana_runtime::{
        bank::Bank,
        bank_forks::{BankPair, SharableBanks},
    },
    solana_time_utils::AtomicInterval,
    std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    },
    thiserror::Error,
};

#[derive(Debug, Error)]
pub(crate) enum ExternalSimulationWorkerError {
    #[error("Sender disconnected")]
    SenderDisconnected,
    #[error("Allocation failed")]
    AllocationFailure,
}

pub(crate) enum IterationResult {
    ProcessedMessage,
    Idle,
}

pub(crate) struct ExternalSimulationWorker {
    exit: Arc<AtomicBool>,
    receiver: shaq::mpmc::Consumer<PackToSimulationWorkerMessage>,
    sender: shaq::mpmc::Producer<SimulationWorkerToPackMessage>,
    allocator: rts_alloc::Allocator,

    shared_leader_state: SharedLeaderState,
    sharable_banks: SharableBanks,
    metrics: SimulationWorkerMetrics,
}

impl ExternalSimulationWorker {
    const RECEIVE_TIMEOUT: Duration = Duration::from_millis(10);

    pub fn new(
        id: u32,
        exit: Arc<AtomicBool>,
        receiver: shaq::mpmc::Consumer<PackToSimulationWorkerMessage>,
        sender: shaq::mpmc::Producer<SimulationWorkerToPackMessage>,
        allocator: rts_alloc::Allocator,
        shared_leader_state: SharedLeaderState,
        sharable_banks: SharableBanks,
    ) -> Self {
        Self {
            exit,
            receiver,
            sender,
            allocator,
            shared_leader_state,
            sharable_banks,
            metrics: SimulationWorkerMetrics::new(id),
        }
    }

    pub fn run(mut self) -> Result<(), ExternalSimulationWorkerError> {
        while !self.exit.load(Ordering::Relaxed) {
            self.iterate(Self::RECEIVE_TIMEOUT)?;
            self.metrics.maybe_report_and_reset();
        }

        Ok(())
    }

    pub(crate) fn iterate(
        &mut self,
        timeout: Duration,
    ) -> Result<IterationResult, ExternalSimulationWorkerError> {
        self.allocator.clean_remote_frees();

        match self.receiver.read_timeout(timeout) {
            Ok(message) => {
                self.process_message(&message)?;
                Ok(IterationResult::ProcessedMessage)
            }
            Err(shaq::error::WaitError::Timeout) => Ok(IterationResult::Idle),
        }
    }

    fn process_message(
        &mut self,
        message: &PackToSimulationWorkerMessage,
    ) -> Result<(), ExternalSimulationWorkerError> {
        self.metrics.num_messages = self.metrics.num_messages.wrapping_add(1);

        if !Self::validate_message(message) {
            self.metrics.num_invalid = self.metrics.num_invalid.wrapping_add(1);
            return self.return_unprocessed_message(message, processed_codes::INVALID);
        }

        let bank = self.select_bank();
        if bank.slot() > message.max_working_slot {
            self.metrics.num_max_working_slot_exceeded =
                self.metrics.num_max_working_slot_exceeded.wrapping_add(1);
            return self
                .return_unprocessed_message(message, processed_codes::MAX_WORKING_SLOT_EXCEEDED);
        }

        // SAFETY: Assumption that external scheduler does not pass messages with batch regions
        //         not pointing to valid regions in the allocator.
        let batch = unsafe {
            TransactionPtrBatch::from_sharable_transaction_batch_region(
                &message.batch,
                &self.allocator,
            )
        };
        let ((translation_results, transactions, _max_ages), translate_us) =
            measure_us!(translate_transaction_batch(&batch, &bank));
        let (outcomes, simulate_us) =
            measure_us!(simulate_bundle(&bank, &translation_results, &transactions));
        self.metrics
            .record_bundle(&outcomes, translate_us, simulate_us);

        let responses = simulation_responses_from_iter(
            &self.allocator,
            outcomes.iter().map(Self::response_from_outcome),
        )
        .ok_or(ExternalSimulationWorkerError::AllocationFailure)?;

        self.sender
            .try_write(SimulationWorkerToPackMessage {
                batch: message.batch,
                processed_code: processed_codes::PROCESSED,
                responses,
            })
            .map_err(|_| ExternalSimulationWorkerError::SenderDisconnected)
    }

    /// Prefer the leader bank when the node is leader, otherwise simulate against the
    /// highest working bank.
    fn select_bank(&self) -> Arc<Bank> {
        let BankPair {
            root_bank: _,
            working_bank,
        } = self.sharable_banks.load();
        active_leader_state(&self.shared_leader_state)
            .and_then(|leader_state| leader_state.working_bank().cloned())
            .unwrap_or(working_bank)
    }

    fn response_from_outcome(outcome: &SimulationOutcome) -> SimulationResponse {
        SimulationResponse {
            simulation_slot: outcome.slot,
            not_included_reason: outcome.not_included_reason,
            cost_units: outcome.cost_units,
            fee_payer_balance: outcome.fee_payer_balance,
        }
    }

    fn return_unprocessed_message(
        &mut self,
        message: &PackToSimulationWorkerMessage,
        processed_code: u8,
    ) -> Result<(), ExternalSimulationWorkerError> {
        assert_ne!(processed_code, processed_codes::PROCESSED);

        self.sender
            .try_write(SimulationWorkerToPackMessage {
                batch: message.batch,
                processed_code,
                responses: SimulationResponseRegion {
                    num_transaction_responses: 0,
                    transaction_responses_offset: 0,
                },
            })
            .map_err(|_| ExternalSimulationWorkerError::SenderDisconnected)
    }

    /// Returns `true` if a message is valid and can be processed.
    fn validate_message(message: &PackToSimulationWorkerMessage) -> bool {
        message.batch.num_transactions > 0
            && usize::from(message.batch.num_transactions) <= MAX_TRANSACTIONS_PER_MESSAGE
            && message.flags == simulation_message_flags::NONE
    }
}

/// Per-worker counters, reported and reset on an interval by the worker thread itself.
struct SimulationWorkerMetrics {
    id: u32,
    interval: AtomicInterval,

    num_messages: u64,
    num_invalid: u64,
    num_max_working_slot_exceeded: u64,
    num_bundles_succeeded: u64,
    num_bundles_failed: u64,
    num_bundles_bank_unavailable: u64,
    num_transactions: u64,
    translate_us: u64,
    simulate_us: u64,
}

impl SimulationWorkerMetrics {
    const REPORT_INTERVAL_MS: u64 = 1000;

    fn new(id: u32) -> Self {
        Self {
            id,
            interval: AtomicInterval::default(),
            num_messages: 0,
            num_invalid: 0,
            num_max_working_slot_exceeded: 0,
            num_bundles_succeeded: 0,
            num_bundles_failed: 0,
            num_bundles_bank_unavailable: 0,
            num_transactions: 0,
            translate_us: 0,
            simulate_us: 0,
        }
    }

    fn record_bundle(
        &mut self,
        outcomes: &[SimulationOutcome],
        translate_us: u64,
        simulate_us: u64,
    ) {
        self.num_transactions = self.num_transactions.wrapping_add(outcomes.len() as u64);
        self.translate_us = self.translate_us.wrapping_add(translate_us);
        self.simulate_us = self.simulate_us.wrapping_add(simulate_us);

        let bank_unavailable = outcomes
            .iter()
            .any(|outcome| outcome.not_included_reason == not_included_reasons::BANK_NOT_AVAILABLE);
        let succeeded = outcomes
            .iter()
            .all(|outcome| outcome.not_included_reason == not_included_reasons::NONE);
        if bank_unavailable {
            self.num_bundles_bank_unavailable = self.num_bundles_bank_unavailable.wrapping_add(1);
        } else if succeeded {
            self.num_bundles_succeeded = self.num_bundles_succeeded.wrapping_add(1);
        } else {
            self.num_bundles_failed = self.num_bundles_failed.wrapping_add(1);
        }
    }

    fn maybe_report_and_reset(&mut self) {
        if !self.interval.should_update(Self::REPORT_INTERVAL_MS) {
            return;
        }
        if self.num_messages == 0 {
            return;
        }

        datapoint_info!(
            "banking_stage_simulation_worker",
            ("id", self.id, i64),
            ("num_messages", self.num_messages, i64),
            ("num_invalid", self.num_invalid, i64),
            (
                "num_max_working_slot_exceeded",
                self.num_max_working_slot_exceeded,
                i64
            ),
            ("num_bundles_succeeded", self.num_bundles_succeeded, i64),
            ("num_bundles_failed", self.num_bundles_failed, i64),
            (
                "num_bundles_bank_unavailable",
                self.num_bundles_bank_unavailable,
                i64
            ),
            ("num_transactions", self.num_transactions, i64),
            ("translate_us", self.translate_us, i64),
            ("simulate_us", self.simulate_us, i64),
        );

        *self = Self::new(self.id);
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::banking_stage::tests::create_slow_genesis_config,
        agave_scheduler_bindings::{SharableTransactionBatchRegion, SharableTransactionRegion},
        agave_scheduler_handshake::{ClientLogon, client, server::Server},
        agave_scheduling_utils::responses_region::SimulationResponsesPtr,
        solana_account::AccountSharedData,
        solana_keypair::Keypair,
        solana_leader_schedule::SlotLeader,
        solana_ledger::genesis_utils::GenesisConfigInfo,
        solana_poh::poh_recorder::LeaderState,
        solana_pubkey::Pubkey,
        solana_runtime::{bank::Bank, bank_forks::BankForks},
        solana_sdk_ids::system_program,
        solana_signer::Signer,
        solana_system_transaction::transfer,
        std::sync::RwLock,
    };

    /// Enough lamports for several rent-exempt transfers to brand new accounts.
    const PAYER_LAMPORTS: u64 = 10_000_000_000;
    /// Rent-exempt transfer amount for a new account.
    const TRANSFER_LAMPORTS: u64 = 1_000_000_000;

    struct SharedBatch {
        region: SharableTransactionBatchRegion,
        transactions: Vec<SharableTransactionRegion>,
    }

    struct SimulationWorkerTestFrame {
        bank: Arc<Bank>,
        _bank_forks: Arc<RwLock<BankForks>>,
        allocator: rts_alloc::Allocator,
        pack_to_simulation_worker: shaq::mpmc::Producer<PackToSimulationWorkerMessage>,
        simulation_worker_to_pack: shaq::mpmc::Consumer<SimulationWorkerToPackMessage>,
        shared_leader_state: SharedLeaderState,
        worker: ExternalSimulationWorker,
    }

    impl SimulationWorkerTestFrame {
        fn send_message(&self, message: PackToSimulationWorkerMessage) {
            self.pack_to_simulation_worker.try_write(message).unwrap();
        }

        fn send_bundle(&self, batch: &SharedBatch) {
            self.send_message(PackToSimulationWorkerMessage {
                flags: simulation_message_flags::NONE,
                max_working_slot: u64::MAX,
                batch: batch.region,
            });
        }

        fn iterate(&mut self) -> Result<(), ExternalSimulationWorkerError> {
            let result = self.worker.iterate(Duration::ZERO)?;
            assert!(matches!(result, IterationResult::ProcessedMessage));
            Ok(())
        }

        fn iterate_idle(&mut self) -> Result<(), ExternalSimulationWorkerError> {
            let result = self.worker.iterate(Duration::ZERO)?;
            assert!(matches!(result, IterationResult::Idle));
            Ok(())
        }

        fn recv_response(&self) -> SimulationWorkerToPackMessage {
            self.simulation_worker_to_pack
                .read_timeout(Duration::from_secs(1))
                .unwrap()
        }

        fn simulation_responses(
            &self,
            region: &SimulationResponseRegion,
        ) -> Vec<SimulationResponse> {
            unsafe {
                // SAFETY: `region` was produced by this worker using the same shared allocator,
                // and the pointed-to allocation contains `SimulationResponse` values.
                let responses = SimulationResponsesPtr::from_transaction_response_region(
                    region,
                    &self.allocator,
                );
                let decoded = responses.iter().copied().collect();
                responses.free(&self.allocator);
                decoded
            }
        }

        fn fund(&self, lamports: u64) -> Keypair {
            let keypair = Keypair::new();
            self.bank.store_account(
                &keypair.pubkey(),
                &AccountSharedData::new(lamports, 0, &system_program::ID),
            );
            keypair
        }

        fn serialized_transfer(&self, from: &Keypair, to: &Pubkey, lamports: u64) -> Vec<u8> {
            wincode::serialize(&transfer(from, to, lamports, self.bank.last_blockhash())).unwrap()
        }

        fn allocate_batch(&self, transactions: &[Vec<u8>]) -> SharedBatch {
            type Batch<'a> = TransactionPtrBatch<'a>;
            assert!(transactions.len() <= MAX_TRANSACTIONS_PER_MESSAGE);

            let batch_ptr = self
                .allocator
                .allocate(Batch::TRANSACTION_META_END as u32)
                .unwrap();
            // SAFETY: `batch_ptr` came from this allocator immediately above.
            let batch_offset = unsafe { self.allocator.offset(batch_ptr) };
            let tx_ptr = batch_ptr.cast::<SharableTransactionRegion>();

            let mut sharable_transactions = Vec::with_capacity(transactions.len());
            for (index, transaction) in transactions.iter().enumerate() {
                let tx_allocation = self
                    .allocator
                    .allocate(transaction.len().try_into().unwrap())
                    .unwrap();
                unsafe {
                    // SAFETY: fresh allocation of exactly `transaction.len()` bytes; regions do
                    // not overlap.
                    std::ptr::copy_nonoverlapping(
                        transaction.as_ptr(),
                        tx_allocation.as_ptr(),
                        transaction.len(),
                    );
                }
                let tx_region = SharableTransactionRegion {
                    // SAFETY: `tx_allocation` came from this allocator immediately above.
                    offset: unsafe { self.allocator.offset(tx_allocation) },
                    length: transaction.len().try_into().unwrap(),
                };
                unsafe {
                    // SAFETY: the batch allocation has room for `MAX_TRANSACTIONS_PER_MESSAGE`
                    // headers and the assert above keeps `index` in bounds.
                    tx_ptr.add(index).write(tx_region)
                };
                sharable_transactions.push(tx_region);
            }

            SharedBatch {
                region: SharableTransactionBatchRegion {
                    num_transactions: transactions.len().try_into().unwrap(),
                    transactions_offset: batch_offset,
                },
                transactions: sharable_transactions,
            }
        }

        fn free_batch(&self, batch: SharedBatch) {
            for tx in batch.transactions {
                unsafe {
                    // SAFETY: allocated by this allocator in `allocate_batch`, owned once.
                    self.allocator
                        .free(self.allocator.ptr_from_offset(tx.offset));
                }
            }
            unsafe {
                // SAFETY: the batch container allocation created by `allocate_batch`.
                self.allocator.free(
                    self.allocator
                        .ptr_from_offset(batch.region.transactions_offset),
                );
            }
        }
    }

    fn setup_test_frame() -> SimulationWorkerTestFrame {
        let GenesisConfigInfo { genesis_config, .. } = create_slow_genesis_config(10_000);
        let (root_bank, _root_bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
        let child_bank = Bank::new_from_parent(root_bank, SlotLeader::new_unique(), 1);
        let (bank, bank_forks) = child_bank.wrap_with_bank_forks_for_tests();

        let logon = ClientLogon {
            worker_count: 1,
            check_worker_count: 1,
            allocator_size: 64 * 1024 * 1024,
            allocator_handles: 1,
            tpu_to_pack_capacity: 16,
            progress_tracker_capacity: 16,
            pack_to_worker_capacity: 16,
            worker_to_pack_capacity: 16,
            flags: 0,
            pack_to_check_worker_capacity: 16,
            check_worker_to_pack_capacity: 16,
            simulation_worker_count: 1,
            pack_to_simulation_worker_capacity: 16,
            simulation_worker_to_pack_capacity: 16,
        };
        let (_agave_session, files) = Server::setup_session(logon).unwrap();
        let mut client_session = client::setup_session(&logon, files).unwrap();
        let allocator = client_session.allocators.pop().unwrap();

        let (pack_to_simulation_worker, receiver) = shaq::mpmc::pair(16).unwrap();
        let (sender, simulation_worker_to_pack) = shaq::mpmc::pair(16).unwrap();
        let worker_allocator = rts_alloc::Allocator::join_from_existing(&allocator)
            .expect("join allocator from test allocator");
        let shared_leader_state = SharedLeaderState::new(0, None, None);
        let worker = ExternalSimulationWorker::new(
            0,
            Arc::new(AtomicBool::new(false)),
            receiver,
            sender,
            worker_allocator,
            shared_leader_state.clone(),
            bank_forks.read().unwrap().sharable_banks(),
        );

        SimulationWorkerTestFrame {
            bank,
            _bank_forks: bank_forks,
            allocator,
            pack_to_simulation_worker,
            simulation_worker_to_pack,
            shared_leader_state,
            worker,
        }
    }

    #[test]
    fn test_idle_timeout() {
        let mut test_frame = setup_test_frame();
        test_frame.iterate_idle().unwrap();
    }

    #[test]
    fn test_invalid_message() {
        let mut test_frame = setup_test_frame();

        // Empty batch.
        test_frame.send_message(PackToSimulationWorkerMessage {
            flags: simulation_message_flags::NONE,
            max_working_slot: u64::MAX,
            batch: SharableTransactionBatchRegion {
                num_transactions: 0,
                transactions_offset: 0,
            },
        });
        test_frame.iterate().unwrap();
        let response = test_frame.recv_response();
        assert_eq!(response.processed_code, processed_codes::INVALID);
        assert_eq!(response.responses.num_transaction_responses, 0);

        // Unknown flags.
        let payer = test_frame.fund(PAYER_LAMPORTS);
        let batch = test_frame.allocate_batch(&[test_frame.serialized_transfer(
            &payer,
            &Pubkey::new_unique(),
            TRANSFER_LAMPORTS,
        )]);
        test_frame.send_message(PackToSimulationWorkerMessage {
            flags: 1,
            max_working_slot: u64::MAX,
            batch: batch.region,
        });
        test_frame.iterate().unwrap();
        let response = test_frame.recv_response();
        assert_eq!(response.processed_code, processed_codes::INVALID);
        assert_eq!(response.responses.num_transaction_responses, 0);
        assert_eq!(response.batch, batch.region);

        test_frame.free_batch(batch);
    }

    #[test]
    fn test_max_working_slot_exceeded() {
        let mut test_frame = setup_test_frame();
        let payer = test_frame.fund(PAYER_LAMPORTS);
        let batch = test_frame.allocate_batch(&[test_frame.serialized_transfer(
            &payer,
            &Pubkey::new_unique(),
            TRANSFER_LAMPORTS,
        )]);

        test_frame.send_message(PackToSimulationWorkerMessage {
            flags: simulation_message_flags::NONE,
            max_working_slot: test_frame.bank.slot() - 1,
            batch: batch.region,
        });
        test_frame.iterate().unwrap();
        let response = test_frame.recv_response();
        assert_eq!(
            response.processed_code,
            processed_codes::MAX_WORKING_SLOT_EXCEEDED
        );
        assert_eq!(response.responses.num_transaction_responses, 0);

        test_frame.free_batch(batch);
    }

    #[test]
    fn test_simulates_dependent_bundle_without_committing() {
        let mut test_frame = setup_test_frame();
        let fee = test_frame.bank.fee_structure().lamports_per_signature;
        let payer = test_frame.fund(PAYER_LAMPORTS);
        let intermediate = Keypair::new();
        let destination = Pubkey::new_unique();

        // The second transfer is only fundable with the state produced by the first.
        let batch = test_frame.allocate_batch(&[
            test_frame.serialized_transfer(&payer, &intermediate.pubkey(), 3 * TRANSFER_LAMPORTS),
            test_frame.serialized_transfer(&intermediate, &destination, TRANSFER_LAMPORTS),
        ]);
        test_frame.send_bundle(&batch);
        test_frame.iterate().unwrap();

        let response = test_frame.recv_response();
        assert_eq!(response.processed_code, processed_codes::PROCESSED);
        assert_eq!(response.batch, batch.region);
        let responses = test_frame.simulation_responses(&response.responses);
        assert_eq!(responses.len(), 2);
        for response in &responses {
            assert_eq!(response.simulation_slot, test_frame.bank.slot());
            assert_eq!(response.not_included_reason, not_included_reasons::NONE);
            assert!(response.cost_units > 0);
        }
        assert_eq!(
            responses[0].fee_payer_balance,
            PAYER_LAMPORTS - 3 * TRANSFER_LAMPORTS - fee
        );
        assert_eq!(
            responses[1].fee_payer_balance,
            3 * TRANSFER_LAMPORTS - TRANSFER_LAMPORTS - fee
        );

        // Nothing was committed.
        assert_eq!(test_frame.bank.get_balance(&payer.pubkey()), PAYER_LAMPORTS);
        assert_eq!(test_frame.bank.get_balance(&intermediate.pubkey()), 0);
        assert_eq!(test_frame.bank.get_balance(&destination), 0);

        test_frame.free_batch(batch);
    }

    #[test]
    fn test_failure_fails_entire_bundle() {
        let mut test_frame = setup_test_frame();
        let payer = test_frame.fund(PAYER_LAMPORTS);
        let unfunded = Keypair::new();

        let batch = test_frame.allocate_batch(&[
            test_frame.serialized_transfer(&payer, &Pubkey::new_unique(), TRANSFER_LAMPORTS),
            test_frame.serialized_transfer(&unfunded, &Pubkey::new_unique(), TRANSFER_LAMPORTS),
            test_frame.serialized_transfer(&payer, &Pubkey::new_unique(), TRANSFER_LAMPORTS),
        ]);
        test_frame.send_bundle(&batch);
        test_frame.iterate().unwrap();

        let response = test_frame.recv_response();
        assert_eq!(response.processed_code, processed_codes::PROCESSED);
        let responses = test_frame.simulation_responses(&response.responses);
        assert_eq!(
            responses
                .iter()
                .map(|response| response.not_included_reason)
                .collect::<Vec<_>>(),
            vec![
                not_included_reasons::ALL_OR_NOTHING_BATCH_FAILURE,
                not_included_reasons::ACCOUNT_NOT_FOUND,
                not_included_reasons::ALL_OR_NOTHING_BATCH_FAILURE,
            ]
        );
        assert!(
            responses
                .iter()
                .all(|response| response.cost_units == 0 && response.fee_payer_balance == 0)
        );
        assert_eq!(test_frame.bank.get_balance(&payer.pubkey()), PAYER_LAMPORTS);

        test_frame.free_batch(batch);
    }

    #[test]
    fn test_translation_failure_rejects_bundle_without_executing() {
        let mut test_frame = setup_test_frame();
        let payer = test_frame.fund(PAYER_LAMPORTS);

        let batch = test_frame.allocate_batch(&[
            test_frame.serialized_transfer(&payer, &Pubkey::new_unique(), TRANSFER_LAMPORTS),
            vec![0xff; 32],
        ]);
        test_frame.send_bundle(&batch);
        test_frame.iterate().unwrap();

        let response = test_frame.recv_response();
        assert_eq!(response.processed_code, processed_codes::PROCESSED);
        let responses = test_frame.simulation_responses(&response.responses);
        assert_eq!(
            responses
                .iter()
                .map(|response| response.not_included_reason)
                .collect::<Vec<_>>(),
            vec![
                not_included_reasons::ALL_OR_NOTHING_BATCH_FAILURE,
                not_included_reasons::SANITIZE_FAILURE,
            ]
        );

        test_frame.free_batch(batch);
    }

    #[test]
    fn test_prefers_active_leader_bank() {
        let mut test_frame = setup_test_frame();
        // Fund before deriving the leader bank: `new_from_parent` freezes the parent.
        let payer = test_frame.fund(PAYER_LAMPORTS);
        let leader_bank = Arc::new(Bank::new_from_parent(
            test_frame.bank.clone(),
            SlotLeader::new_unique(),
            test_frame.bank.slot() + 1,
        ));
        test_frame
            .shared_leader_state
            .store(Arc::new(LeaderState::new(
                Some(leader_bank.clone()),
                leader_bank.tick_height(),
                None,
                None,
            )));

        let batch = test_frame.allocate_batch(&[test_frame.serialized_transfer(
            &payer,
            &Pubkey::new_unique(),
            TRANSFER_LAMPORTS,
        )]);

        // The leader bank is ahead of the working bank and must be gated by `max_working_slot`.
        test_frame.send_message(PackToSimulationWorkerMessage {
            flags: simulation_message_flags::NONE,
            max_working_slot: test_frame.bank.slot(),
            batch: batch.region,
        });
        test_frame.iterate().unwrap();
        let response = test_frame.recv_response();
        assert_eq!(
            response.processed_code,
            processed_codes::MAX_WORKING_SLOT_EXCEEDED
        );

        test_frame.send_bundle(&batch);
        test_frame.iterate().unwrap();
        let response = test_frame.recv_response();
        assert_eq!(response.processed_code, processed_codes::PROCESSED);
        let responses = test_frame.simulation_responses(&response.responses);
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].simulation_slot, leader_bank.slot());
        assert_eq!(responses[0].not_included_reason, not_included_reasons::NONE);

        test_frame.free_batch(batch);
    }
}
