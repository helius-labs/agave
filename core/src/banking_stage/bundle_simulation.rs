//! Protocol-neutral simulation of ordered transaction bundles for external schedulers.
//!
//! The wire format spoken to the external scheduler lives in the simulation worker; this
//! module only decides which bank state a bundle is simulated against and how the runtime's
//! results are reported.

use {
    super::transaction_scheduler::{
        external_translation::reason_from_packet_handling_error,
        receive_and_buffer::PacketHandlingError,
    },
    agave_scheduler_bindings::{
        MAX_TRANSACTIONS_PER_MESSAGE, worker_message_types::not_included_reasons,
    },
    agave_scheduling_utils::error::transaction_error_to_not_included_reason,
    arrayvec::ArrayVec,
    solana_clock::Slot,
    solana_cost_model::cost_model::CostModel,
    solana_runtime::bank::Bank,
    solana_runtime_transaction::transaction_with_meta::TransactionWithMeta,
};

/// Outcome of simulating one transaction as part of a bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SimulationOutcome {
    /// Slot of the bank the bundle was simulated against.
    pub slot: Slot,
    /// [`not_included_reasons::NONE`] if the transaction simulated successfully.
    pub not_included_reason: u8,
    /// Cost units of the simulated execution; zero on failure.
    pub cost_units: u64,
    /// Fee-payer balance after simulated execution; zero on failure.
    pub fee_payer_balance: u64,
}

impl SimulationOutcome {
    const fn failed(slot: Slot, not_included_reason: u8) -> Self {
        Self {
            slot,
            not_included_reason,
            cost_units: 0,
            fee_payer_balance: 0,
        }
    }
}

/// Simulate an ordered bundle atomically against `bank` without committing any state.
///
/// `translation_results` holds one entry per transaction of the bundle as received from
/// the external scheduler, and `transactions` holds the successfully translated
/// transactions in the same order. If any transaction failed translation the bundle is
/// rejected without executing anything: the failing transactions report their translation
/// error and the rest report [`not_included_reasons::ALL_OR_NOTHING_BATCH_FAILURE`].
///
/// If the bank is being retired every transaction reports
/// [`not_included_reasons::BANK_NOT_AVAILABLE`] so the scheduler can retry.
pub(crate) fn simulate_bundle<Tx: TransactionWithMeta>(
    bank: &Bank,
    translation_results: &[Result<(), PacketHandlingError>],
    transactions: &[Tx],
) -> ArrayVec<SimulationOutcome, MAX_TRANSACTIONS_PER_MESSAGE> {
    let slot = bank.slot();

    if transactions.len() != translation_results.len() {
        return translation_results
            .iter()
            .map(|result| {
                SimulationOutcome::failed(
                    slot,
                    match result {
                        Ok(()) => not_included_reasons::ALL_OR_NOTHING_BATCH_FAILURE,
                        Err(err) => reason_from_packet_handling_error(err),
                    },
                )
            })
            .collect();
    }

    let Some(results) = bank.simulate_transaction_batch_unchecked(transactions) else {
        return transactions
            .iter()
            .map(|_| SimulationOutcome::failed(slot, not_included_reasons::BANK_NOT_AVAILABLE))
            .collect();
    };

    transactions
        .iter()
        .zip(results)
        .map(|(transaction, result)| match result.result {
            Ok(()) => SimulationOutcome {
                slot,
                not_included_reason: not_included_reasons::NONE,
                cost_units: CostModel::calculate_cost_for_executed_transaction(
                    transaction,
                    result.units_consumed,
                    result.loaded_accounts_data_size,
                    &bank.feature_set,
                )
                .sum(),
                fee_payer_balance: result.fee_payer_post_balance.unwrap_or(0),
            },
            // The batch is simulated all-or-nothing, so a cancelled transaction maps to
            // `ALL_OR_NOTHING_BATCH_FAILURE`.
            Err(err) => SimulationOutcome::failed(
                slot,
                transaction_error_to_not_included_reason(&err, true),
            ),
        })
        .collect()
}
