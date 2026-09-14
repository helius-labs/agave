//! Translation of shared-memory transaction references received from an external scheduler
//! into runtime transactions. Shared by the external check, execution, and simulation workers.

use {
    crate::banking_stage::{
        scheduler_messages::MaxAge,
        transaction_scheduler::receive_and_buffer::{
            PacketHandlingError, translate_to_runtime_view,
        },
    },
    agave_scheduler_bindings::{
        MAX_TRANSACTIONS_PER_MESSAGE, worker_message_types::not_included_reasons,
    },
    agave_scheduling_utils::transaction_ptr::{TransactionPtr, TransactionPtrBatch},
    agave_transaction_view::{
        resolved_transaction_view::ResolvedTransactionView, sanitize::SanitizeConfig,
    },
    arrayvec::ArrayVec,
    solana_runtime::bank::Bank,
    solana_runtime_transaction::{
        runtime_transaction::RuntimeTransaction, sanitize_config::sanitize_config,
    },
};

/// A transaction translated from an external scheduler's shared memory.
pub(crate) type ExternalTransaction = RuntimeTransaction<ResolvedTransactionView<TransactionPtr>>;

/// Translate a batch of shared-memory transaction references into runtime transactions.
///
/// Returns one translation result per transaction in the batch, and the successfully
/// translated transactions with their maximum ages, in batch order. The two latter
/// collections only contain entries for transactions whose result is `Ok`.
pub(crate) fn translate_transaction_batch(
    batch: &TransactionPtrBatch,
    bank: &Bank,
) -> (
    ArrayVec<Result<(), PacketHandlingError>, MAX_TRANSACTIONS_PER_MESSAGE>,
    ArrayVec<ExternalTransaction, MAX_TRANSACTIONS_PER_MESSAGE>,
    ArrayVec<MaxAge, MAX_TRANSACTIONS_PER_MESSAGE>,
) {
    let sanitize_config = sanitize_config();
    let transaction_account_lock_limit = bank.get_transaction_account_lock_limit();

    let mut translation_results = ArrayVec::new();
    let mut transactions = ArrayVec::new();
    let mut max_ages = ArrayVec::new();
    for (transaction_ptr, _) in batch.iter() {
        match translate_transaction(
            transaction_ptr,
            bank,
            transaction_account_lock_limit,
            &sanitize_config,
        ) {
            Ok((transaction, max_age)) => {
                transactions.push(transaction);
                max_ages.push(max_age);
                translation_results.push(Ok(()));
            }
            Err(err) => translation_results.push(Err(err)),
        }
    }

    (translation_results, transactions, max_ages)
}

fn translate_transaction(
    transaction_ptr: TransactionPtr,
    bank: &Bank,
    transaction_account_lock_limit: usize,
    sanitize_config: &SanitizeConfig,
) -> Result<(ExternalTransaction, MaxAge), PacketHandlingError> {
    translate_to_runtime_view(
        transaction_ptr,
        bank,
        transaction_account_lock_limit,
        sanitize_config,
    )
    .map(|(view, deactivation_slot)| {
        (
            view,
            MaxAge {
                sanitized_epoch: bank.epoch(),
                alt_invalidation_slot: deactivation_slot,
            },
        )
    })
}

/// Map a translation failure to the reason reported to the external scheduler.
pub(crate) fn reason_from_packet_handling_error(err: &PacketHandlingError) -> u8 {
    match err {
        PacketHandlingError::ALTResolution => not_included_reasons::ADDRESS_LOOKUP_TABLE_NOT_FOUND,
        _ => not_included_reasons::SANITIZE_FAILURE,
    }
}
