#![no_std]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! Messages passed between agave and an external pack process.
//! Messages are passed via `shaq` SPSC and MPMC queues.
//!
//! Memory freeing is responsibility of the external pack process,
//! and is done via `rts-alloc` crate. It is also possible the external
//! pack process allocates memory to pass to agave, BUT it will still be
//! the responsibility of the external pack process to free that memory.
//!
//! Setting up the shared memory allocator and queues is done outside of
//! agave - it can be done by the external pack process or another
//! process. agave will just `join` shared memory regions, but not
//! create them.
//! Similarly, agave will not delete files used for shared memory regions.
//! See `shaq` and `rts-alloc` crates for details.
//!
//! The basic architecture is as follows:
//!        ┌───────────────┐       ┌─────────────────┐
//!        │  tpu_to_pack  │       │ progress_tracker│
//!        └───────┬───────┘       └───────┬─────────┘
//!                │                       │
//!                │                       │
//!                │                       │
//!             ┌──▼───────────────────────▼───┐
//!             │  external scheduler          │
//!             └─▲─────── ▲────────────────▲──┘
//!               │        │                │
//!               │        │                │
//!           ┌───▼───┐ ┌──▼─────┐ ...  ┌───▼───┐
//!           │worker1│ │worker2 │      │workerN│
//!           └───────┘ └────────┘      └───────┘
//!
//! - [`TpuToPackMessage`] are sent from `tpu_to_pack` queue to the
//!   external scheduler process. This passes in tpu transactions to be scheduled,
//!   and optionally vote transactions.
//! - [`ProgressMessage`] are sent from `progress_tracker` queue to the
//!   external scheduler process. This passes information about leader status
//!   and slot progress to the external scheduler process.
//! - [`PackToExecutionWorkerMessage`] are sent from the external scheduler process
//!   to execution worker threads within agave. This passes a batch of
//!   transactions to be executed by the worker threads.
//! - [`ExecutionWorkerToPackMessage`] are sent from execution worker threads
//!   within agave back to the external scheduler process. This passes back the
//!   results of executing the transactions.
//! - [`PackToCheckWorkerMessage`] are sent from the external scheduler process to
//!   check worker threads within agave. This passes a batch of transactions to be
//!   checked by the worker threads.
//! - [`CheckWorkerToPackMessage`] are sent from check worker threads within agave
//!   back to the external scheduler process. This passes back the results of
//!   checking the transactions.
//!
//! Ownership and pointer lifetime rule:
//! - Sending a message that contains offsets or pointers to memory transfers
//!   ownership of that memory to the receiver for the lifetime defined by the
//!   protocol.
//! - The sender must treat that memory as not owned until ownership is
//!   explicitly returned by a later message, if any. In particular, the sender
//!   must not free or modify that memory while it is not owned.
//! - If the protocol does not return the memory to the sender, the receiver is
//!   responsible for freeing it.
//!

/// Returns the major version of this crate.
pub fn version() -> u64 {
    env!("CARGO_PKG_VERSION_MAJOR")
        .parse()
        .expect("crate major version must be a u64")
}

/// Reference to a transaction that can shared safely across processes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct SharableTransactionRegion {
    /// Offset within the shared memory allocator.
    pub offset: usize,
    /// Length of the transaction in bytes.
    pub length: u32,
}

/// Reference to an array of Pubkeys that can be shared safely across processes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct SharablePubkeys {
    /// Offset within the shared memory allocator.
    pub offset: usize,
    /// Number of pubkeys in the array.
    /// IF 0, indicates no pubkeys and no allocation needing to be freed.
    pub num_pubkeys: u32,
}

/// Reference to an array of [`SharableTransactionRegion`] that can be shared safely
/// across processes.
/// General flow:
/// 1. External pack process allocates memory for
///    `num_transactions` [`SharableTransactionRegion`].
/// 2. External pack sends a [`PackToExecutionWorkerMessage`] with `batch`.
/// 3. agave processes the transactions and sends back an [`ExecutionWorkerToPackMessage`]
///    with the same `batch`.
/// 4. External pack process frees all transaction memory pointed to by the
///    [`SharableTransactionRegion`] in the batch, then frees the memory for
///    the array of [`SharableTransactionRegion`].
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[repr(C)]
pub struct SharableTransactionBatchRegion {
    /// Number of transactions in the batch.
    pub num_transactions: u8,
    /// Offset within the shared memory allocator for the batch of transactions.
    /// The transactions are laid out back-to-back in memory as a
    /// [`SharableTransactionRegion`] with size `num_transactions`.
    pub transactions_offset: usize,
}
/// Reference to an array of [`worker_message_types::ExecutionResponse`] messages.
/// General flow:
/// 1. agave allocates memory for `num_transaction_responses` execution responses.
/// 2. agave sends an [`ExecutionWorkerToPackMessage`] with `responses`.
/// 3. External pack process processes the responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct ExecutionResponseRegion {
    /// The number of transactions in the original message.
    /// This corresponds to the number of responses pointed to by
    /// `transaction_responses_offset`.
    pub num_transaction_responses: u8,
    /// Offset within the shared memory allocator for the array of
    /// [`worker_message_types::ExecutionResponse`] messages.
    /// The responses are laid out back-to-back in memory starting at this offset.
    /// There are `num_transaction_responses` responses.
    pub transaction_responses_offset: usize,
}

/// Reference to an array of [`worker_message_types::CheckResponse`] messages.
/// General flow:
/// 1. agave allocates memory for `num_transaction_responses` check responses.
/// 2. agave sends a [`CheckWorkerToPackMessage`] with `responses`.
/// 3. External pack process processes the responses, freeing any memory owned by
///    each response as described by [`worker_message_types::CheckResponse`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct CheckResponseRegion {
    /// The number of transactions in the original message.
    /// This corresponds to the number of responses pointed to by
    /// `transaction_responses_offset`.
    pub num_transaction_responses: u8,
    /// Offset within the shared memory allocator for the array of
    /// [`worker_message_types::CheckResponse`] messages.
    /// The responses are laid out back-to-back in memory starting at this offset.
    /// There are `num_transaction_responses` responses.
    pub transaction_responses_offset: usize,
}

/// Message: [TPU -> Pack]
/// TPU passes transactions to the external pack process.
/// This is also a transfer of ownership of the transaction:
///   the external pack process is responsible for freeing the memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct TpuToPackMessage {
    pub transaction: SharableTransactionRegion,
    /// See [`tpu_message_flags`] for details.
    pub flags: u8,
    /// The source address of the transaction.
    /// IPv6-mapped IPv4 addresses: `::ffff:a.b.c.d`
    /// where a.b.c.d is the IPv4 address.
    /// See <https://datatracker.ietf.org/doc/html/rfc4291#section-2.5.5.2>.
    pub src_addr: [u8; 16],
}

pub mod tpu_message_flags {
    /// No special flags.
    pub const NONE: u8 = 0;

    /// The transaction is a simple vote transaction.
    pub const IS_SIMPLE_VOTE: u8 = 1 << 0;
    /// The transaction was forwarded by a validator node.
    pub const FORWARDED: u8 = 1 << 1;
    /// The transaction was sent from a staked node.
    pub const FROM_STAKED_NODE: u8 = 1 << 2;
}

/// The node is not currently in a leader slot.
pub const NOT_LEADER: u8 = 0;
/// The node is in a leader slot but the working bank is not yet ready.
///
/// Transactions cannot be processed in this state.
pub const LEADER_STARTING: u8 = 1;
/// The node is in a leader slot and has a working bank ready.
///
/// Transactions can be processed in this state.
pub const LEADER_READY: u8 = 2;

/// Message: [Agave -> Pack]
/// Agave passes leader status to the external pack process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct ProgressMessage {
    /// Indicates the current leader status of the node.
    ///
    /// - [`NOT_LEADER`]: Node is not in a leader slot.
    /// - [`LEADER_STARTING`]: Node is in a leader slot but bank is not ready.
    /// - [`LEADER_READY`]: Node is in a leader slot with bank ready for transactions.
    ///
    /// # Usage
    ///
    /// - To check if within a leader slot: `leader_state != NOT_LEADER`.
    /// - To check if transactions can be processed: `leader_state == LEADER_READY`.
    pub leader_state: u8,
    /// Progress through the current slot in percentage.
    pub current_slot_progress: u8,
    /// The current epoch of the working bank.
    /// Only valid if `leader_state == LEADER_READY`, otherwise zeroed.
    pub epoch: u64,
    /// The current slot.
    pub current_slot: u64,
    /// Next known leader slot or u64::MAX if unknown.
    /// This will **not** include the current slot if leader.
    /// Node is leader for contiguous slots in the inclusive range
    /// [[`Self::next_leader_slot`], [`Self::leader_range_end`]].
    pub next_leader_slot: u64,
    /// Next known leader slot range end (inclusive) or u64::MAX if unknown.
    /// Node is leader for contiguous slots in the inclusive range
    /// [[`Self::next_leader_slot`], [`Self::leader_range_end`]].
    pub leader_range_end: u64,
    /// The remaining cost units allowed to be packed in the block.
    /// i.e. block_limit - current_cost_units_used.
    /// Only valid if currently leader, otherwise the value is undefined.
    pub remaining_cost_units: u64,
    /// The remaining account data allocation allowed to be packed in the block, in bytes.
    /// i.e. allocated_accounts_data_size_limit - current_allocated_accounts_data_size.
    /// Only valid if currently leader, otherwise the value is undefined.
    pub remaining_allocated_accounts_data_size: u64,
    /// The latest blockhash of the working bank.
    /// Only valid if `leader_state == LEADER_READY`, otherwise zeroed.
    pub latest_blockhash: [u8; 32],
    /// Target duration, in milliseconds, for filling the current leader bank.
    ///
    /// Only valid when `leader_state == LEADER_READY`; otherwise zeroed.
    pub target_bank_time_ms: u16,
}

/// Maximum number of transactions allowed in a [`PackToExecutionWorkerMessage`].
/// If the number of transactions exceeds this value, agave will
/// not process the message.
//
// The reason for this constraint is because rts-alloc currently only
// supports up to 4096 byte allocations. We must ensure that the
// response regions are able to contain responses for all
// transactions sent. This is a conservative bound.
pub const MAX_TRANSACTIONS_PER_MESSAGE: usize = 64;

/// Message: [Pack -> Execution Worker]
/// External pack process passes transactions to execution worker threads within agave.
///
/// These messages do not transfer ownership of the transactions.
/// The external pack process is still responsible for freeing the memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct PackToExecutionWorkerMessage {
    /// Flags on how to handle this message.
    /// See [`execution_message_flags`] for details.
    pub flags: u16,
    /// Maximum working bank slot that this message will be processed
    /// for. For execution, this will check the leader bank if it exists.
    /// If the working bank is ahead of the slot, the response will use
    /// [`processed_codes::MAX_WORKING_SLOT_EXCEEDED`].
    pub max_working_slot: u64,
    /// Offset and number of transactions in the batch.
    /// See [`SharableTransactionBatchRegion`] for details.
    /// Agave will return this batch in the response message, it is
    /// the responsibility of the external pack process to free the memory
    /// ONLY after receiving the response message.
    pub batch: SharableTransactionBatchRegion,
}

/// Message: [Pack -> Check Worker]
/// External pack process passes transactions to check worker threads within agave.
///
/// These messages do not transfer ownership of the transactions.
/// The external pack process is still responsible for freeing the memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct PackToCheckWorkerMessage {
    /// Flags on how to handle this message.
    /// See [`check_message_flags`] for details.
    pub flags: u16,
    /// Offset and number of transactions in the batch.
    /// See [`SharableTransactionBatchRegion`] for details.
    /// Agave will return this batch in the response message, it is
    /// the responsibility of the external pack process to free the memory
    /// ONLY after receiving the response message.
    pub batch: SharableTransactionBatchRegion,
}

pub mod check_message_flags {
    /// Transactions should check status: if transaction has already been processed
    /// or the nonce is invalid.
    pub const STATUS_CHECKS: u16 = 1 << 0;

    /// Fee-payer balance should be fetched for transactions.
    pub const LOAD_FEE_PAYER_BALANCE: u16 = 1 << 1;

    /// Transactions should have ATL pubkeys resolved and returned.
    pub const LOAD_ADDRESS_LOOKUP_TABLES: u16 = 1 << 2;

    /// Calculate transaction fee details and estimated costs for scheduling.
    pub const CALCULATE_SCHEDULING_DETAILS: u16 = 1 << 3;
}

pub mod execution_message_flags {
    //! Flags for [`crate::PackToExecutionWorkerMessage::flags`].

    /// Should failing transactions within the batch be dropped (no fee charged & not
    /// committed).
    pub const DROP_ON_FAILURE: u16 = 1 << 0;
    /// If any transaction in the batch is not committed then the entire batch should not be
    /// committed.
    ///
    /// # Note
    ///
    /// Without `drop_on_failure` this flag will still allow processed but failing transactions
    /// to be committed. If both flags are set then any failing transaction will cause all
    /// transactions to be aborted.
    pub const ALL_OR_NOTHING: u16 = 1 << 1;
}

pub mod processed_codes {
    /// The message was processed.
    pub const PROCESSED: u8 = 0;
    /// The message was not processed because the message was invalid.
    pub const INVALID: u8 = 1;
    /// The message was not processed because `max_working_slot`
    /// was exceeded.
    pub const MAX_WORKING_SLOT_EXCEEDED: u8 = 2;
}

/// Message: [Execution Worker -> Pack]
/// Message from worker threads in response to a [`PackToExecutionWorkerMessage`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct ExecutionWorkerToPackMessage {
    /// Offset and number of transactions in the batch.
    /// See [`SharableTransactionBatchRegion`] for details.
    /// Once the external pack process receives this message,
    /// it is responsible for freeing the memory for this batch,
    /// and is safe to do so - agave will hold no references to this memory
    /// after sending this message.
    pub batch: SharableTransactionBatchRegion,
    /// See [`processed_codes`] for accepted values.
    pub processed_code: u8,
    /// Response per transaction in the batch.
    /// When `processed_code` is [`processed_codes::PROCESSED`],
    /// `responses.num_transaction_responses` MUST be the same as
    /// `batch.num_transactions`. Otherwise, this field is undefined.
    /// See [`ExecutionResponseRegion`] for details.
    pub responses: ExecutionResponseRegion,
}

/// Message: [Check Worker -> Pack]
/// Message from check worker threads in response to a [`PackToCheckWorkerMessage`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct CheckWorkerToPackMessage {
    /// Offset and number of transactions in the batch.
    /// See [`SharableTransactionBatchRegion`] for details.
    /// Once the external pack process receives this message,
    /// it is responsible for freeing the memory for this batch,
    /// and is safe to do so - agave will hold no references to this memory
    /// after sending this message.
    pub batch: SharableTransactionBatchRegion,
    /// See [`processed_codes`] for accepted values.
    pub processed_code: u8,
    /// Response per transaction in the batch.
    /// When `processed_code` is [`processed_codes::PROCESSED`],
    /// `responses.num_transaction_responses` MUST be the same as
    /// `batch.num_transactions`. Otherwise, this field is undefined.
    /// See [`CheckResponseRegion`] for details.
    pub responses: CheckResponseRegion,
}

pub mod worker_message_types {
    use crate::SharablePubkeys;

    /// Response to pack for a transaction that attempted execution.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(C)]
    pub struct ExecutionResponse {
        /// The slot this transaction was executed.
        ///
        /// # Note
        ///
        /// The current semantics are:
        ///
        /// - If we successfully get a bank and try your request, this slot is the latest
        ///   bank we attempted (during a slot roll we can try 2 banks ).
        /// - If we do not attempt or attemp and fail to get a bank, this field will be
        ///   zero.
        pub execution_slot: u64,
        /// Indicates if the transaction was included in the block or not.
        /// If [`not_included_reasons::NONE`], the transaction was included.
        pub not_included_reason: u8,
        /// If included, cost units used by the transaction.
        pub cost_units: u64,
        /// If included, the fee-payer balance after execution.
        pub fee_payer_balance: u64,
    }

    pub mod not_included_reasons {
        /// The transaction was included in the block.
        pub const NONE: u8 = 0;
        /// The transaction could not attempt processing because the
        /// working bank was unavailable.
        pub const BANK_NOT_AVAILABLE: u8 = 1;
        /// Transaction was cancelled by a partial batch failure.
        pub const PARTIAL_BATCH_CANCELLED: u8 = 2;

        /// Transaction dropped because the batch was marked as
        /// all_or_nothing and a different transaction failed.
        pub const ALL_OR_NOTHING_BATCH_FAILURE: u8 = 3;

        // Remaining errors are translations from SDK.
        // Moved up to 64 so we have room to add custom reasons in the future.
        // Also allows for easy distinguishing between custom scheduling errors
        // and sdk errors.

        /// An account is already being processed in another transaction in a way
        /// that does not support parallelism
        pub const ACCOUNT_IN_USE: u8 = 64;
        /// A `Pubkey` appears twice in the transaction's `account_keys`.  Instructions can reference
        /// `Pubkey`s more than once but the message must contain a list with no duplicate keys
        pub const ACCOUNT_LOADED_TWICE: u8 = 65;
        /// Attempt to debit an account but found no record of a prior credit.
        pub const ACCOUNT_NOT_FOUND: u8 = 66;
        /// Attempt to load a program that does not exist
        pub const PROGRAM_ACCOUNT_NOT_FOUND: u8 = 67;
        /// The from `Pubkey` does not have sufficient balance to pay the fee to schedule the transaction
        pub const INSUFFICIENT_FUNDS_FOR_FEE: u8 = 68;
        /// This account may not be used to pay transaction fees
        pub const INVALID_ACCOUNT_FOR_FEE: u8 = 69;
        /// The bank has seen this transaction before. This can occur under normal operation
        pub const ALREADY_PROCESSED: u8 = 70;
        /// The bank has not seen the given `recent_blockhash`
        pub const BLOCKHASH_NOT_FOUND: u8 = 71;
        /// An error occurred while processing an instruction.
        pub const INSTRUCTION_ERROR: u8 = 72;
        /// Loader call chain is too deep
        pub const CALL_CHAIN_TOO_DEEP: u8 = 73;
        /// Transaction requires a fee but has no signature present
        pub const MISSING_SIGNATURE_FOR_FEE: u8 = 74;
        /// Transaction contains an invalid account reference
        pub const INVALID_ACCOUNT_INDEX: u8 = 75;
        /// Transaction did not pass signature verification
        pub const SIGNATURE_FAILURE: u8 = 76;
        /// This program may not be used for executing instructions
        pub const INVALID_PROGRAM_FOR_EXECUTION: u8 = 77;
        /// Transaction failed to sanitize accounts offsets correctly
        pub const SANITIZE_FAILURE: u8 = 78;
        pub const CLUSTER_MAINTENANCE: u8 = 79;
        /// Transaction processing left an account with an outstanding borrowed reference
        pub const ACCOUNT_BORROW_OUTSTANDING: u8 = 80;
        /// Transaction would exceed max Block Cost Limit
        pub const WOULD_EXCEED_MAX_BLOCK_COST_LIMIT: u8 = 81;
        /// Transaction version is unsupported
        pub const UNSUPPORTED_VERSION: u8 = 82;
        /// Transaction loads a writable account that cannot be written
        pub const INVALID_WRITABLE_ACCOUNT: u8 = 83;
        /// Transaction would exceed max account limit within the block
        pub const WOULD_EXCEED_MAX_ACCOUNT_COST_LIMIT: u8 = 84;
        /// Transaction would exceed account data limit within the block
        pub const WOULD_EXCEED_ACCOUNT_DATA_BLOCK_LIMIT: u8 = 85;
        /// Transaction locked too many accounts
        pub const TOO_MANY_ACCOUNT_LOCKS: u8 = 86;
        /// Address lookup table not found
        pub const ADDRESS_LOOKUP_TABLE_NOT_FOUND: u8 = 87;
        /// Attempted to lookup addresses from an account owned by the wrong program
        pub const INVALID_ADDRESS_LOOKUP_TABLE_OWNER: u8 = 88;
        /// Attempted to lookup addresses from an invalid account
        pub const INVALID_ADDRESS_LOOKUP_TABLE_DATA: u8 = 89;
        /// Address table lookup uses an invalid index
        pub const INVALID_ADDRESS_LOOKUP_TABLE_INDEX: u8 = 90;
        /// Transaction leaves an account with a lower balance than rent-exempt minimum
        pub const INVALID_RENT_PAYING_ACCOUNT: u8 = 91;
        /// Transaction would exceed max Vote Cost Limit
        pub const WOULD_EXCEED_MAX_VOTE_COST_LIMIT: u8 = 92;
        /// Transaction would exceed total account data limit
        pub const WOULD_EXCEED_ACCOUNT_DATA_TOTAL_LIMIT: u8 = 93;
        /// Transaction contains a duplicate instruction that is not allowed
        pub const DUPLICATE_INSTRUCTION: u8 = 94;
        /// Transaction results in an account with insufficient funds for rent
        pub const INSUFFICIENT_FUNDS_FOR_RENT: u8 = 95;
        /// Transaction exceeded max loaded accounts data size cap
        pub const MAX_LOADED_ACCOUNTS_DATA_SIZE_EXCEEDED: u8 = 96;
        /// LoadedAccountsDataSizeLimit set for transaction must be greater than 0.
        pub const INVALID_LOADED_ACCOUNTS_DATA_SIZE_LIMIT: u8 = 97;
        /// Sanitized transaction differed before/after feature activation. Needs to be resanitized.
        pub const RESANITIZATION_NEEDED: u8 = 98;
        /// Program execution is temporarily restricted on an account.
        pub const PROGRAM_EXECUTION_TEMPORARILY_RESTRICTED: u8 = 99;
        /// The total balance before the transaction does not equal the total balance after the transaction
        pub const UNBALANCED_TRANSACTION: u8 = 100;
        /// Program cache hit max limit.
        pub const PROGRAM_CACHE_HIT_MAX_LIMIT: u8 = 101;

        // This error in agave is only internal, and to avoid updating the sdk
        // it is mapped into a scheduler-specific reason above.
        // /// Commit cancelled internally.
        // pub const COMMIT_CANCELLED: u8 = 102;
    }

    pub mod parsing_and_sanitization_flags {
        /// Flag set if parsing and sanitization failed.
        pub const FAILED: u8 = 1 << 0;
    }

    pub mod status_check_flags {
        /// Flag set if status checks were requested.
        pub const REQUESTED: u8 = 1 << 0;
        /// Flag set if status checks were performed. A previous failure
        /// could have caused checks to be skipped.
        pub const PERFORMED: u8 = 1 << 1;
        /// Flag set if status checks failed due to the transaction being
        /// too old.
        pub const TOO_OLD: u8 = 1 << 2;
        /// Flag set if status checks failed due to the transaction already
        /// being processed.
        pub const ALREADY_PROCESSED: u8 = 1 << 3;
        /// Flag set if status checks failed due to an invalid nonce state.
        pub const INVALID_NONCE: u8 = 1 << 4;
        /// Flag set if status checks failed due to unsupported version of
        /// transaction was received.
        pub const UNSUPPORTED_VERSION: u8 = 1 << 5;
    }

    pub mod fee_payer_balance_flags {
        /// Flag set if fee-payer balance was requested.
        pub const REQUESTED: u8 = 1 << 0;
        /// Flag set if fee-payer balance fetching was performed. A previous
        /// failure could have caused balance fetching to be skipped.
        pub const PERFORMED: u8 = 1 << 1;
    }

    pub mod resolve_flags {
        /// Flag set if resolving pubkeys was requested.
        pub const REQUESTED: u8 = 1 << 0;
        /// Flag set if resolving pubkeys was performed.
        pub const PERFORMED: u8 = 1 << 1;
        /// Flag set if resolving failed.
        pub const FAILED: u8 = 1 << 2;
    }

    pub mod scheduling_details_flags {
        /// Flag set if scheduling details were requested.
        pub const REQUESTED: u8 = 1 << 0;
        /// Flag set if scheduling detail calculation was performed.
        pub const PERFORMED: u8 = 1 << 1;
        /// Flag set if scheduling detail calculation failed.
        pub const FAILED: u8 = 1 << 2;
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(C)]
    pub struct CheckResponse {
        /// See [`parsing_and_sanitization_flags`] for details.
        pub parsing_and_sanitization_flags: u8,
        /// See [`status_check_flags`] for details.
        pub status_check_flags: u8,
        /// See [`fee_payer_balance_flags`] for details.
        pub fee_payer_balance_flags: u8,
        /// See [`resolve_flags`] for details.
        pub resolve_flags: u8,
        /// See [`scheduling_details_flags`] for details.
        pub scheduling_details_flags: u8,

        /// If [`status_check_flags::ALREADY_PROCESSED`] is set,
        /// this is the slot the transaction was previously included in.
        /// Otherwise the value is undefined.
        pub included_slot: u64,

        /// Set only if [`scheduling_details_flags::PERFORMED`] is set and
        /// [`scheduling_details_flags::FAILED`] is clear, otherwise the value is undefined.
        /// The base transaction fee in lamports.
        pub transaction_fee: u64,
        /// Set only if [`scheduling_details_flags::PERFORMED`] is set and
        /// [`scheduling_details_flags::FAILED`] is clear, otherwise the value is undefined.
        /// The prioritization fee in lamports.
        pub prioritization_fee: u64,
        /// Set only if [`scheduling_details_flags::PERFORMED`] is set and
        /// [`scheduling_details_flags::FAILED`] is clear, otherwise the value is undefined.
        /// Estimated cost units used for scheduling.
        pub estimated_cost_units: u64,
        /// Set only if [`scheduling_details_flags::PERFORMED`] is set and
        /// [`scheduling_details_flags::FAILED`] is clear, otherwise the value is undefined.
        /// Estimated account data allocation in bytes. This cost component is tracked separately
        /// and is not included in [`Self::estimated_cost_units`].
        pub allocated_accounts_data_size: u64,

        /// Set only if [`fee_payer_balance_flags::PERFORMED`] is set,
        /// otherwise the value is undefined.
        /// The slot of the bank used to fetch fee-payer balance.
        pub balance_slot: u64,
        /// Set only if [`fee_payer_balance_flags::PERFORMED`] is set,
        /// otherwise the value is undefined.
        /// The balance of the fee-payer.
        pub fee_payer_balance: u64,

        /// Set only if [`resolve_flags::PERFORMED`] is set,
        /// otherwise the value is undefined.
        /// The slot of the bank used to resolve the pubkeys.
        pub resolution_slot: u64,
        /// Set only if [`resolve_flags::PERFORMED`] is set,
        /// otherwise the value is undefined.
        /// Minimum deactivation slot of any ALT if any.
        /// u64::MAX if no ALTs or deactivation.
        pub min_alt_deactivation_slot: u64,
        /// Set only if [`resolve_flags::PERFORMED`] is set,
        /// otherwise the value is undefined.
        /// Resolved pubkeys - writable then readonly.
        /// Freeing this memory is the responsibility of the external
        /// pack process.
        pub resolved_pubkeys: SharablePubkeys,
    }
}
