#![no_std]

//! Helius extensions to `agave-scheduler-bindings` 5.0.0.

pub use upstream_scheduler_bindings::*;

/// Offset and length of a contiguous array of simulation responses in the shared allocator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct SimulationResponseRegion {
    pub num_transaction_responses: u8,
    pub transaction_responses_offset: usize,
}

/// Message from the external scheduler to the shared simulation-worker pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct PackToSimulationWorkerMessage {
    pub flags: u16,
    pub max_working_slot: u64,
    pub batch: SharableTransactionBatchRegion,
}

/// Message from a simulation worker to the external scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct SimulationWorkerToPackMessage {
    pub batch: SharableTransactionBatchRegion,
    pub processed_code: u8,
    pub responses: SimulationResponseRegion,
}

pub mod simulation_message_flags {
    pub const NONE: u16 = 0;
}

pub mod worker_message_types {
    pub use upstream_scheduler_bindings::worker_message_types::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(C)]
    pub struct SimulationResponse {
        pub simulation_slot: u64,
        pub not_included_reason: u8,
        pub reserved: [u8; 7],
        pub cost_units: u64,
        pub fee_payer_balance: u64,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PackToSimulationWorkerMessage, SimulationResponseRegion, SimulationWorkerToPackMessage,
        worker_message_types::SimulationResponse,
    };

    #[test]
    fn simulation_abi_layout() {
        assert_eq!(core::mem::size_of::<SimulationResponse>(), 32);
        assert_eq!(
            core::mem::offset_of!(SimulationResponse, simulation_slot),
            0
        );
        assert_eq!(
            core::mem::offset_of!(SimulationResponse, not_included_reason),
            8
        );
        assert_eq!(core::mem::offset_of!(SimulationResponse, reserved), 9);
        assert_eq!(core::mem::offset_of!(SimulationResponse, cost_units), 16);
        assert_eq!(
            core::mem::offset_of!(SimulationResponse, fee_payer_balance),
            24
        );
        assert_eq!(core::mem::size_of::<SimulationResponseRegion>(), 16);
        assert_eq!(core::mem::size_of::<PackToSimulationWorkerMessage>(), 32);
        assert_eq!(core::mem::size_of::<SimulationWorkerToPackMessage>(), 40);
    }
}
