//! `SlotLifecycle` — lives on `Bank`, stamped by any thread via atomics.

use {
    solana_clock::Slot,
    std::sync::atomic::{AtomicU64, Ordering},
    super::{
        sink::{self, BankLifecycleRow, TableRow},
        tsc::{hostname, now_datetime, rdtsc, tsc_delta_us, wallclock_us},
    },
};

/// Per-slot lifecycle timestamps. Lives on `Bank`.
/// All fields are atomic for lock-free cross-thread stamping.
/// 0 means "not yet recorded".
#[derive(Debug, Default)]
pub struct SlotLifecycle {
    pub created_bank_tsc: AtomicU64,
    pub created_bank_wallclock_us: AtomicU64,
    pub frozen_tsc: AtomicU64,
    pub optimistic_confirmation_tsc: AtomicU64,
    pub root_tsc: AtomicU64,
}

impl SlotLifecycle {
    #[inline]
    pub fn stamp_created_bank(&self) {
        self.created_bank_tsc.store(rdtsc(), Ordering::Relaxed);
        self.created_bank_wallclock_us.store(wallclock_us(), Ordering::Relaxed);
    }

    #[inline]
    pub fn stamp_frozen(&self) {
        self.frozen_tsc.store(rdtsc(), Ordering::Relaxed);
    }

    #[inline]
    pub fn stamp_optimistic_confirmation(&self) {
        self.optimistic_confirmation_tsc.store(rdtsc(), Ordering::Relaxed);
    }

    #[inline]
    pub fn stamp_root(&self) {
        self.root_tsc.store(rdtsc(), Ordering::Relaxed);
    }

    pub fn log_lifecycle(&self, slot: Slot, epoch: u64) {
        let created = self.created_bank_tsc.load(Ordering::Relaxed);
        let frozen = self.frozen_tsc.load(Ordering::Relaxed);
        let confirmed = self.optimistic_confirmation_tsc.load(Ordering::Relaxed);
        let root = self.root_tsc.load(Ordering::Relaxed);

        sink::record(TableRow::BankLifecycle(BankLifecycleRow {
            timestamp: now_datetime(),
            host: hostname(),
            slot,
            epoch,
            created_bank_wallclock_us: self.created_bank_wallclock_us.load(Ordering::Relaxed),
            created_to_frozen_us: tsc_delta_us(frozen, created),
            created_to_confirmed_us: tsc_delta_us(confirmed, created),
            created_to_root_us: tsc_delta_us(root, created),
            frozen_to_confirmed_us: tsc_delta_us(confirmed, frozen),
            confirmed_to_root_us: tsc_delta_us(root, confirmed),
        }));
    }
}
