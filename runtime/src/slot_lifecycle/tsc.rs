//! TSC (Time Stamp Counter) utilities for low-overhead timestamping.
//!
//! # Safety
//! Uses `std::arch::x86_64::_rdtsc()` — safe: read-only, no side effects,
//! monotonic on modern x86_64 (invariant TSC).

use std::{
    sync::OnceLock,
    time::{Duration, Instant, SystemTime},
};

use time::OffsetDateTime;

/// Read CPU timestamp counter. ~1-2ns overhead.
#[inline(always)]
pub fn rdtsc() -> u64 {
    // SAFETY: _rdtsc is always safe to call on x86_64.
    #[cfg(target_arch = "x86_64")]
    unsafe { std::arch::x86_64::_rdtsc() }
    #[cfg(not(target_arch = "x86_64"))]
    { 0 }
}

fn calibrate_tsc_mhz() -> f64 {
    let start_tsc = rdtsc();
    let start = Instant::now();
    std::thread::sleep(Duration::from_millis(10));
    let elapsed_us = start.elapsed().as_micros() as f64;
    let elapsed_tsc = rdtsc().wrapping_sub(start_tsc) as f64;
    elapsed_tsc / elapsed_us
}

static TSC_MHZ: OnceLock<f64> = OnceLock::new();

pub fn tsc_mhz() -> f64 {
    *TSC_MHZ.get_or_init(calibrate_tsc_mhz)
}

#[inline(always)]
pub fn cycles_to_us(cycles: u64) -> i64 {
    (cycles as f64 / tsc_mhz()) as i64
}

#[inline]
pub fn tsc_delta_us(end: u64, start: u64) -> i64 {
    if end == 0 || start == 0 { -1 } else { cycles_to_us(end.wrapping_sub(start)) }
}

/// Wall-clock epoch microseconds, derived from rdtsc after a one-time anchor.
/// ~1-2ns per call (rdtsc) instead of ~20-25ns (clock_gettime syscall).
pub fn wallclock_us() -> u64 {
    static ANCHOR: OnceLock<(u64, u64)> = OnceLock::new();
    let (anchor_tsc, anchor_us) = ANCHOR.get_or_init(|| {
        let epoch_us = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        (rdtsc(), epoch_us)
    });
    let elapsed_us = cycles_to_us(rdtsc().wrapping_sub(*anchor_tsc));
    anchor_us.wrapping_add(elapsed_us as u64)
}

fn us_to_datetime(us: u64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp_nanos(us as i128 * 1000).unwrap_or(OffsetDateTime::UNIX_EPOCH)
}

/// Cheap wall-clock as OffsetDateTime (rdtsc-based).
pub fn now_datetime() -> OffsetDateTime {
    us_to_datetime(wallclock_us())
}

pub fn hostname() -> &'static str {
    static HOSTNAME: OnceLock<String> = OnceLock::new();
    HOSTNAME.get_or_init(|| {
        gethostname::gethostname()
            .to_string_lossy()
            .into_owned()
    })
}
