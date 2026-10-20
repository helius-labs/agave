use {
    solana_accounts_db::accounts_index::ScanResult,
    solana_rpc_client_api::{config::RpcLargestAccountsFilter, response::RpcAccountBalance},
    solana_runtime::non_circulating_supply::NonCirculatingSupply,
    solana_sdk::clock::BankId,
    std::{
        collections::{HashMap, VecDeque},
        sync::{Arc, Mutex},
        time::{Duration, SystemTime},
    },
};

type SupplyCacheEntry = Arc<Mutex<Option<Arc<NonCirculatingSupply>>>>;

/// Cache immutable bank results, keyed by bank ID rather than slot to distinguish forks.
/// The per-bank mutex coalesces concurrent scans without blocking other banks.
#[derive(Default)]
pub(crate) struct SupplyCache {
    entries: Mutex<VecDeque<(BankId, SupplyCacheEntry)>>,
}

impl SupplyCache {
    const CAPACITY: usize = 8;

    pub(crate) fn get_or_calculate(
        &self,
        bank_id: BankId,
        calculate: impl FnOnce() -> ScanResult<NonCirculatingSupply>,
    ) -> ScanResult<Arc<NonCirculatingSupply>> {
        let entry = {
            let mut entries = self.entries.lock().unwrap();
            if let Some(position) = entries.iter().position(|(id, _)| *id == bank_id) {
                let entry = entries.remove(position).unwrap();
                let value = Arc::clone(&entry.1);
                entries.push_back(entry);
                value
            } else {
                let entry = SupplyCacheEntry::default();
                if entries.len() == Self::CAPACITY {
                    entries.pop_front();
                }
                entries.push_back((bank_id, Arc::clone(&entry)));
                entry
            }
        };
        let mut value = entry.lock().unwrap();
        if let Some(value) = value.as_ref() {
            return Ok(Arc::clone(value));
        }
        // Failed scans leave the entry empty so subsequent requests can retry.
        let result = Arc::new(calculate()?);
        *value = Some(Arc::clone(&result));
        Ok(result)
    }
}

#[derive(Debug, Clone)]
pub struct LargestAccountsCache {
    duration: u64,
    cache: HashMap<Option<RpcLargestAccountsFilter>, LargestAccountsCacheValue>,
}

#[derive(Debug, Clone)]
struct LargestAccountsCacheValue {
    accounts: Vec<RpcAccountBalance>,
    slot: u64,
    cached_time: SystemTime,
}

impl LargestAccountsCache {
    pub(crate) fn new(duration: u64) -> Self {
        Self {
            duration,
            cache: HashMap::new(),
        }
    }

    pub(crate) fn get_largest_accounts(
        &self,
        filter: &Option<RpcLargestAccountsFilter>,
    ) -> Option<(u64, Vec<RpcAccountBalance>)> {
        self.cache.get(filter).and_then(|value| {
            if let Ok(elapsed) = value.cached_time.elapsed() {
                if elapsed < Duration::from_secs(self.duration) {
                    return Some((value.slot, value.accounts.clone()));
                }
            }
            None
        })
    }

    pub(crate) fn set_largest_accounts(
        &mut self,
        filter: &Option<RpcLargestAccountsFilter>,
        slot: u64,
        accounts: &[RpcAccountBalance],
    ) {
        self.cache.insert(
            filter.clone(),
            LargestAccountsCacheValue {
                accounts: accounts.to_owned(),
                slot,
                cached_time: SystemTime::now(),
            },
        );
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    fn supply(lamports: u64) -> ScanResult<NonCirculatingSupply> {
        Ok(NonCirculatingSupply {
            lamports,
            accounts: vec![],
        })
    }

    #[test]
    fn test_supply_cache_reuses_results_and_separates_banks() {
        let cache = SupplyCache::default();
        let first = cache.get_or_calculate(1, || supply(10)).unwrap();
        let second = cache
            .get_or_calculate(1, || panic!("cached bank must not be scanned again"))
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(
            cache.get_or_calculate(2, || supply(20)).unwrap().lamports,
            20
        );
        assert_eq!(first.lamports, 10);
    }

    #[test]
    fn test_supply_cache_retries_failed_scans() {
        let cache = SupplyCache::default();
        assert!(cache
            .get_or_calculate(1, || Err(
                solana_accounts_db::accounts_index::ScanError::Aborted("test".into())
            ))
            .is_err());
        assert_eq!(
            cache.get_or_calculate(1, || supply(10)).unwrap().lamports,
            10
        );
    }

    #[test]
    fn test_supply_cache_allows_other_banks_during_calculation() {
        let cache = SupplyCache::default();
        let result = cache
            .get_or_calculate(1, || {
                assert_eq!(cache.get_or_calculate(2, || supply(20))?.lamports, 20);
                supply(10)
            })
            .unwrap();
        assert_eq!(result.lamports, 10);
    }

    #[test]
    fn test_supply_cache_is_bounded_and_keeps_recent_banks() {
        let cache = SupplyCache::default();
        for id in 0..SupplyCache::CAPACITY as u64 {
            cache.get_or_calculate(id, || supply(id)).unwrap();
        }
        cache.get_or_calculate(0, || panic!("cache hit")).unwrap();
        cache.get_or_calculate(100, || supply(100)).unwrap();
        assert_eq!(cache.entries.lock().unwrap().len(), SupplyCache::CAPACITY);
        cache
            .get_or_calculate(0, || panic!("recent bank evicted"))
            .unwrap();
        assert_eq!(
            cache.get_or_calculate(1, || supply(42)).unwrap().lamports,
            42
        );
    }

    #[test]
    fn test_supply_cache_coalesces_concurrent_scans() {
        let cache = SupplyCache::default();
        let scans = std::sync::atomic::AtomicUsize::new(0);
        let barrier = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    barrier.wait();
                    let result = cache
                        .get_or_calculate(1, || {
                            scans.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            supply(10)
                        })
                        .unwrap();
                    assert_eq!(result.lamports, 10);
                });
            }
        });
        assert_eq!(scans.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn test_old_entries_expire() {
        let mut cache = LargestAccountsCache::new(1);

        let filter = Some(RpcLargestAccountsFilter::Circulating);

        let accounts: Vec<RpcAccountBalance> = Vec::new();

        cache.set_largest_accounts(&filter, 1000, &accounts);
        std::thread::sleep(Duration::from_secs(1));
        assert_eq!(cache.get_largest_accounts(&filter), None);
    }
}
