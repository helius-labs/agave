//! FastPubkey wrapper and base58 encoding cache.

use {
    solana_pubkey::Pubkey,
    std::{collections::HashMap, sync::Arc},
};

/// Pubkey wrapper with fast hashing — same approach as laserstream's FastPubkey.
/// XORs all four 8-byte chunks of the pubkey for the hash value.
/// Safe because ed25519 public keys are cryptographically random.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct FastPubkey(Pubkey);

impl std::hash::Hash for FastPubkey {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let bytes = self.0.as_ref();
        let a = u64::from_le_bytes(bytes[0..8].try_into().expect("pubkey is 32 bytes"));
        let b = u64::from_le_bytes(bytes[8..16].try_into().expect("pubkey is 32 bytes"));
        let c = u64::from_le_bytes(bytes[16..24].try_into().expect("pubkey is 32 bytes"));
        let d = u64::from_le_bytes(bytes[24..32].try_into().expect("pubkey is 32 bytes"));
        state.write_u64(a ^ b ^ c ^ d);
    }
}

impl solana_nohash_hasher::IsEnabled for FastPubkey {}

/// Cache of base58-encoded pubkey strings as Arc<str>.
/// Uses NoHashHasher for O(1) lookup (pubkey bytes are already random).
/// After warmup (~1500 validators), each call is a HashMap probe + Arc::clone.
pub struct PubkeyEncodingCache(
    HashMap<FastPubkey, Arc<str>, solana_nohash_hasher::BuildNoHashHasher<FastPubkey>>,
);

impl PubkeyEncodingCache {
    pub fn new() -> Self {
        Self(HashMap::with_hasher(
            solana_nohash_hasher::BuildNoHashHasher::default(),
        ))
    }

    #[inline]
    pub fn encode(&mut self, pubkey: &Pubkey) -> Arc<str> {
        self.0
            .entry(FastPubkey(*pubkey))
            .or_insert_with(|| Arc::from(pubkey.to_string()))
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fast_pubkey_hash_xors_all_chunks() {
        let mut bytes = [0u8; 32];
        bytes[0..8].copy_from_slice(&1u64.to_le_bytes());
        bytes[8..16].copy_from_slice(&2u64.to_le_bytes());
        bytes[16..24].copy_from_slice(&4u64.to_le_bytes());
        bytes[24..32].copy_from_slice(&8u64.to_le_bytes());
        let fast = FastPubkey(Pubkey::from(bytes));

        let mut hasher = solana_nohash_hasher::NoHashHasher::<FastPubkey>::default();
        std::hash::Hash::hash(&fast, &mut hasher);
        assert_eq!(std::hash::Hasher::finish(&hasher), 1 ^ 2 ^ 4 ^ 8);
    }

    #[test]
    fn test_fast_pubkey_different_keys_different_hashes() {
        let mut bytes1 = [0u8; 32];
        let mut bytes2 = [0u8; 32];
        bytes1[0..8].copy_from_slice(&100u64.to_le_bytes());
        bytes2[0..8].copy_from_slice(&200u64.to_le_bytes());

        let fast1 = FastPubkey(Pubkey::from(bytes1));
        let fast2 = FastPubkey(Pubkey::from(bytes2));

        let mut h1 = solana_nohash_hasher::NoHashHasher::<FastPubkey>::default();
        let mut h2 = solana_nohash_hasher::NoHashHasher::<FastPubkey>::default();
        std::hash::Hash::hash(&fast1, &mut h1);
        std::hash::Hash::hash(&fast2, &mut h2);
        assert_ne!(std::hash::Hasher::finish(&h1), std::hash::Hasher::finish(&h2));
    }

    #[test]
    fn test_fast_pubkey_repr_transparent() {
        assert_eq!(std::mem::size_of::<FastPubkey>(), std::mem::size_of::<Pubkey>());
        assert_eq!(std::mem::align_of::<FastPubkey>(), std::mem::align_of::<Pubkey>());
    }

    #[test]
    fn test_encoding_cache_returns_same_arc() {
        let mut cache = PubkeyEncodingCache::new();
        let pubkey = Pubkey::new_unique();

        let a = cache.encode(&pubkey);
        let b = cache.encode(&pubkey);

        // Same Arc — pointer equality
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(&*a, &pubkey.to_string());
    }

    #[test]
    fn test_encoding_cache_different_keys() {
        let mut cache = PubkeyEncodingCache::new();
        let pk1 = Pubkey::new_unique();
        let pk2 = Pubkey::new_unique();

        let a = cache.encode(&pk1);
        let b = cache.encode(&pk2);

        assert!(!Arc::ptr_eq(&a, &b));
        assert_eq!(&*a, &pk1.to_string());
        assert_eq!(&*b, &pk2.to_string());
    }
}
