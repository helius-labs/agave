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
