//! The in-memory index (DESIGN.md, Index): where each live key's latest
//! Set record sits in the log.
//!
//! Keys are not stored. Each is replaced by a 128-bit SipHash-1-3 under a
//! key drawn at random for every `Index`, so no one can aim two keys at the
//! same slot, and no key is allocated. Two keys sharing a hash would share
//! an entry. `get` checks the key stored in the record, so that would show
//! up as a corruption error, never as another key's value.

use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, BuildHasherDefault, Hasher};

use siphasher::sip128::SipHasher13;

/// Where a live key's latest Set record sits in the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Entry {
    pub offset: u64,
    pub value_len: u32,
    pub key_len: u16,
}

pub(crate) struct Index {
    map: HashMap<u128, Entry, BuildHasherDefault<LowBits>>,
    hasher: KeyHasher,
}

enum KeyHasher {
    Sip(SipHasher13),
    /// Hashes a key to its length, so keys of equal length collide.
    #[cfg(test)]
    Weak,
}

impl KeyHasher {
    fn hash(&self, key: &[u8]) -> u128 {
        match self {
            KeyHasher::Sip(sip) => sip.hash(key).as_u128(),
            #[cfg(test)]
            KeyHasher::Weak => key.len() as u128,
        }
    }
}

impl Index {
    pub(crate) fn new() -> Index {
        Index::with_capacity(0)
    }

    pub(crate) fn with_capacity(capacity: usize) -> Index {
        // std's RandomState is keyed from the OS's randomness; what it
        // hashes to is as unpredictable as its key.
        let seed = RandomState::new();
        Index {
            map: HashMap::with_capacity_and_hasher(capacity, Default::default()),
            hasher: KeyHasher::Sip(SipHasher13::new_with_keys(
                seed.hash_one(0u8),
                seed.hash_one(1u8),
            )),
        }
    }

    #[cfg(test)]
    pub(crate) fn weak() -> Index {
        Index {
            map: HashMap::default(),
            hasher: KeyHasher::Weak,
        }
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<&Entry> {
        self.map.get(&self.hasher.hash(key))
    }

    pub(crate) fn get_mut(&mut self, key: &[u8]) -> Option<&mut Entry> {
        self.map.get_mut(&self.hasher.hash(key))
    }

    pub(crate) fn contains(&self, key: &[u8]) -> bool {
        self.map.contains_key(&self.hasher.hash(key))
    }

    /// Points `key` at `entry`, returning the entry it replaced.
    pub(crate) fn insert(&mut self, key: &[u8], entry: Entry) -> Option<Entry> {
        self.map.insert(self.hasher.hash(key), entry)
    }

    pub(crate) fn remove(&mut self, key: &[u8]) -> Option<Entry> {
        self.map.remove(&self.hasher.hash(key))
    }

    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &Entry> {
        self.map.values()
    }

    #[cfg(test)]
    pub(crate) fn capacity(&self) -> usize {
        self.map.capacity()
    }
}

/// The map's own hash of an already uniform 128-bit hash: its low 64 bits.
#[derive(Default)]
struct LowBits(u64);

impl Hasher for LowBits {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _: &[u8]) {
        unreachable!("the index only hashes u128s");
    }

    fn write_u128(&mut self, hash: u128) {
        self.0 = hash as u64;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(offset: u64) -> Entry {
        Entry {
            offset,
            value_len: 1,
            key_len: 1,
        }
    }

    #[test]
    fn keys_map_to_their_own_entries() {
        let mut index = Index::new();
        assert_eq!(index.insert(b"a", entry(0)), None);
        assert_eq!(index.insert(b"b", entry(17)), None);
        assert_eq!(index.insert(b"a", entry(34)), Some(entry(0)));
        assert_eq!(index.get(b"a"), Some(&entry(34)));
        assert_eq!(index.get(b"b"), Some(&entry(17)));
        assert_eq!(index.get(b"c"), None);
        assert_eq!(index.remove(b"b"), Some(entry(17)));
        assert!(!index.contains(b"b"));
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn two_indexes_hash_the_same_key_differently() {
        let (Index { hasher: a, .. }, Index { hasher: b, .. }) = (Index::new(), Index::new());
        assert_ne!(a.hash(b"key"), b.hash(b"key"));
    }

    #[test]
    fn with_capacity_reserves_up_front() {
        assert!(Index::with_capacity(1000).capacity() >= 1000);
    }

    #[test]
    fn an_entry_is_16_bytes() {
        assert_eq!(size_of::<Entry>(), 16);
    }
}
