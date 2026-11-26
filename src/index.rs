//! The in-memory index (DESIGN.md, Index): where each live key's latest
//! Set record sits in the log.
//!
//! Keys are not stored. Each is replaced by a 128-bit SipHash-1-3 under a
//! key drawn at random for every `Index`, so no one can aim two keys at the
//! same slot, and no key is allocated. Two keys sharing a hash would share
//! an entry. `get` checks the key stored in the record, so that would show
//! up as a corruption error, never as another key's value.
//!
//! The map is split into 64 shards by the top bits of the hash, each behind
//! its own lock, so threads reading and writing different keys rarely wait
//! for each other, `apply_in_order` can insert from several threads at
//! once, and growing the map moves one shard at a time instead of the whole
//! index.

use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, BuildHasherDefault, Hasher};
use std::sync::{PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread;

use siphasher::sip128::SipHasher13;

use crate::record::{self, Fields};

/// Where a live key's latest Set record sits in the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Entry {
    pub offset: u64,
    pub value_len: u32,
    pub key_len: u16,
}

impl Entry {
    /// An entry for the record at `offset` with these fields.
    pub fn new(offset: u64, fields: Fields) -> Entry {
        Entry {
            offset,
            value_len: fields.value_len,
            key_len: fields.key_len,
        }
    }

    /// Length on disk of the record the entry points at.
    pub fn record_len(&self) -> u64 {
        record::record_len(self.key_len, self.value_len)
    }
}

const SHARD_BITS: u32 = 6;
const SHARDS: usize = 1 << SHARD_BITS;

type Map = HashMap<u128, Entry, BuildHasherDefault<LowBits>>;

pub(crate) struct Index {
    shards: Vec<RwLock<Map>>,
    hasher: KeyHasher,
}

/// The top bits: each shard's map places entries by the low bits, which
/// would all be equal within a shard chosen by them.
fn shard_of(hash: u128) -> usize {
    (hash >> (128 - SHARD_BITS)) as usize
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
        let per_shard = capacity.div_ceil(SHARDS);
        Index {
            shards: (0..SHARDS)
                .map(|_| {
                    RwLock::new(HashMap::with_capacity_and_hasher(
                        per_shard,
                        Default::default(),
                    ))
                })
                .collect(),
            hasher: KeyHasher::Sip(SipHasher13::new_with_keys(
                seed.hash_one(0u8),
                seed.hash_one(1u8),
            )),
        }
    }

    #[cfg(test)]
    pub(crate) fn weak() -> Index {
        Index {
            shards: (0..SHARDS).map(|_| RwLock::default()).collect(),
            hasher: KeyHasher::Weak,
        }
    }

    /// A panic while a shard is locked can't leave its map half changed,
    /// so a poisoned lock is used as it is.
    fn read(&self, hash: u128) -> RwLockReadGuard<'_, Map> {
        self.shards[shard_of(hash)]
            .read()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self, hash: u128) -> RwLockWriteGuard<'_, Map> {
        self.shards[shard_of(hash)]
            .write()
            .unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<Entry> {
        let hash = self.hasher.hash(key);
        self.read(hash).get(&hash).copied()
    }

    pub(crate) fn contains(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Points `key` at `entry`, returning the entry it replaced.
    pub(crate) fn insert(&self, key: &[u8], entry: Entry) -> Option<Entry> {
        let hash = self.hasher.hash(key);
        self.write(hash).insert(hash, entry)
    }

    pub(crate) fn remove(&self, key: &[u8]) -> Option<Entry> {
        let hash = self.hasher.hash(key);
        self.write(hash).remove(&hash)
    }

    /// Points `key` at `entry` if it still points at `offset`. Returns
    /// whether it did.
    pub(crate) fn replace(&self, key: &[u8], offset: u64, entry: Entry) -> bool {
        let hash = self.hasher.hash(key);
        match self.write(hash).get_mut(&hash) {
            Some(old) if old.offset == offset => {
                *old = entry;
                true
            }
            _ => false,
        }
    }

    pub(crate) fn len(&self) -> usize {
        (0..SHARDS).map(|shard| self.shard(shard).len()).sum()
    }

    /// Calls `f` with every entry, a shard at a time, each shard as it is
    /// when it is read.
    pub(crate) fn for_each(&self, mut f: impl FnMut(&Entry)) {
        for shard in 0..SHARDS {
            self.shard(shard).values().for_each(&mut f);
        }
    }

    fn shard(&self, shard: usize) -> RwLockReadGuard<'_, Map> {
        self.shards[shard]
            .read()
            .unwrap_or_else(PoisonError::into_inner)
    }

    #[cfg(test)]
    pub(crate) fn capacity(&self) -> usize {
        (0..SHARDS).map(|shard| self.shard(shard).capacity()).sum()
    }

    /// Applies `changes` in order, an insert for `Some` and a remove for
    /// `None`, with the result of applying them one after another. On up to
    /// `threads` threads: they hash the keys, then each applies the changes
    /// that fall in its own shards. Every change to a key falls in the same
    /// shard, so each key's changes still apply in order.
    pub(crate) fn apply_in_order(&mut self, threads: usize, changes: &[(&[u8], Option<Entry>)]) {
        let Index { shards, hasher } = self;
        let mut shards: Vec<&mut Map> = shards
            .iter_mut()
            .map(|shard| shard.get_mut().unwrap_or_else(PoisonError::into_inner))
            .collect();
        let apply = |map: &mut Map, hash: u128, change: &Option<Entry>| match change {
            Some(entry) => {
                map.insert(hash, *entry);
            }
            None => {
                map.remove(&hash);
            }
        };
        if threads <= 1 {
            for (key, change) in changes {
                let hash = hasher.hash(key);
                apply(shards[shard_of(hash)], hash, change);
            }
            return;
        }
        let mut hashes = vec![0u128; changes.len()];
        let per_thread = changes.len().div_ceil(threads).max(1);
        thread::scope(|scope| {
            for (hashes, changes) in hashes
                .chunks_mut(per_thread)
                .zip(changes.chunks(per_thread))
            {
                let hasher = &*hasher;
                scope.spawn(move || {
                    for (hash, (key, _)) in hashes.iter_mut().zip(changes) {
                        *hash = hasher.hash(key);
                    }
                });
            }
        });
        let mut owned: Vec<Vec<&mut Map>> = (0..threads).map(|_| Vec::new()).collect();
        for (shard, map) in shards.into_iter().enumerate() {
            owned[shard % threads].push(map);
        }
        let hashes = &hashes;
        thread::scope(|scope| {
            for (thread, mut maps) in owned.into_iter().enumerate() {
                scope.spawn(move || {
                    for (&hash, (_, change)) in hashes.iter().zip(changes) {
                        let shard = shard_of(hash);
                        if shard % threads == thread {
                            apply(maps[shard / threads], hash, change);
                        }
                    }
                });
            }
        });
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
        let index = Index::new();
        assert_eq!(index.insert(b"a", entry(0)), None);
        assert_eq!(index.insert(b"b", entry(17)), None);
        assert_eq!(index.insert(b"a", entry(34)), Some(entry(0)));
        assert_eq!(index.get(b"a"), Some(entry(34)));
        assert_eq!(index.get(b"b"), Some(entry(17)));
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
    fn apply_in_order_on_many_threads_ends_as_one_change_after_another_would() {
        let keys: Vec<[u8; 4]> = (0..5_000u32).map(u32::to_le_bytes).collect();
        let mut changes: Vec<(&[u8], Option<Entry>)> = Vec::new();
        for (i, key) in keys.iter().enumerate() {
            changes.push((key, Some(entry(i as u64))));
        }
        for (i, key) in keys.iter().enumerate().step_by(2) {
            changes.push((key, None));
            if i % 4 == 0 {
                changes.push((key, Some(entry(10_000 + i as u64))));
            }
        }
        changes.push((b"older", None));
        for threads in [1, 3, 8] {
            let mut index = Index::new();
            index.insert(b"older", entry(3));
            index.insert(b"kept", entry(4));
            index.apply_in_order(threads, &changes);

            assert_eq!(index.len(), 3_751, "{threads} threads");
            assert_eq!(index.get(b"older"), None);
            assert_eq!(index.get(b"kept"), Some(entry(4)));
            assert_eq!(index.get(&keys[0]), Some(entry(10_000)));
            assert_eq!(index.get(&keys[1]), Some(entry(1)));
            assert_eq!(index.get(&keys[2]), None);
            assert_eq!(index.get(&keys[4]), Some(entry(10_004)));
            assert_eq!(index.get(&keys[4_999]), Some(entry(4_999)));
        }
    }

    #[test]
    fn an_entry_is_16_bytes() {
        assert_eq!(size_of::<Entry>(), 16);
    }
}
