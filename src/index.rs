//! The in-memory index (DESIGN.md, Index): where each live key's latest
//! Set record sits in the log.
//!
//! Keys are not stored. Each is replaced by a 128-bit SipHash-1-3 under a
//! key drawn at random for every `Index`, so no one can aim two keys at the
//! same slot, and no key is allocated. Two keys sharing a hash would share
//! an entry. `get` checks the key stored in the record, so that would show
//! up as a corruption error, never as another key's value.
//!
//! The map is split into 64 shards by the top bits of the hash, so `fill`
//! and `apply_in_order` can insert from several threads at once, and
//! growing the map moves one shard at a time instead of the whole index.

use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, BuildHasherDefault, Hasher};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use siphasher::sip128::SipHasher13;

/// Where a live key's latest Set record sits in the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Entry {
    pub offset: u64,
    pub value_len: u32,
    pub key_len: u16,
}

const SHARD_BITS: u32 = 6;
const SHARDS: usize = 1 << SHARD_BITS;
/// Entries a `Filler` holds per shard before taking the shard's lock.
const FLUSH_AT: usize = 256;

type Map = HashMap<u128, Entry, BuildHasherDefault<LowBits>>;

pub(crate) struct Index {
    shards: Vec<Map>,
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
                .map(|_| HashMap::with_capacity_and_hasher(per_shard, Default::default()))
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
            shards: (0..SHARDS).map(|_| Map::default()).collect(),
            hasher: KeyHasher::Weak,
        }
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<&Entry> {
        let hash = self.hasher.hash(key);
        self.shards[shard_of(hash)].get(&hash)
    }

    pub(crate) fn get_mut(&mut self, key: &[u8]) -> Option<&mut Entry> {
        let hash = self.hasher.hash(key);
        self.shards[shard_of(hash)].get_mut(&hash)
    }

    pub(crate) fn contains(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Points `key` at `entry`, returning the entry it replaced.
    pub(crate) fn insert(&mut self, key: &[u8], entry: Entry) -> Option<Entry> {
        let hash = self.hasher.hash(key);
        self.shards[shard_of(hash)].insert(hash, entry)
    }

    pub(crate) fn remove(&mut self, key: &[u8]) -> Option<Entry> {
        let hash = self.hasher.hash(key);
        self.shards[shard_of(hash)].remove(&hash)
    }

    pub(crate) fn len(&self) -> usize {
        self.shards.iter().map(Map::len).sum()
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &Entry> {
        self.shards.iter().flat_map(Map::values)
    }

    #[cfg(test)]
    pub(crate) fn capacity(&self) -> usize {
        self.shards.iter().map(Map::capacity).sum()
    }

    /// Runs `job` on every element of `jobs`, spread over `threads` threads
    /// (the calling thread is one of them), inserting what each pushes into
    /// its `Filler`, over any entry already there. For keys no two jobs
    /// share: if two do, which entry wins is not defined. Returns how many
    /// inserts replaced an entry at `floor` or later, so a caller filling
    /// from `floor` on can tell a key it pushed twice.
    pub(crate) fn fill<J: Sync>(
        &mut self,
        threads: usize,
        jobs: &[J],
        floor: u64,
        job: impl Fn(&J, &mut Filler<'_>) + Sync,
    ) -> usize {
        let shards: Vec<Mutex<Map>> = self.shards.drain(..).map(Mutex::new).collect();
        let next = AtomicUsize::new(0);
        let replaced = AtomicUsize::new(0);
        let work = || {
            let mut filler = Filler {
                hasher: &self.hasher,
                shards: &shards,
                pending: vec![Vec::new(); SHARDS],
                floor,
                replaced: 0,
            };
            while let Some(j) = jobs.get(next.fetch_add(1, Ordering::Relaxed)) {
                job(j, &mut filler);
            }
            filler.flush_all();
            replaced.fetch_add(filler.replaced, Ordering::Relaxed);
        };
        thread::scope(|scope| {
            for _ in 1..threads.min(jobs.len()) {
                scope.spawn(work);
            }
            work();
        });
        self.shards = shards
            .into_iter()
            .map(|shard| shard.into_inner().expect("no filler panicked"))
            .collect();
        replaced.into_inner()
    }

    /// Applies `changes` in order, an insert for `Some` and a remove for
    /// `None`, with the result of applying them one after another. On up to
    /// `threads` threads: they hash the keys, then each applies the changes
    /// that fall in its own shards. Every change to a key falls in the same
    /// shard, so each key's changes still apply in order.
    pub(crate) fn apply_in_order(&mut self, threads: usize, changes: &[(&[u8], Option<Entry>)]) {
        let Index { shards, hasher } = self;
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
                apply(&mut shards[shard_of(hash)], hash, change);
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
        for (shard, map) in shards.iter_mut().enumerate() {
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

/// Where one `fill` thread puts entries. Holds a few per shard so it takes
/// each shard's lock once per batch, not once per entry.
pub(crate) struct Filler<'a> {
    hasher: &'a KeyHasher,
    shards: &'a [Mutex<Map>],
    pending: Vec<Vec<(u128, Entry)>>,
    floor: u64,
    replaced: usize,
}

impl Filler<'_> {
    pub(crate) fn push(&mut self, key: &[u8], entry: Entry) {
        let hash = self.hasher.hash(key);
        let shard = shard_of(hash);
        self.pending[shard].push((hash, entry));
        if self.pending[shard].len() >= FLUSH_AT {
            self.flush(shard);
        }
    }

    fn flush(&mut self, shard: usize) {
        let mut map = self.shards[shard].lock().expect("no filler panicked");
        for (hash, entry) in self.pending[shard].drain(..) {
            if map
                .insert(hash, entry)
                .is_some_and(|old| old.offset >= self.floor)
            {
                self.replaced += 1;
            }
        }
    }

    fn flush_all(&mut self) {
        for shard in 0..SHARDS {
            self.flush(shard);
        }
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
    fn fill_on_many_threads_builds_the_index_inserts_would() {
        let keys: Vec<Vec<u8>> = (0..10_000u32).map(|i| i.to_le_bytes().to_vec()).collect();
        let jobs: Vec<&[Vec<u8>]> = keys.chunks(37).collect();
        let mut filled = Index::new();
        filled.insert(&keys[5], entry(1 << 40));
        filled.insert(&keys[6], entry(99));
        filled.insert(b"older", entry(3));
        let replaced = filled.fill(4, &jobs, 100, |chunk, filler| {
            for key in *chunk {
                filler.push(
                    key,
                    entry(u64::from_le_bytes([
                        key[0], key[1], key[2], key[3], 0, 0, 0, 0,
                    ])),
                );
            }
        });

        assert_eq!(replaced, 1, "keys[6] was below the floor");
        assert_eq!(filled.len(), 10_001);
        assert_eq!(filled.get(b"older"), Some(&entry(3)));
        for (i, key) in keys.iter().enumerate() {
            assert_eq!(filled.get(key), Some(&entry(i as u64)), "key {i}");
        }
        assert_eq!(filled.get(&10_000u32.to_le_bytes()), None);
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
            assert_eq!(index.get(b"kept"), Some(&entry(4)));
            assert_eq!(index.get(&keys[0]), Some(&entry(10_000)));
            assert_eq!(index.get(&keys[1]), Some(&entry(1)));
            assert_eq!(index.get(&keys[2]), None);
            assert_eq!(index.get(&keys[4]), Some(&entry(10_004)));
            assert_eq!(index.get(&keys[4_999]), Some(&entry(4_999)));
        }
    }

    #[test]
    fn fill_with_no_jobs_leaves_the_index_as_it_was() {
        let mut index = Index::new();
        index.insert(b"a", entry(0));
        assert_eq!(index.fill(4, &[] as &[u8], 0, |_, _| unreachable!()), 0);
        assert_eq!(index.get(b"a"), Some(&entry(0)));
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn an_entry_is_16_bytes() {
        assert_eq!(size_of::<Entry>(), 16);
    }
}
