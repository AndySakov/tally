//! A filter of the keys a sealed segment holds a Set record for, so
//! compaction can drop a Delete record that no kept segment needs
//! (DESIGN.md, Compaction).

use std::hash::{BuildHasher, RandomState};

use siphasher::sip::SipHasher13;
use xorf::{BinaryFuse16, Filter as _};

/// Says no for a key never added, and yes for one that was: wrongly about
/// 1 time in 65,536, for the same keys until the filter is built again.
/// Each filter hashes under a key of its own, drawn at random, so which
/// keys it is wrong about changes with every `open`.
pub(crate) struct Filter {
    hasher: SipHasher13,
    keys: Keys,
}

enum Keys {
    None,
    Some(BinaryFuse16),
    /// The filter could not be built, which needs duplicate hashes: say yes
    /// to every key, which only keeps Delete records.
    Any,
}

impl Filter {
    pub(crate) fn builder() -> Builder {
        let seed = RandomState::new();
        Builder {
            hasher: SipHasher13::new_with_keys(seed.hash_one(0u8), seed.hash_one(1u8)),
            hashes: Vec::new(),
        }
    }

    pub(crate) fn may_hold(&self, key: &[u8]) -> bool {
        match &self.keys {
            Keys::None => false,
            Keys::Some(fuse) => fuse.contains(&self.hasher.hash(key)),
            Keys::Any => true,
        }
    }
}

pub(crate) struct Builder {
    hasher: SipHasher13,
    hashes: Vec<u64>,
}

impl Builder {
    pub(crate) fn add(&mut self, key: &[u8]) {
        self.hashes.push(self.hasher.hash(key));
    }

    /// Forgets every key added.
    pub(crate) fn clear(&mut self) {
        self.hashes.clear();
    }

    pub(crate) fn build(mut self) -> Filter {
        self.hashes.sort_unstable();
        self.hashes.dedup();
        let keys = if self.hashes.is_empty() {
            Keys::None
        } else {
            BinaryFuse16::try_from(&self.hashes).map_or(Keys::Any, Keys::Some)
        };
        Filter {
            hasher: self.hasher,
            keys,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn built(keys: impl Iterator<Item = u32>) -> Filter {
        let mut builder = Filter::builder();
        for key in keys {
            builder.add(&key.to_le_bytes());
        }
        builder.build()
    }

    #[test]
    fn a_filter_holds_every_key_added_and_few_others() {
        let filter = built((0..100_000).chain(0..10));
        assert!((0..100_000u32).all(|k| filter.may_hold(&k.to_le_bytes())));
        let wrong = (100_000..1_100_000u32)
            .filter(|k| filter.may_hold(&k.to_le_bytes()))
            .count();
        assert!(wrong < 60, "{wrong} of 1,000,000 keys never added");
    }

    #[test]
    fn an_empty_filter_holds_nothing() {
        assert!(!built(0..0).may_hold(b"a"));
    }
}
