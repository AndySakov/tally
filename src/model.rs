//! A `BTreeMap` the store must agree with, for the tests and the fuzz
//! target that run random operations: the operations, how each changes the
//! map and the log, and a harness that runs them against a store.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::fs::{Fs, LogFile};
use crate::segment::parse_log_name;
use crate::store::Write;
use crate::{AutoCompact, Batch, Key, Options, Store, Value};

/// Keys a thread's operations reach. Few, so overwrites, deletes of live
/// keys and deletes of missing keys all happen often.
pub const KEY_SPACE: u8 = 8;

pub type State = BTreeMap<u8, Vec<u8>>;

/// A set (`Some`) or a delete (`None`) of a key.
pub type Change = (u8, Option<Vec<u8>>);

#[derive(Debug, Clone)]
#[cfg_attr(fuzzing, derive(arbitrary::Arbitrary))]
pub enum Op {
    Set(u8, Vec<u8>),
    Get(u8),
    Delete(u8),
    Reopen,
    Compact,
    Batch(Vec<Change>),
    /// Writes committed as one group, as writes from several threads at
    /// once are (DESIGN.md, Group commit).
    Group(Vec<Vec<Change>>),
}

/// Key `k % KEY_SPACE` of thread `t`.
pub fn key(t: u8, k: u8) -> Key {
    Key::try_from(&[t, k % KEY_SPACE][..]).unwrap()
}

/// Lengths in the format, written out rather than taken from the code
/// under test, so a change to the format fails here.
const KEY_LEN: u64 = 2;
const HEADER_LEN: u64 = 19;
const BATCH_LEN: u64 = HEADER_LEN + 8;
const FILE_HEADER_LEN: u64 = 17;

fn value(v: &[u8]) -> Value {
    Value::try_from(v).unwrap()
}

fn batch(t: u8, changes: &[Change]) -> Batch {
    let mut batch = Batch::new();
    for (k, v) in changes {
        match v {
            Some(v) => batch.set(key(t, *k), value(v)),
            None => batch.delete(key(t, *k)),
        };
    }
    batch
}

/// Applies `changes`, written with one append, to `state`. Returns how
/// many bytes of log they take: a delete of a key that isn't live writes
/// nothing, and more than one record goes inside a Batch record.
fn apply<'c>(state: &mut State, changes: impl IntoIterator<Item = &'c Change>) -> u64 {
    let mut bytes = 0;
    let mut records = 0;
    for (k, v) in changes {
        let k = k % KEY_SPACE;
        let len = match v {
            Some(v) => {
                state.insert(k, v.clone());
                v.len() as u64
            }
            None if state.remove(&k).is_some() => 0,
            None => continue,
        };
        bytes += HEADER_LEN + KEY_LEN + len;
        records += 1;
    }
    if records > 1 {
        bytes += BATCH_LEN;
    }
    bytes
}

/// Bytes of record the live keys of `state` take.
pub fn live_len(state: &State) -> u64 {
    state
        .values()
        .map(|v| HEADER_LEN + KEY_LEN + v.len() as u64)
        .sum()
}

/// Runs `op` against `store` as thread `t` and applies it to `state`,
/// checking that a `Get` reads what `state` holds. Returns the bytes of log
/// it wrote. The caller does `Reopen`.
pub fn run<F: Fs>(store: &Store<F>, t: u8, state: &mut State, op: &Op) -> u64 {
    match op {
        Op::Set(k, v) => {
            store.set(key(t, *k), value(v)).unwrap();
            apply(state, &[(*k, Some(v.clone()))])
        }
        Op::Get(k) => {
            let got = store.get(&key(t, *k)).unwrap();
            assert_eq!(
                got.as_ref().map(Value::as_bytes),
                state.get(&(k % KEY_SPACE)).map(Vec::as_slice),
                "thread {t}, key {}",
                k % KEY_SPACE
            );
            0
        }
        Op::Delete(k) => {
            store.delete(key(t, *k)).unwrap();
            apply(state, &[(*k, None)])
        }
        Op::Reopen => unreachable!("the caller reopens"),
        Op::Compact => {
            store.compact().unwrap();
            0
        }
        Op::Batch(changes) => {
            store.write(batch(t, changes)).unwrap();
            apply(state, changes)
        }
        Op::Group(writes) => {
            let group: Vec<Write> = writes
                .iter()
                .map(|w| Write::Batch(batch(t, w).ops))
                .collect();
            for result in store.commit(&group) {
                result.unwrap();
            }
            apply(state, writes.iter().flatten())
        }
    }
}

/// Checks every key of thread `t` reads what `state` holds.
pub fn check<F: Fs>(store: &Store<F>, t: u8, state: &State) {
    let mut state = state.clone();
    for k in 0..KEY_SPACE {
        run(store, t, &mut state, &Op::Get(k));
    }
}

/// Small segments, so writes roll into new ones and reads and reopens
/// cross several. Automatic compaction, if on, at half dead with no floor,
/// so it cleans some segments and keeps others.
pub fn options(auto: bool) -> Options {
    Options {
        segment_bytes: 100,
        auto_compact: auto.then(|| AutoCompact::new(50, 0)).flatten(),
    }
}

/// One thread's operations against a store of its own, which it can
/// reopen. Without automatic compaction, the log must be exactly as long
/// as the writes that reached it; with it, at least as long as the live
/// records.
pub struct Model<F: Fs + Clone> {
    fs: F,
    dir: PathBuf,
    options: Options,
    store: Option<Store<F>>,
    state: State,
    log_len: u64,
}

impl<F: Fs + Clone> Model<F> {
    pub fn new(fs: F, dir: &Path, options: Options) -> Self {
        Model {
            store: Some(Store::open_in(fs.clone(), dir, options).unwrap()),
            fs,
            dir: dir.to_path_buf(),
            options,
            state: State::new(),
            log_len: 0,
        }
    }

    /// Runs `op` and checks the log's length.
    pub fn step(&mut self, op: &Op) {
        if let Op::Reopen = op {
            self.reopen();
        } else {
            let store = self.store.as_ref().expect("open between steps");
            let wrote = run(store, 0, &mut self.state, op);
            self.log_len = match op {
                Op::Compact => live_len(&self.state),
                _ => self.log_len + wrote,
            };
        }
        let on_disk = self.on_disk();
        if self.options.auto_compact.is_some() {
            assert!(on_disk >= live_len(&self.state), "{on_disk} bytes of log");
        } else {
            assert_eq!(on_disk, self.log_len);
        }
    }

    /// Reopens the store and checks every key.
    pub fn finish(mut self) {
        self.reopen();
        check(self.store(), 0, &self.state);
        self.store().verify().unwrap();
    }

    fn reopen(&mut self) {
        self.store = None;
        self.store = Some(Store::open_in(self.fs.clone(), &self.dir, self.options).unwrap());
    }

    fn store(&self) -> &Store<F> {
        self.store.as_ref().expect("open between steps")
    }

    /// Bytes of record across the log's segment files.
    fn on_disk(&self) -> u64 {
        self.fs
            .list(&self.dir)
            .unwrap()
            .into_iter()
            .filter(|name| parse_log_name(name).is_some())
            .map(|name| {
                let file = self.fs.open_existing(&self.dir.join(name)).unwrap();
                file.size().unwrap() - FILE_HEADER_LEN
            })
            .sum()
    }
}

#[cfg(test)]
mod tests {
    //! Every `set` and `delete` fsyncs on the real disk (about 5 ms on
    //! macOS), so the default case count is small. For a longer run:
    //! `PROPTEST_CASES=2000 cargo test model::`.

    use std::thread;

    use proptest::prelude::*;

    use super::*;
    use crate::fs::OsFs;

    fn changes() -> impl Strategy<Value = Vec<Change>> {
        let value = prop::collection::vec(any::<u8>(), 1..64);
        prop::collection::vec((0..KEY_SPACE / 2, prop::option::of(value)), 0..6)
    }

    /// Keys from half the key space, so they collide more.
    fn op() -> impl Strategy<Value = Op> {
        let key = 0..KEY_SPACE / 2;
        let value = || prop::collection::vec(any::<u8>(), 1..64);
        prop_oneof![
            4 => (key.clone(), value()).prop_map(|(k, v)| Op::Set(k, v)),
            3 => key.clone().prop_map(Op::Get),
            2 => key.prop_map(Op::Delete),
            1 => Just(Op::Reopen),
            1 => Just(Op::Compact),
            2 => changes().prop_map(Op::Batch),
            1 => prop::collection::vec(changes(), 1..4).prop_map(Op::Group),
        ]
    }

    /// The cases an earlier model test shrank failures to, before its
    /// strategies changed and their seeds stopped reproducing them. Only
    /// the values' lengths are kept: their bytes change no segment's size.
    #[test]
    fn the_store_matches_a_map_in_cases_once_shrunk_to() {
        let cases: [(bool, Vec<Op>); 2] = [
            (false, vec![Op::Delete(0)]),
            (
                true,
                vec![
                    Op::Set(0, vec![7; 1]),
                    Op::Set(0, vec![7; 57]),
                    Op::Batch(vec![(3, Some(vec![7; 9]))]),
                    Op::Set(1, vec![7; 49]),
                    Op::Batch(vec![
                        (3, Some(vec![7; 61])),
                        (0, Some(vec![7; 14])),
                        (3, None),
                        (2, Some(vec![7; 30])),
                    ]),
                ],
            ),
        ];
        for (auto, ops) in cases {
            let tmp = tempfile::tempdir().unwrap();
            let mut model = Model::new(OsFs, tmp.path(), options(auto));
            for op in &ops {
                model.step(op);
            }
            model.finish();
        }
    }

    const THREADS: u8 = 4;

    fn cases() -> u32 {
        std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(48)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(cases()))]

        #[test]
        fn the_store_matches_a_map(auto in any::<bool>(), ops in prop::collection::vec(op(), 1..40)) {
            let tmp = tempfile::tempdir().unwrap();
            let mut model = Model::new(OsFs, tmp.path(), options(auto));
            for op in &ops {
                model.step(op);
            }
            model.finish();
        }

        /// Each thread has keys and a map of its own. A reopen in a thread
        /// becomes a compaction, since the threads share the store.
        #[test]
        fn threads_on_one_store_match_their_own_maps(
            auto in any::<bool>(),
            ops in prop::collection::vec(prop::collection::vec(op(), 1..30), THREADS as usize),
        ) {
            let tmp = tempfile::tempdir().unwrap();
            let store = Store::open_in(OsFs, tmp.path(), options(auto)).unwrap();
            let states: Vec<State> = thread::scope(|scope| {
                let threads: Vec<_> = ops
                    .iter()
                    .enumerate()
                    .map(|(t, ops)| {
                        let store = &store;
                        scope.spawn(move || {
                            let mut state = State::new();
                            for op in ops {
                                let op = if let Op::Reopen = op { &Op::Compact } else { op };
                                run(store, t as u8, &mut state, op);
                            }
                            state
                        })
                    })
                    .collect();
                threads.into_iter().map(|t| t.join().unwrap()).collect()
            });
            for (t, state) in states.iter().enumerate() {
                check(&store, t as u8, state);
            }
            store.verify().unwrap();
            drop(store);
            let store = Store::open_in(OsFs, tmp.path(), options(auto)).unwrap();
            for (t, state) in states.iter().enumerate() {
                check(&store, t as u8, state);
            }
            store.verify().unwrap();
        }
    }
}
