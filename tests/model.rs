//! Runs random sequences of operations against a `Store` and a `HashMap`
//! and checks they agree after every step, including across reopens. With
//! automatic compaction off, the log must be exactly as long as the writes
//! that reached it; with it on, at least as long as the live records.
//!
//! A second test runs several threads on one store at once, each with keys
//! of its own and a `HashMap` of its own, while compactions run between
//! their writes.
//!
//! Every `set` and `delete` fsyncs (about 5 ms on macOS), so the default case
//! count is small. For a longer run: `PROPTEST_CASES=2000 cargo test --test model`.

use std::collections::HashMap;
use std::path::Path;
use std::thread;

use proptest::prelude::*;
use tally::{AutoCompact, Batch, Key, Options, Store, Value};

const HEADER_LEN: u64 = 19;

/// Few distinct keys, so overwrites, deletes of live keys and deletes of
/// missing keys all happen often.
const KEY_SPACE: u8 = 4;

#[derive(Debug, Clone)]
enum Op {
    Set(u8, Vec<u8>),
    Get(u8),
    Delete(u8),
    Reopen,
    /// Sets (`Some`) and deletes (`None`) written as one batch.
    Batch(Vec<(u8, Option<Vec<u8>>)>),
}

fn op() -> impl Strategy<Value = Op> {
    let key = 0..KEY_SPACE;
    prop_oneof![
        4 => (key.clone(), prop::collection::vec(any::<u8>(), 1..64))
            .prop_map(|(k, v)| Op::Set(k, v)),
        3 => key.clone().prop_map(Op::Get),
        2 => key.clone().prop_map(Op::Delete),
        1 => Just(Op::Reopen),
        2 => prop::collection::vec(
            (key, prop::option::of(prop::collection::vec(any::<u8>(), 1..64))),
            0..6
        )
        .prop_map(Op::Batch),
    ]
}

#[derive(Debug, Clone)]
enum ThreadOp {
    Set(u8, Vec<u8>),
    Get(u8),
    Delete(u8),
    Batch(Vec<(u8, Option<Vec<u8>>)>),
    Compact,
}

fn thread_op() -> impl Strategy<Value = ThreadOp> {
    let key = 0..KEY_SPACE;
    let value = || prop::collection::vec(any::<u8>(), 1..64);
    prop_oneof![
        4 => (key.clone(), value()).prop_map(|(k, v)| ThreadOp::Set(k, v)),
        3 => key.clone().prop_map(ThreadOp::Get),
        2 => key.clone().prop_map(ThreadOp::Delete),
        2 => prop::collection::vec((key, prop::option::of(value())), 0..6)
            .prop_map(ThreadOp::Batch),
        1 => Just(ThreadOp::Compact),
    ]
}

const THREADS: u8 = 4;

/// Thread `t`'s key `k`.
fn thread_key(t: u8, k: u8) -> Key {
    Key::try_from(&[b't', t, k][..]).unwrap()
}

fn run_thread(store: &Store, t: u8, ops: Vec<ThreadOp>) -> HashMap<u8, Vec<u8>> {
    let mut model = HashMap::new();
    for op in ops {
        match op {
            ThreadOp::Set(k, v) => {
                store
                    .set(thread_key(t, k), Value::try_from(&v[..]).unwrap())
                    .unwrap();
                model.insert(k, v);
            }
            ThreadOp::Get(k) => {
                let got = store.get(&thread_key(t, k)).unwrap();
                assert_eq!(
                    got.as_ref().map(Value::as_bytes),
                    model.get(&k).map(Vec::as_slice),
                    "thread {t}, key {k}"
                );
            }
            ThreadOp::Delete(k) => {
                store.delete(thread_key(t, k)).unwrap();
                model.remove(&k);
            }
            ThreadOp::Batch(ops) => {
                let mut batch = Batch::new();
                for (k, v) in ops {
                    match v {
                        Some(v) => {
                            batch.set(thread_key(t, k), Value::try_from(&v[..]).unwrap());
                            model.insert(k, v);
                        }
                        None => {
                            batch.delete(thread_key(t, k));
                            model.remove(&k);
                        }
                    }
                }
                store.write(batch).unwrap();
            }
            ThreadOp::Compact => store.compact().unwrap(),
        }
    }
    model
}

fn check_models(store: &Store, models: &[HashMap<u8, Vec<u8>>]) {
    for (t, model) in models.iter().enumerate() {
        for k in 0..KEY_SPACE {
            let got = store.get(&thread_key(t as u8, k)).unwrap();
            assert_eq!(
                got.as_ref().map(Value::as_bytes),
                model.get(&k).map(Vec::as_slice),
                "thread {t}, key {k}"
            );
        }
    }
}

/// Small segments, so writes roll into new ones and reads and reopens
/// cross several. Automatic compaction, if on, at half dead with no floor,
/// so it cleans some segments and keeps others.
fn options(auto: bool) -> Options {
    let mut options = Options::default();
    options.segment_bytes = 100;
    options.auto_compact = auto.then(|| AutoCompact::new(50, 0)).flatten();
    options
}

fn live_len(model: &HashMap<u8, Vec<u8>>) -> u64 {
    model
        .iter()
        .map(|(&k, v)| HEADER_LEN + key_bytes(k).len() as u64 + v.len() as u64)
        .sum()
}

fn key_bytes(k: u8) -> [u8; 2] {
    [b'k', k]
}

fn key(k: u8) -> Key {
    Key::try_from(&key_bytes(k)[..]).unwrap()
}

/// Bytes across the log's segment files, `tally.<20 digits>.log`.
fn log_len(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .unwrap()
        .map(Result::unwrap)
        .filter(|e| {
            let name = e.file_name();
            let name = name.to_string_lossy();
            name.len() == 30 && name.starts_with("tally.") && name.ends_with(".log")
        })
        .map(|e| e.metadata().unwrap().len())
        .sum()
}

fn cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(48)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(cases()))]

    #[test]
    fn store_matches_a_hashmap(auto in any::<bool>(), ops in prop::collection::vec(op(), 1..40)) {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open_with(tmp.path(), options(auto)).unwrap();
        let mut model: HashMap<u8, Vec<u8>> = HashMap::new();
        let mut expected_len = 0u64;

        for op in ops {
            match op {
                Op::Set(k, v) => {
                    store.set(key(k), Value::try_from(&v[..]).unwrap()).unwrap();
                    expected_len += HEADER_LEN + key_bytes(k).len() as u64 + v.len() as u64;
                    model.insert(k, v);
                }
                Op::Get(k) => {
                    let got = store.get(&key(k)).unwrap();
                    prop_assert_eq!(
                        got.as_ref().map(Value::as_bytes),
                        model.get(&k).map(Vec::as_slice)
                    );
                }
                Op::Delete(k) => {
                    store.delete(key(k)).unwrap();
                    if model.remove(&k).is_some() {
                        expected_len += HEADER_LEN + key_bytes(k).len() as u64;
                    }
                }
                Op::Reopen => {
                    drop(store);
                    store = Store::open_with(tmp.path(), options(auto)).unwrap();
                }
                Op::Batch(ops) => {
                    let mut batch = Batch::new();
                    let mut written = 0u64;
                    for (k, v) in ops {
                        match v {
                            Some(v) => {
                                batch.set(key(k), Value::try_from(&v[..]).unwrap());
                                written += HEADER_LEN + key_bytes(k).len() as u64 + v.len() as u64;
                                model.insert(k, v);
                            }
                            None => {
                                batch.delete(key(k));
                                if model.remove(&k).is_some() {
                                    written += HEADER_LEN + key_bytes(k).len() as u64;
                                }
                            }
                        }
                    }
                    store.write(batch).unwrap();
                    if written > 0 {
                        expected_len += HEADER_LEN + 8 + written;
                    }
                }
            }
            if auto {
                prop_assert!(log_len(tmp.path()) >= live_len(&model));
            } else {
                prop_assert_eq!(log_len(tmp.path()), expected_len);
            }
        }

        drop(store);
        let store = Store::open_with(tmp.path(), options(auto)).unwrap();
        for k in 0..KEY_SPACE {
            let got = store.get(&key(k)).unwrap();
            prop_assert_eq!(
                got.as_ref().map(Value::as_bytes),
                model.get(&k).map(Vec::as_slice)
            );
        }
    }

    #[test]
    fn threads_on_one_store_match_their_own_hashmaps(
        auto in any::<bool>(),
        ops in prop::collection::vec(prop::collection::vec(thread_op(), 1..30), THREADS as usize),
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open_with(tmp.path(), options(auto)).unwrap();
        let models: Vec<HashMap<u8, Vec<u8>>> = thread::scope(|scope| {
            let threads: Vec<_> = ops
                .into_iter()
                .enumerate()
                .map(|(t, ops)| {
                    let store = &store;
                    scope.spawn(move || run_thread(store, t as u8, ops))
                })
                .collect();
            threads.into_iter().map(|t| t.join().unwrap()).collect()
        });
        check_models(&store, &models);
        store.verify().unwrap();
        drop(store);
        let store = Store::open_with(tmp.path(), options(auto)).unwrap();
        check_models(&store, &models);
        store.verify().unwrap();
    }
}
