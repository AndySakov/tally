//! Runs random sequences of operations against a `Store` and a `HashMap`
//! and checks they agree after every step, including across reopens.
//!
//! Every `set` and `delete` fsyncs (about 5 ms on macOS), so the default case
//! count is small. For a longer run: `PROPTEST_CASES=2000 cargo test --test model`.

use std::collections::HashMap;
use std::path::Path;

use proptest::prelude::*;
use tally::{Batch, Key, Options, Store, Value};

const HEADER_LEN: u64 = 15;

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

/// Small segments, so writes roll into new ones and reads and reopens
/// cross several.
fn options() -> Options {
    let mut options = Options::default();
    options.segment_bytes = 100;
    options
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
    fn store_matches_a_hashmap(ops in prop::collection::vec(op(), 1..40)) {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open_with(tmp.path(), options()).unwrap();
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
                    store = Store::open_with(tmp.path(), options()).unwrap();
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
            prop_assert_eq!(log_len(tmp.path()), expected_len);
        }

        drop(store);
        let store = Store::open_with(tmp.path(), options()).unwrap();
        for k in 0..KEY_SPACE {
            let got = store.get(&key(k)).unwrap();
            prop_assert_eq!(
                got.as_ref().map(Value::as_bytes),
                model.get(&k).map(Vec::as_slice)
            );
        }
    }
}
