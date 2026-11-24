//! Coverage-guided version of `tests/model.rs`: arbitrary operations against
//! a `Store` and a `HashMap`, which must agree after every step and across
//! reopens, with the log exactly as long as the writes that reached it.
//! fsync is skipped under `cfg(fuzzing)`.

#![no_main]

use std::collections::HashMap;
use std::path::Path;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use tally::fuzz::HEADER_LEN;
use tally::{Batch, Key, Options, Store, Value};

/// Few distinct keys, so overwrites and deletes hit live keys often.
const KEY_SPACE: u8 = 8;

#[derive(Arbitrary, Debug)]
enum Op {
    Set(u8, Vec<u8>),
    Get(u8),
    Delete(u8),
    Reopen,
    Compact,
    /// Sets (`Some`) and deletes (`None`) written as one batch.
    Batch(Vec<(u8, Option<Vec<u8>>)>),
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

fuzz_target!(|ops: Vec<Op>| {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open_with(dir.path(), options()).unwrap();
    let mut model: HashMap<u8, Vec<u8>> = HashMap::new();
    let mut expected_len = 0u64;

    for op in ops {
        match op {
            Op::Set(k, v) => {
                let k = k % KEY_SPACE;
                let Ok(value) = Value::try_from(&v[..]) else {
                    continue;
                };
                store.set(key(k), value).unwrap();
                expected_len += (HEADER_LEN + key_bytes(k).len() + v.len()) as u64;
                model.insert(k, v);
            }
            Op::Get(k) => {
                let k = k % KEY_SPACE;
                let got = store.get(&key(k)).unwrap();
                assert_eq!(
                    got.as_ref().map(Value::as_bytes),
                    model.get(&k).map(Vec::as_slice)
                );
            }
            Op::Delete(k) => {
                let k = k % KEY_SPACE;
                store.delete(key(k)).unwrap();
                if model.remove(&k).is_some() {
                    expected_len += (HEADER_LEN + key_bytes(k).len()) as u64;
                }
            }
            Op::Reopen => {
                drop(store);
                store = Store::open_with(dir.path(), options()).unwrap();
            }
            Op::Batch(ops) => {
                let mut batch = Batch::new();
                let mut written = 0u64;
                for (k, v) in ops {
                    let k = k % KEY_SPACE;
                    match v {
                        Some(v) => {
                            let Ok(value) = Value::try_from(&v[..]) else {
                                continue;
                            };
                            batch.set(key(k), value);
                            written += (HEADER_LEN + key_bytes(k).len() + v.len()) as u64;
                            model.insert(k, v);
                        }
                        None => {
                            batch.delete(key(k));
                            if model.remove(&k).is_some() {
                                written += (HEADER_LEN + key_bytes(k).len()) as u64;
                            }
                        }
                    }
                }
                store.write(batch).unwrap();
                if written > 0 {
                    expected_len += (HEADER_LEN + 8) as u64 + written;
                }
            }
            Op::Compact => {
                store.compact().unwrap();
                expected_len = model
                    .iter()
                    .map(|(&k, v)| (HEADER_LEN + key_bytes(k).len() + v.len()) as u64)
                    .sum();
            }
        }
        assert_eq!(log_len(dir.path()), expected_len);
    }

    drop(store);
    let store = Store::open_with(dir.path(), options()).unwrap();
    for k in 0..KEY_SPACE {
        let got = store.get(&key(k)).unwrap();
        assert_eq!(
            got.as_ref().map(Value::as_bytes),
            model.get(&k).map(Vec::as_slice)
        );
    }
});
