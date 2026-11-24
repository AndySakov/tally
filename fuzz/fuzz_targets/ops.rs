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
use tally::{Key, Store, Value};

/// Few distinct keys, so overwrites and deletes hit live keys often.
const KEY_SPACE: u8 = 8;

#[derive(Arbitrary, Debug)]
enum Op {
    Set(u8, Vec<u8>),
    Get(u8),
    Delete(u8),
    Reopen,
    Compact,
}

fn key_bytes(k: u8) -> [u8; 2] {
    [b'k', k]
}

fn key(k: u8) -> Key {
    Key::try_from(&key_bytes(k)[..]).unwrap()
}

fn log_len(dir: &Path) -> u64 {
    std::fs::metadata(dir.join("tally.log")).unwrap().len()
}

fuzz_target!(|ops: Vec<Op>| {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path()).unwrap();
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
                store = Store::open(dir.path()).unwrap();
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
    let store = Store::open(dir.path()).unwrap();
    for k in 0..KEY_SPACE {
        let got = store.get(&key(k)).unwrap();
        assert_eq!(
            got.as_ref().map(Value::as_bytes),
            model.get(&k).map(Vec::as_slice)
        );
    }
});
