//! Coverage-guided version of `tests/model.rs`: arbitrary operations against
//! a `Store` and a `HashMap`, which must agree after every step and across
//! reopens. fsync is skipped under `cfg(fuzzing)`.

#![no_main]

use std::collections::HashMap;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use tally::{Key, Store, Value};

/// Few distinct keys, so overwrites and deletes hit live keys often.
const KEY_SPACE: u8 = 8;

#[derive(Arbitrary, Debug)]
enum Op {
    Set(u8, Vec<u8>),
    Get(u8),
    Delete(u8),
    Reopen,
}

fn key(k: u8) -> Key {
    Key::try_from(&[b'k', k % KEY_SPACE][..]).unwrap()
}

fuzz_target!(|ops: Vec<Op>| {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path()).unwrap();
    let mut model: HashMap<u8, Vec<u8>> = HashMap::new();

    for op in ops {
        match op {
            Op::Set(k, v) => {
                let Ok(value) = Value::try_from(&v[..]) else {
                    continue;
                };
                store.set(key(k), value).unwrap();
                model.insert(k % KEY_SPACE, v);
            }
            Op::Get(k) => {
                let got = store.get(&key(k)).unwrap();
                assert_eq!(
                    got.as_ref().map(Value::as_bytes),
                    model.get(&(k % KEY_SPACE)).map(Vec::as_slice)
                );
            }
            Op::Delete(k) => {
                store.delete(key(k)).unwrap();
                model.remove(&(k % KEY_SPACE));
            }
            Op::Reopen => {
                drop(store);
                store = Store::open(dir.path()).unwrap();
            }
        }
    }
});
