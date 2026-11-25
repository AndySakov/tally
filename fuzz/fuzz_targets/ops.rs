//! Coverage-guided version of `tests/model.rs`: arbitrary operations against
//! a `Store` and a `HashMap`, which must agree after every step and across
//! reopens. With automatic compaction off, the log must be exactly as long
//! as the writes that reached it; with it on, at least as long as the live
//! records.
//! On the in-memory disk: on the real one, file syscalls took most of the
//! time.

#![no_main]

use std::collections::HashMap;
use std::path::Path;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use tally::fs::Fs;
use tally::fuzz::{HEADER_LEN, MemFs, open_in_memory};
use tally::{AutoCompact, Batch, Key, Options, Value};

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
        .map(|(&k, v)| (HEADER_LEN + key_bytes(k).len() + v.len()) as u64)
        .sum()
}

fn key_bytes(k: u8) -> [u8; 2] {
    [b'k', k]
}

fn key(k: u8) -> Key {
    Key::try_from(&key_bytes(k)[..]).unwrap()
}

/// Bytes across the log's segment files, `tally.<20 digits>.log`.
fn log_len(fs: &MemFs, dir: &Path) -> u64 {
    fs.list(dir)
        .unwrap()
        .into_iter()
        .filter(|name| name.len() == 30 && name.starts_with("tally.") && name.ends_with(".log"))
        .map(|name| fs.read(&dir.join(name)).unwrap().len() as u64)
        .sum()
}

fuzz_target!(|input: (bool, Vec<Op>)| {
    let (auto, ops) = input;
    let fs = MemFs::new();
    let dir = Path::new("/db");
    let mut store = open_in_memory(fs.clone(), dir, options(auto)).unwrap();
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
                store = open_in_memory(fs.clone(), dir, options(auto)).unwrap();
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
                expected_len = live_len(&model);
            }
        }
        if auto {
            assert!(log_len(&fs, dir) >= live_len(&model));
        } else {
            assert_eq!(log_len(&fs, dir), expected_len);
        }
    }

    drop(store);
    let store = open_in_memory(fs.clone(), dir, options(auto)).unwrap();
    for k in 0..KEY_SPACE {
        let got = store.get(&key(k)).unwrap();
        assert_eq!(
            got.as_ref().map(Value::as_bytes),
            model.get(&k).map(Vec::as_slice)
        );
    }
});
