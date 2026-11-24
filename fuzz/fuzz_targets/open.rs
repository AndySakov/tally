//! Builds a log from arbitrary records, optionally damages it, and opens it.
//!
//! - Undamaged: `open` succeeds and every key reads back the value the
//!   records describe.
//! - Truncated anywhere, or zero bytes appended: `open` succeeds with the
//!   state of the complete records and truncates the log after the last one.
//! - One byte changed: the record holding it fails its header checksum (if
//!   the byte is in its first 15 bytes) or its payload checksum. CRC32C
//!   catches any change confined to 32 bits. If every byte that checksum
//!   covers, through to the end of the log, is zero, the record is a torn
//!   tail and is truncated; otherwise `open` fails with corruption at the
//!   start of the record and leaves the log alone.

#![no_main]

use std::collections::HashMap;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use tally::fuzz::{HEADER_LEN, Record, encode};
use tally::{CorruptionReason, Error, Key, Store, Value};

#[derive(Arbitrary, Debug)]
enum Op {
    Set { key: u8, value: Vec<u8> },
    Delete { key: u8 },
}

#[derive(Arbitrary, Debug)]
enum Damage {
    None,
    Truncate { at: u32 },
    ChangeByte { at: u32, mask: u8 },
    ZeroTail { len: u8 },
}

#[derive(Arbitrary, Debug)]
struct Input {
    ops: Vec<Op>,
    damage: Damage,
}

type State = HashMap<u8, Vec<u8>>;

fn key(k: u8) -> Key {
    Key::try_from(&[k][..]).unwrap()
}

enum Expect {
    /// `open` succeeds with this state and leaves the log `len` bytes long.
    Opens { state: State, len: usize },
    Corrupt {
        offset: u64,
        reason: CorruptionReason,
    },
}

fuzz_target!(|input: Input| {
    let mut log = Vec::new();
    // `boundaries[i]` is the log length after `i` records; `states[i]` is the
    // key-value state after them.
    let mut boundaries = vec![0];
    let mut states = vec![State::new()];
    let mut state = State::new();

    for op in input.ops {
        let record = match op {
            Op::Set { key: k, value } => {
                let Ok(v) = Value::try_from(&value[..]) else {
                    continue;
                };
                state.insert(k, value);
                Record::Set {
                    key: key(k),
                    value: v,
                }
            }
            Op::Delete { key: k } => {
                state.remove(&k);
                Record::Delete { key: key(k) }
            }
        };
        encode(&record, &mut log);
        boundaries.push(log.len());
        states.push(state.clone());
    }

    let expect = match input.damage {
        Damage::None => Expect::Opens {
            state,
            len: log.len(),
        },
        Damage::Truncate { at } => {
            let len = at as usize % (log.len() + 1);
            log.truncate(len);
            let i = boundaries.iter().rposition(|&b| b <= len).unwrap();
            Expect::Opens {
                state: states.swap_remove(i),
                len: boundaries[i],
            }
        }
        Damage::ChangeByte { at, mask } => {
            if log.is_empty() || mask == 0 {
                return;
            }
            let i = at as usize % log.len();
            log[i] ^= mask;
            let r = boundaries.iter().rposition(|&b| b <= i).unwrap();
            let start = boundaries[r];
            let (covered_from, reason) = if i - start < HEADER_LEN {
                (start, CorruptionReason::HeaderChecksum)
            } else {
                (start + HEADER_LEN, CorruptionReason::PayloadChecksum)
            };
            if log[covered_from..].iter().all(|&b| b == 0) {
                Expect::Opens {
                    state: states.swap_remove(r),
                    len: start,
                }
            } else {
                Expect::Corrupt {
                    offset: start as u64,
                    reason,
                }
            }
        }
        Damage::ZeroTail { len } => {
            let end = log.len();
            log.resize(end + len as usize, 0);
            Expect::Opens { state, len: end }
        }
    };

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tally.log");
    std::fs::write(&path, &log).unwrap();
    let opened = Store::open(dir.path());

    match expect {
        Expect::Opens { state, len } => {
            let store = opened.expect("a log with only a torn tail must open");
            for k in 0..=u8::MAX {
                let got = store.get(&key(k)).expect("a recovered log must read back");
                assert_eq!(
                    got.as_ref().map(Value::as_bytes),
                    state.get(&k).map(Vec::as_slice)
                );
            }
            let on_disk = std::fs::metadata(&path).unwrap().len();
            assert_eq!(
                on_disk, len as u64,
                "open must truncate after the last record"
            );
        }
        Expect::Corrupt { offset, reason } => {
            match opened {
                Err(Error::Corruption {
                    offset: o,
                    reason: r,
                }) if o == offset && r == reason => {}
                other => panic!(
                    "expected corruption at {offset} ({reason}), got {:?}",
                    other.err()
                ),
            }
            assert_eq!(
                std::fs::read(&path).unwrap(),
                log,
                "a failed open must not touch the log"
            );
        }
    }
});
