//! Builds a log from arbitrary records and batches, optionally damages it,
//! and opens it. A batch is one unit: applied whole or not at all, and any
//! error inside it is reported at its start.
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
    Batch(Vec<Inner>),
}

#[derive(Arbitrary, Debug)]
enum Inner {
    Set { key: u8, value: Vec<u8> },
    Delete { key: u8 },
}

/// The record for one set or delete, applied to `state`. `None` for a set
/// whose value is not a valid `Value`.
fn record(op: Inner, state: &mut State) -> Option<Record> {
    match op {
        Inner::Set { key: k, value } => {
            let v = Value::try_from(&value[..]).ok()?;
            state.insert(k, value);
            Some(Record::Set {
                key: key(k),
                value: v,
            })
        }
        Inner::Delete { key: k } => {
            state.remove(&k);
            Some(Record::Delete { key: key(k) })
        }
    }
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
    // `boundaries[i]` is the log length after `i` units (a record or a whole
    // batch); `states[i]` is the key-value state after them. `starts` holds
    // each record's start and the start of the unit it belongs to.
    let mut boundaries = vec![0];
    let mut states = vec![State::new()];
    let mut starts: Vec<(usize, usize)> = Vec::new();
    let mut state = State::new();

    for op in input.ops {
        let unit = log.len();
        match op {
            Op::Set { key, value } => {
                let Some(record) = record(Inner::Set { key, value }, &mut state) else {
                    continue;
                };
                starts.push((unit, unit));
                encode(&record, &mut log);
            }
            Op::Delete { key } => {
                let record = record(Inner::Delete { key }, &mut state).unwrap();
                starts.push((unit, unit));
                encode(&record, &mut log);
            }
            Op::Batch(inner) => {
                let mut body = Vec::new();
                let mut inner_starts = Vec::new();
                for op in inner {
                    if let Some(record) = record(op, &mut state) {
                        inner_starts.push(body.len());
                        encode(&record, &mut body);
                    }
                }
                starts.push((unit, unit));
                encode(
                    &Record::Batch {
                        body_len: body.len() as u64,
                    },
                    &mut log,
                );
                let body_start = log.len();
                starts.extend(inner_starts.into_iter().map(|s| (body_start + s, unit)));
                log.extend_from_slice(&body);
            }
        }
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
            let &(start, unit) = starts.iter().rfind(|&&(s, _)| s <= i).unwrap();
            let r = boundaries.iter().position(|&b| b == unit).unwrap();
            let (covered_from, reason) = if i - start < HEADER_LEN {
                (start, CorruptionReason::HeaderChecksum)
            } else {
                (start + HEADER_LEN, CorruptionReason::PayloadChecksum)
            };
            if log[covered_from..].iter().all(|&b| b == 0) {
                Expect::Opens {
                    state: states.swap_remove(r),
                    len: unit,
                }
            } else {
                Expect::Corrupt {
                    offset: unit as u64,
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
