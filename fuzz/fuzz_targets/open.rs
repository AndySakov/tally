//! Builds a log from arbitrary records, optionally damages it, and opens it.
//!
//! - Undamaged: `open` succeeds and every key reads back the value the
//!   records describe.
//! - One byte changed: `open` fails with corruption at the start of the
//!   record holding the byte. CRC32C catches any change confined to 32 bits,
//!   and the byte is covered by the header checksum if it lies in the first
//!   15 bytes of its record, or the payload checksum otherwise.
//! - Truncated at a record boundary: `open` succeeds with the state of the
//!   records that remain.
//! - Truncated mid-record: `open` fails with `UnexpectedEof`.
//! - Zero bytes appended: fewer than a header fail with `UnexpectedEof`; a
//!   header or more fail with a header checksum error at the old end.
//!
//! The last two are milestone 1 behaviour. In milestone 2 a torn or
//! zero-filled tail is truncated, and `open` must succeed with the state of
//! the complete records.

#![no_main]

use std::collections::HashMap;
use std::io::ErrorKind;

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
    Opens(State),
    Torn,
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
        Damage::None => Expect::Opens(state),
        Damage::Truncate { at } => {
            let len = at as usize % (log.len() + 1);
            log.truncate(len);
            match boundaries.iter().position(|&b| b == len) {
                Some(i) => Expect::Opens(states.swap_remove(i)),
                None => Expect::Torn,
            }
        }
        Damage::ChangeByte { at, mask } => {
            if log.is_empty() || mask == 0 {
                return;
            }
            let i = at as usize % log.len();
            log[i] ^= mask;
            let start = *boundaries.iter().rev().find(|&&b| b <= i).unwrap();
            let reason = if i - start < HEADER_LEN {
                CorruptionReason::HeaderChecksum
            } else {
                CorruptionReason::PayloadChecksum
            };
            Expect::Corrupt {
                offset: start as u64,
                reason,
            }
        }
        Damage::ZeroTail { len } => {
            if len == 0 {
                return;
            }
            let end = log.len();
            log.resize(end + len as usize, 0);
            if (len as usize) < HEADER_LEN {
                Expect::Torn
            } else {
                Expect::Corrupt {
                    offset: end as u64,
                    reason: CorruptionReason::HeaderChecksum,
                }
            }
        }
    };

    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("tally.log"), &log).unwrap();
    let opened = Store::open(dir.path());

    match expect {
        Expect::Opens(state) => {
            let store = opened.expect("an intact log must open");
            for k in 0..=u8::MAX {
                let got = store.get(&key(k)).expect("an intact log must read back");
                assert_eq!(
                    got.as_ref().map(Value::as_bytes),
                    state.get(&k).map(Vec::as_slice)
                );
            }
        }
        Expect::Torn => match opened {
            Err(Error::Io(e)) if e.kind() == ErrorKind::UnexpectedEof => {}
            other => panic!(
                "a torn tail must fail with UnexpectedEof, got {:?}",
                other.err()
            ),
        },
        Expect::Corrupt { offset, reason } => match opened {
            Err(Error::Corruption {
                offset: o,
                reason: r,
            }) if o == offset && r == reason => {}
            other => panic!(
                "expected corruption at {offset} ({reason}), got {:?}",
                other.err()
            ),
        },
    }
});
