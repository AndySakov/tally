//! Builds a log from arbitrary records and batches, optionally damages it,
//! and opens it. A unit is a record on its own or a batch: applied whole or
//! not at all, and any error inside it is reported at its start.
//!
//! - Undamaged, or zeros appended: `open` succeeds and every key reads back
//!   the value the records describe.
//! - Truncated anywhere: `open` succeeds with the state of the complete
//!   units, truncates the log after the last one, and saves the bytes it
//!   cut, unless they are all zeros.
//! - Bytes changed or zeroed: the first unit they changed is damaged. If a
//!   later unit's header is intact, `open` fails with corruption at the
//!   damaged unit and leaves the log alone: its header checksum if the
//!   first changed record changed in its first 19 bytes, else its payload
//!   checksum. Otherwise the damage is a torn tail, which `open` truncates
//!   and saves like a cut one.

#![no_main]

use std::collections::HashMap;
use std::path::Path;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use tally::fuzz::{HEADER_LEN, Place, Record, encode, segment_file};
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

#[derive(Arbitrary, Debug)]
enum Damage {
    None,
    Truncate { at: u32 },
    ChangeByte { at: u32, mask: u8 },
    Zero { at: u32, len: u16 },
    ZeroTail { len: u8 },
}

#[derive(Arbitrary, Debug)]
struct Input {
    ops: Vec<Op>,
    damage: Damage,
}

type State = HashMap<u8, Vec<u8>>;

/// The records of one unit, as (start, end), in log order.
struct Unit {
    start: usize,
    end: usize,
    records: Vec<(usize, usize)>,
}

fn key(k: u8) -> Key {
    Key::try_from(&[k][..]).unwrap()
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

fn push(record: &Record, place: Place, log: &mut Vec<u8>) -> (usize, usize) {
    let start = log.len();
    encode(record, place, start as u64, log);
    (start, log.len())
}

enum Expect {
    /// `open` succeeds with this state and cuts the log at `len`.
    Opens { state: State, len: usize },
    Corrupt {
        offset: u64,
        reason: CorruptionReason,
    },
}

fuzz_target!(|input: Input| {
    let mut log = Vec::new();
    let mut units: Vec<Unit> = Vec::new();
    // `states[i]` is the state after the first `i` units.
    let mut states = vec![State::new()];
    let mut state = State::new();

    for op in input.ops {
        let start = log.len();
        let records = match op {
            Op::Set { key, value } => {
                let Some(record) = record(Inner::Set { key, value }, &mut state) else {
                    continue;
                };
                vec![push(&record, Place::Top, &mut log)]
            }
            Op::Delete { key } => {
                let record = record(Inner::Delete { key }, &mut state).unwrap();
                vec![push(&record, Place::Top, &mut log)]
            }
            Op::Batch(inner) => {
                let frame = push(&Record::Batch { body_len: 0 }, Place::Top, &mut log);
                let mut records = vec![frame];
                for op in inner {
                    if let Some(record) = record(op, &mut state) {
                        records.push(push(&record, Place::Inner, &mut log));
                    }
                }
                let body_len = (log.len() - frame.1) as u64;
                let mut sealed = Vec::new();
                encode(
                    &Record::Batch { body_len },
                    Place::Top,
                    start as u64,
                    &mut sealed,
                );
                log[start..frame.1].copy_from_slice(&sealed);
                records
            }
        };
        units.push(Unit {
            start,
            end: log.len(),
            records,
        });
        states.push(state.clone());
    }

    let original = log.clone();
    let opens_at = |units_kept: usize, states: &mut Vec<State>| Expect::Opens {
        state: states.swap_remove(units_kept),
        len: units_kept.checked_sub(1).map_or(0, |i| units[i].end),
    };
    let expect = match input.damage {
        Damage::None => opens_at(units.len(), &mut states),
        Damage::ZeroTail { len } => {
            log.resize(log.len() + len as usize, 0);
            opens_at(units.len(), &mut states)
        }
        Damage::Truncate { at } => {
            log.truncate(at as usize % (log.len() + 1));
            let kept = units.iter().take_while(|u| u.end <= log.len()).count();
            opens_at(kept, &mut states)
        }
        Damage::ChangeByte { .. } | Damage::Zero { .. } if log.is_empty() => return,
        Damage::ChangeByte { at, mask } => {
            log[at as usize % original.len()] ^= mask;
            damaged(&units, &original, &log, &mut states)
        }
        Damage::Zero { at, len } => {
            let at = at as usize % original.len();
            let end = (at + len as usize).min(original.len());
            log[at..end].fill(0);
            damaged(&units, &original, &log, &mut states)
        }
    };

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tally.00000000000000000000.log");
    let file = segment_file(0, &log);
    std::fs::write(&path, &file).unwrap();
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
            assert_eq!(
                std::fs::read(&path).unwrap(),
                segment_file(0, &log[..len]),
                "open must truncate after the last unit"
            );
            let cut = &log[len..];
            let saved = torn_file(dir.path(), len);
            if cut.iter().all(|&b| b == 0) {
                assert_eq!(saved, None, "zeros are not saved");
            } else {
                assert_eq!(saved.as_deref(), Some(cut), "the cut bytes are saved");
            }
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
                file,
                "a failed open must not touch the log"
            );
        }
    }
});

/// What `open` must do with `log`, which is `original` with some bytes
/// changed.
fn damaged(units: &[Unit], original: &[u8], log: &[u8], states: &mut Vec<State>) -> Expect {
    let changed = |(start, end): (usize, usize)| original[start..end] != log[start..end];
    let Some(u) = units.iter().position(|u| changed((u.start, u.end))) else {
        return Expect::Opens {
            state: states.pop().unwrap(),
            len: log.len(),
        };
    };
    let header_intact = |u: &Unit| !changed((u.start, u.start + HEADER_LEN));
    if !units[u + 1..].iter().any(header_intact) {
        return Expect::Opens {
            state: states.swap_remove(u),
            len: units[u].start,
        };
    }
    let &(start, _) = units[u].records.iter().find(|&&r| changed(r)).unwrap();
    let reason = if changed((start, start + HEADER_LEN)) {
        CorruptionReason::HeaderChecksum
    } else {
        CorruptionReason::PayloadChecksum
    };
    Expect::Corrupt {
        offset: units[u].start as u64,
        reason,
    }
}

/// The bytes `open` saved from a torn tail at `offset`, if it saved any.
fn torn_file(dir: &Path, offset: usize) -> Option<Vec<u8>> {
    let prefix = format!("tally.log.torn-{offset}-");
    let mut found = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.starts_with(&prefix));
    let name = found.next()?;
    assert_eq!(found.next(), None, "one side file per torn tail");
    Some(std::fs::read(dir.join(name)).unwrap())
}
