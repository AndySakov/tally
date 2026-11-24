//! Power-cut simulation (DESIGN.md, Testing): run a workload on the
//! in-memory disk, cut the power before every step it takes, and for every
//! way the cut can leave the disk, check that `open` recovers without losing
//! an acknowledged write. Recovery itself is cut at every step too.
//!
//! Unlike the kill test this sees fsync: a write that was never synced is
//! gone after the cut, so a missing `sync` fails here.

use std::collections::BTreeMap;
use std::path::Path;

use crate::fs::{Fs, LogFile};
use crate::mem_fs::{Cut, Disk, MemFs};
use crate::record::{Record, encode};
use crate::store::LOG_FILE_NAME;
use crate::{Key, Repaired, Store, Value};

const DIR: &str = "/db";

type State = BTreeMap<u8, Vec<u8>>;

#[derive(Debug, Clone)]
enum Op {
    Set(u8, Vec<u8>),
    Delete(u8),
    Reopen,
}

fn key(k: u8) -> Key {
    Key::try_from(&[b'k', k][..]).unwrap()
}

fn workload() -> Vec<Op> {
    use Op::*;
    vec![
        Set(1, b"one".to_vec()),
        Set(2, vec![b'x'; 300]),
        Set(1, b"uno".to_vec()),
        Delete(2),
        Delete(3),
        Reopen,
        Set(3, b"three".to_vec()),
        Delete(1),
        Set(2, b"two again".to_vec()),
        Reopen,
        Set(4, vec![b'y'; 40]),
    ]
}

fn apply(state: &mut State, op: &Op) {
    match op {
        Op::Set(k, v) => {
            state.insert(*k, v.clone());
        }
        Op::Delete(k) => {
            state.remove(k);
        }
        Op::Reopen => {}
    }
}

fn read_state(store: &Store<MemFs>) -> State {
    (0..8)
        .filter_map(|k| {
            let value = store
                .get(&key(k))
                .expect("a recovered store must read back");
            value.map(|v| (k, v.as_bytes().to_vec()))
        })
        .collect()
}

/// Runs the workload with power cut before `step`. Returns the disk at the
/// cut and the states it may hold: every operation finished before the cut,
/// with the one in progress either applied or not. `None` once `step` is
/// past the end of the workload.
fn cut_workload(step: u64) -> Option<(Disk, Vec<State>)> {
    let fs = MemFs::new();
    fs.cut_before(step);
    let dir = Path::new(DIR);
    // states[i] is the state after the first i operations, the open counting
    // as the first; ends[i] is the step count once operation i has finished.
    let mut states = vec![State::new()];
    let mut ends = vec![0];

    let mut store = Store::open_in(&fs, dir).unwrap();
    states.push(State::new());
    ends.push(fs.steps());
    let mut state = State::new();
    for op in workload() {
        match &op {
            Op::Set(k, v) => store
                .set(key(*k), Value::try_from(&v[..]).unwrap())
                .unwrap(),
            Op::Delete(k) => store.delete(key(*k)).unwrap(),
            Op::Reopen => {
                drop(store);
                store = Store::open_in(&fs, dir).unwrap();
            }
        }
        apply(&mut state, &op);
        states.push(state.clone());
        ends.push(fs.steps());
    }

    let snapshot = fs.snapshot()?;
    let in_flight = ends.iter().position(|&end| end >= step).unwrap();
    let mut candidates = vec![states[in_flight - 1].clone()];
    if states[in_flight] != candidates[0] {
        candidates.push(states[in_flight].clone());
    }
    Some((snapshot, candidates))
}

/// Opens the store on the disk `cut` leaves of `snapshot` and checks it
/// recovers to one of `candidates`. With `depth > 0`, first cuts power at
/// every step of that recovery and checks the same of each result.
/// Returns how many disks it checked.
fn check_recovery(
    snapshot: &Disk,
    cut: Cut,
    candidates: &[State],
    depth: u32,
    context: &str,
) -> usize {
    let dir = Path::new(DIR);
    let mut checked = 1;
    if depth > 0 {
        let fs = MemFs::after(snapshot, cut);
        drop(Store::open_in(&fs, dir));
        for step in 1..=fs.steps() {
            let fs = MemFs::after(snapshot, cut);
            fs.cut_before(step);
            drop(Store::open_in(&fs, dir));
            let inner = fs.snapshot().unwrap();
            for inner_cut in inner.cuts() {
                let context = format!("{context}, recovery cut before step {step} {inner_cut:?}");
                checked += check_recovery(&inner, inner_cut, candidates, depth - 1, &context);
            }
        }
    }

    let fs = MemFs::after(snapshot, cut);
    let mut store = Store::open_in(&fs, dir)
        .unwrap_or_else(|err| panic!("{context}: open after the cut failed: {err}"));
    let observed = read_state(&store);
    assert!(
        candidates.contains(&observed),
        "{context}: lost an acknowledged write\nobserved: {observed:?}\nallowed: {candidates:?}"
    );

    store
        .set(key(7), Value::try_from(&b"after"[..]).unwrap())
        .unwrap();
    drop(store);
    let mut expected = observed;
    expected.insert(7, b"after".to_vec());
    let reopened = Store::open_in(&fs, dir).unwrap();
    assert_eq!(
        read_state(&reopened),
        expected,
        "{context}: write after recovery"
    );
    checked
}

#[test]
fn no_acknowledged_write_is_lost_to_a_power_cut_at_any_step() {
    let mut checked = 0;
    for step in 1.. {
        let Some((snapshot, candidates)) = cut_workload(step) else {
            break;
        };
        for cut in snapshot.cuts() {
            let context = format!("cut before step {step} {cut:?}");
            checked += check_recovery(&snapshot, cut, &candidates, 1, &context);
        }
    }
    assert!(checked > 50, "only {checked} disks checked");
    println!("power cut: {checked} disks checked");
}

/// SET_A_HI, a Delete of "a" with its key corrupted, then a Set of "b".
fn log_with_a_corrupt_middle() -> Vec<u8> {
    let mut bytes = Vec::new();
    encode(
        &Record::Set {
            key: Key::try_from(&b"a"[..]).unwrap(),
            value: Value::try_from(&b"hi"[..]).unwrap(),
        },
        &mut bytes,
    );
    let corrupt_at = bytes.len();
    encode(
        &Record::Delete {
            key: Key::try_from(&b"a"[..]).unwrap(),
        },
        &mut bytes,
    );
    bytes[corrupt_at + 15] ^= 0x01;
    encode(
        &Record::Set {
            key: Key::try_from(&b"b"[..]).unwrap(),
            value: Value::try_from(&b"2"[..]).unwrap(),
        },
        &mut bytes,
    );
    bytes
}

#[test]
fn a_power_cut_during_repair_never_loses_a_byte() {
    let dir = Path::new(DIR);
    let log_path = dir.join(LOG_FILE_NAME);
    let side_path = dir.join(format!("{LOG_FILE_NAME}.corrupt-18"));
    let original = log_with_a_corrupt_middle();
    let (kept, moved) = original.split_at(18);

    let fs = MemFs::new();
    fs.put(&log_path, &original);
    Store::repair_in(&fs, dir).unwrap();
    let steps = fs.steps();
    assert!(steps > 0);

    for step in 1..=steps {
        let fs = MemFs::new();
        fs.put(&log_path, &original);
        fs.cut_before(step);
        Store::repair_in(&fs, dir).unwrap();
        let snapshot = fs.snapshot().unwrap();
        for cut in snapshot.cuts() {
            let context = format!("repair cut before step {step} {cut:?}");
            let fs = MemFs::after(&snapshot, cut);
            let log = fs.read(&log_path).unwrap();
            let side = fs.read(&side_path);
            if log != original {
                assert_eq!(log, kept, "{context}: log cut at the wrong place");
                assert_eq!(
                    side.as_deref(),
                    Some(moved),
                    "{context}: the log was cut before the side file was durable"
                );
            }

            let rerun = Store::repair_in(&fs, dir).unwrap();
            if log == original {
                assert_eq!(
                    rerun,
                    Some(Repaired {
                        offset: 18,
                        dropped: moved.len() as u64,
                        side_file: side_path.clone(),
                    }),
                    "{context}"
                );
            } else {
                assert_eq!(rerun, None, "{context}");
            }
            assert_eq!(fs.read(&log_path).unwrap(), kept, "{context}");
            assert_eq!(fs.read(&side_path).unwrap(), moved, "{context}");
            let store = Store::open_in(&fs, dir).unwrap();
            assert_eq!(
                store.get(&Key::try_from(&b"a"[..]).unwrap()).unwrap(),
                Some(Value::try_from(&b"hi"[..]).unwrap()),
                "{context}"
            );
        }
    }
}

/// A process wrote a record and died before its fsync, so the record is in
/// the page cache but not on disk. The next `open` reads it and serves it.
/// A power cut must not then take back what `get` already returned: `open`
/// fsyncs the log before reading it for exactly this.
#[test]
fn what_open_reads_survives_a_power_cut() {
    let dir = Path::new(DIR);
    let fs = MemFs::new();
    let mut store = Store::open_in(&fs, dir).unwrap();
    store
        .set(key(1), Value::try_from(&b"synced"[..]).unwrap())
        .unwrap();
    drop(store);

    let mut unsynced = Vec::new();
    encode(
        &Record::Set {
            key: key(2),
            value: Value::try_from(&b"unsynced"[..]).unwrap(),
        },
        &mut unsynced,
    );
    let log = fs.open_existing(&dir.join(LOG_FILE_NAME)).unwrap();
    let end = log.size().unwrap();
    log.write_all_at(&unsynced, end).unwrap();
    drop(log);

    let store = Store::open_in(&fs, dir).unwrap();
    let served = read_state(&store);
    assert_eq!(
        served,
        State::from([(1, b"synced".to_vec()), (2, b"unsynced".to_vec())])
    );
    drop(store);

    let disk = fs.disk();
    for cut in disk.cuts() {
        let after = MemFs::after(&disk, cut);
        let store = Store::open_in(&after, dir).unwrap();
        assert_eq!(read_state(&store), served, "{cut:?}");
    }
}
