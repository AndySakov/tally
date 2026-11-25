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
use crate::segment::{hint_name, log_name};
use crate::store::LOG_FILE_NAME;
use crate::{AutoCompact, Batch, Key, Options, Repaired, Store, Value};

const DIR: &str = "/db";

type State = BTreeMap<u8, Vec<u8>>;

/// Compacts whenever half the log is dead, however small, so the workload
/// compacts inside writes and recovery as well as when asked.
fn options() -> Options {
    Options {
        auto_compact: AutoCompact::new(50, 0),
        segment_bytes: 64,
    }
}

#[derive(Debug, Clone)]
enum Op {
    Set(u8, Vec<u8>),
    Delete(u8),
    Reopen,
    Compact,
    /// Sets (`Some`) and deletes (`None`), as one batch.
    Batch(Vec<(u8, Option<Vec<u8>>)>),
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
        Compact,
        Batch(vec![
            (5, Some(b"five".to_vec())),
            (1, None),
            (6, Some(vec![b'z'; 200])),
            (5, Some(b"FIVE".to_vec())),
        ]),
        Reopen,
        Set(3, b"three".to_vec()),
        Delete(1),
        Set(2, b"two again".to_vec()),
        Reopen,
        Compact,
        Compact,
        Set(4, vec![b'y'; 40]),
        Batch(vec![(6, None), (4, Some(b"four".to_vec())), (2, None)]),
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
        Op::Batch(ops) => {
            for (k, v) in ops {
                match v {
                    Some(v) => state.insert(*k, v.clone()),
                    None => state.remove(k),
                };
            }
        }
        Op::Reopen | Op::Compact => {}
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
/// with the one in progress either applied or not. One past the last step
/// is a cut after the workload finished. `None` beyond that.
fn cut_workload(step: u64) -> Option<(Disk, Vec<State>)> {
    let fs = MemFs::new();
    fs.cut_before(step);
    let dir = Path::new(DIR);
    // states[i] is the state after the first i operations, the open counting
    // as the first; ends[i] is the step count once operation i has finished.
    let mut states = vec![State::new()];
    let mut ends = vec![0];

    let mut store = Store::open_in(fs.clone(), dir, options()).unwrap();
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
                store = Store::open_in(fs.clone(), dir, options()).unwrap();
            }
            Op::Compact => store.compact().unwrap(),
            Op::Batch(ops) => {
                let mut batch = Batch::new();
                for (k, v) in ops {
                    match v {
                        Some(v) => batch.set(key(*k), Value::try_from(&v[..]).unwrap()),
                        None => batch.delete(key(*k)),
                    };
                }
                store.write(batch).unwrap();
            }
        }
        apply(&mut state, &op);
        states.push(state.clone());
        ends.push(fs.steps());
    }

    if step == fs.steps() + 1 {
        return Some((fs.disk(), vec![state]));
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
        drop(Store::open_in(fs.clone(), dir, options()));
        for step in 1..=fs.steps() {
            let fs = MemFs::after(snapshot, cut);
            fs.cut_before(step);
            drop(Store::open_in(fs.clone(), dir, options()));
            let inner = fs.snapshot().unwrap();
            for inner_cut in inner.cuts() {
                let context = format!("{context}, recovery cut before step {step} {inner_cut:?}");
                checked += check_recovery(&inner, inner_cut, candidates, depth - 1, &context);
            }
        }
    }

    let fs = MemFs::after(snapshot, cut);
    let store = Store::open_in(fs.clone(), dir, options())
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
    let reopened = Store::open_in(fs.clone(), dir, options()).unwrap();
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
    bytes[corrupt_at + 19] ^= 0x01;
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
    let log_path = dir.join(log_name(0));
    let side_path = dir.join(format!("{LOG_FILE_NAME}.corrupt-22"));
    let original = log_with_a_corrupt_middle();
    let (kept, moved) = original.split_at(22);

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
                        offset: 22,
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
            let store = Store::open_in(fs.clone(), dir, options()).unwrap();
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
    let store = Store::open_in(fs.clone(), dir, options()).unwrap();
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
    let log = fs.open_existing(&dir.join(log_name(0))).unwrap();
    let end = log.size().unwrap();
    log.write_all_at(&unsynced, end).unwrap();
    drop(log);

    let store = Store::open_in(fs.clone(), dir, options()).unwrap();
    let served = read_state(&store);
    assert_eq!(
        served,
        State::from([(1, b"synced".to_vec()), (2, b"unsynced".to_vec())])
    );
    drop(store);

    let disk = fs.disk();
    for cut in disk.cuts() {
        let after = MemFs::after(&disk, cut);
        let store = Store::open_in(after.clone(), dir, options()).unwrap();
        assert_eq!(read_state(&store), served, "{cut:?}");
    }
}

/// A hint for more log than there is gets removed. If that removal were
/// not durable, writes that grow the log past the hint's length would make
/// it look valid again after a power cut.
#[test]
fn a_removed_hint_stays_removed_after_a_power_cut() {
    let dir = Path::new(DIR);
    let fs = MemFs::new();
    let store = Store::open_in(fs.clone(), dir, Options::default()).unwrap();
    store
        .set(key(1), Value::try_from(&b"hi"[..]).unwrap())
        .unwrap();
    store
        .set(key(2), Value::try_from(&b"2"[..]).unwrap())
        .unwrap();
    store.compact().unwrap();
    drop(store);
    let compacted = 23 + 22;
    let hint = fs.read(&dir.join(hint_name(compacted))).unwrap();
    let log = fs.read(&dir.join(log_name(compacted))).unwrap();
    assert_eq!(log.len(), 23 + 22);

    let fs = MemFs::new();
    fs.put(&dir.join(log_name(0)), &log[..23]);
    fs.put(&dir.join(hint_name(0)), &hint);
    let store = Store::open_in(fs.clone(), dir, Options::default()).unwrap();
    store
        .set(key(3), Value::try_from(&b"3"[..]).unwrap())
        .unwrap();
    drop(store);
    assert_eq!(fs.read(&dir.join(log_name(0))).unwrap().len(), 23 + 22);

    let disk = fs.disk();
    for cut in disk.cuts() {
        let store = Store::open_in(MemFs::after(&disk, cut), dir, Options::default()).unwrap();
        assert_eq!(
            read_state(&store),
            State::from([(1, b"hi".to_vec()), (3, b"3".to_vec())]),
            "{cut:?}"
        );
    }
}

/// Turning a log from before segments, with its hint, into the first
/// segment renames both and rewrites `tally.log`. A power cut at any point
/// of it must leave a store that opens with every key, and never one an
/// earlier tally would open as empty.
#[test]
fn a_power_cut_while_moving_to_segments_loses_nothing() {
    let dir = Path::new(DIR);
    let fs = MemFs::new();
    let store = Store::open_in(fs.clone(), dir, Options::default()).unwrap();
    store
        .set(key(1), Value::try_from(&b"hi"[..]).unwrap())
        .unwrap();
    store
        .set(key(2), Value::try_from(&b"2"[..]).unwrap())
        .unwrap();
    store.compact().unwrap();
    drop(store);
    let log = fs.read(&dir.join(log_name(45))).unwrap();
    let hint = fs.read(&dir.join(hint_name(45))).unwrap();
    let legacy = || {
        let fs = MemFs::new();
        fs.put(&dir.join(LOG_FILE_NAME), &log);
        fs.put(&dir.join(crate::hint::HINT_FILE_NAME), &hint);
        fs
    };
    let expected = State::from([(1, b"hi".to_vec()), (2, b"2".to_vec())]);

    let fs = legacy();
    drop(Store::open_in(fs.clone(), dir, Options::default()).unwrap());
    let steps = fs.steps();
    let mut checked = 0;
    for step in 1..=steps {
        let fs = legacy();
        fs.cut_before(step);
        drop(Store::open_in(fs.clone(), dir, Options::default()).unwrap());
        let snapshot = fs.snapshot().unwrap();
        for cut in snapshot.cuts() {
            let context = format!("cut before step {step} {cut:?}");
            let after = MemFs::after(&snapshot, cut);
            let legacy_log = after.read(&dir.join(LOG_FILE_NAME));
            assert!(
                legacy_log.as_deref() == Some(&log[..])
                    || legacy_log.as_deref() == Some(&crate::store::marker()[..])
                        && after.read(&dir.join(log_name(0))).as_deref() == Some(&log[..]),
                "{context}: an earlier version would not find the records, or would not be refused"
            );
            let store = Store::open_in(after.clone(), dir, Options::default()).unwrap();
            assert_eq!(read_state(&store), expected, "{context}");
            drop(store);
            assert_eq!(
                after.read(&dir.join(LOG_FILE_NAME)).as_deref(),
                Some(&crate::store::marker()[..]),
                "{context}"
            );
            checked += 1;
        }
    }
    println!("moving to segments: {checked} disks checked");
}
