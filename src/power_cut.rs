//! Power-cut simulation (DESIGN.md, Testing): run a workload on the
//! in-memory disk, cut the power before every step it takes, and for every
//! way the cut can leave the disk, check that `open` recovers without losing
//! an acknowledged write. Recovery itself is cut at every step too.
//!
//! Unlike the kill test this sees fsync: a write that was never synced is
//! gone after the cut, so a missing `sync` fails here.

use std::num::NonZero;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread;

use crate::fs::{Fs, LogFile};
use crate::mem_fs::{Cut, Disk, MemFs};
use crate::model::{self, Op, State};
use crate::record::{Place, Record, encode};
use crate::segment::{FILE_HEADER_LEN, log_name, segment_file};
use crate::store::LOG_FILE_NAME;
use crate::{AutoCompact, Key, Options, Repaired, SideFile, Store, Value};

const DIR: &str = "/db";

/// Compacts whenever half the log is dead, however small, so the workload
/// compacts inside writes and recovery as well as when asked.
fn options() -> Options {
    Options {
        auto_compact: AutoCompact::new(50, 0),
        segment_bytes: 64,
    }
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
        Group(vec![
            vec![(5, Some(b"g5".to_vec()))],
            vec![(4, None)],
            vec![(0, None)],
            vec![(6, Some(b"g6".to_vec())), (5, None)],
            vec![(4, Some(b"g4".to_vec()))],
        ]),
        Batch(vec![(6, None), (4, Some(b"four".to_vec())), (2, None)]),
        Compact,
        Set(0, b"zero".to_vec()),
        Set(1, b"one again".to_vec()),
        Compact,
        Set(2, vec![b'w'; 100]),
        Batch(vec![(2, None), (3, None)]),
    ]
}

fn key(k: u8) -> Key {
    model::key(0, k)
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
/// with the one in progress either applied or not, a group as a whole. One
/// past the last step is a cut after the workload finished. `None` beyond
/// that.
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
        if let Op::Reopen = op {
            drop(store);
            store = Store::open_in(fs.clone(), dir, options()).unwrap();
        } else {
            model::run(&store, 0, &mut state, &op);
        }
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

/// Each thread takes the next step to cut at until the workload runs out:
/// every step reruns the workload on a disk of its own.
#[test]
fn no_acknowledged_write_is_lost_to_a_power_cut_at_any_step() {
    let next = AtomicU64::new(1);
    let checked = AtomicUsize::new(0);
    let threads = thread::available_parallelism().map_or(1, NonZero::get);
    thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                loop {
                    let step = next.fetch_add(1, Ordering::Relaxed);
                    let Some((snapshot, candidates)) = cut_workload(step) else {
                        break;
                    };
                    for cut in snapshot.cuts() {
                        let context = format!("cut before step {step} {cut:?}");
                        let disks = check_recovery(&snapshot, cut, &candidates, 1, &context);
                        checked.fetch_add(disks, Ordering::Relaxed);
                    }
                }
            });
        }
    });
    let checked = checked.into_inner();
    assert!(checked > 50, "only {checked} disks checked");
    println!("power cut: {checked} disks checked");
}

/// The records of the first segment: a Set of "a", a Delete of "a" with
/// its key corrupted at 22, then a Set of "b".
fn log_with_a_corrupt_middle() -> Vec<u8> {
    let records = [
        Record::Set {
            key: Key::try_from(&b"a"[..]).unwrap(),
            value: Value::try_from(&b"hi"[..]).unwrap(),
        },
        Record::Delete {
            key: Key::try_from(&b"a"[..]).unwrap(),
        },
        Record::Set {
            key: Key::try_from(&b"b"[..]).unwrap(),
            value: Value::try_from(&b"2"[..]).unwrap(),
        },
    ];
    let mut bytes = Vec::new();
    for record in &records {
        encode(record, Place::Top, bytes.len() as u64, &mut bytes);
    }
    bytes[22 + 19] ^= 0x01;
    bytes
}

#[test]
fn a_power_cut_during_repair_never_loses_a_byte() {
    let dir = Path::new(DIR);
    let log_path = dir.join(log_name(0));
    let side_path = dir.join(format!("{LOG_FILE_NAME}.corrupt-22"));
    let records = log_with_a_corrupt_middle();
    let original = segment_file(0, &records);
    let kept = &segment_file(0, &records[..22])[..];
    let moved = &records[22..];

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
                        side_file: Some(SideFile {
                            path: side_path.clone(),
                            unreadable: vec![],
                        }),
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

#[test]
fn a_power_cut_while_repairing_a_segment_header_never_loses_a_byte() {
    let dir = Path::new(DIR);
    let damaged_path = dir.join(log_name(0));
    let side_path = dir.join(format!("{LOG_FILE_NAME}.corrupt-0"));
    let set = |k: &[u8], v: &[u8], at: u64| {
        let mut bytes = Vec::new();
        let record = Record::Set {
            key: Key::try_from(k).unwrap(),
            value: Value::try_from(v).unwrap(),
        };
        encode(&record, Place::Top, at, &mut bytes);
        bytes
    };
    let mut original = segment_file(0, &set(b"a", b"hi", 0));
    original[14] ^= 0x01;
    let empty = segment_file(0, b"");
    let later = segment_file(22, &set(b"b", b"2", 22));
    let put = |fs: &MemFs| {
        fs.put(&damaged_path, &original);
        fs.put(&dir.join(log_name(22)), &later);
    };

    let fs = MemFs::new();
    put(&fs);
    Store::repair_in(&fs, dir).unwrap();
    let steps = fs.steps();

    for step in 1..=steps {
        let fs = MemFs::new();
        put(&fs);
        fs.cut_before(step);
        Store::repair_in(&fs, dir).unwrap();
        let snapshot = fs.snapshot().unwrap();
        for cut in snapshot.cuts() {
            let context = format!("repair cut before step {step} {cut:?}");
            let fs = MemFs::after(&snapshot, cut);
            let log = fs.read(&damaged_path).unwrap();
            if log != original {
                assert_eq!(
                    log, empty,
                    "{context}: segment replaced with the wrong bytes"
                );
                assert_eq!(
                    fs.read(&side_path).as_deref(),
                    Some(&original[..]),
                    "{context}: the segment was replaced before the side file was durable"
                );
            }

            let rerun = Store::repair_in(&fs, dir).unwrap();
            assert_eq!(rerun.is_some(), log == original, "{context}");
            assert_eq!(fs.read(&damaged_path).unwrap(), empty, "{context}");
            assert_eq!(fs.read(&side_path).unwrap(), original, "{context}");
            let store = Store::open_in(fs.clone(), dir, options()).unwrap();
            assert_eq!(
                store.get(&Key::try_from(&b"b"[..]).unwrap()).unwrap(),
                Some(Value::try_from(&b"2"[..]).unwrap()),
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

    let log = fs.open_existing(&dir.join(log_name(0))).unwrap();
    let end = log.size().unwrap();
    let mut unsynced = Vec::new();
    encode(
        &Record::Set {
            key: key(2),
            value: Value::try_from(&b"unsynced"[..]).unwrap(),
        },
        Place::Top,
        end - FILE_HEADER_LEN,
        &mut unsynced,
    );
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
