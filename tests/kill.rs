//! Kill test: runs the store in a child process, `kill -9`s it after a
//! random delay, and checks that no acknowledged write was lost.
//!
//! The test binary is its own child: with `TALLY_KILL_CHILD` set it runs a
//! seeded sequence of set, delete, reopen, compact and batch against a store and prints
//! `ack <t> <i>` after operation `i` of stream `t` returns `Ok`. The parent
//! regenerates the same sequences from the seed, so it knows what every ack
//! means. Every other run, the child runs four streams on four threads
//! sharing one store, each with keys of its own, so their writes land in
//! groups (DESIGN.md, Group commit); a reopen in a stream becomes a
//! compaction there, since the threads share the store.
//!
//! Each run kills the child `ROUNDS` times on the same directory without
//! opening the store in between, so the next child's `open` is the first to
//! see what the kill left, and kills can land during its recovery too. After
//! the last round the parent opens the store and checks that:
//!
//! 1. `open` succeeds.
//! 2. The state is that of every acknowledged operation, with each round's
//!    one in-flight operation per stream either applied or not. Nothing
//!    else.
//! 3. A write after recovery works and survives a reopen.
//!
//! This cannot catch a missing fsync: the page cache survives the process.
//! The power-cut simulation covers that.
//!
//! `cargo test --test kill` runs `TALLY_KILL_RUNS` runs (default 20) from
//! `TALLY_KILL_SEED`. The done criterion is `TALLY_KILL_RUNS=1000`.

use std::collections::BTreeMap;
use std::io::Read;
use std::mem;
use std::os::unix::process::ExitStatusExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use tally::{AutoCompact, Batch, Key, Options, Store, Value};

const CHILD_DIR: &str = "TALLY_KILL_CHILD";
const CHILD_SEED: &str = "TALLY_KILL_CHILD_SEED";
const CHILD_STREAMS: &str = "TALLY_KILL_CHILD_STREAMS";
/// Streams in a run with threads. Each has `KEY_SPACE` keys of its own.
const THREADS: u64 = 4;
const ROUNDS: u64 = 3;
const KEY_SPACE: u64 = 8;
const MAX_DELAY_MICROS: u64 = 40_000;
const MAX_OPS: usize = 100_000;

type State = BTreeMap<u8, Vec<u8>>;

#[derive(Debug, Clone)]
enum Op {
    Set(u8, Vec<u8>),
    Delete(u8),
    Reopen,
    Compact,
    /// Sets (`Some`) and deletes (`None`) written as one batch.
    Batch(Vec<(u8, Option<Vec<u8>>)>),
}

/// splitmix64: small, seedable, and good enough to pick test operations.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// The operation sequence for stream `t` of `streams` in one round, on keys
/// from `t * KEY_SPACE`. Values carry the round seed, the stream and the op
/// index, so no two sets write the same value.
fn ops(seed: u64, t: u64, streams: u64) -> impl Iterator<Item = Op> {
    let mut rng = Rng(seed.wrapping_add(t));
    let first = (t * KEY_SPACE) as u8;
    (0..MAX_OPS).map(move |i| {
        let k = first + rng.below(KEY_SPACE) as u8;
        let tag = format!("{seed:x}-{t}-{i}-");
        let value = |rng: &mut Rng, j: u64| -> Vec<u8> {
            let len = 1 + rng.below(4096) as usize;
            format!("{tag}{j}-").bytes().cycle().take(len).collect()
        };
        match rng.below(22) {
            0..=11 => Op::Set(k, value(&mut rng, 0)),
            12..=17 => Op::Delete(k),
            18 if streams == 1 => Op::Reopen,
            18 | 19 => Op::Compact,
            _ => Op::Batch(
                (0..1 + rng.below(8))
                    .map(|j| {
                        let k = first + rng.below(KEY_SPACE) as u8;
                        let v = (rng.below(3) != 0).then(|| value(&mut rng, j + 1));
                        (k, v)
                    })
                    .collect(),
            ),
        }
    })
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

fn key(k: u8) -> Key {
    Key::try_from(&[b'k', k][..]).unwrap()
}

/// Every stream's keys.
fn read_state(store: &Store) -> State {
    (0..(THREADS * KEY_SPACE) as u8)
        .filter_map(|k| {
            let value = store
                .get(&key(k))
                .expect("a recovered store must read back");
            value.map(|v| (k, v.as_bytes().to_vec()))
        })
        .collect()
}

/// Compacts whenever half the log is dead, so kills also land inside
/// automatic compactions, during writes and during `open`. Segments of
/// 16 KiB, a few values each, so compactions clean some segments and keep
/// others.
fn open(dir: &Path) -> Result<Store, tally::Error> {
    let mut options = Options::default();
    options.auto_compact = AutoCompact::new(50, 0);
    options.segment_bytes = 16 << 10;
    Store::open_with(dir, options)
}

fn child(dir: &Path, seed: u64, streams: u64) {
    let mut store = open(dir).unwrap_or_else(|err| fail("open", err));
    println!("opened");
    if streams == 1 {
        for (i, op) in ops(seed, 0, 1).enumerate() {
            let what = format!("op {i}");
            if let Op::Reopen = op {
                drop(store);
                store = open(dir).unwrap_or_else(|err| fail(&what, err));
            } else {
                run_op(&store, op, &what);
            }
            println!("ack 0 {i}");
        }
    } else {
        thread::scope(|scope| {
            for t in 0..streams {
                let store = &store;
                scope.spawn(move || {
                    for (i, op) in ops(seed, t, streams).enumerate() {
                        run_op(store, op, &format!("stream {t} op {i}"));
                        println!("ack {t} {i}");
                    }
                });
            }
        });
    }
    println!("done");
}

fn run_op(store: &Store, op: Op, what: &str) {
    match op {
        Op::Set(k, v) => store
            .set(key(k), Value::try_from(&v[..]).unwrap())
            .unwrap_or_else(|err| fail(what, err)),
        Op::Delete(k) => store.delete(key(k)).unwrap_or_else(|err| fail(what, err)),
        Op::Reopen => unreachable!("the caller reopens"),
        Op::Compact => store.compact().unwrap_or_else(|err| fail(what, err)),
        Op::Batch(ops) => {
            let mut batch = Batch::new();
            for (k, v) in ops {
                match v {
                    Some(v) => batch.set(key(k), Value::try_from(&v[..]).unwrap()),
                    None => batch.delete(key(k)),
                };
            }
            store.write(batch).unwrap_or_else(|err| fail(what, err));
        }
    }
}

fn fail(what: &str, err: tally::Error) -> ! {
    eprintln!("child: {what} failed: {err}");
    std::process::exit(1)
}

/// What one stream of a round's child got through before it died.
struct Round {
    acked: usize,
    in_flight: bool,
}

fn run_round(dir: &Path, seed: u64, streams: u64, delay: Duration) -> Vec<Round> {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .env(CHILD_DIR, dir)
        .env(CHILD_SEED, seed.to_string())
        .env(CHILD_STREAMS, streams.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    std::thread::sleep(delay);
    child.kill().unwrap();
    let status = child.wait().unwrap();

    let mut stdout = String::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut stdout)
        .unwrap();
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    assert!(
        status.signal().is_some() || status.success(),
        "child failed on its own ({status}):\n{stderr}"
    );

    let lines: Vec<&str> = stdout.lines().collect();
    let opened = lines.first() == Some(&"opened");
    let done = lines.last() == Some(&"done");
    (0..streams)
        .map(|t| {
            let prefix = format!("ack {t} ");
            let acks: Vec<&&str> = lines.iter().filter(|l| l.starts_with(&prefix)).collect();
            for (i, line) in acks.iter().enumerate() {
                assert_eq!(**line, format!("{prefix}{i}"), "acks must arrive in order");
            }
            Round {
                acked: acks.len(),
                in_flight: opened && !done,
            }
        })
        .collect()
}

fn run(dir: &Path, seed: u64, streams: u64) {
    let mut rng = Rng(seed);
    // For each stream, the states its keys may be in.
    let mut candidates: Vec<Vec<State>> = (0..streams).map(|_| vec![State::new()]).collect();

    for _ in 0..ROUNDS {
        let round_seed = rng.next();
        let delay = Duration::from_micros(rng.below(MAX_DELAY_MICROS));
        let rounds = run_round(dir, round_seed, streams, delay);

        for (t, round) in rounds.iter().enumerate() {
            let ops: Vec<Op> = ops(round_seed, t as u64, streams)
                .take(round.acked + 1)
                .collect();
            let mut next = Vec::new();
            for mut state in mem::take(&mut candidates[t]) {
                for op in &ops[..round.acked] {
                    apply(&mut state, op);
                }
                if round.in_flight {
                    let mut with = state.clone();
                    apply(&mut with, &ops[round.acked]);
                    if !next.contains(&with) {
                        next.push(with);
                    }
                }
                if !next.contains(&state) {
                    next.push(state);
                }
            }
            candidates[t] = next;
        }
    }

    let store = Store::open(dir).unwrap_or_else(|err| panic!("open after kill failed: {err}"));
    let observed = read_state(&store);
    for (t, candidates) in candidates.iter().enumerate() {
        let keys = (t as u64 * KEY_SPACE) as u8..((t as u64 + 1) * KEY_SPACE) as u8;
        let ours: State = observed
            .iter()
            .filter(|(k, _)| keys.contains(k))
            .map(|(&k, v)| (k, v.clone()))
            .collect();
        assert!(
            candidates.contains(&ours),
            "seed {seed:#x}, {streams} streams: stream {t}'s state after kills matches no \
             acknowledged history\nobserved keys: {:?}\ncandidates: {}",
            ours.keys().collect::<Vec<_>>(),
            candidates.len()
        );
    }

    let store = store;
    store
        .set(key(0), Value::try_from(&b"after recovery"[..]).unwrap())
        .unwrap();
    drop(store);
    let mut expected = observed;
    expected.insert(0, b"after recovery".to_vec());
    assert_eq!(read_state(&Store::open(dir).unwrap()), expected);
}

fn env_u64(name: &str, default: u64) -> u64 {
    match std::env::var(name) {
        Ok(s) => s
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be an integer")),
        Err(_) => default,
    }
}

fn main() {
    if let Ok(dir) = std::env::var(CHILD_DIR) {
        child(
            Path::new(&dir),
            env_u64(CHILD_SEED, 0),
            env_u64(CHILD_STREAMS, 1),
        );
        return;
    }
    // harness = false: honour a `cargo test <filter>` that doesn't name us.
    let filters: Vec<String> = std::env::args()
        .skip(1)
        .filter(|a| !a.starts_with('-'))
        .collect();
    if !filters.is_empty() && !filters.iter().any(|f| "kill".contains(f.as_str())) {
        return;
    }

    let runs = env_u64("TALLY_KILL_RUNS", 20);
    let seed = env_u64("TALLY_KILL_SEED", 0x7a11);
    let mut rng = Rng(seed);
    for i in 0..runs {
        let run_seed = rng.next();
        let tmp = tempfile::tempdir().unwrap();
        let streams = if i % 2 == 0 { 1 } else { THREADS };
        run(tmp.path(), run_seed, streams);
        if (i + 1) % 100 == 0 {
            println!("kill: {} runs passed", i + 1);
        }
    }
    println!("kill: {runs} runs from seed {seed:#x}, no acknowledged write lost");
}
