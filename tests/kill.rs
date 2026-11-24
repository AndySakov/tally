//! Kill test: runs the store in a child process, `kill -9`s it after a
//! random delay, and checks that no acknowledged write was lost.
//!
//! The test binary is its own child: with `TALLY_KILL_CHILD` set it runs a
//! seeded sequence of set, delete, reopen and compact against a store and prints
//! `ack <i>` after each one returns `Ok`. The parent regenerates the same
//! sequence from the seed, so it knows what every ack means.
//!
//! Each run kills the child `ROUNDS` times on the same directory without
//! opening the store in between, so the next child's `open` is the first to
//! see what the kill left, and kills can land during its recovery too. After
//! the last round the parent opens the store and checks that:
//!
//! 1. `open` succeeds.
//! 2. The state is that of every acknowledged operation, with each round's
//!    one in-flight operation either applied or not. Nothing else.
//! 3. A write after recovery works and survives a reopen.
//!
//! This cannot catch a missing fsync: the page cache survives the process.
//! The power-cut simulation covers that.
//!
//! `cargo test --test kill` runs `TALLY_KILL_RUNS` runs (default 20) from
//! `TALLY_KILL_SEED`. The done criterion is `TALLY_KILL_RUNS=1000`.

use std::collections::BTreeMap;
use std::io::Read;
use std::os::unix::process::ExitStatusExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use tally::{AutoCompact, Key, Options, Store, Value};

const CHILD_DIR: &str = "TALLY_KILL_CHILD";
const CHILD_SEED: &str = "TALLY_KILL_CHILD_SEED";
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

/// The operation sequence for one round. Values carry the round seed and
/// the op index, so no two sets write the same value.
fn ops(seed: u64) -> impl Iterator<Item = Op> {
    let mut rng = Rng(seed);
    (0..MAX_OPS).map(move |i| {
        let k = rng.below(KEY_SPACE) as u8;
        match rng.below(20) {
            0..=11 => {
                let len = 1 + rng.below(4096) as usize;
                let tag = format!("{seed:x}-{i}-");
                Op::Set(k, tag.bytes().cycle().take(len).collect())
            }
            12..=17 => Op::Delete(k),
            18 => Op::Reopen,
            _ => Op::Compact,
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
        Op::Reopen | Op::Compact => {}
    }
}

fn key(k: u8) -> Key {
    Key::try_from(&[b'k', k][..]).unwrap()
}

fn read_state(store: &Store) -> State {
    (0..KEY_SPACE as u8)
        .filter_map(|k| {
            let value = store
                .get(&key(k))
                .expect("a recovered store must read back");
            value.map(|v| (k, v.as_bytes().to_vec()))
        })
        .collect()
}

/// Compacts whenever half the log is dead, so kills also land inside
/// automatic compactions, during writes and during `open`.
fn open(dir: &Path) -> Result<Store, tally::Error> {
    let mut options = Options::default();
    options.auto_compact = AutoCompact::new(50, 0);
    Store::open_with(dir, options)
}

fn child(dir: &Path, seed: u64) {
    let mut store = open(dir).unwrap_or_else(|err| fail("open", err));
    println!("opened");
    for (i, op) in ops(seed).enumerate() {
        let what = format!("op {i}");
        match op {
            Op::Set(k, v) => store
                .set(key(k), Value::try_from(&v[..]).unwrap())
                .unwrap_or_else(|err| fail(&what, err)),
            Op::Delete(k) => store.delete(key(k)).unwrap_or_else(|err| fail(&what, err)),
            Op::Reopen => {
                drop(store);
                store = open(dir).unwrap_or_else(|err| fail(&what, err));
            }
            Op::Compact => store.compact().unwrap_or_else(|err| fail(&what, err)),
        }
        println!("ack {i}");
    }
    println!("done");
}

fn fail(what: &str, err: tally::Error) -> ! {
    eprintln!("child: {what} failed: {err}");
    std::process::exit(1)
}

/// What one round's child got through before it died.
struct Round {
    acked: usize,
    in_flight: bool,
}

fn run_round(dir: &Path, seed: u64, delay: Duration) -> Round {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .env(CHILD_DIR, dir)
        .env(CHILD_SEED, seed.to_string())
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
    let acked = lines.iter().filter(|l| l.starts_with("ack ")).count();
    for (i, line) in lines.iter().filter(|l| l.starts_with("ack ")).enumerate() {
        assert_eq!(*line, format!("ack {i}"), "acks must arrive in order");
    }
    Round {
        acked,
        in_flight: opened && !done,
    }
}

fn run(dir: &Path, seed: u64) {
    let mut rng = Rng(seed);
    let mut candidates = vec![State::new()];

    for _ in 0..ROUNDS {
        let round_seed = rng.next();
        let delay = Duration::from_micros(rng.below(MAX_DELAY_MICROS));
        let round = run_round(dir, round_seed, delay);

        let ops: Vec<Op> = ops(round_seed).take(round.acked + 1).collect();
        let mut next = Vec::new();
        for mut state in candidates {
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
        candidates = next;
    }

    let store = Store::open(dir).unwrap_or_else(|err| panic!("open after kill failed: {err}"));
    let observed = read_state(&store);
    assert!(
        candidates.contains(&observed),
        "seed {seed:#x}: state after kills matches no acknowledged history\n\
         observed keys: {:?}\ncandidates: {}",
        observed.keys().collect::<Vec<_>>(),
        candidates.len()
    );

    let mut store = store;
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
        child(Path::new(&dir), env_u64(CHILD_SEED, 0));
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
        run(tmp.path(), run_seed);
        if (i + 1) % 100 == 0 {
            println!("kill: {} runs passed", i + 1);
        }
    }
    println!("kill: {runs} runs from seed {seed:#x}, no acknowledged write lost");
}
