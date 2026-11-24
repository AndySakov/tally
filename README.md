# tally

A crash-safe, log-structured key-value store in Rust, written from scratch. A write returns `Ok` only after its
record is fsynced. No acknowledged write was lost in 1,000 runs killed with `kill -9`, or in a simulated power
cut before every disk operation of a workload, across 1,152 possible disk states.

## Status

v1 is done: an append-only log with an in-memory index, recovery from torn tails, `repair`, and compaction that
runs on its own. tally is an embedded library for one process and one thread.

## Use

```rust
use tally::{Key, Store, Value};

fn main() -> Result<(), tally::Error> {
    let mut store = Store::open("data")?;

    store.set(Key::try_from(&b"user:42"[..])?, Value::try_from(&b"Ada"[..])?)?;

    let key = Key::try_from(&b"user:42"[..])?;
    let value = store.get(&key)?.expect("just written");
    assert_eq!(value.as_bytes(), b"Ada");

    store.delete(key)?;
    Ok(())
}
```

Keys are 1 byte to 1 KiB and values 1 byte to 16 MiB. `Key::try_from` and `Value::try_from` refuse anything
else, so `set` never sees invalid input. A second `open` of the same directory fails with `Error::Locked`.

Overwrites and deletes leave dead bytes in the log. By default, `open`, `set` and `delete` compact the log once
half of it is dead and it is at least 1 MiB. To set the trigger, or turn it off and call `compact` yourself:

```rust
use tally::{AutoCompact, Options, Store};

let mut options = Options::default();
options.auto_compact = AutoCompact::new(30, 64 << 20); // 30% dead, 64 MiB floor
// options.auto_compact = None; // only on store.compact()
let store = Store::open_with("data", options)?;
```

## Numbers

Criterion on an M1 Pro running macOS, where `sync_all` is `F_FULLFSYNC` and flushes the drive's cache.

| Operation | Time |
| --- | --- |
| `set`, 1 KiB value | 4.71 ms, 212 writes/s |
| `set`, 1 KiB value, overwriting 1,000 keys with automatic compaction | 4.95 ms, 202 writes/s |
| Baseline: bare append and `sync_all` of the same record | 5.11 ms, 196 writes/s |
| `get`, 1 KiB value | 1.38 µs |
| `get`, missing key | 4.8 ns |
| `open`, 100k records | 11.2 ms |
| `compact`, 100k live records of 100 B | 30.9 ms |

A write costs what its fsync costs: `set` is within the run-to-run noise of the bare baseline. The overwrite run
compacts every 1,000 writes, and those pauses don't show up in the rate. A batched sync policy is deferred until
there are concurrent writers.

## How it's tested

Each test layer was checked by breaking the code on purpose and watching it fail.

- **Power-cut simulation** (`src/power_cut.rs`): an in-memory disk that keeps only what was synced. A workload of
  sets, deletes and compactions has the power cut before each write, truncate, fsync, rename and directory sync,
  and after its last one. Every way the disk could be left is checked: any prefix of the unsynced writes landing,
  the next one torn or zeroed, and only directory entries made durable by a directory sync surviving. `open`
  must then return every acknowledged write, and recovery is itself cut at every step. Deleting any single fsync
  or directory sync from `open`, `set`, `delete`, `repair` or `compact` fails it.
- **Kill test** (`tests/kill.rs`): the test binary runs itself as a child, applies a seeded sequence of sets,
  deletes, reopens and compactions, and is killed after 0 to 40 ms, three times per run on the same directory.
  Every acknowledged operation must survive. CI runs 1,000 runs on Linux. Making `set` update the index without
  writing the record fails on the first run.
- **Model test** (`tests/model.rs`): random sets, gets, deletes and reopens against a `HashMap`, checking every
  read and the log length after each step. The same broken `set` fails it in 0.55 s.
- **Fuzzing** (`fuzz/`): `decode` on arbitrary bytes, `open` on damaged logs, `ops` as a coverage-guided model
  test that includes compaction.
- **Recovery tests**: every recovery rule, the cases that must fail and leave the log byte-for-byte unchanged,
  and `repair` when the log is intact, torn, corrupt mid-file, already repaired, unsupported or locked.

CI runs lint and tests on Linux and macOS, because fsync and directory sync behave differently on each, and
fuzzes each target for 60 seconds on every pull request.

```sh
cargo test
TALLY_KILL_RUNS=1000 cargo test --test kill
PROPTEST_CASES=2000 cargo test --test model
cargo +nightly fuzz run open
cargo bench
```

## Design

Every record is a 15-byte header with its own CRC32C, then the key and value with a second CRC32C. Because the
header is checked on its own, a torn tail can't be mistaken for a corrupted length mid-file, which would
otherwise drop acknowledged records. On `open`, a torn or zero-filled tail is truncated, fsynced and logged.
Any other damage fails `open` with its offset, and `Store::repair` moves the bad bytes to a side file before
truncating. A failed fsync panics instead of retrying, because on Linux a retry can report success for data that
never reached the disk.

Compaction writes the live records to `tally.log.compact`, fsyncs it, renames it over `tally.log` and fsyncs
the directory. A crash at any point leaves either the old log or the new one, with the same live data. The lock
lives on a separate `tally.lock`, so it survives the rename.

The on-disk format, durability contract, recovery rules and trade-offs are in [DESIGN.md](DESIGN.md).
