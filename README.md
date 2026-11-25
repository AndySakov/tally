# tally

A crash-safe, log-structured key-value store in Rust, written from scratch. A write returns `Ok` only after its
record is fsynced. No acknowledged write was lost in 1,000 runs killed with `kill -9`, or in a simulated power
cut before every disk operation of a workload, across 13,318 possible disk states.

## Status

v1 is done. tally is an embedded library for one process and one thread:

- an append-only log split into segment files, with an in-memory index of 16 bytes per key;
- writes of one record, or of a batch that applies all or nothing, each with one fsync;
- recovery from torn tails on `open`, `repair` for any other damage, and `verify` to check every record;
- compaction that runs on its own and cleans only the segments with the most dead bytes;
- hint files, so `open` reads a compacted segment's keys without its values.

## Use

```rust
use tally::{Batch, Key, Store, Value};

fn main() -> Result<(), tally::Error> {
    let mut store = Store::open("data")?;

    store.set(Key::try_from(&b"user:42"[..])?, Value::try_from(&b"Ada"[..])?)?;

    let key = Key::try_from(&b"user:42"[..])?;
    let value = store.get(&key)?.expect("just written");
    assert_eq!(value.as_bytes(), b"Ada");

    let mut batch = Batch::new();
    batch
        .set(Key::try_from(&b"user:43"[..])?, Value::try_from(&b"Grace"[..])?)
        .delete(key);
    store.write(batch)?; // both or neither, one fsync
    Ok(())
}
```

Keys are 1 byte to 1 KiB and values 1 byte to 16 MiB. `Key::try_from` and `Value::try_from` refuse anything
else, so `set` never sees invalid input. A second `open` of the same directory fails with `Error::Locked`.

Overwrites and deletes leave dead bytes in the log. By default, `open` and every write compact once half of the
log is dead and it is at least 1 MiB, cleaning the segments with the largest share of dead bytes until it no
longer is. To set the trigger or the segment size, or turn compaction off and call `compact` yourself:

```rust
use tally::{AutoCompact, Options, Store};

let mut options = Options::default();
options.auto_compact = AutoCompact::new(30, 64 << 20); // 30% dead, 64 MiB floor
// options.auto_compact = None; // only on store.compact()
options.segment_bytes = 64 << 20; // default 256 MiB
let store = Store::open_with("data", options)?;
```

A directory from an earlier tally, with one `tally.log`, is moved to segments on its first `open`. The move is
one way: `tally.log` then holds a marker that earlier versions refuse to open. Records with 64-bit checksums are one
way too: a version from before them refuses a log that holds one, instead of misreading it.

## Numbers

Criterion on an M1 Pro running macOS, where `sync_all` is `F_FULLFSYNC` and flushes the drive's cache.

| Operation | Time |
| --- | --- |
| `set`, 1 KiB value | 5.53 ms, 181 writes/s |
| `set`, 1 KiB value, overwriting 1,000 keys with automatic compaction | 5.24 ms, 191 writes/s |
| Baseline: bare append and `sync_all` of the same record | 5.51 ms, 181 writes/s |
| `write`, a batch of 100 sets of 1 KiB | 5.97 ms, 16,752 sets/s |
| `write`, a batch of 1,000 sets of 1 KiB | 10.6 ms, 93,993 sets/s |
| `get`, 1 KiB value in the active segment | 1.03 µs |
| `get`, 1 KiB value in a sealed segment (memory-mapped) | 0.49 µs |
| `get`, missing key | 14 ns |
| `open`, 100k records | 12.6 ms |
| `compact`, 100k live records of 100 B | 44.2 ms |

A write costs what its fsync costs: `set` is within the run-to-run noise of the bare baseline, and a batch
shares one fsync across its records.

At 10M keys of 100 B, written in batches of 10,000 and then overwritten 20M times at random with default
options: the slowest batch took 0.88 s, since each automatic compaction cleans a few 256 MiB segments (9.01 s
when it rewrote the whole log), and `open` of the 2.6 GB that leaves takes 1.8 s.

## How it's tested

Each layer below was checked by breaking the code on purpose and watching it fail.

- **Power-cut simulation** (`src/power_cut.rs`): an in-memory disk that keeps only what was synced. A workload of
  sets, deletes, batches and compactions, with 64-byte segments so most compactions clean some segments and keep
  others, has the power cut before each write, truncate, fsync, rename and directory sync, and after its last
  one. Every way the disk could be left is checked: any prefix of each file's unsynced writes, the next one torn
  or zeroed, and any of the directory changes since the last directory sync, in any order that keeps each name's
  changes in order. `open` must then return every acknowledged write, and recovery is itself cut at every step.
  The same is done for `repair` and for the move from one file to segments. A single directory fsync after
  removing all of a compaction's cleaned segments, instead of one after each, fails it: a deleted key comes back.
- **Kill test** (`tests/kill.rs`): the test binary runs itself as a child, applies a seeded sequence of sets,
  deletes, batches, reopens and compactions with 16 KiB segments, and is killed after 0 to 40 ms, three times per
  run on the same directory. Every acknowledged operation must survive. CI runs 1,000 runs on Linux. Making `set` update the
  index without writing the record fails it.
- **Model test** (`tests/model.rs`): random sets, gets, deletes, batches and reopens against a `HashMap`, with
  100-byte segments and, in half the cases, automatic compaction with no floor, checking every read after each step. The same broken
  `set` fails it, and so does dropping the Delete records compaction must keep.
- **Fuzzing** (`fuzz/`): `decode` on arbitrary bytes, `open` on damaged logs, `hint` on damaged hint files, and
  `ops` as a coverage-guided model test on the in-memory disk.
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

Every record is a 19-byte header with its own CRC32C, then the key and value with a 64-bit CRC (CRC-64/NVME), so
a corrupted value goes unnoticed about once in 2⁶⁴ instead of once in 2³². Because the header is checked on its
own, a torn tail can't be mistaken for a corrupted length mid-file, which would otherwise drop acknowledged
records. Records from before 64-bit checksums still read, and compaction rewrites them. On `open`, a torn or zero-filled tail is truncated, fsynced and logged.
Any other damage fails `open` with its offset, and `Store::repair` moves the bad bytes to a side file before
truncating. A failed fsync panics instead of retrying, because on Linux a retry can report success for data that
never reached the disk.

The log is a run of segment files, each named for the log offset of its first byte, so offsets in the index and
in errors address the whole log. Compaction copies the live records of the segments it cleans into a new
segment, with a hint file of its keys, then deletes the cleaned segments oldest first, fsyncing the directory
after each. A Delete record is copied forward while an older, kept segment may still hold the value it hides.
The new segment is durable before the first old one goes, so a crash at any point leaves the same live data.
The lock lives on a separate `tally.lock`, which no compaction removes.

The on-disk format, durability contract, recovery rules and trade-offs are in [DESIGN.md](DESIGN.md).
