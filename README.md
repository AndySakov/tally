# tally

A crash-safe, log-structured key-value store in Rust, written from scratch. A write returns `Ok` only after its
record is fsynced. No acknowledged write was lost in 1,000 runs killed with `kill -9`, or in a simulated power
cut before every disk operation of a workload, across 1,210,896 possible disk states.

## Status

v1 is done. tally is an embedded library for one process, shared by its threads:

- an append-only log split into segment files, with an in-memory index of 16 bytes per key;
- writes of one record, or of a batch that applies all or nothing, each with one fsync, shared by writes from
  other threads that arrive while it waits;
- reads that take no lock while they read, alongside writes and compaction;
- recovery from torn tails on `open`, which saves what it cuts, `repair` for any other damage, and `verify` to check
  every record;
- compaction that runs on its own and cleans only the segments with the most dead bytes;
- hint files, written by compaction and by `open` itself, so `open` reads each sealed segment's keys without its
  values.

## Use

```rust
use tally::{Batch, Key, Store, Value};

fn main() -> Result<(), tally::Error> {
    let store = Store::open("data")?;

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

Every method takes `&self`. To use a store from several threads, put it in an `Arc`: writes that arrive
together share one fsync. If an fsync fails, that write returns the error and every later write returns
`Error::Poisoned` until the store is reopened; reads keep working.

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

This version reads only stores it wrote. `open` refuses a directory written by an earlier tally with
`Unsupported`, without changing it.

## Numbers

Criterion on an M1 Pro running macOS, where `sync_all` is `F_FULLFSYNC` and flushes the drive's cache.

| Operation | Time |
| --- | --- |
| `set`, 1 KiB value | 5.53 ms, 181 writes/s |
| `set`, 1 KiB value, overwriting 1,000 keys with automatic compaction | 5.24 ms, 191 writes/s |
| Baseline: bare append and `sync_all` of the same record | 5.51 ms, 181 writes/s |
| `set`, 1 KiB value, from 16 threads at once | 0.37 ms each, 2,736 writes/s |
| `set`, 1 KiB value, from 64 threads at once | 0.10 ms each, 9,739 writes/s |
| `write`, a batch of 100 sets of 1 KiB | 5.97 ms, 16,752 sets/s |
| `write`, a batch of 1,000 sets of 1 KiB | 10.6 ms, 93,993 sets/s |
| `get`, 1 KiB value | 1.03 µs |
| `get`, missing key | 14 ns |
| `open`, 100k records | 12.6 ms |
| `compact`, 100k live records of 100 B | 44.2 ms |

A write costs what its fsync costs: `set` is within the run-to-run noise of the bare baseline, and a batch
shares one fsync across its records, as writes from several threads share one across the group.

At 10M keys of 100 B, written in batches of 10,000 and then overwritten 20M times at random with default
options: the slowest batch took 0.88 s, since each automatic compaction cleans a few 256 MiB segments (9.01 s
when it rewrote the whole log), and `open` of the 2.6 GB that leaves takes 0.9 s once one `open` has written
hints for the segments compaction didn't (1.8 s without them).

## How it's tested

Each layer below was checked by breaking the code on purpose and watching it fail.

- **Power-cut simulation** (`src/power_cut.rs`): an in-memory disk that keeps only what was synced. A workload of
  sets, deletes, batches, compactions and a group of writes sharing one fsync, with 64-byte segments so most compactions clean some segments and keep
  others, has the power cut before each write, truncate, fsync, rename and directory sync, and after its last
  one. Every way the disk could be left is checked: any prefix of each file's unsynced writes, the next one torn,
  zeroed or landing its end without its start, one file's writes landing while another's don't, and any of the directory changes since the last directory sync, in any order that keeps each name's
  changes in order. `open` must then return every acknowledged write, and recovery is itself cut at every step.
  The same is done for `repair`. When a compaction drops a Delete record, a single directory fsync after removing
  all of its cleaned segments, instead of one after each, fails it: a deleted key comes back.
- **Kill test** (`tests/kill.rs`): the test binary runs itself as a child, applies a seeded sequence of sets,
  deletes, batches, reopens and compactions with 16 KiB segments, and is killed after 0 to 40 ms, three times per
  run on the same directory. Every other run, four threads share the store, each with keys of its own. Every acknowledged operation must survive. CI runs 1,000 runs on Linux. Making `set` update the
  index without writing the record fails it.
- **Model test** (`src/model.rs`): random sets, gets, deletes, batches, groups, compactions and reopens against a
  `BTreeMap`, with 100-byte segments and, in half the cases, automatic compaction with no floor, checking every read
  and the log's length. A second test runs four threads on one store, each against its own map, with compactions between them. The same broken
  `set` fails it, and so does dropping the Delete records compaction must keep.
- **Fuzzing** (`fuzz/`): `decode` on arbitrary bytes at arbitrary offsets, `open` on cut, changed and zeroed logs, `hint` on damaged hint files, and
  `ops` as a coverage-guided model test on the in-memory disk.
- **Recovery tests**: every recovery rule, the cases that must fail and leave the log byte-for-byte unchanged,
  and `repair` when the log is intact, torn, corrupt mid-file, already repaired, unsupported or locked.

CI runs lint and tests on Linux and macOS, because fsync and directory sync behave differently on each, and
fuzzes each target for 60 seconds on every pull request.

```sh
cargo test
TALLY_KILL_RUNS=1000 cargo test --test kill
PROPTEST_CASES=2000 cargo test model::
cargo +nightly fuzz run open
cargo bench
```

## Design

Every record is a 19-byte header with its own CRC32C, which also covers the record's offset, then the key and value
with a 64-bit CRC (CRC-64/NVME), so a corrupted value goes unnoticed about once in 2⁶⁴ instead of once in 2³², and a
valid record found anywhere but where it was written fails. Because the header is checked on its own, a torn tail
can't be mistaken for a corrupted length mid-file, which would otherwise drop acknowledged records. Every write is
one record, or one batch record around several, so damage after which no record starts is the last write torn by
a crash, in whatever order its blocks landed. `open` saves those bytes to a side file, truncates, fsyncs and logs.
Any other damage fails `open` with its offset, and `Store::repair` moves the bad bytes to a side file before
truncating. A failed fsync stops all writes until a reopen instead of retrying, because on Linux a retry can
report success for data that never reached the disk.

The log is a run of segment files, each named for the log offset of its first record and starting with a header
that names its format and base, so offsets in the index and in errors address the whole log. Compaction copies the
live records of the segments it cleans into a new segment, under a temporary name until the copies are durable,
with a hint file of its keys, then deletes the cleaned segments oldest first. If it dropped a Delete record, it
fsyncs the directory after each removal, so the Delete never outlives the value it hides, and otherwise once after
all of them. A Delete record is copied forward only while an older, kept segment may still hold a value it hides,
which a filter of each segment's keys answers. The new segment is durable before the first old one goes, so a crash
at any point leaves the same live data. The lock lives on a separate `tally.lock`, which no compaction removes.

The on-disk format, durability contract, recovery rules and trade-offs are in [DESIGN.md](DESIGN.md).
