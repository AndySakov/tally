# tally

A crash-safe, log-structured key-value store in Rust, written from scratch. A write returns `Ok` only after its
record is fsynced. In 1,000 runs killed with `kill -9` at random moments, it lost no acknowledged write.

## Status

Milestone 2 of 4, crash safety. Recovery from torn tails, `repair` and the kill test are done. The power-cut
simulation is next, then compaction. tally is an embedded library for one process and one thread.

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

## Numbers

Criterion on macOS, from PR #2.

| Operation | Time |
| --- | --- |
| `open`, 100k records | 12.1 ms (126 ms before the index rebuild became one sequential scan) |
| `get`, missing key | 4.8 ns |
| `get`, 1 KiB value | 1.62 µs |
| `set`, 1 KiB value | 4.32 ms, against 4.25 ms for a bare write and `sync_all` |

Writes cost what fsync costs. A batched sync policy is deferred until there are concurrent writers.

## How it's tested

The suite is built to catch regressions, and each part is checked by breaking the code on purpose.

- **Kill test** (`tests/kill.rs`): the test binary runs itself as a child, applies a seeded sequence of sets,
  deletes and reopens, and is killed after 0 to 40 ms. Every acknowledged operation must survive. CI runs 1,000
  runs on Linux. Making `set` update the index without writing the record fails on the first run.
- **Model test** (`tests/model.rs`): random sets, gets, deletes and reopens against a `HashMap`, checking every
  read and the log length after each step. Making `delete` write for a missing key fails it in 0.3 s.
- **Fuzzing** (`fuzz/`): `decode` on arbitrary bytes, `open` on damaged logs, `ops` as a coverage-guided model
  test. A two-minute run of `decode` found no crashes in 44M inputs. Making startup ignore deletes is caught by
  `open` in about 2,900 runs.
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

The on-disk format, durability contract, recovery rules and trade-offs are in [DESIGN.md](DESIGN.md).
