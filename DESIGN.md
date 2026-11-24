# tally design

A single-node, crash-safe, log-structured key-value store. Version 1 is an embedded library plus a small CLI.

## Scope (v1)

- API: `open(dir)`, `open_with(dir, options)`, `set(key, value)`, `get(key) -> Option<value>`, `delete(key)`, `compact()`.
- `open` takes a directory. It holds `tally.log` (the data), `tally.lock` and, during compaction, `tally.log.compact`.
- `delete` of a missing key is a no-op: no record is written and it returns `Ok`.
- Keys and values are non-empty byte strings.
- One process, one thread. `open` takes an exclusive lock (`flock`) on `tally.lock`, a separate file because compaction replaces `tally.log` and a lock on the old file would not cover the new one. A second `open` fails with `Locked` until the first `Store` is dropped or its process exits.

## On-disk format

One append-only data file. Every record is a 15-byte prefix followed by the payload.

| Offset         | Size        | Field              | Notes                            |
| -------------- | ----------- | ------------------ | -------------------------------- |
| 0              | 4           | `header_checksum`  | CRC32C of bytes 4..15            |
| 4              | 1           | `type`             | `0` invalid, `1` Set, `2` Delete |
| 5              | 2           | `key_len`          | u16                              |
| 7              | 4           | `value_len`        | u32                              |
| 11             | 4           | `payload_checksum` | CRC32C of key + value            |
| 15             | `key_len`   | key                |                                  |
| 15 + `key_len` | `value_len` | value              | empty for Delete                 |

- All integers are little-endian.
- The checksum is CRC32C (Castagnoli). The variant is part of the format.
- `header_checksum` covers `payload_checksum`, so the payload checksum is itself protected.
- The header has its own checksum so lengths can be trusted before they are used. A single record checksum cannot tell a torn tail from a mid-file record with a corrupted length, and treating the second as the first would silently drop acknowledged records.
- Type `0` is invalid so a zero-filled region can never parse as a record.

### Record validity

A record whose header checksum passes is still invalid if any of these hold:

- `type == 0`
- `key_len == 0`
- Set with `value_len == 0`
- Delete with `value_len != 0`

## Limits

Policy constants in code, not part of the format. The format allows keys up to 64 KiB and values up to 4 GiB, so the limits can be raised later without migrating files.

|       | Max    | Why                                                        |
| ----- | ------ | ---------------------------------------------------------- |
| Key   | 1 KiB  | Every key lives in the in-memory index                     |
| Value | 16 MiB | Caps the allocation of a single `get` and of startup reads |

Keys and values are newtypes (`Key`, `Value`) built with `TryFrom<&[u8]>`. Construction fails on empty input or input over the limit, so `set` cannot receive invalid input.

## Durability contract

`set` and `delete` return `Ok` only after the record has been written and `fsync`ed. An acknowledged write survives both a process crash and power loss.

v1 has one sync policy: `fsync` on every write. A batched policy (`Interval`) is deferred. When added, it comes with `sync()`, `close(self) -> Result<()>`, and a best-effort sync in `Drop`.

### Write path

1. Build the full record in memory: payload, then `payload_checksum`, then the prefix, then `header_checksum`.
2. Append it with a single write to the end of the file.
3. `fsync` the file.
4. Update the index.
5. Return `Ok`.

### Failed fsync

A failed `fsync` panics. On Linux, a failed `fsync` can drop the dirty pages, so a retry may report success for data that never reached disk. The only safe state after a failure is the one recovery rebuilds from the file, so tally stops instead of retrying. Postgres makes the same choice.

### Failed write

If the append fails partway (disk full, I/O error), `set` or `delete` truncates the file back to where the record started and returns the I/O error. A partial record left at the tail would put the next append after torn bytes, and the following startup would see mid-file corruption. If the truncate itself fails, tally panics.

### New files

`open` syncs the data file, the store directory, and the directory's parent on every start, in that order, whether or not it created them. The file name lives in the directory, and without the directory sync the file itself may not survive power loss. Syncing unconditionally makes `open` idempotent: if a previous `open` failed between creating and syncing, the next one completes it. Directory sync behaviour differs between Linux and macOS and must be tested on both.

## Recovery (startup)

State: `pos = 0`, `len = file length`. Loop:

1. `pos == len`: clean end, stop.
2. Fewer than 15 bytes remain: torn header. Truncate at `pos`.
3. Read the prefix. If `header_checksum` fails: if every byte from `pos` to EOF is zero, truncate at `pos`; otherwise fail with corruption.
4. Header is trusted. Unknown type, or lengths over the limits: fail with an unsupported-record error (likely written by a newer version). Validity violations: fail with corruption.
5. Fewer than `15 + key_len + value_len` bytes remain: torn payload. Truncate at `pos`.
6. Read the payload. If `payload_checksum` fails: if every byte from `pos + 15` to EOF is zero (the header landed but the payload did not), truncate at `pos`; otherwise fail with corruption.
7. Set inserts `key -> Entry { offset: pos, value_len }`. Delete removes the key.
8. `pos += 15 + key_len + value_len`. Repeat.

After the loop, if a truncation happened: truncate the file to `pos`, `fsync`, and log a warning (`log` crate) with the offset and number of bytes dropped: `truncated a torn tail at offset 18: dropped 14 bytes`. The caller sees it only if it installs a logger. This must complete before the first new write. Otherwise the next append lands after the torn bytes, and the following startup sees mid-file corruption.

Recovery is idempotent: a crash during recovery followed by a rerun yields the same result.

### Repair

`open` never discards data outside the cases above. `Store::repair(dir) -> Result<Option<Repaired>, Error>` handles the rest:

1. Take the same exclusive lock as `open`. While a `Store` is open, repair fails with `Locked`.
2. Scan the log as `open` does. If `open` would succeed, return `None` and change nothing: a torn tail is left for `open` to truncate.
3. An unsupported record is returned as the error, not repaired. A newer tally can read it, so dropping it would lose data.
4. On a corrupt record at `offset`: write the bytes from `offset` to EOF to `tally.log.corrupt-<offset>` in the store directory, `fsync` it, and `fsync` the directory. Only then truncate the log at `offset` and `fsync` it. The side file is durable before any byte leaves the log.
5. Return `Repaired { offset, dropped, side_file }`.

Repair is idempotent. A crash before the truncate leaves the log as it was, and the rerun rewrites the same side file. After the truncate the log is clean, so a rerun returns `None`.

If the side file already exists, repair rewrites it only when its bytes are a prefix of what it would move, which is what a crashed run leaves. Anything else means an earlier repair at the same offset saved bytes the log no longer has (the log was cut there, new records were written, and the one at that offset went bad too). Repair then fails with `SideFileExists(path)` and changes nothing: moving the file aside is the operator's call, not something to do silently.

Possible later improvement: when the record after the bad one parses cleanly, drop only the bad record.

## Compaction

`compact()` rewrites the log with only the latest record of each live key:

1. Open `tally.log.compact` and truncate it, discarding anything a crashed compaction left.
2. Read the log in one buffered pass from the first live record to the end, checking every record's checksums, and copy the bytes of each record the index still points at. Each copied record's index entry is pointed at its new offset as it is copied, and its old offset is kept, 8 bytes per live key, so a failure can point them back. Compaction never holds a second index: at 10M live keys that took peak memory from 1.84 GB to 1.38 GB. A corrupt record, live or dead, stops compaction with its offset; nothing on disk has changed yet. One sequential pass beats two positioned reads per live record even counting the dead bytes it reads past: 30 ms against 183 ms for 100k live records.
3. `fsync` the new file.
4. Rename it over `tally.log`. The store now reads and appends to the new file.
5. `fsync` the directory. A failure here panics, as for any failed fsync: the rename may not be durable, and writes after it go to the new file.

Before the rename the old log is untouched, so any failure or crash leaves the store as it was; after a failure the moved entries are pointed back at it, rereading each key from the old log, and a failed reread panics so a restart rebuilds the index. After a crash between the rename and the directory fsync, either name survives, and both hold the same live state. Running it twice writes the same log.

### When it runs

`Store` counts **dead bytes**: overwritten values, deleted values and Delete records. `open` computes it from the scan as log length minus live record lengths; `set` and `delete` add to it; compaction resets it to zero.

`Options { auto_compact: Option<AutoCompact> }` sets the trigger. `AutoCompact::new(dead_percent, min_log_bytes)` (1 to 100 percent) compacts when both hold. The default is 50% of a log of at least 1 MiB. At 50%, each compaction copies at most as many live bytes as the dead bytes written since the last one, so compaction at most doubles the bytes written, and the log stays under twice its live size. The floor keeps small stores from compacting every few writes.

The trigger is checked:

- **On `open`**, after recovery. The scan already happened, so the check is free. Startup pays for at most one rewrite.
- **After each `set` or `delete` that writes.** That call takes the compaction's time: 30 ms for 100k live records of 100 bytes on an M1 Pro (`cargo bench -- compact`), plus reading past the dead ones.

Not on drop: `Drop` cannot return errors, and a kill skips it anyway. Not in the background: that needs concurrent reads and writes, which arrive with the TCP server.

An automatic compaction never turns a successful write or `open` into an error: the write is already durable and the old log untouched. It logs a warning instead and turns automatic compaction off until the next `open`, so a persistent failure (a corrupt live record, a full disk) does not make every later write retry a full rewrite. `compact()` called directly returns its error.

## File system

`Store<F: Fs = OsFs>` reaches the disk only through two traits in `tally::fs`, so the power-cut tests can swap in an in-memory disk:

- `Fs`: `create_dir`, `open_or_create`, `open_existing`, `rename`, `sync_dir`, with `type File: LogFile`.
- `LogFile`: `size`, `read_exact_at`, `reader_at(offset) -> impl Read`, `write_all_at`, `set_len`, `sync`, `try_lock`. Writes go only at the current end: the real file is in append mode, where Linux ignores the offset, and the fake asserts it.

Both traits are sealed: public because `Store`'s type parameter is, but implementable only inside tally. `Store::open` and `Store::repair` are defined on `Store<OsFs>` (a default type parameter is not used for inference, so a generic `open` would need annotations); the generic versions are crate-private. The traits return errors; the panic on a failed fsync stays in `Store`.

## Index

`HashMap<Box<[u8]>, Entry, foldhash::quality::RandomState>` where `Entry { offset: u64, value_len: u32 }`.

- `Box<[u8]>` instead of `Vec<u8>`: keys never grow, so the capacity field is waste. Saves 8 bytes per slot.
- foldhash instead of the default SipHash: `open` on 100k records went from 15.5 ms to 12.1 ms and a missing-key `get` from 12.9 ns to 4.8 ns. The `quality` variant costs the same as `fast` within noise.
- The cost is HashDoS resistance. foldhash defeats only trivial collision attacks and "does not claim to provide HashDoS resistance against interactive attackers" (its README). SipHash does. That is acceptable while keys come from the embedding process. Before keys arrive over the network (the TCP server), switch back to SipHash or put a limit on what one client can insert.
- Measured with a pre-sized map: about 877 MB for 10M keys of 32 bytes. `open` does not pre-size yet (see Deferred ideas), so it peaks higher: old and new tables coexist during each resize.
- Deletes remove the key from the map. The Delete record stays on disk until compaction, and counts as dead bytes.

### Reads

`get` always verifies:

1. Look up the key. Missing: return `None` with no I/O.
2. Read the record at `offset`. The code does two positioned reads, header then payload. One read would do: the index entry holds `value_len`, so the record is `15 + key.len() + value_len` bytes (see Deferred ideas).
3. Verify both checksums and that the stored key equals the requested key.
4. Return the value. Any failure returns a corruption error, never `None`.

## Errors

One error enum, marked `#[non_exhaustive]` so variants can be added without breaking callers. Grouped by what the caller can do:

| Category      | Examples                                           | Caller action                             |
| ------------- | -------------------------------------------------- | ----------------------------------------- |
| I/O           | disk full, permission denied                       | environment problem; may retry or give up |
| Corruption    | bad checksum, invalid record, with the file offset | run `repair`                              |
| Unsupported   | unknown record type, lengths over current limits   | upgrade tally                             |
| Invalid input | empty key or value, over the limit                 | fix the input                             |
| Locked        | store already open in another `Store` or process   | close the other one, or wait              |

Bugs (broken internal invariants) panic. Errors are for failures that can happen when the code is correct. The deliberate exceptions are a failed `fsync` and a failed truncate after a failed write, which panic (see Durability contract).

## Testing

- **Kill test** (`tests/kill.rs`): the test binary runs itself as a child that applies a seeded sequence of set, delete, reopen and compact, with automatic compaction at 50% dead and no size floor, printing an ack after each `Ok`. The parent `kill -9`s it after a random delay, three times on the same directory without opening it in between, so kills also land in the next child's `open` and recovery. Then the parent opens the store and checks that `open` succeeds, that the state is that of every acknowledged operation with each round's in-flight operation applied or not, and that a write after recovery survives a reopen. `TALLY_KILL_RUNS` (default 20) and `TALLY_KILL_SEED`; CI runs 1,000 on Linux. This cannot detect a missing `fsync`, because the OS page cache survives process death.
- **Power-cut simulation** (`src/power_cut.rs`, on the in-memory disk in `src/mem_fs.rs`): runs a workload and cuts the power before every step it takes, a step being any write, truncate, file sync, directory create, rename or directory sync. The workload includes explicit compactions and runs with automatic compaction at 50% dead and no size floor, and the power is also cut after its last step. For each cut it tries every way the disk could be left: any prefix of the unsynced changes landing, the next write landing torn or as zeros, and only directory entries made durable by a directory sync surviving (with everything under a lost directory). A directory sync makes the directory's entries exactly what they are now, so a rename and the name it replaced land together. After each, `open` must succeed with every acknowledged write, and recovery is itself cut at every step and checked the same way. Separate tests cut `repair` at every step (no byte is ever missing from both the log and the side file) and check that what `open` served from a dead process's unsynced write survives a cut. Deleting any one fsync or directory sync from `open`, `append`, `repair` or `compact` fails these tests.
- **Corruption tests:** truncated tails, zero-filled tails, flipped bits in the header, payload, and lengths, mid-file corruption.
- **Model test** (`tests/model.rs`): random sequences of set, get, delete and reopen against a `HashMap`, checking every read and the log length after every step. Real fsyncs, so the default case count is small; `PROPTEST_CASES=2000 cargo test --test model` for a long run.
- **Codec properties** (`record.rs`): round trip, any one-byte change is rejected by the checksum covering it, `decode_header` accepts exactly the valid headers, and a valid header with a matching payload always decodes.
- **Fuzzing** (`fuzz/`, nightly and `cargo-fuzz`): `cargo +nightly fuzz run <target>` with targets `decode` (arbitrary bytes, optionally with checksums fixed up so the fuzzer gets past them), `open` (logs built from arbitrary records, then damaged), and `ops` (the model test, coverage-guided). Fuzz builds skip fsync: durability cannot be observed there and fsync would cut throughput a hundredfold.
- **Benchmarks** (`cargo bench`): `set` next to a bare append + `sync_all` baseline, `get` hit and miss, `open` on an empty log and at 10k and 100k records, and `compact` at 10k and 100k live records. The empty-log `open` fsyncs files with nothing dirty, so it shows the fixed cost of `open`, not the cost of a real fsync; `set` shows that.

### Done criteria

- 1,000 random-kill runs lose zero acknowledged writes.
- Power-cut simulation loses zero acknowledged writes.
- A torn or zero-filled tail is truncated on open. Mid-file corruption fails `open` and `repair` recovers it.
- A benchmark reports writes per second.

## Dependencies

Core logic (format, recovery, index, crash tests) is hand-written. Commodity and security-sensitive pieces come from crates.

| Crate                      | Why                               |
| -------------------------- | --------------------------------- |
| `crc32c`                   | CRC32C with hardware acceleration |
| `foldhash`                 | index hasher, faster than SipHash |
| `log`                      | the recovery warning; the caller picks the logger |
| `tempfile` (dev)           | isolated directories for tests    |
| `proptest` (dev)           | model and codec property tests    |
| `criterion` (dev)          | benchmarks                        |

## Milestones

1. Append-only log and in-memory index. Reopen rebuilds the index.
2. Crash safety: checksums, recovery rules, kill test, power-cut simulation.
3. Compaction: rewrite live keys to a new file, atomic swap via rename, `fsync` the directory, triggered by the share of dead bytes. The lock moves to `tally.lock`.
4. Benchmark and README.

## Deferred ideas

- `Interval` sync policy with group commit, once there are concurrent writers.
- Compaction writes the live key count so the next startup can pre-size the index.
- Background compaction, once the TCP server brings concurrency.
- Smaller index: a seeded 128-bit hash per key instead of the key itself (about 37% less memory, estimated), with a collision test using a deliberately weak hash.
- Benchmark-driven `get_unverified` if checksum cost shows up in profiles.
- `get` in one positioned read, sized from the index entry.
