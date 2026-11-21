# tally design

A single-node, crash-safe, log-structured key-value store. Version 1 is an embedded library plus a small CLI.

## Scope (v1)

- API: `open(dir)`, `set(key, value)`, `get(key) -> Option<value>`, `delete(key)`.
- `open` takes a directory. The data file is `tally.log` inside it. A directory leaves room for compaction's second file and a lock file later.
- `delete` of a missing key is a no-op: no record is written and it returns `Ok`.
- Keys and values are non-empty byte strings.
- One process, one thread. `open` takes an exclusive lock (`flock`) on `tally.log`. A second `open` fails with `Locked` until the first `Store` is dropped or its process exits.

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

After the loop, if a truncation happened: truncate the file to `pos`, `fsync`, and log the offset and number of bytes dropped. This must complete before the first new write. Otherwise the next append lands after the torn bytes, and the following startup sees mid-file corruption.

Recovery is idempotent: a crash during recovery followed by a rerun yields the same result.

### Repair

`open` never discards data outside the cases above. A separate `repair` command handles the rest:

1. Copy the bytes from the bad record to EOF into a side file (`tally.log.corrupt-<offset>`).
2. Truncate at the bad record and `fsync`.
3. Report what was dropped.

Possible later improvement: when the record after the bad one parses cleanly, drop only the bad record.

## Index

`HashMap<Box<[u8]>, Entry, foldhash::quality::RandomState>` where `Entry { offset: u64, value_len: u32 }`.

- `Box<[u8]>` instead of `Vec<u8>`: keys never grow, so the capacity field is waste. Saves 8 bytes per slot.
- foldhash instead of the default SipHash: `open` on 100k records went from 15.5 ms to 12.1 ms and a missing-key `get` from 12.9 ns to 4.8 ns. The `quality` variant costs the same as `fast` within noise and resists crafted colliding keys better, which matters once keys arrive over the network.
- Measured: about 877 MB for 10M keys of 32 bytes (pre-sized map). Growth without pre-sizing peaks higher because old and new tables coexist during resize.
- Deletes remove the key from the map. The Delete record stays on disk until compaction.

### Reads

`get` always verifies:

1. Look up the key. Missing: return `None` with no I/O.
2. One positioned read (`read_at`) of the whole record at `offset`.
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

- **Kill test:** run the store as a child process, `kill -9` it at random points, reopen, and check every acknowledged write is present. This cannot detect a missing `fsync`, because the OS page cache survives process death.
- **Power-cut simulation:** file I/O sits behind a trait. A test implementation tracks written bytes and synced bytes separately. A simulated power cut drops everything not synced.
- **Corruption tests:** truncated tails, zero-filled tails, flipped bits in the header, payload, and lengths, mid-file corruption.
- **Model test** (`tests/model.rs`): random sequences of set, get, delete and reopen against a `HashMap`, checking every read and the log length after every step. Real fsyncs, so the default case count is small; `PROPTEST_CASES=2000 cargo test --test model` for a long run.
- **Codec properties** (`record.rs`): round trip, any one-byte change is rejected by the checksum covering it, `decode_header` accepts exactly the valid headers, and a valid header with a matching payload always decodes.
- **Fuzzing** (`fuzz/`, nightly and `cargo-fuzz`): `cargo +nightly fuzz run <target>` with targets `decode` (arbitrary bytes, optionally with checksums fixed up so the fuzzer gets past them), `open` (logs built from arbitrary records, then damaged), and `ops` (the model test, coverage-guided). Fuzz builds skip fsync: durability cannot be observed there and fsync would cut throughput a hundredfold.
- **Benchmarks** (`cargo bench`): `set` next to a bare append + `sync_all` baseline, `get` hit and miss, and `open` at 10k and 100k records.

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
| `tempfile` (dev)           | isolated directories for tests    |
| `proptest` (dev)           | model and codec property tests    |
| `criterion` (dev)          | benchmarks                        |

## Milestones

1. Append-only log and in-memory index. Reopen rebuilds the index.
2. Crash safety: checksums, recovery rules, kill test, power-cut simulation.
3. Compaction: rewrite live keys to a new file, atomic swap via rename, `fsync` the directory. Move the lock to a separate lock file first: the swap replaces `tally.log`, and a lock held on the old file does not cover the new one.
4. Benchmark and README.

## Deferred ideas

- `Interval` sync policy with group commit, once there are concurrent writers.
- Compaction writes the live key count so the next startup can pre-size the index.
- Smaller index: a seeded 128-bit hash per key instead of the key itself (about 37% less memory, estimated), with a collision test using a deliberately weak hash.
- Benchmark-driven `get_unverified` if checksum cost shows up in profiles.
