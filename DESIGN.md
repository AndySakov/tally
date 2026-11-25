# tally design

A single-node, crash-safe, log-structured key-value store. Version 1 is an embedded library plus a small CLI.

## Scope (v1)

- API: `open(dir)`, `open_with(dir, options)`, `set(key, value)`, `get(key) -> Option<value>`, `delete(key)`, `write(batch)`, `compact()`, `verify()`.
- `open` takes a directory. It holds `tally.log` (the data), `tally.lock`, `tally.hint` after a compaction, and, during one, `tally.log.compact` and `tally.hint.compact`.
- `delete` of a missing key is a no-op: no record is written and it returns `Ok`.
- Keys and values are non-empty byte strings.
- One process, one thread. `open` takes an exclusive lock (`flock`) on `tally.lock`, a separate file because compaction replaces `tally.log` and a lock on the old file would not cover the new one. A second `open` fails with `Locked` until the first `Store` is dropped or its process exits.

## On-disk format

One append-only data file. Every record is a 15-byte prefix followed by the payload.

| Offset         | Size        | Field              | Notes                            |
| -------------- | ----------- | ------------------ | -------------------------------- |
| 0              | 4           | `header_checksum`  | CRC32C of bytes 4..15            |
| 4              | 1           | `type`             | `0` invalid, `1` Set, `2` Delete, `3` Batch |
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
- A Batch record has no key and an 8-byte "value", `body_len` (u64): the byte length of the Set and Delete records that follow it and belong to the batch. Those records are ordinary records with their own checksums, so `get` reads one the same way whether or not it was written in a batch. A version without Batch fails on one with `Unsupported` (unknown type 3) instead of misreading it.

### Record validity

A record whose header checksum passes is still invalid if any of these hold:

- `type == 0`
- `key_len == 0`, for Set and Delete
- Set with `value_len == 0`
- Delete with `value_len != 0`
- Batch with `key_len != 0` or `value_len != 8` (`BadBatch`)

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

## Batches

`Batch` collects sets and deletes in order; `write(batch)` makes all of them durable with one append and one fsync, and recovery applies all of them or none (see Recovery). It is how a single thread gets group commit: the cost of a write is its fsync, so sharing one fsync across a batch divides it. 1 KiB values on an M1 Pro: 212 writes/s one at a time, 2,019/s in batches of 10, 18,663/s in batches of 100, and 100,810/s in batches of 1,000.

- Operations apply in order, so a delete sees the batch's own earlier sets. A delete of a key that isn't live at that point writes nothing, as `delete` does, and a batch that writes nothing writes no Batch record either.
- The 23-byte Batch record counts as dead bytes from the start. Compaction copies the live Set records out of batches like any others; the compacted log has no batches.
- The whole batch is encoded in memory before the write, so its size is bounded by memory, not by the format (`body_len` is a u64).

## Recovery (startup)

If `tally.hint` is usable (see Hint file), the index starts from it and `pos` from the log length it covers. Otherwise the index starts empty. State: `pos` as above (0 without a hint), `len = file length`. Loop:

1. `pos == len`: clean end, stop.
2. Fewer than 15 bytes remain: torn header. Truncate at `pos`.
3. Read the prefix. If `header_checksum` fails: if every byte from `pos` to EOF is zero, truncate at `pos`; otherwise fail with corruption.
4. Header is trusted. Unknown type, or lengths over the limits: fail with an unsupported-record error (likely written by a newer version). Validity violations: fail with corruption.
5. Fewer than `15 + key_len + value_len` bytes remain: torn payload. Truncate at `pos`.
6. Read the payload. If `payload_checksum` fails: if every byte from `pos + 15` to EOF is zero (the header landed but the payload did not), truncate at `pos`; otherwise fail with corruption.
7. Set inserts `key -> Entry { offset: pos, value_len }`. Delete removes the key. A Batch is handled as described below.
8. `pos += 15 + key_len + value_len`. Repeat.

A Batch record at `pos` is followed by `body_len` bytes of records, and the batch is all or nothing:

- If fewer than `body_len` bytes follow, the batch is a torn tail: truncate at `pos`.
- Otherwise read its records with the same rules. Their operations are held back until the whole body has been read and checked. A torn or zero-filled record inside it is a torn tail from `pos`.
- A Batch inside the body, or a record that runs past the body's end, is corruption (`BadBatch`) at `pos`.
- Any other error inside the body is reported at `pos`, not at the record, so `repair` moves the whole batch to the side file and never leaves part of one in the log.
- Once the body checks out, apply its operations in order and continue at `pos + 23 + body_len`.

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

1. Open `tally.log.compact` and `tally.hint.compact` and truncate both, discarding anything a crashed compaction left.
2. Read the log in one buffered pass from the first live record to the end, checking every record's checksums, and copy the bytes of each record the index still points at. Each copied record's index entry is pointed at its new offset as it is copied, and its old offset is kept, 8 bytes per live key, so a failure can point them back. Compaction never holds a second index: at 10M live keys that took peak memory from 1.84 GB to 1.38 GB. Each copied record's key and value length also go to `tally.hint.compact`. A corrupt record, live or dead, stops compaction with its offset; nothing on disk has changed yet. One sequential pass beats two positioned reads per live record even counting the dead bytes it reads past: 30 ms against 183 ms for 100k live records.
3. `fsync` the new log and the new hint.
4. Remove the old `tally.hint` if there is one, and `fsync` the directory. It describes the old log, so it must be gone for good before the new log takes the name.
5. Rename the new log over `tally.log`. The store now reads and appends to the new file.
6. `fsync` the directory. A failure here panics, as for any failed fsync: the rename may not be durable, and writes after it go to the new file.
7. Rename `tally.hint.compact` to `tally.hint`, without a directory fsync: if the name is lost, the next `open` reads the whole log. A failure here is logged, not returned; the compaction has already happened.

Before the rename the old log is untouched, so any failure or crash leaves the store as it was; after a failure the moved entries are pointed back at it, rereading each key from the old log, and a failed reread panics so a restart rebuilds the index. After a crash between the rename and the directory fsync, either name survives, and both hold the same live state. A crash anywhere leaves either no hint or one that matches the log. Running it twice writes the same log.

### Hint file

`tally.hint` lets `open` rebuild the index without reading values. Format (`src/hint.rs`), little-endian:

```text
magic "TLYH" | version u8 | entries | count u64 | log_len u64 | crc32c u32
entry: key_len u16 | value_len u32 | key
```

- Offsets are not stored. A compacted log holds exactly the live records end to end, so each offset is the sum of the record lengths before it, and the sum of them all must equal `log_len`. That doubles as a check that the hint and the log agree.
- The count and log length trail the entries because files are only written at the end and neither is known until compaction finishes. `open` reads the trailer first, with one positioned read, to size the index for `count` keys, never more than the log could hold (`log_len / 17`, the shortest record), since the count is unchecked until every entry is read.
- The checksum covers every byte before it. Entries go into the index as they are read; if anything fails, that index is thrown away.
- `open` reads the hint on one thread, checking every entry's lengths, and gathers checked entries into 16 MiB windows cut into chunks of 8,192 entries, recording where each chunk starts in the log. Each window's chunks are then hashed and inserted by up to 8 threads (the calling thread is one), into the sharded index (see Index), while I/O stays on the calling thread. Hints under 100,000 entries are read on one thread. Splitting needs no change to the format, because the walk that checks lengths also finds where entries start; the cost of not storing chunk offsets is the one sequential pass, which was already there.

On `open`, a hint is used only if its magic, version and checksum hold, every key and value length is in range, the entries add up to `count` distinct keys and exactly `log_len` bytes, and the log is at least `log_len` long. Records after `log_len` are then read as usual. A hint that fails any check is logged, removed and the directory fsynced, and the whole log is read. The removal must be durable: a hint rejected because the log is shorter than it says would look valid again once new writes grew the log past `log_len`.

`repair` removes the hint before it truncates the log, in the same directory fsync that makes the side file durable.

The cost is that `open` no longer checks the checksums of records the hint covers. Damage there shows up as a corruption error from `get`, or from the next compaction, which reads every record; `repair` then finds it with a full scan. This is the Bitcask trade.

At 10M live records of 100 bytes, `open` from the hint took 1.54 to 1.61 s and peaked at 718 MB, against 2.11 s and 1.23 GB for a full scan (before the index stopped storing keys; see Index). With the 128-bit index it took 1.79 to 1.88 s at 557 MB, and reading on 8 threads took that to 0.61 to 0.67 s at 586 MB: the extra 29 MB is the window and per-thread buffers. The hint is 22 bytes per key here (220 MB). With 100 values of 16 MiB, `open` went from 1.18 s to 38 ms.

### When it runs

`Store` counts **dead bytes**: overwritten values, deleted values and Delete records. `open` computes it from the scan as log length minus live record lengths; `set` and `delete` add to it; compaction resets it to zero.

`Options { auto_compact: Option<AutoCompact> }` sets the trigger. `AutoCompact::new(dead_percent, min_log_bytes)` (1 to 100 percent) compacts when both hold. The default is 50% of a log of at least 1 MiB. At 50%, each compaction copies at most as many live bytes as the dead bytes written since the last one, so compaction at most doubles the bytes written, and the log stays under twice its live size. The floor keeps small stores from compacting every few writes.

The trigger is checked:

- **On `open`**, after recovery. The scan already happened, so the check is free. Startup pays for at most one rewrite.
- **After each `set` or `delete` that writes.** That call takes the compaction's time: 38 ms for 100k live records of 100 bytes on an M1 Pro (`cargo bench -- compact`), plus reading past the dead ones. About 12 ms of that is the hint's fsync and the directory fsync that removes the old hint, each a `F_FULLFSYNC` on macOS.

Not on drop: `Drop` cannot return errors, and a kill skips it anyway. Not in the background: that needs concurrent reads and writes, which arrive with the TCP server.

An automatic compaction never turns a successful write or `open` into an error: the write is already durable and the old log untouched. It logs a warning instead and turns automatic compaction off until the next `open`, so a persistent failure (a corrupt live record, a full disk) does not make every later write retry a full rewrite. `compact()` called directly returns its error.

## File system

`Store<F: Fs = OsFs>` reaches the disk only through two traits in `tally::fs`, so the power-cut tests can swap in an in-memory disk:

- `Fs`: `create_dir`, `open_or_create`, `open_existing`, `rename`, `remove`, `sync_dir`, with `type File: LogFile`.
- `LogFile`: `size`, `read_exact_at`, `reader_at(offset) -> impl Read`, `write_all_at`, `set_len`, `sync`, `try_lock`. Writes go only at the current end: the real file is in append mode, where Linux ignores the offset, and the fake asserts it.

Both traits are sealed: public because `Store`'s type parameter is, but implementable only inside tally. `Store::open` and `Store::repair` are defined on `Store<OsFs>` (a default type parameter is not used for inference, so a generic `open` would need annotations); the generic versions are crate-private. The traits return errors; the panic on a failed fsync stays in `Store`.

## Index

`src/index.rs`: `HashMap<u128, Entry>` where `Entry { offset: u64, value_len: u32, key_len: u16 }` (16 bytes, `key_len` filling what was padding). Keys are not stored: each is replaced by its 128-bit SipHash-1-3 under a key drawn at random for every `open`, from std's `RandomState`, which is seeded from the OS. The map hashes that `u128` by taking its low 64 bits.

- No allocation per key, and 32 bytes per slot whatever the key's length. At 10M keys of 16 bytes, building the index alone went from 78 to 58 ns per key and from 681 to 528 MB, and `open` from the hint from 718 to 557 MB at peak. The saving grows with key length.
- The keyed hash also restores HashDoS resistance, which foldhash (the hasher before) did not claim: without the key, no one can choose keys that land in the same slot. That was a prerequisite for keys arriving over the network.
- The cost is a missing-key `get`, which now hashes 128 bits: 4.8 ns before, 10.9 ns after. A hit is dominated by the read and checksum, 1.26 µs.
- Two keys with the same 128-bit hash would share one entry: a `set` of the second would silently replace the first. The chance is about n²/2¹²⁹, 2 × 10⁻²⁴ at 40M live keys, and no one can aim for it without the random key. `get` checks the key stored in the record, so a collision shows up as `KeyMismatch`, never as another key's value; a test with a deliberately weak hash (a key's length) checks that.
- `key_len` lets `open` count dead bytes without the keys.
- The map is 64 maps (shards), chosen by the top 6 bits of the hash. Each shard's map places entries by the low bits, so the shard must come from bits it doesn't use: taking the low bits would give every entry in a shard the same low 6 bits and leave 63 of every 64 home slots empty. No test can see that, as any consistent choice is correct; it would only be slow.
- Shards let `open` insert from several threads at once (`Index::fill`): each thread holds up to 256 entries per shard and then takes that shard's lock, so locks are taken once per 256 entries. Which thread's entry wins for a key two threads both insert is not defined, so `fill` is only for keys no two jobs share, as a hint's are (a hint that repeats a key is rejected by its count).
- Growing the map now moves one shard at a time, so the old and new tables that coexist during a resize are 1/64 of the index. A full scan of 10M records without a hint peaked at 841 to 931 MB, against 0.98 to 1.11 GB with one map, in the same time. A missing-key `get` went from 12.1 to 13.1 ns.
- `open` pre-sizes from the hint's count, a 64th per shard. Without a hint it grows the map as it reads.
- Deletes remove the key from the map. The Delete record stays on disk until compaction, and counts as dead bytes.

### Verify

`verify()` is the full check `open` no longer does once a hint covers most of the log: it reads every record from 0 to the store's end, checking both checksums, and compares the index that scan describes with the one in memory, as lists of entries sorted by offset. It fails with the first bad record's error, or `IndexMismatch` at the first offset where they disagree, including a log whose size is not the store's end. It reads the whole log and holds 32 bytes per live key while comparing; it is meant to be run on demand or on a schedule, not on every open.

### Reads

`get` always verifies:

1. Look up the key. Missing: return `None` with no I/O.
2. Read the record at `offset` in one positioned read of `15 + key_len + value_len` bytes, both from the index entry. If the header there gives other lengths, or the read runs past the end, read it again the old way (header, then the payload length the header gives), so errors report what is actually on disk. One read instead of two took a 1 KiB hit from 1.30 µs to 0.82 µs.
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

- **Kill test** (`tests/kill.rs`): the test binary runs itself as a child that applies a seeded sequence of set, delete, reopen, compact and batch, with automatic compaction at 50% dead and no size floor, printing an ack after each `Ok`. The parent `kill -9`s it after a random delay, three times on the same directory without opening it in between, so kills also land in the next child's `open` and recovery. Then the parent opens the store and checks that `open` succeeds, that the state is that of every acknowledged operation with each round's in-flight operation applied or not, and that a write after recovery survives a reopen. `TALLY_KILL_RUNS` (default 20) and `TALLY_KILL_SEED`; CI runs 1,000 on Linux. This cannot detect a missing `fsync`, because the OS page cache survives process death.
- **Power-cut simulation** (`src/power_cut.rs`, on the in-memory disk in `src/mem_fs.rs`): runs a workload and cuts the power before every step it takes, a step being any write, truncate, file sync, directory create, rename or directory sync. The workload includes explicit compactions and batches and runs with automatic compaction at 50% dead and no size floor, and the power is also cut after its last step. For each cut it tries every way the disk could be left: any prefix of the unsynced changes landing, the next write landing torn or as zeros, and only directory entries made durable by a directory sync surviving (with everything under a lost directory). A directory sync makes the directory's entries exactly what they are now, so a rename and the name it replaced land together. After each, `open` must succeed with every acknowledged write, and recovery is itself cut at every step and checked the same way. Separate tests cut `repair` at every step (no byte is ever missing from both the log and the side file) and check that what `open` served from a dead process's unsynced write survives a cut. Deleting any one fsync or directory sync from `open`, `append`, `repair` or `compact` fails these tests, with two exceptions, both from the hint file. The hint's own fsync: a torn hint fails its checksum and `open` reads the whole log, so correctness never rests on it, and it is kept so that no 32-bit checksum ever has to vouch for one. The directory fsync between removing the old hint and renaming the new log: the fake makes a directory's unsynced changes durable all at once, so it cannot produce the rename without the removal. A separate test (`a_removed_hint_stays_removed_after_a_power_cut`) checks that `open`'s removal of a rejected hint survives a cut.
- **Corruption tests:** truncated tails, zero-filled tails, flipped bits in the header, payload, and lengths, mid-file corruption.
- **Model test** (`tests/model.rs`): random sequences of set, get, delete, batch and reopen against a `HashMap`, checking every read and the log length after every step. Real fsyncs, so the default case count is small; `PROPTEST_CASES=2000 cargo test --test model` for a long run.
- **Codec properties** (`record.rs`): round trip, any one-byte change is rejected by the checksum covering it, `decode_header` accepts exactly the valid headers, and a valid header with a matching payload always decodes.
- **Fuzzing** (`fuzz/`, nightly and `cargo-fuzz`): `cargo +nightly fuzz run <target>` with targets `decode` (arbitrary bytes, optionally with checksums fixed up so the fuzzer gets past them), `open` (logs built from arbitrary records and batches, then damaged), `ops` (the model test, coverage-guided), and `hint` (a compacted store with more writes after it and a hint changed, truncated or replaced with arbitrary bytes: `open` must still read back every key). Fuzz builds skip fsync: durability cannot be observed there and fsync would cut throughput a hundredfold.
- **Benchmarks** (`cargo bench`): `write` with batches of 10, 100 and 1,000; `set` next to a bare append + `sync_all` baseline, `get` hit and miss, `open` on an empty log and at 10k and 100k records, and `compact` at 10k and 100k live records. The empty-log `open` fsyncs files with nothing dirty, so it shows the fixed cost of `open`, not the cost of a real fsync; `set` shows that.

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
| `siphasher`                | 128-bit SipHash-1-3 for the index |
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

- Group commit across concurrent writers (sharing one fsync between callers, as `write` does within a batch), once there are any.
- Background compaction, once the TCP server brings concurrency.
- Benchmark-driven `get_unverified` if checksum cost shows up in profiles.
- A parallel full scan, for logs without a hint. Records must apply in log order, so threads would have to resolve each key to its last record by offset, with deletes in the mix; compaction writes a hint, so this only matters before the first one.
