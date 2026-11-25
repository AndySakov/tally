# tally design

A single-node, crash-safe, log-structured key-value store. Version 1 is an embedded library plus a small CLI.

## Scope (v1)

- API: `open(dir)`, `open_with(dir, options)`, `set(key, value)`, `get(key) -> Option<value>`, `delete(key)`, `write(batch)`, `compact()`, `verify()`.
- `open` takes a directory. It holds the log's segments (`tally.<base>.log`), a hint for each segment a compaction wrote (`tally.<base>.hint`), `tally.lock`, and `tally.log`, which holds only a marker that keeps earlier versions out (see Segments).
- `delete` of a missing key is a no-op: no record is written and it returns `Ok`.
- Keys and values are non-empty byte strings.
- One process, one thread. `open` takes an exclusive lock (`flock`) on `tally.lock`, a separate file because compaction removes segments and a lock on one of them would not outlive it. A second `open` fails with `Locked` until the first `Store` is dropped or its process exits.

## On-disk format

The log is a sequence of append-only segment files (see Segments). Every record is a 15-byte prefix followed by the payload.

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

`open` syncs the newest segment, the store directory, and the directory's parent on every start, in that order, whether or not it created them. The file name lives in the directory, and without the directory sync the file itself may not survive power loss. Syncing unconditionally makes `open` idempotent: if a previous `open` failed between creating and syncing, the next one completes it. Directory sync behaviour differs between Linux and macOS and must be tested on both.

## Batches

`Batch` collects sets and deletes in order; `write(batch)` makes all of them durable with one append and one fsync, and recovery applies all of them or none (see Recovery). It is how a single thread gets group commit: the cost of a write is its fsync, so sharing one fsync across a batch divides it. 1 KiB values on an M1 Pro: 212 writes/s one at a time, 2,019/s in batches of 10, 18,663/s in batches of 100, and 100,810/s in batches of 1,000.

- Operations apply in order, so a delete sees the batch's own earlier sets. A delete of a key that isn't live at that point writes nothing, as `delete` does, and a batch that writes nothing writes no Batch record either.
- The 23-byte Batch record counts as dead bytes from the start. Compaction copies the live Set records out of batches like any others; the compacted log has no batches.
- The whole batch is encoded in memory before the write, so its size is bounded by memory, not by the format (`body_len` is a u64).

## Segments

The log is split into segment files, each named for the log offset of its first byte: `tally.<base, 20 digits>.log`, so sorting the names sorts the log. Offsets everywhere (the index, errors, `Repaired`, side-file names) are offsets into the whole log; `Segment` (`src/segment.rs`) implements `LogFile` by subtracting its base, so the scan, `get` and `verify` read a segment with the same code that read the one file. The index entry stays 16 bytes. The log's offsets only grow: compaction appends copies at the end and deletes the old segments, so the log has holes where they were.

- Writes go to the last segment. Before a write, if that segment holds at least `Options::segment_bytes` (default 256 MiB), a new one is created at the end of the log and the directory fsynced, so its name is durable before any acknowledged write lands in it. A write is one append, so a batch never spans two segments; a segment can exceed `segment_bytes` by one write, and a compaction's output by any amount.
- Only the newest segment can have a torn tail: every other was complete and fsynced before the next was written. In any other segment, a tail `open` would truncate is `CutShort` corruption instead, for `repair`.
- Segments must not overlap: a segment that starts before the previous one ends fails `open` with `SegmentsOverlap`. Nothing tally does can produce one; it means files were copied in from elsewhere.
- Every segment stays open for the life of the `Store`, one file descriptor each. At 256 MiB that is 4 per GiB of log; macOS's default limit of 256 per process is reached around 60 GiB.

### From one file to segments

A directory from before segments has `tally.log` (and maybe `tally.hint`) and no segment files. `open` (and `repair`):

1. Hard-links `tally.log` as the segment at 0, renames `tally.hint` to its hint, and removes any `tally.log.compact` and `tally.hint.compact` a crashed compaction left.
2. Writes the marker to `tally.log.marker` and fsyncs it, then the directory.
3. Renames `tally.log.marker` over `tally.log`, and fsyncs the directory.

Offsets do not change, since the segment at 0 holds exactly the old file. The marker is one record header of type 255 with valid checksums. Every earlier version reads `tally.log` from offset 0, so it fails with `Unsupported` (unknown type 255) instead of finding no records and starting an empty store over the real data; its `repair` refuses unsupported records too.

At every step, then, `tally.log` holds either all the old records or the whole marker, so a power cut never leaves an earlier version a `tally.log` that is missing, torn or zeroed, any of which it would open as an empty store. The directory fsync in step 2 keeps the rename from landing without the link, which would leave the records with no name. The power-cut test checks this at every cut of the move.

- A `tally.log` that holds records while the segment at 0 is the same file (by device and inode) is a move stopped after step 1, and `open` finishes it.
- A `tally.log` shorter than any record (16 bytes) that is not the marker is replaced with it: it holds no records.
- A `tally.log` holding records while any other segment exists fails `open`: an earlier version wrote records the segments know nothing about, and they cannot be put in order.
- On a file system without hard links, step 1 renames `tally.log` instead, and a power cut before step 3 lands can leave no `tally.log` for an earlier version to refuse.

The move is one way.

## Recovery (startup)

`open` reads every segment in log order into one index. For each: if its hint is usable (see Hint file), its entries go into the index and `pos` starts where the hint ends; otherwise `pos` starts at the segment's base. `len` is the segment's end. Loop:

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

A truncation is only allowed in the newest segment; in any other it is `CutShort` corruption at `pos` (see Segments). After the loop, if a truncation happened: truncate the segment to `pos`, `fsync`, and log a warning (`log` crate) with the offset and number of bytes dropped: `truncated a torn tail at offset 18: dropped 14 bytes`. The caller sees it only if it installs a logger. This must complete before the first new write. Otherwise the next append lands after the torn bytes, and the following startup sees mid-file corruption.

Recovery is idempotent: a crash during recovery followed by a rerun yields the same result.

### Repair

`open` never discards data outside the cases above. `Store::repair(dir) -> Result<Option<Repaired>, Error>` handles the rest:

1. Take the same exclusive lock as `open`. While a `Store` is open, repair fails with `Locked`.
2. Scan every segment in order, reading every record (hints are not used). If `open` would succeed, return `None` and change nothing: a torn tail of the newest segment is left for `open` to truncate.
3. An unsupported record is returned as the error, not repaired. A newer tally can read it, so dropping it would lose data.
4. On a corrupt record at `offset`, or a `CutShort` segment: write the bytes from `offset` to the end of its segment to `tally.log.corrupt-<offset>` in the store directory, `fsync` it, remove the segment's hint, and `fsync` the directory. Only then truncate the segment at `offset` and `fsync` it. The side file is durable before any byte leaves the log. Later segments are kept: their records are newer and intact.
5. Return `Repaired { offset, dropped, side_file }`.

Repair is idempotent. A crash before the truncate leaves the log as it was, and the rerun rewrites the same side file. After the truncate the log is clean, so a rerun returns `None`.

If the side file already exists, repair rewrites it only when its bytes are a prefix of what it would move, with zeros where a write never landed, which is what a crashed run leaves. Anything else means an earlier repair at the same offset saved bytes the log no longer has (the log was cut there, new records were written, and the one at that offset went bad too). Repair then fails with `SideFileExists(path)` and changes nothing: moving the file aside is the operator's call, not something to do silently.

Possible later improvement: when the record after the bad one parses cleanly, drop only the bad record.

## Compaction

Compaction cleans segments: it copies their live records to the end of the log and deletes them. `compact()` cleans every segment; automatic compaction cleans only the deadest (see When it runs). To clean a set of segments:

1. If the last segment holds anything, start a new one (as for a write), so the copies go to a segment of their own, which then takes new writes.
2. Read each chosen segment in one buffered pass, checking every record's checksums, and copy the bytes of each Set record the index still points at. Each copied record's index entry is pointed at its new offset as it is copied, and its old offset is kept, 8 bytes per live key, so a failure can point them back. Each copied record's key and value length also go to the new segment's hint. A corrupt record, live or dead, stops compaction with its offset. One sequential pass beats two positioned reads per live record even counting the dead bytes it reads past: 30 ms against 183 ms for 100k live records.
3. Note each Delete record that must be kept: its key is not live, and a segment older than its own is not being cleaned, so that segment may hold a Set the Delete hides. When every older segment is being cleaned, the Delete is dropped.
4. `fsync` the new segment and its hint, then the directory.
5. Append the kept Delete records, one write and one fsync.
6. Remove the cleaned segments and their hints, oldest first, with a directory fsync after each.

Until step 6 the cleaned segments are untouched, so a failure or crash leaves the store as it was, with extra copies that are newer than the records they copy and say the same. After a failure in steps 2 to 4 the moved entries are pointed back, the new segment is truncated to empty and fsynced, and its hint removed with a directory fsync, since a stale hint for a segment that later grows past it would look valid. A failed reread of a moved key panics, so a restart rebuilds the index.

The order in step 6 is what keeps deleted keys deleted. Say segment A holds `a=1` and the newer B holds `delete a`, and both are cleaned: the Delete is dropped. Removing B first and crashing before A is gone would bring `a=1` back. Removing A first, the only states a crash can leave are both, B alone, or neither. The fsync after each removal keeps a file system from making the second removal durable before the first. The Deletes kept in step 5 are durable before any removal for the same reason.

The live records of the cleaned segments all land in one new segment, which can exceed `segment_bytes`: `compact()` on a 1.3 GB log writes one 1.3 GB segment.

What cleaning only some segments buys is shorter pauses. Loading 10M keys of 100 bytes in batches and then overwriting 20M random keys in batches of 10,000, with the default 256 MiB segments and automatic compaction: before segments, two compactions each rewrote the whole log and stalled one batch for 8.05 and 9.01 s; with segments the slowest batch took 0.88 s, as each compaction copied a few segments. The batch p99 stayed at 17.5 ms, the 20M overwrites took 32.2 s against 43.2 s, the directory peaked at 2,672 MB against 2,815 MB, and memory at 923 MB against 890 MB. `compact()` itself costs a little more for the extra directory fsyncs: 34.6 ms against 30.9 ms at 10k live records, 42.0 against 40.1 ms at 100k. `open` of an empty store went from 71 to 140 µs, for listing the directory and checking the marker.

### Hint file

Each segment compaction writes gets a hint, `tally.<base>.hint`, that lets `open` add its entries to the index without reading values. Format (`src/hint.rs`), little-endian:

```text
magic "TLYH" | version u8 | entries | count u64 | log_len u64 | crc32c u32
entry: key_len u16 | value_len u32 | key
```

- Offsets are not stored. A segment compaction wrote starts with exactly the copied records end to end, so each offset is the segment's base plus the sum of the record lengths before it, and the sum of them all must equal `log_len`, the bytes it covers. That doubles as a check that the hint and the segment agree. Records after `log_len` (writes after the compaction, and the Delete records step 5 appended) are scanned.
- The count and log length trail the entries because files are only written at the end and neither is known until compaction finishes. `open` reads the trailer first, with one positioned read, to size the index for `count` keys, never more than the log could hold (`log_len / 17`, the shortest record), since the count is unchecked until every entry is read.
- The checksum covers every byte before it. Entries go into the index as they are read, into the index every segment shares. If anything fails, what this hint added can't be told apart from what earlier segments did, so the hint is removed and the whole index is built again without it.
- `open` reads the hint on one thread, checking every entry's lengths, and gathers checked entries into 16 MiB windows cut into chunks of 8,192 entries, recording where each chunk starts in the log. Each window's chunks are then hashed and inserted by up to 8 threads (the calling thread is one), into the sharded index (see Index), while I/O stays on the calling thread. Hints under 100,000 entries are read on one thread. Splitting needs no change to the format, because the walk that checks lengths also finds where entries start; the cost of not storing chunk offsets is the one sequential pass, which was already there.

On `open`, a hint is used only if its magic, version and checksum hold, every key and value length is in range, the entries add up to `count` distinct keys and exactly `log_len` bytes, and its segment is at least `log_len` long. A hint whose segment is gone is removed. Records after `log_len` are then read as usual. A hint that fails any check is logged, removed and the directory fsynced, and its segment is read in full. The removal must be durable: a hint rejected because the segment is shorter than it says would look valid again once new writes grew the segment past `log_len`.

`repair` removes the segment's hint before it truncates the segment, in the same directory fsync that makes the side file durable.

The cost is that `open` no longer checks the checksums of records the hint covers. Damage there shows up as a corruption error from `get`, or from the next compaction, which reads every record; `repair` then finds it with a full scan. This is the Bitcask trade.

At 10M live records of 100 bytes, `open` from the hint took 1.54 to 1.61 s and peaked at 718 MB, against 2.11 s and 1.23 GB for a full scan (before the index stopped storing keys; see Index). With the 128-bit index it took 1.79 to 1.88 s at 557 MB, and reading on 8 threads took that to 0.61 to 0.67 s at 586 MB: the extra 29 MB is the window and per-thread buffers. The hint is 22 bytes per key here (220 MB). With 100 values of 16 MiB, `open` went from 1.18 s to 38 ms.

### When it runs

`Store` counts **dead bytes** per segment: overwritten values, deleted values, Delete records and Batch records. `open` computes each segment's from the index as its length minus the live records in it; writes add to the segment that holds what died.

`Options { auto_compact: Option<AutoCompact>, segment_bytes: u64 }` sets the trigger. `AutoCompact::new(dead_percent, min_log_bytes)` (1 to 100 percent) compacts when both hold, across the whole log. It then cleans segments with dead bytes, the largest share of dead first, until the log would no longer be due, counting each one's dead bytes as gone: the least copying for the space it frees. With one segment that is the whole log, as before segments. The default is 50% of a log of at least 1 MiB. At 50%, each compaction copies at most as many live bytes as the dead bytes written since the last one, so compaction at most doubles the bytes written, and the log stays under twice its live size. The floor keeps small stores from compacting every few writes.

The trigger is checked:

- **On `open`**, after recovery. The scan already happened, so the check is free. Startup pays for at most one rewrite.
- **After each `set` or `delete` that writes.** That call takes the compaction's time: 38 ms for 100k live records of 100 bytes on an M1 Pro (`cargo bench -- compact`), plus reading past the dead ones. About 12 ms of that is the hint's fsync and the directory fsync that removes the old hint, each a `F_FULLFSYNC` on macOS.

Not on drop: `Drop` cannot return errors, and a kill skips it anyway. Not in the background: that needs concurrent reads and writes, which arrive with the TCP server.

An automatic compaction never turns a successful write or `open` into an error: the write is already durable and the old log untouched. It logs a warning instead and turns automatic compaction off until the next `open`, so a persistent failure (a corrupt live record, a full disk) does not make every later write retry a full rewrite. `compact()` called directly returns its error.

## File system

`Store<F: Fs = OsFs>` reaches the disk only through two traits in `tally::fs`, so the power-cut tests can swap in an in-memory disk:

- `Fs`: `create_dir`, `open_or_create`, `open_existing`, `rename`, `remove`, `hard_link`, `same_file`, `sync_dir`, `list`, with `type File: LogFile`.
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
| Corruption    | bad checksum, invalid record, cut-short or overlapping segment, with the log offset | run `repair`  |
| Unsupported   | unknown record type, lengths over current limits   | upgrade tally                             |
| Invalid input | empty key or value, over the limit                 | fix the input                             |
| Locked        | store already open in another `Store` or process   | close the other one, or wait              |

Bugs (broken internal invariants) panic. Errors are for failures that can happen when the code is correct. The deliberate exceptions are a failed `fsync` and a failed truncate after a failed write, which panic (see Durability contract).

## Testing

- **Kill test** (`tests/kill.rs`): the test binary runs itself as a child that applies a seeded sequence of set, delete, reopen, compact and batch, with automatic compaction at 50% dead and no size floor and 16 KiB segments, printing an ack after each `Ok`. The parent `kill -9`s it after a random delay, three times on the same directory without opening it in between, so kills also land in the next child's `open` and recovery. Then the parent opens the store and checks that `open` succeeds, that the state is that of every acknowledged operation with each round's in-flight operation applied or not, and that a write after recovery survives a reopen. `TALLY_KILL_RUNS` (default 20) and `TALLY_KILL_SEED`; CI runs 1,000 on Linux. This cannot detect a missing `fsync`, because the OS page cache survives process death.
- **Power-cut simulation** (`src/power_cut.rs`, on the in-memory disk in `src/mem_fs.rs`): runs a workload and cuts the power before every step it takes, a step being any write, truncate, file sync, directory create, rename or directory sync. The workload includes explicit compactions and batches and runs with automatic compaction at 50% dead, no size floor and 64-byte segments, so most writes start a segment and most compactions clean some segments and keep others; the power is also cut after its last step. For each cut it tries every way the disk could be left: any prefix of the unsynced changes landing, the next write landing torn or as zeros, and any of the changes to directory entries since the directory's last sync: each create, remove or rename lands whole or not at all, in any combination that keeps the changes to each name in order, and everything under a lost directory is lost. A directory sync makes the directory's entries exactly what they are now. After each, `open` must succeed with every acknowledged write, and recovery is itself cut at every step and checked the same way. Separate tests cut `repair` at every step (no byte is ever missing from both the log and the side file), cut the move from one file to segments at every step, and check that what `open` served from a dead process's unsynced write survives a cut. Deleting any one fsync or directory sync from `open`, `append`, `repair` or `compact` fails these tests, with two exceptions, both from the hint file. The hint's own fsync: a torn hint fails its checksum and `open` reads the whole log, so correctness never rests on it, and it is kept so that no 32-bit checksum ever has to vouch for one. The directory fsync before compaction removes the cleaned segments (step 4): it makes only the new hint's name durable, and without the hint `open` reads that segment in full. It is kept so that a crash during the removals still leaves the hint. A single directory fsync after all the removals, instead of one after each, is caught (a deleted key comes back), as is removing the cleaned segments newest first. Letting unsynced directory changes land in any combination, not only all or none at the next directory sync, also found two states that a crash on a real disk can leave and that had to be handled: a `tally.log` whose name landed with its marker still zeros, which `open` refused as records written by an earlier version, and a side file whose name landed with its bytes still zeros, which `repair` refused to overwrite.
- **Corruption tests:** truncated tails, zero-filled tails, flipped bits in the header, payload, and lengths, mid-file corruption.
- **Model test** (`tests/model.rs`): random sequences of set, get, delete, batch and reopen against a `HashMap`, with 100-byte segments, checking every read and the log length after every step. Real fsyncs, so the default case count is small; `PROPTEST_CASES=2000 cargo test --test model` for a long run.
- **Codec properties** (`record.rs`): round trip, any one-byte change is rejected by the checksum covering it, `decode_header` accepts exactly the valid headers, and a valid header with a matching payload always decodes.
- **Fuzzing** (`fuzz/`, nightly and `cargo-fuzz`): `cargo +nightly fuzz run <target>` with targets `decode` (arbitrary bytes, optionally with checksums fixed up so the fuzzer gets past them), `open` (logs built from arbitrary records and batches, then damaged), `ops` (the model test, coverage-guided, also with 100-byte segments), and `hint` (a compacted store with more writes after it and a hint changed, truncated or replaced with arbitrary bytes: `open` must still read back every key). Fuzz builds skip fsync: durability cannot be observed there and fsync would cut throughput a hundredfold.
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
- A cache of open segment files, for logs of more than a few hundred segments (see Segments).
