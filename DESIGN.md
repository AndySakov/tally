# tally design

A single-node, crash-safe, log-structured key-value store. Version 1 is an embedded library.

## Scope (v1)

- API: `open(dir)`, `open_with(dir, options)`, `set(key, value)`, `get(key) -> Option<value>`, `delete(key)`, `write(batch)`, `compact()`, `verify()`.
- `open` takes a directory. It holds the log's segments (`tally.<base>.log`), a hint for each segment a compaction wrote or `open` read in full (`tally.<base>.hint`), `tally.lock`, and any side files that recovery saved bytes to (see Recovery).
- `delete` of a missing key is a no-op: no record is written and it returns `Ok`.
- Keys are non-empty byte strings. Values may be empty.
- One process, many threads. A `Store` is `Send` and `Sync`, and every method takes `&self`: threads share one through an `Arc`. Reads run alongside writes and compaction; writes from several threads share fsyncs (see Group commit). `open` takes an exclusive lock (`flock`) on `tally.lock`, a separate file because compaction removes segments and a lock on one of them would not outlive it. A second `open` fails with `Locked` until the first `Store` is dropped or its process exits.

## On-disk format

The log is a sequence of append-only segment files (see Segments). Each starts with a 17-byte file header, then records. Every record is a 19-byte header followed by the payload.

| Offset         | Size        | Field              | Notes                            |
| -------------- | ----------- | ------------------ | -------------------------------- |
| 0              | 4           | `header_checksum`  | CRC32C of the record's log offset (u64), then bytes 4..19 |
| 4              | 1           | `type`             | `0x31` Set, `0x32` Delete, `0x33` Batch; `0x40` added inside a batch |
| 5              | 2           | `key_len`          | u16                              |
| 7              | 4           | `value_len`        | u32                              |
| 11             | 8           | `payload_checksum` | CRC-64/NVME of key + value       |
| 19             | `key_len`   | key                |                                  |
| 19 + `key_len` | `value_len` | value              | empty for Delete                 |

- All integers are little-endian.
- The header checksum is CRC32C (Castagnoli), the payload checksum CRC-64/NVME (the NVMe and S3 CRC-64). The variants are part of the format.
- `header_checksum` covers `payload_checksum`, so the payload checksum is itself protected.
- `header_checksum` also covers where the record is, so a valid record anywhere else fails it: stale blocks a file system hands back after a crash (ext4 with `data=writeback`), a record copied from elsewhere in the log or from another store. CRC32C tells any two offsets that differ by one apart, whatever the carry. It costs nothing measurable: `open` of 100k records took 15.3 ms against 16.0 ms without it.
- The payload checksum is 64 bits because it is what stands between bit rot and a wrong value. A corrupted payload passes CRC32C 1 time in 2³², and passes CRC-64 1 time in 2⁶⁴; CRC-64 also catches every error confined to 64 bits, against 32. The header stays at CRC32C: a damaged header that passes it gives wrong lengths, and the payload checksum over the wrong bytes then fails, so the damage is still reported, not returned. With `crc-fast`, CRC-64 is faster than CRC32C from about 100 bytes (15 against 20 ns at 117, 34 against 77 ns at 1 KiB on an M1 Pro).
- The header has its own checksum so lengths can be trusted before they are used. A single record checksum cannot tell a torn tail from a mid-file record with a corrupted length, and treating the second as the first would silently drop acknowledged records.
- Type `0` is invalid so a zero-filled region can never parse as a record. The high nibble, `3`, is this version of the format; types from earlier versions are unknown.
- An append of one record is that record. An append of more is one Batch record around them: a Batch record has no key and an 8-byte "value", `body_len` (u64), the byte length of the records that follow it and belong to it. Each of those has `0x40` in its type, so a record inside a batch is never taken for one written on its own (see Recovery). They are ordinary records otherwise, with their own checksums, so `get` reads one the same way whether or not it was written in a batch.

### Segment file header

| Offset | Size | Field      | Notes                     |
| ------ | ---- | ---------- | ------------------------- |
| 0      | 4    | magic      | `TLYS`                    |
| 4      | 1    | version    | `3`                       |
| 5      | 8    | base       | u64, the segment's base   |
| 13     | 4    | checksum   | CRC32C of bytes 0..13     |

- The header sits outside the log's offsets: `Segment` adds its length when it translates a log offset to one in the file, so the index, errors and records see only records.
- A file whose magic or version is not this one is refused with `Unsupported` (`OtherFormat`) instead of being read as damaged records, and so is a directory holding `tally.log`, the one-file log of the first versions. There is no upgrade in place: stores from before version 3 must be read by the version that wrote them. The other direction is not guarded: a tally from before version 3 doesn't check segment headers, so it can misread a version 3 store (one from before segments sees it as empty). No such version was released, and every version from 3 on checks the header's version, so a later format change is refused both ways.
- A header whose checksum fails or whose base is not the one in the file's name is `SegmentHeader` corruption at the base: a copied or renamed segment would otherwise put its records at the wrong offsets.
- A segment's header is written and fsynced before its name is made durable and before any record goes in. A newest segment no longer than a header that is not the header (cut short, zeros, or stale blocks a crash exposed) holds no record, so `open` writes the header again, unless it names another version. In an older segment that is `CutShort` corruption, which `repair` moves aside (see Repair).

### Record validity

A record whose header checksum passes is still invalid if any of these hold:

- `type == 0`
- `key_len == 0`, for Set and Delete
- Delete with `value_len != 0`
- Batch with `key_len != 0` or `value_len != 8`, or inside a batch (`BadBatch`)

## Limits

Policy constants in code, not part of the format. The format allows keys up to 64 KiB and values up to 4 GiB, so the limits can be raised later without migrating files.

|       | Max    | Why                                                        |
| ----- | ------ | ---------------------------------------------------------- |
| Key   | 1 KiB  | Every key lives in the in-memory index                     |
| Value | 16 MiB | Caps the allocation of a single `get` and of startup reads |

Keys and values are newtypes (`Key`, `Value`) built with `TryFrom<&[u8]>`. Construction fails on an empty key or input over the limit, so `set` cannot receive invalid input.

## Durability contract

`set` and `delete` return `Ok` only after the record has been written and `fsync`ed. An acknowledged write survives both a process crash and power loss.

v1 has one sync policy: `fsync` on every write. A batched policy (`Interval`) is deferred. When added, it comes with `sync()`, `close(self) -> Result<()>`, and a best-effort sync in `Drop`.

### Write path

1. Build the group's records in memory, inside one Batch record if there are more than one: for each, the payload, then `payload_checksum`, then the prefix, then `header_checksum` over its offset and the prefix.
2. Append them with a single write to the end of the file.
3. `fsync` the file.
4. Update the index.
5. Return `Ok`.

### Group commit

A write's cost is its fsync, about 5 ms here, so writes from several threads share one. A write joins a queue. If no group is being written, its thread becomes the leader: it takes the oldest queued writes, up to 1,024 writes or 4 MiB, encodes them in order into one buffer, appends it with one write, fsyncs, updates the index, and hands each waiting thread its result. If its own write was not among them, it leads again. Writes that arrive meanwhile queue for the next leader. A group's results go out before any automatic compaction it makes due, which the leader then runs: the writes that made it due return at once, and the next group waits for it. There is no dedicated thread, and a write alone is written at once.

- The leader decides what each write does in queue order, with the group's earlier writes in view, as `write` does within a batch: a delete of a key an earlier write in the group set is written, and a delete of a key that is not live by then writes nothing.
- A group is one append, one record or one Batch record, so a power cut keeps all of it or none, and recovery treats a group cut short as any torn tail.
- A failure fails every write in the group with the same error: none of them reached the index.
- The index is updated only after the fsync, and a segment's length grows before the entries that point past its old end, so a read never sees a write that is not durable.
- **The leader waits for its group to fill.** Threads that write again as soon as a write returns wake within microseconds of each other, but the first one back would lead alone and spend a whole fsync on one write, so groups would alternate between 1 and the rest. If the last group had more than one write, a new leader waits until the queue holds as many as that group did, or until 10% of the last fsync's measured time has passed, whichever comes first. A new write wakes it. After a group of one it doesn't wait, so a writer alone never does, and on a disk with fast fsyncs the wait is as short. When fewer threads are writing, one leader waits out the 10% and the next expects the smaller group. `set` of 1 KiB from 1, 4, 16 and 64 threads: 192, 439, 1,490 and 5,540 writes/s without the wait; 194, 778, 2,736 and 9,739 with it (`cargo bench -- "concurrent set"`). Waiting 25% or 50% gained nothing more. With threads that pause a random 0 to 10 ms between writes, as server traffic would, writes already pile up during each fsync, and the wait changed neither throughput nor latency beyond run-to-run noise (16 threads: about 1,000 writes/s, p50 7.8 to 8.0 ms, both ways).

### Failed fsync

A failed `fsync` truncates the group away, fsyncs the truncation, and poisons the store: the write gets the I/O error, every write in its group gets it too, and from then on every write, `delete` and `compact` fails with `Poisoned` until the store is reopened. Reads keep working from the index, which never saw the failed writes.

On ext4 and XFS a failed writeback marks the pages clean while reads still see their new bytes, and a file descriptor opened later is never told of the error ([Rebello et al., ATC '20](https://www.usenix.org/conference/atc20/presentation/rebello)). Without the truncate, a reopen would read the failed group back as valid records, writes reported failed would come back, and the group's pages would never be written: later acknowledged writes would follow a hole that a power cut turns into mid-file corruption. A retry of the fsync could report success for the same reason, so tally stops writing instead. Postgres makes the same choice, though it stops the whole process.

One window is left: the process dying between the failed fsync and the truncate. The reopen then reads the group from the page cache, and nothing on disk says it failed.

A failed directory fsync poisons the store too, once it is open: when starting a segment, and after compaction removes one. A retry would fsync a new descriptor, which Linux need not tell about the failure. During `open` a failed directory fsync fails the open, including one in the automatic compaction `open` runs.

### Failed write

If the append fails partway (disk full, I/O error), the leader truncates the file back to where the group started and fsyncs it, and every write in the group returns the I/O error. Without that fsync, a crash could bring back the group's first records, complete and valid, from writes reported failed. A partial record left at the tail would put the next append after torn bytes, and the following startup would see mid-file corruption. The store stays writable. If the truncate itself fails, the store is poisoned (see Failed fsync).

### New files

`open` syncs the newest segment, the store directory, and the directory's parent on every start, in that order, whether or not it created them. The file name lives in the directory, and without the directory sync the file itself may not survive power loss. Syncing unconditionally makes `open` idempotent: if a previous `open` failed between creating and syncing, the next one completes it. Directory sync behaviour differs between Linux and macOS and must be tested on both.

## Batches

`Batch` collects sets and deletes in order; `write(batch)` makes all of them durable with one append and one fsync, and recovery applies all of them or none (see Recovery). It is how a single thread gets group commit (see Group commit for several): the cost of a write is its fsync, so sharing one fsync across a batch divides it. 1 KiB values on an M1 Pro: 212 writes/s one at a time, 2,019/s in batches of 10, 18,663/s in batches of 100, and 100,810/s in batches of 1,000.

- Operations apply in order, so a delete sees the batch's own earlier sets. A delete of a key that isn't live at that point writes nothing, as `delete` does. A batch that writes nothing writes nothing, and one that writes one record writes it on its own.
- The 27-byte Batch record counts as dead bytes from the start. Compaction copies the live Set records out of batches like any others; the compacted log has no batches.
- The whole batch is encoded in memory before the write, so its size is bounded by memory, not by the format (`body_len` is a u64).

## Segments

The log is split into segment files, each named for the log offset of its first record: `tally.<base, 20 digits>.log`, so sorting the names sorts the log. Offsets everywhere (the index, errors, `Repaired`, side-file names) are offsets into the whole log; `Segment` (`src/segment.rs`) implements `LogFile` by subtracting its base and adding its file header's length, so the scan, `get` and `verify` read every segment with the same code. The index entry stays 16 bytes. The log's offsets only grow: compaction appends copies at the end and deletes the old segments, so the log has holes where they were.

- Writes go to the last segment. Before a write, if that segment holds at least `Options::segment_bytes` (default 256 MiB), a new one is created at the end of the log, its header fsynced, then the directory, so its name is durable before any acknowledged write lands in it. A write is one append, so a batch never spans two segments; a segment can exceed `segment_bytes` by one group of writes (see Group commit), and a compaction's output by any amount.
- Only the newest segment can have a torn tail: every other was complete and fsynced before the next was written. In any other segment, a tail `open` would truncate is `CutShort` corruption instead, for `repair`.
- Segments must not overlap: a segment that starts before the previous one ends fails `open` with `SegmentsOverlap`. Nothing tally does can produce one; it means files were copied in from elsewhere.
- Every segment stays open for the life of the `Store`, one file descriptor each. At 256 MiB that is 4 per GiB of log: the default soft limit of 256 per process on macOS is reached around 60 GiB, and Linux's 1,024 around 250 GiB. Past that, `open` or the write that starts a segment fails with the OS's "too many open files"; raise `ulimit -n` or `segment_bytes`. A cache of open files would lift the limit but give every read path a handle to open and evict behind `&self`; RocksDB's default (`max_open_files = -1`) makes the same trade of keeping everything open.

## Recovery (startup)

`open` reads every segment in log order into one index. For each: if its hint is usable (see Hint file), the records it describes are applied to the index in order and `pos` starts where the hint ends; otherwise `pos` starts at the segment's base. `len` is the segment's end. Loop:

1. `pos == len`: clean end, stop.
2. Fewer than 19 bytes remain, or fewer than the record's length: cut short.
3. Read the header. If `header_checksum` fails: damaged.
4. Header is trusted. Unknown type, or lengths over the limits: fail with an unsupported-record error (likely written by a newer version), never truncated. A record inside a batch, or a validity violation: damaged.
5. Read the payload. If `payload_checksum` fails: damaged.
6. Set inserts `key -> Entry { offset: pos, value_len, key_len }`. Delete removes the key. A Batch is handled as described below.
7. `pos += 19 + key_len + value_len`. Repeat.

A Batch record at `pos` is followed by `body_len` bytes of records, and the batch is all or nothing:

- If fewer than `body_len` bytes follow, the batch was cut short.
- Otherwise read its records with the same rules. Their operations are held back until the whole body has been read and checked.
- A record without `0x40`, a Batch inside the body, or a record that runs past the body's end is `BadBatch` at `pos`.
- Any other error inside the body is reported at `pos`, with its own reason, not at the record, so the torn-tail rule and `repair` treat the whole batch as one and never leave part of one in the log.
- Once the body checks out, apply its operations in order and continue at `pos + 27 + body_len`.

**Torn tails.** A record cut short or damaged at `pos` in the newest segment is a torn tail if no record written on its own starts anywhere after `pos`: no header whose checksum passes at its offset and whose type lacks `0x40`, whatever its type and lengths, so a record from a newer version is never cut. Each append is one record, so everything from `pos` on is then the bytes of the last append, which a crash may have landed in any order: cut short, zeros where blocks never landed, later blocks without earlier ones. The search tries every offset after `pos` and stops at the first header that checks, so it costs a CRC per byte of the tail and only runs on damage. If a header does check, the damage is in the middle of the log: `open` fails with corruption at `pos` and leaves the log alone for `repair`. In any other segment, every damage is corruption: each was complete and fsynced before the next began.

The rule before version 3 called damage torn only when every byte after it was zero, so a write whose end landed before its start failed `open` as corruption. The flag on records inside a batch is what keeps a torn batch's own records from looking like a later append.

A torn tail is never dropped silently. Unless every byte of it is zero, `open` writes it to `tally.log.torn-<pos>-<CRC32C of the bytes, 8 hex digits>`, fsyncs it, then the directory; then it truncates the segment to `pos`, fsyncs it, and logs a warning (`log` crate): `truncated a torn tail at offset 22: moved 3 bytes to <path>`, or `dropped 14 zero bytes`. The caller sees it only if it installs a logger. The truncate must complete before the first new write. Otherwise the next append lands after the torn bytes, and the following startup sees mid-file corruption. Bit rot in the last record looks the same as a torn write and is handled the same way: its bytes are saved, and the side file holds the record whole.

A value whose bytes form a valid header for its own offset, which takes a CRC32C chosen on purpose, inside a torn append would make the tail look like mid-log damage: `open` then fails and `repair` moves the bytes aside. It never makes damage look torn.

Recovery is idempotent: a crash during recovery followed by a rerun yields the same result. A rerun saves the same bytes to the same side file.

A scan of 16 MiB or more (from `pos` to the segment's end) runs on several threads. This thread reads and checks the records as above, and hands the operations to an applier thread 65,536 at a time, then reads on while that window is applied. The applier (`Index::apply_in_order`) hashes the window's keys on up to 8 threads, then has each thread apply, in log order, the operations for the shards it owns. Every operation on a key falls in that key's shard, so they still apply in the order they were written. The window read before damage is applied too, since the damage may be a torn tail. Segments written by appends have no hint, so in steady state most of a log is scanned: at 10M keys after 20M random overwrites (2.6 GB in 10 segments, 7 with hints), `open` takes 1.8 s against 3.8 s on one thread. What remains is mostly this thread's reading: a `pread` per MiB, the checksums, and an allocation per key.

### Repair

`open` never discards data, and saves what it cuts. `Store::repair(dir) -> Result<Option<Repaired>, Error>` handles the rest:

1. Take the same exclusive lock as `open`. While a `Store` is open, repair fails with `Locked`.
2. Scan every segment in order, reading every record (hints are not used). If `open` would succeed, return `None`, having changed only what `open` changes before it reads (it removes a crashed compaction's copy and hints without a segment, and writes a cut-short newest header again): a torn tail of the newest segment is left for `open` to truncate.
3. An unsupported record is returned as the error, not repaired. A newer tally can read it, so dropping it would lose data.
4. On a corrupt record at `offset`, or a `CutShort` segment: write the bytes from `offset` to the end of its segment to `tally.log.corrupt-<offset>` in the store directory, `fsync` it, remove the segment's hint, and `fsync` the directory. Only then truncate the segment at `offset` and `fsync` it. The side file is durable before any byte leaves the log. Later segments are kept: their records are newer and intact.
5. On a segment whose file header is damaged, or cut short in a sealed segment, none of its records can be trusted at the offsets they claim: write the whole file, header included, to `tally.log.corrupt-<base>`, `fsync` it, remove the hint and `fsync` the directory, then rename an empty segment with a fresh header over it and `fsync` the directory again.
6. Return `Repaired { offset, dropped, side_file }`.

Repair is idempotent. A crash before the truncate leaves the log as it was, and the rerun rewrites the same side file. After the truncate the log is clean, so a rerun returns `None`.

If the side file already exists, repair rewrites it only when its bytes are a prefix of what it would move, with zeros where a write never landed, which is what a crashed run leaves. Anything else means an earlier repair at the same offset saved bytes the log no longer has (the log was cut there, new records were written, and the one at that offset went bad too). Repair then fails with `SideFileExists(path)` and changes nothing: moving the file aside is the operator's call, not something to do silently.

Possible later improvement: when the record after the bad one parses cleanly, drop only the bad record.

## Compaction

Compaction cleans segments: it copies their live records to the end of the log and deletes them. `compact()` cleans every segment; automatic compaction cleans only the deadest (see When it runs). To clean a set of segments:

1. The copies go to a new segment at the end of the log, which then takes new writes. It is written as `tally.<base>.compact`, with its file header, so no segment holds copies until all of them are durable.
2. Read each chosen segment in one buffered pass, checking every record's checksums, and copy the bytes of each Set record the index still points at. Each copied record's old offset is kept, 8 bytes per live key, and its key and value length go to the new segment's hint. The index is not touched yet: reads go on from the old segments. A corrupt record, live or dead, stops compaction with its offset. One sequential pass beats two positioned reads per live record even counting the dead bytes it reads past: 30 ms against 183 ms for 100k live records.
3. Note each Delete record that must be kept: its key is not live, and a segment older than its own that is not being cleaned may hold a Set of it, which the Delete hides. Each sealed segment has a filter of the keys it holds a Set record for, live or dead (`src/filter.rs`), so a Delete is dropped unless an older kept segment's filter says yes.
4. `fsync` the copy, rename it to `tally.<base>.log` (over the last segment, if that is empty and so has the same base), and `fsync` the directory. Then write the hint's trailer and `fsync` the hint. The trailer waits for the copy's fsync because nothing orders writes to two files: a complete hint could otherwise reach the disk ahead of the copies it lists, and `open` would trust it over the intact originals.
5. Point the index at the copies, reading their keys back from the hint just fsynced, so no key is held in memory. Each entry moves only if it still points at the old offset, checked and changed under its shard's lock, so a write that landed since is never undone; a copy left behind counts as dead. A read that took the old entry and then finds its segment gone looks the key up again (see Reads).
6. Append the kept Delete records, one write and one fsync.
7. Remove the cleaned segments and their hints, oldest first, with a directory fsync after each.

Until step 7 the cleaned segments are untouched, so a failure or crash leaves the store as it was, with extra copies that are newer than the records they copy and say the same. A failure before the rename removes the copy and its hint; the log never saw them, so even a failed fsync of the copy leaves the store writable. `open` removes any `.compact` file a crash left, and any hint without its segment. A failed directory fsync after the rename poisons the store (see Failed fsync). If writing or rereading the hint fails in steps 4 or 5, the hint is removed, the entries already moved point at durable copies and the rest at the old segments, so compaction returns the error and keeps both, and the dead bytes are counted again from the index.

Writes wait while a compaction runs; reads do not.

The order in step 7 is what keeps deleted keys deleted. Say segment A holds `a=1` and the newer B holds `delete a`, and both are cleaned: the Delete is dropped. Removing B first and crashing before A is gone would bring `a=1` back. Removing A first, the only states a crash can leave are both, B alone, or neither. The fsync after each removal keeps a file system from making the second removal durable before the first. The Deletes kept in step 6 are durable before any removal for the same reason.

The live records of the cleaned segments all land in one new segment, which can exceed `segment_bytes`: `compact()` on a 1.3 GB log writes one 1.3 GB segment.

Before the filters, a Delete was kept while any older segment was, whatever keys it held. A segment of keys that never change is never the deadest, so it is never cleaned, and every Delete written after it was copied forward at every compaction, forever: a workload that sets and deletes keys, such as a queue, grew the log without bound. A filter is built the first time a compaction needs it, from the segment's hint and the records after it, or from all its records, and kept until the store is closed: a sealed segment never changes.

The filter is a binary fuse filter (`xorf`) with 16-bit fingerprints: about 18 bits per Set record, against the index's 32 bytes per live key, and a wrong yes 1 time in 65,536 per segment asked. A wrong yes keeps a Delete for as long as the filter lives, since the filter gives the same answer for the same key: each filter hashes under a random key of its own, so a reopen ends it. A Bloom filter of 10 bits per key is wrong about 1 time in 100, and in the measurement below still grew the log by 0.25 bytes per key.

Measured with 2,000 keys that never change, then 100,000 keys each set and deleted in batches of 200, with 64 KiB segments and automatic compaction at 50%: the directory grew by 30 bytes per key (every Delete, carried forever) to 3.29 MB and took 74.7 s before; it stays between 455 and 481 KB and takes 15.9 s with the filters.

What cleaning only some segments buys is shorter pauses. Loading 10M keys of 100 bytes in batches and then overwriting 20M random keys in batches of 10,000, with the default 256 MiB segments and automatic compaction: before segments, two compactions each rewrote the whole log and stalled one batch for 8.05 and 9.01 s; with segments the slowest batch took 0.88 s, as each compaction copied a few segments. The batch p99 stayed at 17.5 ms, the 20M overwrites took 32.2 s against 43.2 s, the directory peaked at 2,672 MB against 2,815 MB, and memory at 923 MB against 890 MB. `compact()` itself costs a little more for the extra directory fsyncs: 34.6 ms against 30.9 ms at 10k live records, 42.0 against 40.1 ms at 100k. `open` of an empty store went from 75 to 120 µs (median of 5,000), 21 µs of it to list the directory.

### Hint file

A hint, `tally.<base>.hint`, describes the records at the start of a segment, so `open` can apply them to the index without reading values. Compaction writes one for each segment it writes, and `open` for each sealed segment it reads (see Hints written by open). Format (`src/hint.rs`), little-endian:

```text
magic "TLYH" | version u8 | entries | count u64 | log_len u64 | crc64 u64
entry: type u8 | key_len u16 | value_len u32 | key
```

- Version 3. An entry is its record's header without the two checksums (bytes 4 to 11), then the key: Set, Delete and Batch records each have one, and an entry must pass the same type, length and validity rules as a header. A Batch entry has no key; the records of its body follow it as their own entries, with `0x40` in their types. The checksum is CRC-64/NVME, for the same reason as the records': a hint that passes it wrongly would put wrong offsets in the index, and compaction would then drop what they hide.
- A hint of another version is rejected like any other bad hint: removed, and its segment read in full.

- Offsets are not stored. A hint has an entry for every record from the segment's start, end to end, so each offset is the segment's base plus the sum of the record lengths before it, and the sum of them all must equal `log_len`, the bytes it covers. That doubles as a check that the hint and the segment agree. Records after `log_len` (writes after a compaction, and the Delete records its step 5 appended) are scanned.
- `count` is the number of Set entries, at most the keys the hint adds. `open` reads every trailer first, with one positioned read each, and sizes the index for the largest count, never more than the log could hold (its length over 17, the shortest record), since the count is unchecked until every entry is read. Not the sum: a key overwritten in every segment would be counted once per segment. On the store below that sum was about 20M for 10M live keys, and sizing for it took the peak from 854 MB to 1.14 GB; past the largest hint, the map grows a shard at a time.
- The count and log length trail the entries because files are only written at the end, and neither is known until the hint is done.
- The checksum covers every byte before it. It is computed under the read buffer, a buffer at a time: fed an entry at a time, CRC-64 took most of the time to read a hint, and moving it took `open` of a compacted 10M-key store from 0.70 to 0.40 s. Entries go into the index every segment shares before the checksum is checked. If anything fails, what this hint changed can't be told apart from what earlier segments did, so the hint is removed and the whole index is built again without it.
- `open` reads the hint on one thread, checking every entry, and gathers their changes into windows of 65,536, as a scan does. Each window's Sets and Deletes are applied in order with `apply_in_order` (see Index) by up to 8 threads (the calling thread is one), while I/O stays on the calling thread. Hints with fewer than 100,000 Set entries are applied on one thread. Windows stay small because each one's list of changes stays allocated after use: 16 MiB windows took the peak from 586 to 913 MB. `open` of 2M keys from a hint peaks at 163 MB with windows of 65,536 changes, against 231 MB with the 2 MiB windows of raw entries before.

On `open`, a hint is used only if its magic, version and checksum hold, every entry passes the record rules, the entries add up to `count` Set entries and exactly `log_len` bytes, and its segment is at least `log_len` long. A hint whose segment is gone is removed. Records after `log_len` are then read as usual. A hint that fails any check is logged, removed and the directory fsynced, and its segment is read in full. The removal must be durable: a hint rejected because the segment is shorter than it says would look valid again once new writes grew the segment past `log_len`.

`repair` removes the segment's hint before it truncates the segment, in the same directory fsync that makes the side file durable.

The cost is that `open` no longer checks the checksums of records the hint covers, which, with the hints `open` writes, is every sealed segment after its first `open`. Damage there shows up as a corruption error from `get`, or from the next compaction, which reads every record; `repair` then finds it with a full scan, and `verify` checks every record on demand. This is the Bitcask trade.

At 10M live records of 100 bytes, `open` from the hint took 1.54 to 1.61 s and peaked at 718 MB, against 2.11 s and 1.23 GB for a full scan (before the index stopped storing keys; see Index). With the 128-bit index it took 1.79 to 1.88 s at 557 MB, and reading on 8 threads took that to 0.61 to 0.67 s at 586 MB. The hint was 22 bytes per key here (220 MB); the type byte makes it 23. With 100 values of 16 MiB, `open` went from 1.18 s to 38 ms.

### Hints written by open

Compaction's hints cover only what it copied, so a store that compacts a few segments at a time read most of its log in full on every `open`: at 10M keys of 100 bytes after 20M overwrites, 1,955 of the 2,424 MB had no hint, in three segments never compacted and in the writes that compaction segments took after their hints. `open` now writes the hints such segments lack.

- A sealed segment (every one but the newest) that `open` reads past its hint, or that has none, gets a hint for all of it: the scan hands every record it reads, Batch records too, to a hint writer. A sealed segment never changes, so the hint stays valid until compaction removes both. The newest segment still grows, and gets none.
- A hint that covers only the start of a segment is extended: reading it leaves the checksum's state where its entries end, so the new entries follow the old ones with no rewrite. Its trailer is cut off first and the new entries appended, so the files stay append-only; a write over the old trailer would append anyway on Linux, where `pwrite` ignores the offset of a file opened with `O_APPEND`.
- Neither the hint nor the directory is fsynced. Every state a crash can leave is either a hint that covers less (the old one, or none) or one whose trailer or checksum fails, which the next `open` removes, with a directory fsync, before reading the segment in full. The power-cut tests cut every write of these hints, in both cases: they check 133,333 disk states, against 13,318 before.
- Hints are best effort. A hint that can't be created or written is logged and left, and `open` goes on: the next `open` removes what was written and reads that segment again. Tests fill the in-memory disk partway through a new hint and through an extension, and the next `open` writes the whole hint.
- Entries are applied in order, as the records would be, so a hint can repeat a key and hold Deletes. Hints used to be loaded with `Index::fill`, which is faster only for keys no two threads share and rejected a hint that repeated a key; `apply_in_order`, already used by the parallel scan, is as fast here, and `fill` is gone.

On the store above, `open` went from 1.48 to 1.77 s at 910 MB on `main` to 0.87 to 0.89 s at 854 MB, after one `open` of 2.42 s that wrote 460 MB of hints. Most of what is left is the newest segment, read past its hint every time. Compacted into one segment, the same store opens in 0.40 s, from 0.70 s, at 570 MB, from 586 MB.

### When it runs

`Store` counts **dead bytes** per segment: overwritten values, deleted values, Delete records and Batch records. `open` computes each segment's from the index as its length minus the live records in it; writes add to the segment that holds what died.

`Options { auto_compact: Option<AutoCompact>, segment_bytes: u64 }` sets the trigger. `AutoCompact::new(dead_percent, min_log_bytes)` (1 to 100 percent) compacts when both hold, across the whole log. It then cleans segments with dead bytes, the largest share of dead first, until the log would no longer be due, counting each one's dead bytes as gone: the least copying for the space it frees. With one segment that is the whole log, as before segments. The default is 50% of a log of at least 1 MiB. At 50%, each compaction copies at most as many live bytes as the dead bytes written since the last one, so compaction at most doubles the bytes written, and the log stays under twice its live size. The floor keeps small stores from compacting every few writes.

The trigger is checked:

- **On `open`**, after recovery. The scan already happened, so the check is free. Startup pays for at most one rewrite.
- **After each group of writes that writes something.** The leader runs it after handing out the group's results, so the groups queued behind it take the compaction's time: 38 ms for 100k live records of 100 bytes on an M1 Pro (`cargo bench -- compact`), plus reading past the dead ones. About 12 ms of that is the hint's fsync and the directory fsync that removes the old hint, each a `F_FULLFSYNC` on macOS.

Not on drop: `Drop` cannot return errors, and a kill skips it anyway. Not in the background yet: that is the next change (see Deferred ideas).

An automatic compaction never turns a successful write or `open` into an error: the write is already durable and the old log untouched. It logs a warning instead and turns automatic compaction off until the next `open`, so a persistent failure (a corrupt live record, a full disk) does not make every later write retry a full rewrite. `compact()` called directly returns its error.

## File system

`Store<F: Fs = OsFs>` reaches the disk only through two traits in `tally::fs`, so the power-cut tests can swap in an in-memory disk:

- `Fs`: `create_dir`, `open_or_create`, `open_existing`, `rename`, `remove`, `sync_dir`, `list`, with `type File: LogFile`.
- `LogFile`: `size`, `read_exact_at`, `reader_at(offset) -> impl Read`, `write_all_at`, `set_len`, `sync`, and `try_lock`. Writes go only at the current end: the real file is in append mode, where Linux ignores the offset, and the fake asserts it.

Both traits are sealed: public because `Store`'s type parameter is, but implementable only inside tally. `Store::open` and `Store::repair` are defined on `Store<OsFs>` (a default type parameter is not used for inference, so a generic `open` would need annotations); the generic versions are crate-private. The traits return errors; poisoning on a failed fsync is `Store`'s.

## Index

`src/index.rs`: `HashMap<u128, Entry>` where `Entry { offset: u64, value_len: u32, key_len: u16 }` (16 bytes, `key_len` filling what was padding). Keys are not stored: each is replaced by its 128-bit SipHash-1-3 under a key drawn at random for every `open`, from std's `RandomState`, which is seeded from the OS. The map hashes that `u128` by taking its low 64 bits.

- No allocation per key, and 32 bytes per slot whatever the key's length. At 10M keys of 16 bytes, building the index alone went from 78 to 58 ns per key and from 681 to 528 MB, and `open` from the hint from 718 to 557 MB at peak. The saving grows with key length.
- The keyed hash also restores HashDoS resistance, which foldhash (the hasher before) did not claim: without the key, no one can choose keys that land in the same slot. That was a prerequisite for keys arriving over the network.
- The cost is a missing-key `get`, which now hashes 128 bits: 4.8 ns before, 10.9 ns after. A hit is dominated by the read and checksum, 1.26 µs.
- Two keys with the same 128-bit hash would share one entry: a `set` of the second would silently replace the first. The chance is about n²/2¹²⁹, 2 × 10⁻²⁴ at 40M live keys, and no one can aim for it without the random key. `get` checks the key stored in the record, so a collision shows up as `KeyMismatch`, never as another key's value; a test with a deliberately weak hash (a key's length) checks that.
- `key_len` lets `open` count dead bytes without the keys.
- The map is 64 maps (shards), chosen by the top 6 bits of the hash. Each shard's map places entries by the low bits, so the shard must come from bits it doesn't use: taking the low bits would give every entry in a shard the same low 6 bits and leave 63 of every 64 home slots empty. No test can see that, as any consistent choice is correct; it would only be slow.
- Shards let `open` apply changes from several threads at once (`Index::apply_in_order`): threads hash the keys of a window of changes, then each applies, in order, the changes that fall in the shards it owns. Every change to a key falls in one shard, so a key's changes still apply in the order they were read.
- Growing the map now moves one shard at a time, so the old and new tables that coexist during a resize are 1/64 of the index. A full scan of 10M records without a hint peaked at 841 to 931 MB, against 0.98 to 1.11 GB with one map, in the same time. A missing-key `get` went from 12.1 to 13.1 ns.
- `open` pre-sizes from the largest hint's count, a 64th per shard (see Hint file). Past that, or without a hint, it grows the map as it reads.
- Deletes remove the key from the map. The Delete record stays on disk until compaction, and counts as dead bytes.

### Verify

`verify()` is the full check `open` no longer does once a hint covers most of the log: it reads every record from 0 to the store's end, checking both checksums, and compares the index that scan describes with the one in memory, as lists of entries sorted by offset. It fails with the first bad record's error, or `IndexMismatch` at the first offset where they disagree, including a log whose size is not the store's end. It reads the whole log and holds 32 bytes per live key while comparing; it is meant to be run on demand or on a schedule, not on every open.

### Reads

`get` always verifies:

1. Look up the key. Missing: return `None` with no I/O.
2. Read the record at `offset` in one positioned read of `19 + key_len + value_len` bytes, all from the index entry. If the header there gives other lengths, or the read runs past the end, read it again the old way (header, then the payload length the header gives), so errors report what is actually on disk. One read instead of two took a 1 KiB hit from 1.30 µs to 0.82 µs.
3. Verify both checksums and that the stored key equals the requested key.
4. Return the value. Any failure returns a corruption error, never `None`.

`get` takes no lock while it reads:

- Each of the index's 64 shards is behind its own `RwLock`. `get` copies the 16-byte entry out and lets go before any I/O, so a write waits on a shard only for a lookup, never for a read of the disk.
- The segment list is an `Arc` of a list of `Arc`ed segments, behind a lock that is held only to clone the outer `Arc`. A new segment or a compaction replaces the list; a read keeps the list it took. A segment removed while a read holds it stays open until the read lets go, which is safe on Unix: an unlinked file stays readable.
- If no segment in the list holds the entry's offset, a compaction moved the key and removed its segment between the lookup and taking the list, since entries only move after their copy is durable and segments only go after that. `get` looks the key up again. If the entry is unchanged, it points outside the log, which is a bug, and `get` panics.

Every read is a `pread`. Mapping sealed segments took a 1 KiB hit in one from 1.03 to 0.50 µs, but an I/O error while faulting in a mapped page kills the process with `SIGBUS` instead of failing the read, and again on every restart that reads that key. A bad sector should cost one `get`, not the process, and the 0.5 µs is lost behind any network round trip. Without the map, tally has no `unsafe` code (`#![forbid(unsafe_code)]`).

## Errors

One error enum, marked `#[non_exhaustive]` so variants can be added without breaking callers. Grouped by what the caller can do:

| Category      | Examples                                           | Caller action                             |
| ------------- | -------------------------------------------------- | ----------------------------------------- |
| I/O           | disk full, permission denied                       | environment problem; may retry or give up |
| Corruption    | bad checksum, invalid record, cut-short or overlapping segment, with the log offset | run `repair`  |
| Unsupported   | unknown record type, lengths over current limits, a store of another version | use the version that wrote it |
| Invalid input | empty key, over the limit                          | fix the input                             |
| Locked        | store already open in another `Store` or process   | close the other one, or wait              |

| Poisoned      | an earlier fsync, directory fsync or truncate failed | reopen the store                       |

Bugs (broken internal invariants) panic. Errors are for failures that can happen when the code is correct. A failed `fsync`, directory fsync or truncate after a failed write returns its error and also poisons the store (see Durability contract). A thread that panics while holding the writer lock, a leader or a compaction, poisons the store too, since it may have left the log half changed. A leader that panics before its group's results go out fails every write in the group with `Poisoned`, so no thread waits forever.

## Testing

- **Kill test** (`tests/kill.rs`): the test binary runs itself as a child that applies a seeded sequence of set, delete, reopen, compact and batch, with automatic compaction at 50% dead and no size floor and 16 KiB segments, printing an ack after each `Ok`. Every other run, the child runs four such sequences on four threads sharing one store, each on keys of its own, with compactions in place of reopens, so writes land in groups and compactions run between them; each sequence's acks are checked on their own. The parent `kill -9`s it after a random delay, three times on the same directory without opening it in between, so kills also land in the next child's `open` and recovery. Then the parent opens the store and checks that `open` succeeds, that the state is that of every acknowledged operation with each round's in-flight operation applied or not, and that a write after recovery survives a reopen. `TALLY_KILL_RUNS` (default 20) and `TALLY_KILL_SEED`; CI runs 1,000 on Linux. This cannot detect a missing `fsync`, because the OS page cache survives process death.
- **Power-cut simulation** (`src/power_cut.rs`, on the in-memory disk in `src/mem_fs.rs`): runs a workload and cuts the power before every step it takes, a step being any write, truncate, file sync, directory create, rename or directory sync. The workload includes explicit compactions, batches, and a group of sets, deletes and a batch written as one, as writes from several threads are (a cut must keep all of the group or none), and runs with automatic compaction at 50% dead, no size floor and 64-byte segments, so most writes start a segment and most compactions clean some segments and keep others; the power is also cut after its last step. For each cut it tries every way the disk could be left: any prefix of the unsynced changes landing, the next write landing torn, as zeros, or with its second half and not its first, one file landing all its changes while the others land none or their next write as zeros (nothing orders writes across files), and any of the changes to directory entries since the directory's last sync: each create, remove or rename lands whole or not at all, in any combination that keeps the changes to each name in order, and everything under a lost directory is lost. A directory sync makes the directory's entries exactly what they are now. After each, `open` must succeed with every acknowledged write, and recovery is itself cut at every step and checked the same way. Separate tests cut `repair` at every step, of a corrupt record and of a damaged segment header (no byte is ever missing from both the log and the side file), and check that what `open` served from a dead process's unsynced write survives a cut. Deleting any one fsync or directory sync from `open`, `append`, `repair` or `compact` fails these tests, with one exception, from the hint file. The hint's own fsync: a torn hint fails its checksum and `open` reads the whole log, so correctness never rests on it, and it is kept so that no 32-bit checksum ever has to vouch for one. A single directory fsync after all the removals, instead of one after each, is caught (a deleted key comes back), as is removing the cleaned segments newest first. Letting unsynced directory changes land in any combination, not only all or none at the next directory sync, also found a state that a crash on a real disk can leave and that had to be handled: a side file whose name landed with its bytes still zeros, which `repair` refused to overwrite. The main test checks 584,293 disk states, each step's on a thread of its own: 21 s on an M1 Pro, against 150 s on one thread.
- **A full disk, and failed fsyncs and truncates:** the in-memory disk can be given a free-space limit (`MemFs::limit_free_space`). A write that doesn't fit lands what fits and fails with `StorageFull`, as a full disk does, and truncating frees what it cut. The tests of hints that `open` couldn't finish use it, as do the tests that a failed write is truncated away and leaves the store writable. `MemFs::fail_truncates` and `fail_dir_syncs` make every truncate or directory sync fail. `fail_syncs` makes every file sync fail as Linux does: what it would have made durable never reaches the disk, even after a later sync, while reads still see it. Tests check that a failed fsync in a write, a failed directory fsync when starting a segment, and a failed truncate after a failed write poison the store, that a failed fsync of a compaction's copy leaves it writable, that reads go on, and that after a reopen and a power cut a failed write is gone and later ones are kept.
- **Threads:** a test holds the writer lock while 8 threads queue a `set` each, then checks the 8 share 2 fsyncs; another checks a group's writes apply in order with one. Readers check every value while 4 threads write and 20 compactions run, which fails if a compaction publishes the copies' length after pointing the index at them. A test drives the read that finds its segment removed, deterministically. Three check the leader's wait: none after a group of one, a wait that ends when the queue refills (three threads, one fsync), and one that ends at 10% of the last fsync. Others check that a thread panicking under the writer lock poisons the store, that a failed compaction leaves no hint behind, and that a group that writes nothing does not compact.
- **Corruption tests:** truncated tails, zero-filled tails, a record whose end landed without its start, flipped bits in the header, payload, and lengths, a valid record at another offset, mid-file corruption, damaged and torn segment headers, and directories of other versions.
- **Model test** (`tests/model.rs`): random sequences of set, get, delete, batch and reopen against a `HashMap`, with 100-byte segments, checking every read after every step. Half the cases run with automatic compaction at 50% dead and no floor, so it cleans some segments and keeps others, and check the log is at least as long as the live records; the rest check the log is exactly as long as the writes. Real fsyncs, so the default case count is small; `PROPTEST_CASES=2000 cargo test --test model` for a long run. A second test runs 4 threads on one store, each with keys and a `HashMap` of its own, doing sets, gets, deletes, batches and compactions; each checks its own reads, then all are checked after a reopen, with `verify`.
- **Codec properties** (`record.rs`): round trip, any one-byte change is rejected by the checksum covering it, `decode_header` accepts exactly the valid headers, and a valid header with a matching payload always decodes.
- **Fuzzing** (`fuzz/`, nightly and `cargo-fuzz`): `cargo +nightly fuzz run <target>` with targets `decode` (arbitrary bytes, optionally with checksums fixed up so the fuzzer gets past them), `open` (logs built from arbitrary records and batches, then truncated, given a changed byte, or zeroed over any range, with the torn-tail rule as the oracle), `ops` (the model test, coverage-guided, on the in-memory disk, where it runs about 4 times faster than on files; the input chooses whether automatic compaction runs), and `hint` (a compacted store with more writes after it and a hint changed, truncated or replaced with arbitrary bytes: `open` must still read back every key). Fuzz builds skip fsync: durability cannot be observed there and fsync would cut throughput a hundredfold.
- **Benchmarks** (`cargo bench`): `write` with batches of 10, 100 and 1,000; `concurrent set` from 1, 4, 16 and 64 threads; `set` next to a bare append + `sync_all` baseline and with overwrites of 1,000 keys under the default automatic compaction (which fails if the log outgrows twice its live size, so compaction must have run), `get` hit in the active and in a sealed segment, and miss, `open` on an empty log and at 10k and 100k records, and `compact` at 10k and 100k live records. The empty-log `open` fsyncs files with nothing dirty, so it shows the fixed cost of `open`, not the cost of a real fsync; `set` shows that.

### Done criteria

- 1,000 random-kill runs lose zero acknowledged writes.
- Power-cut simulation loses zero acknowledged writes.
- A torn tail is saved and truncated on open. Mid-file corruption fails `open` and `repair` recovers it.
- A benchmark reports writes per second.

## Dependencies

Core logic (format, recovery, index, crash tests) is hand-written. Commodity and security-sensitive pieces come from crates.

| Crate                      | Why                               |
| -------------------------- | --------------------------------- |
| `crc32c`                   | CRC32C with hardware acceleration |
| `crc-fast`                 | CRC-64/NVME for payloads and hints, with hardware acceleration; default features off, which drops its C API (26 exported `extern "C"` symbols and the unsafe code behind them) |
| `siphasher`                | 128-bit SipHash-1-3 for the index, 64-bit for the segment filters |
| `xorf`                     | binary fuse filters of each sealed segment's keys; default features off, which drops `rand` |
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

- Background compaction: a thread of the store's own that cleans segments while writes go on, so no write waits for one. New writes would start past offsets reserved for the copies, and the index repoint above already tolerates writes landing during a compaction.
- io_uring on Linux, behind `Fs`, if a profile of the TCP server under load shows system calls costing real time. It cannot make an fsync faster, which is what a write costs; it is Linux only and is turned off on some hosts, so it could never be the only path.
- Benchmark-driven `get_unverified` if checksum cost shows up in profiles.
- A parallel full scan, for logs without a hint. Records must apply in log order, so threads would have to resolve each key to its last record by offset, with deletes in the mix; compaction and `open` write hints, so this only matters for the newest segment and the first `open` of a segment.
- A hint for the newest segment, extended on each `open`: it only grows, and `open` fsyncs it before reading, so a prefix stays valid. On the 10M-key store it is most of what `open` still reads, up to 256 MiB.
- A cache of open segment files, for logs of more than a few hundred segments (see Segments).
