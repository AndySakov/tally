# Redundancy for sealed segments

Proposal. Nothing here is built.

## Problem

Every record carries two checksums, so tally detects damage anywhere in the log. It cannot undo it. `repair` moves the bytes from the first bad record to the end of its segment into `tally.log.corrupt-<offset>` and truncates there: one flipped bit in a sealed 256 MiB segment can cost every record after it in that segment. Only a second copy of the information can give those bytes back.

Sealed segments make that copy cheap to keep correct. A segment is sealed when a newer one exists, every byte of it was fsynced before that newer one was written, and it never changes again until compaction removes it (DESIGN.md, Segments). Redundancy computed once at seal stays valid for the segment's life, and goes when it goes.

## Failure model

| Failure | Cover? | Why |
| --- | --- | --- |
| A flipped bit, in a sealed segment, after it was written | Yes | Checksums find it; one damaged 4 KiB block is the cheapest thing to restore. |
| A bad sector, or a few lost 4 KiB blocks, scattered | Yes | The common case: a drive reports a latent sector error as unreadable, and the `pread` fails with an I/O error. Today that error reaches `repair`'s scan, which returns it: `repair` cannot fix an unreadable sector at all. A study of 1.53 million disks over 32 months is the basis for the usual figures ([Bairavasundaram et al., SIGMETRICS '07](https://research.cs.wisc.edu/adsl/Publications/latent-sigmetrics07.html)); 3.45% of drives developed one, as cited secondhand by Schroeder, Damouras and Gill, not checked against the paper. |
| A torn or zeroed range of a few MiB, or a sealed segment cut short | Yes, up to a budget | Latent sector errors cluster on the disk (same paper, per its summary; not checked against the full text), so damage often arrives as a run of blocks, not one. |
| A lost write: the drive acknowledged a block of a sealed segment it never wrote | Usually | Parity is computed from the page cache, which holds what tally wrote, so it encodes the right bytes even when the disk does not. Only if the pages were not evicted before the seal; if they were, the read comes from the disk and the lost write is encoded with the rest. |
| Damage to the active segment | No | It changes on every write. Checksums detect it and `repair` moves it aside, as today. This window is up to `segment_bytes` (256 MiB) of the newest writes. |
| Damage before parity is computed (a bit flipped in memory, a bug that writes a wrong record) | No | Parity encodes whatever bytes it reads. The record checksums still keep a wrong value from being returned. |
| A whole lost segment file | No | Parity stored per segment cannot rebuild a file that is gone. Parity across segments could, but compaction removes segments one at a time, and every removal would recompute the parity of its group. A deleted file is usually an operator or file system event, and a backup answers it. |
| A lost disk, a lost machine | No | Every file of the store is on that disk, redundancy included. Replication or a mirror below tally is the answer. |

## Options

All numbers are for one 256 MiB segment. Reads cost nothing in every option: `get` never looks at the redundancy, only `repair`, `verify` and `open`'s bookkeeping do.

| | Space | Work at seal | Recovers | Complexity |
| --- | --- | --- | --- | --- |
| 1. Full copy, `tally.<base>.copy` | 100% | Write and fsync 256 MiB again (not measured) | Any damage that does not hit the same block in both files; a deleted segment file | Low: compare blocks, take the copy whose records check |
| 2. Reed-Solomon over the whole segment, k = 64 data + m = 4 parity shards | 6.26% | Read 256 MiB, encode (41 to 43 ms measured), write and fsync 16.0 MiB | One contiguous run up to m × S blocks (16.0 MiB), or scattered blocks with the odds below | Medium: a file format, a decode path, one dependency |
| 3. Parity per group of 16 consecutive blocks (XOR, RAID 5 style) | 6.25% | XOR at memory speed; can be computed as appends land | One block per group: two adjacent lost blocks (8 KiB) are beyond it | Low to medium, no dependency |
| 4. File system or storage layer | 0 to 100% | None in tally | Whatever it provides, including a lost disk with a mirror | None in tally; documentation |

**1. Full copy.** The only option that survives a deleted segment file, if the copy survives. It doubles the bytes every seal and every compaction writes, and it shares the segment's disk and directory, so it does not survive the failures in the "No" rows above. Some SSD controllers deduplicate identical blocks, which would make the copy one physical block (reported for SandForce controllers; not verified). It spends 16 times the space of option 2 for coverage option 2 mostly has.

**2. Reed-Solomon over the segment.** The segment file is cut into k contiguous shards of S blocks of 4 KiB. A 256 MiB segment plus its 17-byte file header is 65,537 blocks, so with k = 64, S = 1,025 and there are 1,025 rows. Row r is block r of every shard: k blocks spread S blocks apart, plus their m parity blocks, and each row is decoded on its own, so each row can lose any m of its k + m blocks. One contiguous run of up to m × S = 4,100 blocks (16.0 MiB) touches each row at most m times, wherever it starts.

The budget is per row, not per segment, and two caveats follow:

- The 16.0 MiB is for one run. Shorter runs whose positions line up modulo S add up in the same rows: a run of 2S + 1 blocks (8.0 MiB) hits the row it starts in 3 times, and a run of S + 1 blocks (4.0 MiB) starting in that row too hits it twice more, so 12.0 MiB in two runs loses a row. Five single blocks S apart, 20 KiB, do the same.
- Damage to the parity file counts against the same rows. Parity is laid out in chunks of 64 rows (see File format), and each chunk is the 1 MiB holding all m parity blocks of 64 rows: 1 MiB lost in the parity file can leave 64 rows no protection at all.

Scattered damage, n bad 4 KiB blocks at random in a 256 MiB segment, with a union bound over its rows:

| n bad blocks | k = 64, m = 2 (3.1%) | k = 128, m = 4 (3.1%) | k = 64, m = 4 (6.3%) |
| --- | --- | --- | --- |
| 16 (64 KiB) | 5.1 × 10⁻⁴ | 5.8 × 10⁻⁸ | 3.4 × 10⁻⁹ |
| 64 (256 KiB) | 3.6 × 10⁻² | 9.4 × 10⁻⁵ | 5.7 × 10⁻⁶ |
| 256 (1 MiB) | ~1 | 8.0 × 10⁻² | 5.7 × 10⁻³ |

The chance that some row loses more than m, so some damage stays. Restoring fewer rows still shrinks what `repair` moves aside.

**3. Parity per block group.** Groups are fixed and local, so parity can be computed per group as appends fill them, which would also cover the active segment's completed groups. But the damage it fixes is the damage that does not cluster, and the evidence above says latent errors do cluster. A group code that survives two losses per group (RS(16 + 2)) costs 12.5% and still loses to three adjacent blocks in one group.

**4. The storage layer.** ZFS checksums every block and repairs from a mirror, raidz, or `copies=2` on one disk; btrfs does the same with `dup` or `raid1` profiles. A RAID 1 or 5 without checksums rebuilds a sector the drive reports unreadable, but cannot tell which copy is right when a drive returns wrong bytes; tally's checksums would then report what the array served. Any of these also covers a lost disk, which nothing inside tally can. It costs tally nothing to say so, and it is worth saying whatever else is chosen.

## Recommendation

Build option 2 with k = 64, m = 4 and document option 4 next to it: tally on ZFS or a checksummed mirror can turn parity off. The Decisions section below records what was settled in review.

### Layout

- Blocks are 4 KiB of the segment file, from byte 0, so the 17-byte file header is covered too. A segment's file has N = ⌈`file_len` / 4096⌉ blocks; the last is padded with zeros that are never stored.
- k = min(64, N), m = min(4, k), so parity never outgrows its segment: a segment of one block gets one parity block.
- S = ⌈N / k⌉ blocks per shard. Shard i is blocks i·S to (i + 1)·S − 1; blocks past N are zeros.
- Row r is block r of each shard. Encoding a batch of 64 rows at a time gives the same parity as encoding each row alone or each shard whole: checked on 256 MiB of pseudorandom data with `reed-solomon-simd` 3.1.0, k = 64, m = 4. So encoding reads 64 positioned 256 KiB pieces per batch, and decoding works one row at a time.
- k, m, S, the block size and the rows per chunk all follow from `file_len` and the format version, so none of them is stored: a parity file cannot say anything about its shape that disagrees with its segment. A later version that changes them changes the version.
- The code is `reed-solomon-simd` (GF(2¹⁶), up to 32,768 original and 32,768 recovery shards). Its docs say shards whose size is a multiple of 64 bytes stay compatible across all its versions; 4096 is one. Per its docs, its only `unsafe` is in its SIMD engines; tally's `#![forbid(unsafe_code)]` covers only tally's own code. `reed-solomon-erasure` was the other candidate: its README looks for new maintainers and says versions from 4.0.0 on are not benchmarked thoroughly.

### File format

`tally.<base, 20 digits>.parity`, little-endian:

```text
header  | parity chunks | table | trailer
header: magic "TLYP" | version u8 | base u64 | file_len u64 | file_crc u64
chunk:  for each batch of 64 rows, the m parity shards' blocks of those rows, shard by shard
table:  CRC32C of each of the N data blocks, then of each of the m × S parity blocks
trailer: crc64 u64, CRC-64/NVME of the header and the table
```

- Version 1. `file_len` is the segment file's length when parity was computed, header included. `file_crc` is CRC-64/NVME of all its bytes: the check that a restore gives back exactly the segment, not a near miss.
- The table locates damage. A record checksum says a record is bad, not which of the 4,096 blocks of a 16 MiB value is. CRC32C is enough here because it only picks the erasures: a block whose CRC32C passes wrongly (1 in 2³²) is taken as intact, the restore then fails `file_crc` or the record checksums, and nothing wrong is returned.
- Chunks are batch by batch, not shard by shard, so the encoder streams: 16 MiB of input and 1 MiB of parity per batch, against holding all 16.0 MiB of parity (81 MiB for the 1.3 GB segment a full `compact()` writes) or writing at offsets, which `LogFile` does not allow: it writes only at the end. The cost is the second caveat under Options: a contiguous loss in the parity file takes several parity blocks from the same rows. It only matters if those rows are damaged in the segment too. Laid out shard by shard, 1 MiB lost would take one parity block from each of 256 rows instead.
- The table is 4 bytes per block of data and of parity: 65,537 + 4,100 entries, 272 KiB for 256 MiB, 0.1%.

A parity file is **usable** when its magic and version hold, the trailer's checksum holds over the header and table, `base` is its segment's, `file_len` equals the segment file's length, and the file's length is exactly what `file_len` gives for the header, chunks, table and trailer. A parity block is usable when its CRC32C matches the table. A damaged parity block is just one more erasure in its row.

### When it is written

One `seal(segment)` function, called everywhere a segment stops being the active one: in `roll()`, when a write starts a new segment, and in compaction, when its copy becomes the last segment and the one that was active is sealed without a roll. The seal-hints branch adds it, with the hint's trailer and rename; parity's encode goes in the same function, under the writer lock. The group that sealed, and those queued behind it, wait about 0.1 s (see Costs). Compaction has no parity code of its own.

`open` backfills only segments it already reads in full: a sealed segment with no hint, or read past its hint, gets its parity after that full, checked read, from a second read of the bytes just scanned: the scan reads in order and the encoder in strides, so they do not share one. A segment with a bad record gets no parity, a warning is logged, and `open` goes on as it does today. Sealed segments that `open` reads from their hints are left alone, so the first `open` after an upgrade does not read and encode the whole store. They, and a segment whose seal crashed after its hint and before its parity, wait for an explicit pass, `Store::protect()`, which reads each sealed segment without usable parity in full, checks every record, and writes its parity. `protect` is idempotent and holds the writer lock. The parity of the newest segment, which can grow, is removed by `open`.

With parity off (see Decisions), neither the seal hook, `open` nor `protect` writes any.

Order of writes: create the file, append the header, chunks and table, fsync, then append the trailer, with no second fsync and no directory fsync. The trailer waits for the fsync for the same reason a compaction's hint trailer waits for its copy's: nothing orders writes within a file, and a trailer that landed before blocks it vouches for would make torn parity usable. Every state a crash leaves is no file, a file without a valid trailer, or a usable one whose body was durable before its trailer was written. `open` removes the first two kinds; the parity is written again by an `open` that reads the segment in full, or by `protect`.

Parity is best effort, like the hints `open` writes. A failure to create, write or fsync it is logged, the file removed, and the write or `open` that triggered it succeeds and is not poisoned: no acknowledged data depends on parity.

At seal the segment is read back through the page cache, which holds what this process wrote. If the pages were evicted, the read comes from the disk and a lost write there is encoded with the rest. Encoding from the append buffers instead would avoid that but needs S, which depends on the final length, before the segment is full.

### Missing or damaged parity

- Missing: the segment is protected by checksums alone, as today, until an `open` that reads it in full, or `protect`, writes it.
- Not usable at `open` (bad trailer, other version, wrong base, wrong length): logged, removed and the directory fsynced. It is written again in the same `open` only if `open` reads that segment in full anyway; otherwise by `protect`.
- `file_len` greater than the segment's length: the segment lost its tail, and that is `CutShort` corruption at the segment's base, not a reason to remove the parity. Today a sealed segment cut at a record boundary opens without an error: the hint that is longer than the segment is dropped and the records past the cut are silently gone (see Follow-ups). `file_len` smaller than the segment: nothing tally does appends to a sealed segment, so the parity is removed, with a warning, and written again as above.
- Some parity blocks damaged: the file stays. `repair` treats them as erasures. `verify` reports them and `repair` writes the parity again when the segment's data is intact.

### `open`

`open` reads each sealed segment's parity header, table and trailer, one positioned read each, 0.1% of the log: about 2.7 MB for the 2.6 GB store in README.md (not measured). It removes parity whose segment is gone, as it removes such hints, and `open_segments` removes any `tally.<base>.restore` a crashed `repair` left, as it removes `.compact` files. It never reads parity blocks and never restores: restoring writes segment files, which is `repair`'s job.

### `repair`

A new first step for each damaged sealed segment, before anything moves aside:

1. Damage is found as today: a `SegmentHeader` or `CutShort` segment from `open_segments`, or the first bad record of the scan. An I/O error reading a sealed segment with parity is damage too, not an error to return: that is how a bad sector shows.
2. If the segment has usable parity: read it block by block, compute the CRC32C of each block and of each parity block, and compare with the table. A block that fails to read, or lies past the segment's end, is erased.
3. Decode each row with at least one erasure and at most m. Each restored block must match its CRC32C in the table, or its row counts as lost.
4. If any block was restored: write `tally.<base>.restore`, the segment file with the restored blocks, of length `file_len`, and fsync it. A new file, so an unreadable sector is never written in place; the drive gets the old blocks back when the old file goes.
5. Check the `.restore` before it replaces anything: scan its records with both checksums, and if every row was restored, check it against `file_crc`. Every restored block must lie in records that now check. If any does not, remove the `.restore` and go on with the original segment as if nothing was restored. Otherwise rename it over `tally.<base>.log`, fsync the directory, and log `restored 3 blocks of the segment at <base> from its parity`. The hint stays: the records are the ones it describes. A restore never puts bytes in place that have not been checked.
6. Damage left in rows parity could not restore goes to the side file as today, from its first bad record, with less behind it than before. `move_aside` removes the parity next to the hint, in the directory fsync that makes the side file durable, before the truncate, so stale parity never describes a segment it no longer matches. That covers `repair_file_header`, which goes through `move_aside` too.
7. A segment whose records all check but whose parity has damaged blocks or does not match: write the parity again from the segment.

A restore loses nothing and leaves nothing for the operator to decide, so `repair` restores every damaged segment it can in one call, and then moves aside at most the first damage it could not restore, as today. It returns both in one `Repaired` (see Decisions).

Idempotent: a crash before the rename leaves the damaged segment and a partial or unchecked `.restore`, which `open_segments` removes, and the rerun restores again. After the rename the segment is intact and the rerun finds nothing there.

### `verify`

For each sealed segment with parity, `verify` reads on past an unreadable block, reporting it as corruption at that block instead of returning the I/O error, and also checks the trailer, every data block's CRC32C against the table and every parity block's. Its read is the segment it already reads plus 6.26%, and the CRC32C table costs 17 ms per 256 MiB (measured). Damage to data fails as today, at the first bad record. Damage to parity alone fails with a new reason, `Corruption { offset: base, reason: Parity }`: the data is intact but its protection is not, and a scrub exists to find exactly that. `repair` fixes both. A sealed segment without parity is not an error: `verify` returns `Ok`, and `protect` writes it.

Parity only helps if damage is found while it is within budget, so `verify` on a schedule is what makes it worth having. More than 60% of the latent sector errors in the same study were found by scrubbing (per the paper's summary; not checked against the full text).

### `get`

Unchanged. A bad record is a `Corruption` error, as today, and the error's documented action is still `repair`, which now restores where it can. `get` does not decode: that would put a second read path, reading up to k + m blocks per damaged row, behind every read, to save the caller one `repair`.

### Compaction

- Cleaned segments lose their parity with their hints: removed before the segment, in step 6's removals and directory fsyncs. Parity whose segment is gone is removed by `open`.
- Compaction still stops at a corrupt record with its offset. It never restores.
- The output gets parity when it is sealed, through `seal()` like any other segment. A write that lands during a background compaction starts a segment past the copy, which seals the copy. When no write overlaps the compaction, the copy becomes the active segment and stays without parity until it fills or a later compaction cleans it: after a `compact()` on an idle store, its live records are not covered until then. Sealing every copy at once was measured and dropped: it saved no copying once compaction ran on its own thread, and before that it left hundreds of small sealed segments.

### Hints

Independent files. A restore keeps the hint, since the records are unchanged byte for byte. A truncating repair removes both, in the same directory fsync. Neither is read to build or check the other.

### Power-cut simulation and crash states

- Parity adds steps to every roll: a create, three appends, an fsync and a trailer append, each a step to cut before, and each step's write is tried landed, torn, zeroed or out of order as every write is now. The main workload's 64-byte segments roll on most writes. The state count (1,210,896 now, 55 s) grows by an amount not yet measured: the build measures it with parity on before deciding whether parity runs in the main workload or in one of its own.
- New check after every recovered state: any parity file `open` keeps is usable and equals a fresh encode of its segment. No torn or stale parity may ever be trusted.
- New test: cut `repair` at every step while it restores a zeroed block of a sealed segment. Every state must give back the original segment after `open` and a rerun of `repair`, with no byte missing from both the log and a side file.
- New tests, each failing on the base branch: `repair` of a sealed segment with one zeroed 4 KiB block restores it byte for byte and writes no side file; with one run of 4,100 blocks zeroed it restores all of it; with m + 1 blocks of one row zeroed it restores the rest and moves aside from the first record it could not; a sealed segment cut at a record boundary with parity fails `open` with `CutShort` and `repair` restores its tail.
- Kill test and model test: end every run with `verify`, which now checks parity.
- Fuzz target `parity`: a store with a sealed segment, then arbitrary damage to the segment and to its parity. `repair` must never change a byte of the segment to anything but its original value, and every record that checked before must check after.

### Costs, measured

`reed-solomon-simd` documents 8.68 GiB/s encoding 64 original and 64 recovery shards of 1 KiB, counting both, on one core of a Ryzen 5 3600 with AVX2, and about the same on an Apple M1 ([docs.rs](https://docs.rs/reed-solomon-simd/3.1.0/reed_solomon_simd/)). Measured here with a throwaway benchmark on an M1 Pro, one thread, best of 5 (median in brackets), 256 MiB of pseudorandom data (65,536 blocks and S = 1,024, one row fewer than a real segment's 65,537 blocks, 0.1%), while the machine's 1-minute load average was 49 on 10 cores: treat every time as an upper bound.

| Step at seal, 256 MiB segment | Time |
| --- | --- |
| `pread` of the segment from the page cache, 1 MiB at a time | 24 ms (27) |
| Encode, k = 64, m = 4, 64 rows a batch | 41 ms (43), 6.2 GiB/s of segment |
| Encode, k = 64, m = 2 / k = 64, m = 8 / k = 128, m = 4 | 31 / 52 / 42 ms |
| CRC32C of each 4 KiB block (the table) | 17 ms (18) |
| CRC-64/NVME of the whole file (`file_crc`) | 6.5 ms (7.0) |
| Write and `sync_all` (`F_FULLFSYNC`) of 16 MiB of parity | 17 ms (22) |
| Total, done in sequence | about 105 to 120 ms |

That is once per 256 MiB written. At the fastest write rate in README.md, batches of 1,000 sets of 1 KiB at 93,993 sets/s, 256 MiB fills in about 2.7 s, so parity adds about 4% to that rate and one stall of about 0.1 s per seal, against the 0.88 s the slowest automatic compaction stalls a batch. At one fsync per write it is lost in the noise. The disk writes 6.26% more for every byte that is sealed or compacted. A 1.3 GB compaction output costs about five times the above.

Restoring, decoding one row of 64 + 4 blocks of 4 KiB with four erased took 0.42 ms (0.44). The worst case inside the budget, a 16.0 MiB run touching all 1,025 rows, is about 0.45 s of decoding plus a read of the segment and its parity and a write of the segment.

The benchmark is `/private/tmp/claude-501/-Users-andysakov-Code-rust-tally/ee0184ec-470b-4f64-928d-91ceb6119f03/scratchpad/parity-bench`, outside the repo.

Not measured: a full copy's write, `open`'s read of the parity tables, the state count of the power-cut test, and anything on Linux, under a cold page cache or for a 1.3 GB compaction output. The build measures the last two before parity goes on the write path (see Decisions).

## Decisions

1. **A compaction's output gets parity when `seal()` seals it**, not at once. A copy no write overlapped stays active, without parity, until it is sealed.
2. **k = 64, m = 4**, as constants in code, with S and the rest derived from `file_len` and the version, never stored.
3. **An off switch**, `Options::parity: bool`, default on, for stores on ZFS or a checksummed mirror. Off writes no new parity: not at seal, not in `open`'s backfill, not in `protect`. Existing parity is still used by `repair` and removed with its segments.
4. **`repair` returns a plain `Repaired`**, not an `Option`: `Repaired { restored: Vec<Restored>, moved: Option<Moved> }`, where no restores and no move means nothing was needed. One way to say nothing, not two.
5. **Parity is encoded on the write path in v1**, in `seal()` under the writer lock. The build PR first measures it under a cold page cache and for a 1.3 GB compaction output.
6. **`verify` fails on damaged parity**, with `Corruption { offset: base, reason: Parity }`. Missing parity stays `Ok`.
7. **Power cut:** the build measures the state count with parity on before deciding whether parity goes in the main workload or one of its own.
8. **Build order:** the two bug fixes under Follow-ups first, each in its own PR (both merged), then seal-hints with its `seal()` function, then parity.

## Follow-ups, outside this change (built first, see Decisions)

- A sealed segment shorter than its hint drops the hint and opens without an error (`read_hint` in `src/store/open.rs`, "the segment is shorter than the hint says"). If the cut lands on a record boundary, the records past it are gone and nothing reports it. That rule exists for the newest segment, whose torn tail `open` truncates; for a sealed segment it should be `CutShort`. Found by reading; no test written.
- An unreadable sector in any segment makes `repair` return the I/O error: its scan returns any error but `Corruption`, and `move_aside` would fail to copy the sector anyway. Without parity, the store cannot be repaired past it by tally. Found by reading `src/store/repair.rs`; no test written.

## Sources

- `reed-solomon-simd` 3.1.0 docs, throughput table, shard limits and compatibility: <https://docs.rs/reed-solomon-simd/3.1.0/reed_solomon_simd/>
- `reed-solomon-erasure` README, maintenance status: <https://github.com/rust-rse/reed-solomon-erasure>
- Bairavasundaram et al., An Analysis of Latent Sector Errors in Disk Drives, SIGMETRICS '07: <https://research.cs.wisc.edu/adsl/Publications/latent-sigmetrics07.html>. Only the abstract (1.53 million disks, 32 months) was read; the 3.45%, clustering and scrubbing figures come from secondary summaries.
