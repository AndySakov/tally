use std::collections::{HashMap, HashSet};
use std::fs::TryLockError;
use std::io::{self, BufReader, ErrorKind, Read};
use std::num::NonZero;
use std::path::{Path, PathBuf};
use std::thread;

use crate::fs::{Fs, LogFile, OsFs};
use crate::hint::{self, COMPACT_HINT_FILE_NAME, HINT_FILE_NAME};
use crate::index::{Entry, Index};
use crate::options::{AutoCompact, Options};
use crate::record::{
    HEADER_LEN, Header, IndexOp, Record, decode_header, decode_index_op, decode_payload, encode,
};
use crate::segment::{Segment, hint_name, log_name, parse_hint_name, parse_log_name};
use crate::{Batch, CorruptionReason, DecodeError, Error, Key, Value};

/// The log before it was split into segments, and now the marker that
/// keeps earlier versions of tally out (DESIGN.md, Segments).
pub(crate) const LOG_FILE_NAME: &str = "tally.log";
pub(crate) const LOCK_FILE_NAME: &str = "tally.lock";
/// Where an earlier version's compaction wrote the new log.
const COMPACT_FILE_NAME: &str = "tally.log.compact";
/// Where the marker is written before it is renamed over `tally.log`.
const MARKER_FILE_NAME: &str = "tally.log.marker";

/// What `tally.log` holds once the log is in segments: a record header of
/// type 255 with valid checksums, which every earlier version reads as an
/// unsupported record at offset 0 instead of starting an empty log.
pub(crate) fn marker() -> [u8; HEADER_LEN] {
    let mut bytes = [0u8; HEADER_LEN];
    bytes[4] = 0xff;
    bytes[11..15].copy_from_slice(&crc32c::crc32c(&[]).to_le_bytes());
    let checksum = crc32c::crc32c(&bytes[4..]);
    bytes[..4].copy_from_slice(&checksum.to_le_bytes());
    bytes
}

/// What `Store::repair` moved out of the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Repaired {
    /// Offset of the first bad record, where its segment now ends.
    pub offset: u64,
    /// Bytes moved out of the log: everything from `offset` to the end of
    /// its segment.
    pub dropped: u64,
    /// Where those bytes were saved: `tally.log.corrupt-<offset>` in the
    /// store directory.
    pub side_file: PathBuf,
}

/// An open store: the log's segments plus an in-memory index of every live
/// key.
///
/// `F` is the file system. It is always [`OsFs`] outside tally's own tests,
/// which swap in an in-memory disk to simulate power cuts.
pub struct Store<F: Fs = OsFs> {
    fs: F,
    dir: PathBuf,
    /// Holds the exclusive lock for as long as the store is open.
    _lock: F::File,
    /// In log order, none overlapping. Writes go to the last, which is
    /// never empty of a file: there is always one to write to.
    segments: Vec<Segment<F::File>>,
    segment_bytes: u64,
    /// `None` once an automatic compaction has failed, until the next open.
    auto_compact: Option<AutoCompact>,
    index: Index,
}

impl Store {
    /// Opens the store in `dir`, creating the directory and its first
    /// segment if they don't exist, and rebuilds the index: for each
    /// segment, from its hint and the records written after it if a
    /// compaction left a usable one, otherwise by reading every record. A
    /// hint that can't be used is logged as a warning and removed
    /// (DESIGN.md, Hint file). A log from before segments, a lone
    /// `tally.log`, becomes the first segment (DESIGN.md, Segments).
    /// Takes an exclusive lock on `tally.lock`: a second `open` of the same
    /// directory fails with `Locked` until this `Store` is dropped.
    ///
    /// Creating any file requires an fsync of its parent directory (see
    /// DESIGN.md, New files).
    ///
    /// A torn or zero-filled tail of the newest segment is truncated,
    /// fsynced and logged as a warning before `open` returns. Any other bad
    /// record `open` reads fails it with its offset; `repair` handles those
    /// (DESIGN.md, Recovery). Records a hint covers aren't read, so damage
    /// to them shows up in `get` instead.
    pub fn open(dir: impl AsRef<Path>) -> Result<Store, Error> {
        Self::open_with(dir, Options::default())
    }

    /// `open`, with `options` in place of the defaults.
    ///
    /// With `auto_compact` set, compacts before returning if the log is due
    /// (see [`AutoCompact`]). A failed automatic compaction leaves the log as
    /// it was, logs a warning and turns automatic compaction off until the
    /// next open; `open` still succeeds.
    pub fn open_with(dir: impl AsRef<Path>, options: Options) -> Result<Store, Error> {
        Self::open_in(OsFs, dir.as_ref(), options)
    }

    /// Makes a log that `open` refuses openable again, without opening it.
    ///
    /// Returns `None` if `open` would succeed (a torn tail of the newest
    /// segment is left for `open` to truncate). On a corrupt record, saves
    /// every byte from it to the end of its segment in a side file, fsyncs
    /// the side file and the directory, then truncates the segment at the
    /// record and fsyncs it. Unsupported records are an error, not something
    /// to repair: a newer tally can read them. Fails with `Locked` while a
    /// `Store` is open.
    ///
    /// If the side file already exists and is the start of what would be
    /// moved, a crashed earlier run left it, and it is rewritten. Otherwise
    /// it holds bytes from an earlier repair that the log no longer has, and
    /// `repair` fails with `SideFileExists` without changing anything.
    pub fn repair(dir: impl AsRef<Path>) -> Result<Option<Repaired>, Error> {
        Self::repair_in(&OsFs, dir.as_ref())
    }
}

impl<F: Fs> Store<F> {
    pub(crate) fn open_in(fs: F, dir: &Path, options: Options) -> Result<Self, Error> {
        match fs.create_dir(dir) {
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        };
        let parent = match dir.parent() {
            Some(p) if !p.as_os_str().is_empty() => p,
            _ => Path::new("."),
        };
        let lock_file = fs.open_or_create(&dir.join(LOCK_FILE_NAME))?;
        lock(&lock_file)?;
        let mut segments = open_segments(&fs, dir)?;
        if segments.is_empty() {
            let file = fs.open_or_create(&dir.join(log_name(0)))?;
            segments.push(Segment {
                base: 0,
                file,
                len: 0,
                dead: 0,
            });
        }
        sync(&segments.last().expect("one was just made").file);
        fs.sync_dir(dir)?;
        fs.sync_dir(parent)?;
        let index = load(&fs, dir, &mut segments)?;
        let mut store = Store {
            fs,
            dir: dir.to_path_buf(),
            _lock: lock_file,
            segments,
            segment_bytes: options.segment_bytes,
            auto_compact: options.auto_compact,
            index,
        };
        store.count_dead();
        store.compact_if_due();
        Ok(store)
    }

    pub(crate) fn repair_in(fs: &F, dir: &Path) -> Result<Option<Repaired>, Error> {
        let _lock = fs.open_or_create(&dir.join(LOCK_FILE_NAME))?;
        lock(&_lock)?;
        let segments = open_segments(fs, dir)?;
        let mut index = Index::new();
        let mut bad = None;
        for (i, segment) in segments.iter().enumerate() {
            match scan(segment, &mut index, segment.base, segment.end()) {
                Ok(end) if end < segment.end() && i + 1 < segments.len() => {
                    bad = Some((segment, end));
                    break;
                }
                Ok(_) => {}
                Err(Error::Corruption { offset, .. }) => {
                    bad = Some((segment, offset));
                    break;
                }
                Err(err) => return Err(err),
            }
        }
        let Some((segment, offset)) = bad else {
            return Ok(None);
        };
        let len = segment.end();

        let side_file = dir.join(format!("{LOG_FILE_NAME}.corrupt-{offset}"));
        let side = match fs.open_existing(&side_file) {
            Ok(existing) => {
                if !starts_tail(&existing, segment, offset, len)? {
                    return Err(Error::SideFileExists(side_file));
                }
                existing
            }
            Err(err) if err.kind() == ErrorKind::NotFound => fs.open_or_create(&side_file)?,
            Err(err) => return Err(err.into()),
        };
        side.set_len(0)?;
        copy_to_end(segment.reader_at(offset).take(len - offset), &side)?;
        sync(&side);
        remove_if_present(fs, &dir.join(hint_name(segment.base)))?;
        fs.sync_dir(dir)?;

        segment.set_len(offset)?;
        sync(segment);
        Ok(Some(Repaired {
            offset,
            dropped: len - offset,
            side_file,
        }))
    }

    fn active(&self) -> &Segment<F::File> {
        self.segments.last().expect("there is always a segment")
    }

    /// The log offset the next record lands at.
    fn end(&self) -> u64 {
        self.active().end()
    }

    /// Bytes no live key points at: overwritten values, deleted values,
    /// Delete records and Batch records.
    #[cfg(test)]
    pub(crate) fn dead(&self) -> u64 {
        self.segments.iter().map(|s| s.dead).sum()
    }

    /// The segment holding log offset `offset`.
    fn segment_of(&self, offset: u64) -> usize {
        self.segments.partition_point(|s| s.base <= offset) - 1
    }

    /// Sets each segment's dead bytes from the index: what no entry covers.
    fn count_dead(&mut self) {
        let mut live = vec![0u64; self.segments.len()];
        for entry in self.index.values() {
            live[self.segment_of(entry.offset)] +=
                record_len(usize::from(entry.key_len), entry.value_len);
        }
        for (segment, live) in self.segments.iter_mut().zip(live) {
            segment.dead = segment.len - live;
        }
    }

    /// Rewrites the log with only the latest record of each live key, so
    /// overwritten values and Delete records stop taking space.
    ///
    /// Cleans every segment (DESIGN.md, Compaction): copies the live records
    /// into a new segment with a hint, fsyncs both and the directory, then
    /// removes the old segments oldest first, fsyncing the directory after
    /// each. Until then the old segments are untouched, so any failure while
    /// copying returns the error and leaves the store as it was. Reads each
    /// segment in one pass and checks every record it reads, so a corrupt
    /// record stops compaction with its offset rather than being copied.
    /// Panics if a directory fsync fails, as for any failed fsync.
    pub fn compact(&mut self) -> Result<(), Error> {
        let all: Vec<u64> = self
            .segments
            .iter()
            .filter(|s| s.len > 0)
            .map(|s| s.base)
            .collect();
        self.clean(&all)
    }

    /// Copies the live records of the segments based at `chosen` (in log
    /// order) to a fresh segment, then removes them. A Delete record is
    /// copied too while an older segment that is kept might hold a Set it
    /// hides.
    fn clean(&mut self, chosen: &[u64]) -> Result<(), Error> {
        if chosen.is_empty() {
            return Ok(());
        }
        if self.active().len > 0 {
            self.roll()?;
        }
        let oldest_kept = self
            .segments
            .iter()
            .filter(|s| !chosen.contains(&s.base))
            .map(|s| s.base)
            .min()
            .expect("the new segment is kept");
        let hint_path = self.dir.join(hint_name(self.end()));
        let mut moved = Vec::with_capacity(self.index.len());
        let mut carry = Vec::new();
        let copied = self.copy_live(chosen, oldest_kept, &hint_path, &mut moved, &mut carry);
        let written = match copied {
            Ok(written) => written,
            Err(err) => {
                self.restore_offsets(&moved);
                let active = self.segments.last().expect("there is always a segment");
                active.file.set_len(0).expect("truncate should not fail");
                sync(&active.file);
                if remove_if_present(&self.fs, &hint_path).expect("removing a fresh hint") {
                    self.fs
                        .sync_dir(&self.dir)
                        .expect("directory fsync failed; restart to recover from the log");
                }
                return Err(err);
            }
        };
        self.segments
            .last_mut()
            .expect("there is always a segment")
            .len = written;
        self.fs
            .sync_dir(&self.dir)
            .expect("directory fsync failed after compaction; restart to recover from the log");
        for segment in &mut self.segments {
            if chosen.contains(&segment.base) {
                segment.dead = segment.len;
            }
        }
        if !carry.is_empty() {
            let mut buffer = Vec::new();
            for key in &carry {
                encode(
                    &Record::Delete {
                        key: Key::try_from(&key[..]).expect("read from a record"),
                    },
                    &mut buffer,
                );
            }
            self.append_bytes(&buffer)?;
            let at = self.segments.len() - 1;
            self.segments[at].dead += buffer.len() as u64;
        }
        for &base in chosen {
            let at = self.segment_of(base);
            remove_if_present(&self.fs, &self.dir.join(hint_name(base)))?;
            self.fs.remove(&self.dir.join(log_name(base)))?;
            self.fs
                .sync_dir(&self.dir)
                .expect("directory fsync failed after compaction; restart to recover from the log");
            self.segments.remove(at);
        }
        Ok(())
    }

    /// Writes each live record of the `chosen` segments to the empty active
    /// segment, and its key and value length to its hint at `hint_path`,
    /// and fsyncs both, pointing its index entry at the new offset as it
    /// goes and pushing the old offset to `moved`, so a failure can put
    /// them back. Pushes to `carry` the key of each Delete record that must
    /// outlive its segment: its key isn't live, and a segment older than
    /// it is kept. Returns the bytes written. One buffered pass per segment:
    /// reading the dead records in between costs less than a positioned
    /// read per live one.
    fn copy_live(
        &mut self,
        chosen: &[u64],
        oldest_kept: u64,
        hint_path: &Path,
        moved: &mut Vec<u64>,
        carry: &mut Vec<Vec<u8>>,
    ) -> Result<u64, Error> {
        let hint_file = self.fs.open_or_create(hint_path)?;
        hint_file.set_len(0)?;
        let mut hint = hint::Writer::new(&hint_file);
        let (inputs, output) = self.segments.split_at(self.segments.len() - 1);
        let output = &output[0];
        let mut carried = HashSet::new();
        let mut header_buf = [0u8; HEADER_LEN];
        let mut payload_buf = Vec::new();
        let mut buffer = Vec::new();
        let mut written = 0u64;
        let mut expected = 0usize;
        for input in inputs.iter().filter(|s| chosen.contains(&s.base)) {
            let mut reader = BufReader::with_capacity(COMPACT_BUFFER, input.reader_at(input.base));
            let mut offset = input.base;
            while offset < input.end() {
                reader.read_exact(&mut header_buf)?;
                let header = decode_header(&header_buf).map_err(|err| err.at(offset))?;
                payload_buf.resize(header.payload_len(), 0);
                reader.read_exact(&mut payload_buf)?;
                let op = decode_index_op(header, &payload_buf).map_err(|err| err.at(offset))?;
                match op {
                    IndexOp::Set { key, value_len } => {
                        if let Some(entry) = self.index.get_mut(key.as_bytes())
                            && entry.offset == offset
                        {
                            entry.offset = output.base + written + buffer.len() as u64;
                            moved.push(offset);
                            hint.push(key.as_bytes(), value_len)?;
                            buffer.extend_from_slice(&header_buf);
                            buffer.extend_from_slice(&payload_buf);
                            if buffer.len() >= COMPACT_BUFFER {
                                output.file.write_all_at(&buffer, written)?;
                                written += buffer.len() as u64;
                                buffer.clear();
                            }
                        }
                    }
                    IndexOp::Delete { key } => {
                        if oldest_kept < input.base
                            && !self.index.contains(key.as_bytes())
                            && carried.insert(key.as_bytes().to_vec())
                        {
                            carry.push(key.as_bytes().to_vec());
                        }
                    }
                    IndexOp::Batch { .. } => {}
                }
                offset += header.record_len();
            }
            expected += 1;
        }
        assert_eq!(expected, chosen.len(), "every chosen segment is read");
        let left_behind = self
            .index
            .values()
            .filter(|e| {
                let at = self.segments.partition_point(|s| s.base <= e.offset) - 1;
                chosen.contains(&self.segments[at].base)
            })
            .count();
        assert_eq!(left_behind, 0, "every live key in them is copied");
        output.file.write_all_at(&buffer, written)?;
        written += buffer.len() as u64;
        sync(&output.file);
        hint.finish(written)?;
        sync(&hint_file);
        Ok(written)
    }

    /// Points the entries `copy_live` moved back at their records in the
    /// old segments, rereading each key from there. Each was read and
    /// checked moments before, so a failure here panics: the old segments
    /// are untouched, and a restart rebuilds the index from them.
    fn restore_offsets(&mut self, moved: &[u64]) {
        for &offset in moved {
            let segment = &self.segments[self.segment_of(offset)];
            let (_, record) = load_record(segment, offset)
                .expect("rereading a key after a failed compaction; restart to rebuild the index");
            let Record::Set { key, .. } = record else {
                unreachable!("only Set records are moved");
            };
            self.index
                .get_mut(key.as_bytes())
                .expect("a moved key is still live")
                .offset = offset;
        }
    }

    /// Compacts if automatic compaction is on and due, cleaning the
    /// segments with the most dead bytes until it no longer would be. The
    /// caller's write has already succeeded, so a failure is logged, not
    /// returned.
    fn compact_if_due(&mut self) {
        let Some(auto) = self.auto_compact else {
            return;
        };
        let sizes: Vec<SegmentSize> = self
            .segments
            .iter()
            .map(|s| SegmentSize {
                base: s.base,
                len: s.len,
                dead: s.dead,
            })
            .collect();
        let chosen = deadest(&sizes, auto);
        if let Err(err) = self.clean(&chosen) {
            self.auto_compact = None;
            log::warn!("automatic compaction failed, off until the next open: {err}");
        }
    }

    /// Starts a new, empty segment at the end of the log, and makes its
    /// name durable before anything is written to it.
    fn roll(&mut self) -> Result<(), Error> {
        let base = self.end();
        let file = self.fs.open_or_create(&self.dir.join(log_name(base)))?;
        self.fs.sync_dir(&self.dir)?;
        self.segments.push(Segment {
            base,
            file,
            len: 0,
            dead: 0,
        });
        Ok(())
    }

    fn append(&mut self, record: &Record) -> Result<u64, Error> {
        let mut buffer: Vec<u8> = vec![];
        encode(record, &mut buffer);
        self.append_bytes(&buffer)
    }

    /// Writes `buffer` at the end of the log in one write and fsyncs it,
    /// first starting a new segment if the active one is full, so a write
    /// never spans two segments.
    fn append_bytes(&mut self, buffer: &[u8]) -> Result<u64, Error> {
        let full = self.active().len;
        if full > 0 && full >= self.segment_bytes {
            self.roll()?;
        }
        let active = self.segments.last_mut().expect("there is always a segment");
        let offset = active.end();
        if let Err(err) = active.write_all_at(buffer, offset) {
            active.set_len(offset).expect("truncate should not fail");
            return Err(Error::Io(err));
        }
        sync(&active.file);
        active.len += buffer.len() as u64;
        Ok(offset)
    }

    /// Appends a Set record, fsyncs, then points the index at it. Then
    /// compacts if automatic compaction is due, as `open_with` describes.
    ///
    /// If the write fails partway through, truncates the log back to `end`
    /// and returns the I/O error. Panics if that truncate fails, or if the
    /// fsync fails.
    pub fn set(&mut self, key: Key, value: Value) -> Result<(), Error> {
        let record = Record::Set { key, value };
        let offset = self.append(&record)?;
        self.indexed(&record, offset);
        self.compact_if_due();
        Ok(())
    }

    /// Applies every operation in `batch` with one append and one fsync,
    /// all or nothing: after a crash, `open` finds the whole batch or none
    /// of it (DESIGN.md, Batches). Operations apply in order, so a delete
    /// sees the batch's own earlier sets. A delete of a key that isn't live
    /// at that point writes nothing, as `delete` does, and a batch that
    /// writes nothing is a no-op. Then compacts if due, as `set` does.
    /// Failed writes are handled as in `set`.
    pub fn write(&mut self, batch: Batch) -> Result<(), Error> {
        let mut live: HashMap<&[u8], bool> = HashMap::new();
        let mut body = Vec::new();
        let mut written = Vec::with_capacity(batch.ops.len());
        for op in &batch.ops {
            let (Record::Set { key, .. } | Record::Delete { key }) = op else {
                unreachable!("a Batch holds only sets and deletes");
            };
            let is_set = matches!(op, Record::Set { .. });
            let was_live = live
                .get(key.as_bytes())
                .copied()
                .unwrap_or_else(|| self.index.contains(key.as_bytes()));
            if !is_set && !was_live {
                continue;
            }
            live.insert(key.as_bytes(), is_set);
            written.push((body.len() as u64, op));
            encode(op, &mut body);
        }
        if written.is_empty() {
            return Ok(());
        }
        let mut buffer = Vec::with_capacity(HEADER_LEN + 8 + body.len());
        encode(
            &Record::Batch {
                body_len: body.len() as u64,
            },
            &mut buffer,
        );
        let header_len = buffer.len() as u64;
        buffer.extend_from_slice(&body);
        let start = self.append_bytes(&buffer)?;
        let at = self.segments.len() - 1;
        self.segments[at].dead += header_len;
        for (at, op) in written {
            self.indexed(op, start + header_len + at);
        }
        self.compact_if_due();
        Ok(())
    }

    /// Updates the index and the dead bytes for a Set or Delete record now
    /// durable at `offset`. A Delete's key must be live.
    fn indexed(&mut self, record: &Record, offset: u64) {
        let died = |store: &mut Self, at: u64, len: u64| {
            let segment = store.segment_of(at);
            store.segments[segment].dead += len;
        };
        match record {
            Record::Set { key, value } => {
                let old = self.index.insert(
                    key.as_bytes(),
                    Entry {
                        offset,
                        value_len: u32::try_from(value.as_bytes().len())
                            .expect("Value guarantees len fits u32"),
                        key_len: entry_key_len(key),
                    },
                );
                if let Some(old) = old {
                    died(
                        self,
                        old.offset,
                        record_len(key.as_bytes().len(), old.value_len),
                    );
                }
            }
            Record::Delete { key } => {
                let key_len = key.as_bytes().len();
                let old = self
                    .index
                    .remove(key.as_bytes())
                    .expect("deleted keys are live");
                died(self, old.offset, record_len(key_len, old.value_len));
                died(self, offset, (HEADER_LEN + key_len) as u64);
            }
            Record::Batch { .. } => unreachable!("not an index operation"),
        }
    }

    /// Returns the value for `key`, or `None` if it isn't live.
    ///
    /// Reads the record with two positioned reads (header, then payload) and
    /// verifies both checksums and that the stored key equals `key`. Any failure is a
    /// corruption error at the record's offset, never `None`.
    pub fn get(&self, key: &Key) -> Result<Option<Value>, Error> {
        match self.index.get(key.as_bytes()) {
            Some(entry) => {
                if let Record::Set {
                    key: stored_key,
                    value,
                } = load_indexed(&self.segments[self.segment_of(entry.offset)], entry)?
                {
                    if *key == stored_key {
                        Ok(Some(value))
                    } else {
                        Err(Error::Corruption {
                            offset: entry.offset,
                            reason: CorruptionReason::KeyMismatch,
                        })
                    }
                } else {
                    Err(Error::Corruption {
                        offset: entry.offset,
                        reason: CorruptionReason::NotASet,
                    })
                }
            }
            None => Ok(None),
        }
    }

    /// Reads every record in the log, checking both checksums of each, and
    /// checks that they describe exactly the index in memory. `open` skips
    /// the checksums of records a hint covers (DESIGN.md, Hint file); this
    /// is the full check, on demand. Reads the whole log and holds two
    /// sorted lists of 16 bytes per live key while it compares them.
    ///
    /// Fails with the first bad record's error, or `IndexMismatch` at the
    /// first offset where the log and the index disagree.
    pub fn verify(&self) -> Result<(), Error> {
        let mismatch = |offset| Error::Corruption {
            offset,
            reason: CorruptionReason::IndexMismatch,
        };
        let mut scanned = Index::new();
        for segment in &self.segments {
            let len = segment.size()?;
            if len != segment.end() {
                return Err(mismatch(len.min(segment.end())));
            }
            let end = scan(segment, &mut scanned, segment.base, segment.end())?;
            if end != segment.end() {
                return Err(mismatch(end));
            }
        }
        let mut ours: Vec<Entry> = self.index.values().copied().collect();
        let mut theirs: Vec<Entry> = scanned.values().copied().collect();
        drop(scanned);
        ours.sort_unstable_by_key(|e| e.offset);
        theirs.sort_unstable_by_key(|e| e.offset);
        let first_difference = (0..ours.len().max(theirs.len()))
            .find(|&i| ours.get(i) != theirs.get(i))
            .map(|i| {
                let offsets = [ours.get(i), theirs.get(i)].into_iter().flatten();
                offsets
                    .map(|e| e.offset)
                    .min()
                    .expect("one side has an entry")
            });
        match first_difference {
            Some(offset) => Err(mismatch(offset)),
            None => Ok(()),
        }
    }

    /// Appends a Delete record, fsyncs, then removes the key from the index.
    /// Then compacts if automatic compaction is due, as in `set`.
    ///
    /// Deleting a key that isn't live is a no-op: nothing is written and it
    /// returns `Ok`. Failed writes are handled as in `set`.
    pub fn delete(&mut self, key: Key) -> Result<(), Error> {
        if self.index.contains(key.as_bytes()) {
            let record = Record::Delete { key };
            let offset = self.append(&record)?;
            self.indexed(&record, offset);
            self.compact_if_due();
        };
        Ok(())
    }
}

/// Whether `side` holds a prefix of `file`'s bytes from `offset` to `len`,
/// as a repair that crashed partway through copying them leaves it: each
/// byte is the one it copies, or zero where that write never landed.
fn starts_tail(
    side: &impl LogFile,
    file: &impl LogFile,
    offset: u64,
    len: u64,
) -> io::Result<bool> {
    let side_len = side.size()?;
    if side_len > len - offset {
        return Ok(false);
    }
    let mut ours = side.reader_at(0).take(side_len);
    let mut theirs = file.reader_at(offset).take(side_len);
    let mut a = vec![0u8; 64 * 1024];
    let mut b = vec![0u8; 64 * 1024];
    loop {
        let n = match ours.read(&mut a) {
            Ok(0) => return Ok(true),
            Ok(n) => n,
            Err(err) if err.kind() == ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        };
        theirs.read_exact(&mut b[..n])?;
        if a[..n]
            .iter()
            .zip(&b[..n])
            .any(|(&ours, &theirs)| ours != 0 && ours != theirs)
        {
            return Ok(false);
        }
    }
}

/// Length on disk of a Set record with these lengths.
fn record_len(key_len: usize, value_len: u32) -> u64 {
    (HEADER_LEN + key_len) as u64 + u64::from(value_len)
}

/// How much `compact` buffers before each write.
const COMPACT_BUFFER: usize = 1 << 20;

fn lock(file: &impl LogFile) -> Result<(), Error> {
    match file.try_lock() {
        Ok(()) => Ok(()),
        Err(TryLockError::WouldBlock) => Err(Error::Locked),
        Err(TryLockError::Error(err)) => Err(Error::Io(err)),
    }
}

/// Appends everything `reader` yields to `file`, in chunks.
fn copy_to_end(mut reader: impl Read, file: &impl LogFile) -> io::Result<()> {
    let mut chunk = vec![0u8; 64 * 1024];
    let mut end = file.size()?;
    loop {
        match reader.read(&mut chunk) {
            Ok(0) => return Ok(()),
            Ok(n) => {
                file.write_all_at(&chunk[..n], end)?;
                end += n as u64;
            }
            Err(err) if err.kind() == ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
}

/// The record `entry` points at, in one read of the length the entry
/// gives. If the header there disagrees with the entry, or the read runs
/// past the end, falls back to `load_record`, which reads the length the
/// header gives and reports what is actually there.
fn load_indexed(file: &impl LogFile, entry: &Entry) -> Result<Record, Error> {
    let len = record_len(usize::from(entry.key_len), entry.value_len);
    let mut buf = vec![0u8; usize::try_from(len).expect("a record fits in memory")];
    match file.read_exact_at(&mut buf, entry.offset) {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
            return load_record(file, entry.offset).map(|(_, record)| record);
        }
        Err(err) => return Err(err.into()),
    }
    let header_buf = buf[..HEADER_LEN].try_into().expect("buf holds a header");
    let header = decode_header(header_buf).map_err(|err| err.at(entry.offset))?;
    if header.record_len() != len {
        return load_record(file, entry.offset).map(|(_, record)| record);
    }
    decode_payload(header, &buf[HEADER_LEN..]).map_err(|err| err.at(entry.offset))
}

fn load_record(file: &impl LogFile, offset: u64) -> Result<(Header, Record), Error> {
    let mut header_buf = [0u8; HEADER_LEN];
    file.read_exact_at(&mut header_buf, offset)?;
    let header = decode_header(&header_buf).map_err(|err| err.at(offset))?;
    let mut payload_buf = vec![0u8; header.payload_len()];
    let header_boundary = (HEADER_LEN as u64) + offset;
    file.read_exact_at(&mut payload_buf, header_boundary)?;
    let record = decode_payload(header, &payload_buf).map_err(|err| err.at(offset))?;
    Ok((header, record))
}

#[derive(Debug, Clone, Copy)]
struct SegmentSize {
    base: u64,
    len: u64,
    dead: u64,
}

/// The bases, in log order, of the segments to clean so that `auto` is no
/// longer due: the largest share of dead bytes first, each taking the log's
/// size down by its dead bytes. Empty if it isn't due.
fn deadest(segments: &[SegmentSize], auto: AutoCompact) -> Vec<u64> {
    let mut dead: u64 = segments.iter().map(|s| s.dead).sum();
    let mut len: u64 = segments.iter().map(|s| s.len).sum();
    let mut candidates: Vec<&SegmentSize> = segments.iter().filter(|s| s.dead > 0).collect();
    candidates.sort_by(|a, b| {
        (u128::from(b.dead) * u128::from(a.len)).cmp(&(u128::from(a.dead) * u128::from(b.len)))
    });
    let mut chosen = Vec::new();
    for segment in candidates {
        if !auto.is_due(dead, len) {
            break;
        }
        dead -= segment.dead;
        len -= segment.dead;
        chosen.push(segment.base);
    }
    chosen.sort_unstable();
    chosen
}

/// Opens every segment in `dir`, in log order, after turning a log from
/// before segments into the first one, putting the marker in `tally.log`,
/// and removing hints whose segment is gone. Fails if two segments overlap.
fn open_segments<F: Fs>(fs: &F, dir: &Path) -> Result<Vec<Segment<F::File>>, Error> {
    let mut changed = migrate(fs, dir)?;
    let names = fs.list(dir)?;
    let mut bases: Vec<u64> = names.iter().filter_map(|n| parse_log_name(n)).collect();
    bases.sort_unstable();
    for base in names.iter().filter_map(|n| parse_hint_name(n)) {
        if bases.binary_search(&base).is_err() {
            fs.remove(&dir.join(hint_name(base)))?;
            changed = true;
        }
    }
    if changed {
        fs.sync_dir(dir)?;
    }
    let mut segments: Vec<Segment<F::File>> = Vec::with_capacity(bases.len());
    for base in bases {
        let file = fs.open_existing(&dir.join(log_name(base)))?;
        let len = file.size()?;
        if let Some(before) = segments.last()
            && before.end() > base
        {
            return Err(Error::Corruption {
                offset: base,
                reason: CorruptionReason::SegmentsOverlap,
            });
        }
        segments.push(Segment {
            base,
            file,
            len,
            dead: 0,
        });
    }
    Ok(segments)
}

/// Turns a `tally.log` from before segments into the segment at 0, with
/// its hint, and leaves the marker in `tally.log`. Returns whether it
/// changed anything the directory must make durable. `tally.log` holds the
/// records or the whole marker at every step, so an earlier version never
/// finds it missing or torn and starts an empty store. Fails if
/// `tally.log` holds records while segments exist: an earlier version
/// wrote them after the log was split, and they can't be put in order.
fn migrate<F: Fs>(fs: &F, dir: &Path) -> Result<bool, Error> {
    let names = fs.list(dir)?;
    let has = |name: &str| names.iter().any(|n| n == name);
    let legacy = dir.join(LOG_FILE_NAME);
    let first = dir.join(log_name(0));
    if has(LOG_FILE_NAME) {
        let file = fs.open_existing(&legacy)?;
        let size = file.size()?;
        if size <= HEADER_LEN as u64 {
            // Shorter than any record: the marker, or a write of it that a
            // power cut tore or left as zeros.
            let mut bytes = vec![0u8; size as usize];
            file.read_exact_at(&mut bytes, 0)?;
            if bytes[..] == marker()[..] {
                return Ok(false);
            }
        } else if !names.iter().any(|n| parse_log_name(n).is_some()) {
            // Without hard links, the rename leaves a window in which
            // `tally.log` is missing.
            if fs.hard_link(&legacy, &first).is_err() {
                fs.rename(&legacy, &first)?;
            }
            if has(HINT_FILE_NAME) {
                fs.rename(&dir.join(HINT_FILE_NAME), &dir.join(hint_name(0)))?;
            }
            remove_if_present(fs, &dir.join(COMPACT_FILE_NAME))?;
            remove_if_present(fs, &dir.join(COMPACT_HINT_FILE_NAME))?;
        } else if !(has(&log_name(0)) && fs.same_file(&legacy, &first)?) {
            return Err(Error::Io(io::Error::other(
                "tally.log holds records next to segment files: an earlier version of tally wrote them",
            )));
        }
    }
    replace_with_marker(fs, dir)?;
    Ok(true)
}

/// Writes the marker to its own file, then renames it over `tally.log`
/// once every name made so far is durable: a link from `tally.log` to the
/// segment at 0 must not be lost while the marker takes its place.
fn replace_with_marker<F: Fs>(fs: &F, dir: &Path) -> io::Result<()> {
    let path = dir.join(MARKER_FILE_NAME);
    let file = fs.open_or_create(&path)?;
    file.set_len(0)?;
    file.write_all_at(&marker(), 0)?;
    file.sync()?;
    fs.sync_dir(dir)?;
    fs.rename(&path, &dir.join(LOG_FILE_NAME))
}

/// Builds the index from every segment in order: from its hint and the
/// records after it, or from all its records. Truncates a torn tail of the
/// newest segment; in any other, one is `CutShort` corruption. A hint that
/// can't be used is logged, removed for good, and the whole index built
/// again without it, since what it put in the index can't be told apart.
fn load<F: Fs>(fs: &F, dir: &Path, segments: &mut [Segment<F::File>]) -> Result<Index, Error> {
    loop {
        match try_load(fs, dir, segments)? {
            Ok(index) => return Ok(index),
            Err((base, hint::Invalid(why))) => {
                log::warn!(
                    "ignored the hint for the segment at {base} ({why}); reading the segment instead"
                );
                fs.remove(&dir.join(hint_name(base)))?;
                fs.sync_dir(dir)?;
            }
        }
    }
}

type HintFailure = (u64, hint::Invalid);

fn try_load<F: Fs>(
    fs: &F,
    dir: &Path,
    segments: &mut [Segment<F::File>],
) -> Result<Result<Index, HintFailure>, Error> {
    let mut hints = Vec::with_capacity(segments.len());
    let mut count = 0u64;
    for segment in segments.iter() {
        let hint = match fs.open_existing(&dir.join(hint_name(segment.base))) {
            Ok(file) => match hint::trailer(&file) {
                Ok(trailer) => {
                    count = count.saturating_add(trailer.count);
                    Some(file)
                }
                Err(invalid) => return Ok(Err((segment.base, invalid))),
            },
            Err(err) if err.kind() == ErrorKind::NotFound => None,
            Err(err) => return Err(err.into()),
        };
        hints.push(hint);
    }
    // The counts are unchecked until every entry is read: never reserve
    // more than the log could hold.
    let total: u64 = segments.iter().map(|s| s.len).sum();
    let capacity = usize::try_from(count.min(total / record_len(1, 1))).unwrap_or(usize::MAX);
    let mut index = Index::with_capacity(capacity);
    let newest = segments.len() - 1;
    for (i, (segment, hint)) in segments.iter_mut().zip(hints).enumerate() {
        let hinted = match hint {
            Some(file) => match read_hint(&file, segment, &mut index, HINT_SIZES, PARALLEL_FROM) {
                Ok(hinted) => hinted,
                Err(invalid) => return Ok(Err((segment.base, invalid))),
            },
            None => segment.base,
        };
        let len = segment.end();
        let end = scan(&*segment, &mut index, hinted, len)?;
        if end < len {
            if i < newest {
                return Err(Error::Corruption {
                    offset: end,
                    reason: CorruptionReason::CutShort,
                });
            }
            segment.set_len(end)?;
            sync(&segment.file);
            segment.len = end - segment.base;
            log::warn!(
                "truncated a torn tail at offset {end}: dropped {} bytes",
                len - end
            );
        }
    }
    Ok(Ok(index))
}

/// Hints with fewer entries than this are read on one thread.
const PARALLEL_FROM: u64 = 100_000;
const MAX_THREADS: usize = 8;
const HINT_SIZES: hint::Sizes = hint::Sizes {
    window_bytes: 16 << 20,
    chunk_entries: 8192,
};

/// Adds the entries of `segment`'s hint `file` to `index`, a window of
/// `sizes` at a time: on one thread below `parallel_from` entries, on up to
/// `MAX_THREADS` from there. Returns the log offset the hint covers to. On
/// `Err`, what it added must be thrown away.
fn read_hint<L: LogFile>(
    file: &impl LogFile,
    segment: &Segment<L>,
    index: &mut Index,
    sizes: hint::Sizes,
    parallel_from: u64,
) -> Result<u64, hint::Invalid> {
    let trailer = hint::trailer(file)?;
    if trailer.log_len > segment.len {
        return Err(hint::Invalid("the segment is shorter than the hint says"));
    }
    let threads = if trailer.count < parallel_from {
        1
    } else {
        thread::available_parallelism()
            .map_or(1, NonZero::get)
            .min(MAX_THREADS)
    };
    let base = segment.base;
    let mut replaced = 0;
    hint::read_entries(file, trailer, sizes, |window| {
        replaced += index.fill(threads, window.chunks(), base, |chunk, filler| {
            for (key, offset, value_len) in window.entries(chunk) {
                let key_len = u16::try_from(key.len()).expect("read_entries checked it");
                filler.push(
                    key,
                    Entry {
                        offset: base + offset,
                        value_len,
                        key_len,
                    },
                );
            }
        });
    })?;
    if replaced > 0 {
        return Err(hint::Invalid("a key appears twice"));
    }
    Ok(base + trailer.log_len)
}

/// Applies the records from `start` to `len` to `index`. Returns where the
/// valid records end: `len`, or the start of a torn or zero-filled tail. A
/// batch is applied only once all of it has been read and checked; a torn
/// one is a torn tail from its start, and an error inside one is reported
/// at its start, so `repair` moves the whole batch.
fn scan(file: &impl LogFile, index: &mut Index, start: u64, len: u64) -> Result<u64, Error> {
    let mut reader = Reader {
        inner: BufReader::new(file.reader_at(start)),
        header_buf: [0u8; HEADER_LEN],
        payload_buf: Vec::new(),
    };
    let mut batch = Vec::new();
    let mut offset = start;
    while offset < len {
        let Next::Op(record_len, op) = reader.next(offset, len)? else {
            return Ok(offset);
        };
        let IndexOp::Batch { body_len } = op else {
            apply(index, offset, op);
            offset += record_len;
            continue;
        };
        let body_start = offset + record_len;
        if len - body_start < body_len {
            return Ok(offset);
        }
        let body_end = body_start + body_len;
        let mut at = body_start;
        while at < body_end {
            let next = reader.next(at, len).map_err(|err| err.moved_to(offset))?;
            let Next::Op(len_here, op) = next else {
                return Ok(offset);
            };
            if matches!(op, IndexOp::Batch { .. }) || len_here > body_end - at {
                return Err(Error::Corruption {
                    offset,
                    reason: CorruptionReason::BadBatch,
                });
            }
            batch.push((at, op));
            at += len_here;
        }
        for (at, op) in batch.drain(..) {
            apply(index, at, op);
        }
        offset = body_end;
    }
    Ok(offset)
}

fn apply(index: &mut Index, offset: u64, op: IndexOp) {
    match op {
        IndexOp::Set { key, value_len } => {
            let key_len = entry_key_len(&key);
            index.insert(
                key.as_bytes(),
                Entry {
                    offset,
                    value_len,
                    key_len,
                },
            );
        }
        IndexOp::Delete { key } => {
            index.remove(key.as_bytes());
        }
        IndexOp::Batch { .. } => unreachable!("scan handles batches"),
    }
}

/// Reads records one after another for `scan`.
struct Reader<R> {
    inner: BufReader<R>,
    header_buf: [u8; HEADER_LEN],
    payload_buf: Vec<u8>,
}

enum Next {
    /// A valid record and its length.
    Op(u64, IndexOp),
    /// The log from here to `len` is a torn or zero-filled tail.
    Torn,
}

impl<R: Read> Reader<R> {
    /// Reads the record at `offset`, which must be where the reader is.
    fn next(&mut self, offset: u64, len: u64) -> Result<Next, Error> {
        if len - offset < HEADER_LEN as u64 {
            return Ok(Next::Torn);
        }
        self.inner.read_exact(&mut self.header_buf)?;
        let header = match decode_header(&self.header_buf) {
            Ok(header) => header,
            Err(DecodeError::Corrupt(CorruptionReason::HeaderChecksum))
                if all_zero(&self.header_buf) && rest_is_zero(&mut self.inner)? =>
            {
                return Ok(Next::Torn);
            }
            Err(err) => return Err(err.at(offset)),
        };
        if len - offset < header.record_len() {
            return Ok(Next::Torn);
        }
        self.payload_buf.resize(header.payload_len(), 0u8);
        self.inner.read_exact(&mut self.payload_buf)?;
        match decode_index_op(header, &self.payload_buf) {
            Ok(op) => Ok(Next::Op(header.record_len(), op)),
            Err(DecodeError::Corrupt(CorruptionReason::PayloadChecksum))
                if all_zero(&self.payload_buf) && rest_is_zero(&mut self.inner)? =>
            {
                Ok(Next::Torn)
            }
            Err(err) => Err(err.at(offset)),
        }
    }
}

fn entry_key_len(key: &Key) -> u16 {
    u16::try_from(key.as_bytes().len()).expect("Key guarantees len fits u16")
}

/// Removes `path`. Returns whether it was there.
fn remove_if_present(fs: &impl Fs, path: &Path) -> io::Result<bool> {
    match fs.remove(path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

fn all_zero(bytes: &[u8]) -> bool {
    bytes.iter().all(|&b| b == 0)
}

/// Reads `reader` to the end in chunks and reports whether every byte was
/// zero. A preallocated log can be gigabytes of zeros, so nothing is kept.
fn rest_is_zero(reader: &mut impl Read) -> io::Result<bool> {
    let mut chunk = [0u8; 8192];
    loop {
        match reader.read(&mut chunk) {
            Ok(0) => return Ok(true),
            Ok(n) if !all_zero(&chunk[..n]) => return Ok(false),
            Ok(_) => {}
            Err(err) if err.kind() == ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
}

/// Flushes `file` to disk. Panics on failure: after a failed fsync the
/// kernel may have dropped the dirty pages, so retrying can report success
/// for data that never reached disk (DESIGN.md, Failed fsync).
fn sync(file: &impl LogFile) {
    file.sync()
        .expect("fsync failed; restart to recover from the log");
}

#[cfg(test)]
mod tests {
    use std::fs::{self, OpenOptions};
    use std::os::unix::fs::FileExt;
    use std::path::PathBuf;

    use super::*;
    use crate::record::tests::{DELETE_A, SET_A_HI};
    use crate::record::{Record, encode};
    use crate::{CorruptionReason, Error, UnsupportedReason};

    fn key(bytes: &[u8]) -> Key {
        Key::try_from(bytes).unwrap()
    }

    fn value(bytes: &[u8]) -> Value {
        Value::try_from(bytes).unwrap()
    }

    /// Bases of the segments in `dir`, in log order.
    fn segment_bases(dir: &Path) -> Vec<u64> {
        let mut bases: Vec<u64> = fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| parse_log_name(e.unwrap().file_name().to_str()?))
            .collect();
        bases.sort_unstable();
        bases
    }

    /// The newest segment, or before the first open, `tally.log`, which
    /// `open` turns into the first segment.
    fn log_path(dir: &Path) -> PathBuf {
        match segment_bases(dir).last() {
            Some(&base) => dir.join(log_name(base)),
            None => dir.join(LOG_FILE_NAME),
        }
    }

    /// Every segment's bytes, in log order.
    fn log_bytes(dir: &Path) -> Vec<u8> {
        let bases = segment_bases(dir);
        if bases.is_empty() {
            return fs::read(dir.join(LOG_FILE_NAME)).unwrap();
        }
        bases
            .into_iter()
            .flat_map(|base| fs::read(dir.join(log_name(base))).unwrap())
            .collect()
    }

    /// Overwrites bytes in the newest segment behind the store's back, at
    /// an offset in its file.
    fn write_log_at(dir: &Path, offset: u64, bytes: &[u8]) {
        let file = OpenOptions::new().write(true).open(log_path(dir)).unwrap();
        file.write_all_at(bytes, offset).unwrap();
    }

    // open

    #[test]
    fn open_creates_the_directory_and_an_empty_log() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("db");
        Store::open(&dir).unwrap();
        assert_eq!(log_bytes(&dir), b"");
    }

    #[test]
    fn open_an_existing_empty_store() {
        let tmp = tempfile::tempdir().unwrap();
        drop(Store::open(tmp.path()).unwrap());
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), None);
    }

    #[test]
    fn a_second_open_of_the_same_directory_is_locked_out() {
        let tmp = tempfile::tempdir().unwrap();
        let first = Store::open(tmp.path()).unwrap();
        assert!(matches!(Store::open(tmp.path()), Err(Error::Locked)));
        drop(first);
        Store::open(tmp.path()).unwrap();
    }

    #[test]
    fn open_fails_on_a_corrupted_record_with_its_offset() {
        let tmp = tempfile::tempdir().unwrap();
        let mut bytes = [SET_A_HI.as_slice(), DELETE_A.as_slice()].concat();
        bytes[16] ^= 0x01; // first record's value: "hi" -> "ii"
        fs::write(log_path(tmp.path()), bytes).unwrap();
        assert!(matches!(
            Store::open(tmp.path()),
            Err(Error::Corruption {
                offset: 0,
                reason: CorruptionReason::PayloadChecksum
            })
        ));
    }

    /// A store directory whose log holds exactly `bytes`.
    fn store_with_log(bytes: &[u8]) -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(log_path(tmp.path()), bytes).unwrap();
        tmp
    }

    fn set_a_hi_then(tail: &[u8]) -> Vec<u8> {
        [SET_A_HI.as_slice(), tail].concat()
    }

    /// DELETE_A's header with its payload (the key) zeroed: the header
    /// reached the disk but the payload did not.
    fn delete_a_with_a_zero_payload() -> Vec<u8> {
        let mut bytes = DELETE_A.to_vec();
        bytes[HEADER_LEN] = 0;
        bytes
    }

    // recovery: torn and zero-filled tails are truncated

    #[test]
    fn open_truncates_a_torn_header() {
        let tmp = store_with_log(&set_a_hi_then(&DELETE_A[..14]));
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        assert_eq!(log_bytes(tmp.path()), SET_A_HI);
    }

    #[test]
    fn open_truncates_a_torn_payload() {
        let tmp = store_with_log(&set_a_hi_then(&DELETE_A[..15]));
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        assert_eq!(log_bytes(tmp.path()), SET_A_HI);
    }

    #[test]
    fn open_truncates_a_zero_filled_tail_shorter_than_a_header() {
        let tmp = store_with_log(&set_a_hi_then(&[0u8; 5]));
        Store::open(tmp.path()).unwrap();
        assert_eq!(log_bytes(tmp.path()), SET_A_HI);
    }

    #[test]
    fn open_truncates_a_zero_filled_tail() {
        let tmp = store_with_log(&set_a_hi_then(&[0u8; 4096]));
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        assert_eq!(log_bytes(tmp.path()), SET_A_HI);
    }

    #[test]
    fn open_truncates_a_log_that_is_all_zeros() {
        let tmp = store_with_log(&[0u8; 4096]);
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), None);
        assert_eq!(log_bytes(tmp.path()), b"");
    }

    #[test]
    fn open_truncates_a_record_whose_payload_is_zeros() {
        let tmp = store_with_log(&set_a_hi_then(&delete_a_with_a_zero_payload()));
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        assert_eq!(log_bytes(tmp.path()), SET_A_HI);
    }

    #[test]
    fn writes_after_recovery_land_where_the_tail_was_cut() {
        let tmp = store_with_log(&set_a_hi_then(&DELETE_A[..14]));
        let mut store = Store::open(tmp.path()).unwrap();
        store.delete(key(b"a")).unwrap();
        drop(store);
        assert_eq!(log_bytes(tmp.path()), set_a_hi_then(&DELETE_A));
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), None);
    }

    #[test]
    fn recovery_is_idempotent() {
        let tmp = store_with_log(&set_a_hi_then(&[0u8; 32]));
        drop(Store::open(tmp.path()).unwrap());
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        assert_eq!(log_bytes(tmp.path()), SET_A_HI);
    }

    // recovery: anything else fails open and leaves the log alone

    fn assert_open_fails_untouched(bytes: &[u8], offset: u64, reason: CorruptionReason) {
        let tmp = store_with_log(bytes);
        let err = Store::open(tmp.path()).err().expect("open must fail");
        assert!(
            matches!(err, Error::Corruption { offset: o, reason: r } if o == offset && r == reason),
            "expected corruption at {offset} ({reason}), got {err:?}"
        );
        assert_eq!(log_bytes(tmp.path()), bytes);
    }

    #[test]
    fn open_fails_on_a_zero_filled_region_before_a_valid_record() {
        let bytes = [SET_A_HI.as_slice(), &[0u8; 16], DELETE_A.as_slice()].concat();
        assert_open_fails_untouched(&bytes, 18, CorruptionReason::HeaderChecksum);
    }

    #[test]
    fn open_fails_on_a_zero_payload_before_a_valid_record() {
        let bytes = [
            SET_A_HI.as_slice(),
            &delete_a_with_a_zero_payload(),
            DELETE_A.as_slice(),
        ]
        .concat();
        assert_open_fails_untouched(&bytes, 18, CorruptionReason::PayloadChecksum);
    }

    #[test]
    fn open_fails_on_a_garbage_tail_with_its_offset() {
        assert_open_fails_untouched(
            &set_a_hi_then(&[0xab; 16]),
            18,
            CorruptionReason::HeaderChecksum,
        );
    }

    #[test]
    fn open_fails_on_a_corrupted_header_checksum_with_its_offset() {
        let mut bytes = set_a_hi_then(&DELETE_A);
        bytes[18] ^= 0x01;
        assert_open_fails_untouched(&bytes, 18, CorruptionReason::HeaderChecksum);
    }

    #[test]
    fn open_fails_on_a_corrupted_length_with_its_offset() {
        let mut bytes = set_a_hi_then(&DELETE_A);
        bytes[18 + 7] ^= 0x01; // second record's value_len: 0 -> 1
        assert_open_fails_untouched(&bytes, 18, CorruptionReason::HeaderChecksum);
    }

    #[test]
    fn open_fails_on_a_corrupted_key_with_its_offset() {
        let mut bytes = set_a_hi_then(&DELETE_A);
        bytes[18 + 15] ^= 0x01; // second record's key: "a" -> "`"
        assert_open_fails_untouched(&bytes, 18, CorruptionReason::PayloadChecksum);
    }

    #[test]
    fn open_fails_on_an_unsupported_tail_record_and_leaves_it() {
        let mut bytes = set_a_hi_then(&DELETE_A);
        bytes[18 + 4] = 9; // second record's type: unknown
        fix_header_checksum(&mut bytes[18..18 + HEADER_LEN]);
        let tmp = store_with_log(&bytes);
        assert!(matches!(
            Store::open(tmp.path()),
            Err(Error::Unsupported {
                offset: 18,
                reason: UnsupportedReason::UnknownType(9)
            })
        ));
        assert_eq!(log_bytes(tmp.path()), bytes);
    }

    fn fix_header_checksum(header: &mut [u8]) {
        let checksum = crc32c::crc32c(&header[4..HEADER_LEN]);
        header[..4].copy_from_slice(&checksum.to_le_bytes());
    }

    // repair

    fn side_file(dir: &Path, offset: u64) -> PathBuf {
        dir.join(format!("{LOG_FILE_NAME}.corrupt-{offset}"))
    }

    /// SET_A_HI, then a Delete of "a" with its key corrupted, then a Set of
    /// "b": the bad record at 18 hides the valid one after it.
    fn log_with_a_corrupt_middle() -> Vec<u8> {
        let mut delete = DELETE_A.to_vec();
        delete[HEADER_LEN] ^= 0x01;
        let mut set_b = Vec::new();
        encode(
            &Record::Set {
                key: key(b"b"),
                value: value(b"2"),
            },
            &mut set_b,
        );
        [SET_A_HI.as_slice(), &delete, &set_b].concat()
    }

    #[test]
    fn repair_of_an_intact_log_does_nothing() {
        let bytes = set_a_hi_then(&DELETE_A);
        let tmp = store_with_log(&bytes);
        assert_eq!(Store::repair(tmp.path()).unwrap(), None);
        assert_eq!(log_bytes(tmp.path()), bytes);
        assert_eq!(
            file_names(tmp.path()),
            ["tally.00000000000000000000.log", "tally.lock", "tally.log"]
        );
    }

    #[test]
    fn repair_leaves_a_torn_tail_for_open() {
        let bytes = set_a_hi_then(&DELETE_A[..14]);
        let tmp = store_with_log(&bytes);
        assert_eq!(Store::repair(tmp.path()).unwrap(), None);
        assert_eq!(log_bytes(tmp.path()), bytes);
    }

    #[test]
    fn repair_moves_the_corrupt_record_and_everything_after_it_to_a_side_file() {
        let bytes = log_with_a_corrupt_middle();
        let tmp = store_with_log(&bytes);
        assert_eq!(
            Store::repair(tmp.path()).unwrap(),
            Some(Repaired {
                offset: 18,
                dropped: 33,
                side_file: side_file(tmp.path(), 18),
            })
        );
        assert_eq!(log_bytes(tmp.path()), SET_A_HI);
        assert_eq!(fs::read(side_file(tmp.path(), 18)).unwrap(), &bytes[18..]);
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        assert_eq!(store.get(&key(b"b")).unwrap(), None);
    }

    #[test]
    fn repair_is_idempotent() {
        let bytes = log_with_a_corrupt_middle();
        let tmp = store_with_log(&bytes);
        Store::repair(tmp.path()).unwrap();
        assert_eq!(Store::repair(tmp.path()).unwrap(), None);
        assert_eq!(log_bytes(tmp.path()), SET_A_HI);
        assert_eq!(fs::read(side_file(tmp.path(), 18)).unwrap(), &bytes[18..]);
    }

    #[test]
    fn repair_rewrites_a_side_file_a_crashed_repair_left() {
        let bytes = log_with_a_corrupt_middle();
        let zeroed_middle = [&bytes[18..20], &[0; 5], &bytes[25..30]].concat();
        for left in [
            &bytes[18..18],
            &bytes[18..25],
            &bytes[18..],
            &[0; 33],
            &zeroed_middle,
        ] {
            let tmp = store_with_log(&bytes);
            fs::write(side_file(tmp.path(), 18), left).unwrap();
            assert_eq!(
                Store::repair(tmp.path()).unwrap(),
                Some(Repaired {
                    offset: 18,
                    dropped: 33,
                    side_file: side_file(tmp.path(), 18),
                }),
                "side file left with {} bytes",
                left.len()
            );
            assert_eq!(log_bytes(tmp.path()), SET_A_HI);
            assert_eq!(fs::read(side_file(tmp.path(), 18)).unwrap(), &bytes[18..]);
        }
    }

    #[test]
    fn repair_refuses_to_overwrite_bytes_an_earlier_repair_saved() {
        let bytes = log_with_a_corrupt_middle();
        let longer = [&bytes[18..], b"!"].concat();
        let changed_first_byte = [&[bytes[18] ^ 1][..], &bytes[19..]].concat();
        for earlier in [
            &b"saved by an earlier repair"[..],
            &longer,
            &changed_first_byte,
        ] {
            let tmp = store_with_log(&bytes);
            let path = side_file(tmp.path(), 18);
            fs::write(&path, earlier).unwrap();
            assert!(matches!(
                Store::repair(tmp.path()),
                Err(Error::SideFileExists(p)) if p == path
            ));
            assert_eq!(log_bytes(tmp.path()), bytes);
            assert_eq!(fs::read(&path).unwrap(), earlier);
        }
    }

    #[test]
    fn repair_refuses_an_unsupported_record() {
        let mut bytes = set_a_hi_then(&DELETE_A);
        bytes[18 + 4] = 9;
        fix_header_checksum(&mut bytes[18..18 + HEADER_LEN]);
        let tmp = store_with_log(&bytes);
        assert!(matches!(
            Store::repair(tmp.path()),
            Err(Error::Unsupported { offset: 18, .. })
        ));
        assert_eq!(log_bytes(tmp.path()), bytes);
    }

    #[test]
    fn repair_is_locked_out_while_the_store_is_open() {
        let tmp = tempfile::tempdir().unwrap();
        let _store = Store::open(tmp.path()).unwrap();
        assert!(matches!(Store::repair(tmp.path()), Err(Error::Locked)));
    }

    // set and get

    #[test]
    fn get_a_missing_key_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), None);
    }

    #[test]
    fn set_then_get() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
    }

    #[test]
    fn set_writes_the_record_format() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        assert_eq!(log_bytes(tmp.path()), SET_A_HI);
    }

    #[test]
    fn set_overwrites_and_appends() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.set(key(b"a"), value(b"yo")).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"yo")));
        assert_eq!(log_bytes(tmp.path()).len(), 36);
    }

    #[test]
    fn keys_are_independent() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"1")).unwrap();
        store.set(key(b"b"), value(b"2")).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"1")));
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
    }

    // delete

    #[test]
    fn delete_removes_the_key_and_appends_a_delete_record() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.delete(key(b"a")).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), None);
        assert_eq!(
            log_bytes(tmp.path()),
            [SET_A_HI.as_slice(), DELETE_A.as_slice()].concat()
        );
    }

    #[test]
    fn delete_of_a_missing_key_writes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.delete(key(b"a")).unwrap();
        assert_eq!(log_bytes(tmp.path()), b"");
    }

    #[test]
    fn delete_of_an_already_deleted_key_writes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.delete(key(b"a")).unwrap();
        store.delete(key(b"a")).unwrap();
        assert_eq!(log_bytes(tmp.path()).len(), 34);
    }

    // reopen

    #[test]
    fn reopen_rebuilds_the_index() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut store = Store::open(tmp.path()).unwrap();
            store.set(key(b"a"), value(b"hi")).unwrap();
            store.set(key(b"b"), value(b"1")).unwrap();
            store.set(key(b"a"), value(b"yo")).unwrap();
            store.delete(key(b"b")).unwrap();
            store.set(key(b"c"), value(b"3")).unwrap();
            store.delete(key(b"c")).unwrap();
            store.set(key(b"c"), value(b"again")).unwrap();
        }
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"yo")));
        assert_eq!(store.get(&key(b"b")).unwrap(), None);
        assert_eq!(store.get(&key(b"c")).unwrap(), Some(value(b"again")));
    }

    #[test]
    fn writes_after_reopen_append_to_the_end() {
        let tmp = tempfile::tempdir().unwrap();
        drop({
            let mut store = Store::open(tmp.path()).unwrap();
            store.set(key(b"a"), value(b"hi")).unwrap();
            store
        });
        let mut store = Store::open(tmp.path()).unwrap();
        store.delete(key(b"a")).unwrap();
        assert_eq!(
            log_bytes(tmp.path()),
            [SET_A_HI.as_slice(), DELETE_A.as_slice()].concat()
        );
        drop(store);
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), None);
    }

    // get verifies what it reads

    #[test]
    fn get_detects_a_corrupted_value_with_the_record_offset() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.set(key(b"b"), value(b"hi")).unwrap();
        write_log_at(tmp.path(), 18 + 17, b"x"); // b's value: "hi" -> "hx"
        assert!(matches!(
            store.get(&key(b"b")),
            Err(Error::Corruption {
                offset: 18,
                reason: CorruptionReason::PayloadChecksum
            })
        ));
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
    }

    #[test]
    fn get_detects_a_corrupted_header() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        write_log_at(tmp.path(), 0, &[0x00]);
        assert!(matches!(
            store.get(&key(b"a")),
            Err(Error::Corruption {
                offset: 0,
                reason: CorruptionReason::HeaderChecksum
            })
        ));
    }

    #[test]
    fn get_detects_a_valid_record_for_a_different_key() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        let mut other = Vec::new();
        encode(
            &Record::Set {
                key: key(b"b"),
                value: value(b"hi"),
            },
            &mut other,
        );
        write_log_at(tmp.path(), 0, &other);
        assert!(matches!(
            store.get(&key(b"a")),
            Err(Error::Corruption {
                offset: 0,
                reason: CorruptionReason::KeyMismatch
            })
        ));
    }

    #[test]
    fn get_detects_a_delete_record_where_the_index_expects_a_set() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        write_log_at(tmp.path(), 0, &DELETE_A);
        assert!(matches!(
            store.get(&key(b"a")),
            Err(Error::Corruption {
                offset: 0,
                reason: CorruptionReason::NotASet
            })
        ));
    }

    fn file_names(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        names
    }

    fn encoded(records: &[Record]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for record in records {
            encode(record, &mut bytes);
        }
        bytes
    }

    fn set_record(k: &[u8], v: &[u8]) -> Record {
        Record::Set {
            key: key(k),
            value: value(v),
        }
    }

    #[test]
    fn compact_keeps_only_the_latest_value_of_each_live_key() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"c"), value(b"3")).unwrap();
        store.set(key(b"a"), value(b"1")).unwrap();
        store.set(key(b"b"), value(b"2")).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.delete(key(b"b")).unwrap();
        assert_eq!(log_bytes(tmp.path()).len(), 85);

        store.compact().unwrap();

        let expected = encoded(&[set_record(b"c", b"3"), set_record(b"a", b"hi")]);
        assert_eq!(expected[17..], SET_A_HI);
        assert_eq!(log_bytes(tmp.path()), expected);
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        assert_eq!(store.get(&key(b"b")).unwrap(), None);
        assert_eq!(store.get(&key(b"c")).unwrap(), Some(value(b"3")));
        drop(store);
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        assert_eq!(store.get(&key(b"b")).unwrap(), None);
        assert_eq!(store.get(&key(b"c")).unwrap(), Some(value(b"3")));
    }

    #[test]
    fn writes_after_compact_land_in_the_new_log_and_survive_a_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"old")).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.compact().unwrap();
        store.set(key(b"b"), value(b"2")).unwrap();
        store.delete(key(b"a")).unwrap();
        drop(store);

        let mut expected = SET_A_HI.to_vec();
        expected.extend(encoded(&[set_record(b"b", b"2")]));
        expected.extend(DELETE_A);
        assert_eq!(log_bytes(tmp.path()), expected);
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), None);
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
    }

    #[test]
    fn compact_of_an_empty_store_leaves_an_empty_log() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.delete(key(b"a")).unwrap();
        store.compact().unwrap();
        assert_eq!(log_bytes(tmp.path()), b"");
        store.set(key(b"a"), value(b"hi")).unwrap();
        assert_eq!(log_bytes(tmp.path()), SET_A_HI);
    }

    #[test]
    fn compact_twice_writes_the_same_log() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"1")).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.compact().unwrap();
        let once = log_bytes(tmp.path());
        store.compact().unwrap();
        assert_eq!(log_bytes(tmp.path()), once);
        assert_eq!(once, SET_A_HI);
    }

    #[test]
    fn compact_leaves_the_log_its_hint_and_the_lock_file() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.compact().unwrap();
        assert_eq!(
            file_names(tmp.path()),
            [
                "tally.00000000000000000018.hint",
                "tally.00000000000000000018.log",
                "tally.lock",
                "tally.log"
            ]
        );
        assert_eq!(fs::read(tmp.path().join(LOG_FILE_NAME)).unwrap(), marker());
    }

    #[test]
    fn keys_sharing_a_hash_read_as_corruption_never_as_each_others_value() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.index = Index::weak();
        store.set(key(b"a"), value(b"1")).unwrap();
        store.set(key(b"b"), value(b"2")).unwrap();

        assert!(matches!(
            store.get(&key(b"a")),
            Err(Error::Corruption {
                offset: 17,
                reason: CorruptionReason::KeyMismatch
            })
        ));
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
        assert_eq!(store.get(&key(b"cc")).unwrap(), None);
    }

    #[test]
    fn get_reads_the_record_even_when_the_index_has_its_lengths_wrong() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"1")).unwrap();
        store.index.get_mut(b"a").unwrap().value_len = 5;
        assert_eq!(
            store.get(&key(b"a")).unwrap(),
            Some(value(b"1")),
            "past the end"
        );

        store.set(key(b"b"), value(b"2")).unwrap();
        store.index.get_mut(b"a").unwrap().value_len = 2;
        assert_eq!(
            store.get(&key(b"a")).unwrap(),
            Some(value(b"1")),
            "header disagrees"
        );
    }

    #[test]
    fn verify_passes_a_healthy_store_before_and_after_compaction() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"1")).unwrap();
        store.set(key(b"b"), value(b"2")).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.delete(key(b"b")).unwrap();
        store.verify().unwrap();
        store.compact().unwrap();
        store.set(key(b"c"), value(b"3")).unwrap();
        store.verify().unwrap();
        drop(store);
        Store::open(tmp.path()).unwrap().verify().unwrap();
    }

    #[test]
    fn verify_finds_damage_that_open_skipped_because_of_the_hint() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        write_log_at(tmp.path(), 16, b"X");
        let store = Store::open(tmp.path()).unwrap();
        assert!(matches!(
            store.verify(),
            Err(Error::Corruption {
                offset: 35,
                reason: CorruptionReason::PayloadChecksum
            })
        ));
    }

    #[test]
    fn verify_finds_an_index_that_disagrees_with_the_log() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        let mut store = Store::open(tmp.path()).unwrap();
        store.index.remove(b"b");
        assert!(matches!(
            store.verify(),
            Err(Error::Corruption {
                offset: 53,
                reason: CorruptionReason::IndexMismatch
            })
        ));
        store.index.get_mut(b"a").unwrap().offset = 53;
        assert!(matches!(
            store.verify(),
            Err(Error::Corruption {
                offset: 35,
                reason: CorruptionReason::IndexMismatch
            })
        ));
    }

    #[test]
    fn verify_finds_a_log_cut_short_behind_the_stores_back() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        let store = Store::open(tmp.path()).unwrap();
        OpenOptions::new()
            .write(true)
            .open(log_path(tmp.path()))
            .unwrap()
            .set_len(18)
            .unwrap();
        assert!(matches!(
            store.verify(),
            Err(Error::Corruption {
                offset: 53,
                reason: CorruptionReason::IndexMismatch
            })
        ));
    }

    // segments

    fn segmented(segment_bytes: u64) -> Options {
        Options {
            auto_compact: None,
            segment_bytes,
        }
    }

    fn put(store: &mut Store, k: &[u8], v: &[u8]) {
        store.set(key(k), value(v)).unwrap();
    }

    fn read(store: &Store, k: &[u8]) -> Option<Vec<u8>> {
        store.get(&key(k)).unwrap().map(|v| v.as_bytes().to_vec())
    }

    #[test]
    fn writes_go_to_a_new_segment_once_one_is_full() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open_with(tmp.path(), segmented(20)).unwrap();
        put(&mut store, b"a", b"hi");
        put(&mut store, b"b", b"2");
        assert_eq!(segment_bases(tmp.path()), [0], "18 bytes is not full yet");
        put(&mut store, b"c", b"3");
        assert_eq!(segment_bases(tmp.path()), [0, 35], "35 bytes is full");
        store
            .write(batch(&[(b"d", Some(b"4")), (b"a", None)]))
            .unwrap();
        assert_eq!(segment_bases(tmp.path()), [0, 35]);
        assert_eq!(
            fs::read(tmp.path().join(log_name(35))).unwrap(),
            encoded(&[
                set_record(b"c", b"3"),
                Record::Batch { body_len: 33 },
                set_record(b"d", b"4"),
                delete_record(b"a"),
            ]),
            "17 bytes is not full: the whole batch goes after c=3"
        );
        assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
        assert_eq!(read(&store, b"c").as_deref(), Some(&b"3"[..]));
        drop(store);

        let store = Store::open_with(tmp.path(), segmented(20)).unwrap();
        assert_eq!(read(&store, b"a"), None);
        assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
        assert_eq!(read(&store, b"c").as_deref(), Some(&b"3"[..]));
        assert_eq!(read(&store, b"d").as_deref(), Some(&b"4"[..]));
        assert_eq!(store.dead(), 18 + 23 + 16);
        store.verify().unwrap();
    }

    /// One record per segment: a=1 at 0, b=2 at 17, a deleted at 34, c=3
    /// at 50.
    fn one_record_per_segment(dir: &Path) -> Store {
        let mut store = Store::open_with(dir, segmented(1)).unwrap();
        put(&mut store, b"a", b"1");
        put(&mut store, b"b", b"2");
        store.delete(key(b"a")).unwrap();
        put(&mut store, b"c", b"3");
        assert_eq!(segment_bases(dir), [0, 17, 34, 50]);
        store
    }

    #[test]
    fn cleaning_a_delete_while_an_older_segment_has_its_key_carries_it() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = one_record_per_segment(tmp.path());
        store.clean(&[34]).unwrap();
        assert_eq!(segment_bases(tmp.path()), [0, 17, 50, 67]);
        assert_eq!(
            fs::read(tmp.path().join(log_name(67))).unwrap(),
            encoded(&[delete_record(b"a")])
        );
        drop(store);

        let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
        assert_eq!(read(&store, b"a"), None, "a=1 at 0 stays hidden");
        assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
        store.verify().unwrap();
    }

    #[test]
    fn cleaning_a_delete_with_every_older_segment_drops_it() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = one_record_per_segment(tmp.path());
        store.clean(&[0, 17, 34]).unwrap();
        assert_eq!(segment_bases(tmp.path()), [50, 67]);
        assert_eq!(
            fs::read(tmp.path().join(log_name(67))).unwrap(),
            encoded(&[set_record(b"b", b"2")]),
            "only b=2 is copied; the Delete has nothing left to hide"
        );
        assert!(tmp.path().join(hint_name(67)).exists());
        drop(store);

        let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
        assert_eq!(read(&store, b"a"), None);
        assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
        assert_eq!(read(&store, b"c").as_deref(), Some(&b"3"[..]));
        assert_eq!(store.dead(), 0);
        store.verify().unwrap();
    }

    #[test]
    fn get_reads_each_key_from_its_own_segment() {
        let tmp = tempfile::tempdir().unwrap();
        let store = one_record_per_segment(tmp.path());
        assert_eq!(store.index.get(b"b").unwrap().offset, 17);
        assert_eq!(store.index.get(b"c").unwrap().offset, 50);
        write_log_at(tmp.path(), 15, b"X");
        assert!(matches!(
            store.get(&key(b"c")),
            Err(Error::Corruption {
                offset: 50,
                reason: CorruptionReason::PayloadChecksum
            })
        ));
        assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
    }

    fn size(sizes: &[(u64, u64, u64)]) -> Vec<SegmentSize> {
        sizes
            .iter()
            .map(|&(base, len, dead)| SegmentSize { base, len, dead })
            .collect()
    }

    #[test]
    fn automatic_compaction_cleans_the_deadest_segments_until_it_is_not_due() {
        let auto = AutoCompact::new(50, 0).unwrap();
        // 315 dead of 500. Cleaning 100 (90% dead) leaves 225 of 410,
        // still due; then 200 (60%) leaves 105 of 290, not due.
        let segments = size(&[
            (0, 100, 55),
            (100, 100, 90),
            (200, 200, 120),
            (400, 100, 50),
            (500, 0, 0),
        ]);
        assert_eq!(deadest(&segments, auto), [100, 200]);
        assert_eq!(
            deadest(&size(&[(0, 100, 49), (100, 0, 0)]), auto),
            [] as [u64; 0]
        );
        assert_eq!(deadest(&size(&[(0, 100, 50), (100, 0, 0)]), auto), [0]);
    }

    #[test]
    fn automatic_compaction_leaves_segments_without_dead_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let options = Options {
            auto_compact: AutoCompact::new(50, 0),
            segment_bytes: 1,
        };
        let mut store = Store::open_with(tmp.path(), options).unwrap();
        put(&mut store, b"a", b"1");
        put(&mut store, b"b", b"2");
        put(&mut store, b"b", b"3");
        // b=3 at 34 made b=2 at 17 dead: 17 of 51, not due. Deleting b
        // makes 17 + 17 + 16 of 67 dead. Cleaning the segment at 17 leaves
        // 33 of 50, still due; then 34 leaves 16 of 33, not due. Neither has
        // a live record, and a=1 at 0 is never read.
        store.delete(key(b"b")).unwrap();
        assert_eq!(segment_bases(tmp.path()), [0, 51, 67]);
        assert_eq!(fs::read(tmp.path().join(log_name(67))).unwrap(), b"");
        assert_eq!(store.dead(), 16);
        drop(store);
        let store = Store::open_with(tmp.path(), options).unwrap();
        assert_eq!(read(&store, b"a").as_deref(), Some(&b"1"[..]));
        assert_eq!(read(&store, b"b"), None);
    }

    #[test]
    fn a_sealed_segment_cut_short_is_corruption_that_repair_moves() {
        let tmp = tempfile::tempdir().unwrap();
        drop(one_record_per_segment(tmp.path()));
        let second = tmp.path().join(log_name(17));
        fs::write(&second, &fs::read(&second).unwrap()[..10]).unwrap();
        assert!(matches!(
            Store::open_with(tmp.path(), segmented(1)),
            Err(Error::Corruption {
                offset: 17,
                reason: CorruptionReason::CutShort
            })
        ));

        let repaired = Store::repair(tmp.path()).unwrap().unwrap();
        assert_eq!((repaired.offset, repaired.dropped), (17, 10));
        let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
        assert_eq!(read(&store, b"b"), None);
        assert_eq!(read(&store, b"c").as_deref(), Some(&b"3"[..]));
        assert_eq!(read(&store, b"a"), None);
    }

    #[test]
    fn segments_that_overlap_are_refused() {
        let tmp = tempfile::tempdir().unwrap();
        drop(one_record_per_segment(tmp.path()));
        fs::write(
            tmp.path().join(log_name(30)),
            encoded(&[set_record(b"x", b"1")]),
        )
        .unwrap();
        assert!(matches!(
            Store::open_with(tmp.path(), segmented(1)),
            Err(Error::Corruption {
                offset: 30,
                reason: CorruptionReason::SegmentsOverlap
            })
        ));
    }

    #[test]
    fn a_hint_whose_segment_is_gone_is_removed() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        let orphan = tmp.path().join(hint_name(1000));
        fs::copy(hint_path(tmp.path()), &orphan).unwrap();
        Store::open(tmp.path()).unwrap();
        assert!(!orphan.exists());
    }

    #[test]
    fn a_hint_that_fails_after_filling_the_index_leaves_nothing_behind() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        // The hint's one entry for "a" now names "c": its lengths still add
        // up, so "c" goes into the index before the checksum fails.
        let mut hint = fs::read(hint_path(tmp.path())).unwrap();
        assert_eq!(hint[11], b'a');
        hint[11] = b'c';
        fs::write(hint_path(tmp.path()), hint).unwrap();

        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(read(&store, b"c"), None);
        assert_eq!(read(&store, b"a").as_deref(), Some(&b"hi"[..]));
        store.verify().unwrap();
    }

    // migration from before segments

    #[test]
    fn a_log_from_before_segments_becomes_the_first_segment_with_its_hint() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        fs::rename(
            tmp.path().join(log_name(35)),
            tmp.path().join(LOG_FILE_NAME),
        )
        .unwrap();
        fs::rename(
            tmp.path().join(hint_name(35)),
            tmp.path().join(HINT_FILE_NAME),
        )
        .unwrap();
        fs::write(tmp.path().join(COMPACT_FILE_NAME), b"left by a crash").unwrap();
        write_log_at(tmp.path(), 16, b"X");

        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(
            file_names(tmp.path()),
            [
                "tally.00000000000000000000.hint",
                "tally.00000000000000000000.log",
                "tally.lock",
                "tally.log"
            ]
        );
        assert_eq!(fs::read(tmp.path().join(LOG_FILE_NAME)).unwrap(), marker());
        assert!(
            matches!(
                store.get(&key(b"a")),
                Err(Error::Corruption {
                    offset: 0,
                    reason: CorruptionReason::PayloadChecksum
                })
            ),
            "read from the hint, not the log"
        );
        assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
    }

    #[test]
    fn a_move_to_segments_stopped_after_its_link_is_finished() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        fs::rename(
            tmp.path().join(log_name(35)),
            tmp.path().join(LOG_FILE_NAME),
        )
        .unwrap();
        fs::remove_file(tmp.path().join(hint_name(35))).unwrap();
        fs::hard_link(tmp.path().join(LOG_FILE_NAME), tmp.path().join(log_name(0))).unwrap();

        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(
            file_names(tmp.path()),
            ["tally.00000000000000000000.log", "tally.lock", "tally.log"]
        );
        assert_eq!(fs::read(tmp.path().join(LOG_FILE_NAME)).unwrap(), marker());
        assert_eq!(read(&store, b"a").as_deref(), Some(&b"hi"[..]));
        assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
    }

    #[test]
    fn the_marker_is_an_unsupported_record_to_any_earlier_tally() {
        assert_eq!(
            decode_header(&marker()),
            Err(DecodeError::Unsupported(UnsupportedReason::UnknownType(
                255
            )))
        );
    }

    #[test]
    fn a_torn_or_zeroed_marker_is_written_again() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        for torn in [&marker()[..7], &[], &[0; HEADER_LEN]] {
            fs::write(tmp.path().join(LOG_FILE_NAME), torn).unwrap();
            let store = Store::open(tmp.path()).unwrap();
            assert_eq!(fs::read(tmp.path().join(LOG_FILE_NAME)).unwrap(), marker());
            assert_eq!(read(&store, b"a").as_deref(), Some(&b"hi"[..]));
        }
    }

    #[test]
    fn records_in_tally_log_next_to_segments_are_refused() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        fs::write(tmp.path().join(LOG_FILE_NAME), SET_A_HI).unwrap();
        assert!(matches!(Store::open(tmp.path()), Err(Error::Io(_))));
        assert_eq!(fs::read(tmp.path().join(LOG_FILE_NAME)).unwrap(), SET_A_HI);
    }

    // batches

    fn batch(ops: &[(&[u8], Option<&[u8]>)]) -> Batch {
        let mut batch = Batch::new();
        for (k, v) in ops {
            match v {
                Some(v) => batch.set(key(k), value(v)),
                None => batch.delete(key(k)),
            };
        }
        batch
    }

    fn delete_record(k: &[u8]) -> Record {
        Record::Delete { key: key(k) }
    }

    #[test]
    fn write_appends_one_batch_and_applies_it_in_order() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store
            .write(batch(&[
                (b"a", Some(b"1")),
                (b"b", Some(b"2")),
                (b"a", Some(b"hi")),
                (b"b", None),
            ]))
            .unwrap();

        assert_eq!(
            log_bytes(tmp.path()),
            encoded(&[
                Record::Batch { body_len: 68 },
                set_record(b"a", b"1"),
                set_record(b"b", b"2"),
                set_record(b"a", b"hi"),
                delete_record(b"b"),
            ])
        );
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        assert_eq!(store.get(&key(b"b")).unwrap(), None);
        assert_eq!(store.dead(), 23 + 17 + 17 + 16);
        drop(store);
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        assert_eq!(store.get(&key(b"b")).unwrap(), None);
        assert_eq!(store.dead(), 23 + 17 + 17 + 16);
        store.verify().unwrap();
    }

    #[test]
    fn a_batch_deletes_only_keys_live_at_that_point_in_it() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.write(batch(&[(b"a", None), (b"b", None)])).unwrap();
        assert_eq!(
            log_bytes(tmp.path()),
            b"",
            "nothing to delete, nothing written"
        );

        store.write(Batch::new()).unwrap();
        assert_eq!(log_bytes(tmp.path()), b"");

        store
            .write(batch(&[(b"a", Some(b"1")), (b"a", None), (b"a", None)]))
            .unwrap();
        assert_eq!(
            log_bytes(tmp.path()),
            encoded(&[
                Record::Batch { body_len: 33 },
                set_record(b"a", b"1"),
                delete_record(b"a"),
            ])
        );
        assert_eq!(store.get(&key(b"a")).unwrap(), None);
    }

    /// b=2, then a batch setting a=1 and deleting b.
    fn log_with_a_batch() -> (Vec<u8>, usize) {
        let before = encoded(&[set_record(b"b", b"2")]);
        let mut log = before.clone();
        log.extend(encoded(&[
            Record::Batch { body_len: 33 },
            set_record(b"a", b"1"),
            delete_record(b"b"),
        ]));
        (log, before.len())
    }

    #[test]
    fn a_batch_cut_anywhere_is_dropped_whole() {
        let (log, before) = log_with_a_batch();
        for cut in before..log.len() {
            let tmp = tempfile::tempdir().unwrap();
            fs::write(log_path(tmp.path()), &log[..cut]).unwrap();
            let store = Store::open(tmp.path()).unwrap();
            assert_eq!(store.get(&key(b"a")).unwrap(), None, "cut at {cut}");
            assert_eq!(
                store.get(&key(b"b")).unwrap(),
                Some(value(b"2")),
                "cut at {cut}"
            );
            assert_eq!(log_bytes(tmp.path()).len(), before, "cut at {cut}");
        }
        let tmp = tempfile::tempdir().unwrap();
        fs::write(log_path(tmp.path()), &log).unwrap();
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"1")));
        assert_eq!(store.get(&key(b"b")).unwrap(), None);
    }

    #[test]
    fn a_batch_whose_last_record_landed_as_zeros_is_dropped_whole() {
        let (mut log, before) = log_with_a_batch();
        let len = log.len();
        log[len - 16..].fill(0);
        let tmp = tempfile::tempdir().unwrap();
        fs::write(log_path(tmp.path()), &log).unwrap();
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), None);
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
        assert_eq!(log_bytes(tmp.path()).len(), before);
    }

    #[test]
    fn corruption_inside_a_batch_is_reported_and_repaired_at_the_batch() {
        let (mut log, before) = log_with_a_batch();
        log[before + 23 + 16] ^= 0x01;
        let tmp = tempfile::tempdir().unwrap();
        fs::write(log_path(tmp.path()), &log).unwrap();
        assert!(matches!(
            Store::open(tmp.path()),
            Err(Error::Corruption {
                offset: 17,
                reason: CorruptionReason::PayloadChecksum
            })
        ));

        let repaired = Store::repair(tmp.path()).unwrap().unwrap();
        assert_eq!((repaired.offset, repaired.dropped), (17, 56));
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), None);
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
    }

    #[test]
    fn a_batch_inside_a_batch_or_a_record_past_its_end_is_corrupt() {
        let nested = encoded(&[
            Record::Batch { body_len: 40 },
            Record::Batch { body_len: 17 },
            set_record(b"a", b"1"),
        ]);
        let short = encoded(&[Record::Batch { body_len: 10 }, set_record(b"a", b"1")]);
        for log in [nested, short] {
            let tmp = tempfile::tempdir().unwrap();
            fs::write(log_path(tmp.path()), &log).unwrap();
            assert!(matches!(
                Store::open(tmp.path()),
                Err(Error::Corruption {
                    offset: 0,
                    reason: CorruptionReason::BadBatch
                })
            ));
        }
    }

    #[test]
    fn compaction_after_a_batch_keeps_only_the_live_sets() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store
            .write(batch(&[
                (b"a", Some(b"1")),
                (b"b", Some(b"2")),
                (b"a", None),
            ]))
            .unwrap();
        store.compact().unwrap();
        assert_eq!(log_bytes(tmp.path()), encoded(&[set_record(b"b", b"2")]));
        assert_eq!(store.dead(), 0);
        drop(store);
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
        store.verify().unwrap();
    }

    // hint file

    /// The hint of the newest segment.
    fn hint_path(dir: &Path) -> PathBuf {
        dir.join(hint_name(*segment_bases(dir).last().unwrap()))
    }

    /// a=hi at 0 and b=2 at 18, compacted into the segment at 35, so a is
    /// at 35, b at 53, and the segment's hint covers both.
    fn compacted_a_and_b(dir: &Path) {
        let mut store = Store::open(dir).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.set(key(b"b"), value(b"2")).unwrap();
        store.compact().unwrap();
    }

    #[test]
    fn open_after_compact_reads_keys_from_the_hint_not_the_log() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        write_log_at(tmp.path(), 16, b"X");

        let store = Store::open(tmp.path()).unwrap();
        assert!(matches!(
            store.get(&key(b"a")),
            Err(Error::Corruption {
                offset: 35,
                reason: CorruptionReason::PayloadChecksum
            })
        ));
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
    }

    #[test]
    fn a_hint_read_on_many_threads_gives_the_index_one_thread_does() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open_with(
            tmp.path(),
            Options {
                auto_compact: None,
                ..Options::default()
            },
        )
        .unwrap();
        let mut batch = Batch::new();
        for i in 0..3000u32 {
            batch.set(
                key(format!("k{i}").as_bytes()),
                value(&vec![b'v'; 1 + i as usize % 7]),
            );
        }
        store.write(batch).unwrap();
        store.compact().unwrap();
        drop(store);

        let fs = OsFs;
        let [base] = segment_bases(tmp.path())[..] else {
            panic!("compaction leaves one segment");
        };
        let file = fs.open_existing(&tmp.path().join(hint_name(base))).unwrap();
        let segment = Segment {
            base,
            file: fs.open_existing(&tmp.path().join(log_name(base))).unwrap(),
            len: log_bytes(tmp.path()).len() as u64,
            dead: 0,
        };
        let mut one = Index::new();
        let len_one = read_hint(&file, &segment, &mut one, HINT_SIZES, u64::MAX).unwrap();
        let small = hint::Sizes {
            window_bytes: 4096,
            chunk_entries: 50,
        };
        let mut many = Index::new();
        let len_many = read_hint(&file, &segment, &mut many, small, 0).unwrap();

        assert_eq!((len_one, len_many), (segment.end(), segment.end()));
        assert_eq!(many.len(), 3000);
        for i in 0..3000u32 {
            let k = format!("k{i}");
            assert_eq!(many.get(k.as_bytes()), one.get(k.as_bytes()), "{k}");
        }
        let store = Store::open(tmp.path()).unwrap();
        store.verify().unwrap();
    }

    #[test]
    fn records_written_after_the_hint_are_read_from_the_log() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"c"), value(b"3")).unwrap();
        store.delete(key(b"a")).unwrap();
        store.set(key(b"b"), value(b"22")).unwrap();
        drop(store);

        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), None);
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"22")));
        assert_eq!(store.get(&key(b"c")).unwrap(), Some(value(b"3")));
        assert_eq!(store.dead(), 18 + 16 + 17);
    }

    #[test]
    fn a_damaged_hint_is_removed_and_the_whole_log_read() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        let mut hint = fs::read(hint_path(tmp.path())).unwrap();
        hint[6] ^= 0x01;
        fs::write(hint_path(tmp.path()), hint).unwrap();

        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(
            file_names(tmp.path()),
            ["tally.00000000000000000035.log", "tally.lock", "tally.log"]
        );
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
    }

    #[test]
    fn a_hint_for_more_log_than_there_is_is_removed() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        let log = log_bytes(tmp.path());
        fs::write(log_path(tmp.path()), &log[..18]).unwrap();

        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(
            file_names(tmp.path()),
            ["tally.00000000000000000035.log", "tally.lock", "tally.log"]
        );
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        assert_eq!(store.get(&key(b"b")).unwrap(), None);
    }

    #[test]
    fn repair_removes_the_hint_before_truncating() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        write_log_at(tmp.path(), 33, b"X");

        let repaired = Store::repair(tmp.path()).unwrap().unwrap();
        assert_eq!(repaired.offset, 53);
        assert!(!hint_path(tmp.path()).exists());
        let mut store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"b")).unwrap(), None);
        store.set(key(b"c"), value(b"3")).unwrap();
        drop(store);
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        assert_eq!(store.get(&key(b"c")).unwrap(), Some(value(b"3")));
    }

    #[test]
    fn a_second_compaction_replaces_the_hint() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        let mut store = Store::open(tmp.path()).unwrap();
        store.delete(key(b"a")).unwrap();
        store.compact().unwrap();
        drop(store);

        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(log_bytes(tmp.path()), encoded(&[set_record(b"b", b"2")]));
        assert_eq!(store.get(&key(b"a")).unwrap(), None);
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
        assert_eq!(store.dead(), 0);
    }

    #[test]
    fn compact_stops_at_a_corrupt_dead_record_too() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"b"), value(b"2")).unwrap();
        store.set(key(b"a"), value(b"1")).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        write_log_at(tmp.path(), 33, b"X");
        let before = log_bytes(tmp.path());

        assert!(matches!(
            store.compact(),
            Err(Error::Corruption {
                offset: 17,
                reason: CorruptionReason::PayloadChecksum
            })
        ));
        assert_eq!(log_bytes(tmp.path()), before);
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
    }

    #[test]
    fn compact_overwrites_a_compact_file_left_by_a_crash() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join(COMPACT_FILE_NAME), [0xAB; 100]).unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.compact().unwrap();
        assert_eq!(log_bytes(tmp.path()), SET_A_HI);
    }

    #[test]
    fn the_lock_still_holds_after_compact_replaces_the_log() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.compact().unwrap();
        assert!(matches!(Store::open(tmp.path()), Err(Error::Locked)));
        assert!(matches!(Store::repair(tmp.path()), Err(Error::Locked)));
        drop(store);
        Store::open(tmp.path()).unwrap();
    }

    #[test]
    fn compact_stops_at_a_corrupt_live_record_and_leaves_the_log_untouched() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.set(key(b"b"), value(b"2")).unwrap();
        store.delete(key(b"b")).unwrap();
        write_log_at(tmp.path(), 17, b"X");
        let before = log_bytes(tmp.path());

        assert!(matches!(
            store.compact(),
            Err(Error::Corruption {
                offset: 0,
                reason: CorruptionReason::PayloadChecksum
            })
        ));
        assert_eq!(log_bytes(tmp.path()), before);
        store.set(key(b"c"), value(b"3")).unwrap();
        assert_eq!(store.get(&key(b"c")).unwrap(), Some(value(b"3")));
    }

    #[test]
    fn a_failed_compaction_points_every_key_back_at_the_old_log() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"1")).unwrap();
        store.set(key(b"b"), value(b"2")).unwrap();
        store.set(key(b"c"), value(b"3")).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        write_log_at(tmp.path(), 49, b"X");

        assert!(matches!(
            store.compact(),
            Err(Error::Corruption {
                offset: 34,
                reason: CorruptionReason::PayloadChecksum
            })
        ));
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        store.set(key(b"d"), value(b"4")).unwrap();
        assert_eq!(store.get(&key(b"d")).unwrap(), Some(value(b"4")));
    }

    #[test]
    fn a_compaction_that_fails_after_writing_copies_empties_the_new_segment() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        let big = vec![b'v'; 700 << 10];
        store.set(key(b"a"), value(&big)).unwrap();
        store.set(key(b"b"), value(&big)).unwrap();
        store.set(key(b"c"), value(b"3")).unwrap();
        let c = 2 * (15 + 1 + (700 << 10));
        write_log_at(tmp.path(), c + 16, b"X");

        // More than the 1 MiB compaction buffer is copied before c fails.
        assert!(matches!(
            store.compact(),
            Err(Error::Corruption { offset, reason: CorruptionReason::PayloadChecksum })
                if offset == c
        ));
        let end = c + 17;
        assert_eq!(segment_bases(tmp.path()), [0, end]);
        assert_eq!(fs::read(tmp.path().join(log_name(end))).unwrap(), b"");
        assert!(!tmp.path().join(hint_name(end)).exists());
        assert_eq!(read(&store, b"a"), Some(big.clone()));
        assert_eq!(read(&store, b"b"), Some(big));
        put(&mut store, b"d", b"4");
        assert_eq!(
            fs::read(tmp.path().join(log_name(end))).unwrap(),
            encoded(&[set_record(b"d", b"4")])
        );
        assert_eq!(store.index.get(b"d").unwrap().offset, end);
    }

    fn auto(dead_percent: u8, min_log_bytes: u64) -> Options {
        Options {
            auto_compact: AutoCompact::new(dead_percent, min_log_bytes),
            ..Options::default()
        }
    }

    #[test]
    fn a_write_that_leaves_half_the_log_dead_compacts_it() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open_with(tmp.path(), auto(50, 0)).unwrap();
        store.set(key(b"a"), value(b"1")).unwrap();
        store.set(key(b"b"), value(b"2")).unwrap();
        store.set(key(b"a"), value(b"3")).unwrap();
        assert_eq!(log_bytes(tmp.path()).len(), 51, "17 of 51 dead");

        store.set(key(b"b"), value(b"4")).unwrap();
        assert_eq!(
            log_bytes(tmp.path()),
            encoded(&[set_record(b"a", b"3"), set_record(b"b", b"4")])
        );

        store.set(key(b"a"), value(b"5")).unwrap();
        assert_eq!(
            log_bytes(tmp.path()).len(),
            51,
            "compaction left nothing dead, so 17 of 51 now"
        );
    }

    #[test]
    fn a_delete_counts_its_own_record_as_dead_too() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open_with(tmp.path(), auto(50, 0)).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.set(key(b"b"), value(b"2")).unwrap();
        store.delete(key(b"a")).unwrap();
        assert_eq!(log_bytes(tmp.path()), encoded(&[set_record(b"b", b"2")]));
    }

    #[test]
    fn a_log_below_the_minimum_size_is_not_compacted() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open_with(tmp.path(), auto(50, 35)).unwrap();
        store.set(key(b"a"), value(b"1")).unwrap();
        store.set(key(b"a"), value(b"2")).unwrap();
        assert_eq!(log_bytes(tmp.path()).len(), 34);
        store.set(key(b"a"), value(b"3")).unwrap();
        assert_eq!(log_bytes(tmp.path()), encoded(&[set_record(b"a", b"3")]));
    }

    #[test]
    fn without_auto_compact_the_log_only_grows() {
        let tmp = tempfile::tempdir().unwrap();
        let options = Options {
            auto_compact: None,
            ..Options::default()
        };
        let mut store = Store::open_with(tmp.path(), options).unwrap();
        for v in [b"1", b"2", b"3"] {
            store.set(key(b"a"), value(v)).unwrap();
        }
        store.delete(key(b"a")).unwrap();
        drop(store);
        Store::open_with(tmp.path(), options).unwrap();
        assert_eq!(log_bytes(tmp.path()).len(), 67);
    }

    #[test]
    fn open_compacts_a_log_that_is_due() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open_with(
            tmp.path(),
            Options {
                auto_compact: None,
                ..Options::default()
            },
        )
        .unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        drop(store);

        let store = Store::open_with(tmp.path(), auto(50, 0)).unwrap();
        assert_eq!(log_bytes(tmp.path()), SET_A_HI);
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
    }

    #[test]
    fn open_counts_the_dead_bytes_already_in_the_log() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open_with(
            tmp.path(),
            Options {
                auto_compact: None,
                ..Options::default()
            },
        )
        .unwrap();
        store.set(key(b"a"), value(b"1")).unwrap();
        store.set(key(b"b"), value(b"2")).unwrap();
        store.set(key(b"a"), value(b"3")).unwrap();
        drop(store);

        let mut store = Store::open_with(tmp.path(), auto(50, 0)).unwrap();
        assert_eq!(log_bytes(tmp.path()).len(), 51, "17 of 51 dead");
        store.set(key(b"b"), value(b"4")).unwrap();
        assert_eq!(
            log_bytes(tmp.path()),
            encoded(&[set_record(b"a", b"3"), set_record(b"b", b"4")])
        );
    }

    #[test]
    fn a_failed_automatic_compaction_keeps_the_write_and_stays_off() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open_with(tmp.path(), auto(50, 0)).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.set(key(b"b"), value(b"2")).unwrap();
        write_log_at(tmp.path(), 17, b"X");
        store.set(key(b"b"), value(b"3")).unwrap();
        store.set(key(b"b"), value(b"4")).unwrap();
        assert_eq!(store.auto_compact, AutoCompact::new(50, 0), "34 of 69 dead");

        store.set(key(b"b"), value(b"5")).unwrap();
        assert_eq!(store.auto_compact, None);
        assert_eq!(log_bytes(tmp.path()).len(), 86);
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"5")));
    }
}
