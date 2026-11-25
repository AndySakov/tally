use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::TryLockError;
use std::io::{self, BufRead, BufReader, ErrorKind, Read};
use std::num::NonZero;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, RwLock, mpsc};
use std::time::{Duration, Instant};
use std::{mem, thread};

use crate::fs::{Fs, LogFile, OsFs};
use crate::hint::{self, COMPACT_HINT_FILE_NAME, HINT_FILE_NAME};
use crate::index::{Entry, Index};
use crate::options::{AutoCompact, Options};
use crate::record::{
    Fields, Format, HEADER_LEN, Header, IndexOp, Kind, Record, SHORTEST_HEADER_LEN, decode_header,
    decode_index_op, decode_payload, encode, encode_set,
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
pub(crate) fn marker() -> [u8; SHORTEST_HEADER_LEN] {
    let mut bytes = [0u8; SHORTEST_HEADER_LEN];
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
/// Every method takes `&self`, and a `Store` on the real file system is
/// `Send` and `Sync`: share one between threads with an `Arc`. Reads run in
/// parallel with each other and with writes. Writes from several threads
/// share fsyncs (DESIGN.md, Group commit).
///
/// `F` is the file system. It is always [`OsFs`] outside tally's own tests,
/// which swap in an in-memory disk to simulate power cuts.
pub struct Store<F: Fs = OsFs> {
    fs: F,
    dir: PathBuf,
    /// Holds the exclusive lock for as long as the store is open.
    _lock: F::File,
    /// In log order, none overlapping. Writes go to the last, which is
    /// never empty of a file: there is always one to write to. Readers take
    /// the list as it is; it is only replaced, by whoever holds `writing`.
    segments: RwLock<Segments<F::File>>,
    segment_bytes: u64,
    auto_compact: Option<AutoCompact>,
    /// Set once an automatic compaction has failed, until the next open.
    auto_failed: AtomicBool,
    index: Index,
    /// Held by whoever appends to the log or changes its segments: the
    /// leader of a group of writes, a compaction, or `verify`.
    writing: Mutex<()>,
    /// Writes waiting for a leader, and the results of those done.
    queue: Mutex<Queue>,
    /// Signalled when a group of writes is done.
    committed: Condvar,
    /// Signalled when a write joins the queue, for a leader waiting to
    /// fill its group.
    arrived: Condvar,
    /// How long the last fsync of the log took, in nanoseconds.
    last_sync_nanos: AtomicU64,
    /// Set by a failed fsync, or a failed truncate after a failed write:
    /// from then on writes fail with `Poisoned` (DESIGN.md, Failed fsync).
    poisoned: AtomicBool,
    /// Appends fsynced, for tests of group commit.
    #[cfg(test)]
    commits: AtomicU64,
}

type Segments<L> = Arc<Vec<Arc<Segment<L>>>>;

/// Holds `Store::writing`, and is the evidence that the caller does. A
/// thread that panics holding it may have left the log half changed, so
/// the store is poisoned.
struct Writing<'a> {
    _guard: MutexGuard<'a, ()>,
    poisoned: &'a AtomicBool,
}

impl Drop for Writing<'_> {
    fn drop(&mut self) {
        if thread::panicking() {
            self.poisoned.store(true, Ordering::Release);
        }
    }
}

/// A write a caller asked for: one record, or a batch.
pub(crate) enum Write {
    One(Record),
    Batch(Vec<Record>),
}

impl Write {
    /// About how many bytes it writes, to cap a group.
    fn size(&self) -> usize {
        let records = match self {
            Write::One(record) => std::slice::from_ref(record),
            Write::Batch(records) => records,
        };
        records
            .iter()
            .map(|record| match record {
                Record::Set { key, value } => key.as_bytes().len() + value.as_bytes().len(),
                Record::Delete { key } => key.as_bytes().len(),
                Record::Batch { .. } => 0,
            })
            .sum()
    }
}

#[derive(Default)]
struct Queue {
    waiting: VecDeque<(u64, Write)>,
    next: u64,
    done: HashMap<u64, Result<(), Error>>,
    /// Whether a leader is writing a group now.
    leading: bool,
    /// How many writes the last group took.
    last_group: usize,
}

/// The share of the last fsync's time, in percent, a leader waits for its
/// group to fill (DESIGN.md, Group commit). 25 and 50 gained nothing more.
const GROUP_WAIT_PERCENT: u64 = 10;

/// A group takes waiting writes until it has this many, or this many bytes.
const GROUP_MOST_WRITES: usize = 1024;
const GROUP_MOST_BYTES: usize = 4 << 20;

/// Locks `mutex` even if a thread panicked holding it. Every lock here
/// guards state a panic can't leave half changed, or the poisoned flag
/// covers it.
fn hold<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
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
            segments.push(Segment::new(0, file, 0));
        }
        segments.last().expect("one was just made").file.sync()?;
        fs.sync_dir(dir)?;
        fs.sync_dir(parent)?;
        let index = load(&fs, dir, &mut segments)?;
        let sealed = segments.len() - 1;
        for segment in &segments[..sealed] {
            segment.seal();
        }
        let store = Store {
            fs,
            dir: dir.to_path_buf(),
            _lock: lock_file,
            segments: RwLock::new(Arc::new(segments.into_iter().map(Arc::new).collect())),
            segment_bytes: options.segment_bytes,
            auto_compact: options.auto_compact,
            auto_failed: AtomicBool::new(false),
            index,
            writing: Mutex::new(()),
            queue: Mutex::default(),
            committed: Condvar::new(),
            arrived: Condvar::new(),
            last_sync_nanos: AtomicU64::new(0),
            poisoned: AtomicBool::new(false),
            #[cfg(test)]
            commits: AtomicU64::new(0),
        };
        {
            let writing = store.writer();
            store.count_dead(&writing);
            store.compact_if_due(&writing);
        }
        Ok(store)
    }

    pub(crate) fn repair_in(fs: &F, dir: &Path) -> Result<Option<Repaired>, Error> {
        let _lock = fs.open_or_create(&dir.join(LOCK_FILE_NAME))?;
        lock(&_lock)?;
        let segments = open_segments(fs, dir)?;
        let mut index = Index::new();
        let mut bad = None;
        for (i, segment) in segments.iter().enumerate() {
            match scan(
                segment,
                &mut index,
                segment.base,
                segment.end(),
                &mut |_, _| {},
            ) {
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
        side.sync()?;
        remove_if_present(fs, &dir.join(hint_name(segment.base)))?;
        fs.sync_dir(dir)?;

        segment.set_len(offset)?;
        segment.sync()?;
        Ok(Some(Repaired {
            offset,
            dropped: len - offset,
            side_file,
        }))
    }

    fn segments(&self) -> Segments<F::File> {
        Arc::clone(&self.segments.read().unwrap_or_else(PoisonError::into_inner))
    }

    fn replace_segments(&self, _: &Writing, segments: Vec<Arc<Segment<F::File>>>) {
        *self
            .segments
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Arc::new(segments);
    }

    fn active(&self) -> Arc<Segment<F::File>> {
        Arc::clone(self.segments().last().expect("there is always a segment"))
    }

    /// Bytes no live key points at: overwritten values, deleted values,
    /// Delete records and Batch records.
    #[cfg(test)]
    pub(crate) fn dead(&self) -> u64 {
        self.segments().iter().map(|s| s.dead()).sum()
    }

    /// Sets each segment's dead bytes from the index: what no entry covers.
    fn count_dead(&self, _: &Writing) {
        let segments = self.segments();
        let mut live = vec![0u64; segments.len()];
        self.index.for_each(|entry| {
            live[segment_of(&segments, entry.offset)] += entry.record_len();
        });
        for (segment, live) in segments.iter().zip(live) {
            segment.set_dead(segment.len() - live);
        }
    }

    fn writer(&self) -> Writing<'_> {
        Writing {
            _guard: hold(&self.writing),
            poisoned: &self.poisoned,
        }
    }

    fn writable(&self) -> Result<(), Error> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(Error::Poisoned);
        }
        Ok(())
    }

    /// Turns writes off until the store is reopened, and returns `err`.
    fn poison(&self, err: impl Into<Error>) -> Error {
        let err = err.into();
        if !self.poisoned.swap(true, Ordering::AcqRel) {
            log::error!("writes are off until the store is reopened: {err}");
        }
        err
    }

    /// Rewrites the log with only the latest record of each live key, so
    /// overwritten values and Delete records stop taking space.
    ///
    /// Cleans every segment (DESIGN.md, Compaction): copies the live records
    /// into a new segment with a hint, fsyncs both, points the index at the
    /// copies, then removes the old segments oldest first, fsyncing the
    /// directory after each. Until then the old segments are untouched, so
    /// any failure while copying returns the error and leaves the store as
    /// it was. Reads each segment in one pass and checks every record it
    /// reads, so a corrupt record stops compaction with its offset rather
    /// than being copied. Reads go on while it runs; writes wait for it. A
    /// failed fsync of the log or the directory poisons the store.
    pub fn compact(&self) -> Result<(), Error> {
        let writing = self.writer();
        self.writable()?;
        let all: Vec<u64> = self
            .segments()
            .iter()
            .filter(|s| s.len() > 0)
            .map(|s| s.base)
            .collect();
        self.clean(&writing, &all)
    }

    /// Copies the live records of the segments based at `chosen` (in log
    /// order) to a fresh segment, then removes them. A Delete record is
    /// copied too while an older segment that is kept might hold a Set it
    /// hides.
    fn clean(&self, writing: &Writing, chosen: &[u64]) -> Result<(), Error> {
        if chosen.is_empty() {
            return Ok(());
        }
        if self.active().len() > 0 {
            self.roll(writing)?;
        }
        let segments = self.segments();
        let (inputs, output) = segments.split_at(segments.len() - 1);
        let output = &output[0];
        let oldest_kept = segments
            .iter()
            .filter(|s| !chosen.contains(&s.base))
            .map(|s| s.base)
            .min()
            .expect("the new segment is kept");
        let hint_path = self.dir.join(hint_name(output.base));
        let mut moved = Vec::with_capacity(self.index.len());
        let mut carry = Vec::new();
        let copied = self
            .fs
            .open_or_create(&hint_path)
            .map_err(Error::from)
            .and_then(|hint| {
                let written = self.copy_live(
                    inputs,
                    output,
                    &hint,
                    chosen,
                    oldest_kept,
                    &mut moved,
                    &mut carry,
                )?;
                Ok((hint, written))
            });
        let (hint_file, written) = match copied {
            Ok(copied) => copied,
            Err(err) => {
                self.undo_copy(output, &hint_path);
                return Err(err);
            }
        };
        if let Err(err) = output.file.sync() {
            self.undo_copy(output, &hint_path);
            return Err(self.poison(err));
        }
        if let Err(err) = hint_file.sync() {
            self.undo_copy(output, &hint_path);
            return Err(err.into());
        }
        output.set_len_written(written);
        if let Err(err) = self.repoint(output, &hint_file, &moved) {
            self.count_dead(writing);
            return Err(err);
        }
        let mut left_behind = 0usize;
        self.index.for_each(|entry| {
            if chosen.contains(&segments[segment_of(&segments, entry.offset)].base) {
                left_behind += 1;
            }
        });
        assert_eq!(left_behind, 0, "every live key in them is copied");
        for segment in inputs {
            if chosen.contains(&segment.base) {
                segment.set_dead(segment.len());
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
            let at = self.append_bytes(writing, &buffer)?;
            let segments = self.segments();
            segments[segment_of(&segments, at)].add_dead(buffer.len() as u64);
        }
        for &base in chosen {
            remove_if_present(&self.fs, &self.dir.join(hint_name(base)))?;
            self.fs.remove(&self.dir.join(log_name(base)))?;
            let kept = self
                .segments()
                .iter()
                .filter(|s| s.base != base)
                .cloned()
                .collect();
            self.replace_segments(writing, kept);
            if let Err(err) = self.fs.sync_dir(&self.dir) {
                return Err(self.poison(err));
            }
        }
        Ok(())
    }

    /// Throws away a compaction's copy: removes its hint, then empties
    /// `output`, each whether or not the other worked. Poisons the store if
    /// either fails, since the next write would land after the copy's
    /// bytes. The hint goes first: it must never vouch for bytes that may
    /// not be on disk.
    fn undo_copy(&self, output: &Segment<F::File>, hint_path: &Path) {
        let hint_gone = remove_if_present(&self.fs, hint_path).and_then(|removed| {
            if removed {
                self.fs.sync_dir(&self.dir)?;
            }
            Ok(())
        });
        let emptied = output.file.set_len(0).and_then(|()| output.file.sync());
        if let Err(err) = hint_gone.and(emptied) {
            self.poison(err);
        }
    }

    /// Writes each live record of the `chosen` segments among `inputs` to
    /// the empty segment `output`, and its key and lengths to `hint_file`,
    /// pushing its old offset to `moved`, in the same order. Pushes to
    /// `carry` the key of each Delete record that must outlive its segment:
    /// its key isn't live, and a segment older than it is kept. Returns the
    /// bytes written; fsyncs neither file. One buffered pass per segment:
    /// reading the dead records in between costs less than a positioned
    /// read per live one.
    #[allow(clippy::too_many_arguments)]
    fn copy_live(
        &self,
        inputs: &[Arc<Segment<F::File>>],
        output: &Segment<F::File>,
        hint_file: &F::File,
        chosen: &[u64],
        oldest_kept: u64,
        moved: &mut Vec<u64>,
        carry: &mut Vec<Vec<u8>>,
    ) -> Result<u64, Error> {
        hint_file.set_len(0)?;
        let mut hint = hint::Writer::new(hint_file);
        let mut carried = HashSet::new();
        let mut header_buf = [0u8; HEADER_LEN];
        let mut payload_buf = Vec::new();
        let mut buffer = Vec::new();
        let mut written = 0u64;
        let mut expected = 0usize;
        for input in inputs.iter().filter(|s| chosen.contains(&s.base)) {
            let mut reader = BufReader::with_capacity(
                hint::buffer_for(input.len(), COMPACT_BUFFER),
                input.reader_at(input.base),
            );
            let mut offset = input.base;
            let end = input.end();
            while offset < end {
                let header_bytes = read_header(&mut reader, &mut header_buf, end - offset)?.ok_or(
                    Error::Corruption {
                        offset,
                        reason: CorruptionReason::CutShort,
                    },
                )?;
                let header = decode_header(header_bytes).map_err(|err| err.at(offset))?;
                payload_buf.resize(header.payload_len(), 0);
                reader.read_exact(&mut payload_buf)?;
                let op = decode_index_op(header, &payload_buf).map_err(|err| err.at(offset))?;
                match op {
                    IndexOp::Set { key, .. } => {
                        if self
                            .index
                            .get(key.as_bytes())
                            .is_some_and(|entry| entry.offset == offset)
                        {
                            moved.push(offset);
                            let fields = Fields {
                                format: Format::Crc64,
                                ..header.fields()
                            };
                            hint.push(fields, key.as_bytes())?;
                            match header.format() {
                                Format::Crc64 => {
                                    buffer.extend_from_slice(header_bytes);
                                    buffer.extend_from_slice(&payload_buf);
                                }
                                Format::Crc32c => {
                                    let (key, value) = payload_buf.split_at(key.as_bytes().len());
                                    encode_set(key, value, &mut buffer);
                                }
                            }
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
        output.file.write_all_at(&buffer, written)?;
        written += buffer.len() as u64;
        hint.finish(written)?;
        Ok(written)
    }

    /// Points each key `copy_live` copied at its copy in `output`, as the
    /// output's durable hint lists them, if the key still points at the old
    /// record `moved` holds the offset of. A copy left unpointed is dead.
    fn repoint(
        &self,
        output: &Segment<F::File>,
        hint_file: &F::File,
        moved: &[u64],
    ) -> Result<(), Error> {
        let unreadable = |hint::Invalid(why)| {
            Error::Io(io::Error::other(format!(
                "rereading a compaction's hint: {why}"
            )))
        };
        let trailer = hint::trailer(hint_file).map_err(unreadable)?;
        let mut old = moved.iter();
        hint::read_entries(hint_file, trailer, HINT_WINDOW, |window| {
            for (fields, key, offset) in window.entries() {
                let from = *old.next().expect("one old offset per copy");
                let entry = Entry {
                    offset: output.base + offset,
                    value_len: fields.value_len,
                    key_len: fields.key_len,
                    format: fields.format,
                };
                if !self.index.replace(key, from, entry) {
                    output.add_dead(entry.record_len());
                }
            }
        })
        .map_err(unreadable)?;
        assert!(old.next().is_none(), "the hint lists every copy");
        Ok(())
    }

    /// Compacts if automatic compaction is on and due, cleaning the
    /// segments with the most dead bytes until it no longer would be. The
    /// caller's write has already succeeded, so a failure is logged, not
    /// returned.
    fn compact_if_due(&self, writing: &Writing) {
        let Some(auto) = self.auto_compact else {
            return;
        };
        if self.auto_failed.load(Ordering::Relaxed) || self.writable().is_err() {
            return;
        }
        let sizes: Vec<SegmentSize> = self
            .segments()
            .iter()
            .map(|s| SegmentSize {
                base: s.base,
                len: s.len(),
                dead: s.dead(),
            })
            .collect();
        let chosen = deadest(&sizes, auto);
        if let Err(err) = self.clean(writing, &chosen) {
            self.auto_failed.store(true, Ordering::Relaxed);
            log::warn!("automatic compaction failed, off until the next open: {err}");
        }
    }

    /// Starts a new, empty segment at the end of the log, and makes its
    /// name durable before anything is written to it.
    fn roll(&self, writing: &Writing) -> Result<(), Error> {
        let segments = self.segments();
        let active = segments.last().expect("there is always a segment");
        let base = active.end();
        let file = self.fs.open_or_create(&self.dir.join(log_name(base)))?;
        self.fs.sync_dir(&self.dir)?;
        active.seal();
        let mut next = segments.to_vec();
        next.push(Arc::new(Segment::new(base, file, 0)));
        self.replace_segments(writing, next);
        Ok(())
    }

    /// Writes `buffer` at the end of the log in one write and fsyncs it,
    /// first starting a new segment if the active one is full, so a write
    /// never spans two segments. Returns where it landed. A failed write is
    /// truncated away and the truncation fsynced; if that fails, or the
    /// write's fsync does, the store is poisoned.
    fn append_bytes(&self, writing: &Writing, buffer: &[u8]) -> Result<u64, Error> {
        let full = self.active().len();
        if full > 0 && full >= self.segment_bytes {
            self.roll(writing)?;
        }
        let active = self.active();
        let offset = active.end();
        if let Err(err) = active.write_all_at(buffer, offset) {
            if let Err(truncate) = active.set_len(offset).and_then(|()| active.sync()) {
                self.poison(truncate);
            }
            return Err(err.into());
        }
        let started = Instant::now();
        if let Err(err) = active.file.sync() {
            return Err(self.poison(err));
        }
        let took = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.last_sync_nanos.store(took, Ordering::Relaxed);
        #[cfg(test)]
        self.commits.fetch_add(1, Ordering::Relaxed);
        active.set_len_written(active.len() + buffer.len() as u64);
        Ok(offset)
    }

    /// Appends a Set record, fsyncs, then points the index at it. Then
    /// compacts if automatic compaction is due, as `open_with` describes.
    ///
    /// Writes from several threads share an fsync (DESIGN.md, Group
    /// commit): a write waits for the group it lands in, and a failure
    /// fails every write in that group with the same error. A write that
    /// fails partway is truncated away. A failed fsync, or a failed truncate
    /// of a failed write, poisons the store: every later write fails with
    /// `Poisoned` until it is reopened, and reads still work.
    pub fn set(&self, key: Key, value: Value) -> Result<(), Error> {
        self.submit(Write::One(Record::Set { key, value }))
    }

    /// Applies every operation in `batch` with one append and one fsync,
    /// all or nothing: after a crash, `open` finds the whole batch or none
    /// of it (DESIGN.md, Batches). Operations apply in order, so a delete
    /// sees the batch's own earlier sets. A delete of a key that isn't live
    /// at that point writes nothing, as `delete` does, and a batch that
    /// writes nothing is a no-op. Then compacts if due, as `set` does.
    /// Failed writes are handled as in `set`.
    pub fn write(&self, batch: Batch) -> Result<(), Error> {
        if batch.is_empty() {
            return Ok(());
        }
        self.submit(Write::Batch(batch.ops))
    }

    /// Appends a Delete record, fsyncs, then removes the key from the index.
    /// Then compacts if automatic compaction is due, as in `set`.
    ///
    /// Deleting a key that isn't live is a no-op: nothing is written and it
    /// returns `Ok`. Failed writes are handled as in `set`.
    pub fn delete(&self, key: Key) -> Result<(), Error> {
        if !self.index.contains(key.as_bytes()) {
            return Ok(());
        }
        self.submit(Write::One(Record::Delete { key }))
    }

    /// Queues `write` and waits until it is done. If no group is being
    /// written, this thread leads: it takes the oldest writes waiting,
    /// writes them with one append and one fsync, and wakes their threads,
    /// then leads again if its own write was not among them.
    fn submit(&self, write: Write) -> Result<(), Error> {
        self.writable()?;
        let mut queue = hold(&self.queue);
        let ticket = queue.next;
        queue.next += 1;
        queue.waiting.push_back((ticket, write));
        self.arrived.notify_one();
        loop {
            if let Some(result) = queue.done.remove(&ticket) {
                return result;
            }
            if queue.leading {
                queue = self
                    .committed
                    .wait(queue)
                    .unwrap_or_else(PoisonError::into_inner);
                continue;
            }
            queue.leading = true;
            queue = self.fill(queue);
            let group = queue.take_group();
            drop(queue);
            let leader = Leader {
                store: self,
                tickets: group.iter().map(|&(ticket, _)| ticket).collect(),
                results: None,
            };
            let writes: Vec<Write> = group.into_iter().map(|(_, write)| write).collect();
            self.commit_then(&writes, move |results| leader.finish(results));
            queue = hold(&self.queue);
        }
    }

    /// Waits, as the leader, for the queue to hold as many writes as the
    /// last group took, or for a share of the last fsync's time, whichever
    /// comes first. Waits not at all if the last group was one write: a
    /// writer alone is written at once.
    fn fill<'q>(&self, mut queue: MutexGuard<'q, Queue>) -> MutexGuard<'q, Queue> {
        let want = queue.last_group;
        if want <= 1 || queue.waiting.len() >= want {
            return queue;
        }
        let wait = self.last_sync_nanos.load(Ordering::Relaxed) / 100 * GROUP_WAIT_PERCENT;
        let deadline = Instant::now() + Duration::from_nanos(wait);
        while queue.waiting.len() < want {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            queue = self
                .arrived
                .wait_timeout(queue, deadline - now)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        queue
    }

    /// Writes `writes` as one group and returns each one's result, in order,
    /// then compacts if that is due.
    #[cfg(test)]
    pub(crate) fn commit(&self, writes: &[Write]) -> Vec<Result<(), Error>> {
        let mut out = None;
        self.commit_then(writes, |results| out = Some(results));
        out.expect("commit_then hands the results over")
    }

    /// Writes `writes` as one group, hands `done` each one's result in
    /// order, then compacts if the group wrote something and that is due.
    /// The results go out first: the writes are durable, and neither a slow
    /// nor a failed compaction changes that.
    fn commit_then(&self, writes: &[Write], done: impl FnOnce(Vec<Result<(), Error>>)) {
        let writing = self.writer();
        let (results, wrote) = self.write_group(&writing, writes);
        done(results);
        if wrote {
            self.compact_if_due(&writing);
        }
    }

    /// Writes a group with one append and one fsync, then updates the index.
    /// Returns each write's result in order, and whether it wrote anything.
    fn write_group(&self, writing: &Writing, writes: &[Write]) -> (Vec<Result<(), Error>>, bool) {
        if let Err(err) = self.writable() {
            return (each(err, writes.len()), false);
        }
        let mut live = HashMap::new();
        let mut buffer = Vec::new();
        let mut ops: Vec<(u64, &Record)> = Vec::new();
        let mut batch_headers = 0u64;
        for write in writes {
            match write {
                Write::One(op) => {
                    if takes_effect(&self.index, op, &mut live) {
                        ops.push((buffer.len() as u64, op));
                        encode(op, &mut buffer);
                    }
                }
                Write::Batch(batch) => {
                    let mut body = Vec::new();
                    let mut in_body = Vec::with_capacity(batch.len());
                    for op in batch {
                        if takes_effect(&self.index, op, &mut live) {
                            in_body.push((body.len() as u64, op));
                            encode(op, &mut body);
                        }
                    }
                    if in_body.is_empty() {
                        continue;
                    }
                    let start = buffer.len() as u64;
                    encode(
                        &Record::Batch {
                            body_len: body.len() as u64,
                        },
                        &mut buffer,
                    );
                    let header_len = buffer.len() as u64 - start;
                    batch_headers += header_len;
                    ops.extend(
                        in_body
                            .into_iter()
                            .map(|(at, op)| (start + header_len + at, op)),
                    );
                    buffer.extend_from_slice(&body);
                }
            }
        }
        let wrote = !ops.is_empty();
        if wrote {
            let start = match self.append_bytes(writing, &buffer) {
                Ok(start) => start,
                Err(err) => return (each(err, writes.len()), false),
            };
            let segments = self.segments();
            segments[segment_of(&segments, start)].add_dead(batch_headers);
            for (at, op) in ops {
                self.indexed(&segments, op, start + at);
            }
        }
        (writes.iter().map(|_| Ok(())).collect(), wrote)
    }

    /// Updates the index and the dead bytes for a Set or Delete record now
    /// durable at `offset`. A Delete's key must be live. Only the writer
    /// calls it.
    fn indexed(&self, segments: &[Arc<Segment<F::File>>], record: &Record, offset: u64) {
        let died = |at: u64, len: u64| segments[segment_of(segments, at)].add_dead(len);
        match record {
            Record::Set { key, value } => {
                let old = self.index.insert(
                    key.as_bytes(),
                    Entry {
                        offset,
                        value_len: u32::try_from(value.as_bytes().len())
                            .expect("Value guarantees len fits u32"),
                        key_len: entry_key_len(key),
                        format: Format::Crc64,
                    },
                );
                if let Some(old) = old {
                    died(old.offset, old.record_len());
                }
            }
            Record::Delete { key } => {
                let key_len = key.as_bytes().len();
                let old = self
                    .index
                    .remove(key.as_bytes())
                    .expect("deleted keys are live");
                died(old.offset, old.record_len());
                died(offset, (HEADER_LEN + key_len) as u64);
            }
            Record::Batch { .. } => unreachable!("not an index operation"),
        }
    }

    /// Returns the value for `key`, or `None` if it isn't live.
    ///
    /// Reads the record with two positioned reads (header, then payload) and
    /// verifies both checksums and that the stored key equals `key`. Any failure is a
    /// corruption error at the record's offset, never `None`. Takes no lock
    /// while it reads, so it runs alongside writes and compaction.
    pub fn get(&self, key: &Key) -> Result<Option<Value>, Error> {
        let Some((entry, segment)) = self
            .index
            .get(key.as_bytes())
            .and_then(|entry| self.locate(key.as_bytes(), entry))
        else {
            return Ok(None);
        };
        match load_indexed(&*segment, &entry)? {
            Record::Set {
                key: stored_key,
                value,
            } if *key == stored_key => Ok(Some(value)),
            Record::Set { .. } => Err(Error::Corruption {
                offset: entry.offset,
                reason: CorruptionReason::KeyMismatch,
            }),
            _ => Err(Error::Corruption {
                offset: entry.offset,
                reason: CorruptionReason::NotASet,
            }),
        }
    }

    /// The segment holding the record `entry`, read from the index for
    /// `key`, points at. If a compaction moved the key and removed that
    /// segment since, the entry the index has now and its segment, or
    /// `None` if the key is no longer live.
    fn locate(&self, key: &[u8], mut entry: Entry) -> Option<(Entry, Arc<Segment<F::File>>)> {
        loop {
            let segments = self.segments();
            if let Some(segment) = holding(&segments, entry.offset) {
                return Some((entry, Arc::clone(segment)));
            }
            let moved = self.index.get(key)?;
            assert_ne!(moved, entry, "an index entry points outside the log");
            entry = moved;
        }
    }

    /// Reads every record in the log, checking both checksums of each, and
    /// checks that they describe exactly the index in memory. `open` skips
    /// the checksums of records a hint covers (DESIGN.md, Hint file); this
    /// is the full check, on demand. Reads the whole log and holds two
    /// sorted lists of 16 bytes per live key while it compares them. Writes
    /// wait for it.
    ///
    /// Fails with the first bad record's error, or `IndexMismatch` at the
    /// first offset where the log and the index disagree.
    pub fn verify(&self) -> Result<(), Error> {
        let _writing = self.writer();
        let mismatch = |offset| Error::Corruption {
            offset,
            reason: CorruptionReason::IndexMismatch,
        };
        let mut scanned = Index::new();
        for segment in self.segments().iter() {
            let len = segment.size()?;
            if len != segment.end() {
                return Err(mismatch(len.min(segment.end())));
            }
            let end = scan(
                &**segment,
                &mut scanned,
                segment.base,
                segment.end(),
                &mut |_, _| {},
            )?;
            if end != segment.end() {
                return Err(mismatch(end));
            }
        }
        let mut ours = Vec::with_capacity(self.index.len());
        self.index.for_each(|entry| ours.push(*entry));
        let mut theirs = Vec::with_capacity(scanned.len());
        scanned.for_each(|entry| theirs.push(*entry));
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
}

/// The leader of a group of writes. Hands each write its result when
/// dropped, and the next waiting thread the lead. If the leader panicked
/// before it had the results, the store is poisoned and every write in the
/// group fails with `Poisoned`.
struct Leader<'a, F: Fs> {
    store: &'a Store<F>,
    tickets: Vec<u64>,
    results: Option<Vec<Result<(), Error>>>,
}

impl<F: Fs> Leader<'_, F> {
    fn finish(mut self, results: Vec<Result<(), Error>>) {
        self.results = Some(results);
    }
}

impl<F: Fs> Drop for Leader<'_, F> {
    fn drop(&mut self) {
        let results = self.results.take().unwrap_or_else(|| {
            self.store.poisoned.store(true, Ordering::Release);
            each(Error::Poisoned, self.tickets.len())
        });
        let mut queue = hold(&self.store.queue);
        queue.leading = false;
        queue.done.extend(self.tickets.drain(..).zip(results));
        drop(queue);
        self.store.committed.notify_all();
    }
}

impl Queue {
    /// The oldest waiting writes, up to a group's limits, and at least one.
    fn take_group(&mut self) -> Vec<(u64, Write)> {
        let mut group = Vec::new();
        let mut bytes = 0;
        while let Some((_, write)) = self.waiting.front() {
            let size = write.size();
            if !group.is_empty()
                && (group.len() == GROUP_MOST_WRITES || bytes + size > GROUP_MOST_BYTES)
            {
                break;
            }
            bytes += size;
            group.push(self.waiting.pop_front().expect("one is waiting"));
        }
        self.last_group = group.len();
        group
    }
}

/// `err` for each of `n` writes. `Error` isn't `Clone`: the first write
/// gets `err` itself, and the rest an `Io` error with its kind and message.
fn each(err: Error, n: usize) -> Vec<Result<(), Error>> {
    let copy = |err: &Error| match err {
        Error::Poisoned => Error::Poisoned,
        Error::Io(io) => Error::Io(io::Error::new(io.kind(), io.to_string())),
        other => Error::Io(io::Error::other(other.to_string())),
    };
    let mut results: Vec<Result<(), Error>> = (1..n).map(|_| Err(copy(&err))).collect();
    results.insert(0, Err(err));
    results
}

/// Whether the Set or Delete `op` writes anything, given the keys a group
/// has already set (`true`) or deleted (`false`) in `live`, which it then
/// updates: a delete of a key that isn't live writes nothing.
fn takes_effect<'w>(index: &Index, op: &'w Record, live: &mut HashMap<&'w [u8], bool>) -> bool {
    let (Record::Set { key, .. } | Record::Delete { key }) = op else {
        unreachable!("only sets and deletes are written");
    };
    let is_set = matches!(op, Record::Set { .. });
    let was_live = live
        .get(key.as_bytes())
        .copied()
        .unwrap_or_else(|| index.contains(key.as_bytes()));
    if !is_set && !was_live {
        return false;
    }
    live.insert(key.as_bytes(), is_set);
    true
}

/// The index in `segments` of the segment starting at or before `offset`.
fn segment_of<L>(segments: &[Arc<Segment<L>>], offset: u64) -> usize {
    segments.partition_point(|s| s.base <= offset) - 1
}

/// The segment in `segments` holding the byte at `offset`, if one does.
fn holding<L>(segments: &[Arc<Segment<L>>], offset: u64) -> Option<&Arc<Segment<L>>> {
    let after = segments.partition_point(|s| s.base <= offset);
    segments[..after].last().filter(|s| s.holds(offset))
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

/// The shortest record there can be: a Set of one-byte key and value in
/// the old format.
const SHORTEST_RECORD: u64 = SHORTEST_HEADER_LEN as u64 + 2;

/// Reads a record's header into `buf` and returns its bytes: the shortest
/// header, then the rest if its type byte names a longer one. `None` if
/// that is more than the `remaining` bytes, which must be at least the
/// shortest header.
///
/// Copies a header the buffer already holds instead: two short
/// `read_exact`s took a scan of 100k records from 12.8 to 14.4 ms.
fn read_header<'a, R: Read>(
    reader: &mut BufReader<R>,
    buf: &'a mut [u8; HEADER_LEN],
    remaining: u64,
) -> io::Result<Option<&'a [u8]>> {
    let buffered = reader.buffer();
    if buffered.len() >= HEADER_LEN {
        let len = Format::of(buffered[4]).header_len();
        if remaining < len as u64 {
            return Ok(None);
        }
        buf[..len].copy_from_slice(&buffered[..len]);
        reader.consume(len);
        return Ok(Some(&buf[..len]));
    }
    reader.read_exact(&mut buf[..SHORTEST_HEADER_LEN])?;
    let len = Format::of(buf[4]).header_len();
    if remaining < len as u64 {
        return Ok(None);
    }
    reader.read_exact(&mut buf[SHORTEST_HEADER_LEN..len])?;
    Ok(Some(&buf[..len]))
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
    let len = entry.record_len();
    let mut buf = vec![0u8; usize::try_from(len).expect("a record fits in memory")];
    match file.read_exact_at(&mut buf, entry.offset) {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
            return load_record(file, entry.offset).map(|(_, record)| record);
        }
        Err(err) => return Err(err.into()),
    }
    if Format::of(buf[4]) != entry.format {
        return load_record(file, entry.offset).map(|(_, record)| record);
    }
    let header_len = entry.format.header_len();
    let header = decode_header(&buf[..header_len]).map_err(|err| err.at(entry.offset))?;
    if header.record_len() != len {
        return load_record(file, entry.offset).map(|(_, record)| record);
    }
    decode_payload(header, &buf[header_len..]).map_err(|err| err.at(entry.offset))
}

fn load_record(file: &impl LogFile, offset: u64) -> Result<(Header, Record), Error> {
    let mut header_buf = [0u8; HEADER_LEN];
    let mut reader = BufReader::with_capacity(HEADER_LEN, file.reader_at(offset));
    let header_bytes = read_header(&mut reader, &mut header_buf, u64::MAX)?
        .expect("no limit on the bytes it may read");
    let header = decode_header(header_bytes).map_err(|err| err.at(offset))?;
    let mut payload_buf = vec![0u8; header.payload_len()];
    let header_boundary = header_bytes.len() as u64 + offset;
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
    let mut names = fs.list(dir)?;
    let mut changed = migrate(fs, dir, &names)?;
    if changed {
        names = fs.list(dir)?;
    }
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
        segments.push(Segment::new(base, file, len));
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
fn migrate<F: Fs>(fs: &F, dir: &Path, names: &[String]) -> Result<bool, Error> {
    let has = |name: &str| names.iter().any(|n| n == name);
    let legacy = dir.join(LOG_FILE_NAME);
    let first = dir.join(log_name(0));
    if has(LOG_FILE_NAME) {
        let file = fs.open_existing(&legacy)?;
        let size = file.size()?;
        if size <= SHORTEST_HEADER_LEN as u64 {
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
    // The largest hint's Set count, not their sum: a key overwritten in
    // every segment is counted in each.
    let mut count = 0u64;
    for segment in segments.iter() {
        let hint = match fs.open_existing(&dir.join(hint_name(segment.base))) {
            Ok(file) => match hint::trailer(&file) {
                Ok(trailer) => {
                    count = count.max(trailer.count);
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
    let total: u64 = segments.iter().map(|s| s.len()).sum();
    let capacity = usize::try_from(count.min(total / SHORTEST_RECORD)).unwrap_or(usize::MAX);
    let mut index = Index::with_capacity(capacity);
    let newest = segments.len() - 1;
    for (i, (segment, hint)) in segments.iter_mut().zip(hints).enumerate() {
        let (hinted, tail) = match &hint {
            Some(file) => match read_hint(file, segment, &mut index, HINT_WINDOW, PARALLEL_FROM) {
                Ok((hinted, tail)) => (hinted, Some(tail)),
                Err(invalid) => return Ok(Err((segment.base, invalid))),
            },
            None => (segment.base, None),
        };
        let len = segment.end();
        // A sealed segment never changes, so a hint for all of it stays
        // valid. Not fsynced: a hint that didn't land whole fails its
        // checksum, and the next open removes it and reads the segment.
        let mut created = None;
        let mut writer = None;
        if i < newest && hinted < len {
            let opened = match (&hint, tail) {
                (Some(file), Some(tail)) => hint::Writer::extend(file, tail),
                _ => fs
                    .open_or_create(&dir.join(hint_name(segment.base)))
                    .map(|file| hint::Writer::new(created.insert(file))),
            };
            match opened {
                Ok(opened) => writer = Some(opened),
                Err(err) => hint_not_written(segment.base, &err),
            }
        }
        let end = scan(&*segment, &mut index, hinted, len, &mut |fields, key| {
            if let Some(hint) = &mut writer
                && let Err(err) = hint.push(fields, key)
            {
                hint_not_written(segment.base, &err);
                writer = None;
            }
        })?;
        if let Some(hint) = writer.take()
            && end == len
            && let Err(err) = hint.finish(len - segment.base)
        {
            hint_not_written(segment.base, &err);
        }
        if end < len {
            if i < newest {
                return Err(Error::Corruption {
                    offset: end,
                    reason: CorruptionReason::CutShort,
                });
            }
            segment.set_len(end)?;
            segment.file.sync()?;
            segment.set_len_written(end - segment.base);
            log::warn!(
                "truncated a torn tail at offset {end}: dropped {} bytes",
                len - end
            );
        }
    }
    Ok(Ok(index))
}

fn hint_not_written(base: u64, err: &io::Error) {
    log::warn!(
        "could not write the hint for the segment at {base} ({err}); the next open reads it"
    );
}

/// Hints with fewer Set entries than this are read on one thread.
const PARALLEL_FROM: u64 = 100_000;
const MAX_THREADS: usize = 8;
const HINT_WINDOW: usize = 2 << 20;

/// Applies the records `segment`'s hint `file` describes to `index`, in
/// order, a window of `window_bytes` at a time: on one thread below
/// `parallel_from` Set entries, on up to `MAX_THREADS` from there. Returns
/// the log offset the hint covers to, and where to extend it from. On
/// `Err`, what it applied must be thrown away.
fn read_hint<L: LogFile>(
    file: &impl LogFile,
    segment: &Segment<L>,
    index: &mut Index,
    window_bytes: usize,
    parallel_from: u64,
) -> Result<(u64, hint::Tail), hint::Invalid> {
    let trailer = hint::trailer(file)?;
    if trailer.log_len > segment.len() {
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
    let tail = hint::read_entries(file, trailer, window_bytes, |window| {
        let changes: Vec<(&[u8], Option<Entry>)> = window
            .entries()
            .filter_map(|(fields, key, offset)| match fields.kind {
                Kind::Set => Some((
                    key,
                    Some(Entry {
                        offset: base + offset,
                        value_len: fields.value_len,
                        key_len: fields.key_len,
                        format: fields.format,
                    }),
                )),
                Kind::Delete => Some((key, None)),
                Kind::Batch => None,
            })
            .collect();
        index.apply_in_order(threads, &changes);
    })?;
    Ok((base + trailer.log_len, tail))
}

/// Scans of fewer bytes than this apply each record as it is read, on one
/// thread.
const PARALLEL_SCAN_FROM: u64 = 16 << 20;
/// Records a parallel scan reads before applying them.
const SCAN_WINDOW: usize = 1 << 16;
const SCAN_BUFFER: usize = 1 << 20;

/// Applies the records from `start` to `len` to `index`. Returns where the
/// valid records end: `len`, or the start of a torn or zero-filled tail. A
/// batch is applied only once all of it has been read and checked; a torn
/// one is a torn tail from its start, and an error inside one is reported
/// at its start, so `repair` moves the whole batch. From
/// `PARALLEL_SCAN_FROM` bytes, the index takes the records a window at a
/// time on up to `MAX_THREADS` threads. Calls `record` with every record
/// read, in log order, Batch records too, including any that a torn batch
/// then leaves out.
fn scan(
    file: &impl LogFile,
    index: &mut Index,
    start: u64,
    len: u64,
    record: &mut impl FnMut(Fields, &[u8]),
) -> Result<u64, Error> {
    let threads = if len - start < PARALLEL_SCAN_FROM {
        1
    } else {
        thread::available_parallelism()
            .map_or(1, NonZero::get)
            .min(MAX_THREADS)
    };
    scan_on(file, index, start, len, threads, record)
}

/// On more than one thread, this one reads while another applies what it
/// read, a window at a time, on `threads` more.
fn scan_on(
    file: &impl LogFile,
    index: &mut Index,
    start: u64,
    len: u64,
    threads: usize,
    record: &mut impl FnMut(Fields, &[u8]),
) -> Result<u64, Error> {
    if threads == 1 {
        return scan_into(
            file,
            start,
            len,
            &mut |offset, op| apply(index, offset, op),
            record,
        );
    }
    let (send, receive) = mpsc::sync_channel::<Window>(1);
    thread::scope(|scope| {
        scope.spawn(move || {
            for window in receive {
                window.apply_to(index, threads);
            }
        });
        let mut window = Window::default();
        let sent = |window| {
            send.send(window)
                .expect("the applier runs until the sender is dropped")
        };
        let end = scan_into(
            file,
            start,
            len,
            &mut |offset, op| {
                window.push(offset, op);
                if window.changes.len() == SCAN_WINDOW {
                    sent(mem::take(&mut window));
                }
            },
            record,
        );
        if end.is_ok() {
            sent(window);
        }
        // The applier stops once the channel closes, and the scope waits
        // for it.
        drop(send);
        end
    })
}

/// Calls `sink` with each record from `start` to `len` that is to be
/// applied, in order, with its offset, and `record` with every record read.
fn scan_into(
    file: &impl LogFile,
    start: u64,
    len: u64,
    sink: &mut impl FnMut(u64, IndexOp),
    record: &mut impl FnMut(Fields, &[u8]),
) -> Result<u64, Error> {
    let mut reader = Reader {
        inner: BufReader::with_capacity(
            hint::buffer_for(len - start, SCAN_BUFFER),
            file.reader_at(start),
        ),
        header_buf: [0u8; HEADER_LEN],
        payload_buf: Vec::new(),
    };
    let mut batch = Vec::new();
    let mut offset = start;
    while offset < len {
        let Next::Op(header, op) = reader.next(offset, len)? else {
            return Ok(offset);
        };
        record(header.fields(), key_of(&op));
        let record_len = header.record_len();
        let IndexOp::Batch { body_len } = op else {
            sink(offset, op);
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
            let Next::Op(header, op) = next else {
                return Ok(offset);
            };
            record(header.fields(), key_of(&op));
            let len_here = header.record_len();
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
            sink(at, op);
        }
        offset = body_end;
    }
    Ok(offset)
}

/// Records a scan has read and not yet applied, with their keys' bytes end
/// to end in `keys`, and where each key ends.
#[derive(Default)]
struct Window {
    keys: Vec<u8>,
    changes: Vec<(usize, Option<Entry>)>,
}

impl Window {
    fn push(&mut self, offset: u64, op: IndexOp) {
        let (key, entry) = match op {
            IndexOp::Set {
                key,
                value_len,
                format,
            } => {
                let key_len = entry_key_len(&key);
                let entry = Entry {
                    offset,
                    value_len,
                    key_len,
                    format,
                };
                (key, Some(entry))
            }
            IndexOp::Delete { key } => (key, None),
            IndexOp::Batch { .. } => unreachable!("scan handles batches"),
        };
        self.keys.extend_from_slice(key.as_bytes());
        self.changes.push((self.keys.len(), entry));
    }

    fn apply_to(self, index: &mut Index, threads: usize) {
        let mut start = 0;
        let changes: Vec<(&[u8], Option<Entry>)> = self
            .changes
            .iter()
            .map(|&(end, entry)| {
                let key = &self.keys[start..end];
                start = end;
                (key, entry)
            })
            .collect();
        index.apply_in_order(threads, &changes);
    }
}

fn apply(index: &mut Index, offset: u64, op: IndexOp) {
    match op {
        IndexOp::Set {
            key,
            value_len,
            format,
        } => {
            let key_len = entry_key_len(&key);
            index.insert(
                key.as_bytes(),
                Entry {
                    offset,
                    value_len,
                    key_len,
                    format,
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
    /// A valid record.
    Op(Header, IndexOp),
    /// The log from here to `len` is a torn or zero-filled tail.
    Torn,
}

impl<R: Read> Reader<R> {
    /// Reads the record at `offset`, which must be where the reader is.
    fn next(&mut self, offset: u64, len: u64) -> Result<Next, Error> {
        if len - offset < SHORTEST_HEADER_LEN as u64 {
            return Ok(Next::Torn);
        }
        let Some(header_bytes) = read_header(&mut self.inner, &mut self.header_buf, len - offset)?
        else {
            return Ok(Next::Torn);
        };
        let header = match decode_header(header_bytes) {
            Ok(header) => header,
            Err(DecodeError::Corrupt(CorruptionReason::HeaderChecksum))
                if all_zero(header_bytes) && rest_is_zero(&mut self.inner)? =>
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
            Ok(op) => Ok(Next::Op(header, op)),
            Err(DecodeError::Corrupt(CorruptionReason::PayloadChecksum))
                if all_zero(&self.payload_buf) && rest_is_zero(&mut self.inner)? =>
            {
                Ok(Next::Torn)
            }
            Err(err) => Err(err.at(offset)),
        }
    }
}

fn key_of(op: &IndexOp) -> &[u8] {
    match op {
        IndexOp::Set { key, .. } | IndexOp::Delete { key } => key.as_bytes(),
        IndexOp::Batch { .. } => &[],
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
#[cfg(test)]
mod tests {
    use std::fs::{self, OpenOptions};
    use std::os::unix::fs::FileExt;
    use std::path::PathBuf;

    use super::*;
    use crate::mem_fs::MemFs;
    use crate::record::tests::{DELETE_A, OLD_DELETE_A, OLD_SET_A_HI, SET_A_HI};
    use crate::record::{Record, encode, encode_as};
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
        bytes[20] ^= 0x01; // first record's value: "hi" -> "ii"
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
    fn open_truncates_a_header_torn_past_the_shortest_header() {
        for cut in 15..19 {
            let tmp = store_with_log(&set_a_hi_then(&DELETE_A[..cut]));
            let store = Store::open(tmp.path()).unwrap();
            assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
            assert_eq!(log_bytes(tmp.path()), SET_A_HI, "cut at {cut}");
        }
    }

    #[test]
    fn open_truncates_a_torn_payload() {
        let tmp = store_with_log(&set_a_hi_then(&DELETE_A[..19]));
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        assert_eq!(log_bytes(tmp.path()), SET_A_HI);
    }

    #[test]
    fn a_log_written_before_64_bit_checksums_recovers_reads_and_compacts() {
        let mut old = Vec::new();
        encode_as(Format::Crc32c, &set_record(b"a", b"hi"), &mut old);
        encode_as(Format::Crc32c, &set_record(b"z", b"9"), &mut old);
        assert_eq!(old[..18], OLD_SET_A_HI);
        let tmp = store_with_log(&[&old[..], &OLD_DELETE_A[..14]].concat());
        let store = Store::open_with(tmp.path(), segmented(1 << 20)).unwrap();
        assert_eq!(log_bytes(tmp.path()), old, "the torn tail is cut");
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        store.set(key(b"b"), value(b"2")).unwrap();
        drop(store);

        let store = Store::open_with(tmp.path(), segmented(1 << 20)).unwrap();
        let format = |store: &Store, k: &[u8]| store.index.get(k).unwrap().format;
        assert_eq!(format(&store, b"a"), Format::Crc32c);
        assert_eq!(format(&store, b"z"), Format::Crc32c);
        assert_eq!(format(&store, b"b"), Format::Crc64);
        assert_eq!(store.dead(), 0);
        store.set(key(b"z"), value(b"0")).unwrap();
        assert_eq!(store.dead(), 17, "the old Set of z");

        store.compact().unwrap();
        assert_eq!(
            log_bytes(tmp.path()),
            encoded(&[
                set_record(b"a", b"hi"),
                set_record(b"b", b"2"),
                set_record(b"z", b"0"),
            ]),
            "a is rewritten in the new format"
        );
        assert_eq!(format(&store, b"a"), Format::Crc64);
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        drop(store);

        let store = Store::open_with(tmp.path(), segmented(1 << 20)).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
        assert_eq!(store.get(&key(b"z")).unwrap(), Some(value(b"0")));
        store.verify().unwrap();
    }

    #[test]
    fn damage_in_a_record_written_before_64_bit_checksums_is_still_caught() {
        let mut bytes = [&OLD_SET_A_HI[..], &OLD_DELETE_A].concat();
        bytes[16] ^= 0x01;
        assert_open_fails_untouched(&bytes, 0, CorruptionReason::PayloadChecksum);
        let mut bytes = [&OLD_SET_A_HI[..], &OLD_DELETE_A].concat();
        bytes[18 + 7] ^= 0x01;
        assert_open_fails_untouched(&bytes, 18, CorruptionReason::HeaderChecksum);
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
        let store = Store::open(tmp.path()).unwrap();
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
        let bytes = [SET_A_HI.as_slice(), &[0u8; 20], DELETE_A.as_slice()].concat();
        assert_open_fails_untouched(&bytes, 22, CorruptionReason::HeaderChecksum);
    }

    #[test]
    fn open_fails_on_a_zero_payload_before_a_valid_record() {
        let bytes = [
            SET_A_HI.as_slice(),
            &delete_a_with_a_zero_payload(),
            DELETE_A.as_slice(),
        ]
        .concat();
        assert_open_fails_untouched(&bytes, 22, CorruptionReason::PayloadChecksum);
    }

    #[test]
    fn open_fails_on_a_garbage_tail_with_its_offset() {
        assert_open_fails_untouched(
            &set_a_hi_then(&[0xab; 20]),
            22,
            CorruptionReason::HeaderChecksum,
        );
    }

    #[test]
    fn open_fails_on_a_corrupted_header_checksum_with_its_offset() {
        let mut bytes = set_a_hi_then(&DELETE_A);
        bytes[22] ^= 0x01;
        assert_open_fails_untouched(&bytes, 22, CorruptionReason::HeaderChecksum);
    }

    #[test]
    fn open_fails_on_a_corrupted_length_with_its_offset() {
        let mut bytes = set_a_hi_then(&DELETE_A);
        bytes[22 + 7] ^= 0x01; // second record's value_len: 0 -> 1
        assert_open_fails_untouched(&bytes, 22, CorruptionReason::HeaderChecksum);
    }

    #[test]
    fn open_fails_on_a_corrupted_key_with_its_offset() {
        let mut bytes = set_a_hi_then(&DELETE_A);
        bytes[22 + 19] ^= 0x01; // second record's key: "a" -> "`"
        assert_open_fails_untouched(&bytes, 22, CorruptionReason::PayloadChecksum);
    }

    #[test]
    fn open_fails_on_an_unsupported_tail_record_and_leaves_it() {
        let mut bytes = set_a_hi_then(&DELETE_A);
        bytes[22 + 4] = 0x19; // second record's type: unknown
        fix_header_checksum(&mut bytes[22..22 + HEADER_LEN]);
        let tmp = store_with_log(&bytes);
        assert!(matches!(
            Store::open(tmp.path()),
            Err(Error::Unsupported {
                offset: 22,
                reason: UnsupportedReason::UnknownType(0x19)
            })
        ));
        assert_eq!(log_bytes(tmp.path()), bytes);
    }

    fn fix_header_checksum(header: &mut [u8]) {
        let checksum = crc32c::crc32c(&header[4..]);
        header[..4].copy_from_slice(&checksum.to_le_bytes());
    }

    // repair

    fn side_file(dir: &Path, offset: u64) -> PathBuf {
        dir.join(format!("{LOG_FILE_NAME}.corrupt-{offset}"))
    }

    /// SET_A_HI, then a Delete of "a" with its key corrupted, then a Set of
    /// "b": the bad record at 22 hides the valid one after it.
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
                offset: 22,
                dropped: 41,
                side_file: side_file(tmp.path(), 22),
            })
        );
        assert_eq!(log_bytes(tmp.path()), SET_A_HI);
        assert_eq!(fs::read(side_file(tmp.path(), 22)).unwrap(), &bytes[22..]);
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
        assert_eq!(fs::read(side_file(tmp.path(), 22)).unwrap(), &bytes[22..]);
    }

    #[test]
    fn repair_rewrites_a_side_file_a_crashed_repair_left() {
        let bytes = log_with_a_corrupt_middle();
        let zeroed_middle = [&bytes[22..24], &[0; 5], &bytes[29..34]].concat();
        for left in [
            &bytes[22..22],
            &bytes[22..29],
            &bytes[22..],
            &[0; 41],
            &zeroed_middle,
        ] {
            let tmp = store_with_log(&bytes);
            fs::write(side_file(tmp.path(), 22), left).unwrap();
            assert_eq!(
                Store::repair(tmp.path()).unwrap(),
                Some(Repaired {
                    offset: 22,
                    dropped: 41,
                    side_file: side_file(tmp.path(), 22),
                }),
                "side file left with {} bytes",
                left.len()
            );
            assert_eq!(log_bytes(tmp.path()), SET_A_HI);
            assert_eq!(fs::read(side_file(tmp.path(), 22)).unwrap(), &bytes[22..]);
        }
    }

    #[test]
    fn repair_refuses_to_overwrite_bytes_an_earlier_repair_saved() {
        let bytes = log_with_a_corrupt_middle();
        let longer = [&bytes[22..], b"!"].concat();
        let changed_first_byte = [&[bytes[22] ^ 1][..], &bytes[23..]].concat();
        for earlier in [
            &b"saved by an earlier repair"[..],
            &longer,
            &changed_first_byte,
        ] {
            let tmp = store_with_log(&bytes);
            let path = side_file(tmp.path(), 22);
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
        bytes[22 + 4] = 0x19;
        fix_header_checksum(&mut bytes[22..22 + HEADER_LEN]);
        let tmp = store_with_log(&bytes);
        assert!(matches!(
            Store::repair(tmp.path()),
            Err(Error::Unsupported { offset: 22, .. })
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
        let store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
    }

    #[test]
    fn set_writes_the_record_format() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        assert_eq!(log_bytes(tmp.path()), SET_A_HI);
    }

    #[test]
    fn set_overwrites_and_appends() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.set(key(b"a"), value(b"yo")).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"yo")));
        assert_eq!(log_bytes(tmp.path()).len(), 44);
    }

    #[test]
    fn keys_are_independent() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"1")).unwrap();
        store.set(key(b"b"), value(b"2")).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"1")));
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
    }

    // delete

    #[test]
    fn delete_removes_the_key_and_appends_a_delete_record() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
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
        let store = Store::open(tmp.path()).unwrap();
        store.delete(key(b"a")).unwrap();
        assert_eq!(log_bytes(tmp.path()), b"");
    }

    #[test]
    fn delete_of_an_already_deleted_key_writes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.delete(key(b"a")).unwrap();
        store.delete(key(b"a")).unwrap();
        assert_eq!(log_bytes(tmp.path()).len(), 42);
    }

    // reopen

    #[test]
    fn reopen_rebuilds_the_index() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let store = Store::open(tmp.path()).unwrap();
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
            let store = Store::open(tmp.path()).unwrap();
            store.set(key(b"a"), value(b"hi")).unwrap();
            store
        });
        let store = Store::open(tmp.path()).unwrap();
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
        let store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.set(key(b"b"), value(b"hi")).unwrap();
        write_log_at(tmp.path(), 22 + 21, b"x"); // b's value: "hi" -> "hx"
        assert!(matches!(
            store.get(&key(b"b")),
            Err(Error::Corruption {
                offset: 22,
                reason: CorruptionReason::PayloadChecksum
            })
        ));
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
    }

    #[test]
    fn get_detects_a_corrupted_header() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
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
        let store = Store::open(tmp.path()).unwrap();
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
        let store = Store::open(tmp.path()).unwrap();
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
        let store = Store::open(tmp.path()).unwrap();
        store.set(key(b"c"), value(b"3")).unwrap();
        store.set(key(b"a"), value(b"1")).unwrap();
        store.set(key(b"b"), value(b"2")).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.delete(key(b"b")).unwrap();
        assert_eq!(log_bytes(tmp.path()).len(), 105);

        store.compact().unwrap();

        let expected = encoded(&[set_record(b"c", b"3"), set_record(b"a", b"hi")]);
        assert_eq!(expected[21..], SET_A_HI);
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
        let store = Store::open(tmp.path()).unwrap();
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
        let store = Store::open(tmp.path()).unwrap();
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
        let store = Store::open(tmp.path()).unwrap();
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
        let store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.compact().unwrap();
        assert_eq!(
            file_names(tmp.path()),
            [
                "tally.00000000000000000022.hint",
                "tally.00000000000000000022.log",
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
                offset: 21,
                reason: CorruptionReason::KeyMismatch
            })
        ));
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
        assert_eq!(store.get(&key(b"cc")).unwrap(), None);
    }

    fn edit(store: &Store, key: &[u8], change: impl FnOnce(&mut Entry)) {
        let mut entry = store.index.get(key).unwrap();
        change(&mut entry);
        store.index.insert(key, entry);
    }

    #[test]
    fn get_reads_the_record_even_when_the_index_has_its_lengths_wrong() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"1")).unwrap();
        edit(&store, b"a", |entry| entry.value_len = 5);
        assert_eq!(
            store.get(&key(b"a")).unwrap(),
            Some(value(b"1")),
            "past the end"
        );

        store.set(key(b"b"), value(b"2")).unwrap();
        edit(&store, b"a", |entry| entry.value_len = 2);
        assert_eq!(
            store.get(&key(b"a")).unwrap(),
            Some(value(b"1")),
            "header disagrees"
        );
    }

    #[test]
    fn verify_passes_a_healthy_store_before_and_after_compaction() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
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
        write_log_at(tmp.path(), 20, b"X");
        let store = Store::open(tmp.path()).unwrap();
        assert!(matches!(
            store.verify(),
            Err(Error::Corruption {
                offset: 43,
                reason: CorruptionReason::PayloadChecksum
            })
        ));
    }

    #[test]
    fn verify_finds_an_index_that_disagrees_with_the_log() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        let store = Store::open(tmp.path()).unwrap();
        store.index.remove(b"b");
        assert!(matches!(
            store.verify(),
            Err(Error::Corruption {
                offset: 65,
                reason: CorruptionReason::IndexMismatch
            })
        ));
        edit(&store, b"a", |entry| entry.offset = 65);
        assert!(matches!(
            store.verify(),
            Err(Error::Corruption {
                offset: 43,
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
            .set_len(22)
            .unwrap();
        assert!(matches!(
            store.verify(),
            Err(Error::Corruption {
                offset: 65,
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

    fn put(store: &Store, k: &[u8], v: &[u8]) {
        store.set(key(k), value(v)).unwrap();
    }

    fn read(store: &Store, k: &[u8]) -> Option<Vec<u8>> {
        store.get(&key(k)).unwrap().map(|v| v.as_bytes().to_vec())
    }

    #[test]
    fn writes_go_to_a_new_segment_once_one_is_full() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open_with(tmp.path(), segmented(24)).unwrap();
        put(&store, b"a", b"hi");
        put(&store, b"b", b"2");
        assert_eq!(segment_bases(tmp.path()), [0], "22 bytes is not full yet");
        put(&store, b"c", b"3");
        assert_eq!(segment_bases(tmp.path()), [0, 43], "43 bytes is full");
        store
            .write(batch(&[(b"d", Some(b"4")), (b"a", None)]))
            .unwrap();
        assert_eq!(segment_bases(tmp.path()), [0, 43]);
        assert_eq!(
            fs::read(tmp.path().join(log_name(43))).unwrap(),
            encoded(&[
                set_record(b"c", b"3"),
                Record::Batch { body_len: 41 },
                set_record(b"d", b"4"),
                delete_record(b"a"),
            ]),
            "21 bytes is not full: the whole batch goes after c=3"
        );
        assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
        assert_eq!(read(&store, b"c").as_deref(), Some(&b"3"[..]));
        drop(store);

        let store = Store::open_with(tmp.path(), segmented(24)).unwrap();
        assert_eq!(read(&store, b"a"), None);
        assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
        assert_eq!(read(&store, b"c").as_deref(), Some(&b"3"[..]));
        assert_eq!(read(&store, b"d").as_deref(), Some(&b"4"[..]));
        assert_eq!(store.dead(), 22 + 27 + 20);
        store.verify().unwrap();
    }

    /// One record per segment: a=1 at 0, b=2 at 21, a deleted at 42, c=3
    /// at 62.
    fn one_record_per_segment(dir: &Path) -> Store {
        let store = Store::open_with(dir, segmented(1)).unwrap();
        put(&store, b"a", b"1");
        put(&store, b"b", b"2");
        store.delete(key(b"a")).unwrap();
        put(&store, b"c", b"3");
        assert_eq!(segment_bases(dir), [0, 21, 42, 62]);
        store
    }

    #[test]
    fn cleaning_a_delete_while_an_older_segment_has_its_key_carries_it() {
        let tmp = tempfile::tempdir().unwrap();
        let store = one_record_per_segment(tmp.path());
        store.clean(&store.writer(), &[42]).unwrap();
        assert_eq!(segment_bases(tmp.path()), [0, 21, 62, 83]);
        assert_eq!(
            fs::read(tmp.path().join(log_name(83))).unwrap(),
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
        let store = one_record_per_segment(tmp.path());
        store.clean(&store.writer(), &[0, 21, 42]).unwrap();
        assert_eq!(segment_bases(tmp.path()), [62, 83]);
        assert_eq!(
            fs::read(tmp.path().join(log_name(83))).unwrap(),
            encoded(&[set_record(b"b", b"2")]),
            "only b=2 is copied; the Delete has nothing left to hide"
        );
        assert!(tmp.path().join(hint_name(83)).exists());
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
        assert_eq!(store.index.get(b"b").unwrap().offset, 21);
        assert_eq!(store.index.get(b"c").unwrap().offset, 62);
        write_log_at(tmp.path(), 19, b"X");
        assert!(matches!(
            store.get(&key(b"c")),
            Err(Error::Corruption {
                offset: 62,
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
        let store = Store::open_with(tmp.path(), options).unwrap();
        put(&store, b"a", b"1");
        put(&store, b"b", b"2");
        put(&store, b"b", b"3");
        // b=3 at 42 made b=2 at 21 dead: 21 of 63, not due. Deleting b
        // makes 21 + 21 + 20 of 83 dead. Cleaning the segment at 21 leaves
        // 41 of 62, still due; then 42 leaves 20 of 41, not due. Neither has
        // a live record, and a=1 at 0 is never read.
        store.delete(key(b"b")).unwrap();
        assert_eq!(segment_bases(tmp.path()), [0, 63, 83]);
        assert_eq!(fs::read(tmp.path().join(log_name(83))).unwrap(), b"");
        assert_eq!(store.dead(), 20);
        drop(store);
        let store = Store::open_with(tmp.path(), options).unwrap();
        assert_eq!(read(&store, b"a").as_deref(), Some(&b"1"[..]));
        assert_eq!(read(&store, b"b"), None);
    }

    #[test]
    fn a_sealed_segment_cut_short_is_corruption_that_repair_moves() {
        let tmp = tempfile::tempdir().unwrap();
        drop(one_record_per_segment(tmp.path()));
        let second = tmp.path().join(log_name(21));
        fs::write(&second, &fs::read(&second).unwrap()[..10]).unwrap();
        assert!(matches!(
            Store::open_with(tmp.path(), segmented(1)),
            Err(Error::Corruption {
                offset: 21,
                reason: CorruptionReason::CutShort
            })
        ));

        let repaired = Store::repair(tmp.path()).unwrap().unwrap();
        assert_eq!((repaired.offset, repaired.dropped), (21, 10));
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
        assert_eq!(hint[12], b'a');
        hint[12] = b'c';
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
            tmp.path().join(log_name(43)),
            tmp.path().join(LOG_FILE_NAME),
        )
        .unwrap();
        fs::rename(
            tmp.path().join(hint_name(43)),
            tmp.path().join(HINT_FILE_NAME),
        )
        .unwrap();
        fs::write(tmp.path().join(COMPACT_FILE_NAME), b"left by a crash").unwrap();
        write_log_at(tmp.path(), 20, b"X");

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
            tmp.path().join(log_name(43)),
            tmp.path().join(LOG_FILE_NAME),
        )
        .unwrap();
        fs::remove_file(tmp.path().join(hint_name(43))).unwrap();
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
        for torn in [&marker()[..7], &[], &[0; SHORTEST_HEADER_LEN]] {
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
        let store = Store::open(tmp.path()).unwrap();
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
                Record::Batch { body_len: 84 },
                set_record(b"a", b"1"),
                set_record(b"b", b"2"),
                set_record(b"a", b"hi"),
                delete_record(b"b"),
            ])
        );
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        assert_eq!(store.get(&key(b"b")).unwrap(), None);
        assert_eq!(store.dead(), 27 + 21 + 21 + 20);
        drop(store);
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        assert_eq!(store.get(&key(b"b")).unwrap(), None);
        assert_eq!(store.dead(), 27 + 21 + 21 + 20);
        store.verify().unwrap();
    }

    #[test]
    fn a_batch_deletes_only_keys_live_at_that_point_in_it() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
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
                Record::Batch { body_len: 41 },
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
            Record::Batch { body_len: 41 },
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
        log[len - 20..].fill(0);
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
        log[before + 27 + 20] ^= 0x01;
        let tmp = tempfile::tempdir().unwrap();
        fs::write(log_path(tmp.path()), &log).unwrap();
        assert!(matches!(
            Store::open(tmp.path()),
            Err(Error::Corruption {
                offset: 21,
                reason: CorruptionReason::PayloadChecksum
            })
        ));

        let repaired = Store::repair(tmp.path()).unwrap().unwrap();
        assert_eq!((repaired.offset, repaired.dropped), (21, 68));
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), None);
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
    }

    #[test]
    fn a_batch_inside_a_batch_or_a_record_past_its_end_is_corrupt() {
        let nested = encoded(&[
            Record::Batch { body_len: 48 },
            Record::Batch { body_len: 21 },
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
        let store = Store::open(tmp.path()).unwrap();
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

    /// a=hi at 0 and b=2 at 22, compacted into the segment at 43, so a is
    /// at 43, b at 65, and the segment's hint covers both.
    fn compacted_a_and_b(dir: &Path) {
        let store = Store::open(dir).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.set(key(b"b"), value(b"2")).unwrap();
        store.compact().unwrap();
    }

    fn mapped(store: &Store) -> Vec<(u64, bool)> {
        store
            .segments()
            .iter()
            .map(|s| (s.base, s.is_mapped()))
            .collect()
    }

    #[test]
    fn every_segment_but_the_active_one_is_mapped() {
        let tmp = tempfile::tempdir().unwrap();
        let store = one_record_per_segment(tmp.path());
        assert_eq!(
            mapped(&store),
            [(0, true), (21, true), (42, true), (62, false)]
        );
        put(&store, b"d", b"4");
        assert_eq!(mapped(&store)[3..], [(62, true), (83, false)]);
        assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
        assert_eq!(read(&store, b"c").as_deref(), Some(&b"3"[..]));
        drop(store);

        let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
        assert_eq!(
            mapped(&store),
            [(0, true), (21, true), (42, true), (62, true), (83, false)]
        );
        assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
        assert_eq!(read(&store, b"a"), None);
    }

    #[test]
    fn a_value_damaged_in_a_mapped_segment_fails_its_checksum() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
        put(&store, b"c", b"3");
        drop(store);
        let file = OpenOptions::new()
            .write(true)
            .open(tmp.path().join(log_name(43)))
            .unwrap();
        file.write_all_at(b"X", 20).unwrap();
        drop(file);

        let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
        assert_eq!(mapped(&store), [(43, true), (86, false)]);
        assert!(matches!(
            store.get(&key(b"a")),
            Err(Error::Corruption {
                offset: 43,
                reason: CorruptionReason::PayloadChecksum
            })
        ));
        assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
    }

    #[test]
    fn open_after_compact_reads_keys_from_the_hint_not_the_log() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        write_log_at(tmp.path(), 20, b"X");

        let store = Store::open(tmp.path()).unwrap();
        assert!(matches!(
            store.get(&key(b"a")),
            Err(Error::Corruption {
                offset: 43,
                reason: CorruptionReason::PayloadChecksum
            })
        ));
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
    }

    #[test]
    fn a_hint_read_on_many_threads_gives_the_index_one_thread_does() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open_with(
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
        let segment = Segment::new(
            base,
            fs.open_existing(&tmp.path().join(log_name(base))).unwrap(),
            log_bytes(tmp.path()).len() as u64,
        );
        let mut one = Index::new();
        let (len_one, _) = read_hint(&file, &segment, &mut one, HINT_WINDOW, u64::MAX).unwrap();
        let mut many = Index::new();
        let (len_many, _) = read_hint(&file, &segment, &mut many, 4096, 0).unwrap();

        assert_eq!((len_one, len_many), (segment.end(), segment.end()));
        assert_eq!(many.len(), 3000);
        for i in 0..3000u32 {
            let k = format!("k{i}");
            assert_eq!(many.get(k.as_bytes()), one.get(k.as_bytes()), "{k}");
        }
        let store = Store::open(tmp.path()).unwrap();
        store.verify().unwrap();
    }

    /// 150,000 records, three windows: sets, overwrites, deletes and
    /// batches, then a torn tail. Returns the log and the offset of a Set
    /// in the third window.
    fn log_of_many_windows() -> (Vec<u8>, u64) {
        let k = |i: u32| key(format!("k{}", i % 40_000).as_bytes());
        let mut log = Vec::new();
        let mut set_at = 0;
        for i in 0..150_000u32 {
            if i == 140_001 {
                set_at = log.len() as u64;
            }
            if i % 7 == 3 {
                encode(&Record::Delete { key: k(i) }, &mut log);
            } else if i % 1000 == 0 {
                let mut body = Vec::new();
                encode(
                    &Record::Set {
                        key: k(i),
                        value: value(b"batched"),
                    },
                    &mut body,
                );
                encode(&Record::Delete { key: k(i + 1) }, &mut body);
                encode(
                    &Record::Batch {
                        body_len: body.len() as u64,
                    },
                    &mut log,
                );
                log.extend_from_slice(&body);
            } else {
                encode(
                    &Record::Set {
                        key: k(i),
                        value: value(&i.to_le_bytes()),
                    },
                    &mut log,
                );
            }
        }
        log.extend_from_slice(&SET_A_HI[..9]);
        (log, set_at)
    }

    #[test]
    fn a_scan_on_many_threads_builds_the_index_one_thread_would() {
        let (log, _) = log_of_many_windows();
        let fs = MemFs::new();
        fs.put(Path::new("/log"), &log);
        let file = fs.open_existing(Path::new("/log")).unwrap();
        let len = log.len() as u64;

        let mut one = Index::new();
        let mut many = Index::new();
        assert_eq!(
            scan_on(&file, &mut one, 0, len, 1, &mut |_, _| {}).unwrap(),
            len - 9
        );
        assert_eq!(
            scan_on(&file, &mut many, 0, len, 4, &mut |_, _| {}).unwrap(),
            len - 9
        );
        assert_eq!(many.len(), one.len());
        assert_eq!(many.len(), 34_285);
        for i in 0..40_000u32 {
            let k = format!("k{i}");
            assert_eq!(many.get(k.as_bytes()), one.get(k.as_bytes()), "{k}");
        }
    }

    #[test]
    fn a_scan_on_many_threads_reports_corruption_where_one_thread_would() {
        let (mut log, set_at) = log_of_many_windows();
        log[set_at as usize + HEADER_LEN] ^= 1;
        let fs = MemFs::new();
        fs.put(Path::new("/log"), &log);
        let file = fs.open_existing(Path::new("/log")).unwrap();
        for threads in [1, 4] {
            assert!(
                matches!(
                    scan_on(
                        &file,
                        &mut Index::new(),
                        0,
                        log.len() as u64,
                        threads,
                        &mut |_, _| {},
                    ),
                    Err(Error::Corruption {
                        offset,
                        reason: CorruptionReason::PayloadChecksum,
                    }) if offset == set_at
                ),
                "{threads} threads"
            );
        }
    }

    #[test]
    fn records_written_after_the_hint_are_read_from_the_log() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        let store = Store::open(tmp.path()).unwrap();
        store.set(key(b"c"), value(b"3")).unwrap();
        store.delete(key(b"a")).unwrap();
        store.set(key(b"b"), value(b"22")).unwrap();
        drop(store);

        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), None);
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"22")));
        assert_eq!(store.get(&key(b"c")).unwrap(), Some(value(b"3")));
        assert_eq!(store.dead(), 22 + 20 + 21);
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
            ["tally.00000000000000000043.log", "tally.lock", "tally.log"]
        );
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
    }

    #[test]
    fn a_hint_for_more_log_than_there_is_is_removed() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        let log = log_bytes(tmp.path());
        fs::write(log_path(tmp.path()), &log[..22]).unwrap();

        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(
            file_names(tmp.path()),
            ["tally.00000000000000000043.log", "tally.lock", "tally.log"]
        );
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        assert_eq!(store.get(&key(b"b")).unwrap(), None);
    }

    #[test]
    fn repair_removes_the_hint_before_truncating() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        write_log_at(tmp.path(), 41, b"X");

        let repaired = Store::repair(tmp.path()).unwrap().unwrap();
        assert_eq!(repaired.offset, 65);
        assert!(!hint_path(tmp.path()).exists());
        let store = Store::open(tmp.path()).unwrap();
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
        let store = Store::open(tmp.path()).unwrap();
        store.delete(key(b"a")).unwrap();
        store.compact().unwrap();
        drop(store);

        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(log_bytes(tmp.path()), encoded(&[set_record(b"b", b"2")]));
        assert_eq!(store.get(&key(b"a")).unwrap(), None);
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
        assert_eq!(store.dead(), 0);
    }

    /// A hint's bytes, from its `entries`, Set count, log length and `crc`,
    /// computed in Python.
    fn hint_bytes(entries: &[&[u8]], count: u64, log_len: u64, crc: u64) -> Vec<u8> {
        let mut bytes = b"TLYH\x02".to_vec();
        for entry in entries {
            bytes.extend_from_slice(entry);
        }
        bytes.extend_from_slice(&count.to_le_bytes());
        bytes.extend_from_slice(&log_len.to_le_bytes());
        bytes.extend_from_slice(&crc.to_le_bytes());
        bytes
    }

    /// Flips a byte of the segment at `base`, at `at` in its file.
    fn damage(dir: &Path, base: u64, at: u64) {
        let path = dir.join(log_name(base));
        let mut bytes = fs::read(&path).unwrap();
        bytes[at as usize] ^= 0x01;
        fs::write(path, bytes).unwrap();
    }

    #[test]
    fn open_writes_a_hint_for_each_sealed_segment_it_reads() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
        store
            .write(batch(&[(b"a", Some(b"1")), (b"b", Some(b"2"))]))
            .unwrap();
        store.delete(key(b"a")).unwrap();
        put(&store, b"c", b"3");
        drop(store);
        assert_eq!(segment_bases(tmp.path()), [0, 69, 89]);

        drop(Store::open_with(tmp.path(), segmented(1)).unwrap());
        assert_eq!(
            file_names(tmp.path()),
            [
                "tally.00000000000000000000.hint",
                "tally.00000000000000000000.log",
                "tally.00000000000000000069.hint",
                "tally.00000000000000000069.log",
                "tally.00000000000000000089.log",
                "tally.lock",
                "tally.log"
            ],
            "the active segment gets none"
        );
        assert_eq!(
            fs::read(tmp.path().join(hint_name(0))).unwrap(),
            hint_bytes(
                &[
                    &[0x13, 0, 0, 8, 0, 0, 0],
                    &[0x11, 1, 0, 1, 0, 0, 0, b'a'],
                    &[0x11, 1, 0, 1, 0, 0, 0, b'b'],
                ],
                2,
                69,
                0x908f_2205_8d4b_c596
            )
        );
        assert_eq!(
            fs::read(tmp.path().join(hint_name(69))).unwrap(),
            hint_bytes(
                &[&[0x12, 1, 0, 0, 0, 0, 0, b'a']],
                0,
                20,
                0x11c1_da6b_3036_b216
            )
        );

        // From the hints, open no longer reads b's record, so damage to its
        // value shows up only when b is read.
        damage(tmp.path(), 0, 68);
        let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
        assert!(matches!(
            store.get(&key(b"b")),
            Err(Error::Corruption {
                offset: 48,
                reason: CorruptionReason::PayloadChecksum
            })
        ));
        assert_eq!(read(&store, b"a"), None);
        assert_eq!(read(&store, b"c").as_deref(), Some(&b"3"[..]));
    }

    #[test]
    fn open_extends_a_hint_that_covers_only_the_start_of_a_sealed_segment() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open_with(tmp.path(), segmented(50)).unwrap();
        put(&store, b"a", b"hi");
        put(&store, b"b", b"2");
        store.compact().unwrap();
        put(&store, b"c", b"3");
        put(&store, b"d", b"4");
        drop(store);
        assert_eq!(segment_bases(tmp.path()), [43, 107]);

        drop(Store::open_with(tmp.path(), segmented(50)).unwrap());
        assert_eq!(
            fs::read(tmp.path().join(hint_name(43))).unwrap(),
            hint_bytes(
                &[
                    &[0x11, 1, 0, 2, 0, 0, 0, b'a'],
                    &[0x11, 1, 0, 1, 0, 0, 0, b'b'],
                    &[0x11, 1, 0, 1, 0, 0, 0, b'c'],
                ],
                3,
                64,
                0x4390_d75d_52fc_839d
            )
        );

        damage(tmp.path(), 43, 63);
        let store = Store::open_with(tmp.path(), segmented(50)).unwrap();
        assert!(matches!(
            store.get(&key(b"c")),
            Err(Error::Corruption {
                offset: 86,
                reason: CorruptionReason::PayloadChecksum
            })
        ));
        assert_eq!(read(&store, b"a").as_deref(), Some(&b"hi"[..]));
        assert_eq!(read(&store, b"d").as_deref(), Some(&b"4"[..]));
    }

    #[test]
    fn a_store_opened_from_the_hints_open_wrote_matches_a_full_read() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open_with(tmp.path(), segmented(64)).unwrap();
        let mut model = HashMap::new();
        for i in 0..90u8 {
            let k = [b'k', i % 7];
            let other = [b'k', i % 5];
            match i % 4 {
                0 => {
                    store.delete(key(&k)).unwrap();
                    model.remove(&k);
                }
                1 => {
                    store
                        .write(batch(&[(&k, Some(&[i])), (&other, None)]))
                        .unwrap();
                    model.insert(k, vec![i]);
                    model.remove(&other);
                }
                _ => {
                    put(&store, &k, &[i]);
                    model.insert(k, vec![i]);
                }
            }
        }
        drop(store);
        let segments = segment_bases(tmp.path()).len();
        assert!(segments > 20, "{segments} segments");

        for _ in 0..2 {
            let store = Store::open_with(tmp.path(), segmented(64)).unwrap();
            store.verify().unwrap();
            for i in 0..7 {
                let k = [b'k', i];
                assert_eq!(read(&store, &k), model.get(&k).cloned(), "{k:?}");
            }
        }
        let hints = file_names(tmp.path())
            .iter()
            .filter(|n| n.ends_with(".hint"))
            .count();
        assert_eq!(hints, segments - 1);
    }

    #[test]
    fn a_sealed_segment_from_before_64_bit_checksums_gets_a_hint_too() {
        let mut old = Vec::new();
        encode_as(Format::Crc32c, &set_record(b"a", b"hi"), &mut old);
        let tmp = store_with_log(&old);
        let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
        put(&store, b"b", b"2");
        drop(store);

        drop(Store::open_with(tmp.path(), segmented(1)).unwrap());
        assert_eq!(
            fs::read(tmp.path().join(hint_name(0))).unwrap(),
            hint_bytes(
                &[&[0x01, 1, 0, 2, 0, 0, 0, b'a']],
                1,
                18,
                0x500e_34e4_b61c_0fa6
            )
        );
        let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
        assert_eq!(store.index.get(b"a").unwrap().format, Format::Crc32c);
        assert_eq!(read(&store, b"a").as_deref(), Some(&b"hi"[..]));
        assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
        store.verify().unwrap();
    }

    fn open_on(fs: &MemFs, options: Options) -> Store<MemFs> {
        Store::open_in(fs.clone(), Path::new("/db"), options).unwrap()
    }

    fn get_on(store: &Store<MemFs>, k: &[u8]) -> Option<Vec<u8>> {
        store.get(&key(k)).unwrap().map(|v| v.as_bytes().to_vec())
    }

    #[test]
    fn a_hint_cut_short_by_a_full_disk_is_replaced_by_the_next_open() {
        let fs = MemFs::new();
        let store = open_on(&fs, segmented(1));
        store.set(key(b"a"), value(b"1")).unwrap();
        store.set(key(b"b"), value(b"2")).unwrap();
        drop(store);
        let hint = Path::new("/db").join(hint_name(0));

        fs.limit_free_space(Some(10));
        let store = open_on(&fs, segmented(1));
        assert_eq!(fs.read(&hint).unwrap(), b"TLYH\x02\x11\x01\x00\x01\x00");
        assert_eq!(get_on(&store, b"a").as_deref(), Some(&b"1"[..]));
        assert_eq!(get_on(&store, b"b").as_deref(), Some(&b"2"[..]));
        drop(store);

        fs.limit_free_space(None);
        let store = open_on(&fs, segmented(1));
        assert_eq!(
            fs.read(&hint).unwrap(),
            hint_bytes(
                &[&[0x11, 1, 0, 1, 0, 0, 0, b'a']],
                1,
                21,
                0xeba4_83e6_d395_4202
            )
        );
        assert_eq!(get_on(&store, b"a").as_deref(), Some(&b"1"[..]));
        assert_eq!(get_on(&store, b"b").as_deref(), Some(&b"2"[..]));
    }

    #[test]
    fn a_hint_extension_cut_short_by_a_full_disk_is_replaced_by_the_next_open() {
        let fs = MemFs::new();
        let store = open_on(&fs, segmented(50));
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.set(key(b"b"), value(b"2")).unwrap();
        store.compact().unwrap();
        store.set(key(b"c"), value(b"3")).unwrap();
        store.set(key(b"d"), value(b"4")).unwrap();
        drop(store);
        let hint = Path::new("/db").join(hint_name(43));
        let compacted = fs.read(&hint).unwrap();
        assert_eq!(compacted.len(), 5 + 16 + 24);

        // Cutting the old trailer frees 24 bytes; the new entry and trailer
        // need 32.
        fs.limit_free_space(Some(0));
        let store = open_on(&fs, segmented(50));
        let cut = fs.read(&hint).unwrap();
        assert_eq!(cut.len(), 5 + 16 + 24);
        assert_eq!(cut[..21], compacted[..21]);
        assert_eq!(cut[21..29], [0x11, 1, 0, 1, 0, 0, 0, b'c']);
        assert_eq!(get_on(&store, b"c").as_deref(), Some(&b"3"[..]));
        drop(store);

        fs.limit_free_space(None);
        let store = open_on(&fs, segmented(50));
        assert_eq!(
            fs.read(&hint).unwrap(),
            hint_bytes(
                &[
                    &[0x11, 1, 0, 2, 0, 0, 0, b'a'],
                    &[0x11, 1, 0, 1, 0, 0, 0, b'b'],
                    &[0x11, 1, 0, 1, 0, 0, 0, b'c'],
                ],
                3,
                64,
                0x4390_d75d_52fc_839d
            )
        );
        for (k, v) in [(b"a", &b"hi"[..]), (b"b", b"2"), (b"c", b"3"), (b"d", b"4")] {
            assert_eq!(get_on(&store, k).as_deref(), Some(v));
        }
    }

    #[test]
    fn open_succeeds_when_it_cannot_write_a_hint() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
        put(&store, b"a", b"1");
        put(&store, b"b", b"2");
        drop(store);

        let mode = |mode| fs::set_permissions(tmp.path(), fs::Permissions::from_mode(mode));
        mode(0o555).unwrap();
        let writable = fs::File::create(tmp.path().join("probe")).is_ok();
        let opened = Store::open_with(tmp.path(), segmented(1));
        mode(0o755).unwrap();
        if writable {
            // Run as root, which writes anyway: nothing to check.
            return;
        }
        let store = opened.unwrap();
        assert!(!tmp.path().join(hint_name(0)).exists());
        assert_eq!(read(&store, b"a").as_deref(), Some(&b"1"[..]));
        assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
    }

    #[test]
    fn compact_stops_at_a_corrupt_dead_record_too() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
        store.set(key(b"b"), value(b"2")).unwrap();
        store.set(key(b"a"), value(b"1")).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        write_log_at(tmp.path(), 41, b"X");
        let before = log_bytes(tmp.path());

        assert!(matches!(
            store.compact(),
            Err(Error::Corruption {
                offset: 21,
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
        let store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.compact().unwrap();
        assert_eq!(log_bytes(tmp.path()), SET_A_HI);
    }

    #[test]
    fn the_lock_still_holds_after_compact_replaces_the_log() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
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
        let store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.set(key(b"b"), value(b"2")).unwrap();
        store.delete(key(b"b")).unwrap();
        write_log_at(tmp.path(), 21, b"X");
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
        let store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"1")).unwrap();
        store.set(key(b"b"), value(b"2")).unwrap();
        store.set(key(b"c"), value(b"3")).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        write_log_at(tmp.path(), 61, b"X");

        assert!(matches!(
            store.compact(),
            Err(Error::Corruption {
                offset: 42,
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
        let store = Store::open(tmp.path()).unwrap();
        let big = vec![b'v'; 700 << 10];
        store.set(key(b"a"), value(&big)).unwrap();
        store.set(key(b"b"), value(&big)).unwrap();
        store.set(key(b"c"), value(b"3")).unwrap();
        let c = 2 * (19 + 1 + (700 << 10));
        write_log_at(tmp.path(), c + 20, b"X");

        // More than the 1 MiB compaction buffer is copied before c fails.
        assert!(matches!(
            store.compact(),
            Err(Error::Corruption { offset, reason: CorruptionReason::PayloadChecksum })
                if offset == c
        ));
        let end = c + 21;
        assert_eq!(segment_bases(tmp.path()), [0, end]);
        assert_eq!(fs::read(tmp.path().join(log_name(end))).unwrap(), b"");
        assert!(!tmp.path().join(hint_name(end)).exists());
        assert_eq!(read(&store, b"a"), Some(big.clone()));
        assert_eq!(read(&store, b"b"), Some(big));
        put(&store, b"d", b"4");
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
        let store = Store::open_with(tmp.path(), auto(50, 0)).unwrap();
        store.set(key(b"a"), value(b"1")).unwrap();
        store.set(key(b"b"), value(b"2")).unwrap();
        store.set(key(b"a"), value(b"3")).unwrap();
        assert_eq!(log_bytes(tmp.path()).len(), 63, "21 of 63 dead");

        store.set(key(b"b"), value(b"4")).unwrap();
        assert_eq!(
            log_bytes(tmp.path()),
            encoded(&[set_record(b"a", b"3"), set_record(b"b", b"4")])
        );

        store.set(key(b"a"), value(b"5")).unwrap();
        assert_eq!(
            log_bytes(tmp.path()).len(),
            63,
            "compaction left nothing dead, so 21 of 63 now"
        );
    }

    #[test]
    fn a_delete_counts_its_own_record_as_dead_too() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open_with(tmp.path(), auto(50, 0)).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.set(key(b"b"), value(b"2")).unwrap();
        store.delete(key(b"a")).unwrap();
        assert_eq!(log_bytes(tmp.path()), encoded(&[set_record(b"b", b"2")]));
    }

    #[test]
    fn a_log_below_the_minimum_size_is_not_compacted() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open_with(tmp.path(), auto(50, 43)).unwrap();
        store.set(key(b"a"), value(b"1")).unwrap();
        store.set(key(b"a"), value(b"2")).unwrap();
        assert_eq!(log_bytes(tmp.path()).len(), 42);
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
        let store = Store::open_with(tmp.path(), options).unwrap();
        for v in [b"1", b"2", b"3"] {
            store.set(key(b"a"), value(v)).unwrap();
        }
        store.delete(key(b"a")).unwrap();
        drop(store);
        Store::open_with(tmp.path(), options).unwrap();
        assert_eq!(log_bytes(tmp.path()).len(), 83);
    }

    #[test]
    fn open_compacts_a_log_that_is_due() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open_with(
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
        let store = Store::open_with(
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

        let store = Store::open_with(tmp.path(), auto(50, 0)).unwrap();
        assert_eq!(log_bytes(tmp.path()).len(), 63, "21 of 63 dead");
        store.set(key(b"b"), value(b"4")).unwrap();
        assert_eq!(
            log_bytes(tmp.path()),
            encoded(&[set_record(b"a", b"3"), set_record(b"b", b"4")])
        );
    }

    #[test]
    fn a_failed_automatic_compaction_keeps_the_write_and_stays_off() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open_with(tmp.path(), auto(50, 0)).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.set(key(b"b"), value(b"2")).unwrap();
        write_log_at(tmp.path(), 21, b"X");
        store.set(key(b"b"), value(b"3")).unwrap();
        store.set(key(b"b"), value(b"4")).unwrap();
        assert!(!store.auto_failed.load(Ordering::Relaxed), "42 of 85 dead");

        store.set(key(b"b"), value(b"5")).unwrap();
        assert!(store.auto_failed.load(Ordering::Relaxed));
        assert_eq!(log_bytes(tmp.path()).len(), 106);
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"5")));
    }

    #[test]
    fn a_store_can_be_shared_between_threads() {
        fn shared<T: Send + Sync>() {}
        shared::<Store>();
    }

    #[test]
    fn writes_waiting_for_the_writer_share_one_fsync() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
        let writing = store.writer();
        thread::scope(|scope| {
            for i in 0..8u8 {
                let store = &store;
                scope.spawn(move || store.set(key(&[i]), value(&[i])).unwrap());
            }
            // The first to queue leads alone and waits for `writing`; the
            // other seven queue behind it.
            while hold(&store.queue).waiting.len() < 7 {
                thread::yield_now();
            }
            drop(writing);
        });
        assert_eq!(store.commits.load(Ordering::Relaxed), 2);
        assert_eq!(log_bytes(tmp.path()).len(), 8 * 21);
        for i in 0..8u8 {
            assert_eq!(read(&store, &[i]), Some(vec![i]));
        }
        store.verify().unwrap();
    }

    #[test]
    fn a_group_applies_its_writes_in_order_with_one_fsync() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
        put(&store, b"b", b"0");
        let results = store.commit(&[
            Write::One(set_record(b"a", b"1")),
            Write::One(delete_record(b"a")),
            Write::One(delete_record(b"c")),
            Write::Batch(vec![delete_record(b"a"), set_record(b"b", b"2")]),
            Write::Batch(vec![delete_record(b"c")]),
        ]);
        assert_eq!(results.len(), 5);
        assert!(results.iter().all(Result::is_ok));
        assert_eq!(store.commits.load(Ordering::Relaxed), 2);
        assert_eq!(
            log_bytes(tmp.path()),
            encoded(&[
                set_record(b"b", b"0"),
                set_record(b"a", b"1"),
                delete_record(b"a"),
                Record::Batch { body_len: 21 },
                set_record(b"b", b"2"),
            ])
        );
        assert_eq!(read(&store, b"a"), None);
        assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
        assert_eq!(store.dead(), 21 + 21 + 20 + 27);
        store.verify().unwrap();
    }

    #[test]
    fn a_failed_group_fails_every_write_in_it() {
        let results = each(Error::Io(io::Error::other("disk on fire")), 3);
        let messages: Vec<String> = results
            .into_iter()
            .map(|r| match r {
                Err(Error::Io(err)) => err.to_string(),
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(messages, ["disk on fire"; 3]);
    }

    fn log_on(fs: &MemFs) -> Vec<u8> {
        fs.read(&Path::new("/db").join(log_name(0))).unwrap()
    }

    #[test]
    fn a_failed_fsync_fails_its_write_and_every_write_after_until_reopen() {
        let fs = MemFs::new();
        let store = open_on(&fs, segmented(1 << 20));
        store.set(key(b"a"), value(b"1")).unwrap();
        fs.fail_syncs(true);
        let err = store.set(key(b"b"), value(b"2")).unwrap_err();
        assert!(
            matches!(&err, Error::Io(io) if io.to_string() == "injected fsync failure"),
            "{err:?}"
        );
        fs.fail_syncs(false);
        assert!(matches!(
            store.set(key(b"c"), value(b"3")),
            Err(Error::Poisoned)
        ));
        assert!(matches!(store.delete(key(b"a")), Err(Error::Poisoned)));
        assert!(matches!(
            store.write(batch(&[(b"c", Some(b"3"))])),
            Err(Error::Poisoned)
        ));
        assert!(matches!(store.compact(), Err(Error::Poisoned)));
        assert_eq!(get_on(&store, b"a").as_deref(), Some(&b"1"[..]));
        assert_eq!(get_on(&store, b"b"), None);
        drop(store);

        let store = open_on(&fs, segmented(1 << 20));
        assert_eq!(
            get_on(&store, b"b").as_deref(),
            Some(&b"2"[..]),
            "the write reached the file; only its fsync failed"
        );
        store.set(key(b"c"), value(b"3")).unwrap();
        assert_eq!(get_on(&store, b"c").as_deref(), Some(&b"3"[..]));
    }

    #[test]
    fn a_write_that_fails_is_truncated_away_and_the_store_stays_writable() {
        let fs = MemFs::new();
        let store = open_on(&fs, segmented(1 << 20));
        store.set(key(b"a"), value(b"1")).unwrap();
        fs.limit_free_space(Some(5));
        let err = store.set(key(b"b"), value(b"2")).unwrap_err();
        assert!(
            matches!(&err, Error::Io(io) if io.kind() == ErrorKind::StorageFull),
            "{err:?}"
        );
        assert_eq!(log_on(&fs), encoded(&[set_record(b"a", b"1")]));

        fs.limit_free_space(None);
        store.set(key(b"c"), value(b"3")).unwrap();
        assert_eq!(
            log_on(&fs),
            encoded(&[set_record(b"a", b"1"), set_record(b"c", b"3")])
        );
        drop(store);
        let store = open_on(&fs, segmented(1 << 20));
        assert_eq!(get_on(&store, b"b"), None);
        assert_eq!(get_on(&store, b"c").as_deref(), Some(&b"3"[..]));
    }

    #[test]
    fn a_failed_truncate_after_a_failed_write_poisons_the_store() {
        let fs = MemFs::new();
        let store = open_on(&fs, segmented(1 << 20));
        store.set(key(b"a"), value(b"1")).unwrap();
        fs.limit_free_space(Some(5));
        fs.fail_truncates(true);
        let err = store.set(key(b"b"), value(b"2")).unwrap_err();
        assert!(
            matches!(&err, Error::Io(io) if io.kind() == ErrorKind::StorageFull),
            "{err:?}"
        );
        fs.limit_free_space(None);
        fs.fail_truncates(false);
        assert!(matches!(
            store.set(key(b"c"), value(b"3")),
            Err(Error::Poisoned)
        ));
        assert_eq!(log_on(&fs).len(), 21 + 5, "the torn write is still there");
        drop(store);

        let store = open_on(&fs, segmented(1 << 20));
        assert_eq!(log_on(&fs), encoded(&[set_record(b"a", b"1")]));
        assert_eq!(get_on(&store, b"a").as_deref(), Some(&b"1"[..]));
        store.set(key(b"c"), value(b"3")).unwrap();
    }

    #[test]
    fn a_failed_fsync_during_compaction_poisons_the_store_and_keeps_the_old_segments() {
        let fs = MemFs::new();
        let store = open_on(&fs, segmented(1));
        store.set(key(b"a"), value(b"1")).unwrap();
        store.set(key(b"a"), value(b"2")).unwrap();
        store.set(key(b"b"), value(b"3")).unwrap();
        fs.fail_syncs(true);
        assert!(matches!(store.compact(), Err(Error::Io(_))));
        fs.fail_syncs(false);
        assert!(matches!(
            store.set(key(b"c"), value(b"4")),
            Err(Error::Poisoned)
        ));
        assert_eq!(get_on(&store, b"a").as_deref(), Some(&b"2"[..]));
        assert_eq!(get_on(&store, b"b").as_deref(), Some(&b"3"[..]));
        drop(store);

        assert_eq!(
            fs.read(&Path::new("/db").join(hint_name(63))),
            None,
            "no hint vouches for copies whose fsync failed"
        );
        let store = open_on(&fs, segmented(1));
        assert_eq!(get_on(&store, b"a").as_deref(), Some(&b"2"[..]));
        assert_eq!(get_on(&store, b"b").as_deref(), Some(&b"3"[..]));
        store.verify().unwrap();
        store.compact().unwrap();
        assert_eq!(get_on(&store, b"a").as_deref(), Some(&b"2"[..]));
    }

    #[test]
    fn a_read_that_finds_its_segment_removed_follows_the_key_to_its_copy() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
        put(&store, b"a", b"1");
        put(&store, b"b", b"2");
        let before = store.index.get(b"a").unwrap();
        assert_eq!(before.offset, 0);
        store.compact().unwrap();
        assert_eq!(segment_bases(tmp.path()), [42]);

        let (entry, segment) = store.locate(b"a", before).unwrap();
        assert_eq!(
            entry,
            Entry {
                offset: 42,
                value_len: 1,
                key_len: 1,
                format: Format::Crc64,
            }
        );
        assert_eq!(segment.base, 42);
        store.delete(key(b"a")).unwrap();
        assert!(store.locate(b"a", before).is_none());
    }

    #[test]
    fn reads_while_other_threads_write_and_compact_see_every_value() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open_with(tmp.path(), segmented(256)).unwrap();
        for i in 0..64u8 {
            put(&store, &[b's', i], &[i]);
            put(&store, &[b's', i], &[i, i]);
        }
        let stop = AtomicBool::new(false);
        thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    while !stop.load(Ordering::Relaxed) {
                        for i in 0..64u8 {
                            assert_eq!(read(&store, &[b's', i]), Some(vec![i, i]));
                        }
                    }
                });
            }
            for t in 0..4u8 {
                let store = &store;
                scope.spawn(move || {
                    for i in 0..50u8 {
                        put(store, &[b'w', t, i % 8], &[i]);
                    }
                });
            }
            for _ in 0..20 {
                store.compact().unwrap();
            }
            stop.store(true, Ordering::Relaxed);
        });
        for t in 0..4u8 {
            for i in 0..8u8 {
                let last = if i < 2 { 48 + i } else { 40 + i };
                assert_eq!(read(&store, &[b'w', t, i]), Some(vec![last]));
            }
        }
        store.verify().unwrap();
        drop(store);
        let store = Store::open_with(tmp.path(), segmented(256)).unwrap();
        assert_eq!(read(&store, &[b's', 7]), Some(vec![7, 7]));
        store.verify().unwrap();
    }

    #[test]
    fn a_thread_that_panics_holding_the_writer_lock_poisons_the_store() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
        put(&store, b"a", b"1");
        let panicked = thread::scope(|scope| {
            scope
                .spawn(|| {
                    let _writing = store.writer();
                    panic!("halfway through changing the log");
                })
                .join()
                .is_err()
        });
        assert!(panicked);
        assert!(matches!(
            store.set(key(b"b"), value(b"2")),
            Err(Error::Poisoned)
        ));
        assert!(matches!(store.compact(), Err(Error::Poisoned)));
        assert_eq!(read(&store, b"a").as_deref(), Some(&b"1"[..]));
    }

    #[test]
    fn a_group_that_writes_nothing_does_not_compact() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open_with(tmp.path(), auto(50, 0)).unwrap();
        store.auto_failed.store(true, Ordering::Relaxed);
        put(&store, b"a", b"1");
        put(&store, b"a", b"2");
        store.auto_failed.store(false, Ordering::Relaxed);
        let results = store.commit(&[Write::One(delete_record(b"b"))]);
        assert!(results.iter().all(Result::is_ok));
        assert_eq!(
            log_bytes(tmp.path()).len(),
            42,
            "half dead and not compacted"
        );
        store.commit(&[Write::One(set_record(b"a", b"3"))]);
        assert_eq!(log_bytes(tmp.path()), encoded(&[set_record(b"a", b"3")]));
    }

    /// A store whose next leader expects a group of `last_group` writes
    /// and whose last fsync took `last_sync`.
    fn expecting(dir: &Path, last_group: usize, last_sync: Duration) -> Store {
        let store = Store::open(dir).unwrap();
        hold(&store.queue).last_group = last_group;
        store
            .last_sync_nanos
            .store(last_sync.as_nanos() as u64, Ordering::Relaxed);
        store
    }

    #[test]
    fn a_writer_after_a_group_of_one_does_not_wait() {
        let tmp = tempfile::tempdir().unwrap();
        let store = expecting(tmp.path(), 1, Duration::from_secs(60));
        let started = Instant::now();
        put(&store, b"a", b"1");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "a wait would take 6 s"
        );
    }

    #[test]
    fn a_leader_waits_for_its_group_to_fill_again() {
        let tmp = tempfile::tempdir().unwrap();
        let store = expecting(tmp.path(), 3, Duration::from_secs(60));
        let started = Instant::now();
        thread::scope(|scope| {
            for i in 0..3u8 {
                let store = &store;
                scope.spawn(move || put(store, &[i], &[i]));
            }
        });
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the wait ends at 3 queued"
        );
        assert_eq!(store.commits.load(Ordering::Relaxed), 1);
        assert_eq!(hold(&store.queue).last_group, 3);
    }

    #[test]
    fn a_leader_stops_waiting_after_a_tenth_of_the_last_fsync() {
        let tmp = tempfile::tempdir().unwrap();
        let store = expecting(tmp.path(), 3, Duration::from_millis(200));
        let started = Instant::now();
        put(&store, b"a", b"1");
        assert!(started.elapsed() >= Duration::from_millis(20));
        assert_eq!(store.commits.load(Ordering::Relaxed), 1);
        assert_eq!(
            hold(&store.queue).last_group,
            1,
            "the next leader won't wait"
        );
    }
}
