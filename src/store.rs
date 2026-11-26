use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fs::TryLockError;
use std::io::{self, BufRead, BufReader, ErrorKind, Read};
use std::num::NonZero;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, RwLock, mpsc};
use std::time::{Duration, Instant};
use std::{mem, thread};

use crate::filter::Filter;
use crate::fs::{Fs, LogFile, OsFs, buffer_for};
use crate::hint;
use crate::index::{Entry, Index};
use crate::options::{AutoCompact, Options};
use crate::record::{
    BATCH_PAYLOAD_LEN, Fields, HEADER_LEN, Header, IndexOp, Kind, Op, Place, Record, check_payload,
    decode_header, decode_payload, encode_batch, encode_op, index_op, move_header, record_len,
    starts_a_record,
};
use crate::segment::{
    FILE_HEADER_LEN, FileHeader, Segment, check_file_header, compact_name, create, hint_name,
    log_name, parse_compact_name, parse_hint_name, parse_log_name, start_file,
};
use crate::{Batch, CorruptionReason, Error, Key, UnsupportedReason, Value};

/// The one-file log of tally before segments, which this version refuses.
pub(crate) const LOG_FILE_NAME: &str = "tally.log";
pub(crate) const LOCK_FILE_NAME: &str = "tally.lock";

/// What `Store::repair` moved out of the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Repaired {
    /// Offset of the first bad record, where its segment now ends, or the
    /// base of a segment whose file header was bad, which is now empty.
    pub offset: u64,
    /// Bytes moved out of the log: everything from `offset` to the end of
    /// its segment, or the whole file of a segment whose header was bad.
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

/// What `copy_live` wrote.
struct Copied<L: LogFile> {
    /// The new segment, fsynced, under its temporary name.
    output: Segment<L>,
    /// Its hint, without the trailer.
    hint: hint::Writer<L>,
    /// The bytes copied.
    written: u64,
    /// Where each record copied was, in the order copied.
    moved: Vec<u64>,
    /// The keys of the Delete records that must outlive their segment:
    /// the key isn't live, and a kept segment older than it may hold a Set
    /// of it.
    carry: BTreeSet<Key>,
}

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

/// A write a caller asked for: one operation, or a batch.
pub(crate) enum Write {
    One(Op),
    Batch(Vec<Op>),
}

impl Write {
    fn ops(&self) -> &[Op] {
        match self {
            Write::One(op) => std::slice::from_ref(op),
            Write::Batch(ops) => ops,
        }
    }

    /// About how many bytes it writes, to cap a group.
    fn size(&self) -> usize {
        self.ops().iter().map(|op| op.fields().payload_len()).sum()
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
/// group to fill (DESIGN.md, Group commit).
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
    /// (DESIGN.md, Hint file). A directory written by another version of
    /// tally fails with `Unsupported`. Takes an exclusive lock on
    /// `tally.lock`: a second `open` of the same directory fails with
    /// `Locked` until this `Store` is dropped.
    ///
    /// A torn tail of the newest segment is saved to a side file unless it
    /// is all zeros, then truncated, fsynced and logged as a warning before
    /// `open` returns. Any other bad record `open` reads fails it with its
    /// offset; `repair` handles those (DESIGN.md, Recovery). Records a hint
    /// covers aren't read, so damage to them shows up in `get` instead.
    pub fn open(dir: impl AsRef<Path>) -> Result<Store, Error> {
        Self::open_with(dir, Options::default())
    }

    /// `open`, with `options` in place of the defaults.
    ///
    /// With `auto_compact` set, compacts before returning if the log is due
    /// (see [`AutoCompact`]). A failed automatic compaction logs a warning
    /// and turns automatic compaction off until the next open, and `open`
    /// still succeeds, unless the failure poisoned the store (a failed
    /// directory fsync): then `open` returns it.
    pub fn open_with(dir: impl AsRef<Path>, options: Options) -> Result<Store, Error> {
        Self::open_in(OsFs, dir.as_ref(), options)
    }

    /// Makes a log that `open` refuses openable again, without opening it.
    ///
    /// Returns `None` if `open` would succeed (a torn tail of the newest
    /// segment is left for `open` to truncate). On a corrupt record, saves
    /// every byte from it to the end of its segment in a side file, fsyncs
    /// the side file and the directory, then truncates the segment at the
    /// record and fsyncs it. A segment whose file header is damaged, or cut
    /// short in a sealed segment, goes to the side file whole and is
    /// replaced by an empty one. Unsupported records are an error, not
    /// something to repair: a newer tally can read them. Fails with
    /// `Locked` while a `Store` is open.
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
            segments.push(create(&fs, &dir.join(log_name(0)), 0)?);
        }
        segments.last().expect("one was just made").file.sync()?;
        fs.sync_dir(dir)?;
        fs.sync_dir(parent)?;
        let index = load(&fs, dir, &mut segments)?;
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
            if let Err(err) = store.compact_if_due(&writing)
                && store.writable().is_err()
            {
                return Err(err);
            }
        }
        Ok(store)
    }

    pub(crate) fn repair_in(fs: &F, dir: &Path) -> Result<Option<Repaired>, Error> {
        let _lock = fs.open_or_create(&dir.join(LOCK_FILE_NAME))?;
        lock(&_lock)?;
        let segments = match open_segments(fs, dir) {
            Err(Error::Corruption {
                offset: base,
                reason: CorruptionReason::SegmentHeader | CorruptionReason::CutShort,
            }) => return repair_file_header(fs, dir, base).map(Some),
            segments => segments?,
        };
        let mut index = Index::new();
        let mut bad = None;
        for (i, segment) in segments.iter().enumerate() {
            let newest = i + 1 == segments.len();
            match scan(
                segment,
                &mut index,
                segment.base,
                segment.end(),
                newest,
                &mut |_| Ok(()),
            ) {
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
        let side_file = corrupt_name(dir, offset);
        move_aside(fs, dir, &side_file, segment, offset, len, segment.base)?;
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
    /// into a new segment with a hint, points the index at the copies, then
    /// removes the old segments oldest first, fsyncing the directory after
    /// each. Until then the old segments are untouched, so any failure while
    /// copying returns the error and leaves the store as it was. Checks
    /// every record it reads, so a corrupt record stops compaction with its
    /// offset rather than being copied. Reads go on while it runs; writes
    /// wait for it. A failed directory fsync poisons the store.
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
    /// order) to a new segment at the end of the log, then removes them. A
    /// Delete record is copied too while an older segment that is kept may
    /// hold a Set it hides.
    fn clean(&self, writing: &Writing, chosen: &[u64]) -> Result<(), Error> {
        if chosen.is_empty() {
            return Ok(());
        }
        let segments = self.segments();
        let base = segments.last().expect("there is always a segment").end();
        let copy_path = self.dir.join(compact_name(base));
        let hint_path = self.dir.join(hint_name(base));
        // The copy is complete and durable before it takes a segment's name,
        // so the newest segment never holds more than one append that a
        // crash could tear.
        let copied = self
            .copy_live(&segments, &copy_path, &hint_path, chosen)
            .and_then(|copied| {
                self.fs.rename(&copy_path, &self.dir.join(log_name(base)))?;
                Ok(copied)
            });
        let Copied {
            output,
            hint,
            written,
            moved,
            carry,
        } = match copied {
            Ok(copied) => copied,
            Err(err) => {
                let _ = remove_if_present(&self.fs, &copy_path);
                let _ = remove_if_present(&self.fs, &hint_path);
                return Err(err);
            }
        };
        if let Err(err) = self.fs.sync_dir(&self.dir) {
            return Err(self.poison(err));
        }
        output.set_len_written(written);
        let output = Arc::new(output);
        let mut next = segments.to_vec();
        if next.last().is_some_and(|s| s.base == base) {
            next.pop();
        }
        next.push(Arc::clone(&output));
        self.replace_segments(writing, next);
        // Written only now: nothing orders writes to two files, and a hint
        // must never reach the disk before the copies it lists.
        let repointed = hint
            .finish(written)
            .and_then(|file| file.sync().map(|()| file))
            .map_err(Error::from)
            .and_then(|file| self.repoint(&output, &file, &moved));
        if let Err(err) = repointed {
            let _ = remove_if_present(&self.fs, &hint_path);
            self.count_dead(writing);
            return Err(err);
        }
        let segments = self.segments();
        let mut left_behind = 0usize;
        self.index.for_each(|entry| {
            if chosen.contains(&segments[segment_of(&segments, entry.offset)].base) {
                left_behind += 1;
            }
        });
        assert_eq!(left_behind, 0, "every live key in them is copied");
        for segment in segments.iter() {
            if chosen.contains(&segment.base) {
                segment.set_dead(segment.len());
            }
        }
        if !carry.is_empty() {
            let deletes: Vec<Op> = carry.into_iter().map(|key| Op::Delete { key }).collect();
            let dead = deletes.iter().map(|op| op.fields().record_len()).sum();
            self.append(writing, &deletes.iter().collect::<Vec<_>>())?;
            self.active().add_dead(dead);
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

    /// Writes each live record of the `chosen` segments to a new segment at
    /// `copy_path`, based where the log ends, and its key and lengths to a
    /// hint at `hint_path`, and fsyncs the copy. One buffered pass per
    /// segment: reading the dead records in between costs less than a
    /// positioned read per live one.
    fn copy_live(
        &self,
        segments: &[Arc<Segment<F::File>>],
        copy_path: &Path,
        hint_path: &Path,
        chosen: &[u64],
    ) -> Result<Copied<F::File>, Error> {
        let base = segments.last().expect("there is always a segment").end();
        let output = create(&self.fs, copy_path, base)?;
        let hint_file = self.fs.open_or_create(hint_path)?;
        hint_file.set_len(0)?;
        let mut hint = hint::Writer::new(hint_file);
        let mut moved = Vec::with_capacity(self.index.len());
        let mut carry = BTreeSet::new();
        let mut buffer = Vec::new();
        let mut written = 0u64;
        for input in segments.iter().filter(|s| chosen.contains(&s.base)) {
            each_record(input, input.base, |read| {
                match read.op {
                    IndexOp::Set { key } => {
                        if self
                            .index
                            .get(key.as_bytes())
                            .is_some_and(|entry| entry.offset == read.offset)
                        {
                            moved.push(read.offset);
                            let fields = Fields {
                                place: Place::Top,
                                ..read.header.fields()
                            };
                            hint.push(fields, key.as_bytes())?;
                            move_header(read.header_bytes, base + written + buffer.len() as u64);
                            buffer.extend_from_slice(read.header_bytes);
                            buffer.extend_from_slice(read.payload);
                            if buffer.len() >= COMPACT_BUFFER {
                                output.write_all_at(&buffer, base + written)?;
                                written += buffer.len() as u64;
                                buffer.clear();
                            }
                        }
                    }
                    IndexOp::Delete { key } => {
                        if !self.index.contains(key.as_bytes())
                            && self.kept_set_of(segments, chosen, input.base, key)?
                        {
                            carry.insert(Key::try_from(key.as_bytes()).expect("a key"));
                        }
                    }
                    IndexOp::Batch { .. } => {}
                }
                Ok(())
            })?;
        }
        output.write_all_at(&buffer, base + written)?;
        written += buffer.len() as u64;
        output.file.sync()?;
        Ok(Copied {
            output,
            hint,
            written,
            moved,
            carry,
        })
    }

    /// Whether a segment older than `before` that is not being cleaned may
    /// hold a Set record of `key`, which a Delete record of it at `before`
    /// would hide. Each such segment is sealed: the newest segment is never
    /// older than one being cleaned.
    fn kept_set_of(
        &self,
        segments: &[Arc<Segment<F::File>>],
        chosen: &[u64],
        before: u64,
        key: &Key,
    ) -> Result<bool, Error> {
        for segment in segments
            .iter()
            .filter(|s| s.base < before && !chosen.contains(&s.base))
        {
            if self.sets_of(segment)?.may_hold(key.as_bytes()) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The filter of the keys sealed `segment` holds a Set record for,
    /// built the first time it is asked for: from its hint and the records
    /// after it, or from all its records.
    fn sets_of<'s>(&self, segment: &'s Segment<F::File>) -> Result<&'s Filter, Error> {
        if let Some(filter) = segment.sets.get() {
            return Ok(filter);
        }
        let mut keys = Filter::builder();
        let mut from = segment.base;
        if let Ok(file) = self
            .fs
            .open_existing(&self.dir.join(hint_name(segment.base)))
        {
            let hinted = hint::trailer(&file).and_then(|trailer| {
                if trailer.log_len > segment.len() {
                    return Err(hint::Invalid("the segment is shorter than the hint says"));
                }
                hint::read_entries(&file, trailer, |fields, key, _| {
                    if fields.kind == Kind::Set {
                        keys.add(key);
                    }
                })?;
                Ok(trailer.log_len)
            });
            match hinted {
                Ok(len) => from += len,
                Err(_) => keys.clear(),
            }
        }
        each_record(segment, from, |read| {
            if let IndexOp::Set { key } = read.op {
                keys.add(key.as_bytes());
            }
            Ok(())
        })?;
        Ok(segment.sets.get_or_init(|| keys.build()))
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
        let mut unmatched = false;
        hint::read_entries(hint_file, trailer, |fields, key, offset| {
            let Some(&from) = old.next() else {
                unmatched = true;
                return;
            };
            let entry = Entry::new(output.base + offset, fields);
            if !self.index.replace(key, from, entry) {
                output.add_dead(entry.record_len());
            }
        })
        .map_err(unreadable)?;
        if unmatched || old.next().is_some() {
            return Err(unreadable(hint::Invalid("not one entry per copy")));
        }
        Ok(())
    }

    /// Compacts if automatic compaction is on and due, cleaning the
    /// segments with the most dead bytes until it no longer would be. A
    /// failure is logged and turns automatic compaction off until the next
    /// open; it is returned only for `open`, which fails if it poisoned the
    /// store. After a write, the write has already succeeded.
    fn compact_if_due(&self, writing: &Writing) -> Result<(), Error> {
        let Some(auto) = self.auto_compact else {
            return Ok(());
        };
        if self.auto_failed.load(Ordering::Relaxed) || self.writable().is_err() {
            return Ok(());
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
        self.clean(writing, &chosen).inspect_err(|err| {
            self.auto_failed.store(true, Ordering::Relaxed);
            log::warn!("automatic compaction failed, off until the next open: {err}");
        })
    }

    /// Starts a new, empty segment at the end of the log, and makes its
    /// name durable before anything is written to it.
    fn roll(&self, writing: &Writing) -> Result<(), Error> {
        let segments = self.segments();
        let base = segments.last().expect("there is always a segment").end();
        let segment = create(&self.fs, &self.dir.join(log_name(base)), base)?;
        if let Err(err) = self.fs.sync_dir(&self.dir) {
            // A retry would fsync a new descriptor, which Linux need not
            // tell about this failure.
            return Err(self.poison(err));
        }
        let mut next = segments.to_vec();
        next.push(Arc::new(segment));
        self.replace_segments(writing, next);
        Ok(())
    }

    /// Appends the records of `ops` in one write and fsyncs it, and
    /// returns where each landed. One record is written on its own, and
    /// more inside a Batch record, so a crash keeps all of them or none;
    /// the Batch header counts as dead. Starts a new segment first if the
    /// active one is full, so an append never spans two. A failed write or
    /// fsync is truncated away and the truncation fsynced. A failed fsync
    /// poisons the store, and so does a failed truncation.
    fn append(&self, writing: &Writing, ops: &[&Op]) -> Result<Vec<u64>, Error> {
        let full = self.active().len();
        if full > 0 && full >= self.segment_bytes {
            self.roll(writing)?;
        }
        let active = self.active();
        let start = active.end();
        let framed = ops.len() > 1;
        let frame_len = if framed {
            record_len(0, BATCH_PAYLOAD_LEN)
        } else {
            0
        };
        let place = if framed { Place::Inner } else { Place::Top };
        let mut buffer = vec![0u8; frame_len as usize];
        let mut offsets = Vec::with_capacity(ops.len());
        for op in ops {
            let offset = start + buffer.len() as u64;
            offsets.push(offset);
            encode_op(op, place, offset, &mut buffer);
        }
        if framed {
            let mut frame = Vec::with_capacity(frame_len as usize);
            encode_batch(buffer.len() as u64 - frame_len, start, &mut frame);
            buffer[..frame.len()].copy_from_slice(&frame);
        }
        if let Err(err) = active.write_all_at(&buffer, start) {
            if let Err(truncate) = active.set_len(start).and_then(|()| active.sync()) {
                self.poison(truncate);
            }
            return Err(err.into());
        }
        let started = Instant::now();
        if let Err(err) = active.file.sync() {
            // Linux can mark the pages clean while reads still see them, so
            // a reopen would take the group as written and a later fsync
            // would not write it, leaving a hole before later writes.
            let _ = active.set_len(start).and_then(|()| active.sync());
            return Err(self.poison(err));
        }
        let took = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.last_sync_nanos.store(took, Ordering::Relaxed);
        #[cfg(test)]
        self.commits.fetch_add(1, Ordering::Relaxed);
        active.set_len_written(active.len() + buffer.len() as u64);
        active.add_dead(frame_len);
        Ok(offsets)
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
        self.submit(Write::One(Op::Set { key, value }))
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
        self.submit(Write::One(Op::Delete { key }))
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
            let _ = self.compact_if_due(&writing);
        }
    }

    /// Writes a group with one append and one fsync, then updates the index.
    /// Returns each write's result in order, and whether it wrote anything.
    fn write_group(&self, writing: &Writing, writes: &[Write]) -> (Vec<Result<(), Error>>, bool) {
        if let Err(err) = self.writable() {
            return (each(err, writes.len()), false);
        }
        let mut live = HashMap::new();
        let ops: Vec<&Op> = writes
            .iter()
            .flat_map(Write::ops)
            .filter(|op| takes_effect(&self.index, op, &mut live))
            .collect();
        if ops.is_empty() {
            return (writes.iter().map(|_| Ok(())).collect(), false);
        }
        let offsets = match self.append(writing, &ops) {
            Ok(offsets) => offsets,
            Err(err) => return (each(err, writes.len()), false),
        };
        let segments = self.segments();
        for (op, offset) in ops.into_iter().zip(offsets) {
            self.indexed(&segments, op, offset);
        }
        (writes.iter().map(|_| Ok(())).collect(), true)
    }

    /// Updates the index and the dead bytes for the record of `op`, now
    /// durable at `offset`. A Delete's key must be live. Only the writer
    /// calls it.
    fn indexed(&self, segments: &[Arc<Segment<F::File>>], op: &Op, offset: u64) {
        let died = |at: u64, len: u64| segments[segment_of(segments, at)].add_dead(len);
        let key = op.key().as_bytes();
        match op {
            Op::Set { .. } => {
                if let Some(old) = self.index.insert(key, Entry::new(offset, op.fields())) {
                    died(old.offset, old.record_len());
                }
            }
            Op::Delete { .. } => {
                let old = self.index.remove(key).expect("deleted keys are live");
                died(old.offset, old.record_len());
                died(offset, op.fields().record_len());
            }
        }
    }

    /// Returns the value for `key`, or `None` if it isn't live.
    ///
    /// Reads the record in one positioned read and verifies both checksums
    /// and that the stored key equals `key`. Any failure is a corruption
    /// error at the record's offset, never `None`. Takes no lock while it
    /// reads, so it runs alongside writes and compaction.
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
                false,
                &mut |_| Ok(()),
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

/// Whether `op` writes anything, given the keys a group has already set
/// (`true`) or deleted (`false`) in `live`, which it then updates: a
/// delete of a key that isn't live writes nothing.
fn takes_effect<'w>(index: &Index, op: &'w Op, live: &mut HashMap<&'w [u8], bool>) -> bool {
    let key = op.key().as_bytes();
    let is_set = matches!(op, Op::Set { .. });
    let was_live = live
        .get(key)
        .copied()
        .unwrap_or_else(|| index.contains(key));
    if !is_set && !was_live {
        return false;
    }
    live.insert(key, is_set);
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

/// Calls `each` with every record of `segment` from `start` to its end, a
/// batch's records as their own, checked as `scan` checks them.
fn each_record<L: LogFile>(
    segment: &Segment<L>,
    start: u64,
    mut each: impl FnMut(Scanned) -> Result<(), Error>,
) -> Result<(), Error> {
    scan_into(
        segment,
        start,
        segment.end(),
        COMPACT_BUFFER,
        &mut |_, _| {},
        &mut each,
    )
}

/// The shortest record there can be: a one-byte key and no value.
const SHORTEST_RECORD: u64 = HEADER_LEN as u64 + 1;

/// Reads a record's header into `buf`, copying it straight from the
/// buffer when the buffer holds it whole.
fn read_header<R: Read>(reader: &mut BufReader<R>, buf: &mut [u8; HEADER_LEN]) -> io::Result<()> {
    if let Some(buffered) = reader.buffer().get(..HEADER_LEN) {
        buf.copy_from_slice(buffered);
        reader.consume(HEADER_LEN);
        return Ok(());
    }
    reader.read_exact(buf)
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
    let reread = || load_record(file, entry.offset);
    match file.read_exact_at(&mut buf, entry.offset) {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::UnexpectedEof => return reread(),
        Err(err) => return Err(err.into()),
    }
    let header_bytes = buf[..HEADER_LEN].try_into().unwrap();
    let header = decode_header(header_bytes, entry.offset).map_err(|err| err.at(entry.offset))?;
    if header.record_len() != len {
        return reread();
    }
    decode_payload(header, &buf[HEADER_LEN..]).map_err(|err| err.at(entry.offset))
}

fn load_record(file: &impl LogFile, offset: u64) -> Result<Record, Error> {
    let mut header_bytes = [0u8; HEADER_LEN];
    file.read_exact_at(&mut header_bytes, offset)?;
    let header = decode_header(&header_bytes, offset).map_err(|err| err.at(offset))?;
    let mut payload = vec![0u8; header.payload_len()];
    file.read_exact_at(&mut payload, offset + HEADER_LEN as u64)?;
    decode_payload(header, &payload).map_err(|err| err.at(offset))
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

/// Where `repair` saves the bytes from log offset `offset`.
fn corrupt_name(dir: &Path, offset: u64) -> PathBuf {
    dir.join(format!("{LOG_FILE_NAME}.corrupt-{offset}"))
}

/// Copies the bytes of `file` from `offset` to `len` to `side_file`, fsyncs
/// it, removes the hint of the segment at `base`, and fsyncs the
/// directory, so the bytes are durable outside the log before they leave
/// it.
fn move_aside<F: Fs>(
    fs: &F,
    dir: &Path,
    side_file: &Path,
    file: &impl LogFile,
    offset: u64,
    len: u64,
    base: u64,
) -> Result<(), Error> {
    let side = match fs.open_existing(side_file) {
        Ok(existing) => {
            if !starts_tail(&existing, file, offset, len)? {
                return Err(Error::SideFileExists(side_file.to_path_buf()));
            }
            existing
        }
        Err(err) if err.kind() == ErrorKind::NotFound => fs.open_or_create(side_file)?,
        Err(err) => return Err(err.into()),
    };
    side.set_len(0)?;
    copy_to_end(file.reader_at(offset).take(len - offset), &side)?;
    side.sync()?;
    remove_if_present(fs, &dir.join(hint_name(base)))?;
    fs.sync_dir(dir)?;
    Ok(())
}

/// Repairs a sealed segment at `base` whose file header is damaged or cut
/// short: none of its records can be trusted at the offsets they claim. The
/// whole file, header included, goes to the side file, and an empty segment
/// is renamed into its place.
fn repair_file_header<F: Fs>(fs: &F, dir: &Path, base: u64) -> Result<Repaired, Error> {
    let path = dir.join(log_name(base));
    let file = fs.open_existing(&path)?;
    let len = file.size()?;
    let side_file = corrupt_name(dir, base);
    move_aside(fs, dir, &side_file, &file, 0, len, base)?;
    let empty = dir.join(compact_name(base));
    create(fs, &empty, base)?;
    fs.rename(&empty, &path)?;
    fs.sync_dir(dir)?;
    Ok(Repaired {
        offset: base,
        dropped: len,
        side_file,
    })
}

/// Opens every segment in `dir`, in log order, after removing hints whose
/// segment is gone and copies a compaction left unfinished. Refuses a
/// directory another version of tally wrote, and segments that overlap.
/// A newest segment whose header a crash cut short is started again: no
/// record was written to it before its header was fsynced.
fn open_segments<F: Fs>(fs: &F, dir: &Path) -> Result<Vec<Segment<F::File>>, Error> {
    let names = fs.list(dir)?;
    if names.iter().any(|n| n == LOG_FILE_NAME) {
        return Err(Error::Unsupported {
            offset: 0,
            reason: UnsupportedReason::OtherFormat,
        });
    }
    let mut bases: Vec<u64> = names.iter().filter_map(|n| parse_log_name(n)).collect();
    bases.sort_unstable();
    let mut changed = false;
    for name in &names {
        let orphan = parse_hint_name(name).is_some_and(|base| bases.binary_search(&base).is_err());
        if orphan || parse_compact_name(name).is_some() {
            fs.remove(&dir.join(name))?;
            changed = true;
        }
    }
    if changed {
        fs.sync_dir(dir)?;
    }
    let mut segments: Vec<Segment<F::File>> = Vec::with_capacity(bases.len());
    for (i, &base) in bases.iter().enumerate() {
        let file = fs.open_existing(&dir.join(log_name(base)))?;
        let corrupt = |reason| Error::Corruption {
            offset: base,
            reason,
        };
        match check_file_header(&file, base)? {
            FileHeader::Valid => {}
            FileHeader::Torn if i + 1 == bases.len() => start_file(&file, base)?,
            FileHeader::Torn => return Err(corrupt(CorruptionReason::CutShort)),
            FileHeader::Damaged => return Err(corrupt(CorruptionReason::SegmentHeader)),
            FileHeader::Foreign => {
                return Err(Error::Unsupported {
                    offset: base,
                    reason: UnsupportedReason::OtherFormat,
                });
            }
        }
        if let Some(before) = segments.last()
            && before.end() > base
        {
            return Err(corrupt(CorruptionReason::SegmentsOverlap));
        }
        let len = file.size()? - FILE_HEADER_LEN;
        segments.push(Segment::new(base, file, len));
    }
    Ok(segments)
}

/// Builds the index from every segment in order: from its hint and the
/// records after it, or from all its records. Truncates a torn tail of the
/// newest segment; in any other, one is `CutShort` corruption. A hint that
/// can't be used is logged, removed for good, and the whole index built
/// again without it, since what it put in the index can't be told apart.
fn load<F: Fs>(fs: &F, dir: &Path, segments: &mut [Segment<F::File>]) -> Result<Index, Error> {
    loop {
        match try_load(fs, dir, segments)? {
            Loaded::Index(index) => return Ok(index),
            Loaded::BadHint(base, hint::Invalid(why)) => {
                log::warn!(
                    "ignored the hint for the segment at {base} ({why}); reading the segment instead"
                );
                fs.remove(&dir.join(hint_name(base)))?;
                fs.sync_dir(dir)?;
            }
        }
    }
}

enum Loaded {
    Index(Index),
    /// The hint of the segment at this base can't be used.
    BadHint(u64, hint::Invalid),
}

fn try_load<F: Fs>(fs: &F, dir: &Path, segments: &mut [Segment<F::File>]) -> Result<Loaded, Error> {
    let mut hints = Vec::with_capacity(segments.len());
    // The largest hint's Set count, not their sum: a key overwritten in
    // every segment is counted in each.
    let mut count = 0u64;
    for segment in segments.iter() {
        let hint = match fs.open_existing(&dir.join(hint_name(segment.base))) {
            Ok(file) => match hint::trailer(&file) {
                Ok(trailer) => {
                    count = count.max(trailer.count.min(trailer.most_sets));
                    Some(file)
                }
                Err(invalid) => return Ok(Loaded::BadHint(segment.base, invalid)),
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
            Some(file) => match read_hint(file, segment, &mut index, WINDOW, PARALLEL_FROM) {
                Ok((hinted, tail)) => (hinted, Some(tail)),
                Err(invalid) => return Ok(Loaded::BadHint(segment.base, invalid)),
            },
            None => (segment.base, None),
        };
        let len = segment.end();
        // A sealed segment never changes, so a hint for all of it stays
        // valid. Not fsynced: a hint that didn't land whole fails its
        // checksum, and the next open removes it and reads the segment.
        let mut writer = None;
        if i < newest && hinted < len {
            let opened = match (hint, tail) {
                (Some(file), Some(tail)) => hint::Writer::extend(file, tail),
                _ => fs
                    .open_or_create(&dir.join(hint_name(segment.base)))
                    .map(hint::Writer::new),
            };
            match opened {
                Ok(opened) => writer = Some(opened),
                Err(err) => hint_not_written(segment.base, &err),
            }
        }
        let end = scan(
            &*segment,
            &mut index,
            hinted,
            len,
            i == newest,
            &mut |read| {
                if let Some(hint) = &mut writer
                    && let Err(err) = hint.push(read.header.fields(), read.op.key())
                {
                    hint_not_written(segment.base, &err);
                    writer = None;
                }
                Ok(())
            },
        )?;
        if let Some(hint) = writer.take()
            && end == len
            && let Err(err) = hint.finish(len - segment.base)
        {
            hint_not_written(segment.base, &err);
        }
        if end < len {
            let saved = save_torn(fs, dir, &*segment, end, len)?;
            segment.set_len(end)?;
            segment.file.sync()?;
            segment.set_len_written(end - segment.base);
            match saved {
                Some(path) => log::warn!(
                    "truncated a torn tail at offset {end}: moved {} bytes to {}",
                    len - end,
                    path.display()
                ),
                None => log::warn!(
                    "truncated a torn tail at offset {end}: dropped {} zero bytes",
                    len - end
                ),
            }
        }
    }
    Ok(Loaded::Index(index))
}

fn hint_not_written(base: u64, err: &io::Error) {
    log::warn!(
        "could not write the hint for the segment at {base} ({err}); the next open reads it"
    );
}

/// Hints with fewer Set entries than this are read on one thread.
const PARALLEL_FROM: u64 = 100_000;
const MAX_THREADS: usize = 8;

/// One thread for less `work` than `from`, else up to `MAX_THREADS`.
fn threads_for(work: u64, from: u64) -> usize {
    if work < from {
        return 1;
    }
    thread::available_parallelism()
        .map_or(1, NonZero::get)
        .min(MAX_THREADS)
}

/// Applies the records `segment`'s hint `file` describes to `index`, in
/// order, `window` at a time: on one thread below `parallel_from` Set
/// entries, on up to `MAX_THREADS` from there. Returns the log offset the
/// hint covers to, and where to extend it from. On `Err`, what it applied
/// must be thrown away.
fn read_hint<L: LogFile>(
    file: &impl LogFile,
    segment: &Segment<L>,
    index: &mut Index,
    window: usize,
    parallel_from: u64,
) -> Result<(u64, hint::Tail), hint::Invalid> {
    let trailer = hint::trailer(file)?;
    if trailer.log_len > segment.len() {
        return Err(hint::Invalid("the segment is shorter than the hint says"));
    }
    let threads = threads_for(trailer.count, parallel_from);
    let base = segment.base;
    let mut changes = Window::default();
    let tail = hint::read_entries(file, trailer, |fields, key, offset| {
        let entry = match fields.kind {
            Kind::Set => Some(Entry::new(base + offset, fields)),
            Kind::Delete => None,
            Kind::Batch => return,
        };
        changes.push(key, entry);
        if changes.changes.len() == window {
            mem::take(&mut changes).apply_to(index, threads);
        }
    })?;
    changes.apply_to(index, threads);
    Ok((base + trailer.log_len, tail))
}

/// Scans of fewer bytes than this apply each record as it is read, on one
/// thread.
const PARALLEL_SCAN_FROM: u64 = 16 << 20;
/// Records a parallel scan or hint read takes before applying them.
const WINDOW: usize = 1 << 16;
const SCAN_BUFFER: usize = 1 << 20;

/// Applies the records from `start` to `len` to `index`, and returns where
/// they end. In the newest segment, damage at an offset after which no
/// record on its own starts is a torn tail: the bytes of an append a crash
/// cut short, in any order. The scan returns where it starts. Any other
/// damage is an error at its offset. A batch is applied only once all of
/// it has been read and checked, and an error inside one is reported at
/// its start, so `repair` moves the whole batch. From `PARALLEL_SCAN_FROM`
/// bytes, the index takes the records a window at a time on up to
/// `MAX_THREADS` threads. Calls `record` with every record read, in log
/// order, Batch records too, including any in a torn tail; an error from
/// it stops the scan.
fn scan(
    file: &impl LogFile,
    index: &mut Index,
    start: u64,
    len: u64,
    newest: bool,
    record: &mut impl FnMut(Scanned) -> Result<(), Error>,
) -> Result<u64, Error> {
    let threads = threads_for(len - start, PARALLEL_SCAN_FROM);
    match scan_on(file, index, start, len, threads, record) {
        Err(Error::Corruption { offset, .. })
            if newest && !record_starts_after(file, offset, len)? =>
        {
            Ok(offset)
        }
        result => result.map(|()| len),
    }
}

/// Whether a record on its own starts after `offset` and before `len`, by
/// its header checksum alone: if one does, the damage at `offset` is in
/// the middle of the log, not the end of it. A record this version can't
/// read counts, so a newer version's records are never cut. Records inside
/// a batch don't, so the bytes of a batch a crash tore never look like a
/// later append.
fn record_starts_after(file: &impl LogFile, offset: u64, len: u64) -> io::Result<bool> {
    const CHUNK: u64 = 1 << 20;
    let mut buf = Vec::new();
    let mut start = offset + 1;
    while len.saturating_sub(start) >= HEADER_LEN as u64 {
        let n = (len - start).min(CHUNK + HEADER_LEN as u64 - 1) as usize;
        buf.resize(n, 0);
        file.read_exact_at(&mut buf, start)?;
        for (i, window) in buf.windows(HEADER_LEN).enumerate() {
            if starts_a_record(window.try_into().unwrap(), start + i as u64) {
                return Ok(true);
            }
        }
        start += (n - HEADER_LEN + 1) as u64;
    }
    Ok(false)
}

/// Saves `file`'s bytes from `offset` to `len` to a side file named for
/// where they were and their CRC32C, fsynced with its name, unless every
/// one is zero. A rerun after a crash rewrites the same file, and a later
/// torn tail at the same offset gets its own.
fn save_torn<F: Fs>(
    fs: &F,
    dir: &Path,
    file: &impl LogFile,
    offset: u64,
    len: u64,
) -> Result<Option<PathBuf>, Error> {
    let mut checksum = 0u32;
    let mut zeros = true;
    let mut chunk = vec![0u8; 64 * 1024];
    let mut reader = file.reader_at(offset).take(len - offset);
    loop {
        let n = match reader.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => n,
            Err(err) if err.kind() == ErrorKind::Interrupted => continue,
            Err(err) => return Err(err.into()),
        };
        zeros &= chunk[..n].iter().all(|&b| b == 0);
        checksum = crc32c::crc32c_append(checksum, &chunk[..n]);
    }
    if zeros {
        return Ok(None);
    }
    let path = dir.join(format!("{LOG_FILE_NAME}.torn-{offset}-{checksum:08x}"));
    let side = fs.open_or_create(&path)?;
    side.set_len(0)?;
    copy_to_end(file.reader_at(offset).take(len - offset), &side)?;
    side.sync()?;
    fs.sync_dir(dir)?;
    Ok(Some(path))
}

/// On more than one thread, this one reads while another applies what it
/// read, a window at a time, on `threads` more.
fn scan_on(
    file: &impl LogFile,
    index: &mut Index,
    start: u64,
    len: u64,
    threads: usize,
    record: &mut impl FnMut(Scanned) -> Result<(), Error>,
) -> Result<(), Error> {
    if threads == 1 {
        return scan_into(
            file,
            start,
            len,
            SCAN_BUFFER,
            &mut |key, entry| apply(index, &key, entry),
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
        let scanned = scan_into(
            file,
            start,
            len,
            SCAN_BUFFER,
            &mut |key, entry| {
                window.push(key.as_bytes(), entry);
                if window.changes.len() == WINDOW {
                    sent(mem::take(&mut window));
                }
            },
            record,
        );
        // Sent even after an error: the records in it all come before the
        // damage, which may be a torn tail.
        sent(window);
        // The applier stops once the channel closes, and the scope waits
        // for it.
        drop(send);
        scanned
    })
}

/// A record a scan read: where it is, its header and operation, and its
/// bytes as read, which the callee may change.
struct Scanned<'r> {
    offset: u64,
    header: Header,
    op: &'r IndexOp,
    header_bytes: &'r mut [u8; HEADER_LEN],
    payload: &'r [u8],
}

/// Reads the records from `start` to `len` through a buffer of up to
/// `buffer` bytes. Calls `record` with every record read, and `sink` with
/// the key and new index entry, `None` for a Delete, of each Set and Delete
/// to apply, in order: a batch's once all of it has been read.
fn scan_into(
    file: &impl LogFile,
    start: u64,
    len: u64,
    buffer: usize,
    sink: &mut impl FnMut(Key, Option<Entry>),
    record: &mut impl FnMut(Scanned) -> Result<(), Error>,
) -> Result<(), Error> {
    let mut reader = Reader {
        inner: BufReader::with_capacity(buffer_for(len - start, buffer), file.reader_at(start)),
        header_bytes: [0u8; HEADER_LEN],
        payload: Vec::new(),
    };
    let mut batch = Vec::new();
    let mut offset = start;
    while offset < len {
        let (header, op) = reader.next(offset, len, Place::Top)?;
        reader.show(offset, header, &op, record)?;
        let record_len = header.record_len();
        let IndexOp::Batch { body_len } = op else {
            let (key, entry) = change(offset, header, op);
            sink(key, entry);
            offset += record_len;
            continue;
        };
        let body_start = offset + record_len;
        if len - body_start < body_len {
            return Err(Error::Corruption {
                offset,
                reason: CorruptionReason::CutShort,
            });
        }
        let body_end = body_start + body_len;
        let mut at = body_start;
        while at < body_end {
            let (header, op) = reader
                .next(at, body_end, Place::Inner)
                .map_err(|err| err.moved_to(offset))?;
            reader
                .show(at, header, &op, record)
                .map_err(|err| err.moved_to(offset))?;
            batch.push(change(at, header, op));
            at += header.record_len();
        }
        for (key, entry) in batch.drain(..) {
            sink(key, entry);
        }
        offset = body_end;
    }
    Ok(())
}

/// The key of the Set or Delete record at `offset`, and its index entry,
/// `None` for a Delete.
fn change(offset: u64, header: Header, op: IndexOp) -> (Key, Option<Entry>) {
    match op {
        IndexOp::Set { key } => (key, Some(Entry::new(offset, header.fields()))),
        IndexOp::Delete { key } => (key, None),
        IndexOp::Batch { .. } => unreachable!("scan_into reads a batch's records itself"),
    }
}

fn bad_batch_at(offset: u64) -> Error {
    Error::Corruption {
        offset,
        reason: CorruptionReason::BadBatch,
    }
}

/// Records a scan has read and not yet applied, with their keys' bytes end
/// to end in `keys`, and where each key ends.
#[derive(Default)]
struct Window {
    keys: Vec<u8>,
    changes: Vec<(usize, Option<Entry>)>,
}

impl Window {
    fn push(&mut self, key: &[u8], entry: Option<Entry>) {
        self.keys.extend_from_slice(key);
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

fn apply(index: &mut Index, key: &Key, entry: Option<Entry>) {
    match entry {
        Some(entry) => {
            index.insert(key.as_bytes(), entry);
        }
        None => {
            index.remove(key.as_bytes());
        }
    }
}

/// Reads records one after another for `scan_into`.
struct Reader<R> {
    inner: BufReader<R>,
    header_bytes: [u8; HEADER_LEN],
    payload: Vec<u8>,
}

impl<R: Read> Reader<R> {
    /// Reads the record at `offset`, which must be where the reader is, and
    /// checks it ends by `end` and sits in `place`. One that runs past the
    /// end of its batch breaks the batch; one past the end of the file was
    /// cut short.
    fn next(&mut self, offset: u64, end: u64, place: Place) -> Result<(Header, IndexOp), Error> {
        let cut_short = Error::Corruption {
            offset,
            reason: match place {
                Place::Top => CorruptionReason::CutShort,
                Place::Inner => CorruptionReason::BadBatch,
            },
        };
        if end - offset < HEADER_LEN as u64 {
            return Err(cut_short);
        }
        read_header(&mut self.inner, &mut self.header_bytes)?;
        let header = decode_header(&self.header_bytes, offset).map_err(|err| err.at(offset))?;
        if header.fields().place != place {
            return Err(bad_batch_at(offset));
        }
        if end - offset < header.record_len() {
            return Err(cut_short);
        }
        self.payload.resize(header.payload_len(), 0u8);
        self.inner.read_exact(&mut self.payload)?;
        check_payload(header, &self.payload).map_err(|err| err.at(offset))?;
        Ok((header, index_op(header, &self.payload)))
    }

    /// Shows `record` the record `next` just read.
    fn show(
        &mut self,
        offset: u64,
        header: Header,
        op: &IndexOp,
        record: &mut impl FnMut(Scanned) -> Result<(), Error>,
    ) -> Result<(), Error> {
        record(Scanned {
            offset,
            header,
            op,
            header_bytes: &mut self.header_bytes,
            payload: &self.payload,
        })
    }
}

/// Removes `path`. Returns whether it was there.
fn remove_if_present(fs: &impl Fs, path: &Path) -> io::Result<bool> {
    match fs.remove(path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{self, OpenOptions};
    use std::os::unix::fs::FileExt;
    use std::path::PathBuf;

    use super::*;
    use crate::mem_fs::{Cut, MemFs, Torn};
    use crate::record::tests::{DELETE_A, SET_A_HI};
    use crate::record::{Record, encode, seal};
    use crate::segment::segment_file;
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

    /// The newest segment, or the first one before the first open.
    fn log_path(dir: &Path) -> PathBuf {
        dir.join(log_name(segment_bases(dir).last().copied().unwrap_or(0)))
    }

    /// The records of the segment at `base`, after checking its header.
    fn segment_records(dir: &Path, base: u64) -> Vec<u8> {
        let bytes = fs::read(dir.join(log_name(base))).unwrap();
        let header = FILE_HEADER_LEN as usize;
        assert_eq!(
            bytes[..header],
            segment_file(base, b"")[..],
            "header of {base}"
        );
        bytes[header..].to_vec()
    }

    /// Every segment's records, in log order.
    fn log_bytes(dir: &Path) -> Vec<u8> {
        segment_bases(dir)
            .into_iter()
            .flat_map(|base| segment_records(dir, base))
            .collect()
    }

    /// Writes a segment at `base` holding `records`.
    fn write_segment(dir: &Path, base: u64, records: &[u8]) {
        fs::write(dir.join(log_name(base)), segment_file(base, records)).unwrap();
    }

    /// Overwrites bytes in the newest segment behind the store's back, at
    /// an offset from its first record.
    fn write_log_at(dir: &Path, offset: u64, bytes: &[u8]) {
        let file = OpenOptions::new().write(true).open(log_path(dir)).unwrap();
        file.write_all_at(bytes, FILE_HEADER_LEN + offset).unwrap();
    }

    /// The records of `ops`, each on its own, the first at `start`.
    fn encoded_at(start: u64, ops: &[Op]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for op in ops {
            encode_op(op, Place::Top, start + bytes.len() as u64, &mut bytes);
        }
        bytes
    }

    fn encoded(ops: &[Op]) -> Vec<u8> {
        encoded_at(0, ops)
    }

    /// The records of `ops` inside one Batch record at `start`, as an
    /// append of more than one writes them.
    fn batched_at(start: u64, ops: &[Op]) -> Vec<u8> {
        let frame = record_len(0, BATCH_PAYLOAD_LEN);
        let mut body = Vec::new();
        for op in ops {
            let offset = start + frame + body.len() as u64;
            encode_op(op, Place::Inner, offset, &mut body);
        }
        let mut bytes = Vec::new();
        encode_batch(body.len() as u64, start, &mut bytes);
        bytes.extend(body);
        bytes
    }

    /// A record encoded on its own, moved to `offset`.
    fn moved(record: &[u8], offset: u64) -> Vec<u8> {
        let mut bytes = record.to_vec();
        seal(&mut bytes[..HEADER_LEN], offset);
        bytes
    }

    /// Side files `open` saved torn tails to, with their bytes.
    fn torn_files(dir: &Path) -> Vec<(String, Vec<u8>)> {
        file_names(dir)
            .into_iter()
            .filter(|name| name.starts_with("tally.log.torn-"))
            .map(|name| {
                let bytes = fs::read(dir.join(&name)).unwrap();
                (name, bytes)
            })
            .collect()
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
        write_segment(tmp.path(), 0, &bytes);
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
        write_segment(tmp.path(), 0, bytes);
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

    // recovery: a torn tail is saved, unless it is zeros, and truncated

    #[test]
    fn open_moves_a_torn_record_to_a_side_file_and_truncates_it() {
        for cut in 1..DELETE_A.len() {
            let tmp = store_with_log(&set_a_hi_then(&DELETE_A[..cut]));
            let store = Store::open(tmp.path()).unwrap();
            assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
            assert_eq!(log_bytes(tmp.path()), SET_A_HI, "cut at {cut}");
            let crc = crc32c::crc32c(&DELETE_A[..cut]);
            assert_eq!(
                torn_files(tmp.path()),
                [(
                    format!("tally.log.torn-22-{crc:08x}"),
                    DELETE_A[..cut].to_vec()
                )],
                "cut at {cut}"
            );
        }
    }

    #[test]
    fn a_record_whose_end_landed_without_its_start_is_a_torn_tail() {
        let mut torn = DELETE_A.to_vec();
        torn[..10].fill(0);
        let tmp = store_with_log(&set_a_hi_then(&torn));
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        assert_eq!(log_bytes(tmp.path()), SET_A_HI);
        assert_eq!(torn_files(tmp.path())[0].1, torn);
    }

    #[test]
    fn a_record_this_version_cannot_read_after_damage_keeps_the_damage_from_being_torn() {
        let mut unknown = encoded_at(42, &[set_record(b"b", b"2")]);
        unknown[4] = 0x34;
        seal(&mut unknown[..HEADER_LEN], 42);
        let mut over_limit = encoded_at(42, &[set_record(b"b", b"2")]);
        over_limit[7..11].copy_from_slice(&u32::MAX.to_le_bytes());
        seal(&mut over_limit[..HEADER_LEN], 42);
        for later in [unknown, over_limit] {
            let mut damaged = set_a_hi_then(&DELETE_A);
            damaged[22 + 19] ^= 0x01;
            damaged.extend(later);
            assert_open_fails_untouched(&damaged, 22, CorruptionReason::PayloadChecksum);
        }
    }

    #[test]
    fn a_valid_record_at_another_offset_fails_its_header_checksum() {
        let bytes = [&SET_A_HI[..], &SET_A_HI, &moved(&DELETE_A, 44)].concat();
        assert_open_fails_untouched(&bytes, 22, CorruptionReason::HeaderChecksum);
    }

    #[test]
    fn an_empty_value_is_stored_and_read_back() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"")).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"")));
        drop(store);
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"")));
        assert_eq!(log_bytes(tmp.path()).len(), 20);
    }

    #[test]
    fn the_side_file_of_a_torn_tail_is_named_for_its_offset_and_checksum() {
        let tmp = store_with_log(&set_a_hi_then(&[0xab; 20]));
        Store::open(tmp.path()).unwrap();
        assert_eq!(log_bytes(tmp.path()), SET_A_HI);
        // CRC32C of twenty 0xab bytes, computed in Python.
        assert_eq!(
            torn_files(tmp.path()),
            [("tally.log.torn-22-2d3e2e45".to_string(), vec![0xab; 20])]
        );
    }

    #[test]
    fn damage_to_the_last_record_is_a_torn_tail() {
        let mut bytes = set_a_hi_then(&DELETE_A);
        bytes[22 + 19] ^= 0x01;
        let tmp = store_with_log(&bytes);
        Store::open(tmp.path()).unwrap();
        assert_eq!(log_bytes(tmp.path()), SET_A_HI);
        assert_eq!(torn_files(tmp.path())[0].1, &bytes[22..]);
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
        assert_eq!(torn_files(tmp.path()), [], "zeros are not saved");
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

    // recovery: damage before a later record fails open and leaves the log
    // alone

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
        let bytes = [SET_A_HI.as_slice(), &[0u8; 20], &moved(&DELETE_A, 42)].concat();
        assert_open_fails_untouched(&bytes, 22, CorruptionReason::HeaderChecksum);
    }

    #[test]
    fn open_fails_on_a_zero_payload_before_a_valid_record() {
        let bytes = [
            SET_A_HI.as_slice(),
            &delete_a_with_a_zero_payload(),
            &moved(&DELETE_A, 42),
        ]
        .concat();
        assert_open_fails_untouched(&bytes, 22, CorruptionReason::PayloadChecksum);
    }

    /// SET_A_HI, DELETE_A, then a Set of "b" at 42.
    fn three_records() -> Vec<u8> {
        let mut bytes = set_a_hi_then(&DELETE_A);
        bytes.extend(encoded_at(42, &[set_record(b"b", b"2")]));
        bytes
    }

    #[test]
    fn open_fails_on_a_corrupted_header_checksum_with_its_offset() {
        let mut bytes = three_records();
        bytes[22] ^= 0x01;
        assert_open_fails_untouched(&bytes, 22, CorruptionReason::HeaderChecksum);
    }

    #[test]
    fn open_fails_on_a_corrupted_length_with_its_offset() {
        let mut bytes = three_records();
        bytes[22 + 7] ^= 0x01; // second record's value_len: 0 -> 1
        assert_open_fails_untouched(&bytes, 22, CorruptionReason::HeaderChecksum);
    }

    #[test]
    fn open_fails_on_a_corrupted_key_with_its_offset() {
        let mut bytes = three_records();
        bytes[22 + 19] ^= 0x01; // second record's key: "a" -> "`"
        assert_open_fails_untouched(&bytes, 22, CorruptionReason::PayloadChecksum);
    }

    #[test]
    fn open_fails_on_an_unsupported_tail_record_and_leaves_it() {
        let mut bytes = set_a_hi_then(&DELETE_A);
        bytes[22 + 4] = 0x19; // second record's type: unknown
        seal(&mut bytes[22..22 + HEADER_LEN], 22);
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

    // repair

    fn side_file(dir: &Path, offset: u64) -> PathBuf {
        dir.join(format!("{LOG_FILE_NAME}.corrupt-{offset}"))
    }

    /// SET_A_HI, then a Delete of "a" with its key corrupted, then a Set of
    /// "b": the bad record at 22 hides the valid one after it.
    fn log_with_a_corrupt_middle() -> Vec<u8> {
        let mut bytes = three_records();
        bytes[22 + HEADER_LEN] ^= 0x01;
        bytes
    }

    #[test]
    fn repair_of_an_intact_log_does_nothing() {
        let bytes = set_a_hi_then(&DELETE_A);
        let tmp = store_with_log(&bytes);
        assert_eq!(Store::repair(tmp.path()).unwrap(), None);
        assert_eq!(log_bytes(tmp.path()), bytes);
        assert_eq!(
            file_names(tmp.path()),
            ["tally.00000000000000000000.log", "tally.lock"]
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
        seal(&mut bytes[22..22 + HEADER_LEN], 22);
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
        write_log_at(tmp.path(), 0, &encoded(&[set_record(b"b", b"hi")]));
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
        write_log_at(tmp.path(), 0, &moved(&DELETE_A, 0));
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

    fn set_record(k: &[u8], v: &[u8]) -> Op {
        Op::Set {
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

        assert_eq!(segment_bases(tmp.path()), [105]);
        assert_eq!(
            log_bytes(tmp.path()),
            encoded_at(105, &[set_record(b"c", b"3"), set_record(b"a", b"hi")])
        );
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

        assert_eq!(
            log_bytes(tmp.path()),
            encoded_at(
                45,
                &[
                    set_record(b"a", b"hi"),
                    set_record(b"b", b"2"),
                    delete_record(b"a"),
                ]
            )
        );
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
        assert_eq!(
            log_bytes(tmp.path()),
            encoded_at(42, &[set_record(b"a", b"hi")])
        );
    }

    #[test]
    fn compact_twice_copies_the_same_records_to_the_end() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"1")).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.compact().unwrap();
        assert_eq!(
            log_bytes(tmp.path()),
            encoded_at(43, &[set_record(b"a", b"hi")])
        );
        store.compact().unwrap();
        assert_eq!(segment_bases(tmp.path()), [65]);
        assert_eq!(
            log_bytes(tmp.path()),
            encoded_at(65, &[set_record(b"a", b"hi")])
        );
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
            ]
        );
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
            .set_len(FILE_HEADER_LEN + 22)
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
            segment_records(tmp.path(), 43),
            [
                encoded_at(43, &[set_record(b"c", b"3")]),
                batched_at(64, &[set_record(b"d", b"4"), delete_record(b"a")]),
            ]
            .concat(),
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
            segment_records(tmp.path(), 83),
            encoded_at(83, &[delete_record(b"a")])
        );
        drop(store);

        let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
        assert_eq!(read(&store, b"a"), None, "a=1 at 0 stays hidden");
        assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
        store.verify().unwrap();
    }

    #[test]
    fn cleaning_a_delete_while_older_kept_segments_lack_its_key_drops_it() {
        let tmp = tempfile::tempdir().unwrap();
        let store = one_record_per_segment(tmp.path());
        store.clean(&store.writer(), &[0, 42]).unwrap();
        assert_eq!(segment_bases(tmp.path()), [21, 62, 83]);
        assert_eq!(
            segment_records(tmp.path(), 83),
            b"",
            "b=2 at 21 is kept but hides nothing of a"
        );
        drop(store);

        let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
        assert_eq!(read(&store, b"a"), None);
        assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
        assert_eq!(store.dead(), 0);
        store.verify().unwrap();
    }

    #[test]
    fn cleaning_a_delete_with_every_older_segment_drops_it() {
        let tmp = tempfile::tempdir().unwrap();
        let store = one_record_per_segment(tmp.path());
        store.clean(&store.writer(), &[0, 21, 42]).unwrap();
        assert_eq!(segment_bases(tmp.path()), [62, 83]);
        assert_eq!(
            segment_records(tmp.path(), 83),
            encoded_at(83, &[set_record(b"b", b"2")]),
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
        assert_eq!(segment_records(tmp.path(), 83), b"");
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
        let cut = FILE_HEADER_LEN as usize + 10;
        fs::write(&second, &fs::read(&second).unwrap()[..cut]).unwrap();
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
        write_segment(tmp.path(), 30, &encoded_at(30, &[set_record(b"x", b"1")]));
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

    // other versions and segment headers

    #[test]
    fn a_directory_from_before_segments_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        fs::write(tmp.path().join(LOG_FILE_NAME), SET_A_HI).unwrap();
        assert!(matches!(
            Store::open(tmp.path()),
            Err(Error::Unsupported {
                offset: 0,
                reason: UnsupportedReason::OtherFormat
            })
        ));
        assert_eq!(fs::read(tmp.path().join(LOG_FILE_NAME)).unwrap(), SET_A_HI);
    }

    #[test]
    fn a_segment_without_this_versions_header_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join(log_name(0)), SET_A_HI).unwrap();
        assert!(matches!(
            Store::open(tmp.path()),
            Err(Error::Unsupported {
                offset: 0,
                reason: UnsupportedReason::OtherFormat
            })
        ));
        assert_eq!(fs::read(tmp.path().join(log_name(0))).unwrap(), SET_A_HI);
    }

    #[test]
    fn a_damaged_segment_header_is_corruption_that_repair_moves_aside() {
        let tmp = tempfile::tempdir().unwrap();
        let mut bytes = segment_file(0, &SET_A_HI);
        bytes[14] ^= 0x01;
        fs::write(tmp.path().join(log_name(0)), &bytes).unwrap();
        assert!(matches!(
            Store::open(tmp.path()),
            Err(Error::Corruption {
                offset: 0,
                reason: CorruptionReason::SegmentHeader
            })
        ));

        let side_file = tmp.path().join("tally.log.corrupt-0");
        assert_eq!(
            Store::repair(tmp.path()).unwrap(),
            Some(Repaired {
                offset: 0,
                dropped: 17 + 22,
                side_file: side_file.clone(),
            })
        );
        assert_eq!(fs::read(&side_file).unwrap(), bytes);
        assert_eq!(
            fs::read(tmp.path().join(log_name(0))).unwrap(),
            segment_file(0, b"")
        );
        assert_eq!(Store::repair(tmp.path()).unwrap(), None);
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(read(&store, b"a"), None);
    }

    #[test]
    fn a_newest_segment_whose_header_a_crash_cut_short_is_started_again() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        let header = segment_file(86, b"");
        for torn in [&header[..9], &[], &[0; 17], &[0xab; 17]] {
            fs::write(tmp.path().join(log_name(86)), torn).unwrap();
            let store = Store::open(tmp.path()).unwrap();
            assert_eq!(fs::read(tmp.path().join(log_name(86))).unwrap(), header);
            assert_eq!(read(&store, b"a").as_deref(), Some(&b"hi"[..]));
        }
    }

    #[test]
    fn a_sealed_segment_whose_header_was_cut_short_is_corruption() {
        let tmp = tempfile::tempdir().unwrap();
        drop(one_record_per_segment(tmp.path()));
        fs::write(tmp.path().join(log_name(21)), &segment_file(21, b"")[..9]).unwrap();
        assert!(matches!(
            Store::open_with(tmp.path(), segmented(1)),
            Err(Error::Corruption {
                offset: 21,
                reason: CorruptionReason::CutShort
            })
        ));

        let repaired = Store::repair(tmp.path()).unwrap().unwrap();
        assert_eq!((repaired.offset, repaired.dropped), (21, 9));
        assert_eq!(segment_bases(tmp.path()), [0, 21, 42, 62]);
        let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
        assert_eq!(read(&store, b"a"), None);
        assert_eq!(read(&store, b"b"), None, "b=2 went with the damage");
        assert_eq!(read(&store, b"c").as_deref(), Some(&b"3"[..]));
        store.verify().unwrap();
    }

    #[test]
    fn open_removes_a_copy_a_crashed_compaction_left() {
        let tmp = tempfile::tempdir().unwrap();
        let left = tmp.path().join(compact_name(0));
        fs::write(&left, [0xAB; 100]).unwrap();
        let store = Store::open(tmp.path()).unwrap();
        assert!(!left.exists());
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.compact().unwrap();
        assert_eq!(
            log_bytes(tmp.path()),
            encoded_at(22, &[set_record(b"a", b"hi")])
        );
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

    fn delete_record(k: &[u8]) -> Op {
        Op::Delete { key: key(k) }
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
            batched_at(
                0,
                &[
                    set_record(b"a", b"1"),
                    set_record(b"b", b"2"),
                    set_record(b"a", b"hi"),
                    delete_record(b"b"),
                ]
            )
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
            batched_at(0, &[set_record(b"a", b"1"), delete_record(b"a")])
        );
        assert_eq!(store.get(&key(b"a")).unwrap(), None);
    }

    /// b=2, then a batch setting a=1 and deleting b.
    fn log_with_a_batch() -> (Vec<u8>, usize) {
        let before = encoded(&[set_record(b"b", b"2")]);
        let mut log = before.clone();
        log.extend(batched_at(
            21,
            &[set_record(b"a", b"1"), delete_record(b"b")],
        ));
        (log, before.len())
    }

    #[test]
    fn a_batch_cut_anywhere_is_dropped_whole() {
        let (log, before) = log_with_a_batch();
        for cut in before..log.len() {
            let tmp = tempfile::tempdir().unwrap();
            write_segment(tmp.path(), 0, &log[..cut]);
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
        write_segment(tmp.path(), 0, &log);
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
        write_segment(tmp.path(), 0, &log);
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), None);
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
        assert_eq!(log_bytes(tmp.path()).len(), before);
    }

    #[test]
    fn corruption_inside_a_batch_is_reported_and_repaired_at_the_batch() {
        let (mut log, before) = log_with_a_batch();
        log[before + 27 + 20] ^= 0x01;
        log.extend(encoded_at(89, &[set_record(b"c", b"3")]));
        let tmp = tempfile::tempdir().unwrap();
        write_segment(tmp.path(), 0, &log);
        assert!(matches!(
            Store::open(tmp.path()),
            Err(Error::Corruption {
                offset: 21,
                reason: CorruptionReason::PayloadChecksum
            })
        ));

        let repaired = Store::repair(tmp.path()).unwrap().unwrap();
        assert_eq!((repaired.offset, repaired.dropped), (21, 68 + 21));
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), None);
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
        assert_eq!(store.get(&key(b"c")).unwrap(), None);
    }

    #[test]
    fn a_batch_inside_a_batch_or_a_record_past_its_end_is_corrupt() {
        let mut nested = Vec::new();
        encode_batch(48, 0, &mut nested);
        encode(
            &Record::Batch { body_len: 21 },
            Place::Inner,
            27,
            &mut nested,
        );
        encode_op(&set_record(b"a", b"1"), Place::Inner, 54, &mut nested);
        let mut short = Vec::new();
        encode_batch(10, 0, &mut short);
        encode_op(&set_record(b"a", b"1"), Place::Inner, 27, &mut short);
        for mut log in [nested, short] {
            log.extend(encoded_at(log.len() as u64, &[set_record(b"b", b"2")]));
            let tmp = tempfile::tempdir().unwrap();
            write_segment(tmp.path(), 0, &log);
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
        assert_eq!(
            log_bytes(tmp.path()),
            encoded_at(89, &[set_record(b"b", b"2")])
        );
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

    #[test]
    fn a_value_damaged_in_a_sealed_segment_fails_its_checksum() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
        put(&store, b"c", b"3");
        drop(store);
        let file = OpenOptions::new()
            .write(true)
            .open(tmp.path().join(log_name(43)))
            .unwrap();
        file.write_all_at(b"X", FILE_HEADER_LEN + 20).unwrap();
        drop(file);

        let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
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
        let (len_one, _) = read_hint(&file, &segment, &mut one, WINDOW, u64::MAX).unwrap();
        let mut many = Index::new();
        let (len_many, _) = read_hint(&file, &segment, &mut many, 100, 0).unwrap();

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
            let at = log.len() as u64;
            if i == 140_001 {
                set_at = at;
            }
            let records = if i % 7 == 3 {
                encoded_at(at, &[Op::Delete { key: k(i) }])
            } else if i % 1000 == 0 {
                let set = Op::Set {
                    key: k(i),
                    value: value(b"batched"),
                };
                batched_at(at, &[set, Op::Delete { key: k(i + 1) }])
            } else {
                let set = Op::Set {
                    key: k(i),
                    value: value(&i.to_le_bytes()),
                };
                encoded_at(at, &[set])
            };
            log.extend(records);
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
        for (index, threads) in [(&mut one, 1), (&mut many, 4)] {
            assert!(
                matches!(
                    scan_on(&file, index, 0, len, threads, &mut |_| Ok(())),
                    Err(Error::Corruption {
                        offset,
                        reason: CorruptionReason::CutShort
                    }) if offset == len - 9
                ),
                "{threads} threads"
            );
        }
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
                        &mut |_| Ok(()),
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
            ["tally.00000000000000000043.log", "tally.lock"]
        );
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
    }

    #[test]
    fn a_hint_for_more_log_than_there_is_is_removed() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        let log = log_bytes(tmp.path());
        write_segment(tmp.path(), 43, &log[..22]);

        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(
            file_names(tmp.path()),
            ["tally.00000000000000000043.log", "tally.lock"]
        );
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        assert_eq!(store.get(&key(b"b")).unwrap(), None);
    }

    #[test]
    fn repair_removes_the_hint_before_truncating() {
        let tmp = tempfile::tempdir().unwrap();
        compacted_a_and_b(tmp.path());
        let store = Store::open(tmp.path()).unwrap();
        store.set(key(b"d"), value(b"4")).unwrap();
        drop(store);
        write_log_at(tmp.path(), 41, b"X");

        let repaired = Store::repair(tmp.path()).unwrap().unwrap();
        assert_eq!((repaired.offset, repaired.dropped), (65, 21 + 21));
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
        assert_eq!(
            log_bytes(tmp.path()),
            encoded_at(106, &[set_record(b"b", b"2")])
        );
        assert_eq!(store.get(&key(b"a")).unwrap(), None);
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
        assert_eq!(store.dead(), 0);
    }

    /// A hint's bytes, from its `entries`, Set count, log length and `crc`,
    /// computed in Python.
    fn hint_bytes(entries: &[&[u8]], count: u64, log_len: u64, crc: u64) -> Vec<u8> {
        let mut bytes = b"TLYH\x03".to_vec();
        for entry in entries {
            bytes.extend_from_slice(entry);
        }
        bytes.extend_from_slice(&count.to_le_bytes());
        bytes.extend_from_slice(&log_len.to_le_bytes());
        bytes.extend_from_slice(&crc.to_le_bytes());
        bytes
    }

    /// Flips a byte of the segment at `base`, at `at` from its first record.
    fn damage(dir: &Path, base: u64, at: u64) {
        let path = dir.join(log_name(base));
        let mut bytes = fs::read(&path).unwrap();
        bytes[(FILE_HEADER_LEN + at) as usize] ^= 0x01;
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
            ],
            "the active segment gets none"
        );
        assert_eq!(
            fs::read(tmp.path().join(hint_name(0))).unwrap(),
            hint_bytes(
                &[
                    &[0x33, 0, 0, 8, 0, 0, 0],
                    &[0x71, 1, 0, 1, 0, 0, 0, b'a'],
                    &[0x71, 1, 0, 1, 0, 0, 0, b'b'],
                ],
                2,
                69,
                0xe65a_6aef_8722_6c39
            )
        );
        assert_eq!(
            fs::read(tmp.path().join(hint_name(69))).unwrap(),
            hint_bytes(
                &[&[0x32, 1, 0, 0, 0, 0, 0, b'a']],
                0,
                20,
                0xc49e_1846_0308_4f35
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
                    &[0x31, 1, 0, 2, 0, 0, 0, b'a'],
                    &[0x31, 1, 0, 1, 0, 0, 0, b'b'],
                    &[0x31, 1, 0, 1, 0, 0, 0, b'c'],
                ],
                3,
                64,
                0x92a6_b3e1_11e1_5f58
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
        assert_eq!(fs.read(&hint).unwrap(), b"TLYH\x03\x31\x01\x00\x01\x00");
        assert_eq!(get_on(&store, b"a").as_deref(), Some(&b"1"[..]));
        assert_eq!(get_on(&store, b"b").as_deref(), Some(&b"2"[..]));
        drop(store);

        fs.limit_free_space(None);
        let store = open_on(&fs, segmented(1));
        assert_eq!(
            fs.read(&hint).unwrap(),
            hint_bytes(
                &[&[0x31, 1, 0, 1, 0, 0, 0, b'a']],
                1,
                21,
                0x3efb_41cb_e0ab_bf21
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
        assert_eq!(cut[21..29], [0x31, 1, 0, 1, 0, 0, 0, b'c']);
        assert_eq!(get_on(&store, b"c").as_deref(), Some(&b"3"[..]));
        drop(store);

        fs.limit_free_space(None);
        let store = open_on(&fs, segmented(50));
        assert_eq!(
            fs.read(&hint).unwrap(),
            hint_bytes(
                &[
                    &[0x31, 1, 0, 2, 0, 0, 0, b'a'],
                    &[0x31, 1, 0, 1, 0, 0, 0, b'b'],
                    &[0x31, 1, 0, 1, 0, 0, 0, b'c'],
                ],
                3,
                64,
                0x92a6_b3e1_11e1_5f58
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
    fn a_compaction_that_fails_after_writing_copies_leaves_no_trace() {
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
        assert_eq!(file_names(tmp.path()), [log_name(0).as_str(), "tally.lock"]);
        assert_eq!(read(&store, b"a"), Some(big.clone()));
        assert_eq!(read(&store, b"b"), Some(big));
        put(&store, b"d", b"4");
        assert_eq!(
            log_bytes(tmp.path())[end as usize..],
            encoded_at(end, &[set_record(b"d", b"4")])
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
            encoded_at(84, &[set_record(b"a", b"3"), set_record(b"b", b"4")])
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
        assert_eq!(
            log_bytes(tmp.path()),
            encoded_at(63, &[set_record(b"b", b"2")])
        );
    }

    #[test]
    fn a_log_below_the_minimum_size_is_not_compacted() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open_with(tmp.path(), auto(50, 43)).unwrap();
        store.set(key(b"a"), value(b"1")).unwrap();
        store.set(key(b"a"), value(b"2")).unwrap();
        assert_eq!(log_bytes(tmp.path()).len(), 42);
        store.set(key(b"a"), value(b"3")).unwrap();
        assert_eq!(
            log_bytes(tmp.path()),
            encoded_at(63, &[set_record(b"a", b"3")])
        );
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
        assert_eq!(
            log_bytes(tmp.path()),
            encoded_at(44, &[set_record(b"a", b"hi")])
        );
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
            encoded_at(84, &[set_record(b"a", b"3"), set_record(b"b", b"4")])
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
        assert_eq!(
            log_bytes(tmp.path()).len(),
            21 + 27 + 7 * 21,
            "one alone, seven in a batch"
        );
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
            [
                encoded(&[set_record(b"b", b"0")]),
                batched_at(
                    21,
                    &[
                        set_record(b"a", b"1"),
                        delete_record(b"a"),
                        set_record(b"b", b"2"),
                    ]
                ),
            ]
            .concat()
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

    /// The records of the first segment on `fs`.
    fn log_on(fs: &MemFs) -> Vec<u8> {
        fs.read(&Path::new("/db").join(log_name(0))).unwrap()[FILE_HEADER_LEN as usize..].to_vec()
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
        assert_eq!(get_on(&store, b"b"), None);
        store.set(key(b"c"), value(b"3")).unwrap();
        drop(store);

        let store = open_on(&power_cut(&fs), segmented(1 << 20));
        assert_eq!(get_on(&store, b"a").as_deref(), Some(&b"1"[..]));
        assert_eq!(get_on(&store, b"b"), None);
        assert_eq!(get_on(&store, b"c").as_deref(), Some(&b"3"[..]));
    }

    /// The disk after power fails with nothing unsynced landing.
    fn power_cut(fs: &MemFs) -> MemFs {
        let nothing = Cut {
            keep: 0,
            torn: Torn::Nothing,
            entries: 0,
            whole: None,
        };
        MemFs::after(&fs.disk(), nothing)
    }

    #[test]
    fn a_failed_directory_fsync_when_starting_a_segment_poisons_the_store() {
        let fs = MemFs::new();
        let store = open_on(&fs, segmented(21));
        store.set(key(b"a"), value(b"1")).unwrap();
        fs.fail_dir_syncs(true);
        let err = store.set(key(b"b"), value(b"2")).unwrap_err();
        assert!(
            matches!(&err, Error::Io(io) if io.to_string() == "injected directory fsync failure"),
            "{err:?}"
        );
        fs.fail_dir_syncs(false);
        assert!(matches!(
            store.set(key(b"b"), value(b"2")),
            Err(Error::Poisoned)
        ));
    }

    #[test]
    fn an_open_whose_compaction_poisons_the_store_fails() {
        let fs = MemFs::new();
        let options = Options {
            auto_compact: AutoCompact::new(50, 0),
            segment_bytes: 1 << 20,
        };
        let store = open_on(
            &fs,
            Options {
                auto_compact: None,
                ..options
            },
        );
        store.set(key(b"a"), value(b"1")).unwrap();
        store.set(key(b"a"), value(b"2")).unwrap();
        drop(store);
        // Past the two of `open` itself, to the one after the copy's rename.
        fs.fail_dir_syncs_after(2);
        let opened = Store::open_in(fs.clone(), Path::new("/db"), options);
        fs.fail_dir_syncs(false);
        let err = opened.err().expect("open fails");
        assert!(
            matches!(&err, Error::Io(io) if io.to_string() == "injected directory fsync failure"),
            "{err:?}"
        );
        let store = open_on(&fs, options);
        assert_eq!(get_on(&store, b"a").as_deref(), Some(&b"2"[..]));
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
    fn a_failed_fsync_of_a_compactions_copy_leaves_the_store_as_it_was() {
        let fs = MemFs::new();
        let store = open_on(&fs, segmented(1));
        store.set(key(b"a"), value(b"1")).unwrap();
        store.set(key(b"a"), value(b"2")).unwrap();
        store.set(key(b"b"), value(b"3")).unwrap();
        fs.fail_syncs(true);
        assert!(matches!(store.compact(), Err(Error::Io(_))));
        fs.fail_syncs(false);
        let db = Path::new("/db");
        assert_eq!(fs.read(&db.join(compact_name(63))), None);
        assert_eq!(fs.read(&db.join(hint_name(63))), None);
        assert_eq!(get_on(&store, b"a").as_deref(), Some(&b"2"[..]));
        assert_eq!(get_on(&store, b"b").as_deref(), Some(&b"3"[..]));
        store.set(key(b"c"), value(b"4")).unwrap();
        drop(store);

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
        assert_eq!(
            log_bytes(tmp.path()),
            encoded_at(63, &[set_record(b"a", b"3")])
        );
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
