use std::fs::TryLockError;
use std::io::{self, ErrorKind};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, RwLock};
use std::thread;

use crate::fs::{Fs, LogFile, OsFs};
use crate::index::{Entry, Index};
use crate::manifest;
use crate::options::{AutoCompact, Options};
use crate::record::{HEADER_LEN, Record, decode_header, decode_payload};
use crate::segment::{Segment, create, log_name};
use crate::{CorruptionReason, Error, Key, Value};

mod commit;
mod compact;
mod open;
mod repair;
mod scan;

use commit::Queue;
#[cfg(any(test, fuzzing))]
pub(crate) use commit::Write;
use open::{Opened, load, open_segments};
use scan::scan;

/// The one-file log of tally before segments, which this version refuses.
pub(crate) const LOG_FILE_NAME: &str = "tally.log";
pub(crate) const LOCK_FILE_NAME: &str = "tally.lock";

/// What `Store::repair` moved out of the log.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Repaired {
    /// Offset of the first bad record, where its segment now ends, or the
    /// base of a segment whose file header was bad, which is now empty.
    pub offset: u64,
    /// Bytes moved out of the log: everything from `offset` to the end of
    /// its segment, or the whole file of a segment whose header was bad.
    pub dropped: u64,
    /// Where those bytes were saved.
    pub side_file: SideFile,
}

/// The file `Store::repair` saved the bytes it moved to.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SideFile {
    /// `tally.log.corrupt-<offset>` in the store directory.
    pub path: PathBuf,
    /// Where in the file the bytes that failed to read are, in the 4 KiB
    /// blocks of the segment's file. The file holds zeros there.
    pub unreadable: Vec<Range<u64>>,
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
    /// A segment `tally.manifest` lists that is not in `dir` fails `open`
    /// with `SegmentMissing` corruption at its base, changing nothing
    /// (DESIGN.md, Manifest).
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
    /// something to repair: a newer tally can read them. So are a missing
    /// segment and a damaged manifest: put the file back, or remove
    /// `tally.manifest` to open without it. Fails with `Locked` while a
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

/// The index in `segments` of the segment starting at or before `offset`.
fn segment_of<L>(segments: &[Arc<Segment<L>>], offset: u64) -> usize {
    segments.partition_point(|s| s.base <= offset) - 1
}

/// The segment in `segments` holding the byte at `offset`, if one does.
fn holding<L>(segments: &[Arc<Segment<L>>], offset: u64) -> Option<&Arc<Segment<L>>> {
    let after = segments.partition_point(|s| s.base <= offset);
    segments[..after].last().filter(|s| s.holds(offset))
}

fn lock(file: &impl LogFile) -> Result<(), Error> {
    match file.try_lock() {
        Ok(()) => Ok(()),
        Err(TryLockError::WouldBlock) => Err(Error::Locked),
        Err(TryLockError::Error(err)) => Err(Error::Io(err)),
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

/// Removes `path`. Returns whether it was there.
fn remove_if_present(fs: &impl Fs, path: &Path) -> io::Result<bool> {
    match fs.remove(path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
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
        let Opened {
            mut segments,
            listed,
        } = open_segments(&fs, dir)?;
        if segments.is_empty() {
            segments.push(create(&fs, &dir.join(log_name(0)), 0)?);
        }
        segments.last().expect("one was just made").file.sync()?;
        fs.sync_dir(dir)?;
        fs.sync_dir(parent)?;
        let index = load(&fs, dir, &mut segments)?;
        if !listed {
            let bases: Vec<u64> = segments.iter().map(|s| s.base).collect();
            manifest::write(&fs, dir, &bases)?;
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
            if let Err(err) = store.compact_if_due(&writing)
                && store.writable().is_err()
            {
                return Err(err);
            }
        }
        Ok(store)
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
            let file_end = segment.size()?;
            if file_end != segment.end() {
                return Err(mismatch(file_end.min(segment.end())));
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

#[cfg(test)]
mod tests;
