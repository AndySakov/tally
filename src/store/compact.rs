//! Compaction (DESIGN.md, Compaction).

use std::collections::BTreeSet;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::{
    Store, Writing, remove_if_present,
    scan::{Scanned, scan_into},
    segment_of,
};
use crate::filter::Filter;
use crate::fs::{Fs, LogFile};
use crate::hint;
use crate::index::Entry;
use crate::options::AutoCompact;
use crate::record::{Fields, IndexOp, Kind, Op, Place, encode_op, move_header};
use crate::segment::{Segment, compact_name, create, hint_name, log_name};
use crate::{Error, Key};

/// What `copy_live` wrote.
struct Copied<L: LogFile> {
    /// The new segment, fsynced, under its temporary name.
    output: Segment<L>,
    /// Its hint, without the trailer.
    hint: hint::Writer<L>,
    /// The bytes copied.
    written: u64,
    /// Where each Set record copied was, in the order copied.
    moved: Vec<u64>,
    /// The bytes of the Delete records written after the copies, which are
    /// dead.
    carried: u64,
    /// Whether it left behind a Delete record of a key that isn't live, which
    /// may hide a Set in an older segment being cleaned.
    dropped_delete: bool,
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

/// How much `compact` buffers before each write.
const COMPACT_BUFFER: usize = 1 << 20;

#[derive(Debug, Clone, Copy)]
pub(super) struct SegmentSize {
    pub(super) base: u64,
    pub(super) len: u64,
    pub(super) dead: u64,
}

/// The bases, in log order, of the segments to clean so that `auto` is no
/// longer due: the largest share of dead bytes first, each taking the log's
/// size down by its dead bytes. Empty if it isn't due.
pub(super) fn deadest(segments: &[SegmentSize], auto: AutoCompact) -> Vec<u64> {
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

impl<F: Fs> Store<F> {
    /// Rewrites the log with only the latest record of each live key, so
    /// overwritten values and Delete records stop taking space.
    ///
    /// Cleans every segment (DESIGN.md, Compaction): copies the live records
    /// into a new segment with a hint, points the index at the copies, then
    /// removes the old segments oldest first, fsyncing the directory after
    /// each if it dropped a Delete record, else once after the last. Until
    /// then the old segments are untouched, so any failure while copying
    /// returns the error and leaves the store as it was. Checks every record
    /// it reads, so a corrupt record stops compaction with its offset rather
    /// than being copied. Reads go on while it runs; writes wait for it. A
    /// failed directory fsync poisons the store.
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
    pub(super) fn clean(&self, writing: &Writing, chosen: &[u64]) -> Result<(), Error> {
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
            carried,
            dropped_delete,
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
        output.add_dead(carried);
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
        // A Delete left behind must not outlive the Set it hides, so then
        // each removal is made durable before the next, oldest first. Without
        // one, the removals may land in any order.
        for (i, &base) in chosen.iter().enumerate() {
            remove_if_present(&self.fs, &self.dir.join(hint_name(base)))?;
            self.fs.remove(&self.dir.join(log_name(base)))?;
            let kept = self
                .segments()
                .iter()
                .filter(|s| s.base != base)
                .cloned()
                .collect();
            self.replace_segments(writing, kept);
            if (dropped_delete || i + 1 == chosen.len())
                && let Err(err) = self.fs.sync_dir(&self.dir)
            {
                return Err(self.poison(err));
            }
        }
        Ok(())
    }

    /// Writes each live record of the `chosen` segments to a new segment at
    /// `copy_path`, based where the log ends, and its key and lengths to a
    /// hint at `hint_path`, then a Delete record of each key that must stay
    /// deleted: it isn't live, and a kept segment older than one of its
    /// Delete records may hold a Set of it. Then fsyncs the copy. One
    /// buffered pass per segment: reading the dead records in between costs
    /// less than a positioned read per live one.
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
        let mut dropped_delete = false;
        let mut buffer = Vec::new();
        let mut written = 0u64;
        for input in segments.iter().filter(|s| chosen.contains(&s.base)) {
            each_record(input, input.base, |read| {
                match read.op {
                    IndexOp::Set { key } => {
                        if self
                            .index
                            .get(key)
                            .is_some_and(|entry| entry.offset == read.offset)
                        {
                            moved.push(read.offset);
                            let fields = Fields {
                                place: Place::Top,
                                ..read.header.fields()
                            };
                            hint.push(fields, key)?;
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
                        if !self.index.contains(key) {
                            if self.kept_set_of(segments, chosen, input.base, key)? {
                                carry.insert(Key::try_from(*key).expect("a key"));
                            } else {
                                dropped_delete = true;
                            }
                        }
                    }
                    IndexOp::Batch { .. } => {}
                }
                Ok(())
            })?;
        }
        let mut carried = 0;
        for key in carry {
            let op = Op::Delete { key };
            hint.push(op.fields(), op.key().as_bytes())?;
            encode_op(
                &op,
                Place::Top,
                base + written + buffer.len() as u64,
                &mut buffer,
            );
            carried += op.fields().record_len();
        }
        output.write_all_at(&buffer, base + written)?;
        written += buffer.len() as u64;
        output.file.sync()?;
        Ok(Copied {
            output,
            hint,
            written,
            moved,
            carried,
            dropped_delete,
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
        key: &[u8],
    ) -> Result<bool, Error> {
        for segment in segments
            .iter()
            .filter(|s| s.base < before && !chosen.contains(&s.base))
        {
            if self.sets_of(segment)?.may_hold(key) {
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
                keys.add(key);
            }
            Ok(())
        })?;
        Ok(segment.sets.get_or_init(|| keys.build()))
    }

    /// Points each key `copy_live` copied at its copy in `output`, as the
    /// output's durable hint lists them, if the key still points at the old
    /// record `moved` holds the offset of. A copy left unpointed is dead.
    /// The Delete records after the copies change nothing.
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
            if fields.kind != Kind::Set {
                return;
            }
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
    pub(super) fn compact_if_due(&self, writing: &Writing) -> Result<(), Error> {
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
}
