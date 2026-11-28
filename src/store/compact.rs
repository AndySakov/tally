//! Compaction (DESIGN.md, Compaction).

use std::collections::BTreeSet;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

#[cfg(test)]
use super::Phase;
use super::{
    Compacting, Inner, Reserved, Segments, remove_if_present,
    scan::{Scanned, scan_into},
    segment_of,
};
use crate::filter::Filter;
use crate::fs::{Fs, LogFile};
use crate::hint;
use crate::index::Entry;
use crate::manifest;
use crate::options::AutoCompact;
use crate::record::{Fields, IndexOp, Kind, Op, Place, encode_op, move_header};
use crate::segment::{Segment, compact_name, create, hint_name, log_name};
use crate::{CorruptionReason, Error, Key};

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
    /// The bases of the kept segments those Delete records may hide Sets in.
    carried_against: BTreeSet<u64>,
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

/// A hint whose checksum matched failing to read back: an io error.
fn unreadable(hint::Invalid(why): hint::Invalid) -> Error {
    Error::Io(io::Error::other(format!(
        "rereading a compaction's hint: {why}"
    )))
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

/// The bases, in log order, of the segments `deadest` picks, and of the
/// sealed segments under an eighth of `segment_bytes`, oldest first, while
/// the copy of them all would fit in `segment_bytes`: each compaction's
/// copy is a segment of its own, and they would otherwise pile up. Empty
/// if `auto` isn't due.
pub(super) fn to_clean(
    segments: &[SegmentSize],
    auto: AutoCompact,
    segment_bytes: u64,
) -> Vec<u64> {
    let mut chosen = deadest(segments, auto);
    if chosen.is_empty() {
        return chosen;
    }
    let copy = |s: &SegmentSize| s.len - s.dead;
    let mut copied: u64 = segments
        .iter()
        .filter(|s| chosen.contains(&s.base))
        .map(copy)
        .sum();
    let sealed = &segments[..segments.len() - 1];
    let small: Vec<u64> = sealed
        .iter()
        .filter(|s| s.len < segment_bytes / 8 && !chosen.contains(&s.base))
        .take_while(|s| {
            copied += copy(s);
            copied <= segment_bytes
        })
        .map(|s| s.base)
        .collect();
    chosen.extend(small);
    chosen.sort_unstable();
    chosen
}

impl<F: Fs> Inner<F> {
    pub(crate) fn compact(&self) -> Result<(), Error> {
        let compacting = self.compacting();
        self.writable()?;
        let cleaned = self.clean(&compacting, |segments| {
            segments
                .iter()
                .filter(|s| s.len() > 0)
                .map(|s| s.base)
                .collect()
        });
        drop(compacting);
        self.wake_stalled();
        cleaned.map(|_| ())
    }

    /// Copies the live records of the segments `choose` picks from the
    /// list (their bases, in log order) to a new segment at the end of the
    /// log, then removes them, and returns whether it picked any. A Delete
    /// record is copied too while an older segment that is kept may hold a
    /// Set it hides. Writes go on meanwhile: the first after `choose` starts
    /// a segment past every offset the copies could take, so none of them
    /// lands in a segment being cleaned, and each is newer than the copies.
    pub(super) fn clean(
        &self,
        _: &Compacting,
        choose: impl FnOnce(&[Arc<Segment<F::File>>]) -> Vec<u64>,
    ) -> Result<bool, Error> {
        let (segments, chosen, base) = {
            let mut writing = self.writer();
            let segments = self.segments();
            let chosen = choose(&segments);
            if chosen.is_empty() {
                return Ok(false);
            }
            #[cfg(test)]
            self.compactions.fetch_add(1, Ordering::Relaxed);
            let base = segments.last().expect("there is always a segment").end();
            // Every copy, and every Delete carried, is a record of the same
            // length in a segment being cleaned.
            let most: u64 = segments
                .iter()
                .filter(|s| chosen.contains(&s.base))
                .map(|s| s.len())
                .sum();
            *writing.reserved = Some(Reserved {
                base,
                end: base + most,
            });
            (segments, chosen, base)
        };
        let release = || *self.writer().reserved = None;
        #[cfg(test)]
        self.between(Phase::Reserved);
        let copy_path = self.dir.join(compact_name(base));
        let hint_path = self.dir.join(hint_name(base));
        // The copy is complete and durable before it takes a segment's name,
        // so the newest segment never holds more than one append that a
        // crash could tear. An empty one never does: once a write has
        // started a segment past it, nothing would remove it.
        let copied = self
            .copy_live(&segments, base, &copy_path, &hint_path, &chosen)
            .and_then(|copied| {
                if copied.written > 0 {
                    self.fs.rename(&copy_path, &self.dir.join(log_name(base)))?;
                }
                Ok(copied)
            });
        let Copied {
            output,
            hint,
            written,
            moved,
            carried,
            carried_against,
            dropped_delete,
        } = match copied {
            Ok(copied) => copied,
            Err(err) => {
                let _ = remove_if_present(&self.fs, &copy_path);
                let _ = remove_if_present(&self.fs, &hint_path);
                release();
                return Err(err);
            }
        };
        if written == 0 {
            drop((output, hint));
            let _ = remove_if_present(&self.fs, &copy_path);
            let _ = remove_if_present(&self.fs, &hint_path);
            let mut writing = self.writer();
            *writing.reserved = None;
            if chosen.contains(&self.active().base) {
                // Nothing was written after it, and it goes.
                self.roll(&writing, base)?;
            }
            drop(writing);
            return self.remove_cleaned(&chosen, None, dropped_delete);
        }
        if let Err(err) = self.fs.sync_dir(&self.dir) {
            release();
            return Err(self.poison(err));
        }
        #[cfg(test)]
        self.copied.fetch_add(written, Ordering::Relaxed);
        output.set_len_written(written);
        output.add_dead(carried);
        output.set_carried(carried, carried_against);
        let output = Arc::new(output);
        let listed = {
            let mut writing = self.writer();
            // Without a write since `choose`, the copy is the last segment
            // and takes the next ones, in place of the last if that was
            // empty and so had the same base.
            let mut next: Vec<_> = self
                .segments()
                .iter()
                .filter(|s| s.base != base)
                .cloned()
                .collect();
            let at = next.partition_point(|s| s.base < base);
            next.insert(at, Arc::clone(&output));
            // The copy's name is durable before the manifest lists it, and
            // the manifest no longer lists the cleaned segments before any
            // goes. Under the writer lock, so it lists every segment a write
            // started.
            let bases: Vec<u64> = next
                .iter()
                .map(|s| s.base)
                .filter(|base| !chosen.contains(base))
                .collect();
            *writing.reserved = None;
            // A write that poisoned the store failed a directory fsync, and
            // Linux need not tell the copy's, so its name may not be
            // durable: the manifest must not list it. A write poisons the
            // store under the writer lock, so it has by now.
            self.writable()?;
            if let Err(err) = manifest::write(&self.fs, &self.dir, &bases) {
                return Err(self.poison(err));
            }
            self.replace_segments(&writing, next);
            self.segments()
        };
        #[cfg(test)]
        self.between(Phase::Copied);
        // Written only now: nothing orders writes to two files, and a hint
        // must never reach the disk before the copies it lists.
        let repointed = hint
            .finish(written)
            .and_then(|file| file.sync().map(|()| file))
            .map_err(Error::from)
            .and_then(|file| self.repoint(&output, &file, &moved));
        if let Err(err) = repointed {
            {
                // The cleaned segments stay in use, so they are listed again.
                let _writing = self.writer();
                let kept: Vec<u64> = self.segments().iter().map(|s| s.base).collect();
                if let Err(unlisted) = manifest::write(&self.fs, &self.dir, &kept) {
                    self.poison(unlisted);
                }
            }
            let _ = remove_if_present(&self.fs, &hint_path);
            if let Err(unread) = self.repoint_by_records(&output, written, &moved) {
                self.poison(unread);
            }
            self.count_dead(&self.writer());
            return Err(err);
        }
        #[cfg(test)]
        self.between(Phase::Repointed);
        self.remove_cleaned(&chosen, Some(listed), dropped_delete)
    }

    /// Removes the `chosen` segments, whose live records are all in a copy
    /// if they had any. `listed` is the list as the manifest last had it
    /// without them, if it has since the compaction began.
    fn remove_cleaned(
        &self,
        chosen: &[u64],
        listed: Option<Segments<F::File>>,
        dropped_delete: bool,
    ) -> Result<bool, Error> {
        let segments = self.segments();
        let mut left_behind = 0usize;
        self.index.for_each(|entry| {
            if chosen.contains(&segments[segment_of(&segments, entry.offset)].base) {
                left_behind += 1;
            }
        });
        assert_eq!(left_behind, 0, "every live key in them is copied");
        let kept = {
            // Out of the list in one step, so a write past `stop_percent`
            // does not wait for their removals.
            let writing = self.writer();
            self.writable()?;
            let kept: Vec<_> = self
                .segments()
                .iter()
                .filter(|s| !chosen.contains(&s.base))
                .cloned()
                .collect();
            // A segment started since would have listed them again.
            if listed.is_none_or(|listed| !Arc::ptr_eq(&listed, &self.segments())) {
                let bases: Vec<u64> = kept.iter().map(|s| s.base).collect();
                if let Err(err) = manifest::write(&self.fs, &self.dir, &bases) {
                    return Err(self.poison(err));
                }
            }
            self.replace_segments(&writing, kept.clone());
            kept
        };
        self.wake_stalled();
        #[cfg(test)]
        self.between(Phase::Unlisted);
        // A Delete left behind must not outlive the Set it hides, so then
        // each removal is made durable before the next, oldest first. Without
        // one, the removals may land in any order. A write that poisoned the
        // store meanwhile failed a directory fsync, and Linux need not tell
        // a later one on a new descriptor, so none of them is durable.
        // Under the writer lock: a write that failed a directory fsync
        // poisons the store before it lets go.
        let writable = || {
            let _writing = self.writer();
            self.writable()
        };
        for (i, &base) in chosen.iter().enumerate() {
            writable()?;
            let removed = remove_if_present(&self.fs, &self.dir.join(hint_name(base)))
                .and_then(|_| self.fs.remove(&self.dir.join(log_name(base))));
            if let Err(err) = removed {
                // Still there, so listed again, for a later compaction.
                let writing = self.writer();
                let mut next = self.segments().to_vec();
                for segment in segments.iter().filter(|s| chosen[i..].contains(&s.base)) {
                    segment.set_dead(segment.len());
                    next.push(Arc::clone(segment));
                }
                next.sort_by_key(|s| s.base);
                self.replace_segments(&writing, next);
                return Err(err.into());
            }
            if (dropped_delete || i + 1 == chosen.len())
                && let Err(err) = self.fs.sync_dir(&self.dir)
            {
                return Err(self.poison(err));
            }
        }
        writable()?;
        // Only now: until every one is gone, a Delete they may hide a Set
        // in is still needed.
        for segment in &kept {
            segment.uncarry(chosen);
        }
        Ok(true)
    }

    /// Writes each live record of the `chosen` segments to a new segment at
    /// `copy_path`, based at `base`, and its key and lengths to a
    /// hint at `hint_path`, then a Delete record of each key that must stay
    /// deleted: it isn't live, and a kept segment older than one of its
    /// Delete records may hold a Set of it. Then fsyncs the copy. One
    /// buffered pass per segment: reading the dead records in between costs
    /// less than a positioned read per live one.
    fn copy_live(
        &self,
        segments: &[Arc<Segment<F::File>>],
        base: u64,
        copy_path: &Path,
        hint_path: &Path,
        chosen: &[u64],
    ) -> Result<Copied<F::File>, Error> {
        let output = create(&self.fs, copy_path, base)?;
        let hint_file = self.fs.open_or_create(hint_path)?;
        hint_file.set_len(0)?;
        let mut hint = hint::Writer::new(hint_file);
        let mut moved = Vec::with_capacity(self.index.len());
        let mut carry = BTreeSet::new();
        let mut carried_against = BTreeSet::new();
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
                            match self.kept_set_of(segments, chosen, input.base, key)? {
                                Some(against) => {
                                    carry.insert(Key::try_from(*key).expect("a key"));
                                    carried_against.insert(against);
                                }
                                None => dropped_delete = true,
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
            carried_against,
            dropped_delete,
        })
    }

    /// The base of the oldest segment older than `before` that is not being
    /// cleaned and may hold a Set record of `key`, which a Delete record of
    /// it at `before` would hide. Each such segment is sealed: the newest
    /// segment is never older than one being cleaned.
    fn kept_set_of(
        &self,
        segments: &[Arc<Segment<F::File>>],
        chosen: &[u64],
        before: u64,
        key: &[u8],
    ) -> Result<Option<u64>, Error> {
        for segment in segments
            .iter()
            .filter(|s| s.base < before && !chosen.contains(&s.base))
        {
            if self.sets_of(segment)?.may_hold(key) {
                return Ok(Some(segment.base));
            }
        }
        Ok(None)
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
                    return Err(hint::Invalid::new(
                        "the segment is shorter than the hint says",
                    ));
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
                Err(hint::Invalid(why)) => {
                    log::warn!(
                        "ignored the hint for the segment at {} ({why}); reading the segment instead",
                        segment.base
                    );
                    keys.clear();
                }
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
    /// The Delete records after the copies change nothing. The hint is
    /// checked whole first: a bad entry would point keys at other records.
    fn repoint(
        &self,
        output: &Segment<F::File>,
        hint_file: &F::File,
        moved: &[u64],
    ) -> Result<(), Error> {
        let corrupt = || Error::Corruption {
            offset: output.base,
            reason: CorruptionReason::CompactionHint,
        };
        if !hint::checksum_matches(hint_file)? {
            return Err(corrupt());
        }
        let trailer = hint::trailer(hint_file).map_err(unreadable)?;
        if trailer.count != moved.len() as u64 {
            return Err(corrupt());
        }
        let mut old = moved.iter();
        hint::read_entries(hint_file, trailer, |fields, key, offset| {
            if fields.kind != Kind::Set {
                return;
            }
            let Some(&from) = old.next() else {
                return;
            };
            let entry = Entry::new(output.base + offset, fields);
            if !self.index.replace(key, from, entry) {
                output.add_dead(entry.record_len());
            }
        })
        .map_err(unreadable)?;
        Ok(())
    }

    /// Points the index at the copies in `output`, the first `written` bytes
    /// of it, read from its records as a reopen without its hint would.
    fn repoint_by_records(
        &self,
        output: &Segment<F::File>,
        written: u64,
        moved: &[u64],
    ) -> Result<(), Error> {
        let mut old = moved.iter();
        scan_into(
            output,
            output.base,
            output.base + written,
            COMPACT_BUFFER,
            &mut |key, entry| {
                if let Some(entry) = entry
                    && let Some(&from) = old.next()
                {
                    self.index.replace(key, from, entry);
                }
            },
            &mut |_| Ok(()),
        )
    }

    /// Compacts if automatic compaction is on and due, cleaning the
    /// segments with the most dead bytes until it no longer would be, and
    /// small sealed ones with them (`to_clean`), and returns whether it
    /// cleaned any. A failure is logged and turns automatic compaction off
    /// until the next open; it is returned only for `open`, which fails if
    /// it poisoned the store. After a write, the write has already
    /// succeeded.
    pub(super) fn compact_if_due(&self, compacting: &Compacting) -> Result<bool, Error> {
        let Some(auto) = self.auto_compact else {
            return Ok(false);
        };
        if self.auto_failed.load(Ordering::Relaxed) || self.writable().is_err() {
            return Ok(false);
        }
        let choose = |segments: &[Arc<Segment<F::File>>]| {
            let sizes: Vec<SegmentSize> = segments
                .iter()
                .map(|s| SegmentSize {
                    base: s.base,
                    len: s.len(),
                    dead: s.reclaimable(),
                })
                .collect();
            to_clean(&sizes, auto, self.segment_bytes)
        };
        self.clean(compacting, choose).inspect_err(|err| {
            self.auto_failed.store(true, Ordering::Relaxed);
            log::warn!("automatic compaction failed, off until the next open: {err}");
        })
    }
}
