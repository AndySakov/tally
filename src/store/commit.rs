//! Writes: group commit and appends (DESIGN.md, Group commit).

use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::atomic::Ordering;
use std::sync::{Arc, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use super::{Store, Writing, hold, segment_of};
use crate::fs::{Fs, LogFile};
use crate::index::{Entry, Index};
use crate::manifest;
use crate::record::{BATCH_PAYLOAD_LEN, Op, Place, encode_batch, encode_op, record_len};
use crate::segment::{Segment, create, log_name};
use crate::{Batch, Error, Key, Value};

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
pub(super) struct Queue {
    pub(super) waiting: VecDeque<(u64, Write)>,
    next: u64,
    done: HashMap<u64, Result<(), Error>>,
    /// Whether a leader is writing a group now.
    leading: bool,
    /// How many writes the last group took.
    pub(super) last_group: usize,
}

/// The share of the last fsync's time, in percent, a leader waits for its
/// group to fill (DESIGN.md, Group commit).
const GROUP_WAIT_PERCENT: u64 = 10;

/// A group takes waiting writes until it has this many, or this many bytes.
const GROUP_MOST_WRITES: usize = 1024;

const GROUP_MOST_BYTES: usize = 4 << 20;

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
pub(super) fn each(err: Error, n: usize) -> Vec<Result<(), Error>> {
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

impl<F: Fs> Store<F> {
    /// Starts a new, empty segment at the end of the log, and makes its
    /// name durable, then the manifest listing it, before anything is
    /// written to it.
    fn roll(&self, writing: &Writing) -> Result<(), Error> {
        let segments = self.segments();
        let base = segments.last().expect("there is always a segment").end();
        let segment = create(&self.fs, &self.dir.join(log_name(base)), base)?;
        let mut next = segments.to_vec();
        next.push(Arc::new(segment));
        let bases: Vec<u64> = next.iter().map(|s| s.base).collect();
        // A retry would fsync a new descriptor, which Linux need not tell
        // about a failure, and the manifest must not list the segment
        // before its name is durable.
        if let Err(err) = self
            .fs
            .sync_dir(&self.dir)
            .and_then(|()| manifest::write(&self.fs, &self.dir, &bases))
        {
            return Err(self.poison(err));
        }
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
    pub(super) fn append(&self, writing: &Writing, ops: &[&Op]) -> Result<Vec<u64>, Error> {
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
    #[cfg(any(test, fuzzing))]
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
}
