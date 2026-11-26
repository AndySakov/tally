//! Reading records in log order, and the torn-tail rule (DESIGN.md,
//! Recovery).

use std::io::{self, BufRead, BufReader, Read};
use std::num::NonZero;
use std::sync::mpsc;
use std::{mem, thread};

use crate::fs::{LogFile, buffer_for};
use crate::index::{Entry, Index};
use crate::record::{
    HEADER_LEN, Header, IndexOp, Place, check_payload, decode_header, index_op, starts_a_record,
};
use crate::{CorruptionReason, Error, Key};

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

const MAX_THREADS: usize = 8;

/// One thread for less `work` than `from`, else up to `MAX_THREADS`.
pub(super) fn threads_for(work: u64, from: u64) -> usize {
    if work < from {
        return 1;
    }
    thread::available_parallelism()
        .map_or(1, NonZero::get)
        .min(MAX_THREADS)
}

/// Scans of fewer bytes than this apply each record as it is read, on one
/// thread.
const PARALLEL_SCAN_FROM: u64 = 16 << 20;

/// Records a parallel scan or hint read takes before applying them.
pub(super) const WINDOW: usize = 1 << 16;

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
pub(super) fn scan(
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

/// On more than one thread, this one reads while another applies what it
/// read, a window at a time, on `threads` more.
pub(super) fn scan_on(
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
pub(super) struct Scanned<'r> {
    pub offset: u64,
    pub header: Header,
    pub op: &'r IndexOp,
    pub header_bytes: &'r mut [u8; HEADER_LEN],
    pub payload: &'r [u8],
}

/// Reads the records from `start` to `len` through a buffer of up to
/// `buffer` bytes. Calls `record` with every record read, and `sink` with
/// the key and new index entry, `None` for a Delete, of each Set and Delete
/// to apply, in order: a batch's once all of it has been read.
pub(super) fn scan_into(
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
            reader.show(at, header, &op, record)?;
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
pub(super) struct Window {
    keys: Vec<u8>,
    pub changes: Vec<(usize, Option<Entry>)>,
}

impl Window {
    pub(super) fn push(&mut self, key: &[u8], entry: Option<Entry>) {
        self.keys.extend_from_slice(key);
        self.changes.push((self.keys.len(), entry));
    }

    pub(super) fn apply_to(self, index: &mut Index, threads: usize) {
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
