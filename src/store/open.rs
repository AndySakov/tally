//! What `open` does before the store exists: opening the segments and
//! building the index from hints and scans (DESIGN.md, Recovery).

use std::io::{self, ErrorKind, Read};
use std::mem;
use std::path::{Path, PathBuf};

use super::{
    LOG_FILE_NAME,
    repair::copy_to_end,
    scan::{WINDOW, Window, applying, scan, threads_for},
};
use crate::fs::{Fs, LogFile};
use crate::hint;
use crate::index::{Entry, Index};
use crate::record::{HEADER_LEN, Kind};
use crate::segment::{
    FILE_HEADER_LEN, FileHeader, Segment, check_file_header, hint_name, log_name,
    parse_compact_name, parse_hint_name, parse_log_name, start_file,
};
use crate::{CorruptionReason, Error, UnsupportedReason};

/// The shortest record there can be: a one-byte key and no value.
const SHORTEST_RECORD: u64 = HEADER_LEN as u64 + 1;

/// Opens every segment in `dir`, in log order, after removing hints whose
/// segment is gone and copies a compaction left unfinished. Refuses a
/// directory another version of tally wrote, and segments that overlap.
/// A newest segment whose header a crash cut short is started again: no
/// record was written to it before its header was fsynced.
pub(super) fn open_segments<F: Fs>(fs: &F, dir: &Path) -> Result<Vec<Segment<F::File>>, Error> {
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
pub(super) fn load<F: Fs>(
    fs: &F,
    dir: &Path,
    segments: &mut [Segment<F::File>],
) -> Result<Index, Error> {
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
                    Some((file, trailer))
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
            Some((file, trailer)) => {
                match read_hint(
                    file,
                    *trailer,
                    segment,
                    &mut index,
                    WINDOW,
                    threads_for(trailer.count, PARALLEL_FROM),
                ) {
                    Ok((hinted, tail)) => (hinted, Some(tail)),
                    Err(invalid) => return Ok(Loaded::BadHint(segment.base, invalid)),
                }
            }
            None => (segment.base, None),
        };
        let len = segment.end();
        // A sealed segment never changes, so a hint for all of it stays
        // valid. Not fsynced: a hint that didn't land whole fails its
        // checksum, and the next open removes it and reads the segment.
        let mut writer = None;
        if i < newest && hinted < len {
            let opened = match (hint, tail) {
                (Some((file, _)), Some(tail)) => hint::Writer::extend(file, tail),
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

/// Applies the records `segment`'s hint `file`, whose trailer is
/// `trailer`, describes to `index`, in order, `window` at a time, by
/// `threads` threads while this one reads on, or by this one alone.
/// Returns the log offset the hint covers to, and where to extend it from.
/// On `Err`, what it applied must be thrown away.
pub(super) fn read_hint<L: LogFile>(
    file: &impl LogFile,
    trailer: hint::Trailer,
    segment: &Segment<L>,
    index: &mut Index,
    window: usize,
    threads: usize,
) -> Result<(u64, hint::Tail), hint::Invalid> {
    if trailer.log_len > segment.len() {
        return Err(hint::Invalid::new(
            "the segment is shorter than the hint says",
        ));
    }
    let base = segment.base;
    let tail = applying(index, threads, |sent| {
        let mut changes = Window::default();
        let tail = hint::read_entries(file, trailer, |fields, key, offset| {
            let entry = match fields.kind {
                Kind::Set => Some(Entry::new(base + offset, fields)),
                Kind::Delete => None,
                Kind::Batch => return,
            };
            changes.push(key, entry);
            if changes.len() == window {
                sent(mem::take(&mut changes));
            }
        });
        sent(changes);
        tail
    })?;
    Ok((base + trailer.log_len, tail))
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
