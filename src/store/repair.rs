//! `repair` (DESIGN.md, Recovery).

use std::io::{self, ErrorKind, Read};
use std::ops::Range;
use std::path::{Path, PathBuf};

use super::{
    LOCK_FILE_NAME, LOG_FILE_NAME, Repaired, SideFile, Store, lock,
    open::open_segments,
    remove_if_present,
    scan::{Scanned, scan, scan_into},
};
use crate::fs::{Fs, LogFile};
use crate::hint;
use crate::index::Index;
use crate::record::IndexOp;
use crate::segment::{FILE_HEADER_LEN, Segment, compact_name, create, hint_name, log_name};
use crate::{CorruptionReason, Error};

/// The blocks of a segment's file `repair` reads, and fills with zeros if
/// the read fails.
const BLOCK: u64 = 4096;

/// Reads `buf.len()` bytes of `file` at `at`, or fills `buf` with zeros
/// and returns false if they fail to read.
fn read_or_zeros(file: &impl LogFile, buf: &mut [u8], at: u64) -> bool {
    let read = file.read_exact_at(buf, at).is_ok();
    if !read {
        buf.fill(0);
    }
    read
}

/// The blocks of the `n` bytes `repair` moves, in side-file offsets, when
/// the first is at `file_offset` in its file: the file's 4 KiB blocks, so
/// the first can be short.
fn blocks(n: u64, file_offset: u64) -> impl Iterator<Item = Range<u64>> {
    let first = BLOCK - file_offset % BLOCK;
    std::iter::successors((n > 0).then(|| 0..first.min(n)), move |block| {
        (block.end < n).then(|| block.end..(block.end + BLOCK).min(n))
    })
}

/// Whether `side` holds a prefix of what `copy_readable` writes for the
/// `bytes` of `file`, as a repair that crashed partway through copying
/// them leaves it: each byte is the one it copies, or zero where that
/// write never landed.
fn starts_tail(
    side: &impl LogFile,
    file: &impl LogFile,
    bytes: &Range<u64>,
    file_offset: u64,
) -> io::Result<bool> {
    let side_len = side.size()?;
    if side_len > bytes.end - bytes.start {
        return Ok(false);
    }
    let mut ours = vec![0u8; BLOCK as usize];
    let mut theirs = vec![0u8; BLOCK as usize];
    for block in blocks(bytes.end - bytes.start, file_offset) {
        if block.start >= side_len {
            break;
        }
        let ours = &mut ours[..(block.end.min(side_len) - block.start) as usize];
        side.read_exact_at(ours, block.start)?;
        let theirs = &mut theirs[..(block.end - block.start) as usize];
        read_or_zeros(file, theirs, bytes.start + block.start);
        if ours
            .iter()
            .zip(&*theirs)
            .any(|(&ours, &theirs)| ours != 0 && ours != theirs)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Appends the `bytes` of `file`, the first at `file_offset` in it, to the
/// empty `side`, a block at a time, with zeros for each block that fails
/// to read, and returns where in `side` those are.
fn copy_readable(
    file: &impl LogFile,
    bytes: &Range<u64>,
    file_offset: u64,
    side: &impl LogFile,
) -> io::Result<Vec<Range<u64>>> {
    let mut buf = vec![0u8; BLOCK as usize];
    let mut unreadable: Vec<Range<u64>> = Vec::new();
    for block in blocks(bytes.end - bytes.start, file_offset) {
        let buf = &mut buf[..(block.end - block.start) as usize];
        if !read_or_zeros(file, buf, bytes.start + block.start) {
            match unreadable.last_mut() {
                Some(last) if last.end == block.start => last.end = block.end,
                _ => unreadable.push(block.clone()),
            }
        }
        side.write_all_at(buf, block.start)?;
    }
    Ok(unreadable)
}

/// Appends everything `reader` yields to `file`, in chunks.
pub(super) fn copy_to_end(mut reader: impl Read, file: &impl LogFile) -> io::Result<()> {
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

/// Where the record or batch a scan reads next starts, from the records it
/// has shown: an I/O error falls in it.
struct Reading {
    at: u64,
    batch_end: u64,
}

impl Reading {
    fn from(at: u64) -> Reading {
        Reading { at, batch_end: 0 }
    }

    fn saw(&mut self, scanned: &Scanned) {
        let end = scanned.offset + scanned.header.record_len();
        match *scanned.op {
            IndexOp::Batch { body_len } if body_len > 0 => {
                self.at = scanned.offset;
                self.batch_end = end + body_len;
            }
            _ if end >= self.batch_end => self.at = end,
            _ => {}
        }
    }
}

/// Where the damage is in `file` from `start`, where a scan's read failed,
/// to `len`: read again unbuffered, each header and payload in a read of
/// its own, so that it is the first record or batch with a byte that fails
/// to read, not the first one a larger read reached. `None` if it all
/// reads.
fn damage_after_a_failed_read(
    file: &impl LogFile,
    start: u64,
    len: u64,
) -> Result<Option<u64>, Error> {
    let mut reading = Reading::from(start);
    match scan_into(file, start, len, 1, &mut |_, _| {}, &mut |scanned| {
        reading.saw(&scanned);
        Ok(())
    }) {
        Ok(()) => Ok(None),
        Err(Error::Corruption { offset, .. }) => Ok(Some(offset)),
        Err(Error::Io(_)) => Ok(Some(reading.at)),
        Err(err) => Err(err),
    }
}

/// Where `repair` saves the bytes from log offset `offset`.
fn corrupt_name(dir: &Path, offset: u64) -> PathBuf {
    dir.join(format!("{LOG_FILE_NAME}.corrupt-{offset}"))
}

/// Copies the `bytes` of `file`, the first at `file_offset` in its file, to
/// `side_file`, fsyncs it, removes the hint of the segment at `base`, and fsyncs the
/// directory, so the bytes are durable outside the log before they leave
/// it. Returns where in `side_file` the bytes that failed to read are,
/// as zeros.
fn move_aside<F: Fs>(
    fs: &F,
    dir: &Path,
    side_file: &Path,
    file: &impl LogFile,
    bytes: Range<u64>,
    file_offset: u64,
    base: u64,
) -> Result<Vec<Range<u64>>, Error> {
    let side = match fs.open_existing(side_file) {
        Ok(existing) => {
            if !starts_tail(&existing, file, &bytes, file_offset)? {
                return Err(Error::SideFileExists(side_file.to_path_buf()));
            }
            existing
        }
        Err(err) if err.kind() == ErrorKind::NotFound => fs.open_or_create(side_file)?,
        Err(err) => return Err(err.into()),
    };
    side.set_len(0)?;
    let unreadable = copy_readable(file, &bytes, file_offset, &side)?;
    side.sync()?;
    remove_if_present(fs, &dir.join(hint_name(base)))?;
    fs.sync_dir(dir)?;
    Ok(unreadable)
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
    let unreadable = move_aside(fs, dir, &side_file, &file, 0..len, 0, base)?;
    if !unreadable.is_empty() {
        log::warn!(
            "repair could not read bytes {unreadable:?} of {}: saved them as zeros in {}",
            path.display(),
            side_file.display()
        );
    }
    let empty = dir.join(compact_name(base));
    create(fs, &empty, base)?;
    fs.rename(&empty, &path)?;
    fs.sync_dir(dir)?;
    Ok(Repaired {
        offset: base,
        dropped: len,
        side_file: Some(SideFile {
            path: side_file,
            unreadable,
        }),
    })
}

impl<F: Fs> Store<F> {
    pub(crate) fn repair_in(fs: &F, dir: &Path) -> Result<Option<Repaired>, Error> {
        let _lock = fs.open_or_create(&dir.join(LOCK_FILE_NAME))?;
        lock(&_lock)?;
        let segments = match open_segments(fs, dir) {
            Err(Error::Corruption {
                offset: base,
                reason: CorruptionReason::SegmentHeader | CorruptionReason::CutShort,
            }) => return repair_file_header(fs, dir, base).map(Some),
            opened => opened?.segments,
        };
        let mut index = Index::new();
        let mut bad = None;
        for (i, segment) in segments.iter().enumerate() {
            let lost = lost_past_hint(fs, dir, segment)?;
            if lost.is_some() {
                remove_if_present(fs, &dir.join(hint_name(segment.base)))?;
                fs.sync_dir(dir)?;
            }
            let newest = i + 1 == segments.len();
            let mut reading = Reading::from(segment.base);
            let damage = match scan(
                segment,
                &mut index,
                segment.base,
                segment.end(),
                newest,
                &mut |scanned| {
                    reading.saw(&scanned);
                    Ok(())
                },
            ) {
                Ok(_) => None,
                Err(Error::Corruption { offset, .. }) => Some(offset),
                Err(Error::Io(_)) => {
                    damage_after_a_failed_read(segment, reading.at, segment.end())?
                }
                Err(err) => return Err(err),
            };
            if let Some(offset) = damage {
                bad = Some((segment, offset, lost.unwrap_or(0)));
                break;
            }
            if let Some(lost) = lost {
                return Ok(Some(Repaired {
                    offset: segment.end(),
                    dropped: lost,
                    side_file: None,
                }));
            }
        }
        let Some((segment, offset, lost)) = bad else {
            return Ok(None);
        };
        let len = segment.end();
        let side_file = corrupt_name(dir, offset);
        let unreadable = move_aside(
            fs,
            dir,
            &side_file,
            segment,
            offset..len,
            offset - segment.base + FILE_HEADER_LEN,
            segment.base,
        )?;
        if !unreadable.is_empty() {
            let in_log: Vec<_> = unreadable
                .iter()
                .map(|r| offset + r.start..offset + r.end)
                .collect();
            log::warn!(
                "repair could not read log offsets {in_log:?}: saved them as zeros in {}",
                side_file.display()
            );
        }
        segment.set_len(offset)?;
        segment.sync()?;
        Ok(Some(Repaired {
            offset,
            dropped: len - offset + lost,
            side_file: Some(SideFile {
                path: side_file,
                unreadable,
            }),
        }))
    }
}

/// The bytes past `segment`'s end that its hint lists, if a hint whose
/// checksum matches lists more than the segment holds: what `open` reports
/// as `CutShort` at its end.
fn lost_past_hint<F: Fs>(
    fs: &F,
    dir: &Path,
    segment: &Segment<F::File>,
) -> Result<Option<u64>, Error> {
    let file = match fs.open_existing(&dir.join(hint_name(segment.base))) {
        Ok(file) => file,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    if !hint::checksum_matches(&file)? {
        return Ok(None);
    }
    let Ok(trailer) = hint::trailer(&file) else {
        return Ok(None);
    };
    Ok(trailer
        .log_len
        .checked_sub(segment.len())
        .filter(|&lost| lost > 0))
}
