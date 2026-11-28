//! `repair` (DESIGN.md, Recovery).

use std::io::{self, ErrorKind, Read};
use std::ops::Range;
use std::path::{Path, PathBuf};

use super::{
    LOCK_FILE_NAME, LOG_FILE_NAME, Repaired, Store, lock, open::open_segments, remove_if_present,
    scan::scan,
};
use crate::fs::{Fs, LogFile};
use crate::index::Index;
use crate::record::IndexOp;
use crate::segment::{compact_name, create, hint_name, log_name};
use crate::{CorruptionReason, Error};

/// What `repair` reads at a time, and fills with zeros if the read fails.
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

/// Whether `side` holds a prefix of what `copy_readable` writes for
/// `file`'s bytes from `offset` to `len`, as a repair that crashed partway
/// through copying them leaves it: each byte is the one it copies, or zero
/// where that write never landed.
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
    let mut ours = vec![0u8; BLOCK as usize];
    let mut theirs = vec![0u8; BLOCK as usize];
    for at in (0..side_len).step_by(BLOCK as usize) {
        let ours = &mut ours[..(side_len - at).min(BLOCK) as usize];
        side.read_exact_at(ours, at)?;
        let theirs = &mut theirs[..(len - offset - at).min(BLOCK) as usize];
        read_or_zeros(file, theirs, offset + at);
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

/// Appends the bytes of `file` from `offset` to `len` to the empty `side`,
/// a block at a time, with zeros for each block that fails to read, and
/// returns where in `side` those are.
fn copy_readable(
    file: &impl LogFile,
    offset: u64,
    len: u64,
    side: &impl LogFile,
) -> io::Result<Vec<Range<u64>>> {
    let mut buf = vec![0u8; BLOCK as usize];
    let mut unreadable: Vec<Range<u64>> = Vec::new();
    for at in (0..len - offset).step_by(BLOCK as usize) {
        let block = &mut buf[..(len - offset - at).min(BLOCK) as usize];
        let end = at + block.len() as u64;
        if !read_or_zeros(file, block, offset + at) {
            match unreadable.last_mut() {
                Some(last) if last.end == at => last.end = end,
                _ => unreadable.push(at..end),
            }
        }
        side.write_all_at(block, at)?;
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

/// Where `repair` saves the bytes from log offset `offset`.
fn corrupt_name(dir: &Path, offset: u64) -> PathBuf {
    dir.join(format!("{LOG_FILE_NAME}.corrupt-{offset}"))
}

/// Copies the bytes of `file` from `offset` to `len` to `side_file`, fsyncs
/// it, removes the hint of the segment at `base`, and fsyncs the
/// directory, so the bytes are durable outside the log before they leave
/// it. Returns where in `side_file` the bytes that failed to read are,
/// as zeros.
fn move_aside<F: Fs>(
    fs: &F,
    dir: &Path,
    side_file: &Path,
    file: &impl LogFile,
    offset: u64,
    len: u64,
    base: u64,
) -> Result<Vec<Range<u64>>, Error> {
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
    let unreadable = copy_readable(file, offset, len, &side)?;
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
    let unreadable = move_aside(fs, dir, &side_file, &file, 0, len, base)?;
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
        side_file,
        unreadable,
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
            segments => segments?,
        };
        let mut index = Index::new();
        let mut bad = None;
        for (i, segment) in segments.iter().enumerate() {
            let newest = i + 1 == segments.len();
            // Where the record or batch the scan reads next starts: an I/O
            // error falls in it.
            let mut reading = segment.base;
            let mut batch_end = 0;
            match scan(
                segment,
                &mut index,
                segment.base,
                segment.end(),
                newest,
                &mut |scanned| {
                    let end = scanned.offset + scanned.header.record_len();
                    match *scanned.op {
                        IndexOp::Batch { body_len } if body_len > 0 => {
                            reading = scanned.offset;
                            batch_end = end + body_len;
                        }
                        _ if end >= batch_end => reading = end,
                        _ => {}
                    }
                    Ok(())
                },
            ) {
                Ok(_) => {}
                Err(Error::Corruption { offset, .. }) => {
                    bad = Some((segment, offset));
                    break;
                }
                Err(Error::Io(_)) => {
                    bad = Some((segment, reading));
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
        let unreadable = move_aside(fs, dir, &side_file, segment, offset, len, segment.base)?;
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
            dropped: len - offset,
            side_file,
            unreadable,
        }))
    }
}
