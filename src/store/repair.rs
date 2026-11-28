//! `repair` (DESIGN.md, Recovery).

use std::io::{self, ErrorKind, Read};
use std::path::{Path, PathBuf};

use super::{
    LOCK_FILE_NAME, LOG_FILE_NAME, Repaired, Store, lock, open::open_segments, remove_if_present,
    scan::scan,
};
use crate::fs::{Fs, LogFile};
use crate::index::Index;
use crate::segment::{compact_name, create, hint_name, log_name};
use crate::{CorruptionReason, Error};

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
}
