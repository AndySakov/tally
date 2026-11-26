//! The file system as `Store` sees it. [`OsFs`] is the real one. The
//! power-cut tests implement these traits with an in-memory disk that loses
//! whatever was not synced, which is why every operation whose durability
//! matters goes through them.
//!
//! Both traits are sealed: they are public only because `Store`'s type
//! parameter is, and cannot be implemented outside tally.

use std::fs::{File, TryLockError};
use std::io::{self, Read};
use std::os::unix::fs::FileExt;
use std::path::Path;

pub(crate) mod sealed {
    pub trait Sealed {}
}

/// Directories, and the files in them.
pub trait Fs: sealed::Sealed {
    type File: LogFile;

    /// Creates the directory `path`. Fails with `AlreadyExists` if it exists.
    /// The new name is durable only after `sync_dir` on the parent.
    fn create_dir(&self, path: &Path) -> io::Result<()>;

    /// Opens `path` for reading and appending, creating it if it doesn't
    /// exist. A new name is durable only after `sync_dir` on its directory.
    fn open_or_create(&self, path: &Path) -> io::Result<Self::File>;

    /// Opens an existing `path` for reading and appending.
    fn open_existing(&self, path: &Path) -> io::Result<Self::File>;

    /// Atomically points the name `to` at the file `from` names, replacing
    /// whatever `to` named. Durable only after `sync_dir` on the directory.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;

    /// Removes the name `path`. Durable only after `sync_dir` on its
    /// directory.
    fn remove(&self, path: &Path) -> io::Result<()>;

    /// Makes every name created, replaced, renamed away or removed inside the
    /// directory `path` durable.
    fn sync_dir(&self, path: &Path) -> io::Result<()>;

    /// The names of the files in the directory `path`, in no order.
    fn list(&self, path: &Path) -> io::Result<Vec<String>>;
}

/// One open file. Writes go only at the end of the file: the real file is
/// in append mode, where Linux ignores the offset and macOS honours it, so
/// writing anywhere else would behave differently on the two.
pub trait LogFile: sealed::Sealed {
    fn size(&self) -> io::Result<u64>;

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;

    /// A sequential reader starting at `offset`.
    fn reader_at(&self, offset: u64) -> impl Read + '_;

    /// Writes all of `buf` at `offset`, which must be the current length.
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()>;

    fn set_len(&self, len: u64) -> io::Result<()>;

    /// Makes the contents and length durable.
    fn sync(&self) -> io::Result<()>;

    /// Takes an exclusive lock, released when the file is dropped.
    fn try_lock(&self) -> Result<(), TryLockError>;
}

/// A buffer for reading `len` bytes with `reader_at`: `most` bytes, or
/// `len` if that is less, since a buffer is zeroed before its first read.
pub(crate) fn buffer_for(len: u64, most: usize) -> usize {
    usize::try_from(len).map_or(most, |len| len.min(most))
}

/// The operating system's file system.
#[derive(Debug, Clone, Copy, Default)]
pub struct OsFs;

impl sealed::Sealed for OsFs {}

impl Fs for OsFs {
    type File = OsFile;

    fn create_dir(&self, path: &Path) -> io::Result<()> {
        std::fs::create_dir(path)
    }

    fn open_or_create(&self, path: &Path) -> io::Result<OsFile> {
        File::options()
            .read(true)
            .append(true)
            .create(true)
            .open(path)
            .map(OsFile)
    }

    fn open_existing(&self, path: &Path) -> io::Result<OsFile> {
        File::options()
            .read(true)
            .append(true)
            .open(path)
            .map(OsFile)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        if cfg!(fuzzing) {
            return Ok(());
        }
        File::open(path)?.sync_all()
    }

    fn list(&self, path: &Path) -> io::Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in std::fs::read_dir(path)? {
            if let Ok(name) = entry?.file_name().into_string() {
                names.push(name);
            }
        }
        Ok(names)
    }
}

/// A file opened by [`OsFs`].
#[derive(Debug)]
pub struct OsFile(File);

impl sealed::Sealed for OsFile {}

impl LogFile for OsFile {
    fn size(&self) -> io::Result<u64> {
        Ok(self.0.metadata()?.len())
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.0.read_exact_at(buf, offset)
    }

    fn reader_at(&self, offset: u64) -> impl Read + '_ {
        OsReader {
            file: &self.0,
            offset,
        }
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.0.write_all_at(buf, offset)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.0.set_len(len)
    }

    fn sync(&self) -> io::Result<()> {
        // Fuzzing can't observe durability (DESIGN.md, Testing).
        if cfg!(fuzzing) {
            return Ok(());
        }
        self.0.sync_all()
    }

    fn try_lock(&self) -> Result<(), TryLockError> {
        self.0.try_lock()
    }
}

/// Positioned reads with a cursor, so no seek moves the file's shared one.
struct OsReader<'a> {
    file: &'a File,
    offset: u64,
}

impl Read for OsReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.file.read_at(buf, self.offset)?;
        self.offset += n as u64;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_read_buffer_is_no_larger_than_what_it_reads() {
        assert_eq!(buffer_for(53, 1 << 20), 53);
        assert_eq!(buffer_for(5 << 20, 1 << 20), 1 << 20);
    }
}
