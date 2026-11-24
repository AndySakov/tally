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

    /// Makes the creation of names inside the directory `path` durable.
    fn sync_dir(&self, path: &Path) -> io::Result<()>;
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

    /// Makes the contents and length durable. Returns the error instead of
    /// panicking: the panic policy belongs to `Store`.
    fn sync(&self) -> io::Result<()>;

    /// Takes an exclusive lock, released when the file is dropped.
    fn try_lock(&self) -> Result<(), TryLockError>;
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

    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        if cfg!(fuzzing) {
            return Ok(());
        }
        File::open(path)?.sync_all()
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
        // Fuzzing can't observe durability, and a 5 ms fsync per run would
        // cut fuzzing throughput a hundredfold.
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
