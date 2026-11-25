//! One file of the log (DESIGN.md, Segments). A segment is named for the
//! log offset its first byte has, so offsets in the index, in errors and in
//! records are offsets into the whole log, and `Segment` translates them to
//! offsets in its file.

use std::fs::TryLockError;
use std::io::{self, Read};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::fs::sealed::Sealed;
use crate::fs::{LogFile, Map};

const PREFIX: &str = "tally.";
const DIGITS: usize = 20;

/// `tally.<base, 20 digits>.log`: sorting the names sorts by base.
pub(crate) fn log_name(base: u64) -> String {
    format!("{PREFIX}{base:0DIGITS$}.log")
}

pub(crate) fn hint_name(base: u64) -> String {
    format!("{PREFIX}{base:0DIGITS$}.hint")
}

/// The base of a segment named `name`, if `name` is one.
pub(crate) fn parse_log_name(name: &str) -> Option<u64> {
    parse(name, ".log")
}

/// The base of the segment a hint named `name` describes, if it is a hint.
pub(crate) fn parse_hint_name(name: &str) -> Option<u64> {
    parse(name, ".hint")
}

fn parse(name: &str, suffix: &str) -> Option<u64> {
    let digits = name.strip_prefix(PREFIX)?.strip_suffix(suffix)?;
    if digits.len() != DIGITS || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// One segment file. Readers share it while writes land in it, so its
/// length and dead bytes are atomics; only the store's writer changes them.
pub(crate) struct Segment<L> {
    pub base: u64,
    pub file: L,
    /// Bytes in the file. An index entry only points below it: the length
    /// grows before an entry points past the old end.
    len: AtomicU64,
    /// Bytes in the file no live key points at.
    dead: AtomicU64,
    /// The file mapped into memory once it is sealed, so reads within it
    /// copy instead of making a system call.
    map: OnceLock<Map>,
}

impl<L> Segment<L> {
    pub fn new(base: u64, file: L, len: u64) -> Segment<L> {
        Segment {
            base,
            file,
            len: AtomicU64::new(len),
            dead: AtomicU64::new(0),
            map: OnceLock::new(),
        }
    }

    pub fn len(&self) -> u64 {
        self.len.load(Ordering::Acquire)
    }

    pub fn set_len_written(&self, len: u64) {
        self.len.store(len, Ordering::Release);
    }

    /// The log offset just past this segment's last byte.
    pub fn end(&self) -> u64 {
        self.base + self.len()
    }

    /// Whether the byte at log offset `offset` is in this segment.
    pub fn holds(&self, offset: u64) -> bool {
        self.base <= offset && offset < self.end()
    }

    pub fn dead(&self) -> u64 {
        self.dead.load(Ordering::Relaxed)
    }

    pub fn add_dead(&self, bytes: u64) {
        self.dead.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn set_dead(&self, bytes: u64) {
        self.dead.store(bytes, Ordering::Relaxed);
    }

    pub fn is_mapped(&self) -> bool {
        self.map.get().is_some()
    }
}

impl<L: LogFile> Segment<L> {
    /// Maps the file, which must take no more writes or truncation from
    /// here on.
    pub fn seal(&self) {
        if let Some(map) = self.file.map(self.len()) {
            let _ = self.map.set(map);
        }
    }
}

impl<L> Sealed for Segment<L> {}

/// The file, seen at log offsets: `size` is `base` plus the file's size.
impl<L: LogFile> LogFile for Segment<L> {
    fn size(&self) -> io::Result<u64> {
        Ok(self.base + self.file.size()?)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        let at = offset - self.base;
        let mapped = self.map.get().and_then(|map| {
            let start = usize::try_from(at).ok()?;
            map.bytes().get(start..start.checked_add(buf.len())?)
        });
        match mapped {
            Some(bytes) => {
                buf.copy_from_slice(bytes);
                Ok(())
            }
            None => self.file.read_exact_at(buf, at),
        }
    }

    fn reader_at(&self, offset: u64) -> impl Read + '_ {
        self.file.reader_at(offset - self.base)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        assert!(!self.is_mapped(), "a sealed segment takes no writes");
        self.file.write_all_at(buf, offset - self.base)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        assert!(!self.is_mapped(), "a sealed segment is never truncated");
        self.file.set_len(len - self.base)
    }

    fn sync(&self) -> io::Result<()> {
        self.file.sync()
    }

    fn try_lock(&self) -> Result<(), TryLockError> {
        self.file.try_lock()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::{Fs, OsFs};

    fn sealed(bytes: &[u8], base: u64) -> (tempfile::TempDir, Segment<crate::fs::OsFile>) {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(log_name(base));
        std::fs::write(&path, bytes).unwrap();
        let segment = Segment::new(base, OsFs.open_existing(&path).unwrap(), bytes.len() as u64);
        segment.seal();
        (tmp, segment)
    }

    #[test]
    fn a_sealed_segment_reads_from_its_map() {
        let (_tmp, segment) = sealed(b"0123456789", 100);
        assert_eq!(segment.map.get().map(|m| m.bytes().len()), Some(10));
        let mut buf = [0u8; 4];
        segment.read_exact_at(&mut buf, 103).unwrap();
        assert_eq!(&buf, b"3456");
    }

    #[test]
    fn a_read_past_the_map_goes_to_the_file() {
        let (_tmp, segment) = sealed(b"0123456789", 100);
        let segment = Segment::new(100, segment.file, 6);
        segment.seal();
        let mut buf = [0u8; 4];
        segment.read_exact_at(&mut buf, 104).unwrap();
        assert_eq!(&buf, b"4567");
        assert!(segment.read_exact_at(&mut buf, 108).is_err());
    }

    #[test]
    fn an_empty_segment_is_not_mapped() {
        let (_tmp, segment) = sealed(b"", 0);
        assert!(!segment.is_mapped());
    }

    #[test]
    #[should_panic(expected = "a sealed segment takes no writes")]
    fn a_sealed_segment_refuses_writes() {
        let (_tmp, segment) = sealed(b"0123456789", 0);
        let _ = segment.write_all_at(b"x", 10);
    }

    #[test]
    #[should_panic(expected = "a sealed segment is never truncated")]
    fn a_sealed_segment_refuses_truncation() {
        let (_tmp, segment) = sealed(b"0123456789", 0);
        let _ = segment.set_len(5);
    }

    #[test]
    fn names_carry_the_base_in_20_digits() {
        assert_eq!(log_name(17), "tally.00000000000000000017.log");
        assert_eq!(hint_name(17), "tally.00000000000000000017.hint");
        assert_eq!(parse_log_name(&log_name(u64::MAX)), Some(u64::MAX));
        assert_eq!(parse_hint_name(&hint_name(0)), Some(0));
    }

    #[test]
    fn other_names_are_not_segments() {
        for name in [
            "tally.log",
            "tally.lock",
            "tally.hint",
            "tally.0017.log",
            "tally.000000000000000000017.log",
            "tally.0000000000000000001x.log",
            "tally.00000000000000000017.log.corrupt-3",
            "tally.99999999999999999999.log",
        ] {
            assert_eq!(parse_log_name(name), None, "{name}");
        }
        assert_eq!(parse_hint_name(&log_name(1)), None);
    }
}
