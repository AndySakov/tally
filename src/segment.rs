//! One file of the log (DESIGN.md, Segments). A segment is named for the
//! log offset its first byte has, so offsets in the index, in errors and in
//! records are offsets into the whole log, and `Segment` translates them to
//! offsets in its file.

use std::fs::TryLockError;
use std::io::{self, Read};

use crate::fs::LogFile;
use crate::fs::sealed::Sealed;

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

pub(crate) struct Segment<L> {
    pub base: u64,
    pub file: L,
    /// Bytes in the file.
    pub len: u64,
    /// Bytes in the file no live key points at.
    pub dead: u64,
}

impl<L> Segment<L> {
    /// The log offset just past this segment's last byte.
    pub fn end(&self) -> u64 {
        self.base + self.len
    }
}

impl<L> Sealed for Segment<L> {}

/// The file, seen at log offsets: `size` is `base` plus the file's size.
impl<L: LogFile> LogFile for Segment<L> {
    fn size(&self) -> io::Result<u64> {
        Ok(self.base + self.file.size()?)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.file.read_exact_at(buf, offset - self.base)
    }

    fn reader_at(&self, offset: u64) -> impl Read + '_ {
        self.file.reader_at(offset - self.base)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.file.write_all_at(buf, offset - self.base)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
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
