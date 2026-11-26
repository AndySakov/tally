//! One file of the log (DESIGN.md, Segments). A segment is named for the
//! log offset its first record has, so offsets in the index, in errors and
//! in records are offsets into the whole log, and `Segment` translates them
//! to offsets in its file, past the file's header.

use std::fs::TryLockError;
use std::io::{self, Read};
use std::path::Path;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::filter::Filter;
use crate::fs::sealed::Sealed;
use crate::fs::{Fs, LogFile};

/// Every segment file starts with this many bytes, outside the log's
/// offsets: a magic number, the format version, and the segment's base,
/// under a CRC32C.
pub(crate) const FILE_HEADER_LEN: u64 = 17;
const MAGIC: &[u8; 4] = b"TLYS";
const VERSION: u8 = 3;

fn file_header(base: u64) -> [u8; FILE_HEADER_LEN as usize] {
    let mut header = [0u8; FILE_HEADER_LEN as usize];
    header[..4].copy_from_slice(MAGIC);
    header[4] = VERSION;
    header[5..13].copy_from_slice(&base.to_le_bytes());
    let checksum = crc32c::crc32c(&header[..13]);
    header[13..].copy_from_slice(&checksum.to_le_bytes());
    header
}

/// What a segment file's header says.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FileHeader {
    /// This format, for this base.
    Valid,
    /// No more than a header's length, and not the header: a crash cut its
    /// write short, or it landed over stale blocks. No record can be in it.
    Torn,
    /// The magic and version are right, but the checksum or base isn't.
    Damaged,
    /// Not a segment of this format.
    Foreign,
}

pub(crate) fn check_file_header(file: &impl LogFile, base: u64) -> io::Result<FileHeader> {
    let expected = file_header(base);
    let size = file.size()?;
    let mut bytes = [0u8; FILE_HEADER_LEN as usize];
    let have = &mut bytes[..size.min(FILE_HEADER_LEN) as usize];
    file.read_exact_at(have, 0)?;
    let other_version = have.len() > 4 && have[..4] == *MAGIC && have[4] != VERSION;
    if size <= FILE_HEADER_LEN && !other_version {
        return Ok(if have[..] == expected[..] {
            FileHeader::Valid
        } else {
            FileHeader::Torn
        });
    }
    Ok(if bytes[..5] != expected[..5] {
        FileHeader::Foreign
    } else if bytes != expected {
        FileHeader::Damaged
    } else {
        FileHeader::Valid
    })
}

/// The bytes of a segment file at `base` holding `records`.
#[cfg(any(test, fuzzing))]
pub fn segment_file(base: u64, records: &[u8]) -> Vec<u8> {
    [&file_header(base)[..], records].concat()
}

/// Empties `file` and writes a header for `base`, fsynced. The caller
/// makes its name durable.
pub(crate) fn start_file(file: &impl LogFile, base: u64) -> io::Result<()> {
    file.set_len(0)?;
    file.write_all_at(&file_header(base), 0)?;
    file.sync()
}

/// Creates the file at `path` as an empty segment at `base`.
pub(crate) fn create<F: Fs>(fs: &F, path: &Path, base: u64) -> io::Result<Segment<F::File>> {
    let file = fs.open_or_create(path)?;
    start_file(&file, base)?;
    Ok(Segment::new(base, file, 0))
}

const PREFIX: &str = "tally.";
const DIGITS: usize = 20;

/// `tally.<base, 20 digits>.log`: sorting the names sorts by base.
pub(crate) fn log_name(base: u64) -> String {
    format!("{PREFIX}{base:0DIGITS$}.log")
}

pub(crate) fn hint_name(base: u64) -> String {
    format!("{PREFIX}{base:0DIGITS$}.hint")
}

/// Where compaction writes a segment at `base` before renaming it in.
pub(crate) fn compact_name(base: u64) -> String {
    format!("{PREFIX}{base:0DIGITS$}.compact")
}

/// The base of a segment named `name`, if `name` is one.
pub(crate) fn parse_log_name(name: &str) -> Option<u64> {
    parse(name, ".log")
}

/// The base of the segment a hint named `name` describes, if it is a hint.
pub(crate) fn parse_hint_name(name: &str) -> Option<u64> {
    parse(name, ".hint")
}

pub(crate) fn parse_compact_name(name: &str) -> Option<u64> {
    parse(name, ".compact")
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
    /// Bytes of records in the file. An index entry only points below it:
    /// the length grows before an entry points past the old end.
    len: AtomicU64,
    /// Bytes in the file no live key points at.
    dead: AtomicU64,
    /// The keys it holds a Set record for, once a compaction has asked.
    /// Only ever built for a sealed segment, which no longer changes.
    pub sets: OnceLock<Filter>,
}

impl<L> Segment<L> {
    pub fn new(base: u64, file: L, len: u64) -> Segment<L> {
        Segment {
            base,
            file,
            len: AtomicU64::new(len),
            dead: AtomicU64::new(0),
            sets: OnceLock::new(),
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
}

impl<L> Sealed for Segment<L> {}

impl<L> Segment<L> {
    /// Where the byte at log offset `offset` is in the file.
    fn in_file(&self, offset: u64) -> u64 {
        offset - self.base + FILE_HEADER_LEN
    }
}

/// The file's records, seen at log offsets: `size` is where they end.
impl<L: LogFile> LogFile for Segment<L> {
    fn size(&self) -> io::Result<u64> {
        Ok(self.base + self.file.size()?.saturating_sub(FILE_HEADER_LEN))
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.file.read_exact_at(buf, self.in_file(offset))
    }

    fn reader_at(&self, offset: u64) -> impl Read + '_ {
        self.file.reader_at(self.in_file(offset))
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.file.write_all_at(buf, self.in_file(offset))
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.file.set_len(self.in_file(len))
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
    use crate::mem_fs::MemFs;

    /// Computed independently of this crate, with a Python CRC32C.
    const HEADER_AT_100: [u8; 17] = [
        0x54, 0x4c, 0x59, 0x53, // "TLYS"
        0x03, // version
        0x64, 0, 0, 0, 0, 0, 0, 0, // base: 100
        0x8c, 0xde, 0x35, 0x8d, // CRC32C of the 13 bytes before
    ];

    #[test]
    fn a_new_segment_starts_with_its_header() {
        let fs = MemFs::new();
        let path = Path::new("/s");
        let segment = create(&fs, path, 100).unwrap();
        assert_eq!(fs.read(path).unwrap(), HEADER_AT_100);
        assert_eq!(segment.len(), 0);
        assert_eq!(
            check_file_header(&segment.file, 100).unwrap(),
            FileHeader::Valid
        );
    }

    #[test]
    fn a_segment_reads_and_writes_at_log_offsets_past_its_header() {
        let fs = MemFs::new();
        let path = Path::new("/s");
        let segment = create(&fs, path, 100).unwrap();
        segment.write_all_at(b"0123456789", 100).unwrap();
        let mut buf = [0u8; 4];
        segment.read_exact_at(&mut buf, 103).unwrap();
        assert_eq!(&buf, b"3456");
        assert_eq!(segment.size().unwrap(), 110);
        segment.set_len(105).unwrap();
        assert_eq!(fs.read(path).unwrap()[17..], *b"01234");
    }

    #[test]
    fn a_file_header_is_checked_against_the_base() {
        let check = |bytes: &[u8], base| {
            let fs = MemFs::new();
            fs.put(Path::new("/s"), bytes);
            check_file_header(&fs.open_existing(Path::new("/s")).unwrap(), base).unwrap()
        };
        let mut with_records = HEADER_AT_100.to_vec();
        with_records.push(0x31);
        assert_eq!(check(&with_records, 100), FileHeader::Valid);
        assert_eq!(check(&with_records, 0), FileHeader::Damaged);
        assert_eq!(check(&HEADER_AT_100[..9], 100), FileHeader::Torn);
        assert_eq!(check(&[0; 17], 100), FileHeader::Torn);
        assert_eq!(check(b"", 100), FileHeader::Torn);
        let mut zeroed_base = HEADER_AT_100;
        zeroed_base[5] = 0;
        assert_eq!(check(&zeroed_base, 100), FileHeader::Torn);
        assert_eq!(check(&[0xab; 17], 100), FileHeader::Torn, "no record fits");
        assert_eq!(check(&[0; 18], 100), FileHeader::Foreign);
        let mut other_version = HEADER_AT_100;
        other_version[4] = 2;
        assert_eq!(check(&other_version, 100), FileHeader::Foreign);
        let mut damaged = with_records.clone();
        damaged[14] ^= 1;
        assert_eq!(check(&damaged, 100), FileHeader::Damaged);
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
