//! The hint file (DESIGN.md, Hint file): the key and value length of every
//! record in a compacted log, so `open` can rebuild the index without
//! reading values.
//!
//! ```text
//! magic "TLYH" | version u8 | entries | count u64 | log_len u64 | crc32c u32
//! entry: key_len u16 | value_len u32 | key
//! ```
//!
//! Little-endian. A compacted log holds exactly the live records end to end,
//! so offsets aren't stored: each is the sum of the record lengths before
//! it, and the sum of them all must be `log_len`. The checksum covers every
//! byte before it. The count and log length trail the entries because files
//! are only written at the end, and neither is known until compaction is.

use std::io::{self, BufReader, Read};

use crate::fs::LogFile;
use crate::record::HEADER_LEN;
use crate::{Key, MAX_VALUE_LEN};

pub(crate) const HINT_FILE_NAME: &str = "tally.hint";
pub(crate) const COMPACT_HINT_FILE_NAME: &str = "tally.hint.compact";

const MAGIC: &[u8; 4] = b"TLYH";
const VERSION: u8 = 1;
const PREFIX_LEN: u64 = 5;
const TRAILER_LEN: u64 = 20;
const ENTRY_HEADER_LEN: usize = 6;
const FLUSH_AT: usize = 1 << 20;

/// What a hint file covers, from its trailer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Trailer {
    pub count: u64,
    pub log_len: u64,
}

/// Why a hint file was not used. Any of these means a full scan instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Invalid(pub &'static str);

impl From<io::Error> for Invalid {
    fn from(_: io::Error) -> Invalid {
        Invalid("unreadable")
    }
}

/// Writes a hint file to an empty `file`, one entry per record of the
/// compacted log, in log order.
pub(crate) struct Writer<'a, F: LogFile> {
    file: &'a F,
    buffer: Vec<u8>,
    written: u64,
    checksum: u32,
    count: u64,
}

impl<'a, F: LogFile> Writer<'a, F> {
    pub(crate) fn new(file: &'a F) -> Writer<'a, F> {
        let mut buffer = Vec::with_capacity(FLUSH_AT);
        buffer.extend_from_slice(MAGIC);
        buffer.push(VERSION);
        Writer {
            file,
            buffer,
            written: 0,
            checksum: 0,
            count: 0,
        }
    }

    pub(crate) fn push(&mut self, key: &[u8], value_len: u32) -> io::Result<()> {
        let key_len = u16::try_from(key.len()).expect("Key guarantees len fits u16");
        self.buffer.extend_from_slice(&key_len.to_le_bytes());
        self.buffer.extend_from_slice(&value_len.to_le_bytes());
        self.buffer.extend_from_slice(key);
        self.count += 1;
        if self.buffer.len() >= FLUSH_AT {
            self.flush()?;
        }
        Ok(())
    }

    /// Writes the trailer for a log of `log_len` bytes. The caller fsyncs.
    pub(crate) fn finish(mut self, log_len: u64) -> io::Result<()> {
        self.buffer.extend_from_slice(&self.count.to_le_bytes());
        self.buffer.extend_from_slice(&log_len.to_le_bytes());
        self.checksum = crc32c::crc32c_append(self.checksum, &self.buffer);
        let checksum = self.checksum;
        self.buffer.extend_from_slice(&checksum.to_le_bytes());
        self.file.write_all_at(&self.buffer, self.written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.write_all_at(&self.buffer, self.written)?;
        self.checksum = crc32c::crc32c_append(self.checksum, &self.buffer);
        self.written += self.buffer.len() as u64;
        self.buffer.clear();
        Ok(())
    }
}

/// The trailer, read on its own so the index can be sized before the
/// entries are read. Not checked: `read_entries` checks it with the rest.
pub(crate) fn trailer(file: &impl LogFile) -> Result<Trailer, Invalid> {
    let size = file.size()?;
    if size < PREFIX_LEN + TRAILER_LEN {
        return Err(Invalid("too short"));
    }
    let mut buf = [0u8; 16];
    file.read_exact_at(&mut buf, size - TRAILER_LEN)?;
    Ok(Trailer {
        count: u64::from_le_bytes(buf[..8].try_into().unwrap()),
        log_len: u64::from_le_bytes(buf[8..].try_into().unwrap()),
    })
}

/// Calls `entry(key, offset, value_len)` for each entry, then checks the
/// checksum and that the entries account for exactly `trailer`. On `Err`,
/// whatever `entry` saw must be thrown away.
pub(crate) fn read_entries(
    file: &impl LogFile,
    trailer: Trailer,
    mut entry: impl FnMut(Key, u64, u32),
) -> Result<(), Invalid> {
    let size = file.size()?;
    let mut reader = Checksummed {
        inner: BufReader::with_capacity(FLUSH_AT, file.reader_at(0)),
        checksum: 0,
    };
    let mut prefix = [0u8; PREFIX_LEN as usize];
    reader.read_exact(&mut prefix)?;
    if prefix[..4] != MAGIC[..] || prefix[4] != VERSION {
        return Err(Invalid("not a tally hint file"));
    }

    let entries_end = size - TRAILER_LEN;
    let mut at = PREFIX_LEN;
    let mut offset = 0u64;
    let mut count = 0u64;
    let mut header = [0u8; ENTRY_HEADER_LEN];
    let mut key = Vec::new();
    while at < entries_end {
        if entries_end - at < ENTRY_HEADER_LEN as u64 {
            return Err(Invalid("torn entry"));
        }
        reader.read_exact(&mut header)?;
        let key_len = u16::from_le_bytes([header[0], header[1]]);
        let value_len = u32::from_le_bytes(header[2..].try_into().unwrap());
        at += ENTRY_HEADER_LEN as u64;
        if entries_end - at < u64::from(key_len) {
            return Err(Invalid("torn entry"));
        }
        key.resize(usize::from(key_len), 0);
        reader.read_exact(&mut key)?;
        at += u64::from(key_len);
        if value_len == 0 || value_len as usize > MAX_VALUE_LEN {
            return Err(Invalid("bad value length"));
        }
        let key = Key::try_from(&key[..]).map_err(|_| Invalid("bad key length"))?;
        let record_len = (HEADER_LEN + usize::from(key_len)) as u64 + u64::from(value_len);
        if offset
            .checked_add(record_len)
            .is_none_or(|end| end > trailer.log_len)
        {
            return Err(Invalid("entries run past the log length"));
        }
        entry(key, offset, value_len);
        offset += record_len;
        count += 1;
    }

    let mut rest = [0u8; 16];
    reader.read_exact(&mut rest)?;
    let computed = reader.checksum;
    let mut stored = [0u8; 4];
    reader.inner.read_exact(&mut stored)?;
    if u32::from_le_bytes(stored) != computed {
        return Err(Invalid("checksum mismatch"));
    }
    if count != trailer.count || offset != trailer.log_len {
        return Err(Invalid("entries don't match the trailer"));
    }
    Ok(())
}

struct Checksummed<R> {
    inner: R,
    checksum: u32,
}

impl<R: Read> Read for Checksummed<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.checksum = crc32c::crc32c_append(self.checksum, &buf[..n]);
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::Fs;
    use crate::mem_fs::MemFs;
    use std::path::Path;

    fn write(entries: &[(&[u8], u32)], log_len: u64) -> Vec<u8> {
        let fs = MemFs::new();
        let path = Path::new("/hint");
        let file = fs.open_or_create(path).unwrap();
        let mut writer = Writer::new(&file);
        for (key, value_len) in entries {
            writer.push(key, *value_len).unwrap();
        }
        writer.finish(log_len).unwrap();
        fs.read(path).unwrap()
    }

    fn read(bytes: &[u8]) -> Result<Vec<(Vec<u8>, u64, u32)>, Invalid> {
        let fs = MemFs::new();
        fs.put(Path::new("/hint"), bytes);
        let file = fs.open_existing(Path::new("/hint")).unwrap();
        let trailer = trailer(&file)?;
        let mut seen = Vec::new();
        read_entries(&file, trailer, |key, offset, value_len| {
            seen.push((key.as_bytes().to_vec(), offset, value_len))
        })?;
        Ok(seen)
    }

    #[test]
    fn the_bytes_of_a_two_entry_hint() {
        let bytes = write(&[(b"a", 2), (b"bc", 1)], 18 + 18);
        let mut expected = b"TLYH\x01".to_vec();
        expected.extend_from_slice(&[1, 0, 2, 0, 0, 0, b'a']);
        expected.extend_from_slice(&[2, 0, 1, 0, 0, 0, b'b', b'c']);
        expected.extend_from_slice(&2u64.to_le_bytes());
        expected.extend_from_slice(&36u64.to_le_bytes());
        expected.extend_from_slice(&crc32c::crc32c(&expected).to_le_bytes());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn entries_come_back_with_their_offsets() {
        let bytes = write(&[(b"a", 2), (b"bc", 1)], 36);
        assert_eq!(
            read(&bytes),
            Ok(vec![(b"a".to_vec(), 0, 2), (b"bc".to_vec(), 18, 1)])
        );
    }

    #[test]
    fn an_empty_hint_covers_an_empty_log() {
        assert_eq!(read(&write(&[], 0)), Ok(vec![]));
    }

    #[test]
    fn every_one_byte_change_is_rejected() {
        let bytes = write(&[(b"a", 2), (b"bc", 1)], 36);
        for i in 0..bytes.len() {
            let mut bad = bytes.clone();
            bad[i] ^= 0x01;
            assert!(read(&bad).is_err(), "flipped byte {i} was accepted");
        }
    }

    #[test]
    fn every_truncation_is_rejected() {
        let bytes = write(&[(b"a", 2), (b"bc", 1)], 36);
        for len in 0..bytes.len() {
            assert!(read(&bytes[..len]).is_err(), "cut at {len} was accepted");
        }
    }

    #[test]
    fn entries_must_add_up_to_the_log_length() {
        assert_eq!(
            read(&write(&[(b"a", 2)], 19)),
            Err(Invalid("entries don't match the trailer"))
        );
        assert_eq!(
            read(&write(&[(b"a", 2)], 17)),
            Err(Invalid("entries run past the log length"))
        );
    }

    #[test]
    fn a_count_that_disagrees_is_rejected() {
        let mut bytes = write(&[(b"a", 2)], 18);
        let at = bytes.len() - 20;
        bytes[at] = 2;
        let crc_at = bytes.len() - 4;
        let crc = crc32c::crc32c(&bytes[..crc_at]);
        bytes[crc_at..].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(
            read(&bytes),
            Err(Invalid("entries don't match the trailer"))
        );
    }
}
