//! The hint file (DESIGN.md, Hint file): the type and lengths of every
//! record at the start of a segment, so `open` can rebuild the index from
//! it without reading values.
//!
//! ```text
//! magic "TLYH" | version u8 | entries | count u64 | log_len u64 | crc64 u64
//! entry: type u8 | key_len u16 | value_len u32 | key
//! ```
//!
//! An entry is its record's header without the checksums (bytes 4 to 11),
//! then the key, so `type` gives the header's length. Every record has one,
//! Set, Delete or Batch, in log order, so offsets aren't stored: each is
//! the sum of the record lengths before it, and the sum of them all must be
//! `log_len`. `count` is the number of Set entries, at most the keys the
//! hint adds to the index. Little-endian. The checksum covers every byte
//! before it. The count and log length trail the entries because files are
//! only written at the end, and `open` extends a hint by writing over them.

use std::io::{self, BufReader, Read};

use crc_fast::{CrcAlgorithm, Digest};

use crate::fs::{LogFile, buffer_for};
use crate::record::{FIELDS_LEN, Fields, Kind, decode_fields};

const MAGIC: &[u8; 4] = b"TLYH";
const VERSION: u8 = 3;
const PREFIX_LEN: u64 = 5;
const TRAILER_LEN: u64 = 24;
const FLUSH_AT: usize = 1 << 20;

/// What a hint file covers, from its trailer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Trailer {
    pub count: u64,
    pub log_len: u64,
    /// The most Set entries the file is big enough to hold, to size an
    /// index by before `count` is checked.
    pub most_sets: u64,
}

/// Where a valid hint's entries end, so more can be written after them.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Tail {
    entries_end: u64,
    checksum: Digest,
    count: u64,
}

/// Why a hint file was not used. Any of these means a full scan instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Invalid(pub &'static str);

impl From<io::Error> for Invalid {
    fn from(_: io::Error) -> Invalid {
        Invalid("unreadable")
    }
}

/// Writes a hint file, one entry per record, in log order.
pub(crate) struct Writer<F: LogFile> {
    file: F,
    buffer: Vec<u8>,
    written: u64,
    checksum: Digest,
    count: u64,
}

impl<F: LogFile> Writer<F> {
    /// A hint in the empty `file`.
    pub(crate) fn new(file: F) -> Writer<F> {
        let mut buffer = Vec::with_capacity(FLUSH_AT);
        buffer.extend_from_slice(MAGIC);
        buffer.push(VERSION);
        Writer {
            file,
            buffer,
            written: 0,
            checksum: Digest::new(CrcAlgorithm::Crc64Nvme),
            count: 0,
        }
    }

    /// More entries for the hint in `file` that `tail` came from. Cuts off
    /// its trailer first, so it is invalid until `finish`.
    pub(crate) fn extend(file: F, tail: Tail) -> io::Result<Writer<F>> {
        file.set_len(tail.entries_end)?;
        Ok(Writer {
            file,
            buffer: Vec::with_capacity(FLUSH_AT),
            written: tail.entries_end,
            checksum: tail.checksum,
            count: tail.count,
        })
    }

    /// The entry for the record `fields` describes, whose key is `key`
    /// (empty for a Batch).
    pub(crate) fn push(&mut self, fields: Fields, key: &[u8]) -> io::Result<()> {
        debug_assert_eq!(usize::from(fields.key_len), key.len());
        self.buffer.extend_from_slice(&fields.to_bytes());
        self.buffer.extend_from_slice(key);
        if fields.kind == Kind::Set {
            self.count += 1;
        }
        if self.buffer.len() >= FLUSH_AT {
            self.flush()?;
        }
        Ok(())
    }

    /// Writes the trailer for the first `log_len` bytes of a segment, and
    /// returns the file.
    pub(crate) fn finish(mut self, log_len: u64) -> io::Result<F> {
        self.buffer.extend_from_slice(&self.count.to_le_bytes());
        self.buffer.extend_from_slice(&log_len.to_le_bytes());
        self.checksum.update(&self.buffer);
        let checksum = self.checksum.finalize();
        self.buffer.extend_from_slice(&checksum.to_le_bytes());
        self.file.write_all_at(&self.buffer, self.written)?;
        Ok(self.file)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.write_all_at(&self.buffer, self.written)?;
        self.checksum.update(&self.buffer);
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
        most_sets: (size - PREFIX_LEN - TRAILER_LEN) / (FIELDS_LEN as u64 + 1),
    })
}

/// Checks each entry's fields and passes them to `entry` with its key and
/// its record's offset from the segment's start, in log order, then checks
/// the checksum and that the entries account for exactly `trailer`. On
/// `Err`, whatever `entry` saw must be thrown away.
pub(crate) fn read_entries(
    file: &impl LogFile,
    trailer: Trailer,
    mut entry: impl FnMut(Fields, &[u8], u64),
) -> Result<Tail, Invalid> {
    let size = file.size()?;
    let entries_end = size - TRAILER_LEN;
    // Checksummed under the buffer, so the checksum takes a buffer at a
    // time, not an entry.
    let mut reader = BufReader::with_capacity(
        buffer_for(entries_end, FLUSH_AT),
        Checksummed {
            inner: file.reader_at(0).take(entries_end),
            checksum: Digest::new(CrcAlgorithm::Crc64Nvme),
        },
    );
    let mut prefix = [0u8; PREFIX_LEN as usize];
    reader.read_exact(&mut prefix)?;
    if prefix[..4] != MAGIC[..] || prefix[4] != VERSION {
        return Err(Invalid("not a tally hint file"));
    }

    let mut at = PREFIX_LEN;
    let mut offset = 0u64;
    let mut count = 0u64;
    let mut header = [0u8; FIELDS_LEN];
    let mut key = Vec::new();
    while at < entries_end {
        if entries_end - at < FIELDS_LEN as u64 {
            return Err(Invalid("torn entry"));
        }
        reader.read_exact(&mut header)?;
        let fields = decode_fields(&header).map_err(|_| Invalid("bad entry"))?;
        at += FIELDS_LEN as u64;
        if entries_end - at < u64::from(fields.key_len) {
            return Err(Invalid("torn entry"));
        }
        key.resize(usize::from(fields.key_len), 0);
        reader.read_exact(&mut key)?;
        at += u64::from(fields.key_len);
        if offset
            .checked_add(fields.record_len())
            .is_none_or(|end| end > trailer.log_len)
        {
            return Err(Invalid("entries run past the log length"));
        }
        entry(fields, &key, offset);
        offset += fields.record_len();
        if fields.kind == Kind::Set {
            count += 1;
        }
    }

    let checksum = reader.get_ref().checksum;
    let mut rest = [0u8; TRAILER_LEN as usize];
    file.read_exact_at(&mut rest, entries_end)?;
    let mut computed = checksum;
    computed.update(&rest[..16]);
    if u64::from_le_bytes(rest[16..].try_into().unwrap()) != computed.finalize() {
        return Err(Invalid("checksum mismatch"));
    }
    if count != trailer.count || offset != trailer.log_len {
        return Err(Invalid("entries don't match the trailer"));
    }
    Ok(Tail {
        entries_end,
        checksum,
        count,
    })
}

struct Checksummed<R> {
    inner: R,
    checksum: Digest,
}

impl<R: Read> Read for Checksummed<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.checksum.update(&buf[..n]);
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::Fs;
    use crate::mem_fs::MemFs;
    use crate::record::Place;
    use std::path::Path;

    fn set(key: &[u8], value_len: u32) -> (Fields, Vec<u8>) {
        fields(Kind::Set, key, value_len)
    }

    fn fields(kind: Kind, key: &[u8], value_len: u32) -> (Fields, Vec<u8>) {
        placed(kind, Place::Top, key, value_len)
    }

    fn placed(kind: Kind, place: Place, key: &[u8], value_len: u32) -> (Fields, Vec<u8>) {
        let fields = Fields {
            kind,
            place,
            key_len: key.len() as u16,
            value_len,
        };
        (fields, key.to_vec())
    }

    fn write(entries: &[(Fields, Vec<u8>)], log_len: u64) -> Vec<u8> {
        let fs = MemFs::new();
        let path = Path::new("/hint");
        let mut writer = Writer::new(fs.open_or_create(path).unwrap());
        for (fields, key) in entries {
            writer.push(*fields, key).unwrap();
        }
        writer.finish(log_len).unwrap();
        fs.read(path).unwrap()
    }

    type Entries = Vec<(Fields, Vec<u8>, u64)>;

    fn read(bytes: &[u8]) -> Result<Entries, Invalid> {
        let fs = MemFs::new();
        fs.put(Path::new("/hint"), bytes);
        let file = fs.open_existing(Path::new("/hint")).unwrap();
        let trailer = trailer(&file)?;
        let mut seen = Vec::new();
        read_entries(&file, trailer, |fields, key, offset| {
            seen.push((fields, key.to_vec(), offset))
        })?;
        Ok(seen)
    }

    fn at(entries: &[(Fields, Vec<u8>)], offsets: &[u64]) -> Entries {
        entries
            .iter()
            .zip(offsets)
            .map(|((fields, key), &offset)| (*fields, key.clone(), offset))
            .collect()
    }

    #[test]
    fn the_bytes_of_a_two_entry_hint() {
        let bytes = write(&[set(b"a", 2), set(b"bc", 1)], 22 + 22);
        let mut expected = b"TLYH\x03".to_vec();
        expected.extend_from_slice(&[0x31, 1, 0, 2, 0, 0, 0, b'a']);
        expected.extend_from_slice(&[0x31, 2, 0, 1, 0, 0, 0, b'b', b'c']);
        expected.extend_from_slice(&2u64.to_le_bytes());
        expected.extend_from_slice(&44u64.to_le_bytes());
        // CRC-64/NVME of the bytes above, computed in Python.
        expected.extend_from_slice(&0xc347098e2db51f9c_u64.to_le_bytes());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn entries_come_back_with_their_offsets() {
        let entries = [set(b"a", 2), set(b"bc", 1)];
        assert_eq!(read(&write(&entries, 44)), Ok(at(&entries, &[0, 22])));
    }

    #[test]
    fn the_bytes_of_a_batch_of_a_delete_and_a_set() {
        let entries = [
            fields(Kind::Batch, b"", 8),
            placed(Kind::Delete, Place::Inner, b"a", 0),
            placed(Kind::Set, Place::Inner, b"b", 1),
        ];
        let bytes = write(&entries, 27 + 20 + 21);
        let mut expected = b"TLYH\x03".to_vec();
        expected.extend_from_slice(&[0x33, 0, 0, 8, 0, 0, 0]);
        expected.extend_from_slice(&[0x72, 1, 0, 0, 0, 0, 0, b'a']);
        expected.extend_from_slice(&[0x71, 1, 0, 1, 0, 0, 0, b'b']);
        expected.extend_from_slice(&1u64.to_le_bytes());
        expected.extend_from_slice(&68u64.to_le_bytes());
        // CRC-64/NVME of the bytes above, computed in Python.
        expected.extend_from_slice(&0x17b5baa2420c2382_u64.to_le_bytes());
        assert_eq!(bytes, expected);
        assert_eq!(read(&bytes), Ok(at(&entries, &[0, 27, 47])));
    }

    #[test]
    fn the_count_is_of_set_entries() {
        let bytes = write(
            &[set(b"a", 2), fields(Kind::Delete, b"a", 0), set(b"a", 1)],
            22 + 20 + 21,
        );
        let count_at = bytes.len() - 24;
        assert_eq!(bytes[count_at..count_at + 8], 2u64.to_le_bytes());
    }

    #[test]
    fn an_extended_hint_is_the_hint_written_in_one_go() {
        let fs = MemFs::new();
        let path = Path::new("/hint");
        let mut writer = Writer::new(fs.open_or_create(path).unwrap());
        writer.push(set(b"a", 2).0, b"a").unwrap();
        let file = writer.finish(22).unwrap();
        let tail = read_entries(&file, trailer(&file).unwrap(), |_, _, _| {}).unwrap();
        let mut writer = Writer::extend(file, tail).unwrap();
        let (fields, key) = fields(Kind::Delete, b"a", 0);
        writer.push(fields, &key).unwrap();
        writer.push(set(b"bc", 1).0, b"bc").unwrap();
        writer.finish(22 + 20 + 22).unwrap();

        let whole = [
            set(b"a", 2),
            self::fields(Kind::Delete, b"a", 0),
            set(b"bc", 1),
        ];
        assert_eq!(fs.read(path).unwrap(), write(&whole, 64));
    }

    #[test]
    fn a_hint_from_another_version_is_rejected() {
        let mut bytes = write(&[set(b"a", 2)], 22);
        bytes[4] = 2;
        reseal(&mut bytes);
        assert_eq!(read(&bytes), Err(Invalid("not a tally hint file")));
    }

    #[test]
    fn an_entry_that_breaks_a_record_rule_is_rejected() {
        // Zero, unknown and older types, a Delete with a value, a Batch with
        // a key, a batch inside a batch, an empty key, a key over the limit.
        let bad: [&[u8]; 8] = [
            &[0x00, 1, 0, 2, 0, 0, 0, b'a'],
            &[0x39, 1, 0, 2, 0, 0, 0, b'a'],
            &[0x11, 1, 0, 2, 0, 0, 0, b'a'],
            &[0x32, 1, 0, 2, 0, 0, 0, b'a'],
            &[0x33, 1, 0, 8, 0, 0, 0, b'a'],
            &[0x73, 0, 0, 8, 0, 0, 0],
            &[0x31, 0, 0, 2, 0, 0, 0],
            &[0x31, 0x01, 0x04, 2, 0, 0, 0],
        ];
        for entry in bad {
            let mut bytes = b"TLYH\x03".to_vec();
            bytes.extend_from_slice(entry);
            bytes.extend_from_slice(&[0; 24]);
            reseal(&mut bytes);
            assert_eq!(read(&bytes), Err(Invalid("bad entry")), "{entry:x?}");
        }
    }

    fn reseal(bytes: &mut [u8]) {
        let crc_at = bytes.len() - 8;
        let crc = crc_fast::checksum(CrcAlgorithm::Crc64Nvme, &bytes[..crc_at]);
        bytes[crc_at..].copy_from_slice(&crc.to_le_bytes());
    }

    #[test]
    fn an_empty_hint_covers_an_empty_log() {
        assert_eq!(read(&write(&[], 0)), Ok(vec![]));
    }

    #[test]
    fn every_one_byte_change_is_rejected() {
        let bytes = write(&[set(b"a", 2), fields(Kind::Delete, b"bc", 0)], 22 + 21);
        for i in 0..bytes.len() {
            let mut bad = bytes.clone();
            bad[i] ^= 0x01;
            assert!(read(&bad).is_err(), "flipped byte {i} was accepted");
        }
    }

    #[test]
    fn every_truncation_is_rejected() {
        let bytes = write(&[set(b"a", 2), set(b"bc", 1)], 44);
        for len in 0..bytes.len() {
            assert!(read(&bytes[..len]).is_err(), "cut at {len} was accepted");
        }
    }

    #[test]
    fn entries_must_add_up_to_the_log_length() {
        assert_eq!(
            read(&write(&[set(b"a", 2)], 23)),
            Err(Invalid("entries don't match the trailer"))
        );
        assert_eq!(
            read(&write(&[set(b"a", 2)], 21)),
            Err(Invalid("entries run past the log length"))
        );
    }

    #[test]
    fn the_trailer_bounds_the_count_by_the_file_size() {
        let fs = MemFs::new();
        let path = Path::new("/h");
        fs.put(path, &write(&[set(b"a", 2), set(b"bc", 1)], 64));
        let file = fs.open_existing(path).unwrap();
        assert_eq!(trailer(&file).unwrap().most_sets, 2);
    }

    #[test]
    fn a_count_that_disagrees_is_rejected() {
        let mut bytes = write(&[set(b"a", 2)], 22);
        let at = bytes.len() - 24;
        bytes[at] = 2;
        reseal(&mut bytes);
        assert_eq!(
            read(&bytes),
            Err(Invalid("entries don't match the trailer"))
        );
    }
}
