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

use crate::fs::LogFile;
use crate::record::{FIELDS_LEN, Fields, Kind, decode_fields};

pub(crate) const HINT_FILE_NAME: &str = "tally.hint";
pub(crate) const COMPACT_HINT_FILE_NAME: &str = "tally.hint.compact";

const MAGIC: &[u8; 4] = b"TLYH";
const VERSION: u8 = 2;
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
pub(crate) struct Writer<'a, F: LogFile> {
    file: &'a F,
    buffer: Vec<u8>,
    written: u64,
    checksum: Digest,
    count: u64,
}

impl<'a, F: LogFile> Writer<'a, F> {
    /// A hint in the empty `file`.
    pub(crate) fn new(file: &'a F) -> Writer<'a, F> {
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
    pub(crate) fn extend(file: &'a F, tail: Tail) -> io::Result<Writer<'a, F>> {
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

    /// Writes the trailer for the first `log_len` bytes of a segment.
    pub(crate) fn finish(mut self, log_len: u64) -> io::Result<()> {
        self.buffer.extend_from_slice(&self.count.to_le_bytes());
        self.buffer.extend_from_slice(&log_len.to_le_bytes());
        self.checksum.update(&self.buffer);
        let checksum = self.checksum.finalize();
        self.buffer.extend_from_slice(&checksum.to_le_bytes());
        self.file.write_all_at(&self.buffer, self.written)
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

/// Entries whose fields have been checked, in log order.
#[derive(Debug, Default)]
pub(crate) struct Window {
    bytes: Vec<u8>,
    offset: u64,
}

impl Window {
    /// Each entry's fields, key and record offset from the segment's start.
    pub(crate) fn entries(&self) -> impl Iterator<Item = (Fields, &[u8], u64)> {
        let mut at = 0;
        let mut offset = self.offset;
        std::iter::from_fn(move || {
            let bytes = self.bytes.get(at..at + FIELDS_LEN)?;
            let fields = decode_fields(bytes.try_into().unwrap()).expect("read_entries checked it");
            let key_start = at + FIELDS_LEN;
            let key = &self.bytes[key_start..key_start + usize::from(fields.key_len)];
            let entry = (fields, key, offset);
            at = key_start + key.len();
            offset += fields.record_len();
            Some(entry)
        })
    }

    fn clear(&mut self) {
        self.bytes.clear();
    }
}

/// A read buffer of `most` bytes, or of `len` if that is less: a 1 MiB
/// buffer is zeroed before its first read.
pub(crate) fn buffer_for(len: u64, most: usize) -> usize {
    usize::try_from(len).map_or(most, |len| len.min(most))
}

/// Checks each entry's fields and passes them to `window` in windows of
/// about `window_bytes`, then checks the checksum and that the entries
/// account for exactly `trailer`. On `Err`, whatever `window` saw must be
/// thrown away.
pub(crate) fn read_entries(
    file: &impl LogFile,
    trailer: Trailer,
    window_bytes: usize,
    mut window: impl FnMut(&Window),
) -> Result<Tail, Invalid> {
    let size = file.size()?;
    let entries_end = size - TRAILER_LEN;
    // Checksummed under the buffer, so the checksum takes a buffer at a
    // time: fed an entry at a time, it took most of the time to read one.
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
    let mut current = Window::default();
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
        if current.bytes.is_empty() {
            current.offset = offset;
        }
        current.bytes.extend_from_slice(&header);
        current.bytes.extend_from_slice(&key);
        if current.bytes.len() >= window_bytes {
            window(&current);
            current.clear();
        }
        offset += fields.record_len();
        if fields.kind == Kind::Set {
            count += 1;
        }
    }
    if !current.bytes.is_empty() {
        window(&current);
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
    use crate::record::Format;
    use std::path::Path;

    fn set(key: &[u8], value_len: u32) -> (Fields, Vec<u8>) {
        fields(Kind::Set, key, value_len)
    }

    fn fields(kind: Kind, key: &[u8], value_len: u32) -> (Fields, Vec<u8>) {
        let fields = Fields {
            kind,
            format: Format::Crc64,
            key_len: key.len() as u16,
            value_len,
        };
        (fields, key.to_vec())
    }

    fn write(entries: &[(Fields, Vec<u8>)], log_len: u64) -> Vec<u8> {
        let fs = MemFs::new();
        let path = Path::new("/hint");
        let file = fs.open_or_create(path).unwrap();
        let mut writer = Writer::new(&file);
        for (fields, key) in entries {
            writer.push(*fields, key).unwrap();
        }
        writer.finish(log_len).unwrap();
        fs.read(path).unwrap()
    }

    type Entries = Vec<(Fields, Vec<u8>, u64)>;

    fn windows(bytes: &[u8], window_bytes: usize) -> Result<Vec<Entries>, Invalid> {
        let fs = MemFs::new();
        fs.put(Path::new("/hint"), bytes);
        let file = fs.open_existing(Path::new("/hint")).unwrap();
        let trailer = trailer(&file)?;
        let mut seen = Vec::new();
        read_entries(&file, trailer, window_bytes, |window| {
            seen.push(
                window
                    .entries()
                    .map(|(fields, key, offset)| (fields, key.to_vec(), offset))
                    .collect(),
            )
        })?;
        Ok(seen)
    }

    fn read(bytes: &[u8]) -> Result<Entries, Invalid> {
        Ok(windows(bytes, 1 << 20)?.into_iter().flatten().collect())
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
        let mut expected = b"TLYH\x02".to_vec();
        expected.extend_from_slice(&[0x11, 1, 0, 2, 0, 0, 0, b'a']);
        expected.extend_from_slice(&[0x11, 2, 0, 1, 0, 0, 0, b'b', b'c']);
        expected.extend_from_slice(&2u64.to_le_bytes());
        expected.extend_from_slice(&44u64.to_le_bytes());
        // CRC-64/NVME of the bytes above, computed in Python.
        expected.extend_from_slice(&0x211b_e5c0_f2cf_45ab_u64.to_le_bytes());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn entries_come_back_with_their_offsets() {
        let entries = [set(b"a", 2), set(b"bc", 1)];
        assert_eq!(read(&write(&entries, 44)), Ok(at(&entries, &[0, 22])));
    }

    #[test]
    fn the_bytes_of_a_delete_and_a_batch() {
        let entries = [
            fields(Kind::Batch, b"", 8),
            fields(Kind::Delete, b"a", 0),
            set(b"b", 1),
        ];
        let bytes = write(&entries, 27 + 20 + 21);
        let mut expected = b"TLYH\x02".to_vec();
        expected.extend_from_slice(&[0x13, 0, 0, 8, 0, 0, 0]);
        expected.extend_from_slice(&[0x12, 1, 0, 0, 0, 0, 0, b'a']);
        expected.extend_from_slice(&[0x11, 1, 0, 1, 0, 0, 0, b'b']);
        expected.extend_from_slice(&1u64.to_le_bytes());
        expected.extend_from_slice(&68u64.to_le_bytes());
        // CRC-64/NVME of the bytes above, computed in Python.
        expected.extend_from_slice(&0x6160_f248_4865_8a2d_u64.to_le_bytes());
        assert_eq!(bytes, expected);
        assert_eq!(read(&bytes), Ok(at(&entries, &[0, 27, 47])));
    }

    #[test]
    fn records_from_before_64_bit_checksums_have_15_byte_headers() {
        let old = |kind, key: &[u8], value_len| {
            let (mut fields, key) = fields(kind, key, value_len);
            fields.format = Format::Crc32c;
            (fields, key)
        };
        let entries = [
            old(Kind::Set, b"a", 2),
            old(Kind::Delete, b"a", 0),
            set(b"b", 1),
        ];
        let bytes = write(&entries, 18 + 16 + 21);
        assert_eq!(bytes[5], 0x01);
        assert_eq!(bytes[13], 0x02);
        assert_eq!(read(&bytes), Ok(at(&entries, &[0, 18, 34])));
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
        let file = fs.open_or_create(path).unwrap();
        let mut writer = Writer::new(&file);
        writer.push(set(b"a", 2).0, b"a").unwrap();
        writer.finish(22).unwrap();
        let tail = read_entries(&file, trailer(&file).unwrap(), 1 << 20, |_| {}).unwrap();
        let mut writer = Writer::extend(&file, tail).unwrap();
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
    fn a_hint_from_before_64_bit_checksums_is_rejected() {
        let mut bytes = b"TLYH\x01".to_vec();
        bytes.extend_from_slice(&[1, 0, 2, 0, 0, 0, b'a']);
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&18u64.to_le_bytes());
        bytes.extend_from_slice(&crc32c::crc32c(&bytes).to_le_bytes());
        assert_eq!(read(&bytes), Err(Invalid("not a tally hint file")));
    }

    #[test]
    fn an_entry_that_breaks_a_record_rule_is_rejected() {
        // Zero and unknown types, a Set without a value, a Delete with one,
        // a Batch with a key, an empty key, a key over the limit.
        let bad: [&[u8]; 7] = [
            &[0x00, 1, 0, 2, 0, 0, 0, b'a'],
            &[0x19, 1, 0, 2, 0, 0, 0, b'a'],
            &[0x11, 1, 0, 0, 0, 0, 0, b'a'],
            &[0x12, 1, 0, 2, 0, 0, 0, b'a'],
            &[0x13, 1, 0, 8, 0, 0, 0, b'a'],
            &[0x11, 0, 0, 2, 0, 0, 0],
            &[0x11, 0x01, 0x04, 2, 0, 0, 0],
        ];
        for entry in bad {
            let mut bytes = b"TLYH\x02".to_vec();
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
    fn entries_are_cut_into_windows_in_log_order() {
        // Entries of 8, 9, 8, 9 and 8 bytes; windows close at 15 bytes.
        let entries = [
            set(b"a", 2),
            set(b"bc", 1),
            set(b"d", 1),
            set(b"ef", 3),
            set(b"g", 1),
        ];
        let bytes = write(&entries, 22 + 22 + 21 + 24 + 21);
        let offsets = [0, 22, 44, 65, 89];
        assert_eq!(
            windows(&bytes, 15),
            Ok(vec![
                at(&entries[..2], &offsets[..2]),
                at(&entries[2..4], &offsets[2..4]),
                at(&entries[4..], &offsets[4..]),
            ])
        );
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

    #[test]
    fn a_read_buffer_is_no_larger_than_what_it_reads() {
        assert_eq!(buffer_for(53, 1 << 20), 53);
        assert_eq!(buffer_for(5 << 20, 1 << 20), 1 << 20);
    }
}
