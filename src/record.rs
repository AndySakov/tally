use crc_fast::CrcAlgorithm;
use crc32c::crc32c;

use crate::{
    CorruptionReason, DecodeError, Field, Key, MAX_KEY_LEN, MAX_VALUE_LEN, UnsupportedReason, Value,
};

/// The header of every record written now.
pub const HEADER_LEN: usize = Format::Crc64.header_len();

/// Enough bytes to tell a record's header length: the shortest header.
pub const SHORTEST_HEADER_LEN: usize = Format::Crc32c.header_len();

const _: () = assert!(
    SHORTEST_HEADER_LEN == 4 + 1 + 2 + 4 + 4 && HEADER_LEN == 4 + 1 + 2 + 4 + 8,
    "header lengths must match the field layout"
);

/// How a record's payload is checksummed, which sets its header's length.
/// Records written before 64-bit checksums are `Crc32c`; every record
/// written now is `Crc64`.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// CRC32C in a 15-byte header. Type bytes 1 to 3.
    Crc32c = 0,
    /// CRC-64/NVME in a 19-byte header. Type bytes 0x11 to 0x13.
    Crc64 = 1,
}

impl Format {
    pub const fn header_len(self) -> usize {
        match self {
            Format::Crc32c => 15,
            Format::Crc64 => 19,
        }
    }

    /// The format a type byte names. Read before the header checksum is
    /// checked: a damaged type byte gives the wrong length, and then the
    /// header checksum fails.
    pub fn of(type_byte: u8) -> Format {
        if type_byte >> 4 == 1 {
            Format::Crc64
        } else {
            Format::Crc32c
        }
    }

    /// One-shot, not streamed: a streaming CRC-64 costs twice as much on a
    /// 100-byte payload.
    fn checksum(self, payload: &[u8]) -> u64 {
        match self {
            Format::Crc32c => u64::from(crc32c(payload)),
            Format::Crc64 => crc_fast::checksum(CrcAlgorithm::Crc64Nvme, payload),
        }
    }
}

/// The payload of a Batch record: the byte length of the records after it
/// that belong to the batch.
pub const BATCH_PAYLOAD_LEN: u32 = 8;

#[derive(Debug, PartialEq, Eq)]
pub enum Record {
    Set {
        key: Key,
        value: Value,
    },
    Delete {
        key: Key,
    },
    /// Starts a batch: the next `body_len` bytes are its Set and Delete
    /// records, applied all together or not at all.
    Batch {
        body_len: u64,
    },
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Set = 1,
    Delete = 2,
    Batch = 3,
}

impl Kind {
    /// The kind a type byte names, in any format.
    fn of(type_byte: u8) -> Result<Kind, DecodeError> {
        match type_byte {
            0 => Err(DecodeError::Corrupt(CorruptionReason::ZeroType)),
            0x01 | 0x11 => Ok(Kind::Set),
            0x02 | 0x12 => Ok(Kind::Delete),
            0x03 | 0x13 => Ok(Kind::Batch),
            byte => Err(DecodeError::Unsupported(UnsupportedReason::UnknownType(
                byte,
            ))),
        }
    }

    /// The type byte of this kind of record in `format`.
    pub fn type_byte(self, format: Format) -> u8 {
        (format as u8) << 4 | self as u8
    }
}

/// A decoded header. If one exists, its header CRC was valid and its
/// fields satisfy every record rule, so only the payload CRC is left
/// to check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    kind: Kind,
    format: Format,
    key_len: u16,
    value_len: u32,
    payload_checksum: u64,
}

impl Header {
    pub fn format(self) -> Format {
        self.format
    }

    pub fn payload_len(self) -> usize {
        self.key_len as usize + self.value_len as usize
    }

    pub fn record_len(self) -> u64 {
        (self.format.header_len() + self.payload_len()) as u64
    }
}

/// Appends the encoded record to `buf`. Does not clear `buf` first.
pub fn encode(record: &Record, buf: &mut Vec<u8>) {
    encode_as(Format::Crc64, record, buf);
}

/// `encode` in a given format. Only `Crc64` is written; `Crc32c` is for
/// tests of logs written before it.
pub fn encode_as(format: Format, record: &Record, buf: &mut Vec<u8>) {
    let body_len;
    match record {
        Record::Set { key, value } => {
            encode_parts(format, Kind::Set, key.as_bytes(), value.as_bytes(), buf)
        }
        Record::Delete { key } => encode_parts(format, Kind::Delete, key.as_bytes(), &[], buf),
        Record::Batch { body_len: len } => {
            body_len = len.to_le_bytes();
            encode_parts(format, Kind::Batch, &[], &body_len, buf)
        }
    }
}

/// Appends a Set record of `key` and `value`, which the caller has checked
/// are within the limits, as when copying a record already read.
pub fn encode_set(key: &[u8], value: &[u8], buf: &mut Vec<u8>) {
    encode_parts(Format::Crc64, Kind::Set, key, value, buf);
}

fn encode_parts(format: Format, kind: Kind, key: &[u8], value: &[u8], buf: &mut Vec<u8>) {
    let header_len = format.header_len();
    let mut header = [0u8; HEADER_LEN];
    header[4] = kind.type_byte(format);
    header[5..7].copy_from_slice(
        &u16::try_from(key.len())
            .expect("Key guarantees len fits u16")
            .to_le_bytes(),
    );
    header[7..11].copy_from_slice(
        &u32::try_from(value.len())
            .expect("Value guarantees len fits u32")
            .to_le_bytes(),
    );
    let start = buf.len();
    buf.reserve(header_len + key.len() + value.len());
    buf.extend_from_slice(&header[..header_len]);
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
    let payload_checksum = format.checksum(&buf[start + header_len..]).to_le_bytes();
    let header = &mut buf[start..start + header_len];
    header[11..].copy_from_slice(&payload_checksum[..header_len - 11]);
    let header_checksum = crc32c(&header[4..]);
    header[..4].copy_from_slice(&header_checksum.to_le_bytes());
}

/// Checks the header CRC, then the kind, the limits and the record rules.
///
/// Panics unless `bytes` is as long as the header `Format::of(bytes[4])`
/// names: reading another length is the caller's bug, not bad data.
pub fn decode_header(bytes: &[u8]) -> Result<Header, DecodeError> {
    let format = Format::of(bytes[4]);
    assert_eq!(
        bytes.len(),
        format.header_len(),
        "caller passed a header of the wrong length"
    );
    let stored_checksum = &bytes[..4];
    let computed_checksum = crc32c(&bytes[4..]).to_le_bytes();
    if stored_checksum != computed_checksum {
        return Err(DecodeError::Corrupt(CorruptionReason::HeaderChecksum));
    }
    let kind = Kind::of(bytes[4])?;
    let key_len = u16::from_le_bytes(bytes[5..7].try_into().unwrap());
    let mut payload_checksum = [0u8; 8];
    payload_checksum[..bytes.len() - 11].copy_from_slice(&bytes[11..]);
    let payload_checksum = u64::from_le_bytes(payload_checksum);
    if kind == Kind::Batch {
        let value_len = u32::from_le_bytes(bytes[7..11].try_into().unwrap());
        if key_len != 0 || value_len != BATCH_PAYLOAD_LEN {
            return Err(DecodeError::Corrupt(CorruptionReason::BadBatch));
        }
        return Ok(Header {
            kind,
            format,
            key_len,
            value_len,
            payload_checksum,
        });
    }
    if key_len == 0 {
        return Err(DecodeError::Corrupt(CorruptionReason::EmptyKey));
    }
    if key_len as usize > MAX_KEY_LEN {
        return Err(DecodeError::Unsupported(UnsupportedReason::OverLimit {
            field: Field::Key,
            len: key_len as usize,
            max: MAX_KEY_LEN,
        }));
    }
    let value_len = u32::from_le_bytes(bytes[7..11].try_into().unwrap());
    if value_len as usize > MAX_VALUE_LEN {
        return Err(DecodeError::Unsupported(UnsupportedReason::OverLimit {
            field: Field::Value,
            len: value_len as usize,
            max: MAX_VALUE_LEN,
        }));
    }
    match kind {
        Kind::Set if value_len == 0 => {
            return Err(DecodeError::Corrupt(CorruptionReason::SetWithoutValue));
        }
        Kind::Delete if value_len != 0 => {
            return Err(DecodeError::Corrupt(CorruptionReason::DeleteWithValue));
        }
        _ => {}
    }
    Ok(Header {
        kind,
        format,
        key_len,
        value_len,
        payload_checksum,
    })
}

/// Checks the payload CRC and builds the record.
///
/// Panics if `payload.len() != header.payload_len()`: that means the
/// caller read the wrong number of bytes, which is a bug, not bad data.
pub fn decode_payload(header: Header, payload: &[u8]) -> Result<Record, DecodeError> {
    match decode_index_op(header, payload)? {
        IndexOp::Set { key, .. } => Ok(Record::Set {
            key,
            value: Value::try_from(&payload[header.key_len as usize..])
                .expect("decode_header validated value_len"),
        }),
        IndexOp::Delete { key } => Ok(Record::Delete { key }),
        IndexOp::Batch { body_len } => Ok(Record::Batch { body_len }),
    }
}

/// What a record does to the index, without the value bytes.
#[derive(Debug, PartialEq, Eq)]
pub enum IndexOp {
    Set {
        key: Key,
        value_len: u32,
        format: Format,
    },
    Delete {
        key: Key,
    },
    Batch {
        body_len: u64,
    },
}

/// Checks the payload CRC and returns what the record does to the index,
/// without copying the value.
///
/// Panics if `payload.len() != header.payload_len()`, as `decode_payload`.
pub fn decode_index_op(header: Header, payload: &[u8]) -> Result<IndexOp, DecodeError> {
    assert_eq!(
        payload.len(),
        header.payload_len(),
        "caller passed a payload of the wrong length"
    );
    if header.payload_checksum != header.format.checksum(payload) {
        return Err(DecodeError::Corrupt(CorruptionReason::PayloadChecksum));
    }
    if header.kind == Kind::Batch {
        let body_len = u64::from_le_bytes(payload.try_into().expect("decode_header checked it"));
        return Ok(IndexOp::Batch { body_len });
    }
    let key_bytes = &payload[..header.key_len as usize];
    let key = Key::try_from(key_bytes).expect("decode_header validated key_len");
    Ok(match header.kind {
        Kind::Set => IndexOp::Set {
            key,
            value_len: header.value_len,
            format: header.format,
        },
        Kind::Delete => IndexOp::Delete { key },
        Kind::Batch => unreachable!("returned above"),
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::Error;

    // Expected bytes computed independently of this crate (Python CRC32C and
    // CRC-64/NVME, each checked against its standard test vector).
    pub(crate) const SET_A_HI: [u8; 22] = [
        0xcf, 0xe1, 0xe6, 0x64, // header_checksum
        0x11, // type: Set, 64-bit
        0x01, 0x00, // key_len: 1
        0x02, 0x00, 0x00, 0x00, // value_len: 2
        0x31, 0x8d, 0xe2, 0x77, 0x90, 0x61, 0xc1, 0x8f, // payload_checksum
        0x61, // key: "a"
        0x68, 0x69, // value: "hi"
    ];

    pub(crate) const DELETE_A: [u8; 20] = [
        0xdd, 0xb5, 0x72, 0x90, // header_checksum
        0x12, // type: Delete, 64-bit
        0x01, 0x00, // key_len: 1
        0x00, 0x00, 0x00, 0x00, // value_len: 0
        0x3c, 0xfc, 0xcb, 0xb4, 0x45, 0x84, 0x2f, 0x8c, // payload_checksum
        0x61, // key: "a"
    ];

    /// A Batch record whose body is 42 bytes: SET_A_HI then DELETE_A.
    pub(crate) const BATCH_42: [u8; 27] = [
        0xd9, 0xd8, 0x1c, 0x94, // header_checksum
        0x13, // type: Batch, 64-bit
        0x00, 0x00, // key_len: 0
        0x08, 0x00, 0x00, 0x00, // value_len: 8
        0xc2, 0x34, 0xe0, 0x29, 0xd3, 0xef, 0xe5, 0x9a, // payload_checksum
        0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // body_len: 42
    ];

    /// The same records as written before 64-bit checksums.
    pub(crate) const OLD_SET_A_HI: [u8; 18] = [
        0xca, 0xa7, 0x41, 0x82, // header_checksum
        0x01, // type: Set
        0x01, 0x00, // key_len: 1
        0x02, 0x00, 0x00, 0x00, // value_len: 2
        0xd9, 0xe4, 0xf8, 0xe7, // payload_checksum
        0x61, // key: "a"
        0x68, 0x69, // value: "hi"
    ];

    pub(crate) const OLD_DELETE_A: [u8; 16] = [
        0x89, 0xd9, 0x75, 0x53, // header_checksum
        0x02, // type: Delete
        0x01, 0x00, // key_len: 1
        0x00, 0x00, 0x00, 0x00, // value_len: 0
        0x30, 0x43, 0xd0, 0xc1, // payload_checksum
        0x61, // key: "a"
    ];

    /// A Batch record whose body is 34 bytes: OLD_SET_A_HI then OLD_DELETE_A.
    pub(crate) const OLD_BATCH_34: [u8; 23] = [
        0x4c, 0x3a, 0x23, 0x67, // header_checksum
        0x03, // type: Batch
        0x00, 0x00, // key_len: 0
        0x08, 0x00, 0x00, 0x00, // value_len: 8
        0x5d, 0x2d, 0x50, 0x13, // payload_checksum
        0x22, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // body_len: 34
    ];

    fn key(bytes: &[u8]) -> Key {
        Key::try_from(bytes).unwrap()
    }

    #[test]
    fn encode_a_batch_record() {
        let mut buf = Vec::new();
        encode(&Record::Batch { body_len: 42 }, &mut buf);
        assert_eq!(buf, BATCH_42);
    }

    #[test]
    fn decode_a_batch_record() {
        let header = decode_header(header_of(&BATCH_42)).unwrap();
        assert_eq!(header.record_len(), 27);
        assert_eq!(
            decode_payload(header, &BATCH_42[19..]),
            Ok(Record::Batch { body_len: 42 })
        );
        let header = decode_header(header_of(&OLD_BATCH_34)).unwrap();
        assert_eq!(header.record_len(), 23);
        assert_eq!(
            decode_payload(header, &OLD_BATCH_34[15..]),
            Ok(Record::Batch { body_len: 34 })
        );
    }

    #[test]
    fn a_batch_header_needs_no_key_and_an_8_byte_payload() {
        for type_byte in [0x03, 0x13] {
            for (key_len, value_len) in [(1, 8), (0, 7), (0, 9), (0, 0)] {
                assert_eq!(
                    decode_header(&header_bytes(type_byte, key_len, value_len, 0)),
                    Err(DecodeError::Corrupt(CorruptionReason::BadBatch)),
                    "type {type_byte:#x}, key_len {key_len}, value_len {value_len}"
                );
            }
            assert!(decode_header(&header_bytes(type_byte, 0, 8, 0)).is_ok());
        }
    }

    fn value(bytes: &[u8]) -> Value {
        Value::try_from(bytes).unwrap()
    }

    fn set(k: &[u8], v: &[u8]) -> Record {
        Record::Set {
            key: key(k),
            value: value(v),
        }
    }

    /// The header at the start of `bytes`, as long as its type byte says.
    fn header_of(bytes: &[u8]) -> &[u8] {
        &bytes[..Format::of(bytes[4]).header_len()]
    }

    /// Builds a header, as long as `type_byte` says, with a valid header
    /// CRC, for testing the checks that run after the CRC. An old-format
    /// header keeps the low 32 bits of `payload_checksum`.
    fn header_bytes(type_byte: u8, key_len: u16, value_len: u32, payload_checksum: u64) -> Vec<u8> {
        let len = Format::of(type_byte).header_len();
        let mut out = vec![0u8; len];
        out[4] = type_byte;
        out[5..7].copy_from_slice(&key_len.to_le_bytes());
        out[7..11].copy_from_slice(&value_len.to_le_bytes());
        out[11..].copy_from_slice(&payload_checksum.to_le_bytes()[..len - 11]);
        let crc = crc32c::crc32c(&out[4..]);
        out[..4].copy_from_slice(&crc.to_le_bytes());
        out
    }

    #[test]
    fn crc32c_dependency_is_castagnoli() {
        assert_eq!(crc32c::crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn crc64_dependency_is_nvme() {
        assert_eq!(Format::Crc64.checksum(b"123456789"), 0xAE8B_1486_0A79_9888);
        assert_eq!(Format::Crc32c.checksum(b"123456789"), 0xE306_9283);
    }

    // encode

    #[test]
    fn encode_set_matches_the_format() {
        let mut buf = Vec::new();
        encode(&set(b"a", b"hi"), &mut buf);
        assert_eq!(buf, SET_A_HI);
    }

    #[test]
    fn encode_delete_matches_the_format() {
        let mut buf = Vec::new();
        encode(&Record::Delete { key: key(b"a") }, &mut buf);
        assert_eq!(buf, DELETE_A);
    }

    #[test]
    fn encode_as_crc32c_writes_the_old_format() {
        let mut buf = Vec::new();
        encode_as(Format::Crc32c, &set(b"a", b"hi"), &mut buf);
        encode_as(Format::Crc32c, &Record::Delete { key: key(b"a") }, &mut buf);
        encode_as(Format::Crc32c, &Record::Batch { body_len: 34 }, &mut buf);
        assert_eq!(
            buf,
            [&OLD_SET_A_HI[..], &OLD_DELETE_A, &OLD_BATCH_34].concat()
        );
    }

    #[test]
    fn encode_set_from_bytes_matches_encode() {
        let mut buf = Vec::new();
        encode_set(b"a", b"hi", &mut buf);
        assert_eq!(buf, SET_A_HI);
    }

    #[test]
    fn encode_appends_without_clearing() {
        let mut buf = vec![0xAA];
        encode(&set(b"a", b"hi"), &mut buf);
        assert_eq!(buf[0], 0xAA);
        assert_eq!(&buf[1..], SET_A_HI);
    }

    #[test]
    fn encode_two_records_back_to_back() {
        let mut buf = Vec::new();
        encode(&set(b"a", b"hi"), &mut buf);
        encode(&Record::Delete { key: key(b"a") }, &mut buf);
        assert_eq!(&buf[..22], SET_A_HI);
        assert_eq!(&buf[22..], DELETE_A);
    }

    // decode_header: valid input

    #[test]
    fn decode_header_set() {
        let header = decode_header(header_of(&SET_A_HI)).unwrap();
        assert_eq!(
            header,
            Header {
                kind: Kind::Set,
                format: Format::Crc64,
                key_len: 1,
                value_len: 2,
                payload_checksum: 0x8fc1_6190_77e2_8d31
            }
        );
        assert_eq!(header.payload_len(), 3);
        assert_eq!(header.record_len(), 22);
    }

    #[test]
    fn decode_header_delete() {
        let header = decode_header(header_of(&DELETE_A)).unwrap();
        assert_eq!(
            header,
            Header {
                kind: Kind::Delete,
                format: Format::Crc64,
                key_len: 1,
                value_len: 0,
                payload_checksum: 0x8c2f_8445_b4cb_fc3c
            }
        );
        assert_eq!(header.payload_len(), 1);
        assert_eq!(header.record_len(), 20);
    }

    #[test]
    fn decode_header_reads_the_old_format() {
        let header = decode_header(header_of(&OLD_SET_A_HI)).unwrap();
        assert_eq!(
            header,
            Header {
                kind: Kind::Set,
                format: Format::Crc32c,
                key_len: 1,
                value_len: 2,
                payload_checksum: 0xe7f8_e4d9
            }
        );
        assert_eq!(header.record_len(), 18);
        let header = decode_header(header_of(&OLD_DELETE_A)).unwrap();
        assert_eq!(header.format(), Format::Crc32c);
        assert_eq!(header.record_len(), 16);
    }

    #[test]
    fn decode_header_accepts_lengths_exactly_at_the_limits() {
        for type_byte in [0x01, 0x11] {
            let bytes = header_bytes(type_byte, MAX_KEY_LEN as u16, MAX_VALUE_LEN as u32, 0);
            let header = decode_header(&bytes).unwrap();
            assert_eq!(header.payload_len(), 1024 + 16_777_216);
        }
    }

    // decode_header: corruption and unsupported records

    #[test]
    fn decode_header_rejects_a_flipped_bit_in_a_length() {
        let mut bytes = header_of(&SET_A_HI).to_vec();
        bytes[7] ^= 0x01; // value_len 2 -> 3
        assert_eq!(
            decode_header(&bytes),
            Err(DecodeError::Corrupt(CorruptionReason::HeaderChecksum))
        );
    }

    #[test]
    fn decode_header_rejects_a_flipped_bit_in_the_payload_checksum() {
        for at in [11, 18] {
            let mut bytes = header_of(&SET_A_HI).to_vec();
            bytes[at] ^= 0x80;
            assert_eq!(
                decode_header(&bytes),
                Err(DecodeError::Corrupt(CorruptionReason::HeaderChecksum)),
                "byte {at}"
            );
        }
    }

    #[test]
    fn a_type_byte_damaged_into_the_other_format_fails_the_header_checksum() {
        let mut old = [&OLD_SET_A_HI[..], &OLD_DELETE_A].concat();
        old[4] = 0x11;
        assert_eq!(
            decode_header(header_of(&old)),
            Err(DecodeError::Corrupt(CorruptionReason::HeaderChecksum))
        );
        let mut new = SET_A_HI.to_vec();
        new[4] = 0x01;
        assert_eq!(
            decode_header(header_of(&new)),
            Err(DecodeError::Corrupt(CorruptionReason::HeaderChecksum))
        );
    }

    #[test]
    #[should_panic(expected = "caller passed a header of the wrong length")]
    fn decode_header_panics_when_the_caller_passes_the_wrong_length() {
        let _ = decode_header(&SET_A_HI[..15]);
    }

    #[test]
    fn decode_header_rejects_all_zeros() {
        assert_eq!(
            decode_header(&[0u8; SHORTEST_HEADER_LEN]),
            Err(DecodeError::Corrupt(CorruptionReason::HeaderChecksum))
        );
    }

    #[test]
    fn decode_header_checks_the_crc_before_trusting_any_field() {
        let mut bytes = header_bytes(7, 1, 2, 0);
        bytes[0] ^= 0x01;
        assert_eq!(
            decode_header(&bytes),
            Err(DecodeError::Corrupt(CorruptionReason::HeaderChecksum))
        );
    }

    #[test]
    fn decode_header_rejects_an_unknown_kind() {
        for type_byte in [7, 0x10, 0x14, 0x21] {
            assert_eq!(
                decode_header(&header_bytes(type_byte, 1, 2, 0)),
                Err(DecodeError::Unsupported(UnsupportedReason::UnknownType(
                    type_byte
                )))
            );
        }
    }

    #[test]
    fn decode_header_rejects_kind_zero_as_corruption() {
        assert_eq!(
            decode_header(&header_bytes(0, 1, 2, 0)),
            Err(DecodeError::Corrupt(CorruptionReason::ZeroType))
        );
    }

    #[test]
    fn decode_header_rejects_a_key_over_the_limit() {
        assert_eq!(
            decode_header(&header_bytes(1, 1025, 2, 0)),
            Err(DecodeError::Unsupported(UnsupportedReason::OverLimit {
                field: Field::Key,
                len: 1025,
                max: 1024,
            }))
        );
    }

    #[test]
    fn decode_header_rejects_a_value_over_the_limit() {
        assert_eq!(
            decode_header(&header_bytes(1, 1, 16_777_217, 0)),
            Err(DecodeError::Unsupported(UnsupportedReason::OverLimit {
                field: Field::Value,
                len: 16_777_217,
                max: 16_777_216,
            }))
        );
    }

    #[test]
    fn decode_header_rejects_an_empty_key() {
        assert_eq!(
            decode_header(&header_bytes(1, 0, 2, 0)),
            Err(DecodeError::Corrupt(CorruptionReason::EmptyKey))
        );
    }

    #[test]
    fn decode_header_rejects_a_set_without_a_value() {
        assert_eq!(
            decode_header(&header_bytes(1, 1, 0, 0)),
            Err(DecodeError::Corrupt(CorruptionReason::SetWithoutValue))
        );
    }

    #[test]
    fn decode_header_rejects_a_delete_with_a_value() {
        assert_eq!(
            decode_header(&header_bytes(2, 1, 3, 0)),
            Err(DecodeError::Corrupt(CorruptionReason::DeleteWithValue))
        );
    }

    // decode_payload

    #[test]
    fn decode_payload_set() {
        let header = decode_header(header_of(&SET_A_HI)).unwrap();
        assert_eq!(
            decode_payload(header, &SET_A_HI[HEADER_LEN..]),
            Ok(set(b"a", b"hi"))
        );
    }

    #[test]
    fn decode_payload_of_the_old_format() {
        let header = decode_header(header_of(&OLD_SET_A_HI)).unwrap();
        assert_eq!(
            decode_payload(header, &OLD_SET_A_HI[15..]),
            Ok(set(b"a", b"hi"))
        );
        let mut payload = OLD_SET_A_HI[15..].to_vec();
        payload[2] ^= 0x01;
        assert_eq!(
            decode_payload(header, &payload),
            Err(DecodeError::Corrupt(CorruptionReason::PayloadChecksum))
        );
    }

    #[test]
    fn decode_payload_delete() {
        let header = decode_header(header_of(&DELETE_A)).unwrap();
        assert_eq!(
            decode_payload(header, &DELETE_A[HEADER_LEN..]),
            Ok(Record::Delete { key: key(b"a") })
        );
    }

    #[test]
    fn decode_payload_rejects_a_flipped_bit_in_the_value() {
        let header = decode_header(header_of(&SET_A_HI)).unwrap();
        let mut payload = SET_A_HI[HEADER_LEN..].to_vec();
        payload[2] ^= 0x01; // "hi" -> "hh"
        assert_eq!(
            decode_payload(header, &payload),
            Err(DecodeError::Corrupt(CorruptionReason::PayloadChecksum))
        );
    }

    #[test]
    fn decode_payload_rejects_a_zero_filled_payload() {
        let header = decode_header(header_of(&SET_A_HI)).unwrap();
        assert_eq!(
            decode_payload(header, &[0, 0, 0]),
            Err(DecodeError::Corrupt(CorruptionReason::PayloadChecksum))
        );
    }

    #[test]
    #[should_panic]
    fn decode_payload_panics_when_the_caller_passes_the_wrong_length() {
        let header = decode_header(header_of(&SET_A_HI)).unwrap();
        let _ = decode_payload(header, &SET_A_HI[HEADER_LEN..HEADER_LEN + 2]);
    }

    // decode_index_op

    #[test]
    fn decode_index_op_of_a_set() {
        let header = decode_header(header_of(&SET_A_HI)).unwrap();
        assert_eq!(
            decode_index_op(header, &SET_A_HI[HEADER_LEN..]),
            Ok(IndexOp::Set {
                key: key(b"a"),
                value_len: 2,
                format: Format::Crc64,
            })
        );
    }

    #[test]
    fn decode_index_op_of_a_delete() {
        let header = decode_header(header_of(&DELETE_A)).unwrap();
        assert_eq!(
            decode_index_op(header, &DELETE_A[HEADER_LEN..]),
            Ok(IndexOp::Delete { key: key(b"a") })
        );
    }

    #[test]
    fn decode_index_op_still_checks_the_value_bytes() {
        let header = decode_header(header_of(&SET_A_HI)).unwrap();
        let mut payload = SET_A_HI[HEADER_LEN..].to_vec();
        payload[2] ^= 0x01;
        assert_eq!(
            decode_index_op(header, &payload),
            Err(DecodeError::Corrupt(CorruptionReason::PayloadChecksum))
        );
    }

    #[test]
    #[should_panic]
    fn decode_index_op_panics_when_the_caller_passes_the_wrong_length() {
        let header = decode_header(header_of(&SET_A_HI)).unwrap();
        let _ = decode_index_op(header, &SET_A_HI[HEADER_LEN..HEADER_LEN + 2]);
    }

    // round trips

    #[test]
    fn round_trip() {
        let records = [
            set(b"a", b"hi"),
            Record::Delete { key: key(b"a") },
            set(&[0xff; 1024], &[0x00, 0x01, 0xfe, 0xff]),
            set(b"big", &vec![0x5a; MAX_VALUE_LEN]),
        ];
        for record in records {
            let mut buf = Vec::new();
            encode(&record, &mut buf);
            let header = decode_header(header_of(&buf)).unwrap();
            assert_eq!(header.record_len(), buf.len() as u64);
            assert_eq!(decode_payload(header, &buf[HEADER_LEN..]), Ok(record));
        }
    }

    // DecodeError::at

    #[test]
    fn at_attaches_the_file_offset() {
        assert!(matches!(
            DecodeError::Corrupt(CorruptionReason::HeaderChecksum).at(4096),
            Error::Corruption {
                offset: 4096,
                reason: CorruptionReason::HeaderChecksum
            }
        ));
        assert!(matches!(
            DecodeError::Unsupported(UnsupportedReason::UnknownType(7)).at(18),
            Error::Unsupported {
                offset: 18,
                reason: UnsupportedReason::UnknownType(7)
            }
        ));
    }

    mod props {
        use proptest::prelude::*;
        use proptest::sample::Index;

        use super::*;

        fn any_record() -> impl Strategy<Value = Record> {
            let key_bytes = prop::collection::vec(any::<u8>(), 1..=MAX_KEY_LEN);
            let value_bytes = prop::collection::vec(any::<u8>(), 1..512);
            prop_oneof![
                (key_bytes.clone(), value_bytes).prop_map(|(k, v)| set(&k, &v)),
                key_bytes.prop_map(|k| Record::Delete { key: key(&k) }),
            ]
        }

        fn encoded(record: &Record) -> Vec<u8> {
            let mut buf = Vec::new();
            encode(record, &mut buf);
            buf
        }

        fn any_format() -> impl Strategy<Value = Format> {
            prop_oneof![Just(Format::Crc32c), Just(Format::Crc64)]
        }

        proptest! {
            #[test]
            fn round_trip(record in any_record()) {
                let buf = encoded(&record);
                let header = decode_header(header_of(&buf)).unwrap();
                prop_assert_eq!(header.record_len(), buf.len() as u64);
                prop_assert_eq!(decode_payload(header, &buf[HEADER_LEN..]), Ok(record));
            }

            /// CRC32C detects every error confined to 32 consecutive bits and
            /// CRC-64 every one confined to 64, so changing any one byte must
            /// be caught by the checksum covering it, in either format. A
            /// changed type byte may name the other format: the header is
            /// then read at the wrong length, and its checksum still fails.
            #[test]
            fn any_one_byte_change_is_detected(
                record in any_record(),
                format in any_format(),
                at in any::<Index>(),
                mask in 1u8..,
            ) {
                let mut buf = Vec::new();
                encode_as(format, &record, &mut buf);
                let header_len = format.header_len();
                let i = at.index(buf.len());
                buf[i] ^= mask;
                if i < header_len {
                    let read_len = Format::of(buf[4]).header_len();
                    prop_assert_eq!(
                        decode_header(&buf[..read_len]),
                        Err(DecodeError::Corrupt(CorruptionReason::HeaderChecksum))
                    );
                } else {
                    let header = decode_header(&buf[..header_len]).unwrap();
                    prop_assert_eq!(
                        decode_payload(header, &buf[header_len..]),
                        Err(DecodeError::Corrupt(CorruptionReason::PayloadChecksum))
                    );
                }
            }

            #[test]
            fn round_trip_in_the_old_format(record in any_record()) {
                let mut buf = Vec::new();
                encode_as(Format::Crc32c, &record, &mut buf);
                let header = decode_header(header_of(&buf)).unwrap();
                prop_assert_eq!(header.record_len(), buf.len() as u64);
                prop_assert_eq!(decode_payload(header, &buf[15..]), Ok(record));
            }

            /// Restates the record rules independently of `decode_header`.
            #[test]
            fn decode_header_accepts_exactly_the_valid_headers(
                type_byte in prop_oneof![0u8..5, 0x10u8..0x15, Just(0x23)],
                key_len in prop_oneof![0u16..4, 1020u16..1030, Just(u16::MAX)],
                value_len in prop_oneof![0u32..4, Just(8), 16_777_210u32..16_777_220, Just(u32::MAX)],
                payload_checksum in any::<u64>(),
            ) {
                let kind = if type_byte >> 4 <= 1 { type_byte & 0x0f } else { 0xff };
                let batch = kind == 3 && key_len == 0 && value_len == 8;
                let valid = batch || matches!(kind, 1 | 2)
                    && key_len != 0
                    && key_len as usize <= MAX_KEY_LEN
                    && value_len as usize <= MAX_VALUE_LEN
                    && (kind != 1 || value_len != 0)
                    && (kind != 2 || value_len == 0);
                let bytes = header_bytes(type_byte, key_len, value_len, payload_checksum);
                prop_assert_eq!(decode_header(&bytes).is_ok(), valid);
            }

            /// A valid header with a matching payload checksum always decodes:
            /// the `expect`s in `decode_payload` can never fire.
            #[test]
            fn a_valid_header_and_matching_payload_always_decode(
                kind in 0u8..4,
                format in any_format(),
                payload in prop::collection::vec(any::<u8>(), 0..16),
                split in any::<Index>(),
            ) {
                let key_len = split.index(payload.len() + 1);
                let value_len = payload.len() - key_len;
                let checksum = format.checksum(&payload);
                let type_byte = (format as u8) << 4 | kind;
                let bytes = header_bytes(type_byte, key_len as u16, value_len as u32, checksum);
                if let Ok(header) = decode_header(&bytes) {
                    prop_assert!(decode_payload(header, &payload).is_ok());
                }
            }
        }
    }
}
