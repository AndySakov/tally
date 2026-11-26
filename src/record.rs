//! The record format (DESIGN.md, Record format).

use crc_fast::CrcAlgorithm;
use crc32c::{crc32c, crc32c_append};

use crate::error::DecodeError;
use crate::{CorruptionReason, Field, Key, MAX_KEY_LEN, MAX_VALUE_LEN, UnsupportedReason, Value};

/// Header checksum, type byte, key length, value length, payload checksum.
pub const HEADER_LEN: usize = 4 + 1 + 2 + 4 + 8;

/// The payload of a Batch record: the byte length of the records after it
/// that belong to the batch.
pub const BATCH_PAYLOAD_LEN: u32 = 8;

/// The high nibble of every type byte this format writes.
const FORMAT: u8 = 0x30;

/// Set in the type byte of a record inside a batch, so a scan looking for
/// where an append starts never takes one for it.
const INNER: u8 = 0x40;

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

/// Where a record sits: on its own in the log, or inside a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Place {
    Top,
    Inner,
}

fn kind_of(type_byte: u8) -> Result<(Kind, Place), DecodeError> {
    match type_byte {
        0 => Err(DecodeError::Corrupt(CorruptionReason::ZeroType)),
        0x31 => Ok((Kind::Set, Place::Top)),
        0x32 => Ok((Kind::Delete, Place::Top)),
        0x33 => Ok((Kind::Batch, Place::Top)),
        0x71 => Ok((Kind::Set, Place::Inner)),
        0x72 => Ok((Kind::Delete, Place::Inner)),
        0x73 => Err(DecodeError::Corrupt(CorruptionReason::BadBatch)),
        byte => Err(DecodeError::Unsupported(UnsupportedReason::UnknownType(
            byte,
        ))),
    }
}

fn type_byte(kind: Kind, place: Place) -> u8 {
    let inner = if place == Place::Inner { INNER } else { 0 };
    FORMAT | inner | kind as u8
}

/// A decoded header. If one exists, its checksum was valid at its offset
/// and its fields satisfy every record rule, so only the payload checksum
/// is left to check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    fields: Fields,
    payload_checksum: u64,
}

impl Header {
    pub fn fields(self) -> Fields {
        self.fields
    }

    pub fn payload_len(self) -> usize {
        self.fields.payload_len()
    }

    pub fn record_len(self) -> u64 {
        self.fields.record_len()
    }
}

/// A Set or Delete, as a caller writes it.
#[derive(Debug, PartialEq, Eq)]
pub enum Op {
    Set { key: Key, value: Value },
    Delete { key: Key },
}

impl Op {
    pub fn key(&self) -> &Key {
        let (Op::Set { key, .. } | Op::Delete { key }) = self;
        key
    }

    fn value(&self) -> &[u8] {
        match self {
            Op::Set { value, .. } => value.as_bytes(),
            Op::Delete { .. } => &[],
        }
    }

    /// The fields of its record, on its own in the log.
    pub fn fields(&self) -> Fields {
        Fields {
            kind: match self {
                Op::Set { .. } => Kind::Set,
                Op::Delete { .. } => Kind::Delete,
            },
            place: Place::Top,
            key_len: u16::try_from(self.key().as_bytes().len())
                .expect("Key guarantees len fits u16"),
            value_len: u32::try_from(self.value().len()).expect("Value guarantees len fits u32"),
        }
    }
}

/// Appends the record of `op`, to be written at log offset `offset` in
/// `place`, to `buf`.
pub fn encode_op(op: &Op, place: Place, offset: u64, buf: &mut Vec<u8>) {
    let kind = op.fields().kind;
    encode_parts(kind, place, op.key().as_bytes(), op.value(), offset, buf);
}

/// Appends a Batch record whose body is the next `body_len` bytes.
pub fn encode_batch(body_len: u64, offset: u64, buf: &mut Vec<u8>) {
    encode_parts(
        Kind::Batch,
        Place::Top,
        &[],
        &body_len.to_le_bytes(),
        offset,
        buf,
    );
}

/// Appends `record` as `encode_op` and `encode_batch` would.
#[cfg(any(test, fuzzing))]
pub fn encode(record: &Record, place: Place, offset: u64, buf: &mut Vec<u8>) {
    match record {
        Record::Set { key, value } => encode_parts(
            Kind::Set,
            place,
            key.as_bytes(),
            value.as_bytes(),
            offset,
            buf,
        ),
        Record::Delete { key } => {
            encode_parts(Kind::Delete, place, key.as_bytes(), &[], offset, buf)
        }
        Record::Batch { body_len } => encode_batch(*body_len, offset, buf),
    }
}

fn encode_parts(
    kind: Kind,
    place: Place,
    key: &[u8],
    value: &[u8],
    offset: u64,
    buf: &mut Vec<u8>,
) {
    let start = buf.len();
    buf.reserve(HEADER_LEN + key.len() + value.len());
    buf.extend_from_slice(&[0; 4]);
    buf.push(type_byte(kind, place));
    buf.extend_from_slice(
        &u16::try_from(key.len())
            .expect("Key guarantees len fits u16")
            .to_le_bytes(),
    );
    buf.extend_from_slice(
        &u32::try_from(value.len())
            .expect("Value guarantees len fits u32")
            .to_le_bytes(),
    );
    buf.extend_from_slice(&[0; 8]);
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
    let payload_checksum = crc_fast::checksum(CrcAlgorithm::Crc64Nvme, &buf[start + HEADER_LEN..]);
    let header = &mut buf[start..start + HEADER_LEN];
    header[11..].copy_from_slice(&payload_checksum.to_le_bytes());
    seal(header, offset);
}

/// Rewrites the header of a record already encoded so it belongs at
/// `offset`, on its own in the log, as when compaction copies it.
pub fn move_header(header: &mut [u8; HEADER_LEN], offset: u64) {
    header[4] &= !INNER;
    seal(header, offset);
}

/// Writes the header checksum of a record at `offset`.
pub fn seal(header: &mut [u8], offset: u64) {
    let checksum = header_checksum(header, offset);
    header[..4].copy_from_slice(&checksum.to_le_bytes());
}

/// Covers the record's offset too, so a valid record left from another
/// place in the log, or another file, fails it.
fn header_checksum(header: &[u8], offset: u64) -> u32 {
    crc32c_append(crc32c(&offset.to_le_bytes()), &header[4..])
}

/// Whether `bytes` are the header of a record written on its own at
/// `offset`, whatever its type and lengths: a later version's record
/// counts too.
pub fn starts_a_record(bytes: &[u8; HEADER_LEN], offset: u64) -> bool {
    bytes[4] != 0
        && bytes[4] & INNER == 0
        && bytes[..4] == header_checksum(bytes, offset).to_le_bytes()
}

/// Checks the header checksum for a record at `offset`, then the type,
/// the limits and the record rules.
pub fn decode_header(bytes: &[u8; HEADER_LEN], offset: u64) -> Result<Header, DecodeError> {
    if bytes[..4] != header_checksum(bytes, offset).to_le_bytes() {
        return Err(DecodeError::Corrupt(CorruptionReason::HeaderChecksum));
    }
    let fields = decode_fields(bytes[4..11].try_into().unwrap())?;
    Ok(Header {
        fields,
        payload_checksum: u64::from_le_bytes(bytes[11..].try_into().unwrap()),
    })
}

/// Header bytes 4 to 11: the type byte, `key_len` and `value_len`. A hint
/// entry starts with the same bytes.
pub const FIELDS_LEN: usize = 7;

/// What a header says about its record, apart from the payload checksum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fields {
    pub kind: Kind,
    pub place: Place,
    pub key_len: u16,
    pub value_len: u32,
}

impl Fields {
    pub fn to_bytes(self) -> [u8; FIELDS_LEN] {
        let mut bytes = [0u8; FIELDS_LEN];
        bytes[0] = type_byte(self.kind, self.place);
        bytes[1..3].copy_from_slice(&self.key_len.to_le_bytes());
        bytes[3..].copy_from_slice(&self.value_len.to_le_bytes());
        bytes
    }

    pub fn payload_len(self) -> usize {
        usize::from(self.key_len) + self.value_len as usize
    }

    pub fn record_len(self) -> u64 {
        record_len(self.key_len, self.value_len)
    }
}

/// The length on disk of a record with these lengths.
pub fn record_len(key_len: u16, value_len: u32) -> u64 {
    (HEADER_LEN + usize::from(key_len)) as u64 + u64::from(value_len)
}

/// Checks a type and lengths against the limits and the record rules.
pub fn decode_fields(bytes: &[u8; FIELDS_LEN]) -> Result<Fields, DecodeError> {
    let (kind, place) = kind_of(bytes[0])?;
    let key_len = u16::from_le_bytes([bytes[1], bytes[2]]);
    let value_len = u32::from_le_bytes(bytes[3..].try_into().unwrap());
    let fields = Fields {
        kind,
        place,
        key_len,
        value_len,
    };
    if kind == Kind::Batch {
        if key_len != 0 || value_len != BATCH_PAYLOAD_LEN {
            return Err(DecodeError::Corrupt(CorruptionReason::BadBatch));
        }
        return Ok(fields);
    }
    if key_len == 0 {
        return Err(DecodeError::Corrupt(CorruptionReason::EmptyKey));
    }
    if usize::from(key_len) > MAX_KEY_LEN {
        return Err(DecodeError::Unsupported(UnsupportedReason::OverLimit {
            field: Field::Key,
            len: usize::from(key_len),
            max: MAX_KEY_LEN,
        }));
    }
    if value_len as usize > MAX_VALUE_LEN {
        return Err(DecodeError::Unsupported(UnsupportedReason::OverLimit {
            field: Field::Value,
            len: value_len as usize,
            max: MAX_VALUE_LEN,
        }));
    }
    if kind == Kind::Delete && value_len != 0 {
        return Err(DecodeError::Corrupt(CorruptionReason::DeleteWithValue));
    }
    Ok(fields)
}

/// Checks the payload checksum and builds the record.
///
/// Panics if `payload.len() != header.payload_len()`: that means the
/// caller read the wrong number of bytes, which is a bug, not bad data.
pub fn decode_payload(header: Header, payload: &[u8]) -> Result<Record, DecodeError> {
    Ok(match decode_index_op(header, payload)? {
        IndexOp::Set { key } => Record::Set {
            key,
            value: Value::try_from(&payload[usize::from(header.fields.key_len)..])
                .expect("decode_header checked value_len"),
        },
        IndexOp::Delete { key } => Record::Delete { key },
        IndexOp::Batch { body_len } => Record::Batch { body_len },
    })
}

/// What a record does to the index, without the value bytes, whose length
/// its header has.
#[derive(Debug, PartialEq, Eq)]
pub enum IndexOp {
    Set { key: Key },
    Delete { key: Key },
    Batch { body_len: u64 },
}

impl IndexOp {
    /// The key's bytes, none for a Batch.
    pub fn key(&self) -> &[u8] {
        match self {
            IndexOp::Set { key } | IndexOp::Delete { key } => key.as_bytes(),
            IndexOp::Batch { .. } => &[],
        }
    }
}

/// Checks the payload checksum and returns what the record does to the
/// index, without copying the value.
///
/// Panics if `payload.len() != header.payload_len()`, as `decode_payload`.
pub fn decode_index_op(header: Header, payload: &[u8]) -> Result<IndexOp, DecodeError> {
    check_payload(header, payload)?;
    Ok(index_op(header, payload))
}

/// Checks the payload checksum.
///
/// Panics if `payload.len() != header.payload_len()`, as `decode_payload`.
pub fn check_payload(header: Header, payload: &[u8]) -> Result<(), DecodeError> {
    assert_eq!(
        payload.len(),
        header.payload_len(),
        "caller passed a payload of the wrong length"
    );
    if header.payload_checksum != crc_fast::checksum(CrcAlgorithm::Crc64Nvme, payload) {
        return Err(DecodeError::Corrupt(CorruptionReason::PayloadChecksum));
    }
    Ok(())
}

/// What the record with this header and checked payload does to the index.
pub fn index_op(header: Header, payload: &[u8]) -> IndexOp {
    let key = || {
        Key::try_from(&payload[..usize::from(header.fields.key_len)])
            .expect("decode_header checked key_len")
    };
    match header.fields.kind {
        Kind::Set => IndexOp::Set { key: key() },
        Kind::Delete => IndexOp::Delete { key: key() },
        Kind::Batch => IndexOp::Batch {
            body_len: u64::from_le_bytes(payload.try_into().expect("decode_header checked it")),
        },
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::Error;

    // Computed independently of this crate, with Python implementations of
    // CRC32C and CRC-64/NVME checked against their standard test vectors.
    /// `a` = `hi`, on its own at offset 0.
    pub(crate) const SET_A_HI: [u8; 22] = [
        0x56, 0x40, 0xe6, 0xb9, // header checksum, over the offset too
        0x31, // type: Set
        0x01, 0x00, // key_len: 1
        0x02, 0x00, 0x00, 0x00, // value_len: 2
        0x31, 0x8d, 0xe2, 0x77, 0x90, 0x61, 0xc1, 0x8f, // payload checksum
        0x61, // key: "a"
        0x68, 0x69, // value: "hi"
    ];

    /// Delete `a`, on its own at offset 22, after `SET_A_HI`.
    pub(crate) const DELETE_A: [u8; 20] = [
        0x72, 0x01, 0xcc, 0x30, // header checksum
        0x32, // type: Delete
        0x01, 0x00, // key_len: 1
        0x00, 0x00, 0x00, 0x00, // value_len: 0
        0x3c, 0xfc, 0xcb, 0xb4, 0x45, 0x84, 0x2f, 0x8c, // payload checksum
        0x61, // key: "a"
    ];

    /// At offset 0: a batch of `a` = `hi` then delete `a`.
    pub(crate) const BATCH_OF_SET_AND_DELETE: [u8; 69] = [
        0x40, 0x79, 0x1c, 0x49, // header checksum
        0x33, // type: Batch
        0x00, 0x00, // key_len: 0
        0x08, 0x00, 0x00, 0x00, // value_len: 8
        0xc2, 0x34, 0xe0, 0x29, 0xd3, 0xef, 0xe5, 0x9a, // payload checksum
        0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // body_len: 42
        0xb3, 0xe7, 0x74, 0x54, // header checksum, at offset 27
        0x71, // type: Set, inside a batch
        0x01, 0x00, 0x02, 0x00, 0x00, 0x00, // key_len 1, value_len 2
        0x31, 0x8d, 0xe2, 0x77, 0x90, 0x61, 0xc1, 0x8f, // payload checksum
        0x61, 0x68, 0x69, // "a", "hi"
        0xac, 0xcf, 0x98, 0x62, // header checksum, at offset 49
        0x72, // type: Delete, inside a batch
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, // key_len 1, value_len 0
        0x3c, 0xfc, 0xcb, 0xb4, 0x45, 0x84, 0x2f, 0x8c, // payload checksum
        0x61, // "a"
    ];

    fn key(bytes: &[u8]) -> Key {
        Key::try_from(bytes).unwrap()
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

    fn header(bytes: &[u8]) -> &[u8; HEADER_LEN] {
        bytes[..HEADER_LEN].try_into().unwrap()
    }

    /// A header with a valid checksum at offset 0 over these fields.
    fn header_with(type_byte: u8, key_len: u16, value_len: u32) -> [u8; HEADER_LEN] {
        let mut bytes = [0u8; HEADER_LEN];
        bytes[4] = type_byte;
        bytes[5..7].copy_from_slice(&key_len.to_le_bytes());
        bytes[7..11].copy_from_slice(&value_len.to_le_bytes());
        seal(&mut bytes, 0);
        bytes
    }

    #[test]
    fn encode_matches_the_format() {
        let mut buf = vec![0xaa];
        encode(&set(b"a", b"hi"), Place::Top, 0, &mut buf);
        encode(&Record::Delete { key: key(b"a") }, Place::Top, 22, &mut buf);
        assert_eq!(buf[0], 0xaa, "encode appends");
        assert_eq!(buf[1..23], SET_A_HI);
        assert_eq!(buf[23..], DELETE_A);

        let mut batch = Vec::new();
        encode(&Record::Batch { body_len: 42 }, Place::Top, 0, &mut batch);
        encode(&set(b"a", b"hi"), Place::Inner, 27, &mut batch);
        encode(
            &Record::Delete { key: key(b"a") },
            Place::Inner,
            49,
            &mut batch,
        );
        assert_eq!(batch, BATCH_OF_SET_AND_DELETE);
    }

    #[test]
    fn decode_reads_what_encode_wrote() {
        let h = decode_header(header(&SET_A_HI), 0).unwrap();
        assert_eq!(
            h.fields(),
            Fields {
                kind: Kind::Set,
                place: Place::Top,
                key_len: 1,
                value_len: 2
            }
        );
        assert_eq!(h.record_len(), 22);
        assert_eq!(
            decode_payload(h, &SET_A_HI[HEADER_LEN..]),
            Ok(set(b"a", b"hi"))
        );
        assert_eq!(
            decode_index_op(h, &SET_A_HI[HEADER_LEN..]),
            Ok(IndexOp::Set { key: key(b"a") })
        );

        let inner = &BATCH_OF_SET_AND_DELETE[27 + 22..];
        let h = decode_header(header(inner), 49).unwrap();
        assert_eq!(h.fields().place, Place::Inner);
        assert_eq!(
            decode_payload(h, &inner[HEADER_LEN..]),
            Ok(Record::Delete { key: key(b"a") })
        );
    }

    #[test]
    fn a_record_at_another_offset_fails_its_header_checksum() {
        assert_eq!(
            decode_header(header(&SET_A_HI), 22),
            Err(DecodeError::Corrupt(CorruptionReason::HeaderChecksum))
        );
    }

    #[test]
    fn moving_a_header_reseals_it_on_its_own_at_the_new_offset() {
        let mut moved = *header(&BATCH_OF_SET_AND_DELETE[27..]);
        move_header(&mut moved, 0);
        assert_eq!(moved, *header(&SET_A_HI));
    }

    #[test]
    fn a_set_may_have_an_empty_value() {
        let mut buf = Vec::new();
        encode(&set(b"a", b""), Place::Top, 7, &mut buf);
        let h = decode_header(header(&buf), 7).unwrap();
        assert_eq!(decode_payload(h, &buf[HEADER_LEN..]), Ok(set(b"a", b"")));
    }

    #[test]
    fn decode_header_rejects_what_breaks_a_rule() {
        let corrupt = |reason| Err(DecodeError::Corrupt(reason));
        let unsupported = |reason| Err(DecodeError::Unsupported(reason));
        let cases = [
            (header_with(0x31, 0, 2), corrupt(CorruptionReason::EmptyKey)),
            (
                header_with(0x32, 1, 1),
                corrupt(CorruptionReason::DeleteWithValue),
            ),
            (header_with(0x33, 1, 8), corrupt(CorruptionReason::BadBatch)),
            (header_with(0x33, 0, 9), corrupt(CorruptionReason::BadBatch)),
            (header_with(0x73, 0, 8), corrupt(CorruptionReason::BadBatch)),
            (header_with(0x00, 1, 2), corrupt(CorruptionReason::ZeroType)),
            (
                header_with(0x11, 1, 2),
                unsupported(UnsupportedReason::UnknownType(0x11)),
            ),
            (
                header_with(0x31, MAX_KEY_LEN as u16 + 1, 2),
                unsupported(UnsupportedReason::OverLimit {
                    field: Field::Key,
                    len: MAX_KEY_LEN + 1,
                    max: MAX_KEY_LEN,
                }),
            ),
            (
                header_with(0x31, 1, MAX_VALUE_LEN as u32 + 1),
                unsupported(UnsupportedReason::OverLimit {
                    field: Field::Value,
                    len: MAX_VALUE_LEN + 1,
                    max: MAX_VALUE_LEN,
                }),
            ),
            ([0; HEADER_LEN], corrupt(CorruptionReason::HeaderChecksum)),
        ];
        for (bytes, expected) in cases {
            assert_eq!(
                decode_header(&bytes, 0).map(Header::fields),
                expected,
                "{bytes:02x?}"
            );
        }
        let at_the_limits = header_with(0x31, MAX_KEY_LEN as u16, MAX_VALUE_LEN as u32);
        assert!(decode_header(&at_the_limits, 0).is_ok());
    }

    #[test]
    fn decode_payload_rejects_a_damaged_or_zeroed_payload() {
        let h = decode_header(header(&SET_A_HI), 0).unwrap();
        for payload in [&b"ahX"[..], &[0, 0, 0]] {
            assert_eq!(
                decode_payload(h, payload),
                Err(DecodeError::Corrupt(CorruptionReason::PayloadChecksum))
            );
        }
    }

    #[test]
    #[should_panic(expected = "caller passed a payload of the wrong length")]
    fn decode_payload_panics_when_the_caller_passes_the_wrong_length() {
        let h = decode_header(header(&SET_A_HI), 0).unwrap();
        let _ = decode_payload(h, b"ah");
    }

    #[test]
    fn at_attaches_the_offset() {
        assert!(matches!(
            DecodeError::Corrupt(CorruptionReason::HeaderChecksum).at(4096),
            Error::Corruption {
                offset: 4096,
                reason: CorruptionReason::HeaderChecksum
            }
        ));
    }

    mod props {
        use super::*;
        use proptest::prelude::*;

        fn any_record() -> impl Strategy<Value = Record> {
            let key = proptest::collection::vec(any::<u8>(), 1..=64);
            let value = proptest::collection::vec(any::<u8>(), 0..=256);
            prop_oneof![
                (key.clone(), value).prop_map(|(k, v)| set(&k, &v)),
                key.prop_map(|k| Record::Delete {
                    key: super::key(&k)
                }),
                any::<u64>().prop_map(|body_len| Record::Batch { body_len }),
            ]
        }

        fn placed() -> impl Strategy<Value = (Record, Place)> {
            (any_record(), any::<bool>()).prop_map(|(record, inner)| {
                let batch = matches!(record, Record::Batch { .. });
                let place = if inner && !batch {
                    Place::Inner
                } else {
                    Place::Top
                };
                (record, place)
            })
        }

        fn decode(bytes: &[u8], offset: u64) -> Result<Record, DecodeError> {
            let h = decode_header(header(bytes), offset)?;
            if bytes.len() != HEADER_LEN + h.payload_len() {
                return Err(DecodeError::Corrupt(CorruptionReason::PayloadChecksum));
            }
            decode_payload(h, &bytes[HEADER_LEN..])
        }

        proptest! {
            #[test]
            fn round_trip((record, place) in placed(), offset in any::<u64>()) {
                let mut buf = Vec::new();
                encode(&record, place, offset, &mut buf);
                let decoded = decode_header(header(&buf), offset).map(|h| h.fields().place);
                prop_assert_eq!(decoded, Ok(place));
                prop_assert_eq!(decode(&buf, offset), Ok(record));
            }

            #[test]
            fn any_one_byte_change_is_detected(
                record in any_record(),
                offset in any::<u64>(),
                at in any::<prop::sample::Index>(),
                flip in 1..=255u8,
            ) {
                let mut buf = Vec::new();
                encode(&record, Place::Top, offset, &mut buf);
                let at = at.index(buf.len());
                buf[at] ^= flip;
                prop_assert!(decode(&buf, offset).is_err());
            }

            #[test]
            fn decode_header_accepts_exactly_the_valid_headers(
                fields in any::<[u8; FIELDS_LEN]>(),
                offset in any::<u64>(),
            ) {
                let mut bytes = [0u8; HEADER_LEN];
                bytes[4..11].copy_from_slice(&fields);
                seal(&mut bytes, offset);
                prop_assert_eq!(
                    decode_header(&bytes, offset).map(Header::fields),
                    decode_fields(&fields)
                );
            }
        }
    }
}
