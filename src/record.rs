use crc32c::{crc32c, crc32c_append};

use crate::{
    CorruptionReason, DecodeError, Field, Key, MAX_KEY_LEN, MAX_VALUE_LEN, UnsupportedReason, Value,
};

pub const HEADER_LEN: usize = 15;

const _: () = assert!(
    HEADER_LEN == 4 + 1 + 2 + 4 + 4,
    "HEADER_LEN must match the field layout"
);

#[derive(Debug, PartialEq, Eq)]
pub enum Record {
    Set { key: Key, value: Value },
    Delete { key: Key },
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Set = 1,
    Delete = 2,
}

impl TryFrom<u8> for Kind {
    type Error = DecodeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Err(DecodeError::Corrupt(CorruptionReason::ZeroType)),
            1 => Ok(Kind::Set),
            2 => Ok(Kind::Delete),
            byte => Err(DecodeError::Unsupported(UnsupportedReason::UnknownType(
                byte,
            ))),
        }
    }
}

/// A decoded header. If one exists, its header CRC was valid and its
/// fields satisfy every record rule, so only the payload CRC is left
/// to check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    kind: Kind,
    key_len: u16,
    value_len: u32,
    payload_checksum: u32,
}

impl Header {
    pub fn payload_len(self) -> usize {
        self.key_len as usize + self.value_len as usize
    }

    pub fn record_len(self) -> u64 {
        (HEADER_LEN + self.payload_len()) as u64
    }
}

/// Appends the encoded record to `buf`. Does not clear `buf` first.
pub fn encode(record: &Record, buf: &mut Vec<u8>) {
    let mut header_bytes: [u8; HEADER_LEN] = [0u8; HEADER_LEN];
    let (kind, key_bytes, value_bytes): (Kind, &[u8], &[u8]) = match record {
        Record::Set { key, value } => (Kind::Set, key.as_bytes(), value.as_bytes()),
        Record::Delete { key } => (Kind::Delete, key.as_bytes(), &[]),
    };
    let key_len = key_bytes.len();
    let value_len = value_bytes.len();
    header_bytes[4] = kind as u8;
    header_bytes[5..7].copy_from_slice(
        &u16::try_from(key_len)
            .expect("key guarantees len fits u16")
            .to_le_bytes(),
    );
    header_bytes[7..11].copy_from_slice(
        &u32::try_from(value_len)
            .expect("Value guarantees len fits u32")
            .to_le_bytes(),
    );

    let payload_checksum = crc32c_append(crc32c(key_bytes), value_bytes);

    header_bytes[11..15].copy_from_slice(&payload_checksum.to_le_bytes());

    let header_checksum = crc32c(&header_bytes[4..]);
    header_bytes[..4].copy_from_slice(&header_checksum.to_le_bytes());

    let to_reserve = HEADER_LEN + key_len + value_len;
    buf.reserve(to_reserve);
    buf.extend_from_slice(&header_bytes);
    buf.extend_from_slice(key_bytes);
    buf.extend_from_slice(value_bytes);
}

/// Checks the header CRC, then the kind, the limits and the record rules.
pub fn decode_header(bytes: &[u8; HEADER_LEN]) -> Result<Header, DecodeError> {
    let stored_checksum = &bytes[..4];
    let computed_checksum = crc32c(&bytes[4..]).to_le_bytes();
    if stored_checksum != computed_checksum {
        return Err(DecodeError::Corrupt(CorruptionReason::HeaderChecksum));
    }
    let kind = Kind::try_from(bytes[4])?;
    let key_len = u16::from_le_bytes(bytes[5..7].try_into().unwrap());
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
    let payload_checksum = u32::from_le_bytes(bytes[11..15].try_into().unwrap());
    Ok(Header {
        kind,
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
    }
}

/// What a record does to the index, without the value bytes.
#[derive(Debug, PartialEq, Eq)]
pub enum IndexOp {
    Set { key: Key, value_len: u32 },
    Delete { key: Key },
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
    if header.payload_checksum != crc32c(payload) {
        return Err(DecodeError::Corrupt(CorruptionReason::PayloadChecksum));
    }
    let key_bytes = &payload[..header.key_len as usize];
    let key = Key::try_from(key_bytes).expect("decode_header validated key_len");
    Ok(match header.kind {
        Kind::Set => IndexOp::Set {
            key,
            value_len: header.value_len,
        },
        Kind::Delete => IndexOp::Delete { key },
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::Error;

    // Expected bytes computed independently of this crate (Python CRC32C,
    // checked against the standard test vector).
    pub(crate) const SET_A_HI: [u8; 18] = [
        0xca, 0xa7, 0x41, 0x82, // header_checksum
        0x01, // kind: Set
        0x01, 0x00, // key_len: 1
        0x02, 0x00, 0x00, 0x00, // value_len: 2
        0xd9, 0xe4, 0xf8, 0xe7, // payload_checksum
        0x61, // key: "a"
        0x68, 0x69, // value: "hi"
    ];

    pub(crate) const DELETE_A: [u8; 16] = [
        0x89, 0xd9, 0x75, 0x53, // header_checksum
        0x02, // kind: Delete
        0x01, 0x00, // key_len: 1
        0x00, 0x00, 0x00, 0x00, // value_len: 0
        0x30, 0x43, 0xd0, 0xc1, // payload_checksum
        0x61, // key: "a"
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

    fn header_of(bytes: &[u8]) -> &[u8; HEADER_LEN] {
        bytes.first_chunk().unwrap()
    }

    /// Builds a header with a valid header CRC, for testing the checks
    /// that run after the CRC.
    fn header_bytes(
        kind: u8,
        key_len: u16,
        value_len: u32,
        payload_checksum: u32,
    ) -> [u8; HEADER_LEN] {
        let mut out = [0u8; HEADER_LEN];
        out[4] = kind;
        out[5..7].copy_from_slice(&key_len.to_le_bytes());
        out[7..11].copy_from_slice(&value_len.to_le_bytes());
        out[11..15].copy_from_slice(&payload_checksum.to_le_bytes());
        let crc = crc32c::crc32c(&out[4..]);
        out[..4].copy_from_slice(&crc.to_le_bytes());
        out
    }

    #[test]
    fn crc32c_dependency_is_castagnoli() {
        assert_eq!(crc32c::crc32c(b"123456789"), 0xE306_9283);
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
        assert_eq!(&buf[..18], SET_A_HI);
        assert_eq!(&buf[18..], DELETE_A);
    }

    // decode_header: valid input

    #[test]
    fn decode_header_set() {
        let header = decode_header(header_of(&SET_A_HI)).unwrap();
        assert_eq!(
            header,
            Header {
                kind: Kind::Set,
                key_len: 1,
                value_len: 2,
                payload_checksum: 0xe7f8_e4d9
            }
        );
        assert_eq!(header.payload_len(), 3);
        assert_eq!(header.record_len(), 18);
    }

    #[test]
    fn decode_header_delete() {
        let header = decode_header(header_of(&DELETE_A)).unwrap();
        assert_eq!(
            header,
            Header {
                kind: Kind::Delete,
                key_len: 1,
                value_len: 0,
                payload_checksum: 0xc1d0_4330
            }
        );
        assert_eq!(header.payload_len(), 1);
        assert_eq!(header.record_len(), 16);
    }

    #[test]
    fn decode_header_accepts_lengths_exactly_at_the_limits() {
        let bytes = header_bytes(1, MAX_KEY_LEN as u16, MAX_VALUE_LEN as u32, 0);
        let header = decode_header(&bytes).unwrap();
        assert_eq!(header.payload_len(), 1024 + 16_777_216);
    }

    // decode_header: corruption and unsupported records

    #[test]
    fn decode_header_rejects_a_flipped_bit_in_a_length() {
        let mut bytes = *header_of(&SET_A_HI);
        bytes[7] ^= 0x01; // value_len 2 -> 3
        assert_eq!(
            decode_header(&bytes),
            Err(DecodeError::Corrupt(CorruptionReason::HeaderChecksum))
        );
    }

    #[test]
    fn decode_header_rejects_a_flipped_bit_in_the_payload_checksum() {
        let mut bytes = *header_of(&SET_A_HI);
        bytes[11] ^= 0x80;
        assert_eq!(
            decode_header(&bytes),
            Err(DecodeError::Corrupt(CorruptionReason::HeaderChecksum))
        );
    }

    #[test]
    fn decode_header_rejects_all_zeros() {
        assert_eq!(
            decode_header(&[0u8; HEADER_LEN]),
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
        assert_eq!(
            decode_header(&header_bytes(7, 1, 2, 0)),
            Err(DecodeError::Unsupported(UnsupportedReason::UnknownType(7)))
        );
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
                value_len: 2
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

        proptest! {
            #[test]
            fn round_trip(record in any_record()) {
                let buf = encoded(&record);
                let header = decode_header(header_of(&buf)).unwrap();
                prop_assert_eq!(header.record_len(), buf.len() as u64);
                prop_assert_eq!(decode_payload(header, &buf[HEADER_LEN..]), Ok(record));
            }

            /// CRC32C detects every error confined to 32 consecutive bits, so
            /// changing any one byte must be caught by the checksum covering it.
            #[test]
            fn any_one_byte_change_is_detected(
                record in any_record(),
                at in any::<Index>(),
                mask in 1u8..,
            ) {
                let mut buf = encoded(&record);
                let i = at.index(buf.len());
                buf[i] ^= mask;
                let header = decode_header(header_of(&buf));
                if i < HEADER_LEN {
                    prop_assert_eq!(header, Err(DecodeError::Corrupt(CorruptionReason::HeaderChecksum)));
                } else {
                    prop_assert_eq!(
                        decode_payload(header.unwrap(), &buf[HEADER_LEN..]),
                        Err(DecodeError::Corrupt(CorruptionReason::PayloadChecksum))
                    );
                }
            }

            /// Restates the record rules independently of `decode_header`.
            #[test]
            fn decode_header_accepts_exactly_the_valid_headers(
                kind in 0u8..5,
                key_len in prop_oneof![0u16..4, 1020u16..1030, Just(u16::MAX)],
                value_len in prop_oneof![0u32..4, 16_777_210u32..16_777_220, Just(u32::MAX)],
                payload_checksum in any::<u32>(),
            ) {
                let valid = matches!(kind, 1 | 2)
                    && key_len != 0
                    && key_len as usize <= MAX_KEY_LEN
                    && value_len as usize <= MAX_VALUE_LEN
                    && (kind != 1 || value_len != 0)
                    && (kind != 2 || value_len == 0);
                let bytes = header_bytes(kind, key_len, value_len, payload_checksum);
                prop_assert_eq!(decode_header(&bytes).is_ok(), valid);
            }

            /// A valid header with a matching payload checksum always decodes:
            /// the `expect`s in `decode_payload` can never fire.
            #[test]
            fn a_valid_header_and_matching_payload_always_decode(
                kind in 0u8..4,
                payload in prop::collection::vec(any::<u8>(), 0..16),
                split in any::<Index>(),
            ) {
                let key_len = split.index(payload.len() + 1);
                let value_len = payload.len() - key_len;
                let checksum = crc32c::crc32c(&payload);
                let bytes = header_bytes(kind, key_len as u16, value_len as u32, checksum);
                if let Ok(header) = decode_header(&bytes) {
                    prop_assert!(decode_payload(header, &payload).is_ok());
                }
            }
        }
    }
}
