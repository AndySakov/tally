//! Arbitrary bytes into the decoder, in either record format: the type byte
//! picks the header's length. Bits of the first byte choose whether to fix
//! up the payload and header checksums first, so the fuzzer reaches the
//! checks behind them instead of stalling on a CRC match.
//!
//! Must never panic, and anything that decodes must re-encode, in its own
//! format, to exactly the bytes it came from.

#![no_main]

use libfuzzer_sys::fuzz_target;
use tally::fuzz::{Format, SHORTEST_HEADER_LEN, decode_header, decode_payload, encode_as};

const FIX_HEADER: u8 = 1;
const FIX_PAYLOAD: u8 = 2;

fuzz_target!(|data: &[u8]| {
    let Some((&flags, rest)) = data.split_first() else {
        return;
    };
    if rest.len() < SHORTEST_HEADER_LEN {
        return;
    }
    let mut bytes = rest.to_vec();
    let format = Format::of(bytes[4]);
    let header_len = format.header_len();
    if bytes.len() < header_len {
        return;
    }

    // The payload checksum sits inside the header, so fix it first.
    if flags & FIX_PAYLOAD != 0 {
        let key_len = u16::from_le_bytes([bytes[5], bytes[6]]) as usize;
        let value_len = u32::from_le_bytes([bytes[7], bytes[8], bytes[9], bytes[10]]) as usize;
        let end = header_len.saturating_add(key_len).saturating_add(value_len);
        if let Some(payload) = bytes.get(header_len..end) {
            match format {
                Format::Crc32c => {
                    let checksum = crc32c::crc32c(payload);
                    bytes[11..15].copy_from_slice(&checksum.to_le_bytes());
                }
                Format::Crc64 => {
                    let checksum = crc_fast::checksum(crc_fast::CrcAlgorithm::Crc64Nvme, payload);
                    bytes[11..19].copy_from_slice(&checksum.to_le_bytes());
                }
            }
        }
    }
    if flags & FIX_HEADER != 0 {
        let checksum = crc32c::crc32c(&bytes[4..header_len]);
        bytes[..4].copy_from_slice(&checksum.to_le_bytes());
    }

    let Ok(header) = decode_header(&bytes[..header_len]) else {
        return;
    };
    assert_eq!(header.format(), format);
    let Some(payload) = bytes[header_len..].get(..header.payload_len()) else {
        return;
    };
    let Ok(record) = decode_payload(header, payload) else {
        return;
    };

    let mut again = Vec::new();
    encode_as(format, &record, &mut again);
    assert_eq!(again, bytes[..header.record_len() as usize]);
});
