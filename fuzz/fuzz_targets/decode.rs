//! Arbitrary bytes into the decoder, at an arbitrary log offset. Bits of the
//! first byte choose whether to fix up the payload and header checksums
//! first, so the fuzzer reaches the checks behind them instead of stalling
//! on a CRC match.
//!
//! Must never panic, and anything that decodes must re-encode, at the same
//! offset and place, to exactly the bytes it came from.

#![no_main]

use libfuzzer_sys::fuzz_target;
use tally::fuzz::{HEADER_LEN, decode_header, decode_payload, encode, seal};

const FIX_HEADER: u8 = 1;
const FIX_PAYLOAD: u8 = 2;

fuzz_target!(|data: &[u8]| {
    let Some((&flags, rest)) = data.split_first() else {
        return;
    };
    let Some((offset, rest)) = rest.split_first_chunk::<8>() else {
        return;
    };
    let offset = u64::from_le_bytes(*offset);
    if rest.len() < HEADER_LEN {
        return;
    }
    let mut bytes = rest.to_vec();

    // The payload checksum sits inside the header, so fix it first.
    if flags & FIX_PAYLOAD != 0 {
        let key_len = u16::from_le_bytes([bytes[5], bytes[6]]) as usize;
        let value_len = u32::from_le_bytes([bytes[7], bytes[8], bytes[9], bytes[10]]) as usize;
        let end = HEADER_LEN.saturating_add(key_len).saturating_add(value_len);
        if let Some(payload) = bytes.get(HEADER_LEN..end) {
            let checksum = crc_fast::checksum(crc_fast::CrcAlgorithm::Crc64Nvme, payload);
            bytes[11..19].copy_from_slice(&checksum.to_le_bytes());
        }
    }
    if flags & FIX_HEADER != 0 {
        seal(&mut bytes[..HEADER_LEN], offset);
    }

    let header_bytes = bytes[..HEADER_LEN].try_into().unwrap();
    let Ok(header) = decode_header(header_bytes, offset) else {
        return;
    };
    assert!(
        decode_header(header_bytes, offset.wrapping_add(1)).is_err(),
        "a header belongs to one offset"
    );
    let Some(payload) = bytes[HEADER_LEN..].get(..header.payload_len()) else {
        return;
    };
    let Ok(record) = decode_payload(header, payload) else {
        return;
    };

    let mut again = Vec::new();
    encode(&record, header.fields().place, offset, &mut again);
    assert_eq!(again, bytes[..header.record_len() as usize]);
});
