use super::*;
use super::{commit::*, compact::*, open::*, scan::*};
use std::collections::HashMap;
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crate::fs::{Fs, OsFs};
use crate::index::{Entry, Index};
use crate::options::{AutoCompact, Options};
use crate::record::{
    BATCH_PAYLOAD_LEN, HEADER_LEN, Op, Place, Record, encode_batch, encode_op, record_len,
};
use crate::segment::{FILE_HEADER_LEN, Segment, compact_name, hint_name, log_name, parse_log_name};
use crate::{Batch, CorruptionReason, Error, Key, UnsupportedReason, Value};
use std::fs::{self, OpenOptions};
use std::os::unix::fs::FileExt;

use crate::mem_fs::{Cut, MemFs, Torn};
use crate::record::tests::{DELETE_A, SET_A_HI};
use crate::record::{encode, seal};
use crate::segment::segment_file;

fn key(bytes: &[u8]) -> Key {
    Key::try_from(bytes).unwrap()
}

fn value(bytes: &[u8]) -> Value {
    Value::try_from(bytes).unwrap()
}

/// Bases of the segments in `dir`, in log order.
fn segment_bases(dir: &Path) -> Vec<u64> {
    let mut bases: Vec<u64> = fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| parse_log_name(e.unwrap().file_name().to_str()?))
        .collect();
    bases.sort_unstable();
    bases
}

/// The newest segment, or the first one before the first open.
fn log_path(dir: &Path) -> PathBuf {
    dir.join(log_name(segment_bases(dir).last().copied().unwrap_or(0)))
}

/// The records of the segment at `base`, after checking its header.
fn segment_records(dir: &Path, base: u64) -> Vec<u8> {
    let bytes = fs::read(dir.join(log_name(base))).unwrap();
    let header = FILE_HEADER_LEN as usize;
    assert_eq!(
        bytes[..header],
        segment_file(base, b"")[..],
        "header of {base}"
    );
    bytes[header..].to_vec()
}

/// Every segment's records, in log order.
fn log_bytes(dir: &Path) -> Vec<u8> {
    segment_bases(dir)
        .into_iter()
        .flat_map(|base| segment_records(dir, base))
        .collect()
}

/// Writes a segment at `base` holding `records`.
fn write_segment(dir: &Path, base: u64, records: &[u8]) {
    fs::write(dir.join(log_name(base)), segment_file(base, records)).unwrap();
}

/// Overwrites bytes in the newest segment behind the store's back, at
/// an offset from its first record.
fn write_log_at(dir: &Path, offset: u64, bytes: &[u8]) {
    let file = OpenOptions::new().write(true).open(log_path(dir)).unwrap();
    file.write_all_at(bytes, FILE_HEADER_LEN + offset).unwrap();
}

/// The records of `ops`, each on its own, the first at `start`.
fn encoded_at(start: u64, ops: &[Op]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for op in ops {
        encode_op(op, Place::Top, start + bytes.len() as u64, &mut bytes);
    }
    bytes
}

fn encoded(ops: &[Op]) -> Vec<u8> {
    encoded_at(0, ops)
}

/// The records of `ops` inside one Batch record at `start`, as an
/// append of more than one writes them.
fn batched_at(start: u64, ops: &[Op]) -> Vec<u8> {
    let frame = record_len(0, BATCH_PAYLOAD_LEN);
    let mut body = Vec::new();
    for op in ops {
        let offset = start + frame + body.len() as u64;
        encode_op(op, Place::Inner, offset, &mut body);
    }
    let mut bytes = Vec::new();
    encode_batch(body.len() as u64, start, &mut bytes);
    bytes.extend(body);
    bytes
}

/// A record encoded on its own, moved to `offset`.
fn moved(record: &[u8], offset: u64) -> Vec<u8> {
    let mut bytes = record.to_vec();
    seal(&mut bytes[..HEADER_LEN], offset);
    bytes
}

/// Side files `open` saved torn tails to, with their bytes.
fn torn_files(dir: &Path) -> Vec<(String, Vec<u8>)> {
    file_names(dir)
        .into_iter()
        .filter(|name| name.starts_with("tally.log.torn-"))
        .map(|name| {
            let bytes = fs::read(dir.join(&name)).unwrap();
            (name, bytes)
        })
        .collect()
}

// open

#[test]
fn open_creates_the_directory_and_an_empty_log() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("db");
    Store::open(&dir).unwrap();
    assert_eq!(log_bytes(&dir), b"");
}

#[test]
fn open_an_existing_empty_store() {
    let tmp = tempfile::tempdir().unwrap();
    drop(Store::open(tmp.path()).unwrap());
    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(store.get(&key(b"a")).unwrap(), None);
}

#[test]
fn a_second_open_of_the_same_directory_is_locked_out() {
    let tmp = tempfile::tempdir().unwrap();
    let first = Store::open(tmp.path()).unwrap();
    assert!(matches!(Store::open(tmp.path()), Err(Error::Locked)));
    drop(first);
    Store::open(tmp.path()).unwrap();
}

#[test]
fn open_fails_on_a_corrupted_record_with_its_offset() {
    let tmp = tempfile::tempdir().unwrap();
    let mut bytes = [SET_A_HI.as_slice(), DELETE_A.as_slice()].concat();
    bytes[20] ^= 0x01; // first record's value: "hi" -> "ii"
    write_segment(tmp.path(), 0, &bytes);
    assert!(matches!(
        Store::open(tmp.path()),
        Err(Error::Corruption {
            offset: 0,
            reason: CorruptionReason::PayloadChecksum
        })
    ));
}

/// A store directory whose log holds exactly `bytes`.
fn store_with_log(bytes: &[u8]) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    write_segment(tmp.path(), 0, bytes);
    tmp
}

fn set_a_hi_then(tail: &[u8]) -> Vec<u8> {
    [SET_A_HI.as_slice(), tail].concat()
}

/// DELETE_A's header with its payload (the key) zeroed: the header
/// reached the disk but the payload did not.
fn delete_a_with_a_zero_payload() -> Vec<u8> {
    let mut bytes = DELETE_A.to_vec();
    bytes[HEADER_LEN] = 0;
    bytes
}

// recovery: a torn tail is saved, unless it is zeros, and truncated

#[test]
fn open_moves_a_torn_record_to_a_side_file_and_truncates_it() {
    for cut in 1..DELETE_A.len() {
        let tmp = store_with_log(&set_a_hi_then(&DELETE_A[..cut]));
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        assert_eq!(log_bytes(tmp.path()), SET_A_HI, "cut at {cut}");
        let crc = crc32c::crc32c(&DELETE_A[..cut]);
        assert_eq!(
            torn_files(tmp.path()),
            [(
                format!("tally.log.torn-22-{crc:08x}"),
                DELETE_A[..cut].to_vec()
            )],
            "cut at {cut}"
        );
    }
}

#[test]
fn a_record_whose_end_landed_without_its_start_is_a_torn_tail() {
    let mut torn = DELETE_A.to_vec();
    torn[..10].fill(0);
    let tmp = store_with_log(&set_a_hi_then(&torn));
    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
    assert_eq!(log_bytes(tmp.path()), SET_A_HI);
    assert_eq!(torn_files(tmp.path())[0].1, torn);
}

#[test]
fn a_record_this_version_cannot_read_after_damage_keeps_the_damage_from_being_torn() {
    let mut unknown = encoded_at(42, &[set_record(b"b", b"2")]);
    unknown[4] = 0x34;
    seal(&mut unknown[..HEADER_LEN], 42);
    let mut over_limit = encoded_at(42, &[set_record(b"b", b"2")]);
    over_limit[7..11].copy_from_slice(&u32::MAX.to_le_bytes());
    seal(&mut over_limit[..HEADER_LEN], 42);
    for later in [unknown, over_limit] {
        let mut damaged = set_a_hi_then(&DELETE_A);
        damaged[22 + 19] ^= 0x01;
        damaged.extend(later);
        assert_open_fails_untouched(&damaged, 22, CorruptionReason::PayloadChecksum);
    }
}

#[test]
fn a_valid_record_at_another_offset_fails_its_header_checksum() {
    let bytes = [&SET_A_HI[..], &SET_A_HI, &moved(&DELETE_A, 44)].concat();
    assert_open_fails_untouched(&bytes, 22, CorruptionReason::HeaderChecksum);
}

#[test]
fn an_empty_value_is_stored_and_read_back() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store.set(key(b"a"), value(b"")).unwrap();
    assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"")));
    drop(store);
    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"")));
    assert_eq!(log_bytes(tmp.path()).len(), 20);
}

#[test]
fn the_side_file_of_a_torn_tail_is_named_for_its_offset_and_checksum() {
    let tmp = store_with_log(&set_a_hi_then(&[0xab; 20]));
    Store::open(tmp.path()).unwrap();
    assert_eq!(log_bytes(tmp.path()), SET_A_HI);
    // CRC32C of twenty 0xab bytes, computed in Python.
    assert_eq!(
        torn_files(tmp.path()),
        [("tally.log.torn-22-2d3e2e45".to_string(), vec![0xab; 20])]
    );
}

#[test]
fn damage_to_the_last_record_is_a_torn_tail() {
    let mut bytes = set_a_hi_then(&DELETE_A);
    bytes[22 + 19] ^= 0x01;
    let tmp = store_with_log(&bytes);
    Store::open(tmp.path()).unwrap();
    assert_eq!(log_bytes(tmp.path()), SET_A_HI);
    assert_eq!(torn_files(tmp.path())[0].1, &bytes[22..]);
}

#[test]
fn open_truncates_a_zero_filled_tail_shorter_than_a_header() {
    let tmp = store_with_log(&set_a_hi_then(&[0u8; 5]));
    Store::open(tmp.path()).unwrap();
    assert_eq!(log_bytes(tmp.path()), SET_A_HI);
}

#[test]
fn open_truncates_a_zero_filled_tail() {
    let tmp = store_with_log(&set_a_hi_then(&[0u8; 4096]));
    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
    assert_eq!(log_bytes(tmp.path()), SET_A_HI);
}

#[test]
fn open_truncates_a_log_that_is_all_zeros() {
    let tmp = store_with_log(&[0u8; 4096]);
    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(store.get(&key(b"a")).unwrap(), None);
    assert_eq!(log_bytes(tmp.path()), b"");
    assert_eq!(torn_files(tmp.path()), [], "zeros are not saved");
}

#[test]
fn open_truncates_a_record_whose_payload_is_zeros() {
    let tmp = store_with_log(&set_a_hi_then(&delete_a_with_a_zero_payload()));
    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
    assert_eq!(log_bytes(tmp.path()), SET_A_HI);
}

#[test]
fn writes_after_recovery_land_where_the_tail_was_cut() {
    let tmp = store_with_log(&set_a_hi_then(&DELETE_A[..14]));
    let store = Store::open(tmp.path()).unwrap();
    store.delete(key(b"a")).unwrap();
    drop(store);
    assert_eq!(log_bytes(tmp.path()), set_a_hi_then(&DELETE_A));
    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(store.get(&key(b"a")).unwrap(), None);
}

#[test]
fn recovery_is_idempotent() {
    let tmp = store_with_log(&set_a_hi_then(&[0u8; 32]));
    drop(Store::open(tmp.path()).unwrap());
    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
    assert_eq!(log_bytes(tmp.path()), SET_A_HI);
}

// recovery: damage before a later record fails open and leaves the log
// alone

fn assert_open_fails_untouched(bytes: &[u8], offset: u64, reason: CorruptionReason) {
    let tmp = store_with_log(bytes);
    let err = Store::open(tmp.path()).err().expect("open must fail");
    assert!(
        matches!(err, Error::Corruption { offset: o, reason: r } if o == offset && r == reason),
        "expected corruption at {offset} ({reason}), got {err:?}"
    );
    assert_eq!(log_bytes(tmp.path()), bytes);
}

#[test]
fn open_fails_on_a_zero_filled_region_before_a_valid_record() {
    let bytes = [SET_A_HI.as_slice(), &[0u8; 20], &moved(&DELETE_A, 42)].concat();
    assert_open_fails_untouched(&bytes, 22, CorruptionReason::HeaderChecksum);
}

#[test]
fn open_fails_on_a_zero_payload_before_a_valid_record() {
    let bytes = [
        SET_A_HI.as_slice(),
        &delete_a_with_a_zero_payload(),
        &moved(&DELETE_A, 42),
    ]
    .concat();
    assert_open_fails_untouched(&bytes, 22, CorruptionReason::PayloadChecksum);
}

/// SET_A_HI, DELETE_A, then a Set of "b" at 42.
fn three_records() -> Vec<u8> {
    let mut bytes = set_a_hi_then(&DELETE_A);
    bytes.extend(encoded_at(42, &[set_record(b"b", b"2")]));
    bytes
}

#[test]
fn open_fails_on_a_corrupted_header_checksum_with_its_offset() {
    let mut bytes = three_records();
    bytes[22] ^= 0x01;
    assert_open_fails_untouched(&bytes, 22, CorruptionReason::HeaderChecksum);
}

#[test]
fn open_fails_on_a_corrupted_length_with_its_offset() {
    let mut bytes = three_records();
    bytes[22 + 7] ^= 0x01; // second record's value_len: 0 -> 1
    assert_open_fails_untouched(&bytes, 22, CorruptionReason::HeaderChecksum);
}

#[test]
fn open_fails_on_a_corrupted_key_with_its_offset() {
    let mut bytes = three_records();
    bytes[22 + 19] ^= 0x01; // second record's key: "a" -> "`"
    assert_open_fails_untouched(&bytes, 22, CorruptionReason::PayloadChecksum);
}

#[test]
fn open_fails_on_an_unsupported_tail_record_and_leaves_it() {
    let mut bytes = set_a_hi_then(&DELETE_A);
    bytes[22 + 4] = 0x19; // second record's type: unknown
    seal(&mut bytes[22..22 + HEADER_LEN], 22);
    let tmp = store_with_log(&bytes);
    assert!(matches!(
        Store::open(tmp.path()),
        Err(Error::Unsupported {
            offset: 22,
            reason: UnsupportedReason::UnknownType(0x19)
        })
    ));
    assert_eq!(log_bytes(tmp.path()), bytes);
}

// repair

fn side_file(dir: &Path, offset: u64) -> PathBuf {
    dir.join(format!("{LOG_FILE_NAME}.corrupt-{offset}"))
}

/// SET_A_HI, then a Delete of "a" with its key corrupted, then a Set of
/// "b": the bad record at 22 hides the valid one after it.
fn log_with_a_corrupt_middle() -> Vec<u8> {
    let mut bytes = three_records();
    bytes[22 + HEADER_LEN] ^= 0x01;
    bytes
}

#[test]
fn repair_of_an_intact_log_does_nothing() {
    let bytes = set_a_hi_then(&DELETE_A);
    let tmp = store_with_log(&bytes);
    assert_eq!(Store::repair(tmp.path()).unwrap(), None);
    assert_eq!(log_bytes(tmp.path()), bytes);
    assert_eq!(
        file_names(tmp.path()),
        ["tally.00000000000000000000.log", "tally.lock"]
    );
}

#[test]
fn repair_leaves_a_torn_tail_for_open() {
    let bytes = set_a_hi_then(&DELETE_A[..14]);
    let tmp = store_with_log(&bytes);
    assert_eq!(Store::repair(tmp.path()).unwrap(), None);
    assert_eq!(log_bytes(tmp.path()), bytes);
}

#[test]
fn repair_moves_the_corrupt_record_and_everything_after_it_to_a_side_file() {
    let bytes = log_with_a_corrupt_middle();
    let tmp = store_with_log(&bytes);
    assert_eq!(
        Store::repair(tmp.path()).unwrap(),
        Some(Repaired {
            offset: 22,
            dropped: 41,
            side_file: side_file(tmp.path(), 22),
        })
    );
    assert_eq!(log_bytes(tmp.path()), SET_A_HI);
    assert_eq!(fs::read(side_file(tmp.path(), 22)).unwrap(), &bytes[22..]);
    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
    assert_eq!(store.get(&key(b"b")).unwrap(), None);
}

#[test]
fn repair_is_idempotent() {
    let bytes = log_with_a_corrupt_middle();
    let tmp = store_with_log(&bytes);
    Store::repair(tmp.path()).unwrap();
    assert_eq!(Store::repair(tmp.path()).unwrap(), None);
    assert_eq!(log_bytes(tmp.path()), SET_A_HI);
    assert_eq!(fs::read(side_file(tmp.path(), 22)).unwrap(), &bytes[22..]);
}

#[test]
fn repair_rewrites_a_side_file_a_crashed_repair_left() {
    let bytes = log_with_a_corrupt_middle();
    let zeroed_middle = [&bytes[22..24], &[0; 5], &bytes[29..34]].concat();
    for left in [
        &bytes[22..22],
        &bytes[22..29],
        &bytes[22..],
        &[0; 41],
        &zeroed_middle,
    ] {
        let tmp = store_with_log(&bytes);
        fs::write(side_file(tmp.path(), 22), left).unwrap();
        assert_eq!(
            Store::repair(tmp.path()).unwrap(),
            Some(Repaired {
                offset: 22,
                dropped: 41,
                side_file: side_file(tmp.path(), 22),
            }),
            "side file left with {} bytes",
            left.len()
        );
        assert_eq!(log_bytes(tmp.path()), SET_A_HI);
        assert_eq!(fs::read(side_file(tmp.path(), 22)).unwrap(), &bytes[22..]);
    }
}

#[test]
fn repair_refuses_to_overwrite_bytes_an_earlier_repair_saved() {
    let bytes = log_with_a_corrupt_middle();
    let longer = [&bytes[22..], b"!"].concat();
    let changed_first_byte = [&[bytes[22] ^ 1][..], &bytes[23..]].concat();
    for earlier in [
        &b"saved by an earlier repair"[..],
        &longer,
        &changed_first_byte,
    ] {
        let tmp = store_with_log(&bytes);
        let path = side_file(tmp.path(), 22);
        fs::write(&path, earlier).unwrap();
        assert!(matches!(
            Store::repair(tmp.path()),
            Err(Error::SideFileExists(p)) if p == path
        ));
        assert_eq!(log_bytes(tmp.path()), bytes);
        assert_eq!(fs::read(&path).unwrap(), earlier);
    }
}

#[test]
fn repair_refuses_an_unsupported_record() {
    let mut bytes = set_a_hi_then(&DELETE_A);
    bytes[22 + 4] = 0x19;
    seal(&mut bytes[22..22 + HEADER_LEN], 22);
    let tmp = store_with_log(&bytes);
    assert!(matches!(
        Store::repair(tmp.path()),
        Err(Error::Unsupported { offset: 22, .. })
    ));
    assert_eq!(log_bytes(tmp.path()), bytes);
}

#[test]
fn repair_is_locked_out_while_the_store_is_open() {
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::open(tmp.path()).unwrap();
    assert!(matches!(Store::repair(tmp.path()), Err(Error::Locked)));
}

// set and get

#[test]
fn get_a_missing_key_returns_none() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(store.get(&key(b"a")).unwrap(), None);
}

#[test]
fn set_then_get() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store.set(key(b"a"), value(b"hi")).unwrap();
    assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
}

#[test]
fn set_writes_the_record_format() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store.set(key(b"a"), value(b"hi")).unwrap();
    assert_eq!(log_bytes(tmp.path()), SET_A_HI);
}

#[test]
fn set_overwrites_and_appends() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store.set(key(b"a"), value(b"hi")).unwrap();
    store.set(key(b"a"), value(b"yo")).unwrap();
    assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"yo")));
    assert_eq!(log_bytes(tmp.path()).len(), 44);
}

#[test]
fn keys_are_independent() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store.set(key(b"a"), value(b"1")).unwrap();
    store.set(key(b"b"), value(b"2")).unwrap();
    assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"1")));
    assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
}

// delete

#[test]
fn delete_removes_the_key_and_appends_a_delete_record() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store.set(key(b"a"), value(b"hi")).unwrap();
    store.delete(key(b"a")).unwrap();
    assert_eq!(store.get(&key(b"a")).unwrap(), None);
    assert_eq!(
        log_bytes(tmp.path()),
        [SET_A_HI.as_slice(), DELETE_A.as_slice()].concat()
    );
}

#[test]
fn delete_of_a_missing_key_writes_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store.delete(key(b"a")).unwrap();
    assert_eq!(log_bytes(tmp.path()), b"");
}

#[test]
fn delete_of_an_already_deleted_key_writes_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store.set(key(b"a"), value(b"hi")).unwrap();
    store.delete(key(b"a")).unwrap();
    store.delete(key(b"a")).unwrap();
    assert_eq!(log_bytes(tmp.path()).len(), 42);
}

// reopen

#[test]
fn reopen_rebuilds_the_index() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.set(key(b"b"), value(b"1")).unwrap();
        store.set(key(b"a"), value(b"yo")).unwrap();
        store.delete(key(b"b")).unwrap();
        store.set(key(b"c"), value(b"3")).unwrap();
        store.delete(key(b"c")).unwrap();
        store.set(key(b"c"), value(b"again")).unwrap();
    }
    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"yo")));
    assert_eq!(store.get(&key(b"b")).unwrap(), None);
    assert_eq!(store.get(&key(b"c")).unwrap(), Some(value(b"again")));
}

#[test]
fn writes_after_reopen_append_to_the_end() {
    let tmp = tempfile::tempdir().unwrap();
    drop({
        let store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store
    });
    let store = Store::open(tmp.path()).unwrap();
    store.delete(key(b"a")).unwrap();
    assert_eq!(
        log_bytes(tmp.path()),
        [SET_A_HI.as_slice(), DELETE_A.as_slice()].concat()
    );
    drop(store);
    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(store.get(&key(b"a")).unwrap(), None);
}

// get verifies what it reads

#[test]
fn get_detects_a_corrupted_value_with_the_record_offset() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store.set(key(b"a"), value(b"hi")).unwrap();
    store.set(key(b"b"), value(b"hi")).unwrap();
    write_log_at(tmp.path(), 22 + 21, b"x"); // b's value: "hi" -> "hx"
    assert!(matches!(
        store.get(&key(b"b")),
        Err(Error::Corruption {
            offset: 22,
            reason: CorruptionReason::PayloadChecksum
        })
    ));
    assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
}

#[test]
fn get_detects_a_corrupted_header() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store.set(key(b"a"), value(b"hi")).unwrap();
    write_log_at(tmp.path(), 0, &[0x00]);
    assert!(matches!(
        store.get(&key(b"a")),
        Err(Error::Corruption {
            offset: 0,
            reason: CorruptionReason::HeaderChecksum
        })
    ));
}

#[test]
fn get_detects_a_valid_record_for_a_different_key() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store.set(key(b"a"), value(b"hi")).unwrap();
    write_log_at(tmp.path(), 0, &encoded(&[set_record(b"b", b"hi")]));
    assert!(matches!(
        store.get(&key(b"a")),
        Err(Error::Corruption {
            offset: 0,
            reason: CorruptionReason::KeyMismatch
        })
    ));
}

#[test]
fn get_detects_a_delete_record_where_the_index_expects_a_set() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store.set(key(b"a"), value(b"hi")).unwrap();
    write_log_at(tmp.path(), 0, &moved(&DELETE_A, 0));
    assert!(matches!(
        store.get(&key(b"a")),
        Err(Error::Corruption {
            offset: 0,
            reason: CorruptionReason::NotASet
        })
    ));
}

fn file_names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

fn set_record(k: &[u8], v: &[u8]) -> Op {
    Op::Set {
        key: key(k),
        value: value(v),
    }
}

#[test]
fn compact_keeps_only_the_latest_value_of_each_live_key() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store.set(key(b"c"), value(b"3")).unwrap();
    store.set(key(b"a"), value(b"1")).unwrap();
    store.set(key(b"b"), value(b"2")).unwrap();
    store.set(key(b"a"), value(b"hi")).unwrap();
    store.delete(key(b"b")).unwrap();
    assert_eq!(log_bytes(tmp.path()).len(), 105);

    store.compact().unwrap();

    assert_eq!(segment_bases(tmp.path()), [105]);
    assert_eq!(
        log_bytes(tmp.path()),
        encoded_at(105, &[set_record(b"c", b"3"), set_record(b"a", b"hi")])
    );
    assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
    assert_eq!(store.get(&key(b"b")).unwrap(), None);
    assert_eq!(store.get(&key(b"c")).unwrap(), Some(value(b"3")));
    drop(store);
    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
    assert_eq!(store.get(&key(b"b")).unwrap(), None);
    assert_eq!(store.get(&key(b"c")).unwrap(), Some(value(b"3")));
}

#[test]
fn writes_after_compact_land_in_the_new_log_and_survive_a_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store.set(key(b"a"), value(b"old")).unwrap();
    store.set(key(b"a"), value(b"hi")).unwrap();
    store.compact().unwrap();
    store.set(key(b"b"), value(b"2")).unwrap();
    store.delete(key(b"a")).unwrap();
    drop(store);

    assert_eq!(
        log_bytes(tmp.path()),
        encoded_at(
            45,
            &[
                set_record(b"a", b"hi"),
                set_record(b"b", b"2"),
                delete_record(b"a"),
            ]
        )
    );
    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(store.get(&key(b"a")).unwrap(), None);
    assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
}

#[test]
fn compact_of_an_empty_store_leaves_an_empty_log() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store.set(key(b"a"), value(b"hi")).unwrap();
    store.delete(key(b"a")).unwrap();
    store.compact().unwrap();
    assert_eq!(log_bytes(tmp.path()), b"");
    store.set(key(b"a"), value(b"hi")).unwrap();
    assert_eq!(
        log_bytes(tmp.path()),
        encoded_at(42, &[set_record(b"a", b"hi")])
    );
}

#[test]
fn compact_twice_copies_the_same_records_to_the_end() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store.set(key(b"a"), value(b"1")).unwrap();
    store.set(key(b"a"), value(b"hi")).unwrap();
    store.compact().unwrap();
    assert_eq!(
        log_bytes(tmp.path()),
        encoded_at(43, &[set_record(b"a", b"hi")])
    );
    store.compact().unwrap();
    assert_eq!(segment_bases(tmp.path()), [65]);
    assert_eq!(
        log_bytes(tmp.path()),
        encoded_at(65, &[set_record(b"a", b"hi")])
    );
}

#[test]
fn compact_leaves_the_log_its_hint_and_the_lock_file() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store.set(key(b"a"), value(b"hi")).unwrap();
    store.compact().unwrap();
    assert_eq!(
        file_names(tmp.path()),
        [
            "tally.00000000000000000022.hint",
            "tally.00000000000000000022.log",
            "tally.lock",
        ]
    );
}

#[test]
fn keys_sharing_a_hash_read_as_corruption_never_as_each_others_value() {
    let tmp = tempfile::tempdir().unwrap();
    let mut store = Store::open(tmp.path()).unwrap();
    store.index = Index::weak();
    store.set(key(b"a"), value(b"1")).unwrap();
    store.set(key(b"b"), value(b"2")).unwrap();

    assert!(matches!(
        store.get(&key(b"a")),
        Err(Error::Corruption {
            offset: 21,
            reason: CorruptionReason::KeyMismatch
        })
    ));
    assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
    assert_eq!(store.get(&key(b"cc")).unwrap(), None);
}

fn edit(store: &Store, key: &[u8], change: impl FnOnce(&mut Entry)) {
    let mut entry = store.index.get(key).unwrap();
    change(&mut entry);
    store.index.insert(key, entry);
}

#[test]
fn get_reads_the_record_even_when_the_index_has_its_lengths_wrong() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store.set(key(b"a"), value(b"1")).unwrap();
    edit(&store, b"a", |entry| entry.value_len = 5);
    assert_eq!(
        store.get(&key(b"a")).unwrap(),
        Some(value(b"1")),
        "past the end"
    );

    store.set(key(b"b"), value(b"2")).unwrap();
    edit(&store, b"a", |entry| entry.value_len = 2);
    assert_eq!(
        store.get(&key(b"a")).unwrap(),
        Some(value(b"1")),
        "header disagrees"
    );
}

#[test]
fn verify_passes_a_healthy_store_before_and_after_compaction() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store.set(key(b"a"), value(b"1")).unwrap();
    store.set(key(b"b"), value(b"2")).unwrap();
    store.set(key(b"a"), value(b"hi")).unwrap();
    store.delete(key(b"b")).unwrap();
    store.verify().unwrap();
    store.compact().unwrap();
    store.set(key(b"c"), value(b"3")).unwrap();
    store.verify().unwrap();
    drop(store);
    Store::open(tmp.path()).unwrap().verify().unwrap();
}

#[test]
fn verify_finds_damage_that_open_skipped_because_of_the_hint() {
    let tmp = tempfile::tempdir().unwrap();
    compacted_a_and_b(tmp.path());
    write_log_at(tmp.path(), 20, b"X");
    let store = Store::open(tmp.path()).unwrap();
    assert!(matches!(
        store.verify(),
        Err(Error::Corruption {
            offset: 43,
            reason: CorruptionReason::PayloadChecksum
        })
    ));
}

#[test]
fn verify_finds_an_index_that_disagrees_with_the_log() {
    let tmp = tempfile::tempdir().unwrap();
    compacted_a_and_b(tmp.path());
    let store = Store::open(tmp.path()).unwrap();
    store.index.remove(b"b");
    assert!(matches!(
        store.verify(),
        Err(Error::Corruption {
            offset: 65,
            reason: CorruptionReason::IndexMismatch
        })
    ));
    edit(&store, b"a", |entry| entry.offset = 65);
    assert!(matches!(
        store.verify(),
        Err(Error::Corruption {
            offset: 43,
            reason: CorruptionReason::IndexMismatch
        })
    ));
}

#[test]
fn verify_finds_a_log_cut_short_behind_the_stores_back() {
    let tmp = tempfile::tempdir().unwrap();
    compacted_a_and_b(tmp.path());
    let store = Store::open(tmp.path()).unwrap();
    OpenOptions::new()
        .write(true)
        .open(log_path(tmp.path()))
        .unwrap()
        .set_len(FILE_HEADER_LEN + 22)
        .unwrap();
    assert!(matches!(
        store.verify(),
        Err(Error::Corruption {
            offset: 65,
            reason: CorruptionReason::IndexMismatch
        })
    ));
}

// segments

fn segmented(segment_bytes: u64) -> Options {
    Options {
        auto_compact: None,
        segment_bytes,
    }
}

fn put(store: &Store, k: &[u8], v: &[u8]) {
    store.set(key(k), value(v)).unwrap();
}

fn read(store: &Store, k: &[u8]) -> Option<Vec<u8>> {
    store.get(&key(k)).unwrap().map(|v| v.as_bytes().to_vec())
}

#[test]
fn writes_go_to_a_new_segment_once_one_is_full() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open_with(tmp.path(), segmented(24)).unwrap();
    put(&store, b"a", b"hi");
    put(&store, b"b", b"2");
    assert_eq!(segment_bases(tmp.path()), [0], "22 bytes is not full yet");
    put(&store, b"c", b"3");
    assert_eq!(segment_bases(tmp.path()), [0, 43], "43 bytes is full");
    store
        .write(batch(&[(b"d", Some(b"4")), (b"a", None)]))
        .unwrap();
    assert_eq!(segment_bases(tmp.path()), [0, 43]);
    assert_eq!(
        segment_records(tmp.path(), 43),
        [
            encoded_at(43, &[set_record(b"c", b"3")]),
            batched_at(64, &[set_record(b"d", b"4"), delete_record(b"a")]),
        ]
        .concat(),
        "21 bytes is not full: the whole batch goes after c=3"
    );
    assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
    assert_eq!(read(&store, b"c").as_deref(), Some(&b"3"[..]));
    drop(store);

    let store = Store::open_with(tmp.path(), segmented(24)).unwrap();
    assert_eq!(read(&store, b"a"), None);
    assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
    assert_eq!(read(&store, b"c").as_deref(), Some(&b"3"[..]));
    assert_eq!(read(&store, b"d").as_deref(), Some(&b"4"[..]));
    assert_eq!(store.dead(), 22 + 27 + 20);
    store.verify().unwrap();
}

/// One record per segment: a=1 at 0, b=2 at 21, a deleted at 42, c=3
/// at 62.
fn one_record_per_segment(dir: &Path) -> Store {
    let store = Store::open_with(dir, segmented(1)).unwrap();
    put(&store, b"a", b"1");
    put(&store, b"b", b"2");
    store.delete(key(b"a")).unwrap();
    put(&store, b"c", b"3");
    assert_eq!(segment_bases(dir), [0, 21, 42, 62]);
    store
}

#[test]
fn cleaning_a_delete_while_an_older_segment_has_its_key_carries_it() {
    let tmp = tempfile::tempdir().unwrap();
    let store = one_record_per_segment(tmp.path());
    store.clean(&store.writer(), &[42]).unwrap();
    assert_eq!(segment_bases(tmp.path()), [0, 21, 62, 83]);
    assert_eq!(
        segment_records(tmp.path(), 83),
        encoded_at(83, &[delete_record(b"a")])
    );
    assert_eq!(
        store.commits.load(Ordering::Relaxed),
        4,
        "the Delete is in the copy, not an append of its own"
    );
    assert_eq!(store.dead(), 21 + 20, "a=1 at 0 and the Delete");
    drop(store);

    let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
    assert_eq!(read(&store, b"a"), None, "a=1 at 0 stays hidden");
    assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
    store.verify().unwrap();
}

#[test]
fn a_copy_of_carried_deletes_is_not_cleaned_again_while_they_are_needed() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
    let a = |i: u8| [b'a', b'0' + i];
    let mut sets = Batch::new();
    let mut deletes = Batch::new();
    for i in 0..10 {
        sets.set(key(&a(i)), value(b"1"));
        deletes.delete(key(&a(i)));
    }
    sets.set(key(b"k"), value(&[b'v'; 1000]));
    store.write(sets).unwrap();
    store.write(deletes).unwrap();
    put(&store, b"c", b"3");
    assert_eq!(segment_bases(tmp.path()), [0, 1267, 1504]);
    store.clean(&store.writer(), &[1267]).unwrap();
    assert_eq!(segment_bases(tmp.path()), [0, 1504, 1525]);
    drop(store);

    // 457 of 1,498 bytes are dead, 210 of them the copied Deletes.
    let options = Options {
        auto_compact: AutoCompact::new(25, 0),
        segment_bytes: 1,
    };
    let store = Store::open_with(tmp.path(), options).unwrap();
    put(&store, b"d", b"4");
    put(&store, b"e", b"5");
    assert_eq!(
        segment_bases(tmp.path()),
        [0, 1504, 1735, 1945, 1966],
        "open copies the Deletes once, and the writes after it copy nothing"
    );
    for i in 0..10 {
        assert_eq!(read(&store, &a(i)), None);
    }
    assert_eq!(read(&store, b"k").as_deref(), Some(&[b'v'; 1000][..]));

    put(&store, b"k", b"new");
    assert_eq!(
        segment_bases(tmp.path()),
        [1504, 1735, 1945, 1966, 1987, 2010],
        "a=1 at 0 went with the rest of its segment"
    );
    let reclaimable: u64 = store.segments().iter().map(|s| s.reclaimable()).sum();
    assert_eq!(
        (reclaimable, store.dead()),
        (210, 210),
        "the Deletes hide nothing now"
    );
}

#[test]
fn a_copy_of_live_sets_and_a_carried_delete_reopens_from_its_hint() {
    let tmp = tempfile::tempdir().unwrap();
    let store = one_record_per_segment(tmp.path());
    store.clean(&store.writer(), &[21, 42]).unwrap();
    assert_eq!(segment_bases(tmp.path()), [0, 62, 83]);
    assert_eq!(
        segment_records(tmp.path(), 83),
        encoded_at(83, &[set_record(b"b", b"2"), delete_record(b"a")])
    );
    assert_eq!(store.dead(), 21 + 20);
    drop(store);

    let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
    assert!(tmp.path().join(hint_name(83)).exists());
    assert_eq!(read(&store, b"a"), None);
    assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
    assert_eq!(store.dead(), 21 + 20);
    store.verify().unwrap();
}

#[test]
fn cleaning_a_delete_carries_it_when_the_older_set_is_only_in_a_hint() {
    let tmp = tempfile::tempdir().unwrap();
    drop(one_record_per_segment(tmp.path()));
    let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
    assert!(tmp.path().join(hint_name(0)).exists());

    store.clean(&store.writer(), &[42]).unwrap();

    assert_eq!(
        segment_records(tmp.path(), 83),
        encoded_at(83, &[delete_record(b"a")])
    );
    drop(store);
    let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
    assert_eq!(read(&store, b"a"), None);
}

#[test]
fn cleaning_a_delete_while_older_kept_segments_lack_its_key_drops_it() {
    let tmp = tempfile::tempdir().unwrap();
    let store = one_record_per_segment(tmp.path());
    store.clean(&store.writer(), &[0, 42]).unwrap();
    assert_eq!(segment_bases(tmp.path()), [21, 62, 83]);
    assert_eq!(
        segment_records(tmp.path(), 83),
        b"",
        "b=2 at 21 is kept but hides nothing of a"
    );
    drop(store);

    let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
    assert_eq!(read(&store, b"a"), None);
    assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
    assert_eq!(store.dead(), 0);
    store.verify().unwrap();
}

#[test]
fn cleaning_a_delete_with_every_older_segment_drops_it() {
    let tmp = tempfile::tempdir().unwrap();
    let store = one_record_per_segment(tmp.path());
    store.clean(&store.writer(), &[0, 21, 42]).unwrap();
    assert_eq!(segment_bases(tmp.path()), [62, 83]);
    assert_eq!(
        segment_records(tmp.path(), 83),
        encoded_at(83, &[set_record(b"b", b"2")]),
        "only b=2 is copied; the Delete has nothing left to hide"
    );
    assert!(tmp.path().join(hint_name(83)).exists());
    drop(store);

    let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
    assert_eq!(read(&store, b"a"), None);
    assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
    assert_eq!(read(&store, b"c").as_deref(), Some(&b"3"[..]));
    assert_eq!(store.dead(), 0);
    store.verify().unwrap();
}

#[test]
fn get_reads_each_key_from_its_own_segment() {
    let tmp = tempfile::tempdir().unwrap();
    let store = one_record_per_segment(tmp.path());
    assert_eq!(store.index.get(b"b").unwrap().offset, 21);
    assert_eq!(store.index.get(b"c").unwrap().offset, 62);
    write_log_at(tmp.path(), 19, b"X");
    assert!(matches!(
        store.get(&key(b"c")),
        Err(Error::Corruption {
            offset: 62,
            reason: CorruptionReason::PayloadChecksum
        })
    ));
    assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
}

fn size(sizes: &[(u64, u64, u64)]) -> Vec<SegmentSize> {
    sizes
        .iter()
        .map(|&(base, len, dead)| SegmentSize { base, len, dead })
        .collect()
}

#[test]
fn automatic_compaction_cleans_the_deadest_segments_until_it_is_not_due() {
    let auto = AutoCompact::new(50, 0).unwrap();
    // 315 dead of 500. Cleaning 100 (90% dead) leaves 225 of 410,
    // still due; then 200 (60%) leaves 105 of 290, not due.
    let segments = size(&[
        (0, 100, 55),
        (100, 100, 90),
        (200, 200, 120),
        (400, 100, 50),
        (500, 0, 0),
    ]);
    assert_eq!(deadest(&segments, auto), [100, 200]);
    assert_eq!(
        deadest(&size(&[(0, 100, 49), (100, 0, 0)]), auto),
        [] as [u64; 0]
    );
    assert_eq!(deadest(&size(&[(0, 100, 50), (100, 0, 0)]), auto), [0]);
}

#[test]
fn automatic_compaction_leaves_segments_without_dead_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let options = Options {
        auto_compact: AutoCompact::new(50, 0),
        segment_bytes: 1,
    };
    let store = Store::open_with(tmp.path(), options).unwrap();
    put(&store, b"a", b"1");
    put(&store, b"b", b"2");
    put(&store, b"b", b"3");
    // b=3 at 42 made b=2 at 21 dead: 21 of 63, not due. Deleting b
    // makes 21 + 21 + 20 of 83 dead. Cleaning the segment at 21 leaves
    // 41 of 62, still due; then 42 leaves 20 of 41, not due. Neither has
    // a live record, and a=1 at 0 is never read.
    store.delete(key(b"b")).unwrap();
    assert_eq!(segment_bases(tmp.path()), [0, 63, 83]);
    assert_eq!(segment_records(tmp.path(), 83), b"");
    assert_eq!(store.dead(), 20);
    drop(store);
    let store = Store::open_with(tmp.path(), options).unwrap();
    assert_eq!(read(&store, b"a").as_deref(), Some(&b"1"[..]));
    assert_eq!(read(&store, b"b"), None);
}

#[test]
fn a_sealed_segment_cut_short_is_corruption_that_repair_moves() {
    let tmp = tempfile::tempdir().unwrap();
    drop(one_record_per_segment(tmp.path()));
    let second = tmp.path().join(log_name(21));
    let cut = FILE_HEADER_LEN as usize + 10;
    fs::write(&second, &fs::read(&second).unwrap()[..cut]).unwrap();
    assert!(matches!(
        Store::open_with(tmp.path(), segmented(1)),
        Err(Error::Corruption {
            offset: 21,
            reason: CorruptionReason::CutShort
        })
    ));

    let repaired = Store::repair(tmp.path()).unwrap().unwrap();
    assert_eq!((repaired.offset, repaired.dropped), (21, 10));
    let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
    assert_eq!(read(&store, b"b"), None);
    assert_eq!(read(&store, b"c").as_deref(), Some(&b"3"[..]));
    assert_eq!(read(&store, b"a"), None);
}

#[test]
fn segments_that_overlap_are_refused() {
    let tmp = tempfile::tempdir().unwrap();
    drop(one_record_per_segment(tmp.path()));
    write_segment(tmp.path(), 30, &encoded_at(30, &[set_record(b"x", b"1")]));
    assert!(matches!(
        Store::open_with(tmp.path(), segmented(1)),
        Err(Error::Corruption {
            offset: 30,
            reason: CorruptionReason::SegmentsOverlap
        })
    ));
}

#[test]
fn a_hint_whose_segment_is_gone_is_removed() {
    let tmp = tempfile::tempdir().unwrap();
    compacted_a_and_b(tmp.path());
    let orphan = tmp.path().join(hint_name(1000));
    fs::copy(hint_path(tmp.path()), &orphan).unwrap();
    Store::open(tmp.path()).unwrap();
    assert!(!orphan.exists());
}

#[test]
fn a_hint_that_fails_after_filling_the_index_leaves_nothing_behind() {
    let tmp = tempfile::tempdir().unwrap();
    compacted_a_and_b(tmp.path());
    // The hint's one entry for "a" now names "c": its lengths still add
    // up, so "c" goes into the index before the checksum fails.
    let mut hint = fs::read(hint_path(tmp.path())).unwrap();
    assert_eq!(hint[12], b'a');
    hint[12] = b'c';
    fs::write(hint_path(tmp.path()), hint).unwrap();

    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(read(&store, b"c"), None);
    assert_eq!(read(&store, b"a").as_deref(), Some(&b"hi"[..]));
    store.verify().unwrap();
}

// other versions and segment headers

#[test]
fn a_directory_from_before_segments_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    compacted_a_and_b(tmp.path());
    fs::write(tmp.path().join(LOG_FILE_NAME), SET_A_HI).unwrap();
    assert!(matches!(
        Store::open(tmp.path()),
        Err(Error::Unsupported {
            offset: 0,
            reason: UnsupportedReason::OtherFormat
        })
    ));
    assert_eq!(fs::read(tmp.path().join(LOG_FILE_NAME)).unwrap(), SET_A_HI);
}

#[test]
fn a_segment_without_this_versions_header_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    fs::write(tmp.path().join(log_name(0)), SET_A_HI).unwrap();
    assert!(matches!(
        Store::open(tmp.path()),
        Err(Error::Unsupported {
            offset: 0,
            reason: UnsupportedReason::OtherFormat
        })
    ));
    assert_eq!(fs::read(tmp.path().join(log_name(0))).unwrap(), SET_A_HI);
}

#[test]
fn a_damaged_segment_header_is_corruption_that_repair_moves_aside() {
    let tmp = tempfile::tempdir().unwrap();
    let mut bytes = segment_file(0, &SET_A_HI);
    bytes[14] ^= 0x01;
    fs::write(tmp.path().join(log_name(0)), &bytes).unwrap();
    assert!(matches!(
        Store::open(tmp.path()),
        Err(Error::Corruption {
            offset: 0,
            reason: CorruptionReason::SegmentHeader
        })
    ));

    let side_file = tmp.path().join("tally.log.corrupt-0");
    assert_eq!(
        Store::repair(tmp.path()).unwrap(),
        Some(Repaired {
            offset: 0,
            dropped: 17 + 22,
            side_file: side_file.clone(),
        })
    );
    assert_eq!(fs::read(&side_file).unwrap(), bytes);
    assert_eq!(
        fs::read(tmp.path().join(log_name(0))).unwrap(),
        segment_file(0, b"")
    );
    assert_eq!(Store::repair(tmp.path()).unwrap(), None);
    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(read(&store, b"a"), None);
}

#[test]
fn a_newest_segment_whose_header_a_crash_cut_short_is_started_again() {
    let tmp = tempfile::tempdir().unwrap();
    compacted_a_and_b(tmp.path());
    let header = segment_file(86, b"");
    for torn in [&header[..9], &[], &[0; 17], &[0xab; 17]] {
        fs::write(tmp.path().join(log_name(86)), torn).unwrap();
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(fs::read(tmp.path().join(log_name(86))).unwrap(), header);
        assert_eq!(read(&store, b"a").as_deref(), Some(&b"hi"[..]));
    }
}

#[test]
fn a_sealed_segment_whose_header_was_cut_short_is_corruption() {
    let tmp = tempfile::tempdir().unwrap();
    drop(one_record_per_segment(tmp.path()));
    fs::write(tmp.path().join(log_name(21)), &segment_file(21, b"")[..9]).unwrap();
    assert!(matches!(
        Store::open_with(tmp.path(), segmented(1)),
        Err(Error::Corruption {
            offset: 21,
            reason: CorruptionReason::CutShort
        })
    ));

    let repaired = Store::repair(tmp.path()).unwrap().unwrap();
    assert_eq!((repaired.offset, repaired.dropped), (21, 9));
    assert_eq!(segment_bases(tmp.path()), [0, 21, 42, 62]);
    let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
    assert_eq!(read(&store, b"a"), None);
    assert_eq!(read(&store, b"b"), None, "b=2 went with the damage");
    assert_eq!(read(&store, b"c").as_deref(), Some(&b"3"[..]));
    store.verify().unwrap();
}

#[test]
fn open_removes_a_copy_a_crashed_compaction_left() {
    let tmp = tempfile::tempdir().unwrap();
    let left = tmp.path().join(compact_name(0));
    fs::write(&left, [0xAB; 100]).unwrap();
    let store = Store::open(tmp.path()).unwrap();
    assert!(!left.exists());
    store.set(key(b"a"), value(b"hi")).unwrap();
    store.compact().unwrap();
    assert_eq!(
        log_bytes(tmp.path()),
        encoded_at(22, &[set_record(b"a", b"hi")])
    );
}

// batches

fn batch(ops: &[(&[u8], Option<&[u8]>)]) -> Batch {
    let mut batch = Batch::new();
    for (k, v) in ops {
        match v {
            Some(v) => batch.set(key(k), value(v)),
            None => batch.delete(key(k)),
        };
    }
    batch
}

fn delete_record(k: &[u8]) -> Op {
    Op::Delete { key: key(k) }
}

#[test]
fn write_appends_one_batch_and_applies_it_in_order() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store
        .write(batch(&[
            (b"a", Some(b"1")),
            (b"b", Some(b"2")),
            (b"a", Some(b"hi")),
            (b"b", None),
        ]))
        .unwrap();

    assert_eq!(
        log_bytes(tmp.path()),
        batched_at(
            0,
            &[
                set_record(b"a", b"1"),
                set_record(b"b", b"2"),
                set_record(b"a", b"hi"),
                delete_record(b"b"),
            ]
        )
    );
    assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
    assert_eq!(store.get(&key(b"b")).unwrap(), None);
    assert_eq!(store.dead(), 27 + 21 + 21 + 20);
    drop(store);
    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
    assert_eq!(store.get(&key(b"b")).unwrap(), None);
    assert_eq!(store.dead(), 27 + 21 + 21 + 20);
    store.verify().unwrap();
}

#[test]
fn a_batch_deletes_only_keys_live_at_that_point_in_it() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store.write(batch(&[(b"a", None), (b"b", None)])).unwrap();
    assert_eq!(
        log_bytes(tmp.path()),
        b"",
        "nothing to delete, nothing written"
    );

    store.write(Batch::new()).unwrap();
    assert_eq!(log_bytes(tmp.path()), b"");

    store
        .write(batch(&[(b"a", Some(b"1")), (b"a", None), (b"a", None)]))
        .unwrap();
    assert_eq!(
        log_bytes(tmp.path()),
        batched_at(0, &[set_record(b"a", b"1"), delete_record(b"a")])
    );
    assert_eq!(store.get(&key(b"a")).unwrap(), None);
}

/// b=2, then a batch setting a=1 and deleting b.
fn log_with_a_batch() -> (Vec<u8>, usize) {
    let before = encoded(&[set_record(b"b", b"2")]);
    let mut log = before.clone();
    log.extend(batched_at(
        21,
        &[set_record(b"a", b"1"), delete_record(b"b")],
    ));
    (log, before.len())
}

#[test]
fn a_batch_cut_anywhere_is_dropped_whole() {
    let (log, before) = log_with_a_batch();
    for cut in before..log.len() {
        let tmp = tempfile::tempdir().unwrap();
        write_segment(tmp.path(), 0, &log[..cut]);
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), None, "cut at {cut}");
        assert_eq!(
            store.get(&key(b"b")).unwrap(),
            Some(value(b"2")),
            "cut at {cut}"
        );
        assert_eq!(log_bytes(tmp.path()).len(), before, "cut at {cut}");
    }
    let tmp = tempfile::tempdir().unwrap();
    write_segment(tmp.path(), 0, &log);
    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"1")));
    assert_eq!(store.get(&key(b"b")).unwrap(), None);
}

#[test]
fn a_batch_whose_last_record_landed_as_zeros_is_dropped_whole() {
    let (mut log, before) = log_with_a_batch();
    let len = log.len();
    log[len - 20..].fill(0);
    let tmp = tempfile::tempdir().unwrap();
    write_segment(tmp.path(), 0, &log);
    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(store.get(&key(b"a")).unwrap(), None);
    assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
    assert_eq!(log_bytes(tmp.path()).len(), before);
}

#[test]
fn corruption_inside_a_batch_is_reported_and_repaired_at_the_batch() {
    let (mut log, before) = log_with_a_batch();
    log[before + 27 + 20] ^= 0x01;
    log.extend(encoded_at(89, &[set_record(b"c", b"3")]));
    let tmp = tempfile::tempdir().unwrap();
    write_segment(tmp.path(), 0, &log);
    assert!(matches!(
        Store::open(tmp.path()),
        Err(Error::Corruption {
            offset: 21,
            reason: CorruptionReason::PayloadChecksum
        })
    ));

    let repaired = Store::repair(tmp.path()).unwrap().unwrap();
    assert_eq!((repaired.offset, repaired.dropped), (21, 68 + 21));
    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(store.get(&key(b"a")).unwrap(), None);
    assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
    assert_eq!(store.get(&key(b"c")).unwrap(), None);
}

#[test]
fn a_batch_inside_a_batch_or_a_record_past_its_end_is_corrupt() {
    let mut nested = Vec::new();
    encode_batch(48, 0, &mut nested);
    encode(
        &Record::Batch { body_len: 21 },
        Place::Inner,
        27,
        &mut nested,
    );
    encode_op(&set_record(b"a", b"1"), Place::Inner, 54, &mut nested);
    assert_eq!(nested[27 + 4], 0x73);
    let mut short = Vec::new();
    encode_batch(10, 0, &mut short);
    encode_op(&set_record(b"a", b"1"), Place::Inner, 27, &mut short);
    for mut log in [nested, short] {
        log.extend(encoded_at(log.len() as u64, &[set_record(b"b", b"2")]));
        let tmp = tempfile::tempdir().unwrap();
        write_segment(tmp.path(), 0, &log);
        assert!(matches!(
            Store::open(tmp.path()),
            Err(Error::Corruption {
                offset: 0,
                reason: CorruptionReason::BadBatch
            })
        ));
    }
}

#[test]
fn compaction_after_a_batch_keeps_only_the_live_sets() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store
        .write(batch(&[
            (b"a", Some(b"1")),
            (b"b", Some(b"2")),
            (b"a", None),
        ]))
        .unwrap();
    store.compact().unwrap();
    assert_eq!(
        log_bytes(tmp.path()),
        encoded_at(89, &[set_record(b"b", b"2")])
    );
    assert_eq!(store.dead(), 0);
    drop(store);
    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
    store.verify().unwrap();
}

// hint file

/// The hint of the newest segment.
fn hint_path(dir: &Path) -> PathBuf {
    dir.join(hint_name(*segment_bases(dir).last().unwrap()))
}

/// a=hi at 0 and b=2 at 22, compacted into the segment at 43, so a is
/// at 43, b at 65, and the segment's hint covers both.
fn compacted_a_and_b(dir: &Path) {
    let store = Store::open(dir).unwrap();
    store.set(key(b"a"), value(b"hi")).unwrap();
    store.set(key(b"b"), value(b"2")).unwrap();
    store.compact().unwrap();
}

#[test]
fn a_value_damaged_in_a_sealed_segment_fails_its_checksum() {
    let tmp = tempfile::tempdir().unwrap();
    compacted_a_and_b(tmp.path());
    let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
    put(&store, b"c", b"3");
    drop(store);
    let file = OpenOptions::new()
        .write(true)
        .open(tmp.path().join(log_name(43)))
        .unwrap();
    file.write_all_at(b"X", FILE_HEADER_LEN + 20).unwrap();
    drop(file);

    let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
    assert!(matches!(
        store.get(&key(b"a")),
        Err(Error::Corruption {
            offset: 43,
            reason: CorruptionReason::PayloadChecksum
        })
    ));
    assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
}

#[test]
fn open_after_compact_reads_keys_from_the_hint_not_the_log() {
    let tmp = tempfile::tempdir().unwrap();
    compacted_a_and_b(tmp.path());
    write_log_at(tmp.path(), 20, b"X");

    let store = Store::open(tmp.path()).unwrap();
    assert!(matches!(
        store.get(&key(b"a")),
        Err(Error::Corruption {
            offset: 43,
            reason: CorruptionReason::PayloadChecksum
        })
    ));
    assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
}

#[test]
fn a_hint_read_on_many_threads_gives_the_index_one_thread_does() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open_with(tmp.path(), segmented(100_000)).unwrap();
    let k = |i: u32| key(format!("k{i}").as_bytes());
    let mut sets = Batch::new();
    for i in 0..3000u32 {
        sets.set(k(i), value(&vec![b'v'; 1 + i as usize % 7]));
    }
    store.write(sets).unwrap();
    let mut changes = Batch::new();
    for i in 0..3000u32 {
        match i % 3 {
            0 => {
                changes.delete(k(i));
            }
            1 => {
                changes.set(k(i), value(b"w"));
            }
            _ => {}
        }
    }
    store.write(changes).unwrap();
    put(&store, b"next", b"segment");
    drop(store);
    drop(Store::open_with(tmp.path(), segmented(100_000)).unwrap());

    let fs = OsFs;
    let file = fs.open_existing(&tmp.path().join(hint_name(0))).unwrap();
    let segment = Segment::new(
        0,
        fs.open_existing(&tmp.path().join(log_name(0))).unwrap(),
        segment_records(tmp.path(), 0).len() as u64,
    );
    let mut one = Index::new();
    let (len_one, _) = read_hint(
        &file,
        crate::hint::trailer(&file).unwrap(),
        &segment,
        &mut one,
        WINDOW,
        u64::MAX,
    )
    .unwrap();
    let mut many = Index::new();
    let (len_many, _) = read_hint(
        &file,
        crate::hint::trailer(&file).unwrap(),
        &segment,
        &mut many,
        100,
        0,
    )
    .unwrap();

    assert_eq!((len_one, len_many), (segment.end(), segment.end()));
    assert_eq!(many.len(), 2000);
    for i in 0..3000u32 {
        let k = format!("k{i}");
        assert_eq!(many.get(k.as_bytes()), one.get(k.as_bytes()), "{k}");
    }

    let mut damaged = std::fs::read(tmp.path().join(hint_name(0))).unwrap();
    let middle = damaged.len() / 2;
    damaged[middle] ^= 1;
    let damaged_path = tmp.path().join("damaged");
    std::fs::write(&damaged_path, damaged).unwrap();
    let damaged = fs.open_existing(&damaged_path).unwrap();
    assert!(
        read_hint(
            &damaged,
            crate::hint::trailer(&damaged).unwrap(),
            &segment,
            &mut Index::new(),
            100,
            0
        )
        .is_err()
    );
}

/// 150,000 records, three windows: sets, overwrites, deletes and
/// batches, then a torn tail. Returns the log and the offset of a Set
/// in the third window.
fn log_of_many_windows() -> (Vec<u8>, u64) {
    let k = |i: u32| key(format!("k{}", i % 40_000).as_bytes());
    let mut log = Vec::new();
    let mut set_at = 0;
    for i in 0..150_000u32 {
        let at = log.len() as u64;
        if i == 140_001 {
            set_at = at;
        }
        let records = if i % 7 == 3 {
            encoded_at(at, &[Op::Delete { key: k(i) }])
        } else if i % 1000 == 0 {
            let set = Op::Set {
                key: k(i),
                value: value(b"batched"),
            };
            batched_at(at, &[set, Op::Delete { key: k(i + 1) }])
        } else {
            let set = Op::Set {
                key: k(i),
                value: value(&i.to_le_bytes()),
            };
            encoded_at(at, &[set])
        };
        log.extend(records);
    }
    log.extend_from_slice(&SET_A_HI[..9]);
    (log, set_at)
}

#[test]
fn a_scan_on_many_threads_builds_the_index_one_thread_would() {
    let (log, _) = log_of_many_windows();
    let fs = MemFs::new();
    fs.put(Path::new("/log"), &log);
    let file = fs.open_existing(Path::new("/log")).unwrap();
    let len = log.len() as u64;

    let mut one = Index::new();
    let mut many = Index::new();
    for (index, threads) in [(&mut one, 1), (&mut many, 4)] {
        assert!(
            matches!(
                scan_on(&file, index, 0, len, threads, &mut |_| Ok(())),
                Err(Error::Corruption {
                    offset,
                    reason: CorruptionReason::CutShort
                }) if offset == len - 9
            ),
            "{threads} threads"
        );
    }
    assert_eq!(many.len(), one.len());
    assert_eq!(many.len(), 34_285);
    for i in 0..40_000u32 {
        let k = format!("k{i}");
        assert_eq!(many.get(k.as_bytes()), one.get(k.as_bytes()), "{k}");
    }
}

#[test]
fn a_scan_on_many_threads_reports_corruption_where_one_thread_would() {
    let (mut log, set_at) = log_of_many_windows();
    log[set_at as usize + HEADER_LEN] ^= 1;
    let fs = MemFs::new();
    fs.put(Path::new("/log"), &log);
    let file = fs.open_existing(Path::new("/log")).unwrap();
    for threads in [1, 4] {
        assert!(
            matches!(
                scan_on(
                    &file,
                    &mut Index::new(),
                    0,
                    log.len() as u64,
                    threads,
                    &mut |_| Ok(()),
                ),
                Err(Error::Corruption {
                    offset,
                    reason: CorruptionReason::PayloadChecksum,
                }) if offset == set_at
            ),
            "{threads} threads"
        );
    }
}

#[test]
fn records_written_after_the_hint_are_read_from_the_log() {
    let tmp = tempfile::tempdir().unwrap();
    compacted_a_and_b(tmp.path());
    let store = Store::open(tmp.path()).unwrap();
    store.set(key(b"c"), value(b"3")).unwrap();
    store.delete(key(b"a")).unwrap();
    store.set(key(b"b"), value(b"22")).unwrap();
    drop(store);

    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(store.get(&key(b"a")).unwrap(), None);
    assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"22")));
    assert_eq!(store.get(&key(b"c")).unwrap(), Some(value(b"3")));
    assert_eq!(store.dead(), 22 + 20 + 21);
}

#[test]
fn a_damaged_hint_is_removed_and_the_whole_log_read() {
    let tmp = tempfile::tempdir().unwrap();
    compacted_a_and_b(tmp.path());
    let mut hint = fs::read(hint_path(tmp.path())).unwrap();
    hint[6] ^= 0x01;
    fs::write(hint_path(tmp.path()), hint).unwrap();

    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(
        file_names(tmp.path()),
        ["tally.00000000000000000043.log", "tally.lock"]
    );
    assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
    assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
}

#[test]
fn a_hint_for_more_log_than_there_is_is_removed() {
    let tmp = tempfile::tempdir().unwrap();
    compacted_a_and_b(tmp.path());
    let log = log_bytes(tmp.path());
    write_segment(tmp.path(), 43, &log[..22]);

    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(
        file_names(tmp.path()),
        ["tally.00000000000000000043.log", "tally.lock"]
    );
    assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
    assert_eq!(store.get(&key(b"b")).unwrap(), None);
}

#[test]
fn repair_removes_the_hint_before_truncating() {
    let tmp = tempfile::tempdir().unwrap();
    compacted_a_and_b(tmp.path());
    let store = Store::open(tmp.path()).unwrap();
    store.set(key(b"d"), value(b"4")).unwrap();
    drop(store);
    write_log_at(tmp.path(), 41, b"X");

    let repaired = Store::repair(tmp.path()).unwrap().unwrap();
    assert_eq!((repaired.offset, repaired.dropped), (65, 21 + 21));
    assert!(!hint_path(tmp.path()).exists());
    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(store.get(&key(b"b")).unwrap(), None);
    store.set(key(b"c"), value(b"3")).unwrap();
    drop(store);
    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
    assert_eq!(store.get(&key(b"c")).unwrap(), Some(value(b"3")));
}

#[test]
fn a_second_compaction_replaces_the_hint() {
    let tmp = tempfile::tempdir().unwrap();
    compacted_a_and_b(tmp.path());
    let store = Store::open(tmp.path()).unwrap();
    store.delete(key(b"a")).unwrap();
    store.compact().unwrap();
    drop(store);

    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(
        log_bytes(tmp.path()),
        encoded_at(106, &[set_record(b"b", b"2")])
    );
    assert_eq!(store.get(&key(b"a")).unwrap(), None);
    assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
    assert_eq!(store.dead(), 0);
}

/// A hint's bytes, from its `entries`, Set count, log length and `crc`,
/// computed in Python.
fn hint_bytes(entries: &[&[u8]], count: u64, log_len: u64, crc: u64) -> Vec<u8> {
    let mut bytes = b"TLYH\x03".to_vec();
    for entry in entries {
        bytes.extend_from_slice(entry);
    }
    bytes.extend_from_slice(&count.to_le_bytes());
    bytes.extend_from_slice(&log_len.to_le_bytes());
    bytes.extend_from_slice(&crc.to_le_bytes());
    bytes
}

/// Flips a byte of the segment at `base`, at `at` from its first record.
fn damage(dir: &Path, base: u64, at: u64) {
    let path = dir.join(log_name(base));
    let mut bytes = fs::read(&path).unwrap();
    bytes[(FILE_HEADER_LEN + at) as usize] ^= 0x01;
    fs::write(path, bytes).unwrap();
}

#[test]
fn open_writes_a_hint_for_each_sealed_segment_it_reads() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
    store
        .write(batch(&[(b"a", Some(b"1")), (b"b", Some(b"2"))]))
        .unwrap();
    store.delete(key(b"a")).unwrap();
    put(&store, b"c", b"3");
    drop(store);
    assert_eq!(segment_bases(tmp.path()), [0, 69, 89]);

    drop(Store::open_with(tmp.path(), segmented(1)).unwrap());
    assert_eq!(
        file_names(tmp.path()),
        [
            "tally.00000000000000000000.hint",
            "tally.00000000000000000000.log",
            "tally.00000000000000000069.hint",
            "tally.00000000000000000069.log",
            "tally.00000000000000000089.log",
            "tally.lock",
        ],
        "the active segment gets none"
    );
    assert_eq!(
        fs::read(tmp.path().join(hint_name(0))).unwrap(),
        hint_bytes(
            &[
                &[0x33, 0, 0, 8, 0, 0, 0],
                &[0x71, 1, 0, 1, 0, 0, 0, b'a'],
                &[0x71, 1, 0, 1, 0, 0, 0, b'b'],
            ],
            2,
            69,
            0xe65a_6aef_8722_6c39
        )
    );
    assert_eq!(
        fs::read(tmp.path().join(hint_name(69))).unwrap(),
        hint_bytes(
            &[&[0x32, 1, 0, 0, 0, 0, 0, b'a']],
            0,
            20,
            0xc49e_1846_0308_4f35
        )
    );

    // From the hints, open no longer reads b's record, so damage to its
    // value shows up only when b is read.
    damage(tmp.path(), 0, 68);
    let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
    assert!(matches!(
        store.get(&key(b"b")),
        Err(Error::Corruption {
            offset: 48,
            reason: CorruptionReason::PayloadChecksum
        })
    ));
    assert_eq!(read(&store, b"a"), None);
    assert_eq!(read(&store, b"c").as_deref(), Some(&b"3"[..]));
}

#[test]
fn open_extends_a_hint_that_covers_only_the_start_of_a_sealed_segment() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open_with(tmp.path(), segmented(50)).unwrap();
    put(&store, b"a", b"hi");
    put(&store, b"b", b"2");
    store.compact().unwrap();
    put(&store, b"c", b"3");
    put(&store, b"d", b"4");
    drop(store);
    assert_eq!(segment_bases(tmp.path()), [43, 107]);

    drop(Store::open_with(tmp.path(), segmented(50)).unwrap());
    assert_eq!(
        fs::read(tmp.path().join(hint_name(43))).unwrap(),
        hint_bytes(
            &[
                &[0x31, 1, 0, 2, 0, 0, 0, b'a'],
                &[0x31, 1, 0, 1, 0, 0, 0, b'b'],
                &[0x31, 1, 0, 1, 0, 0, 0, b'c'],
            ],
            3,
            64,
            0x92a6_b3e1_11e1_5f58
        )
    );

    damage(tmp.path(), 43, 63);
    let store = Store::open_with(tmp.path(), segmented(50)).unwrap();
    assert!(matches!(
        store.get(&key(b"c")),
        Err(Error::Corruption {
            offset: 86,
            reason: CorruptionReason::PayloadChecksum
        })
    ));
    assert_eq!(read(&store, b"a").as_deref(), Some(&b"hi"[..]));
    assert_eq!(read(&store, b"d").as_deref(), Some(&b"4"[..]));
}

#[test]
fn a_store_opened_from_the_hints_open_wrote_matches_a_full_read() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open_with(tmp.path(), segmented(64)).unwrap();
    let mut model = HashMap::new();
    for i in 0..90u8 {
        let k = [b'k', i % 7];
        let other = [b'k', i % 5];
        match i % 4 {
            0 => {
                store.delete(key(&k)).unwrap();
                model.remove(&k);
            }
            1 => {
                store
                    .write(batch(&[(&k, Some(&[i])), (&other, None)]))
                    .unwrap();
                model.insert(k, vec![i]);
                model.remove(&other);
            }
            _ => {
                put(&store, &k, &[i]);
                model.insert(k, vec![i]);
            }
        }
    }
    drop(store);
    let segments = segment_bases(tmp.path()).len();
    assert!(segments > 20, "{segments} segments");

    for _ in 0..2 {
        let store = Store::open_with(tmp.path(), segmented(64)).unwrap();
        store.verify().unwrap();
        for i in 0..7 {
            let k = [b'k', i];
            assert_eq!(read(&store, &k), model.get(&k).cloned(), "{k:?}");
        }
    }
    let hints = file_names(tmp.path())
        .iter()
        .filter(|n| n.ends_with(".hint"))
        .count();
    assert_eq!(hints, segments - 1);
}

fn open_on(fs: &MemFs, options: Options) -> Store<MemFs> {
    Store::open_in(fs.clone(), Path::new("/db"), options).unwrap()
}

fn get_on(store: &Store<MemFs>, k: &[u8]) -> Option<Vec<u8>> {
    store.get(&key(k)).unwrap().map(|v| v.as_bytes().to_vec())
}

#[test]
fn a_hint_cut_short_by_a_full_disk_is_replaced_by_the_next_open() {
    let fs = MemFs::new();
    let store = open_on(&fs, segmented(1));
    store.set(key(b"a"), value(b"1")).unwrap();
    store.set(key(b"b"), value(b"2")).unwrap();
    drop(store);
    let hint = Path::new("/db").join(hint_name(0));

    fs.limit_free_space(Some(10));
    let store = open_on(&fs, segmented(1));
    assert_eq!(fs.read(&hint).unwrap(), b"TLYH\x03\x31\x01\x00\x01\x00");
    assert_eq!(get_on(&store, b"a").as_deref(), Some(&b"1"[..]));
    assert_eq!(get_on(&store, b"b").as_deref(), Some(&b"2"[..]));
    drop(store);

    fs.limit_free_space(None);
    let store = open_on(&fs, segmented(1));
    assert_eq!(
        fs.read(&hint).unwrap(),
        hint_bytes(
            &[&[0x31, 1, 0, 1, 0, 0, 0, b'a']],
            1,
            21,
            0x3efb_41cb_e0ab_bf21
        )
    );
    assert_eq!(get_on(&store, b"a").as_deref(), Some(&b"1"[..]));
    assert_eq!(get_on(&store, b"b").as_deref(), Some(&b"2"[..]));
}

#[test]
fn a_hint_extension_cut_short_by_a_full_disk_is_replaced_by_the_next_open() {
    let fs = MemFs::new();
    let store = open_on(&fs, segmented(50));
    store.set(key(b"a"), value(b"hi")).unwrap();
    store.set(key(b"b"), value(b"2")).unwrap();
    store.compact().unwrap();
    store.set(key(b"c"), value(b"3")).unwrap();
    store.set(key(b"d"), value(b"4")).unwrap();
    drop(store);
    let hint = Path::new("/db").join(hint_name(43));
    let compacted = fs.read(&hint).unwrap();
    assert_eq!(compacted.len(), 5 + 16 + 24);

    // Cutting the old trailer frees 24 bytes; the new entry and trailer
    // need 32.
    fs.limit_free_space(Some(0));
    let store = open_on(&fs, segmented(50));
    let cut = fs.read(&hint).unwrap();
    assert_eq!(cut.len(), 5 + 16 + 24);
    assert_eq!(cut[..21], compacted[..21]);
    assert_eq!(cut[21..29], [0x31, 1, 0, 1, 0, 0, 0, b'c']);
    assert_eq!(get_on(&store, b"c").as_deref(), Some(&b"3"[..]));
    drop(store);

    fs.limit_free_space(None);
    let store = open_on(&fs, segmented(50));
    assert_eq!(
        fs.read(&hint).unwrap(),
        hint_bytes(
            &[
                &[0x31, 1, 0, 2, 0, 0, 0, b'a'],
                &[0x31, 1, 0, 1, 0, 0, 0, b'b'],
                &[0x31, 1, 0, 1, 0, 0, 0, b'c'],
            ],
            3,
            64,
            0x92a6_b3e1_11e1_5f58
        )
    );
    for (k, v) in [(b"a", &b"hi"[..]), (b"b", b"2"), (b"c", b"3"), (b"d", b"4")] {
        assert_eq!(get_on(&store, k).as_deref(), Some(v));
    }
}

#[test]
fn open_succeeds_when_it_cannot_write_a_hint() {
    use std::os::unix::fs::PermissionsExt;

    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
    put(&store, b"a", b"1");
    put(&store, b"b", b"2");
    drop(store);

    let mode = |mode| fs::set_permissions(tmp.path(), fs::Permissions::from_mode(mode));
    mode(0o555).unwrap();
    let writable = fs::File::create(tmp.path().join("probe")).is_ok();
    let opened = Store::open_with(tmp.path(), segmented(1));
    mode(0o755).unwrap();
    if writable {
        // Run as root, which writes anyway: nothing to check.
        return;
    }
    let store = opened.unwrap();
    assert!(!tmp.path().join(hint_name(0)).exists());
    assert_eq!(read(&store, b"a").as_deref(), Some(&b"1"[..]));
    assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
}

#[test]
fn compact_stops_at_a_corrupt_dead_record_too() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store.set(key(b"b"), value(b"2")).unwrap();
    store.set(key(b"a"), value(b"1")).unwrap();
    store.set(key(b"a"), value(b"hi")).unwrap();
    write_log_at(tmp.path(), 41, b"X");
    let before = log_bytes(tmp.path());

    assert!(matches!(
        store.compact(),
        Err(Error::Corruption {
            offset: 21,
            reason: CorruptionReason::PayloadChecksum
        })
    ));
    assert_eq!(log_bytes(tmp.path()), before);
    assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
}

#[test]
fn the_lock_still_holds_after_compact_replaces_the_log() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store.set(key(b"a"), value(b"hi")).unwrap();
    store.compact().unwrap();
    assert!(matches!(Store::open(tmp.path()), Err(Error::Locked)));
    assert!(matches!(Store::repair(tmp.path()), Err(Error::Locked)));
    drop(store);
    Store::open(tmp.path()).unwrap();
}

#[test]
fn compact_stops_at_a_corrupt_live_record_and_leaves_the_log_untouched() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store.set(key(b"a"), value(b"hi")).unwrap();
    store.set(key(b"b"), value(b"2")).unwrap();
    store.delete(key(b"b")).unwrap();
    write_log_at(tmp.path(), 21, b"X");
    let before = log_bytes(tmp.path());

    assert!(matches!(
        store.compact(),
        Err(Error::Corruption {
            offset: 0,
            reason: CorruptionReason::PayloadChecksum
        })
    ));
    assert_eq!(log_bytes(tmp.path()), before);
    store.set(key(b"c"), value(b"3")).unwrap();
    assert_eq!(store.get(&key(b"c")).unwrap(), Some(value(b"3")));
}

#[test]
fn a_failed_compaction_points_every_key_back_at_the_old_log() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    store.set(key(b"a"), value(b"1")).unwrap();
    store.set(key(b"b"), value(b"2")).unwrap();
    store.set(key(b"c"), value(b"3")).unwrap();
    store.set(key(b"a"), value(b"hi")).unwrap();
    write_log_at(tmp.path(), 61, b"X");

    assert!(matches!(
        store.compact(),
        Err(Error::Corruption {
            offset: 42,
            reason: CorruptionReason::PayloadChecksum
        })
    ));
    assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
    assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
    store.set(key(b"d"), value(b"4")).unwrap();
    assert_eq!(store.get(&key(b"d")).unwrap(), Some(value(b"4")));
}

#[test]
fn a_compaction_that_fails_after_writing_copies_leaves_no_trace() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    let big = vec![b'v'; 700 << 10];
    store.set(key(b"a"), value(&big)).unwrap();
    store.set(key(b"b"), value(&big)).unwrap();
    store.set(key(b"c"), value(b"3")).unwrap();
    let c = 2 * (19 + 1 + (700 << 10));
    write_log_at(tmp.path(), c + 20, b"X");

    // More than the 1 MiB compaction buffer is copied before c fails.
    assert!(matches!(
        store.compact(),
        Err(Error::Corruption { offset, reason: CorruptionReason::PayloadChecksum })
            if offset == c
    ));
    let end = c + 21;
    assert_eq!(file_names(tmp.path()), [log_name(0).as_str(), "tally.lock"]);
    assert_eq!(read(&store, b"a"), Some(big.clone()));
    assert_eq!(read(&store, b"b"), Some(big));
    put(&store, b"d", b"4");
    assert_eq!(
        log_bytes(tmp.path())[end as usize..],
        encoded_at(end, &[set_record(b"d", b"4")])
    );
    assert_eq!(store.index.get(b"d").unwrap().offset, end);
}

fn auto(dead_percent: u8, min_log_bytes: u64) -> Options {
    Options {
        auto_compact: AutoCompact::new(dead_percent, min_log_bytes),
        ..Options::default()
    }
}

#[test]
fn a_write_that_leaves_half_the_log_dead_compacts_it() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open_with(tmp.path(), auto(50, 0)).unwrap();
    store.set(key(b"a"), value(b"1")).unwrap();
    store.set(key(b"b"), value(b"2")).unwrap();
    store.set(key(b"a"), value(b"3")).unwrap();
    assert_eq!(log_bytes(tmp.path()).len(), 63, "21 of 63 dead");

    store.set(key(b"b"), value(b"4")).unwrap();
    assert_eq!(
        log_bytes(tmp.path()),
        encoded_at(84, &[set_record(b"a", b"3"), set_record(b"b", b"4")])
    );

    store.set(key(b"a"), value(b"5")).unwrap();
    assert_eq!(
        log_bytes(tmp.path()).len(),
        63,
        "compaction left nothing dead, so 21 of 63 now"
    );
}

#[test]
fn a_delete_counts_its_own_record_as_dead_too() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open_with(tmp.path(), auto(50, 0)).unwrap();
    store.set(key(b"a"), value(b"hi")).unwrap();
    store.set(key(b"b"), value(b"2")).unwrap();
    store.delete(key(b"a")).unwrap();
    assert_eq!(
        log_bytes(tmp.path()),
        encoded_at(63, &[set_record(b"b", b"2")])
    );
}

#[test]
fn a_log_below_the_minimum_size_is_not_compacted() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open_with(tmp.path(), auto(50, 43)).unwrap();
    store.set(key(b"a"), value(b"1")).unwrap();
    store.set(key(b"a"), value(b"2")).unwrap();
    assert_eq!(log_bytes(tmp.path()).len(), 42);
    store.set(key(b"a"), value(b"3")).unwrap();
    assert_eq!(
        log_bytes(tmp.path()),
        encoded_at(63, &[set_record(b"a", b"3")])
    );
}

#[test]
fn without_auto_compact_the_log_only_grows() {
    let tmp = tempfile::tempdir().unwrap();
    let options = Options {
        auto_compact: None,
        ..Options::default()
    };
    let store = Store::open_with(tmp.path(), options).unwrap();
    for v in [b"1", b"2", b"3"] {
        store.set(key(b"a"), value(v)).unwrap();
    }
    store.delete(key(b"a")).unwrap();
    drop(store);
    Store::open_with(tmp.path(), options).unwrap();
    assert_eq!(log_bytes(tmp.path()).len(), 83);
}

#[test]
fn open_compacts_a_log_that_is_due() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open_with(
        tmp.path(),
        Options {
            auto_compact: None,
            ..Options::default()
        },
    )
    .unwrap();
    store.set(key(b"a"), value(b"hi")).unwrap();
    store.set(key(b"a"), value(b"hi")).unwrap();
    drop(store);

    let store = Store::open_with(tmp.path(), auto(50, 0)).unwrap();
    assert_eq!(
        log_bytes(tmp.path()),
        encoded_at(44, &[set_record(b"a", b"hi")])
    );
    assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
}

#[test]
fn open_counts_the_dead_bytes_already_in_the_log() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open_with(
        tmp.path(),
        Options {
            auto_compact: None,
            ..Options::default()
        },
    )
    .unwrap();
    store.set(key(b"a"), value(b"1")).unwrap();
    store.set(key(b"b"), value(b"2")).unwrap();
    store.set(key(b"a"), value(b"3")).unwrap();
    drop(store);

    let store = Store::open_with(tmp.path(), auto(50, 0)).unwrap();
    assert_eq!(log_bytes(tmp.path()).len(), 63, "21 of 63 dead");
    store.set(key(b"b"), value(b"4")).unwrap();
    assert_eq!(
        log_bytes(tmp.path()),
        encoded_at(84, &[set_record(b"a", b"3"), set_record(b"b", b"4")])
    );
}

#[test]
fn a_failed_automatic_compaction_keeps_the_write_and_stays_off() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open_with(tmp.path(), auto(50, 0)).unwrap();
    store.set(key(b"a"), value(b"hi")).unwrap();
    store.set(key(b"b"), value(b"2")).unwrap();
    write_log_at(tmp.path(), 21, b"X");
    store.set(key(b"b"), value(b"3")).unwrap();
    store.set(key(b"b"), value(b"4")).unwrap();
    assert!(!store.auto_failed.load(Ordering::Relaxed), "42 of 85 dead");

    store.set(key(b"b"), value(b"5")).unwrap();
    assert!(store.auto_failed.load(Ordering::Relaxed));
    assert_eq!(log_bytes(tmp.path()).len(), 106);
    assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"5")));
}

#[test]
fn a_store_can_be_shared_between_threads() {
    fn shared<T: Send + Sync>() {}
    shared::<Store>();
}

#[test]
fn writes_waiting_for_the_writer_share_one_fsync() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    let writing = store.writer();
    thread::scope(|scope| {
        for i in 0..8u8 {
            let store = &store;
            scope.spawn(move || store.set(key(&[i]), value(&[i])).unwrap());
        }
        // The first to queue leads alone and waits for `writing`; the
        // other seven queue behind it.
        while hold(&store.queue).waiting.len() < 7 {
            thread::yield_now();
        }
        drop(writing);
    });
    assert_eq!(store.commits.load(Ordering::Relaxed), 2);
    assert_eq!(
        log_bytes(tmp.path()).len(),
        21 + 27 + 7 * 21,
        "one alone, seven in a batch"
    );
    for i in 0..8u8 {
        assert_eq!(read(&store, &[i]), Some(vec![i]));
    }
    store.verify().unwrap();
}

#[test]
fn a_group_applies_its_writes_in_order_with_one_fsync() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    put(&store, b"b", b"0");
    let results = store.commit(&[
        Write::One(set_record(b"a", b"1")),
        Write::One(delete_record(b"a")),
        Write::One(delete_record(b"c")),
        Write::Batch(vec![delete_record(b"a"), set_record(b"b", b"2")]),
        Write::Batch(vec![delete_record(b"c")]),
    ]);
    assert_eq!(results.len(), 5);
    assert!(results.iter().all(Result::is_ok));
    assert_eq!(store.commits.load(Ordering::Relaxed), 2);
    assert_eq!(
        log_bytes(tmp.path()),
        [
            encoded(&[set_record(b"b", b"0")]),
            batched_at(
                21,
                &[
                    set_record(b"a", b"1"),
                    delete_record(b"a"),
                    set_record(b"b", b"2"),
                ]
            ),
        ]
        .concat()
    );
    assert_eq!(read(&store, b"a"), None);
    assert_eq!(read(&store, b"b").as_deref(), Some(&b"2"[..]));
    assert_eq!(store.dead(), 21 + 21 + 20 + 27);
    store.verify().unwrap();
}

#[test]
fn a_failed_group_fails_every_write_in_it() {
    let results = each(Error::Io(io::Error::other("disk on fire")), 3);
    let messages: Vec<String> = results
        .into_iter()
        .map(|r| match r {
            Err(Error::Io(err)) => err.to_string(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(messages, ["disk on fire"; 3]);
}

/// The records of the first segment on `fs`.
fn log_on(fs: &MemFs) -> Vec<u8> {
    fs.read(&Path::new("/db").join(log_name(0))).unwrap()[FILE_HEADER_LEN as usize..].to_vec()
}

#[test]
fn a_failed_fsync_fails_its_write_and_every_write_after_until_reopen() {
    let fs = MemFs::new();
    let store = open_on(&fs, segmented(1 << 20));
    store.set(key(b"a"), value(b"1")).unwrap();
    fs.fail_syncs(true);
    let err = store.set(key(b"b"), value(b"2")).unwrap_err();
    assert!(
        matches!(&err, Error::Io(io) if io.to_string() == "injected fsync failure"),
        "{err:?}"
    );
    fs.fail_syncs(false);
    assert!(matches!(
        store.set(key(b"c"), value(b"3")),
        Err(Error::Poisoned)
    ));
    assert!(matches!(store.delete(key(b"a")), Err(Error::Poisoned)));
    assert!(matches!(
        store.write(batch(&[(b"c", Some(b"3"))])),
        Err(Error::Poisoned)
    ));
    assert!(matches!(store.compact(), Err(Error::Poisoned)));
    assert_eq!(get_on(&store, b"a").as_deref(), Some(&b"1"[..]));
    assert_eq!(get_on(&store, b"b"), None);
    drop(store);

    let store = open_on(&fs, segmented(1 << 20));
    assert_eq!(get_on(&store, b"b"), None);
    store.set(key(b"c"), value(b"3")).unwrap();
    drop(store);

    let store = open_on(&power_cut(&fs), segmented(1 << 20));
    assert_eq!(get_on(&store, b"a").as_deref(), Some(&b"1"[..]));
    assert_eq!(get_on(&store, b"b"), None);
    assert_eq!(get_on(&store, b"c").as_deref(), Some(&b"3"[..]));
}

/// The disk after power fails with nothing unsynced landing.
fn power_cut(fs: &MemFs) -> MemFs {
    let nothing = Cut {
        keep: 0,
        torn: Torn::Nothing,
        entries: 0,
        whole: None,
    };
    MemFs::after(&fs.disk(), nothing)
}

#[test]
fn a_failed_directory_fsync_when_starting_a_segment_poisons_the_store() {
    let fs = MemFs::new();
    let store = open_on(&fs, segmented(21));
    store.set(key(b"a"), value(b"1")).unwrap();
    fs.fail_dir_syncs(true);
    let err = store.set(key(b"b"), value(b"2")).unwrap_err();
    assert!(
        matches!(&err, Error::Io(io) if io.to_string() == "injected directory fsync failure"),
        "{err:?}"
    );
    fs.fail_dir_syncs(false);
    assert!(matches!(
        store.set(key(b"b"), value(b"2")),
        Err(Error::Poisoned)
    ));
}

#[test]
fn an_open_whose_compaction_poisons_the_store_fails() {
    let fs = MemFs::new();
    let options = Options {
        auto_compact: AutoCompact::new(50, 0),
        segment_bytes: 1 << 20,
    };
    let store = open_on(
        &fs,
        Options {
            auto_compact: None,
            ..options
        },
    );
    store.set(key(b"a"), value(b"1")).unwrap();
    store.set(key(b"a"), value(b"2")).unwrap();
    drop(store);
    // Past the two of `open` itself, to the one after the copy's rename.
    fs.fail_dir_syncs_after(2);
    let opened = Store::open_in(fs.clone(), Path::new("/db"), options);
    fs.fail_dir_syncs(false);
    let err = opened.err().expect("open fails");
    assert!(
        matches!(&err, Error::Io(io) if io.to_string() == "injected directory fsync failure"),
        "{err:?}"
    );
    let store = open_on(&fs, options);
    assert_eq!(get_on(&store, b"a").as_deref(), Some(&b"2"[..]));
}

#[test]
fn compaction_with_no_delete_left_behind_fsyncs_its_removals_once() {
    let fs = MemFs::new();
    let store = open_on(&fs, segmented(1));
    for k in [b"a", b"b", b"c"] {
        store.set(key(k), value(b"1")).unwrap();
    }
    // One after the copy's rename, one after removing all three segments.
    fs.fail_dir_syncs_after(2);
    store.compact().unwrap();
    fs.fail_dir_syncs(false);
    drop(store);
    let store = open_on(&fs, segmented(1));
    for k in [b"a", b"b", b"c"] {
        assert_eq!(get_on(&store, k).as_deref(), Some(&b"1"[..]));
    }
}

#[test]
fn a_compaction_whose_hint_reads_back_damaged_moves_no_key() {
    let fs = MemFs::new();
    let store = open_on(&fs, segmented(1));
    for k in [b"a", b"b", b"c", b"d"] {
        store.set(key(k), value(b"1")).unwrap();
    }
    // The first entry's value length goes from 1 to 3, which would point
    // every later key 2 bytes past its copy.
    fs.tamper(&Path::new("/db").join(hint_name(84)), |offset, bytes| {
        if offset == 0 {
            bytes[8] ^= 2;
        }
    });
    let err = store.compact().unwrap_err();
    assert!(
        matches!(
            err,
            Error::Corruption {
                offset: 84,
                reason: CorruptionReason::CompactionHint
            }
        ),
        "{err:?}"
    );
    for k in [b"a", b"b", b"c", b"d"] {
        assert_eq!(get_on(&store, k).as_deref(), Some(&b"1"[..]));
    }
    store.set(key(b"e"), value(b"1")).unwrap();
    drop(store);

    let store = open_on(&fs, segmented(1));
    for k in [b"a", b"b", b"c", b"d", b"e"] {
        assert_eq!(get_on(&store, k).as_deref(), Some(&b"1"[..]));
    }
    store.verify().unwrap();
}

#[test]
fn a_compaction_whose_hint_fsync_fails_keeps_both_and_stays_writable() {
    let fs = MemFs::new();
    let store = open_on(&fs, segmented(1));
    for k in [b"a", b"b", b"c"] {
        store.set(key(k), value(b"1")).unwrap();
    }
    // The copy's header and the copy; then its hint's fails.
    fs.fail_syncs_after(2);
    let err = store.compact().unwrap_err();
    fs.fail_syncs(false);
    assert!(matches!(err, Error::Io(_)), "{err:?}");
    assert_eq!(fs.read(&Path::new("/db").join(hint_name(63))), None);
    assert_eq!(store.dead(), 63, "the copies, which nothing points at");
    for k in [b"a", b"b", b"c"] {
        assert_eq!(get_on(&store, k).as_deref(), Some(&b"1"[..]));
    }
    store.set(key(b"d"), value(b"1")).unwrap();
    drop(store);

    let store = open_on(&fs, segmented(1));
    for k in [b"a", b"b", b"c", b"d"] {
        assert_eq!(get_on(&store, k).as_deref(), Some(&b"1"[..]));
    }
    store.verify().unwrap();
}

#[test]
fn a_compaction_whose_second_removal_fails_loses_nothing_to_a_power_cut() {
    let fs = MemFs::new();
    let store = open_on(&fs, segmented(1));
    for k in [b"a", b"b", b"c"] {
        store.set(key(k), value(b"1")).unwrap();
    }
    // The first segment's absent hint and the segment itself.
    fs.fail_removes_after(2);
    let err = store.compact().unwrap_err();
    assert!(matches!(err, Error::Io(_)), "{err:?}");
    assert_eq!(
        store.segments().iter().map(|s| s.base).collect::<Vec<_>>(),
        [21, 42, 63]
    );
    store.set(key(b"d"), value(b"1")).unwrap();
    let disk = fs.disk();
    drop(store);

    for cut in disk.cuts() {
        let store = open_on(&MemFs::after(&disk, cut), segmented(1));
        for k in [b"a", b"b", b"c", b"d"] {
            assert_eq!(get_on(&store, k).as_deref(), Some(&b"1"[..]), "{cut:?}");
        }
    }
}

#[test]
fn a_write_that_fails_is_truncated_away_and_the_store_stays_writable() {
    let fs = MemFs::new();
    let store = open_on(&fs, segmented(1 << 20));
    store.set(key(b"a"), value(b"1")).unwrap();
    fs.limit_free_space(Some(5));
    let err = store.set(key(b"b"), value(b"2")).unwrap_err();
    assert!(
        matches!(&err, Error::Io(io) if io.kind() == ErrorKind::StorageFull),
        "{err:?}"
    );
    assert_eq!(log_on(&fs), encoded(&[set_record(b"a", b"1")]));

    fs.limit_free_space(None);
    store.set(key(b"c"), value(b"3")).unwrap();
    assert_eq!(
        log_on(&fs),
        encoded(&[set_record(b"a", b"1"), set_record(b"c", b"3")])
    );
    drop(store);
    let store = open_on(&fs, segmented(1 << 20));
    assert_eq!(get_on(&store, b"b"), None);
    assert_eq!(get_on(&store, b"c").as_deref(), Some(&b"3"[..]));
}

#[test]
fn a_failed_truncate_after_a_failed_write_poisons_the_store() {
    let fs = MemFs::new();
    let store = open_on(&fs, segmented(1 << 20));
    store.set(key(b"a"), value(b"1")).unwrap();
    fs.limit_free_space(Some(5));
    fs.fail_truncates(true);
    let err = store.set(key(b"b"), value(b"2")).unwrap_err();
    assert!(
        matches!(&err, Error::Io(io) if io.kind() == ErrorKind::StorageFull),
        "{err:?}"
    );
    fs.limit_free_space(None);
    fs.fail_truncates(false);
    assert!(matches!(
        store.set(key(b"c"), value(b"3")),
        Err(Error::Poisoned)
    ));
    assert_eq!(log_on(&fs).len(), 21 + 5, "the torn write is still there");
    drop(store);

    let store = open_on(&fs, segmented(1 << 20));
    assert_eq!(log_on(&fs), encoded(&[set_record(b"a", b"1")]));
    assert_eq!(get_on(&store, b"a").as_deref(), Some(&b"1"[..]));
    store.set(key(b"c"), value(b"3")).unwrap();
}

#[test]
fn a_failed_fsync_of_a_compactions_copy_leaves_the_store_as_it_was() {
    let fs = MemFs::new();
    let store = open_on(&fs, segmented(1));
    store.set(key(b"a"), value(b"1")).unwrap();
    store.set(key(b"a"), value(b"2")).unwrap();
    store.set(key(b"b"), value(b"3")).unwrap();
    fs.fail_syncs(true);
    assert!(matches!(store.compact(), Err(Error::Io(_))));
    fs.fail_syncs(false);
    let db = Path::new("/db");
    assert_eq!(fs.read(&db.join(compact_name(63))), None);
    assert_eq!(fs.read(&db.join(hint_name(63))), None);
    assert_eq!(get_on(&store, b"a").as_deref(), Some(&b"2"[..]));
    assert_eq!(get_on(&store, b"b").as_deref(), Some(&b"3"[..]));
    store.set(key(b"c"), value(b"4")).unwrap();
    drop(store);

    let store = open_on(&fs, segmented(1));
    assert_eq!(get_on(&store, b"a").as_deref(), Some(&b"2"[..]));
    assert_eq!(get_on(&store, b"b").as_deref(), Some(&b"3"[..]));
    store.verify().unwrap();
    store.compact().unwrap();
    assert_eq!(get_on(&store, b"a").as_deref(), Some(&b"2"[..]));
}

#[test]
fn a_read_that_finds_its_segment_removed_follows_the_key_to_its_copy() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
    put(&store, b"a", b"1");
    put(&store, b"b", b"2");
    let before = store.index.get(b"a").unwrap();
    assert_eq!(before.offset, 0);
    store.compact().unwrap();
    assert_eq!(segment_bases(tmp.path()), [42]);

    let (entry, segment) = store.locate(b"a", before).unwrap();
    assert_eq!(
        entry,
        Entry {
            offset: 42,
            value_len: 1,
            key_len: 1,
        }
    );
    assert_eq!(segment.base, 42);
    store.delete(key(b"a")).unwrap();
    assert!(store.locate(b"a", before).is_none());
}

#[test]
fn reads_while_other_threads_write_and_compact_see_every_value() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open_with(tmp.path(), segmented(256)).unwrap();
    for i in 0..64u8 {
        put(&store, &[b's', i], &[i]);
        put(&store, &[b's', i], &[i, i]);
    }
    let stop = AtomicBool::new(false);
    thread::scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| {
                while !stop.load(Ordering::Relaxed) {
                    for i in 0..64u8 {
                        assert_eq!(read(&store, &[b's', i]), Some(vec![i, i]));
                    }
                }
            });
        }
        for t in 0..4u8 {
            let store = &store;
            scope.spawn(move || {
                for i in 0..50u8 {
                    put(store, &[b'w', t, i % 8], &[i]);
                }
            });
        }
        for _ in 0..20 {
            store.compact().unwrap();
        }
        stop.store(true, Ordering::Relaxed);
    });
    for t in 0..4u8 {
        for i in 0..8u8 {
            let last = if i < 2 { 48 + i } else { 40 + i };
            assert_eq!(read(&store, &[b'w', t, i]), Some(vec![last]));
        }
    }
    store.verify().unwrap();
    drop(store);
    let store = Store::open_with(tmp.path(), segmented(256)).unwrap();
    assert_eq!(read(&store, &[b's', 7]), Some(vec![7, 7]));
    store.verify().unwrap();
}

#[test]
fn a_thread_that_panics_holding_the_writer_lock_poisons_the_store() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    put(&store, b"a", b"1");
    let panicked = thread::scope(|scope| {
        scope
            .spawn(|| {
                let _writing = store.writer();
                panic!("halfway through changing the log");
            })
            .join()
            .is_err()
    });
    assert!(panicked);
    assert!(matches!(
        store.set(key(b"b"), value(b"2")),
        Err(Error::Poisoned)
    ));
    assert!(matches!(store.compact(), Err(Error::Poisoned)));
    assert_eq!(read(&store, b"a").as_deref(), Some(&b"1"[..]));
}

#[test]
fn a_group_that_writes_nothing_does_not_compact() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open_with(tmp.path(), auto(50, 0)).unwrap();
    store.auto_failed.store(true, Ordering::Relaxed);
    put(&store, b"a", b"1");
    put(&store, b"a", b"2");
    store.auto_failed.store(false, Ordering::Relaxed);
    let results = store.commit(&[Write::One(delete_record(b"b"))]);
    assert!(results.iter().all(Result::is_ok));
    assert_eq!(
        log_bytes(tmp.path()).len(),
        42,
        "half dead and not compacted"
    );
    store.commit(&[Write::One(set_record(b"a", b"3"))]);
    assert_eq!(
        log_bytes(tmp.path()),
        encoded_at(63, &[set_record(b"a", b"3")])
    );
}

/// A store whose next leader expects a group of `last_group` writes
/// and whose last fsync took `last_sync`.
fn expecting(dir: &Path, last_group: usize, last_sync: Duration) -> Store {
    let store = Store::open(dir).unwrap();
    hold(&store.queue).last_group = last_group;
    store
        .last_sync_nanos
        .store(last_sync.as_nanos() as u64, Ordering::Relaxed);
    store
}

#[test]
fn a_writer_after_a_group_of_one_does_not_wait() {
    let tmp = tempfile::tempdir().unwrap();
    let store = expecting(tmp.path(), 1, Duration::from_secs(60));
    let started = Instant::now();
    put(&store, b"a", b"1");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "a wait would take 6 s"
    );
}

#[test]
fn a_leader_waits_for_its_group_to_fill_again() {
    let tmp = tempfile::tempdir().unwrap();
    let store = expecting(tmp.path(), 3, Duration::from_secs(60));
    let started = Instant::now();
    thread::scope(|scope| {
        for i in 0..3u8 {
            let store = &store;
            scope.spawn(move || put(store, &[i], &[i]));
        }
    });
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the wait ends at 3 queued"
    );
    assert_eq!(store.commits.load(Ordering::Relaxed), 1);
    assert_eq!(hold(&store.queue).last_group, 3);
}

#[test]
fn a_leader_stops_waiting_after_a_tenth_of_the_last_fsync() {
    let tmp = tempfile::tempdir().unwrap();
    let store = expecting(tmp.path(), 3, Duration::from_millis(200));
    let started = Instant::now();
    put(&store, b"a", b"1");
    assert!(started.elapsed() >= Duration::from_millis(20));
    assert_eq!(store.commits.load(Ordering::Relaxed), 1);
    assert_eq!(
        hold(&store.queue).last_group,
        1,
        "the next leader won't wait"
    );
}

#[test]
fn compaction_reports_damage_in_a_kept_segment_at_its_own_offset() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open_with(tmp.path(), segmented(1)).unwrap();
    put(&store, b"a", b"1");
    store
        .write(batch(&[(b"b", Some(b"2")), (b"b", None)]))
        .unwrap();
    put(&store, b"c", b"3");
    assert_eq!(segment_bases(tmp.path()), [0, 21, 89]);
    let file = OpenOptions::new()
        .write(true)
        .open(tmp.path().join(log_name(0)))
        .unwrap();
    file.write_all_at(b"9", FILE_HEADER_LEN + 20).unwrap();

    let err = store.clean(&store.writer(), &[21]).unwrap_err();

    assert!(
        matches!(
            err,
            Error::Corruption {
                offset: 0,
                reason: CorruptionReason::PayloadChecksum
            }
        ),
        "{err:?}"
    );
}
