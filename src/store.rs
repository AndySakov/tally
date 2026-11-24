use std::collections::HashMap;
use std::fs::TryLockError;
use std::io::{self, BufReader, ErrorKind, Read};
use std::path::{Path, PathBuf};

use crate::fs::{Fs, LogFile, OsFs};
use crate::record::{
    HEADER_LEN, Header, IndexOp, Record, decode_header, decode_index_op, decode_payload, encode,
};
use crate::{CorruptionReason, DecodeError, Error, Key, Value};

pub(crate) const LOG_FILE_NAME: &str = "tally.log";

type Index = HashMap<Box<[u8]>, Entry, foldhash::quality::RandomState>;

/// Where a live key's latest Set record sits in the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Entry {
    offset: u64,
    value_len: u32,
}

/// What `Store::repair` moved out of the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Repaired {
    /// Offset of the first bad record, where the log now ends.
    pub offset: u64,
    /// Bytes moved out of the log: everything from `offset` to the old end.
    pub dropped: u64,
    /// Where those bytes were saved: `tally.log.corrupt-<offset>` in the
    /// store directory.
    pub side_file: PathBuf,
}

/// An open store: the log file plus an in-memory index of every live key.
///
/// `F` is the file system. It is always [`OsFs`] outside tally's own tests,
/// which swap in an in-memory disk to simulate power cuts.
pub struct Store<F: Fs = OsFs> {
    file: F::File,
    /// Length of the log, which is also the offset the next record lands at.
    end: u64,
    index: Index,
}

impl Store {
    /// Opens the store in `dir`, creating the directory and `tally.log` if
    /// they don't exist, and rebuilds the index by reading every record.
    ///
    /// Creating either one requires an fsync of its parent directory (see
    /// DESIGN.md, New files).
    ///
    /// A torn or zero-filled tail is truncated, fsynced and logged as a
    /// warning before `open` returns. Any other bad record fails `open` with
    /// its offset; `repair` handles those (DESIGN.md, Recovery).
    pub fn open(dir: impl AsRef<Path>) -> Result<Store, Error> {
        Self::open_in(&OsFs, dir.as_ref())
    }

    /// Makes a log that `open` refuses openable again, without opening it.
    ///
    /// Returns `None` if `open` would succeed (a torn tail is left for
    /// `open` to truncate). On a corrupt record, saves every byte from it to
    /// the end of the log in a side file, fsyncs the side file and the
    /// directory, then truncates the log at the record and fsyncs it.
    /// Unsupported records are an error, not something to repair: a newer
    /// tally can read them. Fails with `Locked` while a `Store` is open.
    pub fn repair(dir: impl AsRef<Path>) -> Result<Option<Repaired>, Error> {
        Self::repair_in(&OsFs, dir.as_ref())
    }
}

impl<F: Fs> Store<F> {
    pub(crate) fn open_in(fs: &F, dir: &Path) -> Result<Self, Error> {
        let file_path = dir.join(LOG_FILE_NAME);
        match fs.create_dir(dir) {
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        };
        let parent = match dir.parent() {
            Some(p) if !p.as_os_str().is_empty() => p,
            _ => Path::new("."),
        };
        let file = fs.open_or_create(&file_path)?;
        sync(&file);
        fs.sync_dir(dir)?;
        fs.sync_dir(parent)?;
        lock(&file)?;
        let len = file.size()?;
        let (index, end) = load_index(&file, len)?;
        if end < len {
            file.set_len(end)?;
            sync(&file);
            log::warn!(
                "truncated a torn tail at offset {end}: dropped {} bytes",
                len - end
            );
        }
        Ok(Store { file, end, index })
    }

    pub(crate) fn repair_in(fs: &F, dir: &Path) -> Result<Option<Repaired>, Error> {
        let file_path = dir.join(LOG_FILE_NAME);
        let file = fs.open_existing(&file_path)?;
        lock(&file)?;
        let len = file.size()?;
        let offset = match load_index(&file, len) {
            Ok(_) => return Ok(None),
            Err(Error::Corruption { offset, .. }) => offset,
            Err(err) => return Err(err),
        };

        let side_file = dir.join(format!("{LOG_FILE_NAME}.corrupt-{offset}"));
        let side = fs.open_or_create(&side_file)?;
        side.set_len(0)?;
        copy_to_end(file.reader_at(offset).take(len - offset), &side)?;
        sync(&side);
        fs.sync_dir(dir)?;

        file.set_len(offset)?;
        sync(&file);
        Ok(Some(Repaired {
            offset,
            dropped: len - offset,
            side_file,
        }))
    }

    fn append(&mut self, record: &Record) -> Result<u64, Error> {
        let offset = self.end;
        let mut buffer: Vec<u8> = vec![];
        encode(record, &mut buffer);
        if let Err(err) = self.file.write_all_at(&buffer, offset) {
            self.file.set_len(offset).expect("truncate should not fail");
            return Err(Error::Io(err));
        }
        sync(&self.file);
        self.end += buffer.len() as u64;
        Ok(offset)
    }

    /// Appends a Set record, fsyncs, then points the index at it.
    ///
    /// If the write fails partway through, truncates the log back to `end`
    /// and returns the I/O error. Panics if that truncate fails, or if the
    /// fsync fails.
    pub fn set(&mut self, key: Key, value: Value) -> Result<(), Error> {
        let record = Record::Set { key, value };
        let offset = self.append(&record)?;
        if let Record::Set { key, value } = record {
            self.index.insert(
                key.into_bytes(),
                Entry {
                    offset,
                    value_len: u32::try_from(value.as_bytes().len())
                        .expect("Value guarantees len fits u32"),
                },
            );
        } else {
            unreachable!();
        }
        Ok(())
    }

    /// Returns the value for `key`, or `None` if it isn't live.
    ///
    /// Reads the record with two positioned reads (header, then payload) and
    /// verifies both checksums and that the stored key equals `key`. Any failure is a
    /// corruption error at the record's offset, never `None`.
    pub fn get(&self, key: &Key) -> Result<Option<Value>, Error> {
        match self.index.get(key.as_bytes()) {
            Some(entry) => {
                if let (
                    _,
                    Record::Set {
                        key: stored_key,
                        value,
                    },
                ) = load_record(&self.file, entry.offset)?
                {
                    if *key == stored_key {
                        Ok(Some(value))
                    } else {
                        Err(Error::Corruption {
                            offset: entry.offset,
                            reason: CorruptionReason::KeyMismatch,
                        })
                    }
                } else {
                    Err(Error::Corruption {
                        offset: entry.offset,
                        reason: CorruptionReason::NotASet,
                    })
                }
            }
            None => Ok(None),
        }
    }

    /// Appends a Delete record, fsyncs, then removes the key from the index.
    ///
    /// Deleting a key that isn't live is a no-op: nothing is written and it
    /// returns `Ok`. Failed writes are handled as in `set`.
    pub fn delete(&mut self, key: Key) -> Result<(), Error> {
        if self.index.contains_key(key.as_bytes()) {
            let record = Record::Delete { key };
            self.append(&record)?;
            if let Record::Delete { key } = record {
                self.index.remove(&key.into_bytes());
            } else {
                unreachable!();
            }
        };
        Ok(())
    }
}

fn lock(file: &impl LogFile) -> Result<(), Error> {
    match file.try_lock() {
        Ok(()) => Ok(()),
        Err(TryLockError::WouldBlock) => Err(Error::Locked),
        Err(TryLockError::Error(err)) => Err(Error::Io(err)),
    }
}

/// Appends everything `reader` yields to `file`, in chunks.
fn copy_to_end(mut reader: impl Read, file: &impl LogFile) -> io::Result<()> {
    let mut chunk = vec![0u8; 64 * 1024];
    let mut end = file.size()?;
    loop {
        match reader.read(&mut chunk) {
            Ok(0) => return Ok(()),
            Ok(n) => {
                file.write_all_at(&chunk[..n], end)?;
                end += n as u64;
            }
            Err(err) if err.kind() == ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
}

fn load_record(file: &impl LogFile, offset: u64) -> Result<(Header, Record), Error> {
    let mut header_buf = [0u8; HEADER_LEN];
    file.read_exact_at(&mut header_buf, offset)?;
    let header = decode_header(&header_buf).map_err(|err| err.at(offset))?;
    let mut payload_buf = vec![0u8; header.payload_len()];
    let header_boundary = (HEADER_LEN as u64) + offset;
    file.read_exact_at(&mut payload_buf, header_boundary)?;
    let record = decode_payload(header, &payload_buf).map_err(|err| err.at(offset))?;
    Ok((header, record))
}

/// Reads every record in `file` from offset 0 to `len` and returns the index
/// they describe.
fn load_index(file: &impl LogFile, len: u64) -> Result<(Index, u64), Error> {
    let mut index = Index::default();
    let mut header_buf = [0u8; HEADER_LEN];
    let mut payload_buf = vec![];
    let mut offset: u64 = 0;
    let mut buffer = BufReader::new(file.reader_at(0));

    while offset < len {
        if len - offset < HEADER_LEN as u64 {
            return Ok((index, offset));
        }
        buffer.read_exact(&mut header_buf)?;
        let header = match decode_header(&header_buf) {
            Ok(header) => header,
            Err(DecodeError::Corrupt(CorruptionReason::HeaderChecksum))
                if all_zero(&header_buf) && rest_is_zero(&mut buffer)? =>
            {
                return Ok((index, offset));
            }
            Err(err) => return Err(err.at(offset)),
        };
        if len - offset < header.record_len() {
            return Ok((index, offset));
        }
        payload_buf.resize(header.payload_len(), 0u8);
        buffer.read_exact(&mut payload_buf)?;
        let op = match decode_index_op(header, &payload_buf) {
            Ok(op) => op,
            Err(DecodeError::Corrupt(CorruptionReason::PayloadChecksum))
                if all_zero(&payload_buf) && rest_is_zero(&mut buffer)? =>
            {
                return Ok((index, offset));
            }
            Err(err) => return Err(err.at(offset)),
        };
        match op {
            IndexOp::Set { key, value_len } => {
                index.insert(key.into_bytes(), Entry { offset, value_len });
            }
            IndexOp::Delete { key } => {
                index.remove(key.as_bytes());
            }
        }
        offset += header.record_len();
    }
    Ok((index, offset))
}

fn all_zero(bytes: &[u8]) -> bool {
    bytes.iter().all(|&b| b == 0)
}

/// Reads `reader` to the end in chunks and reports whether every byte was
/// zero. A preallocated log can be gigabytes of zeros, so nothing is kept.
fn rest_is_zero(reader: &mut impl Read) -> io::Result<bool> {
    let mut chunk = [0u8; 8192];
    loop {
        match reader.read(&mut chunk) {
            Ok(0) => return Ok(true),
            Ok(n) if !all_zero(&chunk[..n]) => return Ok(false),
            Ok(_) => {}
            Err(err) if err.kind() == ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
}

/// Flushes `file` to disk. Panics on failure: after a failed fsync the
/// kernel may have dropped the dirty pages, so retrying can report success
/// for data that never reached disk (DESIGN.md, Failed fsync).
fn sync(file: &impl LogFile) {
    file.sync()
        .expect("fsync failed; restart to recover from the log");
}

#[cfg(test)]
mod tests {
    use std::fs::{self, OpenOptions};
    use std::os::unix::fs::FileExt;
    use std::path::PathBuf;

    use super::*;
    use crate::record::tests::{DELETE_A, SET_A_HI};
    use crate::record::{Record, encode};
    use crate::{CorruptionReason, Error, UnsupportedReason};

    fn key(bytes: &[u8]) -> Key {
        Key::try_from(bytes).unwrap()
    }

    fn value(bytes: &[u8]) -> Value {
        Value::try_from(bytes).unwrap()
    }

    fn log_path(dir: &Path) -> PathBuf {
        dir.join(LOG_FILE_NAME)
    }

    fn log_bytes(dir: &Path) -> Vec<u8> {
        fs::read(log_path(dir)).unwrap()
    }

    /// Overwrites bytes in the log behind the store's back.
    fn write_log_at(dir: &Path, offset: u64, bytes: &[u8]) {
        let file = OpenOptions::new().write(true).open(log_path(dir)).unwrap();
        file.write_all_at(bytes, offset).unwrap();
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
        bytes[16] ^= 0x01; // first record's value: "hi" -> "ii"
        fs::write(log_path(tmp.path()), bytes).unwrap();
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
        fs::write(log_path(tmp.path()), bytes).unwrap();
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

    // recovery: torn and zero-filled tails are truncated

    #[test]
    fn open_truncates_a_torn_header() {
        let tmp = store_with_log(&set_a_hi_then(&DELETE_A[..14]));
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        assert_eq!(log_bytes(tmp.path()), SET_A_HI);
    }

    #[test]
    fn open_truncates_a_torn_payload() {
        let tmp = store_with_log(&set_a_hi_then(&DELETE_A[..15]));
        let store = Store::open(tmp.path()).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
        assert_eq!(log_bytes(tmp.path()), SET_A_HI);
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
        let mut store = Store::open(tmp.path()).unwrap();
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

    // recovery: anything else fails open and leaves the log alone

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
        let bytes = [SET_A_HI.as_slice(), &[0u8; 16], DELETE_A.as_slice()].concat();
        assert_open_fails_untouched(&bytes, 18, CorruptionReason::HeaderChecksum);
    }

    #[test]
    fn open_fails_on_a_zero_payload_before_a_valid_record() {
        let bytes = [
            SET_A_HI.as_slice(),
            &delete_a_with_a_zero_payload(),
            DELETE_A.as_slice(),
        ]
        .concat();
        assert_open_fails_untouched(&bytes, 18, CorruptionReason::PayloadChecksum);
    }

    #[test]
    fn open_fails_on_a_garbage_tail_with_its_offset() {
        assert_open_fails_untouched(
            &set_a_hi_then(&[0xab; 16]),
            18,
            CorruptionReason::HeaderChecksum,
        );
    }

    #[test]
    fn open_fails_on_a_corrupted_header_checksum_with_its_offset() {
        let mut bytes = set_a_hi_then(&DELETE_A);
        bytes[18] ^= 0x01;
        assert_open_fails_untouched(&bytes, 18, CorruptionReason::HeaderChecksum);
    }

    #[test]
    fn open_fails_on_a_corrupted_length_with_its_offset() {
        let mut bytes = set_a_hi_then(&DELETE_A);
        bytes[18 + 7] ^= 0x01; // second record's value_len: 0 -> 1
        assert_open_fails_untouched(&bytes, 18, CorruptionReason::HeaderChecksum);
    }

    #[test]
    fn open_fails_on_a_corrupted_key_with_its_offset() {
        let mut bytes = set_a_hi_then(&DELETE_A);
        bytes[18 + 15] ^= 0x01; // second record's key: "a" -> "`"
        assert_open_fails_untouched(&bytes, 18, CorruptionReason::PayloadChecksum);
    }

    #[test]
    fn open_fails_on_an_unsupported_tail_record_and_leaves_it() {
        let mut bytes = set_a_hi_then(&DELETE_A);
        bytes[18 + 4] = 9; // second record's type: unknown
        fix_header_checksum(&mut bytes[18..18 + HEADER_LEN]);
        let tmp = store_with_log(&bytes);
        assert!(matches!(
            Store::open(tmp.path()),
            Err(Error::Unsupported {
                offset: 18,
                reason: UnsupportedReason::UnknownType(9)
            })
        ));
        assert_eq!(log_bytes(tmp.path()), bytes);
    }

    fn fix_header_checksum(header: &mut [u8]) {
        let checksum = crc32c::crc32c(&header[4..HEADER_LEN]);
        header[..4].copy_from_slice(&checksum.to_le_bytes());
    }

    // repair

    fn side_file(dir: &Path, offset: u64) -> PathBuf {
        dir.join(format!("{LOG_FILE_NAME}.corrupt-{offset}"))
    }

    /// SET_A_HI, then a Delete of "a" with its key corrupted, then a Set of
    /// "b": the bad record at 18 hides the valid one after it.
    fn log_with_a_corrupt_middle() -> Vec<u8> {
        let mut delete = DELETE_A.to_vec();
        delete[HEADER_LEN] ^= 0x01;
        let mut set_b = Vec::new();
        encode(
            &Record::Set {
                key: key(b"b"),
                value: value(b"2"),
            },
            &mut set_b,
        );
        [SET_A_HI.as_slice(), &delete, &set_b].concat()
    }

    #[test]
    fn repair_of_an_intact_log_does_nothing() {
        let bytes = set_a_hi_then(&DELETE_A);
        let tmp = store_with_log(&bytes);
        assert_eq!(Store::repair(tmp.path()).unwrap(), None);
        assert_eq!(log_bytes(tmp.path()), bytes);
        assert_eq!(fs::read_dir(tmp.path()).unwrap().count(), 1);
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
                offset: 18,
                dropped: 33,
                side_file: side_file(tmp.path(), 18),
            })
        );
        assert_eq!(log_bytes(tmp.path()), SET_A_HI);
        assert_eq!(fs::read(side_file(tmp.path(), 18)).unwrap(), &bytes[18..]);
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
        assert_eq!(fs::read(side_file(tmp.path(), 18)).unwrap(), &bytes[18..]);
    }

    #[test]
    fn repair_refuses_an_unsupported_record() {
        let mut bytes = set_a_hi_then(&DELETE_A);
        bytes[18 + 4] = 9;
        fix_header_checksum(&mut bytes[18..18 + HEADER_LEN]);
        let tmp = store_with_log(&bytes);
        assert!(matches!(
            Store::repair(tmp.path()),
            Err(Error::Unsupported { offset: 18, .. })
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
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
    }

    #[test]
    fn set_writes_the_record_format() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        assert_eq!(log_bytes(tmp.path()), SET_A_HI);
    }

    #[test]
    fn set_overwrites_and_appends() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.set(key(b"a"), value(b"yo")).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"yo")));
        assert_eq!(log_bytes(tmp.path()).len(), 36);
    }

    #[test]
    fn keys_are_independent() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"1")).unwrap();
        store.set(key(b"b"), value(b"2")).unwrap();
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"1")));
        assert_eq!(store.get(&key(b"b")).unwrap(), Some(value(b"2")));
    }

    // delete

    #[test]
    fn delete_removes_the_key_and_appends_a_delete_record() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
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
        let mut store = Store::open(tmp.path()).unwrap();
        store.delete(key(b"a")).unwrap();
        assert_eq!(log_bytes(tmp.path()), b"");
    }

    #[test]
    fn delete_of_an_already_deleted_key_writes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.delete(key(b"a")).unwrap();
        store.delete(key(b"a")).unwrap();
        assert_eq!(log_bytes(tmp.path()).len(), 34);
    }

    // reopen

    #[test]
    fn reopen_rebuilds_the_index() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut store = Store::open(tmp.path()).unwrap();
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
            let mut store = Store::open(tmp.path()).unwrap();
            store.set(key(b"a"), value(b"hi")).unwrap();
            store
        });
        let mut store = Store::open(tmp.path()).unwrap();
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
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        store.set(key(b"b"), value(b"hi")).unwrap();
        write_log_at(tmp.path(), 18 + 17, b"x"); // b's value: "hi" -> "hx"
        assert!(matches!(
            store.get(&key(b"b")),
            Err(Error::Corruption {
                offset: 18,
                reason: CorruptionReason::PayloadChecksum
            })
        ));
        assert_eq!(store.get(&key(b"a")).unwrap(), Some(value(b"hi")));
    }

    #[test]
    fn get_detects_a_corrupted_header() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = Store::open(tmp.path()).unwrap();
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
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        let mut other = Vec::new();
        encode(
            &Record::Set {
                key: key(b"b"),
                value: value(b"hi"),
            },
            &mut other,
        );
        write_log_at(tmp.path(), 0, &other);
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
        let mut store = Store::open(tmp.path()).unwrap();
        store.set(key(b"a"), value(b"hi")).unwrap();
        write_log_at(tmp.path(), 0, &DELETE_A);
        assert!(matches!(
            store.get(&key(b"a")),
            Err(Error::Corruption {
                offset: 0,
                reason: CorruptionReason::NotASet
            })
        ));
    }
}
