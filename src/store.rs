use std::fs::File;
use std::io::{self, ErrorKind};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::{collections::HashMap, fs};

use crate::record::{HEADER_LEN, Header, Record, decode_header, decode_payload, encode};
use crate::{CorruptionReason, Error, Key, Value};

pub(crate) const LOG_FILE_NAME: &str = "tally.log";

/// Where a live key's latest Set record sits in the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Entry {
    offset: u64,
    value_len: u32,
}

/// An open store: the log file plus an in-memory index of every live key.
pub struct Store {
    file: File,
    /// Length of the log, which is also the offset the next record lands at.
    end: u64,
    index: HashMap<Box<[u8]>, Entry>,
}

impl Store {
    /// Opens the store in `dir`, creating the directory and `tally.log` if
    /// they don't exist, and rebuilds the index by reading every record.
    ///
    /// Creating either one requires an fsync of its parent directory (see
    /// DESIGN.md, New files).
    ///
    /// Milestone 1: any record that fails to decode fails `open` with its
    /// offset. Truncating a torn tail comes in milestone 2.
    pub fn open(dir: impl AsRef<Path>) -> Result<Store, Error> {
        let dir = dir.as_ref();
        let file_path = dir.join(LOG_FILE_NAME);
        match fs::create_dir(dir) {
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        };
        let parent = match dir.parent() {
            Some(p) if !p.as_os_str().is_empty() => p,
            _ => Path::new("."),
        };
        let file = File::options()
            .read(true)
            .append(true)
            .create(true)
            .open(&file_path)?;
        sync(&file);
        sync_dir(dir)?;
        sync_dir(parent)?;
        if let Err(err) = file.try_lock() {
            match err {
                fs::TryLockError::Error(error) => return Err(Error::Io(error)),
                fs::TryLockError::WouldBlock => return Err(Error::Locked),
            }
        }
        let len = file.metadata()?.len();
        let index = load_index(&file, len)?;
        Ok(Store {
            file,
            end: len,
            index,
        })
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
    /// Reads the whole record in one positioned read and verifies both
    /// checksums and that the stored key equals `key`. Any failure is a
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

fn load_record(file: &File, offset: u64) -> Result<(Header, Record), Error> {
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
fn load_index(file: &File, len: u64) -> Result<HashMap<Box<[u8]>, Entry>, Error> {
    let mut index: HashMap<Box<[u8]>, Entry> = HashMap::new();
    let mut offset: u64 = 0;

    while offset < len {
        let header = match load_record(file, offset)? {
            (header, Record::Set { key, value }) => {
                let key_bytes = key.into_bytes();
                index.insert(
                    key_bytes,
                    Entry {
                        offset,
                        value_len: u32::try_from(value.as_bytes().len())
                            .expect("Value guarantees len fits u32"),
                    },
                );
                header
            }
            (header, Record::Delete { key }) => {
                index.remove(key.as_bytes());
                header
            }
        };
        offset += header.record_len();
    }
    Ok(index)
}

/// Flushes `file` to disk. Panics on failure: after a failed fsync the
/// kernel may have dropped the dirty pages, so retrying can report success
/// for data that never reached disk (DESIGN.md, Failed fsync).
fn sync(file: &File) {
    file.sync_all()
        .expect("fsync failed; restart to recover from the log");
}

/// Makes a create or rename inside `dir` durable.
fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(test)]
mod tests {
    use std::fs::{self, OpenOptions};
    use std::os::unix::fs::FileExt;
    use std::path::PathBuf;

    use super::*;
    use crate::record::tests::{DELETE_A, SET_A_HI};
    use crate::record::{Record, encode};
    use crate::{CorruptionReason, Error};

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
