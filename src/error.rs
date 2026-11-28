use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Field {
    Key,
    Value,
}

impl Display for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Field::Key => write!(f, "key"),
            Field::Value => write!(f, "value"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CorruptionReason {
    HeaderChecksum,
    PayloadChecksum,
    ZeroType,
    EmptyKey,
    DeleteWithValue,
    KeyMismatch,
    NotASet,
    /// Found by `Store::verify`: reading the whole log gives a different
    /// index from the one in memory, or the log ends before the store's end.
    IndexMismatch,
    /// A Batch record with the wrong lengths, a batch inside a batch, or
    /// records that don't exactly fill the batch.
    BadBatch,
    /// A segment other than the newest ends partway through a record. Only
    /// the newest can be torn by a crash: every other was complete and
    /// fsynced before the next one was written.
    CutShort,
    /// A segment starts before the one before it ends.
    SegmentsOverlap,
    /// A segment file's header has the wrong checksum or names another
    /// base. Reported at the segment's base.
    SegmentHeader,
    /// The hint a compaction just wrote reads back with a bad checksum, or
    /// listing another number of copies than it made. Reported at the
    /// copy's base. The store keeps the old segments and the copy, with the
    /// index on the old ones, so there is nothing to repair.
    CompactionHint,
    /// A segment the manifest lists is not in the directory: removed, or
    /// lost to the file system. Its records are gone, and `repair` fails
    /// with this too. Reported at the segment's base.
    SegmentMissing,
    /// `tally.manifest` is cut short, fails its checksum, or lists bases
    /// out of order. Reported at offset 0.
    Manifest,
}

impl Display for CorruptionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CorruptionReason::HeaderChecksum => write!(f, "header checksum mismatch"),
            CorruptionReason::PayloadChecksum => write!(f, "payload checksum mismatch"),
            CorruptionReason::ZeroType => write!(f, "record type is zero"),
            CorruptionReason::EmptyKey => write!(f, "key length is zero"),
            CorruptionReason::DeleteWithValue => write!(f, "delete record has a value"),
            CorruptionReason::KeyMismatch => write!(f, "stored key does not match index"),
            CorruptionReason::NotASet => write!(f, "indexed record is not a set"),
            CorruptionReason::IndexMismatch => write!(f, "index does not match the log"),
            CorruptionReason::BadBatch => write!(f, "malformed batch"),
            CorruptionReason::CutShort => write!(f, "segment ends partway through a record"),
            CorruptionReason::SegmentsOverlap => write!(f, "segment starts inside the one before"),
            CorruptionReason::SegmentHeader => write!(f, "segment file header is damaged"),
            CorruptionReason::CompactionHint => {
                write!(f, "a compaction's hint does not read back as written")
            }
            CorruptionReason::SegmentMissing => write!(f, "segment file is missing"),
            CorruptionReason::Manifest => write!(f, "the manifest is damaged"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnsupportedReason {
    UnknownType(u8),
    /// A segment file this version can't read, or a `tally.log`: written
    /// by another version of tally, or not tally's.
    OtherFormat,
    OverLimit {
        field: Field,
        len: usize,
        max: usize,
    },
}

impl Display for UnsupportedReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnsupportedReason::UnknownType(byte) => write!(f, "unknown record type {:#04x}", byte),
            UnsupportedReason::OtherFormat => {
                write!(
                    f,
                    "not in this version's format (written by another version of tally?)"
                )
            }
            UnsupportedReason::OverLimit { field, len, max } => {
                write!(
                    f,
                    "{} length {} exceeds this version's limit of {}",
                    field, len, max
                )
            }
        }
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    EmptyKey,
    TooLarge {
        field: Field,
        len: usize,
        max: usize,
    },
    Io(std::io::Error),
    Corruption {
        offset: u64,
        reason: CorruptionReason,
    },
    Unsupported {
        offset: u64,
        reason: UnsupportedReason,
    },
    /// Another `Store` holds the lock on this directory's log.
    Locked,
    /// `repair` would write this side file, but it already holds bytes that
    /// are not the start of what `repair` would move: an earlier repair at
    /// the same offset saved them, and the log no longer has them.
    SideFileExists(std::path::PathBuf),
    /// An fsync of the log failed, or a truncate after a failed write did,
    /// so what the disk holds is unknown. Every write fails with this until
    /// the store is reopened, which reads what the disk does hold. Reads
    /// still work.
    Poisoned,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::EmptyKey => write!(f, "key must not be empty"),
            Error::TooLarge { field, len, max } => {
                write!(f, "{} is {} bytes, over the {}-byte limit", field, len, max)
            }
            Error::Io(_) => write!(f, "I/O error"),
            Error::Corruption { offset, reason } => {
                write!(f, "corrupt record at offset {}: {}", offset, reason)
            }
            Error::Unsupported { offset, reason } => write!(
                f,
                "unsupported record at offset {}: {} (written by a newer version of tally?)",
                offset, reason
            ),
            Error::Locked => write!(f, "store is already open elsewhere"),
            Error::Poisoned => write!(
                f,
                "an earlier write could not be made durable; reopen the store to write again"
            ),
            Error::SideFileExists(path) => write!(
                f,
                "{} already holds bytes from an earlier repair; move it aside and rerun repair",
                path.display()
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    Corrupt(CorruptionReason),
    Unsupported(UnsupportedReason),
}

impl DecodeError {
    pub fn at(self, offset: u64) -> Error {
        match self {
            DecodeError::Corrupt(reason) => Error::Corruption { offset, reason },
            DecodeError::Unsupported(reason) => Error::Unsupported { offset, reason },
        }
    }
}

impl Error {
    /// The same error reported at `offset`: an error inside a batch is
    /// reported at the batch, the unit `repair` moves.
    pub(crate) fn moved_to(self, offset: u64) -> Error {
        match self {
            Error::Corruption { reason, .. } => Error::Corruption { offset, reason },
            Error::Unsupported { reason, .. } => Error::Unsupported { offset, reason },
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;

    #[test]
    fn empty_message() {
        assert_eq!(Error::EmptyKey.to_string(), "key must not be empty");
    }

    #[test]
    fn too_large_message() {
        let err = Error::TooLarge {
            field: Field::Value,
            len: 20_971_520,
            max: 16_777_216,
        };
        assert_eq!(
            err.to_string(),
            "value is 20971520 bytes, over the 16777216-byte limit"
        );
    }

    #[test]
    fn corruption_messages() {
        let cases = [
            (
                CorruptionReason::HeaderChecksum,
                "corrupt record at offset 4096: header checksum mismatch",
            ),
            (
                CorruptionReason::PayloadChecksum,
                "corrupt record at offset 4096: payload checksum mismatch",
            ),
            (
                CorruptionReason::ZeroType,
                "corrupt record at offset 4096: record type is zero",
            ),
            (
                CorruptionReason::EmptyKey,
                "corrupt record at offset 4096: key length is zero",
            ),
            (
                CorruptionReason::DeleteWithValue,
                "corrupt record at offset 4096: delete record has a value",
            ),
            (
                CorruptionReason::KeyMismatch,
                "corrupt record at offset 4096: stored key does not match index",
            ),
            (
                CorruptionReason::NotASet,
                "corrupt record at offset 4096: indexed record is not a set",
            ),
            (
                CorruptionReason::IndexMismatch,
                "corrupt record at offset 4096: index does not match the log",
            ),
            (
                CorruptionReason::BadBatch,
                "corrupt record at offset 4096: malformed batch",
            ),
            (
                CorruptionReason::CutShort,
                "corrupt record at offset 4096: segment ends partway through a record",
            ),
            (
                CorruptionReason::SegmentsOverlap,
                "corrupt record at offset 4096: segment starts inside the one before",
            ),
            (
                CorruptionReason::SegmentMissing,
                "corrupt record at offset 4096: segment file is missing",
            ),
            (
                CorruptionReason::Manifest,
                "corrupt record at offset 4096: the manifest is damaged",
            ),
        ];
        for (reason, expected) in cases {
            assert_eq!(
                Error::Corruption {
                    offset: 4096,
                    reason
                }
                .to_string(),
                expected
            );
        }
    }

    #[test]
    fn unsupported_messages() {
        let unknown = Error::Unsupported {
            offset: 4096,
            reason: UnsupportedReason::UnknownType(7),
        };
        assert_eq!(
            unknown.to_string(),
            "unsupported record at offset 4096: unknown record type 0x07 (written by a newer version of tally?)"
        );

        let over = Error::Unsupported {
            offset: 0,
            reason: UnsupportedReason::OverLimit {
                field: Field::Key,
                len: 2048,
                max: 1024,
            },
        };
        assert_eq!(
            over.to_string(),
            "unsupported record at offset 0: key length 2048 exceeds this version's limit of 1024 (written by a newer version of tally?)"
        );
    }

    #[test]
    fn io_converts_and_exposes_source() {
        let err: Error =
            std::io::Error::new(std::io::ErrorKind::StorageFull, "No space left on device").into();
        assert!(matches!(err, Error::Io(_)));
        assert_eq!(err.to_string(), "I/O error");
        assert_eq!(err.source().unwrap().to_string(), "No space left on device");
    }

    #[test]
    fn locked_message() {
        assert_eq!(Error::Locked.to_string(), "store is already open elsewhere");
        assert_eq!(
            Error::Poisoned.to_string(),
            "an earlier write could not be made durable; reopen the store to write again"
        );
        assert_eq!(
            Error::SideFileExists("/db/tally.log.corrupt-18".into()).to_string(),
            "/db/tally.log.corrupt-18 already holds bytes from an earlier repair; \
             move it aside and rerun repair"
        );
    }

    #[test]
    fn non_io_errors_have_no_source() {
        assert!(Error::EmptyKey.source().is_none());
        assert!(
            Error::Corruption {
                offset: 0,
                reason: CorruptionReason::HeaderChecksum
            }
            .source()
            .is_none()
        );
    }
}
