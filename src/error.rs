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
pub enum CorruptionReason {
    HeaderChecksum,
    PayloadChecksum,
    ZeroType,
    EmptyKey,
    SetWithoutValue,
    DeleteWithValue,
    KeyMismatch,
    NotASet,
}

impl Display for CorruptionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CorruptionReason::HeaderChecksum => write!(f, "header checksum mismatch"),
            CorruptionReason::PayloadChecksum => write!(f, "payload checksum mismatch"),
            CorruptionReason::ZeroType => write!(f, "record type is zero"),
            CorruptionReason::EmptyKey => write!(f, "key length is zero"),
            CorruptionReason::SetWithoutValue => write!(f, "set record has an empty value"),
            CorruptionReason::DeleteWithValue => write!(f, "delete record has a value"),
            CorruptionReason::KeyMismatch => write!(f, "stored key does not match index"),
            CorruptionReason::NotASet => write!(f, "indexed record is not a set"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnsupportedReason {
    UnknownType(u8),
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
    Empty(Field),
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
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Empty(field) => write!(f, "{} must not be empty", field),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;

    #[test]
    fn empty_message() {
        assert_eq!(
            Error::Empty(Field::Key).to_string(),
            "key must not be empty"
        );
        assert_eq!(
            Error::Empty(Field::Value).to_string(),
            "value must not be empty"
        );
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
                CorruptionReason::SetWithoutValue,
                "corrupt record at offset 4096: set record has an empty value",
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
            Error::SideFileExists("/db/tally.log.corrupt-18".into()).to_string(),
            "/db/tally.log.corrupt-18 already holds bytes from an earlier repair; \
             move it aside and rerun repair"
        );
    }

    #[test]
    fn non_io_errors_have_no_source() {
        assert!(Error::Empty(Field::Key).source().is_none());
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
