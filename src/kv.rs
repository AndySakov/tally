use crate::{
    Error::{EmptyKey, TooLarge},
    Field,
    error::Error,
};

pub const MAX_KEY_LEN: usize = 1024;
pub const MAX_VALUE_LEN: usize = 16 * 1024 * 1024;

const _: () = assert!(
    MAX_KEY_LEN <= u16::MAX as usize,
    "MAX_KEY_LEN must fit the u16 key_len field"
);
const _: () = assert!(
    MAX_VALUE_LEN <= u32::MAX as usize,
    "MAX_VALUE_LEN must fit the u32 value_len field"
);

fn check_len(field: Field, len: usize, max: usize) -> Result<(), Error> {
    if len > max {
        return Err(TooLarge { field, len, max });
    }
    Ok(())
}

/// A validated key: non-empty and at most `MAX_KEY_LEN` bytes.
///
/// The field is private, so `Key::try_from` is the only way to build one:
///
/// ```compile_fail
/// let key = tally::Key(Box::from(&b""[..]));
/// ```
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key(Box<[u8]>);

impl Key {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// A validated value: at most `MAX_VALUE_LEN` bytes, possibly none.
///
/// The field is private, so `Value::try_from` is the only way to build one:
///
/// ```compile_fail
/// let value = tally::Value(Box::from(&b"v"[..]));
/// ```
#[derive(PartialEq, Eq)]
pub struct Value(Box<[u8]>);

impl Value {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

const DEBUG_HEAD_LEN: usize = 16;

impl std::fmt::Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let head_len = self.0.len().min(DEBUG_HEAD_LEN);
        let head = &self.0[..head_len];
        f.debug_struct("Value")
            .field("len", &self.0.len())
            .field("head", &head)
            .finish()
    }
}

impl TryFrom<&[u8]> for Key {
    type Error = Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.is_empty() {
            return Err(EmptyKey);
        }
        check_len(Field::Key, bytes.len(), MAX_KEY_LEN)?;
        Ok(Key(bytes.into()))
    }
}

impl TryFrom<&[u8]> for Value {
    type Error = Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        check_len(Field::Value, bytes.len(), MAX_VALUE_LEN)?;
        Ok(Value(bytes.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_rejects_empty() {
        assert!(matches!(Key::try_from(&b""[..]), Err(Error::EmptyKey)));
    }

    #[test]
    fn key_accepts_one_byte_and_exactly_the_limit() {
        assert_eq!(&*Key::try_from(&b"a"[..]).unwrap().0, b"a");
        assert_eq!(Key::try_from(&[7u8; 1024][..]).unwrap().0.len(), 1024);
    }

    #[test]
    fn key_rejects_one_byte_over_the_limit() {
        assert!(matches!(
            Key::try_from(&[7u8; 1025][..]),
            Err(Error::TooLarge {
                field: Field::Key,
                len: 1025,
                max: 1024
            })
        ));
    }

    #[test]
    fn a_value_may_be_empty() {
        assert_eq!(Value::try_from(&b""[..]).unwrap().as_bytes(), b"");
    }

    #[test]
    fn value_accepts_exactly_the_limit() {
        let bytes = vec![7u8; 16 * 1024 * 1024];
        assert_eq!(Value::try_from(&bytes[..]).unwrap().0.len(), 16_777_216);
    }

    #[test]
    fn value_rejects_one_byte_over_the_limit() {
        let bytes = vec![7u8; 16 * 1024 * 1024 + 1];
        assert!(matches!(
            Value::try_from(&bytes[..]),
            Err(Error::TooLarge {
                field: Field::Value,
                len: 16_777_217,
                max: 16_777_216
            })
        ));
    }

    #[test]
    fn value_debug_shows_length_and_short_values_in_full() {
        let value = Value::try_from(&b"hello"[..]).unwrap();
        assert_eq!(
            format!("{value:?}"),
            "Value { len: 5, head: [104, 101, 108, 108, 111] }"
        );
    }

    #[test]
    fn value_debug_truncates_long_values_to_16_bytes() {
        let value = Value::try_from(&[7u8; 1000][..]).unwrap();
        assert_eq!(
            format!("{value:?}"),
            "Value { len: 1000, head: [7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7] }"
        );
    }
}
