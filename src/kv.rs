use crate::{
    Error::{Empty, TooLarge},
    Field, MAX_KEY_LEN, MAX_VALUE_LEN,
    error::Error,
};

pub fn check_len(field: Field, len: usize) -> Result<(), Error> {
    let max = match field {
        Field::Key => MAX_KEY_LEN,
        Field::Value => MAX_VALUE_LEN,
    };
    if len == 0 {
        Err(Empty(field))
    } else if len > max {
        Err(TooLarge { field, len, max })
    } else {
        Ok(())
    }
}

/// A validated key: non-empty and at most `MAX_KEY_LEN` bytes.
///
/// The field is private, so `Key::try_from` is the only way to build one:
///
/// ```compile_fail
/// let key = tally::Key(Box::from(&b""[..]));
/// ```
#[derive(Debug, PartialEq, Eq)]
pub struct Key(Box<[u8]>);

impl Key {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// A validated value: non-empty and at most `MAX_VALUE_LEN` bytes.
///
/// The field is private, so `Value::try_from` is the only way to build one:
///
/// ```compile_fail
/// let value = tally::Value(Box::from(&b""[..]));
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
        let len = bytes.len();
        check_len(Field::Key, len)?;
        Ok(Key(bytes.into()))
    }
}

impl TryFrom<&[u8]> for Value {
    type Error = Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let len = bytes.len();
        check_len(Field::Value, len)?;
        Ok(Value(bytes.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_rejects_empty() {
        assert!(matches!(
            Key::try_from(&b""[..]),
            Err(Error::Empty(Field::Key))
        ));
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
    fn value_rejects_empty() {
        assert!(matches!(
            Value::try_from(&b""[..]),
            Err(Error::Empty(Field::Value))
        ));
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
    fn as_bytes_returns_the_input() {
        assert_eq!(
            Key::try_from(&b"user:42"[..]).unwrap().as_bytes(),
            b"user:42"
        );
        assert_eq!(Value::try_from(&b"hello"[..]).unwrap().as_bytes(), b"hello");
    }

    #[test]
    fn equal_bytes_compare_equal() {
        assert_eq!(
            Key::try_from(&b"a"[..]).unwrap(),
            Key::try_from(&b"a"[..]).unwrap()
        );
        assert_ne!(
            Key::try_from(&b"a"[..]).unwrap(),
            Key::try_from(&b"b"[..]).unwrap()
        );
        assert_eq!(
            Value::try_from(&b"x"[..]).unwrap(),
            Value::try_from(&b"x"[..]).unwrap()
        );
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
