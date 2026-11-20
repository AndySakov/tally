mod constants;
mod error;
mod kv;
mod record;

pub use constants::{MAX_KEY_LEN, MAX_VALUE_LEN};
pub(crate) use error::DecodeError;
pub use error::{CorruptionReason, Error, Field, UnsupportedReason};
pub use kv::{Key, Value};
