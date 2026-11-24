mod constants;
mod error;
mod kv;
mod record;
mod store;

pub use constants::{MAX_KEY_LEN, MAX_VALUE_LEN};
pub(crate) use error::DecodeError;
pub use error::{CorruptionReason, Error, Field, UnsupportedReason};
pub use kv::{Key, Value};
pub use store::{Repaired, Store};

/// The codec, for the fuzz targets in `fuzz/`. Only built under cargo-fuzz.
#[cfg(fuzzing)]
#[doc(hidden)]
pub mod fuzz {
    pub use crate::record::{HEADER_LEN, Record, decode_header, decode_payload, encode};
}
