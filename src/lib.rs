#![forbid(unsafe_code)]

mod batch;
mod error;
mod filter;
pub mod fs;
mod hint;
mod index;
mod kv;
mod manifest;
#[cfg(any(test, fuzzing))]
mod mem_fs;
#[cfg(any(test, fuzzing))]
mod model;
mod options;
#[cfg(test)]
mod power_cut;
mod record;
mod segment;
mod store;

pub use batch::Batch;
pub use error::{CorruptionReason, Error, Field, UnsupportedReason};
pub use kv::{Key, MAX_KEY_LEN, MAX_VALUE_LEN, Value};
pub use options::{AutoCompact, Options};
pub use store::{Repaired, SideFile, Store};

/// The codec and the model, for the fuzz targets in `fuzz/`. Only built under cargo-fuzz.
#[cfg(fuzzing)]
#[doc(hidden)]
pub mod fuzz {
    pub use crate::mem_fs::MemFs;
    pub use crate::model::{Model, Op, options};
    pub use crate::record::{
        HEADER_LEN, Place, Record, decode_header, decode_payload, encode, seal,
    };
    pub use crate::segment::segment_file;
}
