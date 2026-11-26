#![forbid(unsafe_code)]

mod batch;
mod error;
mod filter;
pub mod fs;
mod hint;
mod index;
mod kv;
#[cfg(any(test, fuzzing))]
mod mem_fs;
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
pub use store::{Repaired, Store};

/// The codec, for the fuzz targets in `fuzz/`. Only built under cargo-fuzz.
#[cfg(fuzzing)]
#[doc(hidden)]
pub mod fuzz {
    use std::path::Path;

    pub use crate::mem_fs::MemFs;
    pub use crate::record::{
        HEADER_LEN, Place, Record, decode_header, decode_payload, encode, seal,
    };
    pub use crate::segment::segment_file;
    use crate::{Error, Options, Store};

    /// A store on the in-memory disk, which has no syscalls to wait on.
    pub fn open_in_memory(fs: MemFs, dir: &Path, options: Options) -> Result<Store<MemFs>, Error> {
        Store::open_in(fs, dir, options)
    }
}
