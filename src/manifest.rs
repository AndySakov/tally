//! The manifest (DESIGN.md, Manifest): the bases of the log's segments, so
//! `open` can tell a segment that went missing from a hole compaction left.
//!
//! ```text
//! magic "TLYM" | version u8 | count u64 | base u64, count times | crc32c u32
//! ```
//!
//! Little-endian. The bases are strictly increasing, and there is at least
//! one. The checksum covers every byte before it.

use std::io::{self, ErrorKind};
use std::path::Path;

use crate::fs::{Fs, LogFile};
use crate::{CorruptionReason, Error, UnsupportedReason};

pub(crate) const MANIFEST_NAME: &str = "tally.manifest";
/// Where a new manifest is written before it is renamed over the old one.
pub(crate) const NEW_MANIFEST_NAME: &str = "tally.manifest.new";
const MAGIC: &[u8; 4] = b"TLYM";
const VERSION: u8 = 3;
const PREFIX_LEN: u64 = 13;
const CHECKSUM_LEN: u64 = 4;

fn encode(bases: &[u64]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(PREFIX_LEN as usize + 8 * bases.len() + 4);
    bytes.extend_from_slice(MAGIC);
    bytes.push(VERSION);
    bytes.extend_from_slice(&(bases.len() as u64).to_le_bytes());
    for base in bases {
        bytes.extend_from_slice(&base.to_le_bytes());
    }
    let checksum = crc32c::crc32c(&bytes);
    bytes.extend_from_slice(&checksum.to_le_bytes());
    bytes
}

fn damaged() -> Error {
    Error::Corruption {
        offset: 0,
        reason: CorruptionReason::Manifest,
    }
}

/// The bases the manifest in `dir` lists, or `None` if there is none.
pub(crate) fn read(fs: &impl Fs, dir: &Path) -> Result<Option<Vec<u64>>, Error> {
    let file = match fs.open_existing(&dir.join(MANIFEST_NAME)) {
        Ok(file) => file,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let size = file.size()?;
    if size < PREFIX_LEN + CHECKSUM_LEN {
        return Err(damaged());
    }
    let mut prefix = [0u8; PREFIX_LEN as usize];
    file.read_exact_at(&mut prefix, 0)?;
    if prefix[..4] != *MAGIC {
        return Err(damaged());
    }
    if prefix[4] != VERSION {
        return Err(Error::Unsupported {
            offset: 0,
            reason: UnsupportedReason::OtherFormat,
        });
    }
    let count = u64::from_le_bytes(prefix[5..].try_into().unwrap());
    let expected = count
        .checked_mul(8)
        .and_then(|n| n.checked_add(PREFIX_LEN + CHECKSUM_LEN));
    if count == 0 || expected != Some(size) {
        return Err(damaged());
    }
    let mut bytes = vec![0u8; usize::try_from(size).map_err(|_| damaged())?];
    file.read_exact_at(&mut bytes, 0)?;
    let (body, checksum) = bytes.split_at(bytes.len() - CHECKSUM_LEN as usize);
    if crc32c::crc32c(body).to_le_bytes() != checksum {
        return Err(damaged());
    }
    let bases: Vec<u64> = body[PREFIX_LEN as usize..]
        .as_chunks::<8>()
        .0
        .iter()
        .map(|b| u64::from_le_bytes(*b))
        .collect();
    if !bases.windows(2).all(|w| w[0] < w[1]) {
        return Err(damaged());
    }
    Ok(Some(bases))
}

/// Makes `bases` the manifest in `dir`: written to a new file, fsynced,
/// renamed over the old one, and the directory fsynced, so a crash leaves
/// the old list or this one. Every base must name a segment whose name is
/// already durable.
pub(crate) fn write(fs: &impl Fs, dir: &Path, bases: &[u64]) -> io::Result<()> {
    let path = dir.join(NEW_MANIFEST_NAME);
    let file = fs.open_or_create(&path)?;
    file.set_len(0)?;
    file.write_all_at(&encode(bases), 0)?;
    file.sync()?;
    fs.rename(&path, &dir.join(MANIFEST_NAME))?;
    fs.sync_dir(dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem_fs::MemFs;

    /// Computed independently of this crate, with a Python CRC32C.
    const BASES_0_AND_21: [u8; 33] = [
        0x54, 0x4c, 0x59, 0x4d, // "TLYM"
        0x03, // version
        2, 0, 0, 0, 0, 0, 0, 0, // count
        0, 0, 0, 0, 0, 0, 0, 0, // 0
        21, 0, 0, 0, 0, 0, 0, 0, // 21
        0x97, 0x8d, 0xc9, 0xa4, // CRC32C of the 29 bytes before
    ];

    fn read_bytes(bytes: &[u8]) -> Result<Option<Vec<u64>>, Error> {
        let fs = MemFs::new();
        fs.put(&Path::new("/db").join(MANIFEST_NAME), bytes);
        read(&fs, Path::new("/db"))
    }

    #[test]
    fn a_manifest_is_written_in_its_format_and_read_back() {
        let fs = MemFs::new();
        let dir = Path::new("/db");
        fs.put(&dir.join("x"), b"");
        assert_eq!(read(&fs, dir).unwrap(), None);
        write(&fs, dir, &[0, 21]).unwrap();
        assert_eq!(fs.read(&dir.join(MANIFEST_NAME)).unwrap(), BASES_0_AND_21);
        assert_eq!(fs.read(&dir.join(NEW_MANIFEST_NAME)), None);
        assert_eq!(read(&fs, dir).unwrap(), Some(vec![0, 21]));
    }

    #[test]
    fn a_damaged_manifest_is_corruption() {
        let damaged = |bytes: &[u8]| {
            matches!(
                read_bytes(bytes),
                Err(Error::Corruption {
                    offset: 0,
                    reason: CorruptionReason::Manifest
                })
            )
        };
        for at in (0..BASES_0_AND_21.len()).filter(|&at| at != 4) {
            let mut bytes = BASES_0_AND_21;
            bytes[at] ^= 0x01;
            assert!(damaged(&bytes), "byte {at} changed");
        }
        assert!(damaged(&BASES_0_AND_21[..32]));
        assert!(damaged(b""));
        let none = [0x54, 0x4c, 0x59, 0x4d, 3, 0, 0, 0, 0, 0, 0, 0, 0];
        assert!(damaged(
            &[&none[..], &crc32c::crc32c(&none).to_le_bytes()].concat()
        ));
        let out_of_order = encode(&[21, 0]);
        assert!(damaged(&out_of_order));
    }

    #[test]
    fn a_manifest_of_another_version_is_refused() {
        let mut bytes = BASES_0_AND_21;
        bytes[4] = 4;
        assert!(matches!(
            read_bytes(&bytes),
            Err(Error::Unsupported {
                offset: 0,
                reason: UnsupportedReason::OtherFormat
            })
        ));
    }
}
