//! An in-memory [`Fs`] for the power-cut tests.
//!
//! Each file keeps what reads see now apart from what was synced, plus the
//! changes made since the last sync. Directory entries likewise exist now,
//! and are durable once their directory is synced. A power cut keeps, for
//! each file, its synced contents plus a prefix of the pending changes, the
//! last one possibly torn: cut short, or left as zeros because the file's
//! new size reached the disk before its data did. It keeps the durable
//! entries plus any of the changes to entries made since (a create, remove
//! or rename, each landing whole), in any combination that keeps the
//! changes to each name in order. Directories themselves are created
//! durable only by a sync of their parent.
//!
//! Every mutation and sync counts as a step. `cut_before(n)` snapshots the
//! disk just before step `n` runs, as if power failed there, and the run
//! carries on unaffected, so one run yields a snapshot at any point.

#![cfg_attr(not(test), allow(dead_code))]

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::TryLockError;
use std::io::{self, ErrorKind, Read};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use crate::fs::sealed::Sealed;
use crate::fs::{Fs, LogFile};

#[derive(Debug, Clone)]
enum Change {
    Write { offset: u64, bytes: Vec<u8> },
    SetLen(u64),
}

#[derive(Debug, Clone, Default)]
struct Inode {
    data: Vec<u8>,
    synced: Vec<u8>,
    pending: Vec<Change>,
}

/// Names a create, remove or rename sets, to a file or to nothing. They land
/// together or not at all.
#[derive(Debug, Clone)]
struct EntryChange(Vec<(PathBuf, Option<usize>)>);

/// Above this many unsynced entry changes, `cuts` would try too many
/// combinations to be worth running.
const MOST_ENTRY_CHANGES: usize = 12;

/// The whole disk at one moment.
#[derive(Debug, Clone)]
pub(crate) struct Disk {
    dirs: BTreeSet<PathBuf>,
    files: BTreeMap<PathBuf, usize>,
    durable_dirs: BTreeSet<PathBuf>,
    durable_files: BTreeMap<PathBuf, usize>,
    /// Changes to `files` since `durable_files`, oldest first.
    entry_changes: Vec<EntryChange>,
    inodes: Vec<Inode>,
}

/// What a power cut keeps of each file's pending changes, and of the
/// directories'.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Cut {
    /// How many pending changes land whole.
    pub keep: usize,
    /// What lands of the next one, if it is a write.
    pub torn: Torn,
    /// Bit `i` set: the `i`th unsynced entry change lands.
    pub entries: u16,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Torn {
    Nothing,
    Bytes(usize),
    Zeros(usize),
}

impl Disk {
    fn new() -> Disk {
        let root = BTreeSet::from([PathBuf::from("/")]);
        Disk {
            dirs: root.clone(),
            files: BTreeMap::new(),
            durable_dirs: root,
            durable_files: BTreeMap::new(),
            entry_changes: Vec::new(),
            inodes: Vec::new(),
        }
    }

    /// Each set of unsynced entry changes that can land: any, as long as no
    /// change lands after one to the same name that didn't.
    fn entry_landings(&self) -> Vec<u16> {
        let changes = &self.entry_changes;
        assert!(
            changes.len() <= MOST_ENTRY_CHANGES,
            "{} unsynced entry changes",
            changes.len()
        );
        let shares_a_name = |a: &EntryChange, b: &EntryChange| {
            a.0.iter().any(|(p, _)| b.0.iter().any(|(q, _)| p == q))
        };
        (0..1u16 << changes.len())
            .filter(|mask| {
                (0..changes.len()).all(|i| {
                    mask & (1 << i) != 0
                        || (i + 1..changes.len()).all(|j| {
                            mask & (1 << j) == 0 || !shares_a_name(&changes[i], &changes[j])
                        })
                })
            })
            .collect()
    }

    /// Every cut worth trying on this disk: each number of whole changes,
    /// and for a following write, landing short, half, almost whole, or as
    /// zeros; each with every set of entry changes that can land.
    pub(crate) fn cuts(&self) -> Vec<Cut> {
        let landings = self.entry_landings();
        self.file_cuts()
            .into_iter()
            .flat_map(|cut| landings.iter().map(move |&entries| Cut { entries, ..cut }))
            .collect()
    }

    fn file_cuts(&self) -> Vec<Cut> {
        let most = self
            .inodes
            .iter()
            .map(|i| i.pending.len())
            .max()
            .unwrap_or(0);
        let mut cuts = Vec::new();
        for keep in 0..=most {
            cuts.push(Cut {
                keep,
                torn: Torn::Nothing,
                entries: 0,
            });
            let next_write = self
                .inodes
                .iter()
                .find_map(|inode| match inode.pending.get(keep) {
                    Some(Change::Write { bytes, .. }) => Some(bytes.len()),
                    _ => None,
                });
            if let Some(len) = next_write {
                for n in [1, len / 2, len.saturating_sub(1)] {
                    if n > 0 && n < len {
                        cuts.push(Cut {
                            keep,
                            torn: Torn::Bytes(n),
                            entries: 0,
                        });
                    }
                }
                cuts.push(Cut {
                    keep,
                    torn: Torn::Zeros(len),
                    entries: 0,
                });
            }
        }
        cuts
    }

    /// The disk after power fails and comes back.
    fn after(&self, cut: Cut) -> Disk {
        let inodes = self
            .inodes
            .iter()
            .map(|inode| {
                let mut data = inode.synced.clone();
                for change in inode.pending.iter().take(cut.keep) {
                    apply(&mut data, change);
                }
                if let Some(Change::Write { offset, bytes }) = inode.pending.get(cut.keep) {
                    let landed = match cut.torn {
                        Torn::Nothing => None,
                        Torn::Bytes(n) => Some(bytes[..n.min(bytes.len())].to_vec()),
                        Torn::Zeros(n) => Some(vec![0; n.min(bytes.len())]),
                    };
                    if let Some(bytes) = landed {
                        apply(
                            &mut data,
                            &Change::Write {
                                offset: *offset,
                                bytes,
                            },
                        );
                    }
                }
                Inode {
                    synced: data.clone(),
                    data,
                    pending: Vec::new(),
                }
            })
            .collect();
        // A durable name under a directory whose own name was lost is lost
        // with it.
        let reachable = |path: &Path| {
            path.ancestors()
                .skip(1)
                .all(|dir| self.durable_dirs.contains(dir))
        };
        let dirs: BTreeSet<PathBuf> = self
            .durable_dirs
            .iter()
            .filter(|d| reachable(d))
            .cloned()
            .collect();
        let mut landed = self.durable_files.clone();
        for (i, change) in self.entry_changes.iter().enumerate() {
            if cut.entries & (1 << i) != 0 {
                for (path, entry) in &change.0 {
                    match entry {
                        Some(ino) => landed.insert(path.clone(), *ino),
                        None => landed.remove(path),
                    };
                }
            }
        }
        let files: BTreeMap<PathBuf, usize> =
            landed.into_iter().filter(|(p, _)| reachable(p)).collect();
        Disk {
            durable_dirs: dirs.clone(),
            durable_files: files.clone(),
            dirs,
            files,
            entry_changes: Vec::new(),
            inodes,
        }
    }

    fn has_dir(&self, path: &Path) -> io::Result<()> {
        if self.dirs.contains(path) {
            Ok(())
        } else {
            Err(ErrorKind::NotFound.into())
        }
    }
}

fn apply(data: &mut Vec<u8>, change: &Change) {
    match change {
        Change::Write { offset, bytes } => {
            let start = *offset as usize;
            if data.len() < start + bytes.len() {
                data.resize(start + bytes.len(), 0);
            }
            data[start..start + bytes.len()].copy_from_slice(bytes);
        }
        Change::SetLen(len) => data.resize(*len as usize, 0),
    }
}

struct State {
    disk: Disk,
    steps: u64,
    cut_before: Option<u64>,
    snapshot: Option<Disk>,
}

#[derive(Clone)]
pub struct MemFs(Rc<RefCell<State>>);

impl MemFs {
    /// An empty disk holding only the root directory `/`.
    pub fn new() -> MemFs {
        MemFs::on(Disk::new())
    }

    /// The disk `snapshot` leaves after power fails at it and comes back.
    pub(crate) fn after(snapshot: &Disk, cut: Cut) -> MemFs {
        MemFs::on(snapshot.after(cut))
    }

    fn on(disk: Disk) -> MemFs {
        MemFs(Rc::new(RefCell::new(State {
            disk,
            steps: 0,
            cut_before: None,
            snapshot: None,
        })))
    }

    /// Writes `bytes` to `path` and makes it durable, directories included.
    pub(crate) fn put(&self, path: &Path, bytes: &[u8]) {
        let mut state = self.0.borrow_mut();
        let disk = &mut state.disk;
        for dir in path.ancestors().skip(1) {
            disk.dirs.insert(dir.to_path_buf());
            disk.durable_dirs.insert(dir.to_path_buf());
        }
        disk.inodes.push(Inode {
            data: bytes.to_vec(),
            synced: bytes.to_vec(),
            pending: Vec::new(),
        });
        let ino = disk.inodes.len() - 1;
        disk.files.insert(path.to_path_buf(), ino);
        disk.durable_files.insert(path.to_path_buf(), ino);
    }

    /// The current contents of `path`, if it exists.
    pub fn read(&self, path: &Path) -> Option<Vec<u8>> {
        let state = self.0.borrow();
        let ino = *state.disk.files.get(path)?;
        Some(state.disk.inodes[ino].data.clone())
    }

    pub(crate) fn cut_before(&self, step: u64) {
        self.0.borrow_mut().cut_before = Some(step);
    }

    pub(crate) fn steps(&self) -> u64 {
        self.0.borrow().steps
    }

    pub(crate) fn snapshot(&self) -> Option<Disk> {
        self.0.borrow().snapshot.clone()
    }

    /// The disk as it is now, to cut the power at this moment.
    pub(crate) fn disk(&self) -> Disk {
        self.0.borrow().disk.clone()
    }

    /// Counts one step, snapshotting first if the cut is due.
    fn step(&self) {
        let mut state = self.0.borrow_mut();
        state.steps += 1;
        if state.cut_before == Some(state.steps) {
            state.snapshot = Some(state.disk.clone());
        }
    }

    fn file(&self, ino: usize) -> MemFile {
        MemFile {
            fs: self.clone(),
            ino,
        }
    }

    fn with_inode<T>(&self, ino: usize, f: impl FnOnce(&mut Inode) -> T) -> T {
        f(&mut self.0.borrow_mut().disk.inodes[ino])
    }
}

impl Sealed for MemFs {}

impl Fs for MemFs {
    type File = MemFile;

    fn create_dir(&self, path: &Path) -> io::Result<()> {
        self.step();
        let mut state = self.0.borrow_mut();
        let disk = &mut state.disk;
        disk.has_dir(path.parent().ok_or(ErrorKind::AlreadyExists)?)?;
        if disk.dirs.contains(path) || disk.files.contains_key(path) {
            return Err(ErrorKind::AlreadyExists.into());
        }
        disk.dirs.insert(path.to_path_buf());
        Ok(())
    }

    fn open_or_create(&self, path: &Path) -> io::Result<MemFile> {
        if let Some(&ino) = self.0.borrow().disk.files.get(path) {
            return Ok(self.file(ino));
        }
        self.step();
        let mut state = self.0.borrow_mut();
        let disk = &mut state.disk;
        disk.has_dir(path.parent().ok_or(ErrorKind::InvalidInput)?)?;
        disk.inodes.push(Inode::default());
        let ino = disk.inodes.len() - 1;
        disk.files.insert(path.to_path_buf(), ino);
        disk.entry_changes
            .push(EntryChange(vec![(path.to_path_buf(), Some(ino))]));
        drop(state);
        Ok(self.file(ino))
    }

    fn open_existing(&self, path: &Path) -> io::Result<MemFile> {
        let ino = *self
            .0
            .borrow()
            .disk
            .files
            .get(path)
            .ok_or(ErrorKind::NotFound)?;
        Ok(self.file(ino))
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.step();
        let mut state = self.0.borrow_mut();
        let disk = &mut state.disk;
        assert_eq!(from.parent(), to.parent(), "renames stay in one directory");
        let ino = disk.files.remove(from).ok_or(ErrorKind::NotFound)?;
        disk.files.insert(to.to_path_buf(), ino);
        disk.entry_changes.push(EntryChange(vec![
            (from.to_path_buf(), None),
            (to.to_path_buf(), Some(ino)),
        ]));
        Ok(())
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        self.step();
        let mut state = self.0.borrow_mut();
        let disk = &mut state.disk;
        disk.files.remove(path).ok_or(ErrorKind::NotFound)?;
        disk.entry_changes
            .push(EntryChange(vec![(path.to_path_buf(), None)]));
        Ok(())
    }

    fn hard_link(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.step();
        let mut state = self.0.borrow_mut();
        let disk = &mut state.disk;
        assert_eq!(from.parent(), to.parent(), "links stay in one directory");
        if disk.files.contains_key(to) {
            return Err(ErrorKind::AlreadyExists.into());
        }
        let ino = *disk.files.get(from).ok_or(ErrorKind::NotFound)?;
        disk.files.insert(to.to_path_buf(), ino);
        disk.entry_changes
            .push(EntryChange(vec![(to.to_path_buf(), Some(ino))]));
        Ok(())
    }

    fn same_file(&self, a: &Path, b: &Path) -> io::Result<bool> {
        let state = self.0.borrow();
        let ino = |path| state.disk.files.get(path).ok_or(ErrorKind::NotFound);
        Ok(ino(a)? == ino(b)?)
    }

    fn list(&self, path: &Path) -> io::Result<Vec<String>> {
        let state = self.0.borrow();
        state.disk.has_dir(path)?;
        Ok(state
            .disk
            .files
            .keys()
            .filter(|file| file.parent() == Some(path))
            .filter_map(|file| Some(file.file_name()?.to_str()?.to_owned()))
            .collect())
    }

    /// The directory's durable entries become exactly its current ones, so
    /// creations, replacements, renames away and removals all land together.
    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        self.step();
        let mut state = self.0.borrow_mut();
        let disk = &mut state.disk;
        disk.has_dir(path)?;
        let in_dir = |p: &Path| p.parent() == Some(path);
        disk.durable_dirs.retain(|d| !in_dir(d));
        disk.durable_dirs
            .extend(disk.dirs.iter().filter(|d| in_dir(d)).cloned());
        disk.durable_files.retain(|p, _| !in_dir(p));
        disk.durable_files.extend(
            disk.files
                .iter()
                .filter(|(p, _)| in_dir(p))
                .map(|(p, &ino)| (p.clone(), ino)),
        );
        disk.entry_changes
            .retain(|change| !change.0.iter().all(|(p, _)| in_dir(p)));
        Ok(())
    }
}

pub struct MemFile {
    fs: MemFs,
    ino: usize,
}

impl Sealed for MemFile {}

impl LogFile for MemFile {
    fn size(&self) -> io::Result<u64> {
        Ok(self
            .fs
            .with_inode(self.ino, |inode| inode.data.len() as u64))
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.fs.with_inode(self.ino, |inode| {
            let start = offset as usize;
            let src = inode
                .data
                .get(start..start + buf.len())
                .ok_or(ErrorKind::UnexpectedEof)?;
            buf.copy_from_slice(src);
            Ok(())
        })
    }

    fn reader_at(&self, offset: u64) -> impl Read + '_ {
        MemReader { file: self, offset }
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.fs.step();
        self.fs.with_inode(self.ino, |inode| {
            assert_eq!(
                offset,
                inode.data.len() as u64,
                "LogFile writes go only at the end"
            );
            let change = Change::Write {
                offset,
                bytes: buf.to_vec(),
            };
            apply(&mut inode.data, &change);
            inode.pending.push(change);
        });
        Ok(())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.fs.step();
        self.fs.with_inode(self.ino, |inode| {
            let change = Change::SetLen(len);
            apply(&mut inode.data, &change);
            inode.pending.push(change);
        });
        Ok(())
    }

    fn sync(&self) -> io::Result<()> {
        self.fs.step();
        self.fs.with_inode(self.ino, |inode| {
            inode.synced = inode.data.clone();
            inode.pending.clear();
        });
        Ok(())
    }

    /// One process, so there is no one to lock out.
    fn try_lock(&self) -> Result<(), TryLockError> {
        Ok(())
    }
}

struct MemReader<'a> {
    file: &'a MemFile,
    offset: u64,
}

impl Read for MemReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.file.fs.with_inode(self.file.ino, |inode| {
            let start = (self.offset as usize).min(inode.data.len());
            let src = &inode.data[start..];
            let n = src.len().min(buf.len());
            buf[..n].copy_from_slice(&src[..n]);
            n
        });
        self.offset += n as u64;
        Ok(n)
    }
}
