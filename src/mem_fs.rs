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
//! `limit_free_space` makes the disk fill up: a write that doesn't fit
//! lands what does and fails with `StorageFull`, as a full disk does, and
//! truncating a file frees what it cut. `fail_truncates` makes every
//! truncate fail, changing nothing, and `fail_dir_syncs_after(n)` every
//! directory sync after the next `n`. `fail_syncs` makes every file sync
//! fail the way Linux does: the pending changes never reach the disk, even
//! after a later sync, while reads still see them; `fail_syncs_after(n)`
//! does so after the next `n`. `fail_removes_after(n)` makes every remove
//! after the next `n` fail, changing nothing. `tamper` changes what a
//! write to one path lands, as a bad disk would. `dir_syncs` counts the
//! directory syncs that succeeded.
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
    /// The file at this index in the disk's inodes lands all its pending
    /// changes, whatever `keep` says: nothing orders writes across files.
    pub whole: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Torn {
    Nothing,
    Bytes(usize),
    Zeros(usize),
    /// The write's bytes from this many on, with zeros before them: a later
    /// block of it landed and an earlier one didn't.
    Holes(usize),
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
                whole: None,
            });
            let next_write = self
                .inodes
                .iter()
                .find_map(|inode| match inode.pending.get(keep) {
                    Some(Change::Write { bytes, .. }) => Some(bytes.len()),
                    _ => None,
                });
            if let Some(len) = next_write {
                let half = len / 2;
                let tears = [1, half, len.saturating_sub(1)]
                    .into_iter()
                    .filter(|&n| n > 0 && n < len)
                    .map(Torn::Bytes)
                    .chain((half > 0).then_some(Torn::Holes(half)));
                for torn in tears {
                    cuts.push(Cut {
                        keep,
                        torn,
                        entries: 0,
                        whole: None,
                    });
                }
                cuts.push(Cut {
                    keep,
                    torn: Torn::Zeros(len),
                    entries: 0,
                    whole: None,
                });
            }
        }
        let pending: Vec<usize> = (0..self.inodes.len())
            .filter(|&i| !self.inodes[i].pending.is_empty())
            .collect();
        if pending.len() > 1 {
            for &i in &pending {
                for torn in [Torn::Nothing, Torn::Zeros(usize::MAX)] {
                    cuts.push(Cut {
                        keep: 0,
                        torn,
                        entries: 0,
                        whole: Some(i),
                    });
                }
            }
        }
        cuts
    }

    /// The disk after power fails and comes back.
    fn after(&self, cut: Cut) -> Disk {
        let inodes = self
            .inodes
            .iter()
            .enumerate()
            .map(|(i, inode)| {
                let keep = if cut.whole == Some(i) {
                    inode.pending.len()
                } else {
                    cut.keep
                };
                let mut data = inode.synced.clone();
                for change in inode.pending.iter().take(keep) {
                    apply(&mut data, change);
                }
                if let Some(Change::Write { offset, bytes }) = inode.pending.get(keep) {
                    let landed = match cut.torn {
                        Torn::Nothing => None,
                        Torn::Bytes(n) => Some(bytes[..n.min(bytes.len())].to_vec()),
                        Torn::Zeros(n) => Some(vec![0; n.min(bytes.len())]),
                        Torn::Holes(n) => {
                            let n = n.min(bytes.len());
                            Some([&vec![0; n][..], &bytes[n..]].concat())
                        }
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
    /// Bytes writes can still add, or `None` for no limit.
    free: Option<u64>,
    /// File syncs that succeed before every later one fails.
    syncs_left: Option<usize>,
    /// Directory syncs that succeed before every later one fails.
    dir_syncs_left: Option<usize>,
    dir_syncs: u64,
    fail_truncates: bool,
    /// Removes that go ahead before every later one fails.
    removes_left: Option<usize>,
    tamper: Option<(PathBuf, Tamper)>,
    /// Reads of these bytes of the file at this path fail, and whether a
    /// read that also covers other bytes fails whole.
    unreadable: Option<(PathBuf, std::ops::Range<u64>, bool)>,
}

/// Changes the bytes a write at an offset lands.
type Tamper = fn(u64, &mut [u8]);

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
            free: None,
            syncs_left: None,
            dir_syncs_left: None,
            dir_syncs: 0,
            fail_truncates: false,
            removes_left: None,
            tamper: None,
            unreadable: None,
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

    /// From now on, writes can add only `bytes` more, or any amount for
    /// `None`.
    pub(crate) fn limit_free_space(&self, bytes: Option<u64>) {
        self.0.borrow_mut().free = bytes;
    }

    pub(crate) fn fail_syncs(&self, fail: bool) {
        self.0.borrow_mut().syncs_left = fail.then_some(0);
    }

    pub(crate) fn fail_syncs_after(&self, n: usize) {
        self.0.borrow_mut().syncs_left = Some(n);
    }

    pub(crate) fn fail_removes_after(&self, n: usize) {
        self.0.borrow_mut().removes_left = Some(n);
    }

    /// From now on, `tamper` changes the bytes of each write to the file
    /// at `path`, given the write's offset.
    pub(crate) fn tamper(&self, path: &Path, tamper: Tamper) {
        self.0.borrow_mut().tamper = Some((path.to_path_buf(), tamper));
    }

    /// From now on, reads of `range` of the file at `path` fail, as a bad
    /// sector does; a read that starts before it returns the bytes up to it.
    pub(crate) fn fail_reads(&self, path: &Path, range: std::ops::Range<u64>) {
        self.0.borrow_mut().unreadable = Some((path.to_path_buf(), range, false));
    }

    /// As `fail_reads`, but any read that touches `range` fails whole, as
    /// on a disk that fails a request for one bad sector in it.
    pub(crate) fn fail_whole_reads(&self, path: &Path, range: std::ops::Range<u64>) {
        self.0.borrow_mut().unreadable = Some((path.to_path_buf(), range, true));
    }

    /// The bad range of inode `ino`, if it has one, and whether reads that
    /// touch it fail whole.
    fn unreadable(&self, ino: usize) -> Option<(std::ops::Range<u64>, bool)> {
        let state = self.0.borrow();
        let (path, range, whole) = state.unreadable.as_ref()?;
        (state.disk.files.get(path) == Some(&ino)).then(|| (range.clone(), *whole))
    }

    pub(crate) fn fail_dir_syncs(&self, fail: bool) {
        self.0.borrow_mut().dir_syncs_left = fail.then_some(0);
    }

    pub(crate) fn fail_dir_syncs_after(&self, n: usize) {
        self.0.borrow_mut().dir_syncs_left = Some(n);
    }

    pub(crate) fn fail_truncates(&self, fail: bool) {
        self.0.borrow_mut().fail_truncates = fail;
    }

    /// How much of a `len`-byte write fits, taking that space.
    fn take_space(&self, len: usize) -> usize {
        let mut state = self.0.borrow_mut();
        let Some(free) = &mut state.free else {
            return len;
        };
        let fits = len.min(usize::try_from(*free).unwrap_or(usize::MAX));
        *free -= fits as u64;
        fits
    }

    fn free_space(&self, bytes: u64) {
        if let Some(free) = &mut self.0.borrow_mut().free {
            *free += bytes;
        }
    }

    pub(crate) fn cut_before(&self, step: u64) {
        self.0.borrow_mut().cut_before = Some(step);
    }

    /// The directory syncs that have succeeded.
    pub(crate) fn dir_syncs(&self) -> u64 {
        self.0.borrow().dir_syncs
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
        match &mut state.removes_left {
            Some(0) => return Err(io::Error::other("injected remove failure")),
            Some(left) => *left -= 1,
            None => {}
        }
        let disk = &mut state.disk;
        disk.files.remove(path).ok_or(ErrorKind::NotFound)?;
        disk.entry_changes
            .push(EntryChange(vec![(path.to_path_buf(), None)]));
        Ok(())
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
        match &mut state.dir_syncs_left {
            Some(0) => return Err(io::Error::other("injected directory fsync failure")),
            Some(left) => *left -= 1,
            None => {}
        }
        state.disk.has_dir(path)?;
        state.dir_syncs += 1;
        let disk = &mut state.disk;
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
        if let Some((bad, _)) = self.fs.unreadable(self.ino)
            && offset < bad.end
            && bad.start < offset + buf.len() as u64
        {
            return Err(io::Error::other("injected read failure"));
        }
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
        let fits = self.fs.take_space(buf.len());
        let mut bytes = buf[..fits].to_vec();
        {
            let state = self.fs.0.borrow();
            if let Some((path, tamper)) = &state.tamper
                && state.disk.files.get(path) == Some(&self.ino)
            {
                tamper(offset, &mut bytes);
            }
        }
        self.fs.with_inode(self.ino, |inode| {
            assert_eq!(
                offset,
                inode.data.len() as u64,
                "LogFile writes go only at the end"
            );
            let change = Change::Write { offset, bytes };
            apply(&mut inode.data, &change);
            inode.pending.push(change);
        });
        if fits < buf.len() {
            return Err(ErrorKind::StorageFull.into());
        }
        Ok(())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.fs.step();
        if self.fs.0.borrow().fail_truncates {
            return Err(io::Error::other("injected truncate failure"));
        }
        let cut = self.fs.with_inode(self.ino, |inode| {
            (inode.data.len() as u64).saturating_sub(len)
        });
        self.fs.free_space(cut);
        self.fs.with_inode(self.ino, |inode| {
            let change = Change::SetLen(len);
            apply(&mut inode.data, &change);
            inode.pending.push(change);
        });
        Ok(())
    }

    fn sync(&self) -> io::Result<()> {
        self.fs.step();
        let failed = {
            let mut state = self.fs.0.borrow_mut();
            match &mut state.syncs_left {
                Some(0) => true,
                Some(left) => {
                    *left -= 1;
                    false
                }
                None => false,
            }
        };
        self.fs.with_inode(self.ino, |inode| {
            for change in inode.pending.drain(..) {
                if !failed {
                    apply(&mut inode.synced, &change);
                }
            }
        });
        if failed {
            return Err(io::Error::other("injected fsync failure"));
        }
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
        let mut most = buf.len();
        if let Some((bad, whole)) = self.file.fs.unreadable(self.file.ino) {
            let size = self
                .file
                .fs
                .with_inode(self.file.ino, |inode| inode.data.len() as u64);
            let end = (self.offset + most as u64).min(size);
            if bad.contains(&self.offset) || whole && self.offset < bad.end && bad.start < end {
                return Err(io::Error::other("injected read failure"));
            }
            if self.offset < bad.start {
                most = most.min((bad.start - self.offset) as usize);
            }
        }
        let n = self.file.fs.with_inode(self.file.ino, |inode| {
            let start = (self.offset as usize).min(inode.data.len());
            let src = &inode.data[start..];
            let n = src.len().min(most);
            buf[..n].copy_from_slice(&src[..n]);
            n
        });
        self.offset += n as u64;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_write_that_does_not_fit_lands_what_does() {
        let fs = MemFs::new();
        let path = Path::new("/f");
        let file = fs.open_or_create(path).unwrap();
        fs.limit_free_space(Some(5));
        file.write_all_at(b"abc", 0).unwrap();
        let err = file.write_all_at(b"defg", 3).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::StorageFull);
        assert_eq!(fs.read(path).unwrap(), b"abcde");

        file.set_len(2).unwrap();
        file.write_all_at(b"xyz", 2).unwrap();
        assert_eq!(fs.read(path).unwrap(), b"abxyz");
        let err = file.write_all_at(b"!", 5).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::StorageFull);
        assert_eq!(fs.read(path).unwrap(), b"abxyz");

        fs.limit_free_space(None);
        file.write_all_at(b"!", 5).unwrap();
        assert_eq!(fs.read(path).unwrap(), b"abxyz!");
    }

    #[test]
    fn a_failed_truncate_changes_nothing() {
        let fs = MemFs::new();
        let path = Path::new("/f");
        let file = fs.open_or_create(path).unwrap();
        file.write_all_at(b"abc", 0).unwrap();
        file.sync().unwrap();
        fs.fail_truncates(true);
        assert_eq!(
            file.set_len(1).unwrap_err().to_string(),
            "injected truncate failure"
        );
        assert_eq!(fs.read(path).unwrap(), b"abc");
        assert_eq!(fs.disk().inodes[0].synced, b"abc");
    }

    #[test]
    fn a_failed_sync_loses_its_writes_from_the_disk_but_not_from_reads() {
        let fs = MemFs::new();
        let path = Path::new("/f");
        let file = fs.open_or_create(path).unwrap();
        file.write_all_at(b"abc", 0).unwrap();
        fs.fail_syncs(true);
        assert_eq!(
            file.sync().unwrap_err().to_string(),
            "injected fsync failure"
        );
        fs.fail_syncs(false);
        file.write_all_at(b"d", 3).unwrap();
        file.sync().unwrap();
        assert_eq!(fs.read(path).unwrap(), b"abcd");
        assert_eq!(fs.disk().inodes[0].synced, b"\0\0\0d");
    }

    #[test]
    fn a_failed_directory_sync_leaves_the_entry_unsynced() {
        let fs = MemFs::new();
        fs.open_or_create(Path::new("/f")).unwrap();
        fs.fail_dir_syncs(true);
        assert_eq!(
            fs.sync_dir(Path::new("/")).unwrap_err().to_string(),
            "injected directory fsync failure"
        );
        assert!(fs.disk().durable_files.is_empty());
    }

    #[test]
    fn a_cut_can_land_one_file_whole_and_nothing_of_another() {
        let fs = MemFs::new();
        let a = fs.open_or_create(Path::new("/a")).unwrap();
        let b = fs.open_or_create(Path::new("/b")).unwrap();
        fs.sync_dir(Path::new("/")).unwrap();
        a.write_all_at(b"1", 0).unwrap();
        a.write_all_at(b"2", 1).unwrap();
        b.write_all_at(b"3", 0).unwrap();
        let disk = fs.disk();
        let landed: Vec<(Vec<u8>, Vec<u8>)> = disk
            .cuts()
            .into_iter()
            .filter(|cut| cut.whole.is_some())
            .map(|cut| {
                let after = MemFs::after(&disk, cut);
                (
                    after.read(Path::new("/a")).unwrap(),
                    after.read(Path::new("/b")).unwrap(),
                )
            })
            .collect();
        assert_eq!(
            landed,
            [
                (b"12".to_vec(), b"".to_vec()),
                (b"12".to_vec(), b"\0".to_vec()),
                (b"".to_vec(), b"3".to_vec()),
                (b"\0".to_vec(), b"3".to_vec()),
            ]
        );
    }

    #[test]
    fn a_torn_write_can_land_its_end_without_its_start() {
        let fs = MemFs::new();
        let a = fs.open_or_create(Path::new("/a")).unwrap();
        fs.sync_dir(Path::new("/")).unwrap();
        a.write_all_at(b"1234", 0).unwrap();
        let disk = fs.disk();
        let mut landed: Vec<Vec<u8>> = disk
            .cuts()
            .into_iter()
            .map(|cut| MemFs::after(&disk, cut).read(Path::new("/a")).unwrap())
            .collect();
        landed.sort();
        landed.dedup();
        assert_eq!(
            landed,
            [
                b"".to_vec(),
                b"\0\0\0\0".to_vec(),
                b"\0\0\x334".to_vec(),
                b"1".to_vec(),
                b"12".to_vec(),
                b"123".to_vec(),
                b"1234".to_vec(),
            ]
        );
    }
}
