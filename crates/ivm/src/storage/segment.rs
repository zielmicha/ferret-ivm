//! Immutable, append-only segment files holding serialized nodes.
//!
//! Each checkpoint appends the nodes dirtied since the previous one to the
//! active segment (as do early writes of dirty nodes); sealed segments never
//! change, which makes continuous incremental backup a matter of copying
//! new files.
//!
//! A store keeps a bounded number of segment files open for reading, which
//! every version of it shares, and opens a file again when it needs it: a
//! store of any size and any number of forks holds a fixed number of
//! descriptors. Nothing depends on a file staying open, so a dead segment
//! that a fork may still read stays on disk until the fork is dropped
//! ([`SegmentStore::delete`]).

use super::node::DiskLoc;
use crate::error::{Result, SqlError};
use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const FRAME_HEADER: usize = 8; // u32 len, u32 crc

/// The segment files of a store as one environment sees them: read access
/// (`SegmentReader`) and the append side (`SharedWriter`), which every
/// version of the store shares — the environment, its checkpoint job and
/// its forks read through the same open files and append to the same
/// active segment, each frame in one piece.
pub struct SegmentStore {
    reader: Arc<SegmentReader>,
    writer: Arc<SharedWriter>,
    /// In a fork: its number among the store's forks (`SegmentWriter::forks`)
    /// and the segments it appended to, pinned against deletion until it is
    /// dropped (its trees may be adopted, frames and all).
    fork: Option<(u64, BTreeSet<u32>)>,
}

/// Read access to segment files by location, through at most `cap` open
/// files: a file is opened on first use and closed again, the least
/// recently used first, to open another beyond the bound.
struct SegmentReader {
    dir: PathBuf,
    files: Mutex<OpenFiles>,
}

/// The open files of a `SegmentReader`, replaced in CLOCK order.
struct OpenFiles {
    cap: usize,
    /// (segment, file, used since the hand last passed it)
    open: Vec<(u32, Arc<File>, bool)>,
    /// Segment -> its position in `open`.
    at: FxHashMap<u32, usize>,
    hand: usize,
    /// Files opened so far.
    opened: u64,
}

/// The append side shared by every version of a store.
struct SharedWriter {
    /// The active segment and how much of it is in its file
    /// (`id << 40 | length`): frames before that are read from the file,
    /// the others from the append buffer.
    written: AtomicU64,
    w: Mutex<SegmentWriter>,
}

/// Appends node frames to the active segment and rolls over to the next.
/// It also tracks what the forks of the store keep on disk.
struct SegmentWriter {
    dir: PathBuf,
    active_id: u32,
    active: Arc<File>,
    /// Length handed to the OS (written).
    written_len: u64,
    /// Pending bytes not yet written.
    buf: Vec<u8>,
    max_segment_bytes: u64,
    /// Segments rolled since the last sync that still need fsync (their
    /// files are closed: a sync opens them again).
    unsynced: Vec<u32>,
    /// Segments forks appended to, with the number of forks pinning each.
    pinned: BTreeMap<u32, usize>,
    /// Bytes forks appended per segment, not yet counted in the totals of
    /// the environment they were forked from (`take_foreign`).
    foreign: BTreeMap<u32, u64>,
    /// Lengths of the segments this process sealed (rolled over from).
    sealed: BTreeMap<u32, u64>,
    /// The live forks by number (in the order they were taken), each with
    /// the active segment when it was taken: it may read the segments up
    /// to that one, those that held its nodes.
    forks: BTreeMap<u64, u32>,
    next_fork: u64,
    /// Dead segments that a live fork may still read, each with the number
    /// of the next fork when it was found dead (the forks taken from then
    /// on cannot read it): deleted once no fork before that remains whose
    /// segments reach it.
    deferred: Vec<(u32, u64)>,
}

/// How many segment files a store keeps open for reading unless told
/// otherwise (`EnvOptions::max_open_segments`): a quarter of the process's
/// limit on open files (`RLIMIT_NOFILE`, its soft limit), at least 8 and at
/// most 65,536. The rest of the limit is left to connections, the input
/// log and the like.
pub fn default_max_open() -> usize {
    let mut lim = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
    // SAFETY: getrlimit only writes the struct it is given.
    let soft = if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) } == 0 { lim.rlim_cur } else { 1024 };
    (soft / 4).clamp(8, 1 << 16) as usize
}

pub fn segment_path(dir: &Path, id: u32) -> PathBuf {
    dir.join(format!("seg-{id:08}.dat"))
}

fn pack(id: u32, len: u64) -> u64 {
    (id as u64) << 40 | len
}

impl SegmentStore {
    /// Open the store with `active_id` as the active segment, truncated to
    /// `active_len`, reading through at most `max_open` open files.
    pub fn open(dir: &Path, active_id: u32, active_len: u64, max_segment_bytes: u64, max_open: usize) -> Result<SegmentStore> {
        std::fs::create_dir_all(dir)?;
        let path = segment_path(dir, active_id);
        let created = !path.exists();
        let f = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(&path)?;
        f.set_len(active_len)?;
        if created {
            File::open(dir)?.sync_all()?;
        }
        let w = SegmentWriter {
            dir: dir.to_path_buf(),
            active_id,
            active: Arc::new(f),
            written_len: active_len,
            buf: Vec::new(),
            max_segment_bytes,
            unsynced: Vec::new(),
            pinned: BTreeMap::new(),
            foreign: BTreeMap::new(),
            sealed: BTreeMap::new(),
            forks: BTreeMap::new(),
            next_fork: 0,
            deferred: Vec::new(),
        };
        let writer = Arc::new(SharedWriter { written: AtomicU64::new(pack(active_id, active_len)), w: Mutex::new(w) });
        let files = OpenFiles { cap: max_open.max(1), open: Vec::new(), at: FxHashMap::default(), hand: 0, opened: 0 };
        let reader = Arc::new(SegmentReader { dir: dir.to_path_buf(), files: Mutex::new(files) });
        Ok(SegmentStore { reader, writer, fork: None })
    }

    pub fn active_id(&self) -> u32 {
        (self.writer.written.load(Ordering::Acquire) >> 40) as u32
    }

    /// Append a node payload; returns its location.
    pub fn append(&mut self, payload: &[u8]) -> Result<DiskLoc> {
        let mut w = self.writer.w.lock();
        let loc = w.append(payload, &self.writer.written)?;
        if let Some((_, pins)) = &mut self.fork {
            *w.foreign.entry(loc.seg).or_default() += loc.len as u64;
            if pins.insert(loc.seg) {
                *w.pinned.entry(loc.seg).or_default() += 1;
            }
        }
        Ok(loc)
    }

    /// Make everything appended so far (by any version of the store)
    /// durable; returns the active segment and the length of it that is,
    /// with the lengths of the segments sealed in this process (every
    /// version's appends counted). The files are synced outside the lock:
    /// others go on appending meanwhile, after that length.
    pub fn sync(&self) -> Result<(u32, u64, BTreeMap<u32, u64>)> {
        let (rolled, active, durable) = {
            let mut w = self.writer.w.lock();
            w.write_buffer(&self.writer.written)?;
            (std::mem::take(&mut w.unsynced), w.active.clone(), (w.active_id, w.written_len, w.sealed.clone()))
        };
        // (a write-back error of a file is reported to the next fsync
        // through any descriptor that has not seen it, also one opened
        // after the error, as on Linux since 4.13)
        for id in rolled {
            OpenOptions::new().write(true).open(segment_path(&self.reader.dir, id))?.sync_data()?;
        }
        active.sync_data()?;
        Ok(durable)
    }

    /// Another view of this store, with its read access and append side
    /// (for a checkpoint job).
    pub fn share(&self) -> SegmentStore {
        SegmentStore { reader: self.reader.clone(), writer: self.writer.clone(), fork: None }
    }

    /// The store as a fork of the environment sees it: until it is
    /// dropped, the segments it may read — those up to the active one, as
    /// they hold every node it inherits — stay on disk once dead
    /// (`delete`), and so do the segments it appends to.
    pub fn fork(&self) -> SegmentStore {
        let mut w = self.writer.w.lock();
        let n = w.next_fork;
        w.next_fork += 1;
        let active = w.active_id;
        w.forks.insert(n, active);
        SegmentStore { reader: self.reader.clone(), writer: self.writer.clone(), fork: Some((n, BTreeSet::new())) }
    }

    pub fn read(&self, loc: DiskLoc) -> Result<Vec<u8>> {
        let written = self.writer.written.load(Ordering::Acquire);
        let (seg, len) = ((written >> 40) as u32, written & ((1 << 40) - 1));
        if loc.seg == seg && loc.off + loc.len as u64 > len {
            // (a frame written early may still be in the append buffer)
            let w = self.writer.w.lock();
            if loc.seg == w.active_id && loc.off >= w.written_len {
                let at = (loc.off - w.written_len) as usize;
                return check_frame(w.buf[at..at + loc.len as usize].to_vec(), loc);
            }
        }
        self.reader.read(loc)
    }

    /// Hand the pending bytes to the OS (without syncing them).
    #[cfg(test)]
    pub(super) fn write_buffer(&self) -> Result<()> {
        self.writer.w.lock().write_buffer(&self.writer.written)
    }

    /// The length of the active segment, written or pending.
    #[cfg(test)]
    pub(super) fn active_len(&self) -> u64 {
        let w = self.writer.w.lock();
        w.written_len + w.buf.len() as u64
    }

    /// Delete sealed segment `id`, dead in the environment. Returns whether
    /// the environment is done with it: it is deleted now, or, while a
    /// fork that may read it lives (one taken when `id` was the active
    /// segment or an older one), once no such fork remains. It is kept,
    /// and the environment tries again later, if it is the active segment
    /// or a fork appended to it: that fork's trees may still be adopted,
    /// with nodes in it.
    pub fn delete(&self, id: u32) -> Result<bool> {
        let mut w = self.writer.w.lock();
        if id == w.active_id || w.pinned.contains_key(&id) {
            return Ok(false);
        }
        w.sealed.remove(&id);
        w.foreign.remove(&id);
        if w.forks.values().any(|active| *active >= id) {
            let next = w.next_fork;
            w.deferred.push((id, next));
            return Ok(true);
        }
        drop(w);
        self.reader.unlink(id)?;
        Ok(true)
    }

    /// Does a fork pin segment `id` (did it append to it)?
    #[cfg(test)]
    pub(super) fn pinned(&self, id: u32) -> bool {
        self.writer.w.lock().pinned.contains_key(&id)
    }

    /// Dead segments kept on disk for the forks that may read them.
    #[cfg(test)]
    pub(super) fn deferred(&self) -> Vec<u32> {
        self.writer.w.lock().deferred.iter().map(|(id, _)| *id).collect()
    }

    /// The process dies: forks dropped from now on delete nothing.
    #[cfg(test)]
    pub(super) fn crash(&self) {
        self.writer.w.lock().deferred.clear();
    }

    /// Files open for reading, and opened so far.
    #[cfg(test)]
    pub(super) fn open_files(&self) -> (usize, u64) {
        let f = self.reader.files.lock();
        (f.open.len(), f.opened)
    }

    /// The bound on the files open for reading.
    #[cfg(test)]
    pub(super) fn max_open(&self) -> usize {
        self.reader.files.lock().cap
    }

    /// Bytes that forks appended since the last call, per segment.
    pub fn take_foreign(&self) -> BTreeMap<u32, u64> {
        std::mem::take(&mut self.writer.w.lock().foreign)
    }

    /// Segment ids present on disk.
    pub fn list(&self) -> Result<Vec<u32>> {
        let mut ids = Vec::new();
        for e in std::fs::read_dir(&self.reader.dir)? {
            let name = e?.file_name();
            let name = name.to_string_lossy();
            if let Some(n) = name.strip_prefix("seg-").and_then(|s| s.strip_suffix(".dat"))
                && let Ok(id) = n.parse::<u32>()
            {
                ids.push(id);
            }
        }
        ids.sort();
        Ok(ids)
    }
}

impl Drop for SegmentStore {
    /// A fork releases what it kept on disk, and deletes the dead segments
    /// no remaining fork may read.
    fn drop(&mut self) {
        let Some((n, pins)) = self.fork.take() else { return };
        let gone: Vec<(u32, u64)> = {
            let mut guard = self.writer.w.lock();
            let w = &mut *guard;
            w.forks.remove(&n);
            for id in pins {
                if let Some(c) = w.pinned.get_mut(&id) {
                    *c -= 1;
                    if *c == 0 {
                        w.pinned.remove(&id);
                    }
                }
            }
            let forks = &w.forks;
            let (kept, gone) = w.deferred.drain(..).partition(|(id, next)| forks.range(..*next).any(|(_, active)| active >= id));
            w.deferred = kept;
            gone
        };
        for (id, _) in gone {
            if let Err(e) = self.reader.unlink(id) {
                // (the next start deletes it: dead in the manifest, or not in it)
                log::warn!("cannot delete dead segment {id}: {e}");
            }
        }
    }
}

impl SegmentWriter {
    fn append(&mut self, payload: &[u8], written: &AtomicU64) -> Result<DiskLoc> {
        if self.written_len + self.buf.len() as u64 >= self.max_segment_bytes {
            self.roll(written)?;
        }
        let off = self.written_len + self.buf.len() as u64;
        let len = (payload.len() + FRAME_HEADER) as u32;
        self.buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        self.buf.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
        self.buf.extend_from_slice(payload);
        if self.buf.len() > 4 << 20 {
            self.write_buffer(written)?;
        }
        Ok(DiskLoc { seg: self.active_id, off, len })
    }

    fn write_buffer(&mut self, written: &AtomicU64) -> Result<()> {
        if !self.buf.is_empty() {
            self.active.write_all_at(&self.buf, self.written_len)?;
            self.written_len += self.buf.len() as u64;
            self.buf.clear();
            written.store(pack(self.active_id, self.written_len), Ordering::Release);
        }
        Ok(())
    }

    fn roll(&mut self, written: &AtomicU64) -> Result<()> {
        self.write_buffer(written)?;
        self.active = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(segment_path(&self.dir, self.active_id + 1))?,
        );
        self.unsynced.push(self.active_id);
        self.sealed.insert(self.active_id, self.written_len);
        self.active_id += 1;
        self.written_len = 0;
        written.store(pack(self.active_id, 0), Ordering::Release);
        // the new file must exist after a crash once a checkpoint refers to it
        File::open(&self.dir)?.sync_all()?;
        Ok(())
    }
}

impl SegmentReader {
    fn file(&self, id: u32) -> Result<Arc<File>> {
        if let Some(f) = self.files.lock().get(id) {
            return Ok(f);
        }
        // (opened outside the lock; should another thread open it
        // meanwhile, one of the two is closed)
        let f = Arc::new(
            File::open(segment_path(&self.dir, id)).map_err(|e| SqlError::corrupt(format!("cannot open segment {id}: {e}")))?,
        );
        let mut files = self.files.lock();
        if let Some(f) = files.get(id) {
            return Ok(f);
        }
        let closed = files.insert(id, f.clone());
        drop(files);
        drop(closed);
        Ok(f)
    }

    /// Read and check the frame at `loc`, which is in its file.
    fn read(&self, loc: DiskLoc) -> Result<Vec<u8>> {
        let mut frame = vec![0u8; loc.len as usize];
        self.file(loc.seg)?.read_exact_at(&mut frame, loc.off)?;
        check_frame(frame, loc)
    }

    /// Close segment `id`'s file and delete it.
    fn unlink(&self, id: u32) -> Result<()> {
        let closed = self.files.lock().remove(id);
        drop(closed);
        let p = segment_path(&self.dir, id);
        if p.exists() {
            std::fs::remove_file(p)?;
        }
        Ok(())
    }
}

impl OpenFiles {
    fn get(&mut self, id: u32) -> Option<Arc<File>> {
        let i = *self.at.get(&id)?;
        self.open[i].2 = true;
        Some(self.open[i].1.clone())
    }

    /// Keep `f` open as segment `id`'s file; beyond the bound, in place of
    /// the first file the hand finds unused since it last passed, which is
    /// returned (to be closed outside the lock; a reader still using it
    /// keeps it open until it is done).
    fn insert(&mut self, id: u32, f: Arc<File>) -> Option<Arc<File>> {
        self.opened += 1;
        if self.open.len() < self.cap {
            self.at.insert(id, self.open.len());
            self.open.push((id, f, true));
            return None;
        }
        while std::mem::replace(&mut self.open[self.hand].2, false) {
            self.hand = (self.hand + 1) % self.open.len();
        }
        let (old, closed, _) = std::mem::replace(&mut self.open[self.hand], (id, f, true));
        self.at.remove(&old);
        self.at.insert(id, self.hand);
        self.hand = (self.hand + 1) % self.open.len();
        Some(closed)
    }

    fn remove(&mut self, id: u32) -> Option<Arc<File>> {
        let i = self.at.remove(&id)?;
        let (_, f, _) = self.open.swap_remove(i);
        if let Some((moved, _, _)) = self.open.get(i) {
            self.at.insert(*moved, i);
        }
        if self.hand >= self.open.len() {
            self.hand = 0;
        }
        Some(f)
    }
}

/// The payload of `frame`, read at `loc`, once its length and checksum are
/// checked.
fn check_frame(mut frame: Vec<u8>, loc: DiskLoc) -> Result<Vec<u8>> {
    if frame.len() < FRAME_HEADER {
        return Err(SqlError::corrupt("short node frame"));
    }
    let len = u32::from_le_bytes(frame[0..4].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(frame[4..8].try_into().unwrap());
    if len + FRAME_HEADER != frame.len() {
        return Err(SqlError::corrupt(format!("node frame length mismatch at {loc:?}")));
    }
    let payload = frame.split_off(FRAME_HEADER);
    if crc32fast::hash(&payload) != crc {
        return Err(SqlError::corrupt(format!("node checksum mismatch at {loc:?}")));
    }
    Ok(payload)
}
