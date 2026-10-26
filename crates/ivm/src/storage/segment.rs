//! Immutable, append-only segment files holding serialized nodes.
//!
//! Each checkpoint appends the nodes dirtied since the previous one to the
//! active segment (as do early writes of dirty nodes); sealed segments never
//! change, which makes continuous incremental backup a matter of copying
//! new files.

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
/// its forks append to the same active segment, each frame in one piece.
pub struct SegmentStore {
    reader: Arc<SegmentReader>,
    writer: Arc<SharedWriter>,
    /// In a fork: the segments it appended to, pinned against deletion until
    /// it is dropped (its trees may be adopted, frames and all).
    pins: Option<BTreeSet<u32>>,
}

/// Read access to segment files by location. Files are opened on first use
/// and stay open, so that a fork of the environment keeps reading segments
/// that a later checkpoint deletes (see [`SegmentStore::fork`]).
struct SegmentReader {
    dir: PathBuf,
    files: Mutex<FxHashMap<u32, Arc<File>>>,
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
struct SegmentWriter {
    dir: PathBuf,
    active_id: u32,
    active: Arc<File>,
    /// Length handed to the OS (written).
    written_len: u64,
    /// Pending bytes not yet written.
    buf: Vec<u8>,
    max_segment_bytes: u64,
    /// Segments rolled since the last sync that still need fsync.
    unsynced: Vec<Arc<File>>,
    /// Segments forks appended to, with the number of forks pinning each.
    pinned: BTreeMap<u32, usize>,
    /// Bytes forks appended per segment, not yet counted in the totals of
    /// the environment they were forked from (`take_foreign`).
    foreign: BTreeMap<u32, u64>,
    /// Lengths of the segments this process sealed (rolled over from).
    sealed: BTreeMap<u32, u64>,
}

pub fn segment_path(dir: &Path, id: u32) -> PathBuf {
    dir.join(format!("seg-{id:08}.dat"))
}

fn pack(id: u32, len: u64) -> u64 {
    (id as u64) << 40 | len
}

impl SegmentStore {
    /// Open the store with `active_id` as the active segment, truncated to `active_len`.
    pub fn open(dir: &Path, active_id: u32, active_len: u64, max_segment_bytes: u64) -> Result<SegmentStore> {
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
        };
        let writer = Arc::new(SharedWriter { written: AtomicU64::new(pack(active_id, active_len)), w: Mutex::new(w) });
        let reader = Arc::new(SegmentReader { dir: dir.to_path_buf(), files: Mutex::new(FxHashMap::default()) });
        Ok(SegmentStore { reader, writer, pins: None })
    }

    pub fn active_id(&self) -> u32 {
        (self.writer.written.load(Ordering::Acquire) >> 40) as u32
    }

    /// Append a node payload; returns its location.
    pub fn append(&mut self, payload: &[u8]) -> Result<DiskLoc> {
        let mut w = self.writer.w.lock();
        let loc = w.append(payload, &self.writer.written)?;
        if let Some(pins) = &mut self.pins {
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
        let (files, active, durable) = {
            let mut w = self.writer.w.lock();
            w.write_buffer(&self.writer.written)?;
            (std::mem::take(&mut w.unsynced), w.active.clone(), (w.active_id, w.written_len, w.sealed.clone()))
        };
        for f in files {
            f.sync_data()?;
        }
        active.sync_data()?;
        Ok(durable)
    }

    /// Another view of this store, with its read access and append side
    /// (for a checkpoint job).
    pub fn share(&self) -> SegmentStore {
        SegmentStore { reader: self.reader.clone(), writer: self.writer.clone(), pins: None }
    }

    /// The store as a fork of the environment sees it: the files of
    /// segments `ids` stay open in it, so it can read its nodes after a
    /// checkpoint of the environment deleted their segments, and the
    /// segments it appends to are pinned until it is dropped.
    pub fn fork(&self, ids: impl IntoIterator<Item = u32>) -> Result<SegmentStore> {
        let mut files = FxHashMap::default();
        for id in ids {
            files.insert(id, self.reader.file(id)?);
        }
        let reader = Arc::new(SegmentReader { dir: self.reader.dir.clone(), files: Mutex::new(files) });
        Ok(SegmentStore { reader, writer: self.writer.clone(), pins: Some(BTreeSet::new()) })
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

    /// Delete sealed segment `id`, unless it is the active one or a fork
    /// pins it; returns whether it did.
    pub fn delete(&self, id: u32) -> Result<bool> {
        let mut w = self.writer.w.lock();
        if id == w.active_id || w.pinned.contains_key(&id) {
            return Ok(false);
        }
        w.sealed.remove(&id);
        self.reader.files.lock().remove(&id);
        let p = segment_path(&self.reader.dir, id);
        if p.exists() {
            std::fs::remove_file(p)?;
        }
        Ok(true)
    }

    /// Does a fork pin segment `id`?
    #[cfg(test)]
    pub(super) fn pinned(&self, id: u32) -> bool {
        self.writer.w.lock().pinned.contains_key(&id)
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
    fn drop(&mut self) {
        if let Some(pins) = self.pins.take().filter(|p| !p.is_empty()) {
            let mut w = self.writer.w.lock();
            for id in pins {
                if let Some(n) = w.pinned.get_mut(&id) {
                    *n -= 1;
                    if *n == 0 {
                        w.pinned.remove(&id);
                    }
                }
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
        let old = std::mem::replace(
            &mut self.active,
            Arc::new(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(segment_path(&self.dir, self.active_id + 1))?,
            ),
        );
        self.unsynced.push(old);
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
        if let Some(f) = self.files.lock().get(&id) {
            return Ok(f.clone());
        }
        let f = Arc::new(
            File::open(segment_path(&self.dir, id)).map_err(|e| SqlError::corrupt(format!("cannot open segment {id}: {e}")))?,
        );
        self.files.lock().insert(id, f.clone());
        Ok(f)
    }

    /// Read and check the frame at `loc`, which is in its file.
    fn read(&self, loc: DiskLoc) -> Result<Vec<u8>> {
        let mut frame = vec![0u8; loc.len as usize];
        self.file(loc.seg)?.read_exact_at(&mut frame, loc.off)?;
        check_frame(frame, loc)
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
