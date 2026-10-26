//! Immutable, append-only segment files holding serialized nodes.
//!
//! Each checkpoint appends the nodes dirtied since the previous one to the
//! active segment; sealed segments never change, which makes continuous
//! incremental backup a matter of copying new files.

use super::node::DiskLoc;
use crate::error::{Result, SqlError};
use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const FRAME_HEADER: usize = 8; // u32 len, u32 crc

/// The segment files of a store: shared read access (`SegmentReader`) and,
/// while no checkpoint job holds it, the append side (`SegmentWriter`).
pub struct SegmentStore {
    reader: Arc<SegmentReader>,
    writer: Option<SegmentWriter>,
    /// The active segment (also while a checkpoint job holds the writer,
    /// which may roll over to newer ones).
    active_id: u32,
}

/// Read access to segment files by location. Files are opened on first use
/// and stay open, so that a fork of the environment keeps reading segments
/// that a later checkpoint deletes (see [`SegmentStore::fork`]).
struct SegmentReader {
    dir: PathBuf,
    files: Mutex<FxHashMap<u32, Arc<File>>>,
}

/// Appends node frames to the active segment and rolls over to the next.
struct SegmentWriter {
    dir: PathBuf,
    active_id: u32,
    active: File,
    /// Length durably flushed to the OS (written).
    written_len: u64,
    /// Pending bytes not yet written.
    buf: Vec<u8>,
    max_segment_bytes: u64,
    /// Segments rolled since the last sync that still need fsync.
    unsynced: Vec<File>,
}

pub fn segment_path(dir: &Path, id: u32) -> PathBuf {
    dir.join(format!("seg-{id:08}.dat"))
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
        let writer = SegmentWriter {
            dir: dir.to_path_buf(),
            active_id,
            active: f,
            written_len: active_len,
            buf: Vec::new(),
            max_segment_bytes,
            unsynced: Vec::new(),
        };
        let reader = Arc::new(SegmentReader { dir: dir.to_path_buf(), files: Mutex::new(FxHashMap::default()) });
        Ok(SegmentStore { reader, writer: Some(writer), active_id })
    }

    pub fn active_id(&self) -> u32 {
        self.active_id
    }

    fn writer(&mut self) -> Result<&mut SegmentWriter> {
        self.writer.as_mut().ok_or_else(|| SqlError::internal("segment files are being written by a checkpoint"))
    }

    pub fn active_len(&self) -> u64 {
        self.writer.as_ref().map(|w| w.written_len + w.buf.len() as u64).unwrap_or(0)
    }

    /// Append a node payload; returns its location.
    pub fn append(&mut self, payload: &[u8]) -> Result<DiskLoc> {
        let w = self.writer()?;
        let loc = w.append(payload)?;
        self.active_id = loc.seg;
        Ok(loc)
    }

    /// Make everything appended so far durable.
    pub fn sync(&mut self) -> Result<()> {
        self.writer()?.sync()
    }

    /// Is the append side here (not lent to a checkpoint job)?
    pub fn writable(&self) -> bool {
        self.writer.is_some()
    }

    /// A store with this one's files and its append side, which this one
    /// lacks until `restore` (for a checkpoint job).
    pub fn lend(&mut self) -> Option<SegmentStore> {
        let writer = self.writer.take()?;
        Some(SegmentStore { reader: self.reader.clone(), writer: Some(writer), active_id: self.active_id })
    }

    /// Take the append side back from a store made by `lend` (it may have
    /// rolled over to newer segments).
    pub fn restore(&mut self, mut lent: SegmentStore) {
        if let Some(w) = lent.writer.take() {
            self.active_id = w.active_id;
            self.writer = Some(w);
        }
    }

    /// Read access for a fork of the environment: the files of segments
    /// `ids` stay open in it, so it can read its nodes after a checkpoint
    /// of the parent deleted their segments.
    pub fn fork(&self, ids: impl IntoIterator<Item = u32>) -> Result<SegmentStore> {
        let mut files = FxHashMap::default();
        for id in ids {
            files.insert(id, self.reader.file(id)?);
        }
        let reader = Arc::new(SegmentReader { dir: self.reader.dir.clone(), files: Mutex::new(files) });
        Ok(SegmentStore { reader, writer: None, active_id: self.active_id })
    }

    pub fn read(&self, loc: DiskLoc) -> Result<Vec<u8>> {
        self.reader.read(loc)
    }

    pub fn delete(&mut self, id: u32) -> Result<()> {
        if id == self.active_id {
            return Ok(());
        }
        self.reader.files.lock().remove(&id);
        let p = segment_path(&self.reader.dir, id);
        if p.exists() {
            std::fs::remove_file(p)?;
        }
        Ok(())
    }

    /// Close a segment that a checkpoint job deleted.
    pub fn forget(&self, id: u32) {
        self.reader.files.lock().remove(&id);
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

impl SegmentWriter {
    fn append(&mut self, payload: &[u8]) -> Result<DiskLoc> {
        if self.written_len + self.buf.len() as u64 >= self.max_segment_bytes {
            self.roll()?;
        }
        let off = self.written_len + self.buf.len() as u64;
        let len = (payload.len() + FRAME_HEADER) as u32;
        self.buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        self.buf.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
        self.buf.extend_from_slice(payload);
        if self.buf.len() > 4 << 20 {
            self.write_buffer()?;
        }
        Ok(DiskLoc { seg: self.active_id, off, len })
    }

    fn write_buffer(&mut self) -> Result<()> {
        if !self.buf.is_empty() {
            self.active.write_all_at(&self.buf, self.written_len)?;
            self.written_len += self.buf.len() as u64;
            self.buf.clear();
        }
        Ok(())
    }

    fn roll(&mut self) -> Result<()> {
        self.write_buffer()?;
        let old = std::mem::replace(
            &mut self.active,
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(segment_path(&self.dir, self.active_id + 1))?,
        );
        self.unsynced.push(old);
        self.active_id += 1;
        self.written_len = 0;
        // the new file must exist after a crash once a checkpoint refers to it
        File::open(&self.dir)?.sync_all()?;
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        self.write_buffer()?;
        for f in std::mem::take(&mut self.unsynced) {
            f.sync_data()?;
        }
        self.active.sync_data()?;
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

    /// Read and check the frame at `loc`. (Only frames of completed
    /// checkpoints are read: they are written to the file.)
    fn read(&self, loc: DiskLoc) -> Result<Vec<u8>> {
        let mut frame = vec![0u8; loc.len as usize];
        self.file(loc.seg)?.read_exact_at(&mut frame, loc.off)?;
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
}
