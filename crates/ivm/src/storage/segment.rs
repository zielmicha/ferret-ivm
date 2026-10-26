//! Immutable, append-only segment files holding serialized nodes.
//!
//! Each checkpoint appends the nodes dirtied since the previous one to the
//! active segment; sealed segments never change, which makes continuous
//! incremental backup a matter of copying new files.

use super::node::DiskLoc;
use crate::error::{Result, SqlError};
use rustc_hash::FxHashMap;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

const FRAME_HEADER: usize = 8; // u32 len, u32 crc

pub struct SegmentStore {
    dir: PathBuf,
    active_id: u32,
    active: File,
    /// Length durably flushed to the OS (written).
    written_len: u64,
    /// Pending bytes not yet written.
    buf: Vec<u8>,
    max_segment_bytes: u64,
    readers: FxHashMap<u32, File>,
    /// Segments rolled since the last sync that still need fsync.
    unsynced: Vec<u32>,
    /// Bytes of node frames read so far.
    pub bytes_read: u64,
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
        Ok(SegmentStore {
            dir: dir.to_path_buf(),
            active_id,
            active: f,
            written_len: active_len,
            buf: Vec::new(),
            max_segment_bytes,
            readers: FxHashMap::default(),
            unsynced: Vec::new(),
            bytes_read: 0,
        })
    }

    pub fn active_id(&self) -> u32 {
        self.active_id
    }

    pub fn active_len(&self) -> u64 {
        self.written_len + self.buf.len() as u64
    }

    /// Append a node payload; returns its location.
    pub fn append(&mut self, payload: &[u8]) -> Result<DiskLoc> {
        if self.active_len() >= self.max_segment_bytes {
            self.roll()?;
        }
        let off = self.active_len();
        let len = (payload.len() + FRAME_HEADER) as u32;
        self.buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        self.buf.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
        self.buf.extend_from_slice(payload);
        if self.buf.len() > 4 << 20 {
            self.write_buffer()?;
        }
        Ok(DiskLoc { seg: self.active_id, off, len })
    }

    /// Hand the pending bytes to the OS (without syncing them).
    pub(super) fn write_buffer(&mut self) -> Result<()> {
        if !self.buf.is_empty() {
            self.active.write_all_at(&self.buf, self.written_len)?;
            self.written_len += self.buf.len() as u64;
            self.buf.clear();
        }
        Ok(())
    }

    fn roll(&mut self) -> Result<()> {
        self.write_buffer()?;
        self.unsynced.push(self.active_id);
        let old = std::mem::replace(
            &mut self.active,
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(segment_path(&self.dir, self.active_id + 1))?,
        );
        self.readers.insert(self.active_id, old);
        self.active_id += 1;
        self.written_len = 0;
        // the new file must exist after a crash once a checkpoint refers to it
        File::open(&self.dir)?.sync_all()?;
        Ok(())
    }

    /// Make everything appended so far durable.
    pub fn sync(&mut self) -> Result<()> {
        self.write_buffer()?;
        for id in std::mem::take(&mut self.unsynced) {
            if let Some(f) = self.readers.get(&id) {
                f.sync_data()?;
            }
        }
        self.active.sync_data()?;
        Ok(())
    }

    pub fn read(&mut self, loc: DiskLoc) -> Result<Vec<u8>> {
        self.bytes_read += loc.len as u64;
        let mut frame = vec![0u8; loc.len as usize];
        if loc.seg == self.active_id {
            if loc.off + loc.len as u64 > self.written_len {
                self.write_buffer()?;
            }
            self.active.read_exact_at(&mut frame, loc.off)?;
        } else {
            if !self.readers.contains_key(&loc.seg) {
                let f = File::open(segment_path(&self.dir, loc.seg)).map_err(|e| {
                    SqlError::corrupt(format!("cannot open segment {}: {e}", loc.seg))
                })?;
                self.readers.insert(loc.seg, f);
            }
            self.readers[&loc.seg].read_exact_at(&mut frame, loc.off)?;
        }
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

    pub fn delete(&mut self, id: u32) -> Result<()> {
        if id == self.active_id {
            return Ok(());
        }
        self.readers.remove(&id);
        let p = segment_path(&self.dir, id);
        if p.exists() {
            std::fs::remove_file(p)?;
        }
        Ok(())
    }

    /// Segment ids present on disk.
    pub fn list(&self) -> Result<Vec<u32>> {
        let mut ids = Vec::new();
        for e in std::fs::read_dir(&self.dir)? {
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
