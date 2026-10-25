//! Manifest: append-only log of checkpoint records. The last valid record
//! describes the durable state (tree roots, segment liveness, frontier).

use super::node::DiskLoc;
use super::summary::Summary;
use super::{Gsn, TreeId};
use crate::error::{Result, SqlError};
use crate::types::codec::{get_bytes, get_u8, get_uvarint, put_bytes, put_uvarint};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct TreeRecord {
    pub id: TreeId,
    /// (root location, root summary, root min_seg); None for an empty tree.
    pub root: Option<(DiskLoc, Summary, u32)>,
}

#[derive(Clone, Debug)]
pub struct CheckpointRecord {
    pub seq: u64,
    pub gsn: Gsn,
    pub trees: Vec<TreeRecord>,
    /// (segment id, total bytes, live bytes)
    pub segments: Vec<(u32, u64, u64)>,
    pub active_seg: u32,
    pub active_len: u64,
    pub next_tree_id: TreeId,
    /// Opaque application state (catalog, dataflow, source positions).
    pub app: Vec<u8>,
}

impl CheckpointRecord {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_uvarint(&mut out, self.seq);
        put_uvarint(&mut out, self.gsn);
        put_uvarint(&mut out, self.trees.len() as u64);
        for t in &self.trees {
            put_uvarint(&mut out, t.id);
            match &t.root {
                None => out.push(0),
                Some((loc, sum, min_seg)) => {
                    out.push(1);
                    loc.encode(&mut out);
                    sum.encode(&mut out);
                    put_uvarint(&mut out, *min_seg as u64);
                }
            }
        }
        put_uvarint(&mut out, self.segments.len() as u64);
        for (id, total, live) in &self.segments {
            put_uvarint(&mut out, *id as u64);
            put_uvarint(&mut out, *total);
            put_uvarint(&mut out, *live);
        }
        put_uvarint(&mut out, self.active_seg as u64);
        put_uvarint(&mut out, self.active_len);
        put_uvarint(&mut out, self.next_tree_id);
        put_bytes(&mut out, &self.app);
        out
    }

    fn decode(mut buf: &[u8]) -> Result<CheckpointRecord> {
        let b = &mut buf;
        let seq = get_uvarint(b)?;
        let gsn = get_uvarint(b)?;
        let nt = get_uvarint(b)? as usize;
        let mut trees = Vec::with_capacity(nt);
        for _ in 0..nt {
            let id = get_uvarint(b)?;
            let root = match get_u8(b)? {
                0 => None,
                _ => {
                    let loc = DiskLoc::decode(b)?;
                    let sum = Summary::decode(b)?;
                    let min_seg = get_uvarint(b)? as u32;
                    Some((loc, sum, min_seg))
                }
            };
            trees.push(TreeRecord { id, root });
        }
        let ns = get_uvarint(b)? as usize;
        let mut segments = Vec::with_capacity(ns);
        for _ in 0..ns {
            segments.push((get_uvarint(b)? as u32, get_uvarint(b)?, get_uvarint(b)?));
        }
        let active_seg = get_uvarint(b)? as u32;
        let active_len = get_uvarint(b)?;
        let next_tree_id = get_uvarint(b)?;
        let app = get_bytes(b)?.to_vec();
        Ok(CheckpointRecord { seq, gsn, trees, segments, active_seg, active_len, next_tree_id, app })
    }
}

pub struct Manifest {
    path: PathBuf,
    file: File,
    len: u64,
}

const MAX_MANIFEST_BYTES: u64 = 8 << 20;

impl Manifest {
    /// Open (creating if needed) and return the last valid record.
    pub fn open(dir: &Path) -> Result<(Manifest, Option<CheckpointRecord>)> {
        let path = dir.join("MANIFEST");
        let mut last = None;
        let mut valid_len = 0u64;
        if path.exists() {
            let mut data = Vec::new();
            File::open(&path)?.read_to_end(&mut data)?;
            let mut pos = 0usize;
            while pos + 8 <= data.len() {
                let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                let crc = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap());
                if pos + 8 + len > data.len() {
                    break;
                }
                let payload = &data[pos + 8..pos + 8 + len];
                if crc32fast::hash(payload) != crc {
                    break;
                }
                last = Some(CheckpointRecord::decode(payload)?);
                pos += 8 + len;
                valid_len = pos as u64;
            }
        }
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(&path)?;
        // drop a torn tail
        file.set_len(valid_len)?;
        Ok((Manifest { path, file, len: valid_len }, last))
    }

    pub fn append(&mut self, rec: &CheckpointRecord) -> Result<()> {
        let payload = rec.encode();
        let mut frame = Vec::with_capacity(payload.len() + 8);
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
        frame.extend_from_slice(&payload);
        if self.len + frame.len() as u64 > MAX_MANIFEST_BYTES {
            // rewrite with only this record
            let tmp = self.path.with_extension("new");
            {
                let mut f = File::create(&tmp)?;
                f.write_all(&frame)?;
                f.sync_all()?;
            }
            std::fs::rename(&tmp, &self.path)?;
            if let Some(dir) = self.path.parent() {
                File::open(dir)?.sync_all()?;
            }
            self.file = OpenOptions::new().read(true).append(true).open(&self.path)?;
            self.len = frame.len() as u64;
            return Ok(());
        }
        use std::os::unix::fs::FileExt;
        self.file.write_all_at(&frame, self.len).map_err(SqlError::io)?;
        self.file.sync_data()?;
        self.len += frame.len() as u64;
        Ok(())
    }
}
