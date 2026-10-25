//! The input log: every source commit, DDL statement and source progress
//! event, stamped with its GSN in arrival order. State at GSN g is a pure
//! function of the log up to g; recovery is "load the checkpoint at c,
//! replay from c+1".

use crate::dataflow::TableChange;
use crate::error::{Result, SqlError};
use crate::sql::plan::ObjectId;
use crate::storage::Gsn;
use crate::types::codec::{self, get_bytes, get_ivarint, get_str, get_u8, get_uvarint, put_bytes, put_ivarint, put_str, put_uvarint};
use crate::types::Column;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub enum LogRecord {
    /// A source transaction (source 0 = local tables written through SQL).
    Commit { source: ObjectId, lsn: u64, changes: Vec<(ObjectId, TableChange)> },
    /// A catalog change. `extra` carries logged external responses (e.g. the
    /// table schemas a CREATE SOURCE discovered) as JSON.
    Ddl { req: u64, sql: String, extra: String },
    /// Source progress without data (LSN frontier, snapshot bookkeeping).
    Progress { source: ObjectId, lsn: u64, state: String },
    /// Upstream schema change for a source table.
    Schema { source: ObjectId, table: ObjectId, columns: Vec<Column>, pk: Vec<usize> },
    /// Wall clock reading (microseconds since the Unix epoch): the logical
    /// time `now()` refers to from this GSN on.
    Tick { micros: i64 },
}

impl LogRecord {
    pub fn kind(&self) -> &'static str {
        match self {
            LogRecord::Commit { .. } => "commit",
            LogRecord::Ddl { .. } => "ddl",
            LogRecord::Progress { .. } => "progress",
            LogRecord::Schema { .. } => "schema",
            LogRecord::Tick { .. } => "tick",
        }
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            LogRecord::Commit { source, lsn, changes } => {
                out.push(1);
                put_uvarint(out, *source);
                put_uvarint(out, *lsn);
                put_uvarint(out, changes.len() as u64);
                for (t, c) in changes {
                    put_uvarint(out, *t);
                    match c {
                        TableChange::Upsert { row, unchanged } => {
                            out.push(1);
                            codec::encode_row(row, out);
                            put_uvarint(out, unchanged.len() as u64);
                            for u in unchanged {
                                put_uvarint(out, *u as u64);
                            }
                        }
                        TableChange::Delete { key } => {
                            out.push(2);
                            codec::encode_row(key, out);
                        }
                        TableChange::Insert { row } => {
                            out.push(3);
                            codec::encode_row(row, out);
                        }
                        TableChange::Remove { row } => {
                            out.push(4);
                            codec::encode_row(row, out);
                        }
                        TableChange::Truncate => out.push(5),
                    }
                }
            }
            LogRecord::Ddl { req, sql, extra } => {
                out.push(2);
                put_uvarint(out, *req);
                put_str(out, sql);
                put_str(out, extra);
            }
            LogRecord::Progress { source, lsn, state } => {
                out.push(3);
                put_uvarint(out, *source);
                put_uvarint(out, *lsn);
                put_str(out, state);
            }
            LogRecord::Schema { source, table, columns, pk } => {
                out.push(4);
                put_uvarint(out, *source);
                put_uvarint(out, *table);
                let js = serde_json::to_vec(columns).unwrap();
                put_bytes(out, &js);
                put_uvarint(out, pk.len() as u64);
                for p in pk {
                    put_uvarint(out, *p as u64);
                }
            }
            LogRecord::Tick { micros } => {
                out.push(5);
                put_ivarint(out, *micros);
            }
        }
    }

    pub fn decode(mut buf: &[u8]) -> Result<LogRecord> {
        let b = &mut buf;
        Ok(match get_u8(b)? {
            1 => {
                let source = get_uvarint(b)?;
                let lsn = get_uvarint(b)?;
                let n = get_uvarint(b)? as usize;
                let mut changes = Vec::with_capacity(n.min(1 << 20));
                for _ in 0..n {
                    let t = get_uvarint(b)?;
                    let c = match get_u8(b)? {
                        1 => {
                            let row = codec::decode_row(b)?;
                            let k = get_uvarint(b)? as usize;
                            let mut unchanged = Vec::with_capacity(k);
                            for _ in 0..k {
                                unchanged.push(get_uvarint(b)? as usize);
                            }
                            TableChange::Upsert { row, unchanged }
                        }
                        2 => TableChange::Delete { key: codec::decode_row(b)? },
                        3 => TableChange::Insert { row: codec::decode_row(b)? },
                        4 => TableChange::Remove { row: codec::decode_row(b)? },
                        5 => TableChange::Truncate,
                        t => return Err(SqlError::corrupt(format!("bad change tag {t}"))),
                    };
                    changes.push((t, c));
                }
                LogRecord::Commit { source, lsn, changes }
            }
            2 => LogRecord::Ddl { req: get_uvarint(b)?, sql: get_str(b)?.to_string(), extra: get_str(b)?.to_string() },
            3 => LogRecord::Progress { source: get_uvarint(b)?, lsn: get_uvarint(b)?, state: get_str(b)?.to_string() },
            4 => {
                let source = get_uvarint(b)?;
                let table = get_uvarint(b)?;
                let columns: Vec<Column> =
                    serde_json::from_slice(get_bytes(b)?).map_err(|e| SqlError::corrupt(format!("bad schema record: {e}")))?;
                let n = get_uvarint(b)? as usize;
                let mut pk = Vec::with_capacity(n);
                for _ in 0..n {
                    pk.push(get_uvarint(b)? as usize);
                }
                LogRecord::Schema { source, table, columns, pk }
            }
            5 => LogRecord::Tick { micros: get_ivarint(b)? },
            t => return Err(SqlError::corrupt(format!("bad log record tag {t}"))),
        })
    }
}

const SEGMENT_BYTES: u64 = 64 << 20;
const HEADER: usize = 16; // len u32, crc u32, gsn u64

fn seg_path(dir: &Path, first: Gsn) -> PathBuf {
    dir.join(format!("log-{first:020}.ivl"))
}

fn list_segments(dir: &Path) -> Result<Vec<Gsn>> {
    let mut v = Vec::new();
    if !dir.exists() {
        return Ok(v);
    }
    for e in std::fs::read_dir(dir)? {
        let name = e?.file_name();
        let name = name.to_string_lossy();
        if let Some(n) = name.strip_prefix("log-").and_then(|s| s.strip_suffix(".ivl"))
            && let Ok(g) = n.parse::<Gsn>()
        {
            v.push(g);
        }
    }
    v.sort();
    Ok(v)
}

/// Parse frames from a segment buffer; returns records and the valid length.
fn parse_frames(data: &[u8]) -> (Vec<(Gsn, LogRecord)>, usize) {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos + HEADER <= data.len() {
        let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap());
        let gsn = u64::from_le_bytes(data[pos + 8..pos + 16].try_into().unwrap());
        if pos + HEADER + len > data.len() {
            break;
        }
        let payload = &data[pos + HEADER..pos + HEADER + len];
        let mut h = crc32fast::Hasher::new();
        h.update(&gsn.to_le_bytes());
        h.update(payload);
        if h.finalize() != crc {
            break;
        }
        match LogRecord::decode(payload) {
            Ok(r) => out.push((gsn, r)),
            Err(_) => break,
        }
        pos += HEADER + len;
    }
    (out, pos)
}

pub struct LogWriter {
    dir: PathBuf,
    file: File,
    cur_first: Gsn,
    cur_len: u64,
    buf: Vec<u8>,
    last_gsn: Gsn,
    sync: bool,
}

impl LogWriter {
    /// Open the log, truncating a torn tail. Returns the writer; `last_gsn()`
    /// is the last durable GSN (0 if empty).
    pub fn open(dir: &Path, sync: bool) -> Result<LogWriter> {
        std::fs::create_dir_all(dir)?;
        let segs = list_segments(dir)?;
        let (cur_first, last_gsn, cur_len) = match segs.last() {
            None => (1, 0, 0),
            Some(&first) => {
                let path = seg_path(dir, first);
                let mut data = Vec::new();
                File::open(&path)?.read_to_end(&mut data)?;
                let (recs, valid) = parse_frames(&data);
                if valid < data.len() {
                    log::warn!("truncating torn log tail in {}", path.display());
                    let f = OpenOptions::new().write(true).open(&path)?;
                    f.set_len(valid as u64)?;
                    f.sync_all()?;
                }
                let last = match recs.last() {
                    Some((g, _)) => *g,
                    None => {
                        // empty last segment: last gsn is in the previous one
                        let mut last = first.saturating_sub(1);
                        if segs.len() >= 2 {
                            let prev = seg_path(dir, segs[segs.len() - 2]);
                            let mut d = Vec::new();
                            File::open(&prev)?.read_to_end(&mut d)?;
                            if let Some((g, _)) = parse_frames(&d).0.last() {
                                last = *g;
                            }
                        }
                        last
                    }
                };
                (first, last, valid as u64)
            }
        };
        let file = OpenOptions::new().create(true).append(true).open(seg_path(dir, cur_first))?;
        Ok(LogWriter { dir: dir.to_path_buf(), file, cur_first, cur_len, buf: Vec::new(), last_gsn, sync })
    }

    pub fn last_gsn(&self) -> Gsn {
        self.last_gsn
    }

    /// Buffer a record. GSNs must be strictly increasing.
    pub fn append(&mut self, gsn: Gsn, rec: &LogRecord) -> Result<()> {
        if self.cur_len >= SEGMENT_BYTES && self.buf.is_empty() {
            self.roll(gsn)?;
        }
        let mut payload = Vec::new();
        rec.encode(&mut payload);
        let mut h = crc32fast::Hasher::new();
        h.update(&gsn.to_le_bytes());
        h.update(&payload);
        self.buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        self.buf.extend_from_slice(&h.finalize().to_le_bytes());
        self.buf.extend_from_slice(&gsn.to_le_bytes());
        self.buf.extend_from_slice(&payload);
        self.last_gsn = gsn;
        Ok(())
    }

    fn roll(&mut self, next_gsn: Gsn) -> Result<()> {
        self.file.sync_all()?;
        self.file = OpenOptions::new().create(true).append(true).open(seg_path(&self.dir, next_gsn))?;
        self.cur_first = next_gsn;
        self.cur_len = 0;
        if let Ok(d) = File::open(&self.dir) {
            let _ = d.sync_all();
        }
        Ok(())
    }

    /// Write buffered records and make them durable.
    pub fn flush(&mut self) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        self.file.write_all(&self.buf)?;
        self.cur_len += self.buf.len() as u64;
        self.buf.clear();
        if self.sync {
            self.file.sync_data()?;
        }
        Ok(())
    }

    /// Delete segments whose records are all at or before `gsn`.
    pub fn truncate_before(&mut self, gsn: Gsn) -> Result<()> {
        let segs = list_segments(&self.dir)?;
        for w in segs.windows(2) {
            if w[1] <= gsn + 1 && w[0] != self.cur_first {
                std::fs::remove_file(seg_path(&self.dir, w[0]))?;
            }
        }
        Ok(())
    }
}

/// Read all records with gsn > `after`.
pub fn read_from(dir: &Path, after: Gsn) -> Result<Vec<(Gsn, LogRecord)>> {
    let segs = list_segments(dir)?;
    let mut out = Vec::new();
    for (i, first) in segs.iter().enumerate() {
        // skip segments entirely before `after`
        if let Some(next) = segs.get(i + 1)
            && *next <= after + 1
        {
            continue;
        }
        let mut data = Vec::new();
        File::open(seg_path(dir, *first))?.read_to_end(&mut data)?;
        let (recs, _) = parse_frames(&data);
        out.extend(recs.into_iter().filter(|(g, _)| *g > after));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Datum;

    #[test]
    fn roundtrip_and_torn_tail() {
        let dir = std::env::temp_dir().join(format!("ivm-log-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        {
            let mut w = LogWriter::open(&dir, true).unwrap();
            for g in 1..=10 {
                w.append(
                    g,
                    &LogRecord::Commit {
                        source: 0,
                        lsn: g * 10,
                        changes: vec![(7, TableChange::Upsert { row: vec![Datum::Int4(g as i32), Datum::text("x")], unchanged: vec![1] })],
                    },
                )
                .unwrap();
            }
            w.append(11, &LogRecord::Ddl { req: 1, sql: "create table t (a int)".into(), extra: String::new() }).unwrap();
            w.flush().unwrap();
        }
        // corrupt the tail
        let seg = seg_path(&dir, 1);
        let mut f = OpenOptions::new().append(true).open(&seg).unwrap();
        f.write_all(&[1, 2, 3, 4, 5]).unwrap();
        drop(f);
        let w = LogWriter::open(&dir, true).unwrap();
        assert_eq!(w.last_gsn(), 11);
        let recs = read_from(&dir, 5).unwrap();
        assert_eq!(recs.len(), 6);
        assert!(matches!(&recs[5].1, LogRecord::Ddl { sql, .. } if sql.starts_with("create")));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
