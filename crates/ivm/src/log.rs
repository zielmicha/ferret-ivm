//! The input log: every source commit, DDL statement and source progress
//! event, stamped with its GSN in arrival order. State at GSN g is a pure
//! function of the log up to g; recovery is "load the checkpoint at c,
//! replay from c+1".
//!
//! No frame holds more than about `PART_BYTES` of changes: a larger commit
//! is written as a parts file of bounded frames plus a small `CommitParts`
//! record in the log, its commit point. Replay reads one frame at a time.

use crate::dataflow::TableChange;
use crate::error::{codes, Result, SqlError};
use crate::sql::plan::ObjectId;
use crate::storage::Gsn;
use crate::types::codec::{self, get_bytes, get_ivarint, get_str, get_u8, get_uvarint, put_bytes, put_ivarint, put_str, put_uvarint};
use crate::types::Column;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// A commit's changes, or a part of them, in order.
pub type Changes = Vec<(ObjectId, TableChange)>;

#[derive(Clone, Debug)]
pub enum LogRecord {
    /// A source transaction (source 0 = local tables written through SQL).
    Commit { source: ObjectId, lsn: u64, changes: Vec<(ObjectId, TableChange)> },
    /// A commit whose changes are in the parts file of its GSN, as `parts`
    /// frames holding `changes` changes in order (`read_parts`): one too
    /// large for a frame. This record is its commit point: a parts file
    /// without it is discarded.
    CommitParts { source: ObjectId, lsn: u64, parts: u64, changes: u64 },
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

fn encode_change(out: &mut Vec<u8>, t: ObjectId, c: &TableChange) {
    put_uvarint(out, t);
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

fn decode_change(b: &mut &[u8]) -> Result<(ObjectId, TableChange)> {
    let t = get_uvarint(b)?;
    let c = match get_u8(b)? {
        1 => {
            let row = codec::decode_row(b)?;
            let k = get_uvarint(b)? as usize;
            let mut unchanged = Vec::with_capacity(k.min(4096));
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
    Ok((t, c))
}

/// `n` changes (a count, then the changes), as in a commit record or a part.
fn decode_changes(b: &mut &[u8]) -> Result<Vec<(ObjectId, TableChange)>> {
    let n = get_uvarint(b)? as usize;
    let mut changes = Vec::with_capacity(n.min(1 << 20));
    for _ in 0..n {
        changes.push(decode_change(b)?);
    }
    Ok(changes)
}

impl LogRecord {
    pub fn kind(&self) -> &'static str {
        match self {
            LogRecord::Commit { .. } | LogRecord::CommitParts { .. } => "commit",
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
                    encode_change(out, *t, c);
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
            LogRecord::CommitParts { source, lsn, parts, changes } => {
                out.push(6);
                put_uvarint(out, *source);
                put_uvarint(out, *lsn);
                put_uvarint(out, *parts);
                put_uvarint(out, *changes);
            }
        }
    }

    pub fn decode(mut buf: &[u8]) -> Result<LogRecord> {
        let b = &mut buf;
        Ok(match get_u8(b)? {
            1 => {
                let source = get_uvarint(b)?;
                let lsn = get_uvarint(b)?;
                LogRecord::Commit { source, lsn, changes: decode_changes(b)? }
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
            6 => LogRecord::CommitParts { source: get_uvarint(b)?, lsn: get_uvarint(b)?, parts: get_uvarint(b)?, changes: get_uvarint(b)? },
            t => return Err(SqlError::corrupt(format!("bad log record tag {t}"))),
        })
    }
}

const SEGMENT_BYTES: u64 = 64 << 20;
const HEADER: usize = 16; // len u32, crc u32, gsn (or part index) u64

/// Encoded changes per part of a commit, and the most a commit written as
/// one frame holds: what replay decodes at once (a decoded row takes about
/// ten times its encoded size) and the unit sources spool large
/// transactions in. A part exceeds it by at most one change.
pub const PART_BYTES: usize = 1 << 20;

/// Buffered frames are written out at this size, also in the middle of a
/// batch (made durable at its end).
const WRITE_BYTES: usize = 8 << 20;

/// Estimated encoded size of a change, to cut parts.
pub fn change_len(c: &TableChange) -> usize {
    let row = |r: &[crate::types::Datum]| 2 + r.iter().map(codec::approx_len).sum::<usize>();
    2 + match c {
        TableChange::Upsert { row: r, unchanged } => row(r) + 1 + unchanged.len(),
        TableChange::Delete { key: r } | TableChange::Insert { row: r } | TableChange::Remove { row: r } => row(r),
        TableChange::Truncate => 0,
    }
}

fn seg_path(dir: &Path, first: Gsn) -> PathBuf {
    dir.join(format!("log-{first:020}.ivl"))
}

/// The changes of the `CommitParts` record at `gsn`.
fn parts_path(dir: &Path, gsn: Gsn) -> PathBuf {
    dir.join(format!("parts-{gsn:020}.ivp"))
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

/// Parts files in the log directory, by GSN.
fn list_parts(dir: &Path) -> Result<Vec<Gsn>> {
    let mut v = Vec::new();
    for e in std::fs::read_dir(dir)? {
        let name = e?.file_name();
        let name = name.to_string_lossy();
        if let Some(n) = name.strip_prefix("parts-").and_then(|s| s.strip_suffix(".ivp"))
            && let Ok(g) = n.parse::<Gsn>()
        {
            v.push(g);
        }
    }
    Ok(v)
}

fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

/// Reserve a frame header in `buf`; the payload follows (`finish_frame`).
fn begin_frame(buf: &mut Vec<u8>) -> usize {
    let at = buf.len();
    buf.extend_from_slice(&[0; HEADER]);
    at
}

/// A payload's length as the frame header holds it. A payload too large
/// for the 32-bit length is refused: written with a wrapped length, it
/// would be taken for a torn tail (or corruption) when read back.
fn frame_len(n: usize) -> Result<u32> {
    u32::try_from(n).map_err(|_| SqlError::new(codes::PROGRAM_LIMIT_EXCEEDED, format!("a log record of {n} bytes exceeds the frame limit of 4 GiB")))
}

/// Fill in the header of the frame begun at `at` (a frame that is too
/// large is removed from `buf`).
fn finish_frame(buf: &mut Vec<u8>, at: usize, id: u64) -> Result<()> {
    let len = match frame_len(buf.len() - at - HEADER) {
        Ok(l) => l,
        Err(e) => {
            buf.truncate(at);
            return Err(e);
        }
    };
    let mut h = crc32fast::Hasher::new();
    h.update(&id.to_le_bytes());
    h.update(&buf[at + HEADER..]);
    buf[at..at + 4].copy_from_slice(&len.to_le_bytes());
    buf[at + 4..at + 8].copy_from_slice(&h.finalize().to_le_bytes());
    buf[at + 8..at + 16].copy_from_slice(&id.to_le_bytes());
    Ok(())
}

/// What `FrameReader::next` found.
enum Frame {
    /// A complete frame with a valid CRC: its GSN (part index in a parts
    /// file); the payload is in the caller's buffer.
    Ok(u64),
    /// The end of the file, after the last frame.
    End,
    /// An incomplete frame or one whose CRC does not match, at `pos`.
    Bad,
}

/// Reads the frames of a log segment or parts file one at a time.
struct FrameReader {
    r: BufReader<File>,
    path: PathBuf,
    /// Offset of the next frame: the end of the valid frames read so far.
    pos: u64,
    len: u64,
}

impl FrameReader {
    fn open(path: &Path) -> Result<FrameReader> {
        let f = File::open(path)?;
        let len = f.metadata()?.len();
        Ok(FrameReader { r: BufReader::with_capacity(256 << 10, f), path: path.to_path_buf(), pos: 0, len })
    }

    fn next(&mut self, payload: &mut Vec<u8>) -> Result<Frame> {
        let rest = self.len - self.pos;
        if rest == 0 {
            return Ok(Frame::End);
        }
        if rest < HEADER as u64 {
            return Ok(Frame::Bad);
        }
        let mut h = [0u8; HEADER];
        self.r.read_exact(&mut h)?;
        let len = u32::from_le_bytes(h[0..4].try_into().unwrap()) as u64;
        let crc = u32::from_le_bytes(h[4..8].try_into().unwrap());
        let id = u64::from_le_bytes(h[8..16].try_into().unwrap());
        // (a torn length can claim anything; the file bounds it)
        if len > rest - HEADER as u64 {
            return Ok(Frame::Bad);
        }
        payload.clear();
        payload.resize(len as usize, 0);
        self.r.read_exact(payload)?;
        let mut c = crc32fast::Hasher::new();
        c.update(&id.to_le_bytes());
        c.update(payload);
        if c.finalize() != crc {
            return Ok(Frame::Bad);
        }
        self.pos += HEADER as u64 + len;
        Ok(Frame::Ok(id))
    }
}

/// The last GSN in a segment and the length of its valid frames (checked
/// by CRC, not decoded).
fn scan_segment(path: &Path) -> Result<(Option<Gsn>, u64, u64)> {
    let mut r = FrameReader::open(path)?;
    let mut payload = Vec::new();
    let mut last = None;
    while let Frame::Ok(g) = r.next(&mut payload)? {
        last = Some(g);
    }
    Ok((last, r.pos, r.len))
}

/// Encoded changes of the part being built.
#[derive(Default)]
struct PartBuilder {
    bytes: Vec<u8>,
    n: u64,
}

impl PartBuilder {
    fn push(&mut self, t: ObjectId, c: &TableChange) {
        encode_change(&mut self.bytes, t, c);
        self.n += 1;
    }

    fn clear(&mut self) {
        self.bytes.clear();
        self.n = 0;
    }
}

/// Writes the frames of a parts file (frame ids are the part numbers).
struct PartsWriter {
    file: BufWriter<File>,
    parts: u64,
    changes: u64,
}

impl PartsWriter {
    fn create(path: &Path) -> Result<PartsWriter> {
        let f = OpenOptions::new().write(true).create(true).truncate(true).open(path)?;
        Ok(PartsWriter { file: BufWriter::with_capacity(256 << 10, f), parts: 0, changes: 0 })
    }

    /// Write `part` as the next frame and clear it.
    fn write(&mut self, part: &mut PartBuilder) -> Result<()> {
        let mut count = Vec::with_capacity(10);
        put_uvarint(&mut count, part.n);
        let len = frame_len(count.len() + part.bytes.len())?;
        let id = self.parts;
        let mut h = crc32fast::Hasher::new();
        h.update(&id.to_le_bytes());
        h.update(&count);
        h.update(&part.bytes);
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&h.finalize().to_le_bytes())?;
        self.file.write_all(&id.to_le_bytes())?;
        self.file.write_all(&count)?;
        self.file.write_all(&part.bytes)?;
        self.parts += 1;
        self.changes += part.n;
        part.clear();
        Ok(())
    }

    /// Write `changes` in parts of about `PART_BYTES`, after those so far.
    fn write_changes(&mut self, changes: &[(ObjectId, TableChange)], part: &mut PartBuilder) -> Result<()> {
        for (t, c) in changes {
            part.push(*t, c);
            if part.bytes.len() >= PART_BYTES {
                self.write(part)?;
            }
        }
        if part.n > 0 {
            self.write(part)?;
        }
        Ok(())
    }

    fn finish(mut self, sync: bool) -> Result<(u64, u64)> {
        self.file.flush()?;
        if sync {
            self.file.get_ref().sync_data()?;
        }
        Ok((self.parts, self.changes))
    }
}

/// Where sources spool the transactions too large to keep in memory: the
/// log directory (see `Spool`).
#[derive(Clone, Debug)]
pub struct SpoolDir {
    pub dir: PathBuf,
    /// fsync the spool before the commit is logged (`--no-sync` off).
    pub sync: bool,
}

/// The changes of an upstream transaction still being received, written to
/// a file in the log directory part by part as they arrive, so that a
/// transaction of any size takes bounded memory. At the transaction's
/// commit the spool becomes the parts file of its `CommitParts` record
/// (`LogWriter::append_spooled`). Upstream transactions arrive only once
/// committed, so a spool is never aborted, but it is deleted when the
/// replication session ends before the commit (the transaction is sent
/// again) and, after a crash, when the log is opened.
pub struct Spool {
    w: PartsWriter,
    part: PartBuilder,
    file: SpoolFile,
    sync: bool,
}

/// A spool's file, deleted when dropped unless it was committed.
struct SpoolFile(Option<PathBuf>);

impl Drop for SpoolFile {
    fn drop(&mut self) {
        if let Some(p) = self.0.take() {
            let _ = std::fs::remove_file(p);
        }
    }
}

impl Spool {
    pub fn create(d: &SpoolDir) -> Result<Spool> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = d.dir.join(format!("spool-{}-{}.tmp", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        let w = PartsWriter::create(&path)?;
        Ok(Spool { w, part: PartBuilder::default(), file: SpoolFile(Some(path)), sync: d.sync })
    }

    /// Append changes (after those written so far).
    pub fn write(&mut self, changes: &[(ObjectId, TableChange)]) -> Result<()> {
        self.w.write_changes(changes, &mut self.part)
    }

    /// Changes written so far.
    pub fn changes(&self) -> u64 {
        self.w.changes
    }

    /// Rewrite the changes written so far through `f`, part by part (a
    /// schema change or a failed table in the middle of the transaction
    /// changes or drops some of them).
    pub fn rewrite(&mut self, f: &mut dyn FnMut(&mut Changes)) -> Result<()> {
        let old = self.file.0.clone().expect("spool file");
        let (parts, changes) = (self.w.parts, self.w.changes);
        self.w.file.flush()?;
        let path = old.with_extension("new");
        let mut w = PartsWriter::create(&path)?;
        let new = SpoolFile(Some(path.clone()));
        let mut part = PartBuilder::default();
        for p in read_parts_file(&old, parts, changes)? {
            let mut ch = p?;
            f(&mut ch);
            w.write_changes(&ch, &mut part)?;
        }
        w.file.flush()?;
        std::fs::rename(&path, &old)?;
        drop(new);
        self.w = w;
        Ok(())
    }

    /// Make the spool durable for its commit.
    pub fn finish(self) -> Result<Spooled> {
        let Spool { w, file, sync, .. } = self;
        let (parts, changes) = w.finish(sync)?;
        Ok(Spooled { file, parts, changes })
    }
}

/// A complete spool, to be logged as a `CommitParts` record.
pub struct Spooled {
    file: SpoolFile,
    parts: u64,
    changes: u64,
}

impl Spooled {
    pub fn changes(&self) -> u64 {
        self.changes
    }

    /// The spooled changes (tests).
    #[cfg(test)]
    pub(crate) fn read(&self) -> Vec<(ObjectId, TableChange)> {
        let path = self.file.0.as_ref().expect("spool file");
        read_parts_file(path, self.parts, self.changes).unwrap().flat_map(|p| p.unwrap()).collect()
    }
}

pub struct LogWriter {
    dir: PathBuf,
    file: File,
    cur_first: Gsn,
    cur_len: u64,
    buf: Vec<u8>,
    last_gsn: Gsn,
    sync: bool,
    /// Records were appended since the last `flush`.
    in_batch: bool,
    /// Bytes were written since the last `flush`.
    unsynced: bool,
    part: PartBuilder,
}

impl LogWriter {
    /// Open the log, truncating a torn tail. Returns the writer; `last_gsn()`
    /// is the last durable GSN (0 if empty).
    pub fn open(dir: &Path, sync: bool) -> Result<LogWriter> {
        if !dir.exists() {
            std::fs::create_dir_all(dir)?;
            if sync && let Some(parent) = dir.parent().filter(|p| !p.as_os_str().is_empty()) {
                File::open(parent)?.sync_all()?;
            }
        }
        let segs = list_segments(dir)?;
        let (cur_first, last_gsn, cur_len) = match segs.last() {
            None => (1, 0, 0),
            Some(&first) => {
                let path = seg_path(dir, first);
                let (last, valid, len) = scan_segment(&path)?;
                if valid < len {
                    log::warn!("truncating torn log tail in {}", path.display());
                    let f = OpenOptions::new().write(true).open(&path)?;
                    f.set_len(valid)?;
                    f.sync_all()?;
                }
                let last = match last {
                    Some(g) => g,
                    None => {
                        // empty last segment: last gsn is in the previous one
                        let mut last = first.saturating_sub(1);
                        if segs.len() >= 2
                            && let (Some(g), _, _) = scan_segment(&seg_path(dir, segs[segs.len() - 2]))?
                        {
                            last = g;
                        }
                        last
                    }
                };
                (first, last, valid)
            }
        };
        // Spools of transactions that never committed, and parts files
        // whose CommitParts record was not written (or was in the torn
        // tail): no record refers to them.
        let mut removed = false;
        for e in std::fs::read_dir(dir)? {
            let name = e?.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("spool-") {
                std::fs::remove_file(dir.join(&*name))?;
                removed = true;
            }
        }
        for g in list_parts(dir)?.into_iter().filter(|g| *g > last_gsn) {
            log::warn!("removing the parts of a commit at gsn {g} that was not logged");
            std::fs::remove_file(parts_path(dir, g))?;
            removed = true;
        }
        if removed && sync {
            sync_dir(dir)?;
        }
        let path = seg_path(dir, cur_first);
        let created = !path.exists();
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        if created && sync {
            // (later segments: roll)
            sync_dir(dir)?;
        }
        Ok(LogWriter {
            dir: dir.to_path_buf(),
            file,
            cur_first,
            cur_len,
            buf: Vec::new(),
            last_gsn,
            sync,
            in_batch: false,
            unsynced: false,
            part: PartBuilder::default(),
        })
    }

    pub fn last_gsn(&self) -> Gsn {
        self.last_gsn
    }

    /// Where sources spool large transactions.
    pub fn spool_dir(&self) -> SpoolDir {
        SpoolDir { dir: self.dir.clone(), sync: self.sync }
    }

    /// Continue numbering after `gsn` (a checkpoint taken after records the
    /// log has since lost, e.g. an unsynced tail).
    pub fn skip_to(&mut self, gsn: Gsn) {
        self.last_gsn = self.last_gsn.max(gsn);
    }

    /// The first append of a batch rolls to a new segment when the current
    /// one is full, so segment boundaries fall between batches.
    fn begin_append(&mut self, gsn: Gsn) -> Result<()> {
        if !self.in_batch {
            if self.cur_len >= SEGMENT_BYTES {
                self.roll(gsn)?;
            }
            self.in_batch = true;
        }
        Ok(())
    }

    /// Buffer a record. GSNs must be strictly increasing. A commit with
    /// more than `PART_BYTES` of changes is written to its parts file (made
    /// durable now) and logged as `CommitParts`.
    pub fn append(&mut self, gsn: Gsn, rec: &LogRecord) -> Result<()> {
        self.begin_append(gsn)?;
        match rec {
            LogRecord::Commit { source, lsn, changes } => {
                let mut part = std::mem::take(&mut self.part);
                let r = self.append_commit(gsn, *source, *lsn, changes, &mut part);
                part.clear();
                if part.bytes.capacity() > 2 * PART_BYTES {
                    part = PartBuilder::default();
                }
                self.part = part;
                r?;
            }
            _ => {
                let at = begin_frame(&mut self.buf);
                rec.encode(&mut self.buf);
                finish_frame(&mut self.buf, at, gsn)?;
            }
        }
        self.last_gsn = gsn;
        self.write_some()
    }

    fn append_commit(&mut self, gsn: Gsn, source: ObjectId, lsn: u64, changes: &[(ObjectId, TableChange)], part: &mut PartBuilder) -> Result<()> {
        let mut parts: Option<PartsWriter> = None;
        for (t, c) in changes {
            part.push(*t, c);
            if part.bytes.len() >= PART_BYTES {
                let w = match &mut parts {
                    Some(w) => w,
                    None => parts.insert(PartsWriter::create(&parts_path(&self.dir, gsn))?),
                };
                w.write(part)?;
            }
        }
        match parts {
            None => {
                // one frame, encoded as LogRecord::encode does
                let at = begin_frame(&mut self.buf);
                self.buf.push(1);
                put_uvarint(&mut self.buf, source);
                put_uvarint(&mut self.buf, lsn);
                put_uvarint(&mut self.buf, part.n);
                self.buf.extend_from_slice(&part.bytes);
                finish_frame(&mut self.buf, at, gsn)
            }
            Some(mut w) => {
                if part.n > 0 {
                    w.write(part)?;
                }
                let (parts, changes) = w.finish(self.sync)?;
                self.commit_parts(gsn, source, lsn, parts, changes)
            }
        }
    }

    /// Log the `CommitParts` record of a parts file in place: the file must
    /// be durable, with its name, before the record can be.
    fn commit_parts(&mut self, gsn: Gsn, source: ObjectId, lsn: u64, parts: u64, changes: u64) -> Result<()> {
        if self.sync {
            sync_dir(&self.dir)?;
        }
        let at = begin_frame(&mut self.buf);
        LogRecord::CommitParts { source, lsn, parts, changes }.encode(&mut self.buf);
        finish_frame(&mut self.buf, at, gsn)
    }

    /// Buffer the commit of a spooled transaction: its spool becomes the
    /// parts file of `gsn`. Returns the record logged.
    pub fn append_spooled(&mut self, gsn: Gsn, source: ObjectId, lsn: u64, mut spooled: Spooled) -> Result<LogRecord> {
        self.begin_append(gsn)?;
        let from = spooled.file.0.take().expect("spool file");
        if let Err(e) = std::fs::rename(&from, parts_path(&self.dir, gsn)) {
            let _ = std::fs::remove_file(&from);
            return Err(e.into());
        }
        let (parts, changes) = (spooled.parts, spooled.changes);
        self.commit_parts(gsn, source, lsn, parts, changes)?;
        self.last_gsn = gsn;
        self.write_some()?;
        Ok(LogRecord::CommitParts { source, lsn, parts, changes })
    }

    /// Write the buffer out once it is large (a batch can be large).
    fn write_some(&mut self) -> Result<()> {
        if self.buf.len() >= WRITE_BYTES {
            self.write_buf()?;
        }
        Ok(())
    }

    fn write_buf(&mut self) -> Result<()> {
        self.file.write_all(&self.buf)?;
        self.cur_len += self.buf.len() as u64;
        self.unsynced = true;
        self.buf.clear();
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
        self.in_batch = false;
        if !self.buf.is_empty() {
            self.write_buf()?;
        }
        // (a single huge record must not keep its buffer)
        if self.buf.capacity() > WRITE_BYTES + 2 * PART_BYTES {
            self.buf = Vec::new();
        }
        if self.unsynced {
            self.unsynced = false;
            if self.sync {
                self.file.sync_data()?;
            }
        }
        Ok(())
    }

    /// Delete segments whose records are all at or before `gsn`, and the
    /// parts files of commits at or before it.
    pub fn truncate_before(&mut self, gsn: Gsn) -> Result<()> {
        let segs = list_segments(&self.dir)?;
        for w in segs.windows(2) {
            if w[1] <= gsn + 1 && w[0] != self.cur_first {
                std::fs::remove_file(seg_path(&self.dir, w[0]))?;
            }
        }
        for g in list_parts(&self.dir)?.into_iter().filter(|g| *g <= gsn) {
            std::fs::remove_file(parts_path(&self.dir, g))?;
        }
        Ok(())
    }
}

/// The records with gsn > `after`, in order, read one frame at a time (see
/// `read_from`).
pub struct Replay {
    dir: PathBuf,
    segs: Vec<Gsn>,
    /// Index of the segment being read.
    i: usize,
    reader: Option<FrameReader>,
    after: Gsn,
    expect: Gsn,
    payload: Vec<u8>,
    done: bool,
}

/// Read all records with gsn > `after`, one at a time. Only the last
/// segment can end in a torn write (which `LogWriter::open` truncates);
/// invalid data anywhere else, a frame that does not decode, or a missing
/// GSN, is corruption: replaying around it would silently skip records.
pub fn read_from(dir: &Path, after: Gsn) -> Result<Replay> {
    let segs = list_segments(dir)?;
    // skip segments entirely before `after`
    let i = (0..segs.len()).find(|&i| segs.get(i + 1).is_none_or(|next| *next > after + 1)).unwrap_or(segs.len());
    Ok(Replay { dir: dir.to_path_buf(), segs, i, reader: None, after, expect: after + 1, payload: Vec::new(), done: false })
}

impl Replay {
    fn step(&mut self) -> Result<Option<(Gsn, LogRecord)>> {
        loop {
            if self.i >= self.segs.len() {
                return Ok(None);
            }
            let reader = match &mut self.reader {
                Some(r) => r,
                None => self.reader.insert(FrameReader::open(&seg_path(&self.dir, self.segs[self.i]))?),
            };
            match reader.next(&mut self.payload)? {
                // (frames before `after` are checked, not decoded)
                Frame::Ok(g) if g <= self.after => continue,
                Frame::Ok(g) => {
                    if g != self.expect {
                        return Err(SqlError::corrupt(format!(
                            "input log is missing records: expected gsn {}, found {g} in {}",
                            self.expect,
                            reader.path.display()
                        )));
                    }
                    let rec = LogRecord::decode(&self.payload).map_err(|e| {
                        SqlError::corrupt(format!("input log segment {} has a record that does not decode at gsn {g}: {}", reader.path.display(), e.message))
                    })?;
                    self.expect = g + 1;
                    if self.payload.capacity() > 2 * PART_BYTES {
                        self.payload = Vec::new();
                    }
                    return Ok(Some((g, rec)));
                }
                Frame::End => {}
                Frame::Bad if self.i + 1 < self.segs.len() => {
                    return Err(SqlError::corrupt(format!("input log segment {} is corrupt at byte {}", reader.path.display(), reader.pos)));
                }
                // the end of the newest segment
                Frame::Bad => return Ok(None),
            }
            self.reader = None;
            self.i += 1;
        }
    }
}

impl Iterator for Replay {
    type Item = Result<(Gsn, LogRecord)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let r = self.step().transpose();
        if !matches!(r, Some(Ok(_))) {
            self.done = true;
        }
        r
    }
}

/// The changes of the `CommitParts { parts, changes, .. }` record at `gsn`,
/// one part at a time. A missing or incomplete parts file is corruption.
pub fn read_parts(dir: &Path, gsn: Gsn, parts: u64, changes: u64) -> Result<Parts> {
    let path = parts_path(dir, gsn);
    if !path.exists() {
        return Err(SqlError::corrupt(format!("input log is missing {}, the changes of the commit at gsn {gsn}", path.display())));
    }
    read_parts_file(&path, parts, changes)
}

fn read_parts_file(path: &Path, parts: u64, changes: u64) -> Result<Parts> {
    Ok(Parts { reader: FrameReader::open(path)?, parts, changes, next: 0, seen: 0, payload: Vec::new(), done: false })
}

/// Iterator over the parts of a commit (see `read_parts`).
pub struct Parts {
    reader: FrameReader,
    parts: u64,
    changes: u64,
    next: u64,
    seen: u64,
    payload: Vec<u8>,
    done: bool,
}

impl Parts {
    fn step(&mut self) -> Result<Option<Vec<(ObjectId, TableChange)>>> {
        let bad = |r: &FrameReader, what: &str| SqlError::corrupt(format!("input log parts file {} {what}", r.path.display()));
        if self.next == self.parts {
            if self.seen != self.changes {
                return Err(bad(&self.reader, &format!("holds {} changes, its commit {}", self.seen, self.changes)));
            }
            return Ok(None);
        }
        match self.reader.next(&mut self.payload)? {
            Frame::Ok(i) if i == self.next => {
                let mut b = &self.payload[..];
                let ch = decode_changes(&mut b).map_err(|e| bad(&self.reader, &format!("has a part that does not decode: {}", e.message)))?;
                self.next += 1;
                self.seen += ch.len() as u64;
                Ok(Some(ch))
            }
            Frame::Ok(i) => Err(bad(&self.reader, &format!("has part {i} where part {} belongs", self.next))),
            Frame::End | Frame::Bad => Err(bad(&self.reader, &format!("is corrupt or incomplete at byte {} (part {} of {})", self.reader.pos, self.next, self.parts))),
        }
    }
}

impl Iterator for Parts {
    type Item = Result<Vec<(ObjectId, TableChange)>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let r = self.step().transpose();
        if !matches!(r, Some(Ok(_))) {
            self.done = true;
        }
        r
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Datum;

    fn tick(g: Gsn) -> LogRecord {
        LogRecord::Tick { micros: g as i64 }
    }

    fn tmp(name: &str) -> PathBuf {
        let base = std::env::var("IVM_TEST_TMP").map(PathBuf::from).unwrap_or_else(|_| std::env::temp_dir());
        let dir = base.join(format!("ivm-log-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn all(dir: &Path, after: Gsn) -> Result<Vec<(Gsn, LogRecord)>> {
        read_from(dir, after)?.collect()
    }

    /// A log of two segments (first GSNs 1 and `split`) holding `gsns`.
    fn two_segments(dir: &Path, gsns: &[Gsn], split: Gsn) {
        let _ = std::fs::remove_dir_all(dir);
        let parts: [(Gsn, Vec<Gsn>); 2] =
            [(1, gsns.iter().copied().filter(|g| *g < split).collect()), (split, gsns.iter().copied().filter(|g| *g >= split).collect())];
        for (first, part) in parts {
            let tmp = dir.join("tmp");
            let mut w = LogWriter::open(&tmp, false).unwrap();
            for g in part {
                w.append(g, &tick(g)).unwrap();
            }
            w.flush().unwrap();
            drop(w);
            std::fs::rename(seg_path(&tmp, 1), seg_path(dir, first)).unwrap();
            std::fs::remove_dir(&tmp).unwrap();
        }
    }

    /// Damage before the last segment, or a missing record, is reported
    /// instead of being replayed around.
    #[test]
    fn corruption_before_the_tail_is_an_error() {
        let dir = tmp("corrupt");
        let all_gsns: Vec<Gsn> = (1..=10).collect();
        two_segments(&dir, &all_gsns, 6);
        assert_eq!(all(&dir, 0).unwrap().len(), 10);
        assert_eq!(all(&dir, 7).unwrap().iter().map(|(g, _)| *g).collect::<Vec<_>>(), vec![8, 9, 10]);
        // a flipped byte in the middle of the first segment
        let first = seg_path(&dir, 1);
        let mut data = std::fs::read(&first).unwrap();
        let mid = data.len() / 2;
        data[mid] ^= 0xFF;
        std::fs::write(&first, &data).unwrap();
        assert!(all(&dir, 0).is_err());
        // a segment's records are missing
        let gap: Vec<Gsn> = (1..=10).filter(|g| *g != 7).collect();
        two_segments(&dir, &gap, 6);
        assert!(all(&dir, 0).is_err());
        assert!(all(&dir, 3).is_err());
        assert_eq!(all(&dir, 7).unwrap().len(), 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn roundtrip_and_torn_tail() {
        let dir = tmp("roundtrip");
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
        let recs = all(&dir, 5).unwrap();
        assert_eq!(recs.len(), 6);
        assert!(matches!(&recs[5].1, LogRecord::Ddl { sql, .. } if sql.starts_with("create")));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `n` upserts of rows with a `width`-byte text value.
    fn big_changes(n: usize, width: usize) -> Vec<(ObjectId, TableChange)> {
        (0..n).map(|i| (7, TableChange::Upsert { row: vec![Datum::Int4(i as i32), Datum::text(&"x".repeat(width))], unchanged: vec![] })).collect()
    }

    fn collect_parts(dir: &Path, gsn: Gsn, rec: &LogRecord) -> Vec<Vec<(ObjectId, TableChange)>> {
        let LogRecord::CommitParts { parts, changes, .. } = rec else { panic!("not CommitParts: {rec:?}") };
        read_parts(dir, gsn, *parts, *changes).unwrap().collect::<Result<Vec<_>>>().unwrap()
    }

    /// A commit larger than a frame is logged as bounded parts and a
    /// marker; replay holds one frame at a time, however large the backlog.
    #[test]
    fn large_commits_are_written_in_bounded_parts() {
        let dir = tmp("parts");
        // 8 MiB of changes in one commit
        let changes = big_changes(8 * 1024, 1024);
        {
            let mut w = LogWriter::open(&dir, false).unwrap();
            w.append(1, &tick(1)).unwrap();
            w.append(2, &LogRecord::Commit { source: 3, lsn: 99, changes: changes.clone() }).unwrap();
            w.append(3, &tick(3)).unwrap();
            w.flush().unwrap();
        }
        // the log segment is small: the changes are in the parts file
        assert!(std::fs::metadata(seg_path(&dir, 1)).unwrap().len() < 1024);
        let mut replay = read_from(&dir, 0).unwrap();
        assert!(matches!(replay.next(), Some(Ok((1, LogRecord::Tick { .. })))));
        let (g, rec) = replay.next().unwrap().unwrap();
        assert!(matches!(rec, LogRecord::CommitParts { source: 3, lsn: 99, changes: 8192, .. }));
        // replay's buffer is bounded by the (small) frames it read
        assert!(replay.payload.capacity() <= 2 * PART_BYTES);
        let parts = collect_parts(&dir, g, &rec);
        assert!(parts.len() >= 8, "{} parts", parts.len());
        // each part is about PART_BYTES (the parts file holds their frames)
        let file = std::fs::metadata(parts_path(&dir, g)).unwrap().len() as usize;
        assert!(file <= parts.len() * (PART_BYTES + 2048), "{} parts in {file} bytes", parts.len());
        assert!(parts.iter().all(|p| p.len() <= PART_BYTES / 1024 + 2));
        let flat: Vec<_> = parts.into_iter().flatten().collect();
        assert_eq!(flat.len(), changes.len());
        assert!(flat.iter().zip(&changes).all(|(a, b)| format!("{a:?}") == format!("{b:?}")));
        assert!(matches!(replay.next(), Some(Ok((3, LogRecord::Tick { .. })))));
        assert!(replay.next().is_none());
        // truncation takes the parts file along
        let mut w = LogWriter::open(&dir, false).unwrap();
        w.truncate_before(2).unwrap();
        assert!(!parts_path(&dir, 2).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Replay reads a backlog one frame at a time: what it holds is bounded
    /// by the largest frame, not by the records after the checkpoint (it
    /// used to decode all of them into one vector first).
    #[test]
    fn replay_holds_one_frame_of_a_backlog() {
        let dir = tmp("backlog");
        let mut w = LogWriter::open(&dir, false).unwrap();
        // 24 MiB of commits of 200 KiB, in batches of 10
        for g in 1..=120 {
            w.append(g, &LogRecord::Commit { source: 0, lsn: g, changes: big_changes(200, 1024) }).unwrap();
            if g % 10 == 0 {
                w.flush().unwrap();
            }
        }
        drop(w);
        let mut replay = read_from(&dir, 0).unwrap();
        let mut n = 0;
        while let Some(r) = replay.next() {
            let (g, rec) = r.unwrap();
            assert!(matches!(rec, LogRecord::Commit { lsn, ref changes, .. } if lsn == g && changes.len() == 200));
            assert!(replay.payload.capacity() < 512 << 10, "replay holds {} bytes", replay.payload.capacity());
            n += 1;
        }
        assert_eq!(n, 120);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A multi-part commit whose marker did not make it to the log (a crash
    /// while it was written, or right after its parts) is discarded as a
    /// whole when the log is opened, and its GSN is reused safely.
    #[test]
    fn a_commit_torn_before_its_marker_is_discarded() {
        let dir = tmp("torn-parts");
        let changes = big_changes(3 * 1024, 1024);
        {
            let mut w = LogWriter::open(&dir, false).unwrap();
            w.append(1, &tick(1)).unwrap();
            w.flush().unwrap();
            w.append(2, &LogRecord::Commit { source: 3, lsn: 99, changes: changes.clone() }).unwrap();
            w.flush().unwrap();
        }
        // the marker frame is torn: only part of it reached the disk
        let seg = seg_path(&dir, 1);
        let len = std::fs::metadata(&seg).unwrap().len();
        OpenOptions::new().write(true).open(&seg).unwrap().set_len(len - 3).unwrap();
        // and a source's spool of a transaction that never committed
        let mut spool = Spool::create(&SpoolDir { dir: dir.clone(), sync: false }).unwrap();
        spool.write(&changes).unwrap();
        std::mem::forget(spool);
        assert!(parts_path(&dir, 2).exists());
        let mut w = LogWriter::open(&dir, false).unwrap();
        assert_eq!(w.last_gsn(), 1);
        assert!(!parts_path(&dir, 2).exists(), "the parts of the torn commit are discarded");
        assert!(std::fs::read_dir(&dir).unwrap().all(|e| !e.unwrap().file_name().to_string_lossy().starts_with("spool-")));
        assert_eq!(all(&dir, 0).unwrap().len(), 1);
        // GSN 2 is reused by another record
        w.append(2, &tick(2)).unwrap();
        w.flush().unwrap();
        assert!(matches!(all(&dir, 1).unwrap().as_slice(), [(2, LogRecord::Tick { .. })]));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A spool written part by part becomes the parts file of its commit;
    /// rewriting it (a schema change or a failed table in the middle of the
    /// transaction) changes the changes written so far.
    #[test]
    fn spools_become_commits() {
        let dir = tmp("spool");
        let mut w = LogWriter::open(&dir, true).unwrap();
        let mut spool = Spool::create(&w.spool_dir()).unwrap();
        spool.write(&big_changes(2048, 1024)).unwrap();
        spool.write(&[(8, TableChange::Truncate)]).unwrap();
        spool.write(&big_changes(2048, 1024)).unwrap();
        spool.rewrite(&mut |ch| ch.retain(|(t, c)| *t == 8 || matches!(c, TableChange::Upsert { row, .. } if row[0] == Datum::Int4(5)))).unwrap();
        spool.write(&big_changes(1, 10)).unwrap();
        assert_eq!(spool.changes(), 4);
        let spooled = spool.finish().unwrap();
        let rec = w.append_spooled(1, 2, 77, spooled).unwrap();
        w.flush().unwrap();
        assert!(matches!(rec, LogRecord::CommitParts { source: 2, lsn: 77, changes: 4, .. }));
        let replayed = all(&dir, 0).unwrap();
        assert!(matches!(replayed.as_slice(), [(1, LogRecord::CommitParts { changes: 4, .. })]));
        let flat: Vec<_> = collect_parts(&dir, 1, &rec).into_iter().flatten().collect();
        let tables: Vec<ObjectId> = flat.iter().map(|(t, _)| *t).collect();
        assert_eq!(tables, vec![7, 8, 7, 7]);
        // a spool dropped before its commit leaves nothing behind
        let mut spool = Spool::create(&w.spool_dir()).unwrap();
        spool.write(&big_changes(10, 10)).unwrap();
        drop(spool);
        let names: Vec<String> = std::fs::read_dir(&dir).unwrap().map(|e| e.unwrap().file_name().to_string_lossy().into_owned()).collect();
        assert!(names.iter().all(|n| !n.starts_with("spool-")), "{names:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A record that cannot be framed is refused, not written with a
    /// wrapped length (which the next start would take for a torn tail).
    #[test]
    fn oversized_frames_are_refused() {
        assert_eq!(frame_len(10).unwrap(), 10);
        assert_eq!(frame_len(u32::MAX as usize).unwrap(), u32::MAX);
        assert_eq!(frame_len(u32::MAX as usize + 1).unwrap_err().code, codes::PROGRAM_LIMIT_EXCEEDED);
    }
}
