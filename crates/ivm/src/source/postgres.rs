//! PostgreSQL logical replication source (pgoutput).
//!
//! The source replicates published tables into the engine:
//! - streaming changes become upserts/deletes by primary key (before-images
//!   come from the engine's stored rows, so REPLICA IDENTITY FULL is not
//!   needed; unchanged TOAST values are filled from the stored row);
//! - the initial snapshot (and the snapshot of tables added later) is taken
//!   in chunks in the order of the upstream key index, interleaved with the
//!   stream and ordered against it by DBLog-style low/high watermarks
//!   written into the WAL with pg_logical_emit_message. Streamed changes
//!   apply; in the watermark window they win over the chunk's rows of their
//!   keys, which are looked up, never compared in an order. Chunks are read
//!   by a copy thread on a connection of its own, while the stream goes on;
//! - the slot is acknowledged as soon as a commit is durable in the
//!   engine's input log;
//! - a transaction is collected until its commit, in memory up to
//!   `log::PART_BYTES` of changes and beyond that in a spool file of the
//!   log directory (`log::Spool`), so that its size is not bounded by
//!   memory.

use super::{SourceCtl, SourceTable};
use crate::catalog::SourceDef;
use crate::coord::ddl::DiscoveredTable;
use crate::coord::EngineHandle;
use crate::dataflow::TableChange;
use crate::error::{codes, Result, SqlError};
use crate::log::{LogRecord, Spool, PART_BYTES};
use crate::sql::plan::ObjectId;
use crate::types::encoding::{encode_key, SortSpec};
use crate::types::text::{format_datum, parse_datum};
use crate::types::{Column, ColumnType, Datum, Row, SqlType};
use crossbeam_channel::{Receiver, Sender};
use parking_lot::Mutex;
use pgproto::pgoutput::{LogicalMessage, Relation, TupleValue};
use pgproto::{Client, Config, Lsn, ReplicationEvent, ReplicationStream};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const CHUNK_ROWS: usize = 10_000;

/// Records in flight to the log beyond which the source stops reading the
/// stream until they are durable (see `PgSource::backlogged`).
const MAX_INFLIGHT: usize = 10_000;

/// Keys read again in one chunk (see `SnapState::reread`).
const REREAD_KEYS: usize = 1000;

/// Events the copy thread queues before it waits for the source (pieces of
/// a keyless table: the source logs them between upstream transactions).
const COPY_QUEUE: usize = 2;

/// Longest wait, at a chunk's high watermark, for the rows the copy thread
/// sent before writing it (they are there already).
const COPY_AWAIT: Duration = Duration::from_secs(10);

/// Delay before reconnecting after a failed session: doubles from the
/// minimum up to the maximum while sessions keep failing.
const RECONNECT_MIN: Duration = Duration::from_millis(200);
const RECONNECT_MAX: Duration = Duration::from_secs(10);

/// Delay before retrying a table whose snapshot chunk could not be read (or
/// any chunk, after the copy thread's connection failed): doubles from the
/// minimum up to the maximum while it keeps failing.
const TABLE_RETRY_MIN: Duration = Duration::from_secs(1);
const TABLE_RETRY_MAX: Duration = Duration::from_secs(60);

/// Delay before retrying a chunk whose snapshot did not see every decoded
/// transaction yet (see `snapshot_sees_decoded`).
const CHUNK_WAIT: Duration = Duration::from_millis(100);
/// Recently decoded transactions remembered (see `PgSource::recent`).
const RECENT_XIDS: usize = 1024;
/// Above this many unseen transactions, they are checked against a fresh
/// snapshot (at most once a second) instead of waiting for a chunk.
const UNSEEN_MAX: usize = 10_000;

fn qident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn qlit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Map an upstream type to an engine type (unknown types are kept as text).
fn map_type(oid: u32, typmod: i32) -> (SqlType, i32) {
    match SqlType::from_oid(oid) {
        Some(SqlType::Unknown) | None => (SqlType::Text, -1),
        Some(t) => (t, typmod),
    }
}

fn connect(conninfo: &str, replication: bool) -> Result<Client> {
    let mut cfg = Config::parse(conninfo)?;
    if !cfg.options.iter().any(|(k, _)| k.eq_ignore_ascii_case("timezone")) {
        cfg.options.push(("TimeZone".into(), "UTC".into()));
    }
    if !cfg.options.iter().any(|(k, _)| k.eq_ignore_ascii_case("application_name")) && cfg.application_name.is_none() {
        cfg.application_name = Some("ivm".into());
    }
    let c = if replication { Client::connect_replication(&cfg)? } else { Client::connect(&cfg)? };
    check_encoding(&c)?;
    Ok(c)
}

/// Values must arrive as UTF-8. A SQL_ASCII database stores bytes without
/// checking any encoding, and its text is sent unconverted, so it can hold
/// values that are not UTF-8; another client encoding (set in the
/// connection string) would make the server convert into it.
fn check_encoding(c: &Client) -> Result<()> {
    if c.parameter("server_encoding").is_some_and(|e| e.eq_ignore_ascii_case("SQL_ASCII")) {
        return Err(SqlError::new(
            codes::FEATURE_NOT_SUPPORTED,
            "the upstream database's encoding is SQL_ASCII, which does not validate text: values may not be valid UTF-8",
        )
        .with_hint("Replicate from a database with a real encoding, such as UTF8."));
    }
    match c.parameter("client_encoding") {
        Some(e) if !e.eq_ignore_ascii_case("UTF8") => Err(SqlError::new(
            codes::FEATURE_NOT_SUPPORTED,
            format!("source connections must use client_encoding UTF8, not {e}"),
        )
        .with_hint("Remove client_encoding from the connection string.")),
        _ => Ok(()),
    }
}

/// Discover the tables of a publication (columns, primary keys, replica identity).
pub fn discover(conninfo: &str, publication: &str, only: Option<&[String]>) -> Result<Vec<DiscoveredTable>> {
    let mut c = connect(conninfo, false)?;
    c.simple_query("SET search_path = pg_catalog")?;
    let tabs = c.query(&format!(
        "SELECT n.nspname, cl.relname, cl.oid, cl.relreplident \
         FROM pg_publication_tables pt \
         JOIN pg_namespace n ON n.nspname = pt.schemaname \
         JOIN pg_class cl ON cl.relnamespace = n.oid AND cl.relname = pt.tablename \
         WHERE pt.pubname = {} ORDER BY 1, 2",
        qlit(publication)
    ))?;
    if tabs.rows.is_empty() {
        let p = c.query(&format!("SELECT 1 FROM pg_publication WHERE pubname = {}", qlit(publication)))?;
        if p.rows.is_empty() {
            return Err(SqlError::new(codes::UNDEFINED_OBJECT, format!("publication \"{publication}\" does not exist")));
        }
    }
    let mut out = Vec::new();
    for row in &tabs.rows {
        let schema = row[0].clone().unwrap_or_default();
        let name = row[1].clone().unwrap_or_default();
        if let Some(only) = only
            && !only.iter().any(|t| *t == name || *t == format!("{schema}.{name}"))
        {
            continue;
        }
        let oid = row[2].clone().unwrap_or_default();
        let ident = row[3].clone().unwrap_or_default();
        let cols = c.query(&format!(
            "SELECT a.attname, a.atttypid, a.atttypmod, a.attnotnull FROM pg_attribute a \
             WHERE a.attrelid = {oid} AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum"
        ))?;
        let mut columns = Vec::new();
        for cr in &cols.rows {
            let cname = cr[0].clone().unwrap_or_default();
            let toid: u32 = cr[1].as_deref().unwrap_or("25").parse().unwrap_or(25);
            let tm: i32 = cr[2].as_deref().unwrap_or("-1").parse().unwrap_or(-1);
            let notnull = cr[3].as_deref() == Some("t");
            let (ty, typmod) = map_type(toid, tm);
            columns.push(Column::new(cname, ColumnType { ty, typmod, nullable: !notnull }));
        }
        // The key: the columns of the replica identity index (REPLICA
        // IDENTITY USING INDEX), which are all that deletes and key changes
        // carry of the old row; else the primary key. (With DEFAULT the
        // identity is the primary key; FULL sends whole old rows.)
        let keys = c.query(&format!(
            "SELECT i.indisprimary, i.indisreplident, a.attname \
             FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
             WHERE i.indrelid = {oid} AND (i.indisprimary OR i.indisreplident) \
             ORDER BY i.indexrelid, array_position(i.indkey::int2[], a.attnum)"
        ))?;
        let key_of = |flag: usize| -> Vec<usize> {
            keys.rows
                .iter()
                .filter(|r| r[flag].as_deref() == Some("t"))
                .filter_map(|r| r[2].as_ref())
                .filter_map(|n| columns.iter().position(|c| &c.name == n))
                .collect()
        };
        let identity = if ident == "i" { key_of(1) } else { vec![] };
        // (an identity index that was dropped leaves REPLICA IDENTITY NOTHING)
        let pk = if identity.is_empty() { key_of(0) } else { identity };
        out.push(DiscoveredTable { schema, name, columns, pk, replica_identity_full: ident == "f" });
    }
    Ok(out)
}

#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct SnapState {
    pub done: bool,
    /// Text values of the last copied key: the table's next chunk reads the
    /// keys after it, in the order of the upstream key index.
    pub boundary: Option<Vec<String>>,
    /// The stored rows cannot be trusted (changes may have been lost, or
    /// were decoded with a key that no longer applies): the snapshot starts
    /// by truncating the table, so that rows deleted upstream go away too.
    #[serde(default)]
    pub truncate: bool,
    /// `boundary` is in the order of the upstream key index. Boundaries
    /// logged by earlier versions lack it: they are in the engine's key
    /// order, which differs for keys with string columns (see `PgSource::new`).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub index_order: bool,
    /// Every key range has been copied: only the keys of `reread` remain.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub scanned: bool,
    /// Keys (text values) whose rows are to be read again: the stream moved
    /// a row there leaving TOASTed values unchanged, which neither the chunk
    /// in flight nor (perhaps) the engine had (see `PgSource::add_reread`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reread: Vec<Vec<String>>,
}

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct SourceState {
    pub slot_created: bool,
    pub tables: BTreeMap<ObjectId, SnapState>,
}

/// The entries of a source's state that changed, as a `ProgressDelta`
/// record carries them (the others stay as they are).
#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct StateDelta {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot_created: Option<bool>,
    #[serde(default)]
    pub tables: BTreeMap<ObjectId, SnapState>,
}

impl SourceState {
    /// The state a `Progress` record (or a checkpoint) holds. One that does
    /// not parse is empty: the slot counts as not created, and every table
    /// is copied again.
    pub fn parse(state: &str) -> SourceState {
        serde_json::from_str(state).unwrap_or_default()
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    /// Apply the change a `ProgressDelta` record carries; returns the
    /// tables whose entries it replaced (None: all of the state). One that
    /// does not parse empties the state, as a full state that does not
    /// parse would.
    pub fn apply(&mut self, delta: &str) -> Option<Vec<ObjectId>> {
        match serde_json::from_str::<StateDelta>(delta) {
            Ok(d) => {
                if let Some(created) = d.slot_created {
                    self.slot_created = created;
                }
                let changed = d.tables.keys().copied().collect();
                self.tables.extend(d.tables);
                Some(changed)
            }
            Err(e) => {
                log::error!("a source's progress record does not parse ({e}): its slot is created and its tables copied again");
                *self = SourceState::default();
                None
            }
        }
    }

    /// The state without its tables' entries (which the metadata tree
    /// keeps one by one).
    pub fn head(&self) -> SourceState {
        SourceState { slot_created: self.slot_created, tables: BTreeMap::new() }
    }

    /// The snapshot of `table` is to be done again (as the source does on a
    /// schema change). A pending truncate is kept: the source logs its reset
    /// state, with the truncate a key change needs, just before the schema
    /// record, and the table's copy may start long after it.
    pub fn snapshot_again(&mut self, table: ObjectId) {
        let truncate = self.tables.get(&table).is_some_and(|t| t.truncate);
        self.tables.insert(table, SnapState { truncate, ..SnapState::default() });
    }
}

/// Mapping of an upstream relation onto a catalog table.
struct RelMap {
    table: ObjectId,
    /// catalog column index for each relation column (None: column unknown)
    cols: Vec<Option<usize>>,
}

/// A transaction snapshot as pg_current_snapshot() shows it: xmin, xmax and
/// the transactions in progress.
type Snap = (u64, u64, Vec<u64>);

#[derive(PartialEq, Clone, Copy, Debug)]
enum Phase {
    BeforeLow,
    InWindow,
}

/// A streamed change to the table of a chunk whose rows had not arrived yet:
/// what it does to them is decided when they arrive (`Chunk::resolve`).
enum Touch {
    Key { key: Vec<u8>, phase: Phase, xid: Option<u32> },
    Truncate { phase: Phase, xid: Option<u32> },
    /// An upsert of `row` leaving the values of columns `unchanged` (TOASTed)
    /// as they were in the row of key `from` (`key`, or the old key of a
    /// key change).
    Toast { key: Vec<u8>, from: Vec<u8>, row: Row, unchanged: Vec<usize>, phase: Phase, xid: Option<u32> },
}

/// A snapshot chunk in flight: the next rows of a keyed table, or the whole
/// of a keyless one, which the copy thread reads.
struct Chunk {
    table: ObjectId,
    /// The request to the copy thread (its events carry it).
    seq: u64,
    low: String,
    high: String,
    phase: Phase,
    keyed: bool,
    /// The snapshot the chunk is read in, once the copy thread took it.
    snap: Option<Snap>,
    /// Keyed tables: the chunk's rows by encoded key, once read.
    rows: Option<BTreeMap<Vec<u8>, Row>>,
    /// Keyed tables: streamed changes to the table before the rows arrived.
    touched: Vec<Touch>,
    /// Keyed tables: the text of the chunk's last key, and whether the
    /// chunk is the table's last.
    last_key: Option<Vec<String>>,
    last: bool,
    /// Keyless tables: changes decoded before the snapshot was known, with
    /// their transaction.
    unresolved: Vec<(Option<u32>, TableChange)>,
    /// Keyless tables: changes the snapshot does not contain, applied after
    /// all of its rows.
    pending: Vec<TableChange>,
    /// Keyless tables: every row has been read (and logged).
    complete: bool,
    /// The keys a chunk reading keys again reads (instead of a key range).
    reread: Option<Vec<Vec<String>>>,
}

impl Chunk {
    /// The rows arrived: the stream's changes to the table since the chunk
    /// started take away the rows they are newer than (see `stream_wins`).
    fn resolve(&mut self) {
        let Some(rows) = &mut self.rows else { return };
        for t in std::mem::take(&mut self.touched) {
            match t {
                Touch::Key { key, phase, xid } => {
                    if stream_wins(phase, &self.snap, xid) {
                        rows.remove(&key);
                    }
                }
                Touch::Truncate { phase, xid } => {
                    if stream_wins(phase, &self.snap, xid) {
                        rows.clear();
                    }
                }
                // The engine had no row to fill the upsert's unchanged values
                // from, unless the table is being copied again: the chunk's
                // row takes the upsert's values instead (with the unchanged
                // ones from the chunk's row of the old key, or of the key if
                // the snapshot contains the change), and is logged with the
                // chunk. Later changes of the key are resolved against it.
                Touch::Toast { key, from, mut row, unchanged, phase, xid } => {
                    if !stream_wins(phase, &self.snap, xid) {
                        continue;
                    }
                    // (the old key's row in a snapshot that contains the key
                    // change was added after it, as in `handle_message`)
                    let base = if visible(&self.snap, xid) {
                        if from != key {
                            rows.remove(&from);
                        }
                        rows.remove(&key)
                    } else {
                        rows.remove(&from).or_else(|| rows.remove(&key))
                    };
                    if let Some(base) = base {
                        for c in &unchanged {
                            row[*c] = base[*c].clone();
                        }
                        rows.insert(key, row);
                    }
                }
            }
        }
    }

    /// Has everything the copy thread sends before the high watermark arrived?
    fn read(&self) -> bool {
        if self.keyed { self.rows.is_some() } else { self.complete }
    }
}

/// What the current replication session achieved (see `run`).
#[derive(Default)]
struct Session {
    /// When streaming started.
    streaming_since: Option<Instant>,
    /// An upstream transaction was processed.
    progressed: bool,
}

/// A table whose snapshot chunk could not be read, retried at `retry_at`.
struct TableFailure {
    error: String,
    delay: Duration,
    retry_at: Instant,
}

/// Why a snapshot chunk could not be read.
enum ChunkError {
    /// The table cannot be read (now), but the connection is fine.
    Table(SqlError),
    /// The copy thread's connection failed.
    Session(SqlError),
}

struct Txn {
    xid: u32,
    /// Changes not written to the spool (all of them while they are few).
    changes: Vec<(ObjectId, TableChange)>,
    /// Estimated encoded size of `changes`.
    bytes: usize,
    /// The earlier changes, once they took `PART_BYTES` (see `spill`).
    spool: Option<Spool>,
    /// Chunk rows to emit at this transaction's commit (high watermark).
    finish_chunk: bool,
    /// Keys to read again were added (logged before the commit).
    reread: bool,
}

/// What the source asks of its copy thread.
enum CopyRequest {
    /// Read the chunk of `table` after `boundary` (all of a keyless table;
    /// the rows of `keys`, if given) between the watermarks `low` and
    /// `high`, in a snapshot that sees the transactions `unseen`.
    Chunk {
        seq: u64,
        table: Box<SourceTable>,
        boundary: Option<Vec<String>>,
        keys: Option<Vec<Vec<String>>>,
        low: String,
        high: String,
        unseen: Vec<u32>,
    },
    /// Take a fresh snapshot (answered by a `Snapshot` event with seq 0).
    Snapshot,
}

/// What the copy thread reports about chunk `seq`, in this order: either
/// `NotVisible`, or `Rows` (keyed tables) or `Snapshot` and `Piece`s
/// (keyless tables); and `Failed` at any point, also after the rows when
/// the high watermark cannot be written.
enum CopyEvent {
    Snapshot { seq: u64, snap: Snap },
    /// The snapshot does not see every transaction decoded before the
    /// chunk: it was not read (see `snapshot_sees_decoded`).
    NotVisible { seq: u64, snap: Snap },
    Rows { seq: u64, snap: Snap, rows: Vec<(Vec<u8>, Row)>, last_key: Option<Vec<String>>, last: bool },
    Piece { seq: u64, rows: Vec<Row>, last: bool },
    Failed { seq: u64, error: ChunkError },
}

impl CopyEvent {
    fn seq(&self) -> u64 {
        match self {
            CopyEvent::Snapshot { seq, .. }
            | CopyEvent::NotVisible { seq, .. }
            | CopyEvent::Rows { seq, .. }
            | CopyEvent::Piece { seq, .. }
            | CopyEvent::Failed { seq, .. } => *seq,
        }
    }
}

/// The copy thread of a source, which reads its snapshot chunks on a
/// connection of its own while the source keeps streaming (see `copy_thread`).
struct Copier {
    requests: Sender<CopyRequest>,
    events: Receiver<CopyEvent>,
    /// The chunk the source waits for (0: none); the thread drops others.
    wanted: Arc<AtomicU64>,
}

impl Copier {
    fn start(id: ObjectId, conninfo: String) -> Result<Copier> {
        let (requests, rrx) = crossbeam_channel::unbounded();
        let (etx, events) = crossbeam_channel::bounded(COPY_QUEUE);
        let wanted = Arc::new(AtomicU64::new(0));
        let w = wanted.clone();
        std::thread::Builder::new().name(format!("ivm-copy-{id}")).spawn(move || copy_thread(&conninfo, rrx, etx, &w))?;
        Ok(Copier { requests, events, wanted })
    }
}

struct PgSource {
    id: ObjectId,
    def: SourceDef,
    engine: EngineHandle,
    tables: HashMap<ObjectId, SourceTable>,
    rels: HashMap<u32, RelMap>,
    state: SourceState,
    resume_lsn: u64,
    durable_lsn: u64,
    txn: Option<Txn>,
    chunk: Option<Chunk>,
    status: Arc<Mutex<String>>,
    /// Random id of this run of the source and the number of chunks it
    /// started: they make watermark tags unique (see `watermarks`).
    run_id: u64,
    seq: u64,
    /// Records sent to the log whose durability is not yet confirmed, with
    /// the LSN durable once they are (commits: their own; others: the
    /// last commit's before them).
    inflight: std::collections::VecDeque<(u64, Receiver<u64>)>,
    /// wal_end of a keepalive received while commits were in flight.
    pending_wal_end: u64,
    gauges: Arc<super::SourceGauges>,
    /// Relations announced in this session for tables the source does not
    /// replicate (yet). pgoutput announces a relation once per session, so
    /// a table added later (ALTER SOURCE REFRESH) is mapped from these.
    unmapped_rels: HashMap<u32, Relation>,
    session: Session,
    /// Tables whose snapshot chunk failed (see `maybe_start_chunk`).
    failing: BTreeMap<ObjectId, TableFailure>,
    /// The copy thread's connection failed: no chunk starts before
    /// `retry_at`.
    copy_failure: Option<TableFailure>,
    /// Tables whose snapshot is not done, in the order they are copied.
    incomplete: BTreeSet<ObjectId>,
    /// Tables whose entry in `state` changed since the last logged
    /// progress, and whether `slot_created` did (see `log_progress`).
    dirty: BTreeSet<ObjectId>,
    slot_dirty: bool,
    /// The copy thread, started with the first chunk.
    copier: Option<Copier>,
    /// Transactions decoded while some table is being copied that no
    /// chunk's snapshot has seen yet (see `snapshot_sees_decoded`).
    unseen: BTreeSet<u32>,
    /// The last transactions decoded, for `unseen` when a table starts
    /// (again) to be copied.
    recent: std::collections::VecDeque<u32>,
    /// No chunk before this (see `CHUNK_WAIT`).
    chunk_wait: Option<Instant>,
    /// When `unseen` was last checked against a fresh snapshot, and whether
    /// the copy thread is taking one.
    unseen_checked: Instant,
    snapshot_requested: bool,
}

fn pk_bytes(row: &[Datum], pk: &[usize]) -> Vec<u8> {
    let mut b = Vec::new();
    for c in pk {
        encode_key(&row[*c], SortSpec::ASC, &mut b);
    }
    b
}

/// Is the transaction `xid` visible in the snapshot (both known)?
fn visible(snap: &Option<Snap>, xid: Option<u32>) -> bool {
    matches!((snap, xid), (Some(s), Some(x)) if xid_visible(s, x))
}

/// Does a streamed change in `phase` win over a chunk's row of its key? In
/// the watermark window, yes: its value is at least as new. Before it, only
/// if the chunk's snapshot does not contain the transaction: one that
/// committed before the low watermark is normally in the snapshot, but a
/// commit becomes visible only after its WAL record, so the snapshot can
/// miss it, and then the chunk holds the older row.
fn stream_wins(phase: Phase, snap: &Option<Snap>, xid: Option<u32>) -> bool {
    phase == Phase::InWindow || !visible(snap, xid)
}

impl PgSource {
    /// A source starting from its logged `state`.
    #[allow(clippy::too_many_arguments)]
    fn new(
        id: ObjectId,
        def: SourceDef,
        engine: EngineHandle,
        tables: Vec<SourceTable>,
        mut state: SourceState,
        resume_lsn: u64,
        status: Arc<Mutex<String>>,
        gauges: Arc<super::SourceGauges>,
    ) -> PgSource {
        let tables: HashMap<ObjectId, SourceTable> = tables.into_iter().map(|t| (t.id, t)).collect();
        // A boundary logged by an earlier version is in the engine's key
        // order, which its chunks followed by comparing string key columns
        // as their text in the C collation (other keys in their upstream
        // order). For a key without string columns that is the index order;
        // otherwise the next chunk could miss keys, so the table's copy
        // starts again from its first key (it only upserts what it reads).
        for (id, st) in &mut state.tables {
            let Some(b) = &st.boundary else { continue };
            if st.index_order {
                continue;
            }
            match tables.get(id) {
                Some(t) if b.len() == t.pk.len() && t.pk.iter().all(|c| !t.columns[*c].ty.ty.is_string()) => st.index_order = true,
                _ => {
                    log::info!("source {id}: the snapshot of table {id} was interrupted by an upgrade; copying it again from its first key");
                    st.boundary = None;
                }
            }
        }
        let incomplete = tables.keys().copied().filter(|t| !state.tables.get(t).is_some_and(|s| s.done)).collect();
        PgSource {
            id,
            def,
            engine,
            tables,
            rels: HashMap::new(),
            state,
            resume_lsn,
            durable_lsn: resume_lsn,
            txn: None,
            chunk: None,
            status,
            run_id: crate::source::random_u64(),
            seq: 0,
            inflight: std::collections::VecDeque::new(),
            pending_wal_end: 0,
            gauges,
            unmapped_rels: HashMap::new(),
            session: Session::default(),
            failing: BTreeMap::new(),
            copy_failure: None,
            incomplete,
            dirty: BTreeSet::new(),
            slot_dirty: false,
            copier: None,
            unseen: BTreeSet::new(),
            recent: Default::default(),
            chunk_wait: None,
            unseen_checked: Instant::now(),
            snapshot_requested: false,
        }
    }

    fn table_of(&self, rel: &Relation) -> Option<ObjectId> {
        self.tables.values().find(|t| t.upstream.0 == rel.namespace && t.upstream.1 == rel.name).map(|t| t.id)
    }

    fn convert(&self, rm: &RelMap, tuple: &[TupleValue]) -> Result<(Row, Vec<usize>)> {
        let t = &self.tables[&rm.table];
        let mut row: Row = vec![Datum::Null; t.columns.len()];
        let mut unchanged = Vec::new();
        for (i, v) in tuple.iter().enumerate() {
            let Some(Some(c)) = rm.cols.get(i) else { continue };
            match v {
                TupleValue::Null => {}
                TupleValue::UnchangedToast => unchanged.push(*c),
                TupleValue::Text(_) | TupleValue::Binary(_) => {
                    let col = &t.columns[*c];
                    // (values are requested in text format, as UTF-8)
                    let Some(s) = v.as_str() else {
                        return Err(SqlError::new(
                            codes::CHARACTER_NOT_IN_REPERTOIRE,
                            format!("value of column \"{}\" of {}.{} is not valid UTF-8", col.name, t.upstream.0, t.upstream.1),
                        ));
                    };
                    row[*c] = parse_datum(s, &col.ty.ty, -1)?;
                }
            }
        }
        Ok((row, unchanged))
    }

    /// Is the snapshot of `table` done?
    fn done(&self, table: ObjectId) -> bool {
        !self.incomplete.contains(&table)
    }

    /// Decide what a streamed change to `table` with primary key bytes `key`
    /// does. Returns Some(fill row) to apply it (fill row: the chunk's row of
    /// the key, for unchanged TOAST values), None to skip it.
    ///
    /// A change applies, except one whose transaction the chunk in flight
    /// already contains, for a key the chunk has: the chunk's row is at least
    /// as new. Where the stream wins over the chunk (`stream_wins`), the
    /// chunk's row of the key goes. Keys are only looked up, never compared
    /// in an order: a key the chunk does not have is copied by an earlier
    /// chunk or a later one, whose snapshot contains the change (see
    /// `snapshot_sees_decoded`), or by none (it no longer exists).
    fn snapshot_filter(&mut self, table: ObjectId, key: &[u8]) -> Option<Option<Row>> {
        let xid = self.txn.as_ref().map(|t| t.xid);
        let Some(ch) = self.chunk.as_mut().filter(|ch| ch.table == table) else { return Some(None) };
        let Some(rows) = &mut ch.rows else {
            // decided when the rows arrive
            ch.touched.push(Touch::Key { key: key.to_vec(), phase: ch.phase, xid });
            return Some(None);
        };
        if !rows.contains_key(key) {
            return Some(None);
        }
        if stream_wins(ch.phase, &ch.snap, xid) { Some(rows.remove(key)) } else { None }
    }

    fn handle_message(&mut self, msg: LogicalMessage) -> Result<()> {
        self.decode_message(msg)?;
        self.spill()
    }

    /// Write the open transaction's changes to its spool once they take
    /// `PART_BYTES`: upstream transactions arrive whole, between Begin and
    /// Commit (pgoutput protocol version 1 sends only committed ones), and
    /// can be larger than memory. (Without a spool directory, which only
    /// tests lack, they are held.)
    fn spill(&mut self) -> Result<()> {
        let Some(t) = &mut self.txn else { return Ok(()) };
        if t.bytes < PART_BYTES {
            return Ok(());
        }
        let Some(dir) = self.engine.shared.spool.get() else { return Ok(()) };
        let spool = match &mut t.spool {
            Some(s) => s,
            None => t.spool.insert(Spool::create(dir)?),
        };
        spool.write(&t.changes)?;
        t.changes = Vec::new();
        t.bytes = 0;
        Ok(())
    }

    fn decode_message(&mut self, msg: LogicalMessage) -> Result<()> {
        match msg {
            LogicalMessage::Begin { xid, .. } => {
                self.txn = Some(Txn { xid, changes: Vec::new(), bytes: 0, spool: None, finish_chunk: false, reread: false });
            }
            LogicalMessage::Relation(rel) => self.handle_relation(rel)?,
            LogicalMessage::Insert { relation_id, new } => {
                let Some(rm) = self.rels.get(&relation_id) else { return Ok(()) };
                let table = rm.table;
                let (row, unchanged) = match self.convert(rm, &new) {
                    Ok(r) => r,
                    Err(e) => return self.change_failed(table, e),
                };
                self.push_upsert(table, row, unchanged, None)?;
            }
            LogicalMessage::Update { relation_id, old, old_is_key, new } => {
                let Some(rm) = self.rels.get(&relation_id) else { return Ok(()) };
                let table = rm.table;
                let converted = self.convert(rm, &new).and_then(|(row, unchanged)| match &old {
                    Some(o) => Ok((row, unchanged, Some(self.convert(rm, o)?.0))),
                    None => Ok((row, unchanged, None)),
                });
                let (row, unchanged, old_row) = match converted {
                    Ok(r) => r,
                    Err(e) => return self.change_failed(table, e),
                };
                let pk = self.tables[&table].pk.clone();
                if pk.is_empty() {
                    // REPLICA IDENTITY FULL multiset table
                    let mut new_row = row;
                    if let Some(o) = &old_row {
                        for c in &unchanged {
                            new_row[*c] = o[*c].clone();
                        }
                        self.push_multiset(table, TableChange::Remove { row: o.clone() });
                    }
                    self.push_multiset(table, TableChange::Insert { row: new_row });
                    return Ok(());
                }
                // key change: delete the old key first
                let mut moved = None;
                let mut from = None;
                if let Some(o) = &old_row
                    && (old_is_key || !o.is_empty())
                {
                    let ok: Row = pk.iter().map(|c| o[*c].clone()).collect();
                    if pk_bytes(o, &pk) != pk_bytes(&row, &pk) && !ok.iter().any(|d| d.is_null()) {
                        let kb = pk_bytes(o, &pk);
                        // deletes always apply (see the Delete case); where the
                        // stream wins over the chunk, its row of the old key
                        // has the values of the new row's unchanged columns,
                        // if the chunk's snapshot does not contain the key
                        // change (else that row, if any, was added later
                        // under the old key; the new key's row has them).
                        // Decided with the new key's upsert if the chunk's
                        // rows have not arrived yet.
                        if !(old_is_key && !unchanged.is_empty() && self.rows_pending(table)) {
                            let sees = self.chunk.as_ref().is_some_and(|ch| ch.table == table && visible(&ch.snap, self.txn.as_ref().map(|t| t.xid)));
                            moved = self.snapshot_filter(table, &kb).flatten().filter(|_| !sees);
                        }
                        from = Some(kb);
                        self.push_change(table, TableChange::Delete { key: ok });
                    }
                }
                // a key-only old tuple (a key change under REPLICA IDENTITY
                // DEFAULT) has no values for unchanged TOAST columns: they
                // come from the old key's row, in the chunk in flight or else
                // (filled in by the engine) the row stored under the old key
                if old_is_key {
                    let reread = from.is_some() && !unchanged.is_empty() && moved.is_none() && !self.done(table);
                    let key: Vec<String> = pk.iter().map(|c| format_datum(&row[*c])).collect();
                    self.push_upsert_from(table, row, unchanged, moved, from)?;
                    if reread {
                        self.add_reread(table, key);
                    }
                } else {
                    self.push_upsert(table, row, unchanged, old_row)?;
                }
            }
            LogicalMessage::Delete { relation_id, old, .. } => {
                let Some(rm) = self.rels.get(&relation_id) else { return Ok(()) };
                let table = rm.table;
                let row = match self.convert(rm, &old) {
                    Ok((row, _)) => row,
                    Err(e) => return self.change_failed(table, e),
                };
                let pk = self.tables[&table].pk.clone();
                if pk.is_empty() {
                    self.push_multiset(table, TableChange::Remove { row });
                    return Ok(());
                }
                // A delete always applies, whatever the snapshot filter says
                // (which still decides what happens to the chunk's row): of a
                // key not copied yet it removes what the stream applied to
                // it, or, if the table is being copied again after a schema
                // change, its old row, which no chunk would remove: chunks
                // only upsert the rows that exist.
                let kb = pk_bytes(&row, &pk);
                let _ = self.snapshot_filter(table, &kb);
                let key: Row = pk.iter().map(|c| row[*c].clone()).collect();
                self.push_change(table, TableChange::Delete { key });
            }
            LogicalMessage::Truncate { relation_ids, .. } => {
                let xid = self.txn.as_ref().map(|t| t.xid);
                for r in relation_ids {
                    let Some(t) = self.rels.get(&r).map(|rm| rm.table) else { continue };
                    if self.tables[&t].pk.is_empty() {
                        self.push_multiset(t, TableChange::Truncate);
                        continue;
                    }
                    self.push_change(t, TableChange::Truncate);
                    // the chunk's rows go too, unless its snapshot already
                    // reflects the truncate (a chunk read after it)
                    if let Some(ch) = self.chunk.as_mut().filter(|ch| ch.table == t) {
                        match &mut ch.rows {
                            Some(rows) => {
                                if stream_wins(ch.phase, &ch.snap, xid) {
                                    rows.clear();
                                }
                            }
                            None => ch.touched.push(Touch::Truncate { phase: ch.phase, xid }),
                        }
                    }
                }
            }
            LogicalMessage::Message { prefix, content, .. } => {
                if prefix == "ivm" {
                    let tag = String::from_utf8_lossy(&content).to_string();
                    if let Some(ch) = &mut self.chunk {
                        if tag == ch.low {
                            ch.phase = Phase::InWindow;
                        } else if tag == ch.high
                            && let Some(t) = &mut self.txn
                        {
                            t.finish_chunk = true;
                        }
                    }
                }
            }
            LogicalMessage::Commit { end_lsn, .. } => {
                let txn = self.txn.take();
                if end_lsn.0 <= self.resume_lsn {
                    // already logged before a restart
                    return Ok(());
                }
                self.session.progressed = true;
                let Some(mut txn) = txn else { return Ok(()) };
                self.note_decoded(txn.xid);
                if txn.reread {
                    // before the commit: should the commit be logged and the
                    // keys not, the restarted stream would resume after it
                    self.log_progress()?;
                }
                let progress = txn.finish_chunk && self.finish_chunk(&mut txn.changes)?;
                let n = txn.changes.len() + txn.spool.as_ref().map_or(0, |s| s.changes() as usize);
                if n > 0 {
                    self.gauges.commit(n);
                    // pipelined: the sequencer group-commits; durability is
                    // confirmed asynchronously before acknowledging the slot
                    let rx = match txn.spool {
                        // (the spool becomes the commit's parts file)
                        Some(mut spool) => {
                            spool.write(&txn.changes)?;
                            self.engine.log_spooled(self.id, end_lsn.0, spool.finish()?)?
                        }
                        // (no waiting for room: see `backlogged`)
                        None => self.engine.log_now(LogRecord::Commit { source: self.id, lsn: end_lsn.0, changes: txn.changes })?,
                    };
                    self.inflight.push_back((end_lsn.0, rx));
                } else {
                    let (tx, rx) = crossbeam_channel::bounded(1);
                    let _ = tx.send(0);
                    self.inflight.push_back((end_lsn.0, rx));
                }
                if progress {
                    self.log_progress()?;
                }
                self.drain_acks(false)?;
            }
            LogicalMessage::Origin { .. } | LogicalMessage::Type { .. } => {}
        }
        Ok(())
    }

    /// The transaction carrying the chunk's high watermark commits: the
    /// chunk's remaining rows join its changes (for a keyless table, the
    /// changes its snapshot does not contain, after the pieces not logged
    /// yet), and the table's state moves on. False if the chunk was
    /// abandoned meanwhile.
    fn finish_chunk(&mut self, changes: &mut Vec<(ObjectId, TableChange)>) -> Result<bool> {
        // the copy thread sent all of it before writing the high watermark
        let deadline = Instant::now() + COPY_AWAIT;
        while let Some(ch) = &self.chunk
            && !ch.read()
        {
            match self.copier.as_ref().map(|c| c.events.recv_deadline(deadline)) {
                Some(Ok(ev)) => self.copy_event(ev)?,
                _ => {
                    log::warn!("source {}: a snapshot chunk's rows did not arrive by its high watermark; reading it again", self.id);
                    self.abandon_chunk();
                    self.refresh_status();
                }
            }
        }
        let Some(ch) = self.chunk.take() else { return Ok(false) };
        self.abandon_chunk();
        let t = ch.table;
        let mut st = self.state.tables.get(&t).cloned().unwrap_or_default();
        if ch.keyed {
            changes.extend(ch.rows.into_iter().flatten().map(|(_, row)| (t, TableChange::Upsert { row, unchanged: vec![] })));
            match ch.reread {
                Some(keys) => st.reread.retain(|k| !keys.contains(k)),
                None => {
                    if let Some(k) = ch.last_key {
                        st.boundary = Some(k);
                        st.index_order = true;
                    }
                    st.scanned = ch.last;
                }
            }
            st.done = st.scanned && st.reread.is_empty();
        } else {
            changes.extend(ch.pending.into_iter().map(|c| (t, c)));
            st.done = true;
        }
        self.set_snap(t, st);
        self.refresh_status();
        if !self.copying() {
            self.unseen.clear();
        }
        Ok(true)
    }

    /// A streamed change to `table` has a value that cannot be converted:
    /// the table fails, not the session. Ending the session would fail the
    /// same message again in every new one, and stop every table. Instead
    /// the table's changes in this transaction are dropped and it is copied
    /// again, from an empty table (the lost change may have deleted rows):
    /// like any table being copied, fresh reads of it wait until it is
    /// copied, and a copy that fails (the value is still there) is retried
    /// with a growing delay, shown in the status, while the other tables
    /// are streamed. The reset is logged, so a restart copies the table too.
    fn change_failed(&mut self, table: ObjectId, e: SqlError) -> Result<()> {
        let t = &self.tables[&table];
        log::warn!(
            "source {}: a change to table {}.{} cannot be applied: {e}; copying the table again, streaming the other tables",
            self.id,
            t.upstream.0,
            t.upstream.1
        );
        let truncate = !t.pk.is_empty();
        if let Some(txn) = &mut self.txn {
            txn.changes.retain(|(c, _)| *c != table);
            txn.bytes = txn.changes.iter().map(|(_, c)| crate::log::change_len(c)).sum();
            if let Some(spool) = &mut txn.spool {
                spool.rewrite(&mut |ch| ch.retain(|(c, _)| *c != table))?;
            }
        }
        self.set_snap(table, SnapState { truncate, ..Default::default() });
        self.start_copying();
        if self.chunk.as_ref().is_some_and(|c| c.table == table) {
            self.abandon_chunk();
        }
        self.refresh_status();
        self.log_progress()
    }

    /// A change to a keyless table (REPLICA IDENTITY FULL). Until its
    /// snapshot is done, the table is replaced by its copy, read in one
    /// snapshot: a change before the copy starts is dropped (the copy reads
    /// its effect), as is one the copy's snapshot contains. One the snapshot
    /// does not contain is kept and applied after all of the copy's rows;
    /// one decoded before the snapshot is known waits for it.
    fn push_multiset(&mut self, table: ObjectId, change: TableChange) {
        if self.done(table) {
            return self.push_change(table, change);
        }
        let xid = self.txn.as_ref().map(|t| t.xid);
        let Some(ch) = self.chunk.as_mut().filter(|ch| ch.table == table) else { return };
        if ch.snap.is_none() {
            ch.unresolved.push((xid, change));
        } else if !visible(&ch.snap, xid) {
            ch.pending.push(change);
        }
    }

    fn push_change(&mut self, table: ObjectId, ch: TableChange) {
        match &mut self.txn {
            Some(t) => {
                t.bytes += crate::log::change_len(&ch);
                t.changes.push((table, ch));
            }
            None => log::warn!("change outside a transaction ignored"),
        }
    }

    fn push_upsert(&mut self, table: ObjectId, row: Row, unchanged: Vec<usize>, old: Option<Row>) -> Result<()> {
        self.push_upsert_from(table, row, unchanged, old, None)
    }

    /// Is a chunk of `table` in flight whose rows have not arrived yet?
    fn rows_pending(&self, table: ObjectId) -> bool {
        self.chunk.as_ref().is_some_and(|ch| ch.table == table && ch.keyed && ch.rows.is_none())
    }

    /// Push an upsert whose unchanged values (if any) are those of `old`, if
    /// given, else of the row of key `from` (a key change's old key) or of
    /// its own key.
    fn push_upsert_from(&mut self, table: ObjectId, mut row: Row, unchanged: Vec<usize>, old: Option<Row>, from: Option<Vec<u8>>) -> Result<()> {
        let pk = self.tables[&table].pk.clone();
        if pk.is_empty() {
            self.push_multiset(table, TableChange::Insert { row });
            return Ok(());
        }
        let kb = pk_bytes(&row, &pk);
        if !unchanged.is_empty() && old.is_none() && self.rows_pending(table) {
            // The values may have to come from the chunk's rows, which have
            // not arrived: decided when they do (`Chunk::resolve`). The engine
            // fills them meanwhile if it has the row.
            let xid = self.txn.as_ref().map(|t| t.xid);
            let ch = self.chunk.as_mut().unwrap();
            let from = from.unwrap_or_else(|| kb.clone());
            ch.touched.push(Touch::Toast { key: kb, from, row: row.clone(), unchanged: unchanged.clone(), phase: ch.phase, xid });
            self.push_change(table, TableChange::UpsertIfPresent { row, unchanged });
            return Ok(());
        }
        let Some(fill) = self.snapshot_filter(table, &kb) else { return Ok(()) };
        let mut unchanged = unchanged;
        if !unchanged.is_empty()
            && let Some(f) = old.or(fill)
        {
            for c in &unchanged {
                row[*c] = f[*c].clone();
            }
            unchanged.clear();
        }
        // The engine fills unchanged values from the stored row. While the
        // table is being copied, a key without one has not been copied yet,
        // and its copy will bring the whole row: the upsert does nothing.
        let change = if unchanged.is_empty() || self.done(table) {
            TableChange::Upsert { row, unchanged }
        } else {
            TableChange::UpsertIfPresent { row, unchanged }
        };
        self.push_change(table, change);
        Ok(())
    }

    /// A key change moved a row of `table`, which is being copied, to `key`,
    /// leaving TOASTed values unchanged: their values come from the row of
    /// the old key, which neither the chunk in flight had nor, if the old
    /// key has not been copied yet, the engine (`UpsertIfPresent` then
    /// changes nothing). Should the new key be one copied already, no chunk
    /// would bring its row: it is read again, by a chunk of its own (with a
    /// snapshot that contains the key change), before the table is done.
    /// Logged before the transaction's commit.
    fn add_reread(&mut self, table: ObjectId, key: Vec<String>) {
        // (a chunk reading it again already started: it reads it once more)
        if let Some(ch) = self.chunk.as_mut().filter(|ch| ch.table == table)
            && let Some(keys) = &mut ch.reread
        {
            keys.retain(|k| *k != key);
        }
        let mut st = self.state.tables.get(&table).cloned().unwrap_or_default();
        if !st.reread.contains(&key) {
            st.reread.push(key);
            self.set_snap(table, st);
        }
        if let Some(t) = &mut self.txn {
            t.reread = true;
        }
    }

    /// Start replicating more tables (their snapshots follow).
    fn add_tables(&mut self, tables: Vec<SourceTable>) -> Result<()> {
        for t in tables {
            self.add_table(t);
        }
        self.start_copying();
        let known: Vec<u32> = self.unmapped_rels.values().filter(|r| self.table_of(r).is_some()).map(|r| r.id).collect();
        for id in known {
            if let Some(rel) = self.unmapped_rels.remove(&id) {
                self.handle_relation(rel)?;
            }
        }
        Ok(())
    }

    fn add_table(&mut self, t: SourceTable) {
        if !self.state.tables.get(&t.id).is_some_and(|s| s.done) {
            self.incomplete.insert(t.id);
        }
        self.tables.insert(t.id, t);
    }

    fn handle_relation(&mut self, rel: Relation) -> Result<()> {
        let Some(table) = self.table_of(&rel) else {
            self.unmapped_rels.insert(rel.id, rel);
            return Ok(());
        };
        let t = self.tables[&table].clone();
        // the relation's columns (nullability kept by name, new columns
        // nullable) and the key it implies
        let mut columns: Vec<Column> = rel
            .columns
            .iter()
            .map(|rc| {
                let (ty, typmod) = map_type(rc.type_oid, rc.type_modifier);
                let nullable = t.columns.iter().find(|c| c.name == rc.name).map(|c| c.ty.nullable).unwrap_or(true);
                Column::new(rc.name.clone(), ColumnType { ty, typmod, nullable })
            })
            .collect();
        let mut pk = relation_key(&rel, &t, &columns);
        // detect a changed shape (new/dropped columns or changed types) and
        // a changed key (another replica identity or primary key)
        let same_shape = rel.columns.len() == t.columns.len()
            && rel.columns.iter().zip(&t.columns).all(|(rc, c)| {
                let (ty, _) = map_type(rc.type_oid, rc.type_modifier);
                rc.name == c.name && ty == c.ty.ty
            });
        let names = |key: &[usize], cols: &[Column]| -> std::collections::BTreeSet<String> {
            key.iter().map(|k| cols[*k].name.clone()).collect()
        };
        let same_key = names(&t.pk, &t.columns) == names(&pk, &columns);
        if same_key {
            // the same columns: keep their order
            pk = t.pk.iter().filter_map(|p| columns.iter().position(|c| c.name == t.columns[*p].name)).collect();
        }
        if same_shape && same_key {
            let cols: Vec<Option<usize>> = rel.columns.iter().map(|rc| t.columns.iter().position(|c| c.name == rc.name)).collect();
            self.rels.insert(rel.id, RelMap { table, cols });
            return Ok(());
        }
        for k in &pk {
            columns[*k].ty.nullable = false;
        }
        if same_key {
            log::info!("source table {}.{} changed shape; re-snapshotting", t.upstream.0, t.upstream.1);
        } else {
            let show = |key: &[usize], cols: &[Column]| key.iter().map(|k| cols[*k].name.as_str()).collect::<Vec<_>>().join(", ");
            log::warn!(
                "source table {}.{}: its key changed upstream from ({}) to ({}) (replica identity {:?}); re-keying it and copying it again",
                t.upstream.0,
                t.upstream.1,
                show(&t.pk, &t.columns),
                show(&pk, &columns),
                rel.replica_identity as char
            );
        }
        // The schema change is logged now, before the commit of the
        // transaction announcing it: its changes to the table so far were
        // decoded in the old shape and must take the new one.
        if let Some(txn) = &mut self.txn {
            reshape_changes(&mut txn.changes, &t, table, &columns, &pk);
            txn.bytes = txn.changes.iter().map(|(_, c)| crate::log::change_len(c)).sum();
            if let Some(spool) = &mut txn.spool {
                spool.rewrite(&mut |ch| reshape_changes(ch, &t, table, &columns, &pk))?;
            }
        }
        // Re-snapshot: to fill new columns of existing rows, and after a key
        // change from an empty table, as deletes by the old key were lost
        // (and those decoded above with it are dropped by reshape_changes).
        // The reset is logged before the schema change: a restart in
        // between then copies the table in its old shape and detects the
        // change again, while the other order would forget the re-snapshot.
        self.set_snap(table, SnapState { truncate: !same_key && !pk.is_empty(), ..Default::default() });
        self.start_copying();
        if self.chunk.as_ref().is_some_and(|c| c.table == table) {
            self.abandon_chunk();
            self.refresh_status();
        }
        self.log_progress()?;
        self.log_tracked(LogRecord::Schema { source: self.id, table, columns: columns.clone(), pk: pk.clone() })?;
        self.tables.insert(table, SourceTable { id: table, upstream: t.upstream.clone(), columns, pk });
        let cols: Vec<Option<usize>> = rel.columns.iter().enumerate().map(|(i, _)| Some(i)).collect();
        self.rels.insert(rel.id, RelMap { table, cols });
        Ok(())
    }

    /// Advance the durable LSN over records the log has made durable.
    /// With `wait`, block until every record in flight is durable.
    fn drain_acks(&mut self, wait: bool) -> Result<()> {
        while let Some((_, rx)) = self.inflight.front() {
            let done = if wait {
                rx.recv().map(|_| true).map_err(|_| SqlError::new(codes::ADMIN_SHUTDOWN, "sequencer stopped"))?
            } else {
                match rx.try_recv() {
                    Ok(_) => true,
                    Err(crossbeam_channel::TryRecvError::Empty) => false,
                    Err(_) => return Err(SqlError::new(codes::ADMIN_SHUTDOWN, "sequencer stopped")),
                }
            };
            if !done {
                break;
            }
            self.acked();
        }
        // a keepalive seen while commits were in flight: once they are all
        // durable, everything the server had sent by then is accounted for
        if self.inflight.is_empty() && self.pending_wal_end > 0 && self.chunk.is_none() {
            self.set_frontier(self.pending_wal_end);
            self.pending_wal_end = 0;
        }
        Ok(())
    }

    /// The oldest record in flight is durable.
    fn acked(&mut self) {
        if let Some((lsn, _)) = self.inflight.pop_front() {
            self.durable_lsn = lsn;
            self.set_frontier(lsn);
        }
    }

    /// So many records wait to be durable, or the records the engine has
    /// yet to apply fill their part of the memory budget (whatever source
    /// logged them), that the source stops reading the stream until some
    /// are durable, or applied (the upstream keeps the WAL meanwhile). It
    /// keeps its session, sending status updates as usual: it never waits
    /// for room when it logs (`EngineHandle::log_now`), it stops reading
    /// what it would log instead.
    fn backlogged(&self) -> bool {
        self.inflight.len() > MAX_INFLIGHT || self.engine.input_full()
    }

    /// Append a record to the log without waiting for it (or for room: see
    /// `backlogged`). It is in flight until durable, at the LSN of the last
    /// commit before it.
    fn log_tracked(&mut self, rec: LogRecord) -> Result<()> {
        let lsn = self.last_lsn();
        let rx = self.engine.log_now(rec)?;
        self.inflight.push_back((lsn, rx));
        Ok(())
    }

    /// The end LSN of the last commit sent to the log.
    fn last_lsn(&self) -> u64 {
        self.inflight.back().map(|(l, _)| *l).unwrap_or(self.durable_lsn)
    }

    /// Set the snapshot state of `table`; it is logged with the next
    /// progress record.
    fn set_snap(&mut self, table: ObjectId, st: SnapState) {
        if st.done || !self.tables.contains_key(&table) {
            self.incomplete.remove(&table);
        } else {
            self.incomplete.insert(table);
        }
        self.state.tables.insert(table, st);
        self.dirty.insert(table);
    }

    /// Log the entries of the state that changed since the last progress
    /// record (a `ProgressDelta`: logging all of it every time would make a
    /// snapshot of many tables log their number squared).
    fn log_progress(&mut self) -> Result<()> {
        if self.dirty.is_empty() && !self.slot_dirty {
            return Ok(());
        }
        let tables = std::mem::take(&mut self.dirty);
        let delta = StateDelta {
            slot_created: std::mem::take(&mut self.slot_dirty).then_some(self.state.slot_created),
            tables: tables.into_iter().map(|t| (t, self.state.tables.get(&t).cloned().unwrap_or_default())).collect(),
        };
        let lsn = self.last_lsn();
        self.log_tracked(LogRecord::ProgressDelta { source: self.id, lsn, delta: serde_json::to_string(&delta).unwrap() })
    }

    /// Advance the source's LSN frontier. While a table's snapshot is
    /// incomplete the source covers no LSN at all: reads waiting for an LSN
    /// must not see a partially loaded table.
    fn set_frontier(&self, lsn: u64) {
        if self.incomplete.is_empty() {
            self.engine.advance_frontier(self.id, lsn);
        }
    }

    /// Low and high watermark tags for the next chunk of `table`. They must
    /// be unique: watermarks written by an earlier run of this source (the
    /// stream resumes before them after a restart), or by another engine
    /// whose source and table have the same ids (logical messages reach
    /// every slot of the database), must not be taken for this chunk's.
    fn watermarks(&mut self, table: ObjectId) -> (String, String) {
        self.seq += 1;
        let tag = format!("{}:{table}:{:016x}:{}", self.id, self.run_id, self.seq);
        (format!("L:{tag}"), format!("H:{tag}"))
    }

    /// The copy thread (started on first use).
    fn copier(&mut self) -> Result<&Copier> {
        if self.copier.is_none() {
            self.copier = Some(Copier::start(self.id, self.def.conninfo.clone())?);
        }
        Ok(self.copier.as_ref().unwrap())
    }

    /// Start the next snapshot chunk if none is in flight: ask the copy
    /// thread to read it. A table whose chunk cannot be read (no SELECT
    /// privilege, dropped upstream, a value that does not parse, ...) does
    /// not hold up the others: it is retried with a growing delay, shown in
    /// the status, while the other tables are copied and the stream goes
    /// on. Fresh reads of it keep waiting until it is copied.
    fn maybe_start_chunk(&mut self) -> Result<()> {
        let now = Instant::now();
        if self.chunk.is_some() || self.copy_failure.as_ref().is_some_and(|f| f.retry_at > now) {
            return Ok(());
        }
        if self.unseen.len() > UNSEEN_MAX && self.unseen_checked.elapsed() >= Duration::from_secs(1) && !self.snapshot_requested {
            // (no chunk has checked them for a while: a table that cannot
            // be read, say)
            self.unseen_checked = now;
            self.snapshot_requested = self.copier()?.requests.send(CopyRequest::Snapshot).is_ok();
        }
        if self.chunk_wait.is_some_and(|w| w > now) {
            return Ok(());
        }
        let next = self.incomplete.iter().copied().find(|t| !self.failing.get(t).is_some_and(|f| f.retry_at > now));
        let Some(table) = next else { return Ok(()) };
        let mut st = self.state.tables.get(&table).cloned().unwrap_or_default();
        if st.truncate {
            // Start from an empty table, here between upstream transactions
            // and before the chunk's low watermark: from now on the table
            // fills exactly as in an initial snapshot.
            let lsn = self.last_lsn();
            self.log_tracked(LogRecord::Commit { source: self.id, lsn, changes: vec![(table, TableChange::Truncate)] })?;
            st.truncate = false;
            self.set_snap(table, st.clone());
            self.log_progress()?;
        }
        let t = self.tables[&table].clone();
        // keys to read again first, then the next key range
        let reread = (!st.reread.is_empty()).then(|| st.reread.iter().take(REREAD_KEYS).cloned().collect::<Vec<_>>());
        let (low, high) = self.watermarks(table);
        let seq = self.seq;
        let unseen = self.unseen.iter().copied().collect();
        let keyed = !t.pk.is_empty();
        let req = CopyRequest::Chunk {
            seq,
            table: Box::new(t),
            boundary: st.boundary,
            keys: reread.clone(),
            low: low.clone(),
            high: high.clone(),
            unseen,
        };
        self.chunk = Some(Chunk {
            table,
            seq,
            low,
            high,
            phase: Phase::BeforeLow,
            keyed,
            snap: None,
            rows: None,
            touched: Vec::new(),
            last_key: None,
            last: false,
            unresolved: Vec::new(),
            pending: Vec::new(),
            complete: false,
            reread,
        });
        let copier = self.copier()?;
        copier.wanted.store(seq, Ordering::Release);
        if copier.requests.send(req).is_err() {
            self.copier_stopped();
            return Ok(());
        }
        self.refresh_status();
        Ok(())
    }

    /// Stop waiting for the chunk in flight; the copy thread drops it too.
    /// (The caller refreshes the status.)
    fn abandon_chunk(&mut self) {
        self.chunk = None;
        if let Some(c) = &self.copier {
            c.wanted.store(0, Ordering::Release);
        }
    }

    /// Can the copy thread's events be taken now? A keyless table's rows are
    /// logged between upstream transactions, and not while records pile up
    /// (see `backlogged`): until then they wait in the copy thread.
    fn copy_receivable(&self) -> bool {
        match &self.chunk {
            Some(ch) if !ch.keyed => self.txn.is_none() && !self.backlogged(),
            _ => true,
        }
    }

    /// Handle the copy thread's events that are ready.
    fn poll_copier(&mut self) -> Result<()> {
        while self.copy_receivable() {
            let Some(c) = &self.copier else { break };
            match c.events.try_recv() {
                Ok(ev) => self.copy_event(ev)?,
                Err(crossbeam_channel::TryRecvError::Empty) => break,
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    self.copier_stopped();
                    break;
                }
            }
        }
        Ok(())
    }

    /// Wait up to `timeout` for the oldest record in flight to become durable
    /// (or for the copy thread's next event), while the source does not
    /// read the stream.
    fn wait(&mut self, timeout: Duration) -> Result<()> {
        let events = match &self.copier {
            Some(c) if self.copy_receivable() => c.events.clone(),
            _ => crossbeam_channel::never(),
        };
        let ack = self.inflight.front().map(|(_, rx)| rx.clone()).unwrap_or_else(crossbeam_channel::never);
        crossbeam_channel::select! {
            recv(events) -> ev => match ev {
                Ok(ev) => self.copy_event(ev)?,
                Err(_) => self.copier_stopped(),
            },
            recv(ack) -> r => {
                r.map_err(|_| SqlError::new(codes::ADMIN_SHUTDOWN, "sequencer stopped"))?;
                self.acked();
            }
            default(timeout) => {}
        }
        Ok(())
    }

    /// An event of the copy thread. The rows of a keyless table are logged
    /// as they arrive, as commits of their own, between upstream
    /// transactions (see `copy_receivable`).
    fn copy_event(&mut self, ev: CopyEvent) -> Result<()> {
        if ev.seq() == 0 {
            // (the answer to a `CopyRequest::Snapshot`)
            self.snapshot_requested = false;
            if let CopyEvent::Snapshot { snap, .. } = &ev {
                self.snapshot_sees_decoded(snap);
            }
            return Ok(());
        }
        // (events of a chunk abandoned since are dropped)
        let Some(ch) = self.chunk.as_mut().filter(|ch| ch.seq == ev.seq()) else { return Ok(()) };
        let table = ch.table;
        match ev {
            CopyEvent::Snapshot { snap, .. } => {
                // a keyless table's copy: changes decoded before its
                // snapshot was known are decided now
                for (xid, c) in std::mem::take(&mut ch.unresolved) {
                    if !xid.is_some_and(|x| xid_visible(&snap, x)) {
                        ch.pending.push(c);
                    }
                }
                ch.snap = Some(snap.clone());
                self.snapshot_sees_decoded(&snap);
                // the copy replaces the table (also the rows kept from
                // before a schema change)
                self.log_copy(table, vec![TableChange::Truncate])?;
            }
            CopyEvent::NotVisible { snap, .. } => {
                self.snapshot_sees_decoded(&snap);
                log::debug!("source {}: a chunk's snapshot does not see {} transactions decoded before it; waiting", self.id, self.unseen.len());
                self.abandon_chunk();
                self.chunk_wait = Some(Instant::now() + CHUNK_WAIT);
            }
            CopyEvent::Rows { snap, rows, last_key, last, .. } => {
                ch.rows = Some(rows.into_iter().collect());
                ch.snap = Some(snap.clone());
                ch.last_key = last_key;
                ch.last = last;
                ch.resolve();
                self.snapshot_sees_decoded(&snap);
                self.copied(table);
            }
            CopyEvent::Piece { rows, last, .. } => {
                ch.complete = last;
                self.log_copy(table, rows.into_iter().map(|row| TableChange::Insert { row }).collect())?;
                if last {
                    self.copied(table);
                }
            }
            CopyEvent::Failed { error, .. } => {
                self.abandon_chunk();
                self.copy_failed(table, error);
            }
        }
        Ok(())
    }

    /// Log changes a keyless table's copy makes, as a commit of their own
    /// (at the last commit's LSN).
    fn log_copy(&mut self, table: ObjectId, changes: Vec<TableChange>) -> Result<()> {
        if changes.is_empty() {
            return Ok(());
        }
        self.gauges.commit(changes.len());
        let lsn = self.last_lsn();
        let changes = changes.into_iter().map(|c| (table, c)).collect();
        self.log_tracked(LogRecord::Commit { source: self.id, lsn, changes })
    }

    /// A chunk of `table` was read: it can be copied (again).
    fn copied(&mut self, table: ObjectId) {
        let recovered = self.copy_failure.take().is_some();
        if self.failing.remove(&table).is_some() {
            let t = &self.tables[&table];
            log::info!("source {}: table {}.{} can be copied again", self.id, t.upstream.0, t.upstream.1);
            self.publish_failures();
        }
        if recovered {
            log::info!("source {}: snapshot chunks can be read again", self.id);
        }
        self.refresh_status();
    }

    /// A chunk of `table` could not be read. A table that cannot be read is
    /// retried with a growing delay while the other tables are copied; a
    /// failed connection holds up every chunk the same way.
    fn copy_failed(&mut self, table: ObjectId, e: ChunkError) {
        match e {
            ChunkError::Table(e) => {
                let delay = self.failing.get(&table).map(|f| (f.delay * 2).min(TABLE_RETRY_MAX)).unwrap_or(TABLE_RETRY_MIN);
                let t = &self.tables[&table];
                log::warn!(
                    "source {}: cannot copy table {}.{}: {e}; retrying in {}s, streaming the other tables",
                    self.id,
                    t.upstream.0,
                    t.upstream.1,
                    delay.as_secs()
                );
                self.failing.insert(table, TableFailure { error: e.to_string(), delay, retry_at: Instant::now() + delay });
                self.publish_failures();
            }
            ChunkError::Session(e) => {
                let delay = self.copy_failure.as_ref().map(|f| (f.delay * 2).min(TABLE_RETRY_MAX)).unwrap_or(TABLE_RETRY_MIN);
                log::warn!("source {}: cannot read snapshot chunks: {e}; retrying in {}s, streaming", self.id, delay.as_secs());
                self.copy_failure = Some(TableFailure { error: e.to_string(), delay, retry_at: Instant::now() + delay });
            }
        }
        self.refresh_status();
    }

    /// The copy thread is gone (it panicked): a new one starts with the next
    /// chunk.
    fn copier_stopped(&mut self) {
        self.copier = None;
        match self.chunk.take() {
            Some(ch) => self.copy_failed(ch.table, ChunkError::Session(SqlError::new(codes::INTERNAL_ERROR, "the copy thread stopped"))),
            None => self.refresh_status(),
        }
    }

    /// Set the status line: a table that cannot be copied is an error (the
    /// other tables are still copied and streamed), else what the source
    /// is doing.
    fn refresh_status(&self) {
        let upstream = |t: &ObjectId| self.tables.get(t).map(|t| format!("{}.{}", t.upstream.0, t.upstream.1)).unwrap_or_default();
        let status = match (self.failing.iter().next(), &self.copy_failure, &self.chunk) {
            (Some((t, f)), _, _) => {
                let more = match self.failing.len() {
                    1 => String::new(),
                    n => format!(" (and {} more tables)", n - 1),
                };
                format!(
                    "error: cannot copy table {}: {}{more}; retrying every {}s, streaming the other tables",
                    upstream(t),
                    f.error,
                    f.delay.as_secs()
                )
            }
            (None, Some(f), _) => format!("error: cannot read snapshot chunks: {}; retrying every {}s, streaming", f.error, f.delay.as_secs()),
            (None, None, Some(ch)) => format!("snapshotting {}", upstream(&ch.table)),
            (None, None, None) => "streaming".to_string(),
        };
        *self.status.lock() = status;
    }

    /// Show the failing tables' errors (web UI).
    fn publish_failures(&self) {
        *self.gauges.table_errors.lock() = self.failing.iter().map(|(t, f)| (*t, f.error.clone())).collect();
    }

    /// Some table still has to be copied.
    fn copying(&self) -> bool {
        !self.incomplete.is_empty()
    }

    /// A transaction was decoded (at its commit).
    fn note_decoded(&mut self, xid: u32) {
        if self.recent.len() == RECENT_XIDS {
            self.recent.pop_front();
        }
        self.recent.push_back(xid);
        if self.copying() {
            self.unseen.insert(xid);
        }
    }

    /// Tables start (again) to be copied: transactions decoded shortly
    /// before, which changed their rows, must be visible to their chunks too.
    fn start_copying(&mut self) {
        self.unseen.extend(self.recent.iter().copied());
    }

    /// Does a chunk's snapshot see every transaction decoded while tables
    /// are being copied? (Forgets those it sees: later snapshots see them.)
    ///
    /// The stream's changes to keys that later chunks read (or, for a
    /// keyless table, changes before its copy) are overwritten by those
    /// chunks' rows, which must therefore be at least as new: their
    /// snapshots must contain the decoded transactions. The copy thread
    /// checks a chunk's snapshot against the transactions given in its
    /// request, the source then forgets those the snapshot sees.
    fn snapshot_sees_decoded(&mut self, snap: &Snap) -> bool {
        self.unseen.retain(|x| !xid_visible(snap, *x));
        self.unseen_checked = Instant::now();
        self.unseen.is_empty()
    }
}

/// A server error leaves the connection usable: the table cannot be read.
/// Others are failures of the connection.
fn chunk_error(q: &Client, e: pgproto::Error) -> ChunkError {
    if e.kind == pgproto::ErrorKind::Server && q.is_usable() { ChunkError::Table(e.into()) } else { ChunkError::Session(e.into()) }
}

/// The snapshot in the first row of a pg_current_snapshot() query.
fn snapshot_of(res: &[pgproto::SimpleQueryResult]) -> std::result::Result<Snap, ChunkError> {
    res.iter()
        .find_map(|r| r.rows.first())
        .and_then(|r| r[0].as_deref())
        .and_then(parse_snapshot)
        .ok_or_else(|| ChunkError::Session(SqlError::new(codes::PROTOCOL_VIOLATION, "pg_current_snapshot() returned no snapshot")))
}

/// Open a chunk's REPEATABLE READ transaction and take its snapshot, which
/// the chunk's queries use.
fn begin_snapshot(q: &mut Client) -> std::result::Result<Snap, ChunkError> {
    let res = q.simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT pg_current_snapshot()::text").map_err(|e| chunk_error(q, e))?;
    snapshot_of(&res)
}

/// Write a watermark into the WAL (a transactional message: it appears in
/// the stream at its commit).
fn emit_watermark(q: &mut Client, tag: &str) -> std::result::Result<(), ChunkError> {
    q.simple_query(&format!("SELECT pg_logical_emit_message(true, 'ivm', {})", qlit(tag))).map_err(|e| chunk_error(q, e))?;
    Ok(())
}

/// The query of a keyed table's chunk: the rows after `boundary` in the
/// order of the table's key index (its primary key's, or its replica
/// identity's), which serves both the order and the comparison with the
/// boundary. The key columns are named as they are, so they keep their own
/// type, collation and operators; the boundary's values are untyped
/// literals, which take the columns' types (also of types the engine
/// replicates as text).
fn keyed_chunk_sql(t: &SourceTable, boundary: Option<&[String]>) -> String {
    let tname = format!("{}.{}", qident(&t.upstream.0), qident(&t.upstream.1));
    // values arrive in text format through the simple query protocol
    let cols = t.columns.iter().map(|c| qident(&c.name)).collect::<Vec<_>>().join(", ");
    let key = t.pk.iter().map(|c| qident(&t.columns[*c].name)).collect::<Vec<_>>().join(", ");
    let filter = match boundary {
        None => String::new(),
        Some(b) => format!(" WHERE ({key}) > ({})", b.iter().map(|v| qlit(v)).collect::<Vec<_>>().join(", ")),
    };
    format!("SELECT {cols} FROM {tname}{filter} ORDER BY {key} LIMIT {CHUNK_ROWS}")
}

/// The query of a chunk reading the rows of `keys` again (text values,
/// untyped literals as in `keyed_chunk_sql`).
fn reread_chunk_sql(t: &SourceTable, keys: &[Vec<String>]) -> String {
    let tname = format!("{}.{}", qident(&t.upstream.0), qident(&t.upstream.1));
    let cols = t.columns.iter().map(|c| qident(&c.name)).collect::<Vec<_>>().join(", ");
    let key = t.pk.iter().map(|c| qident(&t.columns[*c].name)).collect::<Vec<_>>().join(", ");
    let list = keys.iter().map(|k| format!("({})", k.iter().map(|v| qlit(v)).collect::<Vec<_>>().join(", "))).collect::<Vec<_>>();
    format!("SELECT {cols} FROM {tname} WHERE ({key}) IN ({})", list.join(", "))
}

/// A source's copy thread: reads snapshot chunks on a connection of its
/// own, so that the replication connection keeps streaming meanwhile (and
/// keeps its session: a long chunk would otherwise outlast
/// wal_sender_timeout). It ends when the source drops its `Copier`.
fn copy_thread(conninfo: &str, requests: Receiver<CopyRequest>, events: Sender<CopyEvent>, wanted: &AtomicU64) {
    let mut conn: Option<Client> = None;
    for req in requests {
        let (seq, r) = match req {
            CopyRequest::Snapshot => {
                let r = fresh_snapshot(&mut conn, conninfo).map(|snap| events.send(CopyEvent::Snapshot { seq: 0, snap }));
                (0, r.map(|_| ()))
            }
            // (abandoned before it started)
            CopyRequest::Chunk { seq, .. } if wanted.load(Ordering::Acquire) != seq => continue,
            CopyRequest::Chunk { seq, table, boundary, keys, low, high, unseen } => {
                let what = match &keys {
                    Some(k) => Read::Keys(k),
                    None => Read::After(boundary.as_deref()),
                };
                let r = copy_chunk(&mut conn, conninfo, seq, &table, what, (&low, &high), &unseen, &events, wanted);
                (seq, r)
            }
        };
        let Err(error) = r else { continue };
        match &error {
            ChunkError::Session(_) => conn = None,
            ChunkError::Table(_) => {
                if let Some(q) = &mut conn
                    && q.transaction_status() != b'I'
                    && q.simple_query("ROLLBACK").is_err()
                {
                    conn = None;
                }
            }
        }
        if events.send(CopyEvent::Failed { seq, error }).is_err() {
            return;
        }
    }
}

fn copy_connection<'a>(conn: &'a mut Option<Client>, conninfo: &str) -> std::result::Result<&'a mut Client, ChunkError> {
    if conn.is_none() {
        *conn = Some(connect(conninfo, false).map_err(ChunkError::Session)?);
    }
    Ok(conn.as_mut().unwrap())
}

/// A snapshot taken now, outside a transaction.
fn fresh_snapshot(conn: &mut Option<Client>, conninfo: &str) -> std::result::Result<Snap, ChunkError> {
    let q = copy_connection(conn, conninfo)?;
    let res = q.simple_query("SELECT pg_current_snapshot()::text").map_err(|e| chunk_error(q, e))?;
    snapshot_of(&res)
}

/// What a chunk of a keyed table reads.
enum Read<'a> {
    /// The next rows after a boundary (from the first key if none).
    After(Option<&'a [String]>),
    /// The rows of these keys.
    Keys(&'a [Vec<String>]),
}

/// Read chunk `seq` of `t` between its watermarks and report it (see
/// `CopyEvent`). A keyed table's chunk is the next `CHUNK_ROWS` rows after
/// a boundary, or the rows of given keys; a keyless table is read whole, in
/// one snapshot, through a cursor in pieces of `CHUNK_ROWS` rows, which the
/// source logs as they come (the events channel is bounded: the thread
/// waits for it).
#[allow(clippy::too_many_arguments)]
fn copy_chunk(
    conn: &mut Option<Client>,
    conninfo: &str,
    seq: u64,
    t: &SourceTable,
    what: Read,
    (low, high): (&str, &str),
    unseen: &[u32],
    events: &Sender<CopyEvent>,
    wanted: &AtomicU64,
) -> std::result::Result<(), ChunkError> {
    let q = copy_connection(conn, conninfo)?;
    emit_watermark(q, low)?;
    let snap = begin_snapshot(q)?;
    // The chunk can only replace changes the stream applied to its rows (or
    // skipped) if its snapshot contains them. A transaction becomes visible
    // only after its commit record is written, which is when it can be
    // decoded: one decoded already can still be invisible (briefly, or for
    // as long as a synchronous standby takes to confirm it). Then the
    // source waits for it.
    if !unseen.iter().all(|x| xid_visible(&snap, *x)) {
        q.simple_query("ROLLBACK").map_err(|e| chunk_error(q, e))?;
        let _ = events.send(CopyEvent::NotVisible { seq, snap });
        return Ok(());
    }
    if t.pk.is_empty() {
        if events.send(CopyEvent::Snapshot { seq, snap }).is_err() {
            return Ok(());
        }
        let tname = format!("{}.{}", qident(&t.upstream.0), qident(&t.upstream.1));
        let cols = t.columns.iter().map(|c| qident(&c.name)).collect::<Vec<_>>().join(", ");
        q.simple_query(&format!("DECLARE ivm_copy NO SCROLL CURSOR FOR SELECT {cols} FROM {tname}")).map_err(|e| chunk_error(q, e))?;
        loop {
            if wanted.load(Ordering::Acquire) != seq {
                // abandoned
                q.simple_query("ROLLBACK").map_err(|e| chunk_error(q, e))?;
                return Ok(());
            }
            let rows = chunk_query(q, &format!("FETCH {CHUNK_ROWS} FROM ivm_copy"), t, &mut Vec::new())?;
            let last = rows.len() < CHUNK_ROWS;
            if last {
                q.simple_query("COMMIT").map_err(|e| chunk_error(q, e))?;
            }
            if events.send(CopyEvent::Piece { seq, rows, last }).is_err() {
                return Ok(());
            }
            if last {
                break;
            }
        }
    } else {
        let mut last_key = Vec::new();
        let sql = match what {
            Read::After(boundary) => keyed_chunk_sql(t, boundary),
            Read::Keys(keys) => reread_chunk_sql(t, keys),
        };
        let rows = chunk_query(q, &format!("{sql}; COMMIT"), t, &mut last_key)?;
        let last = rows.len() < CHUNK_ROWS;
        let last_key = (!rows.is_empty()).then_some(last_key);
        let rows = rows.into_iter().map(|r| (pk_bytes(&r, &t.pk), r)).collect();
        if events.send(CopyEvent::Rows { seq, snap, rows, last_key, last }).is_err() {
            return Ok(());
        }
    }
    emit_watermark(q, high)
}

/// The key a Relation message implies for a table known as `t`, as
/// positions in the relation's `columns`: the replica identity columns it
/// flags, i.e. the primary key with REPLICA IDENTITY DEFAULT (none if the
/// table has no primary key any more: it is then a multiset, as PostgreSQL
/// refuses to publish its updates and deletes) or the index's columns with
/// USING INDEX. FULL flags every column, and NOTHING (also an identity index
/// that was dropped) none: they keep the known key as long as its columns
/// exist.
fn relation_key(rel: &Relation, t: &SourceTable, columns: &[Column]) -> Vec<usize> {
    let flagged: Vec<usize> = rel.columns.iter().enumerate().filter(|(_, c)| c.is_key).map(|(i, _)| i).collect();
    match rel.replica_identity {
        b'd' => flagged,
        b'i' if !flagged.is_empty() => flagged,
        _ => {
            let known: Vec<usize> = t.pk.iter().filter_map(|p| columns.iter().position(|c| c.name == t.columns[*p].name)).collect();
            if known.len() == t.pk.len() { known } else { vec![] }
        }
    }
}

/// Rewrite the changes to `table` (decoded with the columns of `old`) for
/// its new columns and key: values move by column name and are cast to the
/// new types (NULL where that fails), as the engine converts stored rows. A
/// delete by a key that changed columns cannot be converted and is dropped
/// (the table is re-snapshotted anyway).
fn reshape_changes(changes: &mut Vec<(ObjectId, TableChange)>, old: &SourceTable, table: ObjectId, columns: &[Column], pk: &[usize]) {
    let pos = |name: &str| columns.iter().position(|c| c.name == name);
    let cast = |v: &Datum, c: &Column| crate::expr::cast::cast_datum(v, &c.ty.ty, c.ty.typmod, true).unwrap_or(Datum::Null);
    let reshape = |row: &Row| -> Row {
        columns
            .iter()
            .map(|c| match old.columns.iter().position(|o| o.name == c.name) {
                Some(i) => row.get(i).map(|v| cast(v, c)).unwrap_or(Datum::Null),
                None => Datum::Null,
            })
            .collect()
    };
    let same_key = old.pk.len() == pk.len() && old.pk.iter().zip(pk).all(|(o, n)| old.columns[*o].name == columns[*n].name);
    changes.retain_mut(|(t, ch)| {
        if *t != table {
            return true;
        }
        match ch {
            TableChange::Upsert { row, unchanged } | TableChange::UpsertIfPresent { row, unchanged } => {
                *row = reshape(row);
                *unchanged = unchanged.iter().filter_map(|u| old.columns.get(*u).and_then(|c| pos(&c.name))).collect();
            }
            TableChange::Insert { row } | TableChange::Remove { row } => *row = reshape(row),
            TableChange::Delete { key } => {
                if !same_key {
                    log::warn!("delete by a key whose columns changed upstream in the same transaction is dropped");
                    return false;
                }
                *key = key.iter().zip(pk).map(|(v, p)| cast(v, &columns[*p])).collect();
            }
            TableChange::Truncate => {}
        }
        true
    });
}

/// Run a snapshot chunk's statements: the table's rows, parsed from the
/// text values as they arrive (no strings collected per value).
/// `last_pk_text` receives the text of the last row's primary key.
fn chunk_query(
    q: &mut Client,
    sql: &str,
    t: &SourceTable,
    last_pk_text: &mut Vec<String>,
) -> std::result::Result<Vec<Row>, ChunkError> {
    let mut rows = Vec::new();
    let mut err = None;
    q.simple_query_rows(sql, &mut |cols, r| {
        if cols.len() != t.columns.len() {
            return Ok(());
        }
        let mut row = Vec::with_capacity(t.columns.len());
        for (i, c) in t.columns.iter().enumerate() {
            row.push(match r.get(i) {
                None => Datum::Null,
                Some(b) => match parse_datum(&String::from_utf8_lossy(b), &c.ty.ty, -1) {
                    Ok(d) => d,
                    Err(e) => {
                        let msg = e.message.clone();
                        err = Some(e);
                        return Err(msg);
                    }
                },
            });
        }
        // the strings are reused from row to row
        last_pk_text.resize(t.pk.len(), String::new());
        for (s, c) in last_pk_text.iter_mut().zip(&t.pk) {
            s.clear();
            if let Some(b) = r.get(*c) {
                s.push_str(&String::from_utf8_lossy(b));
            }
        }
        rows.push(row);
        Ok(())
    })
    .map_err(|e| match err.take() {
        // a value that does not parse: the table cannot be read
        Some(e) => ChunkError::Table(e),
        None => chunk_error(q, e),
    })?;
    Ok(rows)
}

/// Is the (32-bit) transaction id visible in a pg_current_snapshot()?
fn xid_visible((xmin, xmax, xip): &(u64, u64, Vec<u64>), xid: u32) -> bool {
    // place the 32-bit xid (of a recent transaction) in the epoch that puts
    // it nearest the snapshot: xmin's, or the one before or after it when
    // the snapshot straddles a wraparound
    let base = (xmin & !0xFFFF_FFFF) | xid as u64;
    let dist = |x: u64| if x < *xmin { xmin - x } else { x.saturating_sub(*xmax) };
    let x64 = [base.checked_sub(1 << 32), Some(base), base.checked_add(1 << 32)]
        .into_iter()
        .flatten()
        .min_by_key(|x| dist(*x))
        .unwrap_or(base);
    x64 < *xmin || (x64 < *xmax && !xip.contains(&x64))
}

/// Parse pg_current_snapshot() text "xmin:xmax:xip,xip".
fn parse_snapshot(s: &str) -> Option<(u64, u64, Vec<u64>)> {
    let mut it = s.split(':');
    let xmin = it.next()?.parse().ok()?;
    let xmax = it.next()?.parse().ok()?;
    let xip = it.next().map(|x| x.split(',').filter_map(|v| v.parse().ok()).collect()).unwrap_or_default();
    Some((xmin, xmax, xip))
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    id: ObjectId,
    name: String,
    def: SourceDef,
    tables: Vec<SourceTable>,
    state: SourceState,
    resume_lsn: u64,
    engine: EngineHandle,
    ctl: Receiver<SourceCtl>,
    status: Arc<Mutex<String>>,
) {
    let gauges = super::gauges(&engine.shared, id);
    let mut src = PgSource::new(id, def, engine, tables, state, resume_lsn, status.clone(), gauges);
    let mut backoff = RECONNECT_MIN;
    loop {
        if src.engine.shared.shutdown.load(Ordering::Acquire) {
            return;
        }
        src.session = Session::default();
        let result = stream_once(&mut src, &ctl);
        // A session that replicated something, or stayed up for as long as
        // the longest delay, worked: its failure is a new incident and is
        // retried quickly. Sessions that fail without getting anywhere (the
        // upstream is down, or the same message fails every time) keep
        // doubling the delay.
        if src.session.progressed || src.session.streaming_since.is_some_and(|t| t.elapsed() >= RECONNECT_MAX) {
            backoff = RECONNECT_MIN;
        }
        match result {
            Ok(true) => return,
            Ok(false) => {}
            Err(e) => {
                log::warn!("source {name}: {e}");
                src.txn = None;
                src.abandon_chunk();
                src.rels.clear();
                src.unmapped_rels.clear();
                *status.lock() = format!("error: {e}");
            }
        }
        // stop requests during backoff
        match ctl.recv_timeout(backoff) {
            Ok(SourceCtl::Stop) | Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
            Ok(SourceCtl::AddTables(t)) => {
                // no session: relations are announced again by the next one
                for x in t {
                    src.add_table(x);
                }
                src.start_copying();
            }
            Err(_) => {}
        }
        backoff = (backoff * 2).min(RECONNECT_MAX);
    }
}

/// How a replication slot was lost, from the error START_REPLICATION
/// returned: dropped (by someone else; the source never drops its own while
/// it exists), or invalidated because it retained more WAL than
/// max_slot_wal_keep_size allows.
#[derive(Clone, Copy, PartialEq, Debug)]
enum SlotLost {
    Missing,
    Invalidated,
}

fn slot_lost(e: &pgproto::Error) -> Option<SlotLost> {
    match e.sqlstate.as_deref() {
        Some("42704") if e.message.contains("replication slot") => Some(SlotLost::Missing),
        // "can no longer get changes from replication slot" (PostgreSQL 14
        // to 17), "can no longer access replication slot" (later)
        Some("55000") if e.message.contains("can no longer") && e.message.contains("replication slot") => Some(SlotLost::Invalidated),
        _ => None,
    }
}

impl PgSource {
    /// The slot is gone, and with it the upstream changes since the last
    /// acknowledged position, perhaps: every table is copied again, from
    /// an empty table so that deletes are not missed, and the slot is
    /// created again (an invalidated one is dropped first). The reset is
    /// logged before the new slot exists: a restart in between finds the
    /// slot missing (or new) and the tables still to be copied, never a
    /// slot whose stream lacks the lost changes with tables counted as
    /// complete. Returns the error that ends the session; the next one
    /// creates the slot.
    fn slot_lost(&mut self, lost: SlotLost) -> SqlError {
        let what = match lost {
            SlotLost::Missing => "does not exist (dropped upstream)",
            SlotLost::Invalidated => "was invalidated upstream (it retained more WAL than max_slot_wal_keep_size)",
        };
        log::error!(
            "source {}: replication slot \"{}\" {what}: upstream changes after LSN {} may be lost; \
             re-creating the slot and copying all {} tables again",
            self.id,
            self.def.slot,
            Lsn(self.durable_lsn),
            self.tables.len()
        );
        self.state.slot_created = false;
        self.slot_dirty = true;
        let reset: Vec<(ObjectId, bool)> = self.tables.values().map(|t| (t.id, !t.pk.is_empty())).collect();
        for (id, truncate) in reset {
            self.set_snap(id, SnapState { truncate, ..Default::default() });
        }
        self.start_copying();
        self.abandon_chunk();
        if let Err(e) = self.log_progress() {
            return e;
        }
        if lost == SlotLost::Invalidated {
            // (if this fails, the next session finds the slot invalidated
            // again and retries)
            if let Err(e) = connect(&self.def.conninfo, true).and_then(|mut c| Ok(c.drop_slot(&self.def.slot, false)?)) {
                log::warn!("source {}: dropping the invalidated slot \"{}\" failed: {e}", self.id, self.def.slot);
            }
        }
        SqlError::new(
            codes::OBJECT_NOT_IN_PREREQUISITE_STATE,
            format!("replication slot \"{}\" {what}; re-creating it and copying all tables again", self.def.slot),
        )
    }
}

/// One replication session. Ok(true) = stop requested.
///
/// The loop never blocks for long: snapshot chunks are read by the copy
/// thread, and records go to the log without waiting. When too many
/// records are in flight (`PgSource::backlogged`), it waits for them
/// instead of reading the stream, and goes on sending status updates, so
/// the session stays up.
fn stream_once(src: &mut PgSource, ctl: &Receiver<SourceCtl>) -> Result<bool> {
    // resume exactly after the last durable commit
    src.drain_acks(true)?;
    src.resume_lsn = src.resume_lsn.max(src.durable_lsn);
    let mut repl = connect(&src.def.conninfo, true)?;
    if !src.state.slot_created {
        match repl.create_logical_slot(&src.def.slot, "pgoutput", false) {
            Ok(_) => {}
            Err(e) if e.sqlstate.as_deref() == Some("42710") => {}
            Err(e) => return Err(e.into()),
        }
        src.state.slot_created = true;
        src.slot_dirty = true;
        src.log_progress()?;
    }
    let opts = [("proto_version", "1"), ("publication_names", src.def.publication.as_str()), ("messages", "true")];
    let pub_quoted = format!("\"{}\"", src.def.publication.replace('"', "\"\""));
    let opts2 = [("proto_version", "1"), ("publication_names", pub_quoted.as_str()), ("messages", "true")];
    let _ = opts;
    let mut stream: ReplicationStream = match repl.start_logical_replication(&src.def.slot, Lsn(src.resume_lsn.max(src.durable_lsn)), &opts2) {
        Ok(s) => s,
        Err(e) => {
            if let Some(lost) = slot_lost(&e) {
                return Err(src.slot_lost(lost));
            }
            return Err(e.into());
        }
    };
    src.session.streaming_since = Some(Instant::now());
    src.refresh_status();
    let mut last_status = Instant::now();
    let mut last_wal_end = 0u64;
    loop {
        // control messages
        while let Ok(m) = ctl.try_recv() {
            match m {
                SourceCtl::Stop => return Ok(true),
                SourceCtl::AddTables(t) => src.add_tables(t)?,
            }
        }
        src.poll_copier()?;
        if src.txn.is_none() {
            src.maybe_start_chunk()?;
        }
        let ev = if !src.backlogged() {
            stream.recv(Some(Duration::from_millis(100)))?
        } else {
            src.wait(Duration::from_millis(100))?;
            None
        };
        if let Some(ReplicationEvent::XLogData { wal_end, server_time, .. } | ReplicationEvent::Keepalive { wal_end, server_time, .. }) = &ev {
            src.gauges.upstream(wal_end.0, *server_time);
        }
        src.drain_acks(false)?;
        match ev {
            Some(ReplicationEvent::XLogData { message, .. }) => src.handle_message(message)?,
            Some(ReplicationEvent::Keepalive { wal_end, reply_requested, .. }) => {
                last_wal_end = wal_end.0;
                let idle = src.txn.is_none() && src.chunk.is_none() && src.inflight.is_empty();
                if idle {
                    // nothing in flight: everything up to wal_end is accounted for
                    src.set_frontier(wal_end.0);
                } else if src.txn.is_none() && src.chunk.is_none() {
                    // commits still waiting for the log: advance once they are durable
                    src.pending_wal_end = src.pending_wal_end.max(wal_end.0);
                    src.drain_acks(false)?;
                }
                if reply_requested {
                    let f = Lsn(src.durable_lsn.max(if idle { wal_end.0 } else { 0 }));
                    stream.send_standby_status(f, f, f, false)?;
                    last_status = Instant::now();
                }
            }
            None => {}
        }
        let want = src.engine.shared.want_keepalive.swap(false, Ordering::AcqRel);
        if want || last_status.elapsed() > Duration::from_secs(1) {
            let idle = src.txn.is_none() && src.chunk.is_none() && src.inflight.is_empty();
            let f = Lsn(if idle { src.durable_lsn.max(last_wal_end) } else { src.durable_lsn });
            stream.send_standby_status(f, f, f, want)?;
            last_status = Instant::now();
        }
        if src.engine.shared.shutdown.load(Ordering::Acquire) {
            return Ok(true);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coord::api::Shared;
    use crate::coord::seq::SeqMsg;
    use pgproto::pgoutput::RelationColumn;

    /// An engine handle whose sequencer acknowledges every append at once
    /// and keeps the records (the commit of a spooled transaction as a
    /// `Commit` with its changes).
    fn fake_engine() -> (EngineHandle, Arc<Mutex<Vec<LogRecord>>>) {
        let (etx, _) = crossbeam_channel::unbounded();
        let (stx, srx) = crossbeam_channel::unbounded::<SeqMsg>();
        let log = Arc::new(Mutex::new(Vec::new()));
        let l2 = log.clone();
        // the spool directory, removed with the sequencer (once the handle
        // is dropped)
        static N: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let spool = crate::testkit::TempDir::new(&format!("source-spool-{}", N.fetch_add(1, Ordering::Relaxed)));
        let dir = spool.path().to_path_buf();
        std::thread::spawn(move || {
            let _spool = spool;
            let mut gsn = 0;
            for m in srx {
                let (rec, ack) = match m {
                    SeqMsg::Append { rec, ack } => (rec, ack),
                    SeqMsg::AppendSpooled { source, lsn, spool, ack } => (LogRecord::Commit { source, lsn, changes: spool.read() }, ack),
                    _ => continue,
                };
                gsn += 1;
                l2.lock().push(rec);
                if let Some(a) = ack {
                    let _ = a.send(gsn);
                }
            }
        });
        let shared = Arc::new(Shared::default());
        shared.spool.set(crate::log::SpoolDir { dir, sync: false }).unwrap();
        (EngineHandle { tx: etx, seq: stx, shared }, log)
    }

    fn int_cols(names: &[&str]) -> Vec<Column> {
        names.iter().map(|n| Column::new(*n, ColumnType::new(SqlType::Int4))).collect()
    }

    /// Table 10, upstream public.t, with int columns; first column the key.
    fn table(names: &[&str], pk: Vec<usize>) -> SourceTable {
        SourceTable { id: 10, upstream: ("public".into(), "t".into()), columns: int_cols(names), pk }
    }

    fn source(tables: Vec<SourceTable>, state: SourceState) -> (PgSource, Arc<Mutex<Vec<LogRecord>>>) {
        let (engine, log) = fake_engine();
        let def = SourceDef { conninfo: String::new(), publication: "p".into(), slot: "s".into(), tables: None };
        let src = PgSource::new(1, def, engine, tables, state, 0, Arc::default(), Arc::default());
        (src, log)
    }

    fn done_state(table: ObjectId) -> SourceState {
        let mut st = SourceState { slot_created: true, ..Default::default() };
        st.tables.insert(table, SnapState { done: true, ..Default::default() });
        st
    }

    const REL: u32 = 777;

    fn relation(names: &[&str], keys: usize) -> LogicalMessage {
        LogicalMessage::Relation(Relation {
            id: REL,
            namespace: "public".into(),
            name: "t".into(),
            replica_identity: b'd',
            columns: names
                .iter()
                .enumerate()
                .map(|(i, n)| RelationColumn { is_key: i < keys, name: n.to_string(), type_oid: 23, type_modifier: -1 })
                .collect(),
        })
    }

    fn begin(xid: u32) -> LogicalMessage {
        LogicalMessage::Begin { final_lsn: Lsn(0), commit_time: 0, xid }
    }

    fn commit(end: u64) -> LogicalMessage {
        LogicalMessage::Commit { flags: 0, commit_lsn: Lsn(end - 1), end_lsn: Lsn(end), commit_time: 0 }
    }

    fn insert(vals: &[i32]) -> LogicalMessage {
        LogicalMessage::Insert { relation_id: REL, new: vals.iter().map(|v| TupleValue::Text(v.to_string().into_bytes())).collect() }
    }

    fn message(tag: &str) -> LogicalMessage {
        LogicalMessage::Message { transactional: true, lsn: Lsn(0), prefix: "ivm".into(), content: tag.as_bytes().to_vec() }
    }

    /// The contents of table 10 after applying the logged commits.
    fn replay(log: &[LogRecord]) -> Vec<Row> {
        let mut keyed: BTreeMap<i64, Row> = BTreeMap::new();
        let mut multiset: Vec<Row> = Vec::new();
        for r in log {
            let LogRecord::Commit { changes, .. } = r else { continue };
            for (t, ch) in changes {
                assert_eq!(*t, 10);
                match ch {
                    TableChange::Upsert { row, unchanged } => {
                        assert!(unchanged.is_empty(), "filled in by the source");
                        keyed.insert(row[0].as_i64().unwrap(), row.clone());
                    }
                    TableChange::UpsertIfPresent { row, unchanged } => {
                        let k = row[0].as_i64().unwrap();
                        if let Some(old) = keyed.get(&k) {
                            let mut row = row.clone();
                            for c in unchanged {
                                row[*c] = old[*c].clone();
                            }
                            keyed.insert(k, row);
                        }
                    }
                    TableChange::Delete { key } => {
                        keyed.remove(&key[0].as_i64().unwrap());
                    }
                    TableChange::Insert { row } => multiset.push(row.clone()),
                    TableChange::Remove { row } => {
                        let i = multiset.iter().position(|m| m == row).expect("removed row present");
                        multiset.remove(i);
                    }
                    TableChange::Truncate => {
                        keyed.clear();
                        multiset.clear();
                    }
                }
            }
        }
        multiset.sort_by_key(|r| r[0].as_i64());
        keyed.into_values().chain(multiset).collect()
    }

    fn int_row(vals: &[i32]) -> Row {
        vals.iter().map(|v| Datum::Int4(*v)).collect()
    }

    /// The snapshot of the chunks of these tests: xids below 100 are visible.
    const SNAP: (u64, u64, Vec<u64>) = (100, 100, vec![]);

    /// Start a chunk of table 10 (the copy thread is not involved: its
    /// events are handed to the source by the test), between the stream's
    /// position and its low watermark "L:1:10:1".
    fn start_chunk(src: &mut PgSource, keyed: bool) -> u64 {
        src.seq += 1;
        src.chunk = Some(Chunk {
            table: 10,
            seq: src.seq,
            low: "L:1:10:1".into(),
            high: "H:1:10:1".into(),
            phase: Phase::BeforeLow,
            keyed,
            snap: None,
            rows: None,
            touched: Vec::new(),
            last_key: None,
            last: false,
            unresolved: Vec::new(),
            pending: Vec::new(),
            complete: false,
            reread: None,
        });
        src.seq
    }

    /// The copy thread's report of a keyed chunk with these rows.
    fn rows_read(seq: u64, rows: &[&[i32]], last: bool) -> CopyEvent {
        let rows = rows.iter().map(|r| (pk_bytes(&int_row(r), &[0]), int_row(r))).collect();
        CopyEvent::Rows { seq, snap: SNAP, rows, last_key: None, last }
    }

    /// A chunk of table 10 whose rows have been read (keyed), or whose
    /// snapshot was taken and rows read (keyless; logged as they arrive).
    fn chunk_read(src: &mut PgSource, rows: &[&[i32]], keyed: bool) {
        let seq = start_chunk(src, keyed);
        if keyed {
            src.copy_event(rows_read(seq, rows, true)).unwrap();
        } else {
            src.copy_event(CopyEvent::Snapshot { seq, snap: SNAP }).unwrap();
            let rows = rows.iter().map(|r| int_row(r)).collect();
            src.copy_event(CopyEvent::Piece { seq, rows, last: true }).unwrap();
        }
    }

    /// Transactions carrying the chunk's watermarks.
    fn watermarks(src: &mut PgSource, (xid, end): (u32, u64)) {
        for (i, tag) in ["L:1:10:1", "H:1:10:1"].iter().enumerate() {
            src.handle_message(begin(xid + i as u32)).unwrap();
            src.handle_message(message(tag)).unwrap();
            src.handle_message(commit(end + 10 * i as u64)).unwrap();
        }
    }

    /// A TRUNCATE (and rows inserted after it) that the chunk's snapshot
    /// already saw: the chunk's rows are the table after the truncate. They
    /// used to be thrown away with it, losing the rows inserted after it.
    #[test]
    fn truncate_seen_by_the_chunk_keeps_its_rows() {
        for keyed in [true, false] {
            let pk = if keyed { vec![0] } else { vec![] };
            let (mut src, log) = source(vec![table(&["id", "a"], pk)], SourceState { slot_created: true, ..Default::default() });
            src.handle_message(begin(80)).unwrap();
            src.handle_message(relation(&["id", "a"], if keyed { 1 } else { 0 })).unwrap();
            src.handle_message(commit(100)).unwrap();
            chunk_read(&mut src, &[&[5, 50]], keyed);
            src.handle_message(begin(90)).unwrap();
            src.handle_message(LogicalMessage::Truncate { options: 0, relation_ids: vec![REL] }).unwrap();
            src.handle_message(commit(200)).unwrap();
            src.handle_message(begin(91)).unwrap();
            src.handle_message(insert(&[5, 50])).unwrap();
            src.handle_message(commit(210)).unwrap();
            watermarks(&mut src, (95, 220));
            src.drain_acks(true).unwrap();
            assert_eq!(replay(&log.lock()), vec![vec![Datum::Int4(5), Datum::Int4(50)]], "keyed: {keyed}");
            assert!(src.state.tables[&10].done);
        }
    }

    /// The same for a keyless table whose copy was logged before its low
    /// watermark: a truncate its snapshot saw must not wipe it afterwards.
    #[test]
    fn truncate_seen_by_a_keyless_chunk_in_its_window() {
        let (mut src, log) = source(vec![table(&["id", "a"], vec![])], SourceState { slot_created: true, ..Default::default() });
        src.handle_message(begin(80)).unwrap();
        src.handle_message(relation(&["id", "a"], 0)).unwrap();
        src.handle_message(commit(100)).unwrap();
        chunk_read(&mut src, &[&[7, 70], &[7, 70]], false);
        src.handle_message(begin(95)).unwrap();
        src.handle_message(message("L:1:10:1")).unwrap();
        src.handle_message(commit(200)).unwrap();
        // committed after the low watermark, but before the chunk's snapshot
        src.handle_message(begin(90)).unwrap();
        src.handle_message(LogicalMessage::Truncate { options: 0, relation_ids: vec![REL] }).unwrap();
        src.handle_message(commit(210)).unwrap();
        // ... and after it, a transaction the snapshot did not see
        src.handle_message(begin(150)).unwrap();
        src.handle_message(insert(&[8, 80])).unwrap();
        src.handle_message(commit(220)).unwrap();
        src.handle_message(begin(151)).unwrap();
        src.handle_message(message("H:1:10:1")).unwrap();
        src.handle_message(commit(230)).unwrap();
        src.drain_acks(true).unwrap();
        let row = |a: i32, b: i32| vec![Datum::Int4(a), Datum::Int4(b)];
        assert_eq!(replay(&log.lock()), vec![row(7, 70), row(7, 70), row(8, 80)]);
    }

    /// A keyless table's change decoded before the chunk's low watermark from
    /// a transaction its snapshot did not see (the commit preceded the
    /// watermark but was not visible yet) applies after the chunk's rows;
    /// it used to be dropped.
    #[test]
    fn keyless_changes_the_chunk_missed_apply() {
        let (mut src, log) = source(vec![table(&["id", "a"], vec![])], SourceState { slot_created: true, ..Default::default() });
        src.handle_message(begin(80)).unwrap();
        src.handle_message(relation(&["id", "a"], 0)).unwrap();
        src.handle_message(commit(100)).unwrap();
        chunk_read(&mut src, &[&[1, 10], &[2, 20]], false);
        // seen by the snapshot: already in the chunk
        src.handle_message(begin(90)).unwrap();
        src.handle_message(insert(&[2, 20])).unwrap();
        src.handle_message(commit(200)).unwrap();
        // not seen: must be applied
        src.handle_message(begin(150)).unwrap();
        src.handle_message(insert(&[3, 30])).unwrap();
        src.handle_message(LogicalMessage::Delete {
            relation_id: REL,
            old: [1, 10].iter().map(|v: &i32| TupleValue::Text(v.to_string().into_bytes())).collect(),
            old_is_key: false,
        })
        .unwrap();
        src.handle_message(commit(210)).unwrap();
        watermarks(&mut src, (151, 220));
        src.drain_acks(true).unwrap();
        let row = |a: i32, b: i32| vec![Datum::Int4(a), Datum::Int4(b)];
        assert_eq!(replay(&log.lock()), vec![row(2, 20), row(3, 30)]);
    }

    /// A schema change resets the table's snapshot state and logs it at once:
    /// the re-snapshot used to be recorded only with the next chunk, so a
    /// restart in between left new columns NULL for good. The reset comes
    /// first: a restart right after the schema record would forget it.
    #[test]
    fn schema_change_logs_the_resnapshot() {
        let (mut src, log) = source(vec![table(&["id", "a"], vec![0])], done_state(10));
        src.handle_message(begin(5)).unwrap();
        src.handle_message(relation(&["id", "a", "b"], 1)).unwrap();
        src.handle_message(commit(100)).unwrap();
        src.drain_acks(true).unwrap();
        let recs = log.lock();
        match &recs[0] {
            LogRecord::ProgressDelta { delta, .. } => {
                let d: StateDelta = serde_json::from_str(delta).unwrap();
                assert!(!d.tables[&10].done);
                // the key did not change: existing rows are kept
                assert!(!d.tables[&10].truncate);
            }
            other => panic!("expected the reset snapshot state, got {other:?}"),
        }
        assert!(matches!(&recs[1], LogRecord::Schema { table: 10, columns, .. } if columns.len() == 3), "{recs:?}");
    }

    /// The key a Relation message implies: the flagged identity columns,
    /// except with FULL and NOTHING, which keep the known key.
    #[test]
    fn keys_implied_by_relation_messages() {
        let t = table(&["id", "a", "b"], vec![0]);
        let key = |ident: u8, flags: &[bool], names: &[&str]| {
            let LogicalMessage::Relation(mut rel) = relation(names, 0) else { unreachable!() };
            rel.replica_identity = ident;
            for (c, f) in rel.columns.iter_mut().zip(flags) {
                c.is_key = *f;
            }
            let cols = int_cols(names);
            relation_key(&rel, &t, &cols)
        };
        let abc = ["id", "a", "b"];
        assert_eq!(key(b'd', &[true, false, false], &abc), [0]);
        assert_eq!(key(b'd', &[false, true, true], &abc), [1, 2], "another primary key");
        assert_eq!(key(b'd', &[false, false, false], &abc), Vec::<usize>::new(), "no primary key any more");
        assert_eq!(key(b'i', &[false, false, true], &abc), [2], "an identity index");
        assert_eq!(key(b'i', &[false, false, false], &abc), [0], "the identity index was dropped");
        assert_eq!(key(b'n', &[false, false, false], &abc), [0]);
        assert_eq!(key(b'f', &[true, true, true], &abc), [0]);
        assert_eq!(key(b'f', &[true, true, true], &["b", "id"]), [1], "by name");
        assert_eq!(key(b'f', &[true, true], &["a", "b"]), Vec::<usize>::new(), "the key column was dropped");
    }

    /// A changed key resets the table's snapshot state, which is logged
    /// before the schema record, and the re-snapshot starts from an empty
    /// table: deletes by the old key were lost.
    #[test]
    fn key_changes_resnapshot_from_an_empty_table() {
        let (mut src, log) = source(vec![table(&["id", "a"], vec![0])], done_state(10));
        src.handle_message(begin(5)).unwrap();
        let LogicalMessage::Relation(mut rel) = relation(&["id", "a"], 0) else { unreachable!() };
        rel.replica_identity = b'i';
        rel.columns[1].is_key = true;
        src.handle_message(LogicalMessage::Relation(rel)).unwrap();
        src.handle_message(commit(100)).unwrap();
        src.drain_acks(true).unwrap();
        let recs = log.lock();
        match &recs[..] {
            [LogRecord::ProgressDelta { delta, .. }, LogRecord::Schema { table: 10, columns, pk, .. }] => {
                let d: StateDelta = serde_json::from_str(delta).unwrap();
                assert!(!d.tables[&10].done && d.tables[&10].truncate);
                assert_eq!(pk, &[1]);
                assert!(!columns[1].ty.nullable, "key columns are not null");
            }
            other => panic!("unexpected records {other:?}"),
        }
        assert_eq!(src.tables[&10].pk, [1]);
        // the engine applies the schema record after the logged state: the
        // truncate must survive it (a restart before the table's first chunk
        // used to copy it over the rows deleted by the old key)
        let LogRecord::ProgressDelta { delta, .. } = &recs[0] else { unreachable!() };
        let mut st = done_state(10);
        st.apply(delta);
        st.snapshot_again(10);
        assert!(!st.tables[&10].done && st.tables[&10].truncate);
    }

    /// Changes decoded before a schema change in the same transaction are
    /// logged after the Schema record, so they take the new shape.
    #[test]
    fn changes_before_a_schema_change_take_the_new_shape() {
        let (mut src, log) = source(vec![table(&["id", "a", "c"], vec![0])], done_state(10));
        src.handle_message(begin(5)).unwrap();
        src.handle_message(relation(&["id", "a", "c"], 1)).unwrap();
        src.handle_message(insert(&[1, 2, 3])).unwrap();
        // c dropped, b added
        src.handle_message(relation(&["id", "b", "a"], 1)).unwrap();
        src.handle_message(commit(100)).unwrap();
        src.drain_acks(true).unwrap();
        let recs = log.lock();
        let changes = recs
            .iter()
            .find_map(|r| match r {
                LogRecord::Commit { changes, .. } => Some(changes.clone()),
                _ => None,
            })
            .expect("commit");
        match &changes[..] {
            [(10, TableChange::Upsert { row, .. })] => assert_eq!(row, &vec![Datum::Int4(1), Datum::Null, Datum::Int4(2)]),
            other => panic!("unexpected changes {other:?}"),
        }
    }

    /// Watermarks of one run (process) of the source can still be in the
    /// stream after a restart: the new run's watermarks must differ, or the
    /// old ones would open the window of (or finish) a new chunk early.
    /// Tags used to restart at sequence 1 in every process.
    #[test]
    fn watermarks_differ_between_runs() {
        let (mut old_run, _) = source(vec![table(&["id", "a"], vec![0])], SourceState { slot_created: true, ..Default::default() });
        let (mut src, log) = source(vec![table(&["id", "a"], vec![0])], SourceState { slot_created: true, ..Default::default() });
        let (old_low, old_high) = old_run.watermarks(10);
        let (low, high) = src.watermarks(10);
        assert_ne!(old_low, low);
        assert_ne!(old_high, high);
        chunk_read(&mut src, &[&[1, 10]], true);
        let ch = src.chunk.as_mut().unwrap();
        (ch.low, ch.high) = (low, high);
        src.handle_message(begin(80)).unwrap();
        src.handle_message(relation(&["id", "a"], 1)).unwrap();
        src.handle_message(commit(100)).unwrap();
        for (xid, tag, end) in [(81, &old_low, 110), (82, &old_high, 120)] {
            src.handle_message(begin(xid)).unwrap();
            src.handle_message(message(tag)).unwrap();
            src.handle_message(commit(end)).unwrap();
        }
        src.drain_acks(true).unwrap();
        assert!(src.chunk.as_ref().is_some_and(|c| c.phase == Phase::BeforeLow), "the old run's watermarks were taken for the chunk's");
        assert!(replay(&log.lock()).is_empty());
    }

    /// A text value that is not UTF-8 fails the change (it used to become
    /// ''): its table, not the session, which would fail again on the same
    /// message in every new session, stopping every table. The table's
    /// changes in the transaction so far are dropped and it is copied again,
    /// from an empty table; the other tables' changes apply.
    #[test]
    fn values_that_do_not_convert_fail_their_table() {
        let mut t = table(&["id", "s"], vec![0]);
        t.columns[1] = Column::new("s", ColumnType::new(SqlType::Text));
        let u = SourceTable { id: 11, upstream: ("public".into(), "u".into()), columns: int_cols(&["id"]), pk: vec![0] };
        let mut state = done_state(10);
        state.tables.insert(11, SnapState { done: true, ..Default::default() });
        let (mut src, log) = source(vec![t, u], state);
        src.handle_message(begin(5)).unwrap();
        let LogicalMessage::Relation(mut rel) = relation(&["id", "s"], 1) else { unreachable!() };
        rel.columns[1].type_oid = 25;
        src.handle_message(LogicalMessage::Relation(rel)).unwrap();
        let LogicalMessage::Relation(mut rel_u) = relation(&["id"], 1) else { unreachable!() };
        (rel_u.id, rel_u.name) = (REL + 1, "u".into());
        src.handle_message(LogicalMessage::Relation(rel_u)).unwrap();
        let text = |id: &str, s: &[u8]| LogicalMessage::Insert {
            relation_id: REL,
            new: vec![TupleValue::Text(id.as_bytes().to_vec()), TupleValue::Text(s.to_vec())],
        };
        let insert_u = |id: i32| LogicalMessage::Insert { relation_id: REL + 1, new: vec![TupleValue::Text(id.to_string().into_bytes())] };
        src.handle_message(text("1", "café".as_bytes())).unwrap();
        src.handle_message(insert_u(5)).unwrap();
        // Latin-1 é
        src.handle_message(text("2", &[b'c', b'a', b'f', 0xe9])).unwrap();
        src.handle_message(text("3", b"x")).unwrap();
        src.handle_message(insert_u(6)).unwrap();
        src.handle_message(commit(100)).unwrap();
        src.drain_acks(true).unwrap();
        let recs = log.lock();
        match &recs[..] {
            [LogRecord::ProgressDelta { delta, .. }, LogRecord::Commit { changes, .. }] => {
                let d: StateDelta = serde_json::from_str(delta).unwrap();
                assert!(!d.tables[&10].done && d.tables[&10].truncate, "{d:?}");
                assert!(!d.tables.contains_key(&11), "only what changed");
                let ids: Vec<(ObjectId, i64)> = changes
                    .iter()
                    .map(|(t, c)| match c {
                        TableChange::Upsert { row, .. } => (*t, row[0].as_i64().unwrap()),
                        other => panic!("unexpected change {other:?}"),
                    })
                    .collect();
                // (a later change to the table applies: the copy starts by
                // truncating it)
                assert_eq!(ids, [(11, 5), (10, 3), (11, 6)]);
            }
            other => panic!("unexpected records {other:?}"),
        }
        assert!(!src.state.tables[&10].done && src.state.tables[&10].truncate);
    }

    /// Table 10 (id int, s text) and table 11 (id int), both copied, with
    /// their Relation messages in an open transaction.
    fn text_and_int_tables() -> (PgSource, Arc<Mutex<Vec<LogRecord>>>) {
        let mut t = table(&["id", "s"], vec![0]);
        t.columns[1] = Column::new("s", ColumnType::new(SqlType::Text));
        let u = SourceTable { id: 11, upstream: ("public".into(), "u".into()), columns: int_cols(&["id"]), pk: vec![0] };
        let mut state = done_state(10);
        state.tables.insert(11, SnapState { done: true, ..Default::default() });
        let (mut src, log) = source(vec![t, u], state);
        src.handle_message(begin(5)).unwrap();
        let LogicalMessage::Relation(mut rel) = relation(&["id", "s"], 1) else { unreachable!() };
        rel.columns[1].type_oid = 25;
        src.handle_message(LogicalMessage::Relation(rel)).unwrap();
        let LogicalMessage::Relation(mut rel_u) = relation(&["id"], 1) else { unreachable!() };
        (rel_u.id, rel_u.name) = (REL + 1, "u".into());
        src.handle_message(LogicalMessage::Relation(rel_u)).unwrap();
        (src, log)
    }

    fn text_row(id: i32, s: &[u8]) -> LogicalMessage {
        LogicalMessage::Insert { relation_id: REL, new: vec![TupleValue::Text(id.to_string().into_bytes()), TupleValue::Text(s.to_vec())] }
    }

    fn u_row(id: i32) -> LogicalMessage {
        LogicalMessage::Insert { relation_id: REL + 1, new: vec![TupleValue::Text(id.to_string().into_bytes())] }
    }

    fn logged_changes(log: &Mutex<Vec<LogRecord>>) -> Vec<(ObjectId, TableChange)> {
        log.lock()
            .iter()
            .filter_map(|r| match r {
                LogRecord::Commit { changes, .. } => Some(changes.clone()),
                _ => None,
            })
            .flatten()
            .collect()
    }

    /// While the records the engine has yet to apply fill their part of the
    /// memory budget, the source stops reading its stream (it is
    /// backlogged) instead of waiting for room on its replication thread
    /// when it logs: a commit decoded then is logged at once, and the
    /// source reads on once the engine has applied enough. (It used to wait
    /// in the logging call, sending no status updates meanwhile.)
    #[test]
    fn a_full_input_queue_stops_reading_without_waiting() {
        let (mut src, log) = source(vec![table(&["id", "a"], vec![0])], done_state(10));
        let mem = &src.engine.shared.mem;
        mem.set_total(32 << 20);
        assert!(!src.backlogged());
        // the engine is behind by its whole share
        mem.input.add(mem.input_limit());
        assert!(src.backlogged());
        // a transaction the source had decoded is logged without waiting
        src.handle_message(begin(5)).unwrap();
        src.handle_message(relation(&["id", "a"], 1)).unwrap();
        src.handle_message(insert(&[1, 1])).unwrap();
        let (tx, rx) = crossbeam_channel::bounded(1);
        let shared = src.engine.shared.clone();
        std::thread::scope(|s| {
            s.spawn(|| {
                let _ = src.handle_message(commit(100));
                let _ = tx.send(());
            });
            let logged = rx.recv_timeout(Duration::from_secs(5));
            // (a commit waiting for room gives up at a shutdown)
            shared.shutdown.store(true, Ordering::Release);
            assert!(logged.is_ok(), "the commit waited for room");
        });
        shared.shutdown.store(false, Ordering::Release);
        src.drain_acks(true).unwrap();
        assert!(log.lock().iter().any(|r| matches!(r, LogRecord::Commit { .. })));
        assert!(src.backlogged());
        // the engine applies them
        let mem = &src.engine.shared.mem;
        mem.input.sub(mem.input.used());
        assert!(!src.backlogged());
    }

    /// A transaction larger than memory: its changes beyond `PART_BYTES`
    /// are spooled as they arrive and logged at its commit, in order. It
    /// used to be held whole until its commit.
    #[test]
    fn large_transactions_are_spooled() {
        let (mut src, log) = text_and_int_tables();
        let value = vec![b'v'; 1000];
        for i in 0..3000 {
            src.handle_message(text_row(i, &value)).unwrap();
            let txn = src.txn.as_ref().unwrap();
            assert!(txn.bytes < PART_BYTES, "{} bytes held after {i} changes", txn.bytes);
        }
        assert!(src.txn.as_ref().unwrap().spool.as_ref().is_some_and(|s| s.changes() > 2000));
        src.handle_message(commit(100)).unwrap();
        src.drain_acks(true).unwrap();
        let changes = logged_changes(&log);
        assert_eq!(changes.len(), 3000);
        assert!(changes.iter().enumerate().all(|(i, (t, c))| *t == 10 && matches!(c, TableChange::Upsert { row, .. } if row[0] == Datum::Int4(i as i32))));
        // nothing is left in the spool directory
        let dir = &src.engine.shared.spool.get().unwrap().dir;
        assert_eq!(std::fs::read_dir(dir).unwrap().count(), 0);
    }

    /// Spooled changes take part in what happens later in their
    /// transaction: a schema change reshapes them, a value that fails its
    /// table drops that table's changes; and a session that ends before the
    /// commit leaves no spool behind.
    #[test]
    fn spooled_changes_follow_their_transaction() {
        let value = vec![b'v'; 1000];
        // a schema change of table 10 (column b added) after 2000 changes
        let (mut src, log) = text_and_int_tables();
        for i in 0..2000 {
            src.handle_message(text_row(i, &value)).unwrap();
        }
        assert!(src.txn.as_ref().unwrap().spool.is_some());
        let LogicalMessage::Relation(mut rel) = relation(&["id", "s", "b"], 1) else { unreachable!() };
        rel.columns[1].type_oid = 25;
        src.handle_message(LogicalMessage::Relation(rel)).unwrap();
        src.handle_message(commit(100)).unwrap();
        src.drain_acks(true).unwrap();
        let changes = logged_changes(&log);
        assert_eq!(changes.len(), 2000);
        assert!(changes.iter().all(|(_, c)| matches!(c, TableChange::Upsert { row, .. } if row.len() == 3 && row[2].is_null())));
        // a value of table 10 that does not convert, after spooled changes
        // of tables 10 and 11
        let (mut src, log) = text_and_int_tables();
        for i in 0..2000 {
            src.handle_message(text_row(i, &value)).unwrap();
            src.handle_message(u_row(i)).unwrap();
        }
        assert!(src.txn.as_ref().unwrap().spool.is_some());
        src.handle_message(text_row(5000, &[0xe9])).unwrap();
        src.handle_message(u_row(5000)).unwrap();
        src.handle_message(commit(100)).unwrap();
        src.drain_acks(true).unwrap();
        let changes = logged_changes(&log);
        assert_eq!(changes.len(), 2001);
        assert!(changes.iter().all(|(t, _)| *t == 11));
        // the session ends in the middle of a spooled transaction
        let (mut src, log) = text_and_int_tables();
        for i in 0..2000 {
            src.handle_message(text_row(i, &value)).unwrap();
        }
        let dir = src.engine.shared.spool.get().unwrap().dir.clone();
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
        src.txn = None;
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
        assert!(logged_changes(&log).is_empty());
    }

    /// A change to a key that a later chunk reads is overwritten by that
    /// chunk's row, so that chunk's snapshot must contain the change's
    /// transaction, which can be decoded before it is visible (its commit
    /// record is written first; a synchronous standby can keep it invisible
    /// for long): the chunk waits for it. It used to be read regardless, and
    /// the change was lost.
    #[test]
    fn chunks_wait_for_transactions_decoded_before_them() {
        let (mut src, log) = source(vec![table(&["id", "a"], vec![0])], SourceState { slot_created: true, ..Default::default() });
        src.handle_message(begin(80)).unwrap();
        src.handle_message(relation(&["id", "a"], 1)).unwrap();
        src.handle_message(commit(100)).unwrap();
        let seq = start_chunk(&mut src, true);
        src.copy_event(rows_read(seq, &[&[1, 10], &[2, 20]], false)).unwrap();
        // a key the chunk does not have: applied
        src.handle_message(begin(150)).unwrap();
        src.handle_message(insert(&[5, 50])).unwrap();
        src.handle_message(commit(200)).unwrap();
        watermarks(&mut src, (151, 210));
        src.drain_acks(true).unwrap();
        assert_eq!(replay(&log.lock()).len(), 3);
        assert!(!src.state.tables[&10].done);
        // the next chunk: its snapshot must contain transaction 150
        assert!(!src.snapshot_sees_decoded(&(140, 145, vec![])), "150 is not visible");
        assert!(!src.snapshot_sees_decoded(&(140, 160, vec![150])), "150 is still running");
        assert!(src.snapshot_sees_decoded(&(140, 160, vec![])));
        assert!(src.unseen.is_empty(), "seen transactions are forgotten");
    }

    /// Transactions are tracked while a table is being copied, including
    /// those decoded just before a table starts to be copied again (their
    /// changes were applied to rows the re-snapshot overwrites).
    #[test]
    fn decoded_transactions_are_tracked_while_copying() {
        let (mut src, _) = source(vec![table(&["id", "a"], vec![0])], done_state(10));
        src.handle_message(begin(5)).unwrap();
        src.handle_message(relation(&["id", "a"], 1)).unwrap();
        src.handle_message(insert(&[1, 1])).unwrap();
        src.handle_message(commit(100)).unwrap();
        assert!(src.unseen.is_empty(), "nothing is being copied");
        // a schema change: the table is copied again
        src.handle_message(begin(6)).unwrap();
        src.handle_message(relation(&["id", "a", "b"], 1)).unwrap();
        src.handle_message(commit(110)).unwrap();
        assert_eq!(src.unseen.iter().copied().collect::<Vec<_>>(), [5, 6]);
    }

    fn update(vals: &[Option<i32>]) -> LogicalMessage {
        let new = vals.iter().map(|v| v.map(|v| TupleValue::Text(v.to_string().into_bytes())).unwrap_or(TupleValue::UnchangedToast)).collect();
        LogicalMessage::Update { relation_id: REL, old: None, old_is_key: false, new }
    }

    fn delete(vals: &[i32]) -> LogicalMessage {
        LogicalMessage::Delete { relation_id: REL, old: vals.iter().map(|v| TupleValue::Text(v.to_string().into_bytes())).collect(), old_is_key: false }
    }

    /// One upstream transaction.
    fn txn(src: &mut PgSource, xid: u32, end: u64, msgs: Vec<LogicalMessage>) {
        src.handle_message(begin(xid)).unwrap();
        for m in msgs {
            src.handle_message(m).unwrap();
        }
        src.handle_message(commit(end)).unwrap();
    }

    /// A keyed table being copied and its relation.
    fn copying_source(names: &[&str], keyed: bool) -> (PgSource, Arc<Mutex<Vec<LogRecord>>>) {
        let pk = if keyed { vec![0] } else { vec![] };
        let (mut src, log) = source(vec![table(names, pk)], SourceState { slot_created: true, ..Default::default() });
        txn(&mut src, 80, 100, vec![relation(names, keyed as usize)]);
        (src, log)
    }

    /// Streamed changes apply, whether or not the chunk in flight has their
    /// key; only one the chunk's snapshot contains, for a key the chunk has,
    /// is skipped (the chunk's row is newer). Keys are looked up, never
    /// compared: changes to keys beyond the chunks read so far, in the
    /// engine's key order, used to be skipped, which made chunks follow the
    /// engine's order (text keys compared in the C collation upstream, which
    /// no index serves).
    #[test]
    fn streamed_changes_apply_unless_the_chunk_has_newer() {
        let (mut src, log) = copying_source(&["id", "a"], true);
        let seq = start_chunk(&mut src, true);
        src.copy_event(rows_read(seq, &[&[5, 50]], false)).unwrap();
        // in the chunk's snapshot
        txn(&mut src, 90, 200, vec![update(&[Some(5), Some(51)]), insert(&[7, 70])]);
        // not in it
        txn(&mut src, 150, 210, vec![insert(&[1, 10])]);
        src.handle_message(begin(151)).unwrap();
        src.handle_message(message("L:1:10:1")).unwrap();
        src.handle_message(commit(220)).unwrap();
        txn(&mut src, 160, 230, vec![insert(&[9, 90])]);
        src.handle_message(begin(161)).unwrap();
        src.handle_message(message("H:1:10:1")).unwrap();
        src.handle_message(commit(240)).unwrap();
        src.drain_acks(true).unwrap();
        let rows: Vec<Row> = [[1, 10], [5, 50], [7, 70], [9, 90]].iter().map(|r| int_row(r)).collect();
        assert_eq!(replay(&log.lock()), rows);
        assert!(!src.state.tables[&10].done, "not the last chunk");
    }

    /// The copy thread reads a chunk while the stream goes on: changes to
    /// its table decoded before its rows arrive apply, and are held against
    /// the rows when they do. The rows they are newer than (in the window,
    /// or not in the snapshot) go; the others stay.
    #[test]
    fn changes_before_the_rows_arrive_are_decided_with_them() {
        let (mut src, log) = copying_source(&["id", "a"], true);
        let seq = start_chunk(&mut src, true);
        txn(&mut src, 90, 200, vec![update(&[Some(2), Some(21)])]);
        txn(&mut src, 150, 210, vec![update(&[Some(3), Some(31)])]);
        src.handle_message(begin(151)).unwrap();
        src.handle_message(message("L:1:10:1")).unwrap();
        src.handle_message(commit(220)).unwrap();
        txn(&mut src, 160, 230, vec![update(&[Some(1), Some(11)]), delete(&[4, 40])]);
        // (the snapshot sees transaction 90, so its row 2 is 21)
        src.copy_event(rows_read(seq, &[&[1, 10], &[2, 21], &[3, 30], &[4, 40]], true)).unwrap();
        let left: Vec<Row> = src.chunk.as_ref().unwrap().rows.as_ref().unwrap().values().cloned().collect();
        assert_eq!(left, [int_row(&[2, 21])]);
        src.handle_message(begin(161)).unwrap();
        src.handle_message(message("H:1:10:1")).unwrap();
        src.handle_message(commit(240)).unwrap();
        src.drain_acks(true).unwrap();
        let rows: Vec<Row> = [[1, 11], [2, 21], [3, 31]].iter().map(|r| int_row(r)).collect();
        assert_eq!(replay(&log.lock()), rows);
        assert!(src.state.tables[&10].done);
    }

    /// An update leaving TOASTed values unchanged takes them from the
    /// chunk's row where the stream wins over the chunk. Before the rows
    /// arrive, the update is logged for the engine to fill from its own row
    /// (it has none here: the table is being copied), and the chunk's row of
    /// the key, or of the old key of a key change, takes the update's values
    /// when the rows arrive.
    #[test]
    fn toast_updates_before_the_rows_arrive_are_merged_into_them() {
        let (mut src, log) = copying_source(&["id", "a", "b"], true);
        let seq = start_chunk(&mut src, true);
        src.handle_message(begin(151)).unwrap();
        src.handle_message(message("L:1:10:1")).unwrap();
        src.handle_message(commit(220)).unwrap();
        let key_change = LogicalMessage::Update {
            relation_id: REL,
            old: Some(vec![TupleValue::Text(b"2".to_vec())]),
            old_is_key: true,
            new: vec![TupleValue::Text(b"5".to_vec()), TupleValue::Text(b"55".to_vec()), TupleValue::UnchangedToast],
        };
        txn(&mut src, 160, 230, vec![update(&[Some(1), Some(11), None]), key_change]);
        txn(&mut src, 161, 240, vec![update(&[Some(1), None, Some(111)])]);
        src.copy_event(rows_read(seq, &[&[1, 10, 100], &[2, 20, 200], &[3, 30, 300]], true)).unwrap();
        src.handle_message(begin(162)).unwrap();
        src.handle_message(message("H:1:10:1")).unwrap();
        src.handle_message(commit(250)).unwrap();
        src.drain_acks(true).unwrap();
        let rows: Vec<Row> = [[1, 11, 111], [3, 30, 300], [5, 55, 200]].iter().map(|r| int_row(r)).collect();
        assert_eq!(replay(&log.lock()), rows);
    }

    /// A key change leaving TOASTed values unchanged, from a key neither the
    /// chunk in flight nor perhaps the engine has (not copied yet), to a key
    /// that may be copied already: the new key is read again, by a chunk of
    /// its own, before the table is done. The key is logged before the
    /// transaction's commit. (Its row used to be NULL-filled for good.)
    #[test]
    fn key_changes_from_keys_not_copied_are_read_again() {
        let mut state = SourceState { slot_created: true, ..Default::default() };
        state.tables.insert(10, SnapState { boundary: Some(vec!["9".into()]), index_order: true, scanned: true, ..Default::default() });
        let (mut src, log) = source(vec![table(&["id", "a", "b"], vec![0])], state);
        let key_change = LogicalMessage::Update {
            relation_id: REL,
            old: Some(vec![TupleValue::Text(b"30".to_vec())]),
            old_is_key: true,
            new: vec![TupleValue::Text(b"5".to_vec()), TupleValue::Text(b"55".to_vec()), TupleValue::UnchangedToast],
        };
        txn(&mut src, 80, 100, vec![relation(&["id", "a", "b"], 1), key_change]);
        src.drain_acks(true).unwrap();
        {
            let recs = log.lock();
            match &recs[..] {
                [LogRecord::ProgressDelta { delta, .. }, LogRecord::Commit { changes, .. }] => {
                    let d: StateDelta = serde_json::from_str(delta).unwrap();
                    assert_eq!(d.tables[&10].reread, [vec!["5".to_string()]]);
                    assert!(matches!(&changes[..], [(10, TableChange::Delete { .. }), (10, TableChange::UpsertIfPresent { .. })]), "{changes:?}");
                }
                other => panic!("{other:?}"),
            }
        }
        assert!(!src.state.tables[&10].done, "the key is still to be read");
        let seq = start_chunk(&mut src, true);
        src.chunk.as_mut().unwrap().reread = Some(vec![vec!["5".into()]]);
        src.copy_event(rows_read(seq, &[&[5, 55, 300]], false)).unwrap();
        watermarks(&mut src, (81, 110));
        src.drain_acks(true).unwrap();
        let st = &src.state.tables[&10];
        assert!(st.done && st.reread.is_empty() && st.boundary == Some(vec!["9".into()]), "{st:?}");
        assert_eq!(engine_table(&log.lock()), [int_row(&[5, 55, 300])]);
        assert_eq!(
            reread_chunk_sql(&table(&["id", "a", "b"], vec![0]), &[vec!["5".into()], vec!["7".into()]]),
            r#"SELECT "id", "a", "b" FROM "public"."t" WHERE ("id") IN (('5'), ('7'))"#
        );
    }

    /// While a table is being copied, the engine fills unchanged TOAST values
    /// only from a row it has: a key without one is not copied yet, and its
    /// copy brings the whole row. (It used to make a row of NULLs.)
    #[test]
    fn toast_updates_of_keys_not_copied_are_conditional() {
        for done in [false, true] {
            let state = if done { done_state(10) } else { SourceState { slot_created: true, ..Default::default() } };
            let (mut src, log) = source(vec![table(&["id", "a", "b"], vec![0])], state);
            txn(&mut src, 80, 100, vec![relation(&["id", "a", "b"], 1), update(&[Some(1), Some(11), None])]);
            src.drain_acks(true).unwrap();
            let recs = log.lock();
            let LogRecord::Commit { changes, .. } = recs.last().unwrap() else { panic!("{recs:?}") };
            match (&changes[..], done) {
                ([(10, TableChange::Upsert { unchanged, .. })], true) | ([(10, TableChange::UpsertIfPresent { unchanged, .. })], false) => {
                    assert_eq!(unchanged, &[2])
                }
                (other, _) => panic!("done: {done}: {other:?}"),
            }
        }
    }

    /// A keyless table is read in one snapshot, in pieces logged as they
    /// arrive (it used to be one query and one commit of the whole table);
    /// the changes the snapshot does not contain apply after all of them.
    #[test]
    fn keyless_copies_are_logged_in_pieces() {
        let (mut src, log) = copying_source(&["a", "b"], false);
        let seq = start_chunk(&mut src, false);
        // decoded before the snapshot is known
        txn(&mut src, 90, 200, vec![insert(&[1, 1])]);
        txn(&mut src, 150, 210, vec![insert(&[2, 2])]);
        src.copy_event(CopyEvent::Snapshot { seq, snap: SNAP }).unwrap();
        src.copy_event(CopyEvent::Piece { seq, rows: vec![int_row(&[1, 1]), int_row(&[3, 3])], last: false }).unwrap();
        txn(&mut src, 160, 220, vec![delete(&[3, 3])]);
        src.copy_event(CopyEvent::Piece { seq, rows: vec![int_row(&[4, 4])], last: true }).unwrap();
        watermarks(&mut src, (161, 230));
        src.drain_acks(true).unwrap();
        let recs = log.lock();
        let rows: Vec<Row> = [[1, 1], [2, 2], [4, 4]].iter().map(|r| int_row(r)).collect();
        assert_eq!(replay(&recs), rows);
        let commits: Vec<usize> = recs
            .iter()
            .filter_map(|r| match r {
                LogRecord::Commit { changes, .. } => Some(changes.len()),
                _ => None,
            })
            .collect();
        // the truncate, the two pieces, and the changes the snapshot missed
        // with the high watermark
        assert_eq!(commits, [1, 2, 1, 2]);
        assert!(src.state.tables[&10].done);
    }

    /// Progress records carry only what changed: copying T tables logs
    /// O(T) bytes of progress (whole states made it O(T^2)), and the
    /// records rebuild the source's state.
    #[test]
    fn progress_records_carry_what_changed() {
        let n = 2000;
        let tables: Vec<SourceTable> = (0..n).map(|i| SourceTable { id: 1000 + i, ..table(&["id"], vec![0]) }).collect();
        let (mut src, log) = source(tables, SourceState { slot_created: true, ..Default::default() });
        for (i, t) in (1000..1000 + n).enumerate() {
            let seq = start_chunk(&mut src, true);
            let ch = src.chunk.as_mut().unwrap();
            (ch.table, ch.high) = (t, format!("H:{t}"));
            src.copy_event(CopyEvent::Rows { seq, snap: SNAP, rows: vec![], last_key: Some(vec!["7".into()]), last: true }).unwrap();
            txn(&mut src, 90, 100 + i as u64, vec![message(&format!("H:{t}"))]);
        }
        src.drain_acks(true).unwrap();
        assert!(!src.copying());
        let mut rebuilt = SourceState { slot_created: true, ..Default::default() };
        let mut bytes = 0;
        for r in log.lock().iter() {
            if let LogRecord::ProgressDelta { delta, .. } = r {
                bytes += delta.len();
                rebuilt.apply(delta);
            }
        }
        assert!(bytes < 150 * n as usize, "{bytes} bytes of progress for {n} tables");
        assert_eq!(rebuilt.tables, src.state.tables);
        let st = SnapState { done: true, boundary: Some(vec!["7".into()]), index_order: true, scanned: true, ..Default::default() };
        assert_eq!(rebuilt.tables[&1000], st);
        // a full state still replaces the stored one; a delta that does not
        // parse empties it
        assert!(SourceState::parse(&done_state(10).to_json()).tables[&10].done);
        rebuilt.apply("{");
        assert!(rebuilt.tables.is_empty() && !rebuilt.slot_created);
    }

    /// A snapshot boundary logged by an earlier version is in the engine's
    /// key order: kept for a key without string columns (whose chunks
    /// followed the index order already), else dropped (the copy starts
    /// again from the first key).
    #[test]
    fn boundaries_of_earlier_versions() {
        let mut text = SourceTable { id: 11, ..table(&["k"], vec![0]) };
        text.columns[0].ty.ty = SqlType::Text;
        let mut state = SourceState { slot_created: true, ..Default::default() };
        let old = SnapState { boundary: Some(vec!["5".into()]), ..Default::default() };
        state.tables.insert(10, old.clone());
        state.tables.insert(11, old.clone());
        let (src, _) = source(vec![table(&["id"], vec![0]), text], state);
        assert_eq!(src.state.tables[&10], SnapState { index_order: true, ..old });
        assert_eq!(src.state.tables[&11], SnapState::default());
        assert_eq!(src.incomplete.iter().copied().collect::<Vec<_>>(), [10, 11]);
    }

    /// A keyed chunk's query is served by the key index: the key columns
    /// as they are (their own collation), the boundary as untyped literals.
    #[test]
    fn chunk_queries_follow_the_key_index() {
        let mut t = table(&["k", "n", "a"], vec![0, 1]);
        t.columns[0].ty.ty = SqlType::Text;
        assert_eq!(keyed_chunk_sql(&t, None), r#"SELECT "k", "n", "a" FROM "public"."t" ORDER BY "k", "n" LIMIT 10000"#);
        assert_eq!(
            keyed_chunk_sql(&t, Some(&["it's".into(), "5".into()])),
            r#"SELECT "k", "n", "a" FROM "public"."t" WHERE ("k", "n") > ('it''s', '5') ORDER BY "k", "n" LIMIT 10000"#
        );
    }

    /// An upstream change in the model of `copies_converge_under_random_streams`.
    #[derive(Clone, Debug)]
    enum Op {
        Insert(i32, i32, i32),
        /// update of `a`, the TOASTed `b` unchanged
        SetA(i32, i32),
        SetB(i32, i32),
        Delete(i32),
        /// key change, `b` unchanged
        Move(i32, i32),
        Truncate,
    }

    type Model = BTreeMap<i32, (i32, i32)>;

    fn model_apply(m: &mut Model, op: &Op) {
        match *op {
            Op::Insert(k, a, b) => {
                m.insert(k, (a, b));
            }
            Op::SetA(k, a) => m.get_mut(&k).unwrap().0 = a,
            Op::SetB(k, b) => m.get_mut(&k).unwrap().1 = b,
            Op::Delete(k) => {
                m.remove(&k);
            }
            Op::Move(k1, k2) => {
                let v = m.remove(&k1).unwrap();
                m.insert(k2, v);
            }
            Op::Truncate => m.clear(),
        }
    }

    /// The pgoutput message of a change (to table 10: id, a, b).
    fn op_message(m: &Model, op: &Op) -> LogicalMessage {
        let t = |v: i32| TupleValue::Text(v.to_string().into_bytes());
        let key_only = |k: i32| Some(vec![t(k), TupleValue::Null, TupleValue::Null]);
        match *op {
            Op::Insert(k, a, b) => LogicalMessage::Insert { relation_id: REL, new: vec![t(k), t(a), t(b)] },
            Op::SetA(k, a) => LogicalMessage::Update { relation_id: REL, old: None, old_is_key: false, new: vec![t(k), t(a), TupleValue::UnchangedToast] },
            Op::SetB(k, b) => LogicalMessage::Update { relation_id: REL, old: None, old_is_key: false, new: vec![t(k), t(m[&k].0), t(b)] },
            Op::Delete(k) => LogicalMessage::Delete { relation_id: REL, old: key_only(k).unwrap(), old_is_key: true },
            Op::Move(k1, k2) => {
                LogicalMessage::Update { relation_id: REL, old: key_only(k1), old_is_key: true, new: vec![t(k2), t(m[&k1].0), TupleValue::UnchangedToast] }
            }
            Op::Truncate => LogicalMessage::Truncate { options: 0, relation_ids: vec![REL] },
        }
    }

    /// The keys an operation touches.
    fn op_keys(op: &Op) -> Vec<i32> {
        match *op {
            Op::Insert(k, ..) | Op::SetA(k, _) | Op::SetB(k, _) | Op::Delete(k) => vec![k],
            Op::Move(k1, k2) => vec![k1, k2],
            Op::Truncate => vec![],
        }
    }

    /// A random change to `m` touching no key of `avoid` (and no truncate
    /// unless `truncate`).
    fn random_op(rng: &mut crate::testkit::Rng, m: &Model, avoid: &BTreeSet<i32>, truncate: bool) -> Option<Op> {
        let free = |k: &i32| !avoid.contains(k);
        let present: Vec<i32> = m.keys().copied().filter(free).collect();
        let absent: Vec<i32> = (0..30).filter(|k| !m.contains_key(k) && free(k)).collect();
        let v = rng.range(0, 999) as i32;
        Some(match rng.below(13) {
            0..=2 if !absent.is_empty() => Op::Insert(*rng.pick(&absent), v, v + 1000),
            3..=5 if !present.is_empty() => Op::SetA(*rng.pick(&present), v),
            6 | 7 if !present.is_empty() => Op::SetB(*rng.pick(&present), v),
            8 | 9 if !present.is_empty() => Op::Delete(*rng.pick(&present)),
            10 | 11 if !present.is_empty() && !absent.is_empty() => Op::Move(*rng.pick(&present), *rng.pick(&absent)),
            12 if truncate => Op::Truncate,
            _ => return None,
        })
    }

    /// The table as the engine applies the logged commits (`table_step`):
    /// unchanged values come from the key's row or the row the change just
    /// before deleted, else are NULL (a conditional upsert does nothing
    /// without a row of its key).
    fn engine_table(log: &[LogRecord]) -> Vec<Row> {
        let mut table: BTreeMap<i64, Row> = BTreeMap::new();
        for r in log {
            let LogRecord::Commit { changes, .. } = r else { continue };
            let mut deleted: Option<Row> = None;
            for (_, ch) in changes {
                let just_deleted = deleted.take();
                match ch {
                    TableChange::Upsert { row, unchanged } | TableChange::UpsertIfPresent { row, unchanged } => {
                        let k = row[0].as_i64().unwrap();
                        let mut row = row.clone();
                        if !unchanged.is_empty() {
                            let if_present = matches!(ch, TableChange::UpsertIfPresent { .. });
                            match table.get(&k).cloned().or(just_deleted.filter(|_| !if_present)) {
                                Some(o) => unchanged.iter().for_each(|c| row[*c] = o[*c].clone()),
                                None if if_present => continue,
                                None => unchanged.iter().for_each(|c| row[*c] = Datum::Null),
                            }
                        }
                        table.insert(k, row);
                    }
                    TableChange::Delete { key } => deleted = table.remove(&key[0].as_i64().unwrap()),
                    TableChange::Truncate => table.clear(),
                    other => panic!("{other:?}"),
                }
            }
        }
        table.into_values().collect()
    }

    /// Model test of copying a table while it changes: random upstream
    /// transactions (inserts, updates leaving the TOASTed column unchanged
    /// or not, deletes, key changes, truncates) before, around and between
    /// random chunks, whose rows arrive at random points of the stream (also
    /// before their low watermark), some transactions decoded before the low
    /// watermark but not visible to the chunk's snapshot (they touch keys no
    /// other transaction touches meanwhile, as row locks make them). The
    /// engine's table, replayed from the log as the engine applies it, must
    /// end equal to the upstream table; also for a table copied again (its
    /// rows stored before).
    #[test]
    fn copies_converge_under_random_streams() {
        let seeds = match (std::env::var("T7_SEED"), std::env::var("T7_SEEDS")) {
            (Ok(s), _) => s.parse().unwrap()..s.parse::<u64>().unwrap() + 1,
            (_, Ok(s)) => 0..s.parse::<u64>().unwrap(),
            _ => 0..500,
        };
        for seed in seeds {
            copy_model(seed);
        }
    }

    /// The upstream of `copy_model`: its table, and its transactions so far.
    struct Upstream {
        rng: crate::testkit::Rng,
        up: Model,
        /// the table before the transactions, and every transaction's changes
        init: Model,
        history: Vec<(u32, Vec<Op>)>,
        xid: u32,
        lsn: u64,
    }

    impl Upstream {
        /// A transaction of random changes touching no key of `avoid`, as
        /// stream messages; returns its xid and the keys it touched.
        fn txn(&mut self, avoid: &BTreeSet<i32>, truncate: bool, out: &mut Vec<LogicalMessage>) -> (u32, Vec<i32>) {
            self.xid += 1;
            self.lsn += 10;
            let (mut ops, mut keys) = (Vec::new(), Vec::new());
            out.push(begin(self.xid));
            for _ in 0..1 + self.rng.index(3) {
                if let Some(op) = random_op(&mut self.rng, &self.up, avoid, truncate) {
                    out.push(op_message(&self.up, &op));
                    model_apply(&mut self.up, &op);
                    keys.extend(op_keys(&op));
                    ops.push(op);
                }
            }
            out.push(commit(self.lsn));
            self.history.push((self.xid, ops));
            (self.xid, keys)
        }

        fn txns(&mut self, n: u64, out: &mut Vec<LogicalMessage>) {
            for _ in 0..self.rng.below(n) {
                self.txn(&BTreeSet::new(), true, out);
            }
        }

        fn watermark(&mut self, tag: &str, out: &mut Vec<LogicalMessage>) {
            self.xid += 1;
            self.lsn += 10;
            out.extend([begin(self.xid), message(tag), commit(self.lsn)]);
        }

        /// The table as a snapshot sees it.
        fn seen(&self, snap: &Snap) -> Model {
            let mut m = self.init.clone();
            for (x, ops) in &self.history {
                if xid_visible(snap, *x) {
                    ops.iter().for_each(|op| model_apply(&mut m, op));
                }
            }
            m
        }
    }

    fn copy_model(seed: u64) {
        let (mut src, log) = copying_source(&["id", "a", "b"], true);
        let mut rng = crate::testkit::Rng::new(seed);
        let mut up = Model::new();
        for k in 0..30 {
            if rng.chance(50) {
                up.insert(k, (k, k + 1000));
            }
        }
        if rng.chance(30) {
            // copied again: the stored rows are the upstream's
            let changes = up.iter().map(|(k, (a, b))| (10, TableChange::Upsert { row: int_row(&[*k, *a, *b]), unchanged: vec![] })).collect();
            src.log_tracked(LogRecord::Commit { source: 1, lsn: 100, changes }).unwrap();
        }
        let chunk_rows = 1 + rng.index(5);
        let mut u = Upstream { rng, init: up.clone(), up, history: Vec::new(), xid: 100, lsn: 1000 };
        let feed = |src: &mut PgSource, msgs: Vec<LogicalMessage>| msgs.into_iter().for_each(|m| src.handle_message(m).unwrap());
        let mut msgs = Vec::new();
        u.txns(3, &mut msgs);
        if std::env::var("T7_DEBUG").is_ok() {
            eprintln!("initial {:?}", u.init);
            msgs.iter().for_each(|m| eprintln!("  before: {m:?}"));
        }
        feed(&mut src, std::mem::take(&mut msgs));
        let mut boundary: Option<i32> = None;
        while !src.state.tables.get(&10).is_some_and(|s| s.done) {
            // as `maybe_start_chunk`: keys to read again first, then a range
            let reread: Option<Vec<Vec<String>>> = src.state.tables.get(&10).filter(|s| !s.reread.is_empty()).map(|s| s.reread.clone());
            let seq = start_chunk(&mut src, true);
            let (low, high) = (format!("L:{seq}"), format!("H:{seq}"));
            let ch = src.chunk.as_mut().unwrap();
            (ch.low, ch.high, ch.reread) = (low.clone(), high.clone(), reread.clone());
            let mut stream = Vec::new();
            // before the low watermark, some transactions not visible to the
            // chunk's snapshot (with keys of their own: no truncate meanwhile)
            let (mut racing, mut locked, mut touched) = (Vec::new(), BTreeSet::new(), BTreeSet::new());
            for _ in 0..u.rng.below(4) {
                if u.rng.chance(30) {
                    let (x, keys) = u.txn(&touched.union(&locked).copied().collect(), false, &mut stream);
                    racing.push(x);
                    locked.extend(keys);
                } else {
                    let (_, keys) = u.txn(&locked, racing.is_empty(), &mut stream);
                    touched.extend(keys);
                }
            }
            u.watermark(&low, &mut stream);
            for _ in 0..u.rng.below(3) {
                u.txn(&locked, racing.is_empty(), &mut stream);
            }
            // the chunk's snapshot and rows
            let snap: Snap = (1, u.xid as u64 + 1, racing.iter().map(|x| *x as u64).collect());
            let from = boundary.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
            let seen = u.seen(&snap);
            let rows: Vec<Row> = match &reread {
                Some(keys) => keys.iter().filter_map(|k| k[0].parse().ok()).filter_map(|k| seen.get_key_value(&k)).map(|(k, (a, b))| int_row(&[*k, *a, *b])).collect(),
                None => seen.range((from, std::ops::Bound::Unbounded)).take(chunk_rows).map(|(k, (a, b))| int_row(&[*k, *a, *b])).collect(),
            };
            let last = reread.is_none() && rows.len() < chunk_rows;
            let last_key = rows.last().filter(|_| reread.is_none()).map(|r| r[0].as_i64().unwrap() as i32);
            u.txns(4, &mut stream);
            u.watermark(&high, &mut stream);
            // the rows arrive at some point before the high watermark's commit
            let at = u.rng.index(stream.len());
            if std::env::var("T7_DEBUG").is_ok() {
                eprintln!("chunk {seq} after {boundary:?}: snap {snap:?} rows {:?} last {last} at {at}", rows.iter().map(|r| r[0].as_i64().unwrap()).collect::<Vec<_>>());
                for (i, m) in stream.iter().enumerate() {
                    eprintln!("  {i}: {m:?}");
                }
            }
            let mut rows = Some(rows.into_iter().map(|r| (pk_bytes(&r, &[0]), r)).collect::<Vec<_>>());
            for (i, m) in stream.into_iter().enumerate() {
                if i == at {
                    let rows = rows.take().unwrap();
                    src.copy_event(CopyEvent::Rows { seq, snap: snap.clone(), rows, last_key: last_key.map(|k| vec![k.to_string()]), last }).unwrap();
                }
                src.handle_message(m).unwrap();
            }
            assert!(src.chunk.is_none(), "seed {seed}: the chunk is done");
            if reread.is_none() {
                assert_eq!(src.state.tables[&10].scanned, last, "seed {seed}");
                boundary = last_key.or(boundary);
            }
            u.txns(3, &mut msgs);
            if std::env::var("T7_DEBUG").is_ok() {
                msgs.iter().for_each(|m| eprintln!("  between: {m:?}"));
            }
            feed(&mut src, std::mem::take(&mut msgs));
        }
        src.drain_acks(true).unwrap();
        let want: Vec<Row> = u.up.iter().map(|(k, (a, b))| int_row(&[*k, *a, *b])).collect();
        assert_eq!(engine_table(&log.lock()), want, "seed {seed}");
    }

    #[test]
    fn xid_visibility_across_epochs() {
        let snap = parse_snapshot("100:110:103,105").unwrap();
        assert!(xid_visible(&snap, 99));
        assert!(xid_visible(&snap, 104));
        assert!(!xid_visible(&snap, 103));
        assert!(!xid_visible(&snap, 110));
        // snapshot straddling the 2^32 boundary
        let e = 1u64 << 32;
        let snap = (e - 16, e + 16, vec![e + 3]);
        assert!(xid_visible(&snap, 0xFFFF_FFE0));
        assert!(xid_visible(&snap, 2));
        assert!(!xid_visible(&snap, 3));
        assert!(!xid_visible(&snap, 20));
        // just after a wraparound: an xid from the previous epoch
        let snap = (e + 5, e + 9, vec![]);
        assert!(xid_visible(&snap, 0xFFFF_FFF0));
        assert!(!xid_visible(&snap, 9));
    }
}
