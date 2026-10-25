//! PostgreSQL logical replication source (pgoutput).
//!
//! The source replicates published tables into the engine:
//! - streaming changes become upserts/deletes by primary key (before-images
//!   come from the engine's stored rows, so REPLICA IDENTITY FULL is not
//!   needed; unchanged TOAST values are filled from the stored row);
//! - the initial snapshot (and the snapshot of tables added later) is taken
//!   in primary-key-ordered chunks interleaved with the stream, ordered by
//!   DBLog-style low/high watermarks written into the WAL with
//!   pg_logical_emit_message: inside the watermark window, streamed changes
//!   win over chunk rows; outside it, keys beyond the snapshot boundary are
//!   skipped because a later chunk will capture them;
//! - the slot is acknowledged as soon as a commit is durable in the
//!   engine's input log.

use super::{SourceCtl, SourceTable};
use crate::catalog::SourceDef;
use crate::coord::ddl::DiscoveredTable;
use crate::coord::EngineHandle;
use crate::dataflow::TableChange;
use crate::error::{codes, Result, SqlError};
use crate::log::LogRecord;
use crate::sql::plan::ObjectId;
use crate::types::encoding::{encode_key, SortSpec};
use crate::types::text::{format_datum, parse_datum};
use crate::types::{Column, ColumnType, Datum, Row, SqlType};
use crossbeam_channel::Receiver;
use parking_lot::Mutex;
use pgproto::pgoutput::{LogicalMessage, Relation, TupleValue};
use pgproto::{Client, Config, Lsn, ReplicationEvent, ReplicationStream};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

const CHUNK_ROWS: usize = 10_000;

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
    Ok(c)
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
        let pkr = c.query(&format!(
            "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
             WHERE i.indrelid = {oid} AND i.indisprimary ORDER BY array_position(i.indkey::int2[], a.attnum)"
        ))?;
        let pk: Vec<usize> = pkr
            .rows
            .iter()
            .filter_map(|r| r[0].as_ref())
            .filter_map(|n| columns.iter().position(|c| &c.name == n))
            .collect();
        out.push(DiscoveredTable { schema, name, columns, pk, replica_identity_full: ident == "f" });
    }
    Ok(out)
}

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct SnapState {
    pub done: bool,
    /// Text values of the last snapshotted primary key.
    pub boundary: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct SourceState {
    pub slot_created: bool,
    pub tables: BTreeMap<ObjectId, SnapState>,
}

/// Mapping of an upstream relation onto a catalog table.
struct RelMap {
    table: ObjectId,
    /// catalog column index for each relation column (None: column unknown)
    cols: Vec<Option<usize>>,
}

#[derive(PartialEq)]
enum Phase {
    BeforeLow,
    InWindow,
}

struct Chunk {
    table: ObjectId,
    low: String,
    high: String,
    phase: Phase,
    /// pk-encoded -> row (PK tables)
    rows: BTreeMap<Vec<u8>, Row>,
    /// rows of a multiset table (single chunk)
    multiset: Vec<Row>,
    /// snapshot visibility (multiset tables): xmin, xmax, in-progress xids
    snap: Option<(u64, u64, Vec<u64>)>,
    max_key: Option<Vec<u8>>,
    max_key_text: Option<Vec<String>>,
    last: bool,
}

struct Txn {
    xid: u32,
    changes: Vec<(ObjectId, TableChange)>,
    /// Chunk rows to emit at this transaction's commit (high watermark).
    finish_chunk: bool,
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
    query: Option<Client>,
    status: Arc<Mutex<String>>,
    seq: u64,
    /// Commits sent to the log whose durability is not yet confirmed.
    inflight: std::collections::VecDeque<(u64, Receiver<u64>)>,
    /// wal_end of a keepalive received while commits were in flight.
    pending_wal_end: u64,
    /// Relations announced in this session for tables the source does not
    /// replicate (yet). pgoutput announces a relation once per session, so
    /// a table added later (ALTER SOURCE REFRESH) is mapped from these.
    unmapped_rels: HashMap<u32, Relation>,
}

fn pk_bytes(row: &[Datum], pk: &[usize]) -> Vec<u8> {
    let mut b = Vec::new();
    for c in pk {
        encode_key(&row[*c], SortSpec::ASC, &mut b);
    }
    b
}

fn text_key_bytes(vals: &[String], t: &SourceTable) -> Vec<u8> {
    let mut b = Vec::new();
    for (v, c) in vals.iter().zip(&t.pk) {
        let col = &t.columns[*c];
        let d = parse_datum(v, &col.ty.ty, -1).unwrap_or(Datum::Null);
        encode_key(&d, SortSpec::ASC, &mut b);
    }
    b
}

impl PgSource {
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
                    let s = v.as_str().unwrap_or("");
                    let col = &t.columns[*c];
                    row[*c] = parse_datum(s, &col.ty.ty, -1)?;
                }
            }
        }
        Ok((row, unchanged))
    }

    /// Decide what to do with a change to `table` with primary key bytes `key`.
    /// Returns Some(fill row) to apply (fill row: chunk row for TOAST), None to skip.
    fn snapshot_filter(&mut self, table: ObjectId, key: &[u8]) -> Option<Option<Row>> {
        let st = self.state.tables.get(&table).cloned().unwrap_or_default();
        if st.done {
            return Some(None);
        }
        let t = &self.tables[&table];
        if t.pk.is_empty() {
            return Some(None);
        }
        if let Some(b) = &st.boundary
            && key <= text_key_bytes(b, t).as_slice()
        {
            return Some(None);
        }
        let xid = self.txn.as_ref().map(|t| t.xid);
        if let Some(ch) = &mut self.chunk
            && ch.table == table
            && (ch.last || ch.max_key.as_deref().is_some_and(|m| key <= m))
        {
            match ch.phase {
                // inside the watermark window the stream wins over the chunk
                Phase::InWindow => return Some(ch.rows.remove(key)),
                // A transaction that committed before the low watermark is
                // normally in the chunk's snapshot. A commit becomes visible
                // only after its WAL record, though, so the snapshot can miss
                // it; then the chunk holds the older row and the stream wins.
                Phase::BeforeLow => {
                    if let (Some(snap), Some(x)) = (&ch.snap, xid)
                        && !xid_visible(snap, x)
                    {
                        return Some(ch.rows.remove(key));
                    }
                }
            }
        }
        None
    }

    fn handle_message(&mut self, msg: LogicalMessage) -> Result<()> {
        match msg {
            LogicalMessage::Begin { xid, .. } => {
                self.txn = Some(Txn { xid, changes: Vec::new(), finish_chunk: false });
            }
            LogicalMessage::Relation(rel) => self.handle_relation(rel)?,
            LogicalMessage::Insert { relation_id, new } => {
                let Some(rm) = self.rels.get(&relation_id) else { return Ok(()) };
                let table = rm.table;
                let (row, unchanged) = self.convert(rm, &new)?;
                self.push_upsert(table, row, unchanged, None)?;
            }
            LogicalMessage::Update { relation_id, old, old_is_key, new } => {
                let Some(rm) = self.rels.get(&relation_id) else { return Ok(()) };
                let table = rm.table;
                let (row, unchanged) = self.convert(rm, &new)?;
                let old_row = match &old {
                    Some(o) => Some(self.convert(self.rels.get(&relation_id).unwrap(), o)?.0),
                    None => None,
                };
                let pk = self.tables[&table].pk.clone();
                if pk.is_empty() {
                    // REPLICA IDENTITY FULL multiset table
                    if self.multiset_visible(table) {
                        return Ok(());
                    }
                    let mut new_row = row;
                    if let Some(o) = &old_row {
                        for c in &unchanged {
                            new_row[*c] = o[*c].clone();
                        }
                        self.push_change(table, TableChange::Remove { row: o.clone() });
                    }
                    self.push_change(table, TableChange::Insert { row: new_row });
                    return Ok(());
                }
                // key change: delete the old key first
                if let Some(o) = &old_row
                    && (old_is_key || !o.is_empty())
                {
                    let ok: Row = pk.iter().map(|c| o[*c].clone()).collect();
                    let nk: Row = pk.iter().map(|c| row[*c].clone()).collect();
                    if pk_bytes(o, &pk) != pk_bytes(&row, &pk) && !ok.iter().any(|d| d.is_null()) {
                        let kb = pk_bytes(o, &pk);
                        if self.snapshot_filter(table, &kb).is_some() {
                            self.push_change(table, TableChange::Delete { key: ok });
                        }
                    }
                    let _ = nk;
                }
                self.push_upsert(table, row, unchanged, old_row)?;
            }
            LogicalMessage::Delete { relation_id, old, .. } => {
                let Some(rm) = self.rels.get(&relation_id) else { return Ok(()) };
                let table = rm.table;
                let (row, _) = self.convert(rm, &old)?;
                let pk = self.tables[&table].pk.clone();
                if pk.is_empty() {
                    if !self.multiset_visible(table) {
                        self.push_change(table, TableChange::Remove { row });
                    }
                    return Ok(());
                }
                let kb = pk_bytes(&row, &pk);
                if self.snapshot_filter(table, &kb).is_some() {
                    let key: Row = pk.iter().map(|c| row[*c].clone()).collect();
                    self.push_change(table, TableChange::Delete { key });
                }
            }
            LogicalMessage::Truncate { relation_ids, .. } => {
                for r in relation_ids {
                    if let Some(rm) = self.rels.get(&r) {
                        let t = rm.table;
                        self.push_change(t, TableChange::Truncate);
                        if let Some(ch) = &mut self.chunk
                            && ch.table == t
                        {
                            ch.rows.clear();
                            ch.multiset.clear();
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
                            if self.tables[&ch.table].pk.is_empty() {
                                // multiset chunk is emitted at the low watermark
                                let rows = std::mem::take(&mut ch.multiset);
                                let t = ch.table;
                                for r in rows {
                                    self.push_change(t, TableChange::Insert { row: r });
                                }
                            }
                        } else if tag == ch.high {
                            if let Some(t) = &mut self.txn {
                                t.finish_chunk = true;
                            }
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
                let Some(mut txn) = txn else { return Ok(()) };
                let mut progress = false;
                if txn.finish_chunk
                    && let Some(ch) = self.chunk.take()
                {
                    let t = ch.table;
                    for (_, r) in ch.rows {
                        txn.changes.push((t, TableChange::Upsert { row: r, unchanged: vec![] }));
                    }
                    let st = self.state.tables.entry(t).or_default();
                    st.boundary = ch.max_key_text.clone().or(st.boundary.clone());
                    st.done = ch.last;
                    progress = true;
                }
                if !txn.changes.is_empty() {
                    let rec = LogRecord::Commit { source: self.id, lsn: end_lsn.0, changes: txn.changes };
                    // pipelined: the sequencer group-commits; durability is
                    // confirmed asynchronously before acknowledging the slot
                    let rx = self.engine.log_async(rec)?;
                    self.inflight.push_back((end_lsn.0, rx));
                } else {
                    let (tx, rx) = crossbeam_channel::bounded(1);
                    let _ = tx.send(0);
                    self.inflight.push_back((end_lsn.0, rx));
                }
                if progress {
                    self.log_progress(end_lsn.0)?;
                }
                self.drain_acks(false)?;
            }
            LogicalMessage::Origin { .. } | LogicalMessage::Type { .. } => {}
        }
        Ok(())
    }

    /// For multiset tables during the snapshot window: skip changes from
    /// transactions visible in the chunk's snapshot.
    fn multiset_visible(&self, table: ObjectId) -> bool {
        let st = self.state.tables.get(&table).cloned().unwrap_or_default();
        if st.done {
            return false;
        }
        let Some(ch) = &self.chunk else { return true };
        if ch.table != table {
            return true;
        }
        if ch.phase == Phase::BeforeLow {
            return true;
        }
        let Some(snap) = &ch.snap else { return false };
        let Some(t) = &self.txn else { return false };
        xid_visible(snap, t.xid)
    }

    fn push_change(&mut self, table: ObjectId, ch: TableChange) {
        match &mut self.txn {
            Some(t) => t.changes.push((table, ch)),
            None => log::warn!("change outside a transaction ignored"),
        }
    }

    fn push_upsert(&mut self, table: ObjectId, mut row: Row, unchanged: Vec<usize>, old: Option<Row>) -> Result<()> {
        let pk = self.tables[&table].pk.clone();
        if pk.is_empty() {
            if !self.multiset_visible(table) {
                self.push_change(table, TableChange::Insert { row });
            }
            return Ok(());
        }
        let kb = pk_bytes(&row, &pk);
        match self.snapshot_filter(table, &kb) {
            None => Ok(()),
            Some(fill) => {
                let mut unchanged = unchanged;
                if !unchanged.is_empty() {
                    let src = fill.or(old);
                    if let Some(f) = src {
                        for c in &unchanged {
                            row[*c] = f[*c].clone();
                        }
                        unchanged.clear();
                    }
                }
                self.push_change(table, TableChange::Upsert { row, unchanged });
                Ok(())
            }
        }
    }

    /// Start replicating more tables (their snapshots follow).
    fn add_tables(&mut self, tables: Vec<SourceTable>) -> Result<()> {
        for t in tables {
            self.tables.insert(t.id, t);
        }
        let known: Vec<u32> = self.unmapped_rels.values().filter(|r| self.table_of(r).is_some()).map(|r| r.id).collect();
        for id in known {
            if let Some(rel) = self.unmapped_rels.remove(&id) {
                self.handle_relation(rel)?;
            }
        }
        Ok(())
    }

    fn handle_relation(&mut self, rel: Relation) -> Result<()> {
        let Some(table) = self.table_of(&rel) else {
            self.unmapped_rels.insert(rel.id, rel);
            return Ok(());
        };
        let t = self.tables[&table].clone();
        let cols: Vec<Option<usize>> = rel.columns.iter().map(|rc| t.columns.iter().position(|c| c.name == rc.name)).collect();
        // detect a changed shape: new/dropped columns or changed types
        let same = rel.columns.len() == t.columns.len()
            && rel.columns.iter().zip(&t.columns).all(|(rc, c)| {
                let (ty, _) = map_type(rc.type_oid, rc.type_modifier);
                rc.name == c.name && ty == c.ty.ty
            });
        if !same {
            let columns: Vec<Column> = rel
                .columns
                .iter()
                .map(|rc| {
                    let (ty, typmod) = map_type(rc.type_oid, rc.type_modifier);
                    let nullable = t.columns.iter().find(|c| c.name == rc.name).map(|c| c.ty.nullable).unwrap_or(true);
                    Column::new(rc.name.clone(), ColumnType { ty, typmod, nullable })
                })
                .collect();
            let pk: Vec<usize> = if rel.replica_identity == b'f' {
                // FULL marks every column as key; keep the known PK by name
                t.pk.iter().filter_map(|p| columns.iter().position(|c| c.name == t.columns[*p].name)).collect()
            } else {
                rel.columns.iter().enumerate().filter(|(_, c)| c.is_key).map(|(i, _)| i).collect()
            };
            log::info!("source table {}.{} changed shape; re-snapshotting", t.upstream.0, t.upstream.1);
            self.engine.log(LogRecord::Schema { source: self.id, table, columns: columns.clone(), pk: pk.clone() })?;
            let nt = SourceTable { id: table, upstream: t.upstream.clone(), columns: columns.clone(), pk };
            self.tables.insert(table, nt);
            // re-snapshot to fill new columns of existing rows
            self.state.tables.insert(table, SnapState::default());
            if self.chunk.as_ref().is_some_and(|c| c.table == table) {
                self.chunk = None;
            }
            let cols2: Vec<Option<usize>> = rel.columns.iter().enumerate().map(|(i, _)| Some(i)).collect();
            self.rels.insert(rel.id, RelMap { table, cols: cols2 });
            return Ok(());
        }
        self.rels.insert(rel.id, RelMap { table, cols });
        Ok(())
    }

    /// Advance the durable LSN over commits the log has made durable.
    /// With `wait`, block until every in-flight commit is durable.
    fn drain_acks(&mut self, wait: bool) -> Result<()> {
        while let Some((lsn, rx)) = self.inflight.front() {
            let lsn = *lsn;
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
            self.inflight.pop_front();
            self.durable_lsn = lsn;
            self.set_frontier(lsn);
        }
        // a keepalive seen while commits were in flight: once they are all
        // durable, everything the server had sent by then is accounted for
        if self.inflight.is_empty() && self.pending_wal_end > 0 && self.chunk.is_none() {
            self.set_frontier(self.pending_wal_end);
            self.pending_wal_end = 0;
        }
        // bound memory if the log falls behind
        if self.inflight.len() > 10_000 {
            return self.drain_acks(true);
        }
        Ok(())
    }

    fn log_progress(&mut self, lsn: u64) -> Result<()> {
        let state = serde_json::to_string(&self.state).unwrap();
        self.engine.log(LogRecord::Progress { source: self.id, lsn, state })?;
        Ok(())
    }

    /// Advance the source's LSN frontier. While a table's snapshot is
    /// incomplete the source covers no LSN at all: reads waiting for an LSN
    /// must not see a partially loaded table.
    fn set_frontier(&self, lsn: u64) {
        if self.tables.keys().all(|t| self.state.tables.get(t).is_some_and(|s| s.done)) {
            self.engine.advance_frontier(self.id, lsn);
        }
    }

    /// Start the next snapshot chunk if none is in flight.
    fn maybe_start_chunk(&mut self) -> Result<()> {
        if self.chunk.is_some() {
            return Ok(());
        }
        let next = self
            .tables
            .keys()
            .copied()
            .filter(|t| !self.state.tables.get(t).is_some_and(|s| s.done))
            .min();
        let Some(table) = next else { return Ok(()) };
        let t = self.tables[&table].clone();
        if self.query.is_none() {
            self.query = Some(connect(&self.def.conninfo, false)?);
        }
        self.seq += 1;
        let low = format!("L:{}:{}:{}", self.id, table, self.seq);
        let high = format!("H:{}:{}:{}", self.id, table, self.seq);
        let q = self.query.as_mut().unwrap();
        let tname = format!("{}.{}", qident(&t.upstream.0), qident(&t.upstream.1));
        // values arrive in text format through the simple query protocol;
        // no casts here, so ORDER BY below refers to the real columns
        let cols = t.columns.iter().map(|c| qident(&c.name)).collect::<Vec<_>>().join(", ");
        q.simple_query(&format!("SELECT pg_logical_emit_message(true, 'ivm', {})", qlit(&low)))?;
        let mut chunk = Chunk {
            table,
            low,
            high: high.clone(),
            phase: Phase::BeforeLow,
            rows: BTreeMap::new(),
            multiset: Vec::new(),
            snap: None,
            max_key: None,
            max_key_text: None,
            last: true,
        };
        if t.pk.is_empty() {
            let res = q.simple_query(&format!(
                "BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT pg_current_snapshot()::text; SELECT {cols} FROM {tname}; COMMIT"
            ))?;
            let snap_text = res.iter().find(|r| !r.rows.is_empty() && r.columns.len() == 1 && r.columns[0].name == "pg_current_snapshot")
                .and_then(|r| r.rows[0][0].clone())
                .unwrap_or_default();
            chunk.snap = parse_snapshot(&snap_text);
            if let Some(data) = res.iter().rev().find(|r| r.columns.len() == t.columns.len() && r.columns.first().map(|c| c.name != "pg_current_snapshot").unwrap_or(true)) {
                for r in &data.rows {
                    chunk.multiset.push(parse_text_row(r, &t)?);
                }
            }
        } else {
            // Chunks must be ordered as the engine orders keys (memcmp of
            // the key encoding). For string columns that is the byte order
            // of the text: compare as text in the C collation. Upstream
            // types replicated as text (enums, inet, ...) compare as their
            // text too, and they do not take a collation themselves.
            let pk_cols: Vec<String> = t
                .pk
                .iter()
                .map(|c| {
                    let col = &t.columns[*c];
                    if col.ty.ty.is_string() {
                        format!("{}::text COLLATE \"C\"", qident(&col.name))
                    } else {
                        qident(&col.name)
                    }
                })
                .collect();
            let where_clause = match self.state.tables.get(&table).and_then(|s| s.boundary.clone()) {
                None => String::new(),
                Some(b) => {
                    let lits: Vec<String> = b
                        .iter()
                        .zip(&t.pk)
                        .map(|(v, c)| {
                            let col = &t.columns[*c];
                            let ty = crate::types::format_type(&col.ty.ty, col.ty.typmod);
                            if col.ty.ty.is_string() {
                                // compared as the column is: via its own type to text
                                format!("{}::{ty}::text COLLATE \"C\"", qlit(v))
                            } else {
                                format!("{}::{ty}", qlit(v))
                            }
                        })
                        .collect();
                    format!(" WHERE ({}) > ({})", pk_cols.join(", "), lits.join(", "))
                }
            };
            // the snapshot tells which commits before the low watermark the
            // chunk may have missed
            let res = q.simple_query(&format!(
                "BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT pg_current_snapshot()::text; \
                 SELECT {cols} FROM {tname}{where_clause} ORDER BY {} LIMIT {CHUNK_ROWS}; COMMIT",
                pk_cols.join(", ")
            ))?;
            let snap_text = res
                .iter()
                .find(|r| !r.rows.is_empty() && r.columns.len() == 1 && r.columns[0].name == "pg_current_snapshot")
                .and_then(|r| r.rows[0][0].clone())
                .unwrap_or_default();
            chunk.snap = parse_snapshot(&snap_text);
            let empty = Vec::new();
            let rows = res
                .iter()
                .rev()
                .find(|r| r.columns.len() == t.columns.len() && r.columns.first().map(|c| c.name != "pg_current_snapshot").unwrap_or(true))
                .map(|r| &r.rows)
                .unwrap_or(&empty);
            chunk.last = rows.len() < CHUNK_ROWS;
            for r in rows {
                let row = parse_text_row(r, &t)?;
                let kb = pk_bytes(&row, &t.pk);
                chunk.max_key = Some(kb.clone());
                chunk.max_key_text = Some(t.pk.iter().map(|c| r[*c].clone().unwrap_or_default()).collect());
                chunk.rows.insert(kb, row);
            }
        }
        q.simple_query(&format!("SELECT pg_logical_emit_message(true, 'ivm', {})", qlit(&high)))?;
        self.chunk = Some(chunk);
        *self.status.lock() = format!("snapshotting {}.{}", t.upstream.0, t.upstream.1);
        Ok(())
    }
}

fn parse_text_row(r: &[Option<String>], t: &SourceTable) -> Result<Row> {
    let mut row = Vec::with_capacity(t.columns.len());
    for (v, c) in r.iter().zip(&t.columns) {
        row.push(match v {
            None => Datum::Null,
            Some(s) => parse_datum(s, &c.ty.ty, -1)?,
        });
    }
    Ok(row)
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
    state: String,
    resume_lsn: u64,
    engine: EngineHandle,
    ctl: Receiver<SourceCtl>,
    status: Arc<Mutex<String>>,
) {
    let state: SourceState = serde_json::from_str(&state).unwrap_or_default();
    let mut src = PgSource {
        id,
        def,
        engine,
        tables: tables.into_iter().map(|t| (t.id, t)).collect(),
        rels: HashMap::new(),
        state,
        resume_lsn,
        durable_lsn: resume_lsn,
        txn: None,
        chunk: None,
        query: None,
        status: status.clone(),
        seq: 0,
        inflight: std::collections::VecDeque::new(),
        pending_wal_end: 0,
        unmapped_rels: HashMap::new(),
    };
    let mut backoff = Duration::from_millis(200);
    loop {
        if src.engine.shared.shutdown.load(Ordering::Acquire) {
            return;
        }
        match stream_once(&mut src, &ctl) {
            Ok(true) => return,
            Ok(false) => {}
            Err(e) => {
                log::warn!("source {name}: {e}");
                *status.lock() = format!("error: {e}");
                src.txn = None;
                src.chunk = None;
                src.query = None;
                src.rels.clear();
                src.unmapped_rels.clear();
            }
        }
        // stop requests during backoff
        match ctl.recv_timeout(backoff) {
            Ok(SourceCtl::Stop) | Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
            Ok(SourceCtl::AddTables(t)) => {
                // no session: relations are announced again by the next one
                for x in t {
                    src.tables.insert(x.id, x);
                }
            }
            Err(_) => {}
        }
        backoff = (backoff * 2).min(Duration::from_secs(10));
    }
}

/// One replication session. Ok(true) = stop requested.
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
        src.log_progress(src.durable_lsn)?;
    }
    let opts = [("proto_version", "1"), ("publication_names", src.def.publication.as_str()), ("messages", "true")];
    let pub_quoted = format!("\"{}\"", src.def.publication.replace('"', "\"\""));
    let opts2 = [("proto_version", "1"), ("publication_names", pub_quoted.as_str()), ("messages", "true")];
    let _ = opts;
    let mut stream: ReplicationStream = repl.start_logical_replication(&src.def.slot, Lsn(src.resume_lsn.max(src.durable_lsn)), &opts2)?;
    *src.status.lock() = "streaming".into();
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
        if src.txn.is_none() {
            src.maybe_start_chunk()?;
            if src.chunk.is_none() && !src.status.lock().starts_with("streaming") {
                *src.status.lock() = "streaming".into();
            }
        }
        let ev = stream.recv(Some(Duration::from_millis(100)))?;
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

#[allow(dead_code)]
fn _fmt(d: &Datum) -> String {
    format_datum(d)
}

#[cfg(test)]
mod tests {
    use super::*;

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
