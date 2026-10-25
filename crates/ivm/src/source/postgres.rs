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

/// Delay before reconnecting after a failed session: doubles from the
/// minimum up to the maximum while sessions keep failing.
const RECONNECT_MIN: Duration = Duration::from_millis(200);
const RECONNECT_MAX: Duration = Duration::from_secs(10);

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

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct SnapState {
    pub done: bool,
    /// Text values of the last snapshotted primary key.
    pub boundary: Option<Vec<String>>,
    /// The stored rows cannot be trusted (changes may have been lost, or
    /// were decoded with a key that no longer applies): the snapshot starts
    /// by truncating the table, so that rows deleted upstream go away too.
    #[serde(default)]
    pub truncate: bool,
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
    /// changes to a multiset table to apply after its rows (see push_multiset)
    pending: Vec<TableChange>,
    /// snapshot visibility (multiset tables): xmin, xmax, in-progress xids
    snap: Option<(u64, u64, Vec<u64>)>,
    max_key: Option<Vec<u8>>,
    max_key_text: Option<Vec<String>>,
    last: bool,
}

/// What the current replication session achieved (see `run`).
#[derive(Default)]
struct Session {
    /// When streaming started.
    streaming_since: Option<Instant>,
    /// An upstream transaction was processed.
    progressed: bool,
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
    /// Random id of this run of the source and the number of chunks it
    /// started: they make watermark tags unique (see `watermarks`).
    run_id: u64,
    seq: u64,
    /// Commits sent to the log whose durability is not yet confirmed.
    inflight: std::collections::VecDeque<(u64, Receiver<u64>)>,
    /// wal_end of a keepalive received while commits were in flight.
    pending_wal_end: u64,
    gauges: Arc<super::SourceGauges>,
    /// Relations announced in this session for tables the source does not
    /// replicate (yet). pgoutput announces a relation once per session, so
    /// a table added later (ALTER SOURCE REFRESH) is mapped from these.
    unmapped_rels: HashMap<u32, Relation>,
    session: Session,
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
                if let Some(o) = &old_row
                    && (old_is_key || !o.is_empty())
                {
                    let ok: Row = pk.iter().map(|c| o[*c].clone()).collect();
                    let nk: Row = pk.iter().map(|c| row[*c].clone()).collect();
                    if pk_bytes(o, &pk) != pk_bytes(&row, &pk) && !ok.iter().any(|d| d.is_null()) {
                        let kb = pk_bytes(o, &pk);
                        // deletes always apply (see the Delete case)
                        let _ = self.snapshot_filter(table, &kb);
                        self.push_change(table, TableChange::Delete { key: ok });
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
                    self.push_multiset(table, TableChange::Remove { row });
                    return Ok(());
                }
                // A delete always applies, whatever the snapshot filter says
                // (which still decides what happens to the chunk's row): of a
                // key not snapshotted yet it removes nothing, unless the table
                // is being re-snapshotted after a schema change. Then its old
                // row is stored, and no chunk would remove it: chunks only
                // upsert the rows that exist.
                let kb = pk_bytes(&row, &pk);
                let _ = self.snapshot_filter(table, &kb);
                let key: Row = pk.iter().map(|c| row[*c].clone()).collect();
                self.push_change(table, TableChange::Delete { key });
            }
            LogicalMessage::Truncate { relation_ids, .. } => {
                let xid = self.txn.as_ref().map(|t| t.xid);
                for r in relation_ids {
                    let Some(t) = self.rels.get(&r).map(|rm| rm.table) else { continue };
                    // a chunk read after the truncate already reflects it
                    let in_chunk = self.chunk.as_ref().is_some_and(|ch| {
                        ch.table == t
                            && ch.phase == Phase::BeforeLow
                            && matches!((&ch.snap, xid), (Some(snap), Some(x)) if xid_visible(snap, x))
                    });
                    if self.tables[&t].pk.is_empty() {
                        self.push_multiset(t, TableChange::Truncate);
                    } else {
                        self.push_change(t, TableChange::Truncate);
                    }
                    if !in_chunk
                        && let Some(ch) = &mut self.chunk
                        && ch.table == t
                    {
                        ch.rows.clear();
                        ch.multiset.clear();
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
                                // multiset chunk is emitted at the low watermark;
                                // it is the whole table, replacing what is stored
                                // (rows kept from before a schema change)
                                let rows = std::mem::take(&mut ch.multiset);
                                let pending = std::mem::take(&mut ch.pending);
                                let t = ch.table;
                                self.push_change(t, TableChange::Truncate);
                                for r in rows {
                                    self.push_change(t, TableChange::Insert { row: r });
                                }
                                for c in pending {
                                    self.push_change(t, c);
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
                self.session.progressed = true;
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
                    self.gauges.commit(txn.changes.len());
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

    /// A change to a keyless table (REPLICA IDENTITY FULL). Until its
    /// snapshot is done the table is replaced by its single chunk at the low
    /// watermark: a change the chunk's snapshot saw is dropped, as is one
    /// before any chunk (a later chunk reads it). A change it did not see
    /// applies; one decoded before the low watermark (its commit preceded the
    /// watermark but was not visible yet when the chunk was read) is applied
    /// after the chunk's rows.
    fn push_multiset(&mut self, table: ObjectId, change: TableChange) {
        if self.state.tables.get(&table).is_some_and(|s| s.done) {
            return self.push_change(table, change);
        }
        let xid = self.txn.as_ref().map(|t| t.xid);
        let Some(ch) = self.chunk.as_mut().filter(|ch| ch.table == table) else { return };
        let seen = matches!((&ch.snap, xid), (Some(s), Some(x)) if xid_visible(s, x));
        match ch.phase {
            Phase::InWindow if !seen => self.push_change(table, change),
            Phase::BeforeLow if !seen && ch.snap.is_some() && xid.is_some() => ch.pending.push(change),
            _ => {}
        }
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
            self.push_multiset(table, TableChange::Insert { row });
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
        }
        // Re-snapshot: to fill new columns of existing rows, and after a key
        // change from an empty table, as deletes by the old key were lost
        // (and those decoded above with it are dropped by reshape_changes).
        // The reset is logged before the schema change: a restart in
        // between then copies the table in its old shape and detects the
        // change again, while the other order would forget the re-snapshot.
        self.state.tables.insert(table, SnapState { truncate: !same_key && !pk.is_empty(), ..Default::default() });
        if self.chunk.as_ref().is_some_and(|c| c.table == table) {
            self.chunk = None;
        }
        self.log_progress(self.durable_lsn)?;
        self.engine.log(LogRecord::Schema { source: self.id, table, columns: columns.clone(), pk: pk.clone() })?;
        self.tables.insert(table, SourceTable { id: table, upstream: t.upstream.clone(), columns, pk });
        let cols: Vec<Option<usize>> = rel.columns.iter().enumerate().map(|(i, _)| Some(i)).collect();
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
        if self.state.tables.get(&table).is_some_and(|s| s.truncate) {
            // Start from an empty table, here between upstream transactions
            // and before the chunk's low watermark: from now on the table
            // fills exactly as in an initial snapshot.
            let lsn = self.inflight.back().map(|(l, _)| *l).unwrap_or(self.durable_lsn);
            let rx = self.engine.log_async(LogRecord::Commit { source: self.id, lsn, changes: vec![(table, TableChange::Truncate)] })?;
            self.inflight.push_back((lsn, rx));
            self.state.tables.entry(table).or_default().truncate = false;
            self.log_progress(self.durable_lsn)?;
        }
        let (low, high) = self.watermarks(table);
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
            pending: Vec::new(),
            snap: None,
            max_key: None,
            max_key_text: None,
            last: true,
        };
        if t.pk.is_empty() {
            let (snap_text, rows) = chunk_query(
                q,
                &format!("BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT pg_current_snapshot()::text; SELECT {cols} FROM {tname}; COMMIT"),
                &t,
                &mut Vec::new(),
            )?;
            chunk.snap = parse_snapshot(&snap_text.unwrap_or_default());
            chunk.multiset = rows;
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
            let mut last_pk_text = Vec::new();
            let (snap_text, rows) = chunk_query(
                q,
                &format!(
                    "BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT pg_current_snapshot()::text; \
                     SELECT {cols} FROM {tname}{where_clause} ORDER BY {} LIMIT {CHUNK_ROWS}; COMMIT",
                    pk_cols.join(", ")
                ),
                &t,
                &mut last_pk_text,
            )?;
            chunk.snap = parse_snapshot(&snap_text.unwrap_or_default());
            chunk.last = rows.len() < CHUNK_ROWS;
            if !rows.is_empty() {
                chunk.max_key_text = Some(last_pk_text);
            }
            for row in rows {
                let kb = pk_bytes(&row, &t.pk);
                chunk.max_key = Some(kb.clone());
                chunk.rows.insert(kb, row);
            }
        }
        q.simple_query(&format!("SELECT pg_logical_emit_message(true, 'ivm', {})", qlit(&high)))?;
        self.chunk = Some(chunk);
        *self.status.lock() = format!("snapshotting {}.{}", t.upstream.0, t.upstream.1);
        Ok(())
    }
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
            TableChange::Upsert { row, unchanged } => {
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

/// Run a snapshot chunk's statements: the text of pg_current_snapshot()
/// and the table's rows, parsed from the text values as they arrive (no
/// strings collected per value). `last_pk_text` receives the text of the
/// last row's primary key.
fn chunk_query(q: &mut Client, sql: &str, t: &SourceTable, last_pk_text: &mut Vec<String>) -> Result<(Option<String>, Vec<Row>)> {
    let mut snap = None;
    let mut rows = Vec::new();
    let mut err = None;
    q.simple_query_rows(sql, &mut |cols, r| {
        if snap.is_none() && cols.len() == 1 && cols[0].name == "pg_current_snapshot" {
            snap = r.get(0).map(|b| String::from_utf8_lossy(b).into_owned());
            return Ok(());
        }
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
    .map_err(|e| err.take().unwrap_or_else(|| e.into()))?;
    Ok((snap, rows))
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
    let gauges = super::gauges(&engine.shared, id);
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
        run_id: crate::source::random_u64(),
        seq: 0,
        inflight: std::collections::VecDeque::new(),
        pending_wal_end: 0,
        gauges,
        unmapped_rels: HashMap::new(),
        session: Session::default(),
    };
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
        for (id, t) in &self.tables {
            self.state.tables.insert(*id, SnapState { truncate: !t.pk.is_empty(), ..Default::default() });
        }
        self.chunk = None;
        if let Err(e) = self.log_progress(self.durable_lsn) {
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

#[allow(dead_code)]
fn _fmt(d: &Datum) -> String {
    format_datum(d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coord::api::Shared;
    use crate::coord::seq::SeqMsg;
    use pgproto::pgoutput::RelationColumn;

    /// An engine handle whose sequencer acknowledges every append at once
    /// and keeps the records.
    fn fake_engine() -> (EngineHandle, Arc<Mutex<Vec<LogRecord>>>) {
        let (etx, _) = crossbeam_channel::unbounded();
        let (stx, srx) = crossbeam_channel::unbounded::<SeqMsg>();
        let log = Arc::new(Mutex::new(Vec::new()));
        let l2 = log.clone();
        std::thread::spawn(move || {
            let mut gsn = 0;
            for m in srx {
                if let SeqMsg::Append { rec, ack } = m {
                    gsn += 1;
                    l2.lock().push(rec);
                    if let Some(a) = ack {
                        let _ = a.send(gsn);
                    }
                }
            }
        });
        (EngineHandle { tx: etx, seq: stx, shared: Arc::new(Shared::default()) }, log)
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
        let src = PgSource {
            id: 1,
            gauges: Arc::default(),
            def: SourceDef { conninfo: String::new(), publication: "p".into(), slot: "s".into(), tables: None },
            engine,
            tables: tables.into_iter().map(|t| (t.id, t)).collect(),
            rels: HashMap::new(),
            state,
            resume_lsn: 0,
            durable_lsn: 0,
            txn: None,
            chunk: None,
            query: None,
            status: Arc::new(Mutex::new(String::new())),
            run_id: crate::source::random_u64(),
            seq: 0,
            inflight: std::collections::VecDeque::new(),
            pending_wal_end: 0,
            unmapped_rels: HashMap::new(),
            session: Session::default(),
        };
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
                    TableChange::Upsert { row, .. } => {
                        keyed.insert(row[0].as_i64().unwrap(), row.clone());
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

    /// A chunk of table 10 between the stream's position and its low
    /// watermark, read with a snapshot in which xids below 100 are visible.
    fn chunk_before_low(rows: &[[i32; 2]], keyed: bool) -> Chunk {
        let rows: Vec<Row> = rows.iter().map(|r| vec![Datum::Int4(r[0]), Datum::Int4(r[1])]).collect();
        let mut ch = Chunk {
            table: 10,
            low: "L:1:10:1".into(),
            high: "H:1:10:1".into(),
            phase: Phase::BeforeLow,
            rows: BTreeMap::new(),
            multiset: Vec::new(),
            pending: Vec::new(),
            snap: Some((100, 100, vec![])),
            max_key: None,
            max_key_text: None,
            last: true,
        };
        if keyed {
            for r in rows {
                ch.max_key = Some(pk_bytes(&r, &[0]));
                ch.rows.insert(pk_bytes(&r, &[0]), r);
            }
        } else {
            ch.multiset = rows;
        }
        ch
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
            src.chunk = Some(chunk_before_low(&[[5, 50]], keyed));
            src.handle_message(begin(90)).unwrap();
            src.handle_message(LogicalMessage::Truncate { options: 0, relation_ids: vec![REL] }).unwrap();
            src.handle_message(commit(200)).unwrap();
            src.handle_message(begin(91)).unwrap();
            src.handle_message(insert(&[5, 50])).unwrap();
            src.handle_message(commit(210)).unwrap();
            for (xid, tag, end) in [(95, "L:1:10:1", 220), (96, "H:1:10:1", 230)] {
                src.handle_message(begin(xid)).unwrap();
                src.handle_message(message(tag)).unwrap();
                src.handle_message(commit(end)).unwrap();
            }
            src.drain_acks(true).unwrap();
            assert_eq!(replay(&log.lock()), vec![vec![Datum::Int4(5), Datum::Int4(50)]], "keyed: {keyed}");
            assert!(src.state.tables[&10].done);
        }
    }

    /// The same for a keyless table whose chunk was emitted at the low
    /// watermark: a truncate its snapshot saw must not wipe it afterwards.
    #[test]
    fn truncate_seen_by_a_keyless_chunk_in_its_window() {
        let (mut src, log) = source(vec![table(&["id", "a"], vec![])], SourceState { slot_created: true, ..Default::default() });
        src.handle_message(begin(80)).unwrap();
        src.handle_message(relation(&["id", "a"], 0)).unwrap();
        src.handle_message(commit(100)).unwrap();
        src.chunk = Some(chunk_before_low(&[[7, 70], [7, 70]], false));
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
        src.chunk = Some(chunk_before_low(&[[1, 10], [2, 20]], false));
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
        for (xid, tag, end) in [(151, "L:1:10:1", 220), (152, "H:1:10:1", 230)] {
            src.handle_message(begin(xid)).unwrap();
            src.handle_message(message(tag)).unwrap();
            src.handle_message(commit(end)).unwrap();
        }
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
            LogRecord::Progress { state, .. } => {
                let st: SourceState = serde_json::from_str(state).unwrap();
                assert!(!st.tables[&10].done);
                // the key did not change: existing rows are kept
                assert!(!st.tables[&10].truncate);
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
            [LogRecord::Progress { state, .. }, LogRecord::Schema { table: 10, columns, pk, .. }] => {
                let st: SourceState = serde_json::from_str(state).unwrap();
                assert!(!st.tables[&10].done && st.tables[&10].truncate);
                assert_eq!(pk, &[1]);
                assert!(!columns[1].ty.nullable, "key columns are not null");
            }
            other => panic!("unexpected records {other:?}"),
        }
        assert_eq!(src.tables[&10].pk, [1]);
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
        let mut ch = chunk_before_low(&[[1, 10]], true);
        (ch.low, ch.high) = (low, high);
        src.chunk = Some(ch);
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

    /// A text value that is not UTF-8 fails the change; it used to become ''.
    #[test]
    fn values_that_are_not_utf8_fail() {
        let mut t = table(&["id", "s"], vec![0]);
        t.columns[1] = Column::new("s", ColumnType::new(SqlType::Text));
        let (mut src, _) = source(vec![t], done_state(10));
        src.handle_message(begin(5)).unwrap();
        let LogicalMessage::Relation(mut rel) = relation(&["id", "s"], 1) else { unreachable!() };
        rel.columns[1].type_oid = 25;
        src.handle_message(LogicalMessage::Relation(rel)).unwrap();
        let text = |id: &str, s: &[u8]| LogicalMessage::Insert {
            relation_id: REL,
            new: vec![TupleValue::Text(id.as_bytes().to_vec()), TupleValue::Text(s.to_vec())],
        };
        src.handle_message(text("1", "café".as_bytes())).unwrap();
        // Latin-1 é
        let bad = text("2", &[b'c', b'a', b'f', 0xe9]);
        let err = src.handle_message(bad).unwrap_err();
        assert_eq!(err.code, codes::CHARACTER_NOT_IN_REPERTOIRE);
        assert!(err.message.contains("column \"s\" of public.t"), "{}", err.message);
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
