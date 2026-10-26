//! Introspection for the web UI.
//!
//! Every snapshot is built on the engine thread between two log records, so
//! it is internally consistent (one GSN). Building one is cheap: counts come
//! from tree summaries, arrangement contents are only ever sampled (a few
//! rows, a bounded number of entries examined), and the result is a plain
//! struct that the caller's thread serializes.

use super::*;
use crate::catalog::{CatalogObject, ObjectKind};
use crate::dataflow::mfp::MfpStep;
use crate::dataflow::{operator_arrangements, Arrangement, Node, NodeCounters, NodeKind};
use crate::sql::parser::{AsOf, ExplainKind, ExplainTarget, Statement};
use crate::sql_bail;
use crate::storage::{ReadAt, TreeResidency};
use crate::types::text::format_datum;
use crate::types::{ColumnType, Datum, Row};
use rustc_hash::FxHashSet;
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};

/// Most nodes listed by a dataflow snapshot.
pub const MAX_NODES: usize = 20_000;
/// Most rows in an arrangement or object sample.
pub const MAX_SAMPLE_ROWS: usize = 500;
/// Most arrangement entries examined to find a sample's live rows.
const MAX_SCAN: usize = 20_000;
/// Longest SQL text and sample cell returned.
const MAX_SQL: usize = 64 * 1024;
const MAX_CELL: usize = 2 * 1024;
/// Work budget (root x node visits) for attributing nodes to objects.
const USERS_BUDGET: usize = 5_000_000;

/// What to snapshot.
#[derive(Clone, Debug)]
pub enum InspectQuery {
    /// Small counters for periodic sampling.
    Stats,
    Overview,
    Sources,
    Catalog,
    History { limit: usize },
    Dataflow,
    Node { id: NodeId, sample: usize },
    Object { name: String, sample: usize },
    Sessions,
    Errors,
}

/// A snapshot request; `reply` runs on the engine thread and must not block.
pub struct InspectRequest {
    pub query: InspectQuery,
    pub reply: Box<dyn FnOnce(Result<InspectReply>) + Send>,
}

#[derive(Serialize, Debug)]
#[serde(untagged)]
pub enum InspectReply {
    Stats(EngineStats),
    Overview(Box<Overview>),
    Sources(SourcesInfo),
    Catalog(CatalogInfo),
    History(HistoryInfo),
    Dataflow(DataflowInfo),
    Node(Box<NodeDetail>),
    Object(Box<ObjectDetail>),
    Sessions(SessionsInfo),
    Errors(ErrorsInfo),
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct EngineStats {
    pub processed_gsn: Gsn,
    pub durable_gsn: Gsn,
    pub retention_frontier: Gsn,
    pub checkpoint_gsn: Gsn,
    pub records_since_checkpoint: u64,
    /// Wall clock (Unix ms) of the last checkpoint, or of the engine start.
    pub last_checkpoint_ms: f64,
    /// Logical clock (latest logged tick) as Unix ms.
    pub clock_ms: Option<f64>,
    pub cache_used_bytes: usize,
    /// The buffer pool's current share of the memory budget.
    pub cache_capacity_bytes: usize,
    pub dirty_bytes: usize,
    /// The other parts of the memory budget: group caches, records waiting
    /// to be applied, changes queued for subscriptions and sinks, the
    /// dataflow's transient working memory in use, the buffer pools of forks
    /// of the storage, changes forwarded to a hydration.
    pub group_cache_bytes: usize,
    pub input_queue_bytes: usize,
    pub output_queue_bytes: usize,
    pub work_bytes: usize,
    pub fork_pool_bytes: usize,
    pub forwarded_bytes: usize,
    pub nodes: usize,
    pub arrangements: usize,
    /// Monotonic: change whenever nodes or arrangements are created.
    pub nodes_created: usize,
    pub arrangements_created: usize,
    /// DDL statements applied (bumps on every catalog change).
    pub catalog_version: usize,
    /// Dataflow activity since start (monotonic).
    pub steps: NodeCounters,
    pub subscriptions: usize,
    pub parked_reads: usize,
    pub view_errors: i64,
}

#[derive(Serialize, Debug)]
pub struct Overview {
    pub version: &'static str,
    pub stats: EngineStats,
    pub config: ConfigInfo,
    pub storage: StorageInfo,
    pub counts: Counts,
    pub sources: Vec<StatusBrief>,
    pub sinks: Vec<StatusBrief>,
}

#[derive(Serialize, Debug)]
pub struct ConfigInfo {
    pub data_dir: String,
    pub memory_bytes: usize,
    pub checkpoint_interval_secs: f64,
    pub retention_gsns: u64,
    pub sync: bool,
    pub tick_ms: Option<u64>,
}

#[derive(Serialize, Debug)]
pub struct StorageInfo {
    pub trees: usize,
    pub loaded_nodes: usize,
    pub dirty_nodes: usize,
    pub segments: usize,
    pub segment_bytes: u64,
    pub live_bytes: u64,
}

#[derive(Serialize, Debug, Default)]
pub struct Counts {
    pub tables: usize,
    pub views: usize,
    pub materialized_views: usize,
    pub indexes: usize,
    pub sources: usize,
    pub sinks: usize,
    pub nodes: usize,
    pub arrangements: usize,
    pub records: i64,
}

#[derive(Serialize, Debug)]
pub struct StatusBrief {
    pub id: ObjectId,
    pub name: String,
    pub status: String,
}

#[derive(Serialize, Debug)]
pub struct SourcesInfo {
    pub gsn: Gsn,
    pub sources: Vec<SourceInfo>,
}

#[derive(Serialize, Debug)]
pub struct SourceInfo {
    pub id: ObjectId,
    pub name: String,
    pub publication: String,
    pub slot: String,
    /// Connection string with passwords masked.
    pub conninfo: String,
    pub status: String,
    pub created_gsn: Gsn,
    /// Every upstream commit at or below this LSN is in the input log.
    pub frontier_lsn: u64,
    pub frontier: String,
    /// Last logged commit.
    pub last_commit_lsn: u64,
    pub last_commit: String,
    /// WAL position the upstream walsender reported last.
    pub upstream_lsn: u64,
    pub upstream: String,
    /// upstream - frontier, when both are known.
    pub lag_bytes: Option<u64>,
    /// When the last replication message arrived (Unix ms).
    pub last_message_ms: Option<f64>,
    /// Upstream clock in that message (Unix ms).
    pub server_time_ms: Option<f64>,
    pub commits: u64,
    pub changes: u64,
    pub slot_created: bool,
    pub tables: Vec<SourceTableInfo>,
    pub tables_done: usize,
}

#[derive(Serialize, Debug)]
pub struct SourceTableInfo {
    pub id: ObjectId,
    pub name: String,
    pub upstream: String,
    pub pk: Vec<String>,
    /// "done", "loading" (the chunk in flight is from this table), "error"
    /// (its chunk cannot be read; retried) or "pending".
    pub snapshot: &'static str,
    /// Why the table cannot be copied (snapshot "error").
    pub error: Option<String>,
    /// Primary key of the last snapshotted row (text values).
    pub boundary: Option<Vec<String>>,
    pub rows: i64,
}

#[derive(Serialize, Debug)]
pub struct CatalogInfo {
    pub gsn: Gsn,
    pub version: usize,
    pub objects: Vec<ObjectInfo>,
}

#[derive(Serialize, Debug, Clone)]
pub struct ObjectInfo {
    pub id: ObjectId,
    pub name: String,
    pub kind: &'static str,
    pub created_gsn: Gsn,
    pub sql: String,
    pub columns: Vec<ColumnInfo>,
    pub depends_on: Vec<ObjectId>,
    pub dependents: Vec<ObjectId>,
    pub rows: Option<i64>,
    /// Distinct error records currently held by the view.
    pub errors: Option<usize>,
    pub node: Option<NodeId>,
    pub arrangement: Option<ArrId>,
    pub status: Option<String>,
    /// One-line description (source table, index key, sink target, ...).
    pub detail: Option<String>,
}

#[derive(Serialize, Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub nullable: bool,
    pub key: bool,
}

#[derive(Serialize, Debug)]
pub struct HistoryInfo {
    pub gsn: Gsn,
    pub total: usize,
    /// Newest first.
    pub entries: Vec<HistoryEntry>,
}

#[derive(Serialize, Debug)]
pub struct HistoryEntry {
    pub gsn: Gsn,
    pub action: String,
    pub sql: String,
}

#[derive(Serialize, Debug)]
pub struct DataflowInfo {
    pub gsn: Gsn,
    pub nodes: Vec<NodeInfo>,
    pub arrangements: Vec<ArrInfo>,
    /// [from, to] for every operator input.
    pub edges: Vec<[NodeId; 2]>,
    /// Everything that holds nodes alive: tables, materialized views,
    /// indexes, sinks, subscriptions. `NodeInfo::users` index into it.
    pub objects: Vec<DataflowObject>,
    pub totals: NodeCounters,
    pub truncated: bool,
}

#[derive(Serialize, Debug, Clone)]
pub struct NodeInfo {
    pub id: NodeId,
    pub kind: &'static str,
    /// Name of the table, or of the views this node is the root of.
    pub label: Option<String>,
    /// Operator parameters in one line.
    pub summary: String,
    pub inputs: Vec<NodeId>,
    pub schema: Vec<String>,
    pub refs: usize,
    pub hydrated: bool,
    /// Arrangements owned by the node (indexes of its output, operator state).
    pub arrangements: Vec<ArrId>,
    /// Arrangements the operator reads.
    pub reads: Vec<ArrId>,
    /// Records in the owned arrangements.
    pub records: i64,
    pub users: Vec<usize>,
    pub counters: NodeCounters,
}

#[derive(Serialize, Debug, Clone)]
pub struct ArrInfo {
    pub id: ArrId,
    pub node: NodeId,
    /// "index" (of the node's output) or "state" (operator-private).
    pub role: &'static str,
    pub key: Vec<String>,
    pub summaries: Vec<String>,
    pub refs: usize,
    /// Weighted entry count (rows, counting multiplicity).
    pub records: i64,
    pub resident_bytes: usize,
    pub resident_nodes: usize,
    pub hydrated: bool,
    pub readers: Vec<NodeId>,
}

#[derive(Serialize, Debug, Clone)]
pub struct DataflowObject {
    pub id: Option<ObjectId>,
    pub name: String,
    pub kind: &'static str,
    pub node: NodeId,
    pub arrangement: Option<ArrId>,
}

#[derive(Serialize, Debug)]
pub struct NodeDetail {
    pub gsn: Gsn,
    pub node: NodeInfo,
    pub details: Vec<Detail>,
    pub consumers: Vec<NodeId>,
    /// Output column names when the node is a table or view root.
    pub columns: Vec<String>,
    pub arrangements: Vec<ArrDetail>,
    pub users: Vec<DataflowObject>,
}

#[derive(Serialize, Debug)]
pub struct Detail {
    pub label: String,
    pub value: String,
}

#[derive(Serialize, Debug)]
pub struct ArrDetail {
    #[serde(flatten)]
    pub info: ArrInfo,
    pub columns: Vec<ColumnHeader>,
    pub sample: Sample,
}

#[derive(Serialize, Debug, Clone)]
pub struct ColumnHeader {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
}

#[derive(Serialize, Debug, Default)]
pub struct Sample {
    pub rows: Vec<SampleRow>,
    /// More live rows exist than were returned.
    pub truncated: bool,
    /// Entries examined (including ones with zero weight now).
    pub scanned: usize,
}

#[derive(Serialize, Debug)]
pub struct SampleRow {
    pub values: Vec<Option<String>>,
    pub weight: i64,
}

#[derive(Serialize, Debug)]
pub struct ObjectDetail {
    pub gsn: Gsn,
    pub object: ObjectInfo,
    /// EXPLAIN (logical plan) of a view.
    pub plan: Option<Vec<String>>,
    /// EXPLAIN DATAFLOW of a materialized view.
    pub dataflow: Option<Vec<String>>,
    /// EXPLAIN MAINTENANCE of a materialized view.
    pub maintenance: Option<Vec<MaintenanceRow>>,
    pub errors: Vec<ErrorRecord>,
    pub sample: Option<TableSample>,
    pub source: Option<SourceInfo>,
    /// Dataflow nodes the object depends on.
    pub nodes: Vec<NodeId>,
    /// Parts of the report that could not be produced.
    pub problems: Vec<String>,
}

#[derive(Serialize, Debug)]
pub struct MaintenanceRow {
    pub source: String,
    pub delta_rows_per_tick: String,
    pub work_per_delta: String,
    pub path: String,
}

#[derive(Serialize, Debug)]
pub struct ErrorRecord {
    pub code: String,
    pub message: String,
    pub count: i64,
}

#[derive(Serialize, Debug)]
pub struct TableSample {
    pub columns: Vec<ColumnHeader>,
    #[serde(flatten)]
    pub sample: Sample,
}

#[derive(Serialize, Debug)]
pub struct SessionsInfo {
    pub gsn: Gsn,
    pub subscriptions: Vec<SubscriptionInfo>,
    pub parked: Vec<ParkedInfo>,
    /// Requests waiting for their log record to be applied.
    pub pending_requests: usize,
    pub dml_queued: usize,
    pub dml_inflight: bool,
    pub sinks: Vec<SinkInfo>,
}

#[derive(Serialize, Debug)]
pub struct SubscriptionInfo {
    pub node: NodeId,
    /// Table or view subscribed to (None: an ad hoc query).
    pub target: Option<String>,
    pub progress: bool,
    pub cancelled: bool,
}

#[derive(Serialize, Debug)]
pub struct ParkedInfo {
    /// "gsn", "lsn" or "snapshot".
    pub wait: &'static str,
    pub gsn: Option<Gsn>,
    pub sql: String,
}

#[derive(Serialize, Debug)]
pub struct SinkInfo {
    pub id: ObjectId,
    pub name: String,
    pub from: String,
    pub table: String,
    pub status: String,
    pub committed_gsn: Option<Gsn>,
    pub lag_gsns: Option<u64>,
}

#[derive(Serialize, Debug)]
pub struct ErrorsInfo {
    pub gsn: Gsn,
    pub views: Vec<ViewError>,
    pub sources: Vec<StatusBrief>,
    pub sinks: Vec<StatusBrief>,
    /// Materialized views and indexes that are broken (not maintained).
    pub broken: Vec<StatusBrief>,
}

#[derive(Serialize, Debug)]
pub struct ViewError {
    pub id: ObjectId,
    pub name: String,
    pub code: String,
    pub message: String,
    pub count: i64,
}

impl EngineHandle {
    /// Ask the engine for a snapshot. `reply` runs on the engine thread (it
    /// should only hand the result over); if the engine stops first it is
    /// dropped without being called.
    pub fn inspect(&self, query: InspectQuery, reply: impl FnOnce(Result<InspectReply>) + Send + 'static) -> Result<()> {
        self.tx
            .send(EngineMsg::Request(Request::Inspect(InspectRequest { query, reply: Box::new(reply) })))
            .map_err(|_| SqlError::new(codes::ADMIN_SHUTDOWN, "engine is shutting down"))
    }

    /// Blocking variant of [`EngineHandle::inspect`].
    pub fn inspect_wait(&self, query: InspectQuery, timeout: Duration) -> Result<InspectReply> {
        let (tx, rx) = bounded(1);
        self.inspect(query, move |r| {
            let _ = tx.send(r);
        })?;
        match rx.recv_timeout(timeout) {
            Ok(r) => r,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                Err(SqlError::new(codes::QUERY_CANCELED, "the engine did not answer in time"))
            }
            Err(_) => Err(SqlError::new(codes::ADMIN_SHUTDOWN, "engine is shutting down")),
        }
    }
}

fn unix_ms(t: SystemTime) -> f64 {
    t.duration_since(UNIX_EPOCH).map(|d| d.as_secs_f64() * 1000.0).unwrap_or(0.0)
}

/// PostgreSQL's textual LSN.
pub fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)
}

/// PostgreSQL timestamps (microseconds since 2000-01-01) as Unix ms.
fn pg_micros_to_unix_ms(us: i64) -> f64 {
    (us + 946_684_800_000_000) as f64 / 1000.0
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

fn type_name(t: &ColumnType) -> String {
    crate::types::format_type(&t.ty, t.typmod)
}

fn cell(d: &Datum) -> Option<String> {
    if d.is_null() { None } else { Some(truncate(&format_datum(d), MAX_CELL)) }
}

fn join_display<T: std::fmt::Display>(items: &[T], sep: &str) -> String {
    items.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(sep)
}

fn cols(c: &[usize]) -> String {
    format!("({})", c.iter().map(|i| format!("#{i}")).collect::<Vec<_>>().join(", "))
}

/// Mask passwords in a libpq connection string (key/value or URI form).
pub fn redact_conninfo(s: &str) -> String {
    let t = s.trim_start();
    if t.starts_with("postgres://") || t.starts_with("postgresql://") {
        let scheme_end = s.find("://").map(|i| i + 3).unwrap_or(0);
        let rest = &s[scheme_end..];
        let auth_end = rest.find(['/', '?']).unwrap_or(rest.len());
        let mut out = s[..scheme_end].to_string();
        match rest[..auth_end].rfind('@') {
            Some(at) => {
                let userinfo = &rest[..at];
                match userinfo.find(':') {
                    Some(c) => {
                        out.push_str(&userinfo[..c]);
                        out.push_str(":***");
                    }
                    None => out.push_str(userinfo),
                }
                out.push_str(&rest[at..]);
            }
            None => out.push_str(rest),
        }
        // password=... in the query string
        if let Some(q) = out.find('?') {
            let (head, query) = out.split_at(q + 1);
            let query = query
                .split('&')
                .map(|kv| match kv.split_once('=') {
                    Some((k, _)) if k.to_ascii_lowercase().ends_with("password") => format!("{k}=***"),
                    _ => kv.to_string(),
                })
                .collect::<Vec<_>>()
                .join("&");
            return format!("{head}{query}");
        }
        return out;
    }
    // key = value pairs; values may be single-quoted with backslash escapes
    let b = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < b.len() {
        while i < b.len() && b[i].is_ascii_whitespace() {
            out.push(b[i] as char);
            i += 1;
        }
        let ks = i;
        while i < b.len() && b[i] != b'=' && !b[i].is_ascii_whitespace() {
            i += 1;
        }
        let key = &s[ks..i];
        out.push_str(key);
        while i < b.len() && b[i].is_ascii_whitespace() {
            out.push(b[i] as char);
            i += 1;
        }
        if i < b.len() && b[i] == b'=' {
            out.push('=');
            i += 1;
            while i < b.len() && b[i].is_ascii_whitespace() {
                out.push(b[i] as char);
                i += 1;
            }
            let vs = i;
            if i < b.len() && b[i] == b'\'' {
                i += 1;
                while i < b.len() && b[i] != b'\'' {
                    if b[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
                i = (i + 1).min(b.len());
            } else {
                while i < b.len() && !b[i].is_ascii_whitespace() {
                    i += 1;
                }
            }
            if key.to_ascii_lowercase().ends_with("password") {
                out.push_str("***");
            } else {
                out.push_str(&s[vs..i]);
            }
        }
    }
    out
}

/// Committed GSN from a sink status such as "running (gsn 42)".
fn sink_committed(status: &str) -> Option<Gsn> {
    let i = status.find("(gsn ")?;
    status[i + 5..].split(')').next()?.trim().parse().ok()
}

fn statement_action(sql: &str) -> String {
    let words: Vec<&str> = sql.split_whitespace().take(4).collect();
    let mut out = Vec::new();
    for w in words {
        let u = w.to_ascii_uppercase();
        let keyword = matches!(
            u.as_str(),
            "CREATE" | "DROP" | "ALTER" | "OR" | "REPLACE" | "MATERIALIZED" | "VIEW" | "TABLE" | "INDEX" | "SOURCE" | "SINK" | "UNIQUE"
        );
        if !keyword {
            break;
        }
        out.push(u);
    }
    if out.is_empty() { sql.split_whitespace().next().unwrap_or("").to_ascii_uppercase() } else { out.join(" ") }
}

impl Engine {
    pub(crate) fn inspect(&mut self, req: InspectRequest) {
        let r = match req.query {
            InspectQuery::Stats => Ok(InspectReply::Stats(self.inspect_stats())),
            InspectQuery::Overview => Ok(InspectReply::Overview(Box::new(self.inspect_overview()))),
            InspectQuery::Sources => {
                let ids: Vec<ObjectId> =
                    self.catalog.objects.values().filter(|o| matches!(o.kind, ObjectKind::Source(_))).map(|o| o.id).collect();
                let sources = ids.into_iter().filter_map(|id| self.source_info(id)).collect();
                Ok(InspectReply::Sources(SourcesInfo { gsn: self.processed, sources }))
            }
            InspectQuery::Catalog => Ok(InspectReply::Catalog(self.inspect_catalog())),
            InspectQuery::History { limit } => {
                let h = &self.catalog.history;
                let entries = h
                    .iter()
                    .rev()
                    .take(limit)
                    .map(|(g, sql)| HistoryEntry { gsn: *g, action: statement_action(sql), sql: truncate(sql, MAX_SQL) })
                    .collect();
                Ok(InspectReply::History(HistoryInfo { gsn: self.processed, total: h.len(), entries }))
            }
            InspectQuery::Dataflow => Ok(InspectReply::Dataflow(self.inspect_dataflow())),
            InspectQuery::Node { id, sample } => self.inspect_node(id, sample.min(MAX_SAMPLE_ROWS)).map(|d| InspectReply::Node(Box::new(d))),
            InspectQuery::Object { name, sample } => {
                self.inspect_object(&name, sample.min(MAX_SAMPLE_ROWS)).map(|d| InspectReply::Object(Box::new(d)))
            }
            InspectQuery::Sessions => Ok(InspectReply::Sessions(self.inspect_sessions())),
            InspectQuery::Errors => Ok(InspectReply::Errors(self.inspect_errors())),
        };
        (req.reply)(r);
    }

    fn inspect_stats(&self) -> EngineStats {
        let since = self.last_checkpoint.elapsed();
        EngineStats {
            processed_gsn: self.processed,
            durable_gsn: self.shared.durable(),
            retention_frontier: self.env.frontier(),
            checkpoint_gsn: self.env.checkpoint_gsn(),
            records_since_checkpoint: self.records_since_checkpoint,
            last_checkpoint_ms: unix_ms(SystemTime::now()) - since.as_secs_f64() * 1000.0,
            clock_ms: (self.clock != 0).then(|| pg_micros_to_unix_ms(self.clock)),
            cache_used_bytes: self.env.resident_bytes(),
            cache_capacity_bytes: self.env.cache_capacity(),
            dirty_bytes: self.env.dirty_bytes(),
            group_cache_bytes: self.graph.group_caches.used(),
            input_queue_bytes: self.shared.mem.input.used(),
            output_queue_bytes: self.shared.mem.outputs.used(),
            work_bytes: self.shared.mem.work.used(),
            fork_pool_bytes: self.shared.mem.forks.used(),
            forwarded_bytes: self.shared.mem.forwarded.used(),
            nodes: self.graph.live_nodes().count(),
            arrangements: self.graph.live_arrangements().count(),
            nodes_created: self.graph.node_count(),
            arrangements_created: self.graph.arr_count(),
            catalog_version: self.catalog.history.len(),
            steps: self.graph.step_totals,
            subscriptions: self.subs.len(),
            parked_reads: self.parked.len(),
            view_errors: self.rt.views.values().flat_map(|v| v.errors.values()).filter(|w| **w > 0).sum(),
        }
    }

    fn inspect_overview(&self) -> Overview {
        let st = self.env.stats();
        let mut counts = Counts::default();
        for o in self.catalog.objects.values() {
            match o.kind {
                ObjectKind::Table(_) => counts.tables += 1,
                ObjectKind::View(_) => counts.views += 1,
                ObjectKind::MaterializedView(_) => counts.materialized_views += 1,
                ObjectKind::Index(_) => counts.indexes += 1,
                ObjectKind::Source(_) => counts.sources += 1,
                ObjectKind::Sink(_) => counts.sinks += 1,
            }
        }
        counts.nodes = self.graph.live_nodes().count();
        counts.arrangements = self.graph.live_arrangements().count();
        counts.records = self.graph.live_arrangements().map(|a| self.env.tree_count(a.tree)).sum();
        let brief = |o: &CatalogObject, status: String| StatusBrief { id: o.id, name: o.name.clone(), status };
        Overview {
            version: env!("CARGO_PKG_VERSION"),
            stats: self.inspect_stats(),
            config: ConfigInfo {
                data_dir: self.cfg.data_dir.display().to_string(),
                memory_bytes: self.cfg.memory_bytes,
                checkpoint_interval_secs: self.cfg.checkpoint_interval.as_secs_f64(),
                retention_gsns: self.cfg.retention_gsns,
                sync: self.cfg.sync,
                tick_ms: self.cfg.tick.map(|d| d.as_millis() as u64),
            },
            storage: StorageInfo {
                trees: st.trees,
                loaded_nodes: st.loaded_nodes,
                dirty_nodes: st.dirty_nodes,
                segments: st.segments.len(),
                segment_bytes: st.segments.iter().map(|s| s.1).sum(),
                live_bytes: st.segments.iter().map(|s| s.2).sum(),
            },
            counts,
            sources: self
                .catalog
                .objects
                .values()
                .filter(|o| matches!(o.kind, ObjectKind::Source(_)))
                .map(|o| brief(o, self.source_status(o.id)))
                .collect(),
            sinks: self
                .catalog
                .objects
                .values()
                .filter(|o| matches!(o.kind, ObjectKind::Sink(_)))
                .map(|o| brief(o, self.sink_status(o.id)))
                .collect(),
        }
    }

    fn source_status(&self, id: ObjectId) -> String {
        self.source_handles.get(&id).map(|h| h.status()).unwrap_or_else(|| "stopped".into())
    }

    /// Status of a broken object (in the form of sources' and sinks' error
    /// statuses).
    fn broken_status(&self, id: ObjectId) -> Option<String> {
        self.rt.broken.get(&id).map(|reason| format!("error: broken: {reason}"))
    }

    pub(crate) fn sink_status(&self, id: ObjectId) -> String {
        match self.rt.broken.get(&id) {
            Some(reason) => format!("error: broken: {reason}"),
            None => match self.sink_handles.get(&id) {
                Some(h) => h.status(),
                None => "stopped".into(),
            },
        }
    }

    /// Rows of the arrangement backing a table, materialized view or index.
    fn object_rows(&self, id: ObjectId) -> Option<i64> {
        let arr = self.object_arrangement(id)?;
        let a = self.graph.arrs.get(arr)?.as_ref()?;
        Some(self.env.tree_count(a.tree))
    }

    fn object_arrangement(&self, id: ObjectId) -> Option<ArrId> {
        if let Some(n) = self.rt.tables.get(&id) {
            return self.graph.nodes.get(*n)?.as_ref()?.arrangements.first().copied();
        }
        if let Some(v) = self.rt.views.get(&id) {
            return Some(v.arr);
        }
        self.rt.indexes.get(&id).copied()
    }

    fn object_node(&self, id: ObjectId) -> Option<NodeId> {
        if let Some(n) = self.rt.tables.get(&id) {
            return Some(*n);
        }
        if let Some(v) = self.rt.views.get(&id) {
            return Some(v.node);
        }
        if let Some(a) = self.rt.indexes.get(&id) {
            return self.graph.arrs.get(*a)?.as_ref().map(|a| a.node);
        }
        self.sink_handles.get(&id).map(|s| s.node)
    }

    fn source_info(&mut self, id: ObjectId) -> Option<SourceInfo> {
        let o = self.catalog.get(id)?.clone();
        let ObjectKind::Source(def) = &o.kind else { return None };
        // drop gauges of sources that no longer exist
        self.shared.source_gauges.lock().retain(|k, _| self.catalog.objects.contains_key(k));
        let state: crate::source::postgres::SourceState =
            self.source_states.get(&id).and_then(|s| serde_json::from_str(s).ok()).unwrap_or_default();
        let frontier = self.shared.source_lsn.lock().get(&id).copied().unwrap_or(0);
        let last_commit = self.source_lsns.get(&id).copied().unwrap_or(0);
        let gauges = self.shared.source_gauges.lock().get(&id).cloned();
        let load = |a: &std::sync::atomic::AtomicU64| a.load(Ordering::Relaxed);
        let (upstream, commits, changes, received, server) = match &gauges {
            Some(g) => (
                load(&g.wal_end),
                load(&g.commits),
                load(&g.changes),
                g.received_at.load(Ordering::Relaxed),
                g.server_time.load(Ordering::Relaxed),
            ),
            None => (0, 0, 0, 0, 0),
        };
        let table_errors = gauges.as_ref().map(|g| g.table_errors.lock().clone()).unwrap_or_default();
        let mut tables: Vec<SourceTableInfo> = self
            .source_tables(id)
            .into_iter()
            .map(|t| {
                let st = state.tables.get(&t.id).cloned().unwrap_or_default();
                let error = if st.done { None } else { table_errors.get(&t.id).cloned() };
                SourceTableInfo {
                    id: t.id,
                    name: self.catalog.get(t.id).map(|o| o.name.clone()).unwrap_or_default(),
                    upstream: format!("{}.{}", t.upstream.0, t.upstream.1),
                    pk: t.pk.iter().filter_map(|c| t.columns.get(*c)).map(|c| c.name.clone()).collect(),
                    snapshot: if st.done {
                        "done"
                    } else if error.is_some() {
                        "error"
                    } else {
                        "pending"
                    },
                    error,
                    boundary: st.boundary.clone(),
                    rows: self.object_rows(t.id).unwrap_or(0),
                }
            })
            .collect();
        tables.sort_by_key(|t| t.id);
        // the source snapshots its unfinished tables in id order, one chunk
        // at a time (skipping those that cannot be read)
        if let Some(t) = tables.iter_mut().find(|t| t.snapshot == "pending") {
            t.snapshot = "loading";
        }
        let tables_done = tables.iter().filter(|t| t.snapshot == "done").count();
        Some(SourceInfo {
            id,
            name: o.name.clone(),
            publication: def.publication.clone(),
            slot: def.slot.clone(),
            conninfo: redact_conninfo(&def.conninfo),
            status: self.source_status(id),
            created_gsn: o.created_gsn,
            frontier_lsn: frontier,
            frontier: format_lsn(frontier),
            last_commit_lsn: last_commit,
            last_commit: format_lsn(last_commit),
            upstream_lsn: upstream,
            upstream: format_lsn(upstream),
            lag_bytes: (upstream > 0 && frontier > 0).then(|| upstream.saturating_sub(frontier)),
            last_message_ms: (received > 0).then(|| received as f64 / 1000.0),
            server_time_ms: (server != 0).then(|| pg_micros_to_unix_ms(server)),
            commits,
            changes,
            slot_created: state.slot_created,
            tables,
            tables_done,
        })
    }

    /// Dependencies by the rule DROP uses: a view depends on the relations
    /// its query names, as the binder resolves them (relations read through
    /// a virtual view are that view's dependencies, so the graph reaches
    /// them through it); a definition that no longer binds depends on the
    /// relations whose names its text mentions.
    fn dependency_map(&self) -> HashMap<ObjectId, Vec<ObjectId>> {
        let mut deps: HashMap<ObjectId, Vec<ObjectId>> = HashMap::new();
        for o in self.catalog.objects.values() {
            let mut d: Vec<ObjectId> = match &o.kind {
                ObjectKind::Index(i) => vec![i.on],
                ObjectKind::Sink(s) => vec![s.from],
                ObjectKind::Table(t) => t.source.into_iter().collect(),
                ObjectKind::MaterializedView(v) | ObjectKind::View(v) => {
                    let refs = self.catalog.direct_references(&v.sql).or_else(|| self.catalog.references(&v.sql));
                    match refs {
                        Some(refs) => refs.into_iter().filter(|id| *id != o.id).collect(),
                        None => self
                            .catalog
                            .objects
                            .values()
                            .filter(|x| {
                                x.id != o.id
                                    && matches!(x.kind, ObjectKind::Table(_) | ObjectKind::View(_) | ObjectKind::MaterializedView(_))
                                    && crate::catalog::sql_mentions(&v.sql, &x.name)
                            })
                            .map(|x| x.id)
                            .collect(),
                    }
                }
                ObjectKind::Source(_) => vec![],
            };
            d.sort_unstable();
            d.dedup();
            deps.insert(o.id, d);
        }
        deps
    }

    fn object_info(&self, o: &CatalogObject, deps: &HashMap<ObjectId, Vec<ObjectId>>, rdeps: &HashMap<ObjectId, Vec<ObjectId>>) -> ObjectInfo {
        let col_info = |c: &[crate::types::Column], pk: &[usize]| -> Vec<ColumnInfo> {
            c.iter()
                .enumerate()
                .map(|(i, c)| ColumnInfo { name: c.name.clone(), ty: type_name(&c.ty), nullable: c.ty.nullable, key: pk.contains(&i) })
                .collect()
        };
        let name_of = |id: ObjectId| self.catalog.get(id).map(|o| o.name.clone()).unwrap_or_else(|| format!("#{id}"));
        let (columns, status, detail) = match &o.kind {
            ObjectKind::Table(t) => (
                col_info(&t.columns, &t.pk),
                None,
                Some(match (&t.source, &t.upstream) {
                    (Some(s), Some((us, un))) => format!("replicated from {} ({us}.{un})", name_of(*s)),
                    (Some(s), None) => format!("replicated from {}", name_of(*s)),
                    _ => "local table".to_string(),
                }),
            ),
            ObjectKind::MaterializedView(v) | ObjectKind::View(v) => (col_info(&v.columns, &[]), self.broken_status(o.id), None),
            ObjectKind::Index(i) => {
                let target_cols: Vec<crate::types::Column> = match self.catalog.get(i.on).map(|x| &x.kind) {
                    Some(ObjectKind::Table(t)) => t.columns.clone(),
                    Some(ObjectKind::MaterializedView(v)) => v.columns.clone(),
                    _ => vec![],
                };
                let key: Vec<String> =
                    i.columns.iter().map(|c| target_cols.get(*c).map(|c| c.name.clone()).unwrap_or_else(|| format!("#{c}"))).collect();
                (vec![], self.broken_status(o.id), Some(format!("on {} ({})", name_of(i.on), key.join(", "))))
            }
            ObjectKind::Source(s) => (
                vec![],
                Some(self.source_status(o.id)),
                Some(format!("publication {}, slot {}", s.publication, s.slot)),
            ),
            ObjectKind::Sink(s) => (
                vec![],
                Some(self.sink_status(o.id)),
                Some(format!("{} into table {}", name_of(s.from), s.table)),
            ),
        };
        ObjectInfo {
            id: o.id,
            name: o.name.clone(),
            kind: o.kind.name(),
            created_gsn: o.created_gsn,
            sql: truncate(&o.create_sql, MAX_SQL),
            columns,
            depends_on: deps.get(&o.id).cloned().unwrap_or_default(),
            dependents: rdeps.get(&o.id).cloned().unwrap_or_default(),
            rows: self.object_rows(o.id),
            errors: self.rt.views.get(&o.id).map(|v| v.errors.values().filter(|w| **w > 0).count()),
            node: self.object_node(o.id),
            arrangement: self.object_arrangement(o.id),
            status,
            detail,
        }
    }

    fn reverse_deps(deps: &HashMap<ObjectId, Vec<ObjectId>>) -> HashMap<ObjectId, Vec<ObjectId>> {
        let mut r: HashMap<ObjectId, Vec<ObjectId>> = HashMap::new();
        for (o, ds) in deps {
            for d in ds {
                r.entry(*d).or_default().push(*o);
            }
        }
        for v in r.values_mut() {
            v.sort_unstable();
        }
        r
    }

    fn inspect_catalog(&self) -> CatalogInfo {
        let deps = self.dependency_map();
        let rdeps = Self::reverse_deps(&deps);
        let objects = self.catalog.objects.values().map(|o| self.object_info(o, &deps, &rdeps)).collect();
        CatalogInfo { gsn: self.processed, version: self.catalog.history.len(), objects }
    }

    /// Everything that pins dataflow nodes.
    fn dataflow_objects(&self) -> Vec<DataflowObject> {
        let mut out = Vec::new();
        for o in self.catalog.objects.values() {
            let (kind, node, arrangement) = match &o.kind {
                ObjectKind::Table(_) => match self.rt.tables.get(&o.id) {
                    Some(n) => ("table", *n, self.object_arrangement(o.id)),
                    None => continue,
                },
                ObjectKind::MaterializedView(_) => match self.rt.views.get(&o.id) {
                    Some(v) => ("materialized view", v.node, Some(v.arr)),
                    None => continue,
                },
                ObjectKind::Index(_) => match self.rt.indexes.get(&o.id).and_then(|a| self.graph.arrs.get(*a)?.as_ref()) {
                    Some(a) => ("index", a.node, Some(a.id)),
                    None => continue,
                },
                ObjectKind::Sink(_) => match self.sink_handles.get(&o.id) {
                    Some(s) => ("sink", s.node, None),
                    None => continue,
                },
                _ => continue,
            };
            out.push(DataflowObject { id: Some(o.id), name: o.name.clone(), kind, node, arrangement });
        }
        for (i, s) in self.subs.iter().enumerate() {
            if s.cancel.load(Ordering::Acquire) {
                continue;
            }
            let target = self.node_target(s.node);
            out.push(DataflowObject {
                id: None,
                name: match target {
                    Some(t) => format!("subscription {} ({t})", i + 1),
                    None => format!("subscription {} (query)", i + 1),
                },
                kind: "subscription",
                node: s.node,
                arrangement: None,
            });
        }
        out
    }

    /// Name of the table or materialized view whose root is `node`.
    fn node_target(&self, node: NodeId) -> Option<String> {
        let id = self
            .rt
            .tables
            .iter()
            .find(|(_, n)| **n == node)
            .map(|(id, _)| *id)
            .or_else(|| self.rt.views.iter().find(|(_, v)| v.node == node).map(|(id, _)| *id))?;
        self.catalog.get(id).map(|o| o.name.clone())
    }

    fn live_node(&self, id: NodeId) -> Option<&Node> {
        self.graph.nodes.get(id)?.as_ref()
    }

    fn live_arr(&self, id: ArrId) -> Option<&Arrangement> {
        self.graph.arrs.get(id)?.as_ref()
    }

    /// Nodes `root` depends on (inputs and the owners of arrangements read).
    fn upstream_nodes(&self, root: NodeId, budget: &mut usize) -> Vec<NodeId> {
        let mut seen: FxHashSet<NodeId> = FxHashSet::default();
        let mut stack = vec![root];
        let mut out = Vec::new();
        while let Some(n) = stack.pop() {
            if *budget == 0 {
                break;
            }
            *budget -= 1;
            if !seen.insert(n) {
                continue;
            }
            let Some(node) = self.live_node(n) else { continue };
            out.push(n);
            stack.extend(node.inputs.iter().copied());
            for a in operator_arrangements(&node.kind) {
                if let Some(arr) = self.live_arr(a) {
                    stack.push(arr.node);
                }
            }
        }
        out
    }

    fn node_label(&self, node: &Node) -> Option<String> {
        if let NodeKind::Table { object, .. } = &node.kind {
            return self.catalog.get(*object).map(|o| o.name.clone());
        }
        let views: Vec<String> = self
            .rt
            .views
            .iter()
            .filter(|(_, v)| v.node == node.id)
            .filter_map(|(id, _)| self.catalog.get(*id).map(|o| o.name.clone()))
            .collect();
        if views.is_empty() {
            None
        } else {
            let mut v = views;
            v.sort();
            Some(v.join(", "))
        }
    }

    fn arr_info(&self, a: &Arrangement, owned_index: bool, readers: &[NodeId], res: Option<&TreeResidency>) -> ArrInfo {
        let res = res.copied().unwrap_or_default();
        ArrInfo {
            id: a.id,
            node: a.node,
            role: if owned_index { "index" } else { "state" },
            key: a
                .layout
                .key
                .iter()
                .zip(&a.layout.specs)
                .map(|(k, s)| {
                    let mut t = k.to_string();
                    if s.desc {
                        t.push_str(" DESC");
                    }
                    t
                })
                .collect(),
            summaries: a.layout.summaries.iter().map(|(k, e)| format!("{k:?}({e})")).collect(),
            refs: a.refs,
            records: self.env.tree_count(a.tree),
            resident_bytes: res.bytes,
            resident_nodes: res.nodes,
            hydrated: a.hydrated,
            readers: readers.to_vec(),
        }
    }

    fn node_info(&self, node: &Node, owned: &[ArrId], users: Vec<usize>) -> NodeInfo {
        NodeInfo {
            id: node.id,
            kind: node.kind.name(),
            label: self.node_label(node),
            summary: truncate(&self.node_summary(node), 240),
            inputs: node.inputs.clone(),
            schema: node.schema.iter().map(type_name).collect(),
            refs: node.refs,
            hydrated: node.hydrated,
            arrangements: owned.to_vec(),
            reads: {
                let mut r = operator_arrangements(&node.kind);
                r.sort_unstable();
                r.dedup();
                r
            },
            records: owned.iter().filter_map(|a| self.live_arr(*a)).map(|a| self.env.tree_count(a.tree)).sum(),
            users,
            counters: self.graph.counters.get(&node.id).copied().unwrap_or_default(),
        }
    }

    /// Arrangements owned by each node, and the nodes reading each one.
    fn arrangement_maps(&self) -> (FxHashMap<NodeId, Vec<ArrId>>, FxHashMap<ArrId, Vec<NodeId>>) {
        let mut owned: FxHashMap<NodeId, Vec<ArrId>> = FxHashMap::default();
        for a in self.graph.live_arrangements() {
            owned.entry(a.node).or_default().push(a.id);
        }
        let mut readers: FxHashMap<ArrId, Vec<NodeId>> = FxHashMap::default();
        for n in self.graph.live_nodes() {
            for a in operator_arrangements(&n.kind) {
                let r = readers.entry(a).or_default();
                if r.last() != Some(&n.id) {
                    r.push(n.id);
                }
            }
        }
        (owned, readers)
    }

    fn is_output_index(&self, a: &Arrangement) -> bool {
        self.live_node(a.node).is_some_and(|n| n.arrangements.contains(&a.id))
    }

    fn inspect_dataflow(&self) -> DataflowInfo {
        let objects = self.dataflow_objects();
        let mut users: FxHashMap<NodeId, Vec<usize>> = FxHashMap::default();
        let mut budget = USERS_BUDGET;
        for (i, o) in objects.iter().enumerate() {
            for n in self.upstream_nodes(o.node, &mut budget) {
                users.entry(n).or_default().push(i);
            }
        }
        let (owned, readers) = self.arrangement_maps();
        let residency = self.env.tree_residency();
        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        let mut truncated = budget == 0;
        for n in self.graph.live_nodes() {
            if nodes.len() >= MAX_NODES {
                truncated = true;
                break;
            }
            for i in &n.inputs {
                edges.push([*i, n.id]);
            }
            let own = owned.get(&n.id).map(|v| v.as_slice()).unwrap_or(&[]);
            nodes.push(self.node_info(n, own, users.remove(&n.id).unwrap_or_default()));
        }
        let arrangements = self
            .graph
            .live_arrangements()
            .take(MAX_NODES)
            .map(|a| {
                let r = readers.get(&a.id).map(|v| v.as_slice()).unwrap_or(&[]);
                self.arr_info(a, self.is_output_index(a), r, residency.get(&a.tree))
            })
            .collect();
        DataflowInfo { gsn: self.processed, nodes, arrangements, edges, objects, totals: self.graph.step_totals, truncated }
    }

    fn inspect_node(&mut self, id: NodeId, sample: usize) -> Result<NodeDetail> {
        let Some(node) = self.live_node(id) else {
            sql_bail!(codes::UNDEFINED_OBJECT, "dataflow node {id} does not exist");
        };
        let objects = self.dataflow_objects();
        let mut budget = USERS_BUDGET;
        let mut user_idx = Vec::new();
        for (i, o) in objects.iter().enumerate() {
            if self.upstream_nodes(o.node, &mut budget).contains(&id) {
                user_idx.push(i);
            }
        }
        let (owned, readers) = self.arrangement_maps();
        let own = owned.get(&id).cloned().unwrap_or_default();
        let info = self.node_info(node, &own, user_idx.clone());
        let details = self.node_details(node);
        let consumers: Vec<NodeId> = self.graph.live_nodes().filter(|n| n.inputs.contains(&id)).map(|n| n.id).collect();
        let columns = self.node_column_names(node);
        let schema = node.schema.clone();
        let residency = self.env.tree_residency();
        let mut arrangements = Vec::new();
        for a in &own {
            let Some(arr) = self.live_arr(*a) else { continue };
            let index = self.is_output_index(arr);
            let r = readers.get(a).map(|v| v.as_slice()).unwrap_or(&[]);
            let ainfo = self.arr_info(arr, index, r, residency.get(&arr.tree));
            let headers: Vec<ColumnHeader> = if index {
                schema
                    .iter()
                    .enumerate()
                    .map(|(i, t)| ColumnHeader { name: columns.get(i).cloned().unwrap_or_else(|| format!("#{i}")), ty: type_name(t) })
                    .collect()
            } else {
                (0..arr.layout.row_len).map(|i| ColumnHeader { name: format!("#{i}"), ty: String::new() }).collect()
            };
            let s = self.sample_arrangement(*a, sample);
            arrangements.push(ArrDetail { info: ainfo, columns: headers, sample: s });
        }
        Ok(NodeDetail {
            gsn: self.processed,
            node: info,
            details,
            consumers,
            columns,
            arrangements,
            users: user_idx.into_iter().map(|i| objects[i].clone()).collect(),
        })
    }

    /// Column names of a table or view root node (empty when unknown).
    fn node_column_names(&self, node: &Node) -> Vec<String> {
        let id = match &node.kind {
            NodeKind::Table { object, .. } => Some(*object),
            _ => self.rt.views.iter().find(|(_, v)| v.node == node.id).map(|(id, _)| *id),
        };
        let names: Vec<String> = match id.and_then(|id| self.catalog.get(id)).map(|o| &o.kind) {
            Some(ObjectKind::Table(t)) => t.columns.iter().map(|c| c.name.clone()).collect(),
            Some(ObjectKind::MaterializedView(v)) => v.columns.iter().map(|c| c.name.clone()).collect(),
            _ => vec![],
        };
        if names.len() == node.schema.len() { names } else { vec![] }
    }

    /// Up to `limit` rows of an arrangement with their current weights.
    fn sample_arrangement(&mut self, arr: ArrId, limit: usize) -> Sample {
        let Some(a) = self.live_arr(arr) else { return Sample::default() };
        let layout = a.layout.clone();
        let tree = a.tree;
        let mut out = Sample::default();
        if limit == 0 {
            out.truncated = self.env.tree_count(tree) > 0;
            return out;
        }
        let mut c = self.env.cursor(tree, std::ops::Bound::Unbounded, std::ops::Bound::Unbounded);
        while out.scanned < MAX_SCAN {
            let (k, h) = match self.env.cursor_next(&mut c) {
                Ok(Some(e)) => e,
                Ok(None) => return out,
                Err(e) => {
                    log::warn!("sampling arrangement {arr}: {e}");
                    return out;
                }
            };
            out.scanned += 1;
            let w = h.weight(ReadAt::Latest);
            if w == 0 {
                continue;
            }
            if out.rows.len() >= limit {
                out.truncated = true;
                return out;
            }
            let values = match layout.decode_row(&k) {
                Ok(row) => row.iter().map(cell).collect(),
                Err(e) => vec![Some(format!("<undecodable: {e}>"))],
            };
            out.rows.push(SampleRow { values, weight: w });
        }
        out.truncated = true;
        out
    }

    fn mfp_text(&self, m: &crate::dataflow::mfp::Mfp) -> Vec<String> {
        let mut out = Vec::new();
        let mut next = m.input_arity;
        for s in &m.steps {
            match s {
                MfpStep::Map(e) => {
                    out.push(format!("map #{next} := {e}"));
                    next += 1;
                }
                MfpStep::Filter(e) => out.push(format!("filter {e}")),
            }
        }
        let identity = m.projection.len() == m.input_arity && m.projection.iter().enumerate().all(|(i, c)| i == *c);
        if !identity {
            out.push(format!("project {}", cols(&m.projection)));
        }
        out
    }

    /// Operator parameters in one line.
    fn node_summary(&self, node: &Node) -> String {
        match &node.kind {
            NodeKind::Table { pk, .. } => {
                if pk.is_empty() {
                    "multiset (no primary key)".into()
                } else {
                    format!("primary key {}", cols(pk))
                }
            }
            NodeKind::Constant { rows } => format!("{} rows", rows.len()),
            NodeKind::Mfp { mfp } => {
                let filters = mfp.steps.iter().filter(|s| matches!(s, MfpStep::Filter(_))).count();
                let maps = mfp.steps.len() - filters;
                let mut parts = Vec::new();
                if let Some(MfpStep::Filter(e)) = mfp.steps.iter().find(|s| matches!(s, MfpStep::Filter(_))) {
                    parts.push(if filters > 1 { format!("filter {e} (+{})", filters - 1) } else { format!("filter {e}") });
                }
                if maps > 0 {
                    parts.push(format!("{maps} computed"));
                }
                parts.push(format!("{} → {} cols", mfp.input_arity, mfp.projection.len()));
                parts.join(" · ")
            }
            NodeKind::Union { negate } => {
                let neg = negate.iter().filter(|n| **n).count();
                if neg > 0 { format!("{} inputs, {neg} negated", negate.len()) } else { format!("{} inputs", negate.len()) }
            }
            NodeKind::Negate => "negate weights".into(),
            NodeKind::FlatMap { funcs, ordinality } => {
                let f = funcs.iter().map(|f| format!("{:?}", f.func).to_lowercase()).collect::<Vec<_>>().join(", ");
                if *ordinality { format!("{f} with ordinality") } else { f }
            }
            NodeKind::Join(p) => {
                let presence = p.inputs.iter().filter(|i| i.presence.is_some()).count();
                let lookups: usize = p.paths.iter().map(|d| d.steps.len()).sum();
                let mut s = format!("{} inputs, {} delta paths, {lookups} lookups", p.inputs.len(), p.paths.len());
                if presence > 0 {
                    s.push_str(&format!(", {presence} semijoin"));
                }
                s
            }
            NodeKind::Reduce(p) => {
                let aggs = p.aggs.iter().map(|a| self.agg_text(a)).collect::<Vec<_>>().join(", ");
                if p.group_key.is_empty() { aggs } else { format!("group by {}: {aggs}", cols(&p.group_key)) }
            }
            NodeKind::Distinct { .. } => "distinct rows".into(),
            NodeKind::SetOp { op, all, .. } => format!("{op:?}{}", if *all { " all" } else { "" }).to_lowercase(),
            NodeKind::TopK(p) => {
                let mut s = match p.limit {
                    Some(l) => format!("limit {l}"),
                    None => "no limit".into(),
                };
                if p.offset > 0 {
                    s.push_str(&format!(" offset {}", p.offset));
                }
                if p.with_ties {
                    s.push_str(" with ties");
                }
                if !p.group_key.is_empty() {
                    s.push_str(&format!(" per {}", cols(&p.group_key)));
                }
                s
            }
            NodeKind::Window(p) => {
                let f = p.funcs.iter().map(|f| format!("{:?}", f.func).to_lowercase()).collect::<Vec<_>>().join(", ");
                if p.partition.is_empty() { f } else { format!("{f} per {}", cols(&p.partition)) }
            }
            NodeKind::Recursive(p) => if p.distinct { "fixpoint (union)" } else { "fixpoint (union all)" }.into(),
            NodeKind::Temporal(p) => {
                p.preds.iter().map(|t| format!("{} {} {}", t.other, cmp_text(t.op, true), t.clock)).collect::<Vec<_>>().join(" AND ")
            }
        }
    }

    fn agg_text(&self, a: &crate::dataflow::agg::AggSpec) -> String {
        let name = a.func.name();
        let mut s = if name.ends_with(')') {
            name.to_string()
        } else {
            let mut args = Vec::new();
            if let Some(c) = a.arg {
                args.push(format!("#{c}"));
            }
            if let Some(c) = a.arg2 {
                args.push(format!("#{c}"));
            }
            format!("{name}({})", args.join(", "))
        };
        if let Some(f) = a.filter {
            s.push_str(&format!(" filter #{f}"));
        }
        if let Some(fr) = a.fraction {
            s.push_str(&format!(" [{fr}]"));
        }
        s
    }

    /// Operator parameters in full, as labelled lines.
    fn node_details(&self, node: &Node) -> Vec<Detail> {
        let mut d = Vec::new();
        let mut add = |label: &str, value: String| d.push(Detail { label: label.to_string(), value });
        let arr_name = |a: ArrId| -> String {
            match self.live_arr(a) {
                Some(x) => format!("arr#{a} on node #{} key ({})", x.node, join_display(&x.layout.key, ", ")),
                None => format!("arr#{a}"),
            }
        };
        match &node.kind {
            NodeKind::Table { object, version, pk, primary } => {
                let name = self.catalog.get(*object).map(|o| o.name.clone()).unwrap_or_else(|| format!("#{object}"));
                add("table", format!("{name} (object {object}, schema version {version})"));
                add("primary key", if pk.is_empty() { "none (multiset keyed by all columns)".into() } else { cols(pk) });
                add("primary arrangement", arr_name(*primary));
            }
            NodeKind::Constant { rows } => {
                add("rows", rows.len().to_string());
                for r in rows.iter().take(20) {
                    add("row", r.iter().map(|v| cell(v).unwrap_or_else(|| "NULL".into())).collect::<Vec<_>>().join(", "));
                }
            }
            NodeKind::Mfp { mfp } => {
                add("input columns", mfp.input_arity.to_string());
                for l in self.mfp_text(mfp) {
                    add("step", l);
                }
            }
            NodeKind::Union { negate } => {
                for (i, n) in negate.iter().enumerate() {
                    add(&format!("input {i}"), if *n { "negated".into() } else { "added".into() });
                }
            }
            NodeKind::Negate => add("operation", "multiplies weights by -1".into()),
            NodeKind::FlatMap { funcs, ordinality } => {
                for f in funcs {
                    add("function", format!("{:?}({}) → {} cols", f.func, join_display(&f.args, ", "), f.arity));
                }
                add("ordinality", ordinality.to_string());
            }
            NodeKind::Join(p) => {
                add("output width", p.width.to_string());
                for (i, inp) in p.inputs.iter().enumerate() {
                    let mut s = format!("node #{} → columns {}..{}", inp.node, inp.offset, inp.offset + inp.width);
                    if let Some(a) = inp.presence {
                        s.push_str(&format!(", semijoin presence {}", arr_name(a)));
                    }
                    let m = self.mfp_text(&inp.mfp);
                    if !m.is_empty() {
                        s.push_str(&format!("; {}", m.join("; ")));
                    }
                    add(&format!("input {i}"), s);
                }
                for dp in &p.paths {
                    let mut parts = vec![format!("Δ input {}", dp.source)];
                    if !dp.source_filters.is_empty() {
                        parts.push(format!("filter {}", join_display(&dp.source_filters, " AND ")));
                    }
                    for s in &dp.steps {
                        let ns = if s.null_safe.iter().any(|b| *b) { " (null-safe)" } else { "" };
                        let mut t = format!("lookup input {} in arr#{} by ({}){ns}", s.input, s.arr, join_display(&s.key, ", "));
                        if !s.filters.is_empty() {
                            t.push_str(&format!(" then filter {}", join_display(&s.filters, " AND ")));
                        }
                        parts.push(t);
                    }
                    add(&format!("delta path {}", dp.source), parts.join(" → "));
                }
            }
            NodeKind::Reduce(p) => {
                add("group key", cols(&p.group_key));
                for a in &p.aggs {
                    add("aggregate", format!("{} → {}", self.agg_text(a), type_name(&a.ret)));
                }
                add("input arrangement", arr_name(p.input_arr));
                add("output arrangement", arr_name(p.out_arr));
            }
            NodeKind::Distinct { arr } => add("input arrangement", arr_name(*arr)),
            NodeKind::SetOp { op, all, left_arr, right_arr } => {
                add("operation", format!("{op:?}{}", if *all { " ALL" } else { "" }));
                add("left arrangement", arr_name(*left_arr));
                add("right arrangement", arr_name(*right_arr));
            }
            NodeKind::TopK(p) => {
                add("group key", cols(&p.group_key));
                add("limit", p.limit.map(|l| l.to_string()).unwrap_or_else(|| "none".into()));
                add("offset", p.offset.to_string());
                add("with ties", p.with_ties.to_string());
                add("input arrangement", arr_name(p.input_arr));
            }
            NodeKind::Window(p) => {
                add("partition by", cols(&p.partition));
                add(
                    "order by",
                    p.order
                        .iter()
                        .map(|k| format!("#{}{}", k.col, if k.spec.desc { " DESC" } else { "" }))
                        .collect::<Vec<_>>()
                        .join(", "),
                );
                for f in &p.funcs {
                    add("function", format!("{:?}({}) {:?} → {}", f.func, cols(&f.args), f.frame, type_name(&f.ret)));
                }
                add("input arrangement", arr_name(p.input_arr));
            }
            NodeKind::Recursive(p) => {
                add("semantics", if p.distinct { "UNION (distinct)".into() } else { "UNION ALL".into() });
                add("base", p.base.explain().trim_end().to_string());
                add("step", p.step.explain().trim_end().to_string());
                for (rel, n) in &p.sources {
                    add("reads", format!("relation {rel} from node #{n}"));
                }
                add("output arrangement", arr_name(p.out_arr));
            }
            NodeKind::Temporal(p) => {
                for t in &p.preds {
                    add("predicate", format!("{} {} {}  (clock: {})", t.other, cmp_text(t.op, true), "now", t.clock));
                }
                add("pending events", arr_name(p.events));
            }
        }
        d
    }

    fn inspect_object(&mut self, name: &str, sample: usize) -> Result<ObjectDetail> {
        let Some(o) = self.catalog.by_name(name).cloned() else {
            sql_bail!(codes::UNDEFINED_OBJECT, "object \"{name}\" does not exist");
        };
        let deps = self.dependency_map();
        let rdeps = Self::reverse_deps(&deps);
        let object = self.object_info(&o, &deps, &rdeps);
        let mut problems = Vec::new();
        if let Some(reason) = self.rt.broken.get(&o.id) {
            problems.push(format!("broken: {reason}"));
        }
        let mut plan = None;
        let mut dataflow = None;
        let mut maintenance = None;
        if let ObjectKind::View(v) | ObjectKind::MaterializedView(v) = &o.kind {
            // a stored definition (as recovery plans it)
            match crate::sql::parser::parse_single_query(&v.sql).and_then(|q| crate::sql::plan_stored_query(&self.catalog, &q)) {
                Ok(bq) => plan = Some(bq.rel.explain().lines().map(|l| l.to_string()).collect()),
                Err(e) => problems.push(format!("plan: {e}")),
            }
        }
        if self.rt.views.contains_key(&o.id) {
            match self.explain_rows(ExplainKind::Dataflow, &o.name) {
                Ok(rows) => dataflow = Some(rows.into_iter().map(|r| r.join(" ")).collect()),
                Err(e) => problems.push(format!("dataflow: {e}")),
            }
            match self.explain_rows(ExplainKind::Maintenance, &o.name) {
                Ok(rows) => {
                    maintenance = Some(
                        rows.into_iter()
                            .map(|r| {
                                let mut it = r.into_iter();
                                MaintenanceRow {
                                    source: it.next().unwrap_or_default(),
                                    delta_rows_per_tick: it.next().unwrap_or_default(),
                                    work_per_delta: it.next().unwrap_or_default(),
                                    path: it.next().unwrap_or_default(),
                                }
                            })
                            .collect(),
                    )
                }
                Err(e) => problems.push(format!("maintenance: {e}")),
            }
        }
        let errors = self
            .rt
            .views
            .get(&o.id)
            .map(|v| {
                v.errors
                    .iter()
                    .filter(|(_, w)| **w > 0)
                    .map(|((code, message), w)| ErrorRecord { code: code.clone(), message: message.clone(), count: *w })
                    .collect()
            })
            .unwrap_or_default();
        let sample = match (self.object_arrangement(o.id), &o.kind) {
            (Some(arr), ObjectKind::Table(_) | ObjectKind::MaterializedView(_)) => {
                let columns: Vec<ColumnHeader> = object.columns.iter().map(|c| ColumnHeader { name: c.name.clone(), ty: c.ty.clone() }).collect();
                let s = self.sample_arrangement(arr, sample);
                // a view whose node has hidden columns: show positions instead
                let width = s.rows.first().map(|r| r.values.len()).unwrap_or(columns.len());
                let columns = if width == columns.len() {
                    columns
                } else {
                    (0..width).map(|i| ColumnHeader { name: format!("#{i}"), ty: String::new() }).collect()
                };
                Some(TableSample { columns, sample: s })
            }
            _ => None,
        };
        let source = if matches!(o.kind, ObjectKind::Source(_)) { self.source_info(o.id) } else { None };
        let nodes = match self.object_node(o.id) {
            Some(n) => {
                let mut budget = USERS_BUDGET;
                let mut v = self.upstream_nodes(n, &mut budget);
                v.sort_unstable();
                v
            }
            None => vec![],
        };
        Ok(ObjectDetail { gsn: self.processed, object, plan, dataflow, maintenance, errors, sample, source, nodes, problems })
    }

    /// EXPLAIN DATAFLOW / MAINTENANCE through the regular statement path.
    fn explain_rows(&mut self, kind: ExplainKind, name: &str) -> Result<Vec<Vec<String>>> {
        let Some(h) = self.handle.clone() else {
            sql_bail!(codes::OBJECT_NOT_IN_PREREQUISITE_STATE, "engine is starting");
        };
        let (tx, _rx) = bounded(1);
        let stmt = Statement::Explain { kind, target: ExplainTarget::Name(name.to_string()) };
        match self.execute(stmt, vec![], SessionVars::default(), String::new(), tx, &h)? {
            Some(ExecResult::Query { rows, .. }) => {
                Ok(rows.iter().map(|r: &Row| r.iter().map(|d| cell(d).unwrap_or_default()).collect()).collect())
            }
            _ => Ok(vec![]),
        }
    }

    fn inspect_sessions(&self) -> SessionsInfo {
        let subscriptions = self
            .subs
            .iter()
            .map(|s| SubscriptionInfo {
                node: s.node,
                target: if s.pinned.is_some() { None } else { self.node_target(s.node) },
                progress: s.progress,
                cancelled: s.cancel.load(Ordering::Acquire),
            })
            .collect();
        let parked = self
            .parked
            .iter()
            .map(|p| {
                let (wait, gsn) = match p.wait {
                    Wait::Gsn(g) => ("gsn", Some(g)),
                    Wait::Lsn(_) => ("lsn", None),
                    Wait::Snapshot { .. } => ("snapshot", None),
                    Wait::Hydration(_) => ("hydration", None),
                };
                let sql = match &p.req {
                    Request::Execute { stmt, .. } => match &**stmt {
                        Statement::Select { query, as_of, .. } => {
                            let suffix = match as_of {
                                AsOf::Gsn(g) => format!(" AS OF {g}"),
                                AsOf::AtLeastLsn(l) => format!(" AS OF AT LEAST LSN '{l}'"),
                                _ => String::new(),
                            };
                            format!("{query}{suffix}")
                        }
                        other => format!("{other:?}"),
                    },
                    _ => String::new(),
                };
                ParkedInfo { wait, gsn, sql: truncate(&sql, 4096) }
            })
            .collect();
        let sinks = self
            .catalog
            .objects
            .values()
            .filter_map(|o| match &o.kind {
                ObjectKind::Sink(s) => {
                    let status = self.sink_status(o.id);
                    let committed = sink_committed(&status);
                    Some(SinkInfo {
                        id: o.id,
                        name: o.name.clone(),
                        from: self.catalog.get(s.from).map(|x| x.name.clone()).unwrap_or_default(),
                        table: s.table.clone(),
                        committed_gsn: committed,
                        lag_gsns: committed.map(|c| self.processed.saturating_sub(c)),
                        status,
                    })
                }
                _ => None,
            })
            .collect();
        SessionsInfo {
            gsn: self.processed,
            subscriptions,
            parked,
            pending_requests: self.pending.len(),
            dml_queued: self.dml_queue.len(),
            dml_inflight: !self.dml_pending.is_empty(),
            sinks,
        }
    }

    fn inspect_errors(&self) -> ErrorsInfo {
        let mut views = Vec::new();
        for (id, v) in &self.rt.views {
            let name = self.catalog.get(*id).map(|o| o.name.clone()).unwrap_or_default();
            for ((code, message), w) in &v.errors {
                if *w > 0 && views.len() < 10_000 {
                    views.push(ViewError { id: *id, name: name.clone(), code: code.clone(), message: message.clone(), count: *w });
                }
            }
        }
        views.sort_by(|a, b| a.name.cmp(&b.name).then(b.count.cmp(&a.count)));
        let failing = |kind: fn(&ObjectKind) -> bool, status: &dyn Fn(ObjectId) -> String| -> Vec<StatusBrief> {
            self.catalog
                .objects
                .values()
                .filter(|o| kind(&o.kind))
                .filter_map(|o| {
                    let s = status(o.id);
                    s.starts_with("error").then(|| StatusBrief { id: o.id, name: o.name.clone(), status: s })
                })
                .collect()
        };
        ErrorsInfo {
            gsn: self.processed,
            views,
            sources: failing(|k| matches!(k, ObjectKind::Source(_)), &|id| self.source_status(id)),
            sinks: failing(|k| matches!(k, ObjectKind::Sink(_)), &|id| self.sink_status(id)),
            broken: failing(|k| matches!(k, ObjectKind::MaterializedView(_) | ObjectKind::Index(_)), &|id| {
                self.broken_status(id).unwrap_or_default()
            }),
        }
    }
}

fn cmp_text(op: crate::dataflow::temporal::CmpOp, flipped: bool) -> &'static str {
    use crate::dataflow::temporal::CmpOp::*;
    // `clock OP other` read as `other OP' clock`
    match (op, flipped) {
        (Lt, false) => "<",
        (Le, false) => "<=",
        (Gt, false) => ">",
        (Ge, false) => ">=",
        (Lt, true) => ">",
        (Le, true) => ">=",
        (Gt, true) => "<",
        (Ge, true) => "<=",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conninfo_passwords_are_masked() {
        assert_eq!(redact_conninfo("host=h user=u password=secret dbname=d"), "host=h user=u password=*** dbname=d");
        assert_eq!(redact_conninfo("host=h password = 'a b\\' c' port=5"), "host=h password = *** port=5");
        assert_eq!(redact_conninfo("postgres://u:pw@h:5432/db?sslmode=disable"), "postgres://u:***@h:5432/db?sslmode=disable");
        assert_eq!(redact_conninfo("postgresql://h/db?user=u&password=x"), "postgresql://h/db?user=u&password=***");
        assert_eq!(redact_conninfo("postgresql://u@h/db"), "postgresql://u@h/db");
        assert_eq!(redact_conninfo("host=h"), "host=h");
    }

    #[test]
    fn sink_status_gsn() {
        assert_eq!(sink_committed("running (gsn 42)"), Some(42));
        assert_eq!(sink_committed("starting"), None);
    }

    #[test]
    fn ddl_actions() {
        assert_eq!(statement_action("create materialized view v as select 1"), "CREATE MATERIALIZED VIEW");
        assert_eq!(statement_action("DROP TABLE t CASCADE"), "DROP TABLE");
        assert_eq!(statement_action("CREATE OR REPLACE VIEW v AS SELECT 1"), "CREATE OR REPLACE VIEW");
        assert_eq!(statement_action("ALTER SOURCE pg REFRESH"), "ALTER SOURCE");
    }
}
