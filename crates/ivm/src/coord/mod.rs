//! The coordinator: the engine thread owning storage, dataflow and catalog,
//! fed by the sequencer with durable log records in GSN order.

pub mod api;
pub mod ddl;
mod dml;
pub mod inspect;
mod read;
pub mod seq;

use crate::catalog::{Catalog, ObjectKind};
use crate::dataflow::{ArrId, ErrDelta, Graph, NodeId, Output, TableChange};
use crate::error::{codes, Result, SqlError};
use crate::log::{LogRecord, LogWriter};
use crate::sql::plan::ObjectId;
use crate::storage::{Env, EnvOptions, Gsn, TreeId};
use api::*;
use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use rustc_hash::FxHashMap;
use seq::{SeqMsg, Sequencer};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct Config {
    pub data_dir: PathBuf,
    pub cache_bytes: usize,
    pub checkpoint_interval: Duration,
    /// fsync the input log on every commit batch.
    pub sync: bool,
    /// GSNs of history kept readable with AS OF.
    pub retention_gsns: u64,
    /// Interval of logged clock ticks (the time now() refers to).
    pub tick: Option<Duration>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            data_dir: PathBuf::from("ivm-data"),
            cache_bytes: 1 << 30,
            checkpoint_interval: Duration::from_secs(10),
            sync: true,
            retention_gsns: 1000,
            tick: Some(Duration::from_secs(1)),
        }
    }
}

pub enum EngineMsg {
    Durable(Vec<(Gsn, LogRecord)>),
    Request(Request),
    /// A source frontier advanced: re-check parked reads.
    Wake,
    Shutdown,
}

/// Runtime state of a materialized view.
pub struct ViewRt {
    pub node: NodeId,
    /// Arrangement used for reads.
    pub arr: ArrId,
    pub errors: BTreeMap<(String, String), i64>,
}

#[derive(Default)]
pub struct Runtime {
    pub tables: HashMap<ObjectId, NodeId>,
    pub views: HashMap<ObjectId, ViewRt>,
    pub indexes: HashMap<ObjectId, ArrId>,
    /// Catalog objects without a runtime, with the reason: materialized
    /// views whose stored definition no longer plans at recovery (newer,
    /// stricter code or catalog drift), indexes on them or whose columns
    /// are gone, sinks reading them. They stay in the catalog: reads fail
    /// with the reason, DROP removes them, CREATE OR REPLACE repairs a view.
    pub broken: BTreeMap<ObjectId, String>,
}

pub(crate) struct Subscription {
    pub node: NodeId,
    pub tx: Sender<SubEvent>,
    pub cancel: Arc<std::sync::atomic::AtomicBool>,
    /// Nodes pinned for this subscription (ad hoc queries).
    pub pinned: Option<NodeId>,
    pub progress: bool,
    /// The relations it reads, also through views: it ends when one of
    /// them is dropped.
    pub reads: Vec<ObjectId>,
}

/// A read waiting for the engine to reach a GSN.
pub(crate) struct Parked {
    pub wait: Wait,
    pub req: Request,
}

/// What a parked read waits for.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Wait {
    /// The engine to process this GSN.
    Gsn(Gsn),
    /// Every source frontier to pass the read's LSN.
    Lsn,
    /// The initial snapshots of the tables it reads (rechecked on wake).
    Snapshot,
}

#[derive(Serialize, Deserialize, Default)]
pub(crate) struct AppState {
    pub catalog: Catalog,
    pub trees: Vec<(Vec<u8>, TreeId)>,
    pub view_errors: Vec<(ObjectId, Vec<((String, String), i64)>)>,
    pub source_states: Vec<(ObjectId, String)>,
    #[serde(default)]
    pub source_lsns: Vec<(ObjectId, u64)>,
    pub next_req: u64,
    /// Logical clock (timestamptz microseconds) at the checkpoint.
    #[serde(default)]
    pub clock: i64,
    /// Error records of arranged dataflow nodes (absent in older checkpoints).
    #[serde(default)]
    pub errors: crate::dataflow::ErrorState,
    /// GSN from which each tree's history is complete (by fingerprint).
    #[serde(default)]
    pub arr_since: Vec<(Vec<u8>, Gsn)>,
    /// Recent (gsn, clock) ticks (`Engine::ticks`, the last MAX_SAVED_TICKS
    /// of the retention window), for now() in reads as of earlier GSNs
    /// (absent in older checkpoints).
    #[serde(default)]
    pub ticks: Vec<(Gsn, i64)>,
}

/// DDL statements kept in the catalog history (`ivm.catalog_history`), the
/// most recent ones: the history is part of every checkpoint.
pub const MAX_HISTORY: usize = 10_000;

/// Most clock ticks kept in a checkpoint: a day of ticks at the default
/// interval. now() in a read as of an older GSN (within a larger retention
/// window) after a restart is the oldest kept tick's clock.
const MAX_SAVED_TICKS: usize = 100_000;

pub struct Engine {
    pub cfg: Config,
    pub env: Env,
    pub graph: Graph,
    pub catalog: Catalog,
    pub rt: Runtime,
    pub processed: Gsn,
    pub seq: Sender<SeqMsg>,
    pub shared: Arc<Shared>,
    pending: HashMap<u64, Sender<Result<ExecResult>>>,
    pub(crate) parked: Vec<Parked>,
    pub(crate) subs: Vec<Subscription>,
    pub(crate) next_req: u64,
    dml_inflight: Option<u64>,
    dml_queue: VecDeque<Request>,
    /// Command tags of DML waiting to be applied.
    pub(crate) dml_counts: HashMap<u64, String>,
    /// Requests to run after the current message.
    deferred: Vec<Request>,
    last_checkpoint: Instant,
    records_since_checkpoint: u64,
    pub(crate) replaying: bool,
    /// Bumped by source progress and DDL: parked reads waiting for initial
    /// snapshots are re-run when it moves.
    pub(crate) snap_epoch: u64,
    pub(crate) snap_epoch_seen: u64,
    /// (epoch, replicated tables still loading their initial snapshot)
    pub(crate) loading: (u64, std::collections::HashSet<ObjectId>),
    /// Per-source progress state (opaque JSON owned by the source).
    pub source_states: HashMap<ObjectId, String>,
    /// LSN of the last logged commit per source (resume point).
    pub source_lsns: HashMap<ObjectId, u64>,
    pub(crate) handle: Option<EngineHandle>,
    pub(crate) source_handles: HashMap<ObjectId, crate::source::SourceHandle>,
    pub(crate) sink_handles: HashMap<ObjectId, crate::sink::SinkHandle>,
    /// Logical clock: the latest logged tick (timestamptz microseconds).
    pub clock: i64,
    /// Recent (gsn, clock) ticks, for now() in reads as of a past GSN.
    pub ticks: VecDeque<(Gsn, i64)>,
}

/// Handle used by sessions and sources.
#[derive(Clone)]
pub struct EngineHandle {
    pub tx: Sender<EngineMsg>,
    pub seq: Sender<SeqMsg>,
    pub shared: Arc<Shared>,
}

impl EngineHandle {
    /// Record a source's LSN frontier; wakes reads waiting for it.
    pub fn advance_frontier(&self, source: u64, lsn: u64) {
        let mut m = self.shared.source_lsn.lock();
        let e = m.entry(source).or_insert(0);
        if lsn > *e {
            *e = lsn;
            drop(m);
            if self.shared.lsn_waiters.load(Ordering::Acquire) {
                let _ = self.tx.send(EngineMsg::Wake);
            }
        }
    }

    pub fn execute(&self, stmt: crate::sql::parser::Statement, params: Vec<crate::sql::ParamSlot>, vars: SessionVars, extra: String) -> Result<ExecResult> {
        let (tx, rx) = bounded(1);
        self.tx
            .send(EngineMsg::Request(Request::Execute { stmt: Box::new(stmt), params, vars, extra, reply: tx }))
            .map_err(|_| SqlError::new(codes::ADMIN_SHUTDOWN, "engine is shutting down"))?;
        rx.recv().map_err(|_| SqlError::new(codes::ADMIN_SHUTDOWN, "engine is shutting down"))?
    }

    /// `execute`, giving up when `alive` returns false (polled every 200 ms
    /// while the statement runs or waits): the request is then marked
    /// `gone` in its session's interrupts, so that the engine drops it if
    /// it is still waiting (a parked read), and the call fails with
    /// IO_ERROR.
    pub fn execute_while(
        &self,
        stmt: crate::sql::parser::Statement,
        params: Vec<crate::sql::ParamSlot>,
        vars: SessionVars,
        extra: String,
        mut alive: impl FnMut() -> bool,
    ) -> Result<ExecResult> {
        let interrupts = vars.interrupts.clone();
        let (tx, rx) = bounded(1);
        self.tx
            .send(EngineMsg::Request(Request::Execute { stmt: Box::new(stmt), params, vars, extra, reply: tx }))
            .map_err(|_| SqlError::new(codes::ADMIN_SHUTDOWN, "engine is shutting down"))?;
        loop {
            match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(r) => return r,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    if !alive() {
                        if let Some(i) = &interrupts {
                            i.gone.store(true, Ordering::Release);
                            let _ = self.tx.send(EngineMsg::Wake);
                        }
                        return Err(SqlError::new(codes::IO_ERROR, "client disconnected"));
                    }
                }
                Err(_) => return Err(SqlError::new(codes::ADMIN_SHUTDOWN, "engine is shutting down")),
            }
        }
    }

    pub fn describe(
        &self,
        stmt: crate::sql::parser::Statement,
        params: Vec<crate::sql::ParamSlot>,
    ) -> Result<(Vec<ResultColumn>, Vec<crate::types::SqlType>)> {
        let (tx, rx) = bounded(1);
        self.tx
            .send(EngineMsg::Request(Request::Describe { stmt: Box::new(stmt), params, reply: tx }))
            .map_err(|_| SqlError::new(codes::ADMIN_SHUTDOWN, "engine is shutting down"))?;
        rx.recv().map_err(|_| SqlError::new(codes::ADMIN_SHUTDOWN, "engine is shutting down"))?
    }

    pub fn checkpoint(&self) -> Result<Gsn> {
        let (tx, rx) = bounded(1);
        self.tx
            .send(EngineMsg::Request(Request::Checkpoint { reply: tx }))
            .map_err(|_| SqlError::new(codes::ADMIN_SHUTDOWN, "engine is shutting down"))?;
        rx.recv().map_err(|_| SqlError::new(codes::ADMIN_SHUTDOWN, "engine is shutting down"))?
    }

    /// Append a record and wait until it is durable; returns its GSN.
    pub fn log(&self, rec: LogRecord) -> Result<Gsn> {
        let rx = self.log_async(rec)?;
        rx.recv().map_err(|_| SqlError::new(codes::ADMIN_SHUTDOWN, "sequencer stopped"))
    }

    /// Append a record without waiting; the receiver yields its GSN once durable.
    pub fn log_async(&self, rec: LogRecord) -> Result<Receiver<Gsn>> {
        let (tx, rx) = bounded(1);
        self.seq
            .send(SeqMsg::Append { rec, ack: Some(tx) })
            .map_err(|_| SqlError::new(codes::ADMIN_SHUTDOWN, "sequencer stopped"))?;
        Ok(rx)
    }

    pub fn shutdown(&self) {
        self.shared.shutdown.store(true, Ordering::Release);
        let _ = self.tx.send(EngineMsg::Shutdown);
    }
}

pub struct Running {
    pub handle: EngineHandle,
    pub threads: Vec<JoinHandle<()>>,
    /// Exclusive lock on the data directory.
    _lock: std::fs::File,
}

impl Running {
    /// Stop the engine after a final checkpoint and wait for its threads.
    /// A failure (also a panic) is recorded in `Shared::failed`.
    pub fn shutdown(self) {
        self.handle.shutdown();
        for t in self.threads {
            if t.join().is_err() {
                self.handle.shared.failed.store(true, Ordering::Release);
            }
        }
    }

    /// Are the engine's threads still running (they stop on a shutdown
    /// request, an error or a panic)?
    pub fn alive(&self) -> bool {
        self.threads.iter().all(|t| !t.is_finished())
    }
}

/// Open (recovering if needed) and start the engine threads.
pub fn start(cfg: Config) -> Result<Running> {
    std::fs::create_dir_all(&cfg.data_dir)?;
    // one engine per data directory: held until the process exits. Retried
    // briefly: a process forked concurrently (e.g. a test spawning
    // PostgreSQL) holds a copy of a just-released lock until it execs
    let lock = std::fs::OpenOptions::new().create(true).truncate(false).write(true).open(cfg.data_dir.join("LOCK"))?;
    let mut locked = lock.try_lock().is_ok();
    for _ in 0..40 {
        if locked {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
        locked = lock.try_lock().is_ok();
    }
    if !locked {
        return Err(SqlError::new(
            codes::OBJECT_IN_USE,
            format!("data directory {} is in use by another ivm process", cfg.data_dir.display()),
        ));
    }
    let shared = Arc::new(Shared::default());
    let (etx, erx) = unbounded::<EngineMsg>();
    let (stx, srx) = unbounded::<SeqMsg>();
    let mut writer = LogWriter::open(&cfg.data_dir.join("log"), cfg.sync)?;
    let (env, recovered) = Env::open(
        &cfg.data_dir.join("store"),
        EnvOptions { cache_bytes: cfg.cache_bytes, ..Default::default() },
    )?;
    let mut engine = Engine {
        cfg: cfg.clone(),
        env,
        graph: Graph::new(),
        catalog: Catalog::new(),
        rt: Runtime::default(),
        processed: 0,
        seq: stx.clone(),
        shared: shared.clone(),
        pending: HashMap::new(),
        parked: Vec::new(),
        subs: Vec::new(),
        next_req: 1,
        dml_inflight: None,
        dml_queue: VecDeque::new(),
        dml_counts: HashMap::new(),
        deferred: Vec::new(),
        last_checkpoint: Instant::now(),
        records_since_checkpoint: 0,
        replaying: true,
        snap_epoch: 0,
        snap_epoch_seen: 0,
        loading: (0, std::collections::HashSet::new()),
        source_states: HashMap::new(),
        source_lsns: HashMap::new(),
        handle: None,
        source_handles: HashMap::new(),
        sink_handles: HashMap::new(),
        clock: 0,
        ticks: VecDeque::new(),
    };
    engine.recover(recovered)?;
    // replay the log after the checkpoint
    let records = crate::log::read_from(&cfg.data_dir.join("log"), engine.processed)?;
    if !records.is_empty() {
        log::info!("replaying {} log records after gsn {}", records.len(), engine.processed);
    }
    for (gsn, rec) in records {
        engine.apply_record(gsn, rec)?;
    }
    // The checkpoint can be ahead of the log if the log lost records it
    // covers (an unsynced tail with --no-sync): GSNs must continue after
    // it, or new records would be taken for already applied ones.
    if writer.last_gsn() < engine.processed {
        log::warn!("the input log ends at gsn {} before the checkpoint at {}; continuing after the checkpoint", writer.last_gsn(), engine.processed);
        writer.skip_to(engine.processed);
    }
    shared.durable_gsn.store(writer.last_gsn(), Ordering::Release);
    engine.replaying = false;
    engine.shared.processed_gsn.store(engine.processed, Ordering::Release);
    let handle = EngineHandle { tx: etx.clone(), seq: stx.clone(), shared: shared.clone() };
    engine.handle = Some(handle.clone());
    engine.start_sources_and_sinks(&handle)?;
    let seq = Sequencer { writer, rx: srx, engine: etx.clone(), shared: shared.clone(), tick: cfg.tick };
    let seq_shared = shared.clone();
    let t1 = std::thread::Builder::new().name("ivm-sequencer".into()).spawn(move || {
        if let Err(e) = seq.run() {
            log::error!("sequencer failed: {e}");
            seq_shared.failed.store(true, Ordering::Release);
        }
    })?;
    let h2 = handle.clone();
    // a large stack: statements are bound and planned here (syntax trees up
    // to sql::parser::MAX_OPERATOR_CHAIN deep, bound expressions up to
    // sql::binder_expr::MAX_EXPR_DEPTH); reserved, not committed, memory
    let t2 = std::thread::Builder::new().name("ivm-engine".into()).stack_size(64 << 20).spawn(move || {
        let shared = h2.shared.clone();
        if let Err(e) = engine.run(erx, h2) {
            log::error!("engine failed: {e}");
            shared.failed.store(true, Ordering::Release);
        }
    })?;
    Ok(Running { handle, threads: vec![t1, t2], _lock: lock })
}

impl Engine {
    fn recover(&mut self, recovered: Option<crate::storage::Recovered>) -> Result<()> {
        let Some(rec) = recovered else { return Ok(()) };
        let app: AppState = serde_json::from_slice(&rec.app)
            .map_err(|e| SqlError::corrupt(format!("cannot decode checkpoint state: {e}")))?;
        log::info!("recovering checkpoint at gsn {}", rec.gsn);
        self.catalog = app.catalog;
        self.next_req = app.next_req;
        self.processed = rec.gsn;
        self.graph.recovery = app.trees.into_iter().collect();
        self.graph.recovery_since = app.arr_since.into_iter().collect();
        self.clock = app.clock;
        self.graph.now = app.clock;
        if app.ticks.is_empty() {
            self.ticks.push_back((rec.gsn, app.clock));
        } else {
            self.ticks = app.ticks.into_iter().collect();
        }
        self.source_states = app.source_states.into_iter().collect();
        self.source_lsns = app.source_lsns.into_iter().collect();
        let objects: Vec<_> = self.catalog.objects.values().cloned().collect();
        for obj in &objects {
            if let ObjectKind::Table(t) = &obj.kind {
                self.table_rt(obj.id, t, true)?;
            }
        }
        // views and indexes after the views they read (id order is not a
        // dependency order: CREATE OR REPLACE keeps a view's id). One that
        // cannot be built (its stored definition no longer plans) is kept,
        // marked broken, instead of keeping the engine from starting.
        for (obj, _) in self.dependency_order(&objects) {
            let r = match &obj.kind {
                ObjectKind::MaterializedView(v) => self.create_view_rt(obj.id, &v.sql),
                ObjectKind::Index(i) => self.create_index_rt(obj.id, i),
                _ => continue,
            };
            if let Err(e) = r {
                if e.code.starts_with("58") {
                    return Err(e);
                }
                log::error!("{} {} is broken and not maintained: {}", obj.kind.name(), obj.name, e.message);
                self.rt.broken.insert(obj.id, e.message);
            }
        }
        for (id, errs) in app.view_errors {
            if let Some(v) = self.rt.views.get_mut(&id) {
                v.errors = errs.into_iter().collect();
            }
        }
        self.graph.restore_error_state(app.errors);
        // anything that could not be matched to a recovered tree is rebuilt
        // from the recovered state
        self.graph.hydrate(&mut self.env, self.processed)?;
        for t in self.env.unattached_trees() {
            self.env.drop_tree(t)?;
        }
        self.graph.recovery.clear();
        self.graph.recovery_since.clear();
        self.env.set_frontier(self.processed.saturating_sub(self.cfg.retention_gsns));
        Ok(())
    }

    /// Materialized views and indexes of `objects`, each after the
    /// materialized views and indexes it depends on, with those
    /// dependencies (the relations a view's query reads, including the
    /// views it mentions; an index's target; an error for a view that does
    /// not plan).
    pub(crate) fn dependency_order<'a>(
        &self,
        objects: &'a [crate::catalog::CatalogObject],
    ) -> Vec<(&'a crate::catalog::CatalogObject, Result<Vec<ObjectId>>)> {
        fn gets(r: &crate::sql::Rel, out: &mut Vec<ObjectId>) {
            if let crate::sql::Rel::Get { id, .. } = r {
                out.push(*id);
            }
            for i in r.inputs() {
                gets(i, out);
            }
        }
        let mut pending: Vec<(&crate::catalog::CatalogObject, Result<Vec<ObjectId>>)> = Vec::new();
        for o in objects {
            let deps = match &o.kind {
                ObjectKind::Index(i) => Ok(vec![i.on]),
                ObjectKind::MaterializedView(v) => crate::sql::parser::parse_single_query(&v.sql)
                    .and_then(|q| crate::sql::plan_stored_query(&self.catalog, &q))
                    .map(|bq| {
                        let mut ids = self.catalog.mentioned_views(&v.sql);
                        gets(&bq.rel, &mut ids);
                        ids
                    }),
                _ => continue,
            };
            pending.push((o, deps));
        }
        let ordered: std::collections::HashSet<ObjectId> = pending.iter().map(|(o, _)| o.id).collect();
        let mut done = std::collections::HashSet::new();
        let mut out = Vec::with_capacity(pending.len());
        while !pending.is_empty() {
            let before = out.len();
            let mut rest = Vec::new();
            for (o, deps) in pending {
                // a view that does not plan has no known dependencies
                let ready = match &deps {
                    Ok(d) => d.iter().all(|x| *x == o.id || done.contains(x) || !ordered.contains(x)),
                    Err(_) => true,
                };
                if ready {
                    done.insert(o.id);
                    out.push((o, deps));
                } else {
                    rest.push((o, deps));
                }
            }
            pending = rest;
            if out.len() == before {
                // no progress (cannot happen for a valid catalog)
                out.append(&mut pending);
            }
        }
        out
    }

    fn run(mut self, rx: Receiver<EngineMsg>, handle: EngineHandle) -> Result<()> {
        loop {
            let msg = match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(m) => Some(m),
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => None,
                Err(_) => break,
            };
            match msg {
                Some(EngineMsg::Durable(batch)) => {
                    for (gsn, rec) in batch {
                        let dml = match &rec {
                            LogRecord::Commit { source: 0, lsn, .. } => Some(*lsn),
                            _ => None,
                        };
                        if let Err(e) = self.apply_record(gsn, rec) {
                            log::error!("failed to apply gsn {gsn}: {e}");
                            if e.code.starts_with("XX") || e.code.starts_with("58") {
                                return Err(e);
                            }
                            // (data errors are error records, so this is a
                            // bug) The record is not applied again: its
                            // statement fails instead of leaving the client,
                            // the DML queued behind it and fresh reads
                            // waiting forever.
                            self.processed = self.processed.max(gsn);
                            if let Some(req) = dml {
                                self.dml_counts.remove(&req);
                                self.reply_pending(req, Err(e));
                                self.drain_dml_queue();
                            }
                        }
                    }
                    self.shared.processed_gsn.store(self.processed, Ordering::Release);
                    self.wake_parked(&handle);
                }
                Some(EngineMsg::Request(req)) => self.handle_request(req, &handle),
                Some(EngineMsg::Shutdown) => break,
                Some(EngineMsg::Wake) | None => self.wake_parked(&handle),
            }
            while !self.deferred.is_empty() {
                let reqs = std::mem::take(&mut self.deferred);
                for r in reqs {
                    self.handle_request(r, &handle);
                }
            }
            self.maybe_checkpoint()?;
            self.prune_subscriptions();
        }
        let mut threads: Vec<JoinHandle<()>> = self.source_handles.drain().filter_map(|(_, s)| s.stop()).collect();
        threads.extend(self.sink_handles.drain().filter_map(|(_, s)| s.stop()));
        let _ = self.seq.send(SeqMsg::Shutdown);
        self.checkpoint()?;
        // give sources and sinks a moment to finish; one stuck in a
        // connection attempt is left behind
        let deadline = Instant::now() + Duration::from_secs(5);
        for t in threads {
            while !t.is_finished() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(10));
            }
            if t.is_finished() {
                let _ = t.join();
            }
        }
        Ok(())
    }

    pub(crate) fn alloc_req(&mut self) -> u64 {
        let r = self.next_req;
        self.next_req += 1;
        r
    }

    /// Submit a record to the sequencer; `reply` is answered when it is applied.
    pub(crate) fn submit(&mut self, rec: LogRecord, req: u64, reply: Sender<Result<ExecResult>>) {
        self.pending.insert(req, reply);
        if matches!(rec, LogRecord::Commit { source: 0, .. }) {
            self.dml_inflight = Some(req);
        }
        if self.seq.send(SeqMsg::Append { rec, ack: None }).is_err()
            && let Some(r) = self.pending.remove(&req)
        {
            let _ = r.send(Err(SqlError::new(codes::ADMIN_SHUTDOWN, "sequencer stopped")));
        }
    }

    pub(crate) fn reply_pending(&mut self, req: u64, res: Result<ExecResult>) {
        if let Some(r) = self.pending.remove(&req) {
            let _ = r.send(res);
        }
        if self.dml_inflight == Some(req) {
            self.dml_inflight = None;
        }
    }

    fn handle_request(&mut self, req: Request, handle: &EngineHandle) {
        match req {
            Request::Execute { ref stmt, .. } if self.is_dml(stmt) => {
                if self.dml_inflight.is_some() {
                    self.dml_queue.push_back(req);
                } else {
                    self.run_execute(req, handle);
                }
            }
            Request::Checkpoint { reply } => {
                let r = self.checkpoint().map(|_| self.processed);
                let _ = reply.send(r);
            }
            Request::Log { rec } => {
                let _ = self.seq.send(SeqMsg::Append { rec, ack: None });
            }
            Request::Inspect(r) => self.inspect(r),
            other => self.run_execute(other, handle),
        }
    }

    fn run_execute(&mut self, req: Request, handle: &EngineHandle) {
        match req {
            Request::Execute { stmt, params, vars, extra, reply } => {
                match self.execute(*stmt, params, vars, extra, reply.clone(), handle) {
                    Ok(Some(res)) => {
                        let _ = reply.send(Ok(res));
                    }
                    Ok(None) => {} // deferred (submitted or parked)
                    Err(e) => {
                        let _ = reply.send(Err(e));
                    }
                }
            }
            Request::Describe { stmt, params, reply } => {
                let _ = reply.send(self.describe(&stmt, params));
            }
            other => self.handle_request(other, handle),
        }
    }

    /// Apply one durable record.
    pub(crate) fn apply_record(&mut self, gsn: Gsn, rec: LogRecord) -> Result<()> {
        if gsn <= self.processed {
            return Ok(());
        }
        match rec {
            LogRecord::Commit { source, lsn, changes } => {
                // local DML is planned when it arrives, but DDL logged before
                // it (a DROP of its table) applies first: then it applies nothing
                let dropped = source == 0 && changes.iter().any(|(t, _)| !self.rt.tables.contains_key(t));
                let mut by_node: FxHashMap<NodeId, Vec<TableChange>> = FxHashMap::default();
                for (table, ch) in changes {
                    if let Some(n) = self.rt.tables.get(&table) {
                        by_node.entry(*n).or_default().push(ch);
                    }
                }
                let outputs = self.graph.step(&mut self.env, gsn, by_node)?;
                self.processed = gsn;
                self.deliver(gsn, &outputs);
                if source == 0 {
                    // local DML: lsn carries the request id
                    let n = self.dml_counts.remove(&lsn).unwrap_or_default();
                    let res = if dropped {
                        Err(SqlError::new(codes::UNDEFINED_TABLE, "the table of this statement was dropped before it could be applied"))
                    } else {
                        Ok(ExecResult::Command { tag: n })
                    };
                    self.reply_pending(lsn, res);
                    self.drain_dml_queue();
                } else if self.catalog.get(source).is_some() {
                    // (a dropped source's thread may still log a record
                    // before it notices that it was stopped)
                    let e = self.source_lsns.entry(source).or_insert(0);
                    *e = (*e).max(lsn);
                }
            }
            LogRecord::Ddl { req, sql, extra } => {
                let res = self.apply_ddl(gsn, &sql, &extra);
                self.processed = gsn;
                self.snap_epoch += 1;
                if res.is_ok() {
                    self.catalog.history.push((gsn, sql.clone()));
                    // (every checkpoint saves it; trimmed in steps)
                    if self.catalog.history.len() > MAX_HISTORY + MAX_HISTORY / 10 {
                        let excess = self.catalog.history.len() - MAX_HISTORY;
                        self.catalog.history.drain(..excess);
                    }
                }
                if let Err(e) = &res
                    && self.replaying
                {
                    log::warn!("DDL at gsn {gsn} failed on replay: {e}");
                }
                self.reply_pending(req, res);
                self.drain_dml_queue();
            }
            LogRecord::Progress { source, lsn: _, state } => {
                self.processed = gsn;
                if self.catalog.get(source).is_some() {
                    self.source_states.insert(source, state);
                }
                self.snap_epoch += 1;
            }
            LogRecord::Schema { source: _, table, columns, pk } => {
                let r = self.apply_schema_change(gsn, table, columns, pk);
                self.processed = gsn;
                if let Err(e) = r {
                    log::error!("schema change for table {table} failed: {e}");
                }
            }
            LogRecord::Tick { micros } => {
                // monotonic even if the wall clock steps back
                self.clock = self.clock.max(micros - 946_684_800_000_000);
                self.graph.now = self.clock;
                self.ticks.push_back((gsn, self.clock));
                let frontier = self.env.frontier();
                while self.ticks.len() > 1 && self.ticks[1].0 <= frontier {
                    self.ticks.pop_front();
                }
                // temporal filters release the events that became due
                let outputs = self.graph.step(&mut self.env, gsn, FxHashMap::default())?;
                self.processed = gsn;
                self.deliver(gsn, &outputs);
            }
        }
        self.env.set_frontier(self.processed.saturating_sub(self.cfg.retention_gsns.max(1)));
        self.records_since_checkpoint += 1;
        Ok(())
    }

    fn drain_dml_queue(&mut self) {
        // executed from the main loop via handle_request on the next message;
        // here we only re-enqueue so ordering is preserved
        if self.dml_inflight.is_none()
            && let Some(req) = self.dml_queue.pop_front()
        {
            self.deferred.push(req);
        }
    }

    /// The logical clock as of `gsn` (what now() returns in a read at `gsn`).
    pub(crate) fn clock_at(&self, gsn: Gsn) -> i64 {
        let i = self.ticks.partition_point(|(g, _)| *g <= gsn);
        if i == 0 { self.ticks.front().map(|t| t.1).unwrap_or(self.clock) } else { self.ticks[i - 1].1 }
    }

    /// Route outputs to views (errors) and subscriptions.
    fn deliver(&mut self, gsn: Gsn, outputs: &FxHashMap<NodeId, Output>) {
        for v in self.rt.views.values_mut() {
            if let Some(o) = outputs.get(&v.node) {
                merge_errors(&mut v.errors, &o.errs);
            }
        }
        for s in self.subs.iter() {
            if let Some(o) = outputs.get(&s.node) {
                // a new error record ends the subscription, as it fails
                // reads (a retracted one does not)
                if !o.errs.is_empty() {
                    let mut errs = o.errs.clone();
                    crate::dataflow::consolidate_errs(&mut errs);
                    if let Some(((code, msg), _)) = errs.into_iter().find(|(_, w)| *w > 0) {
                        let _ = s.tx.send(SubEvent::Error(SqlError::new(&code, msg)));
                    }
                }
                if !o.data.is_empty() {
                    let _ = s.tx.send(SubEvent::Data { gsn, rows: o.data.clone() });
                }
            }
            if s.progress {
                let _ = s.tx.send(SubEvent::Progress { gsn });
            }
        }
        for sink in self.sink_handles.values() {
            if let Some(o) = outputs.get(&sink.node) {
                sink.send(gsn, &o.data);
            } else {
                sink.progress(gsn);
            }
        }
    }

    fn prune_subscriptions(&mut self) {
        let mut i = 0;
        while i < self.subs.len() {
            if self.subs[i].cancel.load(Ordering::Acquire) {
                let s = self.subs.swap_remove(i);
                if let Some(n) = s.pinned {
                    let _ = self.graph.release_node(&mut self.env, n);
                }
            } else {
                i += 1;
            }
        }
    }

    fn maybe_checkpoint(&mut self) -> Result<()> {
        if self.records_since_checkpoint == 0 {
            return Ok(());
        }
        let dirty = self.env.dirty_bytes();
        if self.last_checkpoint.elapsed() >= self.cfg.checkpoint_interval || dirty > self.cfg.cache_bytes / 2 {
            self.checkpoint()?;
        }
        Ok(())
    }

    pub(crate) fn checkpoint(&mut self) -> Result<()> {
        let app = AppState {
            catalog: self.catalog.clone(),
            trees: self.graph.arrangement_trees().into_iter().collect(),
            view_errors: self.rt.views.iter().map(|(id, v)| (*id, v.errors.clone().into_iter().collect())).collect(),
            source_states: self.source_states.iter().map(|(k, v)| (*k, v.clone())).collect(),
            source_lsns: self.source_lsns.iter().map(|(k, v)| (*k, *v)).collect(),
            next_req: self.next_req,
            clock: self.clock,
            errors: self.graph.error_state(),
            arr_since: self.graph.arrangement_since().into_iter().collect(),
            ticks: self.ticks.iter().skip(self.ticks.len().saturating_sub(MAX_SAVED_TICKS)).copied().collect(),
        };
        let bytes = serde_json::to_vec(&app).map_err(|e| SqlError::internal(e.to_string()))?;
        self.env.checkpoint(self.processed, &bytes)?;
        self.env.gc(1000)?;
        let keep_from = self.processed.saturating_sub(self.cfg.retention_gsns);
        let _ = self.seq.send(SeqMsg::Truncate(keep_from));
        self.last_checkpoint = Instant::now();
        self.records_since_checkpoint = 0;
        Ok(())
    }

    fn start_sources_and_sinks(&mut self, handle: &EngineHandle) -> Result<()> {
        let objects: Vec<_> = self.catalog.objects.values().cloned().collect();
        for obj in objects {
            match &obj.kind {
                ObjectKind::Source(def) => {
                    let state = self.source_states.get(&obj.id).cloned().unwrap_or_default();
                    let tables = self.source_tables(obj.id);
                    let resume = self.source_lsns.get(&obj.id).copied().unwrap_or(0);
                    let h = crate::source::start(obj.id, obj.name.clone(), def.clone(), tables, state, resume, handle.clone())?;
                    self.source_handles.insert(obj.id, h);
                }
                ObjectKind::Sink(def) => self.start_sink_or_mark_broken(obj.id, &obj.name, def, handle),
                _ => {}
            }
        }
        Ok(())
    }

    /// Start a sink of the catalog; one that cannot start (it reads a
    /// broken view) is marked broken instead.
    pub(crate) fn start_sink_or_mark_broken(&mut self, id: ObjectId, name: &str, def: &crate::catalog::SinkDef, handle: &EngineHandle) {
        self.rt.broken.remove(&id);
        if let Err(e) = self.start_sink(id, name, def, handle) {
            log::error!("sink {name} is broken and not started: {}", e.message);
            self.rt.broken.insert(id, e.message);
        }
    }

    /// The error of using broken object `id`, if it is broken.
    pub(crate) fn broken_error(&self, id: ObjectId) -> Option<SqlError> {
        let reason = self.rt.broken.get(&id)?;
        let what = self.catalog.get(id).map(|o| format!("{} \"{}\"", o.kind.name(), o.name)).unwrap_or_else(|| format!("object {id}"));
        Some(
            SqlError::new(codes::OBJECT_NOT_IN_PREREQUISITE_STATE, format!("{what} is broken: {reason}"))
                .with_hint("Drop it, or replace a broken view with CREATE OR REPLACE MATERIALIZED VIEW."),
        )
    }

    /// Fail if the plan reads a broken relation.
    pub(crate) fn check_broken(&self, rel: &crate::sql::Rel) -> Result<()> {
        if self.rt.broken.is_empty() {
            return Ok(());
        }
        fn gets(r: &crate::sql::Rel, out: &mut Vec<ObjectId>) {
            if let crate::sql::Rel::Get { id, .. } = r {
                out.push(*id);
            }
            for i in r.inputs() {
                gets(i, out);
            }
        }
        let mut ids = Vec::new();
        gets(rel, &mut ids);
        match ids.into_iter().find_map(|id| self.broken_error(id)) {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Tables belonging to a source: (table id, upstream schema, name, columns, pk).
    pub(crate) fn source_tables(&self, source: ObjectId) -> Vec<crate::source::SourceTable> {
        self.catalog
            .objects
            .values()
            .filter_map(|o| match &o.kind {
                ObjectKind::Table(t) if t.source == Some(source) => Some(crate::source::SourceTable {
                    id: o.id,
                    upstream: t.upstream.clone().unwrap_or_default(),
                    columns: t.columns.clone(),
                    pk: t.pk.clone(),
                }),
                _ => None,
            })
            .collect()
    }
}

pub(crate) fn merge_errors(map: &mut BTreeMap<(String, String), i64>, errs: &ErrDelta) {
    for (e, w) in errs {
        let v = map.entry(e.clone()).or_default();
        *v += w;
        if *v == 0 {
            map.remove(e);
        }
    }
}
