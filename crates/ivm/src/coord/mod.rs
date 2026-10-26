//! The coordinator: the engine thread owning storage, dataflow and catalog,
//! fed by the sequencer with durable log records in GSN order.

pub mod api;
mod checkpoint;
pub mod ddl;
mod dml;
mod hydrate;
pub mod inspect;
pub mod meta;
mod read;
mod reader;
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
    /// The memory budget (`--memory-mb`): the buffer pool, group caches and
    /// queues together (`crate::memory`).
    pub memory_bytes: usize,
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
            memory_bytes: crate::memory::DEFAULT_TOTAL,
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
    /// Memory of the events queued for the client (each event holds its
    /// charge until the session drops it).
    pub queue: Arc<crate::memory::Meter>,
    pub cancel: Arc<std::sync::atomic::AtomicBool>,
    /// Nodes pinned for this subscription (ad hoc queries).
    pub pinned: Option<NodeId>,
    /// Changes are held back: its view is being replaced, and it gets the
    /// change to the new version once that is hydrated.
    pub held: bool,
    pub progress: bool,
    /// The materialized view subscribed to by name (its node may be shared
    /// with other views, so dropping it is matched by view, not by node).
    pub view: Option<ObjectId>,
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
    /// The hydration of a relation it reads (re-run when a hydration
    /// finished after this many had).
    Hydration(u64),
}

/// DDL statements kept in the catalog history (`ivm.catalog_history`), the
/// most recent ones (kept in memory and in the metadata tree).
pub const MAX_HISTORY: usize = 10_000;

/// Clock ticks that change nothing but the clock only cost replay time: they
/// make a checkpoint due once this many records were applied since the last
/// one (an hour of ticks at the default interval).
const MAX_TICKS_WITHOUT_CHECKPOINT: u64 = 3600;

/// Most messages the engine handles in one round (before deferred requests,
/// sending the submitted records, and its other work).
const MAX_MESSAGES_PER_ROUND: usize = 256;

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
    /// DML logged but not applied yet, with the rows it writes (`dml.rs`).
    dml_pending: Vec<(u64, dml::Touched)>,
    /// DML waiting for pending DML whose rows it read, in arrival order.
    dml_queue: VecDeque<Request>,
    /// The rows the DML statement being evaluated reads.
    pub(crate) dml_reads: Option<dml::Touched>,
    /// Records submitted in this round of messages, not sent yet.
    outbox: Vec<(u64, LogRecord)>,
    /// Threads running large reads on forks (`reader.rs`), started when
    /// first needed.
    readers: Option<Sender<reader::ReadJob>>,
    /// Snapshots being taken on reader threads for subscriptions and sinks.
    pub(crate) snapshotting: Vec<reader::Snapshotting>,
    /// Command tags of DML waiting to be applied.
    pub(crate) dml_counts: HashMap<u64, String>,
    /// Requests to run after the current message.
    deferred: Vec<Request>,
    last_checkpoint: Instant,
    records_since_checkpoint: u64,
    /// Records since the last checkpoint other than clock ticks that changed
    /// nothing but the clock (and 1 when its maintenance pass left work).
    changes_since_checkpoint: u64,
    /// What the metadata tree holds.
    pub(crate) meta: meta::Meta,
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
    /// The hydration in progress off the engine thread (`hydrate.rs`).
    pub(crate) hydration: Option<hydrate::Hydration>,
    /// What waits for the hydration to begin after it (statements applied
    /// while it was in progress whose dataflow is still pending).
    pub(crate) hydration_next: Vec<hydrate::Then>,
    /// Hydrations finished so far.
    pub(crate) hydrations_done: u64,
    /// What the record being applied leaves for after the hydration of what
    /// it built (`hydrate::Then`).
    pub(crate) record_then: Vec<hydrate::Then>,
    /// Sinks whose changes are held back: their view is being replaced and
    /// they get the change to its new version once it is hydrated.
    pub(crate) held_sinks: rustc_hash::FxHashSet<ObjectId>,
    /// Durable records not applied yet: one that must wait for the
    /// hydration in progress (or for snapshots being taken) and those after
    /// it.
    held: VecDeque<(Gsn, LogRecord)>,
    /// The checkpoint being written in the background (`checkpoint.rs`).
    checkpointing: Option<checkpoint::InFlight>,
    /// Requests for a checkpoint, answered when the next one is committed.
    checkpoint_waiters: Vec<checkpoint::Waiter>,
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

    /// Append a record without waiting for it to be durable; the receiver
    /// yields its GSN once it is. Waits first while the records the engine
    /// has yet to apply fill their part of the memory budget: this is the
    /// backpressure on sources, in bytes of memory.
    pub fn log_async(&self, rec: LogRecord) -> Result<Receiver<Gsn>> {
        let bytes = crate::memory::record_bytes(&rec);
        if !self.shared.mem.admit_input(bytes, || self.shared.shutdown.load(Ordering::Acquire)) {
            return Err(SqlError::new(codes::ADMIN_SHUTDOWN, "engine is shutting down"));
        }
        let (tx, rx) = bounded(1);
        if self.seq.send(SeqMsg::Append { rec, ack: Some(tx) }).is_err() {
            self.shared.mem.input.sub(bytes);
            return Err(SqlError::new(codes::ADMIN_SHUTDOWN, "sequencer stopped"));
        }
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

    /// Stop without the final checkpoint, as if the process had died right
    /// after its last log write: the next start recovers the last
    /// checkpoint and replays the log after it (for tests).
    pub fn crash(self) {
        self.handle.shared.crash.store(true, Ordering::Release);
        self.shutdown();
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
    // before anything is opened (and a torn log tail truncated): a newer
    // format is refused untouched; a directory without a format version is
    // new, or of format 1, migrated below
    let migrate = match meta::read_format(&cfg.data_dir)? {
        Some(v) => v < meta::FORMAT_VERSION,
        None if meta::holds_data(&cfg.data_dir)? => true,
        None => {
            meta::write_format(&cfg.data_dir)?;
            false
        }
    };
    let shared = Arc::new(Shared::default());
    shared.mem.set_total(cfg.memory_bytes);
    let (etx, erx) = unbounded::<EngineMsg>();
    let (stx, srx) = unbounded::<SeqMsg>();
    let mut writer = LogWriter::open(&cfg.data_dir.join("log"), cfg.sync)?;
    let (env, recovered) = Env::open(
        &cfg.data_dir.join("store"),
        EnvOptions { cache_bytes: shared.mem.pool_target(0), ..Default::default() },
    )?;
    let mut engine = Engine::new(cfg.clone(), env, stx.clone(), shared.clone());
    engine.recover(recovered)?;
    // Sinks start from the checkpoint's state and receive the replayed
    // records like live ones: a target that is as far as the checkpoint
    // (it is after a clean shutdown) resumes without being rewritten.
    let handle = EngineHandle { tx: etx.clone(), seq: stx.clone(), shared: shared.clone() };
    engine.handle = Some(handle.clone());
    engine.start_sinks(&handle)?;
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
    if migrate {
        // the recovered state in the current format; until FORMAT records
        // it, a restart migrates again (from whichever record is last)
        engine.checkpoint()?;
        meta::write_format(&cfg.data_dir)?;
        log::info!("migrated the data directory to format {}", meta::FORMAT_VERSION);
    }
    engine.start_sources(&handle)?;
    // slot drops left from before the restart (or replayed DROP SOURCEs)
    crate::source::resume_slot_drops(&shared);
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
    /// An engine with an empty catalog over `env`, before recovery.
    fn new(cfg: Config, mut env: Env, seq: Sender<SeqMsg>, shared: Arc<Shared>) -> Engine {
        let graph = Graph::new();
        graph.group_caches.set_limit(shared.mem.cache_limit());
        // sort buffers and per-GSN deltas draw from the budget's share
        env.set_work(shared.mem.work.clone());
        Engine {
            cfg,
            env,
            graph,
            catalog: Catalog::new(),
            rt: Runtime::default(),
            processed: 0,
            seq,
            shared,
            pending: HashMap::new(),
            parked: Vec::new(),
            subs: Vec::new(),
            next_req: 1,
            dml_pending: Vec::new(),
            dml_queue: VecDeque::new(),
            dml_reads: None,
            outbox: Vec::new(),
            readers: None,
            snapshotting: Vec::new(),
            dml_counts: HashMap::new(),
            deferred: Vec::new(),
            last_checkpoint: Instant::now(),
            records_since_checkpoint: 0,
            changes_since_checkpoint: 0,
            meta: meta::Meta::default(),
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
            hydration: None,
            hydration_next: Vec::new(),
            hydrations_done: 0,
            record_then: Vec::new(),
            held_sinks: Default::default(),
            held: VecDeque::new(),
            checkpointing: None,
            checkpoint_waiters: Vec::new(),
        }
    }

    fn recover(&mut self, recovered: Option<crate::storage::Recovered>) -> Result<()> {
        let Some(rec) = recovered else { return Ok(()) };
        let (app, meta) = meta::load(&mut self.env, &rec.app)?;
        // (format 1 kept the state in the record: the tree holds nothing yet)
        let mirrored = meta.tree.is_some();
        self.meta = meta;
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
        self.shared.slot_drops.lock().extend(app.slot_drops);
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
        self.graph.restore_error_state(app.errors, mirrored);
        for (id, errs) in app.view_errors {
            if let Some(v) = self.rt.views.get(&id) {
                self.graph.restore_view_errors(v.node, &errs);
            }
        }
        // anything that could not be matched to a recovered tree is rebuilt
        // from the recovered state
        self.graph.hydrate(&mut self.env, self.processed)?;
        // a view's error records are those of its root
        for v in self.rt.views.values_mut() {
            v.errors = self.graph.errors_of(v.node).into_iter().collect();
        }
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
        'run: loop {
            let first = match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(m) => Some(m),
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => None,
                Err(_) => break,
            };
            // with the messages already waiting, so that the statements
            // among them are logged together (one log append for all)
            let mut msgs: VecDeque<Option<EngineMsg>> = VecDeque::from([first]);
            while msgs.len() < MAX_MESSAGES_PER_ROUND
                && let Ok(m) = rx.try_recv()
            {
                msgs.push_back(Some(m));
            }
            for msg in msgs {
                match msg {
                    Some(EngineMsg::Durable(batch)) => {
                        self.held.extend(batch);
                        self.apply_durable(&handle)?;
                    }
                    Some(EngineMsg::Request(req)) => self.handle_request(req, &handle),
                    Some(EngineMsg::Shutdown) => {
                        self.flush_outbox();
                        break 'run;
                    }
                    Some(EngineMsg::Wake) | None => self.wake_parked(&handle),
                }
            }
            while !self.deferred.is_empty() {
                let reqs = std::mem::take(&mut self.deferred);
                for r in reqs {
                    self.handle_request(r, &handle);
                }
            }
            self.flush_outbox();
            if !self.snapshotting.is_empty() {
                self.poll_snapshots()?;
                self.apply_durable(&handle)?;
            }
            self.poll_hydration(&handle)?;
            self.poll_checkpoint(false)?;
            self.rebalance_memory();
            self.restart_lagging_sinks();
            self.maybe_checkpoint()?;
            self.prune_subscriptions();
        }
        // ask sources and sinks to stop, without waiting for them here
        let mut threads: Vec<JoinHandle<()>> = self.source_handles.drain().filter_map(|(_, s)| s.stop()).collect();
        for (_, s) in self.sink_handles.drain() {
            // everything applied is delivered: the sink records it as its
            // progress, matching the final checkpoint, so that the next
            // start does not resynchronize it
            s.progress(self.processed);
            threads.extend(s.stop());
        }
        let _ = self.seq.send(SeqMsg::Shutdown);
        if !self.shared.crash.load(Ordering::Acquire) {
            self.checkpoint()?;
        } else {
            // (no writer may outlive the engine: a restart opens the store)
            self.abandon_checkpoint();
        }
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

    /// Apply the durable records received, in GSN order, up to one that
    /// must wait for the hydration in progress (`must_wait_for_hydration`;
    /// it is applied once no hydration is).
    fn apply_durable(&mut self, handle: &EngineHandle) -> Result<()> {
        let mut applied = false;
        while let Some((_, rec)) = self.held.front() {
            // (and DDL while snapshots are taken for the subscriptions and
            // sinks it could end or replace)
            if (self.hydration.is_some() && self.must_wait_for_hydration(rec))
                || (!self.snapshotting.is_empty() && matches!(rec, LogRecord::Ddl { .. } | LogRecord::Schema { .. }))
            {
                break;
            }
            let (gsn, rec) = self.held.pop_front().unwrap();
            applied = true;
            let dml = match &rec {
                LogRecord::Commit { source: 0, lsn, .. } => Some(*lsn),
                _ => None,
            };
            // (counted from its submission until it is applied)
            let bytes = crate::memory::record_bytes(&rec);
            let r = self.apply_record(gsn, rec);
            self.shared.mem.input.sub(bytes);
            self.rebalance_memory();
            if let Err(e) = r {
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
        if applied {
            self.shared.processed_gsn.store(self.processed, Ordering::Release);
            self.wake_parked(handle);
        }
        Ok(())
    }

    pub(crate) fn alloc_req(&mut self) -> u64 {
        let r = self.next_req;
        self.next_req += 1;
        r
    }

    /// Submit a record to the sequencer; `reply` is answered when it is
    /// applied. (Sent at the end of the engine's round of messages, with the
    /// other statements' records: `flush_outbox`.)
    pub(crate) fn submit(&mut self, rec: LogRecord, req: u64, reply: Sender<Result<ExecResult>>) {
        self.pending.insert(req, reply);
        self.outbox.push((req, rec));
        if self.handle.is_none() {
            self.flush_outbox();
        }
    }

    /// Send the submitted records to the sequencer in one message: it
    /// appends them in one batch, with one write and one `fdatasync`. They
    /// are counted in the input queue's memory without waiting for room
    /// (the engine is what empties the queue).
    fn flush_outbox(&mut self) {
        if self.outbox.is_empty() {
            return;
        }
        let (reqs, recs): (Vec<u64>, Vec<LogRecord>) = std::mem::take(&mut self.outbox).into_iter().unzip();
        let bytes: usize = recs.iter().map(crate::memory::record_bytes).sum();
        self.shared.mem.input.add(bytes);
        if self.seq.send(SeqMsg::AppendAll(recs)).is_err() {
            self.shared.mem.input.sub(bytes);
            for req in reqs {
                self.reply_pending(req, Err(SqlError::new(codes::ADMIN_SHUTDOWN, "sequencer stopped")));
            }
            self.drain_dml_queue();
        }
    }

    /// Hand a record to the sequencer, counted in the input queue's memory
    /// but without waiting for room: the engine is what empties the queue.
    fn append(&mut self, rec: LogRecord) -> bool {
        let bytes = crate::memory::record_bytes(&rec);
        self.shared.mem.input.add(bytes);
        let sent = self.seq.send(SeqMsg::Append { rec, ack: None }).is_ok();
        if !sent {
            self.shared.mem.input.sub(bytes);
        }
        sent
    }

    pub(crate) fn reply_pending(&mut self, req: u64, res: Result<ExecResult>) {
        if let Some(r) = self.pending.remove(&req) {
            let _ = r.send(res);
        }
        self.dml_pending.retain(|(r, _)| *r != req);
    }

    fn handle_request(&mut self, req: Request, handle: &EngineHandle) {
        match req {
            Request::Execute { ref stmt, .. } if self.is_dml(stmt) => {
                // (behind DML that is waiting: in arrival order)
                if self.dml_queue.is_empty() {
                    self.run_execute(req, handle);
                } else {
                    self.dml_queue.push_back(req);
                }
            }
            Request::Checkpoint { reply } => self.request_checkpoint(checkpoint::Waiter::Request(reply)),
            Request::Log { rec } => {
                self.append(rec);
            }
            Request::Inspect(r) => self.inspect(r),
            other => self.run_execute(other, handle),
        }
    }

    fn run_execute(&mut self, req: Request, handle: &EngineHandle) {
        match req {
            Request::Execute { stmt, params, vars, extra, reply } => {
                // (a statement reading a relation still being hydrated is
                // parked until it is done: kept to be run again)
                let retry = (self.hydration.is_some() || (!self.dml_pending.is_empty() && self.is_dml(&stmt)))
                    .then(|| (stmt.clone(), params.clone(), vars.clone(), extra.clone()));
                // (current_setting() in the statement reads the session's settings)
                let settings = vars.settings.clone();
                let r = crate::expr::with_session_settings(settings, || self.execute(*stmt, params, vars, extra, reply.clone(), handle));
                match r {
                    Ok(Some(res)) => {
                        let _ = reply.send(Ok(res));
                    }
                    Ok(None) => {} // deferred (submitted or parked)
                    Err(e) if e.code == hydrate::HYDRATING && retry.is_some() => {
                        let (stmt, params, vars, extra) = retry.unwrap();
                        let wait = Wait::Hydration(self.hydrations_done);
                        self.parked.push(Parked { wait, req: Request::Execute { stmt, params, vars, extra, reply } });
                    }
                    // (it read rows pending DML writes: evaluated again after it,
                    // first among the DML waiting)
                    Err(e) if e.code == dml::WAIT && retry.is_some() => {
                        let (stmt, params, vars, extra) = retry.unwrap();
                        self.dml_queue.push_front(Request::Execute { stmt, params, vars, extra, reply });
                    }
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
        // (false for a tick that changed nothing but the clock)
        let mut changed = true;
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
                if self.hydration.is_some() {
                    self.forward_to_hydration(gsn, &by_node)?;
                }
                let observed = self.delivered_nodes();
                let outputs = self.graph.step_observed(&mut self.env, gsn, by_node, observed)?;
                self.processed = gsn;
                self.deliver(gsn, outputs);
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
                let before = self.graph.pending();
                let res = self.apply_ddl(gsn, &sql, &extra);
                self.processed = gsn;
                self.snap_epoch += 1;
                if res.is_ok() {
                    self.catalog.history.push((gsn, sql.clone()));
                    // (trimmed in steps)
                    if self.catalog.history.len() > MAX_HISTORY + MAX_HISTORY / 10 {
                        let excess = self.catalog.history.len() - MAX_HISTORY;
                        self.catalog.history.drain(..excess);
                    }
                }
                // the catalog in the metadata tree as of this GSN
                self.persist_meta(gsn)?;
                if let Err(e) = &res
                    && self.replaying
                {
                    log::warn!("DDL at gsn {gsn} failed on replay: {e}");
                }
                // what it built is hydrated (in the background, or after the
                // hydration in progress): the reply waits for it
                let mut then = std::mem::take(&mut self.record_then);
                if then.is_empty() && self.graph.pending() == before {
                    self.reply_pending(req, res);
                } else {
                    then.push(hydrate::Then::Reply { req, res });
                    self.hydrate(gsn, then)?;
                }
                self.drain_dml_queue();
            }
            LogRecord::Progress { source, lsn: _, state } => {
                self.processed = gsn;
                if self.catalog.get(source).is_some() {
                    self.source_states.insert(source, state);
                }
                self.snap_epoch += 1;
            }
            LogRecord::Schema { source, table, columns, pk } => {
                let r = self.apply_schema_change(gsn, table, columns, pk);
                self.processed = gsn;
                let then = std::mem::take(&mut self.record_then);
                self.hydrate(gsn, then)?;
                // The source snapshots the table again to fill in the new
                // columns. It logs its reset state (with the truncate a key
                // change needs) just before this record; the record resets
                // the state as well, keeping that truncate, so that a restart
                // after it never takes the snapshot as done (records logged
                // by earlier versions reset the state after the schema
                // record).
                if let Some(state) = self.source_states.get_mut(&source) {
                    *state = crate::source::postgres::snapshot_again(state, table);
                }
                self.snap_epoch += 1;
                if let Err(e) = r {
                    log::error!("schema change for table {table} failed: {e}");
                }
                self.persist_meta(gsn)?;
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
                if self.hydration.is_some() {
                    self.forward_to_hydration(gsn, &FxHashMap::default())?;
                }
                // temporal filters release the events that became due
                let observed = self.delivered_nodes();
                let rows_before = self.graph.step_totals.out_rows;
                let outputs = self.graph.step_observed(&mut self.env, gsn, FxHashMap::default(), observed)?;
                self.processed = gsn;
                // (outputs hold the rows of observed nodes only)
                changed = self.graph.step_totals.out_rows != rows_before || !outputs.is_empty();
                self.deliver(gsn, outputs);
            }
        }
        self.env.set_frontier(self.processed.saturating_sub(self.cfg.retention_gsns.max(1)));
        self.records_since_checkpoint += 1;
        self.changes_since_checkpoint += changed as u64;
        Ok(())
    }

    /// Evaluate the waiting DML again (some pending DML was applied): from
    /// the main loop, in order (`handle_request` queues the others again
    /// behind the first that still has to wait).
    fn drain_dml_queue(&mut self) {
        self.deferred.extend(self.dml_queue.drain(..));
    }

    /// The logical clock as of `gsn` (what now() returns in a read at `gsn`).
    pub(crate) fn clock_at(&self, gsn: Gsn) -> i64 {
        let i = self.ticks.partition_point(|(g, _)| *g <= gsn);
        if i == 0 { self.ticks.front().map(|t| t.1).unwrap_or(self.clock) } else { self.ticks[i - 1].1 }
    }

    /// Nodes whose rows `deliver` hands on (subscriptions and sinks); of the
    /// other nodes it only needs the error records.
    fn delivered_nodes(&self) -> rustc_hash::FxHashSet<NodeId> {
        self.subs.iter().map(|s| s.node).chain(self.sink_handles.values().map(|s| s.node)).collect()
    }

    /// Route outputs to views (errors), subscriptions and sinks. Each node's
    /// changes are delivered as one copy shared by all its consumers, and
    /// each consumer's queue is bounded in bytes: a subscription whose
    /// client does not keep up ends with an error, a sink that does not
    /// keep up is restarted from a new snapshot once it has caught up with
    /// what it holds.
    fn deliver(&mut self, gsn: Gsn, mut outputs: FxHashMap<NodeId, Output>) {
        for v in self.rt.views.values_mut() {
            if let Some(o) = outputs.get(&v.node) {
                merge_errors(&mut v.errors, &o.errs);
            }
        }
        if !self.snapshotting.is_empty() {
            self.keep_for_snapshots(gsn, &outputs);
        }
        let limit = self.shared.mem.consumer_limit();
        let mut shared = FxHashMap::default();
        // (held: their view is being replaced, and they get the change to
        // its new version once that is hydrated)
        for s in self.subs.iter().filter(|s| !s.held) {
            let errs = outputs.get(&s.node).map(|o| o.errs.clone()).unwrap_or_default();
            deliver_to(s, gsn, &errs, shared_rows(&mut shared, &mut outputs, s.node), limit);
        }
        for (id, sink) in self.sink_handles.iter_mut() {
            if sink.behind() || self.held_sinks.contains(id) {
                continue;
            }
            let sent = match shared_rows(&mut shared, &mut outputs, sink.node) {
                Some((rows, bytes)) => sink.send(gsn, &rows, bytes, limit),
                None => {
                    sink.progress(gsn);
                    true
                }
            };
            if !sent {
                log::warn!(
                    "sink {id} fell behind: its unwritten changes reached {} MiB; it is restarted from a new snapshot once it has written them",
                    limit >> 20
                );
            }
        }
    }

    /// Restart the sinks that fell behind and have written (or dropped)
    /// what they held: from a new snapshot, which they write over their
    /// targets (they were sent nothing since they fell behind, so their
    /// targets' progress is older).
    fn restart_lagging_sinks(&mut self) {
        let Some(handle) = self.handle.clone() else { return };
        let idle: Vec<ObjectId> = self.sink_handles.iter().filter(|(_, h)| h.behind_and_idle()).map(|(id, _)| *id).collect();
        for id in idle {
            let Some(h) = self.sink_handles.remove(&id) else { continue };
            // (it has nothing left to write: no need to wait for it)
            let _ = h.stop();
            if let Some(obj) = self.catalog.get(id).cloned()
                && let ObjectKind::Sink(def) = &obj.kind
            {
                log::info!("restarting sink {} from a snapshot at gsn {}", obj.name, self.processed);
                self.start_sink_or_mark_broken(id, &obj.name, def, &handle);
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

    /// Give the buffer pool what the other parts of the memory budget leave
    /// over (the next access evicts down to it), and shrink the group caches
    /// if they ran out of room.
    pub(crate) fn rebalance_memory(&mut self) {
        let target = self.shared.mem.pool_target(self.graph.group_caches.used());
        self.env.set_cache_bytes(target);
        crate::dataflow::reduce::trim_caches(&self.graph);
    }

    /// Begin a checkpoint in the background (`checkpoint.rs`) when one is
    /// asked for, or due: every `checkpoint_interval` (or as soon as dirty
    /// nodes still exceed half the pool's share of the memory budget, which
    /// the pool's early writes keep them below) once something durable
    /// changed: records other than clock ticks that changed nothing but the
    /// clock — or the last checkpoint's maintenance pass left space to
    /// reclaim for lack of budget, which the next one continues. Ticks alone
    /// only add replay work: they make one due after
    /// MAX_TICKS_WITHOUT_CHECKPOINT of them. One checkpoint at a time, and
    /// none while a table is rebuilt.
    fn maybe_checkpoint(&mut self) -> Result<()> {
        if self.checkpointing.is_some() || self.rebuilding_table() {
            return Ok(());
        }
        let asked = !self.checkpoint_waiters.is_empty();
        if !asked && self.changes_since_checkpoint == 0 && self.records_since_checkpoint < MAX_TICKS_WITHOUT_CHECKPOINT {
            return Ok(());
        }
        let dirty = self.env.dirty_bytes();
        if asked || self.last_checkpoint.elapsed() >= self.cfg.checkpoint_interval || dirty > self.env.cache_capacity() / 2 {
            self.begin_checkpoint()?;
        }
        Ok(())
    }

    /// Take a checkpoint now, on the engine thread, after the one in
    /// progress (at shutdown, and in an engine without threads).
    pub(crate) fn checkpoint(&mut self) -> Result<()> {
        self.poll_checkpoint(true)?;
        if self.rebuilding_table() {
            // (replay rebuilds it from the schema record after the last one)
            log::info!("no checkpoint while a table is rebuilt after a schema change");
            return Ok(());
        }
        let header = self.prepare_checkpoint()?;
        self.env.checkpoint(self.processed, &header)?;
        self.after_checkpoint(self.processed);
        Ok(())
    }

    /// What every checkpoint does on the engine thread before the dirty
    /// nodes are frozen: write what changed since the last one into the
    /// metadata tree (the manifest record names that tree in a small
    /// header, returned), and reclaim space, so that the checkpoint writes
    /// what the maintenance pass rewrote. Records applied from now on count
    /// towards the next checkpoint.
    pub(crate) fn prepare_checkpoint(&mut self) -> Result<Vec<u8>> {
        self.persist_meta(self.processed)?;
        let maint = self.env.maintain()?;
        self.last_checkpoint = Instant::now();
        self.records_since_checkpoint = 0;
        // (an idle engine goes on reclaiming, a budget per checkpoint, until
        // a pass finds nothing left)
        self.changes_since_checkpoint = maint.unfinished as u64;
        Ok(meta::encode_header(&self.meta, self.next_req))
    }

    /// Is a table being rebuilt in the background (`apply_schema_change`)?
    /// No checkpoint is taken meanwhile: its rows are in neither version's
    /// index alone, and a table's rows exist nowhere else.
    fn rebuilding_table(&self) -> bool {
        self.hydration.is_some() && self.graph.live_nodes().any(|n| !n.hydrated && matches!(n.kind, crate::dataflow::NodeKind::Table { .. }))
    }

    /// Once a checkpoint at `gsn` is committed: recovery replays the log
    /// after it, and nothing else reads the log.
    pub(crate) fn after_checkpoint(&mut self, gsn: Gsn) {
        let _ = self.seq.send(SeqMsg::Truncate(gsn));
    }

    fn start_sources(&mut self, handle: &EngineHandle) -> Result<()> {
        let objects: Vec<_> = self.catalog.objects.values().cloned().collect();
        for obj in objects {
            if let ObjectKind::Source(def) = &obj.kind {
                let state = self.source_states.get(&obj.id).cloned().unwrap_or_default();
                let tables = self.source_tables(obj.id);
                let resume = self.source_lsns.get(&obj.id).copied().unwrap_or(0);
                let h = crate::source::start(obj.id, obj.name.clone(), def.clone(), tables, state, resume, handle.clone())?;
                self.source_handles.insert(obj.id, h);
            }
        }
        Ok(())
    }

    /// Start the sinks of the catalog (before the log is replayed; sinks
    /// created by replayed records start when those are applied).
    fn start_sinks(&mut self, handle: &EngineHandle) -> Result<()> {
        let objects: Vec<_> = self.catalog.objects.values().cloned().collect();
        for obj in objects {
            if let ObjectKind::Sink(def) = &obj.kind {
                // (one that cannot start, e.g. over a broken view, is marked broken)
                self.start_sink_or_mark_broken(obj.id, &obj.name, def, handle);
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

/// Bytes of a subscription event without rows in its queue.
const SUB_EVENT_BYTES: usize = std::mem::size_of::<SubEvent>() + 8;

/// The changes of `node` in `outputs`, moved into one shared copy (made at
/// the first consumer that asks) with its bytes in memory; None if empty.
fn shared_rows(
    shared: &mut FxHashMap<NodeId, (Arc<crate::dataflow::Delta>, usize)>,
    outputs: &mut FxHashMap<NodeId, Output>,
    node: NodeId,
) -> Option<(Arc<crate::dataflow::Delta>, usize)> {
    if let Some((rows, bytes)) = shared.get(&node) {
        return Some((rows.clone(), *bytes));
    }
    let o = outputs.get_mut(&node).filter(|o| !o.data.is_empty())?;
    let rows = Arc::new(std::mem::take(&mut o.data));
    let bytes = crate::memory::delta_bytes(&rows);
    shared.insert(node, (rows.clone(), bytes));
    Some((rows, bytes))
}

/// An output's rows as a copy of their own, with their bytes (for one
/// consumer); None if empty.
pub(crate) fn own_rows(rows: &crate::dataflow::Delta) -> Option<(Arc<crate::dataflow::Delta>, usize)> {
    (!rows.is_empty()).then(|| {
        let rows = Arc::new(rows.clone());
        let bytes = crate::memory::delta_bytes(&rows);
        (rows, bytes)
    })
}

/// Deliver to subscription `s` its node's output at `gsn`: error records
/// `errs` and the rows (shared, with their bytes), then a progress mark if
/// it asked for them. Every event counts in its queue's memory: one whose
/// client does not read as fast ends with 53000 instead.
pub(crate) fn deliver_to(s: &Subscription, gsn: Gsn, errs: &[((String, String), i64)], rows: Option<(Arc<crate::dataflow::Delta>, usize)>, limit: usize) {
    if s.cancel.load(Ordering::Acquire) {
        return;
    }
    // a new error record ends the subscription, as it fails reads (a
    // retracted one does not)
    if !errs.is_empty() {
        let mut errs = errs.to_vec();
        crate::dataflow::consolidate_errs(&mut errs);
        if let Some(((code, msg), _)) = errs.into_iter().find(|(_, w)| *w > 0) {
            let _ = s.tx.send(SubEvent::Error(SqlError::new(&code, msg)));
        }
    }
    if let Some((rows, bytes)) = rows
        && !send_rows(s, gsn, &rows, bytes, limit)
    {
        return;
    }
    if s.progress {
        if s.queue.used() + SUB_EVENT_BYTES > limit {
            return end_slow_subscription(s, SUB_EVENT_BYTES, limit);
        }
        let _ = s.tx.send(SubEvent::Progress { gsn, charge: s.queue.charge(SUB_EVENT_BYTES) });
    }
}

/// Send rows to subscription `s`, counted in its queue's memory; false if
/// that ended it (its client does not read as fast).
pub(crate) fn send_rows(s: &Subscription, gsn: Gsn, rows: &Arc<crate::dataflow::Delta>, bytes: usize, limit: usize) -> bool {
    if s.queue.used() + bytes > limit {
        end_slow_subscription(s, bytes, limit);
        return false;
    }
    let _ = s.tx.send(SubEvent::Data { gsn, rows: SharedRows::shared(rows, s.queue.charge(bytes)) });
    true
}

/// End a subscription whose queue would exceed its limit with `more`
/// bytes: its client does not read as fast as changes come.
fn end_slow_subscription(s: &Subscription, more: usize, limit: usize) {
    let queued = s.queue.used() + more;
    let _ = s.tx.send(SubEvent::Error(
        SqlError::new(
            codes::INSUFFICIENT_RESOURCES,
            format!(
                "subscription ended: its client does not read changes as fast as they come ({:.1} MiB queued, the limit is {:.1} MiB)",
                queued as f64 / (1 << 20) as f64,
                limit as f64 / (1 << 20) as f64
            ),
        )
        .with_hint("Read subscriptions promptly, or give the server more memory (--memory-mb; each subscription may queue a sixteenth of it)."),
    ));
    s.cancel.store(true, Ordering::Release);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::SubscribeTarget;

    /// An engine on a scratch store without its threads: statements are
    /// applied by calling it directly, and nothing is logged.
    fn engine(dir: &std::path::Path) -> Engine {
        let (env, _) = Env::open(&dir.join("store"), EnvOptions::default()).unwrap();
        let (seq, _) = unbounded();
        let mut e = Engine::new(Config { data_dir: dir.to_path_buf(), ..Default::default() }, env, seq, Arc::new(Shared::default()));
        e.replaying = false;
        e
    }

    /// Sources wait for room before submitting a record: by the bytes the
    /// records still to be applied take in memory (an eighth of the
    /// budget), not by their number (10,000 commits of any size per source
    /// were let in, and the sequencer's queue was unbounded).
    #[test]
    fn source_records_wait_for_room_by_their_bytes() {
        use crate::dataflow::TableChange;
        use crate::memory::record_bytes;
        fn rec(kb: usize) -> LogRecord {
            let row = vec![crate::types::Datum::Text("x".repeat(kb << 10).into())];
            LogRecord::Commit { source: 1, lsn: 0, changes: vec![(1, TableChange::Insert { row })] }
        }
        let shared = Arc::new(Shared::default());
        shared.mem.set_total(64 << 20);
        assert_eq!(shared.mem.input_limit(), 8 << 20);
        let (seq, seq_rx) = unbounded();
        let (tx, _rx) = unbounded();
        let h = EngineHandle { tx, seq, shared: shared.clone() };
        // the engine applying what was submitted
        let apply_all = || {
            while let Ok(SeqMsg::Append { rec, .. }) = seq_rx.try_recv() {
                shared.mem.input.sub(record_bytes(&rec));
            }
        };
        // small records: many more than 10,000 get in
        for _ in 0..20_000 {
            h.log_async(rec(0)).unwrap();
        }
        assert!(shared.mem.input.used() < 8 << 20);
        apply_all();
        assert_eq!(shared.mem.input.used(), 0);
        // 3 MiB records: the third gets in while the queue is below the
        // limit, the fourth waits until the engine applies some
        for _ in 0..3 {
            h.log_async(rec(3 << 10)).unwrap();
        }
        let h2 = h.clone();
        let waiting = std::thread::spawn(move || h2.log_async(rec(3 << 10)).is_ok());
        std::thread::sleep(Duration::from_millis(100));
        assert!(!waiting.is_finished(), "a record got in with {} bytes queued", shared.mem.input.used());
        apply_all();
        assert!(waiting.join().unwrap());
        apply_all();
        // a shutdown releases a waiting source
        for _ in 0..3 {
            h.log_async(rec(3 << 10)).unwrap();
        }
        let h2 = h.clone();
        let waiting = std::thread::spawn(move || h2.log_async(rec(3 << 10)).map_err(|e| e.code));
        std::thread::sleep(Duration::from_millis(50));
        shared.shutdown.store(true, Ordering::Release);
        assert_eq!(waiting.join().unwrap().unwrap_err(), codes::ADMIN_SHUTDOWN);
    }

    /// The group caches of all aggregations share one byte budget: they
    /// never hold more than it, and the cache of an aggregation that no
    /// longer changes gives way to one in use (each Reduce used to cache up
    /// to 2^18 groups of any size, never giving any back).
    #[test]
    fn group_caches_share_a_byte_budget() {
        use crate::dataflow::{NodeKind, TableChange};
        use crate::types::Datum;
        let tmp = crate::testkit::TempDir::new("coord-group-caches");
        let mut e = engine(tmp.path());
        let limit = 1 << 20;
        e.graph.group_caches.set_limit(limit);
        let ddl = |sql: &str| LogRecord::Ddl { req: 0, sql: sql.into(), extra: String::new() };
        e.apply_record(1, ddl("CREATE TABLE ta (id int PRIMARY KEY, g int)")).unwrap();
        e.apply_record(2, ddl("CREATE TABLE tb (id int PRIMARY KEY, g int)")).unwrap();
        // (hydrated as the records are applied: inline, without threads)
        e.apply_record(3, ddl("CREATE MATERIALIZED VIEW va AS SELECT g, count(*) AS n, sum(id) AS s FROM ta GROUP BY g")).unwrap();
        e.apply_record(4, ddl("CREATE MATERIALIZED VIEW vb AS SELECT g, count(*) AS n, sum(id) AS s FROM tb GROUP BY g")).unwrap();
        let ids = |e: &Engine| ["ta", "tb"].map(|n| e.catalog.by_name(n).unwrap().id);
        let [ta, tb] = ids(&e);
        let cache_bytes = |e: &Engine, view: &str| {
            let node = e.rt.views[&e.catalog.by_name(view).unwrap().id].node;
            let mut n = node;
            // the view's node, or the Reduce under its projection
            loop {
                match &e.graph.node(n).kind {
                    NodeKind::Reduce(p) => return p.cache.bytes(),
                    _ => n = e.graph.node(n).inputs[0],
                }
            }
        };
        let mut gsn = 4;
        let mut insert = |e: &mut Engine, table, from: i32, version: i32| {
            for chunk in 0..20 {
                gsn += 1;
                let changes = (from + chunk * 1000..from + (chunk + 1) * 1000)
                    .map(|i| (table, TableChange::Upsert { row: vec![Datum::Int4(i), Datum::Int4(i + version)], unchanged: vec![] }))
                    .collect();
                e.apply_record(gsn, LogRecord::Commit { source: 1, lsn: 0, changes }).unwrap();
                e.rebalance_memory();
                assert!(e.graph.group_caches.used() <= limit, "{} bytes cached", e.graph.group_caches.used());
            }
        };
        // 20,000 groups: more than the budget holds
        insert(&mut e, ta, 0, 0);
        let a = cache_bytes(&e, "va");
        assert!(a > limit / 2, "va caches {a} bytes");
        // only vb changes from now on: va's cache gives way
        for round in 0..3 {
            insert(&mut e, tb, 0, round);
        }
        let (a, b) = (cache_bytes(&e, "va"), cache_bytes(&e, "vb"));
        assert!(b > 4 * a, "vb caches {b} bytes, the idle va {a}");
        assert_eq!(e.graph.group_caches.used(), a + b);
    }

    /// A subscription whose query fails to lower releases the operators
    /// built before the failure (create_view_rt did; subscribe left them in
    /// the graph, where they stayed and were maintained for good).
    #[test]
    fn a_subscription_that_fails_to_lower_releases_what_it_built() {
        let tmp = crate::testkit::TempDir::new("coord-sub-lowering");
        let mut e = engine(tmp.path());
        e.apply_ddl(1, "CREATE TABLE t (id int PRIMARY KEY, a int)", "").unwrap();
        e.apply_ddl(2, "CREATE MATERIALIZED VIEW m AS SELECT id, a FROM t", "").unwrap();
        e.processed = 2;
        // m without its dataflow: a query reading it fails to lower after
        // the operators of the aggregate before it were built
        let m = e.catalog.by_name("m").unwrap().id;
        e.rt.views.remove(&m);
        let (nodes, arrs) = (e.graph.live_nodes().count(), e.graph.live_arrangements().count());
        let text = "SELECT s.a, s.n FROM (SELECT a, count(*) AS n FROM t GROUP BY a) s JOIN m ON m.id = s.a";
        let q = crate::sql::parser::parse_single_query(text).unwrap();
        let (reply, _) = bounded(1);
        let err = e.subscribe(SubscribeTarget::Query(Box::new(q), text.to_string()), true, false, vec![], reply).unwrap_err();
        assert!(err.message.contains("has no dataflow"), "{}", err.message);
        assert_eq!((e.graph.live_nodes().count(), e.graph.live_arrangements().count()), (nodes, arrs));
        assert!(e.subs.is_empty());
    }

    /// Clock ticks that change nothing but the clock do not make a
    /// checkpoint due: an idle engine took one every interval, rewriting its
    /// whole state each time. A tick that changes a view does, and so does
    /// a long run of ticks (which bounds the replay of an idle engine).
    #[test]
    fn ticks_alone_do_not_make_a_checkpoint_due() {
        use crate::types::Datum;
        let tmp = crate::testkit::TempDir::new("coord-idle-ticks");
        let mut e = engine(tmp.path());
        e.cfg.checkpoint_interval = Duration::ZERO;
        // 2026-01-01T00:00:00Z in Unix and in PostgreSQL-epoch microseconds
        let (unix, pg) = (1_767_225_600_000_000i64, 1_767_225_600_000_000i64 - 946_684_800_000_000);
        let mut gsn = 0;
        let mut apply = |e: &mut Engine, rec: LogRecord| {
            gsn += 1;
            e.apply_record(gsn, rec).unwrap();
            e.maybe_checkpoint().unwrap();
            gsn
        };
        let ddl = |sql: &str| LogRecord::Ddl { req: 0, sql: sql.into(), extra: String::new() };
        let tick = |secs: i64| LogRecord::Tick { micros: unix + secs * 1_000_000 };
        apply(&mut e, ddl("CREATE TABLE t (id int PRIMARY KEY, until timestamptz)"));
        apply(&mut e, ddl("CREATE MATERIALIZED VIEW live AS SELECT id FROM t WHERE until > now()"));
        let t = e.catalog.by_name("t").unwrap().id;
        let row = vec![Datum::Int4(1), Datum::TimestampTz(pg + 10_000_000)];
        let g = apply(&mut e, LogRecord::Commit { source: 0, lsn: 0, changes: vec![(t, TableChange::Upsert { row, unchanged: vec![] })] });
        assert_eq!(e.env.checkpoint_gsn(), g);
        for s in 1..=5 {
            apply(&mut e, tick(s));
        }
        assert_eq!(e.env.checkpoint_gsn(), g, "ticks that change nothing else");
        // the row expires: the view changes
        let g = apply(&mut e, tick(11));
        assert_eq!(e.env.checkpoint_gsn(), g, "a tick that changes a view");
        for s in 12..12 + MAX_TICKS_WITHOUT_CHECKPOINT as i64 - 1 {
            apply(&mut e, tick(s));
        }
        assert_eq!(e.env.checkpoint_gsn(), g);
        let g = apply(&mut e, tick(100_000));
        assert_eq!(e.env.checkpoint_gsn(), g, "a long run of ticks");
    }

    /// A checkpoint lets the sequencer delete the input log up to its own
    /// GSN, whatever the retention: only recovery reads the log, from the
    /// checkpoint on (the log used to be kept for the retention window too).
    #[test]
    fn checkpoints_truncate_the_log_to_their_gsn() {
        let tmp = crate::testkit::TempDir::new("coord-truncate");
        let (env, _) = Env::open(&tmp.path().join("store"), EnvOptions::default()).unwrap();
        let (seq, rx) = unbounded();
        let cfg = Config { data_dir: tmp.path().to_path_buf(), retention_gsns: 1000, ..Default::default() };
        let mut e = Engine::new(cfg, env, seq, Arc::new(Shared::default()));
        e.replaying = false;
        for gsn in 1..=5 {
            e.apply_record(gsn, LogRecord::Ddl { req: 0, sql: format!("CREATE TABLE t{gsn} (id int)"), extra: String::new() }).unwrap();
        }
        e.checkpoint().unwrap();
        let truncated: Vec<Gsn> = rx.try_iter().filter_map(|m| if let SeqMsg::Truncate(g) = m { Some(g) } else { None }).collect();
        assert_eq!(truncated, [5]);
    }

    /// The metadata tree is maintained like any other: error records that
    /// go away leave retracted entries in leaves that nothing writes again,
    /// which the checkpoints' maintenance passes compact away once the
    /// retention frontier passes them.
    #[test]
    fn the_metadata_tree_is_compacted_by_maintenance() {
        use crate::types::Datum;
        let tmp = crate::testkit::TempDir::new("coord-meta-maintenance");
        let mut e = engine(tmp.path());
        e.cfg.retention_gsns = 1;
        let ddl = |sql: &str| LogRecord::Ddl { req: 0, sql: sql.into(), extra: String::new() };
        e.apply_record(1, ddl("CREATE TABLE t (id int PRIMARY KEY, v text)")).unwrap();
        e.apply_record(2, ddl("CREATE MATERIALIZED VIEW m AS SELECT id, v::int AS n FROM t")).unwrap();
        let t = e.catalog.by_name("t").unwrap().id;
        // an error record per row (the message names the value)
        let rows = (0..20_000)
            .map(|i| (t, TableChange::Upsert { row: vec![Datum::Int4(i), Datum::Text(format!("x{i}").into())], unchanged: vec![] }))
            .collect();
        e.apply_record(3, LogRecord::Commit { source: 1, lsn: 0, changes: rows }).unwrap();
        e.checkpoint().unwrap();
        e.apply_record(4, LogRecord::Commit { source: 1, lsn: 0, changes: vec![(t, TableChange::Truncate)] }).unwrap();
        e.checkpoint().unwrap();
        let meta_entries = |e: &mut Engine| {
            let tree = e.meta.tree.unwrap();
            let (mut entries, mut live) = (0, 0);
            e.env
                .for_each(tree, std::ops::Bound::Unbounded, std::ops::Bound::Unbounded, &mut |_, h| {
                    entries += 1;
                    live += (h.total() != 0) as usize;
                    Ok(true)
                })
                .unwrap();
            (entries, live)
        };
        let (entries, live) = meta_entries(&mut e);
        assert!(entries > 20_000 && live < 100, "{entries} entries, {live} live");
        // the frontier passes the retractions
        e.apply_record(5, LogRecord::Tick { micros: 1_767_225_600_000_000 }).unwrap();
        e.checkpoint().unwrap();
        let (entries, live) = meta_entries(&mut e);
        assert!(entries < 2 * live + 100, "{entries} entries left for {live} live ones");
    }
}
