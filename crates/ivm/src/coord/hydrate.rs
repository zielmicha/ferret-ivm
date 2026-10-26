//! Hydration off the engine thread.
//!
//! The new nodes and arrangements of a statement (a materialized view's or
//! an index's, a subscription's query) are hydrated as of the GSN *g* it is
//! applied at, but not on the engine thread: a fork of the storage (a
//! version of every tree as of *g*, `Env::fork`) and of the part of the
//! graph they read (`Graph::fork_pending`) go to a thread of their own,
//! which hydrates them there and then applies the records after *g* that
//! the engine forwards to it — the same `Graph::step`, on that part of the
//! graph only — until it has caught up. The engine then adopts the new
//! trees (`Env::adopt_trees`, which moves their nodes) and the fragment is
//! maintained with the rest from the next record on. Its contents are
//! exactly those an inline hydration at *g* followed by the same steps
//! would have produced. (The version read is the fork, never the trees'
//! history; the catch-up comes from the forwarded changes.)
//!
//! Meanwhile the engine applies records (the views that exist go on being
//! maintained), but stops at the next DDL or schema record until the
//! hydration is done; requests that read the new fragment wait for it.
//! Replay and recovery hydrate inline: the result is the same.

use super::*;
use crate::storage::TreeId;
use rustc_hash::FxHashSet;
use std::sync::atomic::AtomicBool;

/// Error code of a request that reads a relation still being hydrated: it
/// is parked until the hydration is done (never sent to a client).
pub(crate) const HYDRATING: &str = "IVMHY";

/// How far behind the engine (in records) a hydration may be when the
/// engine waits for it to finish: it then has at most that many records
/// left to apply.
const HANDOFF_LAG: u64 = 100;

/// Most records forwarded to a hydration that has not hydrated its
/// fragment yet (they are kept in memory); beyond, the engine waits for it.
const MAX_BACKLOG: u64 = 1_000_000;

/// The GSN up to which a hydration has applied the forwarded records.
#[derive(Default)]
struct Applied {
    gsn: parking_lot::Mutex<Gsn>,
    moved: parking_lot::Condvar,
}

impl Applied {
    fn get(&self) -> Gsn {
        *self.gsn.lock()
    }

    fn set(&self, g: Gsn) {
        *self.gsn.lock() = g;
        self.moved.notify_all();
    }
}

/// A hydration in progress.
pub(crate) struct Hydration {
    /// The GSN the fragment is hydrated as of.
    pub gsn: Gsn,
    nodes: Vec<NodeId>,
    arrs: Vec<ArrId>,
    /// Table nodes in the fork, whose changes are forwarded.
    tables: FxHashSet<NodeId>,
    tx: Sender<ToFork>,
    done: Receiver<Result<Hydrated>>,
    /// The fork has hydrated the fragment...
    hydrated: Arc<AtomicBool>,
    /// ... and applied the records up to this GSN.
    applied: Arc<Applied>,
    /// The last record forwarded.
    forwarded: Gsn,
    /// Records it may be behind when the next one is forwarded, once it
    /// has hydrated: one less every time, so that it catches up.
    allowed: u64,
    /// What to do once the fragment is live.
    pub then: Vec<Then>,
}

/// What follows a hydration, on the engine thread.
pub(crate) enum Then {
    /// Reply to the DDL statement whose record started the hydration.
    Reply { req: u64, res: Result<ExecResult> },
    /// A new materialized view starts with the error records of its root.
    ViewErrors(ObjectId),
    /// Start a subscription as of the hydration's GSN.
    Subscribe(PendingSub),
}

/// A subscription waiting for its query's dataflow to be hydrated.
pub(crate) struct PendingSub {
    pub node: NodeId,
    pub pinned: Option<NodeId>,
    pub columns: Vec<ResultColumn>,
    pub reads: Vec<ObjectId>,
    pub view: Option<ObjectId>,
    pub snapshot: bool,
    pub progress: bool,
    pub reply: Sender<Result<ExecResult>>,
}

enum ToFork {
    /// A Commit's or Tick's dataflow input: table changes and the clock.
    Step { gsn: Gsn, changes: FxHashMap<NodeId, Vec<TableChange>>, now: i64 },
    Finish,
}

/// What a finished hydration hands back.
struct Hydrated {
    env: Env,
    graph: Graph,
    /// Snapshots of subscribed nodes as of the hydration's GSN.
    snapshots: FxHashMap<NodeId, Output>,
    /// Outputs of subscribed nodes at every record applied after it.
    steps: Vec<(Gsn, FxHashMap<NodeId, Output>)>,
}

/// Hydrate the fork's pending fragment as of `gsn`, then apply forwarded
/// records until told to finish.
#[allow(clippy::too_many_arguments)]
fn run(
    mut env: Env,
    mut graph: Graph,
    gsn: Gsn,
    arrs: Vec<ArrId>,
    watch: Vec<(NodeId, bool)>,
    rx: Receiver<ToFork>,
    hydrated: Arc<AtomicBool>,
    applied: Arc<Applied>,
    wake: Sender<EngineMsg>,
) -> Result<Hydrated> {
    graph.hydrate(&mut env, gsn)?;
    let mut snapshots = FxHashMap::default();
    for (n, snap) in &watch {
        if *snap {
            snapshots.insert(*n, graph.snapshot(&mut env, *n)?);
        }
    }
    graph.prune_arrangements(&arrs);
    applied.set(gsn);
    hydrated.store(true, Ordering::Release);
    let _ = wake.send(EngineMsg::Wake);
    let mut steps = Vec::new();
    loop {
        match rx.recv() {
            Ok(ToFork::Step { gsn, changes, now }) => {
                graph.now = now;
                let out = graph.step(&mut env, gsn, changes)?;
                if !watch.is_empty() {
                    steps.push((gsn, watch.iter().filter_map(|(n, _)| out.get(n).map(|o| (*n, o.clone()))).collect()));
                }
                applied.set(gsn);
                if rx.is_empty() {
                    // caught up with the engine, which may be waiting for that
                    let _ = wake.send(EngineMsg::Wake);
                }
            }
            Ok(ToFork::Finish) => return Ok(Hydrated { env, graph, snapshots, steps }),
            Err(_) => return Err(SqlError::new(codes::ADMIN_SHUTDOWN, "hydration abandoned")),
        }
    }
}

impl Engine {
    /// Hydrate the pending nodes and arrangements as of `gsn` (the record
    /// being applied, or the last one), then do `then`: inline when
    /// replaying or without threads, else on a thread of its own while the
    /// engine goes on (see the module docs). While a hydration is in
    /// progress, the pending nodes and arrangements it does not include are
    /// hydrated by the next one, which begins when it is done.
    pub(crate) fn hydrate(&mut self, gsn: Gsn, then: Vec<Then>) -> Result<()> {
        if self.hydration.is_some() {
            self.hydration_next.extend(then);
            self.reply_deferred = true;
            return Ok(());
        }
        let (nodes, arrs) = self.graph.pending();
        let wake = match &self.handle {
            Some(h) if !self.replaying && !(nodes.is_empty() && arrs.is_empty()) => h.tx.clone(),
            _ => {
                self.graph.hydrate(&mut self.env, gsn)?;
                for t in then {
                    self.then_inline(t)?;
                }
                return Ok(());
            }
        };
        let env = self.env.fork()?;
        let graph = self.graph.fork_pending();
        let tables = graph.live_nodes().filter(|n| matches!(n.kind, crate::dataflow::NodeKind::Table { .. })).map(|n| n.id).collect();
        let watch: Vec<(NodeId, bool)> = then
            .iter()
            .filter_map(|t| match t {
                Then::Subscribe(p) => Some((p.node, p.snapshot)),
                _ => None,
            })
            .collect();
        let (tx, rx) = unbounded();
        let (dtx, done) = bounded(1);
        let hydrated = Arc::new(AtomicBool::new(false));
        let applied = Arc::new(Applied::default());
        let (h2, a2, fork_arrs) = (hydrated.clone(), applied.clone(), arrs.clone());
        // (the engine thread's stack: the same operators run here)
        std::thread::Builder::new().name("ivm-hydrate".into()).stack_size(64 << 20).spawn(move || {
            let r = run(env, graph, gsn, fork_arrs, watch, rx, h2, a2, wake);
            let _ = dtx.send(r);
        })?;
        log::debug!("hydrating {} nodes and {} arrangements as of gsn {gsn} in the background", nodes.len(), arrs.len());
        self.reply_deferred = true;
        self.hydration = Some(Hydration { gsn, nodes, arrs, tables, tx, done, hydrated, applied, forwarded: gsn, allowed: u64::MAX, then });
        Ok(())
    }

    /// `then` after an inline hydration.
    pub(crate) fn then_inline(&mut self, then: Then) -> Result<()> {
        match then {
            Then::Reply { req, res } => self.reply_pending(req, res),
            Then::ViewErrors(id) => self.take_root_errors(id),
            Then::Subscribe(p) => {
                let snapshot = if p.snapshot { Some(self.graph.snapshot(&mut self.env, p.node)?) } else { None };
                self.start_subscription(p, self.processed, snapshot, &[]);
            }
        }
        Ok(())
    }

    /// A new materialized view takes the error records of its root (also
    /// when the root is shared with other views and was hydrated long ago).
    pub(crate) fn take_root_errors(&mut self, id: ObjectId) {
        if let Some(v) = self.rt.views.get(&id) {
            let errs = self.graph.errors_of(v.node);
            merge_errors(&mut self.rt.views.get_mut(&id).unwrap().errors, &errs);
        }
    }

    /// Forward a Commit's or Tick's table changes (by node) to the
    /// hydration in progress, before the engine applies them. Once it has
    /// hydrated its fragment it must catch up: the engine then waits until
    /// it is one record less behind than at the previous record (it applies
    /// two records for every one forwarded, so the engine goes on at about
    /// half the speed of maintaining everything including the new
    /// fragment, which is what it will have to do once it is live).
    pub(crate) fn forward_to_hydration(&mut self, gsn: Gsn, by_node: &FxHashMap<NodeId, Vec<TableChange>>) -> Result<()> {
        let now = self.graph.now;
        let Some(h) = self.hydration.as_mut() else { return Ok(()) };
        let target = if h.hydrated.load(Ordering::Acquire) {
            let lag = h.forwarded.saturating_sub(h.applied.get());
            h.allowed = h.allowed.min(lag).saturating_sub(1).max(HANDOFF_LAG);
            h.forwarded.saturating_sub(h.allowed)
        } else {
            h.forwarded.saturating_sub(MAX_BACKLOG)
        };
        let mut g = h.applied.gsn.lock();
        while *g < target {
            if h.applied.moved.wait_for(&mut g, Duration::from_millis(100)).timed_out()
                && let Ok(r) = h.done.try_recv()
            {
                return Err(r.err().unwrap_or_else(|| SqlError::internal("hydration ended early")));
            }
        }
        drop(g);
        let changes = by_node.iter().filter(|(n, _)| h.tables.contains(n)).map(|(n, c)| (*n, c.clone())).collect();
        // (a hydration that failed reports it when polled)
        let _ = h.tx.send(ToFork::Step { gsn, changes, now });
        h.forwarded = gsn;
        Ok(())
    }

    /// Is a relation that `rel` reads still being hydrated?
    pub(crate) fn reads_hydrating(&self, rel: &crate::sql::Rel) -> bool {
        fn walk(e: &Engine, r: &crate::sql::Rel) -> bool {
            if let crate::sql::Rel::Get { id, .. } = r
                && let Some(v) = e.rt.views.get(id)
                && !e.graph.arr(v.arr).hydrated
            {
                return true;
            }
            r.inputs().into_iter().any(|i| walk(e, i))
        }
        self.hydration.is_some() && walk(self, rel)
    }

    /// Finish the hydration in progress once it has caught up (waiting for
    /// at most `HANDOFF_LAG` records), then apply the records held back
    /// behind it. A failed hydration stops the engine, as a failed inline
    /// one does.
    pub(crate) fn poll_hydration(&mut self, handle: &EngineHandle) -> Result<()> {
        let Some(h) = &self.hydration else { return Ok(()) };
        if let Ok(r) = h.done.try_recv() {
            // it ends before being told to only by failing
            return Err(r.err().unwrap_or_else(|| SqlError::internal("hydration ended early")));
        }
        if !h.hydrated.load(Ordering::Acquire) || h.forwarded.saturating_sub(h.applied.get()) > HANDOFF_LAG {
            return Ok(());
        }
        let h = self.hydration.take().unwrap();
        let _ = h.tx.send(ToFork::Finish);
        let res = h.done.recv().map_err(|_| SqlError::internal("hydration thread vanished"))??;
        self.finish_hydration(h, res)?;
        self.hydrations_done += 1;
        // what was created meanwhile
        let next = std::mem::take(&mut self.hydration_next);
        let (nodes, arrs) = self.graph.pending();
        if !next.is_empty() || !nodes.is_empty() || !arrs.is_empty() {
            self.hydrate(self.processed, next)?;
        }
        self.wake_parked(handle);
        self.apply_durable(handle)
    }

    /// Must durable record `rec` wait until no hydration is in progress?
    /// DDL that hydrates inline (replacing a view, and the re-planning it
    /// entails) or reads a relation's contents at once (a sink's start), and
    /// schema changes; other DDL is applied (a new view or index joins the
    /// next hydration).
    pub(crate) fn must_wait_for_hydration(&self, rec: &LogRecord) -> bool {
        use crate::sql::parser::Statement;
        match rec {
            LogRecord::Schema { .. } => true,
            LogRecord::Ddl { sql, .. } => match crate::sql::parser::parse_statement(sql) {
                Ok(Statement::CreateSink { .. }) => true,
                Ok(Statement::Sql(s)) => match &*s {
                    sqlparser::ast::Statement::CreateView(cv) => {
                        cv.or_replace
                            && crate::sql::binder::object_name(&cv.name).is_ok_and(|(_, n)| self.catalog.by_name(&n).is_some())
                    }
                    _ => false,
                },
                _ => false,
            },
            _ => false,
        }
    }

    /// Make a caught-up hydration's fragment live: adopt its trees and its
    /// nodes' state, then do what was waiting for it.
    fn finish_hydration(&mut self, h: Hydration, mut res: Hydrated) -> Result<()> {
        let trees: Vec<TreeId> = h
            .arrs
            .iter()
            .filter_map(|a| match (self.graph.arrs.get(*a), res.graph.arrs.get(*a)) {
                (Some(Some(mine)), Some(Some(theirs))) if mine.fp == theirs.fp && theirs.hydrated => Some(mine.tree),
                _ => None,
            })
            .collect();
        self.env.adopt_trees(&mut res.env, &trees)?;
        self.graph.adopt_hydrated(&res.graph, &h.nodes, &h.arrs);
        log::debug!("hydration as of gsn {} is live at gsn {}", h.gsn, self.processed);
        let Hydrated { env, graph, mut snapshots, steps } = res;
        drop((env, graph));
        for t in h.then {
            match t {
                Then::Reply { req, res } => self.reply_pending(req, res),
                Then::ViewErrors(id) => self.take_root_errors(id),
                Then::Subscribe(p) => {
                    let snapshot = snapshots.remove(&p.node);
                    self.start_subscription(p, h.gsn, snapshot, &steps);
                }
            }
        }
        Ok(())
    }

    /// Start a subscription as of `gsn`, with its node's snapshot then and
    /// its outputs at the records applied since (`steps`), as `deliver`
    /// would have delivered them.
    fn start_subscription(&mut self, p: PendingSub, gsn: Gsn, snapshot: Option<Output>, steps: &[(Gsn, FxHashMap<NodeId, Output>)]) {
        let (tx, rx) = unbounded();
        let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
        if let Some(s) = snapshot
            && !s.data.is_empty()
        {
            let _ = tx.send(SubEvent::Data { gsn, rows: s.data });
        }
        let _ = tx.send(SubEvent::Progress { gsn });
        let sub = Subscription { node: p.node, tx, cancel: cancel.clone(), pinned: p.pinned, progress: p.progress, view: p.view, reads: p.reads };
        for (g, outs) in steps {
            deliver_to(&sub, *g, outs.get(&sub.node));
        }
        self.subs.push(sub);
        let _ = p.reply.send(Ok(ExecResult::Subscribe { columns: p.columns, rx, cancel, progress: p.progress }));
    }
}
