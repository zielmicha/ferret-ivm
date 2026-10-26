//! Hydration off the engine thread.
//!
//! The new nodes and arrangements of a statement (a new or replaced
//! materialized view's and those of the views re-planned with it, an
//! index's, a table's rebuilt after an upstream schema change, a
//! subscription's query) are hydrated as of the GSN *g* it is applied at,
//! but not on the engine thread: a fork of the storage (a version of every
//! tree as of *g*, `Env::fork`) and of the part of the graph they read
//! (`Graph::fork_of`) go to a thread of their own, which hydrates them
//! there and then applies the records after *g* that the engine forwards to
//! it — the same `Graph::step`, on that part of the graph only — until it
//! has caught up. The engine then adopts the new trees (`Env::adopt_trees`,
//! which moves their nodes) and the fragment is maintained with the rest
//! from the next record on, and what waited for it follows (`Then`). Its
//! contents are exactly those an inline hydration at *g* followed by the
//! same steps would have produced. (The version read is the fork, never
//! the trees' history; the catch-up comes from the forwarded changes.)
//!
//! Meanwhile the engine applies records (the views that exist go on being
//! maintained) and DDL that only adds to the graph (its fragment is
//! hydrated next); records that replace or rebuild something must wait
//! (`must_wait_for_hydration`); requests that read the new fragment wait
//! for it. Replay and recovery hydrate inline: the result is the same.

use super::*;
use crate::dataflow::Delta;
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
    /// Reply to a DDL statement whose record built what is hydrated.
    Reply { req: u64, res: Result<ExecResult> },
    /// A new materialized view starts with the error records of its root.
    ViewErrors(ObjectId),
    /// Start a subscription as of the hydration's GSN.
    Subscribe(PendingSub),
    /// The rest of a materialized view's replacement (`finish_replace`).
    Replaced(Box<Replaced>),
    /// A broken materialized view built again: its sinks start.
    Repaired(ObjectId),
    /// The rest of a table's rebuild after an upstream schema change
    /// (`Engine::apply_schema_change`): its new version is filled from the
    /// old one before the hydration (`fill_rebuilt`); after it, the table's
    /// sinks start over and the old version is released.
    Rebuilt(Box<Rebuilt>),
}

/// A table rebuilt in a new shape.
pub(crate) struct Rebuilt {
    pub old_node: NodeId,
    pub new_node: NodeId,
    /// Per new column, the old column of the same name.
    pub mapping: Vec<Option<usize>>,
    pub columns: Vec<crate::types::Column>,
    pub pk: Vec<usize>,
    pub old_index_arrs: Vec<ArrId>,
    /// The table's sinks (held meanwhile).
    pub sinks: Vec<ObjectId>,
}

/// What filling a rebuilt table's new version needs (`fill_rebuilt`).
#[derive(Clone)]
struct Conversion {
    old_node: NodeId,
    new_node: NodeId,
    mapping: Vec<Option<usize>>,
    columns: Vec<crate::types::Column>,
    pk: Vec<usize>,
}

/// Fill a rebuilt table's new primary index, as of `gsn`, with the old
/// version's rows in the new shape: columns mapped by name and cast to their
/// new types (NULL where that fails); rows without their (new) key are left
/// to the re-snapshot, which upserts by key. The old rows are streamed in
/// batches into a `TreeBuilder`, so a table of any size is rebuilt in
/// bounded memory.
fn fill_rebuilt(graph: &mut Graph, env: &mut Env, c: &Conversion, gsn: Gsn) -> Result<()> {
    let crate::dataflow::NodeKind::Table { primary, .. } = graph.node(c.new_node).kind else {
        return Err(SqlError::internal("rebuilt table without a table node"));
    };
    let arr = graph.arr(primary);
    let (tree, layout) = (arr.tree, arr.layout.clone());
    let mut b = crate::storage::TreeBuilder::new(env, tree, gsn);
    let r = crate::dataflow::ops::snapshot_stream(graph, env, c.old_node, &mut |env, batch| {
        for (r, w) in batch {
            let nr: crate::types::Row = c
                .mapping
                .iter()
                .zip(&c.columns)
                .map(|(m, col)| match m {
                    Some(i) => crate::expr::cast::cast_datum(&r[*i], &col.ty.ty, col.ty.typmod, true).unwrap_or(crate::types::Datum::Null),
                    None => crate::types::Datum::Null,
                })
                .collect();
            if c.pk.iter().any(|k| nr[*k].is_null()) {
                continue;
            }
            b.push(env, layout.encode_row(&nr)?, w)?;
        }
        Ok(())
    });
    match r {
        Ok(_) => b.finish(env)?,
        Err(e) => {
            b.abort(env)?;
            return Err(e);
        }
    }
    let a = graph.arrs[primary].as_mut().unwrap();
    a.hydrated = true;
    a.since = gsn;
    graph.node_mut(c.new_node).hydrated = true;
    Ok(())
}

/// A replaced materialized view (`Engine::replace_view`): its old version,
/// released once the new one is hydrated, and what follows it.
pub(crate) struct Replaced {
    pub id: ObjectId,
    pub old: ViewRt,
    /// Arrangements of the indexes on the old version.
    pub old_index_arrs: Vec<ArrId>,
    /// The new version has the old one's columns: its subscribers (held
    /// meanwhile) and sinks follow it; else the sinks start over.
    pub same: bool,
    /// The view's sinks (held meanwhile).
    pub sinks: Vec<ObjectId>,
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
    /// A Commit's or Tick's dataflow input: table changes and the clock,
    /// with their memory in the budget's part for forwarded changes
    /// (`MemBudget::forwarded`) until they are applied.
    Step { gsn: Gsn, changes: FxHashMap<NodeId, Vec<TableChange>>, now: i64, charge: crate::memory::Charge },
    Finish,
}

/// What a finished hydration hands back.
struct Hydrated {
    env: Env,
    graph: Graph,
    /// Snapshots of subscribed nodes as of the hydration's GSN.
    snapshots: FxHashMap<NodeId, Output>,
    /// Per replaced view, the change from its old to its new contents as of
    /// the hydration's GSN.
    diffs: FxHashMap<ObjectId, Delta>,
    /// Outputs of subscribed nodes at every record applied after it.
    steps: Vec<(Gsn, FxHashMap<NodeId, Output>)>,
}

/// What a hydration thread does besides hydrating.
struct ForkWork {
    /// Its fragment's nodes and those owning its arrangements: what it goes
    /// on stepping.
    roots: Vec<NodeId>,
    arrs: Vec<ArrId>,
    /// Nodes whose outputs at every record are kept (and whose snapshot).
    watch: Vec<(NodeId, bool)>,
    /// Replaced views: (view, old read arrangement, new read arrangement).
    diffs: Vec<(ObjectId, ArrId, ArrId)>,
    /// Rebuilt tables, filled before hydrating.
    conversions: Vec<Conversion>,
}

/// The change from the rows of arrangement `old` to those of `new`.
fn diff_arrangements(graph: &Graph, env: &mut Env, old: ArrId, new: ArrId) -> Result<Delta> {
    let mut diff = graph.scan_arr(env, new, crate::storage::ReadAt::Latest)?;
    diff.extend(graph.scan_arr(env, old, crate::storage::ReadAt::Latest)?.into_iter().map(|(r, w)| (r, -w)));
    crate::dataflow::consolidate(&mut diff);
    Ok(diff)
}

/// Hydrate the fork's pending fragment as of `gsn`, then apply forwarded
/// records until told to finish.
#[allow(clippy::too_many_arguments)]
fn run(
    mut env: Env,
    mut graph: Graph,
    gsn: Gsn,
    work: ForkWork,
    rx: Receiver<ToFork>,
    hydrated: Arc<AtomicBool>,
    applied: Arc<Applied>,
    wake: Sender<EngineMsg>,
) -> Result<Hydrated> {
    let ForkWork { roots, arrs, watch, diffs: to_diff, conversions } = work;
    for c in &conversions {
        fill_rebuilt(&mut graph, &mut env, c, gsn)?;
    }
    graph.hydrate(&mut env, gsn)?;
    let mut snapshots = FxHashMap::default();
    for (n, snap) in &watch {
        if *snap {
            snapshots.insert(*n, graph.snapshot(&mut env, *n)?);
        }
    }
    let mut diffs = FxHashMap::default();
    for (view, old, new) in to_diff {
        diffs.insert(view, diff_arrangements(&graph, &mut env, old, new)?);
    }
    // (the old versions of replaced views are not stepped here)
    graph.retain_closure(&roots);
    graph.prune_arrangements(&arrs);
    applied.set(gsn);
    hydrated.store(true, Ordering::Release);
    let _ = wake.send(EngineMsg::Wake);
    let mut steps = Vec::new();
    let watched: rustc_hash::FxHashSet<NodeId> = watch.iter().map(|(n, _)| *n).collect();
    loop {
        match rx.recv() {
            Ok(ToFork::Step { gsn, changes, now, charge }) => {
                graph.now = now;
                // (the rows of the watched nodes only: the others' outputs
                // stay spilled, or are freed as the step goes)
                let out = graph.step_observed(&mut env, gsn, changes, watched.clone())?;
                crate::dataflow::reduce::trim_caches(&graph);
                if !watch.is_empty() {
                    steps.push((gsn, watch.iter().filter_map(|(n, _)| out.get(n).map(|o| (*n, o.clone()))).collect()));
                }
                drop(charge);
                applied.set(gsn);
                if rx.is_empty() {
                    // caught up with the engine, which may be waiting for that
                    let _ = wake.send(EngineMsg::Wake);
                }
            }
            Ok(ToFork::Finish) => return Ok(Hydrated { env, graph, snapshots, diffs, steps }),
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
            return Ok(());
        }
        let (nodes, arrs) = self.graph.pending();
        let conversions: Vec<Conversion> = then
            .iter()
            .filter_map(|t| match t {
                Then::Rebuilt(r) => Some(Conversion {
                    old_node: r.old_node,
                    new_node: r.new_node,
                    mapping: r.mapping.clone(),
                    columns: r.columns.clone(),
                    pk: r.pk.clone(),
                }),
                _ => None,
            })
            .collect();
        let wake = match &self.handle {
            Some(h) if !self.replaying && !(nodes.is_empty() && arrs.is_empty()) => h.tx.clone(),
            _ => {
                for c in &conversions {
                    fill_rebuilt(&mut self.graph, &mut self.env, c, gsn)?;
                }
                self.graph.hydrate(&mut self.env, gsn)?;
                for t in then {
                    self.then_inline(t)?;
                }
                return Ok(());
            }
        };
        let mut roots = nodes.clone();
        roots.extend(arrs.iter().map(|a| self.graph.arr(*a).node));
        let mut watch: Vec<(NodeId, bool)> = Vec::new();
        let mut diffs = Vec::new();
        let mut fork_roots = roots.clone();
        for t in &then {
            match t {
                Then::Subscribe(p) => watch.push((p.node, p.snapshot)),
                Then::Rebuilt(r) => fork_roots.push(r.old_node),
                Then::Replaced(r) => {
                    // the old version too, for the change to the new one
                    fork_roots.push(r.old.node);
                    if let Some(v) = self.rt.views.get(&r.id) {
                        diffs.push((r.id, r.old.arr, v.arr));
                        if r.same {
                            watch.push((v.node, false));
                        }
                    }
                }
                _ => {}
            }
        }
        // (a watched node may be no part of the fragment: a replacement by a
        // version that exists already; it is stepped there too)
        roots.extend(watch.iter().map(|(n, _)| *n));
        fork_roots.extend(watch.iter().map(|(n, _)| *n));
        let env = self.fork_env()?;
        let graph = self.graph.fork_of(&fork_roots);
        let tables = graph.live_nodes().filter(|n| matches!(n.kind, crate::dataflow::NodeKind::Table { .. })).map(|n| n.id).collect();
        let (tx, rx) = unbounded();
        let (dtx, done) = bounded(1);
        let hydrated = Arc::new(AtomicBool::new(false));
        let applied = Arc::new(Applied::default());
        let (h2, a2) = (hydrated.clone(), applied.clone());
        let work = ForkWork { roots, arrs: arrs.clone(), watch, diffs, conversions };
        // (the engine thread's stack: the same operators run here)
        std::thread::Builder::new().name("ivm-hydrate".into()).stack_size(64 << 20).spawn(move || {
            let r = run(env, graph, gsn, work, rx, h2, a2, wake);
            let _ = dtx.send(r);
        })?;
        log::debug!("hydrating {} nodes and {} arrangements as of gsn {gsn} in the background", nodes.len(), arrs.len());
        self.hydration = Some(Hydration { gsn, nodes, arrs, tables, tx, done, hydrated, applied, forwarded: gsn, allowed: u64::MAX, then });
        Ok(())
    }

    /// `then` after an inline hydration.
    pub(crate) fn then_inline(&mut self, then: Then) -> Result<()> {
        match then {
            Then::Reply { req, res } => self.reply_pending(req, res),
            Then::ViewErrors(id) => self.take_root_errors(id),
            Then::Subscribe(p) if p.snapshot => {
                // (a large one is taken off the engine thread)
                if let Some((snapshot, then)) = self.snapshot_or_defer(p.node, super::reader::AfterSnapshot::Subscription(p))? {
                    self.after_snapshot(then, self.processed, snapshot, Vec::new())?;
                }
            }
            Then::Subscribe(p) => self.start_subscription(p, self.processed, None, &[]),
            Then::Replaced(r) => {
                let diff = match self.rt.views.get(&r.id).map(|v| v.arr) {
                    Some(new) => diff_arrangements(&self.graph, &mut self.env, r.old.arr, new)?,
                    None => Vec::new(),
                };
                self.finish_replace(*r, self.processed, diff, &[])?;
            }
            Then::Repaired(id) => self.finish_repair(id),
            Then::Rebuilt(r) => self.finish_rebuild(*r)?,
        }
        Ok(())
    }

    /// The rest of a table's rebuild (`apply_schema_change`) once its new
    /// version is filled and hydrated: its sinks start over with the new
    /// columns, and the old version is released.
    fn finish_rebuild(&mut self, r: Rebuilt) -> Result<()> {
        for sid in &r.sinks {
            self.held_sinks.remove(sid);
        }
        self.restart_sinks(r.sinks)?;
        for a in r.old_index_arrs {
            self.graph.release_arr(&mut self.env, a)?;
        }
        self.graph.release_node(&mut self.env, r.old_node)
    }

    /// The rest of a view replacement (`replace_view`) once the new version
    /// is hydrated: the view takes its new root's error records; held
    /// subscribers and sinks follow the new version, getting the change
    /// `diff` from the old to the new contents as of `gsn` and the new
    /// version's outputs at the records applied since (`steps`), as they
    /// would have had it been hydrated inline (sinks of a view whose columns
    /// changed start over); the old version is released.
    pub(crate) fn finish_replace(&mut self, r: Replaced, gsn: Gsn, diff: Delta, steps: &[(Gsn, FxHashMap<NodeId, Output>)]) -> Result<()> {
        if let Some(new_node) = self.rt.views.get(&r.id).map(|v| v.node) {
            self.take_root_errors(r.id);
            let limit = self.shared.mem.consumer_limit();
            let diff = own_rows(&diff);
            for s in self.subs.iter_mut().filter(|s| s.view == Some(r.id) && s.held) {
                s.node = new_node;
                s.held = false;
                if let Some((rows, bytes)) = &diff {
                    send_rows(s, gsn, rows, *bytes, limit);
                }
                for (g, outs) in steps {
                    let o = outs.get(&new_node);
                    deliver_to(s, *g, o.map(|o| &o.errs[..]).unwrap_or_default(), o.and_then(|o| own_rows(&o.data)), limit);
                }
            }
            for sid in &r.sinks {
                self.held_sinks.remove(sid);
            }
            if r.same {
                for sid in &r.sinks {
                    if let Some(sink) = self.sink_handles.get_mut(sid) {
                        sink.node = new_node;
                        // (one that falls behind is restarted after the next record)
                        match &diff {
                            Some((rows, bytes)) => sink.send(gsn, rows, *bytes, limit),
                            None => {
                                sink.progress(gsn);
                                true
                            }
                        };
                        for (g, outs) in steps {
                            match outs.get(&new_node).and_then(|o| own_rows(&o.data)) {
                                Some((rows, bytes)) => sink.send(*g, &rows, bytes, limit),
                                None => {
                                    sink.progress(*g);
                                    true
                                }
                            };
                        }
                    }
                }
            } else {
                // (re-planned after an upstream change) new rows, new columns
                self.restart_sinks(r.sinks)?;
            }
        }
        for a in r.old_index_arrs {
            self.graph.release_arr(&mut self.env, a)?;
        }
        self.graph.release_arr(&mut self.env, r.old.arr)?;
        self.graph.release_node(&mut self.env, r.old.node)
    }

    /// A broken view built again (`repair_view`) is hydrated: it takes its
    /// root's error records and its sinks start.
    fn finish_repair(&mut self, id: ObjectId) {
        self.take_root_errors(id);
        // (also while replaying: sinks receive replayed records)
        if let Some(handle) = self.handle.clone() {
            let sinks: Vec<(ObjectId, String, crate::catalog::SinkDef)> = self
                .catalog
                .objects
                .values()
                .filter_map(|o| match &o.kind {
                    ObjectKind::Sink(s) if s.from == id && self.rt.broken.contains_key(&o.id) => Some((o.id, o.name.clone(), s.clone())),
                    _ => None,
                })
                .collect();
            for (sid, sname, sdef) in sinks {
                self.start_sink_or_mark_broken(sid, &sname, &sdef, &handle);
            }
        }
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
    /// hydration in progress, before the engine applies them. The changes
    /// forwarded and not applied there yet are kept in memory, within their
    /// part of the memory budget (`MemBudget::forwarded_limit`): beyond, the
    /// engine waits for the hydration (which may still be hydrating). Once
    /// it has hydrated its fragment it must catch up too: the engine then
    /// waits until it is one record less behind than at the previous record
    /// (it applies two records for every one forwarded, so the engine goes
    /// on at about half the speed of maintaining everything including the
    /// new fragment, which is what it will have to do once it is live).
    pub(crate) fn forward_to_hydration(&mut self, gsn: Gsn, by_node: &FxHashMap<NodeId, Vec<TableChange>>) -> Result<()> {
        let now = self.graph.now;
        let mem = self.shared.clone();
        let Some(h) = self.hydration.as_mut() else { return Ok(()) };
        let target = if h.hydrated.load(Ordering::Acquire) {
            let lag = h.forwarded.saturating_sub(h.applied.get());
            h.allowed = h.allowed.min(lag).saturating_sub(1).max(HANDOFF_LAG);
            h.forwarded.saturating_sub(h.allowed)
        } else {
            0
        };
        let limit = mem.mem.forwarded_limit();
        let mut g = h.applied.gsn.lock();
        while *g < target || mem.mem.forwarded.used() > limit {
            if h.applied.moved.wait_for(&mut g, Duration::from_millis(100)).timed_out()
                && let Ok(r) = h.done.try_recv()
            {
                return Err(r.err().unwrap_or_else(|| SqlError::internal("hydration ended early")));
            }
        }
        drop(g);
        let changes: FxHashMap<NodeId, Vec<TableChange>> =
            by_node.iter().filter(|(n, _)| h.tables.contains(n)).map(|(n, c)| (*n, c.clone())).collect();
        let bytes = changes
            .values()
            .map(|c| crate::memory::heap_block(c.capacity() * std::mem::size_of::<TableChange>()) + c.iter().map(crate::memory::change_heap).sum::<usize>())
            .sum::<usize>()
            + std::mem::size_of::<ToFork>();
        let charge = mem.mem.forwarded.charge(bytes);
        // (a hydration that failed reports it when polled)
        let _ = h.tx.send(ToFork::Step { gsn, changes, now, charge });
        h.forwarded = gsn;
        Ok(())
    }

    /// Is a relation that `rel` reads still being hydrated (a view, or a
    /// table rebuilt after a schema change)?
    pub(crate) fn reads_hydrating(&self, rel: &crate::sql::Rel) -> bool {
        fn walk(e: &Engine, r: &crate::sql::Rel) -> bool {
            if let crate::sql::Rel::Get { id, .. } = r {
                if let Some(v) = e.rt.views.get(id)
                    && !e.graph.arr(v.arr).hydrated
                {
                    return true;
                }
                if let Some(n) = e.rt.tables.get(id)
                    && !e.graph.node(*n).hydrated
                {
                    return true;
                }
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
        let Hydrated { env, graph, mut snapshots, mut diffs, steps } = res;
        drop((env, graph));
        for t in h.then {
            match t {
                Then::Reply { req, res } => self.reply_pending(req, res),
                Then::ViewErrors(id) => self.take_root_errors(id),
                Then::Subscribe(p) => {
                    let snapshot = snapshots.remove(&p.node);
                    self.start_subscription(p, h.gsn, snapshot, &steps);
                }
                Then::Replaced(r) => {
                    let diff = diffs.remove(&r.id).unwrap_or_default();
                    self.finish_replace(*r, h.gsn, diff, &steps)?;
                }
                Then::Repaired(id) => self.finish_repair(id),
                Then::Rebuilt(r) => self.finish_rebuild(*r)?,
            }
        }
        Ok(())
    }

    /// Start a subscription as of `gsn`, with its node's snapshot then and
    /// its outputs at the records applied since (`steps`), as `deliver`
    /// would have delivered them.
    pub(crate) fn start_subscription(&mut self, p: PendingSub, gsn: Gsn, snapshot: Option<Output>, steps: &[(Gsn, FxHashMap<NodeId, Output>)]) {
        let (tx, rx) = unbounded();
        let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
        if let Some(s) = snapshot
            && !s.data.is_empty()
        {
            let _ = tx.send(SubEvent::Data { gsn, rows: SharedRows::new(s.data) });
        }
        let _ = tx.send(SubEvent::Progress { gsn, charge: crate::memory::Charge::none() });
        // (changes queued after the snapshot are bounded by its share of the memory budget)
        let queue = crate::memory::Meter::child(&self.shared.mem.outputs);
        let sub = Subscription {
            node: p.node,
            tx,
            queue,
            cancel: cancel.clone(),
            pinned: p.pinned,
            held: false,
            progress: p.progress,
            view: p.view,
            reads: p.reads,
        };
        let limit = self.shared.mem.consumer_limit();
        for (g, outs) in steps {
            let o = outs.get(&sub.node);
            deliver_to(&sub, *g, o.map(|o| &o.errs[..]).unwrap_or_default(), o.and_then(|o| own_rows(&o.data)), limit);
        }
        self.subs.push(sub);
        // (a client that is gone no longer reads it: it ends)
        if p.reply.send(Ok(ExecResult::Subscribe { columns: p.columns, rx, cancel: cancel.clone(), progress: p.progress })).is_err() {
            cancel.store(true, Ordering::Release);
        }
    }
}
