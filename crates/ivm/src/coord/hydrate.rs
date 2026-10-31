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
//! same steps would have produced. (The version read is the fork; the
//! catch-up comes from the forwarded changes.)
//!
//! Meanwhile the engine applies records (the views that exist go on being
//! maintained) and DDL that only adds to the graph (its fragment is
//! hydrated next); records that replace or rebuild something must wait
//! (`must_wait_for_hydration`); requests that read the new fragment wait
//! for it. Replay and recovery hydrate inline: the result is the same.

use super::*;
use crate::dataflow::delta::GsnDeltas;
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

    /// Changes forwarded were applied (their memory released) within a
    /// record: wake the engine if it waits for room.
    fn nudge(&self) {
        let _g = self.gsn.lock();
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
    /// The memory of the changes forwarded and not applied yet.
    forwarded_bytes: Arc<crate::memory::Meter>,
    /// What to do once the fragment is live.
    pub then: Vec<Then>,
}

impl Hydration {
    /// Wait until `ok(applied GSN, forwarded bytes)` holds (polled as the
    /// hydration applies records; a hydration that ended fails it).
    fn wait(&self, ok: impl Fn(Gsn, usize) -> bool) -> Result<()> {
        let mut g = self.applied.gsn.lock();
        while !ok(*g, self.forwarded_bytes.used()) {
            if self.applied.moved.wait_for(&mut g, Duration::from_millis(100)).timed_out()
                && let Ok(r) = self.done.try_recv()
            {
                return Err(r.err().unwrap_or_else(|| SqlError::internal("hydration ended early")));
            }
        }
        Ok(())
    }
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

/// Fill a rebuilt table's new primary index with the old version's rows in
/// the new shape: columns mapped by name and cast to their new types (NULL
/// where that fails); rows without their (new) key are left to the
/// re-snapshot, which upserts by key. The old rows are streamed in
/// batches into a `TreeBuilder`, so a table of any size is rebuilt in
/// bounded memory.
fn fill_rebuilt(graph: &mut Graph, env: &mut Env, c: &Conversion) -> Result<()> {
    let crate::dataflow::NodeKind::Table { primary, .. } = graph.node(c.new_node).kind else {
        return Err(SqlError::internal("rebuilt table without a table node"));
    };
    let arr = graph.arr(primary);
    let (tree, layout) = (arr.tree, arr.layout.clone());
    let mut b = crate::storage::TreeBuilder::new(env, tree);
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
    graph.set_arr_hydrated(primary, true);
    graph.set_node_hydrated(c.new_node, true);
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
    /// Subscribers or sinks follow the new version (`same`, and there are
    /// some): the change from the old to the new contents is computed for
    /// them, and only then.
    pub consumers: bool,
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

/// A record's dataflow input, as the engine applies it: a Commit's table
/// changes in batches (`Feed`, as many as it has parts), then `Step` (a
/// Tick has only that); `Abort` if the engine failed to apply it.
enum ToFork {
    /// Table changes of the record at `gsn`, with their memory in the
    /// budget's part for forwarded changes (`MemBudget::forwarded`) until
    /// they are fed to the fork's dataflow.
    Feed { gsn: Gsn, changes: FxHashMap<NodeId, Vec<TableChange>>, charge: crate::memory::Charge },
    /// The record at `gsn` is complete: step it with the clock at `now`.
    Step { gsn: Gsn, now: i64 },
    Abort,
    Finish,
}

/// What a finished hydration hands back.
struct Hydrated {
    env: Env,
    graph: Graph,
    /// Per replaced view, the change from its old to its new contents as of
    /// the hydration's GSN and the new version's outputs at every record
    /// applied after it (in temporary trees of `env`).
    changes: Vec<(ObjectId, GsnDeltas)>,
}

/// What a hydration thread does besides hydrating.
struct ForkWork {
    /// Its fragment's nodes and those owning its arrangements: what it goes
    /// on stepping.
    roots: Vec<NodeId>,
    arrs: Vec<ArrId>,
    /// Replaced views whose changes are kept for their subscribers and
    /// sinks: (view, old read arrangement, new read arrangement, new node).
    replaced: Vec<(ObjectId, ArrId, ArrId, NodeId)>,
    /// Rebuilt tables, filled before hydrating.
    conversions: Vec<Conversion>,
}

/// The change from a replaced view's old read arrangement to its new one
/// as of `gsn`, the first of the changes kept for its consumers.
fn replacement_diff(graph: &Graph, env: &mut Env, old: ArrId, new: ArrId, gsn: Gsn) -> Result<GsnDeltas> {
    let schema = graph.node(graph.arr(new).node).schema.clone();
    let mut changes = GsnDeltas::new(env, &schema);
    changes.begin(gsn, Vec::new());
    let r = graph.diff_arrangements(env, old, new, &mut |env, row, w| changes.push(env, &row, w));
    match r {
        Ok(()) => Ok(changes),
        Err(e) => {
            changes.release(env)?;
            Err(e)
        }
    }
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
    let ForkWork { roots, arrs, replaced, conversions } = work;
    for c in &conversions {
        fill_rebuilt(&mut graph, &mut env, c)?;
    }
    graph.hydrate(&mut env)?;
    let mut changes: Vec<(ObjectId, NodeId, GsnDeltas)> = Vec::new();
    for (view, old, new, node) in replaced {
        changes.push((view, node, replacement_diff(&graph, &mut env, old, new, gsn)?));
    }
    // (the old versions of replaced views are not stepped here)
    graph.retain_closure(&roots);
    graph.prune_arrangements(&arrs);
    applied.set(gsn);
    hydrated.store(true, Ordering::Release);
    let _ = wake.send(EngineMsg::Wake);
    let watched: Arc<rustc_hash::FxHashSet<NodeId>> = Arc::new(changes.iter().map(|(_, n, _)| *n).collect());
    // (the record being fed)
    let mut input: Option<crate::dataflow::StepInput> = None;
    loop {
        match rx.recv() {
            Ok(ToFork::Feed { gsn, changes, charge }) => {
                // (the rows of the watched nodes only: the others' outputs
                // stay spilled, or are freed as the step goes)
                let inp = input.get_or_insert_with(|| graph.step_begin(gsn, watched.clone()));
                for (node, ch) in changes {
                    graph.step_feed(&mut env, inp, node, ch)?;
                }
                drop(charge);
                applied.nudge();
            }
            Ok(ToFork::Abort) => {
                if let Some(inp) = input.take() {
                    inp.abort(&mut env)?;
                }
            }
            Ok(ToFork::Step { gsn, now }) => {
                graph.now = now;
                let inp = input.take().unwrap_or_else(|| graph.step_begin(gsn, watched.clone()));
                let mut out = graph.step_finish(&mut env, inp)?;
                crate::dataflow::reduce::trim_caches(&graph);
                // the replaced views' new outputs are kept in their trees
                for (_, node, kept) in changes.iter_mut() {
                    let o = out.get_mut(node);
                    kept.begin(gsn, o.as_ref().map(|o| o.errs.clone()).unwrap_or_default());
                    if let Some(o) = o {
                        o.for_each_chunk(&mut env, &mut |env, rows| {
                            for (row, w) in rows {
                                kept.push(env, &row, w)?;
                            }
                            Ok(())
                        })?;
                    }
                }
                crate::dataflow::release_outputs(&mut env, &mut out)?;
                applied.set(gsn);
                if rx.is_empty() {
                    // caught up with the engine, which may be waiting for that
                    let _ = wake.send(EngineMsg::Wake);
                }
            }
            Ok(ToFork::Finish) => {
                let mut kept = Vec::with_capacity(changes.len());
                for (view, _, mut c) in changes {
                    c.finish(&mut env)?;
                    kept.push((view, c));
                }
                return Ok(Hydrated { env, graph, changes: kept });
            }
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
        // (a small fragment is hydrated inline: forking for it would cost
        // more than reading what it reads)
        let small = || {
            let replaced: u64 = then
                .iter()
                .filter_map(|t| match t {
                    Then::Replaced(r) if r.consumers => Some(self.env.tree_count(self.graph.arr(r.old.arr).tree).unsigned_abs()),
                    _ => None,
                })
                .sum();
            !then.iter().any(|t| matches!(t, Then::Rebuilt(_)))
                && self.graph.pending_input_entries(&self.env) + replaced <= super::reader::INLINE_READ_ENTRIES
        };
        let wake = match &self.handle {
            Some(h) if !self.replaying && !(nodes.is_empty() && arrs.is_empty()) && !small() => h.tx.clone(),
            _ => {
                for c in &conversions {
                    fill_rebuilt(&mut self.graph, &mut self.env, c)?;
                }
                self.graph.hydrate(&mut self.env)?;
                for t in then {
                    self.then_inline(t)?;
                }
                return Ok(());
            }
        };
        let mut roots = nodes.clone();
        roots.extend(arrs.iter().map(|a| self.graph.arr(*a).node));
        let mut replaced = Vec::new();
        let mut fork_roots = roots.clone();
        for t in &then {
            match t {
                Then::Rebuilt(r) => fork_roots.push(r.old_node),
                // (the old version too, for the change to the new one: only
                // for the subscribers and sinks that follow the new version)
                Then::Replaced(r) if r.consumers => {
                    fork_roots.push(r.old.node);
                    if let Some(v) = self.rt.views.get(&r.id) {
                        replaced.push((r.id, r.old.arr, v.arr, v.node));
                    }
                }
                _ => {}
            }
        }
        // (a replaced view's new node may be no part of the fragment: a
        // replacement by a version that exists already; it is stepped there
        // too, for its outputs)
        roots.extend(replaced.iter().map(|r| r.3));
        fork_roots.extend(replaced.iter().map(|r| r.3));
        let env = self.fork_env()?;
        let graph = self.graph.fork_of(&fork_roots);
        let tables = graph.live_nodes().filter(|n| matches!(n.kind, crate::dataflow::NodeKind::Table { .. })).map(|n| n.id).collect();
        let (tx, rx) = unbounded();
        let (dtx, done) = bounded(1);
        let hydrated = Arc::new(AtomicBool::new(false));
        let applied = Arc::new(Applied::default());
        let (h2, a2) = (hydrated.clone(), applied.clone());
        let work = ForkWork { roots, arrs: arrs.clone(), replaced, conversions };
        // (the engine thread's stack: the same operators run here)
        std::thread::Builder::new().name("ivm-hydrate".into()).stack_size(64 << 20).spawn(move || {
            let r = run(env, graph, gsn, work, rx, h2, a2, wake);
            let _ = dtx.send(r);
        })?;
        log::debug!("hydrating {} nodes and {} arrangements as of gsn {gsn} in the background", nodes.len(), arrs.len());
        let forwarded_bytes = self.shared.mem.forwarded.clone();
        self.hydration = Some(Hydration { gsn, nodes, arrs, tables, tx, done, hydrated, applied, forwarded: gsn, allowed: u64::MAX, forwarded_bytes, then });
        Ok(())
    }

    /// `then` after an inline hydration.
    pub(crate) fn then_inline(&mut self, then: Then) -> Result<()> {
        match then {
            Then::Reply { req, res } => self.reply_pending(req, res),
            Then::ViewErrors(id) => self.take_root_errors(id),
            Then::Subscribe(p) => self.start_subscription(p),
            Then::Replaced(r) => {
                let changes = match self.rt.views.get(&r.id).map(|v| v.arr) {
                    Some(new) if r.consumers => {
                        let mut c = replacement_diff(&self.graph, &mut self.env, r.old.arr, new, self.processed)?;
                        c.finish(&mut self.env)?;
                        Some(c)
                    }
                    _ => None,
                };
                self.finish_replace(*r, changes)?;
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
    /// subscribers and sinks follow the new version, getting the kept
    /// `changes` — the change from the old to the new contents as of the
    /// replacement's GSN and the new version's outputs at the records
    /// applied since — a chunk at a time, as they would have had it been
    /// hydrated inline (a sink that asked for a snapshot meanwhile gets one
    /// instead; sinks of a view whose columns changed start over); the old
    /// version is released.
    pub(crate) fn finish_replace(&mut self, r: Replaced, changes: Option<GsnDeltas>) -> Result<()> {
        let mut res = Ok(());
        if let Some(new_node) = self.rt.views.get(&r.id).map(|v| v.node) {
            self.take_root_errors(r.id);
            let limit = self.shared.mem.consumer_limit();
            let subs: Vec<usize> = (0..self.subs.len()).filter(|&i| self.subs[i].view == Some(r.id) && self.subs[i].held).collect();
            for &i in &subs {
                self.subs[i].node = new_node;
                self.subs[i].held = false;
            }
            self.consumers_changed();
            for sid in &r.sinks {
                self.held_sinks.remove(sid);
            }
            let sinks: Vec<ObjectId> = if r.same { r.sinks.iter().filter(|s| self.sink_handles.contains_key(s)).copied().collect() } else { Vec::new() };
            for sid in &sinks {
                self.sink_handles.get_mut(sid).unwrap().node = new_node;
            }
            self.consumers_changed();
            // (one that asked for a snapshot while it was held gets it now)
            let asked: Vec<ObjectId> = sinks.iter().filter(|s| self.snapshot_asked.remove(s)).copied().collect();
            if let Some(c) = &changes {
                let (all_subs, all_sinks) = (&self.subs, &mut self.sink_handles);
                let mut last: Option<Gsn> = None;
                res = c.for_each(&mut self.env, &mut |_, gsn, errs, rows| {
                    if last != Some(gsn) {
                        if let Some(g) = last {
                            for &i in &subs {
                                send_progress(&all_subs[i], g, limit);
                            }
                        }
                        for &i in &subs {
                            deliver_errors(&all_subs[i], errs);
                        }
                        last = Some(gsn);
                    }
                    if rows.is_empty() {
                        return Ok(());
                    }
                    let rows = Arc::new(rows);
                    let bytes = crate::memory::delta_bytes(&rows);
                    for &i in &subs {
                        let s = &all_subs[i];
                        if !s.cancel.load(Ordering::Acquire) {
                            send_rows(s, gsn, &rows, bytes, limit);
                        }
                    }
                    for sid in sinks.iter().filter(|s| !asked.contains(s)) {
                        if let Some(sink) = all_sinks.get_mut(sid)
                            && !sink.behind()
                            && !sink.send(gsn, &rows, bytes, limit)
                        {
                            log::warn!("sink {sid} fell behind: its unwritten changes reached {} MiB; it resynchronises its target from a snapshot", limit >> 20);
                        }
                    }
                    Ok(())
                });
                if let Some(g) = last {
                    for &i in &subs {
                        send_progress(&self.subs[i], g, limit);
                    }
                    for sid in sinks.iter().filter(|s| !asked.contains(s)) {
                        if let Some(sink) = self.sink_handles.get(sid) {
                            sink.progress(g);
                        }
                    }
                }
            }
            for sid in asked {
                self.resync_sink(sid);
            }
            if !r.same {
                // (re-planned after an upstream change) new rows, new columns
                self.restart_sinks(r.sinks)?;
            }
        }
        if let Some(c) = changes {
            c.release(&mut self.env)?;
        }
        for a in r.old_index_arrs {
            self.graph.release_arr(&mut self.env, a)?;
        }
        self.graph.release_arr(&mut self.env, r.old.arr)?;
        self.graph.release_node(&mut self.env, r.old.node)?;
        res
    }

    /// A broken view built again (`replace_view`) is hydrated: it takes its
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

    /// Before a Commit or Tick is applied, the hydration in progress is
    /// forwarded its dataflow input (`forward_feed`, `forward_step`); first
    /// the engine waits for it to keep up. Once it has hydrated its
    /// fragment it must catch up: the engine waits until it is one record
    /// less behind than at the previous record (it applies two records for
    /// every one forwarded, so the engine goes on at about half the speed
    /// of maintaining everything including the new fragment, which is what
    /// it will have to do once it is live).
    pub(crate) fn forward_begin(&mut self) -> Result<()> {
        let Some(h) = self.hydration.as_mut() else { return Ok(()) };
        let target = if h.hydrated.load(Ordering::Acquire) {
            let lag = h.forwarded.saturating_sub(h.applied.get());
            h.allowed = h.allowed.min(lag).saturating_sub(1).max(HANDOFF_LAG);
            h.forwarded.saturating_sub(h.allowed)
        } else {
            0
        };
        h.wait(|g, _| g >= target)
    }

    /// Forward a batch of a Commit's table changes (by node) to the
    /// hydration in progress, before the engine applies them. The changes
    /// forwarded and not applied there yet are kept in memory, within their
    /// part of the memory budget (`MemBudget::forwarded_limit`): beyond, the
    /// engine waits for the hydration (which may still be hydrating; a
    /// commit's batches are fed there as they come, so a commit of any size
    /// passes).
    pub(crate) fn forward_feed(&mut self, gsn: Gsn, by_node: &FxHashMap<NodeId, Vec<TableChange>>) -> Result<()> {
        let mem = self.shared.clone();
        let Some(h) = self.hydration.as_mut() else { return Ok(()) };
        let changes: FxHashMap<NodeId, Vec<TableChange>> =
            by_node.iter().filter(|(n, _)| h.tables.contains(n)).map(|(n, c)| (*n, c.clone())).collect();
        if changes.is_empty() {
            return Ok(());
        }
        let limit = mem.mem.forwarded_limit();
        h.wait(|_, forwarded| forwarded <= limit)?;
        let bytes = changes
            .values()
            .map(|c| crate::memory::heap_block(c.capacity() * std::mem::size_of::<TableChange>()) + c.iter().map(crate::memory::change_heap).sum::<usize>())
            .sum::<usize>()
            + std::mem::size_of::<ToFork>();
        let charge = mem.mem.forwarded.charge(bytes);
        // (a hydration that failed reports it when polled)
        let _ = h.tx.send(ToFork::Feed { gsn, changes, charge });
        Ok(())
    }

    /// The record at `gsn` was applied (its batches forwarded): the
    /// hydration steps it too, with the clock as of it.
    pub(crate) fn forward_step(&mut self, gsn: Gsn) {
        let now = self.graph.now;
        if let Some(h) = self.hydration.as_mut() {
            let _ = h.tx.send(ToFork::Step { gsn, now });
            h.forwarded = gsn;
        }
    }

    /// The engine failed to apply the record being fed: the hydration
    /// drops what it was fed of it.
    pub(crate) fn forward_abort(&mut self) {
        if let Some(h) = self.hydration.as_mut() {
            let _ = h.tx.send(ToFork::Abort);
        }
    }

    /// Is a relation that `rel` reads still being hydrated (a view, or a
    /// table rebuilt after a schema change)?
    pub(crate) fn reads_hydrating(&self, rel: &crate::sql::Rel) -> bool {
        self.hydration.is_some() && rel.reads().any(|id| {
            self.rt.views.get(&id).is_some_and(|v| !self.graph.arr(v.arr).hydrated)
                || self.rt.tables.get(&id).is_some_and(|n| !self.graph.node(*n).hydrated)
        })
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
        let Hydrated { mut env, graph, changes } = res;
        // (the replaced views' kept changes, moved here)
        let mut kept = FxHashMap::default();
        for (view, c) in changes {
            kept.insert(view, c.adopt(&mut self.env, &mut env)?);
        }
        drop((env, graph));
        for t in h.then {
            match t {
                Then::Reply { req, res } => self.reply_pending(req, res),
                Then::ViewErrors(id) => self.take_root_errors(id),
                // (as of now: it is live)
                Then::Subscribe(p) => self.start_subscription(p),
                Then::Replaced(r) => {
                    let c = kept.remove(&r.id);
                    self.finish_replace(*r, c)?;
                }
                Then::Repaired(id) => self.finish_repair(id),
                Then::Rebuilt(r) => self.finish_rebuild(*r)?,
            }
        }
        for (_, c) in kept {
            c.release(&mut self.env)?;
        }
        Ok(())
    }

    /// Start a subscription at the last applied GSN: with its node's
    /// snapshot then, if it asked for one, streamed to its session in chunks
    /// (`snapshot_stream`, off the engine thread when large), and the changes
    /// after it.
    pub(crate) fn start_subscription(&mut self, p: PendingSub) {
        let (tx, rx) = unbounded();
        let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
        // (changes queued after the snapshot are bounded by its share of the
        // memory budget; the snapshot's chunks in flight count too)
        let queue = crate::memory::Meter::child(&self.shared.mem.outputs);
        let gsn = self.processed;
        if p.snapshot {
            let c = cancel.clone();
            let rows = self
                .snapshot_stream(p.node, &queue, Box::new(move || c.load(Ordering::Acquire)))
                .unwrap_or_else(super::stream::failed);
            let _ = tx.send(SubEvent::Snapshot { gsn, rows });
        }
        let _ = tx.send(SubEvent::Progress { gsn, charge: crate::memory::Charge::none() });
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
        self.subs.push(sub);
        self.consumers_changed();
        // (a client that is gone no longer reads it: it ends)
        if p.reply.send(Ok(ExecResult::Subscribe { columns: p.columns, rx, cancel: cancel.clone(), progress: p.progress })).is_err() {
            cancel.store(true, Ordering::Release);
        }
    }
}
