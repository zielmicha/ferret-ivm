//! Incremental dataflow over arrangements.
//!
//! The graph is a DAG of hash-consed operator nodes. Each node maps its input
//! deltas at a GSN to an output delta (consolidated, so no reader observes
//! intermediate retract/insert pairs). Arrangements are persistent indexes of
//! a node's output, shared and refcounted; a node's output delta is applied to
//! its arrangements before any consumer runs, and consumers read "before" or
//! "after" the current GSN through the MVCC histories.
//!
//! Errors are data: expression errors become weighted error records that flow
//! alongside rows and retract when the offending rows retract.

pub mod agg;
pub mod join;
pub mod layout;
pub mod lower;
pub mod mfp;
pub mod ops;
pub mod reduce;
pub mod tablefunc;
pub mod temporal;
pub mod window;

use crate::error::{Result, SqlError};
use crate::expr::Expr;
use crate::sql::plan::SetOpKind;
use crate::storage::{AggKind, Env, Gsn, ReadAt, TreeId};
use crate::types::encoding::SortSpec;
use crate::types::{ColumnType, Row};
use layout::{Layout, LayoutSummarizer};
use mfp::Mfp;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::BTreeMap;
use std::sync::Arc;

pub type NodeId = usize;
pub type ArrId = usize;
pub type Delta = Vec<(Row, i64)>;
/// Error records: (sqlstate, message) with weights.
pub type ErrDelta = Vec<((String, String), i64)>;

#[derive(Default, Debug, Clone)]
pub struct Output {
    pub data: Delta,
    pub errs: ErrDelta,
}

impl Output {
    pub fn is_empty(&self) -> bool {
        self.data.is_empty() && self.errs.is_empty()
    }
}

/// A change to a base table carried by the input log.
#[derive(Clone, Debug)]
pub enum TableChange {
    /// Upsert by primary key; `unchanged` lists columns to keep from the
    /// stored row (unchanged TOAST values).
    Upsert { row: Row, unchanged: Vec<usize> },
    /// Delete by primary key values (in key order).
    Delete { key: Row },
    /// Multiset insert (tables without primary key).
    Insert { row: Row },
    /// Multiset delete of one instance.
    Remove { row: Row },
    Truncate,
}

#[derive(Clone, Debug)]
pub enum NodeKind {
    Table { object: u64, version: u32, pk: Vec<usize>, primary: ArrId },
    Constant { rows: Vec<Row> },
    Mfp { mfp: Mfp },
    Union { negate: Vec<bool> },
    Negate,
    FlatMap { funcs: Vec<crate::sql::plan::SrfCall>, ordinality: bool },
    Join(join::JoinPlan),
    Reduce(reduce::ReducePlan),
    /// Input arranged by all columns; output is the set of distinct rows.
    Distinct { arr: ArrId },
    SetOp { op: SetOpKind, all: bool, left_arr: ArrId, right_arr: ArrId },
    TopK(ops::TopKPlan),
    Window(window::WindowPlan),
    Recursive(ops::RecursivePlan),
    /// Rows passing predicates over now() while they hold.
    Temporal(temporal::TemporalPlan),
}

impl NodeKind {
    pub fn name(&self) -> &'static str {
        match self {
            NodeKind::Table { .. } => "Table",
            NodeKind::Constant { .. } => "Constant",
            NodeKind::Mfp { .. } => "Mfp",
            NodeKind::Union { .. } => "Union",
            NodeKind::Negate => "Negate",
            NodeKind::FlatMap { .. } => "FlatMap",
            NodeKind::Join(_) => "DeltaJoin",
            NodeKind::Reduce(_) => "Reduce",
            NodeKind::Distinct { .. } => "Distinct",
            NodeKind::SetOp { .. } => "SetOp",
            NodeKind::TopK(_) => "TopK",
            NodeKind::Window(_) => "Window",
            NodeKind::Recursive(_) => "Recursive",
            NodeKind::Temporal(_) => "TemporalFilter",
        }
    }
}

pub struct Node {
    pub id: NodeId,
    pub kind: NodeKind,
    pub inputs: Vec<NodeId>,
    pub schema: Vec<ColumnType>,
    pub fp: Vec<u8>,
    /// Consumers + external pins.
    pub refs: usize,
    pub arrangements: Vec<ArrId>,
    pub hydrated: bool,
}

#[derive(Clone)]
pub struct Arrangement {
    pub id: ArrId,
    pub node: NodeId,
    pub layout: Arc<Layout>,
    pub tree: TreeId,
    pub refs: usize,
    pub fp: Vec<u8>,
    pub hydrated: bool,
    /// GSN from which the arrangement's history is complete (it was
    /// hydrated then): reads as of earlier GSNs must not use it.
    pub since: Gsn,
}

impl Arrangement {
    /// Can a read at `at` use this arrangement?
    pub fn covers(&self, at: ReadAt) -> bool {
        self.hydrated
            && match at {
                ReadAt::Latest => true,
                ReadAt::AsOf(t) => self.since <= t,
                ReadAt::Before(t) => self.since < t,
            }
    }
}

/// Recovered arrangement trees, by fingerprint.
pub type RecoveryMap = FxHashMap<Vec<u8>, TreeId>;

#[derive(Default)]
pub struct Graph {
    pub nodes: Vec<Option<Node>>,
    pub arrs: Vec<Option<Arrangement>>,
    node_by_fp: FxHashMap<Vec<u8>, NodeId>,
    arr_by_fp: FxHashMap<Vec<u8>, ArrId>,
    topo: Vec<NodeId>,
    topo_dirty: bool,
    /// Trees from the checkpoint to attach instead of creating new ones.
    pub recovery: RecoveryMap,
    /// `since` of the recovered trees, by fingerprint.
    pub recovery_since: FxHashMap<Vec<u8>, Gsn>,
    /// Per-node delta-rate statistics (rows per tick, EWMA) for the planner.
    pub stats: FxHashMap<NodeId, NodeStats>,
    /// Per-node activity counters for introspection (one update per node
    /// per step, never per row).
    pub counters: FxHashMap<NodeId, NodeCounters>,
    /// Sum of all node counters ever recorded (monotonic).
    pub step_totals: NodeCounters,
    /// Logical time of the current GSN (timestamptz microseconds), from the
    /// logged clock ticks.
    pub now: i64,
    /// `now` as of the previous step (temporal filters run when it moves).
    stepped_now: i64,
    /// Error records currently in each node's contents (its accumulated
    /// output errors). Authoritative for nodes with a hydrated arrangement:
    /// their snapshots read the arrangement, which holds rows only.
    node_errors: FxHashMap<NodeId, BTreeMap<(String, String), i64>>,
}

/// Error state that must survive restarts, by node fingerprint: the error
/// records in the contents of arranged nodes (views created later over
/// them must see them), and the error state of operators that remember
/// which of their groups are in error (to retract the error when the group
/// recovers).
#[derive(Default, Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ErrorState {
    pub nodes: Vec<(Vec<u8>, Vec<((String, String), i64)>)>,
    #[serde(default)]
    pub operators: Vec<(Vec<u8>, Vec<(Vec<u8>, (String, String))>)>,
}

/// Groups of an operator currently in error, by group key prefix.
pub type OpErrors = Arc<parking_lot::Mutex<BTreeMap<Vec<u8>, (String, String)>>>;

/// The per-group error state of an operator, if it keeps one.
fn operator_errors(kind: &NodeKind) -> Option<&OpErrors> {
    match kind {
        NodeKind::Reduce(p) => Some(&p.errors),
        NodeKind::Recursive(p) => Some(&p.errors),
        _ => None,
    }
}

#[derive(Default, Clone, Copy, Debug)]
pub struct NodeStats {
    pub ticks: u64,
    pub rows: u64,
}

/// Activity of a node since it was created.
#[derive(Default, Clone, Copy, Debug, serde::Serialize)]
pub struct NodeCounters {
    /// Steps in which the node ran (it had input changes).
    pub steps: u64,
    /// Input delta rows (base table changes for table nodes).
    pub in_rows: u64,
    /// Output delta rows.
    pub out_rows: u64,
    /// Time spent running the operator and updating its arrangements.
    pub nanos: u64,
}

impl Graph {
    pub fn new() -> Graph {
        Graph::default()
    }

    pub fn node(&self, id: NodeId) -> &Node {
        self.nodes[id].as_ref().expect("dangling node")
    }

    pub fn node_mut(&mut self, id: NodeId) -> &mut Node {
        self.nodes[id].as_mut().expect("dangling node")
    }

    pub fn arr(&self, id: ArrId) -> &Arrangement {
        self.arrs[id].as_ref().expect("dangling arrangement")
    }

    pub fn live_nodes(&self) -> impl Iterator<Item = &Node> {
        self.nodes.iter().flatten()
    }

    pub fn live_arrangements(&self) -> impl Iterator<Item = &Arrangement> {
        self.arrs.iter().flatten()
    }

    /// Structural fingerprint of a node definition: a hash over the operator
    /// and the fingerprints (not ids) of its inputs and arrangements, so it
    /// is independent of creation history.
    fn node_fp(&self, kind: &NodeKind, inputs: &[NodeId]) -> Vec<u8> {
        let cx = FpCtx { g: self };
        let mut fp = Vec::new();
        fp.extend_from_slice(kind.name().as_bytes());
        for i in inputs {
            fp.extend_from_slice(cx.node(*i));
        }
        fp.push(0xFF);
        match kind {
            NodeKind::Table { object, version, pk, .. } => {
                fp.extend_from_slice(&object.to_le_bytes());
                fp.extend_from_slice(&version.to_le_bytes());
                fp.extend_from_slice(format!("{pk:?}").as_bytes());
            }
            NodeKind::Constant { rows } => {
                for r in rows {
                    crate::types::codec::encode_row(r, &mut fp);
                }
            }
            NodeKind::Mfp { mfp } => mfp.fingerprint(&mut fp),
            NodeKind::Union { negate } => fp.extend(negate.iter().map(|b| *b as u8)),
            NodeKind::Negate => {}
            NodeKind::FlatMap { funcs, ordinality } => {
                for f in funcs {
                    fp.extend_from_slice(format!("{:?}/{}", f.func, f.arity).as_bytes());
                    for a in &f.args {
                        a.fingerprint(&mut fp);
                    }
                    fp.push(0xFD);
                }
                fp.push(*ordinality as u8);
            }
            NodeKind::Join(p) => p.fingerprint(&cx, &mut fp),
            NodeKind::Reduce(p) => p.fingerprint(&cx, &mut fp),
            NodeKind::Distinct { arr } => fp.extend_from_slice(cx.arr(*arr)),
            NodeKind::SetOp { op, all, left_arr, right_arr } => {
                fp.extend_from_slice(format!("{op:?}{all}").as_bytes());
                fp.extend_from_slice(cx.arr(*left_arr));
                fp.extend_from_slice(cx.arr(*right_arr));
            }
            NodeKind::TopK(p) => p.fingerprint(&cx, &mut fp),
            NodeKind::Window(p) => p.fingerprint(&cx, &mut fp),
            NodeKind::Recursive(p) => p.fingerprint(&cx, &mut fp),
            NodeKind::Temporal(p) => p.fingerprint(&mut fp),
        }
        hash128(&fp).to_vec()
    }

    /// Add (or find) a node. New nodes start with zero references (their
    /// consumers and pins add them); creating a node references its inputs
    /// and the arrangements its operator reads.
    pub fn add_node(&mut self, kind: NodeKind, inputs: Vec<NodeId>, schema: Vec<ColumnType>) -> NodeId {
        let fp = self.node_fp(&kind, &inputs);
        if let Some(&id) = self.node_by_fp.get(&fp) {
            return id;
        }
        for i in &inputs {
            self.node_mut(*i).refs += 1;
        }
        for a in operator_arrangements(&kind) {
            self.arrs[a].as_mut().expect("operator arrangement").refs += 1;
        }
        let id = self.nodes.len();
        self.nodes.push(Some(Node { id, kind, inputs, schema, fp: fp.clone(), refs: 0, arrangements: vec![], hydrated: false }));
        self.node_by_fp.insert(fp, id);
        self.topo_dirty = true;
        id
    }

    /// Add (or find) a node whose operator reads an arrangement of its own
    /// output (Reduce, Recursive). `patch` stores the arrangement id in the
    /// operator; fingerprints must not depend on it.
    #[allow(clippy::too_many_arguments)]
    pub fn add_node_with_output_arr(
        &mut self,
        env: &mut Env,
        mut kind: NodeKind,
        inputs: Vec<NodeId>,
        schema: Vec<ColumnType>,
        key: Vec<Expr>,
        specs: Vec<SortSpec>,
        patch: fn(&mut NodeKind, ArrId),
    ) -> NodeId {
        let fp = self.node_fp(&kind, &inputs);
        if let Some(&id) = self.node_by_fp.get(&fp) {
            return id;
        }
        let id = self.nodes.len();
        self.nodes.push(Some(Node {
            id,
            kind: NodeKind::Constant { rows: vec![] },
            inputs: inputs.clone(),
            schema: schema.clone(),
            fp: fp.clone(),
            refs: 0,
            arrangements: vec![],
            hydrated: false,
        }));
        self.node_by_fp.insert(fp, id);
        let arr = self.add_arrangement(env, id, key, specs, vec![]);
        patch(&mut kind, arr);
        for i in &inputs {
            self.node_mut(*i).refs += 1;
        }
        for a in operator_arrangements(&kind) {
            self.arrs[a].as_mut().expect("operator arrangement").refs += 1;
        }
        self.node_mut(id).kind = kind;
        self.topo_dirty = true;
        id
    }

    /// Add (or find) a node whose operator keeps private state in an
    /// arrangement over `state_schema` (not an index of its output). The
    /// operator's hydration hook fills it. `patch` stores the arrangement id
    /// in the operator; fingerprints must not depend on it.
    #[allow(clippy::too_many_arguments)]
    pub fn add_node_with_state_arr(
        &mut self,
        env: &mut Env,
        mut kind: NodeKind,
        inputs: Vec<NodeId>,
        schema: Vec<ColumnType>,
        state_schema: Vec<ColumnType>,
        key: Vec<Expr>,
        specs: Vec<SortSpec>,
        patch: fn(&mut NodeKind, ArrId),
    ) -> NodeId {
        let fp = self.node_fp(&kind, &inputs);
        if let Some(&id) = self.node_by_fp.get(&fp) {
            return id;
        }
        let id = self.nodes.len();
        self.nodes.push(Some(Node {
            id,
            kind: NodeKind::Constant { rows: vec![] },
            inputs: inputs.clone(),
            schema,
            fp: fp.clone(),
            refs: 0,
            arrangements: vec![],
            hydrated: false,
        }));
        self.node_by_fp.insert(fp.clone(), id);
        // the state arrangement is identified by its owner's structure
        let mut afp = fp;
        afp.extend_from_slice(b"state");
        for (k, s) in key.iter().zip(&specs) {
            k.fingerprint(&mut afp);
            afp.push(s.desc as u8);
            afp.push(s.nulls_first as u8);
        }
        let afp = hash128(&afp).to_vec();
        let layout = Arc::new(Layout::new(key, specs, &state_schema, vec![]));
        let summarizer = Arc::new(LayoutSummarizer { layout: layout.clone() });
        let (tree, hydrated) = match self.recovery.remove(&afp) {
            Some(t) if env.has_tree(t) => {
                env.attach_tree(t, summarizer).ok();
                (t, true)
            }
            _ => (env.create_tree(summarizer), false),
        };
        let since = self.recovery_since.get(&afp).copied().unwrap_or(0);
        let arr = self.arrs.len();
        self.arrs.push(Some(Arrangement { id: arr, node: id, layout, tree, refs: 0, fp: afp.clone(), hydrated, since }));
        self.arr_by_fp.insert(afp, arr);
        patch(&mut kind, arr);
        for i in &inputs {
            self.node_mut(*i).refs += 1;
        }
        for a in operator_arrangements(&kind) {
            self.arrs[a].as_mut().expect("operator arrangement").refs += 1;
        }
        self.node_mut(id).kind = kind;
        self.topo_dirty = true;
        id
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn arr_count(&self) -> usize {
        self.arrs.len()
    }

    /// Remove nodes and arrangements created at or after the given marks that
    /// ended up unreferenced (planning leftovers).
    pub fn gc_since(&mut self, env: &mut Env, node_mark: usize, arr_mark: usize) -> Result<()> {
        for id in (node_mark..self.nodes.len()).rev() {
            if let Some(n) = &self.nodes[id]
                && n.refs == 0
            {
                self.node_mut(id).refs = 1;
                self.release_node(env, id)?;
            }
        }
        for id in arr_mark..self.arrs.len() {
            if let Some(a) = &self.arrs[id]
                && a.refs == 0
            {
                self.arrs[id].as_mut().unwrap().refs = 1;
                self.release_arr(env, id)?;
            }
        }
        Ok(())
    }

    /// Add (or find) an arrangement of `node`. New arrangements start with
    /// zero references; operators and pins add them.
    pub fn add_arrangement(
        &mut self,
        env: &mut Env,
        node: NodeId,
        key: Vec<Expr>,
        specs: Vec<SortSpec>,
        summaries: Vec<(AggKind, Expr)>,
    ) -> ArrId {
        // structural only (the node's fingerprint, never its index), so a
        // recovered graph finds its trees whatever order it was rebuilt in
        let mut fp = Vec::new();
        for (k, s) in key.iter().zip(&specs) {
            k.fingerprint(&mut fp);
            fp.push(s.desc as u8);
            fp.push(s.nulls_first as u8);
        }
        fp.push(0xFE);
        for (k, e) in &summaries {
            fp.extend_from_slice(format!("{k:?}").as_bytes());
            e.fingerprint(&mut fp);
        }
        // include the node fingerprint so recovery matches on structure
        let mut full = self.node(node).fp.clone();
        full.extend_from_slice(&fp);
        let full_fp = hash128(&full).to_vec();
        if let Some(&id) = self.arr_by_fp.get(&full_fp) {
            return id;
        }
        let schema = self.node(node).schema.clone();
        let layout = Arc::new(Layout::new(key, specs, &schema, summaries));
        let summarizer = Arc::new(LayoutSummarizer { layout: layout.clone() });
        let (tree, hydrated) = match self.recovery.remove(&full_fp) {
            Some(t) if env.has_tree(t) => {
                env.attach_tree(t, summarizer).ok();
                (t, true)
            }
            _ => (env.create_tree(summarizer), false),
        };
        let since = self.recovery_since.get(&full_fp).copied().unwrap_or(0);
        let id = self.arrs.len();
        self.arrs.push(Some(Arrangement { id, node, layout, tree, refs: 0, fp: full_fp.clone(), hydrated, since }));
        self.arr_by_fp.insert(full_fp, id);
        self.node_mut(node).arrangements.push(id);
        id
    }

    pub fn retain_node(&mut self, id: NodeId) {
        self.node_mut(id).refs += 1;
    }

    pub fn retain_arr(&mut self, id: ArrId) {
        self.arrs[id].as_mut().unwrap().refs += 1;
    }

    /// Drop a reference to an arrangement; frees it at zero.
    pub fn release_arr(&mut self, env: &mut Env, id: ArrId) -> Result<()> {
        let a = self.arrs[id].as_mut().unwrap();
        a.refs -= 1;
        if a.refs == 0 {
            let a = self.arrs[id].take().unwrap();
            self.arr_by_fp.remove(&a.fp);
            env.drop_tree(a.tree)?;
            if let Some(n) = self.nodes[a.node].as_mut() {
                n.arrangements.retain(|x| *x != id);
            }
        }
        Ok(())
    }

    /// Drop a reference to a node; frees it (and cascades) at zero.
    pub fn release_node(&mut self, env: &mut Env, id: NodeId) -> Result<()> {
        let n = self.node_mut(id);
        n.refs -= 1;
        if n.refs > 0 {
            return Ok(());
        }
        let node = self.nodes[id].take().unwrap();
        self.node_by_fp.remove(&node.fp);
        self.node_errors.remove(&id);
        self.counters.remove(&id);
        self.topo_dirty = true;
        // arrangements used by this operator
        for a in operator_arrangements(&node.kind) {
            self.release_arr(env, a)?;
        }
        for i in node.inputs {
            self.release_node(env, i)?;
        }
        Ok(())
    }

    /// Topological order of live nodes.
    pub fn topo(&mut self) -> &[NodeId] {
        if self.topo_dirty {
            // node ids are assigned after their inputs, so id order is topological
            self.topo = self.nodes.iter().flatten().map(|n| n.id).collect();
            self.topo_dirty = false;
        }
        &self.topo
    }

    // ------------------------------------------------------------------ reads

    /// Rows of an arrangement with weights as of `at` (decoded).
    pub fn scan_arr(&self, env: &mut Env, arr: ArrId, at: ReadAt) -> Result<Delta> {
        let a = self.arr(arr);
        let layout = &a.layout;
        let mut out = Vec::with_capacity(env.tree_count(a.tree).clamp(0, 1 << 24) as usize);
        env.for_each(a.tree, std::ops::Bound::Unbounded, std::ops::Bound::Unbounded, &mut |k, h| {
            let w = h.weight(at);
            if w != 0 {
                out.push((layout.decode_row(k)?, w));
            }
            Ok(true)
        })?;
        Ok(out)
    }

    /// Rows matching a key prefix.
    pub fn lookup(&self, env: &mut Env, arr: ArrId, key: &[crate::types::Datum], at: ReadAt) -> Result<Delta> {
        let a = self.arr(arr);
        let prefix = a.layout.encode_prefix(key);
        let layout = &a.layout;
        let mut out = Vec::new();
        env.for_each_prefix(a.tree, &prefix, &mut |k, h| {
            let w = h.weight(at);
            if w != 0 {
                out.push((layout.decode_row(k)?, w));
            }
            Ok(true)
        })?;
        Ok(out)
    }

    /// Full contents of a node as of the latest processed time (consolidated).
    pub fn snapshot(&self, env: &mut Env, node: NodeId) -> Result<Output> {
        let mut cache = FxHashMap::default();
        let mut out = ops::owned(ops::snapshot_node(self, env, node, &mut cache)?);
        consolidate(&mut out.data);
        Ok(out)
    }

    /// The node's first hydrated arrangement, if any.
    pub fn any_arrangement(&self, node: NodeId) -> Option<ArrId> {
        self.node(node).arrangements.iter().copied().find(|a| self.arr(*a).hydrated)
    }

    /// Error records in a node's contents (exact for arranged nodes).
    pub fn errors_of(&self, node: NodeId) -> ErrDelta {
        self.node_errors.get(&node).map(|m| m.iter().map(|(e, w)| (e.clone(), *w)).collect()).unwrap_or_default()
    }

    fn add_errors(&mut self, node: NodeId, errs: &ErrDelta) {
        if errs.is_empty() {
            return;
        }
        let m = self.node_errors.entry(node).or_default();
        for (e, w) in errs {
            let v = m.entry(e.clone()).or_default();
            *v += w;
            if *v == 0 {
                m.remove(e);
            }
        }
        if m.is_empty() {
            self.node_errors.remove(&node);
        }
    }

    fn set_errors(&mut self, node: NodeId, errs: &ErrDelta) {
        self.node_errors.remove(&node);
        self.add_errors(node, errs);
    }

    /// Error state for a checkpoint.
    pub fn error_state(&self) -> ErrorState {
        let mut st = ErrorState::default();
        for n in self.live_nodes() {
            if let Some(m) = self.node_errors.get(&n.id)
                && self.any_arrangement(n.id).is_some()
            {
                st.nodes.push((n.fp.clone(), m.iter().map(|(e, w)| (e.clone(), *w)).collect()));
            }
            if let Some(errs) = operator_errors(&n.kind) {
                let errs = errs.lock();
                if !errs.is_empty() {
                    st.operators.push((n.fp.clone(), errs.iter().map(|(k, e)| (k.clone(), e.clone())).collect()));
                }
            }
        }
        st
    }

    /// Restore checkpointed error state into a graph rebuilt for recovery
    /// (before hydrating whatever could not be recovered).
    pub fn restore_error_state(&mut self, st: ErrorState) {
        for (fp, errs) in st.nodes {
            if let Some(&id) = self.node_by_fp.get(&fp) {
                self.set_errors(id, &errs);
            }
        }
        for (fp, groups) in st.operators {
            if let Some(&id) = self.node_by_fp.get(&fp)
                && let Some(errs) = operator_errors(&self.node(id).kind)
            {
                *errs.lock() = groups.into_iter().collect();
            }
        }
    }

    // ------------------------------------------------------------------ ticks

    /// Process one GSN. `changes` holds base table changes keyed by table node.
    /// Returns the output of every node that produced something.
    pub fn step(
        &mut self,
        env: &mut Env,
        gsn: Gsn,
        mut changes: FxHashMap<NodeId, Vec<TableChange>>,
    ) -> Result<FxHashMap<NodeId, Output>> {
        let order: Vec<NodeId> = self.topo().to_vec();
        let mut outputs: FxHashMap<NodeId, Output> = FxHashMap::default();
        let time_moved = self.now != self.stepped_now;
        self.stepped_now = self.now;
        for id in order {
            let node = self.node(id);
            if !node.hydrated {
                continue;
            }
            let (started, in_rows);
            let out = if let NodeKind::Table { .. } = node.kind {
                match changes.remove(&id) {
                    Some(ch) => {
                        (started, in_rows) = (std::time::Instant::now(), ch.len());
                        ops::table_step(self, env, node, ch, gsn)?
                    }
                    None => continue,
                }
            } else {
                let timed = time_moved && matches!(node.kind, NodeKind::Temporal(_));
                if !timed && !node.inputs.iter().any(|i| outputs.get(i).is_some_and(|o| !o.is_empty())) {
                    continue;
                }
                let empty = Output::default();
                let inputs: Vec<&Output> = node.inputs.iter().map(|i| outputs.get(i).unwrap_or(&empty)).collect();
                (started, in_rows) = (std::time::Instant::now(), inputs.iter().map(|o| o.data.len()).sum());
                ops::step_node(self, env, node, &inputs, gsn, &outputs)?
            };
            if out.is_empty() {
                self.count_step(id, in_rows, 0, started);
                continue;
            }
            self.add_errors(id, &out.errs);
            self.apply_to_arrangements(env, id, &out.data, gsn)?;
            self.count_step(id, in_rows, out.data.len(), started);
            let st = self.stats.entry(id).or_default();
            st.ticks += 1;
            st.rows += out.data.len() as u64;
            outputs.insert(id, out);
        }
        Ok(outputs)
    }

    fn count_step(&mut self, id: NodeId, in_rows: usize, out_rows: usize, started: std::time::Instant) {
        let nanos = started.elapsed().as_nanos() as u64;
        for c in [self.counters.entry(id).or_default(), &mut self.step_totals] {
            c.steps += 1;
            c.in_rows += in_rows as u64;
            c.out_rows += out_rows as u64;
            c.nanos += nanos;
        }
    }

    fn apply_to_arrangements(&self, env: &mut Env, node: NodeId, delta: &Delta, gsn: Gsn) -> Result<()> {
        if delta.is_empty() {
            return Ok(());
        }
        for &a in &self.node(node).arrangements {
            let arr = self.arr(a);
            if !arr.hydrated {
                continue;
            }
            let mut ups = Vec::with_capacity(delta.len());
            for (row, w) in delta {
                ups.push((arr.layout.encode_row(row)?, *w));
            }
            env.apply(arr.tree, &mut ups, gsn)?;
        }
        Ok(())
    }

    // ------------------------------------------------------------------ hydration

    /// Bring every new node and arrangement up to date as of `gsn`, computing
    /// their contents from existing state (never from the sources). The
    /// error records in their contents are kept per node (`errors_of`).
    pub fn hydrate(&mut self, env: &mut Env, gsn: Gsn) -> Result<()> {
        let order: Vec<NodeId> = self.topo().to_vec();
        let mut cache: ops::SnapCache = FxHashMap::default();
        // snapshots are cached only while a node still to be hydrated may
        // read them (large views would otherwise hold every intermediate)
        let needs = |g: &Graph, id: NodeId| {
            !g.node(id).hydrated || g.node(id).arrangements.iter().any(|a| !g.arr(*a).hydrated)
        };
        let mut readers: FxHashMap<NodeId, usize> = FxHashMap::default();
        for &id in &order {
            if needs(self, id) {
                for i in &self.node(id).inputs {
                    *readers.entry(*i).or_default() += 1;
                }
            }
        }
        for id in order {
            let needs_node = !self.node(id).hydrated;
            let pending_arrs: Vec<ArrId> =
                self.node(id).arrangements.iter().copied().filter(|a| !self.arr(*a).hydrated).collect();
            if !needs_node && pending_arrs.is_empty() {
                continue;
            }
            if needs_node {
                // stateful operators initialize their own arrangements here
                ops::hydrate_node(self, env, id, gsn, &mut cache)?;
            }
            let pending_arrs: Vec<ArrId> =
                self.node(id).arrangements.iter().copied().filter(|a| !self.arr(*a).hydrated).collect();
            if !pending_arrs.is_empty() {
                let snap = match cache.get(&id) {
                    Some(s) => s.clone(),
                    None => {
                        let s = ops::snapshot_node(self, env, id, &mut cache)?;
                        cache.insert(id, s.clone());
                        s
                    }
                };
                for a in pending_arrs {
                    let arr = self.arr(a);
                    // Entries are applied in key order, in batches that each
                    // cover a key range: unordered batches would each touch
                    // (and rewrite) every leaf.
                    let mut ups = Vec::with_capacity(snap.data.len());
                    for (row, w) in &snap.data {
                        ups.push((arr.layout.encode_row(row)?, *w));
                    }
                    ups.sort_unstable_by(|x, y| x.0.cmp(&y.0));
                    while !ups.is_empty() {
                        let mut batch = ups.split_off(ups.len().saturating_sub(100_000));
                        env.apply(arr.tree, &mut batch, gsn)?;
                    }
                    let a = self.arrs[a].as_mut().unwrap();
                    a.hydrated = true;
                    a.since = gsn;
                }
                // the arrangement holds the rows; the errors are kept here
                self.set_errors(id, &snap.errs);
            } else if needs_node && let Some(s) = cache.get(&id) {
                let errs = s.errs.clone();
                self.set_errors(id, &errs);
            }
            self.node_mut(id).hydrated = true;
            // drop snapshots nobody further down will read (including those
            // of intermediate nodes computed along the way)
            for i in self.node(id).inputs.clone() {
                if let Some(n) = readers.get_mut(&i) {
                    *n = n.saturating_sub(1);
                }
            }
            cache.retain(|k, _| readers.get(k).copied().unwrap_or(0) > 0);
        }
        Ok(())
    }

    // ------------------------------------------------------------------ hydration elsewhere

    /// Nodes and arrangements not hydrated yet: what `hydrate` would compute.
    pub fn pending(&self) -> (Vec<NodeId>, Vec<ArrId>) {
        let nodes = self.live_nodes().filter(|n| !n.hydrated).map(|n| n.id).collect();
        let arrs = self.live_arrangements().filter(|a| !a.hydrated).map(|a| a.id).collect();
        (nodes, arrs)
    }

    /// A copy of nodes `roots` and what they read, transitively — their
    /// inputs and the nodes owning the arrangements their operators read —
    /// with all of those nodes' arrangements, for another thread (with a
    /// fork of the storage). Operator state is copied, not shared; node and
    /// arrangement ids stay the same.
    pub fn fork_of(&self, roots: &[NodeId]) -> Graph {
        let keep_nodes = self.closure(roots);
        let mut g = Graph { now: self.now, stepped_now: self.stepped_now, topo_dirty: true, ..Graph::default() };
        g.nodes = self
            .nodes
            .iter()
            .map(|n| {
                let n = n.as_ref().filter(|n| keep_nodes.contains(&n.id))?;
                let mut kind = n.kind.clone();
                // (a fork steps its operators too: their error state must not be shared)
                let copy = |e: &OpErrors| -> OpErrors { Arc::new(parking_lot::Mutex::new(e.lock().clone())) };
                match &mut kind {
                    NodeKind::Reduce(p) => p.errors = copy(&p.errors),
                    NodeKind::Recursive(p) => p.errors = copy(&p.errors),
                    _ => {}
                }
                Some(Node {
                    id: n.id,
                    kind,
                    inputs: n.inputs.clone(),
                    schema: n.schema.clone(),
                    fp: n.fp.clone(),
                    refs: n.refs,
                    arrangements: n.arrangements.clone(),
                    hydrated: n.hydrated,
                })
            })
            .collect();
        g.arrs = self.arrs.iter().map(|a| a.as_ref().filter(|a| keep_nodes.contains(&a.node)).cloned()).collect();
        g.node_by_fp = g.live_nodes().map(|n| (n.fp.clone(), n.id)).collect();
        g.arr_by_fp = g.live_arrangements().map(|a| (a.fp.clone(), a.id)).collect();
        g.node_errors = self.node_errors.iter().filter(|(id, _)| keep_nodes.contains(id)).map(|(k, v)| (*k, v.clone())).collect();
        g
    }

    /// Nodes `roots` and what they read, transitively: their inputs and the
    /// nodes owning the arrangements their operators read.
    fn closure(&self, roots: &[NodeId]) -> FxHashSet<NodeId> {
        let mut keep: FxHashSet<NodeId> = FxHashSet::default();
        let mut stack: Vec<NodeId> = roots.to_vec();
        while let Some(id) = stack.pop() {
            if !keep.insert(id) {
                continue;
            }
            let n = self.node(id);
            stack.extend(n.inputs.iter().copied());
            stack.extend(operator_arrangements(&n.kind).into_iter().map(|a| self.arr(a).node));
        }
        keep
    }

    /// Drop the nodes that `roots` do not read (`closure`), with their
    /// arrangements (in a fork, so that its steps leave them alone).
    pub fn retain_closure(&mut self, roots: &[NodeId]) {
        let keep = self.closure(roots);
        for id in 0..self.nodes.len() {
            if self.nodes[id].is_some() && !keep.contains(&id) {
                let n = self.nodes[id].take().unwrap();
                self.node_by_fp.remove(&n.fp);
                self.node_errors.remove(&id);
                for a in n.arrangements {
                    if let Some(arr) = self.arrs[a].take() {
                        self.arr_by_fp.remove(&arr.fp);
                    }
                }
            }
        }
        self.topo_dirty = true;
    }

    /// Drop the arrangements that no operator reads, except `keep` (in a
    /// fork once hydrated: its steps maintain only what they need).
    pub fn prune_arrangements(&mut self, keep: &[ArrId]) {
        let mut used: FxHashSet<ArrId> = keep.iter().copied().collect();
        for n in self.live_nodes() {
            used.extend(operator_arrangements(&n.kind));
            // (a recursive query reads its sources' contents at every step)
            if let NodeKind::Recursive(p) = &n.kind {
                used.extend(p.sources.iter().filter_map(|(_, s)| self.any_arrangement(*s)));
            }
        }
        for a in 0..self.arrs.len() {
            if self.arrs[a].is_some() && !used.contains(&a) {
                let arr = self.arrs[a].take().unwrap();
                self.arr_by_fp.remove(&arr.fp);
                if let Some(n) = self.nodes[arr.node].as_mut() {
                    n.arrangements.retain(|x| *x != a);
                }
            }
        }
    }

    /// Take over what fork `from` hydrated of `nodes` and `arrs` (the pending
    /// ones when it was taken; their trees are adopted by the storage
    /// separately): hydration flags, `since`, error records and operator
    /// error state. Those freed here in the meantime are skipped.
    pub fn adopt_hydrated(&mut self, from: &Graph, nodes: &[NodeId], arrs: &[ArrId]) {
        for &a in arrs {
            let (Some(Some(mine)), Some(Some(theirs))) = (self.arrs.get(a), from.arrs.get(a)) else { continue };
            if mine.fp != theirs.fp {
                continue;
            }
            let (hydrated, since) = (theirs.hydrated, theirs.since);
            let mine = self.arrs[a].as_mut().unwrap();
            mine.hydrated = hydrated;
            mine.since = since;
        }
        for &id in nodes {
            let (Some(Some(mine)), Some(Some(theirs))) = (self.nodes.get(id), from.nodes.get(id)) else { continue };
            if mine.fp != theirs.fp {
                continue;
            }
            if let (Some(m), Some(t)) = (operator_errors(&mine.kind), operator_errors(&theirs.kind)) {
                *m.lock() = t.lock().clone();
            }
            let errs = from.errors_of(id);
            self.set_errors(id, &errs);
            self.node_mut(id).hydrated = theirs.hydrated;
        }
    }

    /// Arrangement fingerprint -> tree for checkpoints.
    pub fn arrangement_trees(&self) -> BTreeMap<Vec<u8>, TreeId> {
        self.live_arrangements().filter(|a| a.hydrated).map(|a| (a.fp.clone(), a.tree)).collect()
    }

    /// Arrangement fingerprint -> `since` for checkpoints.
    pub fn arrangement_since(&self) -> BTreeMap<Vec<u8>, Gsn> {
        self.live_arrangements().filter(|a| a.hydrated).map(|a| (a.fp.clone(), a.since)).collect()
    }

    /// The first of a node's arrangements holding its history as of `at`.
    pub fn arrangement_at(&self, node: NodeId, at: ReadAt) -> Option<ArrId> {
        self.node(node).arrangements.iter().copied().find(|a| self.arr(*a).covers(at))
    }
}

/// Resolves ids to fingerprints while fingerprinting operator plans.
pub struct FpCtx<'a> {
    pub g: &'a Graph,
}

impl FpCtx<'_> {
    pub fn node(&self, id: NodeId) -> &[u8] {
        &self.g.node(id).fp
    }
    pub fn arr(&self, id: ArrId) -> &[u8] {
        match self.g.arrs.get(id).and_then(|a| a.as_ref()) {
            Some(a) => &a.fp,
            None => &[],
        }
    }
}

/// Stable 128-bit FNV-1a hash (fingerprints must survive restarts and upgrades).
pub fn hash128(data: &[u8]) -> [u8; 16] {
    const OFFSET: u128 = 0x6c62272e07bb014262b821756295c58d;
    const PRIME: u128 = 0x0000000001000000000000000000013B;
    let mut h = OFFSET;
    for b in data {
        h ^= *b as u128;
        h = h.wrapping_mul(PRIME);
    }
    h.to_le_bytes()
}

/// Arrangements an operator reads (and holds references to).
pub fn operator_arrangements(kind: &NodeKind) -> Vec<ArrId> {
    match kind {
        NodeKind::Table { primary, .. } => vec![*primary],
        NodeKind::Join(p) => p.arrangements(),
        NodeKind::Reduce(p) => vec![p.input_arr, p.out_arr],
        NodeKind::Distinct { arr } => vec![*arr],
        NodeKind::SetOp { left_arr, right_arr, .. } => vec![*left_arr, *right_arr],
        NodeKind::TopK(p) => vec![p.input_arr],
        NodeKind::Window(p) => vec![p.input_arr],
        NodeKind::Recursive(p) => vec![p.out_arr],
        NodeKind::Temporal(p) => vec![p.events],
        _ => vec![],
    }
}

/// Consolidate a delta: sort rows by exact identity, sum weights, drop zeros.
pub fn consolidate(delta: &mut Delta) {
    if delta.len() <= 1 {
        delta.retain(|(_, w)| *w != 0);
        return;
    }
    delta.sort_unstable_by(|a, b| crate::types::datum::exact_row_cmp(&a.0, &b.0));
    // merge runs of equal rows in place (the later ones are dropped)
    delta.dedup_by(|cur, kept| {
        if crate::types::datum::exact_row_cmp(&cur.0, &kept.0) == std::cmp::Ordering::Equal {
            kept.1 += cur.1;
            true
        } else {
            false
        }
    });
    delta.retain(|(_, w)| *w != 0);
}

pub fn consolidate_errs(errs: &mut ErrDelta) {
    let mut m: BTreeMap<(String, String), i64> = BTreeMap::new();
    for (e, w) in errs.drain(..) {
        *m.entry(e).or_default() += w;
    }
    errs.extend(m.into_iter().filter(|(_, w)| *w != 0));
}

pub fn err_record(e: &SqlError) -> (String, String) {
    (e.code.clone(), e.message.clone())
}
