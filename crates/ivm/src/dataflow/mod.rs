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
pub mod delta;
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
    /// Creation sequence number. A node is created after its inputs, so
    /// sequence order is a topological order (ids are reused, see `Graph`).
    pub seq: u64,
    pub kind: NodeKind,
    pub inputs: Vec<NodeId>,
    /// Nodes that have this node as an input (the reverse of `inputs`).
    pub consumers: Vec<NodeId>,
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
    /// Creation sequence number (ids are reused).
    pub seq: u64,
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

/// Nodes and arrangements live in slots indexed by their ids; the slots of
/// freed ones are reused (a subscription to a query would otherwise leave
/// its slots behind for good), so creation order is kept as sequence
/// numbers.
#[derive(Default)]
pub struct Graph {
    pub nodes: Vec<Option<Node>>,
    pub arrs: Vec<Option<Arrangement>>,
    free_nodes: Vec<NodeId>,
    free_arrs: Vec<ArrId>,
    /// Nodes and arrangements created so far (the next sequence numbers).
    node_seq: u64,
    arr_seq: u64,
    node_by_fp: FxHashMap<Vec<u8>, NodeId>,
    arr_by_fp: FxHashMap<Vec<u8>, ArrId>,
    /// Live nodes in sequence order.
    topo: Vec<NodeId>,
    topo_dirty: bool,
    /// Temporal filter nodes: they run whenever the clock moves.
    temporal: std::collections::BTreeSet<NodeId>,
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
    /// Every hydrated temporal filter has released its events up to it, and
    /// schedules only events after the clock: none at or before it is due.
    /// None before the first step.
    stepped_now: Option<i64>,
    /// During a step, `stepped_now` before it: the events temporal filters
    /// release are those after it, up to `now`.
    pub(crate) released_through: Option<i64>,
    /// Error records currently in each node's contents (its accumulated
    /// output errors). Authoritative for nodes with a hydrated arrangement:
    /// their snapshots read the arrangement, which holds rows only.
    node_errors: FxHashMap<NodeId, BTreeMap<(String, String), i64>>,
    /// Nodes whose error records are in the durable mirror of the error
    /// state (`take_error_changes`): the arranged ones it has seen.
    errs_mirrored: rustc_hash::FxHashSet<NodeId>,
    /// Changes to the mirrored error state not yet taken.
    err_changes: BTreeMap<ErrorKey, i64>,
    /// Memory of the group caches of all `Reduce`s, against their share of
    /// the memory budget (`reduce::trim_caches`).
    pub group_caches: Arc<crate::memory::CacheMeter>,
    /// Per node, the index by all columns its spilled outputs are kept in.
    spill_layouts: parking_lot::Mutex<FxHashMap<NodeId, Arc<Layout>>>,
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

/// One record of the error state, by node fingerprint (see `ErrorState`).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ErrorKey {
    /// An error record in an arranged node's contents (weighted by its count).
    Node(Vec<u8>, (String, String)),
    /// The error of an operator's group, by group key (weight 1).
    Group(Vec<u8>, Vec<u8>, (String, String)),
}

fn add_weight<K: Ord>(m: &mut BTreeMap<K, i64>, k: K, w: i64) {
    match m.entry(k) {
        std::collections::btree_map::Entry::Occupied(mut e) => {
            *e.get_mut() += w;
            if *e.get() == 0 {
                e.remove();
            }
        }
        std::collections::btree_map::Entry::Vacant(e) => {
            if w != 0 {
                e.insert(w);
            }
        }
    }
}

/// Groups of an operator currently in error, by group key prefix.
pub type OpErrors = Arc<parking_lot::Mutex<GroupErrors>>;

/// The error records of an operator's groups in error (one per group, by
/// group key prefix), and, once the graph mirrors them durably
/// (`Graph::take_error_changes`), their changes since it last took them.
#[derive(Default, Debug, Clone)]
pub struct GroupErrors {
    groups: BTreeMap<Vec<u8>, (String, String)>,
    /// Weight changes by (group, error); None while not mirrored.
    changes: Option<GroupErrorChanges>,
}

type GroupErrorChanges = BTreeMap<(Vec<u8>, (String, String)), i64>;

impl GroupErrors {
    /// Set a group's error, returning the previous one.
    pub fn insert(&mut self, group: Vec<u8>, err: (String, String)) -> Option<(String, String)> {
        let Some(ch) = &mut self.changes else { return self.groups.insert(group, err) };
        let prev = self.groups.insert(group.clone(), err.clone());
        if prev.as_ref() != Some(&err) {
            if let Some(p) = &prev {
                add_weight(ch, (group.clone(), p.clone()), -1);
            }
            add_weight(ch, (group, err), 1);
        }
        prev
    }

    /// Clear a group's error, returning it.
    pub fn remove(&mut self, group: &[u8]) -> Option<(String, String)> {
        let prev = self.groups.remove(group);
        if let (Some(ch), Some(p)) = (&mut self.changes, &prev) {
            add_weight(ch, (group.to_vec(), p.clone()), -1);
        }
        prev
    }

    pub fn clear(&mut self) {
        let groups = std::mem::take(&mut self.groups);
        if let Some(ch) = &mut self.changes {
            for (g, e) in groups {
                add_weight(ch, (g, e), -1);
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    /// Is the group in error?
    pub fn contains(&self, group: &[u8]) -> bool {
        self.groups.contains_key(group)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &(String, String))> {
        self.groups.iter()
    }
}

/// The per-group error state of an operator, if it keeps one.
fn operator_errors(kind: &NodeKind) -> Option<&OpErrors> {
    match kind {
        NodeKind::Reduce(p) => Some(&p.errors),
        NodeKind::Recursive(p) => Some(&p.errors),
        NodeKind::Window(p) => Some(&p.errors),
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

    /// Store a new node (unreferenced, with a placeholder operator until
    /// `link` is called) in a free slot.
    fn insert_node(&mut self, kind: NodeKind, inputs: Vec<NodeId>, schema: Vec<ColumnType>, fp: Vec<u8>) -> NodeId {
        let id = match self.free_nodes.pop() {
            Some(id) => id,
            None => {
                self.nodes.push(None);
                self.nodes.len() - 1
            }
        };
        let seq = self.node_seq;
        self.node_seq += 1;
        self.nodes[id] = Some(Node {
            id,
            seq,
            kind,
            inputs,
            consumers: vec![],
            schema,
            fp: fp.clone(),
            refs: 0,
            arrangements: vec![],
            hydrated: false,
        });
        self.node_by_fp.insert(fp, id);
        // the newest node comes last in sequence order
        if !self.topo_dirty {
            self.topo.push(id);
        }
        id
    }

    /// Set a new node's operator: reference its inputs (and register it as
    /// their consumer) and the arrangements the operator reads.
    fn link(&mut self, id: NodeId, kind: NodeKind) {
        for i in self.node(id).inputs.clone() {
            let n = self.node_mut(i);
            n.refs += 1;
            n.consumers.push(id);
        }
        for a in operator_arrangements(&kind) {
            self.arrs[a].as_mut().expect("operator arrangement").refs += 1;
        }
        if matches!(kind, NodeKind::Temporal(_)) {
            self.temporal.insert(id);
        }
        self.node_mut(id).kind = kind;
    }

    /// Add (or find) a node. New nodes start with zero references (their
    /// consumers and pins add them); creating a node references its inputs
    /// and the arrangements its operator reads.
    pub fn add_node(&mut self, kind: NodeKind, inputs: Vec<NodeId>, schema: Vec<ColumnType>) -> NodeId {
        let fp = self.node_fp(&kind, &inputs);
        if let Some(&id) = self.node_by_fp.get(&fp) {
            return id;
        }
        let id = self.insert_node(NodeKind::Constant { rows: vec![] }, inputs, schema, fp);
        self.link(id, kind);
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
        let id = self.insert_node(NodeKind::Constant { rows: vec![] }, inputs, schema, fp);
        let arr = self.add_arrangement(env, id, key, specs, vec![]);
        patch(&mut kind, arr);
        self.link(id, kind);
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
        let id = self.insert_node(NodeKind::Constant { rows: vec![] }, inputs, schema, fp.clone());
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
        let arr = self.insert_arr(id, layout, tree, afp, hydrated, since);
        patch(&mut kind, arr);
        self.link(id, kind);
        id
    }

    /// Store a new (unreferenced) arrangement in a free slot.
    fn insert_arr(&mut self, node: NodeId, layout: Arc<Layout>, tree: TreeId, fp: Vec<u8>, hydrated: bool, since: Gsn) -> ArrId {
        let id = match self.free_arrs.pop() {
            Some(id) => id,
            None => {
                self.arrs.push(None);
                self.arrs.len() - 1
            }
        };
        let seq = self.arr_seq;
        self.arr_seq += 1;
        self.arrs[id] = Some(Arrangement { id, seq, node, layout, tree, refs: 0, fp: fp.clone(), hydrated, since });
        self.arr_by_fp.insert(fp, id);
        id
    }

    /// Nodes created so far (a mark for `gc_since`).
    pub fn node_count(&self) -> usize {
        self.node_seq as usize
    }

    /// Arrangements created so far (a mark for `gc_since`).
    pub fn arr_count(&self) -> usize {
        self.arr_seq as usize
    }

    /// Remove nodes and arrangements created at or after the given marks that
    /// ended up unreferenced (planning leftovers).
    pub fn gc_since(&mut self, env: &mut Env, node_mark: usize, arr_mark: usize) -> Result<()> {
        // newest first: consumers before their inputs
        let mut new: Vec<(u64, NodeId)> =
            self.live_nodes().filter(|n| n.seq >= node_mark as u64).map(|n| (n.seq, n.id)).collect();
        new.sort_unstable_by(|a, b| b.cmp(a));
        for (seq, id) in new {
            // (releasing a node may have freed one of its inputs already)
            if let Some(n) = &self.nodes[id]
                && n.seq == seq
                && n.refs == 0
            {
                self.node_mut(id).refs = 1;
                self.release_node(env, id)?;
            }
        }
        let new: Vec<(u64, ArrId)> =
            self.live_arrangements().filter(|a| a.seq >= arr_mark as u64).map(|a| (a.seq, a.id)).collect();
        for (seq, id) in new {
            if let Some(a) = &self.arrs[id]
                && a.seq == seq
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
        let id = self.insert_arr(node, layout, tree, full_fp, hydrated, since);
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
            self.free_arrs.push(id);
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
        self.free_nodes.push(id);
        self.node_by_fp.remove(&node.fp);
        let errs = self.node_errors.remove(&id);
        self.forget_mirrored_errors(id, &node, errs);
        self.spill_layouts.get_mut().remove(&id);
        self.counters.remove(&id);
        self.stats.remove(&id);
        self.temporal.remove(&id);
        self.topo_dirty = true;
        // arrangements used by this operator
        for a in operator_arrangements(&node.kind) {
            self.release_arr(env, a)?;
        }
        for i in node.inputs {
            if let Some(n) = self.nodes[i].as_mut()
                && let Some(p) = n.consumers.iter().position(|c| *c == id)
            {
                n.consumers.swap_remove(p);
            }
            self.release_node(env, i)?;
        }
        Ok(())
    }

    /// Topological order of live nodes.
    pub fn topo(&mut self) -> &[NodeId] {
        if self.topo_dirty {
            // nodes are created after their inputs, so creation order is topological
            let mut live: Vec<(u64, NodeId)> = self.live_nodes().map(|n| (n.seq, n.id)).collect();
            live.sort_unstable();
            self.topo = live.into_iter().map(|(_, id)| id).collect();
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
            visited(1);
            let w = h.weight(at);
            if w != 0 {
                out.push((layout.decode_row(k)?, w));
            }
            Ok(true)
        })?;
        Ok(out)
    }

    /// Rows of an arrangement with weights as of `at`, handed to `sink` in
    /// batches of at most `ops::BATCH_ROWS` rows and about 1 MiB of entries.
    pub fn scan_stream(&self, env: &mut Env, arr: ArrId, at: ReadAt, sink: &mut ops::RowSink) -> Result<()> {
        let a = self.arr(arr);
        let layout = &a.layout;
        let mut from: Option<Vec<u8>> = None;
        loop {
            let lo = match &from {
                Some(k) => std::ops::Bound::Excluded(k.as_slice()),
                None => std::ops::Bound::Unbounded,
            };
            let mut batch = Vec::new();
            let mut bytes = 0;
            let mut last = None;
            env.for_each(a.tree, lo, std::ops::Bound::Unbounded, &mut |k, h| {
                visited(1);
                let w = h.weight(at);
                if w != 0 {
                    batch.push((layout.decode_row(k)?, w));
                    bytes += k.len();
                    if batch.len() >= ops::BATCH_ROWS || bytes >= 1 << 20 {
                        last = Some(k.to_vec());
                        return Ok(false);
                    }
                }
                Ok(true)
            })?;
            if !batch.is_empty() {
                sink(env, batch)?;
            }
            match last {
                Some(k) => from = Some(k),
                None => return Ok(()),
            }
        }
    }

    /// The change from the contents of arrangement `old` to those of `new`
    /// (consolidated). Arrangements that encode rows alike (same key and
    /// columns) are merged in key order, so that only the entries that
    /// differ are decoded and kept; others are read and consolidated.
    pub fn diff_arrangements(&self, env: &mut Env, old: ArrId, new: ArrId) -> Result<Delta> {
        let (a, b) = (self.arr(old), self.arr(new));
        if old == new {
            return Ok(vec![]);
        }
        let (la, lb) = (&a.layout, &b.layout);
        let fps = |l: &Layout| {
            let mut fp = Vec::new();
            for k in &l.key {
                k.fingerprint(&mut fp);
            }
            fp
        };
        let alike = la.key_types == lb.key_types
            && la.specs == lb.specs
            && la.payload == lb.payload
            && la.row_len == lb.row_len
            && fps(la) == fps(lb);
        if !alike {
            let mut diff = self.scan_arr(env, new, ReadAt::Latest)?;
            diff.extend(self.scan_arr(env, old, ReadAt::Latest)?.into_iter().map(|(r, w)| (r, -w)));
            consolidate(&mut diff);
            return Ok(diff);
        }
        let (u, v) = (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded);
        let mut ca = env.cursor(a.tree, u, v);
        let mut cb = env.cursor(b.tree, u, v);
        let (mut x, mut y) = (env.cursor_next(&mut ca)?, env.cursor_next(&mut cb)?);
        let mut diff = Vec::new();
        loop {
            let (key, w) = match (&x, &y) {
                (None, None) => break,
                (Some((ka, ha)), Some((kb, hb))) if ka == kb => {
                    let r = (ka.clone(), hb.total() - ha.total());
                    (x, y) = (env.cursor_next(&mut ca)?, env.cursor_next(&mut cb)?);
                    r
                }
                (Some((ka, ha)), Some((kb, _))) if ka < kb => {
                    let r = (ka.clone(), -ha.total());
                    x = env.cursor_next(&mut ca)?;
                    r
                }
                (Some((ka, ha)), None) => {
                    let r = (ka.clone(), -ha.total());
                    x = env.cursor_next(&mut ca)?;
                    r
                }
                (_, Some((kb, hb))) => {
                    let r = (kb.clone(), hb.total());
                    y = env.cursor_next(&mut cb)?;
                    r
                }
            };
            visited(1);
            if w != 0 {
                diff.push((lb.decode_row(&key)?, w));
            }
        }
        // (equal rows are equal entries, but consolidation also orders them)
        consolidate(&mut diff);
        Ok(diff)
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
        let mut data = Vec::new();
        let errs = ops::snapshot_stream(self, env, node, &mut |_, batch| {
            data.extend(batch);
            Ok(())
        })?;
        consolidate(&mut data);
        Ok(Output { data, errs })
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
        if self.errs_mirrored.contains(&node) {
            let fp = &self.nodes[node].as_ref().expect("dangling node").fp;
            for (e, w) in errs {
                add_weight(&mut self.err_changes, ErrorKey::Node(fp.clone(), e.clone()), *w);
            }
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
        if let Some(old) = self.node_errors.remove(&node)
            && self.errs_mirrored.contains(&node)
        {
            let fp = &self.nodes[node].as_ref().expect("dangling node").fp;
            for (e, w) in old {
                add_weight(&mut self.err_changes, ErrorKey::Node(fp.clone(), e), -w);
            }
        }
        self.add_errors(node, errs);
    }

    /// A node leaves the graph: retract its mirrored error state.
    fn forget_mirrored_errors(&mut self, id: NodeId, node: &Node, errs: Option<BTreeMap<(String, String), i64>>) {
        if self.errs_mirrored.remove(&id) {
            for (e, w) in errs.into_iter().flatten() {
                add_weight(&mut self.err_changes, ErrorKey::Node(node.fp.clone(), e), -w);
            }
        }
        if let Some(op) = operator_errors(&node.kind) {
            let mut op = op.lock();
            if let Some(ch) = op.changes.take() {
                for ((g, e), w) in ch {
                    add_weight(&mut self.err_changes, ErrorKey::Group(node.fp.clone(), g, e), w);
                }
                for (g, e) in &op.groups {
                    add_weight(&mut self.err_changes, ErrorKey::Group(node.fp.clone(), g.clone(), e.clone()), -1);
                }
            }
        }
    }

    /// Changes to the durable error state since the last call, as weighted
    /// records (`ErrorKey`): summed over all calls, they are the error
    /// records of every arranged node (views created later over a node
    /// must see them) and the groups in error of every operator that keeps
    /// them. A node's whole state is returned the first time it is seen
    /// arranged (an operator's, the first time it is seen), and retracted
    /// when it loses its last arrangement or leaves the graph.
    pub fn take_error_changes(&mut self) -> Vec<(ErrorKey, i64)> {
        let mut out = std::mem::take(&mut self.err_changes);
        for n in self.nodes.iter().flatten() {
            let arranged = n.arrangements.iter().any(|a| self.arr(*a).hydrated);
            if arranged != self.errs_mirrored.contains(&n.id) {
                let sign = if arranged { 1 } else { -1 };
                for (e, w) in self.node_errors.get(&n.id).into_iter().flatten() {
                    add_weight(&mut out, ErrorKey::Node(n.fp.clone(), e.clone()), sign * w);
                }
                if arranged {
                    self.errs_mirrored.insert(n.id);
                } else {
                    self.errs_mirrored.remove(&n.id);
                }
            }
            if let Some(op) = operator_errors(&n.kind) {
                let mut op = op.lock();
                match op.changes.replace(BTreeMap::new()) {
                    Some(ch) => {
                        for ((g, e), w) in ch {
                            add_weight(&mut out, ErrorKey::Group(n.fp.clone(), g, e), w);
                        }
                    }
                    None => {
                        for (g, e) in &op.groups {
                            add_weight(&mut out, ErrorKey::Group(n.fp.clone(), g.clone(), e.clone()), 1);
                        }
                    }
                }
            }
        }
        out.into_iter().collect()
    }

    /// Restore error state into a graph rebuilt for recovery (before
    /// hydrating whatever could not be recovered). `mirrored`: the state
    /// was read from the durable mirror that `take_error_changes` keeps, so
    /// it is not returned again, and the records of nodes that no longer
    /// exist are retracted from it; otherwise (a checkpoint of the earlier
    /// format) the next `take_error_changes` returns all of it.
    pub fn restore_error_state(&mut self, st: ErrorState, mirrored: bool) {
        for (fp, errs) in st.nodes {
            match self.node_by_fp.get(&fp) {
                Some(&id) => {
                    self.set_errors(id, &errs);
                    if mirrored {
                        self.errs_mirrored.insert(id);
                    }
                }
                None if mirrored => {
                    for (e, w) in errs {
                        add_weight(&mut self.err_changes, ErrorKey::Node(fp.clone(), e), -w);
                    }
                }
                None => {}
            }
        }
        for (fp, groups) in st.operators {
            match self.node_by_fp.get(&fp).and_then(|&id| operator_errors(&self.node(id).kind)) {
                Some(errs) => {
                    let mut errs = errs.lock();
                    errs.groups = groups.into_iter().collect();
                    errs.changes = mirrored.then(BTreeMap::new);
                }
                None if mirrored => {
                    for (g, e) in groups {
                        add_weight(&mut self.err_changes, ErrorKey::Group(fp.clone(), g, e), -1);
                    }
                }
                None => {}
            }
        }
    }

    /// Give a node the error records a checkpoint of an earlier format
    /// kept only with the materialized view it is the root of, if it has
    /// none of its own.
    pub fn restore_view_errors(&mut self, node: NodeId, errs: &ErrDelta) {
        if !self.node_errors.contains_key(&node) {
            self.set_errors(node, errs);
        }
    }

    // ------------------------------------------------------------------ ticks

    /// Process one GSN. `changes` holds base table changes keyed by table node.
    /// Returns the output of every node that produced something.
    pub fn step(&mut self, env: &mut Env, gsn: Gsn, changes: FxHashMap<NodeId, Vec<TableChange>>) -> Result<FxHashMap<NodeId, Output>> {
        let all = self.live_nodes().map(|n| n.id).collect();
        self.step_observed(env, gsn, changes, all)
    }

    /// `step`, returning the rows only of the `observed` nodes (and the
    /// error records of every node that has some).
    pub fn step_observed(
        &mut self,
        env: &mut Env,
        gsn: Gsn,
        changes: FxHashMap<NodeId, Vec<TableChange>>,
        observed: FxHashSet<NodeId>,
    ) -> Result<FxHashMap<NodeId, Output>> {
        let mut input = self.step_begin(gsn, observed);
        for (node, ch) in changes {
            if let Err(e) = self.step_feed(env, &mut input, node, ch) {
                input.abort(env)?;
                return Err(e);
            }
        }
        self.step_finish(env, input)
    }

    /// Start processing a GSN, whose base-table changes are then fed in
    /// batches (`step_feed`) before the GSN is propagated (`step_finish`),
    /// which returns the rows of the `observed` nodes.
    pub fn step_begin(&self, gsn: Gsn, observed: FxHashSet<NodeId>) -> StepInput {
        StepInput { gsn, tables: FxHashMap::default(), observed }
    }

    /// Is node `id`'s output at a step read, by a consumer that runs (a
    /// hydrated one) or by the caller (`observed`)? An output nobody reads
    /// goes to the node's arrangements directly when it is large.
    fn is_read(&self, id: NodeId, observed: &FxHashSet<NodeId>) -> bool {
        observed.contains(&id) || self.node(id).consumers.iter().any(|c| self.node(*c).hydrated)
    }

    /// Feed changes of table node `node` at the GSN of `input`, in their
    /// order (a table's changes may come in several batches). They are
    /// applied in chunks of about `spill_bytes`, each to the table's
    /// primary index before the next chunk reads it, so the dataflow never
    /// holds a commit's changes as a whole. The table's delta is
    /// accumulated (spilling when large) for `step_finish`, which applies
    /// it to the table's other arrangements; if nothing reads it, a large
    /// one goes to them directly, through `TreeBuilder`s.
    pub fn step_feed(&self, env: &mut Env, input: &mut StepInput, node: NodeId, changes: Vec<TableChange>) -> Result<()> {
        let n = self.node(node);
        if !n.hydrated || changes.is_empty() {
            return Ok(());
        }
        let NodeKind::Table { primary, .. } = n.kind else { return Err(SqlError::internal("changes fed to a node that is not a table")) };
        let started = std::time::Instant::now();
        let gsn = input.gsn;
        let read = self.is_read(node, &input.observed);
        let fed = input.tables.entry(node).or_insert_with(|| {
            // the primary index is updated as the changes are fed
            let targets = self.delta_targets(node, Some(primary));
            FedTable {
                out: if read { delta::DeltaBuf::new(gsn, targets) } else { delta::DeltaBuf::direct(gsn, targets) },
                carry: ops::TableCarry::default(),
                in_rows: 0,
                out_rows: 0,
                nanos: 0,
            }
        });
        fed.in_rows += changes.len();
        let budget = delta::spill_bytes(env);
        let mut changes = changes.into_iter().peekable();
        while changes.peek().is_some() {
            let mut chunk = Vec::new();
            let mut bytes = 0;
            while (chunk.is_empty() || bytes < budget)
                && let Some(ch) = changes.next()
            {
                bytes += match &ch {
                    TableChange::Upsert { row, .. } | TableChange::Insert { row } | TableChange::Remove { row } => row_bytes(row),
                    TableChange::Delete { key } => row_bytes(key),
                    TableChange::Truncate => 0,
                };
                chunk.push(ch);
            }
            ops::table_step(self, env, n, chunk, &mut fed.carry, &mut |env, delta| {
                let arr = self.arr(primary);
                if arr.hydrated {
                    let mut ups = Vec::with_capacity(delta.len());
                    for (row, w) in &delta {
                        ups.push((arr.layout.encode_row(row)?, *w));
                    }
                    env.apply(arr.tree, &mut ups, gsn)?;
                }
                fed.out_rows += delta.len();
                fed.out.extend_consolidated(env, delta)
            })?;
        }
        fed.nanos += started.elapsed().as_nanos() as u64;
        Ok(())
    }

    /// Propagate the GSN of `input` through the graph. Returns the outputs
    /// of the nodes that produced something: the rows of the observed
    /// nodes, the error records of all. A node's output is consolidated, and
    /// spilled into a temporary tree when large, which its consumers read
    /// in batches; it is freed once its consumers have run (a node nobody
    /// reads applies a large output to its arrangements as it goes).
    pub fn step_finish(&mut self, env: &mut Env, input: StepInput) -> Result<FxHashMap<NodeId, Output>> {
        let StepInput { gsn, mut tables, observed } = input;
        let mut deltas: FxHashMap<NodeId, delta::NodeDelta> = FxHashMap::default();
        let mut outputs: FxHashMap<NodeId, Output> = FxHashMap::default();
        let r = self.propagate(env, gsn, &mut tables, &observed, &mut deltas, &mut outputs);
        if let Err(e) = r {
            for (_, fed) in tables.drain() {
                fed.abort(env)?;
            }
            for (_, d) in deltas.drain() {
                d.release(env)?;
            }
            return Err(e);
        }
        Ok(outputs)
    }

    /// Run the nodes that can produce something at this GSN: the tables
    /// changed, the consumers of nodes that produced output and, when the
    /// clock moved, the temporal filters. They run in sequence (topological)
    /// order, so every input is done before its consumers; a node's output
    /// is kept until the consumers it queued have run.
    fn propagate(
        &mut self,
        env: &mut Env,
        gsn: Gsn,
        tables: &mut FxHashMap<NodeId, FedTable>,
        observed: &FxHashSet<NodeId>,
        deltas: &mut FxHashMap<NodeId, delta::NodeDelta>,
        outputs: &mut FxHashMap<NodeId, Output>,
    ) -> Result<()> {
        let time_moved = self.stepped_now != Some(self.now);
        self.released_through = self.stepped_now;
        self.stepped_now = Some(self.now);
        // (a node is only ever queued by nodes before it, so none is queued
        // again once it ran)
        let mut queue: std::collections::BTreeSet<(u64, NodeId)> = tables.keys().map(|&id| (self.node(id).seq, id)).collect();
        if time_moved {
            queue.extend(self.temporal.iter().map(|&id| (self.node(id).seq, id)));
        }
        // consumers still to run, per output kept
        let mut readers: FxHashMap<NodeId, usize> = FxHashMap::default();
        while let Some((_, id)) = queue.pop_first() {
            considered();
            let node = self.node(id);
            if !node.hydrated {
                continue;
            }
            let inputs = node.inputs.clone();
            let ran = if let NodeKind::Table { .. } = node.kind {
                match tables.remove(&id) {
                    Some(fed) => {
                        let started = std::time::Instant::now();
                        let (in_rows, out_rows, nanos) = (fed.in_rows, fed.out_rows, fed.nanos);
                        // (the primary index was updated as the changes were fed)
                        let out = fed.out.into_delta(env)?;
                        if let Err(e) = out.apply(env, gsn) {
                            out.release(env)?;
                            return Err(e);
                        }
                        Some((out, in_rows, out_rows, nanos + started.elapsed().as_nanos() as u64))
                    }
                    None => None,
                }
            } else {
                let empty = delta::NodeDelta::default();
                let ins: Vec<&delta::NodeDelta> = node.inputs.iter().map(|i| deltas.get(i).unwrap_or(&empty)).collect();
                let started = std::time::Instant::now();
                let in_rows = ins.iter().map(|o| o.rows.len()).sum();
                // a node nobody reads hands a large output to its arrangements directly
                let targets = self.delta_targets(id, None);
                let rows = if self.is_read(id, observed) { delta::DeltaBuf::new(gsn, targets) } else { delta::DeltaBuf::direct(gsn, targets) };
                let out = ops::step_node(self, env, node, &ins, gsn, deltas, rows)?;
                if !out.is_empty() {
                    self.add_errors(id, &out.errs);
                    if let Err(e) = out.apply(env, gsn) {
                        out.release(env)?;
                        return Err(e);
                    }
                }
                let out_rows = out.rows.len();
                Some((out, in_rows, out_rows, started.elapsed().as_nanos() as u64))
            };
            if let Some((out, in_rows, out_rows, nanos)) = ran {
                self.count_step(id, in_rows, out_rows, nanos);
                if !out.is_empty() {
                    let st = self.stats.entry(id).or_default();
                    st.ticks += 1;
                    st.rows += out_rows as u64;
                    // its consumers run (once per input they read it as)
                    let mut n = 0;
                    for &c in &self.node(id).consumers {
                        let consumer = self.node(c);
                        if consumer.hydrated {
                            n += 1;
                            queue.insert((consumer.seq, c));
                        }
                    }
                    if n > 0 {
                        readers.insert(id, n);
                        deltas.insert(id, out);
                    } else {
                        retire(env, id, out, outputs, observed)?;
                    }
                }
            }
            // outputs whose consumers have all run
            for i in inputs {
                if let Some(n) = readers.get_mut(&i) {
                    *n -= 1;
                    if *n == 0 {
                        readers.remove(&i);
                        if let Some(d) = deltas.remove(&i) {
                            retire(env, i, d, outputs, observed)?;
                        }
                    }
                }
            }
        }
        for (id, d) in std::mem::take(deltas) {
            retire(env, id, d, outputs, observed)?;
        }
        Ok(())
    }

    /// Set the clock of a graph rebuilt for recovery to the checkpoint's:
    /// the checkpointed temporal filters have released every event up to it.
    pub fn restore_clock(&mut self, now: i64) {
        self.now = now;
        self.stepped_now = Some(now);
    }

    /// The encodings a node's delta is kept in, and the trees it is applied
    /// to: its hydrated arrangements (`applied`, if any, is up to date
    /// already), or an index by all columns for a node without any.
    fn delta_targets(&self, node: NodeId, applied: Option<ArrId>) -> Vec<delta::Target> {
        let mut targets: Vec<delta::Target> = self
            .node(node)
            .arrangements
            .iter()
            .filter(|a| self.arr(**a).hydrated)
            .map(|&a| delta::Target { arr: Some(a), layout: self.arr(a).layout.clone(), apply_to: (Some(a) != applied).then_some(self.arr(a).tree) })
            .collect();
        if targets.is_empty() {
            let layout = self.spill_layouts.lock().entry(node).or_insert_with(|| Arc::new(delta::spill_layout(&self.node(node).schema))).clone();
            targets.push(delta::Target { arr: None, layout, apply_to: None });
        }
        targets
    }

    fn count_step(&mut self, id: NodeId, in_rows: usize, out_rows: usize, nanos: u64) {
        for c in [self.counters.entry(id).or_default(), &mut self.step_totals] {
            c.steps += 1;
            c.in_rows += in_rows as u64;
            c.out_rows += out_rows as u64;
            c.nanos += nanos;
        }
    }

    // ------------------------------------------------------------------ hydration

    /// Bring every new node and arrangement up to date as of `gsn`, computing
    /// their contents from existing state (never from the sources). The
    /// error records in their contents are kept per node (`errors_of`).
    /// Memory stays bounded: a node's snapshot is streamed into its new
    /// arrangements a batch at a time, and each arrangement is built by an
    /// external sort (`TreeBuilder`).
    pub fn hydrate(&mut self, env: &mut Env, gsn: Gsn) -> Result<()> {
        let order: Vec<NodeId> = self.topo().to_vec();
        for id in order {
            let needs_node = !self.node(id).hydrated;
            if needs_node {
                // stateful operators initialize their own arrangements here
                ops::hydrate_node(self, env, id, gsn)?;
            }
            let pending_arrs: Vec<ArrId> =
                self.node(id).arrangements.iter().copied().filter(|a| !self.arr(*a).hydrated).collect();
            if !pending_arrs.is_empty() {
                let layouts: Vec<Arc<Layout>> = pending_arrs.iter().map(|a| self.arr(*a).layout.clone()).collect();
                let mut builders: Vec<crate::storage::TreeBuilder> =
                    pending_arrs.iter().map(|a| crate::storage::TreeBuilder::new(env, self.arr(*a).tree, gsn)).collect();
                // the rows go to every new arrangement, in their key orders
                let r = ops::snapshot_stream(self, env, id, &mut |env, batch| {
                    for (b, layout) in builders.iter_mut().zip(&layouts) {
                        for (row, w) in &batch {
                            b.push(env, layout.encode_row(row)?, *w)?;
                        }
                    }
                    Ok(())
                });
                let errs = match r {
                    Ok(errs) => errs,
                    Err(e) => {
                        for b in builders {
                            b.abort(env)?;
                        }
                        return Err(e);
                    }
                };
                delta::finish_all(env, builders)?;
                for a in pending_arrs {
                    let a = self.arrs[a].as_mut().unwrap();
                    a.hydrated = true;
                    a.since = gsn;
                }
                // the arrangement holds the rows; the errors are kept here
                self.set_errors(id, &errs);
            }
            self.node_mut(id).hydrated = true;
        }
        // Window operators whose partitions in error are not known (neither
        // computed above nor restored from a checkpoint: a new operator that
        // had no snapshot to take, or a checkpoint of an older version)
        // compute them now, so that their steps can change partitions in
        // part.
        let unknown: Vec<window::WindowPlan> = self
            .live_nodes()
            .filter_map(|n| match &n.kind {
                NodeKind::Window(p) if n.hydrated && !p.errors_known.load(std::sync::atomic::Ordering::Acquire) => Some(p.clone()),
                _ => None,
            })
            .collect();
        for p in unknown {
            // (only the state is wanted: the rows are dropped as they come)
            let mut drop_rows = |_: &mut Env, _: Delta| Ok(());
            let mut out = ops::SnapOut::new(&mut drop_rows);
            window::snapshot(self, env, &p, &mut out)?;
            out.finish(env)?;
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
        // (its group caches count in the engine's share of the budget)
        let mut g = Graph {
            now: self.now,
            stepped_now: self.stepped_now,
            released_through: self.released_through,
            // (it creates no nodes: ids and sequence numbers stay those of the graph)
            node_seq: self.node_seq,
            arr_seq: self.arr_seq,
            temporal: self.temporal.iter().copied().filter(|n| keep_nodes.contains(n)).collect(),
            topo_dirty: true,
            group_caches: self.group_caches.clone(),
            ..Graph::default()
        };
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
                    NodeKind::Window(p) => {
                        p.errors = copy(&p.errors);
                        p.errors_known = Arc::new(std::sync::atomic::AtomicBool::new(p.errors_known.load(std::sync::atomic::Ordering::Acquire)));
                    }
                    _ => {}
                }
                Some(Node {
                    id: n.id,
                    seq: n.seq,
                    kind,
                    inputs: n.inputs.clone(),
                    // (those outside the fork are not stepped there)
                    consumers: n.consumers.iter().copied().filter(|c| keep_nodes.contains(c)).collect(),
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
        self.temporal.retain(|n| keep.contains(n));
        for n in self.nodes.iter_mut().flatten() {
            n.consumers.retain(|c| keep.contains(c));
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
            if let (NodeKind::Window(m), NodeKind::Window(t)) = (&mine.kind, &theirs.kind) {
                m.errors_known.store(t.errors_known.load(std::sync::atomic::Ordering::Acquire), std::sync::atomic::Ordering::Release);
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

/// Base-table changes of one GSN, fed in batches (`Graph::step_feed`).
pub struct StepInput {
    gsn: Gsn,
    tables: FxHashMap<NodeId, FedTable>,
    observed: FxHashSet<NodeId>,
}

impl StepInput {
    /// Drop what was accumulated (after a failure).
    pub fn abort(self, env: &mut Env) -> Result<()> {
        for (_, fed) in self.tables {
            fed.abort(env)?;
        }
        Ok(())
    }
}

/// A table node's changes fed so far at a GSN: its primary index is up to
/// date, its delta accumulated (and, if nothing reads it and it is large,
/// applied to its other arrangements).
struct FedTable {
    out: delta::DeltaBuf,
    carry: ops::TableCarry,
    in_rows: usize,
    out_rows: usize,
    nanos: u64,
}

impl FedTable {
    fn abort(self, env: &mut Env) -> Result<()> {
        self.out.abort(env)
    }
}

/// Hand a node's output over to the caller of `step` if it is observed or
/// has error records, and free its rows.
fn retire(env: &mut Env, id: NodeId, mut d: delta::NodeDelta, outputs: &mut FxHashMap<NodeId, Output>, observed: &FxHashSet<NodeId>) -> Result<()> {
    let errs = std::mem::take(&mut d.errs);
    if observed.contains(&id) {
        let data = d.to_vec(env)?;
        outputs.insert(id, Output { data, errs });
    } else if !errs.is_empty() {
        outputs.insert(id, Output { data: Vec::new(), errs });
    }
    d.release(env)
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

/// Estimated memory of a row (its values and what they point to, shared or
/// not), for bounding batches.
pub fn row_bytes(row: &[crate::types::Datum]) -> usize {
    use crate::types::Datum;
    std::mem::size_of_val(row)
        + 24
        + row
            .iter()
            .map(|d| match d {
                Datum::Text(s) | Datum::Json(s) => s.len() + 16,
                Datum::Bytea(b) => b.len() + 16,
                Datum::Numeric(_) | Datum::Jsonb(_) | Datum::Array(_) => 64,
                _ => 0,
            })
            .sum::<usize>()
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

#[cfg(test)]
thread_local! {
    /// Work done on this thread: index entries operators visited and nodes
    /// a step considered (see `visited`).
    pub(crate) static WORK: std::cell::Cell<(u64, u64)> = const { std::cell::Cell::new((0, 0)) };
}

/// Count index entries an operator visits: tests check that the work of a
/// step does not grow with what the step cannot change (a count, unlike
/// timings). Nothing outside tests.
#[inline]
pub(crate) fn visited(_n: u64) {
    #[cfg(test)]
    WORK.with(|w| {
        let (e, s) = w.get();
        w.set((e + _n, s))
    });
}

/// Count a node a step considers running (tests, as `visited`).
#[inline]
fn considered() {
    #[cfg(test)]
    WORK.with(|w| {
        let (e, s) = w.get();
        w.set((e, s + 1))
    });
}
