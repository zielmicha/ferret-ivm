//! Storage: an ordered MVCC index keyed by byte strings (memcomparable
//! (key, value) encodings) holding (GSN, weight) histories, augmented with
//! monoid summaries per subtree.
//!
//! The tree is copy-on-write at checkpoint granularity. Nodes live in a node
//! arena that acts as the buffer pool: clean nodes are evicted under memory
//! pressure (CLOCK), dirty nodes stay until the next checkpoint writes them
//! into the active append-only segment. A checkpoint is: write dirty nodes
//! bottom-up, fsync, append a manifest record naming the new roots. Sealed
//! segments are immutable; space is reclaimed by relocating the live nodes of
//! old segments (found through per-subtree `min_seg` augmentation) and
//! deleting segments once nothing references them.
//!
//! Loaded nodes are shared (`Arc`) and copied on write when shared: a fork
//! of the environment ([`Env::fork`]) is an independent version of every
//! tree as of the moment it was taken, readable and writable on another
//! thread, and a checkpoint is written from such a version of the dirty
//! nodes ([`Env::begin_checkpoint`]) while the environment goes on.

pub mod history;
mod inspect;
pub mod manifest;
pub mod node;
pub mod segment;
pub mod summary;

pub use history::{History, ReadAt};
pub use inspect::TreeResidency;
pub use summary::{AggKind, AggState, FloatSum, Summary};

use crate::error::{Result, SqlError};
use manifest::{CheckpointRecord, Manifest, TreeRecord};
use node::{entry_size, Child, DiskLoc, LeafEntry, Node, NodePtr};
use rustc_hash::FxHashMap;
use segment::SegmentStore;
use std::collections::{BTreeMap, VecDeque};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Global sequence number: the engine's single clock.
pub type Gsn = u64;
pub type TreeId = u64;

/// Computes a tree's per-node summaries from leaf entries.
pub trait Summarizer: Send + Sync {
    fn spec(&self) -> &[AggKind];
    /// Fold one entry (its key bytes and current total weight) into `acc`.
    fn add_entry(&self, acc: &mut Summary, key: &[u8], weight: i64) -> Result<()>;
    /// Fold several entries into `acc`.
    fn add_entries(&self, acc: &mut Summary, entries: &mut dyn Iterator<Item = (&[u8], i64)>) -> Result<()> {
        for (k, w) in entries {
            self.add_entry(acc, k, w)?;
        }
        Ok(())
    }
    /// Update `acc`, a summary covering the entry, for the entry's total
    /// weight changing from `before` to `after`. False when `acc` must be
    /// recomputed from the node's contents instead.
    fn update_entry(&self, acc: &mut Summary, key: &[u8], before: i64, after: i64) -> Result<bool> {
        let _ = key;
        acc.count += after - before;
        Ok(self.spec().is_empty())
    }
}

/// Summarizer maintaining only the weighted entry count.
pub struct CountSummarizer;

impl Summarizer for CountSummarizer {
    fn spec(&self) -> &[AggKind] {
        &[]
    }
    fn add_entry(&self, acc: &mut Summary, _key: &[u8], weight: i64) -> Result<()> {
        acc.count += weight;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct EnvOptions {
    pub cache_bytes: usize,
    pub leaf_bytes: usize,
    pub internal_bytes: usize,
    pub segment_bytes: u64,
}

impl Default for EnvOptions {
    fn default() -> Self {
        EnvOptions { cache_bytes: 1 << 30, leaf_bytes: 16 << 10, internal_bytes: 16 << 10, segment_bytes: 256 << 20 }
    }
}

const NONE: u32 = u32::MAX;

#[derive(Clone)]
struct Slot {
    /// Shared with forks and checkpoint jobs, which hold the version they
    /// were taken at: modified through `node_mut` / `take_node`, which copy
    /// a shared node.
    node: Option<Arc<Node>>,
    disk: Option<DiskLoc>,
    dirty: bool,
    /// Modification stamp (`Env::stamp` when the node's contents last
    /// changed or it became dirty): a checkpoint job's image of a dirty node
    /// is still current when its stamp is unchanged.
    version: u64,
    /// In-place leaf edits since the leaf's entries were last compacted.
    edits: u32,
    parent: u32,
    tree: TreeId,
    loaded_children: u32,
    refbit: bool,
    bytes: usize,
}

#[derive(Clone)]
struct TreeState {
    root: Option<NodePtr>,
    summary: Summary,
    min_seg: u32,
    summarizer: Arc<dyn Summarizer>,
    /// False for trees recovered from the manifest but not yet attached.
    attached: bool,
}

/// What a checkpoint recovered on open.
pub struct Recovered {
    pub gsn: Gsn,
    pub app: Vec<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct EnvStats {
    pub trees: usize,
    pub loaded_nodes: usize,
    pub dirty_nodes: usize,
    pub mem_bytes: usize,
    pub segments: Vec<(u32, u64, u64)>,
    pub checkpoint_gsn: Gsn,
}

pub struct Env {
    #[allow(dead_code)]
    dir: PathBuf,
    opts: EnvOptions,
    segs: SegmentStore,
    slots: Vec<Option<Slot>>,
    free_slots: Vec<u32>,
    mem_bytes: usize,
    /// Bytes of dirty nodes (maintained with the dirty flags).
    dirty_bytes: usize,
    clock_hand: usize,
    trees: FxHashMap<TreeId, TreeState>,
    next_tree_id: TreeId,
    /// segment id -> (total bytes written, live bytes referenced)
    seg_live: BTreeMap<u32, (u64, u64)>,
    frontier: Gsn,
    checkpoint_seq: u64,
    checkpoint_gsn: Gsn,
    /// The manifest (None in forks, and while a checkpoint job holds it
    /// with the segment writer).
    manifest: Option<Manifest>,
    /// Source of `Slot::version` stamps.
    stamp: u64,
    /// Nodes written by `flush_node` (in checkpoint jobs: what the parent
    /// environment marks clean when the job is done).
    flushed: Option<Vec<Flushed>>,
    /// Subtrees of dropped trees still on disk, not released yet: reading
    /// them to release their nodes takes I/O, which the next checkpoint job
    /// does (off the engine thread). Their bytes count as live until then.
    releasing: Vec<DiskLoc>,
    /// Nodes released by a checkpoint job (the parent environment does not
    /// count them as live any more when the job is done).
    released: Vec<DiskLoc>,
    /// Node frames read from segments (`segment_reads`).
    reads: u64,
}

/// A node a checkpoint job wrote: its slot and stamp in the parent
/// environment, its location and the `min_seg` of its subtree.
struct Flushed {
    slot: u32,
    version: u64,
    loc: DiskLoc,
    min_seg: u32,
}

/// A checkpoint in progress (`Env::begin_checkpoint`): a version of the
/// dirty nodes with the store's append side and manifest.
pub struct CheckpointJob {
    env: Env,
    gsn: Gsn,
}

/// A written checkpoint, for `Env::finish_checkpoint`.
pub struct CheckpointDone {
    segs: SegmentStore,
    manifest: Manifest,
    flushed: Vec<Flushed>,
    released: Vec<DiskLoc>,
    deleted: Vec<u32>,
    seq: u64,
    gsn: Gsn,
}

impl CheckpointJob {
    pub fn gsn(&self) -> Gsn {
        self.gsn
    }

    /// Write the nodes, make them durable and commit the checkpoint with the
    /// application state `app` (on any thread). On an error nothing is
    /// committed, and the environment cannot take checkpoints any more.
    pub fn run(mut self, app: &[u8]) -> Result<CheckpointDone> {
        let deleted = self.env.write_checkpoint(self.gsn, app)?;
        let env = self.env;
        Ok(CheckpointDone {
            segs: env.segs,
            manifest: env.manifest.expect("checkpoint job without manifest"),
            flushed: env.flushed.unwrap_or_default(),
            released: env.released,
            deleted,
            seq: env.checkpoint_seq,
            gsn: env.checkpoint_gsn,
        })
    }
}

/// A stateless range cursor: buffers one leaf at a time and re-seeks from the
/// root for the next leaf, so it never pins nodes and tolerates eviction.
pub struct Cursor {
    tree: TreeId,
    lo: Bound<Vec<u8>>,
    hi: Bound<Vec<u8>>,
    buf: VecDeque<(Box<[u8]>, History)>,
    done: bool,
}

fn borrow_bound(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match b {
        Bound::Included(v) => Bound::Included(v.as_slice()),
        Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn own_bound(b: Bound<&[u8]>) -> Bound<Vec<u8>> {
    match b {
        Bound::Included(v) => Bound::Included(v.to_vec()),
        Bound::Excluded(v) => Bound::Excluded(v.to_vec()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

#[inline]
#[allow(dead_code)]
fn above_lo(key: &[u8], lo: Bound<&[u8]>) -> bool {
    match lo {
        Bound::Unbounded => true,
        Bound::Included(a) => key >= a,
        Bound::Excluded(a) => key > a,
    }
}

#[inline]
fn below_hi(key: &[u8], hi: Bound<&[u8]>) -> bool {
    match hi {
        Bound::Unbounded => true,
        Bound::Included(b) => key <= b,
        Bound::Excluded(b) => key < b,
    }
}

/// Child range [clo, chi) fully inside [lo, hi)?
fn contains(lo: Bound<&[u8]>, hi: Bound<&[u8]>, clo: Option<&[u8]>, chi: Option<&[u8]>) -> bool {
    let lo_ok = match (lo, clo) {
        (Bound::Unbounded, _) => true,
        (_, None) => false,
        (Bound::Included(a), Some(c)) => a <= c,
        (Bound::Excluded(a), Some(c)) => a < c,
    };
    let hi_ok = match (hi, chi) {
        (Bound::Unbounded, _) => true,
        (_, None) => false,
        (Bound::Excluded(b), Some(c)) => c <= b,
        (Bound::Included(b), Some(c)) => c <= b,
    };
    lo_ok && hi_ok
}

/// Child range [clo, chi) disjoint from [lo, hi)?
fn disjoint(lo: Bound<&[u8]>, hi: Bound<&[u8]>, clo: Option<&[u8]>, chi: Option<&[u8]>) -> bool {
    let below = match (lo, chi) {
        (Bound::Unbounded, _) | (_, None) => false,
        (Bound::Included(a), Some(c)) | (Bound::Excluded(a), Some(c)) => c <= a,
    };
    let above = match (hi, clo) {
        (Bound::Unbounded, _) | (_, None) => false,
        (Bound::Excluded(b), Some(c)) => c >= b,
        (Bound::Included(b), Some(c)) => c > b,
    };
    below || above
}

/// Index of the child whose range contains `key`.
#[inline]
fn route(children: &[Child], key: &[u8]) -> usize {
    children[1..].partition_point(|c| &*c.sep <= key)
}

/// Children overlapping [lo, hi), with whether each is fully inside. Only
/// the first and last can be partial; children are found by binary search.
fn overlapping(children: &[Child], lo: Bound<&[u8]>, hi: Bound<&[u8]>) -> Vec<(usize, bool)> {
    let n = children.len();
    let start = match lo {
        Bound::Unbounded => 0,
        Bound::Included(k) | Bound::Excluded(k) => route(children, k),
    };
    // one past the last child starting below hi
    let end = match hi {
        Bound::Unbounded => n,
        Bound::Excluded(k) => 1 + children[1..].partition_point(|c| &*c.sep < k),
        Bound::Included(k) => 1 + children[1..].partition_point(|c| &*c.sep <= k),
    };
    let mut out = Vec::with_capacity(end.saturating_sub(start));
    for i in start..end.max(start) {
        let edge = i == start || i + 1 == end;
        let full = !edge || {
            let clo = if i == 0 { None } else { Some(&*children[i].sep) };
            let chi = if i + 1 < n { Some(&*children[i + 1].sep) } else { None };
            if disjoint(lo, hi, clo, chi) {
                continue;
            }
            contains(lo, hi, clo, chi)
        };
        out.push((i, full));
    }
    out
}

impl Env {
    /// Open or create the environment in `dir`. Returns the recovered
    /// checkpoint (if any). Trees from the checkpoint must be re-attached with
    /// their summarizers via [`Env::attach_tree`] before use.
    pub fn open(dir: &Path, opts: EnvOptions) -> Result<(Env, Option<Recovered>)> {
        if !dir.exists() {
            std::fs::create_dir_all(dir)?;
            if let Some(parent) = dir.parent().filter(|p| !p.as_os_str().is_empty()) {
                std::fs::File::open(parent)?.sync_all()?;
            }
        }
        // Without a checkpoint record, opening deletes every segment file but
        // the first and truncates that one: refuse, leaving every file as it
        // is, when segment files hold data that a lost manifest may have
        // described (a manifest without a valid record: Manifest::open).
        if !dir.join("MANIFEST").exists() {
            let mut with_data = Vec::new();
            for e in std::fs::read_dir(dir)? {
                let e = e?;
                let name = e.file_name().to_string_lossy().into_owned();
                if name.starts_with("seg-") && name.ends_with(".dat") && e.metadata()?.len() > 0 {
                    with_data.push(name);
                }
            }
            if !with_data.is_empty() {
                with_data.sort();
                return Err(SqlError::corrupt(format!(
                    "{} is missing but segment files hold data ({}); refusing to start, the store is left untouched",
                    dir.join("MANIFEST").display(),
                    with_data.join(", ")
                )));
            }
        }
        let (manifest, rec) = Manifest::open(dir)?;
        let (active_seg, active_len) = rec.as_ref().map(|r| (r.active_seg, r.active_len)).unwrap_or((1, 0));
        let segs = SegmentStore::open(dir, active_seg, active_len, opts.segment_bytes)?;
        let mut env = Env {
            dir: dir.to_path_buf(),
            opts,
            segs,
            slots: Vec::new(),
            free_slots: Vec::new(),
            mem_bytes: 0,
            dirty_bytes: 0,
            clock_hand: 0,
            trees: FxHashMap::default(),
            next_tree_id: 1,
            seg_live: BTreeMap::new(),
            frontier: 0,
            checkpoint_seq: 0,
            checkpoint_gsn: 0,
            manifest: Some(manifest),
            stamp: 0,
            flushed: None,
            releasing: Vec::new(),
            released: Vec::new(),
            reads: 0,
        };
        let recovered = match rec {
            None => None,
            Some(r) => {
                env.next_tree_id = r.next_tree_id;
                env.checkpoint_seq = r.seq;
                env.checkpoint_gsn = r.gsn;
                for (id, total, live) in &r.segments {
                    env.seg_live.insert(*id, (*total, *live));
                }
                for t in r.trees {
                    let (root, summary, min_seg) = match t.root {
                        None => (None, Summary::empty(&[]), u32::MAX),
                        Some((loc, s, m)) => (Some(NodePtr::Disk(loc)), s, m),
                    };
                    env.trees.insert(
                        t.id,
                        TreeState { root, summary, min_seg, summarizer: Arc::new(CountSummarizer), attached: false },
                    );
                }
                Some(Recovered { gsn: r.gsn, app: r.app })
            }
        };
        // remove segment files not referenced by the checkpoint (torn writes)
        for id in env.segs.list()? {
            if id != env.segs.active_id() && !env.seg_live.contains_key(&id) {
                env.segs.delete(id)?;
            }
        }
        env.seg_live.entry(env.segs.active_id()).or_insert((active_len, 0));
        Ok((env, recovered))
    }

    pub fn create_tree(&mut self, summarizer: Arc<dyn Summarizer>) -> TreeId {
        let id = self.next_tree_id;
        self.next_tree_id += 1;
        let summary = Summary::empty(summarizer.spec());
        self.trees.insert(id, TreeState { root: None, summary, min_seg: u32::MAX, summarizer, attached: true });
        id
    }

    /// Id the next `create_tree` will return (deterministic across replay).
    pub fn next_tree_id(&self) -> TreeId {
        self.next_tree_id
    }

    pub fn attach_tree(&mut self, id: TreeId, summarizer: Arc<dyn Summarizer>) -> Result<()> {
        let t = self.trees.get_mut(&id).ok_or_else(|| SqlError::corrupt(format!("tree {id} not found")))?;
        if t.root.is_none() {
            t.summary = Summary::empty(summarizer.spec());
        }
        t.summarizer = summarizer;
        t.attached = true;
        Ok(())
    }

    pub fn has_tree(&self, id: TreeId) -> bool {
        self.trees.contains_key(&id)
    }

    pub fn tree_ids(&self) -> Vec<TreeId> {
        let mut v: Vec<_> = self.trees.keys().copied().collect();
        v.sort();
        v
    }

    /// Trees recovered from the checkpoint that nobody attached.
    pub fn unattached_trees(&self) -> Vec<TreeId> {
        let mut v: Vec<_> = self.trees.iter().filter(|(_, t)| !t.attached).map(|(id, _)| *id).collect();
        v.sort();
        v
    }

    pub fn set_frontier(&mut self, f: Gsn) {
        if f > self.frontier {
            self.frontier = f;
        }
    }

    pub fn frontier(&self) -> Gsn {
        self.frontier
    }

    pub fn checkpoint_gsn(&self) -> Gsn {
        self.checkpoint_gsn
    }

    pub fn tree_summary(&self, tree: TreeId) -> Summary {
        self.trees.get(&tree).map(|t| t.summary.clone()).unwrap_or_else(|| Summary::empty(&[]))
    }

    pub fn tree_count(&self, tree: TreeId) -> i64 {
        self.trees.get(&tree).map(|t| t.summary.count).unwrap_or(0)
    }

    // ------------------------------------------------------------------
    // arena helpers

    fn slot(&self, id: u32) -> &Slot {
        self.slots[id as usize].as_ref().expect("dangling node slot")
    }

    fn slot_mut(&mut self, id: u32) -> &mut Slot {
        self.slots[id as usize].as_mut().expect("dangling node slot")
    }

    fn node(&self, id: u32) -> &Node {
        self.slot(id).node.as_ref().expect("node taken")
    }

    /// The node in slot `id` for modification in place: copied first if a
    /// fork or checkpoint job shares it.
    fn node_mut(&mut self, id: u32) -> &mut Node {
        Arc::make_mut(self.slot_mut(id).node.as_mut().expect("node taken"))
    }

    /// Take the node out of slot `id` (a copy if it is shared).
    fn take_node(&mut self, id: u32) -> Node {
        let n = self.slot_mut(id).node.take().expect("node taken");
        Arc::try_unwrap(n).unwrap_or_else(|n| (*n).clone())
    }

    fn next_stamp(&mut self) -> u64 {
        self.stamp += 1;
        self.stamp
    }

    fn alloc_slot(&mut self, node: Node, disk: Option<DiskLoc>, parent: u32, tree: TreeId) -> u32 {
        self.alloc_slot_arc(Arc::new(node), disk, parent, tree)
    }

    fn alloc_slot_arc(&mut self, node: Arc<Node>, disk: Option<DiskLoc>, parent: u32, tree: TreeId) -> u32 {
        let bytes = node.byte_size();
        self.mem_bytes += bytes;
        if disk.is_none() {
            self.dirty_bytes += bytes;
        }
        let version = self.next_stamp();
        let slot =
            Slot { node: Some(node), disk, dirty: disk.is_none(), version, edits: 0, parent, tree, loaded_children: 0, refbit: true, bytes };
        if let Some(id) = self.free_slots.pop() {
            self.slots[id as usize] = Some(slot);
            id
        } else {
            self.slots.push(Some(slot));
            (self.slots.len() - 1) as u32
        }
    }

    fn free_slot(&mut self, id: u32) {
        let slot = self.slots[id as usize].take().expect("double free");
        if let Some(loc) = slot.disk {
            self.dec_live(loc);
        }
        self.mem_bytes -= slot.bytes;
        if slot.dirty {
            self.dirty_bytes -= slot.bytes;
        }
        if slot.parent != NONE
            && let Some(p) = self.slots[slot.parent as usize].as_mut()
        {
            p.loaded_children = p.loaded_children.saturating_sub(1);
        }
        self.free_slots.push(id);
    }

    fn dec_live(&mut self, loc: DiskLoc) {
        if let Some(e) = self.seg_live.get_mut(&loc.seg) {
            e.1 = e.1.saturating_sub(loc.len as u64);
        }
    }

    fn make_dirty(&mut self, id: u32) {
        let version = self.next_stamp();
        let slot = self.slot_mut(id);
        // (a new stamp also for a dirty node: it is about to change)
        slot.version = version;
        if !slot.dirty {
            slot.dirty = true;
            let (bytes, disk) = (slot.bytes, slot.disk.take());
            self.dirty_bytes += bytes;
            if let Some(loc) = disk {
                self.dec_live(loc);
            }
        }
    }

    /// Mark a node and all its ancestors dirty (for relocation).
    fn dirty_path(&mut self, mut id: u32) {
        while id != NONE {
            self.make_dirty(id);
            id = self.slot(id).parent;
        }
    }

    fn put_node(&mut self, id: u32, node: Node) {
        let bytes = node.byte_size();
        self.put_node_sized(id, node, bytes);
    }

    /// `put_node` with the node's byte size already known.
    fn put_node_sized(&mut self, id: u32, node: Node, bytes: usize) {
        debug_assert_eq!(bytes, node.byte_size());
        let version = self.next_stamp();
        let slot = self.slot_mut(id);
        let old = slot.bytes;
        let dirty = slot.dirty;
        slot.bytes = bytes;
        slot.version = version;
        slot.node = Some(Arc::new(node));
        self.mem_bytes = self.mem_bytes + bytes - old;
        if dirty {
            self.dirty_bytes = self.dirty_bytes + bytes - old;
        }
    }

    /// Load `ptr` (child of `parent`), returning its slot.
    fn load(&mut self, ptr: NodePtr, parent: u32, tree: TreeId) -> Result<u32> {
        match ptr {
            NodePtr::Mem(id) => {
                self.slot_mut(id).refbit = true;
                Ok(id)
            }
            NodePtr::Disk(loc) => {
                self.reads += 1;
                let bytes = self.segs.read(loc)?;
                let node = Node::decode(&bytes)?;
                let id = self.alloc_slot(node, Some(loc), parent, tree);
                if parent != NONE {
                    self.slot_mut(parent).loaded_children += 1;
                }
                Ok(id)
            }
        }
    }

    /// Load child `idx` of internal node `parent` (which must be in its slot).
    fn load_child(&mut self, parent: u32, idx: usize) -> Result<u32> {
        let (ptr, tree) = match self.node(parent) {
            Node::Internal(ch) => (ch[idx].ptr, self.slot(parent).tree),
            Node::Leaf(_) => return Err(SqlError::internal("load_child on leaf")),
        };
        let id = self.load(ptr, parent, tree)?;
        if let NodePtr::Disk(_) = ptr
            && let Node::Internal(ch) = self.node_mut(parent)
        {
            ch[idx].ptr = NodePtr::Mem(id);
        }
        Ok(id)
    }

    fn root_slot(&mut self, tree: TreeId) -> Result<Option<u32>> {
        let t = self.trees.get(&tree).ok_or_else(|| SqlError::internal(format!("no tree {tree}")))?;
        match t.root {
            None => Ok(None),
            Some(ptr) => {
                let id = self.load(ptr, NONE, tree)?;
                self.trees.get_mut(&tree).unwrap().root = Some(NodePtr::Mem(id));
                Ok(Some(id))
            }
        }
    }

    /// Summary and min_seg of a node computed from its contents.
    fn compute_summary(&self, id: u32, summ: &Arc<dyn Summarizer>) -> Result<(Summary, u32)> {
        let slot = self.slot(id);
        let own = slot.disk.map(|l| l.seg).unwrap_or(u32::MAX);
        match slot.node.as_deref().unwrap() {
            Node::Leaf(entries) => Ok((leaf_summary(entries, summ)?, own)),
            Node::Internal(children) => {
                Ok((children_summary(children, summ), children.iter().map(|c| c.min_seg).fold(own, u32::min)))
            }
        }
    }

    /// min_seg of a node from its own location and its children's.
    fn node_min_seg(&self, id: u32) -> u32 {
        let slot = self.slot(id);
        let own = slot.disk.map(|l| l.seg).unwrap_or(u32::MAX);
        match slot.node.as_deref().unwrap() {
            Node::Leaf(_) => own,
            Node::Internal(children) => children.iter().map(|c| c.min_seg).fold(own, u32::min),
        }
    }

    // ------------------------------------------------------------------
    // writes

    /// Apply weighted updates at time `gsn`. Updates need not be sorted; equal
    /// keys are consolidated.
    pub fn apply(&mut self, tree: TreeId, updates: &mut Vec<(Vec<u8>, i64)>, gsn: Gsn) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }
        updates.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        // consolidate equal keys in place
        updates.dedup_by(|cur, kept| {
            if cur.0 == kept.0 {
                kept.1 += cur.1;
                true
            } else {
                false
            }
        });
        updates.retain(|(_, w)| *w != 0);
        if updates.is_empty() {
            return Ok(());
        }
        let summ = self.trees.get(&tree).ok_or_else(|| SqlError::internal(format!("no tree {tree}")))?.summarizer.clone();
        let root = match self.root_slot(tree)? {
            Some(id) => id,
            None => self.alloc_slot(Node::Leaf(Vec::new()), None, NONE, tree),
        };
        // the tree's summary is replaced below
        let old = std::mem::take(&mut self.trees.get_mut(&tree).unwrap().summary);
        let parts = self.apply_node(root, updates, gsn, &summ, Some(&old));
        updates.clear();
        let mut parts = match parts {
            Ok(p) => p,
            Err(e) => {
                self.trees.get_mut(&tree).unwrap().summary = old;
                return Err(e);
            }
        };
        let top = match parts.len() {
            0 => None,
            1 => parts.pop(),
            _ => Some(self.build_root(parts, tree, &summ)?),
        };
        let (new_root, summary, min_seg) = match top {
            None => (None, Summary::empty(summ.spec()), u32::MAX),
            Some(p) => {
                // a single-child root covers the same entries as its child
                let r = self.collapse_root(p.id)?;
                let min_seg = if r == p.id { p.min_seg } else { self.node_min_seg(r) };
                (Some(r), p.summary, min_seg)
            }
        };
        let t = self.trees.get_mut(&tree).unwrap();
        t.root = new_root.map(NodePtr::Mem);
        t.summary = summary;
        t.min_seg = min_seg;
        self.maybe_evict();
        Ok(())
    }

    /// Replace a single-child internal root by its child.
    fn collapse_root(&mut self, mut root: u32) -> Result<u32> {
        loop {
            let single = matches!(self.node(root), Node::Internal(ch) if ch.len() == 1);
            if !single {
                return Ok(root);
            }
            let child = self.load_child(root, 0)?;
            self.make_dirty(root);
            self.slot_mut(child).parent = NONE;
            // detach so free_slot does not decrement anything for the child
            self.slot_mut(root).loaded_children = 0;
            self.free_slot(root);
            root = child;
        }
    }

    fn build_root(&mut self, mut parts: Parts, tree: TreeId, summ: &Arc<dyn Summarizer>) -> Result<Part> {
        loop {
            let root = self.alloc_slot(Node::Internal(Vec::new()), None, NONE, tree);
            for p in &parts {
                self.slot_mut(p.id).parent = root;
            }
            self.slot_mut(root).loaded_children = parts.len() as u32;
            let children = parts.into_iter().map(Part::into_child).collect();
            let mut new_parts = self.finish_internal(root, children, None, summ)?;
            if new_parts.len() == 1 {
                return Ok(new_parts.pop().unwrap());
            }
            for p in &new_parts {
                self.slot_mut(p.id).parent = NONE;
            }
            parts = new_parts;
        }
    }

    /// Apply sorted, consolidated updates below node `id`, whose summary is
    /// `old` when known. Returns the nodes now covering its key range.
    fn apply_node(
        &mut self,
        id: u32,
        ups: &[(Vec<u8>, i64)],
        gsn: Gsn,
        summ: &Arc<dyn Summarizer>,
        old: Option<&Summary>,
    ) -> Result<Parts> {
        let was_dirty = self.slot(id).dirty;
        let edits = self.slot(id).edits;
        let old_bytes = self.slot(id).bytes;
        self.make_dirty(id);
        let node = self.take_node(id);
        match node {
            Node::Leaf(entries) => {
                let frontier = self.frontier;
                // weight changes as (index into ups, total weight before),
                // folded into the leaf's old summary afterwards if the leaf
                // does not split and that is cheaper than recomputing it
                let mut changes: smallvec::SmallVec<[(usize, i64); 4]> = smallvec::SmallVec::with_capacity(ups.len());
                // Few updates: edit in place. Untouched entries are compacted
                // (dropping versions retracted before the frontier) on the
                // first edit after a checkpoint and then every few edits.
                let in_place = was_dirty && edits < 16 && ups.len() * 8 <= entries.len();
                self.slot_mut(id).edits = if in_place { edits + 1 } else { 0 };
                // the leaf's byte size, kept up to date by in-place edits
                let mut size = old_bytes;
                let out = if in_place {
                    let mut entries = entries;
                    let mut pos = 0usize;
                    for (ui, (k, w)) in ups.iter().enumerate() {
                        pos += entries[pos..].partition_point(|e| *e.key < **k);
                        let before = if pos < entries.len() && *entries[pos].key == **k {
                            let e = &mut entries[pos];
                            let before = e.hist.total();
                            size -= entry_size(e);
                            e.hist.add(gsn, *w, frontier);
                            if e.hist.is_empty() {
                                entries.remove(pos);
                            } else {
                                size += entry_size(&entries[pos]);
                                pos += 1;
                            }
                            before
                        } else {
                            let mut h = History::default();
                            h.add(gsn, *w, frontier);
                            if !h.is_empty() {
                                let e = LeafEntry { key: k.as_slice().into(), hist: h };
                                size += entry_size(&e);
                                entries.insert(pos, e);
                                pos += 1;
                            }
                            0
                        };
                        changes.push((ui, before));
                    }
                    entries
                } else {
                    let mut out: Vec<LeafEntry> = Vec::with_capacity(entries.len() + ups.len());
                    let mut it = entries.into_iter().peekable();
                    for (ui, (k, w)) in ups.iter().enumerate() {
                        while let Some(e) = it.peek() {
                            if *e.key < **k {
                                let mut e = it.next().unwrap();
                                e.hist.compact(frontier);
                                if !e.hist.is_empty() {
                                    out.push(e);
                                }
                            } else {
                                break;
                            }
                        }
                        let before = match it.peek() {
                            Some(e) if *e.key == **k => {
                                let mut e = it.next().unwrap();
                                let before = e.hist.total();
                                e.hist.add(gsn, *w, frontier);
                                if !e.hist.is_empty() {
                                    out.push(e);
                                }
                                before
                            }
                            _ => {
                                let mut h = History::default();
                                h.add(gsn, *w, frontier);
                                if !h.is_empty() {
                                    out.push(LeafEntry { key: k.as_slice().into(), hist: h });
                                }
                                0
                            }
                        };
                        changes.push((ui, before));
                    }
                    for mut e in it {
                        e.hist.compact(frontier);
                        if !e.hist.is_empty() {
                            out.push(e);
                        }
                    }
                    out
                };
                if out.is_empty() {
                    self.put_node(id, Node::Leaf(Vec::new()));
                    self.free_slot(id);
                    return Ok(Parts::new());
                }
                if in_place && size <= self.opts.leaf_bytes {
                    // common case: no split, size already known
                    let summary = summary_after(old, &out, ups, &changes, summ)?;
                    self.put_node_sized(id, Node::Leaf(out), size);
                    // the first part keeps the node's place (and separator) in the parent
                    return Ok(smallvec::smallvec![Part { sep: Box::default(), id, summary, min_seg: u32::MAX }]);
                }
                let chunks = split_by_bytes(out, self.opts.leaf_bytes, entry_size, 1);
                let single = chunks.len() == 1;
                let parent = self.slot(id).parent;
                let tree = self.slot(id).tree;
                let mut parts = Parts::with_capacity(chunks.len());
                // suffix truncation: the shortest separators between chunks
                // (the first part keeps the node's separator in the parent)
                let mut seps: Vec<Box<[u8]>> = vec![Box::default()];
                seps.extend(chunks.windows(2).map(|w| shortest_separator(&w[0].last().unwrap().key, &w[1][0].key)));
                for (i, (chunk, first)) in chunks.into_iter().zip(seps).enumerate() {
                    let summary = if single { summary_after(old, &chunk, ups, &changes, summ)? } else { leaf_summary(&chunk, summ)? };
                    let pid = if i == 0 {
                        self.put_node(id, Node::Leaf(chunk));
                        id
                    } else {
                        let nid = self.alloc_slot(Node::Leaf(chunk), None, parent, tree);
                        if parent != NONE {
                            // new siblings are loaded children of the same parent
                            self.slot_mut(parent).loaded_children += 1;
                        }
                        nid
                    };
                    // dirty leaves have no location of their own yet
                    parts.push(Part { sep: first, id: pid, summary, min_seg: u32::MAX });
                }
                Ok(parts)
            }
            Node::Internal(mut children) => {
                // Put an empty placeholder so loads of children can bump
                // loaded_children on this slot.
                self.put_node(id, Node::Internal(Vec::new()));
                let mut size = old_bytes;
                let mut incr = old.cloned();
                let n = children.len();
                // (child, update range) for each child receiving updates
                let mut runs: smallvec::SmallVec<[(usize, usize, usize); 4]> = smallvec::SmallVec::new();
                let (mut pos, mut ci) = (0usize, 0usize);
                while pos < ups.len() {
                    // the last child whose separator is <= the key (the first
                    // child also takes smaller keys)
                    ci += children[ci + 1..].partition_point(|c| *c.sep <= *ups[pos].0.as_slice());
                    let end = if ci + 1 < n {
                        let next = &*children[ci + 1].sep;
                        pos + ups[pos..].partition_point(|(k, _)| k.as_slice() < next)
                    } else {
                        ups.len()
                    };
                    runs.push((ci, pos, end));
                    pos = end;
                    ci += 1;
                }
                let tree = self.slot(id).tree;
                // right to left, so splicing keeps the remaining indexes valid
                for &(ci, start, end) in runs.iter().rev() {
                    let cid = self.load(children[ci].ptr, id, tree)?;
                    let parts = self.apply_node(cid, &ups[start..end], gsn, summ, Some(&children[ci].summary))?;
                    if let Some(s) = incr.as_mut() {
                        let news: smallvec::SmallVec<[&Summary; 2]> = parts.iter().map(|p| &p.summary).collect();
                        if !s.replace_child(&children[ci].summary, &news) {
                            incr = None;
                        }
                    }
                    // the first part keeps the child's separator
                    size -= node::child_size(&children[ci]);
                    let nparts = parts.len();
                    let mut sep = Some(std::mem::take(&mut children[ci].sep));
                    children.splice(
                        ci..ci + 1,
                        parts.into_iter().map(|mut p| {
                            if let Some(s) = sep.take() {
                                p.sep = s;
                            }
                            p.into_child()
                        }),
                    );
                    size += children[ci..ci + nparts].iter().map(node::child_size).sum::<usize>();
                }
                if children.is_empty() {
                    self.slot_mut(id).loaded_children = 0;
                    self.free_slot(id);
                    return Ok(Parts::new());
                }
                self.finish_internal_sized(id, children, incr, summ, size)
            }
        }
    }

    /// `finish_internal` for a node whose byte size is known: when it does
    /// not need to split, its children's parent links and the loaded count
    /// are already right (maintained by load, allocation and free).
    fn finish_internal_sized(
        &mut self,
        id: u32,
        children: Vec<Child>,
        summary: Option<Summary>,
        summ: &Arc<dyn Summarizer>,
        size: usize,
    ) -> Result<Parts> {
        if size > self.opts.internal_bytes {
            return self.finish_internal(id, children, summary, summ);
        }
        #[cfg(debug_assertions)]
        {
            let loaded = children.iter().filter(|c| matches!(c.ptr, NodePtr::Mem(_))).count() as u32;
            debug_assert_eq!(loaded, self.slot(id).loaded_children, "loaded children");
            for c in &children {
                if let NodePtr::Mem(cid) = c.ptr {
                    debug_assert_eq!(self.slot(cid).parent, id, "parent link");
                }
            }
        }
        let s = match summary {
            Some(s) => s,
            None => children_summary(&children, summ),
        };
        let min_seg = children.iter().map(|c| c.min_seg).fold(u32::MAX, u32::min);
        self.put_node_sized(id, Node::Internal(children), size);
        // the first part keeps the node's place (and separator) in the parent
        Ok(smallvec::smallvec![Part { sep: Box::default(), id, summary: s, min_seg }])
    }

    /// Store `children` in internal node `id`, splitting by size. `summary`
    /// is the node's summary if known (used when it does not split).
    fn finish_internal(
        &mut self,
        id: u32,
        children: Vec<Child>,
        summary: Option<Summary>,
        summ: &Arc<dyn Summarizer>,
    ) -> Result<Parts> {
        let parent = self.slot(id).parent;
        let tree = self.slot(id).tree;
        // internal nodes keep at least two children per chunk so that a tree
        // with oversized separators still converges to a single root
        let chunks = split_by_bytes(children, self.opts.internal_bytes, node::child_size, 2);
        let single = chunks.len() == 1;
        let mut summary = summary;
        let mut parts = Parts::with_capacity(chunks.len());
        for (i, mut chunk) in chunks.into_iter().enumerate() {
            // a new node's separator is its first child's, which the node
            // itself ignores (the first part keeps the node's own separator)
            let first = if i == 0 { Box::default() } else { std::mem::take(&mut chunk[0].sep) };
            let loaded = chunk.iter().filter(|c| matches!(c.ptr, NodePtr::Mem(_))).count() as u32;
            let target = if i == 0 {
                id
            } else {
                let nid = self.alloc_slot(Node::Internal(Vec::new()), None, parent, tree);
                if parent != NONE {
                    self.slot_mut(parent).loaded_children += 1;
                }
                nid
            };
            for c in &chunk {
                if let NodePtr::Mem(cid) = c.ptr {
                    self.slot_mut(cid).parent = target;
                }
            }
            let s = match summary.take() {
                Some(s) if single => s,
                _ => children_summary(&chunk, summ),
            };
            let min_seg = chunk.iter().map(|c| c.min_seg).fold(u32::MAX, u32::min);
            self.slot_mut(target).loaded_children = loaded;
            self.put_node(target, Node::Internal(chunk));
            parts.push(Part { sep: first, id: target, summary: s, min_seg });
        }
        Ok(parts)
    }

    // ------------------------------------------------------------------
    // reads

    /// Exact key lookup.
    pub fn get(&mut self, tree: TreeId, key: &[u8]) -> Result<Option<History>> {
        let Some(mut id) = self.root_slot(tree)? else { return Ok(None) };
        loop {
            match self.node(id) {
                Node::Leaf(entries) => {
                    let r = entries.binary_search_by(|e| (*e.key).cmp(key)).ok().map(|i| entries[i].hist.clone());
                    self.maybe_evict();
                    return Ok(r);
                }
                Node::Internal(children) => {
                    let idx = route(children, key);
                    id = self.load_child(id, idx)?;
                }
            }
        }
    }

    pub fn cursor(&self, tree: TreeId, lo: Bound<&[u8]>, hi: Bound<&[u8]>) -> Cursor {
        Cursor { tree, lo: own_bound(lo), hi: own_bound(hi), buf: VecDeque::new(), done: false }
    }

    pub fn cursor_next(&mut self, c: &mut Cursor) -> Result<Option<(Box<[u8]>, History)>> {
        loop {
            if let Some(x) = c.buf.pop_front() {
                return Ok(Some(x));
            }
            if c.done {
                return Ok(None);
            }
            self.refill(c)?;
        }
    }

    fn refill(&mut self, c: &mut Cursor) -> Result<()> {
        loop {
            let Some(mut id) = self.root_slot(c.tree)? else {
                c.done = true;
                return Ok(());
            };
            let mut next_sep: Option<Box<[u8]>> = None;
            // descend
            loop {
                match self.node(id) {
                    Node::Leaf(_) => break,
                    Node::Internal(children) => {
                        let idx = match borrow_bound(&c.lo) {
                            Bound::Unbounded => 0,
                            Bound::Included(k) | Bound::Excluded(k) => route(children, k),
                        };
                        if idx + 1 < children.len() {
                            next_sep = Some(children[idx + 1].sep.clone());
                        }
                        id = self.load_child(id, idx)?;
                    }
                }
            }
            let Node::Leaf(entries) = self.node(id) else { unreachable!() };
            let start = match borrow_bound(&c.lo) {
                Bound::Unbounded => 0,
                Bound::Included(k) => entries.partition_point(|e| &*e.key < k),
                Bound::Excluded(k) => entries.partition_point(|e| &*e.key <= k),
            };
            let hi = borrow_bound(&c.hi);
            let mut hit_hi = false;
            for e in &entries[start..] {
                if !below_hi(&e.key, hi) {
                    hit_hi = true;
                    break;
                }
                c.buf.push_back((e.key.clone(), e.hist.clone()));
            }
            if hit_hi {
                c.done = true;
            } else {
                match next_sep {
                    None => c.done = true,
                    Some(s) => {
                        if !below_hi(&s, hi) {
                            c.done = true;
                        }
                        c.lo = Bound::Included(s.into_vec());
                    }
                }
            }
            self.maybe_evict();
            if !c.buf.is_empty() || c.done {
                return Ok(());
            }
        }
    }

    /// Collect all entries in a range (convenience for small ranges).
    pub fn scan(&mut self, tree: TreeId, lo: Bound<&[u8]>, hi: Bound<&[u8]>) -> Result<Vec<(Box<[u8]>, History)>> {
        let mut out = Vec::new();
        self.for_each(tree, lo, hi, &mut |k, h| {
            out.push((k.into(), h.clone()));
            Ok(true)
        })?;
        Ok(out)
    }

    /// Entries whose key starts with `prefix`.
    pub fn scan_prefix(&mut self, tree: TreeId, prefix: &[u8]) -> Result<Vec<(Box<[u8]>, History)>> {
        let mut out = Vec::new();
        self.for_each_prefix(tree, prefix, &mut |k, h| {
            out.push((k.into(), h.clone()));
            Ok(true)
        })?;
        Ok(out)
    }

    /// Call `f` with the entries in [lo, hi) in key order until it returns
    /// false. Entries are visited where they are, without copying them.
    pub fn for_each(
        &mut self,
        tree: TreeId,
        lo: Bound<&[u8]>,
        hi: Bound<&[u8]>,
        f: &mut dyn FnMut(&[u8], &History) -> Result<bool>,
    ) -> Result<()> {
        self.walk(tree, lo, &|k| below_hi(k, hi), f)
    }

    /// `for_each` over the entries whose key starts with `prefix`.
    pub fn for_each_prefix(&mut self, tree: TreeId, prefix: &[u8], f: &mut dyn FnMut(&[u8], &History) -> Result<bool>) -> Result<()> {
        self.walk(tree, Bound::Included(prefix), &|k| k.starts_with(prefix), f)
    }

    /// Visit entries from `lo` while `in_range` (true up to some key, false
    /// after it) holds, one leaf at a time: each leaf is found from the root,
    /// so nothing is pinned between leaves.
    fn walk(
        &mut self,
        tree: TreeId,
        lo: Bound<&[u8]>,
        in_range: &dyn Fn(&[u8]) -> bool,
        f: &mut dyn FnMut(&[u8], &History) -> Result<bool>,
    ) -> Result<()> {
        let mut resume: Option<Vec<u8>> = None;
        loop {
            let Some(mut id) = self.root_slot(tree)? else { return Ok(()) };
            let lo = match &resume {
                Some(v) => Bound::Included(v.as_slice()),
                None => lo,
            };
            // the deepest internal node with a child after the path (slots
            // stay put until maybe_evict runs at the end)
            let mut next: Option<(u32, usize)> = None;
            loop {
                match self.node(id) {
                    Node::Leaf(_) => break,
                    Node::Internal(children) => {
                        let idx = match lo {
                            Bound::Unbounded => 0,
                            Bound::Included(k) | Bound::Excluded(k) => route(children, k),
                        };
                        if idx + 1 < children.len() {
                            next = Some((id, idx + 1));
                        }
                        id = self.load_child(id, idx)?;
                    }
                }
            }
            let Node::Leaf(entries) = self.node(id) else { unreachable!() };
            let start = match lo {
                Bound::Unbounded => 0,
                Bound::Included(k) => entries.partition_point(|e| &*e.key < k),
                Bound::Excluded(k) => entries.partition_point(|e| &*e.key <= k),
            };
            for e in &entries[start..] {
                if !in_range(&e.key) || !f(&e.key, &e.hist)? {
                    self.maybe_evict();
                    return Ok(());
                }
            }
            let sep = match next {
                Some((pid, ci)) => match self.node(pid) {
                    Node::Internal(ch) if in_range(&ch[ci].sep) => Some(ch[ci].sep.to_vec()),
                    _ => None,
                },
                None => None,
            };
            match sep {
                Some(s) => resume = Some(s),
                None => {
                    self.maybe_evict();
                    return Ok(());
                }
            }
        }
    }

    /// Summary over the entries in [lo, hi) (current weights).
    pub fn range_summary(&mut self, tree: TreeId, lo: Bound<&[u8]>, hi: Bound<&[u8]>) -> Result<Summary> {
        let summ = self.trees.get(&tree).ok_or_else(|| SqlError::internal(format!("no tree {tree}")))?.summarizer.clone();
        let mut acc = Summary::empty(summ.spec());
        if matches!((lo, hi), (Bound::Unbounded, Bound::Unbounded)) {
            return Ok(self.tree_summary(tree));
        }
        if let Some(root) = self.root_slot(tree)? {
            self.summary_rec(root, lo, hi, &summ, &mut acc)?;
        }
        self.maybe_evict();
        Ok(acc)
    }

    /// Verify that every stored summary equals one recomputed from the
    /// node's contents (tests and debugging).
    pub fn check_summaries(&mut self, tree: TreeId) -> Result<()> {
        let summ = self.trees[&tree].summarizer.clone();
        let stored = self.trees[&tree].summary.clone();
        let Some(root) = self.root_slot(tree)? else {
            return if stored.count == 0 { Ok(()) } else { Err(SqlError::internal("summary of an empty tree")) };
        };
        self.check_node(root, &stored, &summ)
    }

    fn check_node(&mut self, id: u32, stored: &Summary, summ: &Arc<dyn Summarizer>) -> Result<()> {
        let (actual, _) = self.compute_summary(id, summ)?;
        if !actual.exact_eq(stored) {
            return Err(SqlError::internal(format!("stale summary: stored {stored:?}, actual {actual:?}")));
        }
        if let Node::Internal(children) = self.node(id) {
            let stored: Vec<Summary> = children.iter().map(|c| c.summary.clone()).collect();
            for (i, s) in stored.iter().enumerate() {
                let cid = self.load_child(id, i)?;
                self.check_node(cid, s, summ)?;
            }
        }
        Ok(())
    }

    fn summary_rec(
        &mut self,
        id: u32,
        lo: Bound<&[u8]>,
        hi: Bound<&[u8]>,
        summ: &Arc<dyn Summarizer>,
        acc: &mut Summary,
    ) -> Result<()> {
        match self.node(id) {
            Node::Leaf(entries) => {
                let start = match lo {
                    Bound::Unbounded => 0,
                    Bound::Included(k) => entries.partition_point(|e| &*e.key < k),
                    Bound::Excluded(k) => entries.partition_point(|e| &*e.key <= k),
                };
                let end = start + entries[start..].partition_point(|e| below_hi(&e.key, hi));
                let entries = &entries[start..end];
                if summ.spec().is_empty() {
                    acc.count += entries.iter().map(|e| e.hist.total()).sum::<i64>();
                } else {
                    summ.add_entries(acc, &mut entries.iter().map(|e| (&*e.key, e.hist.total())))?;
                }
                Ok(())
            }
            Node::Internal(children) => {
                // (child, fully contained); visited in key order so that ties
                // between MIN/MAX candidates resolve to the leftmost
                let items = overlapping(children, lo, hi);
                for (i, full) in items {
                    if full {
                        let Node::Internal(children) = self.node(id) else { unreachable!() };
                        acc.combine(&children[i].summary);
                    } else {
                        let cid = self.load_child(id, i)?;
                        self.summary_rec(cid, lo, hi, summ, acc)?;
                    }
                }
                Ok(())
            }
        }
    }

    /// The n-th unit of weight (0-based) in [lo, hi): returns the entry and
    /// the offset within that entry's multiplicity.
    pub fn nth(
        &mut self,
        tree: TreeId,
        lo: Bound<&[u8]>,
        hi: Bound<&[u8]>,
        n: i64,
    ) -> Result<Option<(Box<[u8]>, History, i64)>> {
        let Some(root) = self.root_slot(tree)? else { return Ok(None) };
        let mut rem = n;
        let r = self.nth_rec(root, lo, hi, &mut rem)?;
        self.maybe_evict();
        Ok(r)
    }

    fn nth_rec(
        &mut self,
        id: u32,
        lo: Bound<&[u8]>,
        hi: Bound<&[u8]>,
        rem: &mut i64,
    ) -> Result<Option<(Box<[u8]>, History, i64)>> {
        match self.node(id) {
            Node::Leaf(entries) => {
                let start = match lo {
                    Bound::Unbounded => 0,
                    Bound::Included(k) => entries.partition_point(|e| &*e.key < k),
                    Bound::Excluded(k) => entries.partition_point(|e| &*e.key <= k),
                };
                for e in &entries[start..] {
                    if !below_hi(&e.key, hi) {
                        break;
                    }
                    let w = e.hist.total();
                    if w > *rem {
                        return Ok(Some((e.key.clone(), e.hist.clone(), *rem)));
                    }
                    *rem -= w.max(0);
                }
                Ok(None)
            }
            Node::Internal(children) => {
                let plan: Vec<(usize, bool, i64)> =
                    overlapping(children, lo, hi).into_iter().map(|(i, full)| (i, full, children[i].summary.count)).collect();
                for (i, full, count) in plan {
                    if full && count <= *rem {
                        *rem -= count.max(0);
                        continue;
                    }
                    let cid = self.load_child(id, i)?;
                    if let Some(r) = self.nth_rec(cid, lo, hi, rem)? {
                        return Ok(Some(r));
                    }
                }
                Ok(None)
            }
        }
    }

    // ------------------------------------------------------------------
    // eviction

    fn maybe_evict(&mut self) {
        if self.mem_bytes <= self.opts.cache_bytes || self.slots.is_empty() {
            return;
        }
        let n = self.slots.len();
        let mut scanned = 0;
        while self.mem_bytes > self.opts.cache_bytes && scanned < 2 * n {
            let i = self.clock_hand % n;
            self.clock_hand = (i + 1) % n;
            scanned += 1;
            let Some(slot) = self.slots[i].as_mut() else { continue };
            if slot.dirty || slot.loaded_children > 0 || slot.node.is_none() {
                continue;
            }
            if slot.refbit {
                slot.refbit = false;
                continue;
            }
            self.evict(i as u32);
        }
    }

    fn evict(&mut self, id: u32) {
        let slot = self.slots[id as usize].take().unwrap();
        let loc = slot.disk.expect("evicting node without disk location");
        if slot.parent == NONE {
            if let Some(t) = self.trees.get_mut(&slot.tree) {
                t.root = Some(NodePtr::Disk(loc));
            }
        } else if let Some(p) = self.slots[slot.parent as usize].as_mut() {
            p.loaded_children = p.loaded_children.saturating_sub(1);
            if let Some(Node::Internal(ch)) = p.node.as_mut().map(Arc::make_mut) {
                for c in ch.iter_mut() {
                    if c.ptr == NodePtr::Mem(id) {
                        c.ptr = NodePtr::Disk(loc);
                        break;
                    }
                }
            }
        }
        self.mem_bytes -= slot.bytes;
        self.free_slots.push(id);
    }

    // ------------------------------------------------------------------
    // checkpoint, drop, gc

    /// Make all trees durable as of `gsn` together with the application state
    /// (`begin_checkpoint`, `CheckpointJob::run` and `finish_checkpoint` in one).
    pub fn checkpoint(&mut self, gsn: Gsn, app: &[u8]) -> Result<()> {
        let job = self.begin_checkpoint(gsn)?;
        let done = job.run(app)?;
        self.finish_checkpoint(done)
    }

    /// Start a checkpoint of all trees as they are now (at `gsn`): the job
    /// holds a version of the dirty nodes (shared, copied when this
    /// environment modifies them later) and the append side of the store,
    /// and writes them on any thread (`CheckpointJob::run`) while this
    /// environment goes on; `finish_checkpoint` then marks the nodes it wrote
    /// clean where they have not changed since. Nothing else may write
    /// segments meanwhile: a second checkpoint waits for the first.
    pub fn begin_checkpoint(&mut self, gsn: Gsn) -> Result<CheckpointJob> {
        if self.manifest.is_none() || !self.segs.writable() {
            return Err(SqlError::internal("a checkpoint is already in progress"));
        }
        // the dirty nodes, and the clean nodes whose locations their images
        // (and the tree records) need
        let mut slots: Vec<Option<Slot>> = Vec::with_capacity(self.slots.len());
        slots.resize_with(self.slots.len(), || None);
        let (mut mem_bytes, mut dirty_bytes) = (0, 0);
        let mut copy = |slots: &mut Vec<Option<Slot>>, s: &Slot, i: usize| {
            if slots[i].is_none() {
                mem_bytes += s.bytes;
                if s.dirty {
                    dirty_bytes += s.bytes;
                }
                slots[i] = Some(s.clone());
            }
        };
        for (i, s) in self.slots.iter().enumerate() {
            let Some(s) = s.as_ref().filter(|s| s.dirty) else { continue };
            copy(&mut slots, s, i);
            if let Some(Node::Internal(ch)) = s.node.as_deref() {
                for c in ch {
                    if let NodePtr::Mem(cid) = c.ptr {
                        copy(&mut slots, self.slot(cid), cid as usize);
                    }
                }
            }
        }
        for t in self.trees.values() {
            if let Some(NodePtr::Mem(r)) = t.root {
                copy(&mut slots, self.slot(r), r as usize);
            }
        }
        let segs = self.segs.lend().expect("checked above");
        let env = Env {
            dir: self.dir.clone(),
            opts: self.opts.clone(),
            segs,
            slots,
            free_slots: Vec::new(),
            mem_bytes,
            dirty_bytes,
            clock_hand: 0,
            trees: self.trees.clone(),
            next_tree_id: self.next_tree_id,
            seg_live: self.seg_live.clone(),
            frontier: self.frontier,
            checkpoint_seq: self.checkpoint_seq,
            checkpoint_gsn: self.checkpoint_gsn,
            manifest: self.manifest.take(),
            stamp: self.stamp,
            flushed: Some(Vec::new()),
            releasing: std::mem::take(&mut self.releasing),
            released: Vec::new(),
            reads: 0,
        };
        Ok(CheckpointJob { env, gsn })
    }

    /// Take back what a checkpoint job lent and mark the nodes it wrote
    /// clean, except those modified (or freed) since it began: their images
    /// are garbage from the start.
    pub fn finish_checkpoint(&mut self, done: CheckpointDone) -> Result<()> {
        self.segs.restore(done.segs);
        self.manifest = Some(done.manifest);
        // (children before parents)
        for f in done.flushed {
            let len = f.loc.len as u64;
            let e = self.seg_live.entry(f.loc.seg).or_insert((0, 0));
            e.0 += len;
            let current = matches!(self.slots.get(f.slot as usize), Some(Some(s)) if s.dirty && s.version == f.version);
            if !current {
                continue;
            }
            e.1 += len;
            let slot = self.slot_mut(f.slot);
            slot.dirty = false;
            slot.disk = Some(f.loc);
            let bytes = slot.bytes;
            self.dirty_bytes -= bytes;
            self.lower_min_seg(f.slot, f.min_seg);
        }
        for loc in done.released {
            self.dec_live(loc);
        }
        // dead in the committed checkpoint, hence here too: nothing this
        // environment references on disk is outside it
        for id in done.deleted {
            self.seg_live.remove(&id);
            self.segs.forget(id);
        }
        self.checkpoint_seq = done.seq;
        self.checkpoint_gsn = done.gsn;
        let active = self.segs.active_id();
        self.seg_live.entry(active).or_insert((0, 0));
        Ok(())
    }

    /// Lower the `min_seg` recorded for node `id`'s subtree to `ms` (it was
    /// written there), in its parent's child entry and up the ancestors.
    fn lower_min_seg(&mut self, mut id: u32, ms: u32) {
        loop {
            let (parent, tree) = (self.slot(id).parent, self.slot(id).tree);
            if parent == NONE {
                if let Some(t) = self.trees.get_mut(&tree)
                    && t.root == Some(NodePtr::Mem(id))
                {
                    t.min_seg = t.min_seg.min(ms);
                }
                return;
            }
            let Node::Internal(ch) = self.node(parent) else { return };
            let Some(i) = ch.iter().position(|c| c.ptr == NodePtr::Mem(id)) else { return };
            if ch[i].min_seg <= ms {
                return;
            }
            if let Node::Internal(ch) = self.node_mut(parent) {
                ch[i].min_seg = ms;
            }
            id = parent;
        }
    }

    /// Write every dirty node, make the segments durable and append the
    /// checkpoint record (the commit point); then delete sealed segments
    /// without live nodes. Returns those segments.
    fn write_checkpoint(&mut self, gsn: Gsn, app: &[u8]) -> Result<Vec<u32>> {
        // dropped trees' nodes on disk are dead in this checkpoint
        for loc in std::mem::take(&mut self.releasing) {
            self.release(NodePtr::Disk(loc))?;
        }
        let tree_ids = self.tree_ids();
        let mut records = Vec::with_capacity(tree_ids.len());
        for tid in tree_ids {
            let t = &self.trees[&tid];
            let rec = match t.root {
                None => TreeRecord { id: tid, root: None },
                Some(NodePtr::Disk(loc)) => TreeRecord { id: tid, root: Some((loc, t.summary.clone(), t.min_seg)) },
                Some(NodePtr::Mem(id)) => {
                    let was_dirty = self.slot(id).dirty;
                    let (loc, min_seg) = self.flush_node(id)?;
                    let t = self.trees.get_mut(&tid).unwrap();
                    if was_dirty {
                        t.min_seg = min_seg;
                    }
                    TreeRecord { id: tid, root: Some((loc, t.summary.clone(), t.min_seg)) }
                }
            };
            records.push(rec);
        }
        self.segs.sync()?;
        let active = self.segs.active_id();
        let active_len = self.segs.active_len();
        self.seg_live.entry(active).or_insert((0, 0));
        self.checkpoint_seq += 1;
        let rec = CheckpointRecord {
            seq: self.checkpoint_seq,
            gsn,
            trees: records,
            segments: self.seg_live.iter().map(|(id, (t, l))| (*id, *t, *l)).collect(),
            active_seg: active,
            active_len,
            next_tree_id: self.next_tree_id,
            app: app.to_vec(),
        };
        self.manifest.as_mut().ok_or_else(|| SqlError::internal("no manifest to append to"))?.append(&rec)?;
        self.checkpoint_gsn = gsn;
        // reclaim fully dead sealed segments
        let dead: Vec<u32> = self.seg_live.iter().filter(|(id, (_, l))| **id != active && *l == 0).map(|(id, _)| *id).collect();
        for &id in &dead {
            self.seg_live.remove(&id);
            self.segs.delete(id)?;
        }
        Ok(dead)
    }

    fn flush_node(&mut self, id: u32) -> Result<(DiskLoc, u32)> {
        if !self.slot(id).dirty {
            let loc = self.slot(id).disk.unwrap();
            return Ok((loc, loc.seg));
        }
        // children first: their locations, and the min_seg of the dirty
        // ones, are part of this node's image
        let children: Vec<NodePtr> = match self.node(id) {
            Node::Internal(ch) => ch.iter().map(|c| c.ptr).collect(),
            Node::Leaf(_) => Vec::new(),
        };
        let mut locs = Vec::with_capacity(children.len());
        let mut min_segs = Vec::new();
        for (i, ptr) in children.into_iter().enumerate() {
            match ptr {
                NodePtr::Disk(loc) => locs.push(loc),
                NodePtr::Mem(cid) => {
                    let was_dirty = self.slot(cid).dirty;
                    let (loc, ms) = self.flush_node(cid)?;
                    if was_dirty {
                        min_segs.push((i, ms));
                    }
                    locs.push(loc);
                }
            }
        }
        if !min_segs.is_empty()
            && let Node::Internal(ch) = self.node_mut(id)
        {
            for (i, ms) in min_segs {
                ch[i].min_seg = ms;
            }
        }
        let node = self.slot(id).node.clone().expect("node taken");
        let mut buf = Vec::with_capacity(node.byte_size() + 16);
        node.encode(&locs, &mut buf);
        let loc = self.segs.append(&buf)?;
        let e = self.seg_live.entry(loc.seg).or_insert((0, 0));
        e.0 += loc.len as u64;
        e.1 += loc.len as u64;
        let min_seg = match &*node {
            Node::Leaf(_) => loc.seg,
            Node::Internal(ch) => ch.iter().map(|c| c.min_seg).min().unwrap_or(u32::MAX).min(loc.seg),
        };
        let slot = self.slot_mut(id);
        slot.disk = Some(loc);
        slot.dirty = false;
        let (bytes, version) = (slot.bytes, slot.version);
        self.dirty_bytes -= bytes;
        if let Some(f) = self.flushed.as_mut() {
            f.push(Flushed { slot: id, version, loc, min_seg });
        }
        Ok((loc, min_seg))
    }

    /// An independent version of every tree as of now, for another thread:
    /// it shares the loaded nodes (copied on write on either side) and the
    /// open segment files, has a buffer pool of its own and cannot take
    /// checkpoints. Trees created empty here and filled in a fork can be
    /// moved back with `adopt_trees`.
    pub fn fork(&self) -> Result<Env> {
        Ok(Env {
            dir: self.dir.clone(),
            opts: self.opts.clone(),
            // (segments holding nodes: others may be gone, deleted by a
            // checkpoint job in progress, also those holding only nodes of
            // dropped trees still to release, see `fork`)
            segs: self.segs.fork(self.seg_live.iter().filter(|(_, (_, live))| *live > 0).map(|(id, _)| *id))?,
            slots: self.slots.clone(),
            free_slots: self.free_slots.clone(),
            mem_bytes: self.mem_bytes,
            dirty_bytes: self.dirty_bytes,
            clock_hand: 0,
            trees: self.trees.clone(),
            next_tree_id: self.next_tree_id,
            seg_live: self.seg_live.clone(),
            frontier: self.frontier,
            checkpoint_seq: self.checkpoint_seq,
            checkpoint_gsn: self.checkpoint_gsn,
            manifest: None,
            stamp: self.stamp,
            flushed: None,
            releasing: Vec::new(),
            released: Vec::new(),
            reads: 0,
        })
    }

    /// Move trees `ids` from fork `from`, where they were filled, into this
    /// environment, where they must still be as empty as when the fork was
    /// taken. Their nodes are moved, not copied.
    pub fn adopt_trees(&mut self, from: &mut Env, ids: &[TreeId]) -> Result<()> {
        for &id in ids {
            if self.trees.get(&id).is_none_or(|t| t.root.is_some()) {
                return Err(SqlError::internal(format!("tree {id} to adopt is not an empty tree here")));
            }
            let t = from.trees.remove(&id).ok_or_else(|| SqlError::internal(format!("no tree {id} to adopt")))?;
            let root = match t.root {
                None => None,
                Some(ptr) => Some(NodePtr::Mem(self.adopt_node(from, ptr, NONE, id)?)),
            };
            self.trees.insert(id, TreeState { root, ..t });
        }
        Ok(())
    }

    fn adopt_node(&mut self, from: &mut Env, ptr: NodePtr, parent: u32, tree: TreeId) -> Result<u32> {
        // (the fork cannot write nodes: a tree filled there is in its memory)
        let NodePtr::Mem(fid) = ptr else { return Err(SqlError::internal("adopted tree has a node on disk")) };
        let mut slot = from.slots[fid as usize].take().expect("dangling node slot");
        from.free_slots.push(fid);
        from.mem_bytes -= slot.bytes;
        from.dirty_bytes -= slot.bytes;
        debug_assert!(slot.dirty);
        slot.parent = parent;
        slot.tree = tree;
        slot.version = self.next_stamp();
        slot.edits = 0;
        self.mem_bytes += slot.bytes;
        self.dirty_bytes += slot.bytes;
        let children: Vec<NodePtr> = match slot.node.as_deref() {
            Some(Node::Internal(ch)) => ch.iter().map(|c| c.ptr).collect(),
            _ => Vec::new(),
        };
        slot.loaded_children = children.len() as u32;
        let id = match self.free_slots.pop() {
            Some(id) => {
                self.slots[id as usize] = Some(slot);
                id
            }
            None => {
                self.slots.push(Some(slot));
                (self.slots.len() - 1) as u32
            }
        };
        if !children.is_empty() {
            let mut moved = Vec::with_capacity(children.len());
            for c in children {
                moved.push(self.adopt_node(from, c, id, tree)?);
            }
            let Node::Internal(ch) = self.node_mut(id) else { unreachable!() };
            for (c, m) in ch.iter_mut().zip(moved) {
                c.ptr = NodePtr::Mem(m);
            }
        }
        Ok(id)
    }

    /// Drop a tree. Its loaded nodes are freed now; its nodes on disk are
    /// released by the next checkpoint (which reads them off the engine
    /// thread), and count as live until then.
    pub fn drop_tree(&mut self, tree: TreeId) -> Result<()> {
        let Some(t) = self.trees.remove(&tree) else { return Ok(()) };
        if let Some(ptr) = t.root {
            self.release_loaded(ptr);
        }
        Ok(())
    }

    /// Free the loaded nodes of a dropped subtree, leaving its parts on disk
    /// to the next checkpoint.
    fn release_loaded(&mut self, ptr: NodePtr) {
        match ptr {
            NodePtr::Disk(loc) => self.releasing.push(loc),
            NodePtr::Mem(id) => {
                let children: Vec<NodePtr> = match self.node(id) {
                    Node::Internal(ch) => ch.iter().map(|c| c.ptr).collect(),
                    Node::Leaf(_) => Vec::new(),
                };
                for c in children {
                    self.release_loaded(c);
                }
                self.slot_mut(id).loaded_children = 0;
                self.slot_mut(id).parent = NONE;
                self.free_slot(id);
            }
        }
    }

    /// Release a dropped subtree: its nodes no longer count as live.
    fn release(&mut self, ptr: NodePtr) -> Result<()> {
        let (children, disk, slot_id) = match ptr {
            NodePtr::Disk(loc) => {
                // only internal nodes need reading to find their children
                self.reads += 1;
                let bytes = self.segs.read(loc)?;
                let node = Node::decode(&bytes)?;
                let ch = match node {
                    Node::Internal(ch) => ch.into_iter().map(|c| c.ptr).collect(),
                    Node::Leaf(_) => vec![],
                };
                (ch, Some(loc), None)
            }
            NodePtr::Mem(id) => {
                let slot = self.slot(id);
                let ch = match slot.node.as_deref().unwrap() {
                    Node::Internal(ch) => ch.iter().map(|c| c.ptr).collect(),
                    Node::Leaf(_) => vec![],
                };
                (ch, None, Some(id))
            }
        };
        for c in children {
            self.release(c)?;
        }
        if let Some(loc) = disk {
            self.dec_live(loc);
            self.released.push(loc);
        }
        if let Some(id) = slot_id {
            self.slot_mut(id).loaded_children = 0;
            self.slot_mut(id).parent = NONE;
            self.free_slot(id);
        }
        Ok(())
    }

    /// Relocate live nodes of the oldest sparse segments so they can be
    /// deleted after the next checkpoint. Returns the number of nodes dirtied.
    pub fn gc(&mut self, max_nodes: usize) -> Result<usize> {
        let active = self.segs.active_id();
        let victim = self
            .seg_live
            .iter()
            .find(|(id, (total, live))| **id != active && *total > 0 && live * 2 < *total)
            .map(|(id, _)| *id);
        let Some(victim) = victim else { return Ok(0) };
        let mut budget = max_nodes;
        for tid in self.tree_ids() {
            if budget == 0 {
                break;
            }
            if self.trees[&tid].min_seg > victim {
                continue;
            }
            if let Some(root) = self.root_slot(tid)? {
                self.relocate(root, victim, &mut budget)?;
            }
        }
        self.maybe_evict();
        Ok(max_nodes - budget)
    }

    fn relocate(&mut self, id: u32, victim: u32, budget: &mut usize) -> Result<()> {
        if *budget == 0 {
            return Ok(());
        }
        if let Some(loc) = self.slot(id).disk
            && loc.seg <= victim
        {
            self.dirty_path(id);
            *budget -= 1;
        }
        let todo: Vec<usize> = match self.node(id) {
            Node::Internal(ch) => (0..ch.len()).filter(|&i| ch[i].min_seg <= victim).collect(),
            Node::Leaf(_) => vec![],
        };
        for i in todo {
            if *budget == 0 {
                break;
            }
            let cid = self.load_child(id, i)?;
            self.relocate(cid, victim, budget)?;
            let ms = self.compute_min_seg(cid);
            if let Node::Internal(ch) = self.node_mut(id) {
                ch[i].min_seg = ms;
            }
        }
        Ok(())
    }

    fn compute_min_seg(&self, id: u32) -> u32 {
        let slot = self.slot(id);
        let own = slot.disk.map(|l| l.seg).unwrap_or(u32::MAX);
        match slot.node.as_deref().unwrap() {
            Node::Leaf(_) => own,
            Node::Internal(ch) => ch.iter().map(|c| c.min_seg).min().unwrap_or(u32::MAX).min(own),
        }
    }

    pub fn stats(&self) -> EnvStats {
        let mut s = EnvStats { trees: self.trees.len(), mem_bytes: self.mem_bytes, ..Default::default() };
        for slot in self.slots.iter().flatten() {
            s.loaded_nodes += 1;
            if slot.dirty {
                s.dirty_nodes += 1;
            }
        }
        s.segments = self.seg_live.iter().map(|(id, (t, l))| (*id, *t, *l)).collect();
        s.checkpoint_gsn = self.checkpoint_gsn;
        s
    }

    /// Node frames this environment has read from segments.
    pub fn segment_reads(&self) -> u64 {
        self.reads
    }

    pub fn dirty_bytes(&self) -> usize {
        debug_assert_eq!(self.dirty_bytes, self.slots.iter().flatten().filter(|s| s.dirty).map(|s| s.bytes).sum::<usize>());
        self.dirty_bytes
    }
}

/// Split a sorted vector into chunks of about `target` bytes, each with at
/// least `min_items` items, balancing chunk sizes.
fn split_by_bytes<T>(items: Vec<T>, target: usize, size: fn(&T) -> usize, min_items: usize) -> Vec<Vec<T>> {
    let total: usize = items.iter().map(size).sum();
    if total <= target || items.len() < 2 * min_items.max(1) {
        return vec![items];
    }
    // A batch spread over many nodes (a bulk load) leaves room in each, so
    // that the next inserts do not split full nodes right away.
    let fill = if total > 2 * target { target - target / 10 } else { target };
    let nchunks = total.div_ceil(fill).max(2).min(items.len() / min_items.max(1));
    let per = total.div_ceil(nchunks);
    let mut out = Vec::with_capacity(nchunks);
    let mut cur = Vec::new();
    let mut cur_bytes = 0;
    let len = items.len();
    for (i, it) in items.into_iter().enumerate() {
        let s = size(&it);
        let remaining_items = len - i;
        if cur.len() >= min_items && cur_bytes + s > per && remaining_items >= min_items {
            out.push(std::mem::take(&mut cur));
            cur_bytes = 0;
        }
        cur_bytes += s;
        cur.push(it);
    }
    if !cur.is_empty() {
        if cur.len() < min_items
            && let Some(last) = out.last_mut()
        {
            last.extend(cur);
        } else {
            out.push(cur);
        }
    }
    out
}

/// Shortest key `s` with `a < s <= b` (for `a < b`): B+tree suffix truncation.
/// The nodes an update leaves in place of one (usually just that one).
type Parts = smallvec::SmallVec<[Part; 1]>;

/// A node produced by an update: its separator, slot, summary and min_seg.
struct Part {
    sep: Box<[u8]>,
    id: u32,
    summary: Summary,
    min_seg: u32,
}

impl Part {
    fn into_child(self) -> Child {
        Child { sep: self.sep, ptr: NodePtr::Mem(self.id), summary: self.summary, min_seg: self.min_seg }
    }
}

fn leaf_summary(entries: &[LeafEntry], summ: &Arc<dyn Summarizer>) -> Result<Summary> {
    let mut s = Summary::empty(summ.spec());
    if summ.spec().is_empty() {
        s.count = entries.iter().map(|e| e.hist.total()).sum();
    } else {
        summ.add_entries(&mut s, &mut entries.iter().map(|e| (&*e.key, e.hist.total())))?;
    }
    Ok(s)
}

/// Summary of a leaf whose entries are now `entries`, after the weight
/// changes `changes` (index into `ups`, weight before) to a leaf whose
/// summary was `old`: folded into `old` when that is cheaper than
/// recomputing from `entries` and possible (a MIN/MAX may lose its extreme).
fn summary_after(
    old: Option<&Summary>,
    entries: &[LeafEntry],
    ups: &[(Vec<u8>, i64)],
    changes: &[(usize, i64)],
    summ: &Arc<dyn Summarizer>,
) -> Result<Summary> {
    if let Some(old) = old
        && changes.len() < entries.len()
    {
        let mut s = old.clone();
        let mut exact = true;
        for &(i, before) in changes {
            let (k, w) = &ups[i];
            if !summ.update_entry(&mut s, k, before, before + w)? {
                exact = false;
                break;
            }
        }
        if exact {
            return Ok(s);
        }
    }
    leaf_summary(entries, summ)
}

fn children_summary(children: &[Child], summ: &Arc<dyn Summarizer>) -> Summary {
    let mut s = Summary::empty(summ.spec());
    for c in children {
        s.combine(&c.summary);
    }
    s
}

fn shortest_separator(a: &[u8], b: &[u8]) -> Box<[u8]> {
    let common = a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count();
    let n = (common + 1).min(b.len());
    b[..n].to_vec().into_boxed_slice()
}

#[cfg(test)]
mod tests;
