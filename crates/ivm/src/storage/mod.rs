//! Storage: an ordered MVCC index keyed by byte strings (memcomparable
//! (key, value) encodings) holding (GSN, weight) histories, augmented with
//! monoid summaries per subtree.
//!
//! The tree is copy-on-write. Nodes live in a node arena that acts as the
//! buffer pool: clean nodes are evicted under memory pressure (CLOCK), and
//! dirty nodes are written to the active append-only segment by the
//! checkpoint or, when they exceed their share of the cache, early by the
//! pool itself (a written node is clean and can be evicted; nothing refers
//! to it durably until a checkpoint does). A checkpoint is: write the
//! remaining dirty nodes bottom-up, fsync, append a manifest record naming
//! the new roots. Sealed segments are immutable; space is reclaimed by
//! relocating the live nodes of old segments (found through per-subtree
//! `min_seg` augmentation) and deleting segments once nothing references
//! them.

pub mod history;
mod inspect;
pub mod manifest;
pub mod node;
pub mod segment;
pub mod sort;
pub mod summary;

pub use history::{History, ReadAt};
pub use inspect::TreeResidency;
pub use sort::TreeBuilder;
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

struct Slot {
    node: Option<Node>,
    disk: Option<DiskLoc>,
    dirty: bool,
    /// In-place leaf edits since the leaf's entries were last compacted.
    edits: u32,
    parent: u32,
    tree: TreeId,
    loaded_children: u32,
    refbit: bool,
    bytes: usize,
}

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
    /// Slots visited by eviction sweeps so far (their cost).
    pub swept_slots: u64,
}

pub struct Env {
    #[allow(dead_code)]
    dir: PathBuf,
    opts: EnvOptions,
    segs: SegmentStore,
    manifest: Manifest,
    slots: Vec<Option<Slot>>,
    free_slots: Vec<u32>,
    mem_bytes: usize,
    /// Bytes of dirty nodes (maintained with the dirty flags).
    dirty_bytes: usize,
    clock_hand: usize,
    /// (mem_bytes, dirty_bytes) when a sweep last found nothing more to
    /// evict or write: sweeps resume once either has grown well past it.
    evict_stall: Option<(usize, usize)>,
    swept_slots: u64,
    trees: FxHashMap<TreeId, TreeState>,
    next_tree_id: TreeId,
    /// segment id -> (total bytes written, live bytes referenced)
    seg_live: BTreeMap<u32, (u64, u64)>,
    frontier: Gsn,
    checkpoint_seq: u64,
    checkpoint_gsn: Gsn,
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
            manifest,
            slots: Vec::new(),
            free_slots: Vec::new(),
            mem_bytes: 0,
            dirty_bytes: 0,
            clock_hand: 0,
            evict_stall: None,
            swept_slots: 0,
            trees: FxHashMap::default(),
            next_tree_id: 1,
            seg_live: BTreeMap::new(),
            frontier: 0,
            checkpoint_seq: 0,
            checkpoint_gsn: 0,
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

    /// A tree for temporary contents (sort runs), which its user drops. Its
    /// id is taken from the top of the id range downwards, leaving the ids
    /// of `create_tree` a function of the operations that create trees. One
    /// that a checkpoint happens to record is not attached after recovery,
    /// so the engine drops it then.
    pub fn create_temp_tree(&mut self, summarizer: Arc<dyn Summarizer>) -> TreeId {
        let mut id = TreeId::MAX;
        while self.trees.contains_key(&id) {
            id -= 1;
        }
        let summary = Summary::empty(summarizer.spec());
        self.trees.insert(id, TreeState { root: None, summary, min_seg: u32::MAX, summarizer, attached: true });
        id
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

    fn alloc_slot(&mut self, node: Node, disk: Option<DiskLoc>, parent: u32, tree: TreeId) -> u32 {
        let bytes = node.byte_size();
        self.mem_bytes += bytes;
        if disk.is_none() {
            self.dirty_bytes += bytes;
        }
        let slot = Slot { node: Some(node), disk, dirty: disk.is_none(), edits: 0, parent, tree, loaded_children: 0, refbit: true, bytes };
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
        let slot = self.slot_mut(id);
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
        let slot = self.slot_mut(id);
        let old = slot.bytes;
        let dirty = slot.dirty;
        slot.bytes = bytes;
        slot.node = Some(node);
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
            && let Some(Node::Internal(ch)) = self.slot_mut(parent).node.as_mut()
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
        match slot.node.as_ref().unwrap() {
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
        match slot.node.as_ref().unwrap() {
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
        // A large batch is applied in runs of consecutive keys, each dirtying
        // at most a quarter of the cache (a whole leaf per update at worst),
        // and the pool makes room between runs: a batch never needs its
        // dirty nodes to fit in memory at once.
        let budget = (self.opts.cache_bytes / 4).max(4 * self.opts.leaf_bytes);
        let mut start = 0;
        while start < updates.len() {
            let mut end = start;
            let mut cost = 0;
            while end < updates.len() && (end == start || cost + self.opts.leaf_bytes + updates[end].0.len() <= budget) {
                cost += self.opts.leaf_bytes + updates[end].0.len();
                end += 1;
            }
            if let Err(e) = self.apply_run(tree, &updates[start..end], gsn, &summ) {
                updates.clear();
                return Err(e);
            }
            self.maybe_evict()?;
            start = end;
        }
        updates.clear();
        Ok(())
    }

    /// Apply sorted, consolidated updates (one descent from the root).
    fn apply_run(&mut self, tree: TreeId, updates: &[(Vec<u8>, i64)], gsn: Gsn, summ: &Arc<dyn Summarizer>) -> Result<()> {
        let root = match self.root_slot(tree)? {
            Some(id) => id,
            None => self.alloc_slot(Node::Leaf(Vec::new()), None, NONE, tree),
        };
        // the tree's summary is replaced below
        let old = std::mem::take(&mut self.trees.get_mut(&tree).unwrap().summary);
        let parts = self.apply_node(root, updates, gsn, summ, Some(&old));
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
            _ => Some(self.build_root(parts, tree, summ)?),
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
        let node = self.slot_mut(id).node.take().expect("node taken");
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
                    self.maybe_evict()?;
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
            self.maybe_evict()?;
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
                    self.maybe_evict()?;
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
                    self.maybe_evict()?;
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
        self.maybe_evict()?;
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
        self.maybe_evict()?;
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
    // eviction and early writes

    /// Dirty bytes the pool keeps in memory even without memory pressure:
    /// beyond them it writes cold dirty nodes early, which bounds the work
    /// left to a checkpoint.
    fn dirty_limit(&self) -> usize {
        self.opts.cache_bytes / 2
    }

    /// One CLOCK sweep (at most two passes over the slots) while the arena
    /// exceeds the cache or the dirty nodes exceed `dirty_limit`. Only nodes
    /// without loaded children are candidates, and those whose reference bit
    /// is set get a second chance. A cold candidate is evicted while the
    /// arena is over the cache, a dirty one after writing it to the active
    /// segment: dirty and clean nodes compete for the cache alike. Within
    /// the cache, cold dirty candidates are only written, while the dirty
    /// bytes are over their limit. Every loaded subtree has candidates at
    /// its leaves, so a sweep stops early unless dirty internal nodes hold
    /// their clean children in memory. A sweep that ends over the limits
    /// records a stall, and the next sweep waits for the arena or the dirty
    /// bytes to grow well past it, which bounds the cost of sweeping when
    /// nothing can be done.
    fn maybe_evict(&mut self) -> Result<()> {
        let (cache, limit) = (self.opts.cache_bytes, self.dirty_limit());
        if (self.mem_bytes <= cache && self.dirty_bytes <= limit) || self.slots.is_empty() {
            return Ok(());
        }
        if let Some((mem, dirty)) = self.evict_stall {
            let slack = cache / 16 + self.opts.leaf_bytes;
            if self.mem_bytes <= mem + slack && self.dirty_bytes <= dirty + slack {
                return Ok(());
            }
            self.evict_stall = None;
        }
        let n = self.slots.len();
        let mut scanned = 0;
        while (self.mem_bytes > cache || self.dirty_bytes > limit) && scanned < 2 * n {
            let i = self.clock_hand % n;
            self.clock_hand = (i + 1) % n;
            scanned += 1;
            self.swept_slots += 1;
            let Some(slot) = self.slots[i].as_mut() else { continue };
            if slot.loaded_children > 0 || slot.node.is_none() {
                continue;
            }
            if slot.refbit {
                slot.refbit = false;
                continue;
            }
            if slot.dirty {
                if self.mem_bytes <= cache && self.dirty_bytes <= limit {
                    continue;
                }
                // its children are all on disk (none is loaded)
                self.write_node(i as u32, usize::MAX)?;
            }
            if self.mem_bytes > cache {
                self.evict(i as u32);
            }
        }
        if self.mem_bytes > cache || self.dirty_bytes > limit {
            self.evict_stall = Some((self.mem_bytes, self.dirty_bytes));
        }
        Ok(())
    }

    /// Write dirty nodes early until at most `target` dirty bytes remain
    /// (or nothing more can be written without evicting clean nodes), as
    /// the pool does on its own beyond half of the cache. A hook for callers
    /// that manage memory or checkpoints incrementally: the nodes written
    /// are clean and evictable, the next checkpoint only writes the rest.
    /// Returns the dirty bytes left.
    pub fn write_back(&mut self, target: usize) -> Result<usize> {
        // children before parents: leaves first, then each level above
        while self.dirty_bytes > target {
            let before = self.dirty_bytes;
            for i in 0..self.slots.len() {
                if self.dirty_bytes <= target {
                    break;
                }
                let writable = match &self.slots[i] {
                    Some(Slot { dirty: true, node: Some(node), .. }) => match node {
                        Node::Leaf(_) => true,
                        Node::Internal(ch) => {
                            ch.iter().all(|c| !matches!(c.ptr, NodePtr::Mem(cid) if self.slot(cid).dirty))
                        }
                    },
                    _ => false,
                };
                if writable {
                    self.write_node(i as u32, usize::MAX)?;
                }
            }
            if self.dirty_bytes == before {
                break;
            }
        }
        self.maybe_evict()?;
        Ok(self.dirty_bytes)
    }

    /// Write dirty node `id`, whose loaded children are all clean, to the
    /// end of the active segment, recording its children's locations; it
    /// becomes clean (and evictable). Nothing durable refers to the frame
    /// until a checkpoint record names a root above it: copy-on-write never
    /// overwrites a node of the last checkpoint, and a crash discards the
    /// frame with the rest of the segment beyond the recorded length (or
    /// the segment, if it was started after the checkpoint). If the node is
    /// modified again, the frame is garbage like any replaced node's.
    /// `idx` is the node's position in its parent, if known.
    fn write_node(&mut self, id: u32, idx: usize) -> Result<DiskLoc> {
        let slot = self.slot(id);
        debug_assert!(slot.dirty, "writing a clean node");
        let node = slot.node.as_ref().expect("node taken");
        let mut locs = Vec::new();
        let mut min_seg = u32::MAX;
        if let Node::Internal(children) = node {
            locs.reserve(children.len());
            for c in children {
                locs.push(match c.ptr {
                    NodePtr::Disk(loc) => loc,
                    NodePtr::Mem(cid) => self.slot(cid).disk.expect("writing a node above a dirty child"),
                });
                min_seg = min_seg.min(c.min_seg);
            }
        }
        let mut buf = Vec::with_capacity(slot.bytes + 16);
        node.encode(&locs, &mut buf);
        let loc = self.segs.append(&buf)?;
        let e = self.seg_live.entry(loc.seg).or_insert((0, 0));
        e.0 += loc.len as u64;
        e.1 += loc.len as u64;
        let min_seg = min_seg.min(loc.seg);
        let slot = self.slot_mut(id);
        slot.disk = Some(loc);
        slot.dirty = false;
        let (bytes, parent, tree) = (slot.bytes, slot.parent, slot.tree);
        self.dirty_bytes -= bytes;
        // The entry above the node covers exactly its subtree; the entries
        // above that cover the subtree and more, so they can only drop to
        // `min_seg` (the active segment: no lower unless already lower).
        let mut child = id;
        let mut at = parent;
        let mut hint = idx;
        let mut exact = true;
        while at != NONE {
            let Some(Node::Internal(ch)) = self.slots[at as usize].as_mut().and_then(|s| s.node.as_mut()) else {
                unreachable!("parent of a loaded node is not a loaded internal node")
            };
            let i = match ch.get(hint) {
                Some(c) if c.ptr == NodePtr::Mem(child) => hint,
                _ => ch.iter().position(|c| c.ptr == NodePtr::Mem(child)).expect("child pointer"),
            };
            if exact {
                ch[i].min_seg = min_seg;
            } else if ch[i].min_seg <= min_seg {
                return Ok(loc);
            } else {
                ch[i].min_seg = min_seg;
            }
            exact = false;
            hint = usize::MAX;
            child = at;
            at = self.slot(at).parent;
        }
        if let Some(t) = self.trees.get_mut(&tree) {
            t.min_seg = if exact { min_seg } else { t.min_seg.min(min_seg) };
        }
        Ok(loc)
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
            if let Some(Node::Internal(ch)) = p.node.as_mut() {
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

    /// Make all trees durable as of `gsn` together with the application state.
    pub fn checkpoint(&mut self, gsn: Gsn, app: &[u8]) -> Result<()> {
        let tree_ids = self.tree_ids();
        let mut records = Vec::with_capacity(tree_ids.len());
        for tid in tree_ids {
            let loc = match self.trees[&tid].root {
                None => None,
                Some(NodePtr::Disk(loc)) => Some(loc),
                Some(NodePtr::Mem(id)) => {
                    self.flush_node(id, usize::MAX)?;
                    self.slot(id).disk
                }
            };
            let t = &self.trees[&tid];
            records.push(TreeRecord { id: tid, root: loc.map(|l| (l, t.summary.clone(), t.min_seg)) });
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
        self.manifest.append(&rec)?;
        self.checkpoint_gsn = gsn;
        // every node is clean now
        self.evict_stall = None;
        // reclaim fully dead sealed segments
        let dead: Vec<u32> = self.seg_live.iter().filter(|(id, (_, l))| **id != active && *l == 0).map(|(id, _)| *id).collect();
        for id in dead {
            self.seg_live.remove(&id);
            self.segs.delete(id)?;
        }
        Ok(())
    }

    /// Write the dirty nodes of the subtree at `id` (at position `idx` in
    /// its parent), children before parents.
    fn flush_node(&mut self, id: u32, idx: usize) -> Result<()> {
        if !self.slot(id).dirty {
            return Ok(());
        }
        let dirty: Vec<(usize, u32)> = match self.node(id) {
            Node::Leaf(_) => Vec::new(),
            Node::Internal(children) => children
                .iter()
                .enumerate()
                .filter_map(|(i, c)| match c.ptr {
                    NodePtr::Mem(cid) if self.slot(cid).dirty => Some((i, cid)),
                    _ => None,
                })
                .collect(),
        };
        for (i, cid) in dirty {
            self.flush_node(cid, i)?;
        }
        self.write_node(id, idx)?;
        Ok(())
    }

    /// Drop a tree, releasing its nodes.
    pub fn drop_tree(&mut self, tree: TreeId) -> Result<()> {
        let Some(t) = self.trees.remove(&tree) else { return Ok(()) };
        if let Some(ptr) = t.root {
            self.release(ptr)?;
        }
        Ok(())
    }

    fn release(&mut self, ptr: NodePtr) -> Result<()> {
        let (children, disk, slot_id) = match ptr {
            NodePtr::Disk(loc) => {
                // only internal nodes need reading to find their children
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
                let ch = match slot.node.as_ref().unwrap() {
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
        self.maybe_evict()?;
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
            if let Some(Node::Internal(ch)) = self.slot_mut(id).node.as_mut() {
                ch[i].min_seg = ms;
            }
        }
        Ok(())
    }

    fn compute_min_seg(&self, id: u32) -> u32 {
        let slot = self.slot(id);
        let own = slot.disk.map(|l| l.seg).unwrap_or(u32::MAX);
        match slot.node.as_ref().unwrap() {
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
        s.swept_slots = self.swept_slots;
        s
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
