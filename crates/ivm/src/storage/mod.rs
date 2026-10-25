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

pub mod history;
pub mod manifest;
pub mod node;
pub mod segment;
pub mod summary;

pub use history::{History, ReadAt};
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
        std::fs::create_dir_all(dir)?;
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
        let mut ups: Vec<(Box<[u8]>, i64)> = Vec::with_capacity(updates.len());
        for (k, w) in updates.drain(..) {
            match ups.last_mut() {
                Some((lk, lw)) if **lk == *k => *lw += w,
                _ => ups.push((k.into_boxed_slice(), w)),
            }
        }
        ups.retain(|(_, w)| *w != 0);
        if ups.is_empty() {
            return Ok(());
        }
        let t = self.trees.get(&tree).ok_or_else(|| SqlError::internal(format!("no tree {tree}")))?;
        let summ = t.summarizer.clone();
        let old = t.summary.clone();
        let root = match self.root_slot(tree)? {
            Some(id) => id,
            None => self.alloc_slot(Node::Leaf(Vec::new()), None, NONE, tree),
        };
        let mut parts = self.apply_node(root, &ups, gsn, &summ, Some(&old))?;
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

    fn build_root(&mut self, mut parts: Vec<Part>, tree: TreeId, summ: &Arc<dyn Summarizer>) -> Result<Part> {
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
        ups: &[(Box<[u8]>, i64)],
        gsn: Gsn,
        summ: &Arc<dyn Summarizer>,
        old: Option<&Summary>,
    ) -> Result<Vec<Part>> {
        let was_dirty = self.slot(id).dirty;
        let edits = self.slot(id).edits;
        let old_bytes = self.slot(id).bytes;
        self.make_dirty(id);
        let node = self.slot_mut(id).node.take().expect("node taken");
        match node {
            Node::Leaf(entries) => {
                let frontier = self.frontier;
                // the leaf's summary, updated entry by entry while possible
                let mut incr = old.cloned();
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
                    for (k, w) in ups {
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
                                let e = LeafEntry { key: k.clone(), hist: h };
                                size += entry_size(&e);
                                entries.insert(pos, e);
                                pos += 1;
                            }
                            0
                        };
                        if let Some(s) = incr.as_mut()
                            && !summ.update_entry(s, k, before, before + w)?
                        {
                            incr = None;
                        }
                    }
                    entries
                } else {
                    let mut out: Vec<LeafEntry> = Vec::with_capacity(entries.len() + ups.len());
                    let mut it = entries.into_iter().peekable();
                    for (k, w) in ups {
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
                                    out.push(LeafEntry { key: k.clone(), hist: h });
                                }
                                0
                            }
                        };
                        if let Some(s) = incr.as_mut()
                            && !summ.update_entry(s, k, before, before + w)?
                        {
                            incr = None;
                        }
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
                    return Ok(vec![]);
                }
                if in_place && size <= self.opts.leaf_bytes {
                    // common case: no split, size already known
                    let summary = match incr.take() {
                        Some(s) => s,
                        None => leaf_summary(&out, summ)?,
                    };
                    let first = out[0].key.clone();
                    self.put_node_sized(id, Node::Leaf(out), size);
                    return Ok(vec![Part { sep: first, id, summary, min_seg: u32::MAX }]);
                }
                let chunks = split_by_bytes(out, self.opts.leaf_bytes, entry_size, 1);
                let single = chunks.len() == 1;
                let parent = self.slot(id).parent;
                let tree = self.slot(id).tree;
                let mut parts = Vec::with_capacity(chunks.len());
                let mut prev_last: Option<Box<[u8]>> = None;
                for (i, chunk) in chunks.into_iter().enumerate() {
                    // suffix truncation: the shortest separator between chunks
                    let first = match &prev_last {
                        Some(a) => shortest_separator(a, &chunk[0].key),
                        None => chunk[0].key.clone(),
                    };
                    prev_last = Some(chunk.last().unwrap().key.clone());
                    let summary = match incr.take() {
                        Some(s) if single => s,
                        _ => leaf_summary(&chunk, summ)?,
                    };
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
                let mut runs: Vec<(usize, usize, usize)> = Vec::new();
                let (mut pos, mut ci) = (0usize, 0usize);
                while pos < ups.len() {
                    // the last child whose separator is <= the key (the first
                    // child also takes smaller keys)
                    ci += children[ci + 1..].partition_point(|c| *c.sep <= *ups[pos].0);
                    let end = if ci + 1 < n {
                        let next = &*children[ci + 1].sep;
                        pos + ups[pos..].partition_point(|(k, _)| **k < *next)
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
                        let news: Vec<&Summary> = parts.iter().map(|p| &p.summary).collect();
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
                    return Ok(vec![]);
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
    ) -> Result<Vec<Part>> {
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
        let first = children[0].sep.clone();
        self.put_node_sized(id, Node::Internal(children), size);
        Ok(vec![Part { sep: first, id, summary: s, min_seg }])
    }

    /// Store `children` in internal node `id`, splitting by size. `summary`
    /// is the node's summary if known (used when it does not split).
    fn finish_internal(
        &mut self,
        id: u32,
        children: Vec<Child>,
        summary: Option<Summary>,
        summ: &Arc<dyn Summarizer>,
    ) -> Result<Vec<Part>> {
        let parent = self.slot(id).parent;
        let tree = self.slot(id).tree;
        // internal nodes keep at least two children per chunk so that a tree
        // with oversized separators still converges to a single root
        let chunks = split_by_bytes(children, self.opts.internal_bytes, node::child_size, 2);
        let single = chunks.len() == 1;
        let mut summary = summary;
        let mut parts = Vec::with_capacity(chunks.len());
        for (i, chunk) in chunks.into_iter().enumerate() {
            let first = chunk[0].sep.clone();
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
        let mut c = self.cursor(tree, lo, hi);
        let mut out = Vec::new();
        while let Some(x) = self.cursor_next(&mut c)? {
            out.push(x);
        }
        Ok(out)
    }

    /// Entries whose key starts with `prefix`.
    pub fn scan_prefix(&mut self, tree: TreeId, prefix: &[u8]) -> Result<Vec<(Box<[u8]>, History)>> {
        match crate::types::encoding::prefix_successor(prefix) {
            Some(succ) => self.scan(tree, Bound::Included(prefix), Bound::Excluded(&succ)),
            None => self.scan(tree, Bound::Included(prefix), Bound::Unbounded),
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
                for e in &entries[start..] {
                    if !below_hi(&e.key, hi) {
                        break;
                    }
                    if summ.spec().is_empty() {
                        acc.count += e.hist.total();
                    } else {
                        summ.add_entry(acc, &e.key, e.hist.total())?;
                    }
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
        self.manifest.append(&rec)?;
        self.checkpoint_gsn = gsn;
        // reclaim fully dead sealed segments
        let dead: Vec<u32> = self.seg_live.iter().filter(|(id, (_, l))| **id != active && *l == 0).map(|(id, _)| *id).collect();
        for id in dead {
            self.seg_live.remove(&id);
            self.segs.delete(id)?;
        }
        Ok(())
    }

    fn flush_node(&mut self, id: u32) -> Result<(DiskLoc, u32)> {
        if !self.slot(id).dirty {
            let loc = self.slot(id).disk.unwrap();
            return Ok((loc, loc.seg));
        }
        let mut node = self.slot_mut(id).node.take().unwrap();
        let mut locs = Vec::new();
        if let Node::Internal(children) = &mut node {
            locs.reserve(children.len());
            for c in children.iter_mut() {
                match c.ptr {
                    NodePtr::Disk(loc) => locs.push(loc),
                    NodePtr::Mem(cid) => {
                        let was_dirty = self.slot(cid).dirty;
                        let (loc, ms) = self.flush_node(cid)?;
                        if was_dirty {
                            c.min_seg = ms;
                        }
                        locs.push(loc);
                    }
                }
            }
        }
        let mut buf = Vec::with_capacity(node.byte_size() + 16);
        node.encode(&locs, &mut buf);
        let loc = self.segs.append(&buf)?;
        let e = self.seg_live.entry(loc.seg).or_insert((0, 0));
        e.0 += loc.len as u64;
        e.1 += loc.len as u64;
        let min_seg = match &node {
            Node::Leaf(_) => loc.seg,
            Node::Internal(ch) => ch.iter().map(|c| c.min_seg).min().unwrap_or(u32::MAX).min(loc.seg),
        };
        let slot = self.slot_mut(id);
        slot.node = Some(node);
        slot.disk = Some(loc);
        if slot.dirty {
            slot.dirty = false;
            let bytes = slot.bytes;
            self.dirty_bytes -= bytes;
        }
        Ok((loc, min_seg))
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
    let nchunks = total.div_ceil(target).max(2).min(items.len() / min_items.max(1));
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
        for e in entries {
            summ.add_entry(&mut s, &e.key, e.hist.total())?;
        }
    }
    Ok(s)
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
