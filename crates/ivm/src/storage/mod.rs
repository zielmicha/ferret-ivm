//! Storage: an ordered MVCC index keyed by byte strings (memcomparable
//! (key, value) encodings) holding (GSN, weight) histories, augmented with
//! monoid summaries per subtree.
//!
//! The tree is copy-on-write at checkpoint granularity. Nodes live in a node
//! arena that acts as the buffer pool: clean nodes are evicted under memory
//! pressure (CLOCK), dirty nodes stay until the next checkpoint writes them
//! into the active append-only segment. A checkpoint is: write dirty nodes
//! bottom-up, fsync, append a manifest record naming the new roots. Sealed
//! segments are immutable. A budgeted maintenance pass before each
//! checkpoint reclaims space: it compacts histories behind the retention
//! frontier in leaves that per-subtree `due` augmentation says are worth
//! rewriting, merges underfull neighbours, and relocates the live nodes of
//! sparse segments (found through per-subtree `min_seg` augmentation), which
//! are deleted once nothing references them.

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
use manifest::{CheckpointRecord, Manifest, TreeRecord, TreeSpace};
use node::{entry_size, leaf_compaction, leaf_due, Child, DiskLoc, LeafEntry, Node, NodePtr};
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
    /// Dirtied by the maintenance pass and not modified since (its write
    /// does not count towards the next pass's budget).
    by_maint: bool,
}

struct TreeState {
    root: Option<NodePtr>,
    summary: Summary,
    min_seg: u32,
    summarizer: Arc<dyn Summarizer>,
    /// False for trees recovered from the manifest but not yet attached.
    attached: bool,
    /// `Child::due` of the root (a lower bound while the root is dirty).
    due: Gsn,
    /// Live bytes of the tree's nodes per segment, so that dropping the tree
    /// releases them without visiting its nodes.
    seg_live: BTreeMap<u32, u64>,
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
    /// Bytes of node frames read from segments since the store was opened.
    pub bytes_read: u64,
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
    /// Bytes checkpoints wrote for nodes the engine (not the maintenance
    /// pass) modified, since the last pass.
    fg_appended: u64,
    /// Write internal nodes as kind 2 and checkpoint records without tree
    /// space (tests of stores written by earlier versions).
    legacy_format: bool,
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
            trees: FxHashMap::default(),
            next_tree_id: 1,
            seg_live: BTreeMap::new(),
            frontier: 0,
            checkpoint_seq: 0,
            checkpoint_gsn: 0,
            fg_appended: 0,
            legacy_format: false,
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
                let recorded = r.space.is_some();
                let mut space = r.space.unwrap_or_default().into_iter();
                for t in r.trees {
                    let (root, summary, min_seg) = match t.root {
                        None => (None, Summary::empty(&[]), u32::MAX),
                        Some((loc, s, m)) => (Some(NodePtr::Disk(loc)), s, m),
                    };
                    // without recorded space (an earlier version's record),
                    // every subtree is due for a visit (as its kind-2 nodes
                    // say) and the tree's live bytes are counted below
                    let sp = space.next().unwrap_or_default();
                    let seg_live = sp.segs.into_iter().filter(|(_, l)| *l > 0).collect();
                    let summarizer: Arc<dyn Summarizer> = Arc::new(CountSummarizer);
                    let state = TreeState { root, summary, min_seg, summarizer, attached: false, due: sp.due, seg_live };
                    env.trees.insert(t.id, state);
                }
                if !recorded {
                    for id in env.tree_ids() {
                        if let Some(NodePtr::Disk(loc)) = env.trees[&id].root {
                            let live = env.disk_tree_live(loc)?;
                            env.trees.get_mut(&id).unwrap().seg_live = live;
                        }
                    }
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
        let seg_live = BTreeMap::new();
        let state = TreeState { root: None, summary, min_seg: u32::MAX, summarizer, attached: true, due: Gsn::MAX, seg_live };
        self.trees.insert(id, state);
        id
    }

    /// Live bytes per segment of the on-disk tree rooted at `root`, from
    /// its internal nodes: the leaves' sizes are in their parents' pointers.
    fn disk_tree_live(&mut self, root: DiskLoc) -> Result<BTreeMap<u32, u64>> {
        // every leaf is at the same depth: the depth of the leftmost one
        let mut height = 0;
        let mut loc = root;
        while let Node::Internal(ch) = Node::decode(&self.segs.read(loc)?)? {
            let Some(NodePtr::Disk(first)) = ch.first().map(|c| c.ptr) else { break };
            height += 1;
            loc = first;
        }
        let mut live = BTreeMap::new();
        self.add_disk_live(root, height, &mut live)?;
        Ok(live)
    }

    fn add_disk_live(&mut self, loc: DiskLoc, height: usize, live: &mut BTreeMap<u32, u64>) -> Result<()> {
        *live.entry(loc.seg).or_default() += loc.len as u64;
        if height > 0 {
            let Node::Internal(ch) = Node::decode(&self.segs.read(loc)?)? else {
                return Err(SqlError::corrupt(format!("leaf above the leaf level at {loc:?}")));
            };
            for c in ch {
                if let NodePtr::Disk(l) = c.ptr {
                    self.add_disk_live(l, height - 1, live)?;
                }
            }
        }
        Ok(())
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
        let slot = Slot {
            node: Some(node),
            disk,
            dirty: disk.is_none(),
            edits: 0,
            parent,
            tree,
            loaded_children: 0,
            refbit: true,
            bytes,
            by_maint: false,
        };
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
            self.dec_live(slot.tree, loc);
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

    /// A frame of `tree` written at `loc`.
    fn add_live(&mut self, tree: TreeId, loc: DiskLoc) {
        let e = self.seg_live.entry(loc.seg).or_insert((0, 0));
        e.0 += loc.len as u64;
        e.1 += loc.len as u64;
        if let Some(t) = self.trees.get_mut(&tree) {
            *t.seg_live.entry(loc.seg).or_default() += loc.len as u64;
        }
    }

    /// The frame of `tree` at `loc` is no longer referenced.
    fn dec_live(&mut self, tree: TreeId, loc: DiskLoc) {
        if let Some(e) = self.seg_live.get_mut(&loc.seg) {
            e.1 = e.1.saturating_sub(loc.len as u64);
        }
        if let Some(t) = self.trees.get_mut(&tree)
            && let Some(l) = t.seg_live.get_mut(&loc.seg)
        {
            *l = l.saturating_sub(loc.len as u64);
            if *l == 0 {
                t.seg_live.remove(&loc.seg);
            }
        }
    }

    fn make_dirty(&mut self, id: u32) {
        let slot = self.slot_mut(id);
        if !slot.dirty {
            slot.dirty = true;
            let (bytes, disk, tree) = (slot.bytes, slot.disk.take(), slot.tree);
            self.dirty_bytes += bytes;
            if let Some(loc) = disk {
                self.dec_live(tree, loc);
            }
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
        let root = match self.root_slot(tree)? {
            Some(id) => id,
            None => self.alloc_slot(Node::Leaf(Vec::new()), None, NONE, tree),
        };
        // the tree's summary is replaced below
        let old = std::mem::take(&mut self.trees.get_mut(&tree).unwrap().summary);
        let parts = self.apply_node(root, updates, gsn, &summ, Some(&old));
        updates.clear();
        let parts = match parts {
            Ok(p) => p,
            Err(e) => {
                self.trees.get_mut(&tree).unwrap().summary = old;
                return Err(e);
            }
        };
        self.install_root(tree, parts, &summ)?;
        self.maybe_evict();
        Ok(())
    }

    /// Make the nodes now covering a tree's key range (from an update of its
    /// root) its root, with their summary as the tree's.
    fn install_root(&mut self, tree: TreeId, mut parts: Parts, summ: &Arc<dyn Summarizer>) -> Result<()> {
        let top = match parts.len() {
            0 => None,
            1 => parts.pop(),
            _ => Some(self.build_root(parts, tree, summ)?),
        };
        let (new_root, summary, min_seg, due) = match top {
            None => (None, Summary::empty(summ.spec()), u32::MAX, Gsn::MAX),
            Some(p) => {
                // a single-child root covers the same entries as its child
                let (r, due) = self.collapse_root(p.id, p.due)?;
                let min_seg = if r == p.id { p.min_seg } else { self.node_min_seg(r) };
                (Some(r), p.summary, min_seg, due)
            }
        };
        let t = self.trees.get_mut(&tree).unwrap();
        t.root = new_root.map(NodePtr::Mem);
        t.summary = summary;
        t.min_seg = min_seg;
        t.due = due;
        Ok(())
    }

    /// Replace a single-child internal root (whose `due` is `due`) by its
    /// child; returns the new root and its `due`.
    fn collapse_root(&mut self, mut root: u32, mut due: Gsn) -> Result<(u32, Gsn)> {
        loop {
            let single = match self.node(root) {
                Node::Internal(ch) if ch.len() == 1 => Some(ch[0].due),
                _ => None,
            };
            let Some(child_due) = single else { return Ok((root, due)) };
            due = child_due;
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
        // its write is the update's now
        self.slot_mut(id).by_maint = false;
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
                    return Ok(smallvec::smallvec![Part { sep: Box::default(), id, summary, min_seg: u32::MAX, due: 0 }]);
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
                    // dirty leaves have no location of their own yet, and
                    // their compaction state is found by the maintenance
                    // pass or the checkpoint
                    parts.push(Part { sep: first, id: pid, summary, min_seg: u32::MAX, due: 0 });
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
                // children the update made smaller (merged with a neighbour
                // below if that leaves them underfull)
                let mut shrunk: smallvec::SmallVec<[u32; 4]> = smallvec::SmallVec::new();
                // right to left, so splicing keeps the remaining indexes valid
                for &(ci, start, end) in runs.iter().rev() {
                    let cid = self.load(children[ci].ptr, id, tree)?;
                    let before = self.slot(cid).bytes;
                    let parts = self.apply_node(cid, &ups[start..end], gsn, summ, Some(&children[ci].summary))?;
                    if let [p] = &parts[..]
                        && self.slot(p.id).bytes < before
                    {
                        shrunk.push(p.id);
                    }
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
                if !shrunk.is_empty() {
                    let mut unlimited = u64::MAX;
                    self.merge_underfull(id, &mut children, &shrunk, &mut size, &mut unlimited, false)?;
                }
                self.finish_internal_sized(id, children, incr, summ, size)
            }
        }
    }

    /// Merge the children of internal node `id` listed in `shrunk` (just
    /// made smaller) that are below a quarter of their target size with a
    /// neighbour, while the two fit in three quarters of it (so that the
    /// result does not split again soon). Loading a neighbour and rewriting
    /// a clean one take their bytes from `budget`; no merge is tried once it
    /// is exhausted. `maint`: on behalf of the maintenance pass. The
    /// summary of `id` is unchanged; `size` is kept up to date. Returns the
    /// number of merges.
    fn merge_underfull(
        &mut self,
        id: u32,
        children: &mut Vec<Child>,
        shrunk: &[u32],
        size: &mut usize,
        budget: &mut u64,
        maint: bool,
    ) -> Result<usize> {
        let tree = self.slot(id).tree;
        let mut todo: smallvec::SmallVec<[u32; 8]> = shrunk.into();
        let mut merges = 0;
        let mut i = 0;
        while i < children.len() && children.len() > 1 {
            let NodePtr::Mem(cid) = children[i].ptr else {
                i += 1;
                continue;
            };
            let target = if self.node(cid).is_leaf() { self.opts.leaf_bytes } else { self.opts.internal_bytes };
            if !todo.contains(&cid) || self.slot(cid).bytes >= target / 4 {
                i += 1;
                continue;
            }
            // the right neighbour, else the left one
            let mut merged = None;
            for j in [i + 1, i.wrapping_sub(1)] {
                if j >= children.len() || *budget == 0 {
                    continue;
                }
                let nid = match children[j].ptr {
                    NodePtr::Mem(n) => n,
                    ptr @ NodePtr::Disk(loc) => {
                        *budget = budget.saturating_sub(loc.len as u64);
                        let n = self.load(ptr, id, tree)?;
                        children[j].ptr = NodePtr::Mem(n);
                        n
                    }
                };
                debug_assert_eq!(self.node(cid).is_leaf(), self.node(nid).is_leaf(), "siblings at different depths");
                if self.slot(cid).bytes + self.slot(nid).bytes > target * 3 / 4 {
                    continue;
                }
                if !self.slot(nid).dirty {
                    *budget = budget.saturating_sub(self.slot(nid).bytes as u64);
                    self.slot_mut(nid).by_maint = maint;
                }
                let l = i.min(j);
                merged = Some((l, self.merge_pair(children, l, size)));
                break;
            }
            match merged {
                Some((l, keep)) => {
                    merges += 1;
                    // the result may still be underfull
                    todo.push(keep);
                    i = l;
                }
                None => i += 1,
            }
        }
        Ok(merges)
    }

    /// Merge loaded child `l + 1` of an internal node (whose children are
    /// `children` and size `size`) into loaded child `l`; returns the slot
    /// of the merged node.
    fn merge_pair(&mut self, children: &mut Vec<Child>, l: usize, size: &mut usize) -> u32 {
        let (NodePtr::Mem(a), NodePtr::Mem(b)) = (children[l].ptr, children[l + 1].ptr) else {
            unreachable!("merging a child that is not loaded")
        };
        self.make_dirty(a);
        self.make_dirty(b);
        let right = children.remove(l + 1);
        *size -= node::child_size(&right);
        let nb = self.slot_mut(b).node.take().expect("node taken");
        let na = self.slot_mut(a).node.take().expect("node taken");
        let c = &mut children[l];
        c.summary.combine(&right.summary);
        let merged = match (na, nb) {
            (Node::Leaf(mut x), Node::Leaf(y)) => {
                x.extend(y);
                self.slot_mut(a).edits = 0;
                (c.min_seg, c.due) = (u32::MAX, 0);
                Node::Leaf(x)
            }
            (Node::Internal(mut x), Node::Internal(mut y)) => {
                // the right node's first child starts at the right node's separator
                y[0].sep = right.sep;
                for g in &y {
                    if let NodePtr::Mem(gid) = g.ptr {
                        self.slot_mut(gid).parent = a;
                    }
                }
                let moved = std::mem::take(&mut self.slot_mut(b).loaded_children);
                self.slot_mut(a).loaded_children += moved;
                x.extend(y);
                c.min_seg = x.iter().map(|g| g.min_seg).fold(u32::MAX, u32::min);
                c.due = x.iter().map(|g| g.due).min().unwrap_or(Gsn::MAX);
                Node::Internal(x)
            }
            _ => unreachable!("siblings at different depths"),
        };
        self.put_node(a, merged);
        self.put_node(b, Node::Leaf(Vec::new()));
        self.free_slot(b);
        a
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
        let due = children.iter().map(|c| c.due).min().unwrap_or(Gsn::MAX);
        self.put_node_sized(id, Node::Internal(children), size);
        // the first part keeps the node's place (and separator) in the parent
        Ok(smallvec::smallvec![Part { sep: Box::default(), id, summary: s, min_seg, due }])
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
            let due = chunk.iter().map(|c| c.due).min().unwrap_or(Gsn::MAX);
            self.slot_mut(target).loaded_children = loaded;
            self.put_node(target, Node::Internal(chunk));
            parts.push(Part { sep: first, id: target, summary: s, min_seg, due });
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
    // checkpoint, drop

    /// Make all trees durable as of `gsn` together with the application state.
    pub fn checkpoint(&mut self, gsn: Gsn, app: &[u8]) -> Result<()> {
        let tree_ids = self.tree_ids();
        let mut records = Vec::with_capacity(tree_ids.len());
        let mut space = Vec::with_capacity(tree_ids.len());
        for tid in tree_ids {
            let t = &self.trees[&tid];
            let rec = match t.root {
                None => TreeRecord { id: tid, root: None },
                Some(NodePtr::Disk(loc)) => TreeRecord { id: tid, root: Some((loc, t.summary.clone(), t.min_seg)) },
                Some(NodePtr::Mem(id)) => {
                    let was_dirty = self.slot(id).dirty;
                    let (loc, min_seg, due) = self.flush_node(id)?;
                    let t = self.trees.get_mut(&tid).unwrap();
                    if was_dirty {
                        t.min_seg = min_seg;
                        t.due = due;
                    }
                    TreeRecord { id: tid, root: Some((loc, t.summary.clone(), t.min_seg)) }
                }
            };
            records.push(rec);
            let t = &self.trees[&tid];
            space.push(TreeSpace { due: t.due, segs: t.seg_live.iter().map(|(s, l)| (*s, *l)).collect() });
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
            space: if self.legacy_format { None } else { Some(space) },
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

    /// Write a dirty node (and its dirty descendants first). Returns its
    /// location, min_seg and due; for a clean node, only the location is
    /// meaningful (its parent keeps the values it has).
    fn flush_node(&mut self, id: u32) -> Result<(DiskLoc, u32, Gsn)> {
        if !self.slot(id).dirty {
            let loc = self.slot(id).disk.unwrap();
            return Ok((loc, loc.seg, 0));
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
                        let (loc, ms, due) = self.flush_node(cid)?;
                        if was_dirty {
                            c.min_seg = ms;
                            c.due = due;
                        }
                        locs.push(loc);
                    }
                }
            }
        }
        let mut buf = Vec::with_capacity(node.byte_size() + 16);
        node.encode_as(&locs, &mut buf, self.legacy_format);
        let loc = self.segs.append(&buf)?;
        let (tree, by_maint) = (self.slot(id).tree, self.slot(id).by_maint);
        self.add_live(tree, loc);
        if !by_maint {
            self.fg_appended += loc.len as u64;
        }
        let (min_seg, due) = match &node {
            Node::Leaf(entries) => (loc.seg, leaf_due(entries)),
            Node::Internal(ch) => (
                ch.iter().map(|c| c.min_seg).min().unwrap_or(u32::MAX).min(loc.seg),
                ch.iter().map(|c| c.due).min().unwrap_or(Gsn::MAX),
            ),
        };
        let slot = self.slot_mut(id);
        slot.node = Some(node);
        slot.disk = Some(loc);
        slot.by_maint = false;
        if slot.dirty {
            slot.dirty = false;
            let bytes = slot.bytes;
            self.dirty_bytes -= bytes;
        }
        Ok((loc, min_seg, due))
    }

    /// Drop a tree, releasing its nodes. Nothing is read: the nodes in
    /// memory are freed, and the tree's live bytes per segment released.
    pub fn drop_tree(&mut self, tree: TreeId) -> Result<()> {
        let Some(t) = self.trees.get(&tree) else { return Ok(()) };
        if let Some(NodePtr::Mem(id)) = t.root {
            self.free_loaded(id);
        }
        let t = self.trees.remove(&tree).unwrap();
        // what is left is on disk only
        for (seg, live) in t.seg_live {
            if let Some(e) = self.seg_live.get_mut(&seg) {
                e.1 = e.1.saturating_sub(live);
            }
        }
        Ok(())
    }

    /// Free a loaded node and its loaded descendants.
    fn free_loaded(&mut self, id: u32) {
        let loaded: Vec<u32> = match self.node(id) {
            Node::Internal(ch) => ch
                .iter()
                .filter_map(|c| match c.ptr {
                    NodePtr::Mem(cid) => Some(cid),
                    NodePtr::Disk(_) => None,
                })
                .collect(),
            Node::Leaf(_) => vec![],
        };
        for c in loaded {
            self.free_loaded(c);
        }
        let slot = self.slot_mut(id);
        slot.loaded_children = 0;
        slot.parent = NONE;
        self.free_slot(id);
    }

    // ------------------------------------------------------------------
    // maintenance: compaction, merging and segment GC

    /// One maintenance pass with the default budget: for compaction and
    /// for GC each, `MAINT_MIN_BYTES` plus the bytes checkpoints wrote for
    /// the engine's own updates since the previous pass (garbage arises at
    /// that rate, so reclaiming keeps pace with it). Run before a
    /// checkpoint, which then writes the nodes it rewrote.
    pub fn maintain(&mut self) -> Result<MaintStats> {
        let written = std::mem::take(&mut self.fg_appended);
        let b = MAINT_MIN_BYTES.saturating_add(written);
        self.maintain_with(MaintBudget { compact: b, gc: b })
    }

    /// A maintenance pass: rewrite the leaves whose compaction is due (and
    /// compact every dirty leaf, which the next checkpoint writes anyway),
    /// merge what that leaves underfull, and rewrite the live nodes of the
    /// segments chosen for evacuation (compacting the leaves among them).
    /// Only subtrees whose `due` has passed the retention frontier, or that
    /// hold nodes of a chosen segment, are visited; reading and rewriting
    /// clean nodes stops when their budget is exhausted (the next pass
    /// continues). No summary changes: compaction keeps every entry's
    /// current weight, and merging keeps the entries.
    pub fn maintain_with(&mut self, budget: MaintBudget) -> Result<MaintStats> {
        let victims = self.gc_victims(budget.gc);
        let mut m = Maint {
            frontier: self.frontier,
            stats: MaintStats { victims: victims.clone(), ..Default::default() },
            victims,
            compact: budget.compact,
            gc: budget.gc,
            leaf_depth: None,
        };
        for tid in self.tree_ids() {
            let t = &self.trees[&tid];
            let Some(root) = t.root else { continue };
            // (trees not attached after recovery are about to be dropped)
            if !t.attached {
                continue;
            }
            let dirty = matches!(root, NodePtr::Mem(id) if self.slot(id).dirty);
            let gc = t.seg_live.keys().any(|s| m.victim(*s));
            let compact = t.due <= m.frontier;
            if !dirty && !gc && !compact {
                continue;
            }
            if let NodePtr::Disk(loc) = root
                && !m.charge(gc, loc.len as u64)
            {
                continue;
            }
            let (old, due, summ) = (t.summary.clone(), t.due, t.summarizer.clone());
            let rid = self.root_slot(tid)?.expect("tree with a root");
            m.leaf_depth = None;
            match self.maint_node(rid, 0, &old, due, &mut m, &summ)? {
                Visit::Same => {}
                Visit::Due(d) => self.trees.get_mut(&tid).unwrap().due = d,
                Visit::New(parts) => self.install_root(tid, parts, &summ)?,
            }
        }
        self.maybe_evict();
        Ok(m.stats)
    }

    /// Sealed segments to evacuate: those less than half live, in order of
    /// the cost-benefit of cleaning them — free space gained per byte moved
    /// (1 − u) / (1 + u), weighted by age (cold data stays put once moved,
    /// so old segments are worth more) — while their live bytes fit in
    /// `budget` (the first one always does, unless `budget` is 0).
    fn gc_victims(&self, budget: u64) -> Vec<u32> {
        if budget == 0 {
            return Vec::new();
        }
        let active = self.segs.active_id();
        let mut cands: Vec<(f64, u32, u64)> = self
            .seg_live
            .iter()
            .filter(|(id, (total, live))| **id != active && *live > 0 && live * 2 < *total)
            .map(|(id, (total, live))| {
                let u = *live as f64 / *total as f64;
                let age = active.saturating_sub(*id).max(1) as f64;
                ((1.0 - u) * age / (1.0 + u), *id, *live)
            })
            .collect();
        cands.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
        let (mut out, mut sum) = (Vec::new(), 0u64);
        for (_, id, live) in cands {
            if !out.is_empty() && sum.saturating_add(live) > budget {
                continue;
            }
            out.push(id);
            sum += live;
        }
        out.sort_unstable();
        out
    }

    /// Visit node `id` of a maintenance pass (`depth` below the root; `old`
    /// and `due` are its summary and `due` as recorded by its parent).
    fn maint_node(
        &mut self,
        id: u32,
        depth: usize,
        old: &Summary,
        due: Gsn,
        m: &mut Maint,
        summ: &Arc<dyn Summarizer>,
    ) -> Result<Visit> {
        let dirty = self.slot(id).dirty;
        let bytes = self.slot(id).bytes as u64;
        let in_victim = self.slot(id).disk.is_some_and(|l| m.victim(l.seg));
        if let Node::Leaf(entries) = self.node(id) {
            m.leaf_depth = Some(depth);
            let (first, now_due) = leaf_compaction(entries);
            // a dirty leaf is written by the next checkpoint anyway
            let compact = if dirty { first <= m.frontier } else { now_due <= m.frontier };
            let unchanged = if now_due == due { Visit::Same } else { Visit::Due(now_due) };
            if !compact && !in_victim {
                return Ok(unchanged);
            }
            if !dirty {
                if !m.charge(in_victim, bytes) {
                    return Ok(unchanged);
                }
                if in_victim {
                    m.stats.relocated += 1;
                } else {
                    m.stats.compacted += 1;
                }
                self.make_dirty(id);
                self.slot_mut(id).by_maint = true;
            }
            self.slot_mut(id).edits = 0;
            let Some(Node::Leaf(mut entries)) = self.slot_mut(id).node.take() else { unreachable!() };
            let f = m.frontier;
            entries.retain_mut(|e| {
                e.hist.compact(f);
                !e.hist.is_empty()
            });
            if entries.is_empty() {
                self.put_node(id, Node::Leaf(entries));
                self.free_slot(id);
                return Ok(Visit::New(Parts::new()));
            }
            let due = leaf_due(&entries);
            self.put_node(id, Node::Leaf(entries));
            return Ok(Visit::New(smallvec::smallvec![Part { sep: Box::default(), id, summary: old.clone(), min_seg: u32::MAX, due }]));
        }
        let relocate = in_victim && m.charge(true, bytes);
        let tree = self.slot(id).tree;
        let Some(Node::Internal(mut children)) = self.slot_mut(id).node.take() else { unreachable!() };
        // a placeholder, so that loading children counts them on this slot
        self.put_node(id, Node::Internal(Vec::new()));
        let mut size = bytes as usize;
        let mut changed = relocate;
        let mut shrunk: smallvec::SmallVec<[u32; 4]> = smallvec::SmallVec::new();
        let mut i = 0;
        while i < children.len() {
            let c = &children[i];
            // the child's own segment, and whether it is known to be a leaf
            // (every leaf is at the depth of the first one seen)
            let (own, leaf) = match c.ptr {
                NodePtr::Mem(cid) => (self.slot(cid).disk.map(|l| l.seg), Some(self.node(cid).is_leaf())),
                NodePtr::Disk(loc) => (Some(loc.seg), m.leaf_depth.map(|d| d == depth + 1)),
            };
            let gc_self = own.is_some_and(|s| m.victim(s));
            let gc_below = leaf != Some(true) && m.victims.last().is_some_and(|v| c.min_seg <= *v);
            let gc = gc_self || gc_below;
            if !gc && c.due > m.frontier {
                i += 1;
                continue;
            }
            if let NodePtr::Disk(loc) = c.ptr
                && !m.charge(gc, loc.len as u64)
            {
                i += 1;
                continue;
            }
            let cid = self.load(c.ptr, id, tree)?;
            children[i].ptr = NodePtr::Mem(cid);
            let before = self.slot(cid).bytes;
            match self.maint_node(cid, depth + 1, &children[i].summary, children[i].due, m, summ)? {
                Visit::Same => i += 1,
                Visit::Due(d) => {
                    children[i].due = d;
                    changed = true;
                    i += 1;
                }
                Visit::New(parts) => {
                    changed = true;
                    if let [p] = &parts[..]
                        && self.slot(p.id).bytes < before
                    {
                        shrunk.push(p.id);
                    }
                    // the first part keeps the child's separator
                    size -= node::child_size(&children[i]);
                    let n = parts.len();
                    let mut sep = Some(std::mem::take(&mut children[i].sep));
                    children.splice(
                        i..i + 1,
                        parts.into_iter().map(|mut p| {
                            if let Some(s) = sep.take() {
                                p.sep = s;
                            }
                            p.into_child()
                        }),
                    );
                    size += children[i..i + n].iter().map(node::child_size).sum::<usize>();
                    i += n;
                }
            }
        }
        if !shrunk.is_empty() {
            m.stats.merged += self.merge_underfull(id, &mut children, &shrunk, &mut size, &mut m.compact, true)?;
        }
        if !changed {
            self.put_node_sized(id, Node::Internal(children), size);
            return Ok(Visit::Same);
        }
        if relocate {
            m.stats.relocated += 1;
        }
        if !dirty {
            // the path to a rewritten node is rewritten too
            self.make_dirty(id);
            self.slot_mut(id).by_maint = true;
        }
        if children.is_empty() {
            self.slot_mut(id).loaded_children = 0;
            self.free_slot(id);
            return Ok(Visit::New(Parts::new()));
        }
        Ok(Visit::New(self.finish_internal_sized(id, children, Some(old.clone()), summ, size)?))
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
        s.bytes_read = self.segs.bytes_read;
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

/// Bytes of node frames a maintenance pass may read and rewrite for
/// compaction, and as many for GC, besides the share proportional to the
/// engine's writes (see `Env::maintain`).
const MAINT_MIN_BYTES: u64 = 8 << 20;

/// Work a maintenance pass may do: bytes of clean node frames it may read
/// or rewrite (the next checkpoint writes them).
#[derive(Clone, Copy, Debug)]
pub struct MaintBudget {
    /// For leaves whose compaction is due, and for merges.
    pub compact: u64,
    /// For evacuating sparse segments (it also bounds the live bytes of
    /// the segments chosen).
    pub gc: u64,
}

impl MaintBudget {
    pub const UNLIMITED: MaintBudget = MaintBudget { compact: u64::MAX, gc: u64::MAX };
}

/// What a maintenance pass did.
#[derive(Clone, Debug, Default)]
pub struct MaintStats {
    /// Clean leaves rewritten to compact their histories.
    pub compacted: usize,
    /// Clean nodes rewritten because their segment is being evacuated.
    pub relocated: usize,
    /// Merges of underfull neighbours.
    pub merged: usize,
    /// Segments chosen for evacuation.
    pub victims: Vec<u32>,
}

/// State of a maintenance pass.
struct Maint {
    frontier: Gsn,
    /// Segments being evacuated, sorted.
    victims: Vec<u32>,
    /// Remaining budgets.
    compact: u64,
    gc: u64,
    /// Depth of the leaves of the tree being visited, once one was seen.
    leaf_depth: Option<usize>,
    stats: MaintStats,
}

impl Maint {
    fn victim(&self, seg: u32) -> bool {
        self.victims.binary_search(&seg).is_ok()
    }

    /// Take `bytes` from the GC or the compaction budget; false (taking
    /// nothing) once it is exhausted.
    fn charge(&mut self, gc: bool, bytes: u64) -> bool {
        let b = if gc { &mut self.gc } else { &mut self.compact };
        if *b == 0 {
            return false;
        }
        *b = b.saturating_sub(bytes);
        true
    }
}

/// Outcome of visiting a node in a maintenance pass.
enum Visit {
    /// Nothing changed.
    Same,
    /// The node is unchanged, but its `due` is not what its parent recorded.
    Due(Gsn),
    /// The nodes now covering its key range: itself rewritten, or none.
    New(Parts),
}

/// Shortest key `s` with `a < s <= b` (for `a < b`): B+tree suffix truncation.
/// The nodes an update leaves in place of one (usually just that one).
type Parts = smallvec::SmallVec<[Part; 1]>;

/// A node produced by an update: its separator, slot, summary, min_seg and due.
struct Part {
    sep: Box<[u8]>,
    id: u32,
    summary: Summary,
    min_seg: u32,
    due: Gsn,
}

impl Part {
    fn into_child(self) -> Child {
        Child { sep: self.sep, ptr: NodePtr::Mem(self.id), summary: self.summary, min_seg: self.min_seg, due: self.due }
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
