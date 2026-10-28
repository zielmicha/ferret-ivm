//! Storage: an ordered index keyed by byte strings (memcomparable (key,
//! value) encodings) holding a weight per entry, augmented with monoid
//! summaries per subtree. It keeps the current weights only: a change that
//! makes an entry's weight 0 removes the entry.
//!
//! The tree is copy-on-write. Nodes live in a node arena that acts as the
//! buffer pool, accounted in bytes of memory: clean nodes are evicted under
//! memory pressure (CLOCK), also between the leaves of a scan, and dirty
//! nodes are written to the active append-only segment by the checkpoint
//! or, when they exceed their share of the pool, early by the pool itself
//! (a written node is clean and can be evicted; nothing refers to it
//! durably until a checkpoint does). A checkpoint is: write the remaining
//! dirty nodes bottom-up, fsync, append a manifest record naming the new
//! roots. Sealed segments are immutable. A budgeted maintenance pass before
//! each checkpoint reclaims space: it relocates the live nodes of sparse
//! segments (found through per-subtree `min_seg` augmentation), which are
//! deleted once nothing references them, merging the underfull ones with
//! a neighbour.
//!
//! Loaded nodes are shared (`Arc`) and copied on write when shared: a fork
//! of the environment ([`Env::fork`]) is an independent version of every
//! tree as of the moment it was taken, readable and writable on another
//! thread, and a checkpoint is written from such a version of the dirty
//! nodes ([`Env::begin_checkpoint`]) while the environment goes on.

mod inspect;
pub mod manifest;
pub mod node;
pub mod segment;
pub mod sort;
pub mod summary;

pub use inspect::TreeResidency;
pub use sort::TreeBuilder;
pub use summary::{AggKind, AggState, FloatSum, Summary};

use crate::error::{Result, SqlError};
use manifest::{CheckpointRecord, Manifest, TreeRecord, TreeSpace};
use node::{child_heap, entry_heap, entry_size, Child, DiskLoc, LeafEntry, Node, NodePtr};
use rustc_hash::FxHashMap;
use segment::SegmentStore;
use std::collections::{BTreeMap, VecDeque};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Global sequence number: the engine's single clock.
pub type Gsn = u64;

/// Error code of a read that visited more entries than its budget
/// (`Env::set_read_budget`; never sent to a client).
pub const READ_BUDGET: &str = "IVMRB";
pub type TreeId = u64;

/// An entry `Env::nth` finds: its key, its weight and the offset of the
/// unit of weight sought within it.
pub type NthEntry = (Box<[u8]>, i64, i64);

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
    /// Target of the buffer pool: bytes of memory of the loaded nodes
    /// (`Node::mem_size`), beyond which clean nodes are evicted. The engine
    /// adjusts it to the pool's share of the memory budget
    /// (`Env::set_cache_bytes`).
    pub cache_bytes: usize,
    pub leaf_bytes: usize,
    pub internal_bytes: usize,
    pub segment_bytes: u64,
    /// Segment files kept open for reading at most, by the environment and
    /// its forks and checkpoint jobs together (others are opened again when
    /// read); None: a quarter of the process's limit on open files
    /// (`segment::default_max_open`).
    pub max_open_segments: Option<usize>,
}

impl Default for EnvOptions {
    fn default() -> Self {
        EnvOptions { cache_bytes: 1 << 30, leaf_bytes: 16 << 10, internal_bytes: 16 << 10, segment_bytes: 256 << 20, max_open_segments: None }
    }
}

const NONE: u32 = u32::MAX;

/// Ids of temporary trees (`Env::create_temp_tree`) count down from the top
/// of the id range to this; those of `create_tree` count up from 1.
pub const TEMP_TREES: TreeId = 1 << 63;

/// Memory of an arena slot itself, counted with its node.
const SLOT_BYTES: usize = std::mem::size_of::<Option<Slot>>();

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
    /// In-place edits of a leaf since its entries were last rebuilt into a
    /// vector of their size (in-place inserts leave the vector room to
    /// grow, which rebuilding every few edits returns).
    edits: u32,
    parent: u32,
    tree: TreeId,
    loaded_children: u32,
    refbit: bool,
    /// The node's estimated serialized size (split decisions).
    bytes: usize,
    /// Dirtied by the maintenance pass and not modified since (its write
    /// does not count towards the next pass's budget).
    by_maint: bool,
    /// The node's memory (`Node::mem_size`) plus the slot's: what the
    /// buffer pool's accounting counts.
    mem: usize,
    /// In a fork: the node is still the one the environment it was forked
    /// from holds (`Env::fork`), not the fork's own.
    inherited: bool,
}

#[derive(Clone)]
struct TreeState {
    root: Option<NodePtr>,
    summary: Summary,
    min_seg: u32,
    summarizer: Arc<dyn Summarizer>,
    /// False for trees recovered from the manifest but not yet attached.
    attached: bool,
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
    /// Slots visited by eviction sweeps so far (their cost).
    pub swept_slots: u64,
}

pub struct Env {
    #[allow(dead_code)]
    dir: PathBuf,
    opts: EnvOptions,
    segs: SegmentStore,
    slots: Vec<Option<Slot>>,
    free_slots: Vec<u32>,
    /// Memory of the loaded nodes (`Slot::mem`).
    mem_bytes: usize,
    /// Memory of the dirty nodes (maintained with the dirty flags).
    dirty_bytes: usize,
    clock_hand: usize,
    /// (mem_bytes, dirty_bytes) when a sweep last found nothing more to
    /// evict or write: sweeps resume once either has grown well past it.
    evict_stall: Option<(usize, usize)>,
    swept_slots: u64,
    /// The transient working memory that sort runs and spilled deltas are
    /// sized from (`set_work`).
    work: crate::memory::Work,
    trees: FxHashMap<TreeId, TreeState>,
    next_tree_id: TreeId,
    /// segment id -> (total bytes written, live bytes referenced)
    seg_live: BTreeMap<u32, (u64, u64)>,
    checkpoint_seq: u64,
    checkpoint_gsn: Gsn,
    /// Bytes written (by checkpoints or early) for nodes the engine (not
    /// the maintenance pass) modified, since the last pass.
    fg_appended: u64,
    /// Write nodes in the formats of versions that kept history and
    /// checkpoint records without tree space (tests of stores written by
    /// earlier versions).
    legacy_format: bool,
    /// The manifest (None in forks, and while a checkpoint job holds it).
    manifest: Option<Manifest>,
    /// Source of `Slot::version` stamps.
    stamp: u64,
    /// Nodes written by `write_node` (in checkpoint jobs: what the parent
    /// environment marks clean when the job is done).
    flushed: Option<Vec<Flushed>>,
    /// Node frames, and their bytes, read from segments.
    reads: u64,
    read_bytes: u64,
    /// Entries scans may still visit before failing with `READ_BUDGET`.
    read_budget: Option<u64>,
    /// While a checkpoint job is in progress: the stamp when it began.
    /// Dirty nodes stamped no later are frozen in the job, which writes
    /// them; the pool does not write them early.
    frozen_at: Option<u64>,
    /// In a fork: memory (and dirty memory) of the inherited nodes, shared
    /// with the environment it was forked from, which its buffer pool
    /// does not count against its target.
    inherited: (usize, usize),
    /// The share of a memory budget this environment's pool takes while it
    /// lives (`hold`; a fork's).
    held: crate::memory::Charge,
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
/// dirty nodes with the manifest (the append side is shared).
pub struct CheckpointJob {
    env: Env,
    gsn: Gsn,
}

/// A written checkpoint, for `Env::finish_checkpoint`.
pub struct CheckpointDone {
    manifest: Manifest,
    flushed: Vec<Flushed>,
    /// Bytes the job wrote for nodes the engine modified (`fg_appended`).
    fg_appended: u64,
    /// Sealed segments without live bytes in the committed checkpoint.
    dead: Vec<u32>,
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
        let dead = self.env.write_checkpoint(self.gsn, app)?;
        let env = &mut self.env;
        Ok(CheckpointDone {
            manifest: env.manifest.take().expect("checkpoint job without manifest"),
            flushed: env.flushed.take().unwrap_or_default(),
            fg_appended: env.fg_appended,
            dead,
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
    buf: VecDeque<(Box<[u8]>, i64)>,
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

/// Children overlapping [lo, hi), with whether each is fully inside, of a
/// node whose keys lie in the range `node` (`None`: unbounded or unknown,
/// so that its first or last child counts as partial). Only the first and
/// last can be partial; children are found by binary search.
fn overlapping(children: &[Child], lo: Bound<&[u8]>, hi: Bound<&[u8]>, node: KeyRange) -> Vec<(usize, bool)> {
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
            let clo = if i == 0 { node.0 } else { Some(&*children[i].sep) };
            let chi = if i + 1 < n { Some(&*children[i + 1].sep) } else { node.1 };
            if disjoint(lo, hi, clo, chi) {
                continue;
            }
            contains(lo, hi, clo, chi)
        };
        out.push((i, full));
    }
    out
}

/// A key range [lo, hi) of a subtree; `None`: unbounded or unknown.
type KeyRange<'a> = (Option<&'a [u8]>, Option<&'a [u8]>);
type OwnedKeyRange = (Option<Box<[u8]>>, Option<Box<[u8]>>);

/// The key range of child `i` of a node whose range is `node`, owned (to be
/// passed down while the node is borrowed again).
fn child_range(children: &[Child], i: usize, node: KeyRange) -> OwnedKeyRange {
    let lo = if i == 0 { node.0 } else { Some(&*children[i].sep) };
    let hi = if i + 1 < children.len() { Some(&*children[i + 1].sep) } else { node.1 };
    (lo.map(Into::into), hi.map(Into::into))
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
        let max_open = opts.max_open_segments.unwrap_or_else(segment::default_max_open);
        let segs = SegmentStore::open(dir, active_seg, active_len, opts.segment_bytes, max_open)?;
        let opts_cache = opts.cache_bytes;
        let mut env = Env {
            dir: dir.to_path_buf(),
            opts,
            segs,
            slots: Vec::new(),
            free_slots: Vec::new(),
            mem_bytes: 0,
            dirty_bytes: 0,
            clock_hand: 0,
            evict_stall: None,
            swept_slots: 0,
            work: crate::memory::Work::new(opts_cache / 8),
            trees: FxHashMap::default(),
            next_tree_id: 1,
            seg_live: BTreeMap::new(),
            checkpoint_seq: 0,
            checkpoint_gsn: 0,
            fg_appended: 0,
            legacy_format: false,
            manifest: Some(manifest),
            stamp: 0,
            flushed: None,
            reads: 0,
            read_bytes: 0,
            read_budget: None,
            frozen_at: None,
            inherited: (0, 0),
            held: crate::memory::Charge::none(),
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
                    // the tree's live bytes are counted below
                    let sp = space.next().unwrap_or_default();
                    let seg_live = sp.segs.into_iter().filter(|(_, l)| *l > 0).collect();
                    let summarizer: Arc<dyn Summarizer> = Arc::new(CountSummarizer);
                    let state = TreeState { root, summary, min_seg, summarizer, attached: false, seg_live };
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
        // remove the sealed segment files without live nodes in the
        // checkpoint: those it does not mention (torn writes), and those
        // it does, dead, that were not deleted yet (no fork of the previous
        // run reads them any more)
        let active = env.segs.active_id();
        env.seg_live.retain(|id, (_, live)| *live > 0 || *id == active);
        for id in env.segs.list()? {
            if id != active && !env.seg_live.contains_key(&id) {
                env.segs.delete(id)?;
            }
        }
        env.seg_live.entry(active).or_insert((active_len, 0));
        Ok((env, recovered))
    }

    pub fn create_tree(&mut self, summarizer: Arc<dyn Summarizer>) -> TreeId {
        let id = self.next_tree_id;
        self.next_tree_id += 1;
        let summary = Summary::empty(summarizer.spec());
        let seg_live = BTreeMap::new();
        let state = TreeState { root: None, summary, min_seg: u32::MAX, summarizer, attached: true, seg_live };
        self.trees.insert(id, state);
        id
    }

    /// Live bytes per segment of the on-disk tree rooted at `root`, from
    /// its internal nodes: the leaves' sizes are in their parents' pointers.
    fn disk_tree_live(&mut self, root: DiskLoc) -> Result<BTreeMap<u32, u64>> {
        // every leaf is at the same depth: the depth of the leftmost one
        let mut height = 0;
        let mut loc = root;
        while let Node::Internal(ch) = Node::decode(&self.read_frame(loc)?)? {
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
            let Node::Internal(ch) = Node::decode(&self.read_frame(loc)?)? else {
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

    /// A tree for temporary contents (sort runs, spilled deltas), which its
    /// user drops. Its id is taken from the top of the id range downwards
    /// (at least `TEMP_TREES`), leaving the ids of `create_tree` a function
    /// of the operations that create trees. A checkpoint does not record it
    /// (it is no part of the state the checkpoint makes durable): its nodes
    /// are garbage in the checkpoint, and it does not exist after recovery.
    pub fn create_temp_tree(&mut self, summarizer: Arc<dyn Summarizer>) -> TreeId {
        let mut id = TreeId::MAX;
        while self.trees.contains_key(&id) {
            id -= 1;
        }
        debug_assert!(id >= TEMP_TREES, "temporary trees exhausted their ids");
        let summary = Summary::empty(summarizer.spec());
        let state = TreeState { root: None, summary, min_seg: u32::MAX, summarizer, attached: true, seg_live: BTreeMap::new() };
        self.trees.insert(id, state);
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

    pub fn checkpoint_gsn(&self) -> Gsn {
        self.checkpoint_gsn
    }

    /// Set the buffer pool's target (its share of the memory budget); the
    /// next access evicts down to it.
    pub fn set_cache_bytes(&mut self, bytes: usize) {
        self.opts.cache_bytes = bytes;
    }

    /// The transient working memory the users of this environment size
    /// their buffers from and charge them to (by default an eighth of the
    /// pool's initial target, of its own; an engine's is its memory
    /// budget's share).
    pub fn work(&self) -> &crate::memory::Work {
        &self.work
    }

    pub fn set_work(&mut self, work: crate::memory::Work) {
        self.work = work;
    }

    /// Give the buffer pool a target of `charge`'s bytes, which the charge
    /// holds in a memory budget for as long as this environment lives (a
    /// fork's pool, taken from the budget while it exists).
    pub fn hold(&mut self, charge: crate::memory::Charge) {
        self.opts.cache_bytes = charge.bytes();
        self.held = charge;
    }

    /// The stored summary of the whole tree (MIN/MAX values too wide for
    /// node summaries as prefixes; `range_summary` resolves them).
    pub fn tree_summary(&self, tree: TreeId) -> Summary {
        self.trees.get(&tree).map(|t| t.summary.clone()).unwrap_or_else(|| Summary::empty(&[]))
    }

    pub fn tree_count(&self, tree: TreeId) -> i64 {
        self.trees.get(&tree).map(|t| t.summary.count).unwrap_or(0)
    }

    /// Does the tree hold no entries? (Its count is the sum of the current
    /// weights, which may be zero while entries remain.)
    pub fn tree_is_empty(&self, tree: TreeId) -> bool {
        self.trees.get(&tree).is_none_or(|t| t.root.is_none())
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
    /// fork or checkpoint job shares it. (Changing what the node owns is
    /// the caller's to account; changing what its image holds, its stamp.)
    fn node_mut(&mut self, id: u32) -> &mut Node {
        self.own(id);
        if self.slot(id).node.as_ref().is_some_and(|n| Arc::strong_count(n) > 1) {
            let copy = (**self.slot(id).node.as_ref().unwrap()).clone();
            self.slot_mut(id).node = Some(Arc::new(copy));
            self.recount_mem(id);
        }
        Arc::get_mut(self.slot_mut(id).node.as_mut().expect("node taken")).expect("unshared node")
    }

    /// Take the node out of slot `id` (a copy if it is shared; the slot's
    /// memory is then the copy's, which may own blocks of other capacities).
    /// An unshared node is moved out of its allocation, which stays in the
    /// slot (holding an empty leaf) for `put_node` to put the new version
    /// in: an update rewrites every node on its path, and allocating each
    /// anew took as long as a tenth of it.
    fn take_node(&mut self, id: u32) -> Node {
        self.own(id);
        let slot = self.slot_mut(id);
        let n = slot.node.as_mut().expect("node taken");
        if let Some(n) = Arc::get_mut(n) {
            return std::mem::replace(n, Node::Leaf(Vec::new()));
        }
        let shared = slot.node.take().unwrap();
        let n = (*shared).clone();
        let mem = n.mem_size() + SLOT_BYTES;
        self.set_mem(id, mem);
        n
    }

    /// In a fork: the node in slot `id` is about to change, or nobody else
    /// holds it any more; either way it is the fork's own from now on, and
    /// its memory counts against the fork's target.
    fn own(&mut self, id: u32) {
        let slot = self.slot_mut(id);
        if slot.inherited {
            slot.inherited = false;
            let (mem, dirty) = (slot.mem, slot.dirty);
            self.inherited.0 -= mem;
            if dirty {
                self.inherited.1 -= mem;
            }
        }
    }

    /// Account the memory of the node in slot `id` anew (it was copied).
    fn recount_mem(&mut self, id: u32) {
        let mem = self.node(id).mem_size() + SLOT_BYTES;
        self.set_mem(id, mem);
    }

    fn set_mem(&mut self, id: u32, mem: usize) {
        let slot = self.slot_mut(id);
        let (old, dirty) = (slot.mem, slot.dirty);
        slot.mem = mem;
        self.mem_bytes = self.mem_bytes + mem - old;
        if dirty {
            self.dirty_bytes = self.dirty_bytes + mem - old;
        }
    }

    fn next_stamp(&mut self) -> u64 {
        self.stamp += 1;
        self.stamp
    }

    fn alloc_slot(&mut self, node: Node, disk: Option<DiskLoc>, parent: u32, tree: TreeId) -> u32 {
        let bytes = node.byte_size();
        let mem = node.mem_size() + SLOT_BYTES;
        self.mem_bytes += mem;
        if disk.is_none() {
            self.dirty_bytes += mem;
        }
        let version = self.next_stamp();
        let slot = Slot {
            node: Some(Arc::new(node)),
            disk,
            dirty: disk.is_none(),
            version,
            edits: 0,
            parent,
            tree,
            loaded_children: 0,
            refbit: true,
            bytes,
            by_maint: false,
            mem,
            inherited: false,
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
        self.own(id);
        let slot = self.slots[id as usize].take().expect("double free");
        if let Some(loc) = slot.disk {
            self.dec_live(slot.tree, loc);
        }
        self.mem_bytes -= slot.mem;
        if slot.dirty {
            self.dirty_bytes -= slot.mem;
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
        self.own(id);
        let version = self.next_stamp();
        let slot = self.slot_mut(id);
        // (a new stamp also for a dirty node: it is about to change)
        slot.version = version;
        if !slot.dirty {
            slot.dirty = true;
            let (mem, disk, tree) = (slot.mem, slot.disk.take(), slot.tree);
            self.dirty_bytes += mem;
            if let Some(loc) = disk {
                self.dec_live(tree, loc);
            }
        }
    }

    fn put_node(&mut self, id: u32, node: Node) {
        let (bytes, heap) = (node.byte_size(), node.heap_size());
        self.put_node_sized(id, node, bytes, heap);
    }

    /// `put_node` with the node's byte size and heap size (`Node::heap_size`)
    /// already known.
    fn put_node_sized(&mut self, id: u32, node: Node, bytes: usize, heap: usize) {
        debug_assert_eq!(bytes, node.byte_size());
        debug_assert_eq!(heap, node.heap_size());
        self.own(id);
        let mem = node.mem_base() + heap + SLOT_BYTES;
        let version = self.next_stamp();
        let slot = self.slot_mut(id);
        let old = slot.mem;
        let dirty = slot.dirty;
        slot.bytes = bytes;
        slot.mem = mem;
        slot.version = version;
        // (into the slot's allocation unless a fork or checkpoint job
        // shares it)
        match slot.node.as_mut().and_then(Arc::get_mut) {
            Some(n) => *n = node,
            None => slot.node = Some(Arc::new(node)),
        }
        self.mem_bytes = self.mem_bytes + mem - old;
        if dirty {
            self.dirty_bytes = self.dirty_bytes + mem - old;
        }
    }

    /// Read the node frame at `loc` (counted in `segment_reads` and the
    /// stats' bytes read).
    fn read_frame(&mut self, loc: DiskLoc) -> Result<Vec<u8>> {
        self.reads += 1;
        self.read_bytes += loc.len as u64;
        self.segs.read(loc)
    }

    /// Load `ptr` (child of `parent`), returning its slot.
    fn load(&mut self, ptr: NodePtr, parent: u32, tree: TreeId) -> Result<u32> {
        match ptr {
            NodePtr::Mem(id) => {
                self.slot_mut(id).refbit = true;
                Ok(id)
            }
            NodePtr::Disk(loc) => {
                let bytes = self.read_frame(loc)?;
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

    /// Apply weighted updates: each adds its weight to its entry's (an entry
    /// whose weight becomes 0 is removed). Updates need not be sorted; equal
    /// keys are consolidated.
    pub fn apply(&mut self, tree: TreeId, updates: &mut Vec<(Vec<u8>, i64)>) -> Result<()> {
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
        // at most a quarter of the pool's target (a whole leaf per update at
        // worst, in memory), and the pool makes room between runs: a batch
        // never needs its dirty nodes to fit in memory at once.
        let budget = (self.opts.cache_bytes / 4).max(4 * self.leaf_mem(16));
        let mut start = 0;
        while start < updates.len() {
            let mut end = start;
            let mut cost = 0;
            while end < updates.len() && (end == start || cost + self.leaf_mem(updates[end].0.len()) <= budget) {
                cost += self.leaf_mem(updates[end].0.len());
                end += 1;
            }
            if let Err(e) = self.apply_run(tree, &updates[start..end], &summ) {
                updates.clear();
                return Err(e);
            }
            self.maybe_evict()?;
            start = end;
        }
        updates.clear();
        Ok(())
    }

    /// Memory of a full leaf of entries with `key`-byte keys
    /// (`Node::mem_size`): what an update dirties at most, besides the path
    /// above it.
    fn leaf_mem(&self, key: usize) -> usize {
        let entries = self.opts.leaf_bytes / (key + 6) + 1;
        SLOT_BYTES + entries * (std::mem::size_of::<LeafEntry>() + crate::memory::heap_block(key))
    }

    /// Apply sorted, consolidated updates (one descent from the root).
    fn apply_run(&mut self, tree: TreeId, updates: &[(Vec<u8>, i64)], summ: &Arc<dyn Summarizer>) -> Result<()> {
        let root = match self.root_slot(tree)? {
            Some(id) => id,
            None => self.alloc_slot(Node::Leaf(Vec::new()), None, NONE, tree),
        };
        // the tree's summary is replaced below
        let t = self.trees.get_mut(&tree).unwrap();
        let old = std::mem::take(&mut t.summary);
        let min_seg = t.min_seg;
        let parts = self.apply_node(root, updates, summ, Some(&old), min_seg);
        let parts = match parts {
            Ok(p) => p,
            Err(e) => {
                self.trees.get_mut(&tree).unwrap().summary = old;
                return Err(e);
            }
        };
        self.install_root(tree, parts, summ)
    }

    /// Make the nodes now covering a tree's key range (from an update of its
    /// root) its root, with their summary as the tree's.
    fn install_root(&mut self, tree: TreeId, mut parts: Parts, summ: &Arc<dyn Summarizer>) -> Result<()> {
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

    /// Replace a single-child internal root by its child; returns the new
    /// root.
    fn collapse_root(&mut self, mut root: u32) -> Result<u32> {
        loop {
            if !matches!(self.node(root), Node::Internal(ch) if ch.len() == 1) {
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
            let mut new_parts = self.finish_internal(root, children, None, summ, false)?;
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
    /// `old` when known, and whose subtree's `min_seg` is at least
    /// `min_seg` (as its entry above holds it). Returns the nodes now
    /// covering its key range.
    fn apply_node(&mut self, id: u32, ups: &[(Vec<u8>, i64)], summ: &Arc<dyn Summarizer>, old: Option<&Summary>, min_seg: u32) -> Result<Parts> {
        let was_dirty = self.slot(id).dirty;
        let edits = self.slot(id).edits;
        let old_bytes = self.slot(id).bytes;
        self.make_dirty(id);
        // its write is the update's now
        self.slot_mut(id).by_maint = false;
        let node = self.take_node(id);
        // what the entries or children own, kept up to date by in-place edits
        let mut heap = self.slot(id).mem - SLOT_BYTES - node.mem_base();
        match node {
            Node::Leaf(entries) => {
                // only inserts past the leaf's last key: an append, which
                // will not come back to the leaf's left part
                let append = entries.last().is_none_or(|e| *ups[0].0 > *e.key);
                // weight changes as (index into ups, weight before), folded
                // into the leaf's old summary afterwards if the leaf does
                // not split and that is cheaper than recomputing it
                let mut changes: smallvec::SmallVec<[(usize, i64); 4]> = smallvec::SmallVec::with_capacity(ups.len());
                // Few updates to a dirty leaf: edit in place (each insert or
                // removal shifts the entries after it). More updates, the
                // first ones since the leaf was read or written, or those
                // after 16 in-place edits: merge into a new vector sized to
                // the entries, returning the capacity in-place inserts grew
                // and removals left (memory is accounted by capacity).
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
                            let before = entries[pos].weight;
                            entries[pos].weight += w;
                            if entries[pos].weight == 0 {
                                let e = entries.remove(pos);
                                size -= entry_size(&e);
                                heap -= entry_heap(&e);
                            } else {
                                pos += 1;
                            }
                            before
                        } else {
                            let e = LeafEntry { key: k.as_slice().into(), weight: *w };
                            size += entry_size(&e);
                            heap += entry_heap(&e);
                            entries.insert(pos, e);
                            pos += 1;
                            0
                        };
                        changes.push((ui, before));
                    }
                    entries
                } else {
                    let mut out: Vec<LeafEntry> = Vec::with_capacity(entries.len() + ups.len());
                    let mut it = entries.into_iter().peekable();
                    for (ui, (k, w)) in ups.iter().enumerate() {
                        while let Some(e) = it.next_if(|e| *e.key < **k) {
                            out.push(e);
                        }
                        let before = match it.next_if(|e| *e.key == **k) {
                            Some(mut e) => {
                                let before = e.weight;
                                e.weight += w;
                                if e.weight != 0 {
                                    out.push(e);
                                }
                                before
                            }
                            None => {
                                out.push(LeafEntry { key: k.as_slice().into(), weight: *w });
                                0
                            }
                        };
                        changes.push((ui, before));
                    }
                    out.extend(it);
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
                    self.put_node_sized(id, Node::Leaf(out), size, heap);
                    // the first part keeps the node's place (and separator) in the parent
                    return Ok(smallvec::smallvec![Part { sep: Box::default(), id, summary, min_seg: u32::MAX }]);
                }
                let chunks = split_by_bytes(out, self.opts.leaf_bytes, entry_size, 1, append);
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
                    // (dirty leaves have no location of their own yet)
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
                // The subtree's min_seg, the minimum of the children's,
                // follows the children that changed, like its size and
                // summary (computed from all the children only when a child
                // that may have held the minimum got a higher one, or
                // children were merged): scanning a node's children for it
                // took a tenth of a single-row update.
                let mut incr_space = Some(min_seg);
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
                    let was = children[ci].min_seg;
                    let parts = self.apply_node(cid, &ups[start..end], summ, Some(&children[ci].summary), was)?;
                    if let [p] = &parts[..]
                        && self.slot(p.id).bytes < before
                    {
                        shrunk.push(p.id);
                    }
                    if let Some(min_seg) = incr_space.as_mut() {
                        let now = parts.iter().map(|p| p.min_seg).min().unwrap_or(u32::MAX);
                        if was <= *min_seg && now > was {
                            incr_space = None;
                        } else {
                            *min_seg = (*min_seg).min(now);
                        }
                    }
                    if let Some(s) = incr.as_mut() {
                        let news: smallvec::SmallVec<[&Summary; 2]> = parts.iter().map(|p| &p.summary).collect();
                        if !s.replace_child(&children[ci].summary, &news) {
                            incr = None;
                        }
                    }
                    // the first part keeps the child's separator
                    size -= node::child_size(&children[ci]);
                    heap -= child_heap(&children[ci]);
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
                    heap += children[ci..ci + nparts].iter().map(child_heap).sum::<usize>();
                }
                if children.is_empty() {
                    self.slot_mut(id).loaded_children = 0;
                    self.free_slot(id);
                    return Ok(Parts::new());
                }
                if !shrunk.is_empty() {
                    let mut unlimited = u64::MAX;
                    if self.merge_underfull(id, &mut children, &shrunk, &mut size, &mut heap, &mut unlimited, false)? > 0 {
                        incr_space = None;
                    }
                }
                // splits of the last child only: an append at the right edge
                let append = runs.len() == 1 && runs[0].0 + 1 == n;
                self.finish_internal_sized(id, children, incr, incr_space, summ, size, heap, append)
            }
        }
    }

    /// Merge the children of internal node `id` listed in `shrunk` (just
    /// made smaller) that are below a quarter of their target size with a
    /// neighbour, while the two fit in three quarters of it (so that the
    /// result does not split again soon). Loading a neighbour and rewriting
    /// a clean one take their bytes from `budget`; no merge is tried once it
    /// is exhausted. `maint`: on behalf of the maintenance pass. The
    /// summary of `id` is unchanged; `size` and `heap` (its byte size and
    /// what its children own, `child_heap`) are kept up to date. Returns
    /// the number of merges.
    #[allow(clippy::too_many_arguments)]
    fn merge_underfull(
        &mut self,
        id: u32,
        children: &mut Vec<Child>,
        shrunk: &[u32],
        size: &mut usize,
        heap: &mut usize,
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
                merged = Some((l, self.merge_pair(children, l, size, heap)));
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
    /// `children`, byte size `size` and heap size `heap`) into loaded child
    /// `l`; returns the slot of the merged node.
    fn merge_pair(&mut self, children: &mut Vec<Child>, l: usize, size: &mut usize, heap: &mut usize) -> u32 {
        let (NodePtr::Mem(a), NodePtr::Mem(b)) = (children[l].ptr, children[l + 1].ptr) else {
            unreachable!("merging a child that is not loaded")
        };
        self.make_dirty(a);
        self.make_dirty(b);
        let right = children.remove(l + 1);
        // (the merged child's summary changes, and with it its sizes)
        *size -= node::child_size(&right) + node::child_size(&children[l]);
        *heap -= child_heap(&right) + child_heap(&children[l]);
        let nb = self.take_node(b);
        let na = self.take_node(a);
        let c = &mut children[l];
        c.summary.combine(&right.summary);
        let merged = match (na, nb) {
            (Node::Leaf(mut x), Node::Leaf(y)) => {
                x.extend(y);
                self.slot_mut(a).edits = 0;
                c.min_seg = u32::MAX;
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
                Node::Internal(x)
            }
            _ => unreachable!("siblings at different depths"),
        };
        *size += node::child_size(c);
        *heap += child_heap(c);
        self.put_node(a, merged);
        self.put_node(b, Node::Leaf(Vec::new()));
        self.free_slot(b);
        a
    }

    /// `finish_internal` for a node whose byte and heap sizes are known:
    /// when it does not need to split, its children's parent links and the
    /// loaded count are already right (maintained by load, allocation and
    /// free).
    #[allow(clippy::too_many_arguments)]
    fn finish_internal_sized(
        &mut self,
        id: u32,
        children: Vec<Child>,
        summary: Option<Summary>,
        min_seg: Option<u32>,
        summ: &Arc<dyn Summarizer>,
        size: usize,
        heap: usize,
        append: bool,
    ) -> Result<Parts> {
        if size > self.opts.internal_bytes {
            return self.finish_internal(id, children, summary, summ, append);
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
        let min_seg = match min_seg {
            Some(m) => m,
            None => {
                #[cfg(test)]
                SPACE_SCANS.with(|c| c.set(c.get() + children.len() as u64));
                children.iter().map(|c| c.min_seg).fold(u32::MAX, u32::min)
            }
        };
        self.put_node_sized(id, Node::Internal(children), size, heap);
        // the first part keeps the node's place (and separator) in the parent
        Ok(smallvec::smallvec![Part { sep: Box::default(), id, summary: s, min_seg }])
    }

    /// Store `children` in internal node `id`, splitting by size (see
    /// `split_by_bytes` for `append`). `summary` is the node's summary if
    /// known (used when it does not split).
    fn finish_internal(
        &mut self,
        id: u32,
        children: Vec<Child>,
        summary: Option<Summary>,
        summ: &Arc<dyn Summarizer>,
        append: bool,
    ) -> Result<Parts> {
        let parent = self.slot(id).parent;
        let tree = self.slot(id).tree;
        // internal nodes keep at least two children per chunk so that a tree
        // with oversized separators still converges to a single root
        let chunks = split_by_bytes(children, self.opts.internal_bytes, node::child_size, 2, append);
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

    /// Exact key lookup: the entry's weight (None: there is no such entry).
    pub fn get(&mut self, tree: TreeId, key: &[u8]) -> Result<Option<i64>> {
        let Some(mut id) = self.root_slot(tree)? else { return Ok(None) };
        loop {
            match self.node(id) {
                Node::Leaf(entries) => {
                    let r = entries.binary_search_by(|e| (*e.key).cmp(key)).ok().map(|i| entries[i].weight);
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

    /// Let scans (`for_each`, cursors and what uses them) visit at most `n`
    /// more entries, then fail with `READ_BUDGET` (None: no limit): a read
    /// on the engine thread that turns out large is continued elsewhere.
    pub fn set_read_budget(&mut self, n: Option<u64>) {
        self.read_budget = n;
    }

    /// Entries the read budget has left (None: no limit).
    pub fn read_budget(&self) -> Option<u64> {
        self.read_budget
    }

    /// Count `n` entries visited against the read budget.
    fn spend(&mut self, n: u64) -> Result<()> {
        if let Some(b) = self.read_budget.as_mut() {
            if *b < n {
                return Err(SqlError::new(READ_BUDGET, "the read visited more entries than its budget"));
            }
            *b -= n;
        }
        Ok(())
    }

    pub fn cursor(&self, tree: TreeId, lo: Bound<&[u8]>, hi: Bound<&[u8]>) -> Cursor {
        Cursor { tree, lo: own_bound(lo), hi: own_bound(hi), buf: VecDeque::new(), done: false }
    }

    pub fn cursor_next(&mut self, c: &mut Cursor) -> Result<Option<(Box<[u8]>, i64)>> {
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
                c.buf.push_back((e.key.clone(), e.weight));
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
            self.spend(c.buf.len() as u64)?;
            if !c.buf.is_empty() || c.done {
                return Ok(());
            }
        }
    }

    /// Collect all entries in a range (convenience for small ranges).
    pub fn scan(&mut self, tree: TreeId, lo: Bound<&[u8]>, hi: Bound<&[u8]>) -> Result<Vec<(Box<[u8]>, i64)>> {
        let mut out = Vec::new();
        self.for_each(tree, lo, hi, &mut |k, w| {
            out.push((k.into(), w));
            Ok(true)
        })?;
        Ok(out)
    }

    /// Entries whose key starts with `prefix`.
    pub fn scan_prefix(&mut self, tree: TreeId, prefix: &[u8]) -> Result<Vec<(Box<[u8]>, i64)>> {
        let mut out = Vec::new();
        self.for_each_prefix(tree, prefix, &mut |k, w| {
            out.push((k.into(), w));
            Ok(true)
        })?;
        Ok(out)
    }

    /// Call `f` with the entries in [lo, hi) and their weights, in key
    /// order, until it returns false. Entries are visited where they are,
    /// without copying them.
    pub fn for_each(&mut self, tree: TreeId, lo: Bound<&[u8]>, hi: Bound<&[u8]>, f: &mut dyn FnMut(&[u8], i64) -> Result<bool>) -> Result<()> {
        self.walk(tree, lo, &|k| below_hi(k, hi), f)
    }

    /// `for_each` over the entries whose key starts with `prefix`.
    pub fn for_each_prefix(&mut self, tree: TreeId, prefix: &[u8], f: &mut dyn FnMut(&[u8], i64) -> Result<bool>) -> Result<()> {
        self.walk(tree, Bound::Included(prefix), &|k| k.starts_with(prefix), f)
    }

    /// Visit entries from `lo` while `in_range` (true up to some key, false
    /// after it) holds, one leaf at a time: each leaf is found from the root,
    /// so nothing is pinned between leaves. Once the walk has read more than
    /// a quarter of the pool's target from disk, it is a large scan: while
    /// the pool is over its target, the nodes it read are evicted as soon as
    /// it is done with them (`evict_scanned`), so that it recycles its own
    /// nodes instead of loading the whole tree or pushing the pool's working
    /// set out. Before that, what it reads is cached like any other node,
    /// the pool evicting (CLOCK) after every 1/64 of its target read.
    fn walk(
        &mut self,
        tree: TreeId,
        lo: Bound<&[u8]>,
        in_range: &dyn Fn(&[u8]) -> bool,
        f: &mut dyn FnMut(&[u8], i64) -> Result<bool>,
    ) -> Result<()> {
        let mut resume: Option<Vec<u8>> = None;
        // bytes read from disk, and the nodes read that a large scan may
        // still evict (top down)
        let (mut read, mut swept) = (0usize, 0usize);
        let mut scanned: smallvec::SmallVec<[u32; 8]> = smallvec::SmallVec::new();
        loop {
            let Some(mut id) = self.root_slot(tree)? else { return Ok(()) };
            let lo = match &resume {
                Some(v) => Bound::Included(v.as_slice()),
                None => lo,
            };
            // the deepest internal node with a child after the path (slots
            // stay put until the leaf is done)
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
                        let from_disk = matches!(children[idx].ptr, NodePtr::Disk(_));
                        id = self.load_child(id, idx)?;
                        if from_disk {
                            read += self.slot(id).mem;
                            scanned.push(id);
                        }
                    }
                }
            }
            let Node::Leaf(entries) = self.node(id) else { unreachable!() };
            let start = match lo {
                Bound::Unbounded => 0,
                Bound::Included(k) => entries.partition_point(|e| &*e.key < k),
                Bound::Excluded(k) => entries.partition_point(|e| &*e.key <= k),
            };
            let mut visited = 0u64;
            for e in &entries[start..] {
                visited += 1;
                if !in_range(&e.key) || !f(&e.key, e.weight)? {
                    self.maybe_evict()?;
                    return self.spend(visited);
                }
            }
            let spent = self.spend(visited);
            let sep = match next {
                Some((pid, ci)) => match self.node(pid) {
                    Node::Internal(ch) if spent.is_ok() && in_range(&ch[ci].sep) => Some(ch[ci].sep.to_vec()),
                    _ => None,
                },
                None => None,
            };
            let large = read > self.opts.cache_bytes / 4;
            match sep {
                Some(s) => {
                    // done with the nodes not on the path to the next leaf
                    if large {
                        self.evict_scanned(&mut scanned, next.map(|(pid, _)| pid));
                    } else {
                        scanned.clear();
                        // (a sweep per leaf would cost a pass over the arena
                        // per leaf whenever dirty nodes leave little to evict)
                        if read - swept > self.opts.cache_bytes / 64 {
                            swept = read;
                            self.maybe_evict()?;
                        }
                    }
                    resume = Some(s);
                }
                None => {
                    // (also a scan stopped by the read budget: what it read
                    // is done with)
                    if large {
                        self.evict_scanned(&mut scanned, None);
                    }
                    self.maybe_evict()?;
                    return spent;
                }
            }
        }
    }

    /// For a large scan: of the nodes it read (`scanned`, top down), those
    /// not on the path to `keep` (the node it goes on from) are done; evict
    /// them, deepest first, while the pool is over its target, and stop
    /// tracking them either way. O(depth) per leaf, and the pool's other
    /// nodes stay where they are.
    fn evict_scanned(&mut self, scanned: &mut smallvec::SmallVec<[u32; 8]>, keep: Option<u32>) {
        let mut path: smallvec::SmallVec<[u32; 8]> = smallvec::SmallVec::new();
        let mut k = keep.unwrap_or(NONE);
        while k != NONE {
            path.push(k);
            k = self.slot(k).parent;
        }
        let mut i = scanned.len();
        while i > 0 {
            i -= 1;
            let id = scanned[i];
            if path.contains(&id) {
                continue;
            }
            let slot = self.slot(id);
            if self.mem_bytes > self.opts.cache_bytes && !slot.dirty && slot.loaded_children == 0 {
                self.evict(id);
            }
            scanned.remove(i);
        }
    }

    /// Summary over the entries in [lo, hi) (current weights), with the
    /// MIN/MAX values that node summaries hold as prefixes resolved.
    pub fn range_summary(&mut self, tree: TreeId, lo: Bound<&[u8]>, hi: Bound<&[u8]>) -> Result<Summary> {
        let summ = self.trees.get(&tree).ok_or_else(|| SqlError::internal(format!("no tree {tree}")))?.summarizer.clone();
        let empty = || Summary::empty(summ.spec());
        let mut parts = RangeParts { left: empty(), inside: empty(), right: empty(), past_left: false };
        let all = matches!((lo, hi), (Bound::Unbounded, Bound::Unbounded));
        if all {
            parts.inside = self.tree_summary(tree);
        } else if let Some(root) = self.root_slot(tree)? {
            self.summary_rec(root, lo, hi, (None, None), &summ, &mut parts)?;
        }
        self.resolve_extremes(tree, lo, hi, &summ, &mut parts)?;
        self.maybe_evict()?;
        if all {
            return Ok(parts.inside);
        }
        let RangeParts { left: mut acc, inside, right, .. } = parts;
        acc.combine(&inside);
        acc.combine(&right);
        Ok(acc)
    }

    /// Replace the prefixes standing for wide MIN/MAX values in the
    /// summary of the subtrees inside a range by the values, where they can
    /// be the range's extreme. Such an extreme is the leftmost best value
    /// among those subtrees' entries with that prefix, and only subtrees
    /// whose own extreme has the prefix can hold one: when the entries with
    /// it all have the same value, the first one found is it, so the path to
    /// it is all that is visited.
    fn resolve_extremes(
        &mut self,
        tree: TreeId,
        lo: Bound<&[u8]>,
        hi: Bound<&[u8]>,
        summ: &Arc<dyn Summarizer>,
        parts: &mut RangeParts,
    ) -> Result<()> {
        let mut targets = Vec::new();
        for i in 0..parts.inside.aggs.len() {
            if parts.inside.aggs[i].is_prefix() {
                let mut edges = parts.left.aggs[i].clone();
                edges.combine(&parts.right.aggs[i]);
                if parts.inside.aggs[i].settle_with(&edges) {
                    targets.push(i);
                }
            }
        }
        if targets.is_empty() {
            return Ok(());
        }
        let mut found = Summary::empty(summ.spec());
        let mut done = vec![false; targets.len()];
        let all = matches!((lo, hi), (Bound::Unbounded, Bound::Unbounded));
        if let Some(root) = self.root_slot(tree)? {
            self.resolve_rec(root, lo, hi, (None, None), all, summ, &parts.inside, &targets, &mut done, &mut found)?;
        }
        for i in targets {
            if found.aggs[i].is_prefix() || matches!(found.aggs[i], AggState::Min(None) | AggState::Max(None)) {
                return Err(SqlError::internal(format!("no entry for MIN/MAX prefix {:?}", parts.inside.aggs[i])));
            }
            parts.inside.aggs[i] = found.aggs[i].clone();
        }
        Ok(())
    }

    /// Fold into `found`, in key order, the entries below node `id` (with
    /// keys in `node`, a subtree inside [lo, hi) if `inside`) that are in
    /// subtrees inside the range and may hold the extremes `want.aggs[t]`
    /// (`t` in `targets`, not `done`).
    #[allow(clippy::too_many_arguments)]
    fn resolve_rec(
        &mut self,
        id: u32,
        lo: Bound<&[u8]>,
        hi: Bound<&[u8]>,
        node: KeyRange,
        inside: bool,
        summ: &Arc<dyn Summarizer>,
        want: &Summary,
        targets: &[usize],
        done: &mut [bool],
        found: &mut Summary,
    ) -> Result<()> {
        let items = match self.node(id) {
            // the entries at the range's edges are summarized apart
            Node::Leaf(_) if !inside => return Ok(()),
            Node::Leaf(entries) => {
                for e in entries {
                    summ.add_entry(found, &e.key, e.weight)?;
                    let mut all = true;
                    for (d, &t) in done.iter_mut().zip(targets) {
                        *d = *d || want.aggs[t].found_in(&found.aggs[t]);
                        all &= *d;
                    }
                    if all {
                        break;
                    }
                }
                return Ok(());
            }
            Node::Internal(children) if inside => (0..children.len()).map(|i| (i, true)).collect(),
            Node::Internal(children) => overlapping(children, lo, hi, node),
        };
        for (i, full) in items {
            let Node::Internal(children) = self.node(id) else { unreachable!() };
            let sub = &children[i].summary;
            if targets.iter().zip(done.iter()).any(|(&t, &d)| !d && want.aggs[t].may_hold_extreme(&sub.aggs[t], full)) {
                let (clo, chi) = child_range(children, i, node);
                let cid = self.load_child(id, i)?;
                self.resolve_rec(cid, lo, hi, (clo.as_deref(), chi.as_deref()), full, summ, want, targets, done, found)?;
            }
        }
        Ok(())
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

    /// Fold the entries of [lo, hi) below node `id`, with keys in `node`,
    /// into `parts`: the subtrees that lie inside the range are combined, and
    /// only the (at most two per level) partially covered children are
    /// descended into, down to the leaves at the range's edges.
    fn summary_rec(
        &mut self,
        id: u32,
        lo: Bound<&[u8]>,
        hi: Bound<&[u8]>,
        node: KeyRange,
        summ: &Arc<dyn Summarizer>,
        parts: &mut RangeParts,
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
                let acc = if parts.past_left { &mut parts.right } else { &mut parts.left };
                if summ.spec().is_empty() {
                    acc.count += entries.iter().map(|e| e.weight).sum::<i64>();
                } else {
                    summ.add_entries(acc, &mut entries.iter().map(|e| (&*e.key, e.weight)))?;
                }
                Ok(())
            }
            Node::Internal(children) => {
                // (child, fully contained); visited in key order so that ties
                // between MIN/MAX candidates resolve to the leftmost
                let items = overlapping(children, lo, hi, node);
                for (i, full) in items {
                    let Node::Internal(children) = self.node(id) else { unreachable!() };
                    if full {
                        parts.inside.combine(&children[i].summary);
                        parts.past_left = true;
                    } else {
                        let (clo, chi) = child_range(children, i, node);
                        let cid = self.load_child(id, i)?;
                        self.summary_rec(cid, lo, hi, (clo.as_deref(), chi.as_deref()), summ, parts)?;
                    }
                }
                Ok(())
            }
        }
    }

    /// The n-th unit of weight (0-based) in [lo, hi): returns the entry,
    /// its weight and the offset within that entry's multiplicity.
    pub fn nth(&mut self, tree: TreeId, lo: Bound<&[u8]>, hi: Bound<&[u8]>, n: i64) -> Result<Option<NthEntry>> {
        let Some(root) = self.root_slot(tree)? else { return Ok(None) };
        let mut rem = n;
        let r = self.nth_rec(root, lo, hi, &mut rem)?;
        self.maybe_evict()?;
        Ok(r)
    }

    fn nth_rec(&mut self, id: u32, lo: Bound<&[u8]>, hi: Bound<&[u8]>, rem: &mut i64) -> Result<Option<NthEntry>> {
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
                    let w = e.weight;
                    if w > *rem {
                        return Ok(Some((e.key.clone(), w, *rem)));
                    }
                    *rem -= w.max(0);
                }
                Ok(None)
            }
            Node::Internal(children) => {
                let plan: Vec<(usize, bool, i64)> =
                    overlapping(children, lo, hi, (None, None)).into_iter().map(|(i, full)| (i, full, children[i].summary.count)).collect();
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
    ///
    /// Not candidates: dirty nodes frozen in a checkpoint job (which writes
    /// them; they are clean once it is finished), and in a fork the nodes
    /// it inherited, which it shares with the environment it was forked
    /// from and does not count — unless nobody else holds one any more,
    /// when it becomes the fork's own.
    fn maybe_evict(&mut self) -> Result<()> {
        let (cache, limit) = (self.opts.cache_bytes, self.dirty_limit());
        let over = |e: &Env| e.mem_bytes - e.inherited.0 > cache || e.dirty_bytes - e.inherited.1 > limit;
        if !over(self) || self.slots.is_empty() {
            return Ok(());
        }
        if let Some((mem, dirty)) = self.evict_stall {
            let slack = cache / 16 + self.opts.leaf_bytes;
            if self.mem_bytes - self.inherited.0 <= mem + slack && self.dirty_bytes - self.inherited.1 <= dirty + slack {
                return Ok(());
            }
            self.evict_stall = None;
        }
        let n = self.slots.len();
        let mut scanned = 0;
        while over(self) && scanned < 2 * n {
            let i = self.clock_hand % n;
            self.clock_hand = (i + 1) % n;
            scanned += 1;
            self.swept_slots += 1;
            let Some(slot) = self.slots[i].as_mut() else { continue };
            if slot.loaded_children > 0 || slot.node.is_none() {
                continue;
            }
            if slot.inherited {
                if slot.node.as_ref().is_some_and(|n| Arc::strong_count(n) > 1) {
                    continue;
                }
                self.own(i as u32);
            }
            let slot = self.slots[i].as_mut().unwrap();
            if slot.refbit {
                slot.refbit = false;
                continue;
            }
            if slot.dirty {
                if (self.mem_bytes - self.inherited.0 <= cache && self.dirty_bytes - self.inherited.1 <= limit) || self.frozen(i as u32) {
                    continue;
                }
                // its children are all on disk (none is loaded)
                self.write_node(i as u32, usize::MAX)?;
            }
            if self.mem_bytes - self.inherited.0 > cache {
                self.evict(i as u32);
            }
        }
        if over(self) {
            self.evict_stall = Some((self.mem_bytes - self.inherited.0, self.dirty_bytes - self.inherited.1));
        }
        Ok(())
    }

    /// Is the dirty node in slot `id` frozen in the checkpoint job in
    /// progress (dirty when it began, unchanged since)?
    fn frozen(&self, id: u32) -> bool {
        self.frozen_at.is_some_and(|f| self.slot(id).version <= f)
    }

    /// Write dirty nodes early, children before parents, until at most
    /// `target` dirty bytes remain, as the pool does on its own beyond half
    /// of the cache. A hook for callers that manage memory or checkpoints
    /// incrementally: the nodes written are clean and evictable, the next
    /// checkpoint only writes the rest. Returns the dirty bytes left (more
    /// than `target` only while a checkpoint job is writing the rest, or
    /// in a fork, where its inherited nodes are not written).
    pub fn write_back(&mut self, target: usize) -> Result<usize> {
        // children before parents: leaves first, then each level above
        while self.dirty_bytes > target {
            let before = self.dirty_bytes;
            for i in 0..self.slots.len() {
                if self.dirty_bytes <= target {
                    break;
                }
                let writable = match &self.slots[i] {
                    Some(Slot { dirty: true, inherited: false, node: Some(node), .. }) if !self.frozen(i as u32) => match &**node {
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
        // the nodes written can be evicted
        self.evict_stall = None;
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
    ///
    /// The one way nodes are written, early or by a checkpoint: the frame
    /// counts as live in its segment and its tree's, and towards the next
    /// maintenance pass's budget unless that pass dirtied the node; the
    /// child entry above it gets the node's exact `min_seg`, the entries
    /// above that a `min_seg` lowered to it where they were higher (they
    /// cover more than the node), and so does the tree when the node is its
    /// root. `idx` is the node's position in its parent, if known.
    fn write_node(&mut self, id: u32, idx: usize) -> Result<DiskLoc> {
        let slot = self.slot(id);
        debug_assert!(slot.dirty, "writing a clean node");
        let node = slot.node.as_deref().expect("node taken");
        let mut locs = Vec::new();
        if let Node::Internal(children) = node {
            locs.reserve(children.len());
            for c in children {
                locs.push(match c.ptr {
                    NodePtr::Disk(loc) => loc,
                    NodePtr::Mem(cid) => self.slot(cid).disk.expect("writing a node above a dirty child"),
                });
            }
        }
        let mut buf = Vec::with_capacity(slot.bytes + 16);
        node.encode_as(&locs, &mut buf, self.legacy_format);
        // (the entries of clean children are exact)
        let min_seg = match node {
            Node::Leaf(_) => u32::MAX,
            Node::Internal(ch) => ch.iter().map(|c| c.min_seg).min().unwrap_or(u32::MAX),
        };
        let (tree, by_maint) = (slot.tree, slot.by_maint);
        let loc = self.segs.append(&buf)?;
        let min_seg = min_seg.min(loc.seg);
        self.add_live(tree, loc);
        if !by_maint {
            self.fg_appended += loc.len as u64;
        }
        let slot = self.slot_mut(id);
        slot.disk = Some(loc);
        slot.dirty = false;
        slot.by_maint = false;
        let (mem, parent) = (slot.mem, slot.parent);
        self.dirty_bytes -= mem;
        self.set_child_space(parent, id, idx, min_seg);
        if let Some(f) = self.flushed.as_mut() {
            let version = self.slots[id as usize].as_ref().unwrap().version;
            f.push(Flushed { slot: id, version, loc, min_seg });
        }
        Ok(loc)
    }

    /// Node `id`, a child of `parent` (at `idx` there, if known), was
    /// written with subtree `min_seg`: its entry gets it exactly, the
    /// entries above that a `min_seg` lowered to it where they were higher
    /// (they cover more than the node), and so does its tree when the node
    /// is its root.
    fn set_child_space(&mut self, parent: u32, id: u32, idx: usize, min_seg: u32) {
        let tree = self.slot(id).tree;
        if parent == NONE {
            if let Some(t) = self.trees.get_mut(&tree)
                && t.root == Some(NodePtr::Mem(id))
            {
                t.min_seg = min_seg;
            }
            return;
        }
        let Node::Internal(ch) = self.node_mut(parent) else { unreachable!("parent of a loaded node is not a loaded internal node") };
        let i = match ch.get(idx) {
            Some(c) if c.ptr == NodePtr::Mem(id) => idx,
            _ => ch.iter().position(|c| c.ptr == NodePtr::Mem(id)).expect("child pointer"),
        };
        ch[i].min_seg = min_seg;
        let mut child = parent;
        let mut at = self.slot(parent).parent;
        while at != NONE {
            let Node::Internal(ch) = self.node(at) else { unreachable!("parent of a loaded node is not a loaded internal node") };
            let i = ch.iter().position(|c| c.ptr == NodePtr::Mem(child)).expect("child pointer");
            if ch[i].min_seg <= min_seg {
                return;
            }
            let Node::Internal(ch) = self.node_mut(at) else { unreachable!() };
            ch[i].min_seg = min_seg;
            child = at;
            at = self.slot(at).parent;
        }
        if let Some(t) = self.trees.get_mut(&tree)
            && t.root == Some(NodePtr::Mem(child))
        {
            t.min_seg = t.min_seg.min(min_seg);
        }
    }

    fn evict(&mut self, id: u32) {
        self.own(id);
        let slot = self.slots[id as usize].take().unwrap();
        let loc = slot.disk.expect("evicting node without disk location");
        if slot.parent == NONE {
            if let Some(t) = self.trees.get_mut(&slot.tree) {
                t.root = Some(NodePtr::Disk(loc));
            }
        } else if let Some(p) = self.slots[slot.parent as usize].as_mut() {
            p.loaded_children = p.loaded_children.saturating_sub(1);
            if let Node::Internal(ch) = self.node_mut(slot.parent) {
                for c in ch.iter_mut() {
                    if c.ptr == NodePtr::Mem(id) {
                        c.ptr = NodePtr::Disk(loc);
                        break;
                    }
                }
            }
        }
        self.mem_bytes -= slot.mem;
        self.free_slots.push(id);
    }

    // ------------------------------------------------------------------
    // checkpoint, drop

    /// Make all trees durable as of `gsn` together with the application state
    /// (`begin_checkpoint`, `CheckpointJob::run` and `finish_checkpoint` in one).
    pub fn checkpoint(&mut self, gsn: Gsn, app: &[u8]) -> Result<()> {
        let job = self.begin_checkpoint(gsn)?;
        let done = job.run(app)?;
        self.finish_checkpoint(done)
    }

    /// Start a checkpoint of all trees as they are now (at `gsn`): the job
    /// holds a version of the dirty nodes (shared, copied when this
    /// environment modifies them later) and the manifest, and writes them
    /// on any thread (`CheckpointJob::run`) while this environment goes on
    /// — also writing other dirty nodes early, to the same active segment
    /// (the append side is shared); `finish_checkpoint` then marks the
    /// nodes it wrote clean where they have not changed since. A second
    /// checkpoint waits for the first.
    pub fn begin_checkpoint(&mut self, gsn: Gsn) -> Result<CheckpointJob> {
        if self.manifest.is_none() {
            return Err(SqlError::internal("a checkpoint is already in progress"));
        }
        // (the totals of segments forks wrote to, for the record)
        self.count_foreign();
        // the dirty nodes, and the clean nodes whose locations their images
        // (and the tree records) need
        let mut slots: Vec<Option<Slot>> = Vec::with_capacity(self.slots.len());
        slots.resize_with(self.slots.len(), || None);
        let (mut mem_bytes, mut dirty_bytes) = (0, 0);
        let mut copy = |slots: &mut Vec<Option<Slot>>, s: &Slot, i: usize| {
            if slots[i].is_none() {
                mem_bytes += s.mem;
                if s.dirty {
                    dirty_bytes += s.mem;
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
        let segs = self.segs.share();
        self.frozen_at = Some(self.stamp);
        let env = Env {
            dir: self.dir.clone(),
            opts: self.opts.clone(),
            segs,
            slots,
            free_slots: Vec::new(),
            mem_bytes,
            dirty_bytes,
            clock_hand: 0,
            evict_stall: None,
            swept_slots: 0,
            work: self.work.clone(),
            trees: self.trees.clone(),
            next_tree_id: self.next_tree_id,
            seg_live: self.seg_live.clone(),
            checkpoint_seq: self.checkpoint_seq,
            checkpoint_gsn: self.checkpoint_gsn,
            fg_appended: 0,
            legacy_format: self.legacy_format,
            manifest: self.manifest.take(),
            stamp: self.stamp,
            flushed: Some(Vec::new()),
            reads: 0,
            read_bytes: 0,
            read_budget: None,
            frozen_at: None,
            inherited: (0, 0),
            held: crate::memory::Charge::none(),
        };
        Ok(CheckpointJob { env, gsn })
    }

    /// Count the bytes forks appended in the segments' totals.
    fn count_foreign(&mut self) {
        for (seg, bytes) in self.segs.take_foreign() {
            self.seg_live.entry(seg).or_insert((0, 0)).0 += bytes;
        }
    }

    /// Take back the manifest and mark the nodes the job wrote clean,
    /// except those modified (or freed) since it began: their images are
    /// garbage from the start. A node marked clean is accounted as it would
    /// have been had it been written here (`write_node`). Then delete the
    /// sealed segments dead in the committed checkpoint that are dead here
    /// too: this environment may have written to them or adopted trees with
    /// nodes in them since the job began. (Those that a fork may still
    /// read are deleted once it is dropped; those a fork wrote to are kept
    /// for a later checkpoint.)
    pub fn finish_checkpoint(&mut self, done: CheckpointDone) -> Result<()> {
        self.manifest = Some(done.manifest);
        self.frozen_at = None;
        self.fg_appended += done.fg_appended;
        // (children before parents)
        for f in done.flushed {
            let current = matches!(self.slots.get(f.slot as usize), Some(Some(s)) if s.dirty && s.version == f.version);
            if !current {
                self.seg_live.entry(f.loc.seg).or_insert((0, 0)).0 += f.loc.len as u64;
                continue;
            }
            let tree = self.slot(f.slot).tree;
            self.add_live(tree, f.loc);
            let slot = self.slot_mut(f.slot);
            slot.dirty = false;
            slot.disk = Some(f.loc);
            slot.by_maint = false;
            let (mem, parent) = (slot.mem, slot.parent);
            self.dirty_bytes -= mem;
            self.set_child_space(parent, f.slot, usize::MAX, f.min_seg);
        }
        for id in done.dead {
            if self.seg_live.get(&id).is_none_or(|(_, live)| *live == 0) && self.segs.delete(id)? {
                self.seg_live.remove(&id);
            }
        }
        self.checkpoint_seq = done.seq;
        self.checkpoint_gsn = done.gsn;
        // the nodes written can be evicted
        self.evict_stall = None;
        let active = self.segs.active_id();
        self.seg_live.entry(active).or_insert((0, 0));
        Ok(())
    }

    /// Write every dirty node, make the segments durable and append the
    /// checkpoint record (the commit point). Returns the sealed segments
    /// without live nodes in it.
    fn write_checkpoint(&mut self, gsn: Gsn, app: &[u8]) -> Result<Vec<u32>> {
        let tree_ids = self.tree_ids();
        let mut records = Vec::with_capacity(tree_ids.len());
        let mut space = Vec::with_capacity(tree_ids.len());
        for tid in tree_ids {
            // (temporary trees are not recorded)
            if tid >= TEMP_TREES {
                continue;
            }
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
            space.push(TreeSpace { segs: t.seg_live.iter().map(|(s, l)| (*s, *l)).collect() });
        }
        // (what others appended meanwhile and is synced too lies beyond
        // what the record names, as any frame after a checkpoint does)
        let (active, active_len, sealed) = self.segs.sync()?;
        self.seg_live.entry(active).or_insert((0, 0));
        // the totals of the segments written in this process are their
        // lengths, also of what the environment and its forks appended to
        // them since the job began
        for (seg, len) in sealed.into_iter().chain([(active, active_len)]) {
            if let Some(e) = self.seg_live.get_mut(&seg) {
                e.0 = len;
            }
        }
        // live bytes per segment as the checkpoint has them: without the
        // nodes of temporary trees
        let mut segments = self.seg_live.clone();
        for (_, t) in self.trees.iter().filter(|(id, _)| **id >= TEMP_TREES) {
            for (seg, live) in &t.seg_live {
                if let Some(e) = segments.get_mut(seg) {
                    e.1 = e.1.saturating_sub(*live);
                }
            }
        }
        self.checkpoint_seq += 1;
        let rec = CheckpointRecord {
            seq: self.checkpoint_seq,
            gsn,
            trees: records,
            segments: segments.iter().map(|(id, (t, l))| (*id, *t, *l)).collect(),
            active_seg: active,
            active_len,
            next_tree_id: self.next_tree_id,
            app: app.to_vec(),
            space: if self.legacy_format { None } else { Some(space) },
        };
        self.manifest.as_mut().ok_or_else(|| SqlError::internal("no manifest to append to"))?.append(&rec)?;
        self.checkpoint_gsn = gsn;
        Ok(segments.iter().filter(|(id, (_, l))| **id < active && *l == 0).map(|(id, _)| *id).collect())
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

    /// An independent version of every tree as of now, for another thread:
    /// it shares the loaded nodes (copied on write on either side), the
    /// open segment files and the append side, has a buffer pool of its own
    /// and cannot take checkpoints. Its pool counts only its own nodes
    /// against its target: those it loads, creates or modifies, and those
    /// it inherited that nobody else holds any more; it writes and evicts
    /// them like any pool. Until it is dropped, the segments it may read
    /// and those it writes to stay on disk, also once dead. Trees created
    /// empty here and filled in a fork can be moved back with
    /// `adopt_trees`.
    pub fn fork(&self) -> Result<Env> {
        let mut slots = self.slots.clone();
        for s in slots.iter_mut().flatten() {
            s.inherited = true;
        }
        Ok(Env {
            dir: self.dir.clone(),
            opts: self.opts.clone(),
            segs: self.segs.fork(),
            slots,
            free_slots: self.free_slots.clone(),
            mem_bytes: self.mem_bytes,
            dirty_bytes: self.dirty_bytes,
            clock_hand: 0,
            evict_stall: None,
            swept_slots: 0,
            work: self.work.clone(),
            trees: self.trees.clone(),
            next_tree_id: self.next_tree_id,
            seg_live: self.seg_live.clone(),
            checkpoint_seq: self.checkpoint_seq,
            checkpoint_gsn: self.checkpoint_gsn,
            fg_appended: 0,
            legacy_format: self.legacy_format,
            manifest: None,
            stamp: self.stamp,
            flushed: None,
            reads: 0,
            read_bytes: 0,
            read_budget: None,
            frozen_at: None,
            inherited: (self.mem_bytes, self.dirty_bytes),
            held: crate::memory::Charge::none(),
        })
    }

    /// Move trees `ids` from fork `from`, where they were filled, into this
    /// environment, where they must still be as empty as when the fork was
    /// taken. Their loaded nodes are moved, not copied, dirty or clean;
    /// those the fork wrote (all of them in the fork's own segments, which
    /// it pins) count as live here from now on.
    pub fn adopt_trees(&mut self, from: &mut Env, ids: &[TreeId]) -> Result<()> {
        // (the totals of the segments the fork wrote to first)
        self.count_foreign();
        for &id in ids {
            if self.trees.get(&id).is_none_or(|t| t.root.is_some()) {
                return Err(SqlError::internal(format!("tree {id} to adopt is not an empty tree here")));
            }
            self.adopt_tree(from, id, id)?;
        }
        Ok(())
    }

    /// Move temporary tree `id` from fork `from`, where it was created and
    /// filled, into this environment, under a temporary id of its own
    /// (returned): the fork and this environment take temporary ids
    /// independently.
    pub fn adopt_temp_tree(&mut self, from: &mut Env, id: TreeId) -> Result<TreeId> {
        self.count_foreign();
        let summarizer = from.trees.get(&id).ok_or_else(|| SqlError::internal(format!("no tree {id} to adopt")))?.summarizer.clone();
        let into = self.create_temp_tree(summarizer);
        self.adopt_tree(from, id, into)?;
        Ok(into)
    }

    /// Move fork `from`'s tree `id` here as tree `into` (empty here).
    fn adopt_tree(&mut self, from: &mut Env, id: TreeId, into: TreeId) -> Result<()> {
        let t = from.trees.remove(&id).ok_or_else(|| SqlError::internal(format!("no tree {id} to adopt")))?;
        let root = match t.root {
            None => None,
            Some(NodePtr::Disk(loc)) => Some(NodePtr::Disk(loc)),
            Some(NodePtr::Mem(fid)) => Some(NodePtr::Mem(self.adopt_node(from, fid, NONE, into))),
        };
        for (seg, live) in &t.seg_live {
            self.seg_live.entry(*seg).or_insert((0, 0)).1 += live;
        }
        self.trees.insert(into, TreeState { root, ..t });
        Ok(())
    }

    fn adopt_node(&mut self, from: &mut Env, fid: u32, parent: u32, tree: TreeId) -> u32 {
        from.own(fid);
        let mut slot = from.slots[fid as usize].take().expect("dangling node slot");
        from.free_slots.push(fid);
        from.mem_bytes -= slot.mem;
        if slot.dirty {
            from.dirty_bytes -= slot.mem;
        }
        slot.parent = parent;
        slot.tree = tree;
        slot.version = self.next_stamp();
        slot.edits = 0;
        self.mem_bytes += slot.mem;
        if slot.dirty {
            self.dirty_bytes += slot.mem;
        }
        let loaded: Vec<(usize, u32)> = match slot.node.as_deref() {
            Some(Node::Internal(ch)) => ch
                .iter()
                .enumerate()
                .filter_map(|(i, c)| match c.ptr {
                    NodePtr::Mem(cid) => Some((i, cid)),
                    NodePtr::Disk(_) => None,
                })
                .collect(),
            _ => Vec::new(),
        };
        slot.loaded_children = loaded.len() as u32;
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
        if !loaded.is_empty() {
            let mut moved = Vec::with_capacity(loaded.len());
            for (i, cid) in loaded {
                moved.push((i, self.adopt_node(from, cid, id, tree)));
            }
            let Node::Internal(ch) = self.node_mut(id) else { unreachable!() };
            for (i, m) in moved {
                ch[i].ptr = NodePtr::Mem(m);
            }
        }
        id
    }

    // ------------------------------------------------------------------
    // maintenance: segment GC and merging

    /// One maintenance pass with the default budget: `MAINT_MIN_BYTES`
    /// plus the bytes written for the engine's own updates since the
    /// previous pass (garbage arises at that rate, so reclaiming keeps pace
    /// with it). Run before a checkpoint, which then writes the nodes it
    /// rewrote.
    pub fn maintain(&mut self) -> Result<MaintStats> {
        let written = std::mem::take(&mut self.fg_appended);
        let gc = MAINT_MIN_BYTES.saturating_add(written);
        // the nodes it rewrites are dirty until the checkpoint after it
        // writes them: what the pool's target leaves besides the dirty
        // nodes it holds (which the pool keeps within half of the target)
        let memory = self.opts.cache_bytes.saturating_sub(self.dirty_bytes);
        self.maintain_with(MaintBudget { gc, memory })
    }

    /// A maintenance pass: rewrite the live nodes of the segments chosen
    /// for evacuation, and merge those of them that are underfull with a
    /// neighbour (a leaf written by a version that kept history is smaller
    /// once its retracted entries are dropped). Only subtrees that hold
    /// nodes of a chosen segment are visited; reading and rewriting clean
    /// nodes stops when the budget is exhausted (the next pass continues).
    /// No summary changes: relocating and merging keep the entries.
    pub fn maintain_with(&mut self, budget: MaintBudget) -> Result<MaintStats> {
        // (segments' totals with what forks wrote, for choosing victims)
        self.count_foreign();
        let (victims, left_out) = self.gc_victims(budget.gc);
        let mut m = Maint {
            stats: MaintStats { victims: victims.clone(), unfinished: left_out, ..Default::default() },
            victims,
            gc: budget.gc,
            memory: budget.memory,
            leaf_depth: None,
        };
        for tid in self.tree_ids() {
            let t = &self.trees[&tid];
            let Some(root) = t.root else { continue };
            // (trees not attached after recovery are about to be dropped,
            // temporary ones are dropped by their users)
            if !t.attached || tid >= TEMP_TREES || !t.seg_live.keys().any(|s| m.victim(*s)) {
                continue;
            }
            if let NodePtr::Disk(loc) = root
                && !m.charge(loc.len as u64)
            {
                continue;
            }
            let (old, summ) = (t.summary.clone(), t.summarizer.clone());
            let rid = self.root_slot(tid)?.expect("tree with a root");
            m.leaf_depth = None;
            if let Visit::New(parts) = self.maint_node(rid, 0, &old, &mut m, &summ)? {
                self.install_root(tid, parts, &summ)?;
            }
        }
        self.maybe_evict()?;
        Ok(m.stats)
    }

    /// Sealed segments to evacuate: those less than half live, in order of
    /// the cost-benefit of cleaning them — free space gained per byte moved
    /// (1 − u) / (1 + u), weighted by age (cold data stays put once moved,
    /// so old segments are worth more) — while their live bytes fit in
    /// `budget` (the first one always does, unless `budget` is 0). Also
    /// whether a segment to evacuate was left out.
    fn gc_victims(&self, budget: u64) -> (Vec<u32>, bool) {
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
        let (mut out, mut sum, mut left_out) = (Vec::new(), 0u64, false);
        for (_, id, live) in cands {
            if budget == 0 || (!out.is_empty() && sum.saturating_add(live) > budget) {
                left_out = true;
                continue;
            }
            out.push(id);
            sum += live;
        }
        out.sort_unstable();
        (out, left_out)
    }

    /// Visit node `id` of a maintenance pass (`depth` below the root; `old`
    /// is its summary as recorded by its parent).
    fn maint_node(&mut self, id: u32, depth: usize, old: &Summary, m: &mut Maint, summ: &Arc<dyn Summarizer>) -> Result<Visit> {
        let dirty = self.slot(id).dirty;
        let bytes = self.slot(id).bytes as u64;
        // (a dirty node has no location: it is written to the active
        // segment anyway)
        let in_victim = self.slot(id).disk.is_some_and(|l| m.victim(l.seg));
        if self.node(id).is_leaf() {
            m.leaf_depth = Some(depth);
            if !in_victim || !m.charge(bytes) {
                return Ok(Visit::Same);
            }
            m.stats.relocated += 1;
            self.make_dirty(id);
            self.slot_mut(id).by_maint = true;
            // (a leaf of an earlier version whose entries were all retracted)
            if matches!(self.node(id), Node::Leaf(e) if e.is_empty()) {
                self.free_slot(id);
                return Ok(Visit::New(Parts::new()));
            }
            m.dirtied(self.slot(id).mem);
            return Ok(Visit::New(smallvec::smallvec![Part { sep: Box::default(), id, summary: old.clone(), min_seg: u32::MAX }]));
        }
        let relocate = in_victim && m.charge(bytes);
        let tree = self.slot(id).tree;
        let node = self.take_node(id);
        // what the children own, kept up to date like the byte size
        let mut heap = self.slot(id).mem - SLOT_BYTES - node.mem_base();
        let Node::Internal(mut children) = node else { unreachable!() };
        // a placeholder, so that loading children counts them on this slot
        self.put_node(id, Node::Internal(Vec::new()));
        let mut size = bytes as usize;
        let mut changed = relocate;
        // the children rewritten (merged with a neighbour if underfull)
        let mut moved: smallvec::SmallVec<[u32; 4]> = smallvec::SmallVec::new();
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
            if !gc_self && !gc_below {
                i += 1;
                continue;
            }
            if let NodePtr::Disk(loc) = c.ptr
                && !m.charge(loc.len as u64)
            {
                i += 1;
                continue;
            }
            let cid = self.load(c.ptr, id, tree)?;
            children[i].ptr = NodePtr::Mem(cid);
            match self.maint_node(cid, depth + 1, &children[i].summary, m, summ)? {
                Visit::Same => i += 1,
                Visit::New(parts) => {
                    changed = true;
                    if let [p] = &parts[..] {
                        moved.push(p.id);
                    }
                    // the first part keeps the child's separator
                    size -= node::child_size(&children[i]);
                    heap -= child_heap(&children[i]);
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
                    heap += children[i..i + n].iter().map(child_heap).sum::<usize>();
                    i += n;
                }
            }
        }
        if !moved.is_empty() {
            m.stats.merged += self.merge_underfull(id, &mut children, &moved, &mut size, &mut heap, &mut m.gc, true)?;
            // (merges are tried only while there is budget)
            m.stats.unfinished |= m.gc == 0;
        }
        if !changed {
            self.put_node_sized(id, Node::Internal(children), size, heap);
            return Ok(Visit::Same);
        }
        if relocate {
            m.stats.relocated += 1;
        }
        if !dirty {
            // the path to a rewritten node is rewritten too
            self.make_dirty(id);
            self.slot_mut(id).by_maint = true;
            m.dirtied(self.slot(id).mem);
        }
        if children.is_empty() {
            self.slot_mut(id).loaded_children = 0;
            self.free_slot(id);
            return Ok(Visit::New(Parts::new()));
        }
        Ok(Visit::New(self.finish_internal_sized(id, children, Some(old.clone()), None, summ, size, heap, false)?))
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
        s.bytes_read = self.read_bytes;
        s.swept_slots = self.swept_slots;
        s
    }

    /// Node frames this environment has read from segments.
    pub fn segment_reads(&self) -> u64 {
        self.reads
    }

    pub fn dirty_bytes(&self) -> usize {
        debug_assert_eq!(self.dirty_bytes, self.slots.iter().flatten().filter(|s| s.dirty).map(|s| s.mem).sum::<usize>());
        self.dirty_bytes
    }
}

/// Split a sorted vector into chunks of about `target` bytes, each with at
/// least `min_items` items, balancing chunk sizes. After an append past the
/// node's previous last key (`append`), all chunks but the last are filled
/// to 90% instead: the appends that follow go to the last one, so the others
/// would stay half empty (the 10% leave room for later inserts).
fn split_by_bytes<T>(items: Vec<T>, target: usize, size: fn(&T) -> usize, min_items: usize, append: bool) -> Vec<Vec<T>> {
    let total: usize = items.iter().map(size).sum();
    if total <= target || items.len() < 2 * min_items.max(1) {
        return vec![items];
    }
    // A batch spread over many nodes (a bulk load) leaves room in each, so
    // that the next inserts do not split full nodes right away.
    let fill = if total > 2 * target || append { target - target / 10 } else { target };
    let nchunks = total.div_ceil(fill).max(2).min(items.len() / min_items.max(1));
    let per = if append { fill } else { total.div_ceil(nchunks) };
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

/// Bytes of node frames a maintenance pass may read and rewrite, besides
/// the share proportional to the engine's writes (see `Env::maintain`).
const MAINT_MIN_BYTES: u64 = 8 << 20;

/// Work a maintenance pass may do: bytes of clean node frames it may read
/// or rewrite (the next checkpoint writes them).
#[derive(Clone, Copy, Debug)]
pub struct MaintBudget {
    /// For evacuating sparse segments and merging what they leave
    /// underfull (it also bounds the live bytes of the segments chosen).
    pub gc: u64,
    /// Memory (`Slot::mem`) that the clean nodes the pass rewrites may take:
    /// they stay in the buffer pool, dirty, until the checkpoint writes
    /// them. No node is read for rewriting once it is used up.
    pub memory: usize,
}

impl MaintBudget {
    pub const UNLIMITED: MaintBudget = MaintBudget { gc: u64::MAX, memory: usize::MAX };
}

/// What a maintenance pass did.
#[derive(Clone, Debug, Default)]
pub struct MaintStats {
    /// Clean nodes rewritten because their segment is being evacuated.
    pub relocated: usize,
    /// Merges of underfull neighbours.
    pub merged: usize,
    /// Segments chosen for evacuation.
    pub victims: Vec<u32>,
    /// The pass left work it found for lack of budget (nodes it did not
    /// read or rewrite, merges it did not try, segments it did not choose):
    /// the next pass continues it.
    pub unfinished: bool,
}

/// State of a maintenance pass.
struct Maint {
    /// Segments being evacuated, sorted.
    victims: Vec<u32>,
    /// Remaining budgets.
    gc: u64,
    memory: usize,
    /// Depth of the leaves of the tree being visited, once one was seen.
    leaf_depth: Option<usize>,
    stats: MaintStats,
}

impl Maint {
    fn victim(&self, seg: u32) -> bool {
        self.victims.binary_search(&seg).is_ok()
    }

    /// Take `bytes` from the budget; false (taking nothing) once it is
    /// exhausted, or the memory for rewritten nodes is.
    fn charge(&mut self, bytes: u64) -> bool {
        if self.gc == 0 || self.memory == 0 {
            self.stats.unfinished = true;
            return false;
        }
        self.gc = self.gc.saturating_sub(bytes);
        true
    }

    /// The pass made a clean node of `mem` bytes of memory dirty.
    fn dirtied(&mut self, mem: usize) {
        self.memory = self.memory.saturating_sub(mem);
    }
}

/// Outcome of visiting a node in a maintenance pass.
enum Visit {
    /// Nothing changed.
    Same,
    /// The nodes now covering its key range: itself rewritten, or none.
    New(Parts),
}

/// Shortest key `s` with `a < s <= b` (for `a < b`): B+tree suffix truncation.
/// A range summary being computed: the entries at the range's edges folded
/// plainly, before and after the stored summaries of the subtrees inside the
/// range, combined.
struct RangeParts {
    left: Summary,
    inside: Summary,
    right: Summary,
    /// A subtree inside the range has been combined: later entries are right
    /// of it.
    past_left: bool,
}

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

/// Summary of leaf entries in the form node summaries hold: when a MIN/MAX
/// value is too wide for node summaries, the entries are summarized again
/// one by one, to see whether other values share its prefix
/// (`WideExtremes`).
fn leaf_summary(entries: &[LeafEntry], summ: &Arc<dyn Summarizer>) -> Result<Summary> {
    let mut s = Summary::empty(summ.spec());
    if summ.spec().is_empty() {
        s.count = entries.iter().map(|e| e.weight).sum();
        return Ok(s);
    }
    summ.add_entries(&mut s, &mut entries.iter().map(|e| (&*e.key, e.weight)))?;
    if let Some(mut wide) = summary::WideExtremes::of(&s) {
        for e in entries {
            let mut one = Summary::empty(summ.spec());
            summ.add_entry(&mut one, &e.key, e.weight)?;
            wide.note(&s, &one);
        }
        wide.apply(&mut s);
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
            s.bound_extremes();
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
thread_local! {
    /// Children scanned for an internal node's `min_seg` by
    /// `finish_internal_sized` (counted by tests).
    static SPACE_SCANS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
mod tests;
