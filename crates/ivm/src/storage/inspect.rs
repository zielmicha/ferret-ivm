//! Read-only introspection of the buffer pool, for the web UI. A child
//! module so it can read the pool's private bookkeeping without widening
//! the storage API.

use super::{Env, TreeId};
use rustc_hash::FxHashMap;

/// Buffer-pool residency of one tree.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TreeResidency {
    /// Nodes of the tree currently in memory.
    pub nodes: usize,
    /// Bytes of memory of those nodes.
    pub bytes: usize,
    /// Nodes modified since the last checkpoint.
    pub dirty: usize,
}

impl Env {
    /// Resident nodes and bytes per tree, from one pass over the pool.
    pub fn tree_residency(&self) -> FxHashMap<TreeId, TreeResidency> {
        let mut out: FxHashMap<TreeId, TreeResidency> = FxHashMap::default();
        for slot in self.slots.iter().flatten() {
            let r = out.entry(slot.tree).or_default();
            if slot.node.is_some() {
                r.nodes += 1;
            }
            r.bytes += slot.mem;
            if slot.dirty {
                r.dirty += 1;
            }
        }
        out
    }

    /// Memory of the nodes currently in the buffer pool (O(1), unlike `stats`).
    pub fn resident_bytes(&self) -> usize {
        self.mem_bytes
    }

    /// The buffer pool's target: its current share of the memory budget.
    pub fn cache_capacity(&self) -> usize {
        self.opts.cache_bytes
    }
}
