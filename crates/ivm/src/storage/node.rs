//! B+tree node representation and on-disk serialization.

use super::summary::Summary;
use crate::error::{Result, SqlError};
use crate::memory::heap_block;
use crate::types::codec::{get_bytes, get_ivarint, get_u8, get_uvarint, put_bytes, put_ivarint, put_uvarint};
use std::mem::size_of;

/// Location of a serialized node frame in a segment file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DiskLoc {
    pub seg: u32,
    pub off: u64,
    pub len: u32,
}

impl DiskLoc {
    pub fn encode(&self, out: &mut Vec<u8>) {
        put_uvarint(out, self.seg as u64);
        put_uvarint(out, self.off);
        put_uvarint(out, self.len as u64);
    }
    pub fn decode(buf: &mut &[u8]) -> Result<DiskLoc> {
        Ok(DiskLoc { seg: get_uvarint(buf)? as u32, off: get_uvarint(buf)?, len: get_uvarint(buf)? as u32 })
    }
}

/// Reference from a parent to a child node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodePtr {
    Disk(DiskLoc),
    /// Loaded into the node arena (slot index).
    Mem(u32),
}

#[derive(Clone, Debug)]
pub struct LeafEntry {
    pub key: Box<[u8]>,
    /// The entry's weight: never 0 (an entry whose weight a change makes 0
    /// is removed).
    pub weight: i64,
}

#[derive(Clone, Debug)]
pub struct Child {
    /// Lower bound of the child's key range (ignored for the first child).
    pub sep: Box<[u8]>,
    pub ptr: NodePtr,
    pub summary: Summary,
    /// Oldest segment holding any node of this subtree (u32::MAX if none).
    pub min_seg: u32,
}

#[derive(Clone, Debug)]
pub enum Node {
    Leaf(Vec<LeafEntry>),
    Internal(Vec<Child>),
}

/// Leaf whose entries carry (GSN, weight) histories (decoded only: written
/// by versions that kept history; an entry's weight is its history's sum).
pub const KIND_LEAF_HISTORY: u8 = 1;
pub const KIND_INTERNAL: u8 = 2;
/// Internal node with each child's compaction state (decoded only, which
/// is ignored: written by versions that compacted histories).
pub const KIND_INTERNAL_DUE: u8 = 3;
pub const KIND_LEAF: u8 = 4;

impl Node {
    pub fn is_leaf(&self) -> bool {
        matches!(self, Node::Leaf(_))
    }

    pub fn len(&self) -> usize {
        match self {
            Node::Leaf(e) => e.len(),
            Node::Internal(c) => c.len(),
        }
    }

    /// Approximate serialized size, used for split decisions.
    pub fn byte_size(&self) -> usize {
        match self {
            Node::Leaf(entries) => entries.iter().map(entry_size).sum::<usize>() + 8,
            Node::Internal(children) => children.iter().map(child_size).sum::<usize>() + 8,
        }
    }

    /// Bytes the node occupies in memory: its vector of entries or children
    /// (by capacity) and the heap blocks they own. In memory a leaf entry
    /// whose serialized size is a few bytes takes a `LeafEntry` struct plus
    /// a block for its key; summaries take what their values own.
    pub fn mem_size(&self) -> usize {
        self.mem_base() + self.heap_size()
    }

    /// What the entries (`entry_heap`) or children (`child_heap`) own.
    pub fn heap_size(&self) -> usize {
        match self {
            Node::Leaf(entries) => entries.iter().map(entry_heap).sum(),
            Node::Internal(children) => children.iter().map(child_heap).sum(),
        }
    }

    /// The vector of entries or children: `mem_size` is this plus
    /// `heap_size` (which in-place edits maintain incrementally).
    pub fn mem_base(&self) -> usize {
        match self {
            Node::Leaf(entries) => heap_block(entries.capacity() * size_of::<LeafEntry>()),
            Node::Internal(children) => heap_block(children.capacity() * size_of::<Child>()),
        }
    }

    /// Serialize. Children must all have disk locations (`locs[i]`).
    pub fn encode(&self, child_locs: &[DiskLoc], out: &mut Vec<u8>) {
        self.encode_as(child_locs, out, false)
    }

    /// `encode`, in the formats of versions that kept history if `legacy`
    /// (tests of stores they wrote): leaves as kind 1, each weight a
    /// history of two changes, and internal nodes as kind 3, with a
    /// compaction state of 0.
    pub(crate) fn encode_as(&self, child_locs: &[DiskLoc], out: &mut Vec<u8>, legacy: bool) {
        match self {
            Node::Leaf(entries) => {
                out.push(if legacy { KIND_LEAF_HISTORY } else { KIND_LEAF });
                put_uvarint(out, entries.len() as u64);
                let mut prev: &[u8] = &[];
                for e in entries {
                    // prefix-compress keys against the previous key
                    let common = prev.iter().zip(e.key.iter()).take_while(|(a, b)| a == b).count();
                    put_uvarint(out, common as u64);
                    put_bytes(out, &e.key[common..]);
                    if legacy {
                        // [(1, w + 1), (2, -1)]: GSN deltas and weights
                        put_uvarint(out, 2);
                        put_uvarint(out, 1);
                        put_ivarint(out, e.weight + 1);
                        put_uvarint(out, 1);
                        put_ivarint(out, -1);
                    } else {
                        put_ivarint(out, e.weight);
                    }
                    prev = &e.key;
                }
            }
            Node::Internal(children) => {
                out.push(if legacy { KIND_INTERNAL_DUE } else { KIND_INTERNAL });
                put_uvarint(out, children.len() as u64);
                for (c, loc) in children.iter().zip(child_locs) {
                    put_bytes(out, &c.sep);
                    loc.encode(out);
                    put_uvarint(out, c.min_seg as u64);
                    if legacy {
                        put_uvarint(out, 1);
                    }
                    c.summary.encode(out);
                }
            }
        }
    }

    pub fn decode(mut buf: &[u8]) -> Result<Node> {
        let buf = &mut buf;
        match get_u8(buf)? {
            kind @ (KIND_LEAF | KIND_LEAF_HISTORY) => {
                let n = get_uvarint(buf)? as usize;
                let mut entries: Vec<LeafEntry> = Vec::with_capacity(n);
                let mut prev: Vec<u8> = Vec::new();
                for _ in 0..n {
                    let common = get_uvarint(buf)? as usize;
                    let suffix = get_bytes(buf)?;
                    if common > prev.len() {
                        return Err(SqlError::corrupt("bad key prefix in leaf"));
                    }
                    prev.truncate(common);
                    prev.extend_from_slice(suffix);
                    let weight = if kind == KIND_LEAF {
                        get_ivarint(buf)?
                    } else {
                        // a history of (GSN delta, weight) changes: its sum
                        // is the weight, and an entry of weight 0 (retracted,
                        // kept for reads of earlier GSNs) is no entry
                        let mut w = 0i64;
                        for _ in 0..get_uvarint(buf)? {
                            get_uvarint(buf)?;
                            w += get_ivarint(buf)?;
                        }
                        w
                    };
                    if weight != 0 {
                        entries.push(LeafEntry { key: prev.as_slice().into(), weight });
                    }
                }
                Ok(Node::Leaf(entries))
            }
            kind @ (KIND_INTERNAL | KIND_INTERNAL_DUE) => {
                let n = get_uvarint(buf)? as usize;
                let mut children = Vec::with_capacity(n);
                for _ in 0..n {
                    let sep = get_bytes(buf)?.to_vec().into_boxed_slice();
                    let loc = DiskLoc::decode(buf)?;
                    let min_seg = get_uvarint(buf)? as u32;
                    if kind == KIND_INTERNAL_DUE {
                        // (the subtree's compaction state: there are no
                        // histories to compact)
                        get_uvarint(buf)?;
                    }
                    let summary = Summary::decode(buf)?;
                    children.push(Child { sep, ptr: NodePtr::Disk(loc), summary, min_seg });
                }
                Ok(Node::Internal(children))
            }
            k => Err(SqlError::corrupt(format!("bad node kind {k}"))),
        }
    }
}

/// A leaf entry's estimated serialized size: its key, and 4 bytes for its
/// key's encoding and 2 for its weight.
pub fn entry_size(e: &LeafEntry) -> usize {
    e.key.len() + 6
}

/// The separator and summary as encoded, plus the child's location and
/// `min_seg`. (A node's byte size must follow every change of these:
/// merging two children combines their summaries.)
pub fn child_size(c: &Child) -> usize {
    c.sep.len() + 24 + c.summary.encoded_len()
}

/// Heap bytes a leaf entry owns: its key.
pub fn entry_heap(e: &LeafEntry) -> usize {
    heap_block(e.key.len())
}

/// Heap bytes a child pointer owns: its separator and its summary's values.
pub fn child_heap(c: &Child) -> usize {
    heap_block(c.sep.len()) + c.summary.heap_size()
}
