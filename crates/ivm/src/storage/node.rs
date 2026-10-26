//! B+tree node representation and on-disk serialization.

use super::history::History;
use super::summary::Summary;
use super::Gsn;
use crate::error::{Result, SqlError};
use crate::types::codec::{get_bytes, get_ivarint, get_u8, get_uvarint, put_bytes, put_ivarint, put_uvarint};

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
    pub hist: History,
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

pub const KIND_LEAF: u8 = 1;
pub const KIND_INTERNAL: u8 = 2;

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

    /// Approximate serialized size, used for split decisions and cache accounting.
    pub fn byte_size(&self) -> usize {
        match self {
            Node::Leaf(entries) => entries.iter().map(entry_size).sum::<usize>() + 8,
            Node::Internal(children) => children.iter().map(child_size).sum::<usize>() + 8,
        }
    }

    /// Serialize. Children must all have disk locations (`locs[i]`).
    pub fn encode(&self, child_locs: &[DiskLoc], out: &mut Vec<u8>) {
        match self {
            Node::Leaf(entries) => {
                out.push(KIND_LEAF);
                put_uvarint(out, entries.len() as u64);
                let mut prev: &[u8] = &[];
                for e in entries {
                    // prefix-compress keys against the previous key
                    let common = prev.iter().zip(e.key.iter()).take_while(|(a, b)| a == b).count();
                    put_uvarint(out, common as u64);
                    put_bytes(out, &e.key[common..]);
                    put_uvarint(out, e.hist.0.len() as u64);
                    let mut last: Gsn = 0;
                    for (g, w) in e.hist.0.iter() {
                        put_uvarint(out, g - last);
                        put_ivarint(out, *w);
                        last = *g;
                    }
                    prev = &e.key;
                }
            }
            Node::Internal(children) => {
                out.push(KIND_INTERNAL);
                put_uvarint(out, children.len() as u64);
                for (c, loc) in children.iter().zip(child_locs) {
                    put_bytes(out, &c.sep);
                    loc.encode(out);
                    put_uvarint(out, c.min_seg as u64);
                    c.summary.encode(out);
                }
            }
        }
    }

    pub fn decode(mut buf: &[u8]) -> Result<Node> {
        let buf = &mut buf;
        match get_u8(buf)? {
            KIND_LEAF => {
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
                    let hn = get_uvarint(buf)? as usize;
                    let mut hist = History::default();
                    let mut last: Gsn = 0;
                    for _ in 0..hn {
                        last += get_uvarint(buf)?;
                        hist.0.push((last, get_ivarint(buf)?));
                    }
                    entries.push(LeafEntry { key: prev.clone().into_boxed_slice(), hist });
                }
                Ok(Node::Leaf(entries))
            }
            KIND_INTERNAL => {
                let n = get_uvarint(buf)? as usize;
                let mut children = Vec::with_capacity(n);
                for _ in 0..n {
                    let sep = get_bytes(buf)?.to_vec().into_boxed_slice();
                    let loc = DiskLoc::decode(buf)?;
                    let min_seg = get_uvarint(buf)? as u32;
                    let summary = Summary::decode(buf)?;
                    children.push(Child { sep, ptr: NodePtr::Disk(loc), summary, min_seg });
                }
                Ok(Node::Internal(children))
            }
            k => Err(SqlError::corrupt(format!("bad node kind {k}"))),
        }
    }
}

pub fn entry_size(e: &LeafEntry) -> usize {
    e.key.len() + 4 + e.hist.0.len() * 6
}

/// The separator and summary as encoded, plus the child's location and
/// `min_seg`.
pub fn child_size(c: &Child) -> usize {
    c.sep.len() + 24 + c.summary.encoded_len()
}
