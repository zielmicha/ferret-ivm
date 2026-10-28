//! Loading entries that arrive in arbitrary order into a tree in bounded
//! memory: an external merge sort whose runs are temporary trees.
//!
//! Applying unordered batches to a large tree would touch (and rewrite)
//! every leaf with every batch. A `TreeBuilder` instead buffers entries up
//! to a bound, stores each full buffer, sorted, as a run in a temporary tree
//! (whose nodes the buffer pool writes out and evicts like any others), and
//! finally merges the runs into the target in key order, so that each leaf
//! of the target is built once. Entries that arrive in key order (a buffer
//! wholly after everything before it) go to the target directly.

use super::{CountSummarizer, Env, TreeId};
use crate::error::Result;
use crate::memory::{heap_block, Charge, Work};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::ops::Bound;
use std::sync::Arc;

/// Runs merged at once by default (each one's cursor buffers a leaf).
const FAN_IN: usize = 64;

/// Memory of a buffered entry: its place in the buffer and its key's block.
fn entry_mem(key: &[u8]) -> usize {
    std::mem::size_of::<(Vec<u8>, i64)>() + heap_block(key.len())
}

pub struct TreeBuilder {
    target: TreeId,
    buf: Vec<(Vec<u8>, i64)>,
    /// The buffer's memory, charged to the working memory.
    charge: Charge,
    work: Work,
    run_bytes: usize,
    fan_in: usize,
    runs: Vec<TreeId>,
    /// The last entry applied to the target directly.
    direct_max: Option<Vec<u8>>,
}

impl TreeBuilder {
    /// A builder adding entries to `target`. Its buffer is a
    /// buffer of the environment's working memory (`Work::buffer`, within
    /// 1 and 64 MiB), charged to it, and is sorted out as a run once it is
    /// full (`Work::full`).
    pub fn new(env: &Env, target: TreeId) -> TreeBuilder {
        let work = env.work().clone();
        TreeBuilder::with_work(target, work.buffer(1 << 20, 64 << 20), FAN_IN, work)
    }

    /// A builder with the given buffer size and number of runs merged at
    /// once, and working memory of its own.
    pub fn with_limits(target: TreeId, run_bytes: usize, fan_in: usize) -> TreeBuilder {
        TreeBuilder::with_work(target, run_bytes, fan_in, Work::new(usize::MAX / 2))
    }

    fn with_work(target: TreeId, run_bytes: usize, fan_in: usize, work: Work) -> TreeBuilder {
        let charge = work.charge();
        TreeBuilder { target, buf: Vec::new(), charge, work, run_bytes, fan_in: fan_in.max(2), runs: Vec::new(), direct_max: None }
    }

    pub fn push(&mut self, env: &mut Env, entry: Vec<u8>, w: i64) -> Result<()> {
        self.charge.grow(entry_mem(&entry));
        self.buf.push((entry, w));
        if self.work.full(self.charge.bytes(), self.run_bytes) {
            self.spill(env)?;
        }
        Ok(())
    }

    /// Apply the buffer to the target if it lies wholly after what was
    /// applied to it so far (appending at its right edge: the first buffer,
    /// or input in key order or nearly so), else store it as a run; past
    /// twice the fan-in, merge the oldest ones into one (so that a merge
    /// never reads more runs than the fan-in at once). (A buffer applied directly that
    /// later runs overlap costs one more rewrite of its leaves.)
    fn spill(&mut self, env: &mut Env) -> Result<()> {
        let mut buf = std::mem::take(&mut self.buf);
        // (released once the buffer is applied)
        let held = std::mem::replace(&mut self.charge, self.work.charge());
        buf.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let (Some((first, _)), Some((last, _))) = (buf.first(), buf.last()) else { return Ok(()) };
        if self.direct_max.as_ref().is_none_or(|m| first > m) {
            self.direct_max = Some(last.clone());
            return env.apply(self.target, &mut buf);
        }
        let run = env.create_temp_tree(Arc::new(CountSummarizer));
        self.runs.push(run);
        env.apply(run, &mut buf)?;
        drop(held);
        if self.runs.len() >= 2 * self.fan_in {
            self.merge_oldest(env)?;
        }
        Ok(())
    }

    /// Merge the oldest runs (as many as the fan-in) into one.
    fn merge_oldest(&mut self, env: &mut Env) -> Result<()> {
        let merged = env.create_temp_tree(Arc::new(CountSummarizer));
        let old: Vec<TreeId> = self.runs.drain(..self.fan_in).collect();
        self.runs.push(merged);
        merge(env, &old, merged, self.run_bytes / 4)?;
        for t in old {
            env.drop_tree(t)?;
        }
        Ok(())
    }

    /// Apply every entry pushed to the target (equal entries consolidated)
    /// and drop the runs.
    pub fn finish(mut self, env: &mut Env) -> Result<()> {
        self.spill(env)?;
        if self.runs.is_empty() {
            return Ok(());
        }
        while self.runs.len() > self.fan_in {
            self.merge_oldest(env)?;
        }
        let r = merge(env, &self.runs, self.target, self.run_bytes / 4);
        self.abort(env)?;
        r
    }

    /// Drop the runs without applying anything (after a failure).
    pub fn abort(self, env: &mut Env) -> Result<()> {
        for t in self.runs {
            env.drop_tree(t)?;
        }
        Ok(())
    }
}

/// Merge sorted runs into `into` in key order, adding up equal entries,
/// in batches of about `batch_bytes`.
fn merge(env: &mut Env, runs: &[TreeId], into: TreeId, batch_bytes: usize) -> Result<()> {
    let mut cursors: Vec<_> = runs.iter().map(|t| env.cursor(*t, Bound::Unbounded, Bound::Unbounded)).collect();
    let mut heads = BinaryHeap::with_capacity(runs.len());
    for (i, c) in cursors.iter_mut().enumerate() {
        if let Some((k, w)) = env.cursor_next(c)? {
            heads.push(Reverse((k, i, w)));
        }
    }
    let mut batch: Vec<(Vec<u8>, i64)> = Vec::new();
    let mut bytes = 0;
    while let Some(Reverse((k, i, w))) = heads.pop() {
        if let Some((nk, nw)) = env.cursor_next(&mut cursors[i])? {
            heads.push(Reverse((nk, i, nw)));
        }
        match batch.last_mut() {
            Some((last, lw)) if **last == *k => *lw += w,
            _ => {
                // batches end between keys, so equal entries meet in one
                if bytes >= batch_bytes {
                    env.apply(into, &mut batch)?;
                    bytes = 0;
                }
                bytes += entry_mem(&k);
                batch.push((k.into_vec(), w));
            }
        }
    }
    env.apply(into, &mut batch)
}
