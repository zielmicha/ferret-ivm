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

use super::{CountSummarizer, Env, Gsn, TreeId};
use crate::error::Result;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::ops::Bound;
use std::sync::Arc;

/// Runs merged at once by default (each one's cursor buffers a leaf).
const FAN_IN: usize = 64;

/// Bytes a buffered entry costs besides its key.
const ENTRY_OVERHEAD: usize = 40;

pub struct TreeBuilder {
    target: TreeId,
    gsn: Gsn,
    buf: Vec<(Vec<u8>, i64)>,
    buf_bytes: usize,
    run_bytes: usize,
    fan_in: usize,
    runs: Vec<TreeId>,
    /// The last entry applied to the target directly.
    direct_max: Option<Vec<u8>>,
}

impl TreeBuilder {
    /// A builder adding entries to `target` at `gsn`. Its buffer is an
    /// eighth of the cache, within 1 and 64 MiB.
    pub fn new(env: &Env, target: TreeId, gsn: Gsn) -> TreeBuilder {
        TreeBuilder::with_limits(target, gsn, (env.cache_capacity() / 8).clamp(1 << 20, 64 << 20), FAN_IN)
    }

    /// A builder with the given buffer size and number of runs merged at once.
    pub fn with_limits(target: TreeId, gsn: Gsn, run_bytes: usize, fan_in: usize) -> TreeBuilder {
        TreeBuilder { target, gsn, buf: Vec::new(), buf_bytes: 0, run_bytes, fan_in: fan_in.max(2), runs: Vec::new(), direct_max: None }
    }

    pub fn push(&mut self, env: &mut Env, entry: Vec<u8>, w: i64) -> Result<()> {
        self.buf_bytes += entry.len() + ENTRY_OVERHEAD;
        self.buf.push((entry, w));
        if self.buf_bytes >= self.run_bytes {
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
        self.buf_bytes = 0;
        buf.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let (Some((first, _)), Some((last, _))) = (buf.first(), buf.last()) else { return Ok(()) };
        if self.direct_max.as_ref().is_none_or(|m| first > m) {
            self.direct_max = Some(last.clone());
            return env.apply(self.target, &mut buf, self.gsn);
        }
        let run = env.create_temp_tree(Arc::new(CountSummarizer));
        self.runs.push(run);
        env.apply(run, &mut buf, self.gsn)?;
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
        merge(env, &old, merged, self.gsn, self.run_bytes / 4)?;
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
        let r = merge(env, &self.runs, self.target, self.gsn, self.run_bytes / 4);
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
fn merge(env: &mut Env, runs: &[TreeId], into: TreeId, gsn: Gsn, batch_bytes: usize) -> Result<()> {
    let mut cursors: Vec<_> = runs.iter().map(|t| env.cursor(*t, Bound::Unbounded, Bound::Unbounded)).collect();
    let mut heads = BinaryHeap::with_capacity(runs.len());
    for (i, c) in cursors.iter_mut().enumerate() {
        if let Some((k, h)) = env.cursor_next(c)? {
            heads.push(Reverse((k, i, h.total())));
        }
    }
    let mut batch: Vec<(Vec<u8>, i64)> = Vec::new();
    let mut bytes = 0;
    while let Some(Reverse((k, i, w))) = heads.pop() {
        if let Some((nk, h)) = env.cursor_next(&mut cursors[i])? {
            heads.push(Reverse((nk, i, h.total())));
        }
        match batch.last_mut() {
            Some((last, lw)) if **last == *k => *lw += w,
            _ => {
                // batches end between keys, so equal entries meet in one
                if bytes >= batch_bytes {
                    env.apply(into, &mut batch, gsn)?;
                    bytes = 0;
                }
                bytes += k.len() + ENTRY_OVERHEAD;
                batch.push((k.into_vec(), w));
            }
        }
    }
    env.apply(into, &mut batch, gsn)
}
