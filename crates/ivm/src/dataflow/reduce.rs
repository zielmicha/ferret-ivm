//! Grouped aggregation as range queries over an augmented index.
//!
//! The input is arranged by the group key (then ORDER BY columns of ordered
//! aggregates); its tree nodes carry the summaries every aggregate needs, so
//! a group's aggregates are a range summary (O(log n)) regardless of whether
//! the aggregate is invertible. Ordered aggregates (string_agg, array_agg,
//! json_agg) rescan the group. The output is arranged by the group key and
//! supplies the previous value of each affected group.
//!
//! Recently touched groups keep their summary in memory, so a small change
//! to a large group folds the changed rows into the cached summary instead
//! of summarizing the group's range again. The cache is only an
//! accelerator: whenever a change cannot be folded in exactly (a MIN/MAX
//! loses its extreme, or a key representation may change) the group is
//! summarized from the index as before. All group caches of the graph share
//! one byte budget, their part of the memory budget.

use super::agg::AggSpec;
use super::*;
use crate::memory::{datum_heap, heap_block, CacheMeter};
use crate::storage::Summary;
use crate::types::datum::exact_cmp;
use crate::types::Datum;
use std::ops::Bound;

/// Summaries (and key values) of recently touched groups, current as of the
/// last step. Cloning a plan yields an empty cache.
#[derive(Default)]
pub struct GroupCache(parking_lot::Mutex<CacheState>);

/// A group cache's entries — each with its bytes in memory and the epoch
/// of the meter it was last used in — and its meter: the graph's
/// `group_caches`, which it is attached to when it is first used.
#[derive(Default)]
struct CacheState {
    groups: FxHashMap<Vec<u8>, (GroupState, usize, u64)>,
    bytes: usize,
    meter: Option<Arc<CacheMeter>>,
}

/// Memory of a cache entry: the map's slot (at its 7/8 load factor, with
/// its control byte) and what the key, summary and key values own.
fn entry_bytes(prefix: &[u8], (s, kv): &GroupState) -> usize {
    std::mem::size_of::<(Vec<u8>, (GroupState, usize, u64))>() * 8 / 7
        + 1
        + heap_block(prefix.len())
        + s.heap_size()
        + heap_block(kv.capacity() * std::mem::size_of::<Datum>())
        + kv.iter().map(datum_heap).sum::<usize>()
}

impl CacheState {
    fn attach(&mut self, meter: &Arc<CacheMeter>) {
        if self.meter.is_none() {
            self.meter = Some(meter.clone());
        }
    }

    fn remove(&mut self, prefix: &[u8]) -> Option<GroupState> {
        let (state, bytes, _) = self.groups.remove(prefix)?;
        self.release(bytes);
        Some(state)
    }

    /// Cache a group's state, unless the shared budget has no room for it
    /// (the meter is then squeezed, and every cache shrinks after the step).
    fn insert(&mut self, prefix: Vec<u8>, state: GroupState) {
        let Some(meter) = self.meter.clone() else { return };
        let bytes = entry_bytes(&prefix, &state);
        if !meter.fits(bytes) {
            meter.squeeze();
            return;
        }
        self.bytes += bytes;
        meter.add(bytes);
        if let Some((_, old, _)) = self.groups.insert(prefix, (state, bytes, meter.epoch())) {
            self.release(old);
        }
    }

    /// Evict the groups not used since the last shrink (in an epoch before
    /// `epoch`, the one ending), and if that leaves more than half of the
    /// cache, every other one of the rest (hash order is effectively random).
    fn shrink(&mut self, epoch: u64) {
        let before = self.bytes;
        let mut freed = 0usize;
        self.groups.retain(|_, (_, bytes, used)| {
            let keep = *used >= epoch;
            if !keep {
                freed += *bytes;
            }
            keep
        });
        if before - freed > before / 2 {
            let mut n = 0usize;
            self.groups.retain(|_, (_, bytes, _)| {
                n += 1;
                let keep = n % 2 == 1;
                if !keep {
                    freed += *bytes;
                }
                keep
            });
        }
        self.release(freed);
    }

    fn clear(&mut self) {
        self.groups.clear();
        self.release(self.bytes);
    }

    fn release(&mut self, bytes: usize) {
        self.bytes -= bytes;
        if let Some(m) = &self.meter {
            m.sub(bytes);
        }
    }
}

impl Drop for CacheState {
    fn drop(&mut self) {
        self.clear();
    }
}

/// Shrink every group cache when one of them could not take a group (or
/// the caches are over a lowered limit): groups not used since the last
/// shrink go first, so caches of aggregations that no longer change give
/// way to those in use; each cache loses at least half.
pub fn trim_caches(g: &Graph) {
    if !g.group_caches.take_squeezed() {
        return;
    }
    let epoch = g.group_caches.next_epoch();
    for n in g.live_nodes() {
        if let NodeKind::Reduce(p) = &n.kind {
            p.cache.0.lock().shrink(epoch);
        }
    }
}

impl Clone for GroupCache {
    fn clone(&self) -> Self {
        GroupCache::default()
    }
}

impl GroupCache {
    /// Memory of the cached groups.
    pub fn bytes(&self) -> usize {
        self.0.lock().bytes
    }
}

impl std::fmt::Debug for GroupCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("GroupCache")
    }
}

#[derive(Clone, Debug)]
pub struct ReducePlan {
    /// Input columns that form the group key, in key order.
    pub group_key: Vec<usize>,
    pub aggs: Vec<AggSpec>,
    /// Per aggregate: indexes of its summaries in the input layout.
    pub slots: Vec<Vec<usize>>,
    pub input_arr: ArrId,
    pub out_arr: ArrId,
    /// Groups currently in error (so the error retracts when they recover);
    /// checkpointed with the graph's error state.
    pub errors: super::OpErrors,
    pub cache: GroupCache,
}

impl ReducePlan {
    pub fn fingerprint(&self, cx: &FpCtx, fp: &mut Vec<u8>) {
        fp.extend_from_slice(format!("{:?}", self.group_key).as_bytes());
        for a in &self.aggs {
            fp.extend_from_slice(format!("{:?}{:?}{:?}{:?}{:?}{:?}", a.func, a.arg, a.arg2, a.filter, a.arg_type, a.fraction).as_bytes());
        }
        fp.extend_from_slice(cx.arr(self.input_arr));
    }

    fn ordered(&self) -> bool {
        self.aggs.iter().any(|a| a.func.is_ordered())
    }
}

fn group_range(prefix: &[u8]) -> (Bound<&[u8]>, Option<Vec<u8>>) {
    (Bound::Included(prefix), crate::types::encoding::prefix_successor(prefix))
}

type GroupResult = std::result::Result<Option<Row>, SqlError>;

/// A group's summary and key values, as kept in the group cache.
type GroupState = (Summary, Vec<Datum>);

/// Compute the output row of one group (None if the group is empty), with
/// the group's summary and key values for the cache.
///
/// `exact_kv`, when given, are the group's key values exactly as in its
/// first live entry (already read, or the key types have no
/// representations beyond the canonical key encoding), so that entry need
/// not be read again.
fn compute_group(
    g: &Graph,
    env: &mut Env,
    p: &ReducePlan,
    prefix: &[u8],
    exact_kv: Option<&[Datum]>,
) -> Result<(GroupResult, Option<GroupState>)> {
    let a = g.arr(p.input_arr);
    let layout = a.layout.clone();
    let tree = a.tree;
    let (lo, hi) = group_range(prefix);
    let hib = match &hi {
        Some(h) => Bound::Excluded(h.as_slice()),
        None => Bound::Unbounded,
    };
    let summary = if prefix.is_empty() {
        env.range_summary(tree, Bound::Unbounded, Bound::Unbounded)?
    } else {
        env.range_summary(tree, lo, hib)?
    };
    let ng = p.group_key.len();
    if ng > 0 && summary.count == 0 {
        return Ok((Ok(None), None));
    }
    // group key values from the first live entry (exact representation)
    let mut kv: Vec<Datum> = Vec::with_capacity(ng);
    if let Some(exact) = exact_kv
        && summary.count > 0
    {
        kv.extend_from_slice(exact);
    } else if ng > 0 {
        let mut first = None;
        env.for_each(tree, lo, hib, &mut |k, h| {
            if h.total() > 0 {
                first = Some(layout.decode_row(k)?);
                return Ok(false);
            }
            Ok(true)
        })?;
        let Some(first) = first else { return Ok((Ok(None), None)) };
        for c in &p.group_key {
            kv.push(first[*c].clone());
        }
    }
    // ordered aggregates scan the group once
    let mut scanned: Option<Vec<(Row, i64)>> = None;
    if p.ordered() {
        let mut rows = Vec::new();
        env.for_each(tree, lo, hib, &mut |k, h| {
            let w = h.total();
            if w > 0 {
                rows.push((layout.decode_row(k)?, w));
            }
            Ok(true)
        })?;
        scanned = Some(rows);
    }
    let mut row: Row = kv.clone();
    for (i, agg) in p.aggs.iter().enumerate() {
        let v = if agg.func.is_ordered() {
            if matches!(agg.func, crate::sql::plan::AggFunc::PercentileCont | crate::sql::plan::AggFunc::PercentileDisc)
                && agg.filter.is_none()
            {
                // order statistics through count summaries
                let st = &summary.aggs[p.slots[i][0]];
                let n = match st {
                    crate::storage::AggState::Count(c) => *c,
                    _ => 0,
                };
                let col = agg.arg.unwrap();
                // the n non-null values follow the group's NULLs when the
                // ORDER BY column sorts NULLs first (DESC, NULLS FIRST)
                let skip = if layout.specs.get(ng).is_some_and(|s| s.nulls_first) { summary.count - n } else { 0 };
                let mut get = |k: i64| -> Result<Datum> {
                    match env.nth(tree, lo, hib, skip + k)? {
                        Some((key, _, _)) => Ok(layout.decode_row(&key)?[col].clone()),
                        None => Ok(Datum::Null),
                    }
                };
                super::agg::percentile(agg.func, agg.fraction.unwrap_or(0.5), n, &mut get)
            } else {
                let rows = scanned.as_ref().unwrap();
                agg.finalize_ordered(&mut rows.iter().cloned())
            }
        } else {
            agg.finalize(&summary, &p.slots[i])
        };
        match v {
            Ok(d) => row.push(d),
            Err(e) if is_data_error(&e) => return Ok((Err(e), Some((summary, kv)))),
            Err(e) => return Err(e),
        }
    }
    Ok((Ok(Some(row)), Some((summary, kv))))
}

/// Fold a group's changed rows into its cached summary. False when that is
/// not exact and the group must be summarized from the index.
fn fold_rows(p: &ReducePlan, layout: &Layout, s: &mut Summary, kv: &[Datum], rows: &[(&Row, i64)], fidelity_key: bool) -> Result<bool> {
    for (row, w) in rows {
        let w = *w;
        if fidelity_key {
            // the group's key values come from its first entry, which a
            // retraction or another representation of the key may change
            if w < 0 || p.group_key.iter().zip(kv).any(|(c, v)| exact_cmp(&row[*c], v) != std::cmp::Ordering::Equal) {
                return Ok(false);
            }
        }
        s.count += w;
        for (i, (kind, e)) in layout.summaries.iter().enumerate() {
            let v = e.eval(row)?;
            // a retraction may remove the row's entry entirely
            let ok = if w > 0 { kind.update(&mut s.aggs[i], &v, 0, w)? } else { kind.update(&mut s.aggs[i], &v, -w, 0)? };
            if !ok {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// Output row of a group from its summary (non-ordered aggregates only).
fn finish_group(p: &ReducePlan, s: &Summary, kv: &[Datum]) -> Result<GroupResult> {
    if !p.group_key.is_empty() && s.count == 0 {
        return Ok(Ok(None));
    }
    let mut row: Row = kv.to_vec();
    for (i, agg) in p.aggs.iter().enumerate() {
        match agg.finalize(s, &p.slots[i]) {
            Ok(d) => row.push(d),
            Err(e) if is_data_error(&e) => return Ok(Err(e)),
            Err(e) => return Err(e),
        }
    }
    Ok(Ok(Some(row)))
}

fn same_row(a: &Option<Row>, b: &Option<Row>) -> bool {
    match (a, b) {
        (Some(x), Some(y)) => crate::types::datum::exact_row_cmp(x, y) == std::cmp::Ordering::Equal,
        (None, None) => true,
        _ => false,
    }
}

/// Errors caused by data (reported as error records) vs internal failures.
pub fn is_data_error(e: &SqlError) -> bool {
    !e.code.starts_with("XX") && !e.code.starts_with("58")
}

fn old_row(g: &Graph, env: &mut Env, p: &ReducePlan, gvals: &[Datum]) -> Result<Option<Row>> {
    let rows = g.lookup(env, p.out_arr, gvals, ReadAt::Latest)?;
    Ok(rows.into_iter().find(|(_, w)| *w > 0).map(|(r, _)| r))
}

pub fn step(g: &Graph, env: &mut Env, p: &ReducePlan, delta: &Delta, _gsn: Gsn, out: &mut Output) -> Result<()> {
    let r = step_groups(g, env, p, delta, out);
    if r.is_err() {
        // the cache may be ahead of what this failed step will leave behind
        p.cache.0.lock().clear();
    }
    r
}

fn step_groups(g: &Graph, env: &mut Env, p: &ReducePlan, delta: &Delta, out: &mut Output) -> Result<()> {
    let layout = g.arr(p.input_arr).layout.clone();
    let ng = p.group_key.len();
    // affected groups (canonical key prefixes) with their changed rows
    let mut groups: BTreeMap<Vec<u8>, (Vec<Datum>, Vec<(&Row, i64)>)> = BTreeMap::new();
    for (row, w) in delta {
        let kv: Vec<Datum> = p.group_key.iter().map(|c| row[*c].clone()).collect();
        let prefix = layout.encode_prefix(&kv);
        groups.entry(prefix).or_insert_with(|| (kv, Vec::new())).1.push((row, *w));
    }
    static NO_CACHE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    let cacheable = !p.ordered() && !*NO_CACHE.get_or_init(|| std::env::var("IVM_NO_GROUP_CACHE").is_ok());
    let fidelity_key = layout.key_types[..ng].iter().any(crate::types::encoding::has_fidelity);
    let mut errors = p.errors.lock();
    let mut cache = p.cache.0.lock();
    cache.attach(&g.group_caches);
    for (prefix, (kv, rows)) in groups {
        let mut old: Option<Option<Row>> = None;
        let mut result = None;
        if cacheable && let Some((mut s, ckv)) = cache.remove(&prefix) {
            // the cached summary also gives the group's current output
            if let Ok(o) = finish_group(p, &s, &ckv)? {
                old = Some(o);
            }
            if fold_rows(p, &layout, &mut s, &ckv, &rows, fidelity_key)? {
                result = Some(finish_group(p, &s, &ckv)?);
                if s.count != 0 || ng == 0 {
                    cache.insert(prefix.clone(), (s, ckv));
                }
            }
        }
        let old = match old {
            Some(o) => {
                #[cfg(debug_assertions)]
                {
                    let stored = old_row(g, env, p, &kv)?;
                    if !same_row(&stored, &o) {
                        panic!("group cache diverged for {kv:?}: cached {o:?}, stored {stored:?}, delta {rows:?}");
                    }
                }
                o
            }
            None => old_row(g, env, p, &kv)?,
        };
        let result = match result {
            Some(r) => r,
            None => {
                let (r, state) = compute_group(g, env, p, &prefix, (!fidelity_key).then_some(&kv[..]))?;
                if cacheable && let Some(state) = state {
                    cache.insert(prefix.clone(), state);
                }
                r
            }
        };
        match result {
            Ok(new) => {
                // exact comparison: a new representation is a change too
                if !same_row(&old, &new) {
                    if let Some(o) = old {
                        out.data.push((o, -1));
                    }
                    if let Some(n) = new {
                        out.data.push((n, 1));
                    }
                }
                if let Some(prev) = errors.remove(&prefix) {
                    out.errs.push((prev, -1));
                }
            }
            Err(e) => {
                // the group has no valid value: retract it and report the error
                if let Some(o) = old {
                    out.data.push((o, -1));
                }
                let rec = err_record(&e);
                match errors.insert(prefix, rec.clone()) {
                    Some(prev) if prev == rec => {}
                    Some(prev) => {
                        out.errs.push((prev, -1));
                        out.errs.push((rec, 1));
                    }
                    None => out.errs.push((rec, 1)),
                }
            }
        }
    }
    Ok(())
}

pub fn snapshot(g: &Graph, env: &mut Env, p: &ReducePlan, out: &mut Output) -> Result<()> {
    let a = g.arr(p.input_arr);
    let layout = a.layout.clone();
    let tree = a.tree;
    let ng = p.group_key.len();
    // the snapshot rebuilds the operator's state (hydration)
    let mut errors = p.errors.lock();
    errors.clear();
    if ng == 0 {
        match compute_group(g, env, p, &[], None)?.0 {
            Ok(Some(r)) => out.data.push((r, 1)),
            Ok(None) => {}
            Err(e) => {
                // remembered like any group's, so that it retracts on recovery
                let rec = err_record(&e);
                errors.insert(Vec::new(), rec.clone());
                out.errs.push((rec, 1));
            }
        }
        return Ok(());
    }
    // Enumerate groups in key order, seeking past each group's entries
    // instead of visiting them. The first live entry of a group gives its
    // key prefix and its exact key values.
    let mut groups: Vec<(Vec<u8>, Vec<Datum>)> = Vec::new();
    let mut from: Option<Vec<u8>> = None;
    loop {
        let lo = match &from {
            Some(f) => Bound::Included(f.as_slice()),
            None => Bound::Unbounded,
        };
        let mut next = None;
        env.for_each(tree, lo, Bound::Unbounded, &mut |k, h| {
            if h.total() <= 0 {
                return Ok(true);
            }
            let row = layout.decode_row(k)?;
            next = Some((k[..layout.prefix_len(k, ng)?].to_vec(), p.group_key.iter().map(|c| row[*c].clone()).collect()));
            Ok(false)
        })?;
        let Some((prefix, kv)) = next else { break };
        from = crate::types::encoding::prefix_successor(&prefix);
        groups.push((prefix, kv));
        if from.is_none() {
            break;
        }
    }
    for (prefix, kv) in groups {
        match compute_group(g, env, p, &prefix, Some(&kv))?.0 {
            Ok(Some(r)) => out.data.push((r, 1)),
            Ok(None) => {}
            Err(e) => {
                let rec = err_record(&e);
                errors.insert(prefix, rec.clone());
                out.errs.push((rec, 1));
            }
        }
    }
    Ok(())
}
