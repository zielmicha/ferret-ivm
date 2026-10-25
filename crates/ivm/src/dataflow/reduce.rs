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
//! summarized from the index as before.

use super::agg::AggSpec;
use super::*;
use crate::storage::Summary;
use crate::types::datum::exact_cmp;
use crate::types::Datum;
use std::ops::Bound;

/// Groups cached per Reduce; when full, half of them are evicted.
const GROUP_CACHE_CAP: usize = 1 << 18;

/// Summaries (and key values) of recently touched groups, current as of the
/// last step. Cloning a plan yields an empty cache.
#[derive(Default)]
pub struct GroupCache(parking_lot::Mutex<FxHashMap<Vec<u8>, (Summary, Vec<Datum>)>>);

impl Clone for GroupCache {
    fn clone(&self) -> Self {
        GroupCache::default()
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
    /// Groups currently in error (so the error retracts when they recover).
    pub errors: Arc<parking_lot::Mutex<BTreeMap<Vec<u8>, (String, String)>>>,
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

/// Compute the output row of one group (None if the group is empty), with
/// the group's summary and key values for the cache.
fn compute_group(g: &Graph, env: &mut Env, p: &ReducePlan, prefix: &[u8]) -> Result<(GroupResult, Option<(Summary, Vec<Datum>)>)> {
    let a = g.arr(p.input_arr);
    let layout = a.layout.clone();
    let tree = a.tree;
    let (lo, hi) = group_range(prefix);
    let hib = match &hi {
        Some(h) => Bound::Excluded(h.as_slice()),
        None => Bound::Unbounded,
    };
    let summary = if prefix.is_empty() { env.tree_summary(tree) } else { env.range_summary(tree, lo, hib)? };
    let ng = p.group_key.len();
    if ng > 0 && summary.count == 0 {
        return Ok((Ok(None), None));
    }
    // group key values from the first live entry (exact representation)
    let mut kv: Vec<Datum> = Vec::with_capacity(ng);
    if ng > 0 {
        let mut c = env.cursor(tree, lo, hib);
        let mut first = None;
        while let Some((k, h)) = env.cursor_next(&mut c)? {
            if h.total() > 0 {
                first = Some(layout.decode_row(&k)?);
                break;
            }
        }
        let Some(first) = first else { return Ok((Ok(None), None)) };
        for c in &p.group_key {
            kv.push(first[*c].clone());
        }
    }
    // ordered aggregates scan the group once
    let mut scanned: Option<Vec<(Row, i64)>> = None;
    if p.ordered() {
        let mut rows = Vec::new();
        let mut c = env.cursor(tree, lo, hib);
        while let Some((k, h)) = env.cursor_next(&mut c)? {
            let w = h.total();
            if w > 0 {
                rows.push((layout.decode_row(&k)?, w));
            }
        }
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
    let cacheable = !p.ordered() && std::env::var("IVM_NO_GROUP_CACHE").is_err();
    let fidelity_key = layout.key_types[..ng].iter().any(crate::types::encoding::has_fidelity);
    let mut errors = p.errors.lock();
    let mut cache = p.cache.0.lock();
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
                let (r, state) = compute_group(g, env, p, &prefix)?;
                if cacheable && let Some(state) = state {
                    if cache.len() >= GROUP_CACHE_CAP {
                        // evict about half (hash order is effectively random)
                        let mut n = 0usize;
                        cache.retain(|_, _| {
                            n += 1;
                            n % 2 == 0
                        });
                    }
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
    if ng == 0 {
        match compute_group(g, env, p, &[])?.0 {
            Ok(Some(r)) => out.data.push((r, 1)),
            Ok(None) => {}
            Err(e) => out.errs.push((err_record(&e), 1)),
        }
        return Ok(());
    }
    // enumerate groups in key order
    let mut prefixes: Vec<Vec<u8>> = Vec::new();
    let mut c = env.cursor(tree, Bound::Unbounded, Bound::Unbounded);
    while let Some((k, h)) = env.cursor_next(&mut c)? {
        if h.total() <= 0 {
            continue;
        }
        let row = layout.decode_row(&k)?;
        let kv: Vec<Datum> = p.group_key.iter().map(|c| row[*c].clone()).collect();
        let prefix = layout.encode_prefix(&kv);
        if prefixes.last() != Some(&prefix) {
            prefixes.push(prefix);
        }
    }
    let mut errors = p.errors.lock();
    for prefix in prefixes {
        match compute_group(g, env, p, &prefix)?.0 {
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
