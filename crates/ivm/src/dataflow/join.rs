//! Delta joins.
//!
//! An n-way join keeps no intermediate state: for each input i, its delta is
//! extended by a chain of lookups into the other inputs' arrangements (the
//! "delta path" for i). With inputs ordered 0..n, input j is read *after*
//! the current GSN if j < i and *before* it if j > i, which is the standard
//! delta-query decomposition of d(R0 x ... x Rn). Lookup order per path is
//! chosen by the planner to keep intermediate results small.
//!
//! Semijoin inputs are "presence" inputs: their rows are the distinct keys
//! of the underlying collection, read through count summaries.

use super::mfp::Mfp;
use super::*;
use crate::types::datum::exact_cmp;
use crate::types::Datum;

#[derive(Clone, Debug)]
pub struct JoinInput {
    pub node: NodeId,
    /// Applied to the node's rows (deltas and lookup results).
    pub mfp: Mfp,
    /// Output width of this input within the joined row.
    pub width: usize,
    pub offset: usize,
    /// For presence inputs: arrangement keyed by the presence key; the
    /// input's rows are the key values (width = key length).
    pub presence: Option<ArrId>,
    /// Presence matches key values exactly, including the representation
    /// of equal values (numeric scale, -0.0, ...), rather than by equality.
    pub exact: bool,
}

#[derive(Clone, Debug)]
pub struct LookupStep {
    pub input: usize,
    pub arr: ArrId,
    /// Key expressions over the joined row (only bound columns).
    pub key: Vec<Expr>,
    /// Per key column: does NULL match NULL (IS NOT DISTINCT FROM)?
    pub null_safe: Vec<bool>,
    /// Predicates over the joined row checked after this step.
    pub filters: Vec<Expr>,
    /// Keys must be identical, including the representation of equal
    /// values (lookups keyed by the values of an exact presence input).
    pub exact: bool,
    /// Columns of the looked-up entries the input's MFP (and an exact
    /// match) reads, computed on first use; not part of the plan's identity.
    pub decode: std::sync::OnceLock<Option<super::layout::PartialDecode>>,
}

#[derive(Clone, Debug)]
pub struct DeltaPath {
    pub source: usize,
    pub source_filters: Vec<Expr>,
    pub steps: Vec<LookupStep>,
}

/// Counts the expression errors of a regular input's MFP (which may fail)
/// once per input row: an Mfp node over the input's node, an input of the
/// join, applying the MFP and keeping no rows (`arr`, its arrangement, stays
/// empty; it makes the node's error records authoritative).
#[derive(Clone, Debug)]
pub struct JoinTracker {
    pub input: usize,
    pub node: NodeId,
    pub arr: ArrId,
}

#[derive(Clone, Debug)]
pub struct JoinPlan {
    pub inputs: Vec<JoinInput>,
    pub paths: Vec<DeltaPath>,
    pub width: usize,
    /// Error trackers of the inputs whose MFP may fail (the others' cannot).
    pub trackers: Vec<JoinTracker>,
}

impl JoinPlan {
    pub fn arrangements(&self) -> Vec<ArrId> {
        let mut v: Vec<ArrId> = Vec::new();
        for p in &self.paths {
            for s in &p.steps {
                v.push(s.arr);
            }
        }
        for i in &self.inputs {
            if let Some(a) = i.presence {
                v.push(a);
            }
        }
        v.extend(self.trackers.iter().map(|t| t.arr));
        v
    }

    /// Are input `i`'s expression errors counted by an error tracker?
    fn tracked(&self, i: usize) -> bool {
        self.trackers.iter().any(|t| t.input == i)
    }

    pub fn fingerprint(&self, cx: &FpCtx, fp: &mut Vec<u8>) {
        for i in &self.inputs {
            fp.extend_from_slice(cx.node(i.node));
            i.mfp.fingerprint(fp);
            match i.presence {
                Some(a) => {
                    fp.extend_from_slice(cx.arr(a));
                    if i.exact {
                        fp.push(0xE0);
                    }
                }
                None => fp.push(0),
            }
        }
        for p in &self.paths {
            fp.push(0xA0);
            fp.extend_from_slice(&(p.source as u32).to_le_bytes());
            for f in &p.source_filters {
                f.fingerprint(fp);
            }
            for s in &p.steps {
                fp.push(0xA1);
                fp.extend_from_slice(&(s.input as u32).to_le_bytes());
                fp.extend_from_slice(cx.arr(s.arr));
                for k in &s.key {
                    k.fingerprint(fp);
                }
                fp.extend(s.null_safe.iter().map(|b| *b as u8));
                for f in &s.filters {
                    f.fingerprint(fp);
                }
                if s.exact {
                    fp.push(0xE1);
                }
            }
        }
    }
}

/// Presence count of a key under `at` (count summaries; "before" derived from
/// this tick's delta of the presence collection).
fn presence_count(env: &mut Env, g: &Graph, arr: ArrId, prefix: &[u8], at: ReadAt, delta: Option<&FxHashMap<Vec<u8>, i64>>) -> Result<i64> {
    let a = g.arr(arr);
    let hi = crate::types::encoding::prefix_successor(prefix);
    let s = match &hi {
        Some(h) => env.range_summary(a.tree, std::ops::Bound::Included(prefix), std::ops::Bound::Excluded(h))?,
        None => env.range_summary(a.tree, std::ops::Bound::Included(prefix), std::ops::Bound::Unbounded)?,
    };
    let latest = s.count;
    Ok(match at {
        ReadAt::Latest => latest,
        ReadAt::Before(_) => latest - delta.and_then(|d| d.get(prefix)).copied().unwrap_or(0),
        ReadAt::AsOf(_) => latest,
    })
}

/// Key of presence values in their arrangement: the canonical key prefix,
/// or for exact presence the whole entry (values with their representation;
/// presence arrangements are keyed by all columns, so it has no payload).
fn presence_prefix(layout: &Layout, vals: &[Datum], exact: bool) -> Result<Vec<u8>> {
    if exact { layout.encode_row(vals) } else { Ok(layout.encode_prefix(vals)) }
}

struct Ctx<'a> {
    g: &'a Graph,
    p: &'a JoinPlan,
    pres_delta: Vec<Option<FxHashMap<Vec<u8>, i64>>>,
}

/// Key values of a lookup step for a joined row (None: matches nothing).
fn lookup_key(step: &LookupStep, row: &[Datum]) -> Option<Vec<Datum>> {
    let mut key = Vec::with_capacity(step.key.len());
    for (k, safe) in step.key.iter().zip(&step.null_safe) {
        let v = k.eval(row).ok()?;
        if v.is_null() && !safe {
            return None;
        }
        key.push(v);
    }
    Some(key)
}

/// Rows of input `step.input` matching the key values `key`, encoded as
/// `prefix` (post-MFP); for presence inputs, whether the key is present
/// (the row is then the key).
fn lookup_prefix(env: &mut Env, cx: &Ctx, step: &LookupStep, prefix: &[u8], key: &[Datum], at: ReadAt) -> Result<Vec<(Row, i64)>> {
    let inp = &cx.p.inputs[step.input];
    if let Some(parr) = inp.presence {
        let c = presence_count(env, cx.g, parr, prefix, at, cx.pres_delta[step.input].as_ref())?;
        return Ok(if c > 0 { vec![(Vec::new(), 1)] } else { vec![] });
    }
    let a = cx.g.arr(step.arr);
    let layout = &a.layout;
    // only the columns the input's MFP (and an exact match) reads are decoded
    let plan = step
        .decode
        .get_or_init(|| {
            let mut cols = inp.mfp.input_columns();
            if step.exact {
                cols.extend(layout.key.iter().flat_map(|k| k.column_set()));
            }
            layout.decode_plan(&cols)
        })
        .as_ref();
    let mut out = Vec::new();
    env.for_each_prefix(a.tree, prefix, &mut |k, h| {
        let w = h.weight(at);
        if w <= 0 {
            return Ok(true);
        }
        let r = layout.decode_row_with(k, plan)?;
        if step.exact && layout.key_values(&r)?.iter().zip(key).any(|(a, b)| exact_cmp(a, b) != std::cmp::Ordering::Equal) {
            return Ok(true);
        }
        if let Ok(Some(post)) = inp.mfp.eval(&r) {
            out.push((post, w));
        }
        Ok(true)
    })?;
    Ok(out)
}

fn run_path(env: &mut Env, cx: &Ctx, path: &DeltaPath, start: Vec<(Row, i64)>, gsn: Option<Gsn>, out: &mut Delta) -> Result<()> {
    let mut frontier: Vec<(Row, i64)> = Vec::with_capacity(start.len());
    for (row, w) in start {
        if path.source_filters.iter().all(|f| f.eval_pred(&row).unwrap_or(false)) {
            frontier.push((row, w));
        }
    }
    for step in &path.steps {
        if frontier.is_empty() {
            return Ok(());
        }
        let at = match gsn {
            None => ReadAt::Latest,
            Some(g) => {
                if step.input < path.source {
                    ReadAt::Latest
                } else {
                    ReadAt::Before(g)
                }
            }
        };
        let inp = &cx.p.inputs[step.input];
        // Rows whose keys encode equally share one lookup: (grouping key,
        // length of the lookup key in it, row index, key values). Exact
        // steps group by the values' exact encoding too.
        let mut keyed: Vec<(Vec<u8>, usize, usize, Vec<Datum>)> = Vec::with_capacity(frontier.len());
        for (i, (row, _)) in frontier.iter().enumerate() {
            let Some(key) = lookup_key(step, row) else { continue };
            let mut k = match inp.presence {
                Some(parr) => presence_prefix(&cx.g.arr(parr).layout, &key, inp.exact)?,
                None => cx.g.arr(step.arr).layout.encode_prefix(&key),
            };
            let n = k.len();
            if step.exact && inp.presence.is_none() {
                crate::types::codec::encode_row(&key, &mut k);
            }
            keyed.push((k, n, i, key));
        }
        if keyed.len() > 1 {
            keyed.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.2.cmp(&b.2)));
        }
        let mut next = Vec::new();
        let mut j = 0;
        while j < keyed.len() {
            let mut end = j + 1;
            while end < keyed.len() && keyed[end].0 == keyed[j].0 {
                end += 1;
            }
            let (k, n, _, key) = &keyed[j];
            let matches = lookup_prefix(env, cx, step, &k[..*n], key, at)?;
            for (_, _, i, key) in &keyed[j..end] {
                let (row, w) = &frontier[*i];
                for (post, mw) in &matches {
                    let mut r = row.clone();
                    // a presence input's row is the probing key itself
                    let post = if inp.presence.is_some() { key } else { post };
                    r[inp.offset..inp.offset + inp.width].clone_from_slice(post);
                    if step.filters.iter().all(|f| f.eval_pred(&r).unwrap_or(false)) {
                        next.push((r, w * mw));
                    }
                }
            }
            j = end;
        }
        frontier = next;
    }
    out.extend(frontier);
    Ok(())
}

fn presence_deltas(g: &Graph, p: &JoinPlan, all: &FxHashMap<NodeId, Output>) -> Result<Vec<Option<FxHashMap<Vec<u8>, i64>>>> {
    let mut v = Vec::with_capacity(p.inputs.len());
    for inp in &p.inputs {
        match inp.presence {
            None => v.push(None),
            Some(parr) => {
                let layout = &g.arr(parr).layout;
                let mut m: FxHashMap<Vec<u8>, i64> = FxHashMap::default();
                if let Some(o) = all.get(&inp.node) {
                    for (row, w) in &o.data {
                        let kv = layout.key_values(row)?;
                        *m.entry(presence_prefix(layout, &kv, inp.exact)?).or_default() += w;
                    }
                }
                v.push(Some(m));
            }
        }
    }
    Ok(v)
}

pub fn step(g: &Graph, env: &mut Env, _node: &Node, p: &JoinPlan, gsn: Gsn, all: &FxHashMap<NodeId, Output>, out: &mut Output) -> Result<()> {
    let cx = Ctx { g, p, pres_delta: presence_deltas(g, p, all)? };
    for path in &p.paths {
        let src = &p.inputs[path.source];
        let Some(d) = all.get(&src.node) else { continue };
        if d.data.is_empty() {
            continue;
        }
        let mut start = Vec::new();
        if let Some(parr) = src.presence {
            // presence changes of keys touched this tick
            let layout = g.arr(parr).layout.clone();
            let deltas = cx.pres_delta[path.source].as_ref().unwrap();
            let mut keys: Vec<&Vec<u8>> = deltas.keys().collect();
            keys.sort();
            for prefix in keys {
                let dw = deltas[prefix];
                if dw == 0 {
                    continue;
                }
                let new = presence_count(env, g, parr, prefix, ReadAt::Latest, None)?;
                let old = new - dw;
                if (old > 0) != (new > 0) {
                    let kv = if src.exact { layout.decode_row(prefix)? } else { layout.decode_key_values(prefix)?.0 };
                    let mut row = vec![Datum::Null; p.width];
                    row[src.offset..src.offset + src.width].clone_from_slice(&kv);
                    start.push((row, if new > 0 { 1 } else { -1 }));
                }
            }
        } else {
            for (raw, w) in &d.data {
                match src.mfp.eval(raw) {
                    Ok(Some(post)) => {
                        let mut row = vec![Datum::Null; p.width];
                        row[src.offset..src.offset + src.width].clone_from_slice(&post);
                        start.push((row, *w));
                    }
                    Ok(None) => {}
                    Err(e) => {
                        // Input expression errors count once per input row,
                        // by the input's error tracker. (An input without
                        // one has an MFP that cannot fail; were it to fail
                        // anyway, its error is not dropped.)
                        if !p.tracked(path.source) {
                            out.errs.push((err_record(&e), *w));
                        }
                    }
                }
            }
        }
        // changes that cancel after the input's MFP (e.g. updates of
        // columns the join does not use) need no lookups
        consolidate(&mut start);
        match src.presence.and_then(|_| presence_key_exprs(p, path.source)) {
            Some(keys) => {
                // The presence input's columns hold its key as decoded from
                // the index: canonical, without numeric scales, -0 and the
                // like. Paths that look the input up compute them from the
                // other inputs' rows instead; recompute them that way, or
                // rows derived here would not retract rows derived there.
                let mut rows = Vec::new();
                run_path(env, &cx, path, start, Some(gsn), &mut rows)?;
                for (row, _) in rows.iter_mut() {
                    for (j, e) in keys.iter().enumerate() {
                        if let Ok(v) = e.eval(row) {
                            row[src.offset + j] = v;
                        }
                    }
                }
                out.data.extend(rows);
            }
            None => run_path(env, &cx, path, start, Some(gsn), &mut out.data)?,
        }
    }
    Ok(())
}

/// The key expressions with which another path looks up presence input `i`:
/// its columns in terms of the other inputs' columns.
fn presence_key_exprs(p: &JoinPlan, i: usize) -> Option<&[Expr]> {
    p.paths.iter().filter(|pa| pa.source != i).flat_map(|pa| pa.steps.iter()).find(|s| s.input == i).map(|s| s.key.as_slice())
}

/// Full join contents (latest): drive the cheapest regular input's path.
pub fn snapshot(g: &Graph, env: &mut Env, p: &JoinPlan, out: &mut Output) -> Result<()> {
    let cx = Ctx { g, p, pres_delta: p.inputs.iter().map(|_| None).collect() };
    // pick the smallest regular input as the source
    let mut best: Option<(usize, i64)> = None;
    for (i, inp) in p.inputs.iter().enumerate() {
        if inp.presence.is_some() {
            continue;
        }
        let size = match g.any_arrangement(inp.node) {
            Some(a) => env.tree_count(g.arr(a).tree),
            None => i64::MAX / 2,
        };
        if best.is_none_or(|(_, s)| size < s) {
            best = Some((i, size));
        }
    }
    let Some((src, _)) = best else { return Ok(()) };
    let path = p.paths.iter().find(|pa| pa.source == src).expect("path for every input");
    let inp = &p.inputs[src];
    let mut cache = FxHashMap::default();
    // (the inputs' expression errors are their error trackers', which are
    // inputs of the join: lookups skip the failing rows)
    let snap = super::ops::owned(super::ops::snapshot_node(g, env, inp.node, &mut cache)?);
    let mut start = Vec::with_capacity(snap.data.len());
    for (raw, w) in snap.data {
        match inp.mfp.eval(&raw) {
            Ok(Some(post)) => {
                let mut row = vec![Datum::Null; p.width];
                row[inp.offset..inp.offset + inp.width].clone_from_slice(&post);
                start.push((row, w));
            }
            Ok(None) => {}
            Err(e) => {
                if !p.tracked(src) {
                    out.errs.push((err_record(&e), w));
                }
            }
        }
    }
    run_path(env, &cx, path, start, None, &mut out.data)
}
