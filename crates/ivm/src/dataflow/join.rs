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
}

#[derive(Clone, Debug)]
pub struct DeltaPath {
    pub source: usize,
    pub source_filters: Vec<Expr>,
    pub steps: Vec<LookupStep>,
}

#[derive(Clone, Debug)]
pub struct JoinPlan {
    pub inputs: Vec<JoinInput>,
    pub paths: Vec<DeltaPath>,
    pub width: usize,
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
        v
    }

    pub fn fingerprint(&self, cx: &FpCtx, fp: &mut Vec<u8>) {
        for i in &self.inputs {
            fp.extend_from_slice(cx.node(i.node));
            i.mfp.fingerprint(fp);
            match i.presence {
                Some(a) => fp.extend_from_slice(cx.arr(a)),
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

struct Ctx<'a> {
    g: &'a Graph,
    p: &'a JoinPlan,
    pres_delta: Vec<Option<FxHashMap<Vec<u8>, i64>>>,
}

/// Rows of input `step.input` matching the joined row (post-MFP).
fn lookup(env: &mut Env, cx: &Ctx, step: &LookupStep, row: &[Datum], at: ReadAt) -> Result<Vec<(Row, i64)>> {
    let mut key = Vec::with_capacity(step.key.len());
    for (k, safe) in step.key.iter().zip(&step.null_safe) {
        let v = match k.eval(row) {
            Ok(v) => v,
            Err(_) => return Ok(vec![]),
        };
        if v.is_null() && !safe {
            return Ok(vec![]);
        }
        key.push(v);
    }
    let inp = &cx.p.inputs[step.input];
    if let Some(parr) = inp.presence {
        let prefix = cx.g.arr(parr).layout.encode_prefix(&key);
        let c = presence_count(env, cx.g, parr, &prefix, at, cx.pres_delta[step.input].as_ref())?;
        return Ok(if c > 0 { vec![(key, 1)] } else { vec![] });
    }
    let raw = cx.g.lookup(env, step.arr, &key, at)?;
    let mut out = Vec::with_capacity(raw.len());
    for (r, w) in raw {
        if w <= 0 {
            continue;
        }
        if let Ok(Some(post)) = inp.mfp.eval(&r) {
            out.push((post, w));
        }
    }
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
        let mut next = Vec::new();
        for (row, w) in &frontier {
            for (post, mw) in lookup(env, cx, step, row, at)? {
                let mut r = row.clone();
                r[inp.offset..inp.offset + inp.width].clone_from_slice(&post);
                if step.filters.iter().all(|f| f.eval_pred(&r).unwrap_or(false)) {
                    next.push((r, w * mw));
                }
            }
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
                        *m.entry(layout.encode_prefix(&kv)).or_default() += w;
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
                    let (kv, _) = layout.decode_key_values(prefix)?;
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
                        // input expression errors count once per input row
                        if path.source == first_path_of(p, path.source) {
                            out.errs.push((err_record(&e), *w));
                        }
                    }
                }
            }
        }
        // changes that cancel after the input's MFP (e.g. updates of
        // columns the join does not use) need no lookups
        consolidate(&mut start);
        run_path(env, &cx, path, start, Some(gsn), &mut out.data)?;
    }
    Ok(())
}

fn first_path_of(_p: &JoinPlan, source: usize) -> usize {
    source
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
            Err(e) => out.errs.push((err_record(&e), w)),
        }
    }
    run_path(env, &cx, path, start, None, &mut out.data)
}
