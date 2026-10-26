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

use super::delta::{NodeDelta, Rows, StepOut};
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

/// Receives a presence key and its weight change.
type KeyChangeFn<'a> = dyn FnMut(&mut Env, &[u8], i64) -> Result<()> + 'a;

/// This tick's weight change per key of a presence input's collection: in
/// memory, or in a temporary tree when the input's delta was spilled.
enum PresenceDelta {
    Mem(FxHashMap<Vec<u8>, i64>),
    Tree(TreeId),
}

impl PresenceDelta {
    fn get(&self, env: &mut Env, prefix: &[u8]) -> Result<i64> {
        match self {
            PresenceDelta::Mem(m) => Ok(m.get(prefix).copied().unwrap_or(0)),
            PresenceDelta::Tree(t) => Ok(env.get(*t, prefix)?.map(|h| h.total()).unwrap_or(0)),
        }
    }

    /// The keys with a non-zero change, in key order.
    fn for_each(&self, env: &mut Env, f: &mut KeyChangeFn) -> Result<()> {
        match self {
            PresenceDelta::Mem(m) => {
                let mut keys: Vec<&Vec<u8>> = m.keys().collect();
                keys.sort();
                for k in keys {
                    let dw = m[k];
                    if dw != 0 {
                        f(env, k, dw)?;
                    }
                }
                Ok(())
            }
            PresenceDelta::Tree(t) => {
                let mut c = env.cursor(*t, std::ops::Bound::Unbounded, std::ops::Bound::Unbounded);
                while let Some((k, h)) = env.cursor_next(&mut c)? {
                    f(env, &k, h.total())?;
                }
                Ok(())
            }
        }
    }

    fn release(self, env: &mut Env) -> Result<()> {
        match self {
            PresenceDelta::Mem(_) => Ok(()),
            PresenceDelta::Tree(t) => env.drop_tree(t),
        }
    }
}

/// Presence count of a key under `at` (count summaries; "before" derived from
/// this tick's delta of the presence collection).
fn presence_count(env: &mut Env, g: &Graph, arr: ArrId, prefix: &[u8], at: ReadAt, delta: Option<&PresenceDelta>) -> Result<i64> {
    let a = g.arr(arr);
    let hi = crate::types::encoding::prefix_successor(prefix);
    let s = match &hi {
        Some(h) => env.range_summary(a.tree, std::ops::Bound::Included(prefix), std::ops::Bound::Excluded(h))?,
        None => env.range_summary(a.tree, std::ops::Bound::Included(prefix), std::ops::Bound::Unbounded)?,
    };
    let latest = s.count;
    Ok(match at {
        ReadAt::Latest => latest,
        ReadAt::Before(_) => latest - delta.map_or(Ok(0), |d| d.get(env, prefix))?,
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
    pres_delta: Vec<Option<PresenceDelta>>,
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
/// `prefix` (post-MFP), handed to `f` in batches (a key may match many
/// rows); for presence inputs, whether the key is present (the row is then
/// the key).
fn lookup_prefix(
    env: &mut Env,
    cx: &Ctx,
    step: &LookupStep,
    prefix: &[u8],
    key: &[Datum],
    at: ReadAt,
    f: &mut super::ops::RowSink,
) -> Result<()> {
    let inp = &cx.p.inputs[step.input];
    if let Some(parr) = inp.presence {
        let c = presence_count(env, cx.g, parr, prefix, at, cx.pres_delta[step.input].as_ref())?;
        return if c > 0 { f(env, vec![(Vec::new(), 1)]) } else { Ok(()) };
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
    let mut resume: Option<Vec<u8>> = None;
    loop {
        let mut out = Vec::new();
        let mut bytes = 0;
        let mut last = None;
        let mut visit = |k: &[u8], h: &crate::storage::History| {
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
                bytes += k.len();
                if out.len() >= super::ops::BATCH_ROWS || bytes >= 1 << 20 {
                    last = Some(k.to_vec());
                    return Ok(false);
                }
            }
            Ok(true)
        };
        match &resume {
            None => env.for_each_prefix(a.tree, prefix, &mut visit)?,
            Some(from) => {
                let hi = crate::types::encoding::prefix_successor(prefix);
                let hi = hi.as_deref().map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
                env.for_each(a.tree, std::ops::Bound::Excluded(from), hi, &mut visit)?
            }
        }
        if !out.is_empty() {
            f(env, out)?;
        }
        match last {
            Some(k) => resume = Some(k),
            None => return Ok(()),
        }
    }
}

/// Estimated bytes of a path's frontier looked up at once (a lookup step's
/// matches of that many rows are held at a time). Large enough that rows
/// with equal keys share lookups and lookups in key order find their
/// leaves in the cache.
const PATH_BYTES: usize = 8 << 20;

/// Extend `start` (rows of the path's source) along the path, handing the
/// joined rows to `out` in batches.
fn run_path(
    env: &mut Env,
    cx: &Ctx,
    path: &DeltaPath,
    start: Vec<(Row, i64)>,
    gsn: Option<Gsn>,
    out: &mut super::ops::RowSink,
) -> Result<()> {
    let mut frontier: Vec<(Row, i64)> = Vec::with_capacity(start.len());
    for (row, w) in start {
        if path.source_filters.iter().all(|f| f.eval_pred(&row).unwrap_or(false)) {
            frontier.push((row, w));
        }
    }
    run_steps(env, cx, path, &path.steps, frontier, gsn, out)
}

/// Extend `frontier` along `steps`, depth first in chunks of `PATH_BYTES`
/// (so that the rows in flight stay bounded whatever the fan-out).
fn run_steps(
    env: &mut Env,
    cx: &Ctx,
    path: &DeltaPath,
    steps: &[LookupStep],
    frontier: Vec<(Row, i64)>,
    gsn: Option<Gsn>,
    out: &mut super::ops::RowSink,
) -> Result<()> {
    if frontier.is_empty() {
        return Ok(());
    }
    let Some((step, rest)) = steps.split_first() else { return out(env, frontier) };
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
    let mut start = 0;
    while start < frontier.len() {
        let mut end = start;
        let mut bytes = 0;
        while end < frontier.len() && bytes < PATH_BYTES {
            bytes += row_bytes(&frontier[end].0);
            end += 1;
        }
        lookup_step(env, cx, step, at, &frontier[start..end], &mut |env, next| run_steps(env, cx, path, rest, next, gsn, out))?;
        start = end;
    }
    Ok(())
}

/// The rows of `frontier` extended by one lookup step, handed to `emit` in
/// chunks of `PATH_BYTES`.
fn lookup_step(
    env: &mut Env,
    cx: &Ctx,
    step: &LookupStep,
    at: ReadAt,
    frontier: &[(Row, i64)],
    emit: &mut super::ops::RowSink,
) -> Result<()> {
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
    let mut next_bytes = 0;
    let mut j = 0;
    while j < keyed.len() {
        let mut end = j + 1;
        while end < keyed.len() && keyed[end].0 == keyed[j].0 {
            end += 1;
        }
        let (k, n, _, key) = &keyed[j];
        lookup_prefix(env, cx, step, &k[..*n], key, at, &mut |env, matches| {
            for (_, _, i, key) in &keyed[j..end] {
                let (row, w) = &frontier[*i];
                for (post, mw) in &matches {
                    let mut r = row.clone();
                    // a presence input's row is the probing key itself
                    let post = if inp.presence.is_some() { key } else { post };
                    r[inp.offset..inp.offset + inp.width].clone_from_slice(post);
                    if step.filters.iter().all(|f| f.eval_pred(&r).unwrap_or(false)) {
                        next_bytes += row_bytes(&r);
                        next.push((r, w * mw));
                        if next_bytes >= PATH_BYTES {
                            next_bytes = 0;
                            emit(env, std::mem::take(&mut next))?;
                        }
                    }
                }
            }
            Ok(())
        })?;
        j = end;
    }
    if next.is_empty() { Ok(()) } else { emit(env, next) }
}

fn presence_deltas(env: &mut Env, g: &Graph, p: &JoinPlan, all: &FxHashMap<NodeId, NodeDelta>, gsn: Gsn) -> Result<Vec<Option<PresenceDelta>>> {
    let mut v = Vec::with_capacity(p.inputs.len());
    for inp in &p.inputs {
        match inp.presence {
            None => v.push(None),
            Some(parr) => {
                let layout = &g.arr(parr).layout;
                let d = match all.get(&inp.node) {
                    Some(rows @ NodeDelta { rows: Rows::Tree { .. }, .. }) => {
                        let tree = env.create_temp_tree(std::sync::Arc::new(crate::storage::CountSummarizer));
                        let mut b = crate::storage::TreeBuilder::new(env, tree, gsn);
                        let r = rows.for_each_batch(env, &mut |env, batch| {
                            for (row, w) in batch {
                                b.push(env, presence_prefix(layout, &layout.key_values(row)?, inp.exact)?, *w)?;
                            }
                            Ok(())
                        });
                        let r = match r {
                            Ok(()) => b.finish(env),
                            Err(e) => b.abort(env).and(Err(e)),
                        };
                        match r {
                            Ok(()) => PresenceDelta::Tree(tree),
                            Err(e) => {
                                env.drop_tree(tree)?;
                                return Err(e);
                            }
                        }
                    }
                    rows => {
                        let mut m: FxHashMap<Vec<u8>, i64> = FxHashMap::default();
                        if let Some(NodeDelta { rows: Rows::Mem(d), .. }) = rows {
                            for (row, w) in d {
                                let kv = layout.key_values(row)?;
                                *m.entry(presence_prefix(layout, &kv, inp.exact)?).or_default() += w;
                            }
                        }
                        PresenceDelta::Mem(m)
                    }
                };
                v.push(Some(d));
            }
        }
    }
    Ok(v)
}

pub fn step(g: &Graph, env: &mut Env, _node: &Node, p: &JoinPlan, gsn: Gsn, all: &FxHashMap<NodeId, NodeDelta>, out: &mut StepOut) -> Result<()> {
    let cx = Ctx { g, p, pres_delta: presence_deltas(env, g, p, all, gsn)? };
    let r = step_paths(env, &cx, gsn, all, out);
    for d in cx.pres_delta.into_iter().flatten() {
        d.release(env)?;
    }
    r
}

/// Extend the changes of every input along its path.
fn step_paths(env: &mut Env, cx: &Ctx, gsn: Gsn, all: &FxHashMap<NodeId, NodeDelta>, out: &mut StepOut) -> Result<()> {
    let (g, p) = (cx.g, cx.p);
    for path in &p.paths {
        let src = &p.inputs[path.source];
        let Some(d) = all.get(&src.node) else { continue };
        if d.rows.is_empty() {
            continue;
        }
        // The presence input's columns hold its key as decoded from the
        // index: canonical, without numeric scales, -0 and the like. Paths
        // that look the input up compute them from the other inputs' rows
        // instead; recompute them that way, or rows derived here would not
        // retract rows derived there.
        let keys = src.presence.and_then(|_| presence_key_exprs(p, path.source));
        let mut emit = |env: &mut Env, mut rows: Delta| {
            if let Some(keys) = keys {
                for (row, _) in rows.iter_mut() {
                    for (j, e) in keys.iter().enumerate() {
                        if let Ok(v) = e.eval(row) {
                            row[src.offset + j] = v;
                        }
                    }
                }
            }
            for (r, w) in rows {
                out.push(env, r, w)?;
            }
            Ok(())
        };
        // changes that cancel after the input's MFP (e.g. updates of
        // columns the join does not use) need no lookups
        let mut run = |env: &mut Env, mut start: Delta| {
            consolidate(&mut start);
            run_path(env, cx, path, start, Some(gsn), &mut emit)
        };
        let (mut errs, mut errs_limit) = (Vec::new(), super::ops::BATCH_ROWS);
        if let Some(parr) = src.presence {
            // presence changes of keys touched this tick
            let layout = g.arr(parr).layout.clone();
            let deltas = cx.pres_delta[path.source].as_ref().unwrap();
            let mut start = Vec::new();
            let mut bytes = 0;
            deltas.for_each(env, &mut |env, prefix, dw| {
                let new = presence_count(env, g, parr, prefix, ReadAt::Latest, None)?;
                let old = new - dw;
                if (old > 0) != (new > 0) {
                    let kv = if src.exact { layout.decode_row(prefix)? } else { layout.decode_key_values(prefix)?.0 };
                    let mut row = vec![Datum::Null; p.width];
                    row[src.offset..src.offset + src.width].clone_from_slice(&kv);
                    bytes += row_bytes(&row);
                    start.push((row, if new > 0 { 1 } else { -1 }));
                    if bytes >= PATH_BYTES {
                        bytes = 0;
                        run(env, std::mem::take(&mut start))?;
                    }
                }
                Ok(())
            })?;
            run(env, start)?;
        } else {
            d.for_each_batch(env, &mut |env, batch| {
                let mut start = Vec::with_capacity(batch.len());
                for (raw, w) in batch {
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
                                errs.push((err_record(&e), *w));
                            }
                        }
                    }
                }
                if errs.len() >= errs_limit {
                    consolidate_errs(&mut errs);
                    errs_limit = (2 * errs.len()).max(super::ops::BATCH_ROWS);
                }
                run(env, start)
            })?;
        }
        for (e, w) in errs {
            out.err(e, w);
        }
    }
    Ok(())
}

/// The key expressions with which another path looks up presence input `i`:
/// its columns in terms of the other inputs' columns.
fn presence_key_exprs(p: &JoinPlan, i: usize) -> Option<&[Expr]> {
    p.paths.iter().filter(|pa| pa.source != i).flat_map(|pa| pa.steps.iter()).find(|s| s.input == i).map(|s| s.key.as_slice())
}

fn first_path_of(_p: &JoinPlan, source: usize) -> usize {
    source
}

/// Full join contents (latest): drive the cheapest regular input's path,
/// over that input's snapshot a batch at a time.
pub fn snapshot(g: &Graph, env: &mut Env, p: &JoinPlan, out: &mut super::ops::SnapOut) -> Result<()> {
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
    // expression errors in the other regular inputs' maps count once per
    // input row, as on their delta paths (lookups skip the failing rows)
    for (i, other) in p.inputs.iter().enumerate() {
        if i == src || other.presence.is_some() || other.mfp.steps.is_empty() {
            continue;
        }
        super::ops::snapshot_stream(g, env, other.node, &mut |_, batch| {
            for (raw, w) in &batch {
                if let Err(e) = other.mfp.eval(raw) {
                    out.err(err_record(&e), *w);
                }
            }
            Ok(())
        })?;
    }
    // the source's rows are extended in chunks of PATH_BYTES, so that rows
    // with equal keys in a chunk share their lookups
    let mut start = Vec::new();
    let mut start_bytes = 0;
    let (mut errs, mut errs_limit) = (Vec::new(), super::ops::BATCH_ROWS);
    let mut emit = |env: &mut Env, rows: Delta| {
        for (r, w) in rows {
            out.push(env, r, w)?;
        }
        Ok(())
    };
    super::ops::snapshot_stream(g, env, inp.node, &mut |env, batch| {
        for (raw, w) in batch {
            match inp.mfp.eval(&raw) {
                Ok(Some(post)) => {
                    let mut row = vec![Datum::Null; p.width];
                    row[inp.offset..inp.offset + inp.width].clone_from_slice(&post);
                    start_bytes += row_bytes(&row);
                    start.push((row, w));
                }
                Ok(None) => {}
                Err(e) => errs.push((err_record(&e), w)),
            }
        }
        if errs.len() >= errs_limit {
            consolidate_errs(&mut errs);
            errs_limit = (2 * errs.len()).max(super::ops::BATCH_ROWS);
        }
        if start_bytes >= PATH_BYTES {
            start_bytes = 0;
            run_path(env, &cx, path, std::mem::take(&mut start), None, &mut emit)?;
        }
        Ok(())
    })?;
    run_path(env, &cx, path, start, None, &mut emit)?;
    out.errs(errs);
    Ok(())
}
