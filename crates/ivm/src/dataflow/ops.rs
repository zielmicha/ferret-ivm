//! Operator step / snapshot implementations (except join, reduce, window).

use super::*;
use crate::storage::History;
use crate::types::Datum;
use std::ops::Bound;

/// Step dispatch for non-table nodes.
pub fn step_node(
    g: &Graph,
    env: &mut Env,
    node: &Node,
    inputs: &[&Output],
    gsn: Gsn,
    all: &FxHashMap<NodeId, Output>,
) -> Result<Output> {
    let mut out = Output::default();
    // errors flow through every operator
    for i in inputs {
        out.errs.extend(i.errs.iter().cloned());
    }
    match &node.kind {
        NodeKind::Table { .. } | NodeKind::Constant { .. } => {}
        NodeKind::Mfp { mfp } => {
            for (row, w) in &inputs[0].data {
                match mfp.eval(row) {
                    Ok(Some(r)) => out.data.push((r, *w)),
                    Ok(None) => {}
                    Err(e) => out.errs.push((err_record(&e), *w)),
                }
            }
        }
        NodeKind::Union { negate } => {
            for (i, inp) in inputs.iter().enumerate() {
                let s = if negate[i] { -1 } else { 1 };
                out.data.extend(inp.data.iter().map(|(r, w)| (r.clone(), w * s)));
                if negate[i] {
                    // errors are never negated: they union
                }
            }
        }
        NodeKind::Negate => {
            out.data.extend(inputs[0].data.iter().map(|(r, w)| (r.clone(), -w)));
        }
        NodeKind::FlatMap { funcs, ordinality } => {
            for (row, w) in &inputs[0].data {
                match tablefunc::eval(funcs, row, *ordinality) {
                    Ok(rows) => {
                        for extra in rows {
                            let mut r = row.clone();
                            r.extend(extra);
                            out.data.push((r, *w));
                        }
                    }
                    Err(e) => out.errs.push((err_record(&e), *w)),
                }
            }
        }
        NodeKind::Join(p) => join::step(g, env, node, p, gsn, all, &mut out)?,
        NodeKind::Reduce(p) => reduce::step(g, env, p, &inputs[0].data, gsn, &mut out)?,
        NodeKind::Distinct { arr } => distinct_step(g, env, *arr, &inputs[0].data, gsn, &mut out)?,
        NodeKind::SetOp { op, all: is_all, left_arr, right_arr } => {
            setop_step(g, env, *op, *is_all, *left_arr, *right_arr, &inputs[0].data, &inputs[1].data, gsn, &mut out)?
        }
        NodeKind::TopK(p) => topk_step(g, env, p, &inputs[0].data, gsn, &mut out)?,
        NodeKind::Window(p) => window::step(g, env, p, &inputs[0].data, gsn, &mut out)?,
        NodeKind::Recursive(p) => recursive_step(g, env, node, p, gsn, &mut out)?,
        NodeKind::Temporal(p) => temporal::step(g, env, p, &inputs[0].data, gsn, &mut out)?,
    }
    consolidate(&mut out.data);
    consolidate_errs(&mut out.errs);
    Ok(out)
}

/// Hydration hook for operators with private state.
pub fn hydrate_node(g: &mut Graph, env: &mut Env, id: NodeId, gsn: Gsn) -> Result<()> {
    if let NodeKind::Temporal(p) = &g.node(id).kind {
        let p = p.clone();
        let input = g.node(id).inputs[0];
        temporal::hydrate(g, env, &p, input, gsn)?;
    }
    Ok(())
}

/// Rows a snapshot hands on at once (fewer from a source whose entries are
/// large: see `Graph::scan_stream`).
pub const BATCH_ROWS: usize = 1024;

/// Receives rows in batches, with the environment (which their producer
/// does not hold between batches).
pub type RowSink<'a> = dyn FnMut(&mut Env, Delta) -> Result<()> + 'a;

/// The output of a snapshot being computed: rows are handed on to a sink
/// in batches of `BATCH_ROWS`, error records are added up.
pub struct SnapOut<'s, 'a> {
    rows: Delta,
    sink: &'s mut RowSink<'a>,
    errs: BTreeMap<(String, String), i64>,
}

impl<'s, 'a> SnapOut<'s, 'a> {
    pub fn new(sink: &'s mut RowSink<'a>) -> Self {
        SnapOut { rows: Vec::new(), sink, errs: BTreeMap::new() }
    }

    pub fn push(&mut self, env: &mut Env, row: Row, w: i64) -> Result<()> {
        self.rows.push((row, w));
        if self.rows.len() >= BATCH_ROWS {
            self.flush(env)?;
        }
        Ok(())
    }

    pub fn flush(&mut self, env: &mut Env) -> Result<()> {
        if !self.rows.is_empty() {
            (self.sink)(env, std::mem::take(&mut self.rows))?;
        }
        Ok(())
    }

    pub fn err(&mut self, e: (String, String), w: i64) {
        *self.errs.entry(e).or_default() += w;
    }

    pub fn errs(&mut self, errs: ErrDelta) {
        for (e, w) in errs {
            self.err(e, w);
        }
    }

    /// Hand on the last rows; the consolidated error records.
    pub fn finish(mut self, env: &mut Env) -> Result<ErrDelta> {
        self.flush(env)?;
        Ok(self.errs.into_iter().filter(|(_, w)| *w != 0).collect())
    }
}

/// Stream the contents of a node (latest), computed from arrangements or
/// inputs, to `sink` as batches of weighted rows that need not be
/// consolidated (arrangements consolidate entries when they are applied,
/// `Graph::snapshot` its result); returns the node's error records. Only a
/// batch at a time is held per node: a snapshot of a node without an
/// arrangement is streamed from its inputs' (recomputed for each reader).
pub fn snapshot_stream(g: &Graph, env: &mut Env, id: NodeId, sink: &mut RowSink) -> Result<ErrDelta> {
    if let Some(a) = g.any_arrangement(id) {
        g.scan_stream(env, a, ReadAt::Latest, sink)?;
        return Ok(g.errors_of(id));
    }
    let node = g.node(id);
    let mut out = SnapOut::new(sink);
    match &node.kind {
        NodeKind::Table { primary, .. } => {
            g.scan_stream(env, *primary, ReadAt::Latest, out.sink)?;
        }
        NodeKind::Constant { rows } => {
            for r in rows {
                out.push(env, r.clone(), 1)?;
            }
        }
        NodeKind::Mfp { mfp } => {
            let input = snapshot_stream(g, env, node.inputs[0], &mut |env, batch| {
                for (row, w) in batch {
                    match mfp.eval(&row) {
                        Ok(Some(r)) => out.push(env, r, w)?,
                        Ok(None) => {}
                        Err(e) => out.err(err_record(&e), w),
                    }
                }
                Ok(())
            })?;
            out.errs(input);
        }
        NodeKind::Union { negate } => {
            for (i, inp) in node.inputs.iter().enumerate() {
                let sign = if negate[i] { -1 } else { 1 };
                let errs = snapshot_stream(g, env, *inp, &mut |env, mut batch| {
                    for (_, w) in batch.iter_mut() {
                        *w *= sign;
                    }
                    (out.sink)(env, batch)
                })?;
                out.errs(errs);
            }
        }
        NodeKind::Negate => {
            let errs = snapshot_stream(g, env, node.inputs[0], &mut |env, mut batch| {
                for (_, w) in batch.iter_mut() {
                    *w = -*w;
                }
                (out.sink)(env, batch)
            })?;
            out.errs(errs);
        }
        NodeKind::FlatMap { funcs, ordinality } => {
            let input = snapshot_stream(g, env, node.inputs[0], &mut |env, batch| {
                for (row, w) in batch {
                    match tablefunc::eval(funcs, &row, *ordinality) {
                        Ok(rows) => {
                            for extra in rows {
                                let mut r = row.clone();
                                r.extend(extra);
                                out.push(env, r, w)?;
                            }
                        }
                        Err(e) => out.err(err_record(&e), w),
                    }
                }
                Ok(())
            })?;
            out.errs(input);
        }
        NodeKind::Join(p) => {
            for i in &node.inputs {
                let s = snapshot_errs(g, env, *i)?;
                out.errs(s);
            }
            join::snapshot(g, env, p, &mut out)?;
        }
        NodeKind::Reduce(p) => {
            let errs = snapshot_errs(g, env, node.inputs[0])?;
            out.errs(errs);
            reduce::snapshot(g, env, p, &mut out)?;
        }
        NodeKind::Distinct { arr } => {
            let errs = snapshot_errs(g, env, node.inputs[0])?;
            out.errs(errs);
            distinct_snapshot(g, env, *arr, &mut out)?;
        }
        NodeKind::SetOp { op, all, left_arr, right_arr } => {
            let errs = snapshot_errs(g, env, node.inputs[0])?;
            out.errs(errs);
            let errs = snapshot_errs(g, env, node.inputs[1])?;
            out.errs(errs);
            setop_snapshot(g, env, *op, *all, *left_arr, *right_arr, &mut out)?;
        }
        NodeKind::TopK(p) => {
            let errs = snapshot_errs(g, env, node.inputs[0])?;
            out.errs(errs);
            topk_snapshot(g, env, p, &mut out)?;
        }
        NodeKind::Window(p) => {
            let errs = snapshot_errs(g, env, node.inputs[0])?;
            out.errs(errs);
            window::snapshot(g, env, p, &mut out)?;
        }
        NodeKind::Recursive(p) => {
            for i in &node.inputs {
                let s = snapshot_errs(g, env, *i)?;
                out.errs(s);
            }
            recursive_snapshot(g, env, p, &mut out)?;
        }
        NodeKind::Temporal(p) => {
            let errs = snapshot_stream(g, env, node.inputs[0], &mut |env, batch| temporal::snapshot(g, env, p, batch, &mut out))?;
            out.errs(errs);
        }
    }
    out.finish(env)
}

/// Errors present in a node's snapshot (arranged nodes keep theirs in the
/// graph; the others recompute).
fn snapshot_errs(g: &Graph, env: &mut Env, id: NodeId) -> Result<ErrDelta> {
    if g.any_arrangement(id).is_some() {
        return Ok(g.errors_of(id));
    }
    snapshot_stream(g, env, id, &mut |_, _| Ok(()))
}

// ---------------------------------------------------------------- base tables

/// Apply logged changes to a base table: compute the delta against the stored
/// current values (before-images come from the primary index, so the source
/// never needs REPLICA IDENTITY FULL; unchanged TOAST values are filled in).
pub fn table_step(g: &Graph, env: &mut Env, node: &Node, changes: Vec<TableChange>, _gsn: Gsn) -> Result<Output> {
    let NodeKind::Table { pk, primary, .. } = &node.kind else { unreachable!() };
    let arr = g.arr(*primary);
    let layout = arr.layout.clone();
    let tree = arr.tree;
    let mut out = Output::default();
    if pk.is_empty() {
        // multiset table keyed by all columns
        for ch in changes {
            match ch {
                TableChange::Insert { row } | TableChange::Upsert { row, .. } => out.data.push((row, 1)),
                TableChange::Remove { row } => out.data.push((row, -1)),
                TableChange::Delete { key } => out.data.push((key, -1)),
                TableChange::Truncate => {
                    // retract the stored contents; the changes so far in this
                    // tick are dropped (retracting them as well would leave
                    // them negative)
                    let mut cur = g.scan_arr(env, *primary, ReadAt::Latest)?;
                    consolidate(&mut cur);
                    out.data = cur.into_iter().map(|(r, w)| (r, -w)).collect();
                }
            }
        }
        consolidate(&mut out.data);
        return Ok(out);
    }
    // keyed upserts: every key touched (in first-touch order) with its
    // stored row and its row after this tick's changes
    let mut index: FxHashMap<Vec<u8>, usize> = FxHashMap::default();
    let mut touched: Vec<(Option<Row>, After)> = Vec::new();
    let touch = |env: &mut Env, index: &mut FxHashMap<Vec<u8>, usize>, touched: &mut Vec<(Option<Row>, After)>, key: Vec<u8>| {
        if let Some(&i) = index.get(&key) {
            return Ok::<usize, SqlError>(i);
        }
        let mut found = None;
        env.for_each_prefix(tree, &key, &mut |k, h| {
            if h.total() > 0 {
                found = Some(layout.decode_row(k)?);
            }
            Ok(true)
        })?;
        touched.push((found, After::Stored));
        index.insert(key, touched.len() - 1);
        Ok(touched.len() - 1)
    };
    // The row the previous change deleted. A primary key change arrives as a
    // delete of the old key directly followed by an upsert of the new row,
    // whose unchanged (TOASTed) values are those of the old key's row.
    let mut deleted: Option<Row> = None;
    for ch in changes {
        let just_deleted = std::mem::take(&mut deleted);
        match ch {
            TableChange::Upsert { mut row, unchanged } => {
                let i = touch(env, &mut index, &mut touched, layout.encode_prefix_of(&row, pk))?;
                if !unchanged.is_empty() {
                    let (stored, after) = &touched[i];
                    let old = match after {
                        After::Stored => stored.as_ref(),
                        After::Row(r) => Some(r),
                        After::Absent => None,
                    };
                    if let Some(o) = old.or(just_deleted.as_ref()) {
                        for c in &unchanged {
                            row[*c] = o[*c].clone();
                        }
                    } else {
                        log::warn!("unchanged TOAST value for a row not present in the replica; using NULL");
                        for c in &unchanged {
                            row[*c] = Datum::Null;
                        }
                    }
                }
                touched[i].1 = After::Row(row);
            }
            TableChange::Delete { key: kv } => {
                let i = touch(env, &mut index, &mut touched, layout.encode_prefix(&kv))?;
                let (stored, after) = &touched[i];
                deleted = match after {
                    After::Stored => stored.clone(),
                    After::Row(r) => Some(r.clone()),
                    After::Absent => None,
                };
                touched[i].1 = After::Absent;
            }
            TableChange::Insert { row } => {
                let i = touch(env, &mut index, &mut touched, layout.encode_prefix_of(&row, pk))?;
                touched[i].1 = After::Row(row);
            }
            TableChange::Remove { row } => {
                let i = touch(env, &mut index, &mut touched, layout.encode_prefix_of(&row, pk))?;
                touched[i].1 = After::Absent;
            }
            TableChange::Truncate => {
                // every stored row and every row touched so far goes away
                let all = g.scan_arr(env, *primary, ReadAt::Latest)?;
                for (row, _) in all {
                    if let std::collections::hash_map::Entry::Vacant(e) = index.entry(layout.encode_prefix_of(&row, pk)) {
                        e.insert(touched.len());
                        touched.push((Some(row), After::Absent));
                    }
                }
                for t in touched.iter_mut() {
                    t.1 = After::Absent;
                }
            }
        }
    }
    // Rows of different keys differ (in their key columns), so the delta is
    // consolidated once unchanged rows are left out.
    for (stored, after) in touched {
        let new = match after {
            After::Stored => continue,
            After::Row(r) => Some(r),
            After::Absent => None,
        };
        if let (Some(o), Some(n)) = (&stored, &new)
            && crate::types::datum::exact_row_cmp(o, n) == std::cmp::Ordering::Equal
        {
            continue;
        }
        if let Some(o) = stored {
            out.data.push((o, -1));
        }
        if let Some(n) = new {
            out.data.push((n, 1));
        }
    }
    Ok(out)
}

/// A base table row after the changes of one tick.
enum After {
    /// As stored (not changed).
    Stored,
    Row(Row),
    Absent,
}

// ---------------------------------------------------------------- distinct

fn affected_prefixes(layout: &Layout, delta: &Delta) -> Result<Vec<Vec<u8>>> {
    let mut v: Vec<Vec<u8>> = Vec::with_capacity(delta.len());
    for (row, _) in delta {
        let kv = layout.key_values(row)?;
        v.push(layout.encode_prefix(&kv));
    }
    v.sort();
    v.dedup();
    Ok(v)
}

/// First entry with positive weight (as of `at`) under `prefix`, and the total.
fn representative(env: &mut Env, tree: TreeId, layout: &Layout, prefix: &[u8], at: ReadAt) -> Result<(Option<Row>, i64)> {
    let mut rep = None;
    let mut total = 0;
    env.for_each_prefix(tree, prefix, &mut |k, h| {
        let w = h.weight(at);
        if w > 0 && rep.is_none() {
            rep = Some(layout.decode_row(k)?);
        }
        total += w;
        Ok(true)
    })?;
    Ok((rep, total))
}

fn distinct_step(g: &Graph, env: &mut Env, arr: ArrId, delta: &Delta, gsn: Gsn, out: &mut Output) -> Result<()> {
    let a = g.arr(arr);
    let layout = a.layout.clone();
    for p in affected_prefixes(&layout, delta)? {
        let (old, oc) = representative(env, a.tree, &layout, &p, ReadAt::Before(gsn))?;
        let (new, nc) = representative(env, a.tree, &layout, &p, ReadAt::Latest)?;
        if oc > 0
            && let Some(o) = old
        {
            out.data.push((o, -1));
        }
        if nc > 0
            && let Some(n) = new
        {
            out.data.push((n, 1));
        }
    }
    Ok(())
}

fn distinct_snapshot(g: &Graph, env: &mut Env, arr: ArrId, out: &mut SnapOut) -> Result<()> {
    let a = g.arr(arr);
    let layout = a.layout.clone();
    let mut c = env.cursor(a.tree, Bound::Unbounded, Bound::Unbounded);
    let mut last: Option<Vec<u8>> = None;
    while let Some((k, h)) = env.cursor_next(&mut c)? {
        if h.total() <= 0 {
            continue;
        }
        let kb = layout.key_bytes(&k)?.to_vec();
        if last.as_ref() == Some(&kb) {
            continue;
        }
        out.push(env, layout.decode_row(&k)?, 1)?;
        last = Some(kb);
    }
    Ok(())
}

// ---------------------------------------------------------------- set operations

fn setop_mult(op: SetOpKind, all: bool, a: i64, b: i64) -> i64 {
    match (op, all) {
        (SetOpKind::Except, false) => (a > 0 && b <= 0) as i64,
        (SetOpKind::Except, true) => (a - b.max(0)).max(0),
        (SetOpKind::Intersect, false) => (a > 0 && b > 0) as i64,
        (SetOpKind::Intersect, true) => a.min(b).max(0),
        (SetOpKind::Union, false) => (a > 0 || b > 0) as i64,
        (SetOpKind::Union, true) => a + b,
    }
}

#[allow(clippy::too_many_arguments)]
fn setop_step(
    g: &Graph,
    env: &mut Env,
    op: SetOpKind,
    all: bool,
    la: ArrId,
    ra: ArrId,
    ld: &Delta,
    rd: &Delta,
    gsn: Gsn,
    out: &mut Output,
) -> Result<()> {
    let (l, r) = (g.arr(la), g.arr(ra));
    let (ll, rl) = (l.layout.clone(), r.layout.clone());
    let mut prefixes = affected_prefixes(&ll, ld)?;
    prefixes.extend(affected_prefixes(&rl, rd)?);
    prefixes.sort();
    prefixes.dedup();
    for p in prefixes {
        let (lrep_old, a_old) = representative(env, l.tree, &ll, &p, ReadAt::Before(gsn))?;
        let (lrep_new, a_new) = representative(env, l.tree, &ll, &p, ReadAt::Latest)?;
        let (rrep_old, b_old) = representative(env, r.tree, &rl, &p, ReadAt::Before(gsn))?;
        let (rrep_new, b_new) = representative(env, r.tree, &rl, &p, ReadAt::Latest)?;
        let m_old = setop_mult(op, all, a_old, b_old);
        let m_new = setop_mult(op, all, a_new, b_new);
        if m_old > 0
            && let Some(rep) = lrep_old.or(rrep_old)
        {
            out.data.push((rep, -m_old));
        }
        if m_new > 0
            && let Some(rep) = lrep_new.or(rrep_new)
        {
            out.data.push((rep, m_new));
        }
    }
    Ok(())
}

fn setop_snapshot(g: &Graph, env: &mut Env, op: SetOpKind, all: bool, la: ArrId, ra: ArrId, out: &mut SnapOut) -> Result<()> {
    let (l, r) = (g.arr(la), g.arr(ra));
    let layouts = [l.layout.clone(), r.layout.clone()];
    // Both indexes in key order at once: a value (canonical key) is a
    // contiguous range of entries in each. Per value and side, the first
    // live entry (the representative) and the total weight.
    let mut cursors = [env.cursor(l.tree, Bound::Unbounded, Bound::Unbounded), env.cursor(r.tree, Bound::Unbounded, Bound::Unbounded)];
    let mut heads = [None, None];
    for side in 0..2 {
        heads[side] = next_live(env, &mut cursors[side], &layouts[side])?;
    }
    loop {
        let value = match (&heads[0], &heads[1]) {
            (None, None) => return Ok(()),
            (Some((a, _, _)), Some((b, _, _))) => a.min(b).clone(),
            (Some((a, _, _)), None) | (None, Some((a, _, _))) => a.clone(),
        };
        let mut sides: [(Option<Row>, i64); 2] = [(None, 0), (None, 0)];
        for side in 0..2 {
            while let Some((kb, k, w)) = &heads[side] {
                if *kb != value {
                    break;
                }
                let (rep, total) = &mut sides[side];
                if rep.is_none() && *w > 0 {
                    *rep = Some(layouts[side].decode_row(k)?);
                }
                *total += w;
                heads[side] = next_live(env, &mut cursors[side], &layouts[side])?;
            }
        }
        let [(lrep, a), (rrep, b)] = sides;
        let m = setop_mult(op, all, a, b);
        if m > 0
            && let Some(rep) = lrep.or(rrep)
        {
            out.push(env, rep, m)?;
        }
    }
}

/// An entry with its canonical key and weight.
type KeyedEntry = (Vec<u8>, Box<[u8]>, i64);

/// The next entry with a non-zero weight.
fn next_live(env: &mut Env, c: &mut crate::storage::Cursor, layout: &Layout) -> Result<Option<KeyedEntry>> {
    while let Some((k, h)) = env.cursor_next(c)? {
        let w = h.total();
        if w != 0 {
            return Ok(Some((layout.key_bytes(&k)?.to_vec(), k, w)));
        }
    }
    Ok(None)
}

// ---------------------------------------------------------------- top-k

#[derive(Clone, Debug)]
pub struct TopKPlan {
    /// Input columns [0, group_len) in key order form the group (the input
    /// arrangement is keyed by group columns, then order columns, then rest).
    pub group_key: Vec<usize>,
    pub limit: Option<u64>,
    pub offset: u64,
    /// Keep rows tied with the last kept one (the arrangement's whole key is
    /// the group and ORDER BY columns, so ties share the canonical key).
    pub with_ties: bool,
    pub input_arr: ArrId,
}

impl TopKPlan {
    pub fn fingerprint(&self, cx: &FpCtx, fp: &mut Vec<u8>) {
        fp.extend_from_slice(format!("{:?}{:?}{}{}", self.group_key, self.limit, self.offset, self.with_ties).as_bytes());
        fp.extend_from_slice(cx.arr(self.input_arr));
    }
}

/// Rows of a group's top-k as of `at`.
fn topk_rows(env: &mut Env, tree: TreeId, layout: &Layout, prefix: &[u8], at: ReadAt, p: &TopKPlan) -> Result<Delta> {
    let mut out = Vec::new();
    let mut skipped = 0u64;
    let mut taken = 0u64;
    let limit = p.limit.unwrap_or(u64::MAX);
    if limit == 0 {
        return Ok(out);
    }
    let mut last_key: Option<Vec<u8>> = None;
    env.for_each_prefix(tree, prefix, &mut |k, h| {
        let mut w = h.weight(at);
        if w <= 0 {
            return Ok(true);
        }
        if taken >= limit {
            // past the limit only rows tied with the last one remain
            return match &last_key {
                Some(lk) if p.with_ties && layout.key_bytes(k)? == lk.as_slice() => {
                    out.push((layout.decode_row(k)?, w));
                    Ok(true)
                }
                _ => Ok(false),
            };
        }
        if skipped < p.offset {
            let s = (p.offset - skipped).min(w as u64);
            skipped += s;
            w -= s as i64;
            if w == 0 {
                return Ok(true);
            }
        }
        // with ties, the rest of an entry's multiplicity is tied too
        let take = if p.with_ties { w as u64 } else { (limit - taken).min(w as u64) };
        out.push((layout.decode_row(k)?, take as i64));
        taken += take;
        if p.with_ties {
            last_key = Some(layout.key_bytes(k)?.to_vec());
            Ok(true)
        } else {
            Ok(taken < limit)
        }
    })?;
    Ok(out)
}

fn topk_step(g: &Graph, env: &mut Env, p: &TopKPlan, delta: &Delta, gsn: Gsn, out: &mut Output) -> Result<()> {
    let a = g.arr(p.input_arr);
    let layout = a.layout.clone();
    let mut groups: Vec<Vec<u8>> = Vec::new();
    for (row, _) in delta {
        let kv: Vec<Datum> = p.group_key.iter().map(|c| row[*c].clone()).collect();
        groups.push(layout.encode_prefix(&kv));
    }
    groups.sort();
    groups.dedup();
    for prefix in groups {
        for (r, w) in topk_rows(env, a.tree, &layout, &prefix, ReadAt::Before(gsn), p)? {
            out.data.push((r, -w));
        }
        out.data.extend(topk_rows(env, a.tree, &layout, &prefix, ReadAt::Latest, p)?);
    }
    Ok(())
}

fn topk_snapshot(g: &Graph, env: &mut Env, p: &TopKPlan, out: &mut SnapOut) -> Result<()> {
    let a = g.arr(p.input_arr);
    let layout = a.layout.clone();
    let ng = p.group_key.len();
    // enumerate distinct group prefixes, seeking from one group to the next
    let mut from: Option<Vec<u8>> = None;
    loop {
        let lo = match &from {
            Some(f) => Bound::Included(f.as_slice()),
            None => Bound::Unbounded,
        };
        let mut next = None;
        env.for_each(a.tree, lo, Bound::Unbounded, &mut |k, h| {
            if h.total() <= 0 {
                return Ok(true);
            }
            let (vals, _) = layout.decode_key_values(k)?;
            next = Some(layout.encode_prefix(&vals[..ng]));
            Ok(false)
        })?;
        let Some(prefix) = next else { return Ok(()) };
        for (r, w) in topk_rows(env, a.tree, &layout, &prefix, ReadAt::Latest, p)? {
            out.push(env, r, w)?;
        }
        from = crate::types::encoding::prefix_successor(&prefix);
        if from.is_none() {
            return Ok(());
        }
    }
}

// ---------------------------------------------------------------- recursion

/// WITH RECURSIVE evaluated by semi-naive iteration over the current state,
/// maintained by diffing against the previous result.
#[derive(Clone, Debug)]
pub struct RecursivePlan {
    pub base: crate::sql::plan::Rel,
    pub step: crate::sql::plan::Rel,
    pub rec_id: u64,
    pub distinct: bool,
    /// Catalog object -> node for the Gets inside base/step.
    pub sources: Vec<(u64, NodeId)>,
    pub out_arr: ArrId,
    /// The query's error while it has no valid result (under the empty
    /// key), so that it retracts when the query recovers; checkpointed with
    /// the graph's error state.
    pub errors: OpErrors,
}

impl RecursivePlan {
    pub fn fingerprint(&self, cx: &FpCtx, fp: &mut Vec<u8>) {
        fp.extend_from_slice(format!("{:?}", self.base).as_bytes());
        fp.extend_from_slice(format!("{:?}", self.step).as_bytes());
        fp.push(self.distinct as u8);
        for (oid, n) in &self.sources {
            fp.extend_from_slice(&oid.to_le_bytes());
            fp.extend_from_slice(cx.node(*n));
        }
    }
}

/// Contents of a recursive query (hydration: rebuilds its error state).
fn recursive_snapshot(g: &Graph, env: &mut Env, p: &RecursivePlan, out: &mut SnapOut) -> Result<()> {
    let mut errors = p.errors.lock();
    errors.clear();
    match crate::exec::eval_recursive(g, env, p) {
        Ok(rows) => {
            for (r, w) in rows {
                out.push(env, r, w)?;
            }
        }
        Err(e) if super::reduce::is_data_error(&e) => {
            let rec = err_record(&e);
            errors.insert(Vec::new(), rec.clone());
            out.err(rec, 1);
        }
        Err(e) => return Err(e),
    }
    Ok(())
}

fn recursive_step(g: &Graph, env: &mut Env, node: &Node, p: &RecursivePlan, gsn: Gsn, out: &mut Output) -> Result<()> {
    let _ = (node, gsn);
    let old = g.scan_arr(env, p.out_arr, ReadAt::Latest)?;
    out.data.extend(old.into_iter().map(|(r, w)| (r, -w)));
    let mut errors = p.errors.lock();
    match crate::exec::eval_recursive(g, env, p) {
        Ok(rows) => {
            out.data.extend(rows);
            if let Some(prev) = errors.remove(&Vec::new()) {
                out.errs.push((prev, -1));
            }
        }
        // an expression error: the query has no valid result, so its rows
        // retract and the error is reported (never a failed step)
        Err(e) if super::reduce::is_data_error(&e) => {
            let rec = err_record(&e);
            match errors.insert(Vec::new(), rec.clone()) {
                Some(prev) if prev == rec => {}
                Some(prev) => {
                    out.errs.push((prev, -1));
                    out.errs.push((rec, 1));
                }
                None => out.errs.push((rec, 1)),
            }
        }
        Err(e) => return Err(e),
    }
    Ok(())
}

/// Sum of weights in a history as of `at` (helper for operators).
pub fn weight_at(h: &History, at: ReadAt) -> i64 {
    h.weight(at)
}
