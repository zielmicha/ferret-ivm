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
pub fn hydrate_node(g: &mut Graph, env: &mut Env, id: NodeId, gsn: Gsn, cache: &mut SnapCache) -> Result<()> {
    if let NodeKind::Temporal(p) = &g.node(id).kind {
        let p = p.clone();
        let input = snapshot_node(g, env, g.node(id).inputs[0], cache)?;
        temporal::hydrate(g, env, &p, &input, gsn)?;
    }
    Ok(())
}

/// Snapshots computed during one hydration or snapshot read.
pub type SnapCache = FxHashMap<NodeId, Arc<Output>>;

/// Take ownership of a snapshot, copying only if it is shared.
pub fn owned(s: Arc<Output>) -> Output {
    Arc::try_unwrap(s).unwrap_or_else(|a| (*a).clone())
}

/// Contents of a node (latest), computed from arrangements or inputs, as
/// weighted rows that need not be consolidated (see [`Graph::snapshot`]).
/// Only snapshots of nodes with several consumers are kept in `cache`; the
/// rest are handed to their one reader and freed as soon as it is done.
pub fn snapshot_node(g: &Graph, env: &mut Env, id: NodeId, cache: &mut SnapCache) -> Result<Arc<Output>> {
    if let Some(o) = cache.get(&id) {
        return Ok(o.clone());
    }
    if let Some(a) = g.any_arrangement(id) {
        let data = g.scan_arr(env, a, ReadAt::Latest)?;
        return Ok(Arc::new(Output { data, errs: g.errors_of(id) }));
    }
    let node = g.node(id);
    let mut out = Output::default();
    match &node.kind {
        NodeKind::Table { primary, .. } => {
            out.data = g.scan_arr(env, *primary, ReadAt::Latest)?;
        }
        NodeKind::Constant { rows } => {
            out.data = rows.iter().map(|r| (r.clone(), 1)).collect();
        }
        NodeKind::Mfp { mfp } => {
            let input = owned(snapshot_node(g, env, node.inputs[0], cache)?);
            out.errs = input.errs;
            for (row, w) in input.data {
                match mfp.eval(&row) {
                    Ok(Some(r)) => out.data.push((r, w)),
                    Ok(None) => {}
                    Err(e) => out.errs.push((err_record(&e), w)),
                }
            }
        }
        NodeKind::Union { negate } => {
            for (i, inp) in node.inputs.iter().enumerate() {
                let s = owned(snapshot_node(g, env, *inp, cache)?);
                let sign = if negate[i] { -1 } else { 1 };
                out.data.extend(s.data.into_iter().map(|(r, w)| (r, w * sign)));
                out.errs.extend(s.errs);
            }
        }
        NodeKind::Negate => {
            let s = owned(snapshot_node(g, env, node.inputs[0], cache)?);
            out.data = s.data.into_iter().map(|(r, w)| (r, -w)).collect();
            out.errs = s.errs;
        }
        NodeKind::FlatMap { funcs, ordinality } => {
            let s = owned(snapshot_node(g, env, node.inputs[0], cache)?);
            out.errs = s.errs;
            for (row, w) in s.data {
                match tablefunc::eval(funcs, &row, *ordinality) {
                    Ok(rows) => {
                        for extra in rows {
                            let mut r = row.clone();
                            r.extend(extra);
                            out.data.push((r, w));
                        }
                    }
                    Err(e) => out.errs.push((err_record(&e), w)),
                }
            }
        }
        NodeKind::Join(p) => {
            for i in &node.inputs {
                let s = snapshot_errs(g, env, *i, cache)?;
                out.errs.extend(s);
            }
            join::snapshot(g, env, p, &mut out)?;
        }
        NodeKind::Reduce(p) => {
            out.errs = snapshot_errs(g, env, node.inputs[0], cache)?;
            reduce::snapshot(g, env, p, &mut out)?;
        }
        NodeKind::Distinct { arr } => {
            out.errs = snapshot_errs(g, env, node.inputs[0], cache)?;
            distinct_snapshot(g, env, *arr, &mut out)?;
        }
        NodeKind::SetOp { op, all, left_arr, right_arr } => {
            out.errs = snapshot_errs(g, env, node.inputs[0], cache)?;
            out.errs.extend(snapshot_errs(g, env, node.inputs[1], cache)?);
            setop_snapshot(g, env, *op, *all, *left_arr, *right_arr, &mut out)?;
        }
        NodeKind::TopK(p) => {
            out.errs = snapshot_errs(g, env, node.inputs[0], cache)?;
            topk_snapshot(g, env, p, &mut out)?;
        }
        NodeKind::Window(p) => {
            out.errs = snapshot_errs(g, env, node.inputs[0], cache)?;
            window::snapshot(g, env, p, &mut out)?;
        }
        NodeKind::Recursive(p) => {
            for i in &node.inputs {
                let s = snapshot_errs(g, env, *i, cache)?;
                out.errs.extend(s);
            }
            recursive_snapshot(g, env, p, &mut out)?;
        }
        NodeKind::Temporal(p) => {
            let input = owned(snapshot_node(g, env, node.inputs[0], cache)?);
            temporal::snapshot(g, p, input, &mut out);
        }
    }
    // Row-wise operators pass multiplicities through, and the rows of a
    // snapshot are consolidated where that is needed: arrangements
    // consolidate entries when they are applied, Graph::snapshot its result.
    // (Sorting millions of hydrated rows at every level was a large part of
    // creating a view.)
    let row_wise = matches!(
        node.kind,
        NodeKind::Mfp { .. } | NodeKind::Union { .. } | NodeKind::Negate | NodeKind::FlatMap { .. } | NodeKind::Join(_) | NodeKind::Temporal(_)
    );
    if !row_wise {
        consolidate(&mut out.data);
    }
    consolidate_errs(&mut out.errs);
    let out = Arc::new(out);
    if node.refs > 1 {
        cache.insert(id, out.clone());
    }
    Ok(out)
}

/// Errors present in a node's snapshot (arranged nodes keep theirs in the
/// graph; the others recompute).
fn snapshot_errs(g: &Graph, env: &mut Env, id: NodeId, cache: &mut SnapCache) -> Result<ErrDelta> {
    if g.any_arrangement(id).is_some() {
        return Ok(g.errors_of(id));
    }
    Ok(snapshot_node(g, env, id, cache)?.errs.clone())
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

fn distinct_snapshot(g: &Graph, env: &mut Env, arr: ArrId, out: &mut Output) -> Result<()> {
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
        out.data.push((layout.decode_row(&k)?, 1));
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

fn setop_snapshot(g: &Graph, env: &mut Env, op: SetOpKind, all: bool, la: ArrId, ra: ArrId, out: &mut Output) -> Result<()> {
    let (l, r) = (g.arr(la), g.arr(ra));
    let (ll, rl) = (l.layout.clone(), r.layout.clone());
    // collect counts per canonical key from both sides
    let mut groups: BTreeMap<Vec<u8>, (Option<Row>, i64, Option<Row>, i64)> = BTreeMap::new();
    for (tree, layout, left) in [(l.tree, &ll, true), (r.tree, &rl, false)] {
        let mut c = env.cursor(tree, Bound::Unbounded, Bound::Unbounded);
        while let Some((k, h)) = env.cursor_next(&mut c)? {
            let w = h.total();
            if w == 0 {
                continue;
            }
            let kb = layout.key_bytes(&k)?.to_vec();
            let e = groups.entry(kb).or_insert((None, 0, None, 0));
            if left {
                if e.0.is_none() && w > 0 {
                    e.0 = Some(layout.decode_row(&k)?);
                }
                e.1 += w;
            } else {
                if e.2.is_none() && w > 0 {
                    e.2 = Some(layout.decode_row(&k)?);
                }
                e.3 += w;
            }
        }
    }
    for (_, (lrep, a, rrep, b)) in groups {
        let m = setop_mult(op, all, a, b);
        if m > 0
            && let Some(rep) = lrep.or(rrep)
        {
            out.data.push((rep, m));
        }
    }
    Ok(())
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

/// A group of a TopK's input index: its key range, read with the count
/// summaries (positions count rows with their multiplicities).
struct TopKGroup<'a> {
    tree: TreeId,
    layout: &'a Layout,
    lo: Vec<u8>,
    hi: Option<Vec<u8>>,
}

impl TopKGroup<'_> {
    fn hi(&self) -> Bound<&[u8]> {
        match &self.hi {
            Some(h) => Bound::Excluded(h.as_slice()),
            None => Bound::Unbounded,
        }
    }

    /// Visit the entries from `from` up to `to` (within the group).
    fn for_each(&self, env: &mut Env, from: &[u8], to: &Bound<Vec<u8>>, f: &mut dyn FnMut(&[u8], &History) -> Result<bool>) -> Result<()> {
        let to = match to {
            Bound::Unbounded => self.hi(),
            b => b.as_ref().map(|k| k.as_slice()),
        };
        env.for_each(self.tree, Bound::Included(from), to, f)
    }

    /// Rows of the group before `key` (current weights).
    fn rank(&self, env: &mut Env, key: &[u8]) -> Result<i64> {
        Ok(env.range_summary(self.tree, Bound::Included(&self.lo), Bound::Excluded(key))?.count)
    }

    /// The entry holding the row at position `pos` (current weights), with
    /// the position of the row within the entry's multiplicity.
    fn nth(&self, env: &mut Env, pos: i64) -> Result<Option<(Box<[u8]>, i64)>> {
        Ok(env.nth(self.tree, Bound::Included(&self.lo), self.hi(), pos)?.map(|(k, _, o)| (k, o)))
    }

    /// Canonical key of an entry: rows tied in the ORDER BY share it.
    fn tie_key<'k>(&self, key: &'k [u8]) -> Result<&'k [u8]> {
        Ok(&key[..self.layout.prefix_len(key, self.layout.key_len())?])
    }
}

/// The entries of a group changed by a step, in key order, with their weight
/// before and after it and their position in the current order.
struct Changed {
    key: Vec<u8>,
    before: i64,
    after: i64,
    rank: i64,
}

/// One version (before or after a step) of the group's index: the current
/// weights, or the weights before the step derived from them and the
/// changed entries (positions move by the changes before them).
#[derive(Clone, Copy)]
struct Version<'a> {
    changed: &'a [Changed],
    before: bool,
}

impl Version<'_> {
    /// Change of position of the entries at or after `key` (but before the
    /// next changed entry) from the version before the step to the current.
    fn shift(&self, key: &[u8]) -> i64 {
        self.changed.iter().take_while(|c| c.key.as_slice() < key).map(|c| c.after - c.before).sum()
    }

    /// Rows in the group.
    fn len(&self, total: i64) -> i64 {
        if self.before { total - self.changed.iter().map(|c| c.after - c.before).sum::<i64>() } else { total }
    }

    /// The entry holding the row at position `pos`, as `TopKGroup::nth`.
    fn nth(&self, env: &mut Env, g: &TopKGroup, pos: i64) -> Result<Option<(Box<[u8]>, i64)>> {
        if !self.before {
            return g.nth(env, pos);
        }
        // the unchanged entries between two changed ones keep their order,
        // their positions shifted by the changes before them
        let mut shift = 0;
        for c in self.changed {
            let rank = c.rank - shift;
            if pos < rank {
                break;
            }
            if pos < rank + c.before.max(0) {
                return Ok(Some((c.key.clone().into_boxed_slice(), pos - rank)));
            }
            shift += c.after - c.before;
        }
        g.nth(env, pos + shift)
    }
}

/// End of a top-k window.
enum WinEnd {
    /// Before the row at this position.
    Pos(i64),
    /// With the rest of entry `last` and the entries tied with it (keys
    /// before `tie_end`, if any).
    Ties { last: Box<[u8]>, tie_end: Option<Vec<u8>> },
}

/// The rows of one version of a group that a TopK outputs.
struct Window {
    offset: i64,
    /// The entry holding the row at position `offset` (None: empty
    /// window), and that row's position within the entry's multiplicity.
    first: Option<(Box<[u8]>, i64)>,
    end: WinEnd,
    /// The entry holding the window's last row when it ends at a position,
    /// and the row's position within it (None: through the end of the
    /// group).
    last: Option<(Box<[u8]>, i64)>,
}

impl Window {
    fn of(env: &mut Env, g: &TopKGroup, v: Version, total: i64, p: &TopKPlan) -> Result<Window> {
        let n = v.len(total);
        let offset = i64::try_from(p.offset).unwrap_or(i64::MAX);
        let first = if offset < n { v.nth(env, g, offset)? } else { None };
        let limit = p.limit.map(|l| i64::try_from(l).unwrap_or(i64::MAX));
        let (end, last) = match limit {
            Some(l) if offset.saturating_add(l) <= n => {
                let e = offset.saturating_add(l);
                let last = v.nth(env, g, e - 1)?;
                match (p.with_ties, last) {
                    (true, Some((last, _))) => {
                        let tie_end = crate::types::encoding::prefix_successor(g.tie_key(&last)?);
                        (WinEnd::Ties { last, tie_end }, None)
                    }
                    (_, last) => (WinEnd::Pos(e), last),
                }
            }
            _ => (WinEnd::Pos(n), None),
        };
        Ok(Window { offset, first, end, last })
    }

    /// How many of an entry's `w` rows at positions `pos..` are in the window.
    fn rows(&self, key: &[u8], pos: i64, w: i64) -> i64 {
        if self.first.is_none() || w <= 0 {
            return 0;
        }
        let from = pos.max(self.offset);
        match &self.end {
            WinEnd::Pos(e) => (pos + w).min(*e) - from,
            WinEnd::Ties { last, tie_end } => {
                if key <= &last[..] {
                    pos + w - from
                } else if tie_end.as_ref().is_none_or(|t| key < t.as_slice()) {
                    w
                } else {
                    0
                }
            }
        }
        .max(0)
    }

    /// Key range holding the window's rows: [first, end].
    fn range(&self) -> Option<(Vec<u8>, Bound<Vec<u8>>)> {
        let first = self.first.as_ref()?.0.to_vec();
        Some(match (&self.end, &self.last) {
            (WinEnd::Ties { tie_end: Some(t), .. }, _) => (first, Bound::Excluded(t.clone())),
            (WinEnd::Pos(_), Some((l, _))) => (first, Bound::Included(l.to_vec())),
            _ => (first, Bound::Unbounded),
        })
    }

    /// Does an entry come before the window's rows, or after them?
    fn outside(&self, key: &[u8]) -> bool {
        let Some((first, _)) = &self.first else { return true };
        key < &first[..]
            || match (&self.end, &self.last) {
                (WinEnd::Ties { tie_end: Some(t), .. }, _) => key >= t.as_slice(),
                (WinEnd::Pos(_), Some((l, _))) => key > &l[..],
                _ => false,
            }
    }
}

/// Rows of a group's top-k (current weights), from the row at position
/// OFFSET on (found through the count summaries, not by walking the rows
/// before it).
fn topk_rows(env: &mut Env, g: &TopKGroup, p: &TopKPlan) -> Result<Delta> {
    let mut out = Vec::new();
    if p.limit == Some(0) {
        return Ok(out);
    }
    let total = env.range_summary(g.tree, Bound::Included(&g.lo), g.hi())?.count;
    let w = Window::of(env, g, Version { changed: &[], before: false }, total, p)?;
    let Some((from, to)) = w.range() else { return Ok(out) };
    let mut pos = g.rank(env, &from)?;
    g.for_each(env, &from, &to, &mut |k, h| {
        visited(1);
        let n = h.total();
        let take = w.rows(k, pos, n);
        if take > 0 {
            out.push((g.layout.decode_row(k)?, take));
        }
        pos += n.max(0);
        Ok(true)
    })?;
    Ok(out)
}

/// Key ranges [from, to] to visit, merged where they overlap.
fn merge_ranges(mut ranges: Vec<(Vec<u8>, Bound<Vec<u8>>)>) -> Vec<(Vec<u8>, Bound<Vec<u8>>)> {
    ranges.sort_by(|a, b| a.0.cmp(&b.0));
    let mut out: Vec<(Vec<u8>, Bound<Vec<u8>>)> = Vec::with_capacity(ranges.len());
    for (from, to) in ranges {
        if let Some((_, last)) = out.last_mut() {
            let overlaps = match &*last {
                Bound::Unbounded => true,
                Bound::Included(e) => from <= *e,
                Bound::Excluded(e) => from < *e,
            };
            if overlaps {
                let longer = match (&*last, &to) {
                    (Bound::Unbounded, _) => false,
                    (_, Bound::Unbounded) => true,
                    (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) if a != b => b > a,
                    (Bound::Excluded(_), Bound::Included(_)) => true,
                    _ => false,
                };
                if longer {
                    *last = to;
                }
                continue;
            }
        }
        out.push((from, to));
    }
    out
}

/// The change of one group's top-k at a step: the difference between the
/// window of the group before the step and after it, computed only where
/// they can differ. Rows are counted with their multiplicities in index
/// order; entries whose rows are in neither window, or (unchanged) wholly
/// in both, are not visited: those between the two windows' first rows,
/// those between their last rows, and the changed entries are.
fn topk_group(env: &mut Env, g: &TopKGroup, p: &TopKPlan, delta: &[Vec<u8>], gsn: Gsn, out: &mut Delta) -> Result<()> {
    if p.limit == Some(0) {
        return Ok(());
    }
    let mut changed: Vec<Changed> = Vec::with_capacity(delta.len());
    for key in delta {
        let (before, after) = match env.get(g.tree, key)? {
            Some(h) => (h.weight(ReadAt::Before(gsn)), h.total()),
            None => (0, 0),
        };
        if before != after {
            changed.push(Changed { key: key.clone(), before, after, rank: 0 });
        }
    }
    if changed.is_empty() {
        return Ok(());
    }
    for c in changed.iter_mut() {
        c.rank = g.rank(env, &c.key)?;
    }
    // Changes after the windows (of both versions: the positions before the
    // first change are the same in both) change nothing.
    let first = &changed[0];
    let offset = i64::try_from(p.offset).unwrap_or(i64::MAX);
    if let Some(l) = p.limit.map(|l| i64::try_from(l).unwrap_or(i64::MAX)) {
        let e = offset.saturating_add(l);
        if first.rank >= e {
            if !p.with_ties {
                return Ok(());
            }
            // (with ties, unless the first change is tied with the last row)
            let (last, _) = g.nth(env, e - 1)?.expect("row before a changed entry");
            if g.tie_key(&last)? != g.tie_key(&first.key)? {
                return Ok(());
            }
        }
    }
    let total = env.range_summary(g.tree, Bound::Included(&g.lo), g.hi())?.count;
    let (old_v, new_v) = (Version { changed: &changed, before: true }, Version { changed: &changed, before: false });
    let old = Window::of(env, g, old_v, total, p)?;
    let new = Window::of(env, g, new_v, total, p)?;
    // Where the windows can differ: the changed entries in either window,
    // and between the two windows' first rows and their last rows (unless
    // those are the same unchanged entries, at the same position in them).
    let is_changed = |k: &[u8]| changed.binary_search_by(|c| c.key.as_slice().cmp(k)).is_ok();
    let mut ranges: Vec<(Vec<u8>, Bound<Vec<u8>>)> = changed
        .iter()
        .filter(|c| !old.outside(&c.key) || !new.outside(&c.key))
        .map(|c| (c.key.clone(), Bound::Included(c.key.clone())))
        .collect();
    let span = |a: &[u8], b: &[u8]| if a <= b { (a.to_vec(), b.to_vec()) } else { (b.to_vec(), a.to_vec()) };
    let differ = |a: &(Box<[u8]>, i64), b: &(Box<[u8]>, i64)| a != b || is_changed(&a.0);
    match (&old.first, &new.first) {
        (Some(fo), Some(fn_)) => {
            if differ(fo, fn_) {
                let (a, b) = span(&fo.0, &fn_.0);
                ranges.push((a, Bound::Included(b)));
            }
            match (&old.end, &new.end, &old.last, &new.last) {
                (WinEnd::Pos(_), WinEnd::Pos(_), Some(lo), Some(ln)) => {
                    if differ(lo, ln) {
                        let (a, b) = span(&lo.0, &ln.0);
                        ranges.push((a, Bound::Included(b)));
                    }
                }
                (WinEnd::Ties { last: lo, tie_end: to }, WinEnd::Ties { last: ln, tie_end: tn }, _, _) => {
                    // (the changed entries among the ties are visited anyway)
                    if lo != ln || to != tn {
                        let end = match (to, tn) {
                            (Some(x), Some(y)) => Bound::Excluded(x.max(y).clone()),
                            _ => Bound::Unbounded,
                        };
                        ranges.push((lo.min(ln).to_vec(), end));
                    }
                }
                // one through the end of the group: from the other's last row
                (_, _, Some((l, _)), None) | (_, _, None, Some((l, _))) => ranges.push((l.to_vec(), Bound::Unbounded)),
                (WinEnd::Ties { last, .. }, _, _, _) | (_, WinEnd::Ties { last, .. }, _, _) => ranges.push((last.to_vec(), Bound::Unbounded)),
                // both through the end of the group
                _ => {}
            }
        }
        // one window is empty: the other one entirely
        _ => ranges.extend(old.range().into_iter().chain(new.range())),
    }
    for (from, to) in merge_ranges(ranges) {
        let mut pos_new = g.rank(env, &from)?;
        let mut pos_old = pos_new - old_v.shift(&from);
        g.for_each(env, &from, &to, &mut |k, h| {
            visited(1);
            let (wo, wn) = (h.weight(ReadAt::Before(gsn)), h.total());
            let d = new.rows(k, pos_new, wn) - old.rows(k, pos_old, wo);
            if d != 0 {
                out.push((g.layout.decode_row(k)?, d));
            }
            pos_new += wn.max(0);
            pos_old += wo.max(0);
            Ok(true)
        })?;
    }
    Ok(())
}

fn topk_step(g: &Graph, env: &mut Env, p: &TopKPlan, delta: &Delta, gsn: Gsn, out: &mut Output) -> Result<()> {
    let a = g.arr(p.input_arr);
    let layout = a.layout.clone();
    // changed entries by group
    let mut groups: BTreeMap<Vec<u8>, Vec<Vec<u8>>> = BTreeMap::new();
    for (row, _) in delta {
        let kv: Vec<Datum> = p.group_key.iter().map(|c| row[*c].clone()).collect();
        groups.entry(layout.encode_prefix(&kv)).or_default().push(layout.encode_row(row)?);
    }
    for (prefix, mut keys) in groups {
        keys.sort();
        keys.dedup();
        let hi = crate::types::encoding::prefix_successor(&prefix);
        let grp = TopKGroup { tree: a.tree, layout: &layout, lo: prefix, hi };
        topk_group(env, &grp, p, &keys, gsn, &mut out.data)?;
    }
    Ok(())
}

fn topk_snapshot(g: &Graph, env: &mut Env, p: &TopKPlan, out: &mut Output) -> Result<()> {
    let a = g.arr(p.input_arr);
    let layout = a.layout.clone();
    let ng = p.group_key.len();
    // groups in key order, seeking from one group's first live entry to the
    // next group
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
            next = Some(k[..layout.prefix_len(k, ng)?].to_vec());
            Ok(false)
        })?;
        let Some(prefix) = next else { break };
        let hi = crate::types::encoding::prefix_successor(&prefix);
        from = hi.clone();
        let grp = TopKGroup { tree: a.tree, layout: &layout, lo: prefix, hi };
        out.data.extend(topk_rows(env, &grp, p)?);
        if from.is_none() {
            break;
        }
    }
    Ok(())
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
fn recursive_snapshot(g: &Graph, env: &mut Env, p: &RecursivePlan, out: &mut Output) -> Result<()> {
    let mut errors = p.errors.lock();
    errors.clear();
    match crate::exec::eval_recursive(g, env, p) {
        Ok(rows) => out.data = rows,
        Err(e) if super::reduce::is_data_error(&e) => {
            let rec = err_record(&e);
            errors.insert(Vec::new(), rec.clone());
            out.errs.push((rec, 1));
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
