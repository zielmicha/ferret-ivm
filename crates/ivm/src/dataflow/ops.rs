//! Operator step / snapshot implementations (except join, reduce, window).

use super::delta::{DeltaBuf, EntryDeltas, KeySet, Keys, NodeDelta, Overlay, StepOut, Version as Weights};
use super::*;
use crate::types::Datum;
use std::ops::Bound;

/// Step dispatch for non-table nodes. Input deltas are read in batches
/// (spilled ones a batch at a time), and the output is produced into
/// `rows`, which consolidates it (and spills it when large).
pub fn step_node(
    g: &Graph,
    env: &mut Env,
    node: &Node,
    inputs: &[&NodeDelta],
    gsn: Gsn,
    all: &FxHashMap<NodeId, NodeDelta>,
    rows: DeltaBuf,
) -> Result<NodeDelta> {
    let mut out = StepOut::new(rows);
    // errors flow through every operator
    for i in inputs {
        out.errs.extend(i.errs.iter().cloned());
    }
    match step_operator(g, env, node, inputs, gsn, all, &mut out) {
        Ok(()) => out.finish(env),
        Err(e) => {
            out.rows.abort(env)?;
            Err(e)
        }
    }
}

fn step_operator(
    g: &Graph,
    env: &mut Env,
    node: &Node,
    inputs: &[&NodeDelta],
    gsn: Gsn,
    all: &FxHashMap<NodeId, NodeDelta>,
    out: &mut StepOut,
) -> Result<()> {
    match &node.kind {
        NodeKind::Table { .. } | NodeKind::Constant { .. } => {}
        NodeKind::Mfp { mfp } => inputs[0].for_each_batch(env, &mut |env, batch| {
            for (row, w) in batch {
                match mfp.eval(row) {
                    Ok(Some(r)) => out.push(env, r, *w)?,
                    Ok(None) => {}
                    Err(e) => out.err(err_record(&e), *w),
                }
            }
            Ok(())
        })?,
        NodeKind::Union { negate } => {
            for (i, inp) in inputs.iter().enumerate() {
                // (errors are never negated: they union)
                let s = if negate[i] { -1 } else { 1 };
                inp.for_each_batch(env, &mut |env, batch| {
                    for (r, w) in batch {
                        out.push(env, r.clone(), w * s)?;
                    }
                    Ok(())
                })?;
            }
        }
        NodeKind::Negate => inputs[0].for_each_batch(env, &mut |env, batch| {
            for (r, w) in batch {
                out.push(env, r.clone(), -w)?;
            }
            Ok(())
        })?,
        NodeKind::FlatMap { funcs, ordinality } => inputs[0].for_each_batch(env, &mut |env, batch| {
            for (row, w) in batch {
                match tablefunc::eval(funcs, row, *ordinality) {
                    Ok(rows) => {
                        for extra in rows {
                            let mut r = row.clone();
                            r.extend(extra);
                            out.push(env, r, *w)?;
                        }
                    }
                    Err(e) => out.err(err_record(&e), *w),
                }
            }
            Ok(())
        })?,
        NodeKind::Join(p) => join::step(g, env, node, p, gsn, all, out)?,
        NodeKind::Reduce(p) => reduce::step(g, env, p, inputs[0], gsn, out)?,
        NodeKind::Distinct { arr } => distinct_step(g, env, *arr, inputs[0], gsn, out)?,
        NodeKind::SetOp { op, all: is_all, left_arr, right_arr } => {
            setop_step(g, env, *op, *is_all, *left_arr, *right_arr, inputs[0], inputs[1], gsn, out)?
        }
        NodeKind::TopK(p) => topk_step(g, env, p, inputs[0], gsn, out)?,
        NodeKind::Window(p) => window::step(g, env, p, inputs[0], gsn, out)?,
        NodeKind::Recursive(p) => recursive_step(g, env, node, p, gsn, out)?,
        NodeKind::Temporal(p) => temporal::step(g, env, p, inputs[0], gsn, out)?,
    }
    Ok(())
}

/// The distinct key prefixes `key` gives the rows of a delta, in order.
pub fn affected_keys(env: &mut Env, delta: &NodeDelta, gsn: Gsn, key: &dyn Fn(&Row) -> Result<Vec<u8>>) -> Result<Keys> {
    let mut set = KeySet::new(gsn);
    let r = delta.for_each_batch(env, &mut |env, batch| {
        for (row, _) in batch {
            set.push(env, key(row)?)?;
        }
        Ok(())
    });
    match r {
        Ok(()) => set.finish(env),
        Err(e) => {
            set.abort(env)?;
            Err(e)
        }
    }
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
        g.scan_stream(env, a, sink)?;
        return Ok(g.errors_of(id));
    }
    let node = g.node(id);
    let mut out = SnapOut::new(sink);
    match &node.kind {
        NodeKind::Table { primary, .. } => {
            g.scan_stream(env, *primary, out.sink)?;
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

/// What a table's changes at a GSN leave for the next batch of them.
#[derive(Default)]
pub struct TableCarry {
    /// The row the last change deleted (see `table_step`).
    deleted: Option<Row>,
}

/// Apply logged changes to a base table: compute the delta against the stored
/// current values (before-images come from the primary index, so the source
/// never needs REPLICA IDENTITY FULL; unchanged TOAST values are filled in).
/// The delta goes to `emit`, which applies it to the table's arrangements
/// before anything here reads them again: a commit's changes come in
/// batches, each read against the table as the previous ones left it (so
/// the deltas of the batches add up to the commit's), and a `Truncate`
/// emits what precedes it and then retracts the stored rows a batch at a
/// time.
pub fn table_step(
    g: &Graph,
    env: &mut Env,
    node: &Node,
    changes: Vec<TableChange>,
    carry: &mut TableCarry,
    emit: &mut RowSink,
) -> Result<()> {
    let NodeKind::Table { pk, primary, .. } = &node.kind else { unreachable!() };
    let arr = g.arr(*primary);
    let layout = arr.layout.clone();
    let tree = arr.tree;
    // retract every stored row (the changes before were emitted, so this
    // includes them)
    let truncate = |env: &mut Env, emit: &mut RowSink| {
        g.scan_stream(env, *primary, &mut |env, mut rows| {
            for (_, w) in rows.iter_mut() {
                *w = -*w;
            }
            emit(env, rows)
        })
    };
    if pk.is_empty() {
        // multiset table keyed by all columns
        let mut out: Delta = Vec::new();
        for ch in changes {
            match ch {
                TableChange::Insert { row } | TableChange::Upsert { row, .. } | TableChange::UpsertIfPresent { row, .. } => out.push((row, 1)),
                TableChange::Remove { row } => out.push((row, -1)),
                TableChange::Delete { key } => out.push((key, -1)),
                TableChange::Truncate => {
                    consolidate(&mut out);
                    if !out.is_empty() {
                        emit(env, std::mem::take(&mut out))?;
                    }
                    truncate(env, emit)?;
                }
            }
        }
        consolidate(&mut out);
        return if out.is_empty() { Ok(()) } else { emit(env, out) };
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
    let mut deleted: Option<Row> = carry.deleted.take();
    for ch in changes {
        let just_deleted = std::mem::take(&mut deleted);
        let if_present = matches!(ch, TableChange::UpsertIfPresent { .. });
        match ch {
            TableChange::Upsert { mut row, unchanged } | TableChange::UpsertIfPresent { mut row, unchanged } => {
                let i = touch(env, &mut index, &mut touched, layout.encode_prefix_of(&row, pk))?;
                if !unchanged.is_empty() {
                    let (stored, after) = &touched[i];
                    let old = match after {
                        After::Stored => stored.as_ref(),
                        After::Row(r) => Some(r),
                        After::Absent => None,
                    };
                    // (a conditional upsert takes nothing from a deleted row:
                    // the delete before it need not be of its old key)
                    if let Some(o) = old.or(just_deleted.as_ref().filter(|_| !if_present)) {
                        for c in &unchanged {
                            row[*c] = o[*c].clone();
                        }
                    } else if if_present {
                        // (not copied yet: its copy brings the whole row)
                        continue;
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
                index.clear();
                let out = touched_delta(std::mem::take(&mut touched));
                if !out.is_empty() {
                    emit(env, out)?;
                }
                truncate(env, emit)?;
            }
        }
    }
    carry.deleted = deleted;
    let out = touched_delta(touched);
    if out.is_empty() { Ok(()) } else { emit(env, out) }
}

/// The delta of touched keys: `-stored, +final` for each key whose row
/// changed. Rows of different keys differ (in their key columns), so the
/// delta is consolidated once unchanged rows are left out.
fn touched_delta(touched: Vec<(Option<Row>, After)>) -> Delta {
    let mut out = Vec::new();
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
            out.push((o, -1));
        }
        if let Some(n) = new {
            out.push((n, 1));
        }
    }
    out
}

/// A base table row after the changes of one tick.
enum After {
    /// As stored (not changed).
    Stored,
    Row(Row),
    Absent,
}

// ---------------------------------------------------------------- distinct

/// The canonical key prefixes of a delta's rows, in order.
fn affected_prefixes(env: &mut Env, layout: &Layout, delta: &NodeDelta, gsn: Gsn) -> Result<Keys> {
    affected_keys(env, delta, gsn, &|row| Ok(layout.encode_prefix(&layout.key_values(row)?)))
}

/// The first entry with positive weight under `prefix` (the
/// representative) and the total weight, before the step and now: the
/// entries of `tree` with the overlay of the step's changes to them, so
/// that an entry the step removed is seen before it.
fn representatives(env: &mut Env, changes: &Overlay, tree: TreeId, layout: &Layout, prefix: &[u8]) -> Result<[(Option<Row>, i64); 2]> {
    let mut v = [(None, 0), (None, 0)];
    let hi = crate::types::encoding::prefix_successor(prefix);
    let hi = hi.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
    changes.for_each_versioned(env, tree, Bound::Included(prefix), hi, &mut |k, now, before| {
        for ((rep, total), w) in v.iter_mut().zip([before, now]) {
            if w > 0 && rep.is_none() {
                *rep = Some(layout.decode_row(k)?);
            }
            *total += w;
        }
        Ok(true)
    })?;
    Ok(v)
}

fn distinct_step(g: &Graph, env: &mut Env, arr: ArrId, delta: &NodeDelta, gsn: Gsn, out: &mut StepOut) -> Result<()> {
    let a = g.arr(arr);
    let layout = a.layout.clone();
    let changes = delta.overlay(arr)?;
    affected_prefixes(env, &layout, delta, gsn)?.for_each(env, &mut |env, p| {
        let [(old, oc), (new, nc)] = representatives(env, &changes, a.tree, &layout, p)?;
        if oc > 0
            && let Some(o) = old
        {
            out.push(env, o, -1)?;
        }
        if nc > 0
            && let Some(n) = new
        {
            out.push(env, n, 1)?;
        }
        Ok(())
    })
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
    ld: &NodeDelta,
    rd: &NodeDelta,
    gsn: Gsn,
    out: &mut StepOut,
) -> Result<()> {
    let (l, r) = (g.arr(la), g.arr(ra));
    let (ll, rl) = (l.layout.clone(), r.layout.clone());
    let mut prefixes = KeySet::new(gsn);
    for (layout, d) in [(&ll, ld), (&rl, rd)] {
        let res = d.for_each_batch(env, &mut |env, batch| {
            for (row, _) in batch {
                prefixes.push(env, layout.encode_prefix(&layout.key_values(row)?))?;
            }
            Ok(())
        });
        if let Err(e) = res {
            prefixes.abort(env)?;
            return Err(e);
        }
    }
    let (lc, rc) = (ld.overlay(la)?, rd.overlay(ra)?);
    prefixes.finish(env)?.for_each(env, &mut |env, p| {
        let [(lrep_old, a_old), (lrep_new, a_new)] = representatives(env, &lc, l.tree, &ll, p)?;
        let [(rrep_old, b_old), (rrep_new, b_new)] = representatives(env, &rc, r.tree, &rl, p)?;
        let m_old = setop_mult(op, all, a_old, b_old);
        let m_new = setop_mult(op, all, a_new, b_new);
        if m_old > 0
            && let Some(rep) = lrep_old.or(rrep_old)
        {
            out.push(env, rep, -m_old)?;
        }
        if m_new > 0
            && let Some(rep) = lrep_new.or(rrep_new)
        {
            out.push(env, rep, m_new)?;
        }
        Ok(())
    })
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

    /// Visit the entries from `from` up to `to` (within the group) with
    /// their weights now and before the step whose changes to the group's
    /// entries are `deltas` (also the entries the step removed).
    fn for_each_versioned(&self, env: &mut Env, deltas: &EntryDeltas, from: &[u8], to: &Bound<Vec<u8>>, f: &mut super::delta::VersionedFn) -> Result<()> {
        let to = match to {
            Bound::Unbounded => self.hi(),
            b => b.as_ref().map(|k| k.as_slice()),
        };
        deltas.for_each_versioned(env, self.tree, Bound::Included(from), to, f)
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

/// Windows of at most this many rows (OFFSET included) are read from the
/// start of their group: that costs less than locating them.
const SMALL_WINDOW: u64 = 32;

fn small_window(p: &TopKPlan) -> bool {
    p.limit.is_some_and(|l| p.offset.saturating_add(l) <= SMALL_WINDOW)
}

/// Rows of a group's top-k, now or before the step, walking the group from
/// its start (small windows, and groups with many changes).
fn topk_walk(env: &mut Env, g: &TopKGroup, p: &TopKPlan, version: Weights) -> Result<Delta> {
    let mut out = Vec::new();
    let (mut skipped, mut taken) = (0u64, 0u64);
    let limit = p.limit.unwrap_or(u64::MAX);
    if limit == 0 {
        return Ok(out);
    }
    let mut last_key: Option<Vec<u8>> = None;
    version.for_each(env, g.tree, Bound::Included(&g.lo), g.hi(), &mut |k, w| {
        visited(1);
        let mut w = w;
        if w <= 0 {
            return Ok(true);
        }
        if taken >= limit {
            // past the limit only rows tied with the last one remain
            return match &last_key {
                Some(lk) if p.with_ties && g.tie_key(k)? == lk.as_slice() => {
                    out.push((g.layout.decode_row(k)?, w));
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
        out.push((g.layout.decode_row(k)?, take as i64));
        taken += take;
        if p.with_ties {
            last_key = Some(g.tie_key(k)?.to_vec());
            Ok(true)
        } else {
            Ok(taken < limit)
        }
    })?;
    Ok(out)
}

/// Rows of a group's top-k (current weights), from the row at position
/// OFFSET on (found through the count summaries, not by walking the rows
/// before it).
fn topk_rows(env: &mut Env, g: &TopKGroup, p: &TopKPlan) -> Result<Delta> {
    if small_window(p) {
        return topk_walk(env, g, p, Weights::Now);
    }
    let mut out = Vec::new();
    if p.limit == Some(0) {
        return Ok(out);
    }
    let total = env.range_summary(g.tree, Bound::Included(&g.lo), g.hi())?.count;
    let w = Window::of(env, g, Version { changed: &[], before: false }, total, p)?;
    let Some((from, to)) = w.range() else { return Ok(out) };
    let mut pos = g.rank(env, &from)?;
    let to = match &to {
        Bound::Unbounded => g.hi(),
        b => b.as_ref().map(|k| k.as_slice()),
    };
    env.for_each(g.tree, Bound::Included(&from), to, &mut |k, h| {
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
fn topk_group(env: &mut Env, g: &TopKGroup, p: &TopKPlan, deltas: &EntryDeltas, out: &mut Delta) -> Result<()> {
    if p.limit == Some(0) {
        return Ok(());
    }
    if small_window(p) {
        out.extend(topk_walk(env, g, p, Weights::Before(deltas))?.into_iter().map(|(r, w)| (r, -w)));
        out.extend(topk_walk(env, g, p, Weights::Now)?);
        return Ok(());
    }
    let mut changed: Vec<Changed> = Vec::with_capacity(deltas.entries().len());
    for (key, d) in deltas.entries() {
        let after = env.get(g.tree, key)?.map(|h| h.total()).unwrap_or(0);
        changed.push(Changed { key: key.clone(), before: after - d, after, rank: 0 });
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
        g.for_each_versioned(env, deltas, &from, &to, &mut |k, wn, wo| {
            visited(1);
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

/// A group's changes beyond this many entries are not collected: both
/// windows are read by walking the group with the overlay of the delta.
const MANY_CHANGES: usize = 4096;

fn topk_step(g: &Graph, env: &mut Env, p: &TopKPlan, delta: &NodeDelta, _gsn: Gsn, out: &mut StepOut) -> Result<()> {
    let a = g.arr(p.input_arr);
    let layout = a.layout.clone();
    // the changes by group, in the input index's key order
    let changes = delta.overlay(p.input_arr)?;
    changes.groups(env, &layout, p.group_key.len(), MANY_CHANGES, &mut |env, prefix, deltas| {
        let hi = crate::types::encoding::prefix_successor(prefix);
        let grp = TopKGroup { tree: a.tree, layout: &layout, lo: prefix.to_vec(), hi };
        let mut d = Vec::new();
        match deltas {
            Some(deltas) => topk_group(env, &grp, p, deltas, &mut d)?,
            None if p.limit == Some(0) => {}
            None => {
                d.extend(topk_walk(env, &grp, p, Weights::BeforeStep(&changes))?.into_iter().map(|(r, w)| (r, -w)));
                d.extend(topk_walk(env, &grp, p, Weights::Now)?);
            }
        }
        for (r, w) in d {
            out.push(env, r, w)?;
        }
        Ok(())
    })
}

fn topk_snapshot(g: &Graph, env: &mut Env, p: &TopKPlan, out: &mut SnapOut) -> Result<()> {
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
        for (r, w) in topk_rows(env, &grp, p)? {
            out.push(env, r, w)?;
        }
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

fn recursive_step(g: &Graph, env: &mut Env, node: &Node, p: &RecursivePlan, gsn: Gsn, out: &mut StepOut) -> Result<()> {
    let _ = (node, gsn);
    g.scan_stream(env, p.out_arr, &mut |env, old| {
        for (r, w) in old {
            out.push(env, r, -w)?;
        }
        Ok(())
    })?;
    let mut errors = p.errors.lock();
    match crate::exec::eval_recursive(g, env, p) {
        Ok(rows) => {
            for (r, w) in rows {
                out.push(env, r, w)?;
            }
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
