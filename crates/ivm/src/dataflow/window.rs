//! Window functions.
//!
//! The input is arranged by (partition, order, rest), with summaries for
//! the aggregates whose frames reach a partition's ends. Values are computed
//! over a run of consecutive rows of a partition: frames slide over it (a
//! row entering the frame is added, one leaving it subtracted, MIN/MAX kept
//! in a monotonic deque), and what lies outside the run comes from the
//! index (summaries for the parts of frames up to a partition's ends, rows
//! at given positions through the count summaries).
//!
//! A change to a partition recomputes only the rows whose values can
//! change, given the functions and their frames, over the run their values
//! depend on, before and after the change, and emits the difference. That is
//! the whole partition only when the change can change every row (for
//! example `row_number` after an insert at its start), which is intrinsic.

use super::agg::AggSpec;
use super::*;
use crate::sql::plan::{FrameBound, FrameUnits, RangeOffsets, SortKey, WindowFunc};
use crate::storage::{AggKind, Summary};
use crate::types::{Datum, SqlType};
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::Bound;
use std::sync::atomic::AtomicBool;

#[derive(Clone, Debug)]
pub struct WinSpec {
    pub func: WindowFunc,
    pub args: Vec<usize>,
    pub frame: crate::sql::plan::Frame,
    pub arg_types: Vec<SqlType>,
    pub ret: ColumnType,
    pub range: Option<RangeOffsets>,
}

impl WinSpec {
    fn agg(&self) -> Option<AggSpec> {
        let WindowFunc::Agg(func) = self.func else { return None };
        Some(AggSpec {
            func,
            arg: self.args.first().copied(),
            arg2: self.args.get(1).copied(),
            filter: None,
            arg_type: self.arg_types.first().cloned().unwrap_or(SqlType::Int8),
            ret: self.ret.clone(),
            fraction: None,
        })
    }

    /// An aggregate over its frame whose frame parts up to a partition's
    /// ends come from the input index's summaries.
    fn summarized(&self) -> bool {
        matches!(self.func, WindowFunc::Agg(f) if !f.is_ordered())
            && (self.frame.start == FrameBound::UnboundedPreceding || self.frame.end == FrameBound::UnboundedFollowing)
    }

    /// Computed over the rows of its frame (all of them in the run).
    fn reads_frame_rows(&self) -> bool {
        matches!(self.func, WindowFunc::Agg(f) if f.is_ordered())
    }

    fn frame_based(&self) -> bool {
        matches!(self.func, WindowFunc::Agg(_) | WindowFunc::FirstValue | WindowFunc::LastValue | WindowFunc::NthValue)
    }

    /// Uses peer groups (rank-like functions, RANGE and GROUPS frames).
    fn uses_peers(&self) -> bool {
        matches!(self.func, WindowFunc::Rank | WindowFunc::DenseRank | WindowFunc::PercentRank | WindowFunc::CumeDist)
            || (self.frame_based() && self.frame.units != FrameUnits::Rows)
    }
}

/// Summaries the input index keeps for `funcs` (in function order): those
/// of the aggregates whose frame reaches a partition's start or end.
pub fn input_summaries(funcs: &[WinSpec]) -> Vec<(AggKind, Expr)> {
    funcs.iter().filter(|f| f.summarized()).flat_map(|f| f.agg().unwrap().summaries()).collect()
}

#[derive(Clone, Debug)]
pub struct WindowPlan {
    pub partition: Vec<usize>,
    pub order: Vec<SortKey>,
    pub funcs: Vec<WinSpec>,
    pub input_arr: ArrId,
    /// Partitions in error (an error in any of its rows: none of them is
    /// output), by partition key prefix, so that a change can be applied to
    /// part of a partition; checkpointed with the graph's error state.
    pub errors: OpErrors,
    /// Whether `errors` is known (computed by a snapshot, or restored from a
    /// checkpoint that kept it; hydration computes it otherwise); while it
    /// is not, every change recomputes its whole partition.
    pub errors_known: Arc<AtomicBool>,
}

impl WindowPlan {
    pub fn new(partition: Vec<usize>, order: Vec<SortKey>, funcs: Vec<WinSpec>, input_arr: ArrId) -> WindowPlan {
        WindowPlan { partition, order, funcs, input_arr, errors: Default::default(), errors_known: Default::default() }
    }

    pub fn fingerprint(&self, cx: &FpCtx, fp: &mut Vec<u8>) {
        fp.extend_from_slice(format!("{:?}{:?}", self.partition, self.order).as_bytes());
        fp.extend_from_slice(cx.arr(self.input_arr));
        for f in &self.funcs {
            fp.extend_from_slice(format!("{:?}{:?}{:?}{:?}", f.func, f.args, f.frame, f.range).as_bytes());
        }
    }

    /// Per function, its summaries' indexes in the input layout (empty if
    /// it has none), in the order of `input_summaries`.
    fn slots(&self) -> Vec<Vec<usize>> {
        let mut next = 0;
        self.funcs
            .iter()
            .map(|f| {
                if !f.summarized() {
                    return vec![];
                }
                let n = f.agg().unwrap().summaries().len();
                next += n;
                (next - n..next).collect()
            })
            .collect()
    }
}

fn order_cmp(a: &Row, b: &Row, order: &[SortKey]) -> Ordering {
    for k in order {
        let (x, y) = (&a[k.col], &b[k.col]);
        let c = match (x.is_null(), y.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => if k.spec.nulls_first { Ordering::Less } else { Ordering::Greater },
            (false, true) => if k.spec.nulls_first { Ordering::Greater } else { Ordering::Less },
            _ => {
                let c = x.sql_cmp(y);
                if k.spec.desc { c.reverse() } else { c }
            }
        };
        if c != Ordering::Equal {
            return c;
        }
    }
    Ordering::Equal
}

// ------------------------------------------------------------------ runs

/// Consecutive rows of a partition (one per unit of multiplicity), at
/// positions `base..base + rows.len()` of its `n` rows, with what lies
/// outside them: the summaries of the rows before and after them (all of
/// the input layout's summaries; None if there are none), and the rows at
/// other positions.
struct Run<'a> {
    rows: Vec<&'a Row>,
    base: i64,
    n: i64,
    before: Option<Summary>,
    after: Option<Summary>,
    outside: &'a mut dyn FnMut(i64) -> Result<Row>,
}

impl Run<'_> {
    fn end(&self) -> i64 {
        self.base + self.rows.len() as i64
    }

    /// The row at partition position `pos`.
    fn row_at(&mut self, pos: i64) -> Result<Row> {
        if pos >= self.base && pos < self.end() {
            Ok(self.rows[(pos - self.base) as usize].clone())
        } else {
            (self.outside)(pos)
        }
    }
}

/// Peer groups of a run (relative positions).
struct Peers<'a> {
    group: Vec<usize>,
    gstart: Vec<usize>,
    gend: Vec<usize>,
    order: &'a [SortKey],
    /// RANGE offsets: [lo, hi) of the non-null ORDER BY values and those
    /// values in the comparison domain (computed once per run).
    region: std::cell::OnceCell<(usize, usize, Vec<Datum>)>,
}

impl<'a> Peers<'a> {
    fn of(rs: &[&Row], order: &'a [SortKey]) -> Peers<'a> {
        let n = rs.len();
        let mut group = vec![0usize; n];
        let mut gstart = if n > 0 { vec![0usize] } else { vec![] };
        for i in 1..n {
            if order_cmp(rs[i - 1], rs[i], order) == Ordering::Equal {
                group[i] = group[i - 1];
            } else {
                group[i] = group[i - 1] + 1;
                gstart.push(i);
            }
        }
        let ng = gstart.len();
        let gend = (0..ng).map(|g| if g + 1 < ng { gstart[g + 1] - 1 } else { n - 1 }).collect();
        Peers { group, gstart, gend, order, region: Default::default() }
    }
}

/// ORDER BY value of a row in the comparison domain of RANGE offsets.
fn range_value(r: &Row, col: usize, ro: &RangeOffsets) -> Datum {
    match &ro.cmp_type {
        Some(t) if !r[col].is_null() => crate::expr::cast::cast_datum(&r[col], t, -1, false).unwrap_or(Datum::Null),
        _ => r[col].clone(),
    }
}

/// `v` moved by `offset` toward the start (`back`) or the end of the order
/// (None: out of range, beyond every value in that direction).
fn range_shift(v: &Datum, offset: &Datum, ro: &RangeOffsets, desc: bool, back: bool) -> Option<Datum> {
    let f = match crate::expr::func::func_def(if back != desc { ro.minus } else { ro.plus }).imp {
        crate::expr::func::Imp::F2(f) => f,
        _ => return None,
    };
    f(v, offset).ok()
}

/// A RANGE offset bound of row `i` (relative): the first row not before
/// (start) or the last row not after (end) the current value minus/plus the
/// offset.
fn range_bound(rs: &[&Row], ro: &RangeOffsets, p: &Peers, i: usize, offset: &Datum, preceding: bool, start: bool) -> i64 {
    let key = &p.order[0];
    let (col, desc) = (key.col, key.spec.desc);
    let cur = range_value(rs[i], col, ro);
    if cur.is_null() {
        // a NULL ordering value frames its peers
        let g = p.group[i];
        return if start { p.gstart[g] as i64 } else { p.gend[g] as i64 };
    }
    // non-null values are contiguous and sorted
    let (lo, hi, region) = p.region.get_or_init(|| {
        let lo = rs.iter().position(|r| !r[col].is_null()).unwrap_or(0);
        let hi = rs.iter().rposition(|r| !r[col].is_null()).map(|x| x + 1).unwrap_or(lo);
        (lo, hi, rs[lo..hi].iter().map(|r| range_value(r, col, ro)).collect())
    });
    let (lo, hi) = (*lo, *hi);
    let Some(target) = range_shift(&cur, offset, ro, desc, preceding) else {
        // out of range: as in PostgreSQL's in_range functions, the bound lies
        // beyond every value in the offset's direction (before all of them
        // for PRECEDING, after all of them for FOLLOWING)
        return match (start, preceding) {
            (true, true) => lo as i64,
            (true, false) => hi as i64,
            (false, true) => lo as i64 - 1,
            (false, false) => hi as i64 - 1,
        };
    };
    let before = |v: &Datum, strict: bool| {
        let c = v.sql_cmp(&target);
        let c = if desc { c.reverse() } else { c };
        if strict { c == Ordering::Less } else { c != Ordering::Greater }
    };
    if start {
        (lo + region.partition_point(|v| before(v, true))) as i64
    } else {
        (lo + region.partition_point(|v| before(v, false))) as i64 - 1
    }
}

/// Frame of the run's row `i` (relative), as partition positions [s, e).
/// Exact when the run holds the frame's bounds (or the frame reaches the
/// partition's ends there).
fn frame_bounds(run: &Run, f: &WinSpec, p: &Peers, i: usize) -> (i64, i64) {
    let rs = &run.rows;
    let (base, n, len) = (run.base, run.n, rs.len() as i64);
    let (group, gstart, gend) = (&p.group, &p.gstart, &p.gend);
    let at = base + i as i64;
    let g = group[i];
    let ng = gstart.len() as i64;
    let ro = f.range.as_ref().filter(|_| f.frame.units == FrameUnits::Range);
    let fr = &f.frame;
    let start = match (fr.units, fr.start) {
        (FrameUnits::Range, FrameBound::Preceding(_) | FrameBound::Following(_)) if ro.is_some() => {
            let ro = ro.unwrap();
            base + range_bound(rs, ro, p, i, ro.start.as_ref().unwrap(), matches!(fr.start, FrameBound::Preceding(_)), true).max(0)
        }
        (_, FrameBound::UnboundedPreceding) => 0,
        (FrameUnits::Rows, FrameBound::Preceding(k)) => at.saturating_sub(k).max(0),
        (FrameUnits::Rows, FrameBound::CurrentRow) => at,
        // a start past the partition is an empty frame at its end
        (FrameUnits::Rows, FrameBound::Following(k)) => at.saturating_add(k).min(n),
        (FrameUnits::Groups, FrameBound::Preceding(k)) => base + gstart[(g as i64).saturating_sub(k).max(0) as usize] as i64,
        (FrameUnits::Groups, FrameBound::Following(k)) => {
            let gg = (g as i64).saturating_add(k);
            if gg >= ng { base + len } else { base + gstart[gg as usize] as i64 }
        }
        (_, FrameBound::CurrentRow) => base + gstart[g] as i64,
        (_, FrameBound::UnboundedFollowing) => n,
        (FrameUnits::Range, _) => base + gstart[g] as i64,
    };
    // (inclusive)
    let end = match (fr.units, fr.end) {
        (FrameUnits::Range, FrameBound::Preceding(_) | FrameBound::Following(_)) if ro.is_some() => {
            let ro = ro.unwrap();
            base + range_bound(rs, ro, p, i, ro.end.as_ref().unwrap(), matches!(fr.end, FrameBound::Preceding(_)), false)
        }
        (_, FrameBound::UnboundedFollowing) => n - 1,
        (FrameUnits::Rows, FrameBound::Following(k)) => at.saturating_add(k).min(n - 1),
        (FrameUnits::Rows, FrameBound::CurrentRow) => at,
        (FrameUnits::Rows, FrameBound::Preceding(k)) => at.saturating_sub(k),
        (FrameUnits::Groups, FrameBound::Following(k)) => base + gend[((g as i64).saturating_add(k).min(ng - 1)) as usize] as i64,
        (FrameUnits::Groups, FrameBound::Preceding(k)) => {
            let gg = (g as i64).saturating_sub(k);
            if gg < 0 { base - 1 } else { base + gend[gg as usize] as i64 }
        }
        (_, FrameBound::CurrentRow) => base + gend[g] as i64,
        (_, FrameBound::UnboundedPreceding) => -1,
        (FrameUnits::Range, _) => base + gend[g] as i64,
    };
    if end < start { (start, start) } else { (start, end + 1) }
}

/// An aggregate over frames that slide forward over a run (both bounds
/// non-decreasing): rows entering are added, rows leaving subtracted from
/// the invertible states, and MIN/MAX are kept in monotonic deques of
/// candidates (the leftmost of equal values first, as summaries keep it).
struct Sliding {
    kinds: Vec<(AggKind, Expr)>,
    acc: Summary,
    deques: Vec<Option<VecDeque<(usize, Datum)>>>,
    from: usize,
    to: usize,
}

impl Sliding {
    fn new(kinds: Vec<(AggKind, Expr)>) -> Sliding {
        let spec: Vec<AggKind> = kinds.iter().map(|(k, _)| *k).collect();
        let deques = kinds.iter().map(|(k, _)| matches!(k, AggKind::Min | AggKind::Max).then(VecDeque::new)).collect();
        Sliding { kinds, acc: Summary::empty(&spec), deques, from: 0, to: 0 }
    }

    fn add(&mut self, i: usize, row: &Row) -> Result<()> {
        self.acc.count += 1;
        for (k, (kind, e)) in self.kinds.iter().enumerate() {
            let v = e.eval(row)?;
            match &mut self.deques[k] {
                Some(d) => {
                    if v.is_null() {
                        continue;
                    }
                    let worse = |x: &Datum| {
                        let c = x.sql_cmp(&v);
                        if *kind == AggKind::Min { c == Ordering::Greater } else { c == Ordering::Less }
                    };
                    while d.back().is_some_and(|(_, x)| worse(x)) {
                        d.pop_back();
                    }
                    d.push_back((i, v));
                }
                None => kind.add(&mut self.acc.aggs[k], &v, 1)?,
            }
        }
        Ok(())
    }

    fn remove(&mut self, i: usize, row: &Row) -> Result<()> {
        self.acc.count -= 1;
        for (k, (kind, e)) in self.kinds.iter().enumerate() {
            match &mut self.deques[k] {
                Some(d) => {
                    if d.front().is_some_and(|(j, _)| *j == i) {
                        d.pop_front();
                    }
                }
                None => kind.add(&mut self.acc.aggs[k], &e.eval(row)?, -1)?,
            }
        }
        Ok(())
    }

    /// Make the frame [from, to) of the run's rows.
    fn slide(&mut self, rows: &[&Row], from: usize, to: usize) -> Result<()> {
        if from >= self.to {
            let spec: Vec<AggKind> = self.kinds.iter().map(|(k, _)| *k).collect();
            self.acc = Summary::empty(&spec);
            for d in self.deques.iter_mut().flatten() {
                d.clear();
            }
            (self.from, self.to) = (from, from);
        }
        while self.to < to {
            self.add(self.to, rows[self.to])?;
            self.to += 1;
        }
        while self.from < from {
            self.remove(self.from, rows[self.from])?;
            self.from += 1;
        }
        Ok(())
    }

    /// The frame's summary.
    fn state(&self) -> Result<Summary> {
        let mut s = self.acc.clone();
        for (k, d) in self.deques.iter().enumerate() {
            if let Some(d) = d {
                let (kind, _) = &self.kinds[k];
                s.aggs[k] = kind.empty();
                if let Some((_, v)) = d.front() {
                    kind.add(&mut s.aggs[k], v, 1)?;
                }
            }
        }
        Ok(s)
    }
}

/// The part of summary `s` (over all of the input layout's summaries) of
/// one function's slots.
fn part_of(s: &Summary, slots: &[usize]) -> Summary {
    Summary { count: s.count, aggs: slots.iter().map(|i| s.aggs[*i].clone()).collect() }
}

/// Output rows (input row ++ window values) of the run's rows at relative
/// positions `from..to`, consecutive equal rows (units of one row's
/// multiplicity with equal values) merged into one with a weight.
fn compute(run: &mut Run, order: &[SortKey], funcs: &[WinSpec], slots: &[Vec<usize>], from: usize, to: usize) -> Result<Vec<(Row, i64)>> {
    let len = run.rows.len();
    let to = to.min(len);
    if from >= to {
        return Ok(vec![]);
    }
    let p = Peers::of(&run.rows, order);
    let mut cols: Vec<Vec<Datum>> = Vec::with_capacity(funcs.len());
    for (fi, f) in funcs.iter().enumerate() {
        cols.push(compute_func(run, f, &slots[fi], &p, from, to)?);
    }
    let mut out: Vec<(Row, i64)> = Vec::with_capacity(to - from);
    for i in from..to {
        let mut r: Row = Vec::with_capacity(run.rows[i].len() + funcs.len());
        r.extend_from_slice(run.rows[i]);
        for c in &cols {
            r.push(c[i - from].clone());
        }
        match out.last_mut() {
            Some((last, w)) if crate::types::datum::exact_row_cmp(last, &r) == Ordering::Equal => *w += 1,
            _ => out.push((r, 1)),
        }
    }
    Ok(out)
}

/// One function's values for the run's rows `from..to` (relative).
fn compute_func(run: &mut Run, f: &WinSpec, slots: &[usize], p: &Peers, from: usize, to: usize) -> Result<Vec<Datum>> {
    let (group, gstart, gend) = (&p.group, &p.gstart, &p.gend);
    let (base, n) = (run.base, run.n);
    let mut out = Vec::with_capacity(to - from);
    match f.func {
        WindowFunc::RowNumber => out.extend((from..to).map(|i| Datum::Int8(base + i as i64 + 1))),
        WindowFunc::Rank => out.extend((from..to).map(|i| Datum::Int8(base + gstart[group[i]] as i64 + 1))),
        // (a run of dense_rank starts at its partition's start)
        WindowFunc::DenseRank => out.extend((from..to).map(|i| Datum::Int8(group[i] as i64 + 1))),
        WindowFunc::PercentRank => out.extend((from..to).map(|i| {
            Datum::Float8(if n > 1 { (base + gstart[group[i]] as i64) as f64 / (n - 1) as f64 } else { 0.0 })
        })),
        WindowFunc::CumeDist => out.extend((from..to).map(|i| Datum::Float8((base + gend[group[i]] as i64 + 1) as f64 / n as f64))),
        WindowFunc::Ntile => {
            for i in from..to {
                let Some(k) = run.rows[i][f.args[0]].as_i64() else {
                    out.push(Datum::Null);
                    continue;
                };
                if k <= 0 {
                    return Err(SqlError::new("22014", "argument of ntile must be greater than zero"));
                }
                let (k, at) = (k as i128, (base + i as i64) as i128);
                let (per, extra) = ((n as i128) / k, (n as i128) % k);
                // the first `extra` buckets have per+1 rows
                let bucket = if at < extra * (per + 1) { at / (per + 1) } else { extra + (at - extra * (per + 1)) / per.max(1) };
                out.push(Datum::Int4(bucket as i32 + 1));
            }
        }
        WindowFunc::Lag | WindowFunc::Lead => {
            for i in from..to {
                let r = run.rows[i];
                let off = match f.args.get(1) {
                    Some(c) => match r[*c].as_i64() {
                        Some(v) => v,
                        None => {
                            out.push(Datum::Null);
                            continue;
                        }
                    },
                    None => 1,
                };
                let at = base + i as i64;
                let j = if f.func == WindowFunc::Lag { at.saturating_sub(off) } else { at.saturating_add(off) };
                if j >= 0 && j < n {
                    let v = run.row_at(j)?[f.args[0]].clone();
                    out.push(v);
                } else {
                    out.push(f.args.get(2).map(|c| r[*c].clone()).unwrap_or(Datum::Null));
                }
            }
        }
        WindowFunc::FirstValue | WindowFunc::LastValue | WindowFunc::NthValue => {
            for i in from..to {
                let (s, e) = frame_bounds(run, f, p, i);
                let at = match f.func {
                    WindowFunc::FirstValue => (s < e).then_some(s),
                    WindowFunc::LastValue => (s < e).then_some(e - 1),
                    _ => {
                        // a NULL position yields NULL (as in PostgreSQL)
                        let Some(k) = run.rows[i][f.args[1]].as_i64() else {
                            out.push(Datum::Null);
                            continue;
                        };
                        if k <= 0 {
                            return Err(SqlError::new("22016", "argument of nth_value must be greater than zero"));
                        }
                        let idx = s.saturating_add(k - 1);
                        (idx < e).then_some(idx)
                    }
                };
                match at {
                    Some(j) => {
                        let v = run.row_at(j)?[f.args[0]].clone();
                        out.push(v);
                    }
                    None => out.push(Datum::Null),
                }
            }
        }
        WindowFunc::Agg(func) => {
            let spec = f.agg().unwrap();
            let kinds = spec.summaries();
            let spec_kinds: Vec<AggKind> = kinds.iter().map(|(k, _)| *k).collect();
            let all: Vec<usize> = (0..kinds.len()).collect();
            let mut sliding = Sliding::new(kinds);
            let end = run.end();
            for i in from..to {
                let (s, e) = frame_bounds(run, f, p, i);
                // the frame's rows in the run
                let (rs, re) = ((s.max(base) - base) as usize, (e.min(end).max(base) - base) as usize);
                let (rs, re) = (rs.min(re), re);
                if func.is_ordered() {
                    debug_assert!(s >= base && e <= end, "frame [{s}, {e}) of an ordered aggregate outside the run [{base}, {end})");
                    out.push(spec.finalize_ordered(&mut run.rows[rs..re].iter().map(|r| (*r, 1)))?);
                    continue;
                }
                sliding.slide(&run.rows, rs, re)?;
                // the parts up to the partition's ends outside the run (the
                // run holds the rest of the frame)
                debug_assert!(s >= base || (s == 0 && run.before.is_some()), "frame start {s} before the run at {base}");
                debug_assert!(e <= end || (e == n && run.after.is_some()), "frame end {e} after the run at {end}");
                let mut st = Summary::empty(&spec_kinds);
                if s < base
                    && e > s
                    && let Some(b) = &run.before
                {
                    debug_assert!(e >= base, "frame [{s}, {e}) ends before the run at {base}");
                    st.combine(&part_of(b, slots));
                }
                st.combine(&sliding.state()?);
                if e > end
                    && e > s
                    && let Some(a) = &run.after
                {
                    debug_assert!(s <= end, "frame [{s}, {e}) starts after the run at {end}");
                    st.combine(&part_of(a, slots));
                }
                out.push(spec.finalize(&st, &all)?);
            }
        }
    }
    Ok(out)
}

/// Window values of a whole partition: `rows` in window order with their
/// multiplicities; returns the output rows (input ++ values) with theirs.
pub fn compute_partition(rows: &[(Row, i64)], order: &[SortKey], funcs: &[WinSpec]) -> Result<Vec<(Row, i64)>> {
    let mut rs: Vec<&Row> = Vec::new();
    for (r, w) in rows {
        for _ in 0..(*w).max(0) {
            rs.push(r);
        }
    }
    let n = rs.len();
    let mut outside = |pos: i64| -> Result<Row> { Err(SqlError::internal(format!("window row {pos} outside its partition"))) };
    let mut run = Run { rows: rs, base: 0, n: n as i64, before: None, after: None, outside: &mut outside };
    let slots = vec![vec![]; funcs.len()];
    compute(&mut run, order, funcs, &slots, 0, n)
}

// ------------------------------------------------------------------ partitions in the index

/// A partition of the input index: its key range, read through the count
/// summaries (positions count rows with their multiplicities, current
/// weights).
struct Part<'a> {
    tree: TreeId,
    layout: &'a Layout,
    lo: Vec<u8>,
    hi: Option<Vec<u8>>,
}

impl Part<'_> {
    fn hi(&self) -> Bound<&[u8]> {
        match &self.hi {
            Some(h) => Bound::Excluded(h.as_slice()),
            None => Bound::Unbounded,
        }
    }

    fn len(&self, env: &mut Env) -> Result<i64> {
        Ok(env.range_summary(self.tree, Bound::Included(&self.lo), self.hi())?.count)
    }

    /// Rows before `key`.
    fn rank(&self, env: &mut Env, key: &[u8]) -> Result<i64> {
        Ok(env.range_summary(self.tree, Bound::Included(&self.lo), Bound::Excluded(key))?.count)
    }

    /// The entry holding the row at position `pos`.
    fn nth(&self, env: &mut Env, pos: i64) -> Result<Option<Box<[u8]>>> {
        if pos < 0 {
            return Ok(None);
        }
        Ok(env.nth(self.tree, Bound::Included(&self.lo), self.hi(), pos)?.map(|(k, _, _)| k))
    }

    fn row_at(&self, env: &mut Env, pos: i64) -> Result<Row> {
        match self.nth(env, pos)? {
            Some(k) => self.layout.decode_row(&k),
            None => Err(SqlError::internal(format!("window row {pos} beyond its partition"))),
        }
    }

    /// The peer group of an entry (its canonical partition and ORDER BY
    /// key) as positions [start, end).
    fn peers(&self, env: &mut Env, key: &[u8]) -> Result<(i64, i64)> {
        let tie = &key[..self.layout.prefix_len(key, self.layout.key_len())?];
        let start = self.rank(env, tie)?;
        let end = match crate::types::encoding::prefix_successor(tie) {
            Some(s) => self.rank(env, &s)?,
            None => self.len(env)?,
        };
        Ok((start, end))
    }
}

/// How far from a changed row the rows whose values it can change reach
/// (in one direction), or how far from a row its frame reaches.
#[derive(Clone, Debug)]
enum Reach {
    None,
    Rows(i64),
    /// Peer groups (0: the row's own).
    Groups(i64),
    /// A RANGE offset (of these offsets): rows whose ORDER BY value is
    /// within it.
    Value(Datum, RangeOffsets),
    All,
}

/// Groups walked one by one at most; a reach of more is taken as `All`.
const MAX_GROUP_WALK: i64 = 32;

/// Reach of a frame bound (how far from a row its frame's bound lies),
/// toward the partition's start for a start bound, its end for an end bound.
fn bound_reach(f: &WinSpec, start: bool) -> Reach {
    let (b, ro) = (if start { f.frame.start } else { f.frame.end }, f.range.as_ref());
    let offset = |s: bool| ro.and_then(|r| if s { r.start.clone() } else { r.end.clone() });
    match (f.frame.units, b, start) {
        (_, FrameBound::UnboundedPreceding, true) | (_, FrameBound::UnboundedFollowing, false) => Reach::All,
        (FrameUnits::Rows, FrameBound::Preceding(k), true) | (FrameUnits::Rows, FrameBound::Following(k), false) => Reach::Rows(k),
        (FrameUnits::Groups, FrameBound::Preceding(k), true) | (FrameUnits::Groups, FrameBound::Following(k), false) => Reach::Groups(k),
        (FrameUnits::Range, FrameBound::Preceding(_), true) | (FrameUnits::Range, FrameBound::Following(_), false) => match offset(start) {
            Some(o) => Reach::Value(o, ro.unwrap().clone()),
            None => Reach::All,
        },
        (FrameUnits::Rows, FrameBound::CurrentRow, _) => Reach::None,
        (_, FrameBound::CurrentRow, _) => Reach::Groups(0),
        // a frame bound on the other side of the row
        _ => Reach::None,
    }
}

/// Where a frame bound lies from its row: how far, and whether toward the
/// partition's start.
fn bound_pos(f: &WinSpec, start: bool) -> (Reach, bool) {
    let (b, ro) = (if start { f.frame.start } else { f.frame.end }, f.range.as_ref());
    let offset = |s: bool| ro.and_then(|r| if s { r.start.clone() } else { r.end.clone() });
    let by = |k: i64| match f.frame.units {
        FrameUnits::Rows => Reach::Rows(k),
        FrameUnits::Groups => Reach::Groups(k),
        FrameUnits::Range => match offset(start) {
            Some(o) => Reach::Value(o, ro.unwrap().clone()),
            None => Reach::All,
        },
    };
    match b {
        FrameBound::UnboundedPreceding => (Reach::All, true),
        FrameBound::Preceding(k) => (by(k), true),
        FrameBound::Following(k) => (by(k), false),
        FrameBound::UnboundedFollowing => (Reach::All, false),
        // peers: a start bound at the row's group's start, an end bound at its end
        FrameBound::CurrentRow if f.frame.units == FrameUnits::Rows => (Reach::None, start),
        FrameBound::CurrentRow => (Reach::Groups(0), start),
    }
}

/// The rows whose values a change can change, as reaches toward the
/// partition's start and end from the changed rows. `resized`: the
/// partition's size changes.
fn change_reach(f: &WinSpec, resized: bool) -> (Reach, Reach) {
    use WindowFunc as W;
    let all_if = |b: bool, r: Reach| if b { Reach::All } else { r };
    match f.func {
        W::RowNumber => (Reach::None, all_if(resized, Reach::None)),
        W::Rank => (Reach::None, all_if(resized, Reach::Groups(0))),
        W::DenseRank => (Reach::None, Reach::All),
        W::PercentRank => (all_if(resized, Reach::None), all_if(resized, Reach::Groups(0))),
        W::CumeDist => (all_if(resized, Reach::Groups(0)), all_if(resized, Reach::Groups(0))),
        W::Ntile => (all_if(resized, Reach::None), all_if(resized, Reach::None)),
        W::Lag => (Reach::None, if f.args.len() > 1 { Reach::All } else { Reach::Rows(1) }),
        W::Lead => (if f.args.len() > 1 { Reach::All } else { Reach::Rows(1) }, Reach::None),
        // rows whose frame holds the changed row: frames ending after it
        // reach back from it, frames starting before it forward
        _ => (bound_reach(f, false), bound_reach(f, true)),
    }
}

/// Where the rows of a reach from `key` (an entry with rows at positions
/// [at, at_end) of the current version) end: the first position of the
/// rows toward the start (`back`), or the position after the last toward
/// the end. Positions outside the changed rows are those of both versions.
#[allow(clippy::too_many_arguments)]
fn reach_to(env: &mut Env, part: &Part, p: &WindowPlan, n: i64, key: &[u8], at: i64, at_end: i64, reach: &Reach, back: bool) -> Result<i64> {
    Ok(match reach {
        Reach::None => {
            if back {
                at
            } else {
                at_end
            }
        }
        Reach::All => {
            if back {
                0
            } else {
                n
            }
        }
        Reach::Rows(k) => {
            if back {
                at.saturating_sub(*k).max(0)
            } else {
                at_end.saturating_add(*k).min(n)
            }
        }
        Reach::Groups(k) if *k > MAX_GROUP_WALK => {
            if back {
                0
            } else {
                n
            }
        }
        Reach::Groups(k) => {
            let (mut s, mut e) = part.peers(env, key)?;
            for _ in 0..*k {
                if back {
                    match part.nth(env, s - 1)? {
                        Some(prev) => s = part.peers(env, &prev)?.0,
                        None => break,
                    }
                } else {
                    match part.nth(env, e)? {
                        Some(next) => e = part.peers(env, &next)?.1,
                        None => break,
                    }
                }
            }
            if back { s.min(at) } else { e.max(at_end) }
        }
        Reach::Value(offset, ro) => {
            let Some(sk) = p.order.first() else {
                return reach_to(env, part, p, n, key, at, at_end, &Reach::All, back);
            };
            let (col, desc) = (sk.col, sk.spec.desc);
            let v = range_value(&part.layout.decode_row(key)?, col, ro);
            if v.is_null() {
                // a NULL value frames (and is framed by) its peers only
                return reach_to(env, part, p, n, key, at, at_end, &Reach::Groups(0), back);
            }
            let Some(target) = range_shift(&v, offset, ro, desc, back) else {
                return reach_to(env, part, p, n, key, at, at_end, &Reach::All, back);
            };
            // within reach: a non-null value not beyond the target
            let within = |env: &mut Env, pos: i64| -> Result<bool> {
                let x = range_value(&part.row_at(env, pos)?, col, ro);
                if x.is_null() {
                    return Ok(false);
                }
                let c = x.sql_cmp(&target);
                let c = if desc { c.reverse() } else { c };
                Ok(if back { c != Ordering::Less } else { c != Ordering::Greater })
            };
            // bisect the positions on that side (within reach toward the row)
            if back {
                let (mut a, mut b) = (0, at);
                while a < b {
                    let m = a + (b - a) / 2;
                    if within(env, m)? {
                        b = m;
                    } else {
                        a = m + 1;
                    }
                }
                a
            } else {
                let (mut a, mut b) = (at_end, n);
                while a < b {
                    let m = a + (b - a) / 2;
                    if within(env, m)? {
                        a = m + 1;
                    } else {
                        b = m;
                    }
                }
                a
            }
        }
    })
}

/// An entry of a partition changed by a step, with its weight before and
/// after it (from the step's changes) and its position in the current
/// version.
struct Changed {
    key: Vec<u8>,
    before: i64,
    after: i64,
    rank: i64,
}

/// Partitions of at most this many rows are recomputed whole.
const SMALL_PARTITION: i64 = 32;

fn data_error(e: SqlError) -> Result<SqlError> {
    if super::reduce::is_data_error(&e) { Ok(e) } else { Err(e) }
}

/// Apply a step's changes (entries `keys`) to one partition: recompute only
/// the rows whose values can change, before and after the step, over the
/// run of rows their values depend on. None when that is not possible (the
/// partition is or becomes in error, or its error state is unknown): the
/// whole partition is recomputed instead.
fn step_partition(env: &mut Env, p: &WindowPlan, part: &Part, deltas: &EntryDeltas) -> Result<Option<Delta>> {
    if !p.errors_known.load(std::sync::atomic::Ordering::Acquire) || p.errors.lock().contains_key(&part.lo) {
        return Ok(None);
    }
    let n = part.len(env)?;
    // (reading a small partition costs less than finding what to read)
    if n <= SMALL_PARTITION {
        return Ok(None);
    }
    let mut changed: Vec<Changed> = Vec::with_capacity(deltas.entries().len());
    for (key, d) in deltas.entries() {
        let after = env.get(part.tree, key)?.map(|h| h.total()).unwrap_or(0);
        changed.push(Changed { key: key.clone(), before: after - d, after, rank: 0 });
    }
    if changed.is_empty() {
        return Ok(Some(vec![]));
    }
    let resize: i64 = changed.iter().map(|c| c.after - c.before).sum();
    for c in changed.iter_mut() {
        c.rank = part.rank(env, &c.key)?;
    }
    let (first, last) = (&changed[0], &changed[changed.len() - 1]);
    let (at, at_end) = (first.rank, last.rank + last.after);
    // the rows whose values can change: positions [reg_lo, reg_hi) of the
    // current version (the rows outside the changed ones are the same in
    // both versions)
    let (mut reg_lo, mut reg_hi) = (at, at_end);
    for f in &p.funcs {
        let (back, fwd) = change_reach(f, resize != 0);
        reg_lo = reg_lo.min(reach_to(env, part, p, n, &first.key, at, at, &back, true)?);
        reg_hi = reg_hi.max(reach_to(env, part, p, n, &last.key, at_end, at_end, &fwd, false)?);
    }
    // the rows their values depend on: the frames of those rows (but for
    // the parts up to the partition's ends that summaries or row lookups
    // provide), whole peer groups, from the start for dense_rank
    let edge = |env: &mut Env, pos: i64, changed: &Vec<u8>, lo: bool| -> Result<Vec<u8>> {
        Ok(match part.nth(env, pos)? {
            Some(k) if (lo && k.as_ref() < changed.as_slice()) || (!lo && k.as_ref() > changed.as_slice()) => k.to_vec(),
            _ => changed.clone(),
        })
    };
    let reg_lo_key = edge(env, reg_lo, &first.key, true)?;
    let reg_hi_key = edge(env, reg_hi - 1, &last.key, false)?;
    let (mut run_lo, mut run_hi) = (reg_lo, reg_hi);
    for f in &p.funcs {
        // (ROWS frames of first/last/nth_value: rows are read by position)
        if !f.frame_based() || (f.frame.units == FrameUnits::Rows && !matches!(f.func, WindowFunc::Agg(_))) {
            continue;
        }
        for start in [true, false] {
            match bound_pos(f, start) {
                // the parts up to the partition's ends come from summaries
                // (aggregates) or are read by position (first/last/nth_value)
                (Reach::All, _) if !f.reads_frame_rows() => {}
                (r, true) => run_lo = run_lo.min(reach_to(env, part, p, n, &reg_lo_key, reg_lo, reg_lo, &r, true)?),
                (r, false) => run_hi = run_hi.max(reach_to(env, part, p, n, &reg_hi_key, reg_hi, reg_hi, &r, false)?),
            }
        }
    }
    if p.funcs.iter().any(|f| f.func == WindowFunc::DenseRank) {
        run_lo = 0;
    }
    let mut lo_key = edge(env, run_lo, &first.key, true)?;
    let mut hi_key = edge(env, run_hi - 1, &last.key, false)?;
    if p.funcs.iter().any(|f| f.uses_peers()) {
        let (s, _) = part.peers(env, &lo_key)?;
        if let Some(k) = part.nth(env, s)?
            && k.as_ref() < lo_key.as_slice()
        {
            lo_key = k.to_vec();
        }
        let (_, e) = part.peers(env, &hi_key)?;
        if let Some(k) = part.nth(env, e - 1)?
            && k.as_ref() > hi_key.as_slice()
        {
            hi_key = k.to_vec();
        }
    }
    // read the run: rows with their weights before and after the step
    let layout = part.layout;
    let mut entries: Vec<(Row, i64, i64, bool)> = Vec::new();
    env.for_each(part.tree, Bound::Included(&lo_key), Bound::Included(&hi_key), &mut |k, h| {
        visited(1);
        let after = h.total();
        let before = deltas.before(k, after);
        if before > 0 || after > 0 {
            let in_region = k >= reg_lo_key.as_slice() && k <= reg_hi_key.as_slice();
            entries.push((layout.decode_row(k)?, before, after, in_region));
        }
        Ok(true)
    })?;
    let base = part.rank(env, &lo_key)?;
    // the summaries of the rows on either side of the run (no changed rows
    // there: the same in both versions)
    let summarized = p.funcs.iter().any(|f| f.summarized());
    let before_run = if summarized && base > 0 { Some(env.range_summary(part.tree, Bound::Included(&part.lo), Bound::Excluded(&lo_key))?) } else { None };
    let after_run = if summarized { Some(env.range_summary(part.tree, Bound::Excluded(&hi_key), part.hi())?) } else { None };
    let slots = p.slots();
    let mut out = Delta::new();
    for (sign, weight) in [(-1i64, 1usize), (1, 2)] {
        // rows of this version, and those of the region
        let mut rows: Vec<&Row> = Vec::new();
        let (mut from, mut to) = (usize::MAX, 0);
        for (r, b, a, in_region) in &entries {
            let w = if weight == 1 { *b } else { *a };
            for _ in 0..w.max(0) {
                if *in_region {
                    from = from.min(rows.len());
                    to = rows.len() + 1;
                }
                rows.push(r);
            }
        }
        // (rows after the run: positions shifted by the change in the old version)
        let (len, shift) = if sign < 0 { (n - resize, resize) } else { (n, 0) };
        let mut outside = |pos: i64| -> Result<Row> { part.row_at(env, if pos < base { pos } else { pos + shift }) };
        let mut run = Run { rows, base, n: len, before: before_run.clone(), after: after_run.clone(), outside: &mut outside };
        if from == usize::MAX {
            continue;
        }
        match compute(&mut run, &p.order, &p.funcs, &slots, from, to) {
            Ok(rows) => out.extend(rows.into_iter().map(|(r, w)| (r, sign * w))),
            Err(e) => {
                data_error(e)?;
                return Ok(None);
            }
        }
    }
    Ok(Some(out))
}

/// A partition's rows, now or (with the step's changes) before the step.
fn partition_rows(env: &mut Env, part: &Part, before: Option<&EntryDeltas>) -> Result<Vec<(Row, i64)>> {
    let mut out = Vec::new();
    env.for_each(part.tree, Bound::Included(&part.lo), part.hi(), &mut |k, h| {
        visited(1);
        let w = match before {
            Some(d) => d.before(k, h.total()),
            None => h.total(),
        };
        if w > 0 {
            out.push((part.layout.decode_row(k)?, w));
        }
        Ok(true)
    })?;
    Ok(out)
}

/// Recompute a whole partition before and after the step, emitting the
/// difference (an error record while it is in error).
fn step_whole(env: &mut Env, p: &WindowPlan, part: &Part, deltas: &EntryDeltas, out: &mut Output) -> Result<()> {
    let old = partition_rows(env, part, Some(deltas))?;
    let new = partition_rows(env, part, None)?;
    let mut errors = p.errors.lock();
    match compute_partition(&old, &p.order, &p.funcs) {
        Ok(rows) => out.data.extend(rows.into_iter().map(|(r, w)| (r, -w))),
        Err(e) => out.errs.push((err_record(&data_error(e)?), -1)),
    }
    match compute_partition(&new, &p.order, &p.funcs) {
        Ok(rows) => {
            out.data.extend(rows);
            errors.remove(&part.lo);
        }
        Err(e) => {
            let rec = err_record(&data_error(e)?);
            out.errs.push((rec.clone(), 1));
            errors.insert(part.lo.clone(), rec);
        }
    }
    Ok(())
}

pub fn step(g: &Graph, env: &mut Env, p: &WindowPlan, delta: &Delta, _gsn: Gsn, out: &mut Output) -> Result<()> {
    let a = g.arr(p.input_arr);
    let layout = a.layout.clone();
    // the changes by partition
    let mut parts: BTreeMap<Vec<u8>, Vec<(&Row, i64)>> = BTreeMap::new();
    for (row, w) in delta {
        let kv: Vec<Datum> = p.partition.iter().map(|c| row[*c].clone()).collect();
        parts.entry(layout.encode_prefix(&kv)).or_default().push((row, *w));
    }
    for (prefix, rows) in parts {
        let deltas = EntryDeltas::of(&layout, rows.into_iter())?;
        if deltas.entries().is_empty() {
            continue;
        }
        let hi = crate::types::encoding::prefix_successor(&prefix);
        let part = Part { tree: a.tree, layout: &layout, lo: prefix, hi };
        match step_partition(env, p, &part, &deltas)? {
            Some(d) => out.data.extend(d),
            None => step_whole(env, p, &part, &deltas, out)?,
        }
    }
    Ok(())
}

/// Contents of the operator (hydration: rebuilds its error state).
pub fn snapshot(g: &Graph, env: &mut Env, p: &WindowPlan, out: &mut Output) -> Result<()> {
    let a = g.arr(p.input_arr);
    let layout = a.layout.clone();
    let np = p.partition.len();
    let mut errors = p.errors.lock();
    errors.clear();
    let mut c = env.cursor(a.tree, Bound::Unbounded, Bound::Unbounded);
    let mut cur_prefix: Option<Vec<u8>> = None;
    let mut rows: Vec<(Row, i64)> = Vec::new();
    let mut flush = |prefix: &[u8], rows: &mut Vec<(Row, i64)>, out: &mut Output| -> Result<()> {
        match compute_partition(rows, &p.order, &p.funcs) {
            Ok(rs) => out.data.extend(rs),
            Err(e) => {
                let rec = err_record(&data_error(e)?);
                errors.insert(prefix.to_vec(), rec.clone());
                out.errs.push((rec, 1));
            }
        }
        rows.clear();
        Ok(())
    };
    while let Some((k, h)) = env.cursor_next(&mut c)? {
        let w = h.total();
        if w <= 0 {
            continue;
        }
        let prefix = k[..layout.prefix_len(&k, np)?].to_vec();
        if cur_prefix.as_ref() != Some(&prefix) {
            if let Some(cp) = cur_prefix.take()
                && !rows.is_empty()
            {
                flush(&cp, &mut rows, out)?;
            }
            cur_prefix = Some(prefix);
        }
        rows.push((layout.decode_row(&k)?, w));
    }
    if let Some(cp) = cur_prefix
        && !rows.is_empty()
    {
        flush(&cp, &mut rows, out)?;
    }
    drop(errors);
    p.errors_known.store(true, std::sync::atomic::Ordering::Release);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::plan::{AggFunc, Frame};
    use crate::types::encoding::SortSpec;

    /// Aggregates over frames that slide (added and subtracted rows, MIN and
    /// MAX deques) equal the same aggregates summed from scratch for every
    /// row, for frames of every shape over partitions with ties, NULLs and
    /// multiplicities.
    #[test]
    fn sliding_frames_equal_frames_summed_from_scratch() {
        let mut rng = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move |n: u64| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng % n
        };
        use FrameBound as B;
        let bounds = [B::UnboundedPreceding, B::Preceding(0), B::Preceding(1), B::Preceding(3), B::CurrentRow, B::Following(0), B::Following(2), B::Following(5), B::UnboundedFollowing];
        let funcs = [AggFunc::Sum, AggFunc::Count, AggFunc::CountStar, AggFunc::Min, AggFunc::Max, AggFunc::Avg, AggFunc::StringAgg];
        for case in 0..400 {
            // rows (k, v, s): ordered by k, with ties; v nullable; s text
            let n = next(12) as usize;
            let mut rows: Vec<(Row, i64)> = (0..n)
                .map(|i| {
                    // (numerics in several representations: MIN and MAX keep the first)
                    let v = if next(5) == 0 {
                        Datum::Null
                    } else {
                        let scale = ["", ".0", ".00"][next(3) as usize];
                        Datum::numeric(pgnumeric::Numeric::parse(&format!("{}{scale}", next(7) as i64 - 3)).unwrap())
                    };
                    (vec![Datum::Int8(next(4) as i64), v, Datum::text_owned(format!("r{i}"))], 1 + next(3) as i64 / 2)
                })
                .collect();
            rows.sort_by(|a, b| a.0[0].sql_cmp(&b.0[0]).then(crate::types::datum::exact_row_cmp(&a.0, &b.0)));
            let (start, end) = (bounds[next(8) as usize], bounds[1 + next(8) as usize]);
            let units = [FrameUnits::Rows, FrameUnits::Groups, FrameUnits::Range][next(3) as usize];
            let units = if units == FrameUnits::Range && !matches!((start, end), (B::UnboundedPreceding | B::CurrentRow, B::CurrentRow | B::UnboundedFollowing)) {
                FrameUnits::Rows
            } else {
                units
            };
            let func = funcs[case % funcs.len()];
            let (args, arg_types) = match func {
                AggFunc::StringAgg => (vec![2, 2], vec![SqlType::Text, SqlType::Text]),
                AggFunc::CountStar => (vec![], vec![]),
                _ => (vec![1], vec![SqlType::Numeric]),
            };
            let spec = WinSpec {
                func: WindowFunc::Agg(func),
                args,
                frame: Frame { units, start, end },
                arg_types,
                ret: ColumnType::new(if matches!(func, AggFunc::StringAgg) { SqlType::Text } else { SqlType::Numeric }),
                range: None,
            };
            let order = [SortKey { col: 0, spec: SortSpec::ASC }];
            let got = compute_partition(&rows, &order, std::slice::from_ref(&spec)).unwrap();
            // from scratch, row by row
            let rs: Vec<&Row> = rows.iter().flat_map(|(r, w)| std::iter::repeat_n(r, *w as usize)).collect();
            let mut outside = |_: i64| -> Result<Row> { unreachable!() };
            let run = Run { rows: rs.clone(), base: 0, n: rs.len() as i64, before: None, after: None, outside: &mut outside };
            let p = Peers::of(&rs, &order);
            let agg = spec.agg().unwrap();
            let kinds = agg.summaries();
            let spec_kinds: Vec<AggKind> = kinds.iter().map(|(k, _)| *k).collect();
            let mut want: Vec<(Row, i64)> = Vec::new();
            for i in 0..rs.len() {
                let (s, e) = frame_bounds(&run, &spec, &p, i);
                let v = if func.is_ordered() {
                    agg.finalize_ordered(&mut rs[s as usize..e as usize].iter().map(|r| (*r, 1))).unwrap()
                } else {
                    let mut st = Summary::empty(&spec_kinds);
                    for r in &rs[s as usize..e as usize] {
                        st.count += 1;
                        for (k, (kind, ex)) in kinds.iter().enumerate() {
                            kind.add(&mut st.aggs[k], &ex.eval(r).unwrap(), 1).unwrap();
                        }
                    }
                    agg.finalize(&st, &(0..kinds.len()).collect::<Vec<_>>()).unwrap()
                };
                let mut r = rs[i].clone();
                r.push(v);
                match want.last_mut() {
                    Some((last, w)) if crate::types::datum::exact_row_cmp(last, &r) == Ordering::Equal => *w += 1,
                    _ => want.push((r, 1)),
                }
            }
            let exact = |a: &Vec<(Row, i64)>, b: &Vec<(Row, i64)>| {
                a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.1 == y.1 && crate::types::datum::exact_row_cmp(&x.0, &y.0) == Ordering::Equal)
            };
            assert!(exact(&got, &want), "case {case}: {func:?} {units:?} {start:?} {end:?} over {rows:?}: {got:?} != {want:?}");
        }
    }
}
