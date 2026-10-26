//! Window functions by per-partition recomputation.
//!
//! The input is arranged by (partition, order, rest); a change to a partition
//! recomputes its window values before and after the change and emits the
//! difference. Cost is bounded by the partition size, which is intrinsic for
//! functions such as row_number where one insert shifts every later row.

use super::agg::AggSpec;
use super::*;
use crate::sql::plan::{AggFunc, FrameBound, FrameUnits, RangeOffsets, SortKey, WindowFunc};
use crate::storage::Summary;
use crate::types::{Datum, SqlType};
use std::cmp::Ordering;
use std::ops::Bound;

#[derive(Clone, Debug)]
pub struct WinSpec {
    pub func: WindowFunc,
    pub args: Vec<usize>,
    pub frame: crate::sql::plan::Frame,
    pub arg_types: Vec<SqlType>,
    pub ret: ColumnType,
    pub range: Option<RangeOffsets>,
}

#[derive(Clone, Debug)]
pub struct WindowPlan {
    pub partition: Vec<usize>,
    pub order: Vec<SortKey>,
    pub funcs: Vec<WinSpec>,
    pub input_arr: ArrId,
}

impl WindowPlan {
    pub fn fingerprint(&self, cx: &FpCtx, fp: &mut Vec<u8>) {
        fp.extend_from_slice(format!("{:?}{:?}", self.partition, self.order).as_bytes());
        fp.extend_from_slice(cx.arr(self.input_arr));
        for f in &self.funcs {
            fp.extend_from_slice(format!("{:?}{:?}{:?}{:?}", f.func, f.args, f.frame, f.range).as_bytes());
        }
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

/// Compute window function values for one partition. `rows` are in window
/// order with multiplicities; returns output rows (input ++ values).
pub fn compute_partition(rows: &[(Row, i64)], order: &[SortKey], funcs: &[WinSpec]) -> Result<Vec<Row>> {
    let mut rs: Vec<&Row> = Vec::new();
    for (r, w) in rows {
        for _ in 0..(*w).max(0) {
            rs.push(r);
        }
    }
    let n = rs.len();
    if n == 0 {
        return Ok(vec![]);
    }
    // peer groups
    let mut group = vec![0usize; n];
    let mut gstart = vec![0usize];
    for i in 1..n {
        if order_cmp(rs[i - 1], rs[i], order) == Ordering::Equal {
            group[i] = group[i - 1];
        } else {
            group[i] = group[i - 1] + 1;
            gstart.push(i);
        }
    }
    let ngroups = gstart.len();
    let gend: Vec<usize> = (0..ngroups).map(|g| if g + 1 < ngroups { gstart[g + 1] - 1 } else { n - 1 }).collect();
    let mut out: Vec<Row> = rs.iter().map(|r| (*r).clone()).collect();
    let p = Peers { group: &group, gstart: &gstart, gend: &gend, order, region: Default::default() };
    for f in funcs {
        let vals = compute_func(&rs, f, &p)?;
        for (o, v) in out.iter_mut().zip(vals) {
            o.push(v);
        }
    }
    Ok(out)
}

/// Peer groups of a sorted partition.
struct Peers<'a> {
    group: &'a [usize],
    gstart: &'a [usize],
    gend: &'a [usize],
    order: &'a [SortKey],
    /// RANGE offsets: [lo, hi) of the non-null ORDER BY values and those
    /// values in the comparison domain (computed once per partition).
    region: std::cell::OnceCell<(usize, usize, Vec<Datum>)>,
}

/// ORDER BY value of a row in the comparison domain of RANGE offsets.
fn range_value(r: &Row, col: usize, ro: &RangeOffsets) -> Datum {
    match &ro.cmp_type {
        Some(t) if !r[col].is_null() => crate::expr::cast::cast_datum(&r[col], t, -1, false).unwrap_or(Datum::Null),
        _ => r[col].clone(),
    }
}

/// A RANGE offset bound of row `i`: the first row not before (start) or the
/// last row not after (end) the current value minus/plus the offset.
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
    let f = match crate::expr::func::func_def(if preceding != desc { ro.minus } else { ro.plus }).imp {
        crate::expr::func::Imp::F2(f) => f,
        _ => return if start { lo as i64 } else { hi as i64 - 1 },
    };
    let target = match f(&cur, offset) {
        Ok(t) => t,
        // out of range: as in PostgreSQL's in_range functions, the bound lies
        // beyond every value in the offset's direction (before all of them
        // for PRECEDING, after all of them for FOLLOWING)
        Err(_) => {
            return match (start, preceding) {
                (true, true) => lo as i64,
                (true, false) => hi as i64,
                (false, true) => lo as i64 - 1,
                (false, false) => hi as i64 - 1,
            };
        }
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

fn frame_bounds(rs: &[&Row], f: &WinSpec, p: &Peers, i: usize) -> (usize, usize) {
    let (group, gstart, gend) = (p.group, p.gstart, p.gend);
    let n = rs.len();
    let g = group[i];
    let ng = gstart.len() as i64;
    let ro = f.range.as_ref().filter(|_| f.frame.units == FrameUnits::Range);
    let f = &f.frame;
    let start = match (f.units, f.start) {
        (FrameUnits::Range, FrameBound::Preceding(_) | FrameBound::Following(_)) if ro.is_some() => {
            let ro = ro.unwrap();
            range_bound(rs, ro, p, i, ro.start.as_ref().unwrap(), matches!(f.start, FrameBound::Preceding(_)), true).max(0) as usize
        }
        (_, FrameBound::UnboundedPreceding) => 0,
        (FrameUnits::Rows, FrameBound::Preceding(k)) => i.saturating_sub(k as usize),
        (FrameUnits::Rows, FrameBound::CurrentRow) => i,
        // a start past the partition is an empty frame at its end
        (FrameUnits::Rows, FrameBound::Following(k)) => (i as i64).saturating_add(k).min(n as i64) as usize,
        (FrameUnits::Groups, FrameBound::Preceding(k)) => gstart[(g as i64).saturating_sub(k).max(0) as usize],
        (FrameUnits::Groups, FrameBound::Following(k)) => {
            let gg = (g as i64).saturating_add(k);
            if gg >= ng { n } else { gstart[gg as usize] }
        }
        (_, FrameBound::CurrentRow) => gstart[g],
        (_, FrameBound::UnboundedFollowing) => n,
        (FrameUnits::Range, _) => gstart[g],
    };
    let end = match (f.units, f.end) {
        (FrameUnits::Range, FrameBound::Preceding(_) | FrameBound::Following(_)) if ro.is_some() => {
            let ro = ro.unwrap();
            range_bound(rs, ro, p, i, ro.end.as_ref().unwrap(), matches!(f.end, FrameBound::Preceding(_)), false)
        }
        (_, FrameBound::UnboundedFollowing) => n as i64 - 1,
        (FrameUnits::Rows, FrameBound::Following(k)) => (i as i64).saturating_add(k).min(n as i64 - 1),
        (FrameUnits::Rows, FrameBound::CurrentRow) => i as i64,
        (FrameUnits::Rows, FrameBound::Preceding(k)) => (i as i64).saturating_sub(k),
        (FrameUnits::Groups, FrameBound::Following(k)) => gend[((g as i64).saturating_add(k).min(ng - 1)) as usize] as i64,
        (FrameUnits::Groups, FrameBound::Preceding(k)) => {
            let gg = (g as i64).saturating_sub(k);
            if gg < 0 { -1 } else { gend[gg as usize] as i64 }
        }
        (_, FrameBound::CurrentRow) => gend[g] as i64,
        (_, FrameBound::UnboundedPreceding) => -1,
        (FrameUnits::Range, _) => gend[g] as i64,
    };
    if end < start as i64 { (start, start) } else { (start, end as usize + 1) }
}

fn compute_func(rs: &[&Row], f: &WinSpec, p: &Peers) -> Result<Vec<Datum>> {
    let (group, gstart, gend) = (p.group, p.gstart, p.gend);
    let n = rs.len();
    let mut out = Vec::with_capacity(n);
    match f.func {
        WindowFunc::RowNumber => out.extend((0..n).map(|i| Datum::Int8(i as i64 + 1))),
        WindowFunc::Rank => out.extend((0..n).map(|i| Datum::Int8(gstart[group[i]] as i64 + 1))),
        WindowFunc::DenseRank => out.extend((0..n).map(|i| Datum::Int8(group[i] as i64 + 1))),
        WindowFunc::PercentRank => out.extend((0..n).map(|i| {
            Datum::Float8(if n > 1 { gstart[group[i]] as f64 / (n - 1) as f64 } else { 0.0 })
        })),
        WindowFunc::CumeDist => out.extend((0..n).map(|i| Datum::Float8((gend[group[i]] + 1) as f64 / n as f64))),
        WindowFunc::Ntile => {
            for (i, r) in rs.iter().enumerate() {
                let k = r[f.args[0]].as_i64();
                let Some(k) = k else {
                    out.push(Datum::Null);
                    continue;
                };
                if k <= 0 {
                    return Err(SqlError::new("22014", "argument of ntile must be greater than zero"));
                }
                let k = k as usize;
                let per = n / k;
                let extra = n % k;
                // first `extra` buckets have per+1 rows
                let bucket = if i < extra * (per + 1) { i / (per + 1) } else { extra + (i - extra * (per + 1)) / per.max(1) };
                out.push(Datum::Int4(bucket as i32 + 1));
            }
        }
        WindowFunc::Lag | WindowFunc::Lead => {
            for (i, r) in rs.iter().enumerate() {
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
                let j = if f.func == WindowFunc::Lag { i as i64 - off } else { i as i64 + off };
                if j >= 0 && (j as usize) < n {
                    out.push(rs[j as usize][f.args[0]].clone());
                } else {
                    out.push(f.args.get(2).map(|c| r[*c].clone()).unwrap_or(Datum::Null));
                }
            }
        }
        WindowFunc::FirstValue | WindowFunc::LastValue | WindowFunc::NthValue => {
            for i in 0..n {
                let (s, e) = frame_bounds(rs, f, p, i);
                let v = match f.func {
                    WindowFunc::FirstValue => (s < e).then(|| rs[s][f.args[0]].clone()),
                    WindowFunc::LastValue => (s < e).then(|| rs[e - 1][f.args[0]].clone()),
                    _ => {
                        // a NULL position yields NULL (as in PostgreSQL)
                        let Some(k) = rs[i][f.args[1]].as_i64() else {
                            out.push(Datum::Null);
                            continue;
                        };
                        if k <= 0 {
                            return Err(SqlError::new("22016", "argument of nth_value must be greater than zero"));
                        }
                        let idx = s + k as usize - 1;
                        (idx < e).then(|| rs[idx][f.args[0]].clone())
                    }
                };
                out.push(v.unwrap_or(Datum::Null));
            }
        }
        WindowFunc::Agg(func) => {
            let spec = AggSpec {
                func,
                arg: f.args.first().copied(),
                arg2: f.args.get(1).copied(),
                filter: None,
                arg_type: f.arg_types.first().cloned().unwrap_or(SqlType::Int8),
                ret: f.ret.clone(),
                fraction: None,
            };
            let kinds = spec.summaries();
            let spec_kinds: Vec<crate::storage::AggKind> = kinds.iter().map(|(k, _)| *k).collect();
            let slots: Vec<usize> = (0..kinds.len()).collect();
            let running = f.frame.start == FrameBound::UnboundedPreceding && !func.is_ordered();
            let mut acc = Summary::empty(&spec_kinds);
            let mut upto = 0usize;
            for i in 0..n {
                let (s, e) = frame_bounds(rs, f, p, i);
                if func.is_ordered() {
                    let frame: Vec<(Row, i64)> = rs[s..e].iter().map(|r| ((*r).clone(), 1)).collect();
                    out.push(spec.finalize_ordered(&mut frame.into_iter())?);
                    continue;
                }
                if running && s == 0 && e >= upto {
                    for r in &rs[upto..e] {
                        acc.count += 1;
                        for (k, (kind, ex)) in kinds.iter().enumerate() {
                            kind.add(&mut acc.aggs[k], &ex.eval(r)?, 1)?;
                        }
                    }
                    upto = e;
                    out.push(spec.finalize(&acc, &slots)?);
                } else {
                    let mut fr = Summary::empty(&spec_kinds);
                    for r in &rs[s..e] {
                        fr.count += 1;
                        for (k, (kind, ex)) in kinds.iter().enumerate() {
                            kind.add(&mut fr.aggs[k], &ex.eval(r)?, 1)?;
                        }
                    }
                    if matches!(func, AggFunc::CountStar) {
                        out.push(Datum::Int8(fr.count));
                    } else {
                        out.push(spec.finalize(&fr, &slots)?);
                    }
                }
            }
        }
    }
    Ok(out)
}

fn partition_rows(env: &mut Env, tree: TreeId, layout: &Layout, prefix: &[u8], at: ReadAt) -> Result<Vec<(Row, i64)>> {
    let hi = crate::types::encoding::prefix_successor(prefix);
    let mut c = match &hi {
        Some(h) => env.cursor(tree, Bound::Included(prefix), Bound::Excluded(h)),
        None => env.cursor(tree, Bound::Included(prefix), Bound::Unbounded),
    };
    let mut out = Vec::new();
    while let Some((k, h)) = env.cursor_next(&mut c)? {
        let w = h.weight(at);
        if w > 0 {
            out.push((layout.decode_row(&k)?, w));
        }
    }
    Ok(out)
}

pub fn step(g: &Graph, env: &mut Env, p: &WindowPlan, delta: &Delta, gsn: Gsn, out: &mut Output) -> Result<()> {
    let a = g.arr(p.input_arr);
    let layout = a.layout.clone();
    let mut parts: Vec<Vec<u8>> = Vec::new();
    for (row, _) in delta {
        let kv: Vec<Datum> = p.partition.iter().map(|c| row[*c].clone()).collect();
        parts.push(layout.encode_prefix(&kv));
    }
    parts.sort();
    parts.dedup();
    for prefix in parts {
        let old = partition_rows(env, a.tree, &layout, &prefix, ReadAt::Before(gsn))?;
        let new = partition_rows(env, a.tree, &layout, &prefix, ReadAt::Latest)?;
        match (compute_partition(&old, &p.order, &p.funcs), compute_partition(&new, &p.order, &p.funcs)) {
            (Ok(o), Ok(nw)) => {
                out.data.extend(o.into_iter().map(|r| (r, -1)));
                out.data.extend(nw.into_iter().map(|r| (r, 1)));
            }
            (o, nw) => {
                if let Err(e) = &o {
                    out.errs.push((err_record(e), -1));
                } else if let Ok(o) = o {
                    out.data.extend(o.into_iter().map(|r| (r, -1)));
                }
                if let Err(e) = &nw {
                    out.errs.push((err_record(e), 1));
                } else if let Ok(nw) = nw {
                    out.data.extend(nw.into_iter().map(|r| (r, 1)));
                }
            }
        }
    }
    Ok(())
}

pub fn snapshot(g: &Graph, env: &mut Env, p: &WindowPlan, out: &mut super::ops::SnapOut) -> Result<()> {
    let a = g.arr(p.input_arr);
    let layout = a.layout.clone();
    let np = p.partition.len();
    let mut c = env.cursor(a.tree, Bound::Unbounded, Bound::Unbounded);
    let mut cur_prefix: Option<Vec<u8>> = None;
    let mut rows: Vec<(Row, i64)> = Vec::new();
    let flush = |env: &mut Env, rows: &mut Vec<(Row, i64)>, out: &mut super::ops::SnapOut| -> Result<()> {
        match compute_partition(rows, &p.order, &p.funcs) {
            Ok(rs) => {
                for r in rs {
                    out.push(env, r, 1)?;
                }
            }
            Err(e) => out.err(err_record(&e), 1),
        }
        rows.clear();
        Ok(())
    };
    while let Some((k, h)) = env.cursor_next(&mut c)? {
        let w = h.total();
        if w <= 0 {
            continue;
        }
        let row = layout.decode_row(&k)?;
        let kv: Vec<Datum> = p.partition.iter().map(|c| row[*c].clone()).collect();
        let prefix = layout.encode_prefix(&kv[..np]);
        if cur_prefix.as_ref() != Some(&prefix) {
            if !rows.is_empty() {
                flush(env, &mut rows, out)?;
            }
            cur_prefix = Some(prefix);
        }
        rows.push((row, w));
    }
    if !rows.is_empty() {
        flush(env, &mut rows, out)?;
    }
    Ok(())
}
