//! Temporal filters: WHERE predicates over now().
//!
//! The wall clock is an input like any other (logged ticks, see the
//! sequencer), so a predicate such as `ts > now() - interval '1 hour'` is
//! maintained rather than recomputed. Each row is valid over an interval
//! [lo, hi) of logical time, computed from the row by bisecting the predicate
//! itself (its clock side is a non-decreasing function of now(), so the
//! predicate changes value at most once; bisection keeps month arithmetic
//! and time zones exact). The operator emits a row when it becomes valid and
//! retracts it when it expires; future events wait in an arrangement ordered
//! by time, and each step releases the events that are due.

use super::delta::{NodeDelta, StepOut};
use super::*;
use crate::error::codes;
use crate::expr::func::func_def;
use crate::expr::NowKind;
use crate::types::{Datum, SqlType};
use std::cmp::Ordering;
use std::ops::Bound;

/// Logical time range searched for validity bounds (2000-01-01 epoch
/// microseconds): 1900-01-01 .. 2200-01-01. Rows are valid "forever" on a
/// side whose bound lies outside it.
const T_LO: i64 = -3_155_673_600_000_000;
const T_HI: i64 = 6_311_433_600_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmpOp {
    Lt,
    Le,
    Gt,
    Ge,
}

impl CmpOp {
    fn holds(self, o: Ordering) -> bool {
        match self {
            CmpOp::Lt => o == Ordering::Less,
            CmpOp::Le => o != Ordering::Greater,
            CmpOp::Gt => o == Ordering::Greater,
            CmpOp::Ge => o != Ordering::Less,
        }
    }
}

/// `clock OP other`, where `clock` depends only on the clock reading.
#[derive(Clone, Debug)]
pub struct TemporalPred {
    /// Over a one-column row holding the clock reading (timestamptz).
    pub clock: Expr,
    pub op: CmpOp,
    /// Over the input row.
    pub other: Expr,
}

impl TemporalPred {
    /// Holds from some time on (rather than until some time).
    fn lower(&self) -> bool {
        matches!(self.op, CmpOp::Gt | CmpOp::Ge)
    }
}

#[derive(Clone, Debug)]
pub struct TemporalPlan {
    pub preds: Vec<TemporalPred>,
    /// Pending events: (time, kind) ++ row; kind 0: becomes valid, 1: expires.
    pub events: ArrId,
}

impl TemporalPlan {
    pub fn fingerprint(&self, fp: &mut Vec<u8>) {
        for p in &self.preds {
            p.clock.fingerprint(fp);
            fp.push(p.op as u8);
            p.other.fingerprint(fp);
        }
    }
}

fn unsupported() -> SqlError {
    SqlError::new(codes::FEATURE_NOT_SUPPORTED, "now() is only supported in temporal filters of maintained views").with_hint(
        "Use a top-level WHERE condition comparing a column expression with now() (optionally plus or minus a constant interval).",
    )
}

/// A constant: no columns, no clock, immutable functions only.
fn is_const(e: &Expr) -> bool {
    let mut f = Vec::new();
    e.nondeterministic_functions(&mut f);
    e.column_set().is_empty() && f.is_empty()
}

/// Is `e` a non-decreasing function of the clock alone?
fn nondecreasing(e: &Expr) -> bool {
    if e.now_kind().is_some() {
        return true;
    }
    match e {
        Expr::Cast(x, ty, _, _) => matches!(ty, SqlType::Timestamp | SqlType::TimestampTz | SqlType::Date) && nondecreasing(x),
        Expr::Call(c) => match (func_def(c.func).name, c.args.as_slice()) {
            ("+", [a, b]) => (nondecreasing(a) && is_const(b)) || (is_const(a) && nondecreasing(b)),
            ("-", [a, b]) => nondecreasing(a) && is_const(b),
            ("date_trunc", [u, x]) => is_const(u) && nondecreasing(x),
            _ => false,
        },
        _ => false,
    }
}

/// The clock reading as an expression over the one-column clock row.
fn clock_value(k: NowKind) -> Expr {
    let t = Expr::Column(0);
    match k {
        NowKind::TimestampTz => t,
        NowKind::Timestamp => Expr::Cast(Box::new(t), SqlType::Timestamp, -1, false),
        NowKind::Date => Expr::Cast(Box::new(t), SqlType::Date, -1, false),
    }
}

/// Recognize a temporal conjunct. None: the predicate does not use the
/// clock. Error: it does, in a form that is not a temporal filter.
pub fn analyze(pred: &Expr) -> Result<Option<Vec<TemporalPred>>> {
    if !pred.contains_now() {
        return Ok(None);
    }
    let Expr::Call(c) = pred else { return Err(unsupported()) };
    let name = func_def(c.func).name;
    let [a, b] = c.args.as_slice() else { return Err(unsupported()) };
    if !matches!(name, "<" | "<=" | ">" | ">=" | "=") {
        return Err(unsupported());
    }
    // normalize to `clock OP other`
    let (clock, other, flipped) = match (a.contains_now(), b.contains_now()) {
        (true, false) => (a, b, false),
        (false, true) => (b, a, true),
        _ => return Err(unsupported()),
    };
    if !nondecreasing(clock) {
        return Err(unsupported());
    }
    let clock = clock.replace_now(&clock_value);
    let op = |n: &str| match (n, flipped) {
        ("<", false) | (">", true) => CmpOp::Lt,
        ("<=", false) | (">=", true) => CmpOp::Le,
        (">", false) | ("<", true) => CmpOp::Gt,
        _ => CmpOp::Ge,
    };
    let mk = |op| TemporalPred { clock: clock.clone(), op, other: other.clone() };
    Ok(Some(match name {
        // equality holds on an interval: from clock >= other until clock > other
        "=" => vec![mk(CmpOp::Ge), mk(CmpOp::Le)],
        n => vec![mk(op(n))],
    }))
}

/// Split filter conjuncts into regular and temporal ones.
pub fn split(preds: &[Expr]) -> Result<(Vec<Expr>, Vec<TemporalPred>)> {
    let mut regular = Vec::new();
    let mut temporal = Vec::new();
    for p in preds {
        for c in p.clone().conjuncts() {
            match analyze(&c)? {
                Some(t) => temporal.extend(t),
                None => regular.push(c),
            }
        }
    }
    Ok((regular, temporal))
}

/// Smallest t in (lo, hi] with p(t), given !p(lo) and p(hi).
fn first_true(lo: i64, hi: i64, p: &dyn Fn(i64) -> bool) -> i64 {
    let (mut a, mut b) = (lo, hi);
    while b - a > 1 {
        let m = a + (b - a) / 2;
        if p(m) {
            b = m;
        } else {
            a = m;
        }
    }
    b
}

/// The logical time interval [lo, hi) during which `row` passes every
/// predicate (None: never). Errors come from evaluating the row side.
pub fn validity(preds: &[TemporalPred], row: &[Datum]) -> Result<Option<(i64, i64)>> {
    let (mut lo, mut hi) = (i64::MIN, i64::MAX);
    for tp in preds {
        let x = tp.other.eval(row)?;
        if x.is_null() {
            return Ok(None);
        }
        let holds = |t: i64| match tp.clock.eval(&[Datum::TimestampTz(t)]) {
            Ok(v) if !v.is_null() => tp.op.holds(v.sql_cmp(&x)),
            _ => false,
        };
        if tp.lower() {
            if !holds(T_LO) {
                if !holds(T_HI) {
                    return Ok(None);
                }
                lo = lo.max(first_true(T_LO, T_HI, &holds));
            }
        } else if !holds(T_LO) {
            return Ok(None);
        } else if !holds(T_HI) {
            hi = hi.min(first_true(T_LO, T_HI, &|t| !holds(t)));
        }
    }
    Ok(if lo < hi { Some((lo, hi)) } else { None })
}

fn event_row(time: i64, kind: i16, row: &[Datum]) -> Row {
    let mut r = Vec::with_capacity(row.len() + 2);
    r.push(Datum::Int8(time));
    r.push(Datum::Int2(kind));
    r.extend_from_slice(row);
    r
}

/// Schedule the future events of `row` (weight `w`) given that it is valid
/// over [lo, hi); returns whether it is valid now.
fn schedule(layout: &Layout, now: i64, row: &[Datum], w: i64, (lo, hi): (i64, i64), ups: &mut Vec<(Vec<u8>, i64)>) -> Result<bool> {
    if now >= hi {
        return Ok(false);
    }
    if now < lo {
        ups.push((layout.encode_row(&event_row(lo, 0, row))?, w));
    }
    if hi != i64::MAX {
        ups.push((layout.encode_row(&event_row(hi, 1, row))?, w));
    }
    Ok(now >= lo)
}

pub fn step(g: &Graph, env: &mut Env, p: &TemporalPlan, delta: &NodeDelta, gsn: Gsn, out: &mut StepOut) -> Result<()> {
    let now = g.now;
    let ev = g.arr(p.events);
    let layout = ev.layout.clone();
    let tree = ev.tree;
    // event changes, applied as they accumulate: the due events are read
    // first, and new ones lie after them
    let mut ups: Vec<(Vec<u8>, i64)> = Vec::new();
    let flush = |env: &mut Env, ups: &mut Vec<(Vec<u8>, i64)>, all: bool| {
        if ups.len() >= super::ops::BATCH_ROWS * 16 || (all && !ups.is_empty()) {
            env.apply(tree, ups, gsn)?;
        }
        Ok::<(), SqlError>(())
    };
    // events that are due: everything at or before now
    let bound = crate::types::encoding::prefix_successor(&layout.encode_prefix(&[Datum::Int8(now)]));
    let hi = match &bound {
        Some(b) => Bound::Excluded(b.as_slice()),
        None => Bound::Unbounded,
    };
    let mut c = env.cursor(tree, Bound::Unbounded, hi);
    while let Some((k, h)) = env.cursor_next(&mut c)? {
        let w = h.total();
        if w == 0 {
            continue;
        }
        let r = layout.decode_row(&k)?;
        let sign = if matches!(r[1], Datum::Int2(0)) { 1 } else { -1 };
        out.push(env, r[2..].to_vec(), sign * w)?;
        ups.push((k.into_vec(), -w));
        flush(env, &mut ups, false)?;
    }
    // changed rows
    delta.for_each_batch(env, &mut |env, batch| {
        for (row, w) in batch {
            match validity(&p.preds, row) {
                Ok(Some(iv)) => {
                    if schedule(&layout, now, row, *w, iv, &mut ups)? {
                        out.push(env, row.clone(), *w)?;
                    }
                }
                Ok(None) => {}
                Err(e) if super::reduce::is_data_error(&e) => out.err(err_record(&e), *w),
                Err(e) => return Err(e),
            }
        }
        flush(env, &mut ups, false)
    })?;
    flush(env, &mut ups, true)
}

/// Rows of the input valid now (a batch of the input's snapshot).
pub fn snapshot(g: &Graph, env: &mut Env, p: &TemporalPlan, input: Delta, out: &mut super::ops::SnapOut) -> Result<()> {
    for (row, w) in input {
        match validity(&p.preds, &row) {
            Ok(Some((lo, hi))) => {
                if lo <= g.now && g.now < hi {
                    out.push(env, row, w)?;
                }
            }
            Ok(None) => {}
            Err(e) => out.err(err_record(&e), w),
        }
    }
    Ok(())
}

/// Build the pending events of a new operator from the contents of its
/// input node (streamed, the events sorted in bounded memory).
pub fn hydrate(g: &mut Graph, env: &mut Env, p: &TemporalPlan, input: NodeId, gsn: Gsn) -> Result<()> {
    if g.arr(p.events).hydrated {
        return Ok(());
    }
    let ev = g.arr(p.events);
    let layout = ev.layout.clone();
    let mut events = crate::storage::TreeBuilder::new(env, ev.tree, gsn);
    let now = g.now;
    let r = super::ops::snapshot_stream(g, env, input, &mut |env, batch| {
        let mut ups = Vec::new();
        for (row, w) in &batch {
            if let Ok(Some(iv)) = validity(&p.preds, row) {
                schedule(&layout, now, row, *w, iv, &mut ups)?;
            }
        }
        for (e, w) in ups {
            events.push(env, e, w)?;
        }
        Ok(())
    });
    match r {
        Ok(_) => events.finish(env)?,
        Err(e) => {
            events.abort(env)?;
            return Err(e);
        }
    }
    let ev = g.arrs[p.events].as_mut().unwrap();
    ev.hydrated = true;
    ev.since = gsn;
    Ok(())
}

/// Schema of the event arrangement for an input schema.
pub fn event_schema(input: &[ColumnType]) -> Vec<ColumnType> {
    let mut s = vec![ColumnType::new(SqlType::Int8).not_null(), ColumnType::new(SqlType::Int2).not_null()];
    s.extend_from_slice(input);
    s
}
