//! Temporal filters: WHERE predicates over now().
//!
//! The wall clock is an input like any other (logged ticks, see the
//! sequencer), so a predicate such as `ts > now() - interval '1 hour'` is
//! maintained rather than recomputed. Each row is valid over a set of
//! intervals of logical time, found by evaluating parts of the predicate
//! itself (which keeps time zones and truncation exact). Its clock side is
//! a chain of steps applied to the clock reading — casts, truncations,
//! constants added or subtracted — that are all non-decreasing except
//! adding a number of months to a date or timestamp: that clamps the day
//! to the end of a shorter month (March 29th to 31st minus a month are all
//! February 28th, at their time of day), so the sum goes back by up to a
//! day at the midnights starting such days.
//!
//! Without months, the predicate changes value at most once: at a guess
//! derived from the row's value, verified, else found by bisection. With
//! months, the clock side is split at its outermost addition of months,
//! `outer(inner + n months)`: `outer` is non-decreasing, so the predicate
//! holds from some month sum on (found as above); the values of `inner`
//! whose sum reaches it follow from the calendar (`month_sum_at_least`:
//! from one value on, and, if the sum is on the last day of a shorter month,
//! only from its time of day on each day clamped to it); and when `inner`
//! takes those values is found the same way, recursively. The operator
//! emits a row when it becomes valid and retracts it when it expires;
//! future events wait in an arrangement ordered by time, and each step
//! releases the events that became due since the previous one.

use super::delta::{NodeDelta, StepOut};
use super::*;
use crate::error::codes;
use crate::expr::func::func_def;
use crate::expr::NowKind;
use crate::types::{Datum, SqlType};
use smallvec::{smallvec, SmallVec};
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
    /// `clock` split at its additions of months.
    chain: Clock,
}

impl TemporalPred {
    fn new(clock: Expr, op: CmpOp, other: Expr) -> Self {
        TemporalPred { chain: Clock::new(clock.clone()), clock, op, other }
    }

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
            // (earlier versions ended the validity of a row at one of the
            // changes of such a predicate: their events are rebuilt)
            if p.chain.sum.is_some() {
                fp.push(0xD0);
            }
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

/// Is `e` a function of the clock alone made of steps that are
/// non-decreasing, except for additions of months (see `Clock`)?
fn clock_side(e: &Expr) -> bool {
    if e.now_kind().is_some() {
        return true;
    }
    match e {
        Expr::Cast(x, ty, _, _) => matches!(ty, SqlType::Timestamp | SqlType::TimestampTz | SqlType::Date) && clock_side(x),
        Expr::Call(c) => match (func_def(c.func).name, c.args.as_slice()) {
            ("+", [a, b]) => (clock_side(a) && is_const(b)) || (is_const(a) && clock_side(b)),
            ("-", [a, b]) => clock_side(a) && is_const(b),
            ("date_trunc", [u, x]) => is_const(u) && clock_side(x),
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
    if !clock_side(clock) {
        return Err(unsupported());
    }
    let clock = clock.replace_now(&clock_value);
    let op = |n: &str| match (n, flipped) {
        ("<", false) | (">", true) => CmpOp::Lt,
        ("<=", false) | (">=", true) => CmpOp::Le,
        (">", false) | ("<", true) => CmpOp::Gt,
        _ => CmpOp::Ge,
    };
    let mk = |op| TemporalPred::new(clock.clone(), op, other.clone());
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

const DAY: i64 = 86_400_000_000;

/// Margin around the month sums at the ends of the searched range, making
/// a range of sums that contains every sum reached in it: the value months
/// are added to goes back by less than a day (if it adds months itself),
/// so the sums reached lie within a month or two of those at the ends.
const SUM_MARGIN: i64 = 366 * DAY;

/// A clock side as the chain of steps it applies to the clock reading,
/// split at its outermost addition of months.
#[derive(Clone, Debug)]
struct Clock {
    /// Over the clock reading.
    expr: Expr,
    /// None: non-decreasing in the clock reading.
    sum: Option<Box<MonthSum>>,
}

/// `outer(inner + months)`, the sum clamping its day to the end of a
/// shorter month.
#[derive(Clone, Debug)]
struct MonthSum {
    /// A date or timestamp.
    inner: Clock,
    months: i32,
    /// Over the month sum, non-decreasing: the rest of the clock side,
    /// from the days and time of the added interval on.
    outer: Expr,
    /// The sum is a timestamptz (else a timestamp).
    tz: bool,
    /// `inner` is a date: its values are whole days.
    date: bool,
    /// Month sums to search for where `outer` changes: those reached by
    /// the clock in the searched range, and some margin.
    range: (i64, i64),
}

impl Clock {
    fn new(expr: Expr) -> Clock {
        let sum = split_months(&expr).and_then(|(outer, inner, months, tz, date)| {
            let inner = Clock::new(inner);
            let iv = pgdatetime::Interval { months, days: 0, micros: 0 };
            let sum_at = |t| {
                let (u, _) = micros(&inner.expr.eval(&[Datum::TimestampTz(t)]).ok()?)?;
                pgdatetime::timestamp_pl_interval(u, iv).ok()
            };
            // (a clock side failing at the ends of the searched range, with
            // absurd constants, is searched as if non-decreasing)
            let range = (sum_at(T_LO)?.checked_sub(SUM_MARGIN)?, sum_at(T_HI)?.checked_add(SUM_MARGIN)?);
            Some(Box::new(MonthSum { inner, months, outer, tz, date, range }))
        });
        Clock { expr, sum }
    }
}

/// Split a clock side at its outermost addition of months: (what is
/// applied to the sum, over it; the value months are added to; their
/// number; the sum is a timestamptz; the value is a date).
fn split_months(e: &Expr) -> Option<(Expr, Expr, i32, bool, bool)> {
    if let Some(s) = month_step(e) {
        return Some(s);
    }
    let mut outer = e.clone();
    let x = match &mut outer {
        Expr::Cast(x, ..) => x.as_mut(),
        Expr::Call(c) => c.args.iter_mut().find(|a| !is_const(a))?,
        _ => return None,
    };
    let (o, inner, months, tz, date) = split_months(x)?;
    *x = o;
    Some((outer, inner, months, tz, date))
}

/// `e` adds a constant interval with months to a date or timestamp: split
/// as in `split_months`, the interval's days and time going to the outer
/// part (they are added after the months).
fn month_step(e: &Expr) -> Option<(Expr, Expr, i32, bool, bool)> {
    let Expr::Call(c) = e else { return None };
    let (x, k, minus) = match (func_def(c.func).name, c.args.as_slice()) {
        ("+", [a, b]) if is_const(b) => (a, b, false),
        ("+", [a, b]) if is_const(a) => (b, a, false),
        ("-", [a, b]) if is_const(b) => (a, b, true),
        _ => return None,
    };
    let ty = x.ty(&[ColumnType::new(SqlType::TimestampTz)]);
    if !matches!(ty, SqlType::Date | SqlType::Timestamp | SqlType::TimestampTz) {
        return None;
    }
    // (an interval that fails or is infinite makes the step a constant)
    let Ok(Datum::Interval(iv)) = k.eval(&[]) else { return None };
    if iv.months == 0 || !pgdatetime::isfinite_interval(iv) {
        return None;
    }
    let months = if minus { iv.months.checked_neg()? } else { iv.months };
    let tz = ty == SqlType::TimestampTz;
    let rest = pgdatetime::Interval { months: 0, ..iv };
    let outer = if rest.days == 0 && rest.micros == 0 {
        Expr::Column(0)
    } else {
        let sum_ty = if tz { SqlType::TimestampTz } else { SqlType::Timestamp };
        let f = crate::expr::func::resolve(if minus { "-" } else { "+" }, &[sum_ty, SqlType::Interval]).ok()?;
        let args = vec![Expr::Column(0), Expr::Literal(Datum::Interval(rest), SqlType::Interval)];
        Expr::Call(Box::new(crate::expr::Call { func: f.func, args, ret: f.ret }))
    };
    Some((outer, x.clone(), months, tz, ty == SqlType::Date))
}

/// The first day (days since 2000-01-01) and the number of days of the
/// month `shift` months after that of day `d`.
fn month_of(d: i32, shift: i32) -> Option<(i32, i32)> {
    let (y, m, _) = pgdatetime::date_to_ymd(d);
    let k = y as i64 * 12 + m as i64 - 1 + shift as i64;
    let first = |k: i64| pgdatetime::ymd_to_date(i32::try_from(k.div_euclid(12)).ok()?, k.rem_euclid(12) as u32 + 1, 1).ok();
    let a = first(k)?;
    Some((a, first(k + 1)? - a))
}

/// The values (microseconds of a date or timestamp) whose sum with `n`
/// months is at least `c`, as a set of intervals like `Validity`. A value's
/// sum lies in the month `n` months after its own, so the values of later
/// months reach `c` and those of earlier months do not. In the month in
/// between, a value's sum has its day and time of day, the day clamped to
/// the last one of a shorter target month: the values reach `c` from the
/// same day and time on, and if `c` is on that clamped last day, each later
/// day of the month reaches it at its time of day again.
fn month_sum_at_least(n: i32, c: i64) -> Option<Validity> {
    let day = i32::try_from(c.div_euclid(DAY)).ok()?;
    let tod = c.rem_euclid(DAY);
    let (t_first, t_len) = month_of(day, 0)?;
    let (s_first, s_len) = month_of(day, n.checked_neg()?)?;
    let at = |d: i32| (s_first as i64 + d as i64) * DAY;
    let d = day - t_first;
    if d >= s_len {
        // (a longer target month: no sum of a value of the month reaches c)
        return Some(smallvec![(at(s_len), i64::MAX)]);
    }
    if d + 1 < t_len || s_len <= t_len || tod == 0 {
        return Some(smallvec![(at(d) + tod, i64::MAX)]);
    }
    let mut v: Validity = smallvec![(at(d) + tod, at(t_len))];
    for clamped in t_len..s_len - 1 {
        v.push((at(clamped) + tod, at(clamped + 1)));
    }
    v.push((at(s_len - 1) + tod, i64::MAX));
    Some(v)
}

/// Logical times at which a row is valid: sorted, disjoint, non-adjacent
/// intervals [lo, hi), lo = i64::MIN: since before the searched range,
/// hi = i64::MAX: past it.
pub type Validity = SmallVec<[(i64, i64); 2]>;

const ALWAYS: (i64, i64) = (i64::MIN, i64::MAX);

fn complement(v: &[(i64, i64)]) -> Validity {
    let mut out = Validity::new();
    let mut at = i64::MIN;
    for &(lo, hi) in v {
        if at < lo {
            out.push((at, lo));
        }
        at = hi;
    }
    if at < i64::MAX {
        out.push((at, i64::MAX));
    }
    out
}

fn intersect(a: &[(i64, i64)], b: &[(i64, i64)]) -> Validity {
    let mut out = Validity::new();
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        let (lo, hi) = (a[i].0.max(b[j].0), a[i].1.min(b[j].1));
        if lo < hi {
            out.push((lo, hi));
        }
        if a[i].1 < b[j].1 {
            i += 1;
        } else {
            j += 1;
        }
    }
    out
}

/// Sort and merge intervals.
fn normalize(mut v: Validity) -> Validity {
    v.sort_unstable();
    let mut out = Validity::new();
    for (lo, hi) in v {
        match out.last_mut() {
            Some(last) if lo <= last.1 => last.1 = last.1.max(hi),
            _ if lo < hi => out.push((lo, hi)),
            _ => {}
        }
    }
    out
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

/// Where a non-decreasing predicate becomes true within a searched range.
enum Transition {
    /// Already true at its start.
    Before,
    /// Still false at its end.
    After,
    At(i64),
}

#[cfg(test)]
thread_local! {
    static CLOCK_EVALS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Evaluate (a part of) a clock side at `v`.
fn eval_at(e: &Expr, v: Datum) -> Result<Datum> {
    #[cfg(test)]
    CLOCK_EVALS.with(|n| n.set(n.get() + 1));
    e.eval(&[v])
}

/// A value of the timestamp family in microseconds, with the step between
/// consecutive values of its type.
fn micros(d: &Datum) -> Option<(i64, i64)> {
    match d {
        Datum::TimestampTz(v) | Datum::Timestamp(v) if *v != i64::MIN && *v != i64::MAX => Some((*v, 1)),
        Datum::Date(d) if *d != i32::MIN && *d != i32::MAX => Some(((*d as i64).checked_mul(DAY)?, DAY)),
        _ => None,
    }
}

/// A guess at the argument (in microseconds) for which `e`, applied to
/// `at(t)`, reaches `y`: steps are mostly shifts by a constant, so it is
/// t0 + (y - e(t0)) taken at t0 = y.
fn shift_guess(e: &Expr, at: impl Fn(i64) -> Datum, y: i64) -> Option<i64> {
    let (v0, _) = micros(&eval_at(e, at(y)).ok()?)?;
    y.checked_add(y.checked_sub(v0)?)
}

/// Where `p` (non-decreasing in t) becomes true in (lo, hi]. A guess is
/// taken when it is the transition (exact whatever the function: month
/// arithmetic, time zones, truncation); otherwise the range is bisected.
fn transition(p: &dyn Fn(i64) -> bool, guess: Option<i64>, (lo, hi): (i64, i64)) -> Transition {
    if let Some(t) = guess
        && t > lo
        && t <= hi
        && p(t)
        && !p(t - 1)
    {
        return Transition::At(t);
    }
    if p(lo) {
        Transition::Before
    } else if !p(hi) {
        Transition::After
    } else {
        Transition::At(first_true(lo, hi, p))
    }
}

/// When the value of `clock` satisfies `up`, a condition that is false up
/// to some value and true from it on (`y`: a guess at that value, in
/// microseconds).
fn solve(clock: &Clock, up: &dyn Fn(Result<Datum>) -> bool, y: Option<i64>) -> Validity {
    let Some(m) = &clock.sum else { return solve_monotone(&clock.expr, up, y) };
    // the month sums from which it holds (`outer` is non-decreasing)
    let sum = |c| if m.tz { Datum::TimestampTz(c) } else { Datum::Timestamp(c) };
    let guess = y.and_then(|y| shift_guess(&m.outer, sum, y));
    let c = match transition(&|c| up(eval_at(&m.outer, sum(c))), guess, m.range) {
        Transition::Before => return smallvec![ALWAYS],
        Transition::After => return Validity::new(),
        Transition::At(c) => c,
    };
    let Some(values) = month_sum_at_least(m.months, c) else { return solve_monotone(&clock.expr, up, y) };
    // when `inner` takes those values
    let reaches = |a: i64| {
        // (a date reaches `a` from the next midnight on)
        let a = if m.date { a.div_euclid(DAY) * DAY + if a.rem_euclid(DAY) == 0 { 0 } else { DAY } } else { a };
        solve(&m.inner, &|v| matches!(v, Ok(v) if micros(&v).is_some_and(|(u, _)| u >= a)), Some(a))
    };
    let mut out = Validity::new();
    for (a, b) in values {
        let from = reaches(a);
        if b == i64::MAX {
            out.extend(from);
        } else {
            out.extend(intersect(&from, &complement(&reaches(b))));
        }
    }
    normalize(out)
}

/// `solve` for a clock side that is non-decreasing: the condition changes
/// at most once, searched in (T_LO, T_HI].
fn solve_monotone(e: &Expr, up: &dyn Fn(Result<Datum>) -> bool, y: Option<i64>) -> Validity {
    let guess = y.and_then(|y| shift_guess(e, Datum::TimestampTz, y));
    match transition(&|t| up(eval_at(e, Datum::TimestampTz(t))), guess, (T_LO, T_HI)) {
        Transition::Before => smallvec![ALWAYS],
        Transition::After => Validity::new(),
        Transition::At(t) => smallvec![(t, i64::MAX)],
    }
}

/// The logical times at which `row` passes every predicate. A predicate's
/// value is taken where it is in the searched range: before it, as at its
/// start; past it, as at its end. Errors come from evaluating the row side.
pub fn validity(preds: &[TemporalPred], row: &[Datum]) -> Result<Validity> {
    let mut valid: Option<Validity> = None;
    for tp in preds {
        let x = tp.other.eval(row)?;
        if x.is_null() {
            return Ok(Validity::new());
        }
        // a lower bound: where it starts to hold; an upper one: where it
        // stops holding (a failing clock side does not hold)
        let lower = tp.lower();
        let up = |v: Result<Datum>| matches!(v, Ok(v) if !v.is_null() && tp.op.holds(v.sql_cmp(&x))) == lower;
        // the first value of the clock side for which `up` holds
        let y = micros(&x).and_then(|(xv, step)| if matches!(tp.op, CmpOp::Gt | CmpOp::Le) { xv.checked_add(step) } else { Some(xv) });
        let s = solve(&tp.chain, &up, y);
        let s = if lower { s } else { complement(&s) };
        let v = match valid {
            None => s,
            Some(v) => intersect(&v, &s),
        };
        if v.is_empty() {
            return Ok(v);
        }
        valid = Some(v);
    }
    Ok(valid.unwrap_or_else(|| smallvec![ALWAYS]))
}

fn event_row(time: i64, kind: i16, row: &[Datum]) -> Row {
    let mut r = Vec::with_capacity(row.len() + 2);
    r.push(Datum::Int8(time));
    r.push(Datum::Int2(kind));
    r.extend_from_slice(row);
    r
}

/// Schedule the future events of `row` (weight `w`) given when it is
/// valid; returns whether it is valid now.
fn schedule(layout: &Layout, now: i64, row: &[Datum], w: i64, valid: &[(i64, i64)], ups: &mut Vec<(Vec<u8>, i64)>) -> Result<bool> {
    let mut now_valid = false;
    for &(lo, hi) in valid.iter().filter(|(_, hi)| now < *hi) {
        if now < lo {
            ups.push((layout.encode_row(&event_row(lo, 0, row))?, w));
        } else {
            now_valid = true;
        }
        if hi != i64::MAX {
            ups.push((layout.encode_row(&event_row(hi, 1, row))?, w));
        }
    }
    Ok(now_valid)
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
    // Events that are due: those up to now, after the clock of the previous
    // step (it released everything up to it, and events are only ever
    // scheduled after the clock). Released events stay behind as entries of
    // weight zero until they are compacted away; they are never visited.
    let after = |t: i64| crate::types::encoding::prefix_successor(&layout.encode_prefix(&[Datum::Int8(t)]));
    let lo = match g.released_through {
        None => Some(Bound::Unbounded),
        Some(t) if t >= now => None,
        Some(t) => after(t).map(Bound::Included),
    };
    let bound = after(now);
    let hi = match &bound {
        Some(b) => Bound::Excluded(b.as_slice()),
        None => Bound::Unbounded,
    };
    let mut c = match &lo {
        Some(lo) => env.cursor(tree, lo.as_ref().map(|k| k.as_slice()), hi),
        // (nothing can be due)
        None => env.cursor(tree, Bound::Excluded(&[]), Bound::Included(&[])),
    };
    while let Some((k, h)) = env.cursor_next(&mut c)? {
        visited(1);
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
                Ok(valid) => {
                    if schedule(&layout, now, row, *w, &valid, &mut ups)? {
                        out.push(env, row.clone(), *w)?;
                    }
                }
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
            Ok(valid) => {
                if valid.iter().any(|&(lo, hi)| lo <= g.now && g.now < hi) {
                    out.push(env, row, w)?;
                }
            }
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
            if let Ok(valid) = validity(&p.preds, row) {
                schedule(&layout, now, row, *w, &valid, &mut ups)?;
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
    g.set_arr_hydrated(p.events, true, gsn);
    Ok(())
}

/// Schema of the event arrangement for an input schema.
pub fn event_schema(input: &[ColumnType]) -> Vec<ColumnType> {
    let mut s = vec![ColumnType::new(SqlType::Int8).not_null(), ColumnType::new(SqlType::Int2).not_null()];
    s.extend_from_slice(input);
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SqlType;
    use pgdatetime::Interval;
    use SqlType::{Date, Timestamp as Ts, TimestampTz as Tz};

    fn call(name: &str, args: Vec<Expr>, types: &[SqlType]) -> Expr {
        let r = crate::expr::func::resolve(name, types).unwrap();
        Expr::Call(Box::new(crate::expr::Call { func: r.func, args, ret: r.ret }))
    }

    fn interval(months: i32, days: i32, micros: i64) -> Expr {
        Expr::Literal(Datum::Interval(Interval { months, days, micros }), SqlType::Interval)
    }

    fn unit(u: &str) -> Expr {
        Expr::Literal(Datum::text_owned(u.to_string()), SqlType::Text)
    }

    fn rng(seed: u64) -> impl FnMut() -> u64 {
        let mut rng = seed;
        move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        }
    }

    /// A value of type `ty` at `t` microseconds (a date: its day).
    fn value(ty: &SqlType, t: i64) -> Datum {
        match ty {
            Tz => Datum::TimestampTz(t),
            Ts => Datum::Timestamp(t),
            _ => Datum::Date(t.div_euclid(DAY).clamp(i32::MIN as i64, i32::MAX as i64) as i32),
        }
    }

    /// Whether the predicate holds at `t`, evaluated directly.
    fn holds(tp: &TemporalPred, x: &Datum, t: i64) -> bool {
        match tp.clock.eval(&[Datum::TimestampTz(t)]) {
            Ok(v) if !v.is_null() => tp.op.holds(v.sql_cmp(x)),
            _ => false,
        }
    }

    fn contains(v: &[(i64, i64)], t: i64) -> bool {
        v.iter().any(|&(lo, hi)| lo <= t && t < hi)
    }

    /// The validity interval by bisection alone (as it was computed before
    /// guesses were tried), for a clock side without months.
    fn bisected(tp: &TemporalPred, x: &Datum) -> Vec<(i64, i64)> {
        let holds = |t: i64| holds(tp, x, t);
        let (lo, hi) = if tp.lower() {
            if holds(T_LO) {
                (i64::MIN, i64::MAX)
            } else if !holds(T_HI) {
                return vec![];
            } else {
                (first_true(T_LO, T_HI, &holds), i64::MAX)
            }
        } else if !holds(T_LO) {
            return vec![];
        } else if !holds(T_HI) {
            (i64::MIN, first_true(T_LO, T_HI, &|t| !holds(t)))
        } else {
            (i64::MIN, i64::MAX)
        };
        vec![(lo, hi)]
    }

    /// Validity intervals are those bisection finds for clock sides that
    /// are shifts (by intervals of days and of time), truncations and casts
    /// of the clock, compared with values of each timestamp type, also
    /// infinite and out of the searched range; the usual forms take three
    /// evaluations of the clock side instead of ~55.
    #[test]
    fn validity_by_guess_equals_bisection() {
        let now = Expr::Column(0);
        let as_ts = Expr::Cast(Box::new(now.clone()), Ts, -1, false);
        let as_date = Expr::Cast(Box::new(now.clone()), Date, -1, false);
        // (clock side, its type, a shift of the clock)
        let clocks: Vec<(Expr, SqlType, bool)> = vec![
            (now.clone(), Tz, true),
            (call("-", vec![now.clone(), interval(0, 0, 3_600_000_000)], &[Tz, SqlType::Interval]), Tz, true),
            (call("+", vec![now.clone(), interval(0, 7, 0)], &[Tz, SqlType::Interval]), Tz, true),
            (call("date_trunc", vec![unit("day"), now.clone()], &[SqlType::Text, Tz]), Tz, false),
            (as_ts.clone(), Ts, true),
            (call("-", vec![as_ts, interval(0, 1, 0)], &[Ts, SqlType::Interval]), Ts, true),
            (as_date, Date, false),
        ];
        let mut next = rng(0x9E37_79B9_7F4A_7C15);
        let (mut evals, mut rows, mut shift_evals, mut shift_rows) = (0u64, 0u64, 0u64, 0u64);
        for (clock, ty, shift) in &clocks {
            for op in [CmpOp::Lt, CmpOp::Le, CmpOp::Gt, CmpOp::Ge] {
                let tp = TemporalPred::new(clock.clone(), op, Expr::Column(0));
                assert!(tp.chain.sum.is_none());
                for i in 0..300 {
                    // around 2026, at day boundaries, far out, infinite
                    let t: i64 = match i % 6 {
                        0 => 820_000_000_000_000 + (next() % 1_000_000_000_000) as i64,
                        1 => 820_000_000_000_000 + (next() % 1000) as i64 * 86_400_000_000,
                        2 => (next() % 1_000_000_000_000_000_000) as i64 - 500_000_000_000_000_000,
                        3 => [i64::MIN, i64::MAX, T_LO, T_HI, T_LO + 1, T_HI - 1][(next() % 6) as usize],
                        _ => 820_000_000_000_000 + (next() % 100) as i64 * 86_400_000_000 + (next() % 3) as i64 - 1,
                    };
                    let row = [value(ty, t)];
                    CLOCK_EVALS.with(|n| n.set(0));
                    let got = validity(std::slice::from_ref(&tp), &row).unwrap();
                    let n = CLOCK_EVALS.with(|n| n.get());
                    let want: Validity = bisected(&tp, &row[0]).into_iter().filter(|(lo, hi)| lo < hi).collect();
                    assert_eq!(got, want, "{clock:?} {op:?} {:?}", row[0]);
                    evals += n;
                    rows += 1;
                    if *shift && i % 6 != 2 && i % 6 != 3 {
                        shift_evals += n;
                        shift_rows += 1;
                    }
                }
            }
        }
        // shifts of the clock in the searched range: the guess (one
        // evaluation) verified (two)
        assert_eq!(shift_evals, 3 * shift_rows, "{shift_evals} evaluations for {shift_rows} rows");
        assert!(evals < 20 * rows, "{evals} evaluations for {rows} rows");
    }

    /// A random clock side with at least one addition of months: casts,
    /// truncations, constants added and subtracted (months, days and time,
    /// days to a date), nested.
    fn random_clock(next: &mut impl FnMut() -> u64) -> (Expr, SqlType) {
        let iv = SqlType::Interval;
        let (mut e, mut ty) = match next() % 3 {
            0 => (Expr::Column(0), Tz),
            1 => (Expr::Cast(Box::new(Expr::Column(0)), Ts, -1, false), Ts),
            _ => (Expr::Cast(Box::new(Expr::Column(0)), Date, -1, false), Date),
        };
        let steps = 1 + next() % 4;
        let month_step = next() % steps;
        for i in 0..steps {
            let kind = if i == month_step { 0 } else { next() % 6 };
            let sum_ty = if ty == Date { Ts } else { ty.clone() };
            match kind {
                0..=2 => {
                    // months (and sometimes days and time)
                    let months = [1, -1, 1, -1, 2, -2, 12, -12, 13, -11, 25, -24][(next() % 12) as usize];
                    let days = if next().is_multiple_of(3) { (next() % 7) as i32 - 3 } else { 0 };
                    let micros = if next().is_multiple_of(3) { (next() % (2 * DAY as u64)) as i64 - DAY } else { 0 };
                    let k = interval(months, days, micros);
                    e = match next() % 3 {
                        0 => call("+", vec![e, k], &[ty.clone(), iv.clone()]),
                        1 => call("+", vec![k, e], &[iv.clone(), ty.clone()]),
                        _ => call("-", vec![e, k], &[ty.clone(), iv.clone()]),
                    };
                    ty = sum_ty;
                }
                3 => {
                    if ty == Date {
                        let k = Expr::Literal(Datum::Int4((next() % 7) as i32 - 3), SqlType::Int4);
                        e = call("+", vec![e, k], &[Date, SqlType::Int4]);
                    } else {
                        let k = interval(0, (next() % 7) as i32 - 3, (next() % (2 * DAY as u64)) as i64 - DAY);
                        e = call("-", vec![e, k], &[ty.clone(), iv.clone()]);
                    }
                }
                4 => {
                    let to = [Tz, Ts, Date][(next() % 3) as usize].clone();
                    e = Expr::Cast(Box::new(e), to.clone(), -1, false);
                    ty = to;
                }
                _ => {
                    if ty != Date {
                        let u = ["hour", "day", "day", "week", "month", "minute"][(next() % 6) as usize];
                        e = call("date_trunc", vec![unit(u), e], &[SqlType::Text, ty.clone()]);
                    }
                }
            }
        }
        (e, ty)
    }

    /// Clock sides that add months go back at the midnights starting days
    /// clamped to the end of a shorter month: validity, a set of intervals,
    /// equals the predicate evaluated at every minute of ten days around a
    /// month's end (2023 to 2029, leap days included), at every midnight,
    /// and next to every bound found, for rows near where the clock side
    /// reaches them; and each bound is a change of the predicate.
    #[test]
    fn validity_with_months_is_exact() {
        let mut next = rng(0x2545_F491_4F6C_DD1D);
        let (mut cases, mut sets, mut evals) = (0u64, 0u64, 0u64);
        for case in 0..400 {
            let (clock, ty) = random_clock(&mut next);
            let op = [CmpOp::Lt, CmpOp::Le, CmpOp::Gt, CmpOp::Ge][(next() % 4) as usize];
            let tp = TemporalPred::new(clock.clone(), op, Expr::Column(0));
            assert!(tp.chain.sum.is_some(), "{clock:?}");
            // ten days around the start of a month
            let k = (next() % 84) as i64;
            let first = pgdatetime::ymd_to_date(2023 + (k / 12) as i32, (k % 12) as u32 + 1, 1).unwrap() as i64 * DAY;
            let (w0, w1) = (first - 6 * DAY, first + 4 * DAY);
            for _ in 0..4 {
                // a row near the clock side at some time of the window
                let t = w0 + (next() % (w1 - w0) as u64) as i64;
                let Ok(v) = clock.eval(&[Datum::TimestampTz(t)]) else { continue };
                let Some((v, _)) = micros(&v) else { continue };
                let x = match next() % 5 {
                    0 => v,
                    1 => v + (next() % 3) as i64 - 1,
                    2 => v.div_euclid(DAY) * DAY,
                    3 => v + (next() % DAY as u64) as i64 - DAY / 2,
                    _ => v + (next() % (6 * DAY) as u64) as i64 - 3 * DAY,
                };
                let x = value(&ty, x);
                CLOCK_EVALS.with(|n| n.set(0));
                let got = validity(std::slice::from_ref(&tp), std::slice::from_ref(&x)).unwrap();
                evals += CLOCK_EVALS.with(|n| n.get());
                cases += 1;
                sets += (got.len() > 1) as u64;
                let what = format!("case {case}: {clock:?} {op:?} {x:?}: {got:?}");
                assert!(got.windows(2).all(|w| w[0].1 < w[1].0) && got.iter().all(|(lo, hi)| lo < hi), "{what}");
                for &(lo, hi) in &got {
                    for b in [lo, hi] {
                        if b != i64::MIN && b != i64::MAX {
                            assert_ne!(holds(&tp, &x, b), holds(&tp, &x, b - 1), "bound {b} of {what}");
                        }
                    }
                }
                let offset = (next() % 60_000_000) as i64;
                let minutes = (w0..w1).step_by(60_000_000).map(|t| t + offset);
                let midnights = (w0..w1).step_by(DAY as usize).flat_map(|d| [d - 1, d, d + 1]);
                let bounds = got.iter().flat_map(|&(lo, hi)| [lo.saturating_sub(1), lo, hi.saturating_sub(1), hi]).filter(|t| (T_LO..T_HI).contains(t));
                for t in minutes.chain(midnights).chain(bounds) {
                    assert_eq!(contains(&got, t), holds(&tp, &x, t), "at {t}, {what}");
                }
            }
        }
        // the rows near a clamped day's end are valid several times
        assert!(sets > cases / 50, "{sets} of {cases} rows valid several times");
        assert!(evals < 60 * cases, "{evals} evaluations for {cases} rows");
    }

    /// `now() - interval '1 month'` and the like: the month sum from which
    /// a row passes is its own value (one evaluation of the identity,
    /// verified with two), as is the clock reading from which the sum
    /// reaches it, except on the clamped days.
    #[test]
    fn month_shifts_of_the_clock_are_cheap() {
        let now = Expr::Column(0);
        let mut next = rng(0x1234_5678_9ABC_DEF1);
        for k in [interval(-1, 0, 0), interval(1, 0, 0), interval(-12, 0, 0)] {
            let clock = call("+", vec![now.clone(), k], &[Tz, SqlType::Interval]);
            for op in [CmpOp::Lt, CmpOp::Le, CmpOp::Gt, CmpOp::Ge] {
                let tp = TemporalPred::new(clock.clone(), op, Expr::Column(0));
                let (mut evals, mut rows) = (0, 0);
                for _ in 0..1000 {
                    let x = Datum::TimestampTz(820_000_000_000_000 + (next() % 100_000_000_000_000) as i64);
                    CLOCK_EVALS.with(|n| n.set(0));
                    let got = validity(std::slice::from_ref(&tp), std::slice::from_ref(&x)).unwrap();
                    evals += CLOCK_EVALS.with(|n| n.get());
                    rows += 1;
                    if got.len() == 1 {
                        assert_eq!(CLOCK_EVALS.with(|n| n.get()), 6, "{clock:?} {op:?} {x:?}");
                    }
                }
                assert!(evals < 7 * rows, "{evals} evaluations for {rows} rows");
            }
        }
    }
}
