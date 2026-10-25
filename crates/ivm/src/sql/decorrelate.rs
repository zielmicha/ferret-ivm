//! Subquery decorrelation: turn subquery expressions into `Apply` operators,
//! then rewrite every `Apply` into joins.
//!
//! Simple equality correlations (`inner.x = outer.y` in a filter, possibly
//! below a scalar aggregate) become direct joins. Everything else uses the
//! general dependent-join push-down: D = DISTINCT(correlated outer columns)
//! is pushed through the subquery plan so that it computes the subquery for
//! every value of D, then the outer side joins with it on D (IS NOT DISTINCT
//! FROM).

use super::plan::*;
use crate::error::{codes, Result, SqlError};
use crate::expr::func::resolve;
use crate::expr::{Expr, SubqueryExpr, SubqueryKind};
use crate::sql_bail;
use crate::types::{ColumnType, Datum, SqlType};
use std::collections::BTreeMap;

/// Visit Outer(depth, col) references in `rel` (looking into nested
/// subqueries and Apply right sides with increased depth) and replace them.
pub fn map_outer_refs(rel: &mut Rel, depth: usize, f: &mut dyn FnMut(usize, &SqlType) -> Expr) {
    fn in_expr(e: &mut Expr, depth: usize, f: &mut dyn FnMut(usize, &SqlType) -> Expr) {
        match e {
            Expr::Outer(d, c, t) if *d == depth => {
                *e = f(*c, t);
            }
            Expr::Subquery(s) => {
                map_outer_refs(&mut s.rel, depth + 1, f);
                for c in e.children_mut() {
                    in_expr(c, depth, f);
                }
            }
            _ => {
                for c in e.children_mut() {
                    in_expr(c, depth, f);
                }
            }
        }
    }
    for e in rel.exprs_mut() {
        in_expr(e, depth, f);
    }
    match rel {
        Rel::Apply { left, right, .. } => {
            map_outer_refs(left, depth, f);
            map_outer_refs(right, depth + 1, f);
        }
        _ => {
            for i in rel.inputs_mut() {
                map_outer_refs(i, depth, f);
            }
        }
    }
}

/// Decrement the depth of Outer refs deeper than `depth`.
fn shift_outer_depth(rel: &mut Rel, depth: usize) {
    fn in_expr(e: &mut Expr, depth: usize) {
        match e {
            Expr::Outer(d, _, _) if *d > depth => *d -= 1,
            Expr::Subquery(s) => {
                shift_outer_depth(&mut s.rel, depth + 1);
                for c in e.children_mut() {
                    in_expr(c, depth);
                }
            }
            _ => {
                for c in e.children_mut() {
                    in_expr(c, depth);
                }
            }
        }
    }
    for e in rel.exprs_mut() {
        in_expr(e, depth);
    }
    match rel {
        Rel::Apply { left, right, .. } => {
            shift_outer_depth(left, depth);
            shift_outer_depth(right, depth + 1);
        }
        _ => {
            for i in rel.inputs_mut() {
                shift_outer_depth(i, depth);
            }
        }
    }
}

/// Collect the outer columns referenced at `depth`.
fn outer_cols(rel: &Rel, depth: usize, out: &mut BTreeMap<usize, SqlType>) {
    let mut r = rel.clone();
    map_outer_refs(&mut r, depth, &mut |c, t| {
        out.insert(c, t.clone());
        Expr::Outer(depth, c, t.clone())
    });
}

/// Entry point: remove all subqueries and Apply operators.
pub fn decorrelate(rel: Rel) -> Result<Rel> {
    let rel = extract(rel)?;
    let rel = remove_applies(rel)?;
    if rel.has_outer_refs(1) {
        sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unsupported correlated subquery");
    }
    Ok(rel)
}

// ---------------------------------------------------------------------------
// Phase 1: subquery expressions -> Apply

fn extract(rel: Rel) -> Result<Rel> {
    let rel = map_inputs(rel, extract)?;
    match rel {
        Rel::Filter { input, preds } => {
            if !preds.iter().any(|p| p.has_subquery()) {
                return Ok(Rel::Filter { input, preds });
            }
            let mut cur = *input;
            let mut rest = Vec::new();
            for p in preds {
                match p {
                    Expr::Subquery(sq) if matches!(sq.kind, SubqueryKind::Exists | SubqueryKind::In(_)) => {
                        cur = semi_apply(cur, *sq, false)?;
                    }
                    Expr::Not(inner) if matches!(&*inner, Expr::Subquery(sq) if matches!(sq.kind, SubqueryKind::Exists | SubqueryKind::In(_))) => {
                        let Expr::Subquery(sq) = *inner else { unreachable!() };
                        cur = semi_apply(cur, *sq, true)?;
                    }
                    other => rest.push(other),
                }
            }
            if rest.iter().any(|p| p.has_subquery()) {
                let arity = cur.arity();
                let (input2, rewritten) = lift_into_applies(cur, rest)?;
                let extra = input2.arity() - arity;
                let out = input2.filter(rewritten);
                if extra > 0 {
                    return Ok(out.project((0..arity).collect()));
                }
                return Ok(out);
            }
            Ok(cur.filter(rest))
        }
        Rel::Map { input, exprs } => {
            if !exprs.iter().any(|e| e.has_subquery()) {
                return Ok(Rel::Map { input, exprs });
            }
            let n = input.arity();
            let (input2, rewritten) = lift_into_applies(*input, exprs)?;
            // apply columns sit between the input and the map outputs;
            // lift_into_applies already shifted references accordingly
            let k = input2.arity() - n;
            let m = rewritten.len();
            let out = Rel::Map { input: Box::new(input2), exprs: rewritten };
            let mut proj: Vec<usize> = (0..n).collect();
            proj.extend(n + k..n + k + m);
            Ok(out.project(proj))
        }
        Rel::Join { left, right, kind, on } => {
            if !on.iter().any(|e| e.has_subquery()) {
                return Ok(Rel::Join { left, right, kind, on });
            }
            if kind != JoinKind::Inner {
                sql_bail!(codes::FEATURE_NOT_SUPPORTED, "subqueries in outer join conditions are not supported");
            }
            let (with_sq, plain): (Vec<Expr>, Vec<Expr>) = on.into_iter().partition(|e| e.has_subquery());
            let j = Rel::Join { left, right, kind, on: plain };
            extract(Rel::Filter { input: Box::new(j), preds: with_sq })
        }
        Rel::Values { rows, columns } => {
            if rows.iter().flatten().any(|e| e.has_subquery()) {
                sql_bail!(codes::FEATURE_NOT_SUPPORTED, "subqueries in VALUES are not supported");
            }
            Ok(Rel::Values { rows, columns })
        }
        other => {
            if other.exprs().iter().any(|e| e.has_subquery()) {
                sql_bail!(codes::FEATURE_NOT_SUPPORTED, "subquery in unsupported position");
            }
            Ok(other)
        }
    }
}

fn map_inputs(mut rel: Rel, f: fn(Rel) -> Result<Rel>) -> Result<Rel> {
    for i in rel.inputs_mut() {
        let taken = std::mem::replace(i, Rel::unit());
        *i = f(taken)?;
    }
    Ok(rel)
}

fn semi_apply(input: Rel, sq: SubqueryExpr, negated: bool) -> Result<Rel> {
    let right = extract(sq.rel)?;
    let (lhs, null_aware) = match sq.kind {
        SubqueryKind::Exists => (vec![], false),
        SubqueryKind::In(lhs) => (lhs, negated),
        _ => unreachable!(),
    };
    Ok(Rel::Apply { left: Box::new(input), right: Box::new(right), kind: ApplyKind::Semi { lhs, negated, null_aware } })
}

/// Replace subquery expressions inside `exprs` by references to columns
/// appended to `input` through Mark/Scalar applies.
fn lift_into_applies(input: Rel, exprs: Vec<Expr>) -> Result<(Rel, Vec<Expr>)> {
    let n = input.arity();
    let mut cur = input;
    let mut out = Vec::with_capacity(exprs.len());
    let mut added = 0usize;
    for e in exprs {
        let (e2, new_cur, k) = lift_expr(e, cur, n, added)?;
        cur = new_cur;
        added += k;
        out.push(e2);
    }
    // references to columns >= n (earlier map outputs) shift by `added`
    let out = out.into_iter().map(|e| shift_map_refs(e, n, added)).collect();
    Ok((cur, out))
}

/// Shift Column refs >= n by `k` except those marked as apply columns
/// (encoded as usize::MAX - j during lifting).
fn shift_map_refs(e: Expr, n: usize, k: usize) -> Expr {
    match e {
        Expr::Column(c) if c >= usize::MAX / 2 => Expr::Column(n + (usize::MAX - c)),
        Expr::Column(c) if c >= n => Expr::Column(c + k),
        mut other => {
            if let Expr::Subquery(_) = other {
                return other;
            }
            for ch in other.children_mut() {
                let t = std::mem::replace(ch, Expr::null(SqlType::Unknown));
                *ch = shift_map_refs(t, n, k);
            }
            other
        }
    }
}

fn lift_expr(e: Expr, input: Rel, n: usize, added: usize) -> Result<(Expr, Rel, usize)> {
    match e {
        Expr::Subquery(sq) => {
            let right = extract(sq.rel)?;
            // placeholder column markers resolved by shift_map_refs
            let (kind, value, width) = match sq.kind {
                SubqueryKind::Scalar => {
                    // the value, or the cardinality error where it is used
                    // (value and row count columns)
                    let count = Expr::Column(usize::MAX - added - 1);
                    let many = Expr::Call(Box::new(crate::expr::Call {
                        func: resolve(">", &[SqlType::Int8, SqlType::Int8])?.func,
                        args: vec![count, Expr::Literal(Datum::Int8(1), SqlType::Int8)],
                        ret: SqlType::Bool,
                    }));
                    let err = Expr::Error(
                        codes::CARDINALITY_VIOLATION.to_string(),
                        "more than one row returned by a subquery used as an expression".to_string(),
                    );
                    let value = Expr::Case(Box::new(crate::expr::CaseExpr {
                        whens: vec![(many, err)],
                        else_: Expr::Column(usize::MAX - added),
                        ty: sq.ty.clone(),
                    }));
                    (ApplyKind::Scalar, value, 2)
                }
                k => (ApplyKind::Mark { kind: k }, Expr::Column(usize::MAX - added), 1),
            };
            let rel = Rel::Apply { left: Box::new(input), right: Box::new(right), kind };
            Ok((value, rel, width))
        }
        mut other => {
            let mut cur = input;
            let mut k_total = 0;
            for ch in other.children_mut() {
                let t = std::mem::replace(ch, Expr::null(SqlType::Unknown));
                let (t2, c2, k) = lift_expr(t, cur, n, added + k_total)?;
                *ch = t2;
                cur = c2;
                k_total += k;
            }
            Ok((other, cur, k_total))
        }
    }
}

// ---------------------------------------------------------------------------
// Phase 2: Apply -> joins

fn remove_applies(rel: Rel) -> Result<Rel> {
    let rel = map_inputs(rel, remove_applies)?;
    match rel {
        Rel::Apply { left, right, kind } => apply_to_join(*left, *right, kind),
        other => Ok(other),
    }
}

/// IS NOT DISTINCT FROM equality.
fn nd_eq(a: Expr, b: Expr) -> Expr {
    Expr::IsDistinct(Box::new(a), Box::new(b), true)
}

fn sql_eq(a: Expr, at: &SqlType, b: Expr, bt: &SqlType) -> Result<Expr> {
    let res = resolve("=", &[at.clone(), bt.clone()])?;
    let a = if res.arg_types[0] != *at { Expr::Cast(Box::new(a), res.arg_types[0].clone(), -1, false) } else { a };
    let b = if res.arg_types[1] != *bt { Expr::Cast(Box::new(b), res.arg_types[1].clone(), -1, false) } else { b };
    Ok(Expr::Call(Box::new(crate::expr::Call { func: res.func, args: vec![a, b], ret: SqlType::Bool })))
}

fn agg(func: AggFunc, args: Vec<usize>, ret: ColumnType) -> AggCall {
    AggCall { func, args, distinct: false, filter: None, order: vec![], fraction: None, ret }
}

fn apply_to_join(left: Rel, right: Rel, kind: ApplyKind) -> Result<Rel> {
    let nl = left.arity();
    let correlated = right.has_outer_refs(1);
    if !correlated {
        let mut right = right;
        shift_outer_depth(&mut right, 1);
        return uncorrelated(left, right, kind);
    }
    if let Some(r) = try_simple(&left, &right, &kind)? {
        return Ok(r);
    }
    // general: D = DISTINCT(correlated outer columns)
    let mut cols = BTreeMap::new();
    outer_cols(&right, 1, &mut cols);
    let ccols: Vec<usize> = cols.keys().copied().collect();
    let dpos: BTreeMap<usize, usize> = ccols.iter().enumerate().map(|(i, c)| (*c, i)).collect();
    let left_types = left.schema();
    // D's key over left columns: the correlated columns, plus the exact text
    // of those whose equal values can differ in representation (1.0 and
    // 1.00): the subquery must see each outer value as it is
    let mut dkeys: Vec<Expr> = ccols.iter().map(|c| Expr::Column(*c)).collect();
    for c in &ccols {
        if left_types[*c].ty.needs_fidelity() {
            dkeys.push(Expr::Cast(Box::new(Expr::Column(*c)), SqlType::Text, -1, false));
        }
    }
    let k = dkeys.len();
    let d = Rel::Distinct { input: Box::new(left.clone().map(dkeys.clone()).project((nl..nl + k).collect())) };
    let mut r2 = push_dep(right, &d, k, &dpos)?;
    shift_outer_depth(&mut r2, 1);
    let nr = r2.arity() - k;
    let d_cond = |offset: usize| -> Vec<Expr> {
        dkeys.iter().enumerate().map(|(i, e)| nd_eq(e.clone(), Expr::Column(offset + i))).collect()
    };
    match kind {
        ApplyKind::Semi { lhs, negated, null_aware } => {
            if null_aware {
                // NOT IN: keep rows whose IN-mark is false
                let marked = mark_join(left, r2, k, &dkeys, SubqueryKind::In(lhs))?;
                let m = marked.arity() - 1;
                return Ok(marked.filter(vec![Expr::Not(Box::new(Expr::Column(m)))]).project((0..nl).collect()));
            }
            let mut on = d_cond(nl);
            let rtypes = r2.schema();
            for (j, l) in lhs.into_iter().enumerate() {
                let lt = l.ty(&left_types);
                on.push(sql_eq(l, &lt, Expr::Column(nl + k + j), &rtypes[k + j].ty)?);
            }
            Ok(Rel::Join { left: Box::new(left), right: Box::new(r2), kind: if negated { JoinKind::Anti } else { JoinKind::Semi }, on })
        }
        ApplyKind::Mark { kind: sk } => mark_join(left, r2, k, &dkeys, sk),
        ApplyKind::Scalar => {
            // per D value: the value and the number of rows
            let r3 = scalar_value_and_count(r2, k);
            let j = Rel::Join { left: Box::new(left), right: Box::new(r3), kind: JoinKind::Left, on: d_cond(nl) };
            let mut proj: Vec<usize> = (0..nl).collect();
            proj.extend([nl + k, nl + k + 1]);
            Ok(j.project(proj))
        }
        ApplyKind::Lateral { left_outer } => {
            let j = Rel::Join {
                left: Box::new(left),
                right: Box::new(r2),
                kind: if left_outer { JoinKind::Left } else { JoinKind::Inner },
                on: d_cond(nl),
            };
            let mut proj: Vec<usize> = (0..nl).collect();
            proj.extend(nl + k..nl + k + nr);
            Ok(j.project(proj))
        }
    }
}

/// A scalar subquery's rows (k leading D columns, then its value) as one row
/// per D value: [D, value, number of rows].
fn scalar_value_and_count(r: Rel, k: usize) -> Rel {
    let one = || Expr::Literal(Datum::Int8(1), SqlType::Int8);
    let global_agg = k == 0 && matches!(&r, Rel::Aggregate { group, .. } if group.is_empty());
    if (k > 0 && unique_on_prefix(&r, k)) || global_agg {
        return r.map(vec![one()]);
    }
    let mut ret = r.schema()[k].clone();
    ret.nullable = true;
    let i8 = ColumnType::new(SqlType::Int8).not_null();
    Rel::Aggregate {
        input: Box::new(r),
        group: (0..k).collect(),
        aggs: vec![agg(AggFunc::AnyValue, vec![k], ret), agg(AggFunc::CountStar, vec![], i8)],
    }
}

/// Is `rel` known to have at most one row per value of its first k columns?
fn unique_on_prefix(rel: &Rel, k: usize) -> bool {
    match rel {
        Rel::Aggregate { group, .. } => group.len() <= k && group.iter().enumerate().all(|(i, g)| *g == i) || group.len() == k,
        Rel::Project { input, cols } => cols.len() >= k && cols[..k].iter().enumerate().all(|(i, c)| *c == i) && unique_on_prefix(input, k),
        Rel::Map { input, .. } | Rel::Filter { input, .. } => unique_on_prefix(input, k),
        // scalar-aggregate decorrelation: D LEFT JOIN Agg(group = D)
        Rel::Join { left, right, kind: JoinKind::Left, .. } => {
            matches!(&**left, Rel::Distinct { .. }) && left.arity() == k && unique_on_prefix(right, k)
        }
        _ => false,
    }
}

/// Mark join: append a boolean column computed per left row. `r2` starts
/// with the k columns of D, whose key over left columns is `dkeys`.
fn mark_join(left: Rel, r2: Rel, k: usize, dkeys: &[Expr], sk: SubqueryKind) -> Result<Rel> {
    let nl = left.arity();
    let left_types = left.schema();
    let i8 = ColumnType::new(SqlType::Int8).not_null();
    let d_on = |offset: usize| -> Vec<Expr> {
        dkeys.iter().enumerate().map(|(i, e)| nd_eq(e.clone(), Expr::Column(offset + i))).collect()
    };
    match sk {
        SubqueryKind::Exists => {
            // count per D; exists = count > 0
            let cnt = Rel::Aggregate { input: Box::new(r2), group: (0..k).collect(), aggs: vec![agg(AggFunc::CountStar, vec![], i8)] };
            let j = Rel::Join { left: Box::new(left), right: Box::new(cnt), kind: JoinKind::Left, on: d_on(nl) };
            let mark = Expr::IsNotNull(Box::new(Expr::Column(nl + k)));
            let m = j.map(vec![mark]);
            let a = m.arity();
            let mut proj: Vec<usize> = (0..nl).collect();
            proj.push(a - 1);
            Ok(m.project(proj))
        }
        SubqueryKind::In(lhs) if lhs.len() > 1 => {
            // (x, y) IN (...) is = ANY over rows: fields compare as a conjunction
            let lt: Vec<SqlType> = lhs.iter().map(|e| e.ty(&left_types)).collect();
            let rt: Vec<SqlType> = r2.schema()[k..].iter().map(|c| c.ty.clone()).collect();
            quantified_join(left, r2, k, dkeys, lhs, false, &|l, r| {
                let mut parts = Vec::with_capacity(l.len());
                for i in 0..l.len() {
                    parts.push(sql_eq(l[i].clone(), &lt[i], r[i].clone(), &rt[i])?);
                }
                Ok(Expr::And(parts))
            })
        }
        SubqueryKind::In(lhs) => {
            let lhs = lhs.into_iter().next().unwrap();
            let lt = lhs.ty(&left_types);
            let rtypes = r2.schema();
            let ct = rtypes[k].ty.clone();
            // per (D, value): count; per D: count(*), count(value)
            let a1 = Rel::Aggregate {
                input: Box::new(r2.clone().filter(vec![Expr::IsNotNull(Box::new(Expr::Column(k)))])),
                group: (0..=k).collect(),
                aggs: vec![agg(AggFunc::CountStar, vec![], i8.clone())],
            };
            let a2 = Rel::Aggregate {
                input: Box::new(r2),
                group: (0..k).collect(),
                aggs: vec![agg(AggFunc::CountStar, vec![], i8.clone()), agg(AggFunc::Count, vec![k], i8.clone())],
            };
            // left + lhs value as a column
            let lm = left.map(vec![lhs]);
            let xcol = nl;
            let mut on1 = d_on(nl + 1);
            on1.push(sql_eq(Expr::Column(xcol), &lt, Expr::Column(nl + 1 + k), &ct)?);
            let j1 = Rel::Join { left: Box::new(lm), right: Box::new(a1), kind: JoinKind::Left, on: on1 };
            let off2 = nl + 1 + k + 2;
            let j2 = Rel::Join { left: Box::new(j1), right: Box::new(a2), kind: JoinKind::Left, on: d_on(off2) };
            let matched = Expr::IsNotNull(Box::new(Expr::Column(nl + 1 + k + 1)));
            let total = Expr::Coalesce(vec![Expr::Column(off2 + k), Expr::Literal(Datum::Int8(0), SqlType::Int8)], SqlType::Int8);
            let nonnull = Expr::Coalesce(vec![Expr::Column(off2 + k + 1), Expr::Literal(Datum::Int8(0), SqlType::Int8)], SqlType::Int8);
            let gt0 = |e: Expr| -> Result<Expr> {
                sql_eq(e, &SqlType::Int8, Expr::Literal(Datum::Int8(0), SqlType::Int8), &SqlType::Int8).map(|x| Expr::Not(Box::new(x)))
            };
            let has_null = gt0(Expr::Call(Box::new(crate::expr::Call {
                func: resolve("-", &[SqlType::Int8, SqlType::Int8])?.func,
                args: vec![total.clone(), nonnull],
                ret: SqlType::Int8,
            })))?;
            let nonempty = gt0(total)?;
            let mark = Expr::Case(Box::new(crate::expr::CaseExpr {
                whens: vec![
                    (
                        Expr::IsNull(Box::new(Expr::Column(xcol))),
                        Expr::Case(Box::new(crate::expr::CaseExpr {
                            whens: vec![(nonempty, Expr::null(SqlType::Bool))],
                            else_: Expr::bool(false),
                            ty: SqlType::Bool,
                        })),
                    ),
                    (matched, Expr::bool(true)),
                    (has_null, Expr::null(SqlType::Bool)),
                ],
                else_: Expr::bool(false),
                ty: SqlType::Bool,
            }));
            let m = j2.map(vec![mark]);
            let a = m.arity();
            let mut proj: Vec<usize> = (0..nl).collect();
            proj.push(a - 1);
            Ok(m.project(proj))
        }
        SubqueryKind::AnyAll { lhs, func, all } => quantified_join(left, r2, k, dkeys, vec![*lhs], all, &|l, r| {
            Ok(Expr::Call(Box::new(crate::expr::Call { func, args: vec![l[0].clone(), r[0].clone()], ret: SqlType::Bool })))
        }),
        SubqueryKind::Scalar => unreachable!(),
    }
}

/// Quantified comparison mark join: per left row, ANY (or ALL) of `pred`
/// between its `lhs` values and the subquery rows of its D value, with SQL's
/// three-valued logic (an empty subquery gives false for ANY, true for ALL).
///
/// The mark depends on nothing but a row's D and `lhs` values, so it is
/// computed once per distinct pair of those and joined back on them: left
/// rows can be wide (keeping whole rows as grouping keys arranged every
/// column of a join, TOASTed text included, several times over).
fn quantified_join(
    left: Rel,
    r2: Rel,
    k: usize,
    dkeys: &[Expr],
    lhs: Vec<Expr>,
    all: bool,
    pred: &dyn Fn(&[Expr], &[Expr]) -> Result<Expr>,
) -> Result<Rel> {
    let nl = left.arity();
    let m = lhs.len();
    let w = r2.arity() - k;
    let nd = dkeys.len();
    // distinct (D values, compared values) of the left rows
    let keyexprs: Vec<Expr> = dkeys.iter().cloned().chain(lhs).collect();
    let n1 = nd + m;
    let keys = Rel::Distinct { input: Box::new(left.clone().map(keyexprs.clone()).project((nl..nl + n1).collect())) };
    // the subquery rows of each, with a marker that is NULL only when the
    // left join found no row at all
    let rm = r2.map(vec![Expr::bool(true)]);
    let on: Vec<Expr> = (0..nd).map(|i| nd_eq(Expr::Column(i), Expr::Column(n1 + i))).collect();
    let j = Rel::Join { left: Box::new(keys), right: Box::new(rm), kind: JoinKind::Left, on };
    let lcols: Vec<Expr> = (nd..n1).map(Expr::Column).collect();
    let rcols: Vec<Expr> = (n1 + k..n1 + k + w).map(Expr::Column).collect();
    let cmp = pred(&lcols, &rcols)?;
    let present = Expr::IsNotNull(Box::new(Expr::Column(n1 + k + w)));
    let test = |t| Expr::And(vec![present.clone(), Expr::BoolTest(Box::new(cmp.clone()), t)]);
    let jm = j.map(vec![test(crate::expr::BoolTest::IsTrue), test(crate::expr::BoolTest::IsUnknown), test(crate::expr::BoolTest::IsFalse)]);
    let base = jm.arity() - 3;
    let b = ColumnType::new(SqlType::Bool);
    let grouped = Rel::Aggregate {
        input: Box::new(jm),
        group: (0..n1).collect(),
        aggs: vec![
            agg(AggFunc::BoolOr, vec![base], b.clone()),
            agg(AggFunc::BoolOr, vec![base + 1], b.clone()),
            agg(AggFunc::BoolOr, vec![base + 2], b),
        ],
    };
    let (any_true, any_null, any_false) = (Expr::Column(n1), Expr::Column(n1 + 1), Expr::Column(n1 + 2));
    let mark = if all {
        // ALL: false if any false; else null if any null; else true
        Expr::Case(Box::new(crate::expr::CaseExpr {
            whens: vec![(any_false, Expr::bool(false)), (any_null, Expr::null(SqlType::Bool))],
            else_: Expr::bool(true),
            ty: SqlType::Bool,
        }))
    } else {
        // ANY: true if any true; else null if any null; else false
        Expr::Case(Box::new(crate::expr::CaseExpr {
            whens: vec![(any_true, Expr::bool(true)), (any_null, Expr::null(SqlType::Bool))],
            else_: Expr::bool(false),
            ty: SqlType::Bool,
        }))
    };
    let gm = grouped.map(vec![mark]);
    let ga = gm.arity();
    // back to the left rows (each matches the one group of its values)
    let lk = left.map(keyexprs);
    let on: Vec<Expr> = (0..n1).map(|i| nd_eq(Expr::Column(nl + i), Expr::Column(nl + n1 + i))).collect();
    let back = Rel::Join { left: Box::new(lk), right: Box::new(gm), kind: JoinKind::Inner, on };
    let mut proj: Vec<usize> = (0..nl).collect();
    proj.push(nl + n1 + ga - 1);
    Ok(back.project(proj))
}

fn uncorrelated(left: Rel, right: Rel, kind: ApplyKind) -> Result<Rel> {
    let nl = left.arity();
    match kind {
        ApplyKind::Semi { lhs, negated, null_aware } => {
            if null_aware {
                let marked = mark_join(left, right, 0, &[], SubqueryKind::In(lhs))?;
                let m = marked.arity() - 1;
                return Ok(marked.filter(vec![Expr::Not(Box::new(Expr::Column(m)))]).project((0..nl).collect()));
            }
            let left_types = left.schema();
            let rtypes = right.schema();
            let mut on = Vec::new();
            for (j, l) in lhs.into_iter().enumerate() {
                let lt = l.ty(&left_types);
                on.push(sql_eq(l, &lt, Expr::Column(nl + j), &rtypes[j].ty)?);
            }
            Ok(Rel::Join { left: Box::new(left), right: Box::new(right), kind: if negated { JoinKind::Anti } else { JoinKind::Semi }, on })
        }
        ApplyKind::Mark { kind } => mark_join(left, right, 0, &[], kind),
        ApplyKind::Scalar => {
            // exactly one row: the value (NULL when empty) and the row count
            let single = scalar_value_and_count(right, 0);
            Ok(Rel::Join { left: Box::new(left), right: Box::new(single), kind: JoinKind::Inner, on: vec![] })
        }
        ApplyKind::Lateral { left_outer } => Ok(Rel::Join {
            left: Box::new(left),
            right: Box::new(right),
            kind: if left_outer { JoinKind::Left } else { JoinKind::Inner },
            on: vec![],
        }),
    }
}

/// Rewrite an expression of an operator whose input gained k leading D columns.
fn rw(e: &Expr, k: usize, dpos: &BTreeMap<usize, usize>) -> Expr {
    match e {
        Expr::Column(i) => Expr::Column(i + k),
        Expr::Outer(1, c, _) => Expr::Column(dpos[c]),
        _ => {
            let mut out = e.clone();
            for ch in out.children_mut() {
                *ch = rw(ch, k, dpos);
            }
            out
        }
    }
}

/// Push the dependent set D (k columns) through `rel`. The result's output is
/// D's columns followed by `rel`'s columns, evaluated for every D value.
fn push_dep(rel: Rel, d: &Rel, k: usize, dpos: &BTreeMap<usize, usize>) -> Result<Rel> {
    if !rel.has_outer_refs(1) {
        return Ok(Rel::Join { left: Box::new(d.clone()), right: Box::new(rel), kind: JoinKind::Inner, on: vec![] });
    }
    let keep_d = |n: usize| -> Vec<usize> { (0..k).chain(k..k + n).collect() };
    Ok(match rel {
        Rel::Filter { input, preds } => {
            let i2 = push_dep(*input, d, k, dpos)?;
            Rel::Filter { input: Box::new(i2), preds: preds.iter().map(|p| rw(p, k, dpos)).collect() }
        }
        Rel::Map { input, exprs } => {
            let i2 = push_dep(*input, d, k, dpos)?;
            Rel::Map { input: Box::new(i2), exprs: exprs.iter().map(|e| rw(e, k, dpos)).collect() }
        }
        Rel::Project { input, cols } => {
            let i2 = push_dep(*input, d, k, dpos)?;
            let mut c2: Vec<usize> = (0..k).collect();
            c2.extend(cols.iter().map(|c| c + k));
            Rel::Project { input: Box::new(i2), cols: c2 }
        }
        Rel::Join { left, right, kind, on } => {
            let (lc, rc) = (left.has_outer_refs(1), right.has_outer_refs(1));
            let (na, nb) = (left.arity(), right.arity());
            let both_needed = match kind {
                JoinKind::Inner | JoinKind::Semi | JoinKind::Anti => lc && rc || (!lc && rc && matches!(kind, JoinKind::Semi | JoinKind::Anti)),
                JoinKind::Left => rc,
                JoinKind::Right => lc,
                JoinKind::Full => true,
            };
            if !both_needed && lc {
                // [D, a, b]
                let a2 = push_dep(*left, d, k, dpos)?;
                let on2 = on.iter().map(|e| rw(e, k, dpos)).collect();
                Rel::Join { left: Box::new(a2), right, kind, on: on2 }
            } else if !both_needed && rc && kind == JoinKind::Inner {
                // a x [D, b] -> reorder to [D, a, b]
                let b2 = push_dep(*right, d, k, dpos)?;
                let on2: Vec<Expr> = on
                    .iter()
                    .map(|e| {
                        let shifted = e.remapped(&|c| if c < na { c } else { c + k });
                        rw_outer_only(&shifted, na, dpos)
                    })
                    .collect();
                let j = Rel::Join { left, right: Box::new(b2), kind, on: on2 };
                let mut proj: Vec<usize> = (na..na + k).collect();
                proj.extend(0..na);
                proj.extend(na + k..na + k + nb);
                j.project(proj)
            } else {
                // push into both sides; join on D (null-safe); coalesce D for outer joins
                let a2 = push_dep(*left, d, k, dpos)?;
                let b2 = push_dep(*right, d, k, dpos)?;
                let mut on2: Vec<Expr> = on
                    .iter()
                    .map(|e| {
                        // left cols -> +k, right cols -> +2k, outer -> left D
                        let shifted = e.remapped(&|c| if c < na { c + k } else { c + 2 * k });
                        rw_outer_only(&shifted, 0, dpos)
                    })
                    .collect();
                for i in 0..k {
                    on2.push(nd_eq(Expr::Column(i), Expr::Column(k + na + i)));
                }
                let semi = matches!(kind, JoinKind::Semi | JoinKind::Anti);
                let j = Rel::Join { left: Box::new(a2), right: Box::new(b2), kind, on: on2 };
                if semi {
                    j
                } else if matches!(kind, JoinKind::Right | JoinKind::Full) {
                    let total = k + na + k + nb;
                    let coalesced: Vec<Expr> = (0..k)
                        .map(|i| {
                            let t = d.schema()[i].ty.clone();
                            Expr::Coalesce(vec![Expr::Column(i), Expr::Column(k + na + i)], t)
                        })
                        .collect();
                    let m = j.map(coalesced);
                    let mut proj: Vec<usize> = (total..total + k).collect();
                    proj.extend(k..k + na);
                    proj.extend(k + na + k..k + na + k + nb);
                    m.project(proj)
                } else {
                    let mut proj: Vec<usize> = (0..k + na).collect();
                    proj.extend(k + na + k..k + na + k + nb);
                    j.project(proj)
                }
            }
        }
        Rel::Aggregate { input, group, aggs } => {
            let scalar = group.is_empty();
            let i2 = push_dep(*input, d, k, dpos)?;
            let mut g2: Vec<usize> = (0..k).collect();
            g2.extend(group.iter().map(|g| g + k));
            let aggs2: Vec<AggCall> = aggs
                .iter()
                .map(|a| AggCall {
                    args: a.args.iter().map(|x| x + k).collect(),
                    filter: a.filter.map(|x| x + k),
                    order: a.order.iter().map(|o| SortKey { col: o.col + k, spec: o.spec }).collect(),
                    ..a.clone()
                })
                .collect();
            let na = aggs2.len();
            let empty_zero: Vec<bool> = aggs2.iter().map(|a| a.func.empty_is_zero()).collect();
            let grouped = Rel::Aggregate { input: Box::new(i2), group: g2, aggs: aggs2 };
            if !scalar {
                grouped
            } else {
                // one row per D value even when the subquery input is empty
                let on: Vec<Expr> = (0..k).map(|i| nd_eq(Expr::Column(i), Expr::Column(k + i))).collect();
                let j = Rel::Join { left: Box::new(d.clone()), right: Box::new(grouped), kind: JoinKind::Left, on };
                let exprs: Vec<Expr> = (0..na)
                    .map(|i| {
                        let c = Expr::Column(k + k + i);
                        if empty_zero[i] {
                            Expr::Coalesce(vec![c, Expr::Literal(Datum::Int8(0), SqlType::Int8)], SqlType::Int8)
                        } else {
                            c
                        }
                    })
                    .collect();
                let base = 2 * k + na;
                let m = j.map(exprs);
                let mut proj: Vec<usize> = (0..k).collect();
                proj.extend(base..base + na);
                m.project(proj)
            }
        }
        Rel::Distinct { input } => Rel::Distinct { input: Box::new(push_dep(*input, d, k, dpos)?) },
        Rel::TopK { input, group, order, limit, offset, with_ties } => {
            let i2 = push_dep(*input, d, k, dpos)?;
            let mut g2: Vec<usize> = (0..k).collect();
            g2.extend(group.iter().map(|g| g + k));
            Rel::TopK {
                input: Box::new(i2),
                group: g2,
                order: order.iter().map(|o| SortKey { col: o.col + k, spec: o.spec }).collect(),
                limit,
                offset,
                with_ties,
            }
        }
        Rel::Window { input, funcs } => {
            let i2 = push_dep(*input, d, k, dpos)?;
            let funcs2 = funcs
                .iter()
                .map(|f| {
                    let mut partition: Vec<usize> = (0..k).collect();
                    partition.extend(f.partition.iter().map(|p| p + k));
                    WindowCall {
                        args: f.args.iter().map(|a| a + k).collect(),
                        partition,
                        order: f.order.iter().map(|o| SortKey { col: o.col + k, spec: o.spec }).collect(),
                        ..f.clone()
                    }
                })
                .collect();
            Rel::Window { input: Box::new(i2), funcs: funcs2 }
        }
        Rel::UnionAll { inputs } => {
            Rel::UnionAll { inputs: inputs.into_iter().map(|i| push_dep(i, d, k, dpos)).collect::<Result<_>>()? }
        }
        Rel::SetOp { op, all, left, right } => Rel::SetOp {
            op,
            all,
            left: Box::new(push_dep(*left, d, k, dpos)?),
            right: Box::new(push_dep(*right, d, k, dpos)?),
        },
        Rel::Negate { input } => Rel::Negate { input: Box::new(push_dep(*input, d, k, dpos)?) },
        Rel::FlatMap { input, mut funcs, out, ordinality } => {
            let i2 = push_dep(*input, d, k, dpos)?;
            for f in funcs.iter_mut() {
                f.args = f.args.iter().map(|a| rw(a, k, dpos)).collect();
            }
            Rel::FlatMap { input: Box::new(i2), funcs, out, ordinality }
        }
        Rel::Values { rows, columns } => {
            let n = columns.len();
            let mut branches = Vec::new();
            for r in rows {
                let exprs: Vec<Expr> = r.iter().map(|e| rw_outer_only(e, 0, dpos)).collect();
                branches.push(d.clone().map(exprs).project(keep_d(n).iter().map(|c| if *c < k { *c } else { *c }).collect()));
            }
            if branches.is_empty() {
                let mut cols = d.schema();
                cols.extend(columns);
                Rel::Values { rows: vec![], columns: cols }
            } else if branches.len() == 1 {
                branches.pop().unwrap()
            } else {
                Rel::UnionAll { inputs: branches }
            }
        }
        Rel::Recursive { .. } | Rel::RecRef { .. } => {
            sql_bail!(codes::FEATURE_NOT_SUPPORTED, "correlated recursive queries are not supported")
        }
        other @ (Rel::Get { .. } | Rel::Apply { .. }) => {
            return Err(SqlError::internal(format!("unexpected correlated operator: {}", other.explain())));
        }
    })
}

/// Replace Outer(1, c) by Column(base + dpos[c]) leaving other columns alone.
fn rw_outer_only(e: &Expr, base: usize, dpos: &BTreeMap<usize, usize>) -> Expr {
    match e {
        Expr::Outer(1, c, _) => Expr::Column(base + dpos[c]),
        _ => {
            let mut out = e.clone();
            for ch in out.children_mut() {
                *ch = rw_outer_only(ch, base, dpos);
            }
            out
        }
    }
}

// ---------------------------------------------------------------------------
// Fast path: equality correlation pulled up into the join condition.

/// Peel Filter/Map/Project layers of a correlated plan, lifting correlated
/// conjuncts to the top. Returns (uncorrelated plan, lifted predicates over
/// its columns, visible arity): the first `vis` columns of the plan are the
/// original output; columns needed only by lifted predicates follow.
fn lift_filters(rel: &Rel) -> Option<(Rel, Vec<Expr>, usize)> {
    if !rel.has_outer_refs(1) {
        return Some((rel.clone(), vec![], rel.arity()));
    }
    match rel {
        Rel::Filter { input, preds } => {
            let (corr, plain): (Vec<Expr>, Vec<Expr>) = preds.iter().cloned().partition(|p| p.has_outer());
            if corr.iter().any(|p| p.has_subquery()) {
                return None;
            }
            let (inner, mut lifted, vis) = lift_filters(input)?;
            lifted.extend(corr);
            Some((inner.filter(plain), lifted, vis))
        }
        Rel::Map { input, exprs } => {
            if exprs.iter().any(|e| e.has_outer()) {
                return None;
            }
            let (inner, lifted, vis) = lift_filters(input)?;
            let extras = inner.arity() - vis;
            let m = exprs.len();
            // map expressions may reference earlier map outputs at >= vis
            let exprs2: Vec<Expr> = exprs.iter().map(|e| e.remapped(&|c| if c < vis { c } else { c + extras })).collect();
            let mapped = Rel::Map { input: Box::new(inner), exprs: exprs2 };
            // reorder to [visible, map outputs, extras]
            let mut proj: Vec<usize> = (0..vis).collect();
            proj.extend(vis + extras..vis + extras + m);
            proj.extend(vis..vis + extras);
            let lifted2 = lifted.iter().map(|p| p.remapped(&|c| if c < vis { c } else { c + m })).collect();
            Some((mapped.project(proj), lifted2, vis + m))
        }
        Rel::Project { input, cols } => {
            let (inner, lifted, _vis) = lift_filters(input)?;
            let mut needed = Vec::new();
            for p in &lifted {
                p.columns(&mut needed);
            }
            needed.sort();
            needed.dedup();
            let mut proj = cols.clone();
            let mut remap = std::collections::HashMap::new();
            for c in needed {
                remap.insert(c, proj.len());
                proj.push(c);
            }
            let lifted2 = lifted.iter().map(|p| p.remapped(&|c| remap[&c])).collect();
            Some((inner.project(proj), lifted2, cols.len()))
        }
        _ => None,
    }
}

/// Convert a lifted predicate into a join condition over [left, right].
fn to_join_cond(e: &Expr, nl: usize) -> Expr {
    match e {
        Expr::Column(c) => Expr::Column(c + nl),
        Expr::Outer(1, c, _) => Expr::Column(*c),
        Expr::Outer(d, c, t) => Expr::Outer(d - 1, *c, t.clone()),
        _ => {
            let mut o = e.clone();
            for ch in o.children_mut() {
                *ch = to_join_cond(ch, nl);
            }
            o
        }
    }
}

fn try_simple(left: &Rel, right: &Rel, kind: &ApplyKind) -> Result<Option<Rel>> {
    let nl = left.arity();
    match kind {
        ApplyKind::Semi { lhs, negated, null_aware: false } => {
            let Some((mut base, lifted, vis)) = lift_filters(right) else { return Ok(None) };
            if base.has_outer_refs(1) || lifted.is_empty() {
                return Ok(None);
            }
            shift_outer_depth(&mut base, 1);
            let left_types = left.schema();
            let btypes = base.schema();
            let mut on: Vec<Expr> = lifted.iter().map(|p| to_join_cond(p, nl)).collect();
            for (j, l) in lhs.iter().enumerate() {
                if j >= vis {
                    return Ok(None);
                }
                let lt = l.ty(&left_types);
                on.push(sql_eq(l.clone(), &lt, Expr::Column(nl + j), &btypes[j].ty)?);
            }
            Ok(Some(Rel::Join {
                left: Box::new(left.clone()),
                right: Box::new(base),
                kind: if *negated { JoinKind::Anti } else { JoinKind::Semi },
                on,
            }))
        }
        ApplyKind::Lateral { left_outer: false } => {
            let Some((mut base, lifted, vis)) = lift_filters(right) else { return Ok(None) };
            if base.has_outer_refs(1) || lifted.is_empty() {
                return Ok(None);
            }
            shift_outer_depth(&mut base, 1);
            let on: Vec<Expr> = lifted.iter().map(|p| to_join_cond(p, nl)).collect();
            let j = Rel::Join { left: Box::new(left.clone()), right: Box::new(base), kind: JoinKind::Inner, on };
            Ok(Some(j.project((0..nl + vis).collect())))
        }
        ApplyKind::Scalar => simple_scalar_agg(left, right),
        _ => Ok(None),
    }
}

/// Scalar subquery of the form f(AGG(...) FROM base WHERE inner = outer.c ...):
/// group the aggregate by the inner key expressions and LEFT JOIN on them,
/// turning COUNTs of missing groups into 0.
fn simple_scalar_agg(left: &Rel, right: &Rel) -> Result<Option<Rel>> {
    // peel Map/Project layers above the aggregate (top-down)
    let mut layers: Vec<&Rel> = Vec::new();
    let mut cur = right;
    while let Rel::Project { input, .. } | Rel::Map { input, .. } = cur {
        if cur.exprs().iter().any(|e| e.has_outer() || e.has_subquery()) {
            return Ok(None);
        }
        layers.push(cur);
        cur = input;
    }
    let Rel::Aggregate { input, group, aggs } = cur else { return Ok(None) };
    if !group.is_empty() {
        return Ok(None);
    }
    let Some((mut base, lifted, _)) = lift_filters(input) else { return Ok(None) };
    if base.has_outer_refs(1) || lifted.is_empty() {
        return Ok(None);
    }
    // every lifted predicate must be `inner_expr = outer_expr`, an equality
    // between an expression over the subquery's rows and one over outer
    // columns only; the subquery is grouped by the inner expressions and
    // joined with the same equalities (operator and casts unchanged)
    let mut keys: Vec<Expr> = Vec::new();
    let mut eqs: Vec<(&crate::expr::Call, usize)> = Vec::new(); // (equality, argument that is the key)
    let outer_only = |e: &Expr| e.has_outer() && e.column_set().is_empty() && !e.has_subquery();
    for p in &lifted {
        let Expr::Call(c) = p else { return Ok(None) };
        if crate::expr::func::func_def(c.func).name != "=" || c.args.len() != 2 {
            return Ok(None);
        }
        let (a, b) = (&c.args[0], &c.args[1]);
        let k = if outer_only(b) && !a.has_outer() {
            0
        } else if outer_only(a) && !b.has_outer() {
            1
        } else {
            return Ok(None);
        };
        keys.push(c.args[k].clone());
        eqs.push((c, k));
    }
    shift_outer_depth(&mut base, 1);
    let nb = base.arity();
    let nk = keys.len();
    let na = aggs.len();
    let mapped = base.map(keys);
    let agg2 = Rel::Aggregate { input: Box::new(mapped), group: (nb..nb + nk).collect(), aggs: aggs.clone() };
    let nl = left.arity();
    let mut on = Vec::new();
    for (i, (c, k)) in eqs.iter().enumerate() {
        let mut args = vec![Expr::Column(nl + i), to_join_cond(&c.args[1 - k], nl)];
        if *k == 1 {
            args.reverse();
        }
        on.push(Expr::Call(Box::new(crate::expr::Call { func: c.func, args, ret: c.ret.clone() })));
    }
    let j = Rel::Join { left: Box::new(left.clone()), right: Box::new(agg2), kind: JoinKind::Left, on };
    // aggregate values over the joined row (COUNT of no rows is 0)
    let vals: Vec<Expr> = (0..na)
        .map(|i| {
            let c = Expr::Column(nl + nk + i);
            if aggs[i].func.empty_is_zero() {
                Expr::Coalesce(vec![c, Expr::Literal(Datum::Int8(0), SqlType::Int8)], SqlType::Int8)
            } else {
                c
            }
        })
        .collect();
    // compose the peeled layers as expressions over the aggregate outputs
    let mut exprs: Vec<Expr> = (0..na).map(Expr::Column).collect();
    for layer in layers.iter().rev() {
        match layer {
            Rel::Map { exprs: m, .. } => {
                let mut all = exprs.clone();
                for e in m {
                    let s = e.substitute(&all);
                    all.push(s);
                }
                exprs = all;
            }
            Rel::Project { cols, .. } => exprs = cols.iter().map(|c| exprs[*c].clone()).collect(),
            _ => unreachable!(),
        }
    }
    if exprs.len() != 1 {
        return Ok(None);
    }
    let value = exprs[0].substitute(&vals);
    // one aggregate row per outer row: the row count is 1
    let out = j.map(vec![value, Expr::Literal(Datum::Int8(1), SqlType::Int8)]);
    let a = out.arity();
    let mut proj: Vec<usize> = (0..nl).collect();
    proj.extend([a - 2, a - 1]);
    Ok(Some(out.project(proj)))
}
