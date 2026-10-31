//! Remove dependent subqueries over stable logical bindings. Pushing a domain
//! through a relation adds bindings; it never shifts references to existing
//! columns. Dense offsets are assigned after all relational rewrites.

use super::logical::{self, ColumnId, LogicalKind as K, LogicalRel as L, columns, rename};
use super::plan::*;
use crate::error::{Result, SqlError, codes};
use crate::expr::func::resolve;
use crate::expr::{Expr, SubqueryKind};
use crate::sql_bail;
use crate::types::{ColumnType, Datum, SqlType};
use std::collections::BTreeMap;

/// Binder-only substitution in the positional bound syntax tree.
pub fn map_outer_refs(rel: &mut Rel, depth: usize, f: &mut dyn FnMut(usize, &SqlType) -> Expr) {
    rel.visit_exprs_scoped_mut(depth, &mut |e, depth| {
        if let Expr::Outer(d, c, t) = e
            && *d == depth
        {
            *e = f(*c, t);
            return false;
        }
        true
    });
}

/// Compatibility entry point for bound DML plans.
pub fn decorrelate(rel: Rel) -> Result<Rel> {
    Ok(decorrelate_logical(logical::bind(rel)?)?.into_dense())
}

pub fn decorrelate_logical(rel: L) -> Result<L> {
    let rel = remove_applies(rel)?;
    if rel.has_outer_refs(1) {
        sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unsupported correlated subquery");
    }
    Ok(rel)
}
fn remove_applies(rel: L) -> Result<L> {
    let rel = rel.map_inputs(remove_applies)?;
    let output = rel.columns;
    match rel.kind {
        K::Apply { left, right, kind } => Ok(bind_output(apply_to_join(*left, *right, kind)?, output)),
        kind => Ok(L::new(kind, output)),
    }
}
/// Bind newly produced values to the Apply's identities, already referenced
/// by surrounding expressions. Input bindings retain their identities.
fn bind_output(mut rel: L, output: Vec<ColumnId>) -> L {
    assert_eq!(rel.arity(), output.len());
    let mut ids = Vec::new();
    let mut exprs = Vec::new();
    for (&from, &to) in rel.columns.iter().zip(&output) {
        if from != to {
            ids.push(to);
            exprs.push(col(from));
        }
    }
    if !ids.is_empty() {
        rel = rel.map_with(ids, exprs);
    }
    rel.project(output)
}
fn col(c: ColumnId) -> Expr {
    Expr::LogicalColumn(c)
}
fn one() -> Expr {
    Expr::Literal(Datum::Int8(1), SqlType::Int8)
}
fn zero() -> Expr {
    Expr::Literal(Datum::Int8(0), SqlType::Int8)
}
fn i8() -> ColumnType {
    ColumnType::new(SqlType::Int8).not_null()
}
fn nd_eq(a: Expr, b: Expr) -> Expr {
    Expr::IsDistinct(Box::new(a), Box::new(b), true)
}
fn sql_eq(a: Expr, at: &SqlType, b: Expr, bt: &SqlType) -> Result<Expr> {
    let res = resolve("=", &[at.clone(), bt.clone()])?;
    let a = if res.arg_types[0] != *at { Expr::Cast(Box::new(a), res.arg_types[0].clone(), -1, false) } else { a };
    let b = if res.arg_types[1] != *bt { Expr::Cast(Box::new(b), res.arg_types[1].clone(), -1, false) } else { b };
    Ok(Expr::Call(Box::new(crate::expr::Call { func: res.func, args: vec![a, b], ret: SqlType::Bool })))
}
fn agg(func: AggFunc, args: Vec<ColumnId>, ret: ColumnType) -> AggCall<ColumnId> {
    AggCall { func, args, distinct: false, filter: None, order: vec![], fraction: None, ret }
}
fn shift_outer_depth(rel: &mut L, depth: usize) {
    rel.visit_scoped(depth, &mut |e, depth| {
        if let Expr::LogicalOuter(d, _, _) = e
            && *d > depth
        {
            *d -= 1;
        }
    });
}
fn apply_to_join(left: L, mut right: L, kind: ApplyKind) -> Result<L> {
    if !right.has_outer_refs(1) {
        shift_outer_depth(&mut right, 1);
        return finish_apply(left, right, kind, &[], &[]);
    }
    if let Some(rel) = try_simple(&left, &right, &kind)? {
        return Ok(rel);
    }
    let outer: Vec<_> = right.outer_columns(1).into_keys().collect();
    let mut dkeys: Vec<_> = outer.iter().map(|c| col(*c)).collect();
    for c in &outer {
        if left.column_type(*c).ty.needs_fidelity() {
            dkeys.push(Expr::Cast(Box::new(col(*c)), SqlType::Text, -1, false));
        }
    }
    let mapped = left.clone().map(dkeys.clone());
    let ids = mapped.columns[left.arity()..].to_vec();
    let d = mapped.project(ids).distinct();
    let dpos = outer.iter().enumerate().map(|(i, c)| (*c, i)).collect();
    let (mut right, keys) = push_dep(right, &d, &dpos)?;
    shift_outer_depth(&mut right, 1);
    finish_apply(left, right, kind, &dkeys, &keys)
}
fn d_condition(dkeys: &[Expr], keys: &[ColumnId]) -> Vec<Expr> {
    dkeys.iter().zip(keys).map(|(e, c)| nd_eq(e.clone(), col(*c))).collect()
}
fn finish_apply(left: L, right: L, kind: ApplyKind, dkeys: &[Expr], keys: &[ColumnId]) -> Result<L> {
    let left_output = left.columns.clone();
    let values = right.columns[keys.len()..].to_vec();
    match kind {
        ApplyKind::Semi { lhs, negated, null_aware } => {
            if null_aware {
                let marked = mark_join(left, right, dkeys, keys, SubqueryKind::In(lhs))?;
                let mark = *marked.columns.last().unwrap();
                return Ok(marked.filter(vec![Expr::Not(Box::new(col(mark)))]).project(left_output));
            }
            let mut on = d_condition(dkeys, keys);
            for (l, c) in lhs.into_iter().zip(&values) {
                on.push(sql_eq(l.clone(), &left.type_of(&l).ty, col(*c), &right.column_type(*c).ty)?);
            }
            Ok(left.join(right, if negated { JoinKind::Anti } else { JoinKind::Semi }, on))
        }
        ApplyKind::Mark { kind } => mark_join(left, right, dkeys, keys, kind),
        ApplyKind::Scalar => {
            let right = scalar_value_and_count(right, keys);
            let values = right.columns[keys.len()..].to_vec();
            let join = left.join(right, if keys.is_empty() { JoinKind::Inner } else { JoinKind::Left }, d_condition(dkeys, keys));
            Ok(join.project(left_output.into_iter().chain(values).collect()))
        }
        ApplyKind::Lateral { left_outer } => {
            let join = left.join(right, if left_outer { JoinKind::Left } else { JoinKind::Inner }, d_condition(dkeys, keys));
            Ok(join.project(left_output.into_iter().chain(values).collect()))
        }
    }
}
fn scalar_value_and_count(r: L, keys: &[ColumnId]) -> L {
    let global = keys.is_empty() && matches!(&r.kind, K::Aggregate { group, .. } if group.is_empty());
    if (!keys.is_empty() && unique_on(&r, keys)) || global {
        return r.map(vec![one()]);
    }
    let value = r.columns[keys.len()];
    let mut ret = r.column_type(value);
    ret.nullable = true;
    r.aggregate(keys.to_vec(), vec![agg(AggFunc::AnyValue, vec![value], ret), agg(AggFunc::CountStar, vec![], i8())])
}
fn unique_on(r: &L, keys: &[ColumnId]) -> bool {
    match &r.kind {
        K::Aggregate { group, .. } => group.iter().all(|c| keys.contains(c)),
        K::Project { input, .. } | K::Map { input, .. } | K::Filter { input, .. } => unique_on(input, keys),
        K::Join { left, right, kind: JoinKind::Left, .. } => {
            matches!(left.kind, K::Distinct { .. }) && left.columns == keys && unique_on(right, &right.columns[..keys.len()])
        }
        _ => false,
    }
}
fn mark_join(left: L, right: L, dkeys: &[Expr], keys: &[ColumnId], kind: SubqueryKind) -> Result<L> {
    match kind {
        SubqueryKind::Exists => {
            let output = left.columns.clone();
            let count = right.aggregate(keys.to_vec(), vec![agg(AggFunc::CountStar, vec![], i8())]);
            let c = *count.columns.last().unwrap();
            let join = left.join(count, JoinKind::Left, d_condition(dkeys, keys));
            let mark =
                Expr::Not(Box::new(sql_eq(Expr::Coalesce(vec![col(c), zero()], SqlType::Int8), &SqlType::Int8, zero(), &SqlType::Int8)?));
            let mapped = join.map(vec![mark]);
            let id = *mapped.columns.last().unwrap();
            Ok(mapped.project(output.into_iter().chain([id]).collect()))
        }
        SubqueryKind::In(mut lhs) if lhs.len() == 1 => single_in_join(left, right, dkeys, keys, lhs.pop().unwrap()),
        SubqueryKind::In(lhs) => {
            let lt: Vec<_> = lhs.iter().map(|e| left.type_of(e).ty).collect();
            let rt: Vec<_> = right.schema()[keys.len()..].iter().map(|c| c.ty.clone()).collect();
            quantified_join(left, right, dkeys, keys, lhs, false, &|l, r| {
                let parts = (0..l.len()).map(|i| sql_eq(l[i].clone(), &lt[i], r[i].clone(), &rt[i])).collect::<Result<Vec<_>>>()?;
                Ok(if parts.len() == 1 { parts.into_iter().next().unwrap() } else { Expr::And(parts) })
            })
        }
        SubqueryKind::AnyAll { lhs, func, all } => quantified_join(left, right, dkeys, keys, vec![*lhs], all, &|l, r| {
            Ok(Expr::Call(Box::new(crate::expr::Call { func, args: vec![l[0].clone(), r[0].clone()], ret: SqlType::Bool })))
        }),
        SubqueryKind::Scalar => unreachable!(),
    }
}
/// A scalar IN is summarized by value and equijoined. Comparing every
/// distinct left value with every right row would turn it into a Cartesian
/// product; only row-valued/ordered quantified comparisons need that path.
fn single_in_join(left: L, right: L, dkeys: &[Expr], keys: &[ColumnId], lhs: Expr) -> Result<L> {
    let output = left.columns.clone();
    let lt = left.type_of(&lhs).ty;
    let value = right.columns[keys.len()];
    let rt = right.column_type(value).ty;
    let mut by_value = keys.to_vec();
    by_value.push(value);
    let matches = right
        .clone()
        .filter(vec![Expr::IsNotNull(Box::new(col(value)))])
        .aggregate(by_value, vec![agg(AggFunc::CountStar, vec![], i8())])
        .fresh_copy();
    let match_keys = &matches.columns[..keys.len()];
    let match_value = matches.columns[keys.len()];
    let match_count = matches.columns[keys.len() + 1];
    let totals = right.aggregate(keys.to_vec(), vec![agg(AggFunc::CountStar, vec![], i8()), agg(AggFunc::Count, vec![value], i8())]);
    let total_count = totals.columns[keys.len()];
    let nonnull_count = totals.columns[keys.len() + 1];
    let left = left.map(vec![lhs]);
    let x = *left.columns.last().unwrap();
    let mut on = d_condition(dkeys, match_keys);
    on.push(sql_eq(col(x), &lt, col(match_value), &rt)?);
    let joined = left.join(matches, JoinKind::Left, on).join(totals, JoinKind::Left, d_condition(dkeys, keys));
    let total = Expr::Coalesce(vec![col(total_count), zero()], SqlType::Int8);
    let nonnull = Expr::Coalesce(vec![col(nonnull_count), zero()], SqlType::Int8);
    let has_null = Expr::Not(Box::new(sql_eq(total.clone(), &SqlType::Int8, nonnull, &SqlType::Int8)?));
    let nonempty = Expr::Not(Box::new(sql_eq(total, &SqlType::Int8, zero(), &SqlType::Int8)?));
    let mark = Expr::Case(Box::new(crate::expr::CaseExpr {
        whens: vec![
            (
                Expr::IsNull(Box::new(col(x))),
                Expr::Case(Box::new(crate::expr::CaseExpr {
                    whens: vec![(nonempty, Expr::null(SqlType::Bool))],
                    else_: Expr::bool(false),
                    ty: SqlType::Bool,
                })),
            ),
            (Expr::IsNotNull(Box::new(col(match_count))), Expr::bool(true)),
            (has_null, Expr::null(SqlType::Bool)),
        ],
        else_: Expr::bool(false),
        ty: SqlType::Bool,
    }));
    let mapped = joined.map(vec![mark]);
    let mark = *mapped.columns.last().unwrap();
    Ok(mapped.project(output.into_iter().chain([mark]).collect()))
}

fn quantified_join(
    left: L,
    right: L,
    dkeys: &[Expr],
    keys: &[ColumnId],
    lhs: Vec<Expr>,
    all: bool,
    compare: &dyn Fn(&[Expr], &[Expr]) -> Result<Expr>,
) -> Result<L> {
    let output = left.columns.clone();
    let keyexprs: Vec<_> = dkeys.iter().cloned().chain(lhs).collect();
    let domain = left.clone().map(keyexprs.clone());
    let dc = domain.columns[left.arity()..].to_vec();
    let domain = domain.project(dc).distinct().fresh_copy();
    let dc = domain.columns.clone();
    let on = dc.iter().zip(keys).map(|(a, b)| nd_eq(col(*a), col(*b))).collect();
    let rcols: Vec<_> = right.columns[keys.len()..].iter().map(|c| col(*c)).collect();
    let right = right.map(vec![Expr::bool(true)]);
    let present = Expr::IsNotNull(Box::new(col(*right.columns.last().unwrap())));
    let cmp = compare(&dc[dkeys.len()..].iter().map(|c| col(*c)).collect::<Vec<_>>(), &rcols)?;
    let test = |t| Expr::And(vec![present.clone(), Expr::BoolTest(Box::new(cmp.clone()), t)]);
    let mapped = domain.join(right, JoinKind::Left, on).map(vec![
        test(crate::expr::BoolTest::IsTrue),
        test(crate::expr::BoolTest::IsUnknown),
        test(crate::expr::BoolTest::IsFalse),
    ]);
    let tests = &mapped.columns[mapped.arity() - 3..];
    let b = ColumnType::new(SqlType::Bool);
    let calls = tests.iter().map(|c| agg(AggFunc::BoolOr, vec![*c], b.clone())).collect();
    let grouped = mapped.aggregate(dc.clone(), calls);
    let flags = &grouped.columns[dc.len()..];
    let (any_true, any_null, any_false) = (col(flags[0]), col(flags[1]), col(flags[2]));
    let mark = Expr::Case(Box::new(crate::expr::CaseExpr {
        whens: if all {
            vec![(any_false, Expr::bool(false)), (any_null, Expr::null(SqlType::Bool))]
        } else {
            vec![(any_true, Expr::bool(true)), (any_null, Expr::null(SqlType::Bool))]
        },
        else_: Expr::bool(all),
        ty: SqlType::Bool,
    }));
    let grouped = grouped.map(vec![mark]);
    let mark = *grouped.columns.last().unwrap();
    let mapped = left.map(keyexprs);
    let lc = &mapped.columns[output.len()..];
    let on = lc.iter().zip(&dc).map(|(a, b)| nd_eq(col(*a), col(*b))).collect();
    Ok(mapped.join(grouped, JoinKind::Inner, on).project(output.into_iter().chain([mark]).collect()))
}

fn rw(e: &Expr, keys: &[ColumnId], positions: &BTreeMap<ColumnId, usize>) -> Expr {
    e.rewritten(&mut |e| match e {
        Expr::LogicalOuter(1, c, _) => Some(col(keys[positions[c]])),
        _ => None,
    })
}

/// Returns the dependent relation and the bindings carrying its domain.
/// Local references do not change when those bindings are introduced.
fn push_dep(rel: L, domain: &L, positions: &BTreeMap<ColumnId, usize>) -> Result<(L, Vec<ColumnId>)> {
    if !rel.has_outer_refs(1) {
        let d = domain.fresh_copy();
        let keys = d.columns.clone();
        return Ok((d.join(rel, JoinKind::Inner, vec![]), keys));
    }
    let original = rel.columns;
    Ok(match rel.kind {
        K::Filter { input, preds } => {
            let (input, keys) = push_dep(*input, domain, positions)?;
            let preds = preds.iter().map(|e| rw(e, &keys, positions)).collect();
            (input.filter(preds), keys)
        }
        K::Map { input, exprs } => {
            let ids = original[input.arity()..].to_vec();
            let (input, keys) = push_dep(*input, domain, positions)?;
            let exprs = exprs.iter().map(|e| rw(e, &keys, positions)).collect();
            (input.map_with(ids, exprs), keys)
        }
        K::FlatMap { input, mut funcs, out, ordinality } => {
            let ids = original[input.arity()..].to_vec();
            let (input, keys) = push_dep(*input, domain, positions)?;
            for f in &mut funcs {
                f.args = f.args.iter().map(|e| rw(e, &keys, positions)).collect();
            }
            let columns = input.columns.iter().copied().chain(ids).collect();
            (L::new(K::FlatMap { input: Box::new(input), funcs, out, ordinality }, columns), keys)
        }
        K::Project { input, .. } => {
            let (input, keys) = push_dep(*input, domain, positions)?;
            (input.project(keys.iter().copied().chain(original).collect()), keys)
        }
        K::Distinct { input } => {
            let (input, keys) = push_dep(*input, domain, positions)?;
            (input.distinct(), keys)
        }
        K::Negate { input } => {
            let (input, keys) = push_dep(*input, domain, positions)?;
            let cols = input.columns.clone();
            (L::new(K::Negate { input: Box::new(input) }, cols), keys)
        }
        K::UnionAll { inputs } => {
            let branches = inputs.into_iter().map(|i| push_dep(i, domain, positions)).collect::<Result<Vec<_>>>()?;
            let keys: Vec<_> = (0..domain.arity()).map(|_| ColumnId::fresh()).collect();
            let columns = keys.iter().copied().chain(original).collect();
            (L::new(K::UnionAll { inputs: branches.into_iter().map(|b| b.0).collect() }, columns), keys)
        }
        K::SetOp { op, all, left, right } => {
            let (left, _) = push_dep(*left, domain, positions)?;
            let (right, _) = push_dep(*right, domain, positions)?;
            let keys: Vec<_> = (0..domain.arity()).map(|_| ColumnId::fresh()).collect();
            let columns = keys.iter().copied().chain(original).collect();
            (L::new(K::SetOp { op, all, left: Box::new(left), right: Box::new(right) }, columns), keys)
        }
        K::Join { left, right, kind, on } => {
            let lc = left.has_outer_refs(1);
            let rc = right.has_outer_refs(1);
            let both = match kind {
                JoinKind::Inner | JoinKind::Semi | JoinKind::Anti => {
                    lc && rc || (!lc && rc && matches!(kind, JoinKind::Semi | JoinKind::Anti))
                }
                JoinKind::Left => rc,
                JoinKind::Right => lc,
                JoinKind::Full => true,
            };
            if !both && lc {
                let (left, keys) = push_dep(*left, domain, positions)?;
                let on = on.iter().map(|e| rw(e, &keys, positions)).collect();
                (left.join(*right, kind, on), keys)
            } else if !both && rc && kind == JoinKind::Inner {
                let (right, keys) = push_dep(*right, domain, positions)?;
                let on = on.iter().map(|e| rw(e, &keys, positions)).collect();
                (left.join(right, kind, on).project(keys.iter().copied().chain(original).collect()), keys)
            } else {
                let (left, lk) = push_dep(*left, domain, positions)?;
                let (right, rk) = push_dep(*right, domain, positions)?;
                let mut on: Vec<_> = on.iter().map(|e| rw(e, &lk, positions)).collect();
                on.extend(lk.iter().zip(&rk).map(|(l, r)| nd_eq(col(*l), col(*r))));
                let joined = left.join(right, kind, on);
                if matches!(kind, JoinKind::Right | JoinKind::Full) {
                    let exprs =
                        lk.iter().zip(&rk).zip(domain.schema()).map(|((l, r), t)| Expr::Coalesce(vec![col(*l), col(*r)], t.ty)).collect();
                    let mapped = joined.map(exprs);
                    let keys = mapped.columns[mapped.arity() - lk.len()..].to_vec();
                    (mapped.project(keys.iter().copied().chain(original).collect()), keys)
                } else {
                    (joined.project(lk.iter().copied().chain(original).collect()), lk)
                }
            }
        }
        K::Aggregate { input, group, aggs } => {
            let scalar = group.is_empty();
            let (input, keys) = push_dep(*input, domain, positions)?;
            let group: Vec<_> = keys.iter().copied().chain(group).collect();
            if !scalar {
                let cols = keys.iter().copied().chain(original).collect();
                (L::new(K::Aggregate { input: Box::new(input), group, aggs }, cols), keys)
            } else {
                // Preserve a row for domain values whose input was empty.
                let empty_zero: Vec<_> = aggs.iter().map(|a| a.func.empty_is_zero()).collect();
                let grouped = input.aggregate(group, aggs);
                let values = grouped.columns[keys.len()..].to_vec();
                let d = domain.fresh_copy();
                let dk = d.columns.clone();
                let on = dk.iter().zip(&keys).map(|(a, b)| nd_eq(col(*a), col(*b))).collect();
                let joined = d.join(grouped, JoinKind::Left, on);
                let exprs = values
                    .iter()
                    .zip(empty_zero)
                    .map(|(c, zero_empty)| if zero_empty { Expr::Coalesce(vec![col(*c), zero()], SqlType::Int8) } else { col(*c) })
                    .collect();
                let out = joined.map_with(original.clone(), exprs).project(dk.iter().copied().chain(original).collect());
                (out, dk)
            }
        }
        K::TopK { input, group, order, limit, offset, with_ties } => {
            let (input, keys) = push_dep(*input, domain, positions)?;
            let group = keys.iter().copied().chain(group).collect();
            let cols = input.columns.clone();
            (L::new(K::TopK { input: Box::new(input), group, order, limit, offset, with_ties }, cols), keys)
        }
        K::Window { input, mut funcs } => {
            let ids = original[input.arity()..].to_vec();
            let (input, keys) = push_dep(*input, domain, positions)?;
            for f in &mut funcs {
                f.partition.splice(..0, keys.iter().copied());
            }
            let columns = input.columns.iter().copied().chain(ids).collect();
            (L::new(K::Window { input: Box::new(input), funcs }, columns), keys)
        }
        K::Values { rows, columns } => {
            let mut branches = Vec::new();
            for row in rows {
                let d = domain.fresh_copy();
                let keys = d.columns.clone();
                let exprs = row.iter().map(|e| rw(e, &keys, positions)).collect();
                branches.push(d.map_with(original.clone(), exprs));
            }
            if branches.is_empty() {
                let mut types = domain.schema();
                types.extend(columns);
                let empty = L::values(vec![], types);
                let keys = empty.columns[..domain.arity()].to_vec();
                (bind_output(empty, keys.iter().copied().chain(original).collect()), keys)
            } else {
                let keys = branches[0].columns[..domain.arity()].to_vec();
                let cols = branches[0].columns.clone();
                let out = if branches.len() == 1 { branches.pop().unwrap() } else { L::new(K::UnionAll { inputs: branches }, cols) };
                (out, keys)
            }
        }
        K::Recursive { .. } | K::RecRef { .. } => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "correlated recursive queries are not supported"),
        K::Get { .. } | K::Apply { .. } => {
            return Err(SqlError::internal("unexpected correlated operator"));
        }
    })
}

/// Lift correlated filters while keeping their referenced values visible.
/// Projection changes visibility, not the identities in lifted predicates.
fn lift_filters(rel: &L) -> Option<(L, Vec<Expr>)> {
    if !rel.has_outer_refs(1) {
        return Some((rel.clone(), vec![]));
    }
    match &rel.kind {
        K::Filter { input, preds } => {
            let (corr, plain): (Vec<_>, Vec<_>) = preds.iter().cloned().partition(Expr::has_outer);
            let (inner, mut lifted) = lift_filters(input)?;
            lifted.extend(corr);
            Some((inner.filter(plain), lifted))
        }
        K::Map { input, exprs } => {
            if exprs.iter().any(Expr::has_outer) {
                return None;
            }
            let (inner, lifted) = lift_filters(input)?;
            let ids = rel.columns[input.arity()..].to_vec();
            Some((inner.map_with(ids, exprs.clone()), lifted))
        }
        K::Project { input, .. } => {
            let (inner, lifted) = lift_filters(input)?;
            let mut selected = rel.columns.clone();
            for c in lifted.iter().flat_map(columns) {
                if !selected.contains(&c) {
                    selected.push(c);
                }
            }
            Some((inner.project(selected), lifted))
        }
        _ => None,
    }
}
fn to_join_cond(e: &Expr) -> Expr {
    e.rewritten(&mut |e| match e {
        Expr::LogicalOuter(1, c, _) => Some(col(*c)),
        Expr::LogicalOuter(d, c, t) => Some(Expr::LogicalOuter(d - 1, *c, t.clone())),
        _ => None,
    })
}
fn try_simple(left: &L, right: &L, kind: &ApplyKind) -> Result<Option<L>> {
    match kind {
        ApplyKind::Semi { null_aware: false, .. } | ApplyKind::Lateral { left_outer: false } => {
            let Some((mut base, lifted)) = lift_filters(right) else {
                return Ok(None);
            };
            if base.has_outer_refs(1) || lifted.is_empty() {
                return Ok(None);
            }
            shift_outer_depth(&mut base, 1);
            let mut on: Vec<_> = lifted.iter().map(to_join_cond).collect();
            let kind = if let ApplyKind::Semi { lhs, negated, .. } = kind {
                for (l, c) in lhs.iter().zip(&right.columns) {
                    on.push(sql_eq(l.clone(), &left.type_of(l).ty, col(*c), &base.column_type(*c).ty)?);
                }
                if *negated { JoinKind::Anti } else { JoinKind::Semi }
            } else {
                JoinKind::Inner
            };
            let joined = left.clone().join(base, kind, on);
            Ok(Some(if kind == JoinKind::Inner {
                joined.project(left.columns.iter().chain(&right.columns).copied().collect())
            } else {
                joined
            }))
        }
        ApplyKind::Scalar => simple_scalar_agg(left, right),
        _ => Ok(None),
    }
}

fn simple_scalar_agg(left: &L, right: &L) -> Result<Option<L>> {
    let mut layers = Vec::new();
    let mut cur = right;
    while let K::Project { input, .. } | K::Map { input, .. } = &cur.kind {
        if cur.exprs().iter().any(|e| e.has_outer()) {
            return Ok(None);
        }
        layers.push(cur);
        cur = input;
    }
    let K::Aggregate { input, group, aggs } = &cur.kind else {
        return Ok(None);
    };
    if !group.is_empty() || right.arity() != 1 {
        return Ok(None);
    }
    let Some((mut base, lifted)) = lift_filters(input) else {
        return Ok(None);
    };
    if base.has_outer_refs(1) || lifted.is_empty() {
        return Ok(None);
    }
    let mut keys = Vec::new();
    let mut equalities = Vec::new();
    let outer_only = |e: &Expr| e.has_outer() && columns(e).is_empty();
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
        equalities.push((c, k));
    }
    shift_outer_depth(&mut base, 1);
    let old_width = base.arity();
    let mapped = base.map(keys);
    let keys = mapped.columns[old_width..].to_vec();
    let grouped = mapped.aggregate(keys.clone(), aggs.clone());
    let agg_ids = grouped.columns[keys.len()..].to_vec();
    let mut on = Vec::new();
    for ((call, k), key) in equalities.iter().zip(&keys) {
        let mut args = vec![col(*key), to_join_cond(&call.args[1 - k])];
        if *k == 1 {
            args.reverse();
        }
        on.push(Expr::Call(Box::new(crate::expr::Call { func: call.func, args, ret: call.ret.clone() })));
    }
    let joined = left.clone().join(grouped, JoinKind::Left, on);
    let vals: Vec<_> = agg_ids
        .iter()
        .zip(aggs)
        .map(|(c, a)| if a.func.empty_is_zero() { Expr::Coalesce(vec![col(*c), zero()], SqlType::Int8) } else { col(*c) })
        .collect();
    let replacements: BTreeMap<_, _> = cur.columns.iter().copied().zip(vals.clone()).collect();
    let mut value = Some(col(right.columns[0]));
    for layer in &layers {
        if let K::Map { input, exprs } = &layer.kind {
            value = value.and_then(|v| {
                let inliner = super::optimize::Inliner::new(&layer.columns[input.arity()..], exprs);
                if inliner.stable(&v) { inliner.inline(&v) } else { None }
            });
        }
    }
    let (rel, value) = if let Some(value) = value {
        let value = value.rewritten(&mut |e| match e {
            Expr::LogicalColumn(c) => replacements.get(c).cloned(),
            _ => None,
        });
        (joined, value)
    } else {
        let mut rel = joined.map(vals);
        let added = &rel.columns[rel.arity() - cur.arity()..];
        let mut aliases: BTreeMap<_, _> = cur.columns.iter().copied().zip(added.iter().copied()).collect();
        for layer in layers.iter().rev() {
            if let K::Map { input, exprs } = &layer.kind {
                let old_ids = &layer.columns[input.arity()..];
                let new_ids: Vec<_> = old_ids.iter().map(|_| ColumnId::fresh()).collect();
                aliases.extend(old_ids.iter().copied().zip(new_ids.iter().copied()));
                let exprs = exprs.iter().map(|e| rename(e, &|c| aliases.get(&c).copied().unwrap_or(c))).collect();
                rel = rel.map_with(new_ids, exprs);
            }
        }
        (rel, col(*aliases.get(&right.columns[0]).unwrap_or(&right.columns[0])))
    };
    let mapped = rel.map(vec![value, one()]);
    let result = mapped.columns[mapped.arity() - 2..].to_vec();
    Ok(Some(mapped.project(left.columns.iter().copied().chain(result).collect())))
}
