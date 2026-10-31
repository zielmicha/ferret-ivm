//! Logical rewrites over stable bindings. Moving an operator or pruning its
//! inputs never changes the identities referenced by expressions.

use super::logical::{self, ColumnId, LogicalKind as K, LogicalRel as Rel};
use super::plan::{AggCall, INLINE_COPIES, JoinKind, WindowCall};
use crate::error::Result;
use crate::expr::Expr;
use crate::expr::func::func_def;
use std::collections::{BTreeMap, BTreeSet};

type Columns = BTreeSet<ColumnId>;

pub fn optimize(rel: Rel) -> Result<Rel> {
    let rel = fold_all(rel)?;
    let rel = push(rel, vec![])?;
    merge_projects(prune_all(rel))
}

fn fold_all(rel: Rel) -> Result<Rel> {
    let mut rel = rel.map_inputs(fold_all)?;
    for e in rel.exprs_mut() {
        *e = std::mem::replace(e, Expr::bool(true)).fold()?;
    }
    Ok(rel)
}

fn within(e: &Expr, columns: &Columns) -> bool {
    logical::columns(e).is_subset(columns)
}

/// Whether null-extending these bindings forces an expression to NULL.
fn null_under(e: &Expr, columns: &Columns) -> bool {
    match e {
        Expr::LogicalColumn(c) => columns.contains(c),
        Expr::Call(c) => func_def(c.func).strict && c.args.iter().any(|a| null_under(a, columns)),
        Expr::Cast(x, ..) | Expr::Not(x) => null_under(x, columns),
        Expr::Like(l) => null_under(&l.expr, columns) || null_under(&l.pattern, columns),
        Expr::InList(x, _, _) => null_under(x, columns),
        Expr::And(v) | Expr::Or(v) => v.iter().all(|x| null_under(x, columns)),
        // A NULL left operand still gives false/true for an empty ANY/ALL array.
        Expr::AnyAll { array, .. } => null_under(array, columns),
        _ => false,
    }
}

fn rejects_nulls(p: &Expr, columns: &Columns) -> bool {
    match p {
        Expr::IsNotNull(x) => null_under(x, columns),
        Expr::And(v) => v.iter().any(|x| rejects_nulls(x, columns)),
        Expr::Or(v) => v.iter().all(|x| rejects_nulls(x, columns)),
        Expr::BoolTest(x, crate::expr::BoolTest::IsTrue) => null_under(x, columns),
        other => null_under(other, columns),
    }
}

/// Set-operation branches bind their corresponding outputs independently.
fn branch_preds(preds: &[Expr], from: &[ColumnId], to: &[ColumnId]) -> Vec<Expr> {
    let bindings: BTreeMap<_, _> = from.iter().copied().zip(to.iter().copied()).collect();
    preds.iter().map(|p| logical::rename(p, &|c| bindings[&c])).collect()
}

fn push(rel: Rel, preds: Vec<Expr>) -> Result<Rel> {
    let Rel { kind, columns } = rel;
    Ok(match kind {
        K::Filter { input, preds: own } => push(*input, preds.into_iter().chain(own).collect())?,
        K::Map { input, exprs } => {
            let input_columns = input.column_set();
            let ids = columns[input.arity()..].to_vec();
            let inliner = Inliner::new(&ids, &exprs);
            let (mut down, mut up, mut at) = (vec![], vec![], vec![]);
            for p in preds {
                if within(&p, &input_columns) {
                    down.push(p);
                } else if !inliner.stable(&p) {
                    up.push(p);
                } else if let Some(q) = inliner.inline(&p) {
                    down.push(q);
                } else {
                    at.push(p);
                }
            }
            let input = push(*input, down)?;
            let r = if at.is_empty() { input.map_with(ids, exprs) } else { split_map(input, ids, exprs, at) };
            r.filter(up)
        }
        K::Project { input, .. } => push(*input, preds)?.project(columns),
        K::Join { left, right, kind, on } => push_join(*left, *right, kind, on, preds)?,
        K::Aggregate { input, group, aggs } => {
            let group_columns = group.iter().copied().collect();
            // A scalar aggregate yields a row for empty input, so even a
            // constant HAVING false must stay above it.
            let (down, up) = preds.into_iter().partition(|p| !group.is_empty() && within(p, &group_columns));
            Rel::new(K::Aggregate { input: Box::new(push(*input, down)?), group, aggs }, columns).filter(up)
        }
        K::Distinct { input } => push(*input, preds)?.distinct(),
        K::Negate { input } => Rel::new(K::Negate { input: Box::new(push(*input, preds)?) }, columns),
        K::UnionAll { inputs } => {
            let inputs = inputs
                .into_iter()
                .map(|i| {
                    let p = branch_preds(&preds, &columns, &i.columns);
                    push(i, p)
                })
                .collect::<Result<_>>()?;
            Rel::new(K::UnionAll { inputs }, columns)
        }
        K::SetOp { op, all, left, right } => {
            let lp = branch_preds(&preds, &columns, &left.columns);
            let rp = branch_preds(&preds, &columns, &right.columns);
            Rel::new(K::SetOp { op, all, left: Box::new(push(*left, lp)?), right: Box::new(push(*right, rp)?) }, columns)
        }
        K::TopK { input, group, order, limit, offset, with_ties } => {
            let group_columns = group.iter().copied().collect();
            let (down, up) = preds.into_iter().partition(|p| !logical::columns(p).is_empty() && within(p, &group_columns));
            Rel::new(K::TopK { input: Box::new(push(*input, down)?), group, order, limit, offset, with_ties }, columns).filter(up)
        }
        K::Window { input, funcs } => {
            let common = input.columns.iter().copied().filter(|c| funcs.iter().all(|f| f.partition.contains(c))).collect();
            let (down, up) = preds.into_iter().partition(|p| !logical::columns(p).is_empty() && within(p, &common));
            Rel::new(K::Window { input: Box::new(push(*input, down)?), funcs }, columns).filter(up)
        }
        K::FlatMap { input, funcs, out, ordinality } => {
            let input_columns = input.column_set();
            let (down, up) = preds.into_iter().partition(|p| within(p, &input_columns));
            Rel::new(K::FlatMap { input: Box::new(push(*input, down)?), funcs, out, ordinality }, columns).filter(up)
        }
        kind => Rel::new(kind, columns).map_inputs(|i| push(i, vec![]))?.filter(preds),
    })
}

/// Evaluate the columns required by predicates first, and the rest only
/// after those predicates pass. Stable bindings need no offset repair.
fn split_map(input: Rel, ids: Vec<ColumnId>, exprs: Vec<Expr>, preds: Vec<Expr>) -> Rel {
    let mut need: Columns = preds.iter().flat_map(logical::columns).collect();
    for (id, expr) in ids.iter().zip(&exprs).rev() {
        if need.contains(id) {
            need.extend(logical::columns(expr));
        }
    }
    let original = input.columns.iter().copied().chain(ids.iter().copied()).collect();
    let (mut first_ids, mut first, mut rest_ids, mut rest) = (vec![], vec![], vec![], vec![]);
    for (id, expr) in ids.into_iter().zip(exprs) {
        if need.contains(&id) {
            first_ids.push(id);
            first.push(expr);
        } else {
            rest_ids.push(id);
            rest.push(expr);
        }
    }
    input.map_with(first_ids, first).filter(preds).map_with(rest_ids, rest).project(original)
}

fn push_join(left: Rel, right: Rel, mut kind: JoinKind, on: Vec<Expr>, preds: Vec<Expr>) -> Result<Rel> {
    let lc = left.column_set();
    let rc = right.column_set();
    if matches!(kind, JoinKind::Left | JoinKind::Full) && preds.iter().any(|p| rejects_nulls(p, &rc)) {
        kind = if kind == JoinKind::Left { JoinKind::Inner } else { JoinKind::Right };
    }
    if matches!(kind, JoinKind::Right | JoinKind::Full) && preds.iter().any(|p| rejects_nulls(p, &lc)) {
        kind = if kind == JoinKind::Right { JoinKind::Inner } else { JoinKind::Left };
    }
    let (mut lp, mut rp, mut new_on, mut up) = (vec![], vec![], vec![], vec![]);
    let predicates = if kind == JoinKind::Inner { [(on, true), (preds, false)] } else { [(preds, false), (on, true)] };
    for (p, is_on) in predicates.into_iter().flat_map(|(ps, is_on)| ps.into_iter().map(move |p| (p, is_on))) {
        let cs = logical::columns(&p);
        let only_left = !cs.is_empty() && cs.is_subset(&lc);
        let only_right = !cs.is_empty() && cs.is_subset(&rc);
        let move_left = match kind {
            JoinKind::Inner => only_left,
            JoinKind::Left => !is_on && only_left,
            JoinKind::Semi | JoinKind::Anti => !is_on || (kind == JoinKind::Semi && only_left),
            JoinKind::Right => is_on && only_left,
            JoinKind::Full => false,
        };
        let move_right = match kind {
            JoinKind::Inner => only_right,
            JoinKind::Left => is_on && only_right,
            JoinKind::Right => !is_on && only_right,
            JoinKind::Semi | JoinKind::Anti => is_on && only_right,
            JoinKind::Full => false,
        };
        if move_left {
            lp.push(p);
        } else if move_right {
            rp.push(p);
        } else if is_on || kind == JoinKind::Inner {
            new_on.push(p);
        } else {
            up.push(p);
        }
    }
    Ok(push(left, lp)?.join(push(right, rp)?, kind, new_on).filter(up))
}

fn merge_projects(rel: Rel) -> Result<Rel> {
    let Rel { kind, columns } = rel.map_inputs(merge_projects)?;
    Ok(match kind {
        K::Project { input, .. } => input.project(columns),
        K::Filter { input, preds } => input.filter(preds),
        kind => Rel::new(kind, columns),
    })
}

fn prune_all(rel: Rel) -> Rel {
    let columns = rel.columns.clone();
    prune(rel, &columns.iter().copied().collect()).project(columns)
}

/// Keep demanded bindings and dependencies; the surviving expressions and
/// aggregate/window arguments retain their identities unchanged.
fn prune(rel: Rel, demand: &Columns) -> Rel {
    let one;
    let demand = if demand.is_empty() && !rel.columns.is_empty() {
        one = BTreeSet::from([rel.columns[0]]);
        &one
    } else {
        demand
    };
    let Rel { kind, columns } = rel;
    match kind {
        K::Get { .. } | K::Values { .. } | K::RecRef { .. } => {
            let kept = columns.iter().copied().filter(|c| demand.contains(c)).collect();
            Rel::new(kind, columns).project(kept)
        }
        K::Map { input, exprs } => {
            let ids = &columns[input.arity()..];
            let mut need = demand.clone();
            for (id, e) in ids.iter().zip(&exprs).rev() {
                if need.contains(id) {
                    need.extend(logical::columns(e));
                }
            }
            let input_need = need.intersection(&input.column_set()).copied().collect();
            let (ids, exprs) = ids.iter().copied().zip(exprs).filter(|(id, _)| need.contains(id)).unzip();
            prune(*input, &input_need).map_with(ids, exprs)
        }
        K::Filter { input, preds } => {
            let mut need = demand.clone();
            need.extend(preds.iter().flat_map(logical::columns));
            prune(*input, &need).filter(preds)
        }
        K::Project { input, .. } => {
            let kept: Vec<_> = columns.into_iter().filter(|c| demand.contains(c)).collect();
            prune(*input, &kept.iter().copied().collect()).project(kept)
        }
        K::Join { left, right, kind, on } => {
            let mut need = demand.clone();
            need.extend(on.iter().flat_map(logical::columns));
            let ln = need.intersection(&left.column_set()).copied().collect();
            let rn = need.intersection(&right.column_set()).copied().collect();
            prune(*left, &ln).join(prune(*right, &rn), kind, on)
        }
        K::Aggregate { input, group, aggs } => {
            let mut need: Columns = group.iter().copied().collect();
            need.extend(aggs.iter().flat_map(AggCall::columns));
            Rel::new(K::Aggregate { input: Box::new(prune(*input, &need)), group, aggs }, columns)
        }
        K::TopK { input, group, order, limit, offset, with_ties } => {
            let mut need = demand.clone();
            need.extend(group.iter().copied());
            need.extend(order.iter().map(|k| k.col));
            let input = prune(*input, &need);
            let columns = input.columns.clone();
            Rel::new(K::TopK { input: Box::new(input), group, order, limit, offset, with_ties }, columns)
        }
        K::Window { input, funcs } => {
            let appended = columns[input.arity()..].to_vec();
            let mut need: Columns = demand.intersection(&input.column_set()).copied().collect();
            need.extend(funcs.iter().flat_map(WindowCall::columns));
            let input = prune(*input, &need);
            let columns = input.columns.iter().copied().chain(appended).collect();
            Rel::new(K::Window { input: Box::new(input), funcs }, columns)
        }
        K::FlatMap { input, funcs, out, ordinality } => {
            let appended = columns[input.arity()..].to_vec();
            let mut need: Columns = demand.intersection(&input.column_set()).copied().collect();
            need.extend(funcs.iter().flat_map(|f| &f.args).flat_map(logical::columns));
            let input = prune(*input, &need);
            let columns = input.columns.iter().copied().chain(appended).collect();
            Rel::new(K::FlatMap { input: Box::new(input), funcs, out, ordinality }, columns)
        }
        K::UnionAll { inputs } => {
            let kept: Vec<_> = columns.iter().enumerate().filter_map(|(i, c)| demand.contains(c).then_some(i)).collect();
            let inputs = inputs
                .into_iter()
                .map(|input| {
                    let selected: Vec<_> = kept.iter().map(|i| input.columns[*i]).collect();
                    prune(input, &selected.iter().copied().collect()).project(selected)
                })
                .collect();
            let columns = kept.into_iter().map(|i| columns[i]).collect();
            Rel::new(K::UnionAll { inputs }, columns)
        }
        K::Negate { input } => {
            let input = prune(*input, demand);
            let columns = input.columns.clone();
            Rel::new(K::Negate { input: Box::new(input) }, columns)
        }
        // Set multiplicities, recursive bindings and Apply depend on the
        // whole row. Prune their children without changing those outputs.
        kind => {
            let mut rel = Rel::new(kind, columns);
            for child in rel.inputs_mut() {
                *child = prune_all(std::mem::replace(child, Rel::values(vec![], vec![])));
            }
            rel
        }
    }
}

/// Bounded substitution of computed bindings. The size budget prevents
/// nested maps such as SELECT x+x from growing expressions exponentially.
pub(crate) struct Inliner<'a> {
    ids: &'a [ColumnId],
    exprs: &'a [Expr],
    slots: BTreeMap<ColumnId, usize>,
    sizes: Vec<u64>,
    stable: Vec<bool>,
}
impl<'a> Inliner<'a> {
    pub(crate) fn new(ids: &'a [ColumnId], exprs: &'a [Expr]) -> Self {
        let slots = ids.iter().copied().enumerate().map(|(i, c)| (c, i)).collect();
        let mut this = Self { ids, exprs, slots, sizes: vec![], stable: vec![] };
        for e in exprs {
            let stable = e.is_deterministic() && !e.contains_now() && !e.has_subquery() && !e.has_outer() && this.stable(e);
            this.sizes.push(this.inlined_size(e));
            this.stable.push(stable);
        }
        this
    }
    pub(crate) fn stable(&self, e: &Expr) -> bool {
        logical::columns(e).iter().all(|c| self.slots.get(c).is_none_or(|i| self.stable[*i]))
    }
    fn inlined_size(&self, e: &Expr) -> u64 {
        if let Expr::LogicalColumn(c) = e
            && let Some(i) = self.slots.get(c)
        {
            return self.sizes[*i];
        }
        e.children().iter().fold(1u64, |n, c| n.saturating_add(self.inlined_size(c)))
    }
    pub(crate) fn inline(&self, e: &Expr) -> Option<Expr> {
        let mut need = logical::columns(e);
        let mut bound = expr_size(e);
        for (id, expr) in self.ids.iter().zip(self.exprs).rev() {
            if need.contains(id) {
                need.extend(logical::columns(expr));
                bound = bound.saturating_add(INLINE_COPIES.saturating_mul(expr_size(expr)));
            }
        }
        if self.inlined_size(e) > bound {
            return None;
        }
        let mut out = e.clone();
        self.inline_into(&mut out);
        Some(out)
    }
    fn inline_into(&self, e: &mut Expr) {
        while let Expr::LogicalColumn(c) = e
            && let Some(i) = self.slots.get(c)
        {
            *e = self.exprs[*i].clone();
        }
        for child in e.children_mut() {
            self.inline_into(child);
        }
    }
}
fn expr_size(e: &Expr) -> u64 {
    e.children().iter().fold(1u64, |n, c| n.saturating_add(expr_size(c)))
}
