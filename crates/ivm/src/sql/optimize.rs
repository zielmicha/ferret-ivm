//! Logical rewrites: constant folding, predicate pushdown, outer-join
//! simplification, column pruning, projection merging.

use super::plan::*;
use crate::error::Result;
use crate::expr::func::func_def;
use crate::expr::Expr;
use crate::types::Datum;
use std::collections::BTreeSet;

pub fn optimize(rel: Rel) -> Result<Rel> {
    let rel = fold_all(rel)?;
    let rel = push(rel, vec![])?;
    let rel = prune_all(rel);
    let rel = merge_projects(rel);
    Ok(rel)
}

fn fold_all(mut rel: Rel) -> Result<Rel> {
    for i in rel.inputs_mut() {
        let t = std::mem::replace(i, Rel::unit());
        *i = fold_all(t)?;
    }
    for e in rel.exprs_mut() {
        let t = std::mem::replace(e, Expr::bool(true));
        *e = t.fold()?;
    }
    Ok(rel)
}

fn cols_of(e: &Expr) -> Vec<usize> {
    e.column_set()
}

fn all_below(e: &Expr, n: usize) -> bool {
    cols_of(e).iter().all(|c| *c < n)
}

fn all_at_or_above(e: &Expr, n: usize) -> bool {
    cols_of(e).iter().all(|c| *c >= n)
}

/// Does `e` evaluate to NULL whenever every column in [lo, hi) is NULL?
fn null_under(e: &Expr, lo: usize, hi: usize) -> bool {
    match e {
        Expr::Column(c) => *c >= lo && *c < hi,
        Expr::Call(c) => func_def(c.func).strict && c.args.iter().any(|a| null_under(a, lo, hi)),
        Expr::Cast(x, ..) | Expr::Not(x) => null_under(x, lo, hi),
        Expr::Like(l) => null_under(&l.expr, lo, hi) || null_under(&l.pattern, lo, hi),
        Expr::InList(x, _, _) => null_under(x, lo, hi),
        Expr::And(v) => v.iter().all(|x| null_under(x, lo, hi)),
        Expr::Or(v) => v.iter().all(|x| null_under(x, lo, hi)),
        // a NULL left operand still gives false (ANY) or true (ALL) when
        // the array is empty, so only a NULL array guarantees NULL
        Expr::AnyAll { array, .. } => null_under(array, lo, hi),
        _ => false,
    }
}

/// Is predicate `p` false or NULL whenever every column in [lo, hi) is NULL?
fn rejects_nulls(p: &Expr, lo: usize, hi: usize) -> bool {
    match p {
        Expr::IsNotNull(x) => null_under(x, lo, hi),
        Expr::And(v) => v.iter().any(|x| rejects_nulls(x, lo, hi)),
        Expr::Or(v) => v.iter().all(|x| rejects_nulls(x, lo, hi)),
        Expr::BoolTest(x, crate::expr::BoolTest::IsTrue) => null_under(x, lo, hi),
        other => null_under(other, lo, hi),
    }
}

fn finish(rel: Rel, preds: Vec<Expr>) -> Rel {
    rel.filter(preds)
}

/// Push `preds` (over rel's output) as far down as possible.
fn push(rel: Rel, preds: Vec<Expr>) -> Result<Rel> {
    // constant-false predicate: keep (can't drop the subtree's schema)
    match rel {
        Rel::Filter { input, preds: p } => {
            let mut all = preds;
            all.extend(p);
            push(*input, all)
        }
        Rel::Map { input, exprs } => {
            let n = input.arity();
            let (down, up): (Vec<Expr>, Vec<Expr>) = preds.into_iter().partition(|p| all_below(p, n));
            let input = push(*input, down)?;
            Ok(finish(Rel::Map { input: Box::new(input), exprs }, up))
        }
        Rel::Project { input, cols } => {
            let down: Vec<Expr> = preds.into_iter().map(|p| p.remapped(&|c| cols[c])).collect();
            let input = push(*input, down)?;
            Ok(Rel::Project { input: Box::new(input), cols })
        }
        Rel::Join { left, right, kind, on } => push_join(*left, *right, kind, on, preds),
        Rel::Aggregate { input, group, aggs } => {
            let ng = group.len();
            // without GROUP BY the aggregate yields a row even for empty
            // input, so even a constant predicate (HAVING false) must stay
            // above it
            let (down, up): (Vec<Expr>, Vec<Expr>) = preds.into_iter().partition(|p| ng > 0 && all_below(p, ng));
            let down: Vec<Expr> = down.into_iter().map(|p| p.remapped(&|c| group[c])).collect();
            let input = push(*input, down)?;
            Ok(finish(Rel::Aggregate { input: Box::new(input), group, aggs }, up))
        }
        Rel::Distinct { input } => {
            let input = push(*input, preds)?;
            Ok(Rel::Distinct { input: Box::new(input) })
        }
        Rel::UnionAll { inputs } => {
            let inputs = inputs.into_iter().map(|i| push(i, preds.clone())).collect::<Result<Vec<_>>>()?;
            Ok(Rel::UnionAll { inputs })
        }
        Rel::SetOp { op, all, left, right } => {
            let left = push(*left, preds.clone())?;
            let right = push(*right, preds)?;
            Ok(Rel::SetOp { op, all, left: Box::new(left), right: Box::new(right) })
        }
        Rel::TopK { input, group, order, limit, offset, with_ties } => {
            let (down, up): (Vec<Expr>, Vec<Expr>) =
                preds.into_iter().partition(|p| !cols_of(p).is_empty() && cols_of(p).iter().all(|c| group.contains(c)));
            let input = push(*input, down)?;
            Ok(finish(Rel::TopK { input: Box::new(input), group, order, limit, offset, with_ties }, up))
        }
        Rel::Window { input, funcs } => {
            let n = input.arity();
            // predicates over columns that partition every window function
            let common: Vec<usize> = (0..n).filter(|c| funcs.iter().all(|f| f.partition.contains(c))).collect();
            let (down, up): (Vec<Expr>, Vec<Expr>) = preds
                .into_iter()
                .partition(|p| !cols_of(p).is_empty() && cols_of(p).iter().all(|c| common.contains(c)));
            let input = push(*input, down)?;
            Ok(finish(Rel::Window { input: Box::new(input), funcs }, up))
        }
        Rel::FlatMap { input, funcs, out, ordinality } => {
            let n = input.arity();
            let (down, up): (Vec<Expr>, Vec<Expr>) = preds.into_iter().partition(|p| all_below(p, n));
            let input = push(*input, down)?;
            Ok(finish(Rel::FlatMap { input: Box::new(input), funcs, out, ordinality }, up))
        }
        Rel::Negate { input } => {
            let input = push(*input, preds)?;
            Ok(Rel::Negate { input: Box::new(input) })
        }
        Rel::Recursive { id, base, step, distinct, columns } => {
            let base = push(*base, vec![])?;
            let step = push(*step, vec![])?;
            Ok(finish(Rel::Recursive { id, base: Box::new(base), step: Box::new(step), distinct, columns }, preds))
        }
        other @ (Rel::Get { .. } | Rel::Values { .. } | Rel::RecRef { .. } | Rel::Apply { .. }) => {
            let other = match other {
                Rel::Apply { left, right, kind } => {
                    Rel::Apply { left: Box::new(push(*left, vec![])?), right: Box::new(push(*right, vec![])?), kind }
                }
                o => o,
            };
            Ok(finish(other, preds))
        }
    }
}

fn push_join(left: Rel, right: Rel, mut kind: JoinKind, on: Vec<Expr>, preds: Vec<Expr>) -> Result<Rel> {
    let nl = left.arity();
    let nr = right.arity();
    let total = nl + nr;
    // outer join simplification from null-rejecting WHERE predicates
    if matches!(kind, JoinKind::Left | JoinKind::Full) && preds.iter().any(|p| rejects_nulls(p, nl, total)) {
        kind = if kind == JoinKind::Left { JoinKind::Inner } else { JoinKind::Right };
    }
    if matches!(kind, JoinKind::Right | JoinKind::Full) && preds.iter().any(|p| rejects_nulls(p, 0, nl)) {
        kind = if kind == JoinKind::Right { JoinKind::Inner } else { JoinKind::Left };
    }
    let shift_r = |e: Expr| e.remapped(&|c| c - nl);
    let mut lp = Vec::new();
    let mut rp = Vec::new();
    let mut new_on = Vec::new();
    let mut up = Vec::new();
    match kind {
        JoinKind::Inner => {
            for p in on.into_iter().chain(preds) {
                let cs = cols_of(&p);
                if cs.is_empty() {
                    new_on.push(p);
                } else if all_below(&p, nl) {
                    lp.push(p);
                } else if all_at_or_above(&p, nl) {
                    rp.push(shift_r(p));
                } else {
                    new_on.push(p);
                }
            }
        }
        JoinKind::Left => {
            for p in preds {
                if all_below(&p, nl) && !cols_of(&p).is_empty() {
                    lp.push(p);
                } else {
                    up.push(p);
                }
            }
            for p in on {
                if all_at_or_above(&p, nl) && !cols_of(&p).is_empty() {
                    rp.push(shift_r(p));
                } else {
                    new_on.push(p);
                }
            }
        }
        JoinKind::Right => {
            for p in preds {
                if all_at_or_above(&p, nl) && !cols_of(&p).is_empty() {
                    rp.push(shift_r(p));
                } else {
                    up.push(p);
                }
            }
            for p in on {
                if all_below(&p, nl) && !cols_of(&p).is_empty() {
                    lp.push(p);
                } else {
                    new_on.push(p);
                }
            }
        }
        JoinKind::Full => {
            up = preds;
            new_on = on;
        }
        JoinKind::Semi | JoinKind::Anti => {
            // output = left columns only
            for p in preds {
                lp.push(p);
            }
            for p in on {
                let cs = cols_of(&p);
                if !cs.is_empty() && all_at_or_above(&p, nl) {
                    rp.push(shift_r(p));
                } else if kind == JoinKind::Semi && !cs.is_empty() && all_below(&p, nl) {
                    lp.push(p);
                } else {
                    new_on.push(p);
                }
            }
        }
    }
    let left = push(left, lp)?;
    let right = push(right, rp)?;
    Ok(finish(Rel::Join { left: Box::new(left), right: Box::new(right), kind, on: new_on }, up))
}

/// Merge stacked projections and drop identity projections.
fn merge_projects(mut rel: Rel) -> Rel {
    for i in rel.inputs_mut() {
        let t = std::mem::replace(i, Rel::unit());
        *i = merge_projects(t);
    }
    match rel {
        Rel::Project { input, cols } => {
            // columns copied by a Map are columns of its input
            let (input, cols) = match *input {
                Rel::Map { input: inner, exprs } => {
                    let n = inner.arity();
                    let resolve = |c: usize| -> Option<usize> {
                        if c < n {
                            return Some(c);
                        }
                        match &exprs[c - n] {
                            Expr::Column(k) if *k < n => Some(*k),
                            _ => None,
                        }
                    };
                    match cols.iter().map(|c| resolve(*c)).collect::<Option<Vec<usize>>>() {
                        Some(direct) => (inner, direct),
                        None => (Box::new(Rel::Map { input: inner, exprs }), cols),
                    }
                }
                other => (Box::new(other), cols),
            };
            let arity = input.arity();
            if cols.len() == arity && cols.iter().enumerate().all(|(i, c)| i == *c) {
                return *input;
            }
            match *input {
                Rel::Project { input: inner, cols: c0 } => {
                    Rel::Project { input: inner, cols: cols.iter().map(|c| c0[*c]).collect() }
                }
                other => Rel::Project { input: Box::new(other), cols },
            }
        }
        Rel::Filter { input, preds } if preds.is_empty() => *input,
        Rel::Filter { input, preds } if preds.iter().all(|p| matches!(p, Expr::Literal(Datum::Bool(true), _))) => *input,
        other => other,
    }
}

// ---------------------------------------------------------------- pruning

/// Old output column -> new position (None: dropped).
type ColMap = Vec<Option<usize>>;

fn identity(n: usize) -> ColMap {
    (0..n).map(Some).collect()
}

fn map_from(arity: usize, kept: &[usize]) -> ColMap {
    let mut m = vec![None; arity];
    for (i, c) in kept.iter().enumerate() {
        m[*c] = Some(i);
    }
    m
}

fn remap(e: &Expr, m: &ColMap) -> Expr {
    e.remapped(&|c| m[c].expect("pruned column still referenced"))
}

/// Prune below `rel` while keeping all of its columns in order.
fn prune_all(rel: Rel) -> Rel {
    let n = rel.arity();
    let (r, m) = prune(rel, &(0..n).collect());
    let cols: Vec<usize> = m.iter().map(|c| c.expect("demanded column dropped")).collect();
    r.project(cols)
}

/// Keep only the demanded columns of a leaf relation.
fn restrict(rel: Rel, demand: &BTreeSet<usize>) -> (Rel, ColMap) {
    let n = rel.arity();
    if demand.len() == n {
        return (rel, identity(n));
    }
    let cols: Vec<usize> = demand.iter().copied().collect();
    let m = map_from(n, &cols);
    (rel.project(cols), m)
}

/// Column pruning: rewrite `rel` to compute only what its `demand`ed output
/// columns need, so that joins and arrangements carry fewer columns and
/// changes confined to unused columns cancel before they reach a join.
/// Returns the new relation and where each old column went. Columns beyond
/// the demand may survive (e.g. ones a filter needs).
fn prune(rel: Rel, demand: &BTreeSet<usize>) -> (Rel, ColMap) {
    let arity = rel.arity();
    // keep one column rather than produce zero-width intermediate rows
    let one: BTreeSet<usize>;
    let demand = if demand.is_empty() && arity > 0 {
        one = BTreeSet::from([0]);
        &one
    } else {
        demand
    };
    match rel {
        Rel::Get { .. } | Rel::Values { .. } | Rel::RecRef { .. } => restrict(rel, demand),
        Rel::Map { input, exprs } => {
            let n = input.arity();
            // appended expressions may reference earlier ones
            let mut need_expr = vec![false; exprs.len()];
            let mut need_in: BTreeSet<usize> = BTreeSet::new();
            for c in demand {
                if *c < n {
                    need_in.insert(*c);
                } else {
                    need_expr[c - n] = true;
                }
            }
            for i in (0..exprs.len()).rev() {
                if need_expr[i] {
                    for c in exprs[i].column_set() {
                        if c < n {
                            need_in.insert(c);
                        } else {
                            need_expr[c - n] = true;
                        }
                    }
                }
            }
            let (input, imap) = prune(*input, &need_in);
            let n2 = input.arity();
            let mut m: ColMap = imap;
            m.resize(arity, None);
            let mut kept = Vec::new();
            for (i, e) in exprs.into_iter().enumerate() {
                if need_expr[i] {
                    let e2 = remap(&e, &m);
                    m[n + i] = Some(n2 + kept.len());
                    kept.push(e2);
                }
            }
            (input.map(kept), m)
        }
        Rel::Filter { input, preds } => {
            let mut need = demand.clone();
            for p in &preds {
                need.extend(p.column_set());
            }
            let (input, m) = prune(*input, &need);
            let preds = preds.iter().map(|p| remap(p, &m)).collect();
            (input.filter(preds), m)
        }
        Rel::Project { input, cols } => {
            let need: BTreeSet<usize> = demand.iter().map(|d| cols[*d]).collect();
            let (input, imap) = prune(*input, &need);
            let kept: Vec<usize> = demand.iter().copied().collect();
            let new_cols = kept.iter().map(|d| imap[cols[*d]].expect("projected column dropped")).collect();
            (input.project(new_cols), map_from(arity, &kept))
        }
        Rel::Join { left, right, kind, on } => {
            let nl = left.arity();
            let (mut need_l, mut need_r) = (BTreeSet::new(), BTreeSet::new());
            for c in demand.iter().copied().chain(on.iter().flat_map(|p| p.column_set())) {
                if c < nl {
                    need_l.insert(c);
                } else {
                    need_r.insert(c - nl);
                }
            }
            let (left, lmap) = prune(*left, &need_l);
            let (right, rmap) = prune(*right, &need_r);
            let nl2 = left.arity();
            let jm: ColMap = lmap.iter().copied().chain(rmap.iter().map(|r| r.map(|r| nl2 + r))).collect();
            let on = on.iter().map(|p| remap(p, &jm)).collect();
            let m = jm[..arity].to_vec();
            (Rel::Join { left: Box::new(left), right: Box::new(right), kind, on }, m)
        }
        Rel::Aggregate { input, group, aggs } => {
            let mut need: BTreeSet<usize> = group.iter().copied().collect();
            for a in &aggs {
                need.extend(a.args.iter().copied());
                need.extend(a.filter);
                need.extend(a.order.iter().map(|k| k.col));
            }
            let (input, im) = prune(*input, &need);
            let at = |c: usize| im[c].expect("aggregate input dropped");
            let group = group.iter().map(|c| at(*c)).collect();
            let aggs = aggs
                .into_iter()
                .map(|mut a| {
                    a.args = a.args.iter().map(|c| at(*c)).collect();
                    a.filter = a.filter.map(at);
                    for k in &mut a.order {
                        k.col = at(k.col);
                    }
                    a
                })
                .collect();
            (Rel::Aggregate { input: Box::new(input), group, aggs }, identity(arity))
        }
        Rel::TopK { input, group, order, limit, offset, with_ties } => {
            let mut need = demand.clone();
            need.extend(group.iter().copied());
            need.extend(order.iter().map(|k| k.col));
            let (input, m) = prune(*input, &need);
            let at = |c: usize| m[c].expect("top-k column dropped");
            let group = group.iter().map(|c| at(*c)).collect();
            let order = order.iter().map(|k| SortKey { col: at(k.col), spec: k.spec }).collect();
            (Rel::TopK { input: Box::new(input), group, order, limit, offset, with_ties }, m)
        }
        Rel::Window { input, funcs } => {
            let n = input.arity();
            let mut need: BTreeSet<usize> = demand.iter().copied().filter(|c| *c < n).collect();
            for f in &funcs {
                need.extend(f.args.iter().copied());
                need.extend(f.partition.iter().copied());
                need.extend(f.order.iter().map(|k| k.col));
            }
            let (input, im) = prune(*input, &need);
            let n2 = input.arity();
            let at = |c: usize| im[c].expect("window input dropped");
            let funcs = funcs
                .into_iter()
                .map(|mut f| {
                    f.args = f.args.iter().map(|c| at(*c)).collect();
                    f.partition = f.partition.iter().map(|c| at(*c)).collect();
                    for k in &mut f.order {
                        k.col = at(k.col);
                    }
                    f
                })
                .collect();
            let m = (0..arity).map(|c| if c < n { im[c] } else { Some(n2 + c - n) }).collect();
            (Rel::Window { input: Box::new(input), funcs }, m)
        }
        Rel::FlatMap { input, mut funcs, out, ordinality } => {
            let n = input.arity();
            let mut need: BTreeSet<usize> = demand.iter().copied().filter(|c| *c < n).collect();
            for a in funcs.iter().flat_map(|f| f.args.iter()) {
                need.extend(a.column_set());
            }
            let (input, im) = prune(*input, &need);
            let n2 = input.arity();
            for f in funcs.iter_mut() {
                f.args = f.args.iter().map(|a| remap(a, &im)).collect();
            }
            let m = (0..arity).map(|c| if c < n { im[c] } else { Some(n2 + c - n) }).collect();
            (Rel::FlatMap { input: Box::new(input), funcs, out, ordinality }, m)
        }
        Rel::UnionAll { inputs } => {
            let cols: Vec<usize> = demand.iter().copied().collect();
            let inputs = inputs
                .into_iter()
                .map(|i| {
                    let (r, m) = prune(i, demand);
                    r.project(cols.iter().map(|c| m[*c].expect("union column dropped")).collect())
                })
                .collect();
            (Rel::UnionAll { inputs }, map_from(arity, &cols))
        }
        Rel::Negate { input } => {
            let (input, m) = prune(*input, demand);
            (Rel::Negate { input: Box::new(input) }, m)
        }
        // multiplicities depend on every column
        Rel::Distinct { input } => (Rel::Distinct { input: Box::new(prune_all(*input)) }, identity(arity)),
        Rel::SetOp { op, all, left, right } => (
            Rel::SetOp { op, all, left: Box::new(prune_all(*left)), right: Box::new(prune_all(*right)) },
            identity(arity),
        ),
        Rel::Recursive { id, base, step, distinct, columns } => (
            Rel::Recursive { id, base: Box::new(prune_all(*base)), step: Box::new(prune_all(*step)), distinct, columns },
            identity(arity),
        ),
        Rel::Apply { left, right, kind } => (
            Rel::Apply { left: Box::new(prune_all(*left)), right: Box::new(prune_all(*right)), kind },
            identity(arity),
        ),
    }
}
