//! Lowering logical plans to maintained dataflow.
//!
//! Inner joins become n-way delta joins over shared arrangements (lookups on
//! a base table's primary key reuse its primary index, so PK joins need no
//! extra state). Filters/maps/projections are kept as pending MFPs and fused
//! into the consumer. Semijoins use presence (count) lookups; antijoins,
//! outer joins and NOT EXISTS are built from semijoins.

use super::agg::AggSpec;
use super::join::{DeltaPath, JoinInput, JoinPlan, LookupStep};
use super::mfp::Mfp;
use super::ops::{RecursivePlan, TopKPlan};
use super::reduce::ReducePlan;
use super::window::{WinSpec, WindowPlan};
use super::*;
use crate::error::codes;
use crate::expr::func::func_def;
use crate::sql::plan::{AggCall, AggFunc, JoinKind, Rel, SetOpKind, SortKey};
use crate::sql_bail;
use crate::types::{Datum, SqlType};

/// A node plus a pending map/filter/project over its rows.
#[derive(Clone, Debug)]
pub struct Lowered {
    pub node: NodeId,
    pub mfp: Mfp,
    pub schema: Vec<ColumnType>,
}

pub struct Lowerer<'a> {
    pub g: &'a mut Graph,
    pub env: &'a mut Env,
    /// Catalog object id -> node of a table or materialized view.
    pub objects: &'a dyn Fn(u64) -> Option<NodeId>,
}

fn is_eq_call(e: &Expr) -> Option<(&Expr, &Expr, bool)> {
    match e {
        Expr::Call(c) if c.args.len() == 2 && func_def(c.func).name == "=" => Some((&c.args[0], &c.args[1], false)),
        Expr::IsDistinct(a, b, true) => Some((a, b, true)),
        _ => None,
    }
}

impl<'a> Lowerer<'a> {
    fn node_schema(&self, n: NodeId) -> Vec<ColumnType> {
        self.g.node(n).schema.clone()
    }

    fn base(&self, node: NodeId) -> Lowered {
        let schema = self.node_schema(node);
        Lowered { node, mfp: Mfp::identity(schema.len()), schema }
    }

    /// Turn a pending MFP into a node.
    pub fn materialize(&mut self, l: Lowered) -> NodeId {
        if l.mfp.is_identity() {
            return l.node;
        }
        let input_schema = self.node_schema(l.node);
        let schema = l.mfp.schema(&input_schema);
        self.g.add_node(NodeKind::Mfp { mfp: l.mfp }, vec![l.node], schema)
    }

    fn arrange(&mut self, node: NodeId, key: Vec<Expr>, specs: Vec<SortSpec>, summaries: Vec<(AggKind, Expr)>) -> ArrId {
        self.g.add_arrangement(self.env, node, key, specs, summaries)
    }

    pub fn lower(&mut self, rel: &Rel) -> Result<Lowered> {
        match rel {
            Rel::Get { id, name, .. } => {
                let node = (self.objects)(*id)
                    .ok_or_else(|| SqlError::new(codes::UNDEFINED_TABLE, format!("relation \"{name}\" has no dataflow")))?;
                Ok(self.base(node))
            }
            Rel::Values { rows, columns } => {
                let mut out = Vec::with_capacity(rows.len());
                for r in rows {
                    let mut row = Vec::with_capacity(r.len());
                    for e in r {
                        row.push(e.eval(&[])?);
                    }
                    out.push(row);
                }
                let node = self.g.add_node(NodeKind::Constant { rows: out }, vec![], columns.clone());
                Ok(self.base(node))
            }
            Rel::Map { input, exprs } => {
                let mut l = self.lower(input)?;
                for e in exprs {
                    let t = e.column_type(&l.schema);
                    l.mfp.add_map(e.clone());
                    l.schema.push(t);
                }
                Ok(l)
            }
            Rel::Filter { input, preds } => {
                let (regular, temporal) = super::temporal::split(preds)?;
                let mut l = self.lower(input)?;
                for p in regular {
                    l.mfp.add_filter(p);
                }
                if temporal.is_empty() {
                    return Ok(l);
                }
                // conditions over now(): rows enter and leave as time passes
                let node = self.materialize(l);
                let schema = self.node_schema(node);
                let state = super::temporal::event_schema(&schema);
                let kind = NodeKind::Temporal(super::temporal::TemporalPlan { preds: temporal, events: usize::MAX });
                let id = self.g.add_node_with_state_arr(
                    self.env,
                    kind,
                    vec![node],
                    schema,
                    state,
                    vec![Expr::Column(0), Expr::Column(1)],
                    vec![SortSpec::ASC; 2],
                    |k, a| {
                        if let NodeKind::Temporal(p) = k {
                            p.events = a;
                        }
                    },
                );
                Ok(self.base(id))
            }
            Rel::Project { input, cols } => {
                let mut l = self.lower(input)?;
                l.mfp.project(cols);
                l.schema = cols.iter().map(|c| l.schema[*c].clone()).collect();
                Ok(l)
            }
            Rel::Join { left, right, kind, on } => match kind {
                JoinKind::Inner => {
                    let mut inputs = Vec::new();
                    let mut conds = Vec::new();
                    self.flatten_inner(rel, &mut inputs, &mut conds, 0)?;
                    let _ = (left, right, on);
                    self.lower_inner(inputs, conds)
                }
                JoinKind::Semi | JoinKind::Anti => {
                    let l = self.lower(left)?;
                    let r = self.lower(right)?;
                    self.lower_semijoin(l, r, on, *kind == JoinKind::Anti)
                }
                JoinKind::Left | JoinKind::Right | JoinKind::Full => {
                    let l = self.lower(left)?;
                    let r = self.lower(right)?;
                    self.lower_outer(l, r, on, *kind)
                }
            },
            Rel::Aggregate { input, group, aggs } => self.lower_aggregate(input, group, aggs),
            Rel::Distinct { input } => {
                let l = self.lower(input)?;
                self.distinct(l)
            }
            Rel::SetOp { op, all, left, right } => {
                let l = self.lower(left)?;
                let r = self.lower(right)?;
                if *op == SetOpKind::Union {
                    let u = self.union(vec![l, r], vec![false, false])?;
                    return self.distinct(u);
                }
                let schema = l.schema.clone();
                let ln = self.materialize(l);
                let rn = self.materialize(r);
                let full_l = Layout::full(&schema);
                let la = self.arrange(ln, full_l.key.clone(), full_l.specs.clone(), vec![]);
                let ra = self.arrange(rn, full_l.key.clone(), full_l.specs.clone(), vec![]);
                let node = self.g.add_node(
                    NodeKind::SetOp { op: *op, all: *all, left_arr: la, right_arr: ra },
                    vec![ln, rn],
                    schema,
                );
                Ok(self.base(node))
            }
            Rel::UnionAll { inputs } => {
                let ls = inputs.iter().map(|i| self.lower(i)).collect::<Result<Vec<_>>>()?;
                let n = ls.len();
                self.union(ls, vec![false; n])
            }
            Rel::TopK { input, group, order, limit, offset, with_ties } => {
                let l = self.lower(input)?;
                let schema = l.schema.clone();
                let node = self.materialize(l);
                let mut key: Vec<Expr> = group.iter().map(|c| Expr::Column(*c)).collect();
                let mut specs = vec![SortSpec::ASC; group.len()];
                for k in order {
                    key.push(Expr::Column(k.col));
                    specs.push(k.spec);
                }
                let arr = self.arrange(node, key, specs, vec![]);
                let plan = TopKPlan { group_key: group.clone(), limit: *limit, offset: *offset, with_ties: *with_ties, input_arr: arr };
                let n = self.g.add_node(NodeKind::TopK(plan), vec![node], schema);
                Ok(self.base(n))
            }
            Rel::Window { input, funcs } => {
                let l = self.lower(input)?;
                let mut cur_schema = l.schema.clone();
                let mut node = self.materialize(l);
                let mut i = 0;
                while i < funcs.len() {
                    let (part, order) = (&funcs[i].partition, &funcs[i].order);
                    let mut j = i;
                    while j < funcs.len() && funcs[j].partition == *part && funcs[j].order == *order {
                        j += 1;
                    }
                    let mut key: Vec<Expr> = part.iter().map(|c| Expr::Column(*c)).collect();
                    let mut specs = vec![SortSpec::ASC; part.len()];
                    for k in order {
                        key.push(Expr::Column(k.col));
                        specs.push(k.spec);
                    }
                    let arr = self.arrange(node, key, specs, vec![]);
                    let specs_w: Vec<WinSpec> = funcs[i..j]
                        .iter()
                        .map(|f| WinSpec {
                            func: f.func,
                            args: f.args.clone(),
                            frame: f.frame,
                            arg_types: f.args.iter().map(|a| cur_schema[*a].ty.clone()).collect(),
                            ret: f.ret.clone(),
                            range: f.range.clone(),
                        })
                        .collect();
                    let mut schema = cur_schema.clone();
                    schema.extend(funcs[i..j].iter().map(|f| f.ret.clone()));
                    let plan = WindowPlan { partition: part.clone(), order: order.clone(), funcs: specs_w, input_arr: arr };
                    node = self.g.add_node(NodeKind::Window(plan), vec![node], schema.clone());
                    cur_schema = schema;
                    i = j;
                }
                Ok(self.base(node))
            }
            Rel::FlatMap { input, funcs, out, ordinality } => {
                let l = self.lower(input)?;
                let mut schema = l.schema.clone();
                // fold the pending MFP into the args by materializing first
                let node = self.materialize(l);
                schema.extend(out.iter().cloned());
                let n = self.g.add_node(NodeKind::FlatMap { funcs: funcs.clone(), ordinality: *ordinality }, vec![node], schema);
                Ok(self.base(n))
            }
            Rel::Negate { input } => {
                let l = self.lower(input)?;
                let schema = l.schema.clone();
                let node = self.materialize(l);
                let n = self.g.add_node(NodeKind::Negate, vec![node], schema);
                Ok(self.base(n))
            }
            Rel::Recursive { id, base, step, distinct, columns } => {
                let mut ids = Vec::new();
                collect_gets(base, &mut ids);
                collect_gets(step, &mut ids);
                ids.sort();
                ids.dedup();
                let mut sources = Vec::new();
                for oid in ids {
                    let node = (self.objects)(oid).ok_or_else(|| SqlError::internal("recursive source without dataflow"))?;
                    sources.push((oid, node));
                }
                // arrangements of the sources are needed for batch reads
                for (_, n) in &sources {
                    if self.g.node(*n).arrangements.is_empty() {
                        let s = self.node_schema(*n);
                        let full = Layout::full(&s);
                        let a = self.arrange(*n, full.key.clone(), full.specs.clone(), vec![]);
                        self.g.retain_arr(a);
                    }
                }
                let inputs: Vec<NodeId> = sources.iter().map(|(_, n)| *n).collect();
                let plan = RecursivePlan {
                    base: (**base).clone(),
                    step: (**step).clone(),
                    rec_id: *id,
                    distinct: *distinct,
                    sources,
                    out_arr: usize::MAX,
                    errors: Default::default(),
                };
                let full = Layout::full(columns);
                let node = self.g.add_node_with_output_arr(
                    self.env,
                    NodeKind::Recursive(plan),
                    inputs,
                    columns.clone(),
                    full.key.clone(),
                    full.specs.clone(),
                    |k, a| {
                        if let NodeKind::Recursive(p) = k {
                            p.out_arr = a;
                        }
                    },
                );
                Ok(self.base(node))
            }
            Rel::RecRef { .. } => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "recursive reference outside WITH RECURSIVE"),
            Rel::Apply { .. } => Err(SqlError::internal("correlated subquery survived decorrelation")),
        }
    }

    fn union(&mut self, ls: Vec<Lowered>, negate: Vec<bool>) -> Result<Lowered> {
        let schema = ls[0].schema.clone();
        let mut nodes = Vec::with_capacity(ls.len());
        for l in ls {
            nodes.push(self.materialize(l));
        }
        let node = self.g.add_node(NodeKind::Union { negate }, nodes, schema);
        Ok(self.base(node))
    }

    fn distinct(&mut self, l: Lowered) -> Result<Lowered> {
        let schema = l.schema.clone();
        let node = self.materialize(l);
        let full = Layout::full(&schema);
        let arr = self.arrange(node, full.key.clone(), full.specs.clone(), vec![]);
        let n = self.g.add_node(NodeKind::Distinct { arr }, vec![node], schema);
        Ok(self.base(n))
    }

    // ------------------------------------------------------------------ inner joins

    /// Collect the inputs and conditions of a tree of inner joins. Conditions
    /// are expressed over the concatenation of the inputs' outputs.
    fn flatten_inner(&mut self, rel: &Rel, inputs: &mut Vec<Lowered>, conds: &mut Vec<Expr>, offset: usize) -> Result<usize> {
        match rel {
            Rel::Join { left, right, kind: JoinKind::Inner, on } => {
                let nl = self.flatten_inner(left, inputs, conds, offset)?;
                let nr = self.flatten_inner(right, inputs, conds, offset + nl)?;
                for c in on {
                    conds.push(c.remapped(&|x| x + offset));
                }
                Ok(nl + nr)
            }
            Rel::Filter { input, preds }
                if matches!(**input, Rel::Join { kind: JoinKind::Inner, .. }) && !preds.iter().any(|p| p.contains_now()) =>
            {
                let n = self.flatten_inner(input, inputs, conds, offset)?;
                for p in preds {
                    conds.push(p.remapped(&|x| x + offset));
                }
                Ok(n)
            }
            other => {
                let l = self.lower(other)?;
                let w = l.schema.len();
                inputs.push(l);
                Ok(w)
            }
        }
    }

    /// Plan a delta join. `presence[i]` marks semijoin presence inputs
    /// (already materialized and arranged; see lower_semijoin).
    fn plan_join(&mut self, inputs: Vec<JoinInputSpec>, conds: Vec<Expr>) -> Result<(JoinPlan, Vec<NodeId>, Vec<ColumnType>)> {
        let n = inputs.len();
        let mut offsets = Vec::with_capacity(n);
        let mut width = 0;
        for i in &inputs {
            offsets.push(width);
            width += i.width;
        }
        let owner = |c: usize| -> usize { (0..n).rev().find(|i| offsets[*i] <= c).unwrap() };
        let inputs_of = |e: &Expr| -> Vec<usize> {
            let mut v: Vec<usize> = e.column_set().into_iter().map(owner).collect();
            v.dedup();
            v.sort();
            v.dedup();
            v
        };
        // equi edges: (i, expr_i over joined row, j, expr_j over joined row, null_safe)
        struct Edge {
            a: usize,
            ea: Expr,
            b: usize,
            eb: Expr,
            null_safe: bool,
        }
        let mut edges: Vec<Edge> = Vec::new();
        let mut residual: Vec<Expr> = Vec::new();
        for c in conds {
            if let Some((x, y, ns)) = is_eq_call(&c) {
                let (ix, iy) = (inputs_of(x), inputs_of(y));
                if ix.len() == 1 && iy.len() == 1 && ix[0] != iy[0] {
                    edges.push(Edge { a: ix[0], ea: x.clone(), b: iy[0], eb: y.clone(), null_safe: ns });
                    continue;
                }
            }
            residual.push(c);
        }
        let sizes: Vec<i64> = inputs
            .iter()
            .map(|s| {
                self.g.node(s.node).arrangements.first().map(|a| self.env.tree_count(self.g.arr(*a).tree)).unwrap_or(1_000_000)
            })
            .collect();
        // Arrangement keys are evaluated on every row of the arranged node.
        // A key that is not a plain column of an input's node (computed by
        // its MFP or by the join condition) would be evaluated on rows the
        // MFP filters out too, and a key that fails would abort the whole
        // step. Such inputs are materialized with their computed keys as
        // extra columns: their arrangements are then keyed by columns, and
        // expression errors become error records of the materialized node.
        let mut inputs = inputs;
        let mut extra_keys: Vec<Vec<Expr>> = vec![Vec::new(); n];
        for j in 0..n {
            if inputs[j].presence.is_some() {
                continue;
            }
            let defs: Vec<Expr> = (0..inputs[j].width).map(|c| inputs[j].mfp.output_expr(c)).collect();
            let mut computed = false;
            let mut extra: Vec<Expr> = Vec::new();
            for e in &edges {
                for (side, ex) in [(e.a, &e.ea), (e.b, &e.eb)] {
                    if side != j {
                        continue;
                    }
                    let local = ex.remapped(&|c| c - offsets[j]);
                    if local.substitute(&defs).as_column().is_some() {
                        continue;
                    }
                    computed = true;
                    if local.as_column().is_none() && !extra.iter().any(|x| same_expr(x, &local)) {
                        extra.push(local);
                    }
                }
            }
            if !computed {
                continue;
            }
            let s = &inputs[j];
            let (width, schema) = (s.width, s.schema.clone());
            let mut l = Lowered { node: s.node, mfp: s.mfp.clone(), schema: s.schema.clone() };
            for e in &extra {
                l.mfp.add_map(e.clone());
                l.schema.push(e.column_type(&schema));
            }
            let node = self.materialize(l);
            let mut mfp = Mfp::identity(width + extra.len());
            mfp.project(&(0..width).collect::<Vec<_>>());
            inputs[j] = JoinInputSpec { node, mfp, width, schema, presence: None, exact: false };
            extra_keys[j] = extra;
        }
        let mut join_inputs = Vec::with_capacity(n);
        let mut nodes = Vec::with_capacity(n);
        for (i, s) in inputs.iter().enumerate() {
            join_inputs.push(JoinInput {
                node: s.node,
                mfp: s.mfp.clone(),
                width: s.width,
                offset: offsets[i],
                presence: s.presence,
                exact: s.exact,
            });
            nodes.push(s.node);
        }
        // the joined row schema
        let mut schema = Vec::with_capacity(width);
        for s in &inputs {
            schema.extend(s.schema.iter().cloned());
        }
        let mut paths = Vec::with_capacity(n);
        for src in 0..n {
            let mut bound = vec![src];
            let mut steps = Vec::new();
            let mut pending: Vec<Expr> = residual.clone();
            let take_ready = |pending: &mut Vec<Expr>, bound: &[usize]| -> Vec<Expr> {
                let mut ready = Vec::new();
                pending.retain(|p| {
                    if inputs_of(p).iter().all(|i| bound.contains(i)) {
                        ready.push(p.clone());
                        false
                    } else {
                        true
                    }
                });
                ready
            };
            let source_filters = take_ready(&mut pending, &bound);
            while bound.len() < n {
                // choose the next input: most edges to bound inputs, unique keys first, then size
                let mut best: Option<(usize, (usize, bool, i64))> = None;
                for j in 0..n {
                    if bound.contains(&j) {
                        continue;
                    }
                    let cnt = edges
                        .iter()
                        .filter(|e| (e.a == j && bound.contains(&e.b)) || (e.b == j && bound.contains(&e.a)))
                        .count();
                    let jkeys: Vec<Expr> = edges
                        .iter()
                        .filter_map(|e| {
                            if e.a == j && bound.contains(&e.b) {
                                Some(e.ea.remapped(&|c| c - offsets[j]))
                            } else if e.b == j && bound.contains(&e.a) {
                                Some(e.eb.remapped(&|c| c - offsets[j]))
                            } else {
                                None
                            }
                        })
                        .collect();
                    let unique = inputs[j].presence.is_some() || self.covers_unique_key(&inputs[j], &jkeys);
                    let score = (cnt, unique, -sizes[j]);
                    if best.as_ref().is_none_or(|(_, s)| score > *s) {
                        best = Some((j, score));
                    }
                }
                let (j, _) = best.unwrap();
                // key columns for j: pairs (expr over j's raw rows, expr over joined row)
                let mut pairs: Vec<(Expr, Expr, bool)> = Vec::new();
                // keyed by the values of an exact presence input: match by identity
                let mut exact = false;
                for e in &edges {
                    let (je, oe, other) = if e.a == j { (&e.ea, &e.eb, e.b) } else if e.b == j { (&e.eb, &e.ea, e.a) } else { continue };
                    if !bound.contains(&other) {
                        continue;
                    }
                    exact |= inputs[other].exact;
                    // j-side expr over j's local post-mfp columns -> raw node columns
                    let local = je.remapped(&|c| c - offsets[j]);
                    let raw = if inputs[j].presence.is_some() {
                        local
                    } else if let Some(k) = extra_keys[j].iter().position(|x| same_expr(x, &local)) {
                        Expr::Column(inputs[j].width + k)
                    } else {
                        let defs: Vec<Expr> = (0..inputs[j].width).map(|c| inputs[j].mfp.output_expr(c)).collect();
                        local.substitute(&defs)
                    };
                    pairs.push((raw, oe.clone(), e.null_safe));
                }
                // deterministic key order: primary-key order when covered, else by fingerprint
                self.order_key_pairs(&inputs[j], &mut pairs);
                let arr = match inputs[j].presence {
                    Some(parr) => {
                        // presence arrangements are keyed by all presence columns in order
                        // reorder pairs to match its key
                        pairs.sort_by_key(|(raw, _, _)| raw.as_column().unwrap_or(usize::MAX));
                        if pairs.len() != inputs[j].width {
                            sql_bail!(codes::FEATURE_NOT_SUPPORTED, "semijoin keys must all be equality conditions");
                        }
                        parr
                    }
                    None => {
                        let key: Vec<Expr> = pairs.iter().map(|(r, _, _)| r.clone()).collect();
                        let specs = vec![SortSpec::ASC; key.len()];
                        self.arrange(inputs[j].node, key, specs, vec![])
                    }
                };
                bound.push(j);
                let filters = take_ready(&mut pending, &bound);
                steps.push(LookupStep {
                    input: j,
                    arr,
                    key: pairs.iter().map(|(_, o, _)| o.clone()).collect(),
                    null_safe: pairs.iter().map(|(_, _, ns)| *ns).collect(),
                    filters,
                    exact,
                });
            }
            paths.push(DeltaPath { source: src, source_filters, steps });
        }
        Ok((JoinPlan { inputs: join_inputs, paths, width }, nodes, schema))
    }

    /// Is the set of j-side key expressions a superset of a unique key of j?
    fn covers_unique_key(&self, inp: &JoinInputSpec, keys: &[Expr]) -> bool {
        if let NodeKind::Table { pk, .. } = &self.g.node(inp.node).kind {
            if pk.is_empty() {
                return false;
            }
            let defs: Vec<Expr> = (0..inp.width).map(|c| inp.mfp.output_expr(c)).collect();
            let raw: Vec<Option<usize>> = keys.iter().map(|k| k.substitute(&defs).as_column()).collect();
            return pk.iter().all(|p| raw.contains(&Some(*p)));
        }
        false
    }

    fn order_key_pairs(&self, inp: &JoinInputSpec, pairs: &mut [(Expr, Expr, bool)]) {
        if let NodeKind::Table { pk, .. } = &self.g.node(inp.node).kind
            && !pk.is_empty()
            && pairs.len() == pk.len()
            && pk.iter().all(|p| pairs.iter().any(|(r, _, _)| r.as_column() == Some(*p)))
        {
            pairs.sort_by_key(|(r, _, _)| pk.iter().position(|p| r.as_column() == Some(*p)).unwrap());
            return;
        }
        pairs.sort_by(|a, b| {
            let (mut fa, mut fb) = (Vec::new(), Vec::new());
            a.0.fingerprint(&mut fa);
            b.0.fingerprint(&mut fb);
            fa.cmp(&fb)
        });
    }

    fn lower_inner(&mut self, inputs: Vec<Lowered>, conds: Vec<Expr>) -> Result<Lowered> {
        if inputs.len() == 1 {
            let mut l = inputs.into_iter().next().unwrap();
            for c in conds {
                l.mfp.add_filter(c);
            }
            return Ok(l);
        }
        let specs: Vec<JoinInputSpec> = inputs
            .into_iter()
            .map(|l| JoinInputSpec { width: l.schema.len(), node: l.node, mfp: l.mfp, schema: l.schema, presence: None, exact: false })
            .collect();
        let (plan, nodes, schema) = self.plan_join(specs, conds)?;
        let node = self.g.add_node(NodeKind::Join(plan), nodes, schema);
        Ok(self.base(node))
    }

    // ------------------------------------------------------------------ semijoins

    /// l (anti)semijoin r on `on` (over l ++ r columns). Output: l's columns.
    pub fn lower_semijoin(&mut self, l: Lowered, r: Lowered, on: &[Expr], anti: bool) -> Result<Lowered> {
        let nl = l.schema.len();
        let mut left_only = Vec::new();
        let mut right_only = Vec::new();
        let mut pairs: Vec<(Expr, Expr, bool)> = Vec::new(); // (left expr, right expr (local), null_safe)
        let mut general = false;
        for c in on {
            let cols = c.column_set();
            if cols.iter().all(|x| *x < nl) {
                left_only.push(c.clone());
                continue;
            }
            if cols.iter().all(|x| *x >= nl) {
                // filters the right side before matching
                right_only.push(c.remapped(&|v| v - nl));
                continue;
            }
            if let Some((x, y, ns)) = is_eq_call(c) {
                let (xc, yc) = (x.column_set(), y.column_set());
                let x_left = xc.iter().all(|v| *v < nl);
                let y_left = yc.iter().all(|v| *v < nl);
                let x_right = xc.iter().all(|v| *v >= nl);
                let y_right = yc.iter().all(|v| *v >= nl);
                if x_left && y_right && !xc.is_empty() && !yc.is_empty() {
                    pairs.push((x.clone(), y.remapped(&|v| v - nl), ns));
                    continue;
                }
                if y_left && x_right && !xc.is_empty() && !yc.is_empty() {
                    pairs.push((y.clone(), x.remapped(&|v| v - nl), ns));
                    continue;
                }
            }
            general = true;
        }
        let mut r = r;
        for p in right_only {
            r.mfp.add_filter(p);
        }
        let semi = if general {
            // matching left rows via a full join, then presence on all left columns
            let joined = {
                let mut inputs = vec![l.clone(), r];
                let mut conds = Vec::new();
                conds.extend(on.iter().cloned());
                let _ = &mut inputs;
                self.lower_inner(inputs, conds)?
            };
            let mut matched = joined;
            matched.mfp.project(&(0..nl).collect::<Vec<_>>());
            matched.schema.truncate(nl);
            let pairs_all: Vec<(Expr, Expr, bool)> = (0..nl).map(|c| (Expr::Column(c), Expr::Column(c), true)).collect();
            let mut lf = l.clone();
            for p in &left_only {
                lf.mfp.add_filter(p.clone());
            }
            // the matched rows are looked up exactly: rows that are equal but
            // differ in representation (numeric scale, ...) may differ in
            // whether they match, so neither a distinct nor a lookup by
            // equality may conflate them
            self.presence_join(lf, matched, pairs_all, true)?
        } else {
            let mut lf = l.clone();
            for p in &left_only {
                lf.mfp.add_filter(p.clone());
            }
            // right side projected to its key expressions
            let mut rk = r;
            let rk_exprs: Vec<Expr> = pairs.iter().map(|(_, re, _)| re.clone()).collect();
            let base_w = rk.schema.len();
            for e in &rk_exprs {
                let t = e.column_type(&rk.schema);
                rk.mfp.add_map(e.clone());
                rk.schema.push(t);
            }
            let k = rk_exprs.len();
            rk.mfp.project(&(base_w..base_w + k).collect::<Vec<_>>());
            rk.schema = rk.schema[base_w..].to_vec();
            let pairs_local: Vec<(Expr, Expr, bool)> =
                pairs.iter().enumerate().map(|(i, (le, _, ns))| (le.clone(), Expr::Column(i), *ns)).collect();
            self.presence_join(lf, rk, pairs_local, false)?
        };
        if !anti {
            return Ok(semi);
        }
        // anti = l - semi
        self.union(vec![l, semi], vec![false, true])
    }

    /// Semijoin of `l` with the key set of `r` (r's columns are the keys),
    /// matching keys by equality or, if `exact`, by identity.
    fn presence_join(&mut self, l: Lowered, r: Lowered, pairs: Vec<(Expr, Expr, bool)>, exact: bool) -> Result<Lowered> {
        let nl = l.schema.len();
        let k = r.schema.len();
        let r_schema = r.schema.clone();
        let rnode = self.materialize(r);
        let key: Vec<Expr> = (0..k).map(Expr::Column).collect();
        let parr = self.arrange(rnode, key, vec![SortSpec::ASC; k], vec![]);
        let inputs = vec![
            JoinInputSpec { width: nl, node: l.node, mfp: l.mfp.clone(), schema: l.schema.clone(), presence: None, exact: false },
            JoinInputSpec { width: k, node: rnode, mfp: Mfp::identity(k), schema: r_schema, presence: Some(parr), exact },
        ];
        let lschema = l.schema.clone();
        let conds: Vec<Expr> = pairs
            .into_iter()
            .map(|(le, re, ns)| {
                let rr = re.remapped(&|c| c + nl);
                if ns {
                    Expr::IsDistinct(Box::new(le), Box::new(rr), true)
                } else {
                    // operands were type-aligned when "=" was resolved
                    let t = le.ty(&lschema);
                    make_eq(le, rr, &t)
                }
            })
            .collect();
        let (plan, nodes, schema) = self.plan_join(inputs, conds)?;
        let node = self.g.add_node(NodeKind::Join(plan), nodes, schema);
        let mut out = self.base(node);
        out.mfp.project(&(0..nl).collect::<Vec<_>>());
        out.schema.truncate(nl);
        Ok(out)
    }

    fn lower_outer(&mut self, l: Lowered, r: Lowered, on: &[Expr], kind: JoinKind) -> Result<Lowered> {
        let (nl, nr) = (l.schema.len(), r.schema.len());
        let inner = {
            let inputs = vec![l.clone(), r.clone()];
            self.lower_inner(inputs, on.to_vec())?
        };
        let mut parts = vec![inner];
        let null_cols = |schema: &[ColumnType]| -> Vec<Expr> { schema.iter().map(|c| Expr::null(c.ty.clone())).collect() };
        if matches!(kind, JoinKind::Left | JoinKind::Full) {
            let mut anti = self.lower_semijoin(l.clone(), r.clone(), on, true)?;
            for e in null_cols(&r.schema) {
                anti.mfp.add_map(e);
            }
            anti.schema.extend(r.schema.iter().map(|c| ColumnType { nullable: true, ..c.clone() }));
            parts.push(anti);
        }
        if matches!(kind, JoinKind::Right | JoinKind::Full) {
            // r antisemijoin l with the condition re-expressed over r ++ l
            let swapped: Vec<Expr> = on.iter().map(|c| c.remapped(&|x| if x < nl { x + nr } else { x - nl })).collect();
            let mut anti = self.lower_semijoin(r.clone(), l.clone(), &swapped, true)?;
            // output [NULL left..., right...]
            let base = nr;
            for e in null_cols(&l.schema) {
                anti.mfp.add_map(e);
            }
            let mut proj: Vec<usize> = (base..base + nl).collect();
            proj.extend(0..nr);
            anti.mfp.project(&proj);
            let mut schema: Vec<ColumnType> = l.schema.iter().map(|c| ColumnType { nullable: true, ..c.clone() }).collect();
            schema.extend(r.schema.iter().cloned());
            anti.schema = schema;
            parts.push(anti);
        }
        let n = parts.len();
        let mut u = self.union(parts, vec![false; n])?;
        // nullability of the outer side
        for (i, c) in u.schema.iter_mut().enumerate() {
            if (i >= nl && matches!(kind, JoinKind::Left | JoinKind::Full)) || (i < nl && matches!(kind, JoinKind::Right | JoinKind::Full)) {
                c.nullable = true;
            }
        }
        Ok(u)
    }

    // ------------------------------------------------------------------ aggregation

    fn lower_aggregate(&mut self, input: &Rel, group: &[usize], aggs: &[AggCall]) -> Result<Lowered> {
        // DISTINCT aggregates: aggregate over DISTINCT(group, args), joined back
        if aggs.iter().any(|a| a.distinct) {
            return self.lower_distinct_aggs(input, group, aggs);
        }
        // ordered aggregates with different orders get separate reductions
        let orders: Vec<&Vec<SortKey>> = aggs.iter().filter(|a| a.func.is_ordered()).map(|a| &a.order).collect();
        if orders.windows(2).any(|w| w[0] != w[1]) {
            return self.split_aggregates(input, group, aggs, |a| a.func.is_ordered());
        }
        let mut l = self.lower(input)?;
        // keep only the columns the aggregation reads: the input arrangement
        // is state, so its width matters
        let mut needed: Vec<usize> = group.to_vec();
        for a in aggs {
            needed.extend(a.args.iter().copied());
            needed.extend(a.filter);
            needed.extend(a.order.iter().map(|k| k.col));
        }
        let mut uniq: Vec<usize> = Vec::new();
        for c in needed {
            if !uniq.contains(&c) {
                uniq.push(c);
            }
        }
        let pos = |c: usize| uniq.iter().position(|x| *x == c).unwrap();
        l.mfp.project(&uniq);
        l.schema = uniq.iter().map(|c| l.schema[*c].clone()).collect();
        let group: Vec<usize> = group.iter().map(|c| pos(*c)).collect();
        let aggs: Vec<AggCall> = aggs
            .iter()
            .map(|a| AggCall {
                args: a.args.iter().map(|c| pos(*c)).collect(),
                filter: a.filter.map(pos),
                order: a.order.iter().map(|k| SortKey { col: pos(k.col), spec: k.spec }).collect(),
                ..a.clone()
            })
            .collect();
        let (group, aggs) = (&group[..], &aggs[..]);
        let in_schema = l.schema.clone();
        let node = self.materialize(l);
        let mut key: Vec<Expr> = group.iter().map(|c| Expr::Column(*c)).collect();
        let mut specs = vec![SortSpec::ASC; group.len()];
        if let Some(a) = aggs.iter().find(|a| a.func.is_ordered()) {
            for k in &a.order {
                key.push(Expr::Column(k.col));
                specs.push(k.spec);
            }
        }
        let mut agg_specs = Vec::with_capacity(aggs.len());
        let mut summaries: Vec<(AggKind, Expr)> = Vec::new();
        let mut slots = Vec::with_capacity(aggs.len());
        for a in aggs {
            let spec = AggSpec {
                func: a.func,
                arg: a.args.first().copied(),
                arg2: a.args.get(1).copied(),
                filter: a.filter,
                arg_type: a.args.first().map(|c| in_schema[*c].ty.clone()).unwrap_or(SqlType::Int8),
                ret: a.ret.clone(),
                fraction: a.fraction,
            };
            let mut s = Vec::new();
            for sm in spec.summaries() {
                s.push(summaries.len());
                summaries.push(sm);
            }
            slots.push(s);
            agg_specs.push(spec);
        }
        let input_arr = self.arrange(node, key, specs, summaries);
        // output: group columns then aggregates
        let mut out_schema: Vec<ColumnType> = group.iter().map(|c| in_schema[*c].clone()).collect();
        out_schema.extend(aggs.iter().map(|a| a.ret.clone()));
        // output arrangement keyed by group columns; created on the node below
        let ng = group.len();
        let plan = ReducePlan {
            group_key: group.to_vec(),
            aggs: agg_specs,
            slots,
            input_arr,
            out_arr: usize::MAX,
            errors: Default::default(),
            cache: Default::default(),
        };
        let out_key: Vec<Expr> = (0..ng).map(Expr::Column).collect();
        let final_node = self.g.add_node_with_output_arr(
            self.env,
            NodeKind::Reduce(plan),
            vec![node],
            out_schema,
            out_key,
            vec![SortSpec::ASC; ng],
            |k, a| {
                if let NodeKind::Reduce(p) = k {
                    p.out_arr = a;
                }
            },
        );
        Ok(self.base(final_node))
    }

    /// Split aggregates into two reductions joined on the group key.
    fn split_aggregates(&mut self, input: &Rel, group: &[usize], aggs: &[AggCall], pred: impl Fn(&AggCall) -> bool) -> Result<Lowered> {
        let first: Vec<usize> = (0..aggs.len()).filter(|i| pred(&aggs[*i])).take(1).collect();
        let (a_idx, b_idx): (Vec<usize>, Vec<usize>) = (0..aggs.len()).partition(|i| first.contains(i) || (!pred(&aggs[*i]) && first.is_empty()));
        let a_aggs: Vec<AggCall> = a_idx.iter().map(|i| aggs[*i].clone()).collect();
        let b_aggs: Vec<AggCall> = b_idx.iter().map(|i| aggs[*i].clone()).collect();
        let ng = group.len();
        let ra = Rel::Aggregate { input: Box::new(input.clone()), group: group.to_vec(), aggs: a_aggs };
        let rb = Rel::Aggregate { input: Box::new(input.clone()), group: group.to_vec(), aggs: b_aggs };
        let na = ra.arity();
        let on: Vec<Expr> = (0..ng).map(|i| Expr::IsDistinct(Box::new(Expr::Column(i)), Box::new(Expr::Column(na + i)), true)).collect();
        let joined = Rel::Join { left: Box::new(ra), right: Box::new(rb), kind: JoinKind::Inner, on };
        // reorder to group, then aggregates in original order
        let mut proj: Vec<usize> = (0..ng).collect();
        for i in 0..aggs.len() {
            if let Some(p) = a_idx.iter().position(|x| *x == i) {
                proj.push(ng + p);
            } else {
                let p = b_idx.iter().position(|x| *x == i).unwrap();
                proj.push(na + ng + p);
            }
        }
        self.lower(&joined.project(proj))
    }

    fn lower_distinct_aggs(&mut self, input: &Rel, group: &[usize], aggs: &[AggCall]) -> Result<Lowered> {
        let ng = group.len();
        // all DISTINCT aggregates over the same argument set and FILTER can
        // share one distinct input
        let distinct_args: Vec<(Vec<usize>, Option<usize>)> = {
            let mut v: Vec<(Vec<usize>, Option<usize>)> = aggs.iter().filter(|a| a.distinct).map(|a| (a.args.clone(), a.filter)).collect();
            v.sort();
            v.dedup();
            v
        };
        if distinct_args.len() == 1 && aggs.iter().all(|a| a.distinct) {
            // Aggregate(Distinct(Project(group ++ args [++ filter])))
            let (args, filter) = &distinct_args[0];
            let mut cols: Vec<usize> = group.to_vec();
            cols.extend(args.iter().copied());
            // the FILTER flag is part of the distinct rows: a value counts once
            // among the rows that pass the filter, and groups none of whose
            // rows pass it still exist
            if let Some(f) = filter {
                cols.push(*f);
            }
            let projected = Rel::Distinct { input: Box::new(input.clone().project(cols)) };
            let new_aggs: Vec<AggCall> = aggs
                .iter()
                .map(|a| AggCall {
                    distinct: false,
                    filter: a.filter.map(|_| ng + args.len()),
                    args: a.args.iter().map(|x| ng + args.iter().position(|y| y == x).unwrap()).collect(),
                    order: a.order.iter().map(|k| SortKey { col: ng + args.iter().position(|y| *y == k.col).unwrap_or(0), spec: k.spec }).collect(),
                    ..a.clone()
                })
                .collect();
            let agg = Rel::Aggregate { input: Box::new(projected), group: (0..ng).collect(), aggs: new_aggs };
            return self.lower(&agg);
        }
        // mixed: split off one distinct aggregate at a time
        self.split_aggregates(input, group, aggs, |a| a.distinct)
    }
}

/// Spec of a join input during planning.
pub struct JoinInputSpec {
    pub node: NodeId,
    pub mfp: Mfp,
    pub width: usize,
    pub schema: Vec<ColumnType>,
    pub presence: Option<ArrId>,
    /// Presence matches exactly (see JoinInput::exact).
    pub exact: bool,
}

impl From<Lowered> for JoinInputSpec {
    fn from(l: Lowered) -> Self {
        JoinInputSpec { width: l.schema.len(), node: l.node, mfp: l.mfp, schema: l.schema, presence: None, exact: false }
    }
}

/// Structural equality of expressions.
fn same_expr(a: &Expr, b: &Expr) -> bool {
    let (mut x, mut y) = (Vec::new(), Vec::new());
    a.fingerprint(&mut x);
    b.fingerprint(&mut y);
    x == y
}

/// Equality of two expressions of the same (already aligned) type.
fn make_eq(a: Expr, b: Expr, t: &SqlType) -> Expr {
    match crate::expr::func::resolve("=", &[t.clone(), t.clone()]) {
        Ok(r) => Expr::Call(Box::new(crate::expr::Call { func: r.func, args: vec![a, b], ret: SqlType::Bool })),
        Err(_) => Expr::IsDistinct(Box::new(a), Box::new(b), true),
    }
}

fn collect_gets(rel: &Rel, out: &mut Vec<u64>) {
    if let Rel::Get { id, .. } = rel {
        out.push(*id);
    }
    for i in rel.inputs() {
        collect_gets(i, out);
    }
}

/// Constant rows of a Values node (helper for callers).
pub fn values_rows(rows: &[Vec<Expr>]) -> Result<Vec<Row>> {
    rows.iter().map(|r| r.iter().map(|e| e.eval(&[] as &[Datum])).collect()).collect()
}

#[allow(dead_code)]
fn _agg_name(f: AggFunc) -> &'static str {
    f.name()
}
