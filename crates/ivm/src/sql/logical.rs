//! Stable column bindings used by relational rewrites. The binder's ordered
//! slots are resolved once at entry; dense execution slots are assigned only
//! after decorrelation and optimization have finished.

use super::plan::{self, *};
use crate::error::{Result, codes};
use crate::expr::{Expr, SubqueryKind};
use crate::types::{ColumnType, SqlType};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};

/// Identity of a logical value, independent of its position in any row.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ColumnId(pub u64);

impl ColumnId {
    pub fn fresh() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}
impl std::fmt::Display for ColumnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "c{}", self.0)
    }
}

#[derive(Clone, Debug)]
pub struct LogicalRel {
    pub kind: LogicalKind,
    /// Ordered public bindings; projections may repeat a binding.
    pub columns: Vec<ColumnId>,
}

super::plan::relational_node!(LogicalKind, ColumnId, LogicalRel);

pub fn columns(e: &Expr) -> BTreeSet<ColumnId> {
    let mut out = BTreeSet::new();
    fn visit(e: &Expr, out: &mut BTreeSet<ColumnId>) {
        if let Expr::LogicalColumn(c) = e {
            out.insert(*c);
        }
        for child in e.children() {
            visit(child, out);
        }
    }
    visit(e, &mut out);
    out
}

pub fn rename(e: &Expr, f: &impl Fn(ColumnId) -> ColumnId) -> Expr {
    e.rewritten(&mut |e| match e {
        Expr::LogicalColumn(c) => Some(Expr::LogicalColumn(f(*c))),
        _ => None,
    })
}

pub fn dense_expr(e: &Expr, cols: &[ColumnId]) -> Expr {
    e.rewritten(&mut |e| match e {
        Expr::LogicalColumn(c) => Some(Expr::Column(position(cols, *c))),
        _ => None,
    })
}
fn position(cols: &[ColumnId], c: ColumnId) -> usize {
    cols.iter().position(|x| *x == c).unwrap_or_else(|| panic!("unbound logical column {c} in {cols:?}"))
}
fn fresh(n: usize) -> Vec<ColumnId> {
    (0..n).map(|_| ColumnId::fresh()).collect()
}

pub fn map_agg<C: Copy, D>(a: AggCall<C>, f: &impl Fn(C) -> D) -> AggCall<D> {
    AggCall {
        func: a.func,
        args: a.args.into_iter().map(f).collect(),
        distinct: a.distinct,
        filter: a.filter.map(f),
        order: a.order.into_iter().map(|k| SortKey { col: f(k.col), spec: k.spec }).collect(),
        fraction: a.fraction,
        ret: a.ret,
    }
}
pub fn map_window<C: Copy, D>(a: WindowCall<C>, f: &impl Fn(C) -> D) -> WindowCall<D> {
    WindowCall {
        func: a.func,
        args: a.args.into_iter().map(f).collect(),
        partition: a.partition.into_iter().map(f).collect(),
        order: a.order.into_iter().map(|k| SortKey { col: f(k.col), spec: k.spec }).collect(),
        frame: a.frame,
        ret: a.ret,
        range: a.range,
    }
}

impl LogicalRel {
    pub fn new(kind: LogicalKind, columns: Vec<ColumnId>) -> Self {
        Self { kind, columns }
    }
    pub fn unit() -> Self {
        Self::new(LogicalKind::Values { rows: vec![vec![]], columns: vec![] }, vec![])
    }
    pub fn values(rows: Vec<Vec<Expr>>, types: Vec<ColumnType>) -> Self {
        Self::new(LogicalKind::Values { rows, columns: types.clone() }, fresh(types.len()))
    }
    pub fn arity(&self) -> usize {
        self.columns.len()
    }
    pub fn column_set(&self) -> BTreeSet<ColumnId> {
        self.columns.iter().copied().collect()
    }
    pub fn map(self, exprs: Vec<Expr>) -> Self {
        let ids = fresh(exprs.len());
        self.map_with(ids, exprs)
    }
    pub fn map_with(self, ids: Vec<ColumnId>, exprs: Vec<Expr>) -> Self {
        assert_eq!(ids.len(), exprs.len());
        if ids.is_empty() {
            return self;
        }
        let mut cols = self.columns.clone();
        cols.extend(ids);
        Self::new(LogicalKind::Map { input: Box::new(self), exprs }, cols)
    }
    pub fn filter(self, preds: Vec<Expr>) -> Self {
        let preds: Vec<_> = preds.into_iter().filter(|e| !e.is_true_literal()).collect();
        if preds.is_empty() {
            return self;
        }
        let cols = self.columns.clone();
        match self.kind {
            LogicalKind::Filter { input, preds: mut old } => {
                old.extend(preds);
                Self::new(LogicalKind::Filter { input, preds: old }, cols)
            }
            _ => Self::new(LogicalKind::Filter { input: Box::new(self), preds }, cols),
        }
    }
    pub fn project(self, cols: Vec<ColumnId>) -> Self {
        if cols == self.columns {
            return self;
        }
        match self.kind {
            LogicalKind::Project { input, .. } => Self::new(LogicalKind::Project { input, cols: cols.clone() }, cols),
            _ => Self::new(LogicalKind::Project { input: Box::new(self), cols: cols.clone() }, cols),
        }
    }
    pub fn join(self, right: Self, kind: JoinKind, on: Vec<Expr>) -> Self {
        let mut cols = self.columns.clone();
        if !matches!(kind, JoinKind::Semi | JoinKind::Anti) {
            cols.extend(&right.columns);
        }
        Self::new(LogicalKind::Join { left: Box::new(self), right: Box::new(right), kind, on }, cols)
    }
    pub fn aggregate(self, group: Vec<ColumnId>, aggs: Vec<AggCall<ColumnId>>) -> Self {
        let mut cols = group.clone();
        cols.extend(fresh(aggs.len()));
        Self::new(LogicalKind::Aggregate { input: Box::new(self), group, aggs }, cols)
    }
    pub fn distinct(self) -> Self {
        let cols = self.columns.clone();
        Self::new(LogicalKind::Distinct { input: Box::new(self) }, cols)
    }
    pub fn inputs(&self) -> smallvec::SmallVec<[&Self; 2]> {
        self.kind.inputs()
    }
    pub fn inputs_mut(&mut self) -> smallvec::SmallVec<[&mut Self; 2]> {
        self.kind.inputs_mut()
    }
    pub fn map_inputs(mut self, mut f: impl FnMut(Self) -> Result<Self>) -> Result<Self> {
        for child in self.inputs_mut() {
            *child = f(std::mem::replace(child, Self::unit()))?;
        }
        Ok(self)
    }
    pub fn exprs(&self) -> Vec<&Expr> {
        self.kind.exprs()
    }
    pub fn exprs_mut(&mut self) -> Vec<&mut Expr> {
        self.kind.exprs_mut()
    }
    pub fn visit_scoped(&mut self, depth: usize, f: &mut impl FnMut(&mut Expr, usize)) {
        fn visit(e: &mut Expr, depth: usize, f: &mut impl FnMut(&mut Expr, usize)) {
            f(e, depth);
            for c in e.children_mut() {
                visit(c, depth, f);
            }
        }
        for e in self.exprs_mut() {
            visit(e, depth, f);
        }
        let apply = matches!(self.kind, LogicalKind::Apply { .. });
        for (i, child) in self.inputs_mut().into_iter().enumerate() {
            child.visit_scoped(depth + usize::from(apply && i == 1), f);
        }
    }
    pub fn outer_columns(&self, depth: usize) -> BTreeMap<ColumnId, SqlType> {
        fn visit(e: &Expr, depth: usize, out: &mut BTreeMap<ColumnId, SqlType>) {
            if let Expr::LogicalOuter(d, c, t) = e
                && *d == depth
            {
                out.insert(*c, t.clone());
            }
            for c in e.children() {
                visit(c, depth, out);
            }
        }
        let mut out = BTreeMap::new();
        for e in self.exprs() {
            visit(e, depth, &mut out);
        }
        let apply = matches!(self.kind, LogicalKind::Apply { .. });
        for (i, child) in self.inputs().into_iter().enumerate() {
            out.extend(child.outer_columns(depth + usize::from(apply && i == 1)));
        }
        out
    }
    pub fn has_outer_refs(&self, depth: usize) -> bool {
        !self.outer_columns(depth).is_empty()
    }
    /// A separate occurrence of a plan needs separate bindings, even when it
    /// reads the same table or was expanded from the same CTE definition.
    pub fn fresh_copy(&self) -> Self {
        fn collect(r: &LogicalRel, m: &mut BTreeMap<ColumnId, ColumnId>) {
            for c in &r.columns {
                m.entry(*c).or_insert_with(ColumnId::fresh);
            }
            for i in r.inputs() {
                collect(i, m);
            }
        }
        fn remap(r: &mut LogicalRel, m: &BTreeMap<ColumnId, ColumnId>) {
            let at = |c: ColumnId| m.get(&c).copied().unwrap_or(c);
            for c in &mut r.columns {
                *c = at(*c);
            }
            match &mut r.kind {
                LogicalKind::Project { cols, .. } => {
                    for c in cols {
                        *c = at(*c);
                    }
                }
                LogicalKind::Aggregate { group, aggs, .. } => {
                    for c in group {
                        *c = at(*c);
                    }
                    for a in aggs {
                        a.remap(&at);
                    }
                }
                LogicalKind::TopK { group, order, .. } => {
                    for c in group {
                        *c = at(*c);
                    }
                    for k in order {
                        k.col = at(k.col);
                    }
                }
                LogicalKind::Window { funcs, .. } => {
                    for f in funcs {
                        f.remap(&at);
                    }
                }
                _ => {}
            }
            for e in r.exprs_mut() {
                e.rewrite(&mut |e| match e {
                    Expr::LogicalColumn(c) => Some(Expr::LogicalColumn(at(*c))),
                    Expr::LogicalOuter(d, c, t) => Some(Expr::LogicalOuter(*d, at(*c), t.clone())),
                    _ => None,
                });
            }
            for i in r.inputs_mut() {
                remap(i, m);
            }
        }
        let mut map = BTreeMap::new();
        collect(self, &mut map);
        let mut out = self.clone();
        remap(&mut out, &map);
        out
    }
    pub fn type_of(&self, e: &Expr) -> ColumnType {
        dense_expr(e, &self.columns).column_type(&self.schema())
    }
    pub fn column_type(&self, c: ColumnId) -> ColumnType {
        self.schema()[position(&self.columns, c)].clone()
    }
    /// Derive types in this operator's context. In particular, a binding can
    /// become nullable on the null-extended side of an outer join.
    pub fn schema(&self) -> Vec<ColumnType> {
        self.kind.schema_with(|e, schema| dense_expr(e, &self.columns).column_type(schema))
    }

    pub fn column_offset(&self, column: ColumnId) -> usize {
        position(&self.columns, column)
    }
    /// Validate the phase boundary before assigning execution offsets. Stable
    /// allocation identities never enter evaluator or fingerprint code.
    pub fn into_dense(self) -> plan::Rel {
        fn validate(r: &LogicalRel) {
            for e in r.exprs() {
                assert!(!e.has_outer(), "unresolved outer reference at dense lowering");
                assert!(!e.has_subquery(), "unextracted subquery at dense lowering");
                if matches!(r.kind, LogicalKind::Values { .. }) {
                    assert!(columns(e).is_empty(), "unbound logical column in VALUES");
                }
            }
            assert!(!matches!(r.kind, LogicalKind::Apply { .. }), "unresolved Apply at dense lowering");
            for child in r.inputs() {
                validate(child);
            }
        }
        validate(&self);
        self.lower()
    }
    /// The only assignment of row offsets after binding.
    fn lower(self) -> plan::Rel {
        use LogicalKind::*;
        let own = self.columns;
        match self.kind {
            Get { id, name, columns, keys } => plan::Rel::Get { id, name, columns, keys },
            Values { rows, columns } => plan::Rel::Values { rows, columns },
            RecRef { id, columns } => plan::Rel::RecRef { id, columns },
            Map { input, exprs } => {
                plan::Rel::Map { input: Box::new(input.lower()), exprs: exprs.iter().map(|e| dense_expr(e, &own)).collect() }
            }
            Filter { input, preds } => {
                let preds = preds.iter().map(|e| dense_expr(e, &input.columns)).collect();
                plan::Rel::Filter { input: Box::new(input.lower()), preds }
            }
            Project { input, .. } => {
                let cols: Vec<_> = own.iter().map(|c| position(&input.columns, *c)).collect();
                let input = input.lower();
                // Pruning can leave only copied map outputs. Collapse them
                // here, after the logical identities have served their purpose,
                // preserving the canonical execution plan and its fingerprint.
                if let plan::Rel::Map { input: inner, exprs } = &input {
                    let n = inner.arity();
                    let direct = cols
                        .iter()
                        .map(|&c| {
                            if c < n {
                                return Some(c);
                            }
                            match exprs[c - n] {
                                Expr::Column(k) if k < n => Some(k),
                                _ => None,
                            }
                        })
                        .collect::<Option<Vec<_>>>();
                    if let Some(direct) = direct {
                        let plan::Rel::Map { input: inner, .. } = input else { unreachable!() };
                        return inner.project(direct);
                    }
                }
                input.project(cols)
            }
            Join { left, right, kind, on } => {
                let mut cols = left.columns.clone();
                cols.extend(&right.columns);
                let on = on.iter().map(|e| dense_expr(e, &cols)).collect();
                plan::Rel::Join { left: Box::new(left.lower()), right: Box::new(right.lower()), kind, on }
            }
            Aggregate { input, group, aggs } => {
                let at = |c| position(&input.columns, c);
                let group = group.into_iter().map(at).collect();
                let aggs = aggs.into_iter().map(|a| map_agg(a, &at)).collect();
                plan::Rel::Aggregate { input: Box::new(input.lower()), group, aggs }
            }
            Distinct { input } => plan::Rel::Distinct { input: Box::new(input.lower()) },
            SetOp { op, all, left, right } => plan::Rel::SetOp { op, all, left: Box::new(left.lower()), right: Box::new(right.lower()) },
            UnionAll { inputs } => plan::Rel::UnionAll { inputs: inputs.into_iter().map(Self::lower).collect() },
            TopK { input, group, order, limit, offset, with_ties } => {
                let group = group.into_iter().map(|c| position(&input.columns, c)).collect();
                let order = order.into_iter().map(|k| SortKey { col: position(&input.columns, k.col), spec: k.spec }).collect();
                plan::Rel::TopK { input: Box::new(input.lower()), group, order, limit, offset, with_ties }
            }
            Window { input, funcs } => {
                let funcs = funcs.into_iter().map(|f| map_window(f, &|c| position(&input.columns, c))).collect();
                plan::Rel::Window { input: Box::new(input.lower()), funcs }
            }
            FlatMap { input, mut funcs, out, ordinality } => {
                for f in &mut funcs {
                    for e in &mut f.args {
                        *e = dense_expr(e, &input.columns);
                    }
                }
                plan::Rel::FlatMap { input: Box::new(input.lower()), funcs, out, ordinality }
            }
            Recursive { id, base, step, distinct, columns } => {
                plan::Rel::Recursive { id, base: Box::new(base.lower()), step: Box::new(step.lower()), distinct, columns }
            }
            Negate { input } => plan::Rel::Negate { input: Box::new(input.lower()) },
            Apply { .. } => panic!("Apply must be decorrelated before assigning dense slots"),
        }
    }
}

/// Resolve the binder's slots and extract subqueries with their final logical
/// identities. Appending an Apply never changes any previously bound reference.
pub fn bind(rel: plan::Rel) -> Result<LogicalRel> {
    bind_rel(rel, &[])
}

fn bind_kind(mut kind: ApplyKind, locals: &[ColumnId], outers: &[Vec<ColumnId>], input: &mut LogicalRel) -> Result<ApplyKind> {
    let args: Vec<&mut Expr> = match &mut kind {
        ApplyKind::Semi { lhs, .. } | ApplyKind::Mark { kind: SubqueryKind::In(lhs) } => lhs.iter_mut().collect(),
        ApplyKind::Mark { kind: SubqueryKind::AnyAll { lhs, .. } } => vec![lhs],
        _ => vec![],
    };
    for e in args {
        *e = bind_expr(std::mem::replace(e, Expr::bool(true)), locals, outers, input)?;
    }
    Ok(kind)
}
fn apply(left: LogicalRel, right: LogicalRel, kind: ApplyKind) -> LogicalRel {
    let mut cols = left.columns.clone();
    match &kind {
        ApplyKind::Semi { .. } => {}
        ApplyKind::Mark { .. } => cols.extend(fresh(1)),
        ApplyKind::Scalar => cols.extend(fresh(2)),
        ApplyKind::Lateral { .. } => cols.extend(&right.columns),
    }
    LogicalRel::new(LogicalKind::Apply { left: Box::new(left), right: Box::new(right), kind }, cols)
}
fn bind_expr(mut e: Expr, locals: &[ColumnId], outers: &[Vec<ColumnId>], input: &mut LogicalRel) -> Result<Expr> {
    match e {
        Expr::Column(c) => return Ok(Expr::LogicalColumn(locals[c])),
        Expr::Outer(d, c, ty) => return Ok(Expr::LogicalOuter(d, outers[d - 1][c], ty)),
        Expr::Subquery(sq) => {
            let crate::expr::SubqueryExpr { kind, rel, ty } = *sq;
            let kind = match kind {
                SubqueryKind::Scalar => ApplyKind::Scalar,
                kind => ApplyKind::Mark { kind },
            };
            let kind = bind_kind(kind, locals, outers, input)?;
            let mut nested = vec![locals.to_vec()];
            nested.extend_from_slice(outers);
            let right = bind_rel(rel, &nested)?;
            let left = std::mem::replace(input, LogicalRel::unit());
            *input = apply(left, right, kind);
            let end = input.arity();
            return Ok(if matches!(input.kind, LogicalKind::Apply { kind: ApplyKind::Scalar, .. }) {
                let count = Expr::LogicalColumn(input.columns[end - 1]);
                let many = Expr::Call(Box::new(crate::expr::Call {
                    func: crate::expr::func::resolve(">", &[SqlType::Int8, SqlType::Int8])?.func,
                    args: vec![count, Expr::Literal(crate::types::Datum::Int8(1), SqlType::Int8)],
                    ret: SqlType::Bool,
                }));
                Expr::Case(Box::new(crate::expr::CaseExpr {
                    whens: vec![(
                        many,
                        Expr::Error(
                            codes::CARDINALITY_VIOLATION.to_string(),
                            "more than one row returned by a subquery used as an expression".to_string(),
                        ),
                    )],
                    else_: Expr::LogicalColumn(input.columns[end - 2]),
                    ty,
                }))
            } else {
                Expr::LogicalColumn(input.columns[end - 1])
            });
        }
        _ => {}
    }
    for child in e.children_mut() {
        *child = bind_expr(std::mem::replace(child, Expr::bool(true)), locals, outers, input)?;
    }
    Ok(e)
}
fn bind_exprs(es: Vec<Expr>, locals: &[ColumnId], outers: &[Vec<ColumnId>], input: &mut LogicalRel) -> Result<Vec<Expr>> {
    es.into_iter().map(|e| bind_expr(e, locals, outers, input)).collect()
}
fn bind_filter(mut input: LogicalRel, preds: Vec<Expr>, outers: &[Vec<ColumnId>]) -> Result<LogicalRel> {
    let locals = input.columns.clone();
    let mut rest = Vec::new();
    for pred in preds {
        let (sq, negated) = match pred {
            Expr::Subquery(sq) if matches!(sq.kind, SubqueryKind::Exists | SubqueryKind::In(_)) => (sq, false),
            Expr::Not(e) if matches!(&*e, Expr::Subquery(sq) if matches!(sq.kind, SubqueryKind::Exists | SubqueryKind::In(_))) => {
                let Expr::Subquery(sq) = *e else { unreachable!() };
                (sq, true)
            }
            e => {
                rest.push(bind_expr(e, &locals, outers, &mut input)?);
                continue;
            }
        };
        let (lhs, null_aware) = match sq.kind {
            SubqueryKind::In(lhs) => (lhs, negated),
            _ => (vec![], false),
        };
        let kind = bind_kind(ApplyKind::Semi { lhs, negated, null_aware }, &locals, outers, &mut input)?;
        let mut nested = vec![locals.clone()];
        nested.extend_from_slice(outers);
        input = apply(input, bind_rel(sq.rel, &nested)?, kind);
    }
    Ok(input.filter(rest).project(locals))
}
fn bind_rel(rel: plan::Rel, outers: &[Vec<ColumnId>]) -> Result<LogicalRel> {
    use plan::Rel as R;
    Ok(match rel {
        R::Get { id, name, columns, keys } => {
            LogicalRel::new(LogicalKind::Get { id, name, columns: columns.clone(), keys }, fresh(columns.len()))
        }
        R::RecRef { id, columns } => LogicalRel::new(LogicalKind::RecRef { id, columns: columns.clone() }, fresh(columns.len())),
        R::Values { rows, columns } => {
            if rows.iter().flatten().any(Expr::has_subquery) {
                crate::sql_bail!(codes::FEATURE_NOT_SUPPORTED, "subqueries in VALUES are not supported");
            }
            let mut unit = LogicalRel::unit();
            let rows = rows.into_iter().map(|r| bind_exprs(r, &[], outers, &mut unit)).collect::<Result<_>>()?;
            LogicalRel::values(rows, columns)
        }
        R::Map { input, exprs } => {
            let mut input = bind_rel(*input, outers)?;
            let mut output = input.columns.clone();
            if exprs.iter().all(|e| matches!(e, Expr::Column(_))) {
                for e in exprs {
                    let Expr::Column(c) = e else { unreachable!() };
                    output.push(output[c]);
                }
                input.project(output)
            } else {
                // Keep mixed maps intact: their dense shape is part of the
                // existing execution/fingerprint normalization.
                let ids = fresh(exprs.len());
                output.extend(&ids);
                let exprs = bind_exprs(exprs, &output, outers, &mut input)?;
                input.map_with(ids, exprs).project(output)
            }
        }
        R::Filter { input, preds } => bind_filter(bind_rel(*input, outers)?, preds, outers)?,
        R::Project { input, cols } => {
            let input = bind_rel(*input, outers)?;
            let cols = cols.into_iter().map(|c| input.columns[c]).collect();
            input.project(cols)
        }
        R::Join { left, right, kind, on } => {
            let left = bind_rel(*left, outers)?;
            let right = bind_rel(*right, outers)?;
            let mut locals = left.columns.clone();
            locals.extend(&right.columns);
            if on.iter().any(Expr::has_subquery) {
                if kind != JoinKind::Inner {
                    crate::sql_bail!(codes::FEATURE_NOT_SUPPORTED, "subqueries in outer join conditions are not supported");
                }
                bind_filter(left.join(right, kind, vec![]), on, outers)?
            } else {
                let on = bind_exprs(on, &locals, outers, &mut LogicalRel::unit())?;
                left.join(right, kind, on)
            }
        }
        R::Aggregate { input, group, aggs } => {
            let input = bind_rel(*input, outers)?;
            let group = group.into_iter().map(|c| input.columns[c]).collect();
            let aggs = aggs.into_iter().map(|a| map_agg(a, &|c| input.columns[c])).collect();
            input.aggregate(group, aggs)
        }
        R::Distinct { input } => bind_rel(*input, outers)?.distinct(),
        R::SetOp { op, all, left, right } => {
            let left = bind_rel(*left, outers)?;
            let right = bind_rel(*right, outers)?;
            let cols = fresh(left.arity());
            LogicalRel::new(LogicalKind::SetOp { op, all, left: Box::new(left), right: Box::new(right) }, cols)
        }
        R::UnionAll { inputs } => {
            let inputs = inputs.into_iter().map(|i| bind_rel(i, outers)).collect::<Result<Vec<_>>>()?;
            let cols = fresh(inputs[0].arity());
            LogicalRel::new(LogicalKind::UnionAll { inputs }, cols)
        }
        R::TopK { input, group, order, limit, offset, with_ties } => {
            let input = bind_rel(*input, outers)?;
            let cols = input.columns.clone();
            let group = group.into_iter().map(|c| cols[c]).collect();
            let order = order.into_iter().map(|k| SortKey { col: cols[k.col], spec: k.spec }).collect();
            LogicalRel::new(LogicalKind::TopK { input: Box::new(input), group, order, limit, offset, with_ties }, cols)
        }
        R::Window { input, funcs } => {
            let input = bind_rel(*input, outers)?;
            let mut cols = input.columns.clone();
            cols.extend(fresh(funcs.len()));
            let funcs = funcs.into_iter().map(|f| map_window(f, &|c| input.columns[c])).collect();
            LogicalRel::new(LogicalKind::Window { input: Box::new(input), funcs }, cols)
        }
        R::FlatMap { input, mut funcs, out, ordinality } => {
            let mut input = bind_rel(*input, outers)?;
            let mut cols = input.columns.clone();
            for f in &mut funcs {
                if f.args.iter().any(Expr::has_subquery) {
                    crate::sql_bail!(codes::FEATURE_NOT_SUPPORTED, "subquery in unsupported position");
                }
                f.args = bind_exprs(std::mem::take(&mut f.args), &cols, outers, &mut input)?;
            }
            cols.extend(fresh(out.len()));
            LogicalRel::new(LogicalKind::FlatMap { input: Box::new(input), funcs, out, ordinality }, cols)
        }
        R::Recursive { id, base, step, distinct, columns } => {
            let base = bind_rel(*base, outers)?;
            let step = bind_rel(*step, outers)?;
            let cols = fresh(base.arity());
            LogicalRel::new(LogicalKind::Recursive { id, base: Box::new(base), step: Box::new(step), distinct, columns }, cols)
        }
        R::Apply { left, right, kind } => {
            let mut left = bind_rel(*left, outers)?;
            let locals = left.columns.clone();
            let kind = bind_kind(kind, &locals, outers, &mut left)?;
            let mut nested = vec![locals];
            nested.extend_from_slice(outers);
            apply(left, bind_rel(*right, &nested)?, kind)
        }
        R::Negate { input } => {
            let input = bind_rel(*input, outers)?;
            let cols = input.columns.clone();
            LogicalRel::new(LogicalKind::Negate { input: Box::new(input) }, cols)
        }
    })
}
