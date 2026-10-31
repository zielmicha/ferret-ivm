//! Logical plan: relational algebra over bound expressions.

use crate::expr::{Expr, SubqueryKind};
use crate::types::encoding::SortSpec;
use crate::types::{ColumnType, SqlType};
use smallvec::{smallvec, SmallVec};
use std::fmt::Write;

/// Catalog object id.
pub type ObjectId = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    /// Left rows with at least one match (no right columns in output).
    Semi,
    /// Left rows without a match (no right columns in output).
    Anti,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SetOpKind {
    Union,
    Except,
    Intersect,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum AggFunc {
    CountStar,
    Count,
    Sum,
    Avg,
    Min,
    Max,
    BoolAnd,
    BoolOr,
    StringAgg,
    ArrayAgg,
    JsonAgg,
    JsonbAgg,
    JsonObjectAgg,
    JsonbObjectAgg,
    StddevSamp,
    StddevPop,
    VarSamp,
    VarPop,
    PercentileCont,
    PercentileDisc,
    /// Most frequent value (ordered-set aggregate).
    Mode,
    /// Internal: value of a subquery that must produce at most one row.
    SingleValue,
    AnyValue,
}

impl AggFunc {
    pub fn name(&self) -> &'static str {
        use AggFunc::*;
        match self {
            CountStar => "count(*)",
            Count => "count",
            Sum => "sum",
            Avg => "avg",
            Min => "min",
            Max => "max",
            BoolAnd => "bool_and",
            BoolOr => "bool_or",
            StringAgg => "string_agg",
            ArrayAgg => "array_agg",
            JsonAgg => "json_agg",
            JsonbAgg => "jsonb_agg",
            JsonObjectAgg => "json_object_agg",
            JsonbObjectAgg => "jsonb_object_agg",
            StddevSamp => "stddev_samp",
            StddevPop => "stddev_pop",
            VarSamp => "var_samp",
            VarPop => "var_pop",
            PercentileCont => "percentile_cont",
            PercentileDisc => "percentile_disc",
            Mode => "mode",
            SingleValue => "single_value",
            AnyValue => "any_value",
        }
    }

    /// Value over an empty input (COUNT gives 0, everything else NULL).
    pub fn empty_is_zero(&self) -> bool {
        matches!(self, AggFunc::CountStar | AggFunc::Count)
    }

    /// Needs an ordered scan of the group rather than a summary.
    pub fn is_ordered(&self) -> bool {
        matches!(
            self,
            AggFunc::StringAgg
                | AggFunc::ArrayAgg
                | AggFunc::JsonAgg
                | AggFunc::JsonbAgg
                | AggFunc::JsonObjectAgg
                | AggFunc::JsonbObjectAgg
                | AggFunc::PercentileCont
                | AggFunc::PercentileDisc
                | AggFunc::Mode
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SortKey {
    pub col: usize,
    pub spec: SortSpec,
}

#[derive(Clone, Debug)]
pub struct AggCall {
    pub func: AggFunc,
    /// Argument columns of the input.
    pub args: Vec<usize>,
    pub distinct: bool,
    /// Boolean input column for FILTER (WHERE ...).
    pub filter: Option<usize>,
    /// ORDER BY within the aggregate (input columns).
    pub order: Vec<SortKey>,
    /// Constant direct argument (percentile fraction).
    pub fraction: Option<f64>,
    pub ret: ColumnType,
}

impl AggCall {
    pub fn columns(&self) -> impl Iterator<Item = usize> + '_ {
        self.args.iter().copied().chain(self.filter).chain(self.order.iter().map(|k| k.col))
    }

    pub fn remap(&mut self, f: &impl Fn(usize) -> usize) {
        for c in self.args.iter_mut().chain(self.filter.iter_mut()).chain(self.order.iter_mut().map(|k| &mut k.col)) {
            *c = f(*c);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WindowFunc {
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    CumeDist,
    Ntile,
    Lag,
    Lead,
    FirstValue,
    LastValue,
    NthValue,
    Agg(AggFunc),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FrameUnits {
    Rows,
    Range,
    Groups,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(i64),
    CurrentRow,
    Following(i64),
    UnboundedFollowing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Frame {
    pub units: FrameUnits,
    pub start: FrameBound,
    pub end: FrameBound,
}

impl Frame {
    /// Default frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
    pub fn default_frame() -> Frame {
        Frame { units: FrameUnits::Range, start: FrameBound::UnboundedPreceding, end: FrameBound::CurrentRow }
    }
}

#[derive(Clone, Debug)]
pub struct WindowCall {
    pub func: WindowFunc,
    pub args: Vec<usize>,
    pub partition: Vec<usize>,
    pub order: Vec<SortKey>,
    pub frame: Frame,
    pub ret: ColumnType,
    pub range: Option<RangeOffsets>,
}

impl WindowCall {
    pub fn columns(&self) -> impl Iterator<Item = usize> + '_ {
        self.args.iter().chain(&self.partition).copied().chain(self.order.iter().map(|k| k.col))
    }

    pub fn remap(&mut self, f: &impl Fn(usize) -> usize) {
        for c in self.args.iter_mut().chain(&mut self.partition).chain(self.order.iter_mut().map(|k| &mut k.col)) {
            *c = f(*c);
        }
    }
}

/// RANGE frame offsets: a bound is the current row's ORDER BY value minus
/// (PRECEDING) or plus (FOLLOWING) the offset, in sort direction.
#[derive(Clone, Debug)]
pub struct RangeOffsets {
    /// Offsets of the start and end bounds (None: not an offset bound).
    pub start: Option<crate::types::Datum>,
    pub end: Option<crate::types::Datum>,
    pub minus: crate::expr::FuncId,
    pub plus: crate::expr::FuncId,
    /// Type ORDER BY values are compared in (dates compare as timestamps).
    pub cmp_type: Option<SqlType>,
}

/// Table functions usable in FROM (and as select-list set-returning functions).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TableFunc {
    GenerateSeries,
    Unnest,
    JsonbArrayElements,
    JsonbArrayElementsText,
    JsonbEach,
    JsonbEachText,
    JsonbObjectKeys,
    RegexpSplitToTable,
    RegexpMatches,
    /// jsonb_path_query[_tz](target, path, vars, silent)
    JsonbPathQuery { tz: bool },
}

/// One table function of a FlatMap.
#[derive(Clone, Debug)]
pub struct SrfCall {
    pub func: TableFunc,
    pub args: Vec<Expr>,
    /// Number of output columns.
    pub arity: usize,
}

#[derive(Clone, Debug)]
pub enum ApplyKind {
    /// Keep left rows having (or, if negated, lacking) a matching right row.
    /// `lhs` are expressions over the left input compared for equality with
    /// the right input's columns (IN); empty for EXISTS. `null_aware`
    /// implements NOT IN semantics.
    Semi { lhs: Vec<Expr>, negated: bool, null_aware: bool },
    /// Append a boolean column: EXISTS / IN / op ANY|ALL with 3-valued logic.
    Mark { kind: SubqueryKind },
    /// Append the single column of a scalar subquery (NULL when empty) and
    /// the number of rows it produced: more than one is an error, raised
    /// only where the value is used (so CASE can avoid it, as in PostgreSQL).
    Scalar,
    /// LATERAL join: append the right side's columns (cross/left).
    Lateral { left_outer: bool },
}

/// Logical relational operator.
#[derive(Clone, Debug)]
pub enum Rel {
    Get { id: ObjectId, name: String, columns: Vec<ColumnType>, keys: Vec<Vec<usize>> },
    Values { rows: Vec<Vec<Expr>>, columns: Vec<ColumnType> },
    /// Append computed columns (each may reference earlier appended ones).
    Map { input: Box<Rel>, exprs: Vec<Expr> },
    Filter { input: Box<Rel>, preds: Vec<Expr> },
    Project { input: Box<Rel>, cols: Vec<usize> },
    /// `on` refers to left columns followed by right columns.
    Join { left: Box<Rel>, right: Box<Rel>, kind: JoinKind, on: Vec<Expr> },
    /// Output: group columns, then one column per aggregate.
    Aggregate { input: Box<Rel>, group: Vec<usize>, aggs: Vec<AggCall> },
    Distinct { input: Box<Rel> },
    SetOp { op: SetOpKind, all: bool, left: Box<Rel>, right: Box<Rel> },
    UnionAll { inputs: Vec<Rel> },
    /// Per-group top-k by `order` (limit None = all rows after offset).
    TopK { input: Box<Rel>, group: Vec<usize>, order: Vec<SortKey>, limit: Option<u64>, offset: u64, with_ties: bool },
    /// Append one column per window function.
    Window { input: Box<Rel>, funcs: Vec<WindowCall> },
    /// Append the output columns of table functions evaluated per row; with
    /// several, their results are zipped (shorter ones padded with NULLs).
    /// (`ordinality`: the last output column numbers each row's results.)
    FlatMap { input: Box<Rel>, funcs: Vec<SrfCall>, out: Vec<ColumnType>, ordinality: bool },
    /// WITH RECURSIVE: `base` UNION [ALL] `step`, where `step` references the
    /// recursive relation through `RecRef { id }`.
    Recursive { id: u64, base: Box<Rel>, step: Box<Rel>, distinct: bool, columns: Vec<ColumnType> },
    RecRef { id: u64, columns: Vec<ColumnType> },
    /// Correlated subquery application (removed by decorrelation).
    Apply { left: Box<Rel>, right: Box<Rel>, kind: ApplyKind },
    Negate { input: Box<Rel> },
}

impl Rel {
    pub fn empty(columns: Vec<ColumnType>) -> Rel {
        Rel::Values { rows: vec![], columns }
    }

    /// A single row with no columns.
    pub fn unit() -> Rel {
        Rel::Values { rows: vec![vec![]], columns: vec![] }
    }

    pub fn map(self, exprs: Vec<Expr>) -> Rel {
        if exprs.is_empty() {
            return self;
        }
        Rel::Map { input: Box::new(self), exprs }
    }

    pub fn filter(self, preds: Vec<Expr>) -> Rel {
        let preds: Vec<Expr> = preds.into_iter().filter(|p| !p.is_true_literal()).collect();
        if preds.is_empty() {
            return self;
        }
        match self {
            Rel::Filter { input, preds: mut p0 } => {
                p0.extend(preds);
                Rel::Filter { input, preds: p0 }
            }
            other => Rel::Filter { input: Box::new(other), preds },
        }
    }

    pub fn project(self, cols: Vec<usize>) -> Rel {
        let arity = self.arity();
        if cols.len() == arity && cols.iter().enumerate().all(|(i, c)| i == *c) {
            return self;
        }
        match self {
            Rel::Project { input, cols: inner } => {
                Rel::Project { input, cols: cols.iter().map(|c| inner[*c]).collect() }
            }
            other => Rel::Project { input: Box::new(other), cols },
        }
    }

    /// Output column types.
    pub fn schema(&self) -> Vec<ColumnType> {
        match self {
            Rel::Get { columns, .. } | Rel::Values { columns, .. } | Rel::RecRef { columns, .. } => columns.clone(),
            Rel::Recursive { columns, .. } => columns.clone(),
            Rel::Map { input, exprs } => {
                let mut s = input.schema();
                for e in exprs {
                    let t = e.column_type(&s);
                    s.push(t);
                }
                s
            }
            Rel::Filter { input, .. } | Rel::Distinct { input } | Rel::TopK { input, .. } | Rel::Negate { input } => {
                input.schema()
            }
            Rel::Project { input, cols } => {
                let s = input.schema();
                cols.iter().map(|c| s[*c].clone()).collect()
            }
            Rel::Join { left, right, kind, .. } => {
                let mut l = left.schema();
                match kind {
                    JoinKind::Semi | JoinKind::Anti => l,
                    _ => {
                        let mut r = right.schema();
                        if matches!(kind, JoinKind::Right | JoinKind::Full) {
                            for c in l.iter_mut() {
                                c.nullable = true;
                            }
                        }
                        if matches!(kind, JoinKind::Left | JoinKind::Full) {
                            for c in r.iter_mut() {
                                c.nullable = true;
                            }
                        }
                        l.extend(r);
                        l
                    }
                }
            }
            Rel::Aggregate { input, group, aggs } => {
                let s = input.schema();
                let mut out: Vec<ColumnType> = group.iter().map(|g| s[*g].clone()).collect();
                out.extend(aggs.iter().map(|a| a.ret.clone()));
                out
            }
            Rel::SetOp { left, right, .. } => merge_nullability(left.schema(), &right.schema()),
            Rel::UnionAll { inputs } => {
                let mut s = inputs[0].schema();
                for i in &inputs[1..] {
                    s = merge_nullability(s, &i.schema());
                }
                s
            }
            Rel::Window { input, funcs } => {
                let mut s = input.schema();
                s.extend(funcs.iter().map(|f| f.ret.clone()));
                s
            }
            Rel::FlatMap { input, out, .. } => {
                let mut s = input.schema();
                s.extend(out.iter().cloned());
                s
            }
            Rel::Apply { left, right, kind } => {
                let mut s = left.schema();
                match kind {
                    ApplyKind::Semi { .. } => {}
                    ApplyKind::Mark { .. } => s.push(ColumnType::new(SqlType::Bool)),
                    ApplyKind::Scalar => {
                        let r = right.schema();
                        let mut c = r[0].clone();
                        c.nullable = true;
                        s.push(c);
                        s.push(ColumnType::new(SqlType::Int8));
                    }
                    ApplyKind::Lateral { left_outer } => {
                        let mut r = right.schema();
                        if *left_outer {
                            for c in r.iter_mut() {
                                c.nullable = true;
                            }
                        }
                        s.extend(r);
                    }
                }
                s
            }
        }
    }

    pub fn arity(&self) -> usize {
        match self {
            Rel::Get { columns, .. } | Rel::Values { columns, .. } | Rel::RecRef { columns, .. } => columns.len(),
            Rel::Recursive { columns, .. } => columns.len(),
            Rel::Map { input, exprs } => input.arity() + exprs.len(),
            Rel::Filter { input, .. } | Rel::Distinct { input } | Rel::TopK { input, .. } | Rel::Negate { input } => {
                input.arity()
            }
            Rel::Project { cols, .. } => cols.len(),
            Rel::Join { left, right, kind, .. } => match kind {
                JoinKind::Semi | JoinKind::Anti => left.arity(),
                _ => left.arity() + right.arity(),
            },
            Rel::Aggregate { group, aggs, .. } => group.len() + aggs.len(),
            Rel::SetOp { left, .. } => left.arity(),
            Rel::UnionAll { inputs } => inputs[0].arity(),
            Rel::Window { input, funcs } => input.arity() + funcs.len(),
            Rel::FlatMap { input, out, .. } => input.arity() + out.len(),
            Rel::Apply { left, right, kind } => {
                left.arity()
                    + match kind {
                        ApplyKind::Semi { .. } => 0,
                        ApplyKind::Mark { .. } => 1,
                        ApplyKind::Scalar => 2,
                        ApplyKind::Lateral { .. } => right.arity(),
                    }
            }
        }
    }

    pub fn inputs(&self) -> SmallVec<[&Rel; 2]> {
        match self {
            Rel::Get { .. } | Rel::Values { .. } | Rel::RecRef { .. } => smallvec![],
            Rel::Map { input, .. }
            | Rel::Filter { input, .. }
            | Rel::Project { input, .. }
            | Rel::Aggregate { input, .. }
            | Rel::Distinct { input }
            | Rel::TopK { input, .. }
            | Rel::Window { input, .. }
            | Rel::FlatMap { input, .. }
            | Rel::Negate { input } => smallvec![input.as_ref()],
            Rel::Join { left, right, .. } | Rel::SetOp { left, right, .. } | Rel::Apply { left, right, .. } => {
                smallvec![left.as_ref(), right.as_ref()]
            }
            Rel::UnionAll { inputs } => inputs.iter().collect(),
            Rel::Recursive { base, step, .. } => smallvec![base.as_ref(), step.as_ref()],
        }
    }

    /// This operator and its relational inputs, in preorder. Expression
    /// subqueries are separate query scopes and are not traversed.
    pub fn walk(&self) -> impl Iterator<Item = &Rel> {
        let mut pending = vec![self];
        std::iter::from_fn(move || {
            let rel = pending.pop()?;
            pending.extend(rel.inputs().into_iter().rev());
            Some(rel)
        })
    }

    /// Catalog relations read by the plan (including repeated references).
    pub fn reads(&self) -> impl Iterator<Item = ObjectId> {
        self.walk().filter_map(|rel| match rel {
            Rel::Get { id, .. } => Some(*id),
            _ => None,
        })
    }

    /// Replace now()-family calls everywhere by the clock reading `t`.
    pub fn bind_now(mut self, t: i64) -> Rel {
        for i in self.inputs_mut() {
            let r = std::mem::replace(i, Rel::empty(vec![]));
            *i = r.bind_now(t);
        }
        for e in self.exprs_mut() {
            if e.contains_now() {
                *e = e.bind_now(t);
            }
        }
        self
    }

    pub fn inputs_mut(&mut self) -> SmallVec<[&mut Rel; 2]> {
        match self {
            Rel::Get { .. } | Rel::Values { .. } | Rel::RecRef { .. } => smallvec![],
            Rel::Map { input, .. }
            | Rel::Filter { input, .. }
            | Rel::Project { input, .. }
            | Rel::Aggregate { input, .. }
            | Rel::Distinct { input }
            | Rel::TopK { input, .. }
            | Rel::Window { input, .. }
            | Rel::FlatMap { input, .. }
            | Rel::Negate { input } => smallvec![input.as_mut()],
            Rel::Join { left, right, .. } | Rel::SetOp { left, right, .. } | Rel::Apply { left, right, .. } => {
                smallvec![left.as_mut(), right.as_mut()]
            }
            Rel::UnionAll { inputs } => inputs.iter_mut().collect(),
            Rel::Recursive { base, step, .. } => smallvec![base.as_mut(), step.as_mut()],
        }
    }

    /// Transform each direct relational input, preserving this operator.
    pub fn map_inputs(mut self, mut f: impl FnMut(Rel) -> crate::error::Result<Rel>) -> crate::error::Result<Rel> {
        for input in self.inputs_mut() {
            *input = f(std::mem::replace(input, Rel::empty(vec![])))?;
        }
        Ok(self)
    }

    /// Expressions held directly by this operator (not children).
    pub fn exprs(&self) -> Vec<&Expr> {
        match self {
            Rel::Values { rows, .. } => rows.iter().flatten().collect(),
            Rel::Map { exprs, .. } => exprs.iter().collect(),
            Rel::Filter { preds, .. } => preds.iter().collect(),
            Rel::Join { on, .. } => on.iter().collect(),
            Rel::FlatMap { funcs, .. } => funcs.iter().flat_map(|f| f.args.iter()).collect(),
            Rel::Apply { kind: ApplyKind::Semi { lhs, .. }, .. } => lhs.iter().collect(),
            Rel::Apply { kind: ApplyKind::Mark { kind }, .. } => match kind {
                SubqueryKind::In(l) => l.iter().collect(),
                SubqueryKind::AnyAll { lhs, .. } => vec![lhs],
                _ => vec![],
            },
            _ => vec![],
        }
    }

    pub fn exprs_mut(&mut self) -> Vec<&mut Expr> {
        match self {
            Rel::Values { rows, .. } => rows.iter_mut().flatten().collect(),
            Rel::Map { exprs, .. } => exprs.iter_mut().collect(),
            Rel::Filter { preds, .. } => preds.iter_mut().collect(),
            Rel::Join { on, .. } => on.iter_mut().collect(),
            Rel::FlatMap { funcs, .. } => funcs.iter_mut().flat_map(|f| f.args.iter_mut()).collect(),
            Rel::Apply { kind: ApplyKind::Semi { lhs, .. }, .. } => lhs.iter_mut().collect(),
            Rel::Apply { kind: ApplyKind::Mark { kind }, .. } => match kind {
                SubqueryKind::In(l) => l.iter_mut().collect(),
                SubqueryKind::AnyAll { lhs, .. } => vec![lhs],
                _ => vec![],
            },
            _ => vec![],
        }
    }

    /// Search expressions across nested query scopes. The callback receives
    /// the depth of the starting scope; subqueries and Apply right inputs add one.
    pub fn any_expr_scoped(&self, depth: usize, f: &mut dyn FnMut(&Expr, usize) -> bool) -> bool {
        fn in_expr(e: &Expr, depth: usize, f: &mut dyn FnMut(&Expr, usize) -> bool) -> bool {
            f(e, depth)
                || matches!(e, Expr::Subquery(s) if s.rel.any_expr_scoped(depth + 1, f))
                || e.children().iter().any(|c| in_expr(c, depth, f))
        }
        let apply = matches!(self, Rel::Apply { .. });
        self.exprs().iter().any(|e| in_expr(e, depth, f))
            || self.inputs().iter().enumerate().any(|(i, input)| {
                input.any_expr_scoped(depth + usize::from(apply && i == 1), f)
            })
    }

    /// Mutate expressions with the same scope rules as `any_expr_scoped`.
    /// Returning false skips an expression's children, including its subquery.
    pub fn visit_exprs_scoped_mut(&mut self, depth: usize, f: &mut dyn FnMut(&mut Expr, usize) -> bool) {
        fn in_expr(e: &mut Expr, depth: usize, f: &mut dyn FnMut(&mut Expr, usize) -> bool) {
            if !f(e, depth) { return; }
            if let Expr::Subquery(s) = e {
                s.rel.visit_exprs_scoped_mut(depth + 1, f);
            }
            for c in e.children_mut() {
                in_expr(c, depth, f);
            }
        }
        for e in self.exprs_mut() {
            in_expr(e, depth, f);
        }
        let apply = matches!(self, Rel::Apply { .. });
        for (i, input) in self.inputs_mut().into_iter().enumerate() {
            input.visit_exprs_scoped_mut(depth + usize::from(apply && i == 1), f);
        }
    }

    /// Whether this plan references columns of the query `depth` levels out.
    pub fn has_outer_refs(&self, depth: usize) -> bool {
        self.any_expr_scoped(depth, &mut |e, depth| matches!(e, Expr::Outer(d, _, _) if *d == depth))
    }

    /// Indented plan rendering.
    pub fn explain(&self) -> String {
        let mut s = String::new();
        self.explain_into(&mut s, 0);
        s
    }

    fn explain_into(&self, out: &mut String, indent: usize) {
        let pad = "  ".repeat(indent);
        let line = match self {
            Rel::Get { name, .. } => format!("Get {name}"),
            Rel::Values { rows, .. } => format!("Values ({} rows)", rows.len()),
            Rel::Map { exprs, .. } => {
                format!("Map {}", exprs.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", "))
            }
            Rel::Filter { preds, .. } => {
                format!("Filter {}", preds.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(" AND "))
            }
            Rel::Project { cols, .. } => format!("Project {cols:?}"),
            Rel::Join { kind, on, .. } => format!(
                "Join {:?} on {}",
                kind,
                if on.is_empty() { "true".to_string() } else { on.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(" AND ") }
            ),
            Rel::Aggregate { group, aggs, .. } => format!(
                "Aggregate group={:?} aggs=[{}]",
                group,
                aggs.iter()
                    .map(|a| format!("{}{}({:?})", a.func.name(), if a.distinct { " distinct" } else { "" }, a.args))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Rel::Distinct { .. } => "Distinct".to_string(),
            Rel::SetOp { op, all, .. } => format!("{op:?}{}", if *all { " ALL" } else { "" }),
            Rel::UnionAll { .. } => "UnionAll".to_string(),
            Rel::TopK { group, order, limit, offset, .. } => {
                format!("TopK group={group:?} order={:?} limit={limit:?} offset={offset}", order.iter().map(|k| k.col).collect::<Vec<_>>())
            }
            Rel::Window { funcs, .. } => format!("Window {:?}", funcs.iter().map(|f| f.func).collect::<Vec<_>>()),
            Rel::FlatMap { funcs, .. } => format!("FlatMap {:?}", funcs.iter().map(|f| f.func).collect::<Vec<_>>()),
            Rel::Recursive { id, distinct, .. } => format!("Recursive #{id} distinct={distinct}"),
            Rel::RecRef { id, .. } => format!("RecRef #{id}"),
            Rel::Apply { kind, .. } => format!("Apply {kind:?}"),
            Rel::Negate { .. } => "Negate".to_string(),
        };
        let _ = writeln!(out, "{pad}{line}");
        for i in self.inputs() {
            i.explain_into(out, indent + 1);
        }
    }
}

/// How many copies of the expressions it inlines an expression inlined by
/// `Inliner` may hold, by size.
pub const INLINE_COPIES: u64 = 4;

/// Inlines the computed columns of a `Map` into an expression over its
/// output (a predicate pushed below it, an expression over its result):
/// each reference to a computed column is replaced by the column's
/// expression, with the computed columns that one references inlined in
/// turn. A copy per reference can grow exponentially — each level of
/// `SELECT x + x AS x FROM (...)` doubles `x` — so an expression is inlined
/// only if that makes it larger by at most `INLINE_COPIES` times the size of
/// the expressions it inlines. Inlining through any number of maps then adds
/// at most that multiple of their size: expressions stay linear in the size
/// of the plan, and so does the work of inlining them.
pub struct Inliner<'a> {
    /// Arity of the map's input (columns that are not inlined).
    n: usize,
    exprs: &'a [Expr],
    /// Per computed column: its size with the computed columns it references
    /// inlined (saturating).
    sizes: Vec<u64>,
    /// Per computed column: whether a copy of it has the same value (no
    /// non-immutable function, `now()`, subquery or outer reference, also in
    /// the computed columns it references).
    stable: Vec<bool>,
}

impl<'a> Inliner<'a> {
    /// For a map of `exprs` over an input of arity `n` (an expression may
    /// reference the ones before it).
    pub fn new(n: usize, exprs: &'a [Expr]) -> Inliner<'a> {
        let mut sizes = Vec::with_capacity(exprs.len());
        let mut stable: Vec<bool> = Vec::with_capacity(exprs.len());
        for e in exprs {
            let ok = e.is_deterministic()
                && !e.contains_now()
                && !e.has_subquery()
                && !e.has_outer()
                && e.column_set().iter().all(|&c| c < n || stable[c - n]);
            sizes.push(inlined_size(e, n, &sizes));
            stable.push(ok);
        }
        Inliner { n, exprs, sizes, stable }
    }

    /// Are the computed columns `e` references stable (may be copied)?
    pub fn stable(&self, e: &Expr) -> bool {
        e.column_set().iter().all(|&c| c < self.n || self.stable[c - self.n])
    }

    /// `e` with the computed columns inlined, or None if that would exceed
    /// the bound. (Stability is the caller's concern.)
    pub fn inline(&self, e: &Expr) -> Option<Expr> {
        // the expressions inlined: the computed columns e references,
        // directly or through other computed columns
        let mut need = vec![false; self.exprs.len()];
        for c in e.column_set() {
            if c >= self.n {
                need[c - self.n] = true;
            }
        }
        let mut bound = expr_size(e);
        for i in (0..self.exprs.len()).rev() {
            if need[i] {
                bound = bound.saturating_add(INLINE_COPIES.saturating_mul(expr_size(&self.exprs[i])));
                for c in self.exprs[i].column_set() {
                    if c >= self.n {
                        need[c - self.n] = true;
                    }
                }
            }
        }
        if inlined_size(e, self.n, &self.sizes) > bound {
            return None;
        }
        let mut out = e.clone();
        self.inline_into(&mut out);
        Some(out)
    }

    fn inline_into(&self, e: &mut Expr) {
        while let Expr::Column(c) = *e
            && c >= self.n
        {
            *e = self.exprs[c - self.n].clone();
        }
        for x in e.children_mut() {
            self.inline_into(x);
        }
    }
}

/// Number of nodes of `e` (saturating).
fn expr_size(e: &Expr) -> u64 {
    e.children().iter().fold(1, |s, x| s.saturating_add(expr_size(x)))
}

/// Size of `e` with the columns from `n` on (`sizes[c - n]` large) inlined.
fn inlined_size(e: &Expr, n: usize, sizes: &[u64]) -> u64 {
    match e {
        Expr::Column(c) if *c >= n => sizes[c - n],
        _ => e.children().iter().fold(1, |s, x| s.saturating_add(inlined_size(x, n, sizes))),
    }
}

fn merge_nullability(mut a: Vec<ColumnType>, b: &[ColumnType]) -> Vec<ColumnType> {
    for (x, y) in a.iter_mut().zip(b) {
        x.nullable |= y.nullable;
        if x.typmod != y.typmod {
            x.typmod = -1;
        }
    }
    a
}
