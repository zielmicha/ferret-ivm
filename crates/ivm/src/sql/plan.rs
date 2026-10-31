//! Logical plan: relational algebra over bound expressions.

use crate::expr::{Expr, SubqueryKind};
use crate::types::encoding::SortSpec;
use crate::types::{ColumnType, SqlType};
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
pub struct SortKey<C = usize> {
    pub col: C,
    pub spec: SortSpec,
}

#[derive(Clone, Debug)]
pub struct AggCall<C = usize> {
    pub func: AggFunc,
    /// Argument columns of the input.
    pub args: Vec<C>,
    pub distinct: bool,
    /// Boolean input column for FILTER (WHERE ...).
    pub filter: Option<C>,
    /// ORDER BY within the aggregate (input columns).
    pub order: Vec<SortKey<C>>,
    /// Constant direct argument (percentile fraction).
    pub fraction: Option<f64>,
    pub ret: ColumnType,
}

impl<C: Copy> AggCall<C> {
    pub fn columns(&self) -> impl Iterator<Item = C> + '_ {
        self.args.iter().copied().chain(self.filter).chain(self.order.iter().map(|k| k.col))
    }

    pub fn remap(&mut self, f: &impl Fn(C) -> C) {
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
pub struct WindowCall<C = usize> {
    pub func: WindowFunc,
    pub args: Vec<C>,
    pub partition: Vec<C>,
    pub order: Vec<SortKey<C>>,
    pub frame: Frame,
    pub ret: ColumnType,
    pub range: Option<RangeOffsets>,
}

impl<C: Copy> WindowCall<C> {
    pub fn columns(&self) -> impl Iterator<Item = C> + '_ {
        self.args.iter().chain(&self.partition).copied().chain(self.order.iter().map(|k| k.col))
    }

    pub fn remap(&mut self, f: &impl Fn(C) -> C) {
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
    JsonbPathQuery {
        tz: bool,
    },
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
// Both planner phases use the same operator shape and semantics. Concrete
// expansion keeps recursive Clone/Debug derives finite (a recursive associated
// Input type would make their trait bounds overflow).
macro_rules! relational_node {
    ($name:ident, $column:ty, $input:ty) => {
        #[derive(Clone, Debug)]
        pub enum $name {
            Get {
                id: ObjectId,
                name: String,
                columns: Vec<ColumnType>,
                keys: Vec<Vec<usize>>,
            },
            Values {
                rows: Vec<Vec<Expr>>,
                columns: Vec<ColumnType>,
            },
            /// Append computed columns (each may reference earlier appended ones).
            Map {
                input: Box<$input>,
                exprs: Vec<Expr>,
            },
            Filter {
                input: Box<$input>,
                preds: Vec<Expr>,
            },
            Project {
                input: Box<$input>,
                cols: Vec<$column>,
            },
            /// `on` refers to left columns followed by right columns.
            Join {
                left: Box<$input>,
                right: Box<$input>,
                kind: JoinKind,
                on: Vec<Expr>,
            },
            /// Output: group columns, then one column per aggregate.
            Aggregate {
                input: Box<$input>,
                group: Vec<$column>,
                aggs: Vec<AggCall<$column>>,
            },
            Distinct {
                input: Box<$input>,
            },
            SetOp {
                op: SetOpKind,
                all: bool,
                left: Box<$input>,
                right: Box<$input>,
            },
            UnionAll {
                inputs: Vec<$input>,
            },
            /// Per-group top-k by `order` (limit None = all rows after offset).
            TopK {
                input: Box<$input>,
                group: Vec<$column>,
                order: Vec<SortKey<$column>>,
                limit: Option<u64>,
                offset: u64,
                with_ties: bool,
            },
            /// Append one column per window function.
            Window {
                input: Box<$input>,
                funcs: Vec<WindowCall<$column>>,
            },
            /// Append the output columns of table functions evaluated per row; with
            /// several, their results are zipped (shorter ones padded with NULLs).
            /// (`ordinality`: the last output column numbers each row's results.)
            FlatMap {
                input: Box<$input>,
                funcs: Vec<SrfCall>,
                out: Vec<ColumnType>,
                ordinality: bool,
            },
            /// WITH RECURSIVE: `base` UNION [ALL] `step`, where `step` references the
            /// recursive relation through `RecRef { id }`.
            Recursive {
                id: u64,
                base: Box<$input>,
                step: Box<$input>,
                distinct: bool,
                columns: Vec<ColumnType>,
            },
            RecRef {
                id: u64,
                columns: Vec<ColumnType>,
            },
            /// Correlated subquery application (removed by decorrelation).
            Apply {
                left: Box<$input>,
                right: Box<$input>,
                kind: ApplyKind,
            },
            Negate {
                input: Box<$input>,
            },
        }
        impl $name {
            pub fn schema_with(&self, expression_type: impl Fn(&Expr, &[ColumnType]) -> ColumnType) -> Vec<ColumnType> {
                match self {
                    Self::Get { columns, .. } | Self::Values { columns, .. } | Self::RecRef { columns, .. } => columns.clone(),
                    Self::Recursive { columns, .. } => columns.clone(),
                    Self::Map { input, exprs } => {
                        let mut s = input.schema();
                        for e in exprs {
                            let t = expression_type(e, &s);
                            s.push(t);
                        }
                        s
                    }
                    Self::Filter { input, .. } | Self::Distinct { input } | Self::TopK { input, .. } | Self::Negate { input } => {
                        input.schema()
                    }
                    Self::Project { input, cols } => {
                        let s = input.schema();
                        cols.iter().map(|c| s[input.column_offset(*c)].clone()).collect()
                    }
                    Self::Join { left, right, kind, .. } => {
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
                    Self::Aggregate { input, group, aggs } => {
                        let s = input.schema();
                        let mut out: Vec<ColumnType> = group.iter().map(|g| s[input.column_offset(*g)].clone()).collect();
                        out.extend(aggs.iter().map(|a| a.ret.clone()));
                        out
                    }
                    Self::SetOp { left, right, .. } => $crate::sql::plan::merge_nullability(left.schema(), &right.schema()),
                    Self::UnionAll { inputs } => {
                        let mut s = inputs[0].schema();
                        for i in &inputs[1..] {
                            s = $crate::sql::plan::merge_nullability(s, &i.schema());
                        }
                        s
                    }
                    Self::Window { input, funcs } => {
                        let mut s = input.schema();
                        s.extend(funcs.iter().map(|f| f.ret.clone()));
                        s
                    }
                    Self::FlatMap { input, out, .. } => {
                        let mut s = input.schema();
                        s.extend(out.iter().cloned());
                        s
                    }
                    Self::Apply { left, right, kind } => {
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
            pub fn inputs(&self) -> smallvec::SmallVec<[&$input; 2]> {
                match self {
                    Self::Get { .. } | Self::Values { .. } | Self::RecRef { .. } => smallvec::smallvec![],
                    Self::Map { input, .. }
                    | Self::Filter { input, .. }
                    | Self::Project { input, .. }
                    | Self::Aggregate { input, .. }
                    | Self::Distinct { input }
                    | Self::TopK { input, .. }
                    | Self::Window { input, .. }
                    | Self::FlatMap { input, .. }
                    | Self::Negate { input } => smallvec::smallvec![input.as_ref()],
                    Self::Join { left, right, .. } | Self::SetOp { left, right, .. } | Self::Apply { left, right, .. } => {
                        smallvec::smallvec![left.as_ref(), right.as_ref()]
                    }
                    Self::UnionAll { inputs } => inputs.iter().collect(),
                    Self::Recursive { base, step, .. } => smallvec::smallvec![base.as_ref(), step.as_ref()],
                }
            }
            pub fn inputs_mut(&mut self) -> smallvec::SmallVec<[&mut $input; 2]> {
                match self {
                    Self::Get { .. } | Self::Values { .. } | Self::RecRef { .. } => smallvec::smallvec![],
                    Self::Map { input, .. }
                    | Self::Filter { input, .. }
                    | Self::Project { input, .. }
                    | Self::Aggregate { input, .. }
                    | Self::Distinct { input }
                    | Self::TopK { input, .. }
                    | Self::Window { input, .. }
                    | Self::FlatMap { input, .. }
                    | Self::Negate { input } => smallvec::smallvec![input.as_mut()],
                    Self::Join { left, right, .. } | Self::SetOp { left, right, .. } | Self::Apply { left, right, .. } => {
                        smallvec::smallvec![left.as_mut(), right.as_mut()]
                    }
                    Self::UnionAll { inputs } => inputs.iter_mut().collect(),
                    Self::Recursive { base, step, .. } => smallvec::smallvec![base.as_mut(), step.as_mut()],
                }
            }
            pub fn exprs(&self) -> Vec<&Expr> {
                match self {
                    Self::Values { rows, .. } => rows.iter().flatten().collect(),
                    Self::Map { exprs, .. } => exprs.iter().collect(),
                    Self::Filter { preds, .. } => preds.iter().collect(),
                    Self::Join { on, .. } => on.iter().collect(),
                    Self::FlatMap { funcs, .. } => funcs.iter().flat_map(|f| f.args.iter()).collect(),
                    Self::Apply { kind: ApplyKind::Semi { lhs, .. }, .. } => lhs.iter().collect(),
                    Self::Apply { kind: ApplyKind::Mark { kind }, .. } => match kind {
                        SubqueryKind::In(l) => l.iter().collect(),
                        SubqueryKind::AnyAll { lhs, .. } => vec![lhs],
                        _ => vec![],
                    },
                    _ => vec![],
                }
            }
            pub fn exprs_mut(&mut self) -> Vec<&mut Expr> {
                match self {
                    Self::Values { rows, .. } => rows.iter_mut().flatten().collect(),
                    Self::Map { exprs, .. } => exprs.iter_mut().collect(),
                    Self::Filter { preds, .. } => preds.iter_mut().collect(),
                    Self::Join { on, .. } => on.iter_mut().collect(),
                    Self::FlatMap { funcs, .. } => funcs.iter_mut().flat_map(|f| f.args.iter_mut()).collect(),
                    Self::Apply { kind: ApplyKind::Semi { lhs, .. }, .. } => lhs.iter_mut().collect(),
                    Self::Apply { kind: ApplyKind::Mark { kind }, .. } => match kind {
                        SubqueryKind::In(l) => l.iter_mut().collect(),
                        SubqueryKind::AnyAll { lhs, .. } => vec![lhs],
                        _ => vec![],
                    },
                    _ => vec![],
                }
            }
        }
    };
}
pub(crate) use relational_node;

relational_node!(Rel, usize, Rel);

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
            Rel::Project { input, cols: inner } => Rel::Project { input, cols: cols.iter().map(|c| inner[*c]).collect() },
            other => Rel::Project { input: Box::new(other), cols },
        }
    }

    /// Output column types.
    pub fn schema(&self) -> Vec<ColumnType> {
        self.schema_with(|e, schema| e.column_type(schema))
    }

    pub fn column_offset(&self, column: usize) -> usize {
        column
    }

    pub fn arity(&self) -> usize {
        match self {
            Rel::Get { columns, .. } | Rel::Values { columns, .. } | Rel::RecRef { columns, .. } => columns.len(),
            Rel::Recursive { columns, .. } => columns.len(),
            Rel::Map { input, exprs } => input.arity() + exprs.len(),
            Rel::Filter { input, .. } | Rel::Distinct { input } | Rel::TopK { input, .. } | Rel::Negate { input } => input.arity(),
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
            || self.inputs().iter().enumerate().any(|(i, input)| input.any_expr_scoped(depth + usize::from(apply && i == 1), f))
    }

    /// Mutate expressions with the same scope rules as `any_expr_scoped`.
    /// Returning false skips an expression's children, including its subquery.
    pub fn visit_exprs_scoped_mut(&mut self, depth: usize, f: &mut dyn FnMut(&mut Expr, usize) -> bool) {
        fn in_expr(e: &mut Expr, depth: usize, f: &mut dyn FnMut(&mut Expr, usize) -> bool) {
            if !f(e, depth) {
                return;
            }
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

pub(crate) fn merge_nullability(mut a: Vec<ColumnType>, b: &[ColumnType]) -> Vec<ColumnType> {
    for (x, y) in a.iter_mut().zip(b) {
        x.nullable |= y.nullable;
        if x.typmod != y.typmod {
            x.typmod = -1;
        }
    }
    a
}
