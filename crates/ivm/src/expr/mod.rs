//! Scalar expressions: bound, typed, and evaluated with PostgreSQL semantics.

pub mod cast;
pub mod eval;
pub mod func;
mod builtins;
mod ops;

pub use eval::EvalScratch;
pub use func::{registry, FuncDef, FuncId, Volatility};
pub use builtins::{jsonpath, path_context, with_session_settings};

/// to_jsonb conversion of a datum.
pub fn builtins_datum_to_jsonb(d: &Datum) -> crate::error::Result<pgfuncs::json::Jsonb> {
    builtins::datum_to_jsonb(d)
}

/// `json` (not jsonb) text rendering of a jsonb value.
pub fn json_text(j: &pgfuncs::json::Jsonb) -> String {
    builtins::json_text(j)
}

/// Text form of any datum (as used by `||` and concat()).
pub fn ops_as_text(d: &Datum) -> String {
    ops::as_text(d).into_owned()
}

pub fn ops_overflow() -> crate::error::SqlError {
    ops::overflow()
}

/// Compiled regular expression from the per-thread cache.
pub fn regex_cached(pattern: &str, flags: &str) -> crate::error::Result<Arc<pgfuncs::pattern::Regex>> {
    ops::regex(pattern, flags)
}

use crate::types::codec::encode_datum;
use crate::types::{ColumnType, Datum, SqlType};
use std::fmt;
use std::sync::Arc;

/// IS [NOT] TRUE/FALSE/UNKNOWN
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BoolTest {
    IsTrue,
    IsNotTrue,
    IsFalse,
    IsNotFalse,
    IsUnknown,
    IsNotUnknown,
}

#[derive(Clone, Debug)]
pub struct Call {
    pub func: FuncId,
    pub args: Vec<Expr>,
    pub ret: SqlType,
}

#[derive(Clone, Debug)]
pub struct CaseExpr {
    pub whens: Vec<(Expr, Expr)>,
    pub else_: Expr,
    pub ty: SqlType,
}

#[derive(Clone)]
pub struct LikeExpr {
    pub expr: Expr,
    pub pattern: Expr,
    pub escape: Option<char>,
    pub case_insensitive: bool,
    pub negated: bool,
    pub bytea: bool,
    /// Precompiled pattern when `pattern` is a literal.
    pub compiled: Option<Arc<pgfuncs::pattern::LikePattern>>,
}

impl fmt::Debug for LikeExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Like")
            .field("expr", &self.expr)
            .field("pattern", &self.pattern)
            .field("ci", &self.case_insensitive)
            .field("negated", &self.negated)
            .finish()
    }
}

/// A bound scalar expression over an input row.
#[derive(Clone, Debug)]
pub enum Expr {
    Column(usize),
    Literal(Datum, SqlType),
    Call(Box<Call>),
    /// Cast to type with typmod; `explicit` controls truncation semantics.
    Cast(Box<Expr>, SqlType, i32, bool),
    Not(Box<Expr>),
    And(Vec<Expr>),
    Or(Vec<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    BoolTest(Box<Expr>, BoolTest),
    /// a IS [NOT] DISTINCT FROM b (operands already coerced to a common type)
    IsDistinct(Box<Expr>, Box<Expr>, bool),
    Case(Box<CaseExpr>),
    Coalesce(Vec<Expr>, SqlType),
    NullIf(Box<Expr>, Box<Expr>),
    Greatest(Vec<Expr>, SqlType),
    Least(Vec<Expr>, SqlType),
    /// expr [NOT] IN (list), all coerced to a common type
    InList(Box<Expr>, Vec<Expr>, bool),
    Like(Box<LikeExpr>),
    /// ARRAY[...]
    Array(Vec<Expr>, SqlType),
    /// array[i]
    Subscript(Box<Expr>, Box<Expr>, SqlType),
    /// expr op ANY/ALL (array): op is a resolved comparison function
    AnyAll { expr: Box<Expr>, array: Box<Expr>, func: FuncId, all: bool },
    /// Error raised when evaluated (e.g. scalar subquery returned >1 row).
    Error(String, String),
    // ---- binder-internal placeholders, removed before execution ----
    /// Column `col` of the query `depth` levels out (correlated reference).
    Outer(usize, usize, SqlType),
    /// Subquery used as an expression.
    Subquery(Box<SubqueryExpr>),
    /// Reference to the k-th aggregate of the enclosing SELECT.
    AggRef(usize, SqlType),
    /// Reference to the k-th window function of the enclosing SELECT.
    WindowRef(usize, SqlType),
    /// Placeholder for a select-list set-returning function (binder only).
    SrfRef(usize, SqlType),
}

#[derive(Clone, Debug)]
pub enum SubqueryKind {
    Exists,
    Scalar,
    /// lhs IN (subquery): lhs expressions vs the subquery's columns
    In(Vec<Expr>),
    /// lhs op ANY/ALL (subquery) with a resolved comparison function
    AnyAll { lhs: Box<Expr>, func: FuncId, all: bool },
}

#[derive(Clone, Debug)]
pub struct SubqueryExpr {
    pub kind: SubqueryKind,
    pub rel: crate::sql::plan::Rel,
    pub ty: SqlType,
}

impl Expr {
    pub fn lit(d: Datum, ty: SqlType) -> Expr {
        Expr::Literal(d, ty)
    }
    pub fn null(ty: SqlType) -> Expr {
        Expr::Literal(Datum::Null, ty)
    }
    pub fn bool(b: bool) -> Expr {
        Expr::Literal(Datum::Bool(b), SqlType::Bool)
    }
    pub fn col(i: usize) -> Expr {
        Expr::Column(i)
    }

    pub fn is_literal(&self) -> bool {
        matches!(self, Expr::Literal(..))
    }

    pub fn as_column(&self) -> Option<usize> {
        match self {
            Expr::Column(i) => Some(*i),
            _ => None,
        }
    }

    pub fn is_true_literal(&self) -> bool {
        matches!(self, Expr::Literal(Datum::Bool(true), _))
    }

    /// Result type given the input schema.
    pub fn ty(&self, input: &[ColumnType]) -> SqlType {
        match self {
            Expr::Column(i) => input.get(*i).map(|c| c.ty.clone()).unwrap_or(SqlType::Unknown),
            Expr::Literal(_, t) => t.clone(),
            Expr::Call(c) => c.ret.clone(),
            Expr::Cast(_, t, _, _) => t.clone(),
            Expr::Not(_)
            | Expr::And(_)
            | Expr::Or(_)
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::BoolTest(..)
            | Expr::IsDistinct(..)
            | Expr::InList(..)
            | Expr::Like(_)
            | Expr::AnyAll { .. } => SqlType::Bool,
            Expr::Case(c) => c.ty.clone(),
            Expr::Coalesce(_, t) | Expr::Greatest(_, t) | Expr::Least(_, t) => t.clone(),
            Expr::NullIf(a, _) => a.ty(input),
            Expr::Array(_, elem) => elem.clone().array_of(),
            Expr::Subscript(_, _, t) => t.clone(),
            Expr::Error(..) => SqlType::Unknown,
            Expr::Outer(_, _, t) | Expr::AggRef(_, t) | Expr::WindowRef(_, t) | Expr::SrfRef(_, t) => t.clone(),
            Expr::Subquery(s) => s.ty.clone(),
        }
    }

    /// Type modifier of the result where it is statically known.
    pub fn typmod(&self, input: &[ColumnType]) -> i32 {
        match self {
            Expr::Column(i) => input.get(*i).map(|c| c.typmod).unwrap_or(-1),
            Expr::Cast(_, _, tm, _) => *tm,
            _ => -1,
        }
    }

    pub fn column_type(&self, input: &[ColumnType]) -> ColumnType {
        ColumnType { ty: self.ty(input), typmod: self.typmod(input), nullable: self.nullable(input) }
    }

    /// Conservative nullability.
    pub fn nullable(&self, input: &[ColumnType]) -> bool {
        match self {
            Expr::Column(i) => input.get(*i).map(|c| c.nullable).unwrap_or(true),
            Expr::Literal(d, _) => d.is_null(),
            Expr::IsNull(_) | Expr::IsNotNull(_) | Expr::BoolTest(..) | Expr::IsDistinct(..) => false,
            Expr::Cast(e, ..) | Expr::Not(e) => e.nullable(input),
            Expr::Call(c) => {
                let f = &registry().funcs[c.func.0 as usize];
                !f.strict || f.can_return_null || c.args.iter().any(|a| a.nullable(input))
            }
            Expr::Coalesce(args, _) => args.iter().all(|a| a.nullable(input)),
            Expr::And(args) | Expr::Or(args) => args.iter().any(|a| a.nullable(input)),
            Expr::Case(c) => c.whens.iter().any(|(_, t)| t.nullable(input)) || c.else_.nullable(input),
            Expr::Greatest(args, _) | Expr::Least(args, _) => args.iter().all(|a| a.nullable(input)),
            Expr::InList(e, list, _) => e.nullable(input) || list.iter().any(|a| a.nullable(input)),
            Expr::Like(l) => l.expr.nullable(input) || l.pattern.nullable(input),
            Expr::Array(..) => false,
            _ => true,
        }
    }

    /// Visit direct children.
    pub fn children(&self) -> Vec<&Expr> {
        match self {
            Expr::Column(_)
            | Expr::Literal(..)
            | Expr::Error(..)
            | Expr::Outer(..)
            | Expr::AggRef(..)
            | Expr::WindowRef(..)
            | Expr::SrfRef(..) => vec![],
            Expr::Subquery(s) => match &s.kind {
                SubqueryKind::In(lhs) => lhs.iter().collect(),
                SubqueryKind::AnyAll { lhs, .. } => vec![lhs],
                _ => vec![],
            },
            Expr::Call(c) => c.args.iter().collect(),
            Expr::Cast(e, ..) | Expr::Not(e) | Expr::IsNull(e) | Expr::IsNotNull(e) | Expr::BoolTest(e, _) => vec![e],
            Expr::And(v) | Expr::Or(v) | Expr::Coalesce(v, _) | Expr::Greatest(v, _) | Expr::Least(v, _) => {
                v.iter().collect()
            }
            Expr::Array(v, _) => v.iter().collect(),
            Expr::IsDistinct(a, b, _) | Expr::NullIf(a, b) | Expr::Subscript(a, b, _) => vec![a, b],
            Expr::Case(c) => {
                let mut v = Vec::new();
                for (w, t) in &c.whens {
                    v.push(w);
                    v.push(t);
                }
                v.push(&c.else_);
                v
            }
            Expr::InList(e, l, _) => {
                let mut v = vec![&**e];
                v.extend(l.iter());
                v
            }
            Expr::Like(l) => vec![&l.expr, &l.pattern],
            Expr::AnyAll { expr, array, .. } => vec![expr, array],
        }
    }

    pub fn children_mut(&mut self) -> Vec<&mut Expr> {
        match self {
            Expr::Column(_)
            | Expr::Literal(..)
            | Expr::Error(..)
            | Expr::Outer(..)
            | Expr::AggRef(..)
            | Expr::WindowRef(..)
            | Expr::SrfRef(..) => vec![],
            Expr::Subquery(s) => match &mut s.kind {
                SubqueryKind::In(lhs) => lhs.iter_mut().collect(),
                SubqueryKind::AnyAll { lhs, .. } => vec![lhs],
                _ => vec![],
            },
            Expr::Call(c) => c.args.iter_mut().collect(),
            Expr::Cast(e, ..) | Expr::Not(e) | Expr::IsNull(e) | Expr::IsNotNull(e) | Expr::BoolTest(e, _) => vec![e],
            Expr::And(v) | Expr::Or(v) | Expr::Coalesce(v, _) | Expr::Greatest(v, _) | Expr::Least(v, _) => {
                v.iter_mut().collect()
            }
            Expr::Array(v, _) => v.iter_mut().collect(),
            Expr::IsDistinct(a, b, _) | Expr::NullIf(a, b) | Expr::Subscript(a, b, _) => vec![a, b],
            Expr::Case(c) => {
                let mut v = Vec::new();
                for (w, t) in c.whens.iter_mut() {
                    v.push(w);
                    v.push(t);
                }
                v.push(&mut c.else_);
                v
            }
            Expr::InList(e, l, _) => {
                let mut v = vec![&mut **e];
                v.extend(l.iter_mut());
                v
            }
            Expr::Like(l) => vec![&mut l.expr, &mut l.pattern],
            Expr::AnyAll { expr, array, .. } => vec![expr, array],
        }
    }

    /// Columns referenced.
    pub fn columns(&self, out: &mut Vec<usize>) {
        if let Expr::Column(i) = self {
            out.push(*i);
        }
        for c in self.children() {
            c.columns(out);
        }
    }

    pub fn column_set(&self) -> Vec<usize> {
        let mut v = Vec::new();
        self.columns(&mut v);
        v.sort();
        v.dedup();
        v
    }

    pub fn max_column(&self) -> Option<usize> {
        self.column_set().last().copied()
    }

    /// Rewrite column references through `f`.
    pub fn remap(&mut self, f: &dyn Fn(usize) -> usize) {
        if let Expr::Column(i) = self {
            *i = f(*i);
            return;
        }
        for c in self.children_mut() {
            c.remap(f);
        }
    }

    pub fn remapped(&self, f: &dyn Fn(usize) -> usize) -> Expr {
        let mut e = self.clone();
        e.remap(f);
        e
    }

    /// Substitute matching subexpressions, without visiting their replacements
    /// or crossing into subquery plans. Clone the original tree only once.
    pub fn rewritten(&self, f: &mut dyn FnMut(&Expr) -> Option<Expr>) -> Expr {
        let mut e = self.clone();
        e.rewrite(f);
        e
    }

    pub fn rewrite(&mut self, f: &mut dyn FnMut(&Expr) -> Option<Expr>) {
        if let Some(e) = f(self) {
            *self = e;
        } else {
            for c in self.children_mut() {
                c.rewrite(f);
            }
        }
    }

    /// Replace column references by expressions (substitution).
    pub fn substitute(&self, cols: &[Expr]) -> Expr {
        self.rewritten(&mut |e| e.as_column().map(|i| cols[i].clone()))
    }

    /// Whether the expression contains binder placeholders of the given kinds.
    pub fn contains(&self, pred: &dyn Fn(&Expr) -> bool) -> bool {
        pred(self) || self.children().iter().any(|c| c.contains(pred))
    }

    pub fn has_subquery(&self) -> bool {
        self.contains(&|e| matches!(e, Expr::Subquery(_)))
    }

    pub fn has_outer(&self) -> bool {
        self.contains(&|e| match e {
            Expr::Outer(..) => true,
            Expr::Subquery(s) => s.rel.has_outer_refs(1),
            _ => false,
        })
    }

    /// Whether evaluation is deterministic (no volatile/stable functions).
    pub fn is_deterministic(&self) -> bool {
        !self.contains(&|e| matches!(e, Expr::Call(c) if registry().funcs[c.func.0 as usize].volatility != Volatility::Immutable))
    }

    /// Names of non-immutable functions used.
    pub fn nondeterministic_functions(&self, out: &mut Vec<&'static str>) {
        if let Expr::Call(c) = self {
            let f = &registry().funcs[c.func.0 as usize];
            if f.volatility != Volatility::Immutable {
                out.push(f.name);
            }
        }
        for c in self.children() {
            c.nondeterministic_functions(out);
        }
    }

    /// `e * k` over int4.
    pub fn mul_int(e: Expr, k: i32) -> Expr {
        Expr::int4_op("*", e, Expr::Literal(Datum::Int4(k), SqlType::Int4))
    }

    /// `a + b` over int4.
    pub fn add_int(a: Expr, b: Expr) -> Expr {
        Expr::int4_op("+", a, b)
    }

    fn int4_op(op: &str, a: Expr, b: Expr) -> Expr {
        let r = func::resolve(op, &[SqlType::Int4, SqlType::Int4]).expect("int4 operator");
        Expr::Call(Box::new(Call { func: r.func, args: vec![a, b], ret: SqlType::Int4 }))
    }

    /// Which clock reading a call to the now() family returns.
    pub fn now_kind(&self) -> Option<NowKind> {
        let Expr::Call(c) = self else { return None };
        if !c.args.is_empty() {
            return None;
        }
        match registry().funcs[c.func.0 as usize].name {
            "now" | "current_timestamp" | "transaction_timestamp" | "statement_timestamp" => Some(NowKind::TimestampTz),
            "localtimestamp" => Some(NowKind::Timestamp),
            "current_date" => Some(NowKind::Date),
            _ => None,
        }
    }

    pub fn contains_now(&self) -> bool {
        self.now_kind().is_some() || self.children().iter().any(|c| c.contains_now())
    }

    /// Replace now()-family calls by `f(kind)`.
    pub fn replace_now(&self, f: &dyn Fn(NowKind) -> Expr) -> Expr {
        self.rewritten(&mut |e| e.now_kind().map(f))
    }

    /// Replace now()-family calls by the clock reading `t` (timestamptz
    /// microseconds since 2000-01-01, session time zone UTC).
    pub fn bind_now(&self, t: i64) -> Expr {
        self.replace_now(&|k| now_literal(k, t))
    }

    /// Split a predicate into conjuncts.
    pub fn conjuncts(self) -> Vec<Expr> {
        match self {
            Expr::And(v) => v.into_iter().flat_map(|e| e.conjuncts()).collect(),
            Expr::Literal(Datum::Bool(true), _) => vec![],
            e => vec![e],
        }
    }

    pub fn and_all(mut preds: Vec<Expr>) -> Expr {
        match preds.len() {
            0 => Expr::bool(true),
            1 => preds.pop().unwrap(),
            _ => Expr::And(preds),
        }
    }

    /// Deterministic byte fingerprint used for plan hash-consing.
    pub fn fingerprint(&self, out: &mut Vec<u8>) {
        fn tag(out: &mut Vec<u8>, t: u8, n: usize) {
            out.push(t);
            out.extend_from_slice(&(n as u32).to_le_bytes());
        }
        match self {
            Expr::Column(i) => tag(out, 1, *i),
            Expr::Literal(d, t) => {
                tag(out, 2, 0);
                crate::types::codec::encode_type(t, out);
                encode_datum(d, out);
            }
            Expr::Call(c) => {
                tag(out, 3, c.func.0 as usize);
                tag(out, 0, c.args.len());
            }
            Expr::Cast(_, t, tm, explicit) => {
                tag(out, 4, *tm as usize);
                out.push(*explicit as u8);
                crate::types::codec::encode_type(t, out);
            }
            Expr::Not(_) => tag(out, 5, 0),
            Expr::And(v) => tag(out, 6, v.len()),
            Expr::Or(v) => tag(out, 7, v.len()),
            Expr::IsNull(_) => tag(out, 8, 0),
            Expr::IsNotNull(_) => tag(out, 9, 0),
            Expr::BoolTest(_, t) => tag(out, 10, *t as usize),
            Expr::IsDistinct(_, _, n) => tag(out, 11, *n as usize),
            Expr::Case(c) => tag(out, 12, c.whens.len()),
            Expr::Coalesce(v, _) => tag(out, 13, v.len()),
            Expr::NullIf(..) => tag(out, 14, 0),
            Expr::Greatest(v, _) => tag(out, 15, v.len()),
            Expr::Least(v, _) => tag(out, 16, v.len()),
            Expr::InList(_, l, n) => {
                tag(out, 17, l.len());
                out.push(*n as u8)
            }
            Expr::Like(l) => {
                tag(out, 18, l.escape.map(|c| c as usize).unwrap_or(0));
                out.push(l.case_insensitive as u8);
                out.push(l.negated as u8);
                out.push(l.bytea as u8);
            }
            Expr::Array(v, t) => {
                tag(out, 19, v.len());
                crate::types::codec::encode_type(t, out);
            }
            Expr::Subscript(..) => tag(out, 20, 0),
            Expr::AnyAll { func, all, .. } => {
                tag(out, 21, func.0 as usize);
                out.push(*all as u8);
            }
            Expr::Error(c, m) => {
                tag(out, 22, 0);
                out.extend_from_slice(c.as_bytes());
                out.extend_from_slice(m.as_bytes());
            }
            Expr::Outer(d, c, _) => {
                tag(out, 23, *d);
                tag(out, 0, *c);
            }
            Expr::Subquery(_) => tag(out, 24, 0),
            Expr::AggRef(k, _) => tag(out, 25, *k),
            Expr::WindowRef(k, _) => tag(out, 26, *k),
            Expr::SrfRef(k, _) => tag(out, 27, *k),
        }
        for c in self.children() {
            c.fingerprint(out);
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Column(i) => write!(f, "#{i}"),
            Expr::Literal(d, t) => {
                if d.is_null() {
                    write!(f, "NULL")
                } else {
                    match t {
                        SqlType::Int2 | SqlType::Int4 | SqlType::Int8 | SqlType::Numeric | SqlType::Bool => {
                            write!(f, "{}", crate::types::text::format_datum(d))
                        }
                        _ => write!(f, "'{}'::{}", crate::types::text::format_datum(d).replace('\'', "''"), t),
                    }
                }
            }
            Expr::Call(c) => {
                let def = &registry().funcs[c.func.0 as usize];
                if def.is_operator() && c.args.len() == 2 {
                    write!(f, "({} {} {})", c.args[0], def.name, c.args[1])
                } else if def.is_operator() && c.args.len() == 1 {
                    write!(f, "({}{})", def.name, c.args[0])
                } else {
                    write!(f, "{}(", def.name)?;
                    for (i, a) in c.args.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{a}")?;
                    }
                    write!(f, ")")
                }
            }
            Expr::Cast(e, t, tm, _) => write!(f, "{}::{}", e, crate::types::format_type(t, *tm)),
            Expr::Not(e) => write!(f, "NOT {e}"),
            Expr::And(v) => {
                write!(f, "(")?;
                for (i, e) in v.iter().enumerate() {
                    if i > 0 {
                        write!(f, " AND ")?;
                    }
                    write!(f, "{e}")?;
                }
                write!(f, ")")
            }
            Expr::Or(v) => {
                write!(f, "(")?;
                for (i, e) in v.iter().enumerate() {
                    if i > 0 {
                        write!(f, " OR ")?;
                    }
                    write!(f, "{e}")?;
                }
                write!(f, ")")
            }
            Expr::IsNull(e) => write!(f, "{e} IS NULL"),
            Expr::IsNotNull(e) => write!(f, "{e} IS NOT NULL"),
            Expr::BoolTest(e, t) => write!(f, "{e} {t:?}"),
            Expr::IsDistinct(a, b, neg) => {
                write!(f, "{a} IS {}DISTINCT FROM {b}", if *neg { "NOT " } else { "" })
            }
            Expr::Case(c) => {
                write!(f, "CASE")?;
                for (w, t) in &c.whens {
                    write!(f, " WHEN {w} THEN {t}")?;
                }
                write!(f, " ELSE {} END", c.else_)
            }
            Expr::Coalesce(v, _) => write!(f, "COALESCE({})", join(v)),
            Expr::NullIf(a, b) => write!(f, "NULLIF({a}, {b})"),
            Expr::Greatest(v, _) => write!(f, "GREATEST({})", join(v)),
            Expr::Least(v, _) => write!(f, "LEAST({})", join(v)),
            Expr::InList(e, l, n) => write!(f, "{e} {}IN ({})", if *n { "NOT " } else { "" }, join(l)),
            Expr::Like(l) => write!(
                f,
                "{} {}{} {}",
                l.expr,
                if l.negated { "NOT " } else { "" },
                if l.case_insensitive { "ILIKE" } else { "LIKE" },
                l.pattern
            ),
            Expr::Array(v, _) => write!(f, "ARRAY[{}]", join(v)),
            Expr::Subscript(a, i, _) => write!(f, "{a}[{i}]"),
            Expr::AnyAll { expr, array, func, all } => write!(
                f,
                "{expr} {} {}({array})",
                registry().funcs[func.0 as usize].name,
                if *all { "ALL" } else { "ANY" }
            ),
            Expr::Error(_, m) => write!(f, "ERROR({m})"),
            Expr::Outer(d, c, _) => write!(f, "outer{d}#{c}"),
            Expr::Subquery(s) => write!(f, "SUBQUERY({:?})", std::mem::discriminant(&s.kind)),
            Expr::AggRef(k, _) => write!(f, "agg#{k}"),
            Expr::WindowRef(k, _) => write!(f, "win#{k}"),
            Expr::SrfRef(k, _) => write!(f, "srf#{k}"),
        }
    }
}

fn join(v: &[Expr]) -> String {
    v.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")
}

/// The result type of a now()-family function.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NowKind {
    TimestampTz,
    Timestamp,
    Date,
}

/// The value of a now()-family function at clock reading `t`.
pub fn now_literal(k: NowKind, t: i64) -> Expr {
    let tz = Expr::Literal(Datum::TimestampTz(t), SqlType::TimestampTz);
    match k {
        NowKind::TimestampTz => tz,
        NowKind::Timestamp => Expr::Cast(Box::new(tz), SqlType::Timestamp, -1, false).fold().unwrap_or_else(|_| Expr::null(SqlType::Timestamp)),
        NowKind::Date => Expr::Cast(Box::new(tz), SqlType::Date, -1, false).fold().unwrap_or_else(|_| Expr::null(SqlType::Date)),
    }
}
