//! Expression binding.

use super::binder::{common_type, ident, object_name, Binder, Ctx, PendingAgg, PendingWindow};
use super::plan::*;
use super::scope::Scope;
use crate::error::{codes, Result, SqlError};
use crate::expr::cast::{cast_context, CastContext};
use crate::expr::func::{func_def, resolve};
use crate::expr::{BoolTest, CaseExpr, Expr, LikeExpr, SubqueryExpr, SubqueryKind};
use crate::sql_bail;
use crate::types::encoding::SortSpec;
use crate::types::text::{apply_typmod, parse_datum};
use crate::types::{typmod, ColumnType, Datum, SqlType};
use sqlparser::ast;
use std::sync::Arc;

/// Map a SQL data type to (type, typmod).
pub fn sql_type_of(dt: &ast::DataType) -> Result<(SqlType, i32)> {
    use ast::DataType as D;
    let char_len = |l: &Option<ast::CharacterLength>| -> i32 {
        match l {
            Some(ast::CharacterLength::IntegerLength { length, .. }) => typmod::varchar(*length as i32),
            _ => -1,
        }
    };
    let ts_prec = |p: &Option<u64>| p.map(|p| p as i32).unwrap_or(-1);
    Ok(match dt {
        D::Bool | D::Boolean => (SqlType::Bool, -1),
        D::SmallInt(_) | D::Int2(_) => (SqlType::Int2, -1),
        D::Int(_) | D::Integer(_) | D::Int4(_) => (SqlType::Int4, -1),
        D::BigInt(_) | D::Int8(_) => (SqlType::Int8, -1),
        D::Real | D::Float4 => (SqlType::Float4, -1),
        D::DoublePrecision | D::Float8 | D::Double(_) => (SqlType::Float8, -1),
        D::Float(info) => match info {
            ast::ExactNumberInfo::Precision(p) if *p <= 24 => (SqlType::Float4, -1),
            _ => (SqlType::Float8, -1),
        },
        D::Numeric(info) | D::Decimal(info) | D::Dec(info) => match info {
            ast::ExactNumberInfo::None => (SqlType::Numeric, -1),
            ast::ExactNumberInfo::Precision(p) => (SqlType::Numeric, typmod::numeric(*p as i32, 0)),
            ast::ExactNumberInfo::PrecisionAndScale(p, s) => (SqlType::Numeric, typmod::numeric(*p as i32, *s as i32)),
        },
        D::Text => (SqlType::Text, -1),
        D::Varchar(l) | D::CharacterVarying(l) | D::CharVarying(l) => (SqlType::Varchar, char_len(l)),
        D::Char(l) | D::Character(l) => {
            let tm = char_len(l);
            (SqlType::Bpchar, if tm < 0 { typmod::varchar(1) } else { tm })
        }
        D::Bytea => (SqlType::Bytea, -1),
        D::Date => (SqlType::Date, -1),
        D::Time(p, tz) => {
            if matches!(tz, ast::TimezoneInfo::WithTimeZone | ast::TimezoneInfo::Tz) {
                sql_bail!(codes::FEATURE_NOT_SUPPORTED, "type time with time zone is not supported");
            }
            (SqlType::Time, ts_prec(p))
        }
        D::Timestamp(p, tz) => match tz {
            ast::TimezoneInfo::WithTimeZone | ast::TimezoneInfo::Tz => (SqlType::TimestampTz, ts_prec(p)),
            _ => (SqlType::Timestamp, ts_prec(p)),
        },
        D::Interval { fields, precision } => {
            use ast::IntervalFields as F;
            let range = fields.as_ref().map(|f| match f {
                F::Year => interval_range(&[YEAR]),
                F::Month => interval_range(&[MONTH]),
                F::Day => interval_range(&[DAY]),
                F::Hour => interval_range(&[HOUR]),
                F::Minute => interval_range(&[MINUTE]),
                F::Second => interval_range(&[SECOND]),
                F::YearToMonth => interval_range(&[YEAR, MONTH]),
                F::DayToHour => interval_range(&[DAY, HOUR]),
                F::DayToMinute => interval_range(&[DAY, HOUR, MINUTE]),
                F::DayToSecond => interval_range(&[DAY, HOUR, MINUTE, SECOND]),
                F::HourToMinute => interval_range(&[HOUR, MINUTE]),
                F::HourToSecond => interval_range(&[HOUR, MINUTE, SECOND]),
                F::MinuteToSecond => interval_range(&[MINUTE, SECOND]),
            });
            (SqlType::Interval, interval_typmod(range, *precision))
        }
        D::Uuid => (SqlType::Uuid, -1),
        D::JSON => (SqlType::Json, -1),
        D::JSONB => (SqlType::Jsonb, -1),
        D::Array(elem) => {
            let inner = match elem {
                ast::ArrayElemTypeDef::AngleBracket(t)
                | ast::ArrayElemTypeDef::SquareBracket(t, _)
                | ast::ArrayElemTypeDef::Parenthesis(t)
                | ast::ArrayElemTypeDef::Qualified(t, _) => t,
                ast::ArrayElemTypeDef::None => sql_bail!(codes::SYNTAX_ERROR, "array element type required"),
            };
            let (t, _) = sql_type_of(inner)?;
            (t.array_of(), -1)
        }
        D::Custom(name, args) => {
            let (_, n) = object_name(name)?;
            let parse_arg = |i: usize| args.get(i).and_then(|a| a.parse::<i32>().ok());
            match n.as_str() {
                "int2" | "smallint" => (SqlType::Int2, -1),
                "int" | "int4" | "integer" | "serial" | "serial4" => (SqlType::Int4, -1),
                "int8" | "bigint" | "bigserial" | "serial8" => (SqlType::Int8, -1),
                "float4" | "real" => (SqlType::Float4, -1),
                "float8" => (SqlType::Float8, -1),
                "bool" | "boolean" => (SqlType::Bool, -1),
                "text" => (SqlType::Text, -1),
                "varchar" => (SqlType::Varchar, parse_arg(0).map(typmod::varchar).unwrap_or(-1)),
                "bpchar" | "char" => (SqlType::Bpchar, parse_arg(0).map(typmod::varchar).unwrap_or(-1)),
                "name" => (SqlType::Name, -1),
                "numeric" | "decimal" => match (parse_arg(0), parse_arg(1)) {
                    (Some(p), Some(s)) => (SqlType::Numeric, typmod::numeric(p, s)),
                    (Some(p), None) => (SqlType::Numeric, typmod::numeric(p, 0)),
                    _ => (SqlType::Numeric, -1),
                },
                "timestamptz" => (SqlType::TimestampTz, parse_arg(0).unwrap_or(-1)),
                "timestamp" => (SqlType::Timestamp, parse_arg(0).unwrap_or(-1)),
                "time" => (SqlType::Time, parse_arg(0).unwrap_or(-1)),
                "date" => (SqlType::Date, -1),
                "interval" => (SqlType::Interval, -1),
                "uuid" => (SqlType::Uuid, -1),
                "json" => (SqlType::Json, -1),
                "jsonb" => (SqlType::Jsonb, -1),
                "jsonpath" => (SqlType::JsonPath, -1),
                "bytea" => (SqlType::Bytea, -1),
                "oid" | "regclass" | "regtype" | "regproc" | "regnamespace" | "regrole" | "regprocedure" | "regoper" => (SqlType::Oid, -1),
                _ => sql_bail!(codes::UNDEFINED_OBJECT, "type \"{n}\" does not exist"),
            }
        }
        D::Regclass => (SqlType::Oid, -1),
        other => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "type {other} is not supported"),
    })
}

// interval field bits (PostgreSQL's datetime.h)
const MONTH: i32 = 1;
const YEAR: i32 = 2;
const DAY: i32 = 3;
const HOUR: i32 = 10;
const MINUTE: i32 = 11;
const SECOND: i32 = 12;

fn interval_range(fields: &[i32]) -> i32 {
    fields.iter().fold(0, |m, b| m | (1 << b))
}

/// PostgreSQL's interval typmod: (field range << 16) | precision.
fn interval_typmod(range: Option<i32>, precision: Option<u64>) -> i32 {
    if range.is_none() && precision.is_none() {
        return -1;
    }
    (range.unwrap_or(0x7FFF) << 16) | precision.map(|p| p as i32).unwrap_or(0xFFFF)
}

fn binop_name(op: &ast::BinaryOperator) -> Option<&'static str> {
    use ast::BinaryOperator as B;
    Some(match op {
        B::Plus => "+",
        B::Minus => "-",
        B::Multiply => "*",
        B::Divide => "/",
        B::Modulo => "%",
        B::StringConcat => "||",
        B::Gt => ">",
        B::Lt => "<",
        B::GtEq => ">=",
        B::LtEq => "<=",
        B::Eq => "=",
        B::NotEq => "<>",
        B::BitwiseOr => "|",
        B::BitwiseAnd => "&",
        B::PGBitwiseXor => "#",
        B::PGBitwiseShiftLeft => "<<",
        B::PGBitwiseShiftRight => ">>",
        B::PGExp => "^",
        B::PGRegexMatch => "~",
        B::PGRegexIMatch => "~*",
        B::PGRegexNotMatch => "!~",
        B::PGRegexNotIMatch => "!~*",
        B::Arrow => "->",
        B::LongArrow => "->>",
        B::HashArrow => "#>",
        B::HashLongArrow => "#>>",
        B::AtArrow => "@>",
        B::ArrowAt => "<@",
        B::Question => "?",
        B::QuestionAnd => "?&",
        B::QuestionPipe => "?|",
        B::HashMinus => "#-",
        B::PGOverlap => "&&",
        B::AtQuestion => "@?",
        B::AtAt => "@@",
        _ => return None,
    })
}

/// The name and value of a named function argument (`name => value`).
fn named_arg(a: &ast::FunctionArg) -> Option<(String, &ast::FunctionArgExpr)> {
    match a {
        ast::FunctionArg::Named { name, arg, .. } => Some((ident(name), arg)),
        ast::FunctionArg::ExprNamed { name: ast::Expr::Identifier(name), arg, .. } => Some((ident(name), arg)),
        _ => None,
    }
}

/// Functions with (target, path, vars, silent) parameters.
const JSONPATH_FUNCS: &[&str] = &[
    "jsonb_path_exists",
    "jsonb_path_exists_tz",
    "jsonb_path_match",
    "jsonb_path_match_tz",
    "jsonb_path_query",
    "jsonb_path_query_tz",
    "jsonb_path_query_array",
    "jsonb_path_query_array_tz",
    "jsonb_path_query_first",
    "jsonb_path_query_first_tz",
];

const AGGREGATES: &[&str] = &[
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "bool_and",
    "bool_or",
    "every",
    "string_agg",
    "array_agg",
    "json_agg",
    "jsonb_agg",
    "json_object_agg",
    "jsonb_object_agg",
    "stddev",
    "stddev_samp",
    "stddev_pop",
    "variance",
    "var_samp",
    "var_pop",
    "percentile_cont",
    "percentile_disc",
    "mode",
    "any_value",
];

const WINDOW_ONLY: &[&str] =
    &["row_number", "rank", "dense_rank", "percent_rank", "cume_dist", "ntile", "lag", "lead", "first_value", "last_value", "nth_value"];

pub fn is_aggregate_name(n: &str) -> bool {
    AGGREGATES.contains(&n)
}

/// Deepest expression nesting accepted (see Binder::bind_expr).
pub const MAX_EXPR_DEPTH: usize = 1000;

/// While a statement is described, a parameter of still unknown type binds
/// as an unknown literal holding this marker and its index; the first
/// context that coerces it determines its type, as in PostgreSQL. (A NUL
/// character never occurs in a text value, so no literal collides.)
const PARAM_MARK: &str = "\u{0}ivm-param:";

fn param_marker(i: usize) -> Expr {
    Expr::Literal(Datum::text_owned(format!("{PARAM_MARK}{i}")), SqlType::Unknown)
}

fn param_marker_index(s: &str) -> Option<usize> {
    s.strip_prefix(PARAM_MARK)?.parse().ok()
}

/// A character(n) value as text: without its trailing spaces. This is the
/// cast PostgreSQL applies (text(bpchar)), and how bpchar values compare.
fn bpchar_to_text(e: Expr) -> Expr {
    let func = resolve("text", &[SqlType::Bpchar]).expect("text(bpchar) is registered").func;
    let literal = e.is_literal();
    let call = Expr::Call(Box::new(crate::expr::Call { func, args: vec![e], ret: SqlType::Text }));
    if literal { call.clone().fold().unwrap_or(call) } else { call }
}

fn is_comparison(op: &str) -> bool {
    matches!(op, "=" | "<>" | "<" | "<=" | ">" | ">=")
}

impl<'a> Binder<'a> {
    fn types(ctx: &Ctx) -> Vec<ColumnType> {
        ctx.scope.types()
    }

    pub(crate) fn ty_of(&self, e: &Expr, ctx: &Ctx) -> SqlType {
        match e {
            Expr::Column(i) => ctx.scope.items[*i].ty.ty.clone(),
            Expr::Literal(_, t) => t.clone(),
            _ => e.ty(&Self::types(ctx)),
        }
    }

    /// Record the type a parameter of unknown type was first coerced to.
    fn note_param_type(&mut self, i: usize, t: &SqlType) {
        if let Some(p) = self.params.get_mut(i)
            && p.ty == SqlType::Unknown
        {
            p.ty = t.clone();
        }
    }

    /// Coerce `e` (of type `from`) to `to`.
    pub(crate) fn coerce(&mut self, e: Expr, from: &SqlType, to: &SqlType, cctx: CastContext, what: &str) -> Result<Expr> {
        if from == to || *to == SqlType::Unknown {
            return Ok(e);
        }
        if *from == SqlType::Unknown {
            return Ok(match e {
                Expr::Literal(Datum::Null, _) => Expr::Literal(Datum::Null, to.clone()),
                Expr::Literal(Datum::Text(s), _) => match param_marker_index(&s) {
                    Some(i) => {
                        self.note_param_type(i, to);
                        Expr::Literal(Datum::Null, to.clone())
                    }
                    None => Expr::Literal(parse_datum(&s, to, -1)?, to.clone()),
                },
                other => Expr::Cast(Box::new(other), to.clone(), -1, false),
            });
        }
        if *from == SqlType::Bpchar && matches!(to, SqlType::Text | SqlType::Varchar | SqlType::Name) {
            let t = bpchar_to_text(e);
            return Ok(if *to == SqlType::Text { t } else { fold_cast(Expr::Cast(Box::new(t), to.clone(), -1, cctx == CastContext::Explicit)) });
        }
        match cast_context(from, to) {
            Some(c) if c <= cctx => Ok(fold_cast(Expr::Cast(Box::new(e), to.clone(), -1, cctx == CastContext::Explicit))),
            _ => match cctx {
                CastContext::Explicit => {
                    sql_bail!(codes::DATATYPE_MISMATCH, "cannot cast type {} to {}", from, to)
                }
                _ => sql_bail!(
                    codes::DATATYPE_MISMATCH,
                    "{what}: expression is of type {} but {} is required",
                    from,
                    to
                ),
            },
        }
    }

    /// Resolve and build a binary operator application.
    pub(crate) fn make_binary(&mut self, op: &str, l: Expr, lt: SqlType, r: Expr, rt: SqlType) -> Result<Expr> {
        let res = resolve(op, &[lt.clone(), rt.clone()])?;
        if res.arg_types == [SqlType::Bpchar, SqlType::Bpchar] && is_comparison(op) {
            // character(n) compares without trailing spaces: compare the
            // values as text, so that equality is plain value equality (as
            // joins, IN and decorrelation assume)
            let l = self.coerce(l, &lt, &SqlType::Bpchar, CastContext::Implicit, op)?;
            let r = self.coerce(r, &rt, &SqlType::Bpchar, CastContext::Implicit, op)?;
            return self.make_binary(op, bpchar_to_text(l), SqlType::Text, bpchar_to_text(r), SqlType::Text);
        }
        let l = self.coerce(l, &lt, &res.arg_types[0], CastContext::Implicit, op)?;
        let r = self.coerce(r, &rt, &res.arg_types[1], CastContext::Implicit, op)?;
        Ok(Expr::Call(Box::new(crate::expr::Call { func: res.func, args: vec![l, r], ret: res.ret })))
    }

    fn make_call(&mut self, name: &str, args: Vec<Expr>, types: Vec<SqlType>) -> Result<Expr> {
        let res = resolve(name, &types)?;
        let mut out = Vec::with_capacity(args.len());
        for ((a, t), target) in args.into_iter().zip(types).zip(res.arg_types.iter()) {
            out.push(self.coerce(a, &t, target, CastContext::Implicit, name)?);
        }
        Ok(Expr::Call(Box::new(crate::expr::Call { func: res.func, args: out, ret: res.ret })))
    }

    pub(crate) fn bind_expr(&mut self, e: &ast::Expr, ctx: &Ctx) -> Result<Expr> {
        // Deeply nested expressions (long chains of binary operators, which
        // the parser builds without recursing) would overflow the stack in
        // binding and in every later pass over the tree: refuse them, like
        // PostgreSQL's stack depth check does.
        if self.expr_depth >= MAX_EXPR_DEPTH {
            return Err(SqlError::new(codes::STATEMENT_TOO_COMPLEX, "stack depth limit exceeded")
                .with_hint(format!("Expressions may be nested at most {MAX_EXPR_DEPTH} levels deep.")));
        }
        self.expr_depth += 1;
        let r = self.bind_expr_nested(e, ctx);
        self.expr_depth -= 1;
        r
    }

    fn bind_expr_nested(&mut self, e: &ast::Expr, ctx: &Ctx) -> Result<Expr> {
        use ast::Expr as E;
        match e {
            E::Identifier(id) => self.bind_column(None, &ident(id), ctx),
            E::CompoundIdentifier(ids) => {
                let parts: Vec<String> = ids.iter().map(ident).collect();
                match parts.len() {
                    2 => self.bind_column(Some(&parts[0]), &parts[1], ctx),
                    3 => self.bind_column(Some(&parts[1]), &parts[2], ctx),
                    _ => sql_bail!(codes::SYNTAX_ERROR, "improper qualified name: {e}"),
                }
            }
            E::Value(v) => self.bind_value(&v.value),
            E::Nested(inner) => self.bind_expr(inner, ctx),
            E::IsNull(x) => Ok(Expr::IsNull(Box::new(self.bind_expr(x, ctx)?))),
            E::IsNotNull(x) => Ok(Expr::IsNotNull(Box::new(self.bind_expr(x, ctx)?))),
            E::IsTrue(x) => self.bool_test(x, BoolTest::IsTrue, ctx),
            E::IsNotTrue(x) => self.bool_test(x, BoolTest::IsNotTrue, ctx),
            E::IsFalse(x) => self.bool_test(x, BoolTest::IsFalse, ctx),
            E::IsNotFalse(x) => self.bool_test(x, BoolTest::IsNotFalse, ctx),
            E::IsUnknown(x) => self.bool_test(x, BoolTest::IsUnknown, ctx),
            E::IsNotUnknown(x) => self.bool_test(x, BoolTest::IsNotUnknown, ctx),
            E::IsDistinctFrom(a, b) | E::IsNotDistinctFrom(a, b)
                if matches!((&**a, &**b), (E::Tuple(_), E::Tuple(_))) =>
            {
                // rows are distinct when any pair of fields is
                let (E::Tuple(l), E::Tuple(r)) = (&**a, &**b) else { unreachable!() };
                if l.len() != r.len() {
                    sql_bail!(codes::SYNTAX_ERROR, "unequal number of entries in row expressions");
                }
                let mut parts = Vec::with_capacity(l.len());
                for (x, y) in l.iter().zip(r) {
                    let pe = if matches!(e, E::IsDistinctFrom(..)) {
                        E::IsDistinctFrom(Box::new(x.clone()), Box::new(y.clone()))
                    } else {
                        E::IsNotDistinctFrom(Box::new(x.clone()), Box::new(y.clone()))
                    };
                    parts.push(self.bind_expr(&pe, ctx)?);
                }
                Ok(if matches!(e, E::IsDistinctFrom(..)) { Expr::Or(parts) } else { Expr::And(parts) })
            }
            E::IsDistinctFrom(a, b) | E::IsNotDistinctFrom(a, b) => {
                let negated = matches!(e, E::IsNotDistinctFrom(..));
                let (x, y) = (self.bind_expr(a, ctx)?, self.bind_expr(b, ctx)?);
                let (xt, yt) = (self.ty_of(&x, ctx), self.ty_of(&y, ctx));
                // validate comparability through "="
                let res = resolve("=", &[xt.clone(), yt.clone()])?;
                let x = self.coerce(x, &xt, &res.arg_types[0], CastContext::Implicit, "IS DISTINCT FROM")?;
                let y = self.coerce(y, &yt, &res.arg_types[1], CastContext::Implicit, "IS DISTINCT FROM")?;
                let (x, y) = if res.arg_types == [SqlType::Bpchar, SqlType::Bpchar] { (bpchar_to_text(x), bpchar_to_text(y)) } else { (x, y) };
                Ok(Expr::IsDistinct(Box::new(x), Box::new(y), negated))
            }
            E::BinaryOp { left, op, right } => self.bind_binary(left, op, right, ctx),
            E::UnaryOp { op, expr } => self.bind_unary(op, expr, ctx),
            E::Between { expr, negated, low, high } => {
                let x = self.bind_expr(expr, ctx)?;
                let lo = self.bind_expr(low, ctx)?;
                let hi = self.bind_expr(high, ctx)?;
                let (xt, lt, ht) = (self.ty_of(&x, ctx), self.ty_of(&lo, ctx), self.ty_of(&hi, ctx));
                if *negated {
                    let a = self.make_binary("<", x.clone(), xt.clone(), lo, lt)?;
                    let b = self.make_binary(">", x, xt, hi, ht)?;
                    Ok(Expr::Or(vec![a, b]))
                } else {
                    let a = self.make_binary(">=", x.clone(), xt.clone(), lo, lt)?;
                    let b = self.make_binary("<=", x, xt, hi, ht)?;
                    Ok(Expr::And(vec![a, b]))
                }
            }
            E::InList { expr, list, negated } if matches!(&**expr, E::Tuple(_)) => {
                // (a, b) IN ((1, 2), ...): a disjunction of row equalities
                let mut alts = Vec::with_capacity(list.len());
                for item in list {
                    let eq = E::BinaryOp { left: expr.clone(), op: ast::BinaryOperator::Eq, right: Box::new(item.clone()) };
                    alts.push(self.bind_expr(&eq, ctx)?);
                }
                let any = Expr::Or(alts);
                Ok(if *negated { Expr::Not(Box::new(any)) } else { any })
            }
            E::InList { expr, list, negated } => {
                let x = self.bind_expr(expr, ctx)?;
                let mut items = Vec::with_capacity(list.len());
                for i in list {
                    items.push(self.bind_expr(i, ctx)?);
                }
                let mut types = vec![self.ty_of(&x, ctx)];
                types.extend(items.iter().map(|i| self.ty_of(i, ctx)));
                let t = common_type(&types, "IN")?;
                // make sure "=" exists for the type
                resolve("=", &[t.clone(), t.clone()])?;
                let mut x = self.coerce(x, &types[0], &t, CastContext::Implicit, "IN")?;
                let mut out = Vec::with_capacity(items.len());
                for (i, it) in items.into_iter().enumerate() {
                    out.push(self.coerce(it, &types[i + 1], &t, CastContext::Implicit, "IN")?);
                }
                if t == SqlType::Bpchar {
                    // compared without trailing spaces
                    x = bpchar_to_text(x);
                    out = out.into_iter().map(bpchar_to_text).collect();
                }
                Ok(Expr::InList(Box::new(x), out, *negated))
            }
            E::InSubquery { expr, subquery, negated } => self.bind_in_subquery(expr, subquery, *negated, ctx),
            E::Exists { subquery, negated } => {
                let rel = self.bind_subquery(subquery, ctx)?;
                let sq = Expr::Subquery(Box::new(SubqueryExpr { kind: SubqueryKind::Exists, rel, ty: SqlType::Bool }));
                Ok(if *negated { Expr::Not(Box::new(sq)) } else { sq })
            }
            E::Subquery(q) => {
                let rel = self.bind_subquery(q, ctx)?;
                let s = rel.schema();
                if s.len() != 1 {
                    sql_bail!(codes::SYNTAX_ERROR, "subquery must return only one column");
                }
                Ok(Expr::Subquery(Box::new(SubqueryExpr { kind: SubqueryKind::Scalar, rel, ty: s[0].ty.clone() })))
            }
            E::AnyOp { left, compare_op, right, .. } | E::AllOp { left, compare_op, right } => {
                let all = matches!(e, E::AllOp { .. });
                // rows: (a, b) = ANY (subquery) is IN, (a, b) <> ALL is NOT IN
                if let (E::Tuple(_), E::Subquery(q)) = (&**left, &**right) {
                    return match (compare_op, all) {
                        (ast::BinaryOperator::Eq, false) => self.bind_in_subquery(left, q, false, ctx),
                        (ast::BinaryOperator::NotEq, true) => self.bind_in_subquery(left, q, true, ctx),
                        _ => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "row comparisons with {compare_op} ANY/ALL are not supported"),
                    };
                }
                let opname =
                    binop_name(compare_op).ok_or_else(|| SqlError::unsupported(format!("operator {compare_op} in ANY/ALL")))?;
                let x = self.bind_expr(left, ctx)?;
                let xt = self.ty_of(&x, ctx);
                if let E::Subquery(q) = &**right {
                    let rel = self.bind_subquery(q, ctx)?;
                    let st = rel.schema();
                    if st.len() != 1 {
                        sql_bail!(codes::SYNTAX_ERROR, "subquery has too many columns");
                    }
                    let res = resolve(opname, &[xt.clone(), st[0].ty.clone()])?;
                    let l = self.coerce(x, &xt, &res.arg_types[0], CastContext::Implicit, opname)?;
                    let rel = self.coerce_rel(rel, &[ColumnType::new(res.arg_types[1].clone())], opname)?;
                    return Ok(Expr::Subquery(Box::new(SubqueryExpr {
                        kind: SubqueryKind::AnyAll { lhs: Box::new(l), func: res.func, all },
                        rel,
                        ty: SqlType::Bool,
                    })));
                }
                let arr = self.bind_expr(right, ctx)?;
                let at = self.ty_of(&arr, ctx);
                let elem = match &at {
                    SqlType::Array(e) => (**e).clone(),
                    SqlType::Unknown => xt.clone(),
                    other => sql_bail!(codes::DATATYPE_MISMATCH, "op ANY/ALL (array) requires array on right side, got {other}"),
                };
                let arr = self.coerce(arr, &at, &SqlType::Array(Box::new(elem.clone())), CastContext::Implicit, "ANY")?;
                let res = resolve(opname, &[xt.clone(), elem.clone()])?;
                let x = self.coerce(x, &xt, &res.arg_types[0], CastContext::Implicit, opname)?;
                let arr = if res.arg_types[1] != elem {
                    self.coerce(arr, &SqlType::Array(Box::new(elem)), &SqlType::Array(Box::new(res.arg_types[1].clone())), CastContext::Implicit, "ANY")?
                } else {
                    arr
                };
                Ok(Expr::AnyAll { expr: Box::new(x), array: Box::new(arr), func: res.func, all })
            }
            E::Like { negated, expr, pattern, escape_char, any, .. }
            | E::ILike { negated, expr, pattern, escape_char, any, .. } => {
                let ci = matches!(e, E::ILike { .. });
                if *any {
                    return self.bind_like_any(expr, pattern, escape_char.is_some(), ci, *negated, ctx);
                }
                self.bind_like(expr, pattern, escape_char.as_deref(), ci, *negated, ctx)
            }
            E::SimilarTo { negated, expr, pattern, escape_char } => {
                let x = self.bind_expr(expr, ctx)?;
                let p = self.bind_expr(pattern, ctx)?;
                let xt = self.ty_of(&x, ctx);
                // character(n) matches with its padding, as with ~
                let x = if xt == SqlType::Bpchar {
                    Expr::Cast(Box::new(x), SqlType::Text, -1, false)
                } else {
                    self.coerce(x, &xt, &SqlType::Text, CastContext::Implicit, "SIMILAR TO")?
                };
                let escape = match escape_char {
                    Some(ec) => self.const_text(ec, ctx)?.and_then(|s| s.chars().next()),
                    None => Some('\\'),
                };
                let Expr::Literal(Datum::Text(pat), _) = p.fold()? else {
                    sql_bail!(codes::FEATURE_NOT_SUPPORTED, "SIMILAR TO requires a constant pattern");
                };
                let re = pgfuncs::pattern::similar_to_regex(&pat, escape)?;
                let m = self.make_binary(
                    "~",
                    x,
                    SqlType::Text,
                    Expr::Literal(Datum::text_owned(re), SqlType::Text),
                    SqlType::Text,
                )?;
                Ok(if *negated { Expr::Not(Box::new(m)) } else { m })
            }
            E::Cast { expr, data_type, kind, .. } => {
                if matches!(kind, ast::CastKind::TryCast | ast::CastKind::SafeCast) {
                    sql_bail!(codes::FEATURE_NOT_SUPPORTED, "TRY_CAST is not supported");
                }
                let (t, tm) = sql_type_of(data_type)?;
                let x = self.bind_expr(expr, ctx)?;
                // reg* types print as names: map OIDs through the catalog
                let reg = match data_type {
                    ast::DataType::Custom(name, _) => {
                        let n = name.0.last().map(|p| p.to_string().to_lowercase()).unwrap_or_default();
                        matches!(n.as_str(), "regclass" | "regtype" | "regnamespace").then_some(n)
                    }
                    ast::DataType::Regclass => Some("regclass".to_string()),
                    _ => None,
                };
                // and read names: 'users'::regclass is the OID of users (kept
                // an oid, so that it compares with oid columns)
                if let (Some(kind), Expr::Literal(Datum::Text(name), SqlType::Unknown | SqlType::Text)) = (&reg, &x)
                    && name.trim().parse::<i64>().is_err()
                {
                    return Ok(Expr::Literal(Datum::Oid(self.reg_name_oid(kind, name)?), SqlType::Oid));
                }
                let x = self.explicit_cast(x, &t, tm, ctx)?;
                match reg {
                    Some(kind) => Ok(self.oid_lookup(x, &kind, true)),
                    None => Ok(x),
                }
            }
            E::TypedString(ts) => {
                let (t, tm) = sql_type_of(&ts.data_type)?;
                let s = match &ts.value.value {
                    ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => s.clone(),
                    ast::Value::EscapedStringLiteral(s) => s.clone(),
                    other => other.to_string(),
                };
                // `type 'string'` is a cast: too long strings are truncated
                let d = apply_typmod(parse_datum(&s, &t, -1)?, &t, tm, true)?;
                Ok(Expr::Literal(d, t))
            }
            E::Interval(iv) => {
                let v = self.bind_expr(&iv.value, ctx)?.fold()?;
                let text = match &v {
                    Expr::Literal(Datum::Text(s), _) => s.to_string(),
                    Expr::Literal(d, _) if !d.is_null() => crate::types::text::format_datum(d),
                    _ => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "INTERVAL requires a constant"),
                };
                // INTERVAL '...' DAY etc.: the fields restrict the value (and
                // give unit-less numbers their unit), as interval_in does
                use ast::DateTimeField as F;
                let bit = |f: &F| -> Option<i32> {
                    Some(match f {
                        F::Year | F::Years => YEAR,
                        F::Month | F::Months => MONTH,
                        F::Day | F::Days => DAY,
                        F::Hour | F::Hours => HOUR,
                        F::Minute | F::Minutes => MINUTE,
                        F::Second | F::Seconds => SECOND,
                        _ => return None,
                    })
                };
                let bad = || SqlError::syntax(format!("invalid interval qualifier in {e}"));
                let range = match (&iv.leading_field, &iv.last_field) {
                    (None, None) => None,
                    (Some(l), None) => Some(interval_range(&[bit(l).ok_or_else(bad)?])),
                    (Some(l), Some(t)) => {
                        let (l, t) = (bit(l).ok_or_else(bad)?, bit(t).ok_or_else(bad)?);
                        let span: &[i32] = match (l, t) {
                            (YEAR, MONTH) => &[YEAR, MONTH],
                            (DAY, HOUR) => &[DAY, HOUR],
                            (DAY, MINUTE) => &[DAY, HOUR, MINUTE],
                            (DAY, SECOND) => &[DAY, HOUR, MINUTE, SECOND],
                            (HOUR, MINUTE) => &[HOUR, MINUTE],
                            (HOUR, SECOND) => &[HOUR, MINUTE, SECOND],
                            (MINUTE, SECOND) => &[MINUTE, SECOND],
                            _ => return Err(bad()),
                        };
                        Some(interval_range(span))
                    }
                    (None, Some(_)) => return Err(bad()),
                };
                let seconds_only = range == Some(interval_range(&[SECOND]));
                let precision = iv.fractional_seconds_precision.or(if seconds_only { iv.leading_precision } else { None });
                let d = pgdatetime::parse_interval_raw_typmod(&text, interval_typmod(range, precision))?;
                Ok(Expr::Literal(Datum::Interval(d), SqlType::Interval))
            }
            E::AtTimeZone { timestamp, time_zone } => {
                let ts = self.bind_expr(timestamp, ctx)?;
                let tz = self.bind_expr(time_zone, ctx)?;
                let (tst, tzt) = (self.ty_of(&ts, ctx), self.ty_of(&tz, ctx));
                // a date resolves to the timestamptz variant (preferred type)
                let ts_target = match tst {
                    SqlType::Timestamp | SqlType::TimestampTz => tst.clone(),
                    SqlType::Date | SqlType::Unknown => SqlType::TimestampTz,
                    other => sql_bail!(codes::DATATYPE_MISMATCH, "AT TIME ZONE requires a timestamp, got {other}"),
                };
                let ts = self.coerce(ts, &tst, &ts_target, CastContext::Implicit, "AT TIME ZONE")?;
                self.make_call("timezone", vec![tz, ts], vec![tzt, ts_target])
            }
            E::Extract { field, expr, .. } => {
                let x = self.bind_expr(expr, ctx)?;
                let xt = self.ty_of(&x, ctx);
                let f = Expr::Literal(Datum::text(&datetime_field_name(field)), SqlType::Text);
                self.make_call("extract", vec![f, x], vec![SqlType::Text, xt])
            }
            E::Ceil { expr, field } | E::Floor { expr, field } => {
                if !matches!(field, ast::CeilFloorKind::DateTimeField(ast::DateTimeField::NoDateTime)) {
                    sql_bail!(codes::FEATURE_NOT_SUPPORTED, "CEIL/FLOOR with a field is not supported");
                }
                let x = self.bind_expr(expr, ctx)?;
                let xt = self.ty_of(&x, ctx);
                let name = if matches!(e, E::Ceil { .. }) { "ceil" } else { "floor" };
                self.make_call(name, vec![x], vec![xt])
            }
            E::Position { expr, r#in } => {
                let needle = self.bind_expr(expr, ctx)?;
                let hay = self.bind_expr(r#in, ctx)?;
                let (nt, ht) = (self.ty_of(&needle, ctx), self.ty_of(&hay, ctx));
                self.make_call("strpos", vec![hay, needle], vec![ht, nt])
            }
            E::Substring { expr, substring_from, substring_for, .. } => {
                let x = self.bind_expr(expr, ctx)?;
                let mut args = vec![x];
                if let Some(f) = substring_from {
                    args.push(self.bind_expr(f, ctx)?);
                } else {
                    args.push(Expr::Literal(Datum::Int4(1), SqlType::Int4));
                }
                if let Some(f) = substring_for {
                    args.push(self.bind_expr(f, ctx)?);
                }
                let types: Vec<SqlType> = args.iter().map(|a| self.ty_of(a, ctx)).collect();
                self.make_call("substring", args, types)
            }
            E::Trim { trim_where, trim_what, expr, trim_characters } => {
                if trim_characters.is_some() {
                    sql_bail!(codes::FEATURE_NOT_SUPPORTED, "TRIM with character list syntax is not supported");
                }
                let x = self.bind_expr(expr, ctx)?;
                let name = match trim_where {
                    Some(ast::TrimWhereField::Leading) => "ltrim",
                    Some(ast::TrimWhereField::Trailing) => "rtrim",
                    _ => "btrim",
                };
                let mut args = vec![x];
                if let Some(w) = trim_what {
                    args.push(self.bind_expr(w, ctx)?);
                }
                let types: Vec<SqlType> = args.iter().map(|a| self.ty_of(a, ctx)).collect();
                self.make_call(name, args, types)
            }
            E::Overlay { expr, overlay_what, overlay_from, overlay_for } => {
                let mut args = vec![
                    self.bind_expr(expr, ctx)?,
                    self.bind_expr(overlay_what, ctx)?,
                    self.bind_expr(overlay_from, ctx)?,
                ];
                if let Some(f) = overlay_for {
                    args.push(self.bind_expr(f, ctx)?);
                }
                let types: Vec<SqlType> = args.iter().map(|a| self.ty_of(a, ctx)).collect();
                self.make_call("overlay", args, types)
            }
            E::Collate { expr, collation } => {
                let (_, c) = object_name(collation)?;
                if !matches!(c.as_str(), "c" | "C" | "default" | "pg_c_utf8" | "ucs_basic" | "C.UTF-8" | "c.utf-8") {
                    sql_bail!(codes::FEATURE_NOT_SUPPORTED, "collation \"{c}\" is not supported");
                }
                self.bind_expr(expr, ctx)
            }
            E::Function(f) => self.bind_function(f, ctx),
            E::Case { operand, conditions, else_result, .. } => self.bind_case(operand.as_deref(), conditions, else_result.as_deref(), ctx),
            E::Array(arr) => {
                let mut elems = Vec::with_capacity(arr.elem.len());
                for x in &arr.elem {
                    elems.push(self.bind_expr(x, ctx)?);
                }
                let types: Vec<SqlType> = elems.iter().map(|x| self.ty_of(x, ctx)).collect();
                let t = if types.is_empty() { SqlType::Text } else { common_type(&types, "ARRAY")? };
                let mut out = Vec::with_capacity(elems.len());
                for (x, xt) in elems.into_iter().zip(types) {
                    out.push(self.coerce(x, &xt, &t, CastContext::Implicit, "ARRAY")?);
                }
                Ok(Expr::Array(out, t))
            }
            E::CompoundFieldAccess { root, access_chain } => {
                // leading `.name` accesses on an identifier form a qualified column
                let mut idents: Vec<ast::Ident> = match &**root {
                    E::Identifier(i) => vec![i.clone()],
                    E::CompoundIdentifier(v) => v.clone(),
                    _ => vec![],
                };
                let mut rest = &access_chain[..];
                if !idents.is_empty() {
                    while let Some(ast::AccessExpr::Dot(E::Identifier(i))) = rest.first() {
                        idents.push(i.clone());
                        rest = &rest[1..];
                    }
                }
                let mut x = if idents.len() > 1 {
                    self.bind_expr(&E::CompoundIdentifier(idents), ctx)?
                } else {
                    self.bind_expr(root, ctx)?
                };
                let mut k = 0;
                while k < rest.len() {
                    let a = &rest[k];
                    k += 1;
                    let xt = self.ty_of(&x, ctx);
                    if matches!(xt, SqlType::Array(_)) && matches!(a, ast::AccessExpr::Subscript(_)) {
                        // a[i][j] subscripts a 2-D array (not an array of
                        // arrays); with any slice, all subscripts are slices
                        let mut subs = Vec::new();
                        let mut j = k - 1;
                        while let Some(ast::AccessExpr::Subscript(sub)) = rest.get(j) {
                            subs.push(sub);
                            j += 1;
                        }
                        k = j;
                        x = self.bind_array_subscripts(x, xt, &subs, ctx)?;
                        continue;
                    }
                    match a {
                        ast::AccessExpr::Subscript(ast::Subscript::Index { index }) if xt == SqlType::Jsonb => {
                            // jsonb subscripting: j['key'] / j[0]
                            let i = self.bind_expr(index, ctx)?;
                            let it = match self.ty_of(&i, ctx) {
                                SqlType::Unknown => SqlType::Text,
                                t => t,
                            };
                            x = self.make_binary("->", x, SqlType::Jsonb, i, it)?;
                        }
                        ast::AccessExpr::Subscript(_) => {
                            sql_bail!(codes::DATATYPE_MISMATCH, "cannot subscript type {xt} because it does not support subscripting")
                        }
                        _ => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unsupported field access: {e}"),
                    }
                }
                Ok(x)
            }
            E::Tuple(_) => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "row constructors are not supported"),
            E::Prefixed { .. } => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unsupported expression: {e}"),
            _ => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unsupported expression: {e}"),
        }
    }

    /// `expr [NOT] IN (subquery)`; `expr` may be a row constructor.
    fn bind_in_subquery(&mut self, expr: &ast::Expr, subquery: &ast::Query, negated: bool, ctx: &Ctx) -> Result<Expr> {
        use ast::Expr as E;
        {
                // a row constructor compares several columns at once
                let lhs: Vec<Expr> = match expr {
                    E::Tuple(items) => items.iter().map(|i| self.bind_expr(i, ctx)).collect::<Result<_>>()?,
                    other => vec![self.bind_expr(other, ctx)?],
                };
                let rel = self.bind_subquery(subquery, ctx)?;
                let sub_types = rel.schema();
                if sub_types.len() != lhs.len() {
                    sql_bail!(
                        codes::SYNTAX_ERROR,
                        "subquery has too {} columns",
                        if sub_types.len() > lhs.len() { "many" } else { "few" }
                    );
                }
                let mut ls = Vec::with_capacity(lhs.len());
                let mut targets = Vec::with_capacity(lhs.len());
                for (x, st) in lhs.into_iter().zip(&sub_types) {
                    let xt = self.ty_of(&x, ctx);
                    let res = resolve("=", &[xt.clone(), st.ty.clone()])?;
                    let x = self.coerce(x, &xt, &res.arg_types[0], CastContext::Implicit, "IN")?;
                    // character(n) is compared as text without trailing spaces
                    ls.push(if res.arg_types == [SqlType::Bpchar, SqlType::Bpchar] { bpchar_to_text(x) } else { x });
                    targets.push(ColumnType::new(res.arg_types[1].clone()));
                }
                let mut rel = self.coerce_rel(rel, &targets, "IN")?;
                if targets.iter().any(|t| t.ty == SqlType::Bpchar) {
                    let text: Vec<ColumnType> = targets
                        .iter()
                        .map(|t| if t.ty == SqlType::Bpchar { ColumnType::new(SqlType::Text) } else { t.clone() })
                        .collect();
                    rel = self.coerce_rel(rel, &text, "IN")?;
                }
                let sq = Expr::Subquery(Box::new(SubqueryExpr { kind: SubqueryKind::In(ls), rel, ty: SqlType::Bool }));
                Ok(if negated { Expr::Not(Box::new(sq)) } else { sq })
        }
    }

    fn bool_test(&mut self, x: &ast::Expr, t: BoolTest, ctx: &Ctx) -> Result<Expr> {
        let e = self.bind_expr(x, ctx)?;
        let e = self.coerce_bool(e, "IS TRUE")?;
        Ok(Expr::BoolTest(Box::new(e), t))
    }

    fn const_text(&mut self, e: &ast::Expr, ctx: &Ctx) -> Result<Option<String>> {
        let x = self.bind_expr(e, ctx)?.fold()?;
        match x {
            Expr::Literal(Datum::Text(s), _) if param_marker_index(&s).is_some() => Ok(None),
            Expr::Literal(Datum::Text(s), _) => Ok(Some(s.to_string())),
            Expr::Literal(Datum::Null, _) => Ok(None),
            _ => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "a constant string is required"),
        }
    }

    fn bind_like(
        &mut self,
        expr: &ast::Expr,
        pattern: &ast::Expr,
        escape: Option<&ast::Expr>,
        ci: bool,
        negated: bool,
        ctx: &Ctx,
    ) -> Result<Expr> {
        let x = self.bind_expr(expr, ctx)?;
        let p = self.bind_expr(pattern, ctx)?;
        let (xt, pt) = (self.ty_of(&x, ctx), self.ty_of(&p, ctx));
        let bytea = xt == SqlType::Bytea;
        let target = if bytea { SqlType::Bytea } else { SqlType::Text };
        // character(n) matches with its padding (bpcharlike)
        let x = if xt == SqlType::Bpchar {
            Expr::Cast(Box::new(x), SqlType::Text, -1, false)
        } else {
            self.coerce(x, &xt, &target, CastContext::Implicit, "LIKE")?
        };
        let p = self.coerce(p, &pt, &target, CastContext::Implicit, "LIKE")?.fold()?;
        let escape = match escape {
            Some(ec) => match self.const_text(ec, ctx)? {
                Some(s) if s.is_empty() => None,
                Some(s) => {
                    if s.chars().count() != 1 {
                        sql_bail!("22025", "invalid escape string");
                    }
                    s.chars().next()
                }
                None => return Ok(Expr::null(SqlType::Bool)),
            },
            None => Some('\\'),
        };
        // lenient compilation: PostgreSQL only complains about a trailing
        // escape when matching actually reaches it
        let compiled = match &p {
            Expr::Literal(Datum::Text(s), _) if !bytea => Some(Arc::new(pgfuncs::pattern::LikePattern::new_lenient(s, escape, ci))),
            _ => None,
        };
        Ok(Expr::Like(Box::new(LikeExpr { expr: x, pattern: p, escape, case_insensitive: ci, negated, bytea, compiled })))
    }

    /// `x [NOT] [I]LIKE ANY (array)`: the ~~ operators applied to the
    /// elements (NOT LIKE ANY is !~~ ANY: some pattern does not match).
    fn bind_like_any(&mut self, expr: &ast::Expr, pattern: &ast::Expr, escape: bool, ci: bool, negated: bool, ctx: &Ctx) -> Result<Expr> {
        if escape {
            sql_bail!(codes::FEATURE_NOT_SUPPORTED, "LIKE ANY with ESCAPE is not supported");
        }
        let op = match (negated, ci) {
            (false, false) => "~~",
            (false, true) => "~~*",
            (true, false) => "!~~",
            (true, true) => "!~~*",
        };
        let x = self.bind_expr(expr, ctx)?;
        let xt = self.ty_of(&x, ctx);
        // like PostgreSQL's LIKE, character(n) matches with its padding
        let x = if xt == SqlType::Bpchar {
            Expr::Cast(Box::new(x), SqlType::Text, -1, false)
        } else {
            self.coerce(x, &xt, &SqlType::Text, CastContext::Implicit, "LIKE")?
        };
        let pat = match pattern {
            ast::Expr::Nested(inner) => &**inner,
            ast::Expr::Tuple(items) if items.len() == 1 => &items[0],
            other => other,
        };
        let arr = self.bind_expr(pat, ctx)?;
        let at = self.ty_of(&arr, ctx);
        let text_arr = SqlType::Array(Box::new(SqlType::Text));
        let arr = match &at {
            SqlType::Array(_) | SqlType::Unknown => self.coerce(arr, &at, &text_arr, CastContext::Implicit, "LIKE ANY")?,
            other => sql_bail!(codes::DATATYPE_MISMATCH, "op ANY/ALL (array) requires array on right side, got {other}"),
        };
        let func = resolve(op, &[SqlType::Text, SqlType::Text])?.func;
        Ok(Expr::AnyAll { expr: Box::new(x), array: Box::new(arr), func, all: false })
    }

    fn explicit_cast(&mut self, x: Expr, t: &SqlType, tm: i32, ctx: &Ctx) -> Result<Expr> {
        let xt = self.ty_of(&x, ctx);
        match x {
            Expr::Literal(Datum::Text(s), SqlType::Unknown) => match param_marker_index(&s) {
                Some(i) => {
                    self.note_param_type(i, t);
                    Ok(Expr::Literal(Datum::Null, t.clone()))
                }
                // an explicit cast: too long strings are truncated
                None => Ok(Expr::Literal(apply_typmod(parse_datum(&s, t, -1)?, t, tm, true)?, t.clone())),
            },
            Expr::Literal(Datum::Null, _) => Ok(Expr::Literal(Datum::Null, t.clone())),
            x => {
                // character(n) to a string type drops the padding
                let (x, xt) = if xt == SqlType::Bpchar && matches!(t, SqlType::Text | SqlType::Varchar | SqlType::Name) {
                    (bpchar_to_text(x), SqlType::Text)
                } else {
                    (x, xt)
                };
                if xt == *t && tm < 0 {
                    return Ok(x);
                }
                if xt != SqlType::Unknown && cast_context(&xt, t).is_none() {
                    sql_bail!(codes::DATATYPE_MISMATCH, "cannot cast type {} to {}", xt, crate::types::format_type(t, tm));
                }
                Ok(fold_cast(Expr::Cast(Box::new(x), t.clone(), tm, true)))
            }
        }
    }

    fn bind_column(&mut self, table: Option<&str>, name: &str, ctx: &Ctx) -> Result<Expr> {
        if let Some(c) = ctx.scope.resolve(table, name)? {
            return Ok(Expr::Column(c));
        }
        if let Some((depth, col, ty)) = self.lookup_outer(table, name)? {
            return Ok(Expr::Outer(depth, col, ty.ty));
        }
        // a few SQL value functions parse as identifiers
        if table.is_none() {
            match name {
                "current_date" | "current_timestamp" | "localtimestamp" | "now" => {
                    return self.make_call(name, vec![], vec![]);
                }
                "current_user" | "session_user" | "user" | "current_role" => {
                    return Ok(Expr::Literal(Datum::text("ivm"), SqlType::Name));
                }
                "current_catalog" => return Ok(Expr::Literal(Datum::text("ivm"), SqlType::Name)),
                "current_schema" => return Ok(Expr::Literal(Datum::text("public"), SqlType::Name)),
                _ => {}
            }
        }
        match table {
            Some(t) if !ctx.scope.has_table(t) && !self.outer.iter().any(|s| s.has_table(t)) => Err(SqlError::new(
                codes::UNDEFINED_TABLE,
                format!("missing FROM-clause entry for table \"{t}\""),
            )),
            Some(t) => Err(SqlError::new(codes::UNDEFINED_COLUMN, format!("column {t}.{name} does not exist"))),
            None => Err(SqlError::new(codes::UNDEFINED_COLUMN, format!("column \"{name}\" does not exist"))),
        }
    }

    fn bind_value(&mut self, v: &ast::Value) -> Result<Expr> {
        use ast::Value as V;
        Ok(match v {
            V::Number(s, _) => number_literal(s)?,
            V::SingleQuotedString(s) | V::DollarQuotedString(ast::DollarQuotedString { value: s, .. }) => {
                Expr::Literal(Datum::text(s), SqlType::Unknown)
            }
            V::EscapedStringLiteral(s) | V::UnicodeStringLiteral(s) | V::NationalStringLiteral(s) => {
                Expr::Literal(Datum::text(s), SqlType::Unknown)
            }
            V::DoubleQuotedString(s) => Expr::Literal(Datum::text(s), SqlType::Unknown),
            V::HexStringLiteral(s) => {
                // the literal may hold any characters: check them before
                // slicing by byte offsets
                if let Some(c) = s.chars().find(|c| !c.is_ascii_hexdigit()) {
                    sql_bail!(codes::INVALID_TEXT_REPRESENTATION, "\"{c}\" is not a valid hexadecimal digit");
                }
                let bytes = (0..s.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&s[i..(i + 2).min(s.len())], 16))
                    .collect::<std::result::Result<Vec<u8>, _>>()
                    .map_err(|_| SqlError::syntax("invalid hex string"))?;
                Expr::Literal(Datum::bytea(bytes), SqlType::Bytea)
            }
            V::Boolean(b) => Expr::bool(*b),
            V::Null => Expr::Literal(Datum::Null, SqlType::Unknown),
            V::Placeholder(p) => {
                let n: usize = p
                    .trim_start_matches('$')
                    .parse()
                    .map_err(|_| SqlError::syntax(format!("bad parameter placeholder {p}")))?;
                if n == 0 {
                    sql_bail!(codes::UNDEFINED_PARAMETER, "there is no parameter ${n}");
                }
                while self.params.len() < n {
                    self.params.push(super::binder::ParamSlot { ty: SqlType::Unknown, value: None });
                }
                let slot = &self.params[n - 1];
                match &slot.value {
                    Some(d) => Expr::Literal(d.clone(), slot.ty.clone()),
                    // described without a value: its context gives the type
                    None if slot.ty == SqlType::Unknown => param_marker(n - 1),
                    None => Expr::Literal(Datum::Null, slot.ty.clone()),
                }
            }
            other => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unsupported literal {other}"),
        })
    }

    /// `CASE oid WHEN o1 THEN 'text1' ... END` over a catalog mapping; with
    /// `keep`, unknown OIDs print as numbers (as reg* types do).
    fn oid_lookup(&self, x: Expr, kind: &str, keep: bool) -> Expr {
        let whens: Vec<(Expr, Expr)> = self
            .catalog
            .definitions(kind)
            .into_iter()
            .map(|(o, s)| {
                let eq = Expr::IsDistinct(Box::new(x.clone()), Box::new(Expr::Literal(Datum::Oid(o), SqlType::Oid)), true);
                (eq, Expr::Literal(Datum::text_owned(s), SqlType::Text))
            })
            .collect();
        let else_ = if keep { Expr::Cast(Box::new(x), SqlType::Text, -1, true) } else { Expr::null(SqlType::Text) };
        if whens.is_empty() {
            return else_;
        }
        Expr::Case(Box::new(crate::expr::CaseExpr { whens, else_, ty: SqlType::Text }))
    }

    /// The OID named by the input of a reg* type (regclassin etc.): a
    /// possibly schema-qualified, possibly quoted name.
    fn reg_name_oid(&self, kind: &str, input: &str) -> Result<u32> {
        let defs = self.catalog.definitions(kind);
        let find = |n: &str| defs.iter().find(|(_, d)| d == n).map(|(o, _)| *o);
        if kind == "regtype" {
            if let Some(o) = find(input.trim()) {
                return Ok(o);
            }
            let ty = sqlparser::parser::Parser::new(&sqlparser::dialect::PostgreSqlDialect {})
                .try_with_sql(input)
                .and_then(|mut p| p.parse_data_type())
                .ok()
                .and_then(|dt| sql_type_of(&dt).ok());
            return match ty {
                Some((t, _)) => Ok(t.oid()),
                None => Err(SqlError::new(codes::UNDEFINED_OBJECT, format!("type \"{input}\" does not exist"))),
            };
        }
        // the last dotted part, unquoted (else lowercased)
        let mut parts = Vec::new();
        let mut cur = String::new();
        let (mut quoted, mut chars) = (false, input.trim().chars().peekable());
        let mut was_quoted = false;
        while let Some(c) = chars.next() {
            match c {
                '"' if quoted && chars.peek() == Some(&'"') => {
                    cur.push('"');
                    chars.next();
                }
                '"' => {
                    quoted = !quoted;
                    was_quoted = true;
                }
                '.' if !quoted => {
                    parts.push((std::mem::take(&mut cur), was_quoted));
                    was_quoted = false;
                }
                c => cur.push(c),
            }
        }
        parts.push((cur, was_quoted));
        let (name, q) = parts.last().cloned().unwrap_or_default();
        let name = if q { name } else { name.to_lowercase() };
        find(&name).ok_or_else(|| match kind {
            "regnamespace" => SqlError::new("3F000", format!("schema \"{name}\" does not exist")),
            _ => SqlError::new(codes::UNDEFINED_TABLE, format!("relation \"{input}\" does not exist")),
        })
    }

    /// (start, end|length) OVERLAPS (start, end|length).
    fn bind_overlaps(&mut self, left: &ast::Expr, right: &ast::Expr, ctx: &Ctx) -> Result<Expr> {
        let mut ends = Vec::with_capacity(4);
        for (side, e) in [("left", left), ("right", right)] {
            let ast::Expr::Tuple(items) = e else {
                sql_bail!(codes::SYNTAX_ERROR, "OVERLAPS requires a pair of values on the {side} side");
            };
            if items.len() != 2 {
                sql_bail!(codes::SYNTAX_ERROR, "wrong number of parameters on {side} side of OVERLAPS expression");
            }
            let s = self.bind_expr(&items[0], ctx)?;
            let mut e2 = self.bind_expr(&items[1], ctx)?;
            let (st, et) = (self.ty_of(&s, ctx), self.ty_of(&e2, ctx));
            if et == SqlType::Interval {
                // a length: the end is start + length
                e2 = self.make_binary("+", s.clone(), st.clone(), e2, et)?;
            }
            ends.push(s);
            ends.push(e2);
        }
        let types: Vec<SqlType> = ends.iter().map(|e| self.ty_of(e, ctx)).collect();
        // dates compare as timestamps
        let t = match common_type(&types, "OVERLAPS")? {
            SqlType::Date | SqlType::Unknown => SqlType::Timestamp,
            t @ (SqlType::Timestamp | SqlType::TimestampTz | SqlType::Time) => t,
            other => sql_bail!(codes::UNDEFINED_FUNCTION, "function overlaps({other}, {other}, {other}, {other}) does not exist"),
        };
        let mut args = Vec::with_capacity(4);
        for (e, et) in ends.into_iter().zip(&types) {
            args.push(self.coerce(e, et, &t, CastContext::Implicit, "OVERLAPS")?);
        }
        self.make_call("overlaps", args, vec![t.clone(), t.clone(), t.clone(), t])
    }

    /// Row comparison: fields compare left to right and the first unequal
    /// (or null) pair decides.
    fn bind_row_compare(&mut self, l: &[ast::Expr], op: &ast::BinaryOperator, r: &[ast::Expr], ctx: &Ctx) -> Result<Expr> {
        use ast::BinaryOperator as B;
        if l.len() != r.len() {
            sql_bail!(codes::SYNTAX_ERROR, "unequal number of entries in row expressions");
        }
        let pair = |b: &mut Self, i: usize, op: B| b.bind_binary(&l[i], &op, &r[i], ctx);
        let n = l.len();
        Ok(match op {
            B::Eq => Expr::And((0..n).map(|i| pair(self, i, B::Eq)).collect::<Result<_>>()?),
            B::NotEq => Expr::Or((0..n).map(|i| pair(self, i, B::NotEq)).collect::<Result<_>>()?),
            B::Lt | B::LtEq | B::Gt | B::GtEq => {
                let strict = match op {
                    B::LtEq => B::Lt,
                    B::GtEq => B::Gt,
                    o => o.clone(),
                };
                // built from the last field: e_i = l_i < r_i OR (l_i = r_i AND e_{i+1})
                let mut acc = pair(self, n - 1, op.clone())?;
                for i in (0..n - 1).rev() {
                    let lt = pair(self, i, strict.clone())?;
                    let eq = pair(self, i, B::Eq)?;
                    acc = Expr::Or(vec![lt, Expr::And(vec![eq, acc])]);
                }
                acc
            }
            other => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "operator {other} is not supported for row expressions"),
        })
    }

    fn bind_binary(&mut self, left: &ast::Expr, op: &ast::BinaryOperator, right: &ast::Expr, ctx: &Ctx) -> Result<Expr> {
        use ast::BinaryOperator as B;
        if matches!(op, B::Overlaps) {
            return self.bind_overlaps(left, right, ctx);
        }
        if let (ast::Expr::Tuple(l), ast::Expr::Tuple(r)) = (left, right) {
            return self.bind_row_compare(l, op, r, ctx);
        }
        match op {
            B::And | B::Or => {
                // a chain of ANDs (ORs) is one n-ary node: generated SQL has
                // chains of thousands of terms, too deep to bind recursively
                let mut terms = Vec::new();
                let mut stack = vec![right, left];
                while let Some(x) = stack.pop() {
                    match x {
                        ast::Expr::BinaryOp { left: l, op: o, right: r } if o == op => {
                            stack.push(r);
                            stack.push(l);
                        }
                        ast::Expr::Nested(inner) if matches!(&**inner, ast::Expr::BinaryOp { op: o, .. } if o == op) => stack.push(inner),
                        other => terms.push(other),
                    }
                }
                let what = if matches!(op, B::And) { "AND" } else { "OR" };
                let mut bound = Vec::with_capacity(terms.len());
                for t in terms {
                    let b = self.bind_expr(t, ctx)?;
                    bound.push(self.coerce_bool(b, what)?);
                }
                Ok(match op {
                    B::And => Expr::And(bound),
                    _ => Expr::Or(bound),
                })
            }
            B::PGLikeMatch | B::PGILikeMatch | B::PGNotLikeMatch | B::PGNotILikeMatch => {
                let ci = matches!(op, B::PGILikeMatch | B::PGNotILikeMatch);
                let neg = matches!(op, B::PGNotLikeMatch | B::PGNotILikeMatch);
                self.bind_like(left, right, None, ci, neg, ctx)
            }
            B::PGCustomBinaryOperator(parts) => {
                let name = parts.last().cloned().unwrap_or_default();
                let l = self.bind_expr(left, ctx)?;
                let r = self.bind_expr(right, ctx)?;
                let (lt, rt) = (self.ty_of(&l, ctx), self.ty_of(&r, ctx));
                self.make_binary(&name, l, lt, r, rt)
            }
            _ => {
                let name = binop_name(op).ok_or_else(|| SqlError::unsupported(format!("operator {op} is not supported")))?;
                let l = self.bind_expr(left, ctx)?;
                let r = self.bind_expr(right, ctx)?;
                let (lt, rt) = (self.ty_of(&l, ctx), self.ty_of(&r, ctx));
                // || concatenates character(n) values as text (PostgreSQL
                // resolves textcat / anytextcat / textanycat), i.e. unpadded
                if name == "||" && (lt == SqlType::Bpchar || rt == SqlType::Bpchar) {
                    let arrays = matches!(lt, SqlType::Array(_)) || matches!(rt, SqlType::Array(_));
                    if !arrays {
                        let (l, lt) = if lt == SqlType::Bpchar { (bpchar_to_text(l), SqlType::Text) } else { (l, lt) };
                        let (r, rt) = if rt == SqlType::Bpchar { (bpchar_to_text(r), SqlType::Text) } else { (r, rt) };
                        return self.make_binary(name, l, lt, r, rt);
                    }
                }
                // text-key json operators with unknown literals
                self.make_binary(name, l, lt, r, rt)
            }
        }
    }

    fn bind_unary(&mut self, op: &ast::UnaryOperator, expr: &ast::Expr, ctx: &Ctx) -> Result<Expr> {
        use ast::UnaryOperator as U;
        match op {
            U::Not => {
                let x = self.bind_expr(expr, ctx)?;
                let x = self.coerce_bool(x, "NOT")?;
                Ok(Expr::Not(Box::new(x)))
            }
            U::Minus => {
                // fold negative numeric literals so that -2147483648 is int4
                if let ast::Expr::Value(v) = expr
                    && let ast::Value::Number(s, _) = &v.value
                {
                    return number_literal(&format!("-{s}"));
                }
                let x = self.bind_expr(expr, ctx)?;
                let t = self.ty_of(&x, ctx);
                self.make_call("-", vec![x], vec![t])
            }
            U::Plus => {
                let x = self.bind_expr(expr, ctx)?;
                let t = self.ty_of(&x, ctx);
                self.make_call("+", vec![x], vec![t])
            }
            U::PGAbs => {
                let x = self.bind_expr(expr, ctx)?;
                let t = self.ty_of(&x, ctx);
                self.make_call("@", vec![x], vec![t])
            }
            U::BitwiseNot => {
                let x = self.bind_expr(expr, ctx)?;
                let t = self.ty_of(&x, ctx);
                self.make_call("~", vec![x], vec![t])
            }
            U::PGSquareRoot => {
                let x = self.bind_expr(expr, ctx)?;
                let t = self.ty_of(&x, ctx);
                self.make_call("sqrt", vec![x], vec![t])
            }
            U::PGCubeRoot => {
                let x = self.bind_expr(expr, ctx)?;
                let t = self.ty_of(&x, ctx);
                self.make_call("cbrt", vec![x], vec![t])
            }
            _ => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unary operator {op} is not supported"),
        }
    }

    fn bind_case(
        &mut self,
        operand: Option<&ast::Expr>,
        conditions: &[ast::CaseWhen],
        else_result: Option<&ast::Expr>,
        ctx: &Ctx,
    ) -> Result<Expr> {
        let op = match operand {
            Some(o) => Some(self.bind_expr(o, ctx)?),
            None => None,
        };
        let mut whens = Vec::with_capacity(conditions.len());
        let mut results = Vec::with_capacity(conditions.len() + 1);
        for cw in conditions {
            let c = self.bind_expr(&cw.condition, ctx)?;
            let cond = match &op {
                Some(o) => {
                    let (ot, ct) = (self.ty_of(o, ctx), self.ty_of(&c, ctx));
                    self.make_binary("=", o.clone(), ot, c, ct)?
                }
                None => self.coerce_bool(c, "CASE/WHEN")?,
            };
            whens.push(cond);
            results.push(self.bind_expr(&cw.result, ctx)?);
        }
        let else_ = match else_result {
            Some(e) => self.bind_expr(e, ctx)?,
            None => Expr::Literal(Datum::Null, SqlType::Unknown),
        };
        results.push(else_);
        let types: Vec<SqlType> = results.iter().map(|r| self.ty_of(r, ctx)).collect();
        let t = common_type(&types, "CASE")?;
        let mut coerced = Vec::with_capacity(results.len());
        for (r, rt) in results.into_iter().zip(types) {
            coerced.push(self.coerce(r, &rt, &t, CastContext::Implicit, "CASE")?);
        }
        let else_ = coerced.pop().unwrap();
        Ok(Expr::Case(Box::new(CaseExpr { whens: whens.into_iter().zip(coerced).collect(), else_, ty: t })))
    }

    pub(crate) fn bind_subquery(&mut self, q: &ast::Query, ctx: &Ctx) -> Result<Rel> {
        self.outer.push(ctx.scope.clone());
        let saved_ctes = self.ctes.clone();
        let r = self.bind_query(q);
        self.ctes = saved_ctes;
        self.outer.pop();
        Ok(r?.into_rel())
    }

    // ------------------------------------------------------------------ functions

    fn function_args(&mut self, f: &ast::Function) -> Result<(Vec<ast::FunctionArg>, Option<ast::DuplicateTreatment>, Vec<ast::OrderByExpr>, bool)> {
        match &f.args {
            ast::FunctionArguments::None => Ok((vec![], None, vec![], false)),
            ast::FunctionArguments::Subquery(_) => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "subquery function arguments"),
            ast::FunctionArguments::List(l) => {
                let mut order = Vec::new();
                for c in &l.clauses {
                    match c {
                        ast::FunctionArgumentClause::OrderBy(o) => order = o.clone(),
                        ast::FunctionArgumentClause::IgnoreOrRespectNulls(_) => {}
                        _ => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unsupported function argument clause"),
                    }
                }
                let star = l.args.iter().any(|a| matches!(a, ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard)));
                Ok((l.args.clone(), l.duplicate_treatment, order, star))
            }
        }
    }

    /// ARRAY(subquery): the subquery's single column as an array, in its
    /// ORDER BY order (empty, not NULL, when it returns no rows).
    fn bind_array_subquery(&mut self, q: &ast::Query, ctx: &Ctx) -> Result<Expr> {
        self.outer.push(ctx.scope.clone());
        let saved_ctes = self.ctes.clone();
        let r = self.bind_query(q);
        self.ctes = saved_ctes;
        self.outer.pop();
        let bq = r?;
        if bq.visible != 1 {
            sql_bail!(codes::SYNTAX_ERROR, "subquery must return only one column");
        }
        // arrays of arrays accumulate into one more dimension
        let arr = bq.rel.schema()[0].ty.clone().array_of();
        let elem = arr.elem().cloned().unwrap_or(SqlType::Text);
        let rel = if bq.limit.is_some() || bq.offset > 0 {
            Rel::TopK {
                input: Box::new(bq.rel),
                group: vec![],
                order: bq.order.clone(),
                limit: bq.limit,
                offset: bq.offset,
                with_ties: bq.with_ties,
            }
        } else {
            bq.rel
        };
        let agg = AggCall {
            func: AggFunc::ArrayAgg,
            args: vec![0],
            distinct: false,
            filter: None,
            order: bq.order,
            fraction: None,
            ret: ColumnType::new(arr.clone()),
        };
        let rel = Rel::Aggregate { input: Box::new(rel), group: vec![], aggs: vec![agg] };
        let sub = Expr::Subquery(Box::new(SubqueryExpr { kind: SubqueryKind::Scalar, rel, ty: arr.clone() }));
        let empty = Expr::Literal(Datum::array(crate::types::ArrayValue::new_1d(elem, vec![])), arr.clone());
        Ok(Expr::Coalesce(vec![sub, empty], arr))
    }

    fn bind_function(&mut self, f: &ast::Function, ctx: &Ctx) -> Result<Expr> {
        let (_, name) = object_name(&f.name)?;
        if let ast::FunctionArguments::Subquery(q) = &f.args
            && name == "array"
        {
            return self.bind_array_subquery(q, ctx);
        }
        let (args, dup, order, star) = self.function_args(f)?;
        if f.over.is_some() {
            return self.bind_window(&name, f, &args, dup, star, ctx);
        }
        if is_aggregate_name(&name) {
            return self.bind_aggregate(&name, f, &args, dup, &order, star, ctx, None);
        }
        if WINDOW_ONLY.contains(&name.as_str()) {
            sql_bail!(codes::WINDOWING_ERROR, "window function {name} requires an OVER clause");
        }
        if f.filter.is_some() {
            sql_bail!(codes::WRONG_OBJECT_TYPE, "FILTER specified, but {name} is not an aggregate function");
        }
        // positional arguments (named only for make_interval)
        let mut bound = Vec::with_capacity(args.len());
        if name == "make_interval" && args.iter().any(|a| named_arg(a).is_some()) {
            const NAMES: [&str; 7] = ["years", "months", "weeks", "days", "hours", "mins", "secs"];
            let mut slots: Vec<Option<Expr>> = vec![None; 7];
            for (i, a) in args.iter().enumerate() {
                match (named_arg(a), a) {
                    (Some((n, ast::FunctionArgExpr::Expr(e))), _) => {
                        let k = NAMES.iter().position(|x| *x == n).ok_or_else(|| {
                            SqlError::new(codes::UNDEFINED_FUNCTION, format!("make_interval has no parameter \"{n}\""))
                        })?;
                        slots[k] = Some(self.bind_expr(e, ctx)?);
                    }
                    (None, ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e))) if i < 7 => {
                        slots[i] = Some(self.bind_expr(e, ctx)?)
                    }
                    _ => sql_bail!(codes::SYNTAX_ERROR, "bad make_interval argument"),
                }
            }
            for (k, s) in slots.into_iter().enumerate() {
                bound.push(s.unwrap_or(if k == 6 {
                    Expr::Literal(Datum::Float8(0.0), SqlType::Float8)
                } else {
                    Expr::Literal(Datum::Int4(0), SqlType::Int4)
                }));
            }
        } else if JSONPATH_FUNCS.contains(&name.as_str()) {
            bound = self.bind_jsonpath_args(&name, &args, &mut |b, e| b.bind_expr(e, ctx))?;
        } else {
            for a in &args {
                match a {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => bound.push(self.bind_expr(e, ctx)?),
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => {
                        sql_bail!(codes::SYNTAX_ERROR, "{name}(*) is only allowed for aggregates")
                    }
                    _ => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "named arguments are not supported for {name}"),
                }
            }
        }
        let types: Vec<SqlType> = bound.iter().map(|b| self.ty_of(b, ctx)).collect();
        match name.as_str() {
            "coalesce" | "greatest" | "least" => {
                if bound.is_empty() {
                    sql_bail!(codes::SYNTAX_ERROR, "{name} requires at least one argument");
                }
                let t = common_type(&types, &name.to_uppercase())?;
                if name != "coalesce" {
                    resolve("<", &[t.clone(), t.clone()])?;
                }
                let mut out = Vec::with_capacity(bound.len());
                for (b, bt) in bound.into_iter().zip(types) {
                    out.push(self.coerce(b, &bt, &t, CastContext::Implicit, &name)?);
                }
                return Ok(match name.as_str() {
                    "coalesce" => Expr::Coalesce(out, t),
                    "greatest" => Expr::Greatest(out, t),
                    _ => Expr::Least(out, t),
                });
            }
            "nullif" => {
                if bound.len() != 2 {
                    sql_bail!(codes::UNDEFINED_FUNCTION, "function nullif requires 2 arguments");
                }
                let res = resolve("=", &types)?;
                // PostgreSQL compares these with cross-type operators, so the
                // first argument (and the result) keeps its own type
                let datetime = |t: &SqlType| matches!(t, SqlType::Date | SqlType::Timestamp | SqlType::TimestampTz);
                let (t0, t1) = (&types[0], &types[1]);
                if res.arg_types[0] != *t0
                    && ((t0.is_integer() && t1.is_integer()) || (t0.is_float() && t1.is_float()) || (datetime(t0) && datetime(t1)))
                {
                    let eq = self.make_binary("=", bound[0].clone(), t0.clone(), bound[1].clone(), t1.clone())?;
                    let whens = vec![(eq, Expr::null(t0.clone()))];
                    return Ok(Expr::Case(Box::new(CaseExpr { whens, else_: bound[0].clone(), ty: t0.clone() })));
                }
                let mut it = bound.into_iter();
                let a = self.coerce(it.next().unwrap(), &types[0], &res.arg_types[0], CastContext::Implicit, "NULLIF")?;
                let b = self.coerce(it.next().unwrap(), &types[1], &res.arg_types[1], CastContext::Implicit, "NULLIF")?;
                if res.arg_types == [SqlType::Bpchar, SqlType::Bpchar] {
                    // compare without trailing spaces, return the padded value
                    let eq = self.make_binary("=", a.clone(), SqlType::Bpchar, b, SqlType::Bpchar)?;
                    let whens = vec![(eq, Expr::null(SqlType::Bpchar))];
                    return Ok(Expr::Case(Box::new(CaseExpr { whens, else_: a, ty: SqlType::Bpchar })));
                }
                return Ok(Expr::NullIf(Box::new(a), Box::new(b)));
            }
            // definitions come from the catalog: inlined as OID mappings
            "pg_get_indexdef" | "pg_get_constraintdef" | "pg_get_viewdef" if !bound.is_empty() => {
                let kind = match name.as_str() {
                    "pg_get_indexdef" => "indexdef",
                    "pg_get_constraintdef" => "constraintdef",
                    _ => "viewdef",
                };
                let x = self.coerce(bound[0].clone(), &types[0], &SqlType::Oid, CastContext::Implicit, &name)?;
                return Ok(self.oid_lookup(x, kind, false));
            }
            "pg_typeof" => {
                if bound.len() != 1 {
                    sql_bail!(codes::UNDEFINED_FUNCTION, "function pg_typeof requires 1 argument");
                }
                return Ok(Expr::Literal(Datum::text(&types[0].name()), SqlType::Text));
            }
            "version" => return Ok(Expr::Literal(Datum::text("PostgreSQL 17.0 (ivm)"), SqlType::Text)),
            "current_database" => return Ok(Expr::Literal(Datum::text("ivm"), SqlType::Name)),
            "current_schema" => return Ok(Expr::Literal(Datum::text("public"), SqlType::Name)),
            "generate_series" | "unnest" | "jsonb_array_elements" | "jsonb_array_elements_text" | "jsonb_each"
            | "jsonb_each_text" | "jsonb_object_keys" | "regexp_split_to_table" | "regexp_matches" | "jsonb_path_query"
            | "jsonb_path_query_tz" => {
                // in the select list: expanded after aggregation and windows
                if let Some(col) = ctx.collector
                    && ctx.clause == "SELECT"
                    && !ctx.in_agg
                {
                    let (func, args, out, _) = self.resolve_table_function(&name, bound, types)?;
                    if out.len() != 1 {
                        sql_bail!(codes::FEATURE_NOT_SUPPORTED, "set-returning function {name} in the select list must return one column");
                    }
                    let mut fp = format!("{func:?}").into_bytes();
                    for a in &args {
                        a.fingerprint(&mut fp);
                    }
                    let ty = out[0].ty.clone();
                    let mut c = col.borrow_mut();
                    let k = match c.srfs.iter().position(|s| s.fp == fp) {
                        Some(k) => k,
                        None => {
                            c.srfs.push(super::binder::PendingSrf { func, args, out: out[0].clone(), fp });
                            c.srfs.len() - 1
                        }
                    };
                    return Ok(Expr::SrfRef(k, ty));
                }
                sql_bail!(codes::FEATURE_NOT_SUPPORTED, "set-returning functions are not allowed in {}", ctx.clause);
            }
            _ => {}
        }
        if !crate::expr::func::function_exists(&name) {
            sql_bail!(
                codes::UNDEFINED_FUNCTION,
                "function {}({}) does not exist",
                name,
                types.iter().map(|t| t.name()).collect::<Vec<_>>().join(", ")
            );
        }
        self.make_call(&name, bound, types)
    }

    #[allow(clippy::too_many_arguments)]
    fn bind_aggregate(
        &mut self,
        name: &str,
        f: &ast::Function,
        args: &[ast::FunctionArg],
        dup: Option<ast::DuplicateTreatment>,
        order_by: &[ast::OrderByExpr],
        star: bool,
        ctx: &Ctx,
        // context for the arguments, FILTER and ORDER BY when it is not the
        // aggregate's own (window aggregates: they may contain aggregates)
        args_ctx: Option<&Ctx>,
    ) -> Result<Expr> {
        let Some(collector) = ctx.collector else {
            sql_bail!(codes::GROUPING_ERROR, "aggregate functions are not allowed in {}", ctx.clause);
        };
        if ctx.in_agg {
            sql_bail!(codes::GROUPING_ERROR, "aggregate function calls cannot be nested");
        }
        let own = Ctx { scope: ctx.scope, collector: ctx.collector, in_agg: true, clause: ctx.clause };
        let inner = args_ctx.unwrap_or(&own);
        let mut bound = Vec::new();
        if !star {
            for a in args {
                match a {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => bound.push(self.bind_expr(e, inner)?),
                    _ => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unsupported aggregate argument"),
                }
            }
        }
        let distinct = matches!(dup, Some(ast::DuplicateTreatment::Distinct));
        let filter = match &f.filter {
            Some(fe) => {
                let e = self.bind_expr(fe, inner)?;
                Some(self.coerce_bool(e, "FILTER")?)
            }
            None => None,
        };
        let mut order: Vec<(Expr, SortSpec)> = Vec::new();
        for ob in order_by.iter().chain(f.within_group.iter()) {
            let e = self.bind_expr(&ob.expr, inner)?;
            order.push((e, super::binder::sort_spec(&ob.options)));
        }
        // An aggregate whose arguments reference only columns of enclosing
        // queries belongs to the nearest such query in PostgreSQL (it is
        // evaluated over that query's rows); evaluating it over this
        // query's rows would silently give other results. (Window
        // aggregates are always evaluated at their own level.)
        let (mut local, mut outer) = (false, false);
        for e in bound.iter().chain(filter.iter()).chain(order.iter().map(|(e, _)| e)) {
            var_levels(e, &mut local, &mut outer);
        }
        if outer && !local && args_ctx.is_none() {
            sql_bail!(
                codes::FEATURE_NOT_SUPPORTED,
                "aggregate functions whose arguments reference only columns of an outer query are not supported"
            );
        }
        let mut types: Vec<SqlType> = bound.iter().map(|b| self.ty_of(b, ctx)).collect();
        use AggFunc as A;
        let mut fraction = None;
        let (func, ret): (AggFunc, SqlType) = match (name, bound.len()) {
            ("count", 0) if star => (A::CountStar, SqlType::Int8),
            ("count", 1) => (A::Count, SqlType::Int8),
            ("sum", 1) => {
                let t = numeric_arg(&mut bound, &mut types, "sum")?;
                (
                    A::Sum,
                    match t {
                        SqlType::Int2 | SqlType::Int4 => SqlType::Int8,
                        SqlType::Int8 | SqlType::Numeric => SqlType::Numeric,
                        SqlType::Float4 => SqlType::Float4,
                        SqlType::Float8 => SqlType::Float8,
                        other => sql_bail!(codes::UNDEFINED_FUNCTION, "function sum({other}) does not exist"),
                    },
                )
            }
            ("avg", 1) => {
                let t = numeric_arg(&mut bound, &mut types, "avg")?;
                (
                    A::Avg,
                    match t {
                        SqlType::Int2 | SqlType::Int4 | SqlType::Int8 | SqlType::Numeric => SqlType::Numeric,
                        SqlType::Float4 | SqlType::Float8 => SqlType::Float8,
                        other => sql_bail!(codes::UNDEFINED_FUNCTION, "function avg({other}) does not exist"),
                    },
                )
            }
            ("min" | "max", 1) => {
                let t = if types[0] == SqlType::Unknown {
                    bound[0] = self.coerce(bound[0].clone(), &SqlType::Unknown, &SqlType::Text, CastContext::Implicit, name)?;
                    SqlType::Text
                } else {
                    types[0].clone()
                };
                resolve("<", &[t.clone(), t.clone()]).map_err(|_| {
                    SqlError::new(codes::UNDEFINED_FUNCTION, format!("function {name}({t}) does not exist"))
                })?;
                (if name == "min" { A::Min } else { A::Max }, t)
            }
            ("bool_and" | "every" | "bool_or", 1) => {
                bound[0] = self.coerce(bound[0].clone(), &types[0], &SqlType::Bool, CastContext::Implicit, name)?;
                (if name == "bool_or" { A::BoolOr } else { A::BoolAnd }, SqlType::Bool)
            }
            ("string_agg", 2) => {
                let t = if types[0] == SqlType::Bytea { SqlType::Bytea } else { SqlType::Text };
                for i in 0..2 {
                    bound[i] = self.coerce(bound[i].clone(), &types[i], &t, CastContext::Implicit, name)?;
                }
                (A::StringAgg, t)
            }
            ("array_agg", 1) => {
                let t = if types[0] == SqlType::Unknown { SqlType::Text } else { types[0].clone() };
                bound[0] = self.coerce(bound[0].clone(), &types[0], &t, CastContext::Implicit, name)?;
                (A::ArrayAgg, t.array_of())
            }
            ("json_agg", 1) => (A::JsonAgg, SqlType::Json),
            ("jsonb_agg", 1) => (A::JsonbAgg, SqlType::Jsonb),
            ("json_object_agg", 2) => (A::JsonObjectAgg, SqlType::Json),
            ("jsonb_object_agg", 2) => (A::JsonbObjectAgg, SqlType::Jsonb),
            ("stddev" | "stddev_samp" | "stddev_pop" | "variance" | "var_samp" | "var_pop", 1) => {
                let t = numeric_arg(&mut bound, &mut types, name)?;
                let func = match name {
                    "stddev" | "stddev_samp" => A::StddevSamp,
                    "stddev_pop" => A::StddevPop,
                    "variance" | "var_samp" => A::VarSamp,
                    _ => A::VarPop,
                };
                (func, if t.is_float() { SqlType::Float8 } else { SqlType::Numeric })
            }
            ("percentile_cont" | "percentile_disc", 1) => {
                if order.len() != 1 {
                    sql_bail!(codes::SYNTAX_ERROR, "{name} requires WITHIN GROUP (ORDER BY ...)");
                }
                let fr = bound[0].clone().fold()?;
                let v = match crate::expr::cast::cast_datum(
                    &match fr {
                        Expr::Literal(d, _) => d,
                        _ => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "{name} fraction must be a constant"),
                    },
                    &SqlType::Float8,
                    -1,
                    false,
                )? {
                    Datum::Float8(v) => v,
                    _ => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "{name} fraction must not be null"),
                };
                if !(0.0..=1.0).contains(&v) {
                    sql_bail!(codes::NUMERIC_VALUE_OUT_OF_RANGE, "percentile value {v} is not between 0 and 1");
                }
                fraction = Some(v);
                let (oe, _) = &order[0];
                let ot = self.ty_of(oe, ctx);
                let (func, ret) = if name == "percentile_cont" {
                    let oe2 = self.coerce(oe.clone(), &ot, &SqlType::Float8, CastContext::Implicit, name)?;
                    order[0].0 = oe2;
                    (A::PercentileCont, SqlType::Float8)
                } else {
                    (A::PercentileDisc, ot)
                };
                bound = vec![order[0].0.clone()];
                (func, ret)
            }
            ("mode", 0) => {
                if order.len() != 1 {
                    sql_bail!(codes::SYNTAX_ERROR, "mode requires WITHIN GROUP (ORDER BY ...)");
                }
                bound = vec![order[0].0.clone()];
                (A::Mode, self.ty_of(&order[0].0, ctx))
            }
            ("any_value", 1) => (A::AnyValue, types[0].clone()),
            _ => sql_bail!(
                codes::UNDEFINED_FUNCTION,
                "function {}({}) does not exist",
                name,
                types.iter().map(|t| t.name()).collect::<Vec<_>>().join(", ")
            ),
        };
        if distinct && func.is_ordered() && !order.is_empty() {
            // ORDER BY must match the argument for DISTINCT aggregates (PG rule)
        }
        // pin down order-sensitive aggregates without ORDER BY: order by arguments
        if func.is_ordered() && order.is_empty() {
            for b in &bound {
                order.push((b.clone(), SortSpec::ASC));
            }
        }
        let mut fp = Vec::new();
        fp.extend_from_slice(func.name().as_bytes());
        fp.push(distinct as u8);
        for b in &bound {
            b.fingerprint(&mut fp);
        }
        fp.push(0xFE);
        if let Some(fl) = &filter {
            fl.fingerprint(&mut fp);
        }
        fp.push(0xFD);
        for (o, s) in &order {
            o.fingerprint(&mut fp);
            fp.push(s.desc as u8);
            fp.push(s.nulls_first as u8);
        }
        let mut c = collector.borrow_mut();
        if let Some(k) = c.aggs.iter().position(|a| a.fp == fp) {
            return Ok(Expr::AggRef(k, c.aggs[k].ret.ty.clone()));
        }
        let ret_ct = ColumnType { ty: ret.clone(), typmod: -1, nullable: !func.empty_is_zero() };
        c.aggs.push(PendingAgg { func, args: bound, distinct, filter, order, fraction, ret: ret_ct, fp });
        Ok(Expr::AggRef(c.aggs.len() - 1, ret))
    }

    fn bind_window(
        &mut self,
        name: &str,
        f: &ast::Function,
        args: &[ast::FunctionArg],
        dup: Option<ast::DuplicateTreatment>,
        star: bool,
        ctx: &Ctx,
    ) -> Result<Expr> {
        let Some(collector) = ctx.collector else {
            sql_bail!(codes::WINDOWING_ERROR, "window functions are not allowed in {}", ctx.clause);
        };
        if ctx.in_agg {
            sql_bail!(codes::GROUPING_ERROR, "aggregate function calls cannot contain window function calls");
        }
        if matches!(dup, Some(ast::DuplicateTreatment::Distinct)) {
            sql_bail!(codes::FEATURE_NOT_SUPPORTED, "DISTINCT is not implemented for window functions");
        }
        let spec = match f.over.as_ref().unwrap() {
            ast::WindowType::WindowSpec(s) => self.resolve_window_spec(s, 0)?,
            ast::WindowType::NamedWindow(n) => self.named_window(n, 0)?,
        };
        // window arguments may contain aggregates (bound in the outer aggregation)
        let actx = Ctx { scope: ctx.scope, collector: ctx.collector, in_agg: false, clause: ctx.clause };
        let mut bound = Vec::new();
        if !star {
            for a in args {
                match a {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => bound.push(self.bind_expr(e, &actx)?),
                    _ => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unsupported window function argument"),
                }
            }
        }
        let mut partition = Vec::new();
        for p in &spec.partition_by {
            partition.push(self.bind_expr(p, &actx)?);
        }
        let mut order = Vec::new();
        for o in &spec.order_by {
            order.push((self.bind_expr(&o.expr, &actx)?, super::binder::sort_spec(&o.options)));
        }
        let mut range = None;
        let frame = match &spec.window_frame {
            None => Frame::default_frame(),
            Some(wf) => {
                let units = match wf.units {
                    ast::WindowFrameUnits::Rows => FrameUnits::Rows,
                    ast::WindowFrameUnits::Range => FrameUnits::Range,
                    ast::WindowFrameUnits::Groups => FrameUnits::Groups,
                };
                // RANGE offsets are values of the ORDER BY column's offset type
                let range_types = if units == FrameUnits::Range {
                    let has_offset = |b: &ast::WindowFrameBound| {
                        matches!(b, ast::WindowFrameBound::Preceding(Some(_)) | ast::WindowFrameBound::Following(Some(_)))
                    };
                    if has_offset(&wf.start_bound) || wf.end_bound.as_ref().is_some_and(has_offset) {
                        if order.len() != 1 {
                            sql_bail!(
                                codes::WINDOWING_ERROR,
                                "RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column"
                            );
                        }
                        let ot = self.ty_of(&order[0].0, ctx);
                        let off = match &ot {
                            SqlType::Date | SqlType::Timestamp | SqlType::TimestampTz | SqlType::Time | SqlType::Interval => SqlType::Interval,
                            t if t.is_numeric_family() => t.clone(),
                            t => sql_bail!(
                                codes::FEATURE_NOT_SUPPORTED,
                                "RANGE with offset PRECEDING/FOLLOWING is not supported for column type {t}"
                            ),
                        };
                        let cmp = if ot == SqlType::Date { SqlType::Timestamp } else { ot.clone() };
                        let minus = resolve("-", &[cmp.clone(), off.clone()])?.func;
                        let plus = resolve("+", &[cmp.clone(), off.clone()])?.func;
                        Some((off, minus, plus, (ot == SqlType::Date).then_some(cmp)))
                    } else {
                        None
                    }
                } else {
                    None
                };
                let mut offsets: [Option<Datum>; 2] = [None, None];
                let mut bound_of = |b: &mut Binder, x: &ast::WindowFrameBound, slot: usize| -> Result<FrameBound> {
                    Ok(match x {
                        ast::WindowFrameBound::CurrentRow => FrameBound::CurrentRow,
                        ast::WindowFrameBound::Preceding(None) => FrameBound::UnboundedPreceding,
                        ast::WindowFrameBound::Following(None) => FrameBound::UnboundedFollowing,
                        ast::WindowFrameBound::Preceding(Some(e)) | ast::WindowFrameBound::Following(Some(e)) => {
                            let s = Scope::default();
                            let mut v = b.bind_expr(e, &Ctx::plain(&s, "window frame"))?;
                            let target = match &range_types {
                                Some((off, ..)) => off.clone(),
                                None => SqlType::Int8,
                            };
                            let vt = v.ty(&[]);
                            v = b.coerce(v, &vt, &target, CastContext::Implicit, "window frame")?.fold()?;
                            let d = match v {
                                Expr::Literal(d, _) if !d.is_null() => d,
                                _ => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "window frame offsets must be non-null constants"),
                            };
                            let negative = match &d {
                                Datum::Interval(_) => d.sql_cmp(&Datum::Interval(Default::default())) == std::cmp::Ordering::Less,
                                other => other.as_f64().is_some_and(|f| f < 0.0),
                            };
                            if negative {
                                sql_bail!("22013", "invalid preceding or following size in window function");
                            }
                            let n = if range_types.is_some() { 0 } else { d.as_i64().unwrap_or(0) };
                            offsets[slot] = Some(d);
                            if matches!(x, ast::WindowFrameBound::Preceding(_)) {
                                FrameBound::Preceding(n)
                            } else {
                                FrameBound::Following(n)
                            }
                        }
                    })
                };
                let start = bound_of(self, &wf.start_bound, 0)?;
                let end = match &wf.end_bound {
                    Some(e) => bound_of(self, e, 1)?,
                    None => FrameBound::CurrentRow,
                };
                if let Some((_, minus, plus, cmp_type)) = range_types {
                    let [s, e] = offsets;
                    range = Some(crate::sql::plan::RangeOffsets { start: s, end: e, minus, plus, cmp_type });
                }
                Frame { units, start, end }
            }
        };
        let types: Vec<SqlType> = bound.iter().map(|b| self.ty_of(b, ctx)).collect();
        use WindowFunc as W;
        let (func, ret) = match (name, bound.len()) {
            ("row_number", 0) => (W::RowNumber, SqlType::Int8),
            ("rank", 0) => (W::Rank, SqlType::Int8),
            ("dense_rank", 0) => (W::DenseRank, SqlType::Int8),
            ("percent_rank", 0) => (W::PercentRank, SqlType::Float8),
            ("cume_dist", 0) => (W::CumeDist, SqlType::Float8),
            ("ntile", 1) => {
                bound[0] = self.coerce(bound[0].clone(), &types[0], &SqlType::Int4, CastContext::Implicit, "ntile")?;
                (W::Ntile, SqlType::Int4)
            }
            ("lag" | "lead", 1..=3) => {
                if bound.len() >= 2 {
                    bound[1] = self.coerce(bound[1].clone(), &types[1], &SqlType::Int4, CastContext::Implicit, name)?;
                }
                let t = if bound.len() == 3 { common_type(&[types[0].clone(), types[2].clone()], name)? } else { types[0].clone() };
                bound[0] = self.coerce(bound[0].clone(), &types[0], &t, CastContext::Implicit, name)?;
                if bound.len() == 3 {
                    bound[2] = self.coerce(bound[2].clone(), &types[2], &t, CastContext::Implicit, name)?;
                }
                (if name == "lag" { W::Lag } else { W::Lead }, t)
            }
            ("first_value", 1) => (W::FirstValue, types[0].clone()),
            ("last_value", 1) => (W::LastValue, types[0].clone()),
            ("nth_value", 2) => {
                bound[1] = self.coerce(bound[1].clone(), &types[1], &SqlType::Int4, CastContext::Implicit, name)?;
                (W::NthValue, types[0].clone())
            }
            (agg, _) if is_aggregate_name(agg) => {
                // bind as an aggregate to get its type, but with a private collector
                let tmp = std::cell::RefCell::new(super::binder::Collector::default());
                let tctx = Ctx { scope: ctx.scope, collector: Some(&tmp), in_agg: false, clause: ctx.clause };
                // aggregates in the arguments belong to the query's aggregation
                let _ = self.bind_aggregate(agg, f, args, dup, &[], star, &tctx, Some(&actx))?;
                let mut t = tmp.into_inner();
                if t.aggs.len() != 1 {
                    sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unsupported window aggregate");
                }
                let pa = t.aggs.pop().unwrap();
                if pa.filter.is_some() || pa.distinct {
                    sql_bail!(codes::FEATURE_NOT_SUPPORTED, "FILTER/DISTINCT in window aggregates is not supported");
                }
                bound = pa.args;
                (W::Agg(pa.func), pa.ret.ty)
            }
            _ => sql_bail!(
                codes::UNDEFINED_FUNCTION,
                "function {}({}) does not exist",
                name,
                types.iter().map(|t| t.name()).collect::<Vec<_>>().join(", ")
            ),
        };
        let mut c = collector.borrow_mut();
        let nullable = !matches!(func, W::RowNumber | W::Rank | W::DenseRank | W::PercentRank | W::CumeDist | W::Agg(AggFunc::Count) | W::Agg(AggFunc::CountStar));
        c.windows.push(PendingWindow {
            func,
            args: bound,
            partition,
            order,
            frame,
            ret: ColumnType { ty: ret.clone(), typmod: -1, nullable },
            range,
        });
        Ok(Expr::WindowRef(c.windows.len() - 1, ret))
    }

    /// A window of the WINDOW clause, fully resolved.
    fn named_window(&self, name: &ast::Ident, depth: usize) -> Result<ast::WindowSpec> {
        if depth > 32 {
            sql_bail!(codes::WINDOWING_ERROR, "window \"{}\" refers to itself", ident(name));
        }
        let n = ident(name);
        let Some(def) = self.named_windows.iter().find(|d| ident(&d.0) == n) else {
            sql_bail!(codes::UNDEFINED_OBJECT, "window \"{n}\" does not exist");
        };
        match &def.1 {
            ast::NamedWindowExpr::NamedWindow(other) => self.named_window(other, depth + 1),
            ast::NamedWindowExpr::WindowSpec(s) => self.resolve_window_spec(s, depth + 1),
        }
    }

    /// A window specification with its base window (`OVER (w ...)`) merged
    /// in, following PostgreSQL's rules for copying windows.
    fn resolve_window_spec(&self, spec: &ast::WindowSpec, depth: usize) -> Result<ast::WindowSpec> {
        let Some(base_name) = &spec.window_name else { return Ok(spec.clone()) };
        let base = self.named_window(base_name, depth + 1)?;
        let n = ident(base_name);
        if !spec.partition_by.is_empty() {
            sql_bail!(codes::WINDOWING_ERROR, "cannot override PARTITION BY clause of window \"{n}\"");
        }
        if !spec.order_by.is_empty() && !base.order_by.is_empty() {
            sql_bail!(codes::WINDOWING_ERROR, "cannot override ORDER BY clause of window \"{n}\"");
        }
        if base.window_frame.is_some() {
            sql_bail!(codes::WINDOWING_ERROR, "cannot copy window \"{n}\" because it has a frame clause");
        }
        Ok(ast::WindowSpec {
            window_name: None,
            partition_by: base.partition_by,
            order_by: if spec.order_by.is_empty() { base.order_by } else { spec.order_by.clone() },
            window_frame: spec.window_frame.clone(),
        })
    }

    /// Resolve a table function's signature: its coerced arguments, output
    /// columns and their default names.
    pub(crate) fn resolve_table_function(
        &mut self,
        name: &str,
        mut bound: Vec<Expr>,
        types: Vec<SqlType>,
    ) -> Result<(TableFunc, Vec<Expr>, Vec<ColumnType>, Vec<String>)> {
        let (func, out, names): (TableFunc, Vec<ColumnType>, Vec<String>) = match (name, bound.len()) {
            ("generate_series", 2 | 3) => {
                // the step's type follows from the bounds' (interval for
                // timestamps); untyped bounds take the step's
                let bounds = if types[0] == SqlType::Unknown && types[1] == SqlType::Unknown {
                    match types.get(2) {
                        Some(SqlType::Interval) => SqlType::TimestampTz,
                        Some(t) if t.is_numeric_family() => t.clone(),
                        _ => sql_bail!(
                            codes::AMBIGUOUS_FUNCTION,
                            "function generate_series({}) is not unique",
                            types.iter().map(|t| t.name()).collect::<Vec<_>>().join(", ")
                        ),
                    }
                } else {
                    common_type(&types[..2], "generate_series")?
                };
                // numeric bounds and step share one type
                let bounds = match (&bounds, types.get(2)) {
                    (b, Some(st)) if b.is_numeric_family() && st.is_numeric_family() => {
                        common_type(&[bounds.clone(), st.clone()], "generate_series")?
                    }
                    _ => bounds,
                };
                let t = match bounds {
                    SqlType::Int2 | SqlType::Int4 => SqlType::Int4,
                    SqlType::Int8 => SqlType::Int8,
                    SqlType::Numeric => SqlType::Numeric,
                    SqlType::Timestamp => SqlType::Timestamp,
                    // dates resolve to the timestamptz variant (preferred type)
                    SqlType::TimestampTz | SqlType::Date => SqlType::TimestampTz,
                    other => sql_bail!(codes::UNDEFINED_FUNCTION, "function generate_series({other}) does not exist"),
                };
                for i in 0..2 {
                    bound[i] = self.coerce(bound[i].clone(), &types[i], &t, CastContext::Implicit, "generate_series")?;
                }
                if bound.len() == 3 {
                    let st = if matches!(t, SqlType::Timestamp | SqlType::TimestampTz) { SqlType::Interval } else { t.clone() };
                    bound[2] = self.coerce(bound[2].clone(), &types[2], &st, CastContext::Implicit, "generate_series")?;
                } else if matches!(t, SqlType::Timestamp | SqlType::TimestampTz) {
                    sql_bail!(codes::UNDEFINED_FUNCTION, "generate_series over timestamps requires a step");
                }
                (TableFunc::GenerateSeries, vec![ColumnType::new(t).not_null()], vec!["generate_series".into()])
            }
            ("unnest", 1) => {
                let elem = match &types[0] {
                    SqlType::Array(e) => (**e).clone(),
                    other => sql_bail!(codes::UNDEFINED_FUNCTION, "function unnest({other}) does not exist"),
                };
                (TableFunc::Unnest, vec![ColumnType::new(elem)], vec!["unnest".into()])
            }
            ("jsonb_array_elements", 1) => (TableFunc::JsonbArrayElements, vec![ColumnType::new(SqlType::Jsonb)], vec!["value".into()]),
            ("jsonb_array_elements_text", 1) => {
                (TableFunc::JsonbArrayElementsText, vec![ColumnType::new(SqlType::Text)], vec!["value".into()])
            }
            ("jsonb_each", 1) => (
                TableFunc::JsonbEach,
                vec![ColumnType::new(SqlType::Text).not_null(), ColumnType::new(SqlType::Jsonb)],
                vec!["key".into(), "value".into()],
            ),
            ("jsonb_each_text", 1) => (
                TableFunc::JsonbEachText,
                vec![ColumnType::new(SqlType::Text).not_null(), ColumnType::new(SqlType::Text)],
                vec!["key".into(), "value".into()],
            ),
            ("jsonb_object_keys", 1) => {
                (TableFunc::JsonbObjectKeys, vec![ColumnType::new(SqlType::Text).not_null()], vec!["jsonb_object_keys".into()])
            }
            ("jsonb_path_query" | "jsonb_path_query_tz", 4) => {
                let tys = [SqlType::Jsonb, SqlType::JsonPath, SqlType::Jsonb, SqlType::Bool];
                for (i, t) in tys.iter().enumerate() {
                    bound[i] = self.coerce(bound[i].clone(), &types[i], t, CastContext::Implicit, name)?;
                }
                (
                    TableFunc::JsonbPathQuery { tz: name.ends_with("_tz") },
                    vec![ColumnType::new(SqlType::Jsonb).not_null()],
                    vec![name.to_string()],
                )
            }
            ("regexp_split_to_table", 2 | 3) => {
                for i in 0..bound.len() {
                    bound[i] = self.coerce(bound[i].clone(), &types[i], &SqlType::Text, CastContext::Implicit, name)?;
                }
                (TableFunc::RegexpSplitToTable, vec![ColumnType::new(SqlType::Text).not_null()], vec!["regexp_split_to_table".into()])
            }
            ("regexp_matches", 2 | 3) => {
                for i in 0..bound.len() {
                    bound[i] = self.coerce(bound[i].clone(), &types[i], &SqlType::Text, CastContext::Implicit, name)?;
                }
                (
                    TableFunc::RegexpMatches,
                    vec![ColumnType::new(SqlType::Array(Box::new(SqlType::Text))).not_null()],
                    vec!["regexp_matches".into()],
                )
            }
            _ => sql_bail!(codes::UNDEFINED_FUNCTION, "table function {name} with {} arguments does not exist", bound.len()),
        };
        if matches!(func, TableFunc::JsonbArrayElements | TableFunc::JsonbArrayElementsText | TableFunc::JsonbEach | TableFunc::JsonbEachText | TableFunc::JsonbObjectKeys) {
            bound[0] = self.coerce(bound[0].clone(), &types[0], &SqlType::Jsonb, CastContext::Implicit, name)?;
        }
        Ok((func, bound, out, names))
    }

    /// `x[..][..]` on an array: element access (one subscript per dimension,
    /// else NULL) or, if any subscript is a slice, a slice in which plain
    /// subscripts `n` mean `1:n`.
    fn bind_array_subscripts(&mut self, x: Expr, xt: SqlType, subs: &[&ast::Subscript], ctx: &Ctx) -> Result<Expr> {
        if subs.len() > crate::types::datum::MAX_ARRAY_DIMS {
            sql_bail!(
                codes::PROGRAM_LIMIT_EXCEEDED,
                "number of array dimensions ({}) exceeds the maximum allowed ({})",
                subs.len(),
                crate::types::datum::MAX_ARRAY_DIMS
            );
        }
        let int = |b: &mut Self, e: &ast::Expr| -> Result<Expr> {
            let v = b.bind_expr(e, ctx)?;
            let vt = b.ty_of(&v, ctx);
            b.coerce(v, &vt, &SqlType::Int4, CastContext::Implicit, "array subscript")
        };
        let lit = |v: i32| Expr::Literal(Datum::Int4(v), SqlType::Int4);
        let slice = subs.iter().any(|s| matches!(s, ast::Subscript::Slice { .. }));
        let mut args = vec![x];
        for s in subs {
            match s {
                ast::Subscript::Index { index } if slice => {
                    args.push(lit(1));
                    args.push(int(self, index)?);
                }
                ast::Subscript::Index { index } => args.push(int(self, index)?),
                ast::Subscript::Slice { lower_bound, upper_bound, stride: None } => {
                    // omitted bounds are the array's own
                    args.push(match lower_bound {
                        Some(e) => int(self, e)?,
                        None => lit(i32::MIN),
                    });
                    args.push(match upper_bound {
                        Some(e) => int(self, e)?,
                        None => lit(i32::MAX),
                    });
                }
                ast::Subscript::Slice { .. } => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "array slices with a stride are not supported"),
            }
        }
        let elem = xt.elem().cloned().unwrap_or(SqlType::Text);
        if !slice && args.len() == 2 {
            let i = args.pop().unwrap();
            return Ok(Expr::Subscript(Box::new(args.pop().unwrap()), Box::new(i), elem));
        }
        let types: Vec<SqlType> = std::iter::once(xt).chain(std::iter::repeat_n(SqlType::Int4, args.len() - 1)).collect();
        self.make_call(if slice { "array_slice" } else { "array_get" }, args, types)
    }

    /// Arguments of the jsonb_path_* functions: (target, path [, vars
    /// [, silent]]), by position or by name, with PostgreSQL's defaults.
    fn bind_jsonpath_args(
        &mut self,
        fname: &str,
        args: &[ast::FunctionArg],
        bind: &mut dyn FnMut(&mut Self, &ast::Expr) -> Result<Expr>,
    ) -> Result<Vec<Expr>> {
        const NAMES: [&str; 4] = ["target", "path", "vars", "silent"];
        let mut slots: [Option<Expr>; 4] = Default::default();
        let mut named = false;
        for (i, a) in args.iter().enumerate() {
            let (k, e) = match (named_arg(a), a) {
                (None, ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e))) => {
                    if named {
                        sql_bail!(codes::SYNTAX_ERROR, "positional argument cannot follow named argument");
                    }
                    (i, e)
                }
                (Some((n, ast::FunctionArgExpr::Expr(e))), _) => {
                    named = true;
                    let k = NAMES.iter().position(|x| *x == n).ok_or_else(|| {
                        SqlError::new(codes::UNDEFINED_FUNCTION, format!("function {fname} has no parameter named \"{n}\""))
                    })?;
                    if slots[k].is_some() {
                        sql_bail!(codes::SYNTAX_ERROR, "argument name \"{n}\" used more than once");
                    }
                    (k, e)
                }
                _ => sql_bail!(codes::SYNTAX_ERROR, "invalid argument for function {fname}"),
            };
            if k >= NAMES.len() {
                sql_bail!(codes::UNDEFINED_FUNCTION, "function {fname} with {} arguments does not exist", args.len());
            }
            slots[k] = Some(bind(self, e)?);
        }
        let [Some(target), Some(path), vars, silent] = slots else {
            sql_bail!(codes::UNDEFINED_FUNCTION, "function {fname} requires the target and path arguments");
        };
        let empty = pgfuncs::json::Jsonb::parse("{}")?;
        Ok(vec![
            target,
            path,
            vars.unwrap_or(Expr::Literal(Datum::jsonb(empty), SqlType::Jsonb)),
            silent.unwrap_or(Expr::Literal(Datum::Bool(false), SqlType::Bool)),
        ])
    }

    /// Table function in FROM. Returns (rel, scope, lateral).
    pub(crate) fn bind_table_function(
        &mut self,
        name: &str,
        args: &[ast::FunctionArg],
        alias: Option<&ast::TableAlias>,
        left_scope: Option<&Scope>,
        ordinality: bool,
    ) -> Result<(Rel, Scope, bool)> {
        // arguments may reference columns to the left (implicitly LATERAL);
        // with nothing on the left, references go to enclosing queries at
        // their usual depth
        let pushed = left_scope.is_some();
        if let Some(ls) = left_scope {
            self.outer.push(ls.clone());
        }
        let empty = Scope::default();
        let mut bound = Vec::new();
        let mut err = None;
        if JSONPATH_FUNCS.contains(&name) {
            match self.bind_jsonpath_args(name, args, &mut |b, e| b.bind_expr(e, &Ctx::plain(&empty, "FROM"))) {
                Ok(b) => bound = b,
                Err(e) => err = Some(e),
            }
        }
        for a in args.iter().filter(|_| !JSONPATH_FUNCS.contains(&name)) {
            match a {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => match self.bind_expr(e, &Ctx::plain(&empty, "FROM")) {
                    Ok(b) => bound.push(b),
                    Err(e) => {
                        err = Some(e);
                        break;
                    }
                },
                _ => {
                    err = Some(SqlError::unsupported("unsupported table function argument"));
                    break;
                }
            }
        }
        if pushed {
            self.outer.pop();
        }
        if let Some(e) = err {
            return Err(e);
        }
        let lateral = pushed && bound.iter().any(|b| b.has_outer());
        let types: Vec<SqlType> = bound.iter().map(|b| b.ty(&[])).collect();
        // unnest(a, b, ...): one unnest per array, zipped (as ROWS FROM)
        let calls: Vec<(TableFunc, Vec<Expr>, Vec<ColumnType>, Vec<String>)> = if name == "unnest" && bound.len() > 1 {
            bound.into_iter().zip(types).map(|(b, t)| self.resolve_table_function(name, vec![b], vec![t])).collect::<Result<_>>()?
        } else {
            vec![self.resolve_table_function(name, bound, types)?]
        };
        let mut out: Vec<ColumnType> = calls.iter().flat_map(|c| c.2.iter().cloned()).collect();
        let mut names: Vec<String> = calls.iter().flat_map(|c| c.3.iter().cloned()).collect();
        if ordinality {
            out.push(ColumnType::new(SqlType::Int8).not_null());
            names.push("ordinality".into());
        }
        let tname = match alias {
            Some(a) => {
                let cols: Vec<String> = a.columns.iter().map(|c| ident(&c.name)).collect();
                // a single-column function aliased without column list names the column after the alias
                if cols.is_empty() && names.len() == 1 + ordinality as usize {
                    names[0] = ident(&a.name);
                }
                super::binder::apply_alias_names(&mut names, &cols, &ident(&a.name))?;
                ident(&a.name)
            }
            None => name.to_string(),
        };
        for c in out.iter_mut() {
            c.typmod = -1;
        }
        let funcs = calls.into_iter().map(|(func, args, out, _)| crate::sql::plan::SrfCall { func, args, arity: out.len() }).collect();
        let rel = Rel::FlatMap { input: Box::new(Rel::unit()), funcs, out: out.clone(), ordinality };
        Ok((rel, Scope::from_table(&tname, &names, &out), lateral))
    }
}

/// Whether `e` references columns of the current query (`local`) or of
/// enclosing queries (`outer`).
fn var_levels(e: &Expr, local: &mut bool, outer: &mut bool) {
    match e {
        Expr::Column(_) => *local = true,
        Expr::Outer(..) => *outer = true,
        _ => {
            for c in e.children() {
                var_levels(c, local, outer);
            }
        }
    }
}

fn numeric_arg(bound: &mut [Expr], types: &mut [SqlType], name: &str) -> Result<SqlType> {
    let t = types[0].clone();
    if t == SqlType::Unknown {
        bound[0] = Expr::Cast(Box::new(bound[0].clone()), SqlType::Numeric, -1, false);
        types[0] = SqlType::Numeric;
        return Ok(SqlType::Numeric);
    }
    if !t.is_numeric_family() {
        sql_bail!(codes::UNDEFINED_FUNCTION, "function {name}({t}) does not exist");
    }
    Ok(t)
}

/// Integer literal -> int4/int8/numeric; decimal -> numeric.
pub fn number_literal(s: &str) -> Result<Expr> {
    let t = s.replace('_', "");
    if !t.contains(['.', 'e', 'E']) && !t.starts_with("0x") && !t.starts_with("-0x") {
        if let Ok(v) = t.parse::<i64>() {
            if let Ok(v32) = i32::try_from(v) {
                return Ok(Expr::Literal(Datum::Int4(v32), SqlType::Int4));
            }
            return Ok(Expr::Literal(Datum::Int8(v), SqlType::Int8));
        }
    }
    if t.starts_with("0x") || t.starts_with("0o") || t.starts_with("0b") || t.starts_with("-0x") {
        let v = crate::types::text::parse_int(&t, "bigint")?;
        return Ok(match i32::try_from(v) {
            Ok(v) => Expr::Literal(Datum::Int4(v), SqlType::Int4),
            Err(_) => Expr::Literal(Datum::Int8(v), SqlType::Int8),
        });
    }
    Ok(Expr::Literal(Datum::numeric(pgnumeric::Numeric::parse(&t)?), SqlType::Numeric))
}

fn fold_cast(e: Expr) -> Expr {
    match &e {
        Expr::Cast(inner, ..) if inner.is_literal() => e.clone().fold().unwrap_or(e),
        _ => e,
    }
}

pub fn datetime_field_name(f: &ast::DateTimeField) -> String {
    use ast::DateTimeField as F;
    match f {
        F::Year | F::Years => "year".into(),
        F::Month | F::Months => "month".into(),
        F::Week(_) | F::Weeks => "week".into(),
        F::Day | F::Days => "day".into(),
        F::DayOfWeek | F::Dow => "dow".into(),
        F::DayOfYear | F::Doy => "doy".into(),
        F::Hour | F::Hours => "hour".into(),
        F::Minute | F::Minutes => "minute".into(),
        F::Second | F::Seconds => "second".into(),
        F::Century => "century".into(),
        F::Decade => "decade".into(),
        F::Epoch => "epoch".into(),
        F::Isodow => "isodow".into(),
        F::Isoyear => "isoyear".into(),
        F::IsoWeek => "week".into(),
        F::Julian => "julian".into(),
        F::Microsecond | F::Microseconds => "microseconds".into(),
        F::Millenium | F::Millennium => "millennium".into(),
        F::Millisecond | F::Milliseconds => "milliseconds".into(),
        F::Nanosecond | F::Nanoseconds => "nanoseconds".into(),
        F::Quarter => "quarter".into(),
        F::Timezone => "timezone".into(),
        F::TimezoneHour => "timezone_hour".into(),
        F::TimezoneMinute => "timezone_minute".into(),
        F::TimezoneAbbr => "timezone_abbr".into(),
        F::Custom(i) => i.value.to_lowercase(),
        other => other.to_string().to_lowercase(),
    }
}

pub(crate) fn _func_name(id: crate::expr::FuncId) -> &'static str {
    func_def(id).name
}
