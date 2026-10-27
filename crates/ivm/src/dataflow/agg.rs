//! Aggregate functions: summary requirements and PostgreSQL-compatible
//! finalization, shared by the incremental Reduce and the batch executor.

use crate::error::{codes, Result, SqlError};
use crate::expr::Expr;
use crate::sql::plan::AggFunc;
use crate::storage::{AggKind, AggState, FloatSum, Summary};
use crate::types::{ArrayValue, ColumnType, Datum, Row, SqlType};
use pgfuncs::json::Jsonb;
use pgnumeric::Numeric;

/// One aggregate of a Reduce, over input columns.
#[derive(Clone, Debug)]
pub struct AggSpec {
    pub func: AggFunc,
    pub arg: Option<usize>,
    /// Second argument (string_agg delimiter, object-agg value).
    pub arg2: Option<usize>,
    pub filter: Option<usize>,
    pub arg_type: SqlType,
    pub ret: ColumnType,
    pub fraction: Option<f64>,
}

impl AggSpec {
    /// Argument expression with the FILTER folded in (NULL when filtered out).
    fn arg_expr(&self) -> Expr {
        let a = Expr::Column(self.arg.expect("aggregate argument"));
        match self.filter {
            None => a,
            Some(f) => Expr::Case(Box::new(crate::expr::CaseExpr {
                whens: vec![(Expr::Column(f), a)],
                else_: Expr::null(self.arg_type.clone()),
                ty: self.arg_type.clone(),
            })),
        }
    }

    /// Summary aggregates this function needs (appended to the input
    /// arrangement's summaries).
    pub fn summaries(&self) -> Vec<(AggKind, Expr)> {
        use AggFunc as A;
        let sum_kind = |sq: bool| match (&self.arg_type, sq) {
            (t, false) if t.is_integer() => AggKind::SumInt,
            (t, true) if t.is_integer() => AggKind::SumSqInt,
            (SqlType::Numeric, false) => AggKind::SumNumeric,
            (SqlType::Numeric, true) => AggKind::SumSqNumeric,
            (_, false) => AggKind::SumFloat,
            (_, true) => AggKind::SumSqFloat,
        };
        match self.func {
            A::CountStar => match self.filter {
                Some(f) => vec![(AggKind::CountTrue, Expr::Column(f))],
                None => vec![],
            },
            A::Count => vec![(AggKind::CountNonNull, self.arg_expr())],
            A::Sum | A::Avg => vec![(sum_kind(false), self.arg_expr()), (AggKind::CountNonNull, self.arg_expr())],
            A::Min | A::AnyValue => vec![(AggKind::Min, self.arg_expr())],
            A::Max => vec![(AggKind::Max, self.arg_expr())],
            A::BoolAnd => vec![(AggKind::CountFalse, self.arg_expr()), (AggKind::CountNonNull, self.arg_expr())],
            A::BoolOr => vec![(AggKind::CountTrue, self.arg_expr()), (AggKind::CountNonNull, self.arg_expr())],
            // squares of int8 values (up to 2^126) overflow i128 sums: those
            // are summed as numerics (exactly, like PostgreSQL)
            A::StddevSamp | A::StddevPop | A::VarSamp | A::VarPop if self.arg_type == SqlType::Int8 => {
                let num = Expr::Cast(Box::new(self.arg_expr()), SqlType::Numeric, -1, false);
                vec![(AggKind::SumNumeric, num.clone()), (AggKind::SumSqNumeric, num), (AggKind::CountNonNull, self.arg_expr())]
            }
            A::StddevSamp | A::StddevPop | A::VarSamp | A::VarPop => vec![
                (sum_kind(false), self.arg_expr()),
                (sum_kind(true), self.arg_expr()),
                (AggKind::CountNonNull, self.arg_expr()),
            ],
            A::SingleValue => vec![(AggKind::Min, self.arg_expr())],
            A::PercentileCont | A::PercentileDisc => vec![(AggKind::CountNonNull, self.arg_expr())],
            A::Mode => vec![],
            A::StringAgg | A::ArrayAgg | A::JsonAgg | A::JsonbAgg | A::JsonObjectAgg | A::JsonbObjectAgg => vec![],
        }
    }

    /// Finalize from summary slots (non-ordered aggregates).
    pub fn finalize(&self, s: &Summary, slots: &[usize]) -> Result<Datum> {
        use AggFunc as A;
        let st = |i: usize| &s.aggs[slots[i]];
        let count = |i: usize| match st(i) {
            AggState::Count(c) => *c,
            _ => 0,
        };
        Ok(match self.func {
            A::CountStar => Datum::Int8(if self.filter.is_some() { count(0) } else { s.count }),
            A::Count => Datum::Int8(count(0)),
            A::Sum => {
                if count(1) == 0 {
                    Datum::Null
                } else {
                    finalize_sum(st(0), &self.arg_type)?
                }
            }
            A::Avg => {
                let n = count(1);
                if n == 0 {
                    Datum::Null
                } else {
                    finalize_avg(st(0), n, &self.arg_type)?
                }
            }
            A::Min | A::Max | A::AnyValue => match st(0) {
                AggState::Min(Some(d)) | AggState::Max(Some(d)) => d.clone(),
                _ => Datum::Null,
            },
            A::BoolAnd => {
                if count(1) == 0 {
                    Datum::Null
                } else {
                    Datum::Bool(count(0) == 0)
                }
            }
            A::BoolOr => {
                if count(1) == 0 {
                    Datum::Null
                } else {
                    Datum::Bool(count(0) > 0)
                }
            }
            A::StddevSamp | A::StddevPop | A::VarSamp | A::VarPop => {
                let variance = matches!(self.func, A::VarSamp | A::VarPop);
                let sample = matches!(self.func, A::VarSamp | A::StddevSamp);
                finalize_var(st(0), st(1), count(2), &self.arg_type, variance, sample)?
            }
            A::SingleValue => {
                if s.count > 1 {
                    return Err(SqlError::new(
                        codes::CARDINALITY_VIOLATION,
                        "more than one row returned by a subquery used as an expression",
                    ));
                }
                match st(0) {
                    AggState::Min(Some(d)) => d.clone(),
                    _ => Datum::Null,
                }
            }
            _ => return Err(SqlError::internal(format!("{:?} needs an ordered scan", self.func))),
        })
    }

    /// Finalize an ordered aggregate from the group's rows in order.
    pub fn finalize_ordered(&self, rows: &mut dyn Iterator<Item = (&Row, i64)>) -> Result<Datum> {
        use AggFunc as A;
        let arg = self.arg.unwrap_or(0);
        let passes = |r: &Row| match self.filter {
            Some(f) => matches!(r[f], Datum::Bool(true)),
            None => true,
        };
        match self.func {
            A::StringAgg => {
                let mut out: Option<Vec<u8>> = None;
                let bytea = self.arg_type == SqlType::Bytea;
                for (r, w) in rows {
                    if !passes(r) || r[arg].is_null() {
                        continue;
                    }
                    for _ in 0..w.max(0) {
                        let val: &[u8] = if bytea { r[arg].as_bytes().unwrap() } else { r[arg].as_str().unwrap().as_bytes() };
                        match &mut out {
                            None => out = Some(val.to_vec()),
                            Some(o) => {
                                if let Some(d) = self.arg2.map(|c| &r[c])
                                    && !d.is_null()
                                {
                                    o.extend_from_slice(if bytea { d.as_bytes().unwrap() } else { d.as_str().unwrap().as_bytes() });
                                }
                                o.extend_from_slice(val);
                            }
                        }
                    }
                }
                Ok(match out {
                    None => Datum::Null,
                    Some(b) if bytea => Datum::bytea(b),
                    Some(b) => Datum::text_owned(String::from_utf8(b).unwrap_or_default()),
                })
            }
            A::ArrayAgg => {
                let mut elems = Vec::new();
                let mut any = false;
                for (r, w) in rows {
                    if !passes(r) {
                        continue;
                    }
                    any = true;
                    for _ in 0..w.max(0) {
                        elems.push(r[arg].clone());
                    }
                }
                if !any || elems.is_empty() {
                    return Ok(Datum::Null);
                }
                Ok(Datum::array(match &self.arg_type {
                    // arrays accumulate into one more dimension
                    SqlType::Array(inner) => ArrayValue::accumulate((**inner).clone(), &elems)?,
                    t => ArrayValue::new_1d(t.clone(), elems),
                }))
            }
            A::JsonAgg | A::JsonbAgg => {
                let mut vals = Vec::new();
                for (r, w) in rows {
                    if !passes(r) {
                        continue;
                    }
                    let j = crate::expr::builtins_datum_to_jsonb(&r[arg])?;
                    for _ in 0..w.max(0) {
                        vals.push(j.clone());
                    }
                }
                if vals.is_empty() {
                    return Ok(Datum::Null);
                }
                if self.func == A::JsonbAgg {
                    Ok(Datum::jsonb(Jsonb::Array(vals)))
                } else {
                    let parts: Vec<String> = vals.iter().map(crate::expr::json_text).collect();
                    Ok(Datum::Json(std::sync::Arc::from(format!("[{}]", parts.join(", ")).as_str())))
                }
            }
            A::JsonObjectAgg | A::JsonbObjectAgg => {
                let vcol = self.arg2.unwrap_or(arg + 1);
                let mut pairs: Vec<(String, Jsonb)> = Vec::new();
                for (r, w) in rows {
                    if !passes(r) || w <= 0 {
                        continue;
                    }
                    if r[arg].is_null() {
                        // (PostgreSQL 17's wording and codes differ between the two)
                        return Err(if self.func == A::JsonbObjectAgg {
                            SqlError::new(codes::INVALID_PARAMETER_VALUE, "field name must not be null")
                        } else {
                            SqlError::new(codes::NULL_VALUE_NOT_ALLOWED, "null value not allowed for object key")
                        });
                    }
                    let k: String = crate::expr::ops_as_text(&r[arg]);
                    let v = crate::expr::builtins_datum_to_jsonb(&r[vcol])?;
                    for _ in 0..w {
                        pairs.push((k.clone(), v.clone()));
                    }
                }
                if pairs.is_empty() {
                    return Ok(Datum::Null);
                }
                if self.func == A::JsonbObjectAgg {
                    Ok(Datum::jsonb(Jsonb::build_object(pairs)))
                } else {
                    let body: Vec<String> = pairs
                        .iter()
                        .map(|(k, v)| format!("{} : {}", pgfuncs::json::escape_string(k), crate::expr::json_text(v)))
                        .collect();
                    Ok(Datum::Json(std::sync::Arc::from(format!("{{ {} }}", body.join(", ")).as_str())))
                }
            }
            A::Mode => {
                // rows arrive in ORDER BY order: count runs of equal values;
                // the first of equally frequent values wins
                let mut best: Option<(Datum, i64)> = None;
                let mut run: Option<(Datum, i64)> = None;
                for (r, w) in rows {
                    if !passes(r) || r[arg].is_null() || w <= 0 {
                        continue;
                    }
                    match &mut run {
                        Some((v, n)) if v.sql_eq(&r[arg]) => *n += w,
                        _ => {
                            if let Some((v, n)) = run.take()
                                && best.as_ref().is_none_or(|(_, bn)| n > *bn)
                            {
                                best = Some((v, n));
                            }
                            run = Some((r[arg].clone(), w));
                        }
                    }
                }
                if let Some((v, n)) = run
                    && best.as_ref().is_none_or(|(_, bn)| n > *bn)
                {
                    best = Some((v, n));
                }
                Ok(best.map(|(v, _)| v).unwrap_or(Datum::Null))
            }
            A::PercentileCont | A::PercentileDisc => {
                let mut vals: Vec<Datum> = Vec::new();
                for (r, w) in rows {
                    if !passes(r) || r[arg].is_null() {
                        continue;
                    }
                    for _ in 0..w.max(0) {
                        vals.push(r[arg].clone());
                    }
                }
                percentile(self.func, self.fraction.unwrap_or(0.5), vals.len() as i64, &mut |k| Ok(vals[k as usize].clone()))
            }
            _ => Err(SqlError::internal("not an ordered aggregate")),
        }
    }
}

/// Percentile over n sorted non-null values accessed by index.
pub fn percentile(func: AggFunc, f: f64, n: i64, get: &mut dyn FnMut(i64) -> Result<Datum>) -> Result<Datum> {
    if n == 0 {
        return Ok(Datum::Null);
    }
    if func == AggFunc::PercentileDisc {
        // first value whose cumulative position >= f * n (1-based)
        let mut row = (f * n as f64).ceil() as i64;
        if row < 1 {
            row = 1;
        }
        return get(row.min(n) - 1);
    }
    let pos = f * (n - 1) as f64;
    let lo = pos.floor() as i64;
    let hi = pos.ceil() as i64;
    let a = get(lo)?.as_f64().unwrap_or(0.0);
    if lo == hi {
        return Ok(Datum::Float8(a));
    }
    let b = get(hi)?.as_f64().unwrap_or(0.0);
    Ok(Datum::Float8(a + (b - a) * (pos - lo as f64)))
}

fn finalize_sum(st: &AggState, arg: &SqlType) -> Result<Datum> {
    Ok(match (st, arg) {
        (AggState::SumInt(v), SqlType::Int2 | SqlType::Int4) => {
            Datum::Int8(i64::try_from(*v).map_err(|_| SqlError::new(codes::NUMERIC_VALUE_OUT_OF_RANGE, "bigint out of range"))?)
        }
        (AggState::SumInt(v), _) => Datum::numeric(Numeric::from_i128(*v)),
        // (the exact sum may be beyond numeric's range)
        (AggState::SumNumeric(n), _) => Datum::numeric(n.value().checked()?),
        (AggState::SumFloat(f), SqlType::Float4) => {
            // as with float4pl, a finite sum beyond real's range overflows
            let v = float_sum_value(f)?;
            let r = v as f32;
            if r.is_infinite() && v.is_finite() {
                return Err(crate::expr::ops_overflow());
            }
            Datum::Float4(r)
        }
        (AggState::SumFloat(f), _) => Datum::Float8(float_sum_value(f)?),
        _ => return Err(SqlError::internal("sum state mismatch")),
    })
}

fn float_sum_value(f: &FloatSum) -> Result<f64> {
    let v = f.value();
    if v.is_infinite() && f.pos_inf == 0 && f.neg_inf == 0 {
        return Err(crate::expr::ops_overflow());
    }
    Ok(v)
}

fn finalize_avg(st: &AggState, n: i64, arg: &SqlType) -> Result<Datum> {
    Ok(match (st, arg) {
        (AggState::SumInt(v), _) => Datum::numeric(Numeric::from_i128(*v).div(&Numeric::from_i64(n))?),
        (AggState::SumNumeric(s), _) => Datum::numeric(s.value().div(&Numeric::from_i64(n))?),
        (AggState::SumFloat(f), _) => Datum::Float8(f.div_value(n)),
        _ => Datum::Null,
    })
}

fn finalize_var(sum: &AggState, sumsq: &AggState, n: i64, arg: &SqlType, variance: bool, sample: bool) -> Result<Datum> {
    if n == 0 || (sample && n == 1) {
        return Ok(Datum::Null);
    }
    match (sum, sumsq) {
        (AggState::SumInt(s), AggState::SumInt(q)) => {
            let r = Numeric::stddev_from_sums(n, &Numeric::from_i128(*s), &Numeric::from_i128(*q), variance, sample)?;
            Ok(r.map(Datum::numeric).unwrap_or(Datum::Null))
        }
        (AggState::SumNumeric(s), AggState::SumNumeric(q)) => {
            let r = Numeric::stddev_from_sums(n, &s.value(), &q.value(), variance, sample)?;
            Ok(r.map(Datum::numeric).unwrap_or(Datum::Null))
        }
        (AggState::SumFloat(s), AggState::SumFloat(q)) => {
            let _ = arg;
            if s.nan > 0 || s.pos_inf > 0 || s.neg_inf > 0 {
                return Ok(Datum::Float8(f64::NAN));
            }
            let v = if variance { s.variance(q, n, sample) } else { s.std_dev(q, n, sample) };
            // (finite values: as float8 arithmetic, an infinite result overflows)
            if v.is_infinite() {
                return Err(crate::expr::ops_overflow());
            }
            Ok(Datum::Float8(v))
        }
        _ => Ok(Datum::Null),
    }
}
