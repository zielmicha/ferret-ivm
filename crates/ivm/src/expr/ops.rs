//! Operator implementations.

use super::func::{Builder, Ret, P};
use crate::error::{codes, Result, SqlError};
use crate::types::text::{format_datum, SESSION_TZ};
use crate::types::{ArrayValue, Datum, SqlType};
use pgfuncs::json::Jsonb;
use pgnumeric::Numeric;
use std::cmp::Ordering;
use std::sync::Arc;
use SqlType as T;

pub fn range_err(tyname: &str) -> SqlError {
    SqlError::new(codes::NUMERIC_VALUE_OUT_OF_RANGE, format!("{tyname} out of range"))
}

pub fn div_zero() -> SqlError {
    SqlError::new(codes::DIVISION_BY_ZERO, "division by zero")
}

fn internal(op: &str, a: &Datum, b: &Datum) -> SqlError {
    SqlError::internal(format!("bad operands for {op}: {a:?}, {b:?}"))
}

// ---------------------------------------------------------------- integers

fn int_binop(a: &Datum, b: &Datum, name: &str, op: fn(i64, i64) -> Option<i64>) -> Result<Datum> {
    match (a, b) {
        (Datum::Int2(x), Datum::Int2(y)) => op(*x as i64, *y as i64)
            .and_then(|v| i16::try_from(v).ok())
            .map(Datum::Int2)
            .ok_or_else(|| range_err("smallint")),
        (Datum::Int4(x), Datum::Int4(y)) => op(*x as i64, *y as i64)
            .and_then(|v| i32::try_from(v).ok())
            .map(Datum::Int4)
            .ok_or_else(|| range_err("integer")),
        (Datum::Int8(x), Datum::Int8(y)) => op(*x, *y).map(Datum::Int8).ok_or_else(|| range_err("bigint")),
        _ => Err(internal(name, a, b)),
    }
}

fn int_add(a: &Datum, b: &Datum) -> Result<Datum> {
    int_binop(a, b, "+", |x, y| x.checked_add(y))
}
fn int_sub(a: &Datum, b: &Datum) -> Result<Datum> {
    int_binop(a, b, "-", |x, y| x.checked_sub(y))
}
fn int_mul(a: &Datum, b: &Datum) -> Result<Datum> {
    int_binop(a, b, "*", |x, y| x.checked_mul(y))
}
fn int_div(a: &Datum, b: &Datum) -> Result<Datum> {
    if b.as_i64() == Some(0) {
        return Err(div_zero());
    }
    int_binop(a, b, "/", |x, y| x.checked_div(y))
}
fn int_mod(a: &Datum, b: &Datum) -> Result<Datum> {
    if b.as_i64() == Some(0) {
        return Err(div_zero());
    }
    int_binop(a, b, "%", |x, y| if y == -1 { Some(0) } else { x.checked_rem(y) })
}
fn int_and(a: &Datum, b: &Datum) -> Result<Datum> {
    int_binop(a, b, "&", |x, y| Some(x & y))
}
fn int_or(a: &Datum, b: &Datum) -> Result<Datum> {
    int_binop(a, b, "|", |x, y| Some(x | y))
}
fn int_xor(a: &Datum, b: &Datum) -> Result<Datum> {
    int_binop(a, b, "#", |x, y| Some(x ^ y))
}

fn int_shift(a: &Datum, b: &Datum, left: bool) -> Result<Datum> {
    let s = b.as_i64().unwrap_or(0) as u32;
    Ok(match a {
        Datum::Int2(x) => Datum::Int2(if left { x.wrapping_shl(s) } else { x.wrapping_shr(s) }),
        Datum::Int4(x) => Datum::Int4(if left { x.wrapping_shl(s) } else { x.wrapping_shr(s) }),
        Datum::Int8(x) => Datum::Int8(if left { x.wrapping_shl(s) } else { x.wrapping_shr(s) }),
        _ => return Err(internal("<<", a, b)),
    })
}
fn int_shl(a: &Datum, b: &Datum) -> Result<Datum> {
    int_shift(a, b, true)
}
fn int_shr(a: &Datum, b: &Datum) -> Result<Datum> {
    int_shift(a, b, false)
}

fn int_neg(a: &Datum) -> Result<Datum> {
    Ok(match a {
        Datum::Int2(x) => Datum::Int2(x.checked_neg().ok_or_else(|| range_err("smallint"))?),
        Datum::Int4(x) => Datum::Int4(x.checked_neg().ok_or_else(|| range_err("integer"))?),
        Datum::Int8(x) => Datum::Int8(x.checked_neg().ok_or_else(|| range_err("bigint"))?),
        _ => return Err(SqlError::internal("int_neg")),
    })
}
fn int_abs(a: &Datum) -> Result<Datum> {
    Ok(match a {
        Datum::Int2(x) => Datum::Int2(x.checked_abs().ok_or_else(|| range_err("smallint"))?),
        Datum::Int4(x) => Datum::Int4(x.checked_abs().ok_or_else(|| range_err("integer"))?),
        Datum::Int8(x) => Datum::Int8(x.checked_abs().ok_or_else(|| range_err("bigint"))?),
        _ => return Err(SqlError::internal("int_abs")),
    })
}
fn int_not(a: &Datum) -> Result<Datum> {
    Ok(match a {
        Datum::Int2(x) => Datum::Int2(!x),
        Datum::Int4(x) => Datum::Int4(!x),
        Datum::Int8(x) => Datum::Int8(!x),
        _ => return Err(SqlError::internal("int_not")),
    })
}
fn identity(a: &Datum) -> Result<Datum> {
    Ok(a.clone())
}

// ---------------------------------------------------------------- floats

pub fn overflow() -> SqlError {
    SqlError::new(codes::NUMERIC_VALUE_OUT_OF_RANGE, "value out of range: overflow")
}
pub fn underflow() -> SqlError {
    SqlError::new(codes::NUMERIC_VALUE_OUT_OF_RANGE, "value out of range: underflow")
}

pub fn f8_check(r: f64, a: f64, b: f64, zero_ok: bool) -> Result<f64> {
    if r.is_infinite() && !a.is_infinite() && !b.is_infinite() {
        return Err(overflow());
    }
    if !zero_ok && r == 0.0 && a != 0.0 && b != 0.0 {
        return Err(underflow());
    }
    Ok(r)
}

fn float_binop(a: &Datum, b: &Datum, op: char) -> Result<Datum> {
    match (a, b) {
        (Datum::Float8(x), Datum::Float8(y)) => {
            let (x, y) = (*x, *y);
            Ok(Datum::Float8(match op {
                '+' => f8_check(x + y, x, y, true)?,
                '-' => f8_check(x - y, x, y, true)?,
                '*' => f8_check(x * y, x, y, false)?,
                '/' => {
                    if y == 0.0 && !x.is_nan() {
                        return Err(div_zero());
                    }
                    let r = x / y;
                    if r.is_infinite() && !x.is_infinite() {
                        return Err(overflow());
                    }
                    if r == 0.0 && x != 0.0 && !y.is_infinite() {
                        return Err(underflow());
                    }
                    r
                }
                _ => unreachable!(),
            }))
        }
        (Datum::Float4(x), Datum::Float4(y)) => {
            let (x, y) = (*x, *y);
            let r = match op {
                '+' => x + y,
                '-' => x - y,
                '*' => x * y,
                '/' => {
                    if y == 0.0 && !x.is_nan() {
                        return Err(div_zero());
                    }
                    x / y
                }
                _ => unreachable!(),
            };
            if r.is_infinite() && !x.is_infinite() && !(y.is_infinite() && op != '/') {
                return Err(overflow());
            }
            if r == 0.0 && x != 0.0 && ((op == '*' && y != 0.0) || (op == '/' && !y.is_infinite())) {
                return Err(underflow());
            }
            Ok(Datum::Float4(r))
        }
        _ => Err(internal("float op", a, b)),
    }
}
fn float_add(a: &Datum, b: &Datum) -> Result<Datum> {
    float_binop(a, b, '+')
}
fn float_sub(a: &Datum, b: &Datum) -> Result<Datum> {
    float_binop(a, b, '-')
}
fn float_mul(a: &Datum, b: &Datum) -> Result<Datum> {
    float_binop(a, b, '*')
}
fn float_div(a: &Datum, b: &Datum) -> Result<Datum> {
    float_binop(a, b, '/')
}
/// float4 op float8 / float8 op float4 (float48pl etc.): computed in double
/// precision.
fn float48_binop(a: &Datum, b: &Datum, op: char) -> Result<Datum> {
    match (a.as_f64(), b.as_f64()) {
        (Some(x), Some(y)) => float_binop(&Datum::Float8(x), &Datum::Float8(y), op),
        _ => Err(internal("float op", a, b)),
    }
}
fn float48_add(a: &Datum, b: &Datum) -> Result<Datum> {
    float48_binop(a, b, '+')
}
fn float48_sub(a: &Datum, b: &Datum) -> Result<Datum> {
    float48_binop(a, b, '-')
}
fn float48_mul(a: &Datum, b: &Datum) -> Result<Datum> {
    float48_binop(a, b, '*')
}
fn float48_div(a: &Datum, b: &Datum) -> Result<Datum> {
    float48_binop(a, b, '/')
}
fn float_neg(a: &Datum) -> Result<Datum> {
    Ok(match a {
        Datum::Float4(x) => Datum::Float4(-x),
        Datum::Float8(x) => Datum::Float8(-x),
        _ => return Err(SqlError::internal("float_neg")),
    })
}
fn float_abs(a: &Datum) -> Result<Datum> {
    Ok(match a {
        Datum::Float4(x) => Datum::Float4(x.abs()),
        Datum::Float8(x) => Datum::Float8(x.abs()),
        _ => return Err(SqlError::internal("float_abs")),
    })
}

/// PostgreSQL dpow semantics.
pub fn float8_pow(a: &Datum, b: &Datum) -> Result<Datum> {
    let (x, y) = (a.as_f64().unwrap(), b.as_f64().unwrap());
    if x == 0.0 && y < 0.0 {
        return Err(SqlError::new(
            "2201F",
            "zero raised to a negative power is undefined",
        ));
    }
    if x < 0.0 && y.fract() != 0.0 && y.is_finite() {
        return Err(SqlError::new(
            "2201F",
            "a negative number raised to a non-integer power yields a complex result",
        ));
    }
    // PG special cases
    if x.is_nan() {
        return Ok(Datum::Float8(if y == 0.0 { 1.0 } else { f64::NAN }));
    }
    if y.is_nan() {
        return Ok(Datum::Float8(if x == 1.0 { 1.0 } else { f64::NAN }));
    }
    let r = x.powf(y);
    if r.is_infinite() && x.is_finite() && y.is_finite() {
        return Err(overflow());
    }
    if r == 0.0 && x != 0.0 && x.is_finite() && y.is_finite() {
        return Err(underflow());
    }
    Ok(Datum::Float8(r))
}

// ---------------------------------------------------------------- numeric

fn num(a: &Datum) -> &Numeric {
    a.as_numeric().expect("numeric operand")
}
fn num_add(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(num(a).add(num(b))?))
}
fn num_sub(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(num(a).sub(num(b))?))
}
fn num_mul(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(num(a).mul(num(b))?))
}
fn num_div(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(num(a).div(num(b))?))
}
fn num_mod(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(num(a).rem(num(b))?))
}
fn num_pow(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(num(a).pow(num(b))?))
}
fn num_neg(a: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(num(a).neg()))
}
fn num_abs(a: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(num(a).abs()))
}

// ---------------------------------------------------------------- comparison

fn bpchar_trim(s: &str) -> &str {
    s.trim_end_matches(' ')
}

fn cmp_datums(a: &Datum, b: &Datum) -> Ordering {
    a.sql_cmp(b)
}

macro_rules! cmp_fns {
    ($eq:ident, $ne:ident, $lt:ident, $le:ident, $gt:ident, $ge:ident, $cmp:expr) => {
        fn $eq(a: &Datum, b: &Datum) -> Result<Datum> {
            Ok(Datum::Bool($cmp(a, b) == Ordering::Equal))
        }
        fn $ne(a: &Datum, b: &Datum) -> Result<Datum> {
            Ok(Datum::Bool($cmp(a, b) != Ordering::Equal))
        }
        fn $lt(a: &Datum, b: &Datum) -> Result<Datum> {
            Ok(Datum::Bool($cmp(a, b) == Ordering::Less))
        }
        fn $le(a: &Datum, b: &Datum) -> Result<Datum> {
            Ok(Datum::Bool($cmp(a, b) != Ordering::Greater))
        }
        fn $gt(a: &Datum, b: &Datum) -> Result<Datum> {
            Ok(Datum::Bool($cmp(a, b) == Ordering::Greater))
        }
        fn $ge(a: &Datum, b: &Datum) -> Result<Datum> {
            Ok(Datum::Bool($cmp(a, b) != Ordering::Less))
        }
    };
}

cmp_fns!(eq, ne, lt, le, gt, ge, cmp_datums);
fn bpchar_cmp(a: &Datum, b: &Datum) -> Ordering {
    bpchar_trim(a.as_str().unwrap()).cmp(bpchar_trim(b.as_str().unwrap()))
}
cmp_fns!(bp_eq, bp_ne, bp_lt, bp_le, bp_gt, bp_ge, bpchar_cmp);

// ---------------------------------------------------------------- strings

fn textcat(a: &Datum, b: &Datum) -> Result<Datum> {
    let (x, y) = (cast_text(a), cast_text(b));
    let mut s = String::with_capacity(x.len() + y.len());
    s.push_str(&x);
    s.push_str(&y);
    Ok(Datum::text_owned(s))
}

/// `d::text`, which `||` applies to non-text operands: booleans read
/// true/false (their output function writes t/f).
fn cast_text(d: &Datum) -> std::borrow::Cow<'_, str> {
    match d {
        Datum::Bool(b) => std::borrow::Cow::Borrowed(if *b { "true" } else { "false" }),
        other => as_text(other),
    }
}

/// Text form of any datum (for `||` with anynonarray and concat()).
pub fn as_text(d: &Datum) -> std::borrow::Cow<'_, str> {
    match d {
        Datum::Text(s) | Datum::Json(s) => std::borrow::Cow::Borrowed(s),
        other => std::borrow::Cow::Owned(format_datum(other)),
    }
}

fn byteacat(a: &Datum, b: &Datum) -> Result<Datum> {
    let mut v = a.as_bytes().unwrap().to_vec();
    v.extend_from_slice(b.as_bytes().unwrap());
    Ok(Datum::bytea(v))
}

thread_local! {
    static REGEX_CACHE: std::cell::RefCell<rustc_hash::FxHashMap<(String, String), Arc<pgfuncs::pattern::Regex>>> =
        std::cell::RefCell::new(Default::default());
}

/// Compiled regex with a per-thread cache.
pub fn regex(pattern: &str, flags: &str) -> Result<Arc<pgfuncs::pattern::Regex>> {
    REGEX_CACHE.with(|c| {
        let key = (pattern.to_string(), flags.to_string());
        if let Some(r) = c.borrow().get(&key) {
            return Ok(r.clone());
        }
        let r = Arc::new(pgfuncs::pattern::Regex::new(pattern, flags)?);
        let mut m = c.borrow_mut();
        if m.len() > 1000 {
            m.clear();
        }
        m.insert(key, r.clone());
        Ok(r)
    })
}

fn re_match(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::Bool(regex(b.as_str().unwrap(), "")?.is_match(a.as_str().unwrap())))
}
fn re_imatch(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::Bool(regex(b.as_str().unwrap(), "i")?.is_match(a.as_str().unwrap())))
}
fn re_nmatch(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::Bool(!regex(b.as_str().unwrap(), "")?.is_match(a.as_str().unwrap())))
}
fn re_nimatch(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::Bool(!regex(b.as_str().unwrap(), "i")?.is_match(a.as_str().unwrap())))
}

// ---------------------------------------------------------------- date/time

fn dt_err() -> SqlError {
    SqlError::internal("bad datetime operands")
}

fn date_pl_int(a: &Datum, b: &Datum) -> Result<Datum> {
    match (a, b) {
        (Datum::Date(d), Datum::Int4(n)) | (Datum::Int4(n), Datum::Date(d)) => {
            Ok(Datum::Date(pgdatetime::date_pl_days(*d, *n)?))
        }
        _ => Err(dt_err()),
    }
}
fn date_mi_int(a: &Datum, b: &Datum) -> Result<Datum> {
    match (a, b) {
        (Datum::Date(d), Datum::Int4(n)) => Ok(Datum::Date(pgdatetime::date_mi_days(*d, *n)?)),
        _ => Err(dt_err()),
    }
}
fn date_mi_date(a: &Datum, b: &Datum) -> Result<Datum> {
    match (a, b) {
        (Datum::Date(x), Datum::Date(y)) => Ok(Datum::Int4(pgdatetime::date_mi_date(*x, *y)?)),
        _ => Err(dt_err()),
    }
}
fn date_pl_interval(a: &Datum, b: &Datum) -> Result<Datum> {
    match (a, b) {
        (Datum::Date(d), Datum::Interval(i)) | (Datum::Interval(i), Datum::Date(d)) => {
            Ok(Datum::Timestamp(pgdatetime::date_pl_interval(*d, *i)?))
        }
        _ => Err(dt_err()),
    }
}
fn date_mi_interval(a: &Datum, b: &Datum) -> Result<Datum> {
    match (a, b) {
        (Datum::Date(d), Datum::Interval(i)) => Ok(Datum::Timestamp(pgdatetime::date_mi_interval(*d, *i)?)),
        _ => Err(dt_err()),
    }
}
fn date_pl_time(a: &Datum, b: &Datum) -> Result<Datum> {
    match (a, b) {
        (Datum::Date(d), Datum::Time(t)) | (Datum::Time(t), Datum::Date(d)) => {
            Ok(Datum::Timestamp(pgdatetime::date_pl_time(*d, *t)?))
        }
        _ => Err(dt_err()),
    }
}
fn ts_pl_interval(a: &Datum, b: &Datum) -> Result<Datum> {
    match (a, b) {
        (Datum::Timestamp(t), Datum::Interval(i)) | (Datum::Interval(i), Datum::Timestamp(t)) => {
            Ok(Datum::Timestamp(pgdatetime::timestamp_pl_interval(*t, *i)?))
        }
        (Datum::TimestampTz(t), Datum::Interval(i)) | (Datum::Interval(i), Datum::TimestampTz(t)) => {
            Ok(Datum::TimestampTz(pgdatetime::timestamptz_pl_interval(*t, *i, &SESSION_TZ)?))
        }
        _ => Err(dt_err()),
    }
}
fn ts_mi_interval(a: &Datum, b: &Datum) -> Result<Datum> {
    match (a, b) {
        (Datum::Timestamp(t), Datum::Interval(i)) => Ok(Datum::Timestamp(pgdatetime::timestamp_mi_interval(*t, *i)?)),
        (Datum::TimestampTz(t), Datum::Interval(i)) => {
            Ok(Datum::TimestampTz(pgdatetime::timestamptz_mi_interval(*t, *i, &SESSION_TZ)?))
        }
        _ => Err(dt_err()),
    }
}
fn ts_mi_ts(a: &Datum, b: &Datum) -> Result<Datum> {
    match (a, b) {
        (Datum::Timestamp(x), Datum::Timestamp(y)) => Ok(Datum::Interval(pgdatetime::timestamp_mi(*x, *y)?)),
        (Datum::TimestampTz(x), Datum::TimestampTz(y)) => Ok(Datum::Interval(pgdatetime::timestamptz_mi(*x, *y)?)),
        _ => Err(dt_err()),
    }
}
fn time_pl_interval(a: &Datum, b: &Datum) -> Result<Datum> {
    match (a, b) {
        (Datum::Time(t), Datum::Interval(i)) | (Datum::Interval(i), Datum::Time(t)) => {
            Ok(Datum::Time(pgdatetime::time_pl_interval_checked(*t, *i)?))
        }
        _ => Err(dt_err()),
    }
}
fn time_mi_interval(a: &Datum, b: &Datum) -> Result<Datum> {
    match (a, b) {
        (Datum::Time(t), Datum::Interval(i)) => Ok(Datum::Time(pgdatetime::time_mi_interval_checked(*t, *i)?)),
        _ => Err(dt_err()),
    }
}
fn time_mi_time(a: &Datum, b: &Datum) -> Result<Datum> {
    match (a, b) {
        (Datum::Time(x), Datum::Time(y)) => Ok(Datum::Interval(pgdatetime::time_mi_time(*x, *y))),
        _ => Err(dt_err()),
    }
}
fn iv_pl(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::Interval(pgdatetime::interval_pl(a.as_interval().unwrap(), b.as_interval().unwrap())?))
}
fn iv_mi(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::Interval(pgdatetime::interval_mi(a.as_interval().unwrap(), b.as_interval().unwrap())?))
}
fn iv_um(a: &Datum) -> Result<Datum> {
    Ok(Datum::Interval(pgdatetime::interval_um(a.as_interval().unwrap())?))
}
fn iv_mul(a: &Datum, b: &Datum) -> Result<Datum> {
    match (a, b) {
        (Datum::Interval(i), Datum::Float8(f)) | (Datum::Float8(f), Datum::Interval(i)) => {
            Ok(Datum::Interval(pgdatetime::interval_mul(*i, *f)?))
        }
        _ => Err(dt_err()),
    }
}
fn iv_div(a: &Datum, b: &Datum) -> Result<Datum> {
    match (a, b) {
        (Datum::Interval(i), Datum::Float8(f)) => {
            if *f == 0.0 {
                return Err(div_zero());
            }
            Ok(Datum::Interval(pgdatetime::interval_div(*i, *f)?))
        }
        _ => Err(dt_err()),
    }
}

// ---------------------------------------------------------------- jsonb

pub fn jsonb(d: &Datum) -> &Jsonb {
    match d {
        Datum::Jsonb(j) => j,
        _ => panic!("jsonb operand expected"),
    }
}

pub fn text_array(d: &Datum) -> Vec<Option<String>> {
    match d {
        Datum::Array(a) => a.elems.iter().map(|e| e.as_str().map(|s| s.to_string())).collect(),
        _ => vec![],
    }
}

fn opt_jsonb(j: Option<&Jsonb>) -> Datum {
    match j {
        Some(j) => Datum::jsonb(j.clone()),
        None => Datum::Null,
    }
}
fn opt_text(s: Option<String>) -> Datum {
    match s {
        Some(s) => Datum::text_owned(s),
        None => Datum::Null,
    }
}

fn jsonb_get(a: &Datum, b: &Datum) -> Result<Datum> {
    let j = jsonb(a);
    Ok(match b {
        Datum::Text(k) => opt_jsonb(j.get_key(k)),
        Datum::Int4(i) => opt_jsonb(j.get_index(*i as i64)),
        _ => Datum::Null,
    })
}
fn jsonb_get_text(a: &Datum, b: &Datum) -> Result<Datum> {
    let j = jsonb(a);
    let v = match b {
        Datum::Text(k) => j.get_key(k),
        Datum::Int4(i) => j.get_index(*i as i64),
        _ => None,
    };
    Ok(opt_text(v.and_then(|v| v.as_text())))
}
fn path_strs(d: &Datum) -> Option<Vec<String>> {
    let v = text_array(d);
    if v.iter().any(|e| e.is_none()) {
        return None;
    }
    Some(v.into_iter().map(|e| e.unwrap()).collect())
}
fn jsonb_path(a: &Datum, b: &Datum) -> Result<Datum> {
    let Some(p) = path_strs(b) else { return Ok(Datum::Null) };
    let refs: Vec<&str> = p.iter().map(|s| s.as_str()).collect();
    Ok(opt_jsonb(jsonb(a).get_path(&refs)))
}
fn jsonb_path_text(a: &Datum, b: &Datum) -> Result<Datum> {
    let Some(p) = path_strs(b) else { return Ok(Datum::Null) };
    let refs: Vec<&str> = p.iter().map(|s| s.as_str()).collect();
    Ok(opt_text(jsonb(a).get_path(&refs).and_then(|v| v.as_text())))
}
fn jsonb_contains(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::Bool(jsonb(a).contains(jsonb(b))))
}
fn jsonb_contained(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::Bool(jsonb(b).contains(jsonb(a))))
}
fn jsonb_exists(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::Bool(jsonb(a).exists(b.as_str().unwrap())))
}
fn jsonb_exists_any(a: &Datum, b: &Datum) -> Result<Datum> {
    let keys: Vec<String> = text_array(b).into_iter().flatten().collect();
    let refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
    Ok(Datum::Bool(jsonb(a).exists_any(&refs)))
}
fn jsonb_exists_all(a: &Datum, b: &Datum) -> Result<Datum> {
    let keys: Vec<String> = text_array(b).into_iter().flatten().collect();
    let refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
    Ok(Datum::Bool(jsonb(a).exists_all(&refs)))
}
fn jsonb_concat(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::jsonb(jsonb(a).concat(jsonb(b))))
}
fn jsonb_delete(a: &Datum, b: &Datum) -> Result<Datum> {
    let j = jsonb(a);
    Ok(Datum::jsonb(match b {
        Datum::Text(k) => j.delete_key(k)?,
        Datum::Int4(i) => j.delete_index(*i as i64)?,
        Datum::Array(_) => {
            let mut cur = j.clone();
            for k in text_array(b).into_iter().flatten() {
                cur = cur.delete_key(&k)?;
            }
            cur
        }
        _ => j.clone(),
    }))
}
fn jsonb_delete_path(a: &Datum, b: &Datum) -> Result<Datum> {
    let Some(p) = path_strs(b) else { return Ok(Datum::Null) };
    let refs: Vec<&str> = p.iter().map(|s| s.as_str()).collect();
    Ok(Datum::jsonb(jsonb(a).delete_path(&refs)?))
}
fn json_get(a: &Datum, b: &Datum) -> Result<Datum> {
    let s = a.as_str().unwrap();
    let r = match b {
        Datum::Text(k) => pgfuncs::json::json_text_get_key(s, k)?,
        Datum::Int4(i) => pgfuncs::json::json_text_get_index(s, *i as i64)?,
        _ => None,
    };
    Ok(match r {
        Some(v) => Datum::Json(Arc::from(v.as_str())),
        None => Datum::Null,
    })
}
fn json_get_text(a: &Datum, b: &Datum) -> Result<Datum> {
    let s = a.as_str().unwrap();
    let r = match b {
        Datum::Text(k) => pgfuncs::json::json_text_get_key(s, k)?,
        Datum::Int4(i) => pgfuncs::json::json_text_get_index(s, *i as i64)?,
        _ => None,
    };
    Ok(match r {
        Some(v) => opt_text(pgfuncs::json::json_text_as_text(&v)?),
        None => Datum::Null,
    })
}

// ---------------------------------------------------------------- arrays

fn array_of(d: &Datum) -> &ArrayValue {
    match d {
        Datum::Array(a) => a,
        _ => panic!("array operand expected"),
    }
}

fn array_contains(a: &Datum, b: &Datum) -> Result<Datum> {
    let (x, y) = (array_of(a), array_of(b));
    Ok(Datum::Bool(y.elems.iter().all(|e| !e.is_null() && x.elems.iter().any(|f| !f.is_null() && f.sql_eq(e)))))
}
fn array_contained(a: &Datum, b: &Datum) -> Result<Datum> {
    array_contains(b, a)
}
fn array_overlap(a: &Datum, b: &Datum) -> Result<Datum> {
    let (x, y) = (array_of(a), array_of(b));
    Ok(Datum::Bool(y.elems.iter().any(|e| !e.is_null() && x.elems.iter().any(|f| !f.is_null() && f.sql_eq(e)))))
}

// ---------------------------------------------------------------- registration

pub fn register(b: &mut Builder) {
    let ints = [T::Int2, T::Int4, T::Int8];
    for t in &ints {
        b.op2("+", t.clone(), t.clone(), t.clone(), int_add);
        b.op2("-", t.clone(), t.clone(), t.clone(), int_sub);
        b.op2("*", t.clone(), t.clone(), t.clone(), int_mul);
        b.op2("/", t.clone(), t.clone(), t.clone(), int_div);
        b.op2("%", t.clone(), t.clone(), t.clone(), int_mod);
        b.op2("&", t.clone(), t.clone(), t.clone(), int_and);
        b.op2("|", t.clone(), t.clone(), t.clone(), int_or);
        b.op2("#", t.clone(), t.clone(), t.clone(), int_xor);
        b.op2("<<", t.clone(), T::Int4, t.clone(), int_shl);
        b.op2(">>", t.clone(), T::Int4, t.clone(), int_shr);
        b.op1("-", t.clone(), t.clone(), int_neg);
        b.op1("+", t.clone(), t.clone(), identity);
        b.op1("@", t.clone(), t.clone(), int_abs);
        b.op1("~", t.clone(), t.clone(), int_not);
    }
    for t in [T::Float4, T::Float8] {
        b.op2("+", t.clone(), t.clone(), t.clone(), float_add);
        b.op2("-", t.clone(), t.clone(), t.clone(), float_sub);
        b.op2("*", t.clone(), t.clone(), t.clone(), float_mul);
        b.op2("/", t.clone(), t.clone(), t.clone(), float_div);
        b.op1("-", t.clone(), t.clone(), float_neg);
        b.op1("+", t.clone(), t.clone(), identity);
        b.op1("@", t.clone(), t.clone(), float_abs);
    }
    // mixed float4/float8 arithmetic is float8; these operators also make
    // float4 op integer/numeric resolve to float8, as in PostgreSQL (no
    // cross-type comparisons: "=" operands must share a type)
    for (x, y) in [(T::Float4, T::Float8), (T::Float8, T::Float4)] {
        b.op2("+", x.clone(), y.clone(), T::Float8, float48_add);
        b.op2("-", x.clone(), y.clone(), T::Float8, float48_sub);
        b.op2("*", x.clone(), y.clone(), T::Float8, float48_mul);
        b.op2("/", x, y, T::Float8, float48_div);
    }
    b.op2("^", T::Float8, T::Float8, T::Float8, float8_pow);
    b.op2("+", T::Numeric, T::Numeric, T::Numeric, num_add);
    b.op2("-", T::Numeric, T::Numeric, T::Numeric, num_sub);
    b.op2("*", T::Numeric, T::Numeric, T::Numeric, num_mul);
    b.op2("/", T::Numeric, T::Numeric, T::Numeric, num_div);
    b.op2("%", T::Numeric, T::Numeric, T::Numeric, num_mod);
    b.op2("^", T::Numeric, T::Numeric, T::Numeric, num_pow);
    b.op1("-", T::Numeric, T::Numeric, num_neg);
    b.op1("+", T::Numeric, T::Numeric, identity);
    b.op1("@", T::Numeric, T::Numeric, num_abs);

    // comparisons
    let comparable = [
        T::Bool,
        T::Int2,
        T::Int4,
        T::Int8,
        T::Float4,
        T::Float8,
        T::Numeric,
        T::Text,
        T::Bytea,
        T::Date,
        T::Time,
        T::Timestamp,
        T::TimestampTz,
        T::Interval,
        T::Uuid,
        T::Jsonb,
        T::Oid,
    ];
    for t in &comparable {
        b.op2("=", t.clone(), t.clone(), T::Bool, eq);
        b.op2("<>", t.clone(), t.clone(), T::Bool, ne);
        b.op2("<", t.clone(), t.clone(), T::Bool, lt);
        b.op2("<=", t.clone(), t.clone(), T::Bool, le);
        b.op2(">", t.clone(), t.clone(), T::Bool, gt);
        b.op2(">=", t.clone(), t.clone(), T::Bool, ge);
    }
    b.op2("=", T::Bpchar, T::Bpchar, T::Bool, bp_eq);
    b.op2("<>", T::Bpchar, T::Bpchar, T::Bool, bp_ne);
    b.op2("<", T::Bpchar, T::Bpchar, T::Bool, bp_lt);
    b.op2("<=", T::Bpchar, T::Bpchar, T::Bool, bp_le);
    b.op2(">", T::Bpchar, T::Bpchar, T::Bool, bp_gt);
    b.op2(">=", T::Bpchar, T::Bpchar, T::Bool, bp_ge);
    b.op2p("=", P::AnyArray, P::AnyArray, Ret::T(T::Bool), eq);
    b.op2p("<>", P::AnyArray, P::AnyArray, Ret::T(T::Bool), ne);
    b.op2p("<", P::AnyArray, P::AnyArray, Ret::T(T::Bool), lt);
    b.op2p("<=", P::AnyArray, P::AnyArray, Ret::T(T::Bool), le);
    b.op2p(">", P::AnyArray, P::AnyArray, Ret::T(T::Bool), gt);
    b.op2p(">=", P::AnyArray, P::AnyArray, Ret::T(T::Bool), ge);

    // strings
    b.op2("||", T::Text, T::Text, T::Text, textcat);
    b.op2p("||", P::T(T::Text), P::AnyNonArray, Ret::T(T::Text), textcat);
    b.op2p("||", P::AnyNonArray, P::T(T::Text), Ret::T(T::Text), textcat);
    b.op2("||", T::Bytea, T::Bytea, T::Bytea, byteacat);
    // character(n) matches with its padding (bpcharregexeq etc.)
    for t in [T::Text, T::Bpchar] {
        b.op2("~", t.clone(), T::Text, T::Bool, re_match);
        b.op2("~*", t.clone(), T::Text, T::Bool, re_imatch);
        b.op2("!~", t.clone(), T::Text, T::Bool, re_nmatch);
        b.op2("!~*", t, T::Text, T::Bool, re_nimatch);
    }

    // date/time
    b.op2("+", T::Date, T::Int4, T::Date, date_pl_int);
    b.op2("+", T::Int4, T::Date, T::Date, date_pl_int);
    b.op2("-", T::Date, T::Int4, T::Date, date_mi_int);
    b.op2("-", T::Date, T::Date, T::Int4, date_mi_date);
    b.op2("+", T::Date, T::Interval, T::Timestamp, date_pl_interval);
    b.op2("+", T::Interval, T::Date, T::Timestamp, date_pl_interval);
    b.op2("-", T::Date, T::Interval, T::Timestamp, date_mi_interval);
    b.op2("+", T::Date, T::Time, T::Timestamp, date_pl_time);
    b.op2("+", T::Time, T::Date, T::Timestamp, date_pl_time);
    b.op2("+", T::Timestamp, T::Interval, T::Timestamp, ts_pl_interval);
    b.op2("+", T::Interval, T::Timestamp, T::Timestamp, ts_pl_interval);
    b.op2("-", T::Timestamp, T::Interval, T::Timestamp, ts_mi_interval);
    b.op2("-", T::Timestamp, T::Timestamp, T::Interval, ts_mi_ts);
    b.op2("+", T::TimestampTz, T::Interval, T::TimestampTz, ts_pl_interval);
    b.op2("+", T::Interval, T::TimestampTz, T::TimestampTz, ts_pl_interval);
    b.op2("-", T::TimestampTz, T::Interval, T::TimestampTz, ts_mi_interval);
    b.op2("-", T::TimestampTz, T::TimestampTz, T::Interval, ts_mi_ts);
    b.op2("+", T::Time, T::Interval, T::Time, time_pl_interval);
    b.op2("+", T::Interval, T::Time, T::Time, time_pl_interval);
    b.op2("-", T::Time, T::Interval, T::Time, time_mi_interval);
    b.op2("-", T::Time, T::Time, T::Interval, time_mi_time);
    b.op2("+", T::Interval, T::Interval, T::Interval, iv_pl);
    b.op2("-", T::Interval, T::Interval, T::Interval, iv_mi);
    b.op1("-", T::Interval, T::Interval, iv_um);
    b.op2("*", T::Interval, T::Float8, T::Interval, iv_mul);
    b.op2("*", T::Float8, T::Interval, T::Interval, iv_mul);
    b.op2("/", T::Interval, T::Float8, T::Interval, iv_div);

    // jsonb / json
    let text_arr = T::Array(Box::new(T::Text));
    b.op2("->", T::Jsonb, T::Text, T::Jsonb, jsonb_get);
    b.op2("->", T::Jsonb, T::Int4, T::Jsonb, jsonb_get);
    b.op2("->>", T::Jsonb, T::Text, T::Text, jsonb_get_text);
    b.op2("->>", T::Jsonb, T::Int4, T::Text, jsonb_get_text);
    b.op2("#>", T::Jsonb, text_arr.clone(), T::Jsonb, jsonb_path);
    b.op2("#>>", T::Jsonb, text_arr.clone(), T::Text, jsonb_path_text);
    b.op2("@>", T::Jsonb, T::Jsonb, T::Bool, jsonb_contains);
    b.op2("<@", T::Jsonb, T::Jsonb, T::Bool, jsonb_contained);
    b.op2("?", T::Jsonb, T::Text, T::Bool, jsonb_exists);
    b.op2("?|", T::Jsonb, text_arr.clone(), T::Bool, jsonb_exists_any);
    b.op2("?&", T::Jsonb, text_arr.clone(), T::Bool, jsonb_exists_all);
    b.op2("||", T::Jsonb, T::Jsonb, T::Jsonb, jsonb_concat);
    b.op2("-", T::Jsonb, T::Text, T::Jsonb, jsonb_delete);
    b.op2("-", T::Jsonb, T::Int4, T::Jsonb, jsonb_delete);
    b.op2("-", T::Jsonb, text_arr.clone(), T::Jsonb, jsonb_delete);
    b.op2("#-", T::Jsonb, text_arr.clone(), T::Jsonb, jsonb_delete_path);
    b.op2("->", T::Json, T::Text, T::Json, json_get);
    b.op2("->", T::Json, T::Int4, T::Json, json_get);
    b.op2("->>", T::Json, T::Text, T::Text, json_get_text);
    b.op2("->>", T::Json, T::Int4, T::Text, json_get_text);

    // arrays
    // not strict, like array_cat / array_append / array_prepend
    b.op2p("||", P::AnyArray, P::AnyArray, Ret::ArrayOfElem, super::builtins::array_cat2);
    b.last().strict = false;
    b.last().can_return_null = true;
    b.op2p("||", P::AnyArray, P::AnyElement, Ret::ArrayOfElem, super::builtins::array_append2);
    b.last().strict = false;
    b.op2p("||", P::AnyElement, P::AnyArray, Ret::ArrayOfElem, super::builtins::array_prepend2);
    b.last().strict = false;
    b.op2p("@>", P::AnyArray, P::AnyArray, Ret::T(T::Bool), array_contains);
    b.op2p("<@", P::AnyArray, P::AnyArray, Ret::T(T::Bool), array_contained);
    b.op2p("&&", P::AnyArray, P::AnyArray, Ret::T(T::Bool), array_overlap);
}
