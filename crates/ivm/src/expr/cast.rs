//! Casts between SQL types (pg_cast equivalents).

use crate::error::{codes, Result, SqlError};
use crate::types::text::{apply_typmod, format_datum, parse_datum, SESSION_TZ};
use crate::types::{ArrayValue, Datum, SqlType};
use pgnumeric::Numeric;
use std::sync::Arc;
use super::ops::{overflow, range_err, underflow};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum CastContext {
    Implicit,
    Assignment,
    Explicit,
}

/// Whether `from` coerces to `to` implicitly (used by overload resolution).
pub fn implicit_castable(from: &SqlType, to: &SqlType) -> bool {
    cast_context(from, to) == Some(CastContext::Implicit)
}

/// The weakest context in which a cast from `from` to `to` is allowed.
pub fn cast_context(from: &SqlType, to: &SqlType) -> Option<CastContext> {
    use CastContext::*;
    use SqlType::*;
    if from == to {
        return Some(Implicit);
    }
    if *from == Unknown {
        return Some(Implicit);
    }
    let ctx = match (from, to) {
        (Int2, Int4 | Int8 | Numeric | Float4 | Float8 | Oid) => Implicit,
        (Int4, Int8 | Numeric | Float4 | Float8 | Oid) => Implicit,
        (Int8, Numeric | Float4 | Float8 | Oid) => Implicit,
        (Numeric, Float4 | Float8) => Implicit,
        (Float4, Float8) => Implicit,
        (Int4, Int2) | (Int8, Int2 | Int4) => Assignment,
        (Numeric, Int2 | Int4 | Int8) => Assignment,
        (Float4 | Float8, Int2 | Int4 | Int8 | Numeric) => Assignment,
        (Float8, Float4) => Assignment,
        (Oid, Int4 | Int8) => Assignment,
        (Int4, Bool) | (Bool, Int4) => Explicit,
        (Varchar | Bpchar | Name, Text) => Implicit,
        (Text | Bpchar, Varchar) => Implicit,
        (Text | Varchar, Bpchar) => Implicit,
        (Text | Varchar | Bpchar, Name) => Implicit,
        (Date, Timestamp | TimestampTz) => Implicit,
        (Timestamp, TimestampTz) => Implicit,
        (Timestamp | TimestampTz, Date) => Assignment,
        (TimestampTz, Timestamp) => Assignment,
        (Timestamp | TimestampTz, Time) => Assignment,
        (Time, Interval) => Implicit,
        (Interval, Time) => Assignment,
        (Json, Jsonb) | (Jsonb, Json) => Assignment,
        (Jsonb, Bool | Numeric | Int2 | Int4 | Int8 | Float4 | Float8) => Explicit,
        // everything converts to string types via its output function
        (_, Text | Varchar | Bpchar | Name) => Assignment,
        // and from string types via its input function
        (Text | Varchar | Bpchar | Name, _) => Explicit,
        (Array(a), Array(b)) => return cast_context(a, b),
        _ => return None,
    };
    Some(ctx)
}

/// Integer to integer type with range check.
pub fn int_to(v: i64, to: &SqlType) -> Result<Datum> {
    Ok(match to {
        SqlType::Int2 => Datum::Int2(i16::try_from(v).map_err(|_| range_err("smallint"))?),
        SqlType::Int4 => Datum::Int4(i32::try_from(v).map_err(|_| range_err("integer"))?),
        SqlType::Int8 => Datum::Int8(v),
        SqlType::Oid => Datum::Oid(if (i32::MIN as i64..=u32::MAX as i64).contains(&v) {
            v as u32
        } else {
            return Err(range_err("OID"));
        }),
        _ => return Err(SqlError::internal("int_to non-integer")),
    })
}

/// float -> integer: rint() then range check (PostgreSQL ftoi semantics).
pub fn float_to_int(f: f64, to: &SqlType) -> Result<Datum> {
    let r = f.round_ties_even();
    let (lo, hi, name) = match to {
        SqlType::Int2 => (i16::MIN as f64, -(i16::MIN as f64), "smallint"),
        SqlType::Int4 => (i32::MIN as f64, -(i32::MIN as f64), "integer"),
        _ => (i64::MIN as f64, -(i64::MIN as f64), "bigint"),
    };
    if r.is_nan() || r < lo || r >= hi {
        return Err(range_err(name));
    }
    int_to(r as i64, to)
}

fn float4_from_f64(v: f64) -> Result<f32> {
    let r = v as f32;
    if r.is_infinite() && !v.is_infinite() {
        return Err(overflow());
    }
    if r == 0.0 && v != 0.0 {
        return Err(underflow());
    }
    Ok(r)
}

/// Cast a datum. `typmod` is applied after conversion.
pub fn cast_datum(d: &Datum, to: &SqlType, typmod: i32, explicit: bool) -> Result<Datum> {
    if d.is_null() {
        return Ok(Datum::Null);
    }
    let v = convert(d, to)?;
    apply_typmod(v, to, typmod, explicit)
}

fn convert(d: &Datum, to: &SqlType) -> Result<Datum> {
    use SqlType as T;
    Ok(match (d, to) {
        // integers
        (Datum::Int2(_) | Datum::Int4(_) | Datum::Int8(_) | Datum::Oid(_), T::Int2 | T::Int4 | T::Int8 | T::Oid) => {
            int_to(d.as_i64().unwrap(), to)?
        }
        (Datum::Int2(_) | Datum::Int4(_) | Datum::Int8(_) | Datum::Oid(_), T::Float4) => {
            Datum::Float4(d.as_i64().unwrap() as f32)
        }
        (Datum::Int2(_) | Datum::Int4(_) | Datum::Int8(_) | Datum::Oid(_), T::Float8) => {
            Datum::Float8(d.as_i64().unwrap() as f64)
        }
        (Datum::Int2(_) | Datum::Int4(_) | Datum::Int8(_) | Datum::Oid(_), T::Numeric) => {
            Datum::numeric(Numeric::from_i64(d.as_i64().unwrap()))
        }
        (Datum::Int4(v), T::Bool) => Datum::Bool(*v != 0),
        (Datum::Bool(b), T::Int4) => Datum::Int4(*b as i32),
        // floats
        (Datum::Float4(f), T::Float8) => Datum::Float8(*f as f64),
        (Datum::Float8(f), T::Float4) => Datum::Float4(float4_from_f64(*f)?),
        (Datum::Float4(f), T::Int2 | T::Int4 | T::Int8) => float_to_int(*f as f64, to)?,
        (Datum::Float8(f), T::Int2 | T::Int4 | T::Int8) => float_to_int(*f, to)?,
        (Datum::Float4(f), T::Numeric) => Datum::numeric(Numeric::from_f32(*f)?),
        (Datum::Float8(f), T::Numeric) => Datum::numeric(Numeric::from_f64(*f)?),
        // numeric
        (Datum::Numeric(n), T::Int2) => Datum::Int2(n.to_i16()?),
        (Datum::Numeric(n), T::Int4) => Datum::Int4(n.to_i32()?),
        (Datum::Numeric(n), T::Int8) => Datum::Int8(n.to_i64()?),
        (Datum::Numeric(n), T::Float4) => Datum::Float4(n.to_f32()?),
        (Datum::Numeric(n), T::Float8) => Datum::Float8(n.to_f64()?),
        (Datum::Numeric(n), T::Numeric) => Datum::Numeric(n.clone()),
        // date/time
        (Datum::Date(v), T::Timestamp) => Datum::Timestamp(pgdatetime::date_to_timestamp(*v)?),
        (Datum::Date(v), T::TimestampTz) => Datum::TimestampTz(pgdatetime::date_to_timestamptz(*v, &SESSION_TZ)?),
        (Datum::Timestamp(v), T::Date) => Datum::Date(pgdatetime::timestamp_to_date(*v)?),
        (Datum::Timestamp(v), T::TimestampTz) => {
            Datum::TimestampTz(pgdatetime::timestamp_to_timestamptz(*v, &SESSION_TZ)?)
        }
        (Datum::Timestamp(v), T::Time) => pgdatetime::timestamp_to_time_opt(*v)?.map(Datum::Time).unwrap_or(Datum::Null),
        (Datum::TimestampTz(v), T::Date) => Datum::Date(pgdatetime::timestamptz_to_date(*v, &SESSION_TZ)?),
        (Datum::TimestampTz(v), T::Timestamp) => {
            Datum::Timestamp(pgdatetime::timestamptz_to_timestamp(*v, &SESSION_TZ)?)
        }
        (Datum::TimestampTz(v), T::Time) => {
            pgdatetime::timestamptz_to_time_opt(*v, &SESSION_TZ)?.map(Datum::Time).unwrap_or(Datum::Null)
        }
        (Datum::Time(v), T::Interval) => Datum::Interval(pgdatetime::time_to_interval(*v)),
        (Datum::Interval(iv), T::Time) => Datum::Time(pgdatetime::interval_to_time_checked(*iv)?),
        // json
        (Datum::Json(s), T::Jsonb) => Datum::jsonb(pgfuncs::json::Jsonb::parse(s)?),
        (Datum::Jsonb(j), T::Json) => Datum::Json(Arc::from(j.to_string().as_str())),
        (Datum::Jsonb(j), T::Bool | T::Numeric | T::Int2 | T::Int4 | T::Int8 | T::Float4 | T::Float8) => {
            jsonb_scalar_cast(j, to)?
        }
        // strings
        (Datum::Text(s), T::Text | T::Varchar | T::Bpchar | T::Name) => Datum::Text(s.clone()),
        // booltext(), unlike the output function, spells the words out
        (Datum::Bool(b), T::Text | T::Varchar | T::Bpchar) => Datum::text(if *b { "true" } else { "false" }),
        (_, T::Text | T::Varchar | T::Bpchar | T::Name) => Datum::text_owned(format_datum(d)),
        (Datum::Text(s), _) => parse_datum(s, to, -1)?,
        // arrays
        (Datum::Array(a), T::Array(elem)) => {
            let mut elems = Vec::with_capacity(a.elems.len());
            for e in &a.elems {
                elems.push(cast_datum(e, elem, -1, true)?);
            }
            Datum::array(ArrayValue { elem_type: (**elem).clone(), dims: a.dims.clone(), elems })
        }
        // identity
        (d, t) if same_kind(d, t) => d.clone(),
        (d, t) => {
            return Err(SqlError::new(
                codes::DATATYPE_MISMATCH,
                format!("cannot cast value {} to type {}", format_datum(d), t),
            ));
        }
    })
}

fn same_kind(d: &Datum, t: &SqlType) -> bool {
    matches!(
        (d, t),
        (Datum::Bool(_), SqlType::Bool)
            | (Datum::Bytea(_), SqlType::Bytea)
            | (Datum::Date(_), SqlType::Date)
            | (Datum::Time(_), SqlType::Time)
            | (Datum::Timestamp(_), SqlType::Timestamp)
            | (Datum::TimestampTz(_), SqlType::TimestampTz)
            | (Datum::Interval(_), SqlType::Interval)
            | (Datum::Uuid(_), SqlType::Uuid)
            | (Datum::Jsonb(_), SqlType::Jsonb)
            | (Datum::Json(_), SqlType::Json)
            | (Datum::Float4(_), SqlType::Float4)
            | (Datum::Float8(_), SqlType::Float8)
    )
}

fn jsonb_scalar_cast(j: &pgfuncs::json::Jsonb, to: &SqlType) -> Result<Datum> {
    use pgfuncs::json::Jsonb;
    let tyname = to.name();
    let err = |kind: &str| {
        SqlError::new(codes::INVALID_PARAMETER_VALUE, format!("cannot cast jsonb {kind} to type {tyname}"))
    };
    match (j, to) {
        (Jsonb::Bool(b), SqlType::Bool) => Ok(Datum::Bool(*b)),
        (Jsonb::Number(n), _) if *to != SqlType::Bool => {
            let num = n.as_numeric().clone();
            convert(&Datum::numeric(num), to)
        }
        (Jsonb::Null, _) => Ok(Datum::Null),
        (Jsonb::String(_), _) => Err(err("string")),
        (Jsonb::Number(_), _) => Err(err("numeric")),
        (Jsonb::Bool(_), _) => Err(err("boolean")),
        (Jsonb::Array(_), _) => Err(err("array")),
        (Jsonb::Object(_), _) => Err(err("object")),
    }
}
