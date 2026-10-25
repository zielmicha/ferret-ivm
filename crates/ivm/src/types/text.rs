//! Text input/output functions (PostgreSQL typinput/typoutput), with the
//! session settings fixed to DateStyle=ISO, IntervalStyle=postgres,
//! TimeZone=UTC, extra_float_digits=1, bytea_output=hex.

use super::datum::{ArrayValue, Datum};
use super::{typmod, SqlType};
use crate::error::{codes, Result, SqlError};
use pgdatetime::TimeZone;
use std::sync::Arc;

pub const SESSION_TZ: TimeZone = TimeZone::Utc;

/// typoutput: render a non-null datum as PostgreSQL would.
pub fn format_datum(d: &Datum) -> String {
    match d {
        Datum::Null => String::new(),
        Datum::Bool(b) => if *b { "t" } else { "f" }.to_string(),
        Datum::Int2(v) => v.to_string(),
        Datum::Int4(v) => v.to_string(),
        Datum::Int8(v) => v.to_string(),
        Datum::Oid(v) => v.to_string(),
        Datum::Float4(v) => pgnumeric::float::format_f32(*v),
        Datum::Float8(v) => pgnumeric::float::format_f64(*v),
        Datum::Numeric(n) => n.to_string(),
        Datum::Text(s) | Datum::Json(s) => s.to_string(),
        Datum::Bytea(b) => pgfuncs::bytea::format(b),
        Datum::Date(v) => pgdatetime::format_date(*v),
        Datum::Time(v) => pgdatetime::format_time(*v),
        Datum::Timestamp(v) => pgdatetime::format_timestamp(*v),
        Datum::TimestampTz(v) => pgdatetime::format_timestamptz(*v, &SESSION_TZ),
        Datum::Interval(iv) => pgdatetime::format_interval(*iv),
        Datum::Uuid(u) => pgfuncs::uuid::format(u),
        Datum::Jsonb(j) => j.to_string(),
        Datum::Array(a) => format_array(a),
    }
}

fn format_array(a: &ArrayValue) -> String {
    let strs: Vec<Option<String>> =
        a.elems.iter().map(|e| if e.is_null() { None } else { Some(format_datum(e)) }).collect();
    let refs: Vec<Option<&str>> = strs.iter().map(|s| s.as_deref()).collect();
    pgfuncs::array::format(&a.dims, &refs, ',')
}

/// typinput: parse text into a datum of the given type, applying the typmod.
pub fn parse_datum(s: &str, ty: &SqlType, typmod: i32) -> Result<Datum> {
    let d = match ty {
        SqlType::Bool => Datum::Bool(parse_bool(s)?),
        SqlType::Int2 => Datum::Int2(parse_int(s, "smallint")? as i16),
        SqlType::Int4 => Datum::Int4(parse_int(s, "integer")? as i32),
        SqlType::Int8 => Datum::Int8(parse_int(s, "bigint")?),
        SqlType::Oid => {
            let v = parse_int(s, "oid")?;
            if v < i32::MIN as i64 || v > u32::MAX as i64 {
                return Err(SqlError::new(
                    codes::NUMERIC_VALUE_OUT_OF_RANGE,
                    format!("value \"{s}\" is out of range for type oid"),
                ));
            }
            Datum::Oid(v as u32)
        }
        SqlType::Float4 => Datum::Float4(pgnumeric::float::parse_f32(s)?),
        SqlType::Float8 => Datum::Float8(pgnumeric::float::parse_f64(s)?),
        SqlType::Numeric => Datum::numeric(pgnumeric::Numeric::parse(s)?),
        SqlType::Text | SqlType::Name => Datum::text(s),
        SqlType::Varchar | SqlType::Bpchar => Datum::text(s),
        SqlType::Json => {
            pgfuncs::json::validate_json(s)?;
            Datum::Json(Arc::from(s))
        }
        SqlType::Jsonb => Datum::jsonb(pgfuncs::json::Jsonb::parse(s)?),
        SqlType::JsonPath => Datum::text_owned(pgfuncs::json::JsonPath::parse(s)?.to_string_checked()?),
        SqlType::Bytea => Datum::bytea(pgfuncs::bytea::parse(s)?),
        SqlType::Date => Datum::Date(pgdatetime::parse_date(s)?),
        SqlType::Time => Datum::Time(pgdatetime::parse_time(s)?),
        SqlType::Timestamp => Datum::Timestamp(pgdatetime::parse_timestamp(s)?),
        SqlType::TimestampTz => Datum::TimestampTz(pgdatetime::parse_timestamptz(s, &SESSION_TZ)?),
        SqlType::Interval => Datum::Interval(pgdatetime::parse_interval(s)?),
        SqlType::Uuid => Datum::Uuid(pgfuncs::uuid::parse(s)?),
        SqlType::Array(elem) => {
            let parsed = pgfuncs::array::parse(s, ',')?;
            let mut elems = Vec::with_capacity(parsed.elements.len());
            for e in parsed.elements {
                elems.push(match e {
                    None => Datum::Null,
                    Some(t) => parse_datum(&t, elem, -1)?,
                });
            }
            Datum::array(ArrayValue { elem_type: (**elem).clone(), dims: parsed.dims, elems })
        }
        SqlType::Unknown => Datum::text(s),
    };
    apply_typmod(d, ty, typmod, false)
}

pub fn parse_bool(s: &str) -> Result<bool> {
    let t = s.trim().to_ascii_lowercase();
    let ok = |v| Ok(v);
    match t.as_str() {
        "t" | "tr" | "tru" | "true" | "y" | "ye" | "yes" | "on" | "1" => ok(true),
        "f" | "fa" | "fal" | "fals" | "false" | "n" | "no" | "of" | "off" | "0" => ok(false),
        _ => Err(SqlError::new(
            codes::INVALID_TEXT_REPRESENTATION,
            format!("invalid input syntax for type boolean: \"{s}\""),
        )),
    }
}

/// PostgreSQL integer input (int2in/int4in/int8in): whitespace, sign,
/// decimal/hex/octal/binary, underscores between digits.
pub fn parse_int(s: &str, tyname: &str) -> Result<i64> {
    let bad = || {
        SqlError::new(codes::INVALID_TEXT_REPRESENTATION, format!("invalid input syntax for type {tyname}: \"{s}\""))
    };
    let range = || {
        SqlError::new(codes::NUMERIC_VALUE_OUT_OF_RANGE, format!("value \"{s}\" is out of range for type {tyname}"))
    };
    let t = s.trim_matches(|c: char| c.is_ascii_whitespace());
    let (neg, rest) = match t.as_bytes().first() {
        Some(b'-') => (true, &t[1..]),
        Some(b'+') => (false, &t[1..]),
        _ => (false, t),
    };
    let (radix, digits) = if rest.len() > 2 && (rest.starts_with("0x") || rest.starts_with("0X")) {
        (16, &rest[2..])
    } else if rest.len() > 2 && (rest.starts_with("0o") || rest.starts_with("0O")) {
        (8, &rest[2..])
    } else if rest.len() > 2 && (rest.starts_with("0b") || rest.starts_with("0B")) {
        (2, &rest[2..])
    } else {
        (10, rest)
    };
    if digits.is_empty() || digits.starts_with('_') || digits.ends_with('_') || digits.contains("__") {
        return Err(bad());
    }
    let mut acc: i128 = 0;
    for c in digits.chars() {
        if c == '_' {
            continue;
        }
        let v = c.to_digit(radix).ok_or_else(bad)? as i128;
        acc = acc * radix as i128 + v;
        if acc > (1i128 << 64) {
            return Err(range());
        }
    }
    let v = if neg { -acc } else { acc };
    let (lo, hi) = match tyname {
        "smallint" => (i16::MIN as i128, i16::MAX as i128),
        "integer" => (i32::MIN as i128, i32::MAX as i128),
        "oid" => (i32::MIN as i128, u32::MAX as i128),
        _ => (i64::MIN as i128, i64::MAX as i128),
    };
    if v < lo || v > hi {
        return Err(range());
    }
    Ok(v as i64)
}

/// Apply a type modifier (length/precision coercion). `explicit` marks an
/// explicit cast, where varchar/char silently truncate instead of erroring.
pub fn apply_typmod(d: Datum, ty: &SqlType, tm: i32, explicit: bool) -> Result<Datum> {
    if tm < 0 || d.is_null() {
        return Ok(d);
    }
    Ok(match (ty, d) {
        (SqlType::Numeric, Datum::Numeric(n)) => match typmod::numeric_parts(tm) {
            Some((p, s)) => Datum::numeric(n.apply_typmod(p, s)?),
            None => Datum::Numeric(n),
        },
        (SqlType::Varchar, Datum::Text(s)) => {
            let max = typmod::varchar_len(tm).unwrap_or(i32::MAX) as usize;
            let nchars = s.chars().count();
            if nchars <= max {
                Datum::Text(s)
            } else {
                let cut: String = s.chars().take(max).collect();
                if !explicit && s.chars().skip(max).any(|c| c != ' ') {
                    return Err(SqlError::new(
                        codes::STRING_DATA_RIGHT_TRUNCATION,
                        format!("value too long for type character varying({max})"),
                    ));
                }
                Datum::text_owned(cut)
            }
        }
        (SqlType::Bpchar, Datum::Text(s)) => {
            let len = typmod::varchar_len(tm).unwrap_or(1) as usize;
            let nchars = s.chars().count();
            if nchars == len {
                Datum::Text(s)
            } else if nchars < len {
                let mut t = s.to_string();
                t.extend(std::iter::repeat_n(' ', len - nchars));
                Datum::text_owned(t)
            } else {
                if !explicit && s.chars().skip(len).any(|c| c != ' ') {
                    return Err(SqlError::new(
                        codes::STRING_DATA_RIGHT_TRUNCATION,
                        format!("value too long for type character({len})"),
                    ));
                }
                Datum::text_owned(s.chars().take(len).collect())
            }
        }
        (SqlType::Timestamp, Datum::Timestamp(v)) => Datum::Timestamp(pgdatetime::adjust_timestamp(v, tm)?),
        (SqlType::TimestampTz, Datum::TimestampTz(v)) => Datum::TimestampTz(pgdatetime::adjust_timestamp(v, tm)?),
        (SqlType::Time, Datum::Time(v)) => Datum::Time(pgdatetime::adjust_time(v, tm)),
        (SqlType::Interval, Datum::Interval(iv)) => Datum::Interval(pgdatetime::adjust_interval_raw_typmod(iv, tm)?),
        // an array type's modifier applies to each element
        (SqlType::Array(elem), Datum::Array(a)) => {
            let elems = a.elems.iter().map(|e| apply_typmod(e.clone(), elem, tm, explicit)).collect::<Result<Vec<_>>>()?;
            Datum::array(crate::types::ArrayValue { elem_type: a.elem_type.clone(), dims: a.dims.clone(), elems })
        }
        (_, d) => d,
    })
}

/// Binary send format (for the extended protocol with binary results) of a
/// datum of type `ty`.
pub fn send_datum(d: &Datum, ty: &SqlType) -> Option<Vec<u8>> {
    Some(match d {
        Datum::Text(s) if *ty == SqlType::JsonPath => {
            let mut b = vec![1u8];
            b.extend_from_slice(s.as_bytes());
            b
        }
        Datum::Array(a) => {
            // ndim, has-nulls flag, element type, (length, lower bound) per
            // dimension, then each element as length + bytes (-1: NULL)
            let elem = ty.elem().cloned().unwrap_or_else(|| a.elem_type.clone());
            let mut b = Vec::new();
            b.extend_from_slice(&(a.dims.len() as i32).to_be_bytes());
            b.extend_from_slice(&(a.elems.iter().any(|e| e.is_null()) as i32).to_be_bytes());
            b.extend_from_slice(&elem.oid().to_be_bytes());
            for (lb, len) in &a.dims {
                b.extend_from_slice(&len.to_be_bytes());
                b.extend_from_slice(&lb.to_be_bytes());
            }
            for e in &a.elems {
                match send_datum(e, &elem) {
                    Some(v) => {
                        b.extend_from_slice(&(v.len() as i32).to_be_bytes());
                        b.extend_from_slice(&v);
                    }
                    None => b.extend_from_slice(&(-1i32).to_be_bytes()),
                }
            }
            b
        }
        Datum::Null => return None,
        Datum::Bool(b) => vec![*b as u8],
        Datum::Int2(v) => v.to_be_bytes().to_vec(),
        Datum::Int4(v) => v.to_be_bytes().to_vec(),
        Datum::Int8(v) => v.to_be_bytes().to_vec(),
        Datum::Oid(v) => v.to_be_bytes().to_vec(),
        Datum::Float4(v) => v.to_be_bytes().to_vec(),
        Datum::Float8(v) => v.to_be_bytes().to_vec(),
        Datum::Numeric(n) => n.to_pg_binary(),
        Datum::Text(s) | Datum::Json(s) => s.as_bytes().to_vec(),
        Datum::Bytea(b) => b.to_vec(),
        Datum::Date(v) => v.to_be_bytes().to_vec(),
        Datum::Time(v) | Datum::Timestamp(v) | Datum::TimestampTz(v) => v.to_be_bytes().to_vec(),
        Datum::Interval(iv) => {
            let mut b = iv.micros.to_be_bytes().to_vec();
            b.extend_from_slice(&iv.days.to_be_bytes());
            b.extend_from_slice(&iv.months.to_be_bytes());
            b
        }
        Datum::Uuid(u) => u.to_vec(),
        Datum::Jsonb(j) => {
            let mut b = vec![1u8];
            b.extend_from_slice(j.to_string().as_bytes());
            b
        }
    })
}

/// Binary array (see `send_datum`) with elements of type `elem`.
fn recv_array(b: &[u8], elem: &SqlType) -> Result<Datum> {
    let bad = || SqlError::new(codes::INVALID_BINARY_REPRESENTATION, "incorrect binary data format");
    fn take<'a>(b: &'a [u8], pos: &mut usize, n: usize) -> Result<&'a [u8]> {
        let v = b
            .get(*pos..pos.saturating_add(n))
            .ok_or_else(|| SqlError::new(codes::INVALID_BINARY_REPRESENTATION, "incorrect binary data format"))?;
        *pos += n;
        Ok(v)
    }
    let mut pos = 0usize;
    let int = |pos: &mut usize| -> Result<i32> { Ok(i32::from_be_bytes(take(b, pos, 4)?.try_into().unwrap())) };
    let ndim = int(&mut pos)?;
    if !(0..=6).contains(&ndim) {
        return Err(SqlError::new(
            codes::PROGRAM_LIMIT_EXCEEDED,
            format!("number of array dimensions ({ndim}) exceeds the maximum allowed (6)"),
        ));
    }
    let flags = int(&mut pos)?;
    if flags != 0 && flags != 1 {
        return Err(SqlError::new(codes::INVALID_BINARY_REPRESENTATION, "invalid array flags"));
    }
    let _elem_oid = int(&mut pos)?;
    let mut dims = Vec::with_capacity(ndim as usize);
    let mut count: i64 = 1;
    for _ in 0..ndim {
        let len = int(&mut pos)?;
        let lb = int(&mut pos)?;
        if len < 0 {
            return Err(bad());
        }
        count = count.saturating_mul(len as i64);
        dims.push((lb, len));
    }
    if ndim == 0 {
        count = 0;
    }
    let mut elems = Vec::with_capacity(count.min(1 << 20) as usize);
    for _ in 0..count {
        let len = int(&mut pos)?;
        if len == -1 {
            elems.push(Datum::Null);
            continue;
        }
        if len < 0 {
            return Err(bad());
        }
        elems.push(recv_datum(take(b, &mut pos, len as usize)?, elem)?);
    }
    if pos != b.len() {
        return Err(bad());
    }
    Ok(Datum::array(ArrayValue { elem_type: elem.clone(), dims, elems }))
}

/// Binary receive format for bind parameters.
pub fn recv_datum(b: &[u8], ty: &SqlType) -> Result<Datum> {
    let bad = || SqlError::new(codes::INVALID_BINARY_REPRESENTATION, "incorrect binary data format");
    let fixed = |n: usize| if b.len() == n { Ok(()) } else { Err(bad()) };
    Ok(match ty {
        SqlType::Bool => {
            fixed(1)?;
            Datum::Bool(b[0] != 0)
        }
        SqlType::Int2 => {
            fixed(2)?;
            Datum::Int2(i16::from_be_bytes(b.try_into().unwrap()))
        }
        SqlType::Int4 => {
            fixed(4)?;
            Datum::Int4(i32::from_be_bytes(b.try_into().unwrap()))
        }
        SqlType::Int8 => {
            fixed(8)?;
            Datum::Int8(i64::from_be_bytes(b.try_into().unwrap()))
        }
        SqlType::Oid => {
            fixed(4)?;
            Datum::Oid(u32::from_be_bytes(b.try_into().unwrap()))
        }
        SqlType::Float4 => {
            fixed(4)?;
            Datum::Float4(f32::from_be_bytes(b.try_into().unwrap()))
        }
        SqlType::Float8 => {
            fixed(8)?;
            Datum::Float8(f64::from_be_bytes(b.try_into().unwrap()))
        }
        SqlType::Numeric => Datum::numeric(pgnumeric::Numeric::from_pg_binary(b)?),
        SqlType::Date => {
            fixed(4)?;
            Datum::Date(i32::from_be_bytes(b.try_into().unwrap()))
        }
        SqlType::Time | SqlType::Timestamp | SqlType::TimestampTz => {
            fixed(8)?;
            let v = i64::from_be_bytes(b.try_into().unwrap());
            match ty {
                SqlType::Time => Datum::Time(v),
                SqlType::Timestamp => Datum::Timestamp(v),
                _ => Datum::TimestampTz(v),
            }
        }
        SqlType::Interval => {
            fixed(16)?;
            Datum::Interval(pgdatetime::Interval {
                micros: i64::from_be_bytes(b[0..8].try_into().unwrap()),
                days: i32::from_be_bytes(b[8..12].try_into().unwrap()),
                months: i32::from_be_bytes(b[12..16].try_into().unwrap()),
            })
        }
        SqlType::Uuid => {
            fixed(16)?;
            Datum::Uuid(b.try_into().unwrap())
        }
        SqlType::Bytea => Datum::bytea(b.to_vec()),
        SqlType::Jsonb | SqlType::JsonPath => {
            if b.first() != Some(&1) {
                return Err(bad());
            }
            let s = std::str::from_utf8(&b[1..]).map_err(|_| bad())?;
            parse_datum(s, ty, -1)?
        }
        SqlType::Array(elem) => recv_array(b, elem)?,
        _ => {
            let s = std::str::from_utf8(b).map_err(|_| bad())?;
            parse_datum(s, ty, -1)?
        }
    })
}
