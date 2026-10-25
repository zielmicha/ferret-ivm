//! Compact self-describing value codec, used for row payloads in storage and
//! for the input log. Exact (preserves numeric scale, -0, interval fields).

use super::datum::{ArrayValue, Datum};
use super::SqlType;
use crate::error::{Result, SqlError};
use pgdatetime::Interval;
use std::sync::Arc;

pub fn put_uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

pub fn get_uvarint(buf: &mut &[u8]) -> Result<u64> {
    let mut v: u64 = 0;
    let mut shift = 0;
    loop {
        let b = *buf.first().ok_or_else(|| SqlError::corrupt("truncated varint"))?;
        *buf = &buf[1..];
        if shift >= 64 {
            return Err(SqlError::corrupt("varint overflow"));
        }
        v |= ((b & 0x7F) as u64) << shift;
        if b < 0x80 {
            return Ok(v);
        }
        shift += 7;
    }
}

pub fn put_ivarint(out: &mut Vec<u8>, v: i64) {
    put_uvarint(out, ((v << 1) ^ (v >> 63)) as u64);
}

pub fn get_ivarint(buf: &mut &[u8]) -> Result<i64> {
    let u = get_uvarint(buf)?;
    Ok(((u >> 1) as i64) ^ -((u & 1) as i64))
}

pub fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_uvarint(out, b.len() as u64);
    out.extend_from_slice(b);
}

pub fn get_bytes<'a>(buf: &mut &'a [u8]) -> Result<&'a [u8]> {
    let len = get_uvarint(buf)? as usize;
    if buf.len() < len {
        return Err(SqlError::corrupt("truncated bytes"));
    }
    let (a, b) = buf.split_at(len);
    *buf = b;
    Ok(a)
}

pub fn put_str(out: &mut Vec<u8>, s: &str) {
    put_bytes(out, s.as_bytes());
}

pub fn get_str<'a>(buf: &mut &'a [u8]) -> Result<&'a str> {
    std::str::from_utf8(get_bytes(buf)?).map_err(|_| SqlError::corrupt("invalid utf8"))
}

pub fn get_u8(buf: &mut &[u8]) -> Result<u8> {
    let b = *buf.first().ok_or_else(|| SqlError::corrupt("truncated"))?;
    *buf = &buf[1..];
    Ok(b)
}

fn get_fixed<const N: usize>(buf: &mut &[u8]) -> Result<[u8; N]> {
    if buf.len() < N {
        return Err(SqlError::corrupt("truncated"));
    }
    let (a, b) = buf.split_at(N);
    *buf = b;
    Ok(a.try_into().unwrap())
}

const T_NULL: u8 = 0;
const T_FALSE: u8 = 1;
const T_TRUE: u8 = 2;
const T_INT2: u8 = 3;
const T_INT4: u8 = 4;
const T_INT8: u8 = 5;
const T_FLOAT4: u8 = 6;
const T_FLOAT8: u8 = 7;
const T_NUMERIC: u8 = 8;
const T_TEXT: u8 = 9;
const T_JSON: u8 = 10;
const T_BYTEA: u8 = 11;
const T_DATE: u8 = 12;
const T_TIME: u8 = 13;
const T_TIMESTAMP: u8 = 14;
const T_TIMESTAMPTZ: u8 = 15;
const T_INTERVAL: u8 = 16;
const T_UUID: u8 = 17;
const T_JSONB: u8 = 18;
const T_ARRAY: u8 = 19;
const T_OID: u8 = 20;

pub fn encode_datum(d: &Datum, out: &mut Vec<u8>) {
    match d {
        Datum::Null => out.push(T_NULL),
        Datum::Bool(false) => out.push(T_FALSE),
        Datum::Bool(true) => out.push(T_TRUE),
        Datum::Int2(v) => {
            out.push(T_INT2);
            put_ivarint(out, *v as i64);
        }
        Datum::Int4(v) => {
            out.push(T_INT4);
            put_ivarint(out, *v as i64);
        }
        Datum::Int8(v) => {
            out.push(T_INT8);
            put_ivarint(out, *v);
        }
        Datum::Oid(v) => {
            out.push(T_OID);
            put_uvarint(out, *v as u64);
        }
        Datum::Float4(v) => {
            out.push(T_FLOAT4);
            out.extend_from_slice(&v.to_bits().to_le_bytes());
        }
        Datum::Float8(v) => {
            out.push(T_FLOAT8);
            out.extend_from_slice(&v.to_bits().to_le_bytes());
        }
        Datum::Numeric(n) => {
            out.push(T_NUMERIC);
            n.encode_binary(out);
        }
        Datum::Text(s) => {
            out.push(T_TEXT);
            put_str(out, s);
        }
        Datum::Json(s) => {
            out.push(T_JSON);
            put_str(out, s);
        }
        Datum::Bytea(b) => {
            out.push(T_BYTEA);
            put_bytes(out, b);
        }
        Datum::Date(v) => {
            out.push(T_DATE);
            put_ivarint(out, *v as i64);
        }
        Datum::Time(v) => {
            out.push(T_TIME);
            put_ivarint(out, *v);
        }
        Datum::Timestamp(v) => {
            out.push(T_TIMESTAMP);
            put_ivarint(out, *v);
        }
        Datum::TimestampTz(v) => {
            out.push(T_TIMESTAMPTZ);
            put_ivarint(out, *v);
        }
        Datum::Interval(iv) => {
            out.push(T_INTERVAL);
            put_ivarint(out, iv.months as i64);
            put_ivarint(out, iv.days as i64);
            put_ivarint(out, iv.micros);
        }
        Datum::Uuid(u) => {
            out.push(T_UUID);
            out.extend_from_slice(u);
        }
        Datum::Jsonb(j) => {
            out.push(T_JSONB);
            put_str(out, &j.to_string());
        }
        Datum::Array(a) => {
            out.push(T_ARRAY);
            encode_type(&a.elem_type, out);
            put_uvarint(out, a.dims.len() as u64);
            for (lb, len) in &a.dims {
                put_ivarint(out, *lb as i64);
                put_ivarint(out, *len as i64);
            }
            put_uvarint(out, a.elems.len() as u64);
            for e in &a.elems {
                encode_datum(e, out);
            }
        }
    }
}

/// About the length of `encode_datum`'s encoding, without encoding (to
/// bound batches by size): exact but for numeric and jsonb values.
pub fn approx_len(d: &Datum) -> usize {
    fn jsonb(j: &pgfuncs::json::Jsonb) -> usize {
        use pgfuncs::json::Jsonb;
        match j {
            Jsonb::Null | Jsonb::Bool(_) => 5,
            Jsonb::Number(_) => 12,
            Jsonb::String(s) => s.len() + 2,
            Jsonb::Array(v) => 2 + v.iter().map(|e| jsonb(e) + 1).sum::<usize>(),
            Jsonb::Object(v) => 2 + v.iter().map(|(k, e)| k.len() + 4 + jsonb(e)).sum::<usize>(),
        }
    }
    1 + match d {
        Datum::Null | Datum::Bool(_) => 0,
        Datum::Int2(_) | Datum::Int4(_) | Datum::Date(_) | Datum::Oid(_) => 4,
        Datum::Int8(_) | Datum::Float4(_) | Datum::Float8(_) | Datum::Time(_) | Datum::Timestamp(_) | Datum::TimestampTz(_) => 8,
        Datum::Numeric(_) => 16,
        Datum::Text(s) | Datum::Json(s) => s.len() + 3,
        Datum::Bytea(b) => b.len() + 3,
        Datum::Interval(_) => 16,
        Datum::Uuid(_) => 16,
        Datum::Jsonb(j) => jsonb(j) + 3,
        Datum::Array(a) => 8 + a.dims.len() * 8 + a.elems.iter().map(approx_len).sum::<usize>(),
    }
}

pub fn decode_datum(buf: &mut &[u8]) -> Result<Datum> {
    let tag = get_u8(buf)?;
    Ok(match tag {
        T_NULL => Datum::Null,
        T_FALSE => Datum::Bool(false),
        T_TRUE => Datum::Bool(true),
        T_INT2 => Datum::Int2(get_ivarint(buf)? as i16),
        T_INT4 => Datum::Int4(get_ivarint(buf)? as i32),
        T_INT8 => Datum::Int8(get_ivarint(buf)?),
        T_OID => Datum::Oid(get_uvarint(buf)? as u32),
        T_FLOAT4 => Datum::Float4(f32::from_bits(u32::from_le_bytes(get_fixed::<4>(buf)?))),
        T_FLOAT8 => Datum::Float8(f64::from_bits(u64::from_le_bytes(get_fixed::<8>(buf)?))),
        T_NUMERIC => {
            let (n, len) = pgnumeric::Numeric::decode_binary(buf)?;
            *buf = &buf[len..];
            Datum::Numeric(Arc::new(n))
        }
        T_TEXT => Datum::Text(Arc::from(get_str(buf)?)),
        T_JSON => Datum::Json(Arc::from(get_str(buf)?)),
        T_BYTEA => Datum::Bytea(Arc::from(get_bytes(buf)?)),
        T_DATE => Datum::Date(get_ivarint(buf)? as i32),
        T_TIME => Datum::Time(get_ivarint(buf)?),
        T_TIMESTAMP => Datum::Timestamp(get_ivarint(buf)?),
        T_TIMESTAMPTZ => Datum::TimestampTz(get_ivarint(buf)?),
        T_INTERVAL => Datum::Interval(Interval {
            months: get_ivarint(buf)? as i32,
            days: get_ivarint(buf)? as i32,
            micros: get_ivarint(buf)?,
        }),
        T_UUID => Datum::Uuid(get_fixed::<16>(buf)?),
        T_JSONB => Datum::Jsonb(Arc::new(pgfuncs::json::Jsonb::parse(get_str(buf)?)?)),
        T_ARRAY => {
            let elem_type = decode_type(buf)?;
            let ndims = get_uvarint(buf)? as usize;
            let mut dims = Vec::with_capacity(ndims);
            for _ in 0..ndims {
                dims.push((get_ivarint(buf)? as i32, get_ivarint(buf)? as i32));
            }
            let n = get_uvarint(buf)? as usize;
            let mut elems = Vec::with_capacity(n);
            for _ in 0..n {
                elems.push(decode_datum(buf)?);
            }
            Datum::Array(Arc::new(ArrayValue { elem_type, dims, elems }))
        }
        _ => return Err(SqlError::corrupt(format!("bad datum tag {tag}"))),
    })
}

fn skip_varint(buf: &mut &[u8]) -> Result<()> {
    let n = buf.iter().position(|b| *b < 0x80).ok_or_else(|| SqlError::corrupt("truncated varint"))?;
    *buf = &buf[n + 1..];
    Ok(())
}

fn skip_n(buf: &mut &[u8], n: usize) -> Result<()> {
    *buf = buf.get(n..).ok_or_else(|| SqlError::corrupt("truncated"))?;
    Ok(())
}

/// Skip one value encoded by [`encode_datum`] without decoding it.
pub fn skip_datum(buf: &mut &[u8]) -> Result<()> {
    let tag = get_u8(buf)?;
    match tag {
        T_NULL | T_FALSE | T_TRUE => Ok(()),
        T_INT2 | T_INT4 | T_INT8 | T_OID | T_DATE | T_TIME | T_TIMESTAMP | T_TIMESTAMPTZ => skip_varint(buf),
        T_FLOAT4 => skip_n(buf, 4),
        T_FLOAT8 => skip_n(buf, 8),
        T_UUID => skip_n(buf, 16),
        T_NUMERIC => {
            let n = pgnumeric::Numeric::binary_len(buf)?;
            skip_n(buf, n)
        }
        T_TEXT | T_JSON | T_BYTEA | T_JSONB => get_bytes(buf).map(|_| ()),
        T_INTERVAL => {
            skip_varint(buf)?;
            skip_varint(buf)?;
            skip_varint(buf)
        }
        T_ARRAY => {
            decode_type(buf)?;
            let ndims = get_uvarint(buf)?;
            for _ in 0..2 * ndims {
                skip_varint(buf)?;
            }
            let n = get_uvarint(buf)?;
            for _ in 0..n {
                skip_datum(buf)?;
            }
            Ok(())
        }
        _ => Err(SqlError::corrupt(format!("bad datum tag {tag}"))),
    }
}

pub fn encode_row(row: &[Datum], out: &mut Vec<u8>) {
    put_uvarint(out, row.len() as u64);
    for d in row {
        encode_datum(d, out);
    }
}

pub fn decode_row(buf: &mut &[u8]) -> Result<Vec<Datum>> {
    let n = get_uvarint(buf)? as usize;
    let mut row = Vec::with_capacity(n.min(4096));
    for _ in 0..n {
        row.push(decode_datum(buf)?);
    }
    Ok(row)
}

pub fn encode_type(ty: &SqlType, out: &mut Vec<u8>) {
    match ty {
        SqlType::Array(e) => {
            out.push(0xFF);
            encode_type(e, out);
        }
        other => put_uvarint(out, other.oid() as u64),
    }
}

pub fn decode_type(buf: &mut &[u8]) -> Result<SqlType> {
    if buf.first() == Some(&0xFF) {
        *buf = &buf[1..];
        return Ok(SqlType::Array(Box::new(decode_type(buf)?)));
    }
    let oid = get_uvarint(buf)? as u32;
    SqlType::from_oid(oid).ok_or_else(|| SqlError::corrupt(format!("unknown type oid {oid}")))
}
