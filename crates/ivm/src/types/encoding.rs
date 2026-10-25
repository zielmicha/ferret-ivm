//! Order-preserving ("memcomparable") key encoding.
//!
//! `memcmp` of two encodings equals SQL ordering of the encoded values, and
//! every encoding is prefix-free, so encodings concatenate into composite keys
//! and descending columns are produced by inverting the value bytes.
//!
//! The canonical encoding follows SQL *equality*: values that compare equal
//! encode identically (numeric `1.0` vs `1.00`, interval `1 mon` vs `30 days`,
//! float `-0` vs `0`). The bits needed to reconstruct the exact value are kept
//! in a separate "fidelity" suffix appended after all key columns, so key
//! prefixes still group equal values together.

use super::datum::{ArrayValue, Datum};
use super::SqlType;
use crate::error::{Result, SqlError};
use pgdatetime::Interval;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub const TAG_NULL_FIRST: u8 = 0x01;
pub const TAG_VALUE: u8 = 0x02;
pub const TAG_NULL_LAST: u8 = 0x03;

/// Sort direction and null placement of one key column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct SortSpec {
    pub desc: bool,
    pub nulls_first: bool,
}

impl SortSpec {
    pub const ASC: SortSpec = SortSpec { desc: false, nulls_first: false };
    pub const DESC: SortSpec = SortSpec { desc: true, nulls_first: true };
    pub fn new(desc: bool, nulls_first: Option<bool>) -> SortSpec {
        SortSpec { desc, nulls_first: nulls_first.unwrap_or(desc) }
    }
}

impl Default for SortSpec {
    fn default() -> Self {
        SortSpec::ASC
    }
}

/// Append the canonical key encoding of `d`.
pub fn encode_key(d: &Datum, spec: SortSpec, out: &mut Vec<u8>) {
    if d.is_null() {
        out.push(if spec.nulls_first { TAG_NULL_FIRST } else { TAG_NULL_LAST });
        return;
    }
    out.push(TAG_VALUE);
    let start = out.len();
    encode_value(d, out);
    if spec.desc {
        for b in &mut out[start..] {
            *b = !*b;
        }
    }
}

fn encode_value(d: &Datum, out: &mut Vec<u8>) {
    match d {
        Datum::Null => unreachable!(),
        Datum::Bool(b) => out.push(*b as u8),
        Datum::Int2(v) => out.extend_from_slice(&((*v as u16) ^ 0x8000).to_be_bytes()),
        Datum::Int4(v) => out.extend_from_slice(&((*v as u32) ^ 0x8000_0000).to_be_bytes()),
        Datum::Int8(v) => out.extend_from_slice(&((*v as u64) ^ (1 << 63)).to_be_bytes()),
        Datum::Oid(v) => out.extend_from_slice(&v.to_be_bytes()),
        Datum::Float4(v) => {
            let v = if v.is_nan() { f32::NAN } else if *v == 0.0 { 0.0 } else { *v };
            let bits = v.to_bits();
            let enc = if bits & 0x8000_0000 != 0 { !bits } else { bits | 0x8000_0000 };
            out.extend_from_slice(&enc.to_be_bytes());
        }
        Datum::Float8(v) => {
            let v = if v.is_nan() { f64::NAN } else if *v == 0.0 { 0.0 } else { *v };
            let bits = v.to_bits();
            let enc = if bits & (1 << 63) != 0 { !bits } else { bits | (1 << 63) };
            out.extend_from_slice(&enc.to_be_bytes());
        }
        Datum::Numeric(n) => n.encode_key(out),
        Datum::Text(s) | Datum::Json(s) => encode_bytes(s.as_bytes(), out),
        Datum::Bytea(b) => encode_bytes(b, out),
        Datum::Date(v) => out.extend_from_slice(&((*v as u32) ^ 0x8000_0000).to_be_bytes()),
        Datum::Time(v) | Datum::Timestamp(v) | Datum::TimestampTz(v) => {
            out.extend_from_slice(&((*v as u64) ^ (1 << 63)).to_be_bytes())
        }
        Datum::Interval(iv) => {
            let span = pgdatetime::interval_cmp_value(*iv);
            out.extend_from_slice(&((span as u128) ^ (1 << 127)).to_be_bytes());
        }
        Datum::Uuid(u) => out.extend_from_slice(u),
        Datum::Jsonb(j) => j.encode_key(out),
        Datum::Array(a) => {
            // elements, then (as PostgreSQL orders arrays) dimensionality,
            // lengths and lower bounds: arrays differing only in shape differ
            for e in &a.elems {
                if e.is_null() {
                    out.push(TAG_NULL_LAST);
                } else {
                    out.push(TAG_VALUE);
                    encode_value(e, out);
                }
            }
            out.push(0x00);
            out.push(a.dims.len() as u8);
            for (_, len) in &a.dims {
                out.extend_from_slice(&(*len as u32).to_be_bytes());
            }
            for (lb, _) in &a.dims {
                out.extend_from_slice(&((*lb as u32) ^ 0x8000_0000).to_be_bytes());
            }
        }
    }
}

/// Escape 0x00 as 0x00 0xFF and terminate with 0x00 0x00 (prefix-free).
fn encode_bytes(b: &[u8], out: &mut Vec<u8>) {
    for &c in b {
        if c == 0 {
            out.push(0);
            out.push(0xFF);
        } else {
            out.push(c);
        }
    }
    out.push(0);
    out.push(0);
}

/// Decode one key column of type `ty`. Returns the value and bytes consumed.
pub fn decode_key(buf: &[u8], ty: &SqlType, spec: SortSpec) -> Result<(Datum, usize)> {
    let tag = *buf.first().ok_or_else(|| SqlError::corrupt("truncated key"))?;
    match tag {
        TAG_NULL_FIRST | TAG_NULL_LAST => Ok((Datum::Null, 1)),
        TAG_VALUE => {
            let (d, n) = if spec.desc {
                // Decode from an inverted copy. Values are prefix-free so we
                // can decode incrementally; invert lazily up to a bound.
                let inv: Vec<u8> = buf[1..].iter().map(|b| !b).collect();
                decode_value(&inv, ty)?
            } else {
                decode_value(&buf[1..], ty)?
            };
            Ok((d, n + 1))
        }
        _ => Err(SqlError::corrupt(format!("bad key tag {tag}"))),
    }
}

fn take<const N: usize>(buf: &[u8]) -> Result<[u8; N]> {
    buf.get(..N)
        .map(|s| s.try_into().unwrap())
        .ok_or_else(|| SqlError::corrupt("truncated key value"))
}

fn decode_value(buf: &[u8], ty: &SqlType) -> Result<(Datum, usize)> {
    Ok(match ty {
        SqlType::Bool => (Datum::Bool(*buf.first().ok_or_else(|| SqlError::corrupt("truncated"))? != 0), 1),
        SqlType::Int2 => (Datum::Int2((u16::from_be_bytes(take::<2>(buf)?) ^ 0x8000) as i16), 2),
        SqlType::Int4 => (Datum::Int4((u32::from_be_bytes(take::<4>(buf)?) ^ 0x8000_0000) as i32), 4),
        SqlType::Int8 => (Datum::Int8((u64::from_be_bytes(take::<8>(buf)?) ^ (1 << 63)) as i64), 8),
        SqlType::Oid => (Datum::Oid(u32::from_be_bytes(take::<4>(buf)?)), 4),
        SqlType::Float4 => {
            let enc = u32::from_be_bytes(take::<4>(buf)?);
            let bits = if enc & 0x8000_0000 != 0 { enc & 0x7FFF_FFFF } else { !enc };
            (Datum::Float4(f32::from_bits(bits)), 4)
        }
        SqlType::Float8 => {
            let enc = u64::from_be_bytes(take::<8>(buf)?);
            let bits = if enc & (1 << 63) != 0 { enc & !(1 << 63) } else { !enc };
            (Datum::Float8(f64::from_bits(bits)), 8)
        }
        SqlType::Numeric => {
            let (n, len) = pgnumeric::Numeric::decode_key(buf)?;
            (Datum::Numeric(Arc::new(n)), len)
        }
        SqlType::Text | SqlType::Varchar | SqlType::Bpchar | SqlType::Name | SqlType::Json | SqlType::JsonPath => {
            let (bytes, len) = decode_bytes(buf)?;
            let s = String::from_utf8(bytes).map_err(|_| SqlError::corrupt("invalid utf8 in key"))?;
            let a: Arc<str> = Arc::from(s.into_boxed_str());
            (if *ty == SqlType::Json { Datum::Json(a) } else { Datum::Text(a) }, len)
        }
        SqlType::Bytea => {
            let (bytes, len) = decode_bytes(buf)?;
            (Datum::Bytea(Arc::from(bytes.into_boxed_slice())), len)
        }
        SqlType::Date => (Datum::Date((u32::from_be_bytes(take::<4>(buf)?) ^ 0x8000_0000) as i32), 4),
        SqlType::Time | SqlType::Timestamp | SqlType::TimestampTz => {
            let v = (u64::from_be_bytes(take::<8>(buf)?) ^ (1 << 63)) as i64;
            (
                match ty {
                    SqlType::Time => Datum::Time(v),
                    SqlType::Timestamp => Datum::Timestamp(v),
                    _ => Datum::TimestampTz(v),
                },
                8,
            )
        }
        SqlType::Interval => {
            // Canonical form only carries the span; the fidelity suffix
            // restores the exact fields. Default to a micros-only interval.
            let span = (u128::from_be_bytes(take::<16>(buf)?) ^ (1 << 127)) as i128;
            let iv = interval_from_span(span);
            (Datum::Interval(iv), 16)
        }
        SqlType::Uuid => (Datum::Uuid(take::<16>(buf)?), 16),
        SqlType::Jsonb => {
            let (j, len) = pgfuncs::json::Jsonb::decode_key(buf)?;
            (Datum::Jsonb(Arc::new(j)), len)
        }
        SqlType::Array(elem) => {
            let mut pos = 0;
            let mut elems = Vec::new();
            loop {
                let tag = *buf.get(pos).ok_or_else(|| SqlError::corrupt("truncated array key"))?;
                pos += 1;
                match tag {
                    0x00 => break,
                    TAG_NULL_LAST => elems.push(Datum::Null),
                    TAG_VALUE => {
                        let (d, n) = decode_value(&buf[pos..], elem)?;
                        pos += n;
                        elems.push(d);
                    }
                    _ => return Err(SqlError::corrupt("bad array key tag")),
                }
            }
            let nd = *buf.get(pos).ok_or_else(|| SqlError::corrupt("truncated array key"))? as usize;
            pos += 1;
            let word = |at: usize| -> Result<u32> {
                Ok(u32::from_be_bytes(buf.get(at..at + 4).ok_or_else(|| SqlError::corrupt("truncated array key"))?.try_into().unwrap()))
            };
            let mut dims = Vec::with_capacity(nd);
            for d in 0..nd {
                let len = word(pos + 4 * d)? as i32;
                let lb = (word(pos + 4 * (nd + d))? ^ 0x8000_0000) as i32;
                dims.push((lb, len));
            }
            pos += 8 * nd;
            (Datum::Array(Arc::new(ArrayValue { elem_type: (**elem).clone(), dims, elems })), pos)
        }
        SqlType::Unknown => {
            let (bytes, len) = decode_bytes(buf)?;
            (Datum::Text(Arc::from(String::from_utf8_lossy(&bytes).into_owned().into_boxed_str())), len)
        }
    })
}

/// An interval with the given span (interval_cmp_value). Spans beyond what
/// whole months can carry (the infinities, extreme field values) spill into
/// days and then microseconds, so that the result always has exactly this
/// span: re-encoding a decoded key must give the same bytes.
fn interval_from_span(span: i128) -> Interval {
    // interval_cmp_value = ((months*30 + days) * USECS_PER_DAY) + micros
    const USECS_PER_DAY: i128 = 86_400_000_000;
    let clamp32 = |v: i128| v.clamp(i32::MIN as i128, i32::MAX as i128);
    let total_days = span.div_euclid(USECS_PER_DAY);
    let months = clamp32(total_days / 30);
    let days = clamp32(total_days - months * 30);
    let micros = span - (months * 30 + days) * USECS_PER_DAY;
    Interval { months: months as i32, days: days as i32, micros: micros.clamp(i64::MIN as i128, i64::MAX as i128) as i64 }
}

fn decode_bytes(buf: &[u8]) -> Result<(Vec<u8>, usize)> {
    let mut out = Vec::new();
    let mut i = 0;
    loop {
        let c = *buf.get(i).ok_or_else(|| SqlError::corrupt("unterminated bytes in key"))?;
        if c == 0 {
            let n = *buf.get(i + 1).ok_or_else(|| SqlError::corrupt("unterminated bytes in key"))?;
            if n == 0 {
                return Ok((out, i + 2));
            } else if n == 0xFF {
                out.push(0);
                i += 2;
            } else {
                return Err(SqlError::corrupt("bad escape in key"));
            }
        } else {
            out.push(c);
            i += 1;
        }
    }
}

/// Length of one encoded key column, found without decoding (allocating)
/// it for all but jsonb and array values.
pub fn skip_key(buf: &[u8], ty: &SqlType, spec: SortSpec) -> Result<usize> {
    let tag = *buf.first().ok_or_else(|| SqlError::corrupt("truncated key"))?;
    match tag {
        TAG_NULL_FIRST | TAG_NULL_LAST => return Ok(1),
        TAG_VALUE => {}
        _ => return Err(SqlError::corrupt(format!("bad key tag {tag}"))),
    }
    let fixed = |n: usize| if buf.len() > n { Ok(1 + n) } else { Err(SqlError::corrupt("truncated key value")) };
    match ty {
        SqlType::Bool => fixed(1),
        SqlType::Int2 => fixed(2),
        SqlType::Int4 | SqlType::Oid | SqlType::Float4 | SqlType::Date => fixed(4),
        SqlType::Int8 | SqlType::Float8 | SqlType::Time | SqlType::Timestamp | SqlType::TimestampTz => fixed(8),
        SqlType::Interval | SqlType::Uuid => fixed(16),
        SqlType::Numeric => Ok(1 + pgnumeric::Numeric::key_len(&buf[1..], spec.desc)?),
        SqlType::Text
        | SqlType::Varchar
        | SqlType::Bpchar
        | SqlType::Name
        | SqlType::Json
        | SqlType::JsonPath
        | SqlType::Bytea
        | SqlType::Unknown => Ok(1 + skip_bytes(&buf[1..], if spec.desc { 0xFF } else { 0x00 })?),
        SqlType::Jsonb | SqlType::Array(_) => Ok(decode_key(buf, ty, spec)?.1),
    }
}

/// Length of an `encode_bytes` encoding whose bytes are XORed with `mask`.
fn skip_bytes(buf: &[u8], mask: u8) -> Result<usize> {
    let mut i = 0;
    loop {
        let off = buf[i..].iter().position(|b| *b == mask).ok_or_else(|| SqlError::corrupt("unterminated bytes in key"))?;
        i += off;
        match buf.get(i + 1).map(|b| b ^ mask) {
            Some(0) => return Ok(i + 2),
            Some(0xFF) => i += 2,
            _ => return Err(SqlError::corrupt("bad escape in key")),
        }
    }
}

/// Length of the fidelity suffix of a non-null value of type `ty`. Arrays
/// are not supported (their suffix depends on which elements are null).
pub fn skip_fidelity(buf: &[u8], ty: &SqlType) -> Result<usize> {
    let mut rd = buf;
    match ty {
        SqlType::Numeric => {
            super::codec::get_uvarint(&mut rd)?;
        }
        SqlType::Float4 | SqlType::Float8 => rd = rd.get(1..).ok_or_else(|| SqlError::corrupt("truncated fidelity"))?,
        SqlType::Interval => rd = rd.get(16..).ok_or_else(|| SqlError::corrupt("truncated fidelity"))?,
        SqlType::Jsonb => {
            let len = super::codec::get_uvarint(&mut rd)? as usize;
            rd = rd.get(len..).ok_or_else(|| SqlError::corrupt("truncated fidelity"))?;
        }
        SqlType::Array(_) => return Err(SqlError::internal("skip_fidelity on an array")),
        _ => {}
    }
    Ok(buf.len() - rd.len())
}

// ---------------------------------------------------------------------------
// Fidelity suffix

/// Append the bits needed to restore `d` exactly from its canonical encoding.
pub fn encode_fidelity(d: &Datum, out: &mut Vec<u8>) {
    match d {
        Datum::Numeric(n) => super::codec::put_uvarint(out, n.dscale() as u64),
        Datum::Float4(v) => out.push((*v == 0.0 && v.is_sign_negative()) as u8),
        Datum::Float8(v) => out.push((*v == 0.0 && v.is_sign_negative()) as u8),
        Datum::Interval(iv) => {
            out.extend_from_slice(&iv.months.to_le_bytes());
            out.extend_from_slice(&iv.days.to_le_bytes());
            out.extend_from_slice(&iv.micros.to_le_bytes());
        }
        Datum::Jsonb(j) => {
            let s = j.to_string();
            super::codec::put_uvarint(out, s.len() as u64);
            out.extend_from_slice(s.as_bytes());
        }
        Datum::Array(a) => {
            for e in &a.elems {
                if !e.is_null() {
                    encode_fidelity(e, out);
                }
            }
        }
        _ => {}
    }
}

/// Whether a value of this type has a fidelity suffix.
pub fn has_fidelity(ty: &SqlType) -> bool {
    matches!(
        ty,
        SqlType::Numeric | SqlType::Float4 | SqlType::Float8 | SqlType::Interval | SqlType::Jsonb
    ) || matches!(ty, SqlType::Array(e) if has_fidelity(e))
}

/// Restore `d` (decoded canonically) using its fidelity suffix. Returns bytes consumed.
pub fn apply_fidelity(d: &mut Datum, buf: &[u8]) -> Result<usize> {
    let mut rd = buf;
    match d {
        Datum::Numeric(n) => {
            let dscale = super::codec::get_uvarint(&mut rd)? as u32;
            *n = Arc::new(n.with_dscale(dscale));
        }
        Datum::Float4(v) => {
            let neg = *rd.first().ok_or_else(|| SqlError::corrupt("truncated fidelity"))? != 0;
            rd = &rd[1..];
            if neg {
                *v = -0.0;
            }
        }
        Datum::Float8(v) => {
            let neg = *rd.first().ok_or_else(|| SqlError::corrupt("truncated fidelity"))? != 0;
            rd = &rd[1..];
            if neg {
                *v = -0.0;
            }
        }
        Datum::Interval(iv) => {
            let b = take::<16>(rd)?;
            iv.months = i32::from_le_bytes(b[0..4].try_into().unwrap());
            iv.days = i32::from_le_bytes(b[4..8].try_into().unwrap());
            iv.micros = i64::from_le_bytes(b[8..16].try_into().unwrap());
            rd = &rd[16..];
        }
        Datum::Jsonb(j) => {
            let len = super::codec::get_uvarint(&mut rd)? as usize;
            let s = std::str::from_utf8(rd.get(..len).ok_or_else(|| SqlError::corrupt("truncated fidelity"))?)
                .map_err(|_| SqlError::corrupt("bad utf8"))?;
            *j = Arc::new(pgfuncs::json::Jsonb::parse(s)?);
            rd = &rd[len..];
        }
        Datum::Array(a) => {
            let mut arr = (**a).clone();
            for e in arr.elems.iter_mut() {
                if !e.is_null() {
                    let n = apply_fidelity(e, rd)?;
                    rd = &rd[n..];
                }
            }
            *a = Arc::new(arr);
        }
        _ => {}
    }
    Ok(buf.len() - rd.len())
}

/// Smallest byte string greater than every string having `prefix` as a prefix.
/// Returns None if no such string exists (prefix is all 0xFF).
pub fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut v = prefix.to_vec();
    while let Some(last) = v.pop() {
        if last != 0xFF {
            v.push(last + 1);
            return Some(v);
        }
    }
    None
}

#[cfg(test)]
mod order_tests {
    use super::*;
    use crate::types::text::parse_datum;
    use std::cmp::Ordering;

    fn enc(d: &Datum, spec: SortSpec) -> Vec<u8> {
        let mut v = Vec::new();
        encode_key(d, spec, &mut v);
        v
    }

    /// For every pair: memcmp of the encodings orders like sql_cmp (reversed
    /// for DESC), equal values encode identically, and decoding plus the
    /// fidelity suffix restores the exact value.
    fn check(ty: &SqlType, texts: &[&str]) {
        let vals: Vec<Datum> = texts.iter().map(|t| parse_datum(t, ty, -1).unwrap_or_else(|e| panic!("{t}: {e}"))).collect();
        for spec in [SortSpec::ASC, SortSpec::DESC] {
            for (i, a) in vals.iter().enumerate() {
                let ea = enc(a, spec);
                for (j, b) in vals.iter().enumerate() {
                    let eb = enc(b, spec);
                    let want = if spec.desc { b.sql_cmp(a) } else { a.sql_cmp(b) };
                    assert_eq!(ea.cmp(&eb), want, "{ty:?} {spec:?}: {} vs {}", texts[i], texts[j]);
                }
                let (mut d, n) = decode_key(&ea, ty, spec).unwrap();
                assert_eq!(n, ea.len(), "{}: length", texts[i]);
                let mut fid = Vec::new();
                if !a.is_null() && has_fidelity(ty) {
                    encode_fidelity(a, &mut fid);
                    let used = apply_fidelity(&mut d, &fid).unwrap();
                    assert_eq!(used, fid.len());
                }
                assert_eq!(crate::types::datum::exact_cmp(&d, a), Ordering::Equal, "{ty:?}: {} round trip gave {d:?}", texts[i]);
            }
        }
    }

    #[test]
    fn key_order_matches_sql_order() {
        check(&SqlType::Numeric, &["-Infinity", "-1e100", "-12.5", "-12.50", "-1", "-0.001", "0", "0.000", "0.001", "1", "1.0", "9.99", "10", "100", "1e100", "Infinity", "NaN"]);
        check(&SqlType::Float8, &["-Infinity", "-1e308", "-1", "-0", "0", "5e-324", "1", "1.5", "1e308", "Infinity", "NaN"]);
        check(&SqlType::Float4, &["-Infinity", "-1", "-0", "0", "1e-45", "1", "3.4e38", "Infinity", "NaN"]);
        check(&SqlType::Interval, &["-infinity", "-1 year", "-1 day", "0", "1 microsecond", "1 day", "24 hours", "1 mon", "30 days", "1 year", "infinity"]);
        check(&SqlType::Date, &["-infinity", "4713-01-01 BC", "2000-01-01", "2026-10-25", "294276-12-31", "infinity"]);
        check(&SqlType::TimestampTz, &["-infinity", "1999-12-31 23:59:59.999999+00", "2000-01-01 00:00:00+00", "2026-10-25 12:00:00+00", "infinity"]);
        check(&SqlType::Text, &["", "A", "Z", "a", "a ", "aa", "b", "é", "€", "\u{10FFFF}"]);
        check(&SqlType::Bytea, &["\\x", "\\x00", "\\x0000", "\\x0001", "\\x01", "\\xff", "\\xff00"]);
        check(&SqlType::Uuid, &["00000000-0000-0000-0000-000000000000", "00000000-0000-0000-0000-000000000001", "ffffffff-ffff-ffff-ffff-ffffffffffff"]);
        check(&SqlType::Bool, &["false", "true"]);
        check(&SqlType::Array(Box::new(SqlType::Int4)), &["{}", "{1}", "{1,2}", "{1,2,3}", "{1,NULL}", "{2}", "{NULL}", "{{1,2},{3,4}}", "[0:1]={1,2}", "{1,2,3,4}"]);
        check(&SqlType::Array(Box::new(SqlType::Numeric)), &["{}", "{1.0}", "{1.00,2}", "{NaN}", "{-1}"]);
        check(&SqlType::Array(Box::new(SqlType::Text)), &["{}", "{\"\"}", "{a}", "{a,b}", "{ab}", "{b}", "{NULL}"]);
        check(&SqlType::Jsonb, &["null", "\"\"", "\"a\"", "\"b\"", "1", "1.0", "2", "false", "true", "[]", "[1]", "[1,2]", "[2]", "{}", "{\"a\": 1}", "{\"b\": 1}", "{\"a\": 1, \"b\": 2}", "{\"aa\": 1}"]);
    }
}

#[cfg(test)]
mod skip_tests {
    use super::*;

    #[test]
    fn skip_matches_decode() {
        use crate::types::Datum as D;
        use std::sync::Arc;
        let vals: Vec<(D, SqlType)> = vec![
            (D::Null, SqlType::Int4),
            (D::Bool(true), SqlType::Bool),
            (D::Int2(-3), SqlType::Int2),
            (D::Int4(77), SqlType::Int4),
            (D::Int8(-1 << 40), SqlType::Int8),
            (D::Float4(-1.5), SqlType::Float4),
            (D::Float8(f64::NAN), SqlType::Float8),
            (D::Date(-9), SqlType::Date),
            (D::Timestamp(123456789), SqlType::Timestamp),
            (D::Interval(Interval { months: 1, days: -2, micros: 3 }), SqlType::Interval),
            (D::Uuid([0xFF; 16]), SqlType::Uuid),
            (D::text(""), SqlType::Text),
            (D::text("a\0b\0\0"), SqlType::Text),
            (D::text("\u{ff}\u{fe}x"), SqlType::Varchar),
            (D::Bytea(Arc::from(vec![0u8, 0xFF, 0, 0xFE, 1].into_boxed_slice())), SqlType::Bytea),
            (D::numeric("-123.4500".parse().unwrap()), SqlType::Numeric),
            (D::numeric("0".parse().unwrap()), SqlType::Numeric),
            (D::numeric("1e300".parse().unwrap()), SqlType::Numeric),
            (D::array(ArrayValue::new_1d(SqlType::Int4, vec![D::Int4(1), D::Null])), SqlType::Array(Box::new(SqlType::Int4))),
        ];
        for spec in [SortSpec::ASC, SortSpec::DESC, SortSpec { desc: false, nulls_first: true }] {
            for (d, ty) in &vals {
                let mut b = Vec::new();
                encode_key(d, spec, &mut b);
                let n = b.len();
                encode_key(&D::text("tail"), spec, &mut b);
                assert_eq!(skip_key(&b, ty, spec).unwrap(), n, "{d:?} {spec:?}");
                assert_eq!(decode_key(&b, ty, spec).unwrap().1, n, "{d:?} {spec:?}");
                if !d.is_null() && has_fidelity(ty) && !matches!(ty, SqlType::Array(_)) {
                    let mut f = Vec::new();
                    encode_fidelity(d, &mut f);
                    let n = f.len();
                    f.extend_from_slice(&[9, 9, 9]);
                    assert_eq!(skip_fidelity(&f, ty).unwrap(), n, "{d:?}");
                }
            }
        }
    }
}
