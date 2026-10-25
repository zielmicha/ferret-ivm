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

fn interval_from_span(span: i128) -> Interval {
    // interval_cmp_value = ((months*30 + days) * USECS_PER_DAY) + micros
    const USECS_PER_DAY: i128 = 86_400_000_000;
    let days = span.div_euclid(USECS_PER_DAY);
    let micros = span.rem_euclid(USECS_PER_DAY);
    let months = days / 30;
    let days = days % 30;
    Interval { months: months.clamp(i32::MIN as i128, i32::MAX as i128) as i32, days: days as i32, micros: micros as i64 }
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

/// Skip one encoded key column without decoding it.
pub fn skip_key(buf: &[u8], ty: &SqlType, spec: SortSpec) -> Result<usize> {
    // Decoding is cheap enough for all fixed types; variable types need a scan.
    Ok(decode_key(buf, ty, spec)?.1)
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
mod tests {
    use super::*;

    fn enc(d: &Datum, spec: SortSpec) -> Vec<u8> {
        let mut v = Vec::new();
        encode_key(d, spec, &mut v);
        v
    }

    #[test]
    fn int_order_and_roundtrip() {
        let vals = [i64::MIN, -5, -1, 0, 1, 7, i64::MAX];
        for spec in [SortSpec::ASC, SortSpec::DESC] {
            let encs: Vec<_> = vals.iter().map(|v| enc(&Datum::Int8(*v), spec)).collect();
            for w in encs.windows(2) {
                if spec.desc {
                    assert!(w[0] > w[1]);
                } else {
                    assert!(w[0] < w[1]);
                }
            }
            for (v, e) in vals.iter().zip(&encs) {
                let (d, n) = decode_key(e, &SqlType::Int8, spec).unwrap();
                assert_eq!(n, e.len());
                assert_eq!(d.as_i64(), Some(*v));
            }
        }
    }

    #[test]
    fn text_prefix_free() {
        let vals = ["", "\0", "a", "a\0", "a\0b", "ab", "b"];
        for spec in [SortSpec::ASC, SortSpec::DESC] {
            let encs: Vec<_> = vals.iter().map(|v| enc(&Datum::text(v), spec)).collect();
            for w in encs.windows(2) {
                if spec.desc {
                    assert!(w[0] > w[1], "{w:?}");
                } else {
                    assert!(w[0] < w[1], "{w:?}");
                }
            }
            for (v, e) in vals.iter().zip(&encs) {
                let (d, n) = decode_key(e, &SqlType::Text, spec).unwrap();
                assert_eq!(n, e.len());
                assert_eq!(d.as_str(), Some(*v));
            }
        }
    }

    #[test]
    fn null_placement() {
        let n_last = enc(&Datum::Null, SortSpec::ASC);
        let v = enc(&Datum::Int4(i32::MAX), SortSpec::ASC);
        assert!(v < n_last);
        let n_first = enc(&Datum::Null, SortSpec::DESC);
        let v = enc(&Datum::Int4(i32::MIN), SortSpec::DESC);
        assert!(n_first < v);
    }

    #[test]
    fn float_order() {
        let vals = [f64::NEG_INFINITY, -1.5, -0.0, 0.0, 1e-300, 2.0, f64::INFINITY, f64::NAN];
        let encs: Vec<_> = vals.iter().map(|v| enc(&Datum::Float8(*v), SortSpec::ASC)).collect();
        for w in encs.windows(2) {
            assert!(w[0] <= w[1]);
        }
        assert_eq!(encs[2], encs[3]); // -0 == 0 canonically
        let mut f = Vec::new();
        encode_fidelity(&Datum::Float8(-0.0), &mut f);
        let (mut d, _) = decode_key(&encs[2], &SqlType::Float8, SortSpec::ASC).unwrap();
        apply_fidelity(&mut d, &f).unwrap();
        assert!(d.as_f64().unwrap().is_sign_negative());
    }

    #[test]
    fn successor() {
        assert_eq!(prefix_successor(&[1, 2]), Some(vec![1, 3]));
        assert_eq!(prefix_successor(&[1, 0xFF]), Some(vec![2]));
        assert_eq!(prefix_successor(&[0xFF]), None);
    }
}
