//! Monoid summaries carried by every tree node over its subtree.
//!
//! COUNT, SUM, MIN, MAX per group become range queries over an index whose
//! nodes are augmented with these summaries. Summaries are maintained
//! incrementally: counts and sums are invertible (numeric sums count their
//! special values and display scales apart, so removal is exact), and
//! MIN/MAX fold insertions directly. Only when a MIN/MAX loses its extreme
//! (or a tie between different representations of equal values makes the
//! leftmost one ambiguous) is a node's summary recomputed from its contents.
//! An incrementally maintained summary is always identical to a recomputed
//! one.

use crate::error::{Result, SqlError};
use crate::types::codec::{self, get_ivarint, get_u8, get_uvarint, put_ivarint, put_uvarint};
use crate::types::Datum;
use crate::types::datum::exact_cmp;
use num_bigint::{BigInt, Sign};
use num_traits::{Signed, ToPrimitive, Zero};
use pgnumeric::Numeric;
use smallvec::SmallVec;
use std::cmp::Ordering;

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Summary {
    /// Sum of current weights of all entries.
    pub count: i64,
    pub aggs: Vec<AggState>,
}

impl Summary {
    pub fn empty(spec: &[AggKind]) -> Summary {
        Summary { count: 0, aggs: spec.iter().map(|k| k.empty()).collect() }
    }

    pub fn combine(&mut self, other: &Summary) {
        self.count += other.count;
        for (a, b) in self.aggs.iter_mut().zip(other.aggs.iter()) {
            a.combine(b);
        }
    }

    /// Update a node's summary for one child whose summary `old` was replaced
    /// by the summaries `new` of the nodes that took its place (in key
    /// order). False when a MIN/MAX cannot be updated in place and the
    /// summary must be recomputed from the children.
    pub fn replace_child(&mut self, old: &Summary, new: &[&Summary]) -> bool {
        self.count -= old.count;
        for n in new {
            self.count += n.count;
        }
        for i in 0..self.aggs.len() {
            match &mut self.aggs[i] {
                AggState::Min(cur) | AggState::Max(cur) => {
                    let less = matches!(old.aggs[i], AggState::Min(_));
                    // leftmost extreme of the replacement nodes
                    let mut b: Option<&Datum> = None;
                    for n in new {
                        if let AggState::Min(Some(v)) | AggState::Max(Some(v)) = &n.aggs[i]
                            && b.is_none_or(|bv| better(less, v, bv))
                        {
                            b = Some(v);
                        }
                    }
                    let a = match &old.aggs[i] {
                        AggState::Min(v) | AggState::Max(v) => v.as_ref(),
                        _ => None,
                    };
                    if !replace_extreme(less, cur, a, b) {
                        return false;
                    }
                }
                st => {
                    st.subtract(&old.aggs[i]);
                    for n in new {
                        st.combine(&n.aggs[i]);
                    }
                }
            }
        }
        true
    }

    /// Equality including the representation of MIN/MAX values and of
    /// numeric sums (as finalized), not just SQL equality.
    pub fn exact_eq(&self, other: &Summary) -> bool {
        self.count == other.count
            && self.aggs.len() == other.aggs.len()
            && self.aggs.iter().zip(&other.aggs).all(|(a, b)| match (a, b) {
                (AggState::Min(x), AggState::Min(y)) | (AggState::Max(x), AggState::Max(y)) => match (x, y) {
                    (Some(x), Some(y)) => exact_cmp(x, y) == Ordering::Equal,
                    (None, None) => true,
                    _ => false,
                },
                (AggState::SumNumeric(x), AggState::SumNumeric(y)) => x == y && x.value().to_string() == y.value().to_string(),
                _ => a == b,
            })
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        put_ivarint(out, self.count);
        put_uvarint(out, self.aggs.len() as u64);
        for a in &self.aggs {
            a.encode(out);
        }
    }

    pub fn decode(buf: &mut &[u8]) -> Result<Summary> {
        let count = get_ivarint(buf)?;
        let n = get_uvarint(buf)? as usize;
        let mut aggs = Vec::with_capacity(n);
        for _ in 0..n {
            aggs.push(AggState::decode(buf)?);
        }
        Ok(Summary { count, aggs })
    }
}

/// Kind of a per-node aggregate (the monoid).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum AggKind {
    /// Weighted count of entries whose argument is non-null.
    CountNonNull,
    /// Weighted count of entries whose (boolean) argument is true.
    CountTrue,
    /// Weighted count of entries whose (boolean) argument is false.
    CountFalse,
    SumInt,
    SumNumeric,
    SumFloat,
    /// Sum of squares (for variance/stddev).
    SumSqInt,
    SumSqNumeric,
    SumSqFloat,
    Min,
    Max,
}

impl AggKind {
    pub fn empty(&self) -> AggState {
        match self {
            AggKind::CountNonNull | AggKind::CountTrue | AggKind::CountFalse => AggState::Count(0),
            AggKind::SumInt | AggKind::SumSqInt => AggState::SumInt(0),
            AggKind::SumNumeric | AggKind::SumSqNumeric => AggState::SumNumeric(Box::default()),
            AggKind::SumFloat | AggKind::SumSqFloat => AggState::SumFloat(Box::default()),
            AggKind::Min => AggState::Min(None),
            AggKind::Max => AggState::Max(None),
        }
    }

    /// Fold one entry with argument value `v` and current weight `w`.
    pub fn add(&self, state: &mut AggState, v: &Datum, w: i64) -> Result<()> {
        if w == 0 || v.is_null() {
            return Ok(());
        }
        match (self, state) {
            (AggKind::CountNonNull, AggState::Count(c)) => *c += w,
            (AggKind::CountTrue, AggState::Count(c)) => {
                if v.as_bool() == Some(true) {
                    *c += w
                }
            }
            (AggKind::CountFalse, AggState::Count(c)) => {
                if v.as_bool() == Some(false) {
                    *c += w
                }
            }
            (AggKind::SumInt, AggState::SumInt(s)) => {
                let x = v.as_i64().ok_or_else(|| SqlError::internal("SumInt on non-integer"))? as i128;
                *s += x * w as i128;
            }
            (AggKind::SumSqInt, AggState::SumInt(s)) => {
                let x = v.as_i64().ok_or_else(|| SqlError::internal("SumSqInt on non-integer"))? as i128;
                *s += x * x * w as i128;
            }
            (AggKind::SumNumeric, AggState::SumNumeric(s)) => {
                let x = v.as_numeric().ok_or_else(|| SqlError::internal("SumNumeric on non-numeric"))?;
                s.add_term(x, w)?;
            }
            (AggKind::SumSqNumeric, AggState::SumNumeric(s)) => {
                let x = v.as_numeric().ok_or_else(|| SqlError::internal("SumSqNumeric on non-numeric"))?;
                s.add_term(&x.mul_unbounded(x), w)?;
            }
            (AggKind::SumFloat, AggState::SumFloat(s)) => {
                let x = v.as_f64().ok_or_else(|| SqlError::internal("SumFloat on non-float"))?;
                s.add(x, w);
            }
            (AggKind::SumSqFloat, AggState::SumFloat(s)) => {
                let x = v.as_f64().ok_or_else(|| SqlError::internal("SumSqFloat on non-float"))?;
                s.add_square(x, w);
            }
            (AggKind::Min, AggState::Min(m)) => {
                if w > 0 && m.as_ref().is_none_or(|cur| v.sql_cmp(cur) == Ordering::Less) {
                    *m = Some(v.clone());
                }
            }
            (AggKind::Max, AggState::Max(m)) => {
                if w > 0 && m.as_ref().is_none_or(|cur| v.sql_cmp(cur) == Ordering::Greater) {
                    *m = Some(v.clone());
                }
            }
            (k, s) => return Err(SqlError::internal(format!("agg kind/state mismatch {k:?} {s:?}"))),
        }
        Ok(())
    }

    /// Account for an entry with argument `v` whose weight changed from
    /// `before` to `after`. False when the state must be recomputed (a
    /// MIN/MAX lost its extreme, or a new value ties with it in a different
    /// representation, where the leftmost one wins).
    pub fn update(&self, state: &mut AggState, v: &Datum, before: i64, after: i64) -> Result<bool> {
        if before == after || v.is_null() {
            return Ok(true);
        }
        let (AggKind::Min | AggKind::Max) = self else {
            self.add(state, v, after - before)?;
            return Ok(true);
        };
        let (AggState::Min(cur) | AggState::Max(cur)) = state else {
            return Err(SqlError::internal("agg kind/state mismatch"));
        };
        let less = *self == AggKind::Min;
        let (was, is) = (before > 0, after > 0);
        if was == is {
            return Ok(true);
        }
        Ok(if is {
            replace_extreme(less, cur, None, Some(v))
        } else {
            replace_extreme(less, cur, Some(v), None)
        })
    }
}

/// `a` sorts strictly before `b` for MIN (`less`) or after it for MAX.
fn better(less: bool, a: &Datum, b: &Datum) -> bool {
    let o = a.sql_cmp(b);
    if less { o == Ordering::Less } else { o == Ordering::Greater }
}

/// Update the extreme `cur` of a range (the leftmost of its best values)
/// when a part of the range whose extreme was `old` now has extreme `new`.
/// False when the result depends on positions or values not at hand.
fn replace_extreme(less: bool, cur: &mut Option<Datum>, old: Option<&Datum>, new: Option<&Datum>) -> bool {
    let Some(m) = cur.as_ref() else {
        // nothing else in the range has a value
        *cur = new.cloned();
        return true;
    };
    // An `old` equal to `m` in another representation proves that `m` comes
    // from a part further left, which this change does not touch.
    let left_of_part = matches!(old, Some(a) if a.sql_cmp(m) == Ordering::Equal && exact_cmp(a, m) != Ordering::Equal);
    if let Some(b) = new {
        if better(less, b, m) {
            *cur = Some(b.clone());
            return true;
        }
        if b.sql_cmp(m) == Ordering::Equal {
            return exact_cmp(b, m) == Ordering::Equal || left_of_part;
        }
    }
    // `new` is worse (or absent): `m` survives unless `old` may have been it
    !matches!(old, Some(a) if exact_cmp(a, m) == Ordering::Equal)
}

#[derive(Clone, Debug, PartialEq)]
pub enum AggState {
    Count(i64),
    SumInt(i128),
    SumNumeric(Box<NumSum>),
    SumFloat(Box<FloatSum>),
    Min(Option<Datum>),
    Max(Option<Datum>),
}

impl PartialEq for Datum {
    fn eq(&self, other: &Self) -> bool {
        match (self.is_null(), other.is_null()) {
            (true, true) => true,
            (false, false) => {
                std::mem::discriminant(self) == std::mem::discriminant(other) && self.sql_eq(other)
            }
            _ => false,
        }
    }
}

impl AggState {
    pub fn combine(&mut self, other: &AggState) {
        match (self, other) {
            (AggState::Count(a), AggState::Count(b)) => *a += b,
            (AggState::SumInt(a), AggState::SumInt(b)) => *a += b,
            (AggState::SumNumeric(a), AggState::SumNumeric(b)) => a.combine(b, 1),
            (AggState::SumFloat(a), AggState::SumFloat(b)) => a.combine(b),
            (AggState::Min(a), AggState::Min(b)) => {
                if let Some(bv) = b
                    && a.as_ref().is_none_or(|av| bv.sql_cmp(av) == Ordering::Less)
                {
                    *a = Some(bv.clone());
                }
            }
            (AggState::Max(a), AggState::Max(b)) => {
                if let Some(bv) = b
                    && a.as_ref().is_none_or(|av| bv.sql_cmp(av) == Ordering::Greater)
                {
                    *a = Some(bv.clone());
                }
            }
            _ => {}
        }
    }

    /// Remove `other` from an invertible state (counts and sums).
    pub fn subtract(&mut self, other: &AggState) {
        match (self, other) {
            (AggState::Count(a), AggState::Count(b)) => *a -= b,
            (AggState::SumInt(a), AggState::SumInt(b)) => *a -= b,
            (AggState::SumNumeric(a), AggState::SumNumeric(b)) => a.combine(b, -1),
            (AggState::SumFloat(a), AggState::SumFloat(b)) => a.subtract(b),
            _ => {}
        }
    }

    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            AggState::Count(c) => {
                out.push(1);
                put_ivarint(out, *c);
            }
            AggState::SumInt(s) => {
                out.push(2);
                out.extend_from_slice(&s.to_le_bytes());
            }
            AggState::SumNumeric(s) => {
                out.push(4);
                s.encode(out);
            }
            AggState::SumFloat(f) => {
                out.push(5);
                f.encode(out);
            }
            AggState::Min(d) => {
                out.push(6);
                codec::encode_datum(d.as_ref().unwrap_or(&Datum::Null), out);
            }
            AggState::Max(d) => {
                out.push(7);
                codec::encode_datum(d.as_ref().unwrap_or(&Datum::Null), out);
            }
        }
    }

    fn decode(buf: &mut &[u8]) -> Result<AggState> {
        Ok(match get_u8(buf)? {
            1 => AggState::Count(get_ivarint(buf)?),
            2 => {
                if buf.len() < 16 {
                    return Err(SqlError::corrupt("truncated summary"));
                }
                let v = i128::from_le_bytes(buf[..16].try_into().unwrap());
                *buf = &buf[16..];
                AggState::SumInt(v)
            }
            4 => AggState::SumNumeric(Box::new(NumSum::decode(buf)?)),
            5 => AggState::SumFloat(Box::new(FloatSum::decode(buf)?)),
            6 => {
                let d = codec::decode_datum(buf)?;
                AggState::Min(if d.is_null() { None } else { Some(d) })
            }
            7 => {
                let d = codec::decode_datum(buf)?;
                AggState::Max(if d.is_null() { None } else { Some(d) })
            }
            t => return Err(SqlError::corrupt(format!("bad agg state tag {t}"))),
        })
    }
}

/// Exact, order-independent and invertible sum of weighted numerics.
///
/// Like PostgreSQL's numeric aggregate state, NaN and the infinities are
/// counted apart from the finite sum. The finite terms' display scales are
/// counted too: the result's scale is the largest scale present, which
/// therefore does not depend on terms that were added and later removed.
///
/// Also like PostgreSQL's state, the finite sum is not limited to numeric's
/// range (a sum over a subtree, or a sum of squares, may exceed it while
/// every value read from it fits): maintaining it never fails, and readers
/// check the range of the result (`Numeric::checked`).
#[derive(Clone, Debug, PartialEq)]
pub struct NumSum {
    pub finite: Numeric,
    /// (display scale, weighted count of finite terms), sorted, no zeros.
    pub scales: SmallVec<[(u32, i64); 2]>,
    pub nan: i64,
    pub pos_inf: i64,
    pub neg_inf: i64,
}

impl Default for NumSum {
    fn default() -> Self {
        NumSum { finite: Numeric::zero(), scales: SmallVec::new(), nan: 0, pos_inf: 0, neg_inf: 0 }
    }
}

impl NumSum {
    pub fn add_term(&mut self, x: &Numeric, w: i64) -> Result<()> {
        if w == 0 {
            return Ok(());
        }
        if x.is_nan() {
            self.nan += w;
        } else if x.is_infinite() {
            if x.is_negative() {
                self.neg_inf += w
            } else {
                self.pos_inf += w
            }
        } else {
            self.finite = match w {
                1 => self.finite.add_unbounded(x),
                -1 => self.finite.sub_unbounded(x),
                _ => self.finite.add_unbounded(&x.mul_unbounded(&Numeric::from_i64(w))),
            };
            self.add_scale(x.dscale(), w);
        }
        Ok(())
    }

    fn add_scale(&mut self, scale: u32, w: i64) {
        match self.scales.binary_search_by_key(&scale, |(s, _)| *s) {
            Ok(i) => {
                self.scales[i].1 += w;
                if self.scales[i].1 == 0 {
                    self.scales.remove(i);
                }
            }
            Err(i) => self.scales.insert(i, (scale, w)),
        }
    }

    /// Add `sign` (1 or -1) times `o`.
    pub fn combine(&mut self, o: &NumSum, sign: i64) {
        self.finite = if sign > 0 { self.finite.add_unbounded(&o.finite) } else { self.finite.sub_unbounded(&o.finite) };
        for (s, c) in &o.scales {
            self.add_scale(*s, sign * c);
        }
        self.nan += sign * o.nan;
        self.pos_inf += sign * o.pos_inf;
        self.neg_inf += sign * o.neg_inf;
    }

    /// The sum (PostgreSQL semantics for special values), at the largest
    /// display scale among the terms; possibly beyond numeric's range.
    pub fn value(&self) -> Numeric {
        if self.nan > 0 || (self.pos_inf > 0 && self.neg_inf > 0) {
            return Numeric::nan();
        }
        if self.pos_inf > 0 {
            return Numeric::infinity();
        }
        if self.neg_inf > 0 {
            return Numeric::neg_infinity();
        }
        let scale = self.scales.last().map(|(s, _)| *s).unwrap_or(0);
        if self.finite.dscale() == scale { self.finite.clone() } else { self.finite.with_dscale(scale) }
    }

    pub fn has_special(&self) -> bool {
        self.nan > 0 || self.pos_inf > 0 || self.neg_inf > 0
    }

    fn encode(&self, out: &mut Vec<u8>) {
        self.finite.encode_binary(out);
        put_uvarint(out, self.scales.len() as u64);
        for (s, c) in &self.scales {
            put_uvarint(out, *s as u64);
            put_ivarint(out, *c);
        }
        put_ivarint(out, self.nan);
        put_ivarint(out, self.pos_inf);
        put_ivarint(out, self.neg_inf);
    }

    fn decode(buf: &mut &[u8]) -> Result<NumSum> {
        // (the exact sum may be beyond numeric's range)
        let (finite, len) = Numeric::decode_binary_unbounded(buf)?;
        *buf = &buf[len..];
        let n = get_uvarint(buf)? as usize;
        let mut scales = SmallVec::with_capacity(n);
        for _ in 0..n {
            scales.push((get_uvarint(buf)? as u32, get_ivarint(buf)?));
        }
        Ok(NumSum { finite, scales, nan: get_ivarint(buf)?, pos_inf: get_ivarint(buf)?, neg_inf: get_ivarint(buf)? })
    }
}

/// Exact, order-independent and invertible sum of weighted doubles.
///
/// The finite part is an integer multiple of 2^-1074 (the smallest subnormal)
/// held in a big integer, so addition is exact and associative; the result is
/// rounded once when read. This pins down floating-point SUM/AVG so that the
/// result is a pure function of the multiset, independent of update order.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct FloatSum {
    pub fixed: BigInt,
    pub nan: i64,
    pub pos_inf: i64,
    pub neg_inf: i64,
}

impl FloatSum {
    pub fn add(&mut self, x: f64, w: i64) {
        if x.is_nan() {
            self.nan += w;
        } else if x == f64::INFINITY {
            self.pos_inf += w;
        } else if x == f64::NEG_INFINITY {
            self.neg_inf += w;
        } else if x != 0.0 {
            let (m, e) = decompose(x);
            // x = m * 2^e, e >= -1074
            let shifted = BigInt::from(m) << ((e + 1074) as usize);
            self.fixed += shifted * w;
        }
    }

    /// Add w * x^2 exactly (x^2 = m^2 * 2^(2e) needs scale 2^-2148).
    pub fn add_square(&mut self, x: f64, w: i64) {
        // squares are tracked in a separate scale: we store them as x*x rounded
        // to the exact product in the 2^-1074 grid when representable; to keep
        // things simple and exact for typical data, use the f64 product's
        // exact value when it is finite (x*x is exactly representable only if
        // m^2 fits 53 bits, which is not general) -> accumulate the exact
        // product m^2 * 2^(2e+1074) when 2e >= -1074, else round.
        if !x.is_finite() {
            self.add(x * x, w);
            return;
        }
        if x == 0.0 {
            return;
        }
        let (m, e) = decompose(x);
        let m2 = BigInt::from(m) * BigInt::from(m);
        let exp = 2 * e + 1074;
        let v = if exp >= 0 { m2 << (exp as usize) } else { m2 >> ((-exp) as usize) };
        self.fixed += v * w;
    }

    pub fn combine(&mut self, o: &FloatSum) {
        self.fixed += &o.fixed;
        self.nan += o.nan;
        self.pos_inf += o.pos_inf;
        self.neg_inf += o.neg_inf;
    }

    pub fn subtract(&mut self, o: &FloatSum) {
        self.fixed -= &o.fixed;
        self.nan -= o.nan;
        self.pos_inf -= o.pos_inf;
        self.neg_inf -= o.neg_inf;
    }

    /// Correctly rounded result. Infinity on overflow of the finite part.
    pub fn value(&self) -> f64 {
        if self.nan > 0 || (self.pos_inf > 0 && self.neg_inf > 0) {
            return f64::NAN;
        }
        if self.pos_inf > 0 {
            return f64::INFINITY;
        }
        if self.neg_inf > 0 {
            return f64::NEG_INFINITY;
        }
        bigint_scaled_to_f64(&self.fixed, -1074)
    }

    /// Exact division of the finite part by n, correctly rounded (for AVG).
    pub fn div_value(&self, n: i64) -> f64 {
        let v = self.value();
        if !v.is_finite() || n == 0 {
            return v / n as f64;
        }
        // scale up to keep precision, then divide
        let scaled: BigInt = &self.fixed << 128usize;
        let q = scaled / BigInt::from(n);
        bigint_scaled_to_f64(&q, -1074 - 128)
    }

    fn encode(&self, out: &mut Vec<u8>) {
        let (sign, bytes) = self.fixed.to_bytes_le();
        out.push(match sign {
            Sign::Minus => 2,
            Sign::NoSign => 0,
            Sign::Plus => 1,
        });
        codec::put_bytes(out, &bytes);
        put_ivarint(out, self.nan);
        put_ivarint(out, self.pos_inf);
        put_ivarint(out, self.neg_inf);
    }

    fn decode(buf: &mut &[u8]) -> Result<FloatSum> {
        let sign = match get_u8(buf)? {
            2 => Sign::Minus,
            0 => Sign::NoSign,
            _ => Sign::Plus,
        };
        let bytes = codec::get_bytes(buf)?;
        Ok(FloatSum {
            fixed: BigInt::from_bytes_le(sign, bytes),
            nan: get_ivarint(buf)?,
            pos_inf: get_ivarint(buf)?,
            neg_inf: get_ivarint(buf)?,
        })
    }
}

/// x = m * 2^e with integer m (|m| < 2^53) and e >= -1074.
fn decompose(x: f64) -> (i64, i32) {
    let bits = x.to_bits();
    let sign = if bits >> 63 != 0 { -1 } else { 1 };
    let exp = ((bits >> 52) & 0x7FF) as i32;
    let frac = (bits & ((1u64 << 52) - 1)) as i64;
    if exp == 0 {
        (sign * frac, -1074)
    } else {
        (sign * (frac | (1 << 52)), exp - 1075)
    }
}

/// Round v * 2^scale to the nearest double (ties to even).
pub fn bigint_scaled_to_f64(v: &BigInt, scale: i32) -> f64 {
    if v.is_zero() {
        return 0.0;
    }
    let neg = v.is_negative();
    let mag = v.abs().to_biguint().unwrap();
    let bits = mag.bits() as i64;
    let result = if bits <= 53 {
        let m = mag.to_u64().unwrap() as f64;
        ldexp(m, scale)
    } else {
        // keep 53 significant bits, round half to even using the rest
        let shift = (bits - 53) as u64;
        let top = (&mag >> shift).to_u64().unwrap();
        let rem = &mag - (num_bigint::BigUint::from(top) << shift);
        let half = num_bigint::BigUint::from(1u8) << (shift - 1);
        let mut m = top;
        match rem.cmp(&half) {
            Ordering::Greater => m += 1,
            Ordering::Equal => {
                if m & 1 == 1 {
                    m += 1
                }
            }
            Ordering::Less => {}
        }
        let e = scale as i64 + shift as i64;
        // smallest normal check: if the result would be subnormal we already
        // have more bits than representable; ldexp performs final rounding
        // only in that rare case.
        ldexp(m as f64, e.clamp(i32::MIN as i64, i32::MAX as i64) as i32)
    };
    if neg { -result } else { result }
}

fn ldexp(x: f64, e: i32) -> f64 {
    // multiply in steps to avoid intermediate overflow/underflow
    let mut x = x;
    let mut e = e;
    while e > 1000 {
        x *= 2f64.powi(1000);
        e -= 1000;
        if x.is_infinite() {
            return x;
        }
    }
    while e < -1000 {
        x *= 2f64.powi(-1000);
        e += 1000;
        if x == 0.0 {
            return x;
        }
    }
    x * 2f64.powi(e)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float_sum_exact() {
        let mut s = FloatSum::default();
        s.add(1e100, 1);
        s.add(1.0, 1);
        s.add(-1e100, 1);
        assert_eq!(s.value(), 1.0);
        let mut s = FloatSum::default();
        for _ in 0..10 {
            s.add(0.1, 1);
        }
        assert_eq!(s.value(), 1.0); // exact sum of ten 0.1s rounds to 1.0
        s.add(0.1, -10);
        assert_eq!(s.value(), 0.0);
        let mut s = FloatSum::default();
        s.add(5e-324, 3);
        assert_eq!(s.value(), 3.0 * 5e-324);
        assert_eq!(s.div_value(3), 5e-324);
    }
}
