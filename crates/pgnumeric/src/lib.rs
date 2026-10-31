//! pgnumeric: PostgreSQL 17 compatible arbitrary precision `numeric` type and
//! float4/float8 text conversions.
//!
//! [`Numeric`] reproduces PostgreSQL's `numeric` semantics exactly: values
//! are NaN, +/-Infinity or finite decimals with a display scale (`dscale`);
//! result scales of every operation (including division's
//! `select_div_scale` and the precision rules of `sqrt`/`exp`/`ln`/`log`/
//! `power`), rounding (half away from zero), text input/output, error
//! messages, SQLSTATEs and details all follow PostgreSQL 17. The arithmetic
//! is a faithful port of `numeric.c` (base-10000 digits, `mul_var` guard
//! digits, Knuth and "FM" division, Karatsuba square root, Taylor series for
//! `exp`/`ln`), so even the deliberately approximate transcendental results
//! match PostgreSQL digit for digit.
//!
//! The [`float`] module implements `float8out`/`float4out` (PostgreSQL's
//! modified Ryu shortest-decimal output with its notation rules) and
//! `float8in`/`float4in` (glibc `strtod`/`strtof` semantics as seen through
//! PostgreSQL, including hex floats, `nan(...)`, and the exact
//! out-of-range/syntax error texts).
//!
//! [`to_char_numeric`], [`to_char_int8`], [`to_char_int4`],
//! [`to_char_int2`], [`to_char_float8`], [`to_char_float4`] and
//! [`to_number`] port the number formatting of PostgreSQL's
//! `formatting.c` (`to_char(numeric|bigint|integer|smallint|double
//! precision|real, text)` and `to_number(text, text)`, C locale), quirks
//! included.
//!
//! Besides the SQL functions, [`Numeric`] provides storage encodings: an
//! order-preserving, scale-insensitive, self-delimiting key encoding
//! ([`Numeric::encode_key`]), a compact exact value encoding preserving the
//! display scale ([`Numeric::encode_binary`]), and PostgreSQL's binary wire
//! format ([`Numeric::to_pg_binary`] / [`Numeric::from_pg_binary`]).
//!
//! Errors are reported as [`Error`] with PostgreSQL's SQLSTATE, message and
//! detail, e.g. `22012 division by zero`, `22003 value overflows numeric
//! format`, `22P02 invalid input syntax for type numeric: "x"`.
//!
//! Known, documented deviations (all outside PostgreSQL's API surface):
//! [`Numeric::ceil`], [`Numeric::floor`] and [`Numeric::with_dscale`] cannot
//! return errors, so in the pathological case where the result exceeds the
//! numeric range (only possible for |x| >= 10^131071) they saturate to
//! +/-Infinity; use [`Numeric::try_ceil`] / [`Numeric::try_floor`] for
//! PostgreSQL's exact error.
//!
//! Large parts of this crate are ports of PostgreSQL's `numeric.c`, `float.c`,
//! `formatting.c` and its Ryu implementation (`d2s.c`, `f2s.c`). PostgreSQL
//! is released under the PostgreSQL License: Portions Copyright (c)
//! 1996-2024, PostgreSQL Global Development Group; Portions Copyright (c)
//! 1994, The Regents of the University of California. Ryu is Copyright 2018
//! Ulf Adams (Boost/Apache-2.0). The test suite's golden data
//! (`tests/golden`) was generated from a real PostgreSQL 17 server by
//! `tests/gen_golden.py` and `tests/gen_tochar.py`.

#![allow(clippy::approx_constant)] // PG's truncated float literals are intentional

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

mod encoding;
pub mod float;
mod formatting;
mod ryu;
mod ryu_tables;
mod var;

#[cfg(test)]
mod tests;

use var::*;

pub use formatting::{
    to_char_float4, to_char_float8, to_char_int2, to_char_int4, to_char_int8, to_char_numeric,
    to_number,
};

/// Error with PostgreSQL SQLSTATE, message and optional detail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub sqlstate: &'static str,
    pub message: String,
    pub detail: Option<String>,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

/// PostgreSQL `numeric` value: NaN, +/-Infinity, or a finite decimal number
/// with a display scale (`dscale`).
///
/// Equality, ordering and hashing are by numeric value (`1.0 == 1.00`),
/// following `numeric_cmp`: `-Infinity < finite < +Infinity < NaN` and
/// `NaN == NaN`.
#[derive(Clone)]
pub struct Numeric {
    /// NUMERIC_POS / NUMERIC_NEG / NUMERIC_NAN / NUMERIC_PINF / NUMERIC_NINF
    pub(crate) sign: u16,
    /// weight of the first base-10000 digit
    pub(crate) weight: i32,
    /// display scale
    pub(crate) dscale: i32,
    /// base-10000 digits (stripped of leading/trailing zeros in public values)
    pub(crate) digits: Digits,
}

impl PartialEq for Numeric {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Numeric {}

impl PartialOrd for Numeric {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Numeric {
    /// cmp_numerics
    fn cmp(&self, other: &Self) -> Ordering {
        let r = if self.is_special() {
            match self.sign {
                NUMERIC_NAN => {
                    if other.sign == NUMERIC_NAN {
                        0
                    } else {
                        1
                    }
                }
                NUMERIC_PINF => {
                    if other.sign == NUMERIC_NAN {
                        -1
                    } else if other.sign == NUMERIC_PINF {
                        0
                    } else {
                        1
                    }
                }
                _ => {
                    if other.sign == NUMERIC_NINF {
                        0
                    } else {
                        -1
                    }
                }
            }
        } else if other.is_special() {
            if other.sign == NUMERIC_NINF { 1 } else { -1 }
        } else {
            cmp_var(self, other)
        };
        r.cmp(&0)
    }
}

impl Hash for Numeric {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Public values are always normalized (stripped digits, zero has
        // weight 0 and positive sign), so hashing the value fields (but not
        // dscale) is consistent with Eq.
        state.write_u16(self.sign);
        if !self.is_special() && !self.digits.is_empty() {
            state.write_i32(self.weight);
            for &d in self.digits.iter() {
                state.write_i16(d);
            }
        }
    }
}

impl fmt::Debug for Numeric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Numeric({})", self)
    }
}

impl fmt::Display for Numeric {
    /// numeric_out
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.sign {
            NUMERIC_NAN => f.write_str("NaN"),
            NUMERIC_PINF => f.write_str("Infinity"),
            NUMERIC_NINF => f.write_str("-Infinity"),
            _ => {
                let mut s = String::new();
                write_str_from_var(self, &mut s);
                f.write_str(&s)
            }
        }
    }
}

impl std::str::FromStr for Numeric {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        Numeric::parse(s)
    }
}

impl Default for Numeric {
    fn default() -> Self {
        Numeric::zero()
    }
}

impl From<i64> for Numeric {
    fn from(v: i64) -> Self {
        Numeric::from_i64(v)
    }
}

impl From<i32> for Numeric {
    fn from(v: i32) -> Self {
        Numeric::from_i64(v as i64)
    }
}

impl From<i128> for Numeric {
    fn from(v: i128) -> Self {
        Numeric::from_i128(v)
    }
}

/// Format `v` like C's `printf("%.*g", prec, v)` but return the pieces needed
/// to feed `set_var_from_str`: a string `[-]d[.ddd]e<exp>` with trailing
/// fractional zeros removed (as %g does).
fn format_g_for_numeric(v: f64, prec: usize) -> String {
    // Rust's exact formatting rounds half to even on exact decimal ties,
    // like glibc's printf in the default rounding mode.
    let s = format!("{:.*e}", prec - 1, v);
    let (mant, exp) = s.split_at(s.find('e').expect("exponent"));
    let mant = if mant.contains('.') {
        mant.trim_end_matches('0').trim_end_matches('.')
    } else {
        mant
    };
    format!("{}{}", mant, exp)
}

fn numeric_field_overflow(detail: String) -> Error {
    Error {
        sqlstate: "22003",
        message: "numeric field overflow".to_string(),
        detail: Some(detail),
    }
}

impl Numeric {
    // ------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------

    /// Zero with dscale 0.
    pub fn zero() -> Self {
        var_zero(0)
    }

    pub fn nan() -> Self {
        var_special(NUMERIC_NAN)
    }

    pub fn infinity() -> Self {
        var_special(NUMERIC_PINF)
    }

    pub fn neg_infinity() -> Self {
        var_special(NUMERIC_NINF)
    }

    /// int8 -> numeric (dscale 0).
    pub fn from_i64(v: i64) -> Self {
        Self::from_i128(v as i128)
    }

    /// int128 -> numeric (dscale 0).
    pub fn from_i128(v: i128) -> Self {
        // int128_to_var mirrors PG's int128_to_numericvar, which may leave
        // trailing zero digits; public values are always stripped.
        let mut r = int128_to_var(v);
        strip_var(&mut r);
        r
    }

    /// float8 -> numeric cast (`float8_numeric`): formats with `%.15g` and
    /// parses that; NaN -> NaN, +/-inf -> +/-Infinity.
    pub fn from_f64(v: f64) -> Result<Self> {
        Self::from_float(v, 15)
    }

    /// float4 -> numeric cast (`float4_numeric`): formats with `%.6g`.
    pub fn from_f32(v: f32) -> Result<Self> {
        Self::from_float(v as f64, 6)
    }

    fn from_float(v: f64, precision: usize) -> Result<Self> {
        if v.is_nan() {
            return Ok(Self::nan());
        }
        if v.is_infinite() {
            return Ok(if v < 0.0 {
                Self::neg_infinity()
            } else {
                Self::infinity()
            });
        }
        let s = format_g_for_numeric(v, precision);
        let (var, _) = set_var_from_str(&s, s.as_bytes(), 0)?;
        make_result(var)
    }

    /// `numeric_in` with typmod -1 (this is also what SQL casts such as
    /// `'1.5'::numeric(5,2)` use, followed by [`Numeric::apply_typmod`]).
    pub fn parse(s: &str) -> Result<Self> {
        match parse_var(s)? {
            Parsed::Special(v) => Ok(v),
            Parsed::Finite(v) => make_result(v),
        }
    }

    /// `numeric_in` with a typmod `numeric(precision, scale)`, as used by
    /// COPY, record/array input and logical replication apply: the typmod is
    /// applied before the value is checked against the storage format.
    pub fn parse_with_typmod(s: &str, precision: i32, scale: i32) -> Result<Self> {
        check_typmod(precision, scale)?;
        match parse_var(s)? {
            Parsed::Special(v) => apply_typmod_special(v, precision, scale),
            Parsed::Finite(mut v) => {
                apply_typmod_var(&mut v, precision, scale)?;
                make_result(v)
            }
        }
    }

    // ------------------------------------------------------------------
    // Inspection
    // ------------------------------------------------------------------

    pub fn is_nan(&self) -> bool {
        self.sign == NUMERIC_NAN
    }

    pub fn is_infinite(&self) -> bool {
        self.sign == NUMERIC_PINF || self.sign == NUMERIC_NINF
    }

    pub fn is_finite(&self) -> bool {
        !self.is_special()
    }

    pub fn is_zero(&self) -> bool {
        !self.is_special() && self.digits.is_empty()
    }

    /// Negative finite value or -Infinity.
    pub fn is_negative(&self) -> bool {
        self.sign == NUMERIC_NINF || (self.sign == NUMERIC_NEG && !self.digits.is_empty())
    }

    /// Display scale (0 for NaN/Infinity).
    pub fn dscale(&self) -> u32 {
        if self.is_special() {
            0
        } else {
            self.dscale as u32
        }
    }

    /// Returns the same value with the given display scale: pads with zeros,
    /// or rounds half away from zero when the scale is reduced. Special
    /// values are returned unchanged. `dscale` is clamped to 16383 (the
    /// maximum storable display scale). In the pathological case where
    /// rounding overflows the numeric range (only possible for values
    /// >= 10^131071) the result saturates to +/-Infinity.
    pub fn with_dscale(&self, dscale: u32) -> Self {
        if self.is_special() {
            return self.clone();
        }
        let ds = dscale.min(NUMERIC_DSCALE_MAX as u32) as i32;
        let mut v = self.clone();
        round_var(&mut v, ds);
        let neg = self.sign == NUMERIC_NEG;
        make_result(v).unwrap_or_else(|_| {
            if neg {
                Self::neg_infinity()
            } else {
                Self::infinity()
            }
        })
    }

    /// Same value with the minimal display scale (`trim_scale`).
    pub fn normalize(&self) -> Self {
        if self.is_special() {
            return self.clone();
        }
        let mut v = self.clone();
        v.dscale = get_min_scale(self);
        v
    }

    /// SQL `scale()`: the display scale; None for NaN/Infinity.
    pub fn scale(&self) -> Option<i32> {
        if self.is_special() {
            None
        } else {
            Some(self.dscale)
        }
    }

    /// SQL `min_scale()`; None for NaN/Infinity.
    pub fn min_scale(&self) -> Option<i32> {
        if self.is_special() {
            None
        } else {
            Some(get_min_scale(self))
        }
    }

    // ------------------------------------------------------------------
    // Casts to other types
    // ------------------------------------------------------------------

    /// numeric -> int8 cast (rounds half away from zero).
    pub fn to_i64(&self) -> Result<i64> {
        self.to_int("bigint", i64::MIN, i64::MAX)
    }

    /// numeric -> int4 cast.
    pub fn to_i32(&self) -> Result<i32> {
        self.to_int("integer", i32::MIN as i64, i32::MAX as i64)
            .map(|v| v as i32)
    }

    /// numeric -> int2 cast.
    pub fn to_i16(&self) -> Result<i16> {
        self.to_int("smallint", i16::MIN as i64, i16::MAX as i64)
            .map(|v| v as i16)
    }

    fn to_int(&self, type_name: &str, lo: i64, hi: i64) -> Result<i64> {
        match self.sign {
            NUMERIC_NAN => {
                return Err(err("0A000", format!("cannot convert NaN to {}", type_name)));
            }
            NUMERIC_PINF | NUMERIC_NINF => {
                return Err(err(
                    "0A000",
                    format!("cannot convert infinity to {}", type_name),
                ));
            }
            _ => {}
        }
        match var_to_int64(self) {
            Some(v) if v >= lo && v <= hi => Ok(v),
            _ => Err(err("22003", format!("{} out of range", type_name))),
        }
    }

    /// Some(v) only if the value is finite, integral and fits in i128.
    pub fn to_i128_exact(&self) -> Option<i128> {
        if self.is_special() {
            return None;
        }
        if self.digits.is_empty() {
            return Some(0);
        }
        if self.nd() > self.weight + 1 {
            return None; // has a fractional part
        }
        let neg = self.sign == NUMERIC_NEG;
        // accumulate negatively to allow i128::MIN
        let mut val: i128 = -(self.dig(0) as i128);
        for i in 1..=self.weight {
            val = val.checked_mul(NBASE as i128)?;
            if i < self.nd() {
                val = val.checked_sub(self.dig(i) as i128)?;
            }
        }
        if !neg {
            val = val.checked_neg()?;
        }
        Some(val)
    }

    /// numeric -> float8 cast (`numeric_float8`: numeric_out + float8in).
    pub fn to_f64(&self) -> Result<f64> {
        match self.sign {
            NUMERIC_NAN => Ok(f64::NAN),
            NUMERIC_PINF => Ok(f64::INFINITY),
            NUMERIC_NINF => Ok(f64::NEG_INFINITY),
            _ => float::parse_f64(&get_str_from_var(self)),
        }
    }

    /// numeric -> float4 cast (`numeric_float4`: numeric_out + float4in).
    pub fn to_f32(&self) -> Result<f32> {
        match self.sign {
            NUMERIC_NAN => Ok(f32::NAN),
            NUMERIC_PINF => Ok(f32::INFINITY),
            NUMERIC_NINF => Ok(f32::NEG_INFINITY),
            _ => float::parse_f32(&get_str_from_var(self)),
        }
    }

    // ------------------------------------------------------------------
    // Arithmetic
    // ------------------------------------------------------------------

    /// numeric_add
    pub fn add(&self, o: &Self) -> Result<Self> {
        make_result(self.add_unbounded(o))
    }

    /// numeric_sub
    pub fn sub(&self, o: &Self) -> Result<Self> {
        make_result(self.sub_unbounded(o))
    }

    /// numeric_mul
    pub fn mul(&self, o: &Self) -> Result<Self> {
        make_result(self.mul_unbounded(o))
    }

    /// `self + o`, exact, without the storage format's range check. For
    /// accumulators (PostgreSQL's aggregate transition states use unbounded
    /// `NumericVar`s): an intermediate sum may leave the range that
    /// [`Numeric::checked`] enforces when the result is read. Special values
    /// as in [`Numeric::add`].
    pub fn add_unbounded(&self, o: &Self) -> Self {
        if self.is_special() || o.is_special() {
            if self.is_nan() || o.is_nan() {
                return Self::nan();
            }
            if self.sign == NUMERIC_PINF {
                return if o.sign == NUMERIC_NINF {
                    Self::nan()
                } else {
                    Self::infinity()
                };
            }
            if self.sign == NUMERIC_NINF {
                return if o.sign == NUMERIC_PINF {
                    Self::nan()
                } else {
                    Self::neg_infinity()
                };
            }
            return if o.sign == NUMERIC_PINF {
                Self::infinity()
            } else {
                Self::neg_infinity()
            };
        }
        let mut result = add_var(self, o);
        strip_var(&mut result);
        result
    }

    /// `self - o`, as [`Numeric::add_unbounded`].
    pub fn sub_unbounded(&self, o: &Self) -> Self {
        if self.is_special() || o.is_special() {
            if self.is_nan() || o.is_nan() {
                return Self::nan();
            }
            if self.sign == NUMERIC_PINF {
                return if o.sign == NUMERIC_PINF {
                    Self::nan()
                } else {
                    Self::infinity()
                };
            }
            if self.sign == NUMERIC_NINF {
                return if o.sign == NUMERIC_NINF {
                    Self::nan()
                } else {
                    Self::neg_infinity()
                };
            }
            return if o.sign == NUMERIC_PINF {
                Self::neg_infinity()
            } else {
                Self::infinity()
            };
        }
        let mut result = sub_var(self, o);
        strip_var(&mut result);
        result
    }

    /// `self * o` as [`Numeric::mul`] (with its display scale limit), but
    /// without the range check, as [`Numeric::add_unbounded`].
    pub fn mul_unbounded(&self, o: &Self) -> Self {
        if self.is_special() || o.is_special() {
            if self.is_nan() || o.is_nan() {
                return Self::nan();
            }
            let (inf, other) = if self.is_infinite() {
                (self, o)
            } else {
                (o, self)
            };
            let s = sign_internal(other);
            return match (inf.sign == NUMERIC_PINF, s) {
                (_, 0) => Self::nan(),
                (true, 1) | (false, -1) => Self::infinity(),
                _ => Self::neg_infinity(),
            };
        }
        let mut result = mul_var(self, o, self.dscale + o.dscale);
        if result.dscale > NUMERIC_DSCALE_MAX {
            round_var(&mut result, NUMERIC_DSCALE_MAX);
        }
        strip_var(&mut result);
        result
    }

    /// The value, if it fits the storage format (the result of unbounded
    /// arithmetic may not): "value overflows numeric format" otherwise.
    pub fn checked(&self) -> Result<Self> {
        make_result(self.clone())
    }

    /// numeric_div: result scale from select_div_scale; infinity rules.
    pub fn div(&self, o: &Self) -> Result<Self> {
        if self.is_special() || o.is_special() {
            return div_special(self, o);
        }
        let rscale = select_div_scale(self, o);
        make_result(div_var(self, o, rscale, true)?)
    }

    /// numeric_div_trunc: SQL div(numeric, numeric).
    pub fn div_trunc(&self, o: &Self) -> Result<Self> {
        if self.is_special() || o.is_special() {
            return div_special(self, o);
        }
        make_result(div_var(self, o, 0, false)?)
    }

    /// numeric_mod: `%` and mod().
    pub fn rem(&self, o: &Self) -> Result<Self> {
        if self.is_special() || o.is_special() {
            if self.is_nan() || o.is_nan() {
                return Ok(Self::nan());
            }
            if self.is_infinite() {
                if sign_internal(o) == 0 {
                    return Err(err_division_by_zero());
                }
                return Ok(Self::nan());
            }
            // o must be [-]Inf; result is self regardless of sign of o
            return Ok(self.clone());
        }
        make_result(mod_var(self, o)?)
    }

    /// numeric_uminus
    pub fn neg(&self) -> Self {
        let mut r = self.clone();
        match self.sign {
            NUMERIC_NAN => {}
            NUMERIC_PINF => r.sign = NUMERIC_NINF,
            NUMERIC_NINF => r.sign = NUMERIC_PINF,
            _ => {
                if !self.digits.is_empty() {
                    r.sign = if self.sign == NUMERIC_POS {
                        NUMERIC_NEG
                    } else {
                        NUMERIC_POS
                    };
                }
            }
        }
        r
    }

    /// numeric_abs
    pub fn abs(&self) -> Self {
        let mut r = self.clone();
        match self.sign {
            NUMERIC_NAN | NUMERIC_PINF => {}
            NUMERIC_NINF => r.sign = NUMERIC_PINF,
            _ => r.sign = NUMERIC_POS,
        }
        r
    }

    /// numeric_sign: -1, 0, 1 (dscale 0) or NaN.
    pub fn sign(&self) -> Self {
        if self.is_nan() {
            return Self::nan();
        }
        match sign_internal(self) {
            0 => Self::zero(),
            1 => const_one(),
            _ => const_minus_one_var(),
        }
    }

    /// numeric_round(x, scale); negative scale rounds before the point.
    pub fn round(&self, scale: i32) -> Result<Self> {
        if self.is_special() {
            return Ok(self.clone());
        }
        let scale = scale.clamp(
            -(NUMERIC_WEIGHT_MAX + 1) * DEC_DIGITS - 1,
            NUMERIC_DSCALE_MAX,
        );
        let mut arg = self.clone();
        round_var(&mut arg, scale);
        if scale < 0 {
            arg.dscale = 0;
        }
        make_result(arg)
    }

    /// numeric_trunc(x, scale); negative scale truncates before the point.
    pub fn trunc(&self, scale: i32) -> Result<Self> {
        if self.is_special() {
            return Ok(self.clone());
        }
        let scale = scale.clamp(-(NUMERIC_WEIGHT_MAX + 1) * DEC_DIGITS, NUMERIC_DSCALE_MAX);
        let mut arg = self.clone();
        trunc_var(&mut arg, scale);
        if scale < 0 {
            arg.dscale = 0;
        }
        make_result(arg)
    }

    /// numeric_ceil. Saturates to +Infinity in the pathological case where
    /// the result overflows the numeric range (see [`Numeric::try_ceil`]).
    pub fn ceil(&self) -> Self {
        self.try_ceil().unwrap_or_else(|_| Self::infinity())
    }

    /// numeric_floor. Saturates to -Infinity in the pathological case where
    /// the result overflows the numeric range (see [`Numeric::try_floor`]).
    pub fn floor(&self) -> Self {
        self.try_floor().unwrap_or_else(|_| Self::neg_infinity())
    }

    /// numeric_ceil with PostgreSQL's overflow error.
    pub fn try_ceil(&self) -> Result<Self> {
        if self.is_special() {
            return Ok(self.clone());
        }
        make_result(ceil_var(self))
    }

    /// numeric_floor with PostgreSQL's overflow error.
    pub fn try_floor(&self) -> Result<Self> {
        if self.is_special() {
            return Ok(self.clone());
        }
        make_result(floor_var(self))
    }

    // ------------------------------------------------------------------
    // Advanced math
    // ------------------------------------------------------------------

    /// numeric_sqrt
    pub fn sqrt(&self) -> Result<Self> {
        if self.is_special() {
            if self.sign == NUMERIC_NINF {
                return Err(err("2201F", "cannot take square root of a negative number"));
            }
            return Ok(self.clone());
        }
        let sweight = self.weight * DEC_DIGITS / 2 + 1;
        let mut rscale = NUMERIC_MIN_SIG_DIGITS - sweight;
        rscale = rscale.max(self.dscale);
        rscale = rscale.max(NUMERIC_MIN_DISPLAY_SCALE);
        rscale = rscale.min(NUMERIC_MAX_DISPLAY_SCALE);
        make_result(sqrt_var(self, rscale)?)
    }

    /// numeric_exp
    pub fn exp(&self) -> Result<Self> {
        if self.is_special() {
            if self.sign == NUMERIC_NINF {
                return Ok(Self::zero());
            }
            return Ok(self.clone());
        }
        let mut val = var_to_double_no_overflow(self);
        val *= 0.434294481903252;
        val = val.max(-(NUMERIC_MAX_RESULT_SCALE as f64));
        val = val.min(NUMERIC_MAX_RESULT_SCALE as f64);
        let mut rscale = NUMERIC_MIN_SIG_DIGITS - val as i32;
        rscale = rscale.max(self.dscale);
        rscale = rscale.max(NUMERIC_MIN_DISPLAY_SCALE);
        rscale = rscale.min(NUMERIC_MAX_DISPLAY_SCALE);
        make_result(exp_var(self, rscale)?)
    }

    /// numeric_ln
    pub fn ln(&self) -> Result<Self> {
        if self.is_special() {
            if self.sign == NUMERIC_NINF {
                return Err(err("2201E", "cannot take logarithm of a negative number"));
            }
            return Ok(self.clone());
        }
        let ln_dweight = estimate_ln_dweight(self);
        let mut rscale = NUMERIC_MIN_SIG_DIGITS - ln_dweight;
        rscale = rscale.max(self.dscale);
        rscale = rscale.max(NUMERIC_MIN_DISPLAY_SCALE);
        rscale = rscale.min(NUMERIC_MAX_DISPLAY_SCALE);
        make_result(ln_var(self, rscale)?)
    }

    /// SQL log(x) / log10(x) for numeric, defined as log(10, x).
    pub fn log10(&self) -> Result<Self> {
        Self::from_i64(10).log(self)
    }

    /// numeric_log: `log(self, x)` = logarithm of `x` in base `self`.
    pub fn log(&self, x: &Self) -> Result<Self> {
        let num1 = self;
        let num2 = x;
        if num1.is_special() || num2.is_special() {
            if num1.is_nan() || num2.is_nan() {
                return Ok(Self::nan());
            }
            let sign1 = sign_internal(num1);
            let sign2 = sign_internal(num2);
            if sign1 < 0 || sign2 < 0 {
                return Err(err("2201E", "cannot take logarithm of a negative number"));
            }
            if sign1 == 0 || sign2 == 0 {
                return Err(err("2201E", "cannot take logarithm of zero"));
            }
            if num1.sign == NUMERIC_PINF {
                if num2.sign == NUMERIC_PINF {
                    return Ok(Self::nan());
                }
                return Ok(Self::zero());
            }
            return Ok(Self::infinity());
        }
        make_result(log_var(num1, num2)?)
    }

    /// numeric_power: `self ^ exp`.
    pub fn pow(&self, exp: &Self) -> Result<Self> {
        let num1 = self;
        let num2 = exp;
        let zero_pow_neg = || err("2201F", "zero raised to a negative power is undefined");
        if num1.is_special() || num2.is_special() {
            if num1.is_nan() {
                if !num2.is_special() && cmp_var(num2, &var_zero(0)) == 0 {
                    return Ok(const_one());
                }
                return Ok(Self::nan());
            }
            if num2.is_nan() {
                if !num1.is_special() && cmp_var(num1, &const_one()) == 0 {
                    return Ok(const_one());
                }
                return Ok(Self::nan());
            }
            let sign1 = sign_internal(num1);
            let sign2 = sign_internal(num2);
            if sign1 == 0 && sign2 < 0 {
                return Err(zero_pow_neg());
            }
            if sign1 < 0 && !is_integral(num2) {
                return Err(err(
                    "2201F",
                    "a negative number raised to a non-integer power yields a complex result",
                ));
            }
            if !num1.is_special() && cmp_var(num1, &const_one()) == 0 {
                return Ok(const_one());
            }
            if sign2 == 0 {
                return Ok(const_one());
            }
            if sign1 == 0 && sign2 > 0 {
                return Ok(Self::zero());
            }
            if num2.is_infinite() {
                let abs_x_gt_one = if num1.is_special() {
                    true
                } else {
                    if cmp_var(num1, &const_minus_one_var()) == 0 {
                        return Ok(const_one());
                    }
                    let mut a = num1.clone();
                    a.sign = NUMERIC_POS;
                    cmp_var(&a, &const_one()) > 0
                };
                return Ok(if abs_x_gt_one == (sign2 > 0) {
                    Self::infinity()
                } else {
                    Self::zero()
                });
            }
            if num1.sign == NUMERIC_PINF {
                return Ok(if sign2 > 0 {
                    Self::infinity()
                } else {
                    Self::zero()
                });
            }
            // num1 is -Inf
            if sign2 < 0 {
                return Ok(Self::zero());
            }
            let arg2 = num2;
            if arg2.nd() > 0 && arg2.nd() == arg2.weight + 1 && (arg2.dig(arg2.nd() - 1) & 1) != 0 {
                return Ok(Self::neg_infinity());
            }
            return Ok(Self::infinity());
        }

        let sign1 = sign_internal(num1);
        let sign2 = sign_internal(num2);
        if sign1 == 0 && sign2 < 0 {
            return Err(zero_pow_neg());
        }
        make_result(power_var(num1, num2)?)
    }

    /// numeric_gcd
    pub fn gcd(&self, o: &Self) -> Result<Self> {
        if self.is_special() || o.is_special() {
            return Ok(Self::nan());
        }
        make_result(gcd_var(self, o)?)
    }

    /// numeric_lcm
    pub fn lcm(&self, o: &Self) -> Result<Self> {
        if self.is_special() || o.is_special() {
            return Ok(Self::nan());
        }
        let mut result = if self.digits.is_empty() || o.digits.is_empty() {
            var_zero(0)
        } else {
            let g = gcd_var(self, o)?;
            let q = div_var(self, &g, 0, false)?;
            let mut r = mul_var(o, &q, o.dscale);
            r.sign = NUMERIC_POS;
            r
        };
        result.dscale = self.dscale.max(o.dscale);
        make_result(result)
    }

    /// numeric_fac: factorial(n).
    pub fn factorial(n: i64) -> Result<Self> {
        if n < 0 {
            return Err(err("22003", "factorial of a negative number is undefined"));
        }
        if n <= 1 {
            return Ok(const_one());
        }
        if n > 32177 {
            return Err(err_overflow());
        }
        let mut result = int64_to_var(n);
        let mut k = n - 1;
        while k > 1 {
            result = mul_var(&result, &int64_to_var(k), 0);
            k -= 1;
        }
        make_result(result)
    }

    /// Coercion to `numeric(precision, scale)` (the `numeric()` length
    /// coercion function / apply_typmod), including negative scales and
    /// scale > precision (PG15+). Precision must be in 1..=1000 and scale in
    /// -1000..=1000 (PostgreSQL's typmod limits).
    pub fn apply_typmod(&self, precision: i32, scale: i32) -> Result<Self> {
        check_typmod(precision, scale)?;
        if self.is_special() {
            return apply_typmod_special(self.clone(), precision, scale);
        }
        let mut var = self.clone();
        apply_typmod_var(&mut var, precision, scale)?;
        make_result(var)
    }

    // ------------------------------------------------------------------
    // Aggregate support
    // ------------------------------------------------------------------

    /// `numeric_smaller` (the `min()` aggregate transition): returns `self`
    /// if it is strictly smaller than `o`, otherwise `o` (so on ties between
    /// equal values with different scales, the second argument wins, unlike
    /// `Ord::min`).
    pub fn smaller<'a>(&'a self, o: &'a Self) -> &'a Self {
        if self.cmp(o) == Ordering::Less {
            self
        } else {
            o
        }
    }

    /// `numeric_larger` (the `max()` aggregate transition): returns `self`
    /// if it is strictly larger than `o`, otherwise `o`.
    pub fn larger<'a>(&'a self, o: &'a Self) -> &'a Self {
        if self.cmp(o) == Ordering::Greater {
            self
        } else {
            o
        }
    }

    /// Final step of numeric `var_pop` / `var_samp` / `stddev_pop` /
    /// `stddev_samp` (PostgreSQL's `numeric_stddev_internal`).
    ///
    /// * `n` - number of non-NULL inputs (including NaN/Infinity inputs)
    /// * `sum_x` - sum of the inputs (computed with [`Numeric::add`])
    /// * `sum_x2` - sum of the squares of the inputs, each square computed
    ///   with `x.mul(&x)` (exact, like PostgreSQL's `mul_var(X, X, 2*dscale)`)
    /// * `variance` - true for var_*, false for stddev_*
    /// * `sample` - true for *_samp, false for *_pop
    ///
    /// Returns `None` for SQL NULL (no inputs, or a single input for the
    /// sample variants). If any input was NaN or infinite (i.e. `sum_x` or
    /// `sum_x2` is not finite) the result is NaN. For integer inputs, pass
    /// the (exact) integer sums converted with [`Numeric::from_i128`].
    pub fn stddev_from_sums(
        n: i64,
        sum_x: &Numeric,
        sum_x2: &Numeric,
        variance: bool,
        sample: bool,
    ) -> Result<Option<Numeric>> {
        if n <= 0 || (sample && n <= 1) {
            return Ok(None);
        }
        if sum_x.is_special() || sum_x2.is_special() {
            return Ok(Some(Self::nan()));
        }
        let v_n = int64_to_var(n);
        let v_nminus1 = sub_var(&v_n, &const_one());

        let rscale = sum_x.dscale * 2;
        let vsumx = mul_var(sum_x, sum_x, rscale);
        let vsumx2 = mul_var(&v_n, sum_x2, rscale);
        let vsumx2 = sub_var(&vsumx2, &vsumx);

        if cmp_var(&vsumx2, &var_zero(0)) <= 0 {
            return Ok(Some(Self::zero()));
        }
        let denom = if sample {
            mul_var(&v_n, &v_nminus1, 0)
        } else {
            mul_var(&v_n, &v_n, 0)
        };
        let rscale = select_div_scale(&vsumx2, &denom);
        let mut res = div_var(&vsumx2, &denom, rscale, true)?;
        if !variance {
            res = sqrt_var(&res, rscale)?;
        }
        make_result(res).map(Some)
    }

    // ------------------------------------------------------------------
    // Encodings
    // ------------------------------------------------------------------

    /// Order-preserving ("memcomparable") key encoding: `memcmp` of encodings
    /// equals `numeric_cmp`; equal values (e.g. 1.0 and 1.00) produce
    /// identical bytes; self-delimiting.
    pub fn encode_key(&self, out: &mut Vec<u8>) {
        encoding::encode_key(self, out)
    }

    /// Decodes a key produced by [`Numeric::encode_key`]; returns the
    /// normalized value (minimal dscale) and the number of bytes consumed.
    pub fn decode_key(buf: &[u8]) -> Result<(Self, usize)> {
        encoding::decode_key(buf)
    }

    /// Length of the key encoding at the start of `buf` (bytes inverted when
    /// `complemented`), without decoding it.
    pub fn key_len(buf: &[u8], complemented: bool) -> Result<usize> {
        encoding::key_len(buf, complemented)
    }

    /// Length of the [`Numeric::encode_binary`] encoding at the start of
    /// `buf`, without decoding it.
    pub fn binary_len(buf: &[u8]) -> Result<usize> {
        encoding::binary_len(buf)
    }

    /// Compact exact binary encoding preserving dscale; self-delimiting.
    pub fn encode_binary(&self, out: &mut Vec<u8>) {
        encoding::encode_binary(self, out)
    }

    /// Decodes [`Numeric::encode_binary`] output; returns value and bytes
    /// consumed.
    pub fn decode_binary(buf: &[u8]) -> Result<(Self, usize)> {
        encoding::decode_binary(buf)
    }

    /// [`Numeric::decode_binary`], also accepting the finite values beyond
    /// numeric's range that unbounded arithmetic produces (the encoding
    /// represents them; see [`Numeric::add_unbounded`]).
    pub fn decode_binary_unbounded(buf: &[u8]) -> Result<(Self, usize)> {
        encoding::decode_binary_unbounded(buf)
    }

    /// PostgreSQL binary wire format (`numeric_send`).
    pub fn to_pg_binary(&self) -> Vec<u8> {
        encoding::to_pg_binary(self)
    }

    /// PostgreSQL binary wire format (`numeric_recv`, typmod -1).
    pub fn from_pg_binary(buf: &[u8]) -> Result<Self> {
        encoding::from_pg_binary(buf, None)
    }

    /// `numeric_recv` with a typmod `numeric(precision, scale)` (binary COPY
    /// / replication into a typmod'd column).
    pub fn from_pg_binary_with_typmod(buf: &[u8], precision: i32, scale: i32) -> Result<Self> {
        check_typmod(precision, scale)?;
        encoding::from_pg_binary(buf, Some((precision, scale)))
    }
}

/// numeric_sign_internal (caller handles NaN): -1, 0, 1.
fn sign_internal(n: &Numeric) -> i32 {
    match n.sign {
        NUMERIC_PINF => 1,
        NUMERIC_NINF => -1,
        _ => {
            if n.digits.is_empty() {
                0
            } else if n.sign == NUMERIC_NEG {
                -1
            } else {
                1
            }
        }
    }
}

/// numeric_is_integral: NaN is not integral, infinities are.
fn is_integral(n: &Numeric) -> bool {
    if n.is_special() {
        return !n.is_nan();
    }
    n.digits.is_empty() || n.nd() <= n.weight + 1
}

/// Shared NaN/Infinity handling of numeric_div and numeric_div_trunc.
fn div_special(a: &Numeric, b: &Numeric) -> Result<Numeric> {
    if a.is_nan() || b.is_nan() {
        return Ok(Numeric::nan());
    }
    if a.is_infinite() {
        if b.is_special() {
            return Ok(Numeric::nan());
        }
        return match sign_internal(b) {
            0 => Err(err_division_by_zero()),
            s => Ok(if (s > 0) == (a.sign == NUMERIC_PINF) {
                Numeric::infinity()
            } else {
                Numeric::neg_infinity()
            }),
        };
    }
    // a is finite, b is infinite
    Ok(Numeric::zero())
}

/// get_min_scale
fn get_min_scale(var: &Numeric) -> i32 {
    let mut last = var.nd() - 1;
    while last >= 0 && var.dig(last) == 0 {
        last -= 1;
    }
    if last >= 0 {
        let mut min_scale = (last - var.weight) * DEC_DIGITS;
        if min_scale > 0 {
            let mut last_digit = var.dig(last);
            while last_digit % 10 == 0 {
                min_scale -= 1;
                last_digit /= 10;
            }
            min_scale
        } else {
            0
        }
    } else {
        0
    }
}

/// Result of the syntactic part of `numeric_in`.
pub(crate) enum Parsed {
    Special(Numeric),
    Finite(Numeric),
}

/// `numeric_in` up to (but not including) typmod application and the
/// storage-format check.
pub(crate) fn parse_var(s: &str) -> Result<Parsed> {
    let b = s.as_bytes();
    let len = b.len();
    let at = |i: usize| -> u8 { if i < len { b[i] } else { 0 } };

    let mut cp = 0;
    while cp < len && is_space(b[cp]) {
        cp += 1;
    }

    let numstart = cp;
    let mut sign = NUMERIC_POS;
    if at(cp) == b'+' {
        cp += 1;
    } else if at(cp) == b'-' {
        sign = NUMERIC_NEG;
        cp += 1;
    }

    if !at(cp).is_ascii_digit() && at(cp) != b'.' {
        let starts_ci = |pos: usize, pat: &[u8]| -> bool {
            pos + pat.len() <= len && b[pos..pos + pat.len()].eq_ignore_ascii_case(pat)
        };
        let res;
        if starts_ci(numstart, b"nan") {
            res = Numeric::nan();
            cp = numstart + 3;
        } else if starts_ci(cp, b"infinity") {
            res = if sign == NUMERIC_POS {
                Numeric::infinity()
            } else {
                Numeric::neg_infinity()
            };
            cp += 8;
        } else if starts_ci(cp, b"inf") {
            res = if sign == NUMERIC_POS {
                Numeric::infinity()
            } else {
                Numeric::neg_infinity()
            };
            cp += 3;
        } else {
            return Err(invalid_numeric_syntax(s));
        }
        while cp < len {
            if !is_space(b[cp]) {
                return Err(invalid_numeric_syntax(s));
            }
            cp += 1;
        }
        return Ok(Parsed::Special(res));
    }

    let base = if at(cp) == b'0' {
        match at(cp + 1) {
            b'x' | b'X' => 16,
            b'o' | b'O' => 8,
            b'b' | b'B' => 2,
            _ => 10,
        }
    } else {
        10
    };

    let (value, end) = if base == 10 {
        let (mut v, end) = set_var_from_str(s, b, cp)?;
        v.sign = sign;
        (v, end)
    } else {
        set_var_from_non_decimal_integer_str(s, b, cp + 2, sign, base)?
    };

    cp = end;
    while cp < len {
        if !is_space(b[cp]) {
            return Err(invalid_numeric_syntax(s));
        }
        cp += 1;
    }

    Ok(Parsed::Finite(value))
}

/// make_numeric_typmod_safe's validation.
fn check_typmod(precision: i32, scale: i32) -> Result<()> {
    if !(1..=NUMERIC_MAX_PRECISION).contains(&precision) {
        return Err(err(
            "22023",
            format!(
                "NUMERIC precision {} must be between 1 and {}",
                precision, NUMERIC_MAX_PRECISION
            ),
        ));
    }
    if !(NUMERIC_MIN_SCALE..=NUMERIC_MAX_SCALE).contains(&scale) {
        return Err(err(
            "22023",
            format!(
                "NUMERIC scale {} must be between {} and {}",
                scale, NUMERIC_MIN_SCALE, NUMERIC_MAX_SCALE
            ),
        ));
    }
    Ok(())
}

/// apply_typmod_special: NaN passes, infinities are rejected.
pub(crate) fn apply_typmod_special(v: Numeric, precision: i32, scale: i32) -> Result<Numeric> {
    if v.is_nan() {
        return Ok(v);
    }
    Err(numeric_field_overflow(format!(
        "A field with precision {}, scale {} cannot hold an infinite value.",
        precision, scale
    )))
}

/// apply_typmod: round to the target scale and check the precision.
pub(crate) fn apply_typmod_var(var: &mut Numeric, precision: i32, scale: i32) -> Result<()> {
    let maxdigits = precision - scale;
    round_var(var, scale);
    if var.dscale < 0 {
        var.dscale = 0;
    }

    let mut ddigits = (var.weight + 1) * DEC_DIGITS;
    if ddigits > maxdigits {
        for &d in var.digits.iter() {
            if d != 0 {
                if d < 10 {
                    ddigits -= 3;
                } else if d < 100 {
                    ddigits -= 2;
                } else if d < 1000 {
                    ddigits -= 1;
                }
                if ddigits > maxdigits {
                    return Err(numeric_field_overflow(format!(
                        "A field with precision {}, scale {} must round to an absolute value less than {}{}.",
                        precision,
                        scale,
                        if maxdigits != 0 { "10^" } else { "" },
                        if maxdigits != 0 { maxdigits } else { 1 }
                    )));
                }
                break;
            }
            ddigits -= DEC_DIGITS;
        }
    }
    Ok(())
}
