//! JSON numbers: PostgreSQL `numeric_in` / `numeric_out` normalization.
//!
//! Ported from src/backend/utils/adt/numeric.c of PostgreSQL 17
//! (Copyright (c) 1996-2024, PostgreSQL Global Development Group; PostgreSQL
//! License). jsonb stores numbers as `numeric`; this module keeps the exact
//! text `numeric_out` would print for the value `numeric_in` produced, and
//! compares numbers by value (so `1.50 == 1.5`).

use crate::{Error, Result, sqlstate};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

/// NUMERIC_DSCALE_MAX
const DSCALE_MAX: i64 = 0x3FFF;
/// Range of the int16 weight of a stored numeric, in NBASE (10000) digits.
const WEIGHT_MAX: i64 = i16::MAX as i64;
const WEIGHT_MIN: i64 = i16::MIN as i64;
/// Largest decimal exponent `E` (value = 0.D x 10^E) of a storable numeric.
const EXP10_MAX: i64 = (WEIGHT_MAX + 1) * 4;

/// A JSON number: decimal value kept as PostgreSQL `numeric` text, i.e. the
/// output of `numeric_out(numeric_in(input))` (`1e2` -> `100`, `1.50` ->
/// `1.50`, `-0` -> `0`). Equality and ordering are by numeric value.
#[derive(Debug, Clone)]
pub struct JsonNumber {
    text: Box<str>,
}

fn is_c_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

fn invalid_syntax(s: &str) -> Error {
    Error::new(
        sqlstate::INVALID_TEXT_REPRESENTATION,
        format!("invalid input syntax for type numeric: \"{s}\""),
    )
}

fn overflow() -> Error {
    Error::new(
        sqlstate::NUMERIC_VALUE_OUT_OF_RANGE,
        "value overflows numeric format",
    )
}

fn starts_with_ci(b: &[u8], pat: &[u8]) -> bool {
    b.len() >= pat.len() && b[..pat.len()].eq_ignore_ascii_case(pat)
}

/// Canonical decomposition of a nonzero value: value = (-1)^neg * 0.D * 10^exp
/// where D are the significant decimal digits (no leading/trailing zeros).
pub(crate) struct Decomposed<'a> {
    pub neg: bool,
    pub exp: i64,
    int: &'a [u8],
    frac: &'a [u8],
    start: usize,
    end: usize,
}

impl Decomposed<'_> {
    pub fn len(&self) -> usize {
        self.end - self.start
    }
    /// i-th significant digit (0..=9)
    pub fn digit(&self, i: usize) -> u8 {
        let k = self.start + i;
        if k < self.int.len() {
            self.int[k] - b'0'
        } else {
            self.frac[k - self.int.len()] - b'0'
        }
    }
}

struct Parts<'a> {
    neg: bool,
    /// integer digits without leading zeros (empty when |x| < 1)
    int: &'a [u8],
    /// fraction digits (display scale), may have trailing zeros
    frac: &'a [u8],
}

impl JsonNumber {
    /// `numeric_in` followed by `numeric_out`. Accepts the full `numeric`
    /// input syntax (surrounding whitespace, sign, `.5`, exponents,
    /// underscores, `0x`/`0o`/`0b` integers). NaN and infinities are valid
    /// `numeric` values but cannot be JSON numbers: they are rejected with
    /// SQLSTATE 22P02 (PostgreSQL's `to_jsonb` turns them into strings).
    pub fn parse(s: &str) -> Result<JsonNumber> {
        // A C string ends at the first NUL.
        let s = match s.find('\0') {
            Some(p) => &s[..p],
            None => s,
        };
        let b = s.as_bytes();
        let n = b.len();
        let mut i = 0;
        while i < n && is_c_space(b[i]) {
            i += 1;
        }
        let numstart = i;
        let mut neg = false;
        if i < n && b[i] == b'+' {
            i += 1;
        } else if i < n && b[i] == b'-' {
            neg = true;
            i += 1;
        }
        let c = b.get(i).copied().unwrap_or(0);
        if !c.is_ascii_digit() && c != b'.' {
            let end = if starts_with_ci(&b[numstart..], b"nan") {
                numstart + 3
            } else if starts_with_ci(&b[i..], b"infinity") {
                i + 8
            } else if starts_with_ci(&b[i..], b"inf") {
                i + 3
            } else {
                return Err(invalid_syntax(s));
            };
            if b[end..].iter().any(|&c| !is_c_space(c)) {
                return Err(invalid_syntax(s));
            }
            return Err(Error::new(
                sqlstate::INVALID_TEXT_REPRESENTATION,
                format!(
                    "cannot represent numeric value \"{}\" as a JSON number",
                    s.trim()
                ),
            ));
        }
        if c == b'0' && i + 1 < n {
            let base = match b[i + 1] {
                b'x' | b'X' => 16,
                b'o' | b'O' => 8,
                b'b' | b'B' => 2,
                _ => 10,
            };
            if base != 10 {
                return parse_non_decimal(s, i + 2, neg, base);
            }
        }

        // set_var_from_str()
        let mut j = i;
        let mut have_dp = false;
        if b.get(j) == Some(&b'.') {
            have_dp = true;
            j += 1;
        }
        if !b.get(j).is_some_and(u8::is_ascii_digit) {
            return Err(invalid_syntax(s));
        }
        let mut digits: Vec<u8> = Vec::with_capacity(n - j);
        let mut dweight: i64 = -1;
        let mut dscale: i64 = 0;
        while j < n {
            let ch = b[j];
            if ch.is_ascii_digit() {
                digits.push(ch - b'0');
                j += 1;
                if !have_dp {
                    dweight += 1;
                } else {
                    dscale += 1;
                }
            } else if ch == b'.' {
                if have_dp {
                    return Err(invalid_syntax(s));
                }
                have_dp = true;
                j += 1;
                if b.get(j) == Some(&b'_') {
                    return Err(invalid_syntax(s));
                }
            } else if ch == b'_' {
                j += 1;
                if !b.get(j).is_some_and(u8::is_ascii_digit) {
                    return Err(invalid_syntax(s));
                }
            } else {
                break;
            }
        }
        if j < n && (b[j] == b'e' || b[j] == b'E') {
            j += 1;
            let mut eneg = false;
            if b.get(j) == Some(&b'+') {
                j += 1;
            } else if b.get(j) == Some(&b'-') {
                eneg = true;
                j += 1;
            }
            if !b.get(j).is_some_and(u8::is_ascii_digit) {
                return Err(invalid_syntax(s));
            }
            let mut exponent: i64 = 0;
            while j < n {
                let ch = b[j];
                if ch.is_ascii_digit() {
                    exponent = exponent * 10 + i64::from(ch - b'0');
                    j += 1;
                    if exponent > i64::from(i32::MAX / 2) {
                        return Err(overflow());
                    }
                } else if ch == b'_' {
                    j += 1;
                    if !b.get(j).is_some_and(u8::is_ascii_digit) {
                        return Err(invalid_syntax(s));
                    }
                } else {
                    break;
                }
            }
            if eneg {
                exponent = -exponent;
            }
            dweight += exponent;
            dscale -= exponent;
            if dscale < 0 {
                dscale = 0;
            }
        }
        // numeric_in: nothing but spaces may follow
        if b[j..].iter().any(|&c| !is_c_space(c)) {
            return Err(invalid_syntax(s));
        }
        // make_result(): int16 weight and 14-bit dscale must not overflow
        if dscale > DSCALE_MAX {
            return Err(overflow());
        }
        let Some(first) = digits.iter().position(|&d| d != 0) else {
            let mut t = String::with_capacity(2 + dscale as usize);
            t.push('0');
            if dscale > 0 {
                t.push('.');
                t.extend(std::iter::repeat_n('0', dscale as usize));
            }
            return Ok(JsonNumber {
                text: t.into_boxed_str(),
            });
        };
        let p_first = dweight - first as i64;
        let w = p_first.div_euclid(4);
        if !(WEIGHT_MIN..=WEIGHT_MAX).contains(&w) {
            return Err(overflow());
        }
        let digit_at = |e: i64| -> char {
            let k = dweight - e;
            if k >= 0 && (k as usize) < digits.len() {
                (b'0' + digits[k as usize]) as char
            } else {
                '0'
            }
        };
        let mut t = String::with_capacity(3 + p_first.max(0) as usize + dscale as usize);
        if neg {
            t.push('-');
        }
        if p_first >= 0 {
            let mut e = p_first;
            loop {
                t.push(digit_at(e));
                if e == 0 {
                    break;
                }
                e -= 1;
            }
        } else {
            t.push('0');
        }
        if dscale > 0 {
            t.push('.');
            for e in 1..=dscale {
                t.push(digit_at(-e));
            }
        }
        Ok(JsonNumber {
            text: t.into_boxed_str(),
        })
    }

    /// numeric_in for a token already validated by the JSON lexer
    /// (`-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?`). Without an
    /// exponent the canonical text is the token itself, except for negative
    /// zero.
    pub(crate) fn from_json_token(raw: &str) -> Result<JsonNumber> {
        let b = raw.as_bytes();
        if b.len() <= 64 && !b.iter().any(|&c| c == b'e' || c == b'E') {
            let neg_zero =
                b.first() == Some(&b'-') && b[1..].iter().all(|&c| c == b'0' || c == b'.');
            let text = if neg_zero { &raw[1..] } else { raw };
            return Ok(JsonNumber { text: text.into() });
        }
        JsonNumber::parse(raw)
    }

    /// A number from text that is already in `numeric_out` form (as produced
    /// by `pgnumeric::Numeric`'s `Display` for a finite value).
    pub(crate) fn from_numeric_text(text: String) -> JsonNumber {
        debug_assert!(JsonNumber::parse(&text).is_ok_and(|n| n.as_str() == text));
        JsonNumber {
            text: text.into_boxed_str(),
        }
    }

    /// numeric_uminus: zero keeps its (positive) sign.
    pub(crate) fn negate(&self) -> JsonNumber {
        if self.is_zero() {
            return self.clone();
        }
        let text = match self.text.strip_prefix('-') {
            Some(t) => t.to_owned(),
            None => format!("-{}", self.text),
        };
        JsonNumber {
            text: text.into_boxed_str(),
        }
    }

    /// The `numeric_out` text of the value.
    pub fn as_str(&self) -> &str {
        &self.text
    }

    pub fn from_i64(v: i64) -> JsonNumber {
        JsonNumber {
            text: v.to_string().into_boxed_str(),
        }
    }

    /// float8 -> jsonb number as PostgreSQL does it (`to_jsonb(float8)`):
    /// `numeric_in(float8out(v))` with the default `extra_float_digits = 1`,
    /// i.e. the shortest decimal that round-trips. `None` for NaN/infinity
    /// (PostgreSQL produces the JSON strings "NaN"/"Infinity" there).
    pub fn from_f64(v: f64) -> Option<JsonNumber> {
        if !v.is_finite() {
            return None;
        }
        JsonNumber::parse(&format!("{v:e}")).ok()
    }

    fn parts(&self) -> Parts<'_> {
        let b = self.text.as_bytes();
        let (neg, rest) = if b.first() == Some(&b'-') {
            (true, &b[1..])
        } else {
            (false, b)
        };
        let (int, frac) = match rest.iter().position(|&c| c == b'.') {
            Some(p) => (&rest[..p], &rest[p + 1..]),
            None => (rest, &[][..]),
        };
        let int = if int == b"0" { &[][..] } else { int };
        Parts { neg, int, frac }
    }

    pub fn is_zero(&self) -> bool {
        let p = self.parts();
        p.int.is_empty() && p.frac.iter().all(|&c| c == b'0')
    }

    /// -1, 0 or 1
    pub fn signum(&self) -> i32 {
        let p = self.parts();
        if p.int.is_empty() && p.frac.iter().all(|&c| c == b'0') {
            0
        } else if p.neg {
            -1
        } else {
            1
        }
    }

    /// Canonical decomposition; `None` for zero.
    pub(crate) fn decompose(&self) -> Option<Decomposed<'_>> {
        let p = self.parts();
        let total = p.int.len() + p.frac.len();
        let at = |k: usize| {
            if k < p.int.len() {
                p.int[k]
            } else {
                p.frac[k - p.int.len()]
            }
        };
        let mut start = 0;
        while start < total && at(start) == b'0' {
            start += 1;
        }
        if start == total {
            return None;
        }
        let mut end = total;
        while at(end - 1) == b'0' {
            end -= 1;
        }
        let exp = p.int.len() as i64 - start as i64;
        Some(Decomposed {
            neg: p.neg,
            exp,
            int: p.int,
            frac: p.frac,
            start,
            end,
        })
    }

    /// Inverse of [`decompose`]: canonical text with the minimal display
    /// scale. Validates the numeric range.
    pub(crate) fn from_decomposed(neg: bool, exp: i64, digits: &[u8]) -> Option<JsonNumber> {
        let n = digits.len() as i64;
        if digits.is_empty() || digits[0] == 0 || digits[digits.len() - 1] == 0 {
            return None;
        }
        if exp > EXP10_MAX || n - exp > DSCALE_MAX {
            return None;
        }
        let mut t = String::with_capacity((n.max(exp) + exp.unsigned_abs() as i64 + 3) as usize);
        if neg {
            t.push('-');
        }
        let push_digits =
            |t: &mut String, ds: &[u8]| t.extend(ds.iter().map(|&d| (b'0' + d) as char));
        if exp >= n {
            push_digits(&mut t, digits);
            t.extend(std::iter::repeat_n('0', (exp - n) as usize));
        } else if exp > 0 {
            push_digits(&mut t, &digits[..exp as usize]);
            t.push('.');
            push_digits(&mut t, &digits[exp as usize..]);
        } else {
            t.push_str("0.");
            t.extend(std::iter::repeat_n('0', (-exp) as usize));
            push_digits(&mut t, digits);
        }
        Some(JsonNumber {
            text: t.into_boxed_str(),
        })
    }
}

/// set_var_from_non_decimal_integer_str()
fn parse_non_decimal(s: &str, start: usize, neg: bool, base: u32) -> Result<JsonNumber> {
    let b = s.as_bytes();
    let n = b.len();
    let is_digit = |c: u8| match base {
        16 => c.is_ascii_hexdigit(),
        8 => (b'0'..=b'7').contains(&c),
        _ => c == b'0' || c == b'1',
    };
    let mut j = start;
    let mut vals: Vec<u8> = Vec::new();
    while j < n {
        let c = b[j];
        if is_digit(c) {
            vals.push((c as char).to_digit(16).unwrap() as u8);
            j += 1;
        } else if c == b'_' {
            j += 1;
            if !(j < n && is_digit(b[j])) {
                return Err(invalid_syntax(s));
            }
        } else {
            break;
        }
    }
    if j == start {
        return Err(invalid_syntax(s));
    }
    // convert to decimal using base-1e9 limbs (little endian)
    const LIMB: u64 = 1_000_000_000;
    let mut limbs: Vec<u32> = Vec::new();
    for &d in &vals {
        let mut carry = u64::from(d);
        for limb in limbs.iter_mut() {
            let v = u64::from(*limb) * u64::from(base) + carry;
            *limb = (v % LIMB) as u32;
            carry = v / LIMB;
        }
        while carry > 0 {
            limbs.push((carry % LIMB) as u32);
            carry /= LIMB;
        }
    }
    let mut dec = String::new();
    match limbs.split_last() {
        None => dec.push('0'),
        Some((top, rest)) => {
            dec.push_str(&top.to_string());
            for l in rest.iter().rev() {
                dec.push_str(&format!("{l:09}"));
            }
        }
    }
    // weight (in NBASE digits) must fit in int16
    if dec.len() as i64 > EXP10_MAX {
        return Err(overflow());
    }
    if b[j..].iter().any(|&c| !is_c_space(c)) {
        return Err(invalid_syntax(s));
    }
    let text = if neg && dec != "0" {
        format!("-{dec}")
    } else {
        dec
    };
    Ok(JsonNumber {
        text: text.into_boxed_str(),
    })
}

fn cmp_frac(a: &[u8], b: &[u8]) -> Ordering {
    let n = a.len().max(b.len());
    for i in 0..n {
        let x = a.get(i).copied().unwrap_or(b'0');
        let y = b.get(i).copied().unwrap_or(b'0');
        match x.cmp(&y) {
            Ordering::Equal => {}
            o => return o,
        }
    }
    Ordering::Equal
}

impl PartialEq for JsonNumber {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for JsonNumber {}
impl PartialOrd for JsonNumber {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for JsonNumber {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.text == other.text {
            return Ordering::Equal;
        }
        let a = self.parts();
        let b = other.parts();
        let za = a.int.is_empty() && a.frac.iter().all(|&c| c == b'0');
        let zb = b.int.is_empty() && b.frac.iter().all(|&c| c == b'0');
        let sa = if za {
            0
        } else if a.neg {
            -1
        } else {
            1
        };
        let sb = if zb {
            0
        } else if b.neg {
            -1
        } else {
            1
        };
        if sa != sb {
            return sa.cmp(&sb);
        }
        if sa == 0 {
            return Ordering::Equal;
        }
        let mag = a
            .int
            .len()
            .cmp(&b.int.len())
            .then_with(|| a.int.cmp(b.int))
            .then_with(|| cmp_frac(a.frac, b.frac));
        if sa < 0 { mag.reverse() } else { mag }
    }
}

impl Hash for JsonNumber {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self.decompose() {
            None => state.write_u8(0),
            Some(d) => {
                state.write_u8(if d.neg { 1 } else { 2 });
                state.write_i64(d.exp);
                for i in 0..d.len() {
                    state.write_u8(d.digit(i));
                }
                state.write_u8(0xff);
            }
        }
    }
}

impl std::fmt::Display for JsonNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> String {
        JsonNumber::parse(s)
            .map(|n| n.as_str().to_owned())
            .unwrap_or_else(|e| format!("ERR {} {}", e.sqlstate, e.message))
    }

    #[test]
    fn normalization() {
        assert_eq!(p("1e2"), "100");
        assert_eq!(p("1.50"), "1.50");
        assert_eq!(p("1.5e-3"), "0.0015");
        assert_eq!(p("-0"), "0");
        assert_eq!(p("-0.00"), "0.00");
        assert_eq!(p("0e-5"), "0.00000");
        assert_eq!(p(" 12.5e1 "), "125");
        assert_eq!(p("1.55e1"), "15.5");
        assert_eq!(p(".5"), "0.5");
        assert_eq!(p("5."), "5");
        assert_eq!(p("0x1F"), "31");
        assert_eq!(p("-0x1F"), "-31");
        assert_eq!(p("0x_1"), "1");
        assert_eq!(p("1_000.5"), "1000.5");
        assert_eq!(p("0b101"), "5");
        assert_eq!(p("0o17"), "15");
        assert_eq!(p("007"), "7");
        assert_eq!(p("-000.0100"), "-0.0100");
        assert!(p("1e").starts_with("ERR 22P02"));
        assert!(p("1__0").starts_with("ERR 22P02"));
        assert!(p("1e9999999999").starts_with("ERR 22003"));
        assert!(p("1e200000").starts_with("ERR 22003"));
        assert!(p("1e-20000").starts_with("ERR 22003"));
        assert!(p("NaN").starts_with("ERR 22P02"));
    }

    #[test]
    fn ordering() {
        let n = |s: &str| JsonNumber::parse(s).unwrap();
        assert_eq!(n("1.50"), n("1.5"));
        assert!(n("0.01") < n("0.1"));
        assert!(n("-0.01") > n("-0.1"));
        assert!(n("-1") < n("0"));
        assert!(n("10") > n("9.99"));
        assert_eq!(n("0.000"), n("0"));
        assert!(n("100") > n("99.5"));
    }

    #[test]
    fn floats() {
        let f = |v: f64| JsonNumber::from_f64(v).unwrap().as_str().to_owned();
        assert_eq!(f(0.1), "0.1");
        assert_eq!(f(1e20), "100000000000000000000");
        assert_eq!(f(1.5e-7), "0.00000015");
        assert_eq!(f(-0.0), "0");
        assert_eq!(f(123.456), "123.456");
        assert!(JsonNumber::from_f64(f64::NAN).is_none());
    }
}
