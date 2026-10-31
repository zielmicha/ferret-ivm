//! Variable-level arithmetic: a faithful port of the `NumericVar` routines
//! of PostgreSQL 17's `src/backend/utils/adt/numeric.c`.
//!
//! Portions Copyright (c) 1998-2024, PostgreSQL Global Development Group.
//! Used under the PostgreSQL License.
//!
//! The algorithms (including their deliberately truncated intermediate
//! computations, guard digits and scale selection rules) are reproduced as
//! closely as possible so that results match PostgreSQL digit for digit.
//! The main structural difference is that functions return fresh values
//! instead of writing into (possibly aliased) output arguments.

// PostgreSQL uses truncated literals such as 0.434294481903252 (log10(e))
// and 0.301029995663981 (log10(2)); they influence the chosen scales, so they
// must not be replaced by the (slightly different) std constants.
#![allow(clippy::approx_constant)]

use std::cmp::{max, min};

use smallvec::{SmallVec, smallvec};

use crate::{Error, Numeric, Result};

pub(crate) const NBASE: i32 = 10000;
pub(crate) const HALF_NBASE: i32 = 5000;
pub(crate) const DEC_DIGITS: i32 = 4;
const MUL_GUARD_DIGITS: i32 = 2;
const DIV_GUARD_DIGITS: i32 = 4;

pub(crate) const NUMERIC_POS: u16 = 0x0000;
pub(crate) const NUMERIC_NEG: u16 = 0x4000;
pub(crate) const NUMERIC_NAN: u16 = 0xC000;
pub(crate) const NUMERIC_PINF: u16 = 0xD000;
pub(crate) const NUMERIC_NINF: u16 = 0xF000;

pub(crate) const NUMERIC_WEIGHT_MAX: i32 = 32767;
pub(crate) const NUMERIC_WEIGHT_MIN: i32 = -32768;
pub(crate) const NUMERIC_DSCALE_MAX: i32 = 0x3FFF;

pub(crate) const NUMERIC_MAX_PRECISION: i32 = 1000;
pub(crate) const NUMERIC_MIN_SCALE: i32 = -1000;
pub(crate) const NUMERIC_MAX_SCALE: i32 = 1000;
pub(crate) const NUMERIC_MAX_DISPLAY_SCALE: i32 = NUMERIC_MAX_PRECISION;
pub(crate) const NUMERIC_MIN_DISPLAY_SCALE: i32 = 0;
pub(crate) const NUMERIC_MAX_RESULT_SCALE: i32 = NUMERIC_MAX_PRECISION * 2;
pub(crate) const NUMERIC_MIN_SIG_DIGITS: i32 = 16;

const ROUND_POWERS: [i32; 4] = [0, 1000, 100, 10];

/// Base-NBASE digit storage. Values of up to 32 decimal digits stay inline.
pub(crate) type Digits = SmallVec<[i16; 8]>;

/// Internal working value (PostgreSQL's `NumericVar`). Same struct as the
/// public type; public values are additionally always normalized.
pub(crate) type Var = Numeric;

// ----------------------------------------------------------------------
// Errors
// ----------------------------------------------------------------------

pub(crate) fn err(sqlstate: &'static str, message: impl Into<String>) -> Error {
    Error {
        sqlstate,
        message: message.into(),
        detail: None,
    }
}

pub(crate) fn err_division_by_zero() -> Error {
    err("22012", "division by zero")
}

pub(crate) fn err_overflow() -> Error {
    err("22003", "value overflows numeric format")
}

// ----------------------------------------------------------------------
// Constants and simple constructors
// ----------------------------------------------------------------------

#[inline]
pub(crate) fn var_special(sign: u16) -> Var {
    Numeric {
        sign,
        weight: 0,
        dscale: 0,
        digits: SmallVec::new(),
    }
}

#[inline]
pub(crate) fn var_zero(dscale: i32) -> Var {
    Numeric {
        sign: NUMERIC_POS,
        weight: 0,
        dscale,
        digits: SmallVec::new(),
    }
}

#[inline]
fn var_small(sign: u16, weight: i32, dscale: i32, digits: &[i16]) -> Var {
    Numeric {
        sign,
        weight,
        dscale,
        digits: SmallVec::from_slice(digits),
    }
}

#[inline]
pub(crate) fn const_one() -> Var {
    var_small(NUMERIC_POS, 0, 0, &[1])
}

#[inline]
fn const_minus_one() -> Var {
    var_small(NUMERIC_NEG, 0, 0, &[1])
}

#[inline]
fn const_two() -> Var {
    var_small(NUMERIC_POS, 0, 0, &[2])
}

#[inline]
fn const_zero_point_nine() -> Var {
    var_small(NUMERIC_POS, -1, 1, &[9000])
}

#[inline]
fn const_one_point_one() -> Var {
    var_small(NUMERIC_POS, 0, 1, &[1, 1000])
}

impl Numeric {
    #[inline]
    pub(crate) fn nd(&self) -> i32 {
        self.digits.len() as i32
    }

    #[inline]
    pub(crate) fn dig(&self, i: i32) -> i32 {
        self.digits[i as usize] as i32
    }

    #[inline]
    pub(crate) fn is_special(&self) -> bool {
        (self.sign & 0xC000) == 0xC000
    }
}

// ----------------------------------------------------------------------
// Normalization, rounding, truncation
// ----------------------------------------------------------------------

/// strip_var: strip leading and trailing zero digits; normalize zero.
pub(crate) fn strip_var(var: &mut Var) {
    let n = var.digits.len();
    let mut lead = 0;
    while lead < n && var.digits[lead] == 0 {
        lead += 1;
    }
    if lead == n {
        var.digits.clear();
        var.sign = NUMERIC_POS;
        var.weight = 0;
        return;
    }
    let mut end = n;
    while var.digits[end - 1] == 0 {
        end -= 1;
    }
    if end < n {
        var.digits.truncate(end);
    }
    if lead > 0 {
        var.digits.drain(0..lead);
        var.weight -= lead as i32;
    }
}

/// round_var: round to rscale decimal digits after the decimal point
/// (rscale may be negative). Sets dscale = rscale. Does not strip.
pub(crate) fn round_var(var: &mut Var, rscale: i32) {
    var.dscale = rscale;

    // decimal digits wanted
    let mut di = (var.weight + 1) * DEC_DIGITS + rscale;

    if di < 0 {
        var.digits.clear();
        var.weight = 0;
        var.sign = NUMERIC_POS;
        return;
    }

    // NBASE digits wanted
    let keep = (di + DEC_DIGITS - 1) / DEC_DIGITS;
    // 0, or number of decimal digits to keep in last NBASE digit
    di %= DEC_DIGITS;

    let cur = var.nd();
    if keep < cur || (keep == cur && di > 0) {
        let mut idx: i32;
        let mut carry: i32;
        if di == 0 {
            carry = if var.dig(keep) >= HALF_NBASE { 1 } else { 0 };
            idx = keep;
        } else {
            // Must round within last NBASE digit
            let mut pow10 = ROUND_POWERS[di as usize];
            idx = keep - 1;
            let d = var.dig(idx);
            let extra = d % pow10;
            let mut nd = d - extra;
            carry = 0;
            if extra >= pow10 / 2 {
                pow10 += nd;
                if pow10 >= NBASE {
                    pow10 -= NBASE;
                    carry = 1;
                }
                nd = pow10;
            }
            var.digits[idx as usize] = nd as i16;
        }
        var.digits.truncate(keep as usize);

        // Propagate carry if needed
        while carry != 0 {
            idx -= 1;
            if idx < 0 {
                // carry into the (zero) spare digit in front
                var.digits.insert(0, 1);
                var.weight += 1;
                break;
            }
            let c = carry + var.dig(idx);
            if c >= NBASE {
                var.digits[idx as usize] = (c - NBASE) as i16;
                carry = 1;
            } else {
                var.digits[idx as usize] = c as i16;
                carry = 0;
            }
        }
    }
}

/// trunc_var: truncate (towards zero) at rscale decimal digits after the
/// decimal point (rscale may be negative). Sets dscale = rscale.
pub(crate) fn trunc_var(var: &mut Var, rscale: i32) {
    var.dscale = rscale;

    let mut di = (var.weight + 1) * DEC_DIGITS + rscale;

    if di <= 0 {
        var.digits.clear();
        var.weight = 0;
        var.sign = NUMERIC_POS;
        return;
    }

    let ndigits = (di + DEC_DIGITS - 1) / DEC_DIGITS;
    if ndigits <= var.nd() {
        var.digits.truncate(ndigits as usize);
        di %= DEC_DIGITS;
        if di > 0 {
            let pow10 = ROUND_POWERS[di as usize];
            let idx = (ndigits - 1) as usize;
            let d = var.digits[idx] as i32;
            var.digits[idx] = (d - d % pow10) as i16;
        }
    }
}

// ----------------------------------------------------------------------
// Comparison
// ----------------------------------------------------------------------

pub(crate) fn cmp_abs_common(d1: &[i16], mut w1: i32, d2: &[i16], mut w2: i32) -> i32 {
    let mut i1 = 0usize;
    let mut i2 = 0usize;

    // Check any digits before the first common digit
    while w1 > w2 && i1 < d1.len() {
        let d = d1[i1];
        i1 += 1;
        if d != 0 {
            return 1;
        }
        w1 -= 1;
    }
    while w2 > w1 && i2 < d2.len() {
        let d = d2[i2];
        i2 += 1;
        if d != 0 {
            return -1;
        }
        w2 -= 1;
    }

    // At this point, either w1 == w2 or we've run out of digits
    if w1 == w2 {
        while i1 < d1.len() && i2 < d2.len() {
            let stat = d1[i1] as i32 - d2[i2] as i32;
            i1 += 1;
            i2 += 1;
            if stat != 0 {
                return if stat > 0 { 1 } else { -1 };
            }
        }
    }

    // Any remaining nonzero digits imply that side is larger
    while i1 < d1.len() {
        let d = d1[i1];
        i1 += 1;
        if d != 0 {
            return 1;
        }
    }
    while i2 < d2.len() {
        let d = d2[i2];
        i2 += 1;
        if d != 0 {
            return -1;
        }
    }
    0
}

#[inline]
pub(crate) fn cmp_abs(var1: &Var, var2: &Var) -> i32 {
    cmp_abs_common(&var1.digits, var1.weight, &var2.digits, var2.weight)
}

/// cmp_var / cmp_var_common: compare two finite values (zeroes must have
/// no digits).
pub(crate) fn cmp_var(var1: &Var, var2: &Var) -> i32 {
    if var1.digits.is_empty() {
        if var2.digits.is_empty() {
            return 0;
        }
        if var2.sign == NUMERIC_NEG {
            return 1;
        }
        return -1;
    }
    if var2.digits.is_empty() {
        if var1.sign == NUMERIC_POS {
            return 1;
        }
        return -1;
    }

    if var1.sign == NUMERIC_POS {
        if var2.sign == NUMERIC_NEG {
            return 1;
        }
        return cmp_abs(var1, var2);
    }

    if var2.sign == NUMERIC_POS {
        return -1;
    }

    cmp_abs(var2, var1)
}

// ----------------------------------------------------------------------
// Addition / subtraction
// ----------------------------------------------------------------------

/// add_abs: |var1| + |var2| (sign of result is left POS; caller sets it).
pub(crate) fn add_abs(var1: &Var, var2: &Var) -> Var {
    add_sub_abs::<false>(var1, var2)
}

/// sub_abs: |var1| - |var2|; requires |var1| >= |var2|.
pub(crate) fn sub_abs(var1: &Var, var2: &Var) -> Var {
    add_sub_abs::<true>(var1, var2)
}

/// The operation is fixed at compile time; each digit loop retains its own
/// carry/borrow arithmetic while sharing alignment and result construction.
fn add_sub_abs<const SUB: bool>(var1: &Var, var2: &Var) -> Var {
    let var1ndigits = var1.nd();
    let var2ndigits = var2.nd();

    let res_weight = if SUB { var1.weight } else { max(var1.weight, var2.weight) + 1 };
    let res_dscale = max(var1.dscale, var2.dscale);

    // Note: here we are figuring rscale in base-NBASE digits
    let rscale1 = var1ndigits - var1.weight - 1;
    let rscale2 = var2ndigits - var2.weight - 1;
    let res_rscale = max(rscale1, rscale2);

    let mut res_ndigits = res_rscale + res_weight + 1;
    if res_ndigits <= 0 {
        res_ndigits = 1;
    }

    let mut res_digits: Digits = smallvec![0; res_ndigits as usize];

    let mut i1 = res_rscale + var1.weight + 1;
    let mut i2 = res_rscale + var2.weight + 1;
    let mut carry = 0i32;
    for i in (0..res_ndigits as usize).rev() {
        i1 -= 1;
        i2 -= 1;
        if i1 >= 0 && i1 < var1ndigits {
            carry += var1.dig(i1);
        }
        if i2 >= 0 && i2 < var2ndigits {
            carry += if SUB { -var2.dig(i2) } else { var2.dig(i2) };
        }
        if SUB && carry < 0 {
            res_digits[i] = (carry + NBASE) as i16;
            carry = -1;
        } else if !SUB && carry >= NBASE {
            res_digits[i] = (carry - NBASE) as i16;
            carry = 1;
        } else {
            res_digits[i] = carry as i16;
            carry = 0;
        }
    }
    debug_assert_eq!(carry, 0);

    let mut result = Numeric {
        sign: NUMERIC_POS,
        weight: res_weight,
        dscale: res_dscale,
        digits: res_digits,
    };
    strip_var(&mut result);
    result
}

/// add_var: full addition with signs.
pub(crate) fn add_var(var1: &Var, var2: &Var) -> Var {
    if var1.sign == NUMERIC_POS {
        if var2.sign == NUMERIC_POS {
            let mut r = add_abs(var1, var2);
            r.sign = NUMERIC_POS;
            r
        } else {
            match cmp_abs(var1, var2) {
                0 => var_zero(max(var1.dscale, var2.dscale)),
                1 => {
                    let mut r = sub_abs(var1, var2);
                    r.sign = NUMERIC_POS;
                    r
                }
                _ => {
                    let mut r = sub_abs(var2, var1);
                    r.sign = NUMERIC_NEG;
                    r
                }
            }
        }
    } else if var2.sign == NUMERIC_POS {
        match cmp_abs(var1, var2) {
            0 => var_zero(max(var1.dscale, var2.dscale)),
            1 => {
                let mut r = sub_abs(var1, var2);
                r.sign = NUMERIC_NEG;
                r
            }
            _ => {
                let mut r = sub_abs(var2, var1);
                r.sign = NUMERIC_POS;
                r
            }
        }
    } else {
        let mut r = add_abs(var1, var2);
        r.sign = NUMERIC_NEG;
        r
    }
}

/// sub_var: full subtraction with signs.
pub(crate) fn sub_var(var1: &Var, var2: &Var) -> Var {
    if var1.sign == NUMERIC_POS {
        if var2.sign == NUMERIC_NEG {
            let mut r = add_abs(var1, var2);
            r.sign = NUMERIC_POS;
            r
        } else {
            match cmp_abs(var1, var2) {
                0 => var_zero(max(var1.dscale, var2.dscale)),
                1 => {
                    let mut r = sub_abs(var1, var2);
                    r.sign = NUMERIC_POS;
                    r
                }
                _ => {
                    let mut r = sub_abs(var2, var1);
                    r.sign = NUMERIC_NEG;
                    r
                }
            }
        }
    } else if var2.sign == NUMERIC_NEG {
        match cmp_abs(var1, var2) {
            0 => var_zero(max(var1.dscale, var2.dscale)),
            1 => {
                let mut r = sub_abs(var1, var2);
                r.sign = NUMERIC_NEG;
                r
            }
            _ => {
                let mut r = sub_abs(var2, var1);
                r.sign = NUMERIC_POS;
                r
            }
        }
    } else {
        let mut r = add_abs(var1, var2);
        r.sign = NUMERIC_NEG;
        r
    }
}

// ----------------------------------------------------------------------
// Multiplication
// ----------------------------------------------------------------------

/// mul_var: product rounded to no more than rscale fractional digits
/// (computation truncated with MUL_GUARD_DIGITS guard digits, as in PG).
pub(crate) fn mul_var(var1: &Var, var2: &Var, rscale: i32) -> Var {
    // Arrange for var1 to be the shorter of the two numbers.
    let (var1, var2) = if var1.nd() > var2.nd() {
        (var2, var1)
    } else {
        (var1, var2)
    };

    let var1ndigits = var1.nd();
    let var2ndigits = var2.nd();

    if var1ndigits == 0 || var2ndigits == 0 {
        return var_zero(rscale);
    }

    let res_sign = if var1.sign == var2.sign {
        NUMERIC_POS
    } else {
        NUMERIC_NEG
    };
    let res_weight = var1.weight + var2.weight + 2;

    let mut res_ndigits = var1ndigits + var2ndigits + 1;
    let maxdigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS + MUL_GUARD_DIGITS;
    res_ndigits = min(res_ndigits, maxdigits);

    if res_ndigits < 3 {
        // All input digits will be ignored; so result is zero
        return var_zero(rscale);
    }

    let rn = res_ndigits as usize;
    let mut dig: SmallVec<[i32; 24]> = smallvec![0; rn];
    let mut maxdig: i32 = 0;

    let v1 = &var1.digits;
    let v2 = &var2.digits;

    let mut i1 = min(var1ndigits - 1, res_ndigits - 3);
    while i1 >= 0 {
        let var1digit = v1[i1 as usize] as i32;
        if var1digit != 0 {
            // Time to normalize?
            maxdig += var1digit;
            if maxdig > (i32::MAX - i32::MAX / NBASE) / (NBASE - 1) {
                let mut carry = 0i32;
                for i in (0..rn).rev() {
                    let mut newdig = dig[i] + carry;
                    if newdig >= NBASE {
                        carry = newdig / NBASE;
                        newdig -= carry * NBASE;
                    } else {
                        carry = 0;
                    }
                    dig[i] = newdig;
                }
                debug_assert_eq!(carry, 0);
                maxdig = 1 + var1digit;
            }

            let i2limit = min(var2ndigits, res_ndigits - i1 - 2) as usize;
            let base = (i1 + 2) as usize;
            let target = &mut dig[base..base + i2limit];
            for (t, &d2) in target.iter_mut().zip(v2[..i2limit].iter()) {
                *t += var1digit * d2 as i32;
            }
        }
        i1 -= 1;
    }

    // Final carry propagation pass, storing into the result.
    let mut res_digits: Digits = smallvec![0; rn];
    let mut carry = 0i32;
    for i in (0..rn).rev() {
        let mut newdig = dig[i] + carry;
        if newdig >= NBASE {
            carry = newdig / NBASE;
            newdig -= carry * NBASE;
        } else {
            carry = 0;
        }
        res_digits[i] = newdig as i16;
    }
    debug_assert_eq!(carry, 0);

    let mut result = Numeric {
        sign: res_sign,
        weight: res_weight,
        dscale: 0,
        digits: res_digits,
    };
    round_var(&mut result, rscale);
    strip_var(&mut result);
    result
}

// ----------------------------------------------------------------------
// Division
// ----------------------------------------------------------------------

/// div_var: quotient figured to exactly rscale fractional digits, rounded
/// if `round`, else truncated (Knuth algorithm D).
pub(crate) fn div_var(var1: &Var, var2: &Var, rscale: i32, round: bool) -> Result<Var> {
    let var1ndigits = var1.nd();
    let var2ndigits = var2.nd();

    if var2ndigits == 0 || var2.digits[0] == 0 {
        return Err(err_division_by_zero());
    }

    if var2ndigits <= 2 {
        let mut idivisor = var2.dig(0);
        let mut idivisor_weight = var2.weight;
        if var2ndigits == 2 {
            idivisor = idivisor * NBASE + var2.dig(1);
            idivisor_weight -= 1;
        }
        if var2.sign == NUMERIC_NEG {
            idivisor = -idivisor;
        }
        return Ok(div_var_int(var1, idivisor, idivisor_weight, rscale, round));
    }
    if var2ndigits <= 4 {
        let mut idivisor = var2.dig(0) as i64;
        let mut idivisor_weight = var2.weight;
        for i in 1..var2ndigits {
            idivisor = idivisor * NBASE as i64 + var2.dig(i) as i64;
            idivisor_weight -= 1;
        }
        if var2.sign == NUMERIC_NEG {
            idivisor = -idivisor;
        }
        return Ok(div_var_int64(
            var1,
            idivisor,
            idivisor_weight,
            rscale,
            round,
        ));
    }

    // Otherwise, perform full long division.
    if var1ndigits == 0 {
        return Ok(var_zero(rscale));
    }

    let res_sign = if var1.sign == var2.sign {
        NUMERIC_POS
    } else {
        NUMERIC_NEG
    };
    let res_weight = var1.weight - var2.weight;
    let mut res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
    res_ndigits = max(res_ndigits, 1);
    if round {
        res_ndigits += 1;
    }

    let mut div_ndigits = res_ndigits + var2ndigits;
    div_ndigits = max(div_ndigits, var1ndigits);

    let mut dividend: Vec<i32> = vec![0; (div_ndigits + 1) as usize];
    let mut divisor: Vec<i32> = vec![0; (var2ndigits + 1) as usize];
    for i in 0..var1ndigits as usize {
        dividend[i + 1] = var1.digits[i] as i32;
    }
    for i in 0..var2ndigits as usize {
        divisor[i + 1] = var2.digits[i] as i32;
    }

    let rn = res_ndigits as usize;
    let mut res_digits: Digits = smallvec![0; rn];

    let v2n = var2ndigits as usize;

    if divisor[1] < HALF_NBASE {
        let d = NBASE / (divisor[1] + 1);

        let mut carry = 0i32;
        for i in (1..=v2n).rev() {
            carry += divisor[i] * d;
            divisor[i] = carry % NBASE;
            carry /= NBASE;
        }
        debug_assert_eq!(carry, 0);
        carry = 0;
        for i in (0..=var1ndigits as usize).rev() {
            carry += dividend[i] * d;
            dividend[i] = carry % NBASE;
            carry /= NBASE;
        }
        debug_assert_eq!(carry, 0);
    }
    let divisor1 = divisor[1];
    let divisor2 = divisor[2];

    for j in 0..rn {
        let next2digits = dividend[j] * NBASE + dividend[j + 1];

        if next2digits == 0 {
            res_digits[j] = 0;
            continue;
        }

        let mut qhat = if dividend[j] == divisor1 {
            NBASE - 1
        } else {
            next2digits / divisor1
        };

        while divisor2 * qhat > (next2digits - qhat * divisor1) * NBASE + dividend[j + 2] {
            qhat -= 1;
        }

        if qhat > 0 {
            let dividend_j = &mut dividend[j..j + v2n + 1];
            let mut borrow = 0i32;
            for i in (0..=v2n).rev() {
                let tmp_result = dividend_j[i] - borrow - divisor[i] * qhat;
                borrow = (NBASE - 1 - tmp_result) / NBASE;
                dividend_j[i] = tmp_result + borrow * NBASE;
            }

            if borrow != 0 {
                qhat -= 1;
                let mut carry = 0i32;
                for i in (0..=v2n).rev() {
                    carry += dividend_j[i] + divisor[i];
                    if carry >= NBASE {
                        dividend_j[i] = carry - NBASE;
                        carry = 1;
                    } else {
                        dividend_j[i] = carry;
                        carry = 0;
                    }
                }
                debug_assert_eq!(carry, 1);
            }
        }

        res_digits[j] = qhat as i16;
    }

    let mut result = Numeric {
        sign: res_sign,
        weight: res_weight,
        dscale: 0,
        digits: res_digits,
    };
    if round {
        round_var(&mut result, rscale);
    } else {
        trunc_var(&mut result, rscale);
    }
    strip_var(&mut result);
    Ok(result)
}

#[inline]
fn fquotient_to_qdigit(fquotient: f64) -> i32 {
    if fquotient >= 0.0 {
        fquotient as i32
    } else {
        (fquotient as i32) - 1
    }
}

/// div_var_fast: the "FM" library division used by transcendental
/// functions; may be inexact in the last digits, exactly like PG's.
pub(crate) fn div_var_fast(var1: &Var, var2: &Var, rscale: i32, round: bool) -> Result<Var> {
    let var1ndigits = var1.nd();
    let var2ndigits = var2.nd();

    if var2ndigits == 0 || var2.digits[0] == 0 {
        return Err(err_division_by_zero());
    }

    if var2ndigits <= 2 {
        let mut idivisor = var2.dig(0);
        let mut idivisor_weight = var2.weight;
        if var2ndigits == 2 {
            idivisor = idivisor * NBASE + var2.dig(1);
            idivisor_weight -= 1;
        }
        if var2.sign == NUMERIC_NEG {
            idivisor = -idivisor;
        }
        return Ok(div_var_int(var1, idivisor, idivisor_weight, rscale, round));
    }
    if var2ndigits <= 4 {
        let mut idivisor = var2.dig(0) as i64;
        let mut idivisor_weight = var2.weight;
        for i in 1..var2ndigits {
            idivisor = idivisor * NBASE as i64 + var2.dig(i) as i64;
            idivisor_weight -= 1;
        }
        if var2.sign == NUMERIC_NEG {
            idivisor = -idivisor;
        }
        return Ok(div_var_int64(
            var1,
            idivisor,
            idivisor_weight,
            rscale,
            round,
        ));
    }

    if var1ndigits == 0 {
        return Ok(var_zero(rscale));
    }

    let res_sign = if var1.sign == var2.sign {
        NUMERIC_POS
    } else {
        NUMERIC_NEG
    };
    let res_weight = var1.weight - var2.weight + 1;
    let mut div_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
    div_ndigits += DIV_GUARD_DIGITS;
    if div_ndigits < DIV_GUARD_DIGITS {
        div_ndigits = DIV_GUARD_DIGITS;
    }

    let dn = div_ndigits as usize;
    let mut div: Vec<i32> = vec![0; dn + 1];
    let load_ndigits = min(div_ndigits, var1ndigits) as usize;
    for i in 0..load_ndigits {
        div[i + 1] = var1.digits[i] as i32;
    }

    let v2 = &var2.digits;
    let mut fdivisor = v2[0] as f64;
    for i in 1..4 {
        fdivisor *= NBASE as f64;
        if i < var2ndigits {
            fdivisor += v2[i as usize] as f64;
        }
    }
    let fdivisorinverse = 1.0 / fdivisor;

    let mut maxdiv: i32 = 1;

    let approx_dividend = |div: &[i32], qi: usize| -> f64 {
        let mut fdividend = div[qi] as f64;
        for i in 1..4usize {
            fdividend *= NBASE as f64;
            if qi + i <= dn {
                fdividend += div[qi + i] as f64;
            }
        }
        fdividend
    };

    let mut qi = 0usize;
    while qi < dn {
        let fdividend = approx_dividend(&div, qi);
        let fquotient = fdividend * fdivisorinverse;
        let mut qdigit = fquotient_to_qdigit(fquotient);

        if qdigit != 0 {
            maxdiv = maxdiv.wrapping_add(qdigit.wrapping_abs());
            if maxdiv > (i32::MAX - i32::MAX / NBASE - 1) / (NBASE - 1) {
                let mut carry = 0i32;
                let mut i = min(qi as i32 + var2ndigits - 2, div_ndigits);
                while i > qi as i32 {
                    let iu = i as usize;
                    let mut newdig = div[iu].wrapping_add(carry);
                    if newdig < 0 {
                        carry = -((-newdig - 1) / NBASE) - 1;
                        newdig = newdig.wrapping_sub(carry.wrapping_mul(NBASE));
                    } else if newdig >= NBASE {
                        carry = newdig / NBASE;
                        newdig -= carry * NBASE;
                    } else {
                        carry = 0;
                    }
                    div[iu] = newdig;
                    i -= 1;
                }
                div[qi] = div[qi].wrapping_add(carry);

                maxdiv = 1;

                let fdividend = approx_dividend(&div, qi);
                let fquotient = fdividend * fdivisorinverse;
                qdigit = fquotient_to_qdigit(fquotient);
                maxdiv = maxdiv.wrapping_add(qdigit.wrapping_abs());
            }

            if qdigit != 0 {
                let istop = min(var2ndigits, div_ndigits - qi as i32 + 1) as usize;
                let q16 = qdigit as i16 as i32;
                let div_qi = &mut div[qi..qi + istop];
                for (dq, &d2) in div_qi.iter_mut().zip(v2[..istop].iter()) {
                    *dq = dq.wrapping_sub(q16.wrapping_mul(d2 as i32));
                }
            }
        }

        div[qi + 1] = div[qi + 1].wrapping_add(div[qi].wrapping_mul(NBASE));
        div[qi] = qdigit;
        qi += 1;
    }

    // Approximate and store the last quotient digit (div[div_ndigits])
    let mut fdividend = div[qi] as f64;
    for _ in 1..4 {
        fdividend *= NBASE as f64;
    }
    let fquotient = fdividend * fdivisorinverse;
    div[qi] = fquotient_to_qdigit(fquotient);

    // Final carry propagation pass to normalize the digits
    let mut res_digits: Digits = smallvec![0; dn + 1];
    let mut carry = 0i32;
    for i in (0..=dn).rev() {
        let mut newdig = div[i].wrapping_add(carry);
        if newdig < 0 {
            carry = -((-newdig - 1) / NBASE) - 1;
            newdig = newdig.wrapping_sub(carry.wrapping_mul(NBASE));
        } else if newdig >= NBASE {
            carry = newdig / NBASE;
            newdig -= carry * NBASE;
        } else {
            carry = 0;
        }
        res_digits[i] = newdig as i16;
    }
    debug_assert_eq!(carry, 0);

    let mut result = Numeric {
        sign: res_sign,
        weight: res_weight,
        dscale: 0,
        digits: res_digits,
    };
    if round {
        round_var(&mut result, rscale);
    } else {
        trunc_var(&mut result, rscale);
    }
    strip_var(&mut result);
    Ok(result)
}

/// div_var_int: divide by ival * NBASE^ival_weight (ival a nonzero 32-bit
/// integer).
pub(crate) fn div_var_int(var: &Var, ival: i32, ival_weight: i32, rscale: i32, round: bool) -> Var {
    div_var_int64(var, i64::from(ival), ival_weight, rscale, round)
}

/// div_var_int64: divide by ival * NBASE^ival_weight (ival a nonzero 64-bit
/// integer).
pub(crate) fn div_var_int64(
    var: &Var,
    ival: i64,
    ival_weight: i32,
    rscale: i32,
    round: bool,
) -> Var {
    debug_assert!(ival != 0);
    let var_ndigits = var.nd();

    if var_ndigits == 0 {
        return var_zero(rscale);
    }

    let res_sign = if var.sign == NUMERIC_POS {
        if ival > 0 { NUMERIC_POS } else { NUMERIC_NEG }
    } else if ival > 0 {
        NUMERIC_NEG
    } else {
        NUMERIC_POS
    };
    let res_weight = var.weight - ival_weight;
    let mut res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
    res_ndigits = max(res_ndigits, 1);
    if round {
        res_ndigits += 1;
    }

    let rn = res_ndigits as usize;
    let vn = var_ndigits as usize;
    let mut res_digits: Digits = smallvec![0; rn];

    let divisor = ival.unsigned_abs();
    // Use the narrowest carry that holds a base-NBASE digit step.
    if divisor <= (u32::MAX / NBASE as u32) as u64 {
        let divisor = divisor as u32;
        let mut carry: u32 = 0;
        for i in 0..rn {
            carry = carry * NBASE as u32 + if i < vn { var.digits[i] as u32 } else { 0 };
            res_digits[i] = (carry / divisor) as i16;
            carry %= divisor;
        }
    } else if divisor <= u64::MAX / NBASE as u64 {
        let mut carry: u64 = 0;
        for i in 0..rn {
            carry = carry * NBASE as u64 + if i < vn { var.digits[i] as u64 } else { 0 };
            res_digits[i] = (carry / divisor) as i16;
            carry %= divisor;
        }
    } else {
        let divisor = divisor as u128;
        let mut carry: u128 = 0;
        for i in 0..rn {
            carry = carry * NBASE as u128 + if i < vn { var.digits[i] as u128 } else { 0 };
            res_digits[i] = (carry / divisor) as i16;
            carry %= divisor;
        }
    }

    let mut result = Numeric {
        sign: res_sign,
        weight: res_weight,
        dscale: 0,
        digits: res_digits,
    };
    if round {
        round_var(&mut result, rscale);
    } else {
        trunc_var(&mut result, rscale);
    }
    strip_var(&mut result);
    result
}

/// select_div_scale: default result scale for division.
pub(crate) fn select_div_scale(var1: &Var, var2: &Var) -> i32 {
    let mut weight1 = 0;
    let mut firstdigit1 = 0i32;
    for i in 0..var1.nd() {
        firstdigit1 = var1.dig(i);
        if firstdigit1 != 0 {
            weight1 = var1.weight - i;
            break;
        }
    }

    let mut weight2 = 0;
    let mut firstdigit2 = 0i32;
    for i in 0..var2.nd() {
        firstdigit2 = var2.dig(i);
        if firstdigit2 != 0 {
            weight2 = var2.weight - i;
            break;
        }
    }

    let mut qweight = weight1 - weight2;
    if firstdigit1 <= firstdigit2 {
        qweight -= 1;
    }

    let mut rscale = NUMERIC_MIN_SIG_DIGITS - qweight * DEC_DIGITS;
    rscale = max(rscale, var1.dscale);
    rscale = max(rscale, var2.dscale);
    rscale = max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
    rscale = min(rscale, NUMERIC_MAX_DISPLAY_SCALE);
    rscale
}

/// mod_var: x - trunc(x/y)*y
pub(crate) fn mod_var(var1: &Var, var2: &Var) -> Result<Var> {
    let tmp = div_var(var1, var2, 0, false)?;
    let tmp = mul_var(var2, &tmp, var2.dscale);
    Ok(sub_var(var1, &tmp))
}

/// div_mod_var: truncated integer quotient and remainder.
pub(crate) fn div_mod_var(var1: &Var, var2: &Var) -> Result<(Var, Var)> {
    let mut q = div_var_fast(var1, var2, 0, false)?;
    let r = mul_var(var2, &q, var2.dscale);
    let mut r = sub_var(var1, &r);
    let one = const_one();

    while !r.digits.is_empty() && r.sign != var1.sign {
        if var1.sign == var2.sign {
            q = sub_var(&q, &one);
            r = add_var(&r, var2);
        } else {
            q = add_var(&q, &one);
            r = sub_var(&r, var2);
        }
    }

    while cmp_abs(&r, var2) >= 0 {
        if var1.sign == var2.sign {
            q = add_var(&q, &one);
            r = sub_var(&r, var2);
        } else {
            q = sub_var(&q, &one);
            r = add_var(&r, var2);
        }
    }

    Ok((q, r))
}

pub(crate) fn ceil_var(var: &Var) -> Var {
    let mut tmp = var.clone();
    trunc_var(&mut tmp, 0);
    if var.sign == NUMERIC_POS && cmp_var(var, &tmp) != 0 {
        tmp = add_var(&tmp, &const_one());
    }
    tmp
}

pub(crate) fn floor_var(var: &Var) -> Var {
    let mut tmp = var.clone();
    trunc_var(&mut tmp, 0);
    if var.sign == NUMERIC_NEG && cmp_var(var, &tmp) != 0 {
        tmp = sub_var(&tmp, &const_one());
    }
    tmp
}

pub(crate) fn gcd_var(var1: &Var, var2: &Var) -> Result<Var> {
    let res_dscale = max(var1.dscale, var2.dscale);

    let cmp = cmp_abs(var1, var2);
    let (var1, var2) = if cmp < 0 { (var2, var1) } else { (var1, var2) };

    if cmp == 0 || var2.digits.is_empty() {
        let mut result = var1.clone();
        result.sign = NUMERIC_POS;
        result.dscale = res_dscale;
        return Ok(result);
    }

    let mut tmp_arg = var1.clone();
    let mut result = var2.clone();
    loop {
        let m = mod_var(&tmp_arg, &result)?;
        if m.digits.is_empty() {
            break;
        }
        tmp_arg = std::mem::replace(&mut result, m);
    }
    result.sign = NUMERIC_POS;
    result.dscale = res_dscale;
    Ok(result)
}

// ----------------------------------------------------------------------
// Integer conversions
// ----------------------------------------------------------------------

pub(crate) fn int64_to_var(val: i64) -> Var {
    int128_to_var(val as i128)
}

pub(crate) fn int128_to_var(val: i128) -> Var {
    if val == 0 {
        return var_zero(0);
    }
    let sign = if val < 0 { NUMERIC_NEG } else { NUMERIC_POS };
    let mut uval = val.unsigned_abs();
    let mut buf = [0i16; 10];
    let mut n = 0;
    while uval != 0 {
        let newuval = uval / NBASE as u128;
        buf[n] = (uval - newuval * NBASE as u128) as i16;
        uval = newuval;
        n += 1;
    }
    let mut digits: Digits = SmallVec::with_capacity(n);
    for i in (0..n).rev() {
        digits.push(buf[i]);
    }
    Numeric {
        sign,
        weight: n as i32 - 1,
        dscale: 0,
        digits,
    }
}

/// numericvar_to_int64: round to nearest integer and convert; None on
/// overflow.
pub(crate) fn var_to_int64(var: &Var) -> Option<i64> {
    let mut rounded = var.clone();
    round_var(&mut rounded, 0);
    strip_var(&mut rounded);
    let ndigits = rounded.nd();
    if ndigits == 0 {
        return Some(0);
    }

    let weight = rounded.weight;
    debug_assert!(weight >= 0 && ndigits <= weight + 1);

    let neg = rounded.sign == NUMERIC_NEG;
    let mut val: i64 = -(rounded.dig(0) as i64);
    for i in 1..=weight {
        val = val.checked_mul(NBASE as i64)?;
        if i < ndigits {
            val = val.checked_sub(rounded.dig(i) as i64)?;
        }
    }

    if !neg {
        if val == i64::MIN {
            return None;
        }
        val = -val;
    }
    Some(val)
}

// ----------------------------------------------------------------------
// Text conversions
// ----------------------------------------------------------------------

#[inline]
pub(crate) fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
}

/// get_str_from_var: text representation using dscale (numeric_out).
pub(crate) fn get_str_from_var(var: &Var) -> String {
    let mut s = String::new();
    write_str_from_var(var, &mut s);
    s
}

pub(crate) fn write_str_from_var(var: &Var, out: &mut String) {
    let dscale = var.dscale;
    let ndigits = var.nd();

    let mut buf: Vec<u8> = Vec::with_capacity(
        (max((var.weight + 1) * DEC_DIGITS, 1) + max(dscale, 0) + DEC_DIGITS + 2) as usize,
    );

    if var.sign == NUMERIC_NEG {
        buf.push(b'-');
    }

    let mut d: i32;
    if var.weight < 0 {
        d = var.weight + 1;
        buf.push(b'0');
    } else {
        d = 0;
        while d <= var.weight {
            let mut dig = if d < ndigits { var.dig(d) } else { 0 };
            let mut putit = d > 0;
            let d1 = dig / 1000;
            dig -= d1 * 1000;
            putit |= d1 > 0;
            if putit {
                buf.push(b'0' + d1 as u8);
            }
            let d1 = dig / 100;
            dig -= d1 * 100;
            putit |= d1 > 0;
            if putit {
                buf.push(b'0' + d1 as u8);
            }
            let d1 = dig / 10;
            dig -= d1 * 10;
            putit |= d1 > 0;
            if putit {
                buf.push(b'0' + d1 as u8);
            }
            buf.push(b'0' + dig as u8);
            d += 1;
        }
    }

    if dscale > 0 {
        buf.push(b'.');
        let endcp = buf.len() + dscale as usize;
        let mut i = 0;
        while i < dscale {
            let mut dig = if d >= 0 && d < ndigits { var.dig(d) } else { 0 };
            let d1 = dig / 1000;
            dig -= d1 * 1000;
            buf.push(b'0' + d1 as u8);
            let d1 = dig / 100;
            dig -= d1 * 100;
            buf.push(b'0' + d1 as u8);
            let d1 = dig / 10;
            dig -= d1 * 10;
            buf.push(b'0' + d1 as u8);
            buf.push(b'0' + dig as u8);
            d += 1;
            i += DEC_DIGITS;
        }
        buf.truncate(endcp);
    }

    // SAFETY-free: buf only contains ASCII.
    out.push_str(std::str::from_utf8(&buf).expect("ascii"));
}

pub(crate) fn invalid_numeric_syntax(orig: &str) -> Error {
    err(
        "22P02",
        format!("invalid input syntax for type numeric: \"{}\"", orig),
    )
}

/// set_var_from_str: parse a decimal number starting at `cp`. Returns the
/// value and the position after the parsed text.
pub(crate) fn set_var_from_str(orig: &str, b: &[u8], mut cp: usize) -> Result<(Var, usize)> {
    let at = |i: usize| -> u8 { if i < b.len() { b[i] } else { 0 } };

    let mut have_dp = false;
    let mut sign = NUMERIC_POS;
    let mut dweight: i32 = -1;
    let mut dscale: i32 = 0;

    match at(cp) {
        b'+' => cp += 1,
        b'-' => {
            sign = NUMERIC_NEG;
            cp += 1;
        }
        _ => {}
    }

    if at(cp) == b'.' {
        have_dp = true;
        cp += 1;
    }

    if !at(cp).is_ascii_digit() {
        return Err(invalid_numeric_syntax(orig));
    }

    let mut decdigits: SmallVec<[u8; 48]> = SmallVec::new();
    decdigits.extend_from_slice(&[0, 0, 0, 0]);

    loop {
        let c = at(cp);
        if c.is_ascii_digit() {
            decdigits.push(c - b'0');
            cp += 1;
            if !have_dp {
                dweight += 1;
            } else {
                dscale += 1;
            }
        } else if c == b'.' {
            if have_dp {
                return Err(invalid_numeric_syntax(orig));
            }
            have_dp = true;
            cp += 1;
            if at(cp) == b'_' {
                return Err(invalid_numeric_syntax(orig));
            }
        } else if c == b'_' {
            cp += 1;
            if !at(cp).is_ascii_digit() {
                return Err(invalid_numeric_syntax(orig));
            }
        } else {
            break;
        }
    }

    let ddigits = decdigits.len() as i32 - DEC_DIGITS;
    decdigits.extend_from_slice(&[0, 0, 0]);

    if at(cp) == b'e' || at(cp) == b'E' {
        let mut exponent: i64 = 0;
        let mut neg = false;

        cp += 1;
        if at(cp) == b'+' {
            cp += 1;
        } else if at(cp) == b'-' {
            neg = true;
            cp += 1;
        }

        if !at(cp).is_ascii_digit() {
            return Err(invalid_numeric_syntax(orig));
        }

        loop {
            let c = at(cp);
            if c.is_ascii_digit() {
                exponent = exponent * 10 + (c - b'0') as i64;
                cp += 1;
                if exponent > (i32::MAX / 2) as i64 {
                    return Err(err_overflow());
                }
            } else if c == b'_' {
                cp += 1;
                if !at(cp).is_ascii_digit() {
                    return Err(invalid_numeric_syntax(orig));
                }
            } else {
                break;
            }
        }

        if neg {
            exponent = -exponent;
        }

        dweight += exponent as i32;
        dscale -= exponent as i32;
        if dscale < 0 {
            dscale = 0;
        }
    }

    let weight = if dweight >= 0 {
        (dweight + 1 + DEC_DIGITS - 1) / DEC_DIGITS - 1
    } else {
        -((-dweight - 1) / DEC_DIGITS + 1)
    };
    let offset = (weight + 1) * DEC_DIGITS - (dweight + 1);
    let ndigits = (ddigits + offset + DEC_DIGITS - 1) / DEC_DIGITS;

    let mut digits: Digits = SmallVec::with_capacity(ndigits as usize);
    let mut i = (DEC_DIGITS - offset) as usize;
    for _ in 0..ndigits {
        let v = ((decdigits[i] as i32 * 10 + decdigits[i + 1] as i32) * 10
            + decdigits[i + 2] as i32)
            * 10
            + decdigits[i + 3] as i32;
        digits.push(v as i16);
        i += DEC_DIGITS as usize;
    }

    let mut v = Numeric {
        sign,
        weight,
        dscale,
        digits,
    };
    strip_var(&mut v);
    Ok((v, cp))
}

/// set_var_from_non_decimal_integer_str: parse digits in base 2/8/16
/// (prefix already consumed).
pub(crate) fn set_var_from_non_decimal_integer_str(
    orig: &str,
    b: &[u8],
    mut cp: usize,
    sign: u16,
    base: i64,
) -> Result<(Var, usize)> {
    let at = |i: usize| -> u8 { if i < b.len() { b[i] } else { 0 } };
    let digit_value = |c: u8| -> Option<i64> {
        match base {
            16 => (c as char).to_digit(16).map(|d| d as i64),
            8 => {
                if (b'0'..=b'7').contains(&c) {
                    Some((c - b'0') as i64)
                } else {
                    None
                }
            }
            _ => {
                if c == b'0' || c == b'1' {
                    Some((c - b'0') as i64)
                } else {
                    None
                }
            }
        }
    };

    let firstdigit = cp;
    let mut dest = var_zero(0);
    let mut tmp: i64 = 0;
    let mut mul: i64 = 1;
    let limit = i64::MAX / base;

    loop {
        let c = at(cp);
        if let Some(d) = digit_value(c) {
            if mul > limit {
                dest = mul_var(&dest, &int64_to_var(mul), 0);
                dest = add_var(&dest, &int64_to_var(tmp));
                if dest.weight > NUMERIC_WEIGHT_MAX {
                    return Err(err_overflow());
                }
                tmp = 0;
                mul = 1;
            }
            tmp = tmp * base + d;
            mul *= base;
            cp += 1;
        } else if c == b'_' {
            cp += 1;
            if digit_value(at(cp)).is_none() {
                return Err(invalid_numeric_syntax(orig));
            }
        } else {
            break;
        }
    }

    if cp == firstdigit {
        return Err(invalid_numeric_syntax(orig));
    }

    dest = mul_var(&dest, &int64_to_var(mul), 0);
    dest = add_var(&dest, &int64_to_var(tmp));
    if dest.weight > NUMERIC_WEIGHT_MAX {
        return Err(err_overflow());
    }
    dest.sign = sign;
    Ok((dest, cp))
}

/// numericvar_to_double_no_overflow: convert via text, ignoring range
/// errors (strtod semantics: overflow gives +/-inf, underflow 0).
pub(crate) fn var_to_double_no_overflow(var: &Var) -> f64 {
    let s = get_str_from_var(var);
    s.parse::<f64>().unwrap_or(0.0)
}

/// make_result: strip, normalize zero, and check that the value fits the
/// storage format (int16 weight, 14-bit dscale).
pub(crate) fn make_result(mut var: Var) -> Result<Numeric> {
    if var.is_special() {
        var.weight = 0;
        var.dscale = 0;
        var.digits.clear();
        return Ok(var);
    }
    strip_var(&mut var);
    if var.weight > NUMERIC_WEIGHT_MAX
        || var.weight < NUMERIC_WEIGHT_MIN
        || var.dscale > NUMERIC_DSCALE_MAX
        || var.dscale < 0
    {
        return Err(err_overflow());
    }
    Ok(var)
}

// ----------------------------------------------------------------------
// Transcendental functions
// ----------------------------------------------------------------------

/// sqrt_var: Karatsuba square root, rounded to rscale (may be negative).
pub(crate) fn sqrt_var(arg: &Var, rscale: i32) -> Result<Var> {
    let stat = cmp_var(arg, &var_zero(0));
    if stat == 0 {
        return Ok(var_zero(rscale));
    }
    if stat < 0 {
        return Err(err("2201F", "cannot take square root of a negative number"));
    }

    let arg_nd = arg.nd();

    // res_weight = floor(arg->weight / 2)
    let res_weight = if arg.weight >= 0 {
        arg.weight / 2
    } else {
        -((-arg.weight - 1) / 2 + 1)
    };

    let mut res_ndigits = if rscale + 1 >= 0 {
        res_weight + 1 + (rscale + DEC_DIGITS) / DEC_DIGITS
    } else {
        res_weight + 1 - (-rscale - 1) / DEC_DIGITS
    };
    res_ndigits = max(res_ndigits, 1);

    let mut src_ndigits = arg.weight + 1 + (res_ndigits - res_weight - 1) * 2;
    src_ndigits = max(src_ndigits, 1);

    let mut ndigits = [0i32; 32];
    let mut step: i32 = 0;
    loop {
        ndigits[step as usize] = src_ndigits;
        if src_ndigits <= 4 {
            break;
        }
        let mut blen = src_ndigits / 4;
        if blen * 4 == src_ndigits && arg.dig(0) < NBASE / 4 {
            blen -= 1;
        }
        src_ndigits -= 2 * blen;
        step += 1;
    }

    // First iteration (innermost square root and remainder)
    let mut arg_int64: i64 = arg.dig(0) as i64;
    let mut src_idx: i32 = 1;
    while src_idx < src_ndigits {
        arg_int64 *= NBASE as i64;
        if src_idx < arg_nd {
            arg_int64 += arg.dig(src_idx) as i64;
        }
        src_idx += 1;
    }

    let mut s_int64: i64 = (arg_int64 as f64).sqrt() as i64;
    let mut r_int64: i64 = arg_int64 - s_int64 * s_int64;

    while r_int64 < 0 || r_int64 > 2 * s_int64 {
        s_int64 = (s_int64 + arg_int64 / s_int64) / 2;
        r_int64 = arg_int64 - s_int64 * s_int64;
    }

    // Iterations with src_ndigits <= 8
    step -= 1;
    while step >= 0 {
        src_ndigits = ndigits[step as usize];
        if src_ndigits > 8 {
            break;
        }
        let blen = (src_ndigits - src_idx) / 2;
        let mut a0: i64 = 0;
        let mut a1: i64 = 0;
        let mut b: i64 = 1;
        for _ in 0..blen {
            b *= NBASE as i64;
            a1 *= NBASE as i64;
            if src_idx < arg_nd {
                a1 += arg.dig(src_idx) as i64;
            }
            src_idx += 1;
        }
        for _ in 0..blen {
            a0 *= NBASE as i64;
            if src_idx < arg_nd {
                a0 += arg.dig(src_idx) as i64;
            }
            src_idx += 1;
        }

        let numer = r_int64 * b + a1;
        let denom = 2 * s_int64;
        let q = numer / denom;
        let u = numer - q * denom;

        s_int64 = s_int64 * b + q;
        r_int64 = u * b + a0 - q * q;

        if r_int64 < 0 {
            r_int64 += s_int64;
            s_int64 -= 1;
            r_int64 += s_int64;
        }
        debug_assert_eq!(src_idx, src_ndigits);
        step -= 1;
    }

    let mut s_var: Var;
    let mut r_var: Var = var_zero(0);

    if step >= 0 {
        let mut s_int128: i128 = s_int64 as i128;
        let mut r_int128: i128 = r_int64 as i128;

        while step >= 0 {
            src_ndigits = ndigits[step as usize];
            if src_ndigits > 16 {
                break;
            }
            let blen = (src_ndigits - src_idx) / 2;
            let mut a0: i64 = 0;
            let mut a1: i64 = 0;
            let mut b: i64 = 1;
            for _ in 0..blen {
                b *= NBASE as i64;
                a1 *= NBASE as i64;
                if src_idx < arg_nd {
                    a1 += arg.dig(src_idx) as i64;
                }
                src_idx += 1;
            }
            for _ in 0..blen {
                a0 *= NBASE as i64;
                if src_idx < arg_nd {
                    a0 += arg.dig(src_idx) as i64;
                }
                src_idx += 1;
            }

            let numer: i128 = r_int128 * b as i128 + a1 as i128;
            let denom: i128 = 2 * s_int128;
            let q = numer / denom;
            let u = numer - q * denom;

            s_int128 = s_int128 * b as i128 + q;
            r_int128 = u * b as i128 + a0 as i128 - q * q;

            if r_int128 < 0 {
                r_int128 += s_int128;
                s_int128 -= 1;
                r_int128 += s_int128;
            }
            debug_assert_eq!(src_idx, src_ndigits);
            step -= 1;
        }

        s_var = int128_to_var(s_int128);
        if step >= 0 {
            r_var = int128_to_var(r_int128);
        }
    } else {
        s_var = int64_to_var(s_int64);
    }

    // Remaining iterations use numeric variables.
    while step >= 0 {
        src_ndigits = ndigits[step as usize];
        let blen = (src_ndigits - src_idx) / 2;

        // Extract a1 and a0
        let a1_var = if src_idx < arg_nd {
            let tmp_len = min(blen, arg_nd - src_idx);
            let mut v = Numeric {
                sign: NUMERIC_POS,
                weight: blen - 1,
                dscale: 0,
                digits: SmallVec::from_slice(
                    &arg.digits[src_idx as usize..(src_idx + tmp_len) as usize],
                ),
            };
            strip_var(&mut v);
            v
        } else {
            var_zero(0)
        };
        src_idx += blen;

        let a0_var = if src_idx < arg_nd {
            let tmp_len = min(blen, arg_nd - src_idx);
            let mut v = Numeric {
                sign: NUMERIC_POS,
                weight: blen - 1,
                dscale: 0,
                digits: SmallVec::from_slice(
                    &arg.digits[src_idx as usize..(src_idx + tmp_len) as usize],
                ),
            };
            strip_var(&mut v);
            v
        } else {
            var_zero(0)
        };
        src_idx += blen;

        // Compute (q,u) = DivRem(r*b + a1, 2*s)
        let mut q_var = r_var.clone();
        q_var.weight += blen;
        let q_var = add_var(&q_var, &a1_var);
        let u_var = add_var(&s_var, &s_var);
        let (q_var, mut u_var) = div_mod_var(&q_var, &u_var)?;

        // Compute s = s*b + q
        s_var.weight += blen;
        s_var = add_var(&s_var, &q_var);

        // Compute r = u*b + a0 - q^2
        u_var.weight += blen;
        let u_var = add_var(&u_var, &a0_var);
        let q_sq = mul_var(&q_var, &q_var, 0);

        if step > 0 {
            r_var = sub_var(&u_var, &q_sq);
            if r_var.sign == NUMERIC_NEG {
                r_var = add_var(&r_var, &s_var);
                s_var = sub_var(&s_var, &const_one());
                r_var = add_var(&r_var, &s_var);
            }
        } else if cmp_var(&u_var, &q_sq) < 0 {
            s_var = sub_var(&s_var, &const_one());
        }

        debug_assert_eq!(src_idx, src_ndigits);
        step -= 1;
    }

    // Construct the final result, rounding it to the requested precision.
    let mut result = s_var;
    result.weight = res_weight;
    result.sign = NUMERIC_POS;
    round_var(&mut result, rscale);
    strip_var(&mut result);
    Ok(result)
}

/// exp_var: e^x to rscale fractional digits.
pub(crate) fn exp_var(arg: &Var, rscale: i32) -> Result<Var> {
    let mut x = arg.clone();

    let mut val = var_to_double_no_overflow(&x);

    // Guard against overflow/underflow
    if val.abs() >= (NUMERIC_MAX_RESULT_SCALE * 3) as f64 {
        if val > 0.0 {
            return Err(err_overflow());
        }
        return Ok(var_zero(rscale));
    }

    // decimal weight = log10(e^x) = x * log10(e)
    let dweight = (val * 0.434294481903252) as i32;

    let ndiv2: i32;
    if val.abs() > 0.01 {
        let mut n = 1;
        val /= 2.0;
        while val.abs() > 0.01 {
            n += 1;
            val /= 2.0;
        }
        ndiv2 = n;
        let local_rscale = x.dscale + ndiv2;
        x = div_var_int(&x, 1 << ndiv2, 0, local_rscale, true);
    } else {
        ndiv2 = 0;
    }

    let mut sig_digits = 1 + dweight + rscale + (ndiv2 as f64 * 0.301029995663981) as i32;
    sig_digits = max(sig_digits, 0) + 8;

    let mut local_rscale = sig_digits - 1;

    // Taylor series
    let mut result = add_var(&const_one(), &x);

    let mut elem = mul_var(&x, &x, local_rscale);
    let mut ni = 2;
    elem = div_var_int(&elem, ni, 0, local_rscale, true);

    while !elem.digits.is_empty() {
        result = add_var(&result, &elem);
        elem = mul_var(&elem, &x, local_rscale);
        ni += 1;
        elem = div_var_int(&elem, ni, 0, local_rscale, true);
    }

    // Compensate for the argument range reduction.
    let mut n = ndiv2;
    while n > 0 {
        n -= 1;
        local_rscale = sig_digits - result.weight * 2 * DEC_DIGITS;
        local_rscale = max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);
        result = mul_var(&result, &result, local_rscale);
    }

    round_var(&mut result, rscale);
    Ok(result)
}

/// estimate_ln_dweight: approximate log10(abs(ln(var))).
pub(crate) fn estimate_ln_dweight(var: &Var) -> i32 {
    if var.sign != NUMERIC_POS {
        return 0;
    }

    if cmp_var(var, &const_zero_point_nine()) >= 0 && cmp_var(var, &const_one_point_one()) <= 0 {
        // 0.9 <= var <= 1.1: ln(1+x) ~= x
        let x = sub_var(var, &const_one());
        if !x.digits.is_empty() {
            x.weight * DEC_DIGITS + (x.dig(0) as f64).log10() as i32
        } else {
            0
        }
    } else if !var.digits.is_empty() {
        let mut digits = var.dig(0);
        let mut dweight = var.weight * DEC_DIGITS;
        if var.nd() > 1 {
            digits = digits * NBASE + var.dig(1);
            dweight -= DEC_DIGITS;
        }
        let ln_var = (digits as f64).ln() + dweight as f64 * 2.302585092994046;
        ln_var.abs().log10() as i32
    } else {
        0
    }
}

/// ln_var: natural logarithm to rscale fractional digits.
pub(crate) fn ln_var(arg: &Var, rscale: i32) -> Result<Var> {
    let cmp = cmp_var(arg, &var_zero(0));
    if cmp == 0 {
        return Err(err("2201E", "cannot take logarithm of zero"));
    } else if cmp < 0 {
        return Err(err("2201E", "cannot take logarithm of a negative number"));
    }

    let mut x = arg.clone();
    let mut fact = const_two();
    let two = const_two();
    let pt9 = const_zero_point_nine();
    let one_pt_one = const_one_point_one();

    let mut nsqrt = 0;
    while cmp_var(&x, &pt9) <= 0 {
        let local_rscale = rscale - x.weight * DEC_DIGITS / 2 + 8;
        x = sqrt_var(&x, local_rscale)?;
        fact = mul_var(&fact, &two, 0);
        nsqrt += 1;
    }
    while cmp_var(&x, &one_pt_one) >= 0 {
        let local_rscale = rscale - x.weight * DEC_DIGITS / 2 + 8;
        x = sqrt_var(&x, local_rscale)?;
        fact = mul_var(&fact, &two, 0);
        nsqrt += 1;
    }

    let local_rscale = rscale + ((nsqrt + 1) as f64 * 0.301029995663981) as i32 + 8;

    let one = const_one();
    let mut result = sub_var(&x, &one);
    let elem = add_var(&x, &one);
    result = div_var_fast(&result, &elem, local_rscale, true)?;
    let mut xx = result.clone();
    let x = mul_var(&result, &result, local_rscale);

    let mut ni = 1;
    loop {
        ni += 2;
        xx = mul_var(&xx, &x, local_rscale);
        let elem = div_var_int(&xx, ni, 0, local_rscale, true);

        if elem.digits.is_empty() {
            break;
        }

        result = add_var(&result, &elem);

        if elem.weight < (result.weight - local_rscale * 2 / DEC_DIGITS) {
            break;
        }
    }

    // Compensate for argument range reduction, round to requested rscale
    Ok(mul_var(&result, &fact, rscale))
}

/// log_var: logarithm of num in the given base; chooses result dscale.
pub(crate) fn log_var(base: &Var, num: &Var) -> Result<Var> {
    let ln_base_dweight = estimate_ln_dweight(base);
    let ln_num_dweight = estimate_ln_dweight(num);
    let result_dweight = ln_num_dweight - ln_base_dweight;

    let mut rscale = NUMERIC_MIN_SIG_DIGITS - result_dweight;
    rscale = max(rscale, base.dscale);
    rscale = max(rscale, num.dscale);
    rscale = max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
    rscale = min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

    let mut ln_base_rscale = rscale + result_dweight - ln_base_dweight + 8;
    ln_base_rscale = max(ln_base_rscale, NUMERIC_MIN_DISPLAY_SCALE);

    let mut ln_num_rscale = rscale + result_dweight - ln_num_dweight + 8;
    ln_num_rscale = max(ln_num_rscale, NUMERIC_MIN_DISPLAY_SCALE);

    let ln_base = ln_var(base, ln_base_rscale)?;
    let ln_num = ln_var(num, ln_num_rscale)?;

    div_var_fast(&ln_num, &ln_base, rscale, true)
}

/// power_var: base ^ exp; chooses result dscale.
pub(crate) fn power_var(base: &Var, exp: &Var) -> Result<Var> {
    // If exp can be represented as an integer, use power_var_int
    if (exp.digits.is_empty() || exp.nd() <= exp.weight + 1)
        && let Some(expval64) = var_to_int64(exp)
        && expval64 >= i32::MIN as i64
        && expval64 <= i32::MAX as i64
    {
        return power_var_int(base, expval64 as i32, exp.dscale);
    }

    // This avoids log(0) for cases of 0 raised to a non-integer.
    if cmp_var(base, &var_zero(0)) == 0 {
        return Ok(var_zero(NUMERIC_MIN_SIG_DIGITS));
    }

    let res_sign;
    let abs_base;
    let base: &Var = if base.sign == NUMERIC_NEG {
        if !exp.digits.is_empty() && exp.nd() > exp.weight + 1 {
            return Err(err(
                "2201F",
                "a negative number raised to a non-integer power yields a complex result",
            ));
        }
        if !exp.digits.is_empty() && exp.nd() == exp.weight + 1 && (exp.dig(exp.nd() - 1) & 1) != 0
        {
            res_sign = NUMERIC_NEG;
        } else {
            res_sign = NUMERIC_POS;
        }
        let mut b = base.clone();
        b.sign = NUMERIC_POS;
        abs_base = b;
        &abs_base
    } else {
        res_sign = NUMERIC_POS;
        base
    };

    let ln_dweight = estimate_ln_dweight(base);

    let mut local_rscale = 8 - ln_dweight;
    local_rscale = max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

    let ln_base = ln_var(base, local_rscale)?;
    let ln_num = mul_var(&ln_base, exp, local_rscale);

    let mut val = var_to_double_no_overflow(&ln_num);

    // initial overflow/underflow test with fuzz factor
    if val.abs() > NUMERIC_MAX_RESULT_SCALE as f64 * 3.01 {
        if val > 0.0 {
            return Err(err_overflow());
        }
        return Ok(var_zero(NUMERIC_MAX_DISPLAY_SCALE));
    }

    val *= 0.434294481903252; // approximate decimal result weight

    let mut rscale = NUMERIC_MIN_SIG_DIGITS - val as i32;
    rscale = max(rscale, base.dscale);
    rscale = max(rscale, exp.dscale);
    rscale = max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
    rscale = min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

    let mut sig_digits = rscale + val as i32;
    sig_digits = max(sig_digits, 0);

    let mut local_rscale = sig_digits - ln_dweight + 8;
    local_rscale = max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

    let ln_base = ln_var(base, local_rscale)?;
    let ln_num = mul_var(&ln_base, exp, local_rscale);

    let mut result = exp_var(&ln_num, rscale)?;

    if res_sign == NUMERIC_NEG && !result.digits.is_empty() {
        result.sign = NUMERIC_NEG;
    }
    Ok(result)
}

/// power_var_int: base ^ exp for integer exp; chooses result dscale.
pub(crate) fn power_var_int(base: &Var, exp: i32, exp_dscale: i32) -> Result<Var> {
    let f: f64 = if !base.digits.is_empty() {
        let mut f = base.dig(0) as f64;
        let mut p = base.weight * DEC_DIGITS;
        let mut i = 1;
        while i < base.nd() && i * DEC_DIGITS < 16 {
            f = f * NBASE as f64 + base.dig(i) as f64;
            p -= DEC_DIGITS;
            i += 1;
        }
        exp as f64 * (f.log10() + p as f64)
    } else {
        0.0
    };

    // overflow/underflow tests with fuzz factors
    if f > ((NUMERIC_WEIGHT_MAX + 1) * DEC_DIGITS) as f64 {
        return Err(err_overflow());
    }
    if f + 1.0 < -(NUMERIC_MAX_DISPLAY_SCALE as f64) {
        return Ok(var_zero(NUMERIC_MAX_DISPLAY_SCALE));
    }

    let mut rscale = NUMERIC_MIN_SIG_DIGITS - f as i32;
    rscale = max(rscale, base.dscale);
    rscale = max(rscale, exp_dscale);
    rscale = max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
    rscale = min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

    match exp {
        0 => {
            let mut result = const_one();
            result.dscale = rscale;
            return Ok(result);
        }
        1 => {
            let mut result = base.clone();
            round_var(&mut result, rscale);
            return Ok(result);
        }
        -1 => {
            return div_var(&const_one(), base, rscale, true);
        }
        2 => {
            return Ok(mul_var(base, base, rscale));
        }
        _ => {}
    }

    if base.digits.is_empty() {
        if exp < 0 {
            return Err(err_division_by_zero());
        }
        return Ok(var_zero(rscale));
    }

    let mut sig_digits = 1 + rscale + f as i32;
    sig_digits += (exp as f64).abs().ln() as i32 + 8;

    let mut neg = exp < 0;
    let mut mask: u32 = exp.unsigned_abs();

    let mut base_prod = base.clone();

    let mut result = if mask & 1 != 0 {
        base.clone()
    } else {
        const_one()
    };

    loop {
        mask >>= 1;
        if mask == 0 {
            break;
        }
        let mut local_rscale = sig_digits - 2 * base_prod.weight * DEC_DIGITS;
        local_rscale = min(local_rscale, 2 * base_prod.dscale);
        local_rscale = max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

        base_prod = mul_var(&base_prod, &base_prod, local_rscale);

        if mask & 1 != 0 {
            let mut local_rscale = sig_digits - (base_prod.weight + result.weight) * DEC_DIGITS;
            local_rscale = min(local_rscale, base_prod.dscale + result.dscale);
            local_rscale = max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

            result = mul_var(&base_prod, &result, local_rscale);
        }

        if base_prod.weight > NUMERIC_WEIGHT_MAX || result.weight > NUMERIC_WEIGHT_MAX {
            // overflow, unless neg, in which case result should be 0
            if !neg {
                return Err(err_overflow());
            }
            let ds = result.dscale;
            result = var_zero(ds);
            neg = false;
            break;
        }
    }

    if neg {
        div_var_fast(&const_one(), &result, rscale, true)
    } else {
        round_var(&mut result, rscale);
        Ok(result)
    }
}

/// power_ten_int: 10^exp exactly (no rounding / overflow checks).
#[allow(dead_code)]
pub(crate) fn power_ten_int(exp: i32) -> Var {
    let mut result = const_one();
    result.dscale = if exp < 0 { -exp } else { 0 };
    result.weight = if exp >= 0 {
        exp / DEC_DIGITS
    } else {
        (exp + 1) / DEC_DIGITS - 1
    };
    let mut e = exp - result.weight * DEC_DIGITS;
    while e > 0 {
        result.digits[0] *= 10;
        e -= 1;
    }
    result
}

pub(crate) fn const_minus_one_var() -> Var {
    const_minus_one()
}
