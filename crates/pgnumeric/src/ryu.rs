//! Shortest-decimal float output: a port of PostgreSQL's modified Ryu
//! (`src/common/d2s.c`, `src/common/f2s.c`), including PostgreSQL's
//! `STRICTLY_SHORTEST = 0` behaviour (never output the exact midpoint
//! between two representable floats) and its output formatting
//! (fixed-point for decimal exponents in [-4, 15) resp. [-4, 6)).
//!
//! Portions Copyright (c) 2018-2024, PostgreSQL Global Development Group.
//! Originally from github.com/ulfjack/ryu, Copyright 2018 Ulf Adams, used
//! under the Boost Software License 1.0.

#![allow(clippy::manual_is_multiple_of, clippy::needless_late_init)]

use crate::ryu_tables::*;

const DOUBLE_MANTISSA_BITS: u32 = 52;
const DOUBLE_EXPONENT_BITS: u32 = 11;
const DOUBLE_BIAS: i32 = 1023;
const DOUBLE_POW5_INV_BITCOUNT: i32 = 122;
const DOUBLE_POW5_BITCOUNT: i32 = 121;

const FLOAT_MANTISSA_BITS: u32 = 23;
const FLOAT_EXPONENT_BITS: u32 = 8;
const FLOAT_BIAS: i32 = 127;
const FLOAT_POW5_INV_BITCOUNT: i32 = 59;
const FLOAT_POW5_BITCOUNT: i32 = 61;

/// PostgreSQL uses STRICTLY_SHORTEST = 0.
const ACCEPT_BOUNDS: bool = false;

#[inline]
fn pow5bits(e: i32) -> i32 {
    ((((e as u32) * 1217359) >> 19) + 1) as i32
}

#[inline]
fn log10_pow2(e: i32) -> u32 {
    ((e as u32) * 78913) >> 18
}

#[inline]
fn log10_pow5(e: i32) -> u32 {
    ((e as u32) * 732923) >> 20
}

// ----------------------------------------------------------------------
// double
// ----------------------------------------------------------------------

#[inline]
fn pow5_factor_64(mut value: u64) -> u32 {
    let mut count = 0;
    loop {
        let q = value / 5;
        let r = value - 5 * q;
        if r != 0 {
            break;
        }
        value = q;
        count += 1;
    }
    count
}

#[inline]
fn multiple_of_power_of_5_64(value: u64, p: u32) -> bool {
    pow5_factor_64(value) >= p
}

#[inline]
fn multiple_of_power_of_2_64(value: u64, p: u32) -> bool {
    (value & ((1u64 << p) - 1)) == 0
}

#[inline]
fn mul_shift_64(m: u64, mul: &[u64; 2], j: i32) -> u64 {
    let b0 = (m as u128) * (mul[0] as u128);
    let b2 = (m as u128) * (mul[1] as u128);
    (((b0 >> 64) + b2) >> (j - 64)) as u64
}

/// Returns (vr, vp, vm).
#[inline]
fn mul_shift_all_64(m: u64, mul: &[u64; 2], j: i32, mm_shift: u32) -> (u64, u64, u64) {
    let vp = mul_shift_64(4 * m + 2, mul, j);
    let vm = mul_shift_64(4 * m - 1 - mm_shift as u64, mul, j);
    let vr = mul_shift_64(4 * m, mul, j);
    (vr, vp, vm)
}

#[inline]
fn decimal_length_17(v: u64) -> u32 {
    debug_assert!(v < 100000000000000000);
    let mut n = 1;
    let mut p = 10u64;
    while n < 17 && v >= p {
        n += 1;
        p *= 10;
    }
    n
}

struct FloatingDecimal64 {
    mantissa: u64,
    exponent: i32,
}

fn d2d(ieee_mantissa: u64, ieee_exponent: u32) -> FloatingDecimal64 {
    let (e2, m2) = if ieee_exponent == 0 {
        (
            1 - DOUBLE_BIAS - DOUBLE_MANTISSA_BITS as i32 - 2,
            ieee_mantissa,
        )
    } else {
        (
            ieee_exponent as i32 - DOUBLE_BIAS - DOUBLE_MANTISSA_BITS as i32 - 2,
            (1u64 << DOUBLE_MANTISSA_BITS) | ieee_mantissa,
        )
    };

    let accept_bounds = ACCEPT_BOUNDS;

    // Step 2: Determine the interval of legal decimal representations.
    let mv = 4 * m2;
    let mm_shift = (ieee_mantissa != 0 || ieee_exponent <= 1) as u32;

    // Step 3: Convert to a decimal power base using 128-bit arithmetic.
    let mut vr: u64;
    let mut vp: u64;
    let mut vm: u64;
    let e10: i32;
    let mut vm_is_trailing_zeros = false;
    let mut vr_is_trailing_zeros = false;

    if e2 >= 0 {
        let q = log10_pow2(e2) - (e2 > 3) as u32;
        let k = DOUBLE_POW5_INV_BITCOUNT + pow5bits(q as i32) - 1;
        let i = -e2 + q as i32 + k;
        e10 = q as i32;
        (vr, vp, vm) = mul_shift_all_64(m2, &DOUBLE_POW5_INV_SPLIT[q as usize], i, mm_shift);

        if q <= 21 {
            let mv_mod5 = (mv - 5 * (mv / 5)) as u32;
            if mv_mod5 == 0 {
                vr_is_trailing_zeros = multiple_of_power_of_5_64(mv, q);
            } else if accept_bounds {
                vm_is_trailing_zeros = multiple_of_power_of_5_64(mv - 1 - mm_shift as u64, q);
            } else {
                vp -= multiple_of_power_of_5_64(mv + 2, q) as u64;
            }
        }
    } else {
        let q = log10_pow5(-e2) - (-e2 > 1) as u32;
        let i = -e2 - q as i32;
        let k = pow5bits(i) - DOUBLE_POW5_BITCOUNT;
        let j = q as i32 - k;
        e10 = q as i32 + e2;
        (vr, vp, vm) = mul_shift_all_64(m2, &DOUBLE_POW5_SPLIT[i as usize], j, mm_shift);

        if q <= 1 {
            vr_is_trailing_zeros = true;
            if accept_bounds {
                vm_is_trailing_zeros = mm_shift == 1;
            } else {
                vp -= 1;
            }
        } else if q < 63 {
            vr_is_trailing_zeros = multiple_of_power_of_2_64(mv, q - 1);
        }
    }

    // Step 4: Find the shortest decimal representation in the interval.
    let mut removed: i32 = 0;
    let mut last_removed_digit: u8 = 0;
    let output: u64;

    if vm_is_trailing_zeros || vr_is_trailing_zeros {
        loop {
            let vp_div10 = vp / 10;
            let vm_div10 = vm / 10;
            if vp_div10 <= vm_div10 {
                break;
            }
            let vm_mod10 = (vm - 10 * vm_div10) as u32;
            let vr_div10 = vr / 10;
            let vr_mod10 = (vr - 10 * vr_div10) as u32;
            vm_is_trailing_zeros &= vm_mod10 == 0;
            vr_is_trailing_zeros &= last_removed_digit == 0;
            last_removed_digit = vr_mod10 as u8;
            vr = vr_div10;
            vp = vp_div10;
            vm = vm_div10;
            removed += 1;
        }

        if vm_is_trailing_zeros {
            loop {
                let vm_div10 = vm / 10;
                let vm_mod10 = (vm - 10 * vm_div10) as u32;
                if vm_mod10 != 0 {
                    break;
                }
                let vp_div10 = vp / 10;
                let vr_div10 = vr / 10;
                let vr_mod10 = (vr - 10 * vr_div10) as u32;
                vr_is_trailing_zeros &= last_removed_digit == 0;
                last_removed_digit = vr_mod10 as u8;
                vr = vr_div10;
                vp = vp_div10;
                vm = vm_div10;
                removed += 1;
            }
        }

        if vr_is_trailing_zeros && last_removed_digit == 5 && vr % 2 == 0 {
            // Round even if the exact number is .....50..0.
            last_removed_digit = 4;
        }

        output = vr
            + (((vr == vm && (!accept_bounds || !vm_is_trailing_zeros)) || last_removed_digit >= 5)
                as u64);
    } else {
        let mut round_up = false;
        let vp_div100 = vp / 100;
        let vm_div100 = vm / 100;

        if vp_div100 > vm_div100 {
            let vr_div100 = vr / 100;
            let vr_mod100 = (vr - 100 * vr_div100) as u32;
            round_up = vr_mod100 >= 50;
            vr = vr_div100;
            vp = vp_div100;
            vm = vm_div100;
            removed += 2;
        }

        loop {
            let vp_div10 = vp / 10;
            let vm_div10 = vm / 10;
            if vp_div10 <= vm_div10 {
                break;
            }
            let vr_div10 = vr / 10;
            let vr_mod10 = (vr - 10 * vr_div10) as u32;
            round_up = vr_mod10 >= 5;
            vr = vr_div10;
            vp = vp_div10;
            vm = vm_div10;
            removed += 1;
        }

        output = vr + ((vr == vm || round_up) as u64);
    }

    FloatingDecimal64 {
        mantissa: output,
        exponent: e10 + removed,
    }
}

fn d2d_small_int(ieee_mantissa: u64, ieee_exponent: u32) -> Option<FloatingDecimal64> {
    let e2 = ieee_exponent as i32 - DOUBLE_BIAS - DOUBLE_MANTISSA_BITS as i32;
    if e2 >= -(DOUBLE_MANTISSA_BITS as i32) && e2 <= 0 {
        let mask = (1u64 << -e2) - 1;
        let fraction = ieee_mantissa & mask;
        if fraction == 0 {
            let m2 = (1u64 << DOUBLE_MANTISSA_BITS) | ieee_mantissa;
            return Some(FloatingDecimal64 {
                mantissa: m2 >> -e2,
                exponent: 0,
            });
        }
    }
    None
}

/// Format mantissa * 10^exponent the way PostgreSQL's to_chars/to_chars_df
/// do. `fixed_upper` is 15 for double, 6 for float; `exp_digits_min` is
/// the minimum number of exponent digits (2).
fn to_chars(mantissa: u64, exponent: i32, sign: bool, fixed_upper: i32, out: &mut String) {
    let mut output = mantissa;
    let mut olength = decimal_length_17(output) as i32;
    let exp = exponent + olength - 1;

    if sign {
        out.push('-');
    }

    let mut digits_buf = [0u8; 20];
    let fill_digits = |mut v: u64, n: i32, buf: &mut [u8; 20]| {
        for i in (0..n as usize).rev() {
            buf[i] = b'0' + (v % 10) as u8;
            v /= 10;
        }
    };

    if exp >= -4 && exp < fixed_upper {
        // to_chars_df / to_chars_f
        fill_digits(output, olength, &mut digits_buf);
        let digits = std::str::from_utf8(&digits_buf[..olength as usize]).unwrap();
        let nexp = exponent + olength;
        if nexp <= 0 {
            // 0.000ddddd
            out.push_str("0.");
            for _ in 0..(-nexp) {
                out.push('0');
            }
            out.push_str(digits);
        } else if exponent < 0 {
            // dddd.dddd
            out.push_str(&digits[..nexp as usize]);
            out.push('.');
            out.push_str(&digits[nexp as usize..]);
        } else {
            // ddddddddd000000
            out.push_str(digits);
            for _ in 0..exponent {
                out.push('0');
            }
        }
        return;
    }

    // Scientific notation. If we came via the small integer fast path the
    // mantissa may contain trailing decimal zeros; move them into the
    // exponent (the display exponent `exp` does not change).
    if exponent == 0 {
        while output & 1 == 0 {
            let q = output / 10;
            let r = output - 10 * q;
            if r != 0 {
                break;
            }
            output = q;
            olength -= 1;
        }
    }

    fill_digits(output, olength, &mut digits_buf);
    out.push(digits_buf[0] as char);
    if olength > 1 {
        out.push('.');
        out.push_str(std::str::from_utf8(&digits_buf[1..olength as usize]).unwrap());
    }
    out.push('e');
    let mut e = exp;
    if e < 0 {
        out.push('-');
        e = -e;
    } else {
        out.push('+');
    }
    if e < 10 {
        out.push('0');
    }
    out.push_str(&e.to_string());
}

/// double_to_shortest_decimal (float8out with extra_float_digits > 0).
pub(crate) fn format_f64(f: f64, out: &mut String) {
    let bits = f.to_bits();
    let ieee_sign = ((bits >> (DOUBLE_MANTISSA_BITS + DOUBLE_EXPONENT_BITS)) & 1) != 0;
    let ieee_mantissa = bits & ((1u64 << DOUBLE_MANTISSA_BITS) - 1);
    let ieee_exponent =
        ((bits >> DOUBLE_MANTISSA_BITS) & ((1u64 << DOUBLE_EXPONENT_BITS) - 1)) as u32;

    if ieee_exponent == (1u32 << DOUBLE_EXPONENT_BITS) - 1
        || (ieee_exponent == 0 && ieee_mantissa == 0)
    {
        copy_special_str(out, ieee_sign, ieee_exponent != 0, ieee_mantissa != 0);
        return;
    }

    let v = match d2d_small_int(ieee_mantissa, ieee_exponent) {
        Some(v) => v,
        None => d2d(ieee_mantissa, ieee_exponent),
    };
    to_chars(v.mantissa, v.exponent, ieee_sign, 15, out);
}

fn copy_special_str(out: &mut String, sign: bool, exponent: bool, mantissa: bool) {
    if mantissa {
        out.push_str("NaN");
        return;
    }
    if sign {
        out.push('-');
    }
    if exponent {
        out.push_str("Infinity");
    } else {
        out.push('0');
    }
}

// ----------------------------------------------------------------------
// float
// ----------------------------------------------------------------------

#[inline]
fn pow5_factor_32(mut value: u32) -> u32 {
    let mut count = 0;
    loop {
        let q = value / 5;
        let r = value % 5;
        if r != 0 {
            break;
        }
        value = q;
        count += 1;
    }
    count
}

#[inline]
fn multiple_of_power_of_5_32(value: u32, p: u32) -> bool {
    pow5_factor_32(value) >= p
}

#[inline]
fn multiple_of_power_of_2_32(value: u32, p: u32) -> bool {
    (value & ((1u32 << p) - 1)) == 0
}

#[inline]
fn mul_shift_32(m: u32, factor: u64, shift: i32) -> u32 {
    let factor_lo = factor as u32;
    let factor_hi = (factor >> 32) as u32;
    let bits0 = m as u64 * factor_lo as u64;
    let bits1 = m as u64 * factor_hi as u64;
    let sum = (bits0 >> 32) + bits1;
    let shifted_sum = sum >> (shift - 32);
    shifted_sum as u32
}

#[inline]
fn mul_pow5_inv_div_pow2(m: u32, q: u32, j: i32) -> u32 {
    mul_shift_32(m, FLOAT_POW5_INV_SPLIT[q as usize], j)
}

#[inline]
fn mul_pow5_div_pow2(m: u32, i: u32, j: i32) -> u32 {
    mul_shift_32(m, FLOAT_POW5_SPLIT[i as usize], j)
}

struct FloatingDecimal32 {
    mantissa: u32,
    exponent: i32,
}

#[allow(unused_assignments)]
fn f2d(ieee_mantissa: u32, ieee_exponent: u32) -> FloatingDecimal32 {
    let (e2, m2) = if ieee_exponent == 0 {
        (
            1 - FLOAT_BIAS - FLOAT_MANTISSA_BITS as i32 - 2,
            ieee_mantissa,
        )
    } else {
        (
            ieee_exponent as i32 - FLOAT_BIAS - FLOAT_MANTISSA_BITS as i32 - 2,
            (1u32 << FLOAT_MANTISSA_BITS) | ieee_mantissa,
        )
    };

    let accept_bounds = ACCEPT_BOUNDS;

    let mv = 4 * m2;
    let mp = 4 * m2 + 2;
    let mm_shift = (ieee_mantissa != 0 || ieee_exponent <= 1) as u32;
    let mm = 4 * m2 - 1 - mm_shift;

    let mut vr: u32;
    let mut vp: u32;
    let mut vm: u32;
    let e10: i32;
    let mut vm_is_trailing_zeros = false;
    let mut vr_is_trailing_zeros = false;
    let mut last_removed_digit: u8 = 0;

    if e2 >= 0 {
        let q = log10_pow2(e2);
        e10 = q as i32;
        let k = FLOAT_POW5_INV_BITCOUNT + pow5bits(q as i32) - 1;
        let i = -e2 + q as i32 + k;
        vr = mul_pow5_inv_div_pow2(mv, q, i);
        vp = mul_pow5_inv_div_pow2(mp, q, i);
        vm = mul_pow5_inv_div_pow2(mm, q, i);

        if q != 0 && (vp - 1) / 10 <= vm / 10 {
            let l = FLOAT_POW5_INV_BITCOUNT + pow5bits(q as i32 - 1) - 1;
            last_removed_digit =
                (mul_pow5_inv_div_pow2(mv, q - 1, -e2 + q as i32 - 1 + l) % 10) as u8;
        }
        if q <= 9 {
            if mv % 5 == 0 {
                vr_is_trailing_zeros = multiple_of_power_of_5_32(mv, q);
            } else if accept_bounds {
                vm_is_trailing_zeros = multiple_of_power_of_5_32(mm, q);
            } else {
                vp -= multiple_of_power_of_5_32(mp, q) as u32;
            }
        }
    } else {
        let q = log10_pow5(-e2);
        e10 = q as i32 + e2;
        let i = -e2 - q as i32;
        let k = pow5bits(i) - FLOAT_POW5_BITCOUNT;
        let mut j = q as i32 - k;
        vr = mul_pow5_div_pow2(mv, i as u32, j);
        vp = mul_pow5_div_pow2(mp, i as u32, j);
        vm = mul_pow5_div_pow2(mm, i as u32, j);

        if q != 0 && (vp - 1) / 10 <= vm / 10 {
            j = q as i32 - 1 - (pow5bits(i + 1) - FLOAT_POW5_BITCOUNT);
            last_removed_digit = (mul_pow5_div_pow2(mv, (i + 1) as u32, j) % 10) as u8;
        }
        if q <= 1 {
            vr_is_trailing_zeros = true;
            if accept_bounds {
                vm_is_trailing_zeros = mm_shift == 1;
            } else {
                vp -= 1;
            }
        } else if q < 31 {
            vr_is_trailing_zeros = multiple_of_power_of_2_32(mv, q - 1);
        }
    }

    let mut removed: i32 = 0;
    let output: u32;

    if vm_is_trailing_zeros || vr_is_trailing_zeros {
        while vp / 10 > vm / 10 {
            vm_is_trailing_zeros &= vm - (vm / 10) * 10 == 0;
            vr_is_trailing_zeros &= last_removed_digit == 0;
            last_removed_digit = (vr % 10) as u8;
            vr /= 10;
            vp /= 10;
            vm /= 10;
            removed += 1;
        }
        if vm_is_trailing_zeros {
            while vm % 10 == 0 {
                vr_is_trailing_zeros &= last_removed_digit == 0;
                last_removed_digit = (vr % 10) as u8;
                vr /= 10;
                vp /= 10;
                vm /= 10;
                removed += 1;
            }
        }

        if vr_is_trailing_zeros && last_removed_digit == 5 && vr % 2 == 0 {
            last_removed_digit = 4;
        }

        output = vr
            + (((vr == vm && (!accept_bounds || !vm_is_trailing_zeros)) || last_removed_digit >= 5)
                as u32);
    } else {
        while vp / 10 > vm / 10 {
            last_removed_digit = (vr % 10) as u8;
            vr /= 10;
            vp /= 10;
            vm /= 10;
            removed += 1;
        }
        output = vr + ((vr == vm || last_removed_digit >= 5) as u32);
    }

    FloatingDecimal32 {
        mantissa: output,
        exponent: e10 + removed,
    }
}

fn f2d_small_int(ieee_mantissa: u32, ieee_exponent: u32) -> Option<FloatingDecimal32> {
    let e2 = ieee_exponent as i32 - FLOAT_BIAS - FLOAT_MANTISSA_BITS as i32;
    if e2 >= -(FLOAT_MANTISSA_BITS as i32) && e2 <= 0 {
        let mask = (1u32 << -e2) - 1;
        let fraction = ieee_mantissa & mask;
        if fraction == 0 {
            let m2 = (1u32 << FLOAT_MANTISSA_BITS) | ieee_mantissa;
            return Some(FloatingDecimal32 {
                mantissa: m2 >> -e2,
                exponent: 0,
            });
        }
    }
    None
}

/// float_to_shortest_decimal (float4out with extra_float_digits > 0).
pub(crate) fn format_f32(f: f32, out: &mut String) {
    let bits = f.to_bits();
    let ieee_sign = ((bits >> (FLOAT_MANTISSA_BITS + FLOAT_EXPONENT_BITS)) & 1) != 0;
    let ieee_mantissa = bits & ((1u32 << FLOAT_MANTISSA_BITS) - 1);
    let ieee_exponent = (bits >> FLOAT_MANTISSA_BITS) & ((1u32 << FLOAT_EXPONENT_BITS) - 1);

    if ieee_exponent == (1u32 << FLOAT_EXPONENT_BITS) - 1
        || (ieee_exponent == 0 && ieee_mantissa == 0)
    {
        copy_special_str(out, ieee_sign, ieee_exponent != 0, ieee_mantissa != 0);
        return;
    }

    let v = match f2d_small_int(ieee_mantissa, ieee_exponent) {
        Some(v) => v,
        None => f2d(ieee_mantissa, ieee_exponent),
    };
    to_chars(v.mantissa as u64, v.exponent, ieee_sign, 6, out);
}
