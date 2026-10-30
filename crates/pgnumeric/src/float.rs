//! float4/float8 text input/output compatible with PostgreSQL 17
//! (`float8out`/`float4out` with the default extra_float_digits = 1, and
//! `float8in`/`float4in` as built on glibc's `strtod`/`strtof`).

use crate::var::{err, is_space};
use crate::{Error, Result};

/// float8out with the default extra_float_digits = 1 (shortest exact output,
/// e.g. "1e+20", "1.5e-07", "Infinity", "-Infinity", "NaN", "-0").
pub fn format_f64(v: f64) -> String {
    let mut s = String::with_capacity(24);
    crate::ryu::format_f64(v, &mut s);
    s
}

/// float4out with the default extra_float_digits = 1.
pub fn format_f32(v: f32) -> String {
    let mut s = String::with_capacity(16);
    crate::ryu::format_f32(v, &mut s);
    s
}

/// Appends float8out output to `out`.
pub fn write_f64(v: f64, out: &mut String) {
    crate::ryu::format_f64(v, out);
}

/// Appends float4out output to `out`.
pub fn write_f32(v: f32, out: &mut String) {
    crate::ryu::format_f32(v, out);
}

fn invalid_syntax(type_name: &str, orig: &str) -> Error {
    err(
        "22P02",
        format!("invalid input syntax for type {}: \"{}\"", type_name, orig),
    )
}

fn out_of_range(num: &str, type_name: &str) -> Error {
    err(
        "22003",
        format!("\"{}\" is out of range for type {}", num, type_name),
    )
}

/// Result of scanning a strtod-style number.
enum Scan {
    /// decimal number: (consumed length, has nonzero digit)
    Decimal {
        len: usize,
        nonzero: bool,
    },
    /// hexadecimal float: (consumed length, mantissa digits info)
    Hex {
        len: usize,
        mant: u64,
        sticky: bool,
        exp2: i64,
    },
    Inf {
        len: usize,
        neg: bool,
    },
    Nan {
        len: usize,
        neg: bool,
    },
}

/// Scan the longest prefix of `b` that glibc's strtod would accept.
/// Returns None if no conversion could be performed.
fn scan_strtod(b: &[u8]) -> Option<Scan> {
    let at = |i: usize| -> u8 { if i < b.len() { b[i] } else { 0 } };
    let mut i = 0;
    let mut neg = false;
    if at(i) == b'+' || at(i) == b'-' {
        neg = at(i) == b'-';
        i += 1;
    }

    let starts_ci = |pos: usize, pat: &[u8]| -> bool {
        pos + pat.len() <= b.len() && b[pos..pos + pat.len()].eq_ignore_ascii_case(pat)
    };

    if starts_ci(i, b"inf") {
        let len = if starts_ci(i, b"infinity") {
            i + 8
        } else {
            i + 3
        };
        return Some(Scan::Inf { len, neg });
    }
    if starts_ci(i, b"nan") {
        let mut len = i + 3;
        if at(len) == b'(' {
            let mut j = len + 1;
            while at(j).is_ascii_alphanumeric() || at(j) == b'_' {
                j += 1;
            }
            if at(j) == b')' {
                len = j + 1;
            }
        }
        return Some(Scan::Nan { len, neg });
    }

    // Hexadecimal float
    if at(i) == b'0' && (at(i + 1) == b'x' || at(i + 1) == b'X') {
        let mut j = i + 2;
        let mut mant: u64 = 0;
        let mut sticky = false;
        let mut exp2: i64 = 0;
        let mut ndig = 0usize;
        let mut seen_dp = false;
        loop {
            let c = at(j);
            if let Some(d) = (c as char).to_digit(16) {
                ndig += 1;
                if mant >> 60 == 0 {
                    mant = (mant << 4) | d as u64;
                    if seen_dp {
                        exp2 -= 4;
                    }
                } else {
                    // no more room: remember nonzero bits, scale exponent
                    if d != 0 {
                        sticky = true;
                    }
                    if !seen_dp {
                        exp2 += 4;
                    }
                }
                j += 1;
            } else if c == b'.' && !seen_dp {
                seen_dp = true;
                j += 1;
            } else {
                break;
            }
        }
        if ndig > 0 {
            // optional binary exponent
            if at(j) == b'p' || at(j) == b'P' {
                let mut k = j + 1;
                let mut eneg = false;
                if at(k) == b'+' || at(k) == b'-' {
                    eneg = at(k) == b'-';
                    k += 1;
                }
                if at(k).is_ascii_digit() {
                    let mut e: i64 = 0;
                    while at(k).is_ascii_digit() {
                        if e < 1_000_000_000 {
                            e = e * 10 + (at(k) - b'0') as i64;
                        }
                        k += 1;
                    }
                    exp2 += if eneg { -e } else { e };
                    j = k;
                }
            }
            return Some(Scan::Hex {
                len: j,
                mant,
                sticky,
                exp2,
            });
        }
        // "0x" without hex digits: strtod parses just the "0"
        return Some(Scan::Decimal {
            len: i + 1,
            nonzero: false,
        });
    }

    // Decimal float
    let mut j = i;
    let mut ndig = 0;
    let mut nonzero = false;
    while at(j).is_ascii_digit() {
        nonzero |= at(j) != b'0';
        ndig += 1;
        j += 1;
    }
    if at(j) == b'.' {
        j += 1;
        while at(j).is_ascii_digit() {
            nonzero |= at(j) != b'0';
            ndig += 1;
            j += 1;
        }
    }
    if ndig == 0 {
        return None;
    }
    if at(j) == b'e' || at(j) == b'E' {
        let mut k = j + 1;
        if at(k) == b'+' || at(k) == b'-' {
            k += 1;
        }
        if at(k).is_ascii_digit() {
            while at(k).is_ascii_digit() {
                k += 1;
            }
            j = k;
        }
    }
    Some(Scan::Decimal { len: j, nonzero })
}

/// Round a hexadecimal mantissa `mant * 2^exp2` (with sticky bits below) to
/// a binary float with `mant_bits` explicit mantissa bits and exponent bias
/// `bias` (round half to even). Returns (bits without sign, overflowed).
fn round_hex(mant: u64, sticky: bool, exp2: i64, mant_bits: u32, bias: i64) -> (u64, bool) {
    // value = mant * 2^exp2, mant != 0
    let lz = mant.leading_zeros() as i64;
    let m = mant << lz; // top bit set
    let e = exp2 - lz + 63; // unbiased exponent of the leading bit
    let prec = mant_bits as i64 + 1;
    let emin = 1 - bias; // minimum normal exponent
    let emax = bias;

    // number of bits to keep
    let keep = if e >= emin { prec } else { prec - (emin - e) };
    if keep <= 0 {
        // keep == 0 means the value is in [2^(emin-mant_bits-1),
        // 2^(emin-mant_bits)), i.e. between half the smallest subnormal and
        // the smallest subnormal; keep < 0 means it is below half of it.
        if keep == 0 {
            // round half to even: exactly half -> 0 (even), more -> min subnormal
            let rest_nonzero = (m << 1) != 0 || sticky;
            if rest_nonzero {
                return (1, false);
            }
            return (0, false);
        }
        return (0, false);
    }
    let drop = 64 - keep; // >= 64 - prec
    let mut q = m >> drop;
    let rem = m & ((1u64 << drop) - 1);
    let half = 1u64 << (drop - 1);
    // round half to even; sticky bits lie below the lowest bit of `m`
    let round_up = rem > half || (rem == half && (sticky || (q & 1) == 1));
    if round_up {
        q += 1;
    }
    // assemble
    if e >= emin {
        let mut e = e;
        if q >> prec != 0 {
            q >>= 1;
            e += 1;
        }
        if e > emax {
            return (0, true);
        }
        let biased = (e + bias) as u64;
        let frac = q & ((1u64 << mant_bits) - 1);
        ((biased << mant_bits) | frac, false)
    } else {
        // subnormal: q is the raw fraction field (may have rounded up into
        // the smallest normal, which the bit pattern handles naturally)
        (q, false)
    }
}

// Keep decimal parsing at the target precision: parsing as f64 then casting
// to f32 would round twice. Both types otherwise use the same PG input rules.
macro_rules! float_input {
    ($name:ident, $float:ty, $type_name:literal, $mant_bits:literal, $bias:literal) => {
        /// Parse PostgreSQL float input, including hexadecimal numbers and signed NaN.
        pub fn $name(orig: &str) -> Result<$float> {
            let num = orig.trim_start_matches(|c: char| c.is_ascii() && is_space(c as u8));
            let scan =
                scan_strtod(num.as_bytes()).ok_or_else(|| invalid_syntax($type_name, orig))?;
            let (value, len) = match scan {
                Scan::Inf { len, neg } => (
                    if neg {
                        <$float>::NEG_INFINITY
                    } else {
                        <$float>::INFINITY
                    },
                    len,
                ),
                Scan::Nan { len, neg } => (if neg { -<$float>::NAN } else { <$float>::NAN }, len),
                Scan::Decimal { len, nonzero } => {
                    let value: $float = num[..len].parse().unwrap_or(0.0);
                    if value.is_infinite() || (value == 0.0 && nonzero) {
                        return Err(out_of_range(&num[..len], $type_name));
                    }
                    (value, len)
                }
                Scan::Hex {
                    len,
                    mant,
                    sticky,
                    exp2,
                } => {
                    let (bits, overflow) = if mant == 0 {
                        (0, false)
                    } else {
                        round_hex(mant, sticky, exp2, $mant_bits, $bias)
                    };
                    let value = <$float>::from_bits(bits as _);
                    if overflow || (value == 0.0 && mant != 0) {
                        return Err(out_of_range(&num[..len], $type_name));
                    }
                    (if num.starts_with('-') { -value } else { value }, len)
                }
            };
            if !num.as_bytes()[len..].iter().all(|&b| is_space(b)) {
                return Err(invalid_syntax($type_name, orig));
            }
            Ok(value)
        }
    };
}

float_input!(parse_f64, f64, "double precision", 52, 1023);
float_input!(parse_f32, f32, "real", 23, 127);
