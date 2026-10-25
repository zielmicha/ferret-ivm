//! `Decimal`: the numeric result of EXTRACT(), plus small numeric helpers
//! that mirror PostgreSQL's numeric.c behaviour for the few operations the
//! date/time code performs on numerics.

use std::fmt;

use crate::Decimal;

impl Decimal {
    /// numeric 'Infinity' (as returned by EXTRACT on infinite inputs).
    pub const INFINITY: Decimal = Decimal {
        value: i128::MAX,
        scale: u32::MAX,
    };
    /// numeric '-Infinity'
    pub const NEG_INFINITY: Decimal = Decimal {
        value: i128::MIN,
        scale: u32::MAX,
    };

    pub const fn new(value: i128, scale: u32) -> Decimal {
        Decimal { value, scale }
    }

    pub const fn from_i64(v: i64) -> Decimal {
        Decimal {
            value: v as i128,
            scale: 0,
        }
    }

    pub fn is_infinite(&self) -> bool {
        self.scale == u32::MAX && (self.value == i128::MAX || self.value == i128::MIN)
    }

    /// Approximate conversion to f64 (infinities map to f64 infinities).
    pub fn to_f64(&self) -> f64 {
        if self.is_infinite() {
            return if self.value > 0 {
                f64::INFINITY
            } else {
                f64::NEG_INFINITY
            };
        }
        // Parse the exact decimal text to get correct rounding.
        self.to_string().parse().unwrap_or(f64::NAN)
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_infinite() {
            return f.write_str(if self.value > 0 {
                "Infinity"
            } else {
                "-Infinity"
            });
        }
        let neg = self.value < 0;
        let mag = self.value.unsigned_abs();
        let digits = mag.to_string();
        let scale = self.scale as usize;
        let mut s = String::with_capacity(digits.len() + scale + 3);
        if neg {
            s.push('-');
        }
        if scale == 0 {
            s.push_str(&digits);
        } else if digits.len() > scale {
            s.push_str(&digits[..digits.len() - scale]);
            s.push('.');
            s.push_str(&digits[digits.len() - scale..]);
        } else {
            s.push('0');
            s.push('.');
            for _ in 0..(scale - digits.len()) {
                s.push('0');
            }
            s.push_str(&digits);
        }
        f.write_str(&s)
    }
}

/// numeric_div() of two non-negative i64 values as PostgreSQL computes it
/// for EXTRACT(JULIAN): result scale from select_div_scale(), rounded half
/// away from zero.  Returns (value, scale).
pub(crate) fn numeric_div_i64(num: i64, den: i64) -> (i128, u32) {
    numeric_div_i128(num as i128, den as i128)
}

/// numeric_div() for integer operands (|num| < 10^21): select_div_scale()
/// result scale, rounded half away from zero.  Returns (value, scale).
pub(crate) fn numeric_div_i128(num: i128, den: i128) -> (i128, u32) {
    fn weight_first(v: i128) -> (i32, i128) {
        if v == 0 {
            return (0, 0);
        }
        let mut w = 0;
        let mut x = v.unsigned_abs();
        while x >= 10000 {
            x /= 10000;
            w += 1;
        }
        (w, x as i128)
    }
    let (weight1, first1) = weight_first(num);
    let (weight2, first2) = weight_first(den);
    let mut qweight = weight1 - weight2;
    if first1 <= first2 {
        qweight -= 1;
    }
    let rscale = (16 - qweight * 4).clamp(0, 1000) as u32;
    // quotient rounded at rscale: (num * 10^rscale) / den, half away from zero
    let n = num * 10i128.pow(rscale);
    let d = den;
    let q = n / d;
    let r = n % d;
    let q = if (r.abs() * 2) >= d.abs() {
        if (n < 0) != (d < 0) { q - 1 } else { q + 1 }
    } else {
        q
    };
    (q, rscale)
}

fn pad_spaces(s: String, min_width: usize) -> String {
    if s.len() >= min_width {
        s
    } else {
        format!("{}{}", " ".repeat(min_width - s.len()), s)
    }
}

/// C printf "%g" with precision 6, optional minimum width with zero padding
/// (as used in "%02g").
pub(crate) fn c_format_g(v: f64, min_width: usize, zero_pad: bool) -> String {
    // PostgreSQL's own snprintf prints NaN/Infinity and never zero-pads them
    if v.is_nan() {
        return pad_spaces("NaN".to_string(), min_width);
    }
    if v.is_infinite() {
        return pad_spaces(
            if v > 0.0 {
                "Infinity".to_string()
            } else {
                "-Infinity".to_string()
            },
            min_width,
        );
    }
    let body = if v == 0.0 {
        if v.is_sign_negative() {
            "-0".to_string()
        } else {
            "0".to_string()
        }
    } else {
        let p: i32 = 6;
        // exponent of v when rounded to p significant digits
        let sci = format!("{:.*e}", (p - 1) as usize, v);
        let (mant, exp) = sci.split_once('e').unwrap();
        let exp: i32 = exp.parse().unwrap();
        if exp < -4 || exp >= p {
            // scientific; strip trailing zeros of mantissa
            let mut m = mant.to_string();
            if m.contains('.') {
                while m.ends_with('0') {
                    m.pop();
                }
                if m.ends_with('.') {
                    m.pop();
                }
            }
            let sign = if exp < 0 { '-' } else { '+' };
            format!("{}e{}{:02}", m, sign, exp.abs())
        } else {
            let decimals = (p - 1 - exp).max(0) as usize;
            let mut s = format!("{:.*}", decimals, v);
            if s.contains('.') {
                while s.ends_with('0') {
                    s.pop();
                }
                if s.ends_with('.') {
                    s.pop();
                }
            }
            s
        }
    };
    if body.len() >= min_width {
        return body;
    }
    let pad = min_width - body.len();
    if zero_pad {
        if let Some(rest) = body.strip_prefix('-') {
            format!("-{}{}", "0".repeat(pad), rest)
        } else {
            format!("{}{}", "0".repeat(pad), body)
        }
    } else {
        format!("{}{}", " ".repeat(pad), body)
    }
}

/// numeric_round(value/10^scale, target) for target <= scale is plain
/// rounding; for target > scale the value is just rescaled.
pub(crate) fn numeric_round_to(value: i128, scale: u32, target: u32) -> (i128, u32) {
    if target >= scale {
        return (value * 10i128.pow(target - scale), target);
    }
    let div = 10i128.pow(scale - target);
    let q = value / div;
    let r = value % div;
    let q = if r.abs() * 2 >= div {
        if value < 0 { q - 1 } else { q + 1 }
    } else {
        q
    };
    (q, target)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_display() {
        assert_eq!(Decimal::new(5000000, 6).to_string(), "5.000000");
        assert_eq!(Decimal::new(-5, 3).to_string(), "-0.005");
        assert_eq!(Decimal::new(2020, 0).to_string(), "2020");
        assert_eq!(Decimal::INFINITY.to_string(), "Infinity");
        assert_eq!(Decimal::NEG_INFINITY.to_string(), "-Infinity");
    }

    #[test]
    fn format_g() {
        assert_eq!(c_format_g(5.0, 2, true), "05");
        assert_eq!(c_format_g(60.5, 2, true), "60.5");
        assert_eq!(c_format_g(1e10, 0, false), "1e+10");
        assert_eq!(c_format_g(123456789.0, 0, false), "1.23457e+08");
        assert_eq!(c_format_g(0.0001, 0, false), "0.0001");
        assert_eq!(c_format_g(0.00001, 0, false), "1e-05");
        assert_eq!(c_format_g(-1.5, 2, true), "-1.5");
    }
}
