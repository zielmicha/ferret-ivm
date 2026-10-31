//! C printf-style formatting used by date/time output.

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

#[cfg(test)]
mod tests {
    use super::*;

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
