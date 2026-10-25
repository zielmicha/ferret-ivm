//! Built-in scalar functions.

use super::func::{Builder, Imp, Ret, Volatility, P};
use super::ops::{as_text, div_zero, jsonb, overflow, range_err, regex, text_array, underflow};
use crate::error::{codes, Result, SqlError};
use crate::types::text::{format_datum, SESSION_TZ};
use crate::types::{ArrayValue, Datum, SqlType};
use pgfuncs::json::{JsonNumber, JsonPath, Jsonb, PathContext, SessionTz};
use pgnumeric::Numeric;
use std::sync::Arc;
use SqlType as T;

fn s(d: &Datum) -> &str {
    d.as_str().expect("text argument")
}
fn i(d: &Datum) -> i64 {
    d.as_i64().expect("integer argument")
}
fn f(d: &Datum) -> f64 {
    d.as_f64().expect("float argument")
}
fn n(d: &Datum) -> &Numeric {
    d.as_numeric().expect("numeric argument")
}
fn txt(v: String) -> Datum {
    Datum::text_owned(v)
}
fn f8(v: f64) -> Result<Datum> {
    Ok(Datum::Float8(v))
}

fn log_err(msg: &str) -> SqlError {
    SqlError::new("2201E", msg)
}

// ---------------------------------------------------------------- math

fn abs_any(a: &Datum) -> Result<Datum> {
    Ok(match a {
        Datum::Int2(x) => Datum::Int2(x.checked_abs().ok_or_else(|| range_err("smallint"))?),
        Datum::Int4(x) => Datum::Int4(x.checked_abs().ok_or_else(|| range_err("integer"))?),
        Datum::Int8(x) => Datum::Int8(x.checked_abs().ok_or_else(|| range_err("bigint"))?),
        Datum::Float4(x) => Datum::Float4(x.abs()),
        Datum::Float8(x) => Datum::Float8(x.abs()),
        Datum::Numeric(x) => Datum::numeric(x.abs()),
        _ => return Err(SqlError::internal("abs")),
    })
}
fn ceil_f(a: &Datum) -> Result<Datum> {
    f8(f(a).ceil())
}
fn ceil_n(a: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(n(a).ceil()))
}
fn floor_f(a: &Datum) -> Result<Datum> {
    f8(f(a).floor())
}
fn floor_n(a: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(n(a).floor()))
}
fn round_f(a: &Datum) -> Result<Datum> {
    f8(f(a).round_ties_even())
}
fn round_n(a: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(n(a).round(0)?))
}
fn round_n2(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(n(a).round(i(b) as i32)?))
}
fn trunc_f(a: &Datum) -> Result<Datum> {
    f8(f(a).trunc())
}
fn trunc_n(a: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(n(a).trunc(0)?))
}
fn trunc_n2(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(n(a).trunc(i(b) as i32)?))
}
fn sign_f(a: &Datum) -> Result<Datum> {
    let x = f(a);
    f8(if x > 0.0 {
        1.0
    } else if x < 0.0 {
        -1.0
    } else {
        0.0
    })
}
fn sign_n(a: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(n(a).sign()))
}
fn sqrt_f(a: &Datum) -> Result<Datum> {
    let x = f(a);
    if x < 0.0 {
        return Err(SqlError::new("2201F", "cannot take square root of a negative number"));
    }
    f8(x.sqrt())
}
fn sqrt_n(a: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(n(a).sqrt()?))
}
fn cbrt_f(a: &Datum) -> Result<Datum> {
    f8(glibc_cbrt(f(a)))
}

/// cbrt() as PostgreSQL computes it with glibc (sysdeps/ieee754/dbl-64/
/// s_cbrt.c, constants verbatim), which is not correctly rounded:
/// cbrt(27) = 3.0000000000000004. Rust's f64::cbrt gives other last digits.
#[allow(clippy::excessive_precision)]
fn glibc_cbrt(x: f64) -> f64 {
    const CBRT2: f64 = 1.2599210498948731648; // 2^(1/3)
    const SQR_CBRT2: f64 = 1.5874010519681994748; // 2^(2/3)
    const FACTOR: [f64; 5] = [1.0 / SQR_CBRT2, 1.0 / CBRT2, 1.0, CBRT2, SQR_CBRT2];
    if x == 0.0 || !x.is_finite() {
        return x + x;
    }
    // frexp: |x| = xm * 2^xe with xm in [0.5, 1)
    let (mut ax, mut adj) = (x.abs(), 0);
    if ax < f64::MIN_POSITIVE {
        ax *= f64::from_bits(0x4350_0000_0000_0000); // 2^54
        adj = -54;
    }
    let bits = ax.to_bits();
    let xe = ((bits >> 52) & 0x7ff) as i32 - 1022 + adj;
    let xm = f64::from_bits((bits & !(0x7ff_u64 << 52)) | (1022_u64 << 52));
    let u = 0.354895765043919860
        + ((1.50819193781584896
            + ((-2.11499494167371287
                + ((2.44693122563534430 + ((-1.83469277483613086 + (0.784932344976639262 - 0.145263899385486377 * xm) * xm) * xm))
                    * xm))
                * xm))
            * xm);
    let t2 = u * u * u;
    let ym = u * (t2 + 2.0 * xm) / (2.0 * t2 + xm) * FACTOR[(2 + xe % 3) as usize];
    // ldexp(±ym, xe / 3); the exponent stays well inside the normal range
    (if x > 0.0 { ym } else { -ym }) * f64::from_bits(((0x3ff + xe / 3) as u64) << 52)
}
fn exp_f(a: &Datum) -> Result<Datum> {
    let x = f(a);
    let r = x.exp();
    if r.is_infinite() && x.is_finite() {
        return Err(overflow());
    }
    if r == 0.0 && x.is_finite() {
        return Err(underflow());
    }
    f8(r)
}
fn exp_n(a: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(n(a).exp()?))
}
fn ln_f(a: &Datum) -> Result<Datum> {
    let x = f(a);
    if x == 0.0 {
        return Err(log_err("cannot take logarithm of zero"));
    }
    if x < 0.0 {
        return Err(log_err("cannot take logarithm of a negative number"));
    }
    f8(x.ln())
}
fn ln_n(a: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(n(a).ln()?))
}
fn log10_f(a: &Datum) -> Result<Datum> {
    let x = f(a);
    if x == 0.0 {
        return Err(log_err("cannot take logarithm of zero"));
    }
    if x < 0.0 {
        return Err(log_err("cannot take logarithm of a negative number"));
    }
    f8(x.log10())
}
fn log10_n(a: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(n(a).log10()?))
}
fn log_n2(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(n(a).log(n(b))?))
}
fn mod_int(a: &Datum, b: &Datum) -> Result<Datum> {
    if i(b) == 0 {
        return Err(div_zero());
    }
    let r = if i(b) == -1 { 0 } else { i(a) % i(b) };
    super::cast::int_to(r, &datum_int_type(a))
}
fn datum_int_type(d: &Datum) -> SqlType {
    match d {
        Datum::Int2(_) => T::Int2,
        Datum::Int4(_) => T::Int4,
        _ => T::Int8,
    }
}
fn mod_n(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(n(a).rem(n(b))?))
}
fn div_n(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(n(a).div_trunc(n(b))?))
}
fn pi(_: &[Datum]) -> Result<Datum> {
    f8(std::f64::consts::PI)
}
fn degrees(a: &Datum) -> Result<Datum> {
    f8(f(a).to_degrees())
}
fn radians(a: &Datum) -> Result<Datum> {
    f8(f(a).to_radians())
}
macro_rules! trig {
    ($name:ident, $e:expr) => {
        fn $name(a: &Datum) -> Result<Datum> {
            let x = f(a);
            let r: f64 = $e(x);
            if r.is_nan() && !x.is_nan() {
                return Err(SqlError::new(codes::NUMERIC_VALUE_OUT_OF_RANGE, "input is out of range"));
            }
            f8(r)
        }
    };
}
trig!(sin, f64::sin);
trig!(cos, f64::cos);
trig!(tan, f64::tan);
trig!(cot, |x: f64| 1.0 / x.tan());
trig!(asin, f64::asin);
trig!(acos, f64::acos);
trig!(atan, f64::atan);
trig!(sinh, f64::sinh);
trig!(cosh, f64::cosh);
trig!(tanh, f64::tanh);
trig!(asinh, f64::asinh);
trig!(acosh, f64::acosh);
trig!(atanh, f64::atanh);
fn atan2(a: &Datum, b: &Datum) -> Result<Datum> {
    f8(f(a).atan2(f(b)))
}
fn gcd_int(a: &Datum, b: &Datum) -> Result<Datum> {
    let (mut x, mut y) = (i(a).unsigned_abs(), i(b).unsigned_abs());
    while y != 0 {
        let t = x % y;
        x = y;
        y = t;
    }
    let ty = datum_int_type(a);
    let v = i64::try_from(x).map_err(|_| range_err(if ty == T::Int4 { "integer" } else { "bigint" }))?;
    super::cast::int_to(v, &ty)
}
fn lcm_int(a: &Datum, b: &Datum) -> Result<Datum> {
    let (x, y) = (i(a), i(b));
    if x == 0 || y == 0 {
        return super::cast::int_to(0, &datum_int_type(a));
    }
    let g = match gcd_int(a, b)? {
        d => d.as_i64().unwrap(),
    };
    let ty = datum_int_type(a);
    let name = if ty == T::Int4 { "integer" } else { "bigint" };
    let v = (x / g).checked_mul(y).map(|v| v.abs()).ok_or_else(|| range_err(name))?;
    super::cast::int_to(v, &ty)
}
fn gcd_n(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(n(a).gcd(n(b))?))
}
fn lcm_n(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(n(a).lcm(n(b))?))
}
fn factorial(a: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(Numeric::factorial(i(a))?))
}
fn pow_f(a: &Datum, b: &Datum) -> Result<Datum> {
    super::ops::float8_pow(a, b)
}
fn pow_n(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(n(a).pow(n(b))?))
}
fn trim_scale(a: &Datum) -> Result<Datum> {
    Ok(Datum::numeric(n(a).normalize()))
}
fn scale(a: &Datum) -> Result<Datum> {
    Ok(n(a).scale().map(Datum::Int4).unwrap_or(Datum::Null))
}
fn min_scale(a: &Datum) -> Result<Datum> {
    Ok(n(a).min_scale().map(Datum::Int4).unwrap_or(Datum::Null))
}
fn width_bucket_args(cnt: i64, nan: bool, infinite_bound: bool) -> Result<()> {
    if cnt <= 0 {
        return Err(SqlError::new("2201G", "count must be greater than zero"));
    }
    if nan {
        return Err(SqlError::new("2201G", "operand, lower bound, and upper bound cannot be NaN"));
    }
    if infinite_bound {
        return Err(SqlError::new("2201G", "lower and upper bounds must be finite"));
    }
    Ok(())
}
fn width_bucket_result(r: i64) -> Result<Datum> {
    i32::try_from(r).map(Datum::Int4).map_err(|_| SqlError::new(codes::NUMERIC_VALUE_OUT_OF_RANGE, "integer out of range"))
}
/// width_bucket_float8
fn width_bucket_f(args: &[Datum]) -> Result<Datum> {
    let (x, lo, hi, cnt) = (f(&args[0]), f(&args[1]), f(&args[2]), i(&args[3]));
    width_bucket_args(cnt, x.is_nan() || lo.is_nan() || hi.is_nan(), lo.is_infinite() || hi.is_infinite())?;
    // count * (a - b) / (c - d) for an operand inside the range; halve the
    // inputs when the bounds' difference overflows
    let inside = |a: f64, b: f64, c: f64, d: f64| {
        let q = if (c - d).is_infinite() { (a / 2.0 - b / 2.0) / (c / 2.0 - d / 2.0) } else { (a - b) / (c - d) };
        // the quotient can round to 1
        ((cnt as f64 * q) as i64).min(cnt - 1) + 1
    };
    let r = if lo < hi {
        if x < lo {
            0
        } else if x >= hi {
            cnt + 1
        } else {
            inside(x, lo, hi, lo)
        }
    } else if lo > hi {
        if x > lo {
            0
        } else if x <= hi {
            cnt + 1
        } else {
            inside(lo, x, lo, hi)
        }
    } else {
        return Err(SqlError::new("2201G", "lower bound cannot equal upper bound"));
    };
    width_bucket_result(r)
}
/// width_bucket_numeric: exact, ((x - lo) * count) / (hi - lo) + 1 truncated
fn width_bucket_n(args: &[Datum]) -> Result<Datum> {
    let (x, lo, hi, cnt) = (n(&args[0]), n(&args[1]), n(&args[2]), i(&args[3]));
    width_bucket_args(cnt, x.is_nan() || lo.is_nan() || hi.is_nan(), lo.is_infinite() || hi.is_infinite())?;
    let r = match lo.cmp(hi) {
        std::cmp::Ordering::Equal => return Err(SqlError::new("2201G", "lower bound cannot equal upper bound")),
        std::cmp::Ordering::Less if x < lo => 0,
        std::cmp::Ordering::Greater if x > lo => 0,
        std::cmp::Ordering::Less if x >= hi => cnt + 1,
        std::cmp::Ordering::Greater if x <= hi => cnt + 1,
        _ => x.sub(lo)?.mul(&Numeric::from_i64(cnt))?.div_trunc(&hi.sub(lo)?)?.to_i64()? + 1,
    };
    width_bucket_result(r)
}
fn random(_: &[Datum]) -> Result<Datum> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
    let mut x = t ^ 0x9E3779B97F4A7C15;
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    f8((x >> 11) as f64 / (1u64 << 53) as f64)
}

// ---------------------------------------------------------------- strings

fn length(a: &Datum) -> Result<Datum> {
    Ok(Datum::Int4(match a {
        Datum::Bytea(b) => b.len() as i32,
        _ => pgfuncs::text::char_length(s(a)),
    }))
}
fn octet_length(a: &Datum) -> Result<Datum> {
    Ok(Datum::Int4(match a {
        Datum::Bytea(b) => b.len() as i32,
        _ => pgfuncs::text::octet_length(s(a)),
    }))
}
fn bit_length(a: &Datum) -> Result<Datum> {
    Ok(Datum::Int4(match a {
        Datum::Bytea(b) => b.len() as i32 * 8,
        _ => pgfuncs::text::bit_length(s(a)),
    }))
}
fn lower(a: &Datum) -> Result<Datum> {
    Ok(txt(pgfuncs::text::lower(s(a))))
}
fn upper(a: &Datum) -> Result<Datum> {
    Ok(txt(pgfuncs::text::upper(s(a))))
}
fn initcap(a: &Datum) -> Result<Datum> {
    Ok(txt(pgfuncs::text::initcap(s(a))))
}
fn substr2(a: &Datum, b: &Datum) -> Result<Datum> {
    match a {
        Datum::Bytea(x) => Ok(Datum::bytea(pgfuncs::text::substr_bytes(x, i(b), None)?)),
        _ => Ok(txt(pgfuncs::text::substr(s(a), i(b), None)?)),
    }
}
fn substr3(args: &[Datum]) -> Result<Datum> {
    match &args[0] {
        Datum::Bytea(x) => Ok(Datum::bytea(pgfuncs::text::substr_bytes(x, i(&args[1]), Some(i(&args[2])))?)),
        a => Ok(txt(pgfuncs::text::substr(s(a), i(&args[1]), Some(i(&args[2])))?)),
    }
}
fn substring_re(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(match pgfuncs::pattern::regex_substring(s(a), s(b))? {
        Some(v) => txt(v),
        None => Datum::Null,
    })
}
fn strpos(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::Int4(pgfuncs::text::strpos(s(a), s(b))))
}
fn overlay3(args: &[Datum]) -> Result<Datum> {
    Ok(txt(pgfuncs::text::overlay(s(&args[0]), s(&args[1]), i(&args[2]), None)?))
}
fn overlay4(args: &[Datum]) -> Result<Datum> {
    Ok(txt(pgfuncs::text::overlay(s(&args[0]), s(&args[1]), i(&args[2]), Some(i(&args[3])))?))
}
fn btrim1(a: &Datum) -> Result<Datum> {
    Ok(txt(pgfuncs::text::btrim(s(a), " ")))
}
fn btrim2(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(txt(pgfuncs::text::btrim(s(a), s(b))))
}
fn ltrim1(a: &Datum) -> Result<Datum> {
    Ok(txt(pgfuncs::text::ltrim(s(a), " ")))
}
fn ltrim2(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(txt(pgfuncs::text::ltrim(s(a), s(b))))
}
fn rtrim1(a: &Datum) -> Result<Datum> {
    Ok(txt(pgfuncs::text::rtrim(s(a), " ")))
}
/// text(bpchar): the cast from character(n), which drops trailing spaces.
fn bpchar_text(a: &Datum) -> Result<Datum> {
    Ok(txt(s(a).trim_end_matches(' ').to_string()))
}
fn rtrim2(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(txt(pgfuncs::text::rtrim(s(a), s(b))))
}
fn replace(args: &[Datum]) -> Result<Datum> {
    Ok(txt(pgfuncs::text::replace(s(&args[0]), s(&args[1]), s(&args[2]))))
}
fn split_part(args: &[Datum]) -> Result<Datum> {
    Ok(txt(pgfuncs::text::split_part(s(&args[0]), s(&args[1]), i(&args[2]) as i32)?))
}
fn left(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(txt(pgfuncs::text::left(s(a), i(b) as i32)))
}
fn right(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(txt(pgfuncs::text::right(s(a), i(b) as i32)))
}
fn lpad2(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(txt(pgfuncs::text::lpad_checked(s(a), i(b) as i32, " ")?))
}
fn lpad3(args: &[Datum]) -> Result<Datum> {
    Ok(txt(pgfuncs::text::lpad_checked(s(&args[0]), i(&args[1]) as i32, s(&args[2]))?))
}
fn rpad2(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(txt(pgfuncs::text::rpad_checked(s(a), i(b) as i32, " ")?))
}
fn rpad3(args: &[Datum]) -> Result<Datum> {
    Ok(txt(pgfuncs::text::rpad_checked(s(&args[0]), i(&args[1]) as i32, s(&args[2]))?))
}
fn repeat(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(txt(pgfuncs::text::repeat(s(a), i(b) as i32)?))
}
fn reverse(a: &Datum) -> Result<Datum> {
    Ok(txt(pgfuncs::text::reverse(s(a))))
}
fn translate(args: &[Datum]) -> Result<Datum> {
    Ok(txt(pgfuncs::text::translate(s(&args[0]), s(&args[1]), s(&args[2]))))
}
fn ascii(a: &Datum) -> Result<Datum> {
    Ok(Datum::Int4(pgfuncs::text::ascii(s(a))))
}
fn chr(a: &Datum) -> Result<Datum> {
    Ok(txt(pgfuncs::text::chr(i(a) as i32)?))
}
fn starts_with(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::Bool(pgfuncs::text::starts_with(s(a), s(b))))
}
fn md5(a: &Datum) -> Result<Datum> {
    Ok(txt(match a {
        Datum::Bytea(b) => pgfuncs::text::md5_hex(b),
        _ => pgfuncs::text::md5_hex(s(a).as_bytes()),
    }))
}
fn sha256(a: &Datum) -> Result<Datum> {
    Ok(Datum::bytea(pgfuncs::text::sha256(a.as_bytes().unwrap())))
}
fn sha224(a: &Datum) -> Result<Datum> {
    Ok(Datum::bytea(pgfuncs::text::sha224(a.as_bytes().unwrap())))
}
fn sha384(a: &Datum) -> Result<Datum> {
    Ok(Datum::bytea(pgfuncs::text::sha384(a.as_bytes().unwrap())))
}
fn sha512(a: &Datum) -> Result<Datum> {
    Ok(Datum::bytea(pgfuncs::text::sha512(a.as_bytes().unwrap())))
}
fn quote_ident(a: &Datum) -> Result<Datum> {
    Ok(txt(pgfuncs::text::quote_ident(s(a))))
}
fn quote_literal(a: &Datum) -> Result<Datum> {
    Ok(txt(pgfuncs::text::quote_literal(&as_text(a))))
}
fn quote_nullable(args: &[Datum]) -> Result<Datum> {
    Ok(if args[0].is_null() { txt("NULL".into()) } else { txt(pgfuncs::text::quote_literal(&as_text(&args[0]))) })
}
/// quote_literal(character(n)) quotes $1::text, which drops the padding.
fn quote_literal_bpchar(a: &Datum) -> Result<Datum> {
    Ok(txt(pgfuncs::text::quote_literal(s(a).trim_end_matches(' '))))
}
fn quote_nullable_bpchar(args: &[Datum]) -> Result<Datum> {
    if args[0].is_null() { Ok(txt("NULL".into())) } else { quote_literal_bpchar(&args[0]) }
}
fn to_hex(a: &Datum) -> Result<Datum> {
    Ok(txt(match a {
        Datum::Int4(v) => pgfuncs::text::to_hex32(*v),
        _ => pgfuncs::text::to_hex(i(a)),
    }))
}
fn concat(args: &[Datum]) -> Result<Datum> {
    let mut out = String::new();
    for a in args {
        if !a.is_null() {
            out.push_str(&as_text(a));
        }
    }
    Ok(txt(out))
}
fn concat_ws(args: &[Datum]) -> Result<Datum> {
    if args[0].is_null() {
        return Ok(Datum::Null);
    }
    let sep = s(&args[0]);
    let parts: Vec<String> = args[1..].iter().filter(|a| !a.is_null()).map(|a| as_text(a).into_owned()).collect();
    Ok(txt(parts.join(sep)))
}
fn format(args: &[Datum]) -> Result<Datum> {
    if args[0].is_null() {
        return Ok(Datum::Null);
    }
    let rest: Vec<Option<String>> =
        args[1..].iter().map(|a| if a.is_null() { None } else { Some(as_text(a).into_owned()) }).collect();
    Ok(txt(pgfuncs::text::format(s(&args[0]), &rest)?))
}
fn text_array_datum(v: Vec<Option<String>>) -> Datum {
    Datum::array(ArrayValue::new_1d(
        T::Text,
        v.into_iter().map(|e| e.map(txt).unwrap_or(Datum::Null)).collect(),
    ))
}
fn string_to_array(args: &[Datum]) -> Result<Datum> {
    if args[0].is_null() {
        return Ok(Datum::Null);
    }
    let delim = args.get(1).and_then(|d| d.as_str());
    let null_str = args.get(2).and_then(|d| d.as_str());
    Ok(text_array_datum(pgfuncs::text::string_to_array(s(&args[0]), delim, null_str)))
}
fn array_to_string(args: &[Datum]) -> Result<Datum> {
    if args[0].is_null() || args[1].is_null() {
        return Ok(Datum::Null);
    }
    let Datum::Array(a) = &args[0] else { return Err(SqlError::internal("array_to_string")) };
    let sep = s(&args[1]);
    let null_str = args.get(2).and_then(|d| d.as_str());
    let mut parts = Vec::new();
    for e in &a.elems {
        if e.is_null() {
            if let Some(ns) = null_str {
                parts.push(ns.to_string());
            }
        } else {
            parts.push(as_text(e).into_owned());
        }
    }
    Ok(txt(parts.join(sep)))
}
fn regexp_replace(args: &[Datum]) -> Result<Datum> {
    let flags = args.get(3).and_then(|d| d.as_str()).unwrap_or("");
    let global = flags.contains('g');
    let fl: String = flags.chars().filter(|c| *c != 'g').collect();
    let re = regex(s(&args[1]), &fl)?;
    Ok(txt(re.replace(s(&args[0]), s(&args[2]), global)))
}
fn regexp_match(args: &[Datum]) -> Result<Datum> {
    let flags = args.get(2).and_then(|d| d.as_str()).unwrap_or("");
    let re = regex(s(&args[1]), flags)?;
    Ok(match re.match_groups(s(&args[0])) {
        Some(g) => text_array_datum(g),
        None => Datum::Null,
    })
}
fn regexp_like(args: &[Datum]) -> Result<Datum> {
    let flags = args.get(2).and_then(|d| d.as_str()).unwrap_or("");
    Ok(Datum::Bool(regex(s(&args[1]), flags)?.is_match(s(&args[0]))))
}
fn regexp_count(args: &[Datum]) -> Result<Datum> {
    let start = args.get(2).map(|d| i(d) as i32).unwrap_or(1);
    let flags = args.get(3).and_then(|d| d.as_str()).unwrap_or("");
    Ok(Datum::Int4(regex(s(&args[1]), flags)?.count(s(&args[0]), start)?))
}
fn regexp_substr(args: &[Datum]) -> Result<Datum> {
    let start = args.get(2).map(|d| i(d) as i32).unwrap_or(1);
    let nth = args.get(3).map(|d| i(d) as i32).unwrap_or(1);
    let flags = args.get(4).and_then(|d| d.as_str()).unwrap_or("");
    let sub = args.get(5).map(|d| i(d) as i32).unwrap_or(0);
    Ok(match regex(s(&args[1]), flags)?.substr(s(&args[0]), start, nth, sub)? {
        Some(v) => txt(v),
        None => Datum::Null,
    })
}
fn regexp_split_to_array(args: &[Datum]) -> Result<Datum> {
    let flags = args.get(2).and_then(|d| d.as_str()).unwrap_or("");
    let parts = regex(s(&args[1]), flags)?.split(s(&args[0]));
    Ok(text_array_datum(parts.into_iter().map(Some).collect()))
}
fn encode(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(txt(pgfuncs::bytea::encode(a.as_bytes().unwrap(), s(b))?))
}
fn decode(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::bytea(pgfuncs::bytea::decode(s(a), s(b))?))
}
fn unistr(a: &Datum) -> Result<Datum> {
    Ok(txt(pgfuncs::text::unistr(s(a))?))
}
fn num_nulls(args: &[Datum]) -> Result<Datum> {
    Ok(Datum::Int4(args.iter().filter(|a| a.is_null()).count() as i32))
}
fn num_nonnulls(args: &[Datum]) -> Result<Datum> {
    Ok(Datum::Int4(args.iter().filter(|a| !a.is_null()).count() as i32))
}
fn gen_random_uuid(_: &[Datum]) -> Result<Datum> {
    let mut b = [0u8; 16];
    for (k, chunk) in b.chunks_mut(8).enumerate() {
        let r = random(&[])?.as_f64().unwrap().to_bits() ^ (k as u64).wrapping_mul(0x9E3779B97F4A7C15);
        chunk.copy_from_slice(&r.to_le_bytes());
    }
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    Ok(Datum::Uuid(b))
}

// ---------------------------------------------------------------- date/time

/// SQL OVERLAPS (PostgreSQL's overlaps_timestamp, including its NULL rules).
fn overlaps(a: &[Datum]) -> Result<Datum> {
    let norm = |s: &Datum, e: &Datum| -> Option<(Datum, Option<Datum>)> {
        // the lesser endpoint first; a single NULL endpoint stays as the end
        match (s.is_null(), e.is_null()) {
            (true, true) => None,
            (true, false) => Some((e.clone(), None)),
            (false, true) => Some((s.clone(), None)),
            (false, false) => {
                if s.sql_cmp(e) == std::cmp::Ordering::Greater {
                    Some((e.clone(), Some(s.clone())))
                } else {
                    Some((s.clone(), Some(e.clone())))
                }
            }
        }
    };
    let (Some((s1, e1)), Some((s2, e2))) = (norm(&a[0], &a[1]), norm(&a[2], &a[3])) else { return Ok(Datum::Null) };
    use std::cmp::Ordering::*;
    let lt = |x: &Datum, y: &Datum| x.sql_cmp(y) == Less;
    Ok(match s1.sql_cmp(&s2) {
        Greater => match (&e2, &e1) {
            (None, _) => Datum::Null,
            (Some(e2), _) if lt(&s1, e2) => Datum::Bool(true),
            (_, None) => Datum::Null,
            _ => Datum::Bool(false),
        },
        Less => match (&e1, &e2) {
            (None, _) => Datum::Null,
            (Some(e1), _) if lt(&s2, e1) => Datum::Bool(true),
            (_, None) => Datum::Null,
            _ => Datum::Bool(false),
        },
        Equal => {
            if e1.is_none() || e2.is_none() {
                Datum::Null
            } else {
                Datum::Bool(true)
            }
        }
    })
}

fn zero_size(_: &Datum) -> Result<Datum> {
    Ok(Datum::Int8(0))
}
fn zero_size2(_: &Datum, _: &Datum) -> Result<Datum> {
    Ok(Datum::Int8(0))
}
/// pg_size_pretty (PostgreSQL 17's unit table and half rounding).
fn size_pretty(a: &Datum) -> Result<Datum> {
    let mut size: i128 = match a {
        Datum::Numeric(n) => n.trunc(0)?.to_i128_exact().unwrap_or(i128::MAX),
        other => other.as_i64().unwrap_or(0) as i128,
    };
    // (name, limit, half-rounded, unit bits)
    const UNITS: [(&str, i128, bool, u32); 6] =
        [("bytes", 10 * 1024, false, 0), ("kB", 20 * 1024 - 1, true, 10), ("MB", 20 * 1024 - 1, true, 20), ("GB", 20 * 1024 - 1, true, 30), ("TB", 20 * 1024 - 1, true, 40), ("PB", 20 * 1024 - 1, true, 50)];
    let half_rounded = |x: i128| (x + if x < 0 { -1 } else { 1 }) / 2;
    for (i, (name, limit, round, bits)) in UNITS.iter().enumerate() {
        if i + 1 == UNITS.len() || size.abs() < *limit {
            if *round {
                size = half_rounded(size);
            }
            return Ok(Datum::text_owned(format!("{size} {name}")));
        }
        // a half-rounded unit carries one extra bit of precision
        let next = UNITS[i + 1];
        size /= 1i128 << ((next.3 - next.2 as u32) - (bits - *round as u32));
    }
    unreachable!()
}

fn always_true1(_: &Datum) -> Result<Datum> {
    Ok(Datum::Bool(true))
}
fn always_false1(_: &Datum) -> Result<Datum> {
    Ok(Datum::Bool(false))
}
fn always_true(_: &[Datum]) -> Result<Datum> {
    Ok(Datum::Bool(true))
}
fn always_false(_: &[Datum]) -> Result<Datum> {
    Ok(Datum::Bool(false))
}
fn null_text(_: &[Datum]) -> Result<Datum> {
    Ok(Datum::Null)
}
fn owner_name(_: &Datum) -> Result<Datum> {
    Ok(Datum::text("ivm"))
}
/// current_setting(name [, missing_ok]): the server's fixed settings (as
/// reported by SHOW and the startup ParameterStatus messages).
fn current_setting(args: &[Datum]) -> Result<Datum> {
    let name = s(&args[0]).to_ascii_lowercase();
    let v = match name.as_str() {
        "server_version" => "17.0",
        "server_version_num" => "170000",
        "server_encoding" | "client_encoding" => "UTF8",
        "timezone" => "UTC",
        "datestyle" => "ISO, MDY",
        "intervalstyle" => "postgres",
        "transaction_isolation" | "default_transaction_isolation" => "serializable",
        "standard_conforming_strings" | "integer_datetimes" | "is_superuser" => "on",
        "transaction_read_only" | "default_transaction_read_only" => "off",
        "search_path" => "\"$user\", public",
        "max_identifier_length" => "63",
        "extra_float_digits" => "1",
        "bytea_output" => "hex",
        "lc_collate" | "lc_ctype" | "lc_messages" | "lc_monetary" | "lc_numeric" | "lc_time" => "C",
        "application_name" => "",
        _ if args.get(1).and_then(|d| d.as_bool()) == Some(true) => return Ok(Datum::Null),
        _ => {
            return Err(SqlError::new(codes::UNDEFINED_OBJECT, format!("unrecognized configuration parameter \"{}\"", s(&args[0]))));
        }
    };
    Ok(txt(v.to_string()))
}
/// set_config(name, value, is_local): settings cannot be changed through
/// expressions; like SET, the call is accepted and returns the value.
fn set_config(args: &[Datum]) -> Result<Datum> {
    Ok(args[1].clone())
}
fn encoding_name(_: &Datum) -> Result<Datum> {
    Ok(Datum::text("UTF8"))
}
fn backend_pid(_: &[Datum]) -> Result<Datum> {
    Ok(Datum::Int4(std::process::id() as i32))
}
fn current_schemas(a: &Datum) -> Result<Datum> {
    let mut v = Vec::new();
    if a.as_bool() == Some(true) {
        v.push(Datum::text("pg_catalog"));
    }
    v.push(Datum::text("public"));
    Ok(Datum::array(ArrayValue::new_1d(T::Name, v)))
}
fn format_type_fn(a: &[Datum]) -> Result<Datum> {
    let Datum::Oid(o) = a[0] else { return Ok(Datum::Null) };
    let typmod = a[1].as_i64().unwrap_or(-1) as i32;
    Ok(match crate::sysrel::format_type_oid(o, typmod) {
        Some(s) => Datum::text_owned(s),
        None => Datum::text("???"),
    })
}

fn grouping_misplaced(_: &[Datum]) -> Result<Datum> {
    Err(SqlError::new(codes::GROUPING_ERROR, "GROUPING is only allowed in queries with GROUP BY"))
}

fn now(_: &[Datum]) -> Result<Datum> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let us = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64;
    Ok(Datum::TimestampTz(us - 946_684_800_000_000))
}
fn current_date(a: &[Datum]) -> Result<Datum> {
    let Datum::TimestampTz(t) = now(a)? else { unreachable!() };
    Ok(Datum::Date(pgdatetime::timestamptz_to_date(t, &SESSION_TZ)?))
}
fn localtimestamp(a: &[Datum]) -> Result<Datum> {
    let Datum::TimestampTz(t) = now(a)? else { unreachable!() };
    Ok(Datum::Timestamp(pgdatetime::timestamptz_to_timestamp(t, &SESSION_TZ)?))
}

fn decimal_to_numeric(d: pgdatetime::Decimal) -> Result<Datum> {
    Ok(Datum::numeric(Numeric::parse(&d.to_string())?))
}
fn extract(a: &Datum, b: &Datum) -> Result<Datum> {
    let field = s(a).to_ascii_lowercase();
    let d = match b {
        Datum::Date(v) => pgdatetime::extract_date_opt(&field, *v)?,
        Datum::Time(v) => pgdatetime::extract_time_opt(&field, *v)?,
        Datum::Timestamp(v) => pgdatetime::extract_timestamp_opt(&field, *v)?,
        Datum::TimestampTz(v) => pgdatetime::extract_timestamptz_opt(&field, *v, &SESSION_TZ)?,
        Datum::Interval(v) => pgdatetime::extract_interval_opt(&field, *v)?,
        _ => return Err(SqlError::internal("extract")),
    };
    match d {
        Some(d) => decimal_to_numeric(d),
        None => Ok(Datum::Null),
    }
}
fn date_part(a: &Datum, b: &Datum) -> Result<Datum> {
    let field = s(a).to_ascii_lowercase();
    let v = match b {
        Datum::Date(v) => pgdatetime::date_part_date_opt(&field, *v)?,
        Datum::Time(v) => pgdatetime::date_part_time_opt(&field, *v)?,
        Datum::Timestamp(v) => pgdatetime::date_part_timestamp_opt(&field, *v)?,
        Datum::TimestampTz(v) => pgdatetime::date_part_timestamptz_opt(&field, *v, &SESSION_TZ)?,
        Datum::Interval(v) => pgdatetime::date_part_interval_opt(&field, *v)?,
        _ => return Err(SqlError::internal("date_part")),
    };
    Ok(v.map(Datum::Float8).unwrap_or(Datum::Null))
}
fn date_trunc(a: &Datum, b: &Datum) -> Result<Datum> {
    let field = s(a).to_ascii_lowercase();
    Ok(match b {
        Datum::Timestamp(v) => Datum::Timestamp(pgdatetime::date_trunc_timestamp(&field, *v)?),
        Datum::TimestampTz(v) => Datum::TimestampTz(pgdatetime::date_trunc_timestamptz(&field, *v, &SESSION_TZ)?),
        Datum::Interval(v) => Datum::Interval(pgdatetime::date_trunc_interval(&field, *v)?),
        _ => return Err(SqlError::internal("date_trunc")),
    })
}
fn date_bin(args: &[Datum]) -> Result<Datum> {
    let stride = args[0].as_interval().unwrap();
    Ok(match (&args[1], &args[2]) {
        (Datum::Timestamp(x), Datum::Timestamp(o)) => Datum::Timestamp(pgdatetime::date_bin(stride, *x, *o)?),
        (Datum::TimestampTz(x), Datum::TimestampTz(o)) => Datum::TimestampTz(pgdatetime::date_bin(stride, *x, *o)?),
        _ => return Err(SqlError::internal("date_bin")),
    })
}
fn age(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::Interval(match (a, b) {
        (Datum::Timestamp(x), Datum::Timestamp(y)) => pgdatetime::age(*x, *y)?,
        (Datum::TimestampTz(x), Datum::TimestampTz(y)) => pgdatetime::age_tz(*x, *y, &SESSION_TZ)?,
        _ => return Err(SqlError::internal("age")),
    }))
}
fn make_date(args: &[Datum]) -> Result<Datum> {
    Ok(Datum::Date(pgdatetime::make_date(i(&args[0]) as i32, i(&args[1]) as i32, i(&args[2]) as i32)?))
}
fn make_time(args: &[Datum]) -> Result<Datum> {
    Ok(Datum::Time(pgdatetime::make_time(i(&args[0]) as i32, i(&args[1]) as i32, f(&args[2]))?))
}
fn make_timestamp(args: &[Datum]) -> Result<Datum> {
    Ok(Datum::Timestamp(pgdatetime::make_timestamp(
        i(&args[0]) as i32,
        i(&args[1]) as i32,
        i(&args[2]) as i32,
        i(&args[3]) as i32,
        i(&args[4]) as i32,
        f(&args[5]),
    )?))
}
fn make_timestamptz(args: &[Datum]) -> Result<Datum> {
    let (y, mo, d, h, mi, sec) =
        (i(&args[0]) as i32, i(&args[1]) as i32, i(&args[2]) as i32, i(&args[3]) as i32, i(&args[4]) as i32, f(&args[5]));
    Ok(Datum::TimestampTz(match args.get(6) {
        Some(z) => pgdatetime::make_timestamptz_at_zone(y, mo, d, h, mi, sec, s(z))?,
        None => pgdatetime::make_timestamptz(y, mo, d, h, mi, sec, &SESSION_TZ)?,
    }))
}
fn make_interval(args: &[Datum]) -> Result<Datum> {
    let g = |k: usize| args.get(k).map(|d| i(d) as i32).unwrap_or(0);
    let secs = args.get(6).map(f).unwrap_or(0.0);
    Ok(Datum::Interval(pgdatetime::make_interval(g(0), g(1), g(2), g(3), g(4), g(5), secs)?))
}
fn to_timestamp_epoch(a: &Datum) -> Result<Datum> {
    Ok(Datum::TimestampTz(pgdatetime::to_timestamp_epoch(f(a))?))
}
fn to_timestamp_fmt(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::TimestampTz(pgdatetime::to_timestamp(s(a), s(b), &SESSION_TZ)?))
}
fn to_date(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(Datum::Date(pgdatetime::to_date(s(a), s(b))?))
}
fn to_char(a: &Datum, b: &Datum) -> Result<Datum> {
    let r = match a {
        Datum::Timestamp(v) => pgdatetime::to_char_timestamp_opt(*v, s(b))?,
        Datum::TimestampTz(v) => pgdatetime::to_char_timestamptz_opt(*v, s(b), &SESSION_TZ)?,
        Datum::Interval(v) => pgdatetime::to_char_interval_opt(*v, s(b))?,
        // PostgreSQL casts date to timestamptz here
        Datum::Date(v) => {
            pgdatetime::to_char_timestamptz_opt(pgdatetime::date_to_timestamptz(*v, &SESSION_TZ)?, s(b), &SESSION_TZ)?
        }
        // numbers: PostgreSQL's per-type paths differ (smallint goes
        // through the double precision overload)
        Datum::Numeric(n) => Some(pgnumeric::to_char_numeric(n, s(b))?),
        Datum::Int8(v) => Some(pgnumeric::to_char_int8(*v, s(b))?),
        Datum::Int4(v) => Some(pgnumeric::to_char_int4(*v, s(b))?),
        Datum::Int2(v) => Some(pgnumeric::to_char_int2(*v, s(b))?),
        Datum::Float8(v) => Some(pgnumeric::to_char_float8(*v, s(b))?),
        Datum::Float4(v) => Some(pgnumeric::to_char_float4(*v, s(b))?),
        _ => return Err(SqlError::unsupported("to_char for this type is not supported")),
    };
    Ok(r.map(txt).unwrap_or(Datum::Null))
}
fn to_number(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(pgnumeric::to_number(s(a), s(b))?.map(Datum::numeric).unwrap_or(Datum::Null))
}
fn justify_hours(a: &Datum) -> Result<Datum> {
    Ok(Datum::Interval(pgdatetime::justify_hours(a.as_interval().unwrap())?))
}
fn justify_days(a: &Datum) -> Result<Datum> {
    Ok(Datum::Interval(pgdatetime::justify_days(a.as_interval().unwrap())?))
}
fn justify_interval(a: &Datum) -> Result<Datum> {
    Ok(Datum::Interval(pgdatetime::justify_interval(a.as_interval().unwrap())?))
}
fn isfinite(a: &Datum) -> Result<Datum> {
    Ok(Datum::Bool(match a {
        Datum::Date(v) => pgdatetime::isfinite_date(*v),
        Datum::Timestamp(v) | Datum::TimestampTz(v) => pgdatetime::isfinite_timestamp(*v),
        Datum::Interval(v) => pgdatetime::isfinite_interval(*v),
        _ => true,
    }))
}
fn timezone(a: &Datum, b: &Datum) -> Result<Datum> {
    let tz = pgdatetime::TimeZone::parse(s(a))?;
    Ok(match b {
        Datum::TimestampTz(v) => Datum::Timestamp(pgdatetime::timezone_tstz(&tz, *v)?),
        Datum::Timestamp(v) => Datum::TimestampTz(pgdatetime::timezone_ts(&tz, *v)?),
        _ => return Err(SqlError::internal("timezone")),
    })
}

// ---------------------------------------------------------------- json

/// to_jsonb conversion of a datum.
pub fn datum_to_jsonb(d: &Datum) -> Result<Jsonb> {
    Ok(match d {
        Datum::Null => Jsonb::Null,
        Datum::Bool(b) => Jsonb::Bool(*b),
        Datum::Int2(_) | Datum::Int4(_) | Datum::Int8(_) | Datum::Oid(_) => {
            Jsonb::Number(JsonNumber::from_i64(d.as_i64().unwrap()))
        }
        Datum::Float4(_) | Datum::Float8(_) => {
            let v = d.as_f64().unwrap();
            match JsonNumber::from_f64(v) {
                Some(n) if v.is_finite() => Jsonb::Number(JsonNumber::parse(&format_datum(d)).unwrap_or(n)),
                _ => Jsonb::String(format_datum(d)),
            }
        }
        Datum::Numeric(n) => {
            if n.is_finite() {
                Jsonb::Number(JsonNumber::parse(&n.to_string())?)
            } else {
                Jsonb::String(n.to_string())
            }
        }
        Datum::Jsonb(j) => (**j).clone(),
        Datum::Json(s) => Jsonb::parse(s)?,
        Datum::Timestamp(_) | Datum::TimestampTz(_) | Datum::Date(_) | Datum::Time(_) => {
            // JSON uses ISO 8601 with a 'T' separator
            let t = format_datum(d);
            let t = if matches!(d, Datum::Timestamp(_) | Datum::TimestampTz(_)) && t.len() > 10 && t.as_bytes()[10] == b' ' {
                format!("{}T{}", &t[..10], &t[11..])
            } else {
                t
            };
            let t = if let Datum::TimestampTz(_) = d { fix_tz_suffix(t) } else { t };
            Jsonb::String(t)
        }
        Datum::Array(a) => {
            let mut v = Vec::with_capacity(a.elems.len());
            for e in &a.elems {
                v.push(datum_to_jsonb(e)?);
            }
            // nest by dimension, innermost first
            for &(_, len) in a.dims.iter().skip(1).rev() {
                let len = len.max(1) as usize;
                let mut it = v.into_iter();
                let mut outer = Vec::new();
                loop {
                    let chunk: Vec<Jsonb> = it.by_ref().take(len).collect();
                    if chunk.is_empty() {
                        break;
                    }
                    outer.push(Jsonb::Array(chunk));
                }
                v = outer;
            }
            Jsonb::Array(v)
        }
        other => Jsonb::String(format_datum(other)),
    })
}

fn fix_tz_suffix(t: String) -> String {
    // "+00" -> "+00:00" as PostgreSQL JSON output does
    let b = t.as_bytes();
    if b.len() >= 3 {
        let tail = &t[t.len() - 3..];
        if (tail.starts_with('+') || tail.starts_with('-')) && tail[1..].chars().all(|c| c.is_ascii_digit()) {
            return format!("{t}:00");
        }
    }
    t
}

fn to_jsonb(a: &Datum) -> Result<Datum> {
    Ok(Datum::jsonb(datum_to_jsonb(a)?))
}
fn to_json(a: &Datum) -> Result<Datum> {
    let j = datum_to_jsonb(a)?;
    Ok(Datum::Json(Arc::from(json_text(&j).as_str())))
}
/// array_to_json(a [, pretty]): pretty puts line feeds between the
/// elements of the outermost dimension
fn array_to_json(args: &[Datum]) -> Result<Datum> {
    let j = datum_to_jsonb(&args[0])?;
    let text = match (&j, args.get(1).and_then(|d| d.as_bool())) {
        (Jsonb::Array(v), Some(true)) => format!("[{}]", v.iter().map(json_text).collect::<Vec<_>>().join(",\n ")),
        _ => json_text(&j),
    };
    Ok(Datum::Json(Arc::from(text.as_str())))
}
/// json (not jsonb) text rendering: compact separators like PG's json output functions.
pub(crate) fn json_text(j: &Jsonb) -> String {
    match j {
        Jsonb::Array(v) => format!("[{}]", v.iter().map(json_text).collect::<Vec<_>>().join(",")),
        Jsonb::Object(kv) => format!(
            "{{{}}}",
            kv.iter()
                .map(|(k, v)| format!("{}:{}", pgfuncs::json::escape_string(k), json_text(v)))
                .collect::<Vec<_>>()
                .join(",")
        ),
        other => other.to_string(),
    }
}
fn build_object_pairs(args: &[Datum]) -> Result<Vec<(String, Jsonb)>> {
    if args.len() % 2 != 0 {
        return Err(SqlError::new(
            codes::INVALID_PARAMETER_VALUE,
            "argument list must have even number of elements",
        )
        .with_hint("The arguments of jsonb_build_object() must consist of alternating keys and values."));
    }
    let mut pairs = Vec::with_capacity(args.len() / 2);
    for (k, chunk) in args.chunks(2).enumerate() {
        if chunk[0].is_null() {
            return Err(SqlError::new(
                codes::NULL_VALUE_NOT_ALLOWED,
                format!("argument {}: key must not be null", 2 * k + 1),
            ));
        }
        pairs.push((as_text(&chunk[0]).into_owned(), datum_to_jsonb(&chunk[1])?));
    }
    Ok(pairs)
}
fn jsonb_build_object(args: &[Datum]) -> Result<Datum> {
    Ok(Datum::jsonb(Jsonb::build_object(build_object_pairs(args)?)))
}
fn json_build_object(args: &[Datum]) -> Result<Datum> {
    let pairs = build_object_pairs(args)?;
    let body = pairs
        .iter()
        .map(|(k, v)| format!("{} : {}", pgfuncs::json::escape_string(k), json_text(v)))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(Datum::Json(Arc::from(format!("{{{body}}}").as_str())))
}
fn jsonb_build_array(args: &[Datum]) -> Result<Datum> {
    let mut v = Vec::with_capacity(args.len());
    for a in args {
        v.push(datum_to_jsonb(a)?);
    }
    Ok(Datum::jsonb(Jsonb::Array(v)))
}
fn json_build_array(args: &[Datum]) -> Result<Datum> {
    let mut v = Vec::with_capacity(args.len());
    for a in args {
        v.push(json_text(&datum_to_jsonb(a)?));
    }
    Ok(Datum::Json(Arc::from(format!("[{}]", v.join(", ")).as_str())))
}
fn jsonb_typeof(a: &Datum) -> Result<Datum> {
    Ok(txt(jsonb(a).type_name().to_string()))
}
fn json_typeof(a: &Datum) -> Result<Datum> {
    Ok(txt(Jsonb::parse(s(a))?.type_name().to_string()))
}
fn jsonb_array_length(a: &Datum) -> Result<Datum> {
    Ok(Datum::Int4(jsonb(a).array_length()?))
}
fn json_array_length(a: &Datum) -> Result<Datum> {
    Ok(Datum::Int4(Jsonb::parse(s(a))?.array_length()?))
}
fn path_args(args: &[Datum]) -> Option<Vec<String>> {
    let mut out = Vec::new();
    for a in args {
        match a {
            Datum::Null => return None,
            Datum::Array(_) => {
                for e in text_array(a) {
                    out.push(e?);
                }
            }
            other => out.push(as_text(other).into_owned()),
        }
    }
    Some(out)
}
fn jsonb_extract_path(args: &[Datum]) -> Result<Datum> {
    if args[0].is_null() {
        return Ok(Datum::Null);
    }
    let Some(p) = path_args(&args[1..]) else { return Ok(Datum::Null) };
    let refs: Vec<&str> = p.iter().map(|s| s.as_str()).collect();
    Ok(match jsonb(&args[0]).get_path(&refs) {
        Some(j) => Datum::jsonb(j.clone()),
        None => Datum::Null,
    })
}
fn jsonb_extract_path_text(args: &[Datum]) -> Result<Datum> {
    if args[0].is_null() {
        return Ok(Datum::Null);
    }
    let Some(p) = path_args(&args[1..]) else { return Ok(Datum::Null) };
    let refs: Vec<&str> = p.iter().map(|s| s.as_str()).collect();
    Ok(match jsonb(&args[0]).get_path(&refs).and_then(|j| j.as_text()) {
        Some(t) => txt(t),
        None => Datum::Null,
    })
}
fn jsonb_set(args: &[Datum]) -> Result<Datum> {
    let Some(p) = path_args(&args[1..2]) else { return Ok(Datum::Null) };
    let refs: Vec<&str> = p.iter().map(|s| s.as_str()).collect();
    let create = args.get(3).and_then(|d| d.as_bool()).unwrap_or(true);
    Ok(Datum::jsonb(jsonb(&args[0]).set(&refs, jsonb(&args[2]).clone(), create)?))
}
fn jsonb_insert(args: &[Datum]) -> Result<Datum> {
    let Some(p) = path_args(&args[1..2]) else { return Ok(Datum::Null) };
    let refs: Vec<&str> = p.iter().map(|s| s.as_str()).collect();
    let after = args.get(3).and_then(|d| d.as_bool()).unwrap_or(false);
    Ok(Datum::jsonb(jsonb(&args[0]).insert(&refs, jsonb(&args[2]).clone(), after)?))
}
// SQL/JSON path. Values of type jsonpath are their canonical text; parsed
// paths are cached per thread.
thread_local! {
    static PATHS: std::cell::RefCell<rustc_hash::FxHashMap<Box<str>, Arc<JsonPath>>> = Default::default();
}

/// The parsed path of a jsonpath datum.
pub fn jsonpath(d: &Datum) -> Result<Arc<JsonPath>> {
    let s = d.as_str().ok_or_else(|| SqlError::internal("jsonpath argument is not text"))?;
    PATHS.with(|c| {
        if let Some(p) = c.borrow().get(s) {
            return Ok(p.clone());
        }
        let p = Arc::new(JsonPath::parse(s)?);
        let mut c = c.borrow_mut();
        if c.len() >= 4096 {
            c.clear();
        }
        c.insert(s.into(), p.clone());
        Ok(p)
    })
}

/// Evaluation context of the jsonb_path_* functions (`tz`: the _tz variants;
/// the session time zone is always UTC, so these are deterministic too).
pub fn path_context<T>(tz: bool, f: impl FnOnce(&PathContext) -> T) -> T {
    let session = SessionTz::default();
    f(&PathContext::new(&session, tz))
}

/// (target, path, vars, silent)
fn jsonpath_args(args: &[Datum]) -> Result<(&Jsonb, Arc<JsonPath>, &Jsonb, bool)> {
    Ok((jsonb(&args[0]), jsonpath(&args[1])?, jsonb(&args[2]), args[3].as_bool() == Some(true)))
}
fn opt_bool(b: Option<bool>) -> Datum {
    b.map(Datum::Bool).unwrap_or(Datum::Null)
}
fn path_exists_impl(tz: bool, args: &[Datum]) -> Result<Datum> {
    let (doc, path, vars, silent) = jsonpath_args(args)?;
    Ok(opt_bool(path_context(tz, |c| c.exists(doc, &path, vars, silent))?))
}
fn path_match_impl(tz: bool, args: &[Datum]) -> Result<Datum> {
    let (doc, path, vars, silent) = jsonpath_args(args)?;
    Ok(opt_bool(path_context(tz, |c| c.matches(doc, &path, vars, silent))?))
}
fn path_query_array_impl(tz: bool, args: &[Datum]) -> Result<Datum> {
    let (doc, path, vars, silent) = jsonpath_args(args)?;
    Ok(Datum::jsonb(path_context(tz, |c| c.query_array(doc, &path, vars, silent))?))
}
fn path_query_first_impl(tz: bool, args: &[Datum]) -> Result<Datum> {
    let (doc, path, vars, silent) = jsonpath_args(args)?;
    Ok(path_context(tz, |c| c.query_first(doc, &path, vars, silent))?.map(Datum::jsonb).unwrap_or(Datum::Null))
}
fn jsonb_path_exists(args: &[Datum]) -> Result<Datum> {
    path_exists_impl(false, args)
}
fn jsonb_path_exists_tz(args: &[Datum]) -> Result<Datum> {
    path_exists_impl(true, args)
}
fn jsonb_path_match(args: &[Datum]) -> Result<Datum> {
    path_match_impl(false, args)
}
fn jsonb_path_match_tz(args: &[Datum]) -> Result<Datum> {
    path_match_impl(true, args)
}
fn jsonb_path_query_array(args: &[Datum]) -> Result<Datum> {
    path_query_array_impl(false, args)
}
fn jsonb_path_query_array_tz(args: &[Datum]) -> Result<Datum> {
    path_query_array_impl(true, args)
}
fn jsonb_path_query_first(args: &[Datum]) -> Result<Datum> {
    path_query_first_impl(false, args)
}
fn jsonb_path_query_first_tz(args: &[Datum]) -> Result<Datum> {
    path_query_first_impl(true, args)
}
/// jsonb @? jsonpath
fn jsonb_path_exists_opr(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(opt_bool(pgfuncs::json::path_exists_op(jsonb(a), &*jsonpath(b)?)?))
}
/// jsonb @@ jsonpath
fn jsonb_path_match_opr(a: &Datum, b: &Datum) -> Result<Datum> {
    Ok(opt_bool(pgfuncs::json::path_match_op(jsonb(a), &*jsonpath(b)?)?))
}

fn jsonb_strip_nulls(a: &Datum) -> Result<Datum> {
    Ok(Datum::jsonb(jsonb(a).strip_nulls()))
}
fn jsonb_pretty(a: &Datum) -> Result<Datum> {
    Ok(txt(jsonb(a).to_pretty()))
}

// ---------------------------------------------------------------- arrays

fn arr(d: &Datum) -> &ArrayValue {
    match d {
        Datum::Array(a) => a,
        _ => panic!("array argument expected"),
    }
}
fn array_length(a: &Datum, b: &Datum) -> Result<Datum> {
    let x = arr(a);
    let dim = i(b);
    Ok(match x.dims.get((dim - 1).max(0) as usize) {
        Some((_, len)) if dim >= 1 => Datum::Int4(*len),
        _ => Datum::Null,
    })
}
fn array_upper(a: &Datum, b: &Datum) -> Result<Datum> {
    let x = arr(a);
    let dim = i(b);
    Ok(match x.dims.get((dim - 1).max(0) as usize) {
        Some((lb, len)) if dim >= 1 => Datum::Int4(lb + len - 1),
        _ => Datum::Null,
    })
}
fn array_lower(a: &Datum, b: &Datum) -> Result<Datum> {
    let x = arr(a);
    let dim = i(b);
    Ok(match x.dims.get((dim - 1).max(0) as usize) {
        Some((lb, _)) if dim >= 1 => Datum::Int4(*lb),
        _ => Datum::Null,
    })
}
fn cardinality(a: &Datum) -> Result<Datum> {
    Ok(Datum::Int4(arr(a).elems.len() as i32))
}
fn array_dims(a: &Datum) -> Result<Datum> {
    let x = arr(a);
    if x.dims.is_empty() {
        return Ok(Datum::Null);
    }
    Ok(txt(x.dims.iter().map(|(lb, len)| format!("[{}:{}]", lb, lb + len - 1)).collect::<String>()))
}
fn array_ndims(a: &Datum) -> Result<Datum> {
    let x = arr(a);
    Ok(if x.dims.is_empty() { Datum::Null } else { Datum::Int4(x.dims.len() as i32) })
}
fn array_position(args: &[Datum]) -> Result<Datum> {
    if args[0].is_null() {
        return Ok(Datum::Null);
    }
    let x = arr(&args[0]);
    if x.dims.len() > 1 {
        return Err(SqlError::new(codes::FEATURE_NOT_SUPPORTED, "searching for elements in multidimensional arrays is not supported"));
    }
    let lb = x.dims.first().map(|d| d.0).unwrap_or(1);
    for (k, e) in x.elems.iter().enumerate() {
        let hit = if args[1].is_null() { e.is_null() } else { !e.is_null() && e.sql_eq(&args[1]) };
        if hit {
            return Ok(Datum::Int4(lb + k as i32));
        }
    }
    Ok(Datum::Null)
}
/// a[lo:hi][lo:hi]... (args: array, then a (lo, hi) pair per dimension)
fn array_slice(args: &[Datum]) -> Result<Datum> {
    let bounds: Vec<(i64, i64)> = args[1..].chunks(2).map(|p| (i(&p[0]), i(&p[1]))).collect();
    Ok(Datum::array(arr(&args[0]).slice(&bounds)))
}
/// a[i][j]... (args: array, then a subscript per dimension)
fn array_get(args: &[Datum]) -> Result<Datum> {
    let idx: Vec<i64> = args[1..].iter().map(i).collect();
    Ok(arr(&args[0]).get(&idx))
}

fn array_remove(args: &[Datum]) -> Result<Datum> {
    if args[0].is_null() {
        return Ok(Datum::Null);
    }
    let x = arr(&args[0]);
    if x.dims.len() > 1 {
        return Err(SqlError::new(codes::FEATURE_NOT_SUPPORTED, "removing elements from multidimensional arrays is not supported"));
    }
    let elems: Vec<Datum> = x
        .elems
        .iter()
        .filter(|e| if args[1].is_null() { !e.is_null() } else { e.is_null() || !e.sql_eq(&args[1]) })
        .cloned()
        .collect();
    Ok(Datum::array(ArrayValue::new_1d(x.elem_type.clone(), elems)))
}
fn array_replace(args: &[Datum]) -> Result<Datum> {
    if args[0].is_null() {
        return Ok(Datum::Null);
    }
    let x = arr(&args[0]);
    let elems: Vec<Datum> = x
        .elems
        .iter()
        .map(|e| {
            let hit = if args[1].is_null() { e.is_null() } else { !e.is_null() && e.sql_eq(&args[1]) };
            if hit { args[2].clone() } else { e.clone() }
        })
        .collect();
    Ok(Datum::array(ArrayValue { elem_type: x.elem_type.clone(), dims: x.dims.clone(), elems }))
}
// array_append / array_prepend / array_cat and their || forms: not strict
// (a NULL array acts as an empty one, a NULL element is appended)
pub(super) fn array_append2(a: &Datum, e: &Datum) -> Result<Datum> {
    Ok(Datum::array(match a {
        Datum::Array(x) => x.append(e.clone())?,
        _ => ArrayValue::new_1d(T::Text, vec![e.clone()]),
    }))
}
pub(super) fn array_prepend2(e: &Datum, a: &Datum) -> Result<Datum> {
    Ok(Datum::array(match a {
        Datum::Array(x) => x.prepend(e.clone())?,
        _ => ArrayValue::new_1d(T::Text, vec![e.clone()]),
    }))
}
pub(super) fn array_cat2(a: &Datum, b: &Datum) -> Result<Datum> {
    match (a, b) {
        (Datum::Null, b) => Ok(b.clone()),
        (a, Datum::Null) => Ok(a.clone()),
        (Datum::Array(x), Datum::Array(y)) => Ok(Datum::array(ArrayValue::concat(x, y)?)),
        _ => Err(SqlError::internal("array_cat")),
    }
}
fn array_append_f(args: &[Datum]) -> Result<Datum> {
    array_append2(&args[0], &args[1])
}
fn array_prepend_f(args: &[Datum]) -> Result<Datum> {
    array_prepend2(&args[0], &args[1])
}
fn array_cat_f(args: &[Datum]) -> Result<Datum> {
    array_cat2(&args[0], &args[1])
}

// ---------------------------------------------------------------- registration

pub fn register(b: &mut Builder) {
    let num_types = [T::Int2, T::Int4, T::Int8, T::Float4, T::Float8, T::Numeric];
    for t in &num_types {
        b.f1("abs", t.clone(), t.clone(), abs_any);
    }
    for name in ["ceil", "ceiling"] {
        b.f1(name, T::Float8, T::Float8, ceil_f);
        b.f1(name, T::Numeric, T::Numeric, ceil_n);
    }
    b.f1("floor", T::Float8, T::Float8, floor_f);
    b.f1("floor", T::Numeric, T::Numeric, floor_n);
    b.f1("round", T::Float8, T::Float8, round_f);
    b.f1("round", T::Numeric, T::Numeric, round_n);
    b.f2("round", T::Numeric, T::Int4, T::Numeric, round_n2);
    b.f1("trunc", T::Float8, T::Float8, trunc_f);
    b.f1("trunc", T::Numeric, T::Numeric, trunc_n);
    b.f2("trunc", T::Numeric, T::Int4, T::Numeric, trunc_n2);
    b.f1("sign", T::Float8, T::Float8, sign_f);
    b.f1("sign", T::Numeric, T::Numeric, sign_n);
    b.f1("sqrt", T::Float8, T::Float8, sqrt_f);
    b.f1("sqrt", T::Numeric, T::Numeric, sqrt_n);
    b.f1("cbrt", T::Float8, T::Float8, cbrt_f);
    b.f1("exp", T::Float8, T::Float8, exp_f);
    b.f1("exp", T::Numeric, T::Numeric, exp_n);
    b.f1("ln", T::Float8, T::Float8, ln_f);
    b.f1("ln", T::Numeric, T::Numeric, ln_n);
    b.f1("log", T::Float8, T::Float8, log10_f);
    b.f1("log", T::Numeric, T::Numeric, log10_n);
    b.f2("log", T::Numeric, T::Numeric, T::Numeric, log_n2);
    b.f1("log10", T::Float8, T::Float8, log10_f);
    b.f1("log10", T::Numeric, T::Numeric, log10_n);
    for t in [T::Int2, T::Int4, T::Int8] {
        b.f2("mod", t.clone(), t.clone(), t.clone(), mod_int);
    }
    b.f2("mod", T::Numeric, T::Numeric, T::Numeric, mod_n);
    b.f2("div", T::Numeric, T::Numeric, T::Numeric, div_n);
    b.fnn("pi", vec![], T::Float8, pi);
    b.f1("degrees", T::Float8, T::Float8, degrees);
    b.f1("radians", T::Float8, T::Float8, radians);
    b.f1("sin", T::Float8, T::Float8, sin);
    b.f1("cos", T::Float8, T::Float8, cos);
    b.f1("tan", T::Float8, T::Float8, tan);
    b.f1("cot", T::Float8, T::Float8, cot);
    b.f1("asin", T::Float8, T::Float8, asin);
    b.f1("acos", T::Float8, T::Float8, acos);
    b.f1("atan", T::Float8, T::Float8, atan);
    b.f2("atan2", T::Float8, T::Float8, T::Float8, atan2);
    b.f1("sinh", T::Float8, T::Float8, sinh);
    b.f1("cosh", T::Float8, T::Float8, cosh);
    b.f1("tanh", T::Float8, T::Float8, tanh);
    b.f1("asinh", T::Float8, T::Float8, asinh);
    b.f1("acosh", T::Float8, T::Float8, acosh);
    b.f1("atanh", T::Float8, T::Float8, atanh);
    for t in [T::Int4, T::Int8] {
        b.f2("gcd", t.clone(), t.clone(), t.clone(), gcd_int);
        b.f2("lcm", t.clone(), t.clone(), t.clone(), lcm_int);
    }
    b.f2("gcd", T::Numeric, T::Numeric, T::Numeric, gcd_n);
    b.f2("lcm", T::Numeric, T::Numeric, T::Numeric, lcm_n);
    b.f1("factorial", T::Int8, T::Numeric, factorial);
    for name in ["power", "pow"] {
        b.f2(name, T::Float8, T::Float8, T::Float8, pow_f);
        b.f2(name, T::Numeric, T::Numeric, T::Numeric, pow_n);
    }
    b.f1("trim_scale", T::Numeric, T::Numeric, trim_scale);
    b.f1("scale", T::Numeric, T::Int4, scale);
    b.last().can_return_null = true;
    b.f1("min_scale", T::Numeric, T::Int4, min_scale);
    b.last().can_return_null = true;
    b.fnn("width_bucket", vec![T::Float8, T::Float8, T::Float8, T::Int4], T::Int4, width_bucket_f);
    b.fnn("width_bucket", vec![T::Numeric, T::Numeric, T::Numeric, T::Int4], T::Int4, width_bucket_n);
    b.fnn("random", vec![], T::Float8, random);
    b.last().volatility = Volatility::Volatile;

    // strings
    for name in ["length", "char_length", "character_length"] {
        b.f1(name, T::Text, T::Int4, length);
    }
    b.f1("length", T::Bytea, T::Int4, length);
    b.f1("octet_length", T::Text, T::Int4, octet_length);
    // character(n) counts its padding in octet_length (length() does not:
    // it applies to the value cast to text)
    b.f1("octet_length", T::Bpchar, T::Int4, octet_length);
    b.f1("text", T::Bpchar, T::Text, bpchar_text);
    b.f1("octet_length", T::Bytea, T::Int4, octet_length);
    b.f1("bit_length", T::Text, T::Int4, bit_length);
    b.f1("bit_length", T::Bytea, T::Int4, bit_length);
    b.f1("lower", T::Text, T::Text, lower);
    b.f1("upper", T::Text, T::Text, upper);
    b.f1("initcap", T::Text, T::Text, initcap);
    for name in ["substr", "substring"] {
        b.f2(name, T::Text, T::Int4, T::Text, substr2);
        b.fnn(name, vec![T::Text, T::Int4, T::Int4], T::Text, substr3);
        b.f2(name, T::Bytea, T::Int4, T::Bytea, substr2);
        b.fnn(name, vec![T::Bytea, T::Int4, T::Int4], T::Bytea, substr3);
    }
    b.f2("substring", T::Text, T::Text, T::Text, substring_re);
    b.last().can_return_null = true;
    b.f2("strpos", T::Text, T::Text, T::Int4, strpos);
    b.fnn("overlay", vec![T::Text, T::Text, T::Int4], T::Text, overlay3);
    b.fnn("overlay", vec![T::Text, T::Text, T::Int4, T::Int4], T::Text, overlay4);
    b.f1("btrim", T::Text, T::Text, btrim1);
    b.f2("btrim", T::Text, T::Text, T::Text, btrim2);
    b.f1("ltrim", T::Text, T::Text, ltrim1);
    b.f2("ltrim", T::Text, T::Text, T::Text, ltrim2);
    b.f1("rtrim", T::Text, T::Text, rtrim1);
    b.f2("rtrim", T::Text, T::Text, T::Text, rtrim2);
    b.fnn("replace", vec![T::Text, T::Text, T::Text], T::Text, replace);
    b.fnn("split_part", vec![T::Text, T::Text, T::Int4], T::Text, split_part);
    b.f2("left", T::Text, T::Int4, T::Text, left);
    b.f2("right", T::Text, T::Int4, T::Text, right);
    b.f2("lpad", T::Text, T::Int4, T::Text, lpad2);
    b.fnn("lpad", vec![T::Text, T::Int4, T::Text], T::Text, lpad3);
    b.f2("rpad", T::Text, T::Int4, T::Text, rpad2);
    b.fnn("rpad", vec![T::Text, T::Int4, T::Text], T::Text, rpad3);
    b.f2("repeat", T::Text, T::Int4, T::Text, repeat);
    b.f1("reverse", T::Text, T::Text, reverse);
    b.fnn("translate", vec![T::Text, T::Text, T::Text], T::Text, translate);
    b.f1("ascii", T::Text, T::Int4, ascii);
    b.f1("chr", T::Int4, T::Text, chr);
    b.f2("starts_with", T::Text, T::Text, T::Bool, starts_with);
    b.f1("md5", T::Text, T::Text, md5);
    b.f1("md5", T::Bytea, T::Text, md5);
    b.f1("sha224", T::Bytea, T::Bytea, sha224);
    b.f1("sha256", T::Bytea, T::Bytea, sha256);
    b.f1("sha384", T::Bytea, T::Bytea, sha384);
    b.f1("sha512", T::Bytea, T::Bytea, sha512);
    b.f1("quote_ident", T::Text, T::Text, quote_ident);
    b.f1("quote_literal", T::Text, T::Text, quote_literal);
    b.poly("quote_literal", vec![P::AnyElement], Ret::T(T::Text), Imp::F1(quote_literal));
    b.poly("quote_nullable", vec![P::AnyElement], Ret::T(T::Text), Imp::FN(quote_nullable)).strict = false;
    b.f1("quote_literal", T::Bpchar, T::Text, quote_literal_bpchar);
    b.poly("quote_nullable", vec![P::T(T::Bpchar)], Ret::T(T::Text), Imp::FN(quote_nullable_bpchar)).strict = false;
    b.f1("to_hex", T::Int4, T::Text, to_hex);
    b.f1("to_hex", T::Int8, T::Text, to_hex);
    {
        let d = b.poly("concat", vec![P::Any], Ret::T(T::Text), Imp::FN(concat));
        d.variadic = true;
        d.strict = false;
    }
    {
        let d = b.poly("concat_ws", vec![P::T(T::Text), P::Any], Ret::T(T::Text), Imp::FN(concat_ws));
        d.variadic = true;
        d.strict = false;
        d.can_return_null = true;
    }
    {
        let d = b.poly("format", vec![P::T(T::Text), P::Any], Ret::T(T::Text), Imp::FN(format));
        d.variadic = true;
        d.strict = false;
        d.can_return_null = true;
    }
    b.poly("format", vec![P::T(T::Text)], Ret::T(T::Text), Imp::FN(format));
    let text_arr = T::Array(Box::new(T::Text));
    b.fnn("string_to_array", vec![T::Text, T::Text], text_arr.clone(), string_to_array);
    b.last().strict = false;
    b.last().can_return_null = true;
    b.fnn("string_to_array", vec![T::Text, T::Text, T::Text], text_arr.clone(), string_to_array);
    b.last().strict = false;
    b.last().can_return_null = true;
    b.poly("array_to_string", vec![P::AnyArray, P::T(T::Text)], Ret::T(T::Text), Imp::FN(array_to_string));
    b.poly("array_to_string", vec![P::AnyArray, P::T(T::Text), P::T(T::Text)], Ret::T(T::Text), Imp::FN(array_to_string))
        .strict = false;
    b.fnn("regexp_replace", vec![T::Text, T::Text, T::Text], T::Text, regexp_replace);
    b.fnn("regexp_replace", vec![T::Text, T::Text, T::Text, T::Text], T::Text, regexp_replace);
    b.fnn("regexp_match", vec![T::Text, T::Text], text_arr.clone(), regexp_match);
    b.last().can_return_null = true;
    b.fnn("regexp_match", vec![T::Text, T::Text, T::Text], text_arr.clone(), regexp_match);
    b.last().can_return_null = true;
    b.fnn("regexp_like", vec![T::Text, T::Text], T::Bool, regexp_like);
    b.fnn("regexp_like", vec![T::Text, T::Text, T::Text], T::Bool, regexp_like);
    b.fnn("regexp_count", vec![T::Text, T::Text], T::Int4, regexp_count);
    b.fnn("regexp_count", vec![T::Text, T::Text, T::Int4], T::Int4, regexp_count);
    b.fnn("regexp_count", vec![T::Text, T::Text, T::Int4, T::Text], T::Int4, regexp_count);
    b.fnn("regexp_substr", vec![T::Text, T::Text], T::Text, regexp_substr);
    b.last().can_return_null = true;
    b.fnn("regexp_substr", vec![T::Text, T::Text, T::Int4], T::Text, regexp_substr);
    b.last().can_return_null = true;
    b.fnn("regexp_substr", vec![T::Text, T::Text, T::Int4, T::Int4], T::Text, regexp_substr);
    b.last().can_return_null = true;
    b.fnn("regexp_split_to_array", vec![T::Text, T::Text], text_arr.clone(), regexp_split_to_array);
    b.fnn("regexp_split_to_array", vec![T::Text, T::Text, T::Text], text_arr.clone(), regexp_split_to_array);
    b.f2("encode", T::Bytea, T::Text, T::Text, encode);
    b.f2("decode", T::Text, T::Text, T::Bytea, decode);
    b.f1("unistr", T::Text, T::Text, unistr);
    {
        let d = b.poly("num_nulls", vec![P::Any], Ret::T(T::Int4), Imp::FN(num_nulls));
        d.variadic = true;
        d.strict = false;
    }
    {
        let d = b.poly("num_nonnulls", vec![P::Any], Ret::T(T::Int4), Imp::FN(num_nonnulls));
        d.variadic = true;
        d.strict = false;
    }
    // catalog helpers used by client tools (see sysrel)
    for name in ["pg_table_is_visible", "pg_type_is_visible", "pg_function_is_visible"] {
        b.f1(name, T::Oid, T::Bool, always_true1);
    }
    b.f1("pg_get_userbyid", T::Oid, T::Name, owner_name);
    b.fnn("format_type", vec![T::Oid, T::Int4], T::Text, format_type_fn);
    b.last().strict = false;
    b.last().can_return_null = true;
    for (name, params) in [
        ("obj_description", vec![P::T(T::Oid)]),
        ("obj_description", vec![P::T(T::Oid), P::T(T::Name)]),
        ("col_description", vec![P::T(T::Oid), P::T(T::Int4)]),
        ("shobj_description", vec![P::T(T::Oid), P::T(T::Name)]),
        ("pg_get_expr", vec![P::T(T::Text), P::T(T::Oid)]),
        ("pg_get_expr", vec![P::T(T::Text), P::T(T::Oid), P::T(T::Bool)]),
        ("pg_get_indexdef", vec![P::T(T::Oid)]),
        ("pg_get_indexdef", vec![P::T(T::Oid), P::T(T::Int4), P::T(T::Bool)]),
        ("pg_get_constraintdef", vec![P::T(T::Oid)]),
        ("pg_get_constraintdef", vec![P::T(T::Oid), P::T(T::Bool)]),
        ("pg_get_viewdef", vec![P::T(T::Oid)]),
        ("pg_get_viewdef", vec![P::T(T::Oid), P::T(T::Bool)]),
        ("pg_get_ruledef", vec![P::T(T::Oid)]),
        ("pg_get_ruledef", vec![P::T(T::Oid), P::T(T::Bool)]),
        ("pg_get_triggerdef", vec![P::T(T::Oid)]),
        ("pg_get_partkeydef", vec![P::T(T::Oid)]),
        ("pg_get_statisticsobjdef_columns", vec![P::T(T::Oid)]),
        ("pg_get_function_result", vec![P::T(T::Oid)]),
        ("pg_get_function_arguments", vec![P::T(T::Oid)]),
        ("pg_get_function_identity_arguments", vec![P::T(T::Oid)]),
    ] {
        let d = b.poly(name, params, Ret::T(T::Text), Imp::FN(null_text));
        d.can_return_null = true;
    }
    b.f1("pg_relation_is_publishable", T::Oid, T::Bool, always_false1);
    // storage sizes are not visible to expressions; report zero
    for name in ["pg_table_size", "pg_relation_size", "pg_total_relation_size", "pg_indexes_size"] {
        b.f1(name, T::Oid, T::Int8, zero_size);
    }
    b.f2("pg_relation_size", T::Oid, T::Text, T::Int8, zero_size2);
    b.f1("pg_size_pretty", T::Int8, T::Text, size_pretty);
    b.f1("pg_size_pretty", T::Numeric, T::Text, size_pretty);
    b.f1("current_schemas", T::Bool, T::Array(Box::new(T::Name)), current_schemas);
    for name in ["has_table_privilege", "has_schema_privilege", "has_database_privilege", "has_column_privilege", "has_any_column_privilege"] {
        b.poly(name, vec![P::Any, P::Any], Ret::T(T::Bool), Imp::FN(always_true));
        b.poly(name, vec![P::Any, P::Any, P::Any], Ret::T(T::Bool), Imp::FN(always_true));
    }
    b.f1("pg_encoding_to_char", T::Int4, T::Name, encoding_name);
    b.fnn("set_config", vec![T::Text, T::Text, T::Bool], T::Text, set_config);
    b.last().volatility = Volatility::Volatile;
    for params in [vec![T::Text], vec![T::Text, T::Bool]] {
        b.fnn("current_setting", params, T::Text, current_setting);
        b.last().volatility = Volatility::Stable;
        b.last().can_return_null = true;
    }
    // the process id changes when the server restarts (stable, as in
    // PostgreSQL: maintained views must not store it)
    b.fnn("pg_backend_pid", vec![], T::Int4, backend_pid);
    b.last().volatility = Volatility::Stable;
    b.fnn("pg_is_in_recovery", vec![], T::Bool, always_false);
    {
        // GROUPING(...): replaced by the binder after aggregation
        let d = b.poly("grouping", vec![P::Any], Ret::T(T::Int4), Imp::FN(grouping_misplaced));
        d.variadic = true;
        d.strict = false;
    }
    b.fnn("gen_random_uuid", vec![], T::Uuid, gen_random_uuid);
    b.last().volatility = Volatility::Volatile;

    // date/time
    for name in ["now", "current_timestamp", "transaction_timestamp", "statement_timestamp"] {
        b.fnn(name, vec![], T::TimestampTz, now);
        b.last().volatility = Volatility::Stable;
    }
    for t in [T::Timestamp, T::TimestampTz, T::Time] {
        b.fnn("overlaps", vec![t.clone(), t.clone(), t.clone(), t.clone()], T::Bool, overlaps);
        b.last().strict = false;
        b.last().can_return_null = true;
    }
    b.fnn("clock_timestamp", vec![], T::TimestampTz, now);
    b.last().volatility = Volatility::Volatile;
    b.fnn("current_date", vec![], T::Date, current_date);
    b.last().volatility = Volatility::Stable;
    b.fnn("localtimestamp", vec![], T::Timestamp, localtimestamp);
    b.last().volatility = Volatility::Stable;
    for t in [T::Date, T::Time, T::Timestamp, T::TimestampTz, T::Interval] {
        b.f2("extract", T::Text, t.clone(), T::Numeric, extract);
        b.f2("date_part", T::Text, t.clone(), T::Float8, date_part);
    }
    for t in [T::Timestamp, T::TimestampTz, T::Interval] {
        b.f2("date_trunc", T::Text, t.clone(), t.clone(), date_trunc);
    }
    b.fnn("date_bin", vec![T::Interval, T::Timestamp, T::Timestamp], T::Timestamp, date_bin);
    b.fnn("date_bin", vec![T::Interval, T::TimestampTz, T::TimestampTz], T::TimestampTz, date_bin);
    b.f2("age", T::Timestamp, T::Timestamp, T::Interval, age);
    b.f2("age", T::TimestampTz, T::TimestampTz, T::Interval, age);
    b.fnn("make_date", vec![T::Int4, T::Int4, T::Int4], T::Date, make_date);
    b.fnn("make_time", vec![T::Int4, T::Int4, T::Float8], T::Time, make_time);
    b.fnn("make_timestamp", vec![T::Int4, T::Int4, T::Int4, T::Int4, T::Int4, T::Float8], T::Timestamp, make_timestamp);
    b.fnn("make_timestamptz", vec![T::Int4, T::Int4, T::Int4, T::Int4, T::Int4, T::Float8], T::TimestampTz, make_timestamptz);
    b.fnn(
        "make_timestamptz",
        vec![T::Int4, T::Int4, T::Int4, T::Int4, T::Int4, T::Float8, T::Text],
        T::TimestampTz,
        make_timestamptz,
    );
    for k in 0..=7 {
        let mut params = vec![T::Int4; k.min(6)];
        if k == 7 {
            params.push(T::Float8);
        }
        b.fnn("make_interval", params, T::Interval, make_interval);
    }
    b.f1("to_timestamp", T::Float8, T::TimestampTz, to_timestamp_epoch);
    b.f2("to_timestamp", T::Text, T::Text, T::TimestampTz, to_timestamp_fmt);
    b.f2("to_date", T::Text, T::Text, T::Date, to_date);
    for t in [T::Timestamp, T::TimestampTz, T::Interval, T::Numeric, T::Int8, T::Int4, T::Float8, T::Float4] {
        b.f2("to_char", t.clone(), T::Text, T::Text, to_char);
    }
    b.last().can_return_null = true;
    b.f2("to_number", T::Text, T::Text, T::Numeric, to_number);
    b.last().can_return_null = true;
    b.f1("justify_hours", T::Interval, T::Interval, justify_hours);
    b.f1("justify_days", T::Interval, T::Interval, justify_days);
    b.f1("justify_interval", T::Interval, T::Interval, justify_interval);
    for t in [T::Date, T::Timestamp, T::TimestampTz, T::Interval] {
        b.f1("isfinite", t.clone(), T::Bool, isfinite);
    }
    b.f2("timezone", T::Text, T::TimestampTz, T::Timestamp, timezone);
    b.f2("timezone", T::Text, T::Timestamp, T::TimestampTz, timezone);

    // json
    b.poly("to_jsonb", vec![P::AnyElement], Ret::T(T::Jsonb), Imp::F1(to_jsonb));
    b.poly("to_json", vec![P::AnyElement], Ret::T(T::Json), Imp::F1(to_json));
    for (name, fun) in [("jsonb_build_object", jsonb_build_object as super::func::FnN), ("json_build_object", json_build_object)] {
        let ret = if name.starts_with("jsonb") { T::Jsonb } else { T::Json };
        let d = b.poly(name, vec![P::Any], Ret::T(ret.clone()), Imp::FN(fun));
        d.variadic = true;
        d.strict = false;
        b.poly(name, vec![], Ret::T(ret), Imp::FN(fun)).strict = false;
    }
    for (name, fun) in [("jsonb_build_array", jsonb_build_array as super::func::FnN), ("json_build_array", json_build_array)] {
        let ret = if name.starts_with("jsonb") { T::Jsonb } else { T::Json };
        let d = b.poly(name, vec![P::Any], Ret::T(ret.clone()), Imp::FN(fun));
        d.variadic = true;
        d.strict = false;
        b.poly(name, vec![], Ret::T(ret), Imp::FN(fun)).strict = false;
    }
    b.f1("jsonb_typeof", T::Jsonb, T::Text, jsonb_typeof);
    b.f1("json_typeof", T::Json, T::Text, json_typeof);
    b.f1("jsonb_array_length", T::Jsonb, T::Int4, jsonb_array_length);
    b.f1("json_array_length", T::Json, T::Int4, json_array_length);
    {
        let d = b.poly("jsonb_extract_path", vec![P::T(T::Jsonb), P::T(T::Text)], Ret::T(T::Jsonb), Imp::FN(jsonb_extract_path));
        d.variadic = true;
        d.strict = false;
        d.can_return_null = true;
        let d = b.poly(
            "jsonb_extract_path_text",
            vec![P::T(T::Jsonb), P::T(T::Text)],
            Ret::T(T::Text),
            Imp::FN(jsonb_extract_path_text),
        );
        d.variadic = true;
        d.strict = false;
        d.can_return_null = true;
    }
    let path4 = || vec![T::Jsonb, T::JsonPath, T::Jsonb, T::Bool];
    let path_fns: [(&'static str, T, super::func::FnN); 8] = [
        ("jsonb_path_exists", T::Bool, jsonb_path_exists),
        ("jsonb_path_exists_tz", T::Bool, jsonb_path_exists_tz),
        ("jsonb_path_match", T::Bool, jsonb_path_match),
        ("jsonb_path_match_tz", T::Bool, jsonb_path_match_tz),
        ("jsonb_path_query_array", T::Jsonb, jsonb_path_query_array),
        ("jsonb_path_query_array_tz", T::Jsonb, jsonb_path_query_array_tz),
        ("jsonb_path_query_first", T::Jsonb, jsonb_path_query_first),
        ("jsonb_path_query_first_tz", T::Jsonb, jsonb_path_query_first_tz),
    ];
    for (name, ret, f) in path_fns {
        b.fnn(name, path4(), ret, f);
        b.last().can_return_null = true;
    }
    b.op2("@?", T::Jsonb, T::JsonPath, T::Bool, jsonb_path_exists_opr);
    b.last().can_return_null = true;
    b.op2("@@", T::Jsonb, T::JsonPath, T::Bool, jsonb_path_match_opr);
    b.last().can_return_null = true;
    b.fnn("jsonb_set", vec![T::Jsonb, text_arr.clone(), T::Jsonb], T::Jsonb, jsonb_set);
    b.fnn("jsonb_set", vec![T::Jsonb, text_arr.clone(), T::Jsonb, T::Bool], T::Jsonb, jsonb_set);
    b.fnn("jsonb_insert", vec![T::Jsonb, text_arr.clone(), T::Jsonb], T::Jsonb, jsonb_insert);
    b.fnn("jsonb_insert", vec![T::Jsonb, text_arr.clone(), T::Jsonb, T::Bool], T::Jsonb, jsonb_insert);
    b.f1("jsonb_strip_nulls", T::Jsonb, T::Jsonb, jsonb_strip_nulls);
    b.f1("jsonb_pretty", T::Jsonb, T::Text, jsonb_pretty);

    // arrays
    b.poly("array_length", vec![P::AnyArray, P::T(T::Int4)], Ret::T(T::Int4), Imp::F2(array_length)).can_return_null =
        true;
    b.poly("array_upper", vec![P::AnyArray, P::T(T::Int4)], Ret::T(T::Int4), Imp::F2(array_upper)).can_return_null =
        true;
    b.poly("array_lower", vec![P::AnyArray, P::T(T::Int4)], Ret::T(T::Int4), Imp::F2(array_lower)).can_return_null =
        true;
    b.poly("cardinality", vec![P::AnyArray], Ret::T(T::Int4), Imp::F1(cardinality));
    b.poly("array_ndims", vec![P::AnyArray], Ret::T(T::Int4), Imp::F1(array_ndims)).can_return_null = true;
    b.poly("array_dims", vec![P::AnyArray], Ret::T(T::Text), Imp::F1(array_dims)).can_return_null = true;
    b.poly("array_to_json", vec![P::AnyArray], Ret::T(T::Json), Imp::FN(array_to_json));
    b.poly("array_to_json", vec![P::AnyArray, P::T(T::Bool)], Ret::T(T::Json), Imp::FN(array_to_json));
    {
        let d = b.poly("array_position", vec![P::AnyArray, P::AnyElement], Ret::T(T::Int4), Imp::FN(array_position));
        d.strict = false;
        d.can_return_null = true;
    }
    b.poly("array_remove", vec![P::AnyArray, P::AnyElement], Ret::ArrayOfElem, Imp::FN(array_remove)).strict = false;
    // a[lo:hi]... (the binder passes i32::MIN / i32::MAX for omitted bounds)
    b.poly("array_slice", vec![P::AnyArray, P::T(T::Int4)], Ret::ArrayOfElem, Imp::FN(array_slice)).variadic = true;
    // a[i][j]...
    {
        let d = b.poly("array_get", vec![P::AnyArray, P::T(T::Int4)], Ret::Elem, Imp::FN(array_get));
        d.variadic = true;
        d.can_return_null = true;
    }
    b.poly("array_replace", vec![P::AnyArray, P::AnyElement, P::AnyElement], Ret::ArrayOfElem, Imp::FN(array_replace))
        .strict = false;
    b.poly("array_append", vec![P::AnyArray, P::AnyElement], Ret::ArrayOfElem, Imp::FN(array_append_f)).strict = false;
    b.poly("array_prepend", vec![P::AnyElement, P::AnyArray], Ret::ArrayOfElem, Imp::FN(array_prepend_f)).strict =
        false;
    {
        let d = b.poly("array_cat", vec![P::AnyArray, P::AnyArray], Ret::ArrayOfElem, Imp::FN(array_cat_f));
        d.strict = false;
        d.can_return_null = true;
    }
}
