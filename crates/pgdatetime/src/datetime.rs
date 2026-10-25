//! Date/time input parsing: port of ParseDateTime(), DecodeDateTime(),
//! DecodeTimeOnly(), DecodeInterval(), DecodeISO8601Interval() and helpers
//! from PostgreSQL's src/backend/utils/adt/datetime.c (PostgreSQL License).
//!
//! Session settings emulated: DateStyle = 'ISO, MDY', IntervalStyle = postgres.

use crate::Error;
use crate::core::*;
use crate::error::*;
use crate::tz::{self, AbbrevKind, ZoneRef};

pub(crate) const MAXDATELEN: usize = 128;
pub(crate) const MAXDATEFIELDS: usize = 25;

// field types (datetime.h)
pub(crate) const RESERV: i32 = 0;
pub(crate) const MONTH: i32 = 1;
pub(crate) const YEAR: i32 = 2;
pub(crate) const DAY: i32 = 3;
pub(crate) const TZ: i32 = 5;
pub(crate) const DTZ: i32 = 6;
pub(crate) const DYNTZ: i32 = 7;
pub(crate) const IGNORE_DTF: i32 = 8;
pub(crate) const AMPM: i32 = 9;
pub(crate) const HOUR: i32 = 10;
pub(crate) const MINUTE: i32 = 11;
pub(crate) const SECOND: i32 = 12;
pub(crate) const MILLISECOND: i32 = 13;
pub(crate) const MICROSECOND: i32 = 14;
pub(crate) const DOY: i32 = 15;
pub(crate) const DOW: i32 = 16;
pub(crate) const UNITS: i32 = 17;
pub(crate) const ADBC: i32 = 18;
pub(crate) const AGO: i32 = 19;
pub(crate) const ISOTIME: i32 = 23;
pub(crate) const WEEK: i32 = 24;
pub(crate) const DECADE: i32 = 25;
pub(crate) const CENTURY: i32 = 26;
pub(crate) const MILLENNIUM: i32 = 27;
pub(crate) const DTZMOD: i32 = 28;
pub(crate) const UNKNOWN_FIELD: i32 = 31;

pub(crate) const DTK_NUMBER: i32 = 0;
pub(crate) const DTK_STRING: i32 = 1;
pub(crate) const DTK_DATE: i32 = 2;
pub(crate) const DTK_TIME: i32 = 3;
pub(crate) const DTK_TZ: i32 = 4;
pub(crate) const DTK_SPECIAL: i32 = 6;
pub(crate) const DTK_EARLY: i32 = 9;
pub(crate) const DTK_LATE: i32 = 10;
pub(crate) const DTK_EPOCH: i32 = 11;
pub(crate) const DTK_NOW: i32 = 12;
pub(crate) const DTK_YESTERDAY: i32 = 13;
pub(crate) const DTK_TODAY: i32 = 14;
pub(crate) const DTK_TOMORROW: i32 = 15;
pub(crate) const DTK_ZULU: i32 = 16;
pub(crate) const DTK_DELTA: i32 = 17;
pub(crate) const DTK_SECOND: i32 = 18;
pub(crate) const DTK_MINUTE: i32 = 19;
pub(crate) const DTK_HOUR: i32 = 20;
pub(crate) const DTK_DAY: i32 = 21;
pub(crate) const DTK_WEEK: i32 = 22;
pub(crate) const DTK_MONTH: i32 = 23;
pub(crate) const DTK_QUARTER: i32 = 24;
pub(crate) const DTK_YEAR: i32 = 25;
pub(crate) const DTK_DECADE: i32 = 26;
pub(crate) const DTK_CENTURY: i32 = 27;
pub(crate) const DTK_MILLENNIUM: i32 = 28;
pub(crate) const DTK_MILLISEC: i32 = 29;
pub(crate) const DTK_MICROSEC: i32 = 30;
pub(crate) const DTK_JULIAN: i32 = 31;
pub(crate) const DTK_DOW: i32 = 32;
pub(crate) const DTK_DOY: i32 = 33;
pub(crate) const DTK_TZ_HOUR: i32 = 34;
pub(crate) const DTK_TZ_MINUTE: i32 = 35;
pub(crate) const DTK_ISOYEAR: i32 = 36;
pub(crate) const DTK_ISODOW: i32 = 37;

#[inline]
pub(crate) const fn dtk_m(t: i32) -> i32 {
    1 << t
}
pub(crate) const DTK_ALL_SECS_M: i32 = dtk_m(SECOND) | dtk_m(MILLISECOND) | dtk_m(MICROSECOND);
pub(crate) const DTK_DATE_M: i32 = dtk_m(YEAR) | dtk_m(MONTH) | dtk_m(DAY);
pub(crate) const DTK_TIME_M: i32 = dtk_m(HOUR) | dtk_m(MINUTE) | DTK_ALL_SECS_M;

const AM: i32 = 0;
const PM: i32 = 1;
const HR24: i32 = 2;
const BC: i32 = 1;

pub(crate) const INTERVAL_FULL_RANGE: i32 = 0x7FFF;
#[inline]
pub(crate) const fn interval_mask(b: i32) -> i32 {
    1 << b
}

// ---------------------------------------------------------------------------
// C library emulation (C locale)
// ---------------------------------------------------------------------------

#[inline]
pub(crate) fn c_isspace(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}
#[inline]
pub(crate) fn c_isdigit(b: u8) -> bool {
    b.is_ascii_digit()
}
#[inline]
pub(crate) fn c_isalpha(b: u8) -> bool {
    b.is_ascii_alphabetic()
}
#[inline]
pub(crate) fn c_isalnum(b: u8) -> bool {
    b.is_ascii_alphanumeric()
}
#[inline]
pub(crate) fn c_ispunct(b: u8) -> bool {
    b.is_ascii_punctuation()
}

#[inline]
fn byte_at(s: &[u8], i: usize) -> u8 {
    *s.get(i).unwrap_or(&0)
}

/// strtol(str, &end, 10) with 64-bit long.  Returns (value, end index, erange).
pub(crate) fn c_strtol(s: &[u8]) -> (i64, usize, bool) {
    let mut i = 0;
    while i < s.len() && c_isspace(s[i]) {
        i += 1;
    }
    let mut neg = false;
    if i < s.len() && (s[i] == b'+' || s[i] == b'-') {
        neg = s[i] == b'-';
        i += 1;
    }
    let start_digits = i;
    let mut val: i128 = 0;
    let mut overflow = false;
    while i < s.len() && c_isdigit(s[i]) {
        if !overflow {
            val = val * 10 + (s[i] - b'0') as i128;
            if val > (i64::MAX as i128) + 1 {
                overflow = true;
            }
        }
        i += 1;
    }
    if i == start_digits {
        return (0, 0, false);
    }
    let v = if neg { -val } else { val };
    if overflow || v > i64::MAX as i128 || v < i64::MIN as i128 {
        return (if neg { i64::MIN } else { i64::MAX }, i, true);
    }
    (v as i64, i, false)
}

/// PostgreSQL's strtoint(): strtol() narrowed to int, ERANGE on overflow.
pub(crate) fn c_strtoint(s: &[u8]) -> (i32, usize, bool) {
    let (v, end, erange) = c_strtol(s);
    if v != (v as i32) as i64 {
        return (v as i32, end, true);
    }
    (v as i32, end, erange)
}

/// atoi()
pub(crate) fn c_atoi(s: &[u8]) -> i32 {
    c_strtol(s).0 as i32
}

/// strtod(): returns (value, end index, erange).  end == 0 means no conversion.
pub(crate) fn c_strtod(s: &[u8]) -> (f64, usize, bool) {
    let mut i = 0;
    while i < s.len() && c_isspace(s[i]) {
        i += 1;
    }
    let mut neg = false;
    if i < s.len() && (s[i] == b'+' || s[i] == b'-') {
        neg = s[i] == b'-';
        i += 1;
    }
    let sign = if neg { -1.0 } else { 1.0 };
    let rest = &s[i..];
    let lower3: Vec<u8> = rest
        .iter()
        .take(8)
        .map(|b| b.to_ascii_lowercase())
        .collect();
    if lower3.starts_with(b"inf") {
        let n = if lower3.starts_with(b"infinity") {
            8
        } else {
            3
        };
        return (sign * f64::INFINITY, i + n, false);
    }
    if lower3.starts_with(b"nan") {
        let mut j = i + 3;
        if byte_at(s, j) == b'(' {
            let mut k = j + 1;
            while k < s.len() && (c_isalnum(s[k]) || s[k] == b'_') {
                k += 1;
            }
            if byte_at(s, k) == b')' {
                j = k + 1;
            }
        }
        return (f64::NAN, j, false);
    }
    // hexadecimal
    if rest.len() >= 2 && rest[0] == b'0' && (rest[1] == b'x' || rest[1] == b'X') {
        let mut j = 2;
        let mut mant: u128 = 0;
        let mut exp: i64 = 0;
        let mut any = false;
        let mut overflow_digits = 0i64;
        while j < rest.len() && rest[j].is_ascii_hexdigit() {
            any = true;
            if mant >> 120 == 0 {
                mant = mant * 16 + (rest[j] as char).to_digit(16).unwrap() as u128;
            } else {
                overflow_digits += 1;
            }
            j += 1;
        }
        if j < rest.len() && rest[j] == b'.' {
            let mut k = j + 1;
            let mut frac_any = false;
            while k < rest.len() && rest[k].is_ascii_hexdigit() {
                frac_any = true;
                if mant >> 120 == 0 {
                    mant = mant * 16 + (rest[k] as char).to_digit(16).unwrap() as u128;
                    exp -= 4;
                }
                k += 1;
            }
            if any || frac_any {
                any = true;
                j = k;
            }
        }
        if any {
            exp += overflow_digits * 4;
            if j < rest.len() && (rest[j] == b'p' || rest[j] == b'P') {
                let mut k = j + 1;
                let mut eneg = false;
                if k < rest.len() && (rest[k] == b'+' || rest[k] == b'-') {
                    eneg = rest[k] == b'-';
                    k += 1;
                }
                if k < rest.len() && c_isdigit(rest[k]) {
                    let mut e: i64 = 0;
                    while k < rest.len() && c_isdigit(rest[k]) {
                        e = (e * 10 + (rest[k] - b'0') as i64).min(1_000_000);
                        k += 1;
                    }
                    exp += if eneg { -e } else { e };
                    j = k;
                }
            }
            let v = (mant as f64) * 2f64.powi(exp.clamp(-2000, 2000) as i32);
            let erange = v.is_infinite()
                || (v != 0.0 && v.abs() < f64::MIN_POSITIVE)
                || (v == 0.0 && mant != 0);
            return (sign * v, i + j, erange);
        }
        // "0x" without digits: parse the "0"
        return (sign * 0.0, i + 1, false);
    }
    // decimal
    let mut j = 0;
    let mut digits = 0;
    let mut nonzero = false;
    while j < rest.len() && c_isdigit(rest[j]) {
        if rest[j] != b'0' {
            nonzero = true;
        }
        j += 1;
        digits += 1;
    }
    if j < rest.len() && rest[j] == b'.' {
        j += 1;
        while j < rest.len() && c_isdigit(rest[j]) {
            if rest[j] != b'0' {
                nonzero = true;
            }
            j += 1;
            digits += 1;
        }
    }
    if digits == 0 {
        return (0.0, 0, false);
    }
    let mant_end = j;
    if j < rest.len() && (rest[j] == b'e' || rest[j] == b'E') {
        let mut k = j + 1;
        if k < rest.len() && (rest[k] == b'+' || rest[k] == b'-') {
            k += 1;
        }
        if k < rest.len() && c_isdigit(rest[k]) {
            while k < rest.len() && c_isdigit(rest[k]) {
                k += 1;
            }
            j = k;
        }
    }
    let _ = mant_end;
    let text = std::str::from_utf8(&rest[..j]).unwrap_or("0");
    let mut v: f64 = text.parse().unwrap_or(0.0);
    let erange = v.is_infinite() || (nonzero && (v == 0.0 || v.abs() < f64::MIN_POSITIVE));
    v *= sign;
    (v, i + j, erange)
}

/// strtod() that must consume the whole string (used by check_timezone).
pub(crate) fn c_strtod_full(s: &str) -> Option<f64> {
    let b = s.as_bytes();
    let (v, end, _) = c_strtod(b);
    if end != 0 && end == b.len() {
        Some(v)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// keyword tables
// ---------------------------------------------------------------------------

/// datetktbl lookup (DecodeSpecial): returns (type, value)
pub(crate) fn decode_special(lowtoken: &str) -> (i32, i32) {
    let t = tz::truncate_token(lowtoken);
    match t {
        "+infinity" => (RESERV, DTK_LATE),
        "-infinity" => (RESERV, DTK_EARLY),
        "ad" => (ADBC, 0),
        "allballs" => (RESERV, DTK_ZULU),
        "am" => (AMPM, AM),
        "apr" | "april" => (MONTH, 4),
        "at" => (IGNORE_DTF, 0),
        "aug" | "august" => (MONTH, 8),
        "bc" => (ADBC, BC),
        "d" => (UNITS, DTK_DAY),
        "dec" | "december" => (MONTH, 12),
        "dow" => (UNITS, DTK_DOW),
        "doy" => (UNITS, DTK_DOY),
        "dst" => (DTZMOD, SECS_PER_HOUR as i32),
        "epoch" => (RESERV, DTK_EPOCH),
        "feb" | "february" => (MONTH, 2),
        "fri" | "friday" => (DOW, 5),
        "h" => (UNITS, DTK_HOUR),
        "infinity" => (RESERV, DTK_LATE),
        "isodow" => (UNITS, DTK_ISODOW),
        "isoyear" => (UNITS, DTK_ISOYEAR),
        "j" | "jd" | "julian" => (UNITS, DTK_JULIAN),
        "jan" | "january" => (MONTH, 1),
        "jul" | "july" => (MONTH, 7),
        "jun" | "june" => (MONTH, 6),
        "m" => (UNITS, DTK_MONTH),
        "mar" | "march" => (MONTH, 3),
        "may" => (MONTH, 5),
        "mm" => (UNITS, DTK_MINUTE),
        "mon" | "monday" => (DOW, 1),
        "nov" | "november" => (MONTH, 11),
        "now" => (RESERV, DTK_NOW),
        "oct" | "october" => (MONTH, 10),
        "on" => (IGNORE_DTF, 0),
        "pm" => (AMPM, PM),
        "s" => (UNITS, DTK_SECOND),
        "sat" | "saturday" => (DOW, 6),
        "sep" | "sept" | "september" => (MONTH, 9),
        "sun" | "sunday" => (DOW, 0),
        "t" => (ISOTIME, DTK_TIME),
        "thu" | "thur" | "thurs" | "thursday" => (DOW, 4),
        "today" => (RESERV, DTK_TODAY),
        "tomorrow" => (RESERV, DTK_TOMORROW),
        "tue" | "tues" | "tuesday" => (DOW, 2),
        "wed" | "wednesday" | "weds" => (DOW, 3),
        "y" => (UNITS, DTK_YEAR),
        "yesterday" => (RESERV, DTK_YESTERDAY),
        _ => (UNKNOWN_FIELD, 0),
    }
}

/// deltatktbl lookup (DecodeUnits): returns (type, value)
pub(crate) fn decode_units(lowtoken: &str) -> (i32, i32) {
    let t = tz::truncate_token(lowtoken);
    match t {
        "@" => (IGNORE_DTF, 0),
        "ago" => (AGO, 0),
        "c" | "cent" | "centuries" | "century" => (UNITS, DTK_CENTURY),
        "d" | "day" | "days" => (UNITS, DTK_DAY),
        "dec" | "decade" | "decades" | "decs" => (UNITS, DTK_DECADE),
        "h" | "hour" | "hours" | "hr" | "hrs" => (UNITS, DTK_HOUR),
        "m" | "min" | "mins" | "minute" | "minutes" => (UNITS, DTK_MINUTE),
        "microsecon" | "us" | "usec" | "usecond" | "useconds" | "usecs" => (UNITS, DTK_MICROSEC),
        "mil" | "millennia" | "millennium" | "mils" => (UNITS, DTK_MILLENNIUM),
        "millisecon" | "ms" | "msec" | "msecond" | "mseconds" | "msecs" => (UNITS, DTK_MILLISEC),
        "mon" | "mons" | "month" | "months" => (UNITS, DTK_MONTH),
        "qtr" | "quarter" => (UNITS, DTK_QUARTER),
        "s" | "sec" | "second" | "seconds" | "secs" => (UNITS, DTK_SECOND),
        "timezone" => (UNITS, DTK_TZ),
        "timezone_h" => (UNITS, DTK_TZ_HOUR),
        "timezone_m" => (UNITS, DTK_TZ_MINUTE),
        "w" | "week" | "weeks" => (UNITS, DTK_WEEK),
        "y" | "year" | "years" | "yr" | "yrs" => (UNITS, DTK_YEAR),
        _ => (UNKNOWN_FIELD, 0),
    }
}

// ---------------------------------------------------------------------------
// errors
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) enum DtErr {
    Code(i32),
    BadTimezone(String),
    BadZoneAbbrev(String, String),
    /// non-deterministic special value ("now", "today", ...)
    Special(&'static str),
}

impl DtErr {
    pub fn to_error(&self, s: &str, datatype: &str) -> Error {
        match self {
            DtErr::Code(c) => datetime_parse_error(*c, &DtErrExtra::default(), s, datatype),
            DtErr::BadTimezone(tz) => datetime_parse_error(
                DTERR_BAD_TIMEZONE,
                &DtErrExtra {
                    timezone: tz.clone(),
                    abbrev: String::new(),
                },
                s,
                datatype,
            ),
            DtErr::BadZoneAbbrev(tz, ab) => datetime_parse_error(
                DTERR_BAD_ZONE_ABBREV,
                &DtErrExtra {
                    timezone: tz.clone(),
                    abbrev: ab.clone(),
                },
                s,
                datatype,
            ),
            DtErr::Special(v) => Error::new(
                FEATURE_NOT_SUPPORTED,
                format!("special value \"{}\" is not supported", v),
            ),
        }
    }
}

type DtResult<T> = std::result::Result<T, DtErr>;

#[inline]
fn bad() -> DtErr {
    DtErr::Code(DTERR_BAD_FORMAT)
}
#[inline]
fn overflow() -> DtErr {
    DtErr::Code(DTERR_FIELD_OVERFLOW)
}

// ---------------------------------------------------------------------------
// ParseDateTime
// ---------------------------------------------------------------------------

/// ParseDateTime(): break the string into lower-cased fields.  `buflen` is
/// the size of the caller's work buffer (limits accepted input length).
pub(crate) fn parse_date_time(timestr: &str, buflen: usize) -> DtResult<(Vec<String>, Vec<i32>)> {
    let s = timestr.as_bytes();
    let mut fields: Vec<String> = Vec::new();
    let mut ftypes: Vec<i32> = Vec::new();
    let mut used: usize = 0; // bytes used in the work buffer
    let mut cp = 0usize;

    macro_rules! append {
        ($buf:expr, $c:expr) => {{
            if used + 1 >= buflen {
                return Err(bad());
            }
            $buf.push($c as char);
            used += 1;
        }};
    }

    while cp < s.len() {
        if c_isspace(s[cp]) {
            cp += 1;
            continue;
        }
        if fields.len() >= MAXDATEFIELDS {
            return Err(bad());
        }
        let mut buf = String::new();
        let ftype;
        let c = s[cp];
        if c_isdigit(c) {
            append!(buf, s[cp]);
            cp += 1;
            while cp < s.len() && c_isdigit(s[cp]) {
                append!(buf, s[cp]);
                cp += 1;
            }
            if byte_at(s, cp) == b':' {
                ftype = DTK_TIME;
                append!(buf, s[cp]);
                cp += 1;
                while cp < s.len() && (c_isdigit(s[cp]) || s[cp] == b':' || s[cp] == b'.') {
                    append!(buf, s[cp]);
                    cp += 1;
                }
            } else if matches!(byte_at(s, cp), b'-' | b'/' | b'.') {
                let delim = s[cp];
                append!(buf, s[cp]);
                cp += 1;
                if c_isdigit(byte_at(s, cp)) {
                    let mut ft = if delim == b'.' { DTK_NUMBER } else { DTK_DATE };
                    while cp < s.len() && c_isdigit(s[cp]) {
                        append!(buf, s[cp]);
                        cp += 1;
                    }
                    if byte_at(s, cp) == delim {
                        ft = DTK_DATE;
                        append!(buf, s[cp]);
                        cp += 1;
                        while cp < s.len() && (c_isdigit(s[cp]) || s[cp] == delim) {
                            append!(buf, s[cp]);
                            cp += 1;
                        }
                    }
                    ftype = ft;
                } else {
                    ftype = DTK_DATE;
                    while cp < s.len() && (c_isalnum(s[cp]) || s[cp] == delim) {
                        append!(buf, s[cp].to_ascii_lowercase());
                        cp += 1;
                    }
                }
            } else {
                ftype = DTK_NUMBER;
            }
        } else if c == b'.' {
            append!(buf, s[cp]);
            cp += 1;
            while cp < s.len() && c_isdigit(s[cp]) {
                append!(buf, s[cp]);
                cp += 1;
            }
            ftype = DTK_NUMBER;
        } else if c_isalpha(c) {
            let mut ft = DTK_STRING;
            append!(buf, s[cp].to_ascii_lowercase());
            cp += 1;
            while cp < s.len() && c_isalpha(s[cp]) {
                append!(buf, s[cp].to_ascii_lowercase());
                cp += 1;
            }
            let mut is_date = false;
            let nc = byte_at(s, cp);
            if nc == b'-' || nc == b'/' || nc == b'.' {
                is_date = true;
            } else if nc == b'+' || c_isdigit(nc) {
                // we need search only the core token table, not TZ names
                if decode_special(&buf).0 == UNKNOWN_FIELD {
                    is_date = true;
                }
            }
            if is_date {
                ft = DTK_DATE;
                loop {
                    append!(buf, s[cp].to_ascii_lowercase());
                    cp += 1;
                    let n = byte_at(s, cp);
                    if !(n == b'+'
                        || n == b'-'
                        || n == b'/'
                        || n == b'_'
                        || n == b'.'
                        || n == b':'
                        || c_isalnum(n))
                        || cp >= s.len()
                    {
                        break;
                    }
                }
            }
            ftype = ft;
        } else if c == b'+' || c == b'-' {
            append!(buf, s[cp]);
            cp += 1;
            while cp < s.len() && c_isspace(s[cp]) {
                cp += 1;
            }
            if c_isdigit(byte_at(s, cp)) {
                ftype = DTK_TZ;
                append!(buf, s[cp]);
                cp += 1;
                while cp < s.len()
                    && (c_isdigit(s[cp]) || s[cp] == b':' || s[cp] == b'.' || s[cp] == b'-')
                {
                    append!(buf, s[cp]);
                    cp += 1;
                }
            } else if c_isalpha(byte_at(s, cp)) {
                ftype = DTK_SPECIAL;
                append!(buf, s[cp].to_ascii_lowercase());
                cp += 1;
                while cp < s.len() && c_isalpha(s[cp]) {
                    append!(buf, s[cp].to_ascii_lowercase());
                    cp += 1;
                }
            } else {
                return Err(bad());
            }
        } else if c_ispunct(c) {
            cp += 1;
            continue;
        } else {
            return Err(bad());
        }
        // force in a delimiter after each field
        used += 1;
        fields.push(buf);
        ftypes.push(ftype);
    }
    Ok((fields, ftypes))
}

// ---------------------------------------------------------------------------
// Decoding helpers
// ---------------------------------------------------------------------------

/// ParseFraction(): cp starts with '.'; must be followed only by digits.
fn parse_fraction(cp: &[u8]) -> DtResult<f64> {
    debug_assert!(cp.first() == Some(&b'.'));
    if cp.len() == 1 {
        return Ok(0.0);
    }
    let (v, end, erange) = c_strtod(cp);
    if end != cp.len() || erange {
        return Err(bad());
    }
    Ok(v)
}

/// ParseFractionalSecond()
fn parse_fractional_second(cp: &[u8]) -> DtResult<i32> {
    let frac = parse_fraction(cp)?;
    Ok(rint(frac * 1_000_000.0) as i32)
}

/// Result of DecodeDateTime / DecodeTimeOnly
pub(crate) struct Decoded {
    pub dtype: i32,
    pub tm: Tm,
    pub fsec: i32,
    /// PG "tz" (seconds west of UTC)
    pub tz: i32,
}

struct DateState {
    tm: Tm,
    fsec: i32,
    is2digits: bool,
    tm_yday: i32,
}

/// DecodeDate()
fn decode_date(
    s: &str,
    mut fmask: i32,
    tmask: &mut i32,
    is2digits: &mut bool,
    st: &mut DateState,
) -> DtResult<()> {
    let b = s.as_bytes();
    let mut flds: Vec<Option<&[u8]>> = Vec::new();
    *tmask = 0;
    let mut i = 0usize;
    while i < b.len() && flds.len() < MAXDATEFIELDS {
        while i < b.len() && !c_isalnum(b[i]) {
            i += 1;
        }
        if i >= b.len() {
            return Err(bad());
        }
        let start = i;
        if c_isdigit(b[i]) {
            while i < b.len() && c_isdigit(b[i]) {
                i += 1;
            }
        } else if c_isalpha(b[i]) {
            while i < b.len() && c_isalpha(b[i]) {
                i += 1;
            }
        }
        let end = i;
        if i < b.len() {
            i += 1; // replace delimiter by NUL
        }
        flds.push(Some(&b[start..end]));
    }
    let mut have_text_month = false;
    let n = flds.len();
    for k in 0..n {
        let f = flds[k].unwrap();
        if !f.is_empty() && c_isalpha(f[0]) {
            let tok = std::str::from_utf8(f).unwrap_or("");
            let (ty, val) = decode_special(tok);
            if ty == IGNORE_DTF {
                continue;
            }
            let dmask = dtk_m(ty);
            match ty {
                MONTH => {
                    st.tm.mon = val;
                    have_text_month = true;
                }
                _ => return Err(bad()),
            }
            if fmask & dmask != 0 {
                return Err(bad());
            }
            fmask |= dmask;
            *tmask |= dmask;
            flds[k] = None;
        }
    }
    for k in 0..n {
        let Some(f) = flds[k] else { continue };
        if f.is_empty() {
            return Err(bad());
        }
        let mut dmask = 0;
        // DecodeDate passes a local fsec to DecodeNumber (result discarded)
        let saved_fsec = st.fsec;
        let r = decode_number(
            f.len(),
            f,
            have_text_month,
            fmask,
            &mut dmask,
            st,
            is2digits,
        );
        st.fsec = saved_fsec;
        r?;
        if fmask & dmask != 0 {
            return Err(bad());
        }
        fmask |= dmask;
        *tmask |= dmask;
    }
    if (fmask & !(dtk_m(DOY) | dtk_m(TZ))) != DTK_DATE_M {
        return Err(bad());
    }
    Ok(())
}

/// ValidateDate()
pub(crate) fn validate_date(
    fmask: i32,
    isjulian: bool,
    is2digits: bool,
    bc: bool,
    tm: &mut Tm,
    tm_yday: i32,
) -> std::result::Result<(), i32> {
    if fmask & dtk_m(YEAR) != 0 {
        if isjulian {
            // tm_year is correct
        } else if bc {
            if tm.year <= 0 {
                return Err(DTERR_FIELD_OVERFLOW);
            }
            tm.year = -(tm.year - 1);
        } else if is2digits {
            if tm.year < 0 {
                return Err(DTERR_FIELD_OVERFLOW);
            }
            if tm.year < 70 {
                tm.year += 2000;
            } else if tm.year < 100 {
                tm.year += 1900;
            }
        } else if tm.year <= 0 {
            return Err(DTERR_FIELD_OVERFLOW);
        }
    }
    if fmask & dtk_m(DOY) != 0 {
        let (y, m, d) = j2date(date2j(tm.year, 1, 1).wrapping_add(tm_yday).wrapping_sub(1));
        tm.year = y;
        tm.mon = m;
        tm.mday = d;
    }
    if fmask & dtk_m(MONTH) != 0 && (tm.mon < 1 || tm.mon > MONTHS_PER_YEAR) {
        return Err(DTERR_MD_FIELD_OVERFLOW);
    }
    if fmask & dtk_m(DAY) != 0 && (tm.mday < 1 || tm.mday > 31) {
        return Err(DTERR_MD_FIELD_OVERFLOW);
    }
    if (fmask & DTK_DATE_M) == DTK_DATE_M
        && tm.mday > DAY_TAB[isleap(tm.year) as usize][(tm.mon - 1) as usize]
    {
        return Err(DTERR_FIELD_OVERFLOW);
    }
    Ok(())
}

/// struct pg_itm subset used by DecodeTimeCommon
struct ItmTime {
    usec: i32,
    sec: i32,
    min: i32,
    hour: i64,
}

/// DecodeTimeCommon()
fn decode_time_common(s: &[u8], range: i32, tmask: &mut i32) -> DtResult<ItmTime> {
    *tmask = DTK_TIME_M;
    let mut itm = ItmTime {
        usec: 0,
        sec: 0,
        min: 0,
        hour: 0,
    };
    let mut fsec: i32 = 0;
    let (h, mut cp, erange) = c_strtol(s);
    if erange {
        return Err(overflow());
    }
    itm.hour = h;
    if byte_at(s, cp) != b':' {
        return Err(bad());
    }
    let rest = &s[cp + 1..];
    let (m, end, erange) = c_strtoint(rest);
    if erange {
        return Err(overflow());
    }
    // strtoint with no digits leaves the pointer at its start
    cp = cp + 1 + end;
    itm.min = m;
    let c = byte_at(s, cp);
    if cp >= s.len() {
        itm.sec = 0;
        if range == (interval_mask(MINUTE) | interval_mask(SECOND)) {
            if itm.hour > i32::MAX as i64 || itm.hour < i32::MIN as i64 {
                return Err(overflow());
            }
            itm.sec = itm.min;
            itm.min = itm.hour as i32;
            itm.hour = 0;
        }
    } else if c == b'.' {
        fsec = parse_fractional_second(&s[cp..])?;
        if itm.hour > i32::MAX as i64 || itm.hour < i32::MIN as i64 {
            return Err(overflow());
        }
        itm.sec = itm.min;
        itm.min = itm.hour as i32;
        itm.hour = 0;
    } else if c == b':' {
        let rest = &s[cp + 1..];
        let (sv, end, erange) = c_strtoint(rest);
        if erange {
            return Err(overflow());
        }
        cp = cp + 1 + end;
        itm.sec = sv;
        if byte_at(s, cp) == b'.' {
            fsec = parse_fractional_second(&s[cp..])?;
        } else if cp < s.len() {
            return Err(bad());
        }
    } else {
        return Err(bad());
    }
    if itm.hour < 0
        || itm.min < 0
        || itm.min > 59
        || itm.sec < 0
        || itm.sec > 60
        || fsec < 0
        || fsec as i64 > USECS_PER_SEC
    {
        return Err(overflow());
    }
    itm.usec = fsec;
    Ok(itm)
}

/// DecodeTime(): for timestamps
fn decode_time(s: &[u8], range: i32, tmask: &mut i32, tm: &mut Tm, fsec: &mut i32) -> DtResult<()> {
    let itm = decode_time_common(s, range, tmask)?;
    if itm.hour > i32::MAX as i64 {
        return Err(overflow());
    }
    tm.hour = itm.hour as i32;
    tm.min = itm.min;
    tm.sec = itm.sec;
    *fsec = itm.usec;
    Ok(())
}

/// DecodeTimeForInterval()
fn decode_time_for_interval(
    s: &[u8],
    range: i32,
    tmask: &mut i32,
    itm_in: &mut ItmIn,
) -> DtResult<()> {
    let itm = decode_time_common(s, range, tmask)?;
    itm_in.usec = itm.usec as i64;
    if !int64_multiply_add(itm.hour, USECS_PER_HOUR, &mut itm_in.usec)
        || !int64_multiply_add(itm.min as i64, USECS_PER_MINUTE, &mut itm_in.usec)
        || !int64_multiply_add(itm.sec as i64, USECS_PER_SEC, &mut itm_in.usec)
    {
        return Err(overflow());
    }
    Ok(())
}

/// DecodeNumber()
fn decode_number(
    flen: usize,
    s: &[u8],
    have_text_month: bool,
    fmask: i32,
    tmask: &mut i32,
    st: &mut DateState,
    is2digits: &mut bool,
) -> DtResult<()> {
    *tmask = 0;
    let (val, cp, erange) = c_strtoint(s);
    if erange {
        return Err(overflow());
    }
    if cp == 0 {
        return Err(bad());
    }
    if byte_at(s, cp) == b'.' {
        if cp > 2 {
            decode_number_field(flen, s, fmask | DTK_DATE_M, tmask, st, is2digits)?;
            return Ok(());
        }
        st.fsec = parse_fractional_second(&s[cp..])?;
    } else if cp < s.len() {
        return Err(bad());
    }
    // Special case for day of year
    if flen == 3 && (fmask & DTK_DATE_M) == dtk_m(YEAR) && (1..=366).contains(&val) {
        *tmask = dtk_m(DOY) | dtk_m(MONTH) | dtk_m(DAY);
        st.tm_yday = val;
        return Ok(());
    }
    match fmask & DTK_DATE_M {
        0 => {
            // DateOrder == DATEORDER_MDY
            if flen >= 3 {
                *tmask = dtk_m(YEAR);
                st.tm.year = val;
            } else {
                *tmask = dtk_m(MONTH);
                st.tm.mon = val;
            }
        }
        x if x == dtk_m(YEAR) => {
            *tmask = dtk_m(MONTH);
            st.tm.mon = val;
        }
        x if x == dtk_m(MONTH) => {
            if have_text_month {
                if flen >= 3 {
                    *tmask = dtk_m(YEAR);
                    st.tm.year = val;
                } else {
                    *tmask = dtk_m(DAY);
                    st.tm.mday = val;
                }
            } else {
                *tmask = dtk_m(DAY);
                st.tm.mday = val;
            }
        }
        x if x == (dtk_m(YEAR) | dtk_m(MONTH)) => {
            if have_text_month {
                if flen >= 3 && *is2digits {
                    *tmask = dtk_m(DAY);
                    st.tm.mday = st.tm.year;
                    st.tm.year = val;
                    *is2digits = false;
                } else {
                    *tmask = dtk_m(DAY);
                    st.tm.mday = val;
                }
            } else {
                *tmask = dtk_m(DAY);
                st.tm.mday = val;
            }
        }
        x if x == dtk_m(DAY) => {
            *tmask = dtk_m(MONTH);
            st.tm.mon = val;
        }
        x if x == (dtk_m(MONTH) | dtk_m(DAY)) => {
            *tmask = dtk_m(YEAR);
            st.tm.year = val;
        }
        x if x == (dtk_m(YEAR) | dtk_m(MONTH) | dtk_m(DAY)) => {
            decode_number_field(flen, s, fmask, tmask, st, is2digits)?;
            return Ok(());
        }
        _ => return Err(bad()),
    }
    if *tmask == dtk_m(YEAR) {
        *is2digits = flen <= 2;
    }
    Ok(())
}

/// DecodeNumberField(): returns DTK_DATE / DTK_TIME on success.
fn decode_number_field(
    len: usize,
    s: &[u8],
    fmask: i32,
    tmask: &mut i32,
    st: &mut DateState,
    is2digits: &mut bool,
) -> DtResult<i32> {
    let mut s: &[u8] = s;
    let mut len = len;
    if let Some(dot) = s.iter().position(|&c| c == b'.') {
        let cp = &s[dot..];
        if cp.len() == 1 {
            st.fsec = 0;
        } else {
            let (frac, _end, erange) = c_strtod(cp);
            if erange {
                return Err(bad());
            }
            st.fsec = rint(frac * 1_000_000.0) as i32;
        }
        s = &s[..dot];
        len = s.len();
    } else if (fmask & DTK_DATE_M) != DTK_DATE_M && len >= 6 {
        *tmask = DTK_DATE_M;
        st.tm.mday = c_atoi(&s[len - 2..]);
        st.tm.mon = c_atoi(&s[len - 4..len - 2]);
        st.tm.year = c_atoi(&s[..len - 4]);
        if len - 4 == 2 {
            *is2digits = true;
        }
        return Ok(DTK_DATE);
    }
    if (fmask & DTK_TIME_M) != DTK_TIME_M {
        if len == 6 {
            *tmask = DTK_TIME_M;
            st.tm.sec = c_atoi(&s[4..]);
            st.tm.min = c_atoi(&s[2..4]);
            st.tm.hour = c_atoi(&s[..2]);
            return Ok(DTK_TIME);
        } else if len == 4 {
            *tmask = DTK_TIME_M;
            st.tm.sec = 0;
            st.tm.min = c_atoi(&s[2..4]);
            st.tm.hour = c_atoi(&s[..2]);
            return Ok(DTK_TIME);
        }
    }
    Err(bad())
}

/// DecodeTimezone(): numeric zone; returns PG tz (seconds west).
pub(crate) fn decode_timezone(s: &[u8]) -> std::result::Result<i32, i32> {
    let first = byte_at(s, 0);
    if first != b'+' && first != b'-' {
        return Err(DTERR_BAD_FORMAT);
    }
    let (mut hr, end, erange) = c_strtoint(&s[1..]);
    if erange {
        return Err(DTERR_TZDISP_OVERFLOW);
    }
    let mut cp = 1 + end;
    let mut min;
    let mut sec = 0;
    if byte_at(s, cp) == b':' {
        let (m, e, erange) = c_strtoint(&s[cp + 1..]);
        if erange {
            return Err(DTERR_TZDISP_OVERFLOW);
        }
        min = m;
        cp = cp + 1 + e;
        if byte_at(s, cp) == b':' {
            let (sv, e, erange) = c_strtoint(&s[cp + 1..]);
            if erange {
                return Err(DTERR_TZDISP_OVERFLOW);
            }
            sec = sv;
            cp = cp + 1 + e;
        }
    } else if cp >= s.len() && s.len() > 3 {
        min = hr % 100;
        hr /= 100;
    } else {
        min = 0;
    }
    if hr < 0 || hr > MAX_TZDISP_HOUR {
        return Err(DTERR_TZDISP_OVERFLOW);
    }
    if min < 0 || min >= 60 {
        return Err(DTERR_TZDISP_OVERFLOW);
    }
    if sec < 0 || sec >= 60 {
        return Err(DTERR_TZDISP_OVERFLOW);
    }
    let _ = &mut min;
    let mut tz = (hr * 60 + min) * 60 + sec;
    if first == b'-' {
        tz = -tz;
    }
    if cp < s.len() {
        return Err(DTERR_BAD_FORMAT);
    }
    Ok(-tz)
}

/// A decoded time zone abbreviation (DecodeTimezoneAbbrev)
enum AbbrevLookup {
    Unknown,
    /// (type TZ or DTZ, offset seconds east)
    Fixed(i32, i32),
    Dyn(ZoneRef),
}

fn decode_timezone_abbrev(lowtoken: &str) -> DtResult<AbbrevLookup> {
    match tz::lookup_abbrev(lowtoken) {
        None => Ok(AbbrevLookup::Unknown),
        Some(AbbrevKind::Tz(off)) => Ok(AbbrevLookup::Fixed(TZ, off)),
        Some(AbbrevKind::Dtz(off)) => Ok(AbbrevLookup::Fixed(DTZ, off)),
        Some(AbbrevKind::Dyn(zone)) => match tz::fetch_dynamic_zone(zone) {
            Some(z) => Ok(AbbrevLookup::Dyn(z)),
            None => Err(DtErr::BadZoneAbbrev(
                zone.to_string(),
                tz::truncate_token(lowtoken).to_string(),
            )),
        },
    }
}

fn special_name(val: i32) -> &'static str {
    match val {
        DTK_NOW => "now",
        DTK_TODAY => "today",
        DTK_TOMORROW => "tomorrow",
        DTK_YESTERDAY => "yesterday",
        _ => "?",
    }
}

/// DecodeDateTime().  `session` is the zone used when the input has no
/// zone; pass None when the caller ignores the zone (date, timestamp).
pub(crate) fn decode_date_time(
    fields: &mut [String],
    ftype: &mut [i32],
    session: Option<&ZoneRef>,
) -> DtResult<Decoded> {
    let nf = fields.len();
    let mut fmask: i32 = 0;
    let mut tmask: i32 = 0;
    let mut ptype: i32 = 0;
    let mut mer = HR24;
    let mut have_text_month = false;
    let mut isjulian = false;
    let mut is2digits = false;
    let mut bc = false;
    let mut named_tz: Option<ZoneRef> = None;
    let mut abbrev_tz: Option<ZoneRef> = None;
    let mut abbrev: String = String::new();
    let mut dtype = DTK_DATE;
    let mut st = DateState {
        tm: Tm {
            isdst: -1,
            ..Default::default()
        },
        fsec: 0,
        is2digits: false,
        tm_yday: 0,
    };
    let mut tz: i32 = 0;
    let _ = st.is2digits;

    let mut i = 0;
    while i < nf {
        let field = fields[i].clone();
        let fb = field.as_bytes();
        match ftype[i] {
            DTK_DATE => {
                if ptype == DTK_JULIAN {
                    let (jday, cp, erange) = c_strtoint(fb);
                    if erange || jday < 0 {
                        return Err(overflow());
                    }
                    let (y, m, d) = j2date(jday);
                    st.tm.year = y;
                    st.tm.mon = m;
                    st.tm.mday = d;
                    isjulian = true;
                    tz = decode_timezone(&fb[cp..]).map_err(DtErr::Code)?;
                    tmask = DTK_DATE_M | DTK_TIME_M | dtk_m(TZ);
                    ptype = 0;
                } else if ptype != 0
                    || (fmask & (dtk_m(MONTH) | dtk_m(DAY))) == (dtk_m(MONTH) | dtk_m(DAY))
                {
                    if c_isdigit(byte_at(fb, 0)) || ptype != 0 {
                        if ptype != 0 {
                            if ptype != DTK_TIME {
                                return Err(bad());
                            }
                            ptype = 0;
                        }
                        if (fmask & DTK_TIME_M) == DTK_TIME_M {
                            return Err(bad());
                        }
                        let Some(dash) = fb.iter().position(|&c| c == b'-') else {
                            return Err(bad());
                        };
                        tz = decode_timezone(&fb[dash..]).map_err(DtErr::Code)?;
                        let head = &fb[..dash];
                        decode_number_field(
                            head.len(),
                            head,
                            fmask,
                            &mut tmask,
                            &mut st,
                            &mut is2digits,
                        )?;
                        tmask |= dtk_m(TZ);
                    } else {
                        match tz::pg_tzset(&field) {
                            Some(z) => named_tz = Some(z.zone_ref()),
                            None => return Err(DtErr::BadTimezone(field.clone())),
                        }
                        tmask = dtk_m(TZ);
                    }
                } else {
                    decode_date(&field, fmask, &mut tmask, &mut is2digits, &mut st)?;
                }
            }
            DTK_TIME => {
                if ptype != 0 {
                    if ptype != DTK_TIME {
                        return Err(bad());
                    }
                    ptype = 0;
                }
                let mut fsec = 0;
                decode_time(fb, INTERVAL_FULL_RANGE, &mut tmask, &mut st.tm, &mut fsec)?;
                st.fsec = fsec;
                if time_overflows(st.tm.hour, st.tm.min, st.tm.sec, st.fsec) {
                    return Err(overflow());
                }
            }
            DTK_TZ => {
                tz = decode_timezone(fb).map_err(DtErr::Code)?;
                tmask = dtk_m(TZ);
            }
            DTK_NUMBER => {
                if ptype != 0 {
                    let (value, cp, erange) = c_strtoint(fb);
                    if erange {
                        return Err(overflow());
                    }
                    let c = byte_at(fb, cp);
                    if c != b'.' && cp < fb.len() {
                        return Err(bad());
                    }
                    match ptype {
                        DTK_JULIAN => {
                            if value < 0 {
                                return Err(overflow());
                            }
                            tmask = DTK_DATE_M;
                            let (y, m, d) = j2date(value);
                            st.tm.year = y;
                            st.tm.mon = m;
                            st.tm.mday = d;
                            isjulian = true;
                            if c == b'.' {
                                let time = parse_fraction(&fb[cp..])? * USECS_PER_DAY as f64;
                                let (h, mi, s, fs) = dt2time(time as i64);
                                st.tm.hour = h;
                                st.tm.min = mi;
                                st.tm.sec = s;
                                st.fsec = fs;
                                tmask |= DTK_TIME_M;
                            }
                        }
                        DTK_TIME => {
                            decode_number_field(
                                fb.len(),
                                fb,
                                fmask | DTK_DATE_M,
                                &mut tmask,
                                &mut st,
                                &mut is2digits,
                            )?;
                            if tmask != DTK_TIME_M {
                                return Err(bad());
                            }
                        }
                        _ => return Err(bad()),
                    }
                    ptype = 0;
                    dtype = DTK_DATE;
                } else {
                    let flen = fb.len();
                    let dot = fb.iter().position(|&c| c == b'.');
                    if dot.is_some() && (fmask & DTK_DATE_M) == 0 {
                        decode_date(&field, fmask, &mut tmask, &mut is2digits, &mut st)?;
                    } else if let Some(d) = dot
                        && flen - (flen - d) > 2
                    {
                        decode_number_field(flen, fb, fmask, &mut tmask, &mut st, &mut is2digits)?;
                    } else if flen >= 6 && ((fmask & DTK_DATE_M) == 0 || (fmask & DTK_TIME_M) == 0)
                    {
                        decode_number_field(flen, fb, fmask, &mut tmask, &mut st, &mut is2digits)?;
                    } else {
                        decode_number(
                            flen,
                            fb,
                            have_text_month,
                            fmask,
                            &mut tmask,
                            &mut st,
                            &mut is2digits,
                        )?;
                    }
                }
            }
            DTK_STRING | DTK_SPECIAL => {
                // timezone abbrevs take precedence over built-in tokens
                let ab = decode_timezone_abbrev(&field)?;
                let (mut ty, mut val) = match ab {
                    AbbrevLookup::Unknown => (UNKNOWN_FIELD, 0),
                    AbbrevLookup::Fixed(t, off) => (t, off),
                    AbbrevLookup::Dyn(_) => (DYNTZ, 0),
                };
                let valtz = if let AbbrevLookup::Dyn(z) = ab {
                    Some(z)
                } else {
                    None
                };
                if ty == UNKNOWN_FIELD {
                    let (t, v) = decode_special(&field);
                    ty = t;
                    val = v;
                }
                if ty == IGNORE_DTF {
                    i += 1;
                    continue;
                }
                tmask = dtk_m(ty);
                match ty {
                    RESERV => match val {
                        DTK_NOW | DTK_YESTERDAY | DTK_TODAY | DTK_TOMORROW => {
                            return Err(DtErr::Special(special_name(val)));
                        }
                        DTK_ZULU => {
                            tmask = DTK_TIME_M | dtk_m(TZ);
                            dtype = DTK_DATE;
                            st.tm.hour = 0;
                            st.tm.min = 0;
                            st.tm.sec = 0;
                            tz = 0;
                        }
                        DTK_EPOCH | DTK_LATE | DTK_EARLY => {
                            tmask = DTK_DATE_M | DTK_TIME_M | dtk_m(TZ);
                            dtype = val;
                        }
                        _ => return Err(bad()),
                    },
                    MONTH => {
                        if (fmask & dtk_m(MONTH)) != 0
                            && !have_text_month
                            && (fmask & dtk_m(DAY)) == 0
                            && st.tm.mon >= 1
                            && st.tm.mon <= 31
                        {
                            st.tm.mday = st.tm.mon;
                            tmask = dtk_m(DAY);
                        }
                        have_text_month = true;
                        st.tm.mon = val;
                    }
                    DTZMOD => {
                        tmask |= dtk_m(DTZ);
                        st.tm.isdst = 1;
                        tz -= val;
                    }
                    DTZ => {
                        tmask |= dtk_m(TZ);
                        st.tm.isdst = 1;
                        tz = -val;
                    }
                    TZ => {
                        st.tm.isdst = 0;
                        tz = -val;
                    }
                    DYNTZ => {
                        tmask |= dtk_m(TZ);
                        abbrev_tz = valtz;
                        abbrev = field.clone();
                    }
                    AMPM => mer = val,
                    ADBC => bc = val == BC,
                    DOW => st.tm.wday = val,
                    UNITS => {
                        tmask = 0;
                        if ptype != 0 {
                            return Err(bad());
                        }
                        ptype = val;
                    }
                    ISOTIME => {
                        tmask = 0;
                        if (fmask & DTK_DATE_M) != DTK_DATE_M {
                            return Err(bad());
                        }
                        if ptype != 0 {
                            return Err(bad());
                        }
                        ptype = val;
                    }
                    UNKNOWN_FIELD => {
                        match tz::pg_tzset(&field) {
                            Some(z) => named_tz = Some(z.zone_ref()),
                            None => return Err(bad()),
                        }
                        tmask = dtk_m(TZ);
                    }
                    _ => return Err(bad()),
                }
            }
            _ => return Err(bad()),
        }
        if tmask & fmask != 0 {
            return Err(bad());
        }
        fmask |= tmask;
        i += 1;
    }

    if ptype != 0 {
        return Err(bad());
    }

    if dtype == DTK_DATE {
        validate_date(fmask, isjulian, is2digits, bc, &mut st.tm, st.tm_yday)
            .map_err(DtErr::Code)?;
        if mer != HR24 && st.tm.hour > HOURS_PER_DAY / 2 {
            return Err(overflow());
        }
        if mer == AM && st.tm.hour == HOURS_PER_DAY / 2 {
            st.tm.hour = 0;
        } else if mer == PM && st.tm.hour != HOURS_PER_DAY / 2 {
            st.tm.hour += HOURS_PER_DAY / 2;
        }
        if (fmask & DTK_DATE_M) != DTK_DATE_M {
            if (fmask & DTK_TIME_M) == DTK_TIME_M {
                return Err(DtErr::Code(1));
            }
            return Err(bad());
        }
        if let Some(z) = named_tz {
            if fmask & dtk_m(DTZMOD) != 0 {
                return Err(bad());
            }
            tz = tz::determine_offset(&mut st.tm, &z);
        }
        if let Some(z) = abbrev_tz {
            if fmask & dtk_m(DTZMOD) != 0 {
                return Err(bad());
            }
            tz = tz::determine_abbrev_offset(&mut st.tm, &abbrev, &z);
        }
        if fmask & dtk_m(TZ) == 0 {
            if fmask & dtk_m(DTZMOD) != 0 {
                return Err(bad());
            }
            if let Some(z) = session {
                tz = tz::determine_offset(&mut st.tm, z);
            }
        }
    }
    Ok(Decoded {
        dtype,
        tm: st.tm,
        fsec: st.fsec,
        tz,
    })
}

/// DecodeTimeOnly().  The time zone result is not needed by our callers
/// (we only implement `time without time zone`), but all checks are kept.
pub(crate) fn decode_time_only(fields: &mut [String], ftype: &mut [i32]) -> DtResult<Decoded> {
    let nf = fields.len();
    let mut fmask: i32 = 0;
    let mut tmask: i32 = 0;
    let mut ptype: i32 = 0;
    let mut isjulian = false;
    let mut is2digits = false;
    let mut bc = false;
    let mut mer = HR24;
    let mut named_tz: Option<ZoneRef> = None;
    let mut abbrev_tz: Option<ZoneRef> = None;
    let mut dtype = DTK_TIME;
    let mut st = DateState {
        tm: Tm {
            isdst: -1,
            ..Default::default()
        },
        fsec: 0,
        is2digits: false,
        tm_yday: 0,
    };
    let mut tz: i32 = 0;

    let mut i = 0;
    while i < nf {
        let field = fields[i].clone();
        let fb = field.as_bytes();
        match ftype[i] {
            DTK_DATE => {
                if i == 0 && nf >= 2 && (ftype[nf - 1] == DTK_DATE || ftype[1] == DTK_TIME) {
                    decode_date(&field, fmask, &mut tmask, &mut is2digits, &mut st)?;
                } else if c_isdigit(byte_at(fb, 0)) {
                    if (fmask & DTK_TIME_M) == DTK_TIME_M {
                        return Err(bad());
                    }
                    let Some(dash) = fb.iter().position(|&c| c == b'-') else {
                        return Err(bad());
                    };
                    tz = decode_timezone(&fb[dash..]).map_err(DtErr::Code)?;
                    let head = &fb[..dash];
                    let r = decode_number_field(
                        head.len(),
                        head,
                        fmask | DTK_DATE_M,
                        &mut tmask,
                        &mut st,
                        &mut is2digits,
                    )?;
                    ftype[i] = r;
                    tmask |= dtk_m(TZ);
                } else {
                    match tz::pg_tzset(&field) {
                        Some(z) => named_tz = Some(z.zone_ref()),
                        None => return Err(DtErr::BadTimezone(field.clone())),
                    }
                    ftype[i] = DTK_TZ;
                    tmask = dtk_m(TZ);
                }
            }
            DTK_TIME => {
                if ptype != 0 {
                    if ptype != DTK_TIME {
                        return Err(bad());
                    }
                    ptype = 0;
                }
                let mut fsec = 0;
                decode_time(fb, INTERVAL_FULL_RANGE, &mut tmask, &mut st.tm, &mut fsec)?;
                st.fsec = fsec;
            }
            DTK_TZ => {
                tz = decode_timezone(fb).map_err(DtErr::Code)?;
                tmask = dtk_m(TZ);
            }
            DTK_NUMBER => {
                if ptype != 0 {
                    let (value, cp, erange) = c_strtoint(fb);
                    if erange {
                        return Err(overflow());
                    }
                    let c = byte_at(fb, cp);
                    if c != b'.' && cp < fb.len() {
                        return Err(bad());
                    }
                    match ptype {
                        DTK_JULIAN => {
                            if value < 0 {
                                return Err(overflow());
                            }
                            tmask = DTK_DATE_M;
                            let (y, m, d) = j2date(value);
                            st.tm.year = y;
                            st.tm.mon = m;
                            st.tm.mday = d;
                            isjulian = true;
                            if c == b'.' {
                                let time = parse_fraction(&fb[cp..])? * USECS_PER_DAY as f64;
                                let (h, mi, s, fs) = dt2time(time as i64);
                                st.tm.hour = h;
                                st.tm.min = mi;
                                st.tm.sec = s;
                                st.fsec = fs;
                                tmask |= DTK_TIME_M;
                            }
                        }
                        DTK_TIME => {
                            let r = decode_number_field(
                                fb.len(),
                                fb,
                                fmask | DTK_DATE_M,
                                &mut tmask,
                                &mut st,
                                &mut is2digits,
                            )?;
                            ftype[i] = r;
                            if tmask != DTK_TIME_M {
                                return Err(bad());
                            }
                        }
                        _ => return Err(bad()),
                    }
                    ptype = 0;
                    dtype = DTK_DATE;
                } else {
                    let flen = fb.len();
                    if let Some(d) = fb.iter().position(|&c| c == b'.') {
                        if i == 0 && nf >= 2 && ftype[nf - 1] == DTK_DATE {
                            decode_date(&field, fmask, &mut tmask, &mut is2digits, &mut st)?;
                        } else if flen - (flen - d) > 2 {
                            let r = decode_number_field(
                                flen,
                                fb,
                                fmask | DTK_DATE_M,
                                &mut tmask,
                                &mut st,
                                &mut is2digits,
                            )?;
                            ftype[i] = r;
                        } else {
                            return Err(bad());
                        }
                    } else if flen > 4 {
                        let r = decode_number_field(
                            flen,
                            fb,
                            fmask | DTK_DATE_M,
                            &mut tmask,
                            &mut st,
                            &mut is2digits,
                        )?;
                        ftype[i] = r;
                    } else {
                        decode_number(
                            flen,
                            fb,
                            false,
                            fmask | DTK_DATE_M,
                            &mut tmask,
                            &mut st,
                            &mut is2digits,
                        )?;
                    }
                }
            }
            DTK_STRING | DTK_SPECIAL => {
                let ab = decode_timezone_abbrev(&field)?;
                let (mut ty, mut val) = match ab {
                    AbbrevLookup::Unknown => (UNKNOWN_FIELD, 0),
                    AbbrevLookup::Fixed(t, off) => (t, off),
                    AbbrevLookup::Dyn(_) => (DYNTZ, 0),
                };
                let valtz = if let AbbrevLookup::Dyn(z) = ab {
                    Some(z)
                } else {
                    None
                };
                if ty == UNKNOWN_FIELD {
                    let (t, v) = decode_special(&field);
                    ty = t;
                    val = v;
                }
                if ty == IGNORE_DTF {
                    i += 1;
                    continue;
                }
                tmask = dtk_m(ty);
                match ty {
                    RESERV => match val {
                        DTK_NOW => return Err(DtErr::Special("now")),
                        DTK_ZULU => {
                            tmask = DTK_TIME_M | dtk_m(TZ);
                            dtype = DTK_TIME;
                            st.tm.hour = 0;
                            st.tm.min = 0;
                            st.tm.sec = 0;
                            st.tm.isdst = 0;
                        }
                        _ => return Err(bad()),
                    },
                    DTZMOD => {
                        tmask |= dtk_m(DTZ);
                        st.tm.isdst = 1;
                        tz -= val;
                    }
                    DTZ => {
                        tmask |= dtk_m(TZ);
                        st.tm.isdst = 1;
                        tz = -val;
                        ftype[i] = DTK_TZ;
                    }
                    TZ => {
                        st.tm.isdst = 0;
                        tz = -val;
                        ftype[i] = DTK_TZ;
                    }
                    DYNTZ => {
                        tmask |= dtk_m(TZ);
                        abbrev_tz = valtz;
                        ftype[i] = DTK_TZ;
                    }
                    AMPM => mer = val,
                    ADBC => bc = val == BC,
                    UNITS => {
                        tmask = 0;
                        if ptype != 0 {
                            return Err(bad());
                        }
                        ptype = val;
                    }
                    ISOTIME => {
                        tmask = 0;
                        if ptype != 0 {
                            return Err(bad());
                        }
                        ptype = val;
                    }
                    UNKNOWN_FIELD => {
                        match tz::pg_tzset(&field) {
                            Some(z) => named_tz = Some(z.zone_ref()),
                            None => return Err(bad()),
                        }
                        tmask = dtk_m(TZ);
                    }
                    _ => return Err(bad()),
                }
            }
            _ => return Err(bad()),
        }
        if tmask & fmask != 0 {
            return Err(bad());
        }
        fmask |= tmask;
        i += 1;
    }

    if ptype != 0 {
        return Err(bad());
    }
    validate_date(fmask, isjulian, is2digits, bc, &mut st.tm, st.tm_yday).map_err(DtErr::Code)?;
    if mer != HR24 && st.tm.hour > HOURS_PER_DAY / 2 {
        return Err(overflow());
    }
    if mer == AM && st.tm.hour == HOURS_PER_DAY / 2 {
        st.tm.hour = 0;
    } else if mer == PM && st.tm.hour != HOURS_PER_DAY / 2 {
        st.tm.hour += HOURS_PER_DAY / 2;
    }
    if time_overflows(st.tm.hour, st.tm.min, st.tm.sec, st.fsec) {
        return Err(overflow());
    }
    if (fmask & DTK_TIME_M) != DTK_TIME_M {
        return Err(bad());
    }
    if let Some(z) = named_tz {
        if fmask & dtk_m(DTZMOD) != 0 {
            return Err(bad());
        }
        if let Some(off) = z.single_offset() {
            tz = -off;
        } else {
            if (fmask & DTK_DATE_M) != DTK_DATE_M {
                return Err(bad());
            }
            tz = tz::determine_offset(&mut st.tm, &z);
        }
    }
    if abbrev_tz.is_some() {
        if fmask & dtk_m(DTZMOD) != 0 {
            return Err(bad());
        }
        if (fmask & DTK_DATE_M) != 0 && (fmask & DTK_DATE_M) != DTK_DATE_M {
            return Err(bad());
        }
    }
    if fmask & dtk_m(TZ) == 0 {
        if fmask & dtk_m(DTZMOD) != 0 {
            return Err(bad());
        }
        if (fmask & DTK_DATE_M) != 0 && (fmask & DTK_DATE_M) != DTK_DATE_M {
            return Err(bad());
        }
    }
    Ok(Decoded {
        dtype,
        tm: st.tm,
        fsec: st.fsec,
        tz,
    })
}

// ---------------------------------------------------------------------------
// Interval decoding
// ---------------------------------------------------------------------------

fn int64_multiply_add(val: i64, multiplier: i64, sum: &mut i64) -> bool {
    match val.checked_mul(multiplier).and_then(|p| sum.checked_add(p)) {
        Some(v) => {
            *sum = v;
            true
        }
        None => false,
    }
}

fn adjust_fract_microseconds(frac: f64, scale: i64, itm: &mut ItmIn) -> bool {
    if frac == 0.0 {
        return true;
    }
    let frac = frac * scale as f64;
    let mut usec = frac as i64;
    let rem = frac - usec as f64;
    if rem > 0.5 {
        usec += 1;
    } else if rem < -0.5 {
        usec -= 1;
    }
    match itm.usec.checked_add(usec) {
        Some(v) => {
            itm.usec = v;
            true
        }
        None => false,
    }
}

fn adjust_fract_days(frac: f64, scale: i32, itm: &mut ItmIn) -> bool {
    if frac == 0.0 {
        return true;
    }
    let frac = frac * scale as f64;
    let extra_days = frac as i32;
    match itm.mday.checked_add(extra_days) {
        Some(v) => itm.mday = v,
        None => return false,
    }
    adjust_fract_microseconds(frac - extra_days as f64, USECS_PER_DAY, itm)
}

fn adjust_fract_years(frac: f64, scale: i32, itm: &mut ItmIn) -> bool {
    let extra_months = rint(frac * scale as f64 * MONTHS_PER_YEAR as f64) as i32;
    match itm.mon.checked_add(extra_months) {
        Some(v) => {
            itm.mon = v;
            true
        }
        None => false,
    }
}

fn adjust_microseconds(val: i64, fval: f64, scale: i64, itm: &mut ItmIn) -> bool {
    if !int64_multiply_add(val, scale, &mut itm.usec) {
        return false;
    }
    adjust_fract_microseconds(fval, scale, itm)
}

fn adjust_days(val: i64, scale: i32, itm: &mut ItmIn) -> bool {
    if val < i32::MIN as i64 || val > i32::MAX as i64 {
        return false;
    }
    match (val as i32)
        .checked_mul(scale)
        .and_then(|d| itm.mday.checked_add(d))
    {
        Some(v) => {
            itm.mday = v;
            true
        }
        None => false,
    }
}

fn adjust_months(val: i64, itm: &mut ItmIn) -> bool {
    if val < i32::MIN as i64 || val > i32::MAX as i64 {
        return false;
    }
    match itm.mon.checked_add(val as i32) {
        Some(v) => {
            itm.mon = v;
            true
        }
        None => false,
    }
}

fn adjust_years(val: i64, scale: i32, itm: &mut ItmIn) -> bool {
    if val < i32::MIN as i64 || val > i32::MAX as i64 {
        return false;
    }
    match (val as i32)
        .checked_mul(scale)
        .and_then(|y| itm.year.checked_add(y))
    {
        Some(v) => {
            itm.year = v;
            true
        }
        None => false,
    }
}

/// DecodeInterval(): returns (dtype, itm_in)
pub(crate) fn decode_interval(
    fields: &[String],
    ftype: &[i32],
    range: i32,
) -> DtResult<(i32, ItmIn)> {
    let nf = fields.len();
    let force_negative = false; // IntervalStyle != SQL_STANDARD
    let mut is_before = false;
    let mut parsing_unit_val = false;
    let mut fmask: i32 = 0;
    let mut tmask: i32;
    let mut ty: i32 = IGNORE_DTF;
    let mut dtype = DTK_DELTA;
    let mut itm = ItmIn::default();

    let mut i = nf as isize - 1;
    while i >= 0 {
        let iu = i as usize;
        let field = &fields[iu];
        let fb = field.as_bytes();
        let mut ftype_cur = ftype[iu];
        tmask = 0;
        if ftype_cur == DTK_TIME {
            decode_time_for_interval(fb, range, &mut tmask, &mut itm)?;
            if force_negative && itm.usec > 0 {
                itm.usec = -itm.usec;
            }
            ty = DTK_DAY;
            parsing_unit_val = false;
            if tmask & fmask != 0 {
                return Err(bad());
            }
            fmask |= tmask;
            i -= 1;
            continue;
        }
        if ftype_cur == DTK_TZ {
            let mut handled = false;
            if fb.len() > 1 && fb[1..].contains(&b':') {
                let mut tm2 = 0;
                let mut itm2 = itm;
                if decode_time_for_interval(&fb[1..], range, &mut tm2, &mut itm2).is_ok() {
                    itm = itm2;
                    tmask = tm2;
                    if fb[0] == b'-' {
                        if itm.usec == i64::MIN {
                            return Err(overflow());
                        }
                        itm.usec = -itm.usec;
                    }
                    if force_negative && itm.usec > 0 {
                        itm.usec = -itm.usec;
                    }
                    ty = DTK_DAY;
                    parsing_unit_val = false;
                    handled = true;
                }
            }
            if handled {
                if tmask & fmask != 0 {
                    return Err(bad());
                }
                fmask |= tmask;
                i -= 1;
                continue;
            }
            ftype_cur = DTK_NUMBER;
        }
        match ftype_cur {
            DTK_DATE | DTK_NUMBER => {
                if ty == IGNORE_DTF {
                    ty = match range {
                        x if x == interval_mask(YEAR) => DTK_YEAR,
                        x if x == interval_mask(MONTH)
                            || x == (interval_mask(YEAR) | interval_mask(MONTH)) =>
                        {
                            DTK_MONTH
                        }
                        x if x == interval_mask(DAY) => DTK_DAY,
                        x if x == interval_mask(HOUR)
                            || x == (interval_mask(DAY) | interval_mask(HOUR)) =>
                        {
                            DTK_HOUR
                        }
                        x if x == interval_mask(MINUTE)
                            || x == (interval_mask(HOUR) | interval_mask(MINUTE))
                            || x == (interval_mask(DAY)
                                | interval_mask(HOUR)
                                | interval_mask(MINUTE)) =>
                        {
                            DTK_MINUTE
                        }
                        _ => DTK_SECOND,
                    };
                }
                let (mut val, cp, erange) = c_strtol(fb);
                if erange {
                    return Err(overflow());
                }
                let c = byte_at(fb, cp);
                let mut fval: f64;
                if c == b'-' {
                    // SQL "years-months" syntax
                    let (mut val2, e2, erange2) = c_strtoint(&fb[cp + 1..]);
                    if erange2 || val2 < 0 || val2 >= MONTHS_PER_YEAR {
                        return Err(overflow());
                    }
                    if cp + 1 + e2 < fb.len() {
                        return Err(bad());
                    }
                    ty = DTK_MONTH;
                    if fb[0] == b'-' {
                        val2 = -val2;
                    }
                    val = val
                        .checked_mul(MONTHS_PER_YEAR as i64)
                        .ok_or_else(overflow)?;
                    val = val.checked_add(val2 as i64).ok_or_else(overflow)?;
                    fval = 0.0;
                } else if c == b'.' {
                    fval = parse_fraction(&fb[cp..])?;
                    if fb[0] == b'-' {
                        fval = -fval;
                    }
                } else if cp >= fb.len() {
                    fval = 0.0;
                } else {
                    return Err(bad());
                }
                if force_negative {
                    if val > 0 {
                        val = -val;
                    }
                    if fval > 0.0 {
                        fval = -fval;
                    }
                }
                match ty {
                    DTK_MICROSEC => {
                        if !adjust_microseconds(val, fval, 1, &mut itm) {
                            return Err(overflow());
                        }
                        tmask = dtk_m(MICROSECOND);
                    }
                    DTK_MILLISEC => {
                        if !adjust_microseconds(val, fval, 1000, &mut itm) {
                            return Err(overflow());
                        }
                        tmask = dtk_m(MILLISECOND);
                    }
                    DTK_SECOND => {
                        if !adjust_microseconds(val, fval, USECS_PER_SEC, &mut itm) {
                            return Err(overflow());
                        }
                        tmask = if fval == 0.0 {
                            dtk_m(SECOND)
                        } else {
                            DTK_ALL_SECS_M
                        };
                    }
                    DTK_MINUTE => {
                        if !adjust_microseconds(val, fval, USECS_PER_MINUTE, &mut itm) {
                            return Err(overflow());
                        }
                        tmask = dtk_m(MINUTE);
                    }
                    DTK_HOUR => {
                        if !adjust_microseconds(val, fval, USECS_PER_HOUR, &mut itm) {
                            return Err(overflow());
                        }
                        tmask = dtk_m(HOUR);
                        ty = DTK_DAY;
                    }
                    DTK_DAY => {
                        if !adjust_days(val, 1, &mut itm)
                            || !adjust_fract_microseconds(fval, USECS_PER_DAY, &mut itm)
                        {
                            return Err(overflow());
                        }
                        tmask = dtk_m(DAY);
                    }
                    DTK_WEEK => {
                        if !adjust_days(val, 7, &mut itm) || !adjust_fract_days(fval, 7, &mut itm) {
                            return Err(overflow());
                        }
                        tmask = dtk_m(WEEK);
                    }
                    DTK_MONTH => {
                        if !adjust_months(val, &mut itm)
                            || !adjust_fract_days(fval, DAYS_PER_MONTH, &mut itm)
                        {
                            return Err(overflow());
                        }
                        tmask = dtk_m(MONTH);
                    }
                    DTK_YEAR => {
                        if !adjust_years(val, 1, &mut itm) || !adjust_fract_years(fval, 1, &mut itm)
                        {
                            return Err(overflow());
                        }
                        tmask = dtk_m(YEAR);
                    }
                    DTK_DECADE => {
                        if !adjust_years(val, 10, &mut itm)
                            || !adjust_fract_years(fval, 10, &mut itm)
                        {
                            return Err(overflow());
                        }
                        tmask = dtk_m(DECADE);
                    }
                    DTK_CENTURY => {
                        if !adjust_years(val, 100, &mut itm)
                            || !adjust_fract_years(fval, 100, &mut itm)
                        {
                            return Err(overflow());
                        }
                        tmask = dtk_m(CENTURY);
                    }
                    DTK_MILLENNIUM => {
                        if !adjust_years(val, 1000, &mut itm)
                            || !adjust_fract_years(fval, 1000, &mut itm)
                        {
                            return Err(overflow());
                        }
                        tmask = dtk_m(MILLENNIUM);
                    }
                    _ => return Err(bad()),
                }
                parsing_unit_val = false;
            }
            DTK_STRING | DTK_SPECIAL => {
                if parsing_unit_val {
                    return Err(bad());
                }
                let (mut t, mut uval) = decode_units(field);
                if t == UNKNOWN_FIELD {
                    let (t2, v2) = decode_special(field);
                    t = t2;
                    uval = v2;
                }
                if t == IGNORE_DTF {
                    i -= 1;
                    continue;
                }
                tmask = 0;
                match t {
                    UNITS => {
                        ty = uval;
                        parsing_unit_val = true;
                    }
                    AGO => {
                        if iu != nf - 1 {
                            return Err(bad());
                        }
                        is_before = true;
                        ty = uval;
                    }
                    RESERV => {
                        tmask = DTK_DATE_M | DTK_TIME_M;
                        if uval != DTK_LATE && uval != DTK_EARLY {
                            return Err(bad());
                        }
                        if iu != nf - 1 {
                            return Err(bad());
                        }
                        dtype = uval;
                    }
                    _ => return Err(bad()),
                }
            }
            _ => return Err(bad()),
        }
        if tmask & fmask != 0 {
            return Err(bad());
        }
        fmask |= tmask;
        i -= 1;
    }
    if fmask == 0 {
        return Err(bad());
    }
    if parsing_unit_val {
        return Err(bad());
    }
    if is_before {
        if itm.usec == i64::MIN
            || itm.mday == i32::MIN
            || itm.mon == i32::MIN
            || itm.year == i32::MIN
        {
            return Err(overflow());
        }
        itm.usec = -itm.usec;
        itm.mday = -itm.mday;
        itm.mon = -itm.mon;
        itm.year = -itm.year;
    }
    Ok((dtype, itm))
}

/// ParseISO8601Number(): returns (ipart, fpart, new position)
fn parse_iso8601_number(s: &[u8], pos: usize) -> DtResult<(i64, f64, usize)> {
    let c = byte_at(s, pos);
    if !(c_isdigit(c) || c == b'-' || c == b'.') {
        return Err(bad());
    }
    let (val, end, erange) = c_strtod(&s[pos..]);
    if end == 0 || erange {
        return Err(bad());
    }
    if val.is_nan() || !(-1.0e15..=1.0e15).contains(&val) {
        return Err(overflow());
    }
    let ipart = if val >= 0.0 {
        val.floor() as i64
    } else {
        -((-val).floor()) as i64
    };
    let fpart = val - ipart as f64;
    Ok((ipart, fpart, pos + end))
}

fn iso8601_integer_width(s: &[u8], start: usize) -> usize {
    let mut p = start;
    if byte_at(s, p) == b'-' {
        p += 1;
    }
    let mut n = 0;
    while p < s.len() && c_isdigit(s[p]) {
        n += 1;
        p += 1;
    }
    n
}

/// DecodeISO8601Interval()
pub(crate) fn decode_iso8601_interval(sstr: &str) -> DtResult<(i32, ItmIn)> {
    let s = sstr.as_bytes();
    let mut itm = ItmIn::default();
    let mut datepart = true;
    let mut havefield = false;
    if s.len() < 2 || s[0] != b'P' {
        return Err(bad());
    }
    let mut pos = 1;
    while pos < s.len() {
        if s[pos] == b'T' {
            datepart = false;
            havefield = false;
            pos += 1;
            continue;
        }
        let fieldstart = pos;
        let (mut val, mut fval, np) = parse_iso8601_number(s, pos)?;
        pos = np;
        let unit = byte_at(s, pos);
        pos += 1; // may step past end; unit == 0 handled below
        if datepart {
            match unit {
                b'Y' => {
                    if !adjust_years(val, 1, &mut itm) || !adjust_fract_years(fval, 1, &mut itm) {
                        return Err(overflow());
                    }
                }
                b'M' => {
                    if !adjust_months(val, &mut itm)
                        || !adjust_fract_days(fval, DAYS_PER_MONTH, &mut itm)
                    {
                        return Err(overflow());
                    }
                }
                b'W' => {
                    if !adjust_days(val, 7, &mut itm) || !adjust_fract_days(fval, 7, &mut itm) {
                        return Err(overflow());
                    }
                }
                b'D' => {
                    if !adjust_days(val, 1, &mut itm)
                        || !adjust_fract_microseconds(fval, USECS_PER_DAY, &mut itm)
                    {
                        return Err(overflow());
                    }
                }
                b'T' | 0 | b'-' => {
                    if (unit == b'T' || unit == 0)
                        && iso8601_integer_width(s, fieldstart) == 8
                        && !havefield
                    {
                        if !adjust_years(val / 10000, 1, &mut itm)
                            || !adjust_months((val / 100) % 100, &mut itm)
                            || !adjust_days(val % 100, 1, &mut itm)
                            || !adjust_fract_microseconds(fval, USECS_PER_DAY, &mut itm)
                        {
                            return Err(overflow());
                        }
                        if unit == 0 {
                            return Ok((DTK_DELTA, itm));
                        }
                        datepart = false;
                        havefield = false;
                        continue;
                    }
                    // extended alternative format
                    if havefield {
                        return Err(bad());
                    }
                    if !adjust_years(val, 1, &mut itm) || !adjust_fract_years(fval, 1, &mut itm) {
                        return Err(overflow());
                    }
                    if unit == 0 {
                        return Ok((DTK_DELTA, itm));
                    }
                    if unit == b'T' {
                        datepart = false;
                        havefield = false;
                        continue;
                    }
                    let (v, f, np) = parse_iso8601_number(s, pos)?;
                    val = v;
                    fval = f;
                    pos = np;
                    if !adjust_months(val, &mut itm)
                        || !adjust_fract_days(fval, DAYS_PER_MONTH, &mut itm)
                    {
                        return Err(overflow());
                    }
                    if pos >= s.len() {
                        return Ok((DTK_DELTA, itm));
                    }
                    if s[pos] == b'T' {
                        datepart = false;
                        havefield = false;
                        continue;
                    }
                    if s[pos] != b'-' {
                        return Err(bad());
                    }
                    pos += 1;
                    let (v, f, np) = parse_iso8601_number(s, pos)?;
                    val = v;
                    fval = f;
                    pos = np;
                    if !adjust_days(val, 1, &mut itm)
                        || !adjust_fract_microseconds(fval, USECS_PER_DAY, &mut itm)
                    {
                        return Err(overflow());
                    }
                    if pos >= s.len() {
                        return Ok((DTK_DELTA, itm));
                    }
                    if s[pos] == b'T' {
                        datepart = false;
                        havefield = false;
                        continue;
                    }
                    return Err(bad());
                }
                _ => return Err(bad()),
            }
        } else {
            match unit {
                b'H' => {
                    if !adjust_microseconds(val, fval, USECS_PER_HOUR, &mut itm) {
                        return Err(overflow());
                    }
                }
                b'M' => {
                    if !adjust_microseconds(val, fval, USECS_PER_MINUTE, &mut itm) {
                        return Err(overflow());
                    }
                }
                b'S' => {
                    if !adjust_microseconds(val, fval, USECS_PER_SEC, &mut itm) {
                        return Err(overflow());
                    }
                }
                0 | b':' => {
                    if unit == 0 && iso8601_integer_width(s, fieldstart) == 6 && !havefield {
                        if !adjust_microseconds(val / 10000, 0.0, USECS_PER_HOUR, &mut itm)
                            || !adjust_microseconds(
                                (val / 100) % 100,
                                0.0,
                                USECS_PER_MINUTE,
                                &mut itm,
                            )
                            || !adjust_microseconds(val % 100, 0.0, USECS_PER_SEC, &mut itm)
                            || !adjust_fract_microseconds(fval, 1, &mut itm)
                        {
                            return Err(overflow());
                        }
                        return Ok((DTK_DELTA, itm));
                    }
                    if havefield {
                        return Err(bad());
                    }
                    if !adjust_microseconds(val, fval, USECS_PER_HOUR, &mut itm) {
                        return Err(overflow());
                    }
                    if unit == 0 {
                        return Ok((DTK_DELTA, itm));
                    }
                    let (v, f, np) = parse_iso8601_number(s, pos)?;
                    val = v;
                    fval = f;
                    pos = np;
                    if !adjust_microseconds(val, fval, USECS_PER_MINUTE, &mut itm) {
                        return Err(overflow());
                    }
                    if pos >= s.len() {
                        return Ok((DTK_DELTA, itm));
                    }
                    if s[pos] != b':' {
                        return Err(bad());
                    }
                    pos += 1;
                    let (v, f, np) = parse_iso8601_number(s, pos)?;
                    val = v;
                    fval = f;
                    pos = np;
                    if !adjust_microseconds(val, fval, USECS_PER_SEC, &mut itm) {
                        return Err(overflow());
                    }
                    if pos >= s.len() {
                        return Ok((DTK_DELTA, itm));
                    }
                    return Err(bad());
                }
                _ => return Err(bad()),
            }
        }
        havefield = true;
    }
    Ok((DTK_DELTA, itm))
}
