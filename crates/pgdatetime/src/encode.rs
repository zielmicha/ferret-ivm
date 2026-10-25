//! Date/time output: port of EncodeDateOnly(), EncodeTimeOnly(),
//! EncodeDateTime(), EncodeInterval() and helpers from PostgreSQL's
//! datetime.c (PostgreSQL License).

use std::fmt::Write;

use crate::core::*;

/// IntervalStyle setting for interval output.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum IntervalStyle {
    #[default]
    Postgres,
    PostgresVerbose,
    SqlStandard,
    Iso8601,
}

/// pg_ultostr_zeropad()
#[inline]
pub(crate) fn push_zeropad(out: &mut String, value: i64, width: usize) {
    let _ = write!(out, "{:0width$}", value, width = width);
}

/// AppendSeconds(): seconds and (trailing-zero-stripped) fraction; signs
/// are stripped.
pub(crate) fn append_seconds(
    out: &mut String,
    sec: i32,
    fsec: i32,
    precision: usize,
    fillzeros: bool,
) {
    let asec = (sec as i64).abs();
    if fillzeros {
        push_zeropad(out, asec, 2);
    } else {
        let _ = write!(out, "{}", asec);
    }
    if fsec != 0 {
        let afsec = (fsec as i64).abs();
        let mut value = afsec;
        let mut digits = [0u8; 16];
        let mut gotnonzero = false;
        let mut end = 0usize;
        let mut p = precision;
        while p > 0 {
            p -= 1;
            let oldval = value;
            value /= 10;
            let rem = oldval - value * 10;
            if rem != 0 {
                gotnonzero = true;
            }
            if gotnonzero {
                digits[p] = b'0' + rem as u8;
                if end == 0 {
                    end = p + 1;
                }
            }
        }
        out.push('.');
        if value != 0 {
            let _ = write!(out, "{}", afsec);
            return;
        }
        for d in digits.iter().take(end) {
            out.push(*d as char);
        }
    }
}

/// EncodeTimezone(): tz is seconds WEST of UTC.
pub(crate) fn encode_timezone(out: &mut String, tz: i32, xsd: bool) {
    let mut sec = (tz as i64).abs();
    let mut min = sec / 60;
    sec -= min * 60;
    let hour = min / 60;
    min -= hour * 60;
    out.push(if tz <= 0 { '+' } else { '-' });
    if sec != 0 {
        push_zeropad(out, hour, 2);
        out.push(':');
        push_zeropad(out, min, 2);
        out.push(':');
        push_zeropad(out, sec, 2);
    } else if min != 0 || xsd {
        push_zeropad(out, hour, 2);
        out.push(':');
        push_zeropad(out, min, 2);
    } else {
        push_zeropad(out, hour, 2);
    }
}

#[inline]
fn display_year(y: i32) -> i64 {
    if y > 0 { y as i64 } else { -(y as i64 - 1) }
}

/// EncodeDateOnly() for DateStyle ISO
pub(crate) fn encode_date_only(tm: &Tm) -> String {
    let mut out = String::with_capacity(16);
    push_zeropad(&mut out, display_year(tm.year), 4);
    out.push('-');
    push_zeropad(&mut out, tm.mon as i64, 2);
    out.push('-');
    push_zeropad(&mut out, tm.mday as i64, 2);
    if tm.year <= 0 {
        out.push_str(" BC");
    }
    out
}

/// EncodeTimeOnly() without time zone
pub(crate) fn encode_time_only(tm: &Tm, fsec: i32) -> String {
    let mut out = String::with_capacity(16);
    push_zeropad(&mut out, tm.hour as i64, 2);
    out.push(':');
    push_zeropad(&mut out, tm.min as i64, 2);
    out.push(':');
    append_seconds(&mut out, tm.sec, fsec, MAX_TIME_PRECISION as usize, true);
    out
}

/// EncodeDateTime() for DateStyle ISO
pub(crate) fn encode_date_time(tm: &Tm, fsec: i32, print_tz: bool, tz: i32) -> String {
    let mut out = String::with_capacity(32);
    let print_tz = print_tz && tm.isdst >= 0;
    push_zeropad(&mut out, display_year(tm.year), 4);
    out.push('-');
    push_zeropad(&mut out, tm.mon as i64, 2);
    out.push('-');
    push_zeropad(&mut out, tm.mday as i64, 2);
    out.push(' ');
    push_zeropad(&mut out, tm.hour as i64, 2);
    out.push(':');
    push_zeropad(&mut out, tm.min as i64, 2);
    out.push(':');
    append_seconds(
        &mut out,
        tm.sec,
        fsec,
        MAX_TIMESTAMP_PRECISION as usize,
        true,
    );
    if print_tz {
        encode_timezone(&mut out, tz, false);
    }
    if tm.year <= 0 {
        out.push_str(" BC");
    }
    out
}

fn add_iso8601_int_part(out: &mut String, value: i64, units: char) {
    if value == 0 {
        return;
    }
    let _ = write!(out, "{}{}", value, units);
}

fn add_postgres_int_part(
    out: &mut String,
    value: i64,
    units: &str,
    is_zero: &mut bool,
    is_before: &mut bool,
) {
    if value == 0 {
        return;
    }
    let _ = write!(
        out,
        "{}{}{} {}{}",
        if !*is_zero { " " } else { "" },
        if *is_before && value > 0 { "+" } else { "" },
        value,
        units,
        if value != 1 { "s" } else { "" }
    );
    *is_before = value < 0;
    *is_zero = false;
}

fn add_verbose_int_part(
    out: &mut String,
    value: i64,
    units: &str,
    is_zero: &mut bool,
    is_before: &mut bool,
) {
    if value == 0 {
        return;
    }
    let mut value = value;
    if *is_zero {
        *is_before = value < 0;
        value = value.abs();
    } else if *is_before {
        value = -value;
    }
    let _ = write!(
        out,
        " {} {}{}",
        value,
        units,
        if value == 1 { "" } else { "s" }
    );
    *is_zero = false;
}

/// EncodeInterval()
pub(crate) fn encode_interval(itm: &Itm, style: IntervalStyle) -> String {
    let mut out = String::with_capacity(48);
    let mut year = itm.year as i64;
    let mut mon = itm.mon as i64;
    let mut mday = itm.mday as i64;
    let mut hour = itm.hour;
    let mut min = itm.min as i64;
    let mut sec = itm.sec;
    let mut fsec = itm.usec;
    let mut is_before = false;
    let mut is_zero = true;
    match style {
        IntervalStyle::SqlStandard => {
            let has_negative =
                year < 0 || mon < 0 || mday < 0 || hour < 0 || min < 0 || sec < 0 || fsec < 0;
            let has_positive =
                year > 0 || mon > 0 || mday > 0 || hour > 0 || min > 0 || sec > 0 || fsec > 0;
            let has_year_month = year != 0 || mon != 0;
            let has_day_time = mday != 0 || hour != 0 || min != 0 || sec != 0 || fsec != 0;
            let has_day = mday != 0;
            let sql_standard_value =
                !(has_negative && has_positive) && !(has_year_month && has_day_time);
            if has_negative && sql_standard_value {
                out.push('-');
                year = -year;
                mon = -mon;
                mday = -mday;
                hour = -hour;
                min = -min;
                sec = -sec;
                fsec = -fsec;
            }
            if !has_negative && !has_positive {
                out.push('0');
            } else if !sql_standard_value {
                let year_sign = if year < 0 || mon < 0 { '-' } else { '+' };
                let day_sign = if mday < 0 { '-' } else { '+' };
                let sec_sign = if hour < 0 || min < 0 || sec < 0 || fsec < 0 {
                    '-'
                } else {
                    '+'
                };
                let _ = write!(
                    out,
                    "{}{}-{} {}{} {}{}:{:02}:",
                    year_sign,
                    year.abs(),
                    mon.abs(),
                    day_sign,
                    mday.abs(),
                    sec_sign,
                    hour.unsigned_abs(),
                    min.abs()
                );
                append_seconds(&mut out, sec, fsec, MAX_INTERVAL_PRECISION as usize, true);
            } else if has_year_month {
                let _ = write!(out, "{}-{}", year, mon);
            } else if has_day {
                let _ = write!(out, "{} {}:{:02}:", mday, hour, min);
                append_seconds(&mut out, sec, fsec, MAX_INTERVAL_PRECISION as usize, true);
            } else {
                let _ = write!(out, "{}:{:02}:", hour, min);
                append_seconds(&mut out, sec, fsec, MAX_INTERVAL_PRECISION as usize, true);
            }
        }
        IntervalStyle::Iso8601 => {
            if year == 0 && mon == 0 && mday == 0 && hour == 0 && min == 0 && sec == 0 && fsec == 0
            {
                out.push_str("PT0S");
                return out;
            }
            out.push('P');
            add_iso8601_int_part(&mut out, year, 'Y');
            add_iso8601_int_part(&mut out, mon, 'M');
            add_iso8601_int_part(&mut out, mday, 'D');
            if hour != 0 || min != 0 || sec != 0 || fsec != 0 {
                out.push('T');
            }
            add_iso8601_int_part(&mut out, hour, 'H');
            add_iso8601_int_part(&mut out, min, 'M');
            if sec != 0 || fsec != 0 {
                if sec < 0 || fsec < 0 {
                    out.push('-');
                }
                append_seconds(&mut out, sec, fsec, MAX_INTERVAL_PRECISION as usize, false);
                out.push('S');
            }
        }
        IntervalStyle::Postgres => {
            add_postgres_int_part(&mut out, year, "year", &mut is_zero, &mut is_before);
            add_postgres_int_part(&mut out, mon, "mon", &mut is_zero, &mut is_before);
            add_postgres_int_part(&mut out, mday, "day", &mut is_zero, &mut is_before);
            if is_zero || hour != 0 || min != 0 || sec != 0 || fsec != 0 {
                let minus = hour < 0 || min < 0 || sec < 0 || fsec < 0;
                let _ = write!(
                    out,
                    "{}{}{:02}:{:02}:",
                    if is_zero { "" } else { " " },
                    if minus {
                        "-"
                    } else if is_before {
                        "+"
                    } else {
                        ""
                    },
                    hour.unsigned_abs(),
                    min.abs()
                );
                append_seconds(&mut out, sec, fsec, MAX_INTERVAL_PRECISION as usize, true);
            }
        }
        IntervalStyle::PostgresVerbose => {
            out.push('@');
            add_verbose_int_part(&mut out, year, "year", &mut is_zero, &mut is_before);
            add_verbose_int_part(&mut out, mon, "mon", &mut is_zero, &mut is_before);
            add_verbose_int_part(&mut out, mday, "day", &mut is_zero, &mut is_before);
            add_verbose_int_part(&mut out, hour, "hour", &mut is_zero, &mut is_before);
            add_verbose_int_part(&mut out, min, "min", &mut is_zero, &mut is_before);
            if sec != 0 || fsec != 0 {
                out.push(' ');
                if sec < 0 || (sec == 0 && fsec < 0) {
                    if is_zero {
                        is_before = true;
                    } else if !is_before {
                        out.push('-');
                    }
                } else if is_before {
                    out.push('-');
                }
                append_seconds(&mut out, sec, fsec, MAX_INTERVAL_PRECISION as usize, false);
                let _ = write!(
                    out,
                    " sec{}",
                    if sec.abs() != 1 || fsec != 0 { "s" } else { "" }
                );
                is_zero = false;
            }
            if is_zero {
                out.push_str(" 0");
            }
            if is_before {
                out.push_str(" ago");
            }
        }
    }
    out
}
