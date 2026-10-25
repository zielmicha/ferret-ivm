//! EXTRACT(), date_part() and date_trunc(): ports of timestamp_part_common(),
//! timestamptz_part_common(), interval_part_common(), extract_date(),
//! time_part_common(), timestamp_trunc(), timestamptz_trunc_internal() and
//! interval_trunc() (PostgreSQL License).

use crate::core::*;
use crate::datetime::*;
use crate::decimal::numeric_div_i64;
use crate::error::*;
use crate::io::EPOCH_TIMESTAMP;
use crate::tz::{self, ZoneRef};
use crate::{Date, Decimal, Error, Interval, Result, Time, TimeZone, Timestamp, TimestampTz};

const T_TIMESTAMP: &str = "timestamp without time zone";
const T_TIMESTAMPTZ: &str = "timestamp with time zone";
const T_DATE: &str = "date";
const T_TIME: &str = "time without time zone";
const T_INTERVAL: &str = "interval";

/// A date_part / extract result before conversion to numeric or float8.
#[derive(Clone, Copy, Debug)]
enum Part {
    Int(i64),
    /// value / 10^scale computed exactly (int64_div_fast_to_numeric)
    Scaled(i64, u32),
    /// numeric with explicit (value, scale) and an independently computed float
    Numeric(i128, u32, f64),
    Float(f64),
    PosInf,
    NegInf,
    Null,
}

fn not_recognized(lowunits: &str, ty: &str) -> Error {
    Error::new(
        INVALID_PARAMETER_VALUE,
        format!("unit \"{}\" not recognized for type {}", lowunits, ty),
    )
}

fn not_supported(lowunits: &str, ty: &str) -> Error {
    Error::new(
        FEATURE_NOT_SUPPORTED,
        format!("unit \"{}\" not supported for type {}", lowunits, ty),
    )
}

fn lookup_units(field: &str) -> (String, i32, i32) {
    let lowunits = tz::downcase_truncate(field);
    let (mut ty, mut val) = decode_units(&lowunits);
    if ty == UNKNOWN_FIELD {
        let (t, v) = decode_special(&lowunits);
        ty = t;
        val = v;
    }
    (lowunits, ty, val)
}

fn to_decimal(p: Part) -> Option<Decimal> {
    match p {
        Part::Int(v) => Some(Decimal::new(v as i128, 0)),
        Part::Scaled(v, s) => Some(Decimal::new(v as i128, s)),
        Part::Numeric(v, s, _) => Some(Decimal::new(v, s)),
        Part::Float(f) => Some(Decimal::new(f as i128, 0)),
        Part::PosInf => Some(Decimal::INFINITY),
        Part::NegInf => Some(Decimal::NEG_INFINITY),
        Part::Null => None,
    }
}

fn to_float(p: Part) -> Option<f64> {
    match p {
        Part::Int(v) => Some(v as f64),
        Part::Scaled(v, _) => Some(v as f64),
        Part::Numeric(_, _, f) => Some(f),
        Part::Float(f) => Some(f),
        Part::PosInf => Some(f64::INFINITY),
        Part::NegInf => Some(f64::NEG_INFINITY),
        Part::Null => None,
    }
}

/// NonFiniteTimestampTzPart()
fn non_finite_timestamp_part(
    ty: i32,
    unit: i32,
    lowunits: &str,
    is_negative: bool,
    tyname: &str,
) -> Result<Part> {
    if ty != UNITS && ty != RESERV {
        return Err(not_recognized(lowunits, tyname));
    }
    match unit {
        DTK_MICROSEC | DTK_MILLISEC | DTK_SECOND | DTK_MINUTE | DTK_HOUR | DTK_DAY | DTK_MONTH
        | DTK_QUARTER | DTK_WEEK | DTK_DOW | DTK_ISODOW | DTK_DOY | DTK_TZ | DTK_TZ_MINUTE
        | DTK_TZ_HOUR => Ok(Part::Null),
        DTK_YEAR | DTK_DECADE | DTK_CENTURY | DTK_MILLENNIUM | DTK_JULIAN | DTK_ISOYEAR
        | DTK_EPOCH => Ok(if is_negative {
            Part::NegInf
        } else {
            Part::PosInf
        }),
        _ => Err(not_supported(lowunits, tyname)),
    }
}

/// The calendar units shared by timestamp and timestamptz.  `retnumeric`
/// selects the numeric or float8 computation where they differ.
fn tm_part(val: i32, tm: &Tm, fsec: i32, retnumeric: bool) -> Option<Part> {
    let usec_of_minute = tm.sec as i64 * 1_000_000 + fsec as i64;
    Some(match val {
        DTK_MICROSEC => Part::Int(usec_of_minute),
        DTK_MILLISEC => {
            if retnumeric {
                Part::Scaled(usec_of_minute, 3)
            } else {
                Part::Float(tm.sec as f64 * 1000.0 + fsec as f64 / 1000.0)
            }
        }
        DTK_SECOND => {
            if retnumeric {
                Part::Scaled(usec_of_minute, 6)
            } else {
                Part::Float(tm.sec as f64 + fsec as f64 / 1_000_000.0)
            }
        }
        DTK_MINUTE => Part::Int(tm.min as i64),
        DTK_HOUR => Part::Int(tm.hour as i64),
        DTK_DAY => Part::Int(tm.mday as i64),
        DTK_MONTH => Part::Int(tm.mon as i64),
        DTK_QUARTER => Part::Int(((tm.mon - 1) / 3 + 1) as i64),
        DTK_WEEK => Part::Int(date2isoweek(tm.year, tm.mon, tm.mday) as i64),
        DTK_YEAR => Part::Int(if tm.year > 0 {
            tm.year as i64
        } else {
            tm.year as i64 - 1
        }),
        DTK_DECADE => Part::Int(if tm.year >= 0 {
            (tm.year / 10) as i64
        } else {
            -((8 - (tm.year - 1)) / 10) as i64
        }),
        DTK_CENTURY => Part::Int(if tm.year > 0 {
            ((tm.year + 99) / 100) as i64
        } else {
            -((99 - (tm.year - 1)) / 100) as i64
        }),
        DTK_MILLENNIUM => Part::Int(if tm.year > 0 {
            ((tm.year + 999) / 1000) as i64
        } else {
            -((999 - (tm.year - 1)) / 1000) as i64
        }),
        DTK_JULIAN => {
            let jd = date2j(tm.year, tm.mon, tm.mday) as i64;
            let usecs = (((tm.hour as i64 * MINS_PER_HOUR) + tm.min as i64) * SECS_PER_MINUTE
                + tm.sec as i64)
                * 1_000_000
                + fsec as i64;
            let fl = jd as f64
                + (((tm.hour as i64 * MINS_PER_HOUR + tm.min as i64) * SECS_PER_MINUTE) as f64
                    + tm.sec as f64
                    + (fsec as f64 / 1_000_000.0))
                    / SECS_PER_DAY as f64;
            if retnumeric {
                let (q, scale) = numeric_div_i64(usecs, SECS_PER_DAY * 1_000_000);
                Part::Numeric(jd as i128 * 10i128.pow(scale) + q, scale, fl)
            } else {
                Part::Float(fl)
            }
        }
        DTK_ISOYEAR => {
            let mut r = date2isoyear(tm.year, tm.mon, tm.mday) as i64;
            if r <= 0 {
                r -= 1;
            }
            Part::Int(r)
        }
        DTK_DOW | DTK_ISODOW => {
            let mut r = j2day(date2j(tm.year, tm.mon, tm.mday)) as i64;
            if val == DTK_ISODOW && r == 0 {
                r = 7;
            }
            Part::Int(r)
        }
        DTK_DOY => Part::Int((date2j(tm.year, tm.mon, tm.mday) - date2j(tm.year, 1, 1) + 1) as i64),
        _ => return None,
    })
}

fn epoch_part(timestamp: i64, retnumeric: bool) -> Part {
    let epoch = EPOCH_TIMESTAMP;
    if retnumeric {
        if timestamp < i64::MAX + epoch {
            Part::Scaled128(timestamp as i128 - epoch as i128, 6)
        } else {
            // numeric_div() then numeric_round(..., 6), as PostgreSQL does
            let (q, s) =
                crate::decimal::numeric_div_i128(timestamp as i128 - epoch as i128, 1_000_000);
            let (v, s) = crate::decimal::numeric_round_to(q, s, 6);
            Part::Scaled128(v, s)
        }
    } else if timestamp < i64::MAX + epoch {
        Part::Float((timestamp - epoch) as f64 / 1_000_000.0)
    } else {
        Part::Float((timestamp as f64 - epoch as f64) / 1_000_000.0)
    }
}

#[allow(non_snake_case)]
impl Part {
    fn Scaled128(v: i128, s: u32) -> Part {
        Part::Numeric(v, s, 0.0)
    }
}

/// timestamp_part_common()
fn timestamp_part(field: &str, timestamp: Timestamp, retnumeric: bool) -> Result<Part> {
    let (lowunits, ty, val) = lookup_units(field);
    if timestamp_not_finite(timestamp) {
        return non_finite_timestamp_part(ty, val, &lowunits, timestamp == DT_NOBEGIN, T_TIMESTAMP);
    }
    if ty == UNITS {
        let (tm, fsec) = timestamp2tm(timestamp)?;
        match tm_part(val, &tm, fsec, retnumeric) {
            Some(p) => Ok(p),
            None => Err(not_supported(&lowunits, T_TIMESTAMP)),
        }
    } else if ty == RESERV {
        match val {
            DTK_EPOCH => Ok(epoch_part(timestamp, retnumeric)),
            _ => Err(not_supported(&lowunits, T_TIMESTAMP)),
        }
    } else {
        Err(not_recognized(&lowunits, T_TIMESTAMP))
    }
}

/// timestamptz_part_common()
fn timestamptz_part(
    field: &str,
    timestamp: TimestampTz,
    zone: &ZoneRef,
    retnumeric: bool,
) -> Result<Part> {
    let (lowunits, ty, val) = lookup_units(field);
    if timestamp_not_finite(timestamp) {
        return non_finite_timestamp_part(
            ty,
            val,
            &lowunits,
            timestamp == DT_NOBEGIN,
            T_TIMESTAMPTZ,
        );
    }
    if ty == UNITS {
        let tt = timestamp2tm_tz(timestamp, zone)?;
        let tzv = tt.tz;
        match val {
            DTK_TZ => Ok(Part::Int(-(tzv as i64))),
            DTK_TZ_MINUTE => Ok(Part::Int(((tzv.wrapping_neg() / 60) % 60) as i64)),
            DTK_TZ_HOUR => Ok(Part::Int((tzv.wrapping_neg() / 3600) as i64)),
            _ => match tm_part(val, &tt.tm, tt.fsec, retnumeric) {
                Some(p) => Ok(p),
                None => Err(not_supported(&lowunits, T_TIMESTAMPTZ)),
            },
        }
    } else if ty == RESERV {
        match val {
            DTK_EPOCH => Ok(epoch_part(timestamp, retnumeric)),
            _ => Err(not_supported(&lowunits, T_TIMESTAMPTZ)),
        }
    } else {
        Err(not_recognized(&lowunits, T_TIMESTAMPTZ))
    }
}

/// extract_date()
fn date_part_impl(field: &str, date: Date) -> Result<Part> {
    let (lowunits, ty, val) = lookup_units(field);
    if date_not_finite(date) && (ty == UNITS || ty == RESERV) {
        return match val {
            DTK_DAY | DTK_MONTH | DTK_QUARTER | DTK_WEEK | DTK_DOW | DTK_ISODOW | DTK_DOY => {
                Ok(Part::Null)
            }
            DTK_YEAR | DTK_DECADE | DTK_CENTURY | DTK_MILLENNIUM | DTK_JULIAN | DTK_ISOYEAR
            | DTK_EPOCH => Ok(if date == DATEVAL_NOBEGIN {
                Part::NegInf
            } else {
                Part::PosInf
            }),
            _ => Err(not_supported(&lowunits, T_DATE)),
        };
    }
    if ty == UNITS {
        let (year, mon, mday) = j2date(date.wrapping_add(POSTGRES_EPOCH_JDATE));
        let r: i64 = match val {
            DTK_DAY => mday as i64,
            DTK_MONTH => mon as i64,
            DTK_QUARTER => ((mon - 1) / 3 + 1) as i64,
            DTK_WEEK => date2isoweek(year, mon, mday) as i64,
            DTK_YEAR => {
                if year > 0 {
                    year as i64
                } else {
                    year as i64 - 1
                }
            }
            DTK_DECADE => {
                if year >= 0 {
                    (year / 10) as i64
                } else {
                    -((8 - (year - 1)) / 10) as i64
                }
            }
            DTK_CENTURY => {
                if year > 0 {
                    ((year + 99) / 100) as i64
                } else {
                    -((99 - (year - 1)) / 100) as i64
                }
            }
            DTK_MILLENNIUM => {
                if year > 0 {
                    ((year + 999) / 1000) as i64
                } else {
                    -((999 - (year - 1)) / 1000) as i64
                }
            }
            DTK_JULIAN => date as i64 + POSTGRES_EPOCH_JDATE as i64,
            DTK_ISOYEAR => {
                let mut r = date2isoyear(year, mon, mday) as i64;
                if r <= 0 {
                    r -= 1;
                }
                r
            }
            DTK_DOW | DTK_ISODOW => {
                let mut r = j2day(date.wrapping_add(POSTGRES_EPOCH_JDATE)) as i64;
                if val == DTK_ISODOW && r == 0 {
                    r = 7;
                }
                r
            }
            DTK_DOY => (date2j(year, mon, mday) - date2j(year, 1, 1) + 1) as i64,
            _ => return Err(not_supported(&lowunits, T_DATE)),
        };
        Ok(Part::Int(r))
    } else if ty == RESERV {
        match val {
            DTK_EPOCH => Ok(Part::Int(
                (date as i64 + POSTGRES_EPOCH_JDATE as i64 - UNIX_EPOCH_JDATE as i64)
                    * SECS_PER_DAY,
            )),
            _ => Err(not_supported(&lowunits, T_DATE)),
        }
    } else {
        Err(not_recognized(&lowunits, T_DATE))
    }
}

/// time_part_common()
fn time_part(field: &str, time: Time, retnumeric: bool) -> Result<Part> {
    let (lowunits, ty, val) = lookup_units(field);
    if ty == UNITS {
        let (tm, fsec) = crate::io::time2tm(time);
        let usec_of_minute = tm.sec as i64 * 1_000_000 + fsec as i64;
        match val {
            DTK_MICROSEC => Ok(Part::Int(usec_of_minute)),
            DTK_MILLISEC => Ok(if retnumeric {
                Part::Scaled(usec_of_minute, 3)
            } else {
                Part::Float(tm.sec as f64 * 1000.0 + fsec as f64 / 1000.0)
            }),
            DTK_SECOND => Ok(if retnumeric {
                Part::Scaled(usec_of_minute, 6)
            } else {
                Part::Float(tm.sec as f64 + fsec as f64 / 1_000_000.0)
            }),
            DTK_MINUTE => Ok(Part::Int(tm.min as i64)),
            DTK_HOUR => Ok(Part::Int(tm.hour as i64)),
            _ => Err(not_supported(&lowunits, T_TIME)),
        }
    } else if ty == RESERV && val == DTK_EPOCH {
        Ok(if retnumeric {
            Part::Scaled(time, 6)
        } else {
            Part::Float(time as f64 / 1_000_000.0)
        })
    } else {
        Err(not_recognized(&lowunits, T_TIME))
    }
}

/// NonFiniteIntervalPart()
fn non_finite_interval_part(ty: i32, unit: i32, lowunits: &str, is_negative: bool) -> Result<Part> {
    if ty != UNITS && ty != RESERV {
        return Err(not_recognized(lowunits, T_INTERVAL));
    }
    match unit {
        DTK_MICROSEC | DTK_MILLISEC | DTK_SECOND | DTK_MINUTE | DTK_MONTH | DTK_QUARTER => {
            Ok(Part::Null)
        }
        DTK_HOUR | DTK_DAY | DTK_YEAR | DTK_DECADE | DTK_CENTURY | DTK_MILLENNIUM | DTK_EPOCH => {
            Ok(if is_negative {
                Part::NegInf
            } else {
                Part::PosInf
            })
        }
        _ => Err(not_supported(lowunits, T_INTERVAL)),
    }
}

/// interval_part_common()
fn interval_part(field: &str, interval: Interval, retnumeric: bool) -> Result<Part> {
    let (lowunits, ty, val) = lookup_units(field);
    if interval_not_finite(&interval) {
        return non_finite_interval_part(ty, val, &lowunits, interval_is_nobegin(&interval));
    }
    if ty == UNITS {
        let tm = interval2itm(interval);
        let usec_of_minute = tm.sec as i64 * 1_000_000 + tm.usec as i64;
        match val {
            DTK_MICROSEC => Ok(Part::Int(usec_of_minute)),
            DTK_MILLISEC => Ok(if retnumeric {
                Part::Scaled(usec_of_minute, 3)
            } else {
                Part::Float(tm.sec as f64 * 1000.0 + tm.usec as f64 / 1000.0)
            }),
            DTK_SECOND => Ok(if retnumeric {
                Part::Scaled(usec_of_minute, 6)
            } else {
                Part::Float(tm.sec as f64 + tm.usec as f64 / 1_000_000.0)
            }),
            DTK_MINUTE => Ok(Part::Int(tm.min as i64)),
            DTK_HOUR => Ok(Part::Int(tm.hour)),
            DTK_DAY => Ok(Part::Int(tm.mday as i64)),
            DTK_MONTH => Ok(Part::Int(tm.mon as i64)),
            DTK_QUARTER => Ok(Part::Int((tm.mon / 3 + 1) as i64)),
            DTK_YEAR => Ok(Part::Int(tm.year as i64)),
            DTK_DECADE => Ok(Part::Int((tm.year / 10) as i64)),
            DTK_CENTURY => Ok(Part::Int((tm.year / 100) as i64)),
            DTK_MILLENNIUM => Ok(Part::Int((tm.year / 1000) as i64)),
            _ => Err(not_supported(&lowunits, T_INTERVAL)),
        }
    } else if ty == RESERV && val == DTK_EPOCH {
        if retnumeric {
            let secs_from_day_month: i64 = ((4.0 * DAYS_PER_YEAR) as i64
                * (interval.months / MONTHS_PER_YEAR) as i64
                + (4 * DAYS_PER_MONTH) as i64 * (interval.months % MONTHS_PER_YEAR) as i64
                + 4 * interval.days as i64)
                * (SECS_PER_DAY / 4);
            Ok(Part::Numeric(
                secs_from_day_month as i128 * 1_000_000 + interval.micros as i128,
                6,
                0.0,
            ))
        } else {
            let mut result = interval.micros as f64 / 1_000_000.0;
            result +=
                (DAYS_PER_YEAR * SECS_PER_DAY as f64) * (interval.months / MONTHS_PER_YEAR) as f64;
            result += (DAYS_PER_MONTH as f64 * SECS_PER_DAY as f64)
                * (interval.months % MONTHS_PER_YEAR) as f64;
            result += SECS_PER_DAY as f64 * interval.days as f64;
            Ok(Part::Float(result))
        }
    } else {
        Err(not_recognized(&lowunits, T_INTERVAL))
    }
}

// --- public API ---------------------------------------------------------------

pub fn extract_date_opt(field: &str, d: Date) -> Result<Option<Decimal>> {
    Ok(to_decimal(date_part_impl(field, d)?))
}
pub fn extract_date(field: &str, d: Date) -> Result<Decimal> {
    extract_date_opt(field, d)?.ok_or_else(null_result)
}
pub fn extract_time_opt(field: &str, t: Time) -> Result<Option<Decimal>> {
    Ok(to_decimal(time_part(field, t, true)?))
}
pub fn extract_time(field: &str, t: Time) -> Result<Decimal> {
    extract_time_opt(field, t)?.ok_or_else(null_result)
}
pub fn extract_timestamp_opt(field: &str, ts: Timestamp) -> Result<Option<Decimal>> {
    Ok(to_decimal(timestamp_part(field, ts, true)?))
}
pub fn extract_timestamp(field: &str, ts: Timestamp) -> Result<Decimal> {
    extract_timestamp_opt(field, ts)?.ok_or_else(null_result)
}
pub fn extract_timestamptz_opt(
    field: &str,
    t: TimestampTz,
    tz: &TimeZone,
) -> Result<Option<Decimal>> {
    Ok(to_decimal(timestamptz_part(
        field,
        t,
        &tz.zone_ref(),
        true,
    )?))
}
pub fn extract_timestamptz(field: &str, t: TimestampTz, tz: &TimeZone) -> Result<Decimal> {
    extract_timestamptz_opt(field, t, tz)?.ok_or_else(null_result)
}
pub fn extract_interval_opt(field: &str, iv: Interval) -> Result<Option<Decimal>> {
    Ok(to_decimal(interval_part(field, iv, true)?))
}
pub fn extract_interval(field: &str, iv: Interval) -> Result<Decimal> {
    extract_interval_opt(field, iv)?.ok_or_else(null_result)
}

pub fn date_part_timestamp_opt(field: &str, ts: Timestamp) -> Result<Option<f64>> {
    Ok(to_float(timestamp_part(field, ts, false)?))
}
pub fn date_part_timestamp(field: &str, ts: Timestamp) -> Result<f64> {
    date_part_timestamp_opt(field, ts)?.ok_or_else(null_result)
}
pub fn date_part_timestamptz_opt(
    field: &str,
    t: TimestampTz,
    tz: &TimeZone,
) -> Result<Option<f64>> {
    Ok(to_float(timestamptz_part(field, t, &tz.zone_ref(), false)?))
}
pub fn date_part_timestamptz(field: &str, t: TimestampTz, tz: &TimeZone) -> Result<f64> {
    date_part_timestamptz_opt(field, t, tz)?.ok_or_else(null_result)
}
/// date_part(text, date): PostgreSQL evaluates it as
/// date_part(text, date::timestamp).
pub fn date_part_date_opt(field: &str, d: Date) -> Result<Option<f64>> {
    let ts = crate::arith::date_to_timestamp(d)?;
    date_part_timestamp_opt(field, ts)
}
pub fn date_part_date(field: &str, d: Date) -> Result<f64> {
    date_part_date_opt(field, d)?.ok_or_else(null_result)
}
pub fn date_part_time_opt(field: &str, t: Time) -> Result<Option<f64>> {
    Ok(to_float(time_part(field, t, false)?))
}
pub fn date_part_time(field: &str, t: Time) -> Result<f64> {
    date_part_time_opt(field, t)?.ok_or_else(null_result)
}
pub fn date_part_interval_opt(field: &str, iv: Interval) -> Result<Option<f64>> {
    Ok(to_float(interval_part(field, iv, false)?))
}
pub fn date_part_interval(field: &str, iv: Interval) -> Result<f64> {
    date_part_interval_opt(field, iv)?.ok_or_else(null_result)
}

// --- date_trunc ---------------------------------------------------------------

/// Truncate broken-down fields; returns whether the zone offset must be
/// recomputed (units >= day).
fn trunc_tm(val: i32, tm: &mut Tm, fsec: &mut i32, lowunits: &str, tyname: &str) -> Result<bool> {
    let mut redotz = false;
    match val {
        DTK_WEEK => {
            let woy = date2isoweek(tm.year, tm.mon, tm.mday);
            if woy >= 52 && tm.mon == 1 {
                tm.year -= 1;
            }
            if woy <= 1 && tm.mon == MONTHS_PER_YEAR {
                tm.year += 1;
            }
            let (y, m, d) = isoweek2date(woy, tm.year);
            tm.year = y;
            tm.mon = m;
            tm.mday = d;
            tm.hour = 0;
            tm.min = 0;
            tm.sec = 0;
            *fsec = 0;
            redotz = true;
        }
        DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER | DTK_MONTH
        | DTK_DAY | DTK_HOUR | DTK_MINUTE | DTK_SECOND => {
            if val == DTK_MILLENNIUM {
                if tm.year > 0 {
                    tm.year = ((tm.year + 999) / 1000) * 1000 - 999;
                } else {
                    tm.year = -((999 - (tm.year - 1)) / 1000) * 1000 + 1;
                }
            }
            if val == DTK_MILLENNIUM || val == DTK_CENTURY {
                if tm.year > 0 {
                    tm.year = ((tm.year + 99) / 100) * 100 - 99;
                } else {
                    tm.year = -((99 - (tm.year - 1)) / 100) * 100 + 1;
                }
            }
            if val == DTK_DECADE {
                if tm.year > 0 {
                    tm.year = (tm.year / 10) * 10;
                } else {
                    tm.year = -((8 - (tm.year - 1)) / 10) * 10;
                }
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR) {
                tm.mon = 1;
            }
            if matches!(
                val,
                DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER
            ) {
                tm.mon = (3 * ((tm.mon - 1) / 3)) + 1;
            }
            if matches!(
                val,
                DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER | DTK_MONTH
            ) {
                tm.mday = 1;
            }
            if matches!(
                val,
                DTK_MILLENNIUM
                    | DTK_CENTURY
                    | DTK_DECADE
                    | DTK_YEAR
                    | DTK_QUARTER
                    | DTK_MONTH
                    | DTK_DAY
            ) {
                tm.hour = 0;
                redotz = true;
            }
            if val != DTK_SECOND && val != DTK_MINUTE {
                tm.min = 0;
            }
            if val != DTK_SECOND {
                tm.sec = 0;
            }
            *fsec = 0;
        }
        DTK_MILLISEC => *fsec = (*fsec / 1000) * 1000,
        DTK_MICROSEC => {}
        _ => return Err(not_supported(lowunits, tyname)),
    }
    Ok(redotz)
}

/// timestamp_trunc()
pub fn date_trunc_timestamp(field: &str, ts: Timestamp) -> Result<Timestamp> {
    if timestamp_not_finite(ts) {
        return Ok(ts);
    }
    let lowunits = tz::downcase_truncate(field);
    let (ty, val) = decode_units(&lowunits);
    if ty != UNITS {
        return Err(not_recognized(&lowunits, T_TIMESTAMP));
    }
    let (mut tm, mut fsec) = timestamp2tm(ts)?;
    trunc_tm(val, &mut tm, &mut fsec, &lowunits, T_TIMESTAMP)?;
    tm2timestamp(&tm, fsec, None)
}

/// timestamptz_trunc() / timestamptz_trunc_zone()
pub fn date_trunc_timestamptz(field: &str, t: TimestampTz, tz: &TimeZone) -> Result<TimestampTz> {
    date_trunc_timestamptz_zone(field, t, &tz.zone_ref())
}

pub(crate) fn date_trunc_timestamptz_zone(
    field: &str,
    t: TimestampTz,
    zone: &ZoneRef,
) -> Result<TimestampTz> {
    if timestamp_not_finite(t) {
        return Ok(t);
    }
    let lowunits = tz::downcase_truncate(field);
    let (ty, val) = decode_units(&lowunits);
    if ty != UNITS {
        return Err(not_recognized(&lowunits, T_TIMESTAMPTZ));
    }
    let tt = timestamp2tm_tz(t, zone)?;
    let mut tm = tt.tm;
    let mut fsec = tt.fsec;
    let mut tzo = tt.tz;
    let redotz = trunc_tm(val, &mut tm, &mut fsec, &lowunits, T_TIMESTAMPTZ)?;
    if redotz {
        tzo = tz::determine_offset(&mut tm, zone);
    }
    tm2timestamp(&tm, fsec, Some(tzo))
}

/// date_trunc(text, timestamptz, zone text): truncation in the named zone
/// (the zone is resolved like DecodeTimezoneNameToTz()).
pub fn date_trunc_timestamptz_at_zone(
    field: &str,
    t: TimestampTz,
    zone: &str,
) -> Result<TimestampTz> {
    if timestamp_not_finite(t) {
        return Ok(t);
    }
    let z = match tz::decode_timezone_name(zone)? {
        tz::DecodedTzName::Fixed(off) => ZoneRef::Fixed(off),
        tz::DecodedTzName::Dyn(z) | tz::DecodedTzName::Zone(z) => z,
    };
    date_trunc_timestamptz_zone(field, t, &z)
}

/// interval_trunc()
pub fn date_trunc_interval(field: &str, iv: Interval) -> Result<Interval> {
    if interval_not_finite(&iv) {
        return Ok(iv);
    }
    let lowunits = tz::downcase_truncate(field);
    let (ty, val) = decode_units(&lowunits);
    if ty != UNITS {
        return Err(not_recognized(&lowunits, T_INTERVAL));
    }
    let mut tm = interval2itm(iv);
    match val {
        DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER | DTK_MONTH
        | DTK_DAY | DTK_HOUR | DTK_MINUTE | DTK_SECOND => {
            if val == DTK_MILLENNIUM {
                tm.year = (tm.year / 1000) * 1000;
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY) {
                tm.year = (tm.year / 100) * 100;
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE) {
                tm.year = (tm.year / 10) * 10;
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR) {
                tm.mon = 0;
            }
            if matches!(
                val,
                DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER
            ) {
                tm.mon = 3 * (tm.mon / 3);
            }
            if matches!(
                val,
                DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER | DTK_MONTH
            ) {
                tm.mday = 0;
            }
            if !matches!(val, DTK_HOUR | DTK_MINUTE | DTK_SECOND) {
                tm.hour = 0;
            }
            if !matches!(val, DTK_MINUTE | DTK_SECOND) {
                tm.min = 0;
            }
            if val != DTK_SECOND {
                tm.sec = 0;
            }
            tm.usec = 0;
        }
        DTK_MILLISEC => tm.usec = (tm.usec / 1000) * 1000,
        DTK_MICROSEC => {}
        _ => {
            let e = not_supported(&lowunits, T_INTERVAL);
            return Err(if val == DTK_WEEK {
                e.with_detail("Months usually have fractional weeks.")
            } else {
                e
            });
        }
    }
    itm2interval(&tm).ok_or_else(interval_out_of_range)
}
