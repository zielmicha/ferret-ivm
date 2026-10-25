//! Text input/output for date, time, timestamp, timestamptz and interval
//! (date_in/date_out, time_in/time_out, timestamp_in/out, ... in PostgreSQL).

use crate::core::*;
use crate::datetime::*;
use crate::encode::{self, IntervalStyle};
use crate::error::*;
use crate::{Date, Error, Interval, Result, Time, TimeZone, Timestamp, TimestampTz};

/// SetEpochTimestamp(): 1970-01-01 00:00:00
pub(crate) const EPOCH_TIMESTAMP: i64 = -946_684_800_000_000;

pub fn parse_date(s: &str) -> Result<Date> {
    let (mut f, mut t) = parse_date_time(s, MAXDATELEN + 1).map_err(|e| e.to_error(s, "date"))?;
    let d = decode_date_time(&mut f, &mut t, None).map_err(|e| e.to_error(s, "date"))?;
    let tm = match d.dtype {
        DTK_DATE => d.tm,
        DTK_EPOCH => Tm {
            year: 1970,
            mon: 1,
            mday: 1,
            ..Default::default()
        },
        DTK_LATE => return Ok(DATEVAL_NOEND),
        DTK_EARLY => return Ok(DATEVAL_NOBEGIN),
        _ => return Err(DtErr::Code(DTERR_BAD_FORMAT).to_error(s, "date")),
    };
    let oor = || {
        Error::new(
            DATETIME_VALUE_OUT_OF_RANGE,
            format!("date out of range: \"{}\"", s),
        )
    };
    if !is_valid_julian(tm.year, tm.mon, tm.mday) {
        return Err(oor());
    }
    let date = (date2j(tm.year, tm.mon, tm.mday) - POSTGRES_EPOCH_JDATE) as i64;
    if !is_valid_date(date) {
        return Err(oor());
    }
    Ok(date as i32)
}

pub fn format_date(d: Date) -> String {
    if d == DATEVAL_NOBEGIN {
        return "-infinity".to_string();
    }
    if d == DATEVAL_NOEND {
        return "infinity".to_string();
    }
    let (year, mon, mday) = j2date(d.wrapping_add(POSTGRES_EPOCH_JDATE));
    encode::encode_date_only(&Tm {
        year,
        mon,
        mday,
        ..Default::default()
    })
}

/// tm2time()
fn tm2time(tm: &Tm, fsec: i32) -> Time {
    (((tm.hour as i64 * MINS_PER_HOUR + tm.min as i64) * SECS_PER_MINUTE) + tm.sec as i64)
        * USECS_PER_SEC
        + fsec as i64
}

/// time2tm()
pub(crate) fn time2tm(time: Time) -> (Tm, i32) {
    let mut t = time;
    let hour = t / USECS_PER_HOUR;
    t -= hour * USECS_PER_HOUR;
    let min = t / USECS_PER_MINUTE;
    t -= min * USECS_PER_MINUTE;
    let sec = t / USECS_PER_SEC;
    t -= sec * USECS_PER_SEC;
    (
        Tm {
            hour: hour as i32,
            min: min as i32,
            sec: sec as i32,
            ..Default::default()
        },
        t as i32,
    )
}

pub fn parse_time(s: &str) -> Result<Time> {
    let (mut f, mut t) = parse_date_time(s, MAXDATELEN + 1).map_err(|e| e.to_error(s, "time"))?;
    let d = decode_time_only(&mut f, &mut t).map_err(|e| e.to_error(s, "time"))?;
    Ok(tm2time(&d.tm, d.fsec))
}

pub fn format_time(t: Time) -> String {
    let (tm, fsec) = time2tm(t);
    encode::encode_time_only(&tm, fsec)
}

pub fn parse_timestamp(s: &str) -> Result<Timestamp> {
    let (mut f, mut t) =
        parse_date_time(s, MAXDATELEN + MAXDATEFIELDS).map_err(|e| e.to_error(s, "timestamp"))?;
    let d = decode_date_time(&mut f, &mut t, None).map_err(|e| e.to_error(s, "timestamp"))?;
    match d.dtype {
        DTK_DATE => tm2timestamp(&d.tm, d.fsec, None).map_err(|_| {
            Error::new(
                DATETIME_VALUE_OUT_OF_RANGE,
                format!("timestamp out of range: \"{}\"", s),
            )
        }),
        DTK_EPOCH => Ok(EPOCH_TIMESTAMP),
        DTK_LATE => Ok(DT_NOEND),
        DTK_EARLY => Ok(DT_NOBEGIN),
        _ => Err(DtErr::Code(DTERR_BAD_FORMAT).to_error(s, "timestamp")),
    }
}

pub fn format_timestamp(ts: Timestamp) -> String {
    if ts == DT_NOBEGIN {
        return "-infinity".to_string();
    }
    if ts == DT_NOEND {
        return "infinity".to_string();
    }
    match timestamp2tm(ts) {
        Ok((tm, fsec)) => encode::encode_date_time(&tm, fsec, false, 0),
        Err(_) => "timestamp out of range".to_string(),
    }
}

pub fn parse_timestamptz(s: &str, tz: &TimeZone) -> Result<TimestampTz> {
    let dt = "timestamp with time zone";
    let zone = tz.zone_ref();
    let (mut f, mut t) =
        parse_date_time(s, MAXDATELEN + MAXDATEFIELDS).map_err(|e| e.to_error(s, dt))?;
    let d = decode_date_time(&mut f, &mut t, Some(&zone)).map_err(|e| e.to_error(s, dt))?;
    match d.dtype {
        DTK_DATE => tm2timestamp(&d.tm, d.fsec, Some(d.tz)).map_err(|_| {
            Error::new(
                DATETIME_VALUE_OUT_OF_RANGE,
                format!("timestamp out of range: \"{}\"", s),
            )
        }),
        DTK_EPOCH => Ok(EPOCH_TIMESTAMP),
        DTK_LATE => Ok(DT_NOEND),
        DTK_EARLY => Ok(DT_NOBEGIN),
        _ => Err(DtErr::Code(DTERR_BAD_FORMAT).to_error(s, dt)),
    }
}

pub fn format_timestamptz(t: TimestampTz, tz: &TimeZone) -> String {
    if t == DT_NOBEGIN {
        return "-infinity".to_string();
    }
    if t == DT_NOEND {
        return "infinity".to_string();
    }
    match timestamp2tm_tz(t, &tz.zone_ref()) {
        Ok(tt) => encode::encode_date_time(&tt.tm, tt.fsec, true, tt.tz),
        Err(_) => "timestamp out of range".to_string(),
    }
}

pub fn parse_interval(s: &str) -> Result<Interval> {
    parse_interval_range(s, INTERVAL_FULL_RANGE)
}

/// interval_in() with a typmod: `INTERVAL fields (precision)`.  The field
/// restriction also changes how unit-less input is read (e.g. '5' with
/// `HOUR` means 5 hours).  `precision` < 0 means unspecified.
pub fn parse_interval_typmod(
    s: &str,
    fields: Option<IntervalFields>,
    precision: i32,
) -> Result<Interval> {
    let iv = parse_interval_range(s, interval_range_mask(fields))?;
    adjust_interval_typmod(iv, fields, precision)
}

/// Decode a raw PostgreSQL interval typmod (`range << 16 | precision`, -1 =
/// none) into fields and precision (-1 = unspecified).
pub fn interval_typmod_decode(typmod: i32) -> Result<(Option<IntervalFields>, i32)> {
    if typmod < 0 {
        return Ok((None, -1));
    }
    let range = (typmod >> 16) & 0x7FFF;
    let precision = typmod & 0xFFFF;
    let fields = if range == INTERVAL_FULL_RANGE {
        None
    } else {
        use IntervalFields::*;
        let all = [
            Year,
            Month,
            Day,
            Hour,
            Minute,
            Second,
            YearToMonth,
            DayToHour,
            DayToMinute,
            DayToSecond,
            HourToMinute,
            HourToSecond,
            MinuteToSecond,
        ];
        match all
            .into_iter()
            .find(|f| interval_range_mask(Some(*f)) == range)
        {
            Some(f) => Some(f),
            None => {
                return Err(Error::new(
                    "XX000",
                    format!("unrecognized interval typmod: {}", typmod),
                ));
            }
        }
    };
    let precision = if precision == 0xFFFF {
        -1
    } else if precision > MAX_INTERVAL_PRECISION {
        return Err(Error::new(
            INVALID_PARAMETER_VALUE,
            format!(
                "interval({}) precision must be between {} and {}",
                precision, 0, MAX_INTERVAL_PRECISION
            ),
        ));
    } else {
        precision
    };
    Ok((fields, precision))
}

/// AdjustIntervalForTypmod() with a raw PostgreSQL interval typmod.
pub fn adjust_interval_raw_typmod(iv: Interval, typmod: i32) -> Result<Interval> {
    if interval_not_finite(&iv) {
        return Ok(iv);
    }
    let (fields, precision) = interval_typmod_decode(typmod)?;
    adjust_interval_typmod(iv, fields, precision)
}

/// interval_in() with a raw PostgreSQL interval typmod.
pub fn parse_interval_raw_typmod(s: &str, typmod: i32) -> Result<Interval> {
    let (fields, precision) = interval_typmod_decode(typmod)?;
    parse_interval_typmod(s, fields, precision)
}

fn interval_range_mask(fields: Option<IntervalFields>) -> i32 {
    use IntervalFields::*;
    let (y, mo, d, h, mi, s) = (
        interval_mask(YEAR),
        interval_mask(MONTH),
        interval_mask(DAY),
        interval_mask(HOUR),
        interval_mask(MINUTE),
        interval_mask(SECOND),
    );
    match fields {
        None => INTERVAL_FULL_RANGE,
        Some(Year) => y,
        Some(Month) => mo,
        Some(Day) => d,
        Some(Hour) => h,
        Some(Minute) => mi,
        Some(Second) => s,
        Some(YearToMonth) => y | mo,
        Some(DayToHour) => d | h,
        Some(DayToMinute) => d | h | mi,
        Some(DayToSecond) => d | h | mi | s,
        Some(HourToMinute) => h | mi,
        Some(HourToSecond) => h | mi | s,
        Some(MinuteToSecond) => mi | s,
    }
}

fn parse_interval_range(s: &str, range: i32) -> Result<Interval> {
    let r = parse_date_time(s, 256).and_then(|(f, t)| decode_interval(&f, &t, range));
    let r = match r {
        Err(DtErr::Code(DTERR_BAD_FORMAT)) => decode_iso8601_interval(s),
        other => other,
    };
    let (dtype, itm) = match r {
        Ok(v) => v,
        Err(DtErr::Code(DTERR_FIELD_OVERFLOW)) => {
            return Err(DtErr::Code(DTERR_INTERVAL_OVERFLOW).to_error(s, "interval"));
        }
        Err(e) => return Err(e.to_error(s, "interval")),
    };
    match dtype {
        DTK_DELTA => itmin2interval(&itm).ok_or_else(interval_out_of_range),
        DTK_LATE => Ok(INTERVAL_NOEND),
        DTK_EARLY => Ok(INTERVAL_NOBEGIN),
        _ => Err(DtErr::Code(DTERR_BAD_FORMAT).to_error(s, "interval")),
    }
}

pub fn format_interval(iv: Interval) -> String {
    format_interval_style(iv, IntervalStyle::Postgres)
}

/// interval_out() with an explicit IntervalStyle.
pub fn format_interval_style(iv: Interval, style: IntervalStyle) -> String {
    if interval_is_nobegin(&iv) {
        return "-infinity".to_string();
    }
    if interval_is_noend(&iv) {
        return "infinity".to_string();
    }
    encode::encode_interval(&interval2itm(iv), style)
}

const SCALES: [i64; 7] = [1_000_000, 100_000, 10_000, 1_000, 100, 10, 1];
const OFFSETS: [i64; 7] = [500_000, 50_000, 5_000, 500, 50, 5, 0];

/// Clamp a declared precision like the typmodin functions do: values above
/// 6 are reduced to 6 (PostgreSQL warns), negative means "unspecified".
fn clamp_precision(p: i32) -> Option<usize> {
    if p < 0 { None } else { Some(p.min(6) as usize) }
}

/// AdjustTimeForTypmod()
pub fn adjust_time(t: Time, precision: i32) -> Time {
    let Some(p) = clamp_precision(precision) else {
        return t;
    };
    if t >= 0 {
        (t.wrapping_add(OFFSETS[p]) / SCALES[p]).wrapping_mul(SCALES[p])
    } else {
        ((t.wrapping_neg().wrapping_add(OFFSETS[p]) / SCALES[p]).wrapping_mul(SCALES[p]))
            .wrapping_neg()
    }
}

/// AdjustTimestampForTypmod() (timestamp and timestamptz)
pub fn adjust_timestamp(ts: Timestamp, precision: i32) -> Result<Timestamp> {
    let Some(p) = clamp_precision(precision) else {
        return Ok(ts);
    };
    if timestamp_not_finite(ts) || p == MAX_TIMESTAMP_PRECISION as usize {
        return Ok(ts);
    }
    Ok(if ts >= 0 {
        (ts.wrapping_add(OFFSETS[p]) / SCALES[p]).wrapping_mul(SCALES[p])
    } else {
        ((ts.wrapping_neg().wrapping_add(OFFSETS[p]) / SCALES[p]).wrapping_mul(SCALES[p]))
            .wrapping_neg()
    })
}

/// AdjustIntervalForTypmod() for INTERVAL(p) (full field range).
pub fn adjust_interval(iv: Interval, precision: i32) -> Result<Interval> {
    adjust_interval_typmod(iv, None, precision)
}

/// Interval field restrictions (`INTERVAL YEAR TO MONTH` etc.)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntervalFields {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    YearToMonth,
    DayToHour,
    DayToMinute,
    DayToSecond,
    HourToMinute,
    HourToSecond,
    MinuteToSecond,
}

/// AdjustIntervalForTypmod() with an optional field restriction and
/// precision (negative = unspecified).
pub fn adjust_interval_typmod(
    iv: Interval,
    fields: Option<IntervalFields>,
    precision: i32,
) -> Result<Interval> {
    let mut iv = iv;
    if interval_not_finite(&iv) {
        return Ok(iv);
    }
    use IntervalFields::*;
    match fields {
        None | Some(Second) | Some(DayToSecond) | Some(HourToSecond) | Some(MinuteToSecond) => {}
        Some(Year) => {
            iv.months = (iv.months / MONTHS_PER_YEAR) * MONTHS_PER_YEAR;
            iv.days = 0;
            iv.micros = 0;
        }
        Some(Month) | Some(YearToMonth) => {
            iv.days = 0;
            iv.micros = 0;
        }
        Some(Day) => iv.micros = 0,
        Some(Hour) | Some(DayToHour) => iv.micros = (iv.micros / USECS_PER_HOUR) * USECS_PER_HOUR,
        Some(Minute) | Some(DayToMinute) | Some(HourToMinute) => {
            iv.micros = (iv.micros / USECS_PER_MINUTE) * USECS_PER_MINUTE
        }
    }
    if let Some(p) = clamp_precision(precision) {
        if iv.micros >= 0 {
            iv.micros = iv
                .micros
                .checked_add(OFFSETS[p])
                .ok_or_else(interval_out_of_range)?;
        } else {
            iv.micros = iv
                .micros
                .checked_sub(OFFSETS[p])
                .ok_or_else(interval_out_of_range)?;
        }
        iv.micros -= iv.micros % SCALES[p];
    }
    Ok(iv)
}
