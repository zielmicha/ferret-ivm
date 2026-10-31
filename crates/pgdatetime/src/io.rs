//! Text input/output for date, time, timestamp, timestamptz and interval
//! (date_in/date_out, time_in/time_out, timestamp_in/out, ... in PostgreSQL).

use crate::core::*;
use crate::datetime::*;
use crate::encode::{self, IntervalStyle};
use crate::error::*;
use crate::{Date, Error, Interval, Result, Time, TimeZone, Timestamp, TimestampTz};

/// SetEpochTimestamp(): 1970-01-01 00:00:00
pub(crate) const EPOCH_TIMESTAMP: i64 = -946_684_800_000_000;

/// The canonical ISO forms PostgreSQL prints with DateStyle ISO, parsed
/// without the general machinery: "YYYY-MM-DD", optionally followed by
/// " HH:MM:SS" and a fraction of 1 to 6 digits, with every field in its
/// ordinary range (year 1-9999, hour 0-23, second 0-59). Returns the fields
/// (time fields zero for a date) and the fraction in microseconds, or None
/// for anything else, which the general parser then handles.
fn iso_fields(s: &[u8], with_time: bool) -> Option<(Tm, i32)> {
    fn num(b: &[u8]) -> Option<i32> {
        let mut v = 0i32;
        for &c in b {
            if !c.is_ascii_digit() {
                return None;
            }
            v = v * 10 + (c - b'0') as i32;
        }
        Some(v)
    }
    if s.len() < 10 || s[4] != b'-' || s[7] != b'-' {
        return None;
    }
    let mut tm = Tm { year: num(&s[0..4])?, mon: num(&s[5..7])?, mday: num(&s[8..10])?, ..Default::default() };
    if tm.year < 1 || !(1..=12).contains(&tm.mon) || tm.mday < 1 || tm.mday > days_in_month(tm.year, tm.mon) {
        return None;
    }
    let mut fsec = 0;
    if !with_time || s.len() == 10 {
        return (s.len() == 10).then_some((tm, 0));
    }
    if s.len() < 19 || s[10] != b' ' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    tm.hour = num(&s[11..13])?;
    tm.min = num(&s[14..16])?;
    tm.sec = num(&s[17..19])?;
    if tm.hour > 23 || tm.min > 59 || tm.sec > 59 {
        return None;
    }
    if s.len() > 19 {
        let frac = &s[20..];
        if s[19] != b'.' || frac.is_empty() || frac.len() > 6 {
            return None;
        }
        fsec = num(frac)? * 10i32.pow(6 - frac.len() as u32);
    }
    Some((tm, fsec))
}

/// `iso_fields` followed by a UTC offset "+HH" or "+HH:MM" (or "-"), as
/// PostgreSQL prints timestamptz values: the fields, the fraction and the
/// zone in PostgreSQL's convention (seconds west of UTC).
fn iso_fields_tz(s: &[u8]) -> Option<(Tm, i32, i32)> {
    let sign = s.iter().rposition(|c| *c == b'+' || *c == b'-')?;
    if sign < 19 {
        return None;
    }
    let (ts, zone) = s.split_at(sign);
    let (hh, mm) = match zone.len() {
        3 => (&zone[1..3], &b"00"[..]),
        6 if zone[3] == b':' => (&zone[1..3], &zone[4..6]),
        _ => return None,
    };
    if !hh.iter().chain(mm).all(u8::is_ascii_digit) {
        return None;
    }
    let (h, m) = ((hh[0] - b'0') as i32 * 10 + (hh[1] - b'0') as i32, (mm[0] - b'0') as i32 * 10 + (mm[1] - b'0') as i32);
    if h > 15 || m > 59 {
        return None;
    }
    let east = h * 3600 + m * 60;
    let tz = if zone[0] == b'+' { -east } else { east };
    let (tm, fsec) = iso_fields(ts, true)?;
    Some((tm, fsec, tz))
}

pub fn parse_date(s: &str) -> Result<Date> {
    if let Some((tm, _)) = iso_fields(s.as_bytes(), false) {
        return Ok(date2j(tm.year, tm.mon, tm.mday) - POSTGRES_EPOCH_JDATE);
    }
    parse_date_general(s)
}

fn parse_date_general(s: &str) -> Result<Date> {
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
    if let Some((tm, fsec)) = iso_fields(s.as_bytes(), true)
        && let Ok(ts) = tm2timestamp(&tm, fsec, None)
    {
        return Ok(ts);
    }
    parse_timestamp_general(s, None)
}

fn parse_timestamp_general(s: &str, tz: Option<&TimeZone>) -> Result<Timestamp> {
    let dt = if tz.is_some() { "timestamp with time zone" } else { "timestamp" };
    let zone = tz.map(TimeZone::zone_ref);
    let (mut f, mut t) =
        parse_date_time(s, MAXDATELEN + MAXDATEFIELDS).map_err(|e| e.to_error(s, dt))?;
    let d = decode_date_time(&mut f, &mut t, zone.as_ref()).map_err(|e| e.to_error(s, dt))?;
    match d.dtype {
        DTK_DATE => tm2timestamp(&d.tm, d.fsec, tz.map(|_| d.tz)).map_err(|_| {
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
    if let Some((tm, fsec, off)) = iso_fields_tz(s.as_bytes())
        && let Ok(ts) = tm2timestamp(&tm, fsec, Some(off))
    {
        return Ok(ts);
    }
    parse_timestamp_general(s, Some(tz))
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The ISO fast paths agree with the general parser on what they accept
    /// and leave everything else to it.
    #[test]
    fn iso_fast_paths_match_general_parser() {
        let mut x = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move |n: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % n
        };
        for _ in 0..20000 {
            let (y, m, d) = (next(10000), 1 + next(12), 1 + next(31));
            let (h, mi, sec) = (next(25), next(61), next(61));
            let date = format!("{y:04}-{m:02}-{d:02}");
            let frac = match next(4) {
                0 => String::new(),
                1 => format!(".{}", next(10)),
                2 => format!(".{:06}", next(1_000_000)),
                _ => format!(".{:03}", next(1000)),
            };
            let ts = format!("{date} {h:02}:{mi:02}:{sec:02}{frac}");
            let a = parse_date(&date).map_err(|e| e.message);
            let b = parse_date_general(&date).map_err(|e| e.message);
            assert_eq!(a, b, "{date}");
            let a = parse_timestamp(&ts).map_err(|e| e.message);
            let b = parse_timestamp_general(&ts, None).map_err(|e| e.message);
            assert_eq!(a, b, "{ts}");
        }
        for s in ["2024-01-01 24:00:00", "2024-02-29 23:59:60", " 2024-01-01", "2024-1-01", "2024-01-01T00:00:00", "2024-01-01 00:00:00.1234567"] {
            assert_eq!(parse_timestamp(s).map_err(|e| e.message), parse_timestamp_general(s, None).map_err(|e| e.message), "{s}");
        }
        let zone = TimeZone::Utc;
        for _ in 0..20000 {
            let (y, m, d) = (1 + next(9999), 1 + next(12), 1 + next(28));
            let (h, mi, sec) = (next(24), next(60), next(60));
            let zone_text = match next(4) {
                0 => format!("+{:02}", next(17)),
                1 => format!("-{:02}", next(17)),
                2 => format!("+{:02}:{:02}", next(16), next(60)),
                _ => format!("-{:02}:{:02}", next(16), [0, 15, 30, 45][next(4) as usize]),
            };
            let frac = if next(2) == 0 { String::new() } else { format!(".{}", next(1000)) };
            let s = format!("{y:04}-{m:02}-{d:02} {h:02}:{mi:02}:{sec:02}{frac}{zone_text}");
            assert_eq!(
                parse_timestamptz(&s, &zone).map_err(|e| e.message),
                parse_timestamp_general(&s, Some(&zone)).map_err(|e| e.message),
                "{s}"
            );
        }
    }
}
