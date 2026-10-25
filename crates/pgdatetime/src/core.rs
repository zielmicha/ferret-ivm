//! Calendar arithmetic and broken-down time conversions.
//! Port of parts of PostgreSQL's datetime.c / timestamp.c / date.c
//! (PostgreSQL License).

use crate::error::*;
use crate::tz::{TzName, ZoneRef};
use crate::{Error, Interval, Result, Timestamp};

pub(crate) const MONTHS_PER_YEAR: i32 = 12;
pub(crate) const DAYS_PER_MONTH: i32 = 30;
pub(crate) const DAYS_PER_WEEK: i32 = 7;
pub(crate) const HOURS_PER_DAY: i32 = 24;
pub(crate) const SECS_PER_DAY: i64 = 86400;
pub(crate) const SECS_PER_HOUR: i64 = 3600;
pub(crate) const SECS_PER_MINUTE: i64 = 60;
pub(crate) const MINS_PER_HOUR: i64 = 60;
pub(crate) const USECS_PER_DAY: i64 = 86_400_000_000;
pub(crate) const USECS_PER_HOUR: i64 = 3_600_000_000;
pub(crate) const USECS_PER_MINUTE: i64 = 60_000_000;
pub(crate) const USECS_PER_SEC: i64 = 1_000_000;
pub(crate) const DAYS_PER_YEAR: f64 = 365.25;

pub(crate) const MAX_TZDISP_HOUR: i32 = 15;

pub(crate) const UNIX_EPOCH_JDATE: i32 = 2440588;
pub(crate) const POSTGRES_EPOCH_JDATE: i32 = 2451545;

pub(crate) const JULIAN_MINYEAR: i32 = -4713;
pub(crate) const JULIAN_MINMONTH: i32 = 11;
pub(crate) const JULIAN_MAXYEAR: i32 = 5874898;
pub(crate) const JULIAN_MAXMONTH: i32 = 6;

pub(crate) const DATETIME_MIN_JULIAN: i32 = 0;
pub(crate) const DATE_END_JULIAN: i32 = 2147483494;
pub(crate) const TIMESTAMP_END_JULIAN: i32 = 109203528;

pub(crate) const MIN_TIMESTAMP: i64 = -211813488000000000;
pub(crate) const END_TIMESTAMP: i64 = 9223371331200000000;

pub(crate) const DT_NOBEGIN: i64 = i64::MIN;
pub(crate) const DT_NOEND: i64 = i64::MAX;
pub(crate) const DATEVAL_NOBEGIN: i32 = i32::MIN;
pub(crate) const DATEVAL_NOEND: i32 = i32::MAX;

pub(crate) const MAX_TIMESTAMP_PRECISION: i32 = 6;
pub(crate) const MAX_INTERVAL_PRECISION: i32 = 6;
pub(crate) const MAX_TIME_PRECISION: i32 = 6;

pub(crate) const DAY_TAB: [[i32; 13]; 2] = [
    [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0],
    [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0],
];

pub(crate) const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];
pub(crate) const DAYS: [&str; 7] = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
];

#[inline]
pub(crate) fn isleap(y: i32) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

#[inline]
pub(crate) fn days_in_month(y: i32, m: i32) -> i32 {
    DAY_TAB[isleap(y) as usize][(m - 1) as usize]
}

#[inline]
pub(crate) fn is_valid_julian(y: i32, m: i32, _d: i32) -> bool {
    (y > JULIAN_MINYEAR || (y == JULIAN_MINYEAR && m >= JULIAN_MINMONTH))
        && (y < JULIAN_MAXYEAR || (y == JULIAN_MAXYEAR && m < JULIAN_MAXMONTH))
}

#[inline]
pub(crate) fn is_valid_date(d: i64) -> bool {
    ((DATETIME_MIN_JULIAN - POSTGRES_EPOCH_JDATE) as i64) <= d
        && d < (DATE_END_JULIAN - POSTGRES_EPOCH_JDATE) as i64
}

#[inline]
pub(crate) fn is_valid_timestamp(t: i64) -> bool {
    MIN_TIMESTAMP <= t && t < END_TIMESTAMP
}

#[inline]
pub(crate) fn timestamp_not_finite(t: i64) -> bool {
    t == DT_NOBEGIN || t == DT_NOEND
}

#[inline]
pub(crate) fn date_not_finite(d: i32) -> bool {
    d == DATEVAL_NOBEGIN || d == DATEVAL_NOEND
}

#[inline]
pub(crate) fn interval_is_nobegin(i: &Interval) -> bool {
    i.months == i32::MIN && i.days == i32::MIN && i.micros == i64::MIN
}

#[inline]
pub(crate) fn interval_is_noend(i: &Interval) -> bool {
    i.months == i32::MAX && i.days == i32::MAX && i.micros == i64::MAX
}

#[inline]
pub(crate) fn interval_not_finite(i: &Interval) -> bool {
    interval_is_nobegin(i) || interval_is_noend(i)
}

pub(crate) const INTERVAL_NOBEGIN: Interval = Interval {
    months: i32::MIN,
    days: i32::MIN,
    micros: i64::MIN,
};
pub(crate) const INTERVAL_NOEND: Interval = Interval {
    months: i32::MAX,
    days: i32::MAX,
    micros: i64::MAX,
};

/// date2j(): calendar date to Julian day (int arithmetic, wraps like C).
pub(crate) fn date2j(year: i32, month: i32, day: i32) -> i32 {
    let (mut year, mut month) = (year, month);
    if month > 2 {
        month = month.wrapping_add(1);
        year = year.wrapping_add(4800);
    } else {
        month = month.wrapping_add(13);
        year = year.wrapping_add(4799);
    }
    let century = year / 100;
    let mut julian = year.wrapping_mul(365).wrapping_sub(32167);
    julian = julian.wrapping_add((year / 4).wrapping_sub(century).wrapping_add(century / 4));
    julian = julian.wrapping_add((7834i32.wrapping_mul(month) / 256).wrapping_add(day));
    julian
}

/// j2date(): Julian day to calendar date (unsigned arithmetic like C).
pub(crate) fn j2date(jd: i32) -> (i32, i32, i32) {
    let mut julian: u32 = jd as u32;
    julian = julian.wrapping_add(32044);
    let mut quad: u32 = julian / 146097;
    let extra: u32 = (julian.wrapping_sub(quad.wrapping_mul(146097)))
        .wrapping_mul(4)
        .wrapping_add(3);
    julian = julian.wrapping_add(
        60u32
            .wrapping_add(quad.wrapping_mul(3))
            .wrapping_add(extra / 146097),
    );
    quad = julian / 1461;
    julian = julian.wrapping_sub(quad.wrapping_mul(1461));
    let mut y: i32 = (julian.wrapping_mul(4) / 1461) as i32;
    julian = (if y != 0 {
        (julian + 305) % 365
    } else {
        (julian + 306) % 366
    }) + 123;
    y = y.wrapping_add(quad.wrapping_mul(4) as i32);
    let year = y.wrapping_sub(4800);
    let quad = julian.wrapping_mul(2141) / 65536;
    let day = julian.wrapping_sub(7834u32.wrapping_mul(quad) / 256) as i32;
    let month = ((quad + 10) % (MONTHS_PER_YEAR as u32) + 1) as i32;
    (year, month, day)
}

/// j2day(): day of week, 0..6 == Sun..Sat
#[inline]
pub(crate) fn j2day(date: i32) -> i32 {
    let mut d = date.wrapping_add(1) % 7;
    if d < 0 {
        d += 7;
    }
    d
}

/// Broken-down time (struct pg_tm). Year is the full year (1 BC = 0),
/// month is 1-based.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Tm {
    pub year: i32,
    pub mon: i32,
    pub mday: i32,
    pub hour: i32,
    pub min: i32,
    pub sec: i32,
    pub wday: i32,
    pub isdst: i32,
    pub gmtoff: i64,
}

/// dt2time(): split microseconds-of-day into fields.
#[inline]
pub(crate) fn dt2time(jd: i64) -> (i32, i32, i32, i32) {
    let mut time = jd;
    let hour = time / USECS_PER_HOUR;
    time -= hour * USECS_PER_HOUR;
    let min = time / USECS_PER_MINUTE;
    time -= min * USECS_PER_MINUTE;
    let sec = time / USECS_PER_SEC;
    let fsec = time - sec * USECS_PER_SEC;
    (hour as i32, min as i32, sec as i32, fsec as i32)
}

#[inline]
pub(crate) fn time2t(hour: i32, min: i32, sec: i32, fsec: i32) -> i64 {
    ((((hour as i64 * MINS_PER_HOUR) + min as i64) * SECS_PER_MINUTE + sec as i64) * USECS_PER_SEC)
        + fsec as i64
}

/// Result of timestamp2tm() with time zone rotation.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TmTz {
    pub tm: Tm,
    pub fsec: i32,
    /// PostgreSQL's "tz" value: seconds WEST of UTC (i.e. -gmtoff)
    pub tz: i32,
    pub tzn: Option<TzName>,
}

/// timestamp2tm() without time zone conversion.
pub(crate) fn timestamp2tm(dt: Timestamp) -> Result<(Tm, i32)> {
    let mut time = dt;
    let mut date = time / USECS_PER_DAY;
    if date != 0 {
        time -= date * USECS_PER_DAY;
    }
    if time < 0 {
        time += USECS_PER_DAY;
        date -= 1;
    }
    date += POSTGRES_EPOCH_JDATE as i64;
    if date < 0 || date > i32::MAX as i64 {
        return Err(timestamp_out_of_range());
    }
    let (year, mon, mday) = j2date(date as i32);
    let (hour, min, sec, fsec) = dt2time(time);
    let tm = Tm {
        year,
        mon,
        mday,
        hour,
        min,
        sec,
        wday: 0,
        isdst: -1,
        gmtoff: 0,
    };
    Ok((tm, fsec))
}

/// timestamp2tm() with time zone rotation into `zone`.
pub(crate) fn timestamp2tm_tz(dt: Timestamp, zone: &ZoneRef) -> Result<TmTz> {
    let (tm0, fsec) = timestamp2tm(dt)?;
    let _ = tm0;
    // utime = (dt - fsec) / USECS_PER_SEC + (POSTGRES_EPOCH - UNIX_EPOCH) * SECS_PER_DAY
    let utime = (dt - fsec as i64) / USECS_PER_SEC
        + (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) as i64 * SECS_PER_DAY;
    let lt = zone.localtime(utime);
    // compute local broken-down time of utime + utoff (equivalent to timesub)
    let local = utime + lt.utoff as i64;
    let mut days = local.div_euclid(SECS_PER_DAY);
    let rem = local.rem_euclid(SECS_PER_DAY);
    days += UNIX_EPOCH_JDATE as i64;
    let (year, mon, mday) = j2date(days as i32);
    let hour = (rem / SECS_PER_HOUR) as i32;
    let min = ((rem % SECS_PER_HOUR) / SECS_PER_MINUTE) as i32;
    let sec = (rem % SECS_PER_MINUTE) as i32;
    let tm = Tm {
        year,
        mon,
        mday,
        hour,
        min,
        sec,
        wday: 0,
        isdst: lt.isdst as i32,
        gmtoff: lt.utoff as i64,
    };
    Ok(TmTz {
        tm,
        fsec,
        tz: lt.utoff.wrapping_neg(),
        tzn: Some(lt.abbrev),
    })
}

/// tm2timestamp(): convert broken-down time to timestamp; `tzp` is the
/// PostgreSQL-convention offset (seconds west of UTC) if any.
pub(crate) fn tm2timestamp(tm: &Tm, fsec: i32, tzp: Option<i32>) -> Result<Timestamp> {
    if !is_valid_julian(tm.year, tm.mon, tm.mday) {
        return Err(timestamp_out_of_range());
    }
    let date = (date2j(tm.year, tm.mon, tm.mday) - POSTGRES_EPOCH_JDATE) as i64;
    let time = time2t(tm.hour, tm.min, tm.sec, fsec);
    let result = date.wrapping_mul(USECS_PER_DAY).wrapping_add(time);
    // check for major overflow
    if (result.wrapping_sub(time)) / USECS_PER_DAY != date {
        return Err(timestamp_out_of_range());
    }
    // check for just-barely overflow (okay except time-of-day wraps)
    if (result < 0 && date > 0) || (result > 0 && date < -1) {
        return Err(timestamp_out_of_range());
    }
    let result = match tzp {
        Some(tz) => dt2local(result, tz.wrapping_neg()),
        None => result,
    };
    if !is_valid_timestamp(result) {
        return Err(timestamp_out_of_range());
    }
    Ok(result)
}

#[inline]
pub(crate) fn dt2local(dt: i64, timezone: i32) -> i64 {
    dt.wrapping_sub((timezone as i64).wrapping_mul(USECS_PER_SEC))
}

/// struct pg_itm
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Itm {
    pub usec: i32,
    pub sec: i32,
    pub min: i32,
    pub hour: i64,
    pub mday: i32,
    pub mon: i32,
    pub year: i32,
}

/// struct pg_itm_in
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ItmIn {
    pub usec: i64,
    pub mday: i32,
    pub mon: i32,
    pub year: i32,
}

pub(crate) fn interval2itm(span: Interval) -> Itm {
    let mut itm = Itm {
        year: span.months / MONTHS_PER_YEAR,
        mon: span.months % MONTHS_PER_YEAR,
        mday: span.days,
        ..Default::default()
    };
    let mut time = span.micros;
    let mut tfrac = time / USECS_PER_HOUR;
    time -= tfrac * USECS_PER_HOUR;
    itm.hour = tfrac;
    tfrac = time / USECS_PER_MINUTE;
    time -= tfrac * USECS_PER_MINUTE;
    itm.min = tfrac as i32;
    tfrac = time / USECS_PER_SEC;
    time -= tfrac * USECS_PER_SEC;
    itm.sec = tfrac as i32;
    itm.usec = time as i32;
    itm
}

/// itm2interval(): returns None on overflow (incl. infinite results).
pub(crate) fn itm2interval(itm: &Itm) -> Option<Interval> {
    let total_months = itm.year as i64 * MONTHS_PER_YEAR as i64 + itm.mon as i64;
    if total_months > i32::MAX as i64 || total_months < i32::MIN as i64 {
        return None;
    }
    let mut time = itm.hour.checked_mul(USECS_PER_HOUR)?;
    time = time.checked_add(itm.min as i64 * USECS_PER_MINUTE)?;
    time = time.checked_add(itm.sec as i64 * USECS_PER_SEC)?;
    time = time.checked_add(itm.usec as i64)?;
    let span = Interval {
        months: total_months as i32,
        days: itm.mday,
        micros: time,
    };
    if interval_not_finite(&span) {
        return None;
    }
    Some(span)
}

/// itmin2interval(): infinite results are not treated as overflow.
pub(crate) fn itmin2interval(itm: &ItmIn) -> Option<Interval> {
    let total_months = itm.year as i64 * MONTHS_PER_YEAR as i64 + itm.mon as i64;
    if total_months > i32::MAX as i64 || total_months < i32::MIN as i64 {
        return None;
    }
    Some(Interval {
        months: total_months as i32,
        days: itm.mday,
        micros: itm.usec,
    })
}

/// isoweek2j(): Julian day of Monday of ISO week `week` of ISO year `year`.
pub(crate) fn isoweek2j(year: i32, week: i32) -> i32 {
    let day4 = date2j(year, 1, 4);
    let day0 = j2day(day4.wrapping_sub(1));
    (week.wrapping_sub(1).wrapping_mul(7)).wrapping_add(day4.wrapping_sub(day0))
}

pub(crate) fn isoweek2date(woy: i32, year: i32) -> (i32, i32, i32) {
    j2date(isoweek2j(year, woy))
}

pub(crate) fn isoweekdate2date(isoweek: i32, wday: i32, year: i32) -> (i32, i32, i32) {
    let mut jday = isoweek2j(year, isoweek);
    if wday > 1 {
        jday = jday.wrapping_add(wday.wrapping_sub(2));
    } else {
        jday = jday.wrapping_add(6);
    }
    j2date(jday)
}

pub(crate) fn date2isoweek(year: i32, mon: i32, mday: i32) -> i32 {
    let dayn = date2j(year, mon, mday);
    let mut day4 = date2j(year, 1, 4);
    let mut day0 = j2day(day4.wrapping_sub(1));
    if dayn < day4.wrapping_sub(day0) {
        day4 = date2j(year.wrapping_sub(1), 1, 4);
        day0 = j2day(day4.wrapping_sub(1));
    }
    let mut result = ((dayn.wrapping_sub(day4.wrapping_sub(day0))) / 7 + 1) as f64;
    if result >= 52.0 {
        day4 = date2j(year.wrapping_add(1), 1, 4);
        day0 = j2day(day4.wrapping_sub(1));
        if dayn >= day4.wrapping_sub(day0) {
            result = ((dayn.wrapping_sub(day4.wrapping_sub(day0))) / 7 + 1) as f64;
        }
    }
    result as i32
}

pub(crate) fn date2isoyear(year: i32, mon: i32, mday: i32) -> i32 {
    let mut year = year;
    let dayn = date2j(year, mon, mday);
    let mut day4 = date2j(year, 1, 4);
    let mut day0 = j2day(day4.wrapping_sub(1));
    if dayn < day4.wrapping_sub(day0) {
        day4 = date2j(year.wrapping_sub(1), 1, 4);
        day0 = j2day(day4.wrapping_sub(1));
        year = year.wrapping_sub(1);
    }
    let result = ((dayn.wrapping_sub(day4.wrapping_sub(day0))) / 7 + 1) as f64;
    if result >= 52.0 {
        day4 = date2j(year.wrapping_add(1), 1, 4);
        day0 = j2day(day4.wrapping_sub(1));
        if dayn >= day4.wrapping_sub(day0) {
            year = year.wrapping_add(1);
        }
    }
    year
}

pub(crate) fn date2isoyearday(year: i32, mon: i32, mday: i32) -> i32 {
    date2j(year, mon, mday)
        .wrapping_sub(isoweek2j(date2isoyear(year, mon, mday), 1))
        .wrapping_add(1)
}

/// time_overflows()
pub(crate) fn time_overflows(hour: i32, min: i32, sec: i32, fsec: i32) -> bool {
    if hour < 0
        || hour > HOURS_PER_DAY
        || min < 0
        || min >= MINS_PER_HOUR as i32
        || sec < 0
        || sec > SECS_PER_MINUTE as i32
        || fsec < 0
        || fsec as i64 > USECS_PER_SEC
    {
        return true;
    }
    if (((hour as i64 * MINS_PER_HOUR + min as i64) * SECS_PER_MINUTE + sec as i64) * USECS_PER_SEC)
        + fsec as i64
        > USECS_PER_DAY
    {
        return true;
    }
    false
}

/// float_time_overflows()
pub(crate) fn float_time_overflows(hour: i32, min: i32, sec: f64) -> bool {
    if hour < 0 || hour > HOURS_PER_DAY || min < 0 || min >= MINS_PER_HOUR as i32 {
        return true;
    }
    if sec.is_nan() {
        return true;
    }
    let sec = rint(sec * USECS_PER_SEC as f64);
    if sec < 0.0 || sec > (SECS_PER_MINUTE * USECS_PER_SEC) as f64 {
        return true;
    }
    if ((hour as i64 * MINS_PER_HOUR + min as i64) * SECS_PER_MINUTE * USECS_PER_SEC) + sec as i64
        > USECS_PER_DAY
    {
        return true;
    }
    false
}

/// C rint() with the default rounding mode (round half to even).
#[inline]
pub(crate) fn rint(x: f64) -> f64 {
    x.round_ties_even()
}

/// FLOAT8_FITS_IN_INT64
#[inline]
pub(crate) fn float8_fits_in_int64(num: f64) -> bool {
    num >= i64::MIN as f64 && num < -(i64::MIN as f64)
}

/// FLOAT8_FITS_IN_INT32
#[inline]
pub(crate) fn float8_fits_in_int32(num: f64) -> bool {
    num >= i32::MIN as f64 && num < -(i32::MIN as f64)
}

/// Julian-day based helpers for the public API.
pub(crate) fn ymd_to_date_checked(y: i32, m: i32, d: i32) -> Result<i32> {
    if !(1..=12).contains(&m) || d < 1 || d > days_in_month(y, m) {
        return Err(Error::new(
            DATETIME_FIELD_OVERFLOW,
            format!("date field value out of range: {}-{:02}-{:02}", y, m, d),
        ));
    }
    if !is_valid_julian(y, m, d) {
        return Err(Error::new(
            DATETIME_VALUE_OUT_OF_RANGE,
            format!("date out of range: {}-{:02}-{:02}", y, m, d),
        ));
    }
    let date = (date2j(y, m, d) - POSTGRES_EPOCH_JDATE) as i64;
    if !is_valid_date(date) {
        return Err(Error::new(
            DATETIME_VALUE_OUT_OF_RANGE,
            format!("date out of range: {}-{:02}-{:02}", y, m, d),
        ));
    }
    Ok(date as i32)
}
