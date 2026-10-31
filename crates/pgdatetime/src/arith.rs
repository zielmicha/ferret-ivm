//! Casts and arithmetic: ports of date.c / timestamp.c operators
//! (PostgreSQL License).

use std::cmp::Ordering;

use crate::core::*;
use crate::decimal::c_format_g;
use crate::error::*;
use crate::tz::{self, DecodedTzName, ZoneRef};
use crate::{Date, Error, Interval, Result, Time, TimeZone, Timestamp, TimestampTz};

// ---------------------------------------------------------------------------
// casts
// ---------------------------------------------------------------------------

fn date_oor_for_timestamp() -> Error {
    Error::new(
        DATETIME_VALUE_OUT_OF_RANGE,
        "date out of range for timestamp",
    )
}

/// date2timestamp()
pub fn date_to_timestamp(d: Date) -> Result<Timestamp> {
    if d == DATEVAL_NOBEGIN {
        return Ok(DT_NOBEGIN);
    }
    if d == DATEVAL_NOEND {
        return Ok(DT_NOEND);
    }
    if d >= TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE {
        return Err(date_oor_for_timestamp());
    }
    Ok((d as i64).wrapping_mul(USECS_PER_DAY))
}

/// timestamp_date()
pub fn timestamp_to_date(ts: Timestamp) -> Result<Date> {
    if ts == DT_NOBEGIN {
        return Ok(DATEVAL_NOBEGIN);
    }
    if ts == DT_NOEND {
        return Ok(DATEVAL_NOEND);
    }
    let (tm, _) = timestamp2tm(ts)?;
    Ok(date2j(tm.year, tm.mon, tm.mday) - POSTGRES_EPOCH_JDATE)
}

/// date2timestamptz()
pub fn date_to_timestamptz(d: Date, tz: &TimeZone) -> Result<TimestampTz> {
    if d == DATEVAL_NOBEGIN {
        return Ok(DT_NOBEGIN);
    }
    if d == DATEVAL_NOEND {
        return Ok(DT_NOEND);
    }
    if d >= TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE {
        return Err(date_oor_for_timestamp());
    }
    let (year, mon, mday) = j2date(d + POSTGRES_EPOCH_JDATE);
    let mut tm = Tm {
        year,
        mon,
        mday,
        ..Default::default()
    };
    let off = tz::determine_offset(&mut tm, &tz.zone_ref());
    let result = (d as i64)
        .wrapping_mul(USECS_PER_DAY)
        .wrapping_add(off as i64 * USECS_PER_SEC);
    if !is_valid_timestamp(result) {
        return Err(date_oor_for_timestamp());
    }
    Ok(result)
}

/// timestamptz_date()
pub fn timestamptz_to_date(t: TimestampTz, tz: &TimeZone) -> Result<Date> {
    if t == DT_NOBEGIN {
        return Ok(DATEVAL_NOBEGIN);
    }
    if t == DT_NOEND {
        return Ok(DATEVAL_NOEND);
    }
    let tt = timestamp2tm_tz(t, &tz.zone_ref())?;
    Ok(date2j(tt.tm.year, tt.tm.mon, tt.tm.mday) - POSTGRES_EPOCH_JDATE)
}

/// timestamp2timestamptz()
pub fn timestamp_to_timestamptz(ts: Timestamp, tz: &TimeZone) -> Result<TimestampTz> {
    timestamp_to_timestamptz_zone(ts, &tz.zone_ref())
}

pub(crate) fn timestamp_to_timestamptz_zone(ts: Timestamp, zone: &ZoneRef) -> Result<TimestampTz> {
    if timestamp_not_finite(ts) {
        return Ok(ts);
    }
    let (mut tm, _) = timestamp2tm(ts)?;
    let off = tz::determine_offset(&mut tm, zone);
    let result = dt2local(ts, off.wrapping_neg());
    if is_valid_timestamp(result) {
        Ok(result)
    } else {
        Err(timestamp_out_of_range())
    }
}

/// timestamptz2timestamp()
pub fn timestamptz_to_timestamp(t: TimestampTz, tz: &TimeZone) -> Result<Timestamp> {
    timestamptz_to_timestamp_zone(t, &tz.zone_ref())
}

pub(crate) fn timestamptz_to_timestamp_zone(t: TimestampTz, zone: &ZoneRef) -> Result<Timestamp> {
    if timestamp_not_finite(t) {
        return Ok(t);
    }
    let tt = timestamp2tm_tz(t, zone)?;
    tm2timestamp(&tt.tm, tt.fsec, None)
}

/// timestamp_time(); PostgreSQL returns NULL for infinite input.
pub fn timestamp_to_time_opt(ts: Timestamp) -> Result<Option<Time>> {
    if timestamp_not_finite(ts) {
        return Ok(None);
    }
    let (tm, fsec) = timestamp2tm(ts)?;
    Ok(Some(time2t(tm.hour, tm.min, tm.sec, fsec)))
}

pub fn timestamp_to_time(ts: Timestamp) -> Result<Time> {
    timestamp_to_time_opt(ts)?.ok_or_else(null_result)
}

/// timestamptz_time(); PostgreSQL returns NULL for infinite input.
pub fn timestamptz_to_time_opt(t: TimestampTz, tz: &TimeZone) -> Result<Option<Time>> {
    if timestamp_not_finite(t) {
        return Ok(None);
    }
    let tt = timestamp2tm_tz(t, &tz.zone_ref())?;
    Ok(Some(time2t(tt.tm.hour, tt.tm.min, tt.tm.sec, tt.fsec)))
}

pub fn timestamptz_to_time(t: TimestampTz, tz: &TimeZone) -> Result<Time> {
    timestamptz_to_time_opt(t, tz)?.ok_or_else(null_result)
}

pub fn time_to_interval(t: Time) -> Interval {
    Interval {
        months: 0,
        days: 0,
        micros: t,
    }
}

/// interval_time(): errors for infinite intervals.
pub fn interval_to_time_checked(iv: Interval) -> Result<Time> {
    if interval_not_finite(&iv) {
        return Err(Error::new(
            DATETIME_VALUE_OUT_OF_RANGE,
            "cannot convert infinite interval to time",
        ));
    }
    Ok(interval_to_time(iv))
}

/// interval_time() (for infinite input PostgreSQL raises an error; use
/// `interval_to_time_checked` to get it).
pub fn interval_to_time(iv: Interval) -> Time {
    let mut result = iv.micros % USECS_PER_DAY;
    if result < 0 {
        result += USECS_PER_DAY;
    }
    result
}

// ---------------------------------------------------------------------------
// date arithmetic
// ---------------------------------------------------------------------------

/// date_pli()
pub fn date_pl_days(d: Date, n: i32) -> Result<Date> {
    if date_not_finite(d) {
        return Ok(d);
    }
    let result = d.wrapping_add(n);
    if (if n >= 0 { result < d } else { result > d }) || !is_valid_date(result as i64) {
        return Err(date_out_of_range());
    }
    Ok(result)
}

/// date_mii()
pub fn date_mi_days(d: Date, n: i32) -> Result<Date> {
    if date_not_finite(d) {
        return Ok(d);
    }
    let result = d.wrapping_sub(n);
    if (if n >= 0 { result > d } else { result < d }) || !is_valid_date(result as i64) {
        return Err(date_out_of_range());
    }
    Ok(result)
}

/// date_mi()
pub fn date_mi_date(a: Date, b: Date) -> Result<i32> {
    if date_not_finite(a) || date_not_finite(b) {
        return Err(Error::new(
            DATETIME_VALUE_OUT_OF_RANGE,
            "cannot subtract infinite dates",
        ));
    }
    Ok(a.wrapping_sub(b))
}

pub fn date_pl_interval(d: Date, iv: Interval) -> Result<Timestamp> {
    timestamp_pl_interval(date_to_timestamp(d)?, iv)
}

pub fn date_mi_interval(d: Date, iv: Interval) -> Result<Timestamp> {
    timestamp_mi_interval(date_to_timestamp(d)?, iv)
}

/// datetime_timestamp()
pub fn date_pl_time(d: Date, t: Time) -> Result<Timestamp> {
    let mut result = date_to_timestamp(d)?;
    if !timestamp_not_finite(result) {
        result = result.wrapping_add(t);
        if !is_valid_timestamp(result) {
            return Err(timestamp_out_of_range());
        }
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// timestamp arithmetic
// ---------------------------------------------------------------------------

fn add_months_tm(tm: &mut Tm, months: i32) -> Result<()> {
    tm.mon = tm
        .mon
        .checked_add(months)
        .ok_or_else(timestamp_out_of_range)?;
    if tm.mon > MONTHS_PER_YEAR {
        tm.year += (tm.mon - 1) / MONTHS_PER_YEAR;
        tm.mon = ((tm.mon - 1) % MONTHS_PER_YEAR) + 1;
    } else if tm.mon < 1 {
        tm.year += tm.mon / MONTHS_PER_YEAR - 1;
        tm.mon = tm.mon % MONTHS_PER_YEAR + MONTHS_PER_YEAR;
    }
    let dim = DAY_TAB[isleap(tm.year) as usize][(tm.mon - 1) as usize];
    if tm.mday > dim {
        tm.mday = dim;
    }
    Ok(())
}

/// timestamp_pl_interval()
pub fn timestamp_pl_interval(ts: Timestamp, span: Interval) -> Result<Timestamp> {
    timestamp_add_interval(ts, span, None)
}

/// Calendar arithmetic is shared by timestamp and timestamptz. A zone is
/// resolved again after each calendar step, so months and days cross DST
/// in local time while the final microseconds remain elapsed time.
fn timestamp_add_interval(ts: Timestamp, span: Interval, zone: Option<&ZoneRef>) -> Result<Timestamp> {
    if interval_is_nobegin(&span) {
        if ts == DT_NOEND {
            return Err(timestamp_out_of_range());
        }
        return Ok(DT_NOBEGIN);
    }
    if interval_is_noend(&span) {
        if ts == DT_NOBEGIN {
            return Err(timestamp_out_of_range());
        }
        return Ok(DT_NOEND);
    }
    if timestamp_not_finite(ts) {
        return Ok(ts);
    }
    let mut timestamp = ts;
    for (amount, months) in [(span.months, true), (span.days, false)] {
        if amount == 0 {
            continue;
        }
        let (mut tm, fsec) = match zone {
            Some(z) => {
                let tt = timestamp2tm_tz(timestamp, z)?;
                (tt.tm, tt.fsec)
            }
            None => timestamp2tm(timestamp)?,
        };
        if months {
            add_months_tm(&mut tm, amount)?;
        } else {
            let julian = date2j(tm.year, tm.mon, tm.mday).checked_add(amount)
                .filter(|j| *j >= if zone.is_some() { -1 } else { 0 })
                .ok_or_else(timestamp_out_of_range)?;
            (tm.year, tm.mon, tm.mday) = j2date(julian);
        }
        let offset = zone.map(|z| tz::determine_offset(&mut tm, z));
        timestamp = tm2timestamp(&tm, fsec, offset)?;
    }
    timestamp = timestamp.checked_add(span.micros).ok_or_else(timestamp_out_of_range)?;
    if !is_valid_timestamp(timestamp) {
        return Err(timestamp_out_of_range());
    }
    Ok(timestamp)
}

/// interval_um_internal()
fn interval_negate(iv: &Interval) -> Result<Interval> {
    if interval_is_nobegin(iv) {
        return Ok(INTERVAL_NOEND);
    }
    if interval_is_noend(iv) {
        return Ok(INTERVAL_NOBEGIN);
    }
    checked_interval(iv.months.checked_neg(), iv.days.checked_neg(), iv.micros.checked_neg())
}

pub fn timestamp_mi_interval(ts: Timestamp, span: Interval) -> Result<Timestamp> {
    timestamp_pl_interval(ts, interval_negate(&span)?)
}

/// timestamptz_pl_interval_internal()
pub(crate) fn timestamptz_pl_interval_zone(
    ts: TimestampTz,
    span: Interval,
    zone: &ZoneRef,
) -> Result<TimestampTz> {
    timestamp_add_interval(ts, span, Some(zone))
}

pub fn timestamptz_pl_interval(t: TimestampTz, iv: Interval, tz: &TimeZone) -> Result<TimestampTz> {
    timestamptz_pl_interval_zone(t, iv, &tz.zone_ref())
}

pub fn timestamptz_mi_interval(t: TimestampTz, iv: Interval, tz: &TimeZone) -> Result<TimestampTz> {
    timestamptz_pl_interval_zone(t, interval_negate(&iv)?, &tz.zone_ref())
}

/// timestamp_mi()
pub fn timestamp_mi(dt1: Timestamp, dt2: Timestamp) -> Result<Interval> {
    if timestamp_not_finite(dt1) || timestamp_not_finite(dt2) {
        if dt1 == DT_NOBEGIN {
            if dt2 == DT_NOBEGIN {
                return Err(interval_out_of_range());
            }
            return Ok(INTERVAL_NOBEGIN);
        } else if dt1 == DT_NOEND {
            if dt2 == DT_NOEND {
                return Err(interval_out_of_range());
            }
            return Ok(INTERVAL_NOEND);
        } else if dt2 == DT_NOBEGIN {
            return Ok(INTERVAL_NOEND);
        } else {
            return Ok(INTERVAL_NOBEGIN);
        }
    }
    let time = dt1.checked_sub(dt2).ok_or_else(interval_out_of_range)?;
    justify_hours(Interval {
        months: 0,
        days: 0,
        micros: time,
    })
}

pub fn timestamptz_mi(a: TimestampTz, b: TimestampTz) -> Result<Interval> {
    timestamp_mi(a, b)
}

// ---------------------------------------------------------------------------
// time arithmetic
// ---------------------------------------------------------------------------

/// time_pl_interval() (PostgreSQL errors for infinite intervals; see
/// `time_pl_interval_checked`)
pub fn time_pl_interval(t: Time, iv: Interval) -> Time {
    let mut result = t.wrapping_add(iv.micros);
    result -= result / USECS_PER_DAY * USECS_PER_DAY;
    if result < 0 {
        result += USECS_PER_DAY;
    }
    result
}

pub fn time_pl_interval_checked(t: Time, iv: Interval) -> Result<Time> {
    if interval_not_finite(&iv) {
        return Err(Error::new(
            DATETIME_VALUE_OUT_OF_RANGE,
            "cannot add infinite interval to time",
        ));
    }
    Ok(time_pl_interval(t, iv))
}

/// time_mi_interval() (see `time_mi_interval_checked` for infinite input)
pub fn time_mi_interval(t: Time, iv: Interval) -> Time {
    let mut result = t.wrapping_sub(iv.micros);
    result -= result / USECS_PER_DAY * USECS_PER_DAY;
    if result < 0 {
        result += USECS_PER_DAY;
    }
    result
}

pub fn time_mi_interval_checked(t: Time, iv: Interval) -> Result<Time> {
    if interval_not_finite(&iv) {
        return Err(Error::new(
            DATETIME_VALUE_OUT_OF_RANGE,
            "cannot subtract infinite interval from time",
        ));
    }
    Ok(time_mi_interval(t, iv))
}

pub fn time_mi_time(a: Time, b: Time) -> Interval {
    Interval {
        months: 0,
        days: 0,
        micros: a.wrapping_sub(b),
    }
}

// ---------------------------------------------------------------------------
// interval arithmetic
// ---------------------------------------------------------------------------

fn checked_interval(months: Option<i32>, days: Option<i32>, micros: Option<i64>) -> Result<Interval> {
    let r = Interval {
        months: months.ok_or_else(interval_out_of_range)?,
        days: days.ok_or_else(interval_out_of_range)?,
        micros: micros.ok_or_else(interval_out_of_range)?,
    };
    if interval_not_finite(&r) {
        return Err(interval_out_of_range());
    }
    Ok(r)
}

fn finite_interval_pl(a: &Interval, b: &Interval) -> Result<Interval> {
    checked_interval(a.months.checked_add(b.months), a.days.checked_add(b.days), a.micros.checked_add(b.micros))
}

fn finite_interval_mi(a: &Interval, b: &Interval) -> Result<Interval> {
    checked_interval(a.months.checked_sub(b.months), a.days.checked_sub(b.days), a.micros.checked_sub(b.micros))
}

/// interval_pl()
pub fn interval_pl(a: Interval, b: Interval) -> Result<Interval> {
    if interval_is_nobegin(&a) {
        if interval_is_noend(&b) {
            return Err(interval_out_of_range());
        }
        return Ok(INTERVAL_NOBEGIN);
    }
    if interval_is_noend(&a) {
        if interval_is_nobegin(&b) {
            return Err(interval_out_of_range());
        }
        return Ok(INTERVAL_NOEND);
    }
    if interval_not_finite(&b) {
        return Ok(b);
    }
    finite_interval_pl(&a, &b)
}

/// interval_mi()
pub fn interval_mi(a: Interval, b: Interval) -> Result<Interval> {
    if interval_is_nobegin(&a) {
        if interval_is_nobegin(&b) {
            return Err(interval_out_of_range());
        }
        return Ok(INTERVAL_NOBEGIN);
    }
    if interval_is_noend(&a) {
        if interval_is_noend(&b) {
            return Err(interval_out_of_range());
        }
        return Ok(INTERVAL_NOEND);
    }
    if interval_is_nobegin(&b) {
        return Ok(INTERVAL_NOEND);
    }
    if interval_is_noend(&b) {
        return Ok(INTERVAL_NOBEGIN);
    }
    finite_interval_mi(&a, &b)
}

/// interval_um()
pub fn interval_um(a: Interval) -> Result<Interval> {
    interval_negate(&a)
}

/// TSROUND()
#[inline]
fn tsround(j: f64) -> f64 {
    rint(j * 1_000_000.0) / 1_000_000.0
}

/// interval_mul()
pub fn interval_mul(span: Interval, factor: f64) -> Result<Interval> {
    if factor.is_nan() {
        return Err(interval_out_of_range());
    }
    if interval_not_finite(&span) {
        if factor == 0.0 {
            return Err(interval_out_of_range());
        }
        return if factor < 0.0 {
            interval_negate(&span)
        } else {
            Ok(span)
        };
    }
    if factor.is_infinite() {
        let isign = interval_sign(&span);
        if isign == 0 {
            return Err(interval_out_of_range());
        }
        return Ok(if factor * (isign as f64) < 0.0 {
            INTERVAL_NOBEGIN
        } else {
            INTERVAL_NOEND
        });
    }
    scale_interval(span, |x| x * factor)
}

/// interval_div()
pub fn interval_div(span: Interval, factor: f64) -> Result<Interval> {
    if factor == 0.0 {
        return Err(Error::new(DIVISION_BY_ZERO, "division by zero"));
    }
    if factor.is_nan() {
        return Err(interval_out_of_range());
    }
    if interval_not_finite(&span) {
        if factor.is_infinite() {
            return Err(interval_out_of_range());
        }
        return if factor < 0.0 {
            interval_negate(&span)
        } else {
            Ok(span)
        };
    }
    scale_interval(span, |x| x / factor)
}

/// Scale the finite components, carrying fractional months into days and
/// fractional days into microseconds with PostgreSQL's rounding rules.
fn scale_interval(span: Interval, scale: impl Fn(f64) -> f64) -> Result<Interval> {
    let mut result = Interval::default();
    let months = scale(span.months as f64);
    if months.is_nan() || !float8_fits_in_int32(months) {
        return Err(interval_out_of_range());
    }
    result.months = months as i32;
    let days = scale(span.days as f64);
    if days.is_nan() || !float8_fits_in_int32(days) {
        return Err(interval_out_of_range());
    }
    result.days = days as i32;
    let month_remainder_days =
        tsround((months - result.months as f64) * DAYS_PER_MONTH as f64);
    let mut sec_remainder = tsround(
        (days - result.days as f64 + month_remainder_days
            - (month_remainder_days as i32) as f64)
            * SECS_PER_DAY as f64,
    );
    if sec_remainder.abs() >= SECS_PER_DAY as f64 {
        let extra = (sec_remainder / SECS_PER_DAY as f64) as i32;
        result.days = result
            .days
            .checked_add(extra)
            .ok_or_else(interval_out_of_range)?;
        sec_remainder -= extra as f64 * SECS_PER_DAY as f64;
    }
    result.days = result
        .days
        .checked_add(month_remainder_days as i32)
        .ok_or_else(interval_out_of_range)?;
    let rd = rint(scale(span.micros as f64) + sec_remainder * USECS_PER_SEC as f64);
    if rd.is_nan() || !float8_fits_in_int64(rd) {
        return Err(interval_out_of_range());
    }
    result.micros = rd as i64;
    if interval_not_finite(&result) {
        return Err(interval_out_of_range());
    }
    Ok(result)
}

/// interval_cmp_value(): the linear span used for comparison and hashing
/// (days are 24 hours, months are 30 days).
pub fn interval_cmp_value(a: Interval) -> i128 {
    let days = a.months as i64 * 30 + a.days as i64;
    a.micros as i128 + days as i128 * USECS_PER_DAY as i128
}

pub fn interval_cmp(a: Interval, b: Interval) -> Ordering {
    interval_cmp_value(a).cmp(&interval_cmp_value(b))
}

fn interval_sign(a: &Interval) -> i32 {
    match interval_cmp_value(*a).cmp(&0) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

/// interval_hash() value: hashint8 of the low 64 bits of interval_cmp_value
/// (the low 64 bits are returned; equal intervals give equal values).
pub fn interval_hash_key(a: Interval) -> i64 {
    interval_cmp_value(a) as i64
}

/// interval_justify_hours()
pub fn justify_hours(span: Interval) -> Result<Interval> {
    let mut r = span;
    if interval_not_finite(&r) {
        return Ok(r);
    }
    let wholeday = r.micros / USECS_PER_DAY;
    if wholeday != 0 {
        r.micros -= wholeday * USECS_PER_DAY;
    }
    r.days = (r.days as i64)
        .checked_add(wholeday)
        .filter(|v| *v >= i32::MIN as i64 && *v <= i32::MAX as i64)
        .ok_or_else(interval_out_of_range)? as i32;
    if r.days > 0 && r.micros < 0 {
        r.micros += USECS_PER_DAY;
        r.days -= 1;
    } else if r.days < 0 && r.micros > 0 {
        r.micros -= USECS_PER_DAY;
        r.days += 1;
    }
    Ok(r)
}

/// interval_justify_days()
pub fn justify_days(span: Interval) -> Result<Interval> {
    let mut r = span;
    if interval_not_finite(&r) {
        return Ok(r);
    }
    let wholemonth = r.days / DAYS_PER_MONTH;
    r.days -= wholemonth * DAYS_PER_MONTH;
    r.months = r
        .months
        .checked_add(wholemonth)
        .ok_or_else(interval_out_of_range)?;
    if r.months > 0 && r.days < 0 {
        r.days += DAYS_PER_MONTH;
        r.months -= 1;
    } else if r.months < 0 && r.days > 0 {
        r.days -= DAYS_PER_MONTH;
        r.months += 1;
    }
    Ok(r)
}

/// interval_justify_interval()
pub fn justify_interval(span: Interval) -> Result<Interval> {
    let mut r = span;
    if interval_not_finite(&r) {
        return Ok(r);
    }
    if (r.days > 0 && r.micros > 0) || (r.days < 0 && r.micros < 0) {
        let wholemonth = r.days / DAYS_PER_MONTH;
        r.days -= wholemonth * DAYS_PER_MONTH;
        r.months = r
            .months
            .checked_add(wholemonth)
            .ok_or_else(interval_out_of_range)?;
    }
    let wholeday = r.micros / USECS_PER_DAY;
    if wholeday != 0 {
        r.micros -= wholeday * USECS_PER_DAY;
    }
    r.days = (r.days as i64 + wholeday) as i32;
    let wholemonth = r.days / DAYS_PER_MONTH;
    r.days -= wholemonth * DAYS_PER_MONTH;
    r.months = r
        .months
        .checked_add(wholemonth)
        .ok_or_else(interval_out_of_range)?;
    if r.months > 0 && (r.days < 0 || (r.days == 0 && r.micros < 0)) {
        r.days += DAYS_PER_MONTH;
        r.months -= 1;
    } else if r.months < 0 && (r.days > 0 || (r.days == 0 && r.micros > 0)) {
        r.days -= DAYS_PER_MONTH;
        r.months += 1;
    }
    if r.days > 0 && r.micros < 0 {
        r.micros += USECS_PER_DAY;
        r.days -= 1;
    } else if r.days < 0 && r.micros > 0 {
        r.micros -= USECS_PER_DAY;
        r.days += 1;
    }
    Ok(r)
}

// ---------------------------------------------------------------------------
// age, date_bin
// ---------------------------------------------------------------------------

fn age_common(dt1: i64, dt2: i64, tm1: &Tm, fsec1: i32, tm2: &Tm, fsec2: i32) -> Result<Interval> {
    let mut tm = Itm {
        usec: fsec1 - fsec2,
        sec: tm1.sec - tm2.sec,
        min: tm1.min - tm2.min,
        hour: (tm1.hour - tm2.hour) as i64,
        mday: tm1.mday - tm2.mday,
        mon: tm1.mon - tm2.mon,
        year: tm1.year - tm2.year,
    };
    let flip = |tm: &mut Itm| {
        tm.usec = -tm.usec;
        tm.sec = -tm.sec;
        tm.min = -tm.min;
        tm.hour = -tm.hour;
        tm.mday = -tm.mday;
        tm.mon = -tm.mon;
        tm.year = -tm.year;
    };
    if dt1 < dt2 {
        flip(&mut tm);
    }
    while tm.usec < 0 {
        tm.usec += USECS_PER_SEC as i32;
        tm.sec -= 1;
    }
    while tm.sec < 0 {
        tm.sec += SECS_PER_MINUTE as i32;
        tm.min -= 1;
    }
    while tm.min < 0 {
        tm.min += MINS_PER_HOUR as i32;
        tm.hour -= 1;
    }
    while tm.hour < 0 {
        tm.hour += HOURS_PER_DAY as i64;
        tm.mday -= 1;
    }
    while tm.mday < 0 {
        if dt1 < dt2 {
            tm.mday += DAY_TAB[isleap(tm1.year) as usize][(tm1.mon - 1) as usize];
        } else {
            tm.mday += DAY_TAB[isleap(tm2.year) as usize][(tm2.mon - 1) as usize];
        }
        tm.mon -= 1;
    }
    while tm.mon < 0 {
        tm.mon += MONTHS_PER_YEAR;
        tm.year -= 1;
    }
    if dt1 < dt2 {
        flip(&mut tm);
    }
    itm2interval(&tm).ok_or_else(interval_out_of_range)
}

fn age_infinities(dt1: i64, dt2: i64) -> Option<Result<Interval>> {
    if dt1 == DT_NOBEGIN {
        if dt2 == DT_NOBEGIN {
            return Some(Err(interval_out_of_range()));
        }
        return Some(Ok(INTERVAL_NOBEGIN));
    }
    if dt1 == DT_NOEND {
        if dt2 == DT_NOEND {
            return Some(Err(interval_out_of_range()));
        }
        return Some(Ok(INTERVAL_NOEND));
    }
    if dt2 == DT_NOBEGIN {
        return Some(Ok(INTERVAL_NOEND));
    }
    if dt2 == DT_NOEND {
        return Some(Ok(INTERVAL_NOBEGIN));
    }
    None
}

/// timestamp_age()
pub fn age(a: Timestamp, b: Timestamp) -> Result<Interval> {
    if let Some(r) = age_infinities(a, b) {
        return r;
    }
    let (tm1, fsec1) = timestamp2tm(a)?;
    let (tm2, fsec2) = timestamp2tm(b)?;
    age_common(a, b, &tm1, fsec1, &tm2, fsec2)
}

/// timestamptz_age()
pub fn age_tz(a: TimestampTz, b: TimestampTz, tz: &TimeZone) -> Result<Interval> {
    if let Some(r) = age_infinities(a, b) {
        return r;
    }
    let zone = tz.zone_ref();
    let t1 = timestamp2tm_tz(a, &zone)?;
    let t2 = timestamp2tm_tz(b, &zone)?;
    age_common(a, b, &t1.tm, t1.fsec, &t2.tm, t2.fsec)
}

/// timestamp_bin() / timestamptz_bin()
pub fn date_bin(stride: Interval, source: Timestamp, origin: Timestamp) -> Result<Timestamp> {
    if timestamp_not_finite(source) {
        return Ok(source);
    }
    if timestamp_not_finite(origin) {
        return Err(Error::new(
            DATETIME_VALUE_OUT_OF_RANGE,
            "origin out of range",
        ));
    }
    if interval_not_finite(&stride) {
        return Err(Error::new(
            DATETIME_VALUE_OUT_OF_RANGE,
            "timestamps cannot be binned into infinite intervals",
        ));
    }
    if stride.months != 0 {
        return Err(Error::new(
            FEATURE_NOT_SUPPORTED,
            "timestamps cannot be binned into intervals containing months or years",
        ));
    }
    let stride_usecs = (stride.days as i64)
        .checked_mul(USECS_PER_DAY)
        .and_then(|v| v.checked_add(stride.micros))
        .ok_or_else(interval_out_of_range)?;
    if stride_usecs <= 0 {
        return Err(Error::new(
            DATETIME_VALUE_OUT_OF_RANGE,
            "stride must be greater than zero",
        ));
    }
    let tm_diff = source
        .checked_sub(origin)
        .ok_or_else(interval_out_of_range)?;
    let tm_modulo = tm_diff % stride_usecs;
    let tm_delta = tm_diff - tm_modulo;
    let mut result = origin + tm_delta;
    if tm_modulo < 0 {
        result = match result.checked_sub(stride_usecs) {
            Some(r) if is_valid_timestamp(r) => r,
            _ => return Err(timestamp_out_of_range()),
        };
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// constructors
// ---------------------------------------------------------------------------

/// make_date()
pub fn make_date(y: i32, m: i32, d: i32) -> Result<Date> {
    let mut tm = Tm {
        year: y,
        mon: m,
        mday: d,
        ..Default::default()
    };
    let mut bc = false;
    if tm.year < 0 {
        bc = true;
        tm.year = tm.year.wrapping_neg();
    }
    if crate::datetime::validate_date(crate::datetime::DTK_DATE_M, false, false, bc, &mut tm, 0)
        .is_err()
    {
        return Err(Error::new(
            DATETIME_FIELD_OVERFLOW,
            format!(
                "date field value out of range: {}-{:02}-{:02}",
                tm.year, tm.mon, tm.mday
            ),
        ));
    }
    let oor = |tm: &Tm| {
        Error::new(
            DATETIME_VALUE_OUT_OF_RANGE,
            format!(
                "date out of range: {}-{:02}-{:02}",
                tm.year, tm.mon, tm.mday
            ),
        )
    };
    if !is_valid_julian(tm.year, tm.mon, tm.mday) {
        return Err(oor(&tm));
    }
    let date = (date2j(tm.year, tm.mon, tm.mday) - POSTGRES_EPOCH_JDATE) as i64;
    if !is_valid_date(date) {
        return Err(oor(&tm));
    }
    Ok(date as i32)
}

/// make_time()
pub fn make_time(h: i32, m: i32, s: f64) -> Result<Time> {
    if float_time_overflows(h, m, s) {
        return Err(Error::new(
            DATETIME_FIELD_OVERFLOW,
            format!(
                "time field value out of range: {}:{:02}:{}",
                h,
                m,
                c_format_g(s, 2, true)
            ),
        ));
    }
    Ok(
        ((h as i64 * MINS_PER_HOUR + m as i64) * SECS_PER_MINUTE) * USECS_PER_SEC
            + rint(s * USECS_PER_SEC as f64) as i64,
    )
}

/// make_timestamp_internal()
fn make_timestamp_internal(
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    min: i32,
    sec: f64,
) -> Result<Timestamp> {
    let mut tm = Tm {
        year,
        mon: month,
        mday: day,
        ..Default::default()
    };
    let mut bc = false;
    if tm.year < 0 {
        bc = true;
        tm.year = tm.year.wrapping_neg();
    }
    if crate::datetime::validate_date(crate::datetime::DTK_DATE_M, false, false, bc, &mut tm, 0)
        .is_err()
    {
        return Err(Error::new(
            DATETIME_FIELD_OVERFLOW,
            format!(
                "date field value out of range: {}-{:02}-{:02}",
                year, month, day
            ),
        ));
    }
    if !is_valid_julian(tm.year, tm.mon, tm.mday) {
        return Err(Error::new(
            DATETIME_VALUE_OUT_OF_RANGE,
            format!("date out of range: {}-{:02}-{:02}", year, month, day),
        ));
    }
    let date = (date2j(tm.year, tm.mon, tm.mday) - POSTGRES_EPOCH_JDATE) as i64;
    if float_time_overflows(hour, min, sec) {
        return Err(Error::new(
            DATETIME_FIELD_OVERFLOW,
            format!(
                "time field value out of range: {}:{:02}:{}",
                hour,
                min,
                c_format_g(sec, 2, true)
            ),
        ));
    }
    let time = ((hour as i64 * MINS_PER_HOUR + min as i64) * SECS_PER_MINUTE) * USECS_PER_SEC
        + rint(sec * USECS_PER_SEC as f64) as i64;
    let oor = || {
        Error::new(
            DATETIME_VALUE_OUT_OF_RANGE,
            format!(
                "timestamp out of range: {}-{:02}-{:02} {}:{:02}:{}",
                year,
                month,
                day,
                hour,
                min,
                c_format_g(sec, 2, true)
            ),
        )
    };
    let result = date.wrapping_mul(USECS_PER_DAY).wrapping_add(time);
    if (result.wrapping_sub(time)) / USECS_PER_DAY != date {
        return Err(oor());
    }
    if (result < 0 && date > 0) || (result > 0 && date < -1) {
        return Err(oor());
    }
    if !is_valid_timestamp(result) {
        return Err(oor());
    }
    Ok(result)
}

pub fn make_timestamp(y: i32, mo: i32, d: i32, h: i32, mi: i32, s: f64) -> Result<Timestamp> {
    make_timestamp_internal(y, mo, d, h, mi, s)
}

/// make_timestamptz() with 6 arguments: the fields are local time in `tz`
/// (the session time zone).
pub fn make_timestamptz(
    y: i32,
    mo: i32,
    d: i32,
    h: i32,
    mi: i32,
    s: f64,
    tz: &TimeZone,
) -> Result<TimestampTz> {
    let ts = make_timestamp_internal(y, mo, d, h, mi, s)?;
    timestamp_to_timestamptz(ts, tz)
}

/// make_timestamptz(y, m, d, h, mi, s, zone text): parse_sane_timezone()
/// semantics for the zone (numeric offsets use ISO sign convention).
pub fn make_timestamptz_at_zone(
    y: i32,
    mo: i32,
    d: i32,
    h: i32,
    mi: i32,
    s: f64,
    zone: &str,
) -> Result<TimestampTz> {
    let ts = make_timestamp_internal(y, mo, d, h, mi, s)?;
    let (mut tm, _) = timestamp2tm(ts)?;
    let tzo = parse_sane_timezone(&mut tm, zone)?;
    let result = dt2local(ts, -tzo);
    if !is_valid_timestamp(result) {
        return Err(timestamp_out_of_range());
    }
    Ok(result)
}

/// parse_sane_timezone(): returns PG tz (seconds west)
fn parse_sane_timezone(tm: &mut Tm, tzname: &str) -> Result<i32> {
    let b = tzname.as_bytes();
    if !b.is_empty() && b[0].is_ascii_digit() {
        return Err(Error::new(
            INVALID_PARAMETER_VALUE,
            format!(
                "invalid input syntax for type numeric time zone: \"{}\"",
                tzname
            ),
        ));
    }
    match crate::datetime::decode_timezone(b) {
        Ok(tz) => Ok(tz),
        Err(DTERR_TZDISP_OVERFLOW) => Err(Error::new(
            INVALID_PARAMETER_VALUE,
            format!("numeric time zone \"{}\" out of range", tzname),
        )),
        Err(DTERR_BAD_FORMAT) => match tz::decode_timezone_name(tzname)? {
            DecodedTzName::Fixed(off) => Ok(-off),
            DecodedTzName::Dyn(z) => Ok(tz::determine_abbrev_offset(tm, tzname, &z)),
            DecodedTzName::Zone(z) => Ok(tz::determine_offset(tm, &z)),
        },
        Err(_) => Err(Error::new(
            INVALID_PARAMETER_VALUE,
            format!("time zone \"{}\" not recognized", tzname),
        )),
    }
}

/// make_interval()
pub fn make_interval(
    years: i32,
    months: i32,
    weeks: i32,
    days: i32,
    hours: i32,
    mins: i32,
    secs: f64,
) -> Result<Interval> {
    if secs.is_infinite() || secs.is_nan() {
        return Err(interval_out_of_range());
    }
    let month = years
        .checked_mul(MONTHS_PER_YEAR)
        .and_then(|v| v.checked_add(months))
        .ok_or_else(interval_out_of_range)?;
    let day = weeks
        .checked_mul(DAYS_PER_WEEK)
        .and_then(|v| v.checked_add(days))
        .ok_or_else(interval_out_of_range)?;
    let mut time = hours as i64 * USECS_PER_HOUR + mins as i64 * USECS_PER_MINUTE;
    let s = rint(float8_mul(secs, USECS_PER_SEC as f64)?);
    if !float8_fits_in_int64(s) {
        return Err(interval_out_of_range());
    }
    time = time
        .checked_add(s as i64)
        .ok_or_else(interval_out_of_range)?;
    let r = Interval {
        months: month,
        days: day,
        micros: time,
    };
    if interval_not_finite(&r) {
        return Err(interval_out_of_range());
    }
    Ok(r)
}

/// float8_mul() with its overflow checks
fn float8_mul(a: f64, b: f64) -> Result<f64> {
    let r = a * b;
    if r.is_infinite() && !a.is_infinite() && !b.is_infinite() {
        return Err(Error::new("22003", "value out of range: overflow"));
    }
    if r == 0.0 && a != 0.0 && b != 0.0 {
        return Err(Error::new("22003", "value out of range: underflow"));
    }
    Ok(r)
}

/// float8_timestamptz(): to_timestamp(double precision)
pub fn to_timestamp_epoch(secs: f64) -> Result<TimestampTz> {
    if secs.is_nan() {
        return Err(Error::new(
            DATETIME_VALUE_OUT_OF_RANGE,
            "timestamp cannot be NaN",
        ));
    }
    if secs.is_infinite() {
        return Ok(if secs < 0.0 { DT_NOBEGIN } else { DT_NOEND });
    }
    let oor = || {
        Error::new(
            DATETIME_VALUE_OUT_OF_RANGE,
            format!("timestamp out of range: \"{}\"", c_format_g(secs, 0, false)),
        )
    };
    if secs < SECS_PER_DAY as f64 * (DATETIME_MIN_JULIAN - UNIX_EPOCH_JDATE) as f64
        || secs >= SECS_PER_DAY as f64 * (TIMESTAMP_END_JULIAN - UNIX_EPOCH_JDATE) as f64
    {
        return Err(oor());
    }
    let s = secs - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) as i64 * SECS_PER_DAY) as f64;
    let s = rint(s * USECS_PER_SEC as f64);
    let result = s as i64;
    if !is_valid_timestamp(result) {
        return Err(oor());
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// AT TIME ZONE
// ---------------------------------------------------------------------------

/// timestamptz_zone(): `timestamptz AT TIME ZONE zone`
pub fn timezone_tstz(zone: &TimeZone, t: TimestampTz) -> Result<Timestamp> {
    if timestamp_not_finite(t) {
        return Ok(t);
    }
    let result = match zone {
        TimeZone::Utc => t,
        TimeZone::Fixed(off) => dt2local(t, off.wrapping_neg()),
        TimeZone::Named(_) => {
            let tt = timestamp2tm_tz(t, &zone.zone_ref())?;
            tm2timestamp(&tt.tm, tt.fsec, None)?
        }
    };
    if !is_valid_timestamp(result) {
        return Err(timestamp_out_of_range());
    }
    Ok(result)
}

/// timestamp_zone(): `timestamp AT TIME ZONE zone`
pub fn timezone_ts(zone: &TimeZone, ts: Timestamp) -> Result<TimestampTz> {
    if timestamp_not_finite(ts) {
        return Ok(ts);
    }
    let result = match zone {
        TimeZone::Utc => ts,
        TimeZone::Fixed(off) => dt2local(ts, *off),
        TimeZone::Named(_) => {
            let z = zone.zone_ref();
            let (mut tm, fsec) = timestamp2tm(ts)?;
            let tzo = tz::determine_offset(&mut tm, &z);
            tm2timestamp(&tm, fsec, Some(tzo))?
        }
    };
    if !is_valid_timestamp(result) {
        return Err(timestamp_out_of_range());
    }
    Ok(result)
}

/// `timestamptz AT TIME ZONE 'name'` with the zone given as text, including
/// PostgreSQL's handling of dynamic abbreviations (e.g. 'MSK').
pub fn timezone_tstz_by_name(zone: &str, t: TimestampTz) -> Result<Timestamp> {
    if timestamp_not_finite(t) {
        return Ok(t);
    }
    let result = match tz::decode_timezone_name(zone)? {
        DecodedTzName::Fixed(off) => dt2local(t, -off),
        DecodedTzName::Dyn(z) => {
            let (tzo, _) = tz::determine_abbrev_offset_ts(t, zone, &z)?;
            dt2local(t, tzo)
        }
        DecodedTzName::Zone(z) => {
            let tt = timestamp2tm_tz(t, &z)?;
            tm2timestamp(&tt.tm, tt.fsec, None)?
        }
    };
    if !is_valid_timestamp(result) {
        return Err(timestamp_out_of_range());
    }
    Ok(result)
}

/// `timestamp AT TIME ZONE 'name'` with the zone given as text.
pub fn timezone_ts_by_name(zone: &str, ts: Timestamp) -> Result<TimestampTz> {
    if timestamp_not_finite(ts) {
        return Ok(ts);
    }
    let result = match tz::decode_timezone_name(zone)? {
        DecodedTzName::Fixed(off) => dt2local(ts, off),
        DecodedTzName::Dyn(z) => {
            let (mut tm, _) = timestamp2tm(ts)?;
            let tzo = -tz::determine_abbrev_offset(&mut tm, zone, &z);
            dt2local(ts, tzo)
        }
        DecodedTzName::Zone(z) => {
            let (mut tm, fsec) = timestamp2tm(ts)?;
            let tzo = tz::determine_offset(&mut tm, &z);
            tm2timestamp(&tm, fsec, Some(tzo))?
        }
    };
    if !is_valid_timestamp(result) {
        return Err(timestamp_out_of_range());
    }
    Ok(result)
}

/// `timestamp AT TIME ZONE interval` (timestamp_izone)
pub fn timezone_ts_interval(zone: Interval, ts: Timestamp) -> Result<TimestampTz> {
    if timestamp_not_finite(ts) {
        return Ok(ts);
    }
    check_interval_zone(&zone)?;
    let tzo = (zone.micros / USECS_PER_SEC) as i32;
    let result = dt2local(ts, tzo);
    if !is_valid_timestamp(result) {
        return Err(timestamp_out_of_range());
    }
    Ok(result)
}

/// `timestamptz AT TIME ZONE interval` (timestamptz_izone)
pub fn timezone_tstz_interval(zone: Interval, t: TimestampTz) -> Result<Timestamp> {
    if timestamp_not_finite(t) {
        return Ok(t);
    }
    check_interval_zone(&zone)?;
    let tzo = -((zone.micros / USECS_PER_SEC) as i32);
    let result = dt2local(t, tzo);
    if !is_valid_timestamp(result) {
        return Err(timestamp_out_of_range());
    }
    Ok(result)
}

fn check_interval_zone(zone: &Interval) -> Result<()> {
    if interval_not_finite(zone) {
        return Err(Error::new(
            INVALID_PARAMETER_VALUE,
            format!(
                "interval time zone \"{}\" must be finite",
                crate::io::format_interval(*zone)
            ),
        ));
    }
    if zone.months != 0 || zone.days != 0 {
        return Err(Error::new(
            INVALID_PARAMETER_VALUE,
            format!(
                "interval time zone \"{}\" must not include months or days",
                crate::io::format_interval(*zone)
            ),
        ));
    }
    Ok(())
}

pub fn isfinite_interval(iv: Interval) -> bool {
    !interval_not_finite(&iv)
}

// ---------------------------------------------------------------------------
// cross-type comparisons (date_cmp_timestamp_internal() etc.)
// ---------------------------------------------------------------------------

/// date2timestamp_opt_overflow(): (value, overflow)
fn date2timestamp_overflow(d: Date) -> (i64, i32) {
    if d == DATEVAL_NOBEGIN {
        return (DT_NOBEGIN, 0);
    }
    if d == DATEVAL_NOEND {
        return (DT_NOEND, 0);
    }
    if d >= TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE {
        return (DT_NOEND, 1);
    }
    ((d as i64).wrapping_mul(USECS_PER_DAY), 0)
}

/// date2timestamptz_opt_overflow(): (value, overflow)
fn date2timestamptz_overflow(d: Date, zone: &ZoneRef) -> (i64, i32) {
    if d == DATEVAL_NOBEGIN {
        return (DT_NOBEGIN, 0);
    }
    if d == DATEVAL_NOEND {
        return (DT_NOEND, 0);
    }
    if d >= TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE {
        return (DT_NOEND, 1);
    }
    let (year, mon, mday) = j2date(d.wrapping_add(POSTGRES_EPOCH_JDATE));
    let mut tm = Tm {
        year,
        mon,
        mday,
        ..Default::default()
    };
    let off = tz::determine_offset(&mut tm, zone);
    let result = (d as i64)
        .wrapping_mul(USECS_PER_DAY)
        .wrapping_add(off as i64 * USECS_PER_SEC);
    if !is_valid_timestamp(result) {
        return if result < MIN_TIMESTAMP {
            (DT_NOBEGIN, -1)
        } else {
            (DT_NOEND, 1)
        };
    }
    (result, 0)
}

/// timestamp2timestamptz_opt_overflow(): (value, overflow)
fn timestamp2timestamptz_overflow(ts: Timestamp, zone: &ZoneRef) -> (i64, i32) {
    if timestamp_not_finite(ts) {
        return (ts, 0);
    }
    let Ok((mut tm, _)) = timestamp2tm(ts) else {
        return (ts, 0);
    };
    let off = tz::determine_offset(&mut tm, zone);
    let result = dt2local(ts, off.wrapping_neg());
    if is_valid_timestamp(result) {
        (result, 0)
    } else if result < MIN_TIMESTAMP {
        (DT_NOBEGIN, -1)
    } else {
        (DT_NOEND, 1)
    }
}

fn cmp_with_overflow(v: i64, overflow: i32, other: i64) -> Ordering {
    if overflow > 0 {
        // larger than any finite timestamp, but less than infinity
        return if other == DT_NOEND {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }
    if overflow < 0 {
        return if other == DT_NOBEGIN {
            Ordering::Greater
        } else {
            Ordering::Less
        };
    }
    v.cmp(&other)
}

/// `date <op> timestamp` (date_cmp_timestamp_internal): dates beyond the
/// timestamp range compare as larger than any finite timestamp.
pub fn date_cmp_timestamp(d: Date, ts: Timestamp) -> Ordering {
    let (v, o) = date2timestamp_overflow(d);
    cmp_with_overflow(v, o, ts)
}

/// `date <op> timestamptz` (date_cmp_timestamptz_internal); the date is
/// midnight local time in `tz` (the session time zone).
pub fn date_cmp_timestamptz(d: Date, t: TimestampTz, tz: &TimeZone) -> Ordering {
    let (v, o) = date2timestamptz_overflow(d, &tz.zone_ref());
    cmp_with_overflow(v, o, t)
}

/// `timestamp <op> timestamptz` (timestamp_cmp_timestamptz_internal); the
/// timestamp is local time in `tz` (the session time zone).
pub fn timestamp_cmp_timestamptz(ts: Timestamp, t: TimestampTz, tz: &TimeZone) -> Ordering {
    let (v, o) = timestamp2timestamptz_overflow(ts, &tz.zone_ref());
    cmp_with_overflow(v, o, t)
}

/// The SQL OVERLAPS operator with PostgreSQL's NULL handling
/// (overlaps_timestamp / overlaps_time); `None` is SQL NULL.  Works for any
/// of the integer-encoded types (timestamp, timestamptz, time).
pub fn overlaps<T: Ord + Copy>(
    ts1: Option<T>,
    te1: Option<T>,
    ts2: Option<T>,
    te2: Option<T>,
) -> Option<bool> {
    let ordered = |start, end| Some(match (start, end) {
        (None, None) => return None,
        (None, Some(e)) => (e, None),
        (Some(s), None) => (s, None),
        (Some(s), Some(e)) if s > e => (e, Some(s)),
        (Some(s), Some(e)) => (s, Some(e)),
    });
    let (ts1, te1) = ordered(ts1, te1)?;
    let (ts2, te2) = ordered(ts2, te2)?;
    if ts1 > ts2 {
        let te2 = te2?;
        if ts1 < te2 {
            return Some(true);
        }
        te1?;
        Some(false)
    } else if ts1 < ts2 {
        let te1 = te1?;
        if ts2 < te1 {
            return Some(true);
        }
        te2?;
        Some(false)
    } else {
        if te1.is_none() || te2.is_none() {
            return None;
        }
        Some(true)
    }
}

// ---------------------------------------------------------------------------
// interval aggregates
// ---------------------------------------------------------------------------

/// Transition state of PostgreSQL 17's sum(interval) / avg(interval)
/// (IntervalAggState), including the inverse transition used for moving
/// aggregates.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IntervalAggState {
    /// count of finite intervals
    pub n: i64,
    /// sum of finite intervals
    pub sum: Interval,
    /// count of +infinity intervals
    pub pinf: i64,
    /// count of -infinity intervals
    pub ninf: i64,
}

impl IntervalAggState {
    pub fn new() -> Self {
        Self::default()
    }

    /// do_interval_accum()
    pub fn accum(&mut self, v: Interval) -> Result<()> {
        if interval_is_nobegin(&v) {
            self.ninf += 1;
            return Ok(());
        }
        if interval_is_noend(&v) {
            self.pinf += 1;
            return Ok(());
        }
        self.sum = finite_interval_pl(&self.sum, &v)?;
        self.n += 1;
        Ok(())
    }

    /// do_interval_discard() (inverse transition)
    pub fn discard(&mut self, v: Interval) -> Result<()> {
        if interval_is_nobegin(&v) {
            self.ninf -= 1;
            return Ok(());
        }
        if interval_is_noend(&v) {
            self.pinf -= 1;
            return Ok(());
        }
        self.n -= 1;
        if self.n > 0 {
            self.sum = finite_interval_mi(&self.sum, &v)?;
        } else {
            self.sum = Interval::default();
        }
        Ok(())
    }

    /// interval_avg_combine()
    pub fn combine(&mut self, other: &IntervalAggState) -> Result<()> {
        self.n += other.n;
        self.pinf += other.pinf;
        self.ninf += other.ninf;
        if other.n > 0 {
            self.sum = finite_interval_pl(&self.sum, &other.sum)?;
        }
        Ok(())
    }

    fn total(&self) -> i64 {
        self.n + self.pinf + self.ninf
    }

    /// interval_sum(): None when there were no non-null inputs.
    pub fn sum(&self) -> Result<Option<Interval>> {
        if self.total() == 0 {
            return Ok(None);
        }
        if self.pinf > 0 && self.ninf > 0 {
            return Err(interval_out_of_range());
        }
        Ok(Some(if self.pinf > 0 {
            INTERVAL_NOEND
        } else if self.ninf > 0 {
            INTERVAL_NOBEGIN
        } else {
            self.sum
        }))
    }

    /// interval_avg(): None when there were no non-null inputs.
    pub fn avg(&self) -> Result<Option<Interval>> {
        if self.total() == 0 {
            return Ok(None);
        }
        if self.pinf > 0 || self.ninf > 0 {
            if self.pinf > 0 && self.ninf > 0 {
                return Err(interval_out_of_range());
            }
            return Ok(Some(if self.pinf > 0 {
                INTERVAL_NOEND
            } else {
                INTERVAL_NOBEGIN
            }));
        }
        interval_div(self.sum, self.n as f64).map(Some)
    }
}
