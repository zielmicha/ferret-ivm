//! pgdatetime: PostgreSQL 17 compatible date/time types and functions.
//!
//! Semantics follow PostgreSQL 17 exactly for the session settings
//! `DateStyle = 'ISO, MDY'`, `IntervalStyle = 'postgres'`,
//! `timezone_abbreviations = 'Default'`, `lc_time = 'C'`; the session time
//! zone is passed explicitly as a [`TimeZone`].
//!
//! Large parts of this crate are ports of PostgreSQL's
//! `src/backend/utils/adt/{datetime.c,date.c,timestamp.c,formatting.c}` and
//! `src/timezone/localtime.c` (PostgreSQL License: Portions Copyright (c)
//! 1996-2024, PostgreSQL Global Development Group; Portions Copyright (c)
//! 1994, Regents of the University of California; localtime.c is in the
//! public domain).  Time zone data: the IANA tz database (public domain),
//! embedded from the compiled zoneinfo that PostgreSQL uses (see
//! `tools/gen_tzdata.py` and [`tzdata_version`]).
//!
//! # Conventions
//!
//! * Functions that can return SQL NULL in PostgreSQL (EXTRACT/date_part of
//!   some units of infinite values, to_char of infinite values or with an
//!   empty format, timestamp → time of infinite values) have `_opt` variants
//!   returning `Option`; the plain variants return an error for which
//!   [`Error::is_null_result`] is true (SQLSTATE [`SQLSTATE_NULL_RESULT`]).
//! * Numeric infinities from EXTRACT are [`Decimal::INFINITY`] /
//!   [`Decimal::NEG_INFINITY`] (Display prints `Infinity` / `-Infinity`).
//! * [`Interval`]'s derived `PartialEq`/`Hash` compare raw fields; SQL equality
//!   and hashing use [`interval_cmp`] / [`interval_cmp_value`]
//!   (`'1 mon' = '30 days'`).
//! * Arithmetic follows PostgreSQL's `-fwrapv` behaviour where PostgreSQL does
//!   not check for overflow; no function panics on any input.
//! * [`date_to_ymd`]/[`ymd_to_date`] use PostgreSQL's internal year numbering
//!   (0 = 1 BC, -1 = 2 BC, ...).
//!
//! # Time zones
//!
//! * [`TimeZone::parse`] resolves a zone like `AT TIME ZONE`,
//!   `date_trunc(..., zone)` and `timestamptz + interval` at a zone do
//!   (DecodeTimezoneName): abbreviation table first (`'PST'`, `'CET'`, `'UTC'`
//!   are fixed offsets), then IANA names (case-insensitive), then POSIX TZ
//!   strings (`'+05'`, `'-5'`, `'UTC+3'` use the POSIX sign convention:
//!   positive = west of Greenwich).
//! * [`TimeZone::parse_session`] resolves a zone like `SET TIME ZONE` /
//!   the `TimeZone` setting: plain numbers are hours east (`'-5'` is UTC-5),
//!   `INTERVAL '+05:30'` is accepted, abbreviations are not.
//! * Zone data is the tz database compiled into PostgreSQL's zoneinfo
//!   directory at build time of this crate (see [`tzdata_version`]), not
//!   chrono-tz's bundled tables; `chrono_tz::Tz` only identifies the zone.
//!   Transitions beyond the explicit table follow the POSIX TZ rule exactly
//!   like PostgreSQL (e.g. DST in 2300).
//!
//! # Extras beyond the core API
//!
//! `_opt` variants; [`parse_interval_typmod`] / [`adjust_interval_typmod`]
//! (`INTERVAL DAY TO SECOND(3)` etc.) and their raw-typmod forms
//! [`parse_interval_raw_typmod`] / [`adjust_interval_raw_typmod`] /
//! [`interval_typmod_decode`] (`atttypmod` values); [`format_interval_style`]
//! (all IntervalStyles); [`make_timestamptz_at_zone`] (7-argument form);
//! [`date_trunc_timestamptz_at_zone`]; [`timezone_tstz_by_name`] /
//! [`timezone_ts_by_name`] (exact `AT TIME ZONE 'text'`, including dynamic
//! abbreviations such as `'MSK'`); [`timezone_ts_interval`] /
//! [`timezone_tstz_interval`] (`AT TIME ZONE interval`); [`date_part_date`];
//! cross-type comparisons [`date_cmp_timestamp`], [`date_cmp_timestamptz`],
//! [`timestamp_cmp_timestamptz`] (out-of-range values compare instead of
//! failing, as in PostgreSQL); [`overlaps`]; [`IntervalAggState`]
//! (sum/avg(interval) with inverse transition); `_checked` variants of
//! [`interval_to_time`], [`time_pl_interval`], [`time_mi_interval`] that
//! raise PostgreSQL's errors for infinite intervals.
//!
//! # Known deviations
//!
//! * `TimeZone::parse` maps dynamic abbreviations (`MSK`, `ART`, ... — whose
//!   offset depends on the date) to their underlying zone, because
//!   [`TimeZone`] cannot represent them; use the `*_by_name` functions for
//!   exact `AT TIME ZONE` semantics.  Timestamp input parsing handles them
//!   exactly.
//! * `TimeZone::parse`/`parse_session` reject POSIX TZ strings with DST rules
//!   that are not zone names (e.g. `'XYZ3ABC,M3.2.0,M11.1.0'`); such strings
//!   inside timestamptz input text work.  The machine-specific zone name
//!   `localtime` is not available; `Factory` becomes a fixed +00 offset.
//! * Only DateStyle ISO/MDY input and output; `time with time zone` is not
//!   implemented.

#![allow(clippy::too_many_arguments)]
// The ports keep PostgreSQL's C structure (explicit range checks, index loops)
// to make them easy to compare with the original.
#![allow(
    clippy::manual_range_contains,
    clippy::needless_range_loop,
    clippy::needless_late_init
)]

mod arith;
mod core;
mod datetime;
mod decimal;
mod encode;
pub(crate) mod error;
mod fields;
mod formatting;
mod io;
mod tz;

pub use chrono_tz;

pub use arith::{
    IntervalAggState, age, age_tz, date_bin, date_cmp_timestamp, date_cmp_timestamptz,
    date_mi_date, date_mi_days, date_mi_interval, date_pl_days, date_pl_interval, date_pl_time,
    date_to_timestamp, date_to_timestamptz, interval_cmp, interval_cmp_value, interval_div,
    interval_hash_key, interval_mi, interval_mul, interval_pl, interval_to_time,
    interval_to_time_checked, interval_um, isfinite_interval, justify_days, justify_hours,
    justify_interval, make_date, make_interval, make_time, make_timestamp, make_timestamptz,
    make_timestamptz_at_zone, overlaps, time_mi_interval, time_mi_interval_checked, time_mi_time,
    time_pl_interval, time_pl_interval_checked, time_to_interval, timestamp_cmp_timestamptz,
    timestamp_mi, timestamp_mi_interval, timestamp_pl_interval, timestamp_to_date,
    timestamp_to_time, timestamp_to_time_opt, timestamp_to_timestamptz, timestamptz_mi,
    timestamptz_mi_interval, timestamptz_pl_interval, timestamptz_to_date, timestamptz_to_time,
    timestamptz_to_time_opt, timestamptz_to_timestamp, timezone_ts, timezone_ts_by_name,
    timezone_ts_interval, timezone_tstz, timezone_tstz_by_name, timezone_tstz_interval,
    to_timestamp_epoch,
};
pub use encode::IntervalStyle;
pub use error::SQLSTATE_NULL_RESULT;
pub use fields::{
    date_part_date, date_part_date_opt, date_part_interval, date_part_interval_opt, date_part_time,
    date_part_time_opt, date_part_timestamp, date_part_timestamp_opt, date_part_timestamptz,
    date_part_timestamptz_opt, date_trunc_interval, date_trunc_timestamp, date_trunc_timestamptz,
    date_trunc_timestamptz_at_zone, extract_date, extract_date_opt, extract_interval,
    extract_interval_opt, extract_time, extract_time_opt, extract_timestamp, extract_timestamp_opt,
    extract_timestamptz, extract_timestamptz_opt,
};
pub use formatting::{
    DateTimeTemplate, ParsedDateTime, to_char_interval, to_char_interval_opt, to_char_timestamp,
    to_char_timestamp_opt, to_char_timestamptz, to_char_timestamptz_opt, to_date, to_timestamp,
};
pub use io::{
    IntervalFields, adjust_interval, adjust_interval_raw_typmod, adjust_interval_typmod,
    adjust_time, adjust_timestamp, format_date, format_interval, format_interval_style,
    format_time, format_timestamp, format_timestamptz, interval_typmod_decode, parse_date,
    parse_interval, parse_interval_raw_typmod, parse_interval_typmod, parse_time, parse_timestamp,
    parse_timestamptz,
};
pub use tz::tzdata_version;

/// days since 2000-01-01 (PG DateADT); i32::MIN = -infinity, i32::MAX = infinity
pub type Date = i32;
/// microseconds since midnight, 0..=86_400_000_000
pub type Time = i64;
/// microseconds since 2000-01-01 00:00:00; i64::MIN = -infinity, i64::MAX = infinity
pub type Timestamp = i64;
/// microseconds since 2000-01-01 00:00:00 UTC
pub type TimestampTz = i64;

/// PG17 interval incl. +/-infinity representation (all fields at
/// i32::MIN/i64::MIN resp. i32::MAX/i64::MAX).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}

impl Interval {
    pub const INFINITY: Interval = Interval {
        months: i32::MAX,
        days: i32::MAX,
        micros: i64::MAX,
    };
    pub const NEG_INFINITY: Interval = Interval {
        months: i32::MIN,
        days: i32::MIN,
        micros: i64::MIN,
    };
}

/// value * 10^-scale; EXTRACT's numeric result; Display = PG numeric text.
/// Numeric infinities are represented by [`Decimal::INFINITY`] and
/// [`Decimal::NEG_INFINITY`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Decimal {
    pub value: i128,
    pub scale: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub sqlstate: &'static str,
    pub message: String,
    pub detail: Option<String>,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

/// A time zone.  `Utc` behaves like the zone 'UTC' (abbreviation "UTC");
/// `Fixed` is a fixed offset in seconds EAST of UTC (like
/// `SET TIME ZONE INTERVAL '+05:30'`, abbreviation "+05:30"); `Named` is an
/// IANA zone.  See [`TimeZone::parse`] and [`TimeZone::parse_session`].
#[derive(Clone, Debug, PartialEq)]
pub enum TimeZone {
    Utc,
    /// seconds east of UTC
    Fixed(i32),
    Named(chrono_tz::Tz),
}

// ---------------------------------------------------------------------------
// misc API
// ---------------------------------------------------------------------------

pub fn isfinite_date(d: Date) -> bool {
    d != i32::MIN && d != i32::MAX
}

pub fn isfinite_timestamp(ts: Timestamp) -> bool {
    ts != i64::MIN && ts != i64::MAX
}

/// (year, month, day) of a finite date; years <= 0 are BC (0 = 1 BC).
pub fn date_to_ymd(d: Date) -> (i32, u32, u32) {
    let (y, m, dd) = core::j2date(d.wrapping_add(core::POSTGRES_EPOCH_JDATE));
    (y, m as u32, dd as u32)
}

/// Date from (year, month, day); years <= 0 are BC (0 = 1 BC).
pub fn ymd_to_date(y: i32, m: u32, d: u32) -> Result<Date> {
    core::ymd_to_date_checked(
        y,
        m.min(i32::MAX as u32) as i32,
        d.min(i32::MAX as u32) as i32,
    )
}
