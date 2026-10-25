//! Datetime items of jsonpath: `.datetime()`, `.datetime(template)`,
//! `.date()`, `.time()`, `.time_tz()`, `.timestamp()`, `.timestamp_tz()`,
//! their comparisons and their JSON output. Ported from
//! executeDateTimeMethod() and compareDatetime() (jsonpath_exec.c),
//! JsonEncodeDateTime() (json.c) and the date/time support routines of
//! PostgreSQL 17 (Copyright (c) 1996-2024, PostgreSQL Global Development
//! Group; PostgreSQL License). Conversions that involve the session time
//! zone use the `pgdatetime` crate.

use super::ast::{DtMethod, Kind};
use super::dtformat::{self, POSTGRES_EPOCH_JDATE, Parsed, Template, USECS_PER_DAY, USECS_PER_SEC};
use crate::json::JsonNumber;
use crate::{Error, Result, sqlstate};
use pgdatetime::TimeZone;
use std::cmp::Ordering;
use std::sync::OnceLock;

/// The session settings PostgreSQL consults for datetime items: the
/// `TimeZone` setting, and the current date (transaction start) that
/// PostgreSQL uses when it casts `time` to `time with time zone`.
#[derive(Clone, Debug)]
pub struct SessionTz {
    pub time_zone: TimeZone,
    /// days since 2000-01-01
    pub current_date: pgdatetime::Date,
}

impl Default for SessionTz {
    /// `TimeZone = 'UTC'`, current date 2000-01-01.
    fn default() -> SessionTz {
        SessionTz {
            time_zone: TimeZone::Utc,
            current_date: 0,
        }
    }
}

impl SessionTz {
    pub fn new(time_zone: TimeZone, current_date: pgdatetime::Date) -> SessionTz {
        SessionTz {
            time_zone,
            current_date,
        }
    }
}

/// The method being executed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Target {
    Datetime,
    Date,
    Time,
    TimeTz,
    Timestamp,
    TimestampTz,
}

impl Target {
    pub fn from_dt(m: DtMethod) -> Target {
        match m {
            DtMethod::Datetime => Target::Datetime,
            DtMethod::Time => Target::Time,
            DtMethod::TimeTz => Target::TimeTz,
            DtMethod::Timestamp => Target::Timestamp,
            DtMethod::TimestampTz => Target::TimestampTz,
        }
    }
}

/// A datetime value (JsonbValue of type jbvDatetime).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DtVal {
    Date(i32),
    Time(i64),
    /// time, zone (seconds west of UTC)
    TimeTz(i64, i32),
    Timestamp(i64),
    TimestampTz(i64),
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct DateTime {
    pub v: DtVal,
    /// the time zone offset (seconds west) a timestamptz is displayed with
    pub tz: i32,
}

fn dt_error(e: pgdatetime::Error) -> Error {
    Error {
        sqlstate: e.sqlstate,
        message: e.message,
        detail: e.detail,
    }
}

/// time2tm() / dt2time(): (hour, min, sec, fsec)
fn time2tm(t: i64) -> (i64, i64, i64, i64) {
    let hour = t / 3_600_000_000;
    let t = t - hour * 3_600_000_000;
    let min = t / 60_000_000;
    let t = t - min * 60_000_000;
    let sec = t / USECS_PER_SEC;
    (hour, min, sec, t - sec * USECS_PER_SEC)
}

/// Zero-padded decimal (pg_ultostr_zeropad).
fn pad(out: &mut String, v: i64, width: usize) {
    out.push_str(&format!("{v:0width$}"));
}

/// AppendSeconds() with precision 6 and zero filling.
fn append_seconds(out: &mut String, sec: i64, fsec: i64) {
    pad(out, sec.abs(), 2);
    if fsec != 0 {
        let mut digits = format!("{:06}", fsec.abs());
        while digits.ends_with('0') {
            digits.pop();
        }
        out.push('.');
        out.push_str(&digits);
    }
}

/// EncodeTimezone() in XSD style; `tz` is seconds west.
fn encode_timezone(out: &mut String, tz: i32) {
    let mut sec = tz.unsigned_abs() as i64;
    let mut min = sec / 60;
    sec -= min * 60;
    let hour = min / 60;
    min -= hour * 60;
    out.push(if tz <= 0 { '+' } else { '-' });
    pad(out, hour, 2);
    out.push(':');
    pad(out, min, 2);
    if sec != 0 {
        out.push(':');
        pad(out, sec, 2);
    }
}

fn push_year(out: &mut String, y: i32) {
    let yy = if y > 0 {
        i64::from(y)
    } else {
        -(i64::from(y) - 1)
    };
    pad(out, yy, 4);
}

/// timestamp2tm() without a time zone: None if out of range.
fn timestamp2tm(dt: i64) -> Option<(i32, i32, i32, i64, i64, i64, i64)> {
    let mut date = dt / USECS_PER_DAY;
    let mut time = dt - date * USECS_PER_DAY;
    if time < 0 {
        time += USECS_PER_DAY;
        date -= 1;
    }
    date += i64::from(POSTGRES_EPOCH_JDATE);
    if date < 0 || date > i64::from(i32::MAX) {
        return None;
    }
    let (y, m, d) = dtformat::j2date(date as i32);
    let (hh, mi, ss, fs) = time2tm(time);
    Some((y, m, d, hh, mi, ss, fs))
}

/// EncodeDateTime() in XSD style.
fn encode_datetime(ts: i64, tz: Option<i32>) -> Result<String> {
    let Some((y, m, d, hh, mi, ss, fs)) = timestamp2tm(ts) else {
        return Err(Error::new(
            sqlstate::DATETIME_VALUE_OUT_OF_RANGE,
            "timestamp out of range",
        ));
    };
    let mut out = String::with_capacity(32);
    push_year(&mut out, y);
    out.push('-');
    pad(&mut out, i64::from(m), 2);
    out.push('-');
    pad(&mut out, i64::from(d), 2);
    out.push('T');
    pad(&mut out, hh, 2);
    out.push(':');
    pad(&mut out, mi, 2);
    out.push(':');
    append_seconds(&mut out, ss, fs);
    if let Some(tz) = tz {
        encode_timezone(&mut out, tz);
    }
    if y <= 0 {
        out.push_str(" BC");
    }
    Ok(out)
}

fn encode_time(t: i64, tz: Option<i32>) -> String {
    let (hh, mi, ss, fs) = time2tm(t);
    let mut out = String::with_capacity(24);
    pad(&mut out, hh, 2);
    out.push(':');
    pad(&mut out, mi, 2);
    out.push(':');
    append_seconds(&mut out, ss, fs);
    if let Some(tz) = tz {
        encode_timezone(&mut out, tz);
    }
    out
}

impl DateTime {
    /// JsonbTypeName()
    pub fn type_name(&self) -> &'static str {
        match self.v {
            DtVal::Date(_) => "date",
            DtVal::Time(_) => "time without time zone",
            DtVal::TimeTz(..) => "time with time zone",
            DtVal::Timestamp(_) => "timestamp without time zone",
            DtVal::TimestampTz(_) => "timestamp with time zone",
        }
    }

    /// JsonEncodeDateTime() with the item's time zone.
    pub fn to_json_string(self) -> Result<String> {
        Ok(match self.v {
            DtVal::Date(d) => {
                if d == i32::MIN {
                    return Ok("-infinity".to_owned());
                }
                if d == i32::MAX {
                    return Ok("infinity".to_owned());
                }
                let (y, m, dd) = dtformat::j2date(d.wrapping_add(POSTGRES_EPOCH_JDATE));
                let mut out = String::with_capacity(16);
                push_year(&mut out, y);
                out.push('-');
                pad(&mut out, i64::from(m), 2);
                out.push('-');
                pad(&mut out, i64::from(dd), 2);
                if y <= 0 {
                    out.push_str(" BC");
                }
                out
            }
            DtVal::Time(t) => encode_time(t, None),
            DtVal::TimeTz(t, z) => encode_time(t, Some(z)),
            DtVal::Timestamp(ts) => {
                if ts == i64::MIN {
                    return Ok("-infinity".to_owned());
                }
                if ts == i64::MAX {
                    return Ok("infinity".to_owned());
                }
                encode_datetime(ts, None)?
            }
            DtVal::TimestampTz(ts) => {
                let shifted = ts.wrapping_sub(i64::from(self.tz) * USECS_PER_SEC);
                if shifted == i64::MIN {
                    return Ok("-infinity".to_owned());
                }
                if shifted == i64::MAX {
                    return Ok("infinity".to_owned());
                }
                encode_datetime(shifted, Some(self.tz))?
            }
        })
    }
}

/// checkTimezoneIsUsedForCast(): always raised, even in silent mode.
fn check_tz(use_tz: bool, from: &str, to: &str) -> Result<()> {
    if !use_tz {
        return Err(Error::new(
            sqlstate::FEATURE_NOT_SUPPORTED,
            format!("cannot convert value from {from} to {to} without time zone usage"),
        ));
    }
    Ok(())
}

/// Offset (seconds west) of the session time zone at local timestamp `ts`
/// (DetermineTimeZoneOffset()).
fn local_offset(ts: i64, tz: &SessionTz) -> Result<i32> {
    let utc = pgdatetime::timestamp_to_timestamptz(ts, &tz.time_zone).map_err(dt_error)?;
    Ok(((utc - ts) / USECS_PER_SEC) as i32)
}

/// time_timetz(): the zone is the session offset on the current date.
fn time_timetz(t: i64, tz: &SessionTz) -> Result<(i64, i32)> {
    let day = pgdatetime::date_to_timestamp(tz.current_date).map_err(dt_error)?;
    Ok((t, local_offset(day + t, tz)?))
}

/// timestamptz_timetz(): local time of day and offset in the session zone.
fn timestamptz_timetz(t: i64, tz: &SessionTz) -> Result<(i64, i32)> {
    let local = pgdatetime::timestamptz_to_timestamp(t, &tz.time_zone).map_err(dt_error)?;
    let zone = ((t - local) / USECS_PER_SEC) as i32;
    Ok((local.rem_euclid(USECS_PER_DAY), zone))
}

/// timetz_cmp_internal()
fn timetz_cmp(a: (i64, i32), b: (i64, i32)) -> Ordering {
    let t1 = a.0 + i64::from(a.1) * USECS_PER_SEC;
    let t2 = b.0 + i64::from(b.1) * USECS_PER_SEC;
    t1.cmp(&t2).then(a.1.cmp(&b.1))
}

/// compareDatetime(): None if the types are not comparable.
pub(crate) fn compare(
    a: &DateTime,
    b: &DateTime,
    use_tz: bool,
    tz: &SessionTz,
) -> Result<Option<i32>> {
    use DtVal::*;
    let zone = &tz.time_zone;
    let c = match (a.v, b.v) {
        (Date(x), Date(y)) => x.cmp(&y),
        (Date(x), Timestamp(y)) => pgdatetime::date_cmp_timestamp(x, y),
        (Date(x), TimestampTz(y)) => {
            check_tz(use_tz, "date", "timestamptz")?;
            pgdatetime::date_cmp_timestamptz(x, y, zone)
        }
        (Time(x), Time(y)) => x.cmp(&y),
        (Time(x), TimeTz(y, z)) => {
            check_tz(use_tz, "time", "timetz")?;
            timetz_cmp(time_timetz(x, tz)?, (y, z))
        }
        (TimeTz(x, xz), Time(y)) => {
            check_tz(use_tz, "time", "timetz")?;
            timetz_cmp((x, xz), time_timetz(y, tz)?)
        }
        (TimeTz(x, xz), TimeTz(y, yz)) => timetz_cmp((x, xz), (y, yz)),
        (Timestamp(x), Date(y)) => pgdatetime::date_cmp_timestamp(y, x).reverse(),
        (Timestamp(x), Timestamp(y)) => x.cmp(&y),
        (Timestamp(x), TimestampTz(y)) => {
            check_tz(use_tz, "timestamp", "timestamptz")?;
            pgdatetime::timestamp_cmp_timestamptz(x, y, zone)
        }
        (TimestampTz(x), Date(y)) => {
            check_tz(use_tz, "date", "timestamptz")?;
            pgdatetime::date_cmp_timestamptz(y, x, zone).reverse()
        }
        (TimestampTz(x), Timestamp(y)) => {
            check_tz(use_tz, "timestamp", "timestamptz")?;
            pgdatetime::timestamp_cmp_timestamptz(y, x, zone).reverse()
        }
        (TimestampTz(x), TimestampTz(y)) => x.cmp(&y),
        // uncomparable types
        _ => return Ok(None),
    };
    Ok(Some(c as i32))
}

/// The ISO formats tried by `.datetime()` and friends without a template.
fn iso_templates() -> &'static [Template] {
    static T: OnceLock<Vec<Template>> = OnceLock::new();
    T.get_or_init(|| {
        [
            "yyyy-mm-dd",
            "HH24:MI:SS.USTZ",
            "HH24:MI:SSTZ",
            "HH24:MI:SS.US",
            "HH24:MI:SS",
            "yyyy-mm-dd HH24:MI:SS.USTZ",
            "yyyy-mm-dd HH24:MI:SSTZ",
            "yyyy-mm-dd\"T\"HH24:MI:SS.USTZ",
            "yyyy-mm-dd\"T\"HH24:MI:SSTZ",
            "yyyy-mm-dd HH24:MI:SS.US",
            "yyyy-mm-dd HH24:MI:SS",
            "yyyy-mm-dd\"T\"HH24:MI:SS.US",
            "yyyy-mm-dd\"T\"HH24:MI:SS",
        ]
        .iter()
        .map(|f| Template::parse(f).expect("valid ISO template"))
        .collect()
    })
}

/// Cache of user templates (PostgreSQL keeps a similar DCH cache).
fn cached_template(fmt: &str) -> Result<std::sync::Arc<Template>> {
    use std::sync::{Arc, Mutex};
    const ENTRIES: usize = 32;
    static CACHE: Mutex<Vec<(String, Arc<Template>)>> = Mutex::new(Vec::new());
    {
        let mut c = CACHE.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(i) = c.iter().position(|(f, _)| f == fmt) {
            let e = c.remove(i);
            let t = e.1.clone();
            c.insert(0, e);
            return Ok(t);
        }
    }
    let t = Arc::new(Template::parse(fmt)?);
    let mut c = CACHE.lock().unwrap_or_else(|e| e.into_inner());
    c.insert(0, (fmt.to_owned(), t.clone()));
    c.truncate(ENTRIES);
    Ok(t)
}

fn not_recognized(what: &str, s: &str) -> Error {
    Error::new(
        sqlstate::INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION,
        format!("{what} format is not recognized: \"{s}\""),
    )
}

/// The part of executeDateTimeMethod() after the string check: Ok(Ok(v)) on
/// success, Ok(Err(e)) for errors that silent mode suppresses, Err(e) for
/// errors that are always raised.
pub(crate) fn execute(
    s: &str,
    target: Target,
    name: &str,
    arg: Option<&Kind>,
    use_tz: bool,
    tz: &SessionTz,
) -> Result<std::result::Result<DateTime, Error>> {
    let mut time_precision: i32 = -1;
    let (parsed, mut zone) = if target == Target::Datetime
        && let Some(arg) = arg
    {
        let Kind::String(template) = arg else {
            return Err(Error::new(
                sqlstate::INTERNAL_ERROR,
                "invalid jsonpath item type for .datetime() argument",
            ));
        };
        let t = cached_template(template)?;
        let p = match dtformat::parse_datetime(s, &t) {
            Ok((p, _)) => p,
            Err(e) => return Ok(Err(e)),
        };
        (p, parsed_zone(p))
    } else {
        // Optional precision for methods other than .datetime() and .date()
        if target != Target::Datetime
            && target != Target::Date
            && let Some(arg) = arg
        {
            let Kind::Numeric(n) = arg else {
                return Err(Error::new(
                    sqlstate::INTERNAL_ERROR,
                    format!("invalid jsonpath item type for {name} argument"),
                ));
            };
            match numeric_int4(n) {
                Some(p) => time_precision = p,
                None => {
                    return Ok(Err(Error::new(
                        sqlstate::INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION,
                        format!(
                            "time precision of jsonpath item method .{name}() is out of range for type integer"
                        ),
                    )));
                }
            }
        }
        let found = iso_templates()
            .iter()
            .find_map(|t| dtformat::parse_datetime(s, t).ok());
        match found {
            Some((p, _)) => (p, parsed_zone(p)),
            None => {
                let what = if target == Target::Datetime {
                    "datetime"
                } else {
                    name
                };
                return Ok(Err(not_recognized(what, s)));
            }
        }
    };
    let z = &tz.time_zone;
    let v = match target {
        Target::Datetime => to_val(parsed),
        Target::Date => match parsed {
            Parsed::Date(d) => DtVal::Date(d),
            Parsed::Time(_) | Parsed::TimeTz(..) => return Ok(Err(not_recognized("date", s))),
            Parsed::Timestamp(ts) => {
                DtVal::Date(pgdatetime::timestamp_to_date(ts).map_err(dt_error)?)
            }
            Parsed::TimestampTz(t, _) => {
                check_tz(use_tz, "timestamptz", "date")?;
                DtVal::Date(pgdatetime::timestamptz_to_date(t, z).map_err(dt_error)?)
            }
        },
        Target::Time => {
            let t = match parsed {
                Parsed::Date(_) => return Ok(Err(not_recognized("time", s))),
                Parsed::Time(t) => t,
                Parsed::TimeTz(t, _) => {
                    check_tz(use_tz, "timetz", "time")?;
                    t
                }
                Parsed::Timestamp(ts) => pgdatetime::timestamp_to_time(ts).map_err(dt_error)?,
                Parsed::TimestampTz(t, _) => {
                    check_tz(use_tz, "timestamptz", "time")?;
                    pgdatetime::timestamptz_to_time(t, z).map_err(dt_error)?
                }
            };
            // Force the user-given time precision, if any (larger values
            // are reduced to 6 with a warning in PostgreSQL)
            DtVal::Time(if time_precision != -1 {
                dtformat::adjust_time(t, time_precision.min(6))
            } else {
                t
            })
        }
        Target::TimeTz => {
            let (t, tzone) = match parsed {
                Parsed::Date(_) | Parsed::Timestamp(_) => {
                    return Ok(Err(not_recognized("time_tz", s)));
                }
                Parsed::Time(t) => {
                    check_tz(use_tz, "time", "timetz")?;
                    time_timetz(t, tz)?
                }
                Parsed::TimeTz(t, tzone) => (t, tzone),
                Parsed::TimestampTz(t, _) => timestamptz_timetz(t, tz)?,
            };
            DtVal::TimeTz(
                if time_precision != -1 {
                    dtformat::adjust_time(t, time_precision.min(6))
                } else {
                    t
                },
                tzone,
            )
        }
        Target::Timestamp => {
            let ts = match parsed {
                Parsed::Date(d) => pgdatetime::date_to_timestamp(d).map_err(dt_error)?,
                Parsed::Time(_) | Parsed::TimeTz(..) => {
                    return Ok(Err(not_recognized("timestamp", s)));
                }
                Parsed::Timestamp(ts) => ts,
                Parsed::TimestampTz(t, _) => {
                    check_tz(use_tz, "timestamptz", "timestamp")?;
                    pgdatetime::timestamptz_to_timestamp(t, z).map_err(dt_error)?
                }
            };
            DtVal::Timestamp(if time_precision != -1 {
                dtformat::adjust_timestamp(ts, time_precision.min(6))
            } else {
                ts
            })
        }
        Target::TimestampTz => {
            let t = match parsed {
                Parsed::Date(d) => {
                    check_tz(use_tz, "date", "timestamptz")?;
                    let local = pgdatetime::date_to_timestamp(d).map_err(dt_error)?;
                    zone = local_offset(local, tz)?;
                    pgdatetime::date_to_timestamptz(d, z).map_err(dt_error)?
                }
                Parsed::Time(_) | Parsed::TimeTz(..) => {
                    return Ok(Err(not_recognized("timestamp_tz", s)));
                }
                Parsed::Timestamp(ts) => {
                    check_tz(use_tz, "timestamp", "timestamptz")?;
                    zone = local_offset(ts, tz)?;
                    pgdatetime::timestamp_to_timestamptz(ts, z).map_err(dt_error)?
                }
                Parsed::TimestampTz(t, _) => t,
            };
            DtVal::TimestampTz(if time_precision != -1 {
                dtformat::adjust_timestamp(t, time_precision.min(6))
            } else {
                t
            })
        }
    };
    Ok(Ok(DateTime { v, tz: zone }))
}

fn parsed_zone(p: Parsed) -> i32 {
    match p {
        Parsed::TimeTz(_, z) | Parsed::TimestampTz(_, z) => z,
        _ => 0,
    }
}

fn to_val(p: Parsed) -> DtVal {
    match p {
        Parsed::Date(d) => DtVal::Date(d),
        Parsed::Time(t) => DtVal::Time(t),
        Parsed::TimeTz(t, z) => DtVal::TimeTz(t, z),
        Parsed::Timestamp(t) => DtVal::Timestamp(t),
        Parsed::TimestampTz(t, _) => DtVal::TimestampTz(t),
    }
}

/// numeric_int4_opt_error()
fn numeric_int4(n: &JsonNumber) -> Option<i32> {
    pgnumeric::Numeric::parse(n.as_str()).ok()?.to_i32().ok()
}
