//! Golden tests: expected results recorded from PostgreSQL 17 by
//! tools/gen_golden.py (no PostgreSQL needed at test time).

use pgdatetime::*;
use std::fmt::Write as _;

type R = std::result::Result<Option<String>, Error>;

fn tz_of(s: &str) -> std::result::Result<TimeZone, Error> {
    TimeZone::parse_session(s)
}

/// float8out() with extra_float_digits = 1 (shortest round-trip).
fn float8_out(v: f64) -> String {
    if v.is_nan() {
        return "NaN".into();
    }
    if v.is_infinite() {
        return if v > 0.0 {
            "Infinity".into()
        } else {
            "-Infinity".into()
        };
    }
    if v == 0.0 {
        return if v.is_sign_negative() {
            "-0".into()
        } else {
            "0".into()
        };
    }
    // shortest digits via Rust's exponent formatting
    let e = format!("{:e}", v);
    let (mant, exp) = e.split_once('e').unwrap();
    let exp: i32 = exp.parse().unwrap();
    let neg = mant.starts_with('-');
    let digits: String = mant.chars().filter(|c| c.is_ascii_digit()).collect();
    let olength = digits.len() as i32;
    // decimal point position relative to the start of digits
    let dexp = exp + 1;
    let mut out = String::new();
    if neg {
        out.push('-');
    }
    if -4 < dexp && dexp <= 15 {
        if dexp <= 0 {
            out.push_str("0.");
            for _ in 0..(-dexp) {
                out.push('0');
            }
            out.push_str(&digits);
        } else if dexp >= olength {
            out.push_str(&digits);
            for _ in 0..(dexp - olength) {
                out.push('0');
            }
        } else {
            out.push_str(&digits[..dexp as usize]);
            out.push('.');
            out.push_str(&digits[dexp as usize..]);
        }
    } else {
        out.push_str(&digits[..1]);
        if olength > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        let _ = write!(out, "e{}{:02}", if exp < 0 { '-' } else { '+' }, exp.abs());
    }
    out
}

fn ok(s: String) -> R {
    Ok(Some(s))
}

fn opt<T>(r: std::result::Result<Option<T>, Error>, f: impl Fn(T) -> String) -> R {
    r.map(|o| o.map(f))
}

/// Parse a float8 SQL literal as used by the generator: plain numbers,
/// quoted 'NaN'/'Infinity', or "a/b" divisions.
fn float_arg(s: &str) -> f64 {
    let t = s.trim_matches('\'');
    if let Some((a, b)) = t.split_once('/') {
        return a.parse::<f64>().unwrap() / b.parse::<f64>().unwrap();
    }
    match t {
        "NaN" => f64::NAN,
        "Infinity" => f64::INFINITY,
        "-Infinity" => f64::NEG_INFINITY,
        _ => t.parse().unwrap(),
    }
}

fn int_arg(s: &str) -> i32 {
    s.parse().unwrap()
}

fn eval(func: &str, tzs: &str, a: &[&str]) -> R {
    let tz = || tz_of(tzs);
    let fd = format_date;
    let ft = format_timestamp;
    let fi = format_interval;
    match func {
        "date_in" => ok(fd(parse_date(a[0])?)),
        "time_in" => ok(format_time(parse_time(a[0])?)),
        "timestamp_in" => ok(ft(parse_timestamp(a[0])?)),
        "timestamptz_in" => {
            let z = tz()?;
            ok(format_timestamptz(parse_timestamptz(a[0], &z)?, &z))
        }
        "interval_in" => ok(fi(parse_interval(a[0])?)),
        "interval_out_style" => {
            let style = match a[0] {
                "postgres" => IntervalStyle::Postgres,
                "postgres_verbose" => IntervalStyle::PostgresVerbose,
                "sql_standard" => IntervalStyle::SqlStandard,
                "iso_8601" => IntervalStyle::Iso8601,
                _ => unreachable!(),
            };
            ok(format!(
                "{}{}",
                a[0],
                format_interval_style(parse_interval(a[1])?, style)
            ))
        }
        "time_p" => ok(format_time(adjust_time(parse_time(a[0])?, int_arg(a[1])))),
        "timestamp_p" => ok(ft(adjust_timestamp(parse_timestamp(a[0])?, int_arg(a[1]))?)),
        "timestamptz_p" => {
            let z = tz()?;
            ok(format_timestamptz(
                adjust_timestamp(parse_timestamptz(a[0], &z)?, int_arg(a[1]))?,
                &z,
            ))
        }
        "interval_p" => ok(fi(adjust_interval(parse_interval(a[0])?, int_arg(a[1]))?)),
        "interval_typmod" => {
            let (spec, prec) = match a[1].split_once('(') {
                Some((s, p)) => (s, int_arg(p.trim_end_matches(')'))),
                None => (a[1], -1),
            };
            let fields = match spec {
                "year" => IntervalFields::Year,
                "month" => IntervalFields::Month,
                "day" => IntervalFields::Day,
                "hour" => IntervalFields::Hour,
                "minute" => IntervalFields::Minute,
                "second" => IntervalFields::Second,
                "year to month" => IntervalFields::YearToMonth,
                "day to hour" => IntervalFields::DayToHour,
                "day to minute" => IntervalFields::DayToMinute,
                "day to second" => IntervalFields::DayToSecond,
                "hour to minute" => IntervalFields::HourToMinute,
                "hour to second" => IntervalFields::HourToSecond,
                "minute to second" => IntervalFields::MinuteToSecond,
                _ => unreachable!("{}", spec),
            };
            let r = parse_interval_typmod(a[0], Some(fields), prec);
            // the raw-typmod entry points must agree
            let mask = |b: i32| 1 << b;
            let (y, mo, d, h, mi, s) = (mask(2), mask(1), mask(3), mask(10), mask(11), mask(12));
            let range = match fields {
                IntervalFields::Year => y,
                IntervalFields::Month => mo,
                IntervalFields::Day => d,
                IntervalFields::Hour => h,
                IntervalFields::Minute => mi,
                IntervalFields::Second => s,
                IntervalFields::YearToMonth => y | mo,
                IntervalFields::DayToHour => d | h,
                IntervalFields::DayToMinute => d | h | mi,
                IntervalFields::DayToSecond => d | h | mi | s,
                IntervalFields::HourToMinute => h | mi,
                IntervalFields::HourToSecond => h | mi | s,
                IntervalFields::MinuteToSecond => mi | s,
            };
            let typmod = (range << 16) | if prec < 0 { 0xFFFF } else { prec.min(6) };
            assert_eq!(
                r,
                parse_interval_raw_typmod(a[0], typmod),
                "raw typmod for {:?}",
                a
            );
            if let Ok(v) = parse_interval(a[0]) {
                assert_eq!(
                    adjust_interval_typmod(v, Some(fields), prec),
                    adjust_interval_raw_typmod(v, typmod)
                );
            }
            ok(fi(r?))
        }
        "date_to_timestamp" => ok(ft(date_to_timestamp(parse_date(a[0])?)?)),
        "timestamp_to_date" => ok(fd(timestamp_to_date(parse_timestamp(a[0])?)?)),
        "date_to_timestamptz" => {
            let z = tz()?;
            ok(format_timestamptz(
                date_to_timestamptz(parse_date(a[0])?, &z)?,
                &z,
            ))
        }
        "timestamptz_to_date" => {
            let z = tz()?;
            ok(fd(timestamptz_to_date(parse_timestamptz(a[0], &z)?, &z)?))
        }
        "timestamp_to_timestamptz" => {
            let z = tz()?;
            ok(format_timestamptz(
                timestamp_to_timestamptz(parse_timestamp(a[0])?, &z)?,
                &z,
            ))
        }
        "timestamptz_to_timestamp" => {
            let z = tz()?;
            ok(ft(timestamptz_to_timestamp(
                parse_timestamptz(a[0], &z)?,
                &z,
            )?))
        }
        "timestamp_to_time" => opt(timestamp_to_time_opt(parse_timestamp(a[0])?), format_time),
        "timestamptz_to_time" => {
            let z = tz()?;
            opt(
                timestamptz_to_time_opt(parse_timestamptz(a[0], &z)?, &z),
                format_time,
            )
        }
        "time_to_interval" => ok(fi(time_to_interval(parse_time(a[0])?))),
        "interval_to_time" => ok(format_time(interval_to_time_checked(parse_interval(
            a[0],
        )?)?)),
        "date_pl_days" => ok(fd(date_pl_days(parse_date(a[0])?, int_arg(a[1]))?)),
        "date_mi_days" => ok(fd(date_mi_days(parse_date(a[0])?, int_arg(a[1]))?)),
        "date_mi_date" => ok(date_mi_date(parse_date(a[0])?, parse_date(a[1])?)?.to_string()),
        "date_pl_interval" => ok(ft(date_pl_interval(
            parse_date(a[0])?,
            parse_interval(a[1])?,
        )?)),
        "date_mi_interval" => ok(ft(date_mi_interval(
            parse_date(a[0])?,
            parse_interval(a[1])?,
        )?)),
        "date_pl_time" => ok(ft(date_pl_time(parse_date(a[0])?, parse_time(a[1])?)?)),
        "timestamp_pl_interval" => ok(ft(timestamp_pl_interval(
            parse_timestamp(a[0])?,
            parse_interval(a[1])?,
        )?)),
        "timestamp_mi_interval" => ok(ft(timestamp_mi_interval(
            parse_timestamp(a[0])?,
            parse_interval(a[1])?,
        )?)),
        "timestamptz_pl_interval" => {
            let z = tz()?;
            ok(format_timestamptz(
                timestamptz_pl_interval(parse_timestamptz(a[0], &z)?, parse_interval(a[1])?, &z)?,
                &z,
            ))
        }
        "timestamptz_mi_interval" => {
            let z = tz()?;
            ok(format_timestamptz(
                timestamptz_mi_interval(parse_timestamptz(a[0], &z)?, parse_interval(a[1])?, &z)?,
                &z,
            ))
        }
        "timestamp_mi" => ok(fi(timestamp_mi(
            parse_timestamp(a[0])?,
            parse_timestamp(a[1])?,
        )?)),
        "timestamptz_mi" => {
            let z = tz()?;
            ok(fi(timestamptz_mi(
                parse_timestamptz(a[0], &z)?,
                parse_timestamptz(a[1], &z)?,
            )?))
        }
        "time_pl_interval" => ok(format_time(time_pl_interval_checked(
            parse_time(a[0])?,
            parse_interval(a[1])?,
        )?)),
        "time_mi_interval" => ok(format_time(time_mi_interval_checked(
            parse_time(a[0])?,
            parse_interval(a[1])?,
        )?)),
        "time_mi_time" => ok(fi(time_mi_time(parse_time(a[0])?, parse_time(a[1])?))),
        "interval_pl" => ok(fi(interval_pl(
            parse_interval(a[0])?,
            parse_interval(a[1])?,
        )?)),
        "interval_mi" => ok(fi(interval_mi(
            parse_interval(a[0])?,
            parse_interval(a[1])?,
        )?)),
        "interval_um" => ok(fi(interval_um(parse_interval(a[0])?)?)),
        "interval_mul" => ok(fi(interval_mul(parse_interval(a[0])?, float_arg(a[1]))?)),
        "interval_div" => ok(fi(interval_div(parse_interval(a[0])?, float_arg(a[1]))?)),
        "interval_cmp" => {
            let o = interval_cmp(parse_interval(a[0])?, parse_interval(a[1])?);
            ok((o as i32).to_string())
        }
        "justify_hours" => ok(fi(justify_hours(parse_interval(a[0])?)?)),
        "justify_days" => ok(fi(justify_days(parse_interval(a[0])?)?)),
        "justify_interval" => ok(fi(justify_interval(parse_interval(a[0])?)?)),
        "extract_date" => opt(extract_date_opt(a[0], parse_date(a[1])?), |d| d.to_string()),
        "extract_time" => opt(extract_time_opt(a[0], parse_time(a[1])?), |d| d.to_string()),
        "extract_timestamp" => opt(extract_timestamp_opt(a[0], parse_timestamp(a[1])?), |d| {
            d.to_string()
        }),
        "extract_timestamptz" => {
            let z = tz()?;
            opt(
                extract_timestamptz_opt(a[0], parse_timestamptz(a[1], &z)?, &z),
                |d| d.to_string(),
            )
        }
        "extract_interval" => opt(extract_interval_opt(a[0], parse_interval(a[1])?), |d| {
            d.to_string()
        }),
        "date_part_date" => opt(date_part_date_opt(a[0], parse_date(a[1])?), float8_out),
        "date_part_time" => opt(date_part_time_opt(a[0], parse_time(a[1])?), float8_out),
        "date_part_timestamp" => opt(
            date_part_timestamp_opt(a[0], parse_timestamp(a[1])?),
            float8_out,
        ),
        "date_part_timestamptz" => {
            let z = tz()?;
            opt(
                date_part_timestamptz_opt(a[0], parse_timestamptz(a[1], &z)?, &z),
                float8_out,
            )
        }
        "date_part_interval" => opt(
            date_part_interval_opt(a[0], parse_interval(a[1])?),
            float8_out,
        ),
        "date_trunc_timestamp" => ok(ft(date_trunc_timestamp(a[0], parse_timestamp(a[1])?)?)),
        "date_trunc_timestamptz" => {
            let z = tz()?;
            ok(format_timestamptz(
                date_trunc_timestamptz(a[0], parse_timestamptz(a[1], &z)?, &z)?,
                &z,
            ))
        }
        "date_trunc_timestamptz_at_zone" => {
            let z = tz()?;
            ok(format_timestamptz(
                date_trunc_timestamptz_at_zone(a[0], parse_timestamptz(a[1], &z)?, a[2])?,
                &z,
            ))
        }
        "date_trunc_interval" => ok(fi(date_trunc_interval(a[0], parse_interval(a[1])?)?)),
        "date_bin" => ok(ft(date_bin(
            parse_interval(a[0])?,
            parse_timestamp(a[1])?,
            parse_timestamp(a[2])?,
        )?)),
        "age" => ok(fi(age(parse_timestamp(a[0])?, parse_timestamp(a[1])?)?)),
        "age_tz" => {
            let z = tz()?;
            ok(fi(age_tz(
                parse_timestamptz(a[0], &z)?,
                parse_timestamptz(a[1], &z)?,
                &z,
            )?))
        }
        "make_date" => ok(fd(make_date(int_arg(a[0]), int_arg(a[1]), int_arg(a[2]))?)),
        "make_time" => ok(format_time(make_time(
            int_arg(a[0]),
            int_arg(a[1]),
            float_arg(a[2]),
        )?)),
        "make_timestamp" => ok(ft(make_timestamp(
            int_arg(a[0]),
            int_arg(a[1]),
            int_arg(a[2]),
            int_arg(a[3]),
            int_arg(a[4]),
            float_arg(a[5]),
        )?)),
        "make_timestamptz" => {
            let z = tz()?;
            ok(format_timestamptz(
                make_timestamptz(
                    int_arg(a[0]),
                    int_arg(a[1]),
                    int_arg(a[2]),
                    int_arg(a[3]),
                    int_arg(a[4]),
                    float_arg(a[5]),
                    &z,
                )?,
                &z,
            ))
        }
        "make_timestamptz_at_zone" => {
            let z = tz()?;
            ok(format_timestamptz(
                make_timestamptz_at_zone(
                    int_arg(a[0]),
                    int_arg(a[1]),
                    int_arg(a[2]),
                    int_arg(a[3]),
                    int_arg(a[4]),
                    float_arg(a[5]),
                    a[6],
                )?,
                &z,
            ))
        }
        "make_interval" => ok(fi(make_interval(
            int_arg(a[0]),
            int_arg(a[1]),
            int_arg(a[2]),
            int_arg(a[3]),
            int_arg(a[4]),
            int_arg(a[5]),
            float_arg(a[6]),
        )?)),
        "to_timestamp_epoch" => {
            let z = tz()?;
            ok(format_timestamptz(to_timestamp_epoch(float_arg(a[0]))?, &z))
        }
        "timezone_tstz" => {
            let z = tz()?;
            let t = parse_timestamptz(a[1], &z)?;
            let r = timezone_tstz_by_name(a[0], t)?;
            // the TimeZone-based variant must agree whenever it can parse the zone
            if let Ok(zone) = TimeZone::parse(a[0])
                && !matches!(a[0].to_ascii_lowercase().as_str(), "msk" | "art" | "vet")
            {
                let r2 = timezone_tstz(&zone, t)?;
                assert_eq!(r, r2, "timezone_tstz vs by_name for {:?}", a);
            }
            ok(ft(r))
        }
        "timezone_ts" => {
            let z = tz()?;
            let t = parse_timestamp(a[1])?;
            let r = timezone_ts_by_name(a[0], t)?;
            if let Ok(zone) = TimeZone::parse(a[0])
                && !matches!(a[0].to_ascii_lowercase().as_str(), "msk" | "art" | "vet")
            {
                let r2 = timezone_ts(&zone, t)?;
                assert_eq!(r, r2, "timezone_ts vs by_name for {:?}", a);
            }
            ok(format_timestamptz(r, &z))
        }
        "timezone_ts_interval" => {
            let z = tz()?;
            ok(format_timestamptz(
                timezone_ts_interval(parse_interval(a[0])?, parse_timestamp(a[1])?)?,
                &z,
            ))
        }
        "timezone_tstz_interval" => {
            let z = tz()?;
            ok(ft(timezone_tstz_interval(
                parse_interval(a[0])?,
                parse_timestamptz(a[1], &z)?,
            )?))
        }
        "isfinite_date" => ok(isfinite_date(parse_date(a[0])?).to_string()),
        "isfinite_timestamp" => ok(isfinite_timestamp(parse_timestamp(a[0])?).to_string()),
        "isfinite_interval" => ok(isfinite_interval(parse_interval(a[0])?).to_string()),
        "to_char_timestamp" => to_char_timestamp_opt(parse_timestamp(a[0])?, a[1]),
        "to_char_timestamptz" => {
            let z = tz()?;
            to_char_timestamptz_opt(parse_timestamptz(a[0], &z)?, a[1], &z)
        }
        "to_char_interval" => to_char_interval_opt(parse_interval(a[0])?, a[1]),
        "to_date" => ok(fd(to_date(a[0], a[1])?)),
        "to_timestamp" => {
            let z = tz()?;
            ok(format_timestamptz(to_timestamp(a[0], a[1], &z)?, &z))
        }
        "date_cmp_timestamp" => {
            ok((date_cmp_timestamp(parse_date(a[0])?, parse_timestamp(a[1])?) as i32).to_string())
        }
        "date_cmp_timestamptz" => {
            let z = tz()?;
            ok(
                (date_cmp_timestamptz(parse_date(a[0])?, parse_timestamptz(a[1], &z)?, &z) as i32)
                    .to_string(),
            )
        }
        "timestamp_cmp_timestamptz" => {
            let z = tz()?;
            ok((timestamp_cmp_timestamptz(parse_timestamp(a[0])?, parse_timestamptz(a[1], &z)?, &z) as i32).to_string())
        }
        "overlaps_ts" => {
            let p = |s: &str| -> std::result::Result<Option<i64>, Error> {
                if s == "NULL" {
                    Ok(None)
                } else {
                    parse_timestamp(s).map(Some)
                }
            };
            Ok(overlaps(p(a[0])?, p(a[1])?, p(a[2])?, p(a[3])?).map(|b| b.to_string()))
        }
        "interval_sum" | "interval_avg" => {
            let mut st = IntervalAggState::new();
            if !a[0].is_empty() {
                for s in a[0].split('|') {
                    st.accum(parse_interval(s)?)?;
                }
            }
            let r = if func == "interval_sum" {
                st.sum()?
            } else {
                st.avg()?
            };
            Ok(r.map(fi))
        }
        "to_timestamp_roundtrip" => {
            let z = tz()?;
            let t = parse_timestamptz(a[0], &z)?;
            match to_char_timestamptz_opt(t, a[1], &z)? {
                Some(s) => ok(format_timestamptz(to_timestamp(&s, a[1], &z)?, &z)),
                None => Ok(None),
            }
        }
        "to_date_roundtrip" => match to_char_timestamp_opt(parse_timestamp(a[0])?, a[1])? {
            Some(s) => ok(fd(to_date(&s, a[1])?)),
            None => Ok(None),
        },
        "session_tz" => {
            let z = tz()?;
            ok(format_timestamptz(
                parse_timestamptz("2020-07-01 12:00:00+00", &TimeZone::Utc)?,
                &z,
            ))
        }
        _ => panic!("unknown func {}", func),
    }
}

fn render(r: R) -> String {
    match r {
        Ok(Some(s)) => format!("OK {}", s),
        Ok(None) => "NULL".to_string(),
        Err(e) => format!("ERR {} {}", e.sqlstate, e.message),
    }
}

fn golden_dir() -> String {
    std::env::var("PGDT_GOLDEN_DIR")
        .unwrap_or_else(|_| format!("{}/tests/golden", env!("CARGO_MANIFEST_DIR")))
}

/// One-off sweeps: `PGDT_GOLDEN_DIR=/some/dir cargo test --test golden -- --ignored`
/// runs every `*.tsv` file in that directory.
#[test]
#[ignore]
fn golden_extra_dir() {
    let dir = golden_dir();
    let mut names: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            e.file_name()
                .to_str()?
                .strip_suffix(".tsv")
                .map(str::to_string)
        })
        .collect();
    names.sort();
    for n in names {
        check(&n);
    }
}

fn run_file(name: &str) -> (usize, Vec<String>) {
    let path = format!("{}/{}.tsv", golden_dir(), name);
    let data = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {}", path, e));
    let mut failures = Vec::new();
    let mut n = 0;
    for line in data.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        let arrow = parts.iter().position(|p| *p == "=>").expect("missing =>");
        let func = parts[0];
        let tz = parts[1];
        let args = &parts[2..arrow];
        let expected = parts[arrow + 1..].join("\t");
        n += 1;
        let got = std::panic::catch_unwind(|| render(eval(func, tz, args))).unwrap_or_else(|e| {
            format!(
                "PANIC {:?}",
                e.downcast_ref::<String>()
                    .cloned()
                    .or(e.downcast_ref::<&str>().map(|s| s.to_string()))
            )
        });
        if got != expected {
            failures.push(format!(
                "{}\t{}\t{}\n    expected: {}\n    got:      {}",
                func,
                tz,
                args.join(" | "),
                expected,
                got
            ));
        }
    }
    (n, failures)
}

fn check(name: &str) {
    let (n, failures) = run_file(name);
    if !failures.is_empty() {
        let shown: Vec<&String> = failures.iter().take(60).collect();
        panic!(
            "{}: {} of {} cases failed:\n{}",
            name,
            failures.len(),
            n,
            shown
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join("\n")
        );
    }
    eprintln!("{}: {} cases ok", name, n);
}

#[test]
fn golden_io() {
    check("io");
}
#[test]
fn golden_tz_io() {
    check("tz_io");
}
#[test]
fn golden_session_tz() {
    check("session_tz");
}
#[test]
fn golden_typmod() {
    check("typmod");
}
#[test]
fn golden_casts() {
    check("casts");
}
#[test]
fn golden_arith() {
    check("arith");
}
#[test]
fn golden_fields() {
    check("fields");
}
#[test]
fn golden_trunc() {
    check("trunc");
}
#[test]
fn golden_misc() {
    check("misc");
}
#[test]
fn golden_to_char() {
    check("to_char");
}
#[test]
fn golden_to_date() {
    check("to_date");
}
#[test]
fn golden_edge() {
    check("edge");
}
#[test]
fn golden_compare() {
    check("compare");
}
#[test]
fn golden_to_timestamp_fuzz() {
    check("to_timestamp_fuzz");
}
#[test]
fn golden_abbrevs() {
    check("abbrevs");
}
#[test]
fn golden_random() {
    check("random");
}
#[test]
fn golden_zones() {
    check("zones");
}
#[test]
fn golden_input_fuzz() {
    check("input_fuzz");
}
#[test]
fn golden_to_char_fuzz() {
    check("to_char_fuzz");
}
