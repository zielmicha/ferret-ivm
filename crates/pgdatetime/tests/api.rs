//! API-level tests (behaviour not expressible as single SQL expressions).

use pgdatetime::*;

fn iv(s: &str) -> Interval {
    parse_interval(s).unwrap()
}

#[test]
fn interval_agg_inverse_transition() {
    let vals = [
        "1 day",
        "2 hours",
        "-3 mons",
        "infinity",
        "1 year 2 days",
        "-infinity",
        "00:00:00.5",
    ];
    let mut all = IntervalAggState::new();
    for v in vals {
        all.accum(iv(v)).unwrap();
    }
    // remove a prefix: must equal accumulating the suffix only
    let mut moving = all;
    for v in &vals[..3] {
        moving.discard(iv(v)).unwrap();
    }
    let mut suffix = IntervalAggState::new();
    for v in &vals[3..] {
        suffix.accum(iv(v)).unwrap();
    }
    assert_eq!(moving, suffix);
    // combine
    let mut a = IntervalAggState::new();
    let mut b = IntervalAggState::new();
    a.accum(iv("1 day")).unwrap();
    b.accum(iv("2 days")).unwrap();
    b.accum(iv("infinity")).unwrap();
    a.combine(&b).unwrap();
    assert_eq!(a.sum().unwrap(), Some(Interval::INFINITY));
    // discarding everything resets the sum
    let mut c = IntervalAggState::new();
    c.accum(iv("1 day")).unwrap();
    c.discard(iv("1 day")).unwrap();
    assert_eq!(c.sum().unwrap(), None);
    assert_eq!(c.avg().unwrap(), None);
    let mut d = IntervalAggState::new();
    d.accum(iv("1 day")).unwrap();
    d.accum(iv("2 days")).unwrap();
    assert_eq!(format_interval(d.avg().unwrap().unwrap()), "1 day 12:00:00");
}

#[test]
fn timezone_parsing_semantics() {
    // AT TIME ZONE semantics: numeric strings are POSIX (positive = west)
    assert_eq!(TimeZone::parse("+05").unwrap(), TimeZone::Fixed(-5 * 3600));
    assert_eq!(TimeZone::parse("-5").unwrap(), TimeZone::Fixed(5 * 3600));
    // abbreviations are fixed offsets, even when a zone of that name exists
    assert_eq!(TimeZone::parse("CET").unwrap(), TimeZone::Fixed(3600));
    assert_eq!(TimeZone::parse("pst").unwrap(), TimeZone::Fixed(-8 * 3600));
    assert_eq!(TimeZone::parse("UTC").unwrap(), TimeZone::Utc);
    assert_eq!(
        TimeZone::parse("europe/WARSAW").unwrap(),
        TimeZone::Named(chrono_tz::Tz::Europe__Warsaw)
    );
    // dynamic abbreviations resolve to their zone
    assert_eq!(
        TimeZone::parse("MSK").unwrap(),
        TimeZone::Named(chrono_tz::Tz::Europe__Moscow)
    );
    // SET TIME ZONE semantics: numbers are hours east, zone names are zones
    assert_eq!(
        TimeZone::parse_session("-5").unwrap(),
        TimeZone::Fixed(-5 * 3600)
    );
    assert_eq!(
        TimeZone::parse_session("5.5").unwrap(),
        TimeZone::Fixed(19800)
    );
    assert_eq!(
        TimeZone::parse_session("INTERVAL '+05:30'").unwrap(),
        TimeZone::Fixed(19800)
    );
    assert_eq!(
        TimeZone::parse_session("CET").unwrap(),
        TimeZone::Named(chrono_tz::Tz::CET)
    );
    assert_eq!(
        TimeZone::parse_session("posixrules").unwrap(),
        TimeZone::Named(chrono_tz::Tz::America__New_York)
    );
    let e = TimeZone::parse_session("PST").unwrap_err();
    assert_eq!(e.sqlstate, "22023");
    assert_eq!(
        e.message,
        "invalid value for parameter \"TimeZone\": \"PST\""
    );
    let e = TimeZone::parse("Foo/Bar").unwrap_err();
    assert_eq!(e.message, "time zone \"Foo/Bar\" not recognized");
    assert_eq!(TimeZone::Fixed(19800).name(), "<+05:30>-05:30");
    assert!(!tzdata_version().is_empty());
}

#[test]
fn null_results() {
    let inf = parse_timestamp("infinity").unwrap();
    assert_eq!(extract_timestamp_opt("month", inf).unwrap(), None);
    assert_eq!(
        extract_timestamp_opt("year", inf).unwrap(),
        Some(Decimal::INFINITY)
    );
    assert!(
        extract_timestamp("month", inf)
            .unwrap_err()
            .is_null_result()
    );
    assert_eq!(timestamp_to_time_opt(inf).unwrap(), None);
    assert_eq!(to_char_timestamp_opt(0, "").unwrap(), None);
    assert!(to_char_timestamp(inf, "YYYY").unwrap_err().is_null_result());
    assert_eq!(
        date_part_interval_opt("hour", Interval::NEG_INFINITY).unwrap(),
        Some(f64::NEG_INFINITY)
    );
}

#[test]
fn special_values_rejected() {
    for (s, v) in [
        ("now", "now"),
        ("today", "today"),
        ("2020-01-01 tomorrow", "tomorrow"),
        ("YESTERDAY", "yesterday"),
    ] {
        let e = parse_timestamp(s).unwrap_err();
        assert_eq!(e.sqlstate, "0A000");
        assert_eq!(
            e.message,
            format!("special value \"{}\" is not supported", v)
        );
    }
    assert_eq!(parse_date("epoch").unwrap(), -10957);
    assert_eq!(parse_time("allballs").unwrap(), 0);
    assert_eq!(parse_timestamp("-infinity").unwrap(), i64::MIN);
}

#[test]
fn helpers() {
    assert_eq!(date_to_ymd(0), (2000, 1, 1));
    assert_eq!(
        date_to_ymd(parse_date("0044-03-15 BC").unwrap()),
        (-43, 3, 15)
    );
    assert_eq!(ymd_to_date(2000, 1, 1).unwrap(), 0);
    assert_eq!(ymd_to_date(2001, 2, 29).unwrap_err().sqlstate, "22008");
    assert_eq!(
        interval_cmp_value(iv("1 mon")),
        interval_cmp_value(iv("30 days"))
    );
    assert_eq!(
        interval_cmp(iv("1 day"), iv("24 hours")),
        std::cmp::Ordering::Equal
    );
    assert_eq!(Decimal::new(-123456, 3).to_string(), "-123.456");
    assert_eq!(overlaps(Some(1), Some(5), Some(5), Some(9)), Some(false));
    assert_eq!(overlaps(Some(1), None, Some(0), Some(9)), Some(true));
    assert_eq!(overlaps::<i64>(None, None, Some(0), Some(9)), None);
}
