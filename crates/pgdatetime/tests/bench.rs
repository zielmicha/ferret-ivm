//! Rough timings: `cargo test --release -p pgdatetime --test bench -- --ignored --nocapture`

use pgdatetime::*;
use std::hint::black_box;
use std::time::Instant;

fn time<T>(name: &str, n: usize, mut f: impl FnMut(usize) -> T) {
    let start = Instant::now();
    for i in 0..n {
        black_box(f(i));
    }
    let el = start.elapsed();
    println!(
        "{:<40} {:>8.1} ns/op",
        name,
        el.as_nanos() as f64 / n as f64
    );
}

#[test]
#[ignore]
fn bench() {
    let n = 1_000_000;
    let waw = TimeZone::parse("Europe/Warsaw").unwrap();
    let ny = TimeZone::parse("America/New_York").unwrap();
    let ts: Vec<i64> = (0..1024)
        .map(|i| (i as i64 - 512) * 86_400_000_000 * 37 + i as i64 * 1_234_567)
        .collect();
    let iv = parse_interval("1 mon 2 days 03:04:05").unwrap();
    time("parse_date", n, |_| parse_date("2020-01-15").unwrap());
    time("parse_timestamp", n, |_| {
        parse_timestamp("2020-01-15 12:34:56.789").unwrap()
    });
    time("parse_timestamptz (Warsaw)", n, |_| {
        parse_timestamptz("2020-01-15 12:34:56.789", &waw).unwrap()
    });
    time("parse_timestamptz (+02)", n, |_| {
        parse_timestamptz("2020-01-15 12:34:56.789+02", &waw).unwrap()
    });
    time("parse_interval", n, |_| {
        parse_interval("1 year 2 mons 3 days 04:05:06").unwrap()
    });
    time("format_date", n, |i| format_date((i % 100000) as i32));
    time("format_timestamp", n, |i| format_timestamp(ts[i % 1024]));
    time("format_timestamptz (Warsaw)", n, |i| {
        format_timestamptz(ts[i % 1024], &waw)
    });
    time("format_interval", n, |_| format_interval(iv));
    time("timestamp_pl_interval", n, |i| {
        timestamp_pl_interval(ts[i % 1024], iv)
    });
    time("timestamptz_pl_interval (NY)", n, |i| {
        timestamptz_pl_interval(ts[i % 1024], iv, &ny)
    });
    time("timestamp_to_timestamptz (NY)", n, |i| {
        timestamp_to_timestamptz(ts[i % 1024], &ny)
    });
    time("extract_timestamp(year)", n, |i| {
        extract_timestamp("year", ts[i % 1024])
    });
    time("extract_timestamptz(hour, NY)", n, |i| {
        extract_timestamptz("hour", ts[i % 1024], &ny)
    });
    time("date_trunc_timestamp(month)", n, |i| {
        date_trunc_timestamp("month", ts[i % 1024])
    });
    time("date_trunc_timestamptz(day, NY)", n, |i| {
        date_trunc_timestamptz("day", ts[i % 1024], &ny)
    });
    time("to_char_timestamp", n, |i| {
        to_char_timestamp(ts[i % 1024], "YYYY-MM-DD HH24:MI:SS")
    });
    time("to_timestamp", n / 4, |_| {
        to_timestamp("2020-01-15 12:34:56", "YYYY-MM-DD HH24:MI:SS", &ny)
    });
    time("age", n, |i| age(ts[i % 1024], ts[(i + 7) % 1024]));
    time("interval_cmp", n, |i| {
        interval_cmp(
            iv,
            Interval {
                months: 0,
                days: i as i32,
                micros: 0,
            },
        )
    });
    time("TimeZone::parse", n / 10, |_| {
        TimeZone::parse("Europe/Warsaw").unwrap()
    });
}
