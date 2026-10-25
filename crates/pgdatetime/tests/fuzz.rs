//! Robustness: no public function may panic (debug builds trap integer
//! overflow) for arbitrary inputs, including out-of-range raw values.

use pgdatetime::*;
use std::panic::{AssertUnwindSafe, catch_unwind};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545F4914F6CDD1D)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn i64(&mut self) -> i64 {
        match self.below(8) {
            0 => [i64::MIN, i64::MAX, i64::MIN + 1, i64::MAX - 1, 0, -1, 1][self.below(7)],
            1 => self.next() as i64,
            2 => (self.next() as i64) % 1_000_000_000_000_000,
            3 => (self.next() as i64) % 9_223_371_331_200_000_000,
            _ => (self.next() as i64) % 10_000_000_000_000_000,
        }
    }
    fn i32(&mut self) -> i32 {
        match self.below(6) {
            0 => [i32::MIN, i32::MAX, i32::MIN + 1, i32::MAX - 1, 0, -1, 1][self.below(7)],
            1 => self.next() as i32,
            2 => (self.next() as i32) % 100_000,
            _ => (self.next() as i32) % 3_000_000,
        }
    }
    fn f64(&mut self) -> f64 {
        match self.below(6) {
            0 => [
                f64::NAN,
                f64::INFINITY,
                f64::NEG_INFINITY,
                0.0,
                -0.0,
                1e308,
                -1e308,
                5e-324,
            ][self.below(8)],
            1 => f64::from_bits(self.next()),
            _ => ((self.next() as i64) % 1_000_000_000) as f64 / 1000.0,
        }
    }
    fn interval(&mut self) -> Interval {
        match self.below(5) {
            0 => [
                Interval::INFINITY,
                Interval::NEG_INFINITY,
                Interval::default(),
            ][self.below(3)],
            _ => Interval {
                months: self.i32(),
                days: self.i32(),
                micros: self.i64(),
            },
        }
    }
    fn string(&mut self, alphabet: &[&str], max: usize) -> String {
        let n = self.below(max + 1);
        (0..n)
            .map(|_| alphabet[self.below(alphabet.len())])
            .collect()
    }
}

const ALPHA: &[&str] = &[
    "0",
    "1",
    "2",
    "9",
    "12",
    "2020",
    "99",
    "-",
    "+",
    "/",
    ".",
    ":",
    " ",
    ",",
    "T",
    "Z",
    "J",
    "BC",
    "AD",
    "am",
    "pm",
    "Jan",
    "january",
    "Mon",
    "PST",
    "MSK",
    "Europe/Warsaw",
    "UTC",
    "epoch",
    "infinity",
    "allballs",
    "year",
    "mons",
    "day",
    "hour",
    "min",
    "sec",
    "ago",
    "@",
    "P",
    "Y",
    "M",
    "W",
    "D",
    "H",
    "S",
    "e",
    "x",
    "\u{e9}",
    "\t",
    "\"",
    "'",
    "(",
    "999999999999",
    "2147483648",
    "9223372036854775808",
    "0x1p3",
    "inf",
    "nan",
    "<",
    ">",
    "_",
    "DST",
];

const FMT: &[&str] = &[
    "YYYY", "YYY", "YY", "Y", "Y,YYY", "IYYY", "MM", "MON", "Mon", "MONTH", "DD", "DDD", "IDDD",
    "D", "ID", "DAY", "Dy", "WW", "W", "IW", "CC", "J", "Q", "RM", "rm", "HH", "HH12", "HH24",
    "MI", "SS", "SSSS", "MS", "US", "FF1", "FF6", "AM", "a.m.", "BC", "b.c.", "TZ", "tz", "TZH",
    "TZM", "OF", "FM", "TM", "TH", "th", "SP", "FX", "-", "/", " ", ":", ".", "\"", "\\", "x",
    "\u{e9}", "9", "0",
];

fn tz(rng: &mut Rng) -> TimeZone {
    match rng.below(5) {
        0 => TimeZone::Utc,
        1 => TimeZone::Fixed(rng.i32() % 700_000),
        2 => TimeZone::Fixed(rng.i32()),
        _ => TimeZone::Named(chrono_tz::TZ_VARIANTS[rng.below(chrono_tz::TZ_VARIANTS.len())]),
    }
}

const UNITS: &[&str] = &[
    "microseconds",
    "milliseconds",
    "second",
    "minute",
    "hour",
    "day",
    "week",
    "month",
    "quarter",
    "year",
    "decade",
    "century",
    "millennium",
    "julian",
    "isoyear",
    "dow",
    "isodow",
    "doy",
    "epoch",
    "timezone",
    "timezone_hour",
    "timezone_minute",
    "foo",
    "",
];

#[test]
fn no_panics() {
    let seed = std::env::var("PGDT_FUZZ_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0x1234_5678_9abc_def1u64);
    let iters: usize = std::env::var("PGDT_FUZZ_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60_000);
    let mut rng = Rng(seed | 1);
    let mut panics = Vec::new();
    for i in 0..iters {
        let s = rng.string(ALPHA, 8);
        let f = rng.string(FMT, 8);
        let z = tz(&mut rng);
        let (a, b) = (rng.i64(), rng.i64());
        let (d1, d2) = (rng.i32(), rng.i32());
        let (iv, iv2) = (rng.interval(), rng.interval());
        let fl = rng.f64();
        let unit = UNITS[rng.below(UNITS.len())];
        let r = catch_unwind(AssertUnwindSafe(|| {
            let _ = parse_date(&s);
            let _ = parse_time(&s);
            let _ = parse_timestamp(&s);
            let _ = parse_timestamptz(&s, &z);
            let _ = parse_interval(&s);
            let _ = TimeZone::parse(&s);
            let _ = TimeZone::parse_session(&s);
            let _ = to_date(&s, &f);
            let _ = to_timestamp(&s, &f, &z);
            let _ = to_char_timestamp(a, &f);
            let _ = to_char_timestamptz(a, &f, &z);
            let _ = to_char_interval(iv, &f);
            let _ = format_date(d1);
            let _ = format_time(a);
            let _ = format_timestamp(a);
            let _ = format_timestamptz(a, &z);
            let _ = format_interval(iv);
            for st in [
                IntervalStyle::PostgresVerbose,
                IntervalStyle::SqlStandard,
                IntervalStyle::Iso8601,
            ] {
                let _ = format_interval_style(iv, st);
            }
            let _ = adjust_time(a, d2 % 10);
            let _ = adjust_timestamp(a, d2 % 10);
            let _ = adjust_interval(iv, d2 % 10);
            let _ = date_to_timestamp(d1);
            let _ = timestamp_to_date(a);
            let _ = date_to_timestamptz(d1, &z);
            let _ = timestamptz_to_date(a, &z);
            let _ = timestamp_to_timestamptz(a, &z);
            let _ = timestamptz_to_timestamp(a, &z);
            let _ = timestamp_to_time(a);
            let _ = timestamptz_to_time(a, &z);
            let _ = time_to_interval(a);
            let _ = interval_to_time(iv);
            let _ = date_pl_days(d1, d2);
            let _ = date_mi_days(d1, d2);
            let _ = date_mi_date(d1, d2);
            let _ = date_pl_interval(d1, iv);
            let _ = date_mi_interval(d1, iv);
            let _ = date_pl_time(d1, b);
            let _ = timestamp_pl_interval(a, iv);
            let _ = timestamp_mi_interval(a, iv);
            let _ = timestamptz_pl_interval(a, iv, &z);
            let _ = timestamptz_mi_interval(a, iv, &z);
            let _ = timestamp_mi(a, b);
            let _ = timestamptz_mi(a, b);
            let _ = time_pl_interval(a, iv);
            let _ = time_mi_interval(a, iv);
            let _ = time_mi_time(a, b);
            let _ = interval_pl(iv, iv2);
            let _ = interval_mi(iv, iv2);
            let _ = interval_um(iv);
            let _ = interval_mul(iv, fl);
            let _ = interval_div(iv, fl);
            let _ = interval_cmp(iv, iv2);
            let _ = justify_hours(iv);
            let _ = justify_days(iv);
            let _ = justify_interval(iv);
            let _ = extract_date(unit, d1);
            let _ = extract_time(unit, a);
            let _ = extract_timestamp(unit, a);
            let _ = extract_timestamptz(unit, a, &z);
            let _ = extract_interval(unit, iv);
            let _ = date_part_timestamp(unit, a);
            let _ = date_part_timestamptz(unit, a, &z);
            let _ = date_part_time(unit, a);
            let _ = date_part_interval(unit, iv);
            let _ = date_part_date(unit, d1);
            let _ = date_trunc_timestamp(unit, a);
            let _ = date_trunc_timestamptz(unit, a, &z);
            let _ = date_trunc_interval(unit, iv);
            let _ = date_bin(iv, a, b);
            let _ = age(a, b);
            let _ = age_tz(a, b, &z);
            let _ = make_date(d1, d2 % 20, rng_small(d1));
            let _ = make_time(d1 % 30, d2 % 70, fl);
            let _ = make_timestamp(d1, d2 % 20, rng_small(d2), d1 % 30, d2 % 70, fl);
            let _ = make_timestamptz(d1, d2 % 20, rng_small(d2), d1 % 30, d2 % 70, fl, &z);
            let _ = make_interval(d1, d2, d1, d2, d1, d2, fl);
            let _ = to_timestamp_epoch(fl);
            let _ = timezone_tstz(&z, a);
            let _ = timezone_ts(&z, a);
            let _ = timezone_tstz_by_name(&s, a);
            let _ = timezone_ts_by_name(&s, a);
            let _ = date_to_ymd(d1);
            let _ = ymd_to_date(d1, d2 as u32, b as u32);
            let _ = date_trunc_timestamptz_at_zone(unit, a, &s);
            let _ = make_timestamptz_at_zone(2020, 1, 1, 0, 0, 0.0, &s);
        }));
        if r.is_err() && panics.len() < 20 {
            panics.push(format!(
                "iteration {}: s={:?} f={:?} z={:?} a={} b={} d1={} d2={} iv={:?} fl={}",
                i, s, f, z, a, b, d1, d2, iv, fl
            ));
        }
    }
    assert!(panics.is_empty(), "panics:\n{}", panics.join("\n"));
}

fn rng_small(x: i32) -> i32 {
    x % 40
}
