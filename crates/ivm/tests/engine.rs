//! Engine-level tests through the public API (no PostgreSQL needed): exact
//! semantics of temporal filters, recovery, specific features, and
//! regression tests for bugs (some `#[ignore]`d with the bug described, for
//! bugs that are still open). Randomized comparisons of views with their
//! queries are in selfdiff.rs.

mod common;
use common::{exec, rows, start_in as start};

use ivm::coord::api::{ExecResult, SessionVars, SubEvent};
use ivm::coord::{self, EngineHandle};
use ivm::log::LogRecord;
use ivm::types::text::format_datum;
use std::path::PathBuf;

fn tmpdir(name: &str) -> PathBuf {
    let base = std::env::var("IVM_TEST_TMP").unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().into_owned());
    let d = PathBuf::from(base).join(format!("ivm-engine-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

/// Log a clock tick at `secs` seconds after 2026-01-01 00:00:00 UTC.
fn tick(h: &EngineHandle, secs: i64) {
    let base = 1_767_225_600_000_000i64; // 2026-01-01T00:00:00Z in Unix microseconds
    h.log(LogRecord::Tick { micros: base + secs * 1_000_000 }).unwrap();
}

/// A conditional upsert (what a source logs for an update leaving TOASTed
/// values unchanged while the table is being copied) fills the unchanged
/// values from the key's row, and changes nothing without one: the key has
/// not been copied yet, and its copy brings the whole row. Unlike a plain
/// upsert, it takes nothing from a row deleted just before (the delete need
/// not be of its old key).
#[test]
fn conditional_upserts_need_a_row() {
    use ivm::dataflow::TableChange::{Delete, UpsertIfPresent};
    use ivm::types::Datum;
    let dir = tmpdir("upsert-if-present");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, a int, b text)");
    exec(&h, "INSERT INTO t VALUES (1, 1, 'kept')");
    let upsert = |id: i32, a: i32| UpsertIfPresent { row: vec![Datum::Int4(id), Datum::Int4(a), Datum::Null], unchanged: vec![2] };
    // (table t is object 1000; the source, which need not exist, is 4242)
    let commit = |changes| LogRecord::Commit { source: 4242, lsn: 1, changes };
    h.log(commit(vec![(1000, upsert(1, 2)), (1000, upsert(2, 2))])).unwrap();
    assert_eq!(rows(&h, "SELECT * FROM t ORDER BY id"), ["1|2|kept"]);
    h.log(commit(vec![(1000, Delete { key: vec![Datum::Int4(1)] }), (1000, upsert(5, 5))])).unwrap();
    assert!(rows(&h, "SELECT * FROM t ORDER BY id").is_empty());
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn temporal_filters_follow_the_logged_clock() {
    let dir = tmpdir("temporal");
    let running = start(&dir);
    let h = running.handle.clone();
    tick(&h, 0);
    exec(&h, "CREATE TABLE ev (id int PRIMARY KEY, ts timestamptz)");
    exec(
        &h,
        "INSERT INTO ev VALUES (1, '2026-01-01 00:00:00+00'), (2, '2026-01-01 00:00:10+00'), \
         (3, '2026-01-01 00:01:00+00'), (4, NULL)",
    );
    // a sliding window and a "becomes due" filter, plus BETWEEN and equality on dates
    exec(&h, "CREATE MATERIALIZED VIEW recent AS SELECT id FROM ev WHERE ts > now() - interval '30 seconds'");
    exec(&h, "CREATE MATERIALIZED VIEW due AS SELECT id FROM ev WHERE ts <= now()");
    exec(&h, "CREATE MATERIALIZED VIEW win AS SELECT id FROM ev WHERE ts BETWEEN now() - interval '5 seconds' AND now()");
    exec(&h, "CREATE MATERIALIZED VIEW today AS SELECT id FROM ev WHERE ts::date = current_date");
    exec(&h, "CREATE MATERIALIZED VIEW counts AS SELECT count(*) AS n FROM ev WHERE ts > now() - interval '30 seconds'");
    let state = |h: &EngineHandle| {
        let ids = |v: &str| rows(h, &format!("SELECT id FROM {v} ORDER BY id")).join(",");
        format!(
            "recent={} due={} win={} today={} n={}",
            ids("recent"),
            ids("due"),
            ids("win"),
            ids("today"),
            rows(h, "SELECT n FROM counts").join(",")
        )
    };
    assert_eq!(state(&h), "recent=1,2,3 due=1 win=1 today=1,2,3 n=3");
    tick(&h, 10);
    assert_eq!(state(&h), "recent=1,2,3 due=1,2 win=2 today=1,2,3 n=3");
    tick(&h, 30); // row 1 is exactly 30s old: no longer > now() - 30s
    assert_eq!(state(&h), "recent=2,3 due=1,2 win= today=1,2,3 n=2");
    tick(&h, 61);
    assert_eq!(state(&h), "recent=3 due=1,2,3 win=3 today=1,2,3 n=1");
    // changes while time stands still are filtered at the current time
    exec(&h, "INSERT INTO ev VALUES (5, '2026-01-01 00:01:01+00')");
    exec(&h, "UPDATE ev SET ts = '2026-01-01 00:00:59+00' WHERE id = 3");
    assert_eq!(state(&h), "recent=3,5 due=1,2,3,5 win=3,5 today=1,2,3,5 n=2");
    tick(&h, 24 * 3600);
    assert_eq!(state(&h), "recent= due=1,2,3,5 win= today= n=0");
    // now() in a read is the logical clock as of the read
    assert_eq!(rows(&h, "SELECT now()"), vec!["2026-01-02 00:00:00+00".to_string()]);
    // after a restart the clock and pending events come back
    running.shutdown();
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "DELETE FROM ev WHERE id = 5");
    exec(&h, "INSERT INTO ev VALUES (6, '2026-01-02 00:00:20+00')");
    assert_eq!(state(&h), "recent=6 due=1,2,3 win= today=6 n=1");
    tick(&h, 24 * 3600 + 20);
    assert_eq!(state(&h), "recent=6 due=1,2,3,6 win=6 today=6 n=1");
    tick(&h, 24 * 3600 + 50);
    assert_eq!(state(&h), "recent= due=1,2,3,6 win= today=6 n=0");
    // the clock never goes backwards
    tick(&h, 0);
    assert_eq!(rows(&h, "SELECT now()"), vec!["2026-01-02 00:00:50+00".to_string()]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Adding a month clamps the day to the end of a shorter month: January
/// 29th to 31st plus a month are all February 28th, at their time of day,
/// so `now() + interval '1 month'` goes back a day at the midnights
/// starting them, and a row of February 28th noon enters
/// `ts > now() + interval '1 month'` on each of those days until noon.
/// Likewise `now() - interval '1 month'` on March 28th to 31st.
#[test]
fn temporal_filters_with_months_follow_the_calendar() {
    let dir = tmpdir("temporal-months");
    let running = start(&dir);
    let h = running.handle.clone();
    tick(&h, 0);
    exec(&h, "CREATE TABLE ev (id int PRIMARY KEY, ts timestamptz)");
    exec(&h, "INSERT INTO ev VALUES (1, '2026-02-28 12:00:00+00'), (2, '2026-02-28 00:00:00+00'), (3, '2026-03-01 00:00:00+00')");
    let ahead = "SELECT id FROM ev WHERE ts > now() + interval '1 month'";
    let recent = "SELECT id FROM ev WHERE ts > now() - interval '1 month'";
    exec(&h, &format!("CREATE MATERIALIZED VIEW ahead AS {ahead}"));
    exec(&h, &format!("CREATE MATERIALIZED VIEW recent AS {recent}"));
    let ids = |h: &EngineHandle, q: &str| {
        let mut r = rows(h, q);
        r.sort();
        r.join(",")
    };
    // (day of 2026, hour, ids in `ahead`, ids in `recent`)
    let steps = [
        (27, 6, "1,3", "1,2,3"),
        (27, 13, "3", "1,2,3"),
        (28, 6, "1,3", "1,2,3"),
        (29, 18, "3", "1,2,3"),
        (30, 6, "1,3", "1,2,3"), // January 31st, 06:00
        (30, 13, "3", "1,2,3"),
        (31, 6, "", "1,2,3"), // February 1st: plus a month is March 1st
        (31 + 28 + 27, 6, "", "1,3"), // March 28th
        (31 + 28 + 27, 13, "", "3"),
        (31 + 28 + 28, 6, "", "1,3"),
        (31 + 28 + 28, 13, "", "3"),
        (31 + 28 + 30, 6, "", "1,3"), // March 31st
        (31 + 28 + 30, 13, "", "3"),
        (31 + 28 + 31, 6, "", ""), // April 1st: minus a month is March 1st
    ];
    for (day, hour, want_ahead, want_recent) in steps {
        tick(&h, day * 86_400 + hour * 3600);
        let at = rows(&h, "SELECT now()").join("");
        assert_eq!(ids(&h, ahead), want_ahead, "{ahead} at {at}");
        assert_eq!(ids(&h, recent), want_recent, "{recent} at {at}");
        assert_eq!(ids(&h, "SELECT id FROM ahead"), want_ahead, "view ahead at {at}");
        assert_eq!(ids(&h, "SELECT id FROM recent"), want_recent, "view recent at {at}");
    }
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn temporal_filter_errors() {
    let dir = tmpdir("temporal-errors");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE ev (id int PRIMARY KEY, ts timestamptz)");
    for sql in [
        "CREATE MATERIALIZED VIEW v AS SELECT now() AS t FROM ev",
        "CREATE MATERIALIZED VIEW v AS SELECT id FROM ev WHERE ts > now() OR id = 1",
        "CREATE MATERIALIZED VIEW v AS SELECT id FROM ev WHERE now() - ts < interval '1 minute'",
        "CREATE MATERIALIZED VIEW v AS SELECT id FROM ev WHERE ts <> now()",
        "CREATE MATERIALIZED VIEW v AS SELECT id FROM ev WHERE ts > clock_timestamp()",
    ] {
        let stmt = ivm::sql::parse_statement(sql).unwrap();
        let r = h.execute(stmt, vec![], SessionVars::default(), String::new());
        assert!(r.is_err(), "{sql} should be rejected");
    }
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

struct Rng(u64);
impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

/// Temporal filters anywhere in a plan (joined, aggregated, mixed with
/// regular predicates) must equal the same query evaluated at the current
/// logical time, as random changes and clock ticks interleave.
#[test]
fn temporal_views_match_recompute_as_time_passes() {
    let dir = tmpdir("temporal-random");
    let running = start(&dir);
    let h = running.handle.clone();
    tick(&h, 0);
    exec(&h, "CREATE TABLE ev (id int PRIMARY KEY, k int, ts timestamptz, ttl int)");
    exec(&h, "CREATE TABLE ks (k int PRIMARY KEY, name text, since date)");
    let views = [
        "SELECT id, k FROM ev WHERE ts > now() - interval '30 seconds'",
        "SELECT id FROM ev WHERE ts <= now() AND ts + ttl * interval '1 second' > now()",
        "SELECT k, count(*) AS n, max(ts) AS last FROM ev WHERE ts BETWEEN now() - interval '1 minute' AND now() GROUP BY k",
        "SELECT e.id, s.name FROM ev e JOIN ks s ON s.k = e.k WHERE e.ts > now() - interval '45 seconds' AND s.since <= current_date",
        "SELECT s.name, count(e.id) AS n FROM ks s LEFT JOIN ev e ON e.k = s.k AND e.id % 2 = 0 WHERE s.since > current_date - 2 GROUP BY s.name",
        "SELECT count(*) AS n FROM ev WHERE date_trunc('minute', now()) <= ts",
    ];
    for (i, v) in views.iter().enumerate() {
        exec(&h, &format!("CREATE MATERIALIZED VIEW tv{i} AS {v}"));
    }
    let mut rng = Rng(0x9E3779B97F4A7C15);
    let mut clock = 0i64;
    for step in 0..250 {
        match rng.below(5) {
            0 | 1 => {
                // events around the current time, some in the future
                let id = rng.below(60);
                let ts = 1_767_225_600 + clock + rng.below(240) as i64 - 120;
                exec(
                    &h,
                    &format!(
                        "INSERT INTO ev VALUES ({id}, {}, to_timestamp({ts}), {}) ON CONFLICT DO NOTHING",
                        rng.below(4),
                        rng.below(90)
                    ),
                );
            }
            2 => {
                exec(&h, &format!("DELETE FROM ev WHERE id = {}", rng.below(60)));
            }
            3 => {
                let day = rng.below(5) as i64 - 3;
                exec(
                    &h,
                    &format!(
                        "INSERT INTO ks VALUES ({}, 'n{}', DATE '2026-01-01' + {day} + {}) ON CONFLICT DO NOTHING",
                        rng.below(4),
                        rng.below(3),
                        clock / 86_400
                    ),
                );
            }
            _ => {
                // time moves on, sometimes across a day
                clock += if rng.below(10) == 0 { 86_400 } else { rng.below(40) as i64 };
                tick(&h, clock);
            }
        }
        for (i, v) in views.iter().enumerate() {
            let mut got = rows(&h, &format!("SELECT * FROM tv{i}"));
            let mut want = rows(&h, v);
            got.sort();
            want.sort();
            assert_eq!(got, want, "step {step} (clock {clock}): view tv{i} = {v}");
        }
    }
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Temporal filters adding months to the clock, which goes back at the
/// midnights starting days clamped to the end of a shorter month, equal
/// their queries at every clock reading of a walk across month ends
/// (January to April 2026, and February 2024's leap day two years back),
/// as rows near those ends change, views are created over existing rows,
/// and the engine restarts.
#[test]
fn temporal_views_with_months_match_recompute() {
    let dir = tmpdir("temporal-months-random");
    let mut running = start(&dir);
    let mut h = running.handle.clone();
    tick(&h, 0);
    exec(&h, "CREATE TABLE ev (id int PRIMARY KEY, ts timestamptz, d date, lts timestamp)");
    let views = [
        "SELECT id FROM ev WHERE ts > now() - interval '1 month'",
        "SELECT id FROM ev WHERE ts <= now() + interval '1 month'",
        "SELECT id FROM ev WHERE ts BETWEEN now() - interval '1 month 2 days' AND now() + interval '1 month -3 hours'",
        "SELECT id FROM ev WHERE d > current_date - interval '1 month'",
        "SELECT id FROM ev WHERE lts >= localtimestamp + interval '-1 month 6 hours'",
        "SELECT count(*) AS n FROM ev WHERE ts::date = (now() - interval '1 month')::date",
        "SELECT id FROM ev WHERE ts > (now() + interval '1 month') - interval '1 month' - interval '29 days'",
        "SELECT id FROM ev WHERE date_trunc('hour', ts) < date_trunc('minute', now() - interval '2 years')",
        "SELECT id FROM ev WHERE ts = date_trunc('hour', now() + interval '13 months') - interval '1 year 1 month'",
    ];
    // the last three are created over existing rows
    let early = 6;
    for (i, v) in views[..early].iter().enumerate() {
        exec(&h, &format!("CREATE MATERIALIZED VIEW mv{i} AS {v}"));
    }
    let mut rng = Rng(0x2545_F491_4F6C_DD1D);
    // a time near a month's end, as SQL: ten days around the start of one
    // of February to April 2026 (or two years before, around February
    // 29th 2024), at an hour, minute or microsecond
    let near_end = |rng: &mut Rng| {
        let month = ["2026-02-01", "2026-03-01", "2026-04-01", "2024-03-01"][rng.below(4) as usize];
        let secs = rng.below(10 * 86_400) as i64 - 6 * 86_400;
        let secs = match rng.below(3) {
            0 => secs - secs.rem_euclid(3600),
            1 => secs - secs.rem_euclid(60),
            _ => secs,
        };
        format!("timestamp '{month}' + {secs} * interval '1 second' + {} * interval '1 microsecond'", rng.below(2))
    };
    let row = |rng: &mut Rng, id: u64| {
        let (a, b, c) = (near_end(rng), near_end(rng), near_end(rng));
        format!("({id}, ({a}) AT TIME ZONE 'UTC', ({b})::date, {c})")
    };
    let rows_now: Vec<String> = (0..60).map(|id| row(&mut rng, id)).collect();
    exec(&h, &format!("INSERT INTO ev VALUES {}", rows_now.join(", ")));
    // from January 20th to April 5th 2026
    let mut clock = 19 * 86_400;
    let mut step = 0;
    while clock < 94 * 86_400 {
        clock += match rng.below(4) {
            0 => rng.below(60) as i64,
            1 => rng.below(3600) as i64,
            _ => rng.below(7 * 3600) as i64,
        };
        tick(&h, clock);
        match rng.below(6) {
            0 => {
                let id = rng.below(100);
                let r = row(&mut rng, id);
                exec(&h, &format!("DELETE FROM ev WHERE id = {id}"));
                exec(&h, &format!("INSERT INTO ev VALUES {r}"));
            }
            1 => {
                exec(&h, &format!("DELETE FROM ev WHERE id = {}", rng.below(100)));
            }
            _ => {}
        }
        if step == 40 {
            for (i, v) in views.iter().enumerate().skip(early) {
                exec(&h, &format!("CREATE MATERIALIZED VIEW mv{i} AS {v}"));
            }
        }
        if step == 150 || step == 300 {
            running.shutdown();
            running = start(&dir);
            h = running.handle.clone();
        }
        for (i, v) in views.iter().enumerate() {
            if i >= early && step < 40 {
                continue;
            }
            let mut got = rows(&h, &format!("SELECT * FROM mv{i}"));
            let mut want = rows(&h, v);
            got.sort();
            want.sort();
            let at = rows(&h, "SELECT now()").join("");
            assert_eq!(got, want, "step {step} at {at}: view mv{i} = {v}");
        }
        step += 1;
    }
    assert!(step > 300, "{step} clock readings");
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn recovery_after_drops_keeps_table_rows() {
    // Dropping objects leaves holes in the dataflow graph, so a restarted
    // engine builds its nodes at different positions: recovery must still
    // find every checkpointed index (a table's rows exist nowhere else).
    let dir = tmpdir("recover-drops");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE a (id int PRIMARY KEY, v text)");
    exec(&h, "CREATE MATERIALIZED VIEW tmp1 AS SELECT v, count(*) FROM a GROUP BY v");
    exec(&h, "CREATE MATERIALIZED VIEW tmp2 AS SELECT id FROM a WHERE v = 'v1'");
    exec(&h, "CREATE TABLE b (k int, w int)"); // no primary key: a multiset
    exec(&h, "INSERT INTO a SELECT g, 'v' || (g % 3) FROM generate_series(1, 50) g");
    exec(&h, "INSERT INTO b VALUES (1, 1), (1, 1), (2, NULL)");
    exec(&h, "DROP VIEW tmp1");
    exec(&h, "CREATE MATERIALIZED VIEW j AS SELECT a.v, sum(b.w) AS s, count(*) AS n FROM a JOIN b ON a.id = b.k GROUP BY a.v");
    exec(&h, "DROP VIEW tmp2");
    exec(&h, "CHECKPOINT");
    exec(&h, "INSERT INTO b VALUES (3, 5)"); // replayed from the log
    let state = |h: &EngineHandle| {
        [
            rows(h, "SELECT * FROM a ORDER BY id"),
            rows(h, "SELECT * FROM b ORDER BY k, w"),
            rows(h, "SELECT * FROM j ORDER BY v"),
        ]
    };
    let before = state(&h);
    assert_eq!((before[0].len(), before[1].len(), before[2].len()), (50, 4, 3));
    running.shutdown();

    let running = start(&dir);
    let h = running.handle.clone();
    assert_eq!(state(&h), before);
    // maintenance continues from the recovered state
    exec(&h, "DELETE FROM a WHERE id = 1");
    exec(&h, "INSERT INTO b VALUES (2, 7)");
    let recompute = rows(&h, "SELECT a.v, sum(b.w), count(*) FROM a JOIN b ON a.id = b.k GROUP BY a.v ORDER BY a.v");
    assert_eq!(rows(&h, "SELECT * FROM j ORDER BY v"), recompute);
    assert_eq!(recompute.len(), 2);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn jsonpath_views_are_maintained() {
    let dir = tmpdir("jsonpath");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE d (id int PRIMARY KEY, j jsonb)");
    exec(&h, r#"INSERT INTO d VALUES (1, '{"items": [{"p": 5}, {"p": 12}]}'), (2, '{"items": []}')"#);
    exec(&h, r#"CREATE MATERIALIZED VIEW expensive AS SELECT id, jsonb_path_query_array(j, '$.items[*] ? (@.p > $min).p', '{"min": 10}') AS ps FROM d"#);
    exec(&h, "CREATE MATERIALIZED VIEW prices AS SELECT id, p FROM d, jsonb_path_query(j, '$.items[*].p') AS p");
    exec(&h, "CREATE MATERIALIZED VIEW n AS SELECT count(*) AS n FROM d WHERE j @? '$.items[*] ? (@.p > 10)'");
    assert_eq!(rows(&h, "SELECT * FROM expensive ORDER BY id"), ["1|[12]", "2|[]"]);
    assert_eq!(rows(&h, "SELECT * FROM prices ORDER BY id, p"), ["1|5", "1|12"]);
    assert_eq!(rows(&h, "SELECT * FROM n"), ["1"]);
    exec(&h, r#"UPDATE d SET j = '{"items": [{"p": 20}]}' WHERE id = 2"#);
    exec(&h, "DELETE FROM d WHERE id = 1");
    assert_eq!(rows(&h, "SELECT * FROM expensive ORDER BY id"), ["2|[20]"]);
    assert_eq!(rows(&h, "SELECT * FROM prices ORDER BY id, p"), ["2|20"]);
    assert_eq!(rows(&h, "SELECT * FROM n"), ["1"]);
    // canonical text, named arguments with defaults, silent mode
    assert_eq!(rows(&h, "SELECT '$.a  ? (@ > 1)'::jsonpath"), [r#"$."a"?(@ > 1)"#]);
    assert_eq!(rows(&h, "SELECT jsonb_path_query_first('{}', 'strict $.x', silent => true) IS NULL"), ["t"]);
    assert_eq!(rows(&h, r#"SELECT jsonb_path_query_array(target => '{"a": [1, 2]}', path => '$.a[*] ? (@ > $x)', vars => '{"x": 1}')"#), ["[2]"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn multidimensional_arrays() {
    let dir = tmpdir("arrays");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE m (id int PRIMARY KEY, a int[])");
    exec(&h, "INSERT INTO m VALUES (1, '{{1,2},{3,4}}'), (2, '{1,2,3,4}'), (3, '[0:1]={1,2}'), (4, '{1,2}'), (5, ARRAY[[1,2],[3,4]])");
    // arrays differing only in shape are distinct values, ordered as in PostgreSQL
    exec(&h, "CREATE MATERIALIZED VIEW g AS SELECT a, count(*) AS n FROM m GROUP BY a");
    let expect = ["[0:1]={1,2}|1", "{1,2}|1", "{1,2,3,4}|1", "{{1,2},{3,4}}|2"];
    assert_eq!(rows(&h, "SELECT a, n FROM g ORDER BY a"), expect);
    assert_eq!(rows(&h, "SELECT a, count(*) FROM m GROUP BY a ORDER BY a"), expect);
    assert_eq!(rows(&h, "SELECT a[2][1], a[1:1][2:2], a[2] IS NULL, pg_typeof(a) FROM m WHERE id = 1"), ["3|{{2}}|t|integer[]"]);
    assert_eq!(rows(&h, "SELECT array_agg(a ORDER BY id) FROM m WHERE id IN (1, 5)"), ["{{{1,2},{3,4}},{{1,2},{3,4}}}"]);
    // several set-returning functions in a select list run in lockstep
    assert_eq!(rows(&h, "SELECT unnest(ARRAY[1,2,3]), unnest(ARRAY['a','b'])"), ["1|a", "2|b", "3|"]);
    exec(&h, "CREATE MATERIALIZED VIEW z AS SELECT id, unnest(a) AS x, generate_series(1, 3) AS k FROM m WHERE id IN (3, 4)");
    assert_eq!(rows(&h, "SELECT * FROM z ORDER BY id, k"), ["3|1|1", "3|2|2", "3||3", "4|1|1", "4|2|2", "4||3"]);
    exec(&h, "UPDATE m SET a = '{7}' WHERE id = 3");
    assert_eq!(rows(&h, "SELECT * FROM z ORDER BY id, k"), ["3|7|1", "3||2", "3||3", "4|1|1", "4|2|2", "4||3"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Hydrating a top-k (DISTINCT ON) view re-encodes the group keys it decodes
/// from the input index; decoding clamped infinite intervals, so their groups
/// went missing.
#[test]
fn distinct_on_infinite_intervals() {
    let dir = tmpdir("distinct-on-intervals");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, iv interval)");
    exec(&h, "INSERT INTO t VALUES (1, 'infinity'), (2, 'infinity'), (3, '-infinity'), (4, '1 mon'), (5, '30 days'), (6, NULL)");
    let q = "SELECT DISTINCT ON (iv) iv, id FROM t ORDER BY iv, id";
    exec(&h, &format!("CREATE MATERIALIZED VIEW v AS {q}"));
    let want = ["-infinity|3", "1 mon|4", "infinity|1", "|6"];
    assert_eq!(rows(&h, q), want);
    assert_eq!(rows(&h, "SELECT * FROM v ORDER BY iv, id"), want);
    exec(&h, "DELETE FROM t WHERE id IN (1, 4)");
    assert_eq!(rows(&h, "SELECT * FROM v ORDER BY iv, id"), ["-infinity|3", "30 days|5", "infinity|2", "|6"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Recovery re-plans every view, and delta join paths are ordered by the
/// current sizes of their inputs. When the sizes changed since the view was
/// created, the re-planned joins look up other indexes, the view's
/// arrangements no longer match the checkpointed ones and are hydrated
/// afresh, from the current state: the view's contents are those of its
/// query, as before the restart.
#[test]
fn views_replanned_at_restart_after_table_sizes_change() {
    let dir = tmpdir("replan-sizes");
    let cfg = coord::Config { data_dir: dir.clone(), sync: false, tick: None, ..Default::default() };
    let running = coord::start(cfg.clone()).unwrap();
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE a (id int PRIMARY KEY, k1 int, k3 int)");
    exec(&h, "CREATE TABLE b (id int PRIMARY KEY, k1 int, k2 int)");
    exec(&h, "CREATE TABLE c (id int PRIMARY KEY, k2 int, k3 int)");
    exec(&h, "INSERT INTO a SELECT g, g % 3, g % 5 FROM generate_series(1, 10) g");
    exec(&h, "INSERT INTO b SELECT g, g % 3, g % 4 FROM generate_series(1, 5) g");
    exec(&h, "INSERT INTO c SELECT g, g % 4, g % 5 FROM generate_series(1, 50) g");
    let q = "SELECT count(*) AS n FROM a JOIN b ON a.k1 = b.k1 JOIN c ON b.k2 = c.k2 AND a.k3 = c.k3";
    exec(&h, &format!("CREATE MATERIALIZED VIEW v AS {q}"));
    // b grows, c shrinks: the join order flips
    exec(&h, "DELETE FROM c WHERE id > 3");
    exec(&h, "INSERT INTO b SELECT g, g % 3, g % 4 FROM generate_series(6, 200) g");
    let now = rows(&h, q);
    assert_eq!(rows(&h, "SELECT * FROM v"), now);
    running.shutdown();
    let running = coord::start(cfg).unwrap();
    let h = running.handle.clone();
    assert_eq!(rows(&h, "SELECT * FROM v"), now);
    exec(&h, "DELETE FROM a WHERE id % 2 = 0");
    assert_eq!(rows(&h, "SELECT * FROM v"), rows(&h, q));
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Identical aggregate calls in a query are computed once, but percentiles
/// differing only in their fraction counted as identical: every one
/// returned the first one's percentile (ad hoc and in views).
#[test]
fn percentiles_with_different_fractions() {
    let dir = tmpdir("percentiles");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, g int, v int)");
    exec(&h, "INSERT INTO t SELECT i, i % 2, i * 10 FROM generate_series(1, 11) i");
    let q = "SELECT g, percentile_cont(0) WITHIN GROUP (ORDER BY v), percentile_cont(0.25) WITHIN GROUP (ORDER BY v), \
             percentile_disc(0.5) WITHIN GROUP (ORDER BY v), percentile_disc(1) WITHIN GROUP (ORDER BY v) FROM t GROUP BY g";
    exec(&h, &format!("CREATE MATERIALIZED VIEW v AS {q}"));
    assert_eq!(rows(&h, &format!("{q} ORDER BY g")), ["0|20|40|60|100", "1|10|35|50|110"]);
    exec(&h, "DELETE FROM t WHERE id IN (1, 2)");
    assert_eq!(rows(&h, &format!("{q} ORDER BY g")), ["0|40|55|60|100", "1|30|50|70|110"]);
    assert_eq!(rows(&h, "SELECT * FROM v ORDER BY g"), rows(&h, &format!("{q} ORDER BY g")));
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A set-returning function after an explicit LATERAL dropped its WITH
/// ORDINALITY column (the column list then named one column too many).
#[test]
fn lateral_functions_with_ordinality() {
    let dir = tmpdir("lateral-ordinality");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, a int[])");
    exec(&h, "INSERT INTO t VALUES (1, '{5,6}'), (2, '{7}')");
    let q = "SELECT t.id, x.v, x.n FROM t, LATERAL unnest(t.a) WITH ORDINALITY AS x(v, n)";
    exec(&h, &format!("CREATE MATERIALIZED VIEW v AS {q}"));
    assert_eq!(rows(&h, &format!("{q} ORDER BY 1, 3")), ["1|5|1", "1|6|2", "2|7|1"]);
    exec(&h, "UPDATE t SET a = '{8,9,10}' WHERE id = 2");
    assert_eq!(rows(&h, "SELECT * FROM v ORDER BY 1, 3"), ["1|5|1", "1|6|2", "2|8|1", "2|9|2", "2|10|3"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A predicate on a derived table's columns stayed above the projection
/// computing them, so every column was computed for rows the predicate
/// then dropped: an expression failing on such a row failed the query (and
/// the view), where PostgreSQL pushes the predicate into the subquery and
/// never evaluates the column for it.
#[test]
fn predicates_on_derived_columns_filter_before_other_columns() {
    let dir = tmpdir("pushdown");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, c0 int[], c1 float8, c2 int)");
    // || fails for the two-dimensional arrays, which the predicates drop
    exec(&h, "INSERT INTO t VALUES (1, '{{1,2},{3,4}}', 0, NULL), (2, '{5}', 0, 7), (3, '{{6}}', 100, 9), (4, '{8}', 1, 3)");
    let q1 = "SELECT d.*, 1 AS one FROM (SELECT a0.c2 AS x0, array_ndims(a0.c0) AS x1, (a0.c0 || ARRAY[9]) AS x2 FROM t a0 \
              WHERE a0.c1 IN (0::float8, 100::float8)) d WHERE d.x0 IS NOT NULL AND d.x1 = 1";
    let q2 = "SELECT * FROM (SELECT id + 1 AS x, c0 || ARRAY[9] AS y FROM t) d WHERE d.x IN (3, 5)";
    for (i, (q, want)) in [(q1, vec!["7|1|{5,9}|1"]), (q2, vec!["3|{5,9}", "5|{8,9}"])].into_iter().enumerate() {
        assert_eq!(rows(&h, &format!("{q} ORDER BY 1")), want, "{q}");
        exec(&h, &format!("CREATE MATERIALIZED VIEW v{i} AS {q}"));
        assert_eq!(rows(&h, &format!("SELECT * FROM v{i} ORDER BY 1")), want, "view {q}");
    }
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A quantified comparison with a correlated subquery (`x <> ANY (...)`,
/// `x > ALL (...)`) was decorrelated into a mark computed per distinct
/// outer row: whole rows became grouping and join keys, so a view over a
/// wide join (TOASTed text in it) kept several arrangements of every
/// column; one in ivm-difftest grew to gigabytes and never finished
/// hydrating. The mark is now computed per distinct correlation and
/// compared values. Three-valued logic, empty subqueries, duplicate outer
/// rows and exact correlation values (1.0 and 1.00) must come out as in
/// PostgreSQL, ad hoc and maintained.
#[test]
fn quantified_subqueries() {
    let dir = tmpdir("quantified");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE qa (id int PRIMARY KEY, k numeric, x int, big text)");
    exec(&h, "CREATE TABLE qb (id int PRIMARY KEY, k numeric, y int)");
    exec(
        &h,
        "INSERT INTO qa VALUES (1, 1.0, 5, repeat('a', 3000)), (2, 1.00, 5, repeat('b', 3000)), (3, 2, NULL, 'c'), \
         (4, 3, 7, 'd'), (5, 1.0, 9, 'e'), (6, 4, 5, 'f'), (7, 1.0, 5, 'g')",
    );
    exec(&h, "INSERT INTO qb VALUES (1, 1, 5), (2, 1, 6), (3, 2, 1), (4, 3, NULL), (5, 3, 7)");
    let marks = "SELECT a.id, a.x <> ANY (SELECT b.y FROM qb b WHERE b.k = a.k), a.x = ANY (SELECT b.y FROM qb b WHERE b.k = a.k), \
                 a.x > ALL (SELECT b.y FROM qb b WHERE b.k = a.k), a.x + 4 <= ALL (SELECT b.y + length(a.k::text) FROM qb b WHERE b.k = a.k) \
                 FROM qa a";
    let wide = "SELECT count(*), sum(length(big)) FROM qa a WHERE a.x <> ANY (SELECT b.y FROM qb b WHERE b.k = a.k)";
    exec(&h, &format!("CREATE MATERIALIZED VIEW marks AS {marks}"));
    exec(&h, &format!("CREATE MATERIALIZED VIEW wide AS {wide}"));
    let check = |want: &[&str], want_wide: &str| {
        assert_eq!(rows(&h, &format!("{marks} ORDER BY 1")), want);
        assert_eq!(rows(&h, "SELECT * FROM marks ORDER BY 1"), want);
        assert_eq!(rows(&h, wide), [want_wide]);
        assert_eq!(rows(&h, "SELECT * FROM wide"), [want_wide]);
    };
    check(&["1|t|t|f|f", "2|t|t|f|t", "3||||", "4||t|f|f", "5|t|f|t|f", "6|f|f|t|t", "7|t|t|f|f"], "4|6002");
    exec(&h, "UPDATE qb SET y = 9 WHERE id = 2");
    exec(&h, "DELETE FROM qb WHERE id = 4");
    check(&["1|t|t|f|f", "2|t|t|f|t", "3||||", "4|f|t|f|f", "5|t|t|f|f", "6|f|f|t|t", "7|t|t|f|f"], "4|6002");
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Ad hoc execution deduplicated a group's rows for a DISTINCT ordered
/// aggregate in place, so ordered aggregates after it saw only the distinct
/// rows (here mode() saw x = 17 alone). Expected values are PostgreSQL's.
#[test]
fn distinct_ordered_aggregates_leave_other_aggregates_alone() {
    let dir = tmpdir("distinct-ordered-aggs");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, g int, s text, x int)");
    exec(&h, "INSERT INTO t VALUES (1, 1, 'a', 17), (2, 1, 'a', 7), (3, 2, 'a', 1), (4, 2, 'b', NULL)");
    let q = "SELECT g, string_agg(DISTINCT s, '|' ORDER BY s) AS s, mode() WITHIN GROUP (ORDER BY x) AS m, \
             array_agg(x ORDER BY x) AS xs, count(DISTINCT s) AS n FROM t GROUP BY g";
    exec(&h, &format!("CREATE MATERIALIZED VIEW v AS {q}"));
    let want = ["1|a|7|{7,17}|1", "2|a|b|1|{1,NULL}|2"];
    assert_eq!(rows(&h, &format!("{q} ORDER BY g")), want);
    assert_eq!(rows(&h, "SELECT * FROM v ORDER BY g"), want);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A join seeded by a presence change (here the decorrelated EXISTS, whose
/// presence index is keyed by the whole outer row) took the outer row's
/// columns from the canonically decoded key: c0 = 0 instead of 0.000. Rows
/// derived that way did not retract the ones derived from the stored rows,
/// so the renamed row 58 lived on in the view (found by ivm-difftest).
#[test]
fn presence_changes_keep_exact_representations() {
    let dir = tmpdir("presence-repr");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t2 (id int PRIMARY KEY, c0 numeric, r0 int, pr int)");
    exec(&h, "CREATE TABLE t3 (id int PRIMARY KEY)");
    exec(&h, "INSERT INTO t2 VALUES (43, 0.000, 35, 24), (58, 0.000, 5, 16)");
    exec(&h, "INSERT INTO t3 VALUES (4)");
    let q = "SELECT (SELECT count(*) FROM t2 sq WHERE sq.pr = a1.id) AS n, a1.id AS a, a2.id AS b, a1.c0 AS c \
             FROM t2 a1 LEFT JOIN t2 a2 ON a2.r0 = a1.r0 + 30 \
             WHERE a1.c0 IS DISTINCT FROM a2.c0 AND EXISTS (SELECT 1 FROM t3 sq WHERE sq.id < a1.r0 AND sq.id > a1.r0 - 3)";
    exec(&h, &format!("CREATE MATERIALIZED VIEW v AS {q}"));
    for sql in ["UPDATE t2 SET r0 = 2 WHERE id = 43", "UPDATE t2 SET id = id + 1000000 WHERE id = 58", "INSERT INTO t2 VALUES (75, 10, 44, 58)"] {
        exec(&h, sql);
        assert_eq!(rows(&h, "SELECT * FROM v ORDER BY a"), rows(&h, &format!("{q} ORDER BY a")), "after {sql}");
    }
    assert_eq!(rows(&h, "SELECT * FROM v"), ["0|1000058||0.000"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Dropping a view cancelled every subscription on its dataflow node, also
/// those to other views sharing the node (here an identical view): they got
/// a "was dropped" error and no further changes.
#[test]
fn dropping_a_view_leaves_subscriptions_to_its_twin_alone() {
    let dir = tmpdir("subscribe-twin");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, a int)");
    exec(&h, "INSERT INTO t VALUES (1, 1), (2, 2)");
    exec(&h, "CREATE MATERIALIZED VIEW a AS SELECT DISTINCT a FROM t");
    exec(&h, "CREATE MATERIALIZED VIEW b AS SELECT DISTINCT a FROM t");
    let ExecResult::Subscribe { rx, .. } = exec(&h, "SUBSCRIBE TO a") else { panic!("not a subscription") };
    exec(&h, "DROP VIEW b");
    exec(&h, "INSERT INTO t VALUES (3, 3)");
    exec(&h, "DELETE FROM t WHERE id = 1");
    let mut acc = std::collections::BTreeMap::new();
    while let Ok(ev) = rx.try_recv() {
        match ev {
            ev @ (SubEvent::Data { .. } | SubEvent::Snapshot { .. }) => {
                for (r, w) in ev.rows().unwrap() {
                    *acc.entry(r.iter().map(format_datum).collect::<Vec<_>>().join("|")).or_insert(0i64) += w;
                }
            }
            SubEvent::Progress { .. } => {}
            SubEvent::Error(e) => panic!("subscription failed: {e}"),
        }
    }
    acc.retain(|_, w| *w != 0);
    assert_eq!(acc.keys().cloned().collect::<Vec<_>>(), rows(&h, "SELECT * FROM a ORDER BY 1"));
    assert_eq!(rows(&h, "SELECT * FROM a ORDER BY 1"), ["2", "3"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Subscriptions to a view go on across a replacement that keeps its
/// columns, with the change to the new contents. One changing them went on
/// too, streaming rows of the new shape under the old column list (wider
/// rows, text where an int was announced); it now ends with an error, as
/// subscriptions to a table that changes its columns upstream do.
#[test]
fn replacing_a_view_with_other_columns_ends_its_subscriptions() {
    let dir = tmpdir("replace-columns");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, a int, b text)");
    exec(&h, "INSERT INTO t VALUES (1, 10, 'x'), (2, 20, 'y')");
    exec(&h, "CREATE MATERIALIZED VIEW v AS SELECT id, a FROM t");
    let ExecResult::Subscribe { rx, .. } = exec(&h, "SUBSCRIBE TO v") else { panic!("not a subscription") };
    // same columns: the subscription follows
    exec(&h, "CREATE OR REPLACE MATERIALIZED VIEW v AS SELECT id, a * 2 AS a FROM t");
    exec(&h, "CREATE OR REPLACE MATERIALIZED VIEW v AS SELECT b, id, a FROM t");
    exec(&h, "INSERT INTO t VALUES (3, 30, 'z')");
    let mut acc = std::collections::BTreeMap::new();
    let mut error = None;
    while let Ok(ev) = rx.try_recv() {
        match ev {
            ev @ (SubEvent::Data { .. } | SubEvent::Snapshot { .. }) => {
                assert!(error.is_none(), "rows after the error");
                for (r, w) in ev.rows().unwrap() {
                    assert_eq!(r.len(), 2, "a row of another shape: {r:?}");
                    *acc.entry(r.iter().map(format_datum).collect::<Vec<_>>().join("|")).or_insert(0i64) += w;
                }
            }
            SubEvent::Progress { .. } => {}
            SubEvent::Error(e) => error = Some(e.code),
        }
    }
    acc.retain(|_, w| *w != 0);
    assert_eq!(acc.keys().cloned().collect::<Vec<_>>(), ["1|20", "2|40"], "the stream before the second replacement");
    assert_eq!(error.as_deref(), Some("55000"));
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Replacing a view moved every subscription on its dataflow node to the
/// new definition, also those to another view sharing the node (here an
/// identical view b): they then streamed the replacement's changes.
#[test]
fn replacing_a_view_leaves_subscriptions_to_its_twin_alone() {
    let dir = tmpdir("replace-twin");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, a int)");
    exec(&h, "INSERT INTO t VALUES (1, 1), (2, 2)");
    exec(&h, "CREATE MATERIALIZED VIEW a AS SELECT DISTINCT a FROM t");
    exec(&h, "CREATE MATERIALIZED VIEW b AS SELECT DISTINCT a FROM t");
    let ExecResult::Subscribe { rx, .. } = exec(&h, "SUBSCRIBE TO b") else { panic!("not a subscription") };
    exec(&h, "CREATE OR REPLACE MATERIALIZED VIEW a AS SELECT DISTINCT a + 10 AS a FROM t");
    exec(&h, "INSERT INTO t VALUES (3, 3)");
    let mut acc = std::collections::BTreeMap::new();
    while let Ok(ev) = rx.try_recv() {
        if matches!(ev, SubEvent::Data { .. } | SubEvent::Snapshot { .. }) {
            for (r, w) in ev.rows().unwrap() {
                *acc.entry(r.iter().map(format_datum).collect::<Vec<_>>().join("|")).or_insert(0i64) += w;
            }
        }
    }
    acc.retain(|_, w| *w != 0);
    assert_eq!(acc.keys().cloned().collect::<Vec<_>>(), ["1", "2", "3"]);
    assert_eq!(rows(&h, "SELECT * FROM a ORDER BY 1"), ["11", "12", "13"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Reads that stop at their LIMIT — in the order of an arrangement that is
/// the ORDER BY (also within an equality on a key prefix, under computed
/// columns, and after deletes), or keeping only the top rows of another
/// order — return what sorting the whole result returns. The
/// reference queries order by `+ 0` expressions, which no arrangement is in.
#[test]
fn limited_reads_match_sorts() {
    let dir = tmpdir("limited-reads");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, g int, v text)");
    let mut rng = Rng(0x0BAD_5EED_1234_5678);
    let vals: Vec<String> = (1..=2000)
        .map(|i| {
            let g = if i % 50 == 0 { "NULL".to_string() } else { rng.below(20).to_string() };
            format!("({i}, {g}, 'v{}')", rng.below(1000))
        })
        .collect();
    exec(&h, &format!("INSERT INTO t VALUES {}", vals.join(", ")));
    exec(&h, "CREATE INDEX t_g ON t (g, id)");
    // (in the ORDER BY clause, and an equality that an index could serve)
    let slow = |q: &str| {
        let (head, tail) = q.split_once(" ORDER BY ").unwrap();
        let tail = tail.replace("id", "id + 0").replace("g ", "g + 0 ").replace("g,", "g + 0,").replace("v ", "v || '' ");
        format!("{} ORDER BY {tail}", head.replace("g = ", "g + 0 = "))
    };
    let mut n = 0;
    for q in [
        "SELECT * FROM t ORDER BY id LIMIT 7",
        "SELECT id, v FROM t ORDER BY id OFFSET 13 LIMIT 5",
        "SELECT id FROM t WHERE id > 1500 ORDER BY id LIMIT 3",
        "SELECT id, v FROM t WHERE g = 3 ORDER BY id LIMIT 4",
        "SELECT g, id FROM t ORDER BY g, id LIMIT 9",
        "SELECT g, id FROM t ORDER BY g, id OFFSET 1990 LIMIT 20",
        "SELECT g, id FROM t ORDER BY g NULLS FIRST, id LIMIT 6",
        "SELECT g, id FROM t ORDER BY g DESC, id LIMIT 5",
        "SELECT id, id * 2 AS d FROM t ORDER BY id LIMIT 3",
        "SELECT v, id FROM t ORDER BY v DESC, id LIMIT 8",
        "SELECT t.id, u.v FROM t JOIN t u ON u.id = t.id + 1 ORDER BY t.id LIMIT 4",
    ] {
        let (fast, reference) = (rows(&h, q), rows(&h, &slow(q)));
        assert_eq!(fast, reference, "{q}");
        n += fast.len();
    }
    // with ties, the set of rows is determined
    for q in ["SELECT g, id FROM t ORDER BY g FETCH FIRST 3 ROWS WITH TIES", "SELECT g, id FROM t ORDER BY g DESC FETCH FIRST 1 ROW WITH TIES"] {
        let (mut fast, mut reference) = (rows(&h, q), rows(&h, &slow(q)));
        fast.sort();
        reference.sort();
        assert_eq!(fast, reference, "{q}");
        assert!(fast.len() > 3, "{q}: {fast:?}");
    }
    // without ORDER BY, any rows that pass
    assert_eq!(rows(&h, "SELECT id FROM t WHERE v LIKE 'v1%' LIMIT 5").len(), 5);
    assert_eq!(rows(&h, "SELECT * FROM t LIMIT 0"), Vec::<String>::new());
    assert_eq!(n, 64);
    // after deletes
    exec(&h, "DELETE FROM t WHERE id % 2 = 0");
    let q = "SELECT id, g FROM t ORDER BY id LIMIT 6";
    assert_eq!(rows(&h, q), rows(&h, &slow(q)));
    assert_eq!(rows(&h, q).len(), 6);
    assert_eq!(rows(&h, "SELECT id FROM t ORDER BY id LIMIT 2"), ["1", "3"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A subscription's snapshot comes in chunks read from the arrangement as
/// the session takes them (it used to be built whole on the engine thread
/// and sent as one event).
#[test]
fn subscription_snapshots_come_in_chunks() {
    let dir = tmpdir("sub-chunks");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, a int)");
    exec(&h, "INSERT INTO t SELECT g, g % 7 FROM generate_series(1, 25000) g");
    let ExecResult::Subscribe { rx, .. } = exec(&h, "SUBSCRIBE TO t") else { panic!("not a subscription") };
    let mut sizes = Vec::new();
    loop {
        match rx.recv_timeout(std::time::Duration::from_secs(10)).expect("an event") {
            SubEvent::Data { rows, .. } => sizes.push(rows.len()),
            SubEvent::Snapshot { rows, .. } => sizes.extend(rows.map(|c| c.unwrap().len())),
            SubEvent::Progress { .. } => break,
            SubEvent::Error(e) => panic!("subscription failed: {e}"),
        }
    }
    assert_eq!(sizes.iter().sum::<usize>(), 25000);
    assert!(sizes.len() >= 3 && sizes.iter().all(|n| *n <= 10_000), "{sizes:?}");
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Reads through arrangements (key ranges, column subsets, index lookups
/// for joins) return what a plain scan returns. The reference queries hide
/// the columns behind `+ 0`, which no index can serve.
#[test]
fn indexed_reads_match_scans() {
    let dir = tmpdir("indexed-reads");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE a (id int PRIMARY KEY, region text, v numeric)");
    exec(&h, "CREATE TABLE t (id bigint PRIMARY KEY, a_id int, kind text, amount numeric)");
    let mut vals = Vec::new();
    for i in 1..=60 {
        vals.push(format!("({i}, 'r{}', {}.{})", i % 5, i * 3, i % 4));
    }
    exec(&h, &format!("INSERT INTO a VALUES {}", vals.join(", ")));
    let mut rng = Rng(0x1234_5678_9ABC_DEF1);
    let mut vals = Vec::new();
    for i in 1..=600 {
        // some rows reference no account, some a missing one
        let a_id = if i % 37 == 0 { "NULL".to_string() } else { (1 + rng.below(70)).to_string() };
        vals.push(format!("({i}, {a_id}, 'k{}', {}.{:02})", rng.below(4), rng.below(500), rng.below(100)));
    }
    exec(&h, &format!("INSERT INTO t VALUES {}", vals.join(", ")));
    // arrangements: per_a by id, t by a_id (from the join), and an index
    exec(
        &h,
        "CREATE MATERIALIZED VIEW per_a AS SELECT a.id, a.region, count(t.id) AS n, sum(t.amount) AS total \
         FROM a LEFT JOIN t ON t.a_id = a.id GROUP BY a.id, a.region",
    );
    exec(&h, "CREATE INDEX t_kind ON t (kind)");
    let sorted = |q: &str| {
        let mut r = rows(&h, q);
        r.sort();
        r
    };
    let check = |fast: &str, slow: &str| {
        let (x, y) = (sorted(fast), sorted(slow));
        assert_eq!(x, y, "{fast}");
        x.len()
    };
    let mut n = 0;
    for (lo, hi) in [(10, 20), (0, 3), (55, 99), (7, 7), (30, 29)] {
        n += check(
            &format!("SELECT * FROM per_a WHERE id BETWEEN {lo} AND {hi}"),
            &format!("SELECT * FROM per_a WHERE id + 0 BETWEEN {lo} AND {hi}"),
        );
        n += check(
            &format!("SELECT id, kind FROM t WHERE id > {lo} AND id <= {hi} AND kind <> 'k1'"),
            &format!("SELECT id, kind FROM t WHERE id + 0 > {lo} AND id + 0 <= {hi} AND kind <> 'k1'"),
        );
    }
    for q in [
        "SELECT * FROM per_a WHERE id < 4",
        "SELECT * FROM per_a WHERE id >= 58",
        "SELECT region, n FROM per_a WHERE id = 12",
        "SELECT a_id, amount FROM t WHERE a_id = 9",
        "SELECT id FROM t WHERE kind = 'k2' AND id < 100",
    ] {
        n += check(q, &q.replace("id ", "id + 0 ").replace("kind =", "kind || '' ="));
    }
    // joins probing an arrangement of the right side, against hash joins
    for k in [1, 7, 33, 61, 70] {
        n += check(
            &format!("SELECT t.kind, count(*), sum(t.amount) FROM a JOIN t ON t.a_id = a.id WHERE a.id = {k} GROUP BY t.kind"),
            &format!("SELECT t.kind, count(*), sum(t.amount) FROM a JOIN t ON t.a_id + 0 = a.id WHERE a.id = {k} GROUP BY t.kind"),
        );
        n += check(
            &format!("SELECT a.id, t.id, t.kind FROM a LEFT JOIN t ON t.a_id = a.id AND t.kind <> 'k0' WHERE a.id IN ({k}, {})", k + 1),
            &format!("SELECT a.id, t.id, t.kind FROM a LEFT JOIN t ON t.a_id + 0 = a.id AND t.kind <> 'k0' WHERE a.id IN ({k}, {})", k + 1),
        );
        n += check(
            &format!("SELECT a.id FROM a WHERE a.id < {k} AND EXISTS (SELECT 1 FROM t WHERE t.a_id = a.id AND t.amount > 250)"),
            &format!("SELECT a.id FROM a WHERE a.id < {k} AND EXISTS (SELECT 1 FROM t WHERE t.a_id + 0 = a.id AND t.amount > 250)"),
        );
        n += check(
            &format!("SELECT a.id FROM a WHERE a.id < {k} AND NOT EXISTS (SELECT 1 FROM t WHERE t.a_id = a.id AND t.kind = 'k3')"),
            &format!("SELECT a.id FROM a WHERE a.id < {k} AND NOT EXISTS (SELECT 1 FROM t WHERE t.a_id + 0 = a.id AND t.kind = 'k3')"),
        );
    }
    assert!(n > 200, "the queries return rows ({n})");
    // after changes, and through an index built after them
    exec(&h, "DELETE FROM t WHERE id % 3 = 0");
    exec(&h, "UPDATE a SET region = 'moved' WHERE id % 4 = 0");
    let changed = check("SELECT * FROM per_a WHERE id BETWEEN 5 AND 25", "SELECT * FROM per_a WHERE id + 0 BETWEEN 5 AND 25");
    assert_eq!(changed, 21);
    exec(&h, "DROP INDEX t_kind");
    exec(&h, "CREATE INDEX t_kind_a ON t (kind, a_id)");
    let mut m = 0;
    for k in [1, 2] {
        m += check(&format!("SELECT id FROM t WHERE kind = 'k{k}'"), &format!("SELECT id FROM t WHERE kind || '' = 'k{k}'"));
        m += check(
            &format!("SELECT a.id, t.id FROM a JOIN t ON t.kind = 'k' || a.id::text WHERE a.id = {k}"),
            &format!("SELECT a.id, t.id FROM a JOIN t ON t.kind || '' = 'k' || a.id::text WHERE a.id = {k}"),
        );
    }
    assert!(m > 60, "the queries after the changes return rows ({m})");
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
