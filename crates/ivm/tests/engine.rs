//! Engine-level tests through the public API (no PostgreSQL needed): exact
//! semantics of temporal filters, recovery, specific features, and
//! regression tests for bugs (some `#[ignore]`d with the bug described, for
//! bugs that are still open). Randomized comparisons of views with their
//! queries are in selfdiff.rs.

use ivm::coord::api::{ExecResult, SessionVars};
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

fn start(dir: &PathBuf) -> coord::Running {
    let cfg = coord::Config { data_dir: dir.clone(), sync: false, tick: None, ..Default::default() };
    coord::start(cfg).expect("engine starts")
}

fn exec(h: &EngineHandle, sql: &str) -> ExecResult {
    let stmt = ivm::sql::parse_statement(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    h.execute(stmt, vec![], SessionVars::default(), String::new()).unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// Result rows as text, in result order.
fn rows(h: &EngineHandle, sql: &str) -> Vec<String> {
    match exec(h, sql) {
        ExecResult::Query { rows, .. } => {
            rows.iter().map(|r| r.iter().map(format_datum).collect::<Vec<_>>().join("|")).collect()
        }
        other => panic!("{sql}: expected rows, got {other:?}"),
    }
}

/// Log a clock tick at `secs` seconds after 2026-01-01 00:00:00 UTC.
fn tick(h: &EngineHandle, secs: i64) {
    let base = 1_767_225_600_000_000i64; // 2026-01-01T00:00:00Z in Unix microseconds
    h.log(LogRecord::Tick { micros: base + secs * 1_000_000 }).unwrap();
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
    assert_eq!(rows(&h, &format!("{q}")), want);
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
/// afresh: their history is gone, so reads AS OF a GSN before the restart
/// return the hydrated state (nothing, here) instead of what the view held.
#[test]
#[ignore = "bug: recovery re-plans joins from current table sizes; changed plans re-hydrate views and lose their AS OF history"]
fn as_of_history_survives_restart_after_table_sizes_change() {
    let dir = tmpdir("replan-history");
    let cfg = coord::Config { data_dir: dir.clone(), sync: false, tick: None, retention_gsns: 100_000, ..Default::default() };
    let running = coord::start(cfg.clone()).unwrap();
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE a (id int PRIMARY KEY, k1 int, k3 int)");
    exec(&h, "CREATE TABLE b (id int PRIMARY KEY, k1 int, k2 int)");
    exec(&h, "CREATE TABLE c (id int PRIMARY KEY, k2 int, k3 int)");
    exec(&h, "INSERT INTO a SELECT g, g % 3, g % 5 FROM generate_series(1, 10) g");
    exec(&h, "INSERT INTO b SELECT g, g % 3, g % 4 FROM generate_series(1, 5) g");
    exec(&h, "INSERT INTO c SELECT g, g % 4, g % 5 FROM generate_series(1, 50) g");
    exec(&h, "CREATE MATERIALIZED VIEW v AS SELECT count(*) AS n FROM a JOIN b ON a.k1 = b.k1 JOIN c ON b.k2 = c.k2 AND a.k3 = c.k3");
    let gsn = rows(&h, "SHOW GSN")[0].clone();
    let then = rows(&h, "SELECT * FROM v");
    // b grows, c shrinks: the join order flips
    exec(&h, "DELETE FROM c WHERE id > 3");
    exec(&h, "INSERT INTO b SELECT g, g % 3, g % 4 FROM generate_series(6, 200) g");
    assert_eq!(rows(&h, &format!("SELECT * FROM v AS OF {gsn}")), then);
    running.shutdown();
    let running = coord::start(cfg).unwrap();
    let h = running.handle.clone();
    assert_eq!(rows(&h, "SELECT * FROM v"), rows(&h, "SELECT count(*) FROM a JOIN b ON a.k1 = b.k1 JOIN c ON b.k2 = c.k2 AND a.k3 = c.k3"));
    assert_eq!(rows(&h, &format!("SELECT * FROM v AS OF {gsn}")), then, "view v AS OF {gsn} after the restart");
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Errors are data: a view whose rows include an expression error fails to
/// read until the offending rows are gone. Error records are kept per view
/// and collected only from changes after the view was created, and
/// arrangements hold no errors: a view hydrated from an existing node misses
/// that node's current errors, and reading it succeeds with the failing
/// rows missing. Both a view reusing an existing dataflow node (`e1`, `e2`:
/// the same aggregate as `e0`) and a view over a failing view created after
/// the failure (`late`; `early` existed before and fails as it should).
#[test]
#[ignore = "bug: views hydrated from an existing node (shared nodes, views over views) miss its current error records"]
fn views_hydrated_from_failing_nodes_report_their_errors() {
    let dir = tmpdir("shared-errors");
    let running = start(&dir);
    let h = running.handle.clone();
    let code = |sql: &str| {
        let stmt = ivm::sql::parse_statement(sql).unwrap();
        h.execute(stmt, vec![], SessionVars::default(), String::new()).map(|_| ()).map_err(|e| e.code)
    };
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, k int, a int[])");
    let q = "SELECT (a || ARRAY[9]) AS x, count(*) FROM t GROUP BY 1";
    exec(&h, &format!("CREATE MATERIALIZED VIEW e0 AS {q}"));
    exec(&h, "CREATE MATERIALIZED VIEW vin AS SELECT k AS x0, (a || ARRAY[9]) AS x1 FROM t");
    exec(&h, "CREATE MATERIALIZED VIEW early AS SELECT x0, count(*) AS n FROM vin GROUP BY x0");
    exec(&h, "INSERT INTO t VALUES (1, 1, '{1,2}'), (2, 1, '{{1,2},{3,4}}')");
    exec(&h, &format!("CREATE MATERIALIZED VIEW e1 AS {q}"));
    exec(&h, "CREATE MATERIALIZED VIEW e2 AS SELECT (a || ARRAY[9]) AS x, count(*) AS n FROM t GROUP BY 1");
    exec(&h, "CREATE MATERIALIZED VIEW late AS SELECT x0, count(*) AS n FROM vin GROUP BY x0");
    assert_eq!(code(q), Err("2202E".to_string()));
    for v in ["e0", "vin", "early", "e1", "e2", "late"] {
        assert_eq!(code(&format!("SELECT * FROM {v}")), Err("2202E".to_string()), "reading {v}");
    }
    exec(&h, "DELETE FROM t WHERE id = 2");
    for v in ["e0", "e1", "e2"] {
        assert_eq!(rows(&h, &format!("SELECT * FROM {v}")), ["{1,2,9}|1"]);
    }
    for v in ["early", "late"] {
        assert_eq!(rows(&h, &format!("SELECT * FROM {v}")), ["1|1"]);
    }
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
