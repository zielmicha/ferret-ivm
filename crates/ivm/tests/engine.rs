//! Engine-level tests through the public API (no Postgres needed).

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

/// Maintained aggregates must equal a recompute, including when equal group
/// keys and arguments come in several representations (numeric scales).
#[test]
fn aggregates_match_recompute_with_mixed_representations() {
    let dir = tmpdir("mixed-repr");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, k numeric, g int, v numeric, b bool)");
    let views = [
        "SELECT k, count(*) AS n, sum(v) AS s, min(v) AS mn, max(v) AS mx, avg(v) AS a FROM t GROUP BY k",
        "SELECT g, k, count(DISTINCT v) AS d, count(*) AS n FROM t GROUP BY g, k",
        "SELECT (id % 3) AS m, k, max(g) AS mg, min(k) AS mk FROM t WHERE b GROUP BY (id % 3), k",
        "SELECT sum(v) AS s, count(v) AS c, min(k) AS mk, max(k) AS xk FROM t",
    ];
    for (i, v) in views.iter().enumerate() {
        exec(&h, &format!("CREATE MATERIALIZED VIEW v{i} AS {v}"));
    }
    let reprs = ["1", "1.0", "1.00", "2", "2.0", "-0.5", "-0.50", "NULL", "3.000", "3"];
    let mut rng = Rng(0x2545F4914F6CDD1D);
    for step in 0..300 {
        let id = rng.below(40);
        let sql = match rng.below(4) {
            0 => format!("DELETE FROM t WHERE id = {id}"),
            1 => format!("UPDATE t SET v = {} WHERE id = {id}", reprs[rng.below(10) as usize]),
            _ => format!(
                "INSERT INTO t VALUES ({id}, {}, {}, {}, {}) ON CONFLICT DO NOTHING",
                reprs[rng.below(10) as usize],
                rng.below(3),
                reprs[rng.below(10) as usize],
                rng.below(2) == 0
            ),
        };
        exec(&h, &sql);
        for (i, v) in views.iter().enumerate() {
            // values must agree; representations of keys may differ only where
            // several are present, so compare by value with trailing zeros
            let norm = |rows: Vec<String>| {
                let mut r: Vec<String> = rows
                    .into_iter()
                    .map(|row| {
                        row.split('|')
                            .map(|f| if f.contains('.') { f.trim_end_matches('0').trim_end_matches('.').to_string() } else { f.to_string() })
                            .collect::<Vec<_>>()
                            .join("|")
                    })
                    .collect();
                r.sort();
                r
            };
            let got = norm(rows(&h, &format!("SELECT * FROM v{i}")));
            let want = norm(rows(&h, v));
            assert_eq!(got, want, "step {step} after {sql}: view v{i} = {v}");
        }
    }
    running.shutdown();
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
    // as of an earlier GSN
    let gsn: u64 = rows(&h, "SHOW GSN")[0].parse().unwrap();
    exec(&h, "DELETE FROM t WHERE id % 3 = 0");
    exec(&h, "UPDATE a SET region = 'moved' WHERE id % 4 = 0");
    let old = check(
        &format!("SELECT * FROM per_a WHERE id BETWEEN 5 AND 25 AS OF {gsn}"),
        &format!("SELECT * FROM per_a WHERE id + 0 BETWEEN 5 AND 25 AS OF {gsn}"),
    );
    assert_eq!(old, 21);
    check("SELECT * FROM per_a WHERE id BETWEEN 5 AND 25", "SELECT * FROM per_a WHERE id + 0 BETWEEN 5 AND 25");
    // arrangements built after the GSN lack the history before it: AS OF
    // reads use neither their key ranges nor their lookups
    exec(&h, "DROP INDEX t_kind");
    exec(&h, "CREATE INDEX t_kind_a ON t (kind, a_id)");
    let mut m = 0;
    for k in [1, 2] {
        m += check(
            &format!("SELECT id FROM t WHERE kind = 'k{k}' AS OF {gsn}"),
            &format!("SELECT id FROM t WHERE kind || '' = 'k{k}' AS OF {gsn}"),
        );
        m += check(
            &format!("SELECT a.id, t.id FROM a JOIN t ON t.kind = 'k' || a.id::text WHERE a.id = {k} AS OF {gsn}"),
            &format!("SELECT a.id, t.id FROM a JOIN t ON t.kind || '' = 'k' || a.id::text WHERE a.id = {k} AS OF {gsn}"),
        );
    }
    assert!(m > 100, "the AS OF queries return rows ({m})");
    // a relation rebuilt after the GSN has no history before it
    exec(&h, "CREATE OR REPLACE MATERIALIZED VIEW per_a AS SELECT a.id, a.region FROM a");
    for q in [format!("SELECT region FROM per_a WHERE id = 5 AS OF {gsn}"), format!("SELECT id FROM per_a AS OF {gsn}")] {
        let stmt = ivm::sql::parse_statement(&q).unwrap();
        let err = h.execute(stmt, vec![], SessionVars::default(), String::new()).err().map(|e| e.message);
        assert!(err.as_deref().is_some_and(|e| e.contains("before the history")), "{q}: {err:?}");
    }
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
