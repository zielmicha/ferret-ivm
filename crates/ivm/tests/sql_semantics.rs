//! SQL front-end regression tests: results that must match PostgreSQL 17
//! (expected values were checked against a real PostgreSQL), and inputs that
//! must produce errors rather than crash the engine.

use ivm::coord::api::{ExecResult, SessionVars};
use ivm::coord::{self, EngineHandle};
use ivm::types::text::format_datum;
use std::path::PathBuf;

fn tmpdir(name: &str) -> PathBuf {
    let base = std::env::var("IVM_TEST_TMP").unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().into_owned());
    let d = PathBuf::from(base).join(format!("ivm-sql-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn start(name: &str) -> coord::Running {
    let cfg = coord::Config { data_dir: tmpdir(name), sync: false, tick: None, ..Default::default() };
    coord::start(cfg).expect("engine starts")
}

fn try_exec(h: &EngineHandle, sql: &str) -> Result<ExecResult, String> {
    let stmt = ivm::sql::parse_statement(sql).map_err(|e| e.message)?;
    h.execute(stmt, vec![], SessionVars::default(), String::new()).map_err(|e| e.message)
}

fn exec(h: &EngineHandle, sql: &str) {
    try_exec(h, sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// Result rows as `|`-separated text, in result order.
fn try_rows(h: &EngineHandle, sql: &str) -> Result<Vec<String>, String> {
    match try_exec(h, sql)? {
        ExecResult::Query { rows, .. } => {
            Ok(rows.iter().map(|r| r.iter().map(format_datum).collect::<Vec<_>>().join("|")).collect())
        }
        other => panic!("{sql}: expected rows, got {other:?}"),
    }
}

fn rows(h: &EngineHandle, sql: &str) -> Vec<String> {
    try_rows(h, sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// Result type names of a query, as pg_typeof would print them.
fn column_types(h: &EngineHandle, sql: &str) -> Vec<String> {
    match try_exec(h, sql).unwrap_or_else(|e| panic!("{sql}: {e}")) {
        ExecResult::Query { columns, .. } => columns.iter().map(|c| c.ty.name()).collect(),
        other => panic!("{sql}: expected rows, got {other:?}"),
    }
}

#[test]
fn malformed_hex_literal_is_an_error_not_a_crash() {
    let running = start("hexlit");
    let h = running.handle.clone();
    // a multibyte character inside X'...' used to panic the engine thread
    let err = try_rows(&h, "SELECT X'aü'").unwrap_err();
    assert!(err.contains("not a valid hexadecimal digit"), "{err}");
    assert!(try_rows(&h, "SELECT X'zz'").is_err());
    assert_eq!(rows(&h, "SELECT X'0aff'"), vec!["\\x0aff"]);
    // the engine still answers
    assert_eq!(rows(&h, "SELECT 1"), vec!["1"]);
    running.shutdown();
}

/// t1 / t2 with NULLs, duplicates and numerics of differing scale.
fn setup_t1_t2(h: &EngineHandle) {
    exec(h, "CREATE TABLE t1 (id int PRIMARY KEY, a int, b int, c numeric, s text, d date, ts timestamp)");
    exec(h, "CREATE TABLE t2 (id int PRIMARY KEY, x int, y int, c numeric, s text)");
    exec(
        h,
        "INSERT INTO t1 VALUES (1, 1, 10, 1.0, 'a', '2024-01-01', '2024-01-01 10:00'), \
         (2, 2, NULL, 1.00, 'b', '2024-01-02', '2024-01-01 00:00'), (3, NULL, 30, 2.5, NULL, NULL, '2024-01-02 12:00'), \
         (4, 2, 20, NULL, 'a', '2024-01-01', NULL), (5, 3, 30, 3, 'c', '2024-01-03', '2024-01-03 00:00')",
    );
    exec(
        h,
        "INSERT INTO t2 VALUES (1, 1, 100, 1.000, 'a'), (2, 2, NULL, 2.5, 'x'), (3, 2, 200, 1, NULL), \
         (4, NULL, 300, NULL, 'b'), (5, 5, 500, 3.0, 'c')",
    );
}

#[test]
fn having_without_group_by_filters_the_single_row() {
    let running = start("having-global");
    let h = running.handle.clone();
    setup_t1_t2(&h);
    // a constant HAVING used to be pushed below the aggregate, which then
    // produced its row for the (now empty) input anyway
    assert_eq!(rows(&h, "SELECT count(*) FROM t1 HAVING false"), Vec::<String>::new());
    assert_eq!(rows(&h, "SELECT count(*) FROM t1 HAVING 1 = 0"), Vec::<String>::new());
    assert_eq!(rows(&h, "SELECT a, count(*) FROM t1 GROUP BY ROLLUP (a) HAVING false"), Vec::<String>::new());
    assert_eq!(rows(&h, "SELECT count(*) FROM t1 HAVING true"), vec!["5"]);
    assert_eq!(rows(&h, "SELECT count(*), sum(a) FROM t1 WHERE false"), vec!["0|"]);
    exec(&h, "CREATE MATERIALIZED VIEW hv AS SELECT count(*) AS n FROM t1 HAVING 1 > 2");
    assert_eq!(rows(&h, "SELECT * FROM hv"), Vec::<String>::new());
    running.shutdown();
}

#[test]
fn outer_join_kept_for_any_all_over_empty_arrays() {
    let running = start("anyall-outer");
    let h = running.handle.clone();
    setup_t1_t2(&h);
    // NULL = ALL('{}') is true and NULL = ANY('{}') is false, so these WHERE
    // clauses do not reject the null-extended rows of the LEFT JOIN
    let all = "SELECT t1.id, t2.id FROM t1 LEFT JOIN t2 ON t1.a = t2.x AND t2.y > 150 \
               WHERE t2.y = ALL ('{}'::int[]) ORDER BY 1, 2";
    assert_eq!(rows(&h, all), vec!["1|", "2|3", "3|", "4|3", "5|"]);
    let not_any = "SELECT t1.id, t2.id FROM t1 LEFT JOIN t2 ON t1.a = t2.x AND t2.y > 150 \
                   WHERE NOT (t2.y = ANY ('{}'::int[])) ORDER BY 1, 2";
    assert_eq!(rows(&h, not_any), vec!["1|", "2|3", "3|", "4|3", "5|"]);
    // a NULL array still makes the comparison NULL: the join may become inner
    let null_arr = "SELECT t1.id, t2.id FROM t1 LEFT JOIN t2 ON t1.a = t2.x WHERE t2.y = ANY (NULL::int[]) ORDER BY 1";
    assert_eq!(rows(&h, null_arr), Vec::<String>::new());
    running.shutdown();
}

#[test]
fn untyped_literals_in_query_outputs_resolve_to_text() {
    let running = start("unknown-outputs");
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE d (id int PRIMARY KEY, day date)");
    exec(&h, "CREATE VIEW vu AS SELECT 'abc' AS x, NULL AS n");
    exec(&h, "CREATE MATERIALIZED VIEW mu AS SELECT 'abc' AS x FROM d");
    for sql in [
        "SELECT pg_typeof(x) FROM (SELECT 'abc' AS x) s",
        "WITH c AS (SELECT 'abc' AS x) SELECT pg_typeof(x) FROM c",
        "SELECT pg_typeof(x) FROM vu",
        "SELECT pg_typeof(n) FROM vu",
        "SELECT pg_typeof(x) FROM (VALUES ('a')) v(x)",
    ] {
        assert_eq!(rows(&h, sql), vec!["text"], "{sql}");
    }
    assert_eq!(column_types(&h, "SELECT 'abc', NULL"), vec!["text", "text"]);
    assert_eq!(column_types(&h, "SELECT * FROM mu"), vec!["text"]);
    // set operations still unify untyped branches with the others
    assert_eq!(rows(&h, "SELECT '1' UNION SELECT 2 ORDER BY 1"), vec!["1", "2"]);
    assert_eq!(column_types(&h, "SELECT '1' UNION SELECT 2"), vec!["integer"]);
    assert_eq!(column_types(&h, "(SELECT '1') UNION ALL (SELECT 2)"), vec!["integer"]);
    // and INSERT ... SELECT still converts untyped values to the column type
    exec(&h, "INSERT INTO d SELECT 1, '2024-01-01'");
    assert_eq!(rows(&h, "SELECT day FROM d"), vec!["2024-01-01"]);
    running.shutdown();
}

#[test]
fn at_time_zone_and_generate_series_resolve_like_postgresql() {
    let running = start("tz-series");
    let h = running.handle.clone();
    // a date converts to timestamptz (the preferred type), so the result is a
    // timestamp in the given zone (it used to be treated as a timestamp)
    let q = "SELECT date '2024-06-01' AT TIME ZONE 'Asia/Tokyo'";
    assert_eq!(rows(&h, q), vec!["2024-06-01 09:00:00"]);
    assert_eq!(column_types(&h, q), vec!["timestamp without time zone"]);
    // untyped bounds take the step's type (used to fail: generate_series(text))
    let q = "SELECT g FROM generate_series('2024-01-01', '2024-01-02', '1 day'::interval) g";
    assert_eq!(rows(&h, q), vec!["2024-01-01 00:00:00+00", "2024-01-02 00:00:00+00"]);
    assert_eq!(column_types(&h, q), vec!["timestamp with time zone"]);
    assert_eq!(rows(&h, "SELECT g FROM generate_series('1', '2', 1) g"), vec!["1", "2"]);
    let err = try_rows(&h, "SELECT g FROM generate_series('1', '2') g").unwrap_err();
    assert!(err.contains("is not unique"), "{err}");
    // numeric bounds and step share one type
    let q = "SELECT g FROM generate_series(1, 2, 0.5) g";
    assert_eq!(rows(&h, q), vec!["1", "1.5", "2.0"]);
    assert_eq!(column_types(&h, q), vec!["numeric"]);
    assert_eq!(column_types(&h, "SELECT * FROM generate_series(1, 3, 2::bigint)"), vec!["bigint"]);
    running.shutdown();
}
