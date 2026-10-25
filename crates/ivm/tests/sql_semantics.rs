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
    let rec = "WITH RECURSIVE t(n) AS (SELECT 'a' UNION ALL SELECT n || 'b' FROM t WHERE length(n) < 3) SELECT n, pg_typeof(n) FROM t ORDER BY n";
    assert_eq!(rows(&h, rec), vec!["a|text", "ab|text", "abb|text"]);
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

#[test]
fn same_named_output_columns_are_ambiguous_only_if_different() {
    let running = start("orderby-names");
    let h = running.handle.clone();
    setup_t1_t2(&h);
    // used to fail with 'ORDER BY "a" is ambiguous'
    assert_eq!(rows(&h, "SELECT a, a FROM t1 WHERE id < 3 ORDER BY a"), vec!["1|1", "2|2"]);
    assert_eq!(rows(&h, "SELECT *, id FROM t2 WHERE id = 1 ORDER BY id"), vec!["1|1|100|1.000|a|1"]);
    assert_eq!(rows(&h, "SELECT a + 1 AS x, a + 1 AS x FROM t1 GROUP BY x ORDER BY x"), vec!["2|2", "3|3", "4|4", "|"]);
    assert_eq!(rows(&h, "SELECT DISTINCT ON (a) a, a FROM t1 ORDER BY a"), vec!["1|1", "2|2", "3|3", "|"]);
    let err = try_rows(&h, "SELECT a AS x, b AS x FROM t1 ORDER BY x").unwrap_err();
    assert!(err.contains("ambiguous"), "{err}");
    running.shutdown();
}

#[test]
fn aggregates_inside_window_aggregates() {
    let running = start("window-of-agg");
    let h = running.handle.clone();
    setup_t1_t2(&h);
    // used to fail with 'aggregate function calls cannot be nested'
    assert_eq!(
        rows(&h, "SELECT a, count(*), sum(count(*)) OVER () FROM t1 GROUP BY a ORDER BY a"),
        vec!["1|1|5", "2|2|5", "3|1|5", "|1|5"]
    );
    assert_eq!(
        rows(&h, "SELECT a, sum(count(*)) OVER (ORDER BY a), avg(sum(b)) OVER () FROM t1 GROUP BY a ORDER BY a"),
        vec!["1|1|22.5000000000000000", "2|3|22.5000000000000000", "3|4|22.5000000000000000", "|5|22.5000000000000000"]
    );
    exec(&h, "CREATE MATERIALIZED VIEW share AS SELECT a, count(*) * 100 / sum(count(*)) OVER () AS pct FROM t1 GROUP BY a");
    let pct = |v: [&str; 4]| -> Vec<String> { ["1", "2", "3", ""].iter().zip(v).map(|(a, p)| format!("{a}|{p}")).collect() };
    assert_eq!(rows(&h, "SELECT * FROM share ORDER BY a"), pct(["20.0000000000000000", "40.0000000000000000", "20.0000000000000000", "20.0000000000000000"]));
    exec(&h, "INSERT INTO t1 VALUES (6, 2, 0, 0, 'z', NULL, NULL)");
    assert_eq!(rows(&h, "SELECT * FROM share ORDER BY a"), pct(["16.6666666666666667", "50.0000000000000000", "16.6666666666666667", "16.6666666666666667"]));
    // a window function over outer columns is evaluated in the subquery
    assert_eq!(
        rows(&h, "SELECT (SELECT sum(t1.a) OVER () FROM t2 LIMIT 1) FROM t1 WHERE id < 6 ORDER BY 1"),
        vec!["5", "10", "10", "15", ""]
    );
    running.shutdown();
}

#[test]
fn aggregates_of_outer_columns_are_rejected() {
    let running = start("outer-agg");
    let h = running.handle.clone();
    setup_t1_t2(&h);
    // PostgreSQL evaluates max(t1.a) over the rows of the outer query (one
    // result row); evaluating it in the subquery gave one row per t1 row
    for sql in [
        "SELECT (SELECT max(t1.a) FROM t2 LIMIT 1) FROM t1",
        "SELECT id, (SELECT count(*) FILTER (WHERE t1.a > 1) FROM t2) FROM t1",
        "SELECT (SELECT string_agg(t1.s, ',') FROM t2) FROM t1",
    ] {
        let err = try_rows(&h, sql).unwrap_err();
        assert!(err.contains("not supported"), "{sql}: {err}");
    }
    assert!(try_rows(&h, "SELECT a FROM t1 GROUP BY a HAVING EXISTS (SELECT 1 FROM t2 WHERE t2.x = max(t1.b))").is_err());
    // aggregates mixing local and outer columns are local
    assert_eq!(rows(&h, "SELECT (SELECT max(t1.a + t2.x) FROM t2) FROM t1 ORDER BY 1"), vec!["6", "7", "7", "8", ""]);
    assert_eq!(rows(&h, "SELECT (SELECT count(*) FROM t2 WHERE t2.x = t1.a) FROM t1 ORDER BY id"), vec!["1", "2", "0", "2", "0"]);
    running.shutdown();
}

#[test]
fn correlated_scalar_aggregates_keep_casts_of_outer_columns() {
    let running = start("scalar-agg-casts");
    let h = running.handle.clone();
    setup_t1_t2(&h);
    exec(&h, "CREATE TABLE ev (id int PRIMARY KEY, d date, s text)");
    exec(&h, "INSERT INTO ev VALUES (1, '2024-01-01', '1'), (2, '2024-01-02', '2'), (3, '2024-01-02', '2')");
    // the cast used to be stripped from t1.ts::date, so ts was compared with
    // d as timestamps (0 matches for 10:00); t1.a::text failed to bind
    let q = "SELECT id, (SELECT count(*) FROM ev WHERE ev.d = t1.ts::date), \
             (SELECT count(*) FROM ev WHERE ev.s = t1.a::text), \
             (SELECT max(ev.id) FROM ev WHERE ev.d = t1.d + 1) FROM t1 ORDER BY id";
    let want = vec!["1|1|1|3", "2|1|2|", "3|2|0|", "4|0|2|3", "5|0|0|"];
    assert_eq!(rows(&h, q), want);
    exec(&h, &format!("CREATE MATERIALIZED VIEW cnt AS {}", q.replace(" ORDER BY id", "")));
    assert_eq!(rows(&h, "SELECT * FROM cnt ORDER BY id"), want);
    exec(&h, "INSERT INTO ev VALUES (4, '2024-01-03', '3')");
    assert_eq!(rows(&h, "SELECT * FROM cnt ORDER BY id"), vec!["1|1|1|3", "2|1|2|4", "3|2|0|", "4|0|2|3", "5|1|1|"]);
    running.shutdown();
}

#[test]
fn decorrelated_subqueries_see_outer_values_exactly() {
    let running = start("decorrelate-fidelity");
    let h = running.handle.clone();
    setup_t1_t2(&h);
    exec(&h, "CREATE TABLE iv (id int PRIMARY KEY, v interval)");
    exec(&h, "INSERT INTO iv VALUES (1, '1 day'), (2, '24 hours'), (3, '1 mon'), (4, '30 days')");
    // subqueries are evaluated once per distinct outer value: 1.0 and 1.00
    // (or 1 day and 24 hours) are equal, but must not share a result
    let lateral = "SELECT t1.id, s.cc FROM t1, LATERAL (SELECT t1.c AS cc FROM t2 WHERE t2.id = 1) s ORDER BY 1";
    assert_eq!(rows(&h, lateral), vec!["1|1.0", "2|1.00", "3|2.5", "4|", "5|3"]);
    let scalar = "SELECT id, (SELECT t1.c * 2 FROM t2 WHERE t2.id = 1), (SELECT t1.c::text FROM t2 LIMIT 1) FROM t1 ORDER BY id";
    assert_eq!(rows(&h, scalar), vec!["1|2.0|1.0", "2|2.00|1.00", "3|5.0|2.5", "4||", "5|6|3"]);
    let marks = "SELECT id, c IN (SELECT t2.c FROM t2 WHERE t2.c::text <> t1.c::text), \
                 c = ANY (SELECT t2.c * 1.0 FROM t2 WHERE t2.c::text = t1.c::text) FROM t1 ORDER BY id";
    assert_eq!(rows(&h, marks), vec!["1|t|f", "2|t|f", "3|f|t", "4|f|f", "5|t|f"]);
    let intervals = "SELECT id, (SELECT extract(hour FROM iv.v) + count(*) FROM t2) FROM iv ORDER BY id";
    assert_eq!(rows(&h, intervals), vec!["1|5", "2|29", "3|5", "4|5"]);
    // maintained incrementally too
    exec(&h, &format!("CREATE MATERIALIZED VIEW sc AS {}", scalar.replace(" ORDER BY id", "")));
    exec(&h, "INSERT INTO t1 VALUES (6, 7, 70, 1.000, 'q', NULL, NULL)");
    exec(&h, "UPDATE t1 SET c = 2.50 WHERE id = 5");
    assert_eq!(
        rows(&h, "SELECT * FROM sc ORDER BY id"),
        vec!["1|2.0|1.0", "2|2.00|1.00", "3|5.0|2.5", "4||", "5|5.00|2.50", "6|2.000|1.000"]
    );
    running.shutdown();
}

#[test]
fn character_n_compares_and_casts_without_padding() {
    let running = start("bpchar");
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (a int PRIMARY KEY, b text, d char(3))");
    exec(&h, "INSERT INTO t VALUES (1, 'x', 'a'), (2, 'y', 'b '), (3, NULL, NULL)");
    exec(&h, "CREATE TABLE c5 (k char(5) PRIMARY KEY, v int)");
    exec(&h, "INSERT INTO c5 VALUES ('a', 1), ('b ', 2)");
    // IN lists, IS DISTINCT FROM and NULLIF compared the padded values
    assert_eq!(rows(&h, "SELECT a FROM t WHERE d IN ('a', 'b') ORDER BY a"), vec!["1", "2"]);
    assert_eq!(
        rows(&h, "SELECT a, d IN ('a', 'x'), d NOT IN ('b'), d IS DISTINCT FROM 'a', nullif(d, 'a') FROM t ORDER BY a"),
        vec!["1|t|t|f|", "2|f|f|t|b  ", "3|||t|"]
    );
    // joins, IN and EXISTS between character(3) and character(5)
    let want = vec!["1|1", "2|2"];
    assert_eq!(rows(&h, "SELECT t.a, c5.v FROM t JOIN c5 ON t.d = c5.k ORDER BY 1"), want);
    assert_eq!(rows(&h, "SELECT t.a, (SELECT v FROM c5 WHERE c5.k = t.d) FROM t ORDER BY 1"), vec!["1|1", "2|2", "3|"]);
    assert_eq!(rows(&h, "SELECT a FROM t WHERE d IN (SELECT k FROM c5) ORDER BY 1"), vec!["1", "2"]);
    assert_eq!(rows(&h, "SELECT a FROM t WHERE EXISTS (SELECT 1 FROM c5 WHERE c5.k = t.d) ORDER BY 1"), vec!["1", "2"]);
    exec(&h, "CREATE MATERIALIZED VIEW j AS SELECT t.a, c5.v FROM t JOIN c5 ON t.d = c5.k");
    exec(&h, "INSERT INTO t VALUES (4, 'z', 'a')");
    assert_eq!(rows(&h, "SELECT * FROM j ORDER BY a"), vec!["1|1", "2|2", "4|1"]);
    // casts to text drop the padding (and so do functions taking text)
    assert_eq!(
        rows(&h, "SELECT d::text || '#', length(d), octet_length(d), d::varchar(2) || '#', upper(d), d || 1 FROM t WHERE a < 3 ORDER BY a"),
        vec!["a#|1|3|a#|A|a1", "b#|1|3|b#|B|b1"]
    );
    // comparisons with text compare the unpadded value, with varchar as character
    assert_eq!(rows(&h, "SELECT 'a'::char(3) = 'a'::varchar, 'a '::varchar = 'a'::char(3), 'a'::char(3) = 'a  '::text"), vec!["t|t|f"]);
    // the common type of character and text is character
    assert_eq!(rows(&h, "SELECT coalesce(d, b) || '#', pg_typeof(coalesce(d, b)) FROM t WHERE a < 3 ORDER BY a"), vec!["a#|character", "b#|character"]);
    assert_eq!(column_types(&h, "SELECT 'a'::varchar UNION SELECT 'b'::text"), vec!["character varying"]);
    // pattern matching sees the padding, as in PostgreSQL
    assert_eq!(rows(&h, "SELECT 'a'::char(3) LIKE 'a', d ~ 'a  ', d SIMILAR TO 'a  ' FROM t WHERE a = 1"), vec!["f|t|t"]);
    running.shutdown();
}

#[test]
fn float4_arithmetic_with_other_numbers_is_float8() {
    let running = start("float48");
    let h = running.handle.clone();
    // PostgreSQL resolves real op integer/numeric to the float4-float8
    // operators (float48pl, ...): computed and returned in double precision
    let q = "SELECT 1.5::float4 + 1, 0.1::float4 + 1, 0.1::float4 * 3, 0.1::float4 + 0.1::float4, 1::float4 / 3, \
             0.1::float4 - 1.5, 2 * 0.1::float4, 0.1::float4 + 0.2::float8, 0.5::float4 + '1'";
    assert_eq!(
        rows(&h, q),
        vec!["2.5|1.1000000014901161|0.30000000447034836|0.2|0.3333333333333333|-1.3999999985098839|0.20000000298023224|0.30000000149011613|1.5"]
    );
    assert_eq!(
        column_types(&h, q),
        vec!["double precision", "double precision", "double precision", "real", "double precision", "double precision", "double precision", "double precision", "real"]
    );
    running.shutdown();
}

#[test]
fn nullif_keeps_the_type_of_its_first_argument() {
    let running = start("nullif-type");
    let h = running.handle.clone();
    setup_t1_t2(&h);
    // used to return timestamps (the type '=' coerced both arguments to)
    assert_eq!(
        rows(&h, "SELECT nullif(d, '2024-01-02 00:00'::timestamp), pg_typeof(nullif(d, '2024-01-02'::timestamp)) FROM t1 ORDER BY id"),
        vec!["2024-01-01|date", "|date", "|date", "2024-01-01|date", "2024-01-03|date"]
    );
    assert_eq!(
        rows(&h, "SELECT nullif(1::int2, 2), pg_typeof(nullif(1::int2, 2)), nullif(0.5::float4, 0.5::float8), \
                  pg_typeof(nullif(0.1::float4, 0.5::float8)), nullif(0.1::float4, 0.5::float8), nullif(2, 2::int8)"),
        vec!["1|smallint||real|0.1|"]
    );
    // without a cross-type operator the arguments share the common type
    assert_eq!(rows(&h, "SELECT nullif(1, 2.5), pg_typeof(nullif(1, 2.5))"), vec!["1|numeric"]);
    running.shutdown();
}

fn column_names(h: &EngineHandle, sql: &str) -> Vec<String> {
    match try_exec(h, sql).unwrap_or_else(|e| panic!("{sql}: {e}")) {
        ExecResult::Query { columns, .. } => columns.iter().map(|c| c.name.clone()).collect(),
        other => panic!("{sql}: expected rows, got {other:?}"),
    }
}

#[test]
fn explicit_casts_of_literals_truncate() {
    let running = start("cast-truncate");
    let h = running.handle.clone();
    // used to fail with 'value too long' (assignment rules)
    assert_eq!(rows(&h, "SELECT 'abcdef'::varchar(3), CAST('abcdef' AS char(2)), varchar(3) 'abcdef'"), vec!["abc|ab|abc"]);
    assert_eq!(rows(&h, "SELECT 'ab'::char(4) || '#', 'abc   '::varchar(3)::text || '#'"), vec!["ab#|abc#"]);
    // assignment still rejects them
    exec(&h, "CREATE TABLE v3 (id int PRIMARY KEY, s varchar(3))");
    assert!(try_exec(&h, "INSERT INTO v3 VALUES (1, 'abcdef')").is_err());
    running.shutdown();
}

#[test]
fn output_column_names_follow_postgresql() {
    let running = start("colnames");
    let h = running.handle.clone();
    setup_t1_t2(&h);
    let q = "SELECT CASE WHEN a > 1 THEN 1 ELSE b END, CASE WHEN a > 1 THEN b END, a::text::int, 1::text::int, '{}'::int[], \
             (SELECT b FROM t1 LIMIT 1)::text, true::int, date '2024-01-01'::text, coalesce(a, 1)::text, \
             trim(leading 'x' from 'xa'), trim(trailing 'x' from 'ax'), x.y, substr('ab', 2), substring('ab', 2), \
             (ARRAY[1, 2])[1], (ARRAY[s])[1:1] FROM t1, (SELECT 1 AS y) x LIMIT 1";
    assert_eq!(
        column_names(&h, q),
        vec!["b", "case", "a", "int4", "int4", "b", "int4", "text", "coalesce", "ltrim", "rtrim", "y", "substr", "substring", "array", "array"]
    );
    running.shutdown();
}

#[test]
fn binder_name_resolution_corner_cases() {
    let running = start("names");
    let h = running.handle.clone();
    setup_t1_t2(&h);
    // GROUP BY prefers this query's FROM columns, then output aliases, and
    // only then outer columns (t1.b used to hide the alias b)
    assert_eq!(
        rows(&h, "SELECT id, (SELECT count(*) FROM (SELECT x + 1 AS b FROM t2 GROUP BY b) z) FROM t1 ORDER BY id"),
        vec!["1|4", "2|4", "3|4", "4|4", "5|4"]
    );
    // an alias on a USING join names its merged column (used to be ambiguous)
    assert_eq!(rows(&h, "SELECT j.id, j.a FROM (t1 JOIN t2 USING (id)) AS j ORDER BY 1 LIMIT 2"), vec!["1|1", "2|2"]);
    assert_eq!(
        column_names(&h, "SELECT j.* FROM (t1 JOIN t2 USING (id)) AS j"),
        vec!["id", "a", "b", "c", "s", "d", "ts", "x", "y", "c", "s"]
    );
    assert_eq!(column_names(&h, "SELECT t1.* FROM t1 JOIN t2 USING (id)").len(), 7);
    // WITH RECURSIVE without a self-reference (the CTE name only occurs as a
    // column alias) used to be rejected
    assert_eq!(rows(&h, "WITH RECURSIVE t AS (SELECT 1 AS t) SELECT * FROM t"), vec!["1"]);
    assert_eq!(rows(&h, "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT * FROM r ORDER BY n"), vec!["1", "2", "3"]);
    assert!(try_rows(&h, "WITH RECURSIVE r AS (SELECT * FROM r) SELECT * FROM r").is_err());
    running.shutdown();
}

#[test]
fn interval_field_qualifiers_and_int2_shifts() {
    let running = start("interval-fields");
    let h = running.handle.clone();
    // the fields restrict the value (they used to be ignored but for
    // unit-less numbers)
    assert_eq!(
        rows(&h, "SELECT interval '1 day 2 hours' day, interval '2 years 3 months' year, interval '1:30' hour to minute, \
                  interval '100' minute, interval '1 day 02:03:04.5678' day to second(2)"),
        vec!["1 day|2 years|01:30:00|01:40:00|1 day 02:03:04.57"]
    );
    assert_eq!(rows(&h, "SELECT '1 day 2 hours'::interval day, '1 year 2 months'::interval month"), vec!["1 day|1 year 2 mons"]);
    // smallint shifts like C ints (1 << 16 used to give 1)
    assert_eq!(rows(&h, "SELECT 1::int2 << 16, 1::int2 << 15, (-8)::int2 >> 1, 1 << 33"), vec!["0|-32768|-4|2"]);
    running.shutdown();
}

#[test]
fn distinct_on_with_expressions_outside_the_select_list() {
    let running = start("distinct-on");
    let h = running.handle.clone();
    setup_t1_t2(&h);
    // both used to be rejected ('must appear in the select list')
    assert_eq!(rows(&h, "SELECT DISTINCT ON (a + 1) a, id FROM t1 ORDER BY a + 1, id"), vec!["1|1", "2|2", "3|5", "|3"]);
    assert_eq!(rows(&h, "SELECT DISTINCT ON (b) id FROM t1 ORDER BY b, id"), vec!["1", "4", "3", "2"]);
    let q = "SELECT DISTINCT ON (a) a, id FROM t1 ORDER BY a, b DESC NULLS LAST, id";
    assert_eq!(rows(&h, q), vec!["1|1", "2|4", "3|5", "|3"]);
    exec(&h, &format!("CREATE MATERIALIZED VIEW don AS {q}"));
    exec(&h, "UPDATE t1 SET b = 99 WHERE id = 2");
    assert_eq!(rows(&h, "SELECT * FROM don ORDER BY a"), vec!["1|1", "2|2", "3|5", "|3"]);
    // plain DISTINCT still requires ORDER BY expressions in the select list
    assert!(try_rows(&h, "SELECT DISTINCT a FROM t1 ORDER BY b").is_err());
    running.shutdown();
}

#[test]
fn group_by_primary_key_through_using_and_lateral() {
    let running = start("fd");
    let h = running.handle.clone();
    setup_t1_t2(&h);
    // other columns of a table whose primary key is grouped may be used; this
    // failed through USING joins and LATERAL
    for q in [
        "SELECT t1.id, t1.a, count(*) FROM t1 JOIN t2 USING (id) GROUP BY t1.id ORDER BY 1 LIMIT 2",
        "SELECT id, a, count(*) FROM t1 JOIN t2 USING (id) GROUP BY id ORDER BY 1 LIMIT 2",
        "SELECT id, a, count(*) FROM t1 LEFT JOIN t2 USING (id) GROUP BY id ORDER BY 1 LIMIT 2",
    ] {
        assert_eq!(rows(&h, q), vec!["1|1|1", "2|2|1"], "{q}");
    }
    assert_eq!(
        rows(&h, "SELECT t1.id, t1.a, l.n FROM t1, LATERAL (SELECT count(*) n FROM t2 WHERE t2.x = t1.a) l GROUP BY t1.id, l.n ORDER BY 1 LIMIT 2"),
        vec!["1|1|1", "2|2|2"]
    );
    // but not through a FULL JOIN's merged column (a COALESCE)
    assert!(try_rows(&h, "SELECT id, a FROM t1 FULL JOIN t2 USING (id) GROUP BY id").is_err());
    // (PostgreSQL also rejects derived tables; plain column copies of a base
    // table keep the dependency, so ivm accepts them)
    assert_eq!(rows(&h, "SELECT s.id, s.a FROM (SELECT * FROM t1 WHERE id < 3) s GROUP BY s.id ORDER BY 1"), vec!["1|1", "2|2"]);
    assert!(try_rows(&h, "SELECT s.id, s.a FROM (SELECT DISTINCT * FROM t1) s GROUP BY s.id").is_err());
    running.shutdown();
}

#[test]
fn reg_types_accept_names() {
    let running = start("regclass");
    let h = running.handle.clone();
    setup_t1_t2(&h);
    // used to fail: invalid input syntax for type oid: "t1"
    assert_eq!(
        rows(&h, "SELECT attname FROM pg_attribute WHERE attrelid = 't1'::regclass AND attnum > 0 ORDER BY attnum"),
        vec!["id", "a", "b", "c", "s", "d", "ts"]
    );
    assert_eq!(
        rows(&h, "SELECT 'public.t1'::regclass = (SELECT oid FROM pg_class WHERE relname = 't1'), '\"t1\"'::regclass = 't1'::regclass"),
        vec!["t|t"]
    );
    assert!(try_rows(&h, "SELECT 'nope'::regclass").unwrap_err().contains("does not exist"));
    assert_eq!(rows(&h, "SELECT 'integer'::regtype::oid, 'int4'::regtype::oid, 'text[]'::regtype::oid, 'public'::regnamespace::oid"), vec!["23|23|1009|2200"]);
    // OIDs still print as names
    assert_eq!(rows(&h, "SELECT (SELECT oid FROM pg_class WHERE relname = 't2')::regclass"), vec!["t2"]);
    // a reg* cast in the select list prints the name, also when it read one;
    // system catalogs have PostgreSQL's fixed OIDs
    let q = "SELECT 't1'::regclass, 'pg_class'::regclass, 'pg_catalog.pg_type'::regclass::oid, 'int4'::regtype, 1259::regclass";
    assert_eq!(rows(&h, q), vec!["t1|pg_class|1247|integer|pg_class"]);
    assert_eq!(column_names(&h, q), vec!["regclass", "regclass", "oid", "regtype", "regclass"]);
    running.shutdown();
}

#[test]
fn current_setting_reports_server_settings() {
    let running = start("current-setting");
    let h = running.handle.clone();
    assert_eq!(
        rows(&h, "SELECT current_setting('server_version_num'), current_setting('TimeZone'), current_setting('DateStyle'), current_setting('nope', true)"),
        vec!["170000|UTC|ISO, MDY|"]
    );
    assert!(try_rows(&h, "SELECT current_setting('nope')").unwrap_err().contains("unrecognized configuration parameter"));
    running.shutdown();
}

#[test]
fn scalar_subquery_cardinality_errors_only_where_evaluated() {
    let running = start("scalar-cardinality");
    let h = running.handle.clone();
    setup_t1_t2(&h);
    // PostgreSQL only evaluates the subquery where CASE (COALESCE) reaches
    // it; ivm computed it for every row and failed for a = 2 (two rows)
    assert_eq!(
        rows(&h, "SELECT id, CASE WHEN a = 1 THEN (SELECT x FROM t2 WHERE t2.x = t1.a) END FROM t1 ORDER BY 1"),
        vec!["1|1", "2|", "3|", "4|", "5|"]
    );
    assert_eq!(rows(&h, "SELECT CASE WHEN false THEN (SELECT x FROM t2) END, coalesce(1, (SELECT x FROM t2))"), vec!["|1"]);
    // where the value is used, more than one row is still an error
    for q in [
        "SELECT id, (SELECT x FROM t2 WHERE t2.x = t1.a) FROM t1",
        "SELECT (SELECT x FROM t2)",
        "SELECT id FROM t1 WHERE a = (SELECT x FROM t2 WHERE t2.x = t1.a)",
    ] {
        assert!(try_rows(&h, q).unwrap_err().contains("more than one row"), "{q}");
    }
    assert_eq!(rows(&h, "SELECT id, (SELECT y FROM t2 WHERE t2.id = t1.id) FROM t1 ORDER BY 1"), vec!["1|100", "2|", "3|200", "4|300", "5|500"]);
    // maintained: the error appears and disappears with the rows needing it
    exec(&h, "CREATE MATERIALIZED VIEW g AS SELECT id, CASE WHEN a = 1 THEN (SELECT y FROM t2 WHERE t2.x = t1.a) END AS v FROM t1");
    assert_eq!(rows(&h, "SELECT * FROM g ORDER BY id"), vec!["1|100", "2|", "3|", "4|", "5|"]);
    exec(&h, "INSERT INTO t2 VALUES (6, 1, 600, NULL, NULL)");
    assert!(try_rows(&h, "SELECT * FROM g").unwrap_err().contains("more than one row"));
    exec(&h, "DELETE FROM t2 WHERE id = 6");
    assert_eq!(rows(&h, "SELECT * FROM g ORDER BY id"), vec!["1|100", "2|", "3|", "4|", "5|"]);
    running.shutdown();
}

#[test]
fn pg_attribute_describes_array_columns_as_varlena() {
    let running = start("attlen");
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE arrs (id int PRIMARY KEY, a int[], t text[])");
    assert_eq!(
        rows(&h, "SELECT attname, attlen, attbyval, attstorage, attndims FROM pg_attribute WHERE attrelid = 'arrs'::regclass AND attnum > 0 ORDER BY attnum"),
        vec!["id|4|t|p|0", "a|-1|f|x|1", "t|-1|f|x|1"]
    );
    running.shutdown();
}

#[test]
fn long_operator_chains_do_not_crash_the_engine() {
    let running = start("deep-exprs");
    let h = running.handle.clone();
    setup_t1_t2(&h);
    // a chain of 2000 '+' overflowed the engine thread's stack and aborted
    // the process; such chains are now refused like PostgreSQL does
    let deep = format!("SELECT {}", vec!["1"; 2000].join(" + "));
    assert!(try_rows(&h, &deep).unwrap_err().contains("stack depth limit exceeded"));
    let ok = format!("SELECT {}", vec!["1"; 900].join(" + "));
    assert_eq!(rows(&h, &ok), vec!["900"]);
    // AND / OR chains are flat: generated queries with thousands of terms work
    let ors = (0..20_000).map(|i| format!("a = {i}")).collect::<Vec<_>>().join(" OR ");
    assert_eq!(rows(&h, &format!("SELECT count(*) FROM t1 WHERE {ors}")), vec!["4"]);
    let ands = vec!["a IS NOT NULL"; 20_000].join(" AND ");
    exec(&h, &format!("CREATE MATERIALIZED VIEW wide AS SELECT id FROM t1 WHERE {ands}"));
    assert_eq!(rows(&h, "SELECT count(*) FROM wide"), vec!["4"]);
    // deep (but accepted) expressions in a maintained view
    exec(&h, &format!("CREATE MATERIALIZED VIEW deep AS SELECT id, {} AS v FROM t1", vec!["a"; 900].join(" + ")));
    exec(&h, "UPDATE t1 SET a = 10 WHERE id = 1");
    assert_eq!(rows(&h, "SELECT v FROM deep WHERE id = 1"), vec!["9000"]);
    // beyond what the parser accepts at all
    let huge = format!("SELECT {}", vec!["1"; 60_000].join("+"));
    assert!(try_rows(&h, &huge).unwrap_err().contains("stack depth limit exceeded"));
    assert_eq!(rows(&h, "SELECT 1"), vec!["1"]);
    running.shutdown();
}

#[test]
fn offset_with_fetch() {
    let running = start("offset-fetch");
    let h = running.handle.clone();
    setup_t1_t2(&h);
    // the SQL standard form used to fail: cannot use both LIMIT and FETCH
    assert_eq!(rows(&h, "SELECT id FROM t1 ORDER BY id OFFSET 1 ROW FETCH NEXT 2 ROWS ONLY"), vec!["2", "3"]);
    assert_eq!(rows(&h, "SELECT id FROM t1 ORDER BY a OFFSET 1 ROWS FETCH FIRST 1 ROW WITH TIES"), vec!["2", "4"]);
    assert!(try_rows(&h, "SELECT id FROM t1 LIMIT 1 FETCH FIRST 1 ROW ONLY").is_err());
    running.shutdown();
}

#[test]
fn like_any_multi_unnest_and_row_any() {
    let running = start("like-any");
    let h = running.handle.clone();
    setup_t1_t2(&h);
    // all three used to be rejected
    assert_eq!(rows(&h, "SELECT id FROM t1 WHERE s LIKE ANY (ARRAY['a%', 'b%']) ORDER BY 1"), vec!["1", "2", "4"]);
    assert_eq!(
        rows(&h, "SELECT id, s NOT LIKE ANY (ARRAY['a%', 'b%']), s ILIKE ANY ('{A%}') FROM t1 ORDER BY 1"),
        vec!["1|t|t", "2|t|f", "3||", "4|t|t", "5|t|f"]
    );
    assert_eq!(rows(&h, "SELECT * FROM unnest(ARRAY[1, 2], ARRAY['a', 'b', 'c']) AS u(n, s) ORDER BY s"), vec!["1|a", "2|b", "|c"]);
    assert_eq!(rows(&h, "SELECT * FROM unnest(ARRAY[1, 2], ARRAY['x']) WITH ORDINALITY ORDER BY 3"), vec!["1|x|1", "2||2"]);
    assert_eq!(rows(&h, "SELECT a, b FROM t1 WHERE (a, b) = ANY (SELECT x, y / 10 FROM t2) ORDER BY 1"), vec!["1|10", "2|20"]);
    assert_eq!(rows(&h, "SELECT 'a_c' ~~ 'a\\_c', 'abc' ~~* 'A%', 'abc' !~~ 'b%'"), vec!["t|t|t"]);
    running.shutdown();
}

#[test]
fn invalid_window_frames_are_rejected() {
    let running = start("frames");
    let h = running.handle.clone();
    setup_t1_t2(&h);
    // these used to run and return NULLs (or a wrong frame, for -1)
    for (frame, msg) in [
        ("ROWS BETWEEN 1 FOLLOWING AND 1 PRECEDING", "frame starting from following row cannot have preceding rows"),
        ("ROWS BETWEEN CURRENT ROW AND 1 PRECEDING", "frame starting from current row cannot have preceding rows"),
        ("ROWS BETWEEN 1 FOLLOWING AND CURRENT ROW", "frame starting from following row cannot have preceding rows"),
        ("ROWS 1 FOLLOWING", "frame starting from following row cannot end with current row"),
        ("ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW", "frame start cannot be UNBOUNDED FOLLOWING"),
        ("ROWS BETWEEN 1 PRECEDING AND UNBOUNDED PRECEDING", "frame end cannot be UNBOUNDED PRECEDING"),
        ("ROWS BETWEEN -1 PRECEDING AND CURRENT ROW", "frame starting offset must not be negative"),
        ("ROWS BETWEEN CURRENT ROW AND -2 FOLLOWING", "frame ending offset must not be negative"),
        ("RANGE BETWEEN -1 PRECEDING AND CURRENT ROW", "invalid preceding or following size in window function"),
    ] {
        let q = format!("SELECT sum(id) OVER (ORDER BY id {frame}) FROM t1");
        let err = try_rows(&h, &q).unwrap_err();
        assert!(err.contains(msg), "{frame}: {err}");
    }
    let err = try_rows(&h, "SELECT count(*) OVER (GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t1").unwrap_err();
    assert!(err.contains("GROUPS mode requires an ORDER BY clause"), "{err}");
    // valid (if empty) frames still work
    assert_eq!(
        rows(&h, "SELECT id, sum(id) OVER (ORDER BY id ROWS BETWEEN 2 FOLLOWING AND 1 FOLLOWING) FROM t1 ORDER BY 1 LIMIT 2"),
        vec!["1|", "2|"]
    );
    running.shutdown();
}

#[test]
fn bool_casts_to_text_and_case_typing() {
    let running = start("bool-text");
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE f1 (id int PRIMARY KEY, b bool, c char(4), s text)");
    exec(&h, "INSERT INTO f1 VALUES (1, true, 'ab', 'x')");
    // the cast is booltext() (true/false), the output function writes t/f
    assert_eq!(
        rows(&h, "SELECT true::text, false::varchar, true::char(5) || '#', (array[true, null])::text[], array[true]::text, format('%s', true), concat(false)"),
        vec!["true|false|true#|{true,NULL}|{t}|t|f"]
    );
    // CASE takes the type of its ELSE first: text here, not character
    assert_eq!(
        rows(&h, "SELECT pg_typeof(CASE WHEN b THEN c ELSE s END), (CASE WHEN id = 1 THEN c ELSE s END)::text || '|' FROM f1"),
        vec!["text|ab|"]
    );
    running.shutdown();
}

#[test]
fn cbrt_matches_postgresql_bit_for_bit() {
    let running = start("cbrt");
    let h = running.handle.clone();
    // PostgreSQL uses glibc's cbrt, which is not correctly rounded
    assert_eq!(
        rows(&h, "SELECT cbrt(5), cbrt(27), cbrt(0.001), cbrt(1e300), cbrt(5e-324), cbrt(-0.0::float8)"),
        vec!["1.7099759466766968|3.0000000000000004|0.1|1e+100|1.7031839360032601e-108|-0"]
    );
    // checksum of 5000 values over many magnitudes, as computed by PostgreSQL 17
    let q = "SELECT md5(string_agg(cbrt(x)::text, ',' ORDER BY g)) FROM \
             (SELECT g, ((g * 1.7 - 2000.3) * power(10::float8, (g % 41) - 20)) AS x FROM generate_series(1, 5000) g) v";
    assert_eq!(rows(&h, q), vec!["0d12115ba9db9890a67ac84d017eac9b"]);
    running.shutdown();
}

#[test]
fn constant_folding_skips_unreachable_branches() {
    let running = start("lazy-fold");
    let h = running.handle.clone();
    setup_t1_t2(&h);
    // used to fail with division by zero while folding at plan time
    assert_eq!(
        rows(&h, "SELECT CASE WHEN false THEN 1/0 END, coalesce(1, 1/0), false AND 1/0 = 1, true OR 1/0 = 1, \
                  CASE WHEN true THEN 2 ELSE 1/0 END, CASE WHEN NULL THEN 1/0 ELSE 3 END"),
        vec!["|1|f|t|2|3"]
    );
    // reachable constant errors still surface at plan time, as in PostgreSQL
    assert!(try_rows(&h, "SELECT CASE WHEN a > 0 THEN 1/0 END FROM t1").unwrap_err().contains("division by zero"));
    assert!(try_rows(&h, "SELECT 1/0 = 1 AND false").unwrap_err().contains("division by zero"));
    running.shutdown();
}

#[test]
fn distinct_aggregates_order_by_their_arguments() {
    let running = start("distinct-agg-order");
    let h = running.handle.clone();
    setup_t1_t2(&h);
    // ordering DISTINCT values by another column used to be accepted
    for q in ["SELECT string_agg(DISTINCT s, ',' ORDER BY a) FROM t1", "SELECT count(DISTINCT a ORDER BY b) FROM t1"] {
        let err = try_rows(&h, q).unwrap_err();
        assert!(err.contains("ORDER BY expressions must appear in argument list"), "{q}: {err}");
    }
    assert_eq!(
        rows(&h, "SELECT string_agg(DISTINCT s, ',' ORDER BY s DESC), array_agg(DISTINCT a ORDER BY a DESC) FROM t1"),
        vec!["c,b,a|{NULL,3,2,1}"]
    );
    running.shutdown();
}

#[test]
fn sql_value_functions_and_catalog_helpers() {
    let running = start("value-functions");
    let h = running.handle.clone();
    // the parser reads these as function calls, which did not exist
    assert_eq!(
        rows(&h, "SELECT current_user, session_user, user, current_catalog, current_role, pg_typeof(current_user)"),
        vec!["ivm|ivm|ivm|ivm|ivm|name"]
    );
    // used by psql's \\d+ on views
    assert_eq!(rows(&h, "SELECT pg_get_ruledef(0::oid, true) IS NULL"), vec!["t"]);
    running.shutdown();
}

#[test]
fn sum_of_reals_overflows_like_postgresql() {
    let running = start("sum-real");
    let h = running.handle.clone();
    // used to return Infinity
    let err = try_rows(&h, "SELECT sum(r) FROM (VALUES (3e38::real), (3e38::real)) v(r)").unwrap_err();
    assert!(err.contains("value out of range: overflow"), "{err}");
    assert_eq!(rows(&h, "SELECT sum(r) FROM (VALUES (1.5::real), ('Infinity'::real)) v(r)"), vec!["Infinity"]);
    exec(&h, "CREATE TABLE rr (id int PRIMARY KEY, r real)");
    exec(&h, "CREATE MATERIALIZED VIEW rs AS SELECT sum(r) AS s FROM rr");
    exec(&h, "INSERT INTO rr VALUES (1, 3e38), (2, 3e38)");
    assert!(try_rows(&h, "SELECT * FROM rs").unwrap_err().contains("overflow"));
    exec(&h, "DELETE FROM rr WHERE id = 2");
    assert_eq!(rows(&h, "SELECT * FROM rs"), vec!["3e+38"]);
    running.shutdown();
}

#[test]
fn relhasindex_counts_secondary_indexes() {
    let running = start("relhasindex");
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE ri (a int, b int)");
    exec(&h, "CREATE MATERIALIZED VIEW rm AS SELECT a, count(*) AS c FROM ri GROUP BY a");
    let q = "SELECT relname, relhasindex FROM pg_class WHERE relname IN ('ri', 'rm') ORDER BY 1";
    assert_eq!(rows(&h, q), vec!["ri|f", "rm|f"]);
    exec(&h, "CREATE INDEX ON ri (b)");
    exec(&h, "CREATE INDEX ON rm (a)");
    // psql's \d only lists indexes of relations with relhasindex set
    assert_eq!(rows(&h, q), vec!["ri|t", "rm|t"]);
    assert_eq!(rows(&h, "SELECT hasindexes FROM pg_tables WHERE tablename = 'ri'"), vec!["t"]);
    assert_eq!(rows(&h, "SELECT hasindexes FROM pg_matviews WHERE matviewname = 'rm'"), vec!["t"]);
    running.shutdown();
}

#[test]
fn unknown_literal_arguments_prefer_preferred_types() {
    let running = start("unknown-preferred");
    let h = running.handle.clone();
    // abs(unknown) resolved to abs(smallint), the first overload registered,
    // where PostgreSQL picks the preferred type of the category (float8)
    assert_eq!(rows(&h, "SELECT abs('-1.5'), abs('100000'), pg_typeof(abs(NULL))"), vec!["1.5|100000|double precision"]);
    assert_eq!(rows(&h, "SELECT round('2.5'), pg_typeof(sign('-2')), pg_typeof(abs(1::int2))"), vec!["2|double precision|smallint"]);
    running.shutdown();
}
