//! Regression tests for the incremental dataflow operators and the batch
//! executor: maintained views must equal a recompute (an ad hoc query or a
//! freshly hydrated copy) after every change.

use ivm::coord::api::{ExecResult, SessionVars};
use ivm::coord::{self, EngineHandle};
use ivm::types::text::format_datum;
use std::path::PathBuf;

fn tmpdir(name: &str) -> PathBuf {
    let base = std::env::var("IVM_TEST_TMP").unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().into_owned());
    let d = PathBuf::from(base).join(format!("ivm-dataflow-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn start(dir: &PathBuf) -> coord::Running {
    let cfg = coord::Config { data_dir: dir.clone(), sync: false, tick: None, ..Default::default() };
    coord::start(cfg).expect("engine starts")
}

fn try_exec(h: &EngineHandle, sql: &str) -> Result<ExecResult, String> {
    let stmt = ivm::sql::parse_statement(sql).map_err(|e| format!("parse: {e}"))?;
    h.execute(stmt, vec![], SessionVars::default(), String::new()).map_err(|e| format!("{} {}", e.code, e.message))
}

fn rows_of(h: &EngineHandle, sql: &str) -> Result<Vec<String>, String> {
    match try_exec(h, sql)? {
        ExecResult::Query { rows, .. } => Ok(rows.iter().map(|r| r.iter().map(format_datum).collect::<Vec<_>>().join("|")).collect()),
        other => Err(format!("{other:?}")),
    }
}

fn rows(h: &EngineHandle, sql: &str) -> Vec<String> {
    rows_of(h, sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
}

fn exec(h: &EngineHandle, sql: &str) {
    try_exec(h, sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// Frames that start past the end of the partition are empty (they used to
/// slice out of bounds and panic the engine thread), and huge offsets
/// saturate instead of overflowing.
#[test]
fn window_frames_past_the_partition_end() {
    let dir = tmpdir("frames-end");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE w (id int PRIMARY KEY, v int)");
    exec(&h, "INSERT INTO w VALUES (1, 1), (2, 2), (3, 3), (4, 4)");
    let q = "SELECT id, sum(v) OVER (ORDER BY id ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING) AS s, \
             string_agg(v::text, ',') OVER (ORDER BY id ROWS BETWEEN 3 FOLLOWING AND 5 FOLLOWING) AS sa, \
             count(*) OVER (ORDER BY id ROWS BETWEEN 9223372036854775807 FOLLOWING AND UNBOUNDED FOLLOWING) AS c0, \
             count(*) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND 9223372036854775807 FOLLOWING) AS call, \
             count(*) OVER (ORDER BY id ROWS BETWEEN 9223372036854775807 PRECEDING AND 9223372036854775807 PRECEDING) AS cp, \
             count(*) OVER (ORDER BY v GROUPS BETWEEN 9223372036854775807 PRECEDING AND 9223372036854775807 FOLLOWING) AS cg, \
             first_value(v) OVER (ORDER BY id ROWS BETWEEN 2 FOLLOWING AND 9 FOLLOWING) AS fv \
             FROM w";
    exec(&h, &format!("CREATE MATERIALIZED VIEW fr AS {q}"));
    let want = ["1|7|4|0|4|0|4|3", "2|4||0|4|0|4|4", "3|||0|4|0|4|", "4|||0|4|0|4|"];
    assert_eq!(rows(&h, &format!("{q} ORDER BY id")), want);
    assert_eq!(rows(&h, "SELECT * FROM fr ORDER BY id"), want);
    exec(&h, "DELETE FROM w WHERE id = 2");
    let want = ["1|4||0|3|0|3|4", "3|||0|3|0|3|", "4|||0|3|0|3|"];
    assert_eq!(rows(&h, &format!("{q} ORDER BY id")), want);
    assert_eq!(rows(&h, "SELECT * FROM fr ORDER BY id"), want);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A RANGE offset bound that overflows the ORDER BY type lies beyond every
/// value in the offset's direction (PostgreSQL's in_range semantics), and
/// nth_value with a NULL position is NULL. Expected rows are PostgreSQL's.
#[test]
fn window_range_offsets_that_overflow() {
    let dir = tmpdir("range-overflow");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE w (id int PRIMARY KEY, v int)");
    exec(&h, "INSERT INTO w VALUES (1, 1), (2, 2), (3, 2147483647), (4, -2147483648)");
    let q = "SELECT id, sum(v::int8) OVER (ORDER BY v RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS a, \
             sum(v::int8) OVER (ORDER BY v RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) AS b, \
             count(*) OVER (ORDER BY v RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) AS c, \
             count(*) OVER (ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS d, \
             count(*) OVER (ORDER BY v DESC RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS e, \
             count(*) OVER (ORDER BY v RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS f, \
             count(*) OVER (ORDER BY v NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS g, \
             nth_value(v, CASE WHEN id = 1 THEN NULL ELSE 1 END) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS h \
             FROM w";
    exec(&h, &format!("CREATE MATERIALIZED VIEW r AS {q}"));
    let want = ["1|2||3|3|2|2|1|", "2||1|3|3|2|1|2|1", "3|||1|4|1|0|3|1", "4|||4|1|1|3|0|1"];
    assert_eq!(rows(&h, &format!("{q} ORDER BY id")), want);
    assert_eq!(rows(&h, "SELECT * FROM r ORDER BY id"), want);
    // NULL ordering values: past the last non-null value comes the NULL row
    exec(&h, "INSERT INTO w VALUES (5, NULL)");
    let want = ["1|2||4|3|2|3|2|", "2||1|4|3|2|2|3|1", "3|||2|4|1|1|4|1", "4|||5|1|1|4|1|1", "5|||1|5|1|1|1|1"];
    assert_eq!(rows(&h, &format!("{q} ORDER BY id")), want);
    assert_eq!(rows(&h, "SELECT * FROM r ORDER BY id"), want);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// DISTINCT aggregates with FILTER: the filter applies before duplicates
/// are removed, and groups none of whose rows pass the filter still exist.
/// (Lowering them used to recurse forever and overflow the engine's stack.)
/// Expected rows are PostgreSQL's.
#[test]
fn distinct_aggregates_with_filter() {
    let dir = tmpdir("distinct-filter");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, g int, x int, y int)");
    exec(&h, "INSERT INTO t VALUES (1, 1, 1, -1), (2, 1, 1, 5), (3, 1, 2, 5), (4, 1, NULL, 1), (5, 2, NULL, 1), (6, 2, 7, 1), (7, 1, 9, 0), (8, 3, 4, -1)");
    exec(&h, "CREATE MATERIALIZED VIEW one AS SELECT g, count(DISTINCT x) FILTER (WHERE y > 0) AS c FROM t GROUP BY g");
    exec(
        &h,
        "CREATE MATERIALIZED VIEW mixed AS SELECT g, count(DISTINCT x) FILTER (WHERE y > 0) AS c, count(DISTINCT x) AS d, \
         sum(DISTINCT x) FILTER (WHERE y <= 0) AS s, array_agg(DISTINCT x ORDER BY x) FILTER (WHERE y > 0) AS a, count(*) AS n \
         FROM t GROUP BY g",
    );
    exec(
        &h,
        "CREATE MATERIALIZED VIEW global AS SELECT count(DISTINCT x) FILTER (WHERE y > 0) AS c, \
         string_agg(DISTINCT x::text, ',' ORDER BY x::text) FILTER (WHERE y <> 5) AS s FROM t",
    );
    assert_eq!(rows(&h, "SELECT * FROM one ORDER BY g"), ["1|2", "2|1", "3|0"]);
    assert_eq!(rows(&h, "SELECT * FROM mixed ORDER BY g"), ["1|2|3|10|{1,2,NULL}|5", "2|1|1||{7,NULL}|2", "3|0|1|4||1"]);
    assert_eq!(rows(&h, "SELECT * FROM global"), ["3|1,4,7,9"]);
    exec(&h, "UPDATE t SET y = 3 WHERE id = 8");
    exec(&h, "DELETE FROM t WHERE id = 2");
    exec(&h, "INSERT INTO t VALUES (9, 2, 7, -3), (10, 2, 7, 2)");
    assert_eq!(rows(&h, "SELECT * FROM one ORDER BY g"), ["1|1", "2|1", "3|1"]);
    assert_eq!(rows(&h, "SELECT * FROM mixed ORDER BY g"), ["1|1|3|10|{2,NULL}|4", "2|1|1|7|{7,NULL}|4", "3|1|1||{4}|1"]);
    assert_eq!(rows(&h, "SELECT * FROM global"), ["3|1,4,7,9"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// The batch executor (ad hoc queries) applies FILTER before removing
/// duplicates of DISTINCT aggregates, and a DISTINCT ordered aggregate no
/// longer hands its deduplicated rows to the group's other aggregates.
/// Expected rows are PostgreSQL's; the maintained views must agree.
#[test]
fn adhoc_distinct_aggregates() {
    let dir = tmpdir("adhoc-distinct");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, g int, x int, y int)");
    exec(&h, "INSERT INTO t VALUES (1, 1, 1, -1), (2, 1, 1, 5), (3, 1, 2, 5), (4, 1, NULL, 1), (5, 2, NULL, 1), (6, 2, 7, 1), (7, 1, 9, 0), (8, 3, 4, -1)");
    let cases: [(&str, &[&str]); 2] = [
        (
            "SELECT g, count(DISTINCT x) FILTER (WHERE y > 0) AS c, count(DISTINCT x) AS d, \
             sum(DISTINCT x) FILTER (WHERE y <= 0) AS s, array_agg(DISTINCT x ORDER BY x) FILTER (WHERE y > 0) AS a, count(*) AS n \
             FROM t GROUP BY g",
            &["1|2|3|10|{1,2,NULL}|5", "2|1|1||{7,NULL}|2", "3|0|1|4||1"],
        ),
        (
            "SELECT g, array_agg(DISTINCT x ORDER BY x) AS a, array_agg(x ORDER BY x) AS b, \
             string_agg(DISTINCT x::text, ',' ORDER BY x::text) FILTER (WHERE y > 0) AS c, string_agg(x::text, ',' ORDER BY x) AS d \
             FROM t GROUP BY g",
            &["1|{1,2,9,NULL}|{1,1,2,9,NULL}|1,2|1,1,2,9", "2|{7,NULL}|{7,NULL}|7|7", "3|{4}|{4}||4"],
        ),
    ];
    for (i, (q, want)) in cases.iter().enumerate() {
        assert_eq!(rows(&h, &format!("{q} ORDER BY g")), *want, "{q}");
        exec(&h, &format!("CREATE MATERIALIZED VIEW v{i} AS {q}"));
        assert_eq!(rows(&h, &format!("SELECT * FROM v{i} ORDER BY g")), *want, "{q}");
    }
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Maintained percentiles read order statistics through the index; the
/// non-null values follow the group's NULLs when the ORDER BY sorts NULLs
/// first (DESC, NULLS FIRST). Expected rows are PostgreSQL's.
#[test]
fn percentiles_with_nulls_first() {
    let dir = tmpdir("percentile-nulls");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, g int, x int)");
    exec(&h, "INSERT INTO t VALUES (1, 1, 1), (2, 1, 1), (3, 1, 2), (4, 1, NULL), (5, 2, NULL), (6, 2, 7), (7, 1, 9), (8, 3, NULL)");
    let grouped = "SELECT g, percentile_disc(0.5) WITHIN GROUP (ORDER BY x DESC) AS a, percentile_cont(0.5) WITHIN GROUP (ORDER BY x DESC) AS b, \
                   percentile_disc(0.25) WITHIN GROUP (ORDER BY x NULLS FIRST) AS c, percentile_cont(0.25) WITHIN GROUP (ORDER BY x DESC NULLS LAST) AS d \
                   FROM t GROUP BY g";
    let global = "SELECT percentile_disc(0.9) WITHIN GROUP (ORDER BY x DESC) AS a, percentile_cont(0.1) WITHIN GROUP (ORDER BY x NULLS FIRST) AS b FROM t";
    exec(&h, &format!("CREATE MATERIALIZED VIEW pg AS {grouped}"));
    exec(&h, &format!("CREATE MATERIALIZED VIEW pa AS {global}"));
    let check = |want_g: &[&str], want_a: &[&str]| {
        assert_eq!(rows(&h, "SELECT * FROM pg ORDER BY g"), want_g);
        assert_eq!(rows(&h, &format!("{grouped} ORDER BY g")), want_g);
        assert_eq!(rows(&h, "SELECT * FROM pa"), want_a);
        assert_eq!(rows(&h, global), want_a);
    };
    check(&["1|2|1.5|1|3.75", "2|7|7|7|7", "3||||"], &["1|1"]);
    exec(&h, "INSERT INTO t VALUES (9, 2, NULL), (10, 2, 3), (11, 3, 5)");
    exec(&h, "DELETE FROM t WHERE id = 3");
    check(&["1|1|1|1|5", "2|7|5|3|6", "3|5|5|5|5"], &["1|1"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Semijoins, antijoins and outer joins on conditions other than
/// equalities compare left rows by identity: rows that are equal but differ
/// in representation (numeric scale) can differ in whether they match.
/// Expected rows are PostgreSQL's.
#[test]
fn general_semijoins_keep_representations_apart() {
    let dir = tmpdir("semijoin-repr");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE l (x numeric)"); // a multiset
    exec(&h, "CREATE TABLE r (id int PRIMARY KEY, s text)");
    exec(&h, "INSERT INTO l VALUES (1.0), (1.00), (2), (1.0)");
    exec(&h, "INSERT INTO r VALUES (1, '1.0')");
    let semi = "SELECT x FROM l WHERE EXISTS (SELECT 1 FROM r WHERE r.s LIKE l.x::text)";
    let anti = "SELECT x FROM l WHERE NOT EXISTS (SELECT 1 FROM r WHERE r.s LIKE l.x::text)";
    let outer = "SELECT l.x, r.id FROM l LEFT JOIN r ON r.s LIKE l.x::text";
    exec(&h, &format!("CREATE MATERIALIZED VIEW semi AS {semi}"));
    exec(&h, &format!("CREATE MATERIALIZED VIEW anti AS {anti}"));
    exec(&h, &format!("CREATE MATERIALIZED VIEW outer_ AS {outer}"));
    let check = |want: [&[&str]; 3]| {
        for ((q, v), want) in [(semi, "semi"), (anti, "anti"), (outer, "outer_")].into_iter().zip(want) {
            let mut want = want.to_vec();
            want.sort();
            let mut got = rows(&h, &format!("SELECT * FROM {v}"));
            got.sort();
            assert_eq!(got, want, "{v}");
            let mut adhoc = rows(&h, q);
            adhoc.sort();
            assert_eq!(adhoc, want, "{q}");
        }
    };
    check([&["1.0", "1.0"], &["1.00", "2"], &["1.0|1", "1.0|1", "1.00|", "2|"]]);
    exec(&h, "INSERT INTO r VALUES (2, '1.00')");
    exec(&h, "DELETE FROM r WHERE id = 1");
    check([&["1.00"], &["1.0", "1.0", "2"], &["1.0|", "1.0|", "1.00|2", "2|"]]);
    exec(&h, "DELETE FROM l WHERE x = 2");
    exec(&h, "INSERT INTO r VALUES (3, '1.0'), (4, '2')");
    exec(&h, "INSERT INTO l VALUES (2.0)");
    check([&["1.0", "1.0", "1.00"], &["2.0"], &["1.0|3", "1.0|3", "1.00|2", "2.0|"]]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

fn read_error(h: &EngineHandle, sql: &str) -> Option<String> {
    rows_of(h, sql).err()
}

/// A view created over existing dataflow (an identical view, or a query
/// sharing an arranged node of another view) sees the error records already
/// in it, also after a restart; their retraction then clears the view
/// instead of driving its error count negative (which hid later errors).
#[test]
fn errors_of_shared_nodes() {
    let dir = tmpdir("shared-errors");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, x int)");
    exec(&h, "INSERT INTO t VALUES (1, 1), (2, 0)");
    exec(&h, "CREATE MATERIALIZED VIEW v1 AS SELECT id, 10 / x AS q FROM t");
    exec(&h, "CREATE MATERIALIZED VIEW v2 AS SELECT id, 10 / x AS q FROM t"); // the same root node
    exec(&h, "CREATE MATERIALIZED VIEW v3 AS SELECT count(q) AS n FROM v1"); // reads v1's arranged node
    let div0 = Some("22012 division by zero".to_string());
    for v in ["v1", "v2", "v3"] {
        assert_eq!(read_error(&h, &format!("SELECT * FROM {v}")), div0, "{v}");
    }
    exec(&h, "DELETE FROM t WHERE id = 2");
    assert_eq!(rows(&h, "SELECT * FROM v2"), ["1|10"]);
    assert_eq!(rows(&h, "SELECT * FROM v3"), ["1"]);
    exec(&h, "INSERT INTO t VALUES (3, 0)");
    for v in ["v1", "v2", "v3"] {
        assert_eq!(read_error(&h, &format!("SELECT * FROM {v}")), div0, "{v}");
    }
    // after a restart, new views over the recovered nodes see their errors
    running.shutdown();
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE MATERIALIZED VIEW v4 AS SELECT id, 10 / x AS q FROM t");
    exec(&h, "CREATE MATERIALIZED VIEW v5 AS SELECT sum(q) AS s FROM v1");
    for v in ["v1", "v2", "v3", "v4", "v5"] {
        assert_eq!(read_error(&h, &format!("SELECT * FROM {v}")), div0, "{v}");
    }
    exec(&h, "UPDATE t SET x = 5 WHERE id = 3");
    assert_eq!(rows(&h, "SELECT * FROM v4 ORDER BY id"), ["1|10", "3|2"]);
    assert_eq!(rows(&h, "SELECT * FROM v5"), ["12"]);
    assert_eq!(rows(&h, "SELECT * FROM v3"), ["2"]);
    exec(&h, "INSERT INTO t VALUES (4, 0)");
    for v in ["v1", "v2", "v3", "v4", "v5"] {
        assert_eq!(read_error(&h, &format!("SELECT * FROM {v}")), div0, "{v}");
    }
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A group whose aggregate fails is remembered so that its error record
/// retracts when the group recovers: also for a global aggregate that was
/// already failing when the view was created, and after a restart.
#[test]
fn aggregate_errors_retract_after_recovery() {
    let dir = tmpdir("agg-errors");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE f (id int PRIMARY KEY, g int, x float8)");
    exec(&h, "CREATE MATERIALIZED VIEW grouped AS SELECT g, sum(x) AS s FROM f GROUP BY g");
    exec(&h, "INSERT INTO f VALUES (1, 1, 1e308), (2, 1, 1e308), (3, 2, 1), (4, 3, 1e308), (5, 3, 1e308), (6, 4, 1e308), (7, 4, 1e308)");
    exec(&h, "CREATE MATERIALIZED VIEW total AS SELECT sum(x) AS s FROM f"); // failing when created
    exec(&h, "CREATE MATERIALIZED VIEW total2 AS SELECT sum(x) AS s FROM f WHERE g = 4");
    let overflow = Some("22003 value out of range: overflow".to_string());
    for v in ["grouped", "total", "total2"] {
        assert_eq!(read_error(&h, &format!("SELECT * FROM {v}")), overflow, "{v}");
    }
    // recovery without a restart
    exec(&h, "DELETE FROM f WHERE id = 7");
    assert_eq!(rows(&h, "SELECT * FROM total2"), rows(&h, "SELECT sum(x) FROM f WHERE g = 4"));
    // and after one
    running.shutdown();
    let running = start(&dir);
    let h = running.handle.clone();
    for v in ["grouped", "total"] {
        assert_eq!(read_error(&h, &format!("SELECT * FROM {v}")), overflow, "{v}");
    }
    exec(&h, "DELETE FROM f WHERE id IN (2, 4, 5)");
    exec(&h, "DELETE FROM f WHERE id = 6");
    assert_eq!(rows(&h, "SELECT * FROM grouped ORDER BY g"), rows(&h, "SELECT g, sum(x) FROM f GROUP BY g ORDER BY g"));
    assert_eq!(rows(&h, "SELECT * FROM total"), rows(&h, "SELECT sum(x) FROM f"));
    assert_eq!(rows(&h, "SELECT count(*) FROM grouped").join(","), "2");
    // a new failure is reported again
    exec(&h, "INSERT INTO f VALUES (8, 2, 1e308), (9, 2, 1e308)");
    for v in ["grouped", "total"] {
        assert_eq!(read_error(&h, &format!("SELECT * FROM {v}")), overflow, "{v}");
    }
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Run a statement, failing (instead of hanging) if no reply comes: a DML
/// whose dataflow step fails is never acknowledged.
fn exec_within(h: &EngineHandle, sql: &str, secs: u64) {
    let (h2, sql2) = (h.clone(), sql.to_string());
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(try_exec(&h2, &sql2).map(|_| ()));
    });
    match rx.recv_timeout(std::time::Duration::from_secs(secs)) {
        Ok(r) => r.unwrap_or_else(|e| panic!("{sql}: {e}")),
        Err(_) => panic!("{sql}: no reply within {secs}s (its dataflow step failed)"),
    }
}

/// An expression error while evaluating a recursive query is an error
/// record of the view (its rows retract until the query recovers), not a
/// failed step: the failed step was never acknowledged and left the
/// dataflow half-updated.
#[test]
fn recursive_query_errors_are_data() {
    let dir = tmpdir("recursive-errors");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, x int)");
    exec(&h, "INSERT INTO t VALUES (1, 4)");
    let q = "WITH RECURSIVE r(n) AS (SELECT x FROM t UNION SELECT 100 / n FROM r WHERE n <> 1) SELECT n FROM r";
    exec(&h, &format!("CREATE MATERIALIZED VIEW r1 AS {q}"));
    assert_eq!(rows(&h, "SELECT * FROM r1 ORDER BY n"), ["4", "25"]);
    exec_within(&h, "INSERT INTO t VALUES (2, 0)", 20);
    let div0 = Some("22012 division by zero".to_string());
    assert_eq!(read_error(&h, "SELECT * FROM r1"), div0);
    exec(&h, &format!("CREATE MATERIALIZED VIEW r2 AS {q}")); // failing when created
    assert_eq!(read_error(&h, "SELECT * FROM r2"), div0);
    running.shutdown();
    let running = start(&dir);
    let h = running.handle.clone();
    assert_eq!(read_error(&h, "SELECT * FROM r1"), div0);
    exec_within(&h, "DELETE FROM t WHERE id = 2", 20);
    exec_within(&h, "INSERT INTO t VALUES (3, 5)", 20);
    for v in ["r1", "r2"] {
        assert_eq!(rows(&h, &format!("SELECT * FROM {v} ORDER BY n")), ["4", "5", "20", "25"], "{v}");
    }
    assert_eq!(rows(&h, "SELECT * FROM t ORDER BY id"), ["1|4", "3|5"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
