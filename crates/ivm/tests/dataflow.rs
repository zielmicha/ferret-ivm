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

/// Join keys computed by an input's filtered map (or by the condition
/// itself) are not evaluated on rows the filter excludes, and a key that
/// fails is an error record of the view: arranging the raw input by the
/// expression failed hydration and, for later rows, the whole step (whose
/// DML then never completed).
#[test]
fn computed_join_keys() {
    let dir = tmpdir("computed-keys");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE a (id int PRIMARY KEY, v int, code text)");
    exec(&h, "CREATE TABLE b (id int PRIMARY KEY, x int)");
    exec(&h, "INSERT INTO a VALUES (1, 50, '1'), (2, 100, 'x'), (3, 25, '3')");
    exec(&h, "INSERT INTO b VALUES (1, 2), (2, 0), (3, 1)");
    let filtered = "SELECT a.id AS aid, b2.id AS bid FROM a JOIN (SELECT id, 100 / x AS q FROM b WHERE x <> 0) b2 ON a.v = b2.q";
    let cast = "SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.code::int = b.id WHERE a.code ~ '^[0-9]+$'";
    let failing = "SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.v = 100 / b.x";
    exec(&h, &format!("CREATE MATERIALIZED VIEW filtered AS {filtered}"));
    exec(&h, &format!("CREATE MATERIALIZED VIEW cast_ AS {cast}"));
    exec(&h, &format!("CREATE MATERIALIZED VIEW failing AS {failing}"));
    let check = |h: &EngineHandle| {
        for (v, q) in [("filtered", filtered), ("cast_", cast)] {
            assert_eq!(rows(h, &format!("SELECT * FROM {v} ORDER BY 1, 2")), rows(h, &format!("{q} ORDER BY 1, 2")), "{v}");
        }
    };
    assert_eq!(rows(&h, "SELECT * FROM filtered ORDER BY 1, 2"), ["1|1", "2|3"]);
    assert_eq!(rows(&h, "SELECT * FROM cast_ ORDER BY 1, 2"), ["1|1", "3|3"]);
    let div0 = Some("22012 division by zero".to_string());
    assert_eq!(read_error(&h, "SELECT * FROM failing"), div0);
    exec_within(&h, "INSERT INTO b VALUES (4, 0), (5, 4)", 20);
    exec_within(&h, "INSERT INTO a VALUES (4, 25, 'y'), (5, 100, '5')", 20);
    check(&h);
    assert_eq!(rows(&h, "SELECT * FROM filtered ORDER BY 1, 2"), ["1|1", "2|3", "3|5", "4|5", "5|3"]);
    exec_within(&h, "DELETE FROM b WHERE x = 0", 20);
    check(&h);
    assert_eq!(rows(&h, "SELECT * FROM failing ORDER BY 1, 2"), rows(&h, &format!("{failing} ORDER BY 1, 2")));
    exec_within(&h, "UPDATE b SET x = 0 WHERE id = 3", 20);
    assert_eq!(read_error(&h, "SELECT * FROM failing"), div0);
    check(&h);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Expression errors in the per-input maps of a join count once per input
/// row, whether the view was hydrated over the row or saw it arrive: the
/// hydration of a join used to count them only for the input it scanned
/// (the smallest), so views created later disagreed with older ones.
#[test]
fn join_input_errors_hydrate_like_steps() {
    let dir = tmpdir("join-input-errors");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE small (id int PRIMARY KEY, v int)");
    exec(&h, "CREATE TABLE big (id int PRIMARY KEY, x int)");
    exec(&h, "INSERT INTO small VALUES (1, 1)");
    exec(&h, "INSERT INTO big SELECT g, g FROM generate_series(1, 20) g");
    let q = "SELECT small.id, s.q FROM small JOIN (SELECT id, 10 / x AS q FROM big) s ON small.v = s.id";
    exec(&h, &format!("CREATE MATERIALIZED VIEW before_ AS {q}"));
    exec(&h, "UPDATE big SET x = 0 WHERE id = 7"); // joins nothing
    // the same join under another root (hydrated, not shared)
    exec(&h, &format!("CREATE MATERIALIZED VIEW after_ AS SELECT * FROM ({q}) j WHERE id > 0"));
    let div0 = Some("22012 division by zero".to_string());
    for v in ["before_", "after_"] {
        assert_eq!(read_error(&h, &format!("SELECT * FROM {v}")), div0, "{v}");
    }
    exec(&h, "UPDATE big SET x = 7 WHERE id = 7");
    for v in ["before_", "after_"] {
        assert_eq!(rows(&h, &format!("SELECT * FROM {v}")), ["1|10"], "{v}");
    }
    exec(&h, "UPDATE big SET x = 0 WHERE id = 8");
    for v in ["before_", "after_"] {
        assert_eq!(read_error(&h, &format!("SELECT * FROM {v}")), div0, "{v}");
    }
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Variances of int8 values: the sum of squares (up to 2^126 per value)
/// overflowed its i128 accumulator, panicking the engine thread (debug
/// builds) or wrapping around. Expected rows are PostgreSQL's.
#[test]
fn int8_variance_does_not_overflow() {
    let dir = tmpdir("int8-variance");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE b (id int PRIMARY KEY, g int, x int8)");
    exec(&h, "INSERT INTO b VALUES (1, 1, 9223372036854775807), (2, 1, 9223372036854775807), (3, 1, -9223372036854775807), (4, 2, 3037000499), (5, 2, 1)");
    let grouped = "SELECT g, var_samp(x) AS a, stddev_pop(x) AS b, variance(x) AS c FROM b GROUP BY g";
    let global = "SELECT var_pop(x) AS v FROM b";
    exec(&h, &format!("CREATE MATERIALIZED VIEW vg AS {grouped}"));
    exec(&h, &format!("CREATE MATERIALIZED VIEW va AS {global}"));
    let want_g = [
        "1|113427455640312821129862543712310001665|8695878550221854807|113427455640312821129862543712310001665",
        "2|4611686012426124002|1518500249|4611686012426124002",
    ];
    assert_eq!(rows(&h, "SELECT * FROM vg ORDER BY g"), want_g);
    assert_eq!(rows(&h, &format!("{grouped} ORDER BY g")), want_g);
    assert_eq!(rows(&h, "SELECT * FROM va"), ["47639531366690474037008890079615688220"]);
    assert_eq!(
        rows(&h, "SELECT id, var_pop(x) OVER (PARTITION BY g ORDER BY id) FROM b ORDER BY id"),
        ["1|0", "2|0", "3|75618303760208547419908362474873334444", "4|0", "5|2305843006213062001"]
    );
    exec(&h, "DELETE FROM b WHERE id = 2");
    exec(&h, "INSERT INTO b VALUES (6, 2, -9223372036854775807)");
    let want_g = [
        "1|170141183460469231694793815568465002498|9223372036854775807|170141183460469231694793815568465002498",
        "2|28356863919415333781411417455122884417|4347939275826755286|28356863919415333781411417455122884417",
    ];
    assert_eq!(rows(&h, "SELECT * FROM vg ORDER BY g"), want_g);
    assert_eq!(rows(&h, &format!("{grouped} ORDER BY g")), want_g);
    assert_eq!(rows(&h, "SELECT * FROM va"), ["47639531371172295715027125688135192780"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Aggregate summaries are kept exactly beyond the range of numeric (as
/// PostgreSQL's aggregate states are); only a result out of range is an
/// error, as a record of its group. The square of a value above 1e65536
/// used to fail the index update in the middle of a dataflow step: the
/// statement was never acknowledged, the dataflow was left half-updated,
/// and the engine thread panicked at the next checkpoint.
#[test]
fn numeric_aggregates_beyond_the_numeric_range() {
    let dir = tmpdir("numeric-range");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, g int, n numeric)");
    exec(&h, "INSERT INTO t VALUES (1, 1, 1), (2, 1, 2)");
    exec(&h, "CREATE MATERIALIZED VIEW s AS SELECT g, sum(n) AS s, var_pop(n) AS v, avg(n) AS a FROM t GROUP BY g");
    exec_within(&h, "INSERT INTO t VALUES (3, 2, '1e100000'), (4, 2, '1e100000')", 10);
    let check = "SELECT g, s = 3 OR s = 2e100000 OR s = 9e131071, v = 0.25 OR v = 0, a = 1.5 OR a = 1e100000 OR a = 9e131071 FROM s ORDER BY g";
    assert_eq!(rows(&h, check), ["1|t|t|t", "2|t|t|t"]);
    // a sum beyond the range is the group's error, until it is back in range
    exec_within(&h, "INSERT INTO t VALUES (5, 3, '9e131071'), (6, 3, '9e131071')", 10);
    assert_eq!(rows_of(&h, "SELECT * FROM s"), Err("22003 value overflows numeric format".to_string()));
    exec(&h, "CHECKPOINT");
    running.shutdown();
    let running = start(&dir);
    let h = running.handle.clone();
    assert_eq!(rows_of(&h, "SELECT * FROM s"), Err("22003 value overflows numeric format".to_string()));
    exec_within(&h, "DELETE FROM t WHERE id = 6", 10);
    assert_eq!(rows(&h, check), ["1|t|t|t", "2|t|t|t", "3|t|t|t"]);
    exec(&h, "DELETE FROM t WHERE id IN (3, 4)");
    assert_eq!(rows(&h, "SELECT g, s FROM s WHERE g < 3 ORDER BY g"), ["1|3"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A small deterministic random generator (xorshift64*).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

/// Random changes to `t (id int PRIMARY KEY, g int, v int)` with few
/// distinct values (ties, duplicate rows in projections, NULLs): inserts,
/// updates and deletes of one row or of many in one statement. After each,
/// every view must equal its query run ad hoc.
fn random_changes_keep_views_equal(name: &str, queries: &[&str], steps: usize, seed: u64) {
    random_changes_keep_views_equal_from(name, queries, steps, seed, 20)
}

/// As `random_changes_keep_views_equal`, starting with `rows` rows; a view
/// that fails must fail with the same error as its query.
fn random_changes_keep_views_equal_from(name: &str, queries: &[&str], steps: usize, seed: u64, initial: usize) {
    let dir = tmpdir(name);
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, g int, v int)");
    let mut rng = Rng(seed);
    let mut ids: Vec<i64> = Vec::new();
    let mut next_id = 1;
    let val = |rng: &mut Rng| if rng.below(10) == 0 { "NULL".to_string() } else { rng.below(8).to_string() };
    for _ in 0..initial {
        exec(&h, &format!("INSERT INTO t VALUES ({next_id}, {}, {})", rng.below(3), val(&mut rng)));
        ids.push(next_id);
        next_id += 1;
    }
    for (i, q) in queries.iter().enumerate() {
        exec(&h, &format!("CREATE MATERIALIZED VIEW v{i} AS {q}"));
    }
    for step in 0..steps {
        let pick = |rng: &mut Rng, ids: &[i64]| ids[rng.below(ids.len() as u64) as usize];
        let sql = match rng.below(9) {
            0 | 1 if ids.len() < 60.max(initial + 20) || ids.is_empty() => {
                let n = 1 + rng.below(3);
                let mut vals = Vec::new();
                for _ in 0..n {
                    vals.push(format!("({next_id}, {}, {})", rng.below(3), val(&mut rng)));
                    ids.push(next_id);
                    next_id += 1;
                }
                format!("INSERT INTO t VALUES {}", vals.join(", "))
            }
            _ if ids.is_empty() => continue,
            0..=2 => format!("UPDATE t SET v = {} WHERE id = {}", val(&mut rng), pick(&mut rng, &ids)),
            3 => format!("UPDATE t SET g = {} WHERE id = {}", rng.below(3), pick(&mut rng, &ids)),
            4 => format!("UPDATE t SET v = v + 1 WHERE id % {} = {}", 2 + rng.below(4), rng.below(2)),
            5 => format!("UPDATE t SET v = v - 1, g = (g + 1) % 3 WHERE v > {}", rng.below(8)),
            6 | 7 => {
                let id = pick(&mut rng, &ids);
                ids.retain(|x| *x != id);
                format!("DELETE FROM t WHERE id = {id}")
            }
            _ => {
                let g = rng.below(3);
                let gone = rows(&h, &format!("SELECT id FROM t WHERE g = {g}"));
                ids.retain(|x| !gone.contains(&x.to_string()));
                format!("DELETE FROM t WHERE g = {g}")
            }
        };
        exec(&h, &sql);
        for (i, q) in queries.iter().enumerate() {
            let want = rows_of(&h, q).map(sorted);
            let got = rows_of(&h, &format!("SELECT * FROM v{i}")).map(sorted);
            assert_eq!(got, want, "step {step} ({sql}): view {i}: {q}");
        }
    }
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Top-k windows (LIMIT, OFFSET, WITH TIES, per group) follow random
/// changes, multiplicities and ties included: a change is applied where the
/// window before and after it can differ, found through the count
/// summaries.
#[test]
fn topk_follows_random_changes() {
    let queries = [
        "SELECT id, v FROM t ORDER BY v, id LIMIT 3 OFFSET 2",
        "SELECT g, v FROM t ORDER BY v DESC NULLS LAST OFFSET 4 FETCH FIRST 3 ROWS WITH TIES",
        "SELECT v FROM t ORDER BY v LIMIT 5 OFFSET 3",
        "SELECT v FROM t ORDER BY v NULLS FIRST OFFSET 6",
        "SELECT g, v FROM t ORDER BY v FETCH FIRST 1 ROWS WITH TIES",
        "SELECT DISTINCT ON (g) g, v FROM t ORDER BY g, v DESC",
        "SELECT v FROM t ORDER BY v LIMIT 0",
        "SELECT g FROM t ORDER BY g LIMIT 7 OFFSET 7",
        "SELECT k.g, x.v FROM (SELECT DISTINCT g FROM t) k, LATERAL (SELECT v FROM t WHERE t.g = k.g ORDER BY v LIMIT 2 OFFSET 1) x",
        "SELECT k.g, x.v FROM (SELECT DISTINCT g FROM t) k, LATERAL (SELECT v FROM t WHERE t.g = k.g ORDER BY v DESC FETCH FIRST 2 ROWS WITH TIES) x",
    ];
    for seed in [1, 2, 3] {
        random_changes_keep_views_equal(&format!("topk-random-{seed}"), &queries, 150, seed * 7919);
    }
}

/// Window functions of every kind, with ROWS, RANGE (also offsets) and
/// GROUPS frames, follow random changes, multiplicities, ties, NULLs and
/// errors included: a change recomputes the rows whose values it can
/// change, over the rows their frames reach.
#[test]
fn windows_follow_random_changes() {
    let queries = [
        "SELECT id, row_number() OVER (PARTITION BY g ORDER BY v, id) AS rn FROM t",
        "SELECT id, rank() OVER w AS r, dense_rank() OVER w AS d, percent_rank() OVER w AS pr, cume_dist() OVER w AS cd FROM t WINDOW w AS (PARTITION BY g ORDER BY v)",
        "SELECT id, sum(v) OVER (PARTITION BY g ORDER BY id) AS s, count(*) OVER (PARTITION BY g ORDER BY v) AS c FROM t",
        "SELECT id, sum(v) OVER (PARTITION BY g ORDER BY v, id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS s, min(v) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS m FROM t",
        "SELECT id, min(v) OVER w AS mn, max(v) OVER w AS mx, avg(v) OVER w AS a, count(v) OVER w AS c FROM t WINDOW w AS (PARTITION BY g ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING)",
        "SELECT id, sum(v) OVER (ORDER BY v RANGE BETWEEN 2 PRECEDING AND 1 FOLLOWING) AS s, count(v) OVER (ORDER BY v DESC RANGE BETWEEN 1 FOLLOWING AND 3 FOLLOWING) AS c, max(v) OVER (ORDER BY v NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS mx FROM t",
        "SELECT id, sum(v) OVER (PARTITION BY g ORDER BY v GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s, count(*) OVER (PARTITION BY g ORDER BY v GROUPS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS c FROM t",
        "SELECT id, lag(v) OVER (PARTITION BY g ORDER BY id) AS l1, lead(v, 2, -1) OVER (PARTITION BY g ORDER BY id) AS l2, ntile(3) OVER (PARTITION BY g ORDER BY id) AS nt FROM t",
        "SELECT id, first_value(v) OVER w AS f, last_value(v) OVER w AS l, nth_value(v, 2) OVER w AS n2 FROM t WINDOW w AS (PARTITION BY g ORDER BY id ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING)",
        "SELECT id, first_value(v) OVER (PARTITION BY g ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS f, last_value(v) OVER (PARTITION BY g ORDER BY v GROUPS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS l FROM t",
        "SELECT id, string_agg(v::text, ',') OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS sa, array_agg(v) OVER (PARTITION BY g ORDER BY id) AS aa FROM t",
        "SELECT g, v, count(*) OVER (PARTITION BY g ORDER BY v) AS c, sum(v) OVER (PARTITION BY g ORDER BY v ROWS UNBOUNDED PRECEDING) AS s, row_number() OVER (PARTITION BY g ORDER BY v) AS rn FROM t",
        "SELECT id, count(*) OVER (PARTITION BY g) AS c, sum(v) OVER () AS s, max(v) OVER () AS mx FROM t",
        "SELECT id, rank() OVER w AS r, sum(v) OVER (w ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s, max(v) OVER (w ROWS UNBOUNDED PRECEDING) AS m, sum(v) OVER (w ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) AS p FROM t WINDOW w AS (PARTITION BY g ORDER BY v, id)",
        "SELECT id, dense_rank() OVER (ORDER BY v) AS d, sum(v) OVER (ORDER BY v ROWS BETWEEN 3 FOLLOWING AND UNBOUNDED FOLLOWING) AS s FROM t",
        "SELECT id, ntile(v) OVER (PARTITION BY g ORDER BY id) AS nt FROM t",
        "SELECT id, count(*) OVER (PARTITION BY g ORDER BY d RANGE BETWEEN interval '2 days' PRECEDING AND interval '1 day' FOLLOWING) AS c FROM (SELECT id, g, date '2026-01-01' + v AS d FROM t) s",
        "SELECT id, sum(v) OVER (PARTITION BY g ORDER BY v GROUPS BETWEEN 40 PRECEDING AND 1 PRECEDING) AS s, lag(v, 2) OVER (PARTITION BY g ORDER BY id) AS l FROM t",
    ];
    // (partitions of more than 32 rows are changed in part, smaller ones whole)
    for seed in [1, 2] {
        random_changes_keep_views_equal(&format!("window-random-{seed}"), &queries, 60, seed * 104_729);
    }
    for seed in [3, 4, 5] {
        random_changes_keep_views_equal_from(&format!("window-random-large-{seed}"), &queries, 80, seed * 104_729, 150);
    }
}

/// The partitions of a window operator that are in error are remembered
/// (and checkpointed): a change to part of such a partition, also after a
/// restart, keeps it in error until the failing row is gone, and changes
/// to other partitions are applied in part (partitions of more than 32
/// rows).
#[test]
fn window_partition_errors_across_restart() {
    let dir = tmpdir("window-errors");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, g int, v int)");
    exec(&h, "INSERT INTO t SELECT i, i % 2, 1 + i % 5 FROM generate_series(1, 200) i");
    let q = "SELECT id, ntile(v) OVER (PARTITION BY g ORDER BY id) AS nt, sum(v) OVER (PARTITION BY g ORDER BY id) AS s FROM t";
    exec(&h, &format!("CREATE MATERIALIZED VIEW w AS {q}"));
    let check = |h: &EngineHandle| assert_eq!(rows_of(h, "SELECT * FROM w").map(sorted), rows_of(h, q).map(sorted));
    check(&h);
    exec(&h, "UPDATE t SET v = 0 WHERE id = 10");
    assert_eq!(read_error(&h, "SELECT * FROM w"), Some("22014 argument of ntile must be greater than zero".to_string()));
    exec(&h, "UPDATE t SET v = 3 WHERE id = 12");
    exec(&h, "CHECKPOINT");
    running.shutdown();
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "UPDATE t SET v = 2 WHERE id = 14");
    exec(&h, "UPDATE t SET v = 2 WHERE id = 15");
    assert_eq!(read_error(&h, "SELECT * FROM w"), Some("22014 argument of ntile must be greater than zero".to_string()));
    exec(&h, "UPDATE t SET v = 1 WHERE id = 10");
    check(&h);
    exec(&h, "INSERT INTO t VALUES (201, 1, 4)");
    check(&h);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
