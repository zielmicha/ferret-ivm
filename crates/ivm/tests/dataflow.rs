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
