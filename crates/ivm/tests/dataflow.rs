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
