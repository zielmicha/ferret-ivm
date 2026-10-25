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
