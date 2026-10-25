//! Recovery and durability regression tests through the public API (no
//! PostgreSQL needed).

use ivm::coord::api::{ExecResult, SessionVars};
use ivm::coord::{self, EngineHandle};
use ivm::types::text::format_datum;
use std::path::PathBuf;

fn tmpdir(name: &str) -> PathBuf {
    let base = std::env::var("IVM_TEST_TMP").unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().into_owned());
    let d = PathBuf::from(base).join(format!("ivm-recovery-{name}-{}", std::process::id()));
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

fn rows(h: &EngineHandle, sql: &str) -> Vec<String> {
    match exec(h, sql) {
        ExecResult::Query { rows, .. } => {
            rows.iter().map(|r| r.iter().map(format_datum).collect::<Vec<_>>().join("|")).collect()
        }
        other => panic!("{sql}: expected rows, got {other:?}"),
    }
}

/// CREATE OR REPLACE keeps a view's object id, so a replaced view can read
/// from a view created after it. Recovery re-plans the checkpointed catalog
/// and must still build every view after the views it reads.
#[test]
fn recovery_replans_views_in_dependency_order() {
    let dir = tmpdir("replace-order");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, v int)");
    exec(&h, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    exec(&h, "CREATE MATERIALIZED VIEW x AS SELECT id FROM t");
    exec(&h, "CREATE MATERIALIZED VIEW y AS SELECT id, v FROM t WHERE v > 10");
    // x (the older object) now reads y (the newer one)
    exec(&h, "CREATE OR REPLACE MATERIALIZED VIEW x AS SELECT id, v * 2 AS w FROM y");
    assert_eq!(rows(&h, "SELECT * FROM x ORDER BY id"), ["2|40", "3|60"]);
    exec(&h, "CHECKPOINT");
    running.shutdown();

    let running = start(&dir);
    let h = running.handle.clone();
    assert_eq!(rows(&h, "SELECT * FROM x ORDER BY id"), ["2|40", "3|60"]);
    exec(&h, "INSERT INTO t VALUES (4, 40)");
    assert_eq!(rows(&h, "SELECT * FROM x ORDER BY id"), ["2|40", "3|60", "4|80"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
