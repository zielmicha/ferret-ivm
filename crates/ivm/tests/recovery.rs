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

fn try_exec(h: &EngineHandle, sql: &str) -> Result<ExecResult, String> {
    let stmt = ivm::sql::parse_statement(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    h.execute(stmt, vec![], SessionVars::default(), String::new()).map_err(|e| e.message)
}

/// Views reading a replaced view must read its new definition at once, as
/// they do after a restart (state is a function of the log only).
#[test]
fn views_over_a_replaced_view_follow_its_new_definition() {
    let dir = tmpdir("replace-deps");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, v int)");
    exec(&h, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    exec(&h, "CREATE VIEW plain AS SELECT id, v FROM t WHERE v > 10");
    exec(&h, "CREATE MATERIALIZED VIEW x AS SELECT id, v FROM t WHERE v > 10");
    exec(&h, "CREATE MATERIALIZED VIEW z AS SELECT count(*) AS n FROM x");
    exec(&h, "CREATE MATERIALIZED VIEW w AS SELECT z.n, count(*) AS m FROM z, x GROUP BY z.n");
    exec(&h, "CREATE MATERIALIZED VIEW p AS SELECT count(*) AS n FROM plain");
    exec(&h, "CREATE OR REPLACE MATERIALIZED VIEW x AS SELECT id, v FROM t WHERE v > 20");
    exec(&h, "CREATE OR REPLACE VIEW plain AS SELECT id, v FROM t WHERE v > 25");
    exec(&h, "INSERT INTO t VALUES (4, 15), (5, 50)");
    let state = |h: &EngineHandle| [rows(h, "SELECT * FROM x ORDER BY id"), rows(h, "SELECT * FROM z"), rows(h, "SELECT * FROM w"), rows(h, "SELECT * FROM p")];
    let live = state(&h);
    assert_eq!(live, [vec!["3|30".to_string(), "5|50".to_string()], vec!["2".to_string()], vec!["2|2".to_string()], vec!["2".to_string()]]);
    running.shutdown();
    let running = start(&dir);
    let h = running.handle.clone();
    assert_eq!(state(&h), live);
    exec(&h, "DELETE FROM t WHERE id = 3");
    assert_eq!(rows(&h, "SELECT * FROM z"), ["1"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Indexes on a replaced materialized view index its new version.
#[test]
fn indexes_follow_a_replaced_view() {
    let dir = tmpdir("replace-index");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, v int)");
    exec(&h, "INSERT INTO t VALUES (1, 10), (2, 20)");
    exec(&h, "CREATE MATERIALIZED VIEW x AS SELECT id, v * 2 AS v FROM t");
    exec(&h, "CREATE INDEX xi ON x (v)");
    exec(&h, "CREATE OR REPLACE MATERIALIZED VIEW x AS SELECT id, v + 1 AS v FROM t");
    // every arrangement belongs to a live operator (this used to panic the engine)
    let arrs = rows(&h, "SHOW ARRANGEMENTS");
    exec(&h, "INSERT INTO t VALUES (3, 30)");
    assert_eq!(rows(&h, "SELECT * FROM x ORDER BY id"), ["1|11", "2|21", "3|31"]);
    running.shutdown();
    let running = start(&dir);
    let h = running.handle.clone();
    assert_eq!(rows(&h, "SHOW ARRANGEMENTS").len(), arrs.len());
    assert_eq!(rows(&h, "SELECT * FROM x ORDER BY id"), ["1|11", "2|21", "3|31"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A replacement that would invalidate a dependent is rejected and changes
/// nothing (the engine must also still start afterwards).
#[test]
fn invalidating_view_replacements_are_rejected() {
    let dir = tmpdir("replace-reject");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, v int)");
    exec(&h, "INSERT INTO t VALUES (1, 10)");
    exec(&h, "CREATE MATERIALIZED VIEW x AS SELECT id, v FROM t");
    exec(&h, "CREATE MATERIALIZED VIEW z AS SELECT v FROM x");
    exec(&h, "CREATE MATERIALIZED VIEW y AS SELECT id, v FROM t");
    exec(&h, "CREATE INDEX yi ON y (v)");
    exec(&h, "CREATE SINK s FROM y INTO POSTGRES CONNECTION 'host=/nonexistent' TABLE 'ys'");
    for sql in [
        "CREATE OR REPLACE MATERIALIZED VIEW x AS SELECT id FROM t",         // z reads x.v
        "CREATE OR REPLACE MATERIALIZED VIEW y AS SELECT v AS w, id FROM t", // yi indexes y.v
        "CREATE OR REPLACE VIEW y AS SELECT id, v FROM t",                   // an index needs a materialized view
    ] {
        assert!(try_exec(&h, sql).is_err(), "{sql} should be rejected");
    }
    exec(&h, "INSERT INTO t VALUES (2, 20)");
    assert_eq!(rows(&h, "SELECT * FROM z ORDER BY v"), ["10", "20"]);
    assert_eq!(rows(&h, "SELECT * FROM y ORDER BY id"), ["1|10", "2|20"]);
    running.shutdown();
    let running = start(&dir);
    let h = running.handle.clone();
    assert_eq!(rows(&h, "SELECT * FROM z ORDER BY v"), ["10", "20"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
