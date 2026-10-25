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

/// Variance and standard deviation of large int8 values: the sum of their
/// squares does not fit 128 bits (it used to overflow, panicking debug
/// builds and wrapping around in release builds).
#[test]
fn int8_variance_does_not_overflow() {
    let dir = tmpdir("int8-var");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, b int8)");
    let q = "SELECT var_pop(b) AS vp, stddev_samp(b) AS ss, variance(b) AS v, count(*) AS n FROM t";
    exec(&h, &format!("CREATE MATERIALIZED VIEW v AS {q}"));
    exec(&h, "INSERT INTO t VALUES (1, 9223372036854775807), (2, 9223372036854775807), (3, 9223372036854775807), (4, -9223372036854775808)");
    // values computed by PostgreSQL 17
    let want = [
        "63802943797675961892465209865815457792|9223372036854775807|85070591730234615856620279821087277056|4".to_string(),
    ];
    assert_eq!(rows(&h, "SELECT * FROM v"), want);
    assert_eq!(rows(&h, q), want);
    exec(&h, "DELETE FROM t WHERE id = 4");
    assert_eq!(rows(&h, "SELECT * FROM v"), ["0|0|0|3"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// The log can lose records a checkpoint covers (its unsynced tail, with
/// --no-sync). New records must not reuse those GSNs: they used to be taken
/// for already applied ones and skipped (their DML never answered), and
/// fresh reads went back to the state at the end of the log.
#[test]
fn log_tail_lost_behind_a_checkpoint() {
    let dir = tmpdir("lost-tail");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY)");
    for i in 0..5 {
        exec(&h, &format!("INSERT INTO t VALUES ({i})"));
    }
    exec(&h, "CHECKPOINT");
    running.shutdown();
    let seg = std::fs::read_dir(dir.join("log")).unwrap().next().unwrap().unwrap().path();
    let len = std::fs::metadata(&seg).unwrap().len();
    std::fs::OpenOptions::new().write(true).open(&seg).unwrap().set_len(len / 3).unwrap();
    let running = start(&dir);
    let h = running.handle.clone();
    assert_eq!(rows(&h, "SELECT count(*) FROM t"), ["5"]);
    let h2 = h.clone();
    let insert = std::thread::spawn(move || try_exec(&h2, "INSERT INTO t VALUES (100)"));
    let t0 = std::time::Instant::now();
    while !insert.is_finished() {
        assert!(t0.elapsed() < std::time::Duration::from_secs(20), "INSERT never answered");
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    insert.join().unwrap().unwrap();
    assert_eq!(rows(&h, "SELECT count(*) FROM t"), ["6"]);
    running.shutdown();
    let running = start(&dir);
    let h = running.handle.clone();
    assert_eq!(rows(&h, "SELECT count(*) FROM t"), ["6"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// AS OF reads use only arrangements whose history covers the read's GSN:
/// an index built later (it has no history before its creation) used to
/// answer point queries with nothing, and a view rebuilt by CREATE OR
/// REPLACE silently read as empty before the replacement. Also across a
/// restart (the history starts are checkpointed).
#[test]
fn as_of_reads_use_arrangements_with_history() {
    let dir = tmpdir("asof-history");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, v int)");
    exec(&h, "INSERT INTO t VALUES (1, 10), (2, 20)");
    exec(&h, "CREATE MATERIALIZED VIEW x AS SELECT id, v FROM t WHERE v > 10");
    let g: String = rows(&h, "SHOW GSN").remove(0);
    exec(&h, "INSERT INTO t VALUES (3, 10)");
    exec(&h, "CREATE INDEX ti ON t (v)");
    exec(&h, "CREATE OR REPLACE MATERIALIZED VIEW x AS SELECT id, v FROM t WHERE v > 0");
    let check = |h: &EngineHandle| {
        assert_eq!(rows(h, &format!("SELECT * FROM t WHERE v = 10 ORDER BY id AS OF {g}")), ["1|10"]);
        assert_eq!(rows(h, &format!("SELECT * FROM t WHERE v = 10 ORDER BY id")), ["1|10", "3|10"]);
        let err = try_exec(h, &format!("SELECT * FROM x AS OF {g}")).unwrap_err();
        assert!(err.contains("before the history"), "{err}");
    };
    check(&h);
    running.shutdown();
    let running = start(&dir);
    check(&running.handle);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A view whose dataflow is shared with an existing view (here: the same
/// query) reports that view's errors; it used to start without them and
/// read its partial rows as if valid (also after a restart).
#[test]
fn views_sharing_a_dataflow_share_its_errors() {
    let dir = tmpdir("shared-errors");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (a int PRIMARY KEY, b int)");
    exec(&h, "INSERT INTO t VALUES (1, 0), (2, 1)");
    exec(&h, "CREATE MATERIALIZED VIEW v1 AS SELECT a, 10 / b AS x FROM t");
    exec(&h, "CREATE MATERIALIZED VIEW v2 AS SELECT a, 10 / b AS x FROM t");
    for v in ["v1", "v2"] {
        assert_eq!(try_exec(&h, &format!("SELECT * FROM {v}")).unwrap_err(), "division by zero", "{v}");
    }
    running.shutdown();
    let running = start(&dir);
    let h = running.handle.clone();
    assert_eq!(try_exec(&h, "SELECT * FROM v2").unwrap_err(), "division by zero");
    exec(&h, "DELETE FROM t WHERE a = 1");
    assert_eq!(rows(&h, "SELECT * FROM v2"), ["2|10"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A view depends on the relations it reads, not on every object whose
/// name it happens to contain (here a column named like a table): DROP used
/// to refuse, and DROP ... CASCADE dropped the unrelated view.
#[test]
fn dependencies_are_the_relations_read() {
    let dir = tmpdir("false-deps");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, total int)");
    exec(&h, "CREATE TABLE total (id int PRIMARY KEY)");
    exec(&h, "CREATE TABLE u (id int PRIMARY KEY)");
    exec(&h, "CREATE MATERIALIZED VIEW v AS SELECT id, total FROM t");
    exec(&h, "CREATE VIEW pv AS SELECT id FROM u");
    exec(&h, "CREATE MATERIALIZED VIEW w AS SELECT count(*) AS n FROM pv");
    exec(&h, "DROP TABLE total");
    assert_eq!(rows(&h, "SELECT count(*) FROM v"), ["0"]);
    // real dependencies, also through a plain view, still hold
    assert!(try_exec(&h, "DROP TABLE u").is_err());
    exec(&h, "DROP TABLE u CASCADE");
    for gone in ["pv", "w"] {
        assert!(try_exec(&h, &format!("SELECT * FROM {gone}")).is_err(), "{gone} should be gone");
    }
    assert_eq!(rows(&h, "SELECT count(*) FROM v"), ["0"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
