//! Recovery and durability regression tests through the public API (no
//! PostgreSQL needed).

use ivm::coord::api::{ExecResult, SessionVars};
use ivm::coord::{self, EngineHandle};
use ivm::types::text::format_datum;
use std::path::{Path, PathBuf};
use std::time::Duration;

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

/// A commit larger than a log frame (a statement inserting 3 MB) is logged
/// as a parts file of bounded frames and a CommitParts record, and is
/// replayed from them after a crash; the checkpoint that covers it deletes
/// the parts file with the log segments before it.
#[test]
fn large_commits_replay_from_their_parts() {
    let dir = tmpdir("parts");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, s text)");
    exec(&h, "CREATE MATERIALIZED VIEW c AS SELECT count(*) AS n, sum(length(s)) AS l FROM t");
    exec(&h, "CHECKPOINT");
    exec(&h, "INSERT INTO t SELECT g, repeat('x', 1000) FROM generate_series(1, 3000) g");
    exec(&h, "DELETE FROM t WHERE id <= 1000");
    let parts = || std::fs::read_dir(dir.join("log")).unwrap().filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().starts_with("parts-")).count();
    assert_eq!(parts(), 1);
    assert_eq!(rows(&h, "SELECT * FROM c"), ["2000|2000000"]);
    running.crash();
    let running = start(&dir);
    let h = running.handle.clone();
    assert_eq!(rows(&h, "SELECT * FROM c"), ["2000|2000000"]);
    assert_eq!(rows(&h, "SELECT count(*), min(id) FROM t"), ["2000|1001"]);
    assert_eq!(parts(), 1);
    exec(&h, "CHECKPOINT");
    // (the sequencer truncates after the checkpoint is committed)
    let t0 = std::time::Instant::now();
    while parts() != 0 {
        assert!(t0.elapsed() < Duration::from_secs(10), "the checkpoint left the parts file");
        std::thread::sleep(Duration::from_millis(20));
    }
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

/// DML planned before a concurrent DROP of its table (logged just before
/// it) applies nothing; it used to report success.
#[test]
fn dml_racing_a_drop_of_its_table_fails() {
    use ivm::coord::api::Request;
    use ivm::coord::EngineMsg;
    let dir = tmpdir("dml-drop");
    // fsync: the DROP's group commit takes long enough for the INSERT to be planned before it applies
    let running = coord::start(coord::Config { data_dir: dir.clone(), sync: true, tick: None, ..Default::default() }).unwrap();
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY)");
    let send = |sql: &str| {
        let (tx, rx) = crossbeam_channel::bounded(1);
        let stmt = ivm::sql::parse_statement(sql).unwrap();
        h.tx.send(EngineMsg::Request(Request::Execute { stmt: Box::new(stmt), params: vec![], vars: SessionVars::default(), extra: String::new(), reply: tx }))
            .unwrap();
        rx
    };
    let drop = send("DROP TABLE t");
    let ins = send("INSERT INTO t VALUES (1)");
    drop.recv().unwrap().unwrap();
    assert!(ins.recv().unwrap().is_err(), "the INSERT applied nothing");
    // later DML is not held up
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY)");
    exec(&h, "INSERT INTO t VALUES (2)");
    assert_eq!(rows(&h, "SELECT * FROM t"), ["2"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Replacing a view so that a materialized view reading it would become
/// non-deterministic is rejected (it used to pass, and the dependent view
/// was then dropped while being re-planned).
#[test]
fn replacements_keep_dependents_deterministic() {
    let dir = tmpdir("replace-nondet");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY)");
    exec(&h, "INSERT INTO t VALUES (1)");
    exec(&h, "CREATE VIEW pv AS SELECT id, id * 1.0 AS r FROM t");
    exec(&h, "CREATE MATERIALIZED VIEW m AS SELECT count(*) AS n, sum(r) AS s FROM pv");
    assert!(try_exec(&h, "CREATE OR REPLACE VIEW pv AS SELECT id, random() AS r FROM t").is_err());
    exec(&h, "INSERT INTO t VALUES (2)");
    assert_eq!(rows(&h, "SELECT * FROM m"), ["2|3.0"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Sizes of the store's manifest and of all its files.
fn store_sizes(dir: &Path) -> (u64, u64) {
    let store = dir.join("store");
    let manifest = std::fs::metadata(store.join("MANIFEST")).unwrap().len();
    (manifest, std::fs::read_dir(&store).unwrap().map(|e| e.unwrap().metadata().unwrap().len()).sum())
}

/// Bytes a checkpoint after one INSERT appends to the manifest and to the
/// store.
fn insert_checkpoint_cost(h: &EngineHandle, dir: &Path, id: i32) -> (u64, u64) {
    let (m0, s0) = store_sizes(dir);
    exec(h, &format!("INSERT INTO t VALUES ({id})"));
    exec(h, "CHECKPOINT");
    let (m1, s1) = store_sizes(dir);
    (m1 - m0, s1 - s0)
}

/// A checkpoint writes what changed since the previous one, so its cost
/// does not depend on the size of what did not change, such as the DDL
/// history or the error records of a view over dirty data. Every
/// checkpoint used to write the whole catalog with its history (up to
/// 11,000 statements), every error record and the rest of the engine state
/// as JSON into its manifest record, and an idle engine took one every
/// interval.
#[test]
fn checkpoint_cost_does_not_grow_with_the_catalog_history_or_the_errors() {
    let dir = tmpdir("checkpoint-cost");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY)");
    exec(&h, "CREATE TABLE dirty (id int PRIMARY KEY, v text)");
    exec(&h, "CREATE MATERIALIZED VIEW parsed AS SELECT id, v::int AS n FROM dirty");
    exec(&h, "CHECKPOINT");
    let before = insert_checkpoint_cost(&h, &dir, 1);
    for i in 0..1000 {
        exec(&h, &format!("CREATE TABLE x{i} (a int, b text, c numeric)"));
        exec(&h, &format!("DROP TABLE x{i}"));
    }
    // 2,000 error records, one per value that does not parse
    let values: Vec<String> = (0..2000).map(|i| format!("({i}, 'bad value {i}')")).collect();
    exec(&h, &format!("INSERT INTO dirty VALUES {}", values.join(", ")));
    assert_eq!(rows(&h, "SHOW VIEWS"), ["parsed|materialized|0|2000|ok"]);
    exec(&h, "CHECKPOINT");
    let after = insert_checkpoint_cost(&h, &dir, 2);
    // (varints of GSNs, node offsets, sizes and ids grew by a byte or two
    // each; the leaf of t gained a row)
    assert!(after.0 <= before.0 + 40 && after.1 <= before.1 + 80, "{before:?} -> {after:?}");
    assert!(after.0 < 200, "a manifest record of {} bytes", after.0);
    running.shutdown();
    let running = start(&dir);
    let h = running.handle.clone();
    assert_eq!(rows(&h, "SELECT count(*) FROM ivm.catalog_history"), ["2003"]);
    assert_eq!(rows(&h, "SELECT statement FROM ivm.catalog_history ORDER BY gsn DESC LIMIT 1"), ["DROP TABLE x999"]);
    assert_eq!(rows(&h, "SELECT * FROM t ORDER BY id"), ["1", "2"]);
    assert_eq!(rows(&h, "SHOW VIEWS"), ["parsed|materialized|0|2000|ok"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

fn frontier(h: &EngineHandle, name: &str) -> u64 {
    let row = rows(h, "SHOW FRONTIERS").into_iter().find(|r| r.starts_with(&format!("{name}|"))).unwrap();
    row[name.len() + 1..].parse().unwrap()
}

/// An idle engine takes no checkpoints: clock ticks that change nothing but
/// the clock do not make one due.
#[test]
fn an_idle_engine_takes_no_checkpoints() {
    let dir = tmpdir("idle");
    let tick = Some(std::time::Duration::from_millis(2));
    let cfg = coord::Config { data_dir: dir.clone(), sync: false, tick, checkpoint_interval: std::time::Duration::from_millis(10), ..Default::default() };
    let running = coord::start(cfg).unwrap();
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY)");
    exec(&h, "CHECKPOINT");
    let before = frontier(&h, "checkpoint_gsn");
    // a change makes one due
    exec(&h, "INSERT INTO t VALUES (1)");
    let t0 = std::time::Instant::now();
    while frontier(&h, "checkpoint_gsn") == before {
        assert!(t0.elapsed() < std::time::Duration::from_secs(10), "no checkpoint after the INSERT");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let (checkpoint, sizes) = (frontier(&h, "checkpoint_gsn"), store_sizes(&dir));
    // fifty ticks later
    while frontier(&h, "processed_gsn") < checkpoint + 50 {
        assert!(t0.elapsed() < std::time::Duration::from_secs(20), "the clock does not tick");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!((frontier(&h, "checkpoint_gsn"), store_sizes(&dir)), (checkpoint, sizes));
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// The error state survives restarts exactly, also through the changes
/// made since the last checkpoint and replayed after a crash: error
/// records of views and the errors of aggregation groups, also of views
/// dropped and created again (whose records replace the old ones rather
/// than adding to them).
#[test]
fn the_error_state_survives_restarts() {
    let dir = tmpdir("error-state");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, v text)");
    exec(&h, "INSERT INTO t VALUES (1, '10'), (2, 'x'), (3, 'y')");
    exec(&h, "CREATE MATERIALIZED VIEW m AS SELECT id, v::int AS n FROM t");
    exec(&h, "CREATE TABLE r (k int PRIMARY KEY, g int, x real)");
    exec(&h, "INSERT INTO r VALUES (1, 1, 3e38), (2, 1, 3e38), (3, 2, 1)");
    exec(&h, "CREATE MATERIALIZED VIEW s AS SELECT g, sum(x) AS total FROM r GROUP BY g");
    exec(&h, "CHECKPOINT");
    exec(&h, "INSERT INTO t VALUES (4, 'z')");
    exec(&h, "DROP MATERIALIZED VIEW s");
    exec(&h, "CREATE MATERIALIZED VIEW s AS SELECT g, sum(x) AS total FROM r GROUP BY g");
    exec(&h, "CHECKPOINT");
    exec(&h, "DROP MATERIALIZED VIEW m");
    exec(&h, "CREATE MATERIALIZED VIEW m AS SELECT id, v::int AS n FROM t");
    exec(&h, "INSERT INTO r VALUES (4, 3, 3e38), (5, 3, 3e38)");
    running.crash();
    let running = start(&dir);
    let h = running.handle.clone();
    assert_eq!(rows(&h, "SHOW VIEWS"), ["s|materialized|1|1|ok", "m|materialized|1|3|ok"]);
    exec(&h, "CHECKPOINT");
    running.shutdown();
    let running = start(&dir);
    let h = running.handle.clone();
    assert_eq!(rows(&h, "SHOW VIEWS"), ["s|materialized|1|1|ok", "m|materialized|1|3|ok"]);
    exec(&h, "DELETE FROM t WHERE id > 1");
    exec(&h, "DELETE FROM r WHERE k IN (2, 5)");
    assert_eq!(rows(&h, "SELECT * FROM m"), ["1|10"]);
    assert_eq!(rows(&h, "SELECT * FROM s ORDER BY g"), ["1|3e+38", "2|1", "3|3e+38"]);
    running.shutdown();
    let running = start(&dir);
    let h = running.handle.clone();
    assert_eq!(rows(&h, "SHOW VIEWS"), ["s|materialized|3|0|ok", "m|materialized|1|0|ok"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
