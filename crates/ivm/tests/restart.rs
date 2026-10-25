//! Restarts: recovery of data directories whose catalog or checkpoint the
//! current code no longer accepts as it is, and what survives a restart.

use ivm::coord::api::{ExecResult, SessionVars};
use ivm::coord::inspect::{InspectQuery, InspectReply};
use ivm::coord::{self, EngineHandle};
use ivm::error::SqlError;
use ivm::types::text::format_datum;
use std::path::{Path, PathBuf};
use std::time::Duration;

fn tmpdir(name: &str) -> PathBuf {
    let base = std::env::var("IVM_TEST_TMP").unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().into_owned());
    let d = PathBuf::from(base).join(format!("ivm-restart-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn start_in(dir: &Path) -> coord::Running {
    let cfg = coord::Config { data_dir: dir.to_path_buf(), sync: false, tick: None, ..Default::default() };
    coord::start(cfg).expect("engine starts")
}

struct Engine {
    running: coord::Running,
    h: EngineHandle,
}

impl Engine {
    fn start(dir: &Path) -> Engine {
        let running = start_in(dir);
        let h = running.handle.clone();
        Engine { running, h }
    }

    fn shutdown(self) {
        self.running.shutdown();
    }

    fn try_exec(&self, sql: &str) -> Result<ExecResult, SqlError> {
        let stmt = ivm::sql::parse_statement(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        self.h.execute(stmt, vec![], SessionVars::default(), String::new())
    }

    fn exec(&self, sql: &str) -> ExecResult {
        self.try_exec(sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    /// The error of a statement that must fail.
    fn err(&self, sql: &str) -> SqlError {
        match self.try_exec(sql) {
            Ok(r) => panic!("{sql}: expected an error, got {r:?}"),
            Err(e) => e,
        }
    }

    fn rows(&self, sql: &str) -> Vec<String> {
        match self.exec(sql) {
            ExecResult::Query { rows, .. } => rows.iter().map(|r| r.iter().map(format_datum).collect::<Vec<_>>().join("|")).collect(),
            other => panic!("{sql}: expected rows, got {other:?}"),
        }
    }
}

/// Rewrite the application state (catalog and engine state, JSON) of the
/// last checkpoint of a stopped engine's data directory.
fn rewrite_checkpoint(dir: &Path, f: impl FnOnce(&mut serde_json::Value)) {
    let (mut env, rec) = ivm::storage::Env::open(&dir.join("store"), Default::default()).unwrap();
    let rec = rec.expect("a checkpoint");
    let mut app: serde_json::Value = serde_json::from_slice(&rec.app).unwrap();
    f(&mut app);
    env.checkpoint(rec.gsn, &serde_json::to_vec(&app).unwrap()).unwrap();
}

/// Give materialized view `name` of a checkpointed catalog a definition
/// that no longer plans (as stricter code or catalog drift would).
fn break_view(dir: &Path, name: &str, sql: &str) {
    rewrite_checkpoint(dir, |app| {
        let objects = app["catalog"]["objects"].as_object_mut().unwrap();
        let v = objects.values_mut().find(|o| o["name"] == name).expect("the view");
        v["kind"]["MaterializedView"]["sql"] = serde_json::Value::String(sql.into());
    });
}

/// A materialized view whose stored definition no longer plans used to
/// keep the engine from starting. It is kept in the catalog, marked broken:
/// the engine starts, reads of it (and of what reads it) fail with the
/// reason, SHOW VIEWS and the web UI show it, its indexes and sinks are
/// left alone, and DROP or CREATE OR REPLACE fix it.
#[test]
fn views_that_no_longer_plan_are_broken_not_fatal() {
    let dir = tmpdir("broken-views");
    let e = Engine::start(&dir);
    e.exec("CREATE TABLE t (a int PRIMARY KEY, b int)");
    e.exec("INSERT INTO t VALUES (1, 10), (2, 20)");
    e.exec("CREATE MATERIALIZED VIEW v AS SELECT a, b FROM t");
    e.exec("CREATE INDEX vi ON v (b)");
    e.exec("CREATE MATERIALIZED VIEW w AS SELECT count(*) AS n FROM v");
    e.exec("CREATE VIEW pv AS SELECT a FROM v");
    e.exec("CREATE MATERIALIZED VIEW ok AS SELECT a FROM t");
    e.exec("CREATE SINK s FROM v INTO POSTGRES CONNECTION 'host=127.0.0.1 port=1 connect_timeout=1' TABLE 'target'");
    e.exec("CHECKPOINT");
    e.shutdown();
    break_view(&dir, "v", "SELECT a, nosuch FROM t");

    let e = Engine::start(&dir);
    assert_eq!(e.rows("SELECT a FROM ok ORDER BY a"), ["1", "2"]);
    e.exec("INSERT INTO t VALUES (3, 30)");
    assert_eq!(e.rows("SELECT a FROM ok ORDER BY a"), ["1", "2", "3"]);
    let reason = "column \"nosuch\" does not exist";
    for (sql, what) in [
        ("SELECT * FROM v", "materialized view \"v\" is broken"),
        ("SELECT * FROM w", "materialized view \"w\" is broken"),
        ("SELECT * FROM pv", "materialized view \"v\" is broken"),
        ("SUBSCRIBE TO v", "materialized view \"v\" is broken"),
        ("SUBSCRIBE TO (SELECT a FROM v)", "materialized view \"v\" is broken"),
        ("EXPLAIN DATAFLOW FOR v", "materialized view \"v\" is broken"),
        ("CREATE INDEX vi2 ON v (a)", "materialized view \"v\" is broken"),
        ("CREATE MATERIALIZED VIEW w2 AS SELECT a FROM v", "materialized view \"v\" is broken"),
        ("CREATE SINK s2 FROM v INTO POSTGRES CONNECTION 'host=127.0.0.1 port=1' TABLE 'x'", "materialized view \"v\" is broken"),
    ] {
        let er = e.err(sql);
        assert_eq!(er.code, "55000", "{sql}: {er}");
        assert!(er.message.starts_with(what) && er.message.contains(reason), "{sql}: {}", er.message);
    }
    assert_eq!(
        e.rows("SHOW VIEWS"),
        [
            format!("v|materialized|||broken: {reason}"),
            format!("w|materialized|||broken: materialized view \"v\" is broken: {reason}"),
            "pv|virtual|||".to_string(),
            "ok|materialized|3|0|ok".to_string(),
        ]
    );
    assert_eq!(e.rows("SHOW INDEXES"), ["vi|v|[1]"]);
    let sinks = e.rows("SHOW SINKS");
    assert!(sinks.len() == 1 && sinks[0].starts_with("s|v|target|error: broken: materialized view \"v\" is broken"), "{sinks:?}");
    e.rows("SHOW ARRANGEMENTS");
    match e.h.inspect_wait(InspectQuery::Errors, Duration::from_secs(10)).unwrap() {
        InspectReply::Errors(d) => assert_eq!(d.broken.iter().map(|b| b.name.as_str()).collect::<Vec<_>>(), ["v", "vi", "w"]),
        other => panic!("unexpected reply {other:?}"),
    }
    match e.h.inspect_wait(InspectQuery::Catalog, Duration::from_secs(10)).unwrap() {
        InspectReply::Catalog(c) => {
            let v = c.objects.iter().find(|o| o.name == "v").unwrap();
            assert_eq!(v.status.as_deref(), Some(format!("error: broken: {reason}").as_str()));
        }
        other => panic!("unexpected reply {other:?}"),
    }

    // a replacement repairs it, and the views reading it; its index and
    // sink are rebuilt and started
    e.exec("CREATE OR REPLACE MATERIALIZED VIEW v AS SELECT a, b FROM t");
    assert_eq!(e.rows("SELECT * FROM v ORDER BY a"), ["1|10", "2|20", "3|30"]);
    assert_eq!(e.rows("SELECT n FROM w"), ["3"]);
    assert_eq!(e.rows("SELECT a FROM pv ORDER BY a"), ["1", "2", "3"]);
    assert!(e.rows("SHOW VIEWS").iter().all(|r| r.ends_with("|ok") || r.ends_with("|")), "{:?}", e.rows("SHOW VIEWS"));
    assert!(!e.rows("SHOW SINKS")[0].contains("broken"));
    e.exec("INSERT INTO t VALUES (4, 40)");
    assert_eq!(e.rows("SELECT n FROM w"), ["4"]);
    e.shutdown();
    let e = Engine::start(&dir);
    assert_eq!(e.rows("SELECT n FROM w"), ["4"]);
    e.exec("CHECKPOINT");
    e.shutdown();

    // DROP removes a broken view (with CASCADE, what reads it)
    break_view(&dir, "v", "SELECT a, nosuch FROM t");
    let e = Engine::start(&dir);
    assert_eq!(e.err("DROP MATERIALIZED VIEW v").code, "2BP01");
    e.exec("DROP MATERIALIZED VIEW v CASCADE");
    assert_eq!(e.rows("SELECT name FROM ivm.objects ORDER BY name"), ["ok", "t"]);
    assert_eq!(e.rows("SHOW SINKS"), Vec::<String>::new());
    e.shutdown();
    let e = Engine::start(&dir);
    assert_eq!(e.rows("SELECT name FROM ivm.objects ORDER BY name"), ["ok", "t"]);
    assert_eq!(e.rows("SELECT a FROM ok ORDER BY a"), ["1", "2", "3", "4"]);
    e.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// After an upstream schema change, the indexes on the table move to its
/// new version, following their columns by name (a dropped column moves
/// the ones after it), and those whose columns are gone are dropped. They
/// used to stay on the old table node (SHOW ARRANGEMENTS panicked with
/// "dangling node", start-up panicked when an indexed column was gone).
#[test]
fn indexes_follow_upstream_schema_changes() {
    use ivm::types::{Column, ColumnType, SqlType};
    let dir = tmpdir("schema-indexes");
    let e = Engine::start(&dir);
    e.exec("CREATE TABLE t (id int PRIMARY KEY, a int, b text)");
    e.exec("INSERT INTO t VALUES (1, 10, 'x'), (2, 20, 'y')");
    e.exec("CREATE INDEX ta ON t (a)");
    e.exec("CREATE INDEX tb ON t (b)");
    e.exec("CREATE INDEX tbi ON t (b, id)");
    e.exec("CHECKPOINT");
    let oid: u64 = e.rows("SELECT oid FROM ivm.objects WHERE name = 't'")[0].parse().unwrap();
    // as a source logs it: a was dropped upstream, b moved to position 1
    let columns = vec![Column::new("id", ColumnType::new(SqlType::Int4).not_null()), Column::new("b", ColumnType::new(SqlType::Text))];
    e.h.log(ivm::log::LogRecord::Schema { source: 0, table: oid - 16384, columns, pk: vec![0] }).unwrap();
    assert_eq!(e.rows("SELECT * FROM t ORDER BY id"), ["1|x", "2|y"]);
    let state = |e: &Engine| (e.rows("SHOW INDEXES"), e.rows("SHOW ARRANGEMENTS").into_iter().map(|r| r.split('|').skip(2).collect::<Vec<_>>().join("|")).collect::<Vec<_>>());
    let expected = (
        vec!["tb|t|[1]".to_string(), "tbi|t|[1, 0]".to_string()],
        vec!["Table|#0|2|1".to_string(), "Table|#1|2|1".to_string(), "Table|#1, #0|2|1".to_string()],
    );
    assert_eq!(state(&e), expected);
    e.exec("INSERT INTO t VALUES (3, 'z')");
    e.shutdown();
    // replayed from the log, then from a checkpoint
    let e = Engine::start(&dir);
    assert_eq!(e.rows("SELECT * FROM t ORDER BY id"), ["1|x", "2|y", "3|z"]);
    let (indexes, arrangements) = state(&e);
    assert_eq!(indexes, expected.0);
    assert_eq!(arrangements.len(), 3, "{arrangements:?}");
    e.exec("CHECKPOINT");
    e.shutdown();
    let e = Engine::start(&dir);
    assert_eq!(state(&e).0, expected.0);
    assert_eq!(e.rows("SELECT * FROM t WHERE b = 'z'"), ["3|z"]);
    e.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// now() in a read AS OF an earlier GSN is the logical clock of that GSN,
/// also after a restart: the checkpoint keeps the clock ticks of the
/// retention window (a restart used to answer every earlier GSN with the
/// checkpoint's clock).
#[test]
fn now_as_of_earlier_gsns_after_a_restart() {
    let dir = tmpdir("now-as-of");
    let e = Engine::start(&dir);
    let tick = |e: &Engine, secs: i64| {
        let base = 1_767_225_600_000_000i64; // 2026-01-01T00:00:00Z in Unix microseconds
        e.h.log(ivm::log::LogRecord::Tick { micros: base + secs * 1_000_000 }).unwrap();
    };
    e.exec("CREATE TABLE t (id int PRIMARY KEY)");
    tick(&e, 10);
    e.exec("INSERT INTO t VALUES (1)");
    let g1: u64 = e.rows("SHOW GSN")[0].parse().unwrap();
    tick(&e, 20);
    e.exec("INSERT INTO t VALUES (2)");
    let g2: u64 = e.rows("SHOW GSN")[0].parse().unwrap();
    tick(&e, 30);
    let reads = |e: &Engine| {
        [g1, g2].map(|g| e.rows(&format!("SELECT now(), count(*) FROM t AS OF {g}"))[0].clone())
    };
    let want = ["2026-01-01 00:00:10+00|1", "2026-01-01 00:00:20+00|2"];
    assert_eq!(reads(&e), want);
    e.exec("CHECKPOINT");
    e.shutdown();
    let e = Engine::start(&dir);
    assert_eq!(reads(&e), want);
    assert_eq!(e.rows("SELECT now()"), ["2026-01-01 00:00:30+00"]);
    e.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Names and sizes of the files in a directory.
fn files(dir: &Path) -> Vec<(String, u64)> {
    let mut v: Vec<(String, u64)> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| {
            let e = e.unwrap();
            (e.file_name().to_string_lossy().into_owned(), e.metadata().unwrap().len())
        })
        .collect();
    v.sort();
    v
}

/// A store whose MANIFEST is missing or has no valid record, but whose
/// segment files hold data, is refused, and every file is left as it is.
/// Opening it used to delete the segments the manifest did not list (all
/// of them) and truncate the rest, destroying the checkpoint.
#[test]
fn a_store_without_its_manifest_is_refused_untouched() {
    let dir = tmpdir("manifest");
    let e = Engine::start(&dir);
    e.exec("CREATE TABLE t (id int PRIMARY KEY)");
    e.exec("INSERT INTO t VALUES (1), (2)");
    e.exec("CHECKPOINT");
    e.shutdown();
    let store = dir.join("store");
    let manifest = std::fs::read(store.join("MANIFEST")).unwrap();
    let cfg = coord::Config { data_dir: dir.clone(), sync: false, tick: None, ..Default::default() };
    // unreadable: the checksum of its first record fails (a later record
    // after a valid one would be a torn tail, dropped)
    let mut bad = manifest.clone();
    bad[8] ^= 0xFF;
    std::fs::write(store.join("MANIFEST"), &bad).unwrap();
    let before = files(&store);
    let err = coord::start(cfg.clone()).err().expect("refused");
    assert!(err.message.contains("no valid checkpoint record"), "{err}");
    assert_eq!(files(&store), before);
    // missing
    std::fs::remove_file(store.join("MANIFEST")).unwrap();
    let before = files(&store);
    let err = coord::start(cfg.clone()).err().expect("refused");
    assert!(err.message.contains("MANIFEST is missing"), "{err}");
    assert_eq!(files(&store), before);
    // restored, nothing was lost
    std::fs::write(store.join("MANIFEST"), &manifest).unwrap();
    let e = Engine::start(&dir);
    assert_eq!(e.rows("SELECT count(*) FROM t"), ["2"]);
    e.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A materialized view that an earlier version accepted but the current
/// rules reject (here: it calls pg_backend_pid(), which counts as stable
/// now) keeps being maintained, and does not hold back replacements of
/// views it does not read: CREATE OR REPLACE checked every materialized
/// view, so every replacement failed with "materialized view "m" would no
/// longer be valid". Those that read the replaced view are still checked.
#[test]
fn replacements_check_only_the_views_reading_the_replaced_one() {
    let dir = tmpdir("replace-checks");
    let e = Engine::start(&dir);
    e.exec("CREATE TABLE t (a int PRIMARY KEY, b int)");
    e.exec("INSERT INTO t VALUES (1, 10)");
    e.exec("CREATE MATERIALIZED VIEW old AS SELECT a, 0 AS p FROM t");
    e.exec("CREATE VIEW unrelated AS SELECT 1 AS x");
    e.exec("CREATE VIEW vb AS SELECT a, b FROM t");
    e.exec("CREATE MATERIALIZED VIEW reader AS SELECT a, b FROM vb");
    e.exec("CHECKPOINT");
    e.shutdown();
    // as an earlier version would have stored it
    break_view(&dir, "old", "SELECT a, pg_backend_pid() AS p FROM t");
    let e = Engine::start(&dir);
    assert_eq!(e.rows("SELECT a, p > 0 FROM old"), ["1|t"]);
    e.exec("CREATE OR REPLACE VIEW unrelated AS SELECT 2 AS x");
    assert_eq!(e.rows("SELECT x FROM unrelated"), ["2"]);
    // a replacement that would make a reader non-deterministic is refused
    let er = e.err("CREATE OR REPLACE VIEW vb AS SELECT a, b + pg_backend_pid() AS b FROM t");
    assert_eq!(er.code, "2BP01", "{er}");
    assert!(er.message.contains("materialized view \"reader\" would no longer be valid"), "{}", er.message);
    e.exec("CREATE OR REPLACE VIEW vb AS SELECT a, b + 1 AS b FROM t");
    assert_eq!(e.rows("SELECT a, b FROM reader"), ["1|11"]);
    e.exec("INSERT INTO t VALUES (2, 20)");
    assert_eq!(e.rows("SELECT a, p > 0 FROM old ORDER BY a"), ["1|t", "2|t"]);
    e.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
