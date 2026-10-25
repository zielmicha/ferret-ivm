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
