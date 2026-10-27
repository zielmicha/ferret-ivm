//! Restarts: recovery of data directories whose catalog or checkpoint the
//! current code no longer accepts as it is, and what survives a restart.

use ivm::coord::api::{ExecResult, SessionVars};
use ivm::coord::inspect::{InspectQuery, InspectReply};
use ivm::coord::{self, EngineHandle};
use ivm::error::SqlError;
use ivm::types::text::format_datum;
use std::io::Write;
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

/// Give materialized view `name` of a checkpointed catalog a definition
/// that no longer plans (as stricter code or catalog drift would).
fn break_view(dir: &Path, name: &str, sql: &str) {
    ivm::coord::meta::edit_stored_catalog(dir, |objects| {
        let v = objects.values_mut().find(|o| o.name == name).expect("the view");
        let ivm::catalog::ObjectKind::MaterializedView(def) = &mut v.kind else { panic!("not a materialized view") };
        def.sql = sql.into();
    })
    .unwrap();
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

/// A data directory written by the last version of format 1, which kept
/// the catalog and the engine state as JSON in every checkpoint record
/// (and had no `FORMAT` file): `ivm --retention-gsns 1000000 --tick-ms 1000
/// --checkpoint-secs 1`, replicating from a PostgreSQL server, ran
///
/// ```sql
/// CREATE TABLE t (id int PRIMARY KEY, v text);
/// INSERT INTO t VALUES (1, '10'), (2, '20'), (3, 'x'), (4, 'y');
/// CREATE MATERIALIZED VIEW m AS SELECT id, v::int AS n FROM t;  -- two error records
/// CREATE TABLE r (k int PRIMARY KEY, g int, x real);
/// INSERT INTO r VALUES (1, 1, 3e38), (2, 1, 3e38), (3, 2, 1);
/// CREATE MATERIALIZED VIEW s AS SELECT g, sum(x) AS total FROM r GROUP BY g;  -- group 1 in error
/// CREATE INDEX ti ON t (v);
/// CREATE VIEW pv AS SELECT id FROM t WHERE id > 1;
/// CREATE SOURCE src FROM POSTGRES CONNECTION '...' PUBLICATION 'pub';  -- table up: (1, 'a'), (2, 'b')
/// CREATE SOURCE src2 FROM POSTGRES CONNECTION '...' PUBLICATION 'pub2' SLOT 'fixture_src2';
/// -- upstream: INSERT INTO up VALUES (3, 'c'); now at GSN 21
/// UPDATE t SET v = '30' WHERE id = 2;
/// CREATE MATERIALIZED VIEW late AS SELECT id, v FROM t;
/// CREATE TABLE tmp (a int);
/// DROP TABLE tmp;
/// -- the upstream server stopped
/// DROP SOURCE src2;  -- its slot drop is pending
/// ```
///
/// and stopped on SIGTERM (final checkpoint at GSN 31).
const FORMAT1: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/format1");

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        if e.file_type().unwrap().is_dir() {
            copy_dir(&e.path(), &to.join(e.file_name()));
        } else {
            std::fs::copy(e.path(), to.join(e.file_name())).unwrap();
        }
    }
}

/// The application state (JSON) of the format-1 directory's checkpoint
/// (read from a copy named `name`: opening a store may modify it).
fn format1_state(name: &str) -> serde_json::Value {
    let dir = tmpdir(name);
    copy_dir(Path::new(FORMAT1), &dir);
    let (_env, rec) = ivm::storage::Env::open(&dir.join("store"), Default::default()).unwrap();
    let app = serde_json::from_slice(&rec.expect("a checkpoint").app).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    app
}

/// Everything the format-1 directory holds, as the engine shows it,
/// against its checkpointed state `app`.
fn check_format1(e: &Engine, app: &serde_json::Value) {
    let s = |v: &serde_json::Value| v.as_str().unwrap().to_string();
    // the catalog and its history
    let history: Vec<String> = app["catalog"]["history"].as_array().unwrap().iter().map(|h| format!("{}|{}", h[0], s(&h[1]))).collect();
    assert_eq!(history.len(), 12);
    assert_eq!(e.rows("SELECT gsn, statement FROM ivm.catalog_history ORDER BY gsn"), history);
    let objects: Vec<String> =
        app["catalog"]["objects"].as_object().unwrap().values().map(|o| format!("{}|{}|{}", o["id"], s(&o["name"]), o["created_gsn"])).collect();
    assert_eq!(e.rows("SELECT oid::int8 - 16384, name, created_gsn FROM ivm.objects ORDER BY oid"), objects);
    // error records
    let name = |id: &serde_json::Value| app["catalog"]["objects"][id.to_string()]["name"].as_str().unwrap().to_string();
    let mut want: Vec<String> = Vec::new();
    for v in app["view_errors"].as_array().unwrap() {
        for r in v[1].as_array().unwrap() {
            want.push(format!("{}|{}|{}|{}", name(&v[0]), s(&r[0][0]), s(&r[0][1]), r[1]));
        }
    }
    want.sort();
    assert_eq!(want.len(), 3);
    match e.h.inspect_wait(InspectQuery::Errors, Duration::from_secs(10)).unwrap() {
        InspectReply::Errors(d) => {
            let mut got: Vec<String> = d.views.iter().map(|v| format!("{}|{}|{}|{}", v.name, v.code, v.message, v.count)).collect();
            got.sort();
            assert_eq!(got, want);
        }
        other => panic!("unexpected reply {other:?}"),
    }
    assert_eq!(e.err("SELECT * FROM m").message, "invalid input syntax for type integer: \"x\"");
    assert_eq!(e.err("SELECT * FROM s").message, "value out of range: overflow");
    assert_eq!(e.rows("SHOW VIEWS"), ["m|materialized|2|2|ok", "s|materialized|1|1|ok", "pv|virtual|||", "late|materialized|4|0|ok"]);
    // the source's resume point and state, the pending slot drop
    match e.h.inspect_wait(InspectQuery::Sources, Duration::from_secs(10)).unwrap() {
        InspectReply::Sources(d) => {
            let src = &d.sources[0];
            assert_eq!((d.sources.len(), src.name.as_str()), (1, "src"));
            assert_eq!(src.last_commit_lsn, app["source_lsns"][0][1].as_u64().unwrap());
            assert!(src.slot_created);
            assert_eq!((src.tables.len(), src.tables_done), (1, 1));
        }
        other => panic!("unexpected reply {other:?}"),
    }
    let drops: Vec<(String, String)> = e.h.shared.slot_drops.lock().iter().map(|d| (d.conninfo.clone(), d.slot.clone())).collect();
    assert_eq!(drops, [(s(&app["slot_drops"][0]["conninfo"]), "fixture_src2".to_string())]);
    assert_eq!(e.rows("SELECT * FROM up ORDER BY id"), ["1|a", "2|b", "3|c"]);
    // the clock as of earlier GSNs (as the format-1 engine answered), and
    // histories: ti's starts at its creation (GSN 9), so the read as of
    // GSN 8 does not use it
    assert_eq!(e.rows("SELECT now(), count(*) FROM t AS OF 21"), ["2026-10-25 23:18:50.490978+00|4"]);
    assert_eq!(e.rows("SELECT now()"), ["2026-10-25 23:18:55.673051+00"]);
    assert_eq!(e.rows("SELECT v FROM t WHERE id = 2 AS OF 21"), ["20"]);
    assert_eq!(e.rows("SELECT id FROM t WHERE v = '20' AS OF 8"), ["2"]);
    assert_eq!(e.rows("SELECT id FROM t WHERE v = '30'"), ["2"]);
}

/// The last checkpoint record's application state.
fn checkpoint_state(dir: &Path) -> Vec<u8> {
    ivm::storage::Env::open(&dir.join("store"), Default::default()).unwrap().1.expect("a checkpoint").app
}

/// A data directory of format 1 opens, and is migrated at its first start:
/// everything it held is kept (catalog, DDL history, error records of views
/// and aggregation groups, source states and resume LSNs, pending slot
/// drops, clock ticks, the histories of indexes), and from then on its
/// checkpoints carry a small header naming the metadata tree.
#[test]
fn format1_data_directories_are_migrated() {
    let app = format1_state("format1-state");
    let dir = tmpdir("format1");
    copy_dir(Path::new(FORMAT1), &dir);
    assert!(!dir.join("FORMAT").exists());
    let e = Engine::start(&dir);
    check_format1(&e, &app);
    assert_eq!(std::fs::read_to_string(dir.join("FORMAT")).unwrap(), "2\n");
    e.shutdown();
    let header = checkpoint_state(&dir);
    assert!(header[0] == 2 && header.len() < 16, "{header:?}");

    let e = Engine::start(&dir);
    check_format1(&e, &app);
    // the error state is complete: a view created over m's dataflow starts
    // with its error records, and the errors retract with their rows
    e.exec("CREATE MATERIALIZED VIEW m2 AS SELECT n FROM m");
    assert_eq!(e.err("SELECT * FROM m2").message, "invalid input syntax for type integer: \"x\"");
    e.exec("DELETE FROM t WHERE id > 2");
    assert_eq!(e.rows("SELECT * FROM m ORDER BY id"), ["1|10", "2|30"]);
    assert_eq!(e.rows("SELECT * FROM m2 ORDER BY n"), ["10", "30"]);
    e.exec("DELETE FROM r WHERE k = 2");
    assert_eq!(e.rows("SELECT * FROM s ORDER BY g"), ["1|3e+38", "2|1"]);
    e.shutdown();
    let e = Engine::start(&dir);
    assert_eq!(e.rows("SELECT * FROM m2 ORDER BY n"), ["10", "30"]);
    assert_eq!(e.rows("SELECT * FROM s ORDER BY g"), ["1|3e+38", "2|1"]);
    assert_eq!(e.rows("SELECT count(*) FROM ivm.catalog_history"), ["13"]);
    e.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A crash during the migration loses nothing: until the migration's
/// checkpoint is committed the next start finds the format-1 record and
/// migrates again; after it, but before `FORMAT` records the migration, it
/// finds a record of the current format and only records the format.
#[test]
fn a_crash_during_migration_migrates_again() {
    let app = format1_state("format1-crash-state");
    let dir = tmpdir("format1-crash");
    copy_dir(Path::new(FORMAT1), &dir);
    let manifest = std::fs::read(dir.join("store/MANIFEST")).unwrap();
    Engine::start(&dir).shutdown();
    // before the commit: the format-1 manifest, with the migration's
    // record torn
    let mut torn = manifest.clone();
    torn.extend_from_slice(&[0x40, 0, 0, 0, 1, 2, 3, 4, 5]);
    std::fs::write(dir.join("store/MANIFEST"), &torn).unwrap();
    std::fs::remove_file(dir.join("FORMAT")).unwrap();
    let e = Engine::start(&dir);
    check_format1(&e, &app);
    e.shutdown();
    assert_eq!(checkpoint_state(&dir)[0], 2);
    // after the commit
    std::fs::remove_file(dir.join("FORMAT")).unwrap();
    let e = Engine::start(&dir);
    check_format1(&e, &app);
    e.shutdown();
    assert_eq!(std::fs::read_to_string(dir.join("FORMAT")).unwrap(), "2\n");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Names and sizes of the files under a directory.
fn all_files(dir: &Path) -> Vec<(PathBuf, u64)> {
    let mut v = Vec::new();
    for e in std::fs::read_dir(dir).unwrap() {
        let e = e.unwrap();
        if e.file_type().unwrap().is_dir() {
            v.extend(all_files(&e.path()));
        } else {
            v.push((e.path(), e.metadata().unwrap().len()));
        }
    }
    v.sort();
    v
}

/// A new data directory records its format first; one of a newer format
/// than this code reads is refused before anything in it is touched (a
/// torn log tail is not truncated, for one).
#[test]
fn newer_formats_are_refused_untouched() {
    let dir = tmpdir("format-newer");
    let e = Engine::start(&dir);
    assert_eq!(std::fs::read_to_string(dir.join("FORMAT")).unwrap(), "2\n");
    e.exec("CREATE TABLE t (id int PRIMARY KEY)");
    e.shutdown();
    std::fs::write(dir.join("FORMAT"), "3\n").unwrap();
    let seg = std::fs::read_dir(dir.join("log")).unwrap().next().unwrap().unwrap().path();
    std::fs::OpenOptions::new().append(true).open(&seg).unwrap().write_all(&[1, 2, 3]).unwrap();
    let before = all_files(&dir);
    let cfg = coord::Config { data_dir: dir.clone(), sync: false, tick: None, ..Default::default() };
    let err = coord::start(cfg).err().expect("refused");
    assert_eq!(err.code, "0A000", "{err}");
    assert!(err.message.contains("format 3"), "{err}");
    assert_eq!(all_files(&dir), before);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A data directory written by the version before ntile read its argument
/// at the partition's first row where it is not NULL and lag and lead
/// recomputed the rows that a negative offset, or one differing between
/// rows, reads a change from (`perf-integration` at 58e8ea2): `ivm
/// --retention-gsns 1000000 --tick-ms 0` ran
///
/// ```sql
/// CREATE TABLE w (id int PRIMARY KEY, g int, a int);
/// INSERT INTO w SELECT i, i % 2, CASE WHEN i % 7 = 0 THEN NULL ELSE i % 5 + 1 END FROM generate_series(1, 90) i;
/// CREATE MATERIALIZED VIEW nv AS SELECT id, g, ntile(a) OVER (PARTITION BY g ORDER BY id) AS n FROM w;
/// CREATE MATERIALIZED VIEW nc AS SELECT g, n, count(*) AS c FROM nv GROUP BY g, n;
/// CREATE MATERIALIZED VIEW lv AS SELECT id, lag(a, (id % 3 - 1)::int, -1) OVER (PARTITION BY g ORDER BY id) AS l, lead(id, -1) OVER (PARTITION BY g ORDER BY id) AS ld FROM w;
/// CREATE MATERIALIZED VIEW cv AS SELECT id, ntile(4) OVER (PARTITION BY g ORDER BY id) AS n, lag(a, 2) OVER (PARTITION BY g ORDER BY id) AS l, lead(a) OVER (PARTITION BY g ORDER BY id) AS ld FROM w;
/// DELETE FROM w WHERE id IN (40, 41);  -- GSN 7
/// ```
///
/// and stopped on SIGTERM (final checkpoint). Its `nv` (and `nc`) hold
/// ntile values computed from every row's argument, and its `lv` still
/// reads the deleted rows in three rows.
const WINDOW_REVISION: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/window-revision");

/// Window operators that this version maintains otherwise than the one
/// that wrote a data directory (`WinSpec::revised`) do not match its
/// checkpoint: they are rebuilt from their input at the first start, with
/// the views over them, and follow changes from then on (matched, the
/// first change would have retracted rows they never held). Those of
/// functions maintained as before are recovered.
#[test]
fn revised_window_operators_are_rebuilt() {
    let dir = tmpdir("window-revision");
    copy_dir(Path::new(WINDOW_REVISION), &dir);
    let queries = [
        ("nv", "SELECT id, g, ntile(a) OVER (PARTITION BY g ORDER BY id) AS n FROM w"),
        ("nc", "SELECT g, n, count(*) AS c FROM (SELECT g, ntile(a) OVER (PARTITION BY g ORDER BY id) AS n FROM w) s GROUP BY g, n"),
        ("lv", "SELECT id, lag(a, (id % 3 - 1)::int, -1) OVER (PARTITION BY g ORDER BY id) AS l, lead(id, -1) OVER (PARTITION BY g ORDER BY id) AS ld FROM w"),
        ("cv", "SELECT id, ntile(4) OVER (PARTITION BY g ORDER BY id) AS n, lag(a, 2) OVER (PARTITION BY g ORDER BY id) AS l, lead(a) OVER (PARTITION BY g ORDER BY id) AS ld FROM w"),
    ];
    let check = |e: &Engine, when: &str| {
        for (view, q) in queries {
            let mut got = e.rows(&format!("SELECT * FROM {view}"));
            let mut want = e.rows(q);
            got.sort();
            want.sort();
            assert_eq!(got, want, "{view} {when}");
        }
    };
    let e = Engine::start(&dir);
    check(&e, "at the first start");
    // (ntile's buckets from each partition's first argument, 3 and 2)
    assert_eq!(e.rows("SELECT n, c FROM nc WHERE g = 1 ORDER BY n"), ["1|22", "2|22"]);
    // rebuilt: their history starts at the restart; cv's indexes were found
    for view in ["nv", "nc", "lv"] {
        let err = e.err(&format!("SELECT count(*) FROM {view} AS OF 6")).message;
        assert!(err.contains("before the history"), "{view}: {err}");
    }
    assert_eq!(e.rows("SELECT count(*) FROM cv AS OF 6"), ["90"]);
    for (i, sql) in [
        "UPDATE w SET a = 1 WHERE id = 1",
        "DELETE FROM w WHERE id = 2",
        "INSERT INTO w VALUES (40, 0, 4), (41, 1, NULL), (91, 1, 2)",
        "UPDATE w SET a = NULL WHERE id IN (3, 4)",
        "DELETE FROM w WHERE id BETWEEN 50 AND 54",
    ]
    .iter()
    .enumerate()
    {
        e.exec(sql);
        check(&e, &format!("after change {i}"));
    }
    e.exec("CHECKPOINT");
    e.shutdown();
    let e = Engine::start(&dir);
    check(&e, "after another restart");
    e.exec("UPDATE w SET a = 5 WHERE id = 5");
    check(&e, "after the last change");
    e.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
