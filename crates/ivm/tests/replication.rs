//! Replication regression tests against a private PostgreSQL cluster
//! (pgproto's TempCluster; PostgreSQL binaries from `PG_BIN`).

use ivm::coord::{self, Config};
use ivm::server::{self, ServerConfig};
use pgproto::testutil::{ClusterOptions, TempCluster};
use pgproto::{Client, Config as PgConfig};
use std::net::TcpListener;
use std::path::PathBuf;
use std::time::{Duration, Instant};

fn tmpdir(name: &str) -> PathBuf {
    let base = std::env::var("IVM_TEST_TMP").unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().into_owned());
    let d = PathBuf::from(base).join(format!("ivm-repl-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn connect(conninfo: &str) -> Client {
    Client::connect(&PgConfig::parse(conninfo).expect("conninfo")).expect("connect")
}

/// An engine with a pgwire server on a free port.
struct Ivm {
    running: Option<coord::Running>,
    client: Client,
}

impl Ivm {
    fn start(dir: &PathBuf) -> Ivm {
        let cfg = Config { data_dir: dir.clone(), sync: false, ..Default::default() };
        let running = coord::start(cfg).expect("engine starts");
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let h = running.handle.clone();
        std::thread::spawn(move || {
            let _ = server::serve_listener(listener, ServerConfig { listen: String::new(), password: None }, h);
        });
        let client = connect(&format!("host=127.0.0.1 port={port} user=ivm dbname=ivm"));
        Ivm { running: Some(running), client }
    }

    fn exec(&mut self, sql: &str) {
        self.client.simple_query(sql).unwrap_or_else(|e| panic!("{sql}: {}", e.message));
    }

    fn rows(&mut self, sql: &str) -> Vec<String> {
        let r = self.client.query(sql).unwrap_or_else(|e| panic!("{sql}: {}", e.message));
        r.rows
            .into_iter()
            .map(|r| r.into_iter().map(|v| v.unwrap_or_else(|| "NULL".into())).collect::<Vec<_>>().join("|"))
            .collect()
    }

    fn shutdown(&mut self) {
        if let Some(r) = self.running.take() {
            r.shutdown();
        }
    }
}

impl Drop for Ivm {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct Upstream {
    _pg: TempCluster,
    conninfo: String,
    client: Client,
}

impl Upstream {
    fn start() -> Upstream {
        let pg = TempCluster::start(ClusterOptions { extra_conf: vec!["timezone = 'UTC'".into()], ..Default::default() })
            .expect("start postgres");
        let conninfo = pg.conninfo("postgres");
        let client = connect(&conninfo);
        Upstream { _pg: pg, conninfo, client }
    }

    fn exec(&mut self, sql: &str) {
        self.client.simple_query(sql).unwrap_or_else(|e| panic!("{sql}: {}", e.message));
    }

    fn rows(&mut self, sql: &str) -> Vec<String> {
        let r = self.client.query(sql).unwrap_or_else(|e| panic!("{sql}: {}", e.message));
        r.rows
            .into_iter()
            .map(|r| r.into_iter().map(|v| v.unwrap_or_else(|| "NULL".into())).collect::<Vec<_>>().join("|"))
            .collect()
    }

    fn lsn(&mut self) -> String {
        self.rows("SELECT pg_current_wal_lsn()").remove(0)
    }
}

/// `query` on the engine once it has caught up with everything committed
/// upstream so far (including initial snapshots).
fn caught_up(ivm: &mut Ivm, up: &mut Upstream, query: &str) -> Vec<String> {
    let lsn = up.lsn();
    // fail (with the sources' status) rather than wait forever
    let t0 = Instant::now();
    loop {
        let sources = ivm.rows("SHOW SOURCES");
        if sources.iter().all(|s| parse_lsn(s.split('|').nth(4).unwrap()) >= parse_lsn(&lsn)) {
            break;
        }
        assert!(t0.elapsed() < Duration::from_secs(60), "timed out waiting for LSN {lsn}: {sources:?}");
        std::thread::sleep(Duration::from_millis(20));
    }
    ivm.rows(&format!("{query} AS OF AT LEAST LSN '{lsn}'"))
}

fn parse_lsn(s: &str) -> u64 {
    let (hi, lo) = s.split_once('/').unwrap();
    (u64::from_str_radix(hi, 16).unwrap() << 32) | u64::from_str_radix(lo, 16).unwrap()
}

fn setup(name: &str, ddl: &[&str]) -> (Upstream, Ivm, PathBuf) {
    let mut up = Upstream::start();
    for d in ddl {
        up.exec(d);
    }
    up.exec("CREATE PUBLICATION p FOR ALL TABLES");
    let dir = tmpdir(name);
    let mut ivm = Ivm::start(&dir);
    ivm.exec(&format!("CREATE SOURCE s FROM POSTGRES CONNECTION '{}' PUBLICATION 'p'", up.conninfo));
    (up, ivm, dir)
}

/// Initial snapshot and streaming through the test harness.
#[test]
fn snapshot_then_stream() {
    let (mut up, mut ivm, _dir) = setup("smoke", &["CREATE TABLE t (id int PRIMARY KEY, a int)", "INSERT INTO t VALUES (1, 1), (2, 2)"]);
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT * FROM t ORDER BY id"), ["1|1", "2|2"]);
    up.exec("UPDATE t SET a = 10 WHERE id = 1");
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT * FROM t ORDER BY id"), ["1|10", "2|2"]);
}

#[test]
fn views_over_views_follow_upstream_schema_changes() {
    let (mut up, mut ivm, dir) = setup("transitive", &["CREATE TABLE t (id int PRIMARY KEY, a int)", "INSERT INTO t VALUES (1, 1)"]);
    let _ = caught_up(&mut ivm, &mut up, "SELECT * FROM t");
    ivm.exec("CREATE MATERIALIZED VIEW x AS SELECT id, a FROM t");
    ivm.exec("CREATE MATERIALIZED VIEW z AS SELECT count(*) AS n, sum(a) AS s FROM x");
    ivm.exec("CREATE INDEX xa ON x (a)");
    up.exec("ALTER TABLE t ADD COLUMN b int");
    up.exec("INSERT INTO t VALUES (2, 2, 2)");
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT * FROM z"), ["2|3"]);
    up.exec("INSERT INTO t VALUES (3, 3, 3)");
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT * FROM x ORDER BY id"), ["1|1", "2|2", "3|3"]);
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT * FROM z"), ["3|6"]);
    let arrs = ivm.rows("SHOW ARRANGEMENTS").len();
    ivm.shutdown();
    let mut ivm = Ivm::start(&dir);
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT * FROM z"), ["3|6"]);
    assert_eq!(ivm.rows("SHOW ARRANGEMENTS").len(), arrs);
}

/// A view that an upstream schema change invalidates is dropped (with its
/// dependents), instead of being frozen and keeping the engine from
/// starting again.
#[test]
fn views_invalidated_upstream_are_dropped() {
    let (mut up, mut ivm, dir) = setup("dropcol", &["CREATE TABLE t (id int PRIMARY KEY, a int, b int)", "INSERT INTO t VALUES (1, 1, 1)"]);
    let _ = caught_up(&mut ivm, &mut up, "SELECT * FROM t");
    ivm.exec("CREATE MATERIALIZED VIEW v AS SELECT id, b FROM t");
    ivm.exec("CREATE MATERIALIZED VIEW vv AS SELECT count(*) FROM v");
    ivm.exec("CREATE INDEX vb ON v (b)");
    ivm.exec("CREATE MATERIALIZED VIEW ok AS SELECT id, a FROM t");
    up.exec("ALTER TABLE t DROP COLUMN b");
    up.exec("INSERT INTO t VALUES (2, 2)");
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT * FROM t ORDER BY id"), ["1|1", "2|2"]);
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT * FROM ok ORDER BY id"), ["1|1", "2|2"]);
    for gone in ["v", "vv"] {
        assert!(ivm.client.query(&format!("SELECT * FROM {gone}")).is_err(), "{gone} should be gone");
    }
    assert!(ivm.rows("SHOW INDEXES").is_empty());
    ivm.shutdown();
    let mut ivm = Ivm::start(&dir);
    up.exec("INSERT INTO t VALUES (3, 3)");
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT * FROM ok ORDER BY id"), ["1|1", "2|2", "3|3"]);
}

/// A view over `SELECT *` takes the table's new shape (reads used to
/// mix the old column list with new rows and panic the engine).
#[test]
fn star_views_take_the_new_table_shape() {
    let (mut up, mut ivm, _dir) = setup("star", &["CREATE TABLE t (id int PRIMARY KEY, a text, b int)", "INSERT INTO t VALUES (1, 'one', 10)"]);
    let _ = caught_up(&mut ivm, &mut up, "SELECT * FROM t");
    ivm.exec("CREATE MATERIALIZED VIEW v AS SELECT * FROM t");
    up.exec("ALTER TABLE t DROP COLUMN a");
    up.exec("INSERT INTO t VALUES (2, 20)");
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT * FROM v ORDER BY id"), ["1|10", "2|20"]);
    assert_eq!(ivm.rows("SELECT b FROM v ORDER BY id"), ["10", "20"]);
    up.exec("ALTER TABLE t ADD COLUMN c int DEFAULT 5");
    up.exec("INSERT INTO t VALUES (3, 30, 6)");
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT * FROM v ORDER BY id"), ["1|10|5", "2|20|5", "3|30|6"]);
}

/// Tables keyed by upstream types replicated as text (enum, inet) are
/// snapshotted in chunks ordered like the engine's keys (the chunk query
/// used to apply COLLATE to them and fail, stalling the whole source).
#[test]
fn text_replicated_keys_snapshot_in_chunks() {
    let (mut up, mut ivm, _dir) = setup(
        "enum",
        &[
            "CREATE TYPE color AS ENUM ('red', 'green', 'blue')",
            "CREATE TABLE e (c color, n int, a int, PRIMARY KEY (c, n))",
            "INSERT INTO e SELECT (ARRAY['red', 'green', 'blue']::color[])[1 + g % 3], g, g FROM generate_series(1, 25000) g",
            "CREATE TABLE i (addr inet PRIMARY KEY, a int)",
            "INSERT INTO i SELECT ('10.0.' || (g / 250) || '.' || (g % 250))::inet, g FROM generate_series(1, 12000) g",
            "CREATE TABLE vc (k varchar(10) PRIMARY KEY, a int)",
            "INSERT INTO vc SELECT 'k' || g, g FROM generate_series(1, 11000) g",
        ],
    );
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT count(*), sum(a) FROM e"), ["25000|312512500"]);
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT count(*), sum(a) FROM i"), ["12000|72006000"]);
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT count(*), sum(a) FROM vc"), ["11000|60505500"]);
    up.exec("UPDATE e SET a = 0 WHERE c = 'blue' AND n = 2");
    up.exec("DELETE FROM i WHERE addr = '10.0.0.1'");
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT count(*), sum(a) FROM e"), ["25000|312512498"]);
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT count(*), sum(a) FROM i"), ["11999|72005999"]);
}

/// Changes to a table added by ALTER SOURCE REFRESH are replicated even when
/// PostgreSQL announced the table (a Relation message, sent once per
/// session) before the source knew it; they used to be ignored for good.
#[test]
fn refreshed_tables_announced_earlier_are_replicated() {
    let mut up = Upstream::start();
    up.exec("CREATE TABLE a (id int PRIMARY KEY)");
    up.exec("CREATE TABLE b (id int PRIMARY KEY, v int)");
    up.exec("CREATE PUBLICATION p FOR TABLE a");
    let dir = tmpdir("refresh");
    let mut ivm = Ivm::start(&dir);
    ivm.exec(&format!("CREATE SOURCE s FROM POSTGRES CONNECTION '{}' PUBLICATION 'p'", up.conninfo));
    let _ = caught_up(&mut ivm, &mut up, "SELECT count(*) FROM a");
    up.exec("ALTER PUBLICATION p ADD TABLE b");
    up.exec("INSERT INTO b VALUES (1, 1)"); // announces b to the running session
    up.exec("INSERT INTO a VALUES (1)");
    let _ = caught_up(&mut ivm, &mut up, "SELECT count(*) FROM a");
    ivm.exec("ALTER SOURCE s REFRESH");
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT * FROM b ORDER BY id"), ["1|1"]);
    up.exec("INSERT INTO b VALUES (2, 2)");
    up.exec("UPDATE b SET v = 10 WHERE id = 1");
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT * FROM b ORDER BY id"), ["1|10", "2|2"]);
}

/// One upstream transaction changing rows before and after an ALTER TABLE:
/// the rows decoded in the old shape used to reach the engine after the
/// schema change and panic it (on every restart, too).
#[test]
fn ddl_and_dml_in_one_upstream_transaction() {
    let (mut up, mut ivm, dir) = setup("txn-ddl", &["CREATE TABLE t (id int PRIMARY KEY, a int)", "INSERT INTO t VALUES (1, 1)"]);
    ivm.exec("CREATE MATERIALIZED VIEW v AS SELECT id, a FROM t");
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT * FROM t ORDER BY id"), ["1|1"]);
    up.exec("BEGIN; INSERT INTO t VALUES (2, 2); ALTER TABLE t ADD COLUMN b int DEFAULT 0; INSERT INTO t VALUES (3, 3, 3); UPDATE t SET a = 10 WHERE id = 1; COMMIT");
    let want = ["1|10|0", "2|2|0", "3|3|3"];
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT * FROM t ORDER BY id"), want);
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT * FROM v ORDER BY id"), ["1|10", "2|2", "3|3"]);
    ivm.shutdown();
    let mut ivm = Ivm::start(&dir);
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT * FROM t ORDER BY id"), want);
}

/// Rows deleted upstream while a table is re-snapshotted after a schema
/// change are deleted: the re-snapshot only upserts the rows it reads, and
/// the deletes of keys it had not reached yet used to be skipped.
#[test]
fn deletes_during_a_resnapshot_apply() {
    let (mut up, mut ivm, _dir) = setup(
        "resnap-delete",
        &["CREATE TABLE t (id int PRIMARY KEY, a int)", "INSERT INTO t SELECT g, g FROM generate_series(1, 12000) g"],
    );
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT count(*) FROM t"), ["12000"]);
    up.exec("ALTER TABLE t ADD COLUMN b int DEFAULT 7");
    up.exec("INSERT INTO t VALUES (12001, 0, 0)");
    // beyond the first chunk of the re-snapshot
    up.exec("DELETE FROM t WHERE id = 11999");
    up.exec("UPDATE t SET id = 20000 WHERE id = 11998");
    let q = "SELECT count(*), count(b), sum(b), sum(id) FROM t";
    assert_eq!(caught_up(&mut ivm, &mut up, q), up.rows(q));
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT id FROM t WHERE id > 11996 ORDER BY id"), ["11997", "12000", "12001", "20000"]);
}

/// Re-snapshotting a table without a primary key (REPLICA IDENTITY FULL)
/// after a schema change replaces its rows; it used to add a second copy
/// of every row.
#[test]
fn keyless_tables_resnapshot_without_duplicates() {
    let (mut up, mut ivm, _dir) = setup(
        "multiset-schema",
        &["CREATE TABLE m (a int, b text)", "ALTER TABLE m REPLICA IDENTITY FULL", "INSERT INTO m VALUES (1, 'x'), (1, 'x'), (2, 'y')"],
    );
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT * FROM m ORDER BY a, b"), ["1|x", "1|x", "2|y"]);
    up.exec("ALTER TABLE m ADD COLUMN c int DEFAULT 5");
    up.exec("INSERT INTO m VALUES (3, 'z', 6)");
    up.exec("DELETE FROM m WHERE a = 2");
    let q = "SELECT * FROM m ORDER BY a, b, c";
    assert_eq!(caught_up(&mut ivm, &mut up, q), up.rows(q));
    assert_eq!(up.rows(q), ["1|x|5", "1|x|5", "3|z|6"]);
}

/// Wait until `f` returns true (or fail after a timeout).
fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    let t0 = Instant::now();
    while !f() {
        assert!(t0.elapsed() < Duration::from_secs(60), "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// A sink keeps its target equal to the view across engine restarts.
#[test]
fn sink_delivers_exactly_once_across_restarts() {
    let mut up = Upstream::start();
    up.exec("CREATE TABLE t (id int PRIMARY KEY, a int)");
    up.exec("INSERT INTO t SELECT g, g % 5 FROM generate_series(1, 100) g");
    up.exec("CREATE PUBLICATION p FOR TABLE t");
    let dir = tmpdir("sink");
    let mut ivm = Ivm::start(&dir);
    ivm.exec(&format!("CREATE SOURCE s FROM POSTGRES CONNECTION '{}' PUBLICATION 'p'", up.conninfo));
    ivm.exec("CREATE MATERIALIZED VIEW v AS SELECT a, count(*) AS n FROM t GROUP BY a");
    ivm.exec("CREATE MATERIALIZED VIEW w AS SELECT a FROM t WHERE id % 10 = 0");
    ivm.exec(&format!("CREATE SINK sk FROM v INTO POSTGRES CONNECTION '{}' TABLE 'v_copy' KEY (a)", up.conninfo));
    ivm.exec(&format!("CREATE SINK sw FROM w INTO POSTGRES CONNECTION '{}' TABLE 'w_copy'", up.conninfo));
    let want_v = "SELECT a, count(*) FROM t GROUP BY a ORDER BY a";
    let want_w = "SELECT a FROM t WHERE id % 10 = 0 ORDER BY a";
    let synced = |up: &mut Upstream| {
        eventually("sinks", || {
            let v = up.client.query("SELECT a, n FROM v_copy ORDER BY a").map(|r| r.rows).unwrap_or_default();
            let w = up.client.query("SELECT a FROM w_copy ORDER BY a").map(|r| r.rows).unwrap_or_default();
            let ev = up.client.query(want_v).unwrap().rows;
            let ew = up.client.query(want_w).unwrap().rows;
            v == ev && w == ew
        })
    };
    synced(&mut up);
    up.exec("UPDATE t SET a = 7 WHERE id <= 30");
    up.exec("DELETE FROM t WHERE id > 90");
    synced(&mut up);
    for round in 0..3 {
        ivm.shutdown();
        up.exec(&format!("UPDATE t SET a = a + 1 WHERE id % 3 = {round}"));
        up.exec(&format!("INSERT INTO t VALUES ({}, 1)", 1000 + round));
        ivm = Ivm::start(&dir);
        up.exec(&format!("DELETE FROM t WHERE id = {}", 10 + round * 10));
        synced(&mut up);
    }
}

/// A sink without a key deletes a retracted row by comparing every column;
/// json has no equality operator, so the first retraction of a row with a
/// json column used to fail, and the sink stopped delivering for good.
#[test]
fn keyless_sink_with_json_columns() {
    let mut up = Upstream::start();
    up.exec("CREATE TABLE t (id int PRIMARY KEY, j json)");
    up.exec("INSERT INTO t VALUES (1, '{\"a\": 1}'), (2, '[1, 2]'), (3, NULL)");
    up.exec("CREATE PUBLICATION p FOR TABLE t");
    let dir = tmpdir("sink-json");
    let mut ivm = Ivm::start(&dir);
    ivm.exec(&format!("CREATE SOURCE s FROM POSTGRES CONNECTION '{}' PUBLICATION 'p'", up.conninfo));
    ivm.exec("CREATE MATERIALIZED VIEW v AS SELECT j FROM t");
    ivm.exec(&format!("CREATE SINK sk FROM v INTO POSTGRES CONNECTION '{}' TABLE 'v_copy'", up.conninfo));
    let copy = |up: &mut Upstream| up.client.query("SELECT j::text FROM v_copy ORDER BY 1").map(|r| r.rows).unwrap_or_default();
    eventually("snapshot", || copy(&mut up).len() == 3);
    up.exec("DELETE FROM t WHERE id IN (1, 3)");
    up.exec("INSERT INTO t VALUES (4, '{\"b\": 2}')");
    eventually("retractions", || copy(&mut up) == up.client.query("SELECT j::text FROM t ORDER BY 1").unwrap().rows);
}

/// A sink's target table has its view's columns: a replacement changing
/// them is rejected (the sink used to go on writing the new rows under the
/// old column names); one keeping them is delivered.
#[test]
fn sinks_keep_their_columns_across_view_replacements() {
    let mut up = Upstream::start();
    up.exec("CREATE TABLE t (id int PRIMARY KEY, a int, b int)");
    up.exec("INSERT INTO t VALUES (1, 10, 100)");
    up.exec("CREATE PUBLICATION p FOR TABLE t");
    let dir = tmpdir("sink-replace");
    let mut ivm = Ivm::start(&dir);
    ivm.exec(&format!("CREATE SOURCE s FROM POSTGRES CONNECTION '{}' PUBLICATION 'p'", up.conninfo));
    ivm.exec("CREATE MATERIALIZED VIEW v AS SELECT id, a FROM t");
    ivm.exec(&format!("CREATE SINK sk FROM v INTO POSTGRES CONNECTION '{}' TABLE 'v_copy' KEY (id)", up.conninfo));
    let copy = |up: &mut Upstream| up.client.query("SELECT * FROM v_copy ORDER BY id").map(|r| r.rows).unwrap_or_default();
    eventually("snapshot", || copy(&mut up).len() == 1);
    assert!(ivm.client.simple_query("CREATE OR REPLACE MATERIALIZED VIEW v AS SELECT id, b, a FROM t").is_err());
    ivm.exec("CREATE OR REPLACE MATERIALIZED VIEW v AS SELECT id, a + b AS a FROM t");
    up.exec("INSERT INTO t VALUES (2, 20, 200)");
    let want = vec![vec![Some("1".to_string()), Some("110".to_string())], vec![Some("2".to_string()), Some("220".to_string())]];
    eventually("replacement delivered", || copy(&mut up) == want);
}

/// Changes produced while a sink cannot connect yet are delivered once it
/// can; retrying the connection used to read (and drop) a queued change
/// each time.
#[test]
fn sink_keeps_changes_while_its_target_is_unreachable() {
    let mut up = Upstream::start();
    up.exec("CREATE TABLE t (id int PRIMARY KEY, a int)");
    up.exec("INSERT INTO t VALUES (1, 1)");
    up.exec("CREATE PUBLICATION p FOR TABLE t");
    let dir = tmpdir("sink-unreachable");
    let mut ivm = Ivm::start(&dir);
    ivm.exec(&format!("CREATE SOURCE s FROM POSTGRES CONNECTION '{}' PUBLICATION 'p'", up.conninfo));
    let _ = caught_up(&mut ivm, &mut up, "SELECT * FROM t");
    ivm.exec("CREATE MATERIALIZED VIEW v AS SELECT id, a FROM t");
    let target = up.conninfo.replace("dbname=postgres", "dbname=later");
    ivm.exec(&format!("CREATE SINK sk FROM v INTO POSTGRES CONNECTION '{target}' TABLE 'v_copy' KEY (id)"));
    for i in 2..=6 {
        up.exec(&format!("INSERT INTO t VALUES ({i}, {i})"));
        let _ = caught_up(&mut ivm, &mut up, "SELECT count(*) FROM t");
        std::thread::sleep(Duration::from_millis(150));
    }
    up.exec("CREATE DATABASE later");
    let mut later = connect(&target);
    eventually("sink", || {
        later.query("SELECT count(*) FROM v_copy").map(|r| r.rows[0][0].clone()).ok().flatten() == Some("6".to_string())
    });
}

/// Tests driving the engine through its API (`EngineHandle::execute`)
/// rather than the wire protocol.
mod api {
    use ivm::coord::api::{ExecResult, SessionVars};
    use ivm::coord::{self, EngineHandle};
    use ivm::types::text::format_datum;
    use pgproto::testutil::{ClusterOptions, TempCluster};
    use pgproto::{Client, Config as PgConfig};
    use std::path::{Path, PathBuf};

    fn exec(h: &EngineHandle, sql: &str, extra: String) -> ExecResult {
        let stmt = ivm::sql::parse_statement(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        h.execute(stmt, vec![], SessionVars::default(), extra).unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    fn rows(h: &EngineHandle, sql: &str) -> Vec<String> {
        match exec(h, sql, String::new()) {
            ExecResult::Query { rows, .. } => rows.iter().map(|r| r.iter().map(format_datum).collect::<Vec<_>>().join("|")).collect(),
            other => panic!("{sql}: expected rows, got {other:?}"),
        }
    }

    fn upstream_rows(c: &mut Client, sql: &str) -> Vec<String> {
        c.query(sql).unwrap().rows.into_iter().map(|r| r.into_iter().map(|v| v.unwrap_or_default()).collect::<Vec<_>>().join("|")).collect()
    }

    fn connect(conninfo: &str) -> Client {
        Client::connect(&PgConfig::parse(conninfo).unwrap()).unwrap()
    }

    fn current_lsn(c: &mut Client) -> String {
        upstream_rows(c, "SELECT pg_current_wal_lsn()")[0].clone()
    }

    /// A fresh engine data directory under `IVM_TEST_TMP` (default: the system
    /// temp directory).
    fn scratch(name: &str) -> PathBuf {
        let base = std::env::var_os("IVM_TEST_TMP").map(PathBuf::from).unwrap_or_else(std::env::temp_dir);
        let dir = base.join(format!("ivm-replication-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn config(dir: &Path) -> coord::Config {
        coord::Config { data_dir: dir.to_path_buf(), sync: false, tick: None, ..Default::default() }
    }

    /// CREATE SOURCE src for publication `p`.
    fn create_source(h: &EngineHandle, conninfo: &str) {
        let discovered = serde_json::to_string(&ivm::source::discover(conninfo, "p", None).unwrap()).unwrap();
        exec(h, &format!("CREATE SOURCE src FROM POSTGRES CONNECTION '{conninfo}' PUBLICATION 'p'"), discovered);
    }

    /// Replicated tables in the engine equal upstream once it has caught up
    /// with everything committed there so far.
    fn assert_caught_up(h: &EngineHandle, up: &mut Client, queries: &[&str]) {
        let lsn = current_lsn(up);
        for q in queries {
            assert_eq!(rows(h, &format!("{q} AS OF AT LEAST LSN '{lsn}'")), upstream_rows(up, q), "{q}");
        }
    }

    fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !done() {
            assert!(std::time::Instant::now() < deadline, "timed out waiting for {what}");
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }

    /// Updates that change a row's primary key but not its TOASTed columns
    /// replicate the new key with "unchanged TOAST" placeholders. Their values
    /// were taken from the old key tuple, which carries only the key, so they
    /// turned into NULLs; they must come from the row stored under the old key.
    #[test]
    fn toasted_values_survive_primary_key_changes() {
        let pg = TempCluster::start(ClusterOptions { extra_conf: vec!["timezone = 'UTC'".into()], ..Default::default() }).expect("start postgres");
        let conninfo = pg.conninfo("postgres");
        let mut up = connect(&conninfo);
        up.simple_query(
            "CREATE TABLE t (id int PRIMARY KEY, n int, big text);
             ALTER TABLE t ALTER COLUMN big SET STORAGE EXTERNAL;
             INSERT INTO t SELECT g, g, repeat(md5(g::text), 200) FROM generate_series(1, 6) g;
             CREATE PUBLICATION p FOR TABLE t;",
        )
        .unwrap();

        let dir = scratch("toast");
        let running = coord::start(config(&dir)).unwrap();
        let h = running.handle.clone();
        create_source(&h, &conninfo);
        exec(&h, "CREATE MATERIALIZED VIEW v AS SELECT id, n, length(big) AS len, md5(big) AS h FROM t", String::new());
        // the initial snapshot must be done, or it copies the updated rows
        assert_caught_up(&h, &mut up, &["SELECT count(big) FROM t"]);

        up.simple_query(
            "UPDATE t SET id = id + 100 WHERE id = 1;
             UPDATE t SET n = -2 WHERE id = 2;
             UPDATE t SET id = 1000, n = n + 1 WHERE id = 4;
             BEGIN; UPDATE t SET id = 200 WHERE id = 3; UPDATE t SET id = 300 WHERE id = 200; COMMIT;
             BEGIN; UPDATE t SET id = -5 WHERE id = 5; UPDATE t SET n = 50 WHERE id = -5; COMMIT;",
        )
        .unwrap();
        let q = "SELECT id, n, length(big), md5(big) FROM t ORDER BY id";
        assert_caught_up(&h, &mut up, &[q]);
        let want = upstream_rows(&mut up, q);
        assert_eq!(want.len(), 6);
        assert_eq!(rows(&h, "SELECT * FROM v ORDER BY id"), want);
        running.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A sink without a key deletes a retracted row by comparing every column,
    /// and equality matches other representations of equal values: retracting
    /// 1.50 could delete 1.5 (or 0.0 delete -0.0), leaving the other
    /// representation behind. (json, which has no equality at all, is
    /// keyless_sink_with_json_columns above.)
    #[test]
    fn keyless_sinks_retract_exact_representations() {
        let pg = TempCluster::start(ClusterOptions::default()).expect("start postgres");
        let conninfo = pg.conninfo("postgres");
        let mut target = connect(&conninfo);
        let dir = scratch("sink-retract");
        let running = coord::start(config(&dir)).unwrap();
        let h = running.handle.clone();
        exec(&h, "CREATE TABLE t (id int PRIMARY KEY, d numeric, f float8)", String::new());
        exec(&h, "INSERT INTO t VALUES (1, 1.5, 0.0), (2, 1.50, -0.0), (3, -0.0, 2.5)", String::new());
        exec(&h, "CREATE MATERIALIZED VIEW d AS SELECT d, f FROM t", String::new());
        exec(&h, &format!("CREATE SINK s_d FROM d INTO POSTGRES (CONNECTION '{conninfo}', TABLE 'd_copy')"), String::new());
        exec(&h, "DELETE FROM t WHERE id = 2", String::new());
        let q = "SELECT d, f FROM d_copy ORDER BY d::text, f::text";
        let want = vec!["0.0|2.5", "1.5|0"];
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        loop {
            let got = target.query(q).map(|r| r.rows).unwrap_or_default();
            let got: Vec<String> = got.into_iter().map(|r| r.into_iter().map(|v| v.unwrap_or_default()).collect::<Vec<_>>().join("|")).collect();
            if got == want {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "{q}: sink target {got:?}, expected {want:?}");
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        running.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Should the engine stop after an upstream schema change but before the
    /// re-snapshot that fills in the new columns has logged progress, the
    /// recovered source state still had the table's snapshot done: it was
    /// never redone, and existing rows kept NULL in the new column in place of
    /// its default.
    #[test]
    fn resnapshots_resume_after_a_crash() {
        let pg = TempCluster::start(ClusterOptions::default()).expect("start postgres");
        let conninfo = pg.conninfo("postgres");
        let mut up = connect(&conninfo);
        up.simple_query(
            "CREATE TABLE t (id int PRIMARY KEY, n int);
             INSERT INTO t SELECT g, g FROM generate_series(1, 6) g;
             CREATE PUBLICATION p FOR TABLE t;",
        )
        .unwrap();
        let dir = scratch("resnapshot-crash");
        let running = coord::start(config(&dir)).unwrap();
        create_source(&running.handle, &conninfo);
        assert_caught_up(&running.handle, &mut up, &["SELECT count(*) FROM t"]);
        running.shutdown();

        // while the engine is down: a new column, which existing rows get with
        // its default without a replicated change, and a change bringing the
        // new shape into the replication stream
        up.simple_query("ALTER TABLE t ADD COLUMN x int DEFAULT 7; UPDATE t SET n = -1 WHERE id = 1;").unwrap();
        // hold back the re-snapshot: its first chunk waits for this lock
        let mut locker = connect(&conninfo);
        locker.simple_query("BEGIN; LOCK TABLE t IN ACCESS EXCLUSIVE MODE").unwrap();
        let running = coord::start(config(&dir)).unwrap();
        let h = running.handle.clone();
        let chunk_waiting = |up: &mut Client| {
            upstream_rows(up, "SELECT pid FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND backend_type = 'client backend'")
        };
        wait_until("the schema change", || {
            let stmt = ivm::sql::parse_statement("SELECT x FROM t AS OF LATEST").unwrap();
            h.execute(stmt, vec![], SessionVars::default(), String::new()).is_ok()
        });
        wait_until("the re-snapshot to wait for the lock", || !chunk_waiting(&mut up).is_empty());
        // stop the engine: the source, once its chunk query fails, sees the
        // stop request and logs nothing more (should it see the failure first,
        // it retries, and its next chunk query waits for the lock again)
        let crashing = std::thread::spawn(move || running.crash());
        while !crashing.is_finished() {
            for pid in chunk_waiting(&mut up) {
                up.simple_query(&format!("SELECT pg_terminate_backend({pid})")).unwrap();
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        crashing.join().unwrap();
        locker.simple_query("ROLLBACK").unwrap();

        let running = coord::start(config(&dir)).unwrap();
        assert_caught_up(&running.handle, &mut up, &["SELECT id, n, x FROM t ORDER BY id"]);
        running.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A table without a primary key (REPLICA IDENTITY FULL) is a multiset. A
    /// TRUNCATE after other changes to it in the same transaction retracted
    /// those changes twice, leaving negative multiplicities: hidden in reads,
    /// they swallowed rows added later. (Its re-snapshot after a schema change
    /// is keyless_tables_resnapshot_without_duplicates above.)
    #[test]
    fn keyless_tables_truncate_within_a_transaction() {
        let pg = TempCluster::start(ClusterOptions::default()).expect("start postgres");
        let conninfo = pg.conninfo("postgres");
        let mut up = connect(&conninfo);
        up.simple_query(
            "CREATE TABLE m (a int, b text);
             ALTER TABLE m REPLICA IDENTITY FULL;
             INSERT INTO m VALUES (1, 'x'), (1, 'x'), (2, 'y');
             CREATE PUBLICATION p FOR TABLE m;",
        )
        .unwrap();
        let dir = scratch("keyless");
        let running = coord::start(config(&dir)).unwrap();
        let h = running.handle.clone();
        create_source(&h, &conninfo);
        exec(&h, "CREATE MATERIALIZED VIEW v AS SELECT a, count(*) AS n FROM m GROUP BY a", String::new());
        let q = "SELECT a, b, count(*) FROM m GROUP BY a, b ORDER BY a, b";
        assert_caught_up(&h, &mut up, &[q]);
        up.simple_query("BEGIN; INSERT INTO m VALUES (3, 'z'); TRUNCATE m; INSERT INTO m VALUES (4, 'w'), (4, 'w'), (5, 'v'); COMMIT;").unwrap();
        assert_caught_up(&h, &mut up, &[q]);
        // (a negative multiplicity hides in reads; adding the row exposes it)
        up.simple_query("INSERT INTO m VALUES (3, 'z')").unwrap();
        assert_caught_up(&h, &mut up, &[q]);
        assert_eq!(rows(&h, "SELECT * FROM v ORDER BY a"), upstream_rows(&mut up, "SELECT a, count(*) FROM m GROUP BY a ORDER BY a"));
        running.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A TRUNCATE committed before a snapshot chunk was read, but replicated
    /// after the chunk started, cleared the chunk: its rows were those added
    /// after the truncate, whose own changes the source skips as the chunk has
    /// them, so they were lost. (Here the chunk is a re-snapshot after a schema
    /// change; it reads the table after both transactions, which the engine,
    /// stopped, replicates only afterwards.)
    #[test]
    fn snapshots_keep_rows_added_after_a_truncate() {
        let pg = TempCluster::start(ClusterOptions::default()).expect("start postgres");
        let conninfo = pg.conninfo("postgres");
        let mut up = connect(&conninfo);
        up.simple_query(
            "CREATE TABLE t (id int PRIMARY KEY, n int);
             INSERT INTO t SELECT g, g FROM generate_series(1, 6) g;
             CREATE PUBLICATION p FOR TABLE t;",
        )
        .unwrap();
        let dir = scratch("snapshot-truncate");
        let running = coord::start(config(&dir)).unwrap();
        create_source(&running.handle, &conninfo);
        assert_caught_up(&running.handle, &mut up, &["SELECT count(*) FROM t"]);
        running.shutdown();
        up.simple_query("ALTER TABLE t ADD COLUMN x int DEFAULT 7; UPDATE t SET n = -1 WHERE id = 1;").unwrap();
        up.simple_query("BEGIN; TRUNCATE t; INSERT INTO t VALUES (10, 10), (11, 11); COMMIT;").unwrap();
        up.simple_query("INSERT INTO t VALUES (12, 12)").unwrap();
        let running = coord::start(config(&dir)).unwrap();
        assert_caught_up(&running.handle, &mut up, &["SELECT id, n, x FROM t ORDER BY id"]);
        running.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A row whose primary key changes while a snapshot chunk holding it is in
    /// flight (the change after the chunk's snapshot), its TOASTed values
    /// unchanged: the replicated new row has placeholders for them, and the old
    /// key's tuple carries only the key. The values are in the chunk's row of
    /// the old key, which the source dropped (the stream wins) instead of
    /// filling them in from it; the engine, which has yet to receive that row,
    /// had nothing to fill them from either, and they became NULL.
    #[test]
    fn key_changes_during_a_snapshot_keep_toasted_values() {
        let pg = TempCluster::start(ClusterOptions::default()).expect("start postgres");
        let conninfo = pg.conninfo("postgres");
        let mut up = connect(&conninfo);
        up.simple_query(
            "CREATE TABLE t (id int PRIMARY KEY, n int, big text);
             ALTER TABLE t ALTER COLUMN big SET STORAGE EXTERNAL;
             INSERT INTO t SELECT g, g, repeat(md5(g::text), 200) FROM generate_series(1, 4) g;
             CREATE PUBLICATION p FOR TABLE t;",
        )
        .unwrap();
        let dir = scratch("snapshot-key-change");
        let running = coord::start(config(&dir)).unwrap();
        create_source(&running.handle, &conninfo);
        assert_caught_up(&running.handle, &mut up, &["SELECT count(*) FROM t"]);
        running.shutdown();

        // while the engine is down: a schema change (the engine snapshots the
        // table again when it sees it) and a new row, which the snapshot reads
        up.simple_query("ALTER TABLE t ADD COLUMN x int DEFAULT 7; UPDATE t SET n = -1 WHERE id = 1;").unwrap();
        up.simple_query("INSERT INTO t VALUES (50, 50, repeat(md5('50'), 200))").unwrap();
        // the chunk takes its snapshot, then waits for this lock; the key
        // change commits after that snapshot, within the chunk's window
        let mut locker = connect(&conninfo);
        locker.simple_query("BEGIN; LOCK TABLE t IN ACCESS EXCLUSIVE MODE").unwrap();
        let running = coord::start(config(&dir)).unwrap();
        wait_until("the chunk to wait for the lock", || {
            !upstream_rows(&mut up, "SELECT pid FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND backend_type = 'client backend'").is_empty()
        });
        locker.simple_query("UPDATE t SET id = 1050 WHERE id = 50; COMMIT").unwrap();
        assert_caught_up(&running.handle, &mut up, &["SELECT id, n, x, md5(big) FROM t ORDER BY id"]);
        running.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
