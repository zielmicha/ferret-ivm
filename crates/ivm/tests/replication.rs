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
