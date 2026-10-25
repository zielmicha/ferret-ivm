//! Replication regression tests against a private PostgreSQL cluster
//! (pgproto's TempCluster; PostgreSQL binaries from `PG_BIN`).

use ivm::coord::{self, Config};
use ivm::server::{self, ServerConfig};
use pgproto::testutil::{ClusterOptions, TempCluster};
use pgproto::{Client, Config as PgConfig};
use std::net::TcpListener;
use std::path::PathBuf;

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
    ivm.rows(&format!("{query} AS OF AT LEAST LSN '{lsn}'"))
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
