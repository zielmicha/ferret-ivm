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
    port: u16,
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
        Ivm { running: Some(running), client, port }
    }

    /// Another session.
    fn connect(&self) -> Client {
        connect(&format!("host=127.0.0.1 port={} user=ivm dbname=ivm", self.port))
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
        Upstream::start_with(&[])
    }

    /// A cluster with extra postgresql.conf lines.
    fn start_with(conf: &[&str]) -> Upstream {
        let mut extra_conf = vec!["timezone = 'UTC'".to_string()];
        extra_conf.extend(conf.iter().map(|c| c.to_string()));
        let pg = TempCluster::start(ClusterOptions { extra_conf, ..Default::default() }).expect("start postgres");
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
    let (mut up, mut ivm, dir) = setup("smoke", &["CREATE TABLE t (id int PRIMARY KEY, a int)", "INSERT INTO t VALUES (1, 1), (2, 2)"]);
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT * FROM t ORDER BY id"), ["1|1", "2|2"]);
    up.exec("UPDATE t SET a = 10 WHERE id = 1");
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT * FROM t ORDER BY id"), ["1|10", "2|2"]);
    drop(ivm);
    let _ = std::fs::remove_dir_all(&dir);
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
    drop(ivm);
    let _ = std::fs::remove_dir_all(&dir);
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
    drop(ivm);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A view over `SELECT *` takes the table's new shape (reads used to
/// mix the old column list with new rows and panic the engine).
#[test]
fn star_views_take_the_new_table_shape() {
    let (mut up, mut ivm, dir) = setup("star", &["CREATE TABLE t (id int PRIMARY KEY, a text, b int)", "INSERT INTO t VALUES (1, 'one', 10)"]);
    let _ = caught_up(&mut ivm, &mut up, "SELECT * FROM t");
    ivm.exec("CREATE MATERIALIZED VIEW v AS SELECT * FROM t");
    up.exec("ALTER TABLE t DROP COLUMN a");
    up.exec("INSERT INTO t VALUES (2, 20)");
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT * FROM v ORDER BY id"), ["1|10", "2|20"]);
    assert_eq!(ivm.rows("SELECT b FROM v ORDER BY id"), ["10", "20"]);
    up.exec("ALTER TABLE t ADD COLUMN c int DEFAULT 5");
    up.exec("INSERT INTO t VALUES (3, 30, 6)");
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT * FROM v ORDER BY id"), ["1|10|5", "2|20|5", "3|30|6"]);
    drop(ivm);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Tables keyed by upstream types replicated as text (enum, inet) are
/// snapshotted in chunks ordered like the engine's keys (the chunk query
/// used to apply COLLATE to them and fail, stalling the whole source).
#[test]
fn text_replicated_keys_snapshot_in_chunks() {
    let (mut up, mut ivm, dir) = setup(
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
    drop(ivm);
    let _ = std::fs::remove_dir_all(&dir);
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
    drop(ivm);
    let _ = std::fs::remove_dir_all(&dir);
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
    drop(ivm);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Rows deleted upstream while a table is re-snapshotted after a schema
/// change are deleted: the re-snapshot only upserts the rows it reads, and
/// the deletes of keys it had not reached yet used to be skipped.
#[test]
fn deletes_during_a_resnapshot_apply() {
    let (mut up, mut ivm, dir) = setup(
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
    drop(ivm);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Re-snapshotting a table without a primary key (REPLICA IDENTITY FULL)
/// after a schema change replaces its rows; it used to add a second copy
/// of every row.
#[test]
fn keyless_tables_resnapshot_without_duplicates() {
    let (mut up, mut ivm, dir) = setup(
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
    drop(ivm);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A table whose replica identity is a unique index other than the primary
/// key is keyed by that index's columns, which are all that deletes and key
/// changes carry of the old row: they used to be silently lost. A table with
/// such an index but no primary key used to be refused.
#[test]
fn replica_identity_index_is_the_key() {
    let (mut up, mut ivm, _dir) = setup(
        "ri-index",
        &[
            "CREATE TABLE t (id int PRIMARY KEY, u text NOT NULL, a int)",
            "CREATE UNIQUE INDEX t_u ON t (u)",
            "ALTER TABLE t REPLICA IDENTITY USING INDEX t_u",
            "INSERT INTO t SELECT g, 'u' || g, g FROM generate_series(1, 5) g",
            "CREATE TABLE n (u int NOT NULL, v int NOT NULL, a int)",
            "CREATE UNIQUE INDEX n_vu ON n (v, u)",
            "ALTER TABLE n REPLICA IDENTITY USING INDEX n_vu",
            "INSERT INTO n SELECT g, -g, g FROM generate_series(1, 5) g",
        ],
    );
    let (qt, qn) = ("SELECT * FROM t ORDER BY id", "SELECT * FROM n ORDER BY u");
    assert_eq!(caught_up(&mut ivm, &mut up, qt), up.rows(qt));
    assert_eq!(caught_up(&mut ivm, &mut up, qn), up.rows(qn));
    up.exec("DELETE FROM t WHERE id = 1");
    up.exec("UPDATE t SET u = 'x' WHERE id = 2"); // the identity changes
    up.exec("UPDATE t SET id = 30 WHERE id = 3"); // only the primary key changes
    up.exec("UPDATE t SET a = 0 WHERE id = 4");
    up.exec("DELETE FROM n WHERE u = 1");
    up.exec("UPDATE n SET v = 20 WHERE u = 2");
    up.exec("UPDATE n SET a = 0 WHERE u = 3");
    assert_eq!(caught_up(&mut ivm, &mut up, qt), ["2|x|2", "4|u4|0", "5|u5|5", "30|u3|3"]);
    assert_eq!(caught_up(&mut ivm, &mut up, qn), up.rows(qn));
    assert_eq!(up.rows(qn), ["2|20|2", "3|-3|0", "4|-4|4", "5|-5|5"]);
}

/// Upstream changes of a table's replica identity or primary key are
/// detected (in the Relation message that precedes the table's next change):
/// the table is re-keyed and copied again. Deletes after such a change used
/// to be silently lost, and a table whose primary key was dropped merged
/// rows that were no longer unique.
#[test]
fn replica_identity_and_key_changes_are_detected() {
    let (mut up, mut ivm, dir) = setup(
        "ri-change",
        &[
            "CREATE TABLE t (id int PRIMARY KEY, u int NOT NULL, a int)",
            "CREATE UNIQUE INDEX t_u ON t (u)",
            "INSERT INTO t SELECT g, -g, g FROM generate_series(1, 20) g",
            "CREATE TABLE k (id int PRIMARY KEY, a int NOT NULL)",
            "INSERT INTO k SELECT g, 100 + g FROM generate_series(1, 20) g",
            "CREATE TABLE m (a int, b int)",
            "ALTER TABLE m REPLICA IDENTITY FULL",
            "INSERT INTO m SELECT g, g FROM generate_series(1, 20) g",
            "CREATE TABLE x (id int PRIMARY KEY, a int)",
            "INSERT INTO x SELECT g, g FROM generate_series(1, 5) g",
        ],
    );
    let all = |ivm: &mut Ivm, up: &mut Upstream| {
        for t in ["t", "k", "m", "x"] {
            let q = format!("SELECT * FROM {t} ORDER BY 1, 2");
            assert_eq!(caught_up(ivm, up, &q), up.rows(&q), "table {t}");
        }
    };
    all(&mut ivm, &mut up);
    // the identity becomes a unique index instead of the primary key
    up.exec("ALTER TABLE t REPLICA IDENTITY USING INDEX t_u");
    up.exec("DELETE FROM t WHERE id <= 3");
    up.exec("UPDATE t SET id = 100 WHERE id = 4");
    // another primary key
    up.exec("ALTER TABLE k DROP CONSTRAINT k_pkey, ADD PRIMARY KEY (a)");
    up.exec("DELETE FROM k WHERE id <= 3");
    // a keyless table gets a primary key (and the default identity)
    up.exec("ALTER TABLE m ADD PRIMARY KEY (a), REPLICA IDENTITY DEFAULT");
    up.exec("DELETE FROM m WHERE a <= 3");
    up.exec("UPDATE m SET a = 100 WHERE a = 4");
    // no primary key any more: rows need not be unique
    up.exec("ALTER TABLE x DROP CONSTRAINT x_pkey");
    up.exec("INSERT INTO x VALUES (1, 1), (6, 6)");
    all(&mut ivm, &mut up);
    ivm.shutdown();
    let mut ivm = Ivm::start(&dir);
    up.exec("DELETE FROM t WHERE id = 5");
    up.exec("DELETE FROM k WHERE id = 5");
    up.exec("DELETE FROM m WHERE a = 5");
    up.exec("INSERT INTO x VALUES (6, 6)");
    all(&mut ivm, &mut up);
}

/// After a key change the re-snapshot starts by truncating the table, as
/// deletes decoded with the old key could not be applied. The truncate is
/// due when the table's first chunk starts, which can be long after the
/// schema record: here the copy of another table (with a lower id, so it
/// goes first) waits for a transaction that is decoded but stays invisible
/// (it waits for a synchronous standby that never comes). The engine is
/// restarted meanwhile. Applying the schema record used to reset the logged
/// snapshot state without the truncate flag, so the restarted source copied
/// the table over its old rows and a row deleted by the old key stayed.
#[test]
fn key_change_truncate_survives_a_restart() {
    let mut up = Upstream::start_with(&["synchronous_standby_names = 'nobody'", "synchronous_commit = local"]);
    up.exec("CREATE TABLE a (id int PRIMARY KEY, v int)");
    up.exec("INSERT INTO a SELECT g, g FROM generate_series(1, 10) g");
    up.exec("CREATE TABLE c (id int PRIMARY KEY)");
    up.exec("CREATE TABLE t (id int PRIMARY KEY, u int NOT NULL, x int)");
    up.exec("INSERT INTO t SELECT g, -g, g FROM generate_series(1, 10) g");
    up.exec("CREATE PUBLICATION p FOR ALL TABLES");
    let dir = tmpdir("key-change-restart");
    let mut ivm = Ivm::start(&dir);
    ivm.exec(&format!("CREATE SOURCE s FROM POSTGRES CONNECTION '{}' PUBLICATION 'p'", up.conninfo));
    for q in ["SELECT * FROM a ORDER BY id", "SELECT * FROM t ORDER BY id"] {
        assert_eq!(caught_up(&mut ivm, &mut up, q), up.rows(q));
    }
    // committed, decoded, but invisible while it waits for the standby (it
    // keeps its locks meanwhile, hence a table of its own)
    let mut writer = connect(&up.conninfo);
    let pid = writer.query("SELECT pg_backend_pid()").unwrap().rows[0][0].clone().unwrap();
    writer.simple_query("SET synchronous_commit = on").unwrap();
    let blocked = std::thread::spawn(move || writer.simple_query("INSERT INTO c VALUES (100)").map(|_| ()));
    eventually("the insert to wait for the standby", || {
        up.rows(&format!("SELECT wait_event FROM pg_stat_activity WHERE pid = {pid}")) == ["SyncRep"]
    });
    // a changes shape: its copy (first, by id) waits for the invisible
    // transaction. t's key changes after a delete by the old key, so its
    // copy must start with a truncate. c's row makes the commit logged.
    up.exec(
        "BEGIN; ALTER TABLE a ADD COLUMN w int; UPDATE a SET w = 1 WHERE id = 2; DELETE FROM t WHERE id = 1; \
         ALTER TABLE t DROP CONSTRAINT t_pkey, ADD PRIMARY KEY (u); INSERT INTO t VALUES (11, -11, 11); \
         INSERT INTO c VALUES (1); COMMIT",
    );
    ivm.exec("SET read_mode = latest");
    eventually("the transaction to be applied", || ivm.rows("SELECT id FROM c WHERE id = 1") == ["1"]);
    // the truncate has not happened yet: the deleted row is still there
    assert_eq!(ivm.rows("SELECT id, u FROM t WHERE id = 1"), ["1|-1"]);
    ivm.shutdown();
    up.exec(&format!("SELECT pg_cancel_backend({pid})"));
    blocked.join().unwrap().expect("the insert commits");
    let mut ivm = Ivm::start(&dir);
    for q in ["SELECT * FROM t ORDER BY u", "SELECT * FROM a ORDER BY id", "SELECT * FROM c ORDER BY id"] {
        assert_eq!(caught_up(&mut ivm, &mut up, q), up.rows(q), "{q}");
    }
    drop(ivm);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A table the source cannot read (here: no SELECT privilege) does not stop
/// the others: they are copied and streamed while it is retried, the error
/// shows in the source's status, fresh reads of that table wait, and it is
/// copied once it can be read. The failing chunk query used to end every
/// session, so the source never streamed again.
#[test]
fn an_unreadable_table_does_not_stop_the_others() {
    let mut up = Upstream::start();
    for t in ["a", "secret", "z"] {
        up.exec(&format!("CREATE TABLE {t} (id int PRIMARY KEY, v int)"));
        up.exec(&format!("INSERT INTO {t} SELECT g, g FROM generate_series(1, 100) g"));
    }
    up.exec("CREATE ROLE repl LOGIN REPLICATION");
    up.exec("GRANT SELECT ON a, z TO repl");
    up.exec("CREATE PUBLICATION p FOR ALL TABLES");
    let conninfo = up.conninfo.replace("user=postgres", "user=repl");
    let dir = tmpdir("unreadable");
    let mut ivm = Ivm::start(&dir);
    ivm.exec(&format!("CREATE SOURCE s FROM POSTGRES CONNECTION '{conninfo}' PUBLICATION 'p'"));
    // the tables before and after it are copied, and their changes stream
    up.exec("INSERT INTO a VALUES (1000, 1000)");
    up.exec("DELETE FROM z WHERE id = 1");
    eventually("the readable tables", || {
        ivm.rows("SELECT count(*), sum(v) FROM a") == ["101|6050"] && ivm.rows("SELECT count(*) FROM z") == ["99"]
    });
    let status = ivm.rows("SHOW SOURCES")[0].split('|').nth(5).unwrap().to_string();
    assert!(status.starts_with("error: cannot copy table public.secret: permission denied"), "{status}");
    // a fresh read of the table waits for it
    let mut session = ivm.connect();
    let read = std::thread::spawn(move || session.query("SELECT count(*), sum(v) FROM secret").map(|r| r.rows));
    std::thread::sleep(Duration::from_millis(300));
    assert!(!read.is_finished(), "a fresh read of a table that is not copied waited");
    up.exec("UPDATE secret SET v = 0 WHERE id = 1"); // skipped until it is copied
    up.exec("GRANT SELECT ON secret TO repl");
    let rows = read.join().unwrap().expect("read");
    assert_eq!(rows, [vec![Some("100".to_string()), Some("5049".to_string())]]);
    let q = "SELECT count(*), sum(v) FROM secret";
    assert_eq!(caught_up(&mut ivm, &mut up, q), up.rows(q));
    assert_eq!(ivm.rows("SHOW SOURCES")[0].split('|').nth(5).unwrap(), "streaming");
}

/// A change decoded while its table is not copied yet is skipped: the
/// table's chunk reads the row's current value. A transaction can be
/// decoded before it is visible, though (its commit record is written
/// first): here it waits for a synchronous standby that never comes, so it
/// stays invisible until that wait is canceled. A chunk read meanwhile does
/// not contain the change; it used to be read anyway and the change was
/// lost. The chunk now waits until its snapshot sees every decoded
/// transaction.
#[test]
fn chunks_see_transactions_decoded_before_them() {
    let mut up = Upstream::start_with(&["synchronous_standby_names = 'nobody'", "synchronous_commit = local"]);
    up.exec("CREATE TABLE t (id int PRIMARY KEY, a int)");
    up.exec("INSERT INTO t SELECT g, g FROM generate_series(1, 100) g");
    up.exec("CREATE ROLE repl LOGIN REPLICATION");
    up.exec("CREATE PUBLICATION p FOR ALL TABLES");
    let dir = tmpdir("decoded-invisible");
    let mut ivm = Ivm::start(&dir);
    // t cannot be copied until the role may read it
    let conninfo = up.conninfo.replace("user=postgres", "user=repl");
    ivm.exec(&format!("CREATE SOURCE s FROM POSTGRES CONNECTION '{conninfo}' PUBLICATION 'p'"));
    let status = |ivm: &mut Ivm| ivm.rows("SHOW SOURCES")[0].split('|').nth(5).unwrap().to_string();
    eventually("the copy to fail", || status(&mut ivm).contains("permission denied"));
    // committed locally, decoded, but invisible while it waits for the standby
    let mut writer = connect(&up.conninfo);
    let pid = writer.query("SELECT pg_backend_pid()").unwrap().rows[0][0].clone().unwrap();
    writer.simple_query("SET synchronous_commit = on").unwrap();
    let update = std::thread::spawn(move || writer.simple_query("UPDATE t SET a = -1 WHERE id = 5").map(|_| ()));
    eventually("the update to wait for the standby", || {
        up.rows(&format!("SELECT wait_event FROM pg_stat_activity WHERE pid = {pid}")) == ["SyncRep"]
    });
    let flushed = up.rows("SELECT pg_current_wal_flush_lsn()").remove(0);
    eventually("the update to be decoded", || {
        up.rows(&format!("SELECT confirmed_flush_lsn >= '{flushed}' FROM pg_replication_slots")) == ["t"]
    });
    // the next attempt reads t while the update is invisible
    let retry: u64 = status(&mut ivm).split("retrying every ").nth(1).unwrap().split('s').next().unwrap().parse().unwrap();
    up.exec("GRANT SELECT ON t TO repl");
    std::thread::sleep(Duration::from_millis(retry * 1000 + 500));
    up.exec(&format!("SELECT pg_cancel_backend({pid})"));
    update.join().unwrap().expect("the update commits");
    let q = "SELECT * FROM t WHERE id IN (4, 5, 6) ORDER BY id";
    assert_eq!(up.rows(q), ["4|4", "5|-1", "6|6"]);
    assert_eq!(caught_up(&mut ivm, &mut up, q), up.rows(q));
}

/// Default slot names contain an id unique to the data directory: two
/// engines with similar catalogs replicating one database used to get the
/// same slot name, and fought over the slot. The name is logged with the
/// source, so replaying the log (here: from the start, without the
/// checkpoint) gives the same one.
#[test]
fn default_slot_names_differ_between_engines() {
    let mut up = Upstream::start();
    up.exec("CREATE TABLE t (id int PRIMARY KEY)");
    up.exec("INSERT INTO t VALUES (1)");
    up.exec("CREATE PUBLICATION p FOR ALL TABLES");
    let (dir1, dir2) = (tmpdir("slot-names-1"), tmpdir("slot-names-2"));
    let (mut a, mut b) = (Ivm::start(&dir1), Ivm::start(&dir2));
    for ivm in [&mut a, &mut b] {
        ivm.exec(&format!("CREATE SOURCE s FROM POSTGRES CONNECTION '{}' PUBLICATION 'p'", up.conninfo));
    }
    let (sa, sb) = (slot_name(&mut a), slot_name(&mut b));
    assert_ne!(sa, sb);
    assert!(sa.starts_with("ivm_s_") && sa.ends_with("_1000") && sa.len() > "ivm_s_1000".len(), "{sa}");
    up.exec("INSERT INTO t VALUES (2)");
    for ivm in [&mut a, &mut b] {
        assert_eq!(caught_up(ivm, &mut up, "SELECT count(*) FROM t"), ["2"]);
    }
    a.shutdown();
    std::fs::remove_dir_all(dir1.join("store")).unwrap();
    let mut a = Ivm::start(&dir1);
    assert_eq!(slot_name(&mut a), sa);
    up.exec("INSERT INTO t VALUES (3)");
    assert_eq!(caught_up(&mut a, &mut up, "SELECT count(*) FROM t"), ["3"]);
    assert_eq!(up.rows("SELECT count(*) FROM pg_replication_slots"), ["2"]);
}

fn slot_exists(up: &mut Upstream, slot: &str) -> bool {
    up.rows(&format!("SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{slot}'")) == ["1"]
}

/// The slot of a dropped source is dropped even if the upstream is down
/// for longer than a few seconds: it used to be given up after about 5 s,
/// leaving a slot that retains WAL for good.
#[test]
fn slots_of_dropped_sources_are_dropped_after_an_outage() {
    let (mut up, mut ivm, _dir) = setup("slot-drop-outage", &["CREATE TABLE t (id int PRIMARY KEY)"]);
    let _ = caught_up(&mut ivm, &mut up, "SELECT count(*) FROM t");
    let slot = slot_name(&mut ivm);
    up._pg.stop();
    ivm.exec("DROP SOURCE s");
    std::thread::sleep(Duration::from_secs(6));
    up._pg.restart().unwrap();
    up.client = connect(&up.conninfo);
    assert!(slot_exists(&mut up, &slot));
    eventually("the slot to be dropped", || !slot_exists(&mut up, &slot));
}

/// A slot drop that has not succeeded yet when the engine stops is retried
/// after the next start; it used to be forgotten.
#[test]
fn slot_drops_survive_a_restart() {
    let (mut up, mut ivm, dir) = setup("slot-drop-restart", &["CREATE TABLE t (id int PRIMARY KEY)"]);
    let _ = caught_up(&mut ivm, &mut up, "SELECT count(*) FROM t");
    let slot = slot_name(&mut ivm);
    up._pg.stop();
    ivm.exec("DROP SOURCE s");
    ivm.shutdown();
    // (longer than the drop used to be retried)
    std::thread::sleep(Duration::from_secs(6));
    up._pg.restart().unwrap();
    up.client = connect(&up.conninfo);
    assert!(slot_exists(&mut up, &slot));
    let _ivm = Ivm::start(&dir);
    eventually("the slot to be dropped", || !slot_exists(&mut up, &slot));
}

/// The replication slot of the (only) source.
fn slot_name(ivm: &mut Ivm) -> String {
    ivm.rows("SHOW SOURCES")[0].split('|').nth(2).unwrap().to_string()
}

/// Tables of the slot tests and their checks.
const SLOT_TABLES: &[&str] = &[
    "CREATE TABLE t (id int PRIMARY KEY, a int)",
    "INSERT INTO t SELECT g, g FROM generate_series(1, 25000) g",
    "CREATE TABLE m (a int)",
    "ALTER TABLE m REPLICA IDENTITY FULL",
    "INSERT INTO m VALUES (1), (2), (2)",
];

fn slot_tables_agree(ivm: &mut Ivm, up: &mut Upstream) {
    for q in ["SELECT count(*), sum(a), sum(id) FROM t", "SELECT a FROM m ORDER BY a"] {
        assert_eq!(caught_up(ivm, up, q), up.rows(q), "{q}");
    }
    assert_eq!(caught_up(ivm, up, "SELECT * FROM v"), up.rows("SELECT count(*), sum(a) FROM t"));
}

/// Changes no source sees: made while the slot does not exist.
fn lost_changes(up: &mut Upstream, round: i32) {
    up.exec(&format!("DELETE FROM t WHERE id % 7 = {round}"));
    up.exec(&format!("UPDATE t SET a = -a WHERE id % 7 = {}", round + 1));
    up.exec(&format!("INSERT INTO t VALUES ({}, 1)", 100_000 + round));
    up.exec(&format!("DELETE FROM m WHERE a = {}", round + 1));
    up.exec(&format!("INSERT INTO m VALUES ({})", 10 + round));
}

/// A replication slot dropped upstream is re-created and every table copied
/// again, so that changes made while it was missing, deletes included, are
/// not lost. The source used to fail for good (`replication slot ... does
/// not exist`).
#[test]
fn dropped_slots_are_recreated() {
    let (mut up, mut ivm, dir) = setup("slot-dropped", SLOT_TABLES);
    ivm.exec("CREATE MATERIALIZED VIEW v AS SELECT count(*) AS n, sum(a) AS s FROM t");
    slot_tables_agree(&mut ivm, &mut up);
    let slot = slot_name(&mut ivm);
    // dropped while the engine is stopped
    ivm.shutdown();
    up.exec(&format!("SELECT pg_drop_replication_slot('{slot}')"));
    lost_changes(&mut up, 0);
    let mut ivm = Ivm::start(&dir);
    up.exec("DELETE FROM t WHERE id = 24999"); // while it is copied again
    slot_tables_agree(&mut ivm, &mut up);
    // dropped while the engine runs: end its session, drop the slot before
    // the source reconnects
    eventually("the slot to be dropped", || {
        up.exec("SELECT pg_terminate_backend(pid) FROM pg_stat_replication WHERE application_name = 'ivm'");
        up.client.simple_query(&format!("SELECT pg_drop_replication_slot('{slot}')")).is_ok()
    });
    lost_changes(&mut up, 2);
    slot_tables_agree(&mut ivm, &mut up);
    assert_eq!(up.rows(&format!("SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{slot}'")), ["1"]);
}

/// A slot invalidated because it retained too much WAL
/// (max_slot_wal_keep_size) cannot be used again: it is dropped and
/// re-created, and every table copied again.
#[test]
fn invalidated_slots_are_recreated() {
    let mut up = Upstream::start_with(&["max_slot_wal_keep_size = 1MB"]);
    for d in SLOT_TABLES {
        up.exec(d);
    }
    up.exec("CREATE PUBLICATION p FOR ALL TABLES");
    let dir = tmpdir("slot-invalidated");
    let mut ivm = Ivm::start(&dir);
    ivm.exec(&format!("CREATE SOURCE s FROM POSTGRES CONNECTION '{}' PUBLICATION 'p'", up.conninfo));
    ivm.exec("CREATE MATERIALIZED VIEW v AS SELECT count(*) AS n, sum(a) AS s FROM t");
    slot_tables_agree(&mut ivm, &mut up);
    let slot = slot_name(&mut ivm);
    ivm.shutdown();
    lost_changes(&mut up, 0);
    eventually("the slot to be invalidated", || {
        up.exec("SELECT pg_switch_wal(); CHECKPOINT");
        up.rows(&format!("SELECT wal_status FROM pg_replication_slots WHERE slot_name = '{slot}'")) == ["lost"]
    });
    let mut ivm = Ivm::start(&dir);
    slot_tables_agree(&mut ivm, &mut up);
    assert_eq!(up.rows(&format!("SELECT wal_status <> 'lost' FROM pg_replication_slots WHERE slot_name = '{slot}'")), ["t"]);
}

/// Text in a SQL_ASCII database is not checked for any encoding, so values
/// that are not UTF-8 could reach the source (where they used to become '',
/// or U+FFFD in snapshots). Such upstreams are rejected.
#[test]
fn sql_ascii_upstreams_are_rejected() {
    let mut up = Upstream::start();
    up.exec("CREATE DATABASE legacy TEMPLATE template0 ENCODING 'SQL_ASCII' LOCALE_PROVIDER libc LOCALE 'C'");
    let conninfo = up.conninfo.replace("dbname=postgres", "dbname=legacy");
    let mut db = connect(&conninfo);
    db.simple_query("CREATE TABLE t (id int PRIMARY KEY, s text); INSERT INTO t VALUES (1, E'caf\\xe9'); CREATE PUBLICATION p FOR ALL TABLES")
        .unwrap();
    let dir = tmpdir("sql-ascii");
    let mut ivm = Ivm::start(&dir);
    let err = ivm.client.simple_query(&format!("CREATE SOURCE s FROM POSTGRES CONNECTION '{conninfo}' PUBLICATION 'p'")).unwrap_err();
    assert!(err.message.contains("SQL_ASCII"), "{}", err.message);
    // values must arrive as UTF-8, not in another client encoding
    up.exec("CREATE TABLE u (id int PRIMARY KEY, s text); CREATE PUBLICATION p FOR ALL TABLES");
    let latin1 = format!("{} client_encoding=LATIN1", up.conninfo);
    let err = ivm.client.simple_query(&format!("CREATE SOURCE s FROM POSTGRES CONNECTION '{latin1}' PUBLICATION 'p'")).unwrap_err();
    assert!(err.message.contains("client_encoding UTF8"), "{}", err.message);
    assert!(ivm.rows("SHOW SOURCES").is_empty());
}

/// Wait until `f` returns true (or fail after a timeout).
fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    let t0 = Instant::now();
    while !f() {
        assert!(t0.elapsed() < Duration::from_secs(60), "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Process ids of the engine's walsenders (its sources' replication sessions).
fn walsenders(up: &mut Upstream) -> Vec<String> {
    up.rows("SELECT pid FROM pg_stat_replication WHERE application_name = 'ivm' ORDER BY pid")
}

/// Terminate the source's replication session and wait until it has
/// reconnected; returns how long the reconnection took.
fn kill_walsender(up: &mut Upstream) -> Duration {
    let mut old = Vec::new();
    eventually("a walsender", || {
        old = walsenders(up);
        !old.is_empty()
    });
    up.exec("SELECT pg_terminate_backend(pid) FROM pg_stat_replication WHERE application_name = 'ivm'");
    let t0 = Instant::now();
    eventually("the source to reconnect", || {
        let now = walsenders(up);
        !now.is_empty() && now.iter().all(|p| !old.contains(p))
    });
    t0.elapsed()
}

/// The reconnect delay starts small again after a session that worked: it
/// used to keep doubling for good, so a source that had failed a few times
/// waited 10 s before every later reconnect.
#[test]
fn reconnect_backoff_resets_after_a_working_session() {
    let (mut up, mut ivm, _dir) = setup("backoff", &["CREATE TABLE t (id int PRIMARY KEY)"]);
    let _ = caught_up(&mut ivm, &mut up, "SELECT count(*) FROM t");
    // sessions that end at once, without doing anything: the delay grows
    // (0.2 s, 0.4 s, 0.8 s, 1.6 s, 3.2 s)
    for _ in 0..5 {
        kill_walsender(&mut up);
    }
    // a session that replicates something works: the next delay is short
    // again (it would be 6.4 s otherwise)
    up.exec("INSERT INTO t VALUES (1)");
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT count(*) FROM t"), ["1"]);
    let took = kill_walsender(&mut up);
    assert!(took < Duration::from_secs(3), "reconnecting took {took:?}");
    up.exec("INSERT INTO t VALUES (2)");
    assert_eq!(caught_up(&mut ivm, &mut up, "SELECT count(*) FROM t"), ["2"]);
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
    drop(ivm);
    let _ = std::fs::remove_dir_all(&dir);
}

/// An engine restart does not rewrite a sink's target that is up to date:
/// every restart used to resynchronize every sink (DELETE and INSERT of the
/// whole table), because the progress row only recorded GSNs at which the
/// relation changed, always older than the restarted engine's snapshot.
#[test]
fn restarts_do_not_resync_sinks() {
    let mut up = Upstream::start();
    up.exec("CREATE TABLE t (id int PRIMARY KEY, a int)");
    up.exec("INSERT INTO t SELECT g, g % 3 FROM generate_series(1, 30) g");
    up.exec("CREATE PUBLICATION p FOR TABLE t");
    let dir = tmpdir("sink-no-resync");
    let mut ivm = Ivm::start(&dir);
    ivm.exec(&format!("CREATE SOURCE s FROM POSTGRES CONNECTION '{}' PUBLICATION 'p'", up.conninfo));
    ivm.exec("CREATE MATERIALIZED VIEW v AS SELECT a, count(*) AS n FROM t GROUP BY a");
    ivm.exec(&format!("CREATE SINK sk FROM v INTO POSTGRES CONNECTION '{}' TABLE 'v_copy' KEY (a)", up.conninfo));
    let want = "SELECT a, count(*) FROM t GROUP BY a ORDER BY a";
    let synced = |up: &mut Upstream| {
        eventually("the sink", || up.client.query("SELECT a, n FROM v_copy ORDER BY a").map(|r| r.rows).ok() == up.client.query(want).ok().map(|r| r.rows))
    };
    synced(&mut up);
    // (written by the transaction that wrote each row)
    let xmins = |up: &mut Upstream| up.rows("SELECT a, xmin FROM v_copy ORDER BY a");
    let before = xmins(&mut up);
    let sink_status = |ivm: &mut Ivm| ivm.rows("SHOW SINKS")[0].split('|').nth(3).unwrap().to_string();
    for round in 0..3 {
        ivm.shutdown();
        if round == 2 {
            // a record logged after the last checkpoint, as a crash leaves
            // them: replayed at the start, it reaches the sink like a live one
            let mut log = ivm::log::LogWriter::open(&dir.join("log"), true).unwrap();
            let gsn = log.last_gsn() + 1;
            log.append(gsn, &ivm::log::LogRecord::Tick { micros: 1_000_000_000_000_000 }).unwrap();
            log.flush().unwrap();
        }
        ivm = Ivm::start(&dir);
        eventually("the sink to run", || sink_status(&mut ivm).starts_with("running"));
        // (ticks and changes to other rows keep coming)
        std::thread::sleep(Duration::from_millis(1500));
        assert_eq!(xmins(&mut up), before, "the target was rewritten (restart {round})");
    }
    up.exec("UPDATE t SET a = 5 WHERE id = 1");
    synced(&mut up);
    let after = xmins(&mut up);
    assert_eq!(after.iter().filter(|r| before.contains(r)).count(), before.len() - 1, "only the changed row was written: {after:?}");
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
    drop(ivm);
    let _ = std::fs::remove_dir_all(&dir);
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
    drop(ivm);
    let _ = std::fs::remove_dir_all(&dir);
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
    drop(ivm);
    let _ = std::fs::remove_dir_all(&dir);
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
