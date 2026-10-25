//! Replication and sink edge cases against a private PostgreSQL 17
//! (pgproto's `TempCluster`; set `PG_BIN` if it is not in
//! /usr/lib/postgresql/17/bin). Broad randomized coverage lives in
//! ivm-difftest; these are regression tests for specific bugs.

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

/// A sink without a key deletes a retracted row by comparing every column.
/// PostgreSQL has no equality for json, so every batch retracting a json
/// value failed and was retried forever (the target stopped following the
/// view while the sink reported itself running); and equality matches other
/// representations of equal values, so retracting 1.50 could delete 1.5.
/// Also, a sink whose first connection attempt failed (here: two sinks
/// creating their progress table at once) discarded a batch that arrived
/// while it retried, leaving the snapshot's row behind.
#[test]
fn sinks_retract_json_and_exact_representations() {
    let pg = TempCluster::start(ClusterOptions::default()).expect("start postgres");
    let conninfo = pg.conninfo("postgres");
    let mut target = connect(&conninfo);
    let dir = scratch("sink-retract");
    let running = coord::start(config(&dir)).unwrap();
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id int PRIMARY KEY, a int, d numeric)", String::new());
    exec(&h, "INSERT INTO t VALUES (1, 10, 1.5), (2, 20, 1.50), (3, 30, -0.0)", String::new());
    exec(&h, "CREATE MATERIALIZED VIEW j AS SELECT json_agg(a ORDER BY a) AS j, count(*) AS n FROM t", String::new());
    exec(&h, "CREATE MATERIALIZED VIEW d AS SELECT d FROM t", String::new());
    for v in ["j", "d"] {
        exec(&h, &format!("CREATE SINK s_{v} FROM {v} INTO POSTGRES (CONNECTION '{conninfo}', TABLE '{v}_copy')"), String::new());
    }
    exec(&h, "UPDATE t SET a = 40 WHERE id = 1", String::new());
    exec(&h, "DELETE FROM t WHERE id = 2", String::new());
    for (q, want) in [("SELECT j::text, n FROM j_copy", vec!["[30, 40]|2"]), ("SELECT d FROM d_copy ORDER BY d::text", vec!["0.0", "1.5"])] {
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
    }
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// An upstream schema change re-planned the views reading the table, but
/// not views over those views: they kept reading the old version of their
/// source view, which no longer received changes, and went stale.
#[test]
fn schema_changes_replan_views_over_views() {
    let pg = TempCluster::start(ClusterOptions::default()).expect("start postgres");
    let conninfo = pg.conninfo("postgres");
    let mut up = connect(&conninfo);
    up.simple_query(
        "CREATE TABLE t (id int PRIMARY KEY, k int);
         INSERT INTO t SELECT g, g % 3 FROM generate_series(1, 9) g;
         CREATE PUBLICATION p FOR TABLE t;",
    )
    .unwrap();
    let dir = scratch("schema-vv");
    let running = coord::start(config(&dir)).unwrap();
    let h = running.handle.clone();
    create_source(&h, &conninfo);
    exec(&h, "CREATE MATERIALIZED VIEW v AS SELECT k, count(*) AS n FROM t GROUP BY k", String::new());
    exec(&h, "CREATE MATERIALIZED VIEW vv AS SELECT sum(n) AS total, count(*) AS groups FROM v", String::new());
    // after the initial snapshot: earlier changes reach the engine through
    // the snapshot only, which would not announce the new shape
    assert_caught_up(&h, &mut up, &["SELECT count(*) FROM t"]);
    up.simple_query(
        "ALTER TABLE t ADD COLUMN x int DEFAULT 7;
         UPDATE t SET k = 5 WHERE id = 1;
         INSERT INTO t VALUES (10, 6, 1), (11, 6, 2);",
    )
    .unwrap();
    // (a read is planned before it waits: wait without the new column first)
    assert_caught_up(&h, &mut up, &["SELECT count(*) FROM t"]);
    assert_eq!(rows(&h, "SELECT count(*), sum(x) FROM t"), ["11|66"]);
    assert_eq!(rows(&h, "SELECT * FROM v ORDER BY k"), ["0|3", "1|2", "2|3", "5|1", "6|2"]);
    assert_eq!(rows(&h, "SELECT * FROM vv"), ["11|5"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// An upstream schema change makes the source snapshot the table again, to
/// fill in the new columns of existing rows, while the engine keeps the
/// rows it has. Changes to keys the snapshot had yet to reach were skipped,
/// as the snapshot reads those keys later: right for inserts and updates,
/// but a deleted row (or the old key of a changed one) is not there to be
/// read, so it stayed in the engine for good.
#[test]
fn resnapshots_keep_deletes() {
    let pg = TempCluster::start(ClusterOptions::default()).expect("start postgres");
    let conninfo = pg.conninfo("postgres");
    let mut up = connect(&conninfo);
    up.simple_query(
        "CREATE TABLE t (id int PRIMARY KEY, n int);
         INSERT INTO t SELECT g, g FROM generate_series(1, 10) g;
         CREATE PUBLICATION p FOR TABLE t;",
    )
    .unwrap();
    let dir = scratch("resnapshot-deletes");
    let running = coord::start(config(&dir)).unwrap();
    let h = running.handle.clone();
    create_source(&h, &conninfo);
    exec(&h, "CREATE MATERIALIZED VIEW v AS SELECT count(*) AS c, sum(n) AS s FROM t", String::new());
    assert_caught_up(&h, &mut up, &["SELECT count(*) FROM t"]);
    // in one transaction, so that the changes reach the source before the
    // snapshot can start
    up.simple_query(
        "BEGIN;
         ALTER TABLE t ADD COLUMN x int DEFAULT 7;
         DELETE FROM t WHERE id > 7;
         UPDATE t SET id = id + 100 WHERE id = 2;
         UPDATE t SET n = -3 WHERE id = 3;
         COMMIT;",
    )
    .unwrap();
    assert_caught_up(&h, &mut up, &["SELECT id, n, x FROM t ORDER BY id"]);
    assert_eq!(rows(&h, "SELECT * FROM v"), upstream_rows(&mut up, "SELECT count(*), sum(n) FROM t"));
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
/// those changes twice (leaving negative multiplicities), and the
/// re-snapshot after a schema change added every row again next to the
/// old ones.
#[test]
fn keyless_tables_truncate_and_resnapshot() {
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
    up.simple_query("ALTER TABLE m ADD COLUMN c int DEFAULT 9; UPDATE m SET b = 'u' WHERE a = 5;").unwrap();
    assert_caught_up(&h, &mut up, &["SELECT a, b, c, count(*) FROM m GROUP BY a, b, c ORDER BY a, b"]);
    assert_eq!(rows(&h, "SELECT * FROM v ORDER BY a"), upstream_rows(&mut up, "SELECT a, count(*) FROM m GROUP BY a ORDER BY a"));
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
