//! Replication and sink edge cases against a private PostgreSQL 17
//! (pgproto's `TempCluster`; set `PG_BIN` if it is not in
//! /usr/lib/postgresql/17/bin). Broad randomized coverage lives in
//! ivm-difftest; these are regression tests for specific bugs.

use ivm::coord::api::{ExecResult, SessionVars};
use ivm::coord::{self, EngineHandle};
use ivm::types::text::format_datum;
use pgproto::testutil::{ClusterOptions, TempCluster};
use pgproto::{Client, Config as PgConfig};
use std::path::PathBuf;

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

/// Updates that change a row's primary key but not its TOASTed columns
/// replicate the new key with "unchanged TOAST" placeholders. Their values
/// were taken from the old key tuple, which carries only the key, so they
/// turned into NULLs; they must come from the row stored under the old key.
#[test]
fn toasted_values_survive_primary_key_changes() {
    let pg = TempCluster::start(ClusterOptions { extra_conf: vec!["timezone = 'UTC'".into()], ..Default::default() }).expect("start postgres");
    let conninfo = pg.conninfo("postgres");
    let mut up = Client::connect(&PgConfig::parse(&conninfo).unwrap()).unwrap();
    up.simple_query(
        "CREATE TABLE t (id int PRIMARY KEY, n int, big text);
         ALTER TABLE t ALTER COLUMN big SET STORAGE EXTERNAL;
         INSERT INTO t SELECT g, g, repeat(md5(g::text), 200) FROM generate_series(1, 6) g;
         CREATE PUBLICATION p FOR TABLE t;",
    )
    .unwrap();

    let dir = PathBuf::from(std::env::var("IVM_TEST_TMP").unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().into_owned()))
        .join(format!("ivm-replication-toast-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let running = coord::start(coord::Config { data_dir: dir.clone(), sync: false, tick: None, ..Default::default() }).unwrap();
    let h = running.handle.clone();
    let discovered = serde_json::to_string(&ivm::source::discover(&conninfo, "p", None).unwrap()).unwrap();
    exec(&h, &format!("CREATE SOURCE src FROM POSTGRES CONNECTION '{conninfo}' PUBLICATION 'p'"), discovered);
    exec(&h, "CREATE MATERIALIZED VIEW v AS SELECT id, n, length(big) AS len, md5(big) AS h FROM t", String::new());
    // the initial snapshot must be done, or it copies the updated rows
    let lsn = upstream_rows(&mut up, "SELECT pg_current_wal_lsn()")[0].clone();
    assert_eq!(rows(&h, &format!("SELECT count(big) FROM t AS OF AT LEAST LSN '{lsn}'")), ["6"]);

    up.simple_query(
        "UPDATE t SET id = id + 100 WHERE id = 1;
         UPDATE t SET n = -2 WHERE id = 2;
         UPDATE t SET id = 1000, n = n + 1 WHERE id = 4;
         BEGIN; UPDATE t SET id = 200 WHERE id = 3; UPDATE t SET id = 300 WHERE id = 200; COMMIT;
         BEGIN; UPDATE t SET id = -5 WHERE id = 5; UPDATE t SET n = 50 WHERE id = -5; COMMIT;",
    )
    .unwrap();
    let lsn = upstream_rows(&mut up, "SELECT pg_current_wal_lsn()")[0].clone();
    let q = "SELECT id, n, length(big), md5(big) FROM t ORDER BY id";
    let want = upstream_rows(&mut up, q);
    assert_eq!(want.len(), 6);
    assert_eq!(rows(&h, &format!("{q} AS OF AT LEAST LSN '{lsn}'")), want);
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
    let mut target = Client::connect(&PgConfig::parse(&conninfo).unwrap()).unwrap();
    let dir = PathBuf::from(std::env::var("IVM_TEST_TMP").unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().into_owned()))
        .join(format!("ivm-sink-retract-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let running = coord::start(coord::Config { data_dir: dir.clone(), sync: false, tick: None, ..Default::default() }).unwrap();
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
    let mut up = Client::connect(&PgConfig::parse(&conninfo).unwrap()).unwrap();
    up.simple_query(
        "CREATE TABLE t (id int PRIMARY KEY, k int);
         INSERT INTO t SELECT g, g % 3 FROM generate_series(1, 9) g;
         CREATE PUBLICATION p FOR TABLE t;",
    )
    .unwrap();
    let dir = PathBuf::from(std::env::var("IVM_TEST_TMP").unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().into_owned()))
        .join(format!("ivm-schema-vv-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let running = coord::start(coord::Config { data_dir: dir.clone(), sync: false, tick: None, ..Default::default() }).unwrap();
    let h = running.handle.clone();
    let discovered = serde_json::to_string(&ivm::source::discover(&conninfo, "p", None).unwrap()).unwrap();
    exec(&h, &format!("CREATE SOURCE src FROM POSTGRES CONNECTION '{conninfo}' PUBLICATION 'p'"), discovered);
    exec(&h, "CREATE MATERIALIZED VIEW v AS SELECT k, count(*) AS n FROM t GROUP BY k", String::new());
    exec(&h, "CREATE MATERIALIZED VIEW vv AS SELECT sum(n) AS total, count(*) AS groups FROM v", String::new());
    // after the initial snapshot: earlier changes reach the engine through
    // the snapshot only, which would not announce the new shape
    let lsn = upstream_rows(&mut up, "SELECT pg_current_wal_lsn()")[0].clone();
    assert_eq!(rows(&h, &format!("SELECT count(*) FROM t AS OF AT LEAST LSN '{lsn}'")), ["9"]);
    up.simple_query(
        "ALTER TABLE t ADD COLUMN x int DEFAULT 7;
         UPDATE t SET k = 5 WHERE id = 1;
         INSERT INTO t VALUES (10, 6, 1), (11, 6, 2);",
    )
    .unwrap();
    let lsn = upstream_rows(&mut up, "SELECT pg_current_wal_lsn()")[0].clone();
    // (a read is planned before it waits: wait without the new column first)
    assert_eq!(rows(&h, &format!("SELECT count(*) FROM t AS OF AT LEAST LSN '{lsn}'")), ["11"]);
    assert_eq!(rows(&h, "SELECT count(*), sum(x) FROM t"), ["11|66"]);
    assert_eq!(rows(&h, "SELECT * FROM v ORDER BY k"), ["0|3", "1|2", "2|3", "5|1", "6|2"]);
    assert_eq!(rows(&h, "SELECT * FROM vv"), ["11|5"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
