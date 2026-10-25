//! Replication edge cases against a private PostgreSQL 17 (pgproto's
//! `TempCluster`; set `PG_BIN` if it is not in /usr/lib/postgresql/17/bin).
//! Broad randomized coverage lives in ivm-difftest; these are regression
//! tests for specific bugs.

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
