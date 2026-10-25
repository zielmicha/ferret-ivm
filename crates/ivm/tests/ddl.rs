//! DDL semantics through the public API (no PostgreSQL needed): validation
//! of definitions, DROP and its dependencies, the catalog history.

use ivm::coord::api::{ExecResult, SessionVars};
use ivm::coord::{self, EngineHandle};
use ivm::error::SqlError;
use ivm::types::text::format_datum;
use std::path::{Path, PathBuf};

fn tmpdir(name: &str) -> PathBuf {
    let base = std::env::var("IVM_TEST_TMP").unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().into_owned());
    let d = PathBuf::from(base).join(format!("ivm-ddl-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

/// A running engine whose data directory is removed at shutdown.
struct Engine {
    running: coord::Running,
    dir: PathBuf,
    h: EngineHandle,
}

impl Engine {
    fn start(name: &str) -> Engine {
        let dir = tmpdir(name);
        let running = start_in(&dir);
        let h = running.handle.clone();
        Engine { running, dir, h }
    }

    /// Stop and start again on the same data directory.
    fn restart(self) -> Engine {
        let Engine { running, dir, .. } = self;
        running.shutdown();
        let running = start_in(&dir);
        let h = running.handle.clone();
        Engine { running, dir, h }
    }

    fn shutdown(self) {
        self.running.shutdown();
        let _ = std::fs::remove_dir_all(&self.dir);
    }

    fn try_exec(&self, sql: &str) -> Result<ExecResult, SqlError> {
        let stmt = ivm::sql::parse_statement(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        self.h.execute(stmt, vec![], SessionVars::default(), String::new())
    }

    fn exec(&self, sql: &str) -> ExecResult {
        self.try_exec(sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    /// The error of a statement that must fail: (SQLSTATE, message).
    fn err(&self, sql: &str) -> (String, String) {
        match self.try_exec(sql) {
            Ok(r) => panic!("{sql}: expected an error, got {r:?}"),
            Err(e) => (e.code, e.message),
        }
    }

    /// Result rows as text, in result order.
    fn rows(&self, sql: &str) -> Vec<String> {
        match self.exec(sql) {
            ExecResult::Query { rows, .. } => rows.iter().map(|r| r.iter().map(format_datum).collect::<Vec<_>>().join("|")).collect(),
            other => panic!("{sql}: expected rows, got {other:?}"),
        }
    }

    /// Result column names.
    fn columns(&self, sql: &str) -> Vec<String> {
        match self.exec(sql) {
            ExecResult::Query { columns, .. } => columns.into_iter().map(|c| c.name).collect(),
            other => panic!("{sql}: expected rows, got {other:?}"),
        }
    }
}

fn start_in(dir: &Path) -> coord::Running {
    let cfg = coord::Config { data_dir: dir.to_path_buf(), sync: false, tick: None, ..Default::default() };
    coord::start(cfg).expect("engine starts")
}

fn err(code: &str, msg: &str) -> (String, String) {
    (code.to_string(), msg.to_string())
}

/// Two column-level PRIMARY KEYs are two primary keys (an error in
/// PostgreSQL), not one composite key.
#[test]
fn a_table_has_at_most_one_primary_key() {
    let e = Engine::start("two-pks");
    assert_eq!(
        e.err("CREATE TABLE t (a int PRIMARY KEY, b int PRIMARY KEY)"),
        err("42P16", "multiple primary keys for table \"t\" are not allowed")
    );
    assert_eq!(
        e.err("CREATE TABLE t (a int PRIMARY KEY, b int, PRIMARY KEY (b))"),
        err("42P16", "multiple primary keys for table \"t\" are not allowed")
    );
    assert_eq!(e.err("CREATE TABLE t (a int, PRIMARY KEY (a, a))"), err("42701", "column \"a\" appears twice in primary key constraint"));
    assert_eq!(e.err("CREATE TABLE t (a int, PRIMARY KEY (b))"), err("42703", "column \"b\" named in key does not exist"));
    // nothing was created by the failed statements
    e.exec("CREATE TABLE t (a int, b int, PRIMARY KEY (b, a))");
    assert_eq!(e.rows("SHOW COLUMNS FROM t"), ["a|integer|false|true", "b|integer|false|true"]);
    e.exec("INSERT INTO t VALUES (1, 1), (2, 1)");
    assert_eq!(e.rows("SELECT a, b FROM t ORDER BY a"), ["1|1", "2|1"]);
    e.shutdown();
}

/// A view's column list renames the leading columns of its query; a list
/// longer than the select list is an error (as in PostgreSQL).
#[test]
fn view_column_lists() {
    let e = Engine::start("view-columns");
    e.exec("CREATE TABLE base (a int PRIMARY KEY, b int, c int)");
    e.exec("INSERT INTO base VALUES (1, 2, 3)");
    assert_eq!(e.err("CREATE VIEW v (x, y, z, w) AS SELECT a, b, c FROM base"), err("42601", "CREATE VIEW specifies more column names than columns"));
    assert_eq!(e.err("CREATE MATERIALIZED VIEW m (x, y, z, w) AS SELECT a, b, c FROM base"), err("42601", "too many column names were specified"));
    assert_eq!(e.rows("SELECT count(*) FROM pg_class WHERE relname IN ('v', 'm')"), ["0"]);
    e.exec("CREATE VIEW v (x) AS SELECT a, b, c FROM base");
    e.exec("CREATE MATERIALIZED VIEW m (x, y) AS SELECT a, b, c FROM base");
    // a checkpoint holds m; v and w are replayed from the log
    e.exec("CHECKPOINT");
    e.exec("CREATE MATERIALIZED VIEW w (p, q, r) AS SELECT a, b, c FROM base");
    let check = |e: &Engine| {
        assert_eq!(e.columns("SELECT * FROM v"), ["x", "b", "c"]);
        assert_eq!(e.columns("SELECT * FROM m"), ["x", "y", "c"]);
        assert_eq!(e.columns("SELECT * FROM w"), ["p", "q", "r"]);
        assert_eq!(e.rows("SELECT x, b, c FROM v"), ["1|2|3"]);
        assert_eq!(e.rows("SELECT x, y, c FROM m"), ["1|2|3"]);
        assert_eq!(e.rows("SELECT p + q + r FROM w"), ["6"]);
        assert_eq!(e.rows("SHOW COLUMNS FROM m"), ["x|integer|false|false", "y|integer|true|false", "c|integer|true|false"]);
    };
    check(&e);
    let e = e.restart();
    check(&e);
    e.shutdown();
}
