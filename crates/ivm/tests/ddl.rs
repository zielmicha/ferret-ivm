//! DDL semantics through the public API (no PostgreSQL needed): validation
//! of definitions, DROP and its dependencies, the catalog history.

use ivm::coord::api::{ExecResult, SessionVars};
use ivm::coord::inspect::{InspectQuery, InspectReply};
use ivm::log::LogRecord;
use std::time::Duration;

mod common;
use common::Engine;

impl Engine {
    fn err_pair(&self, sql: &str) -> (String, String) {
        let e = self.err(sql);
        (e.code, e.message)
    }
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
        e.err_pair("CREATE TABLE t (a int PRIMARY KEY, b int PRIMARY KEY)"),
        err("42P16", "multiple primary keys for table \"t\" are not allowed")
    );
    assert_eq!(
        e.err_pair("CREATE TABLE t (a int PRIMARY KEY, b int, PRIMARY KEY (b))"),
        err("42P16", "multiple primary keys for table \"t\" are not allowed")
    );
    assert_eq!(e.err_pair("CREATE TABLE t (a int, PRIMARY KEY (a, a))"), err("42701", "column \"a\" appears twice in primary key constraint"));
    assert_eq!(e.err_pair("CREATE TABLE t (a int, PRIMARY KEY (b))"), err("42703", "column \"b\" named in key does not exist"));
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
    assert_eq!(e.err_pair("CREATE VIEW v (x, y, z, w) AS SELECT a, b, c FROM base"), err("42601", "CREATE VIEW specifies more column names than columns"));
    assert_eq!(e.err_pair("CREATE MATERIALIZED VIEW m (x, y, z, w) AS SELECT a, b, c FROM base"), err("42601", "too many column names were specified"));
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

/// CREATE SINK checks its KEY against the relation's columns: the sink
/// thread creates the target table with the key as primary key, and used
/// to retry an impossible definition forever. A key must compare in the
/// target's primary key as in the engine: `character` values differing only
/// in trailing spaces are distinct keys in the engine, but one in the
/// target, which merged their rows (equal values of other representations,
/// `1.0` and `1.00`, are one key in both).
#[test]
fn sink_keys_are_validated() {
    let e = Engine::start("sink-keys");
    e.exec("CREATE TABLE t (a int PRIMARY KEY, b text, j json, jp jsonpath[], c bpchar, cs bpchar[], c3 char(3), c3s char(3)[], n numeric, iv interval[])");
    e.exec("CREATE VIEW v AS SELECT a FROM t");
    let sink = |from: &str, key: &str| format!("CREATE SINK s FROM {from} INTO POSTGRES CONNECTION 'host=127.0.0.1 port=1' TABLE 'target' KEY ({key})");
    assert_eq!(e.err_pair(&sink("t", "a, nope")), err("42703", "sink key column \"nope\" does not exist in \"t\""));
    assert_eq!(e.err_pair(&sink("t", "b, a, b")), err("42701", "column \"b\" appears twice in the sink key"));
    assert_eq!(e.err_pair(&sink("t", "j")), err("0A000", "sink key column \"j\" has type json, which cannot be part of a primary key"));
    assert_eq!(e.err_pair(&sink("t", "jp")), err("0A000", "sink key column \"jp\" has type jsonpath[], which cannot be part of a primary key"));
    let spaces = |c: &str, ty: &str| {
        err("0A000", &format!("sink key column \"{c}\" has type {ty}, whose values differing only in trailing spaces would be one key in the target"))
    };
    assert_eq!(e.err_pair(&sink("t", "a, c")), spaces("c", "bpchar"));
    assert_eq!(e.err_pair(&sink("t", "cs")), spaces("cs", "bpchar[]"));
    assert_eq!(e.err_pair(&sink("v", "a")), err("42809", "sink source \"v\" must be a table or materialized view"));
    assert_eq!(e.err_pair(&sink("nope", "a")), err("42P01", "relation \"nope\" does not exist"));
    // rejected before anything was logged
    assert_eq!(e.rows("SHOW SINKS"), Vec::<String>::new());
    assert_eq!(e.rows("SELECT count(*) FROM ivm.catalog_history"), ["2"]);
    // (character(n) values all have n characters)
    e.exec(&sink("t", "c3, c3s, n, iv"));
    assert_eq!(e.rows("SELECT count(*) FROM ivm.catalog_history"), ["3"]);
    e.shutdown();
}

/// A relation's secondary indexes are dropped with it, without CASCADE (as
/// in PostgreSQL); views and sinks still need CASCADE.
#[test]
fn dropping_a_relation_drops_its_indexes() {
    let e = Engine::start("drop-indexes");
    e.exec("CREATE TABLE t (a int PRIMARY KEY, b int)");
    e.exec("INSERT INTO t VALUES (1, 10), (2, 20)");
    e.exec("CREATE INDEX t_b ON t (b)");
    e.exec("CREATE MATERIALIZED VIEW m AS SELECT a, b * 2 AS c FROM t");
    e.exec("CREATE INDEX m_c ON m (c)");
    let arrangements = e.rows("SHOW ARRANGEMENTS").len();
    // the view, not the (older) index, is what holds the table back
    let stmt = ivm::sql::parse_statement("DROP TABLE t").unwrap();
    let er = e.handle.execute(stmt, vec![], SessionVars::default(), String::new()).unwrap_err();
    assert_eq!((er.code.as_str(), er.detail.as_deref()), ("2BP01", Some("materialized view m depends on table t")));
    e.exec("DROP MATERIALIZED VIEW m");
    assert_eq!(e.rows("SHOW INDEXES"), ["t_b|t|[1]"]);
    assert!(e.rows("SHOW ARRANGEMENTS").len() < arrangements);
    e.exec("DROP TABLE t");
    assert_eq!(e.rows("SHOW INDEXES"), Vec::<String>::new());
    assert_eq!(e.rows("SHOW ARRANGEMENTS"), Vec::<String>::new());
    // the names are free again, also after a restart
    let e = e.restart();
    assert_eq!(e.rows("SELECT count(*) FROM ivm.objects"), ["0"]);
    e.exec("CREATE TABLE t (a int PRIMARY KEY, b int)");
    e.exec("CREATE INDEX t_b ON t (b)");
    e.shutdown();
}

/// A DROP naming several objects is atomic (an error drops nothing) and
/// lands in the catalog history; the objects it names do not hold each
/// other back.
#[test]
fn multi_name_drop_is_atomic() {
    let e = Engine::start("multi-drop");
    e.exec("CREATE TABLE t (a int PRIMARY KEY)");
    e.exec("CREATE VIEW a AS SELECT a FROM t");
    e.exec("CREATE MATERIALIZED VIEW b AS SELECT a FROM a");
    let history = |e: &Engine| e.rows("SELECT statement FROM ivm.catalog_history ORDER BY gsn");
    let objects = |e: &Engine| e.rows("SELECT name FROM ivm.objects ORDER BY name");
    let before = history(&e);
    // errors leave every object in place and record nothing
    assert_eq!(e.err_pair("DROP VIEW b, a, nosuch"), err("42P01", "view \"nosuch\" does not exist"));
    assert_eq!(e.err_pair("DROP VIEW b, t"), err("42809", "\"t\" is not a view"));
    assert_eq!(e.err_pair("DROP VIEW a").0, "2BP01");
    assert_eq!(objects(&e), ["a", "b", "t"]);
    assert_eq!(history(&e), before);
    e.exec("DROP VIEW a, b");
    assert_eq!(objects(&e), ["t"]);
    let after = history(&e);
    assert_eq!(after.len(), before.len() + 1);
    assert_eq!(after.last().unwrap(), "DROP VIEW a, b");
    e.exec("DROP TABLE IF EXISTS nosuch, t, t");
    assert_eq!(objects(&e), Vec::<String>::new());
    let after = history(&e);
    let e = e.restart();
    assert_eq!(history(&e), after);
    e.shutdown();
}

/// SUBSCRIBE to a view streams the view's columns, named by its column
/// list, as reads of the view see them.
#[test]
fn subscriptions_use_the_view_column_names() {
    let e = Engine::start("subscribe-columns");
    e.exec("CREATE TABLE base (a int PRIMARY KEY, b int, c int)");
    e.exec("INSERT INTO base VALUES (1, 2, 3)");
    e.exec("CREATE VIEW v (x) AS SELECT a, b, c FROM base");
    e.exec("CREATE MATERIALIZED VIEW m (x) AS SELECT a, b, c FROM base");
    for name in ["v", "m"] {
        assert_eq!(e.columns(&format!("SELECT * FROM {name}")), ["x", "b", "c"]);
        match e.exec(&format!("SUBSCRIBE TO {name}")) {
            ExecResult::Subscribe { columns, cancel, .. } => {
                assert_eq!(columns.into_iter().map(|c| c.name).collect::<Vec<_>>(), ["x", "b", "c"], "SUBSCRIBE TO {name}");
                cancel.store(true, std::sync::atomic::Ordering::Release);
            }
            other => panic!("expected a subscription, got {other:?}"),
        }
    }
    e.shutdown();
}

/// Definitions stored by earlier versions may contain a parameter
/// placeholder (it binds as NULL). Wherever the engine plans a stored
/// definition it must accept one, as recovery does, instead of rejecting it
/// like a new statement: the web UI showed such a view as broken, EXPLAIN
/// failed, replacing a view it reads dropped it, and replacing any other
/// materialized view was refused.
#[test]
fn legacy_definitions_with_placeholders() {
    let e = Engine::start("legacy-placeholders");
    e.exec("CREATE TABLE t (a int PRIMARY KEY, b int)");
    e.exec("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    e.exec("CREATE VIEW pv AS SELECT a, b FROM t");
    // statements as an earlier version logged them
    for sql in ["CREATE MATERIALIZED VIEW lv AS SELECT a FROM pv WHERE b = $1 OR a > 1", "CREATE VIEW lpv AS SELECT a FROM t WHERE a > $1 OR a = 1"] {
        e.handle.log(LogRecord::Ddl { req: 0, sql: sql.into(), extra: String::new() }).unwrap();
    }
    assert_eq!(e.rows("SELECT a FROM lv ORDER BY a"), ["2", "3"]);
    assert_eq!(e.rows("SELECT a FROM lpv"), ["1"]);
    // new statements still reject placeholders
    assert_eq!(e.err_pair("CREATE VIEW nv AS SELECT a FROM t WHERE a > $1").0, "42P02");
    assert!(!e.rows("EXPLAIN PLAN FOR VIEW lv").is_empty());
    match e.handle.inspect_wait(InspectQuery::Object { name: "lv".into(), sample: 0 }, Duration::from_secs(10)).unwrap() {
        InspectReply::Object(d) => {
            assert_eq!(d.problems, Vec::<String>::new());
            assert!(d.plan.is_some());
        }
        other => panic!("unexpected reply {other:?}"),
    }
    // replacing a view it reads re-plans it; replacing another one checks
    // that it still plans
    e.exec("CREATE OR REPLACE VIEW pv AS SELECT a, b + 1 AS b FROM t");
    e.exec("CREATE MATERIALIZED VIEW other AS SELECT a FROM t");
    e.exec("CREATE OR REPLACE MATERIALIZED VIEW other AS SELECT a, b FROM t");
    e.exec("INSERT INTO t VALUES (4, 40)");
    assert_eq!(e.rows("SELECT a FROM lv ORDER BY a"), ["2", "3", "4"]);
    // replayed from the log, then recovered from a checkpoint
    let e = e.restart();
    assert_eq!(e.rows("SELECT a FROM lv ORDER BY a"), ["2", "3", "4"]);
    e.exec("CHECKPOINT");
    let e = e.restart();
    assert_eq!(e.rows("SELECT a FROM lv ORDER BY a"), ["2", "3", "4"]);
    assert_eq!(e.rows("SELECT a FROM lpv"), ["1"]);
    e.shutdown();
}

/// The catalog history keeps the most recent coord::MAX_HISTORY statements
/// (it is part of every checkpoint, and grew without bound).
#[test]
fn the_catalog_history_is_bounded() {
    let e = Engine::start("history-bound");
    let max = ivm::coord::MAX_HISTORY;
    // two statements each, past the trimming point
    let last = (max + max / 10).div_ceil(2);
    for i in 0..=last {
        e.exec(&format!("CREATE TABLE t{i} (a int)"));
        e.exec(&format!("DROP TABLE t{i}"));
    }
    let n: usize = e.rows("SELECT count(*) FROM ivm.catalog_history")[0].parse().unwrap();
    assert!(n <= max + max / 10 && n >= max, "{n} statements");
    let newest = e.rows("SELECT statement FROM ivm.catalog_history ORDER BY gsn DESC LIMIT 1");
    assert_eq!(newest, [format!("DROP TABLE t{last}")]);
    let e = e.restart();
    let after: usize = e.rows("SELECT count(*) FROM ivm.catalog_history")[0].parse().unwrap();
    assert_eq!(after, n);
    e.shutdown();
}
