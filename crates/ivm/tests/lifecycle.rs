//! Lifecycle of requests and background work through the public API (no
//! PostgreSQL needed): reads that wait, subscriptions, sources and sinks
//! that cannot connect.

use crossbeam_channel::Receiver;
use ivm::coord::api::{ExecResult, SessionVars};
use ivm::coord::{self, EngineHandle};
use ivm::error::SqlError;
use std::path::PathBuf;
use std::time::Duration;

fn tmpdir(name: &str) -> PathBuf {
    let base = std::env::var("IVM_TEST_TMP").unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().into_owned());
    let d = PathBuf::from(base).join(format!("ivm-lifecycle-{name}-{}", std::process::id()));
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
        let cfg = coord::Config { data_dir: dir.clone(), sync: false, tick: None, ..Default::default() };
        let running = coord::start(cfg).expect("engine starts");
        let h = running.handle.clone();
        Engine { running, dir, h }
    }

    fn shutdown(self) {
        self.running.shutdown();
        let _ = std::fs::remove_dir_all(&self.dir);
    }

    /// Run a statement on another thread (with logged external input
    /// `extra`, as a session passes the tables CREATE SOURCE discovered).
    fn spawn_with(&self, sql: &str, vars: SessionVars, extra: &str) -> Receiver<Result<ExecResult, SqlError>> {
        let stmt = ivm::sql::parse_statement(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        let (h, extra) = (self.h.clone(), extra.to_string());
        let (tx, rx) = crossbeam_channel::bounded(1);
        std::thread::spawn(move || {
            let _ = tx.send(h.execute(stmt, vec![], vars, extra));
        });
        rx
    }

    fn spawn(&self, sql: &str) -> Receiver<Result<ExecResult, SqlError>> {
        self.spawn_with(sql, SessionVars::default(), "")
    }

    /// The result of a statement, which must arrive within `secs`.
    fn within(&self, sql: &str, secs: u64) -> Result<ExecResult, SqlError> {
        self.spawn(sql).recv_timeout(Duration::from_secs(secs)).unwrap_or_else(|_| panic!("{sql}: no reply within {secs}s"))
    }

    fn exec(&self, sql: &str) -> ExecResult {
        self.within(sql, 30).unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    /// CREATE SOURCE without contacting the upstream: no tables discovered.
    fn create_source(&self, name: &str, conninfo: &str) {
        let sql = format!("CREATE SOURCE {name} FROM POSTGRES CONNECTION '{conninfo}' PUBLICATION 'p'");
        let r = self.spawn_with(&sql, SessionVars::default(), "[]").recv_timeout(Duration::from_secs(30)).expect("CREATE SOURCE returns");
        r.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

/// Assert that a statement failed with `code` and a message containing `text`.
fn assert_err(r: Result<ExecResult, SqlError>, code: &str, text: &str) {
    match r {
        Ok(r) => panic!("expected an error, got {r:?}"),
        Err(e) => assert!(e.code == code && e.message.contains(text), "{} {}", e.code, e.message),
    }
}

/// AS OF AT LEAST LSN waits for every source to reach the LSN; without any
/// source it fails at once (it used to wait forever), also when the last
/// source is dropped while the read waits.
#[test]
fn at_least_lsn_without_sources() {
    let e = Engine::start("lsn-no-source");
    let read = "SELECT 1 AS OF AT LEAST LSN '0/16B3748'";
    assert_err(e.within(read, 10), "55000", "cannot be satisfied: there is no source");
    // a source that never reports a frontier (nothing listens there)
    e.create_source("s", "host=127.0.0.1 port=1 connect_timeout=1");
    let waiting = e.spawn(read);
    assert!(waiting.recv_timeout(Duration::from_millis(500)).is_err(), "the read waits for the source");
    e.exec("DROP SOURCE s");
    assert_err(waiting.recv_timeout(Duration::from_secs(10)).expect("the read ends"), "55000", "there is no source");
    e.shutdown();
}

/// The events of a new subscription.
fn subscribe(e: &Engine, sql: &str) -> Receiver<ivm::coord::api::SubEvent> {
    match e.exec(sql) {
        ExecResult::Subscribe { rx, .. } => rx,
        other => panic!("{sql}: expected a subscription, got {other:?}"),
    }
}

/// A subscription's events until `until` holds for one (or a timeout):
/// data rows as "diff:row", errors as "error: message".
fn events_until(rx: &Receiver<ivm::coord::api::SubEvent>, until: impl Fn(&str) -> bool) -> Vec<String> {
    use ivm::coord::api::SubEvent;
    let mut out = Vec::new();
    while let Ok(ev) = rx.recv_timeout(Duration::from_secs(5)) {
        let evs = match ev {
            SubEvent::Data { rows, .. } => rows
                .iter()
                .map(|(r, w)| format!("{w}:{}", r.iter().map(ivm::types::text::format_datum).collect::<Vec<_>>().join("|")))
                .collect(),
            SubEvent::Progress { .. } => vec![],
            SubEvent::Error(e) => vec![format!("error: {}", e.message)],
        };
        for ev in evs {
            let done = until(&ev);
            out.push(ev);
            if done {
                return out;
            }
        }
    }
    out.push("(timeout)".into());
    out
}

/// A subscription ends with an error when a relation it reads is dropped
/// (it used to wait forever for a table, a view or a query's relations),
/// and goes on when an error record of its view is retracted (it used to
/// end).
#[test]
fn subscription_lifecycle() {
    let e = Engine::start("subscriptions");
    e.exec("CREATE TABLE t (id int PRIMARY KEY, x int)");
    e.exec("INSERT INTO t VALUES (1, 5), (2, 0)");
    e.exec("CREATE MATERIALIZED VIEW v AS SELECT id, 10 / x AS q FROM t");
    let rx = subscribe(&e, "SUBSCRIBE TO v");
    e.exec("DELETE FROM t WHERE id = 2");
    e.exec("INSERT INTO t VALUES (3, 1)");
    assert_eq!(events_until(&rx, |ev| ev.starts_with("1:3|")), ["1:1|2", "1:3|10"]);
    e.exec("INSERT INTO t VALUES (4, 0)");
    assert_eq!(events_until(&rx, |ev| ev.starts_with("error")), ["error: division by zero"]);

    e.exec("CREATE TABLE a (id int PRIMARY KEY)");
    e.exec("CREATE VIEW av AS SELECT id FROM a");
    e.exec("CREATE MATERIALIZED VIEW am AS SELECT id FROM a");
    let subs: Vec<_> = ["SUBSCRIBE TO a", "SUBSCRIBE TO av", "SUBSCRIBE TO (SELECT count(*) FROM av)", "SUBSCRIBE TO am"]
        .into_iter()
        .map(|sql| subscribe(&e, sql))
        .collect();
    e.exec("DROP MATERIALIZED VIEW am");
    assert_eq!(events_until(&subs[3], |ev| ev.starts_with("error")), ["error: materialized view \"am\" was dropped"]);
    e.exec("DROP VIEW av");
    assert_eq!(events_until(&subs[1], |ev| ev.starts_with("error")), ["error: view \"av\" was dropped"]);
    assert_eq!(events_until(&subs[2], |ev| ev.starts_with("error")), ["1:0", "error: view \"av\" was dropped"]);
    e.exec("DROP TABLE a");
    assert_eq!(events_until(&subs[0], |ev| ev.starts_with("error")), ["error: table \"a\" was dropped"]);
    e.shutdown();
}

/// A server that accepts connections and never answers: a sink or source
/// connecting to it waits in its connection attempt (no connect_timeout).
fn silent_server() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        let mut open = Vec::new();
        for c in listener.incoming().flatten() {
            open.push(c);
        }
    });
    port
}

/// DROP SINK returns while the sink thread is stuck connecting (the engine
/// thread used to wait for it, and froze), and so does a shutdown.
#[test]
fn stuck_sinks_and_sources_do_not_block_the_engine() {
    let port = silent_server();
    let e = Engine::start("stuck-threads");
    e.exec("CREATE TABLE t (id int PRIMARY KEY)");
    e.exec(&format!("CREATE SINK s FROM t INTO POSTGRES CONNECTION 'host=127.0.0.1 port={port}' TABLE 'target'"));
    std::thread::sleep(Duration::from_millis(300));
    e.within("DROP SINK s", 5).unwrap();
    e.within("INSERT INTO t VALUES (1)", 5).unwrap();
    // one left running at shutdown
    e.exec(&format!("CREATE SINK s2 FROM t INTO POSTGRES CONNECTION 'host=127.0.0.1 port={port}' TABLE 'target'"));
    std::thread::sleep(Duration::from_millis(300));
    let (tx, rx) = crossbeam_channel::bounded(1);
    std::thread::spawn(move || {
        e.shutdown();
        let _ = tx.send(());
    });
    rx.recv_timeout(Duration::from_secs(20)).expect("the engine stops");
}
