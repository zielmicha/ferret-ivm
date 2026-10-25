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
