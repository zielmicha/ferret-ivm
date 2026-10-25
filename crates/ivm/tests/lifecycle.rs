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
        Engine::start_with(name, |_| {})
    }

    fn start_with(name: &str, configure: impl FnOnce(&mut coord::Config)) -> Engine {
        let dir = tmpdir(name);
        let mut cfg = coord::Config { data_dir: dir.clone(), sync: false, tick: None, ..Default::default() };
        configure(&mut cfg);
        let running = coord::start(cfg).expect("engine starts");
        let h = running.handle.clone();
        Engine { running, dir, h }
    }

    fn stats(&self) -> ivm::coord::inspect::EngineStats {
        match self.h.inspect_wait(ivm::coord::inspect::InspectQuery::Stats, Duration::from_secs(10)).unwrap() {
            ivm::coord::inspect::InspectReply::Stats(s) => s,
            other => panic!("unexpected reply {other:?}"),
        }
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

/// A subscription to a query has a dataflow built on the current versions
/// of the materialized views it reads: when one of them (directly, through
/// a virtual view or through another materialized view) is replaced, the
/// subscription ends. It used to go on with the view's old definition.
/// A subscription to the view itself follows the replacement.
#[test]
fn query_subscriptions_end_when_a_view_they_read_is_replaced() {
    let e = Engine::start("query-sub-replaced");
    e.exec("CREATE TABLE t (id int PRIMARY KEY, x int)");
    e.exec("INSERT INTO t VALUES (1, 1), (2, 2)");
    e.exec("CREATE MATERIALIZED VIEW m AS SELECT id, x FROM t WHERE x > 0");
    e.exec("CREATE MATERIALIZED VIEW m2 AS SELECT count(*) AS n FROM m");
    e.exec("CREATE VIEW vm AS SELECT id FROM m");
    let subs: Vec<_> = [
        "SUBSCRIBE TO (SELECT id FROM m WHERE id < 10)",
        "SUBSCRIBE TO (SELECT count(*) FROM vm)",
        "SUBSCRIBE TO (SELECT n + 0 FROM m2)",
        "SUBSCRIBE TO m",
    ]
    .into_iter()
    .map(|sql| subscribe(&e, sql))
    .collect();
    e.exec("CREATE OR REPLACE MATERIALIZED VIEW m AS SELECT id, x FROM t WHERE x > 1");
    for (i, rx) in subs[..3].iter().enumerate() {
        let evs = events_until(rx, |ev| ev.starts_with("error"));
        assert_eq!(evs.last().map(|s| s.as_str()), Some("error: materialized view \"m\" was replaced"), "subscription {i}: {evs:?}");
    }
    e.exec("INSERT INTO t VALUES (3, 3)");
    assert_eq!(events_until(&subs[3], |ev| ev == "1:3|3"), ["1:1|1", "1:2|2", "-1:1|1", "1:3|3"]);
    // a new subscription reads the new version
    let rx = subscribe(&e, "SUBSCRIBE TO (SELECT id FROM m WHERE id < 10)");
    e.exec("INSERT INTO t VALUES (4, 0), (5, 5)");
    assert_eq!(events_until(&rx, |ev| ev == "1:5"), ["1:2", "1:3", "1:5"]);
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

/// DROP SINK and DROP SOURCE return while the sink or source thread is
/// stuck connecting (the engine thread used to wait for it, and froze),
/// and so does a shutdown.
#[test]
fn stuck_sinks_and_sources_do_not_block_the_engine() {
    let port = silent_server();
    let e = Engine::start("stuck-threads");
    e.exec("CREATE TABLE t (id int PRIMARY KEY)");
    e.exec(&format!("CREATE SINK s FROM t INTO POSTGRES CONNECTION 'host=127.0.0.1 port={port}' TABLE 'target'"));
    e.create_source("src", &format!("host=127.0.0.1 port={port}"));
    std::thread::sleep(Duration::from_millis(300));
    e.within("DROP SINK s", 5).unwrap();
    e.within("DROP SOURCE src", 5).unwrap();
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

/// Insert `n` rows of about 1 KB into `t` (id, s), 1000 per statement.
fn insert_kb_rows(e: &Engine, from: i64, n: i64) {
    let mut i = from;
    while i < from + n {
        e.exec(&format!("INSERT INTO t SELECT g, repeat('x', 1000) FROM generate_series({i}, {}) g", i + 999));
        i += 1000;
    }
}

/// A subscription whose client does not read ends with an error once its
/// queued changes reach their limit (a sixteenth of the memory budget, at
/// least 4 MiB), instead of queueing every change for good; dropping the
/// queue releases its memory.
#[test]
fn a_subscription_that_is_not_read_ends_at_its_memory_limit() {
    use ivm::coord::api::SubEvent;
    let e = Engine::start_with("slow-subscriber", |c| c.memory_bytes = 16 << 20);
    e.exec("CREATE TABLE t (id int PRIMARY KEY, s text)");
    let rx = subscribe(&e, "SUBSCRIBE TO t");
    // about 12 MB of changes, three times the limit
    insert_kb_rows(&e, 0, 12_000);
    let st = e.stats();
    assert_eq!(st.subscriptions, 0, "the subscription goes on");
    assert!(st.output_queue_bytes < 5 << 20, "{} bytes queued with a 4 MiB limit", st.output_queue_bytes);
    // the changes up to the limit, then the error
    let (mut rows, mut error) = (0, None);
    while let Ok(ev) = rx.try_recv() {
        match ev {
            SubEvent::Data { rows: r, .. } => {
                assert!(error.is_none(), "changes after the error");
                rows += r.len();
            }
            SubEvent::Progress { .. } => {}
            SubEvent::Error(e) => error = Some((e.code, e.message)),
        }
    }
    let (code, message) = error.expect("the subscription ends with an error");
    assert_eq!(code, "53000");
    assert!(message.contains("does not read changes as fast as they come"), "{message}");
    assert!((1000..12_000).contains(&rows), "{rows} rows before the error");
    drop(rx);
    assert_eq!(e.stats().output_queue_bytes, 0);
    e.shutdown();
}

/// A sink whose target is unreachable stops receiving changes once its
/// unwritten changes reach their limit, instead of keeping them all in
/// memory (it is restarted from a new snapshot once it has written what it
/// holds); dropping it releases them.
#[test]
fn a_sink_that_cannot_write_stops_receiving_changes_at_its_memory_limit() {
    let e = Engine::start_with("slow-sink", |c| c.memory_bytes = 16 << 20);
    e.exec("CREATE TABLE t (id int PRIMARY KEY, s text)");
    e.exec("CREATE SINK s FROM t INTO POSTGRES CONNECTION 'host=127.0.0.1 port=1 connect_timeout=1' TABLE 'target'");
    insert_kb_rows(&e, 0, 12_000);
    let queued = e.stats().output_queue_bytes;
    assert!((1 << 20..5 << 20).contains(&queued), "{queued} bytes queued for the sink with a 4 MiB limit");
    e.exec("DROP SINK s");
    let t0 = std::time::Instant::now();
    while e.stats().output_queue_bytes != 0 {
        assert!(t0.elapsed() < Duration::from_secs(20), "the dropped sink's changes are not released");
        std::thread::sleep(Duration::from_millis(50));
    }
    e.shutdown();
}
