//! Long operations do not stop the engine: views are hydrated off the
//! engine thread while records go on being applied and requests answered.
//! Measured in counts of things that happened meanwhile (records applied,
//! statements answered), not in wall-clock time.

use ivm::coord::api::{ExecResult, SessionVars, SubEvent};
use ivm::coord::{self, EngineHandle};
use ivm::types::text::format_datum;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

fn tmpdir(name: &str) -> PathBuf {
    let base = std::env::var("IVM_TEST_TMP").unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().into_owned());
    let d = PathBuf::from(base).join(format!("ivm-stall-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn start(dir: &Path) -> coord::Running {
    let cfg = coord::Config { data_dir: dir.to_path_buf(), sync: false, tick: None, retention_gsns: 100_000, ..Default::default() };
    coord::start(cfg).expect("engine starts")
}

fn try_exec(h: &EngineHandle, sql: &str) -> Result<ExecResult, String> {
    let stmt = ivm::sql::parse_statement(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    h.execute(stmt, vec![], SessionVars::default(), String::new()).map_err(|e| format!("{sql}: {e}"))
}

fn exec(h: &EngineHandle, sql: &str) -> ExecResult {
    try_exec(h, sql).unwrap_or_else(|e| panic!("{e}"))
}

/// Result rows as text, sorted.
fn rows(h: &EngineHandle, sql: &str) -> Vec<String> {
    match exec(h, sql) {
        ExecResult::Query { rows, .. } => {
            let mut v: Vec<String> = rows.iter().map(|r| r.iter().map(format_datum).collect::<Vec<_>>().join("|")).collect();
            v.sort();
            v
        }
        other => panic!("{sql}: expected rows, got {other:?}"),
    }
}

/// Rows in table `big` (enough that hydrating the view below takes a
/// while: much longer than the handful of statements run meanwhile).
const BIG: i64 = 300_000;

/// A view whose hydration is slow: a join through an index built for it,
/// and an aggregation.
const SLOW_VIEW: &str = "SELECT a.g, count(*) AS n, sum(b.x) AS s FROM big a JOIN big b ON a.x = b.id GROUP BY a.g";

fn load_big(h: &EngineHandle) {
    exec(h, "CREATE TABLE big (id bigint PRIMARY KEY, x bigint, g int)");
    let chunk = 50_000;
    for lo in (0..BIG).step_by(chunk) {
        let hi = (lo + chunk as i64).min(BIG) - 1;
        exec(h, &format!("INSERT INTO big SELECT i, (i::bigint * 7919) % {BIG}, i % 97 FROM generate_series({lo}, {hi}) i"));
    }
    exec(h, "CREATE TABLE small (id int PRIMARY KEY, v int)");
}

/// Is the view in the catalog (its DDL record applied)? Answered by the
/// engine thread, so only while it is not busy hydrating.
fn listed(h: &EngineHandle, view: &str) -> bool {
    rows(h, "SHOW MATERIALIZED VIEWS").iter().any(|r| r.starts_with(&format!("{view}|")))
}

/// Run `sql` on a thread, returning when its DDL record has been applied
/// (the view is listed) — before it is done.
fn start_ddl(h: &EngineHandle, sql: &str, view: &str) -> std::thread::JoinHandle<Result<ExecResult, String>> {
    let (h2, sql) = (h.clone(), sql.to_string());
    let t = std::thread::spawn(move || try_exec(&h2, &sql));
    let t0 = Instant::now();
    while !listed(h, view) {
        assert!(t0.elapsed() < Duration::from_secs(60), "{view} never listed");
        std::thread::sleep(Duration::from_millis(1));
    }
    t
}

/// While a large view hydrates, single-row commits to another table are
/// applied (their statements answered, which happens once applied), reads
/// are answered, and the statement creating the view is still running
/// after all of that. (The view's hydration used to run inline when its
/// DDL record was applied, stopping every other view, commit and read.)
#[test]
fn commits_are_applied_while_a_view_hydrates() {
    let dir = tmpdir("hydrate-commits");
    let running = start(&dir);
    let h = running.handle.clone();
    load_big(&h);
    let create = start_ddl(&h, &format!("CREATE MATERIALIZED VIEW slow AS {SLOW_VIEW}"), "slow");
    for i in 0..20 {
        exec(&h, &format!("INSERT INTO small VALUES ({i}, {i})"));
        assert_eq!(rows(&h, "SELECT count(*) FROM small"), vec![(i + 1).to_string()]);
    }
    assert!(!create.is_finished(), "the view was done before 20 single-row commits were applied (they waited for it?)");
    create.join().unwrap().unwrap();
    let want = rows(&h, SLOW_VIEW);
    assert_eq!(want.len(), 97);
    assert_eq!(rows(&h, "SELECT * FROM slow"), want);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// The GSN the engine has applied.
fn gsn(h: &EngineHandle) -> u64 {
    rows(h, "SHOW GSN")[0].parse().unwrap()
}

/// Requests that read a view still being hydrated wait for it (they would
/// otherwise read it empty); commits that do not read it do not wait; DDL
/// logged meanwhile is applied after it. Views created over it, an index
/// on it and a subscription to it all see its full contents, and reads as
/// of GSNs applied while it was hydrating see the contents then (its
/// history starts at its DDL's GSN).
#[test]
fn requests_reading_a_hydrating_view_wait_for_it() {
    let dir = tmpdir("hydrate-reads");
    let running = start(&dir);
    let h = running.handle.clone();
    load_big(&h);
    exec(&h, "CREATE TABLE counts (k int PRIMARY KEY, n bigint)");
    let create = start_ddl(&h, &format!("CREATE MATERIALIZED VIEW slow AS {SLOW_VIEW}"), "slow");
    let mut threads = Vec::new();
    let spawn = |sql: &str| {
        let (h2, sql) = (h.clone(), sql.to_string());
        std::thread::spawn(move || match try_exec(&h2, &sql) {
            Ok(ExecResult::Query { rows, .. }) => rows.iter().map(|r| r.iter().map(format_datum).collect::<Vec<_>>().join("|")).collect(),
            Ok(ExecResult::Command { tag }) => vec![tag],
            Ok(ExecResult::Subscribe { rx, cancel, .. }) => {
                // the snapshot, up to the first progress mark
                let mut n = 0i64;
                for ev in rx.iter() {
                    match ev {
                        SubEvent::Data { rows, .. } => n += rows.iter().map(|(_, w)| w).sum::<i64>(),
                        SubEvent::Progress { .. } => break,
                        SubEvent::Error(e) => panic!("{e}"),
                    }
                }
                cancel.store(true, std::sync::atomic::Ordering::Release);
                vec![format!("subscribed {n}")]
            }
            Err(e) => vec![format!("error: {e}")],
        })
    };
    threads.push(spawn("SELECT count(*), sum(n) FROM slow"));
    threads.push(spawn("INSERT INTO counts SELECT 1, count(*) FROM slow"));
    threads.push(spawn("SUBSCRIBE slow"));
    // a record applied while it hydrates
    let g0 = gsn(&h);
    exec(&h, "INSERT INTO big VALUES (-1, 0, 1000)");
    let g1 = gsn(&h);
    assert!(g1 > g0);
    assert!(!create.is_finished(), "the view was done before a commit was applied (it waited for it?)");
    // DDL behind the hydration (and the records after it) waits for it
    threads.push(spawn("CREATE MATERIALIZED VIEW over_slow AS SELECT sum(n) AS total FROM slow"));
    threads.push(spawn("CREATE INDEX slow_n ON slow (n)"));
    let results: Vec<Vec<String>> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    create.join().unwrap().unwrap();
    let n = (BIG + 1).to_string();
    assert_eq!(results[0], vec![format!("98|{n}")], "read of the hydrating view");
    assert_eq!(results[1], vec!["INSERT 0 1".to_string()]);
    assert_eq!(rows(&h, "SELECT n FROM counts"), vec!["98"]);
    assert_eq!(results[2], vec!["subscribed 98".to_string()]);
    assert_eq!(results[3], vec!["CREATE MATERIALIZED VIEW".to_string()]);
    assert_eq!(rows(&h, "SELECT total FROM over_slow"), vec![n.clone()]);
    assert_eq!(results[4], vec!["CREATE INDEX".to_string()]);
    assert_eq!(rows(&h, "SELECT count(*) FROM slow WHERE n = 1"), vec!["1"]);
    // as of a GSN applied while it was hydrating
    assert_eq!(rows(&h, &format!("SELECT sum(n) FROM slow AS OF {g0}")), vec![BIG.to_string()]);
    assert_eq!(rows(&h, &format!("SELECT sum(n) FROM slow AS OF {g1}")), vec![n]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A crash while a view is being hydrated (after its DDL record, with
/// commits to its tables and a checkpoint meanwhile) recovers to the state
/// an uninterrupted run reaches: recovery replays the DDL record and
/// hydrates inline. Also the view's error records survive (a checkpoint
/// taken while it hydrates does not save its error counts: recovery takes
/// them from its root, as a new view does).
#[test]
fn a_crash_during_a_hydration_recovers_the_same_state() {
    let view = "SELECT a.g, count(*) AS n, sum(b.x) AS s, sum(1 / (b.x - 7)) AS e FROM big a JOIN big b ON a.x = b.id GROUP BY a.g";
    let mut states = Vec::new();
    for crash in [false, true] {
        let dir = tmpdir(&format!("hydrate-crash-{crash}"));
        let running = start(&dir);
        let h = running.handle.clone();
        load_big(&h);
        let create = start_ddl(&h, &format!("CREATE MATERIALIZED VIEW v AS {view}"), "v");
        for i in 0..10 {
            exec(&h, &format!("INSERT INTO big VALUES ({}, {i}, {i})", -1 - i));
            exec(&h, &format!("UPDATE big SET x = x + 1 WHERE id = {}", i * 1000));
            exec(&h, &format!("INSERT INTO small VALUES ({i}, {i})"));
            if i == 5 {
                exec(&h, "CHECKPOINT");
            }
        }
        let running = if crash {
            assert!(!create.is_finished(), "the view was done before the crash: make it slower");
            running.crash();
            let _ = create.join();
            start(&dir)
        } else {
            create.join().unwrap().unwrap();
            running
        };
        let h = running.handle.clone();
        // (reads of the view fail on its division by zero, as the query does)
        let err = try_exec(&h, "SELECT * FROM v").expect_err("the view has an error record");
        assert!(err.contains("division by zero"), "{err}");
        assert!(try_exec(&h, view).is_err());
        exec(&h, "DELETE FROM big WHERE x = 7");
        let state = (rows(&h, "SELECT * FROM v"), rows(&h, "SELECT * FROM small"), rows(&h, "SELECT count(*), sum(x) FROM big"));
        assert_eq!(state.0, rows(&h, view), "the view equals its query");
        states.push(state);
        running.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }
    assert_eq!(states[0], states[1]);
}

/// A subscription to a query whose dataflow is hydrated in the background
/// starts as of the GSN it was requested at: its snapshot and the changes
/// after it (commits applied while it hydrated included) add up to the
/// query's result.
#[test]
fn subscriptions_to_queries_add_up_while_they_hydrate() {
    let dir = tmpdir("hydrate-subscribe");
    let running = start(&dir);
    let h = running.handle.clone();
    load_big(&h);
    let q = "SELECT a.g, count(*) AS n FROM big a JOIN big b ON a.x = b.id WHERE b.g < 50 GROUP BY a.g";
    let h2 = h.clone();
    let sub = std::thread::spawn(move || exec(&h2, &format!("SUBSCRIBE ({q}) WITH (PROGRESS)")));
    let t0 = Instant::now();
    let mut i = 0;
    while !sub.is_finished() {
        exec(&h, &format!("INSERT INTO big VALUES ({}, {}, {})", -1 - i, i * 3, i % 7));
        i += 1;
        assert!(t0.elapsed() < Duration::from_secs(120));
    }
    let ExecResult::Subscribe { rx, cancel, .. } = sub.join().unwrap() else { panic!("not a subscription") };
    exec(&h, "INSERT INTO big VALUES (-100000000, 5, 3)");
    let last = gsn(&h);
    let mut acc: std::collections::BTreeMap<String, i64> = Default::default();
    let mut progress = 0;
    for ev in rx.iter() {
        match ev {
            SubEvent::Data { rows, .. } => {
                for (r, w) in rows {
                    *acc.entry(r.iter().map(format_datum).collect::<Vec<_>>().join("|")).or_default() += w;
                }
            }
            SubEvent::Progress { gsn } => {
                assert!(gsn > progress || progress == 0, "progress {gsn} after {progress}");
                progress = gsn;
                if gsn >= last {
                    break;
                }
            }
            SubEvent::Error(e) => panic!("{e}"),
        }
    }
    cancel.store(true, std::sync::atomic::Ordering::Release);
    let mut got: Vec<String> = acc.into_iter().filter(|(_, w)| *w != 0).map(|(r, w)| {
        assert_eq!(w, 1, "{r}");
        r
    }).collect();
    got.sort();
    assert!(i > 0, "no commit while the subscription hydrated");
    assert_eq!(got, rows(&h, q));
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
