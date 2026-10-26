//! Long operations do not stop the engine: views are hydrated, and
//! checkpoints written, off the engine thread while records go on being
//! applied and requests answered; DML statements share log appends.
//! Measured in counts of things that happened meanwhile (records applied,
//! statements answered), not in wall-clock time.

use ivm::coord::api::{ExecResult, SessionVars, SubEvent};
use ivm::coord::{self, EngineHandle};
use ivm::types::text::format_datum;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// These tests measure what happens meanwhile: one at a time, so that they
/// do not slow each other down unevenly.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

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
    let _serial = serial();
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
/// logged meanwhile is applied after it. Reads, DML and a subscription
/// issued while it hydrates, views created over it and an index on it all
/// see its full contents, including a commit applied meanwhile.
#[test]
fn requests_reading_a_hydrating_view_wait_for_it() {
    let _serial = serial();
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
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// DDL logged while a view is being hydrated does not hold up the records
/// after it: new tables and views are created, and commits after them are
/// applied, before the hydration is done; a view created meanwhile over
/// the one being hydrated is hydrated next. Only DDL that replaces a view
/// (or reads a relation's contents at once, as a sink's start) waits.
#[test]
fn ddl_during_a_hydration_does_not_hold_up_commits() {
    let _serial = serial();
    let dir = tmpdir("hydrate-ddl");
    let running = start(&dir);
    let h = running.handle.clone();
    load_big(&h);
    let create = start_ddl(&h, &format!("CREATE MATERIALIZED VIEW slow AS {SLOW_VIEW}"), "slow");
    exec(&h, "CREATE TABLE t2 (id int PRIMARY KEY, v int)");
    // (new views are hydrated after the one in progress: their statements
    // return then)
    let t2_sum = start_ddl(&h, "CREATE MATERIALIZED VIEW t2_sum AS SELECT sum(v) AS s FROM t2", "t2_sum");
    let over = start_ddl(&h, "CREATE MATERIALIZED VIEW over_slow AS SELECT sum(n) AS total FROM slow", "over_slow");
    for i in 0..10 {
        exec(&h, &format!("INSERT INTO t2 VALUES ({i}, {i})"));
    }
    assert_eq!(rows(&h, "SELECT sum(v) FROM t2"), vec!["45"]);
    assert!(!create.is_finished(), "the view was done before the commits after DDL logged meanwhile were applied");
    create.join().unwrap().unwrap();
    t2_sum.join().unwrap().unwrap();
    over.join().unwrap().unwrap();
    assert_eq!(rows(&h, "SELECT s FROM t2_sum"), vec!["45"]);
    assert_eq!(rows(&h, "SELECT total FROM over_slow"), vec![BIG.to_string()]);
    // a replacement waits for the hydration in progress
    let create = start_ddl(&h, "CREATE MATERIALIZED VIEW slow2 AS SELECT a.g, count(*) AS n FROM big a JOIN big b ON a.x = b.id GROUP BY a.g", "slow2");
    exec(&h, "CREATE OR REPLACE MATERIALIZED VIEW t2_sum AS SELECT sum(v) + 1 AS s FROM t2");
    assert_eq!(rows(&h, "SELECT s FROM t2_sum"), vec!["46"]);
    create.join().unwrap().unwrap();
    assert_eq!(rows(&h, "SELECT count(*) FROM slow2"), vec!["97"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Replacing a view hydrates the new version off the engine thread too:
/// commits are applied and answered meanwhile, and a subscription to the
/// view gets the change from the old to the new contents and then the new
/// version's changes, adding up to the new contents.
#[test]
fn replacing_a_view_does_not_hold_up_commits() {
    let _serial = serial();
    let dir = tmpdir("replace");
    let running = start(&dir);
    let h = running.handle.clone();
    load_big(&h);
    exec(&h, &format!("CREATE MATERIALIZED VIEW slow AS {SLOW_VIEW}"));
    let ExecResult::Subscribe { rx, cancel, .. } = exec(&h, "SUBSCRIBE slow WITH (PROGRESS)") else { panic!("not a subscription") };
    // (another join: a key computed by an index of its own)
    let replacement = "SELECT a.g, count(*) AS n, sum(b.x) AS s FROM big a JOIN big b ON a.x = b.id + 0 WHERE b.g < 50 GROUP BY a.g";
    let (h2, sql) = (h.clone(), format!("CREATE OR REPLACE MATERIALIZED VIEW slow AS {replacement}"));
    let replace = std::thread::spawn(move || {
        let t0 = Instant::now();
        try_exec(&h2, &sql).unwrap();
        t0.elapsed()
    });
    let (mut i, mut slowest) = (0, Duration::ZERO);
    while !replace.is_finished() {
        let t0 = Instant::now();
        exec(&h, &format!("INSERT INTO big VALUES ({}, {}, {})", -1 - i, i * 13, i % 5));
        i += 1;
        if !replace.is_finished() {
            slowest = slowest.max(t0.elapsed());
        }
    }
    let took = replace.join().unwrap();
    assert!(i >= 3, "{i} commits answered while the view was replaced");
    assert!(slowest * 3 < took, "a commit took {slowest:?} of the replacement's {took:?}");
    exec(&h, "INSERT INTO big VALUES (-1000000, 3, 3)");
    let last = gsn(&h);
    let mut acc: std::collections::BTreeMap<String, i64> = Default::default();
    let mut progress = 0;
    for ev in rx.iter() {
        match ev {
            SubEvent::Data { gsn, rows } => {
                assert!(gsn > progress || gsn == progress && progress > 0, "data at {gsn} after progress {progress}");
                for (r, w) in rows {
                    *acc.entry(r.iter().map(format_datum).collect::<Vec<_>>().join("|")).or_default() += w;
                }
            }
            SubEvent::Progress { gsn } => {
                assert!(gsn >= progress, "progress {gsn} after {progress}");
                progress = gsn;
                if gsn >= last {
                    break;
                }
            }
            SubEvent::Error(e) => panic!("{e}"),
        }
    }
    cancel.store(true, std::sync::atomic::Ordering::Release);
    let got: Vec<String> = acc.into_iter().filter(|(_, w)| *w != 0).map(|(r, _)| r).collect();
    assert_eq!(got, rows(&h, replacement));
    assert_eq!(rows(&h, "SELECT * FROM slow"), rows(&h, replacement));
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// An upstream schema change rebuilds the table, from its old version's
/// rows, and re-plans the views over it off the engine thread: commits to
/// other tables are applied meanwhile, reads of the table wait for it, and
/// no checkpoint is taken until it is rebuilt (one asked for meanwhile
/// waits): its rows are in neither version's index alone. A crash in the
/// middle recovers it from the log.
#[test]
fn schema_changes_rebuild_tables_off_the_engine_thread() {
    let _serial = serial();
    for crash in [false, true] {
        let dir = tmpdir(&format!("schema-{crash}"));
        let running = start(&dir);
        let h = running.handle.clone();
        load_big(&h);
        exec(&h, "CREATE MATERIALIZED VIEW sv AS SELECT a.g, count(*) AS n, sum(b.x) AS s FROM big a JOIN big b ON a.x = b.id GROUP BY a.g");
        let oid: u64 = rows(&h, "SELECT oid FROM ivm.objects WHERE name = 'big'")[0].parse().unwrap();
        use ivm::types::{Column, ColumnType, SqlType};
        let col = |n: &str, ty: SqlType, nullable: bool| Column::new(n, ColumnType { nullable, ..ColumnType::new(ty) });
        let columns = vec![col("id", SqlType::Int8, false), col("x", SqlType::Int8, true), col("g", SqlType::Int4, true), col("y", SqlType::Int4, true)];
        h.log(ivm::log::LogRecord::Schema { source: 0, table: oid - 16384, columns, pk: vec![0] }).unwrap();
        let (h2, h3) = (h.clone(), h.clone());
        // (an error when the engine crashes first)
        let read = std::thread::spawn(move || try_exec(&h2, "SELECT count(*), count(y) FROM big").map(|_| ()));
        let ck = std::thread::spawn(move || h3.checkpoint());
        let mut i = 0;
        while !read.is_finished() && i < 20 {
            exec(&h, &format!("INSERT INTO small VALUES ({i}, {i})"));
            i += 1;
        }
        assert!(!read.is_finished(), "the table was rebuilt before 20 commits were applied");
        let running = if crash {
            running.crash();
            let _ = (read.join(), ck.join());
            start(&dir)
        } else {
            read.join().unwrap().unwrap();
            ck.join().unwrap().unwrap();
            running
        };
        let h = running.handle.clone();
        assert_eq!(rows(&h, "SELECT count(*), count(y) FROM big"), vec![format!("{BIG}|0")]);
        assert_eq!(rows(&h, "SELECT * FROM sv"), rows(&h, "SELECT a.g, count(*) AS n, sum(b.x) AS s FROM big a JOIN big b ON a.x = b.id GROUP BY a.g"));
        assert_eq!(rows(&h, "SELECT count(*) FROM small"), vec![i.to_string()]);
        running.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// A crash while a view is being hydrated (after its DDL record, with
/// commits to its tables and a checkpoint meanwhile) recovers to the state
/// an uninterrupted run reaches: recovery replays the DDL record and
/// hydrates inline. Also the view's error records survive (a checkpoint
/// taken while it hydrates does not save its error counts: recovery takes
/// them from its root, as a new view does).
#[test]
fn a_crash_during_a_hydration_recovers_the_same_state() {
    let _serial = serial();
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
    let _serial = serial();
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

/// While a large checkpoint is written, single-row commits are applied and
/// answered: commits with GSNs after the checkpoint's are answered before
/// it is, and none waits for a good part of it. (The checkpoint used to be
/// written on the engine thread, which applied nothing meanwhile: the first
/// commit after it began waited for all of it.)
#[test]
fn commits_are_applied_while_a_checkpoint_is_written() {
    let _serial = serial();
    let dir = tmpdir("checkpoint-commits");
    // (no checkpoint before the one asked for: it writes everything loaded)
    let cfg = coord::Config {
        data_dir: dir.clone(),
        sync: false,
        tick: None,
        checkpoint_interval: Duration::from_secs(3600),
        ..Default::default()
    };
    let running = coord::start(cfg).unwrap();
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE wide (id int PRIMARY KEY, pad text)");
    for lo in (0..600_000).step_by(100_000) {
        exec(&h, &format!("INSERT INTO wide SELECT i, repeat('x', 200) || i FROM generate_series({lo}, {}) i", lo + 99_999));
    }
    exec(&h, "CREATE TABLE small (id int PRIMARY KEY, v int)");
    // (the inserts are the only records: their GSNs follow this one)
    let g0 = gsn(&h);
    let h2 = h.clone();
    let ck = std::thread::spawn(move || {
        let t0 = Instant::now();
        let g = h2.checkpoint().unwrap();
        (g, t0.elapsed())
    });
    let (mut during, mut slowest) = (Vec::new(), Duration::ZERO);
    let mut i = 0;
    while !ck.is_finished() {
        let t0 = Instant::now();
        exec(&h, &format!("INSERT INTO small VALUES ({i}, {i})"));
        i += 1;
        if !ck.is_finished() {
            during.push(g0 + i);
            slowest = slowest.max(t0.elapsed());
        }
    }
    let (c, took) = ck.join().unwrap();
    assert_eq!(gsn(&h), g0 + i);
    let after = during.iter().filter(|g| **g > c).count();
    assert!(after >= 3, "{after} commits after the checkpoint's GSN {c} answered while it was written ({} in all)", during.len());
    assert!(slowest * 3 < took, "a commit took {slowest:?} of the checkpoint's {took:?}");
    // the next one (with nothing written meanwhile) has everything
    assert!(h.checkpoint().unwrap() >= *during.last().unwrap());
    running.shutdown();
    let running = start(&dir);
    assert_eq!(rows(&running.handle, "SELECT count(*) FROM small"), vec![i.to_string()]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Run `n` statements from each of `clients` threads at once (`sql(client,
/// i)`), returning the results in order per client.
fn concurrently(h: &EngineHandle, clients: usize, n: usize, sql: fn(usize, usize) -> String) -> Vec<Vec<Result<ExecResult, String>>> {
    let threads: Vec<_> = (0..clients)
        .map(|c| {
            let h = h.clone();
            std::thread::spawn(move || (0..n).map(|i| try_exec(&h, &sql(c, i))).collect::<Vec<_>>())
        })
        .collect();
    threads.into_iter().map(|t| t.join().unwrap()).collect()
}

/// Concurrent single-row DML statements share log appends (and their
/// fsyncs): each one is logged as soon as it is evaluated unless it read
/// rows a statement logged before it writes, instead of waiting for the
/// statement before it to be applied. (Each used to cost an append and an
/// fsync of its own.) Results stay those of running them one after the
/// other: no lost update, no duplicate key, every statement's own result.
#[test]
fn concurrent_dml_shares_log_appends() {
    let _serial = serial();
    let dir = tmpdir("group-commit");
    let cfg = coord::Config { data_dir: dir.clone(), sync: true, tick: None, ..Default::default() };
    let running = coord::start(cfg).unwrap();
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE d (client int, i int, PRIMARY KEY (client, i))");
    exec(&h, "CREATE TABLE acct (id int PRIMARY KEY, bal bigint)");
    exec(&h, "INSERT INTO acct SELECT g, 0 FROM generate_series(0, 9) g");
    exec(&h, "CREATE MATERIALIZED VIEW per_client AS SELECT client, count(*) AS n FROM d GROUP BY client");
    let batches = |h: &EngineHandle| h.shared.log_batches.load(std::sync::atomic::Ordering::Relaxed);
    let (clients, n) = (16, 200);
    // inserts of distinct rows: nothing to wait for
    let b0 = batches(&h);
    let res = concurrently(&h, clients, n, |c, i| format!("INSERT INTO d VALUES ({c}, {i})"));
    let inserts = batches(&h) - b0;
    for r in res.iter().flatten() {
        assert!(matches!(r, Ok(ExecResult::Command { tag }) if tag == "INSERT 0 1"), "{r:?}");
    }
    let statements = (clients * n) as u64;
    assert!(inserts * 2 <= statements, "{inserts} log appends for {statements} concurrent inserts");
    assert_eq!(rows(&h, "SELECT count(*), count(DISTINCT client) FROM d"), vec![format!("{statements}|{clients}")]);
    assert_eq!(rows(&h, "SELECT DISTINCT n FROM per_client"), vec![n.to_string()]);
    // updates of a few hot rows: each reads what the one before wrote
    let res = concurrently(&h, clients, 50, |c, i| format!("UPDATE acct SET bal = bal + 1 WHERE id = {}", (c * 7 + i * 3) % 10));
    for r in res.iter().flatten() {
        assert!(matches!(r, Ok(ExecResult::Command { tag }) if tag == "UPDATE 1"), "{r:?}");
    }
    let mut want = [0i64; 10];
    for c in 0..clients {
        for i in 0..50 {
            want[(c * 7 + i * 3) % 10] += 1;
        }
    }
    let want: Vec<String> = (0..10).map(|k| format!("{k}|{}", want[k])).collect();
    assert_eq!(rows(&h, "SELECT id, bal FROM acct"), want);
    // the same key from every client: exactly one insert succeeds
    let res = concurrently(&h, clients, 1, |_, _| "INSERT INTO d VALUES (-1, -1)".to_string());
    let ok = res.iter().flatten().filter(|r| r.is_ok()).count();
    assert_eq!(ok, 1, "{res:?}");
    assert!(res.iter().flatten().filter_map(|r| r.as_ref().err()).all(|e| e.contains("duplicate key")));
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Large ad hoc reads run off the engine thread (on a fork of the storage
/// taken when they exceed their budget there): commits keep being applied
/// and answered while they run, none waiting for a good part of them, and
/// each returns the table as of one GSN. (Six queued `LIMIT 1` scans used
/// to hold up a single-row insert for seconds.)
#[test]
fn large_reads_do_not_hold_up_commits() {
    let _serial = serial();
    let dir = tmpdir("reads");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE wide (id int PRIMARY KEY, g int, pad text)");
    for lo in (0..600_000).step_by(100_000) {
        exec(&h, &format!("INSERT INTO wide SELECT i, i % 7, repeat('x', 50) FROM generate_series({lo}, {}) i", lo + 99_999));
    }
    exec(&h, "CREATE TABLE small (id int PRIMARY KEY, v int)");
    let g0 = gsn(&h);
    let reads: Vec<_> = (0..6)
        .map(|k| {
            let h = h.clone();
            std::thread::spawn(move || {
                let t0 = Instant::now();
                let q = format!("SELECT g, count(*), min(pad), max(id) FROM wide WHERE id % 6 <> {k} GROUP BY g ORDER BY g LIMIT 1");
                (rows(&h, &q), t0.elapsed())
            })
        })
        .collect();
    let (mut during, mut slowest) = (0, Duration::ZERO);
    let mut i = 0;
    while reads.iter().any(|r| !r.is_finished()) {
        let t0 = Instant::now();
        exec(&h, &format!("INSERT INTO wide VALUES ({}, 0, 'y')", 1_000_000 + i));
        i += 1;
        if reads.iter().any(|r| !r.is_finished()) {
            during += 1;
            slowest = slowest.max(t0.elapsed());
        }
    }
    let results: Vec<(Vec<String>, Duration)> = reads.into_iter().map(|t| t.join().unwrap()).collect();
    let longest = results.iter().map(|r| r.1).max().unwrap();
    assert!(during >= 3, "{during} commits answered while the reads ran");
    assert!(slowest * 3 < longest, "a commit took {slowest:?} of the reads' {longest:?}");
    // each read saw the table as of some GSN between g0 and the end: all of
    // the 600,000 rows, plus some of the inserts of group 0
    for (k, (r, _)) in results.iter().enumerate() {
        let base = (0..600_000).filter(|x| x % 7 == 0 && x % 6 != k).count();
        let n: usize = r[0].split('|').nth(1).unwrap().parse().unwrap();
        assert!(r[0].starts_with("0|") && n >= base && n <= base + i as usize, "read {k}: {r:?} (base {base}, {i} inserts)");
    }
    assert_eq!(gsn(&h), g0 + i);
    let sum = (0..600_000i64).map(|x| x % 7).sum::<i64>();
    assert_eq!(rows(&h, "SELECT count(*), sum(g) FROM wide"), vec![format!("{}|{sum}", 600_000 + i)]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// The snapshot of a large view that a subscription starts with is taken
/// off the engine thread: commits are applied and answered meanwhile, and
/// the snapshot (as of the GSN the subscription started at) and the changes
/// after it add up to the view's contents. (A sink's starting snapshot is
/// taken the same way.)
#[test]
fn large_subscription_snapshots_do_not_hold_up_commits() {
    let _serial = serial();
    let dir = tmpdir("subscribe-snapshot");
    let running = start(&dir);
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE wide (id int PRIMARY KEY, g int, pad text)");
    for lo in (0..600_000).step_by(100_000) {
        exec(&h, &format!("INSERT INTO wide SELECT i, i % 7, repeat('x', 50) FROM generate_series({lo}, {}) i", lo + 99_999));
    }
    exec(&h, "CREATE MATERIALIZED VIEW wv AS SELECT id, g FROM wide WHERE g < 5");
    let h2 = h.clone();
    let sub = std::thread::spawn(move || {
        let t0 = Instant::now();
        let r = exec(&h2, "SUBSCRIBE wv WITH (PROGRESS)");
        (r, t0.elapsed())
    });
    let (mut i, mut slowest) = (0, Duration::ZERO);
    while !sub.is_finished() {
        let t0 = Instant::now();
        exec(&h, &format!("INSERT INTO wide VALUES ({}, {}, 'y')", 1_000_000 + i, i % 7));
        i += 1;
        if !sub.is_finished() {
            slowest = slowest.max(t0.elapsed());
        }
    }
    let (r, took) = sub.join().unwrap();
    let ExecResult::Subscribe { rx, cancel, .. } = r else { panic!("not a subscription") };
    assert!(i >= 3, "{i} commits answered while the snapshot was taken");
    assert!(slowest * 3 < took, "a commit took {slowest:?} of the snapshot's {took:?}");
    exec(&h, "INSERT INTO wide VALUES (-1, 1, 'z')");
    let last = gsn(&h);
    let mut n = 0i64;
    for ev in rx.iter() {
        match ev {
            SubEvent::Data { rows, .. } => n += rows.iter().map(|(_, w)| w).sum::<i64>(),
            SubEvent::Progress { gsn } if gsn >= last => break,
            SubEvent::Progress { .. } => {}
            SubEvent::Error(e) => panic!("{e}"),
        }
    }
    cancel.store(true, std::sync::atomic::Ordering::Release);
    assert_eq!(vec![n.to_string()], rows(&h, "SELECT count(*) FROM wv"));
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A small deterministic generator (splitmix64).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// DDL, DML from several sessions, large and small reads, a subscription and
/// checkpoints, all at once, with the hydrations, background checkpoints,
/// shared log appends and off-thread reads they cause overlapping: every
/// statement gets a proper result, and afterwards — also after a crash and
/// recovery — every view equals its query and the subscription adds up to
/// its view.
#[test]
fn concurrent_ddl_dml_reads_and_checkpoints_stay_consistent() {
    let _serial = serial();
    let dir = tmpdir("concurrent");
    let cfg = coord::Config {
        data_dir: dir.clone(),
        sync: false,
        tick: None,
        checkpoint_interval: Duration::from_millis(300),
        ..Default::default()
    };
    let running = coord::start(cfg.clone()).unwrap();
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE a (id int PRIMARY KEY, x int, g int)");
    exec(&h, "CREATE TABLE b (id int PRIMARY KEY, y int, k int)");
    exec(&h, "INSERT INTO a SELECT i, (i * 37) % 20000, i % 13 FROM generate_series(0, 59999) i");
    exec(&h, "INSERT INTO b SELECT i, i % 101, i % 13 FROM generate_series(0, 19999) i");
    let defs = [
        "SELECT g, count(*) AS n, sum(x) AS s FROM a GROUP BY g",
        "SELECT a.g, count(*) AS n, sum(b.y) AS s FROM a JOIN b ON a.x = b.id GROUP BY a.g",
        "SELECT b.k AS g, count(*) AS n, max(a.x) AS s FROM a JOIN b ON a.g = b.k WHERE b.y < 3 GROUP BY b.k",
        "SELECT g, count(DISTINCT x) AS n, min(x) AS s FROM a WHERE x % 5 = 0 GROUP BY g",
    ];
    exec(&h, &format!("CREATE MATERIALIZED VIEW stable AS {}", defs[1]));
    let ExecResult::Subscribe { rx, cancel, .. } = exec(&h, "SUBSCRIBE stable WITH (PROGRESS)") else { panic!("not a subscription") };
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let fatal = |e: &str| e.contains("internal") || e.contains("shutting down") || e.contains("I/O");
    let mut threads = Vec::new();
    for c in 0..4u64 {
        let (h, stop) = (h.clone(), stop.clone());
        threads.push(std::thread::spawn(move || {
            let mut rng = Rng(c + 1);
            while !stop.load(std::sync::atomic::Ordering::Acquire) {
                let id = rng.below(70_000);
                let sql = match rng.below(6) {
                    0 => format!("INSERT INTO a VALUES ({id}, {}, {})", rng.below(20_000), rng.below(13)),
                    1 => format!("UPDATE a SET x = x + 1 WHERE id = {id}"),
                    2 => format!("DELETE FROM a WHERE id = {id}"),
                    3 => format!("INSERT INTO b VALUES ({}, {}, {})", id % 25_000, rng.below(101), rng.below(13)),
                    4 => format!("UPDATE b SET y = (y + 1) % 101 WHERE id = {}", id % 25_000),
                    _ => format!("UPDATE a SET g = (g + 1) % 13 WHERE id % 997 = {}", rng.below(997)),
                };
                if let Err(e) = try_exec(&h, &sql) {
                    assert!(e.contains("duplicate key"), "{e}");
                }
            }
        }));
    }
    {
        let (h, stop) = (h.clone(), stop.clone());
        threads.push(std::thread::spawn(move || {
            let mut rng = Rng(99);
            while !stop.load(std::sync::atomic::Ordering::Acquire) {
                let sql = match rng.below(4) {
                    0 => "SELECT g, count(*) FROM a GROUP BY g".to_string(),
                    1 => format!("SELECT * FROM a WHERE id = {}", rng.below(60_000)),
                    2 => format!("SELECT * FROM v{} LIMIT 3", rng.below(6)),
                    _ => "SELECT count(*), sum(n) FROM stable".to_string(),
                };
                if let Err(e) = try_exec(&h, &sql) {
                    assert!(e.contains("does not exist") && !fatal(&e), "{e}");
                }
            }
        }));
    }
    // DDL from this thread, over the DML and reads: views (also over views,
    // which replacements re-plan or drop), indexes, drops, checkpoints
    let mut rng = Rng(7);
    let mut views: std::collections::BTreeMap<String, String> = Default::default();
    for step in 0..40 {
        let name = format!("v{}", rng.below(6));
        let def = match rng.below(4) {
            0 => format!("SELECT g, sum(n) AS n, count(*) AS s FROM v{} GROUP BY g", rng.below(6)),
            _ => defs[rng.below(defs.len() as u64) as usize].to_string(),
        };
        let sql = match rng.below(5) {
            0 | 1 => format!("CREATE OR REPLACE MATERIALIZED VIEW {name} AS {def}"),
            2 => format!("DROP MATERIALIZED VIEW IF EXISTS {name} CASCADE"),
            3 => format!("CREATE INDEX IF NOT EXISTS {name}_n ON {name} (n)"),
            _ => "CHECKPOINT".to_string(),
        };
        match try_exec(&h, &sql) {
            Ok(_) if sql.starts_with("CREATE OR REPLACE") => {
                views.insert(name, def);
            }
            Ok(_) => {}
            // (a view reading itself, a replacement its dependents reject)
            Err(e) => assert!(!fatal(&e), "step {step}: {e}"),
        }
    }
    stop.store(true, std::sync::atomic::Ordering::Release);
    for t in threads {
        t.join().unwrap();
    }
    // (the views that exist: drops and re-planning removed some)
    let check = |h: &EngineHandle, views: &std::collections::BTreeMap<String, String>| {
        assert_eq!(rows(h, "SELECT * FROM stable"), rows(h, defs[1]));
        for (name, def) in views {
            if listed(h, name) {
                assert_eq!(rows(h, &format!("SELECT * FROM {name}")), rows(h, def), "view {name}");
            }
        }
    };
    check(&h, &views);
    // the subscription adds up to its view
    exec(&h, "INSERT INTO b VALUES (-1, 0, 0)");
    let last = gsn(&h);
    let mut acc: std::collections::BTreeMap<String, i64> = Default::default();
    for ev in rx.iter() {
        match ev {
            SubEvent::Data { rows, .. } => {
                for (r, w) in rows {
                    *acc.entry(r.iter().map(format_datum).collect::<Vec<_>>().join("|")).or_default() += w;
                }
            }
            SubEvent::Progress { gsn } if gsn >= last => break,
            SubEvent::Progress { .. } => {}
            SubEvent::Error(e) => panic!("{e}"),
        }
    }
    cancel.store(true, std::sync::atomic::Ordering::Release);
    let got: Vec<String> = acc.into_iter().filter(|(_, w)| *w != 0).map(|(r, _)| r).collect();
    assert_eq!(got, rows(&h, defs[1]));
    running.crash();
    let running = coord::start(cfg.clone()).unwrap();
    check(&running.handle, &views);
    running.shutdown();
    let running = coord::start(cfg).unwrap();
    check(&running.handle, &views);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
