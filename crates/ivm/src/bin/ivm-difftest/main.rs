//! Differential testing against a real PostgreSQL.
//!
//! Starts a private PostgreSQL cluster and an engine, replicates randomly
//! generated tables through logical replication, creates random
//! materialized views, applies random transactions upstream, and after each
//! round compares every view with PostgreSQL recomputing the same query.
//!
//! usage: ivm-difftest [--seed N] [--views N] [--rounds N] [--restarts N] [--kill] [--keep]
//!
//! - `--restarts N`: restart the engine N times at random points of the run
//!   (a clean shutdown, or a crash that skips the final checkpoint so that
//!   recovery replays the log) while replication continues upstream.
//! - `--kill`: run the engine as a child process (the `ivm` binary next to
//!   this one) and SIGKILL it for the restarts, at random moments: while it
//!   applies changes, during a checkpoint, or during the initial snapshot.
//! - `DIFFTEST_VERBOSE=1` prints the views and details of every mismatch.
//! - `DIFFTEST_DUMP_DIR=dir` writes the upstream tables as INSERT statements
//!   before every round (`dir/round-N.sql`), to replay a failing round.
//! - `DIFFTEST_EXTRA_VIEWS='q1;;q2'` adds views (e0, e1, ...) that are
//!   compared like the generated ones, without changing the seed's random
//!   choices: to narrow down a mismatch with simplified variants of a query.
//!
//! The schema, query and change generators (sqlgen.rs) are shared with the
//! engine tests (tests/engine.rs), which compare views with ad hoc queries
//! without PostgreSQL.

mod sqlgen;
#[path = "../../testkit.rs"]
mod testkit;

use sqlgen::Gen;
use ivm::coord::{self, Config};
use ivm::server::{self, ServerConfig};
use pgproto::testutil::{ClusterOptions, TempCluster};
use pgproto::{Client, Config as PgConfig};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use testkit::Rng;

fn connect(conninfo: &str) -> Client {
    let cfg = PgConfig::parse(conninfo).expect("conninfo");
    Client::connect(&cfg).expect("connect")
}

fn rows(c: &mut Client, sql: &str) -> Result<(Vec<u32>, Vec<Vec<Option<String>>>), String> {
    match c.query(sql) {
        Ok(r) => Ok((r.columns.iter().map(|c| c.type_oid).collect(), r.rows)),
        Err(e) => Err(e.message),
    }
}

/// Normalize a result for comparison (sorted; floats rounded).
fn normalize(types: &[u32], rows: Vec<Vec<Option<String>>>) -> Vec<String> {
    normalize_with(types, rows, false)
}

/// `trim_numeric`: compare numerics by value ("1.0" = "1"). Where SQL
/// deduplicates equal values (GROUP BY, DISTINCT, set operations, MIN/MAX),
/// PostgreSQL keeps whichever representation it met first, so only the
/// value is defined there.
fn normalize_with(types: &[u32], mut rows: Vec<Vec<Option<String>>>, trim_numeric: bool) -> Vec<String> {
    for r in rows.iter_mut() {
        for (i, v) in r.iter_mut().enumerate() {
            if let Some(s) = v
                && matches!(types.get(i), Some(700) | Some(701))
                && let Ok(f) = s.parse::<f64>()
            {
                *s = if f == 0.0 { "0".into() } else { format!("{:.9e}", f) };
            }
            if let Some(s) = v
                && trim_numeric
                && types.get(i) == Some(&1700)
                && s.contains('.')
            {
                let t = s.trim_end_matches('0').trim_end_matches('.');
                *s = if t == "-0" || t.is_empty() { "0".into() } else { t.to_string() };
            }
        }
    }
    let mut out: Vec<String> =
        rows.into_iter().map(|r| r.into_iter().map(|v| v.unwrap_or_else(|| "∅".into())).collect::<Vec<_>>().join("|")).collect();
    out.sort();
    out
}

/// The engine under test: in this process, or a child process to SIGKILL.
enum Engine {
    InProcess { running: Option<coord::Running>, data: PathBuf },
    Child { child: std::process::Child, bin: PathBuf, data: PathBuf, port: u16 },
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

impl Engine {
    fn start(kill: bool, data: &Path) -> (Engine, Client) {
        if kill {
            let bin = std::env::current_exe().unwrap().with_file_name("ivm");
            if !bin.exists() {
                eprintln!("--kill needs the ivm binary at {} (cargo build --release)", bin.display());
                std::process::exit(2);
            }
            Engine::spawn(bin, data.to_path_buf())
        } else {
            let running = coord::start(Config { data_dir: data.to_path_buf(), sync: false, ..Default::default() }).expect("engine");
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            let h = running.handle.clone();
            std::thread::spawn(move || {
                let _ = server::serve_listener(listener, ServerConfig { listen: String::new(), password: None }, h);
            });
            let client = connect(&format!("host=127.0.0.1 port={port} user=ivm dbname=ivm"));
            (Engine::InProcess { running: Some(running), data: data.to_path_buf() }, client)
        }
    }

    fn spawn(bin: PathBuf, data: PathBuf) -> (Engine, Client) {
        let port = free_port();
        let child = std::process::Command::new(&bin)
            .args(["--data-dir", data.to_str().unwrap(), "--listen", &format!("127.0.0.1:{port}"), "--no-sync"])
            .args(["--tick-ms", "0", "--checkpoint-secs", "1"])
            .env("RUST_LOG", std::env::var("RUST_LOG").unwrap_or_else(|_| "warn".into()))
            .spawn()
            .expect("spawn ivm");
        let cfg = PgConfig::parse(&format!("host=127.0.0.1 port={port} user=ivm dbname=ivm")).unwrap();
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            match Client::connect(&cfg) {
                Ok(c) => return (Engine::Child { child, bin, data, port }, c),
                Err(e) if Instant::now() > deadline => panic!("ivm did not come up on port {port}: {e}"),
                Err(_) => std::thread::sleep(Duration::from_millis(20)),
            }
        }
    }

    /// Stop (cleanly, or as a crash) and start again; returns a new client.
    fn restart(&mut self, crash: bool) -> Client {
        match self {
            Engine::InProcess { running, data } => {
                let r = running.take().unwrap();
                if crash { r.crash() } else { r.shutdown() }
                let (e, c) = Engine::start(false, data);
                *self = e;
                c
            }
            Engine::Child { child, bin, data, .. } => {
                child.kill().expect("kill ivm");
                let _ = child.wait();
                let (e, c) = Engine::spawn(bin.clone(), data.clone());
                *self = e;
                c
            }
        }
    }

    /// A separate connection (to run a statement the engine is killed in).
    fn port(&self) -> Option<u16> {
        match self {
            Engine::Child { port, .. } => Some(*port),
            Engine::InProcess { .. } => None,
        }
    }

    fn stop(self) {
        match self {
            Engine::InProcess { running, .. } => running.unwrap().shutdown(),
            Engine::Child { mut child, .. } => {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

struct Args {
    seed: u64,
    views: usize,
    rounds: usize,
    restarts: usize,
    kill: bool,
    keep: bool,
}

fn parse_args() -> Args {
    let mut a = Args { seed: 42, views: 25, rounds: 20, restarts: 0, kill: false, keep: false };
    let mut it = std::env::args().skip(1);
    let usage = || -> ! {
        eprintln!("usage: ivm-difftest [--seed N] [--views N] [--rounds N] [--restarts N] [--kill] [--keep]");
        std::process::exit(2)
    };
    while let Some(arg) = it.next() {
        let mut num = || it.next().and_then(|v| v.parse::<u64>().ok()).unwrap_or_else(|| usage());
        match arg.as_str() {
            "--seed" => a.seed = num(),
            "--views" => a.views = num() as usize,
            "--rounds" => a.rounds = num() as usize,
            "--restarts" => a.restarts = num() as usize,
            "--kill" => a.kill = true,
            "--keep" => a.keep = true,
            _ => usage(),
        }
    }
    if a.kill && a.restarts == 0 {
        a.restarts = 3;
    }
    a
}

fn main() {
    let args = parse_args();
    let seed = args.seed;
    let verbose = std::env::var("DIFFTEST_VERBOSE").is_ok();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();
    let mut rng = Rng::new(seed);
    let pg = TempCluster::start(ClusterOptions { extra_conf: vec!["timezone = 'UTC'".into()], keep: args.keep, ..Default::default() })
        .expect("start postgres");
    let pg_conn = pg.conninfo("postgres");
    let mut up = connect(&pg_conn);

    let data = std::env::temp_dir().join(format!("ivm-difftest-{}-{seed}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data);
    let (mut engine, mut ivm) = Engine::start(args.kill, &data);
    // restarts happen in random rounds, at random moments of the round
    let mut restart_rounds: Vec<usize> = (0..args.restarts).map(|_| 1 + rng.below(args.rounds.max(1) as u64) as usize).collect();
    restart_rounds.sort();
    let mut restarts_done = 0;

    // schema + initial data
    let mut tables = sqlgen::gen_schema(&mut rng);
    for t in &tables {
        if verbose {
            println!("{};", sqlgen::create_table_sql(t));
        }
        up.execute(&sqlgen::create_table_sql(t)).unwrap();
        for s in sqlgen::toast_sql(t) {
            up.execute(&s).unwrap();
        }
    }
    for ti in 0..tables.len() {
        for _ in 0..(30 + rng.below(80)) {
            let sql = sqlgen::insert_sql(&mut rng, &mut tables, ti);
            up.simple_query(&sql).unwrap();
        }
    }
    up.execute("CREATE PUBLICATION difftest FOR ALL TABLES").unwrap();
    ivm.execute(&format!("CREATE SOURCE src FROM POSTGRES CONNECTION '{pg_conn}' PUBLICATION 'difftest'")).expect("create source");
    if args.kill && args.restarts > 0 && rng.chance(30) {
        // killed during the initial snapshot
        std::thread::sleep(Duration::from_millis(rng.below(200)));
        println!("killing the engine during the initial snapshot");
        ivm = engine.restart(true);
    }

    // views
    let mut views: Vec<(String, String)> = Vec::new();
    let mut unsupported = 0;
    let mut attempts = 0;
    while views.len() < args.views && attempts < args.views * 20 {
        attempts += 1;
        let q = Gen { rng: &mut rng, tables: &tables }.query();
        if rows(&mut up, &q).is_err() {
            continue; // invalid in PostgreSQL too
        }
        let name = format!("v{}", views.len());
        match ivm.execute(&format!("CREATE MATERIALIZED VIEW {name} AS {q}")) {
            Ok(_) => {
                if verbose {
                    println!("{name}: {q}");
                }
                views.push((name, q))
            }
            Err(e) => {
                unsupported += 1;
                println!("unsupported: {} :: {q}", e.message);
            }
        }
    }
    // views over views (in the engine they read the view instead of inlining it)
    let mut reads: std::collections::HashMap<String, String> = Default::default();
    let base = views.len();
    for k in 0..(base / 4) {
        let (src_name, src_q) = views[rng.below(base as u64) as usize].clone();
        let (q, ivm_q) = sqlgen::view_over_view(&mut rng, &src_name, &src_q);
        if rows(&mut up, &q).is_err() {
            continue;
        }
        let name = format!("vv{k}");
        match ivm.execute(&format!("CREATE MATERIALIZED VIEW {name} AS {ivm_q}")) {
            Ok(_) => {
                if verbose {
                    println!("{name}: {ivm_q}");
                }
                reads.insert(name.clone(), src_name.clone());
                views.push((name, q))
            }
            Err(e) => {
                unsupported += 1;
                println!("unsupported: {} :: {ivm_q}", e.message);
            }
        }
    }
    if let Ok(extra) = std::env::var("DIFFTEST_EXTRA_VIEWS") {
        for (k, q) in extra.split(";;").map(str::trim).filter(|q| !q.is_empty()).enumerate() {
            let name = format!("e{k}");
            match ivm.execute(&format!("CREATE MATERIALIZED VIEW {name} AS {q}")) {
                Ok(_) => views.push((name, q.to_string())),
                Err(e) => println!("extra view {name} rejected: {}", e.message),
            }
        }
    }
    println!("created {} views ({unsupported} rejected)", views.len());

    let mut failures = 0;
    for round in 0..=args.rounds {
        let restart_now = restarts_done < restart_rounds.len() && restart_rounds[restarts_done] == round;
        // when in the round: 0 before the changes, 1 right after them, 2
        // during a checkpoint (child process only)
        let when = if restart_now { rng.below(if engine.port().is_some() { 3 } else { 2 }) } else { u64::MAX };
        let crash = restart_now && (args.kill || rng.chance(70));
        if when == 0 {
            println!("round {round}: {} the engine", if crash { "crashing" } else { "restarting" });
            ivm = engine.restart(crash);
            restarts_done += 1;
        }
        if let Ok(dir) = std::env::var("DIFFTEST_DUMP_DIR") {
            let mut out = String::new();
            for t in &tables {
                for r in up.query(&format!("SELECT * FROM {} ORDER BY id", t.name)).unwrap().rows {
                    let vals: Vec<String> = r.iter().map(|v| v.as_ref().map(|s| format!("'{}'", s.replace('\'', "''"))).unwrap_or_else(|| "NULL".into())).collect();
                    out.push_str(&format!("INSERT INTO {} VALUES ({});\n", t.name, vals.join(", ")));
                }
            }
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(format!("{dir}/round-{round}.sql"), out).unwrap();
        }
        if round > 0 {
            for _ in 0..(1 + rng.below(4)) {
                let stmts = sqlgen::transaction(&mut rng, &mut tables, false);
                let sql = format!("BEGIN;{}COMMIT;", stmts.concat());
                if verbose {
                    println!("round {round} upstream: {sql}");
                }
                if let Err(e) = up.simple_query(&sql) {
                    let _ = up.simple_query("ROLLBACK");
                    if verbose {
                        println!("upstream batch failed (ignored): {}", e.message);
                    }
                }
            }
            if rng.chance(20) {
                let _ = ivm.execute("CHECKPOINT");
            }
        }
        if when == 1 {
            // replication of the changes is in flight
            std::thread::sleep(Duration::from_millis(rng.below(30)));
            println!("round {round}: {} the engine after the changes", if crash { "crashing" } else { "restarting" });
            ivm = engine.restart(crash);
            restarts_done += 1;
        } else if when == 2 {
            let port = engine.port().unwrap();
            let t = std::thread::spawn(move || {
                if let Ok(mut c) = Client::connect(&PgConfig::parse(&format!("host=127.0.0.1 port={port} user=ivm dbname=ivm")).unwrap()) {
                    let _ = c.execute("CHECKPOINT");
                }
            });
            std::thread::sleep(Duration::from_millis(rng.below(20)));
            println!("round {round}: killing the engine during a checkpoint");
            ivm = engine.restart(true);
            let _ = t.join();
            restarts_done += 1;
        }
        let lsn = up.query("SELECT pg_current_wal_lsn()").unwrap().rows[0][0].clone().unwrap();
        let t0 = Instant::now();
        let mut total_rows = 0;
        let mut both_err = 0;
        let mut matched = 0;
        for (name, q) in &views {
            // A view over a failing view misses the rows of the failure,
            // while the inlined query need not compute the failing column
            // (engine test views_hydrated_from_failing_nodes_report_their_errors).
            if let Some(src) = reads.get(name)
                && rows(&mut ivm, &format!("SELECT * FROM {src} AS OF AT LEAST LSN '{lsn}'")).is_err()
            {
                if verbose {
                    println!("note round {round} view {name}: not compared, it reads failing view {src}");
                }
                matched += 1;
                continue;
            }
            let expect = rows(&mut up, q);
            let got = rows(&mut ivm, &format!("SELECT * FROM {name} AS OF AT LEAST LSN '{lsn}'"));
            match (expect, got) {
                (Ok((et, er)), Ok((_, gr))) => {
                    let (a, b) = (normalize(&et, er.clone()), normalize(&et, gr.clone()));
                    total_rows += a.len();
                    let same_values = a != b && sqlgen::dedups(q) && normalize_with(&et, er, true) == normalize_with(&et, gr, true);
                    if same_values && verbose {
                        println!("note round {round} view {name}: equal values in other representations (PostgreSQL keeps the first it meets)");
                    }
                    if a == b || same_values {
                        matched += 1;
                    } else {
                        failures += 1;
                        println!("MISMATCH round {round} view {name}: {q}");
                        let only_pg: Vec<&String> = a.iter().filter(|x| !b.contains(x)).take(5).collect();
                        let only_ivm: Vec<&String> = b.iter().filter(|x| !a.contains(x)).take(5).collect();
                        println!("  postgres rows {} / ivm rows {}", a.len(), b.len());
                        println!("  only in postgres: {only_pg:?}");
                        println!("  only in ivm:      {only_ivm:?}");
                        if only_pg.is_empty() && only_ivm.is_empty() {
                            // same values, different multiplicities
                            println!("  postgres: {a:?}");
                            println!("  ivm:      {b:?}");
                        }
                        // the same query run ad hoc (not incrementally) in ivm
                        match rows(&mut ivm, &format!("{q} AS OF AT LEAST LSN '{lsn}'")) {
                            Ok((_, r)) if normalize(&et, r.clone()) == a => println!("  ivm ad hoc agrees with postgres: a maintenance bug"),
                            Ok((_, r)) if normalize(&et, r.clone()) == b => println!("  ivm ad hoc agrees with the view: a query semantics bug"),
                            Ok(_) => println!("  ivm ad hoc agrees with neither"),
                            Err(e) => println!("  ivm ad hoc fails: {e}"),
                        }
                    }
                }
                (Err(_), Err(_)) => both_err += 1,
                (Err(e), Ok(_)) => println!("round {round} {name}: postgres errors ({e}) but ivm returned rows: {q}"),
                (Ok(_), Err(e)) => {
                    failures += 1;
                    println!("ERROR round {round} {name}: ivm failed: {e} :: {q}");
                }
            }
        }
        println!("round {round}: {matched}/{} views match ({total_rows} rows, {both_err} both-error) in {:?}", views.len(), t0.elapsed());
        if failures > 0 && round > 0 {
            break;
        }
    }
    engine.stop();
    if !args.keep {
        let _ = std::fs::remove_dir_all(&data);
    }
    drop(pg);
    if failures > 0 {
        println!("FAILED with {failures} mismatches (seed {seed})");
        std::process::exit(1);
    }
    println!("OK (seed {seed})");
}
