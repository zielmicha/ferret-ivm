//! Reproducible performance benchmark of the engine.
//!
//! Generates a deterministic upstream data set in PostgreSQL, replicates it
//! into an in-process engine (with its pgwire server, driven through the
//! wire protocol like a real client) and measures:
//!
//! - `load`: initial snapshot of `accounts` and `txns` (time, CPU, peak RSS)
//! - `checkpoint`: explicit checkpoints, their time and the on-disk size
//! - `hydrate`: CREATE MATERIALIZED VIEW over the loaded data (by_region,
//!   acct_totals)
//! - `small`: N small upstream transactions (single-row inserts, updates,
//!   deletes) applied while the engine runs (`small_live`), and as many
//!   committed while it was down, caught up after a restart (`small_catchup`)
//! - `recovery`: engine restart from its checkpoint
//! - `large`: one 500k-row INSERT and one 500k-row UPDATE transaction, live
//! - `reads`: point / range / scan SELECTs on views and tables, ad hoc joins
//!   and aggregates over base tables
//! - `subscribe`: SUBSCRIBE snapshot and live delivery over the wire
//!
//! CPU time is per thread (`/proc/self/task/*/schedstat`), grouped by thread
//! name: engine (`ivm-engine`, which owns storage and dataflow), source
//! (pgoutput decoding), sequencer (input log), session (pgwire). Wall times
//! are noisy on a shared machine; compare CPU times across runs.
//!
//! Upstream: by default a private PostgreSQL cluster is started (`PG_BIN`,
//! default /usr/lib/postgresql/17/bin) and the data generated in it. With
//! `--upstream CONNINFO` an existing cluster (wal_level=logical) is used: the
//! data set is generated once into a template database and every run works
//! on a fresh copy of it, so runs start from identical data.
//!
//! ```text
//! cargo build --release
//! ./target/release/ivm-bench                       # 100k accounts, 2M txns
//! ./target/release/ivm-bench --txns 500000 --small 5000 --large 100000
//! ./target/release/ivm-bench --upstream 'host=/var/run/postgresql port=5432 user=postgres dbname=postgres'
//! ./target/release/ivm-bench --skip reads,subscribe --json out.json
//! ```
//!
//! Every measurement is also printed as a `RESULT {...}` JSON line, and its
//! time window (nanoseconds since the start of the process) as a `WINDOW`
//! line, to select it in a profile, e.g. under `gprofng collect app`:
//! `gprofng display text -filters 'TSTAMP>=START && TSTAMP<=END' -functions x.er`.

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

/// mimalloc, counting allocations per thread: allocation counts are a
/// deterministic measure of allocation churn, unlike CPU time on a busy
/// machine.
struct CountingAlloc;

struct AllocSlot {
    tid: AtomicU32,
    count: AtomicU64,
    bytes: AtomicU64,
}

const ALLOC_SLOTS: usize = 4096;
static SLOTS: [AllocSlot; ALLOC_SLOTS] =
    [const { AllocSlot { tid: AtomicU32::new(0), count: AtomicU64::new(0), bytes: AtomicU64::new(0) } }; ALLOC_SLOTS];
static NEXT_SLOT: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static SLOT: std::cell::Cell<usize> = const { std::cell::Cell::new(usize::MAX) };
}

unsafe extern "C" {
    fn gettid() -> i32;
}

fn count_alloc(size: usize) {
    let _ = SLOT.try_with(|s| {
        let mut i = s.get();
        if i == usize::MAX {
            i = NEXT_SLOT.fetch_add(1, Ordering::Relaxed) % ALLOC_SLOTS;
            SLOTS[i].count.store(0, Ordering::Relaxed);
            SLOTS[i].bytes.store(0, Ordering::Relaxed);
            SLOTS[i].tid.store(unsafe { gettid() } as u32, Ordering::Relaxed);
            s.set(i);
        }
        SLOTS[i].count.fetch_add(1, Ordering::Relaxed);
        SLOTS[i].bytes.fetch_add(size as u64, Ordering::Relaxed);
    });
}

unsafe impl std::alloc::GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        count_alloc(layout.size());
        unsafe { mimalloc::MiMalloc.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: std::alloc::Layout) -> *mut u8 {
        count_alloc(layout.size());
        unsafe { mimalloc::MiMalloc.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        unsafe { mimalloc::MiMalloc.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: std::alloc::Layout, new_size: usize) -> *mut u8 {
        count_alloc(new_size);
        unsafe { mimalloc::MiMalloc.realloc(ptr, layout, new_size) }
    }
}

/// Allocations (count, bytes) per thread id so far.
fn thread_allocs() -> HashMap<u32, (u64, u64)> {
    let n = NEXT_SLOT.load(Ordering::Relaxed).min(ALLOC_SLOTS);
    let mut m = HashMap::with_capacity(n);
    for s in &SLOTS[..n] {
        m.insert(s.tid.load(Ordering::Relaxed), (s.count.load(Ordering::Relaxed), s.bytes.load(Ordering::Relaxed)));
    }
    m
}

use ivm::coord::{self, Config};
use ivm::server::{self, ServerConfig};
use pgproto::testutil::{ClusterOptions, TempCluster};
use pgproto::{Client, Config as PgConfig};
use std::collections::{BTreeMap, HashMap};
use std::io::{BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// options

struct Opts {
    accounts: u64,
    txns: u64,
    small: u64,
    large: u64,
    point_reads: u64,
    sub_txns: u64,
    upstream: Option<String>,
    data_dir: Option<PathBuf>,
    memory_mb: usize,
    sync: bool,
    skip: Vec<String>,
    json: Option<PathBuf>,
    keep_data: bool,
    repeat: usize,
}

fn usage() -> ! {
    eprintln!(
        "usage: ivm-bench [--accounts N] [--txns N] [--small N] [--large N] [--point-reads N]\n\
         \x20                [--sub-txns N] [--upstream CONNINFO] [--data-dir DIR] [--memory-mb N]\n\
         \x20                [--no-sync] [--skip PHASE,...] [--json FILE] [--keep-data] [--repeat N]\n\
         phases: small, recovery, large, reads, subscribe (load, checkpoint and hydrate always run)\n\
         --repeat N: run hydration, small and large transactions N times each"
    );
    std::process::exit(2)
}

fn parse_opts() -> Opts {
    let mut o = Opts {
        accounts: 100_000,
        txns: 2_000_000,
        small: 20_000,
        large: 500_000,
        point_reads: 2_000,
        sub_txns: 5_000,
        upstream: None,
        data_dir: None,
        memory_mb: 2048,
        sync: true,
        skip: vec![],
        json: None,
        keep_data: false,
        repeat: 1,
    };
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut val = || args.next().unwrap_or_else(|| usage());
        match a.as_str() {
            "--accounts" => o.accounts = val().parse().unwrap_or_else(|_| usage()),
            "--txns" => o.txns = val().parse().unwrap_or_else(|_| usage()),
            "--small" => o.small = val().parse().unwrap_or_else(|_| usage()),
            "--large" => o.large = val().parse().unwrap_or_else(|_| usage()),
            "--point-reads" => o.point_reads = val().parse().unwrap_or_else(|_| usage()),
            "--sub-txns" => o.sub_txns = val().parse().unwrap_or_else(|_| usage()),
            "--upstream" => o.upstream = Some(val()),
            "--data-dir" => o.data_dir = Some(PathBuf::from(val())),
            "--memory-mb" | "--cache-mb" => o.memory_mb = val().parse().unwrap_or_else(|_| usage()),
            "--no-sync" => o.sync = false,
            "--skip" => o.skip = val().split(',').map(|s| s.trim().to_string()).collect(),
            "--json" => o.json = Some(PathBuf::from(val())),
            "--keep-data" => o.keep_data = true,
            "--repeat" => o.repeat = val().parse::<usize>().unwrap_or_else(|_| usage()).max(1),
            _ => usage(),
        }
    }
    o
}

// ---------------------------------------------------------------------------
// measurement

/// Per thread of this process: (name, CPU time ns, run-queue wait ns).
fn thread_cpu() -> HashMap<u32, (String, u64, u64)> {
    let mut out = HashMap::new();
    let Ok(dir) = std::fs::read_dir("/proc/self/task") else { return out };
    for e in dir.flatten() {
        let Ok(tid) = e.file_name().to_string_lossy().parse::<u32>() else { continue };
        let p = e.path();
        let name = std::fs::read_to_string(p.join("comm")).unwrap_or_default().trim().to_string();
        let (ns, wait) = match std::fs::read_to_string(p.join("schedstat")) {
            Ok(s) => {
                let mut f = s.split_whitespace().map(|v| v.parse::<u64>().unwrap_or(0));
                (f.next().unwrap_or(0), f.next().unwrap_or(0))
            }
            Err(_) => {
                // utime + stime in clock ticks (fields 14, 15)
                let st = std::fs::read_to_string(p.join("stat")).unwrap_or_default();
                (stat_ticks(&st, &[11, 12]) * 10_000_000, 0)
            }
        };
        out.insert(tid, (name, ns, wait));
    }
    out
}

/// Sum of the given fields (0-based, counted after the command name) of a
/// /proc stat line.
fn stat_ticks(st: &str, fields: &[usize]) -> u64 {
    let rest = st.rsplit_once(')').map(|x| x.1).unwrap_or("");
    let f: Vec<&str> = rest.split_whitespace().collect();
    fields.iter().map(|i| f.get(*i).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0)).sum()
}

/// Postmaster of the upstream cluster, when it runs on this machine.
static UPSTREAM_PID: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

/// CPU time of the upstream cluster: the postmaster with its reaped
/// children, plus its live children (walsender, backends).
fn upstream_cpu_s() -> f64 {
    let pm = UPSTREAM_PID.load(Ordering::Relaxed);
    if pm == 0 {
        return 0.0;
    }
    let mut ticks = stat_ticks(&std::fs::read_to_string(format!("/proc/{pm}/stat")).unwrap_or_default(), &[11, 12, 13, 14]);
    if let Ok(rd) = std::fs::read_dir("/proc") {
        for e in rd.flatten() {
            if e.file_name().to_string_lossy().parse::<u32>().is_err() {
                continue;
            }
            let Ok(st) = std::fs::read_to_string(e.path().join("stat")) else { continue };
            if stat_ticks(&st, &[1]) == pm as u64 {
                ticks += stat_ticks(&st, &[11, 12]);
            }
        }
    }
    ticks as f64 / 100.0
}

fn group_of(name: &str) -> &'static str {
    if name.starts_with("ivm-engine") {
        "engine"
    } else if name.starts_with("ivm-sequencer") {
        "sequencer"
    } else if name.starts_with("ivm-source") {
        "source"
    } else if name.starts_with("ivm-session") {
        "session"
    } else if name.starts_with("ivm-sink") {
        "sink"
    } else {
        "driver"
    }
}

fn process_cpu_s() -> f64 {
    stat_ticks(&std::fs::read_to_string("/proc/self/stat").unwrap_or_default(), &[11, 12]) as f64 / 100.0
}

fn status_kb(field: &str) -> u64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    s.lines()
        .find(|l| l.starts_with(field))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// Reset the peak RSS (VmHWM) to the current RSS.
fn reset_peak_rss() {
    let _ = std::fs::write("/proc/self/clear_refs", "5");
}

/// When the process started, the origin of the measurement windows printed
/// for profilers (gprofng's timestamps count from about the same moment).
static ORIGIN: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

fn since_origin(t: Instant) -> f64 {
    t.saturating_duration_since(*ORIGIN.get_or_init(Instant::now)).as_secs_f64()
}

struct Meter {
    name: String,
    t0: Instant,
    cpu0: HashMap<u32, (String, u64, u64)>,
    allocs0: HashMap<u32, (u64, u64)>,
    proc0: f64,
    up0: f64,
}

#[derive(Default, Clone)]
struct Measure {
    wall: f64,
    cpu: BTreeMap<&'static str, f64>,
    /// Time the engine thread waited for a CPU (contention on a busy machine).
    engine_wait: f64,
    /// Allocations (count, bytes) of the engine thread.
    engine_allocs: (u64, u64),
    /// Allocations of the source threads.
    source_allocs: u64,
    process_cpu: f64,
    upstream_cpu: f64,
    peak_rss_mb: f64,
}

impl Meter {
    fn start(name: &str) -> Meter {
        reset_peak_rss();
        let allocs0 = thread_allocs();
        Meter { name: name.to_string(), t0: Instant::now(), cpu0: thread_cpu(), allocs0, proc0: process_cpu_s(), up0: upstream_cpu_s() }
    }

    fn stop(&self) -> Measure {
        self.stop_at(Instant::now())
    }

    /// Wall time up to `end`; CPU up to now.
    fn stop_at(&self, end: Instant) -> Measure {
        // the window for profilers, e.g. gprofng display text -filters 'TSTAMP>=a && TSTAMP<=b'
        println!("WINDOW {} {:.0} {:.0}", self.name, since_origin(self.t0) * 1e9, since_origin(end) * 1e9);
        let now = thread_cpu();
        let allocs = thread_allocs();
        let mut cpu: BTreeMap<&'static str, f64> = BTreeMap::new();
        let mut engine_wait = 0.0;
        let mut engine_allocs = (0, 0);
        let mut source_allocs = 0;
        for (tid, (name, ns, wait)) in &now {
            let (b, bw) = self.cpu0.get(tid).filter(|(n, _, _)| n == name).map(|(_, b, w)| (*b, *w)).unwrap_or((0, 0));
            let g = group_of(name);
            *cpu.entry(g).or_default() += ns.saturating_sub(b) as f64 / 1e9;
            let (c, by) = allocs.get(tid).copied().unwrap_or((0, 0));
            let (c0, by0) = self.allocs0.get(tid).copied().unwrap_or((0, 0));
            if g == "engine" {
                engine_wait += wait.saturating_sub(bw) as f64 / 1e9;
                engine_allocs.0 += c.saturating_sub(c0);
                engine_allocs.1 += by.saturating_sub(by0);
            } else if g == "source" {
                source_allocs += c.saturating_sub(c0);
            }
        }
        cpu.entry("engine").or_default();
        Measure {
            wall: end.saturating_duration_since(self.t0).as_secs_f64(),
            cpu,
            engine_wait,
            engine_allocs,
            source_allocs,
            process_cpu: process_cpu_s() - self.proc0,
            upstream_cpu: upstream_cpu_s() - self.up0,
            peak_rss_mb: status_kb("VmHWM:") as f64 / 1024.0,
        }
    }
}

/// Collected results, printed as they come and optionally written as JSON.
#[derive(Default)]
struct Report {
    lines: Vec<String>,
}

fn json_str(s: &str) -> String {
    serde_json::to_string(s).unwrap()
}

impl Report {
    fn record(&mut self, phase: &str, m: &Measure, extra: &[(&str, f64)]) {
        let mut parts = vec![format!("\"phase\":{}", json_str(phase)), format!("\"wall_s\":{:.4}", m.wall)];
        let mut human = format!("{phase:<28} wall {:>8.3} s", m.wall);
        for g in ["engine", "source", "sequencer", "session"] {
            let v = m.cpu.get(g).copied().unwrap_or(0.0);
            parts.push(format!("\"{g}_cpu_s\":{v:.4}"));
            if v >= 0.0005 {
                human.push_str(&format!(" | {g} {v:.3} s"));
            }
        }
        parts.push(format!("\"engine_wait_s\":{:.4}", m.engine_wait));
        parts.push(format!("\"engine_allocs\":{}", m.engine_allocs.0));
        parts.push(format!("\"engine_alloc_mb\":{:.1}", m.engine_allocs.1 as f64 / 1e6));
        parts.push(format!("\"source_allocs\":{}", m.source_allocs));
        parts.push(format!("\"process_cpu_s\":{:.3}", m.process_cpu));
        parts.push(format!("\"upstream_cpu_s\":{:.3}", m.upstream_cpu));
        parts.push(format!("\"peak_rss_mb\":{:.1}", m.peak_rss_mb));
        if m.engine_wait >= 0.01 {
            human.push_str(&format!(" | engine waited {:.2} s", m.engine_wait));
        }
        if m.upstream_cpu >= 0.01 {
            human.push_str(&format!(" | upstream {:.2} s", m.upstream_cpu));
        }
        if m.engine_allocs.0 > 0 {
            human.push_str(&format!(" | engine allocs {:.2}M", m.engine_allocs.0 as f64 / 1e6));
        }
        human.push_str(&format!(" | rss peak {:.0} MB", m.peak_rss_mb));
        for (k, v) in extra {
            parts.push(format!("\"{k}\":{v}"));
            human.push_str(&format!(" | {k} {}", fmt_num(*v)));
        }
        println!("{human}");
        let line = format!("{{{}}}", parts.join(","));
        println!("RESULT {line}");
        self.lines.push(line);
    }

    fn note(&mut self, phase: &str, extra: &[(&str, f64)]) {
        let mut parts = vec![format!("\"phase\":{}", json_str(phase))];
        let mut human = format!("{phase:<28}");
        for (k, v) in extra {
            parts.push(format!("\"{k}\":{v}"));
            human.push_str(&format!(" {k} {} |", fmt_num(*v)));
        }
        println!("{}", human.trim_end_matches('|'));
        let line = format!("{{{}}}", parts.join(","));
        println!("RESULT {line}");
        self.lines.push(line);
    }
}

fn fmt_num(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e15 { format!("{}", v as i64) } else if v.abs() >= 100.0 { format!("{v:.1}") } else { format!("{v:.4}") }
}

fn dir_bytes(p: &Path) -> u64 {
    let mut total = 0;
    if let Ok(rd) = std::fs::read_dir(p) {
        for e in rd.flatten() {
            let Ok(md) = e.metadata() else { continue };
            if md.is_dir() {
                total += dir_bytes(&e.path());
            } else {
                total += md.len();
            }
        }
    }
    total
}

/// Watches the engine's processed GSN and remembers when it last moved.
struct Monitor {
    stop: Arc<AtomicBool>,
    last: Arc<Mutex<(u64, Instant)>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Monitor {
    fn start(shared: Arc<ivm::coord::api::Shared>) -> Monitor {
        let stop = Arc::new(AtomicBool::new(false));
        let last = Arc::new(Mutex::new((shared.processed(), Instant::now())));
        let (s, l) = (stop.clone(), last.clone());
        let thread = std::thread::spawn(move || {
            while !s.load(Ordering::Relaxed) {
                let p = shared.processed();
                let mut g = l.lock().unwrap();
                if p != g.0 {
                    *g = (p, Instant::now());
                }
                drop(g);
                std::thread::sleep(Duration::from_micros(250));
            }
        });
        Monitor { stop, last, thread: Some(thread) }
    }

    /// (last processed GSN, when it was reached)
    fn finish(mut self) -> (u64, Instant) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
        *self.last.lock().unwrap()
    }
}

// ---------------------------------------------------------------------------
// upstream

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }
}

const KINDS: [&str; 5] = ["deposit", "withdrawal", "transfer", "fee", "refund"];

fn connect(conninfo: &str) -> Client {
    let cfg = PgConfig::parse(conninfo).unwrap_or_else(|e| panic!("bad conninfo {conninfo}: {e}"));
    Client::connect(&cfg).unwrap_or_else(|e| panic!("connect {conninfo}: {e}"))
}

fn with_dbname(conninfo: &str, db: &str) -> String {
    let kept: Vec<&str> = conninfo.split_whitespace().filter(|kv| !kv.starts_with("dbname=")).collect();
    format!("{} dbname={db}", kept.join(" "))
}

/// SELECT producing the txns rows with ids lo..=hi: account, kind and amount
/// come from multiplicative hashes of the id, so that they are spread and
/// independent of each other (about 100 (region, kind) groups of distinct
/// amounts), deterministically.
fn txns_rows(accounts: u64, lo: u64, hi: u64, start: &str) -> String {
    format!(
        "SELECT g, 1 + h1 % {accounts}, (ARRAY['deposit','withdrawal','transfer','fee','refund'])[1 + (h1 / 65536) % 5],
           ((h2 / 4096) % 100000) / 100.0, timestamp '{start}' + g * interval '1 second'
         FROM (SELECT g, (g * 2654435761) % 4294967296 AS h1, (g * 3266489917) % 4294967296 AS h2
               FROM generate_series({lo}::bigint, {hi}) g) s"
    )
}

fn generate_data(c: &mut Client, accounts: u64, txns: u64) {
    let t0 = Instant::now();
    let sql = format!(
        "CREATE TABLE accounts (id int PRIMARY KEY, region text, name text, balance numeric(12,2));
         INSERT INTO accounts SELECT g, 'region-' || lpad((g % 20)::text, 2, '0'), 'account ' || g, (g % 1000)::numeric
           FROM generate_series(1, {accounts}) g;
         CREATE TABLE txns (id bigint PRIMARY KEY, account_id int, kind text, amount numeric(12,2), created timestamp);
         INSERT INTO txns {};
         CREATE PUBLICATION bench_pub FOR TABLE accounts, txns;",
        txns_rows(accounts, 1, txns, "2024-01-01")
    );
    c.simple_query(&sql).expect("generate data");
    c.simple_query("VACUUM ANALYZE accounts").expect("vacuum");
    c.simple_query("VACUUM ANALYZE txns").expect("vacuum");
    eprintln!("generated upstream data ({accounts} accounts, {txns} txns) in {:.1?}", t0.elapsed());
}

/// Drop run databases (and their replication slots) left behind by runs
/// whose process is gone, e.g. after a crash.
fn cleanup_stale_runs(c: &mut Client) {
    let dbs = c.query("SELECT datname FROM pg_database WHERE datname LIKE 'ivmbench_run_%'").unwrap();
    for row in dbs.rows {
        let Some(db) = row[0].clone() else { continue };
        let pid: u32 = db.trim_start_matches("ivmbench_run_").parse().unwrap_or(0);
        let alive = std::fs::read_to_string(format!("/proc/{pid}/comm")).is_ok_and(|n| n.trim() == "ivm-bench");
        if alive {
            continue;
        }
        let slots = c.query(&format!("SELECT slot_name FROM pg_replication_slots WHERE database = '{db}' AND NOT active")).unwrap();
        for s in slots.rows {
            let _ = c.execute(&format!("SELECT pg_drop_replication_slot('{}')", s[0].clone().unwrap_or_default()));
        }
        match c.execute(&format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)")) {
            Ok(_) => eprintln!("dropped stale run database {db}"),
            Err(e) => eprintln!("could not drop stale run database {db}: {e}"),
        }
    }
}

/// Remember the upstream postmaster (for its CPU time), if it is local.
fn set_upstream_pid(data_dir: &Path) {
    let pid = std::fs::read_to_string(data_dir.join("postmaster.pid"))
        .ok()
        .and_then(|s| s.lines().next().and_then(|l| l.trim().parse::<u32>().ok()))
        .unwrap_or(0);
    UPSTREAM_PID.store(pid, Ordering::Relaxed);
}

/// The upstream database for this run.
struct Upstream {
    _cluster: Option<TempCluster>,
    /// conninfo of the run database
    conninfo: String,
    /// conninfo of a maintenance database (for dropping the run database / slots)
    admin: Option<String>,
    run_db: String,
}

fn setup_upstream(o: &Opts) -> Upstream {
    match &o.upstream {
        None => {
            let cluster = TempCluster::start(ClusterOptions {
                extra_conf: vec![
                    "timezone = 'UTC'".into(),
                    "shared_buffers = '512MB'".into(),
                    "max_wal_size = '16GB'".into(),
                    "checkpoint_timeout = '1h'".into(),
                ],
                ..Default::default()
            })
            .expect("start postgres");
            let conninfo = cluster.conninfo("postgres");
            let mut c = connect(&conninfo);
            set_upstream_pid(&cluster.data_dir);
            generate_data(&mut c, o.accounts, o.txns);
            Upstream { _cluster: Some(cluster), conninfo, admin: None, run_db: "postgres".into() }
        }
        Some(base) => {
            let admin = with_dbname(base, "postgres");
            let mut c = connect(&admin);
            if let Ok(r) = c.query("SHOW data_directory")
                && let Some(Some(d)) = r.rows.first().map(|r| r[0].clone())
            {
                set_upstream_pid(Path::new(&d));
            }
            // (v2: hashed account, kind and amount columns)
            let template = format!("ivmbench_v2_{}_{}", o.accounts, o.txns);
            let exists = !c.query(&format!("SELECT 1 FROM pg_database WHERE datname = '{template}'")).unwrap().rows.is_empty();
            if !exists {
                // generated under a temporary name: a template exists only when complete
                let tmp = format!("{template}_tmp");
                c.execute(&format!("DROP DATABASE IF EXISTS {tmp}")).unwrap();
                c.execute(&format!("CREATE DATABASE {tmp}")).unwrap();
                {
                    let mut t = connect(&with_dbname(base, &tmp));
                    generate_data(&mut t, o.accounts, o.txns);
                }
                c.execute(&format!("ALTER DATABASE {tmp} RENAME TO {template}")).unwrap();
            }
            cleanup_stale_runs(&mut c);
            let run_db = format!("ivmbench_run_{}", std::process::id());
            let t0 = Instant::now();
            c.execute(&format!("CREATE DATABASE {run_db} TEMPLATE {template}")).expect("copy template database");
            eprintln!("created run database {run_db} from {template} in {:.1?}", t0.elapsed());
            Upstream { _cluster: None, conninfo: with_dbname(base, &run_db), admin: Some(admin), run_db }
        }
    }
}

impl Upstream {
    fn current_lsn(&self, c: &mut Client) -> String {
        c.query("SELECT pg_current_wal_lsn()").unwrap().rows[0][0].clone().unwrap()
    }

    /// Drop the run database and the replication slots on it.
    fn cleanup(&self) {
        let Some(admin) = &self.admin else { return };
        let mut c = connect(admin);
        for _ in 0..50 {
            let r = c
                .query(&format!(
                    "SELECT slot_name, active FROM pg_replication_slots WHERE database = '{}'",
                    self.run_db
                ))
                .unwrap();
            if r.rows.is_empty() {
                break;
            }
            for row in &r.rows {
                if row[1].as_deref() == Some("f") {
                    let _ = c.execute(&format!("SELECT pg_drop_replication_slot('{}')", row[0].clone().unwrap()));
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        if let Err(e) = c.execute(&format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.run_db)) {
            eprintln!("could not drop {}: {e}", self.run_db);
        }
    }
}

// ---------------------------------------------------------------------------
// engine

struct Engine {
    running: Option<coord::Running>,
    port: u16,
    cfg: Config,
}

impl Engine {
    fn start(cfg: Config) -> Engine {
        let running = coord::start(cfg.clone()).expect("engine starts");
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let h = running.handle.clone();
        std::thread::spawn(move || {
            let _ = server::serve_listener(listener, ServerConfig::default(), h);
        });
        Engine { running: Some(running), port, cfg }
    }

    fn client(&self) -> Client {
        connect(&format!("host=127.0.0.1 port={} user=ivm dbname=ivm", self.port))
    }

    fn shared(&self) -> Arc<ivm::coord::api::Shared> {
        self.running.as_ref().unwrap().handle.shared.clone()
    }

    fn shutdown(&mut self) {
        if let Some(r) = self.running.take() {
            r.shutdown();
        }
    }
}

/// Block until the engine has applied everything upstream committed up to `lsn`.
fn wait_lsn(c: &mut Client, lsn: &str) {
    c.query(&format!("SELECT count(*) FROM by_region AS OF AT LEAST LSN '{lsn}'")).expect("wait for lsn");
}

const BY_REGION: &str = "CREATE MATERIALIZED VIEW by_region AS
  SELECT a.region, t.kind, count(*) AS n, sum(t.amount) AS total, max(t.amount) AS mx
  FROM txns t JOIN accounts a ON a.id = t.account_id GROUP BY a.region, t.kind";
const ACCT_TOTALS: &str = "CREATE MATERIALIZED VIEW acct_totals AS
  SELECT a.id, a.region, count(t.id) AS n, sum(t.amount) AS total
  FROM accounts a LEFT JOIN txns t ON t.account_id = a.id GROUP BY a.id, a.region";

// ---------------------------------------------------------------------------
// raw pgwire client for SUBSCRIBE (streams rows of a query that never ends)

struct RawConn {
    r: BufReader<TcpStream>,
    w: TcpStream,
}

impl RawConn {
    fn connect(port: u16) -> RawConn {
        let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_nodelay(true).unwrap();
        let mut c = RawConn { r: BufReader::with_capacity(1 << 16, s.try_clone().unwrap()), w: s };
        let mut body = Vec::new();
        body.extend_from_slice(&196608i32.to_be_bytes());
        for (k, v) in [("user", "ivm"), ("database", "ivm")] {
            body.extend_from_slice(k.as_bytes());
            body.push(0);
            body.extend_from_slice(v.as_bytes());
            body.push(0);
        }
        body.push(0);
        let mut msg = ((body.len() + 4) as i32).to_be_bytes().to_vec();
        msg.extend_from_slice(&body);
        c.w.write_all(&msg).unwrap();
        loop {
            let (t, _) = c.read_msg();
            if t == b'Z' {
                break;
            }
            assert_ne!(t, b'E', "startup failed");
        }
        c
    }

    fn query(&mut self, sql: &str) {
        let mut msg = vec![b'Q'];
        msg.extend_from_slice(&((sql.len() + 5) as i32).to_be_bytes());
        msg.extend_from_slice(sql.as_bytes());
        msg.push(0);
        self.w.write_all(&msg).unwrap();
    }

    fn read_msg(&mut self) -> (u8, Vec<u8>) {
        let mut h = [0u8; 5];
        self.r.read_exact(&mut h).unwrap();
        let len = i32::from_be_bytes(h[1..5].try_into().unwrap()) as usize - 4;
        let mut body = vec![0u8; len];
        self.r.read_exact(&mut body).unwrap();
        (h[0], body)
    }
}

/// Fields of a DataRow message (None = NULL).
fn data_row_fields(body: &[u8]) -> Vec<Option<&[u8]>> {
    let n = i16::from_be_bytes(body[0..2].try_into().unwrap()) as usize;
    let mut out = Vec::with_capacity(n);
    let mut pos = 2;
    for _ in 0..n {
        let len = i32::from_be_bytes(body[pos..pos + 4].try_into().unwrap());
        pos += 4;
        if len < 0 {
            out.push(None);
        } else {
            out.push(Some(&body[pos..pos + len as usize]));
            pos += len as usize;
        }
    }
    out
}

// ---------------------------------------------------------------------------
// workloads

/// SQL of `n` small single-statement transactions, deterministic for a seed.
/// Returns (batches of statements, number of transactions, next txn id).
fn small_txns(rng: &mut Rng, n: u64, accounts: u64, txns: u64, mut next_id: u64, del_from: &mut u64) -> (Vec<String>, u64) {
    let mut batches = Vec::new();
    let mut cur = String::new();
    for i in 0..n {
        let p = rng.below(1000);
        let stmt = if p < 500 {
            let id = next_id;
            next_id += 1;
            let acct = 1 + rng.below(accounts);
            let kind = KINDS[rng.below(5) as usize];
            let amount = rng.below(100_000) as f64 / 100.0;
            format!("INSERT INTO txns VALUES ({id}, {acct}, '{kind}', {amount:.2}, timestamp '2025-01-01' + interval '{i} seconds')")
        } else if p < 850 {
            // updates hit the lower half, deletes the upper half: no no-op transactions
            let id = 1 + rng.below(txns / 2);
            format!("UPDATE txns SET amount = amount + 1 WHERE id = {id}")
        } else if p < 950 {
            let id = *del_from;
            *del_from -= 1;
            format!("DELETE FROM txns WHERE id = {id}")
        } else if p < 995 {
            let id = 1 + rng.below(accounts);
            format!("UPDATE accounts SET balance = balance + 1 WHERE id = {id}")
        } else {
            let id = 1 + rng.below(accounts);
            let r = rng.below(20);
            format!("UPDATE accounts SET region = 'region-{r:02}' WHERE id = {id}")
        };
        cur.push_str("BEGIN;");
        cur.push_str(&stmt);
        cur.push_str(";COMMIT;");
        if (i + 1) % 200 == 0 {
            batches.push(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        batches.push(cur);
    }
    (batches, next_id)
}

fn latency_stats(mut v: Vec<f64>) -> (f64, f64, f64) {
    if v.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mean = v.iter().sum::<f64>() / v.len() as f64;
    let p = |q: f64| v[((v.len() as f64 - 1.0) * q).round() as usize];
    (mean, p(0.5), p(0.99))
}

/// Run `n` queries produced by `q(i)`; report latency and CPU.
fn read_bench(rep: &mut Report, c: &mut Client, phase: &str, n: u64, q: &dyn Fn(u64) -> String) -> u64 {
    let m = Meter::start(phase);
    let mut lat = Vec::with_capacity(n as usize);
    let mut rows = 0u64;
    for i in 0..n {
        let t = Instant::now();
        let r = c.query(&q(i)).unwrap_or_else(|e| panic!("{}: {e}", q(i)));
        lat.push(t.elapsed().as_secs_f64() * 1e3);
        rows += r.rows.len() as u64;
    }
    let meas = m.stop();
    let (mean, p50, p99) = latency_stats(lat);
    let per_q_cpu = (meas.cpu.get("engine").copied().unwrap_or(0.0) + meas.cpu.get("session").copied().unwrap_or(0.0)) / n as f64;
    rep.record(
        phase,
        &meas,
        &[
            ("queries", n as f64),
            ("rows", rows as f64),
            ("mean_ms", mean),
            ("p50_ms", p50),
            ("p99_ms", p99),
            ("cpu_ms_per_query", per_q_cpu * 1e3),
        ],
    );
    rows
}

fn main() {
    let o = parse_opts();
    ORIGIN.get_or_init(Instant::now);
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();
    let mut rep = Report::default();
    let up = setup_upstream(&o);
    let mut upc = connect(&up.conninfo);
    // every commit flushed: pg_current_wal_lsn() then covers it and the
    // walsender sees it at once
    upc.execute("SET synchronous_commit = on").unwrap();

    let base = o.data_dir.clone().unwrap_or_else(std::env::temp_dir);
    let data = base.join(format!("ivm-bench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data);
    let cfg = Config {
        data_dir: data.clone(),
        memory_bytes: o.memory_mb << 20,
        // only explicit checkpoints (and the one at shutdown), so phases are separable
        checkpoint_interval: Duration::from_secs(24 * 3600),
        sync: o.sync,
        tick: None,
        ..Default::default()
    };
    println!(
        "ivm-bench: {} accounts, {} txns, {} small txns, {} large, sync={}, memory {} MB",
        o.accounts, o.txns, o.small, o.large, o.sync, o.memory_mb
    );
    let mut eng = Engine::start(cfg.clone());
    let mut c = eng.client();

    // ---- (c) initial snapshot
    let m = Meter::start("load");
    // a slot name of our own: slot names are per cluster
    let slot = format!("ivmbench_{}", std::process::id());
    c.execute(&format!("CREATE SOURCE b FROM POSTGRES CONNECTION '{}' PUBLICATION 'bench_pub' SLOT '{slot}'", up.conninfo))
        .expect("create source");
    c.query("SELECT id FROM accounts WHERE id = 1").expect("accounts loaded");
    c.query("SELECT id FROM txns WHERE id = 1").expect("txns loaded");
    let meas = m.stop();
    let rows = (o.accounts + o.txns) as f64;
    rep.record("load", &meas, &[("rows", rows), ("rows_per_s", rows / meas.wall), ("engine_us_per_row", meas.cpu["engine"] * 1e6 / rows), ("source_us_per_row", meas.cpu.get("source").copied().unwrap_or(0.0) * 1e6 / rows)]);

    // ---- (e) checkpoint of the loaded tables
    let checkpoint = |rep: &mut Report, c: &mut Client, phase: &str| {
        let m = Meter::start(phase);
        c.execute("CHECKPOINT").expect("checkpoint");
        let meas = m.stop();
        let store = dir_bytes(&data.join("store")) as f64 / 1e6;
        let log = dir_bytes(&data.join("log")) as f64 / 1e6;
        rep.record(phase, &meas, &[("store_mb", store), ("log_mb", log)]);
    };
    checkpoint(&mut rep, &mut c, "checkpoint_tables");

    // ---- (c) hydration of new views over existing data
    for r in 0..o.repeat {
        if r > 0 {
            c.execute("DROP MATERIALIZED VIEW acct_totals").expect("drop view");
            c.execute("DROP MATERIALIZED VIEW by_region").expect("drop view");
        }
        for (name, sql) in [("hydrate_by_region", BY_REGION), ("hydrate_acct_totals", ACCT_TOTALS)] {
            let m = Meter::start(name);
            c.execute(sql).expect("create view");
            rep.record(name, &m.stop(), &[]);
        }
    }
    checkpoint(&mut rep, &mut c, "checkpoint_views");

    let mut rng = Rng(0x2545F4914F6CDD1D);
    let mut next_id = o.txns + 1;
    let mut del_from = o.txns;

    // ---- (a) small transactions, first live (warm cache, right after a
    // checkpoint), then generated while the engine is down and caught up
    // after a restart (cold cache)
    let reps = |phase: &str| if o.skip.iter().any(|s| s == phase) { 0 } else { o.repeat };
    for _ in 0..reps("small") {
        let (batches, nid) = small_txns(&mut rng, o.small, o.accounts, o.txns, next_id, &mut del_from);
        next_id = nid;
        let m = Meter::start("small_live");
        let mon = Monitor::start(eng.shared());
        let t = Instant::now();
        for b in &batches {
            upc.simple_query(b).expect("small txn batch");
        }
        let up_done = Instant::now();
        let lsn = up.current_lsn(&mut upc);
        wait_lsn(&mut c, &lsn);
        let (_, last) = mon.finish();
        let meas = m.stop_at(last);
        let n = o.small as f64;
        rep.record(
            "small_live",
            &meas,
            &[
                ("txns", n),
                ("txns_per_s", n / meas.wall),
                ("upstream_s", (up_done - t).as_secs_f64()),
                ("engine_us_per_txn", meas.cpu["engine"] * 1e6 / n),
                ("engine_allocs_per_txn", meas.engine_allocs.0 as f64 / n),
                ("source_us_per_txn", meas.cpu.get("source").copied().unwrap_or(0.0) * 1e6 / n),
                ("seq_us_per_txn", meas.cpu.get("sequencer").copied().unwrap_or(0.0) * 1e6 / n),
            ],
        );

        let t = Instant::now();
        eng.shutdown();
        rep.note("shutdown", &[("wall_s", t.elapsed().as_secs_f64())]);
        let (batches, nid) = small_txns(&mut rng, o.small, o.accounts, o.txns, next_id, &mut del_from);
        next_id = nid;
        let t = Instant::now();
        for b in &batches {
            upc.simple_query(b).expect("small txn batch");
        }
        let lsn = up.current_lsn(&mut upc);

        eprintln!("upstream: {} small transactions in {:.1?}", o.small, t.elapsed());
        let t = Instant::now();
        eng = Engine::start(cfg.clone());
        rep.note("recovery_start", &[("wall_s", t.elapsed().as_secs_f64())]);
        let m = Meter::start("small_catchup");
        let mon = Monitor::start(eng.shared());
        c = eng.client();
        wait_lsn(&mut c, &lsn);
        let (_, last) = mon.finish();
        let meas = m.stop_at(last);
        let n = o.small as f64;
        rep.record(
            "small_catchup",
            &meas,
            &[
                ("txns", n),
                ("txns_per_s", n / meas.wall),
                ("engine_us_per_txn", meas.cpu["engine"] * 1e6 / n),
                ("engine_allocs_per_txn", meas.engine_allocs.0 as f64 / n),
                ("source_us_per_txn", meas.cpu.get("source").copied().unwrap_or(0.0) * 1e6 / n),
                ("seq_us_per_txn", meas.cpu.get("sequencer").copied().unwrap_or(0.0) * 1e6 / n),
            ],
        );
    }

    // ---- (b) large transactions, engine live
    for _ in 0..reps("large") {
        let lo = next_id;
        let hi = next_id + o.large - 1;
        next_id = hi + 1;
        let accounts = o.accounts;
        let stmts = [
            (
                "large_insert",
                format!("INSERT INTO txns {}", txns_rows(accounts, lo, hi, "2025-06-01")),
            ),
            ("large_update", format!("UPDATE txns SET amount = amount + 0.01 WHERE id BETWEEN 1 AND {}", o.large)),
        ];
        for (phase, sql) in stmts {
            let m = Meter::start(phase);
            let mon = Monitor::start(eng.shared());
            let t = Instant::now();
            let n = upc.execute(&sql).expect("large transaction");
            let committed = Instant::now();
            let up_s = (committed - t).as_secs_f64();
            let lsn = up.current_lsn(&mut upc);
            wait_lsn(&mut c, &lsn);
            let (_, last) = mon.finish();
            let meas = m.stop_at(last);
            rep.record(
                phase,
                &meas,
                &[
                    ("rows", n as f64),
                    ("upstream_s", up_s),
                    ("catchup_after_commit_s", last.saturating_duration_since(committed).as_secs_f64()),
                    ("engine_us_per_row", meas.cpu["engine"] * 1e6 / n as f64),
                    ("engine_allocs_per_row", meas.engine_allocs.0 as f64 / n as f64),
                ],
            );
        }
    }

    // ---- (d) reads
    if !o.skip.iter().any(|s| s == "reads") {
        let accounts = o.accounts;
        let txns = o.txns;
        let n = o.point_reads;
        let seed = AtomicU64::new(12345);
        let rnd = |m: u64| {
            let mut r = Rng(seed.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed) | 1);
            1 + r.below(m)
        };
        read_bench(&mut rep, &mut c, "read_point_view", n, &|_| format!("SELECT * FROM acct_totals WHERE id = {}", rnd(accounts)));
        read_bench(&mut rep, &mut c, "read_point_table", n, &|_| format!("SELECT * FROM txns WHERE id = {}", rnd(txns / 2)));
        read_bench(&mut rep, &mut c, "read_scan_small_view", n / 10, &|_| "SELECT * FROM by_region".to_string());
        read_bench(&mut rep, &mut c, "read_range_view", 20, &|_| {
            let k = rnd(accounts - 100);
            format!("SELECT * FROM acct_totals WHERE id BETWEEN {k} AND {}", k + 99)
        });
        read_bench(&mut rep, &mut c, "read_scan_large_view", 3, &|_| "SELECT * FROM acct_totals".to_string());
        read_bench(&mut rep, &mut c, "adhoc_agg_table", 3, &|_| "SELECT kind, count(*), sum(amount) FROM txns GROUP BY kind".to_string());
        read_bench(&mut rep, &mut c, "adhoc_join_point", 5, &|_| {
            format!(
                "SELECT t.kind, count(*), sum(t.amount) FROM accounts a JOIN txns t ON t.account_id = a.id WHERE a.id = {} GROUP BY t.kind",
                rnd(accounts)
            )
        });
        read_bench(&mut rep, &mut c, "adhoc_join_agg", 3, &|_| {
            "SELECT a.region, count(*), sum(t.amount) FROM accounts a JOIN txns t ON t.account_id = a.id GROUP BY a.region".to_string()
        });
    }

    // ---- (d) SUBSCRIBE: snapshot and live delivery over the wire
    if !o.skip.iter().any(|s| s == "subscribe") {
        let port = eng.port;
        let snap_done = Arc::new(AtomicBool::new(false));
        let data_rows = Arc::new(AtomicU64::new(0));
        let last_row = Arc::new(Mutex::new(Instant::now()));
        let stop = Arc::new(AtomicBool::new(false));
        let (sd, dr, lr, st) = (snap_done.clone(), data_rows.clone(), last_row.clone(), stop.clone());
        let m = Meter::start("subscribe_snapshot");
        let t0 = Instant::now();
        let sub = std::thread::spawn(move || {
            let mut rc = RawConn::connect(port);
            rc.query("SUBSCRIBE TO acct_totals WITH (PROGRESS)");
            let mut snapshot_rows = 0u64;
            let mut snap_time = 0.0;
            while !st.load(Ordering::Relaxed) {
                let (t, body) = rc.read_msg();
                match t {
                    b'D' => {
                        let f = data_row_fields(&body);
                        let progressed = f[1] == Some(b"t".as_slice());
                        if progressed {
                            if !sd.load(Ordering::Relaxed) {
                                snap_time = t0.elapsed().as_secs_f64();
                                sd.store(true, Ordering::Release);
                            }
                        } else if !sd.load(Ordering::Relaxed) {
                            snapshot_rows += 1;
                        } else {
                            dr.fetch_add(1, Ordering::Release);
                            *lr.lock().unwrap() = Instant::now();
                        }
                    }
                    b'E' => panic!("subscribe failed: {}", String::from_utf8_lossy(&body)),
                    _ => {}
                }
            }
            (snapshot_rows, snap_time)
        });
        while !snap_done.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(1));
        }
        let snap_meas = m.stop();
        // live: single-row inserts, each changes one acct_totals row (-old, +new)
        let m = Meter::start("subscribe_live");
        let n = o.sub_txns;
        let mut batches = Vec::new();
        let mut cur = String::new();
        for i in 0..n {
            let id = next_id;
            next_id += 1;
            let acct = 1 + rng.below(o.accounts);
            cur.push_str(&format!(
                "BEGIN;INSERT INTO txns VALUES ({id}, {acct}, 'fee', 1.00, timestamp '2026-01-01' + interval '{i} seconds');COMMIT;"
            ));
            if (i + 1) % 50 == 0 {
                batches.push(std::mem::take(&mut cur));
            }
        }
        if !cur.is_empty() {
            batches.push(cur);
        }
        let t = Instant::now();
        for b in &batches {
            upc.simple_query(b).expect("subscribe txns");
        }
        let up_done = Instant::now();
        let lsn = up.current_lsn(&mut upc);
        wait_lsn(&mut c, &lsn);
        let expect = 2 * n;
        let deadline = Instant::now() + Duration::from_secs(120);
        while data_rows.load(Ordering::Acquire) < expect && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        let last = *last_row.lock().unwrap();
        let live = m.stop_at(last);
        let got = data_rows.load(Ordering::Acquire);
        stop.store(true, Ordering::Relaxed);
        // one more change unblocks the reader so it sees the stop flag
        upc.execute(&format!("INSERT INTO txns VALUES ({next_id}, 1, 'fee', 1.00, timestamp '2026-06-01')")).unwrap();
        let (snapshot_rows, snap_time) = sub.join().unwrap();
        rep.record("subscribe_snapshot", &snap_meas, &[("rows", snapshot_rows as f64), ("first_progress_s", snap_time)]);
        rep.record(
            "subscribe_live",
            &live,
            &[
                ("txns", n as f64),
                ("rows_delivered", got as f64),
                ("upstream_s", (up_done - t).as_secs_f64()),
                ("lag_after_upstream_s", last.saturating_duration_since(up_done).as_secs_f64()),
                ("engine_us_per_txn", live.cpu["engine"] * 1e6 / n as f64),
            ],
        );
    }

    // ---- (e) recovery: clean shutdown (checkpoint) and restart
    if !o.skip.iter().any(|s| s == "recovery") {
        let t = Instant::now();
        eng.shutdown();
        rep.note("recovery_shutdown", &[("wall_s", t.elapsed().as_secs_f64())]);
        let m = Meter::start("recovery");
        eng = Engine::start(cfg.clone());
        let started = m.stop();
        c = eng.client();
        let t = Instant::now();
        c.query("SELECT count(*) FROM by_region").expect("first read");
        let first = t.elapsed().as_secs_f64();
        let store = dir_bytes(&data.join("store")) as f64 / 1e6;
        rep.record("recovery", &started, &[("first_read_s", first), ("store_mb", store)]);
    }

    // ---- cleanup
    let _ = c.execute("DROP SOURCE b");
    eng.shutdown();
    up.cleanup();
    if !o.keep_data {
        let _ = std::fs::remove_dir_all(&data);
    }
    if let Some(p) = &o.json {
        let body = format!("[\n{}\n]\n", rep.lines.join(",\n"));
        std::fs::write(p, body).expect("write json");
    }
    let _ = &eng.cfg;
}
