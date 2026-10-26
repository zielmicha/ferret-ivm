//! Memory: the buffer pool's accounting against the allocations it really
//! makes, and bounds on what work on data much larger than the pool takes,
//! measured by a counting allocator.
//!
//! The allocator counts per thread, so that the accounting tests can run in
//! parallel without disturbing each other, and for the whole process, for
//! the bounds of an engine's work, which runs on threads of its own. The
//! bound tests hold `EXCLUSIVE` for writing, the others for reading, so that
//! nothing else allocates while a bound is measured.
//!
//! The pool used to count estimated serialized bytes: an 8-byte key with
//! one version was 18 bytes, while in memory it is a 56-byte entry plus the
//! key's heap block, so 128 MiB of accounted cache took over 500 MiB; and a
//! full scan loaded the whole tree before evicting anything.

use ivm::coord::api::{ExecResult, SessionVars, SubEvent};
use ivm::coord::{self, EngineHandle};
use ivm::storage::{AggKind, CountSummarizer, Env, EnvOptions, Summarizer, Summary};
use ivm::types::text::format_datum;
use ivm::types::Datum;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

struct Counting;

thread_local! {
    /// (bytes allocated and not freed by this thread, high-water mark)
    static LIVE: Cell<(isize, isize)> = const { Cell::new((0, 0)) };
}

/// Bytes allocated and not freed by the process, and its high-water mark.
static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

/// Held for writing by the tests measuring the whole process.
static EXCLUSIVE: RwLock<()> = RwLock::new(());

fn count(delta: isize) {
    let _ = LIVE.try_with(|c| {
        let (live, peak) = c.get();
        c.set((live + delta, peak.max(live + delta)));
    });
    if delta >= 0 {
        let now = CURRENT.fetch_add(delta as usize, Ordering::Relaxed) + delta as usize;
        PEAK.fetch_max(now, Ordering::Relaxed);
    } else {
        CURRENT.fetch_sub(delta.unsigned_abs(), Ordering::Relaxed);
    }
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            count(l.size() as isize);
        }
        p
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(l) };
        if !p.is_null() {
            count(l.size() as isize);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) };
        count(-(l.size() as isize));
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        let q = unsafe { System.realloc(p, l, new) };
        if !q.is_null() {
            count(new as isize - l.size() as isize);
        }
        q
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn live() -> isize {
    LIVE.with(|c| c.get().0)
}

/// Restart this thread's high-water mark at its current live bytes.
fn reset_peak() {
    LIVE.with(|c| c.set((c.get().0, c.get().0)));
}

fn peak() -> isize {
    LIVE.with(|c| c.get().1)
}

/// The process's heap in use, and its peak since the last call.
fn heap() -> (usize, usize) {
    let now = CURRENT.load(Ordering::Relaxed);
    (now, PEAK.swap(now, Ordering::Relaxed))
}

const MB: usize = 1 << 20;

fn dir(name: &str) -> PathBuf {
    let base = std::env::var("IVM_TEST_TMP").map(PathBuf::from).unwrap_or_else(|_| std::env::temp_dir());
    let d = base.join(format!("ivm-memory-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

/// MAX over the key's bytes as text: every child pointer of an internal
/// node carries one key-sized value.
struct MaxKey;

impl Summarizer for MaxKey {
    fn spec(&self) -> &[AggKind] {
        &[AggKind::Max]
    }
    fn add_entry(&self, acc: &mut Summary, key: &[u8], weight: i64) -> ivm::Result<()> {
        acc.count += weight;
        let v = Datum::Text(String::from_utf8_lossy(key).into());
        AggKind::Max.add(&mut acc.aggs[0], &v, weight)
    }
}

/// Build a tree of `n` entries (keys from `key`, with `versions` changes
/// each), checkpoint it, reopen the store and load all of it with a cache
/// large enough to hold it: returns (accounted bytes, allocated bytes).
fn loaded(name: &str, n: u64, versions: u64, opts: EnvOptions, summ: fn() -> Arc<dyn Summarizer>, key: fn(u64) -> Vec<u8>) -> (usize, usize) {
    let d = dir(name);
    let tree = {
        let (mut env, _) = Env::open(&d, opts.clone()).unwrap();
        let t = env.create_tree(summ());
        for g in 1..=versions {
            let mut i = 0;
            while i < n {
                let mut ups: Vec<(Vec<u8>, i64)> = (i..(i + 50_000).min(n)).map(|j| (key(j), if g % 2 == 1 { 1 } else { -1 })).collect();
                env.apply(t, &mut ups, g).unwrap();
                i += 50_000;
            }
        }
        env.checkpoint(versions, b"").unwrap();
        t
    };
    let (mut env, _) = Env::open(&d, opts).unwrap();
    env.attach_tree(tree, summ()).unwrap();
    let before = live();
    let mut seen = 0u64;
    env.for_each(tree, Bound::Unbounded, Bound::Unbounded, &mut |_, _| {
        seen += 1;
        Ok(true)
    })
    .unwrap();
    assert_eq!(seen, n);
    let allocated = (live() - before) as usize;
    let accounted = env.stats().mem_bytes;
    drop(env);
    let _ = std::fs::remove_dir_all(&d);
    (accounted, allocated)
}

fn big_cache() -> EnvOptions {
    EnvOptions { cache_bytes: 1 << 40, ..Default::default() }
}

fn assert_close(what: &str, (accounted, allocated): (usize, usize)) {
    let ratio = accounted as f64 / allocated as f64;
    println!("{what}: accounted {accounted} B, allocated {allocated} B, ratio {ratio:.2}");
    assert!((0.8..1.4).contains(&ratio), "{what}: accounted {accounted} B for {allocated} B allocated (ratio {ratio:.2})");
}

/// Narrow entries: 8-byte keys, one version each (counted as 18 bytes
/// apiece before; a quarter of what they take).
#[test]
fn narrow_entries_are_accounted_at_their_size_in_memory() {
    let _shared = EXCLUSIVE.read().unwrap_or_else(|e| e.into_inner());
    let r = loaded("narrow", 200_000, 1, big_cache(), || Arc::new(CountSummarizer), |i| i.to_be_bytes().to_vec());
    assert_close("narrow entries", r);
}

/// Histories longer than two changes leave their entry for a heap block.
#[test]
fn long_histories_are_accounted() {
    let _shared = EXCLUSIVE.read().unwrap_or_else(|e| e.into_inner());
    let r = loaded("history", 50_000, 5, big_cache(), || Arc::new(CountSummarizer), |i| i.to_be_bytes().to_vec());
    assert_close("entries with five versions", r);
}

/// Wide entries with wide summaries: 300-byte keys, and a MAX over them in
/// every child pointer (counted as 8 bytes apiece before). Small nodes make
/// the internal nodes a large part of the tree.
#[test]
fn wide_entries_and_summaries_are_accounted() {
    let _shared = EXCLUSIVE.read().unwrap_or_else(|e| e.into_inner());
    let opts = EnvOptions { leaf_bytes: 2 << 10, internal_bytes: 4 << 10, ..big_cache() };
    let key = |i: u64| format!("{i:020}{}", "x".repeat(280)).into_bytes();
    let r = loaded("wide", 20_000, 1, opts, || Arc::new(MaxKey), key);
    assert_close("wide entries and summaries", r);
}

/// SUM over a float derived from the key: values spanning the whole
/// exponent range, so that the exact sums in the child pointers hold
/// magnitudes of hundreds of limbs, beyond the four kept inline.
struct WideFloatSum;

impl Summarizer for WideFloatSum {
    fn spec(&self) -> &[AggKind] {
        &[AggKind::SumFloat]
    }
    fn add_entry(&self, acc: &mut Summary, key: &[u8], weight: i64) -> ivm::Result<()> {
        acc.count += weight;
        let i = u64::from_be_bytes(key[..8].try_into().unwrap());
        let x = if i % 2 == 0 { 1e300 * (1 + i % 7) as f64 } else { 1e-300 * (1 + i % 5) as f64 };
        AggKind::SumFloat.add(&mut acc.aggs[0], &Datum::Float8(x), weight)
    }
}

/// Exact float sums whose magnitudes no longer fit inline: the summary of
/// every child pointer owns a heap block for them, which the pool counts.
#[test]
fn spilled_float_sums_are_accounted() {
    let _shared = EXCLUSIVE.read().unwrap_or_else(|e| e.into_inner());
    // leaves of a few entries: the child pointers hold most of the memory
    let opts = EnvOptions { leaf_bytes: 64, internal_bytes: 1 << 10, ..big_cache() };
    let r = loaded("float-sums", 50_000, 1, opts, || Arc::new(WideFloatSum), |i| i.to_be_bytes().to_vec());
    assert_close("float sums with spilled magnitudes", r);
}

/// A full scan keeps the pool at its target: it evicts after every leaf
/// instead of loading the whole tree first (a 128 MiB cache used to grow by
/// 1.5 GB during one scan).
#[test]
fn a_full_scan_stays_within_the_cache() {
    let _shared = EXCLUSIVE.read().unwrap_or_else(|e| e.into_inner());
    let d = dir("scan");
    let cache = 2 << 20;
    let opts = EnvOptions { cache_bytes: cache, ..Default::default() };
    let n = 300_000u64;
    let key = |i: u64| format!("{i:032}").into_bytes();
    let tree = {
        let (mut env, _) = Env::open(&d, opts.clone()).unwrap();
        let t = env.create_tree(Arc::new(CountSummarizer));
        let mut i = 0;
        while i < n {
            let mut ups: Vec<(Vec<u8>, i64)> = (i..(i + 50_000).min(n)).map(|j| (key(j), 1)).collect();
            env.apply(t, &mut ups, 1).unwrap();
            env.checkpoint(1, b"").unwrap();
            i += 50_000;
        }
        t
    };
    let (mut env, _) = Env::open(&d, opts).unwrap();
    env.attach_tree(tree, Arc::new(CountSummarizer)).unwrap();
    let before = live();
    reset_peak();
    let mut seen = 0u64;
    let mut max_accounted = 0usize;
    // (the cursor runs the same per-leaf descent; it lets us look at the
    // pool between leaves)
    env.for_each(tree, Bound::Unbounded, Bound::Unbounded, &mut |_, _| {
        seen += 1;
        Ok(true)
    })
    .unwrap();
    let grew = (peak() - before) as usize;
    let mut c = env.cursor(tree, Bound::Unbounded, Bound::Unbounded);
    while env.cursor_next(&mut c).unwrap().is_some() {
        max_accounted = max_accounted.max(env.stats().mem_bytes);
    }
    assert_eq!(seen, n);
    println!("scan of {n} entries with a {cache} B cache: peak allocation +{grew} B, pool at most {max_accounted} B");
    // a tree of ~30 MB: the scan holds about the cache, plus the path being
    // read and the segment read buffer
    assert!(grew < 2 * cache, "the scan allocated {grew} B with a {cache} B cache");
    assert!(max_accounted < cache + (64 << 10), "the pool held {max_accounted} B with a {cache} B cache");
    drop(env);
    let _ = std::fs::remove_dir_all(&d);
}

/// A scan recycles the nodes it reads instead of pushing the pool's working
/// set out: after a full scan of a tree larger than the pool, the nodes of
/// another tree that were in the pool are still there.
#[test]
fn a_scan_keeps_the_working_set() {
    let _shared = EXCLUSIVE.read().unwrap_or_else(|e| e.into_inner());
    let d = dir("working-set");
    let cache = 2 << 20;
    let opts = EnvOptions { cache_bytes: cache, ..Default::default() };
    let key = |i: u64| format!("{i:032}").into_bytes();
    let fill = |env: &mut Env, t: u64, n: u64| {
        let mut i = 0;
        while i < n {
            let mut ups: Vec<(Vec<u8>, i64)> = (i..(i + 50_000).min(n)).map(|j| (key(j), 1)).collect();
            env.apply(t, &mut ups, 1).unwrap();
            env.checkpoint(1, b"").unwrap();
            i += 50_000;
        }
    };
    let (hot, big) = {
        let (mut env, _) = Env::open(&d, opts.clone()).unwrap();
        let hot = env.create_tree(Arc::new(CountSummarizer));
        let big = env.create_tree(Arc::new(CountSummarizer));
        fill(&mut env, hot, 8_000);
        fill(&mut env, big, 300_000);
        (hot, big)
    };
    let (mut env, _) = Env::open(&d, opts).unwrap();
    env.attach_tree(hot, Arc::new(CountSummarizer)).unwrap();
    env.attach_tree(big, Arc::new(CountSummarizer)).unwrap();
    env.for_each(hot, Bound::Unbounded, Bound::Unbounded, &mut |_, _| Ok(true)).unwrap();
    let before = env.tree_residency()[&hot];
    assert!(before.bytes > cache / 4 && before.bytes < cache, "the hot tree takes {} bytes", before.bytes);
    env.for_each(big, Bound::Unbounded, Bound::Unbounded, &mut |_, _| Ok(true)).unwrap();
    let after = env.tree_residency().get(&hot).copied().unwrap_or_default();
    println!("hot tree resident before the scan {} B ({} nodes), after {} B ({} nodes)", before.bytes, before.nodes, after.bytes, after.nodes);
    assert!(after.nodes * 10 >= before.nodes * 9, "the scan evicted the working set: {before:?} -> {after:?}");
    assert!(env.stats().mem_bytes <= cache, "the pool holds {} bytes", env.stats().mem_bytes);
    drop(env);
    let _ = std::fs::remove_dir_all(&d);
}

fn try_exec(h: &EngineHandle, sql: &str) -> Result<ExecResult, String> {
    let stmt = ivm::sql::parse_statement(sql).map_err(|e| format!("parse: {e}"))?;
    h.execute(stmt, vec![], SessionVars::default(), String::new()).map_err(|e| format!("{} {}", e.code, e.message))
}

fn exec(h: &EngineHandle, sql: &str) {
    try_exec(h, sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn rows(h: &EngineHandle, sql: &str) -> Vec<String> {
    match try_exec(h, sql).unwrap_or_else(|e| panic!("{sql}: {e}")) {
        ExecResult::Query { rows, .. } => rows.iter().map(|r| r.iter().map(format_datum).collect::<Vec<_>>().join("|")).collect(),
        other => panic!("{sql}: {other:?}"),
    }
}

#[test]
fn memory_stays_within_the_cache() {
    let _exclusive = EXCLUSIVE.write().unwrap_or_else(|e| e.into_inner());
    let dir = dir("bounds");
    let cache = 4 * MB;
    let cfg = coord::Config { data_dir: dir.clone(), sync: false, tick: None, memory_bytes: cache, ..Default::default() };
    let running = coord::start(cfg.clone()).expect("engine starts");
    let h = running.handle.clone();
    hydration(&h, cache);
    let alone = large_commit(&h, cache);
    large_commit_delivered(&h, cache, alone);
    large_replacement_delivered(&h, cache);
    let running = large_commit_replayed(running, cfg, cache);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A commit too large for a log frame is logged as parts, and replayed
/// after a crash a part at a time: from the log through the dataflow (the
/// table and the views of `large_commit`) into the arrangements, its rows
/// are never all in memory at once. (Replay used to assemble every part
/// of the commit before the dataflow stepped it.)
fn large_commit_replayed(running: coord::Running, cfg: coord::Config, cache: usize) -> coord::Running {
    let h = running.handle.clone();
    exec(&h, "CHECKPOINT");
    exec(&h, "INSERT INTO q SELECT g, g % 1000, repeat('r', 300) || g FROM generate_series(300001, 450000) g");
    let parts = std::fs::read_dir(cfg.data_dir.join("log")).unwrap().filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().starts_with("parts-")).count();
    assert_eq!(parts, 1, "the commit is logged as parts");
    running.crash();
    let (before, _) = heap();
    let running = coord::start(cfg).expect("engine restarts");
    let (_, peak) = heap();
    let used = peak.saturating_sub(before);
    println!("replaying a commit of 150,000 wide rows from its parts: peak {} MiB", used / MB);
    // (the rows alone take about 70 MiB in memory)
    assert!(used < cache + 40 * MB, "replaying the commit used {} MiB", used / MB);
    let h = running.handle.clone();
    assert_eq!(rows(&h, "SELECT count(*) FROM q"), ["450000"]);
    assert_eq!(rows(&h, "SELECT count(*), sum(k) FROM qf"), rows(&h, "SELECT count(*), sum(k) FROM q"));
    assert_eq!(rows(&h, "SELECT count(*), sum(c) FROM qa"), ["1000|450000"]);
    running
}

/// Hydrating views of a table about ten times the buffer pool, and views
/// several times larger still (a filter, joins with fan-out, aggregates)
/// peaks within the pool and a constant. (Hydration used to hold the whole
/// snapshot, every encoded entry and the dirty tree: 135 to 370 MiB here.)
fn hydration(h: &EngineHandle, cache: usize) {
    exec(h, "CREATE TABLE t (id bigint PRIMARY KEY, k bigint, s text)");
    let n = 200_000;
    for i in (0..n).step_by(20_000) {
        exec(h, &format!("INSERT INTO t SELECT g, g % 50000, repeat('s', 150) || g FROM generate_series({i}, {}) g", i + 19_999));
    }
    exec(h, "CHECKPOINT");
    // (the first join builds the index on t (k) the others use)
    let views = [
        ("filter", "SELECT id, k, s FROM t WHERE k >= 0", "200000"),
        ("fanout", "SELECT a.k, b.s FROM t a JOIN t b ON a.k = b.k", "800000"),
        ("join", "SELECT a.id AS aid, b.id AS bid, a.s FROM t a JOIN t b ON a.k = b.k AND a.id <> b.id", "600000"),
        ("agg", "SELECT k, count(*) AS c, max(s) AS m FROM t GROUP BY k", "50000"),
        ("joinagg", "SELECT a.k, count(*) AS c, max(b.s) AS m FROM t a JOIN t b ON a.k = b.k GROUP BY a.k", "50000"),
    ];
    for (name, q, count) in views {
        let (before, _) = heap();
        exec(h, &format!("CREATE MATERIALIZED VIEW {name} AS {q}"));
        let (_, peak) = heap();
        let used = peak.saturating_sub(before);
        println!("hydrating {name}: peak {} MiB above the start", used / MB);
        assert!(used < cache + 40 * MB, "hydrating {name} used {} MiB above {} MiB", used / MB, before / MB);
        assert_eq!(rows(h, &format!("SELECT count(*) FROM {name}")), [count], "{name}");
    }
    // the views are maintained as usual afterwards
    exec(h, "UPDATE t SET k = k + 1 WHERE id < 10");
    assert_eq!(rows(h, "SELECT count(*) FROM filter WHERE k >= 1 AND id < 10"), ["10"]);
    assert_eq!(rows(h, "SELECT sum(c) FROM joinagg"), rows(h, "SELECT count(*) FROM t a JOIN t b ON a.k = b.k"));
    assert_eq!(rows(h, "SELECT sum(c) FROM agg"), ["200000"]);
}

/// A commit many times the buffer pool, into a table with views (a filter,
/// a join, an aggregate), costs the dataflow no more than the pool and a
/// constant beyond what the same commit costs into a table without views
/// (the statement and the log record themselves, outside the dataflow).
/// The deltas used to be held in memory, all of them until the end of the
/// GSN.
fn large_commit(h: &EngineHandle, cache: usize) -> usize {
    exec(h, "CREATE TABLE plain (id bigint PRIMARY KEY, k bigint, s text)");
    exec(h, "CREATE TABLE q (id bigint PRIMARY KEY, k bigint, s text)");
    exec(h, "CREATE TABLE d (k bigint PRIMARY KEY, name text)");
    exec(h, "INSERT INTO d SELECT g, 'name' || g FROM generate_series(0, 999) g");
    exec(h, "CREATE MATERIALIZED VIEW qf AS SELECT id, k, s FROM q WHERE k >= 0");
    exec(h, "CREATE MATERIALIZED VIEW qj AS SELECT q.id, d.name, q.s FROM q JOIN d ON q.k = d.k");
    exec(h, "CREATE MATERIALIZED VIEW qa AS SELECT k, count(*) AS c, max(s) AS m FROM q GROUP BY k");
    exec(h, "CHECKPOINT");
    let insert = |t: &str| {
        let (before, _) = heap();
        exec(h, &format!("INSERT INTO {t} SELECT g, g % 1000, repeat('s', 300) || g FROM generate_series(1, 150000) g"));
        let (_, peak) = heap();
        peak.saturating_sub(before)
    };
    let alone = insert("plain");
    let with_views = insert("q");
    println!("a commit of 150,000 wide rows: peak {} MiB without views, {} MiB with views", alone / MB, with_views / MB);
    assert!(
        with_views < alone + cache + 40 * MB,
        "the commit used {} MiB with views, {} MiB without",
        with_views / MB,
        alone / MB
    );
    assert_eq!(rows(h, "SELECT count(*), sum(k) FROM qf"), rows(h, "SELECT count(*), sum(k) FROM q"));
    assert_eq!(rows(h, "SELECT count(*) FROM qj"), ["150000"]);
    assert_eq!(rows(h, "SELECT count(*), sum(c) FROM qa"), ["1000|150000"]);
    alone
}

/// A subscription's event (`subscriptions`), with a timeout.
fn next_event(rx: &crossbeam_channel::Receiver<SubEvent>) -> SubEvent {
    rx.recv_timeout(std::time::Duration::from_secs(30)).expect("an event")
}

/// The changes of such a commit reach a subscriber in chunks read from the
/// trees the step spilled them to: the commit costs no more with a
/// subscriber than with views alone, and a subscriber that does not read
/// them is ended at its queue's limit. (A spilled output was read back
/// whole for its consumers, and copied once more.)
fn large_commit_delivered(h: &EngineHandle, cache: usize, alone: usize) {
    let ExecResult::Subscribe { rx, .. } = try_exec(h, "SUBSCRIBE TO qf WITH (SNAPSHOT = false)").unwrap() else { panic!("not a subscription") };
    assert!(matches!(next_event(&rx), SubEvent::Progress { .. }));
    let (before, _) = heap();
    exec(h, "INSERT INTO q SELECT g, g % 1000, repeat('s', 300) || g FROM generate_series(150001, 300000) g");
    let (_, peak) = heap();
    let used = peak.saturating_sub(before);
    println!("a commit of 150,000 wide rows with a subscriber to their view: peak {} MiB", used / MB);
    assert!(used < alone + cache + 40 * MB, "the commit used {} MiB with a subscriber", used / MB);
    let (mut chunks, mut error) = (0, None);
    while error.is_none() {
        match next_event(&rx) {
            SubEvent::Data { rows, .. } => {
                assert!(rows.len() <= 1024, "a chunk of {} rows", rows.len());
                chunks += 1;
            }
            SubEvent::Error(e) => error = Some(e.code),
            _ => {}
        }
    }
    assert!(chunks >= 2, "{chunks} chunks");
    assert_eq!(error.as_deref(), Some("53000"));
}

/// Replacing a view with subscribers computes the change from the old to
/// the new contents into a temporary tree and delivers it in chunks: the
/// replacement stays within the pool and a constant, and the subscriber
/// gets the change a chunk at a time (ended at its queue's limit here).
/// (The change was computed and delivered as one vector in memory.)
fn large_replacement_delivered(h: &EngineHandle, cache: usize) {
    exec(h, "CREATE MATERIALIZED VIEW rv AS SELECT id, s FROM q WHERE k < 100");
    let ExecResult::Subscribe { rx, .. } = try_exec(h, "SUBSCRIBE TO rv WITH (SNAPSHOT = false)").unwrap() else { panic!("not a subscription") };
    assert!(matches!(next_event(&rx), SubEvent::Progress { .. }));
    let (before, _) = heap();
    // (about 120,000 rows more: some 50 MiB in memory)
    exec(h, "CREATE OR REPLACE MATERIALIZED VIEW rv AS SELECT id, s FROM q WHERE k < 500");
    let (_, peak) = heap();
    let used = peak.saturating_sub(before);
    println!("replacing a view with a subscriber by one 120,000 wide rows larger: peak {} MiB", used / MB);
    // (the change alone takes about 50 MiB in memory; it used to take 27
    // MiB, held as one vector, where it now takes 5)
    assert!(used < cache + 12 * MB, "the replacement used {} MiB", used / MB);
    let (mut chunks, mut error) = (0, None);
    while error.is_none() {
        match next_event(&rx) {
            SubEvent::Data { rows, .. } => {
                assert!(rows.len() <= 1024, "a chunk of {} rows", rows.len());
                assert!(rows.iter().all(|(_, w)| *w == 1));
                chunks += 1;
            }
            SubEvent::Error(e) => error = Some(e.code),
            _ => {}
        }
    }
    assert!(chunks >= 2, "{chunks} chunks");
    assert_eq!(error.as_deref(), Some("53000"));
    assert_eq!(rows(h, "SELECT count(*) FROM rv"), ["150000"]);
}
