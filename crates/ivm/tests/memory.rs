//! Memory bounds: work on data much larger than the buffer pool must stay
//! within the pool plus a constant. A counting allocator measures the
//! process's peak heap (this file holds a single test, so that nothing else
//! allocates meanwhile).

use ivm::coord::api::{ExecResult, SessionVars};
use ivm::coord::{self, EngineHandle};
use ivm::types::text::format_datum;
use std::alloc::{GlobalAlloc, Layout, System};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Counting;

static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            let now = CURRENT.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            if new_size >= layout.size() {
                let now = CURRENT.fetch_add(new_size - layout.size(), Ordering::Relaxed) + new_size - layout.size();
                PEAK.fetch_max(now, Ordering::Relaxed);
            } else {
                CURRENT.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        p
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Heap in use, and the peak since the last call.
fn heap() -> (usize, usize) {
    let now = CURRENT.load(Ordering::Relaxed);
    (now, PEAK.swap(now, Ordering::Relaxed))
}

const MB: usize = 1 << 20;

fn tmpdir(name: &str) -> PathBuf {
    let base = std::env::var("IVM_TEST_TMP").unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().into_owned());
    let d = PathBuf::from(base).join(format!("ivm-memory-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
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

/// Hydrating views of a table about ten times the buffer pool, and views
/// several times larger still (a filter, joins with fan-out, aggregates)
/// peaks within the pool and a constant. (Hydration used to hold the whole
/// snapshot, every encoded entry and the dirty tree: 135 to 370 MiB here.)
#[test]
fn hydration_memory_stays_within_the_cache() {
    let dir = tmpdir("hydration");
    let cache = 4 * MB;
    let cfg = coord::Config { data_dir: dir.clone(), sync: false, tick: None, cache_bytes: cache, ..Default::default() };
    let running = coord::start(cfg).expect("engine starts");
    let h = running.handle.clone();
    exec(&h, "CREATE TABLE t (id bigint PRIMARY KEY, k bigint, s text)");
    let n = 200_000;
    for i in (0..n).step_by(20_000) {
        exec(&h, &format!("INSERT INTO t SELECT g, g % 50000, repeat('s', 150) || g FROM generate_series({i}, {}) g", i + 19_999));
    }
    exec(&h, "CHECKPOINT");
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
        exec(&h, &format!("CREATE MATERIALIZED VIEW {name} AS {q}"));
        let (_, peak) = heap();
        let used = peak.saturating_sub(before);
        assert!(used < cache + 40 * MB, "hydrating {name} used {} MiB above {} MiB", used / MB, before / MB);
        assert_eq!(rows(&h, &format!("SELECT count(*) FROM {name}")), [count], "{name}");
    }
    // the views are maintained as usual afterwards
    exec(&h, "UPDATE t SET k = k + 1 WHERE id < 10");
    assert_eq!(rows(&h, "SELECT count(*) FROM filter WHERE k >= 1 AND id < 10"), ["10"]);
    assert_eq!(rows(&h, "SELECT sum(c) FROM joinagg"), rows(&h, "SELECT count(*) FROM t a JOIN t b ON a.k = b.k"));
    assert_eq!(rows(&h, "SELECT sum(c) FROM agg"), ["200000"]);
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
