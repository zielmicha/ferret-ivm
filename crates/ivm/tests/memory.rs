//! Memory accounting of the buffer pool against the allocations it really
//! makes, measured by a counting allocator (per thread, so that tests
//! running in parallel do not disturb each other).
//!
//! The pool used to count estimated serialized bytes: an 8-byte key with
//! one version was 18 bytes, while in memory it is a 56-byte entry plus the
//! key's heap block, so 128 MiB of accounted cache took over 500 MiB; and a
//! full scan loaded the whole tree before evicting anything.

use ivm::storage::{AggKind, CountSummarizer, Env, EnvOptions, Summarizer, Summary};
use ivm::types::Datum;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::Arc;

struct Counting;

thread_local! {
    /// (bytes allocated and not freed by this thread, high-water mark)
    static LIVE: Cell<(isize, isize)> = const { Cell::new((0, 0)) };
}

fn count(delta: isize) {
    let _ = LIVE.try_with(|c| {
        let (live, peak) = c.get();
        c.set((live + delta, peak.max(live + delta)));
    });
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        count(l.size() as isize);
        unsafe { System.alloc(l) }
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        count(l.size() as isize);
        unsafe { System.alloc_zeroed(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        count(-(l.size() as isize));
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        count(new as isize - l.size() as isize);
        unsafe { System.realloc(p, l, new) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn live() -> isize {
    LIVE.with(|c| c.get().0)
}

/// Restart the high-water mark at the current live bytes.
fn reset_peak() {
    LIVE.with(|c| c.set((c.get().0, c.get().0)));
}

fn peak() -> isize {
    LIVE.with(|c| c.get().1)
}

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
    let r = loaded("narrow", 200_000, 1, big_cache(), || Arc::new(CountSummarizer), |i| i.to_be_bytes().to_vec());
    assert_close("narrow entries", r);
}

/// Histories longer than two changes leave their entry for a heap block.
#[test]
fn long_histories_are_accounted() {
    let r = loaded("history", 50_000, 5, big_cache(), || Arc::new(CountSummarizer), |i| i.to_be_bytes().to_vec());
    assert_close("entries with five versions", r);
}

/// Wide entries with wide summaries: 300-byte keys, and a MAX over them in
/// every child pointer (counted as 8 bytes apiece before). Small nodes make
/// the internal nodes a large part of the tree.
#[test]
fn wide_entries_and_summaries_are_accounted() {
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
