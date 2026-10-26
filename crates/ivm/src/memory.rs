//! The process memory budget.
//!
//! One limit (`--memory-mb`) covers the memory the server keeps data in:
//! the buffer pool of tree nodes, the group caches of aggregations, the
//! records waiting to be applied, the changes queued for subscriptions
//! and sinks, and the dataflow's transient working memory (`Work`).
//! Everything is measured in bytes of memory — the sizes of the structs and
//! of the heap blocks they own ([`heap_block`]) — not in serialized bytes or
//! in counts, so that a limit means the same for narrow and for wide rows.
//!
//! Queues, caches and working memory get limits derived from the total; the
//! buffer pool gets what they leave over, but never less than a quarter of
//! it (it is the only part that can always give memory back, by evicting
//! clean nodes and writing dirty ones).

use crate::dataflow::{Delta, TableChange};
use crate::log::LogRecord;
use crate::storage::Gsn;
use crate::types::{Datum, Row};
use std::mem::{size_of, size_of_val};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Default of `--memory-mb`.
pub const DEFAULT_TOTAL: usize = 2 << 30;

/// Least room for queued records and for each consumer's queued changes,
/// whatever the budget (a budget of a few MB, as tests use to exercise
/// eviction, must not make subscriptions impossible).
const MIN_QUEUE: usize = 4 << 20;

/// Bytes a heap allocation of `n` bytes occupies. Allocators hand out small
/// blocks in size classes: glibc adds an 8-byte header and rounds to 16
/// bytes, mimalloc rounds to classes up to 12.5% apart. Rounding the size
/// plus a word up to 16 bytes is close to both.
#[inline]
pub fn heap_block(n: usize) -> usize {
    if n == 0 { 0 } else { (n + 8 + 15) & !15 }
}

/// Heap bytes owned by a datum (beyond its own size). Reference-counted
/// payloads are counted in full, also when they are shared.
pub fn datum_heap(d: &Datum) -> usize {
    // an Arc's block holds two counters before the value
    const ARC: usize = 16;
    match d {
        Datum::Numeric(n) => heap_block(ARC + size_of_val(&**n)),
        Datum::Text(s) | Datum::Json(s) => heap_block(ARC + s.len()),
        Datum::Bytea(b) => heap_block(ARC + b.len()),
        Datum::Jsonb(j) => heap_block(ARC + size_of_val(&**j)) + jsonb_heap(j),
        Datum::Array(a) => {
            heap_block(ARC + size_of_val(&**a))
                + heap_block(a.dims.capacity() * size_of::<(i32, i32)>())
                + heap_block(a.elems.capacity() * size_of::<Datum>())
                + a.elems.iter().map(datum_heap).sum::<usize>()
        }
        _ => 0,
    }
}

fn jsonb_heap(j: &pgfuncs::json::Jsonb) -> usize {
    use pgfuncs::json::Jsonb;
    match j {
        Jsonb::Null | Jsonb::Bool(_) => 0,
        Jsonb::Number(n) => heap_block(n.as_str().len()),
        Jsonb::String(s) => heap_block(s.capacity()),
        Jsonb::Array(v) => heap_block(v.capacity() * size_of::<Jsonb>()) + v.iter().map(jsonb_heap).sum::<usize>(),
        Jsonb::Object(v) => {
            heap_block(v.capacity() * size_of::<(String, Jsonb)>())
                + v.iter().map(|(k, x)| heap_block(k.capacity()) + jsonb_heap(x)).sum::<usize>()
        }
    }
}

/// Heap bytes owned by a row (its vector and the datums' payloads).
pub fn row_heap(r: &Row) -> usize {
    heap_block(r.capacity() * size_of::<Datum>()) + r.iter().map(datum_heap).sum::<usize>()
}

/// Bytes of a delta in memory.
pub fn delta_bytes(d: &Delta) -> usize {
    size_of::<Delta>() + heap_block(d.capacity() * size_of::<(Row, i64)>()) + d.iter().map(|(r, _)| row_heap(r)).sum::<usize>()
}

/// Bytes of a log record in memory, from its submission to the sequencer
/// until the engine has applied it (with its place in the queues).
pub fn record_bytes(rec: &LogRecord) -> usize {
    let own = match rec {
        LogRecord::Commit { changes, .. } => {
            heap_block(changes.capacity() * size_of::<(u64, TableChange)>())
                + changes
                    .iter()
                    .map(|(_, c)| match c {
                        TableChange::Upsert { row, unchanged } => row_heap(row) + heap_block(unchanged.capacity() * size_of::<usize>()),
                        TableChange::Delete { key: row } | TableChange::Insert { row } | TableChange::Remove { row } => row_heap(row),
                        TableChange::Truncate => 0,
                    })
                    .sum::<usize>()
        }
        LogRecord::Ddl { sql, extra, .. } => heap_block(sql.capacity()) + heap_block(extra.capacity()),
        LogRecord::Progress { state, .. } => heap_block(state.capacity()),
        LogRecord::Schema { columns, pk, .. } => {
            heap_block(columns.capacity() * size_of::<crate::types::Column>())
                + columns.iter().map(|c| heap_block(c.name.capacity())).sum::<usize>()
                + heap_block(pk.capacity() * size_of::<usize>())
        }
        LogRecord::Tick { .. } => 0,
    };
    // the record sits in a sequencer message, then in a durable batch
    2 * size_of::<(Gsn, LogRecord)>() + own
}

/// A byte counter, optionally counting in a parent as well (a consumer's
/// queue in the total of all queues).
#[derive(Default, Debug)]
pub struct Meter {
    used: AtomicUsize,
    parent: Option<Arc<Meter>>,
}

impl Meter {
    pub fn new() -> Arc<Meter> {
        Arc::new(Meter::default())
    }

    /// A meter whose bytes are also counted in `parent`.
    pub fn child(parent: &Arc<Meter>) -> Arc<Meter> {
        Arc::new(Meter { used: AtomicUsize::new(0), parent: Some(parent.clone()) })
    }

    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    pub fn add(&self, n: usize) {
        self.used.fetch_add(n, Ordering::Relaxed);
        if let Some(p) = &self.parent {
            p.add(n);
        }
    }

    pub fn sub(&self, n: usize) {
        // (saturating: a miscount must not wrap around to "full")
        let _ = self.used.try_update(Ordering::Relaxed, Ordering::Relaxed, |u| Some(u.saturating_sub(n)));
        if let Some(p) = &self.parent {
            p.sub(n);
        }
    }

    /// Count `n` bytes until the returned charge is dropped.
    pub fn charge(self: &Arc<Self>, n: usize) -> Charge {
        self.add(n);
        Charge { meter: Some(self.clone()), bytes: n }
    }
}

/// Bytes counted in a meter for as long as the value holding the charge
/// lives: a queued change releases its bytes when the consumer drops it,
/// wherever that happens (consumed, discarded with the queue, ...).
#[derive(Debug, Default)]
pub struct Charge {
    meter: Option<Arc<Meter>>,
    bytes: usize,
}

impl Charge {
    /// A charge counting nothing.
    pub fn none() -> Charge {
        Charge::default()
    }

    /// Count `n` more bytes (a charge counting nothing only notes them).
    pub fn grow(&mut self, n: usize) {
        if let Some(m) = &self.meter {
            m.add(n);
        }
        self.bytes += n;
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        if let Some(m) = &self.meter {
            m.sub(self.bytes);
        }
    }
}

/// Memory of caches — pure accelerators — against their limit. A cache
/// that would go over it does not take the new entry but marks the meter
/// squeezed; the owner then shrinks every cache (`take_squeezed`), evicting
/// first what was not used since the last time (`epoch`), so that caches
/// nobody uses any more do not keep the budget from the ones in use.
#[derive(Debug)]
pub struct CacheMeter {
    used: AtomicUsize,
    limit: AtomicUsize,
    squeezed: AtomicBool,
    /// Number of shrinks so far: entries remember the epoch they were last
    /// used in.
    epoch: AtomicU64,
}

impl Default for CacheMeter {
    fn default() -> Self {
        CacheMeter {
            used: AtomicUsize::new(0),
            limit: AtomicUsize::new(DEFAULT_TOTAL / 8),
            squeezed: AtomicBool::new(false),
            epoch: AtomicU64::new(0),
        }
    }
}

impl CacheMeter {
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    pub fn limit(&self) -> usize {
        self.limit.load(Ordering::Relaxed)
    }

    pub fn set_limit(&self, bytes: usize) {
        self.limit.store(bytes, Ordering::Relaxed);
    }

    /// Would `n` more bytes stay within the limit?
    pub fn fits(&self, n: usize) -> bool {
        self.used() + n <= self.limit()
    }

    pub fn add(&self, n: usize) {
        self.used.fetch_add(n, Ordering::Relaxed);
    }

    pub fn sub(&self, n: usize) {
        let _ = self.used.try_update(Ordering::Relaxed, Ordering::Relaxed, |u| Some(u.saturating_sub(n)));
    }

    /// A cache could not take an entry for lack of room.
    pub fn squeeze(&self) {
        self.squeezed.store(true, Ordering::Relaxed);
    }

    /// Should the caches shrink (squeezed since the last call, or over a
    /// lowered limit)?
    pub fn take_squeezed(&self) -> bool {
        self.squeezed.swap(false, Ordering::Relaxed) || self.used() > self.limit()
    }

    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Relaxed)
    }

    /// Start a new epoch (when the caches shrink); returns the one ending.
    pub fn next_epoch(&self) -> u64 {
        self.epoch.fetch_add(1, Ordering::Relaxed)
    }
}

/// Transient working memory of the dataflow: deltas and key sets kept in
/// memory for one GSN, sort buffers of indexes built or updated in bulk,
/// and the rows joins look up at once. Every such buffer charges what it
/// holds (`charge`) against one share of the budget, and spills (or
/// applies) it to temporary trees or the target index once it is `full`:
/// beyond its own cap, or, while the buffers together exceed the share,
/// beyond a sixteenth of it. The share is set aside from the buffer pool's.
#[derive(Clone, Debug)]
pub struct Work {
    meter: Arc<Meter>,
    limit: Arc<AtomicUsize>,
}

impl Work {
    /// Working memory of its own, within `limit` bytes (an environment
    /// outside an engine).
    pub fn new(limit: usize) -> Work {
        Work { meter: Meter::new(), limit: Arc::new(AtomicUsize::new(limit)) }
    }

    pub fn limit(&self) -> usize {
        self.limit.load(Ordering::Relaxed)
    }

    pub fn used(&self) -> usize {
        self.meter.used()
    }

    /// The cap of one buffer: half the share, within `lo` and `hi`.
    pub fn buffer(&self, lo: usize, hi: usize) -> usize {
        (self.limit() / 2).clamp(lo, hi)
    }

    /// A charge to grow as a buffer fills (`Charge::grow`), released with it.
    pub fn charge(&self) -> Charge {
        self.meter.charge(0)
    }

    /// Should a buffer whose cap is `cap` spill what it holds (`own`)?
    pub fn full(&self, own: usize, cap: usize) -> bool {
        own > cap || (own > cap / 16 && self.used() > self.limit())
    }
}

/// The process memory budget and the meters of the parts that are not the
/// buffer pool (the pool's own use is `Env`'s accounting; the group caches
/// are metered by the dataflow graph).
#[derive(Debug)]
pub struct MemBudget {
    total: AtomicUsize,
    /// Records submitted to the input log that the engine has not applied.
    pub input: Meter,
    /// Changes queued for subscriptions and sinks (all of them).
    pub outputs: Arc<Meter>,
    /// The dataflow's transient working memory: an eighth.
    pub work: Work,
}

impl Default for MemBudget {
    fn default() -> Self {
        MemBudget {
            total: AtomicUsize::new(DEFAULT_TOTAL),
            input: Meter::default(),
            outputs: Meter::new(),
            work: Work::new(DEFAULT_TOTAL / 8),
        }
    }
}

impl MemBudget {
    pub fn total(&self) -> usize {
        self.total.load(Ordering::Relaxed)
    }

    pub fn set_total(&self, bytes: usize) {
        self.total.store(bytes, Ordering::Relaxed);
        self.work.limit.store(bytes / 8, Ordering::Relaxed);
    }

    /// Records the engine may still have to apply: an eighth of the total
    /// (at least 4 MiB).
    pub fn input_limit(&self) -> usize {
        (self.total() / 8).max(MIN_QUEUE)
    }

    /// Changes queued for one subscription or sink: a sixteenth (at least
    /// 4 MiB).
    pub fn consumer_limit(&self) -> usize {
        (self.total() / 16).max(MIN_QUEUE)
    }

    /// All group caches together: an eighth.
    pub fn cache_limit(&self) -> usize {
        self.total() / 8
    }

    /// The buffer pool's share: what the other parts leave over (the
    /// working memory's share set aside), at least a quarter of the total.
    pub fn pool_target(&self, caches: usize) -> usize {
        let total = self.total();
        total.saturating_sub(self.input.used() + self.outputs.used() + caches + self.work.limit()).max(total / 4)
    }

    /// Wait until the records waiting to be applied are below their limit,
    /// then count `bytes` more. A record larger than the limit gets in once
    /// the queue is below it, so that it is never refused. False when
    /// `stop` turns true first (polled every millisecond).
    pub fn admit_input(&self, bytes: usize, stop: impl Fn() -> bool) -> bool {
        while self.input.used() >= self.input_limit() {
            if stop() {
                return false;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        self.input.add(bytes);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn charges_release_their_bytes_in_the_parent_too() {
        let total = Meter::new();
        let a = Meter::child(&total);
        let c1 = a.charge(100);
        let c2 = a.charge(50);
        assert_eq!((a.used(), total.used()), (150, 150));
        drop(c1);
        assert_eq!((a.used(), total.used()), (50, 50));
        drop(c2);
        assert_eq!((a.used(), total.used()), (0, 0));
        a.sub(10);
        assert_eq!(a.used(), 0, "saturates");
    }

    #[test]
    fn the_pool_gets_what_the_others_leave_but_a_quarter_at_least() {
        let b = MemBudget::default();
        b.set_total(1000);
        assert_eq!(b.work.limit(), 125);
        assert_eq!(b.pool_target(0), 875);
        b.input.add(100);
        let _c = b.outputs.charge(200);
        assert_eq!(b.pool_target(50), 525);
        assert_eq!(b.pool_target(600), 250);
    }

    #[test]
    fn working_buffers_spill_at_their_cap_or_when_together_over_the_share() {
        let w = Work::new(1000);
        assert_eq!(w.buffer(0, usize::MAX), 500);
        assert_eq!(w.buffer(600, usize::MAX), 600);
        let mut a = w.charge();
        a.grow(400);
        assert!(!w.full(400, 500));
        assert!(w.full(501, 500), "over its cap");
        let mut b = w.charge();
        b.grow(700);
        assert_eq!(w.used(), 1100);
        assert!(w.full(400, 500), "together over the share");
        assert!(!w.full(20, 500), "too small to be worth spilling");
        drop(b);
        assert_eq!(w.used(), 400);
        assert!(!w.full(400, 500));
    }

    #[test]
    fn input_admission_waits_for_the_queue_to_drain() {
        let b = Arc::new(MemBudget::default());
        assert_eq!(b.input_limit(), DEFAULT_TOTAL / 8);
        b.set_total(1 << 20);
        assert_eq!(b.input_limit(), MIN_QUEUE);
        // one record larger than the limit gets in when the queue is below it
        assert!(b.admit_input(5 << 20, || false));
        assert!(!b.admit_input(10, || true), "a full queue does not admit");
        let b2 = b.clone();
        let t = std::thread::spawn(move || b2.admit_input(10, || false));
        std::thread::sleep(Duration::from_millis(20));
        assert!(!t.is_finished(), "admitted while the queue is full");
        b.input.sub(5 << 20);
        assert!(t.join().unwrap());
        assert_eq!(b.input.used(), 10);
    }
}
