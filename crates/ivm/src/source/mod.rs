//! Sources: the CDC edge. The engine is a replica of the source tables:
//! every change becomes an upsert/delete by primary key, logged with a GSN.

pub mod postgres;

use crate::catalog::SourceDef;
use crate::coord::EngineHandle;
use crate::error::Result;
use crate::sql::plan::ObjectId;
use crate::types::Column;
use crossbeam_channel::Sender;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub use postgres::discover;

/// A source table as known to the catalog.
#[derive(Clone, Debug)]
pub struct SourceTable {
    pub id: ObjectId,
    pub upstream: (String, String),
    pub columns: Vec<Column>,
    pub pk: Vec<usize>,
}

/// Live counters of a source, updated by its thread with relaxed atomic
/// stores and read by the web UI (see `Shared::source_gauges`).
#[derive(Default, Debug)]
pub struct SourceGauges {
    /// Upstream end of WAL, as last reported by the server.
    pub wal_end: AtomicU64,
    /// Server clock of the last message (microseconds since 2000-01-01).
    pub server_time: AtomicI64,
    /// Local wall clock (Unix microseconds) when the last message arrived.
    pub received_at: AtomicI64,
    /// Transactions with changes handed to the input log.
    pub commits: AtomicU64,
    /// Row changes in those transactions (including snapshot rows).
    pub changes: AtomicU64,
    /// Tables whose snapshot chunk cannot be read, with the error.
    pub table_errors: Mutex<std::collections::BTreeMap<ObjectId, String>>,
}

impl SourceGauges {
    pub fn upstream(&self, wal_end: u64, server_time: i64) {
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_micros() as i64).unwrap_or(0);
        self.wal_end.fetch_max(wal_end, Ordering::Relaxed);
        self.server_time.store(server_time, Ordering::Relaxed);
        self.received_at.store(now, Ordering::Relaxed);
    }

    pub fn commit(&self, changes: usize) {
        self.commits.fetch_add(1, Ordering::Relaxed);
        self.changes.fetch_add(changes as u64, Ordering::Relaxed);
    }
}

/// The gauges of source `id` (created on first use).
pub fn gauges(shared: &crate::coord::api::Shared, id: ObjectId) -> Arc<SourceGauges> {
    shared.source_gauges.lock().entry(id).or_default().clone()
}

pub enum SourceCtl {
    AddTables(Vec<SourceTable>),
    Stop,
}

pub struct SourceHandle {
    tx: Sender<SourceCtl>,
    status: Arc<Mutex<String>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl SourceHandle {
    pub fn stop(mut self) {
        let _ = self.tx.send(SourceCtl::Stop);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
    pub fn status(&self) -> String {
        self.status.lock().clone()
    }
    pub fn add_tables(&self, tables: Vec<SourceTable>) {
        let _ = self.tx.send(SourceCtl::AddTables(tables));
    }
}

/// Start a source thread. `state` is the source's last logged progress
/// state; `resume_lsn` the LSN of its last logged commit.
pub fn start(
    id: ObjectId,
    name: String,
    def: SourceDef,
    tables: Vec<SourceTable>,
    state: String,
    resume_lsn: u64,
    engine: EngineHandle,
) -> Result<SourceHandle> {
    let (tx, rx) = crossbeam_channel::unbounded();
    let status = Arc::new(Mutex::new("starting".to_string()));
    let st = status.clone();
    let thread = std::thread::Builder::new().name(format!("ivm-source-{name}")).spawn(move || {
        postgres::run(id, name, def, tables, state, resume_lsn, engine, rx, st);
    })?;
    Ok(SourceHandle { tx, status, thread: Some(thread) })
}

/// A random 64-bit value, different in every call and every process (not
/// cryptographically strong; for identifiers that must not repeat).
pub(crate) fn random_u64() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    static CALLS: AtomicU64 = AtomicU64::new(0);
    // RandomState is seeded from the operating system's generator
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0));
    h.write_u32(std::process::id());
    h.write_u64(CALLS.fetch_add(1, Ordering::Relaxed));
    h.finish()
}

/// The replication slot of a dropped source, still to be dropped upstream.
/// Pending drops are kept in `Shared::slot_drops` and in checkpoints (and
/// re-derived from replayed `DROP SOURCE`s), so they are retried until they
/// succeed, across restarts: a slot left behind retains WAL upstream.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SlotDrop {
    pub conninfo: String,
    pub slot: String,
}

/// Drop a dropped source's slot: record it as pending and, with `start`
/// (not while replaying the log), start dropping it in the background.
pub fn drop_slot_async(shared: &Arc<crate::coord::api::Shared>, conninfo: String, slot: String, start: bool) {
    let d = SlotDrop { conninfo, slot };
    if shared.slot_drops.lock().insert(d.clone()) && start {
        spawn_slot_drop(shared.clone(), d);
    }
}

/// A new source uses `slot`: a pending drop of a slot by that name (of a
/// dropped source with the same explicit SLOT) must not drop it.
pub fn cancel_slot_drop(shared: &crate::coord::api::Shared, slot: &str) {
    shared.slot_drops.lock().retain(|d| d.slot != slot);
}

/// Start dropping every pending slot (after recovery).
pub fn resume_slot_drops(shared: &Arc<crate::coord::api::Shared>) {
    let pending: Vec<SlotDrop> = shared.slot_drops.lock().iter().cloned().collect();
    for d in pending {
        spawn_slot_drop(shared.clone(), d);
    }
}

fn spawn_slot_drop(shared: Arc<crate::coord::api::Shared>, d: SlotDrop) {
    let name = format!("ivm-slot-drop-{}", d.slot);
    let r = std::thread::Builder::new().name(name).spawn(move || drop_slot(&shared, &d));
    if let Err(e) = r {
        log::error!("cannot start a thread to drop a replication slot: {e}");
    }
}

/// Drop a slot, retrying (with a delay doubling up to 10 s, a warning at
/// attempts 1, 2, 4, 8, ...) until it is dropped or gone, the drop is
/// cancelled, or the engine shuts down (then the next start retries).
/// Retries also wait out the source's own session releasing the slot.
fn drop_slot(shared: &crate::coord::api::Shared, d: &SlotDrop) {
    let stopped = || shared.shutdown.load(Ordering::Acquire) || !shared.slot_drops.lock().contains(d);
    let mut delay = Duration::from_millis(250);
    for attempt in 1u64.. {
        if stopped() {
            return;
        }
        let r = pgproto::Config::parse(&d.conninfo).and_then(|c| pgproto::Client::connect_replication(&c)).and_then(|mut c| c.drop_slot(&d.slot, false));
        match r {
            Ok(()) => log::info!("dropped the replication slot \"{}\" of a dropped source", d.slot),
            Err(e) if e.sqlstate.as_deref() == Some("42704") => log::info!("the replication slot \"{}\" of a dropped source no longer exists", d.slot),
            Err(e) => {
                if attempt.is_power_of_two() {
                    log::warn!(
                        "dropping the replication slot \"{}\" of a dropped source failed (attempt {attempt}): {e}; \
                         retrying: until it is dropped it retains WAL upstream",
                        d.slot
                    );
                }
                let until = Instant::now() + delay;
                while Instant::now() < until {
                    if stopped() {
                        return;
                    }
                    std::thread::sleep(until.saturating_duration_since(Instant::now()).min(Duration::from_millis(100)));
                }
                delay = (delay * 2).min(Duration::from_secs(10));
                continue;
            }
        }
        shared.slot_drops.lock().remove(d);
        return;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coord::api::Shared;

    /// Pending slot drops are recorded, and cancelled when a new source
    /// takes the slot's name; a drop in progress stops when cancelled.
    #[test]
    fn slot_drops_are_recorded_and_cancelled() {
        let shared = Arc::new(Shared::default());
        // (an unreachable upstream: every attempt fails)
        let gone = "host=/nonexistent/ivm-test port=1".to_string();
        drop_slot_async(&shared, gone.clone(), "s1".into(), false);
        drop_slot_async(&shared, gone.clone(), "s2".into(), false);
        assert_eq!(shared.slot_drops.lock().len(), 2);
        let d = SlotDrop { conninfo: gone, slot: "s1".into() };
        let worker = {
            let (shared, d) = (shared.clone(), d.clone());
            std::thread::spawn(move || drop_slot(&shared, &d))
        };
        std::thread::sleep(Duration::from_millis(300));
        assert!(!worker.is_finished(), "retrying");
        cancel_slot_drop(&shared, "s1");
        let t0 = Instant::now();
        while !worker.is_finished() {
            assert!(t0.elapsed() < Duration::from_secs(5), "the cancelled drop stops");
            std::thread::sleep(Duration::from_millis(20));
        }
        let left: Vec<String> = shared.slot_drops.lock().iter().map(|d| d.slot.clone()).collect();
        assert_eq!(left, ["s2"]);
    }
}
