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
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

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

/// Drop a replication slot in the background (best effort).
pub fn drop_slot_async(conninfo: String, slot: String) {
    std::thread::spawn(move || {
        for _ in 0..20 {
            match pgproto::Config::parse(&conninfo).and_then(|c| pgproto::Client::connect_replication(&c)) {
                Ok(mut c) => match c.drop_slot(&slot, false) {
                    Ok(()) => return,
                    Err(e) if e.sqlstate.as_deref() == Some("42704") => return,
                    Err(_) => {}
                },
                Err(_) => {}
            }
            std::thread::sleep(std::time::Duration::from_millis(250));
        }
    });
}
