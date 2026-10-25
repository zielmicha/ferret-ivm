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
