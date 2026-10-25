//! Sinks: deliver a view's changes to an external system, idempotently by GSN.

pub mod postgres;

use crate::catalog::SinkDef;
use crate::coord::EngineHandle;
use crate::dataflow::{Delta, NodeId};
use crate::error::Result;
use crate::sql::plan::ObjectId;
use crate::storage::Gsn;
use crate::types::Column;
use crossbeam_channel::Sender;
use parking_lot::Mutex;
use std::sync::Arc;

pub enum SinkMsg {
    Data(Gsn, Delta),
    Progress(Gsn),
    Stop,
}

pub struct SinkHandle {
    pub node: NodeId,
    tx: Sender<SinkMsg>,
    status: Arc<Mutex<String>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl SinkHandle {
    pub fn send(&self, gsn: Gsn, rows: &Delta) {
        if rows.is_empty() {
            self.progress(gsn);
        } else {
            let _ = self.tx.send(SinkMsg::Data(gsn, rows.clone()));
        }
    }
    pub fn progress(&self, gsn: Gsn) {
        let _ = self.tx.send(SinkMsg::Progress(gsn));
    }
    pub fn status(&self) -> String {
        self.status.lock().clone()
    }
    /// Ask the sink thread to stop, without waiting for it: it may be in a
    /// connection attempt or a long write, which the engine thread must not
    /// wait for. Returns the thread, to join where waiting is fine. (A thread
    /// that outlives its sink cannot undo a newer sink of the same name:
    /// writers take turns on the progress row, which only moves forward, and
    /// one whose progress is outdated cannot write; see `postgres::Writer`.)
    pub fn stop(mut self) -> Option<std::thread::JoinHandle<()>> {
        let _ = self.tx.send(SinkMsg::Stop);
        self.thread.take()
    }
}

#[allow(clippy::too_many_arguments)]
pub fn start(
    id: ObjectId,
    name: String,
    def: SinkDef,
    node: NodeId,
    columns: Vec<Column>,
    snapshot: Delta,
    gsn: Gsn,
    _engine: EngineHandle,
) -> Result<SinkHandle> {
    let (tx, rx) = crossbeam_channel::unbounded();
    let status = Arc::new(Mutex::new("starting".to_string()));
    let st = status.clone();
    let thread = std::thread::Builder::new().name(format!("ivm-sink-{name}")).spawn(move || {
        postgres::run(id, name, def, columns, snapshot, gsn, rx, st);
    })?;
    Ok(SinkHandle { node, tx, status, thread: Some(thread) })
}
