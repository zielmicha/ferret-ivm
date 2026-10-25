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
        if !rows.is_empty() {
            let _ = self.tx.send(SinkMsg::Data(gsn, rows.clone()));
        }
    }
    pub fn progress(&self, gsn: Gsn) {
        let _ = self.tx.send(SinkMsg::Progress(gsn));
    }
    pub fn status(&self) -> String {
        self.status.lock().clone()
    }
    pub fn stop(mut self) {
        let _ = self.tx.send(SinkMsg::Stop);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
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
