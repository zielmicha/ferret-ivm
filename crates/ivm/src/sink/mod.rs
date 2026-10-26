//! Sinks: deliver a view's changes to an external system, idempotently by GSN.

pub mod postgres;

use crate::catalog::SinkDef;
use crate::coord::api::SharedRows;
use crate::coord::EngineHandle;
use crate::dataflow::{Delta, NodeId};
use crate::error::Result;
use crate::memory::{Charge, Meter};
use crate::sql::plan::ObjectId;
use crate::storage::Gsn;
use crate::types::Column;
use crossbeam_channel::Sender;
use parking_lot::Mutex;
use std::sync::Arc;

/// Messages to a sink thread. Changes and progress marks hold a charge on
/// the sink's queue until the thread drops them: changes once written,
/// which may take long while the target is unreachable.
pub enum SinkMsg {
    Data(Gsn, SharedRows),
    Progress(Gsn, Charge),
    Stop,
}

/// Bytes of a message in the sink's channel.
const MSG_BYTES: usize = std::mem::size_of::<SinkMsg>() + 8;

pub struct SinkHandle {
    pub node: NodeId,
    tx: Sender<SinkMsg>,
    status: Arc<Mutex<String>>,
    thread: Option<std::thread::JoinHandle<()>>,
    /// Memory of the changes queued for the sink and not written yet.
    queue: Arc<Meter>,
    /// The queue would have exceeded its limit: the sink is sent nothing
    /// more (not even progress, which would claim that the target holds
    /// the changes it missed) and is restarted from a new snapshot once it
    /// has written what it holds (`Engine::deliver`).
    behind: bool,
}

impl SinkHandle {
    /// Queue the changes of a GSN (`bytes` in memory), unless the queue
    /// would exceed `limit` bytes: then the sink falls behind and false is
    /// returned.
    pub fn send(&mut self, gsn: Gsn, rows: &Arc<Delta>, bytes: usize, limit: usize) -> bool {
        if self.behind || self.queue.used() + bytes + MSG_BYTES > limit {
            self.behind = true;
            return false;
        }
        let _ = self.tx.send(SinkMsg::Data(gsn, SharedRows::shared(rows, self.queue.charge(bytes + MSG_BYTES))));
        true
    }
    /// The relation did not change at `gsn` (nothing for a sink that is behind).
    pub fn progress(&self, gsn: Gsn) {
        if !self.behind {
            let _ = self.tx.send(SinkMsg::Progress(gsn, self.queue.charge(MSG_BYTES)));
        }
    }
    /// Is the sink behind, and has it written (or dropped) everything it
    /// was sent, so that it can be restarted?
    pub fn behind_and_idle(&self) -> bool {
        self.behind && self.queue.used() == 0
    }
    pub fn behind(&self) -> bool {
        self.behind
    }
    pub fn status(&self) -> String {
        let st = self.status.lock().clone();
        if self.behind { format!("behind ({st})") } else { st }
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
    engine: EngineHandle,
) -> Result<SinkHandle> {
    let (tx, rx) = crossbeam_channel::unbounded();
    let queue = Meter::child(&engine.shared.mem.outputs);
    let status = Arc::new(Mutex::new("starting".to_string()));
    let st = status.clone();
    let thread = std::thread::Builder::new().name(format!("ivm-sink-{name}")).spawn(move || {
        postgres::run(id, name, def, columns, snapshot, gsn, rx, st);
    })?;
    Ok(SinkHandle { node, tx, status, thread: Some(thread), queue, behind: false })
}
