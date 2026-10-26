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
    /// Rows of the relation as of the GSN, which the sink asked for to
    /// resynchronize its target (`Request::SinkSnapshot`): one chunk of
    /// them. The changes sent before the first chunk are at or before the
    /// GSN, those after the last one after it.
    Snapshot(Gsn, SharedRows),
    /// The end of a snapshot; an error if it could not be read whole.
    SnapshotEnd(Gsn, Option<String>),
    Stop,
}

/// Bytes of a message in the sink's channel.
const MSG_BYTES: usize = std::mem::size_of::<SinkMsg>() + 8;

/// Bounds of a sink's writes and memory.
#[derive(Clone, Copy, Debug)]
pub struct SinkLimits {
    /// Most SQL text sent to the target in one message (a single statement
    /// can be larger): a transaction is written in as many messages as it
    /// needs (PostgreSQL rejects messages over 1 GB).
    pub message_bytes: usize,
    /// Most changes (estimated encoded bytes) a sink keeps while it cannot
    /// write them; beyond it they are dropped and the target is
    /// resynchronized from a fresh snapshot once it can be written again.
    pub pending_bytes: usize,
}

impl Default for SinkLimits {
    fn default() -> Self {
        SinkLimits { message_bytes: 8 << 20, pending_bytes: 256 << 20 }
    }
}

pub struct SinkHandle {
    pub node: NodeId,
    /// Identifies this sink's thread in its snapshot requests (a replaced
    /// sink's thread can still be running).
    pub token: u64,
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
    /// The sink's channel (to send it a snapshot).
    pub fn sender(&self) -> Sender<SinkMsg> {
        self.tx.clone()
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

/// Start a sink that receives the changes of `node` after `gsn` (the
/// engine's last applied GSN). It asks the engine for a snapshot when its
/// target has to be resynchronized.
#[allow(clippy::too_many_arguments)]
pub fn start(
    id: ObjectId,
    name: String,
    def: SinkDef,
    node: NodeId,
    columns: Vec<Column>,
    gsn: Gsn,
    limits: SinkLimits,
    engine: EngineHandle,
) -> Result<SinkHandle> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static TOKENS: AtomicU64 = AtomicU64::new(1);
    let token = TOKENS.fetch_add(1, Ordering::Relaxed);
    let (tx, rx) = crossbeam_channel::unbounded();
    let queue = Meter::child(&engine.shared.mem.outputs);
    let status = Arc::new(Mutex::new("starting".to_string()));
    let st = status.clone();
    let thread = std::thread::Builder::new().name(format!("ivm-sink-{name}")).spawn(move || {
        let ask = move || engine.request_sink_snapshot(id, token);
        postgres::run(name, def, columns, gsn, limits, rx, st, &ask);
    })?;
    Ok(SinkHandle { node, token, tx, status, thread: Some(thread), queue, behind: false })
}
