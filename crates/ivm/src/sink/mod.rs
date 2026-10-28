//! Sinks: deliver a view's changes to an external system, idempotently by GSN.

pub mod postgres;

use crate::catalog::SinkDef;
use crate::coord::api::{Chunks, SharedRows};
use crate::coord::EngineHandle;
use crate::dataflow::{Delta, NodeId};
use crate::error::Result;
use crate::memory::{Charge, Meter};
use crate::sql::plan::ObjectId;
use crate::storage::Gsn;
use crate::types::Column;
use crossbeam_channel::Sender;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Messages to a sink thread. Changes and progress marks hold a charge on
/// the sink's queue until the thread drops them: changes once written,
/// which may take long while the target is unreachable.
pub enum SinkMsg {
    Data(Gsn, SharedRows),
    Progress(Gsn, Charge),
    /// The relation's rows as of the GSN, which the sink asked for to
    /// resynchronize its target (`Request::SinkSnapshot`), read in chunks
    /// from the stream as the sink writes them (each chunk counted in the
    /// sink's queue until written). The changes sent before it are at or
    /// before the GSN, those after it after it.
    Snapshot(Gsn, Chunks<Delta>),
    /// The sink's queue reached its limit: the changes after those sent so
    /// far are not sent, and only a snapshot (which the sink asks for)
    /// brings its target up to date again.
    Behind,
    Stop,
}

/// Bytes of a message in the sink's channel.
const MSG_BYTES: usize = std::mem::size_of::<SinkMsg>() + 8;

/// Bounds of a sink's writes.
#[derive(Clone, Copy, Debug)]
pub struct SinkLimits {
    /// Most SQL text sent to the target in one message (a single statement
    /// can be larger): a transaction is written in as many messages as it
    /// needs (PostgreSQL rejects messages over 1 GB).
    pub message_bytes: usize,
}

impl Default for SinkLimits {
    fn default() -> Self {
        SinkLimits { message_bytes: 8 << 20 }
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
    /// Set when the sink is asked to stop (a snapshot being written stops).
    stopping: Arc<AtomicBool>,
    /// Memory of the changes (and snapshot chunks) queued for the sink and
    /// not written yet: the one bound on what a sink holds.
    queue: Arc<Meter>,
    /// The queue would have exceeded its limit: the sink was told
    /// (`SinkMsg::Behind`) and is sent nothing more (not even progress,
    /// which would claim that the target holds the changes it missed) until
    /// it asks for a snapshot (`resync`).
    behind: bool,
}

impl SinkHandle {
    /// Queue the changes of a GSN (`bytes` in memory; a GSN's changes may
    /// come in several chunks), unless the queue would exceed `limit`
    /// bytes: then the sink falls behind and false is returned.
    pub fn send(&mut self, gsn: Gsn, rows: &Arc<Delta>, bytes: usize, limit: usize) -> bool {
        if self.behind {
            return false;
        }
        if self.queue.used() + bytes + MSG_BYTES > limit {
            self.behind = true;
            let _ = self.tx.send(SinkMsg::Behind);
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
    pub fn behind(&self) -> bool {
        self.behind
    }
    /// The memory of what is queued for the sink (snapshot chunks are
    /// charged to it).
    pub fn queue(&self) -> &Arc<Meter> {
        &self.queue
    }
    /// Send the sink the snapshot it asked for, as of `gsn`: it is current
    /// from there on, and is sent the changes after it.
    pub fn resync(&mut self, gsn: Gsn, rows: Chunks<Delta>) {
        self.behind = false;
        let _ = self.tx.send(SinkMsg::Snapshot(gsn, rows));
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
        self.stopping.store(true, Ordering::Release);
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
    use std::sync::atomic::AtomicU64;
    static TOKENS: AtomicU64 = AtomicU64::new(1);
    let token = TOKENS.fetch_add(1, Ordering::Relaxed);
    let (tx, rx) = crossbeam_channel::unbounded();
    let queue = Meter::child(&engine.shared.mem.outputs);
    let status = Arc::new(Mutex::new("starting".to_string()));
    let stopping = Arc::new(AtomicBool::new(false));
    let (st, sp) = (status.clone(), stopping.clone());
    let thread = std::thread::Builder::new().name(format!("ivm-sink-{name}")).spawn(move || {
        let ask = move || engine.request_sink_snapshot(id, token);
        postgres::run(name, def, columns, gsn, limits, rx, st, sp, &ask);
    })?;
    Ok(SinkHandle { node, token, tx, status, thread: Some(thread), stopping, queue, behind: false })
}
