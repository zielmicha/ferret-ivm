//! Sinks: deliver a view's changes to an external system, idempotently by GSN.

pub mod postgres;

use crate::catalog::SinkDef;
use crate::coord::feed::{ChangeEvent, FeedId};
use crate::coord::EngineHandle;
use crate::error::Result;
use crate::sql::plan::ObjectId;
use crate::storage::Gsn;
use crate::types::Column;
use crossbeam_channel::Receiver;
use parking_lot::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

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

/// Only the external writer's lifecycle lives here. The coordinator's
/// feed registry owns delivery, queue limits and generation fencing.
pub struct SinkHandle {
    pub(crate) feed: FeedId,
    status: Arc<Mutex<String>>,
    /// Joined only where waiting is safe, after the feed has been stopped.
    pub(crate) thread: std::thread::JoinHandle<()>,
}

impl SinkHandle {
    pub fn status(&self) -> String {
        self.status.lock().clone()
    }
}

/// Start the network consumer of an already registered change feed.
#[allow(clippy::too_many_arguments)]
pub(crate) fn start(
    id: ObjectId,
    name: String,
    def: SinkDef,
    feed: FeedId,
    columns: Vec<Column>,
    gsn: Gsn,
    limits: SinkLimits,
    engine: EngineHandle,
    rx: Receiver<ChangeEvent>,
    stopping: Arc<AtomicBool>,
) -> Result<SinkHandle> {
    let status = Arc::new(Mutex::new("starting".to_string()));
    let st = status.clone();
    let thread = std::thread::Builder::new().name(format!("ivm-sink-{name}")).spawn(move || {
        let ask = move || engine.request_sink_snapshot(id, feed);
        postgres::run(name, def, columns, gsn, limits, rx, st, stopping, &ask);
    })?;
    Ok(SinkHandle { feed, status, thread })
}
