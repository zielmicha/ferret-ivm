//! Requests from sessions (and sources) to the engine thread.

pub use super::stream::{Chunk, Chunks};
use crate::error::Result;
use crate::log::LogRecord;
use crate::memory::Charge;
use crate::sql::parser::Statement;
use crate::sql::ParamSlot;
use crate::storage::Gsn;
use crate::types::{Row, SqlType};
use crossbeam_channel::{Receiver, Sender};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// Column of a result set.
#[derive(Clone, Debug)]
pub struct ResultColumn {
    pub name: String,
    pub ty: SqlType,
    pub typmod: i32,
}

/// Subscription events use the common change-feed protocol. Subscriptions
/// emit Data, Snapshot, Progress and Error; Behind and Stop are controls
/// reserved for sinks that can resynchronize after overflow.
pub use super::feed::ChangeEvent as SubEvent;

/// The changes of one node at one GSN as delivered to a subscription or a
/// sink: one copy shared by all of them (not one per consumer), with the
/// receiving consumer's charge on its queue's memory, which is released
/// when the consumer drops the event (or the queue holding it).
#[derive(Debug)]
pub struct SharedRows {
    rows: Arc<Vec<(Row, i64)>>,
    _charge: Charge,
}

impl SharedRows {
    /// Rows counted in no queue (e.g. a snapshot).
    pub fn new(rows: Vec<(Row, i64)>) -> SharedRows {
        SharedRows { rows: Arc::new(rows), _charge: Charge::none() }
    }

    pub fn shared(rows: &Arc<Vec<(Row, i64)>>, charge: Charge) -> SharedRows {
        SharedRows { rows: rows.clone(), _charge: charge }
    }
}

impl std::ops::Deref for SharedRows {
    type Target = Vec<(Row, i64)>;
    fn deref(&self) -> &Self::Target {
        &self.rows
    }
}

impl IntoIterator for SharedRows {
    type Item = (Row, i64);
    type IntoIter = std::vec::IntoIter<(Row, i64)>;
    /// The rows, copied unless this was the last holder (rows clone
    /// cheaply: their large values are reference counted).
    fn into_iter(self) -> Self::IntoIter {
        Arc::unwrap_or_clone(self.rows).into_iter()
    }
}

/// A statement result, with collected rows by default. Sessions consume
/// `StreamingResult` so that large queries stay bounded in memory.
pub enum ExecResult<R = Vec<Row>> {
    Query { columns: Vec<ResultColumn>, rows: R },
    Command { tag: String },
    Subscribe { columns: Vec<ResultColumn>, rx: Receiver<SubEvent>, cancel: Arc<AtomicBool>, progress: bool },
}

pub type StreamingResult = ExecResult<RowStream>;

/// Query rows, whether already available or arriving in bounded chunks.
/// The current chunk stays charged while a consumer (including a suspended
/// portal) holds it. Dropping the rows stops the reader at its next chunk.
pub struct RowStream {
    chunk: std::vec::IntoIter<Row>,
    _charge: Charge,
    rest: Option<Chunks<Vec<Row>>>,
}

impl From<Vec<Row>> for RowStream {
    fn from(rows: Vec<Row>) -> Self {
        Self::new(Chunk::new(rows, Charge::none()), None)
    }
}

impl RowStream {
    pub(crate) fn new(first: Chunk<Vec<Row>>, rest: Option<Chunks<Vec<Row>>>) -> Self {
        let (rows, charge) = first.into_parts();
        Self { chunk: rows.into_iter(), _charge: charge, rest }
    }

    /// Collect the rows, reusing the first chunk's allocation.
    pub fn collect(self) -> Result<Vec<Row>> {
        let mut rows: Vec<Row> = self.chunk.collect();
        if let Some(rest) = self.rest {
            for chunk in rest {
                rows.extend(chunk?.into_parts().0);
            }
        }
        Ok(rows)
    }
}

impl Iterator for RowStream {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(row) = self.chunk.next() {
                return Some(Ok(row));
            }
            // IntoIter keeps the Vec allocation even after its last row.
            self.chunk = Vec::new().into_iter();
            self._charge = Charge::none();
            match self.rest.as_mut()?.next() {
                Some(Ok(chunk)) => {
                    let (rows, charge) = chunk.into_parts();
                    (self.chunk, self._charge) = (rows.into_iter(), charge);
                }
                end => {
                    self.rest = None;
                    return end.and_then(Result::err).map(Err);
                }
            }
        }
    }
}

impl StreamingResult {
    pub fn collected(self) -> Result<ExecResult> {
        Ok(match self {
            ExecResult::Query { columns, rows } => ExecResult::Query { columns, rows: rows.collect()? },
            ExecResult::Command { tag } => ExecResult::Command { tag },
            ExecResult::Subscribe { columns, rx, cancel, progress } => ExecResult::Subscribe { columns, rx, cancel, progress },
        })
    }
}

impl<R> std::fmt::Debug for ExecResult<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecResult::Query { columns, .. } => write!(f, "Query({} cols)", columns.len()),
            ExecResult::Command { tag } => write!(f, "Command({tag})"),
            ExecResult::Subscribe { .. } => write!(f, "Subscribe"),
        }
    }
}

/// Per-session settings relevant to the engine.
#[derive(Clone, Debug, Default)]
pub struct SessionVars {
    /// "fresh" (default) or "latest".
    pub read_mode: String,
    /// Interrupts of the session's statements (None: never interrupted).
    pub interrupts: Option<Arc<Interrupts>>,
    /// The session's settings by lowercase name (what `SET` set, and the
    /// values the session started with), for current_setting(); others
    /// are the server's fixed settings.
    pub settings: Arc<std::collections::BTreeMap<String, String>>,
}

/// Interrupts of a session's statement, set by the session thread and
/// checked by the engine while the statement waits (a parked read, DML
/// queued behind another statement).
#[derive(Debug, Default)]
pub struct Interrupts {
    /// A CancelRequest for the running statement (reset when the next
    /// statement starts).
    pub cancel: AtomicBool,
    /// Nobody waits for the result any more: the client disconnected, or
    /// gave up waiting.
    pub gone: AtomicBool,
}

impl Interrupts {
    pub fn cancelled(&self) -> bool {
        self.cancel.load(Ordering::Acquire)
    }

    pub fn gone(&self) -> bool {
        self.gone.load(Ordering::Acquire)
    }
}

pub enum Request {
    Execute {
        stmt: Box<Statement>,
        params: Vec<ParamSlot>,
        vars: SessionVars,
        /// Logged external responses (e.g. discovered source schemas).
        extra: String,
        reply: Sender<Result<StreamingResult>>,
    },
    /// Result columns and parameter types of a statement, without running it.
    Describe { stmt: Box<Statement>, params: Vec<ParamSlot>, reply: Sender<Result<(Vec<ResultColumn>, Vec<SqlType>)>> },
    /// Append a record on behalf of a source (sources go straight to the
    /// sequencer; this is for engine-originated records).
    Log { rec: LogRecord },
    Checkpoint { reply: Sender<Result<Gsn>> },
    /// A sink's thread asks for its relation's contents, to resynchronize
    /// its target; they are sent through the sink's channel
    /// (`SubEvent::Snapshot`). `token` is the thread's feed generation.
    SinkSnapshot { sink: crate::sql::plan::ObjectId, token: u64 },
    /// Introspection snapshot for the web UI (see `coord::inspect`).
    Inspect(super::inspect::InspectRequest),
}

/// State shared between threads.
#[derive(Default)]
pub struct Shared {
    /// Last GSN assigned (durable) by the sequencer.
    pub durable_gsn: AtomicU64,
    /// Last GSN applied by the engine.
    pub processed_gsn: AtomicU64,
    /// Batches of records the sequencer has appended to the input log, each
    /// made durable with one write and one `fdatasync`.
    pub log_batches: AtomicU64,
    /// Per-source LSN frontier: every commit at or below it has been logged.
    pub source_lsn: Mutex<HashMap<u64, u64>>,
    /// Sources asked to request a keepalive (to advance the LSN frontier).
    pub want_keepalive: AtomicBool,
    /// Reads are parked until a source frontier passes some LSN.
    pub lsn_waiters: AtomicBool,
    /// While `lsn_waiters`: the smallest LSN they wait for. A source whose
    /// frontier passes it wakes the engine (not on every advance).
    pub lsn_wanted: AtomicU64,
    /// Upstream position and ingest counters per source (web UI).
    pub source_gauges: Mutex<HashMap<u64, Arc<crate::source::SourceGauges>>>,
    /// Replication slots of dropped sources still to be dropped (saved in
    /// checkpoints).
    pub slot_drops: Mutex<std::collections::BTreeSet<crate::source::SlotDrop>>,
    pub shutdown: AtomicBool,
    /// The engine or the sequencer thread stopped with an error (including
    /// a failed final checkpoint).
    pub failed: AtomicBool,
    /// Stop without the final checkpoint (simulated crash, for tests).
    pub crash: AtomicBool,
    /// The memory budget, and the memory of queued records and changes.
    pub mem: crate::memory::MemBudget,
    /// Where sources spool transactions too large to hold in memory (set
    /// once the log is open; without it they hold them).
    pub spool: std::sync::OnceLock<crate::log::SpoolDir>,
}

impl Shared {
    pub fn durable(&self) -> Gsn {
        self.durable_gsn.load(Ordering::Acquire)
    }
    pub fn processed(&self) -> Gsn {
        self.processed_gsn.load(Ordering::Acquire)
    }
}
