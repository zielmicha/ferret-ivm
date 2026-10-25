//! Requests from sessions (and sources) to the engine thread.

use crate::error::{Result, SqlError};
use crate::log::LogRecord;
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

#[derive(Debug)]
pub enum SubEvent {
    /// Changes at a GSN (row, diff).
    Data { gsn: Gsn, rows: Vec<(Row, i64)> },
    /// All changes up to and including `gsn` have been delivered.
    Progress { gsn: Gsn },
    Error(SqlError),
}

pub enum ExecResult {
    Query { columns: Vec<ResultColumn>, rows: Vec<Row> },
    Command { tag: String },
    Subscribe { columns: Vec<ResultColumn>, rx: Receiver<SubEvent>, cancel: Arc<AtomicBool>, progress: bool },
}

impl std::fmt::Debug for ExecResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecResult::Query { columns, rows } => write!(f, "Query({} cols, {} rows)", columns.len(), rows.len()),
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
}

pub enum Request {
    Execute {
        stmt: Box<Statement>,
        params: Vec<ParamSlot>,
        vars: SessionVars,
        /// Logged external responses (e.g. discovered source schemas).
        extra: String,
        reply: Sender<Result<ExecResult>>,
    },
    /// Result columns and parameter types of a statement, without running it.
    Describe { stmt: Box<Statement>, params: Vec<ParamSlot>, reply: Sender<Result<(Vec<ResultColumn>, Vec<SqlType>)>> },
    /// Append a record on behalf of a source (sources go straight to the
    /// sequencer; this is for engine-originated records).
    Log { rec: LogRecord },
    Checkpoint { reply: Sender<Result<Gsn>> },
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
    /// Per-source LSN frontier: every commit at or below it has been logged.
    pub source_lsn: Mutex<HashMap<u64, u64>>,
    /// Sources asked to request a keepalive (to advance the LSN frontier).
    pub want_keepalive: AtomicBool,
    /// Reads are parked until a source frontier passes some LSN.
    pub lsn_waiters: AtomicBool,
    /// Upstream position and ingest counters per source (web UI).
    pub source_gauges: Mutex<HashMap<u64, Arc<crate::source::SourceGauges>>>,
    pub shutdown: AtomicBool,
}

impl Shared {
    pub fn durable(&self) -> Gsn {
        self.durable_gsn.load(Ordering::Acquire)
    }
    pub fn processed(&self) -> Gsn {
        self.processed_gsn.load(Ordering::Acquire)
    }
}
