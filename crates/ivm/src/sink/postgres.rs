//! Postgres write-back sink.
//!
//! The target table mirrors the view. Each batch of GSNs is applied in one
//! transaction together with the sink's progress row, so delivery is exactly
//! once: after (re)connecting, the sink reads its committed progress and
//! skips every GSN at or below it (a batch whose commit acknowledgement was
//! lost is not applied twice). A target behind the changes the sink has is
//! resynchronized from a snapshot the sink asks the engine for. A
//! transaction is sent in messages of bounded size, and the changes a sink
//! keeps while it cannot write are bounded too (beyond the bound it
//! resynchronizes instead).

use super::{SinkLimits, SinkMsg};
use crate::catalog::SinkDef;
use crate::coord::api::{Chunks, SharedRows};
use crate::dataflow::Delta;
use crate::error::{Result, SqlError};
use crate::storage::Gsn;
use crate::types::text::format_datum;
use crate::types::{format_type, Column, Datum, Equality, Row};
use crossbeam_channel::{Receiver, RecvTimeoutError};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// While nothing is pending, the progress row is advanced to the last GSN
/// delivered at most this often (and when the sink stops): after a restart,
/// a target that is that far needs no resynchronization.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(5);

/// Delay before retrying after a failed connection or write: doubles from
/// the minimum up to the maximum while attempts keep failing, and is reset
/// by a write that succeeds (or a connection, while no write has failed).
const RETRY_MIN: Duration = Duration::from_millis(100);
const RETRY_MAX: Duration = Duration::from_secs(10);

/// Rows of pending changes written per transaction (whole GSNs, at least
/// one): a backlog is caught up in several transactions.
const FLUSH_ROWS: usize = 50_000;

/// Rows per INSERT statement (fewer when they would exceed a message).
const INSERT_ROWS: usize = 500;

fn lit(d: &Datum, c: &Column) -> String {
    if d.is_null() {
        return "NULL".into();
    }
    format!("'{}'::{}", format_datum(d).replace('\'', "''"), format_type(&c.ty.ty, c.ty.typmod))
}

/// A condition that a target row's column `c` holds `d` itself, not just a
/// value equal to it: a keyless sink deletes one row matching every column
/// per retracted row, and one holding another representation of an equal
/// value (`'30 days'` for `'1 mon'`, `1.0` for `1.00`, `-0` for `0`) would
/// leave the retracted representation behind. Values are compared with `=`
/// (NULL with IS NULL), which an index on the column can serve (IS NOT
/// DISTINCT FROM cannot), and where `=` is coarser than identity or missing
/// (`ColumnType::equality`) by their text as well, which tells the
/// representations apart under any session settings.
fn same_value(c: &Column, d: &Datum) -> String {
    let col = qident(&c.name);
    if d.is_null() {
        return format!("{col} IS NULL");
    }
    let v = lit(d, c);
    match c.ty.equality() {
        Equality::Identity => format!("{col} = {v}"),
        // (the cast of a character value to text drops its trailing
        // spaces; that of an array of them keeps them)
        Equality::TrailingSpaces if c.ty.ty.elem().is_none() => format!("{col} = {v} AND octet_length({col}) = octet_length({v})"),
        Equality::Representation | Equality::TrailingSpaces => format!("{col} = {v} AND {col}::text = ({v})::text"),
        Equality::None => format!("{col}::text = ({v})::text"),
    }
}

fn qident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn qtable(t: &str) -> String {
    t.split('.').map(qident).collect::<Vec<_>>().join(".")
}

/// What a write transaction requires of the committed progress (see
/// `Writer::begin`).
#[derive(Clone, Copy)]
enum Check {
    /// This writer's progress: it applies changes on top of it.
    Is(Gsn),
    /// At most this writer's snapshot GSN: it rewrites the target.
    NotAfter(Gsn),
}

struct Writer {
    client: pgproto::Client,
    sink: String,
    table: String,
    key: Option<Vec<usize>>,
    columns: Vec<Column>,
    /// `INSERT INTO <table> (<columns>) VALUES ` and what follows the
    /// values (`ON CONFLICT ...`).
    insert_head: String,
    insert_tail: String,
    /// Statements of the open transaction not sent yet.
    sql: Packer,
}

/// Statements packed into messages of at most `max` bytes
/// (`SinkLimits::message_bytes`; a longer statement is a message alone).
struct Packer {
    max: usize,
    buf: String,
}

impl Packer {
    /// Add a statement; returns the message to send before it when the
    /// statement does not fit in the current one.
    fn push(&mut self, stmt: &str) -> Option<String> {
        let full = (!self.buf.is_empty() && self.buf.len() + stmt.len() + 1 > self.max).then(|| self.take());
        self.buf.push_str(stmt);
        self.buf.push(';');
        full
    }

    fn take(&mut self) -> String {
        std::mem::replace(&mut self.buf, String::with_capacity(self.max.min(1 << 20)))
    }
}

/// Builds INSERT statements of at most `INSERT_ROWS` rows, fewer when a
/// statement would exceed `max` bytes.
struct Inserts<'a> {
    head: &'a str,
    tail: &'a str,
    max: usize,
    stmt: String,
    n: usize,
}

impl<'a> Inserts<'a> {
    fn new(head: &'a str, tail: &'a str, max: usize) -> Inserts<'a> {
        Inserts { head, tail, max, stmt: String::new(), n: 0 }
    }

    /// Add a row's values (`v1, v2, ...`); returns a finished statement
    /// when the row starts the next one.
    fn add(&mut self, vals: &str) -> Option<String> {
        let full = (self.n > 0 && (self.n == INSERT_ROWS || self.stmt.len() + vals.len() + self.tail.len() + 4 > self.max)).then(|| self.finish()).flatten();
        self.stmt.push_str(if self.n == 0 { self.head } else { ", " });
        self.stmt.push('(');
        self.stmt.push_str(vals);
        self.stmt.push(')');
        self.n += 1;
        full
    }

    fn finish(&mut self) -> Option<String> {
        if self.n == 0 {
            return None;
        }
        self.n = 0;
        self.stmt.push_str(self.tail);
        Some(std::mem::take(&mut self.stmt))
    }
}

impl Writer {
    fn connect(name: &str, def: &SinkDef, columns: &[Column], max_message: usize) -> Result<Writer> {
        let cfg = pgproto::Config::parse(&def.conninfo)?;
        let mut client = pgproto::Client::connect(&cfg)?;
        client.simple_query("CREATE TABLE IF NOT EXISTS ivm_sink_progress (sink text PRIMARY KEY, gsn bigint NOT NULL)")?;
        let key = match &def.key {
            Some(k) => {
                let mut v = Vec::new();
                for n in k {
                    v.push(columns.iter().position(|c| &c.name == n).ok_or_else(|| {
                        crate::error::SqlError::new(crate::error::codes::UNDEFINED_COLUMN, format!("sink key column \"{n}\" does not exist"))
                    })?);
                }
                Some(v)
            }
            None => None,
        };
        let cols = columns
            .iter()
            .map(|c| format!("{} {}", qident(&c.name), format_type(&c.ty.ty, c.ty.typmod)))
            .collect::<Vec<_>>()
            .join(", ");
        let pk = match &key {
            Some(k) => format!(", PRIMARY KEY ({})", k.iter().map(|i| qident(&columns[*i].name)).collect::<Vec<_>>().join(", ")),
            None => String::new(),
        };
        client.simple_query(&format!("CREATE TABLE IF NOT EXISTS {} ({cols}{pk})", qtable(&def.table)))?;
        let table = qtable(&def.table);
        let names = columns.iter().map(|c| qident(&c.name)).collect::<Vec<_>>().join(", ");
        let insert_head = format!("INSERT INTO {table} ({names}) VALUES ");
        let insert_tail = match &key {
            Some(k) => {
                let kc = k.iter().map(|i| qident(&columns[*i].name)).collect::<Vec<_>>().join(", ");
                let upd = columns
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| !k.contains(i))
                    .map(|(_, c)| format!("{0} = EXCLUDED.{0}", qident(&c.name)))
                    .collect::<Vec<_>>()
                    .join(", ");
                if upd.is_empty() { format!(" ON CONFLICT ({kc}) DO NOTHING") } else { format!(" ON CONFLICT ({kc}) DO UPDATE SET {upd}") }
            }
            None => String::new(),
        };
        let sql = Packer { max: max_message, buf: String::new() };
        Ok(Writer { client, sink: name.to_string(), table, key, columns: columns.to_vec(), insert_head, insert_tail, sql })
    }

    fn committed_gsn(&mut self) -> Result<Option<Gsn>> {
        let r = self.client.query(&format!(
            "SELECT gsn FROM ivm_sink_progress WHERE sink = '{}'",
            self.sink.replace('\'', "''")
        ))?;
        Ok(r.rows.first().and_then(|row| row[0].as_ref()).and_then(|s| s.parse().ok()))
    }

    /// The progress upsert; the row only moves forward.
    fn progress_sql(&self, gsn: Gsn) -> String {
        format!(
            "INSERT INTO ivm_sink_progress (sink, gsn) VALUES ('{}', {gsn}) \
             ON CONFLICT (sink) DO UPDATE SET gsn = GREATEST(ivm_sink_progress.gsn, EXCLUDED.gsn)",
            self.sink.replace('\'', "''")
        )
    }

    /// Open a write transaction holding the lock of the sink's progress row,
    /// so that the writers of a sink take turns: a sink thread can outlive
    /// its sink (it is not waited for, and may be in a long write) while a
    /// new sink of the same name starts. The row must pass `check` (or be
    /// missing): otherwise another writer moved it, and this writer's
    /// changes, made for another state of the target, must not land.
    fn begin(&mut self, check: Check) -> Result<()> {
        self.sql.buf.clear();
        let r = self.client.simple_query(&format!(
            "BEGIN; SELECT gsn FROM ivm_sink_progress WHERE sink = '{}' FOR UPDATE",
            self.sink.replace('\'', "''")
        ));
        let r = match r {
            Ok(r) => r,
            Err(e) => {
                let _ = self.client.simple_query("ROLLBACK");
                return Err(e.into());
            }
        };
        let current: Option<Gsn> = r.iter().find_map(|q| q.rows.first()).and_then(|row| row[0].as_deref()).and_then(|s| s.parse().ok());
        let moved = match (current, check) {
            (None, _) => None,
            (Some(c), Check::Is(g)) => (c != g).then(|| format!("sink progress moved to gsn {c} by another writer (this one is at {g})")),
            (Some(c), Check::NotAfter(g)) => {
                (c > g).then(|| format!("sink progress is at gsn {c}, after this writer's snapshot at {g}: another writer is newer"))
            }
        };
        if let Some(m) = moved {
            let _ = self.client.simple_query("ROLLBACK");
            return Err(crate::error::SqlError::new(crate::error::codes::OBJECT_NOT_IN_PREREQUISITE_STATE, m));
        }
        Ok(())
    }

    /// Add a statement to the open transaction (sending those before it
    /// when the message is full).
    fn push(&mut self, stmt: &str) -> Result<()> {
        match self.sql.push(stmt) {
            Some(msg) => self.send(&msg),
            None => Ok(()),
        }
    }

    /// Send a message of the open transaction; on an error, roll back.
    fn send(&mut self, msg: &str) -> Result<()> {
        if let Err(e) = self.client.simple_query(msg) {
            self.abort();
            return Err(e.into());
        }
        Ok(())
    }

    /// Roll back the open transaction.
    fn abort(&mut self) {
        self.sql.buf.clear();
        let _ = self.client.simple_query("ROLLBACK");
    }

    /// End the open transaction: the progress row, then COMMIT.
    fn commit(&mut self, progress: Gsn) -> Result<()> {
        let p = self.progress_sql(progress);
        self.push(&p)?;
        self.push("COMMIT")?;
        let msg = self.sql.take();
        self.send(&msg)
    }

    /// Add INSERT statements for `rows` to the open transaction.
    fn insert<'r>(&mut self, rows: impl Iterator<Item = &'r Row>) -> Result<()> {
        let (head, tail) = (std::mem::take(&mut self.insert_head), std::mem::take(&mut self.insert_tail));
        let mut ins = Inserts::new(&head, &tail, self.sql.max);
        let mut r = Ok(());
        for row in rows {
            let vals = row.iter().zip(&self.columns).map(|(d, c)| lit(d, c)).collect::<Vec<_>>().join(", ");
            if let Some(stmt) = ins.add(&vals) {
                r = self.push(&stmt);
                if r.is_err() {
                    break;
                }
            }
        }
        if r.is_ok()
            && let Some(stmt) = ins.finish()
        {
            r = self.push(&stmt);
        }
        (self.insert_head, self.insert_tail) = (head, tail);
        r
    }

    fn delete_sql(&self, r: &Row) -> String {
        match &self.key {
            // Keys are unique as `=` compares them (the target's primary
            // key), which only the retracted row's own key matches: the
            // engine keys by `=` too, and CREATE SINK refuses columns whose
            // values it tells apart where `=` does not (`check_sink`).
            Some(k) => {
                let cond = k
                    .iter()
                    .map(|i| format!("{} = {}", qident(&self.columns[*i].name), lit(&r[*i], &self.columns[*i])))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                format!("DELETE FROM {} WHERE {cond}", self.table)
            }
            None => {
                let cond = self.columns.iter().zip(r).map(|(c, d)| same_value(c, d)).collect::<Vec<_>>().join(" AND ");
                format!("DELETE FROM {0} WHERE ctid = (SELECT ctid FROM {0} WHERE {cond} LIMIT 1)", self.table)
            }
        }
    }

    /// Open the transaction that replaces the target's contents with a
    /// snapshot at `gsn` (its rows follow, `insert`; then `commit(gsn)`).
    fn begin_resync(&mut self, gsn: Gsn) -> Result<()> {
        // (after a write of another writer still in progress: its rows go too)
        self.begin(Check::NotAfter(gsn))?;
        let del = format!("DELETE FROM {}", self.table);
        self.push(&del)
    }

    /// Record that the target is as of `gsn` (nothing pending; the
    /// committed progress is `applied`).
    fn save_progress(&mut self, gsn: Gsn, applied: Gsn) -> Result<()> {
        self.begin(Check::Is(applied))?;
        self.commit(gsn)
    }

    /// Apply a batch and record that the target is as of `progress` (at
    /// least the batch's last GSN; the committed progress is `applied`).
    fn apply(&mut self, batch: &[(Gsn, SharedRows)], progress: Gsn, applied: Gsn) -> Result<()> {
        self.begin(Check::Is(applied))?;
        for (_, delta) in batch {
            // retractions first, then insertions (an update is -old +new)
            for (r, w) in delta.iter().filter(|(_, w)| *w < 0) {
                let del = self.delete_sql(r);
                for _ in 0..(-*w) {
                    self.push(&del)?;
                }
            }
            self.insert(expand(delta))?;
        }
        self.commit(progress)
    }
}

/// The rows of a delta's insertions, each repeated by its multiplicity.
fn expand(delta: &Delta) -> impl Iterator<Item = &Row> {
    delta.iter().filter(|(_, w)| *w > 0).flat_map(|(r, w)| std::iter::repeat_n(r, *w as usize))
}

/// Where a resynchronization of the target stands.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Resync {
    /// None in progress.
    Idle,
    /// The engine was asked for a snapshot.
    Asked,
}

struct Sink<'a> {
    name: String,
    def: SinkDef,
    columns: Vec<Column>,
    limits: SinkLimits,
    rx: Receiver<SinkMsg>,
    status: Arc<Mutex<String>>,
    /// Ask the engine for a snapshot (`SinkMsg::Snapshot`).
    ask: &'a dyn Fn(),
    writer: Option<Writer>,
    /// Changes received and not written yet, in GSN order (a GSN's changes
    /// may come in several chunks). Each holds its charge on the sink's
    /// queue, which the engine bounds: beyond it, the sink falls behind.
    pending: VecDeque<(Gsn, SharedRows)>,
    pending_rows: usize,
    /// GSNs at or below this are already in the target.
    applied: Option<Gsn>,
    /// Every change after this GSN has been received (the engine's GSN when
    /// the sink started, then the GSN of the last resynchronization):
    /// `pending` holds those after `applied`, unless the sink fell behind.
    base: Gsn,
    /// The last GSN the engine delivered (a change, or a GSN without one): once
    /// `pending` is written, the target is the relation as of this GSN, which
    /// is what the progress row records. Recording only the GSNs of changes
    /// made every restart rewrite the target: the restarted engine's snapshot
    /// is always later (clock ticks, other tables).
    seen: Gsn,
    /// Every change at or below this GSN has been received: a GSN's changes
    /// can come in several chunks, and it is complete once a later GSN's
    /// changes, or progress at or after it, arrive (at the latest when the
    /// sink stops, which comes after everything the engine sent). Only
    /// complete GSNs are written.
    complete: Gsn,
    /// The GSN of the last changes received.
    last_data: Gsn,
    /// The target has to be rewritten from a snapshot: it was behind `base`
    /// at the first connection, or the sink fell behind (`SinkMsg::Behind`)
    /// and its changes were dropped.
    resync_needed: bool,
    resync: Resync,
    /// The sink was asked to stop (`SinkHandle::stop`): set before the
    /// `Stop` message, for a snapshot being written to give up.
    stopping: Arc<std::sync::atomic::AtomicBool>,
    backoff: Duration,
    /// The error of the last attempt to write to the target (a batch, or the
    /// progress row), with the progress that write was to commit: set until a
    /// write succeeds, so that a target that keeps rejecting a batch shows the
    /// error (reconnecting proves nothing), and retries keep backing off.
    failure: Option<(String, Gsn)>,
    /// A write failed since the main loop last looked.
    failed: Option<(SqlError, Gsn)>,
    last_flush: Instant,
    last_progress: Instant,
    stop: bool,
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    name: String,
    def: SinkDef,
    columns: Vec<Column>,
    start_gsn: Gsn,
    limits: SinkLimits,
    rx: Receiver<SinkMsg>,
    status: Arc<Mutex<String>>,
    stopping: Arc<std::sync::atomic::AtomicBool>,
    ask: &dyn Fn(),
) {
    let mut s = Sink {
        name,
        def,
        columns,
        limits,
        rx,
        status,
        ask,
        writer: None,
        pending: VecDeque::new(),
        pending_rows: 0,
        applied: None,
        base: start_gsn,
        seen: start_gsn,
        complete: start_gsn,
        last_data: start_gsn,
        resync_needed: false,
        resync: Resync::Idle,
        stopping,
        backoff: RETRY_MIN,
        failure: None,
        failed: None,
        last_flush: Instant::now(),
        last_progress: Instant::now(),
        stop: false,
    };
    s.run();
}

impl Sink<'_> {
    fn run(&mut self) {
        loop {
            if self.writer.is_none() && !self.connect() {
                // changes are kept meanwhile; a Stop ends the sink
                if !self.wait() {
                    return;
                }
                self.backoff = (self.backoff * 2).min(RETRY_MAX);
                continue;
            }
            if self.resync_needed && self.resync == Resync::Idle {
                (self.ask)();
                self.resync = Resync::Asked;
            }
            // a backlog is written without waiting for more
            let backlog = self.pending.front().is_some_and(|(g, _)| *g <= self.complete);
            let wait = if self.writable() && backlog { Duration::ZERO } else { Duration::from_millis(50) };
            match self.rx.recv_timeout(wait) {
                Ok(m) => self.receive(m),
                Err(RecvTimeoutError::Disconnected) => self.stop = true,
                Err(RecvTimeoutError::Timeout) => {}
            }
            // (what queued up meanwhile, a bounded amount at a time)
            for _ in 0..1000 {
                if self.failed.is_some() {
                    break;
                }
                match self.rx.try_recv() {
                    Ok(m) => self.receive(m),
                    Err(crossbeam_channel::TryRecvError::Disconnected) => self.stop = true,
                    Err(crossbeam_channel::TryRecvError::Empty) => break,
                }
            }
            if self.failed.is_none() {
                self.flush();
                // stopping: everything received is written first
                while self.stop && self.failed.is_none() && self.writable() && !self.pending.is_empty() {
                    self.flush();
                }
            }
            if let Some((e, progress)) = self.failed.take() {
                // A batch stays pending; after reconnecting, the committed
                // progress tells whether it already landed. A target that
                // rejects it (a constraint, a value that does not convert)
                // keeps doing so: retry with a growing delay.
                log::warn!("sink {}: writing to the target failed: {e}; retrying in {} ms", self.name, self.backoff.as_millis());
                self.set_status(format!("error: {e}"));
                self.failure = Some((e.to_string(), progress));
                self.writer = None;
                if self.stop || !self.wait() {
                    return;
                }
                self.backoff = (self.backoff * 2).min(RETRY_MAX);
                continue;
            }
            if self.stop {
                return;
            }
        }
    }

    fn set_status(&self, s: String) {
        *self.status.lock() = s;
    }

    /// Pending changes can be written now.
    fn writable(&self) -> bool {
        self.writer.is_some() && !self.resync_needed && self.applied.is_some()
    }

    /// Connect, and decide from what the target holds where delivery
    /// resumes. False if that failed.
    fn connect(&mut self) -> bool {
        let w = match Writer::connect(&self.name, &self.def, &self.columns, self.limits.message_bytes) {
            Ok(w) => self.writer.insert(w),
            Err(e) => {
                self.set_status(format!("error: {e}"));
                return false;
            }
        };
        let c = match w.committed_gsn() {
            Ok(c) => c,
            Err(e) => {
                self.set_status(format!("error: {e}"));
                self.writer = None;
                return false;
            }
        };
        if !self.resync_needed {
            match c {
                Some(c) if c >= self.base => {
                    self.applied = Some(c);
                    self.drop_pending_through(c);
                }
                // no progress yet, or behind the changes this sink has:
                // only at its first synchronisation
                _ if self.applied.is_none() => self.resync_needed = true,
                _ => {
                    self.set_status("error: sink progress row went backwards".into());
                    self.writer = None;
                    return false;
                }
            }
        }
        if let (Some((_, g)), Some(c)) = (&self.failure, c)
            && c >= *g
        {
            // the write landed: what failed was the acknowledgement of
            // its commit
            self.failure = None;
        }
        match &self.failure {
            // the write still has to be retried
            Some((f, _)) => self.set_status(format!("error: {f}")),
            None => {
                if let Some(a) = self.applied {
                    self.set_status(format!("running (gsn {a})"));
                }
                self.backoff = RETRY_MIN;
            }
        }
        true
    }

    /// Wait out a backoff, still taking messages: changes are kept for
    /// later (within the bound), a Stop ends the sink. False on Stop.
    fn wait(&mut self) -> bool {
        let deadline = Instant::now() + self.backoff;
        loop {
            match self.rx.recv_deadline(deadline) {
                Ok(SinkMsg::Stop) | Err(RecvTimeoutError::Disconnected) => return false,
                Ok(m) => self.receive(m),
                Err(RecvTimeoutError::Timeout) => return true,
            }
        }
    }

    fn receive(&mut self, m: SinkMsg) {
        match m {
            SinkMsg::Data(g, d) => {
                self.seen = self.seen.max(g);
                if g > self.last_data {
                    self.complete = self.complete.max(self.last_data);
                    self.last_data = g;
                }
                if g > self.applied.unwrap_or(self.base) {
                    self.pending_rows += d.len();
                    self.pending.push_back((g, d));
                }
            }
            SinkMsg::Progress(g, _) => {
                self.seen = self.seen.max(g);
                self.complete = self.complete.max(g);
            }
            SinkMsg::Behind => {
                // (the engine sends nothing more until the snapshot)
                log::warn!(
                    "sink {}: its unwritten changes reached their memory limit; dropping them and resynchronizing the target from a snapshot",
                    self.name
                );
                self.clear_pending();
                self.resync_needed = true;
            }
            SinkMsg::Snapshot(g, rows) => self.write_snapshot(g, rows),
            SinkMsg::Stop => self.stop = true,
        }
    }

    /// Write the snapshot at `gsn` that the sink asked for over the target,
    /// in one transaction: its chunks as they come (a reader thread reads
    /// them as they are taken). A snapshot it did not ask for (any more) is
    /// dropped, which stops its reader. If writing it fails, the sink asks
    /// for a new one once it is connected again.
    fn write_snapshot(&mut self, gsn: Gsn, rows: Chunks<Delta>) {
        if self.resync != Resync::Asked {
            return;
        }
        self.resync = Resync::Idle;
        // everything received before the snapshot is in it
        self.clear_pending();
        self.seen = self.seen.max(gsn);
        self.complete = self.complete.max(gsn);
        let Some(w) = self.writer.as_mut() else { return };
        *self.status.lock() = format!("resynchronizing to gsn {gsn}");
        let stopping = self.stopping.clone();
        let r = (|| {
            w.begin_resync(gsn)?;
            for chunk in rows {
                let chunk = match chunk {
                    Ok(c) => c,
                    Err(e) => {
                        w.abort();
                        return Err(SqlError::internal(format!("the engine could not read the snapshot: {e}")));
                    }
                };
                if stopping.load(std::sync::atomic::Ordering::Acquire) {
                    w.abort();
                    return Err(SqlError::new(crate::error::codes::ADMIN_SHUTDOWN, "the sink is stopping"));
                }
                w.insert(expand(&chunk))?;
            }
            w.commit(gsn)
        })();
        match r {
            Ok(()) => {
                self.applied = Some(gsn);
                self.base = gsn;
                self.resync_needed = false;
                self.failure = None;
                self.backoff = RETRY_MIN;
                self.last_progress = Instant::now();
                self.set_status(format!("running (gsn {gsn})"));
            }
            Err(_) if self.stopping.load(std::sync::atomic::Ordering::Acquire) => self.stop = true,
            Err(e) => self.failed = Some((e, gsn)),
        }
    }

    fn clear_pending(&mut self) {
        self.pending.clear();
        self.pending_rows = 0;
    }

    fn drop_pending_through(&mut self, gsn: Gsn) {
        while self.pending.front().is_some_and(|(g, _)| *g <= gsn) {
            let (_, d) = self.pending.pop_front().unwrap();
            self.pending_rows -= d.len();
        }
    }

    /// Write pending changes (whole GSNs, `FLUSH_ROWS` rows or so per
    /// transaction), or advance the progress row while nothing is pending.
    fn flush(&mut self) {
        if !self.writable() {
            return;
        }
        let applied = self.applied.unwrap_or(self.base);
        let w = self.writer.as_mut().unwrap();
        if self.stop {
            self.complete = self.seen;
        }
        let ready = self.pending.front().is_some_and(|(g, _)| *g <= self.complete);
        if ready && (self.stop || self.pending_rows > 1000 || self.last_flush.elapsed() > Duration::from_millis(100)) {
            let batch = self.pending.make_contiguous();
            // whole GSNs (with every chunk of each), complete ones only
            let (mut take, mut rows) = (0, 0);
            while take < batch.len() && batch[take].0 <= self.complete {
                let g = batch[take].0;
                let (mut end, mut n) = (take, 0);
                while end < batch.len() && batch[end].0 == g {
                    n += batch[end].1.len();
                    end += 1;
                }
                if take > 0 && rows + n > FLUSH_ROWS {
                    break;
                }
                rows += n;
                take = end;
            }
            let last = batch[take - 1].0;
            let progress = if take == batch.len() { self.seen } else { last };
            match w.apply(&batch[..take], progress, applied) {
                Ok(()) => {
                    self.applied = Some(progress);
                    self.drop_pending_through(last);
                    self.last_flush = Instant::now();
                    self.last_progress = Instant::now();
                    self.wrote(progress);
                }
                Err(e) => self.failed = Some((e, progress)),
            }
        } else if self.pending.is_empty() && applied < self.seen && (self.stop || self.last_progress.elapsed() >= PROGRESS_INTERVAL) {
            // nothing changed since: the target is current as of `seen`
            self.last_progress = Instant::now();
            match w.save_progress(self.seen, applied) {
                Ok(()) => {
                    self.applied = Some(self.seen);
                    self.wrote(self.seen);
                }
                Err(e) => self.failed = Some((e, self.seen)),
            }
        }
    }

    fn wrote(&mut self, gsn: Gsn) {
        self.set_status(format!("running (gsn {gsn})"));
        self.failure = None;
        self.backoff = RETRY_MIN;
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    /// A retraction matches a column by `=` or IS NULL, which an index can
    /// serve, and where `=` is coarser than identity or missing, by the
    /// column's text too (`ColumnType::equality`).
    #[test]
    fn retractions_match_exact_values() {
        use crate::types::{text::parse_datum, ColumnType, SqlType as T};
        let arr = |t: T| T::Array(Box::new(t));
        let cases = [
            (T::Int4, -1, "5", r#""c" = '5'::integer"#),
            (T::Int4, -1, "", r#""c" IS NULL"#),
            (T::Text, -1, "it's", r#""c" = 'it''s'::text"#),
            (T::Bpchar, 7, "a", r#""c" = 'a  '::character(3)"#),
            (T::TimestampTz, -1, "2024-01-01 05:45:00+05:45", r#""c" = '2024-01-01 00:00:00+00'::timestamp with time zone"#),
            (T::Interval, -1, "1 mon", r#""c" = '1 mon'::interval AND "c"::text = ('1 mon'::interval)::text"#),
            (T::Numeric, -1, "1.50", r#""c" = '1.50'::numeric AND "c"::text = ('1.50'::numeric)::text"#),
            (T::Float8, -1, "-0", r#""c" = '-0'::double precision AND "c"::text = ('-0'::double precision)::text"#),
            (T::Jsonb, -1, "[1.0]", r#""c" = '[1.0]'::jsonb AND "c"::text = ('[1.0]'::jsonb)::text"#),
            (T::Bpchar, -1, "a  ", r#""c" = 'a  '::bpchar AND octet_length("c") = octet_length('a  '::bpchar)"#),
            (T::Json, -1, "[1]", r#""c"::text = ('[1]'::json)::text"#),
            (T::JsonPath, -1, "$.a", r#""c"::text = ('$."a"'::jsonpath)::text"#),
            (arr(T::Int4), -1, "{1,NULL}", r#""c" = '{1,NULL}'::integer[]"#),
            (arr(T::Interval), -1, "{{1 mon},{NULL}}", r#""c" = '{{"1 mon"},{NULL}}'::interval[] AND "c"::text = ('{{"1 mon"},{NULL}}'::interval[])::text"#),
            (arr(T::Bpchar), -1, "{\"a \"}", r#""c" = '{"a "}'::bpchar[] AND "c"::text = ('{"a "}'::bpchar[])::text"#),
            (arr(T::Bpchar), 7, "{a}", r#""c" = '{"a  "}'::character(3)[]"#),
            (arr(T::Json), -1, "{1}", r#""c"::text = ('{1}'::json[])::text"#),
        ];
        for (ty, typmod, v, want) in cases {
            let c = Column::new("c", ColumnType::with_typmod(ty.clone(), typmod));
            let d = if v.is_empty() { Datum::Null } else { parse_datum(v, &ty, typmod).unwrap() };
            assert_eq!(same_value(&c, &d), want, "{ty:?} {v}");
        }
    }

    /// A transaction is sent in messages of at most the configured size
    /// (a resynchronization used to be one SQL string with every row, and
    /// PostgreSQL rejects messages over 1 GB), its statements whole and in
    /// order; a statement larger than the bound is a message of its own.
    #[test]
    fn transactions_are_sent_in_bounded_messages() {
        let max = 4096;
        let mut packer = Packer { max, buf: String::new() };
        let mut ins = Inserts::new("INSERT INTO t (a, b) VALUES ", " ON CONFLICT (a) DO NOTHING", max);
        let mut stmts = vec!["DELETE FROM t".to_string()];
        for i in 0..3000 {
            // (a few rows larger than a message)
            let vals = if i % 1000 == 7 { format!("{i}, '{}'", "x".repeat(2 * max)) } else { format!("{i}, 'v{i}'") };
            stmts.extend(ins.add(&vals));
        }
        stmts.extend(ins.finish());
        stmts.push("COMMIT".into());
        let mut msgs: Vec<String> = stmts.iter().filter_map(|s| packer.push(s)).collect();
        msgs.push(packer.take());
        assert!(msgs.len() > 10, "{} messages", msgs.len());
        for m in &msgs {
            assert!(m.len() <= max || m.matches(';').count() == 1, "a message of {} bytes with {} statements", m.len(), m.matches(';').count());
        }
        assert_eq!(msgs.concat(), stmts.iter().map(|s| format!("{s};")).collect::<String>());
        let rows: usize = stmts.iter().filter(|s| s.starts_with("INSERT")).map(|s| s.matches("), (").count() + 1).sum();
        assert_eq!(rows, 3000);
        assert!(stmts.iter().all(|s| s.matches("), (").count() < INSERT_ROWS));
    }
}
