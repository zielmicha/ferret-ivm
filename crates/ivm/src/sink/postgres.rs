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
use crate::dataflow::Delta;
use crate::error::{Result, SqlError};
use crate::storage::Gsn;
use crate::types::text::format_datum;
use crate::types::{format_type, Column, Datum, Row};
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

/// Columns a retraction must match by text: types PostgreSQL has no
/// equality for (json, jsonpath), and types whose equal values differ in
/// representation (numeric scales, -0, jsonb numbers), where equality could
/// delete a row holding another representation than the retracted one.
fn compare_as_text(ty: &crate::types::SqlType) -> bool {
    use crate::types::SqlType;
    match ty {
        SqlType::Json | SqlType::JsonPath | SqlType::Jsonb | SqlType::Numeric | SqlType::Float4 | SqlType::Float8 => true,
        SqlType::Array(e) => compare_as_text(e),
        _ => false,
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
            Some(k) => {
                let cond = k
                    .iter()
                    .map(|i| format!("{} = {}", qident(&self.columns[*i].name), lit(&r[*i], &self.columns[*i])))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                format!("DELETE FROM {} WHERE {cond}", self.table)
            }
            None => {
                // (no index can serve this: each deletion scans the target)
                let cond = self
                    .columns
                    .iter()
                    .zip(r)
                    .map(|(c, d)| {
                        if compare_as_text(&c.ty.ty) {
                            // the text identifies the value exactly
                            format!("{}::text IS NOT DISTINCT FROM ({})::text", qident(&c.name), lit(d, c))
                        } else {
                            format!("{} IS NOT DISTINCT FROM {}", qident(&c.name), lit(d, c))
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(" AND ");
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
    fn apply(&mut self, batch: &[(Gsn, Delta)], progress: Gsn, applied: Gsn) -> Result<()> {
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

/// Estimated memory of a delta's rows, for the bound on pending changes.
fn delta_bytes(delta: &Delta) -> usize {
    delta.iter().map(|(r, _)| 32 + r.iter().map(crate::types::codec::approx_len).sum::<usize>()).sum()
}

/// Where a resynchronization of the target stands.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Resync {
    /// None in progress.
    Idle,
    /// The engine was asked for a snapshot.
    Asked,
    /// The snapshot at this GSN is being written, in the transaction opened
    /// at its first chunk.
    Writing(Gsn),
    /// The rest of a snapshot whose writing failed is dropped.
    Dropping,
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
    /// Changes received and not written yet, in GSN order.
    pending: VecDeque<(Gsn, Delta)>,
    pending_rows: usize,
    pending_bytes: usize,
    /// GSNs at or below this are already in the target.
    applied: Option<Gsn>,
    /// Every change after this GSN has been received (the engine's GSN when
    /// the sink started, then the GSN of the last resynchronization):
    /// `pending` holds those after `applied`, unless `overflowed`.
    base: Gsn,
    /// The last GSN the engine delivered (a change, or a GSN without one): once
    /// `pending` is written, the target is the relation as of this GSN, which
    /// is what the progress row records. Recording only the GSNs of changes
    /// made every restart rewrite the target: the restarted engine's snapshot
    /// is always later (clock ticks, other tables).
    seen: Gsn,
    /// The target has to be rewritten from a snapshot: it was behind `base`
    /// at the first connection, or changes were dropped (`overflowed`).
    resync_needed: bool,
    /// Pending changes exceeded `limits.pending_bytes` and were dropped:
    /// only a snapshot can bring the target up to date.
    overflowed: bool,
    resync: Resync,
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
        pending_bytes: 0,
        applied: None,
        base: start_gsn,
        seen: start_gsn,
        resync_needed: false,
        overflowed: false,
        resync: Resync::Idle,
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
            let wait = if self.writable() && self.pending_rows > 0 { Duration::ZERO } else { Duration::from_millis(50) };
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
                // (changes before a snapshot that will be written are in it)
                let covered = self.overflowed && !matches!(self.resync, Resync::Writing(_));
                if g > self.applied.unwrap_or(self.base) && !covered {
                    self.pending_rows += d.len();
                    self.pending_bytes += delta_bytes(&d);
                    self.pending.push_back((g, d));
                    // (a single change set is kept, however large: the
                    // engine has it in memory anyway)
                    if self.pending_bytes > self.limits.pending_bytes && self.pending.len() > 1 {
                        log::warn!(
                            "sink {}: more than {} MiB of changes are waiting to be written; dropping them and resynchronizing the target from a snapshot",
                            self.name,
                            self.limits.pending_bytes >> 20
                        );
                        self.clear_pending();
                        self.overflowed = true;
                        self.resync_needed = true;
                    }
                }
            }
            SinkMsg::Progress(g) => self.seen = self.seen.max(g),
            SinkMsg::Snapshot(g, rows) => {
                if self.start_resync(g) {
                    let w = self.writer.as_mut().expect("writer");
                    if let Err(e) = w.insert(expand(&rows)) {
                        self.resync_failed(e, g);
                    }
                }
            }
            SinkMsg::SnapshotEnd(g, error) => {
                // (an empty relation's snapshot has no chunks)
                if self.start_resync(g) {
                    let w = self.writer.as_mut().expect("writer");
                    if let Some(e) = error {
                        w.abort();
                        self.resync = Resync::Idle;
                        self.failed = Some((SqlError::internal(format!("the engine could not read the snapshot: {e}")), g));
                        return;
                    }
                    match w.commit(g) {
                        Ok(()) => {
                            self.applied = Some(g);
                            self.base = g;
                            self.resync_needed = false;
                            self.overflowed = false;
                            self.resync = Resync::Idle;
                            self.failure = None;
                            self.backoff = RETRY_MIN;
                            self.last_progress = Instant::now();
                            self.set_status(format!("running (gsn {g})"));
                        }
                        Err(e) => self.resync_failed(e, g),
                    }
                }
                if self.resync == Resync::Dropping {
                    self.resync = Resync::Idle;
                }
            }
            SinkMsg::Stop => self.stop = true,
        }
    }

    /// A chunk of the snapshot at `gsn` arrived: open the resynchronizing
    /// transaction at the first one. True if its rows are to be written.
    fn start_resync(&mut self, gsn: Gsn) -> bool {
        match self.resync {
            Resync::Writing(g) => g == gsn,
            Resync::Asked => {
                // everything received before the snapshot is in it
                self.clear_pending();
                self.overflowed = false;
                self.seen = self.seen.max(gsn);
                if self.writer.is_none() {
                    self.resync = Resync::Dropping;
                    return false;
                }
                self.set_status(format!("resynchronizing to gsn {gsn}"));
                match self.writer.as_mut().unwrap().begin_resync(gsn) {
                    Ok(()) => {
                        self.resync = Resync::Writing(gsn);
                        true
                    }
                    Err(e) => {
                        self.resync_failed(e, gsn);
                        false
                    }
                }
            }
            Resync::Idle | Resync::Dropping => false,
        }
    }

    /// Writing a snapshot failed: drop the rest of it; once connected
    /// again, the sink asks for a new one.
    fn resync_failed(&mut self, e: SqlError, gsn: Gsn) {
        self.resync = Resync::Dropping;
        self.failed = Some((e, gsn));
    }

    fn clear_pending(&mut self) {
        self.pending.clear();
        self.pending_rows = 0;
        self.pending_bytes = 0;
    }

    fn drop_pending_through(&mut self, gsn: Gsn) {
        while self.pending.front().is_some_and(|(g, _)| *g <= gsn) {
            let (_, d) = self.pending.pop_front().unwrap();
            self.pending_rows -= d.len();
            self.pending_bytes -= delta_bytes(&d);
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
        if !self.pending.is_empty() && (self.stop || self.pending_rows > 1000 || self.last_flush.elapsed() > Duration::from_millis(100)) {
            let batch = self.pending.make_contiguous();
            let mut take = 0;
            let mut rows = 0;
            while take < batch.len() && (take == 0 || rows + batch[take].1.len() <= FLUSH_ROWS) {
                rows += batch[take].1.len();
                take += 1;
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
