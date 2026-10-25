//! Postgres write-back sink.
//!
//! The target table mirrors the view. Each batch of GSNs is applied in one
//! transaction together with the sink's progress row, so delivery is exactly
//! once: after (re)connecting, the sink reads its committed progress and
//! skips every GSN at or below it (a batch whose commit acknowledgement was
//! lost is not applied twice). A target behind the engine's snapshot is
//! resynchronized from the snapshot.

use super::SinkMsg;
use crate::catalog::SinkDef;
use crate::dataflow::Delta;
use crate::error::Result;
use crate::sql::plan::ObjectId;
use crate::storage::Gsn;
use crate::types::text::format_datum;
use crate::types::{format_type, Column, Datum, Row};
use crossbeam_channel::{Receiver, RecvTimeoutError};
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// While nothing is pending, the progress row is advanced to the last GSN
/// delivered at most this often (and when the sink stops): after a restart,
/// a target that is that far needs no resynchronization.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(5);

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

struct Writer {
    client: pgproto::Client,
    sink: String,
    table: String,
    columns: Vec<Column>,
    key: Option<Vec<usize>>,
}

impl Writer {
    fn connect(name: &str, def: &SinkDef, columns: &[Column]) -> Result<Writer> {
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
        Ok(Writer { client, sink: name.to_string(), table: qtable(&def.table), columns: columns.to_vec(), key })
    }

    fn committed_gsn(&mut self) -> Result<Option<Gsn>> {
        let r = self.client.query(&format!(
            "SELECT gsn FROM ivm_sink_progress WHERE sink = '{}'",
            self.sink.replace('\'', "''")
        ))?;
        Ok(r.rows.first().and_then(|row| row[0].as_ref()).and_then(|s| s.parse().ok()))
    }

    fn progress_sql(&self, gsn: Gsn) -> String {
        format!(
            "INSERT INTO ivm_sink_progress (sink, gsn) VALUES ('{}', {gsn}) ON CONFLICT (sink) DO UPDATE SET gsn = EXCLUDED.gsn",
            self.sink.replace('\'', "''")
        )
    }

    fn insert_sql(&self, rows: &[&Row]) -> String {
        let cols = self.columns.iter().map(|c| qident(&c.name)).collect::<Vec<_>>().join(", ");
        let vals = rows
            .iter()
            .map(|r| format!("({})", r.iter().zip(&self.columns).map(|(d, c)| lit(d, c)).collect::<Vec<_>>().join(", ")))
            .collect::<Vec<_>>()
            .join(", ");
        match &self.key {
            Some(k) => {
                let kc = k.iter().map(|i| qident(&self.columns[*i].name)).collect::<Vec<_>>().join(", ");
                let upd = self
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| !k.contains(i))
                    .map(|(_, c)| format!("{0} = EXCLUDED.{0}", qident(&c.name)))
                    .collect::<Vec<_>>()
                    .join(", ");
                if upd.is_empty() {
                    format!("INSERT INTO {} ({cols}) VALUES {vals} ON CONFLICT ({kc}) DO NOTHING", self.table)
                } else {
                    format!("INSERT INTO {} ({cols}) VALUES {vals} ON CONFLICT ({kc}) DO UPDATE SET {upd}", self.table)
                }
            }
            None => format!("INSERT INTO {} ({cols}) VALUES {vals}", self.table),
        }
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

    /// Replace the target contents with a snapshot.
    fn resync(&mut self, rows: &Delta, gsn: Gsn) -> Result<()> {
        let mut sql = format!("BEGIN; DELETE FROM {};", self.table);
        let expanded: Vec<&Row> = rows.iter().flat_map(|(r, w)| std::iter::repeat_n(r, (*w).max(0) as usize)).collect();
        for chunk in expanded.chunks(500) {
            sql.push_str(&self.insert_sql(chunk));
            sql.push(';');
        }
        sql.push_str(&self.progress_sql(gsn));
        sql.push_str("; COMMIT;");
        self.client.simple_query(&sql)?;
        Ok(())
    }

    /// Record that the target is as of `gsn` (nothing pending).
    fn save_progress(&mut self, gsn: Gsn) -> Result<()> {
        self.client.simple_query(&self.progress_sql(gsn))?;
        Ok(())
    }

    /// Apply a batch and record that the target is as of `progress` (at
    /// least the batch's last GSN).
    fn apply(&mut self, batch: &[(Gsn, Delta)], progress: Gsn) -> Result<()> {
        let mut sql = String::from("BEGIN;");
        for (_, delta) in batch {
            // retractions first, then insertions (an update is -old +new)
            for (r, w) in delta.iter().filter(|(_, w)| *w < 0) {
                for _ in 0..(-*w) {
                    sql.push_str(&self.delete_sql(r));
                    sql.push(';');
                }
            }
            let ins: Vec<&Row> = delta.iter().filter(|(_, w)| *w > 0).flat_map(|(r, w)| std::iter::repeat_n(r, *w as usize)).collect();
            for chunk in ins.chunks(500) {
                sql.push_str(&self.insert_sql(chunk));
                sql.push(';');
            }
        }
        sql.push_str(&self.progress_sql(progress));
        sql.push_str("; COMMIT;");
        if let Err(e) = self.client.simple_query(&sql) {
            let _ = self.client.simple_query("ROLLBACK");
            return Err(e.into());
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    _id: ObjectId,
    name: String,
    def: SinkDef,
    columns: Vec<Column>,
    snapshot: Delta,
    snapshot_gsn: Gsn,
    rx: Receiver<SinkMsg>,
    status: Arc<Mutex<String>>,
) {
    let mut pending: Vec<(Gsn, Delta)> = Vec::new();
    let mut writer: Option<Writer> = None;
    // GSNs at or below this are already in the target
    let mut applied: Option<Gsn> = None;
    // The last GSN the engine delivered (a change, or a GSN without one): once
    // `pending` is written, the target is the relation as of this GSN, which
    // is what the progress row records. Recording only the GSNs of changes
    // made every restart rewrite the target: the restarted engine's snapshot
    // is always later (clock ticks, other tables).
    let mut seen: Gsn = snapshot_gsn;
    let mut backoff = Duration::from_millis(100);
    let mut last_flush = Instant::now();
    let mut last_progress = Instant::now();
    // Wait out a backoff, still taking messages: changes are kept for later
    // (they used to be read and dropped), a Stop ends the sink. False on Stop.
    let wait = |backoff: Duration, applied: Option<Gsn>, pending: &mut Vec<(Gsn, Delta)>, seen: &mut Gsn| -> bool {
        let deadline = Instant::now() + backoff;
        loop {
            match rx.recv_deadline(deadline) {
                Ok(SinkMsg::Data(g, d)) => {
                    *seen = (*seen).max(g);
                    if g > applied.unwrap_or(snapshot_gsn) {
                        pending.push((g, d));
                    }
                }
                Ok(SinkMsg::Progress(g)) => *seen = (*seen).max(g),
                Ok(SinkMsg::Stop) | Err(RecvTimeoutError::Disconnected) => return false,
                Err(RecvTimeoutError::Timeout) => return true,
            }
        }
    };
    loop {
        if writer.is_none() {
            match Writer::connect(&name, &def, &columns) {
                Ok(w) => {
                    writer = Some(w);
                    backoff = Duration::from_millis(100);
                }
                Err(e) => {
                    *status.lock() = format!("error: {e}");
                    if !wait(backoff, applied, &mut pending, &mut seen) {
                        return;
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(10));
                    continue;
                }
            }
            // what the target holds decides where delivery resumes
            let w = writer.as_mut().unwrap();
            let r = match w.committed_gsn() {
                Ok(Some(c)) if c >= snapshot_gsn => Ok(c),
                Ok(_) if applied.is_none() => w.resync(&snapshot, snapshot_gsn).map(|_| snapshot_gsn),
                Ok(_) => Err(crate::error::SqlError::new(
                    crate::error::codes::OBJECT_NOT_IN_PREREQUISITE_STATE,
                    "sink progress row went backwards",
                )),
                Err(e) => Err(e),
            };
            match r {
                Ok(c) => {
                    applied = Some(c);
                    pending.retain(|(g, _)| *g > c);
                    *status.lock() = format!("running (gsn {c})");
                }
                Err(e) => {
                    *status.lock() = format!("error: {e}");
                    writer = None;
                    if !wait(backoff, applied, &mut pending, &mut seen) {
                        return;
                    }
                    continue;
                }
            }
        }
        let stop = match rx.recv_timeout(Duration::from_millis(50)) {
            Ok(SinkMsg::Data(g, d)) => {
                seen = seen.max(g);
                if g > applied.unwrap_or(snapshot_gsn) {
                    pending.push((g, d));
                }
                false
            }
            Ok(SinkMsg::Progress(g)) => {
                seen = seen.max(g);
                false
            }
            Ok(SinkMsg::Stop) | Err(RecvTimeoutError::Disconnected) => true,
            Err(RecvTimeoutError::Timeout) => false,
        };
        let w = writer.as_mut().unwrap();
        let rows: usize = pending.iter().map(|(_, d)| d.len()).sum();
        if !pending.is_empty() && (stop || rows > 1000 || last_flush.elapsed() > Duration::from_millis(100)) {
            match w.apply(&pending, seen) {
                Ok(()) => {
                    *status.lock() = format!("running (gsn {seen})");
                    applied = Some(seen);
                    pending.clear();
                    last_flush = Instant::now();
                    last_progress = Instant::now();
                }
                Err(e) => {
                    *status.lock() = format!("error: {e}");
                    writer = None;
                    // the batch stays pending; after reconnecting, the
                    // committed progress tells whether it already landed
                }
            }
        } else if pending.is_empty() && applied.is_some_and(|a| a < seen) && (stop || last_progress.elapsed() >= PROGRESS_INTERVAL) {
            // nothing changed since: the target is current as of `seen`
            match w.save_progress(seen) {
                Ok(()) => {
                    *status.lock() = format!("running (gsn {seen})");
                    applied = Some(seen);
                }
                Err(e) => {
                    *status.lock() = format!("error: {e}");
                    writer = None;
                }
            }
            last_progress = Instant::now();
        }
        if stop {
            return;
        }
    }
}
