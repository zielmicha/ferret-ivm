//! The sequencer: assigns GSNs in arrival order, appends to the input log,
//! makes batches durable (group commit), acknowledges submitters and hands
//! durable records to the engine in order.

use super::api::Shared;
use super::EngineMsg;
use crate::error::Result;
use crate::log::{LogRecord, LogWriter, Spooled};
use crate::storage::Gsn;
use crossbeam_channel::{Receiver, Sender};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub enum SeqMsg {
    Append { rec: LogRecord, ack: Option<Sender<Gsn>> },
    /// The commit of a transaction a source spooled (`log::Spool`): logged
    /// as `CommitParts`, with the spool as its parts file.
    AppendSpooled { source: crate::sql::plan::ObjectId, lsn: u64, spool: Spooled, ack: Option<Sender<Gsn>> },
    Truncate(Gsn),
    Shutdown,
}

pub struct Sequencer {
    pub writer: LogWriter,
    pub rx: Receiver<SeqMsg>,
    pub engine: Sender<EngineMsg>,
    pub shared: Arc<Shared>,
    /// Interval of logged wall-clock ticks (None: no ticks).
    pub tick: Option<Duration>,
}

/// Most durable records the engine may still have to apply (checked before
/// each batch, so give or take a batch). Beyond it the sequencer takes no
/// more appends until the engine catches up: sources
/// then wait for their acknowledgements (and stop reading the replication
/// stream; the upstream keeps the WAL), so the engine's queue of durable
/// batches stays bounded instead of growing without limit when the engine
/// cannot keep up. The engine never waits for the sequencer, so it always
/// catches up.
const MAX_ENGINE_LAG: u64 = 10_000;

fn wall_micros() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_micros() as i64).unwrap_or(0)
}

impl Sequencer {
    pub fn run(mut self) -> Result<()> {
        let mut next = self.writer.last_gsn() + 1;
        let mut next_tick = Instant::now();
        let mut last_micros = 0i64;
        loop {
            // backpressure (not while shutting down: the engine stops applying)
            while self.shared.durable().saturating_sub(self.shared.processed()) >= MAX_ENGINE_LAG
                && !self.shared.shutdown.load(Ordering::Acquire)
            {
                std::thread::sleep(Duration::from_millis(1));
            }
            // the wall clock enters the log as a tick stream, so time is as
            // deterministic on replay as any other input
            let first = match self.tick {
                Some(_) => match self.rx.recv_timeout(next_tick.saturating_duration_since(Instant::now())) {
                    Ok(m) => Some(m),
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => None,
                    Err(_) => return Ok(()),
                },
                None => match self.rx.recv() {
                    Ok(m) => Some(m),
                    Err(_) => return Ok(()),
                },
            };
            let mut batch: Vec<SeqMsg> = first.into_iter().collect();
            // the clock ticks on schedule whether or not work is waiting
            // (under steady load a message is always ready first)
            if let Some(every) = self.tick
                && Instant::now() >= next_tick
            {
                next_tick = Instant::now() + every;
                last_micros = wall_micros().max(last_micros + 1);
                batch.push(SeqMsg::Append { rec: LogRecord::Tick { micros: last_micros }, ack: None });
            }
            while batch.len() < 4096 {
                match self.rx.try_recv() {
                    Ok(m) => batch.push(m),
                    Err(_) => break,
                }
            }
            let mut acks = Vec::new();
            let mut durable = Vec::new();
            let mut shutdown = false;
            for m in batch {
                match m {
                    SeqMsg::Append { rec, ack } => {
                        let gsn = next;
                        next += 1;
                        self.writer.append(gsn, &rec)?;
                        if let Some(a) = ack {
                            acks.push((a, gsn));
                        }
                        durable.push((gsn, rec));
                    }
                    SeqMsg::AppendSpooled { source, lsn, spool, ack } => {
                        let gsn = next;
                        next += 1;
                        let rec = self.writer.append_spooled(gsn, source, lsn, spool)?;
                        if let Some(a) = ack {
                            acks.push((a, gsn));
                        }
                        durable.push((gsn, rec));
                    }
                    SeqMsg::Truncate(g) => self.writer.truncate_before(g)?,
                    SeqMsg::Shutdown => shutdown = true,
                }
            }
            if !durable.is_empty() {
                self.writer.flush()?;
                let last = durable.last().unwrap().0;
                self.shared.durable_gsn.store(last, Ordering::Release);
                for (a, g) in acks {
                    let _ = a.send(g);
                }
                if self.engine.send(EngineMsg::Durable(durable)).is_err() {
                    return Ok(());
                }
            }
            if shutdown {
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::{read_from, LogWriter};

    /// Clock ticks keep coming while the sequencer is never idle: under a
    /// steady stream of appends, receiving the next message always beat the
    /// tick deadline, so the logical clock (and temporal filters) stood
    /// still for as long as the load lasted.
    #[test]
    fn ticks_are_not_starved_by_load() {
        let base = std::env::var("IVM_TEST_TMP").map(std::path::PathBuf::from).unwrap_or_else(|_| std::env::temp_dir());
        let dir = base.join(format!("ivm-seq-ticks-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        // with fsync, every group commit takes long enough for the next
        // appends to be queued when the sequencer looks for work
        let writer = LogWriter::open(&dir, true).unwrap();
        let (tx, rx) = crossbeam_channel::unbounded();
        let (etx, erx) = crossbeam_channel::unbounded();
        // (an engine that keeps up: no backpressure)
        let shared = Arc::new(Shared::default());
        shared.processed_gsn.store(u64::MAX, Ordering::Release);
        let seq = Sequencer { writer, rx, engine: etx, shared, tick: Some(Duration::from_millis(5)) };
        let t = std::thread::spawn(move || seq.run().unwrap());
        // keep the sequencer busy for 300ms
        let t0 = Instant::now();
        while t0.elapsed() < Duration::from_millis(300) {
            tx.send(SeqMsg::Append { rec: LogRecord::Progress { source: 1, lsn: 0, state: String::new() }, ack: None }).unwrap();
            std::thread::sleep(Duration::from_micros(50));
        }
        tx.send(SeqMsg::Shutdown).unwrap();
        t.join().unwrap();
        drop(erx);
        let ticks = read_from(&dir, 0).unwrap().map(|r| r.unwrap()).filter(|(_, r)| matches!(r, LogRecord::Tick { .. })).count();
        assert!(ticks >= 10, "only {ticks} ticks in 300ms at a 5ms interval");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The sequencer stops taking appends while the engine is MAX_ENGINE_LAG
    /// records behind (give or take a batch), and goes on when it catches up
    /// (the queue of durable batches used to grow without bound).
    #[test]
    fn appends_wait_for_a_lagging_engine() {
        let base = std::env::var("IVM_TEST_TMP").map(std::path::PathBuf::from).unwrap_or_else(|_| std::env::temp_dir());
        let dir = base.join(format!("ivm-seq-lag-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let writer = LogWriter::open(&dir, false).unwrap();
        let (tx, rx) = crossbeam_channel::unbounded();
        let (etx, _erx) = crossbeam_channel::unbounded();
        let shared = Arc::new(Shared::default());
        let seq = Sequencer { writer, rx, engine: etx, shared: shared.clone(), tick: None };
        let t = std::thread::spawn(move || seq.run().unwrap());
        let n = MAX_ENGINE_LAG + 10_000;
        let (atx, arx) = crossbeam_channel::unbounded();
        for _ in 0..n {
            tx.send(SeqMsg::Append { rec: LogRecord::Progress { source: 1, lsn: 0, state: String::new() }, ack: Some(atx.clone()) }).unwrap();
        }
        let durable_after = |ms: u64| {
            std::thread::sleep(Duration::from_millis(ms));
            shared.durable()
        };
        // (checked between batches of up to 4096 records)
        let stalled = durable_after(300);
        assert!((MAX_ENGINE_LAG..MAX_ENGINE_LAG + 4096).contains(&stalled), "durable {stalled} of {n} with nothing applied");
        assert_eq!(arx.len() as u64, stalled, "only durable records are acknowledged");
        // the engine catches up
        shared.processed_gsn.store(stalled, Ordering::Release);
        let t0 = Instant::now();
        while shared.durable() < n {
            assert!(t0.elapsed() < Duration::from_secs(10), "the sequencer does not go on");
            std::thread::sleep(Duration::from_millis(5));
        }
        tx.send(SeqMsg::Shutdown).unwrap();
        t.join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
