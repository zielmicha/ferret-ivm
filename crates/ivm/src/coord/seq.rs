//! The sequencer: assigns GSNs in arrival order, appends to the input log,
//! makes batches durable (group commit), acknowledges submitters and hands
//! durable records to the engine in order.
//!
//! It never waits for the engine: the memory of the records between their
//! submission and their application is bounded where they are submitted
//! (`EngineHandle::log_async` waits for room in the memory budget's input
//! queue), so that its own queue cannot grow without limit either.

use super::api::Shared;
use super::EngineMsg;
use crate::error::Result;
use crate::log::{LogRecord, LogWriter};
use crate::storage::Gsn;
use crossbeam_channel::{Receiver, Sender};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub enum SeqMsg {
    Append { rec: LogRecord, ack: Option<Sender<Gsn>> },
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

fn wall_micros() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_micros() as i64).unwrap_or(0)
}

impl Sequencer {
    pub fn run(mut self) -> Result<()> {
        let mut next = self.writer.last_gsn() + 1;
        let mut next_tick = Instant::now();
        let mut last_micros = 0i64;
        loop {
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
                let rec = LogRecord::Tick { micros: last_micros };
                // (in the input queue's memory until applied, like any record)
                self.shared.mem.input.add(crate::memory::record_bytes(&rec));
                batch.push(SeqMsg::Append { rec, ack: None });
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
        let ticks = read_from(&dir, 0).unwrap().iter().filter(|(_, r)| matches!(r, LogRecord::Tick { .. })).count();
        assert!(ticks >= 10, "only {ticks} ticks in 300ms at a 5ms interval");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
