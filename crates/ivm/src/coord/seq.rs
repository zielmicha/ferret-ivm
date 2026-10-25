//! The sequencer: assigns GSNs in arrival order, appends to the input log,
//! makes batches durable (group commit), acknowledges submitters and hands
//! durable records to the engine in order.

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
                Some(every) => match self.rx.recv_timeout(next_tick.saturating_duration_since(Instant::now())) {
                    Ok(m) => m,
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                        next_tick = Instant::now() + every;
                        last_micros = wall_micros().max(last_micros + 1);
                        SeqMsg::Append { rec: LogRecord::Tick { micros: last_micros }, ack: None }
                    }
                    Err(_) => return Ok(()),
                },
                None => match self.rx.recv() {
                    Ok(m) => m,
                    Err(_) => return Ok(()),
                },
            };
            let mut batch = vec![first];
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
