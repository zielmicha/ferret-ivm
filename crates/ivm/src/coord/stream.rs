//! Rows streamed to a consumer in chunks: a query's result to its session,
//! a snapshot to a subscription's session or to a sink's thread.
//!
//! Every chunk holds a charge on a meter of the memory budget (the
//! consumer's queue, or the outputs' total) until the consumer drops it. A
//! stream produced off the engine thread (by a reader thread, from a fork
//! of the storage) goes through a channel of a few chunks: its producer
//! waits for the consumer, so that what is in flight stays bounded however
//! large the result and however slow the client. The engine thread never
//! waits for a consumer: it streams only what it read within its read
//! budget, all at once.

use crate::error::{codes, Result, SqlError};
use crate::memory::{Charge, Meter};
use crossbeam_channel::{Receiver, SendTimeoutError, Sender};
use std::sync::Arc;
use std::time::Duration;

/// Chunks a stream produced off the engine thread holds in flight.
const IN_FLIGHT: usize = 2;

/// How often a producer waiting for its consumer checks whether it should
/// stop (`ChunkSender::stop`).
const POLL: Duration = Duration::from_millis(200);

/// Rows handed to a consumer, counted in the memory budget until dropped.
#[derive(Debug)]
pub struct Chunk<T> {
    rows: T,
    _charge: Charge,
}

impl<T> Chunk<T> {
    pub fn new(rows: T, charge: Charge) -> Chunk<T> {
        Chunk { rows, _charge: charge }
    }

    /// The rows and their charge (to keep while they are in memory).
    pub fn into_parts(self) -> (T, Charge) {
        (self.rows, self._charge)
    }
}

impl<T> std::ops::Deref for Chunk<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.rows
    }
}

/// The consumer's end of a stream: its chunks in order, until it ends (an
/// error ends it too). Dropping it stops the producer at its next chunk.
#[derive(Debug)]
pub struct Chunks<T> {
    rx: Receiver<Result<Chunk<T>>>,
}

impl<T> Iterator for Chunks<T> {
    type Item = Result<Chunk<T>>;
    fn next(&mut self) -> Option<Self::Item> {
        self.rx.recv().ok()
    }
}

/// The producer's end of a stream.
pub struct ChunkSender<T> {
    tx: Sender<Result<Chunk<T>>>,
    meter: Arc<Meter>,
    /// Polled while the consumer does not take a chunk: true stops the
    /// stream (a cancelled statement or subscription).
    stop: Box<dyn Fn() -> bool + Send>,
}

impl<T> ChunkSender<T> {
    /// Hand on `rows`, `bytes` in memory, waiting for the consumer to take
    /// the chunks before it. False once the stream should end: the
    /// consumer is gone, or `stop` says so.
    pub fn send(&self, rows: T, bytes: usize) -> bool {
        let mut msg = Ok(Chunk::new(rows, self.meter.charge(bytes)));
        loop {
            match self.tx.send_timeout(msg, POLL) {
                Ok(()) => return true,
                Err(SendTimeoutError::Disconnected(_)) => return false,
                Err(SendTimeoutError::Timeout(m)) => {
                    if (self.stop)() {
                        return false;
                    }
                    msg = m;
                }
            }
        }
    }

    /// End the stream with an error (the consumer gets it after the chunks
    /// sent before), waiting for room as `send` does.
    pub fn fail(&self, e: SqlError) {
        let mut msg = Err(e);
        while let Err(SendTimeoutError::Timeout(m)) = self.tx.send_timeout(msg, POLL) {
            if (self.stop)() {
                return;
            }
            msg = m;
        }
    }
}

/// A stream produced off the engine thread: at most a few chunks in
/// flight, charged to `meter`; the producer stops when `stop` returns true
/// while it waits.
pub fn bounded<T>(meter: Arc<Meter>, stop: Box<dyn Fn() -> bool + Send>) -> (ChunkSender<T>, Chunks<T>) {
    let (tx, rx) = crossbeam_channel::bounded(IN_FLIGHT);
    (ChunkSender { tx, meter, stop }, Chunks { rx })
}

/// A stream of chunks the engine thread has already read (within its read
/// budget), charged to `meter`, ending with `end`.
pub fn ready<T>(meter: &Arc<Meter>, chunks: Vec<(T, usize)>, end: Result<()>) -> Chunks<T> {
    let (tx, rx) = crossbeam_channel::unbounded();
    for (rows, bytes) in chunks {
        let _ = tx.send(Ok(Chunk::new(rows, meter.charge(bytes))));
    }
    if let Err(e) = end {
        let _ = tx.send(Err(e));
    }
    Chunks { rx }
}

/// A stream that fails with `e` at once.
pub fn failed<T>(e: SqlError) -> Chunks<T> {
    let (tx, rx) = crossbeam_channel::unbounded();
    let _ = tx.send(Err(e));
    Chunks { rx }
}

/// The error of a stream stopped by a cancel.
pub fn cancelled() -> SqlError {
    SqlError::new(codes::QUERY_CANCELED, "canceling statement due to user request")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// A producer waits for its consumer with a bounded number of chunks
    /// in flight, each counted until the consumer drops it; it stops when
    /// the consumer goes away or when told to.
    #[test]
    fn producers_wait_for_their_consumer_and_stop() {
        let meter = Meter::new();
        let (tx, mut rx) = bounded::<Vec<u8>>(meter.clone(), Box::new(|| false));
        let m = meter.clone();
        let t = std::thread::spawn(move || {
            for i in 0..10u8 {
                if !tx.send(vec![i; 100], 1000) {
                    return i;
                }
                // (never more than the chunks in flight and the one taken)
                assert!(m.used() <= (IN_FLIGHT + 2) * 1000, "{} bytes in flight", m.used());
            }
            10
        });
        let first = rx.next().unwrap().unwrap();
        assert_eq!(first[0], 0);
        std::thread::sleep(Duration::from_millis(50));
        assert!(meter.used() <= (IN_FLIGHT + 2) * 1000);
        drop(first);
        drop(rx);
        let sent = t.join().unwrap();
        assert!(sent < 10, "the producer went on after its consumer left");
        assert_eq!(meter.used(), 0);
        // told to stop while its consumer does not read
        let stop = Arc::new(AtomicBool::new(false));
        let s2 = stop.clone();
        let (tx, _rx) = bounded::<u8>(meter.clone(), Box::new(move || s2.load(Ordering::Relaxed)));
        let t = std::thread::spawn(move || (0..100).take_while(|i| tx.send(*i, 1)).count());
        std::thread::sleep(Duration::from_millis(50));
        stop.store(true, Ordering::Relaxed);
        assert_eq!(t.join().unwrap(), IN_FLIGHT);
    }
}
