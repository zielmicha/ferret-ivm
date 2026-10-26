//! Checkpoints in the background. The engine thread freezes a version of the
//! dirty nodes (`Env::begin_checkpoint`) and builds the application state;
//! a thread of its own serializes the state, writes the nodes, `fsync`s and
//! appends the manifest record (`CheckpointJob::run`); the engine thread,
//! which went on applying records and answering requests meanwhile, then
//! marks the written nodes clean (`Env::finish_checkpoint`), relocates some
//! nodes for GC and truncates the log. One checkpoint at a time.

use super::*;
use crate::storage::CheckpointDone;

/// A checkpoint being written.
pub(crate) struct InFlight {
    gsn: Gsn,
    done: Receiver<Result<CheckpointDone>>,
    waiters: Vec<Waiter>,
}

/// Someone waiting for a checkpoint to be committed.
pub(crate) enum Waiter {
    /// `EngineHandle::checkpoint`: the checkpoint's GSN.
    Request(Sender<Result<Gsn>>),
    /// The `CHECKPOINT` statement.
    Statement(Sender<Result<ExecResult>>),
}

impl Waiter {
    fn reply(self, r: &Result<Gsn>) {
        match self {
            Waiter::Request(tx) => {
                let _ = tx.send(r.clone());
            }
            Waiter::Statement(tx) => {
                let _ = tx.send(r.clone().map(|_| ExecResult::Command { tag: "CHECKPOINT".into() }));
            }
        }
    }
}

impl Engine {
    /// Ask for a checkpoint of everything applied so far: the reply comes
    /// once one begun after now is committed (at once without threads).
    pub(crate) fn request_checkpoint(&mut self, w: Waiter) {
        if self.handle.is_none() {
            let r = self.checkpoint().map(|_| self.processed);
            return w.reply(&r);
        }
        self.checkpoint_waiters.push(w);
    }

    /// Freeze the dirty nodes and the application state at the last applied
    /// GSN and write them on a thread of their own.
    pub(crate) fn begin_checkpoint(&mut self) -> Result<()> {
        let app = self.app_state();
        let gsn = self.processed;
        let job = self.env.begin_checkpoint(gsn)?;
        let wake = self.handle.as_ref().map(|h| h.tx.clone());
        let (tx, done) = bounded(1);
        std::thread::Builder::new().name("ivm-checkpoint".into()).spawn(move || {
            let r = serde_json::to_vec(&app).map_err(|e| SqlError::internal(e.to_string())).and_then(|bytes| job.run(&bytes));
            let _ = tx.send(r);
            if let Some(w) = wake {
                let _ = w.send(EngineMsg::Wake);
            }
        })?;
        self.checkpointing = Some(InFlight { gsn, done, waiters: std::mem::take(&mut self.checkpoint_waiters) });
        self.last_checkpoint = Instant::now();
        self.records_since_checkpoint = 0;
        Ok(())
    }

    /// Finish the checkpoint in progress if it has been written (`wait`:
    /// wait for it). A checkpoint that failed stops the engine: the store's
    /// append side went with it.
    pub(crate) fn poll_checkpoint(&mut self, wait: bool) -> Result<()> {
        let Some(c) = &self.checkpointing else { return Ok(()) };
        let r = if wait {
            c.done.recv().map_err(|_| SqlError::internal("checkpoint thread vanished"))
        } else {
            match c.done.try_recv() {
                Ok(r) => Ok(r),
                Err(crossbeam_channel::TryRecvError::Empty) => return Ok(()),
                Err(_) => Err(SqlError::internal("checkpoint thread vanished")),
            }
        };
        let c = self.checkpointing.take().unwrap();
        let r = r.and_then(|r| r).and_then(|done| self.env.finish_checkpoint(done)).and_then(|_| self.after_checkpoint(c.gsn));
        let reply = r.as_ref().map(|_| c.gsn).map_err(|e| e.clone());
        for w in c.waiters {
            w.reply(&reply);
        }
        r
    }

    /// Wait for the checkpoint in progress to be written, without finishing
    /// it (a simulated crash).
    pub(crate) fn abandon_checkpoint(&mut self) {
        if let Some(c) = self.checkpointing.take() {
            let _ = c.done.recv();
        }
    }
}
