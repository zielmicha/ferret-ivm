//! Large reads off the engine thread. A read runs on the engine thread with
//! a budget of entries it may visit (and of rows it may return); one that
//! exceeds it is run again on a reader thread, against a fork of the
//! storage taken at once (every tree as of the last applied GSN,
//! `Env::fork`) and a copy of the part of the graph it reads: the same
//! state and plan, hence the same result, while the engine goes on. The
//! budget bounds what a read costs the engine thread; small reads (point
//! lookups, short ranges) cost no fork.
//!
//! The snapshots that a subscription `WITH SNAPSHOT` starts with and that a
//! sink resynchronises its target from are taken the same way: within the
//! budget on the engine thread, else on a reader thread from a fork as of
//! the GSN the subscription or the snapshot starts at. Either way the
//! consumer gets the snapshot as a stream of chunks (`coord::stream`) in
//! its queue at that GSN, before the changes after it: a reader thread
//! sends the chunks as the consumer takes them, a few at a time, so that a
//! snapshot of any size reaches a slow consumer in bounded memory, and the
//! engine keeps nothing for it.

use super::stream::{ChunkSender, Chunks};
use super::*;
use crate::dataflow::Delta;
use crate::exec::ExecCtx;
use crate::memory::Meter;

/// Entries a read may visit on the engine thread.
pub(crate) const INLINE_READ_ENTRIES: u64 = 50_000;

/// Reader threads (each read holds a fork with a buffer pool of its own).
const READERS: usize = 4;

/// Work for a reader thread, with the fork it reads.
pub(crate) enum ReadJob {
    /// A read, answered to its client.
    Query {
        env: Env,
        graph: Graph,
        /// Relation id -> its node, for the relations the plan reads.
        objects: Vec<(ObjectId, NodeId)>,
        bq: Box<crate::sql::BoundQuery>,
        settings: Arc<BTreeMap<String, String>>,
        interrupts: Option<Arc<Interrupts>>,
        reply: Sender<Result<ExecResult>>,
        meter: Arc<Meter>,
    },
    /// A node's snapshot, streamed to its consumer.
    Snapshot { env: Env, graph: Graph, node: NodeId, tx: ChunkSender<Delta> },
}

fn run(job: ReadJob) {
    match job {
        ReadJob::Query { mut env, graph, objects, bq, settings, interrupts, reply, meter } => {
            let objects = |oid: u64| objects.iter().find(|(o, _)| *o == oid).map(|(_, n)| *n);
            let mut ctx = ExecCtx { g: &graph, env: &mut env, objects: &objects, rec: Default::default(), visited: 0 };
            // the rows go to the session in chunks as they are found; a
            // session that stops reading them (or a cancel) ends the read
            let stop = interrupts.clone();
            let stop = Box::new(move || stop.as_ref().is_some_and(|i| i.cancelled() || i.gone()));
            let mut res = super::read::ChunkedReply::streaming(super::read::result_columns(&bq), reply, meter, stop);
            let r = crate::expr::with_session_settings(settings, || {
                ctx.run_query(&bq, &mut |row| {
                    if interrupts.as_ref().is_some_and(|i| i.cancelled()) {
                        return Err(super::stream::cancelled());
                    }
                    res.push(row)
                })
            });
            // (stopped by a cancel while the session did not take its rows)
            let r = match r {
                Ok(()) if interrupts.as_ref().is_some_and(|i| i.cancelled()) => Err(super::stream::cancelled()),
                r => r,
            };
            res.finish(r);
        }
        ReadJob::Snapshot { mut env, graph, node, tx } => {
            if let Err(e) = snapshot_chunks(&graph, &mut env, node, &mut |rows, mem| tx.send(rows, mem)) {
                tx.fail(e);
            }
        }
    }
}

/// The rows of `node` now, handed to `f` in chunks of `SNAPSHOT_CHUNK_ROWS`
/// rows or about `SNAPSHOT_CHUNK_BYTES` of memory, with their memory, until
/// `f` returns false.
fn snapshot_chunks(graph: &Graph, env: &mut Env, node: NodeId, f: &mut dyn FnMut(Delta, usize) -> bool) -> Result<()> {
    use super::read::{SNAPSHOT_CHUNK_BYTES, SNAPSHOT_CHUNK_ROWS};
    let (mut chunk, mut mem, mut stopped) = (Vec::new(), 0, false);
    let r = crate::dataflow::ops::snapshot_stream(graph, env, node, &mut |_, batch| {
        for (row, w) in batch {
            mem += crate::dataflow::delta::row_mem(&row);
            chunk.push((row, w));
            if (chunk.len() >= SNAPSHOT_CHUNK_ROWS || mem >= SNAPSHOT_CHUNK_BYTES) && !f(std::mem::take(&mut chunk), std::mem::take(&mut mem)) {
                stopped = true;
                return Err(super::stream::cancelled());
            }
        }
        Ok(())
    });
    match r {
        Err(_) if stopped => Ok(()),
        Err(e) => Err(e),
        Ok(_) => {
            if !chunk.is_empty() {
                f(chunk, mem);
            }
            Ok(())
        }
    }
}

impl Engine {
    /// Run a read that exceeded its budget on a reader thread, against the
    /// state it was planned in (the engine has applied nothing since).
    pub(crate) fn read_elsewhere(&mut self, bq: crate::sql::BoundQuery, vars: SessionVars, reply: Sender<Result<ExecResult>>) -> Result<()> {
        let objects: Vec<(ObjectId, NodeId)> = bq.rel.reads()
            .filter_map(|id| self.rt.node(id).map(|n| (id, n)))
            .collect();
        let graph = self.graph.fork_of(&objects.iter().map(|(_, n)| *n).collect::<Vec<_>>());
        let env = self.fork_env()?;
        let meter = self.shared.mem.outputs.clone();
        let job = ReadJob::Query { env, graph, objects, bq: Box::new(bq), settings: vars.settings, interrupts: vars.interrupts, reply, meter };
        self.send_to_reader(job)
    }

    /// End a read's budget on the engine thread, counting the entries it
    /// visited there.
    pub(crate) fn end_inline_read(&mut self) {
        if let Some(left) = self.env.read_budget() {
            self.reads.inline_entries += INLINE_READ_ENTRIES.saturating_sub(left);
        }
        self.env.set_read_budget(None);
    }

    fn send_to_reader(&mut self, job: ReadJob) -> Result<()> {
        self.reads.offloaded += 1;
        let tx = match &self.readers {
            Some(tx) => tx.clone(),
            None => {
                let (tx, rx) = unbounded::<ReadJob>();
                for i in 0..READERS {
                    let rx = rx.clone();
                    // (the engine thread's stack: the same executor runs here)
                    std::thread::Builder::new().name(format!("ivm-reader-{i}")).stack_size(64 << 20).spawn(move || {
                        for job in rx.iter() {
                            run(job);
                        }
                    })?;
                }
                self.readers = Some(tx.clone());
                tx
            }
        };
        tx.send(job).map_err(|_| SqlError::internal("reader threads stopped"))
    }

    /// The snapshot of `node` at the last applied GSN, as a stream of
    /// chunks charged to `meter`: read on the engine thread if that takes
    /// no more than the read budget (always when replaying or without
    /// threads), else on a reader thread from a fork, which sends the chunks
    /// as the consumer takes them and gives up when `stop` returns true
    /// while it waits.
    pub(crate) fn snapshot_stream(&mut self, node: NodeId, meter: &Arc<Meter>, stop: Box<dyn Fn() -> bool + Send>) -> Result<Chunks<Delta>> {
        let inline = self.handle.is_none() || self.replaying;
        let mut chunks = Vec::new();
        if !inline {
            self.env.set_read_budget(Some(INLINE_READ_ENTRIES));
        }
        let r = snapshot_chunks(&self.graph, &mut self.env, node, &mut |rows, mem| {
            chunks.push((rows, mem));
            true
        });
        self.end_inline_read();
        match r {
            Err(e) if e.code == crate::storage::READ_BUDGET => {}
            Err(e) => return Err(e),
            Ok(()) => return Ok(super::stream::ready(meter, chunks, Ok(()))),
        }
        drop(chunks);
        let graph = self.graph.fork_of(&[node]);
        let env = self.fork_env()?;
        let (tx, rx) = super::stream::bounded(meter.clone(), stop);
        self.send_to_reader(ReadJob::Snapshot { env, graph, node, tx })?;
        Ok(rx)
    }
}
