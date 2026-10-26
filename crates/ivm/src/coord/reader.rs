//! Large reads off the engine thread. A read runs on the engine thread with
//! a budget of entries it may visit; one that exceeds it is run again on a
//! reader thread, against a fork of the storage taken at once (every tree
//! as of the last applied GSN, `Env::fork`) and a copy of the part of the
//! graph it reads: the same read point and plan, hence the same result,
//! while the engine goes on. The budget bounds what a read costs the engine
//! thread; small reads (point lookups, short ranges) cost no fork.
//!
//! The snapshots that a subscription `WITH SNAPSHOT` to a relation and a
//! sink start from are taken the same way: within the budget on the engine
//! thread, else on a reader thread as of the GSN the subscription or sink
//! starts at, while the outputs of the relation's node at the records
//! applied meanwhile are kept, to be delivered after the snapshot.

use super::*;
use crate::exec::ExecCtx;
use crate::storage::ReadAt;

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
        at: ReadAt,
        settings: Arc<BTreeMap<String, String>>,
        interrupts: Option<Arc<Interrupts>>,
        reply: Sender<Result<ExecResult>>,
    },
    /// A node's snapshot, handed back to the engine (which it wakes).
    Snapshot { env: Env, graph: Graph, node: NodeId, done: Sender<Result<Output>>, wake: Sender<EngineMsg> },
}

fn run(job: ReadJob) {
    match job {
        ReadJob::Query { mut env, graph, objects, bq, at, settings, interrupts, reply } => {
            let objects = |oid: u64| objects.iter().find(|(o, _)| *o == oid).map(|(_, n)| *n);
            let mut ctx = ExecCtx { g: &graph, env: &mut env, at, objects: &objects, rec: Default::default(), visited: 0 };
            // the rows go to the session in chunks as they are found; a
            // session that stops reading them (or a cancel) ends the read
            let mut res = super::read::ChunkedReply::new(super::read::result_columns(&bq), reply);
            let r = crate::expr::with_session_settings(settings, || {
                ctx.run_query(&bq, &mut |row| {
                    if interrupts.as_ref().is_some_and(|i| i.cancelled()) {
                        return Err(SqlError::new(codes::QUERY_CANCELED, "canceling statement due to user request"));
                    }
                    Ok(res.push(row))
                })
            });
            res.finish(r);
        }
        ReadJob::Snapshot { mut env, graph, node, done, wake } => {
            let _ = done.send(graph.snapshot(&mut env, node));
            let _ = wake.send(EngineMsg::Wake);
        }
    }
}

/// A snapshot being taken on a reader thread for a subscription or a sink
/// that starts at `gsn`, and the outputs of its node at the records applied
/// since (None: it did not change), delivered after it.
pub(crate) struct Snapshotting {
    gsn: Gsn,
    pub(crate) node: NodeId,
    done: Receiver<Result<Output>>,
    outputs: Vec<(Gsn, Option<Output>)>,
    then: AfterSnapshot,
}

pub(crate) enum AfterSnapshot {
    Subscription(super::hydrate::PendingSub),
}

impl Engine {
    /// Run a read that exceeded its budget on a reader thread, as of `at`
    /// (the engine has applied nothing since it was planned).
    pub(crate) fn read_elsewhere(&mut self, bq: crate::sql::BoundQuery, at: ReadAt, vars: SessionVars, reply: Sender<Result<ExecResult>>) -> Result<()> {
        fn gets(r: &crate::sql::Rel, out: &mut Vec<ObjectId>) {
            if let crate::sql::Rel::Get { id, .. } = r {
                out.push(*id);
            }
            for i in r.inputs() {
                gets(i, out);
            }
        }
        let mut ids = Vec::new();
        gets(&bq.rel, &mut ids);
        let objects: Vec<(ObjectId, NodeId)> = ids
            .into_iter()
            .filter_map(|id| self.rt.tables.get(&id).copied().or_else(|| self.rt.views.get(&id).map(|v| v.node)).map(|n| (id, n)))
            .collect();
        let graph = self.graph.fork_of(&objects.iter().map(|(_, n)| *n).collect::<Vec<_>>());
        let env = self.fork_env()?;
        self.send_to_reader(ReadJob::Query { env, graph, objects, bq: Box::new(bq), at, settings: vars.settings, interrupts: vars.interrupts, reply })
    }

    fn send_to_reader(&mut self, job: ReadJob) -> Result<()> {
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

    /// The snapshot of `node` now, if it is taken within the read budget
    /// (always when replaying or without threads); else it is taken on a
    /// reader thread, as of the last applied GSN, and `then` follows it
    /// (`poll_snapshots`).
    pub(crate) fn snapshot_or_defer(&mut self, node: NodeId, then: AfterSnapshot) -> Result<Option<(Output, AfterSnapshot)>> {
        let Some(wake) = self.handle.as_ref().filter(|_| !self.replaying).map(|h| h.tx.clone()) else {
            return Ok(Some((self.graph.snapshot(&mut self.env, node)?, then)));
        };
        self.env.set_read_budget(Some(INLINE_READ_ENTRIES));
        let r = self.graph.snapshot(&mut self.env, node);
        self.env.set_read_budget(None);
        match r {
            Err(e) if e.code == crate::storage::READ_BUDGET => {}
            r => return Ok(Some((r?, then))),
        }
        let graph = self.graph.fork_of(&[node]);
        let env = self.fork_env()?;
        let (tx, done) = bounded(1);
        self.send_to_reader(ReadJob::Snapshot { env, graph, node, done: tx, wake })?;
        self.snapshotting.push(Snapshotting { gsn: self.processed, node, done, outputs: Vec::new(), then });
        Ok(None)
    }

    /// Keep a record's outputs for the snapshots being taken.
    pub(crate) fn keep_for_snapshots(&mut self, gsn: Gsn, outputs: &FxHashMap<NodeId, Output>) {
        for s in self.snapshotting.iter_mut() {
            s.outputs.push((gsn, outputs.get(&s.node).cloned()));
        }
    }

    /// Start the subscriptions and sinks whose snapshots have been taken,
    /// with the outputs kept since.
    pub(crate) fn poll_snapshots(&mut self) -> Result<()> {
        let mut i = 0;
        while i < self.snapshotting.len() {
            let r = match self.snapshotting[i].done.try_recv() {
                Ok(r) => r,
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    i += 1;
                    continue;
                }
                Err(_) => Err(SqlError::internal("reader thread vanished")),
            };
            let s = self.snapshotting.remove(i);
            let snapshot = r?;
            self.after_snapshot(s.then, s.gsn, snapshot, s.outputs)?;
        }
        Ok(())
    }

    /// Start a subscription or a sink from its snapshot as of `gsn` and the
    /// outputs of its node at the records applied since.
    pub(crate) fn after_snapshot(&mut self, then: AfterSnapshot, gsn: Gsn, snapshot: Output, outputs: Vec<(Gsn, Option<Output>)>) -> Result<()> {
        match then {
            AfterSnapshot::Subscription(p) => {
                let steps: Vec<(Gsn, FxHashMap<NodeId, Output>)> =
                    outputs.into_iter().map(|(g, o)| (g, o.map(|o| (p.node, o)).into_iter().collect())).collect();
                self.start_subscription(p, gsn, Some(snapshot), &steps);
            }
        }
        Ok(())
    }
}
