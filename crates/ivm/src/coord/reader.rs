//! Large reads off the engine thread. A read runs on the engine thread with
//! a budget of entries it may visit; one that exceeds it is run again on a
//! reader thread, against a fork of the storage taken at once (every tree
//! as of the last applied GSN, `Env::fork`) and a copy of the part of the
//! graph it reads: the same read point and plan, hence the same result,
//! while the engine goes on. The budget bounds what a read costs the engine
//! thread; small reads (point lookups, short ranges) cost no fork.

use super::*;
use crate::exec::{ExecCtx, Rows};
use crate::storage::ReadAt;

/// Entries a read may visit on the engine thread.
pub(crate) const INLINE_READ_ENTRIES: u64 = 50_000;

/// Reader threads (each read holds a fork with a buffer pool of its own).
const READERS: usize = 4;

/// A read to run on a reader thread.
pub(crate) struct ReadJob {
    env: Env,
    graph: Graph,
    /// Relation id -> its node, for the relations the plan reads.
    objects: Vec<(ObjectId, NodeId)>,
    bq: crate::sql::BoundQuery,
    at: ReadAt,
    settings: Arc<BTreeMap<String, String>>,
    reply: Sender<Result<ExecResult>>,
}

fn run(mut job: ReadJob) -> Result<ExecResult> {
    let objects = |oid: u64| job.objects.iter().find(|(o, _)| *o == oid).map(|(_, n)| *n);
    let mut ctx = ExecCtx { g: &job.graph, env: &mut job.env, at: job.at, objects: &objects, rec: Default::default() };
    let rows: Rows = crate::expr::with_session_settings(job.settings.clone(), || ctx.execute(&job.bq.rel))?;
    Ok(super::read::finish_read(job.bq, rows))
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
        let env = self.env.fork()?;
        let tx = match &self.readers {
            Some(tx) => tx.clone(),
            None => {
                let (tx, rx) = unbounded::<ReadJob>();
                for i in 0..READERS {
                    let rx = rx.clone();
                    // (the engine thread's stack: the same executor runs here)
                    std::thread::Builder::new().name(format!("ivm-reader-{i}")).stack_size(64 << 20).spawn(move || {
                        for job in rx.iter() {
                            let reply = job.reply.clone();
                            let _ = reply.send(run(job));
                        }
                    })?;
                }
                self.readers = Some(tx.clone());
                tx
            }
        };
        let job = ReadJob { env, graph, objects, bq, at, settings: vars.settings, reply };
        tx.send(job).map_err(|_| SqlError::internal("reader threads stopped"))
    }
}
