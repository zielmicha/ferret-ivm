//! Reads: point-in-time SELECT (fresh / AS OF GSN / at least LSN),
//! SUBSCRIBE, SHOW, EXPLAIN.

use super::*;
use crate::catalog::ObjectKind;
use crate::dataflow::lower::Lowerer;
use crate::dataflow::NodeKind;
use crate::exec::{sort_rows, ExecCtx, Rows};
use crate::sql::parser::{AsOf, ExplainKind, ExplainTarget, ShowKind, Statement, SubscribeTarget};
use crate::sql::plan::Rel;
use crate::sql::ParamSlot;
use crate::sql_bail;
use crate::storage::ReadAt;
use crate::types::{ColumnType, Datum, Row, SqlType};
use sqlparser::ast;

fn col(name: &str, ty: SqlType) -> ResultColumn {
    ResultColumn { name: name.into(), ty, typmod: -1 }
}

fn text_rows(columns: &[&str], rows: Vec<Vec<String>>) -> ExecResult {
    ExecResult::Query {
        columns: columns.iter().map(|c| col(c, SqlType::Text)).collect(),
        rows: rows.into_iter().map(|r| r.into_iter().map(Datum::text_owned).collect()).collect(),
    }
}

/// The error ending a waiting request whose statement was interrupted: by a
/// cancel, or (for a read, which has no effects) because nobody waits for
/// its result any more. Queued DML of a client that is gone still runs.
fn interruption(req: &Request, read: bool) -> Option<SqlError> {
    let Request::Execute { vars, .. } = req else { return None };
    let i = vars.interrupts.as_ref()?;
    if i.cancelled() {
        return Some(SqlError::new(codes::QUERY_CANCELED, "canceling statement due to user request"));
    }
    (read && i.gone()).then(|| SqlError::new(codes::QUERY_CANCELED, "canceling statement: the client is gone"))
}

pub fn parse_lsn(s: &str) -> Result<u64> {
    let (hi, lo) = s.split_once('/').ok_or_else(|| SqlError::new(codes::INVALID_TEXT_REPRESENTATION, format!("invalid LSN \"{s}\"")))?;
    let hi = u64::from_str_radix(hi.trim(), 16).map_err(|_| SqlError::new(codes::INVALID_TEXT_REPRESENTATION, format!("invalid LSN \"{s}\"")))?;
    let lo = u64::from_str_radix(lo.trim(), 16).map_err(|_| SqlError::new(codes::INVALID_TEXT_REPRESENTATION, format!("invalid LSN \"{s}\"")))?;
    Ok((hi << 32) | lo)
}

impl Engine {
    pub(crate) fn execute(
        &mut self,
        stmt: Statement,
        params: Vec<ParamSlot>,
        vars: SessionVars,
        extra: String,
        reply: Sender<Result<ExecResult>>,
        _handle: &EngineHandle,
    ) -> Result<Option<ExecResult>> {
        if Self::is_ddl(&stmt) {
            return self.submit_ddl(&stmt, extra, reply);
        }
        match stmt {
            Statement::Select { query, as_of, .. } => self.select(query, as_of, params, vars, reply),
            Statement::Subscribe { target, snapshot, progress, as_of: _, copy: _ } => {
                self.subscribe(target, snapshot, progress, params).map(Some)
            }
            Statement::Show(k) => self.show(k).map(Some),
            Statement::Explain { kind, target } => self.explain(kind, target, params).map(Some),
            Statement::Checkpoint => {
                self.checkpoint()?;
                Ok(Some(ExecResult::Command { tag: "CHECKPOINT".into() }))
            }
            Statement::Empty => Ok(Some(ExecResult::Command { tag: String::new() })),
            Statement::Sql(s) => match *s {
                ast::Statement::Insert(_) | ast::Statement::Update(_) | ast::Statement::Delete(_) => {
                    self.execute_dml(&s, params, reply)
                }
                ast::Statement::StartTransaction { .. } => Ok(Some(ExecResult::Command { tag: "BEGIN".into() })),
                ast::Statement::Commit { .. } => Ok(Some(ExecResult::Command { tag: "COMMIT".into() })),
                ast::Statement::Rollback { .. } => Ok(Some(ExecResult::Command { tag: "ROLLBACK".into() })),
                ast::Statement::Set(_) => Ok(Some(ExecResult::Command { tag: "SET".into() })),
                ast::Statement::ShowVariable { variable } => {
                    let name = variable.iter().map(|i| i.value.to_lowercase()).collect::<Vec<_>>().join(" ");
                    self.show(ShowKind::Variable(name)).map(Some)
                }
                ast::Statement::Explain { statement, .. } => match *statement {
                    ast::Statement::Query(q) => self.explain(ExplainKind::Plan, ExplainTarget::Query(q), params).map(Some),
                    other => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "EXPLAIN is only supported for queries: {other}"),
                },
                ast::Statement::Query(q) => self.select(q, AsOf::Default, params, vars, reply),
                ast::Statement::Discard { .. } | ast::Statement::Deallocate { .. } => {
                    Ok(Some(ExecResult::Command { tag: "DISCARD ALL".into() }))
                }
                other => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unsupported statement: {other}"),
            },
            other => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unsupported statement: {other:?}"),
        }
    }

    /// Run a logical plan against current (or historical) state.
    pub(crate) fn execute_rel(&mut self, rel: &Rel, at: ReadAt) -> Result<Rows> {
        let rt = &self.rt;
        let objects = |oid: u64| -> Option<NodeId> { rt.tables.get(&oid).copied().or_else(|| rt.views.get(&oid).map(|v| v.node)) };
        let mut ctx = ExecCtx { g: &self.graph, env: &mut self.env, at, objects: &objects, rec: Default::default() };
        ctx.execute(rel)
    }

    fn check_view_errors(&self, rel: &Rel) -> Result<()> {
        fn gets(r: &Rel, out: &mut Vec<u64>) {
            if let Rel::Get { id, .. } = r {
                out.push(*id);
            }
            for i in r.inputs() {
                gets(i, out);
            }
        }
        let mut ids = Vec::new();
        gets(rel, &mut ids);
        for id in ids {
            if let Some(v) = self.rt.views.get(&id)
                && let Some(((code, msg), _)) = v.errors.iter().find(|(_, w)| **w > 0)
            {
                return Err(SqlError::new(code, msg.clone()));
            }
        }
        Ok(())
    }

    /// Replicated tables whose initial snapshot is still loading (their
    /// contents are no state the upstream ever had); cached per epoch.
    fn loading_tables(&mut self) -> &std::collections::HashSet<ObjectId> {
        if self.loading.0 != self.snap_epoch || self.snap_epoch == 0 {
            let mut states: HashMap<ObjectId, crate::source::postgres::SourceState> = HashMap::new();
            let mut set = std::collections::HashSet::new();
            for o in self.catalog.objects.values() {
                if let ObjectKind::Table(t) = &o.kind
                    && let Some(src) = t.source
                {
                    let st = states
                        .entry(src)
                        .or_insert_with(|| self.source_states.get(&src).and_then(|s| serde_json::from_str(s).ok()).unwrap_or_default());
                    if !st.tables.get(&o.id).is_some_and(|s| s.done) {
                        set.insert(o.id);
                    }
                }
            }
            self.loading = (self.snap_epoch, set);
        }
        &self.loading.1
    }

    /// A table the plan reads (directly or through views) whose initial
    /// snapshot is still loading.
    fn snapshotting_table(&mut self, rel: &Rel) -> Option<ObjectId> {
        if self.loading_tables().is_empty() {
            return None;
        }
        fn gets(r: &Rel, out: &mut Vec<u64>) {
            if let Rel::Get { id, .. } = r {
                out.push(*id);
            }
            for i in r.inputs() {
                gets(i, out);
            }
        }
        let mut ids = Vec::new();
        gets(rel, &mut ids);
        // views read their tables' current contents too
        let mut seen = std::collections::HashSet::new();
        while let Some(id) = ids.pop() {
            if !seen.insert(id) {
                continue;
            }
            match self.catalog.get(id).map(|o| &o.kind) {
                Some(ObjectKind::Table(_)) => {
                    if self.loading.1.contains(&id) {
                        return Some(id);
                    }
                }
                Some(ObjectKind::View(v)) | Some(ObjectKind::MaterializedView(v)) => {
                    if let Ok(q) = crate::sql::parser::parse_single_query(&v.sql)
                        && let Ok(bq) = crate::sql::plan_stored_query(&self.catalog, &q)
                    {
                        gets(&bq.rel, &mut ids);
                    }
                }
                _ => {}
            }
        }
        None
    }

    fn select(
        &mut self,
        query: Box<ast::Query>,
        as_of: AsOf,
        params: Vec<ParamSlot>,
        vars: SessionVars,
        reply: Sender<Result<ExecResult>>,
    ) -> Result<Option<ExecResult>> {
        // fresh reads wait for the initial snapshots of what they read
        let fresh = match &as_of {
            AsOf::Default => vars.read_mode != "latest",
            AsOf::Fresh | AsOf::AtLeastLsn(_) => true,
            AsOf::Gsn(_) | AsOf::Latest => false,
        };
        if fresh && !self.loading_tables().is_empty() {
            let bq = crate::sql::plan_query(&self.catalog, &query, params.clone())?;
            if self.snapshotting_table(&bq.rel).is_some() {
                self.parked.push(Parked {
                    wait: Wait::Snapshot,
                    req: Request::Execute {
                        stmt: Box::new(Statement::Select { query, as_of, text: String::new() }),
                        params,
                        vars,
                        extra: String::new(),
                        reply,
                    },
                });
                return Ok(None);
            }
        }
        let target = match &as_of {
            AsOf::Gsn(g) => *g,
            AsOf::Latest => self.processed,
            AsOf::Default if vars.read_mode == "latest" => self.processed,
            AsOf::Default | AsOf::Fresh => self.shared.durable(),
            AsOf::AtLeastLsn(l) => {
                let lsn = parse_lsn(l)?;
                if !self.lsn_reached(l, lsn)? {
                    self.shared.lsn_waiters.store(true, Ordering::Release);
                    self.shared.want_keepalive.store(true, Ordering::Release);
                    self.parked.push(Parked {
                        wait: Wait::Lsn,
                        req: Request::Execute {
                            stmt: Box::new(Statement::Select { query, as_of: as_of.clone(), text: String::new() }),
                            params,
                            vars,
                            extra: String::new(),
                            reply,
                        },
                    });
                    return Ok(None);
                }
                self.shared.durable()
            }
        };
        if target > self.processed {
            self.parked.push(Parked {
                wait: Wait::Gsn(target),
                req: Request::Execute {
                    stmt: Box::new(Statement::Select { query, as_of: AsOf::Gsn(target), text: String::new() }),
                    params,
                    vars,
                    extra: String::new(),
                    reply,
                },
            });
            return Ok(None);
        }
        let at = if target >= self.processed {
            ReadAt::Latest
        } else {
            if target < self.env.frontier() {
                sql_bail!(
                    codes::OBJECT_NOT_IN_PREREQUISITE_STATE,
                    "AS OF {target} is before the retention frontier {}",
                    self.env.frontier()
                );
            }
            ReadAt::AsOf(target)
        };
        let mut bq = crate::sql::plan_query(&self.catalog, &query, params)?;
        self.check_broken(&bq.rel)?;
        self.check_view_errors(&bq.rel)?;
        // now() is the logical clock as of the read, so reads are repeatable
        bq.rel = std::mem::replace(&mut bq.rel, crate::sql::Rel::unit()).bind_now(self.clock_at(target));
        let mut rows = self.execute_rel(&bq.rel, at)?;
        crate::dataflow::consolidate(&mut rows);
        rows.retain(|(_, w)| *w > 0);
        if !bq.order.is_empty() {
            sort_rows(&mut rows, &bq.order);
        }
        // expand multiplicities, apply OFFSET/LIMIT, project visible columns
        let mut out: Vec<Row> = Vec::new();
        let mut skipped = 0u64;
        let limit = bq.limit.unwrap_or(u64::MAX);
        let mut last: Option<Row> = None;
        'outer: for (r, w) in rows {
            for _ in 0..w {
                if skipped < bq.offset {
                    skipped += 1;
                    continue;
                }
                if out.len() as u64 >= limit {
                    // FETCH ... WITH TIES keeps rows tied with the last one
                    let tied = bq.with_ties
                        && last.as_ref().is_some_and(|l| bq.order.iter().all(|k| l[k.col].sql_cmp(&r[k.col]) == std::cmp::Ordering::Equal));
                    if !tied {
                        break 'outer;
                    }
                }
                out.push(r[..bq.visible].to_vec());
                last = Some(r.clone());
            }
        }
        let types = bq.column_types();
        let columns = bq
            .names
            .iter()
            .take(bq.visible)
            .zip(types)
            .map(|(n, t)| ResultColumn { name: n.clone(), ty: t.ty, typmod: t.typmod })
            .collect();
        Ok(Some(ExecResult::Query { columns, rows: out }))
    }

    /// Has every source's LSN frontier reached `lsn` (written `text`)? A
    /// source that has not reported a frontier yet (it is snapshotting or
    /// has not connected) has not. An error without any source: the LSN is
    /// a position in a source's upstream WAL, so nothing could ever reach
    /// it, and reading at once could not show the commit the client waits
    /// for (read-your-writes).
    pub(crate) fn lsn_reached(&self, text: &str, lsn: u64) -> Result<bool> {
        let sources: Vec<ObjectId> =
            self.catalog.objects.values().filter(|o| matches!(o.kind, ObjectKind::Source(_))).map(|o| o.id).collect();
        if sources.is_empty() {
            return Err(SqlError::new(
                codes::OBJECT_NOT_IN_PREREQUISITE_STATE,
                format!("AS OF AT LEAST LSN '{text}' cannot be satisfied: there is no source"),
            )
            .with_hint("The LSN is a position in the WAL of a source's upstream server; create the source first."));
        }
        let frontiers = self.shared.source_lsn.lock();
        Ok(sources.iter().all(|s| frontiers.get(s).is_some_and(|f| *f >= lsn)))
    }

    /// Re-run parked reads whose GSN (or LSN) condition is now satisfied;
    /// end those (and queued DML) whose statement was interrupted.
    pub(crate) fn wake_parked(&mut self, _handle: &EngineHandle) {
        if !self.dml_queue.is_empty() {
            for req in std::mem::take(&mut self.dml_queue) {
                match interruption(&req, false) {
                    Some(e) => {
                        if let Request::Execute { reply, .. } = &req {
                            let _ = reply.send(Err(e));
                        }
                    }
                    None => self.dml_queue.push_back(req),
                }
            }
        }
        if self.parked.is_empty() {
            return;
        }
        // snapshot progress or catalog changes since reads were parked
        let snapshots_changed = self.snap_epoch != self.snap_epoch_seen;
        self.snap_epoch_seen = self.snap_epoch;
        let mut still = Vec::new();
        for p in std::mem::take(&mut self.parked) {
            if let Some(e) = interruption(&p.req, true) {
                if let Request::Execute { reply, .. } = &p.req {
                    let _ = reply.send(Err(e));
                }
                continue;
            }
            let ready = match p.wait {
                // (re-run on an error: it fails then, e.g. when the last
                // source was dropped)
                Wait::Lsn => match &p.req {
                    Request::Execute { stmt, .. } => match &**stmt {
                        Statement::Select { as_of: AsOf::AtLeastLsn(l), .. } => {
                            parse_lsn(l).and_then(|lsn| self.lsn_reached(l, lsn)).unwrap_or(true)
                        }
                        _ => true,
                    },
                    _ => true,
                },
                // re-run: it parks again if a snapshot is still loading
                Wait::Snapshot => snapshots_changed,
                Wait::Gsn(g) => self.processed >= g,
            };
            if ready {
                self.deferred.push(p.req);
            } else {
                still.push(p);
            }
        }
        let lsn_waiting = still.iter().any(|p| p.wait == Wait::Lsn);
        self.shared.lsn_waiters.store(lsn_waiting, Ordering::Release);
        if lsn_waiting {
            // keep asking until the frontier passes
            self.shared.want_keepalive.store(true, Ordering::Release);
        }
        self.parked = still;
    }

    pub(super) fn subscribe(&mut self, target: SubscribeTarget, snapshot: bool, progress: bool, params: Vec<ParamSlot>) -> Result<ExecResult> {
        let mut view = None;
        let (node, pinned, columns, reads): (NodeId, Option<NodeId>, Vec<ResultColumn>, Vec<ObjectId>) = match target {
            SubscribeTarget::Name(name) => {
                let Some(obj) = self.catalog.by_name(&name).cloned() else {
                    sql_bail!(codes::UNDEFINED_TABLE, "relation \"{name}\" does not exist");
                };
                if let Some(e) = self.broken_error(obj.id) {
                    return Err(e);
                }
                match &obj.kind {
                    ObjectKind::MaterializedView(v) => {
                        view = Some(obj.id);
                        let rt = &self.rt.views[&obj.id];
                        let cols = v.columns.iter().map(|c| ResultColumn { name: c.name.clone(), ty: c.ty.ty.clone(), typmod: c.ty.typmod }).collect();
                        (rt.node, None, cols, vec![obj.id])
                    }
                    ObjectKind::Table(t) => (
                        self.rt.tables[&obj.id],
                        None,
                        t.columns.iter().map(|c| ResultColumn { name: c.name.clone(), ty: c.ty.ty.clone(), typmod: c.ty.typmod }).collect(),
                        vec![obj.id],
                    ),
                    ObjectKind::View(_) => {
                        // through the view (not its query): its column list
                        // names the columns, as in reads of it
                        let text = format!("SELECT * FROM \"{}\"", name.replace('"', "\"\""));
                        let q = crate::sql::parser::parse_single_query(&text)?;
                        return self.subscribe(SubscribeTarget::Query(Box::new(q), text), snapshot, progress, params);
                    }
                    _ => sql_bail!(codes::WRONG_OBJECT_TYPE, "cannot subscribe to {} \"{name}\"", obj.kind.name()),
                }
            }
            SubscribeTarget::Query(q, _) => {
                let bq = crate::sql::plan_query(&self.catalog, &q, params)?;
                super::ddl::check_deterministic(&bq.rel)?;
                super::ddl::check_catalog_reads(&bq)?;
                self.check_broken(&bq.rel)?;
                // what it reads: the relations of the plan, and the views
                // (inlined in it) it reads them through
                fn gets(r: &Rel, out: &mut Vec<ObjectId>) {
                    if let Rel::Get { id, .. } = r {
                        out.push(*id);
                    }
                    for i in r.inputs() {
                        gets(i, out);
                    }
                }
                let mut reads = self.catalog.references(&q.to_string()).unwrap_or_default();
                gets(&bq.rel, &mut reads);
                // and what the materialized views it reads read in turn: the
                // subscription's dataflow is built on their current versions,
                // so it ends when one of them is replaced or re-planned
                self.add_view_reads(&mut reads);
                reads.sort_unstable();
                reads.dedup();
                let names: Vec<String> = bq.names.iter().take(bq.visible).cloned().collect();
                let types: Vec<ColumnType> = bq.column_types();
                let rel = bq.into_rel();
                let node_mark = self.graph.node_count();
                let arr_mark = self.graph.arr_count();
                let rt = &self.rt;
                let objects = |oid: u64| -> Option<NodeId> { rt.tables.get(&oid).copied().or_else(|| rt.views.get(&oid).map(|v| v.node)) };
                let lowered = {
                    let mut lw = Lowerer { g: &mut self.graph, env: &mut self.env, objects: &objects };
                    lw.lower(&rel).map(|l| lw.materialize(l))
                };
                let node = match lowered {
                    Ok(n) => n,
                    Err(e) => {
                        // what lowering created before it failed is unreferenced
                        self.graph.gc_since(&mut self.env, node_mark, arr_mark)?;
                        return Err(e);
                    }
                };
                self.graph.retain_node(node);
                self.graph.gc_since(&mut self.env, node_mark, arr_mark)?;
                if let Err(e) = self.graph.hydrate(&mut self.env, self.processed) {
                    let _ = self.graph.release_node(&mut self.env, node);
                    return Err(e);
                }
                let cols = names.into_iter().zip(types).map(|(n, t)| ResultColumn { name: n, ty: t.ty, typmod: t.typmod }).collect();
                (node, Some(node), cols, reads)
            }
        };
        let (tx, rx) = unbounded();
        let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
        if snapshot {
            let snap = self.graph.snapshot(&mut self.env, node)?;
            if !snap.data.is_empty() {
                let _ = tx.send(SubEvent::Data { gsn: self.processed, rows: SharedRows::new(snap.data) });
            }
        }
        let _ = tx.send(SubEvent::Progress { gsn: self.processed, charge: crate::memory::Charge::none() });
        // (changes queued after the snapshot are bounded by its share of the memory budget)
        let queue = crate::memory::Meter::child(&self.shared.mem.outputs);
        self.subs.push(Subscription { node, tx, queue, cancel: cancel.clone(), pinned, progress, view, reads });
        Ok(ExecResult::Subscribe { columns, rx, cancel, progress })
    }

    /// Add to `reads` the relations that the materialized views in it read
    /// (directly or through views), transitively.
    fn add_view_reads(&self, reads: &mut Vec<ObjectId>) {
        let mut i = 0;
        while i < reads.len() {
            if let Some(ObjectKind::MaterializedView(v)) = self.catalog.get(reads[i]).map(|o| &o.kind) {
                for r in self.catalog.references(&v.sql).unwrap_or_default() {
                    if !reads.contains(&r) {
                        reads.push(r);
                    }
                }
            }
            i += 1;
        }
    }

    /// Result columns and parameter types without executing.
    pub(crate) fn describe(&mut self, stmt: &Statement, params: Vec<ParamSlot>) -> Result<(Vec<ResultColumn>, Vec<SqlType>)> {
        match stmt {
            Statement::Select { query, .. } => {
                let mut b = crate::sql::Binder::new(&self.catalog).with_params(params);
                let bq = b.bind_query(query)?;
                let types = bq.column_types();
                let cols = bq
                    .names
                    .iter()
                    .take(bq.visible)
                    .zip(types)
                    .map(|(n, t)| ResultColumn { name: n.clone(), ty: t.ty, typmod: t.typmod })
                    .collect();
                // still unknown: the session describes them as text but
                // keeps binding their values as untyped literals
                let ptypes = b.params.iter().map(|p| p.ty.clone()).collect();
                Ok((cols, ptypes))
            }
            Statement::Sql(s) => {
                // count parameters used by DML
                let text = s.to_string();
                let mut max = 0;
                let b = text.as_bytes();
                let mut i = 0;
                while i < b.len() {
                    if b[i] == b'$' {
                        let mut j = i + 1;
                        while j < b.len() && b[j].is_ascii_digit() {
                            j += 1;
                        }
                        if let Ok(n) = text[i + 1..j].parse::<usize>() {
                            max = max.max(n);
                        }
                        i = j;
                    } else {
                        i += 1;
                    }
                }
                let ptypes = (0..max).map(|k| params.get(k).map(|p| p.ty.clone()).unwrap_or(SqlType::Unknown)).collect();
                Ok((vec![], ptypes))
            }
            Statement::Show(_) | Statement::Explain { .. } => {
                // executed eagerly by the session; columns are reported then
                Ok((vec![], vec![]))
            }
            _ => Ok((vec![], vec![])),
        }
    }

    fn show(&mut self, k: ShowKind) -> Result<ExecResult> {
        Ok(match k {
            ShowKind::Sources => {
                let mut rows = Vec::new();
                for o in self.catalog.objects.values() {
                    if let ObjectKind::Source(s) = &o.kind {
                        let lsn = self.shared.source_lsn.lock().get(&o.id).copied().unwrap_or(0);
                        let status = self.source_handles.get(&o.id).map(|h| h.status()).unwrap_or_else(|| "stopped".into());
                        rows.push(vec![
                            o.name.clone(),
                            s.publication.clone(),
                            s.slot.clone(),
                            self.source_tables(o.id).len().to_string(),
                            format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF),
                            status,
                        ]);
                    }
                }
                text_rows(&["name", "publication", "slot", "tables", "lsn", "status"], rows)
            }
            ShowKind::Tables => {
                let mut rows = Vec::new();
                let tables: Vec<(ObjectId, String, Option<ObjectId>)> = self
                    .catalog
                    .objects
                    .values()
                    .filter_map(|o| match &o.kind {
                        ObjectKind::Table(t) => Some((o.id, o.name.clone(), t.source)),
                        _ => None,
                    })
                    .collect();
                for (id, name, source) in tables {
                    let n = match self.rt.tables.get(&id).copied() {
                        Some(node) => self.node_rows(node),
                        None => 0,
                    };
                    let src = source.and_then(|s| self.catalog.get(s)).map(|s| s.name.clone()).unwrap_or_else(|| "(local)".into());
                    rows.push(vec![name, src, n.to_string()]);
                }
                text_rows(&["name", "source", "rows"], rows)
            }
            ShowKind::Views | ShowKind::MaterializedViews => {
                let mut rows = Vec::new();
                for o in self.catalog.objects.values() {
                    match &o.kind {
                        ObjectKind::MaterializedView(_) => match self.rt.views.get(&o.id) {
                            Some(v) => {
                                let n = self.env.tree_count(self.graph.arr(v.arr).tree);
                                let errs = v.errors.values().filter(|w| **w > 0).count();
                                rows.push(vec![o.name.clone(), "materialized".into(), n.to_string(), errs.to_string(), "ok".into()]);
                            }
                            None => {
                                let reason = self.rt.broken.get(&o.id).cloned().unwrap_or_default();
                                rows.push(vec![o.name.clone(), "materialized".into(), String::new(), String::new(), format!("broken: {reason}")]);
                            }
                        },
                        ObjectKind::View(_) if matches!(k, ShowKind::Views) => {
                            rows.push(vec![o.name.clone(), "virtual".into(), String::new(), String::new(), String::new()]);
                        }
                        _ => {}
                    }
                }
                text_rows(&["name", "kind", "rows", "errors", "status"], rows)
            }
            ShowKind::Indexes => {
                let mut rows = Vec::new();
                for o in self.catalog.objects.values() {
                    if let ObjectKind::Index(i) = &o.kind {
                        let on = self.catalog.get(i.on).map(|x| x.name.clone()).unwrap_or_default();
                        rows.push(vec![o.name.clone(), on, format!("{:?}", i.columns)]);
                    }
                }
                text_rows(&["name", "on", "columns"], rows)
            }
            ShowKind::Sinks => {
                let mut rows = Vec::new();
                for o in self.catalog.objects.values() {
                    if let ObjectKind::Sink(s) = &o.kind {
                        let from = self.catalog.get(s.from).map(|x| x.name.clone()).unwrap_or_default();
                        rows.push(vec![o.name.clone(), from, s.table.clone(), self.sink_status(o.id)]);
                    }
                }
                text_rows(&["name", "from", "table", "status"], rows)
            }
            ShowKind::Arrangements => {
                let mut rows = Vec::new();
                let arrs: Vec<(usize, usize, TreeId, usize, String)> = self
                    .graph
                    .live_arrangements()
                    .map(|a| (a.id, a.node, a.tree, a.refs, a.layout.key.iter().map(|k| k.to_string()).collect::<Vec<_>>().join(", ")))
                    .collect();
                for (id, node, tree, refs, key) in arrs {
                    let op = self.graph.node(node).kind.name().to_string();
                    rows.push(vec![id.to_string(), node.to_string(), op, key, self.env.tree_count(tree).to_string(), refs.to_string()]);
                }
                text_rows(&["arrangement", "node", "operator", "key", "rows", "refs"], rows)
            }
            ShowKind::Frontiers => {
                let st = self.env.stats();
                let mut rows = vec![
                    vec!["durable_gsn".into(), self.shared.durable().to_string()],
                    vec!["processed_gsn".into(), self.processed.to_string()],
                    vec!["checkpoint_gsn".into(), st.checkpoint_gsn.to_string()],
                    vec!["retention_frontier".into(), self.env.frontier().to_string()],
                    vec!["cache_bytes".into(), st.mem_bytes.to_string()],
                    vec!["segments".into(), st.segments.len().to_string()],
                ];
                for (src, lsn) in self.shared.source_lsn.lock().iter() {
                    // (a dropped source's thread may report once more)
                    let Some(o) = self.catalog.get(*src) else { continue };
                    rows.push(vec![format!("source {} lsn", o.name), format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)]);
                }
                text_rows(&["frontier", "value"], rows)
            }
            ShowKind::Gsn => ExecResult::Query {
                columns: vec![col("gsn", SqlType::Int8)],
                rows: vec![vec![Datum::Int8(self.processed as i64)]],
            },
            ShowKind::Columns(name) => {
                let Some(o) = self.catalog.by_name(&name) else {
                    sql_bail!(codes::UNDEFINED_TABLE, "relation \"{name}\" does not exist");
                };
                let cols = match &o.kind {
                    ObjectKind::Table(t) => t.columns.clone(),
                    ObjectKind::MaterializedView(v) | ObjectKind::View(v) => v.columns.clone(),
                    _ => vec![],
                };
                let pk = match &o.kind {
                    ObjectKind::Table(t) => t.pk.clone(),
                    _ => vec![],
                };
                text_rows(
                    &["name", "type", "nullable", "key"],
                    cols.iter()
                        .enumerate()
                        .map(|(i, c)| {
                            vec![
                                c.name.clone(),
                                crate::types::format_type(&c.ty.ty, c.ty.typmod),
                                c.ty.nullable.to_string(),
                                pk.contains(&i).to_string(),
                            ]
                        })
                        .collect(),
                )
            }
            ShowKind::CreateView(name) => {
                let Some(o) = self.catalog.by_name(&name) else {
                    sql_bail!(codes::UNDEFINED_TABLE, "relation \"{name}\" does not exist");
                };
                text_rows(&["name", "create_sql"], vec![vec![o.name.clone(), o.create_sql.clone()]])
            }
            ShowKind::Variable(v) => {
                let val = match v.as_str() {
                    "server_version" => "17.0".to_string(),
                    "server_encoding" | "client_encoding" => "UTF8".to_string(),
                    "timezone" | "time zone" => "UTC".to_string(),
                    "datestyle" => "ISO, MDY".to_string(),
                    "intervalstyle" => "postgres".to_string(),
                    "transaction isolation level" | "transaction_isolation" => "serializable".to_string(),
                    "standard_conforming_strings" => "on".to_string(),
                    "gsn" => self.processed.to_string(),
                    other => sql_bail!(codes::UNDEFINED_OBJECT, "unrecognized configuration parameter \"{other}\""),
                };
                text_rows(&[v.as_str()], vec![vec![val]])
            }
            ShowKind::All => text_rows(
                &["name", "setting"],
                vec![
                    vec!["server_version".into(), "17.0".into()],
                    vec!["timezone".into(), "UTC".into()],
                    vec!["datestyle".into(), "ISO, MDY".into()],
                ],
            ),
        })
    }

    fn node_rows(&mut self, node: NodeId) -> i64 {
        match self.graph.node(node).arrangements.first() {
            Some(a) => self.env.tree_count(self.graph.arr(*a).tree),
            None => 0,
        }
    }

    fn explain(&mut self, kind: ExplainKind, target: ExplainTarget, params: Vec<ParamSlot>) -> Result<ExecResult> {
        match (kind, target) {
            (ExplainKind::Plan, ExplainTarget::Query(q)) => {
                let bq = crate::sql::plan_query(&self.catalog, &q, params)?;
                let text = bq.rel.explain();
                Ok(text_rows(&["QUERY PLAN"], text.lines().map(|l| vec![l.to_string()]).collect()))
            }
            (ExplainKind::Plan, ExplainTarget::Name(name)) => {
                let Some(o) = self.catalog.by_name(&name).cloned() else {
                    sql_bail!(codes::UNDEFINED_TABLE, "relation \"{name}\" does not exist");
                };
                let (ObjectKind::MaterializedView(v) | ObjectKind::View(v)) = &o.kind else {
                    sql_bail!(codes::WRONG_OBJECT_TYPE, "\"{name}\" is not a view");
                };
                let q = crate::sql::parser::parse_single_query(&v.sql)?;
                // a stored definition (as recovery plans it)
                let bq = crate::sql::plan_stored_query(&self.catalog, &q)?;
                Ok(text_rows(&["QUERY PLAN"], bq.rel.explain().lines().map(|l| vec![l.to_string()]).collect()))
            }
            (k, ExplainTarget::Name(name)) => {
                let Some(o) = self.catalog.by_name(&name).cloned() else {
                    sql_bail!(codes::UNDEFINED_TABLE, "relation \"{name}\" does not exist");
                };
                if let Some(e) = self.broken_error(o.id) {
                    return Err(e);
                }
                let Some(v) = self.rt.views.get(&o.id) else {
                    sql_bail!(codes::WRONG_OBJECT_TYPE, "\"{name}\" is not a materialized view");
                };
                let root = v.node;
                if k == ExplainKind::Dataflow {
                    Ok(text_rows(&["DATAFLOW"], self.describe_dataflow(root).into_iter().map(|l| vec![l]).collect()))
                } else {
                    Ok(text_rows(&["source", "delta rows/tick", "expected work per delta", "path"], self.maintenance_report(root)))
                }
            }
            (_, ExplainTarget::Query(_)) => {
                sql_bail!(codes::FEATURE_NOT_SUPPORTED, "EXPLAIN DATAFLOW/MAINTENANCE requires a materialized view name")
            }
        }
    }

    fn describe_dataflow(&mut self, root: NodeId) -> Vec<String> {
        let mut lines = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        self.describe_node(root, 0, &mut seen, &mut lines);
        lines
    }

    fn describe_node(&mut self, id: NodeId, depth: usize, seen: &mut std::collections::BTreeSet<NodeId>, out: &mut Vec<String>) {
        let pad = "  ".repeat(depth);
        let node = self.graph.node(id);
        let shared = if node.refs > 1 { format!(" shared x{}", node.refs) } else { String::new() };
        let detail = match &node.kind {
            NodeKind::Table { object, .. } => self.catalog.get(*object).map(|o| o.name.clone()).unwrap_or_default(),
            NodeKind::Join(p) => format!("{} inputs, {} delta paths", p.inputs.len(), p.paths.len()),
            NodeKind::Reduce(p) => format!("group by {:?}, {} aggregates", p.group_key, p.aggs.len()),
            NodeKind::TopK(p) => format!("limit {:?} offset {}", p.limit, p.offset),
            _ => String::new(),
        };
        let arrs: Vec<String> = node
            .arrangements
            .iter()
            .map(|a| {
                let arr = self.graph.arr(*a);
                format!("arr#{}[{}]", a, arr.layout.key.iter().map(|k| k.to_string()).collect::<Vec<_>>().join(","))
            })
            .collect();
        let inputs = node.inputs.clone();
        let kind = node.kind.name();
        let again = !seen.insert(id);
        out.push(format!(
            "{pad}#{id} {kind} {detail}{shared}{}{}",
            if arrs.is_empty() { String::new() } else { format!(" {}", arrs.join(" ")) },
            if again { " (see above)" } else { "" }
        ));
        if again {
            return;
        }
        for i in inputs {
            self.describe_node(i, depth + 1, seen, out);
        }
    }

    /// Expected work per source delta: follows each base table's deltas to
    /// the view, multiplying by measured lookup fanouts on delta-join paths.
    fn maintenance_report(&mut self, root: NodeId) -> Vec<Vec<String>> {
        // collect subgraph
        let mut stack = vec![root];
        let mut nodes = std::collections::BTreeSet::new();
        while let Some(n) = stack.pop() {
            if nodes.insert(n) {
                stack.extend(self.graph.node(n).inputs.iter().copied());
            }
        }
        let mut rows = Vec::new();
        for &n in &nodes {
            let NodeKind::Table { object, .. } = self.graph.node(n).kind else { continue };
            let name = self.catalog.get(object).map(|o| o.name.clone()).unwrap_or_default();
            let st = self.graph.stats.get(&n).copied().unwrap_or_default();
            let rate = if st.ticks > 0 { st.rows as f64 / st.ticks as f64 } else { 0.0 };
            // walk consumers toward the root, estimating per-row work
            let mut work = 1.0f64;
            let mut path = vec![name.clone()];
            let mut cur = n;
            let mut guard = 0;
            while cur != root && guard < 64 {
                guard += 1;
                let consumers: Vec<NodeId> = nodes.iter().copied().filter(|c| self.graph.node(*c).inputs.contains(&cur)).collect();
                let Some(&next) = consumers.first() else { break };
                let kind = self.graph.node(next).kind.clone();
                match &kind {
                    NodeKind::Join(p) => {
                        let src = p.inputs.iter().position(|i| i.node == cur).unwrap_or(0);
                        let mut fan = 1.0;
                        let mut lookups = 0.0;
                        if let Some(dp) = p.paths.iter().find(|d| d.source == src) {
                            for s in &dp.steps {
                                lookups += fan;
                                fan *= self.estimate_fanout(s.arr);
                            }
                        }
                        work *= 1.0 + lookups;
                        path.push(format!("join({:.1} lookups)", lookups));
                    }
                    NodeKind::Reduce(_) => {
                        work += 2.0;
                        path.push("reduce(range summary)".into());
                    }
                    NodeKind::TopK(p) => {
                        work += p.limit.unwrap_or(100) as f64;
                        path.push("topk".into());
                    }
                    NodeKind::Window(_) => {
                        work *= 10.0;
                        path.push("window(partition recompute)".into());
                    }
                    other => path.push(other.name().to_lowercase()),
                }
                cur = next;
            }
            rows.push(vec![name, format!("{rate:.2}"), format!("{work:.1}"), path.join(" -> ")]);
        }
        rows
    }

    /// Average live entries per key prefix of an arrangement, sampled from
    /// runs of entries starting at evenly spaced ranks of its weight (found
    /// through the subtree counts, which skip entries retracted but not yet
    /// compacted away without visiting them; within a run they are skipped
    /// too).
    fn estimate_fanout(&mut self, arr: ArrId) -> f64 {
        use std::ops::Bound;
        const RUNS: i64 = 16;
        const RUN: usize = 128;
        let a = self.graph.arr(arr);
        let layout = a.layout.clone();
        let tree = a.tree;
        let total = self.env.tree_count(tree);
        if total <= 0 {
            return 1.0;
        }
        // a small tree is one run from its start
        let runs = if total <= RUNS * RUN as i64 { 1 } else { RUNS };
        let (mut entries, mut keys) = (0usize, 0usize);
        for r in 0..runs {
            let rank = if runs == 1 { 0 } else { (total as i128 * (2 * r as i128 + 1) / (2 * runs as i128)) as i64 };
            let Ok(Some((start, _, _))) = self.env.nth(tree, Bound::Unbounded, Bound::Unbounded, rank) else { continue };
            let (mut n, mut visited) = (0usize, 0usize);
            let mut last: Option<Vec<u8>> = None;
            let _ = self.env.for_each(tree, Bound::Included(&start), Bound::Unbounded, &mut |k, h| {
                visited += 1;
                if h.total() > 0
                    && let Ok(kb) = layout.key_bytes(k)
                {
                    // entries with equal keys are adjacent
                    if last.as_deref() != Some(kb) {
                        keys += 1;
                        last = Some(kb.to_vec());
                    }
                    n += 1;
                }
                Ok(n < RUN && visited < 8 * RUN)
            });
            entries += n;
        }
        if keys == 0 { 1.0 } else { (entries as f64 / keys as f64).max(1.0) }
    }
}
