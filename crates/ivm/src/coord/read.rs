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
                        && let Ok(bq) = crate::sql::plan_query(&self.catalog, &q, vec![])
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
                let frontiers = self.shared.source_lsn.lock().clone();
                if frontiers.is_empty() || frontiers.values().any(|f| *f < lsn) {
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

    /// Re-run parked reads whose GSN (or LSN) condition is now satisfied.
    pub(crate) fn wake_parked(&mut self, _handle: &EngineHandle) {
        if self.parked.is_empty() {
            return;
        }
        let frontiers = self.shared.source_lsn.lock().clone();
        // snapshot progress or catalog changes since reads were parked
        let snapshots_changed = self.snap_epoch != self.snap_epoch_seen;
        self.snap_epoch_seen = self.snap_epoch;
        let mut still = Vec::new();
        for p in std::mem::take(&mut self.parked) {
            let ready = match p.wait {
                Wait::Lsn => match &p.req {
                    Request::Execute { stmt, .. } => match &**stmt {
                        Statement::Select { as_of: AsOf::AtLeastLsn(l), .. } => match parse_lsn(l) {
                            Ok(lsn) => !frontiers.is_empty() && frontiers.values().all(|f| *f >= lsn),
                            Err(_) => true,
                        },
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

    fn subscribe(&mut self, target: SubscribeTarget, snapshot: bool, progress: bool, params: Vec<ParamSlot>) -> Result<ExecResult> {
        let (node, pinned, columns): (NodeId, Option<NodeId>, Vec<ResultColumn>) = match target {
            SubscribeTarget::Name(name) => {
                let Some(obj) = self.catalog.by_name(&name).cloned() else {
                    sql_bail!(codes::UNDEFINED_TABLE, "relation \"{name}\" does not exist");
                };
                match &obj.kind {
                    ObjectKind::MaterializedView(v) => {
                        let rt = &self.rt.views[&obj.id];
                        (rt.node, None, v.columns.iter().map(|c| ResultColumn { name: c.name.clone(), ty: c.ty.ty.clone(), typmod: c.ty.typmod }).collect())
                    }
                    ObjectKind::Table(t) => (
                        self.rt.tables[&obj.id],
                        None,
                        t.columns.iter().map(|c| ResultColumn { name: c.name.clone(), ty: c.ty.ty.clone(), typmod: c.ty.typmod }).collect(),
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
                let names: Vec<String> = bq.names.iter().take(bq.visible).cloned().collect();
                let types: Vec<ColumnType> = bq.column_types();
                let rel = bq.into_rel();
                let node_mark = self.graph.node_count();
                let arr_mark = self.graph.arr_count();
                let rt = &self.rt;
                let objects = |oid: u64| -> Option<NodeId> { rt.tables.get(&oid).copied().or_else(|| rt.views.get(&oid).map(|v| v.node)) };
                let node = {
                    let mut lw = Lowerer { g: &mut self.graph, env: &mut self.env, objects: &objects };
                    let l = lw.lower(&rel)?;
                    lw.materialize(l)
                };
                self.graph.retain_node(node);
                self.graph.gc_since(&mut self.env, node_mark, arr_mark)?;
                self.graph.hydrate(&mut self.env, self.processed)?;
                let cols = names.into_iter().zip(types).map(|(n, t)| ResultColumn { name: n, ty: t.ty, typmod: t.typmod }).collect();
                (node, Some(node), cols)
            }
        };
        let (tx, rx) = unbounded();
        let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
        if snapshot {
            let snap = self.graph.snapshot(&mut self.env, node)?;
            if !snap.data.is_empty() {
                let _ = tx.send(SubEvent::Data { gsn: self.processed, rows: snap.data });
            }
        }
        let _ = tx.send(SubEvent::Progress { gsn: self.processed });
        self.subs.push(Subscription { node, tx, cancel: cancel.clone(), pinned, progress });
        Ok(ExecResult::Subscribe { columns, rx, cancel, progress })
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
                        ObjectKind::MaterializedView(_) => {
                            let v = &self.rt.views[&o.id];
                            let n = self.env.tree_count(self.graph.arr(v.arr).tree);
                            let errs = v.errors.values().filter(|w| **w > 0).count();
                            rows.push(vec![o.name.clone(), "materialized".into(), n.to_string(), errs.to_string()]);
                        }
                        ObjectKind::View(_) if matches!(k, ShowKind::Views) => {
                            rows.push(vec![o.name.clone(), "virtual".into(), String::new(), String::new()]);
                        }
                        _ => {}
                    }
                }
                text_rows(&["name", "kind", "rows", "errors"], rows)
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
                        let status = self.sink_handles.get(&o.id).map(|h| h.status()).unwrap_or_else(|| "stopped".into());
                        rows.push(vec![o.name.clone(), from, s.table.clone(), status]);
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
                    let name = self.catalog.get(*src).map(|o| o.name.clone()).unwrap_or_default();
                    rows.push(vec![format!("source {name} lsn"), format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)]);
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
                self.explain(ExplainKind::Plan, ExplainTarget::Query(Box::new(q)), params)
            }
            (k, ExplainTarget::Name(name)) => {
                let Some(o) = self.catalog.by_name(&name).cloned() else {
                    sql_bail!(codes::UNDEFINED_TABLE, "relation \"{name}\" does not exist");
                };
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

    /// Average entries per key prefix of an arrangement (sampled).
    fn estimate_fanout(&mut self, arr: ArrId) -> f64 {
        let a = self.graph.arr(arr);
        let layout = a.layout.clone();
        let tree = a.tree;
        let total = self.env.tree_count(tree).max(0) as f64;
        if total == 0.0 {
            return 1.0;
        }
        let mut c = self.env.cursor(tree, std::ops::Bound::Unbounded, std::ops::Bound::Unbounded);
        let mut keys = std::collections::BTreeSet::new();
        let mut n = 0;
        while let Ok(Some((k, _))) = self.env.cursor_next(&mut c) {
            if let Ok(kb) = layout.key_bytes(&k) {
                keys.insert(kb.to_vec());
            }
            n += 1;
            if n >= 2000 {
                break;
            }
        }
        if keys.is_empty() { 1.0 } else { (n as f64 / keys.len() as f64).max(1.0) }
    }
}
