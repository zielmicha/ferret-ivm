//! Catalog changes. DDL is validated, logged with its GSN, and applied when
//! the record comes back durable, so replay re-applies it identically.

use super::*;
use crate::catalog::{CatalogObject, IndexDef, SinkDef, SourceDef, TableDef, ViewDef};
use crate::dataflow::layout::Layout;
use crate::dataflow::lower::Lowerer;
use crate::dataflow::NodeKind;
use crate::sql::binder::{ident, object_name};
use crate::sql::binder_expr::sql_type_of;
use crate::sql::parser::{parse_statement, Statement};
use crate::sql_bail;
use crate::types::encoding::SortSpec;
use crate::types::{Column, ColumnType};
use sqlparser::ast;

/// Upstream table discovered by CREATE SOURCE (logged in the DDL record).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoveredTable {
    pub schema: String,
    pub name: String,
    pub columns: Vec<Column>,
    pub pk: Vec<usize>,
    pub replica_identity_full: bool,
}

/// Canonical SQL text of a DDL statement (what goes into the log).
pub fn statement_sql(stmt: &Statement) -> String {
    match stmt {
        Statement::Sql(s) => s.to_string(),
        Statement::CreateSource { name, conninfo, publication, slot, tables } => {
            let mut s = format!(
                "CREATE SOURCE {} FROM POSTGRES CONNECTION '{}' PUBLICATION '{}'",
                quote(name),
                conninfo.replace('\'', "''"),
                publication.replace('\'', "''")
            );
            if let Some(sl) = slot {
                s.push_str(&format!(" SLOT '{}'", sl.replace('\'', "''")));
            }
            if let Some(t) = tables {
                s.push_str(&format!(" FOR TABLES ({})", t.iter().map(|x| quote(x)).collect::<Vec<_>>().join(", ")));
            }
            s
        }
        Statement::DropSource { name, if_exists } => {
            format!("DROP SOURCE {}{}", if *if_exists { "IF EXISTS " } else { "" }, quote(name))
        }
        Statement::AlterSourceRefresh { name } => format!("ALTER SOURCE {} REFRESH", quote(name)),
        Statement::CreateSink { name, from, conninfo, table, key } => {
            let mut s = format!(
                "CREATE SINK {} FROM {} INTO POSTGRES CONNECTION '{}' TABLE '{}'",
                quote(name),
                quote(from),
                conninfo.replace('\'', "''"),
                table.replace('\'', "''")
            );
            if let Some(k) = key {
                s.push_str(&format!(" KEY ({})", k.iter().map(|x| quote(x)).collect::<Vec<_>>().join(", ")));
            }
            s
        }
        Statement::DropSink { name, if_exists } => {
            format!("DROP SINK {}{}", if *if_exists { "IF EXISTS " } else { "" }, quote(name))
        }
        other => format!("{other:?}"),
    }
}

/// A valid replication slot name unique to this source object, so that a
/// re-created source never shares a slot that is still being dropped.
fn default_slot_name(source: &str, id: ObjectId) -> String {
    let suffix = format!("_{id}");
    let mut base: String = source
        .chars()
        .map(|c| c.to_ascii_lowercase())
        .map(|c| if c.is_ascii_lowercase() || c.is_ascii_digit() { c } else { '_' })
        .collect();
    base.truncate(63 - 4 - suffix.len());
    format!("ivm_{base}{suffix}")
}

fn parse_discovered(extra: &str) -> Result<Vec<DiscoveredTable>> {
    serde_json::from_str(extra).map_err(|e| SqlError::internal(format!("bad source discovery record: {e}")))
}

fn quote(s: &str) -> String {
    if s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') && !s.is_empty() {
        s.to_string()
    } else {
        format!("\"{}\"", s.replace('"', "\"\""))
    }
}

impl Engine {
    pub(crate) fn is_ddl(stmt: &Statement) -> bool {
        match stmt {
            Statement::Sql(s) => matches!(
                **s,
                ast::Statement::CreateTable(_)
                    | ast::Statement::CreateView(_)
                    | ast::Statement::CreateIndex(_)
                    | ast::Statement::Drop { .. }
            ),
            Statement::CreateSource { .. }
            | Statement::DropSource { .. }
            | Statement::AlterSourceRefresh { .. }
            | Statement::CreateSink { .. }
            | Statement::DropSink { .. } => true,
            _ => false,
        }
    }

    /// Validate a DDL statement against the current catalog and log it.
    pub(crate) fn submit_ddl(&mut self, stmt: &Statement, extra: String, reply: Sender<Result<ExecResult>>) -> Result<Option<ExecResult>> {
        // dry run: plan views to report errors before logging
        if let Statement::Sql(s) = stmt {
            match &**s {
                ast::Statement::CreateView(cv) => {
                    let (_, name) = object_name(&cv.name)?;
                    if self.catalog.by_name(&name).is_some() && !cv.or_replace {
                        if cv.if_not_exists {
                            return Ok(Some(ExecResult::Command { tag: "CREATE VIEW".into() }));
                        }
                        sql_bail!(codes::DUPLICATE_TABLE, "relation \"{name}\" already exists");
                    }
                    let bq = crate::sql::plan_query(&self.catalog, &cv.query, vec![])?;
                    if cv.materialized {
                        check_deterministic(&bq.rel)?;
                    }
                }
                ast::Statement::CreateTable(ct) => {
                    let (_, name) = object_name(&ct.name)?;
                    if self.catalog.by_name(&name).is_some() {
                        if ct.if_not_exists {
                            return Ok(Some(ExecResult::Command { tag: "CREATE TABLE".into() }));
                        }
                        sql_bail!(codes::DUPLICATE_TABLE, "relation \"{name}\" already exists");
                    }
                    table_def_from_ast(ct)?;
                }
                _ => {}
            }
        }
        match stmt {
            Statement::CreateSource { name, .. } => {
                if self.catalog.by_name(name).is_some() {
                    sql_bail!(codes::DUPLICATE_OBJECT, "source \"{name}\" already exists");
                }
                self.check_source_tables(name, &parse_discovered(&extra)?)?;
            }
            Statement::AlterSourceRefresh { name } => {
                let Some(src) = self.catalog.by_name(name) else {
                    sql_bail!(codes::UNDEFINED_OBJECT, "source \"{name}\" does not exist");
                };
                self.check_source_tables(name, &self.new_source_tables(src.id, parse_discovered(&extra)?))?;
            }
            _ => {}
        }
        let req = self.alloc_req();
        let sql = statement_sql(stmt);
        self.submit(LogRecord::Ddl { req, sql, extra }, req, reply);
        Ok(None)
    }

    /// Apply a logged DDL statement.
    pub(crate) fn apply_ddl(&mut self, gsn: Gsn, sql: &str, extra: &str) -> Result<ExecResult> {
        let stmt = parse_statement(sql)?;
        match stmt {
            Statement::Sql(s) => match *s {
                ast::Statement::CreateTable(ct) => {
                    let (_, name) = object_name(&ct.name)?;
                    if self.catalog.by_name(&name).is_some() {
                        if ct.if_not_exists {
                            return Ok(ExecResult::Command { tag: "CREATE TABLE".into() });
                        }
                        sql_bail!(codes::DUPLICATE_TABLE, "relation \"{name}\" already exists");
                    }
                    let def = table_def_from_ast(&ct)?;
                    let id = self.catalog.alloc_id();
                    self.create_table_rt(id, &def, gsn)?;
                    self.catalog.insert(CatalogObject { id, name, kind: ObjectKind::Table(def), created_gsn: gsn, create_sql: sql.to_string() });
                    Ok(ExecResult::Command { tag: "CREATE TABLE".into() })
                }
                ast::Statement::CreateView(cv) => self.apply_create_view(gsn, sql, &cv),
                ast::Statement::CreateIndex(ci) => self.apply_create_index(gsn, sql, &ci),
                ast::Statement::Drop { object_type, if_exists, names, cascade, .. } => {
                    for n in &names {
                        let (_, name) = object_name(n)?;
                        let Some(obj) = self.catalog.by_name(&name).cloned() else {
                            if if_exists {
                                continue;
                            }
                            sql_bail!(codes::UNDEFINED_TABLE, "{} \"{}\" does not exist", object_type_name(object_type), name);
                        };
                        let ok = matches!(
                            (object_type, &obj.kind),
                            (ast::ObjectType::Table, ObjectKind::Table(_))
                                | (ast::ObjectType::View, ObjectKind::View(_))
                                | (ast::ObjectType::View, ObjectKind::MaterializedView(_))
                                | (ast::ObjectType::MaterializedView, ObjectKind::MaterializedView(_))
                                | (ast::ObjectType::Index, ObjectKind::Index(_))
                        );
                        if !ok {
                            sql_bail!(codes::WRONG_OBJECT_TYPE, "\"{}\" is not a {}", name, object_type_name(object_type));
                        }
                        if let ObjectKind::Table(t) = &obj.kind
                            && t.source.is_some()
                        {
                            sql_bail!(codes::WRONG_OBJECT_TYPE, "cannot drop source table \"{name}\"; drop the source instead");
                        }
                        self.drop_with_dependents(obj.id, cascade)?;
                    }
                    Ok(ExecResult::Command {
                        tag: match object_type {
                            ast::ObjectType::Table => "DROP TABLE",
                            ast::ObjectType::View => "DROP VIEW",
                            ast::ObjectType::MaterializedView => "DROP MATERIALIZED VIEW",
                            ast::ObjectType::Index => "DROP INDEX",
                            _ => "DROP",
                        }
                        .into(),
                    })
                }
                other => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unsupported DDL: {other}"),
            },
            Statement::CreateSource { name, conninfo, publication, slot, tables } => {
                if self.catalog.by_name(&name).is_some() {
                    sql_bail!(codes::DUPLICATE_OBJECT, "source \"{name}\" already exists");
                }
                // validate everything before touching the catalog
                let discovered = parse_discovered(extra)?;
                self.check_source_tables(&name, &discovered)?;
                let id = self.catalog.alloc_id();
                let slot = slot.unwrap_or_else(|| default_slot_name(&name, id));
                let def = SourceDef { conninfo, publication, slot, tables };
                self.catalog.insert(CatalogObject {
                    id,
                    name: name.clone(),
                    kind: ObjectKind::Source(def.clone()),
                    created_gsn: gsn,
                    create_sql: sql.to_string(),
                });
                self.add_source_tables(gsn, id, &discovered)?;
                if !self.replaying
                    && let Some(h) = self.handle.clone()
                {
                    let tables = self.source_tables(id);
                    let state = self.source_states.get(&id).cloned().unwrap_or_default();
                    let resume = self.source_lsns.get(&id).copied().unwrap_or(0);
                    // the source is durable now; a failure to start it is not a DDL error
                    match crate::source::start(id, name.clone(), def, tables, state, resume, h) {
                        Ok(sh) => {
                            self.source_handles.insert(id, sh);
                        }
                        Err(e) => log::error!("source {name}: failed to start: {e}"),
                    }
                }
                Ok(ExecResult::Command { tag: "CREATE SOURCE".into() })
            }
            Statement::AlterSourceRefresh { name } => {
                let Some(src) = self.catalog.by_name(&name).cloned() else {
                    sql_bail!(codes::UNDEFINED_OBJECT, "source \"{name}\" does not exist");
                };
                let new = self.new_source_tables(src.id, parse_discovered(extra)?);
                self.check_source_tables(&name, &new)?;
                let n = new.len();
                self.add_source_tables(gsn, src.id, &new)?;
                if !self.replaying
                    && let Some(h) = self.source_handles.get(&src.id)
                {
                    h.add_tables(self.source_tables(src.id));
                }
                Ok(ExecResult::Command { tag: format!("ALTER SOURCE {n}") })
            }
            Statement::DropSource { name, if_exists } => {
                let Some(src) = self.catalog.by_name(&name).cloned() else {
                    if if_exists {
                        return Ok(ExecResult::Command { tag: "DROP SOURCE".into() });
                    }
                    sql_bail!(codes::UNDEFINED_OBJECT, "source \"{name}\" does not exist");
                };
                self.drop_with_dependents(src.id, true)?;
                Ok(ExecResult::Command { tag: "DROP SOURCE".into() })
            }
            Statement::CreateSink { name, from, conninfo, table, key } => {
                if self.catalog.by_name(&name).is_some() {
                    sql_bail!(codes::DUPLICATE_OBJECT, "sink \"{name}\" already exists");
                }
                let Some(v) = self.catalog.by_name(&from).cloned() else {
                    sql_bail!(codes::UNDEFINED_TABLE, "relation \"{from}\" does not exist");
                };
                if !matches!(v.kind, ObjectKind::MaterializedView(_) | ObjectKind::Table(_)) {
                    sql_bail!(codes::WRONG_OBJECT_TYPE, "sink source \"{from}\" must be a table or materialized view");
                }
                let id = self.catalog.alloc_id();
                let def = SinkDef { from: v.id, conninfo, table, key };
                self.catalog.insert(CatalogObject {
                    id,
                    name: name.clone(),
                    kind: ObjectKind::Sink(def.clone()),
                    created_gsn: gsn,
                    create_sql: sql.to_string(),
                });
                if !self.replaying
                    && let Some(h) = self.handle.clone()
                {
                    self.start_sink(id, &name, &def, &h)?;
                }
                Ok(ExecResult::Command { tag: "CREATE SINK".into() })
            }
            Statement::DropSink { name, if_exists } => {
                let Some(s) = self.catalog.by_name(&name).cloned() else {
                    if if_exists {
                        return Ok(ExecResult::Command { tag: "DROP SINK".into() });
                    }
                    sql_bail!(codes::UNDEFINED_OBJECT, "sink \"{name}\" does not exist");
                };
                self.drop_object(s.id)?;
                Ok(ExecResult::Command { tag: "DROP SINK".into() })
            }
            other => sql_bail!(codes::INTERNAL_ERROR, "not a DDL statement: {other:?}"),
        }
    }

    /// Can these upstream tables be replicated under their names? Checked
    /// before logging and again (against the catalog of the time) on apply.
    fn check_source_tables(&self, source: &str, tables: &[DiscoveredTable]) -> Result<()> {
        let mut names = std::collections::HashSet::new();
        for t in tables {
            if t.name == source {
                return Err(SqlError::new(
                    codes::DUPLICATE_OBJECT,
                    format!("upstream table \"{}.{}\" has the same name as source \"{source}\"", t.schema, t.name),
                )
                .with_hint("Sources, tables, views and indexes share one namespace; give the source a different name."));
            }
            if t.pk.is_empty() && !t.replica_identity_full {
                sql_bail!(
                    codes::OBJECT_NOT_IN_PREREQUISITE_STATE,
                    "table \"{}.{}\" has no primary key and REPLICA IDENTITY is not FULL",
                    t.schema,
                    t.name
                );
            }
            if self.catalog.by_name(&t.name).is_some() || !names.insert(t.name.as_str()) {
                sql_bail!(codes::DUPLICATE_TABLE, "relation \"{}\" already exists", t.name);
            }
        }
        Ok(())
    }

    /// Discovered tables a source does not replicate yet.
    fn new_source_tables(&self, source: ObjectId, discovered: Vec<DiscoveredTable>) -> Vec<DiscoveredTable> {
        let existing: Vec<(String, String)> = self.source_tables(source).into_iter().map(|t| t.upstream).collect();
        discovered.into_iter().filter(|t| !existing.contains(&(t.schema.clone(), t.name.clone()))).collect()
    }

    /// Add validated (see `check_source_tables`) upstream tables to a source.
    fn add_source_tables(&mut self, gsn: Gsn, source: ObjectId, tables: &[DiscoveredTable]) -> Result<()> {
        for t in tables {
            let id = self.catalog.alloc_id();
            let def = TableDef {
                columns: t.columns.clone(),
                pk: t.pk.clone(),
                source: Some(source),
                upstream: Some((t.schema.clone(), t.name.clone())),
                version: 0,
            };
            self.create_table_rt(id, &def, gsn)?;
            self.catalog.insert(CatalogObject {
                id,
                name: t.name.clone(),
                kind: ObjectKind::Table(def),
                created_gsn: gsn,
                create_sql: format!("-- table {}.{} replicated by source {}", t.schema, t.name, source),
            });
        }
        Ok(())
    }

    /// Create the Table node and primary index of a new (empty) base table
    /// at `gsn`.
    pub(crate) fn create_table_rt(&mut self, id: ObjectId, def: &TableDef, gsn: Gsn) -> Result<()> {
        self.table_rt(id, def, false)?;
        let node = self.rt.tables[&id];
        let NodeKind::Table { primary, .. } = self.graph.node(node).kind else { unreachable!() };
        self.graph.arrs[primary].as_mut().unwrap().since = gsn;
        Ok(())
    }

    /// Create the Table node and primary index of a base table: a new table
    /// is empty, hence up to date; a recovered one must find its index in the
    /// checkpoint, as its rows exist nowhere else.
    pub(crate) fn table_rt(&mut self, id: ObjectId, def: &TableDef, recovering: bool) -> Result<()> {
        let schema: Vec<ColumnType> = def.columns.iter().map(|c| c.ty.clone()).collect();
        let key: Vec<usize> = if def.pk.is_empty() { (0..schema.len()).collect() } else { def.pk.clone() };
        let node = self.graph.add_node_with_output_arr(
            &mut self.env,
            NodeKind::Table { object: id, version: def.version, pk: def.pk.clone(), primary: usize::MAX },
            vec![],
            schema,
            key.iter().map(|c| crate::expr::Expr::Column(*c)).collect(),
            vec![SortSpec::ASC; key.len()],
            |k, a| {
                if let NodeKind::Table { primary, .. } = k {
                    *primary = a;
                }
            },
        );
        self.graph.retain_node(node);
        let NodeKind::Table { primary, .. } = self.graph.node(node).kind else { unreachable!() };
        if recovering {
            if !self.graph.arr(primary).hydrated {
                return Err(SqlError::corrupt(format!(
                    "the checkpoint has no index for table {id} (version {}); refusing to start without its rows",
                    def.version
                )));
            }
        } else {
            self.graph.arrs[primary].as_mut().unwrap().hydrated = true;
        }
        self.graph.node_mut(node).hydrated = true;
        self.rt.tables.insert(id, node);
        Ok(())
    }

    /// Plan and lower a materialized view; pins its root and read arrangement.
    pub(crate) fn create_view_rt(&mut self, id: ObjectId, sql: &str) -> Result<()> {
        let q = crate::sql::parser::parse_single_query(sql)?;
        // new definitions were planned with plan_query before
        let bq = crate::sql::plan_stored_query(&self.catalog, &q)?;
        let rel = bq.into_rel();
        let node_mark = self.graph.node_count();
        let arr_mark = self.graph.arr_count();
        let rt = &self.rt;
        let objects = |oid: u64| -> Option<NodeId> { rt.tables.get(&oid).copied().or_else(|| rt.views.get(&oid).map(|v| v.node)) };
        let lowered = {
            let mut lw = Lowerer { g: &mut self.graph, env: &mut self.env, objects: &objects };
            let l = lw.lower(&rel)?;
            lw.materialize(l)
        };
        self.graph.retain_node(lowered);
        let arr = match self.graph.node(lowered).arrangements.first().copied() {
            Some(a) => a,
            None => {
                let schema = self.graph.node(lowered).schema.clone();
                let full = Layout::full(&schema);
                self.graph.add_arrangement(&mut self.env, lowered, full.key.clone(), full.specs.clone(), vec![])
            }
        };
        self.graph.retain_arr(arr);
        self.graph.gc_since(&mut self.env, node_mark, arr_mark)?;
        self.rt.views.insert(id, ViewRt { node: lowered, arr, errors: BTreeMap::new() });
        Ok(())
    }

    /// Definition (query text and output columns) of CREATE [MATERIALIZED]
    /// VIEW against the current catalog.
    fn view_def(&self, cv: &ast::CreateView) -> Result<ViewDef> {
        let bq = crate::sql::plan_query(&self.catalog, &cv.query, vec![])?;
        if cv.materialized {
            check_deterministic(&bq.rel)?;
        }
        let mut names: Vec<String> = bq.names.iter().take(bq.visible).cloned().collect();
        for (n, c) in names.iter_mut().zip(&cv.columns) {
            *n = ident(&c.name);
        }
        let types = bq.column_types();
        let columns: Vec<Column> = names.into_iter().zip(types).map(|(n, t)| Column::new(n, t)).collect();
        Ok(ViewDef { sql: cv.query.to_string(), columns })
    }

    /// `view_def` of a view's original CREATE statement.
    fn view_def_of_sql(&self, create_sql: &str) -> Result<ViewDef> {
        match parse_statement(create_sql)? {
            Statement::Sql(s) => match *s {
                ast::Statement::CreateView(cv) => self.view_def(&cv),
                _ => sql_bail!(codes::INTERNAL_ERROR, "not a view definition: {create_sql}"),
            },
            _ => sql_bail!(codes::INTERNAL_ERROR, "not a view definition: {create_sql}"),
        }
    }

    fn apply_create_view(&mut self, gsn: Gsn, sql: &str, cv: &ast::CreateView) -> Result<ExecResult> {
        let (_, name) = object_name(&cv.name)?;
        let tag = if cv.materialized { "CREATE MATERIALIZED VIEW" } else { "CREATE VIEW" };
        let existing = self.catalog.by_name(&name).cloned();
        if let Some(e) = &existing {
            if cv.if_not_exists {
                return Ok(ExecResult::Command { tag: tag.into() });
            }
            if !cv.or_replace {
                sql_bail!(codes::DUPLICATE_TABLE, "relation \"{name}\" already exists");
            }
            if !matches!(e.kind, ObjectKind::MaterializedView(_) | ObjectKind::View(_)) {
                sql_bail!(codes::WRONG_OBJECT_TYPE, "\"{name}\" is not a view");
            }
        }
        let def = self.view_def(cv)?;
        let query_sql = def.sql.clone();
        let id = match &existing {
            Some(e) => e.id,
            None => self.catalog.alloc_id(),
        };
        if let Some(e) = &existing {
            let kind = if cv.materialized { ObjectKind::MaterializedView(def.clone()) } else { ObjectKind::View(def.clone()) };
            self.check_replacement(e, &kind)?;
            // replacement: build the new version, swap at this GSN
            if cv.materialized && matches!(e.kind, ObjectKind::MaterializedView(_)) {
                let r = self.replace_view(gsn, id, &name, sql, def)?;
                self.replan_dependents(gsn, id)?;
                return Ok(r);
            }
            self.drop_object(e.id)?;
        }
        if cv.materialized {
            self.create_view_rt(id, &query_sql)?;
            self.graph.hydrate(&mut self.env, gsn)?;
            // the errors in the root's contents (also when it is shared with
            // other views and was hydrated long ago)
            let errs = self.graph.errors_of(self.rt.views[&id].node);
            merge_errors(&mut self.rt.views.get_mut(&id).unwrap().errors, &errs);
            self.catalog.insert(CatalogObject { id, name, kind: ObjectKind::MaterializedView(def), created_gsn: gsn, create_sql: sql.to_string() });
        } else {
            self.catalog.insert(CatalogObject { id, name, kind: ObjectKind::View(def), created_gsn: gsn, create_sql: sql.to_string() });
        }
        if existing.is_some() {
            // materialized views reading the old definition read the new one
            self.replan_dependents(gsn, id)?;
        }
        Ok(ExecResult::Command { tag: tag.into() })
    }


    /// Would replacing view `old` by `new` leave everything that depends on
    /// it valid? Indexes and sinks need a materialized view (indexes with
    /// their columns in place); every other materialized view must still
    /// plan. Checked before anything changes, so a rejected replacement has
    /// no effect.
    fn check_replacement(&self, old: &CatalogObject, new: &ObjectKind) -> Result<()> {
        let old_cols = match &old.kind {
            ObjectKind::MaterializedView(v) | ObjectKind::View(v) => &v.columns,
            _ => return Ok(()),
        };
        let new_cols = match new {
            ObjectKind::MaterializedView(v) => Some(&v.columns),
            _ => None,
        };
        let refuse = |what: String| {
            Err(SqlError::new(codes::DEPENDENT_OBJECTS_STILL_EXIST, format!("cannot replace view \"{}\": {what}", old.name)))
        };
        for o in self.catalog.objects.values() {
            match &o.kind {
                ObjectKind::Index(i) if i.on == old.id && !new_cols.is_some_and(|c| index_fits(&i.columns, old_cols, c)) => {
                    return refuse(format!("index \"{}\" depends on its columns", o.name));
                }
                ObjectKind::Sink(s) if s.from == old.id && !new_cols.is_some_and(|c| same_columns(c, old_cols)) => {
                    // its target table has the view's columns
                    return refuse(format!("sink \"{}\" reads its columns", o.name));
                }
                _ => {}
            }
        }
        let mut cat = self.catalog.clone();
        cat.insert(CatalogObject { kind: new.clone(), ..old.clone() });
        for o in self.catalog.objects.values() {
            if let ObjectKind::MaterializedView(v) = &o.kind
                && o.id != old.id
            {
                let q = crate::sql::parser::parse_single_query(&v.sql)?;
                // (it inlines the views it reads: they must keep it deterministic)
                if let Err(e) = crate::sql::plan_query(&cat, &q, vec![]).and_then(|bq| check_deterministic(&bq.rel)) {
                    return refuse(format!("materialized view \"{}\" would no longer be valid: {}", o.name, e.message));
                }
            }
        }
        Ok(())
    }

    /// After object `changed` got a new definition or dataflow at `gsn`,
    /// re-plan the materialized views reading it (directly or through other
    /// views) so that they read the new version, as a restart would. A view
    /// that no longer plans is dropped together with its indexes and sinks
    /// (and, in turn, the views reading it).
    pub(crate) fn replan_dependents(&mut self, gsn: Gsn, changed: ObjectId) -> Result<()> {
        let objects: Vec<CatalogObject> = self.catalog.objects.values().cloned().collect();
        let order: Vec<(CatalogObject, Result<Vec<ObjectId>>)> =
            self.dependency_order(&objects).into_iter().map(|(o, d)| (o.clone(), d)).collect();
        let mut affected = std::collections::HashSet::from([changed]);
        // views (inlined into queries) over a changed object change with it
        loop {
            let more: Vec<ObjectId> = objects
                .iter()
                .filter(|o| !affected.contains(&o.id))
                .filter(|o| matches!(&o.kind, ObjectKind::View(v) if affected.iter().any(|a| self.catalog.query_reads(&v.sql, *a))))
                .map(|o| o.id)
                .collect();
            if more.is_empty() {
                break;
            }
            affected.extend(more);
        }
        for (o, deps) in order {
            if o.id == changed || !matches!(o.kind, ObjectKind::MaterializedView(_)) || self.catalog.get(o.id).is_none() {
                continue;
            }
            let res = match deps {
                Ok(d) if !d.iter().any(|x| affected.contains(x)) => continue,
                Ok(_) => self.view_def_of_sql(&o.create_sql).and_then(|def| self.replace_view(gsn, o.id, &o.name, &o.create_sql, def)),
                Err(e) => Err(e),
            };
            if let Err(e) = res {
                log::error!("materialized view {} is dropped: it is no longer valid after a change to object {changed}: {e}", o.name);
                self.drop_view_and_attachments(o.id)?;
            }
            affected.insert(o.id);
        }
        Ok(())
    }

    /// Drop a view with the indexes on it and the sinks reading it.
    fn drop_view_and_attachments(&mut self, id: ObjectId) -> Result<()> {
        let attached: Vec<ObjectId> = self
            .catalog
            .objects
            .values()
            .filter(|o| match &o.kind {
                ObjectKind::Index(i) => i.on == id,
                ObjectKind::Sink(s) => s.from == id,
                _ => false,
            })
            .map(|o| o.id)
            .collect();
        for a in attached {
            self.drop_object(a)?;
        }
        self.drop_object(id)
    }

    /// Replace a materialized view: plan the new version (sharing unchanged
    /// fragments), hydrate it, switch readers and subscribers at `gsn`, and
    /// release the old version.
    fn replace_view(&mut self, gsn: Gsn, id: ObjectId, name: &str, sql: &str, def: ViewDef) -> Result<ExecResult> {
        let old = self.rt.views.remove(&id).expect("view runtime");
        let old_rows = self.graph.scan_arr(&mut self.env, old.arr, crate::storage::ReadAt::Latest)?;
        let old_cat = self.catalog.get(id).cloned();
        self.catalog.insert(CatalogObject {
            id,
            name: name.to_string(),
            kind: ObjectKind::MaterializedView(def.clone()),
            created_gsn: gsn,
            create_sql: sql.to_string(),
        });
        if let Err(e) = self.create_view_rt(id, &def.sql) {
            // roll back
            self.rt.views.insert(id, old);
            if let Some(o) = old_cat {
                self.catalog.insert(o);
            }
            return Err(e);
        }
        // indexes on the view move to the new version; one whose columns
        // are gone is dropped (user replacements are checked beforehand)
        let old_cols = match old_cat.as_ref().map(|o| &o.kind) {
            Some(ObjectKind::MaterializedView(v)) => v.columns.clone(),
            _ => vec![],
        };
        let indexes: Vec<(ObjectId, String, IndexDef)> = self
            .catalog
            .objects
            .values()
            .filter_map(|o| match &o.kind {
                ObjectKind::Index(i) if i.on == id => Some((o.id, o.name.clone(), i.clone())),
                _ => None,
            })
            .collect();
        let mut old_index_arrs = Vec::new();
        for (iid, iname, idef) in indexes {
            if let Some(a) = self.rt.indexes.remove(&iid) {
                old_index_arrs.push(a);
            }
            if index_fits(&idef.columns, &old_cols, &def.columns) {
                self.create_index_rt(iid, &idef)?;
            } else {
                log::error!("index {iname} is dropped: its columns are gone from materialized view {name}");
                self.catalog.remove(iid);
            }
        }
        self.graph.hydrate(&mut self.env, gsn)?;
        // the new root's error records (also when it is shared and was
        // hydrated long ago, or is the old version's own root)
        let new_node = self.rt.views[&id].node;
        let errs = self.graph.errors_of(new_node);
        merge_errors(&mut self.rt.views.get_mut(&id).unwrap().errors, &errs);
        let new_arr = self.rt.views[&id].arr;
        let new_rows = self.graph.scan_arr(&mut self.env, new_arr, crate::storage::ReadAt::Latest)?;
        // subscribers see the change from the old to the new definition
        let mut diff: Vec<(crate::types::Row, i64)> = new_rows;
        diff.extend(old_rows.into_iter().map(|(r, w)| (r, -w)));
        crate::dataflow::consolidate(&mut diff);
        // (by view: other views and ad hoc subscriptions may share the old node)
        for s in self.subs.iter_mut() {
            if s.view == Some(id) {
                s.node = new_node;
                if !diff.is_empty() {
                    let _ = s.tx.send(SubEvent::Data { gsn, rows: diff.clone() });
                }
            }
        }
        // the view's sinks (not those of other views sharing the old node)
        let sinks: Vec<ObjectId> = self
            .sink_handles
            .keys()
            .copied()
            .filter(|sid| matches!(self.catalog.get(*sid).map(|o| &o.kind), Some(ObjectKind::Sink(sd)) if sd.from == id))
            .collect();
        if same_columns(&def.columns, &old_cols) {
            for sid in &sinks {
                if let Some(sink) = self.sink_handles.get_mut(sid) {
                    sink.node = new_node;
                    sink.send(gsn, &diff);
                }
            }
        } else {
            // (re-planned after an upstream change) new rows, new columns
            self.restart_sinks(sinks)?;
        }
        for a in old_index_arrs {
            self.graph.release_arr(&mut self.env, a)?;
        }
        self.graph.release_arr(&mut self.env, old.arr)?;
        self.graph.release_node(&mut self.env, old.node)?;
        Ok(ExecResult::Command { tag: "CREATE MATERIALIZED VIEW".into() })
    }

    fn apply_create_index(&mut self, gsn: Gsn, sql: &str, ci: &ast::CreateIndex) -> Result<ExecResult> {
        let (_, tname) = object_name(&ci.table_name)?;
        let Some(target) = self.catalog.by_name(&tname).cloned() else {
            sql_bail!(codes::UNDEFINED_TABLE, "relation \"{tname}\" does not exist");
        };
        let columns: Vec<Column> = match &target.kind {
            ObjectKind::Table(t) => t.columns.clone(),
            ObjectKind::MaterializedView(v) => v.columns.clone(),
            _ => sql_bail!(codes::WRONG_OBJECT_TYPE, "\"{tname}\" is not a table or materialized view"),
        };
        let mut key = Vec::new();
        for c in &ci.columns {
            let ast::Expr::Identifier(id) = &c.column.expr else {
                sql_bail!(codes::FEATURE_NOT_SUPPORTED, "only column indexes are supported");
            };
            let n = ident(id);
            let pos = columns.iter().position(|col| col.name == n).ok_or_else(|| {
                SqlError::new(codes::UNDEFINED_COLUMN, format!("column \"{n}\" does not exist"))
            })?;
            key.push(pos);
        }
        let name = match &ci.name {
            Some(n) => object_name(n)?.1,
            None => format!("{}_{}_idx", tname, key.iter().map(|k| columns[*k].name.clone()).collect::<Vec<_>>().join("_")),
        };
        if self.catalog.by_name(&name).is_some() {
            if ci.if_not_exists {
                return Ok(ExecResult::Command { tag: "CREATE INDEX".into() });
            }
            sql_bail!(codes::DUPLICATE_TABLE, "relation \"{name}\" already exists");
        }
        let id = self.catalog.alloc_id();
        let def = IndexDef { on: target.id, columns: key };
        self.create_index_rt(id, &def)?;
        self.graph.hydrate(&mut self.env, gsn)?;
        self.catalog.insert(CatalogObject { id, name, kind: ObjectKind::Index(def), created_gsn: gsn, create_sql: sql.to_string() });
        Ok(ExecResult::Command { tag: "CREATE INDEX".into() })
    }

    pub(crate) fn create_index_rt(&mut self, id: ObjectId, def: &IndexDef) -> Result<()> {
        let node = self
            .rt
            .tables
            .get(&def.on)
            .copied()
            .or_else(|| self.rt.views.get(&def.on).map(|v| v.node))
            .ok_or_else(|| SqlError::internal("index target without dataflow"))?;
        let key: Vec<crate::expr::Expr> = def.columns.iter().map(|c| crate::expr::Expr::Column(*c)).collect();
        let n = key.len();
        let arr = self.graph.add_arrangement(&mut self.env, node, key, vec![SortSpec::ASC; n], vec![]);
        self.graph.retain_arr(arr);
        self.rt.indexes.insert(id, arr);
        Ok(())
    }

    fn drop_with_dependents(&mut self, id: ObjectId, cascade: bool) -> Result<()> {
        let deps = self.catalog.dependents(id);
        if !deps.is_empty() && !cascade {
            let obj = self.catalog.get(id).unwrap();
            let dep = self.catalog.get(deps[0]).unwrap();
            return Err(SqlError::new(
                codes::DEPENDENT_OBJECTS_STILL_EXIST,
                format!("cannot drop {} {} because other objects depend on it", obj.kind.name(), obj.name),
            )
            .with_detail(format!("{} {} depends on {} {}", dep.kind.name(), dep.name, obj.kind.name(), obj.name))
            .with_hint("Use DROP ... CASCADE to drop the dependent objects too."));
        }
        for d in deps {
            if self.catalog.get(d).is_some() {
                self.drop_with_dependents(d, true)?;
            }
        }
        self.drop_object(id)
    }

    pub(crate) fn drop_object(&mut self, id: ObjectId) -> Result<()> {
        let Some(obj) = self.catalog.remove(id) else { return Ok(()) };
        match obj.kind {
            ObjectKind::Table(_) => {
                if let Some(n) = self.rt.tables.remove(&id) {
                    self.graph.release_node(&mut self.env, n)?;
                }
            }
            ObjectKind::MaterializedView(_) => {
                if let Some(v) = self.rt.views.remove(&id) {
                    for s in self.subs.iter() {
                        if s.view == Some(id) {
                            let _ = s.tx.send(SubEvent::Error(SqlError::new(
                                codes::UNDEFINED_TABLE,
                                format!("materialized view \"{}\" was dropped", obj.name),
                            )));
                            s.cancel.store(true, std::sync::atomic::Ordering::Release);
                        }
                    }
                    self.graph.release_arr(&mut self.env, v.arr)?;
                    self.graph.release_node(&mut self.env, v.node)?;
                }
            }
            ObjectKind::Index(_) => {
                if let Some(a) = self.rt.indexes.remove(&id) {
                    self.graph.release_arr(&mut self.env, a)?;
                }
            }
            ObjectKind::Source(def) => {
                if let Some(h) = self.source_handles.remove(&id) {
                    h.stop();
                }
                if !self.replaying {
                    crate::source::drop_slot_async(def.conninfo.clone(), def.slot.clone());
                }
                self.source_states.remove(&id);
                // a dropped source no longer holds back reads waiting for an LSN
                self.source_lsns.remove(&id);
                self.lsn_map.retain(|(s, _, _)| *s != id);
                self.shared.source_lsn.lock().remove(&id);
            }
            ObjectKind::Sink(_) => {
                if let Some(h) = self.sink_handles.remove(&id) {
                    h.stop();
                }
            }
            ObjectKind::View(_) => {}
        }
        Ok(())
    }

    /// Upstream schema change: rebuild the table's index in the new shape,
    /// re-plan dependent views and swap them at `gsn`.
    pub(crate) fn apply_schema_change(&mut self, gsn: Gsn, table: ObjectId, columns: Vec<Column>, pk: Vec<usize>) -> Result<()> {
        let Some(obj) = self.catalog.get(table).cloned() else { return Ok(()) };
        let ObjectKind::Table(mut def) = obj.kind.clone() else { return Ok(()) };
        if def.columns == columns && def.pk == pk {
            return Ok(());
        }
        let old_node = self.rt.tables[&table];
        let old_rows = self.graph.snapshot(&mut self.env, old_node)?.data;
        let old_columns = def.columns.clone();
        // map old columns to new by name
        let mapping: Vec<Option<usize>> = columns.iter().map(|c| def.columns.iter().position(|o| o.name == c.name)).collect();
        def.columns = columns.clone();
        def.pk = pk;
        def.version += 1;
        self.create_table_rt(table, &def, gsn)?;
        let new_node = self.rt.tables[&table];
        let mut rows = Vec::with_capacity(old_rows.len());
        for (r, w) in old_rows {
            let nr: crate::types::Row = mapping
                .iter()
                .zip(&columns)
                .map(|(m, c)| match m {
                    Some(i) => crate::expr::cast::cast_datum(&r[*i], &c.ty.ty, c.ty.typmod, true).unwrap_or(crate::types::Datum::Null),
                    None => crate::types::Datum::Null,
                })
                .collect();
            // a row without its (new) key cannot be found again by the key
            // the re-snapshot upserts: leave it to the re-snapshot
            if def.pk.iter().any(|c| nr[*c].is_null()) {
                continue;
            }
            rows.push((nr, w));
        }
        let NodeKind::Table { primary, .. } = self.graph.node(new_node).kind else { unreachable!() };
        let arr = self.graph.arr(primary);
        let mut ups = Vec::with_capacity(rows.len());
        for (r, w) in &rows {
            ups.push((arr.layout.encode_row(r)?, *w));
        }
        let tree = arr.tree;
        self.env.apply(tree, &mut ups, gsn)?;
        self.catalog.insert(CatalogObject { kind: ObjectKind::Table(def.clone()), ..obj.clone() });
        // indexes on the table move to the new version (or go with their columns)
        let indexes: Vec<(ObjectId, String, IndexDef)> = self
            .catalog
            .objects
            .values()
            .filter_map(|o| match &o.kind {
                ObjectKind::Index(i) if i.on == table => Some((o.id, o.name.clone(), i.clone())),
                _ => None,
            })
            .collect();
        for (iid, iname, idef) in indexes {
            if let Some(a) = self.rt.indexes.remove(&iid) {
                self.graph.release_arr(&mut self.env, a)?;
            }
            if index_fits(&idef.columns, &old_columns, &columns) {
                self.create_index_rt(iid, &idef)?;
            } else {
                log::error!("index {iname} is dropped: its columns are gone from table {}", obj.name);
                self.catalog.remove(iid);
            }
        }
        self.graph.hydrate(&mut self.env, gsn)?;
        // subscribers to the table were promised the old columns
        for s in self.subs.iter() {
            if s.node == old_node {
                let _ = s.tx.send(SubEvent::Error(SqlError::new(
                    codes::OBJECT_NOT_IN_PREREQUISITE_STATE,
                    format!("table \"{}\" changed its columns upstream", obj.name),
                )));
                s.cancel.store(true, std::sync::atomic::Ordering::Release);
            }
        }
        // sinks from the table start over with the new columns
        self.restart_sinks_on(old_node)?;
        self.replan_dependents(gsn, table)?;
        self.graph.release_node(&mut self.env, old_node)?;
        Ok(())
    }

    /// Restart the sinks reading dataflow node `node` (whose relation was
    /// rebuilt with other columns) from their relation's current version.
    fn restart_sinks_on(&mut self, node: NodeId) -> Result<()> {
        let sinks: Vec<ObjectId> = self.sink_handles.iter().filter(|(_, h)| h.node == node).map(|(id, _)| *id).collect();
        self.restart_sinks(sinks)
    }

    /// Restart sinks from their relation's current version.
    fn restart_sinks(&mut self, sinks: Vec<ObjectId>) -> Result<()> {
        for id in sinks {
            if let Some(h) = self.sink_handles.remove(&id) {
                h.stop();
            }
            if let (Some(o), Some(handle)) = (self.catalog.get(id).cloned(), self.handle.clone())
                && let ObjectKind::Sink(sd) = &o.kind
            {
                self.start_sink(id, &o.name, sd, &handle)?;
            }
        }
        Ok(())
    }

    pub(crate) fn start_sink(&mut self, id: ObjectId, name: &str, def: &SinkDef, handle: &EngineHandle) -> Result<()> {
        let node = self
            .rt
            .views
            .get(&def.from)
            .map(|v| v.node)
            .or_else(|| self.rt.tables.get(&def.from).copied())
            .ok_or_else(|| SqlError::internal("sink source without dataflow"))?;
        let columns = match self.catalog.get(def.from).map(|o| &o.kind) {
            Some(ObjectKind::MaterializedView(v)) => v.columns.clone(),
            Some(ObjectKind::Table(t)) => t.columns.clone(),
            _ => vec![],
        };
        let snapshot = self.graph.snapshot(&mut self.env, node)?.data;
        let h = crate::sink::start(id, name.to_string(), def.clone(), node, columns, snapshot, self.processed, handle.clone())?;
        self.sink_handles.insert(id, h);
        Ok(())
    }
}

/// Same column names and types (as a sink's target table has them)?
fn same_columns(a: &[Column], b: &[Column]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.name == y.name && x.ty.ty == y.ty.ty && x.ty.typmod == y.ty.typmod)
}

/// Do index key columns (positions into `old`) denote the same columns in
/// `new`?
fn index_fits(key: &[usize], old: &[Column], new: &[Column]) -> bool {
    key.iter().all(|&k| k < new.len() && old.get(k).is_some_and(|c| c.name == new[k].name))
}

fn object_type_name(t: ast::ObjectType) -> &'static str {
    match t {
        ast::ObjectType::Table => "table",
        ast::ObjectType::View => "view",
        ast::ObjectType::MaterializedView => "materialized view",
        ast::ObjectType::Index => "index",
        _ => "object",
    }
}

/// Reject non-deterministic functions in maintained views.
pub(crate) fn check_deterministic(rel: &crate::sql::Rel) -> Result<()> {
    let mut bad = Vec::new();
    fn walk(r: &crate::sql::Rel, bad: &mut Vec<&'static str>) -> Result<()> {
        match r {
            // now() is allowed in temporal filters: the clock is a logged input
            crate::sql::Rel::Filter { preds, .. } => {
                let (regular, temporal) = crate::dataflow::temporal::split(preds)?;
                for e in &regular {
                    e.nondeterministic_functions(bad);
                }
                for t in &temporal {
                    t.other.nondeterministic_functions(bad);
                }
            }
            _ => {
                for e in r.exprs() {
                    if e.contains_now() {
                        return Err(crate::dataflow::temporal::analyze(e).err().unwrap_or_else(|| {
                            SqlError::new(codes::FEATURE_NOT_SUPPORTED, "now() is only supported in temporal filters of maintained views")
                        }));
                    }
                    e.nondeterministic_functions(bad);
                }
            }
        }
        for i in r.inputs() {
            walk(i, bad)?;
        }
        Ok(())
    }
    walk(rel, &mut bad)?;
    if let Some(f) = bad.first() {
        return Err(SqlError::new(
            codes::FEATURE_NOT_SUPPORTED,
            format!("function {f}() is not allowed in a materialized view"),
        )
        .with_detail("Maintained views must be deterministic: their state is a pure function of the input log."));
    }
    Ok(())
}

/// Build a local table definition from CREATE TABLE.
pub(crate) fn table_def_from_ast(ct: &ast::CreateTable) -> Result<TableDef> {
    if ct.query.is_some() {
        sql_bail!(codes::FEATURE_NOT_SUPPORTED, "CREATE TABLE AS is not supported");
    }
    let mut columns = Vec::new();
    let mut pk: Vec<usize> = Vec::new();
    for (i, c) in ct.columns.iter().enumerate() {
        let (ty, typmod) = sql_type_of(&c.data_type)?;
        let mut nullable = true;
        for o in &c.options {
            match &o.option {
                ast::ColumnOption::NotNull => nullable = false,
                ast::ColumnOption::Null => nullable = true,
                ast::ColumnOption::PrimaryKey(_) => {
                    pk.push(i);
                    nullable = false;
                }
                ast::ColumnOption::Unique(_) | ast::ColumnOption::Default(_) | ast::ColumnOption::Check(_) => {}
                ast::ColumnOption::ForeignKey(_) => {}
                _ => {}
            }
        }
        let name = ident(&c.name);
        if columns.iter().any(|x: &Column| x.name == name) {
            sql_bail!(codes::DUPLICATE_COLUMN, "column \"{name}\" specified more than once");
        }
        columns.push(Column::new(name, ColumnType { ty, typmod, nullable }));
    }
    for con in &ct.constraints {
        if let ast::TableConstraint::PrimaryKey(p) = con {
            if !pk.is_empty() {
                sql_bail!(codes::INVALID_COLUMN_REFERENCE, "multiple primary keys for table are not allowed");
            }
            for ic in &p.columns {
                let ast::Expr::Identifier(id) = &ic.column.expr else {
                    sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unsupported primary key expression");
                };
                let n = ident(id);
                let pos = columns.iter().position(|c| c.name == n).ok_or_else(|| {
                    SqlError::new(codes::UNDEFINED_COLUMN, format!("column \"{n}\" named in key does not exist"))
                })?;
                columns[pos].ty.nullable = false;
                pk.push(pos);
            }
        }
    }
    Ok(TableDef { columns, pk, source: None, upstream: None, version: 0 })
}
