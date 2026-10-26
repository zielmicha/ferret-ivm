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
/// re-created source never shares a slot that is still being dropped, and
/// to the data directory (`instance`), so that engines with similar catalogs
/// replicating the same database do not share one either. (Sources logged
/// by versions without the instance id keep names without it.)
fn default_slot_name(source: &str, id: ObjectId, instance: Option<&str>) -> String {
    let suffix = match instance {
        Some(i) => format!("_{i}_{id}"),
        None => format!("_{id}"),
    };
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

/// What a CREATE SOURCE record logs besides its statement: the discovered
/// tables and the data directory's id for the default slot name. (Older
/// records hold just the tables.)
#[derive(Serialize, Deserialize)]
struct SourceExtra {
    tables: Vec<DiscoveredTable>,
    instance: Option<String>,
}

fn parse_source_extra(extra: &str) -> Result<SourceExtra> {
    match serde_json::from_str::<SourceExtra>(extra) {
        Ok(x) => Ok(x),
        Err(_) => Ok(SourceExtra { tables: parse_discovered(extra)?, instance: None }),
    }
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
    pub(crate) fn submit_ddl(&mut self, stmt: &Statement, mut extra: String, reply: Sender<Result<ExecResult>>) -> Result<Option<ExecResult>> {
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
                    // planning, the determinism check and the column names
                    self.view_def(cv, false)?;
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
                let tables = parse_discovered(&extra)?;
                self.check_source_tables(name, &tables)?;
                // the data directory's id goes into the record, so that
                // replay names the slot the same way
                let instance = self.catalog.instance_id.get_or_insert_with(|| format!("{:08x}", crate::source::random_u64() as u32)).clone();
                extra = serde_json::to_string(&SourceExtra { tables, instance: Some(instance) }).map_err(|e| SqlError::internal(e.to_string()))?;
            }
            Statement::AlterSourceRefresh { name } => {
                let Some(src) = self.catalog.by_name(name) else {
                    sql_bail!(codes::UNDEFINED_OBJECT, "source \"{name}\" does not exist");
                };
                self.check_source_tables(name, &self.new_source_tables(src.id, parse_discovered(&extra)?))?;
            }
            Statement::CreateSink { name, from, key, .. } => {
                self.check_sink(name, from, key.as_deref())?;
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
                    // every name is resolved and checked before anything is
                    // dropped: the statement is atomic, as in PostgreSQL
                    let mut targets: Vec<ObjectId> = Vec::new();
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
                        if !targets.contains(&obj.id) {
                            targets.push(obj.id);
                        }
                    }
                    // objects dropped by the same statement do not hold
                    // each other back
                    if !cascade {
                        for &t in &targets {
                            self.check_dependents(t, &targets)?;
                        }
                    }
                    for t in targets {
                        if self.catalog.get(t).is_some() {
                            self.drop_with_dependents(t, true)?;
                        }
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
                let SourceExtra { tables: discovered, instance } = parse_source_extra(extra)?;
                self.check_source_tables(&name, &discovered)?;
                if self.catalog.instance_id.is_none() {
                    self.catalog.instance_id = instance.clone();
                }
                let id = self.catalog.alloc_id();
                let slot = slot.unwrap_or_else(|| default_slot_name(&name, id, instance.as_deref()));
                // a dropped source's slot by the same name is this source's now
                crate::source::cancel_slot_drop(&self.shared, &slot);
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
                let v = self.check_sink(&name, &from, key.as_deref())?;
                let id = self.catalog.alloc_id();
                let def = SinkDef { from: v.id, conninfo, table, key };
                self.catalog.insert(CatalogObject {
                    id,
                    name: name.clone(),
                    kind: ObjectKind::Sink(def.clone()),
                    created_gsn: gsn,
                    create_sql: sql.to_string(),
                });
                // (also while replaying: sinks receive replayed records)
                if let Some(h) = self.handle.clone() {
                    // the sink is durable now; a failure to start it is not a DDL error
                    self.start_sink_or_mark_broken(id, &name, &def, &h);
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

    /// Can sink `name` read relation `from` with this key? Returns the
    /// relation. Checked before logging and again on apply: the sink thread
    /// creates its target table with the key as primary key, and would
    /// otherwise retry an impossible definition forever.
    fn check_sink(&self, name: &str, from: &str, key: Option<&[String]>) -> Result<CatalogObject> {
        if self.catalog.by_name(name).is_some() {
            sql_bail!(codes::DUPLICATE_OBJECT, "sink \"{name}\" already exists");
        }
        let Some(v) = self.catalog.by_name(from).cloned() else {
            sql_bail!(codes::UNDEFINED_TABLE, "relation \"{from}\" does not exist");
        };
        let columns = match &v.kind {
            ObjectKind::MaterializedView(d) => &d.columns,
            ObjectKind::Table(t) => &t.columns,
            _ => sql_bail!(codes::WRONG_OBJECT_TYPE, "sink source \"{from}\" must be a table or materialized view"),
        };
        if let Some(e) = self.broken_error(v.id) {
            return Err(e);
        }
        let key = key.unwrap_or_default();
        for (i, k) in key.iter().enumerate() {
            let Some(c) = columns.iter().find(|c| &c.name == k) else {
                sql_bail!(codes::UNDEFINED_COLUMN, "sink key column \"{k}\" does not exist in \"{from}\"");
            };
            if key[..i].contains(k) {
                sql_bail!(codes::DUPLICATE_COLUMN, "column \"{k}\" appears twice in the sink key");
            }
            fn has_equality(t: &crate::types::SqlType) -> bool {
                use crate::types::SqlType as T;
                match t {
                    T::Json | T::JsonPath => false,
                    T::Array(e) => has_equality(e),
                    _ => true,
                }
            }
            if !has_equality(&c.ty.ty) {
                sql_bail!(
                    codes::FEATURE_NOT_SUPPORTED,
                    "sink key column \"{k}\" has type {}, which cannot be part of a primary key",
                    crate::types::format_type(&c.ty.ty, c.ty.typmod)
                );
            }
        }
        Ok(v)
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
        let node = self.add_table_node(id, def);
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
        Ok(())
    }

    /// The Table node and primary index of a base table, retained and
    /// registered (hydrated by the caller).
    fn add_table_node(&mut self, id: ObjectId, def: &TableDef) -> NodeId {
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
        self.rt.tables.insert(id, node);
        node
    }

    /// Plan and lower a materialized view; pins its root and read arrangement.
    pub(crate) fn create_view_rt(&mut self, id: ObjectId, sql: &str) -> Result<()> {
        let q = crate::sql::parser::parse_single_query(sql)?;
        // new definitions were planned with plan_query before
        let bq = crate::sql::plan_stored_query(&self.catalog, &q)?;
        let rel = bq.into_rel();
        // (a view over a broken one cannot be built either)
        self.check_broken(&rel)?;
        let node_mark = self.graph.node_count();
        let arr_mark = self.graph.arr_count();
        let rt = &self.rt;
        let objects = |oid: u64| -> Option<NodeId> { rt.tables.get(&oid).copied().or_else(|| rt.views.get(&oid).map(|v| v.node)) };
        let lowered = {
            let mut lw = Lowerer { g: &mut self.graph, env: &mut self.env, objects: &objects };
            lw.lower(&rel).map(|l| lw.materialize(l))
        };
        let lowered = match lowered {
            Ok(n) => n,
            Err(e) => {
                // what lowering created before it failed is unreferenced
                self.graph.gc_since(&mut self.env, node_mark, arr_mark)?;
                return Err(e);
            }
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
    /// VIEW against the current catalog. `stored`: a logged statement or a
    /// stored definition, which earlier versions may have accepted with a
    /// parameter placeholder (statements are checked with `stored` false
    /// before they are logged).
    fn view_def(&self, cv: &ast::CreateView, stored: bool) -> Result<ViewDef> {
        let bq = if stored {
            crate::sql::plan_stored_query(&self.catalog, &cv.query)?
        } else {
            crate::sql::plan_query(&self.catalog, &cv.query, vec![])?
        };
        if cv.materialized {
            check_deterministic(&bq.rel)?;
            check_catalog_reads(&bq)?;
        }
        self.check_broken(&bq.rel)?;
        let mut names: Vec<String> = bq.names.iter().take(bq.visible).cloned().collect();
        // as in PostgreSQL: the column list renames the leading columns and
        // must not be longer than the query's select list
        if cv.columns.len() > names.len() {
            sql_bail!(
                codes::SYNTAX_ERROR,
                "{}",
                if cv.materialized { "too many column names were specified" } else { "CREATE VIEW specifies more column names than columns" }
            );
        }
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
                ast::Statement::CreateView(cv) => self.view_def(&cv, true),
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
        let def = self.view_def(cv, true)?;
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
            self.catalog.insert(CatalogObject { id, name, kind: ObjectKind::MaterializedView(def), created_gsn: gsn, create_sql: sql.to_string() });
            // (hydrated after the statement, in the background: its reply
            // waits for it)
            self.record_then.push(hydrate::Then::ViewErrors(id));
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
                ObjectKind::Index(i) if i.on == old.id && !new_cols.is_some_and(|c| remap_index(&i.columns, old_cols, c).is_some()) => {
                    return refuse(format!("index \"{}\" depends on its columns", o.name));
                }
                ObjectKind::Sink(s) if s.from == old.id && !new_cols.is_some_and(|c| same_columns(c, old_cols)) => {
                    // its target table has the view's columns
                    return refuse(format!("sink \"{}\" reads its columns", o.name));
                }
                _ => {}
            }
        }
        // (a broken view does not plan, whatever the replacement; a view
        // that does not read the replaced one is not re-planned, and may
        // predate rules that are stricter now)
        let readers: Vec<&CatalogObject> = self
            .catalog
            .objects
            .values()
            .filter(|o| {
                matches!(&o.kind, ObjectKind::MaterializedView(v)
                    if o.id != old.id && !self.rt.broken.contains_key(&o.id) && self.catalog.query_reads(&v.sql, old.id))
            })
            .collect();
        if readers.is_empty() {
            return Ok(());
        }
        // the catalog with the replacement (planning a maintained view never
        // reads the DDL history: maintained views cannot read the catalog)
        let mut cat = Catalog::new();
        cat.objects = self.catalog.objects.clone();
        cat.next_id = self.catalog.next_id;
        cat.instance_id = self.catalog.instance_id.clone();
        cat.insert(CatalogObject { kind: new.clone(), ..old.clone() });
        for o in readers {
            if let ObjectKind::MaterializedView(v) = &o.kind {
                let q = crate::sql::parser::parse_single_query(&v.sql)?;
                // (it inlines the views it reads: they must keep it deterministic)
                if let Err(e) = crate::sql::plan_stored_query(&cat, &q).and_then(|bq| {
                    check_deterministic(&bq.rel)?;
                    check_catalog_reads(&bq)
                }) {
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
    /// (and, in turn, the views reading it); a broken view is rebuilt if it
    /// plans now, and otherwise stays broken.
    pub(crate) fn replan_dependents(&mut self, gsn: Gsn, changed: ObjectId) -> Result<()> {
        // what each view reads, as the binder resolves it (bound once here,
        // not per test); conservatively by name for one that does not bind
        let reads: Vec<(CatalogObject, Option<Vec<ObjectId>>)> = self
            .catalog
            .objects
            .values()
            .filter_map(|o| match &o.kind {
                ObjectKind::View(v) | ObjectKind::MaterializedView(v) => Some((o.clone(), self.catalog.references(&v.sql))),
                _ => None,
            })
            .collect();
        let reads_any = |o: &CatalogObject, refs: &Option<Vec<ObjectId>>, set: &std::collections::HashSet<ObjectId>| match refs {
            Some(r) => r.iter().any(|x| set.contains(x)),
            None => match &o.kind {
                ObjectKind::View(v) | ObjectKind::MaterializedView(v) => {
                    set.iter().any(|a| self.catalog.get(*a).is_some_and(|ao| crate::catalog::sql_mentions(&v.sql, &ao.name)))
                }
                _ => false,
            },
        };
        // views (inlined into queries) over a changed object change with it
        let mut affected = std::collections::HashSet::from([changed]);
        loop {
            let more: Vec<ObjectId> = reads
                .iter()
                .filter(|(o, refs)| matches!(o.kind, ObjectKind::View(_)) && !affected.contains(&o.id) && reads_any(o, refs, &affected))
                .map(|(o, _)| o.id)
                .collect();
            if more.is_empty() {
                break;
            }
            affected.extend(more);
        }
        // the materialized views reading them, and those reading these in
        // turn (re-planned after them): only those are planned
        let mut reached = affected.clone();
        let mut replan: Vec<CatalogObject> = Vec::new();
        loop {
            let more: Vec<&CatalogObject> = reads
                .iter()
                .filter(|(o, refs)| matches!(o.kind, ObjectKind::MaterializedView(_)) && !reached.contains(&o.id) && reads_any(o, refs, &reached))
                .map(|(o, _)| o)
                .collect();
            if more.is_empty() {
                break;
            }
            for o in more {
                reached.insert(o.id);
                replan.push(o.clone());
            }
        }
        let order: Vec<(CatalogObject, Result<Vec<ObjectId>>)> =
            self.dependency_order(&replan).into_iter().map(|(o, d)| (o.clone(), d)).collect();
        for (o, deps) in order {
            if self.catalog.get(o.id).is_none() {
                continue;
            }
            let res = match deps {
                Ok(d) if !d.iter().any(|x| affected.contains(x)) => continue,
                Ok(_) => self.view_def_of_sql(&o.create_sql).and_then(|def| self.replace_view(gsn, o.id, &o.name, &o.create_sql, def)),
                Err(e) => Err(e),
            };
            if let Err(e) = res {
                if let std::collections::btree_map::Entry::Occupied(mut b) = self.rt.broken.entry(o.id) {
                    b.insert(e.message);
                    continue;
                }
                log::error!("materialized view {} is dropped: it is no longer valid after a change to object {changed}: {e}", o.name);
                self.drop_view_and_attachments(o.id)?;
            }
            affected.insert(o.id);
        }
        Ok(())
    }

    /// Drop a view that is no longer valid with the indexes on it, the sinks
    /// reading it and the virtual views reading it (which no longer plan
    /// either), with what depends on those, as DROP ... CASCADE would.
    /// (Materialized views reading it are re-planned after it, and dropped
    /// then.)
    fn drop_view_and_attachments(&mut self, id: ObjectId) -> Result<()> {
        let name = self.catalog.get(id).map(|o| o.name.clone()).unwrap_or_default();
        let views: Vec<ObjectId> = self
            .catalog
            .dependents(id)
            .into_iter()
            .filter(|d| matches!(self.catalog.get(*d).map(|o| &o.kind), Some(ObjectKind::View(_))))
            .collect();
        for v in views {
            if let Some(o) = self.catalog.get(v) {
                log::error!("view {} is dropped: it reads {name}, which is no longer valid", o.name);
                self.drop_with_dependents(v, true)?;
            }
        }
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
    /// fragments) and switch readers to it at `gsn`. Once it is hydrated
    /// (after the statement, in the background) its subscribers and sinks
    /// get the change from the old to the new contents as of `gsn`, and the
    /// old version is released (`finish_replace`).
    fn replace_view(&mut self, gsn: Gsn, id: ObjectId, name: &str, sql: &str, def: ViewDef) -> Result<ExecResult> {
        if self.rt.broken.contains_key(&id) {
            return self.repair_view(gsn, id, name, sql, def);
        }
        let old = self.rt.views.remove(&id).expect("view runtime");
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
        // indexes on the view move to the new version (user replacements
        // are checked beforehand)
        let old_cols = match old_cat.as_ref().map(|o| &o.kind) {
            Some(ObjectKind::MaterializedView(v)) => v.columns.clone(),
            _ => vec![],
        };
        let old_index_arrs = self.move_indexes(id, &format!("materialized view {name}"), &old_cols, &def.columns);
        // (by view: other views and ad hoc subscriptions may share the old
        // node) subscribers see the change from the old to the new
        // definition once it is hydrated: until then they get nothing
        let same = same_columns(&def.columns, &old_cols);
        for s in self.subs.iter_mut() {
            if s.view != Some(id) {
                continue;
            }
            if same {
                s.held = true;
            } else {
                // they were promised the old columns
                let _ = s.tx.send(SubEvent::Error(SqlError::new(
                    codes::OBJECT_NOT_IN_PREREQUISITE_STATE,
                    format!("materialized view \"{name}\" changed its columns"),
                )));
                s.cancel.store(true, std::sync::atomic::Ordering::Release);
            }
        }
        // subscriptions to queries reading the view (directly or through
        // other views) have dataflows built on its old version, which they
        // would go on with (and which receives no more changes when the
        // view was re-planned after an upstream change): they end
        for s in self.subs.iter() {
            if s.view != Some(id) && s.reads.contains(&id) && !s.cancel.load(std::sync::atomic::Ordering::Acquire) {
                let _ = s.tx.send(SubEvent::Error(SqlError::new(
                    codes::OBJECT_NOT_IN_PREREQUISITE_STATE,
                    format!("materialized view \"{name}\" was replaced"),
                )));
                s.cancel.store(true, std::sync::atomic::Ordering::Release);
            }
        }
        // the view's sinks (not those of other views sharing the old node)
        let sinks: Vec<ObjectId> = self
            .sink_handles
            .keys()
            .copied()
            .filter(|sid| matches!(self.catalog.get(*sid).map(|o| &o.kind), Some(ObjectKind::Sink(sd)) if sd.from == id))
            .collect();
        // (held until then, also those that start over with new columns:
        // their targets must not get the old version's later changes)
        self.held_sinks.extend(sinks.iter().copied());
        // (no change to compute for nobody, nor when the new version is the
        // old dataflow: the difference of an arrangement with itself)
        let consumers =
            same && (!sinks.is_empty() || self.subs.iter().any(|s| s.view == Some(id) && !s.cancel.load(std::sync::atomic::Ordering::Acquire)));
        self.record_then.push(hydrate::Then::Replaced(Box::new(hydrate::Replaced { id, old, old_index_arrs, same, sinks, consumers })));
        Ok(ExecResult::Command { tag: "CREATE MATERIALIZED VIEW".into() })
    }

    /// Build a broken materialized view (it has no dataflow) under a new or
    /// re-planned definition: on success it is maintained again, its indexes
    /// are rebuilt (or dropped with their columns) and its sinks start.
    fn repair_view(&mut self, gsn: Gsn, id: ObjectId, name: &str, sql: &str, def: ViewDef) -> Result<ExecResult> {
        let old_cat = self.catalog.get(id).cloned();
        self.catalog.insert(CatalogObject {
            id,
            name: name.to_string(),
            kind: ObjectKind::MaterializedView(def.clone()),
            created_gsn: gsn,
            create_sql: sql.to_string(),
        });
        if let Err(e) = self.create_view_rt(id, &def.sql) {
            if let Some(o) = old_cat {
                self.catalog.insert(o);
            }
            return Err(e);
        }
        self.rt.broken.remove(&id);
        log::info!("materialized view {name} is maintained again");
        let old_cols = match old_cat.as_ref().map(|o| &o.kind) {
            Some(ObjectKind::MaterializedView(v)) => v.columns.clone(),
            _ => vec![],
        };
        // (broken views have no index arrangements to release)
        self.move_indexes(id, &format!("materialized view {name}"), &old_cols, &def.columns);
        // its sinks start once it is hydrated (`finish_repair`)
        self.record_then.push(hydrate::Then::Repaired(id));
        Ok(ExecResult::Command { tag: "CREATE MATERIALIZED VIEW".into() })
    }

    /// Move the indexes on relation `on` to its new version, whose columns
    /// are `new` (previously `old`): each follows its key columns by name,
    /// and one whose columns are gone is dropped (logged). Returns the
    /// arrangements of the old version, to release once nothing reads them.
    fn move_indexes(&mut self, on: ObjectId, what: &str, old: &[Column], new: &[Column]) -> Vec<ArrId> {
        let indexes: Vec<CatalogObject> =
            self.catalog.objects.values().filter(|o| matches!(&o.kind, ObjectKind::Index(i) if i.on == on)).cloned().collect();
        let mut old_arrs = Vec::new();
        for o in indexes {
            let ObjectKind::Index(idef) = &o.kind else { continue };
            if let Some(a) = self.rt.indexes.remove(&o.id) {
                old_arrs.push(a);
            }
            self.rt.broken.remove(&o.id);
            match remap_index(&idef.columns, old, new) {
                Some(columns) => {
                    let idef = IndexDef { on, columns };
                    if let Err(e) = self.create_index_rt(o.id, &idef) {
                        log::error!("index {} is broken: {}", o.name, e.message);
                        self.rt.broken.insert(o.id, e.message);
                    }
                    self.catalog.insert(CatalogObject { kind: ObjectKind::Index(idef), ..o });
                }
                None => {
                    log::error!("index {} is dropped: its columns are gone from {what}", o.name);
                    self.catalog.remove(o.id);
                }
            }
        }
        old_arrs
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
        self.catalog.insert(CatalogObject { id, name, kind: ObjectKind::Index(def), created_gsn: gsn, create_sql: sql.to_string() });
        // (hydrated after the statement, in the background: its reply waits
        // for it)
        Ok(ExecResult::Command { tag: "CREATE INDEX".into() })
    }

    pub(crate) fn create_index_rt(&mut self, id: ObjectId, def: &IndexDef) -> Result<()> {
        let node = match self.rt.tables.get(&def.on).copied().or_else(|| self.rt.views.get(&def.on).map(|v| v.node)) {
            Some(n) => n,
            None => return Err(self.broken_error(def.on).unwrap_or_else(|| SqlError::internal("index target without dataflow"))),
        };
        // (key columns beyond the relation's width would panic the layout)
        let width = self.graph.node(node).schema.len();
        if let Some(c) = def.columns.iter().find(|c| **c >= width) {
            sql_bail!(codes::UNDEFINED_COLUMN, "index column {} does not exist in its relation ({width} columns)", c + 1);
        }
        let key: Vec<crate::expr::Expr> = def.columns.iter().map(|c| crate::expr::Expr::Column(*c)).collect();
        let n = key.len();
        let arr = self.graph.add_arrangement(&mut self.env, node, key, vec![SortSpec::ASC; n], vec![]);
        self.graph.retain_arr(arr);
        self.rt.indexes.insert(id, arr);
        Ok(())
    }

    /// Fail unless object `id` can be dropped without CASCADE when the
    /// objects in `dropped` go too. As in PostgreSQL, a relation's indexes
    /// go with it: only views, sinks and a source's tables need CASCADE.
    fn check_dependents(&self, id: ObjectId, dropped: &[ObjectId]) -> Result<()> {
        let deps = self.catalog.dependents(id);
        let blocking = deps
            .iter()
            .find(|d| !dropped.contains(d) && !matches!(self.catalog.get(**d).map(|o| &o.kind), Some(ObjectKind::Index(_))));
        if let Some(&first) = blocking {
            let obj = self.catalog.get(id).unwrap();
            let dep = self.catalog.get(first).unwrap();
            return Err(SqlError::new(
                codes::DEPENDENT_OBJECTS_STILL_EXIST,
                format!("cannot drop {} {} because other objects depend on it", obj.kind.name(), obj.name),
            )
            .with_detail(format!("{} {} depends on {} {}", dep.kind.name(), dep.name, obj.kind.name(), obj.name))
            .with_hint("Use DROP ... CASCADE to drop the dependent objects too."));
        }
        Ok(())
    }

    fn drop_with_dependents(&mut self, id: ObjectId, cascade: bool) -> Result<()> {
        if !cascade {
            self.check_dependents(id, &[])?;
        }
        let deps = self.catalog.dependents(id);
        for d in deps {
            if self.catalog.get(d).is_some() {
                self.drop_with_dependents(d, true)?;
            }
        }
        self.drop_object(id)
    }

    pub(crate) fn drop_object(&mut self, id: ObjectId) -> Result<()> {
        let Some(obj) = self.catalog.remove(id) else { return Ok(()) };
        self.rt.broken.remove(&id);
        // subscriptions reading it (directly, through views or in a query)
        // end with an error
        for s in self.subs.iter() {
            if s.reads.contains(&id) && !s.cancel.load(std::sync::atomic::Ordering::Acquire) {
                let _ = s.tx.send(SubEvent::Error(SqlError::new(
                    codes::UNDEFINED_TABLE,
                    format!("{} \"{}\" was dropped", obj.kind.name(), obj.name),
                )));
                s.cancel.store(true, std::sync::atomic::Ordering::Release);
            }
        }
        match obj.kind {
            ObjectKind::Table(_) => {
                if let Some(n) = self.rt.tables.remove(&id) {
                    self.graph.release_node(&mut self.env, n)?;
                }
            }
            ObjectKind::MaterializedView(_) => {
                if let Some(v) = self.rt.views.remove(&id) {
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
                // (the engine thread does not wait for the source thread;
                // dropping the slot retries until its session let go of it)
                if let Some(h) = self.source_handles.remove(&id) {
                    let _ = h.stop();
                }
                // recorded also when replaying: the drop may not have
                // succeeded before the restart (it is retried after it)
                crate::source::drop_slot_async(&self.shared, def.conninfo.clone(), def.slot.clone(), !self.replaying);
                self.source_states.remove(&id);
                // a dropped source no longer holds back reads waiting for an LSN
                self.source_lsns.remove(&id);
                self.shared.source_lsn.lock().remove(&id);
            }
            ObjectKind::Sink(_) => {
                // (the engine thread does not wait for the sink thread)
                if let Some(h) = self.sink_handles.remove(&id) {
                    let _ = h.stop();
                }
                self.forget_sink_snapshot(id);
            }
            ObjectKind::View(_) => {}
        }
        Ok(())
    }

    /// Upstream schema change: rebuild the table's index in the new shape,
    /// re-plan dependent views and swap them at `gsn`. The new version is
    /// filled from the old one's rows and hydrated after the record, in the
    /// background (`hydrate::Rebuilt`); the table's sinks start over then.
    pub(crate) fn apply_schema_change(&mut self, gsn: Gsn, table: ObjectId, columns: Vec<Column>, pk: Vec<usize>) -> Result<()> {
        let Some(obj) = self.catalog.get(table).cloned() else { return Ok(()) };
        let ObjectKind::Table(mut def) = obj.kind.clone() else { return Ok(()) };
        if def.columns == columns && def.pk == pk {
            return Ok(());
        }
        let old_node = self.rt.tables[&table];
        let old_columns = def.columns.clone();
        // map old columns to new by name
        let mapping: Vec<Option<usize>> = columns.iter().map(|c| def.columns.iter().position(|o| o.name == c.name)).collect();
        def.columns = columns.clone();
        def.pk = pk;
        def.version += 1;
        let new_node = self.add_table_node(table, &def);
        self.catalog.insert(CatalogObject { kind: ObjectKind::Table(def.clone()), ..obj.clone() });
        // indexes on the table move to the new version
        let old_index_arrs = self.move_indexes(table, &format!("table {}", obj.name), &old_columns, &columns);
        // subscribers to the table were promised the old columns (and
        // queries over it would go on reading the old version)
        for s in self.subs.iter() {
            if s.reads.contains(&table) {
                let _ = s.tx.send(SubEvent::Error(SqlError::new(
                    codes::OBJECT_NOT_IN_PREREQUISITE_STATE,
                    format!("table \"{}\" changed its columns upstream", obj.name),
                )));
                s.cancel.store(true, std::sync::atomic::Ordering::Release);
            }
        }
        // sinks from the table start over with the new columns, once it is
        // filled: they get nothing until then
        let sinks: Vec<ObjectId> = self.sink_handles.iter().filter(|(_, h)| h.node == old_node).map(|(id, _)| *id).collect();
        self.held_sinks.extend(sinks.iter().copied());
        self.replan_dependents(gsn, table)?;
        self.record_then.push(hydrate::Then::Rebuilt(Box::new(hydrate::Rebuilt {
            old_node,
            new_node,
            mapping,
            columns,
            pk: def.pk,
            old_index_arrs,
            sinks,
        })));
        Ok(())
    }

    /// Restart sinks from their relation's current version.
    pub(crate) fn restart_sinks(&mut self, sinks: Vec<ObjectId>) -> Result<()> {
        for id in sinks {
            if let Some(h) = self.sink_handles.remove(&id) {
                let _ = h.stop();
            }
            self.forget_sink_snapshot(id);
            if let (Some(o), Some(handle)) = (self.catalog.get(id).cloned(), self.handle.clone())
                && let ObjectKind::Sink(sd) = &o.kind
            {
                self.start_sink_or_mark_broken(id, &o.name, sd, &handle);
            }
        }
        Ok(())
    }

    pub(crate) fn start_sink(&mut self, id: ObjectId, name: &str, def: &SinkDef, _handle: &EngineHandle) -> Result<()> {
        let node = match self.rt.views.get(&def.from).map(|v| v.node).or_else(|| self.rt.tables.get(&def.from).copied()) {
            Some(n) => n,
            None => return Err(self.broken_error(def.from).unwrap_or_else(|| SqlError::internal("sink source without dataflow"))),
        };
        let columns = match self.catalog.get(def.from).map(|o| &o.kind) {
            Some(ObjectKind::MaterializedView(v)) => v.columns.clone(),
            Some(ObjectKind::Table(t)) => t.columns.clone(),
            _ => vec![],
        };
        // (a large snapshot is taken off the engine thread: the sink starts then)
        let then = super::reader::AfterSnapshot::Sink { id, name: name.to_string(), def: def.clone(), columns };
        if let Some((snapshot, then)) = self.snapshot_or_defer(node, then)? {
            self.after_snapshot(then, self.processed, snapshot, Vec::new())?;
        }
        Ok(())
    }
}

/// Same column names and types (as a sink's target table has them)?
fn same_columns(a: &[Column], b: &[Column]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.name == y.name && x.ty.ty == y.ty.ty && x.ty.typmod == y.ty.typmod)
}

/// Index key columns (positions into `old`) at the positions of the same
/// columns, by name, in `new`; None if one of them is gone.
fn remap_index(key: &[usize], old: &[Column], new: &[Column]) -> Option<Vec<usize>> {
    key.iter().map(|&k| old.get(k).and_then(|c| new.iter().position(|n| n.name == c.name))).collect()
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

/// Reject reads of the catalog in maintained views: system relations,
/// regclass casts and functions such as pg_get_viewdef() are bound as
/// constants from the catalog of the time. A view would not follow the
/// catalog's changes (no log record changes it), and planning it again (a
/// restart does) would change it.
pub(crate) fn check_catalog_reads(bq: &crate::sql::BoundQuery) -> Result<()> {
    if let Some(what) = &bq.catalog_reads {
        return Err(SqlError::new(codes::FEATURE_NOT_SUPPORTED, format!("{what} is not allowed in a materialized view"))
            .with_detail("Maintained views must be deterministic: their state is a pure function of the input log, which does not hold the catalog's contents."));
    }
    Ok(())
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
    let (_, table) = object_name(&ct.name)?;
    let multiple_keys = || {
        Err(SqlError::new(codes::INVALID_TABLE_DEFINITION, format!("multiple primary keys for table \"{table}\" are not allowed")))
    };
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
                    // one column-level key is the whole key: a second one
                    // is a second primary key, not a composite one
                    if !pk.is_empty() {
                        return multiple_keys();
                    }
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
                return multiple_keys();
            }
            for ic in &p.columns {
                let ast::Expr::Identifier(id) = &ic.column.expr else {
                    sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unsupported primary key expression");
                };
                let n = ident(id);
                let pos = columns.iter().position(|c| c.name == n).ok_or_else(|| {
                    SqlError::new(codes::UNDEFINED_COLUMN, format!("column \"{n}\" named in key does not exist"))
                })?;
                if pk.contains(&pos) {
                    sql_bail!(codes::DUPLICATE_COLUMN, "column \"{n}\" appears twice in primary key constraint");
                }
                columns[pos].ty.nullable = false;
                pk.push(pos);
            }
        }
    }
    Ok(TableDef { columns, pk, source: None, upstream: None, version: 0 })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_slot_names() {
        assert_eq!(default_slot_name("pg", 1000, Some("3f9a1c2e")), "ivm_pg_3f9a1c2e_1000");
        // sources logged by older versions keep their names
        assert_eq!(default_slot_name("Second-Src", 1011, None), "ivm_second_src_1011");
        let long = default_slot_name(&"x".repeat(100), 1000, Some("3f9a1c2e"));
        assert_eq!(long.len(), 63);
        assert!(long.ends_with("_3f9a1c2e_1000"), "{long}");
    }

    /// CREATE SOURCE records carry the data directory's id; older ones are
    /// just the discovered tables.
    #[test]
    fn source_extra_formats() {
        let t = DiscoveredTable { schema: "public".into(), name: "t".into(), columns: vec![], pk: vec![], replica_identity_full: true };
        let old = serde_json::to_string(&vec![t.clone()]).unwrap();
        let x = parse_source_extra(&old).unwrap();
        assert_eq!((x.tables.len(), x.instance), (1, None));
        let new = serde_json::to_string(&SourceExtra { tables: vec![t], instance: Some("ab".into()) }).unwrap();
        let x = parse_source_extra(&new).unwrap();
        assert_eq!((x.tables.len(), x.instance.as_deref()), (1, Some("ab")));
        assert!(parse_source_extra("{").is_err());
    }
}
