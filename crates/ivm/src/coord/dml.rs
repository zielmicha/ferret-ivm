//! INSERT / UPDATE / DELETE on local tables. Statements are evaluated against
//! the current state and logged as row changes (not SQL), so replay is
//! deterministic.
//!
//! Several statements are in flight at once (their records share the log's
//! appends and fsyncs): a statement is evaluated against the applied state
//! as soon as it arrives, and logged, unless it read rows that a statement
//! logged before it but not applied yet writes — then its result could
//! differ from the one it gets after that statement, and it waits for it
//! (queued in arrival order, `dml_queue`) and is evaluated again. Reads and
//! writes are tracked by table and, where a statement names rows by their
//! whole primary key, by key (`Touched`). Local tables change only through
//! these records, so a statement that read nothing that pending ones write
//! sees what it would see after them: the result is that of evaluating the
//! statements one after another in log order.

use super::*;
use crate::catalog::TableDef;
use crate::dataflow::NodeKind;
use crate::expr::cast::{cast_context, cast_datum, CastContext};
use crate::expr::Expr;
use crate::sql::binder::{object_name, Binder, Ctx};
use crate::sql::parser::Statement;
use crate::sql::plan::Rel;
use crate::sql::ParamSlot;
use crate::sql_bail;
use crate::types::{Datum, Row, SqlType};
use rustc_hash::{FxHashMap, FxHashSet};
use sqlparser::ast;

/// Error code of a DML statement that read rows a pending statement writes:
/// it is queued until that one is applied (never sent to a client).
pub(crate) const WAIT: &str = "IVMDW";

/// Rows of local tables a statement read or writes: per table, all of them
/// (None) or those with these primary keys (canonical key encodings); `any`
/// for a read of a materialized view, which depends on every table.
#[derive(Default, Debug)]
pub(crate) struct Touched {
    tables: FxHashMap<ObjectId, Option<FxHashSet<Vec<u8>>>>,
    any: bool,
}

impl Touched {
    fn add_key(&mut self, table: ObjectId, key: Vec<u8>) {
        if let Some(keys) = self.tables.entry(table).or_insert_with(|| Some(FxHashSet::default())) {
            keys.insert(key);
        }
    }

    fn add_all(&mut self, table: ObjectId) {
        self.tables.insert(table, None);
    }

    /// Do reads `self` see any row that writes `w` change?
    fn overlaps(&self, w: &Touched) -> bool {
        if self.any && !w.tables.is_empty() {
            return true;
        }
        self.tables.iter().any(|(t, mine)| match (mine, w.tables.get(t)) {
            (_, None) => false,
            (None, Some(_)) | (Some(_), Some(None)) => true,
            (Some(a), Some(Some(b))) => a.iter().any(|k| b.contains(k)),
        })
    }
}

/// Canonical encoding of primary key values (as lookups by key use them).
fn key_bytes(key: &[Datum]) -> Vec<u8> {
    let mut b = Vec::new();
    for k in key {
        crate::types::encoding::encode_key(k, Default::default(), &mut b);
    }
    b
}

/// Does a literal compared with a column of type `t` encode as that
/// column's values do? (As for index ranges of reads.)
fn literal_of_type(d: &Datum, t: &SqlType) -> bool {
    matches!(
        (d, t),
        (Datum::Int2(_), SqlType::Int2)
            | (Datum::Int4(_), SqlType::Int4)
            | (Datum::Int8(_), SqlType::Int8)
            | (Datum::Text(_), SqlType::Text | SqlType::Varchar | SqlType::Bpchar | SqlType::Name)
            | (Datum::Numeric(_), SqlType::Numeric)
            | (Datum::Date(_), SqlType::Date)
            | (Datum::Timestamp(_), SqlType::Timestamp)
            | (Datum::TimestampTz(_), SqlType::TimestampTz)
            | (Datum::Uuid(_), SqlType::Uuid)
            | (Datum::Bool(_), SqlType::Bool)
    )
}

impl Engine {
    pub(crate) fn is_dml(&self, stmt: &Statement) -> bool {
        matches!(stmt, Statement::Sql(s) if matches!(**s, ast::Statement::Insert(_) | ast::Statement::Update(_) | ast::Statement::Delete(_)))
    }

    fn local_table(&self, name: &ast::ObjectName, verb: &str) -> Result<(ObjectId, String, TableDef)> {
        let (_, tname) = object_name(name)?;
        let Some(obj) = self.catalog.by_name(&tname) else {
            sql_bail!(codes::UNDEFINED_TABLE, "relation \"{tname}\" does not exist");
        };
        match &obj.kind {
            ObjectKind::Table(t) if t.source.is_none() => Ok((obj.id, tname, t.clone())),
            ObjectKind::Table(_) => {
                sql_bail!(codes::INSUFFICIENT_PRIVILEGE, "cannot {verb} source table \"{tname}\": it is replicated from its source")
            }
            ObjectKind::View(_) | ObjectKind::MaterializedView(_) => {
                sql_bail!(codes::WRONG_OBJECT_TYPE, "cannot {verb} view \"{tname}\"")
            }
            _ => sql_bail!(codes::WRONG_OBJECT_TYPE, "\"{tname}\" is not a table"),
        }
    }

    /// Current row stored under a primary key.
    fn stored_row(&mut self, table: ObjectId, key: &[Datum]) -> Result<Option<Row>> {
        if let Some(r) = self.dml_reads.as_mut() {
            r.add_key(table, key_bytes(key));
        }
        let node = self.rt.tables[&table];
        let NodeKind::Table { primary, .. } = self.graph.node(node).kind else { unreachable!() };
        let rows = self.graph.lookup(&mut self.env, primary, key)?;
        Ok(rows.into_iter().find(|(_, w)| *w > 0).map(|(r, _)| r))
    }

    fn check_row(&self, tname: &str, def: &TableDef, row: &Row) -> Result<()> {
        for (c, v) in def.columns.iter().zip(row) {
            if !c.ty.nullable && v.is_null() {
                return Err(SqlError::new(
                    codes::NOT_NULL_VIOLATION,
                    format!("null value in column \"{}\" of relation \"{}\" violates not-null constraint", c.name, tname),
                ));
            }
        }
        Ok(())
    }

    fn dup_key_error(tname: &str, def: &TableDef, key: &[Datum]) -> SqlError {
        let cols = def.pk.iter().map(|c| def.columns[*c].name.clone()).collect::<Vec<_>>().join(", ");
        let vals = key.iter().map(crate::types::text::format_datum).collect::<Vec<_>>().join(", ");
        SqlError::new(codes::UNIQUE_VIOLATION, format!("duplicate key value violates unique constraint \"{tname}_pkey\""))
            .with_detail(format!("Key ({cols})=({vals}) already exists."))
    }

    fn coerce_value(&self, v: Datum, from: &crate::types::SqlType, col: &crate::types::Column) -> Result<Datum> {
        if v.is_null() {
            return Ok(Datum::Null);
        }
        if *from != col.ty.ty && *from != crate::types::SqlType::Unknown && cast_context(from, &col.ty.ty).is_none() {
            return Err(SqlError::new(
                codes::DATATYPE_MISMATCH,
                format!("column \"{}\" is of type {} but expression is of type {}", col.name, col.ty.ty, from),
            ));
        }
        cast_datum(&v, &col.ty.ty, col.ty.typmod, false)
    }

    pub(crate) fn execute_dml(&mut self, stmt: &ast::Statement, params: Vec<ParamSlot>, reply: Sender<Result<ExecResult>>) -> Result<Option<ExecResult>> {
        self.dml_reads = Some(Touched::default());
        let r = match stmt {
            ast::Statement::Insert(ins) => self.plan_insert(ins, params),
            ast::Statement::Update(upd) => self.plan_update(upd, params),
            ast::Statement::Delete(del) => self.plan_delete(del, params),
            _ => Err(SqlError::new(codes::INTERNAL_ERROR, "not DML")),
        };
        let reads = self.dml_reads.take().unwrap_or_default();
        // (an error, or no change, could be a consequence of the stale rows too)
        if self.dml_pending.iter().any(|(_, w)| reads.overlaps(w)) {
            return Err(SqlError::new(WAIT, "it reads rows a statement not applied yet writes"));
        }
        let (changes, tag) = r?;
        if changes.is_empty() {
            return Ok(Some(ExecResult::Command { tag }));
        }
        let req = self.alloc_req();
        self.dml_counts.insert(req, tag);
        let writes = self.writes_of(&changes);
        self.dml_pending.push((req, writes));
        self.submit(LogRecord::Commit { source: 0, lsn: req, changes }, req, reply);
        Ok(None)
    }

    /// The rows `changes` write.
    fn writes_of(&self, changes: &[(ObjectId, TableChange)]) -> Touched {
        let mut w = Touched::default();
        for (t, ch) in changes {
            let pk = match self.catalog.get(*t).map(|o| &o.kind) {
                Some(ObjectKind::Table(def)) if !def.pk.is_empty() => &def.pk,
                _ => {
                    w.add_all(*t);
                    continue;
                }
            };
            match ch {
                TableChange::Upsert { row, .. } => w.add_key(*t, key_bytes(&pk.iter().map(|c| row[*c].clone()).collect::<Vec<_>>())),
                TableChange::Delete { key } => w.add_key(*t, key_bytes(key)),
                _ => w.add_all(*t),
            }
        }
        w
    }

    /// Record the rows a plan executed for a DML statement reads: a table
    /// filtered on its whole primary key by constants reads that row, any
    /// other table all of it, a materialized view everything.
    pub(crate) fn record_dml_reads(&mut self, rel: &Rel) {
        fn walk(e: &Engine, rel: &Rel, out: &mut Touched) {
            match rel {
                Rel::Get { id, .. } => match e.catalog.get(*id).map(|o| &o.kind) {
                    Some(ObjectKind::Table(_)) => out.add_all(*id),
                    _ => out.any = true,
                },
                Rel::Filter { input, preds } => match &**input {
                    Rel::Get { id, .. } if let Some(key) = e.pk_point(*id, preds) => out.add_key(*id, key),
                    _ => walk(e, input, out),
                },
                _ => {
                    for i in rel.inputs() {
                        walk(e, i, out);
                    }
                }
            }
        }
        let Some(mut out) = self.dml_reads.take() else { return };
        walk(self, rel, &mut out);
        self.dml_reads = Some(out);
    }

    /// The primary key of the one row of local table `table` that `preds`
    /// (a conjunction) can select, when they equate every key column with a
    /// constant.
    fn pk_point(&self, table: ObjectId, preds: &[Expr]) -> Option<Vec<u8>> {
        let Some(ObjectKind::Table(def)) = self.catalog.get(table).map(|o| &o.kind) else { return None };
        if def.pk.is_empty() {
            return None;
        }
        let mut conj: Vec<&Expr> = Vec::new();
        fn flatten<'e>(e: &'e Expr, out: &mut Vec<&'e Expr>) {
            match e {
                Expr::And(v) => v.iter().for_each(|x| flatten(x, out)),
                e => out.push(e),
            }
        }
        preds.iter().for_each(|p| flatten(p, &mut conj));
        let mut key = Vec::with_capacity(def.pk.len());
        for &c in &def.pk {
            let ty = &def.columns[c].ty.ty;
            let v = conj.iter().find_map(|p| {
                let Expr::Call(call) = p else { return None };
                if call.args.len() != 2 || crate::expr::func::func_def(call.func).name != "=" {
                    return None;
                }
                match (&call.args[0], &call.args[1]) {
                    (Expr::Column(x), Expr::Literal(d, _)) | (Expr::Literal(d, _), Expr::Column(x)) if *x == c && literal_of_type(d, ty) => Some(d.clone()),
                    _ => None,
                }
            })?;
            key.push(v);
        }
        Some(key_bytes(&key))
    }

    fn plan_insert(&mut self, ins: &ast::Insert, params: Vec<ParamSlot>) -> Result<(Vec<(ObjectId, TableChange)>, String)> {
        if ins.returning.is_some() {
            sql_bail!(codes::FEATURE_NOT_SUPPORTED, "RETURNING is not supported");
        }
        let ast::TableObject::TableName(name) = &ins.table else {
            sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unsupported INSERT target");
        };
        let (id, tname, def) = self.local_table(name, "insert into")?;
        let ncols = def.columns.len();
        let target: Vec<usize> = if ins.columns.is_empty() {
            (0..ncols).collect()
        } else {
            let mut v = Vec::new();
            for c in &ins.columns {
                let (_, cn) = object_name(c)?;
                let pos = def.columns.iter().position(|x| x.name == cn).ok_or_else(|| {
                    SqlError::new(codes::UNDEFINED_COLUMN, format!("column \"{cn}\" of relation \"{tname}\" does not exist"))
                })?;
                v.push(pos);
            }
            v
        };
        let Some(source) = &ins.source else { sql_bail!(codes::FEATURE_NOT_SUPPORTED, "INSERT requires VALUES or a query") };
        // rows of (value, source type)
        let mut rows: Vec<Vec<(Datum, crate::types::SqlType)>> = Vec::new();
        let now = statement_time();
        let plain_values = match &*source.body {
            ast::SetExpr::Values(v) if source.order_by.is_none() && source.limit_clause.is_none() => Some(v),
            _ => None,
        };
        if let Some(values) = plain_values {
            // VALUES rows are coerced to the target column types directly
            let scope = crate::sql::scope::Scope::default();
            let mut b = Binder::new(&self.catalog).with_params(params);
            for r in &values.rows {
                if r.content.len() > target.len() {
                    sql_bail!(codes::SYNTAX_ERROR, "INSERT has more expressions than target columns");
                }
                if r.content.len() < target.len() {
                    sql_bail!(codes::SYNTAX_ERROR, "INSERT has more target columns than expressions");
                }
                let mut row = Vec::with_capacity(target.len());
                for (k, e) in r.content.iter().enumerate() {
                    if let ast::Expr::Identifier(i) = e
                        && i.value.eq_ignore_ascii_case("default")
                        && i.quote_style.is_none()
                    {
                        row.push((Datum::Null, crate::types::SqlType::Unknown));
                        continue;
                    }
                    let ex = b.bind_expr(e, &Ctx::plain(&scope, "VALUES"))?;
                    let from = ex.ty(&[]);
                    let col = &def.columns[target[k]];
                    let ex = if from == crate::types::SqlType::Unknown {
                        b.coerce(ex, &from, &col.ty.ty, CastContext::Explicit, "INSERT")?
                    } else {
                        ex
                    };
                    let ty = ex.ty(&[]);
                    row.push((ex.bind_now(now).fold()?.eval(&[])?, ty));
                }
                rows.push(row);
            }
        } else {
            let bq = crate::sql::plan_query(&self.catalog, source, params)?;
            let types: Vec<crate::types::SqlType> = bq.column_types().into_iter().map(|c| c.ty).collect();
            if types.len() != target.len() {
                sql_bail!(
                    codes::SYNTAX_ERROR,
                    "INSERT has more {} than {}",
                    if types.len() > target.len() { "expressions" } else { "target columns" },
                    if types.len() > target.len() { "target columns" } else { "expressions" }
                );
            }
            for (r, w) in self.execute_rel(&bq.into_rel().bind_now(now))? {
                for _ in 0..w.max(0) {
                    rows.push(r.iter().cloned().zip(types.iter().cloned()).collect());
                }
            }
        }
        let mut changes = Vec::with_capacity(rows.len());
        let mut seen: rustc_hash::FxHashSet<Vec<u8>> = Default::default();
        let do_nothing = matches!(&ins.on, Some(ast::OnInsert::OnConflict(oc)) if matches!(oc.action, ast::OnConflictAction::DoNothing));
        if ins.on.is_some() && !do_nothing {
            sql_bail!(codes::FEATURE_NOT_SUPPORTED, "only ON CONFLICT DO NOTHING is supported");
        }
        let mut n = 0;
        for vals in rows {
            let mut full: Row = vec![Datum::Null; ncols];
            for ((v, from), t) in vals.into_iter().zip(&target) {
                full[*t] = self.coerce_value(v, &from, &def.columns[*t])?;
            }
            self.check_row(&tname, &def, &full)?;
            if def.pk.is_empty() {
                changes.push((id, TableChange::Insert { row: full }));
            } else {
                let key: Vec<Datum> = def.pk.iter().map(|c| full[*c].clone()).collect();
                let kb = {
                    let mut b = Vec::new();
                    for k in &key {
                        crate::types::encoding::encode_key(k, Default::default(), &mut b);
                    }
                    b
                };
                if !seen.insert(kb) || self.stored_row(id, &key)?.is_some() {
                    if do_nothing {
                        continue;
                    }
                    return Err(Self::dup_key_error(&tname, &def, &key));
                }
                changes.push((id, TableChange::Upsert { row: full, unchanged: vec![] }));
            }
            n += 1;
        }
        Ok((changes, format!("INSERT 0 {n}")))
    }

    /// Bind `SELECT t.*, <exprs> FROM t WHERE pred` for UPDATE/DELETE.
    fn select_rows(
        &mut self,
        relation: &ast::TableFactor,
        selection: Option<&ast::Expr>,
        exprs: &[(usize, &ast::Expr)],
        params: Vec<ParamSlot>,
        def: &TableDef,
    ) -> Result<Vec<(Row, i64)>> {
        let ast::TableFactor::Table { name, alias, .. } = relation else {
            sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unsupported DML target");
        };
        let (schema, tname) = object_name(name)?;
        let mut b = Binder::new(&self.catalog).with_params(params);
        let (rel, scope) = b.bind_named_relation(schema.as_deref(), &tname, alias.as_ref())?;
        let mut rel = rel;
        if let Some(sel) = selection {
            let p = b.bind_expr(sel, &Ctx::plain(&scope, "WHERE"))?;
            let p = b.coerce_bool(p, "WHERE")?;
            rel = rel.filter(p.conjuncts());
        }
        let mut new_exprs = Vec::new();
        for (col, e) in exprs {
            let ex = b.bind_expr(e, &Ctx::plain(&scope, "UPDATE"))?;
            let from = ex.ty(&scope.types());
            let c = &def.columns[*col];
            let ex = if from == c.ty.ty {
                ex
            } else {
                b.coerce(ex, &from, &c.ty.ty, CastContext::Assignment, &format!("column \"{}\"", c.name))?
            };
            new_exprs.push(ex);
        }
        let rel = rel.map(new_exprs);
        let rel = crate::sql::decorrelate::decorrelate(rel)?;
        self.execute_rel(&rel.bind_now(statement_time()))
    }

    fn plan_update(&mut self, upd: &ast::Update, params: Vec<ParamSlot>) -> Result<(Vec<(ObjectId, TableChange)>, String)> {
        if upd.returning.is_some() || upd.from.is_some() {
            sql_bail!(codes::FEATURE_NOT_SUPPORTED, "UPDATE ... FROM / RETURNING is not supported");
        }
        let ast::TableFactor::Table { name, .. } = &upd.table.relation else {
            sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unsupported UPDATE target");
        };
        let (id, tname, def) = self.local_table(name, "update")?;
        let mut sets: Vec<(usize, &ast::Expr)> = Vec::new();
        for a in &upd.assignments {
            let ast::AssignmentTarget::ColumnName(cn) = &a.target else {
                sql_bail!(codes::FEATURE_NOT_SUPPORTED, "multi-column assignment is not supported");
            };
            let (_, col) = object_name(cn)?;
            let pos = def.columns.iter().position(|c| c.name == col).ok_or_else(|| {
                SqlError::new(codes::UNDEFINED_COLUMN, format!("column \"{col}\" of relation \"{tname}\" does not exist"))
            })?;
            sets.push((pos, &a.value));
        }
        let rows = self.select_rows(&upd.table.relation, upd.selection.as_ref(), &sets, params, &def)?;
        let ncols = def.columns.len();
        let mut changes = Vec::new();
        let mut n = 0;
        let mut new_keys: rustc_hash::FxHashMap<Vec<u8>, ()> = Default::default();
        let mut old_keys: rustc_hash::FxHashMap<Vec<u8>, ()> = Default::default();
        let enc = |key: &[Datum]| {
            let mut b = Vec::new();
            for k in key {
                crate::types::encoding::encode_key(k, Default::default(), &mut b);
            }
            b
        };
        let mut pending = Vec::new();
        for (r, w) in rows {
            let old: Row = r[..ncols].to_vec();
            let mut new = old.clone();
            for (k, (col, _)) in sets.iter().enumerate() {
                let v = r[ncols + k].clone();
                let c = &def.columns[*col];
                new[*col] = cast_datum(&v, &c.ty.ty, c.ty.typmod, false)?;
            }
            self.check_row(&tname, &def, &new)?;
            for _ in 0..w.max(1) {
                n += 1;
            }
            if !def.pk.is_empty() {
                let ok: Vec<Datum> = def.pk.iter().map(|c| old[*c].clone()).collect();
                old_keys.insert(enc(&ok), ());
            }
            pending.push((old, new, w));
        }
        for (old, new, w) in pending {
            if def.pk.is_empty() {
                for _ in 0..w.max(0) {
                    changes.push((id, TableChange::Remove { row: old.clone() }));
                    changes.push((id, TableChange::Insert { row: new.clone() }));
                }
                continue;
            }
            let ok: Vec<Datum> = def.pk.iter().map(|c| old[*c].clone()).collect();
            let nk: Vec<Datum> = def.pk.iter().map(|c| new[*c].clone()).collect();
            let nkb = enc(&nk);
            if new_keys.insert(nkb.clone(), ()).is_some() {
                return Err(Self::dup_key_error(&tname, &def, &nk));
            }
            if enc(&ok) != nkb {
                // key change: the new key must be free (or vacated by this update)
                if !old_keys.contains_key(&nkb) && self.stored_row(id, &nk)?.is_some() {
                    return Err(Self::dup_key_error(&tname, &def, &nk));
                }
                changes.push((id, TableChange::Delete { key: ok }));
            }
            changes.push((id, TableChange::Upsert { row: new, unchanged: vec![] }));
        }
        // deletes of vacated keys must precede upserts that reuse them
        changes.sort_by_key(|(_, c)| !matches!(c, TableChange::Delete { .. }));
        Ok((changes, format!("UPDATE {n}")))
    }

    fn plan_delete(&mut self, del: &ast::Delete, params: Vec<ParamSlot>) -> Result<(Vec<(ObjectId, TableChange)>, String)> {
        if del.returning.is_some() || del.using.is_some() {
            sql_bail!(codes::FEATURE_NOT_SUPPORTED, "DELETE ... USING / RETURNING is not supported");
        }
        let tables = match &del.from {
            ast::FromTable::WithFromKeyword(t) | ast::FromTable::WithoutKeyword(t) => t,
        };
        if tables.len() != 1 || !tables[0].joins.is_empty() {
            sql_bail!(codes::FEATURE_NOT_SUPPORTED, "DELETE from multiple tables is not supported");
        }
        let ast::TableFactor::Table { name, .. } = &tables[0].relation else {
            sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unsupported DELETE target");
        };
        let (id, _tname, def) = self.local_table(name, "delete from")?;
        let rows = self.select_rows(&tables[0].relation, del.selection.as_ref(), &[], params, &def)?;
        let mut changes = Vec::new();
        let mut n = 0;
        for (r, w) in rows {
            if def.pk.is_empty() {
                for _ in 0..w.max(0) {
                    changes.push((id, TableChange::Remove { row: r.clone() }));
                    n += 1;
                }
            } else {
                let key: Vec<Datum> = def.pk.iter().map(|c| r[*c].clone()).collect();
                changes.push((id, TableChange::Delete { key }));
                n += 1;
            }
        }
        Ok((changes, format!("DELETE {n}")))
    }
}

#[allow(dead_code)]
fn _unused(_: &Rel, _: &Expr) {}

/// now() in DML: the statement's start time (the resulting values are
/// logged, so replay does not depend on the clock).
fn statement_time() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_micros() as i64).unwrap_or(0) - 946_684_800_000_000
}
