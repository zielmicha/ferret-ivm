//! Binder: SQL AST -> logical plan (`Rel`), with name resolution, type
//! checking and PostgreSQL coercion rules.

use super::plan::*;
use super::scope::{Scope, ScopeItem};
use crate::error::{codes, Result, SqlError};
use crate::expr::Expr;
use crate::sql_bail;
use crate::types::encoding::SortSpec;
use crate::types::{Column, ColumnType, Datum, SqlType};
use sqlparser::ast;
use std::cell::RefCell;

/// What the binder needs from the catalog.
pub trait Catalog {
    fn resolve_relation(&self, schema: Option<&str>, name: &str) -> Option<CatalogRelation>;
    /// OID -> text mappings for catalog functions and reg* casts (see sysrel).
    fn definitions(&self, kind: &str) -> Vec<(u32, String)> {
        let _ = kind;
        vec![]
    }
}

#[derive(Clone, Debug)]
pub enum CatalogRelation {
    /// Anything backed by a dataflow collection (base table, source table,
    /// materialized view, system relation).
    Table { id: ObjectId, name: String, columns: Vec<Column>, keys: Vec<Vec<usize>> },
    /// Virtual view, expanded inline.
    View { id: ObjectId, name: String, columns: Vec<Column>, sql: String },
    /// Constant rows (system catalogs, computed when the query is bound).
    Constant { name: String, columns: Vec<Column>, rows: Vec<crate::types::Row> },
}

/// A bound query with presentation details.
#[derive(Clone, Debug)]
pub struct BoundQuery {
    pub rel: Rel,
    pub names: Vec<String>,
    /// The first `visible` columns are the result; the rest are sort helpers.
    pub visible: usize,
    pub order: Vec<SortKey>,
    pub limit: Option<u64>,
    pub offset: u64,
    /// FETCH ... WITH TIES: rows tied with the last one by ORDER BY are kept.
    pub with_ties: bool,
}

impl BoundQuery {
    /// The query as a relation (ORDER BY only matters together with LIMIT).
    pub fn into_rel(self) -> Rel {
        let mut rel = self.rel;
        if self.limit.is_some() || self.offset > 0 {
            rel = Rel::TopK {
                input: Box::new(rel),
                group: vec![],
                order: self.order,
                limit: self.limit,
                offset: self.offset,
                with_ties: self.with_ties,
            };
        }
        rel.project((0..self.visible).collect())
    }

    pub fn column_types(&self) -> Vec<ColumnType> {
        self.rel.schema().into_iter().take(self.visible).collect()
    }

    /// Resolve untyped (unknown) result columns as text, as PostgreSQL does
    /// for the outputs of a query, subquery, CTE or view (but not for the
    /// branches of a set operation, which unify their types first).
    pub fn resolve_unknowns(mut self, b: &mut Binder) -> Result<BoundQuery> {
        let schema = self.rel.schema();
        if !schema.iter().take(self.visible).any(|c| c.ty == SqlType::Unknown) {
            return Ok(self);
        }
        let target: Vec<ColumnType> = schema
            .iter()
            .enumerate()
            .map(|(i, c)| if i < self.visible && c.ty == SqlType::Unknown { ColumnType::new(SqlType::Text).nullable(c.nullable) } else { c.clone() })
            .collect();
        self.rel = b.coerce_rel(self.rel, &target, "SELECT")?;
        Ok(self)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CteBinding {
    pub rel: Rel,
    pub names: Vec<String>,
}

/// Parameter ($n) slot: type and, when executing, its value.
#[derive(Clone, Debug)]
pub struct ParamSlot {
    pub ty: SqlType,
    pub value: Option<Datum>,
}

pub(crate) struct PendingAgg {
    pub func: AggFunc,
    pub args: Vec<Expr>,
    pub distinct: bool,
    pub filter: Option<Expr>,
    pub order: Vec<(Expr, SortSpec)>,
    pub fraction: Option<f64>,
    pub ret: ColumnType,
    pub fp: Vec<u8>,
}

pub(crate) struct PendingWindow {
    pub func: WindowFunc,
    pub args: Vec<Expr>,
    pub partition: Vec<Expr>,
    pub order: Vec<(Expr, SortSpec)>,
    pub frame: Frame,
    pub ret: ColumnType,
    pub range: Option<RangeOffsets>,
}

/// A set-returning function in a select list.
pub(crate) struct PendingSrf {
    pub func: TableFunc,
    pub args: Vec<Expr>,
    pub out: ColumnType,
    pub fp: Vec<u8>,
}

/// Collects aggregates, window functions and select-list set-returning
/// functions found while binding a SELECT.
#[derive(Default)]
pub(crate) struct Collector {
    pub aggs: Vec<PendingAgg>,
    pub windows: Vec<PendingWindow>,
    pub srfs: Vec<PendingSrf>,
}

/// Expression binding context.
pub(crate) struct Ctx<'s> {
    pub scope: &'s Scope,
    pub collector: Option<&'s RefCell<Collector>>,
    pub in_agg: bool,
    pub clause: &'static str,
}

impl<'s> Ctx<'s> {
    pub fn plain(scope: &'s Scope, clause: &'static str) -> Ctx<'s> {
        Ctx { scope, collector: None, in_agg: false, clause }
    }
}

pub struct Binder<'a> {
    pub catalog: &'a dyn Catalog,
    pub(crate) ctes: Vec<Vec<(String, CteBinding)>>,
    /// Scopes of enclosing queries (innermost last) for correlated refs.
    pub(crate) outer: Vec<Scope>,
    pub params: Vec<ParamSlot>,
    pub(crate) next_rec_id: u64,
    view_depth: usize,
    /// WINDOW clause of the SELECT being bound.
    pub(crate) named_windows: Vec<ast::NamedWindowDefinition>,
}

pub(crate) fn ident(i: &ast::Ident) -> String {
    match i.quote_style {
        Some(_) => i.value.clone(),
        None => i.value.to_lowercase(),
    }
}

/// Split an object name into (schema, name).
pub fn object_name(n: &ast::ObjectName) -> Result<(Option<String>, String)> {
    let parts: Vec<String> = n
        .0
        .iter()
        .map(|p| p.as_ident().map(ident).ok_or_else(|| SqlError::syntax(format!("unsupported name {n}"))))
        .collect::<Result<_>>()?;
    match parts.len() {
        1 => Ok((None, parts[0].clone())),
        2 => Ok((Some(parts[0].clone()), parts[1].clone())),
        3 => Ok((Some(parts[1].clone()), parts[2].clone())),
        _ => Err(SqlError::syntax(format!("improper qualified name (too many dotted names): {n}"))),
    }
}

impl<'a> Binder<'a> {
    pub fn new(catalog: &'a dyn Catalog) -> Binder<'a> {
        Binder {
            catalog,
            ctes: Vec::new(),
            outer: Vec::new(),
            params: Vec::new(),
            next_rec_id: 1,
            view_depth: 0,
            named_windows: Vec::new(),
        }
    }

    pub fn with_params(mut self, params: Vec<ParamSlot>) -> Self {
        self.params = params;
        self
    }

    // ------------------------------------------------------------------ queries

    pub fn bind_query(&mut self, q: &ast::Query) -> Result<BoundQuery> {
        if q.fetch.is_some() && q.limit_clause.is_some() {
            sql_bail!(codes::SYNTAX_ERROR, "cannot use both LIMIT and FETCH");
        }
        if !q.locks.is_empty() {
            sql_bail!(codes::FEATURE_NOT_SUPPORTED, "FOR UPDATE/SHARE is not supported");
        }
        let pushed = match &q.with {
            Some(w) => {
                self.bind_with(w)?;
                true
            }
            None => false,
        };
        let r = self.bind_query_body(q);
        if pushed {
            self.ctes.pop();
        }
        r
    }

    fn bind_with(&mut self, w: &ast::With) -> Result<()> {
        self.ctes.push(Vec::new());
        for cte in &w.cte_tables {
            let name = ident(&cte.alias.name);
            let alias_cols: Vec<String> = cte.alias.columns.iter().map(|c| ident(&c.name)).collect();
            let binding = if w.recursive && query_references(&cte.query, &name) {
                self.bind_recursive_cte(&name, &alias_cols, &cte.query)?
            } else {
                let bq = self.bind_query(&cte.query)?.resolve_unknowns(self)?;
                let mut names = bq.names.clone();
                names.truncate(bq.visible);
                apply_alias_names(&mut names, &alias_cols, &name)?;
                CteBinding { rel: bq.into_rel(), names }
            };
            self.ctes.last_mut().unwrap().push((name, binding));
        }
        Ok(())
    }

    fn bind_recursive_cte(&mut self, name: &str, alias_cols: &[String], q: &ast::Query) -> Result<CteBinding> {
        let ast::SetExpr::SetOperation { left, op: ast::SetOperator::Union, set_quantifier, right } = &*q.body else {
            sql_bail!(
                codes::INVALID_RECURSION,
                "recursive query \"{name}\" does not have the form non-recursive-term UNION [ALL] recursive-term"
            );
        };
        let distinct = !matches!(set_quantifier, ast::SetQuantifier::All);
        let (base, mut names) = self.bind_set_expr(left)?;
        apply_alias_names(&mut names, alias_cols, name)?;
        let id = self.next_rec_id;
        self.next_rec_id += 1;
        let mut columns = base.schema();
        for c in columns.iter_mut() {
            c.nullable = true;
        }
        // register the working table and bind the recursive term
        self.ctes.last_mut().unwrap().push((
            name.to_string(),
            CteBinding { rel: Rel::RecRef { id, columns: columns.clone() }, names: names.clone() },
        ));
        let step = self.bind_set_expr(right);
        self.ctes.last_mut().unwrap().pop();
        let (step, _) = step?;
        if step.arity() != base.arity() {
            sql_bail!(codes::SYNTAX_ERROR, "each UNION query must have the same number of columns");
        }
        let step = self.coerce_rel(step, &columns, "UNION")?;
        Ok(CteBinding { rel: Rel::Recursive { id, base: Box::new(base), step: Box::new(step), distinct, columns }, names })
    }

    fn bind_limit(&mut self, q: &ast::Query) -> Result<(Option<u64>, u64, bool)> {
        let eval_count = |b: &mut Binder, e: &ast::Expr, what: &'static str| -> Result<Option<u64>> {
            let scope = Scope::default();
            let ex = b.bind_expr(e, &Ctx::plain(&scope, what))?;
            let ty = ex.ty(&[]);
            let ex = b.coerce(ex, &ty, &SqlType::Int8, crate::expr::cast::CastContext::Assignment, what)?.fold()?;
            match ex {
                Expr::Literal(Datum::Null, _) => Ok(None),
                Expr::Literal(d, _) => {
                    let v = d.as_i64().unwrap_or(0);
                    if v < 0 {
                        sql_bail!(
                            "2201W",
                            "{} must not be negative",
                            if what == "LIMIT" { "LIMIT" } else { "OFFSET" }
                        );
                    }
                    Ok(Some(v as u64))
                }
                _ => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "{what} must be a constant"),
            }
        };
        let mut limit = None;
        let mut offset = 0;
        match &q.limit_clause {
            None => {}
            Some(ast::LimitClause::LimitOffset { limit: l, offset: o, limit_by }) => {
                if !limit_by.is_empty() {
                    sql_bail!(codes::FEATURE_NOT_SUPPORTED, "LIMIT BY is not supported");
                }
                if let Some(l) = l {
                    limit = eval_count(self, l, "LIMIT")?;
                }
                if let Some(o) = o {
                    offset = eval_count(self, &o.value, "OFFSET")?.unwrap_or(0);
                }
            }
            Some(ast::LimitClause::OffsetCommaLimit { offset: o, limit: l }) => {
                limit = eval_count(self, l, "LIMIT")?;
                offset = eval_count(self, o, "OFFSET")?.unwrap_or(0);
            }
        }
        let mut with_ties = false;
        if let Some(f) = &q.fetch {
            if f.percent {
                sql_bail!(codes::FEATURE_NOT_SUPPORTED, "FETCH ... PERCENT is not supported");
            }
            with_ties = f.with_ties;
            limit = match &f.quantity {
                Some(e) => eval_count(self, e, "LIMIT")?,
                None => Some(1),
            };
        }
        Ok((limit, offset, with_ties))
    }

    fn bind_query_body(&mut self, q: &ast::Query) -> Result<BoundQuery> {
        let order_by: Vec<ast::OrderByExpr> = match &q.order_by {
            None => vec![],
            Some(ob) => match &ob.kind {
                ast::OrderByKind::Expressions(v) => v.clone(),
                ast::OrderByKind::All(_) => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "ORDER BY ALL is not supported"),
            },
        };
        let (limit, offset, with_ties) = self.bind_limit(q)?;
        if with_ties && order_by.is_empty() {
            sql_bail!(codes::SYNTAX_ERROR, "WITH TIES cannot be specified without ORDER BY clause");
        }
        let mut bq = self.bind_query_body_inner(q, &order_by, limit, offset)?;
        bq.with_ties = with_ties;
        Ok(bq)
    }

    fn bind_query_body_inner(
        &mut self,
        q: &ast::Query,
        order_by: &[ast::OrderByExpr],
        limit: Option<u64>,
        offset: u64,
    ) -> Result<BoundQuery> {
        match &*q.body {
            ast::SetExpr::Select(s) => self.bind_select(s, order_by, limit, offset),
            body => {
                let (rel, names) = self.bind_set_expr(body)?;
                let scope = Scope::new(
                    names
                        .iter()
                        .zip(rel.schema())
                        .map(|(n, t)| ScopeItem { table: None, name: n.clone(), ty: t, hidden: false })
                        .collect(),
                );
                let visible = rel.arity();
                let mut rel = rel;
                let mut order = Vec::new();
                let mut extra = Vec::new();
                for ob in order_by {
                    let spec = sort_spec(&ob.options);
                    let col = match self.order_by_output_ref(&ob.expr, &names, visible, None)? {
                        Some(c) => c,
                        None => {
                            let e = self.bind_expr(&ob.expr, &Ctx::plain(&scope, "ORDER BY"))?;
                            if let Expr::Column(c) = e {
                                c
                            } else {
                                extra.push(e);
                                visible + extra.len() - 1
                            }
                        }
                    };
                    order.push(SortKey { col, spec });
                }
                rel = rel.map(extra);
                Ok(BoundQuery { rel, names, visible, order, limit, offset, with_ties: false })
            }
        }
    }

    /// ORDER BY item naming an output column (alias or ordinal). Several
    /// output columns of that name are ambiguous unless they are the same
    /// expression (`exprs`, when known).
    fn order_by_output_ref(&self, e: &ast::Expr, names: &[String], visible: usize, exprs: Option<&[Expr]>) -> Result<Option<usize>> {
        match e {
            ast::Expr::Value(v) => {
                if let ast::Value::Number(n, _) = &v.value {
                    let k: usize = n.parse().map_err(|_| SqlError::syntax("non-integer constant in ORDER BY"))?;
                    if k == 0 || k > visible {
                        sql_bail!(codes::INVALID_COLUMN_REFERENCE, "ORDER BY position {k} is not in select list");
                    }
                    return Ok(Some(k - 1));
                }
                Ok(None)
            }
            ast::Expr::Identifier(id) => {
                let name = ident(id);
                let hits: Vec<usize> = (0..visible).filter(|i| names[*i] == name).collect();
                match hits.len() {
                    0 => Ok(None),
                    1 => Ok(Some(hits[0])),
                    _ if exprs.is_some_and(|x| same_exprs(x, &hits)) => Ok(Some(hits[0])),
                    _ => sql_bail!(codes::AMBIGUOUS_COLUMN, "ORDER BY \"{name}\" is ambiguous"),
                }
            }
            _ => Ok(None),
        }
    }

    pub(crate) fn bind_set_expr(&mut self, body: &ast::SetExpr) -> Result<(Rel, Vec<String>)> {
        match body {
            ast::SetExpr::Select(s) => {
                let bq = self.bind_select(s, &[], None, 0)?;
                let mut names = bq.names.clone();
                names.truncate(bq.visible);
                Ok((bq.into_rel(), names))
            }
            ast::SetExpr::Query(q) => {
                let bq = self.bind_query(q)?;
                let mut names = bq.names.clone();
                names.truncate(bq.visible);
                Ok((bq.into_rel(), names))
            }
            ast::SetExpr::SetOperation { left, op, set_quantifier, right } => {
                let (l, names) = self.bind_set_expr(left)?;
                let (r, _) = self.bind_set_expr(right)?;
                let opname = match op {
                    ast::SetOperator::Union => "UNION",
                    ast::SetOperator::Except => "EXCEPT",
                    ast::SetOperator::Intersect => "INTERSECT",
                    _ => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "set operator {op} is not supported"),
                };
                if l.arity() != r.arity() {
                    sql_bail!(codes::SYNTAX_ERROR, "each {opname} query must have the same number of columns");
                }
                let (ls, rs) = (l.schema(), r.schema());
                let mut target = Vec::with_capacity(ls.len());
                for (a, b) in ls.iter().zip(&rs) {
                    let t = common_type(&[a.ty.clone(), b.ty.clone()], opname)?;
                    let typmod = if a.ty == t && b.ty == t && a.typmod == b.typmod { a.typmod } else { -1 };
                    target.push(ColumnType { ty: t, typmod, nullable: a.nullable || b.nullable });
                }
                let l = self.coerce_rel(l, &target, opname)?;
                let r = self.coerce_rel(r, &target, opname)?;
                let all = matches!(set_quantifier, ast::SetQuantifier::All);
                let rel = match op {
                    ast::SetOperator::Union if all => Rel::UnionAll { inputs: vec![l, r] },
                    ast::SetOperator::Union => {
                        Rel::SetOp { op: SetOpKind::Union, all: false, left: Box::new(l), right: Box::new(r) }
                    }
                    ast::SetOperator::Except => {
                        Rel::SetOp { op: SetOpKind::Except, all, left: Box::new(l), right: Box::new(r) }
                    }
                    _ => Rel::SetOp { op: SetOpKind::Intersect, all, left: Box::new(l), right: Box::new(r) },
                };
                Ok((rel, names))
            }
            ast::SetExpr::Values(v) => self.bind_values(v),
            ast::SetExpr::Table(t) => {
                let name = t.table_name.clone().ok_or_else(|| SqlError::syntax("TABLE requires a name"))?;
                let (rel, scope) = self.bind_named_relation(t.schema_name.as_deref(), &name, None)?;
                let names = scope.items.iter().filter(|i| !i.hidden).map(|i| i.name.clone()).collect();
                Ok((rel, names))
            }
            _ => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unsupported query body: {body}"),
        }
    }

    pub(crate) fn bind_values(&mut self, v: &ast::Values) -> Result<(Rel, Vec<String>)> {
        if v.rows.is_empty() {
            sql_bail!(codes::SYNTAX_ERROR, "VALUES list is empty");
        }
        let width = v.rows[0].content.len();
        let scope = Scope::default();
        let mut rows: Vec<Vec<Expr>> = Vec::with_capacity(v.rows.len());
        for r in &v.rows {
            if r.content.len() != width {
                sql_bail!(codes::SYNTAX_ERROR, "VALUES lists must all be the same length");
            }
            let mut row = Vec::with_capacity(width);
            for e in &r.content {
                row.push(self.bind_expr(e, &Ctx::plain(&scope, "VALUES"))?);
            }
            rows.push(row);
        }
        let mut columns = Vec::with_capacity(width);
        for c in 0..width {
            let types: Vec<SqlType> = rows.iter().map(|r| r[c].ty(&[])).collect();
            let t = common_type(&types, "VALUES")?;
            let nullable = rows.iter().any(|r| r[c].nullable(&[]));
            for r in rows.iter_mut() {
                let from = r[c].ty(&[]);
                let e = std::mem::replace(&mut r[c], Expr::null(SqlType::Unknown));
                r[c] = self.coerce(e, &from, &t, crate::expr::cast::CastContext::Implicit, "VALUES")?;
            }
            columns.push(ColumnType { ty: t, typmod: -1, nullable });
        }
        // correlated VALUES (rare) keeps expressions; otherwise fold
        let mut folded = Vec::with_capacity(rows.len());
        for r in rows {
            let mut fr = Vec::with_capacity(width);
            for e in r {
                fr.push(if e.has_outer() || e.has_subquery() { e } else { e.fold()? });
            }
            folded.push(fr);
        }
        let names = (1..=width).map(|i| format!("column{i}")).collect();
        Ok((Rel::Values { rows: folded, columns }, names))
    }

    /// Cast columns of `rel` to `target` types (adding a Map + Project if needed).
    pub(crate) fn coerce_rel(&mut self, rel: Rel, target: &[ColumnType], what: &str) -> Result<Rel> {
        let schema = rel.schema();
        if schema.iter().zip(target).all(|(a, b)| a.ty == b.ty) {
            return Ok(rel);
        }
        let n = schema.len();
        let mut exprs = Vec::with_capacity(n);
        for (i, (a, b)) in schema.iter().zip(target).enumerate() {
            exprs.push(self.coerce(Expr::Column(i), &a.ty, &b.ty, crate::expr::cast::CastContext::Implicit, what)?);
        }
        Ok(rel.map(exprs).project((n..2 * n).collect()))
    }

    // ------------------------------------------------------------------ SELECT

    pub(crate) fn bind_select(
        &mut self,
        s: &ast::Select,
        order_by: &[ast::OrderByExpr],
        limit: Option<u64>,
        offset: u64,
    ) -> Result<BoundQuery> {
        let saved = std::mem::replace(&mut self.named_windows, s.named_window.clone());
        let r = self.bind_select_inner(s, order_by, limit, offset);
        self.named_windows = saved;
        r
    }

    fn bind_select_inner(
        &mut self,
        s: &ast::Select,
        order_by: &[ast::OrderByExpr],
        limit: Option<u64>,
        offset: u64,
    ) -> Result<BoundQuery> {
        if s.top.is_some() || s.into.is_some() || !s.lateral_views.is_empty() || s.qualify.is_some() {
            sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unsupported SELECT clause");
        }
        let (mut rel, scope) = if s.from.is_empty() { (Rel::unit(), Scope::default()) } else { self.bind_from(&s.from)? };
        if let Some(w) = &s.selection {
            let p = self.bind_expr(w, &Ctx::plain(&scope, "WHERE"))?;
            let p = self.coerce_bool(p, "WHERE")?;
            rel = rel.filter(p.conjuncts());
        }

        let collector = RefCell::new(Collector::default());
        let ctx = Ctx { scope: &scope, collector: Some(&collector), in_agg: false, clause: "SELECT" };

        // select list
        let mut sel_exprs: Vec<Expr> = Vec::new();
        let mut names: Vec<String> = Vec::new();
        for item in &s.projection {
            match item {
                ast::SelectItem::UnnamedExpr(e) => {
                    names.push(derive_name(e));
                    sel_exprs.push(self.bind_expr(e, &ctx)?);
                }
                ast::SelectItem::ExprWithAlias { expr, alias } => {
                    names.push(ident(alias));
                    sel_exprs.push(self.bind_expr(expr, &ctx)?);
                }
                ast::SelectItem::Wildcard(_) => {
                    let cols = scope.star(None)?;
                    if cols.is_empty() && !s.from.is_empty() {
                        // SELECT * over zero columns is allowed
                    }
                    for c in cols {
                        names.push(scope.items[c].name.clone());
                        sel_exprs.push(Expr::Column(c));
                    }
                }
                ast::SelectItem::QualifiedWildcard(kind, _) => {
                    let ast::SelectItemQualifiedWildcardKind::ObjectName(on) = kind else {
                        sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unsupported wildcard");
                    };
                    let (_, t) = object_name(on)?;
                    for c in scope.star(Some(&t))? {
                        names.push(scope.items[c].name.clone());
                        sel_exprs.push(Expr::Column(c));
                    }
                }
                _ => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unsupported select item"),
            }
        }

        // GROUP BY: the distinct grouping expressions, and the grouping sets
        // over them when GROUPING SETS / ROLLUP / CUBE are used
        let mut group_exprs: Vec<Expr> = Vec::new();
        let mut grouping_sets: Option<Vec<Vec<usize>>> = None;
        match &s.group_by {
            ast::GroupByExpr::All(_) => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "GROUP BY ALL is not supported"),
            ast::GroupByExpr::Expressions(exprs, mods) => {
                if !mods.is_empty() {
                    sql_bail!(codes::FEATURE_NOT_SUPPORTED, "GROUP BY modifiers are not supported");
                }
                let bind_one = |b: &mut Self, e: &ast::Expr, group_exprs: &mut Vec<Expr>| -> Result<usize> {
                    let g = b.bind_group_expr(e, &scope, &names, &sel_exprs)?;
                    if g.contains(&|x| matches!(x, Expr::AggRef(..))) {
                        sql_bail!(codes::GROUPING_ERROR, "aggregate functions are not allowed in GROUP BY");
                    }
                    if g.contains(&|x| matches!(x, Expr::WindowRef(..))) {
                        sql_bail!(codes::WINDOWING_ERROR, "window functions are not allowed in GROUP BY");
                    }
                    let mut fp = Vec::new();
                    g.fingerprint(&mut fp);
                    if let Some(i) = group_exprs.iter().position(|x| {
                        let mut f2 = Vec::new();
                        x.fingerprint(&mut f2);
                        f2 == fp
                    }) {
                        return Ok(i);
                    }
                    group_exprs.push(g);
                    Ok(group_exprs.len() - 1)
                };
                let bind_list = |b: &mut Self, l: &[ast::Expr], group_exprs: &mut Vec<Expr>| -> Result<Vec<usize>> {
                    l.iter().map(|e| bind_one(b, e, group_exprs)).collect()
                };
                // each element contributes alternatives; the sets are their cross product
                let mut combos: Vec<Vec<usize>> = vec![vec![]];
                let mut uses_sets = false;
                for e in exprs {
                    let alts: Vec<Vec<usize>> = match e {
                        ast::Expr::Rollup(lists) => {
                            uses_sets = true;
                            let ls: Vec<Vec<usize>> =
                                lists.iter().map(|l| bind_list(self, l, &mut group_exprs)).collect::<Result<_>>()?;
                            (0..=ls.len()).rev().map(|n| ls[..n].concat()).collect()
                        }
                        ast::Expr::Cube(lists) => {
                            uses_sets = true;
                            let ls: Vec<Vec<usize>> =
                                lists.iter().map(|l| bind_list(self, l, &mut group_exprs)).collect::<Result<_>>()?;
                            if ls.len() > 12 {
                                sql_bail!(codes::PROGRAM_LIMIT_EXCEEDED, "CUBE is limited to 12 elements");
                            }
                            (0..1usize << ls.len())
                                .rev()
                                .map(|mask| (0..ls.len()).filter(|i| mask & (1 << (ls.len() - 1 - i)) != 0).flat_map(|i| ls[i].clone()).collect())
                                .collect()
                        }
                        ast::Expr::GroupingSets(sets) => {
                            uses_sets = true;
                            sets.iter().map(|l| bind_list(self, l, &mut group_exprs)).collect::<Result<_>>()?
                        }
                        e => vec![vec![bind_one(self, e, &mut group_exprs)?]],
                    };
                    let mut next = Vec::with_capacity(combos.len() * alts.len());
                    for c in &combos {
                        for a in &alts {
                            let mut set = c.clone();
                            for i in a {
                                if !set.contains(i) {
                                    set.push(*i);
                                }
                            }
                            next.push(set);
                        }
                    }
                    combos = next;
                }
                if uses_sets {
                    if combos.len() > 4096 {
                        sql_bail!(codes::PROGRAM_LIMIT_EXCEEDED, "too many grouping sets present");
                    }
                    grouping_sets = Some(combos);
                }
            }
        }

        let having = match &s.having {
            Some(h) => {
                let hctx = Ctx { scope: &scope, collector: Some(&collector), in_agg: false, clause: "HAVING" };
                let e = self.bind_expr(h, &hctx)?;
                Some(self.coerce_bool(e, "HAVING")?)
            }
            None => None,
        };

        // ORDER BY: output alias, ordinal, or expression over the input
        let mut order_items: Vec<(OrderTarget, SortSpec)> = Vec::new();
        for ob in order_by {
            let spec = sort_spec(&ob.options);
            if let Some(c) = self.order_by_output_ref(&ob.expr, &names, names.len(), Some(&sel_exprs))? {
                // an alias that is also an input column name prefers the output column (PG)
                order_items.push((OrderTarget::Output(c), spec));
                continue;
            }
            let octx = Ctx { scope: &scope, collector: Some(&collector), in_agg: false, clause: "ORDER BY" };
            let e = self.bind_expr(&ob.expr, &octx)?;
            order_items.push((OrderTarget::Expr(e), spec));
        }

        let is_agg =
            !group_exprs.is_empty() || grouping_sets.is_some() || !collector.borrow().aggs.is_empty() || having.is_some();
        let Collector { aggs, windows, srfs } = collector.into_inner();

        // Rewriter from input-scope expressions to the post-aggregation scope.
        let mut post: Box<dyn Fn(&Expr) -> Result<Expr>> = Box::new(|e: &Expr| Ok(e.clone()));
        let mut windows = windows;
        if is_agg {
            // functional dependencies: columns of tables whose PK is grouped,
            // added only when the query refers to them (an unused one would
            // make every change to it move rows between groups)
            let mut all_group = group_exprs.clone();
            if grouping_sets.is_none() {
                let mut used: std::collections::BTreeSet<usize> = std::collections::BTreeSet::new();
                for e in sel_exprs.iter().chain(having.iter()) {
                    used.extend(e.column_set());
                }
                for (t, _) in &order_items {
                    if let OrderTarget::Expr(e) = t {
                        used.extend(e.column_set());
                    }
                }
                for w in &windows {
                    for e in w.args.iter().chain(w.partition.iter()).chain(w.order.iter().map(|(e, _)| e)) {
                        used.extend(e.column_set());
                    }
                }
                for f in &srfs {
                    for e in &f.args {
                        used.extend(e.column_set());
                    }
                }
                for c in self.functional_dependencies(&rel, &scope, &group_exprs) {
                    if used.contains(&c) && !all_group.iter().any(|g| matches!(g, Expr::Column(x) if *x == c)) {
                        all_group.push(Expr::Column(c));
                    }
                }
            }
            let base = rel.arity();
            let mut pre: Vec<Expr> = all_group.clone();
            let mut calls = Vec::with_capacity(aggs.len());
            for a in &aggs {
                let mut args = Vec::new();
                for e in &a.args {
                    pre.push(e.clone());
                    args.push(base + pre.len() - 1);
                }
                let filter = a.filter.as_ref().map(|f| {
                    pre.push(f.clone());
                    base + pre.len() - 1
                });
                let mut order = Vec::new();
                for (e, spec) in &a.order {
                    pre.push(e.clone());
                    order.push(SortKey { col: base + pre.len() - 1, spec: *spec });
                }
                calls.push(AggCall {
                    func: a.func,
                    args,
                    distinct: a.distinct,
                    filter,
                    order,
                    fraction: a.fraction,
                    ret: a.ret.clone(),
                });
            }
            let ngroup = all_group.len();
            let naggs = calls.len();
            let input = rel.map(pre);
            rel = match &grouping_sets {
                None => Rel::Aggregate { input: Box::new(input), group: (base..base + ngroup).collect(), aggs: calls },
                Some(sets) => {
                    // one aggregation per grouping set, unioned in a common
                    // layout: all grouping columns (NULL where not grouped),
                    // the aggregates, then a flag per column that is not grouped
                    let types = input.schema();
                    let mut branches = Vec::with_capacity(sets.len());
                    for set in sets {
                        let agg = Rel::Aggregate {
                            input: Box::new(input.clone()),
                            group: set.iter().map(|i| base + i).collect(),
                            aggs: calls.clone(),
                        };
                        let mut cols: Vec<Expr> = (0..ngroup)
                            .map(|i| match set.iter().position(|x| *x == i) {
                                Some(p) => Expr::Column(p),
                                None => Expr::null(types[base + i].ty.clone()),
                            })
                            .collect();
                        cols.extend((0..naggs).map(|j| Expr::Column(set.len() + j)));
                        cols.extend((0..ngroup).map(|i| Expr::Literal(Datum::Int4(!set.contains(&i) as i32), SqlType::Int4)));
                        let w = set.len() + naggs;
                        branches.push(agg.map(cols).project((w..w + 2 * ngroup + naggs).collect()));
                    }
                    if branches.len() == 1 { branches.pop().unwrap() } else { Rel::UnionAll { inputs: branches } }
                }
            };
            let absent = grouping_sets.as_ref().map(|_| ngroup + naggs);
            let group_fps: Vec<(Vec<u8>, usize)> = all_group
                .iter()
                .enumerate()
                .map(|(i, g)| {
                    let mut fp = Vec::new();
                    g.fingerprint(&mut fp);
                    (fp, i)
                })
                .collect();
            let names_for_err: Vec<String> = scope
                .items
                .iter()
                .map(|i| match &i.table {
                    Some(t) if !t.is_empty() => format!("{t}.{}", i.name),
                    _ => i.name.clone(),
                })
                .collect();
            post = Box::new(move |e: &Expr| rewrite_post_agg(e, &group_fps, ngroup, absent, &names_for_err));
            if let Some(h) = &having {
                let h = post(h)?;
                rel = rel.filter(h.conjuncts());
            }
            // window arguments live in the post-aggregation scope too
            for w in windows.iter_mut() {
                for e in w.args.iter_mut().chain(w.partition.iter_mut()) {
                    *e = post(e)?;
                }
                for (e, _) in w.order.iter_mut() {
                    *e = post(e)?;
                }
            }
        } else if having.is_some() {
            unreachable!();
        }

        // window functions
        let mut win_base = 0;
        if !windows.is_empty() {
            let base = rel.arity();
            let mut pre: Vec<Expr> = Vec::new();
            let mut calls = Vec::new();
            for w in &windows {
                let mut args = Vec::new();
                for e in &w.args {
                    pre.push(e.clone());
                    args.push(base + pre.len() - 1);
                }
                let mut partition = Vec::new();
                for e in &w.partition {
                    pre.push(e.clone());
                    partition.push(base + pre.len() - 1);
                }
                let mut order = Vec::new();
                for (e, spec) in &w.order {
                    pre.push(e.clone());
                    order.push(SortKey { col: base + pre.len() - 1, spec: *spec });
                }
                calls.push(WindowCall {
                    func: w.func,
                    args,
                    partition,
                    order,
                    frame: w.frame,
                    ret: w.ret.clone(),
                    range: w.range.clone(),
                });
            }
            rel = rel.map(pre);
            win_base = rel.arity();
            rel = Rel::Window { input: Box::new(rel), funcs: calls };
        }
        let win_rewrite = |e: Expr| -> Expr { replace_window_refs(e, win_base) };

        // set-returning functions in the select list: one row per result,
        // several in lockstep (as LATERAL ROWS FROM)
        let mut srf_base = 0;
        if !srfs.is_empty() {
            let mut funcs = Vec::with_capacity(srfs.len());
            for f in &srfs {
                let args = f.args.iter().map(|a| Ok(win_rewrite(post(a)?))).collect::<Result<Vec<_>>>()?;
                funcs.push(SrfCall { func: f.func, args, arity: 1 });
            }
            srf_base = rel.arity();
            let out = srfs.iter().map(|f| f.out.clone()).collect();
            rel = Rel::FlatMap { input: Box::new(rel), funcs, out, ordinality: false };
        }
        let win_rewrite = |e: Expr| -> Expr { replace_srf_refs(replace_window_refs(e, win_base), srf_base) };

        // final projection: visible columns then sort helpers
        let mut proj_exprs = Vec::with_capacity(sel_exprs.len());
        for e in &sel_exprs {
            proj_exprs.push(win_rewrite(post(e)?));
        }
        let visible = proj_exprs.len();
        let mut order = Vec::new();
        let mut extra: Vec<Expr> = Vec::new();
        for (target, spec) in order_items {
            let col = match target {
                OrderTarget::Output(c) => c,
                OrderTarget::Expr(e) => {
                    let e = win_rewrite(post(&e)?);
                    let mut fp = Vec::new();
                    e.fingerprint(&mut fp);
                    match proj_exprs.iter().position(|p| {
                        let mut f2 = Vec::new();
                        p.fingerprint(&mut f2);
                        f2 == fp
                    }) {
                        Some(c) => c,
                        None => {
                            if s.distinct.is_some() {
                                sql_bail!(
                                    codes::INVALID_COLUMN_REFERENCE,
                                    "for SELECT DISTINCT, ORDER BY expressions must appear in select list"
                                );
                            }
                            extra.push(e);
                            visible + extra.len() - 1
                        }
                    }
                }
            };
            order.push(SortKey { col, spec });
        }
        let out_exprs = proj_exprs.clone();
        let mut all_out = proj_exprs;
        all_out.extend(extra);
        let base = rel.arity();
        let n_out = all_out.len();
        rel = rel.map(all_out).project((base..base + n_out).collect());
        let mut names_all = names;
        while names_all.len() < n_out {
            names_all.push("?sort?".into());
        }

        // DISTINCT / DISTINCT ON
        match &s.distinct {
            None | Some(ast::Distinct::All) => {}
            Some(ast::Distinct::Distinct) => {
                rel = Rel::Distinct { input: Box::new(rel) };
            }
            Some(ast::Distinct::On(exprs)) => {
                // DISTINCT ON expressions are bound against the select list scope
                let out_scope = Scope::new(
                    names_all
                        .iter()
                        .zip(rel.schema())
                        .map(|(n, t)| ScopeItem { table: None, name: n.clone(), ty: t, hidden: false })
                        .collect(),
                );
                let mut group = Vec::new();
                let mut extra = Vec::new();
                for e in exprs {
                    let c = match self.order_by_output_ref(e, &names_all, visible, Some(&out_exprs))? {
                        Some(c) => c,
                        None => {
                            // expression over input columns: must match a select expression
                            let ex = self.bind_expr(e, &Ctx::plain(&scope, "DISTINCT ON"))?;
                            let ex = post(&ex)?;
                            match self.find_matching_output(&rel, &ex) {
                                Some(c) => c,
                                None => {
                                    let _ = &out_scope;
                                    extra.push(ex);
                                    usize::MAX
                                }
                            }
                        }
                    };
                    if c == usize::MAX {
                        sql_bail!(codes::FEATURE_NOT_SUPPORTED, "DISTINCT ON expressions must appear in the select list");
                    }
                    group.push(c);
                }
                rel = Rel::TopK { input: Box::new(rel), group, order: order.clone(), limit: Some(1), offset: 0, with_ties: false };
            }
        }
        Ok(BoundQuery { rel, names: names_all, visible, order, limit, offset, with_ties: false })
    }

    fn find_matching_output(&self, _rel: &Rel, _e: &Expr) -> Option<usize> {
        None
    }

    /// Group-by item: output ordinal, output alias (if not an input column), or expression.
    fn bind_group_expr(&mut self, e: &ast::Expr, scope: &Scope, names: &[String], sel: &[Expr]) -> Result<Expr> {
        match e {
            ast::Expr::Value(v) => {
                if let ast::Value::Number(n, _) = &v.value {
                    let k: usize = n.parse().map_err(|_| SqlError::syntax("non-integer constant in GROUP BY"))?;
                    if k == 0 || k > sel.len() {
                        sql_bail!(codes::INVALID_COLUMN_REFERENCE, "GROUP BY position {k} is not in select list");
                    }
                    return Ok(sel[k - 1].clone());
                }
            }
            ast::Expr::Identifier(id) => {
                let name = ident(id);
                if scope.resolve(None, &name)?.is_none() && self.lookup_outer(None, &name)?.is_none() {
                    let hits: Vec<usize> = (0..names.len()).filter(|i| names[*i] == name).collect();
                    if !hits.is_empty() && same_exprs(sel, &hits) {
                        return Ok(sel[hits[0]].clone());
                    }
                }
            }
            _ => {}
        }
        self.bind_expr(e, &Ctx::plain(scope, "GROUP BY"))
    }

    /// Columns functionally dependent on grouped primary keys.
    fn functional_dependencies(&self, rel: &Rel, scope: &Scope, group: &[Expr]) -> Vec<usize> {
        let grouped: Vec<usize> = group.iter().filter_map(|g| g.as_column()).collect();
        let mut out = Vec::new();
        // find Get leaves and their column offsets within the FROM scope
        let mut leaves = Vec::new();
        collect_get_offsets(rel, 0, &mut leaves);
        for (offset, arity, keys) in leaves {
            for key in keys {
                if !key.is_empty() && key.iter().all(|k| grouped.contains(&(offset + k))) {
                    for c in 0..arity {
                        if offset + c < scope.len() {
                            out.push(offset + c);
                        }
                    }
                }
            }
        }
        out.sort();
        out.dedup();
        out
    }

    // ------------------------------------------------------------------ FROM

    pub(crate) fn bind_from(&mut self, from: &[ast::TableWithJoins]) -> Result<(Rel, Scope)> {
        let mut acc: Option<(Rel, Scope)> = None;
        for twj in from {
            acc = Some(self.bind_table_with_joins(twj, acc)?);
        }
        Ok(acc.unwrap())
    }

    /// Bind one comma-separated FROM item and combine it with the items to
    /// its left (`left`), which LATERAL items may reference.
    fn bind_table_with_joins(&mut self, twj: &ast::TableWithJoins, left: Option<(Rel, Scope)>) -> Result<(Rel, Scope)> {
        let (first, fscope, lateral) = self.bind_table_factor(&twj.relation, left.as_ref().map(|(_, s)| s))?;
        if lateral && let Some((lrel, lscope)) = left {
            // the lateral item applies to everything on its left; any joins
            // written after it attach to the combined relation
            let mut rel =
                Rel::Apply { left: Box::new(lrel), right: Box::new(first), kind: ApplyKind::Lateral { left_outer: false } };
            let mut scope = lscope.concat(&fscope);
            for j in &twj.joins {
                let (r, rs, lat) = self.bind_table_factor(&j.relation, Some(&scope))?;
                (rel, scope) = self.bind_join(rel, scope, r, rs, &j.join_operator, lat)?;
            }
            return Ok((rel, scope));
        }
        let (mut rel, mut scope) = (first, fscope);
        for j in &twj.joins {
            let (r, rs, lat) = self.bind_table_factor(&j.relation, Some(&scope))?;
            (rel, scope) = self.bind_join(rel, scope, r, rs, &j.join_operator, lat)?;
        }
        Ok(match left {
            None => (rel, scope),
            Some((l, ls)) => {
                let s = ls.concat(&scope);
                (Rel::Join { left: Box::new(l), right: Box::new(rel), kind: JoinKind::Inner, on: vec![] }, s)
            }
        })
    }

    fn bind_join(
        &mut self,
        l: Rel,
        ls: Scope,
        r: Rel,
        rs: Scope,
        op: &ast::JoinOperator,
        lateral: bool,
    ) -> Result<(Rel, Scope)> {
        use ast::JoinOperator as J;
        let (kind, constraint) = match op {
            J::Join(c) | J::Inner(c) => (JoinKind::Inner, Some(c)),
            J::Left(c) | J::LeftOuter(c) => (JoinKind::Left, Some(c)),
            J::Right(c) | J::RightOuter(c) => (JoinKind::Right, Some(c)),
            J::FullOuter(c) => (JoinKind::Full, Some(c)),
            J::CrossJoin(c) => (JoinKind::Inner, Some(c)),
            _ => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unsupported join type"),
        };
        if lateral {
            if !matches!(kind, JoinKind::Inner | JoinKind::Left) {
                sql_bail!(codes::FEATURE_NOT_SUPPORTED, "only INNER and LEFT LATERAL joins are supported");
            }
            let scope = ls.concat(&rs);
            let mut rel = Rel::Apply { left: Box::new(l), right: Box::new(r), kind: ApplyKind::Lateral { left_outer: kind == JoinKind::Left } };
            if let Some(ast::JoinConstraint::On(e)) = constraint {
                let p = self.bind_expr(e, &Ctx::plain(&scope, "JOIN/ON"))?;
                let p = self.coerce_bool(p, "JOIN/ON")?;
                if kind == JoinKind::Left && !p.is_true_literal() {
                    sql_bail!(codes::FEATURE_NOT_SUPPORTED, "LEFT JOIN LATERAL requires ON true");
                }
                rel = rel.filter(p.conjuncts());
            }
            return Ok((rel, scope));
        }
        match constraint {
            None | Some(ast::JoinConstraint::None) => {
                let scope = ls.concat(&rs);
                Ok((Rel::Join { left: Box::new(l), right: Box::new(r), kind, on: vec![] }, scope))
            }
            Some(ast::JoinConstraint::On(e)) => {
                let scope = ls.concat(&rs);
                let p = self.bind_expr(e, &Ctx::plain(&scope, "JOIN/ON"))?;
                let p = self.coerce_bool(p, "JOIN/ON")?;
                Ok((Rel::Join { left: Box::new(l), right: Box::new(r), kind, on: p.conjuncts() }, scope))
            }
            Some(ast::JoinConstraint::Using(names)) => {
                let cols: Vec<String> = names
                    .iter()
                    .map(|n| object_name(n).map(|(_, c)| c))
                    .collect::<Result<_>>()?;
                self.using_join(l, ls, r, rs, kind, &cols)
            }
            Some(ast::JoinConstraint::Natural) => {
                let cols: Vec<String> = ls
                    .items
                    .iter()
                    .filter(|i| !i.hidden)
                    .map(|i| i.name.clone())
                    .filter(|n| rs.items.iter().any(|j| !j.hidden && &j.name == n))
                    .collect();
                self.using_join(l, ls, r, rs, kind, &cols)
            }
        }
    }

    fn using_join(&mut self, l: Rel, ls: Scope, r: Rel, rs: Scope, kind: JoinKind, cols: &[String]) -> Result<(Rel, Scope)> {
        let nl = ls.len();
        let mut on = Vec::new();
        let mut pairs = Vec::new();
        for c in cols {
            let li = ls.resolve(None, c)?.ok_or_else(|| {
                SqlError::new(codes::UNDEFINED_COLUMN, format!("column \"{c}\" specified in USING clause does not exist in left table"))
            })?;
            let ri = rs.resolve(None, c)?.ok_or_else(|| {
                SqlError::new(codes::UNDEFINED_COLUMN, format!("column \"{c}\" specified in USING clause does not exist in right table"))
            })?;
            let (lt, rt) = (ls.items[li].ty.ty.clone(), rs.items[ri].ty.ty.clone());
            let eq = self.make_binary("=", Expr::Column(li), lt.clone(), Expr::Column(nl + ri), rt.clone())?;
            on.push(eq);
            pairs.push((li, ri, common_type(&[lt, rt], "JOIN/USING")?));
        }
        let join = Rel::Join { left: Box::new(l), right: Box::new(r), kind, on };
        let joined = ls.concat(&rs);
        // merged columns first, then the rest of left, then the rest of right
        let mut exprs = Vec::new();
        let mut items = Vec::new();
        for (li, ri, t) in &pairs {
            let le = self.coerce(Expr::Column(*li), &ls.items[*li].ty.ty, t, crate::expr::cast::CastContext::Implicit, "USING")?;
            let re = self.coerce(Expr::Column(nl + *ri), &rs.items[*ri].ty.ty, t, crate::expr::cast::CastContext::Implicit, "USING")?;
            let e = match kind {
                JoinKind::Inner | JoinKind::Left => le,
                JoinKind::Right => re,
                _ => Expr::Coalesce(vec![le, re], t.clone()),
            };
            let mut ty = ls.items[*li].ty.clone();
            ty.ty = t.clone();
            ty.nullable = match kind {
                JoinKind::Inner => ls.items[*li].ty.nullable,
                JoinKind::Left => ls.items[*li].ty.nullable,
                JoinKind::Right => rs.items[*ri].ty.nullable,
                _ => ls.items[*li].ty.nullable && rs.items[*ri].ty.nullable,
            };
            exprs.push(e);
            items.push(ScopeItem { table: None, name: ls.items[*li].name.clone(), ty, hidden: false });
        }
        let base = joined.len();
        let mut proj: Vec<usize> = (base..base + pairs.len()).collect();
        for (i, it) in joined.items.iter().enumerate() {
            let merged = pairs.iter().any(|(li, ri, _)| *li == i || nl + *ri == i);
            proj.push(i);
            let mut it = it.clone();
            if merged {
                it.hidden = true;
            }
            items.push(it);
        }
        let rel = join.map(exprs).project(proj);
        Ok((rel, Scope::new(items)))
    }

    /// Returns (rel, scope, is_lateral).
    fn bind_table_factor(&mut self, tf: &ast::TableFactor, left_scope: Option<&Scope>) -> Result<(Rel, Scope, bool)> {
        match tf {
            ast::TableFactor::Table { name, alias, args, version, with_ordinality, .. } => {
                if version.is_some() {
                    sql_bail!(codes::FEATURE_NOT_SUPPORTED, "table versions are not supported; use AS OF on the query");
                }
                if let Some(args) = args {
                    let (_, fname) = object_name(name)?;
                    let fargs: Vec<ast::FunctionArg> = args.args.clone();
                    return self.bind_table_function(&fname, &fargs, alias.as_ref(), left_scope, *with_ordinality);
                }
                let (schema, tname) = object_name(name)?;
                let (rel, scope) = self.bind_named_relation(schema.as_deref(), &tname, alias.as_ref())?;
                Ok((rel, scope, false))
            }
            ast::TableFactor::Derived { lateral, subquery, alias, .. } => {
                if *lateral {
                    let ls = left_scope.cloned().unwrap_or_default();
                    self.outer.push(ls);
                    let r = self.bind_query(subquery);
                    self.outer.pop();
                    let bq = r?;
                    let (rel, scope) = self.derived(bq, alias.as_ref())?;
                    return Ok((rel, scope, true));
                }
                let bq = self.bind_query(subquery)?;
                let (rel, scope) = self.derived(bq, alias.as_ref())?;
                Ok((rel, scope, false))
            }
            ast::TableFactor::NestedJoin { table_with_joins, alias } => {
                let (rel, mut scope) = self.bind_table_with_joins(table_with_joins, None)?;
                if let Some(a) = alias {
                    let t = ident(&a.name);
                    for it in scope.items.iter_mut() {
                        it.table = Some(t.clone());
                    }
                }
                Ok((rel, scope, false))
            }
            ast::TableFactor::Function { name, args, alias, .. } => {
                let (_, fname) = object_name(name)?;
                self.bind_table_function(&fname, args, alias.as_ref(), left_scope, false)
            }
            ast::TableFactor::UNNEST { alias, array_exprs, with_offset, with_ordinality, .. } => {
                if *with_offset {
                    sql_bail!(codes::FEATURE_NOT_SUPPORTED, "UNNEST WITH OFFSET is not supported");
                }
                let fargs: Vec<ast::FunctionArg> = array_exprs
                    .iter()
                    .map(|e| ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e.clone())))
                    .collect();
                self.bind_table_function("unnest", &fargs, alias.as_ref(), left_scope, *with_ordinality)
            }
            _ => sql_bail!(codes::FEATURE_NOT_SUPPORTED, "unsupported FROM item: {tf}"),
        }
    }

    fn derived(&mut self, bq: BoundQuery, alias: Option<&ast::TableAlias>) -> Result<(Rel, Scope)> {
        let bq = bq.resolve_unknowns(self)?;
        let mut names = bq.names.clone();
        names.truncate(bq.visible);
        let rel = bq.into_rel();
        let types = rel.schema();
        let (tname, cols) = match alias {
            Some(a) => (ident(&a.name), a.columns.iter().map(|c| ident(&c.name)).collect::<Vec<_>>()),
            None => (String::new(), vec![]),
        };
        apply_alias_names(&mut names, &cols, &tname)?;
        Ok((rel, Scope::from_table(&tname, &names, &types)))
    }

    pub(crate) fn bind_named_relation(
        &mut self,
        schema: Option<&str>,
        name: &str,
        alias: Option<&ast::TableAlias>,
    ) -> Result<(Rel, Scope)> {
        let (tname, alias_cols) = match alias {
            Some(a) => (ident(&a.name), a.columns.iter().map(|c| ident(&c.name)).collect::<Vec<_>>()),
            None => (name.to_string(), vec![]),
        };
        // CTEs shadow catalog objects
        if schema.is_none() {
            for level in self.ctes.iter().rev() {
                if let Some((_, b)) = level.iter().rev().find(|(n, _)| n == name) {
                    let mut names = b.names.clone();
                    apply_alias_names(&mut names, &alias_cols, &tname)?;
                    let types = b.rel.schema();
                    return Ok((b.rel.clone(), Scope::from_table(&tname, &names, &types)));
                }
            }
        }
        let rel = self.catalog.resolve_relation(schema, name).ok_or_else(|| {
            SqlError::new(codes::UNDEFINED_TABLE, format!("relation \"{}\" does not exist", match schema {
                Some(s) => format!("{s}.{name}"),
                None => name.to_string(),
            }))
        })?;
        match rel {
            CatalogRelation::Table { id, name: oname, columns, keys } => {
                let mut names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
                apply_alias_names(&mut names, &alias_cols, &tname)?;
                let types: Vec<ColumnType> = columns.iter().map(|c| c.ty.clone()).collect();
                Ok((Rel::Get { id, name: oname, columns: types.clone(), keys }, Scope::from_table(&tname, &names, &types)))
            }
            CatalogRelation::Constant { columns, rows, .. } => {
                let mut names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
                apply_alias_names(&mut names, &alias_cols, &tname)?;
                let types: Vec<ColumnType> = columns.iter().map(|c| c.ty.clone()).collect();
                let rows = rows
                    .into_iter()
                    .map(|r| r.into_iter().zip(&types).map(|(d, t)| Expr::Literal(d, t.ty.clone())).collect())
                    .collect();
                Ok((Rel::Values { rows, columns: types.clone() }, Scope::from_table(&tname, &names, &types)))
            }
            CatalogRelation::View { name: vname, columns, sql, .. } => {
                if self.view_depth > 32 {
                    sql_bail!(codes::INVALID_RECURSION, "infinite recursion detected in view \"{vname}\"");
                }
                let stmt = super::parser::parse_single_query(&sql)?;
                let mut sub = Binder::new(self.catalog);
                sub.view_depth = self.view_depth + 1;
                sub.next_rec_id = self.next_rec_id + 1000;
                let bq = sub.bind_query(&stmt)?.resolve_unknowns(&mut sub)?;
                self.next_rec_id = sub.next_rec_id;
                let rel = bq.into_rel();
                let mut names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
                apply_alias_names(&mut names, &alias_cols, &tname)?;
                let types = rel.schema();
                Ok((rel, Scope::from_table(&tname, &names, &types)))
            }
        }
    }

    /// Look a column up in enclosing query scopes: Some((depth, col, type)).
    pub(crate) fn lookup_outer(&self, table: Option<&str>, name: &str) -> Result<Option<(usize, usize, ColumnType)>> {
        for (k, s) in self.outer.iter().rev().enumerate() {
            if let Some(c) = s.resolve(table, name)? {
                return Ok(Some((k + 1, c, s.items[c].ty.clone())));
            }
        }
        Ok(None)
    }

    pub(crate) fn coerce_bool(&mut self, e: Expr, clause: &str) -> Result<Expr> {
        let t = e.ty(&[]);
        match t {
            SqlType::Bool => Ok(e),
            SqlType::Unknown => self.coerce(e, &t, &SqlType::Bool, crate::expr::cast::CastContext::Implicit, clause),
            other => sql_bail!(codes::DATATYPE_MISMATCH, "argument of {clause} must be type boolean, not type {}", other),
        }
    }
}

enum OrderTarget {
    Output(usize),
    Expr(Expr),
}

/// Whether the expressions at `idx` are all the same.
fn same_exprs(exprs: &[Expr], idx: &[usize]) -> bool {
    let fp = |e: &Expr| {
        let mut v = Vec::new();
        e.fingerprint(&mut v);
        v
    };
    let first = fp(&exprs[idx[0]]);
    idx[1..].iter().all(|i| fp(&exprs[*i]) == first)
}

fn collect_get_offsets(rel: &Rel, offset: usize, out: &mut Vec<(usize, usize, Vec<Vec<usize>>)>) {
    match rel {
        Rel::Get { columns, keys, .. } => out.push((offset, columns.len(), keys.clone())),
        Rel::Filter { input, .. } => collect_get_offsets(input, offset, out),
        Rel::Join { left, right, kind, .. } if matches!(kind, JoinKind::Inner | JoinKind::Left | JoinKind::Right | JoinKind::Full) => {
            collect_get_offsets(left, offset, out);
            collect_get_offsets(right, offset + left.arity(), out);
        }
        _ => {}
    }
}

pub(crate) fn sort_spec(o: &ast::OrderByOptions) -> SortSpec {
    let desc = matches!(o.sort, Some(ast::OrderBySort::Desc));
    SortSpec::new(desc, o.nulls_first)
}

pub(crate) fn apply_alias_names(names: &mut [String], alias: &[String], table: &str) -> Result<()> {
    if alias.len() > names.len() {
        sql_bail!(
            codes::INVALID_COLUMN_REFERENCE,
            "table \"{table}\" has {} columns available but {} columns specified",
            names.len(),
            alias.len()
        );
    }
    for (n, a) in names.iter_mut().zip(alias) {
        *n = a.clone();
    }
    Ok(())
}

/// PostgreSQL's output column naming.
pub(crate) fn derive_name(e: &ast::Expr) -> String {
    match e {
        ast::Expr::Identifier(i) => ident(i),
        ast::Expr::CompoundIdentifier(v) => v.last().map(ident).unwrap_or_default(),
        ast::Expr::Function(f) => f.name.0.last().and_then(|p| p.as_ident()).map(ident).unwrap_or_else(|| "?column?".into()),
        ast::Expr::Cast { expr, data_type, .. } => match &**expr {
            ast::Expr::Value(_) => type_name_for_alias(data_type),
            other => {
                let n = derive_name(other);
                if n == "?column?" { type_name_for_alias(data_type) } else { n }
            }
        },
        ast::Expr::Nested(inner) => derive_name(inner),
        ast::Expr::Case { .. } => "case".into(),
        ast::Expr::Exists { .. } => "exists".into(),
        ast::Expr::Array(_) => "array".into(),
        ast::Expr::Extract { .. } => "extract".into(),
        ast::Expr::Substring { .. } => "substring".into(),
        ast::Expr::Position { .. } => "position".into(),
        ast::Expr::Trim { .. } => "btrim".into(),
        ast::Expr::Ceil { .. } => "ceil".into(),
        ast::Expr::Floor { .. } => "floor".into(),
        ast::Expr::Overlay { .. } => "overlay".into(),
        ast::Expr::Subquery(q) => match &*q.body {
            ast::SetExpr::Select(s) if s.projection.len() == 1 => match &s.projection[0] {
                ast::SelectItem::UnnamedExpr(e) => derive_name(e),
                ast::SelectItem::ExprWithAlias { alias, .. } => ident(alias),
                _ => "?column?".into(),
            },
            _ => "?column?".into(),
        },
        ast::Expr::TypedString(ts) => type_name_for_alias(&ts.data_type),
        ast::Expr::Interval(_) => "interval".into(),
        ast::Expr::AtTimeZone { .. } => "timezone".into(),
        ast::Expr::Value(v) if matches!(v.value, ast::Value::Boolean(_)) => "bool".into(),
        _ => "?column?".into(),
    }
}

fn type_name_for_alias(dt: &ast::DataType) -> String {
    match super::binder_expr::sql_type_of(dt) {
        Ok((t, _)) => match t {
            SqlType::Int2 => "int2".into(),
            SqlType::Int4 => "int4".into(),
            SqlType::Int8 => "int8".into(),
            SqlType::Float4 => "float4".into(),
            SqlType::Float8 => "float8".into(),
            SqlType::Bool => "bool".into(),
            SqlType::Varchar => "varchar".into(),
            SqlType::Bpchar => "bpchar".into(),
            SqlType::Timestamp => "timestamp".into(),
            SqlType::TimestampTz => "timestamptz".into(),
            SqlType::Time => "time".into(),
            SqlType::Array(_) => "?column?".into(),
            other => other.name(),
        },
        Err(_) => "?column?".into(),
    }
}

/// Does `q` reference relation `name` (for detecting recursive CTEs)?
fn query_references(q: &ast::Query, name: &str) -> bool {
    let s = q.to_string().to_lowercase();
    // conservative textual check on word boundaries
    s.split(|c: char| !(c.is_alphanumeric() || c == '_')).any(|w| w == name)
}

/// Rewrite an input-scope expression into the post-aggregation scope.
/// Map an input-scope expression to the aggregation output: grouping
/// expressions to their columns, aggregates to theirs. `absent` is the
/// first of the per-grouping-column "not grouped" flags (grouping sets).
fn rewrite_post_agg(e: &Expr, group_fps: &[(Vec<u8>, usize)], ngroup: usize, absent: Option<usize>, names: &[String]) -> Result<Expr> {
    let mut fp = Vec::new();
    e.fingerprint(&mut fp);
    if !matches!(e, Expr::Literal(..))
        && let Some((_, i)) = group_fps.iter().find(|(g, _)| *g == fp)
    {
        return Ok(Expr::Column(*i));
    }
    if let Expr::Call(c) = e
        && crate::expr::func::func_def(c.func).name == "grouping"
    {
        // bit k (from the right) is set when argument n-1-k is not grouped
        let mut bits: Vec<Expr> = Vec::new();
        let n = c.args.len();
        if n > 31 {
            sql_bail!(codes::TOO_MANY_ARGUMENTS, "GROUPING must have fewer than 32 arguments");
        }
        for (j, a) in c.args.iter().enumerate() {
            let mut f = Vec::new();
            a.fingerprint(&mut f);
            let Some((_, i)) = group_fps.iter().find(|(g, _)| *g == f) else {
                sql_bail!(codes::GROUPING_ERROR, "arguments to GROUPING must be grouping expressions of the associated query level");
            };
            if let Some(base) = absent {
                let flag = Expr::Column(base + i);
                let weight = 1i32 << (n - 1 - j);
                bits.push(crate::expr::Expr::mul_int(flag, weight));
            }
        }
        return Ok(bits.into_iter().reduce(crate::expr::Expr::add_int).unwrap_or(Expr::Literal(Datum::Int4(0), SqlType::Int4)));
    }
    match e {
        Expr::AggRef(k, _) => Ok(Expr::Column(ngroup + k)),
        Expr::Column(c) => sql_bail!(
            codes::GROUPING_ERROR,
            "column \"{}\" must appear in the GROUP BY clause or be used in an aggregate function",
            names.get(*c).cloned().unwrap_or_else(|| format!("#{c}"))
        ),
        Expr::Subquery(s) => {
            // correlated references (depth 1) to input columns must map to group keys
            let mut s2 = (**s).clone();
            let mut err = None;
            super::decorrelate::map_outer_refs(&mut s2.rel, 1, &mut |col, ty| {
                let mut f = Vec::new();
                Expr::Column(col).fingerprint(&mut f);
                match group_fps.iter().find(|(g, _)| *g == f) {
                    Some((_, i)) => Expr::Outer(1, *i, ty.clone()),
                    None => {
                        err = Some(SqlError::new(
                            codes::GROUPING_ERROR,
                            format!(
                                "subquery uses ungrouped column \"{}\" from outer query",
                                names.get(col).cloned().unwrap_or_default()
                            ),
                        ));
                        Expr::Outer(1, col, ty.clone())
                    }
                }
            });
            if let Some(e) = err {
                return Err(e);
            }
            let mut out = Expr::Subquery(Box::new(s2));
            for c in out.children_mut() {
                *c = rewrite_post_agg(c, group_fps, ngroup, absent, names)?;
            }
            Ok(out)
        }
        _ => {
            let mut out = e.clone();
            for c in out.children_mut() {
                *c = rewrite_post_agg(c, group_fps, ngroup, absent, names)?;
            }
            Ok(out)
        }
    }
}

fn replace_srf_refs(e: Expr, base: usize) -> Expr {
    match e {
        Expr::SrfRef(k, _) => Expr::Column(base + k),
        mut other => {
            for c in other.children_mut() {
                let taken = std::mem::replace(c, Expr::null(SqlType::Unknown));
                *c = replace_srf_refs(taken, base);
            }
            other
        }
    }
}

fn replace_window_refs(e: Expr, base: usize) -> Expr {
    match e {
        Expr::WindowRef(k, _) => Expr::Column(base + k),
        mut other => {
            for c in other.children_mut() {
                let taken = std::mem::replace(c, Expr::null(SqlType::Unknown));
                *c = replace_window_refs(taken, base);
            }
            other
        }
    }
}

/// PostgreSQL's select_common_type: unknown inputs are ignored (all unknown:
/// text); every other input must be of the first one's type category; a
/// later input's type replaces the candidate when the candidate is not the
/// category's preferred type and converts to it implicitly, but not back.
pub fn common_type(types: &[SqlType], context: &str) -> Result<SqlType> {
    use crate::expr::cast::implicit_castable;
    let known: Vec<&SqlType> = types.iter().filter(|t| **t != SqlType::Unknown).collect();
    let Some(first) = known.first() else { return Ok(SqlType::Text) };
    let mut ptype = (*first).clone();
    for t in &known[1..] {
        if **t == ptype {
            continue;
        }
        if t.category() != ptype.category() {
            sql_bail!(codes::DATATYPE_MISMATCH, "{context} types {} and {} cannot be matched", ptype, t);
        }
        if !ptype.is_preferred() && implicit_castable(&ptype, t) && !implicit_castable(t, &ptype) {
            ptype = (*t).clone();
        }
    }
    Ok(ptype)
}
