//! SQL front end: parsing, binding, decorrelation, logical optimization.

pub mod binder;
pub mod binder_expr;
pub mod decorrelate;
pub mod optimize;
pub mod parser;
pub mod plan;
pub mod scope;

pub use binder::{BoundQuery, Binder, Catalog, CatalogRelation, ParamSlot};
pub use parser::{parse_statement, split_statements, AsOf, Statement};
pub use plan::Rel;

use crate::error::Result;

/// Bind, decorrelate and optimize a query.
///
/// Planning runs on the engine thread, where a panic would stop the engine
/// for every session; planning only reads the catalog, so a panic (a bug)
/// is contained and reported as an internal error instead.
pub fn plan_query(catalog: &dyn Catalog, q: &sqlparser::ast::Query, params: Vec<ParamSlot>) -> Result<BoundQuery> {
    plan_guarded(catalog, q, params, false)
}

/// Plan the stored definition of a materialized view (at recovery): as
/// plan_query, but tolerating what definitions accepted by earlier versions
/// may contain (a parameter placeholder, which binds as NULL).
pub fn plan_stored_query(catalog: &dyn Catalog, q: &sqlparser::ast::Query) -> Result<BoundQuery> {
    plan_guarded(catalog, q, vec![], true)
}

fn plan_guarded(catalog: &dyn Catalog, q: &sqlparser::ast::Query, params: Vec<ParamSlot>, stored: bool) -> Result<BoundQuery> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| plan_query_unguarded(catalog, q, params, stored))) {
        Ok(r) => r,
        Err(p) => {
            let msg = p.downcast_ref::<&str>().map(|s| s.to_string()).or_else(|| p.downcast_ref::<String>().cloned());
            Err(crate::error::SqlError::internal(format!("internal error while planning: {}", msg.unwrap_or_default())))
        }
    }
}

fn plan_query_unguarded(catalog: &dyn Catalog, q: &sqlparser::ast::Query, params: Vec<ParamSlot>, stored: bool) -> Result<BoundQuery> {
    let mut b = Binder::new(catalog).with_params(params);
    b.stored_definition = stored;
    let mut bq = b.bind_query(q)?.resolve_unknowns(&mut b)?;
    bq.catalog_reads = b.catalog_reads.take();
    let rel = std::mem::replace(&mut bq.rel, Rel::unit());
    let rel = decorrelate::decorrelate(rel)?;
    bq.rel = optimize::optimize(rel)?;
    Ok(bq)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct PanickingCatalog;

    impl Catalog for PanickingCatalog {
        fn resolve_relation(&self, _: Option<&str>, _: &str) -> Option<CatalogRelation> {
            panic!("bug while resolving");
        }
    }

    struct NoRelations;

    impl Catalog for NoRelations {
        fn resolve_relation(&self, _: Option<&str>, _: &str) -> Option<CatalogRelation> {
            None
        }
    }

    #[test]
    fn correlated_scopes_and_substitution_boundaries() {
        use crate::expr::{Expr, SubqueryExpr, SubqueryKind};
        use crate::types::SqlType;
        let outer = |depth, col| Expr::Outer(depth, col, SqlType::Int4);
        let sq = Expr::Subquery(Box::new(SubqueryExpr {
            kind: SubqueryKind::In(vec![outer(2, 3)]),
            rel: Rel::unit().map(vec![outer(3, 4), outer(2, 5)]),
            ty: SqlType::Bool,
        }));
        let mut rel = Rel::Apply {
            left: Box::new(Rel::unit().map(vec![outer(1, 0)])),
            right: Box::new(Rel::unit().map(vec![outer(2, 1), outer(1, 2), sq])),
            kind: plan::ApplyKind::Lateral { left_outer: false },
        };
        let mut seen = Vec::new();
        rel.any_expr_scoped(1, &mut |e, depth| {
            if let Expr::Outer(d, c, _) = e && *d == depth { seen.push(*c); }
            false
        });
        assert_eq!(seen, [0, 1, 4, 3]);
        // Replace references to this scope, leaving references to inner scopes alone.
        decorrelate::map_outer_refs(&mut rel, 1, &mut |col, _| Expr::Column(col));
        assert!(!rel.has_outer_refs(1));
        assert!(rel.has_outer_refs(0));
        // Substitution must not substitute the columns of its own replacements.
        let e = Expr::And(vec![Expr::Column(0), Expr::Column(1)]);
        let Expr::And(args) = e.substitute(&[Expr::Column(1), Expr::Column(0)]) else { panic!() };
        assert!(matches!(args.as_slice(), [Expr::Column(1), Expr::Column(0)]));
    }

    #[test]
    fn stored_definitions_tolerate_parameter_placeholders() {
        let Statement::Select { query, .. } = parse_statement("SELECT $1 AS x").unwrap() else { panic!() };
        let err = plan_query(&NoRelations, &query, vec![]).unwrap_err();
        assert_eq!(err.message, "there is no parameter $1");
        // a materialized view defined so by an earlier version still recovers
        assert!(plan_stored_query(&NoRelations, &query).is_ok());
    }

    /// Tables `t (x int)` and `u (k int, y int)`, and views `v1`, `v2`, ...,
    /// each `levels` nested subqueries `SELECT x + x AS x FROM (...)` over
    /// the one before (`v1` over `t`).
    struct Nests {
        levels: usize,
    }

    impl Catalog for Nests {
        fn resolve_relation(&self, _: Option<&str>, name: &str) -> Option<CatalogRelation> {
            use crate::types::{Column, ColumnType, SqlType};
            let int = |n: &str| Column::new(n, ColumnType::new(SqlType::Int4));
            match name {
                "t" => Some(CatalogRelation::Table { id: 1, name: name.into(), columns: vec![int("x")], keys: vec![] }),
                "u" => Some(CatalogRelation::Table { id: 2, name: name.into(), columns: vec![int("k"), int("y")], keys: vec![] }),
                _ => {
                    let i: u64 = name.strip_prefix('v')?.parse().ok()?;
                    let below = if i == 1 { "t".to_string() } else { format!("v{}", i - 1) };
                    let sql = nest(&format!("SELECT x FROM {below}"), self.levels);
                    Some(CatalogRelation::View { id: 100 + i, name: name.into(), columns: vec![int("x")], sql })
                }
            }
        }
    }

    fn nest(q: &str, levels: usize) -> String {
        (0..levels).fold(q.to_string(), |q, i| format!("SELECT x + x AS x FROM ({q}) s{i}"))
    }

    /// Expression nodes in a plan.
    fn plan_size(rel: &Rel) -> usize {
        fn expr_size(e: &crate::expr::Expr) -> usize {
            1 + e.children().into_iter().map(expr_size).sum::<usize>()
        }
        rel.exprs().into_iter().map(expr_size).sum::<usize>() + rel.inputs().into_iter().map(plan_size).sum::<usize>()
    }

    /// Plans as large as the query: predicate push-down inlines the
    /// expressions of nested projections only while that does not copy them
    /// too often (each level of `x + x AS x` used to double the predicate,
    /// so planning took exponential time), and so does the decorrelation of
    /// a scalar aggregate below them. Up to 600 levels plan in linear size
    /// (and time: the exponential versions would never finish).
    #[test]
    fn plans_of_nested_projections_stay_linear() {
        let (tx, rx) = std::sync::mpsc::channel();
        // (deep syntax trees: a stack as large as the engine thread's)
        std::thread::Builder::new()
            .stack_size(64 << 20)
            .spawn(move || {
                let size = |views: usize, q: &str| {
                    let Statement::Select { query, .. } = parse_statement(q).unwrap() else { panic!() };
                    plan_size(&plan_query(&Nests { levels: 20 }, &query, vec![]).unwrap().rel) as f64 / (20 * views) as f64
                };
                let mut sizes = Vec::new();
                for views in [1, 2, 8, 30] {
                    sizes.push(size(views, &format!("SELECT x FROM v{views} WHERE x > 0")));
                    sizes.push(size(views, &format!("SELECT x FROM v{views} a JOIN u ON a.x = u.k WHERE a.x > 0 AND u.y > a.x")));
                }
                // (as deep as the parser allows a single query)
                let sub = nest("SELECT sum(y) AS x FROM u WHERE u.k = t.x", 20);
                sizes.push(size(1, &format!("SELECT ({sub}) FROM t")));
                tx.send(sizes).unwrap();
            })
            .unwrap();
        let sizes = rx.recv_timeout(std::time::Duration::from_secs(60)).expect("plans within a minute");
        // expression nodes per level
        assert!(sizes.iter().all(|s| *s < 6.0), "{sizes:?}");
    }

    #[test]
    fn planning_panics_become_errors() {
        let Statement::Select { query, .. } = parse_statement("SELECT * FROM t").unwrap() else { panic!() };
        let err = plan_query(&PanickingCatalog, &query, vec![]).unwrap_err();
        assert!(err.message.contains("internal error while planning: bug while resolving"), "{}", err.message);
    }
}
