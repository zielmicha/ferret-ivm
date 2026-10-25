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
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| plan_query_unguarded(catalog, q, params))) {
        Ok(r) => r,
        Err(p) => {
            let msg = p.downcast_ref::<&str>().map(|s| s.to_string()).or_else(|| p.downcast_ref::<String>().cloned());
            Err(crate::error::SqlError::internal(format!("internal error while planning: {}", msg.unwrap_or_default())))
        }
    }
}

fn plan_query_unguarded(catalog: &dyn Catalog, q: &sqlparser::ast::Query, params: Vec<ParamSlot>) -> Result<BoundQuery> {
    let mut b = Binder::new(catalog).with_params(params);
    let mut bq = b.bind_query(q)?.resolve_unknowns(&mut b)?;
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

    #[test]
    fn planning_panics_become_errors() {
        let Statement::Select { query, .. } = parse_statement("SELECT * FROM t").unwrap() else { panic!() };
        let err = plan_query(&PanickingCatalog, &query, vec![]).unwrap_err();
        assert!(err.message.contains("internal error while planning: bug while resolving"), "{}", err.message);
    }
}
