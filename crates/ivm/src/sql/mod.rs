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
pub fn plan_query(catalog: &dyn Catalog, q: &sqlparser::ast::Query, params: Vec<ParamSlot>) -> Result<BoundQuery> {
    let mut b = Binder::new(catalog).with_params(params);
    let mut bq = b.bind_query(q)?.resolve_unknowns(&mut b)?;
    let rel = std::mem::replace(&mut bq.rel, Rel::unit());
    let rel = decorrelate::decorrelate(rel)?;
    bq.rel = optimize::optimize(rel)?;
    Ok(bq)
}
