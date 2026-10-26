//! Fused map / filter / project.
//!
//! Steps run in order so that SQL evaluation order is preserved (a WHERE
//! clause guards the select list's expressions against errors).

use crate::error::Result;
use crate::expr::Expr;
use crate::types::{ColumnType, Datum, Row};

#[derive(Clone, Debug)]
pub enum MfpStep {
    /// Append a computed column.
    Map(Expr),
    /// Drop the row unless the predicate is true.
    Filter(Expr),
}

#[derive(Clone, Debug, Default)]
pub struct Mfp {
    pub input_arity: usize,
    pub steps: Vec<MfpStep>,
    /// Output columns of the extended row.
    pub projection: Vec<usize>,
}

impl Mfp {
    pub fn identity(arity: usize) -> Mfp {
        Mfp { input_arity: arity, steps: vec![], projection: (0..arity).collect() }
    }

    pub fn is_identity(&self) -> bool {
        self.steps.is_empty() && self.projection.len() == self.input_arity && self.projection.iter().enumerate().all(|(i, c)| i == *c)
    }

    pub fn output_arity(&self) -> usize {
        self.projection.len()
    }

    pub fn extended_arity(&self) -> usize {
        self.input_arity + self.steps.iter().filter(|s| matches!(s, MfpStep::Map(_))).count()
    }

    /// Compose `self` after `inner` (inner runs first).
    pub fn after(&self, inner: &Mfp) -> Mfp {
        // translate self's references to inner's output columns into
        // references to inner's extended row
        let base = inner.extended_arity();
        let mut steps = inner.steps.clone();
        let mut ext_map: Vec<usize> = inner.projection.clone(); // self col -> extended col
        let mut next = base;
        for s in &self.steps {
            match s {
                MfpStep::Map(e) => {
                    let m = ext_map.clone();
                    steps.push(MfpStep::Map(e.remapped(&|c| m[c])));
                    ext_map.push(next);
                    next += 1;
                }
                MfpStep::Filter(e) => {
                    let m = ext_map.clone();
                    steps.push(MfpStep::Filter(e.remapped(&|c| m[c])));
                }
            }
        }
        let projection = self.projection.iter().map(|c| ext_map[*c]).collect();
        Mfp { input_arity: inner.input_arity, steps, projection }
    }

    pub fn add_filter(&mut self, pred: Expr) {
        // pred refers to the current output columns
        let proj = self.projection.clone();
        self.steps.push(MfpStep::Filter(pred.remapped(&|c| proj[c])));
    }

    pub fn add_map(&mut self, e: Expr) {
        let proj = self.projection.clone();
        let ext = self.extended_arity();
        self.steps.push(MfpStep::Map(e.remapped(&|c| proj[c])));
        self.projection.push(ext);
    }

    pub fn project(&mut self, cols: &[usize]) {
        self.projection = cols.iter().map(|c| self.projection[*c]).collect();
    }

    /// Apply to one row. Ok(None) when filtered out.
    pub fn eval(&self, row: &[Datum]) -> Result<Option<Row>> {
        if self.steps.is_empty() {
            return Ok(Some(self.projection.iter().map(|c| row[*c].clone()).collect()));
        }
        let mut ext: Row = Vec::with_capacity(self.extended_arity());
        ext.extend_from_slice(row);
        for s in &self.steps {
            match s {
                MfpStep::Map(e) => {
                    let v = e.eval(&ext)?;
                    ext.push(v);
                }
                MfpStep::Filter(p) => {
                    if !p.eval_pred(&ext)? {
                        return Ok(None);
                    }
                }
            }
        }
        Ok(Some(self.projection.iter().map(|c| ext[*c].clone()).collect()))
    }

    /// Input columns the steps and the projection read.
    pub fn input_columns(&self) -> Vec<usize> {
        let mut cols: Vec<usize> = self.projection.iter().copied().filter(|c| *c < self.input_arity).collect();
        for s in &self.steps {
            let (MfpStep::Map(e) | MfpStep::Filter(e)) = s;
            cols.extend(e.column_set().into_iter().filter(|c| *c < self.input_arity));
        }
        cols.sort();
        cols.dedup();
        cols
    }

    /// Output schema.
    pub fn schema(&self, input: &[ColumnType]) -> Vec<ColumnType> {
        let mut ext: Vec<ColumnType> = input.to_vec();
        for s in &self.steps {
            if let MfpStep::Map(e) = s {
                let t = e.column_type(&ext);
                ext.push(t);
            }
        }
        // filters with IS NOT NULL make columns non-null
        for s in &self.steps {
            if let MfpStep::Filter(Expr::IsNotNull(inner)) = s
                && let Expr::Column(c) = &**inner
            {
                ext[*c].nullable = false;
            }
        }
        self.projection.iter().map(|c| ext[*c].clone()).collect()
    }

    pub fn fingerprint(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&(self.input_arity as u32).to_le_bytes());
        for s in &self.steps {
            match s {
                MfpStep::Map(e) => {
                    out.push(1);
                    e.fingerprint(out);
                }
                MfpStep::Filter(e) => {
                    out.push(2);
                    e.fingerprint(out);
                }
            }
        }
        out.push(3);
        for c in &self.projection {
            out.extend_from_slice(&(*c as u32).to_le_bytes());
        }
    }

    /// Expression for output column `c` in terms of input columns, if it is
    /// computed without depending on filters (for arrangement keys).
    pub fn output_expr(&self, c: usize) -> Expr {
        let mut defs: Vec<Expr> = (0..self.input_arity).map(Expr::Column).collect();
        for s in &self.steps {
            if let MfpStep::Map(e) = s {
                let d = e.substitute(&defs);
                defs.push(d);
            }
        }
        defs[self.projection[c]].clone()
    }

    /// Can evaluating the steps fail on some row of the `input` types? (No
    /// only when every step provably evaluates without error.)
    pub fn may_fail(&self, input: &[ColumnType]) -> bool {
        let mut ext: Vec<ColumnType> = input.to_vec();
        for s in &self.steps {
            match s {
                MfpStep::Map(e) => {
                    if !cannot_fail(e, &ext) {
                        return true;
                    }
                    ext.push(e.column_type(&ext));
                }
                MfpStep::Filter(e) => {
                    if !cannot_fail(e, &ext) {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Predicates expressed over input columns.
    pub fn predicates_over_input(&self) -> Vec<Expr> {
        let mut defs: Vec<Expr> = (0..self.input_arity).map(Expr::Column).collect();
        let mut out = Vec::new();
        for s in &self.steps {
            match s {
                MfpStep::Map(e) => {
                    let d = e.substitute(&defs);
                    defs.push(d);
                }
                MfpStep::Filter(p) => out.push(p.substitute(&defs)),
            }
        }
        out
    }
}

/// Does `e` evaluate without error on every row of `types`? Conservative:
/// columns, constants, comparisons of two values of the same (scalar) type,
/// and null tests, boolean connectives, CASE and COALESCE over those.
fn cannot_fail(e: &Expr, types: &[ColumnType]) -> bool {
    use crate::types::SqlType as T;
    let ok = |x: &Expr| cannot_fail(x, types);
    // comparing values of one type cannot fail (no casts, no overflow)
    let comparable = |xs: &[&Expr]| {
        let t = xs[0].ty(types);
        matches!(
            t,
            T::Bool | T::Int2 | T::Int4 | T::Int8 | T::Float4 | T::Float8 | T::Numeric | T::Text | T::Bytea | T::Date | T::Time
                | T::Timestamp | T::TimestampTz | T::Interval | T::Uuid | T::Oid
        ) && xs.iter().all(|x| x.ty(types) == t && ok(x))
    };
    match e {
        Expr::Column(_) | Expr::Literal(..) => true,
        Expr::Not(x) | Expr::IsNull(x) | Expr::IsNotNull(x) | Expr::BoolTest(x, _) => ok(x),
        Expr::And(xs) | Expr::Or(xs) | Expr::Coalesce(xs, _) => xs.iter().all(ok),
        Expr::Case(c) => c.whens.iter().all(|(w, t)| ok(w) && ok(t)) && ok(&c.else_),
        Expr::IsDistinct(a, b, _) | Expr::NullIf(a, b) => comparable(&[a, b]),
        Expr::InList(x, list, _) => comparable(&std::iter::once(&**x).chain(list).collect::<Vec<_>>()),
        Expr::Call(c) => {
            matches!(crate::expr::func::func_def(c.func).name, "=" | "<>" | "<" | "<=" | ">" | ">=")
                && c.args.len() == 2
                && comparable(&[&c.args[0], &c.args[1]])
        }
        _ => false,
    }
}
