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
