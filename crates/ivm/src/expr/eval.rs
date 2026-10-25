//! Expression evaluation.

use super::cast::cast_datum;
use super::func::{func_def, Imp, Volatility};
use super::{BoolTest, Expr};
use crate::error::{codes, Result, SqlError};
use crate::types::{ArrayValue, Datum};
use std::cmp::Ordering;

/// Reserved for vectorized evaluation state.
#[derive(Default)]
pub struct EvalScratch;

impl Expr {
    /// Evaluate against one input row.
    pub fn eval(&self, row: &[Datum]) -> Result<Datum> {
        match self {
            Expr::Column(i) => Ok(row[*i].clone()),
            Expr::Literal(d, _) => Ok(d.clone()),
            Expr::Call(c) => {
                let def = func_def(c.func);
                match def.imp {
                    Imp::F1(f) => {
                        let a = c.args[0].eval(row)?;
                        if def.strict && a.is_null() {
                            return Ok(Datum::Null);
                        }
                        f(&a)
                    }
                    Imp::F2(f) => {
                        let a = c.args[0].eval(row)?;
                        if def.strict && a.is_null() {
                            return Ok(Datum::Null);
                        }
                        let b = c.args[1].eval(row)?;
                        if def.strict && b.is_null() {
                            return Ok(Datum::Null);
                        }
                        f(&a, &b)
                    }
                    Imp::FN(f) => {
                        let mut args = Vec::with_capacity(c.args.len());
                        for a in &c.args {
                            let v = a.eval(row)?;
                            if def.strict && v.is_null() {
                                return Ok(Datum::Null);
                            }
                            args.push(v);
                        }
                        f(&args)
                    }
                }
            }
            Expr::Cast(e, t, tm, explicit) => cast_datum(&e.eval(row)?, t, *tm, *explicit),
            Expr::Not(e) => Ok(match e.eval(row)? {
                Datum::Bool(b) => Datum::Bool(!b),
                _ => Datum::Null,
            }),
            Expr::And(v) => {
                let mut saw_null = false;
                for e in v {
                    match e.eval(row)? {
                        Datum::Bool(false) => return Ok(Datum::Bool(false)),
                        Datum::Bool(true) => {}
                        _ => saw_null = true,
                    }
                }
                Ok(if saw_null { Datum::Null } else { Datum::Bool(true) })
            }
            Expr::Or(v) => {
                let mut saw_null = false;
                for e in v {
                    match e.eval(row)? {
                        Datum::Bool(true) => return Ok(Datum::Bool(true)),
                        Datum::Bool(false) => {}
                        _ => saw_null = true,
                    }
                }
                Ok(if saw_null { Datum::Null } else { Datum::Bool(false) })
            }
            Expr::IsNull(e) => Ok(Datum::Bool(e.eval(row)?.is_null())),
            Expr::IsNotNull(e) => Ok(Datum::Bool(!e.eval(row)?.is_null())),
            Expr::BoolTest(e, t) => {
                let v = e.eval(row)?.as_bool();
                Ok(Datum::Bool(match t {
                    BoolTest::IsTrue => v == Some(true),
                    BoolTest::IsNotTrue => v != Some(true),
                    BoolTest::IsFalse => v == Some(false),
                    BoolTest::IsNotFalse => v != Some(false),
                    BoolTest::IsUnknown => v.is_none(),
                    BoolTest::IsNotUnknown => v.is_some(),
                }))
            }
            Expr::IsDistinct(a, b, negated) => {
                let (x, y) = (a.eval(row)?, b.eval(row)?);
                let distinct = match (x.is_null(), y.is_null()) {
                    (true, true) => false,
                    (true, false) | (false, true) => true,
                    _ => !x.sql_eq(&y),
                };
                Ok(Datum::Bool(distinct != *negated))
            }
            Expr::Case(c) => {
                for (w, t) in &c.whens {
                    if let Datum::Bool(true) = w.eval(row)? {
                        return t.eval(row);
                    }
                }
                c.else_.eval(row)
            }
            Expr::Coalesce(v, _) => {
                for e in v {
                    let d = e.eval(row)?;
                    if !d.is_null() {
                        return Ok(d);
                    }
                }
                Ok(Datum::Null)
            }
            Expr::NullIf(a, b) => {
                let x = a.eval(row)?;
                let y = b.eval(row)?;
                if !x.is_null() && !y.is_null() && x.sql_eq(&y) { Ok(Datum::Null) } else { Ok(x) }
            }
            Expr::Greatest(v, _) | Expr::Least(v, _) => {
                let want = if matches!(self, Expr::Greatest(..)) { Ordering::Greater } else { Ordering::Less };
                let mut best = Datum::Null;
                for e in v {
                    let d = e.eval(row)?;
                    if d.is_null() {
                        continue;
                    }
                    if best.is_null() || d.sql_cmp(&best) == want {
                        best = d;
                    }
                }
                Ok(best)
            }
            Expr::InList(e, list, negated) => {
                let x = e.eval(row)?;
                if x.is_null() {
                    return Ok(Datum::Null);
                }
                let mut saw_null = false;
                for item in list {
                    let y = item.eval(row)?;
                    if y.is_null() {
                        saw_null = true;
                    } else if x.sql_eq(&y) {
                        return Ok(Datum::Bool(!negated));
                    }
                }
                Ok(if saw_null { Datum::Null } else { Datum::Bool(*negated) })
            }
            Expr::Like(l) => {
                let v = l.expr.eval(row)?;
                if v.is_null() {
                    return Ok(Datum::Null);
                }
                let m = if l.bytea {
                    let p = l.pattern.eval(row)?;
                    if p.is_null() {
                        return Ok(Datum::Null);
                    }
                    pgfuncs::pattern::like_bytes(v.as_bytes().unwrap(), p.as_bytes().unwrap(), l.escape.map(|c| c as u8))?
                } else if let Some(c) = &l.compiled {
                    c.try_matches(v.as_str().unwrap())?
                } else {
                    let p = l.pattern.eval(row)?;
                    if p.is_null() {
                        return Ok(Datum::Null);
                    }
                    pgfuncs::pattern::LikePattern::new_lenient(p.as_str().unwrap(), l.escape, l.case_insensitive)
                        .try_matches(v.as_str().unwrap())?
                };
                Ok(Datum::Bool(m != l.negated))
            }
            Expr::Array(v, elem) => {
                let mut elems = Vec::with_capacity(v.len());
                for e in v {
                    elems.push(e.eval(row)?);
                }
                Ok(Datum::array(match elem {
                    // ARRAY[ARRAY[..], ..]: one more dimension
                    crate::types::SqlType::Array(inner) => ArrayValue::stack((**inner).clone(), &elems)?,
                    _ => ArrayValue::new_1d(elem.clone(), elems),
                }))
            }
            Expr::Subscript(a, idx, _) => {
                let av = a.eval(row)?;
                let iv = idx.eval(row)?;
                match (&av, iv.as_i64()) {
                    (Datum::Array(arr), Some(k)) => Ok(arr.get(&[k])),
                    _ => Ok(Datum::Null),
                }
            }
            Expr::AnyAll { expr, array, func, all } => {
                let x = expr.eval(row)?;
                let av = array.eval(row)?;
                let Datum::Array(arr) = av else { return Ok(Datum::Null) };
                let def = func_def(*func);
                let Imp::F2(f) = def.imp else { return Err(SqlError::internal("ANY/ALL operator")) };
                let mut saw_null = false;
                for e in &arr.elems {
                    let r = if x.is_null() || e.is_null() { Datum::Null } else { f(&x, e)? };
                    match (r.as_bool(), all) {
                        (Some(true), false) => return Ok(Datum::Bool(true)),
                        (Some(false), true) => return Ok(Datum::Bool(false)),
                        (None, _) => saw_null = true,
                        _ => {}
                    }
                }
                Ok(if saw_null { Datum::Null } else { Datum::Bool(*all) })
            }
            Expr::Error(code, msg) => Err(SqlError::new(code, msg.clone())),
            Expr::Outer(..) | Expr::Subquery(_) | Expr::AggRef(..) | Expr::WindowRef(..) | Expr::SrfRef(..) => {
                Err(SqlError::internal(format!("unplanned expression {self}")))
            }
        }
    }

    /// Evaluate as a predicate: NULL counts as false.
    #[inline]
    pub fn eval_pred(&self, row: &[Datum]) -> Result<bool> {
        Ok(matches!(self.eval(row)?, Datum::Bool(true)))
    }

    /// Fold constant subexpressions (no column refs, immutable functions).
    ///
    /// Like PostgreSQL's eval_const_expressions, parts that a constant makes
    /// unreachable are dropped without being folded, so their errors do not
    /// surface: CASE branches with a constant false condition (and anything
    /// after a constant true one), COALESCE arguments after a non-null
    /// constant, AND / OR arguments after a constant false / true.
    pub fn fold(self) -> Result<Expr> {
        let mut e = self;
        match e {
            Expr::And(v) => {
                let mut out = Vec::new();
                for x in v {
                    match x.fold()? {
                        Expr::Literal(Datum::Bool(true), _) => {}
                        Expr::Literal(Datum::Bool(false), _) => return Ok(Expr::bool(false)),
                        Expr::And(inner) => out.extend(inner),
                        other => out.push(other),
                    }
                }
                return Ok(match out.len() {
                    0 => Expr::bool(true),
                    1 => out.pop().unwrap(),
                    _ => Expr::And(out),
                });
            }
            Expr::Or(v) => {
                let mut out = Vec::new();
                for x in v {
                    match x.fold()? {
                        Expr::Literal(Datum::Bool(false), _) => {}
                        Expr::Literal(Datum::Bool(true), _) => return Ok(Expr::bool(true)),
                        Expr::Or(inner) => out.extend(inner),
                        other => out.push(other),
                    }
                }
                return Ok(match out.len() {
                    0 => Expr::bool(false),
                    1 => out.pop().unwrap(),
                    _ => Expr::Or(out),
                });
            }
            Expr::Case(mut c) => {
                let mut whens = Vec::new();
                let mut else_ = None;
                for (w, t) in std::mem::take(&mut c.whens) {
                    match w.fold()? {
                        // a false or NULL condition: the branch is unreachable
                        Expr::Literal(d, _) if !matches!(d, Datum::Bool(true)) => {}
                        // a true condition ends the CASE
                        Expr::Literal(..) => {
                            else_ = Some(t.fold()?);
                            break;
                        }
                        w => whens.push((w, t.fold()?)),
                    }
                }
                let else_ = match else_ {
                    Some(t) => t,
                    None => c.else_.fold()?,
                };
                if whens.is_empty() {
                    return Ok(else_);
                }
                c.whens = whens;
                c.else_ = else_;
                return Ok(Expr::Case(c));
            }
            Expr::Coalesce(v, t) => {
                let mut out = Vec::new();
                for x in v {
                    match x.fold()? {
                        Expr::Literal(Datum::Null, _) => {}
                        // later arguments are unreachable
                        l @ Expr::Literal(..) => {
                            out.push(l);
                            break;
                        }
                        other => out.push(other),
                    }
                }
                return Ok(match out.len() {
                    0 => Expr::Literal(Datum::Null, t),
                    1 if out[0].is_literal() => out.pop().unwrap(),
                    _ => Expr::Coalesce(out, t),
                });
            }
            _ => {}
        }
        for c in e.children_mut() {
            let taken = std::mem::replace(c, Expr::Literal(Datum::Null, crate::types::SqlType::Unknown));
            *c = taken.fold()?;
        }
        let foldable = !matches!(
            e,
            Expr::Literal(..)
                | Expr::Column(_)
                | Expr::Error(..)
                | Expr::Outer(..)
                | Expr::Subquery(_)
                | Expr::AggRef(..)
                | Expr::WindowRef(..)
                | Expr::SrfRef(..)
        )
            && e.children().iter().all(|c| c.is_literal())
            && match &e {
                Expr::Call(c) => func_def(c.func).volatility == Volatility::Immutable,
                _ => true,
            };
        if foldable {
            let ty = e.ty(&[]);
            match e.eval(&[]) {
                Ok(d) => return Ok(Expr::Literal(d, ty)),
                Err(err) => {
                    // keep runtime semantics for data-dependent errors inside
                    // CASE etc.; constant errors surface at plan time like PG
                    if err.code == codes::INTERNAL_ERROR {
                        return Ok(e);
                    }
                    return Err(err);
                }
            }
        }
        Ok(e)
    }
}
