//! Function and operator registry with PostgreSQL-style overload resolution.

use super::cast::implicit_castable;
use crate::error::{codes, Result, SqlError};
use crate::types::{Datum, SqlType};
use rustc_hash::FxHashMap;
use std::sync::OnceLock;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FuncId(pub u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Volatility {
    Immutable,
    Stable,
    Volatile,
}

/// Parameter type pattern.
#[derive(Clone, Debug, PartialEq)]
pub enum P {
    T(SqlType),
    /// "any": accepts anything, no unification
    Any,
    AnyElement,
    AnyNonArray,
    AnyArray,
}

/// Return type rule.
#[derive(Clone, Debug, PartialEq)]
pub enum Ret {
    T(SqlType),
    /// Type of the polymorphic element (anyelement / element of anyarray).
    Elem,
    /// Array of the polymorphic element.
    ArrayOfElem,
}

pub type Fn1 = fn(&Datum) -> Result<Datum>;
pub type Fn2 = fn(&Datum, &Datum) -> Result<Datum>;
pub type FnN = fn(&[Datum]) -> Result<Datum>;

#[derive(Clone, Copy)]
pub enum Imp {
    F1(Fn1),
    F2(Fn2),
    FN(FnN),
}

pub struct FuncDef {
    pub name: &'static str,
    pub params: Vec<P>,
    pub variadic: bool,
    pub ret: Ret,
    pub imp: Imp,
    /// NULL in any argument yields NULL without calling.
    pub strict: bool,
    /// May return NULL for non-null arguments.
    pub can_return_null: bool,
    pub volatility: Volatility,
    pub operator: bool,
}

impl FuncDef {
    pub fn is_operator(&self) -> bool {
        self.operator
    }
}

pub struct Registry {
    pub funcs: Vec<FuncDef>,
    by_name: FxHashMap<&'static str, Vec<FuncId>>,
}

static REGISTRY: OnceLock<Registry> = OnceLock::new();

pub fn registry() -> &'static Registry {
    REGISTRY.get_or_init(|| {
        let mut b = Builder { funcs: Vec::new() };
        super::ops::register(&mut b);
        super::builtins::register(&mut b);
        let mut by_name: FxHashMap<&'static str, Vec<FuncId>> = FxHashMap::default();
        for (i, f) in b.funcs.iter().enumerate() {
            by_name.entry(f.name).or_default().push(FuncId(i as u32));
        }
        Registry { funcs: b.funcs, by_name }
    })
}

pub struct Builder {
    pub funcs: Vec<FuncDef>,
}

impl Builder {
    pub fn add(&mut self, def: FuncDef) {
        self.funcs.push(def);
    }

    fn base(name: &'static str, params: Vec<P>, ret: Ret, imp: Imp, operator: bool) -> FuncDef {
        FuncDef {
            name,
            params,
            variadic: false,
            ret,
            imp,
            strict: true,
            can_return_null: false,
            volatility: Volatility::Immutable,
            operator,
        }
    }

    pub fn op2(&mut self, name: &'static str, a: SqlType, b: SqlType, ret: SqlType, f: Fn2) {
        self.add(Self::base(name, vec![P::T(a), P::T(b)], Ret::T(ret), Imp::F2(f), true));
    }
    pub fn op1(&mut self, name: &'static str, a: SqlType, ret: SqlType, f: Fn1) {
        self.add(Self::base(name, vec![P::T(a)], Ret::T(ret), Imp::F1(f), true));
    }
    pub fn op2p(&mut self, name: &'static str, a: P, b: P, ret: Ret, f: Fn2) {
        self.add(Self::base(name, vec![a, b], ret, Imp::F2(f), true));
    }
    pub fn f1(&mut self, name: &'static str, a: SqlType, ret: SqlType, f: Fn1) {
        self.add(Self::base(name, vec![P::T(a)], Ret::T(ret), Imp::F1(f), false));
    }
    pub fn f2(&mut self, name: &'static str, a: SqlType, b: SqlType, ret: SqlType, f: Fn2) {
        self.add(Self::base(name, vec![P::T(a), P::T(b)], Ret::T(ret), Imp::F2(f), false));
    }
    pub fn fnn(&mut self, name: &'static str, params: Vec<SqlType>, ret: SqlType, f: FnN) {
        self.add(Self::base(name, params.into_iter().map(P::T).collect(), Ret::T(ret), Imp::FN(f), false));
    }
    /// Generic registration.
    pub fn poly(&mut self, name: &'static str, params: Vec<P>, ret: Ret, imp: Imp) -> &mut FuncDef {
        self.add(Self::base(name, params, ret, imp, false));
        self.funcs.last_mut().unwrap()
    }
    pub fn last(&mut self) -> &mut FuncDef {
        self.funcs.last_mut().unwrap()
    }
}

/// Result of overload resolution.
#[derive(Clone, Debug)]
pub struct Resolved {
    pub func: FuncId,
    /// Type each argument must be coerced to.
    pub arg_types: Vec<SqlType>,
    pub ret: SqlType,
}

fn numeric_rank(t: &SqlType) -> Option<i32> {
    Some(match t {
        SqlType::Int2 => 0,
        SqlType::Int4 => 1,
        SqlType::Int8 => 2,
        SqlType::Numeric => 3,
        SqlType::Float4 => 4,
        SqlType::Float8 => 5,
        _ => return None,
    })
}

fn datetime_rank(t: &SqlType) -> Option<i32> {
    Some(match t {
        SqlType::Date => 0,
        SqlType::Timestamp => 1,
        SqlType::TimestampTz => 2,
        _ => return None,
    })
}

/// Cost of converting an argument of type `from` to parameter type `to`
/// (None = not possible).
fn conversion_cost(from: &SqlType, to: &SqlType) -> Option<i32> {
    if from == to {
        return Some(0);
    }
    if *from == SqlType::Unknown {
        return Some(if *to == SqlType::Text { 5 } else { 10 });
    }
    if !implicit_castable(from, to) {
        return None;
    }
    let base = if to.is_preferred() { 20 } else { 30 };
    let dist = match (numeric_rank(from), numeric_rank(to)) {
        (Some(a), Some(b)) => (b - a).abs(),
        _ => match (datetime_rank(from), datetime_rank(to)) {
            (Some(a), Some(b)) => (b - a).abs(),
            _ => 1,
        },
    };
    Some(base + dist)
}

/// Try to match `args` against `def`; returns (cost, coerced types, ret).
fn try_match(def: &FuncDef, args: &[SqlType]) -> Option<(i32, Vec<SqlType>, SqlType)> {
    let n = def.params.len();
    if def.variadic {
        if args.len() < n.saturating_sub(1) {
            return None;
        }
    } else if args.len() != n {
        return None;
    }
    let param = |i: usize| -> &P { if i < n { &def.params[i] } else { &def.params[n - 1] } };
    // unify polymorphic element type
    let mut elem: Option<SqlType> = None;
    for (i, a) in args.iter().enumerate() {
        match param(i) {
            P::AnyElement | P::AnyNonArray if *a != SqlType::Unknown => {
                if let P::AnyNonArray = param(i)
                    && matches!(a, SqlType::Array(_))
                {
                    return None;
                }
                match &elem {
                    None => elem = Some(a.clone()),
                    Some(e) if e == a => {}
                    Some(e) => {
                        // allow implicit promotion between candidates
                        if implicit_castable(a, e) {
                        } else if implicit_castable(e, a) {
                            elem = Some(a.clone());
                        } else {
                            return None;
                        }
                    }
                }
            }
            P::AnyArray if *a != SqlType::Unknown => match a {
                SqlType::Array(e) => match &elem {
                    None => elem = Some((**e).clone()),
                    Some(x) if x == &**e => {}
                    Some(x) => {
                        if implicit_castable(x, e) {
                            elem = Some((**e).clone());
                        } else if !implicit_castable(e, x) {
                            return None;
                        }
                    }
                },
                _ => return None,
            },
            _ => {}
        }
    }
    let has_poly = (0..args.len()).any(|i| matches!(param(i), P::AnyElement | P::AnyNonArray | P::AnyArray));
    if has_poly && elem.is_none() {
        // all polymorphic args unknown: resolve as text
        elem = Some(SqlType::Text);
    }
    let mut cost = 0;
    let mut types = Vec::with_capacity(args.len());
    for (i, a) in args.iter().enumerate() {
        let target = match param(i) {
            P::T(t) => t.clone(),
            P::Any => {
                cost += if *a == SqlType::Unknown { 10 } else { 15 };
                types.push(if *a == SqlType::Unknown { SqlType::Text } else { a.clone() });
                continue;
            }
            P::AnyElement | P::AnyNonArray => {
                cost += 15;
                elem.clone().unwrap()
            }
            P::AnyArray => {
                cost += 15;
                elem.clone().unwrap().array_of()
            }
        };
        cost += conversion_cost(a, &target)?;
        types.push(target);
    }
    let ret = match &def.ret {
        Ret::T(t) => t.clone(),
        Ret::Elem => elem.clone().unwrap_or(SqlType::Text),
        Ret::ArrayOfElem => elem.clone().unwrap_or(SqlType::Text).array_of(),
    };
    Some((cost, types, ret))
}

/// A matching overload: (cost, tie-breakers, function, argument types, result type).
type Candidate = (i32, (i32, i32), FuncId, Vec<SqlType>, SqlType);

/// Resolve a function or operator call by name and argument types.
pub fn resolve(name: &str, args: &[SqlType]) -> Result<Resolved> {
    let reg = registry();
    let lname = name.to_ascii_lowercase();
    let cands = reg.by_name.get(lname.as_str()).map(|v| v.as_slice()).unwrap_or(&[]);
    let is_op = cands.first().map(|c| reg.funcs[c.0 as usize].operator).unwrap_or_else(|| {
        !lname.chars().next().is_some_and(|c| c.is_alphabetic() || c == '_')
    });
    // binary operator with one unknown side: try (T, T) with the known type first
    if is_op && args.len() == 2 {
        let known = match (&args[0], &args[1]) {
            (SqlType::Unknown, t) | (t, SqlType::Unknown) if *t != SqlType::Unknown => Some(t.clone()),
            _ => None,
        };
        if let Some(t) = known {
            for c in cands {
                let def = &reg.funcs[c.0 as usize];
                if def.params == vec![P::T(t.clone()), P::T(t.clone())] {
                    let (_, types, ret) = try_match(def, &[t.clone(), t.clone()]).unwrap();
                    return Ok(Resolved { func: *c, arg_types: types, ret });
                }
            }
        }
    }
    let mut best: Option<Candidate> = None;
    for c in cands {
        let def = &reg.funcs[c.0 as usize];
        if let Some((cost, types, ret)) = try_match(def, args) {
            // tie-breakers: prefer text where unknown literals were given, then
            // the preferred type of the category (abs(unknown) is abs(float8))
            let unknown_as = |pred: &dyn Fn(&SqlType) -> bool| {
                args.iter().zip(&types).filter(|(a, t)| **a == SqlType::Unknown && pred(t)).count() as i32
            };
            let tie = (unknown_as(&|t| *t == SqlType::Text), unknown_as(&|t| t.is_preferred()));
            let better = match &best {
                None => true,
                Some((bc, bt, ..)) => cost < *bc || (cost == *bc && tie > *bt),
            };
            if better {
                best = Some((cost, tie, *c, types, ret));
            }
        }
    }
    match best {
        Some((_, _, func, arg_types, ret)) => Ok(Resolved { func, arg_types, ret }),
        None => {
            let tys = args.iter().map(|t| t.name()).collect::<Vec<_>>();
            if is_op {
                let msg = match tys.len() {
                    1 => format!("operator does not exist: {} {}", name, tys[0]),
                    _ => format!("operator does not exist: {} {} {}", tys[0], name, tys.get(1).cloned().unwrap_or_default()),
                };
                Err(SqlError::new(codes::UNDEFINED_FUNCTION, msg).with_hint(
                    "No operator matches the given name and argument types. You might need to add explicit type casts.",
                ))
            } else {
                Err(SqlError::new(
                    codes::UNDEFINED_FUNCTION,
                    format!("function {}({}) does not exist", name, tys.join(", ")),
                )
                .with_hint(
                    "No function matches the given name and argument types. You might need to add explicit type casts.",
                ))
            }
        }
    }
}

pub fn function_exists(name: &str) -> bool {
    registry().by_name.contains_key(name.to_ascii_lowercase().as_str())
}

pub fn func_def(id: FuncId) -> &'static FuncDef {
    &registry().funcs[id.0 as usize]
}
