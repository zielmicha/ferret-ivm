//! jsonb operators: ports of JsonbDeepContains() (jsonb_util.c),
//! jsonb_concat/jsonb_delete*/jsonb_set/jsonb_insert/setPath*()
//! (jsonfuncs.c) of PostgreSQL 17 (Copyright (c) 1996-2024, PostgreSQL
//! Global Development Group; PostgreSQL License).
//!
//! The recursive algorithms of PostgreSQL are implemented iteratively (same
//! evaluation order), so deep values cannot overflow the Rust stack; the
//! `check_stack_depth()` limits PostgreSQL hits with its default 2MB
//! `max_stack_depth` are emulated where the API can report errors.

use super::parser::stack_depth_error;
use super::{Jsonb, find_key, normalize_pairs};
use crate::{Error, Result, sqlstate};

/// strtoint(): C `strtol(s, &end, 10)` that must consume the whole string
/// and fit in int32.
pub(crate) fn strtoint(s: &str) -> Option<i32> {
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() && matches!(b[i], b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r') {
        i += 1;
    }
    let mut neg = false;
    if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
        neg = b[i] == b'-';
        i += 1;
    }
    let start = i;
    let mut v: i64 = 0;
    while i < b.len() && b[i].is_ascii_digit() {
        v = v.saturating_mul(10).saturating_add(i64::from(b[i] - b'0'));
        i += 1;
    }
    if i == start || i != b.len() {
        return None;
    }
    let v = if neg { -v } else { v };
    i32::try_from(v).ok()
}

pub(crate) fn some_path<'a>(path: &[&'a str]) -> Vec<Option<&'a str>> {
    path.iter().map(|p| Some(*p)).collect()
}

#[inline]
fn is_scalar(v: &Jsonb) -> bool {
    !matches!(v, Jsonb::Array(_) | Jsonb::Object(_))
}

/// equalsJsonbScalarValue() for same-typed scalars; false otherwise.
fn scalar_eq(a: &Jsonb, b: &Jsonb) -> bool {
    match (a, b) {
        (Jsonb::Null, Jsonb::Null) => true,
        (Jsonb::String(x), Jsonb::String(y)) => x == y,
        (Jsonb::Number(x), Jsonb::Number(y)) => x == y,
        (Jsonb::Bool(x), Jsonb::Bool(y)) => x == y,
        _ => false,
    }
}

/// Elements of a root array; a raw scalar is a one-element array.
fn root_elems(v: &Jsonb) -> &[Jsonb] {
    match v {
        Jsonb::Array(a) => a,
        Jsonb::Object(_) => &[],
        s => std::slice::from_ref(s),
    }
}

fn root_count(v: &Jsonb) -> usize {
    match v {
        Jsonb::Array(a) => a.len(),
        Jsonb::Object(p) => p.len(),
        _ => 1,
    }
}

// ------------------------------------------------------------- containment

/// Nesting of JsonbDeepContains() calls at which PostgreSQL 17 hits
/// `check_stack_depth()` (default 2MB `max_stack_depth`, top-level query).
pub(crate) const CONTAINS_MAX_DEPTH: usize = 8180;

/// One JsonbDeepContains() activation.
enum Task<'a> {
    /// rhs pairs `ri..` still to be checked
    Obj {
        lhs: &'a [(String, Jsonb)],
        rhs: &'a [(String, Jsonb)],
        ri: usize,
    },
    /// rhs elements `ri..` still to be checked; `li` is the lhs candidate
    /// being tried for a container element `rhs[ri]`
    Arr {
        lhs: &'a [Jsonb],
        rhs: &'a [Jsonb],
        ri: usize,
        li: usize,
    },
}

enum Step<'a> {
    Done(bool),
    /// call JsonbDeepContains on two nested containers (None: their kinds
    /// differ, so the call returns false right after its stack check)
    Call(Option<Task<'a>>),
}

/// Advances `t` given the result of its last nested call.
fn step<'a>(t: &mut Task<'a>, ret: Option<bool>) -> Step<'a> {
    match t {
        Task::Obj { lhs, rhs, ri } => {
            let (lhs, rhs) = (*lhs, *rhs);
            match ret {
                Some(false) => return Step::Done(false),
                Some(true) => *ri += 1,
                None => {
                    if *ri == 0 && lhs.len() < rhs.len() {
                        return Step::Done(false);
                    }
                }
            }
            loop {
                let Some((k, rv)) = rhs.get(*ri) else {
                    return Step::Done(true);
                };
                let Some(i) = find_key(lhs, k) else {
                    return Step::Done(false);
                };
                let lv = &lhs[i].1;
                if is_scalar(lv) != is_scalar(rv) {
                    return Step::Done(false);
                }
                if !is_scalar(lv) {
                    return Step::Call(task(lv, rv));
                }
                if !scalar_eq(lv, rv) {
                    return Step::Done(false);
                }
                *ri += 1;
            }
        }
        Task::Arr { lhs, rhs, ri, li } => {
            let (lhs, rhs) = (*lhs, *rhs);
            match ret {
                Some(true) => {
                    *ri += 1;
                    *li = 0;
                }
                Some(false) => *li += 1,
                None => {}
            }
            loop {
                let Some(rv) = rhs.get(*ri) else {
                    return Step::Done(true);
                };
                if is_scalar(rv) {
                    if !lhs.iter().any(|lv| scalar_eq(lv, rv)) {
                        return Step::Done(false);
                    }
                    *ri += 1;
                    *li = 0;
                    continue;
                }
                // try the next container element of lhs
                match lhs[*li..].iter().position(|lv| !is_scalar(lv)) {
                    None => return Step::Done(false),
                    Some(off) => {
                        *li += off;
                        return Step::Call(task(&lhs[*li], rv));
                    }
                }
            }
        }
    }
}

fn task<'a>(lv: &'a Jsonb, rv: &'a Jsonb) -> Option<Task<'a>> {
    match (lv, rv) {
        (Jsonb::Object(l), Jsonb::Object(r)) => Some(Task::Obj {
            lhs: l,
            rhs: r,
            ri: 0,
        }),
        (Jsonb::Array(l), Jsonb::Array(r)) => Some(Task::Arr {
            lhs: l,
            rhs: r,
            ri: 0,
            li: 0,
        }),
        _ => None,
    }
}

/// jsonb_contains (`@>`). With `max_depth`, nesting deeper than that raises
/// "stack depth limit exceeded" as in PostgreSQL.
pub(crate) fn contains(val: &Jsonb, tmpl: &Jsonb, max_depth: Option<usize>) -> Result<bool> {
    if matches!(val, Jsonb::Object(_)) != matches!(tmpl, Jsonb::Object(_)) {
        return Ok(false);
    }
    let root = match (val, tmpl) {
        (Jsonb::Object(l), Jsonb::Object(r)) => Task::Obj {
            lhs: l,
            rhs: r,
            ri: 0,
        },
        _ => {
            // arrays or raw scalars (pseudo one-element arrays); a raw scalar
            // cannot contain a real array
            if is_scalar(val) && !is_scalar(tmpl) {
                return Ok(false);
            }
            Task::Arr {
                lhs: root_elems(val),
                rhs: root_elems(tmpl),
                ri: 0,
                li: 0,
            }
        }
    };
    let mut stack = vec![root];
    let mut ret: Option<bool> = None;
    while let Some(top) = stack.last_mut() {
        match step(top, ret.take()) {
            Step::Done(r) => {
                stack.pop();
                ret = Some(r);
            }
            Step::Call(t) => {
                if max_depth.is_some_and(|m| stack.len() + 1 > m) {
                    return Err(stack_depth_error());
                }
                match t {
                    Some(t) => stack.push(t),
                    None => ret = Some(false),
                }
            }
        }
    }
    Ok(ret.unwrap_or(true))
}

// ------------------------------------------------------------------ concat

/// jsonb_concat (`||`) / IteratorConcat()
pub(crate) fn concat(a: &Jsonb, b: &Jsonb) -> Jsonb {
    let ao = matches!(a, Jsonb::Object(_));
    let bo = matches!(b, Jsonb::Object(_));
    if ao == bo {
        if root_count(a) == 0 && !is_scalar(b) {
            return b.clone();
        } else if root_count(b) == 0 && !is_scalar(a) {
            return a.clone();
        }
    }
    match (a, b) {
        (Jsonb::Object(x), Jsonb::Object(y)) => {
            let mut pairs = Vec::with_capacity(x.len() + y.len());
            pairs.extend(x.iter().cloned());
            pairs.extend(y.iter().cloned());
            Jsonb::Object(normalize_pairs(pairs))
        }
        (Jsonb::Object(_), _) => {
            let mut elems = Vec::with_capacity(1 + root_count(b));
            elems.push(a.clone());
            elems.extend(root_elems(b).iter().cloned());
            Jsonb::Array(elems)
        }
        (_, Jsonb::Object(_)) => {
            let mut elems = Vec::with_capacity(1 + root_count(a));
            elems.extend(root_elems(a).iter().cloned());
            elems.push(b.clone());
            Jsonb::Array(elems)
        }
        _ => {
            let mut elems = Vec::with_capacity(root_count(a) + root_count(b));
            elems.extend(root_elems(a).iter().cloned());
            elems.extend(root_elems(b).iter().cloned());
            Jsonb::Array(elems)
        }
    }
}

// ------------------------------------------------------------------ delete

fn err(msg: &str) -> Error {
    Error::new(sqlstate::INVALID_PARAMETER_VALUE, msg)
}

/// jsonb_delete (`-` text)
pub(crate) fn delete_key(v: &Jsonb, key: &str) -> Result<Jsonb> {
    match v {
        Jsonb::Object(p) => {
            if p.is_empty() {
                return Ok(v.clone());
            }
            Ok(Jsonb::Object(
                p.iter().filter(|(k, _)| k != key).cloned().collect(),
            ))
        }
        Jsonb::Array(a) => {
            if a.is_empty() {
                return Ok(v.clone());
            }
            Ok(Jsonb::Array(
                a.iter()
                    .filter(|e| !matches!(e, Jsonb::String(s) if s == key))
                    .cloned()
                    .collect(),
            ))
        }
        _ => Err(err("cannot delete from scalar")),
    }
}

/// jsonb_delete_idx (`-` int)
pub(crate) fn delete_index(v: &Jsonb, idx: i64) -> Result<Jsonb> {
    match v {
        Jsonb::Array(a) => {
            if a.is_empty() {
                return Ok(v.clone());
            }
            let n = a.len() as i64;
            let idx = if idx < 0 {
                if -idx > n { n } else { n + idx }
            } else {
                idx
            };
            if idx >= n {
                return Ok(v.clone());
            }
            let mut out = Vec::with_capacity(a.len() - 1);
            out.extend(a[..idx as usize].iter().cloned());
            out.extend(a[idx as usize + 1..].iter().cloned());
            Ok(Jsonb::Array(out))
        }
        Jsonb::Object(_) => Err(err("cannot delete from object using integer index")),
        _ => Err(err("cannot delete from scalar")),
    }
}

// ----------------------------------------------------------------- setPath

/// setPath() recursion level at which PostgreSQL 17 hits
/// `check_stack_depth()` (default 2MB `max_stack_depth`, top-level query).
pub(crate) const SETPATH_MAX_LEVEL: usize = 6886;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Op {
    Create,
    Delete,
    Replace,
    InsertBefore,
    InsertAfter,
}

impl Op {
    fn create_or_insert(self) -> bool {
        matches!(self, Op::Create | Op::InsertBefore | Op::InsertAfter)
    }
}

/// jsonb_delete_path (`#-`)
pub(crate) fn delete_path(v: &Jsonb, path: &[Option<&str>]) -> Result<Jsonb> {
    if is_scalar(v) {
        return Err(err("cannot delete path in scalar"));
    }
    if root_count(v) == 0 || path.is_empty() {
        return Ok(v.clone());
    }
    set_path(v, path, None, Op::Delete)
}

/// jsonb_set
pub(crate) fn set(v: &Jsonb, path: &[Option<&str>], newval: &Jsonb, create: bool) -> Result<Jsonb> {
    if is_scalar(v) {
        return Err(err("cannot set path in scalar"));
    }
    if (root_count(v) == 0 && !create) || path.is_empty() {
        return Ok(v.clone());
    }
    set_path(
        v,
        path,
        Some(newval),
        if create { Op::Create } else { Op::Replace },
    )
}

/// jsonb_insert
pub(crate) fn insert(
    v: &Jsonb,
    path: &[Option<&str>],
    newval: &Jsonb,
    after: bool,
) -> Result<Jsonb> {
    if is_scalar(v) {
        return Err(err("cannot set path in scalar"));
    }
    if path.is_empty() {
        return Ok(v.clone());
    }
    set_path(
        v,
        path,
        Some(newval),
        if after {
            Op::InsertAfter
        } else {
            Op::InsertBefore
        },
    )
}

/// What setPath() does at one level.
enum Level<'a> {
    /// recurse into child number `i` (array index / pair index)
    Into(usize, &'a Jsonb),
    /// the level's result, computed without recursing further
    Result(Jsonb),
}

/// setPath(): walks down the path (performing PostgreSQL's checks in the same
/// order), then rebuilds the containers along the path bottom-up.
fn set_path(root: &Jsonb, path: &[Option<&str>], newval: Option<&Jsonb>, op: Op) -> Result<Jsonb> {
    let mut trail: Vec<(&Jsonb, usize)> = Vec::new();
    let mut cur = root;
    let mut level = 0;
    let mut result = loop {
        if level >= SETPATH_MAX_LEVEL {
            return Err(stack_depth_error());
        }
        let Some(elem) = path[level] else {
            return Err(Error::new(
                sqlstate::NULL_VALUE_NOT_ALLOWED,
                format!("path element at position {} is null", level + 1),
            ));
        };
        let step = match cur {
            Jsonb::Array(a) => set_path_array(a, elem, path.len(), level, newval, op)?,
            Jsonb::Object(p) => set_path_object(p, elem, path.len(), level, newval, op)?,
            scalar => Level::Result(scalar.clone()),
        };
        match step {
            Level::Into(i, child) => {
                trail.push((cur, i));
                cur = child;
                level += 1;
            }
            Level::Result(r) => break r,
        }
    };
    while let Some((container, i)) = trail.pop() {
        result = match container {
            Jsonb::Array(a) => {
                let mut out = Vec::with_capacity(a.len());
                out.extend(a[..i].iter().cloned());
                out.push(result);
                out.extend(a[i + 1..].iter().cloned());
                Jsonb::Array(out)
            }
            Jsonb::Object(p) => {
                let mut out = Vec::with_capacity(p.len());
                out.extend(p[..i].iter().cloned());
                out.push((p[i].0.clone(), result));
                out.extend(p[i + 1..].iter().cloned());
                Jsonb::Object(out)
            }
            _ => unreachable!(),
        };
    }
    Ok(result)
}

/// setPathObject()
fn set_path_object<'a>(
    pairs: &'a [(String, Jsonb)],
    elem: &str,
    path_len: usize,
    level: usize,
    newval: Option<&Jsonb>,
    op: Op,
) -> Result<Level<'a>> {
    let last = level == path_len - 1;
    match find_key(pairs, elem) {
        Some(i) if !last => Ok(Level::Into(i, &pairs[i].1)),
        Some(i) => {
            if matches!(op, Op::InsertBefore | Op::InsertAfter) {
                return Err(err("cannot replace existing key"));
            }
            let mut out: Vec<(String, Jsonb)> = Vec::with_capacity(pairs.len());
            for (j, (k, v)) in pairs.iter().enumerate() {
                if j != i {
                    out.push((k.clone(), v.clone()));
                } else if op != Op::Delete {
                    out.push((k.clone(), newval.unwrap().clone()));
                }
            }
            Ok(Level::Result(Jsonb::Object(out)))
        }
        None => {
            let mut out: Vec<(String, Jsonb)> = pairs.to_vec();
            if op.create_or_insert() && last {
                out.push((elem.to_owned(), newval.unwrap().clone()));
                out = normalize_pairs(out);
            }
            Ok(Level::Result(Jsonb::Object(out)))
        }
    }
}

/// setPathArray()
fn set_path_array<'a>(
    elems: &'a [Jsonb],
    elem: &str,
    path_len: usize,
    level: usize,
    newval: Option<&Jsonb>,
    op: Op,
) -> Result<Level<'a>> {
    let last = level == path_len - 1;
    let nelems = elems.len() as i64;
    let Some(idx) = strtoint(elem) else {
        return Err(Error::new(
            sqlstate::INVALID_TEXT_REPRESENTATION,
            format!(
                "path element at position {} is not an integer: \"{}\"",
                level + 1,
                elem
            ),
        ));
    };
    let mut idx = i64::from(idx);
    // a negative index before the start becomes INT_MIN: prepend
    let prepend = idx < 0 && -idx > nelems;
    if idx < 0 && !prepend {
        idx += nelems;
    }
    if idx > nelems {
        idx = nelems;
    }
    let hit = !prepend && idx < nelems;
    if hit && !last {
        return Ok(Level::Into(idx as usize, &elems[idx as usize]));
    }
    let mut out: Vec<Jsonb> = Vec::with_capacity(elems.len() + 1);
    let mut done = false;
    if (prepend || nelems == 0) && last && op.create_or_insert() {
        out.push(newval.unwrap().clone());
        done = true;
    }
    for (i, e) in elems.iter().enumerate() {
        if hit && i as i64 == idx {
            // last level
            done = true;
            if matches!(op, Op::InsertBefore | Op::Create) {
                out.push(newval.unwrap().clone());
            }
            if matches!(op, Op::InsertAfter | Op::InsertBefore) {
                out.push(e.clone());
            }
            if matches!(op, Op::InsertAfter | Op::Replace) {
                out.push(newval.unwrap().clone());
            }
        } else {
            out.push(e.clone());
        }
    }
    if op.create_or_insert() && !done && last {
        out.push(newval.unwrap().clone());
    }
    Ok(Level::Result(Jsonb::Array(out)))
}
