//! json / jsonb with PostgreSQL 17 semantics.
//!
//! Ported from PostgreSQL 17 (src/common/jsonapi.c and
//! src/backend/utils/adt/{json.c,jsonb.c,jsonb_util.c,jsonb_op.c,
//! jsonfuncs.c,numeric.c}), Copyright (c) 1996-2024, PostgreSQL Global
//! Development Group, Portions Copyright (c) 1994, The Regents of the
//! University of California, used under the PostgreSQL License.
//!
//! A [`Jsonb`] value always represents a whole jsonb datum: a top-level
//! scalar is PostgreSQL's "raw scalar" (a pseudo one-element array). Object
//! keys are unique and stored in jsonb order (shorter keys first, then
//! bytewise); every constructor in this module maintains that invariant, and
//! [`Jsonb::build_object`] / [`Jsonb::normalize`] establish it for values
//! assembled by hand.
//!
//! The [`path`] submodule implements `jsonpath` (parsing, output and
//! execution); its main items are re-exported here.
//!
//! Additions to the planned API: [`Jsonb::try_contains`] (with PostgreSQL's
//! recursion-depth error), [`Jsonb::set_opt`] / [`Jsonb::insert_opt`] /
//! [`Jsonb::delete_path_opt`] (SQL NULL path elements, which PostgreSQL
//! rejects lazily), [`Jsonb::object_entries_named`] (`jsonb_each_text`
//! error text), [`Jsonb::normalize`], `Hash` impls consistent with `Eq`.
//!
//! Notes and known deviations from PostgreSQL:
//! - Errors carry SQLSTATE, message and detail; PostgreSQL's CONTEXT line
//!   ("JSON data, line 1: ...") and HINTs are not represented.
//! - PostgreSQL's recursion limits come from `check_stack_depth()` (2MB
//!   default `max_stack_depth`) and depend on the caller's stack usage. They
//!   are emulated with values measured on PostgreSQL 17 in a top-level query:
//!   input nesting about 16354 arrays / 14537 objects (json/jsonb),
//!   `jsonb_set`/`jsonb_insert`/`#-` paths of about 6886 levels, and
//!   containment nesting of about 8180 levels ([`Jsonb::try_contains`];
//!   [`Jsonb::contains`] cannot fail and just answers).
//! - All algorithms here are iterative, but the compiler-generated `Drop` of
//!   a value nested N levels deep recurses: about 130 bytes of stack per
//!   level (opt-level >= 1, ~200 at opt-level 0), i.e. ~2MB for the deepest
//!   documents PostgreSQL accepts. Threads that may drop such values should
//!   have stacks of 4MB or more.
//! - Limits on the total size of a jsonb container (256MB) are not enforced;
//!   the per-string limit (256MB) is.
//! - [`JsonNumber::parse`] rejects NaN/Infinity (valid `numeric`s but not
//!   JSON numbers; PostgreSQL's `to_jsonb` makes them strings).
//! - [`Jsonb::decode_key`] returns numbers with the minimal display scale
//!   (`1.50` comes back as `1.5`), since equal values encode identically.

mod keyenc;
mod number;
mod ops;
mod output;
mod parser;
pub mod path;
mod text;

pub use number::JsonNumber;
pub use path::{
    JsonPath, PathContext, SessionTz, path_exists, path_exists_op, path_exists_tz, path_match,
    path_match_op, path_match_tz, path_query, path_query_array, path_query_array_tz,
    path_query_first, path_query_first_tz, path_query_tz,
};
pub use text::{
    escape_string, json_text_as_text, json_text_get_index, json_text_get_key, validate_json,
};

use crate::{Error, Result, sqlstate};
use parser::{Lexer, Scalar, Sem, Tok};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

/// jsonb value. Object keys are unique and sorted in jsonb order (shorter
/// keys first, then bytewise); duplicate keys on input: last wins.
#[derive(Debug)]
pub enum Jsonb {
    Null,
    Bool(bool),
    Number(JsonNumber),
    String(String),
    Array(Vec<Jsonb>),
    Object(Vec<(String, Jsonb)>),
}

/// JENTRY_OFFLENMASK: maximum length of a jsonb string.
const MAX_STRING_LEN: usize = 0x0FFF_FFFF;

/// Order of object keys in storage (lengthCompareJsonbString()).
#[inline]
pub(crate) fn key_cmp(a: &str, b: &str) -> Ordering {
    a.len()
        .cmp(&b.len())
        .then_with(|| a.as_bytes().cmp(b.as_bytes()))
}

#[inline]
pub(crate) fn find_key(pairs: &[(String, Jsonb)], key: &str) -> Option<usize> {
    let _ = pairs.first()?;
    pairs.binary_search_by(|(k, _)| key_cmp(k, key)).ok()
}

/// Sorts pairs into jsonb key order, keeping the last of duplicate keys
/// (uniqueifyJsonbObject()).
pub(crate) fn normalize_pairs(mut pairs: Vec<(String, Jsonb)>) -> Vec<(String, Jsonb)> {
    if pairs.len() < 2 {
        return pairs;
    }
    if pairs
        .windows(2)
        .all(|w| key_cmp(&w[0].0, &w[1].0) == Ordering::Less)
    {
        return pairs;
    }
    pairs.sort_by(|a, b| key_cmp(&a.0, &b.0));
    let mut out: Vec<(String, Jsonb)> = Vec::with_capacity(pairs.len());
    for p in pairs {
        if let Some(last) = out.last_mut()
            && last.0 == p.0
        {
            *last = p;
            continue;
        }
        out.push(p);
    }
    out
}

fn check_string_len(len: usize) -> Result<()> {
    if len > MAX_STRING_LEN {
        return Err(Error::with_detail(
            sqlstate::PROGRAM_LIMIT_EXCEEDED,
            "string too long to represent as jsonb string",
            format!(
                "Due to an implementation restriction, jsonb strings cannot exceed {MAX_STRING_LEN} bytes."
            ),
        ));
    }
    Ok(())
}

/// jsonb_in semantic actions: builds the tree.
#[derive(Default)]
struct Builder {
    stack: Vec<Building>,
    result: Option<Jsonb>,
}

enum Building {
    Array(Vec<Jsonb>),
    Object(Vec<(String, Jsonb)>, String),
}

impl Builder {
    fn add(&mut self, v: Jsonb) {
        match self.stack.last_mut() {
            None => self.result = Some(v),
            Some(Building::Array(a)) => a.push(v),
            Some(Building::Object(p, key)) => p.push((std::mem::take(key), v)),
        }
    }
}

impl<'a> Sem<'a> for Builder {
    fn object_start(&mut self, _lex: &Lexer<'a>) -> Result<()> {
        self.stack.push(Building::Object(Vec::new(), String::new()));
        Ok(())
    }
    fn object_end(&mut self, _lex: &Lexer<'a>) -> Result<()> {
        if let Some(Building::Object(pairs, _)) = self.stack.pop() {
            self.add(Jsonb::Object(normalize_pairs(pairs)));
        }
        Ok(())
    }
    fn array_start(&mut self, _lex: &Lexer<'a>) -> Result<()> {
        self.stack.push(Building::Array(Vec::new()));
        Ok(())
    }
    fn array_end(&mut self, _lex: &Lexer<'a>) -> Result<()> {
        if let Some(Building::Array(a)) = self.stack.pop() {
            self.add(Jsonb::Array(a));
        }
        Ok(())
    }
    fn object_field_start(
        &mut self,
        _lex: &Lexer<'a>,
        fname: &mut String,
        _isnull: bool,
    ) -> Result<()> {
        check_string_len(fname.len())?;
        if let Some(Building::Object(_, key)) = self.stack.last_mut() {
            *key = std::mem::take(fname);
        }
        Ok(())
    }
    fn scalar(&mut self, _lex: &Lexer<'a>, val: Scalar<'a>, tok: Tok) -> Result<()> {
        let v = match (tok, val) {
            (Tok::String, Scalar::Str(s)) => {
                check_string_len(s.len())?;
                Jsonb::String(s)
            }
            (Tok::Number, Scalar::Raw(raw)) => Jsonb::Number(JsonNumber::from_json_token(raw)?),
            (Tok::True, _) => Jsonb::Bool(true),
            (Tok::False, _) => Jsonb::Bool(false),
            _ => Jsonb::Null,
        };
        self.add(v);
        Ok(())
    }
}

/// Rank of a value in nested comparisons (enum jbvType order):
/// Null < String < Number < Bool < Array < Object.
#[inline]
fn type_rank(v: &Jsonb) -> u8 {
    match v {
        Jsonb::Null => 0,
        Jsonb::String(_) => 1,
        Jsonb::Number(_) => 2,
        Jsonb::Bool(_) => 3,
        Jsonb::Array(_) => 4,
        Jsonb::Object(_) => 5,
    }
}

/// Compares two values as elements of containers; for two equal-length
/// containers pushes them on `stack` (to be compared element by element).
#[inline]
fn cmp_node<'a>(a: &'a Jsonb, b: &'a Jsonb, stack: &mut Vec<CmpFrame<'a>>) -> Ordering {
    match (a, b) {
        (Jsonb::Null, Jsonb::Null) => Ordering::Equal,
        (Jsonb::String(x), Jsonb::String(y)) => x.as_bytes().cmp(y.as_bytes()),
        (Jsonb::Number(x), Jsonb::Number(y)) => x.cmp(y),
        (Jsonb::Bool(x), Jsonb::Bool(y)) => x.cmp(y),
        (Jsonb::Array(x), Jsonb::Array(y)) => {
            let o = x.len().cmp(&y.len());
            if o == Ordering::Equal && !x.is_empty() {
                stack.push(CmpFrame::Arr(x, y, 0));
            }
            o
        }
        (Jsonb::Object(x), Jsonb::Object(y)) => {
            let o = x.len().cmp(&y.len());
            if o == Ordering::Equal && !x.is_empty() {
                stack.push(CmpFrame::Obj(x, y, 0));
            }
            o
        }
        _ => type_rank(a).cmp(&type_rank(b)),
    }
}

enum CmpFrame<'a> {
    Arr(&'a [Jsonb], &'a [Jsonb], usize),
    Obj(&'a [(String, Jsonb)], &'a [(String, Jsonb)], usize),
}

/// Comparison of values nested inside containers (iterative; the order is
/// that of compareJsonbContainers(): counts first, then elements, keys and
/// values in storage order).
fn cmp_nested(a: &Jsonb, b: &Jsonb) -> Ordering {
    let mut stack: Vec<CmpFrame<'_>> = Vec::new();
    let mut cur = (a, b);
    loop {
        let o = cmp_node(cur.0, cur.1, &mut stack);
        if o != Ordering::Equal {
            return o;
        }
        loop {
            let Some(top) = stack.last_mut() else {
                return Ordering::Equal;
            };
            match top {
                CmpFrame::Arr(x, y, i) => {
                    if *i < x.len() {
                        cur = (&x[*i], &y[*i]);
                        *i += 1;
                        break;
                    }
                }
                CmpFrame::Obj(x, y, i) => {
                    if *i < x.len() {
                        let ((ka, va), (kb, vb)) = (&x[*i], &y[*i]);
                        *i += 1;
                        match ka.as_bytes().cmp(kb.as_bytes()) {
                            Ordering::Equal => {
                                cur = (va, vb);
                                break;
                            }
                            o => return o,
                        }
                    }
                }
            }
            stack.pop();
        }
    }
}

/// Class of a top-level value in compareJsonbContainers(): raw scalars are
/// pseudo arrays that sort after empty arrays and before all other arrays.
#[inline]
fn top_class(v: &Jsonb) -> u8 {
    match v {
        Jsonb::Array(a) if a.is_empty() => 0,
        Jsonb::Null | Jsonb::Bool(_) | Jsonb::Number(_) | Jsonb::String(_) => 1,
        Jsonb::Array(_) => 2,
        Jsonb::Object(_) => 3,
    }
}

impl PartialEq for Jsonb {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Jsonb {}
impl PartialOrd for Jsonb {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
/// jsonb btree ordering (compareJsonbContainers()).
impl Ord for Jsonb {
    fn cmp(&self, other: &Self) -> Ordering {
        let (ca, cb) = (top_class(self), top_class(other));
        if ca != cb {
            return ca.cmp(&cb);
        }
        cmp_nested(self, other)
    }
}

/// Consistent with `Eq` (numbers hash by value).
impl Hash for Jsonb {
    fn hash<H: Hasher>(&self, state: &mut H) {
        enum Item<'a> {
            V(&'a Jsonb),
            K(&'a str),
        }
        let mut stack = Vec::new();
        let mut next = Some(Item::V(self));
        while let Some(item) = next.take().or_else(|| stack.pop()) {
            let v = match item {
                Item::K(k) => {
                    k.hash(state);
                    continue;
                }
                Item::V(v) => v,
            };
            match v {
                Jsonb::Null => state.write_u8(0),
                Jsonb::String(s) => {
                    state.write_u8(1);
                    s.hash(state);
                }
                Jsonb::Number(n) => {
                    state.write_u8(2);
                    n.hash(state);
                }
                Jsonb::Bool(b) => state.write_u8(if *b { 4 } else { 3 }),
                Jsonb::Array(a) => {
                    state.write_u8(5);
                    state.write_usize(a.len());
                    stack.extend(a.iter().rev().map(Item::V));
                }
                Jsonb::Object(p) => {
                    state.write_u8(6);
                    state.write_usize(p.len());
                    for (k, v) in p.iter().rev() {
                        stack.push(Item::V(v));
                        stack.push(Item::K(k));
                    }
                }
            }
        }
    }
}

/// Deep copy (iterative, so deep values cannot overflow the stack).
impl Clone for Jsonb {
    fn clone(&self) -> Jsonb {
        match self {
            Jsonb::Null => Jsonb::Null,
            Jsonb::Bool(b) => Jsonb::Bool(*b),
            Jsonb::Number(n) => Jsonb::Number(n.clone()),
            Jsonb::String(s) => Jsonb::String(s.clone()),
            _ => rebuild(self, false),
        }
    }
}

enum RbSrc<'a> {
    Arr(std::slice::Iter<'a, Jsonb>),
    Obj(std::slice::Iter<'a, (String, Jsonb)>),
}

enum RbDst {
    Arr(Vec<Jsonb>),
    Obj(Vec<(String, Jsonb)>),
}

impl RbDst {
    fn push(&mut self, key: Option<String>, v: Jsonb) {
        match self {
            RbDst::Arr(a) => a.push(v),
            RbDst::Obj(p) => p.push((key.unwrap_or_default(), v)),
        }
    }
    fn finish(self) -> Jsonb {
        match self {
            RbDst::Arr(a) => Jsonb::Array(a),
            RbDst::Obj(p) => Jsonb::Object(p),
        }
    }
}

/// Iterative deep copy of a container; with `strip_nulls`, object fields
/// whose value is null are dropped at every level (jsonb_strip_nulls).
fn rebuild(root: &Jsonb, strip_nulls: bool) -> Jsonb {
    fn open(v: &Jsonb, key: Option<String>) -> (RbSrc<'_>, RbDst, Option<String>) {
        match v {
            Jsonb::Array(a) => (
                RbSrc::Arr(a.iter()),
                RbDst::Arr(Vec::with_capacity(a.len())),
                key,
            ),
            Jsonb::Object(p) => (
                RbSrc::Obj(p.iter()),
                RbDst::Obj(Vec::with_capacity(p.len())),
                key,
            ),
            _ => unreachable!(),
        }
    }
    let mut stack = vec![open(root, None)];
    loop {
        let top = stack.last_mut().unwrap();
        let next = match &mut top.0 {
            RbSrc::Arr(it) => it.next().map(|v| (None, v)),
            RbSrc::Obj(it) => loop {
                match it.next() {
                    Some((_, Jsonb::Null)) if strip_nulls => continue,
                    Some((k, v)) => break Some((Some(k.clone()), v)),
                    None => break None,
                }
            },
        };
        match next {
            Some((key, child @ (Jsonb::Array(_) | Jsonb::Object(_)))) => {
                stack.push(open(child, key))
            }
            Some((key, scalar)) => top.1.push(key, scalar.clone()),
            None => {
                let (_, dst, key) = stack.pop().unwrap();
                let built = dst.finish();
                match stack.last_mut() {
                    None => return built,
                    Some(parent) => parent.1.push(key, built),
                }
            }
        }
    }
}

/// jsonb_out: `{"a": 1, "b": [1, 2]}`
impl std::fmt::Display for Jsonb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output::write_jsonb(f, self, false)
    }
}

impl Jsonb {
    /// jsonb_in, with PostgreSQL's error messages (SQLSTATE 22P02 "invalid
    /// input syntax for type json" + detail, 22P05 for `\u0000`, numeric
    /// overflow errors, ...).
    pub fn parse(s: &str) -> Result<Jsonb> {
        let mut lex = Lexer::new(s, true);
        let mut b = Builder::default();
        parser::parse_json(&mut lex, &mut b, parser::BUDGET_JSONB)?;
        Ok(b.result.unwrap_or(Jsonb::Null))
    }

    /// jsonb_pretty
    pub fn to_pretty(&self) -> String {
        let mut s = String::new();
        let _ = output::write_jsonb(&mut s, self, true);
        s
    }

    /// `->` text: field of an object (None for non-objects / missing keys).
    pub fn get_key(&self, key: &str) -> Option<&Jsonb> {
        match self {
            Jsonb::Object(pairs) => find_key(pairs, key).map(|i| &pairs[i].1),
            _ => None,
        }
    }

    /// `->` int: array element, negative indexes count from the end. As in
    /// PostgreSQL, a scalar behaves as a one-element array (`'"a"' -> 0` is
    /// `"a"`).
    pub fn get_index(&self, idx: i64) -> Option<&Jsonb> {
        let elems: &[Jsonb] = match self {
            Jsonb::Array(a) => a,
            Jsonb::Object(_) => return None,
            scalar => std::slice::from_ref(scalar),
        };
        let n = elems.len() as i64;
        let i = if idx < 0 {
            if -idx > n {
                return None;
            }
            idx + n
        } else {
            idx
        };
        elems.get(usize::try_from(i).ok()?)
    }

    /// `#>`: follows a path of keys / array indexes. An empty path yields the
    /// value itself. (SQL NULLs in the path array make the result NULL; the
    /// caller handles them.)
    pub fn get_path(&self, path: &[&str]) -> Option<&Jsonb> {
        let mut cur = self;
        for p in path {
            cur = match cur {
                Jsonb::Object(pairs) => &pairs[find_key(pairs, p)?].1,
                Jsonb::Array(elems) => {
                    let idx = ops::strtoint(p)?;
                    let n = elems.len() as i64;
                    let i = if idx >= 0 {
                        i64::from(idx)
                    } else {
                        if idx == i32::MIN || -i64::from(idx) > n {
                            return None;
                        }
                        n + i64::from(idx)
                    };
                    elems.get(i as usize)?
                }
                _ => return None,
            };
        }
        Some(cur)
    }

    /// Result of `->>` / `#>>` applied to this (already extracted) value:
    /// strings unquoted, JSON null -> None, everything else as jsonb text.
    pub fn as_text(&self) -> Option<String> {
        match self {
            Jsonb::Null => None,
            Jsonb::Bool(true) => Some("true".to_owned()),
            Jsonb::Bool(false) => Some("false".to_owned()),
            Jsonb::Number(n) => Some(n.as_str().to_owned()),
            Jsonb::String(s) => Some(s.clone()),
            _ => Some(self.to_string()),
        }
    }

    /// jsonb_typeof
    pub fn type_name(&self) -> &'static str {
        match self {
            Jsonb::Null => "null",
            Jsonb::Bool(_) => "boolean",
            Jsonb::Number(_) => "number",
            Jsonb::String(_) => "string",
            Jsonb::Array(_) => "array",
            Jsonb::Object(_) => "object",
        }
    }

    /// jsonb_array_length
    pub fn array_length(&self) -> Result<i32> {
        match self {
            Jsonb::Array(a) => Ok(a.len() as i32),
            Jsonb::Object(_) => Err(Error::new(
                sqlstate::INVALID_PARAMETER_VALUE,
                "cannot get array length of a non-array",
            )),
            _ => Err(Error::new(
                sqlstate::INVALID_PARAMETER_VALUE,
                "cannot get array length of a scalar",
            )),
        }
    }

    /// jsonb_object_keys (keys in storage order)
    pub fn object_keys(&self) -> Result<Vec<String>> {
        match self {
            Jsonb::Object(p) => Ok(p.iter().map(|(k, _)| k.clone()).collect()),
            Jsonb::Array(_) => Err(Error::new(
                sqlstate::INVALID_PARAMETER_VALUE,
                "cannot call jsonb_object_keys on an array",
            )),
            _ => Err(Error::new(
                sqlstate::INVALID_PARAMETER_VALUE,
                "cannot call jsonb_object_keys on a scalar",
            )),
        }
    }

    /// jsonb_array_elements (also jsonb_array_elements_text, with
    /// [`Jsonb::as_text`] applied to each element)
    pub fn array_elements(&self) -> Result<Vec<Jsonb>> {
        match self {
            Jsonb::Array(a) => Ok(a.clone()),
            Jsonb::Object(_) => Err(Error::new(
                sqlstate::INVALID_PARAMETER_VALUE,
                "cannot extract elements from an object",
            )),
            _ => Err(Error::new(
                sqlstate::INVALID_PARAMETER_VALUE,
                "cannot extract elements from a scalar",
            )),
        }
    }

    /// jsonb_each
    pub fn object_entries(&self) -> Result<Vec<(String, Jsonb)>> {
        self.object_entries_named("jsonb_each")
    }

    /// jsonb_each / jsonb_each_text: `funcname` is used in the error message
    /// ("cannot call jsonb_each_text on a non-object").
    pub fn object_entries_named(&self, funcname: &str) -> Result<Vec<(String, Jsonb)>> {
        match self {
            Jsonb::Object(p) => Ok(p.clone()),
            _ => Err(Error::new(
                sqlstate::INVALID_PARAMETER_VALUE,
                format!("cannot call {funcname} on a non-object"),
            )),
        }
    }

    /// jsonb_strip_nulls: removes object fields with null values,
    /// recursively (array elements are kept).
    pub fn strip_nulls(&self) -> Jsonb {
        match self {
            Jsonb::Array(_) | Jsonb::Object(_) => rebuild(self, true),
            scalar => scalar.clone(),
        }
    }

    /// jsonb_build_object: sorts keys into jsonb order; for duplicate keys
    /// the last value wins.
    pub fn build_object(pairs: Vec<(String, Jsonb)>) -> Jsonb {
        Jsonb::Object(normalize_pairs(pairs))
    }

    /// Re-establishes the object-key invariant recursively (for values
    /// constructed by hand); iterative.
    pub fn normalize(self) -> Jsonb {
        enum Src {
            Arr(std::vec::IntoIter<Jsonb>),
            Obj(std::vec::IntoIter<(String, Jsonb)>),
        }
        fn open(
            v: Jsonb,
            key: Option<String>,
        ) -> std::result::Result<(Src, RbDst, Option<String>), Jsonb> {
            match v {
                Jsonb::Array(a) => {
                    let n = a.len();
                    Ok((
                        Src::Arr(a.into_iter()),
                        RbDst::Arr(Vec::with_capacity(n)),
                        key,
                    ))
                }
                Jsonb::Object(p) => {
                    let n = p.len();
                    Ok((
                        Src::Obj(p.into_iter()),
                        RbDst::Obj(Vec::with_capacity(n)),
                        key,
                    ))
                }
                scalar => Err(scalar),
            }
        }
        let mut stack = match open(self, None) {
            Ok(f) => vec![f],
            Err(scalar) => return scalar,
        };
        loop {
            let top = stack.last_mut().unwrap();
            let next = match &mut top.0 {
                Src::Arr(it) => it.next().map(|v| (None, v)),
                Src::Obj(it) => it.next().map(|(k, v)| (Some(k), v)),
            };
            match next {
                Some((key, child @ (Jsonb::Array(_) | Jsonb::Object(_)))) => {
                    if let Ok(f) = open(child, key) {
                        stack.push(f);
                    }
                }
                Some((key, scalar)) => top.1.push(key, scalar),
                None => {
                    let (_, dst, key) = stack.pop().unwrap();
                    let built = match dst {
                        RbDst::Arr(a) => Jsonb::Array(a),
                        RbDst::Obj(p) => Jsonb::Object(normalize_pairs(p)),
                    };
                    match stack.last_mut() {
                        None => return built,
                        Some(parent) => parent.1.push(key, built),
                    }
                }
            }
        }
    }

    /// `?`: top-level object key or string array element (a string scalar
    /// matches itself).
    pub fn exists(&self, key: &str) -> bool {
        match self {
            Jsonb::Object(p) => find_key(p, key).is_some(),
            Jsonb::Array(a) => a.iter().any(|e| matches!(e, Jsonb::String(s) if s == key)),
            Jsonb::String(s) => s == key,
            _ => false,
        }
    }

    /// `?|` (SQL NULL keys are skipped by the caller)
    pub fn exists_any(&self, keys: &[&str]) -> bool {
        keys.iter().any(|k| self.exists(k))
    }

    /// `?&` (SQL NULL keys are skipped by the caller)
    pub fn exists_all(&self, keys: &[&str]) -> bool {
        keys.iter().all(|k| self.exists(k))
    }

    /// `@>`. Never fails: values nested beyond PostgreSQL's recursion limit
    /// (where PostgreSQL raises "stack depth limit exceeded") are still
    /// evaluated; use [`Jsonb::try_contains`] for the exact behavior.
    pub fn contains(&self, other: &Jsonb) -> bool {
        ops::contains(self, other, None).unwrap_or(false)
    }

    /// `@>` including PostgreSQL's "stack depth limit exceeded" error for
    /// containment checks nested deeper than its default stack allows
    /// (about 8180 levels).
    pub fn try_contains(&self, other: &Jsonb) -> Result<bool> {
        ops::contains(self, other, Some(ops::CONTAINS_MAX_DEPTH))
    }

    /// `||`
    pub fn concat(&self, other: &Jsonb) -> Jsonb {
        ops::concat(self, other)
    }

    /// `-` text
    pub fn delete_key(&self, key: &str) -> Result<Jsonb> {
        ops::delete_key(self, key)
    }

    /// `-` int
    pub fn delete_index(&self, idx: i64) -> Result<Jsonb> {
        ops::delete_index(self, idx)
    }

    /// `#-`
    pub fn delete_path(&self, path: &[&str]) -> Result<Jsonb> {
        ops::delete_path(self, &ops::some_path(path))
    }

    /// `#-` with SQL NULLs allowed in the path array (they raise "path
    /// element at position N is null" only when reached, like PostgreSQL).
    pub fn delete_path_opt(&self, path: &[Option<&str>]) -> Result<Jsonb> {
        ops::delete_path(self, path)
    }

    /// jsonb_set
    pub fn set(&self, path: &[&str], value: Jsonb, create_missing: bool) -> Result<Jsonb> {
        ops::set(self, &ops::some_path(path), &value, create_missing)
    }

    /// jsonb_set with SQL NULLs allowed in the path array.
    pub fn set_opt(
        &self,
        path: &[Option<&str>],
        value: Jsonb,
        create_missing: bool,
    ) -> Result<Jsonb> {
        ops::set(self, path, &value, create_missing)
    }

    /// jsonb_insert
    pub fn insert(&self, path: &[&str], value: Jsonb, after: bool) -> Result<Jsonb> {
        ops::insert(self, &ops::some_path(path), &value, after)
    }

    /// jsonb_insert with SQL NULLs allowed in the path array.
    pub fn insert_opt(&self, path: &[Option<&str>], value: Jsonb, after: bool) -> Result<Jsonb> {
        ops::insert(self, path, &value, after)
    }

    /// Order-preserving, self-delimiting binary encoding:
    /// `memcmp(enc(a), enc(b))` agrees with `a.cmp(b)`, and equal values
    /// (e.g. `1.5` and `1.50`) encode identically.
    pub fn encode_key(&self, out: &mut Vec<u8>) {
        keyenc::encode_top(self, out)
    }

    /// Inverse of [`Jsonb::encode_key`]; returns the value (Ord-equal to the
    /// encoded one, numbers with minimal display scale) and the number of
    /// bytes consumed.
    pub fn decode_key(buf: &[u8]) -> Result<(Jsonb, usize)> {
        keyenc::decode_top(buf)
    }
}
