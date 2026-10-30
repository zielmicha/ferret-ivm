//! The SQL/JSON path language (`jsonpath`) with PostgreSQL 17 semantics.
//!
//! Ported from PostgreSQL 17 (src/backend/utils/adt/{jsonpath.c,
//! jsonpath_gram.y, jsonpath_scan.l, jsonpath_exec.c}), Copyright (c)
//! 2019-2024, PostgreSQL Global Development Group, used under the
//! PostgreSQL License.
//!
//! [`JsonPath::parse`] is `jsonpath_in` (exact syntax, error messages and
//! SQLSTATEs; the scanner and parser are driven by flex's and bison's own
//! tables), `Display` is `jsonpath_out`. The functions mirror the SQL API:
//!
//! | SQL | Rust |
//! |---|---|
//! | `jsonb_path_query(target, path, vars, silent)` | [`path_query`] |
//! | `jsonb_path_query_array(...)` | [`path_query_array`] |
//! | `jsonb_path_query_first(...)` | [`path_query_first`] |
//! | `jsonb_path_exists(...)` | [`path_exists`] |
//! | `jsonb_path_match(...)` | [`path_match`] |
//! | `jsonb @? jsonpath` | [`path_exists_op`] |
//! | `jsonb @@ jsonpath` | [`path_match_op`] |
//! | `jsonb_path_query_tz(...)` etc. | [`path_query_tz`], ... |
//!
//! SQL defaults: `vars => '{}'`, `silent => false`. A NULL argument makes
//! the SQL functions return NULL; that is left to the caller. Datetime
//! items depend on session settings ([`SessionTz`]: `TimeZone` and the
//! current date); see [`PathContext`].
//!
//! Notes:
//! - Errors carry SQLSTATE, message and detail; HINTs and WARNINGs are not
//!   represented (e.g. `.time(9)` silently uses precision 6, where
//!   PostgreSQL also emits a warning).
//! - `like_regex` uses the crate's regex engine, i.e. the semantics of the
//!   builtin `C.UTF-8` collation (see the crate docs).
//! - Datetime templates (`.datetime(template)`) follow `lc_time = 'C'`.
//! - Recursion limits: bison's parser stack limit (YYMAXDEPTH) is exact.
//!   PostgreSQL's `check_stack_depth()` failures ("stack depth limit
//!   exceeded", default 2MB `max_stack_depth`) while parsing, printing and
//!   executing paths are emulated from measured C frame sizes; the cut-off
//!   matches PostgreSQL 17 within a level or two on the tested shapes but
//!   depends on the caller's stack usage in PostgreSQL itself. Paths or
//!   documents deep enough to need it are executed on a helper thread with a
//!   large stack.

mod ast;
mod datetime;
mod exec;
mod gram;
mod layout;
mod scan;
#[rustfmt::skip]
mod tables;

pub use datetime::SessionTz;

use super::Jsonb;
use crate::{Error, Result, sqlstate};
use ast::{Node, NodeId};
use exec::{Jper, Val};

/// A parsed jsonpath value.
#[derive(Clone, Debug)]
pub struct JsonPath {
    lax: bool,
    nodes: Vec<Node>,
    root: NodeId,
    /// recursion depth of the item tree (flatten/print in PostgreSQL)
    depth: u32,
    /// whether the path contains `.**`, whose recursion follows the
    /// document's nesting rather than the path's
    has_any: bool,
}

/// Converts an error of the numeric library into a crate error.
pub(crate) fn exec_error(e: pgnumeric::Error) -> Error {
    Error {
        sqlstate: e.sqlstate,
        message: e.message,
        detail: e.detail,
    }
}

impl JsonPath {
    /// `jsonpath_in`: parses a jsonpath (`[lax|strict] expression`).
    pub fn parse(s: &str) -> Result<JsonPath> {
        // A C string ends at the first NUL.
        let s = match s.find('\0') {
            Some(p) => &s[..p],
            None => s,
        };
        let Some(p) = gram::parse(s.as_bytes())? else {
            return Err(Error::new(
                sqlstate::INVALID_TEXT_REPRESENTATION,
                format!("invalid input syntax for type jsonpath: \"{s}\""),
            ));
        };
        let depth = ast::check_tree(&p.nodes, p.root)?;
        let has_any = p
            .nodes
            .iter()
            .any(|n| matches!(n.kind, ast::Kind::Any { .. }));
        Ok(JsonPath {
            lax: p.lax,
            nodes: p.nodes,
            root: p.root,
            depth,
            has_any,
        })
    }

    /// Whether the path is in lax mode (the default) rather than strict.
    pub fn is_lax(&self) -> bool {
        self.lax
    }

    /// `jsonpath_out` including PostgreSQL's "stack depth limit exceeded"
    /// error for paths nested too deeply to print (see the module docs on
    /// recursion limits); `Display` prints such paths anyway.
    pub fn to_string_checked(&self) -> Result<String> {
        if self.depth > ast::PRINT_MAX_DEPTH {
            return Err(ast::stack_depth_error());
        }
        Ok(self.to_string())
    }

    /// Runs the executor (executeJsonPath()) and hands the result list to
    /// `f`. Deep paths or documents are evaluated on a thread with a large
    /// stack, since the executor recurses like PostgreSQL's.
    fn run<R: Send>(
        &self,
        doc: &Jsonb,
        vars: Option<&Jsonb>,
        silent: bool,
        want_list: bool,
        (use_tz, tz): (bool, &SessionTz),
        f: impl FnOnce(Jper, Option<&[Val<'_>]>) -> Result<R> + Send,
    ) -> Result<R> {
        let go = || -> Result<R> {
            let mut list = Vec::new();
            let res = exec::execute(
                &self.nodes,
                self.root,
                self.lax,
                vars,
                doc,
                !silent,
                if want_list { Some(&mut list) } else { None },
                use_tz,
                tz,
            )?;
            f(res, if want_list { Some(&list) } else { None })
        };
        let mut risk = self.nodes.len();
        if self.has_any {
            risk += max_depth(doc) + vars.map_or(0, max_depth);
        }
        if risk <= 200 {
            return go();
        }
        std::thread::scope(|sc| {
            std::thread::Builder::new()
                .stack_size(512 << 20)
                .spawn_scoped(sc, go)
                .expect("failed to spawn jsonpath thread")
                .join()
                .unwrap_or_else(|e| std::panic::resume_unwind(e))
        })
    }
}

/// Nesting depth of a jsonb value (iterative).
fn max_depth(v: &Jsonb) -> usize {
    let mut max = 0;
    let mut stack = vec![(v, 1usize)];
    while let Some((v, d)) = stack.pop() {
        match v {
            Jsonb::Array(a) => {
                max = max.max(d);
                stack.extend(a.iter().map(|c| (c, d + 1)));
            }
            Jsonb::Object(p) => {
                max = max.max(d);
                stack.extend(p.iter().map(|(_, c)| (c, d + 1)));
            }
            _ => {}
        }
    }
    max
}

impl std::fmt::Display for JsonPath {
    /// jsonpath_out
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = String::new();
        if !self.lax {
            s.push_str("strict ");
        }
        ast::print(&self.nodes, self.root, &mut s);
        f.write_str(&s)
    }
}

impl std::str::FromStr for JsonPath {
    type Err = Error;
    fn from_str(s: &str) -> Result<JsonPath> {
        JsonPath::parse(s)
    }
}

fn to_jsonb_list(list: &[Val<'_>]) -> Result<Vec<Jsonb>> {
    list.iter().map(Val::to_jsonb).collect()
}

static UTC_SESSION: SessionTz = SessionTz {
    time_zone: pgdatetime::TimeZone::Utc,
    current_date: 0,
};

/// Evaluation context of the jsonpath functions: the session settings that
/// datetime items consult, and whether time zone conversions are allowed
/// (the `jsonb_path_*_tz` variants).
///
/// PostgreSQL's non-`_tz` functions still read the session `TimeZone` in
/// one case: `.time_tz()` applied to a `timestamp with time zone` value.
/// The plain functions of this module ([`path_query`], ...) use
/// `TimeZone = 'UTC'`; use a context to evaluate them with another zone.
#[derive(Clone, Copy, Debug)]
pub struct PathContext<'a> {
    pub session: &'a SessionTz,
    pub use_tz: bool,
}

impl PathContext<'static> {
    /// `TimeZone = 'UTC'`, no time zone conversions (the plain functions).
    pub fn utc() -> PathContext<'static> {
        PathContext {
            session: &UTC_SESSION,
            use_tz: false,
        }
    }
}

impl<'a> PathContext<'a> {
    pub fn new(session: &'a SessionTz, use_tz: bool) -> PathContext<'a> {
        PathContext { session, use_tz }
    }

    fn tz(&self) -> (bool, &'a SessionTz) {
        (self.use_tz, self.session)
    }

    /// `jsonb_path_query[_tz](target, path, vars, silent)`
    pub fn query(
        &self,
        doc: &Jsonb,
        path: &JsonPath,
        vars: &Jsonb,
        silent: bool,
    ) -> Result<Vec<Jsonb>> {
        path.run(doc, Some(vars), silent, true, self.tz(), |_, l| {
            to_jsonb_list(l.unwrap())
        })
    }

    /// `jsonb_path_query_array[_tz](target, path, vars, silent)`
    pub fn query_array(
        &self,
        doc: &Jsonb,
        path: &JsonPath,
        vars: &Jsonb,
        silent: bool,
    ) -> Result<Jsonb> {
        path.run(doc, Some(vars), silent, true, self.tz(), |_, l| {
            Ok(Jsonb::Array(to_jsonb_list(l.unwrap())?))
        })
    }

    /// `jsonb_path_query_first[_tz](target, path, vars, silent)`
    pub fn query_first(
        &self,
        doc: &Jsonb,
        path: &JsonPath,
        vars: &Jsonb,
        silent: bool,
    ) -> Result<Option<Jsonb>> {
        path.run(doc, Some(vars), silent, true, self.tz(), |_, l| {
            l.unwrap().first().map(Val::to_jsonb).transpose()
        })
    }

    /// `jsonb_path_exists[_tz](target, path, vars, silent)`
    pub fn exists(
        &self,
        doc: &Jsonb,
        path: &JsonPath,
        vars: &Jsonb,
        silent: bool,
    ) -> Result<Option<bool>> {
        path.run(doc, Some(vars), silent, false, self.tz(), |res, _| {
            Ok(exists_result(res))
        })
    }

    /// `jsonb_path_match[_tz](target, path, vars, silent)`
    pub fn matches(
        &self,
        doc: &Jsonb,
        path: &JsonPath,
        vars: &Jsonb,
        silent: bool,
    ) -> Result<Option<bool>> {
        path.run(doc, Some(vars), silent, true, self.tz(), |_, l| {
            match_result(l.unwrap(), silent)
        })
    }

    /// `jsonb @? jsonpath`
    pub fn exists_op(&self, doc: &Jsonb, path: &JsonPath) -> Result<Option<bool>> {
        path.run(doc, None, true, false, self.tz(), |res, _| {
            Ok(exists_result(res))
        })
    }

    /// `jsonb @@ jsonpath`
    pub fn match_op(&self, doc: &Jsonb, path: &JsonPath) -> Result<Option<bool>> {
        path.run(doc, None, true, true, self.tz(), |_, l| {
            match_result(l.unwrap(), true)
        })
    }
}

/// `jsonb_path_query(target, path, vars, silent)`: all items returned by
/// the path, in order. In silent mode, items found before a suppressed
/// error are returned (as in PostgreSQL).
pub fn path_query(doc: &Jsonb, path: &JsonPath, vars: &Jsonb, silent: bool) -> Result<Vec<Jsonb>> {
    PathContext::utc().query(doc, path, vars, silent)
}

/// `jsonb_path_query_array(target, path, vars, silent)`
pub fn path_query_array(doc: &Jsonb, path: &JsonPath, vars: &Jsonb, silent: bool) -> Result<Jsonb> {
    PathContext::utc().query_array(doc, path, vars, silent)
}

/// `jsonb_path_query_first(target, path, vars, silent)`: the first item, or
/// None (SQL NULL).
pub fn path_query_first(
    doc: &Jsonb,
    path: &JsonPath,
    vars: &Jsonb,
    silent: bool,
) -> Result<Option<Jsonb>> {
    PathContext::utc().query_first(doc, path, vars, silent)
}

/// `jsonb_path_exists(target, path, vars, silent)`: None (SQL NULL) when a
/// suppressed error occurred.
pub fn path_exists(
    doc: &Jsonb,
    path: &JsonPath,
    vars: &Jsonb,
    silent: bool,
) -> Result<Option<bool>> {
    PathContext::utc().exists(doc, path, vars, silent)
}

/// `jsonb_path_match(target, path, vars, silent)`: the single boolean
/// result of a predicate path; None (SQL NULL) for a null or (in silent
/// mode) non-boolean result.
pub fn path_match(
    doc: &Jsonb,
    path: &JsonPath,
    vars: &Jsonb,
    silent: bool,
) -> Result<Option<bool>> {
    PathContext::utc().matches(doc, path, vars, silent)
}

/// `jsonb @? jsonpath` (jsonb_path_exists without vars, silent).
pub fn path_exists_op(doc: &Jsonb, path: &JsonPath) -> Result<Option<bool>> {
    PathContext::utc().exists_op(doc, path)
}

/// `jsonb @@ jsonpath` (jsonb_path_match without vars, silent).
pub fn path_match_op(doc: &Jsonb, path: &JsonPath) -> Result<Option<bool>> {
    PathContext::utc().match_op(doc, path)
}

/// `jsonb_path_query_tz(target, path, vars, silent)` with the given session
/// settings.
pub fn path_query_tz(
    doc: &Jsonb,
    path: &JsonPath,
    vars: &Jsonb,
    silent: bool,
    session: &SessionTz,
) -> Result<Vec<Jsonb>> {
    PathContext::new(session, true).query(doc, path, vars, silent)
}

/// `jsonb_path_query_array_tz(target, path, vars, silent)`
pub fn path_query_array_tz(
    doc: &Jsonb,
    path: &JsonPath,
    vars: &Jsonb,
    silent: bool,
    session: &SessionTz,
) -> Result<Jsonb> {
    PathContext::new(session, true).query_array(doc, path, vars, silent)
}

/// `jsonb_path_query_first_tz(target, path, vars, silent)`
pub fn path_query_first_tz(
    doc: &Jsonb,
    path: &JsonPath,
    vars: &Jsonb,
    silent: bool,
    session: &SessionTz,
) -> Result<Option<Jsonb>> {
    PathContext::new(session, true).query_first(doc, path, vars, silent)
}

/// `jsonb_path_exists_tz(target, path, vars, silent)`
pub fn path_exists_tz(
    doc: &Jsonb,
    path: &JsonPath,
    vars: &Jsonb,
    silent: bool,
    session: &SessionTz,
) -> Result<Option<bool>> {
    PathContext::new(session, true).exists(doc, path, vars, silent)
}

/// `jsonb_path_match_tz(target, path, vars, silent)`
pub fn path_match_tz(
    doc: &Jsonb,
    path: &JsonPath,
    vars: &Jsonb,
    silent: bool,
    session: &SessionTz,
) -> Result<Option<bool>> {
    PathContext::new(session, true).matches(doc, path, vars, silent)
}

fn exists_result(res: Jper) -> Option<bool> {
    match res {
        Jper::Error => None,
        r => Some(r == Jper::Ok),
    }
}

fn match_result(found: &[Val<'_>], silent: bool) -> Result<Option<bool>> {
    if found.len() == 1 {
        match found[0] {
            Val::Bool(b) => return Ok(Some(b)),
            Val::Null => return Ok(None),
            _ => {}
        }
    }
    if !silent {
        return Err(Error::new(
            sqlstate::SINGLETON_SQL_JSON_ITEM_REQUIRED,
            "single boolean result is expected",
        ));
    }
    Ok(None)
}
