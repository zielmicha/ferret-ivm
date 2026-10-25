//! Statement parsing: engine-specific commands plus standard SQL via sqlparser.

use crate::error::{codes, Result, SqlError};
use crate::sql_bail;
use sqlparser::ast;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

/// Read point for queries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AsOf {
    /// Session default (normally "fresh").
    Default,
    Gsn(u64),
    /// Wait until the source has ingested everything up to this LSN.
    AtLeastLsn(String),
    /// Wait for the ingestion frontier at the time the query arrives.
    Fresh,
    /// Whatever is processed right now (no waiting).
    Latest,
}

#[derive(Clone, Debug)]
pub enum SubscribeTarget {
    Name(String),
    Query(Box<ast::Query>, String),
}

#[derive(Clone, Debug)]
pub enum ShowKind {
    Sources,
    Tables,
    Views,
    MaterializedViews,
    Indexes,
    Sinks,
    Arrangements,
    Frontiers,
    Gsn,
    Columns(String),
    CreateView(String),
    Variable(String),
    All,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExplainKind {
    Plan,
    Dataflow,
    Maintenance,
}

#[derive(Clone, Debug)]
pub enum ExplainTarget {
    Name(String),
    Query(Box<ast::Query>),
}

#[derive(Clone, Debug)]
pub enum Statement {
    /// Standard statement handled from the sqlparser AST.
    Sql(Box<ast::Statement>),
    Select { query: Box<ast::Query>, as_of: AsOf, text: String },
    CreateSource { name: String, conninfo: String, publication: String, slot: Option<String>, tables: Option<Vec<String>> },
    DropSource { name: String, if_exists: bool },
    AlterSourceRefresh { name: String },
    CreateSink { name: String, from: String, conninfo: String, table: String, key: Option<Vec<String>> },
    DropSink { name: String, if_exists: bool },
    Subscribe { target: SubscribeTarget, snapshot: bool, progress: bool, as_of: AsOf, copy: bool },
    Show(ShowKind),
    Explain { kind: ExplainKind, target: ExplainTarget },
    Checkpoint,
    Empty,
}

/// Split a query string into statements at top-level semicolons.
pub fn split_statements(sql: &str) -> Vec<String> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let mut start = 0;
    let mut i = 0;
    let mut depth = 0i32;
    while i < b.len() {
        match b[i] {
            b'\'' | b'"' => {
                let q = b[i];
                i += 1;
                while i < b.len() {
                    if b[i] == q {
                        if i + 1 < b.len() && b[i + 1] == q {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'-' if i + 1 < b.len() && b[i + 1] == b'-' => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < b.len() && b[i + 1] == b'*' => {
                i += 2;
                while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                    i += 1;
                }
                i += 1;
            }
            b'$' => {
                // dollar quoting: $tag$ ... $tag$
                let mut j = i + 1;
                while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_') {
                    j += 1;
                }
                if j < b.len() && b[j] == b'$' && !(j > i + 1 && b[i + 1].is_ascii_digit()) {
                    let tag = &sql[i..=j];
                    if let Some(end) = sql[j + 1..].find(tag) {
                        i = j + 1 + end + tag.len() - 1;
                    } else {
                        i = b.len();
                    }
                }
            }
            b'(' => depth += 1,
            b')' => depth -= 1,
            b';' if depth <= 0 => {
                let s = sql[start..i].trim();
                if !s.is_empty() {
                    out.push(s.to_string());
                }
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    let s = sql[start..].trim();
    if !s.is_empty() {
        out.push(s.to_string());
    }
    out
}

fn sqlparser_err(e: sqlparser::parser::ParserError) -> SqlError {
    let msg = e.to_string();
    let msg = msg.strip_prefix("sql parser error: ").unwrap_or(&msg).to_string();
    SqlError::new(codes::SYNTAX_ERROR, msg)
}

/// Parse a single SQL query (used for view definitions).
pub fn parse_single_query(sql: &str) -> Result<ast::Query> {
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(sqlparser_err)?;
    match stmts.into_iter().next() {
        Some(ast::Statement::Query(q)) => Ok(*q),
        _ => sql_bail!(codes::SYNTAX_ERROR, "expected a query"),
    }
}

/// A tiny token cursor over a statement for the engine-specific grammar.
struct Toks {
    toks: Vec<String>,
    raw: Vec<String>,
    pos: usize,
}

fn tokenize(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let c: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < c.len() {
        let ch = c[i];
        if ch.is_whitespace() {
            i += 1;
        } else if ch == '\'' {
            let mut t = String::from("'");
            i += 1;
            while i < c.len() {
                if c[i] == '\'' {
                    if i + 1 < c.len() && c[i + 1] == '\'' {
                        t.push('\'');
                        i += 2;
                        continue;
                    }
                    break;
                }
                t.push(c[i]);
                i += 1;
            }
            i += 1;
            t.push('\'');
            out.push(t);
        } else if ch == '"' {
            let mut t = String::from("\"");
            i += 1;
            while i < c.len() && c[i] != '"' {
                t.push(c[i]);
                i += 1;
            }
            i += 1;
            t.push('"');
            out.push(t);
        } else if ch.is_alphanumeric() || ch == '_' || ch == '.' {
            let mut t = String::new();
            while i < c.len() && (c[i].is_alphanumeric() || c[i] == '_' || c[i] == '.' || c[i] == '/') {
                t.push(c[i]);
                i += 1;
            }
            out.push(t);
        } else {
            out.push(ch.to_string());
            i += 1;
        }
    }
    out
}

impl Toks {
    fn new(s: &str) -> Toks {
        let raw = tokenize(s);
        let toks = raw.iter().map(|t| if t.starts_with('\'') { t.clone() } else { t.to_lowercase() }).collect();
        Toks { toks, raw, pos: 0 }
    }
    fn peek(&self) -> Option<&str> {
        self.toks.get(self.pos).map(|s| s.as_str())
    }
    fn peek_at(&self, k: usize) -> Option<&str> {
        self.toks.get(self.pos + k).map(|s| s.as_str())
    }
    fn next(&mut self) -> Option<String> {
        let t = self.toks.get(self.pos).cloned();
        self.pos += 1;
        t
    }
    fn eat(&mut self, kw: &str) -> bool {
        if self.peek() == Some(kw) {
            self.pos += 1;
            true
        } else {
            false
        }
    }
    fn expect(&mut self, kw: &str) -> Result<()> {
        if self.eat(kw) {
            Ok(())
        } else {
            sql_bail!(codes::SYNTAX_ERROR, "syntax error: expected {} near {:?}", kw.to_uppercase(), self.peek().unwrap_or("end of input"))
        }
    }
    fn ident(&mut self) -> Result<String> {
        let raw = self.raw.get(self.pos).cloned().ok_or_else(|| SqlError::syntax("unexpected end of input"))?;
        self.pos += 1;
        if raw.starts_with('"') {
            Ok(raw.trim_matches('"').to_string())
        } else if raw.chars().next().is_some_and(|c| c.is_alphanumeric() || c == '_') {
            Ok(raw.to_lowercase())
        } else {
            sql_bail!(codes::SYNTAX_ERROR, "syntax error at or near \"{raw}\"")
        }
    }
    fn string(&mut self) -> Result<String> {
        let raw = self.raw.get(self.pos).cloned().ok_or_else(|| SqlError::syntax("expected a string literal"))?;
        self.pos += 1;
        if raw.starts_with('\'') && raw.len() >= 2 {
            Ok(raw[1..raw.len() - 1].to_string())
        } else {
            sql_bail!(codes::SYNTAX_ERROR, "expected a string literal, got {raw}")
        }
    }
    fn at_end(&self) -> bool {
        self.pos >= self.toks.len()
    }
    fn ident_list(&mut self) -> Result<Vec<String>> {
        self.expect("(")?;
        let mut v = Vec::new();
        loop {
            v.push(self.ident()?);
            if self.eat(")") {
                break;
            }
            self.expect(",")?;
        }
        Ok(v)
    }
}

/// Strip a trailing `AS OF ...` clause from a SELECT.
fn strip_as_of(sql: &str) -> Result<(String, AsOf)> {
    let lower = sql.to_lowercase();
    let Some(idx) = find_top_level(&lower, " as of ") else { return Ok((sql.to_string(), AsOf::Default)) };
    let rest = sql[idx + 7..].trim();
    let mut t = Toks::new(rest);
    let as_of = if t.eat("at") {
        t.expect("least")?;
        t.expect("lsn")?;
        AsOf::AtLeastLsn(t.string()?)
    } else if t.eat("fresh") {
        AsOf::Fresh
    } else if t.eat("latest") {
        AsOf::Latest
    } else {
        let n = t.next().ok_or_else(|| SqlError::syntax("AS OF requires a GSN"))?;
        AsOf::Gsn(n.parse().map_err(|_| SqlError::syntax(format!("invalid GSN {n}")))?)
    };
    if !t.at_end() {
        sql_bail!(codes::SYNTAX_ERROR, "unexpected tokens after AS OF clause");
    }
    Ok((sql[..idx].to_string(), as_of))
}

/// Last occurrence of `pat` outside quotes/parentheses.
fn find_top_level(s: &str, pat: &str) -> Option<usize> {
    let b = s.as_bytes();
    let mut depth = 0;
    let mut in_q: Option<u8> = None;
    let mut found = None;
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        match in_q {
            Some(q) => {
                if c == q {
                    in_q = None;
                }
            }
            None => match c {
                b'\'' | b'"' => in_q = Some(c),
                b'(' => depth += 1,
                b')' => depth -= 1,
                _ => {
                    if depth == 0 && s[i..].starts_with(pat) {
                        found = Some(i);
                    }
                }
            },
        }
        i += 1;
    }
    found
}

fn parse_std(sql: &str) -> Result<ast::Statement> {
    let mut stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(sqlparser_err)?;
    match stmts.len() {
        0 => sql_bail!(codes::SYNTAX_ERROR, "empty statement"),
        1 => Ok(stmts.pop().unwrap()),
        _ => sql_bail!(codes::SYNTAX_ERROR, "expected a single statement"),
    }
}

/// Parse one statement.
pub fn parse_statement(sql: &str) -> Result<Statement> {
    let sql = sql.trim().trim_end_matches(';').trim();
    if sql.is_empty() {
        return Ok(Statement::Empty);
    }
    let mut t = Toks::new(sql);
    let first = t.peek().unwrap_or("").to_string();
    match first.as_str() {
        "create" => {
            if t.peek_at(1) == Some("source") {
                t.pos += 2;
                return parse_create_source(&mut t);
            }
            if t.peek_at(1) == Some("sink") {
                t.pos += 2;
                return parse_create_sink(&mut t);
            }
        }
        "drop" => {
            if matches!(t.peek_at(1), Some("source") | Some("sink")) {
                let is_source = t.peek_at(1) == Some("source");
                t.pos += 2;
                let if_exists = if t.eat("if") {
                    t.expect("exists")?;
                    true
                } else {
                    false
                };
                let name = t.ident()?;
                t.eat("cascade");
                return Ok(if is_source { Statement::DropSource { name, if_exists } } else { Statement::DropSink { name, if_exists } });
            }
        }
        "alter" => {
            if t.peek_at(1) == Some("source") {
                t.pos += 2;
                let name = t.ident()?;
                t.expect("refresh")?;
                return Ok(Statement::AlterSourceRefresh { name });
            }
        }
        "subscribe" => {
            t.pos += 1;
            return parse_subscribe(sql, &mut t, false);
        }
        "copy" => {
            // COPY (SUBSCRIBE ...) TO STDOUT
            let lower = sql.to_lowercase();
            if let Some(p) = lower.find("subscribe")
                && lower[..p].trim_end().ends_with('(')
            {
                let close = matching_paren(sql, lower[..p].rfind('(').unwrap())
                    .ok_or_else(|| SqlError::syntax("unbalanced parentheses in COPY"))?;
                let inner = &sql[p..close];
                let tail = sql[close + 1..].trim().to_lowercase();
                if !tail.starts_with("to stdout") {
                    sql_bail!(codes::FEATURE_NOT_SUPPORTED, "COPY (SUBSCRIBE ...) only supports TO STDOUT");
                }
                let mut t2 = Toks::new(inner);
                t2.pos = 1;
                return parse_subscribe(inner, &mut t2, true);
            }
            sql_bail!(codes::FEATURE_NOT_SUPPORTED, "COPY is only supported as COPY (SUBSCRIBE ...) TO STDOUT");
        }
        "show" => {
            if let Some(s) = parse_show(&mut t)? {
                return Ok(Statement::Show(s));
            }
        }
        "explain" => {
            t.pos += 1;
            let kind = if t.eat("maintenance") {
                ExplainKind::Maintenance
            } else if t.eat("dataflow") {
                ExplainKind::Dataflow
            } else {
                t.eat("plan");
                ExplainKind::Plan
            };
            t.eat("for");
            let rest_start = t.pos;
            let rest = rest_of(sql, &t.raw, rest_start);
            let rl = rest.trim().to_lowercase();
            if rl.starts_with("select") || rl.starts_with("with") || rl.starts_with("values") || rl.starts_with('(') {
                let q = parse_single_query(rest.trim())?;
                return Ok(Statement::Explain { kind, target: ExplainTarget::Query(Box::new(q)) });
            }
            let mut t2 = Toks::new(rest.trim());
            t2.eat("view");
            t2.eat("materialized");
            t2.eat("view");
            let name = t2.ident()?;
            return Ok(Statement::Explain { kind, target: ExplainTarget::Name(name) });
        }
        "checkpoint" => return Ok(Statement::Checkpoint),
        "select" | "with" | "values" | "table" | "(" => {
            let (q, as_of) = strip_as_of(sql)?;
            let stmt = parse_std(&q)?;
            if let ast::Statement::Query(query) = stmt {
                return Ok(Statement::Select { query, as_of, text: q });
            }
            return Ok(Statement::Sql(Box::new(stmt)));
        }
        _ => {}
    }
    Ok(Statement::Sql(Box::new(parse_std(sql)?)))
}

fn matching_paren(s: &str, open: usize) -> Option<usize> {
    let b = s.as_bytes();
    let mut depth = 0;
    let mut in_q: Option<u8> = None;
    for (i, &c) in b.iter().enumerate().skip(open) {
        match in_q {
            Some(q) => {
                if c == q {
                    in_q = None;
                }
            }
            None => match c {
                b'\'' | b'"' => in_q = Some(c),
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(i);
                    }
                }
                _ => {}
            },
        }
    }
    None
}

/// Reconstruct the source text starting at token index `pos`.
fn rest_of(sql: &str, raw: &[String], pos: usize) -> String {
    // find the char offset of token `pos` by scanning tokens in order
    let mut offset = 0;
    for (i, tok) in raw.iter().enumerate() {
        let tlen = tok.len();
        let found = sql[offset..].find(tok.as_str()).map(|p| p + offset);
        match found {
            Some(p) => {
                if i == pos {
                    return sql[p..].to_string();
                }
                offset = p + tlen;
            }
            None => break,
        }
    }
    if pos >= raw.len() { String::new() } else { sql.to_string() }
}

fn parse_create_source(t: &mut Toks) -> Result<Statement> {
    let name = t.ident()?;
    t.expect("from")?;
    t.expect("postgres")?;
    let mut conninfo = None;
    let mut publication = None;
    let mut slot = None;
    let mut tables = None;
    t.eat("(");
    while !t.at_end() {
        if t.eat(",") || t.eat(")") {
            continue;
        }
        let kw = t.next().unwrap();
        match kw.as_str() {
            "connection" => {
                t.eat("=");
                conninfo = Some(t.string()?);
            }
            "publication" => {
                t.eat("=");
                publication = Some(t.string()?);
            }
            "slot" => {
                t.eat("=");
                slot = Some(t.string()?);
            }
            "for" => {
                if t.eat("all") {
                    t.expect("tables")?;
                } else {
                    t.expect("tables")?;
                    tables = Some(t.ident_list()?);
                }
            }
            other => sql_bail!(codes::SYNTAX_ERROR, "unexpected option {other} in CREATE SOURCE"),
        }
    }
    Ok(Statement::CreateSource {
        name,
        conninfo: conninfo.ok_or_else(|| SqlError::syntax("CREATE SOURCE requires CONNECTION"))?,
        publication: publication.ok_or_else(|| SqlError::syntax("CREATE SOURCE requires PUBLICATION"))?,
        slot,
        tables,
    })
}

fn parse_create_sink(t: &mut Toks) -> Result<Statement> {
    let name = t.ident()?;
    t.expect("from")?;
    let from = t.ident()?;
    t.expect("into")?;
    t.expect("postgres")?;
    let mut conninfo = None;
    let mut table = None;
    let mut key = None;
    t.eat("(");
    while !t.at_end() {
        if t.eat(",") || t.eat(")") {
            continue;
        }
        let kw = t.next().unwrap();
        match kw.as_str() {
            "connection" => {
                t.eat("=");
                conninfo = Some(t.string()?);
            }
            "table" => {
                t.eat("=");
                table = Some(t.string()?);
            }
            "key" => {
                t.eat("=");
                key = Some(t.ident_list()?);
            }
            other => sql_bail!(codes::SYNTAX_ERROR, "unexpected option {other} in CREATE SINK"),
        }
    }
    Ok(Statement::CreateSink {
        name,
        from,
        conninfo: conninfo.ok_or_else(|| SqlError::syntax("CREATE SINK requires CONNECTION"))?,
        table: table.ok_or_else(|| SqlError::syntax("CREATE SINK requires TABLE"))?,
        key,
    })
}

fn parse_subscribe(sql: &str, t: &mut Toks, copy: bool) -> Result<Statement> {
    t.eat("to");
    let target = if t.peek() == Some("(") {
        // query in parentheses
        let open = sql.find('(').unwrap();
        let close = matching_paren(sql, open).ok_or_else(|| SqlError::syntax("unbalanced parentheses in SUBSCRIBE"))?;
        let qtext = sql[open + 1..close].to_string();
        let q = parse_single_query(&qtext)?;
        // skip tokens up to the closing paren
        let mut depth = 0;
        while let Some(tok) = t.next() {
            if tok == "(" {
                depth += 1;
            } else if tok == ")" {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
        }
        SubscribeTarget::Query(Box::new(q), qtext)
    } else {
        SubscribeTarget::Name(t.ident()?)
    };
    let mut snapshot = true;
    let mut progress = false;
    let mut as_of = AsOf::Default;
    while !t.at_end() {
        if t.eat("with") {
            t.expect("(")?;
            loop {
                let opt = t.ident()?;
                let val = if t.eat("=") || matches!(t.peek(), Some("true") | Some("false")) {
                    let v = t.next().unwrap_or_default();
                    v == "true" || v == "on" || v == "1"
                } else {
                    true
                };
                match opt.as_str() {
                    "snapshot" => snapshot = val,
                    "progress" => progress = val,
                    other => sql_bail!(codes::SYNTAX_ERROR, "unknown SUBSCRIBE option {other}"),
                }
                if t.eat(")") {
                    break;
                }
                t.expect(",")?;
            }
        } else if t.eat("as") {
            t.expect("of")?;
            let n = t.next().ok_or_else(|| SqlError::syntax("AS OF requires a GSN"))?;
            as_of = AsOf::Gsn(n.parse().map_err(|_| SqlError::syntax(format!("invalid GSN {n}")))?);
        } else {
            sql_bail!(codes::SYNTAX_ERROR, "syntax error in SUBSCRIBE near {:?}", t.peek());
        }
    }
    Ok(Statement::Subscribe { target, snapshot, progress, as_of, copy })
}

fn parse_show(t: &mut Toks) -> Result<Option<ShowKind>> {
    let save = t.pos;
    t.pos += 1;
    let k = match t.next().as_deref() {
        Some("sources") => ShowKind::Sources,
        Some("tables") => ShowKind::Tables,
        Some("views") => ShowKind::Views,
        Some("materialized") => {
            t.expect("views")?;
            ShowKind::MaterializedViews
        }
        Some("indexes") => ShowKind::Indexes,
        Some("sinks") => ShowKind::Sinks,
        Some("arrangements") => ShowKind::Arrangements,
        Some("frontiers") => ShowKind::Frontiers,
        Some("gsn") => ShowKind::Gsn,
        Some("all") => ShowKind::All,
        Some("columns") => {
            if !t.eat("from") {
                t.expect("in")?;
            }
            ShowKind::Columns(t.ident()?)
        }
        Some("create") => {
            t.eat("materialized");
            t.expect("view")?;
            ShowKind::CreateView(t.ident()?)
        }
        Some(v) => {
            let mut name = v.to_string();
            while t.eat(".") {
                name.push('.');
                name.push_str(&t.ident()?);
            }
            ShowKind::Variable(name)
        }
        None => {
            t.pos = save;
            return Ok(None);
        }
    };
    if !t.at_end() {
        t.pos = save;
        return Ok(None);
    }
    Ok(Some(k))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split() {
        let v = split_statements("select 1; select ';' ; -- c;\nselect $$a;b$$; select (1;2)");
        assert_eq!(v.len(), 4, "{v:?}");
    }

    #[test]
    fn custom() {
        match parse_statement("CREATE SOURCE pg FROM POSTGRES CONNECTION 'host=x' PUBLICATION 'p' FOR TABLES (a, b)").unwrap() {
            Statement::CreateSource { name, conninfo, publication, tables, .. } => {
                assert_eq!(name, "pg");
                assert_eq!(conninfo, "host=x");
                assert_eq!(publication, "p");
                assert_eq!(tables, Some(vec!["a".to_string(), "b".to_string()]));
            }
            other => panic!("{other:?}"),
        }
        match parse_statement("select * from t as of 42").unwrap() {
            Statement::Select { as_of, .. } => assert_eq!(as_of, AsOf::Gsn(42)),
            other => panic!("{other:?}"),
        }
        match parse_statement("SUBSCRIBE TO v WITH (SNAPSHOT = false, PROGRESS)").unwrap() {
            Statement::Subscribe { snapshot, progress, .. } => {
                assert!(!snapshot);
                assert!(progress);
            }
            other => panic!("{other:?}"),
        }
        match parse_statement("COPY (SUBSCRIBE (select a from t where b > 1)) TO STDOUT").unwrap() {
            Statement::Subscribe { copy, target: SubscribeTarget::Query(..), .. } => assert!(copy),
            other => panic!("{other:?}"),
        }
    }
}
