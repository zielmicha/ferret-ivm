//! Statement parsing: engine-specific commands plus standard SQL via sqlparser.

use crate::error::{codes, Result, SqlError};
use crate::sql_bail;
use sqlparser::ast;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

/// What a query waits for before it reads the engine's latest state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AsOf {
    /// Session default (normally "fresh").
    Default,
    /// Wait until the source has ingested everything up to this LSN.
    AtLeastLsn(String),
    /// Wait for the ingestion frontier at the time the query arrives.
    Fresh,
    /// Wait until the engine has applied this GSN (never parsed: a fresh
    /// read parked until then is retried in this form).
    AtLeastGsn(u64),
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
    Subscribe { target: SubscribeTarget, snapshot: bool, progress: bool, copy: bool },
    Show(ShowKind),
    Explain { kind: ExplainKind, target: ExplainTarget },
    Checkpoint,
    Empty,
}

/// If a string literal, quoted identifier, comment or dollar-quoted string
/// starts at byte `i`, the index of its last byte (clamped to the input).
fn skip_lexical(b: &[u8], i: usize) -> Option<usize> {
    let ident_byte = |c: u8| c.is_ascii_alphanumeric() || c == b'_' || c >= 0x80;
    match b[i] {
        q @ (b'\'' | b'"') => {
            // E'...' strings also take backslash escapes
            let escapes = q == b'\''
                && i > 0
                && matches!(b[i - 1], b'e' | b'E')
                && !(i > 1 && (ident_byte(b[i - 2]) || b[i - 2] == b'$'));
            let mut j = i + 1;
            while j < b.len() {
                if escapes && b[j] == b'\\' {
                    j += 2;
                    continue;
                }
                if b[j] == q {
                    if j + 1 < b.len() && b[j + 1] == q {
                        j += 2;
                        continue;
                    }
                    return Some(j);
                }
                j += 1;
            }
            Some(b.len() - 1)
        }
        b'-' if b.get(i + 1) == Some(&b'-') => {
            let mut j = i;
            while j + 1 < b.len() && b[j + 1] != b'\n' {
                j += 1;
            }
            Some(j)
        }
        b'/' if b.get(i + 1) == Some(&b'*') => {
            // block comments nest
            let (mut j, mut depth) = (i + 2, 1);
            while j < b.len() {
                if b[j] == b'/' && b.get(j + 1) == Some(&b'*') {
                    depth += 1;
                    j += 2;
                } else if b[j] == b'*' && b.get(j + 1) == Some(&b'/') {
                    depth -= 1;
                    j += 2;
                    if depth == 0 {
                        return Some(j - 1);
                    }
                } else {
                    j += 1;
                }
            }
            Some(b.len() - 1)
        }
        // dollar quoting: $tag$ ... $tag$ (not $1 parameters, not inside identifiers)
        b'$' if !(i > 0 && ident_byte(b[i - 1])) => {
            let mut j = i + 1;
            while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_' || b[j] >= 0x80) {
                j += 1;
            }
            if j < b.len() && b[j] == b'$' && !(j > i + 1 && b[i + 1].is_ascii_digit()) {
                let tag = &b[i..=j];
                let end = b[j + 1..].windows(tag.len()).position(|w| w == tag);
                return Some(match end {
                    Some(p) => j + p + tag.len(),
                    None => b.len() - 1,
                });
            }
            None
        }
        _ => None,
    }
}

/// The highest parameter number ($n) used in a statement (0 if none).
pub fn max_param_index(sql: &str) -> usize {
    let b = sql.as_bytes();
    let mut max = 0;
    let mut i = 0;
    while i < b.len() {
        if let Some(end) = skip_lexical(b, i) {
            i = end + 1;
            continue;
        }
        if b[i] == b'$' && !(i > 0 && (b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_' || b[i - 1] >= 0x80)) {
            let mut j = i + 1;
            while j < b.len() && b[j].is_ascii_digit() {
                j += 1;
            }
            if let Ok(n) = sql[i + 1..j].parse::<usize>() {
                max = max.max(n);
            }
            i = j.max(i + 1);
            continue;
        }
        i += 1;
    }
    max
}

/// Longest chain of operators accepted at one nesting level (see
/// check_operator_chains).
pub const MAX_OPERATOR_CHAIN: usize = 50_000;

/// The parser turns a chain of binary (or postfix) operators into a tree as
/// deep as the chain is long without recursing, but cloning or dropping that
/// tree recurses once per level and would overflow the stack (aborting the
/// process) for very long chains. Bound the depth from the text: count the
/// operators (symbols and operator keywords) between commas, per level of
/// parentheses and CASE.
fn check_operator_chains(sql: &str) -> Result<()> {
    // keywords of infix and postfix operators (prefix NOT recurses in the
    // parser, which limits that by itself; NOT LIKE etc. are one operator)
    const INFIX: &[&str] = &["and", "or", "is", "like", "ilike", "similar", "between", "in", "at", "collate", "overlaps", "isnull", "notnull"];
    const RESET: &[&str] = &[
        "select", "from", "where", "group", "having", "order", "limit", "offset", "union", "intersect", "except", "join", "on",
        "using", "when", "then", "else", "values", "set", "returning", "window",
    ];
    let b = sql.as_bytes();
    let is_op = |c: u8| b"+-*/%^<>=!|&#~@?:".contains(&c);
    let is_word = |c: u8| c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80;
    let mut counts = vec![0usize];
    let mut i = 0;
    while i < b.len() {
        if let Some(end) = skip_lexical(b, i) {
            i = end + 1;
            continue;
        }
        let c = b[i];
        let mut word = None;
        if is_op(c) {
            while i < b.len() && is_op(b[i]) && skip_lexical(b, i).is_none() {
                i += 1;
            }
            *counts.last_mut().unwrap() += 1;
        } else if is_word(c) {
            let start = i;
            while i < b.len() && is_word(b[i]) {
                i += 1;
            }
            word = Some(sql[start..i].to_ascii_lowercase());
        } else {
            match c {
                b'(' => counts.push(0),
                b')' if counts.len() > 1 => {
                    counts.pop();
                }
                b',' | b';' => *counts.last_mut().unwrap() = 0,
                _ => {}
            }
            i += 1;
        }
        if let Some(w) = word {
            if w == "case" {
                counts.push(0);
            } else if w == "end" && counts.len() > 1 {
                counts.pop();
            } else if INFIX.contains(&w.as_str()) {
                *counts.last_mut().unwrap() += 1;
            } else if RESET.contains(&w.as_str()) {
                *counts.last_mut().unwrap() = 0;
            }
        }
        if counts.last().is_some_and(|n| *n > MAX_OPERATOR_CHAIN) {
            return Err(SqlError::new(codes::STATEMENT_TOO_COMPLEX, "stack depth limit exceeded")
                .with_hint(format!("Chains of more than {MAX_OPERATOR_CHAIN} operators are not supported.")));
        }
    }
    Ok(())
}

/// Whether `sql` holds nothing but whitespace and comments.
fn is_blank(sql: &str) -> bool {
    let b = sql.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        let comment = b[i..].starts_with(b"--") || b[i..].starts_with(b"/*");
        match skip_lexical(b, i) {
            Some(end) if comment => i = end + 1,
            _ => return false,
        }
    }
    true
}

/// Split a query string into statements at top-level semicolons (dropping
/// statements that are only comments, as PostgreSQL does).
pub fn split_statements(sql: &str) -> Vec<String> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let mut start = 0;
    let mut i = 0;
    let mut depth = 0i32;
    while i < b.len() {
        if let Some(end) = skip_lexical(b, i) {
            i = end + 1;
            continue;
        }
        match b[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b';' if depth <= 0 => {
                let s = sql[start..i].trim();
                if !is_blank(s) {
                    out.push(s.to_string());
                }
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    let s = sql[start..].trim();
    if !is_blank(s) {
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
    // ASCII lowercasing keeps byte offsets valid for `sql`
    let lower = sql.to_ascii_lowercase();
    let Some((idx, rest_at)) = find_as_of(&lower) else { return Ok((sql.to_string(), AsOf::Default)) };
    let rest = sql[rest_at..].trim();
    let mut t = Toks::new(rest);
    let as_of = if t.eat("at") {
        t.expect("least")?;
        t.expect("lsn")?;
        AsOf::AtLeastLsn(t.string()?)
    } else if t.eat("fresh") {
        AsOf::Fresh
    } else if t.eat("latest") {
        sql_bail!(codes::FEATURE_NOT_SUPPORTED, "AS OF LATEST is not supported; use SET read_mode = latest");
    } else if t.peek().is_some_and(|n| n.parse::<u64>().is_ok()) {
        sql_bail!(codes::FEATURE_NOT_SUPPORTED, "AS OF <gsn> is not supported: reads see the latest state (the engine keeps no history)");
    } else {
        sql_bail!(codes::SYNTAX_ERROR, "AS OF expects FRESH or AT LEAST LSN '<lsn>'");
    };
    if !t.at_end() {
        sql_bail!(codes::SYNTAX_ERROR, "unexpected tokens after AS OF clause");
    }
    Ok((sql[..idx].to_string(), as_of))
}

/// The last `AS OF` (words separated by whitespace, followed by more text)
/// outside parentheses, quotes and comments of a lowercased statement:
/// (offset of the whitespace before AS, offset just after OF).
fn find_as_of(s: &str) -> Option<(usize, usize)> {
    let b = s.as_bytes();
    let ws = |c: u8| c.is_ascii_whitespace();
    let mut depth = 0;
    let mut found = None;
    let mut i = 0;
    while i < b.len() {
        if let Some(end) = skip_lexical(b, i) {
            i = end + 1;
            continue;
        }
        match b[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            c if depth == 0 && ws(c) && b[i + 1..].starts_with(b"as") => {
                let mut j = i + 3;
                if j < b.len() && ws(b[j]) {
                    while j < b.len() && ws(b[j]) {
                        j += 1;
                    }
                    if b[j..].starts_with(b"of") && b.get(j + 2).is_some_and(|c| ws(*c)) {
                        found = Some((i, j + 2));
                    }
                }
            }
            _ => {}
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
    if is_blank(sql) {
        return Ok(Statement::Empty);
    }
    check_operator_chains(sql)?;
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
            // COPY (SUBSCRIBE ...) TO STDOUT (ASCII lowercasing keeps
            // byte offsets valid for `sql`)
            let lower = sql.to_ascii_lowercase();
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
            if !t2.at_end() {
                // not a name: EXPLAIN of DML or DDL, or with options
                let word = rl.split(|c: char| !(c.is_alphanumeric() || c == '_')).next().unwrap_or_default().to_uppercase();
                sql_bail!(
                    codes::FEATURE_NOT_SUPPORTED,
                    "EXPLAIN {word} is not supported: EXPLAIN takes a query (SELECT, WITH, VALUES) or the name of a view"
                );
            }
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
            sql_bail!(codes::FEATURE_NOT_SUPPORTED, "SUBSCRIBE ... AS OF is not supported: a subscription starts at the last applied GSN");
        } else {
            sql_bail!(codes::SYNTAX_ERROR, "syntax error in SUBSCRIBE near {:?}", t.peek());
        }
    }
    Ok(Statement::Subscribe { target, snapshot, progress, copy })
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

    /// EXPLAIN of anything but a query or a view name fails as such: the
    /// first word used to be taken as a view name (`EXPLAIN INSERT ...`:
    /// relation "insert" does not exist).
    #[test]
    fn explain_targets() {
        for sql in ["EXPLAIN INSERT INTO t VALUES (1)", "EXPLAIN ANALYZE SELECT 1", "EXPLAIN DATAFLOW DELETE FROM t", "explain update t set a = 1"] {
            let e = parse_statement(sql).map(|_| ()).unwrap_err();
            assert_eq!(e.code, codes::FEATURE_NOT_SUPPORTED, "{sql}: {}", e.message);
            let word = sql.split_whitespace().find(|w| !["explain", "dataflow"].contains(&w.to_lowercase().as_str())).unwrap().to_uppercase();
            assert!(e.message.starts_with(&format!("EXPLAIN {word} is not supported")), "{sql}: {}", e.message);
        }
        for (sql, name) in [("EXPLAIN v", "v"), ("EXPLAIN MATERIALIZED VIEW \"V w\"", "V w"), ("EXPLAIN MAINTENANCE FOR m", "m")] {
            match parse_statement(sql).unwrap() {
                Statement::Explain { target: ExplainTarget::Name(n), .. } => assert_eq!(n, name, "{sql}"),
                other => panic!("{sql}: {other:?}"),
            }
        }
        assert!(matches!(parse_statement("EXPLAIN SELECT 1").unwrap(), Statement::Explain { target: ExplainTarget::Query(_), .. }));
    }

    #[test]
    fn split() {
        let v = split_statements("select 1; select ';' ; -- c;\nselect $$a;b$$; select (1;2)");
        assert_eq!(v.len(), 4, "{v:?}");
        // escaped quotes in E'' strings, nested comments, tagged dollar quotes
        let v = split_statements(r"select E'a\';b'; select /* x /* y; */ z; */ 2; select $f$;$$;$f$; select 'ü;'");
        // comment-only statements are no statements
        assert_eq!(split_statements("select 1; -- done\n /* end */"), vec!["select 1"]);
        assert!(split_statements("-- nothing").is_empty());
        assert!(matches!(parse_statement("/* nothing */"), Ok(Statement::Empty)));
        assert_eq!(v, vec![r"select E'a\';b'", "select /* x /* y; */ z; */ 2", "select $f$;$$;$f$", "select 'ü;'"]);
    }

    fn as_of(sql: &str) -> AsOf {
        match parse_statement(sql).unwrap_or_else(|e| panic!("{sql}: {e}")) {
            Statement::Select { as_of, .. } => as_of,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn as_of_clause() {
        // non-ASCII text outside quotes used to panic (byte slicing inside a character)
        assert_eq!(as_of("select 1 as ü"), AsOf::Default);
        assert_eq!(as_of("select 'İİ' as x, ü from t as of fresh"), AsOf::Fresh);
        // AS OF inside comments, strings and dollar quotes is not a clause
        assert_eq!(as_of("select a from t -- as of 1"), AsOf::Default);
        assert_eq!(as_of("select a from t /* as of 1 */"), AsOf::Default);
        assert_eq!(as_of("select $$ as of 1 $$"), AsOf::Default);
        assert_eq!(as_of("select ' as of 1'"), AsOf::Default);
        // any whitespace separates the words
        assert_eq!(as_of("select a from t\nAS\tOF at\tleast lsn '0/1'"), AsOf::AtLeastLsn("0/1".into()));
        match parse_statement("select a from t as of fresh").unwrap() {
            Statement::Select { text, .. } => assert_eq!(text, "select a from t"),
            other => panic!("{other:?}"),
        }
        // time travel is gone
        let code = |sql: &str| parse_statement(sql).unwrap_err().code;
        assert_eq!(code("select a from t as of 7"), codes::FEATURE_NOT_SUPPORTED);
        assert_eq!(code("select a from t as of latest"), codes::FEATURE_NOT_SUPPORTED);
        assert_eq!(code("subscribe to v as of 7"), codes::FEATURE_NOT_SUPPORTED);
        assert_eq!(code("select a from t as of yesterday"), codes::SYNTAX_ERROR);
        assert!(parse_statement("COPY (SUBSCRIBE (select 'İ' from t)) TO STDOUT").is_ok());
    }

    #[test]
    fn operator_chains() {
        // deep syntax trees are dropped recursively: use a session-sized stack
        std::thread::Builder::new().stack_size(16 << 20).spawn(operator_chains_body).unwrap().join().unwrap();
    }

    fn operator_chains_body() {
        let chain = |n: usize, sep: &str| vec!["1"; n].join(sep);
        assert!(parse_statement(&format!("select {}", chain(MAX_OPERATOR_CHAIN, "+"))).is_ok());
        let err = parse_statement(&format!("select {}", chain(MAX_OPERATOR_CHAIN + 2, "+"))).unwrap_err();
        assert!(err.message.contains("stack depth limit exceeded"), "{}", err.message);
        assert!(parse_statement(&format!("select 1 where {}", vec!["a = 1"; MAX_OPERATOR_CHAIN].join(" or "))).is_err());
        // commas, CASE branches and parentheses start new chains
        assert!(parse_statement(&format!("select 1 where a in ({})", vec!["-1"; 3 * MAX_OPERATOR_CHAIN].join(", "))).is_ok());
        let parened = vec!["(a = 1 and b = 2)"; MAX_OPERATOR_CHAIN / 2].join(" or ");
        assert!(parse_statement(&format!("select 1 where {parened}")).is_ok());
        assert!(parse_statement(&format!("select 1 where '{}' = ''", "+".repeat(10 * MAX_OPERATOR_CHAIN))).is_ok());
    }

    #[test]
    fn param_count() {
        assert_eq!(max_param_index("select $1, $12 + $3"), 12);
        assert_eq!(max_param_index("select '$4', $$ $5 $$, $t$ $6 $t$, \"$7\", a$8 -- $9\n, $2"), 2);
        assert_eq!(max_param_index("select 1"), 0);
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
        match parse_statement("select * from t as of at least lsn '0/16B3748'").unwrap() {
            Statement::Select { as_of, .. } => assert_eq!(as_of, AsOf::AtLeastLsn("0/16B3748".into())),
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
