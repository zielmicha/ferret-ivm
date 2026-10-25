//! POSIX regular expressions with PostgreSQL 17 semantics.
//!
//! This is a port of the Henry Spencer regex engine used by PostgreSQL
//! (lexer, parser, subexpression tree and match/dissection algorithms), so
//! match positions and submatch boundaries follow PostgreSQL's
//! leftmost-longest/shortest "preference" rules exactly, including
//! backreferences and lookahead/lookbehind constraints. SQL-level wrappers
//! are ported from src/backend/utils/adt/regexp.c and varlena.c
//! (replace_text_regexp).
//!
//! Portions Copyright (c) 1998, 1999 Henry Spencer. All rights reserved.
//! Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group.

#![allow(clippy::needless_range_loop, clippy::while_let_loop)]

mod charset;
mod compile;
mod dfa;
mod exec;
mod fxhash;

use crate::{Error, Result, sqlstate};
use compile::{REG_ADVANCED, REG_EXPANDED, REG_EXTENDED, REG_ICASE, REG_NEWLINE, REG_NLANCH, REG_NLSTOP, REG_QUOTE};
use exec::{Compiled, Matcher};
use std::sync::Arc;

/// A compiled POSIX (ARE by default) regular expression.
#[derive(Clone, Debug)]
pub struct Regex {
    inner: Arc<Compiled>,
    glob: bool,
}

/// Parsed regexp flags (parse_re_flags).
fn parse_re_flags(opts: &str) -> Result<(u32, bool)> {
    let mut cflags = REG_ADVANCED;
    let mut glob = false;
    for c in opts.chars() {
        match c {
            'g' => glob = true,
            'b' => cflags &= !(REG_ADVANCED | REG_EXTENDED | REG_QUOTE),
            'c' => cflags &= !REG_ICASE,
            'e' => {
                cflags |= REG_EXTENDED;
                cflags &= !(REG_ADVANCED | REG_QUOTE);
            }
            'i' => cflags |= REG_ICASE,
            'm' | 'n' => cflags |= REG_NEWLINE,
            'p' => {
                cflags |= REG_NLSTOP;
                cflags &= !REG_NLANCH;
            }
            'q' => {
                cflags |= REG_QUOTE;
                cflags &= !(REG_ADVANCED | REG_EXTENDED);
            }
            's' => cflags &= !REG_NEWLINE,
            't' => cflags &= !REG_EXPANDED,
            'w' => {
                cflags &= !REG_NLSTOP;
                cflags |= REG_NLANCH;
            }
            'x' => cflags |= REG_EXPANDED,
            _ => {
                return Err(Error::new(
                    sqlstate::INVALID_PARAMETER_VALUE,
                    format!("invalid regular expression option: \"{c}\""),
                ));
            }
        }
    }
    Ok((cflags, glob))
}

fn exec_error(code: i32) -> Error {
    Error::new(
        sqlstate::INVALID_REGULAR_EXPRESSION,
        format!("regular expression failed: {}", compile::reg_error_message(code)),
    )
}

fn invalid_param(name: &str, v: i32) -> Error {
    Error::new(sqlstate::INVALID_PARAMETER_VALUE, format!("invalid value for parameter \"{name}\": {v}"))
}

/// Subject string converted to code points, with byte offsets.
struct Subject<'s> {
    s: &'s str,
    chars: Vec<u32>,
    /// byte offset of each char (len+1 entries); None if ASCII
    offs: Option<Vec<usize>>,
}

impl<'s> Subject<'s> {
    fn new(s: &'s str) -> Subject<'s> {
        if s.is_ascii() {
            Subject { s, chars: s.bytes().map(u32::from).collect(), offs: None }
        } else {
            let mut chars = Vec::with_capacity(s.len());
            let mut offs = Vec::with_capacity(s.len() + 1);
            for (i, c) in s.char_indices() {
                chars.push(c as u32);
                offs.push(i);
            }
            offs.push(s.len());
            Subject { s, chars, offs: Some(offs) }
        }
    }

    #[inline]
    fn byte(&self, ci: usize) -> usize {
        match &self.offs {
            None => ci,
            Some(o) => o[ci],
        }
    }

    fn slice(&self, so: i64, eo: i64) -> &'s str {
        &self.s[self.byte(so as usize)..self.byte(eo as usize)]
    }

    fn len(&self) -> usize {
        self.chars.len()
    }
}

/// One match found by the matching loop: overall match and, if requested,
/// the subpattern locations.
struct MatchLoc {
    whole: (i64, i64),
    groups: Vec<(i64, i64)>,
}

impl Regex {
    /// Compiles `pattern` with regexp flags as accepted by PostgreSQL's
    /// regexp_* functions (`b c e i m n p q s t w x`, and `g`, which is only
    /// recorded; see [`Regex::global_flag`]). An empty flag string gives the
    /// semantics of the `~` operator; use `"i"` for `~*`.
    pub fn new(pattern: &str, flags: &str) -> Result<Regex> {
        let (cflags, glob) = parse_re_flags(flags)?;
        Self::compile(pattern, cflags, glob)
    }

    fn compile(pattern: &str, cflags: u32, glob: bool) -> Result<Regex> {
        let chars: Vec<u32> = pattern.chars().map(|c| c as u32).collect();
        let big = chars.len() > 2000;
        let parsed = if big {
            // Very large patterns recurse deeply; give the parser room.
            let chars2 = chars.clone();
            std::thread::Builder::new()
                .stack_size(256 << 20)
                .spawn(move || compile::parse_regex(&chars2, cflags).and_then(Compiled::from_parsed))
                .ok()
                .and_then(|h| h.join().ok())
                .unwrap_or(Err(compile::REG_ETOOBIG))
        } else {
            compile::parse_regex(&chars, cflags).and_then(Compiled::from_parsed)
        };
        match parsed {
            Ok(c) => Ok(Regex { inner: Arc::new(c), glob }),
            Err(code) => Err(Error::new(
                sqlstate::INVALID_REGULAR_EXPRESSION,
                format!("invalid regular expression: {}", compile::reg_error_message(code)),
            )),
        }
    }

    /// Like [`Regex::new`], but compiled expressions are kept in a small
    /// process-wide most-recently-used cache keyed by (pattern, flags), like
    /// PostgreSQL's RE_compile_and_cache. Use this when patterns come from
    /// data (e.g. `col ~ other_col`). Compilation errors are not cached.
    pub fn new_cached(pattern: &str, flags: &str) -> Result<Regex> {
        use std::sync::Mutex;
        const CAPACITY: usize = 64;
        static CACHE: Mutex<Vec<(String, String, Regex)>> = Mutex::new(Vec::new());
        {
            let mut c = CACHE.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(i) = c.iter().position(|(p, f, _)| p == pattern && f == flags) {
                let hit = c[i].2.clone();
                if i > 0 {
                    let e = c.remove(i);
                    c.insert(0, e);
                }
                return Ok(hit);
            }
        }
        let re = Regex::new(pattern, flags)?;
        let mut c = CACHE.lock().unwrap_or_else(|e| e.into_inner());
        c.insert(0, (pattern.to_string(), flags.to_string(), re.clone()));
        c.truncate(CAPACITY);
        Ok(re)
    }

    /// Whether the `g` flag was given.
    pub fn global_flag(&self) -> bool {
        self.glob
    }

    /// The error PostgreSQL raises when `func` (e.g. "regexp_match()") is
    /// given the `g` flag; Ok if the flag was not given.
    pub fn reject_global(&self, func: &str) -> Result<()> {
        if self.glob {
            return Err(Error::new(
                sqlstate::INVALID_PARAMETER_VALUE,
                format!("{func} does not support the \"global\" option"),
            ));
        }
        Ok(())
    }

    /// Number of capturing subexpressions.
    pub fn captures_len(&self) -> usize {
        self.inner.nsub
    }

    /// `~` / `~*` / `regexp_like`: does the pattern match anywhere in `s`?
    pub fn is_match(&self, s: &str) -> bool {
        self.try_is_match(s).unwrap_or(false)
    }

    /// Like [`Regex::is_match`] but reports execution errors (e.g. "regular
    /// expression failed: regular expression is too complex").
    pub fn try_is_match(&self, s: &str) -> Result<bool> {
        if let Some(r) = self.inner.dfa_is_match(s) {
            return Ok(r);
        }
        self.run(|| {
            let subj = Subject::new(s);
            let mut m = Matcher::new(&self.inner, &subj.chars);
            m.exec(0, 0).map_err(exec_error)
        })
    }

    /// Runs `f` directly, or on a thread with a large stack when the regex
    /// has a very deep subexpression tree (matching is recursive).
    fn run<R: Send>(&self, f: impl FnOnce() -> R + Send) -> R {
        if !self.inner.deep {
            return f();
        }
        std::thread::scope(|sc| {
            let h = std::thread::Builder::new()
                .stack_size(256 << 20)
                .spawn_scoped(sc, f)
                .expect("failed to spawn regex matching thread");
            h.join().unwrap_or_else(|e| std::panic::resume_unwind(e))
        })
    }

    /// The matching loop of setup_regexp_matches().
    fn find_all(
        &self,
        subj: &Subject,
        start_search: usize,
        use_subpatterns: bool,
        ignore_degenerate: bool,
        glob: bool,
    ) -> Result<Vec<MatchLoc>> {
        self.run(|| self.find_all_inner(subj, start_search, use_subpatterns, ignore_degenerate, glob))
    }

    fn find_all_inner(
        &self,
        subj: &Subject,
        start_search: usize,
        use_subpatterns: bool,
        ignore_degenerate: bool,
        glob: bool,
    ) -> Result<Vec<MatchLoc>> {
        let nsub = self.inner.nsub;
        let use_sub = use_subpatterns && nsub > 0;
        let nmatch = if use_sub { nsub + 1 } else { 1 };
        let mut m = Matcher::new(&self.inner, &subj.chars);
        let len = subj.len() as i64;
        let mut out = Vec::new();
        let mut prev_match_end: i64 = 0;
        let mut start = start_search;
        loop {
            if !m.exec(start, nmatch).map_err(exec_error)? {
                break;
            }
            let (so, eo) = m.pmatch[0];
            if !ignore_degenerate || (so < len && eo > prev_match_end) {
                let groups = if use_sub { m.pmatch[1..=nsub].to_vec() } else { Vec::new() };
                out.push(MatchLoc { whole: (so, eo), groups });
            }
            prev_match_end = eo;
            if !glob {
                break;
            }
            start = eo as usize;
            if so == eo {
                start += 1;
            }
            if start as i64 > len {
                break;
            }
        }
        Ok(out)
    }

    fn loc_str(subj: &Subject, (so, eo): (i64, i64)) -> Option<String> {
        if so < 0 || eo < 0 { None } else { Some(subj.slice(so, eo).to_string()) }
    }

    /// `regexp_match(s, pattern [, flags])`: the captured substrings of the
    /// first match (the whole match if the pattern has no capturing
    /// parentheses); `None` if there is no match.
    pub fn match_groups(&self, s: &str) -> Option<Vec<Option<String>>> {
        self.try_match_groups(s).unwrap_or(None)
    }

    pub fn try_match_groups(&self, s: &str) -> Result<Option<Vec<Option<String>>>> {
        let subj = Subject::new(s);
        let ms = self.find_all(&subj, 0, true, false, false)?;
        Ok(ms.first().map(|m| self.match_result(&subj, m)))
    }

    fn match_result(&self, subj: &Subject, m: &MatchLoc) -> Vec<Option<String>> {
        if self.inner.nsub > 0 {
            m.groups.iter().map(|&l| Self::loc_str(subj, l)).collect()
        } else {
            vec![Self::loc_str(subj, m.whole)]
        }
    }

    /// `regexp_matches(s, pattern, 'g')`: one array per match.
    pub fn all_matches(&self, s: &str) -> Vec<Vec<Option<String>>> {
        self.try_all_matches(s, true).unwrap_or_default()
    }

    /// `regexp_matches(s, pattern [, flags])` with explicit global-ness.
    pub fn try_all_matches(&self, s: &str, global: bool) -> Result<Vec<Vec<Option<String>>>> {
        let subj = Subject::new(s);
        let ms = self.find_all(&subj, 0, true, false, global)?;
        Ok(ms.iter().map(|m| self.match_result(&subj, m)).collect())
    }

    /// `regexp_split_to_array(s, pattern [, flags])` / `regexp_split_to_table`.
    pub fn split(&self, s: &str) -> Vec<String> {
        self.try_split(s).unwrap_or_default()
    }

    pub fn try_split(&self, s: &str) -> Result<Vec<String>> {
        let subj = Subject::new(s);
        let ms = self.find_all(&subj, 0, false, true, true)?;
        let mut out = Vec::with_capacity(ms.len() + 1);
        let mut startpos: i64 = 0;
        for m in &ms {
            out.push(subj.slice(startpos, m.whole.0).to_string());
            startpos = m.whole.1;
        }
        out.push(subj.slice(startpos, subj.len() as i64).to_string());
        Ok(out)
    }

    /// `regexp_count(s, pattern [, start [, flags]])`.
    pub fn count(&self, s: &str, start: i32) -> Result<i32> {
        if start <= 0 {
            return Err(invalid_param("start", start));
        }
        let subj = Subject::new(s);
        let ms = self.find_all(&subj, (start - 1) as usize, false, false, true)?;
        Ok(ms.len() as i32)
    }

    /// `regexp_instr(s, pattern [, start [, n [, endoption [, flags [, subexpr]]]]])`.
    pub fn instr(&self, s: &str, start: i32, n: i32, endoption: i32, subexpr: i32) -> Result<i32> {
        if start <= 0 {
            return Err(invalid_param("start", start));
        }
        if n <= 0 {
            return Err(invalid_param("n", n));
        }
        if endoption != 0 && endoption != 1 {
            return Err(invalid_param("endoption", endoption));
        }
        if subexpr < 0 {
            return Err(invalid_param("subexpr", subexpr));
        }
        let subj = Subject::new(s);
        let ms = self.find_all(&subj, (start - 1) as usize, subexpr > 0, false, true)?;
        let npatterns = if subexpr > 0 && self.inner.nsub > 0 { self.inner.nsub } else { 1 };
        if n as usize > ms.len() || subexpr as usize > npatterns {
            return Ok(0);
        }
        let m = &ms[(n - 1) as usize];
        let loc = if subexpr > 0 && self.inner.nsub > 0 { m.groups[(subexpr - 1) as usize] } else { m.whole };
        let v = if endoption == 1 { loc.1 } else { loc.0 };
        Ok(if v >= 0 { (v + 1) as i32 } else { 0 })
    }

    /// `regexp_substr(s, pattern [, start [, n [, flags [, subexpr]]]])`.
    pub fn substr(&self, s: &str, start: i32, n: i32, subexpr: i32) -> Result<Option<String>> {
        if start <= 0 {
            return Err(invalid_param("start", start));
        }
        if n <= 0 {
            return Err(invalid_param("n", n));
        }
        if subexpr < 0 {
            return Err(invalid_param("subexpr", subexpr));
        }
        let subj = Subject::new(s);
        let ms = self.find_all(&subj, (start - 1) as usize, subexpr > 0, false, true)?;
        let npatterns = if subexpr > 0 && self.inner.nsub > 0 { self.inner.nsub } else { 1 };
        if n as usize > ms.len() || subexpr as usize > npatterns {
            return Ok(None);
        }
        let m = &ms[(n - 1) as usize];
        let loc = if subexpr > 0 && self.inner.nsub > 0 { m.groups[(subexpr - 1) as usize] } else { m.whole };
        Ok(Self::loc_str(&subj, loc))
    }

    /// `regexp_replace(s, pattern, replacement [, flags])`: replaces the first
    /// match, or all matches when `global`. `\1`..`\9` and `\&` in the
    /// replacement refer to submatches / the whole match; `\\` is a
    /// backslash.
    pub fn replace(&self, s: &str, replacement: &str, global: bool) -> String {
        self.replace_text(s, replacement, 0, if global { 0 } else { 1 }).unwrap_or_else(|_| s.to_string())
    }

    /// Like [`Regex::replace`] but reports execution errors.
    pub fn try_replace(&self, s: &str, replacement: &str, global: bool) -> Result<String> {
        self.replace_text(s, replacement, 0, if global { 0 } else { 1 })
    }

    /// `regexp_replace(s, pattern, replacement, start [, n [, flags]])`
    /// (PostgreSQL 16+): searching starts at character `start` (1-based);
    /// replaces the `n`-th match, or all matches from `start` if `n` = 0.
    pub fn replace_nth(&self, s: &str, replacement: &str, start: i32, n: i32) -> Result<String> {
        if start <= 0 {
            return Err(invalid_param("start", start));
        }
        if n < 0 {
            return Err(invalid_param("n", n));
        }
        self.replace_text(s, replacement, (start - 1) as usize, n)
    }

    /// replace_text_regexp() from varlena.c.
    fn replace_text(&self, src: &str, repl: &str, search_start: usize, n: i32) -> Result<String> {
        self.run(|| self.replace_text_inner(src, repl, search_start, n))
    }

    fn replace_text_inner(&self, src: &str, repl: &str, search_start: usize, n: i32) -> Result<String> {
        let subj = Subject::new(src);
        let data_len = subj.len();
        let escape_status = check_replace_text_has_escape(repl);
        let nmatch = if escape_status < 2 { 1 } else { 10 };
        let mut m = Matcher::new(&self.inner, &subj.chars);
        let mut buf = String::with_capacity(src.len());
        let mut data_pos: usize = 0;
        let mut nmatches = 0;
        let mut search_start = search_start;
        while search_start <= data_len {
            if !m.exec(search_start, nmatch).map_err(exec_error)? {
                break;
            }
            let (so, eo) = (m.pmatch[0].0 as usize, m.pmatch[0].1 as usize);
            nmatches += 1;
            if n > 0 && nmatches != n {
                search_start = eo;
                if so == eo {
                    search_start += 1;
                }
                continue;
            }
            if so > data_pos {
                buf.push_str(&src[subj.byte(data_pos)..subj.byte(so)]);
            }
            if escape_status > 0 {
                append_regexp_substr(&mut buf, repl, &m.pmatch, &subj);
            } else {
                buf.push_str(repl);
            }
            data_pos = eo;
            if n > 0 {
                break;
            }
            search_start = data_pos;
            if so == eo {
                search_start += 1;
            }
        }
        if data_pos < data_len {
            buf.push_str(&src[subj.byte(data_pos)..]);
        }
        Ok(buf)
    }
}

/// check_replace_text_has_escape(): 0 = no backslashes, 1 = escapes but no
/// \digit, 2 = has a submatch reference.
fn check_replace_text_has_escape(r: &str) -> i32 {
    let b = r.as_bytes();
    let mut result = 0;
    let mut i = 0;
    while i < b.len() {
        match b[i..].iter().position(|&c| c == b'\\') {
            None => break,
            Some(off) => {
                i += off + 1;
                if i < b.len() {
                    if (b'1'..=b'9').contains(&b[i]) {
                        return 2;
                    }
                    result = 1;
                    i += 1;
                }
            }
        }
    }
    result
}

/// appendStringInfoRegexpSubstr()
fn append_regexp_substr(buf: &mut String, repl: &str, pmatch: &[(i64, i64)], subj: &Subject) {
    let b = repl.as_bytes();
    let mut p = 0;
    while p < b.len() {
        let chunk_start = p;
        let q = match b[p..].iter().position(|&c| c == b'\\') {
            None => b.len(),
            Some(off) => p + off,
        };
        if q > chunk_start {
            buf.push_str(&repl[chunk_start..q]);
        }
        if q >= b.len() {
            break;
        }
        p = q + 1;
        if p >= b.len() {
            buf.push('\\');
            break;
        }
        let (so, eo);
        if (b'1'..=b'9').contains(&b[p]) {
            let idx = (b[p] - b'0') as usize;
            (so, eo) = pmatch.get(idx).copied().unwrap_or((-1, -1));
            p += 1;
        } else if b[p] == b'&' {
            (so, eo) = pmatch[0];
            p += 1;
        } else if b[p] == b'\\' {
            buf.push('\\');
            p += 1;
            continue;
        } else {
            buf.push('\\');
            continue;
        }
        if so >= 0 && eo >= 0 {
            buf.push_str(subj.slice(so, eo));
        }
    }
}

/// `substring(s from pattern)` (POSIX form): the part matching the first
/// parenthesized subexpression if there is one, else the whole match;
/// `None` if there is no match (or the subexpression did not participate).
pub fn regex_substring(s: &str, pattern: &str) -> Result<Option<String>> {
    let re = Regex::compile(pattern, REG_ADVANCED, false)?;
    re.run(|| {
        let subj = Subject::new(s);
        let mut m = Matcher::new(&re.inner, &subj.chars);
        if !m.exec(0, 2).map_err(exec_error)? {
            return Ok(None);
        }
        let (so, eo) = if re.inner.nsub > 0 { m.pmatch[1] } else { m.pmatch[0] };
        if so < 0 || eo < 0 {
            return Ok(None);
        }
        Ok(Some(subj.slice(so, eo).to_string()))
    })
}

/// `substring(s similar pattern escape esc)` (SQL regular expression form):
/// translates the pattern with similar_to_regex and returns the part
/// matched between the escape-double-quote separators (or the whole match).
pub fn similar_substring(s: &str, pattern: &str, escape: Option<char>) -> Result<Option<String>> {
    let re_text = super::similar::similar_to_regex(pattern, escape)?;
    regex_substring(s, &re_text)
}
