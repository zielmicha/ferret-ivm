//! LIKE / ILIKE, ported from PostgreSQL's src/backend/utils/adt/like.c and
//! like_match.c (PostgreSQL License, Copyright (c) 1996-2024, PostgreSQL
//! Global Development Group, Portions Copyright (c) 1994, Regents of the
//! University of California).
//!
//! Semantics for database encoding UTF8 and a deterministic builtin
//! collation: matching is byte-wise with `_` consuming one character. ILIKE
//! lower-cases both the pattern (after ESCAPE processing) and the subject
//! with `lower()` (Unicode simple lowercase mapping) and then matches.

use crate::{Error, Result, sqlstate};

fn trailing_escape_error() -> Error {
    Error::new(sqlstate::INVALID_ESCAPE_SEQUENCE, "LIKE pattern must not end with escape character")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LikeRes {
    True,
    False,
    Abort,
}

#[inline]
fn utf8_len(b: u8) -> usize {
    if b < 0x80 {
        1
    } else if b & 0xe0 == 0xc0 {
        2
    } else if b & 0xf0 == 0xe0 {
        3
    } else if b & 0xf8 == 0xf0 {
        4
    } else {
        1
    }
}

/// Maximum MatchText recursion depth before PostgreSQL 17 (default
/// max_stack_depth = 2MB, top-level SELECT) reports "stack depth limit
/// exceeded"; measured per call path. Approximate by nature.
pub(crate) const LIKE_MAX_DEPTH: usize = 32728;
pub(crate) const ILIKE_MAX_DEPTH: usize = 32729;
pub(crate) const BYTEA_LIKE_MAX_DEPTH: usize = 32737;

fn stack_depth_error() -> Error {
    Error::new(sqlstate::STATEMENT_TOO_COMPLEX, "stack depth limit exceeded")
}

/// Port of `MatchText` (like_match.c), with the recursion replaced by an
/// explicit stack of `%` choice points (same traversal order, so errors are
/// raised exactly when PostgreSQL raises them). `char_mode` selects UTF-8
/// character stepping for `_`/`%` (text) versus byte stepping (bytea). The
/// pattern uses `\` as its escape character.
fn match_text(t: &[u8], p: &[u8], char_mode: bool, max_depth: usize) -> Result<LikeRes> {
    struct Frame {
        pi: usize,
        ti: usize,
        firstpat: u8,
    }
    let (tlen, plen) = (t.len(), p.len());
    let next_char = |ti: usize| -> usize { if char_mode { (ti + utf8_len(t[ti])).min(tlen) } else { ti + 1 } };
    let mut stack: Vec<Frame> = Vec::new();
    let (mut ti, mut pi) = (0usize, 0usize);
    'calls: loop {
        // One invocation of MatchText on t[ti..], p[pi..].
        let res = 'call: {
            // Fast path for match-everything pattern
            if plen - pi == 1 && p[pi] == b'%' {
                break 'call LikeRes::True;
            }
            if stack.len() + 1 > max_depth {
                return Err(stack_depth_error());
            }
            while ti < tlen && pi < plen {
                if p[pi] == b'\\' {
                    // Next pattern byte must match literally, whatever it is
                    pi += 1;
                    if pi >= plen {
                        return Err(trailing_escape_error());
                    }
                    if p[pi] != t[ti] {
                        break 'call LikeRes::False;
                    }
                } else if p[pi] == b'%' {
                    pi += 1;
                    while pi < plen {
                        if p[pi] == b'%' {
                            pi += 1;
                        } else if p[pi] == b'_' {
                            if ti >= tlen {
                                break 'call LikeRes::Abort;
                            }
                            ti = next_char(ti);
                            pi += 1;
                        } else {
                            break;
                        }
                    }
                    if pi >= plen {
                        break 'call LikeRes::True;
                    }
                    let firstpat = if p[pi] == b'\\' {
                        if plen - pi < 2 {
                            return Err(trailing_escape_error());
                        }
                        p[pi + 1]
                    } else {
                        p[pi]
                    };
                    while ti < tlen && t[ti] != firstpat {
                        ti = next_char(ti);
                    }
                    if ti >= tlen {
                        break 'call LikeRes::Abort;
                    }
                    // "recursive call" at this candidate position
                    stack.push(Frame { pi, ti, firstpat });
                    continue 'calls;
                } else if p[pi] == b'_' {
                    ti = next_char(ti);
                    pi += 1;
                    continue;
                } else if p[pi] != t[ti] {
                    break 'call LikeRes::False;
                }
                ti += 1;
                pi += 1;
            }
            if ti < tlen {
                break 'call LikeRes::False;
            }
            while pi < plen && p[pi] == b'%' {
                pi += 1;
            }
            if pi >= plen {
                break 'call LikeRes::True;
            }
            LikeRes::Abort
        };
        // Return `res` to the parent invocation. TRUE and ABORT propagate to
        // the top; FALSE makes the parent try its next candidate position.
        match res {
            LikeRes::True | LikeRes::Abort => return Ok(res),
            LikeRes::False => {
                let Some(f) = stack.last_mut() else {
                    return Ok(LikeRes::False);
                };
                let mut nti = next_char(f.ti);
                while nti < tlen && t[nti] != f.firstpat {
                    nti = next_char(nti);
                }
                if nti >= tlen {
                    // the parent returns ABORT, which propagates to the top
                    return Ok(LikeRes::Abort);
                }
                f.ti = nti;
                ti = nti;
                pi = f.pi;
            }
        }
    }
}

/// Port of `do_like_escape`: converts a pattern using `escape` (None = no
/// escape character, i.e. `ESCAPE ''`) to one using backslash.
fn like_escape(pat: &[u8], escape: Option<&[u8]>, char_mode: bool) -> Vec<u8> {
    let mut r = Vec::with_capacity(pat.len() * 2);
    let step = |i: usize| -> usize { if char_mode { (i + utf8_len(pat[i])).min(pat.len()) } else { i + 1 } };
    match escape {
        None => {
            let mut i = 0;
            while i < pat.len() {
                if pat[i] == b'\\' {
                    r.push(b'\\');
                }
                let j = step(i);
                r.extend_from_slice(&pat[i..j]);
                i = j;
            }
        }
        Some(e) if e == b"\\" => r.extend_from_slice(pat),
        Some(e) => {
            let mut afterescape = false;
            let mut i = 0;
            while i < pat.len() {
                let j = step(i);
                if &pat[i..j] == e && !afterescape {
                    r.push(b'\\');
                    afterescape = true;
                } else if pat[i] == b'\\' {
                    r.push(b'\\');
                    if !afterescape {
                        r.push(b'\\');
                    }
                    afterescape = false;
                } else {
                    r.extend_from_slice(&pat[i..j]);
                    afterescape = false;
                }
                i = j;
            }
        }
    }
    r
}

/// Returns true if matching could ever reach the trailing-escape error,
/// i.e. the pattern ends with an unpaired escape character.
fn has_trailing_escape(p: &[u8]) -> bool {
    let mut i = 0;
    while i < p.len() {
        if p[i] == b'\\' {
            if i + 1 >= p.len() {
                return true;
            }
            i += 2;
        } else {
            i += 1;
        }
    }
    false
}

#[derive(Debug, Clone)]
enum Kind {
    /// Pattern is exactly "%".
    All,
    /// No wildcards: equality with the literal.
    Exact(Vec<u8>),
    /// literal followed by one or more '%'.
    Prefix(Vec<u8>),
    /// one or more '%' followed by literal.
    Suffix(Vec<u8>),
    /// '%' literal '%'.
    Contains(Vec<u8>),
    /// Anything else: use the general matcher.
    General,
}

/// Splits an escape-normalized pattern into a fast-path class.
fn classify(p: &[u8]) -> Kind {
    if p == b"%" {
        return Kind::All;
    }
    // Parse into tokens: literal bytes / '%' / '_'.
    let mut lead_pct = 0usize;
    let mut i = 0;
    while i < p.len() && p[i] == b'%' {
        lead_pct += 1;
        i += 1;
    }
    let mut lit = Vec::new();
    while i < p.len() {
        match p[i] {
            b'\\' => {
                if i + 1 >= p.len() {
                    return Kind::General;
                }
                lit.push(p[i + 1]);
                i += 2;
            }
            b'%' | b'_' => break,
            c => {
                lit.push(c);
                i += 1;
            }
        }
    }
    let mut trail_pct = 0usize;
    while i < p.len() && p[i] == b'%' {
        trail_pct += 1;
        i += 1;
    }
    if i != p.len() {
        return Kind::General;
    }
    match (lead_pct > 0, trail_pct > 0) {
        (false, false) => Kind::Exact(lit),
        (false, true) => Kind::Prefix(lit),
        (true, false) => Kind::Suffix(lit),
        (true, true) => {
            if lit.is_empty() {
                Kind::All
            } else {
                Kind::Contains(lit)
            }
        }
    }
}

fn find_bytes(h: &[u8], n: &[u8]) -> bool {
    if n.is_empty() {
        return true;
    }
    if n.len() > h.len() {
        return false;
    }
    let first = n[0];
    let last_start = h.len() - n.len();
    let mut i = 0;
    while i <= last_start {
        match h[i..=last_start].iter().position(|&b| b == first) {
            None => return false,
            Some(off) => {
                let s = i + off;
                if &h[s..s + n.len()] == n {
                    return true;
                }
                i = s + 1;
            }
        }
    }
    false
}

/// A compiled LIKE / ILIKE pattern.
#[derive(Debug, Clone)]
pub struct LikePattern {
    /// Escape-normalized pattern (backslash escapes), lower-cased for ILIKE.
    pat: Vec<u8>,
    ci: bool,
    kind: Kind,
    trailing_escape: bool,
}

impl LikePattern {
    /// Compiles `pattern [NOT] [I]LIKE ... ESCAPE escape`. `escape` is the
    /// escape character (`Some('\\')` is the default when there is no ESCAPE
    /// clause); `None` means `ESCAPE ''` (no escape character).
    ///
    /// Errors with "LIKE pattern must not end with escape character" (22025)
    /// if the pattern ends with an unpaired escape. Note that PostgreSQL
    /// raises this error lazily, only when matching reaches the end of the
    /// pattern; use [`LikePattern::new_lenient`] + [`LikePattern::try_matches`]
    /// for exactly that behaviour.
    pub fn new(pattern: &str, escape: Option<char>, case_insensitive: bool) -> Result<LikePattern> {
        let p = Self::new_lenient(pattern, escape, case_insensitive);
        if p.trailing_escape {
            return Err(trailing_escape_error());
        }
        Ok(p)
    }

    /// Like [`LikePattern::new`] but never fails; a trailing escape is
    /// reported by [`LikePattern::try_matches`] exactly when PostgreSQL would
    /// report it (and makes [`LikePattern::matches`] return false).
    pub fn new_lenient(pattern: &str, escape: Option<char>, case_insensitive: bool) -> LikePattern {
        let mut ebuf = [0u8; 4];
        let esc = escape.map(|c| &*c.encode_utf8(&mut ebuf));
        let mut pat = like_escape(pattern.as_bytes(), esc.map(|s| s.as_bytes()), true);
        if case_insensitive {
            // lower() of the escaped pattern; it is valid UTF-8 because the
            // conversion only inserts ASCII backslashes.
            let s = String::from_utf8(pat).expect("valid UTF-8");
            pat = crate::text::lower(&s).into_bytes();
        }
        let trailing_escape = has_trailing_escape(&pat);
        let kind = if trailing_escape { Kind::General } else { classify(&pat) };
        LikePattern { pat, ci: case_insensitive, kind, trailing_escape }
    }

    /// Tests whether `s` matches. If the pattern ends with an unpaired escape
    /// (only possible with [`LikePattern::new_lenient`]) and PostgreSQL would
    /// raise an error for this input, returns false.
    pub fn matches(&self, s: &str) -> bool {
        self.try_matches(s).unwrap_or(false)
    }

    /// Tests whether `s` matches, reporting PostgreSQL's lazily raised
    /// "LIKE pattern must not end with escape character" error.
    pub fn try_matches(&self, s: &str) -> Result<bool> {
        let lowered;
        let t: &[u8] = if self.ci {
            lowered = crate::text::lower(s);
            lowered.as_bytes()
        } else {
            s.as_bytes()
        };
        Ok(match &self.kind {
            Kind::All => true,
            Kind::Exact(l) => t == l.as_slice(),
            Kind::Prefix(l) => t.starts_with(l),
            Kind::Suffix(l) => t.ends_with(l),
            Kind::Contains(l) => find_bytes(t, l),
            Kind::General => {
                let max_depth = if self.ci { ILIKE_MAX_DEPTH } else { LIKE_MAX_DEPTH };
                match_text(t, &self.pat, true, max_depth)? == LikeRes::True
            }
        })
    }
}

/// `bytea LIKE bytea [ESCAPE bytea]`: byte-wise matching where `_` matches
/// one byte. `escape` = `Some(b'\\')` by default; `None` means no escape.
pub fn like_bytes(s: &[u8], pattern: &[u8], escape: Option<u8>) -> Result<bool> {
    let ebuf;
    let esc = match escape {
        Some(e) => {
            ebuf = [e];
            Some(&ebuf[..])
        }
        None => None,
    };
    let pat = like_escape(pattern, esc, false);
    Ok(match_text(s, &pat, false, BYTEA_LIKE_MAX_DEPTH)? == LikeRes::True)
}

/// `like_escape(pattern, escape)` on text, with PostgreSQL's validation of
/// the escape string ("invalid escape string" when longer than one
/// character). Returns the backslash-escaped pattern.
pub fn like_escape_text(pattern: &str, escape: &str) -> Result<String> {
    let mut chars = escape.chars();
    let esc = match (chars.next(), chars.next()) {
        (None, _) => None,
        (Some(c), None) => Some(c),
        _ => {
            return Err(Error::new(sqlstate::INVALID_ESCAPE_SEQUENCE, "invalid escape string"));
        }
    };
    let mut ebuf = [0u8; 4];
    let e = esc.map(|c| c.encode_utf8(&mut ebuf).as_bytes().to_vec());
    let r = like_escape(pattern.as_bytes(), e.as_deref(), true);
    Ok(String::from_utf8(r).expect("valid UTF-8"))
}
