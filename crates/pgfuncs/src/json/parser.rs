//! JSON lexer and parser: a port of src/common/jsonapi.c (PostgreSQL 17,
//! Copyright (c) 1996-2024, PostgreSQL Global Development Group; PostgreSQL
//! License).
//!
//! PostgreSQL's recursive-descent parser is reproduced as an explicit-stack
//! state machine that performs the same lexer calls, semantic callbacks and
//! error checks in the same order, so error precedence and messages match.
//! PostgreSQL's recursion is bounded by `check_stack_depth()` (2MB default
//! `max_stack_depth`); that limit is emulated with per-container costs
//! calibrated against PostgreSQL 17 (about 16354 nested arrays or 14537
//! nested objects for jsonb input).

use crate::{Error, Result, sqlstate};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Tok {
    ObjectStart,
    ObjectEnd,
    ArrayStart,
    ArrayEnd,
    Comma,
    Colon,
    String,
    Number,
    True,
    False,
    Null,
    End,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ParseErr {
    InvalidToken,
    EscapingInvalid,
    EscapingRequired,
    ExpectedEnd,
    ExpectedArrayFirst,
    ExpectedArrayNext,
    ExpectedColon,
    ExpectedJson,
    ExpectedMore,
    ExpectedObjectFirst,
    ExpectedObjectNext,
    ExpectedString,
    UnicodeCodePointZero,
    UnicodeEscapeFormat,
    UnicodeHighSurrogate,
    UnicodeLowSurrogate,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Ctx {
    End,
    Value,
    String,
    ArrayStart,
    ArrayNext,
    ObjectStart,
    ObjectLabel,
    ObjectNext,
}

/// Stack-depth emulation (see module docs).
pub(crate) const BUDGET_JSONB: u64 = 1_000_000_000;
pub(crate) const BUDGET_JSON: u64 = 1_000_050_000;
pub(crate) const BUDGET_JSON_GET: u64 = 1_000_190_000;
const COST_ARRAY: u64 = 61_145;
const COST_OBJECT: u64 = 68_787;

#[derive(Clone)]
pub(crate) struct Lexer<'a> {
    pub src: &'a str,
    pub input: &'a [u8],
    /// start of the current token; `None` at end of input
    pub token_start: Option<usize>,
    pub token_terminator: usize,
    pub prev_token_terminator: usize,
    pub token_type: Tok,
    pub lex_level: usize,
    /// de-escape strings into `strval` (PostgreSQL's `need_escapes`)
    pub need_escapes: bool,
    pub strval: String,
}

#[inline]
fn is_alnum(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c >= 0x80
}

/// pg_utf_mblen()
fn utf8_mblen(c: u8) -> usize {
    if c & 0x80 == 0 {
        1
    } else if c & 0xe0 == 0xc0 {
        2
    } else if c & 0xf0 == 0xe0 {
        3
    } else if c & 0xf8 == 0xf0 {
        4
    } else {
        1
    }
}

pub(crate) fn stack_depth_error() -> Error {
    Error::new(
        sqlstate::STATEMENT_TOO_COMPLEX,
        "stack depth limit exceeded",
    )
}

impl<'a> Lexer<'a> {
    pub fn new(src: &'a str, need_escapes: bool) -> Lexer<'a> {
        Lexer {
            src,
            input: src.as_bytes(),
            token_start: Some(0),
            token_terminator: 0,
            prev_token_terminator: 0,
            token_type: Tok::End,
            lex_level: 0,
            need_escapes,
            strval: String::new(),
        }
    }

    fn token_text(&self) -> String {
        let start = self
            .token_start
            .unwrap_or(self.token_terminator)
            .min(self.token_terminator);
        String::from_utf8_lossy(&self.input[start..self.token_terminator]).into_owned()
    }

    /// json_errsave_error() + json_errdetail()
    pub fn error(&self, e: ParseErr) -> Error {
        use ParseErr::*;
        let detail = match e {
            InvalidToken => format!("Token \"{}\" is invalid.", self.token_text()),
            EscapingInvalid => format!("Escape sequence \"\\{}\" is invalid.", self.token_text()),
            EscapingRequired => format!(
                "Character with value 0x{:02x} must be escaped.",
                self.input.get(self.token_terminator).copied().unwrap_or(0)
            ),
            ExpectedEnd => format!(
                "Expected end of input, but found \"{}\".",
                self.token_text()
            ),
            ExpectedArrayFirst => {
                format!(
                    "Expected array element or \"]\", but found \"{}\".",
                    self.token_text()
                )
            }
            ExpectedArrayNext => format!(
                "Expected \",\" or \"]\", but found \"{}\".",
                self.token_text()
            ),
            ExpectedColon => format!("Expected \":\", but found \"{}\".", self.token_text()),
            ExpectedJson => format!("Expected JSON value, but found \"{}\".", self.token_text()),
            ExpectedMore => "The input string ended unexpectedly.".to_owned(),
            ExpectedObjectFirst => format!(
                "Expected string or \"}}\", but found \"{}\".",
                self.token_text()
            ),
            ExpectedObjectNext => format!(
                "Expected \",\" or \"}}\", but found \"{}\".",
                self.token_text()
            ),
            ExpectedString => format!("Expected string, but found \"{}\".", self.token_text()),
            UnicodeCodePointZero => {
                return Error::with_detail(
                    sqlstate::UNTRANSLATABLE_CHARACTER,
                    "unsupported Unicode escape sequence",
                    "\\u0000 cannot be converted to text.",
                );
            }
            UnicodeEscapeFormat => {
                "\"\\u\" must be followed by four hexadecimal digits.".to_owned()
            }
            UnicodeHighSurrogate => {
                "Unicode high surrogate must not follow a high surrogate.".to_owned()
            }
            UnicodeLowSurrogate => "Unicode low surrogate must follow a high surrogate.".to_owned(),
        };
        Error::with_detail(
            sqlstate::INVALID_TEXT_REPRESENTATION,
            "invalid input syntax for type json",
            detail,
        )
    }

    fn fail_at_char_end(&mut self, s: usize, e: ParseErr) -> Error {
        let end = self.input.len();
        let charlen = utf8_mblen(self.input[s]);
        self.token_terminator = if s + charlen <= end { s + charlen } else { end };
        self.error(e)
    }

    #[inline]
    fn single(&mut self, s: usize, t: Tok) {
        self.prev_token_terminator = self.token_terminator;
        self.token_terminator = s + 1;
        self.token_type = t;
    }

    /// json_lex()
    pub fn lex(&mut self) -> Result<()> {
        let b = self.input;
        let end = b.len();
        let mut s = self.token_terminator;
        while s < end && matches!(b[s], b' ' | b'\t' | b'\n' | b'\r') {
            s += 1;
        }
        self.token_start = Some(s);
        if s >= end {
            self.token_start = None;
            self.prev_token_terminator = self.token_terminator;
            self.token_terminator = s;
            self.token_type = Tok::End;
            return Ok(());
        }
        match b[s] {
            b'{' => self.single(s, Tok::ObjectStart),
            b'}' => self.single(s, Tok::ObjectEnd),
            b'[' => self.single(s, Tok::ArrayStart),
            b']' => self.single(s, Tok::ArrayEnd),
            b',' => self.single(s, Tok::Comma),
            b':' => self.single(s, Tok::Colon),
            b'"' => {
                self.lex_string()?;
                self.token_type = Tok::String;
            }
            b'-' => {
                self.lex_number(s + 1)?;
                self.token_type = Tok::Number;
            }
            b'0'..=b'9' => {
                self.lex_number(s)?;
                self.token_type = Tok::Number;
            }
            _ => {
                let mut p = s;
                while p < end && is_alnum(b[p]) {
                    p += 1;
                }
                if p == s {
                    self.prev_token_terminator = self.token_terminator;
                    self.token_terminator = s + 1;
                    return Err(self.error(ParseErr::InvalidToken));
                }
                self.prev_token_terminator = self.token_terminator;
                self.token_terminator = p;
                let w = &b[s..p];
                self.token_type = if w.len() == 4 {
                    if w == b"true" {
                        Tok::True
                    } else if w == b"null" {
                        Tok::Null
                    } else {
                        return Err(self.error(ParseErr::InvalidToken));
                    }
                } else if w == b"false" {
                    Tok::False
                } else {
                    return Err(self.error(ParseErr::InvalidToken));
                };
            }
        }
        Ok(())
    }

    /// json_lex_string()
    fn lex_string(&mut self) -> Result<()> {
        let b = self.input;
        let end = b.len();
        let start = self.token_start.unwrap();
        if self.need_escapes {
            self.strval.clear();
        }
        let mut hi_surrogate: i32 = -1;
        let mut s = start;
        loop {
            s += 1;
            if s >= end {
                self.token_terminator = s;
                return Err(self.error(ParseErr::InvalidToken));
            }
            let c = b[s];
            if c == b'"' {
                break;
            } else if c == b'\\' {
                s += 1;
                if s >= end {
                    self.token_terminator = s;
                    return Err(self.error(ParseErr::InvalidToken));
                }
                if b[s] == b'u' {
                    let mut ch: i32 = 0;
                    for _ in 0..4 {
                        s += 1;
                        if s >= end {
                            self.token_terminator = s;
                            return Err(self.error(ParseErr::InvalidToken));
                        }
                        let d = b[s];
                        let v = match d {
                            b'0'..=b'9' => d - b'0',
                            b'a'..=b'f' => d - b'a' + 10,
                            b'A'..=b'F' => d - b'A' + 10,
                            _ => {
                                return Err(self.fail_at_char_end(s, ParseErr::UnicodeEscapeFormat));
                            }
                        };
                        ch = ch * 16 + i32::from(v);
                    }
                    if self.need_escapes {
                        if (0xD800..=0xDBFF).contains(&ch) {
                            if hi_surrogate != -1 {
                                return Err(
                                    self.fail_at_char_end(s, ParseErr::UnicodeHighSurrogate)
                                );
                            }
                            hi_surrogate = ch;
                            continue;
                        } else if (0xDC00..=0xDFFF).contains(&ch) {
                            if hi_surrogate == -1 {
                                return Err(self.fail_at_char_end(s, ParseErr::UnicodeLowSurrogate));
                            }
                            ch = (((hi_surrogate & 0x3FF) << 10) | (ch & 0x3FF)) + 0x10000;
                            hi_surrogate = -1;
                        }
                        if hi_surrogate != -1 {
                            return Err(self.fail_at_char_end(s, ParseErr::UnicodeLowSurrogate));
                        }
                        if ch == 0 {
                            return Err(self.fail_at_char_end(s, ParseErr::UnicodeCodePointZero));
                        }
                        self.strval
                            .push(char::from_u32(ch as u32).unwrap_or('\u{FFFD}'));
                    }
                } else if self.need_escapes {
                    if hi_surrogate != -1 {
                        return Err(self.fail_at_char_end(s, ParseErr::UnicodeLowSurrogate));
                    }
                    let out = match b[s] {
                        b'"' => '"',
                        b'\\' => '\\',
                        b'/' => '/',
                        b'b' => '\u{8}',
                        b'f' => '\u{c}',
                        b'n' => '\n',
                        b'r' => '\r',
                        b't' => '\t',
                        _ => {
                            self.token_start = Some(s);
                            return Err(self.fail_at_char_end(s, ParseErr::EscapingInvalid));
                        }
                    };
                    self.strval.push(out);
                } else if !b"\"\\/bfnrt\0".contains(&b[s]) {
                    self.token_start = Some(s);
                    return Err(self.fail_at_char_end(s, ParseErr::EscapingInvalid));
                }
            } else {
                if hi_surrogate != -1 {
                    return Err(self.fail_at_char_end(s, ParseErr::UnicodeLowSurrogate));
                }
                let mut p = s;
                while p < end {
                    let c = b[p];
                    if c == b'\\' || c == b'"' {
                        break;
                    } else if c <= 31 {
                        self.token_terminator = p;
                        return Err(self.error(ParseErr::EscapingRequired));
                    }
                    p += 1;
                }
                if self.need_escapes {
                    self.strval.push_str(&self.src[s..p]);
                }
                s = p - 1;
            }
        }
        if hi_surrogate != -1 {
            self.token_terminator = s + 1;
            return Err(self.error(ParseErr::UnicodeLowSurrogate));
        }
        self.prev_token_terminator = self.token_terminator;
        self.token_terminator = s + 1;
        Ok(())
    }

    /// json_lex_number(); `s` points just past an optional leading '-'
    fn lex_number(&mut self, mut s: usize) -> Result<()> {
        let b = self.input;
        let n = b.len();
        let mut error = false;
        if s < n && b[s] == b'0' {
            s += 1;
        } else if s < n && (b'1'..=b'9').contains(&b[s]) {
            loop {
                s += 1;
                if !(s < n && b[s].is_ascii_digit()) {
                    break;
                }
            }
        } else {
            error = true;
        }
        if s < n && b[s] == b'.' {
            s += 1;
            if s == n || !b[s].is_ascii_digit() {
                error = true;
            } else {
                loop {
                    s += 1;
                    if !(s < n && b[s].is_ascii_digit()) {
                        break;
                    }
                }
            }
        }
        if s < n && (b[s] == b'e' || b[s] == b'E') {
            s += 1;
            if s < n && (b[s] == b'+' || b[s] == b'-') {
                s += 1;
            }
            if s == n || !b[s].is_ascii_digit() {
                error = true;
            } else {
                loop {
                    s += 1;
                    if !(s < n && b[s].is_ascii_digit()) {
                        break;
                    }
                }
            }
        }
        while s < n && is_alnum(b[s]) {
            s += 1;
            error = true;
        }
        self.prev_token_terminator = self.token_terminator;
        self.token_terminator = s;
        if error {
            return Err(self.error(ParseErr::InvalidToken));
        }
        Ok(())
    }

    /// report_parse_error()
    pub fn report_parse_error(&self, ctx: Ctx) -> Error {
        if self.token_start.is_none() || self.token_type == Tok::End {
            return self.error(ParseErr::ExpectedMore);
        }
        self.error(match ctx {
            Ctx::End => ParseErr::ExpectedEnd,
            Ctx::Value => ParseErr::ExpectedJson,
            Ctx::String => ParseErr::ExpectedString,
            Ctx::ArrayStart => ParseErr::ExpectedArrayFirst,
            Ctx::ArrayNext => ParseErr::ExpectedArrayNext,
            Ctx::ObjectStart => ParseErr::ExpectedObjectFirst,
            Ctx::ObjectLabel => ParseErr::ExpectedColon,
            Ctx::ObjectNext => ParseErr::ExpectedObjectNext,
        })
    }

    /// lex_expect()
    #[inline]
    pub fn expect(&mut self, ctx: Ctx, t: Tok) -> Result<()> {
        if self.token_type == t {
            self.lex()
        } else {
            Err(self.report_parse_error(ctx))
        }
    }

    /// Raw text of the current token.
    pub fn token_raw(&self) -> &'a str {
        let start = self.token_start.unwrap_or(self.token_terminator);
        &self.src[start..self.token_terminator]
    }
}

/// Scalar value handed to [`Sem::scalar`]: de-escaped string or raw token.
pub(crate) enum Scalar<'a> {
    Str(String),
    Raw(&'a str),
}

/// Semantic callbacks (PostgreSQL's JsonSemAction). The default
/// implementations do nothing.
pub(crate) trait Sem<'a> {
    /// whether field names / scalar values must be extracted
    const WANTS_VALUES: bool = true;
    fn object_start(&mut self, _lex: &Lexer<'a>) -> Result<()> {
        Ok(())
    }
    fn object_end(&mut self, _lex: &Lexer<'a>) -> Result<()> {
        Ok(())
    }
    fn array_start(&mut self, _lex: &Lexer<'a>) -> Result<()> {
        Ok(())
    }
    fn array_end(&mut self, _lex: &Lexer<'a>) -> Result<()> {
        Ok(())
    }
    /// `fname` may be taken by the callback; `object_field_end` then sees an
    /// empty string.
    fn object_field_start(
        &mut self,
        _lex: &Lexer<'a>,
        _fname: &mut String,
        _isnull: bool,
    ) -> Result<()> {
        Ok(())
    }
    fn object_field_end(&mut self, _lex: &Lexer<'a>, _fname: &str, _isnull: bool) -> Result<()> {
        Ok(())
    }
    fn array_element_start(&mut self, _lex: &Lexer<'a>, _isnull: bool) -> Result<()> {
        Ok(())
    }
    fn array_element_end(&mut self, _lex: &Lexer<'a>, _isnull: bool) -> Result<()> {
        Ok(())
    }
    fn scalar(&mut self, _lex: &Lexer<'a>, _val: Scalar<'a>, _tok: Tok) -> Result<()> {
        Ok(())
    }
}

/// Validation only.
pub(crate) struct NullSem;
impl Sem<'_> for NullSem {
    const WANTS_VALUES: bool = false;
}

enum Frame {
    Array { isnull: bool },
    Object { fname: String, isnull: bool },
}

pub(crate) struct Depth {
    pub budget: u64,
    pub used: u64,
}

impl Depth {
    #[inline]
    fn enter(&mut self, cost: u64) -> Result<()> {
        self.used += cost;
        if self.used > self.budget {
            Err(stack_depth_error())
        } else {
            Ok(())
        }
    }
}

/// parse_object_field() up to (and including) the field-start callback.
fn begin_field<'a, S: Sem<'a>>(lex: &mut Lexer<'a>, sem: &mut S) -> Result<(String, bool)> {
    if lex.token_type != Tok::String {
        return Err(lex.report_parse_error(Ctx::String));
    }
    let mut fname = if S::WANTS_VALUES && lex.need_escapes {
        std::mem::take(&mut lex.strval)
    } else {
        String::new()
    };
    lex.lex()?;
    lex.expect(Ctx::ObjectLabel, Tok::Colon)?;
    let isnull = lex.token_type == Tok::Null;
    sem.object_field_start(lex, &mut fname, isnull)?;
    Ok((fname, isnull))
}

/// parse_scalar()
fn parse_scalar<'a, S: Sem<'a>>(lex: &mut Lexer<'a>, sem: &mut S) -> Result<()> {
    let tok = lex.token_type;
    if !matches!(
        tok,
        Tok::String | Tok::Number | Tok::True | Tok::False | Tok::Null
    ) {
        return Err(lex.report_parse_error(Ctx::Value));
    }
    if !S::WANTS_VALUES {
        return lex.lex();
    }
    let val = if tok == Tok::String {
        Scalar::Str(std::mem::take(&mut lex.strval))
    } else {
        Scalar::Raw(lex.token_raw())
    };
    lex.lex()?;
    sem.scalar(lex, val, tok)
}

/// Parses exactly one JSON value starting at the current token (the
/// recursive parse_object / parse_array / parse_scalar of PostgreSQL).
pub(crate) fn parse_value<'a, S: Sem<'a>>(
    lex: &mut Lexer<'a>,
    sem: &mut S,
    depth: &mut Depth,
) -> Result<()> {
    let mut stack: Vec<Frame> = Vec::new();
    'value: loop {
        match lex.token_type {
            Tok::ObjectStart => {
                depth.enter(COST_OBJECT)?;
                sem.object_start(lex)?;
                lex.lex_level += 1;
                lex.lex()?;
                match lex.token_type {
                    Tok::String => {
                        let (fname, isnull) = begin_field(lex, sem)?;
                        stack.push(Frame::Object { fname, isnull });
                        continue 'value;
                    }
                    Tok::ObjectEnd => {
                        lex.expect(Ctx::ObjectNext, Tok::ObjectEnd)?;
                        lex.lex_level -= 1;
                        sem.object_end(lex)?;
                        depth.used -= COST_OBJECT;
                    }
                    _ => return Err(lex.report_parse_error(Ctx::ObjectStart)),
                }
            }
            Tok::ArrayStart => {
                depth.enter(COST_ARRAY)?;
                sem.array_start(lex)?;
                lex.lex_level += 1;
                lex.expect(Ctx::ArrayStart, Tok::ArrayStart)?;
                if lex.token_type != Tok::ArrayEnd {
                    let isnull = lex.token_type == Tok::Null;
                    sem.array_element_start(lex, isnull)?;
                    stack.push(Frame::Array { isnull });
                    continue 'value;
                }
                lex.expect(Ctx::ArrayNext, Tok::ArrayEnd)?;
                lex.lex_level -= 1;
                sem.array_end(lex)?;
                depth.used -= COST_ARRAY;
            }
            _ => parse_scalar(lex, sem)?,
        }
        // A value is complete: finish enclosing containers as far as possible.
        loop {
            match stack.pop() {
                None => return Ok(()),
                Some(Frame::Array { isnull }) => {
                    sem.array_element_end(lex, isnull)?;
                    if lex.token_type == Tok::Comma {
                        lex.lex()?;
                        let isnull = lex.token_type == Tok::Null;
                        sem.array_element_start(lex, isnull)?;
                        stack.push(Frame::Array { isnull });
                        continue 'value;
                    }
                    lex.expect(Ctx::ArrayNext, Tok::ArrayEnd)?;
                    lex.lex_level -= 1;
                    sem.array_end(lex)?;
                    depth.used -= COST_ARRAY;
                }
                Some(Frame::Object { fname, isnull }) => {
                    sem.object_field_end(lex, &fname, isnull)?;
                    if lex.token_type == Tok::Comma {
                        lex.lex()?;
                        let (fname, isnull) = begin_field(lex, sem)?;
                        stack.push(Frame::Object { fname, isnull });
                        continue 'value;
                    }
                    lex.expect(Ctx::ObjectNext, Tok::ObjectEnd)?;
                    lex.lex_level -= 1;
                    sem.object_end(lex)?;
                    depth.used -= COST_OBJECT;
                }
            }
        }
    }
}

/// pg_parse_json(): a complete document.
pub(crate) fn parse_json<'a, S: Sem<'a>>(
    lex: &mut Lexer<'a>,
    sem: &mut S,
    budget: u64,
) -> Result<()> {
    lex.lex()?;
    let mut depth = Depth { budget, used: 0 };
    parse_value(lex, sem, &mut depth)?;
    lex.expect(Ctx::End, Tok::End)
}

/// json_count_array_elements(): the lexer must be positioned on '['.
pub(crate) fn count_array_elements(lex: &Lexer<'_>) -> Result<i64> {
    let mut copy = lex.clone();
    copy.need_escapes = false;
    copy.lex_level += 1;
    let mut count = 0i64;
    copy.expect(Ctx::ArrayStart, Tok::ArrayStart)?;
    let mut depth = Depth {
        budget: BUDGET_JSON_GET,
        used: COST_ARRAY,
    };
    if copy.token_type != Tok::ArrayEnd {
        loop {
            count += 1;
            parse_value(&mut copy, &mut NullSem, &mut depth)?;
            if copy.token_type != Tok::Comma {
                break;
            }
            copy.lex()?;
        }
    }
    copy.expect(Ctx::ArrayNext, Tok::ArrayEnd)?;
    Ok(count)
}
