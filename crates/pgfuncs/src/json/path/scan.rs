//! jsonpath scanner: a port of src/backend/utils/adt/jsonpath_scan.l of
//! PostgreSQL 17 (Copyright (c) 2019-2024, PostgreSQL Global Development
//! Group; PostgreSQL License).
//!
//! The DFA is flex's own (see `tables.rs`); this module re-implements the
//! flex matching loop (longest match, earliest rule on ties, start
//! conditions, `yyless`) and the rule actions. `yytext` is tracked exactly as
//! flex leaves it, because syntax error messages quote it ("at or near
//! \"...\"", or "at end of jsonpath input" when it is empty).

use super::tables::*;
use crate::{Error, Result, sqlstate};

/// Start conditions.
const INITIAL: i16 = 0;
const XQ: i16 = 1;
const XNQ: i16 = 2;
const XVQ: i16 = 3;
const XC: i16 = 4;

/// A token for the parser: bison token code (0 = end of input) and the
/// semantic value `yylval->str` for tokens that carry a string.
pub(super) struct Token {
    pub code: i32,
    pub str: Option<Vec<u8>>,
}

pub(super) struct Scanner<'a> {
    input: &'a [u8],
    /// yy_c_buf_p
    pos: usize,
    start: i16,
    /// yytext as (offset, length); empty at end of input and after yyless(0)
    yytext: (usize, usize),
    scanstring: Vec<u8>,
}

/// jsonpath_yyerror() for a message and the current `yytext`.
pub(super) fn syntax_error(message: &str, yytext: &[u8]) -> Error {
    if yytext.is_empty() {
        Error::new(
            sqlstate::SYNTAX_ERROR,
            format!("{message} at end of jsonpath input"),
        )
    } else {
        Error::new(
            sqlstate::SYNTAX_ERROR,
            format!(
                "{message} at or near \"{}\" of jsonpath input",
                String::from_utf8_lossy(yytext)
            ),
        )
    }
}

fn invalid_syntax_detail(detail: &str) -> Error {
    Error::with_detail(
        sqlstate::INVALID_TEXT_REPRESENTATION,
        "invalid input syntax for type jsonpath",
        detail,
    )
}

/// (keyword, lowercase-only, token) sorted by length, then name.
const KEYWORDS: &[(&str, bool, i32)] = &[
    ("is", false, IS_P),
    ("to", false, TO_P),
    ("abs", false, ABS_P),
    ("lax", false, LAX_P),
    ("date", false, DATE_P),
    ("flag", false, FLAG_P),
    ("last", false, LAST_P),
    ("null", true, NULL_P),
    ("size", false, SIZE_P),
    ("time", false, TIME_P),
    ("true", true, TRUE_P),
    ("type", false, TYPE_P),
    ("with", false, WITH_P),
    ("false", true, FALSE_P),
    ("floor", false, FLOOR_P),
    ("bigint", false, BIGINT_P),
    ("double", false, DOUBLE_P),
    ("exists", false, EXISTS_P),
    ("number", false, NUMBER_P),
    ("starts", false, STARTS_P),
    ("strict", false, STRICT_P),
    ("string", false, STRINGFUNC_P),
    ("boolean", false, BOOLEAN_P),
    ("ceiling", false, CEILING_P),
    ("decimal", false, DECIMAL_P),
    ("integer", false, INTEGER_P),
    ("time_tz", false, TIME_TZ_P),
    ("unknown", false, UNKNOWN_P),
    ("datetime", false, DATETIME_P),
    ("keyvalue", false, KEYVALUE_P),
    ("timestamp", false, TIMESTAMP_P),
    ("like_regex", false, LIKE_REGEX_P),
    ("timestamp_tz", false, TIMESTAMP_TZ_P),
];

/// checkKeyword(): keywords match case-insensitively (ASCII), except
/// null/true/false which must be lowercase.
fn check_keyword(s: &[u8]) -> i32 {
    for &(kw, lowercase, tok) in KEYWORDS {
        if kw.len() == s.len() && kw.as_bytes().eq_ignore_ascii_case(s) {
            if lowercase && kw.as_bytes() != s {
                return IDENT_P;
            }
            return tok;
        }
    }
    IDENT_P
}

fn hexval(c: u8) -> u32 {
    (c as char).to_digit(16).unwrap_or(0)
}

impl<'a> Scanner<'a> {
    pub fn new(input: &'a [u8]) -> Scanner<'a> {
        Scanner {
            input,
            pos: 0,
            start: INITIAL,
            yytext: (0, 0),
            scanstring: Vec::new(),
        }
    }

    pub fn yytext(&self) -> &'a [u8] {
        let (o, l) = self.yytext;
        &self.input[o..o + l]
    }

    fn error(&self, message: &str) -> Error {
        syntax_error(message, self.yytext())
    }

    /// One flex match from the current position: (action, end of match).
    fn match_rule(&self) -> (i16, usize) {
        let b = self.input;
        let bp = self.pos;
        if bp >= b.len() {
            return (YY_END_OF_BUFFER + 1 + self.start, bp);
        }
        let mut state: i16 = 1 + 2 * self.start;
        let mut cp = bp;
        let mut last: Option<(i16, usize)> = None;
        loop {
            if YY_ACCEPT[state as usize] != 0 {
                last = Some((state, cp));
            }
            if cp >= b.len() {
                break;
            }
            let mut c = YY_EC[b[cp] as usize];
            let mut s = state;
            while YY_CHK[(YY_BASE[s as usize] + c as i16) as usize] != s {
                s = YY_DEF[s as usize];
                if s >= YY_TEMPLATE_STATES {
                    c = YY_META[c as usize];
                }
            }
            state = YY_NXT[(YY_BASE[s as usize] + c as i16) as usize];
            cp += 1;
            if state == YY_JAMSTATE {
                break;
            }
        }
        match last {
            Some((s, end)) => (YY_ACCEPT[s as usize], end),
            None => (0, bp),
        }
    }

    fn token(&self, code: i32) -> Token {
        Token { code, str: None }
    }

    fn token_str(&self, code: i32) -> Token {
        Token {
            code,
            str: Some(self.scanstring.clone()),
        }
    }

    /// jsonpath_yylex()
    pub fn lex(&mut self) -> Result<Token> {
        loop {
            let bp = self.pos;
            let (act, end) = self.match_rule();
            self.yytext = (bp, end - bp);
            self.pos = end;
            let text = &self.input[bp..end];
            match act {
                // <xnq>{other}+
                1 => self.scanstring.extend_from_slice(text),
                // <xnq>{blank}+
                2 => {
                    self.start = INITIAL;
                    return Ok(self.token_str(check_keyword(&self.scanstring)));
                }
                // <xnq>\/\*  (the identifier is dropped, as in PostgreSQL)
                3 => self.start = XC,
                // <xnq>({special}|\")
                4 => {
                    self.yyless(0);
                    self.start = INITIAL;
                    return Ok(self.token_str(check_keyword(&self.scanstring)));
                }
                // <xnq,xq,xvq> escapes
                5 => self.scanstring.push(0x08),
                6 => self.scanstring.push(0x0c),
                7 => self.scanstring.push(b'\n'),
                8 => self.scanstring.push(b'\r'),
                9 => self.scanstring.push(b'\t'),
                10 => self.scanstring.push(0x0b),
                // {unicode}+
                11 => self.parse_unicode(text)?,
                // {hex_char}
                12 => {
                    let ch = (hexval(text[2]) << 4) | hexval(text[3]);
                    self.add_unicode_char(ch)?;
                }
                // {unicode}*{unicodefail}
                13 => return Err(self.error("invalid Unicode escape sequence")),
                // {hex_fail}
                14 => return Err(self.error("invalid hexadecimal character sequence")),
                // {unicode}+\\  (throw back the backslash)
                15 => {
                    self.yyless(text.len() - 1);
                    let text = &self.input[bp..self.pos];
                    self.parse_unicode(text)?;
                }
                // \\.
                16 => self.scanstring.push(text[1]),
                // \\
                17 => return Err(self.error("unexpected end after backslash")),
                // <xq>\"
                18 => {
                    self.start = INITIAL;
                    return Ok(self.token_str(STRING_P));
                }
                // <xvq>\"
                19 => {
                    self.start = INITIAL;
                    return Ok(self.token_str(VARIABLE_P));
                }
                // <xq,xvq>[^\\\"]+
                20 => self.scanstring.extend_from_slice(text),
                // <xc>\*\/
                21 => self.start = INITIAL,
                // <xc>[^\*]+  <xc>\*
                22 | 23 => {}
                24 => return Ok(self.token(AND_P)),
                25 => return Ok(self.token(OR_P)),
                26 => return Ok(self.token(NOT_P)),
                27 => return Ok(self.token(ANY_P)),
                28 => return Ok(self.token(LESS_P)),
                29 => return Ok(self.token(LESSEQUAL_P)),
                30 => return Ok(self.token(EQUAL_P)),
                31 | 32 => return Ok(self.token(NOTEQUAL_P)),
                33 => return Ok(self.token(GREATEREQUAL_P)),
                34 => return Ok(self.token(GREATER_P)),
                // \${other}+
                35 => {
                    self.scanstring.clear();
                    self.scanstring.extend_from_slice(&text[1..]);
                    return Ok(self.token_str(VARIABLE_P));
                }
                // \$\"
                36 => {
                    self.scanstring.clear();
                    self.start = XVQ;
                }
                // {special}
                37 => return Ok(self.token(i32::from(text[0]))),
                // {blank}+
                38 => {}
                // \/\*
                39 => {
                    self.scanstring.clear();
                    self.start = XC;
                }
                // {real} {decimal}
                40 | 41 => {
                    self.scanstring.clear();
                    self.scanstring.extend_from_slice(text);
                    return Ok(self.token_str(NUMERIC_P));
                }
                // {decinteger} {hexinteger} {octinteger} {bininteger}
                42..=45 => {
                    self.scanstring.clear();
                    self.scanstring.extend_from_slice(text);
                    return Ok(self.token_str(INT_P));
                }
                46 => return Err(self.error("invalid numeric literal")),
                47..=49 => return Err(self.error("trailing junk after numeric literal")),
                // \"
                50 => {
                    self.scanstring.clear();
                    self.start = XQ;
                }
                // \\  (an identifier starting with an escape)
                51 => {
                    self.yyless(0);
                    self.scanstring.clear();
                    self.start = XNQ;
                }
                // {other}+
                52 => {
                    self.scanstring.clear();
                    self.scanstring.extend_from_slice(text);
                    self.start = XNQ;
                }
                a if a == YY_END_OF_BUFFER + 1 + INITIAL => return Ok(self.token(0)),
                a if a == YY_END_OF_BUFFER + 1 + XNQ => {
                    self.start = INITIAL;
                    return Ok(self.token_str(check_keyword(&self.scanstring)));
                }
                a if a == YY_END_OF_BUFFER + 1 + XQ || a == YY_END_OF_BUFFER + 1 + XVQ => {
                    return Err(self.error("unterminated quoted string"));
                }
                a if a == YY_END_OF_BUFFER + 1 + XC => {
                    return Err(self.error("unexpected end of comment"));
                }
                _ => {
                    return Err(Error::new(sqlstate::INTERNAL_ERROR, "flex scanner jammed"));
                }
            }
        }
    }

    /// yyless(n): keep the first `n` bytes of the current match.
    fn yyless(&mut self, n: usize) {
        let bp = self.yytext.0;
        self.pos = bp + n;
        self.yytext = (bp, n);
    }

    /// addUnicodeChar()
    fn add_unicode_char(&mut self, ch: u32) -> Result<()> {
        if ch == 0 {
            return Err(Error::with_detail(
                sqlstate::UNTRANSLATABLE_CHARACTER,
                "unsupported Unicode escape sequence",
                "\\u0000 cannot be converted to text.",
            ));
        }
        // pg_unicode_to_server()
        match char::from_u32(ch) {
            Some(c) if ch <= 0x10FFFF => {
                let mut buf = [0u8; 4];
                self.scanstring
                    .extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                Ok(())
            }
            _ => Err(Error::new(
                sqlstate::SYNTAX_ERROR,
                "invalid Unicode code point",
            )),
        }
    }

    /// addUnicode(): handles surrogate pairs.
    fn add_unicode(&mut self, ch: u32, hi_surrogate: &mut Option<u32>) -> Result<()> {
        let low_must_follow =
            || invalid_syntax_detail("Unicode low surrogate must follow a high surrogate.");
        if (0xD800..=0xDBFF).contains(&ch) {
            if hi_surrogate.is_some() {
                return Err(invalid_syntax_detail(
                    "Unicode high surrogate must not follow a high surrogate.",
                ));
            }
            *hi_surrogate = Some(ch);
            return Ok(());
        }
        let ch = if (0xDC00..=0xDFFF).contains(&ch) {
            match hi_surrogate.take() {
                None => return Err(low_must_follow()),
                Some(hi) => ((hi & 0x3FF) << 10) + (ch & 0x3FF) + 0x10000,
            }
        } else {
            if hi_surrogate.is_some() {
                return Err(low_must_follow());
            }
            ch
        };
        self.add_unicode_char(ch)
    }

    /// parseUnicode(): a run of `\uXXXX` / `\u{X...}` escapes.
    fn parse_unicode(&mut self, s: &[u8]) -> Result<()> {
        let l = s.len();
        let mut hi_surrogate: Option<u32> = None;
        let mut i = 2;
        while i < l {
            let mut ch: u32 = 0;
            if s[i] == b'{' {
                i += 1;
                while i < l && s[i] != b'}' {
                    ch = (ch << 4) | hexval(s[i]);
                    i += 1;
                }
                i += 1; // skip '}'
            } else {
                let mut j = 0;
                while j < 4 && i < l {
                    ch = (ch << 4) | hexval(s[i]);
                    i += 1;
                    j += 1;
                }
            }
            self.add_unicode(ch, &mut hi_surrogate)?;
            i += 2; // skip '\u'
        }
        if hi_surrogate.is_some() {
            return Err(invalid_syntax_detail(
                "Unicode low surrogate must follow a high surrogate.",
            ));
        }
        Ok(())
    }
}
