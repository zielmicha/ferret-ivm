//! Number formatting: `to_char(numeric | integer | bigint | real | double
//! precision, text)` and `to_number(text, text)`.
//!
//! This is a port of the NUM ("number") half of PostgreSQL 17's
//! `src/backend/utils/adt/formatting.c`: the format-picture parser
//! (`parse_format` / `NUMDesc_prepare`), the processor shared by both
//! directions (`NUM_processor`, `NUM_numpart_to_char`,
//! `NUM_numpart_from_char`) and the per-type wrappers (`numeric_to_char`,
//! `int4_to_char`, `int8_to_char`, `float4_to_char`, `float8_to_char`,
//! `numeric_to_number`). The structure follows the C code closely, because
//! the output is defined by its control flow rather than by a
//! specification; in particular these quirks are reproduced on purpose:
//!
//! * The processor writes the NUL terminator of the digit string into the
//!   output when a picture has more digit positions than the digit string
//!   has characters (this happens for floats, whose digits PostgreSQL limits
//!   to `FLT_DIG`/`DBL_DIG`, and for `NaN`/`Infinity`), and `SG` writes a
//!   NUL "sign" in Roman-numeral mode. PostgreSQL computes the result length
//!   with `strlen`, so the output ends at the first such NUL.
//! * NaN and infinities have no special formatting: numeric's `numeric_out`
//!   text ("NaN", "Infinity") and the floats' text from PostgreSQL's own
//!   `snprintf` ("NaN", "Infinity", never "nan"/"inf") are laid out as if
//!   their letters were digits; with `EEEE` they become a `#` pattern.
//! * `integer` and `bigint` differ: `int4_to_char` formats `EEEE` with C's
//!   `%e` (round half to even on the exact binary value) while
//!   `int8_to_char` goes through `numeric_out_sci` (round half away from
//!   zero), and the `V` multiplier is computed with wrapping 32-bit
//!   arithmetic for `integer` (`(int32) pow(10, n)` is `INT_MIN` on x86-64
//!   for n >= 10) but with overflow checks for `bigint`.
//! * `smallint` has no `to_char` of its own: PostgreSQL resolves
//!   `to_char(smallint, text)` to the `double precision` variant, which
//!   [`to_char_int2`] reproduces.
//!
//! The locale is C (`lc_numeric = lc_monetary = 'C'`): `D` is ".", `G` is
//! ",", the signs are "+" and "-", and `L` prints a single space (the C
//! locale has an empty currency symbol, which PostgreSQL replaces by " ").
//! Strings are assumed to be in a UTF-8 database. A NUL character (which
//! PostgreSQL text cannot contain) ends the format picture, as it would
//! end the C string PostgreSQL parses.

use std::fmt::Write as _;

use crate::var::{DEC_DIGITS, Var, div_var, err, get_str_from_var, power_ten_int};
use crate::{
    Error, Numeric, Parsed, Result, apply_typmod_special, apply_typmod_var, make_result, parse_var,
};

// ----------------------------------------------------------------------
// Locale (C)
// ----------------------------------------------------------------------

/// The values `NUM_prepare_locale` ends up with in the C locale, whether or
/// not the picture asked for locale-specific symbols (`need_locale`).
const L_NEGATIVE_SIGN: &[u8] = b"-";
const L_POSITIVE_SIGN: &[u8] = b"+";
const DECIMAL: &[u8] = b".";
const L_THOUSANDS_SEP: &[u8] = b",";
const L_CURRENCY_SYMBOL: &[u8] = b" ";

/// `float.h` constants used to limit the digits printed for floats.
const FLT_DIG: i32 = 6;
const DBL_DIG: i32 = 15;

/// `VARHDRSZ`, part of the numeric typmod encoding used by `to_number`.
const VARHDRSZ: i32 = 4;

/// Format pictures of this many bytes or more yield '' (`to_char`) or NULL
/// (`to_number`): `(INT_MAX - VARHDRSZ) / NUM_MAX_ITEM_SIZ` and
/// `INT_MAX / NUM_MAX_ITEM_SIZ`, which happen to be equal.
const MAX_FORMAT_LEN: usize = (i32::MAX as usize - 4) / 8;

// ----------------------------------------------------------------------
// Format pictures
// ----------------------------------------------------------------------

/// Keyword ids (`NUM_poz`). Lower-case keywords share the upper-case id,
/// except "rn" and "th" whose output is in lower case.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Key {
    Comma,
    Dec,
    Zero,
    Nine,
    B,
    C,
    D,
    E,
    Fm,
    G,
    L,
    Mi,
    Pl,
    Pr,
    Rn,
    Sg,
    Sp,
    S,
    Th,
    V,
    RnLower,
    ThLower,
}

struct KeyWord {
    name: &'static [u8],
    id: Key,
}

const fn kw(name: &'static [u8], id: Key) -> KeyWord {
    KeyWord { name, id }
}

/// `NUM_keywords[]`, in PostgreSQL's order. The first keyword that is a
/// prefix of the rest of the picture wins (so "SG" and "SP" are tried before
/// "S"). PostgreSQL starts the scan at an index entry for the first
/// character and stops when the first character changes; keywords sharing
/// a first character are adjacent here, so a plain scan is equivalent.
/// Note that "C", "SP" and "B" are keywords that print nothing, and that a
/// lone "E", "P", "M", "R", "T" or "F" is not a keyword at all.
static NUM_KEYWORDS: &[KeyWord] = &[
    kw(b",", Key::Comma),
    kw(b".", Key::Dec),
    kw(b"0", Key::Zero),
    kw(b"9", Key::Nine),
    kw(b"B", Key::B),
    kw(b"C", Key::C),
    kw(b"D", Key::D),
    kw(b"EEEE", Key::E),
    kw(b"FM", Key::Fm),
    kw(b"G", Key::G),
    kw(b"L", Key::L),
    kw(b"MI", Key::Mi),
    kw(b"PL", Key::Pl),
    kw(b"PR", Key::Pr),
    kw(b"RN", Key::Rn),
    kw(b"SG", Key::Sg),
    kw(b"SP", Key::Sp),
    kw(b"S", Key::S),
    kw(b"TH", Key::Th),
    kw(b"V", Key::V),
    kw(b"b", Key::B),
    kw(b"c", Key::C),
    kw(b"d", Key::D),
    kw(b"eeee", Key::E),
    kw(b"fm", Key::Fm),
    kw(b"g", Key::G),
    kw(b"l", Key::L),
    kw(b"mi", Key::Mi),
    kw(b"pl", Key::Pl),
    kw(b"pr", Key::Pr),
    kw(b"rn", Key::RnLower),
    kw(b"sg", Key::Sg),
    kw(b"sp", Key::Sp),
    kw(b"s", Key::S),
    kw(b"th", Key::ThLower),
    kw(b"v", Key::V),
];

/// `index_seq_search`: the keyword at the start of `s`, if any.
fn keyword_at(s: &[u8]) -> Option<&'static KeyWord> {
    // KeyWord_INDEX_FILTER: only printable ASCII other than ' ' and '~'
    // can start a keyword (multibyte characters are negative `char`s in C).
    let c = *s.first()?;
    if c <= b' ' || c >= b'~' {
        return None;
    }
    NUM_KEYWORDS.iter().find(|k| s.starts_with(k.name))
}

// NUMDesc flags.
const NUM_F_DECIMAL: u32 = 1 << 1;
const NUM_F_LDECIMAL: u32 = 1 << 2;
const NUM_F_ZERO: u32 = 1 << 3;
const NUM_F_BLANK: u32 = 1 << 4;
const NUM_F_FILLMODE: u32 = 1 << 5;
const NUM_F_LSIGN: u32 = 1 << 6;
const NUM_F_BRACKET: u32 = 1 << 7;
const NUM_F_MINUS: u32 = 1 << 8;
const NUM_F_PLUS: u32 = 1 << 9;
const NUM_F_ROMAN: u32 = 1 << 10;
const NUM_F_MULTI: u32 = 1 << 11;
const NUM_F_PLUS_POST: u32 = 1 << 12;
const NUM_F_MINUS_POST: u32 = 1 << 13;
const NUM_F_EEEE: u32 = 1 << 14;

const NUM_LSIGN_PRE: i32 = -1;
const NUM_LSIGN_POST: i32 = 1;
const NUM_LSIGN_NONE: i32 = 0;

/// `NUMDesc`: the summary of a format picture. (`need_locale` is omitted:
/// in the C locale the locale-specific symbols equal the defaults.)
#[derive(Clone, Debug, Default)]
struct NumDesc {
    /// number of digit positions before the decimal point
    pre: i32,
    /// number of digit positions after the decimal point
    post: i32,
    /// position of the locale sign `S`: NUM_LSIGN_PRE/POST/NONE
    lsign: i32,
    flag: u32,
    /// `pre` when `S` was seen
    pre_lsign_num: i32,
    /// number of digits after `V`
    multi: i32,
    /// position of the first `0` (1-based), and of the last one
    zero_start: i32,
    zero_end: i32,
}

impl NumDesc {
    fn is(&self, flag: u32) -> bool {
        self.flag & flag != 0
    }
}

/// A format picture element (`FormatNode`).
#[derive(Clone, Copy, Debug)]
enum Node<'a> {
    Action(Key),
    /// A literal character (whole UTF-8 sequence): copied by `to_char`,
    /// skips one input character in `to_number`.
    Char(&'a [u8]),
}

fn syntax_error(message: &str) -> Error {
    err("42601", message)
}

/// `pg_utf_mblen` (bounded by the end of the buffer).
fn mblen(s: &[u8], i: usize) -> usize {
    let c = s[i];
    let len = if c & 0x80 == 0 {
        1
    } else if c & 0xe0 == 0xc0 {
        2
    } else if c & 0xf0 == 0xe0 {
        3
    } else if c & 0xf8 == 0xf0 {
        4
    } else {
        1
    };
    len.min(s.len() - i)
}

/// `pg_mbstrlen`: the number of UTF-8 characters in `s`.
fn mbstrlen(s: &[u8]) -> usize {
    s.iter().filter(|&&b| b & 0xc0 != 0x80).count()
}

/// `NUMDesc_prepare`: account for one keyword, rejecting invalid
/// combinations with PostgreSQL's messages.
fn num_desc_prepare(num: &mut NumDesc, id: Key) -> Result<()> {
    if num.is(NUM_F_EEEE) && id != Key::E {
        return Err(syntax_error("\"EEEE\" must be the last pattern used"));
    }
    match id {
        Key::Nine => {
            if num.is(NUM_F_BRACKET) {
                return Err(syntax_error("\"9\" must be ahead of \"PR\""));
            }
            if num.is(NUM_F_MULTI) {
                num.multi += 1;
            } else if num.is(NUM_F_DECIMAL) {
                num.post += 1;
            } else {
                num.pre += 1;
            }
        }
        Key::Zero => {
            if num.is(NUM_F_BRACKET) {
                return Err(syntax_error("\"0\" must be ahead of \"PR\""));
            }
            if !num.is(NUM_F_ZERO) && !num.is(NUM_F_DECIMAL) {
                num.flag |= NUM_F_ZERO;
                num.zero_start = num.pre + 1;
            }
            if !num.is(NUM_F_DECIMAL) {
                num.pre += 1;
            } else {
                num.post += 1;
            }
            num.zero_end = num.pre + num.post;
        }
        Key::B => {
            if num.pre == 0 && num.post == 0 && !num.is(NUM_F_ZERO) {
                num.flag |= NUM_F_BLANK;
            }
        }
        Key::D | Key::Dec => {
            if id == Key::D {
                num.flag |= NUM_F_LDECIMAL;
            }
            if num.is(NUM_F_DECIMAL) {
                return Err(syntax_error("multiple decimal points"));
            }
            if num.is(NUM_F_MULTI) {
                return Err(syntax_error("cannot use \"V\" and decimal point together"));
            }
            num.flag |= NUM_F_DECIMAL;
        }
        Key::Fm => num.flag |= NUM_F_FILLMODE,
        Key::S => {
            if num.is(NUM_F_LSIGN) {
                return Err(syntax_error("cannot use \"S\" twice"));
            }
            if num.is(NUM_F_PLUS) || num.is(NUM_F_MINUS) || num.is(NUM_F_BRACKET) {
                return Err(syntax_error(
                    "cannot use \"S\" and \"PL\"/\"MI\"/\"SG\"/\"PR\" together",
                ));
            }
            if !num.is(NUM_F_DECIMAL) {
                num.lsign = NUM_LSIGN_PRE;
                num.pre_lsign_num = num.pre;
                num.flag |= NUM_F_LSIGN;
            } else if num.lsign == NUM_LSIGN_NONE {
                num.lsign = NUM_LSIGN_POST;
                num.flag |= NUM_F_LSIGN;
            }
        }
        Key::Mi => {
            if num.is(NUM_F_LSIGN) {
                return Err(syntax_error("cannot use \"S\" and \"MI\" together"));
            }
            num.flag |= NUM_F_MINUS;
            if num.is(NUM_F_DECIMAL) {
                num.flag |= NUM_F_MINUS_POST;
            }
        }
        Key::Pl => {
            if num.is(NUM_F_LSIGN) {
                return Err(syntax_error("cannot use \"S\" and \"PL\" together"));
            }
            num.flag |= NUM_F_PLUS;
            if num.is(NUM_F_DECIMAL) {
                num.flag |= NUM_F_PLUS_POST;
            }
        }
        Key::Sg => {
            if num.is(NUM_F_LSIGN) {
                return Err(syntax_error("cannot use \"S\" and \"SG\" together"));
            }
            num.flag |= NUM_F_MINUS | NUM_F_PLUS;
        }
        Key::Pr => {
            if num.is(NUM_F_LSIGN) || num.is(NUM_F_PLUS) || num.is(NUM_F_MINUS) {
                return Err(syntax_error(
                    "cannot use \"PR\" and \"S\"/\"PL\"/\"MI\"/\"SG\" together",
                ));
            }
            num.flag |= NUM_F_BRACKET;
        }
        Key::Rn | Key::RnLower => num.flag |= NUM_F_ROMAN,
        Key::V => {
            if num.is(NUM_F_DECIMAL) {
                return Err(syntax_error("cannot use \"V\" and decimal point together"));
            }
            num.flag |= NUM_F_MULTI;
        }
        Key::E => {
            if num.is(NUM_F_EEEE) {
                return Err(syntax_error("cannot use \"EEEE\" twice"));
            }
            if num.flag
                & (NUM_F_BLANK
                    | NUM_F_FILLMODE
                    | NUM_F_LSIGN
                    | NUM_F_BRACKET
                    | NUM_F_MINUS
                    | NUM_F_PLUS
                    | NUM_F_ROMAN
                    | NUM_F_MULTI)
                != 0
            {
                return Err(Error {
                    sqlstate: "42601",
                    message: "\"EEEE\" is incompatible with other formats".to_string(),
                    detail: Some(
                        "\"EEEE\" may only be used together with digit and decimal point patterns."
                            .to_string(),
                    ),
                });
            }
            num.flag |= NUM_F_EEEE;
        }
        // L and G only make the picture locale-dependent; ",", "C", "SP" and
        // "TH" do not affect the description.
        Key::L | Key::G | Key::Comma | Key::C | Key::Sp | Key::Th | Key::ThLower => {}
    }
    Ok(())
}

/// `parse_format` for the NUM keywords. `fmt` is the picture up to (not
/// including) its first NUL, like the C string PostgreSQL parses.
fn parse_format(fmt: &[u8]) -> Result<(Vec<Node<'_>>, NumDesc)> {
    let mut num = NumDesc::default();
    let mut nodes = Vec::with_capacity(fmt.len());
    let mut i = 0;
    while i < fmt.len() {
        if let Some(k) = keyword_at(&fmt[i..]) {
            i += k.name.len();
            num_desc_prepare(&mut num, k.id)?;
            nodes.push(Node::Action(k.id));
        } else if fmt[i] == b'"' {
            // Double-quoted literal; a backslash quotes the next character.
            i += 1;
            while i < fmt.len() {
                if fmt[i] == b'"' {
                    i += 1;
                    break;
                }
                if fmt[i] == b'\\' && i + 1 < fmt.len() {
                    i += 1;
                }
                let len = mblen(fmt, i);
                nodes.push(Node::Char(&fmt[i..i + len]));
                i += len;
            }
        } else {
            // Outside quotes a backslash is only special before a quote.
            if fmt[i] == b'\\' && fmt.get(i + 1) == Some(&b'"') {
                i += 1;
            }
            let len = mblen(fmt, i);
            nodes.push(Node::Char(&fmt[i..i + len]));
            i += len;
        }
    }
    Ok((nodes, num))
}

/// The bytes of a text value as the C string PostgreSQL's parser sees.
fn c_str(s: &str) -> &[u8] {
    let b = s.as_bytes();
    match b.iter().position(|&c| c == 0) {
        Some(n) => &b[..n],
        None => b,
    }
}

/// `NUM_TOCHAR_prepare`: None means the result is the empty string.
fn num_tochar_prepare(fmt: &str) -> Result<Option<(Vec<Node<'_>>, NumDesc)>> {
    if fmt.is_empty() || fmt.len() >= MAX_FORMAT_LEN {
        return Ok(None);
    }
    parse_format(c_str(fmt)).map(Some)
}

// ----------------------------------------------------------------------
// to_char processor
// ----------------------------------------------------------------------

/// `NUMProc` state for `to_char`. The output is append-only; NUL bytes the
/// C code writes into it are kept and cut off at the end.
struct ToChar<'a> {
    num: NumDesc,
    /// b'-', b'+', or 0 (Roman numerals)
    sign: u8,
    sign_wrote: bool,
    num_count: i32,
    num_in: bool,
    num_curr: i32,
    out_pre_spaces: i32,
    /// the digit string; index `number.len()` reads as its NUL terminator
    number: &'a [u8],
    number_p: usize,
    /// FM: last relevant position after the decimal point
    last_relevant: Option<usize>,
    out: Vec<u8>,
}

impl ToChar<'_> {
    /// `*Np->number_p`
    fn cur(&self) -> u8 {
        self.number.get(self.number_p).copied().unwrap_or(0)
    }

    /// `Np->last_relevant && *Np->last_relevant == '.'`
    fn last_relevant_is_point(&self) -> bool {
        self.last_relevant.is_some_and(|i| self.number[i] == b'.')
    }

    /// `IS_PREDEC_SPACE`: the leading "0" of "0.xxx" is printed as a space
    /// (or not at all in FM mode) by 9-positions.
    fn is_predec_space(&self) -> bool {
        !self.num.is(NUM_F_ZERO)
            && self.number_p == 0
            && self.number.first() == Some(&b'0')
            && self.num.post != 0
    }

    fn locale_sign(&self) -> &'static [u8] {
        if self.sign == b'-' {
            L_NEGATIVE_SIGN
        } else {
            L_POSITIVE_SIGN
        }
    }

    /// `NUM_numpart_to_char`: one 9, 0, D or "." position.
    fn numpart_to_char(&mut self, id: Key) {
        if self.num.is(NUM_F_ROMAN) {
            return;
        }
        let fm = self.num.is(NUM_F_FILLMODE);
        self.num_in = false;

        // Write the sign if a real digit is going to be written next.
        if !self.sign_wrote
            && (self.num_curr >= self.out_pre_spaces
                || (self.num.is(NUM_F_ZERO) && self.num.zero_start == self.num_curr))
            && (!self.is_predec_space() || self.last_relevant_is_point())
        {
            if self.num.is(NUM_F_LSIGN) {
                if self.num.lsign == NUM_LSIGN_PRE {
                    self.out.extend_from_slice(self.locale_sign());
                    self.sign_wrote = true;
                }
            } else if self.num.is(NUM_F_BRACKET) {
                self.out.push(if self.sign == b'+' { b' ' } else { b'<' });
                self.sign_wrote = true;
            } else if self.sign == b'+' {
                if !fm {
                    self.out.push(b' ');
                }
                self.sign_wrote = true;
            } else if self.sign == b'-' {
                self.out.push(b'-');
                self.sign_wrote = true;
            }
        }

        // Digits / FM / zero / decimal point
        if self.num_curr < self.out_pre_spaces
            && (self.num.zero_start > self.num_curr || !self.num.is(NUM_F_ZERO))
        {
            // blank space
            if !fm {
                self.out.push(b' ');
            }
        } else if self.num.is(NUM_F_ZERO)
            && self.num_curr < self.out_pre_spaces
            && self.num.zero_start <= self.num_curr
        {
            // leading zero
            self.out.push(b'0');
            self.num_in = true;
        } else {
            if self.cur() == b'.' {
                // decimal point ("FM9.9" prints "n." for integral values)
                if !self.last_relevant_is_point() || fm {
                    self.out.extend_from_slice(DECIMAL);
                }
            } else if self.last_relevant.is_some_and(|lr| self.number_p > lr) && id != Key::Zero {
                // FM: trailing zeros are not printed
            } else if self.is_predec_space() {
                if !fm {
                    // "0.1" with "9.9" gives " .1"
                    self.out.push(b' ');
                } else if self.last_relevant_is_point() {
                    // "0" with "FM9.9" gives "0."
                    self.out.push(b'0');
                }
            } else {
                // a digit, or the terminating NUL once the string runs out
                self.out.push(self.cur());
                self.num_in = true;
            }
            if self.cur() != 0 {
                self.number_p += 1;
            }
        }

        let mut end = self.num_count
            + i32::from(self.out_pre_spaces != 0)
            + i32::from(self.num.is(NUM_F_DECIMAL));
        if self.last_relevant == Some(self.number_p) {
            end = self.num_curr;
        }
        if self.num_curr + 1 == end {
            if self.sign_wrote && self.num.is(NUM_F_BRACKET) {
                self.out.push(if self.sign == b'+' { b' ' } else { b'>' });
            } else if self.num.is(NUM_F_LSIGN) && self.num.lsign == NUM_LSIGN_POST {
                self.out.extend_from_slice(self.locale_sign());
            }
        }

        self.num_curr += 1;
    }
}

/// `get_last_relevant_decnum`: position of the last non-zero digit after the
/// decimal point (or of the point itself); None without a decimal point.
fn get_last_relevant_decnum(num: &[u8]) -> Option<usize> {
    let p = num.iter().position(|&c| c == b'.')?;
    let last_nonzero = num[p + 1..].iter().rposition(|&c| c != b'0');
    Some(last_nonzero.map_or(p, |i| p + 1 + i))
}

/// `get_th`: the ordinal suffix for the digit string `num`.
fn get_th(num: &[u8], upper: bool) -> Result<&'static str> {
    let len = num.len();
    let Some(&last) = num.last().filter(|c| c.is_ascii_digit()) else {
        return Err(err(
            "22P02",
            format!("\"{}\" is not a number", String::from_utf8_lossy(num)),
        ));
    };
    // all "teens" (x1[0-9]) get "th"
    let last = if len > 1 && num[len - 2] == b'1' {
        0
    } else {
        last
    };
    Ok(match (last, upper) {
        (b'1', true) => "ST",
        (b'1', false) => "st",
        (b'2', true) => "ND",
        (b'2', false) => "nd",
        (b'3', true) => "RD",
        (b'3', false) => "rd",
        (_, true) => "TH",
        (_, false) => "th",
    })
}

/// The result text: PostgreSQL takes `strlen` of the output buffer.
fn finish_output(mut out: Vec<u8>) -> String {
    if let Some(n) = out.iter().position(|&c| c == 0) {
        out.truncate(n);
    }
    // Only whole UTF-8 sequences from the picture and ASCII are written.
    String::from_utf8(out).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

/// `NUM_processor` in the `to_char` direction. `number` is the digit string
/// prepared by the type-specific wrapper (without sign), `sign` is b'+',
/// b'-' or 0, and `out_pre_spaces` the number of unused leading positions.
fn num_processor_to_char(
    nodes: &[Node],
    mut num: NumDesc,
    number: &[u8],
    out_pre_spaces: i32,
    sign: u8,
) -> Result<String> {
    if num.zero_start != 0 {
        num.zero_start -= 1;
    }

    // EEEE: the scientific notation string is the whole result; everything
    // else in the picture is ignored.
    if num.is(NUM_F_EEEE) {
        return Ok(finish_output(number.to_vec()));
    }

    // Roman correction (the sign and out_pre_spaces are reset too, but the
    // callers pass 0 for both anyway)
    if num.is(NUM_F_ROMAN) {
        num.lsign = 0;
        num.pre_lsign_num = 0;
        num.post = 0;
        num.pre = 0;
        num.flag = (num.flag & NUM_F_FILLMODE) | NUM_F_ROMAN;
    }

    let mut np = ToChar {
        num,
        sign,
        sign_wrote: false,
        num_count: 0,
        num_in: false,
        num_curr: 0,
        out_pre_spaces,
        number,
        number_p: 0,
        last_relevant: None,
        out: Vec::with_capacity(nodes.len() + number.len() + 8),
    };
    let fm = np.num.is(NUM_F_FILLMODE);

    // Sign
    if np.num.is(NUM_F_PLUS) || np.num.is(NUM_F_MINUS) {
        // MI/PL/SG write the sign themselves; PL alone still needs the
        // space a positive sign occupies
        np.sign_wrote = !(np.num.is(NUM_F_PLUS) && !np.num.is(NUM_F_MINUS));
    } else {
        if np.sign != b'-' && fm {
            np.num.flag &= !NUM_F_BRACKET;
        }
        np.sign_wrote = np.sign == b'+' && fm && !np.num.is(NUM_F_LSIGN);
        if np.num.lsign == NUM_LSIGN_PRE && np.num.pre == np.num.pre_lsign_num {
            np.num.lsign = NUM_LSIGN_POST;
        }
    }

    // Count
    np.num_count = np.num.post + np.num.pre - 1;
    if fm && np.num.is(NUM_F_DECIMAL) {
        np.last_relevant = get_last_relevant_decnum(number);
        // Don't strip digits printed by '0' positions, but don't go beyond
        // the end of the digit string either.
        if let Some(lr) = np.last_relevant
            && np.num.zero_end > out_pre_spaces
        {
            let last_zero = (number.len() as i32 - 1).min(np.num.zero_end - out_pre_spaces);
            if (lr as i32) < last_zero {
                np.last_relevant = Some(last_zero as usize);
            }
        }
    }
    if !np.sign_wrote && out_pre_spaces == 0 {
        np.num_count += 1;
    }

    for node in nodes {
        let id = match *node {
            Node::Char(c) => {
                np.out.extend_from_slice(c);
                continue;
            }
            Node::Action(id) => id,
        };
        match id {
            Key::Nine | Key::Zero | Key::Dec | Key::D => np.numpart_to_char(id),
            Key::Comma => {
                if np.num_in {
                    np.out.push(b',');
                } else if !fm {
                    np.out.push(b' ');
                }
            }
            Key::G => {
                if np.num_in {
                    np.out.extend_from_slice(L_THOUSANDS_SEP);
                } else if !fm {
                    // one space per character of the separator
                    np.out
                        .resize(np.out.len() + mbstrlen(L_THOUSANDS_SEP), b' ');
                }
            }
            Key::L => np.out.extend_from_slice(L_CURRENCY_SYMBOL),
            Key::Rn | Key::RnLower => {
                // (number_p never moves in Roman-numeral mode)
                let mut roman = number[np.number_p..].to_vec();
                if id == Key::RnLower {
                    roman.make_ascii_lowercase();
                }
                // sprintf("%15s") unless FM
                if !fm && roman.len() < 15 {
                    np.out.resize(np.out.len() + 15 - roman.len(), b' ');
                }
                np.out.extend_from_slice(&roman);
            }
            Key::Th | Key::ThLower => {
                if np.num.is(NUM_F_ROMAN)
                    || number.first() == Some(&b'#')
                    || np.sign == b'-'
                    || np.num.is(NUM_F_DECIMAL)
                {
                    continue;
                }
                np.out
                    .extend_from_slice(get_th(number, id == Key::Th)?.as_bytes());
            }
            Key::Mi => {
                if np.sign == b'-' {
                    np.out.push(b'-');
                } else if !fm {
                    np.out.push(b' ');
                }
            }
            Key::Pl => {
                if np.sign == b'+' {
                    np.out.push(b'+');
                } else if !fm {
                    np.out.push(b' ');
                }
            }
            // a NUL in Roman-numeral mode, which ends the result
            Key::Sg => np.out.push(np.sign),
            Key::B | Key::C | Key::E | Key::Fm | Key::Pr | Key::Sp | Key::S | Key::V => {}
        }
    }

    Ok(finish_output(np.out))
}

// ----------------------------------------------------------------------
// to_number processor
// ----------------------------------------------------------------------

/// `NUMProc` state for `to_number`.
struct FromChar<'a> {
    num: NumDesc,
    input: &'a [u8],
    /// `Np->inout_p`
    pos: usize,
    /// the extracted number; byte 0 is the sign (' ', '-' or '+')
    number: Vec<u8>,
    read_dec: bool,
    read_post: i32,
    read_pre: i32,
}

impl FromChar<'_> {
    /// `OVERLOAD_TEST`: the input is exhausted
    fn overload(&self) -> bool {
        self.pos >= self.input.len()
    }

    /// `AMOUNT_TEST(len) && strncmp(inout_p, pat, len) == 0`
    fn looking_at(&self, pat: &[u8]) -> bool {
        !pat.is_empty()
            && self.pos + pat.len() <= self.input.len()
            && &self.input[self.pos..self.pos + pat.len()] == pat
    }

    /// `NUM_numpart_from_char`: one 9, 0, D or "." position.
    fn numpart_from_char(&mut self, id: Key) {
        let mut isread = false;

        if self.overload() {
            return;
        }
        if self.input[self.pos] == b' ' {
            self.pos += 1;
        }
        if self.overload() {
            return;
        }

        // read the sign before the number
        if self.number[0] == b' '
            && matches!(id, Key::Zero | Key::Nine)
            && self.read_pre + self.read_post == 0
        {
            if self.num.is(NUM_F_LSIGN) && self.num.lsign == NUM_LSIGN_PRE {
                if self.looking_at(L_NEGATIVE_SIGN) {
                    self.pos += L_NEGATIVE_SIGN.len();
                    self.number[0] = b'-';
                } else if self.looking_at(L_POSITIVE_SIGN) {
                    self.pos += L_POSITIVE_SIGN.len();
                    self.number[0] = b'+';
                }
            } else {
                let c = self.input[self.pos];
                if c == b'-' || (self.num.is(NUM_F_BRACKET) && c == b'<') {
                    self.number[0] = b'-';
                    self.pos += 1;
                } else if c == b'+' {
                    self.number[0] = b'+';
                    self.pos += 1;
                }
            }
        }

        if self.overload() {
            return;
        }

        // read a digit or the decimal point
        let c = self.input[self.pos];
        if c.is_ascii_digit() {
            if self.read_dec && self.read_post == self.num.post {
                return;
            }
            self.number.push(c);
            if self.read_dec {
                self.read_post += 1;
            } else {
                self.read_pre += 1;
            }
            isread = true;
        } else if self.num.is(NUM_F_DECIMAL) && !self.read_dec && self.looking_at(DECIMAL) {
            self.pos += DECIMAL.len() - 1;
            self.number.push(b'.');
            self.read_dec = true;
            isread = true;
        }

        if self.overload() {
            return;
        }

        // Read a sign behind the "last" digit: the exact position of a
        // trailing sign is hard to determine, so accept it after any digit.
        if self.number[0] == b' ' && self.read_pre + self.read_post > 0 {
            if self.num.is(NUM_F_LSIGN)
                && isread
                && self.pos + 1 < self.input.len()
                && !self.input[self.pos + 1].is_ascii_digit()
            {
                // locale sign (S) anchored after the last digit
                let tmp = self.pos;
                self.pos += 1;
                if self.looking_at(L_NEGATIVE_SIGN) {
                    self.pos += L_NEGATIVE_SIGN.len() - 1;
                    self.number[0] = b'-';
                } else if self.looking_at(L_POSITIVE_SIGN) {
                    self.pos += L_POSITIVE_SIGN.len() - 1;
                    self.number[0] = b'+';
                }
                if self.number[0] == b' ' {
                    self.pos = tmp;
                }
            } else if !isread
                && !self.num.is(NUM_F_LSIGN)
                && (self.num.is(NUM_F_PLUS) || self.num.is(NUM_F_MINUS))
            {
                // MI/PL/SG at an inexact position, e.g. "FM9.999999MI"
                let c = self.input[self.pos];
                if c == b'-' || c == b'+' {
                    self.number[0] = c;
                }
            }
        }
    }

    /// `NUM_eat_non_data_chars`: skip up to `n` input characters that
    /// cannot be part of the number.
    fn eat_non_data_chars(&mut self, n: usize) {
        for _ in 0..n {
            if self.overload() {
                break;
            }
            let c = self.input[self.pos];
            // strchr() also finds the terminating NUL
            if c == 0 || b"0123456789.,+-".contains(&c) {
                break;
            }
            self.pos += mblen(self.input, self.pos);
        }
    }
}

/// `NUM_processor` in the `to_number` direction: returns the extracted
/// number string (sign byte, digits, decimal point) and sets `num.post` to
/// the number of fractional digits read.
fn num_processor_from_char(nodes: &[Node], num: &mut NumDesc, input: &[u8]) -> Result<String> {
    if num.is(NUM_F_EEEE) {
        return Err(err("0A000", "\"EEEE\" not supported for input"));
    }
    if num.is(NUM_F_ROMAN) {
        return Err(err("0A000", "\"RN\" not supported for input"));
    }

    let mut np = FromChar {
        num: num.clone(),
        input,
        pos: 0,
        number: Vec::with_capacity(nodes.len() + 2),
        read_dec: false,
        read_post: 0,
        read_pre: 0,
    };
    np.number.push(b' ');
    let fm = np.num.is(NUM_F_FILLMODE);

    for node in nodes {
        // at least one byte must remain to be scanned
        if np.overload() {
            break;
        }
        let id = match *node {
            // each literal picture character skips one input character,
            // whether or not they match
            Node::Char(_) => {
                np.pos += mblen(input, np.pos);
                continue;
            }
            Node::Action(id) => id,
        };
        match id {
            Key::Nine | Key::Zero | Key::Dec | Key::D => {
                np.numpart_from_char(id);
                np.pos += 1;
            }
            // (to_number never sets num_in, so FM always skips these)
            Key::Comma => {
                if !fm && input[np.pos] == b',' {
                    np.pos += 1;
                }
            }
            Key::G => {
                if !fm && np.looking_at(L_THOUSANDS_SEP) {
                    np.pos += L_THOUSANDS_SEP.len();
                }
            }
            Key::L => np.eat_non_data_chars(mbstrlen(L_CURRENCY_SYMBOL)),
            Key::Th | Key::ThLower => {
                // (Np->sign is never '-' and the number never starts with
                // '#' in this direction)
                if !np.num.is(NUM_F_DECIMAL) {
                    // all variants of "TH" occupy 2 characters
                    np.eat_non_data_chars(2);
                }
            }
            Key::Mi => {
                if input[np.pos] == b'-' {
                    np.number[0] = b'-';
                    np.pos += 1;
                } else {
                    np.eat_non_data_chars(1);
                }
            }
            Key::Pl => {
                if input[np.pos] == b'+' {
                    np.number[0] = b'+';
                    np.pos += 1;
                } else {
                    np.eat_non_data_chars(1);
                }
            }
            Key::Sg => {
                let c = input[np.pos];
                if c == b'-' || c == b'+' {
                    np.number[0] = c;
                    np.pos += 1;
                } else {
                    np.eat_non_data_chars(1);
                }
            }
            Key::Rn
            | Key::RnLower
            | Key::B
            | Key::C
            | Key::E
            | Key::Fm
            | Key::Pr
            | Key::Sp
            | Key::S
            | Key::V => {}
        }
    }

    // drop a trailing decimal point
    if np.number.last() == Some(&b'.') {
        np.number.pop();
    }
    // precision of the decimal number
    num.post = np.read_post;
    Ok(String::from_utf8(np.number).expect("ASCII"))
}

// ----------------------------------------------------------------------
// Helpers for the type-specific wrappers
// ----------------------------------------------------------------------

/// `int_to_roman`: 15 '#' outside 1..=3999.
fn int_to_roman(number: i32) -> Vec<u8> {
    const RM1: [&str; 9] = ["I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX"];
    const RM10: [&str; 9] = ["X", "XX", "XXX", "XL", "L", "LX", "LXX", "LXXX", "XC"];
    const RM100: [&str; 9] = ["C", "CC", "CCC", "CD", "D", "DC", "DCC", "DCCC", "CM"];
    if !(1..=3999).contains(&number) {
        return vec![b'#'; 15];
    }
    let digits = number.to_string();
    let mut result = String::new();
    for (i, d) in digits.bytes().enumerate() {
        let len = digits.len() - i;
        let Some(n) = (d - b'0').checked_sub(1) else {
            continue;
        };
        let n = n as usize;
        match len {
            4 => result.extend(std::iter::repeat_n('M', n + 1)),
            3 => result.push_str(RM100[n]),
            2 => result.push_str(RM10[n]),
            _ => result.push_str(RM1[n]),
        }
    }
    result.into_bytes()
}

/// The digit string for a value that does not fit the picture: '#' for
/// every digit position, with the decimal point after the integral ones.
fn overflow_str(num: &NumDesc) -> Vec<u8> {
    let mut s = vec![b'#'; (num.pre + num.post + 1) as usize];
    s[num.pre as usize] = b'.';
    s
}

/// The `EEEE` digit string for NaN and infinities: room for a sign, the
/// digits, the decimal point and a 4-character exponent.
fn eeee_special_str(num: &NumDesc) -> Vec<u8> {
    let mut s = vec![b'#'; (num.pre + num.post + 6) as usize];
    s[0] = b' ';
    s[(num.pre + 1) as usize] = b'.';
    s
}

/// Final part of the wrappers for plain digit pictures: split off the sign
/// of `orgnum` and compare its integral digits with the picture
/// ("needs padding?" / "overflowed prefix digit format?"). Returns the digit
/// string, the sign and out_pre_spaces.
fn layout_digits(orgnum: &str, num: &NumDesc, pad_post_zeros: bool) -> (Vec<u8>, u8, i32) {
    let (sign, digits) = match orgnum.strip_prefix('-') {
        Some(rest) => (b'-', rest),
        None => (b'+', orgnum),
    };
    let pre_len = digits.find('.').unwrap_or(digits.len()) as i32;
    let mut numstr = digits.as_bytes().to_vec();
    if pad_post_zeros && num.post != 0 {
        // integers: pad out the fractional positions with zeros
        numstr.push(b'.');
        numstr.resize(numstr.len() + num.post as usize, b'0');
    }
    if pre_len < num.pre {
        (numstr, sign, num.pre - pre_len)
    } else if pre_len > num.pre {
        (overflow_str(num), sign, 0)
    } else {
        (numstr, sign, 0)
    }
}

/// PostgreSQL's own `snprintf` (src/port/snprintf.c, `fmtfloat`) for
/// `"%.*f"` (`exp == false`) and `"%.*e"` / `"%+.*e"` (`exp == true`,
/// `forcesign`): NaN is always "NaN" (never signed), infinities are
/// "Infinity"/"-Infinity", and finite values get the C library's exact
/// decimal expansion (round half to even) with the precision capped at 350
/// and zero-padded beyond that. The exponent has a sign and at least two
/// digits.
fn pg_fmtfloat(value: f64, precision: i32, exp: bool, forcesign: bool) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    let precision = precision.max(0) as usize;
    let prec = precision.min(350);
    let zeropad = precision - prec;
    let mut out = String::new();
    // (-0.0 counts as negative)
    let value = if value.is_sign_negative() {
        out.push('-');
        -value
    } else {
        if forcesign {
            out.push('+');
        }
        value
    };
    if value.is_infinite() {
        out.push_str("Infinity");
        return out;
    }
    if exp {
        let s = format!("{:.*e}", prec, value);
        let (mant, e) = s.split_at(s.find('e').expect("exponent"));
        let e: i32 = e[1..].parse().expect("exponent");
        out.push_str(mant);
        out.extend(std::iter::repeat_n('0', zeropad));
        let _ = write!(
            out,
            "e{}{:02}",
            if e < 0 { '-' } else { '+' },
            e.unsigned_abs()
        );
    } else {
        let _ = write!(out, "{:.*}", prec, value);
        out.extend(std::iter::repeat_n('0', zeropad));
    }
    out
}

/// `psprintf("%+.*e", post, value)` with the leading '+' swapped for a
/// space, as the float and int4 `EEEE` branches do.
fn eeee_float_str(value: f64, post: i32) -> Vec<u8> {
    let mut s = pg_fmtfloat(value, post, true, true).into_bytes();
    if s[0] == b'+' {
        s[0] = b' ';
    }
    s
}

/// glibc's `pow(10, n)` for n >= 0, the `V` multiplier of the integer and
/// float variants: exact up to 10^22, correctly rounded above except for
/// n = 23 and n = 210 where glibc's result is one ulp higher, and infinity
/// beyond 10^308.
fn pow10_glibc(n: i32) -> f64 {
    match n {
        ..=22 => 10f64.powi(n),
        23 => f64::from_bits(0x44B5_2D02_C7E1_4AF7),
        210 => f64::from_bits(0x6B88_557F_3132_6BBC),
        309.. => f64::INFINITY,
        _ => format!("1e{n}").parse().expect("valid float"),
    }
}

/// `(int) rint(value)` as compiled for x86-64: round half to even, and
/// `INT_MIN` for NaN and out-of-range values (cvttsd2si).
fn rint_to_int(value: f64) -> i32 {
    double_to_int32(value.round_ties_even())
}

/// `(int32) value` on x86-64: truncation, `INT_MIN` when out of range.
fn double_to_int32(value: f64) -> i32 {
    if value.is_nan() || value <= -2147483649.0 || value >= 2147483648.0 {
        i32::MIN
    } else {
        value as i32
    }
}

fn bigint_out_of_range() -> Error {
    err("22003", "bigint out of range")
}

/// `numeric_out_sci` / `get_str_from_var_sci`: scientific notation with
/// `scale` fractional digits (rounded half away from zero; the significand
/// can round up to "10.0").
fn numeric_out_sci(num: &Numeric, scale: i32) -> String {
    if num.is_nan() {
        return "NaN".to_string();
    }
    if num.is_infinite() {
        return if num.is_negative() {
            "-Infinity".to_string()
        } else {
            "Infinity".to_string()
        };
    }
    let var: &Var = num;
    let rscale = scale.max(0);
    let exponent = if var.nd() > 0 {
        // (int) log10(digits[0]) is the number of decimal digits of the
        // first NBASE digit minus one
        let d0 = var.dig(0);
        let log10 = match d0 {
            1000.. => 3,
            100.. => 2,
            10.. => 1,
            _ => 0,
        };
        (var.weight + 1) * DEC_DIGITS - (DEC_DIGITS - log10)
    } else {
        0
    };
    let significand =
        div_var(var, &power_ten_int(exponent), rscale, true).expect("10^n is not zero");
    let mut s = get_str_from_var(&significand);
    let _ = write!(
        s,
        "e{}{:02}",
        if exponent < 0 { '-' } else { '+' },
        exponent.unsigned_abs()
    );
    s
}

// ----------------------------------------------------------------------
// Public API
// ----------------------------------------------------------------------

/// `to_char(numeric, text)` (`numeric_to_char`).
///
/// Errors are PostgreSQL's: invalid pictures (42601, e.g. `"EEEE" is
/// incompatible with other formats`), `RN` with NaN/infinite or
/// out-of-integer-range values (0A000 / 22003), `TH` with NaN or Infinity
/// (22P02 `"NaN" is not a number`), and overflow of `10^n` for huge `V`
/// multipliers.
pub fn to_char_numeric(value: &Numeric, fmt: &str) -> Result<String> {
    let Some((nodes, mut num)) = num_tochar_prepare(fmt)? else {
        return Ok(String::new());
    };
    let (numstr, sign, out_pre_spaces) = if num.is(NUM_F_ROMAN) {
        let x = value.round(0)?;
        (int_to_roman(x.to_i32()?), 0, 0)
    } else if num.is(NUM_F_EEEE) {
        let orgnum = numeric_out_sci(value, num.post);
        // numeric_out_sci does not emit a sign for positive numbers: add a
        // space so that positive and negative numbers align
        let numstr = if value.is_special() {
            eeee_special_str(&num)
        } else if !orgnum.starts_with('-') {
            format!(" {orgnum}").into_bytes()
        } else {
            orgnum.into_bytes()
        };
        (numstr, 0, 0)
    } else {
        let mut val = value.clone();
        if num.is(NUM_F_MULTI) {
            let x = Numeric::from_i64(10).pow(&Numeric::from_i64(num.multi as i64))?;
            val = value.mul(&x)?;
            num.pre += num.multi;
        }
        let x = val.round(num.post)?;
        layout_digits(&x.to_string(), &num, false)
    };
    num_processor_to_char(&nodes, num, &numstr, out_pre_spaces, sign)
}

/// `to_char(integer, text)` (`int4_to_char`). Not for `smallint`: see
/// [`to_char_int2`].
pub fn to_char_int4(value: i32, fmt: &str) -> Result<String> {
    let Some((nodes, mut num)) = num_tochar_prepare(fmt)? else {
        return Ok(String::new());
    };
    let (numstr, sign, out_pre_spaces) = if num.is(NUM_F_ROMAN) {
        (int_to_roman(value), 0, 0)
    } else if num.is(NUM_F_EEEE) {
        // float8 represents every int4 exactly
        (eeee_float_str(value as f64, num.post), 0, 0)
    } else {
        let value = if num.is(NUM_F_MULTI) {
            // value * ((int32) pow(10, multi)), wrapping like PostgreSQL's
            // -fwrapv build
            let multi = double_to_int32(pow10_glibc(num.multi));
            num.pre += num.multi;
            value.wrapping_mul(multi)
        } else {
            value
        };
        layout_digits(&value.to_string(), &num, true)
    };
    num_processor_to_char(&nodes, num, &numstr, out_pre_spaces, sign)
}

/// `to_char(bigint, text)` (`int8_to_char`).
pub fn to_char_int8(value: i64, fmt: &str) -> Result<String> {
    let Some((nodes, mut num)) = num_tochar_prepare(fmt)? else {
        return Ok(String::new());
    };
    let (numstr, sign, out_pre_spaces) = if num.is(NUM_F_ROMAN) {
        // int84: only int4 values can be converted
        let v = i32::try_from(value).map_err(|_| err("22003", "integer out of range"))?;
        (int_to_roman(v), 0, 0)
    } else if num.is(NUM_F_EEEE) {
        // via numeric to avoid loss of precision
        let orgnum = numeric_out_sci(&Numeric::from_i64(value), num.post);
        let numstr = if !orgnum.starts_with('-') {
            format!(" {orgnum}").into_bytes()
        } else {
            orgnum.into_bytes()
        };
        (numstr, 0, 0)
    } else {
        let value = if num.is(NUM_F_MULTI) {
            // int8mul(value, dtoi8(pow(10, multi)))
            let multi = pow10_glibc(num.multi).round_ties_even();
            if !(-9223372036854775808.0..9223372036854775808.0).contains(&multi) {
                return Err(bigint_out_of_range());
            }
            num.pre += num.multi;
            value
                .checked_mul(multi as i64)
                .ok_or_else(bigint_out_of_range)?
        } else {
            value
        };
        layout_digits(&value.to_string(), &num, true)
    };
    num_processor_to_char(&nodes, num, &numstr, out_pre_spaces, sign)
}

/// Common part of `float4_to_char`/`float8_to_char` for digit pictures:
/// at most `max_digits` (FLT_DIG/DBL_DIG) digits are printed in total,
/// fractional ones first giving way to integral ones.
fn float_layout(val: f64, num: &mut NumDesc, max_digits: i32) -> (Vec<u8>, u8, i32) {
    let pre_len = pg_fmtfloat(val.abs(), 0, false, false).len() as i32;
    if pre_len >= max_digits {
        num.post = 0;
    } else if pre_len + num.post > max_digits {
        num.post = max_digits - pre_len;
    }
    let orgnum = pg_fmtfloat(val, num.post, false, false);
    layout_digits(&orgnum, num, false)
}

/// `to_char(double precision, text)` (`float8_to_char`).
///
/// At most 15 significant digits are printed (`DBL_DIG`); NaN and
/// infinities print as "NaN"/"Infinity" laid out like digits (or `#` with
/// `EEEE`).
pub fn to_char_float8(value: f64, fmt: &str) -> Result<String> {
    let Some((nodes, mut num)) = num_tochar_prepare(fmt)? else {
        return Ok(String::new());
    };
    let (numstr, sign, out_pre_spaces) = if num.is(NUM_F_ROMAN) {
        (int_to_roman(rint_to_int(value)), 0, 0)
    } else if num.is(NUM_F_EEEE) {
        let numstr = if value.is_nan() || value.is_infinite() {
            eeee_special_str(&num)
        } else {
            eeee_float_str(value, num.post)
        };
        (numstr, 0, 0)
    } else {
        let mut val = value;
        if num.is(NUM_F_MULTI) {
            val = value * pow10_glibc(num.multi);
            num.pre += num.multi;
        }
        float_layout(val, &mut num, DBL_DIG)
    };
    num_processor_to_char(&nodes, num, &numstr, out_pre_spaces, sign)
}

/// `to_char(smallint, text)`: PostgreSQL has no `smallint` variant and
/// resolves the call to `to_char(double precision, text)` (`double
/// precision` is the preferred numeric type), so this is
/// [`to_char_float8`] of the value. It differs from [`to_char_int4`] e.g. in
/// the 15-digit limit and in `V` with 10 or more digits.
pub fn to_char_int2(value: i16, fmt: &str) -> Result<String> {
    to_char_float8(value as f64, fmt)
}

/// `to_char(real, text)` (`float4_to_char`): like [`to_char_float8`] but
/// with at most 6 significant digits (`FLT_DIG`) and the `V` multiplication
/// done in single precision.
pub fn to_char_float4(value: f32, fmt: &str) -> Result<String> {
    let Some((nodes, mut num)) = num_tochar_prepare(fmt)? else {
        return Ok(String::new());
    };
    let (numstr, sign, out_pre_spaces) = if num.is(NUM_F_ROMAN) {
        (int_to_roman(rint_to_int(value as f64)), 0, 0)
    } else if num.is(NUM_F_EEEE) {
        let numstr = if value.is_nan() || value.is_infinite() {
            eeee_special_str(&num)
        } else {
            eeee_float_str(value as f64, num.post)
        };
        (numstr, 0, 0)
    } else {
        let mut val = value;
        if num.is(NUM_F_MULTI) {
            let multi = pow10_glibc(num.multi) as f32;
            val = value * multi;
            num.pre += num.multi;
        }
        float_layout(val as f64, &mut num, FLT_DIG)
    };
    num_processor_to_char(&nodes, num, &numstr, out_pre_spaces, sign)
}

/// `to_number(text, text)` (`numeric_to_number`).
///
/// Returns `Ok(None)` (SQL NULL) for an empty format picture, like
/// PostgreSQL. The extracted digits are converted by `numeric_in` with the
/// typmod implied by the picture, so errors include `22P02 invalid input
/// syntax for type numeric: " "` when no digit was found and `22003 numeric
/// field overflow` when there are more integral digits than positions;
/// `RN` and `EEEE` are rejected with 0A000.
pub fn to_number(s: &str, fmt: &str) -> Result<Option<Numeric>> {
    if fmt.is_empty() || fmt.len() >= MAX_FORMAT_LEN {
        return Ok(None);
    }
    let (nodes, mut num) = parse_format(c_str(fmt))?;
    let numstr = num_processor_from_char(&nodes, &mut num, s.as_bytes())?;

    let scale = num.post;
    let precision = num.pre + num.multi + scale;
    let typmod = (precision.wrapping_shl(16) | scale).wrapping_add(VARHDRSZ);
    let mut result = numeric_in_typmod(&numstr, typmod)?;

    if num.is(NUM_F_MULTI) {
        let x = Numeric::from_i64(10).pow(&Numeric::from_i64(-(num.multi as i64)))?;
        result = result.mul(&x)?;
    }
    Ok(Some(result))
}

/// `numeric_in` with a raw typmod (as built by `numeric_to_number`, which
/// does not validate it).
fn numeric_in_typmod(s: &str, typmod: i32) -> Result<Numeric> {
    // is_valid_numeric_typmod / numeric_typmod_precision / numeric_typmod_scale
    let typmod = (typmod >= VARHDRSZ).then(|| {
        let t = typmod - VARHDRSZ;
        ((t >> 16) & 0xffff, ((t & 0x7ff) ^ 1024) - 1024)
    });
    match parse_var(s)? {
        Parsed::Special(v) => match typmod {
            Some((precision, scale)) => apply_typmod_special(v, precision, scale),
            None => Ok(v),
        },
        Parsed::Finite(mut v) => {
            if let Some((precision, scale)) = typmod {
                apply_typmod_var(&mut v, precision, scale)?;
            }
            make_result(v)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(s: &str) -> Numeric {
        Numeric::parse(s).unwrap()
    }

    #[test]
    fn basic_pictures() {
        assert_eq!(
            to_char_numeric(&n("-1234.567"), "9,999.99").unwrap(),
            "-1,234.57"
        );
        assert_eq!(to_char_numeric(&n("0.5"), "9.9").unwrap(), "  .5");
        assert_eq!(to_char_numeric(&n("12"), "FM999th").unwrap(), "12th");
        assert_eq!(to_char_int4(25, "9EEEE").unwrap(), " 2e+01");
        assert_eq!(to_char_int8(25, "9EEEE").unwrap(), " 3e+01");
        assert_eq!(to_char_int4(1, "9V9999999999").unwrap(), " -2147483648");
        assert_eq!(to_char_int4(1999, "RN").unwrap(), "        MCMXCIX");
        assert_eq!(to_char_int4(5, "RNSG").unwrap(), "              V");
        assert_eq!(to_char_float8(f64::NAN, "9999.99").unwrap(), "  NaN");
        assert_eq!(
            to_char_float8(1.5, "9.99999999999999999\"abc\"").unwrap(),
            " 1.50000000000000"
        );
        assert_eq!(to_char_numeric(&n("9.99"), "9.9EEEE").unwrap(), " 10.0e+00");
        assert_eq!(to_char_int4(12, "").unwrap(), "");
    }

    #[test]
    fn to_number_basics() {
        assert_eq!(
            to_number("-1,234.5", "9,999.99")
                .unwrap()
                .unwrap()
                .to_string(),
            "-1234.5"
        );
        assert_eq!(to_number("12", "").unwrap(), None);
        let e = to_number("abc", "999").unwrap_err();
        assert_eq!(
            (e.sqlstate, e.message.as_str()),
            ("22P02", "invalid input syntax for type numeric: \" \"")
        );
        let e = to_number("12345", "99.999").unwrap_err();
        assert_eq!(e.sqlstate, "22003");
        assert_eq!(
            e.detail.as_deref(),
            Some(
                "A field with precision 2, scale 0 must round to an absolute value less than 10^2."
            )
        );
    }

    #[test]
    fn pow10_matches_correct_rounding_elsewhere() {
        for k in 0..=308 {
            if k == 23 || k == 210 {
                continue;
            }
            let lit: f64 = format!("1e{k}").parse().unwrap();
            assert_eq!(pow10_glibc(k), lit, "10^{k}");
        }
    }
}
