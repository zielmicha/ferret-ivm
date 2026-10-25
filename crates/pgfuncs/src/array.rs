//! Array text input/output (array_in / array_out).
//!
//! Ported from PostgreSQL 17 `src/backend/utils/adt/arrayfuncs.c`
//! (`array_in`, `ReadArrayDimensions`, `ReadDimensionInt`, `ReadArrayStr`,
//! `ReadArrayToken`, `array_out`).
//! Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group.
//! Portions Copyright (c) 1994, Regents of the University of California.
//! Used under the PostgreSQL License.
//!
//! Only the array structure is handled here: element strings are returned
//! (for input) or taken (for output) in their external text form; applying
//! the element type's input/output function is the caller's job.
//!
//! Known deviation: `array_in` also rejects arrays whose total element data
//! exceeds 1 GB ("array size exceeds the maximum allowed (1073741823)");
//! that depends on the element type's storage and is not checked here.

use crate::{Error, Result, sqlstate};

/// Maximum number of array dimensions (`MAXDIM`).
pub const MAXDIM: usize = 6;

/// `MaxArraySize` = `MaxAllocSize / sizeof(Datum)`.
pub const MAX_ARRAY_SIZE: usize = 0x3fff_ffff / 8;

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedArray {
    /// (lower bound, length) per dimension. Empty for an empty array.
    pub dims: Vec<(i32, i32)>,
    /// Row-major elements; NULL -> None.
    pub elements: Vec<Option<String>>,
}

impl ParsedArray {
    /// Number of dimensions (0 for an empty array).
    pub fn ndim(&self) -> usize {
        self.dims.len()
    }

    /// True if the array has no elements.
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }
}

/// `scanner_isspace`: the whitespace characters recognized by the array
/// parser and quoted by the array output function.
#[inline]
pub fn array_isspace(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

enum Token {
    LevelStart,
    LevelEnd,
    Delim,
    Elem,
    ElemNull,
}

struct Parser<'a> {
    /// Input bytes (truncated at the first NUL, like a C string).
    s: &'a [u8],
    /// Original input, for error messages.
    orig: &'a str,
    delim: char,
    delim_bytes: [u8; 4],
    delim_len: usize,
}

impl<'a> Parser<'a> {
    /// Byte at `p`, or 0 past the end (emulating the NUL terminator).
    #[inline]
    fn at(&self, p: usize) -> u8 {
        if p < self.s.len() { self.s[p] } else { 0 }
    }

    #[inline]
    fn is_delim(&self, p: usize) -> bool {
        if self.delim_len == 1 {
            self.at(p) == self.delim_bytes[0] && p < self.s.len()
        } else {
            self.s[p.min(self.s.len())..].starts_with(&self.delim_bytes[..self.delim_len])
        }
    }

    fn malformed(&self, detail: impl Into<String>) -> Error {
        Error::with_detail(
            sqlstate::INVALID_TEXT_REPRESENTATION,
            format!("malformed array literal: \"{}\"", self.orig),
            detail,
        )
    }

    fn unexpected_char(&self, c: char) -> Error {
        self.malformed(format!("Unexpected \"{c}\" character."))
    }

    /// `ReadDimensionInt`: parse an optionally signed decimal integer
    /// (`strtol` semantics). Returns None (leaving `p` unchanged) if there
    /// are no digits.
    fn read_dimension_int(&self, p: &mut usize) -> Result<Option<i32>> {
        let start = *p;
        let c = self.at(start);
        if !c.is_ascii_digit() && c != b'-' && c != b'+' {
            return Ok(None);
        }
        let mut q = start;
        let negative = c == b'-';
        if c == b'-' || c == b'+' {
            q += 1;
        }
        let digits_start = q;
        let mut value: i64 = 0;
        let mut overflow = false;
        while self.at(q).is_ascii_digit() {
            if !overflow {
                value = value * 10 + (self.at(q) - b'0') as i64;
                if value > (i32::MAX as i64) + 1 {
                    overflow = true;
                }
            }
            q += 1;
        }
        if q == digits_start {
            // strtol: no conversion performed, end pointer = start.
            return Ok(None);
        }
        *p = q;
        let value = if negative { -value } else { value };
        if overflow || value > i32::MAX as i64 || value < i32::MIN as i64 {
            return Err(Error::new(sqlstate::PROGRAM_LIMIT_EXCEEDED, "array bound is out of integer range"));
        }
        Ok(Some(value as i32))
    }

    /// `ReadArrayDimensions`: parse `[m:n]` / `[n]` items. Returns ndim.
    fn read_array_dimensions(&self, p: &mut usize, dim: &mut [i32; MAXDIM], lbound: &mut [i32; MAXDIM]) -> Result<usize> {
        let mut ndim = 0usize;
        loop {
            while array_isspace(self.at(*p)) {
                *p += 1;
            }
            if self.at(*p) != b'[' {
                break;
            }
            *p += 1;
            if ndim >= MAXDIM {
                return Err(too_many_dims());
            }
            let i = match self.read_dimension_int(p)? {
                Some(i) => i,
                None => {
                    return Err(self.malformed("\"[\" must introduce explicitly-specified array dimensions."));
                }
            };
            let ub = if self.at(*p) == b':' {
                lbound[ndim] = i;
                *p += 1;
                match self.read_dimension_int(p)? {
                    Some(u) => u,
                    None => return Err(self.malformed("Missing array dimension value.")),
                }
            } else {
                lbound[ndim] = 1;
                i
            };
            if self.at(*p) != b']' {
                return Err(self.malformed("Missing \"]\" after array dimensions."));
            }
            *p += 1;

            if ub < lbound[ndim] {
                return Err(Error::new(
                    sqlstate::ARRAY_SUBSCRIPT_ERROR,
                    "upper bound cannot be less than lower bound",
                ));
            }
            if ub == i32::MAX {
                return Err(Error::new(
                    sqlstate::PROGRAM_LIMIT_EXCEEDED,
                    format!("array upper bound is too large: {ub}"),
                ));
            }
            let len = match ub.checked_sub(lbound[ndim]).and_then(|d| d.checked_add(1)) {
                Some(l) => l,
                None => return Err(array_too_large()),
            };
            dim[ndim] = len;
            ndim += 1;
        }
        Ok(ndim)
    }

    /// `ReadArrayToken`: read one token starting at `*p`. For element tokens
    /// the de-escaped element is left in `buf`.
    fn read_token(&self, p: &mut usize, buf: &mut Vec<u8>) -> Result<Token> {
        buf.clear();
        let mut q = *p;

        // Identify token type, skipping leading whitespace.
        loop {
            match self.at(q) {
                0 => return Err(self.malformed("Unexpected end of input.")),
                b'{' => {
                    *p = q + 1;
                    return Ok(Token::LevelStart);
                }
                b'}' => {
                    *p = q + 1;
                    return Ok(Token::LevelEnd);
                }
                b'"' => {
                    q += 1;
                    return self.read_quoted(p, q, buf);
                }
                c => {
                    if self.is_delim(q) {
                        *p = q + self.delim_len;
                        return Ok(Token::Delim);
                    }
                    if array_isspace(c) {
                        q += 1;
                        continue;
                    }
                    return self.read_unquoted(p, q, buf);
                }
            }
        }
    }

    fn read_quoted(&self, p: &mut usize, mut q: usize, buf: &mut Vec<u8>) -> Result<Token> {
        let s = self.s;
        loop {
            // Copy a run of ordinary bytes.
            let run_start = q;
            while q < s.len() && s[q] != b'"' && s[q] != b'\\' {
                q += 1;
            }
            buf.extend_from_slice(&s[run_start..q]);
            match self.at(q) {
                0 => return Err(self.malformed("Unexpected end of input.")),
                b'\\' => {
                    // Skip backslash, copy next byte as-is.
                    q += 1;
                    let c = self.at(q);
                    if c == 0 {
                        return Err(self.malformed("Unexpected end of input."));
                    }
                    buf.push(c);
                    q += 1;
                }
                _ => {
                    // Closing quote: the next non-whitespace must be a
                    // delimiter or a brace.
                    q += 1;
                    while self.at(q) != 0 {
                        let c = self.at(q);
                        if self.is_delim(q) || c == b'}' || c == b'{' {
                            *p = q;
                            return Ok(Token::Elem);
                        }
                        if !array_isspace(c) {
                            return Err(self.malformed("Incorrectly quoted array element."));
                        }
                        q += 1;
                    }
                    return Err(self.malformed("Unexpected end of input."));
                }
            }
        }
    }

    fn read_unquoted(&self, p: &mut usize, mut q: usize, buf: &mut Vec<u8>) -> Result<Token> {
        let s = self.s;
        // Trailing whitespace is not part of the element; `dstlen` tracks
        // how much of the output is known not to be trailing whitespace.
        let mut dstlen = 0usize;
        let mut has_escapes = false;
        let delim0 = self.delim_bytes[0];
        loop {
            // Copy a run of ordinary bytes, noting the last non-whitespace.
            let run_start = q;
            let mut nonws_end = 0usize;
            while q < s.len() {
                let c = s[q];
                if matches!(c, b'{' | b'}' | b'"' | b'\\') || (c == delim0 && self.is_delim(q)) {
                    break;
                }
                if !array_isspace(c) {
                    nonws_end = q + 1;
                }
                q += 1;
            }
            if q > run_start {
                let base = buf.len();
                buf.extend_from_slice(&s[run_start..q]);
                if nonws_end > 0 {
                    dstlen = base + (nonws_end - run_start);
                }
            }
            match self.at(q) {
                0 => return Err(self.malformed("Unexpected end of input.")),
                b'{' => return Err(self.unexpected_char('{')),
                b'"' => return Err(self.malformed("Incorrectly quoted array element.")),
                b'\\' => {
                    q += 1;
                    let c = self.at(q);
                    if c == 0 {
                        return Err(self.malformed("Unexpected end of input."));
                    }
                    buf.push(c);
                    q += 1;
                    dstlen = buf.len();
                    has_escapes = true;
                }
                _ => {
                    // A delimiter or '}' ends the element.
                    buf.truncate(dstlen);
                    *p = q;
                    if !has_escapes && buf.eq_ignore_ascii_case(b"NULL") {
                        return Ok(Token::ElemNull);
                    }
                    return Ok(Token::Elem);
                }
            }
        }
    }

    /// `ReadArrayStr`: parse the `{...}` part, deducing or checking the
    /// dimensions. Returns ndim; elements are appended to `elements`.
    fn read_array_str(
        &self,
        p: &mut usize,
        mut ndim: usize,
        dim: &mut [i32; MAXDIM],
        elements: &mut Vec<Option<String>>,
    ) -> Result<usize> {
        let dimensions_specified = ndim != 0;
        let mut buf: Vec<u8> = Vec::new();
        let mut nest_level = 0usize;
        let mut ndim_frozen = dimensions_specified;
        let mut expect_delim = false;
        let mut nelems = [0i32; MAXDIM];

        let dimension_error = || {
            if dimensions_specified {
                self.malformed("Specified array dimensions do not match array contents.")
            } else {
                self.malformed("Multidimensional arrays must have sub-arrays with matching dimensions.")
            }
        };

        debug_assert_eq!(self.at(*p), b'{');
        loop {
            match self.read_token(p, &mut buf)? {
                Token::LevelStart => {
                    if expect_delim {
                        return Err(self.unexpected_char('{'));
                    }
                    if nest_level >= MAXDIM {
                        return Err(too_many_dims());
                    }
                    nelems[nest_level] = 0;
                    nest_level += 1;
                    if nest_level > ndim {
                        if ndim_frozen {
                            return Err(dimension_error());
                        }
                        ndim = nest_level;
                    }
                }
                Token::LevelEnd => {
                    debug_assert!(nest_level > 0);
                    if nelems[nest_level - 1] > 0 && !expect_delim {
                        return Err(self.unexpected_char('}'));
                    }
                    nest_level -= 1;
                    if nest_level > 0 {
                        nelems[nest_level - 1] += 1;
                    }
                    if dim[nest_level] < 0 {
                        dim[nest_level] = nelems[nest_level];
                    } else if nelems[nest_level] != dim[nest_level] {
                        return Err(dimension_error());
                    }
                    expect_delim = true;
                }
                Token::Delim => {
                    if !expect_delim {
                        return Err(self.unexpected_char(self.delim));
                    }
                    expect_delim = false;
                }
                tok @ (Token::Elem | Token::ElemNull) => {
                    debug_assert!(nest_level > 0);
                    if expect_delim {
                        return Err(self.malformed("Unexpected array element."));
                    }
                    if elements.len() >= MAX_ARRAY_SIZE {
                        return Err(array_too_large());
                    }
                    if matches!(tok, Token::ElemNull) {
                        elements.push(None);
                    } else {
                        // The buffer holds whole UTF-8 characters: only ASCII
                        // backslashes and ASCII whitespace were removed.
                        let s = match std::str::from_utf8(&buf) {
                            Ok(v) => v.to_owned(),
                            Err(_) => String::from_utf8_lossy(&buf).into_owned(),
                        };
                        elements.push(Some(s));
                    }
                    ndim_frozen = true;
                    if nest_level != ndim {
                        return Err(dimension_error());
                    }
                    nelems[nest_level - 1] += 1;
                    expect_delim = true;
                }
            }
            if nest_level == 0 {
                break;
            }
        }
        Ok(ndim)
    }
}

fn too_many_dims() -> Error {
    Error::new(
        sqlstate::PROGRAM_LIMIT_EXCEEDED,
        format!("number of array dimensions exceeds the maximum allowed ({MAXDIM})"),
    )
}

fn array_too_large() -> Error {
    Error::new(
        sqlstate::PROGRAM_LIMIT_EXCEEDED,
        format!("array size exceeds the maximum allowed ({MAX_ARRAY_SIZE})"),
    )
}

/// Parse the external text form of an array (`array_in`), for an element
/// type whose delimiter is `delim` (`,` for nearly all types, `;` for box).
///
/// Handles `{}`, nested sub-arrays, explicit bounds (`[2:3]={a,b}`,
/// `[1:2][1:1]={{a},{b}}`), double quotes and backslash escapes, unquoted
/// `NULL` (case-insensitive) as SQL NULL, and the whitespace rules of
/// PostgreSQL 17. Errors carry PostgreSQL's exact SQLSTATE, message and
/// detail.
pub fn parse(s: &str, delim: char) -> Result<ParsedArray> {
    // The C code works on a NUL-terminated string.
    let s = match s.as_bytes().iter().position(|&b| b == 0) {
        Some(n) => &s[..n],
        None => s,
    };
    let mut delim_bytes = [0u8; 4];
    let delim_len = delim.encode_utf8(&mut delim_bytes).len();
    let parser = Parser { s: s.as_bytes(), orig: s, delim, delim_bytes, delim_len };

    let mut dim = [-1i32; MAXDIM];
    let mut lbound = [1i32; MAXDIM];
    let mut p = 0usize;
    let ndim = parser.read_array_dimensions(&mut p, &mut dim, &mut lbound)?;

    if ndim == 0 {
        if parser.at(p) != b'{' {
            return Err(parser.malformed("Array value must start with \"{\" or dimension information."));
        }
    } else {
        if parser.at(p) != b'=' {
            return Err(parser.malformed("Missing \"=\" after array dimensions."));
        }
        p += 1;
        while array_isspace(parser.at(p)) {
            p += 1;
        }
        if parser.at(p) != b'{' {
            return Err(parser.malformed("Array contents must start with \"{\"."));
        }
    }

    let mut elements = Vec::new();
    let ndim = parser.read_array_str(&mut p, ndim, &mut dim, &mut elements)?;

    // Only whitespace is allowed after the closing brace.
    while p < parser.s.len() {
        if !array_isspace(parser.s[p]) {
            return Err(parser.malformed("Junk after closing right brace."));
        }
        p += 1;
    }

    if elements.is_empty() {
        return Ok(ParsedArray { dims: Vec::new(), elements });
    }
    let dims = (0..ndim).map(|i| (lbound[i], dim[i])).collect();
    Ok(ParsedArray { dims, elements })
}

/// Does `array_out` need to double-quote this element?
#[inline]
fn needs_quote(v: &str, delim: char) -> bool {
    if v.is_empty() || v.eq_ignore_ascii_case("NULL") {
        return true;
    }
    let ascii_delim = if delim.is_ascii() { Some(delim as u8) } else { None };
    for &b in v.as_bytes() {
        if matches!(b, b'"' | b'\\' | b'{' | b'}') || array_isspace(b) || Some(b) == ascii_delim {
            return true;
        }
    }
    ascii_delim.is_none() && v.contains(delim)
}

/// Format an array in its external text form (`array_out`).
///
/// `dims` are (lower bound, length) per dimension and `elements` are in
/// row-major order (their count must equal the product of the lengths).
/// Zero-element arrays are printed as `{}`; the `[lb:ub]...=` decoration is
/// added when any lower bound is not 1. Elements are double-quoted when
/// empty, equal to `NULL` (case-insensitive), or containing braces, the
/// delimiter, double quotes, backslashes or whitespace; `"` and `\` are
/// backslash-escaped inside quotes.
pub fn format(dims: &[(i32, i32)], elements: &[Option<&str>], delim: char) -> String {
    let ndim = dims.len();
    let nitems: usize = if ndim == 0 {
        0
    } else {
        dims.iter().map(|&(_, n)| n.max(0) as usize).product()
    };
    if nitems == 0 {
        return "{}".to_string();
    }
    assert_eq!(
        elements.len(),
        nitems,
        "pgfuncs::array::format: element count does not match the dimensions"
    );

    let needdims = dims.iter().any(|&(lb, _)| lb != 1);

    let mut cap = 2 * nitems + 2 * ndim + 2;
    for e in elements {
        cap += e.map_or(4, |s| s.len() + 2);
    }
    let mut out = String::with_capacity(cap);

    if needdims {
        use std::fmt::Write as _;
        for &(lb, n) in dims {
            let ub = lb.wrapping_add(n).wrapping_sub(1);
            let _ = write!(out, "[{lb}:{ub}]");
        }
        out.push('=');
    }

    out.push('{');
    let mut indx = vec![0i32; ndim];
    let mut j = 0usize;
    let mut k = 0usize;
    loop {
        for _ in j..ndim - 1 {
            out.push('{');
        }

        match elements[k] {
            None => out.push_str("NULL"),
            Some(v) => {
                if needs_quote(v, delim) {
                    out.push('"');
                    let bytes = v.as_bytes();
                    let mut start = 0;
                    for (idx, &b) in bytes.iter().enumerate() {
                        if b == b'"' || b == b'\\' {
                            out.push_str(&v[start..idx]);
                            out.push('\\');
                            start = idx;
                        }
                    }
                    out.push_str(&v[start..]);
                    out.push('"');
                } else {
                    out.push_str(v);
                }
            }
        }
        k += 1;

        let mut i = ndim as isize - 1;
        while i >= 0 {
            let iu = i as usize;
            indx[iu] += 1;
            if indx[iu] < dims[iu].1 {
                out.push(delim);
                break;
            }
            indx[iu] = 0;
            out.push('}');
            i -= 1;
        }
        if i < 0 {
            break;
        }
        j = i as usize;
    }
    out
}

/// Convenience wrapper around [`format`](fn@format) for owned elements, e.g. a
/// [`ParsedArray`].
pub fn format_owned(dims: &[(i32, i32)], elements: &[Option<String>], delim: char) -> String {
    let refs: Vec<Option<&str>> = elements.iter().map(|e| e.as_deref()).collect();
    format(dims, &refs, delim)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> ParsedArray {
        parse(s, ',').unwrap()
    }

    #[test]
    fn simple() {
        let a = p("{a,b,NULL,\"NULL\",\" x \"}");
        assert_eq!(a.dims, vec![(1, 5)]);
        assert_eq!(
            a.elements,
            vec![Some("a".into()), Some("b".into()), None, Some("NULL".into()), Some(" x ".into())]
        );
        assert_eq!(format_owned(&a.dims, &a.elements, ','), "{a,b,NULL,\"NULL\",\" x \"}");
    }

    #[test]
    fn empty() {
        assert_eq!(p("{}"), ParsedArray { dims: vec![], elements: vec![] });
        assert_eq!(p(" {{},{}} "), ParsedArray { dims: vec![], elements: vec![] });
        assert_eq!(format(&[], &[], ','), "{}");
        assert_eq!(format(&[(1, 0)], &[], ','), "{}");
    }

    #[test]
    fn bounds() {
        let a = p("[0:1][2:2]={{a},{b}}");
        assert_eq!(a.dims, vec![(0, 2), (2, 1)]);
        assert_eq!(format_owned(&a.dims, &a.elements, ','), "[0:1][2:2]={{a},{b}}");
    }

    #[test]
    fn errors() {
        let e = parse("{a,}", ',').unwrap_err();
        assert_eq!(e.message, "malformed array literal: \"{a,}\"");
        assert_eq!(e.detail.as_deref(), Some("Unexpected \"}\" character."));
        let e = parse("[2:1]={}", ',').unwrap_err();
        assert_eq!(e.sqlstate, "2202E");
    }
}
