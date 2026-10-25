//! String functions with PostgreSQL 17 semantics (database encoding UTF8,
//! builtin collation provider with locale `C.UTF-8`).
//!
//! "Characters" are Unicode code points. Integer arguments typed `i64` in this
//! module correspond to `int4` SQL arguments; behaviour is exact for values in
//! the `int4` range (PostgreSQL itself never passes larger values).
//!
//! Ported from PostgreSQL's src/backend/utils/adt/varlena.c, oracle_compat.c,
//! quote.c, ruleutils.c (quote_identifier), formatting.c (initcap) and
//! src/backend/utils/adt/numutils.c (pg_strtoint32) — PostgreSQL License,
//! Copyright (c) 1996-2024, PostgreSQL Global Development Group, Portions
//! Copyright (c) 1994, The Regents of the University of California.

use crate::unicode;
use crate::{Error, Result, sqlstate};
use md5::Digest as _;

/// PostgreSQL's `MaxAllocSize`.
pub(crate) const MAX_ALLOC_SIZE: i64 = 0x3fff_ffff;

const VARHDRSZ: i64 = 4;

/// Database encoding max character length (UTF8).
const ENCODING_MAX_LENGTH: i64 = 4;

#[inline]
pub(crate) fn count_chars(s: &str) -> usize {
    // Count bytes that are not UTF-8 continuation bytes.
    s.as_bytes().iter().filter(|&&b| (b as i8) >= -0x40).count()
}

/// Byte offset of the `n`-th character (0-based); `s.len()` if past the end.
#[inline]
fn char_byte_offset(s: &str, n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    match s.char_indices().nth(n) {
        Some((i, _)) => i,
        None => s.len(),
    }
}

fn requested_length_too_large() -> Error {
    Error::new(sqlstate::PROGRAM_LIMIT_EXCEEDED, "requested length too large")
}

fn negative_substring_length() -> Error {
    Error::new(sqlstate::SUBSTRING_ERROR, "negative substring length not allowed")
}

/// `char_length(text)` / `length(text)`
pub fn char_length(s: &str) -> i32 {
    count_chars(s) as i32
}

/// `octet_length(text)`
pub fn octet_length(s: &str) -> i32 {
    s.len() as i32
}

/// `bit_length(text)`
pub fn bit_length(s: &str) -> i32 {
    (s.len() as i64 * 8) as i32
}

/// `lower(text)`: Unicode simple lowercase mapping.
pub fn lower(s: &str) -> String {
    if s.is_ascii() {
        return s.to_ascii_lowercase();
    }
    s.chars().map(unicode::to_lower).collect()
}

/// `upper(text)`: Unicode simple uppercase mapping.
pub fn upper(s: &str) -> String {
    if s.is_ascii() {
        return s.to_ascii_uppercase();
    }
    s.chars().map(unicode::to_upper).collect()
}

/// `initcap(text)` for the builtin provider (PG 17): word boundaries are the
/// start of the string and every change between alphanumeric and
/// non-alphanumeric characters; characters at a boundary are mapped with the
/// simple *uppercase* mapping, all others with the simple lowercase mapping.
pub fn initcap(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_alnum: Option<bool> = None;
    for c in s.chars() {
        let alnum = unicode::is_alnum(c, true);
        let boundary = prev_alnum != Some(alnum);
        prev_alnum = Some(alnum);
        out.push(if boundary { unicode::to_upper(c) } else { unicode::to_lower(c) });
    }
    out
}

/// Computes the (start, length) window of `substring(... from start for len)`
/// in units (characters or bytes). Returns None when the result is empty.
/// `Some((s1, None))` means "from s1 (1-based) to the end".
fn substring_window(start: i64, len: Option<i64>) -> Result<Option<(i64, Option<i64>)>> {
    let s1 = start.max(1);
    match len {
        None => Ok(Some((s1, None))),
        Some(l) if l < 0 => Err(negative_substring_length()),
        Some(l) => {
            let e = start as i128 + l as i128;
            if e > i32::MAX as i128 {
                // S + L overflows int4: the substring runs to the end.
                Ok(Some((s1, None)))
            } else if e < 1 {
                Ok(None)
            } else {
                Ok(Some((s1, Some(e as i64 - s1))))
            }
        }
    }
}

/// `substring(s from start [for len])` / `substr(s, start [, len])`.
pub fn substr(s: &str, start: i64, len: Option<i64>) -> Result<String> {
    let Some((s1, l1)) = substring_window(start, len)? else {
        return Ok(String::new());
    };
    let skip = (s1 - 1) as usize;
    let b0 = char_byte_offset(s, skip);
    if b0 >= s.len() {
        return Ok(String::new());
    }
    let rest = &s[b0..];
    let b1 = match l1 {
        None => rest.len(),
        Some(l) => char_byte_offset(rest, l.max(0) as usize),
    };
    Ok(rest[..b1].to_string())
}

/// `substring(bytea from start [for len])` / `substr(bytea, ...)`.
pub fn substr_bytes(b: &[u8], start: i64, len: Option<i64>) -> Result<Vec<u8>> {
    let Some((s1, l1)) = substring_window(start, len)? else {
        return Ok(Vec::new());
    };
    let off = (s1 - 1) as u64;
    if off >= b.len() as u64 {
        return Ok(Vec::new());
    }
    let off = off as usize;
    let end = match l1 {
        None => b.len(),
        Some(l) => (off as u64 + l.max(0) as u64).min(b.len() as u64) as usize,
    };
    Ok(b[off..end].to_vec())
}

/// `position(needle in s)` / `strpos(s, needle)`: 1-based character position
/// of the first occurrence, 0 if not found, 1 for an empty needle.
pub fn strpos(s: &str, needle: &str) -> i32 {
    if needle.is_empty() {
        return 1;
    }
    match s.find(needle) {
        Some(b) => count_chars(&s[..b]) as i32 + 1,
        None => 0,
    }
}

/// `overlay(s placing repl from from [for len])`.
pub fn overlay(s: &str, repl: &str, from: i64, len: Option<i64>) -> Result<String> {
    let sl = match len {
        Some(l) => l,
        None => char_length(repl) as i64,
    };
    if from <= 0 {
        return Err(negative_substring_length());
    }
    let sp_pl_sl = from as i128 + sl as i128;
    if sp_pl_sl > i32::MAX as i128 || sp_pl_sl < i32::MIN as i128 {
        return Err(Error::new(sqlstate::NUMERIC_VALUE_OUT_OF_RANGE, "integer out of range"));
    }
    let s1 = substr(s, 1, Some(from - 1))?;
    let s2 = substr(s, sp_pl_sl as i64, None)?;
    let mut out = String::with_capacity(s1.len() + repl.len() + s2.len());
    out.push_str(&s1);
    out.push_str(repl);
    out.push_str(&s2);
    Ok(out)
}

/// Character-set membership test used by the trim functions.
struct CharSet<'a> {
    ascii: [bool; 128],
    others: Vec<char>,
    _s: std::marker::PhantomData<&'a ()>,
}

impl<'a> CharSet<'a> {
    fn new(chars: &'a str) -> CharSet<'a> {
        let mut ascii = [false; 128];
        let mut others = Vec::new();
        for c in chars.chars() {
            if c.is_ascii() {
                ascii[c as usize] = true;
            } else {
                others.push(c);
            }
        }
        CharSet { ascii, others, _s: std::marker::PhantomData }
    }

    #[inline]
    fn contains(&self, c: char) -> bool {
        if c.is_ascii() { self.ascii[c as usize] } else { self.others.contains(&c) }
    }
}

/// `btrim(s, chars)` (`trim(both chars from s)`).
pub fn btrim(s: &str, chars: &str) -> String {
    if chars.is_empty() || s.is_empty() {
        return s.to_string();
    }
    let set = CharSet::new(chars);
    s.trim_matches(|c| set.contains(c)).to_string()
}

/// `ltrim(s, chars)` (`trim(leading chars from s)`).
pub fn ltrim(s: &str, chars: &str) -> String {
    if chars.is_empty() || s.is_empty() {
        return s.to_string();
    }
    let set = CharSet::new(chars);
    s.trim_start_matches(|c| set.contains(c)).to_string()
}

/// `rtrim(s, chars)` (`trim(trailing chars from s)`).
pub fn rtrim(s: &str, chars: &str) -> String {
    if chars.is_empty() || s.is_empty() {
        return s.to_string();
    }
    let set = CharSet::new(chars);
    s.trim_end_matches(|c| set.contains(c)).to_string()
}

/// `replace(s, from, to)`: non-overlapping, left to right; an empty `from`
/// leaves the string unchanged.
pub fn replace(s: &str, from: &str, to: &str) -> String {
    if s.is_empty() || from.is_empty() {
        return s.to_string();
    }
    s.replace(from, to)
}

/// `split_part(s, delim, n)`; negative `n` counts from the end.
pub fn split_part(s: &str, delim: &str, n: i32) -> Result<String> {
    if n == 0 {
        return Err(Error::new(sqlstate::INVALID_PARAMETER_VALUE, "field position must not be zero"));
    }
    if s.is_empty() {
        return Ok(String::new());
    }
    if delim.is_empty() || !s.contains(delim) {
        return Ok(if n == 1 || n == -1 { s.to_string() } else { String::new() });
    }
    if n > 0 {
        Ok(s.split(delim).nth((n - 1) as usize).unwrap_or("").to_string())
    } else {
        let fields: Vec<&str> = s.split(delim).collect();
        let k = fields.len() as i64 + n as i64; // index from start
        if k < 0 { Ok(String::new()) } else { Ok(fields[k as usize].to_string()) }
    }
}

/// `left(s, n)`: first `n` characters; negative `n` drops `|n|` characters
/// from the end.
pub fn left(s: &str, n: i32) -> String {
    if n < 0 {
        let keep = count_chars(s) as i64 + n as i64;
        if keep <= 0 {
            return String::new();
        }
        s[..char_byte_offset(s, keep as usize)].to_string()
    } else {
        s[..char_byte_offset(s, n as usize)].to_string()
    }
}

/// `right(s, n)`: last `n` characters; negative `n` drops `|n|` characters
/// from the start.
pub fn right(s: &str, n: i32) -> String {
    // PostgreSQL computes -n in int32 arithmetic (INT_MIN stays INT_MIN).
    let skip: i64 = if n < 0 { n.wrapping_neg() as i64 } else { count_chars(s) as i64 - n as i64 };
    if skip <= 0 {
        return s.to_string();
    }
    s[char_byte_offset(s, skip as usize)..].to_string()
}

fn pad_check(s: &str, len: i32, fill: &str) -> Result<(usize, usize)> {
    let len = len.max(0) as i64;
    let mut s1len = count_chars(s) as i64;
    if s1len > len {
        s1len = len;
    }
    let len = if fill.is_empty() { s1len } else { len };
    let bytelen = ENCODING_MAX_LENGTH * len + VARHDRSZ;
    if bytelen > i32::MAX as i64 || bytelen > MAX_ALLOC_SIZE {
        return Err(requested_length_too_large());
    }
    Ok((s1len as usize, (len - s1len) as usize))
}

fn push_fill(out: &mut String, fill: &str, mut m: usize) {
    if m == 0 || fill.is_empty() {
        return;
    }
    let flen = count_chars(fill);
    while m >= flen {
        out.push_str(fill);
        m -= flen;
    }
    if m > 0 {
        out.push_str(&fill[..char_byte_offset(fill, m)]);
    }
}

/// `lpad(s, len, fill)` with PostgreSQL's error for oversized results.
pub fn lpad_checked(s: &str, len: i32, fill: &str) -> Result<String> {
    let (s1len, m) = pad_check(s, len, fill)?;
    let mut out = String::new();
    push_fill(&mut out, fill, m);
    out.push_str(&s[..char_byte_offset(s, s1len)]);
    Ok(out)
}

/// `rpad(s, len, fill)` with PostgreSQL's error for oversized results.
pub fn rpad_checked(s: &str, len: i32, fill: &str) -> Result<String> {
    let (s1len, m) = pad_check(s, len, fill)?;
    let mut out = String::new();
    out.push_str(&s[..char_byte_offset(s, s1len)]);
    push_fill(&mut out, fill, m);
    Ok(out)
}

/// `lpad(s, len, fill)`.
///
/// # Panics
/// PostgreSQL raises "requested length too large" (54000) when `len` exceeds
/// 268435454 with a non-empty fill; this function panics in that case. Use
/// [`lpad_checked`] to get the error instead.
pub fn lpad(s: &str, len: i32, fill: &str) -> String {
    lpad_checked(s, len, fill).unwrap_or_else(|e| panic!("lpad: {e}"))
}

/// `rpad(s, len, fill)`.
///
/// # Panics
/// See [`lpad`]; use [`rpad_checked`] to get the error instead.
pub fn rpad(s: &str, len: i32, fill: &str) -> String {
    rpad_checked(s, len, fill).unwrap_or_else(|e| panic!("rpad: {e}"))
}

/// `repeat(s, n)`.
pub fn repeat(s: &str, n: i32) -> Result<String> {
    let count = n.max(0) as i64;
    let tlen = count * s.len() as i64;
    if tlen > i32::MAX as i64 || tlen + VARHDRSZ > i32::MAX as i64 || tlen + VARHDRSZ > MAX_ALLOC_SIZE {
        return Err(requested_length_too_large());
    }
    Ok(s.repeat(count as usize))
}

/// `reverse(s)`: reverses the order of characters.
pub fn reverse(s: &str) -> String {
    s.chars().rev().collect()
}

/// `translate(s, from, to)`.
pub fn translate(s: &str, from: &str, to: &str) -> String {
    if s.is_empty() || from.is_empty() {
        return s.to_string();
    }
    let from: Vec<char> = from.chars().collect();
    let to: Vec<char> = to.chars().collect();
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match from.iter().position(|&f| f == c) {
            Some(i) => {
                if let Some(&t) = to.get(i) {
                    out.push(t);
                }
            }
            None => out.push(c),
        }
    }
    out
}

/// `ascii(s)`: code point of the first character, 0 for an empty string.
pub fn ascii(s: &str) -> i32 {
    s.chars().next().map_or(0, |c| c as i32)
}

/// `chr(code)`.
pub fn chr(code: i32) -> Result<String> {
    if code < 0 {
        return Err(Error::new(sqlstate::INVALID_PARAMETER_VALUE, "character number must be positive"));
    }
    if code == 0 {
        return Err(Error::new(sqlstate::PROGRAM_LIMIT_EXCEEDED, "null character not permitted"));
    }
    let cvalue = code as u32;
    if cvalue > 0x10ffff {
        return Err(Error::new(
            sqlstate::PROGRAM_LIMIT_EXCEEDED,
            format!("requested character too large for encoding: {cvalue}"),
        ));
    }
    match char::from_u32(cvalue) {
        Some(c) => Ok(c.to_string()),
        None => Err(Error::new(
            sqlstate::PROGRAM_LIMIT_EXCEEDED,
            format!("requested character not valid for encoding: {cvalue}"),
        )),
    }
}

/// `starts_with(s, prefix)` / `s ^@ prefix`.
pub fn starts_with(s: &str, prefix: &str) -> bool {
    s.as_bytes().starts_with(prefix.as_bytes())
}

const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

pub(crate) fn hex_lower(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len() * 2);
    for &b in data {
        out.push(HEX_DIGITS[(b >> 4) as usize] as char);
        out.push(HEX_DIGITS[(b & 15) as usize] as char);
    }
    out
}

/// `md5(text|bytea)`: lowercase hex digest.
pub fn md5_hex(data: &[u8]) -> String {
    hex_lower(&md5::Md5::digest(data))
}

/// `sha224(bytea)`
pub fn sha224(data: &[u8]) -> Vec<u8> {
    sha2::Sha224::digest(data).to_vec()
}

/// `sha256(bytea)`
pub fn sha256(data: &[u8]) -> Vec<u8> {
    sha2::Sha256::digest(data).to_vec()
}

/// `sha384(bytea)`
pub fn sha384(data: &[u8]) -> Vec<u8> {
    sha2::Sha384::digest(data).to_vec()
}

/// `sha512(bytea)`
pub fn sha512(data: &[u8]) -> Vec<u8> {
    sha2::Sha512::digest(data).to_vec()
}

/// Keywords that are not UNRESERVED_KEYWORD in PostgreSQL 17's kwlist.h
/// (these must be quoted by quote_ident). Sorted.
static NON_UNRESERVED_KEYWORDS: &[&str] = &[
    "all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric", "authorization",
    "between", "bigint", "binary", "bit", "boolean", "both", "case", "cast", "char", "character",
    "check", "coalesce", "collate", "collation", "column", "concurrently", "constraint", "create",
    "cross", "current_catalog", "current_date", "current_role", "current_schema", "current_time",
    "current_timestamp", "current_user", "dec", "decimal", "default", "deferrable", "desc", "distinct",
    "do", "else", "end", "except", "exists", "extract", "false", "fetch", "float", "for", "foreign",
    "freeze", "from", "full", "grant", "greatest", "group", "grouping", "having", "ilike", "in",
    "initially", "inner", "inout", "int", "integer", "intersect", "interval", "into", "is", "isnull",
    "join", "json", "json_array", "json_arrayagg", "json_exists", "json_object", "json_objectagg",
    "json_query", "json_scalar", "json_serialize", "json_table", "json_value", "lateral", "leading",
    "least", "left", "like", "limit", "localtime", "localtimestamp", "merge_action", "national",
    "natural", "nchar", "none", "normalize", "not", "notnull", "null", "nullif", "numeric", "offset",
    "on", "only", "or", "order", "out", "outer", "overlaps", "overlay", "placing", "position",
    "precision", "primary", "real", "references", "returning", "right", "row", "select",
    "session_user", "setof", "similar", "smallint", "some", "substring", "symmetric", "system_user",
    "table", "tablesample", "then", "time", "timestamp", "to", "trailing", "treat", "trim", "true",
    "union", "unique", "user", "using", "values", "varchar", "variadic", "verbose", "when", "where",
    "window", "with", "xmlattributes", "xmlconcat", "xmlelement", "xmlexists", "xmlforest",
    "xmlnamespaces", "xmlparse", "xmlpi", "xmlroot", "xmlserialize", "xmltable",
];

/// `quote_ident(s)` (ruleutils.c quote_identifier with
/// quote_all_identifiers = off).
pub fn quote_ident(s: &str) -> String {
    let b = s.as_bytes();
    let mut safe = matches!(b.first(), Some(c) if c.is_ascii_lowercase() || *c == b'_');
    let mut nquotes = 0;
    for &c in b {
        if !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_') {
            safe = false;
            if c == b'"' {
                nquotes += 1;
            }
        }
    }
    if safe && NON_UNRESERVED_KEYWORDS.binary_search(&s).is_ok() {
        safe = false;
    }
    if safe {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len() + nquotes + 2);
    out.push('"');
    for c in s.chars() {
        if c == '"' {
            out.push('"');
        }
        out.push(c);
    }
    out.push('"');
    out
}

/// `quote_literal(s)`: single quotes doubled; backslashes doubled and an `E`
/// prefix added when the string contains a backslash.
pub fn quote_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 3);
    if s.contains('\\') {
        out.push('E');
    }
    out.push('\'');
    for c in s.chars() {
        if c == '\'' || c == '\\' {
            out.push(c);
        }
        out.push(c);
    }
    out.push('\'');
    out
}

/// `quote_nullable(s)`: like quote_literal, but NULL yields `NULL`.
pub fn quote_nullable(s: Option<&str>) -> String {
    match s {
        Some(s) => quote_literal(s),
        None => "NULL".to_string(),
    }
}

/// `to_hex(bigint)`: two's complement representation for negative values.
pub fn to_hex(v: i64) -> String {
    format!("{:x}", v as u64)
}

/// `to_hex(integer)`: 32-bit two's complement for negative values.
pub fn to_hex32(v: i32) -> String {
    format!("{:x}", v as u32)
}

/// `to_bin(bigint)`
pub fn to_bin(v: i64) -> String {
    format!("{:b}", v as u64)
}

/// `to_bin(integer)`
pub fn to_bin32(v: i32) -> String {
    format!("{:b}", v as u32)
}

/// `to_oct(bigint)`
pub fn to_oct(v: i64) -> String {
    format!("{:o}", v as u64)
}

/// `to_oct(integer)`
pub fn to_oct32(v: i32) -> String {
    format!("{:o}", v as u32)
}

/// C-locale `isspace`.
#[inline]
pub(crate) fn c_isspace(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

/// `pg_strtoint32` (int4 input): accepts surrounding whitespace, a sign,
/// decimal / 0x / 0o / 0b prefixes and underscores between digits.
pub fn parse_int4(s: &str) -> Result<i32> {
    let out_of_range = || {
        Error::new(sqlstate::NUMERIC_VALUE_OUT_OF_RANGE, format!("value \"{s}\" is out of range for type integer"))
    };
    let invalid = || {
        Error::new(sqlstate::INVALID_TEXT_REPRESENTATION, format!("invalid input syntax for type integer: \"{s}\""))
    };
    let b = s.as_bytes();
    // A NUL byte cannot occur in text; treat end of slice as the terminator.
    let at = |i: usize| -> u8 { if i < b.len() { b[i] } else { 0 } };
    const LIMIT10: u32 = 214748364; // -(INT_MIN / 10)

    // Fast path: plain decimal digits.
    {
        let mut p = 0;
        let mut neg = false;
        if at(p) == b'-' {
            p += 1;
            neg = true;
        }
        let d = at(p).wrapping_sub(b'0');
        if d < 10 {
            p += 1;
            let mut tmp: u32 = d as u32;
            let mut slow = false;
            loop {
                let d = at(p).wrapping_sub(b'0');
                if d >= 10 {
                    break;
                }
                p += 1;
                if tmp > LIMIT10 {
                    return Err(out_of_range());
                }
                tmp = tmp * 10 + d as u32;
            }
            if p != b.len() {
                slow = true;
            }
            if !slow {
                if neg {
                    if tmp > 2147483648 {
                        return Err(out_of_range());
                    }
                    return Ok((tmp as i64).wrapping_neg() as i32);
                }
                if tmp > i32::MAX as u32 {
                    return Err(out_of_range());
                }
                return Ok(tmp as i32);
            }
        }
    }

    // Slow path.
    let mut p = 0;
    let mut neg = false;
    let mut tmp: u32 = 0;
    while c_isspace(at(p)) {
        p += 1;
    }
    if at(p) == b'-' {
        p += 1;
        neg = true;
    } else if at(p) == b'+' {
        p += 1;
    }
    let firstdigit;
    let radix_prefix = |c: u8| -> Option<u32> {
        match c {
            b'x' | b'X' => Some(16),
            b'o' | b'O' => Some(8),
            b'b' | b'B' => Some(2),
            _ => None,
        }
    };
    if at(p) == b'0'
        && let Some(base) = radix_prefix(at(p + 1))
    {
        p += 2;
        firstdigit = p;
        let limit = 2147483648u32 / base; // -(INT_MIN / base)
        loop {
            let c = at(p);
            let dv = (c as char).to_digit(base);
            if let Some(dv) = dv.filter(|_| c != 0) {
                if tmp > limit {
                    return Err(out_of_range());
                }
                tmp = tmp * base + dv;
                p += 1;
            } else if c == b'_' {
                p += 1;
                let n = at(p);
                if n == 0 || (n as char).to_digit(base).is_none() {
                    return Err(invalid());
                }
            } else {
                break;
            }
        }
    } else {
        firstdigit = p;
        loop {
            let c = at(p);
            if c.is_ascii_digit() {
                if tmp > LIMIT10 {
                    return Err(out_of_range());
                }
                tmp = tmp * 10 + (c - b'0') as u32;
                p += 1;
            } else if c == b'_' {
                if p == firstdigit {
                    return Err(invalid());
                }
                p += 1;
                if !at(p).is_ascii_digit() {
                    return Err(invalid());
                }
            } else {
                break;
            }
        }
    }
    if p == firstdigit {
        return Err(invalid());
    }
    while c_isspace(at(p)) {
        p += 1;
    }
    if p != b.len() {
        return Err(invalid());
    }
    if neg {
        if tmp > 2147483648 {
            return Err(out_of_range());
        }
        return Ok((tmp as i64).wrapping_neg() as i32);
    }
    if tmp > i32::MAX as u32 {
        return Err(out_of_range());
    }
    Ok(tmp as i32)
}

const FORMAT_HINT: &str = "For a single \"%\" use \"%%\".";

fn format_err(msg: impl Into<String>) -> Error {
    Error::new(sqlstate::INVALID_PARAMETER_VALUE, msg)
}

fn unterminated_format() -> Error {
    // The hint is not part of the Error struct; message only.
    let _ = FORMAT_HINT;
    format_err("unterminated format() type specifier")
}

fn check_enlarge(out: &str, needed: usize) -> Result<()> {
    if needed as i64 >= MAX_ALLOC_SIZE - out.len() as i64 {
        return Err(Error::with_detail(
            sqlstate::PROGRAM_LIMIT_EXCEEDED,
            "out of memory",
            format!("Cannot enlarge string buffer containing {} bytes by {} more bytes.", out.len(), needed),
        ));
    }
    Ok(())
}

/// `format(fmt, args...)`. Each argument is given as its text representation
/// (output function result); NULL is `None`. Width arguments (`%*s`) are
/// converted to int4 like PostgreSQL does for non-integer argument types
/// (`pg_strtoint32` on the text form), which gives identical results for
/// int2/int4 arguments.
pub fn format(fmt: &str, args: &[Option<String>]) -> Result<String> {
    let b = fmt.as_bytes();
    let end = b.len();
    let nargs = args.len() + 1;
    let mut out = String::with_capacity(fmt.len() + 16);
    let mut arg: usize = 1;
    let mut cp = 0usize;

    // ADVANCE_PARSE_POINTER
    let advance = |cp: &mut usize| -> Result<()> {
        *cp += 1;
        if *cp >= end { Err(unterminated_format()) } else { Ok(()) }
    };
    // text_format_parse_digits: returns Some(value) if any digits were found.
    let parse_digits = |cp: &mut usize| -> Result<Option<i32>> {
        let mut found = false;
        let mut val: i32 = 0;
        while *cp < end && b[*cp].is_ascii_digit() {
            let digit = (b[*cp] - b'0') as i32;
            val = val
                .checked_mul(10)
                .and_then(|v| v.checked_add(digit))
                .ok_or_else(|| Error::new(sqlstate::NUMERIC_VALUE_OUT_OF_RANGE, "number is out of range"))?;
            advance(cp)?;
            found = true;
        }
        Ok(if found { Some(val) } else { None })
    };

    while cp < end {
        let start_run = cp;
        while cp < end && b[cp] != b'%' {
            cp += 1;
        }
        if cp > start_run {
            out.push_str(&fmt[start_run..cp]);
        }
        if cp >= end {
            break;
        }
        // b[cp] == '%'
        advance(&mut cp)?;
        if b[cp] == b'%' {
            out.push('%');
            cp += 1;
            continue;
        }

        // text_format_parse_format
        let mut argpos: i64 = -1;
        let mut widthpos: i64 = -1;
        let mut minus = false;
        let mut width: i32 = 0;
        let mut direct_width_done = false;
        if let Some(n) = parse_digits(&mut cp)? {
            if b[cp] != b'$' {
                width = n;
                direct_width_done = true;
            } else {
                argpos = n as i64;
                if n == 0 {
                    return Err(format_err("format specifies argument 0, but arguments are numbered from 1"));
                }
                advance(&mut cp)?;
            }
        }
        if !direct_width_done {
            while b[cp] == b'-' {
                minus = true;
                advance(&mut cp)?;
            }
            if b[cp] == b'*' {
                advance(&mut cp)?;
                if let Some(n) = parse_digits(&mut cp)? {
                    if b[cp] != b'$' {
                        return Err(format_err("width argument position must be ended by \"$\""));
                    }
                    widthpos = n as i64;
                    if n == 0 {
                        return Err(format_err("format specifies argument 0, but arguments are numbered from 1"));
                    }
                    advance(&mut cp)?;
                } else {
                    widthpos = 0;
                }
            } else if let Some(n) = parse_digits(&mut cp)? {
                width = n;
            }
        }

        // Conversion character.
        let conv = b[cp];
        if !matches!(conv, b's' | b'I' | b'L') {
            let ch = fmt[cp..].chars().next().unwrap_or('?');
            return Err(format_err(format!("unrecognized format() type specifier \"{ch}\"")));
        }

        if widthpos >= 0 {
            if widthpos > 0 {
                arg = widthpos as usize;
            }
            if arg >= nargs {
                return Err(format_err("too few arguments for format()"));
            }
            width = match &args[arg - 1] {
                None => 0,
                Some(v) => parse_int4(v)?,
            };
            arg += 1;
        }

        if argpos > 0 {
            arg = argpos as usize;
        }
        if arg >= nargs {
            return Err(format_err("too few arguments for format()"));
        }
        let value = args[arg - 1].as_deref();
        arg += 1;

        let formatted: std::borrow::Cow<str> = match (conv, value) {
            (b's', None) => "".into(),
            (b'L', None) => "NULL".into(),
            (b'I', None) => {
                return Err(Error::new(
                    sqlstate::NULL_VALUE_NOT_ALLOWED,
                    "null values cannot be formatted as an SQL identifier",
                ));
            }
            (b'I', Some(v)) => quote_ident(v).into(),
            (b'L', Some(v)) => quote_literal(v).into(),
            (_, Some(v)) => v.into(),
            _ => unreachable!(),
        };

        // text_format_append_string
        if width == 0 {
            check_enlarge(&out, formatted.len())?;
            out.push_str(&formatted);
        } else {
            let mut align_left = false;
            let mut w = width as i64;
            if w < 0 {
                align_left = true;
                if width == i32::MIN {
                    return Err(Error::new(sqlstate::NUMERIC_VALUE_OUT_OF_RANGE, "number is out of range"));
                }
                w = -w;
            } else if minus {
                align_left = true;
            }
            let len = count_chars(&formatted) as i64;
            if align_left {
                check_enlarge(&out, formatted.len())?;
                out.push_str(&formatted);
                if len < w {
                    check_enlarge(&out, (w - len) as usize)?;
                    out.extend(std::iter::repeat_n(' ', (w - len) as usize));
                }
            } else {
                if len < w {
                    check_enlarge(&out, (w - len) as usize)?;
                    out.extend(std::iter::repeat_n(' ', (w - len) as usize));
                }
                check_enlarge(&out, formatted.len())?;
                out.push_str(&formatted);
            }
        }
        cp += 1;
    }
    Ok(out)
}

/// `string_to_array(s, delim [, null_str])`. A NULL `delim` splits into
/// characters; an empty `delim` yields the whole string as one element; an
/// empty input yields an empty array.
pub fn string_to_array(s: &str, delim: Option<&str>, null_str: Option<&str>) -> Vec<Option<String>> {
    let accum = |field: &str| -> Option<String> {
        match null_str {
            Some(n) if n == field => None,
            _ => Some(field.to_string()),
        }
    };
    match delim {
        Some(d) => {
            if s.is_empty() {
                return Vec::new();
            }
            if d.is_empty() {
                return vec![accum(s)];
            }
            s.split(d).map(accum).collect()
        }
        None => {
            let mut out = Vec::with_capacity(s.len());
            let mut buf = [0u8; 4];
            for c in s.chars() {
                out.push(accum(c.encode_utf8(&mut buf)));
            }
            out
        }
    }
}

fn hexval_n(b: &[u8]) -> u32 {
    b.iter().fold(0u32, |acc, &c| (acc << 4) | (c as char).to_digit(16).unwrap_or(0))
}

/// `unistr(s)`: decodes `\XXXX`, `\+XXXXXX`, `\uXXXX`, `\UXXXXXXXX` escapes
/// and `\\`.
pub fn unistr(s: &str) -> Result<String> {
    let b = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut pair_first: u32 = 0;
    let invalid_pair = || Error::new(sqlstate::SYNTAX_ERROR, "invalid Unicode surrogate pair");
    let is_x = |b: &[u8]| b.iter().all(|c| c.is_ascii_hexdigit());
    let mut i = 0usize;
    while i < b.len() {
        let len = b.len() - i;
        if b[i] == b'\\' {
            let (code, consumed) = if len >= 2 && b[i + 1] == b'\\' {
                if pair_first != 0 {
                    return Err(invalid_pair());
                }
                out.push('\\');
                i += 2;
                continue;
            } else if len >= 5 && is_x(&b[i + 1..i + 5]) {
                (hexval_n(&b[i + 1..i + 5]), 5)
            } else if len >= 6 && b[i + 1] == b'u' && is_x(&b[i + 2..i + 6]) {
                (hexval_n(&b[i + 2..i + 6]), 6)
            } else if len >= 8 && b[i + 1] == b'+' && is_x(&b[i + 2..i + 8]) {
                (hexval_n(&b[i + 2..i + 8]), 8)
            } else if len >= 10 && b[i + 1] == b'U' && is_x(&b[i + 2..i + 10]) {
                (hexval_n(&b[i + 2..i + 10]), 10)
            } else {
                return Err(Error::new(sqlstate::SYNTAX_ERROR, "invalid Unicode escape"));
            };
            let mut unicode = code;
            if !(unicode > 0 && unicode <= 0x10ffff) {
                return Err(Error::new(
                    sqlstate::INVALID_PARAMETER_VALUE,
                    format!("invalid Unicode code point: {unicode:04X}"),
                ));
            }
            let is_first = |c: u32| (0xD800..=0xDBFF).contains(&c);
            let is_second = |c: u32| (0xDC00..=0xDFFF).contains(&c);
            if pair_first != 0 {
                if is_second(unicode) {
                    unicode = (((pair_first & 0x3FF) << 10) | (unicode & 0x3FF)) + 0x10000;
                    pair_first = 0;
                } else {
                    return Err(invalid_pair());
                }
            } else if is_second(unicode) {
                return Err(invalid_pair());
            }
            if is_first(unicode) {
                pair_first = unicode;
            } else {
                // Valid scalar value at this point.
                out.push(char::from_u32(unicode).ok_or_else(invalid_pair)?);
            }
            i += consumed;
        } else {
            if pair_first != 0 {
                return Err(invalid_pair());
            }
            // Copy one whole character.
            let c = s[i..].chars().next().unwrap();
            out.push(c);
            i += c.len_utf8();
        }
    }
    if pair_first != 0 {
        return Err(invalid_pair());
    }
    Ok(out)
}
