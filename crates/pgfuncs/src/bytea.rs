//! bytea input/output and encode/decode.
//!
//! Ported from PostgreSQL 17 `src/backend/utils/adt/varlena.c` (`byteain`,
//! `byteaout`) and `src/backend/utils/adt/encode.c` (hex, base64 and escape
//! encodings).
//! Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group.
//! Portions Copyright (c) 1994, Regents of the University of California.
//! Used under the PostgreSQL License.

use crate::{Error, Result, sqlstate};

/// MaxAllocSize (1 GB - 1).
const MAX_ALLOC_SIZE: u64 = 0x3fff_ffff;
/// Size of a varlena header.
const VARHDRSZ: u64 = 4;

const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

/// `hexlookup` from encode.c (ASCII only; -1 = not a hex digit).
static HEX_LOOKUP: [i8; 256] = {
    let mut t = [-1i8; 256];
    let mut i = 0;
    while i < 10 {
        t[b'0' as usize + i] = i as i8;
        i += 1;
    }
    let mut i = 0;
    while i < 6 {
        t[b'a' as usize + i] = 10 + i as i8;
        t[b'A' as usize + i] = 10 + i as i8;
        i += 1;
    }
    t
};

const BASE64_CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// `b64lookup` from encode.c (-1 = not a base64 symbol).
static B64_LOOKUP: [i8; 256] = {
    let mut t = [-1i8; 256];
    let mut i = 0;
    while i < 64 {
        t[BASE64_CHARS[i] as usize] = i as i8;
        i += 1;
    }
    t
};

/// Treat `s` like a C string: everything up to the first NUL byte.
#[inline]
fn c_str(s: &str) -> &str {
    match s.as_bytes().iter().position(|&b| b == 0) {
        Some(n) => &s[..n],
        None => s,
    }
}

/// The (complete) UTF-8 character starting at byte offset `pos` of `s`,
/// mirroring `"%.*s", pg_mblen_range(s, srcend), s` in error messages.
fn char_at(s: &[u8], pos: usize) -> String {
    let b = s[pos];
    let len = if b < 0x80 {
        1
    } else if b & 0xE0 == 0xC0 {
        2
    } else if b & 0xF0 == 0xE0 {
        3
    } else if b & 0xF8 == 0xF0 {
        4
    } else {
        1
    };
    let end = (pos + len).min(s.len());
    String::from_utf8_lossy(&s[pos..end]).into_owned()
}

fn invalid_bytea_syntax() -> Error {
    Error::new(sqlstate::INVALID_TEXT_REPRESENTATION, "invalid input syntax for type bytea")
}

/// Parse the external text form of a bytea value (`byteain`).
///
/// Supports the hex format (`\x` followed by hex digit pairs, with
/// whitespace allowed between pairs) and the traditional escape format
/// (`\\` for a backslash, `\ooo` octal escapes with the first digit 0-3, all
/// other bytes literal).
pub fn parse(s: &str) -> Result<Vec<u8>> {
    let s = c_str(s);
    let b = s.as_bytes();
    if b.len() >= 2 && b[0] == b'\\' && b[1] == b'x' {
        let mut out = Vec::with_capacity((b.len() - 2) / 2);
        hex_decode_into(&b[2..], &mut out)?;
        return Ok(out);
    }

    // Traditional escaped style.
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c != b'\\' {
            out.push(c);
            i += 1;
        } else if i + 3 < b.len()
            && (b'0'..=b'3').contains(&b[i + 1])
            && (b'0'..=b'7').contains(&b[i + 2])
            && (b'0'..=b'7').contains(&b[i + 3])
        {
            out.push(((b[i + 1] - b'0') << 6) | ((b[i + 2] - b'0') << 3) | (b[i + 3] - b'0'));
            i += 4;
        } else if i + 1 < b.len() && b[i + 1] == b'\\' {
            out.push(b'\\');
            i += 2;
        } else {
            return Err(invalid_bytea_syntax());
        }
    }
    Ok(out)
}

/// Format a bytea value in the hex output format (`byteaout` with
/// `bytea_output = hex`), e.g. `\x0102`.
pub fn format(b: &[u8]) -> String {
    let mut out = Vec::with_capacity(2 + b.len() * 2);
    out.extend_from_slice(b"\\x");
    hex_encode_into(b, &mut out);
    // All bytes are ASCII.
    String::from_utf8(out).expect("ascii")
}

/// Format a bytea value in the traditional escape output format (`byteaout`
/// with `bytea_output = escape`).
pub fn format_escape(b: &[u8]) -> String {
    let mut out = String::with_capacity(b.len());
    for &c in b {
        if c == b'\\' {
            out.push_str("\\\\");
        } else if !(0x20..=0x7e).contains(&c) {
            out.push('\\');
            out.push((b'0' + (c >> 6)) as char);
            out.push((b'0' + ((c >> 3) & 7)) as char);
            out.push((b'0' + (c & 7)) as char);
        } else {
            out.push(c as char);
        }
    }
    out
}

#[inline]
fn hex_encode_into(src: &[u8], out: &mut Vec<u8>) {
    out.reserve(src.len() * 2);
    for &c in src {
        out.push(HEX_CHARS[(c >> 4) as usize]);
        out.push(HEX_CHARS[(c & 0x0F) as usize]);
    }
}

/// `hex_decode_safe`: whitespace (space, tab, CR, LF) is allowed between
/// digit pairs only.
fn hex_decode_into(src: &[u8], out: &mut Vec<u8>) -> Result<()> {
    let mut i = 0;
    let n = src.len();
    while i < n {
        let c = src[i];
        if c == b' ' || c == b'\n' || c == b'\t' || c == b'\r' {
            i += 1;
            continue;
        }
        let v1 = HEX_LOOKUP[c as usize];
        if v1 < 0 {
            return Err(invalid_hex_digit(src, i));
        }
        i += 1;
        if i >= n {
            return Err(Error::new(
                sqlstate::INVALID_PARAMETER_VALUE,
                "invalid hexadecimal data: odd number of digits",
            ));
        }
        let v2 = HEX_LOOKUP[src[i] as usize];
        if v2 < 0 {
            return Err(invalid_hex_digit(src, i));
        }
        i += 1;
        out.push(((v1 as u8) << 4) | v2 as u8);
    }
    Ok(())
}

fn invalid_hex_digit(src: &[u8], pos: usize) -> Error {
    Error::new(
        sqlstate::INVALID_PARAMETER_VALUE,
        format!("invalid hexadecimal digit: \"{}\"", char_at(src, pos)),
    )
}

/// `pg_base64_encode`: a newline is emitted after every 76 output
/// characters (including after the last group if it ends a line).
fn base64_encode(src: &[u8]) -> String {
    let mut out: Vec<u8> = Vec::with_capacity(base64_enc_len(src.len() as u64) as usize);
    let mut line_end = 76usize;
    let mut pos: i32 = 2;
    let mut buf: u32 = 0;
    for &c in src {
        buf |= (c as u32) << (pos << 3);
        pos -= 1;
        if pos < 0 {
            out.push(BASE64_CHARS[((buf >> 18) & 0x3f) as usize]);
            out.push(BASE64_CHARS[((buf >> 12) & 0x3f) as usize]);
            out.push(BASE64_CHARS[((buf >> 6) & 0x3f) as usize]);
            out.push(BASE64_CHARS[(buf & 0x3f) as usize]);
            pos = 2;
            buf = 0;
        }
        if out.len() >= line_end {
            out.push(b'\n');
            line_end = out.len() + 76;
        }
    }
    if pos != 2 {
        out.push(BASE64_CHARS[((buf >> 18) & 0x3f) as usize]);
        out.push(BASE64_CHARS[((buf >> 12) & 0x3f) as usize]);
        out.push(if pos == 0 { BASE64_CHARS[((buf >> 6) & 0x3f) as usize] } else { b'=' });
        out.push(b'=');
    }
    String::from_utf8(out).expect("ascii")
}

fn base64_enc_len(srclen: u64) -> u64 {
    srclen.div_ceil(3) * 4 + srclen / (76 * 3 / 4)
}

/// `pg_base64_decode`, including its quirks (whitespace anywhere, `=`
/// allowed only in the third or fourth position of a group, groups after
/// the padding are decoded with the padding's length).
fn base64_decode(src: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(src.len() * 3 / 4);
    let mut buf: u32 = 0;
    let mut pos = 0;
    let mut end = 0;
    let mut i = 0;
    while i < src.len() {
        let c = src[i];
        i += 1;
        if c == b' ' || c == b'\t' || c == b'\n' || c == b'\r' {
            continue;
        }
        let b: u32 = if c == b'=' {
            if end == 0 {
                if pos == 2 {
                    end = 1;
                } else if pos == 3 {
                    end = 2;
                } else {
                    return Err(Error::new(
                        sqlstate::INVALID_PARAMETER_VALUE,
                        "unexpected \"=\" while decoding base64 sequence",
                    ));
                }
            }
            0
        } else {
            // `c > 0 && c < 127` with a signed char excludes NUL, DEL and
            // all bytes with the high bit set.
            let v = if c > 0 && c < 127 { B64_LOOKUP[c as usize] } else { -1 };
            if v < 0 {
                return Err(Error::new(
                    sqlstate::INVALID_PARAMETER_VALUE,
                    format!(
                        "invalid symbol \"{}\" found while decoding base64 sequence",
                        char_at(src, i - 1)
                    ),
                ));
            }
            v as u32
        };
        buf = (buf << 6).wrapping_add(b);
        pos += 1;
        if pos == 4 {
            out.push(((buf >> 16) & 255) as u8);
            if end == 0 || end > 1 {
                out.push(((buf >> 8) & 255) as u8);
            }
            if end == 0 || end > 2 {
                out.push((buf & 255) as u8);
            }
            buf = 0;
            pos = 0;
        }
    }
    if pos != 0 {
        // PostgreSQL also reports the hint "Input data is missing padding,
        // is truncated, or is otherwise corrupted."
        return Err(Error::new(sqlstate::INVALID_PARAMETER_VALUE, "invalid base64 end sequence"));
    }
    Ok(out)
}

/// `esc_encode`: NUL and high-bit bytes become `\ooo`, backslash becomes
/// `\\`, everything else (including other control characters) is copied.
fn escape_encode(src: &[u8]) -> String {
    let mut out: Vec<u8> = Vec::with_capacity(src.len());
    for &c in src {
        if c == 0 || c & 0x80 != 0 {
            out.push(b'\\');
            out.push(b'0' + (c >> 6));
            out.push(b'0' + ((c >> 3) & 7));
            out.push(b'0' + (c & 7));
        } else if c == b'\\' {
            out.push(b'\\');
            out.push(b'\\');
        } else {
            out.push(c);
        }
    }
    String::from_utf8(out).expect("ascii")
}

/// `esc_decode` (with the validation of `esc_dec_len`).
fn escape_decode(src: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(src.len());
    let end = src.len();
    let mut i = 0;
    while i < end {
        if src[i] != b'\\' {
            out.push(src[i]);
            i += 1;
        } else if i + 3 < end
            && (b'0'..=b'3').contains(&src[i + 1])
            && (b'0'..=b'7').contains(&src[i + 2])
            && (b'0'..=b'7').contains(&src[i + 3])
        {
            out.push(((src[i + 1] - b'0') << 6) | ((src[i + 2] - b'0') << 3) | (src[i + 3] - b'0'));
            i += 4;
        } else if i + 1 < end && src[i + 1] == b'\\' {
            out.push(b'\\');
            i += 2;
        } else {
            return Err(invalid_bytea_syntax());
        }
    }
    Ok(out)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Encoding {
    Hex,
    Base64,
    Escape,
}

/// `pg_find_encoding`: ASCII case-insensitive lookup of the format name.
fn find_encoding(name: &str) -> Result<Encoding> {
    let n = c_str(name);
    if n.eq_ignore_ascii_case("hex") {
        Ok(Encoding::Hex)
    } else if n.eq_ignore_ascii_case("base64") {
        Ok(Encoding::Base64)
    } else if n.eq_ignore_ascii_case("escape") {
        Ok(Encoding::Escape)
    } else {
        Err(Error::new(sqlstate::INVALID_PARAMETER_VALUE, format!("unrecognized encoding: \"{n}\"")))
    }
}

/// `encode(bytea, text)`: formats `hex`, `base64` (lines wrapped at 76
/// characters) and `escape`; the format name is case-insensitive.
pub fn encode(b: &[u8], format: &str) -> Result<String> {
    let enc = find_encoding(format)?;
    let len = b.len() as u64;
    let result_len = match enc {
        Encoding::Hex => len << 1,
        Encoding::Base64 => base64_enc_len(len),
        Encoding::Escape => b
            .iter()
            .map(|&c| if c == 0 || c & 0x80 != 0 { 4u64 } else if c == b'\\' { 2 } else { 1 })
            .sum(),
    };
    if result_len > MAX_ALLOC_SIZE - VARHDRSZ {
        return Err(Error::new(sqlstate::PROGRAM_LIMIT_EXCEEDED, "result of encoding conversion is too large"));
    }
    Ok(match enc {
        Encoding::Hex => {
            let mut out = Vec::with_capacity(b.len() * 2);
            hex_encode_into(b, &mut out);
            String::from_utf8(out).expect("ascii")
        }
        Encoding::Base64 => base64_encode(b),
        Encoding::Escape => escape_encode(b),
    })
}

/// `decode(text, text)`: formats `hex`, `base64` and `escape`; the format
/// name is case-insensitive.
pub fn decode(s: &str, format: &str) -> Result<Vec<u8>> {
    let enc = find_encoding(format)?;
    let src = s.as_bytes();
    match enc {
        Encoding::Hex => {
            let mut out = Vec::with_capacity(src.len() / 2);
            hex_decode_into(src, &mut out)?;
            Ok(out)
        }
        Encoding::Base64 => base64_decode(src),
        Encoding::Escape => escape_decode(src),
    }
}
