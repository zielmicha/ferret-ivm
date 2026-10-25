//! uuid input/output (uuid_in / uuid_out).
//!
//! Ported from PostgreSQL 17 `src/backend/utils/adt/uuid.c`
//! (`string_to_uuid`, `uuid_out`).
//! Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group.
//! Portions Copyright (c) 1994, Regents of the University of California.
//! Used under the PostgreSQL License.

use crate::{Error, Result, sqlstate};

/// Value of an ASCII hex digit (`isxdigit` in the C locale), or 0xFF.
static HEX_VAL: [u8; 256] = {
    let mut t = [0xFFu8; 256];
    let mut i = 0;
    while i < 10 {
        t[b'0' as usize + i] = i as u8;
        i += 1;
    }
    let mut i = 0;
    while i < 6 {
        t[b'a' as usize + i] = 10 + i as u8;
        t[b'A' as usize + i] = 10 + i as u8;
        i += 1;
    }
    t
};

/// Parse a uuid like PostgreSQL's `uuid_in`.
///
/// Accepts 32 hex digits (either case), optionally with a hyphen after any
/// group of four hex digits (but not after the last), optionally enclosed in
/// braces. Anything else (including surrounding whitespace) is rejected with
/// `invalid input syntax for type uuid: "..."` (22P02).
pub fn parse(s: &str) -> Result<[u8; 16]> {
    // The C code works on a NUL-terminated string.
    let s = match s.as_bytes().iter().position(|&b| b == 0) {
        Some(n) => &s[..n],
        None => s,
    };
    let src = s.as_bytes();
    let syntax_error = || {
        Error::new(
            sqlstate::INVALID_TEXT_REPRESENTATION,
            format!("invalid input syntax for type uuid: \"{s}\""),
        )
    };

    let mut pos = 0usize;
    let braces = src.first() == Some(&b'{');
    if braces {
        pos = 1;
    }

    let mut out = [0u8; 16];
    for (i, byte) in out.iter_mut().enumerate() {
        if pos + 1 >= src.len() {
            return Err(syntax_error());
        }
        let hi = HEX_VAL[src[pos] as usize];
        let lo = HEX_VAL[src[pos + 1] as usize];
        if hi == 0xFF || lo == 0xFF {
            return Err(syntax_error());
        }
        *byte = (hi << 4) | lo;
        pos += 2;
        if pos < src.len() && src[pos] == b'-' && i % 2 == 1 && i < 15 {
            pos += 1;
        }
    }

    if braces {
        if src.get(pos) != Some(&b'}') {
            return Err(syntax_error());
        }
        pos += 1;
    }

    if pos != src.len() {
        return Err(syntax_error());
    }
    Ok(out)
}

/// Format a uuid like PostgreSQL's `uuid_out`: lowercase, 8-4-4-4-12.
pub fn format(u: &[u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut buf = [0u8; 36];
    let mut p = 0;
    for (i, &b) in u.iter().enumerate() {
        if i == 4 || i == 6 || i == 8 || i == 10 {
            buf[p] = b'-';
            p += 1;
        }
        buf[p] = HEX[(b >> 4) as usize];
        buf[p + 1] = HEX[(b & 0x0F) as usize];
        p += 2;
    }
    // SAFETY-free: all bytes are ASCII.
    String::from_utf8(buf.to_vec()).expect("ascii")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_basic() {
        let u = parse("A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11").unwrap();
        assert_eq!(format(&u), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        let u2 = parse("{a0eebc999c0b4ef8bb6d6bb9bd380a11}").unwrap();
        assert_eq!(u, u2);
        let u3 = parse("a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11").unwrap();
        assert_eq!(u, u3);
    }

    #[test]
    fn errors() {
        for s in [
            "",
            "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}",
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1",
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a111",
            "a0eebc99--9c0b-4ef8-bb6d-6bb9bd380a11",
            "a0eebc9-99c0b-4ef8-bb6d-6bb9bd380a11",
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11-",
            " a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
        ] {
            let e = parse(s).unwrap_err();
            assert_eq!(e.sqlstate, "22P02");
            assert_eq!(e.message, format!("invalid input syntax for type uuid: \"{s}\""));
        }
    }
}
