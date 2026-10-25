//! jsonb_out / jsonb_pretty / escape_json: ports of JsonbToCStringWorker()
//! (src/backend/utils/adt/jsonb.c) and escape_json() (json.c) of PostgreSQL
//! 17 (Copyright (c) 1996-2024, PostgreSQL Global Development Group;
//! PostgreSQL License). Iterative, so arbitrarily deep values are safe.

use super::Jsonb;
use std::fmt::{self, Write};

/// escape_json(): appends `s` as a JSON string literal.
pub(crate) fn escape_json<W: Write + ?Sized>(out: &mut W, s: &str) -> fmt::Result {
    out.write_char('"')?;
    let b = s.as_bytes();
    let mut start = 0;
    for (i, &c) in b.iter().enumerate() {
        let esc: Option<&str> = match c {
            0x08 => Some("\\b"),
            0x0c => Some("\\f"),
            b'\n' => Some("\\n"),
            b'\r' => Some("\\r"),
            b'\t' => Some("\\t"),
            b'"' => Some("\\\""),
            b'\\' => Some("\\\\"),
            c if c < b' ' => None,
            _ => continue,
        };
        out.write_str(&s[start..i])?;
        match esc {
            Some(e) => out.write_str(e)?,
            None => write!(out, "\\u{:04x}", c)?,
        }
        start = i + 1;
    }
    out.write_str(&s[start..])?;
    out.write_char('"')
}

fn write_scalar<W: Write + ?Sized>(out: &mut W, v: &Jsonb) -> fmt::Result {
    match v {
        Jsonb::Null => out.write_str("null"),
        Jsonb::Bool(true) => out.write_str("true"),
        Jsonb::Bool(false) => out.write_str("false"),
        Jsonb::Number(n) => out.write_str(n.as_str()),
        Jsonb::String(s) => escape_json(out, s),
        Jsonb::Array(_) | Jsonb::Object(_) => unreachable!(),
    }
}

fn add_indent<W: Write + ?Sized>(out: &mut W, indent: bool, level: usize) -> fmt::Result {
    if indent {
        out.write_char('\n')?;
        for _ in 0..level {
            out.write_str("    ")?;
        }
    }
    Ok(())
}

struct Frame<'a> {
    v: &'a Jsonb,
    idx: usize,
}

fn open<W: Write + ?Sized>(out: &mut W, v: &Jsonb) -> fmt::Result {
    out.write_char(if matches!(v, Jsonb::Array(_)) {
        '['
    } else {
        '{'
    })
}

/// JsonbToCStringWorker(): compact (`indent == false`, jsonb_out) or
/// jsonb_pretty layout.
pub(crate) fn write_jsonb<W: Write + ?Sized>(out: &mut W, v: &Jsonb, indent: bool) -> fmt::Result {
    if !matches!(v, Jsonb::Array(_) | Jsonb::Object(_)) {
        return write_scalar(out, v);
    }
    let sep = if indent { "," } else { ", " };
    open(out, v)?;
    let mut stack = vec![Frame { v, idx: 0 }];
    loop {
        let level = stack.len().wrapping_sub(1);
        let Some(top) = stack.last_mut() else { break };
        let i = top.idx;
        let child = match top.v {
            Jsonb::Array(a) => a.get(i).map(|c| (None, c)),
            Jsonb::Object(p) => p.get(i).map(|(k, c)| (Some(k.as_str()), c)),
            _ => unreachable!(),
        };
        match child {
            Some((key, c)) => {
                top.idx += 1;
                if i > 0 {
                    out.write_str(sep)?;
                }
                add_indent(out, indent, level + 1)?;
                if let Some(k) = key {
                    escape_json(out, k)?;
                    out.write_str(": ")?;
                }
                if matches!(c, Jsonb::Array(_) | Jsonb::Object(_)) {
                    open(out, c)?;
                    stack.push(Frame { v: c, idx: 0 });
                } else {
                    write_scalar(out, c)?;
                }
            }
            None => {
                let close = if matches!(top.v, Jsonb::Array(_)) {
                    ']'
                } else {
                    '}'
                };
                add_indent(out, indent, level)?;
                out.write_char(close)?;
                stack.pop();
            }
        }
    }
    Ok(())
}
