//! SIMILAR TO pattern translation, ported from `similar_escape_internal` in
//! PostgreSQL's src/backend/utils/adt/regexp.c (PostgreSQL License,
//! Copyright (c) 1996-2024, PostgreSQL Global Development Group).

use crate::{Error, Result, sqlstate};

/// Translates a SQL `SIMILAR TO` pattern to a POSIX (ARE) regular expression,
/// like PostgreSQL's `similar_to_escape(pattern [, escape])`. `escape` is the
/// escape character: `Some('\\')` when there is no ESCAPE clause, `None` for
/// `ESCAPE ''`.
pub fn similar_to_regex(pattern: &str, escape: Option<char>) -> Result<String> {
    let multibyte_escape = escape.is_some_and(|e| e.len_utf8() > 1);
    let mut r = String::with_capacity(pattern.len() * 3 + 23);
    r.push_str("^(?:");
    let mut afterescape = false;
    let mut nquotes = 0;
    let mut bracket_depth = 0i32;
    let mut charclass_pos = 0i32;
    for pchar in pattern.chars() {
        if multibyte_escape && pchar.len_utf8() > 1 {
            // slow, multi-byte path (no character-class bookkeeping)
            if afterescape {
                r.push('\\');
                r.push(pchar);
                afterescape = false;
            } else if Some(pchar) == escape {
                afterescape = true;
            } else {
                r.push(pchar);
            }
            continue;
        }
        if afterescape {
            if pchar == '"' && bracket_depth < 1 {
                match nquotes {
                    0 => r.push_str("){1,1}?("),
                    1 => r.push_str("){1,1}(?:"),
                    _ => {
                        return Err(Error::new(
                            sqlstate::INVALID_USE_OF_ESCAPE_CHARACTER,
                            "SQL regular expression may not contain more than two escape-double-quote separators",
                        ));
                    }
                }
                nquotes += 1;
            } else {
                r.push('\\');
                r.push(pchar);
                charclass_pos = 3;
            }
            afterescape = false;
        } else if escape.is_some() && Some(pchar) == escape {
            afterescape = true;
        } else if bracket_depth > 0 {
            if pchar == '\\' {
                r.push('\\');
            }
            r.push(pchar);
            if pchar == ']' && charclass_pos > 2 {
                bracket_depth -= 1;
            } else if pchar == '[' {
                bracket_depth += 1;
                charclass_pos = 3;
            } else if pchar == '^' {
                charclass_pos += 1;
            } else {
                charclass_pos = 3;
            }
        } else if pchar == '[' {
            r.push('[');
            bracket_depth = 1;
            charclass_pos = 1;
        } else if pchar == '%' {
            r.push_str(".*");
        } else if pchar == '_' {
            r.push('.');
        } else if pchar == '(' {
            r.push_str("(?:");
        } else if matches!(pchar, '\\' | '.' | '^' | '$') {
            r.push('\\');
            r.push(pchar);
        } else {
            r.push(pchar);
        }
    }
    r.push_str(")$");
    Ok(r)
}

/// `similar_to_escape(pattern, escape_text)` with the escape given as text,
/// including PostgreSQL's "invalid escape string" check.
pub fn similar_to_regex_text(pattern: &str, escape: Option<&str>) -> Result<String> {
    match escape {
        None => similar_to_regex(pattern, Some('\\')),
        Some(e) => {
            let mut it = e.chars();
            match (it.next(), it.next()) {
                (None, _) => similar_to_regex(pattern, None),
                (Some(c), None) => similar_to_regex(pattern, Some(c)),
                _ => Err(Error::new(sqlstate::INVALID_ESCAPE_SEQUENCE, "invalid escape string")),
            }
        }
    }
}
