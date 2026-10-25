//! Functions on the `json` type (text kept verbatim): ports of json_in()
//! (json.c) and get_worker() with its semantic actions (jsonfuncs.c) of
//! PostgreSQL 17 (Copyright (c) 1996-2024, PostgreSQL Global Development
//! Group; PostgreSQL License).

use super::output;
use super::parser::{self, Lexer, NullSem, Scalar, Sem, Tok};
use crate::Result;

/// json_in: validates `s` as json (the json type keeps the original text).
/// Unlike jsonb, strings are not de-escaped here, so `\u0000` and unpaired
/// surrogates are accepted.
pub fn validate_json(s: &str) -> Result<()> {
    let mut lex = Lexer::new(s, false);
    parser::parse_json(&mut lex, &mut NullSem, parser::BUDGET_JSON)
}

/// escape_json: `s` as a JSON string literal (with quotes).
pub fn escape_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    let _ = output::escape_json(&mut out, s);
    out
}

/// GetState of get_worker() for a one-element path, without result
/// normalization (the `->` operators).
struct GetOne<'k> {
    key: Option<&'k str>,
    index: Option<i64>,
    array_cur_index: i64,
    result_start: Option<usize>,
    tresult: Option<(usize, usize)>,
}

impl<'a> Sem<'a> for GetOne<'_> {
    fn object_field_start(
        &mut self,
        lex: &Lexer<'a>,
        fname: &mut String,
        _isnull: bool,
    ) -> Result<()> {
        if lex.lex_level == 1 && self.key.is_some_and(|k| k == fname.as_str()) {
            self.tresult = None;
            self.result_start = lex.token_start;
        }
        Ok(())
    }

    fn object_field_end(&mut self, lex: &Lexer<'a>, fname: &str, _isnull: bool) -> Result<()> {
        if lex.lex_level == 1
            && self.key.is_some_and(|k| k == fname)
            && let Some(start) = self.result_start.take()
        {
            self.tresult = Some((start, lex.prev_token_terminator));
        }
        Ok(())
    }

    fn array_start(&mut self, lex: &Lexer<'a>) -> Result<()> {
        if lex.lex_level == 0
            && let Some(idx) = self.index
        {
            self.array_cur_index = -1;
            if idx < 0 && idx != i64::from(i32::MIN) {
                let nelements = parser::count_array_elements(lex)?;
                if -idx <= nelements {
                    self.index = Some(idx + nelements);
                }
            }
        }
        Ok(())
    }

    fn array_element_start(&mut self, lex: &Lexer<'a>, _isnull: bool) -> Result<()> {
        if lex.lex_level == 1 && self.index.is_some() {
            self.array_cur_index += 1;
            if Some(self.array_cur_index) == self.index {
                self.tresult = None;
                self.result_start = lex.token_start;
            }
        }
        Ok(())
    }

    fn array_element_end(&mut self, lex: &Lexer<'a>, _isnull: bool) -> Result<()> {
        if lex.lex_level == 1
            && self.index.is_some()
            && Some(self.array_cur_index) == self.index
            && let Some(start) = self.result_start.take()
        {
            self.tresult = Some((start, lex.prev_token_terminator));
        }
        Ok(())
    }
}

fn get_one(json: &str, key: Option<&str>, index: Option<i64>) -> Result<Option<String>> {
    let mut lex = Lexer::new(json, true);
    let mut st = GetOne {
        key,
        index,
        array_cur_index: -1,
        result_start: None,
        tresult: None,
    };
    parser::parse_json(&mut lex, &mut st, parser::BUDGET_JSON_GET)?;
    Ok(st.tresult.map(|(s, e)| json[s..e].to_owned()))
}

/// json `->` text: the raw text of the value of `key` in a top-level object
/// (last one wins for duplicate keys); None if absent or not an object.
/// The whole document is parsed with de-escaping, so e.g. `\u0000` anywhere
/// raises an error as in PostgreSQL.
pub fn json_text_get_key(json: &str, key: &str) -> Result<Option<String>> {
    get_one(json, Some(key), None)
}

/// json `->` int: raw text of an element of a top-level array (negative
/// indexes count from the end).
pub fn json_text_get_index(json: &str, idx: i64) -> Result<Option<String>> {
    get_one(json, None, Some(idx))
}

/// get_worker() with an empty path and result normalization (as used by
/// `json #>> '{}'`).
struct AsText {
    result_start: Option<usize>,
    tresult: Option<String>,
    is_null: bool,
}

impl<'a> Sem<'a> for AsText {
    fn object_start(&mut self, lex: &Lexer<'a>) -> Result<()> {
        if lex.lex_level == 0 {
            self.result_start = lex.token_start;
        }
        Ok(())
    }
    fn object_end(&mut self, lex: &Lexer<'a>) -> Result<()> {
        if lex.lex_level == 0 {
            let start = self.result_start.unwrap_or(0);
            self.tresult = Some(lex.src[start..lex.prev_token_terminator].to_owned());
        }
        Ok(())
    }
    fn array_start(&mut self, lex: &Lexer<'a>) -> Result<()> {
        self.object_start(lex)
    }
    fn array_end(&mut self, lex: &Lexer<'a>) -> Result<()> {
        self.object_end(lex)
    }
    fn scalar(&mut self, lex: &Lexer<'a>, val: Scalar<'a>, tok: Tok) -> Result<()> {
        if lex.lex_level == 0 {
            match (tok, val) {
                (Tok::String, Scalar::Str(s)) => self.tresult = Some(s),
                (Tok::Null, _) => {
                    self.tresult = None;
                    self.is_null = true;
                }
                _ => self.tresult = Some(lex.src[..lex.prev_token_terminator].to_owned()),
            }
        }
        Ok(())
    }
}

/// json `->>` on a value obtained with [`json_text_get_key`] /
/// [`json_text_get_index`]: strings are de-escaped, JSON null -> None, other
/// values are returned as their json text. (For a whole document this is
/// `json #>> '{}'`, including its whitespace quirks.)
pub fn json_text_as_text(raw_json_value: &str) -> Result<Option<String>> {
    let mut lex = Lexer::new(raw_json_value, true);
    let mut st = AsText {
        result_start: None,
        tresult: None,
        is_null: false,
    };
    parser::parse_json(&mut lex, &mut st, parser::BUDGET_JSON_GET)?;
    if st.is_null {
        return Ok(None);
    }
    Ok(st.tresult)
}
