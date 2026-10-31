//! Shared helpers for golden tests. Golden files are JSON lines
//! written by tests/gen/pgeval.py: {"fn", "args", "result"} or {"fn", "args",
//! "error": {"sqlstate", "message", "detail"}}.
#![allow(dead_code)]

use serde_json::Value;

#[path = "../../../../tests/support/golden.rs"]
mod golden;
pub type Golden = golden::Golden<pgfuncs::Error>;

impl golden::GoldenError for pgfuncs::Error {
    fn fields(&self) -> (&str, &str, Option<&str>) {
        (self.sqlstate, &self.message, self.detail.as_deref())
    }
}

pub fn s(v: &Value) -> &str {
    v.as_str().unwrap_or_else(|| panic!("expected string, got {v}"))
}

pub fn opt_s(v: &Value) -> Option<&str> {
    v.as_str()
}

pub fn i(v: &Value) -> i64 {
    v.as_i64().unwrap_or_else(|| panic!("expected int, got {v}"))
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

pub fn unhex(s: &str) -> Vec<u8> {
    (0..s.len() / 2).map(|k| u8::from_str_radix(&s[2 * k..2 * k + 2], 16).unwrap()).collect()
}

/// Renders like PostgreSQL's `to_json(text[])::text` for a 1-D array.
pub fn json_text_array(v: &[Option<String>]) -> String {
    serde_json::to_string(v).unwrap()
}

pub fn json_escape(s: &str) -> String {
    serde_json::to_string(s).unwrap()
}
