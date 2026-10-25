//! Shared helpers for golden tests (text/pattern). Golden files are JSON lines
//! written by tests/gen/pgeval.py: {"fn", "args", "result"} or {"fn", "args",
//! "error": {"sqlstate", "message", "detail"}}.
#![allow(dead_code)]

use serde_json::Value;

pub struct Golden {
    pub name: String,
    pub records: Vec<Value>,
    pub failures: Vec<String>,
    pub checked: usize,
}

impl Golden {
    pub fn load(file: &str) -> Golden {
        let path = format!("{}/tests/data/{}", env!("CARGO_MANIFEST_DIR"), file);
        Golden::load_path(&path)
    }

    pub fn load_path(path: &str) -> Golden {
        let file = path.rsplit('/').next().unwrap_or(path);
        let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{path}: {e}"));
        let records = text
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        Golden { name: file.to_string(), records, failures: Vec::new(), checked: 0 }
    }

    /// Runs `f(fn_name, args)` for every record and compares with the
    /// expected result (`Ok(None)` = SQL NULL).
    pub fn run(&mut self, mut f: impl FnMut(&str, &[Value]) -> pgfuncs::Result<Option<String>>) {
        let records = std::mem::take(&mut self.records);
        for rec in &records {
            let fn_name = rec["fn"].as_str().unwrap();
            let args = rec["args"].as_array().unwrap();
            let got = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(fn_name, args)));
            let got = match got {
                Ok(g) => g,
                Err(p) => {
                    let msg = p
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                        .unwrap_or_default();
                    self.failures.push(format!("{rec}\n    PANIC: {msg}"));
                    continue;
                }
            };
            self.check(rec, got);
        }
        self.records = records;
    }

    pub fn check(&mut self, rec: &Value, got: pgfuncs::Result<Option<String>>) {
        self.checked += 1;
        if let Some(err) = rec.get("error") {
            let want_state = err["sqlstate"].as_str().unwrap();
            let want_msg = err["message"].as_str().unwrap();
            let want_detail = err.get("detail").and_then(|d| d.as_str());
            match got {
                Err(e) => {
                    let detail_ok = match want_detail {
                        Some(d) => e.detail.as_deref() == Some(d),
                        None => e.detail.is_none(),
                    };
                    if e.sqlstate != want_state || e.message != want_msg || !detail_ok {
                        self.failures.push(format!(
                            "{rec}\n    got error: {} {:?} detail={:?}",
                            e.sqlstate, e.message, e.detail
                        ));
                    }
                }
                Ok(v) => self.failures.push(format!("{rec}\n    got OK: {v:?}")),
            }
        } else {
            let want = rec["result"].as_str();
            match got {
                Ok(v) => {
                    if v.as_deref() != want {
                        self.failures.push(format!("{rec}\n    got: {v:?}"));
                    }
                }
                Err(e) => self.failures.push(format!("{rec}\n    got error: {} {:?}", e.sqlstate, e.message)),
            }
        }
    }

    pub fn finish(&self) {
        if !self.failures.is_empty() {
            let n = self.failures.len();
            let shown: Vec<&String> = self.failures.iter().take(60).collect();
            panic!(
                "{}: {} of {} cases failed:\n{}",
                self.name,
                n,
                self.checked,
                shown.iter().map(|s| s.as_str()).collect::<Vec<_>>().join("\n")
            );
        }
        eprintln!("{}: {} cases OK", self.name, self.checked);
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
    let mut out = String::from("[");
    for (k, e) in v.iter().enumerate() {
        if k > 0 {
            out.push(',');
        }
        match e {
            None => out.push_str("null"),
            Some(s) => out.push_str(&json_escape(s)),
        }
    }
    out.push(']');
    out
}

/// PostgreSQL escape_json.
pub fn json_escape(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
