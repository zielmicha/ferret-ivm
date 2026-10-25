//! Golden tests for `to_char` / `to_number`: expected results generated from
//! a real PostgreSQL 17 server by `tests/gen_tochar.py` and stored in
//! `tests/golden/tochar.jsonl`.
//!
//! Each line is `{"fn": F, "args": [value, format], "result": text|null}` or
//! `{"fn": F, "args": [value, format], "error": {"sqlstate", "message",
//! "detail"}}`, where F is the SQL argument type of `to_char` or
//! "to_number".

use pgnumeric::float::{parse_f32, parse_f64};
use pgnumeric::{
    Error, Numeric, to_char_float4, to_char_float8, to_char_int2, to_char_int4, to_char_int8,
    to_char_numeric, to_number,
};

/// Minimal JSON reader for the golden files (objects, arrays, strings,
/// null; no numbers or booleans are needed).
mod json {
    #[derive(Debug, Clone, PartialEq)]
    pub enum Value {
        Null,
        Str(String),
        Arr(Vec<Value>),
        Obj(Vec<(String, Value)>),
    }

    impl Value {
        pub fn get(&self, key: &str) -> Option<&Value> {
            match self {
                Value::Obj(fields) => fields.iter().find(|(k, _)| k == key).map(|(_, v)| v),
                _ => None,
            }
        }

        pub fn str(&self) -> Option<&str> {
            match self {
                Value::Str(s) => Some(s),
                _ => None,
            }
        }

        pub fn arr(&self) -> &[Value] {
            match self {
                Value::Arr(a) => a,
                _ => panic!("expected array, got {self:?}"),
            }
        }
    }

    pub fn parse(s: &str) -> Value {
        let mut p = Parser {
            b: s.as_bytes(),
            i: 0,
        };
        let v = p.value();
        p.ws();
        assert_eq!(p.i, p.b.len(), "trailing data in {s}");
        v
    }

    struct Parser<'a> {
        b: &'a [u8],
        i: usize,
    }

    impl Parser<'_> {
        fn ws(&mut self) {
            while self.i < self.b.len() && matches!(self.b[self.i], b' ' | b'\t' | b'\n' | b'\r') {
                self.i += 1;
            }
        }

        fn eat(&mut self, c: u8) {
            self.ws();
            assert_eq!(
                self.b.get(self.i),
                Some(&c),
                "expected {:?} at {}",
                c as char,
                self.i
            );
            self.i += 1;
        }

        fn value(&mut self) -> Value {
            self.ws();
            match self.b[self.i] {
                b'"' => Value::Str(self.string()),
                b'n' => {
                    assert!(self.b[self.i..].starts_with(b"null"));
                    self.i += 4;
                    Value::Null
                }
                b'[' => {
                    self.i += 1;
                    let mut items = Vec::new();
                    self.ws();
                    if self.b[self.i] == b']' {
                        self.i += 1;
                        return Value::Arr(items);
                    }
                    loop {
                        items.push(self.value());
                        self.ws();
                        let c = self.b[self.i];
                        self.i += 1;
                        match c {
                            b',' => continue,
                            b']' => return Value::Arr(items),
                            _ => panic!("bad array at {}", self.i),
                        }
                    }
                }
                b'{' => {
                    self.i += 1;
                    let mut fields = Vec::new();
                    self.ws();
                    if self.b[self.i] == b'}' {
                        self.i += 1;
                        return Value::Obj(fields);
                    }
                    loop {
                        self.ws();
                        let k = self.string();
                        self.eat(b':');
                        let v = self.value();
                        fields.push((k, v));
                        self.ws();
                        let c = self.b[self.i];
                        self.i += 1;
                        match c {
                            b',' => continue,
                            b'}' => return Value::Obj(fields),
                            _ => panic!("bad object at {}", self.i),
                        }
                    }
                }
                c => panic!("unexpected {:?} at {}", c as char, self.i),
            }
        }

        fn hex4(&mut self) -> u32 {
            let h = std::str::from_utf8(&self.b[self.i..self.i + 4]).unwrap();
            self.i += 4;
            u32::from_str_radix(h, 16).unwrap()
        }

        fn string(&mut self) -> String {
            assert_eq!(self.b[self.i], b'"');
            self.i += 1;
            let mut out = Vec::new();
            loop {
                let c = self.b[self.i];
                self.i += 1;
                match c {
                    b'"' => return String::from_utf8(out).unwrap(),
                    b'\\' => {
                        let e = self.b[self.i];
                        self.i += 1;
                        let ch = match e {
                            b'"' => '"',
                            b'\\' => '\\',
                            b'/' => '/',
                            b'b' => '\u{8}',
                            b'f' => '\u{c}',
                            b'n' => '\n',
                            b'r' => '\r',
                            b't' => '\t',
                            b'u' => {
                                let mut cp = self.hex4();
                                if (0xD800..0xDC00).contains(&cp) {
                                    assert_eq!(&self.b[self.i..self.i + 2], b"\\u");
                                    self.i += 2;
                                    let lo = self.hex4();
                                    cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                                }
                                char::from_u32(cp).unwrap()
                            }
                            _ => panic!("bad escape"),
                        };
                        let mut buf = [0u8; 4];
                        out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                    }
                    _ => out.push(c),
                }
            }
        }
    }
}

type Outcome = Result<Option<String>, Error>;

fn eval(f: &str, value: &str, fmt: &str) -> Outcome {
    let some = |r: Result<String, Error>| r.map(Some);
    match f {
        "numeric" => some(to_char_numeric(&Numeric::parse(value)?, fmt)),
        "int8" => some(to_char_int8(value.parse().expect("int8"), fmt)),
        "int4" => some(to_char_int4(value.parse().expect("int4"), fmt)),
        "int2" => some(to_char_int2(value.parse().expect("int2"), fmt)),
        "float8" => some(to_char_float8(parse_f64(value)?, fmt)),
        "float4" => some(to_char_float4(parse_f32(value)?, fmt)),
        "to_number" => to_number(value, fmt).map(|r| r.map(|n| n.to_string())),
        _ => panic!("unknown fn {f}"),
    }
}

fn run_path(path: &str) {
    let data = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{path}: {e}"));
    let mut failures = Vec::new();
    let mut count = 0;
    for (lineno, line) in data.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let rec = json::parse(line);
        let f = rec.get("fn").and_then(|v| v.str()).expect("fn");
        let args = rec.get("args").expect("args").arr();
        let (value, fmt) = (
            args[0].str().expect("value"),
            args[1].str().expect("format"),
        );
        let got = std::panic::catch_unwind(|| eval(f, value, fmt));
        count += 1;
        let got = match got {
            Ok(g) => g,
            Err(_) => {
                failures.push(format!("{}:{}: {line}\n    PANIC", path, lineno + 1));
                continue;
            }
        };
        let ok = match rec.get("error") {
            Some(e) => match &got {
                Err(g) => {
                    g.sqlstate == e.get("sqlstate").and_then(|v| v.str()).unwrap()
                        && g.message == e.get("message").and_then(|v| v.str()).unwrap()
                        && g.detail.as_deref() == e.get("detail").and_then(|v| v.str())
                }
                Ok(_) => false,
            },
            None => {
                let want = rec.get("result").expect("result").str();
                matches!(&got, Ok(r) if r.as_deref() == want)
            }
        };
        if !ok {
            let shown = match &got {
                Ok(r) => format!("{r:?}"),
                Err(e) => format!("error {} {:?} detail={:?}", e.sqlstate, e.message, e.detail),
            };
            failures.push(format!("{}:{}: {line}\n    got {shown}", path, lineno + 1));
        }
    }
    if !failures.is_empty() {
        for f in failures.iter().take(50) {
            eprintln!("{f}");
        }
        panic!("{path}: {} of {count} golden cases failed", failures.len());
    }
    eprintln!("{path}: {count} cases passed");
}

#[test]
fn golden_tochar() {
    run_path(&format!(
        "{}/tests/golden/tochar.jsonl",
        env!("CARGO_MANIFEST_DIR")
    ));
}

/// Runs extra golden files listed (colon separated) in the environment
/// variable PGNUMERIC_EXTRA_TOCHAR, e.g. large randomized runs produced by
/// `gen_tochar.py --seed N --scale K --out FILE` that are not checked in.
#[test]
fn golden_tochar_external() {
    if let Ok(paths) = std::env::var("PGNUMERIC_EXTRA_TOCHAR") {
        for p in paths.split(':').filter(|p| !p.is_empty()) {
            run_path(p);
        }
    }
}
