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
        let rec: serde_json::Value = serde_json::from_str(line).expect("golden JSON");
        let f = rec.get("fn").and_then(|v| v.as_str()).expect("fn");
        let args = rec.get("args").expect("args").as_array().expect("array");
        let (value, fmt) = (
            args[0].as_str().expect("value"),
            args[1].as_str().expect("format"),
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
                    g.sqlstate == e.get("sqlstate").and_then(|v| v.as_str()).unwrap()
                        && g.message == e.get("message").and_then(|v| v.as_str()).unwrap()
                        && g.detail.as_deref() == e.get("detail").and_then(|v| v.as_str())
                }
                Ok(_) => false,
            },
            None => {
                let want = rec.get("result").expect("result").as_str();
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
