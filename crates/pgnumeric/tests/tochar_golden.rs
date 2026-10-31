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

#[path = "../../../tests/support/golden.rs"]
mod golden;

impl golden::GoldenError for Error {
    fn fields(&self) -> (&str, &str, Option<&str>) {
        (self.sqlstate, &self.message, self.detail.as_deref())
    }
}

fn run_path(path: &str) {
    golden::Golden::run_path(path, |f, args| eval(f, args[0].as_str().unwrap(), args[1].as_str().unwrap()));
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
