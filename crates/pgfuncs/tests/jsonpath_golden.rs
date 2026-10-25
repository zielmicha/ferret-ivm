//! Golden tests for pgfuncs::json::path (jsonpath) against results recorded
//! from PostgreSQL 17 (see tests/gen/gen_jsonpath.py).

use pgfuncs::Error;
use pgfuncs::json::Jsonb;
use pgfuncs::json::path::{self, JsonPath, PathContext, SessionTz};
use serde_json::Value;

type Res = Result<Option<String>, Error>;

fn s(v: &Value) -> &str {
    v.as_str()
        .unwrap_or_else(|| panic!("expected string arg, got {v}"))
}

fn show_bool(b: Option<bool>) -> Option<String> {
    b.map(|b| b.to_string())
}

/// pre + rep * n + mid + post * n
fn repeated(a: &[Value]) -> String {
    let n = a[2].as_u64().unwrap() as usize;
    format!(
        "{}{}{}{}",
        s(&a[0]),
        s(&a[1]).repeat(n),
        s(&a[3]),
        s(&a[4]).repeat(n)
    )
}

fn eval(fname: &str, a: &[Value], session: &SessionTz) -> Res {
    match fname {
        "in" => {
            return JsonPath::parse(s(&a[0]))
                .and_then(|p| p.to_string_checked())
                .map(Some);
        }
        "valid" => return JsonPath::parse(s(&a[0])).map(|_| Some("true".to_owned())),
        "in_rep" => {
            return JsonPath::parse(&repeated(a))
                .and_then(|p| p.to_string_checked())
                .map(|t| {
                    Some(format!(
                        "{}:{}",
                        pgfuncs::text::md5_hex(t.as_bytes()),
                        t.chars().count()
                    ))
                });
        }
        "valid_rep" => return JsonPath::parse(&repeated(a)).map(|_| Some("true".to_owned())),
        "exec_rep" => {
            let n = a[3].as_u64().unwrap() as usize;
            let build = |spec: &Value| {
                let p: Vec<&str> = spec.as_array().unwrap().iter().map(s).collect();
                format!(
                    "{}{}{}{}{}",
                    p[0],
                    p[1].repeat(n),
                    p[2],
                    p[3].repeat(n),
                    p[4]
                )
            };
            let doc = Jsonb::parse(&build(&a[1]))?;
            let jp = JsonPath::parse(&build(&a[2]))?;
            let vars = Jsonb::parse("{}").unwrap();
            return match s(&a[0]) {
                "query_array" => {
                    path::path_query_array(&doc, &jp, &vars, false).map(|v| Some(v.to_string()))
                }
                "exists" => path::path_exists(&doc, &jp, &vars, false).map(show_bool),
                _ => path::path_match(&doc, &jp, &vars, false).map(show_bool),
            };
        }
        _ => {}
    }
    let (base, use_tz) = match fname.strip_suffix("_tz") {
        Some(b) => (b, true),
        None => (fname, false),
    };
    let cx = PathContext::new(session, use_tz);
    let doc = Jsonb::parse(s(&a[0]))?;
    let jp = JsonPath::parse(s(&a[1]))?;
    if base == "exists_op" {
        return cx.exists_op(&doc, &jp).map(show_bool);
    }
    if base == "match_op" {
        return cx.match_op(&doc, &jp).map(show_bool);
    }
    let vars = Jsonb::parse(s(&a[2]))?;
    let silent = a[3].as_bool().unwrap();
    match base {
        "query" => cx
            .query(&doc, &jp, &vars, silent)
            .map(|v| Some(Jsonb::Array(v).to_string())),
        "query_array" => cx
            .query_array(&doc, &jp, &vars, silent)
            .map(|v| Some(v.to_string())),
        "query_first" => cx
            .query_first(&doc, &jp, &vars, silent)
            .map(|v| v.map(|v| v.to_string())),
        "exists" => cx.exists(&doc, &jp, &vars, silent).map(show_bool),
        "match" => cx.matches(&doc, &jp, &vars, silent).map(show_bool),
        other => panic!("unknown fn {other}"),
    }
}

fn show(r: &Res) -> String {
    match r {
        Ok(Some(v)) => {
            let mut t: String = v.chars().take(300).collect();
            if v.len() > 300 {
                t.push_str("...");
            }
            format!("{t:?}")
        }
        Ok(None) => "NULL".to_owned(),
        Err(e) => format!("ERROR {} {:?} detail={:?}", e.sqlstate, e.message, e.detail),
    }
}

fn line_args(args: &[Value]) -> String {
    let s = serde_json::to_string(args).unwrap();
    if s.chars().count() > 300 {
        format!("{}...", s.chars().take(300).collect::<String>())
    } else {
        s
    }
}

#[test]
fn jsonpath_golden() {
    // JSONPATH_GOLDEN_FILE runs another data file in the same format (e.g.
    // the output of tests/gen/fuzz_jsonpath.py).
    let file = std::env::var("JSONPATH_GOLDEN_FILE").unwrap_or_else(|_| {
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/data/jsonpath_golden.jsonl"
        )
        .to_owned()
    });
    let data = std::fs::read_to_string(&file).unwrap();
    let mut failures = Vec::new();
    let mut total = 0;
    for line in data.lines() {
        let rec: Value = serde_json::from_str(line).unwrap();
        let fname = rec["fn"].as_str().unwrap();
        let args = rec["args"].as_array().unwrap();
        total += 1;
        let expected: Res = if let Some(e) = rec.get("error") {
            Err(Error {
                sqlstate: Box::leak(e["sqlstate"].as_str().unwrap().to_owned().into_boxed_str()),
                message: e["message"].as_str().unwrap().to_owned(),
                detail: e["detail"].as_str().map(str::to_owned),
            })
        } else {
            Ok(rec["result"].as_str().map(str::to_owned))
        };
        let session = SessionTz::new(
            pgdatetime::TimeZone::parse_session(
                rec.get("tz").and_then(Value::as_str).unwrap_or("UTC"),
            )
            .unwrap(),
            rec.get("today").and_then(Value::as_i64).unwrap_or(0) as i32,
        );
        let got = std::panic::catch_unwind(|| eval(fname, args, &session));
        let ok = match &got {
            Ok(g) => *g == expected,
            Err(_) => false,
        };
        if !ok {
            let got_s = match &got {
                Ok(g) => show(g),
                Err(_) => "PANIC".to_owned(),
            };
            failures.push(format!(
                "{fname}({}):\n   expected {}\n   got      {}",
                line_args(args),
                show(&expected),
                got_s
            ));
        }
    }
    if let Ok(dump) = std::env::var("JSONPATH_GOLDEN_DUMP") {
        std::fs::write(dump, failures.join("\n")).unwrap();
    }
    for f in failures.iter().take(80) {
        eprintln!("{f}");
    }
    assert!(
        failures.is_empty(),
        "{} of {} golden cases failed",
        failures.len(),
        total
    );
    eprintln!("{total} golden cases passed");
}

/// Prints the deepest accepted level of several recursion patterns (compare
/// with PostgreSQL to calibrate the stack-depth emulation):
/// `cargo test --test jsonpath_golden calibrate_stack -- --ignored --nocapture`
#[test]
#[ignore]
fn calibrate_stack() {
    fn nest_obj(n: usize, key: &str) -> String {
        format!("{}1{}", format!("{{\"{key}\":").repeat(n), "}".repeat(n))
    }
    fn nest_arr(n: usize) -> String {
        format!("{}1{}", "[".repeat(n), "]".repeat(n))
    }
    fn nest_arrobj(n: usize) -> String {
        format!("{}1{}", "[{\"a\":".repeat(n), "}]".repeat(n))
    }
    fn filt_nest(n: usize) -> String {
        let mut inner = "@ == 1".to_owned();
        for _ in 0..n {
            inner = format!("@ ? ({inner}) == 1");
        }
        format!("$ ? ({inner})")
    }
    fn gai_nest(n: usize) -> String {
        let mut p = "0".to_owned();
        for _ in 0..n {
            p = format!("$[{p}]");
        }
        p
    }
    type B = fn(usize) -> String;
    let patterns: Vec<(&str, B, B, &str)> = vec![
        (
            "key_chain",
            |n| nest_obj(n, "a"),
            |n| format!("${}", ".a".repeat(n)),
            "query",
        ),
        (
            "idx_chain",
            nest_arr,
            |n| format!("${}", "[0]".repeat(n)),
            "query",
        ),
        (
            "anyarr_chain",
            nest_arr,
            |n| format!("${}", "[*]".repeat(n)),
            "query",
        ),
        (
            "anykey_chain",
            |n| nest_obj(n, "a"),
            |n| format!("${}", ".*".repeat(n)),
            "query",
        ),
        (
            "any_filter",
            nest_arr,
            |_| "$.** ? (@ == 2)".to_owned(),
            "query",
        ),
        (
            "any_strict_exists",
            nest_arr,
            |_| "strict $.**".to_owned(),
            "exists",
        ),
        (
            "unwrap_key_chain",
            nest_arrobj,
            |n| format!("${}", ".a".repeat(n)),
            "query",
        ),
        (
            "arith_chain",
            |_| "1".to_owned(),
            |n| format!("1{}", " + 1".repeat(n)),
            "query",
        ),
        (
            "and_chain",
            |_| "1".to_owned(),
            |n| format!("$ == 1{}", " && $ == 1".repeat(n)),
            "match",
        ),
        (
            "or_chain",
            |_| "1".to_owned(),
            |n| format!("$ == 2{}", " || $ == 2".repeat(n)),
            "match",
        ),
        ("filter_nest", |_| "1".to_owned(), filt_nest, "query"),
        (
            "kv_chain",
            |n| nest_obj(n, "value"),
            |n| format!("${}", ".keyvalue().value".repeat(n)),
            "query",
        ),
        (
            "abs_chain",
            |_| "-1".to_owned(),
            |n| format!("${}", ".abs()".repeat(n)),
            "query",
        ),
        (
            "decimal_chain",
            |_| "1".to_owned(),
            |n| format!("${}", ".decimal()".repeat(n)),
            "query",
        ),
        ("gai_nest", |_| "[0]".to_owned(), gai_nest, "query"),
        (
            "current_chain",
            |n| nest_obj(n, "a"),
            |n| format!("$ ? (@{} == 1)", ".a".repeat(n)),
            "query",
        ),
        (
            "strict_key_chain",
            |n| nest_obj(n, "a"),
            |n| format!("strict ${}", ".a".repeat(n)),
            "query",
        ),
        (
            "exists_key_chain",
            |n| nest_obj(n, "a"),
            |n| format!("${}", ".a".repeat(n)),
            "exists",
        ),
    ];
    let filter: Vec<String> = std::env::args().skip_while(|a| a != "--").skip(1).collect();
    for (name, doc, path, func) in patterns {
        let run = |n: usize| -> Result<(), String> {
            let d = Jsonb::parse(&doc(n)).map_err(|e| e.message)?;
            let p = JsonPath::parse(&path(n)).map_err(|e| e.message)?;
            let vars = Jsonb::parse("{}").unwrap();
            let r = match func {
                "query" => path::path_query_array(&d, &p, &vars, false).map(|_| ()),
                "exists" => path::path_exists(&d, &p, &vars, false).map(|_| ()),
                _ => path::path_match(&d, &p, &vars, false).map(|_| ()),
            };
            r.map_err(|e| e.message)
        };
        let _ = &filter;
        let (mut lo, mut hi) = (1usize, 16000usize);
        if run(hi).is_ok() {
            eprintln!("{name} >= {hi}");
            continue;
        }
        while lo < hi {
            let mid = (lo + hi).div_ceil(2);
            if run(mid).is_ok() {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        eprintln!("{name} {lo} {}", run(lo + 1).unwrap_err());
    }
}
