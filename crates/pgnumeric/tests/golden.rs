//! Golden tests: expected results generated from a real PostgreSQL 17
//! server by `tests/gen_golden.py` and stored in `tests/golden/*.tsv`.

use pgnumeric::float::{format_f32, format_f64, parse_f32, parse_f64};
use pgnumeric::{Error, Numeric};

type Outcome = Result<String, Error>;

fn unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut it = s.chars();
    while let Some(c) = it.next() {
        if c == '\\' {
            match it.next() {
                Some('t') => out.push('\t'),
                Some('n') => out.push('\n'),
                Some('r') => out.push('\r'),
                Some('\\') => out.push('\\'),
                Some(o) => {
                    out.push('\\');
                    out.push(o);
                }
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Expand the compact repeat notation `<c*n>` used for huge inputs.
fn expand(s: &str) -> String {
    let mut out = String::new();
    let mut rest = s;
    while let Some(start) = rest.find('<') {
        let after = &rest[start + 1..];
        let mut chars = after.chars();
        let c = chars.next();
        let parsed = (|| {
            let c = c?;
            let tail = &after[c.len_utf8()..];
            let tail = tail.strip_prefix('*')?;
            let end = tail.find('>')?;
            let n: usize = tail[..end].parse().ok()?;
            Some((c, n, c.len_utf8() + 1 + end + 1))
        })();
        match parsed {
            Some((c, n, consumed)) => {
                out.push_str(&rest[..start]);
                for _ in 0..n {
                    out.push(c);
                }
                rest = &rest[start + 1 + consumed..];
            }
            None => {
                out.push_str(&rest[..start + 1]);
                rest = &rest[start + 1..];
            }
        }
    }
    out.push_str(rest);
    out
}

fn n(s: &str) -> Result<Numeric, Error> {
    Numeric::parse(s)
}

fn show(r: Result<Numeric, Error>) -> Outcome {
    r.map(|v| v.to_string())
}

fn int_arg(s: &str) -> i32 {
    s.parse().expect("integer argument")
}

fn eval(op: &str, a: &str, b: &str, c: &str) -> Outcome {
    match op {
        "in" => show(n(a)),
        "add" => show(n(a)?.add(&n(b)?)),
        "sub" => show(n(a)?.sub(&n(b)?)),
        "mul" => show(n(a)?.mul(&n(b)?)),
        "div" => show(n(a)?.div(&n(b)?)),
        "div_trunc" => show(n(a)?.div_trunc(&n(b)?)),
        "mod" => show(n(a)?.rem(&n(b)?)),
        "cmp" => {
            let o = n(a)?.cmp(&n(b)?);
            Ok((o as i32).to_string())
        }
        "round" => show(n(a)?.round(int_arg(b))),
        "trunc" => show(n(a)?.trunc(int_arg(b))),
        "ceil" => show(n(a)?.try_ceil()),
        "floor" => show(n(a)?.try_floor()),
        "abs" => show(Ok(n(a)?.abs())),
        "neg" => show(Ok(n(a)?.neg())),
        "sign" => show(Ok(n(a)?.sign())),
        "scale" => Ok(n(a)?
            .scale()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "<NULL>".into())),
        "min_scale" => Ok(n(a)?
            .min_scale()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "<NULL>".into())),
        "trim_scale" => show(Ok(n(a)?.normalize())),
        "sqrt" => show(n(a)?.sqrt()),
        "exp" => show(n(a)?.exp()),
        "ln" => show(n(a)?.ln()),
        "log10" => show(n(a)?.log10()),
        "log" => show(n(a)?.log(&n(b)?)),
        "pow" => show(n(a)?.pow(&n(b)?)),
        "gcd" => show(n(a)?.gcd(&n(b)?)),
        "lcm" => show(n(a)?.lcm(&n(b)?)),
        "fac" => show(Numeric::factorial(a.parse().unwrap())),
        "int8" => n(a)?.to_i64().map(|v| v.to_string()),
        "int4" => n(a)?.to_i32().map(|v| v.to_string()),
        "int2" => n(a)?.to_i16().map(|v| v.to_string()),
        "float8" => n(a)?.to_f64().map(format_f64),
        "float4" => n(a)?.to_f32().map(format_f32),
        "f8in" => parse_f64(a).map(format_f64),
        "f8num" => show(Numeric::from_f64(parse_f64(a)?)),
        "f4in" => parse_f32(a).map(format_f32),
        "f4num" => show(Numeric::from_f32(parse_f32(a)?)),
        "typmod" => show(n(a)?.apply_typmod(int_arg(b), int_arg(c))),
        "send" => {
            let bytes = n(a)?.to_pg_binary();
            let mut s = String::from("\\x");
            for x in bytes {
                s.push_str(&format!("{:02x}", x));
            }
            Ok(s)
        }
        "in_typmod" => show(Numeric::parse_with_typmod(a, int_arg(b), int_arg(c))),
        "recv_typmod" => {
            let bytes: Vec<u8> = (0..a.len() / 2)
                .map(|i| u8::from_str_radix(&a[2 * i..2 * i + 2], 16).unwrap())
                .collect();
            show(Numeric::from_pg_binary_with_typmod(
                &bytes,
                int_arg(b),
                int_arg(c),
            ))
        }
        "recv" => {
            let bytes: Vec<u8> = (0..a.len() / 2)
                .map(|i| u8::from_str_radix(&a[2 * i..2 * i + 2], 16).unwrap())
                .collect();
            show(Numeric::from_pg_binary(&bytes))
        }
        _ => panic!("unknown op {op}"),
    }
}

fn run_file(name: &str) {
    let path = format!("{}/tests/golden/{}.tsv", env!("CARGO_MANIFEST_DIR"), name);
    run_path(name, &path);
}

fn run_path(name: &str, path: &str) {
    let data = std::fs::read_to_string(path).expect("golden file");
    let mut failures = Vec::new();
    let mut count = 0;
    for (lineno, line) in data.lines().enumerate() {
        if line.is_empty() {
            continue;
        }
        let f: Vec<String> = line.split('\t').map(unescape).collect();
        let (op, a, b, c) = (f[0].as_str(), expand(&f[1]), expand(&f[2]), expand(&f[3]));
        let got = eval(op, &a, &b, &c);
        count += 1;
        let ok = match f[4].as_str() {
            "ok" => matches!(&got, Ok(s) if *s == f[5]),
            "err" => match &got {
                Err(e) => {
                    let detail = e.detail.clone().unwrap_or_default();
                    e.sqlstate == f[5] && e.message == f[6] && detail == f[7]
                }
                Ok(_) => false,
            },
            k => panic!("bad kind {k}"),
        };
        if !ok {
            let short = |s: &str| {
                if s.len() > 200 {
                    format!("{}...({} chars)", &s[..200], s.len())
                } else {
                    s.to_string()
                }
            };
            failures.push(format!(
                "{}:{}: {}({}, {}, {}): expected {} got {}",
                name,
                lineno + 1,
                op,
                short(&f[1]),
                short(&f[2]),
                short(&f[3]),
                short(&f[4..].join(" | ")),
                short(&match &got {
                    Ok(s) => format!("ok | {}", s),
                    Err(e) => format!(
                        "err | {} | {} | {}",
                        e.sqlstate,
                        e.message,
                        e.detail.clone().unwrap_or_default()
                    ),
                })
            ));
        }
    }
    if !failures.is_empty() {
        for f in failures.iter().take(60) {
            eprintln!("{}", f);
        }
        panic!(
            "{}: {} of {} golden cases failed",
            name,
            failures.len(),
            count
        );
    }
    eprintln!("{}: {} cases passed", name, count);
}

#[test]
fn golden_in() {
    run_file("in");
}

#[test]
fn golden_arith() {
    run_file("arith");
}

#[test]
fn golden_unary() {
    run_file("unary");
}

#[test]
fn golden_math() {
    run_file("math");
}

#[test]
fn golden_casts() {
    run_file("casts");
}

#[test]
fn golden_typmod() {
    run_file("typmod");
}

#[test]
fn golden_send() {
    run_file("send");
}

#[test]
fn golden_recv() {
    run_file("recv");
}

#[test]
fn golden_in_typmod() {
    run_file("in_typmod");
}

#[test]
fn golden_precision() {
    run_file("precision");
}

/// Runs extra golden files listed (colon separated) in the environment
/// variable PGNUMERIC_EXTRA_GOLDEN, e.g. large randomized differential runs
/// produced with gen_golden.py that are not checked in.
#[test]
fn golden_external() {
    if let Ok(paths) = std::env::var("PGNUMERIC_EXTRA_GOLDEN") {
        for p in paths.split(':').filter(|p| !p.is_empty()) {
            run_path(p, p);
        }
    }
}
