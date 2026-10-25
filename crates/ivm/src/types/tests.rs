//! Property tests of value encodings over random values of every SQL type
//! (NULLs, NaN in several bit patterns, infinities, -0.0, numerics in several
//! scales, intervals with equal spans but different fields, jsonb with
//! numbers of different scales, multi-dimensional arrays with non-default
//! lower bounds and NULL elements):
//! - the key encoding orders like `Datum::sql_cmp` in every direction and
//!   NULL placement, also as composite keys; it is prefix-free;
//! - canonical decoding re-encodes to the same bytes, and with the fidelity
//!   suffix restores the exact value; key plus suffix tell two values apart
//!   exactly when `exact_cmp` does;
//! - the row codec, text I/O and binary send/receive round trip exactly.
//!
//! `IVM_TEST_SEED=N cargo test -p ivm types::tests` reproduces a seed.
//! Long variant: `cargo test -p ivm --release types::tests -- --ignored`.

use super::codec::{decode_datum, decode_row, encode_datum, encode_row};
use super::datum::{exact_cmp, ArrayValue, Datum};
use super::encoding::*;
use super::text::{format_datum, parse_datum, recv_datum, send_datum};
use super::SqlType;
use crate::testkit::{run_seeds, seeds, Rng};
use pgdatetime::Interval;
use std::cmp::Ordering;

const SCALARS: &[SqlType] = &[
    SqlType::Bool,
    SqlType::Int2,
    SqlType::Int4,
    SqlType::Int8,
    SqlType::Oid,
    SqlType::Float4,
    SqlType::Float8,
    SqlType::Numeric,
    SqlType::Text,
    SqlType::Bytea,
    SqlType::Date,
    SqlType::Time,
    SqlType::Timestamp,
    SqlType::TimestampTz,
    SqlType::Interval,
    SqlType::Uuid,
    SqlType::Json,
    SqlType::Jsonb,
];

fn gen_type(rng: &mut Rng) -> SqlType {
    let t = rng.pick(SCALARS).clone();
    if rng.chance(25) { SqlType::Array(Box::new(t)) } else { t }
}

fn gen_numeric(rng: &mut Rng) -> pgnumeric::Numeric {
    let s = match rng.below(12) {
        0 => ["NaN", "Infinity", "-Infinity"][rng.index(3)].to_string(),
        1 => format!("{}0.{}", ["", "-"][rng.index(2)], "0".repeat(rng.index(6))),
        2 => format!("{}e{}", rng.range(1, 999), rng.range(-40, 40)),
        _ => {
            let int: String = (0..rng.index(22)).map(|_| char::from(b'0' + rng.below(10) as u8)).collect();
            let frac: String = (0..rng.index(22)).map(|_| char::from(b'0' + rng.below(10) as u8)).collect();
            let sign = if rng.chance(40) { "-" } else { "" };
            match (int.is_empty(), frac.is_empty()) {
                (true, true) => "0".to_string(),
                (false, true) => format!("{sign}{int}"),
                (_, false) => format!("{sign}{int}.{frac}"),
            }
        }
    };
    pgnumeric::Numeric::parse(&s).unwrap_or_else(|e| panic!("numeric {s}: {e}"))
}

fn gen_f64(rng: &mut Rng) -> f64 {
    match rng.below(4) {
        0 => *rng.pick(&[
            f64::NAN,
            -f64::NAN,
            f64::from_bits(0x7FF0_0000_0000_0001), // signalling NaN payload
            f64::INFINITY,
            f64::NEG_INFINITY,
            0.0,
            -0.0,
            f64::MIN_POSITIVE,
            5e-324,
            -5e-324,
            f64::MAX,
            f64::MIN,
            1.5,
            -2.25,
            0.1,
        ]),
        1 => f64::from_bits(rng.next()),
        _ => rng.range(-1000, 1000) as f64 / *rng.pick(&[1.0, 4.0, 10.0, 3.0]),
    }
}

/// JSON text: numbers in several scales, strings with escapes, nesting.
fn gen_json(rng: &mut Rng, depth: u32) -> String {
    let scalar = |rng: &mut Rng| -> String {
        match rng.below(4) {
            0 => rng.pick(&["0", "-0", "1", "1.0", "1.50", "-2", "1e2", "1E-2", "0.000", "-0.5", "123456789012345678901234567890"]).to_string(),
            1 => rng.pick(&[r#""""#, r#""a""#, r#""b""#, r#""é""#, r#""a\nb""#, r#""\"q\"""#, r#""é""#]).to_string(),
            2 => rng.pick(&["true", "false"]).to_string(),
            _ => "null".to_string(),
        }
    };
    if depth >= 3 || rng.chance(50) {
        return scalar(rng);
    }
    let n = rng.index(4);
    if rng.chance(50) {
        let items: Vec<String> = (0..n).map(|_| gen_json(rng, depth + 1)).collect();
        format!("[{}]", items.join(", "))
    } else {
        let items: Vec<String> = (0..n).map(|_| format!(r#""{}": {}"#, rng.pick(&["a", "b", "c", "aa", ""]), gen_json(rng, depth + 1))).collect();
        format!("{{{}}}", items.join(", "))
    }
}

fn gen_scalar(rng: &mut Rng, ty: &SqlType) -> Datum {
    match ty {
        SqlType::Bool => Datum::Bool(rng.chance(50)),
        SqlType::Int2 => Datum::Int2(if rng.chance(20) { *rng.pick(&[i16::MIN, i16::MAX, 0, -1]) } else { rng.next() as i16 }),
        SqlType::Int4 => Datum::Int4(if rng.chance(20) { *rng.pick(&[i32::MIN, i32::MAX, 0, -1]) } else { rng.range(-100, 100) as i32 }),
        SqlType::Int8 => Datum::Int8(if rng.chance(20) { *rng.pick(&[i64::MIN, i64::MAX, 0, -1]) } else { rng.next() as i64 >> rng.below(64) }),
        SqlType::Oid => Datum::Oid(if rng.chance(20) { *rng.pick(&[0, u32::MAX]) } else { rng.next() as u32 }),
        SqlType::Float4 => Datum::Float4(gen_f64(rng) as f32),
        SqlType::Float8 => Datum::Float8(gen_f64(rng)),
        SqlType::Numeric => Datum::numeric(gen_numeric(rng)),
        SqlType::Text => {
            let n = rng.index(6);
            Datum::text_owned((0..n).map(|_| *rng.pick(&['a', 'b', 'Z', ' ', '~', 'é', 'ß', '😀', '\u{7f}', '\u{1}'])).collect())
        }
        SqlType::Bytea => Datum::bytea((0..rng.index(6)).map(|_| *rng.pick(&[0u8, 1, 0x7F, 0xFE, 0xFF, b'a'])).collect()),
        SqlType::Date => Datum::Date(match rng.below(6) {
            0 => *rng.pick(&[i32::MIN, i32::MAX]), // -infinity, infinity
            1 => *rng.pick(&[-2_451_545, 2_145_031_947, 0, -1]),
            _ => rng.range(-2_451_545, 2_145_031_947) as i32 >> rng.below(16),
        }),
        SqlType::Time => Datum::Time(if rng.chance(10) { 86_400_000_000 } else { rng.range(0, 86_399_999_999) }),
        SqlType::Timestamp | SqlType::TimestampTz => {
            let v = match rng.below(6) {
                0 => *rng.pick(&[i64::MIN, i64::MAX]),
                1 => *rng.pick(&[-211_813_488_000_000_000, 9_223_371_331_199_999_999, 0, -1]),
                _ => rng.range(-211_813_488_000_000_000, 9_223_371_331_199_999_999) >> rng.below(40),
            };
            if *ty == SqlType::Timestamp { Datum::Timestamp(v) } else { Datum::TimestampTz(v) }
        }
        SqlType::Interval => Datum::Interval(match rng.below(8) {
            0 => *rng.pick(&[Interval::INFINITY, Interval::NEG_INFINITY]),
            1 => Interval { months: rng.next() as i32 >> 8, days: rng.next() as i32 >> 8, micros: rng.next() as i64 >> 12 },
            _ => Interval { months: rng.range(-30, 30) as i32, days: rng.range(-60, 60) as i32, micros: rng.range(-5, 5) * 3_600_000_000 },
        }),
        SqlType::Uuid => {
            let mut u = [0u8; 16];
            for b in u.iter_mut() {
                *b = if rng.chance(30) { 0 } else { rng.next() as u8 };
            }
            Datum::Uuid(u)
        }
        SqlType::Json => Datum::Json(gen_json(rng, 0).into()),
        SqlType::Jsonb => Datum::jsonb(pgfuncs::json::Jsonb::parse(&gen_json(rng, 0)).unwrap()),
        other => unreachable!("{other:?}"),
    }
}

fn gen_array(rng: &mut Rng, elem: &SqlType) -> ArrayValue {
    let lens: Vec<i32> = match rng.below(10) {
        0 => vec![],
        1..=5 => vec![rng.range(1, 4) as i32],
        6..=8 => vec![rng.range(1, 3) as i32, rng.range(1, 3) as i32],
        _ => vec![2, 1, rng.range(1, 2) as i32],
    };
    let dims: Vec<(i32, i32)> = lens.iter().map(|l| (if rng.chance(20) { rng.range(-3, 5) as i32 } else { 1 }, *l)).collect();
    let n: i32 = if dims.is_empty() { 0 } else { lens.iter().product() };
    let elems = (0..n).map(|_| if rng.chance(15) { Datum::Null } else { gen_scalar(rng, elem) }).collect();
    ArrayValue { elem_type: elem.clone(), dims, elems }
}

fn gen_value(rng: &mut Rng, ty: &SqlType) -> Datum {
    if rng.chance(8) {
        return Datum::Null;
    }
    match ty {
        SqlType::Array(e) => Datum::array(gen_array(rng, e)),
        t => gen_scalar(rng, t),
    }
}

/// Another value equal to `d` in a different representation, where the type
/// has such (otherwise `d` itself).
fn other_repr(rng: &mut Rng, d: &Datum) -> Datum {
    match d {
        Datum::Numeric(n) if !n.is_nan() && !n.is_infinite() => Datum::numeric(n.with_dscale(n.dscale() + 1 + rng.below(3) as u32)),
        Datum::Float8(v) if *v == 0.0 => Datum::Float8(-v),
        Datum::Float8(v) if v.is_nan() => Datum::Float8(f64::from_bits(v.to_bits() ^ (1 << 63))),
        Datum::Float4(v) if *v == 0.0 => Datum::Float4(-v),
        Datum::Interval(iv) if iv.months > i32::MIN + 1 && iv.months < i32::MAX && iv.days < i32::MAX - 30 && iv.days > i32::MIN => {
            if rng.chance(50) {
                Datum::Interval(Interval { months: iv.months - 1, days: iv.days + 30, micros: iv.micros })
            } else if iv.micros < i64::MAX - 86_400_000_000 {
                Datum::Interval(Interval { months: iv.months, days: iv.days - 1, micros: iv.micros + 86_400_000_000 })
            } else {
                d.clone()
            }
        }
        Datum::Jsonb(j) => pgfuncs::json::Jsonb::parse(&rescale_json_numbers(&j.to_string())).map(Datum::jsonb).unwrap_or_else(|_| d.clone()),
        Datum::Array(a) => {
            let mut b = (**a).clone();
            for e in b.elems.iter_mut() {
                *e = other_repr(rng, e);
            }
            Datum::array(b)
        }
        _ => d.clone(),
    }
}

/// JSON text with every number in a larger scale ("1" -> "1.0", "1.5" -> "1.50").
fn rescale_json_numbers(text: &str) -> String {
    let mut out = String::new();
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' => {
                out.push(c);
                while let Some(c) = chars.next() {
                    out.push(c);
                    match c {
                        '\\' => out.extend(chars.next()),
                        '"' => break,
                        _ => {}
                    }
                }
            }
            '-' | '0'..='9' => {
                let mut num = String::from(c);
                while let Some(&n) = chars.peek() {
                    if n.is_ascii_digit() || matches!(n, '.' | 'e' | 'E' | '+' | '-') {
                        num.push(n);
                        chars.next();
                    } else {
                        break;
                    }
                }
                if !num.contains(['e', 'E']) {
                    num.push_str(if num.contains('.') { "0" } else { ".0" });
                }
                out.push_str(&num);
            }
            _ => out.push(c),
        }
    }
    out
}

/// A pair of values of type `ty` that is often equal or close.
fn gen_pair(rng: &mut Rng, ty: &SqlType, pool: &mut Vec<Datum>) -> (Datum, Datum) {
    let a = if !pool.is_empty() && rng.chance(30) { pool[rng.index(pool.len())].clone() } else { gen_value(rng, ty) };
    if pool.len() < 8 {
        pool.push(a.clone());
    }
    let b = match rng.below(10) {
        0..=2 => other_repr(rng, &a),
        3..=4 if !pool.is_empty() => pool[rng.index(pool.len())].clone(),
        5 => match &a {
            // same elements, other shape
            Datum::Array(x) if x.dims.len() == 1 && x.dims[0].1 >= 1 => {
                let mut y = (**x).clone();
                y.dims[0].0 += 1;
                Datum::array(y)
            }
            _ => gen_value(rng, ty),
        },
        _ => gen_value(rng, ty),
    };
    (a, b)
}

/// The order `encode_key` must produce under `spec`.
fn spec_cmp(a: &Datum, b: &Datum, spec: SortSpec) -> Ordering {
    match (a.is_null(), b.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => if spec.nulls_first { Ordering::Less } else { Ordering::Greater },
        (false, true) => if spec.nulls_first { Ordering::Greater } else { Ordering::Less },
        _ => {
            let c = a.sql_cmp(b);
            if spec.desc { c.reverse() } else { c }
        }
    }
}

fn enc(d: &Datum, spec: SortSpec) -> Vec<u8> {
    let mut v = Vec::new();
    encode_key(d, spec, &mut v);
    v
}

/// Canonical key plus fidelity suffix: the identity of a key value.
fn identity(d: &Datum, ty: &SqlType) -> Vec<u8> {
    let mut v = enc(d, SortSpec::ASC);
    if !d.is_null() && has_fidelity(ty) {
        encode_fidelity(d, &mut v);
    }
    v
}

/// NaNs of any bit pattern are one value: PostgreSQL prints and compares them
/// alike, and key encodings canonicalize them.
fn canonical_nans(d: &Datum) -> Datum {
    match d {
        Datum::Float8(v) if v.is_nan() => Datum::Float8(f64::NAN),
        Datum::Float4(v) if v.is_nan() => Datum::Float4(f32::NAN),
        Datum::Array(a) => {
            let mut b = (**a).clone();
            b.elems = b.elems.iter().map(canonical_nans).collect();
            Datum::array(b)
        }
        _ => d.clone(),
    }
}

/// Bit-for-bit identity (including NaN payloads and array element types).
fn identical(a: &Datum, b: &Datum) -> bool {
    match (a, b) {
        (Datum::Float4(x), Datum::Float4(y)) => x.to_bits() == y.to_bits(),
        (Datum::Float8(x), Datum::Float8(y)) => x.to_bits() == y.to_bits(),
        (Datum::Array(x), Datum::Array(y)) => {
            x.elem_type == y.elem_type && x.dims == y.dims && x.elems.len() == y.elems.len() && x.elems.iter().zip(&y.elems).all(|(p, q)| identical(p, q))
        }
        (Datum::Text(x), Datum::Text(y)) | (Datum::Json(x), Datum::Json(y)) => x == y,
        _ => std::mem::discriminant(a) == std::mem::discriminant(b) && exact_cmp(a, b) == Ordering::Equal,
    }
}

const SPECS: [SortSpec; 4] = [
    SortSpec { desc: false, nulls_first: false },
    SortSpec { desc: true, nulls_first: true },
    SortSpec { desc: false, nulls_first: true },
    SortSpec { desc: true, nulls_first: false },
];

fn check_pair(ty: &SqlType, a: &Datum, b: &Datum) {
    for spec in SPECS {
        let (ea, eb) = (enc(a, spec), enc(b, spec));
        assert_eq!(ea.cmp(&eb), spec_cmp(a, b, spec), "{ty:?} {spec:?}: order of {a:?} vs {b:?}");
        // prefix-free: neither encoding is a proper prefix of the other
        if ea != eb {
            assert!(!ea.starts_with(&eb) && !eb.starts_with(&ea), "{ty:?} {spec:?}: {a:?} vs {b:?} not prefix-free");
        }
    }
    let exact_eq = exact_cmp(&canonical_nans(a), &canonical_nans(b)) == Ordering::Equal && a.is_null() == b.is_null();
    assert_eq!(identity(a, ty) == identity(b, ty), exact_eq, "{ty:?}: key identity of {a:?} vs {b:?}");
}

fn check_value(ty: &SqlType, d: &Datum) {
    for spec in SPECS {
        let e = enc(d, spec);
        let mut padded = e.clone();
        padded.extend_from_slice(&[0x00, 0xFF, 0x02, 0x01]);
        let (back, n) = decode_key(&padded, ty, spec).unwrap_or_else(|err| panic!("{ty:?} {spec:?}: decoding {d:?}: {err}"));
        assert_eq!(n, e.len(), "{ty:?} {spec:?}: decoded length of {d:?}");
        assert_eq!(skip_key(&padded, ty, spec).unwrap(), e.len());
        assert_eq!(enc(&back, spec), e, "{ty:?} {spec:?}: canonical re-encoding of {d:?} (decoded {back:?})");
        assert_eq!(back.sql_cmp(d), Ordering::Equal, "{ty:?} {spec:?}: decoded {back:?} vs {d:?}");
        if !d.is_null() && has_fidelity(ty) {
            let mut f = Vec::new();
            encode_fidelity(d, &mut f);
            f.extend_from_slice(&[7, 7, 7]);
            let mut exact = back.clone();
            let used = apply_fidelity(&mut exact, &f).unwrap();
            assert_eq!(used, f.len() - 3, "{ty:?}: fidelity length of {d:?}");
            assert!(identical(&exact, &canonical_nans(d)), "{ty:?}: fidelity restores {exact:?}, not {d:?}");
        } else if !d.is_null() {
            assert!(identical(&back, &canonical_nans(d)), "{ty:?}: decoded {back:?}, not {d:?}");
        }
    }
    // row codec: exact, NaN payloads included
    let mut buf = Vec::new();
    encode_datum(d, &mut buf);
    let mut rd = &buf[..];
    let back = decode_datum(&mut rd).unwrap();
    assert!(rd.is_empty() && identical(&back, d), "{ty:?}: codec round trip of {d:?} gave {back:?}");
    if d.is_null() {
        return;
    }
    // text output then input
    let text = format_datum(d);
    let parsed = parse_datum(&text, ty, -1).unwrap_or_else(|e| panic!("{ty:?}: parsing {text:?} (from {d:?}): {e}"));
    let text_exact = !matches!(d, Datum::Text(s) if s.contains('\0'));
    if text_exact {
        assert!(identical(&parsed, &canonical_nans(d)), "{ty:?}: text {text:?} of {d:?} parses as {parsed:?}");
    }
    // binary send then receive
    if let Some(bytes) = send_datum(d, ty) {
        let back = recv_datum(&bytes, ty).unwrap_or_else(|e| panic!("{ty:?}: receiving {d:?}: {e}"));
        assert!(identical(&back, d), "{ty:?}: binary round trip of {d:?} gave {back:?}");
    }
}

fn run(seed: u64, iterations: usize) {
    let mut rng = Rng::new(seed);
    for _ in 0..iterations {
        let ty = gen_type(&mut rng);
        let mut pool = Vec::new();
        for _ in 0..8 {
            let (a, b) = gen_pair(&mut rng, &ty, &mut pool);
            check_value(&ty, &a);
            check_pair(&ty, &a, &b);
        }
        // composite keys order lexicographically
        let ty2 = gen_type(&mut rng);
        let (a1, b1) = gen_pair(&mut rng, &ty, &mut pool);
        let mut pool2 = Vec::new();
        let (a2, b2) = gen_pair(&mut rng, &ty2, &mut pool2);
        for (s1, s2) in [(SPECS[0], SPECS[1]), (SPECS[2], SPECS[3])] {
            let ka = [enc(&a1, s1), enc(&a2, s2)].concat();
            let kb = [enc(&b1, s1), enc(&b2, s2)].concat();
            let want = spec_cmp(&a1, &b1, s1).then(spec_cmp(&a2, &b2, s2));
            assert_eq!(ka.cmp(&kb), want, "composite ({ty:?}, {ty2:?}): ({a1:?}, {a2:?}) vs ({b1:?}, {b2:?})");
        }
        // rows of mixed types
        let row: Vec<Datum> = (0..rng.index(6))
            .map(|_| {
                let t = gen_type(&mut rng);
                gen_value(&mut rng, &t)
            })
            .collect();
        let mut buf = Vec::new();
        encode_row(&row, &mut buf);
        let mut rd = &buf[..];
        let back = decode_row(&mut rd).unwrap();
        assert!(rd.is_empty() && back.len() == row.len() && row.iter().zip(&back).all(|(a, b)| identical(a, b)), "row {row:?}");
        // prefix_successor bounds every extension of a prefix
        let prefix: Vec<u8> = (0..rng.index(4)).map(|_| *rng.pick(&[0u8, 1, 0xFE, 0xFF])).collect();
        match prefix_successor(&prefix) {
            None => assert!(prefix.iter().all(|b| *b == 0xFF)),
            Some(s) => {
                assert!(s > prefix);
                let mut ext = prefix.clone();
                ext.extend((0..rng.index(4)).map(|_| *rng.pick(&[0u8, 0xFF])));
                assert!(ext < s && !s.starts_with(&prefix), "successor {s:?} of {prefix:?}");
            }
        }
    }
}

#[test]
fn encodings_order_and_round_trip() {
    run_seeds("encodings_order_and_round_trip", &seeds(&[1, 2], false), |seed| run(seed, 1500));
}

#[test]
#[ignore = "long-running; see the module docs"]
fn encodings_order_and_round_trip_long() {
    run_seeds("encodings_order_and_round_trip_long", &seeds(&[], true), |seed| run(seed, 100_000));
}
