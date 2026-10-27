//! Property tests (proptest) of value encodings over values of every SQL
//! type: NULLs, NaNs in several bit patterns, infinities, -0.0, numerics in
//! several scales, intervals with equal spans but different fields, jsonb
//! with numbers in several scales, multi-dimensional arrays with non-default
//! lower bounds and NULL elements.
//! - The key encoding orders like `Datum::sql_cmp` in every direction and
//!   NULL placement, also in composite keys, and is prefix-free.
//! - Canonical decoding re-encodes to the same bytes; with the fidelity
//!   suffix it restores the exact value; key plus suffix tell two values
//!   apart exactly when `exact_cmp` does.
//! - The row codec, text I/O and binary send/receive round trip exactly.
//! - Skipping a key column, a fidelity suffix or a codec value consumes
//!   exactly what decoding it does.
//!
//! Failures shrink to a minimal case and are recorded under
//! `proptest-regressions/`. `PROPTEST_CASES=N` runs N cases per property
//! instead of 4096; `PROPTEST_RNG_SEED=N` fixes the seed.

use super::codec::{decode_datum, decode_row, encode_datum, encode_row, skip_datum};
use super::datum::{exact_cmp, ArrayValue, Datum};
use super::encoding::*;
use super::text::{format_datum, parse_datum, recv_datum, send_datum};
use super::SqlType;
use pgdatetime::Interval;
use proptest::prelude::*;
use proptest::sample::select;
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

// ---------------------------------------------------------------------------
// strategies

fn arb_type() -> impl Strategy<Value = SqlType> {
    let scalar = select(SCALARS.to_vec());
    prop_oneof![3 => scalar.clone(), 1 => scalar.prop_map(|t| SqlType::Array(Box::new(t)))]
}

fn numeric(s: &str) -> Datum {
    Datum::numeric(pgnumeric::Numeric::parse(s).unwrap_or_else(|e| panic!("numeric {s}: {e}")))
}

fn arb_numeric() -> impl Strategy<Value = Datum> {
    prop_oneof![
        1 => select(vec!["NaN", "Infinity", "-Infinity"]).prop_map(numeric),
        1 => (any::<bool>(), 0..6usize).prop_map(|(neg, z)| numeric(&format!("{}0.{}", if neg { "-" } else { "" }, "0".repeat(z)))),
        1 => (1..1000u32, -40..40i32).prop_map(|(m, e)| numeric(&format!("{m}e{e}"))),
        6 => ("-?[0-9]{1,20}", proptest::option::of("[0-9]{1,20}"))
            .prop_map(|(i, f)| numeric(&match f { Some(f) => format!("{i}.{f}"), None => i })),
    ]
}

fn arb_f64() -> impl Strategy<Value = f64> {
    prop_oneof![
        select(vec![
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
        any::<u64>().prop_map(f64::from_bits),
        (-1000..1000i64, select(vec![1.0, 4.0, 10.0, 3.0])).prop_map(|(a, b)| a as f64 / b),
    ]
}

/// JSON text: numbers in several scales, strings with escapes, nesting.
fn arb_json() -> impl Strategy<Value = String> {
    let leaf = prop_oneof![
        select(vec!["0", "-0", "1", "1.0", "1.50", "-2", "1e2", "1E-2", "0.000", "-0.5", "123456789012345678901234567890"]),
        select(vec![r#""""#, r#""a""#, r#""b""#, r#""é""#, r#""a\nb""#, r#""\"q\"""#, r#""é""#]),
        select(vec!["true", "false", "null"]),
    ]
    .prop_map(str::to_string);
    leaf.prop_recursive(3, 16, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(|v| format!("[{}]", v.join(", "))),
            prop::collection::vec((select(vec!["a", "b", "c", "aa", ""]), inner), 0..4).prop_map(|v| {
                let items: Vec<String> = v.into_iter().map(|(k, x)| format!(r#""{k}": {x}"#)).collect();
                format!("{{{}}}", items.join(", "))
            }),
        ]
    })
}

fn arb_interval() -> impl Strategy<Value = Interval> {
    prop_oneof![
        1 => select(vec![Interval::INFINITY, Interval::NEG_INFINITY]),
        1 => (any::<i32>(), any::<i32>(), any::<i64>()).prop_map(|(m, d, us)| Interval { months: m >> 8, days: d >> 8, micros: us >> 12 }),
        6 => (-30..30i32, -60..60i32, -5..5i64).prop_map(|(m, d, h)| Interval { months: m, days: d, micros: h * 3_600_000_000 }),
    ]
}

fn arb_scalar(ty: &SqlType) -> BoxedStrategy<Datum> {
    match ty {
        SqlType::Bool => any::<bool>().prop_map(Datum::Bool).boxed(),
        SqlType::Int2 => any::<i16>().prop_map(Datum::Int2).boxed(),
        SqlType::Int4 => prop_oneof![select(vec![i32::MIN, i32::MAX, 0, -1]), -100..100i32, any::<i32>()].prop_map(Datum::Int4).boxed(),
        SqlType::Int8 => prop_oneof![select(vec![i64::MIN, i64::MAX, 0, -1]), -100..100i64, any::<i64>()].prop_map(Datum::Int8).boxed(),
        SqlType::Oid => any::<u32>().prop_map(Datum::Oid).boxed(),
        SqlType::Float4 => arb_f64().prop_map(|v| Datum::Float4(v as f32)).boxed(),
        SqlType::Float8 => arb_f64().prop_map(Datum::Float8).boxed(),
        SqlType::Numeric => arb_numeric().boxed(),
        SqlType::Text => "[abZ ~éß😀\u{7f}\u{1}]{0,6}".prop_map(Datum::text_owned).boxed(),
        SqlType::Bytea => prop::collection::vec(select(vec![0u8, 1, 0x7F, 0xFE, 0xFF, b'a']), 0..6).prop_map(Datum::bytea).boxed(),
        // the valid range, -infinity and infinity
        SqlType::Date => prop_oneof![select(vec![i32::MIN, i32::MAX, -2_451_545, 2_145_031_947, 0]), -2_451_545..2_145_031_948i32, -100_000..100_000i32]
            .prop_map(Datum::Date)
            .boxed(),
        SqlType::Time => prop_oneof![Just(86_400_000_000i64), 0..86_400_000_000i64].prop_map(Datum::Time).boxed(),
        SqlType::Timestamp | SqlType::TimestampTz => {
            let tz = *ty == SqlType::TimestampTz;
            prop_oneof![
                select(vec![i64::MIN, i64::MAX, -211_813_488_000_000_000, 9_223_371_331_199_999_999, 0]),
                -211_813_488_000_000_000..9_223_371_331_200_000_000i64,
                -1_000_000_000_000..1_000_000_000_000i64,
            ]
            .prop_map(move |v| if tz { Datum::TimestampTz(v) } else { Datum::Timestamp(v) })
            .boxed()
        }
        SqlType::Interval => arb_interval().prop_map(Datum::Interval).boxed(),
        SqlType::Uuid => any::<[u8; 16]>().prop_map(Datum::Uuid).boxed(),
        SqlType::Json => arb_json().prop_map(|s| Datum::Json(s.into())).boxed(),
        SqlType::Jsonb => arb_json().prop_map(|s| Datum::jsonb(pgfuncs::json::Jsonb::parse(&s).unwrap())).boxed(),
        other => unreachable!("{other:?}"),
    }
}

fn arb_array(elem: SqlType) -> BoxedStrategy<Datum> {
    let lens = prop_oneof![
        1 => Just(vec![]),
        5 => (1..=4i32).prop_map(|l| vec![l]),
        3 => (1..=3i32, 1..=3i32).prop_map(|(a, b)| vec![a, b]),
        1 => (1..=2i32).prop_map(|c| vec![2, 1, c]),
    ];
    lens.prop_flat_map(move |lens| {
        let n = if lens.is_empty() { 0 } else { lens.iter().product::<i32>() as usize };
        let bounds = prop::collection::vec(prop_oneof![4 => Just(1i32), 1 => -3..=5i32], lens.len());
        let elems = prop::collection::vec(prop_oneof![1 => Just(Datum::Null), 6 => arb_scalar(&elem)], n);
        let elem = elem.clone();
        (Just(lens), bounds, elems).prop_map(move |(lens, bounds, elems)| {
            let dims = bounds.into_iter().zip(lens).collect();
            Datum::array(ArrayValue { elem_type: elem.clone(), dims, elems })
        })
    })
    .boxed()
}

fn arb_value(ty: &SqlType) -> BoxedStrategy<Datum> {
    let v = match ty {
        SqlType::Array(e) => arb_array((**e).clone()),
        t => arb_scalar(t),
    };
    prop_oneof![1 => Just(Datum::Null), 12 => v].boxed()
}

fn arb_typed_value() -> impl Strategy<Value = (SqlType, Datum)> {
    arb_type().prop_flat_map(|ty| (Just(ty.clone()), arb_value(&ty)))
}

/// A type and two values: independent, or the second derived from the first
/// (another representation of it, itself, or another array shape).
fn arb_typed_pair() -> impl Strategy<Value = (SqlType, Datum, Datum)> {
    arb_type().prop_flat_map(|ty| (Just(ty.clone()), arb_value(&ty), arb_value(&ty), 0..5u8)).prop_map(|(ty, a, b, how)| {
        let b = match how {
            0 | 1 => other_repr(&a, how == 0),
            2 => a.clone(),
            3 => match &a {
                Datum::Array(x) if !x.dims.is_empty() => {
                    let mut y = (**x).clone();
                    y.dims[0].0 += 1;
                    Datum::array(y)
                }
                _ => b,
            },
            _ => b,
        };
        (ty, a, b)
    })
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

/// Another value equal to `d` in a different representation, where the type
/// has such (otherwise `d` itself).
fn other_repr(d: &Datum, alt: bool) -> Datum {
    match d {
        Datum::Numeric(n) if !n.is_nan() && !n.is_infinite() => Datum::numeric(n.with_dscale(n.dscale() + 1 + alt as u32)),
        Datum::Float8(v) if *v == 0.0 => Datum::Float8(-v),
        Datum::Float8(v) if v.is_nan() => Datum::Float8(f64::from_bits(v.to_bits() ^ (1 << 63))),
        Datum::Float4(v) if *v == 0.0 => Datum::Float4(-v),
        Datum::Interval(iv) if *iv != Interval::INFINITY && *iv != Interval::NEG_INFINITY => {
            let shifted = if alt {
                iv.months.checked_sub(1).zip(iv.days.checked_add(30)).map(|(m, d)| Interval { months: m, days: d, micros: iv.micros })
            } else {
                iv.days.checked_sub(1).zip(iv.micros.checked_add(86_400_000_000)).map(|(d, us)| Interval { months: iv.months, days: d, micros: us })
            };
            shifted.map(Datum::Interval).unwrap_or_else(|| d.clone())
        }
        Datum::Jsonb(j) => pgfuncs::json::Jsonb::parse(&rescale_json_numbers(&j.to_string())).map(Datum::jsonb).unwrap_or_else(|_| d.clone()),
        Datum::Array(a) => {
            let mut b = (**a).clone();
            for e in b.elems.iter_mut() {
                *e = other_repr(e, alt);
            }
            Datum::array(b)
        }
        _ => d.clone(),
    }
}

// ---------------------------------------------------------------------------
// checks

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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(crate::testkit::proptest_cases(4096)))]

    #[test]
    fn key_order_matches_sql_cmp((ty, a, b) in arb_typed_pair()) {
        for spec in SPECS {
            let (ea, eb) = (enc(&a, spec), enc(&b, spec));
            prop_assert_eq!(ea.cmp(&eb), spec_cmp(&a, &b, spec), "{:?} {:?}", ty, spec);
            if ea != eb {
                prop_assert!(!ea.starts_with(&eb) && !eb.starts_with(&ea), "not prefix-free under {:?}", spec);
            }
        }
        let exact_eq = a.is_null() == b.is_null() && exact_cmp(&canonical_nans(&a), &canonical_nans(&b)) == Ordering::Equal;
        prop_assert_eq!(identity(&a, &ty) == identity(&b, &ty), exact_eq, "key identity");
    }

    #[test]
    fn composite_keys_order_lexicographically((t1, a1, b1) in arb_typed_pair(), (t2, a2, b2) in arb_typed_pair(), s in 0..4usize) {
        let (s1, s2) = (SPECS[s], SPECS[3 - s]);
        let ka = [enc(&a1, s1), enc(&a2, s2)].concat();
        let kb = [enc(&b1, s1), enc(&b2, s2)].concat();
        prop_assert_eq!(ka.cmp(&kb), spec_cmp(&a1, &b1, s1).then(spec_cmp(&a2, &b2, s2)), "({:?}, {:?})", t1, t2);
    }

    #[test]
    fn keys_decode_canonically_and_exactly((ty, d) in arb_typed_value()) {
        for spec in SPECS {
            let e = enc(&d, spec);
            let padded = [&e[..], &[0x00, 0xFF, 0x02, 0x01]].concat();
            let (back, n) = decode_key(&padded, &ty, spec).unwrap();
            prop_assert_eq!(n, e.len(), "decoded length under {:?}", spec);
            prop_assert_eq!(skip_key(&padded, &ty, spec).unwrap(), e.len());
            prop_assert_eq!(enc(&back, spec), e, "canonical re-encoding of {:?} under {:?}", back, spec);
            prop_assert_eq!(back.sql_cmp(&d), Ordering::Equal, "decoded {:?}", back);
            if !d.is_null() && has_fidelity(&ty) {
                let mut f = Vec::new();
                encode_fidelity(&d, &mut f);
                let mut exact = back.clone();
                let used = apply_fidelity(&mut exact, &[&f[..], &[7, 7, 7]].concat()).unwrap();
                prop_assert_eq!(used, f.len());
                if !matches!(ty, SqlType::Array(_)) {
                    prop_assert_eq!(skip_fidelity(&[&f[..], &[7, 7, 7]].concat(), &ty).unwrap(), f.len());
                }
                prop_assert!(identical(&exact, &canonical_nans(&d)), "fidelity restores {:?}", exact);
            } else if !d.is_null() {
                prop_assert!(identical(&back, &canonical_nans(&d)), "decoded {:?}", back);
            }
        }
    }

    #[test]
    fn values_round_trip_through_codec_text_and_binary((ty, d) in arb_typed_value()) {
        let mut buf = Vec::new();
        encode_datum(&d, &mut buf);
        let mut rd = &buf[..];
        let back = decode_datum(&mut rd).unwrap();
        prop_assert!(rd.is_empty() && identical(&back, &d), "codec gave {:?}", back);
        let padded = [&buf[..], &[0x7F, 0x00]].concat();
        let mut rd = &padded[..];
        skip_datum(&mut rd).unwrap();
        prop_assert_eq!(padded.len() - rd.len(), buf.len(), "skipped length");
        if !d.is_null() {
            let text = format_datum(&d);
            let parsed = parse_datum(&text, &ty, -1).map_err(|e| TestCaseError::fail(format!("parsing {text:?}: {e}")))?;
            prop_assert!(identical(&parsed, &canonical_nans(&d)), "text {:?} parses as {:?}", text, parsed);
            if let Some(bytes) = send_datum(&d, &ty) {
                let back = recv_datum(&bytes, &ty).map_err(|e| TestCaseError::fail(format!("receiving: {e}")))?;
                prop_assert!(identical(&back, &d), "binary round trip gave {:?}", back);
            }
        }
    }

    #[test]
    fn rows_round_trip(row in prop::collection::vec(arb_typed_value().prop_map(|(_, d)| d), 0..6)) {
        let mut buf = Vec::new();
        encode_row(&row, &mut buf);
        let mut rd = &buf[..];
        let back = decode_row(&mut rd).unwrap();
        prop_assert!(rd.is_empty() && back.len() == row.len() && row.iter().zip(&back).all(|(a, b)| identical(a, b)), "got {:?}", back);
    }

    #[test]
    fn prefix_successor_bounds_every_extension(
        prefix in prop::collection::vec(select(vec![0u8, 1, 0xFE, 0xFF]), 0..5),
        ext in prop::collection::vec(select(vec![0u8, 0xFF]), 0..4),
    ) {
        match prefix_successor(&prefix) {
            None => prop_assert!(prefix.iter().all(|b| *b == 0xFF)),
            Some(s) => {
                let extended = [&prefix[..], &ext[..]].concat();
                prop_assert!(s > prefix && extended < s && !s.starts_with(&prefix), "successor {:?}", s);
            }
        }
    }
}

/// An array type's modifier is its elements': a `bpchar[]` without one is
/// not `character[]`, which PostgreSQL reads as `character(1)[]` (a sink
/// created its target column and wrote its literals so, truncating every
/// element to one character).
#[test]
fn array_types_are_formatted_with_their_elements_modifier() {
    use super::format_type;
    let arr = |t: SqlType| SqlType::Array(Box::new(t));
    assert_eq!(format_type(&arr(SqlType::Bpchar), -1), "bpchar[]");
    assert_eq!(format_type(&arr(SqlType::Bpchar), 7), "character(3)[]");
    assert_eq!(format_type(&arr(SqlType::Varchar), -1), "character varying[]");
    assert_eq!(format_type(&arr(SqlType::Numeric), -1), "numeric[]");
    assert_eq!(format_type(&arr(SqlType::TimestampTz), -1), "timestamp with time zone[]");
}
