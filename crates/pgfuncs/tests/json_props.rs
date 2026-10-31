//! Property tests for pgfuncs::json: key encoding vs Ord, roundtrips, hash
//! consistency, total order laws; plus a stack-safety test for deep values.

use pgfuncs::json::{JsonNumber, Jsonb};
use proptest::prelude::*;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

fn number() -> impl Strategy<Value = Jsonb> {
    (
        prop::bool::ANY,
        prop::sample::select(vec![
            "0",
            "1",
            "2",
            "9",
            "10",
            "12",
            "100",
            "123",
            "1000000000000000000001",
        ]),
        prop::option::of(prop::sample::select(vec![
            "0", "00", "5", "50", "05", "25", "001", "999",
        ])),
        prop::option::of(-4i32..4),
    )
        .prop_map(|(neg, int, frac, exp)| {
            let mut s = String::new();
            if neg {
                s.push('-');
            }
            s.push_str(int);
            if let Some(f) = frac {
                s.push('.');
                s.push_str(f);
            }
            if let Some(e) = exp {
                s.push_str(&format!("e{e}"));
            }
            Jsonb::Number(JsonNumber::parse(&s).unwrap())
        })
}

fn string() -> impl Strategy<Value = String> {
    prop_oneof![
        prop::sample::select(vec![
            "", "a", "b", "ab", "ba", "aa", "é", "\u{1}", "a\u{0}b", "Z", "日本"
        ])
        .prop_map(str::to_owned),
        "[a-c\u{e9}\u{0}]{0,4}",
    ]
}

fn scalar() -> impl Strategy<Value = Jsonb> {
    prop_oneof![
        Just(Jsonb::Null),
        prop::bool::ANY.prop_map(Jsonb::Bool),
        number(),
        string().prop_map(Jsonb::String),
    ]
}

fn jsonb() -> impl Strategy<Value = Jsonb> {
    scalar().prop_recursive(4, 32, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Jsonb::Array),
            prop::collection::vec((string(), inner), 0..4).prop_map(Jsonb::build_object),
        ]
    })
}

/// Replaces NUL characters (impossible in PostgreSQL text) in strings/keys.
fn strip_nul(v: Jsonb) -> Jsonb {
    match v {
        Jsonb::String(s) => Jsonb::String(s.replace('\0', "0")),
        Jsonb::Array(a) => Jsonb::Array(a.into_iter().map(strip_nul).collect()),
        Jsonb::Object(p) => Jsonb::build_object(
            p.into_iter()
                .map(|(k, v)| (k.replace('\0', "0"), strip_nul(v)))
                .collect(),
        ),
        other => other,
    }
}

fn enc(v: &Jsonb) -> Vec<u8> {
    let mut out = Vec::new();
    v.encode_key(&mut out);
    out
}

fn hash(v: &Jsonb) -> u64 {
    let mut h = DefaultHasher::new();
    v.hash(&mut h);
    h.finish()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4000))]

    #[test]
    fn encoding_matches_ord(a in jsonb(), b in jsonb()) {
        prop_assert_eq!(enc(&a).cmp(&enc(&b)), a.cmp(&b));
        if a == b {
            prop_assert_eq!(hash(&a), hash(&b));
        }
    }

    #[test]
    fn encoding_roundtrip(a in jsonb(), b in jsonb()) {
        let ea = enc(&a);
        let (da, n) = Jsonb::decode_key(&ea).unwrap();
        prop_assert_eq!(n, ea.len());
        prop_assert!(da == a);
        prop_assert_eq!(enc(&da), ea.clone());
        // self-delimiting: concatenations decode piecewise
        let mut cat = ea.clone();
        cat.extend(enc(&b));
        let (x, n1) = Jsonb::decode_key(&cat).unwrap();
        let (y, n2) = Jsonb::decode_key(&cat[n1..]).unwrap();
        prop_assert!(x == a && y == b);
        prop_assert_eq!(n1 + n2, cat.len());
    }

    #[test]
    fn text_roundtrip(a in jsonb()) {
        // strings with NUL cannot exist in PostgreSQL text
        let a = strip_nul(a);
        let t = a.to_string();
        let p = Jsonb::parse(&t).unwrap();
        prop_assert_eq!(p.to_string(), t.clone());
        prop_assert!(p == a);
        for text in [a.to_pretty(), a.to_compact()] {
            let q = Jsonb::parse(&text).unwrap();
            prop_assert_eq!(q.to_string(), t.clone());
        }
    }

    #[test]
    fn total_order(a in jsonb(), b in jsonb(), c in jsonb()) {
        prop_assert_eq!(a.cmp(&b), b.cmp(&a).reverse());
        if a <= b && b <= c {
            prop_assert!(a <= c);
        }
        prop_assert_eq!(a.cmp(&a), std::cmp::Ordering::Equal);
    }

    #[test]
    fn contains_reflexive(a in jsonb()) {
        prop_assert!(a.contains(&a));
    }
}

#[test]
fn normalize_sorts_nested_objects_and_keeps_last_duplicate() {
    let value = Jsonb::Object(vec![
        ("z".into(), Jsonb::Bool(false)),
        ("a".into(), Jsonb::Null),
        ("z".into(), Jsonb::Array(vec![Jsonb::Object(vec![
            ("bb".into(), Jsonb::Null),
            ("a".into(), Jsonb::Bool(false)),
            ("a".into(), Jsonb::Bool(true)),
        ])])),
    ]).normalize();
    assert_eq!(value.to_string(), r#"{"a": null, "z": [{"a": true, "bb": null}]}"#);
    assert_eq!(value.to_compact(), r#"{"a":null,"z":[{"a":true,"bb":null}]}"#);
    assert_eq!(value.clone().normalize(), value);
}

#[test]
fn equal_numbers_encode_identically() {
    let n = |s: &str| Jsonb::parse(s).unwrap();
    for (x, y) in [
        ("1.50", "1.5"),
        ("0.000", "0"),
        ("-0", "0"),
        ("100", "1e2"),
        ("[1.0, {\"a\": 2.00}]", "[1, {\"a\": 2}]"),
    ] {
        assert_eq!(n(x), n(y));
        assert_eq!(enc(&n(x)), enc(&n(y)));
        assert_eq!(hash(&n(x)), hash(&n(y)));
    }
    let (d, _) = Jsonb::decode_key(&enc(&n("[1.50, -0.0100, 1e3]"))).unwrap();
    assert_eq!(d.to_string(), "[1.5, -0.01, 1000]");
    assert!(Jsonb::decode_key(&[0x03, 0, 0, 0, 5]).is_err());
    assert!(Jsonb::decode_key(&[]).is_err());
    assert!(Jsonb::decode_key(&[0x02, 0x99]).is_err());
}

/// Frozen from the original JSON codec, independently of Numeric's key
/// encoding. Reopening persisted keys must survive changes in representation.
#[test]
fn durable_numeric_key_bytes() {
    for (text, hex) in [
        ("0", "021202"),
        ("-0.000", "021202"),
        ("1", "021203800000010b00"),
        ("-1", "0212017ffffffef4ff"),
        ("1.50", "021203800000011000"),
        ("-1.50", "0212017ffffffeefff"),
        ("0.00120", "0212037ffffffe0d00"),
        ("-0.00120", "02120180000001f2ff"),
        ("100", "021203800000030b00"),
        ("12345.67890", "021203800000050d23394f5b00"),
        ("1e131071", "021203800200000b00"),
        ("1e-16383", "0212037fffc0020b00"),
    ] {
        let expected: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        let value = Jsonb::parse(text).unwrap();
        assert_eq!(enc(&value), expected, "{text}");
        let (decoded, len) = Jsonb::decode_key(&expected).unwrap();
        assert_eq!(decoded, value, "{text}");
        assert_eq!(len, expected.len());
    }
}

/// Deep values must not overflow a 2MB thread stack in recursive operations.
#[test]
fn deep_values_stack_safety() {
    let t = std::thread::Builder::new()
        .stack_size(2 << 20)
        .spawn(|| {
            let docs = [
                "[".repeat(16000) + &"]".repeat(16000),
                "{\"a\":".repeat(14200) + "1" + &"}".repeat(14200),
                "[{\"a\":".repeat(7500) + "null" + &"}]".repeat(7500),
            ];
            for d in &docs {
                let v = Jsonb::parse(d).unwrap();
                let s = v.to_string();
                assert_eq!(v.to_compact(), *d);
                let p = v.to_pretty();
                let c = v.clone();
                assert!(v == c);
                assert!(v.contains(&c));
                let e = enc(&v);
                let (back, n) = Jsonb::decode_key(&e).unwrap();
                assert_eq!(n, e.len());
                assert!(back == v);
                let _ = hash(&v);
                let _ = v.strip_nulls();
                let _ = v.clone().normalize();
                assert!(!s.is_empty() && !p.is_empty());
                let _ = v.concat(&c);
                let _ = v.set(&["0"], Jsonb::Null, true);
            }
        })
        .unwrap();
    t.join().unwrap();
}
