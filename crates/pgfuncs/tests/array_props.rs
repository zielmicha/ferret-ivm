//! Property tests for pgfuncs::array: format/parse roundtrips.

use pgfuncs::array::{self, ParsedArray};
use proptest::prelude::*;

/// Element strings that exercise quoting: structural characters, whitespace
/// recognized by the array parser, NULL look-alikes, and multibyte text.
fn element() -> impl Strategy<Value = Option<String>> {
    let tricky = prop::sample::select(vec![
        "", "NULL", "null", "NuLl", " NULL", "NULL ", "a", " ", "\t", "\n", "\r", "\x0b", "\x0c", "\"", "\\",
        "\\\"", "{", "}", "{}", ",", ";", "|", "a,b", "a;b", "=", "[1:2]", "é", "日本語", "😀", "\u{a0}",
        "\u{3000}", "x y", " x ", "N", "NUL", "NULLL", "¦", "a¦b",
    ]);
    let random = "[a-zA-Z0-9 ,;|{}\"\\\\\t\n\r\x0b\x0c=:\\[\\]éü日😀\u{a0}¦]{0,8}";
    prop_oneof![
        1 => Just(None),
        3 => tricky.prop_map(|s| Some(s.to_string())),
        4 => random.prop_map(Some),
    ]
}

fn lower_bound() -> impl Strategy<Value = i32> {
    prop_oneof![
        6 => -5i32..=5,
        1 => Just(i32::MIN),
        1 => Just(i32::MAX - 7),
        1 => -1_000_000i32..1_000_000,
    ]
}

/// A non-empty array: 1..=6 dimensions with small lengths, plus elements.
fn array_value() -> impl Strategy<Value = ParsedArray> {
    (1usize..=6)
        .prop_flat_map(|ndim| {
            let max_len = if ndim <= 2 { 4 } else { 2 };
            prop::collection::vec((lower_bound(), 1i32..=max_len), ndim)
        })
        .prop_flat_map(|dims| {
            let n: usize = dims.iter().map(|&(_, l)| l as usize).product();
            (Just(dims), prop::collection::vec(element(), n))
        })
        .prop_map(|(dims, elements)| ParsedArray { dims, elements })
}

fn delimiter() -> impl Strategy<Value = char> {
    prop::sample::select(vec![',', ';', '|', '¦'])
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 3000, ..ProptestConfig::default() })]

    #[test]
    fn format_then_parse_roundtrips(a in array_value(), delim in delimiter()) {
        let text = array::format_owned(&a.dims, &a.elements, delim);
        let back = array::parse(&text, delim);
        prop_assert_eq!(back.as_ref().ok(), Some(&a), "text: {:?}, result: {:?}", text, back);
        // Formatting is a fixed point.
        prop_assert_eq!(array::format_owned(&a.dims, &a.elements, delim), text);
    }

    #[test]
    fn decoration_only_for_non_default_bounds(a in array_value()) {
        let text = array::format_owned(&a.dims, &a.elements, ',');
        let needs = a.dims.iter().any(|&(lb, _)| lb != 1);
        prop_assert_eq!(text.starts_with('['), needs);
        prop_assert_eq!(text.starts_with('{'), !needs);
    }

    /// Whitespace around elements, braces and delimiters is insignificant
    /// for quoted output, and the parse result does not depend on it.
    #[test]
    fn whitespace_insensitive(a in array_value(), ws in prop::sample::select(vec![" ", "\t", "\n", "  ", "\r\n", "\x0b", "\x0c"])) {
        let text = array::format_owned(&a.dims, &a.elements, ',');
        // Insert whitespace after every structural character outside quotes.
        let mut spaced = String::new();
        let mut in_quotes = false;
        let mut escaped = false;
        let mut in_dims = text.starts_with('[');
        for c in text.chars() {
            spaced.push(c);
            if in_dims {
                if c == '=' { in_dims = false; spaced.push_str(ws); }
                continue;
            }
            if escaped { escaped = false; continue; }
            match c {
                '\\' => escaped = true,
                '"' => in_quotes = !in_quotes,
                '{' | '}' | ',' if !in_quotes => spaced.push_str(ws),
                _ => {}
            }
        }
        let spaced = format!("{ws}{spaced}");
        let back = array::parse(&spaced, ',');
        prop_assert_eq!(back.as_ref().ok(), Some(&a), "text: {:?}, result: {:?}", spaced, back);
    }

    /// The parser never panics on arbitrary input.
    #[test]
    fn parse_never_panics(s in "[\\[\\]{}\",;\\\\ \taNUL:=0-9é+-]{0,24}", delim in delimiter()) {
        let _ = array::parse(&s, delim);
    }
}

#[test]
fn empty_arrays_format_as_braces() {
    assert_eq!(array::format(&[], &[], ','), "{}");
    assert_eq!(array::format(&[(1, 0)], &[], ','), "{}");
    assert_eq!(array::format(&[(5, 3), (1, 0)], &[], ','), "{}");
    let e = array::parse("{}", ',').unwrap();
    assert!(e.dims.is_empty() && e.elements.is_empty());
}

#[test]
fn non_ascii_delimiter() {
    let a = array::parse("{a¦\"b¦c\"¦NULL}", '¦').unwrap();
    assert_eq!(a.dims, vec![(1, 3)]);
    assert_eq!(a.elements, vec![Some("a".to_string()), Some("b¦c".to_string()), None]);
    assert_eq!(array::format_owned(&a.dims, &a.elements, '¦'), "{a¦\"b¦c\"¦NULL}");
}
