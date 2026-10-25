//! Compile-time check that the public API has exactly the agreed signatures.
#![allow(clippy::type_complexity)]

use pgfuncs::array::{self, ParsedArray};
use pgfuncs::json::{self, JsonNumber, Jsonb};
use pgfuncs::pattern::{self, LikePattern, Regex};
use pgfuncs::{Error, Result, bytea, text, uuid};

#[test]
fn api_surface() {
    // error type
    let e = Error {
        sqlstate: "22023",
        message: String::new(),
        detail: None,
    };
    let _: &dyn std::error::Error = &e;
    let _: Result<()> = Ok(());

    // text
    let _: fn(&str) -> i32 = text::char_length;
    let _: fn(&str) -> i32 = text::octet_length;
    let _: fn(&str) -> i32 = text::bit_length;
    let _: fn(&str) -> String = text::lower;
    let _: fn(&str) -> String = text::upper;
    let _: fn(&str) -> String = text::initcap;
    let _: fn(&str, i64, Option<i64>) -> Result<String> = text::substr;
    let _: fn(&[u8], i64, Option<i64>) -> Result<Vec<u8>> = text::substr_bytes;
    let _: fn(&str, &str) -> i32 = text::strpos;
    let _: fn(&str, &str, i64, Option<i64>) -> Result<String> = text::overlay;
    let _: fn(&str, &str) -> String = text::btrim;
    let _: fn(&str, &str) -> String = text::ltrim;
    let _: fn(&str, &str) -> String = text::rtrim;
    let _: fn(&str, &str, &str) -> String = text::replace;
    let _: fn(&str, &str, i32) -> Result<String> = text::split_part;
    let _: fn(&str, i32) -> String = text::left;
    let _: fn(&str, i32) -> String = text::right;
    let _: fn(&str, i32, &str) -> String = text::lpad;
    let _: fn(&str, i32, &str) -> String = text::rpad;
    let _: fn(&str, i32) -> Result<String> = text::repeat;
    let _: fn(&str) -> String = text::reverse;
    let _: fn(&str, &str, &str) -> String = text::translate;
    let _: fn(&str) -> i32 = text::ascii;
    let _: fn(i32) -> Result<String> = text::chr;
    let _: fn(&str, &str) -> bool = text::starts_with;
    let _: fn(&[u8]) -> String = text::md5_hex;
    let _: fn(&[u8]) -> Vec<u8> = text::sha224;
    let _: fn(&[u8]) -> Vec<u8> = text::sha256;
    let _: fn(&[u8]) -> Vec<u8> = text::sha384;
    let _: fn(&[u8]) -> Vec<u8> = text::sha512;
    let _: fn(&str) -> String = text::quote_ident;
    let _: fn(&str) -> String = text::quote_literal;
    let _: fn(i64) -> String = text::to_hex;
    let _: fn(i64) -> String = text::to_bin;
    let _: fn(i64) -> String = text::to_oct;
    let _: fn(&str, &[Option<String>]) -> Result<String> = text::format;
    let _: fn(&str, Option<&str>, Option<&str>) -> Vec<Option<String>> = text::string_to_array;
    let _: fn(&str) -> Result<String> = text::unistr;

    // pattern
    let _: fn(&str, Option<char>, bool) -> Result<LikePattern> = LikePattern::new;
    let _: fn(&LikePattern, &str) -> bool = LikePattern::matches;
    let _: fn(&[u8], &[u8], Option<u8>) -> Result<bool> = pattern::like_bytes;
    let _: fn(&str, Option<char>) -> Result<String> = pattern::similar_to_regex;
    let _: fn(&str, &str) -> Result<Regex> = Regex::new;
    let _: fn(&Regex, &str) -> bool = Regex::is_match;
    let _: fn(&Regex, &str, &str, bool) -> String = Regex::replace;
    let _: fn(&Regex, &str, &str, i32, i32) -> Result<String> = Regex::replace_nth;
    let _: fn(&Regex, &str) -> Option<Vec<Option<String>>> = Regex::match_groups;
    let _: fn(&Regex, &str) -> Vec<Vec<Option<String>>> = Regex::all_matches;
    let _: fn(&Regex, &str) -> Vec<String> = Regex::split;
    let _: fn(&Regex, &str, i32) -> Result<i32> = Regex::count;
    let _: fn(&Regex, &str, i32, i32, i32, i32) -> Result<i32> = Regex::instr;
    let _: fn(&Regex, &str, i32, i32, i32) -> Result<Option<String>> = Regex::substr;
    let _: fn(&str, &str) -> Result<Option<String>> = pattern::regex_substring;

    // bytea / uuid
    let _: fn(&str) -> Result<Vec<u8>> = bytea::parse;
    let _: fn(&[u8]) -> String = bytea::format;
    let _: fn(&[u8], &str) -> Result<String> = bytea::encode;
    let _: fn(&str, &str) -> Result<Vec<u8>> = bytea::decode;
    let _: fn(&str) -> Result<[u8; 16]> = uuid::parse;
    let _: fn(&[u8; 16]) -> String = uuid::format;

    // array
    let _: fn(&str, char) -> Result<ParsedArray> = array::parse;
    let _: fn(&[(i32, i32)], &[Option<&str>], char) -> String = array::format;
    let pa = ParsedArray {
        dims: vec![(1, 1)],
        elements: vec![Some("x".to_string())],
    };
    let _ = pa.clone() == pa;

    // json
    let _: fn(&str) -> Result<JsonNumber> = JsonNumber::parse;
    let _: fn(&JsonNumber) -> &str = JsonNumber::as_str;
    let _: fn(i64) -> JsonNumber = JsonNumber::from_i64;
    let _: fn(f64) -> Option<JsonNumber> = JsonNumber::from_f64;
    let _: fn(&str) -> Result<Jsonb> = Jsonb::parse;
    let _: fn(&Jsonb) -> String = Jsonb::to_pretty;
    let _: for<'a> fn(&'a Jsonb, &str) -> Option<&'a Jsonb> = Jsonb::get_key;
    let _: fn(&Jsonb, i64) -> Option<&Jsonb> = Jsonb::get_index;
    let _: for<'a> fn(&'a Jsonb, &[&str]) -> Option<&'a Jsonb> = Jsonb::get_path;
    let _: fn(&Jsonb) -> Option<String> = Jsonb::as_text;
    let _: fn(&Jsonb, &Jsonb) -> bool = Jsonb::contains;
    let _: fn(&Jsonb, &str) -> bool = Jsonb::exists;
    let _: fn(&Jsonb, &[&str]) -> bool = Jsonb::exists_any;
    let _: fn(&Jsonb, &[&str]) -> bool = Jsonb::exists_all;
    let _: fn(&Jsonb, &Jsonb) -> Jsonb = Jsonb::concat;
    let _: fn(&Jsonb, &str) -> Result<Jsonb> = Jsonb::delete_key;
    let _: fn(&Jsonb, i64) -> Result<Jsonb> = Jsonb::delete_index;
    let _: fn(&Jsonb, &[&str]) -> Result<Jsonb> = Jsonb::delete_path;
    let _: fn(&Jsonb, &[&str], Jsonb, bool) -> Result<Jsonb> = Jsonb::set;
    let _: fn(&Jsonb, &[&str], Jsonb, bool) -> Result<Jsonb> = Jsonb::insert;
    let _: fn(&Jsonb) -> &'static str = Jsonb::type_name;
    let _: fn(&Jsonb) -> Result<i32> = Jsonb::array_length;
    let _: fn(&Jsonb) -> Result<Vec<String>> = Jsonb::object_keys;
    let _: fn(&Jsonb) -> Result<Vec<Jsonb>> = Jsonb::array_elements;
    let _: fn(&Jsonb) -> Result<Vec<(String, Jsonb)>> = Jsonb::object_entries;
    let _: fn(&Jsonb) -> Jsonb = Jsonb::strip_nulls;
    let _: fn(Vec<(String, Jsonb)>) -> Jsonb = Jsonb::build_object;
    let _: fn(&Jsonb, &mut Vec<u8>) = Jsonb::encode_key;
    let _: fn(&[u8]) -> Result<(Jsonb, usize)> = Jsonb::decode_key;
    let _: fn(&str) -> Result<()> = json::validate_json;
    let _: fn(&str) -> String = json::escape_string;
    let _: fn(&str, &str) -> Result<Option<String>> = json::json_text_get_key;
    let _: fn(&str, i64) -> Result<Option<String>> = json::json_text_get_index;
    let _: fn(&str) -> Result<Option<String>> = json::json_text_as_text;

    // jsonpath
    use json::path::{self, JsonPath, PathContext, SessionTz};
    let _: fn(&str) -> Result<JsonPath> = JsonPath::parse;
    let _: fn(&JsonPath) -> Result<String> = JsonPath::to_string_checked;
    let _: fn(&JsonPath) -> bool = JsonPath::is_lax;
    let _: fn(&Jsonb, &JsonPath, &Jsonb, bool) -> Result<Vec<Jsonb>> = path::path_query;
    let _: fn(&Jsonb, &JsonPath, &Jsonb, bool) -> Result<Jsonb> = path::path_query_array;
    let _: fn(&Jsonb, &JsonPath, &Jsonb, bool) -> Result<Option<Jsonb>> = path::path_query_first;
    let _: fn(&Jsonb, &JsonPath, &Jsonb, bool) -> Result<Option<bool>> = path::path_exists;
    let _: fn(&Jsonb, &JsonPath, &Jsonb, bool) -> Result<Option<bool>> = path::path_match;
    let _: fn(&Jsonb, &JsonPath) -> Result<Option<bool>> = path::path_exists_op;
    let _: fn(&Jsonb, &JsonPath) -> Result<Option<bool>> = path::path_match_op;
    let _: fn(&Jsonb, &JsonPath, &Jsonb, bool, &SessionTz) -> Result<Vec<Jsonb>> =
        path::path_query_tz;
    let _: fn(&Jsonb, &JsonPath, &Jsonb, bool, &SessionTz) -> Result<Jsonb> =
        path::path_query_array_tz;
    let _: fn(&Jsonb, &JsonPath, &Jsonb, bool, &SessionTz) -> Result<Option<Jsonb>> =
        path::path_query_first_tz;
    let _: fn(&Jsonb, &JsonPath, &Jsonb, bool, &SessionTz) -> Result<Option<bool>> =
        path::path_exists_tz;
    let _: fn(&Jsonb, &JsonPath, &Jsonb, bool, &SessionTz) -> Result<Option<bool>> =
        path::path_match_tz;
    let session = SessionTz::default();
    let cx = PathContext::new(&session, true);
    let _ = PathContext::utc();
    let jp: JsonPath = "$.a".parse().unwrap();
    let doc = Jsonb::parse(r#"{"a": 1}"#).unwrap();
    let _ = cx.query(&doc, &jp, &doc, false);
    let _ = cx.query_array(&doc, &jp, &doc, false);
    let _ = cx.query_first(&doc, &jp, &doc, false);
    let _ = cx.exists(&doc, &jp, &doc, false);
    let _ = cx.matches(&doc, &jp, &doc, false);
    let _ = cx.exists_op(&doc, &jp);
    let _ = cx.match_op(&doc, &jp);
    let _ = format!("{jp} {jp:?}");
    let _ = jp.clone();

    // jsonb enum shape, traits
    let v = Jsonb::Array(vec![
        Jsonb::Null,
        Jsonb::Bool(true),
        Jsonb::Number(JsonNumber::from_i64(1)),
    ]);
    let w = Jsonb::Object(vec![("a".to_string(), Jsonb::String("x".to_string()))]);
    assert!(v < w);
    assert_eq!(v.clone(), v);
    let _ = format!("{v} {v:?}");
    fn assert_traits<T: Clone + std::fmt::Debug + Ord + Eq + PartialOrd + PartialEq>() {}
    assert_traits::<Jsonb>();
    assert_traits::<JsonNumber>();
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Regex>();
    assert_send_sync::<LikePattern>();
    assert_send_sync::<Jsonb>();
    assert_send_sync::<JsonPath>();
    assert_send_sync::<SessionTz>();
}
