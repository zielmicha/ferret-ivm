//! Golden tests for `pgfuncs::text`, generated from PostgreSQL 17 by
//! tests/gen/gen_text.py.

mod common;

use common::{Golden, hex, s, i, opt_s};
use pgfuncs::text;

#[test]
fn text_golden() {
    let mut g = Golden::load("text_cases.jsonl");
    g.run(|fn_name, a| {
        let r: pgfuncs::Result<String> = match fn_name {
            "char_length" => Ok(text::char_length(s(&a[0])).to_string()),
            "octet_length" => Ok(text::octet_length(s(&a[0])).to_string()),
            "bit_length" => Ok(text::bit_length(s(&a[0])).to_string()),
            "lower" => Ok(text::lower(s(&a[0]))),
            "upper" => Ok(text::upper(s(&a[0]))),
            "initcap" => Ok(text::initcap(s(&a[0]))),
            "reverse" => Ok(text::reverse(s(&a[0]))),
            "ascii" => Ok(text::ascii(s(&a[0])).to_string()),
            "md5" => Ok(text::md5_hex(s(&a[0]).as_bytes())),
            "quote_ident" => Ok(text::quote_ident(s(&a[0]))),
            "quote_literal" => Ok(text::quote_literal(s(&a[0]))),
            "substr" => text::substr(s(&a[0]), i(&a[1]), a.get(2).map(i)),
            "substr_bytes" => {
                let b = common::unhex(s(&a[0]));
                text::substr_bytes(&b, i(&a[1]), a.get(2).map(i)).map(|v| format!("\\x{}", hex(&v)))
            }
            "strpos" => Ok(text::strpos(s(&a[0]), s(&a[1])).to_string()),
            "overlay" => text::overlay(s(&a[0]), s(&a[1]), i(&a[2]), a.get(3).map(i)),
            "btrim" => Ok(text::btrim(s(&a[0]), s(&a[1]))),
            "ltrim" => Ok(text::ltrim(s(&a[0]), s(&a[1]))),
            "rtrim" => Ok(text::rtrim(s(&a[0]), s(&a[1]))),
            "replace" => Ok(text::replace(s(&a[0]), s(&a[1]), s(&a[2]))),
            "split_part" => text::split_part(s(&a[0]), s(&a[1]), i(&a[2]) as i32),
            "left" => Ok(text::left(s(&a[0]), i(&a[1]) as i32)),
            "right" => Ok(text::right(s(&a[0]), i(&a[1]) as i32)),
            "lpad" => text::lpad_checked(s(&a[0]), i(&a[1]) as i32, s(&a[2])),
            "rpad" => text::rpad_checked(s(&a[0]), i(&a[1]) as i32, s(&a[2])),
            "repeat" => text::repeat(s(&a[0]), i(&a[1]) as i32),
            "translate" => Ok(text::translate(s(&a[0]), s(&a[1]), s(&a[2]))),
            "chr" => text::chr(i(&a[0]) as i32),
            "starts_with" => Ok(text::starts_with(s(&a[0]), s(&a[1])).to_string()),
            "sha224" => Ok(hex(&text::sha224(s(&a[0]).as_bytes()))),
            "sha256" => Ok(hex(&text::sha256(s(&a[0]).as_bytes()))),
            "sha384" => Ok(hex(&text::sha384(s(&a[0]).as_bytes()))),
            "sha512" => Ok(hex(&text::sha512(s(&a[0]).as_bytes()))),
            "to_hex" => Ok(text::to_hex(i(&a[0]))),
            "to_bin" => Ok(text::to_bin(i(&a[0]))),
            "to_oct" => Ok(text::to_oct(i(&a[0]))),
            "to_hex32" => Ok(text::to_hex32(i(&a[0]) as i32)),
            "to_bin32" => Ok(text::to_bin32(i(&a[0]) as i32)),
            "to_oct32" => Ok(text::to_oct32(i(&a[0]) as i32)),
            "format" => {
                let args: Vec<Option<String>> =
                    a[1].as_array().unwrap().iter().map(|v| opt_s(v).map(str::to_string)).collect();
                text::format(s(&a[0]), &args)
            }
            "string_to_array" => {
                let v = text::string_to_array(s(&a[0]), opt_s(&a[1]), opt_s(&a[2]));
                Ok(common::json_text_array(&v))
            }
            "unistr" => text::unistr(s(&a[0])),
            other => panic!("unknown fn {other}"),
        };
        r.map(Some)
    });
    g.finish();
}
