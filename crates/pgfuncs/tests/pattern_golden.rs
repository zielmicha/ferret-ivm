//! Golden tests for `pgfuncs::pattern` (LIKE, SIMILAR TO, regex), generated
//! from PostgreSQL 17 by tests/gen/gen_pattern.py.

mod common;

use common::{Golden, i, json_text_array, s};
use pgfuncs::pattern::{self, LikePattern, Regex};
use pgfuncs::Result;

fn like_escape_arg(e: &str) -> Result<Option<char>> {
    // validate like PostgreSQL's like_escape()
    pattern::like_escape_text("", e)?;
    Ok(e.chars().next())
}

fn json_agg(rows: &[Vec<Option<String>>]) -> String {
    if rows.is_empty() {
        return "[]".to_string();
    }
    let parts: Vec<String> = rows.iter().map(|r| json_text_array(r)).collect();
    format!("[{}]", parts.join(", \n "))
}

#[test]
fn pattern_golden() {
    let mut g = Golden::load("pattern_cases.jsonl");
    g.run(dispatch);
    g.finish();
}

#[test]
fn regex_fuzz_golden() {
    let mut g = Golden::load("regex_fuzz.jsonl");
    g.run(dispatch);
    g.finish();
}

/// Large differential test against data produced by tests/gen/fuzz_regex.py;
/// run with PGFUNCS_FUZZ_FILE=/path/to/file.jsonl cargo test -- --ignored
#[test]
#[ignore]
fn regex_fuzz_external() {
    let Ok(path) = std::env::var("PGFUNCS_FUZZ_FILE") else {
        eprintln!("PGFUNCS_FUZZ_FILE not set; skipping");
        return;
    };
    let mut g = Golden::load_path(&path);
    g.run(dispatch);
    g.finish();
}

fn dispatch(fn_name: &str, a: &[serde_json::Value]) -> Result<Option<String>> {
    {
        let b = |x: bool| Some(x.to_string());
        match fn_name {
            "like" => Ok(b(LikePattern::new_lenient(s(&a[1]), Some('\\'), false).try_matches(s(&a[0]))?)),
            "ilike" => Ok(b(LikePattern::new_lenient(s(&a[1]), Some('\\'), true).try_matches(s(&a[0]))?)),
            "like_esc" | "ilike_esc" => {
                let esc = like_escape_arg(s(&a[2]))?;
                let p = LikePattern::new_lenient(s(&a[1]), esc, fn_name == "ilike_esc");
                Ok(b(p.try_matches(s(&a[0]))?))
            }
            "like_bytea" => Ok(b(pattern::like_bytes(s(&a[0]).as_bytes(), s(&a[1]).as_bytes(), Some(b'\\'))?)),
            "like_bytea_lit" => {
                let (x, y) = (pgfuncs::bytea::parse(s(&a[0]))?, pgfuncs::bytea::parse(s(&a[1]))?);
                Ok(b(pattern::like_bytes(&x, &y, Some(b'\\'))?))
            }
            "similar_escape" => Ok(Some(pattern::similar_to_regex(s(&a[0]), Some('\\'))?)),
            "similar_escape2" => Ok(Some(pattern::similar_to_regex_text(s(&a[0]), Some(s(&a[1])))?)),
            "similar" => {
                let re = pattern::similar_to_regex(s(&a[1]), Some('\\'))?;
                Ok(b(Regex::new(&re, "")?.try_is_match(s(&a[0]))?))
            }
            "similar_substring" => {
                let esc = s(&a[2]);
                let re = pattern::similar_to_regex_text(s(&a[1]), Some(esc))?;
                pattern::regex_substring(s(&a[0]), &re)
            }
            "match" | "rmatch" | "match_i" => {
                let flags = if fn_name == "match_i" { "i" } else { "" };
                let re = Regex::new(s(&a[1]), flags)?;
                Ok(re.try_match_groups(s(&a[0]))?.map(|v| json_text_array(&v)))
            }
            "match_flags" => {
                let re = Regex::new(s(&a[1]), s(&a[2]))?;
                re.reject_global("regexp_match()")?;
                Ok(re.try_match_groups(s(&a[0]))?.map(|v| json_text_array(&v)))
            }
            "is_match" => Ok(b(Regex::new(s(&a[1]), "")?.try_is_match(s(&a[0]))?)),
            "is_match_i" => Ok(b(Regex::new(s(&a[1]), "i")?.try_is_match(s(&a[0]))?)),
            "matches_g" | "rmatches" => {
                let re = Regex::new(s(&a[1]), "g")?;
                Ok(Some(json_agg(&re.try_all_matches(s(&a[0]), true)?)))
            }
            "split" => {
                let re = Regex::new(s(&a[1]), "")?;
                let v: Vec<Option<String>> = re.try_split(s(&a[0]))?.into_iter().map(Some).collect();
                Ok(Some(json_text_array(&v)))
            }
            "count" => Ok(Some(Regex::new(s(&a[1]), "")?.count(s(&a[0]), 1)?.to_string())),
            "count_start" => Ok(Some(Regex::new(s(&a[1]), "")?.count(s(&a[0]), i(&a[2]) as i32)?.to_string())),
            "replace" => Ok(Some(Regex::new(s(&a[1]), "")?.try_replace(s(&a[0]), s(&a[2]), false)?)),
            "replace_g" | "rreplace" => Ok(Some(Regex::new(s(&a[1]), "g")?.try_replace(s(&a[0]), s(&a[2]), true)?)),
            "replace_nth" => {
                let re = Regex::new(s(&a[1]), "")?;
                Ok(Some(re.replace_nth(s(&a[0]), s(&a[2]), i(&a[3]) as i32, i(&a[4]) as i32)?))
            }
            "substring" => pattern::regex_substring(s(&a[0]), s(&a[1])),
            "instr" => {
                let re = Regex::new(s(&a[1]), "")?;
                Ok(Some(
                    re.instr(s(&a[0]), i(&a[2]) as i32, i(&a[3]) as i32, i(&a[4]) as i32, i(&a[5]) as i32)?
                        .to_string(),
                ))
            }
            "rsubstr" => {
                let re = Regex::new(s(&a[1]), "")?;
                re.substr(s(&a[0]), i(&a[2]) as i32, i(&a[3]) as i32, i(&a[4]) as i32)
            }
            "fmatch" => {
                let re = Regex::new(s(&a[1]), s(&a[2]))?;
                Ok(re.try_match_groups(s(&a[0]))?.map(|v| json_text_array(&v)))
            }
            "fmatches" => {
                let re = Regex::new(s(&a[1]), s(&a[2]))?;
                Ok(Some(json_agg(&re.try_all_matches(s(&a[0]), true)?)))
            }
            "freplace" => {
                let re = Regex::new(s(&a[1]), s(&a[3]))?;
                Ok(Some(re.try_replace(s(&a[0]), s(&a[2]), true)?))
            }
            "fsplit" => {
                let re = Regex::new(s(&a[1]), s(&a[2]))?;
                let v: Vec<Option<String>> = re.try_split(s(&a[0]))?.into_iter().map(Some).collect();
                Ok(Some(json_text_array(&v)))
            }
            "fcount" => {
                let re = Regex::new(s(&a[1]), s(&a[3]))?;
                Ok(Some(re.count(s(&a[0]), i(&a[2]) as i32)?.to_string()))
            }
            "fis_match" => Ok(b(Regex::new(s(&a[1]), s(&a[2]))?.try_is_match(s(&a[0]))?)),
            other => panic!("unknown fn {other}"),
        }
    }
}
