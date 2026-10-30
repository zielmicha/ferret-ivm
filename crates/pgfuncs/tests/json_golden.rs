//! Golden tests for pgfuncs::json against results recorded from PostgreSQL 17
//! (see tests/gen/gen_json.py).

mod common;
use common::{Golden, s};
use pgfuncs::Error;
use pgfuncs::json::{self, JsonNumber, Jsonb};
use serde_json::Value;

type Res = Result<Option<String>, Error>;

fn strs(v: &Value) -> Vec<&str> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap())
        .collect()
}

fn opt_strs(v: &Value) -> Vec<Option<&str>> {
    v.as_array().unwrap().iter().map(|x| x.as_str()).collect()
}

fn jb(v: &Value) -> Jsonb {
    Jsonb::parse(s(v)).unwrap_or_else(|e| panic!("bad test doc {v}: {e:?}"))
}

fn some(x: impl ToString) -> Res {
    Ok(Some(x.to_string()))
}

fn text_or_null(v: &Jsonb) -> Jsonb {
    v.as_text().map(Jsonb::String).unwrap_or(Jsonb::Null)
}

fn eval(fname: &str, a: &[Value]) -> Res {
    match fname {
        "jsonb_in" => Jsonb::parse(s(&a[0])).map(|j| Some(j.to_string())),
        "json_in" => json::validate_json(s(&a[0])).map(|_| Some(s(&a[0]).to_owned())),
        "jsonb_pretty" => Jsonb::parse(s(&a[0])).map(|j| Some(j.to_pretty())),
        "numeric_in" => JsonNumber::parse(s(&a[0])).map(|n| Some(n.as_str().to_owned())),
        "float8_to_jsonb" => {
            let f: f64 = s(&a[0]).parse().unwrap();
            Ok(JsonNumber::from_f64(f).map(|n| n.as_str().to_owned()))
        }
        "typeof" => some(jb(&a[0]).type_name()),
        "array_length" => jb(&a[0]).array_length().map(|n| Some(n.to_string())),
        "object_keys" => jb(&a[0])
            .object_keys()
            .map(|ks| Some(Jsonb::Array(ks.into_iter().map(Jsonb::String).collect()).to_string())),
        "array_elements" => jb(&a[0])
            .array_elements()
            .map(|es| Some(Jsonb::Array(es).to_string())),
        "array_elements_text" => jb(&a[0])
            .array_elements()
            .map(|es| Some(Jsonb::Array(es.iter().map(text_or_null).collect()).to_string())),
        "object_entries" => jb(&a[0]).object_entries().map(|es| {
            Some(
                Jsonb::Array(
                    es.into_iter()
                        .map(|(k, v)| Jsonb::Array(vec![Jsonb::String(k), v]))
                        .collect(),
                )
                .to_string(),
            )
        }),
        "object_entries_text" => jb(&a[0]).object_entries_named("jsonb_each_text").map(|es| {
            Some(
                Jsonb::Array(
                    es.into_iter()
                        .map(|(k, v)| Jsonb::Array(vec![Jsonb::String(k), text_or_null(&v)]))
                        .collect(),
                )
                .to_string(),
            )
        }),
        "strip_nulls" => some(jb(&a[0]).strip_nulls()),
        "pretty" => some(jb(&a[0]).to_pretty()),
        "as_text_root" => Ok(jb(&a[0]).as_text()),
        "get_key" => Ok(jb(&a[0]).get_key(s(&a[1])).map(|v| v.to_string())),
        "get_key_text" => Ok(jb(&a[0]).get_key(s(&a[1])).and_then(|v| v.as_text())),
        "exists" => some(jb(&a[0]).exists(s(&a[1]))),
        "delete_key" => jb(&a[0]).delete_key(s(&a[1])).map(|v| Some(v.to_string())),
        "get_index" => Ok(jb(&a[0])
            .get_index(a[1].as_i64().unwrap())
            .map(|v| v.to_string())),
        "get_index_text" => Ok(jb(&a[0])
            .get_index(a[1].as_i64().unwrap())
            .and_then(|v| v.as_text())),
        "delete_index" => jb(&a[0])
            .delete_index(a[1].as_i64().unwrap())
            .map(|v| Some(v.to_string())),
        "get_path" => Ok(jb(&a[0]).get_path(&strs(&a[1])).map(|v| v.to_string())),
        "get_path_text" => Ok(jb(&a[0]).get_path(&strs(&a[1])).and_then(|v| v.as_text())),
        "delete_path" => jb(&a[0])
            .delete_path(&strs(&a[1]))
            .map(|v| Some(v.to_string())),
        "set" => jb(&a[0])
            .set(&strs(&a[1]), jb(&a[2]), a[3].as_bool().unwrap())
            .map(|v| Some(v.to_string())),
        "insert" => jb(&a[0])
            .insert(&strs(&a[1]), jb(&a[2]), a[3].as_bool().unwrap())
            .map(|v| Some(v.to_string())),
        "set_opt" => jb(&a[0])
            .set_opt(&opt_strs(&a[1]), Jsonb::parse("1").unwrap(), true)
            .map(|v| Some(v.to_string())),
        "insert_opt" => jb(&a[0])
            .insert_opt(&opt_strs(&a[1]), Jsonb::parse("1").unwrap(), false)
            .map(|v| Some(v.to_string())),
        "delete_path_opt" => jb(&a[0])
            .delete_path_opt(&opt_strs(&a[1]))
            .map(|v| Some(v.to_string())),
        "exists_any" => some(jb(&a[0]).exists_any(&strs(&a[1]))),
        "exists_all" => some(jb(&a[0]).exists_all(&strs(&a[1]))),
        "contains" => some(jb(&a[0]).contains(&jb(&a[1]))),
        "concat" => some(jb(&a[0]).concat(&jb(&a[1]))),
        "cmp" => some(jb(&a[0]).cmp(&jb(&a[1])) as i32),
        "build_object" => {
            let pairs = a
                .chunks(2)
                .map(|c| (s(&c[0]).to_owned(), jb(&c[1])))
                .collect();
            some(Jsonb::build_object(pairs))
        }
        "json_get_key" => json::json_text_get_key(s(&a[0]), s(&a[1])),
        "json_get_key_text" => match json::json_text_get_key(s(&a[0]), s(&a[1]))? {
            Some(raw) => json::json_text_as_text(&raw),
            None => Ok(None),
        },
        "json_get_index" => json::json_text_get_index(s(&a[0]), a[1].as_i64().unwrap()),
        "json_get_index_text" => match json::json_text_get_index(s(&a[0]), a[1].as_i64().unwrap())?
        {
            Some(raw) => json::json_text_as_text(&raw),
            None => Ok(None),
        },
        "json_as_text_root" => json::json_text_as_text(s(&a[0])),
        "escape" => some(json::escape_string(s(&a[0]))),
        "sort" => {
            let vals: Vec<Jsonb> = a[0].as_array().unwrap().iter().map(jb).collect();
            let mut idx: Vec<usize> = (0..vals.len()).collect();
            idx.sort_by(|&x, &y| vals[x].cmp(&vals[y]).then(x.cmp(&y)));
            some(format!(
                "[{}]",
                idx.iter()
                    .map(|i| i.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        }
        "eq_classes" => {
            let vals: Vec<Jsonb> = a[0].as_array().unwrap().iter().map(jb).collect();
            let firsts: Vec<String> = vals
                .iter()
                .map(|v| vals.iter().position(|w| w == v).unwrap().to_string())
                .collect();
            some(format!("[{}]", firsts.join(", ")))
        }
        "depth_set" | "depth_contains" => {
            let (shape, n) = (s(&a[0]), a[1].as_u64().unwrap() as usize);
            let doc = if shape == "obj" {
                "{\"a\":".repeat(n) + "1" + &"}".repeat(n)
            } else {
                "[".repeat(n) + &"]".repeat(n)
            };
            let v = Jsonb::parse(&doc).unwrap();
            if fname == "depth_set" {
                let path = vec![if shape == "obj" { "a" } else { "0" }; n];
                v.set(&path, Jsonb::parse("2").unwrap(), true)
                    .map(|r| Some(r.to_string().len().to_string()))
            } else {
                v.try_contains(&v.clone()).map(|b| Some(b.to_string()))
            }
        }
        "depth" => {
            let (typ, shape, n) = (s(&a[0]), s(&a[1]), a[2].as_u64().unwrap() as usize);
            let doc = match shape {
                "arr" => "[".repeat(n) + &"]".repeat(n),
                "obj" => "{\"a\":".repeat(n) + "1" + &"}".repeat(n),
                _ => "[{\"a\":".repeat(n) + "1" + &"}]".repeat(n),
            };
            if typ == "jsonb" {
                Jsonb::parse(&doc).map(|j| Some(j.to_string().len().to_string()))
            } else {
                json::validate_json(&doc).map(|_| Some(doc.len().to_string()))
            }
        }
        other => panic!("unknown fn {other}"),
    }
}

#[test]
fn json_golden() {
    let mut g = Golden::load("json_golden.jsonl");
    g.run(eval);
    g.finish();
}
