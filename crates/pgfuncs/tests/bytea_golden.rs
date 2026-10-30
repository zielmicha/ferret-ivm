//! Golden tests for pgfuncs::bytea against PostgreSQL 17 output.

mod common;
use common::{Golden, s};
use pgfuncs::bytea;

#[test]
fn bytea_golden() {
    let mut g = Golden::load("bytea_golden.jsonl");
    assert!(g.records.len() > 1000, "too few golden cases");
    g.run(|name, args| {
        let a = s(&args[0]);
        match name {
            "bytea_in" => bytea::parse(a).map(|b| bytea::format(&b)),
            "bytea_out_escape" => bytea::parse(a).map(|b| bytea::format_escape(&b)),
            "encode" => bytea::parse(a).and_then(|b| bytea::encode(&b, s(&args[1]))),
            "decode" => bytea::decode(a, s(&args[1])).map(|b| bytea::format(&b)),
            other => panic!("unknown fn {other}"),
        }.map(Some)
    });
    g.finish();
}
