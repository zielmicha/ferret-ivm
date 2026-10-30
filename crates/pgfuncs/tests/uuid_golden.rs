//! Golden tests for pgfuncs::uuid against PostgreSQL 17 output.

mod common;
use common::{Golden, s};
use pgfuncs::uuid;
use proptest::prelude::*;

#[test]
fn uuid_golden() {
    let mut g = Golden::load("uuid_golden.jsonl");
    assert!(g.records.len() > 1000, "too few golden cases");
    let mut ok_cases = 0;
    g.run(|name, args| {
        assert_eq!(name, "uuid_in");
        uuid::parse(s(&args[0])).map(|v| {
            ok_cases += 1;
            let out = uuid::format(&v);
            assert_eq!(uuid::parse(&out), Ok(v));
            Some(out)
        })
    });
    assert!(ok_cases > 100, "too few successful golden cases");
    g.finish();
}

proptest! {
    #[test]
    fn format_parse_roundtrip(bytes in prop::array::uniform16(any::<u8>())) {
        let s = uuid::format(&bytes);
        prop_assert_eq!(s.len(), 36);
        prop_assert_eq!(uuid::parse(&s), Ok(bytes));
        prop_assert_eq!(uuid::parse(&s.to_uppercase()), Ok(bytes));
        prop_assert_eq!(uuid::parse(&format!("{{{}}}", s.replace('-', ""))), Ok(bytes));
    }
}
