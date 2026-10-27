//! Random schemas, data changes and view queries for differential testing,
//! shared by ivm-difftest (views against PostgreSQL) and the self-differential
//! test, tests/selfdiff.rs (views against the same queries run ad hoc,
//! without PostgreSQL).
//!
//! Queries are deterministic: every order that decides which rows a LIMIT,
//! DISTINCT ON, window function or ordered aggregate sees is total (it ends
//! with the ids of all joined tables), so every correct implementation
//! returns the same rows. Values that compare equal but print differently
//! (numerics of different scales) can still come out in either
//! representation where SQL deduplicates; see [`dedups`].

#![allow(dead_code)]

use crate::testkit::Rng;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Ty {
    Int,
    BigInt,
    Numeric,
    /// unconstrained numeric: values of varying scale
    Decimal,
    Text,
    Bool,
    Date,
    Timestamp,
    Float,
    Jsonb,
    /// int[] with one- and two-dimensional values
    IntArr,
    /// intervals without two equal values of different representation
    Interval,
    /// text of several KB, stored out of line upstream (TOAST)
    Big,
}

impl Ty {
    pub fn sql(&self) -> &'static str {
        match self {
            Ty::Int => "int",
            Ty::BigInt => "bigint",
            Ty::Numeric => "numeric(10,2)",
            Ty::Decimal => "numeric",
            Ty::Text | Ty::Big => "text",
            Ty::Bool => "boolean",
            Ty::Date => "date",
            Ty::Timestamp => "timestamp",
            Ty::Float => "float8",
            Ty::Jsonb => "jsonb",
            Ty::IntArr => "int[]",
            Ty::Interval => "interval",
        }
    }
    pub fn is_num(&self) -> bool {
        matches!(self, Ty::Int | Ty::BigInt | Ty::Numeric | Ty::Decimal | Ty::Float)
    }
    pub fn is_int(&self) -> bool {
        matches!(self, Ty::Int | Ty::BigInt)
    }
    /// min() / max() exist
    pub fn minmax(&self) -> bool {
        *self != Ty::Jsonb
    }
    /// Equal values always print alike (safe inside ordered aggregates).
    pub fn canonical(&self) -> bool {
        matches!(self, Ty::Int | Ty::BigInt | Ty::Numeric | Ty::Text | Ty::Bool | Ty::Date | Ty::Timestamp | Ty::Interval | Ty::Big)
    }
}

#[derive(Clone, Debug)]
pub struct Col {
    pub name: String,
    pub ty: Ty,
    pub nullable: bool,
    /// int column referencing a table's id (possibly its own)
    pub refs: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct Table {
    pub name: String,
    pub cols: Vec<Col>,
    pub next_id: i64,
}

impl Table {
    /// The column referencing this table's own id, if any.
    pub fn self_ref(&self, me: usize) -> Option<&Col> {
        self.cols.iter().find(|c| c.refs == Some(me))
    }
}

pub fn literal(rng: &mut Rng, ty: Ty) -> String {
    match ty {
        Ty::Int => rng.range(-5, 40).to_string(),
        Ty::BigInt => rng.range(-1000, 1000).to_string(),
        Ty::Numeric => format!("{}.{:02}", rng.range(-100, 100), rng.below(100)),
        Ty::Decimal => rng.pick(&["1", "1.0", "1.50", "-2.125", "0.000", "3", "10.5", "-0.25", "2.00", "7.0001", "-4"]).to_string(),
        Ty::Text => {
            let w = rng.pick(&["a", "b", "foo", "bar", "Baz", "", "ü", "a b", "zz", "A"]).to_string();
            if rng.chance(30) { format!("'{w}{}'", rng.below(5)) } else { format!("'{w}'") }
        }
        Ty::Bool => (if rng.chance(50) { "true" } else { "false" }).into(),
        Ty::Date => format!("date '2024-01-01' + {}", rng.below(60)),
        Ty::Timestamp => format!("timestamp '2024-01-01 00:00:00' + interval '{} minutes'", rng.below(100000)),
        Ty::Float => format!("{}::float8", rng.pick(&["0", "1.5", "-2.25", "3.75", "100", "0.1", "-0.5", "7"])),
        Ty::Jsonb => {
            // integers only: equal jsonb numbers of different scale would make
            // PostgreSQL's choice of representation nondeterministic
            let (n, m) = (rng.range(0, 6), rng.range(0, 6));
            let tags = rng.pick(&[r#"["x", "y"]"#, r#"["y"]"#, "[]", r#"["x", "z", "x"]"#]).to_string();
            let docs = [
                format!(r#"'{{"a": {n}, "t": {tags}, "items": [{{"p": {m}}}, {{"p": {n}, "q": "s{m}"}}]}}'"#),
                format!(r#"'{{"a": {n}, "items": []}}'"#),
                format!(r#"'{{"t": {tags}}}'"#),
                format!("'[{n}, {m}]'"),
                format!(r#"'{{"a": {{"b": [{n}, {{"c": {m}}}]}}}}'"#),
                "'null'".to_string(),
            ];
            rng.pick(&docs).clone()
        }
        Ty::IntArr => {
            let (a, b, c, d) = (rng.range(0, 5), rng.range(0, 5), rng.range(0, 5), rng.range(0, 5));
            let arrs = [
                format!("'{{{a},{b}}}'"),
                format!("'{{{a},{b},{c}}}'"),
                format!("'{{{{{a},{b}}},{{{c},{d}}}}}'"),
                "'{}'".to_string(),
                format!("'{{{a},NULL}}'"),
                format!("'[0:1]={{{a},{b}}}'"),
                format!("'{{{{{a}}},{{{b}}}}}'"),
                format!("'{{{a}}}'"),
            ];
            rng.pick(&arrs).clone()
        }
        Ty::Interval => {
            format!("interval '{}'", rng.pick(&["1 hour", "90 minutes", "2 days", "3 mons", "1 year", "-1 day", "00:00:01.5", "0"]))
        }
        Ty::Big => format!("repeat(md5('{}'), {})", rng.below(12), rng.range(80, 300)),
    }
}

pub fn value(rng: &mut Rng, c: &Col, tables: &[Table]) -> String {
    if c.nullable && rng.chance(12) {
        return "NULL".into();
    }
    if let Some(t) = c.refs {
        let max = tables[t].next_id.max(2);
        return rng.range(1, max + 2).to_string();
    }
    literal(rng, c.ty)
}

pub fn gen_schema(rng: &mut Rng) -> Vec<Table> {
    let n = 3 + rng.below(2) as usize;
    let mut tables: Vec<Table> = Vec::new();
    for i in 0..n {
        let mut cols = vec![Col { name: "id".into(), ty: Ty::Int, nullable: false, refs: None }];
        let ncols = 2 + rng.below(4) as usize;
        for j in 0..ncols {
            let tys = [
                Ty::Int,
                Ty::BigInt,
                Ty::Numeric,
                Ty::Decimal,
                Ty::Text,
                Ty::Text,
                Ty::Bool,
                Ty::Date,
                Ty::Timestamp,
                Ty::Float,
                Ty::Int,
                Ty::Jsonb,
                Ty::IntArr,
                Ty::Interval,
            ];
            let ty = *rng.pick(&tys);
            cols.push(Col { name: format!("c{j}"), ty, nullable: rng.chance(60), refs: None });
        }
        if rng.chance(20) {
            cols.push(Col { name: "big".into(), ty: Ty::Big, nullable: rng.chance(30), refs: None });
        }
        // foreign-key-like columns to earlier tables, and to itself (trees)
        if i > 0 {
            for k in 0..(1 + rng.below(2) as usize) {
                let target = rng.below(i as u64) as usize;
                cols.push(Col { name: format!("r{k}"), ty: Ty::Int, nullable: rng.chance(30), refs: Some(target) });
            }
        }
        if rng.chance(30) {
            cols.push(Col { name: "pr".into(), ty: Ty::Int, nullable: true, refs: Some(i) });
        }
        tables.push(Table { name: format!("t{i}"), cols, next_id: 1 });
    }
    tables
}

pub fn create_table_sql(t: &Table) -> String {
    let cols: Vec<String> =
        t.cols.iter().map(|c| format!("{} {}{}", c.name, c.ty.sql(), if c.name == "id" { " PRIMARY KEY" } else { "" })).collect();
    format!("CREATE TABLE {} ({})", t.name, cols.join(", "))
}

/// PostgreSQL only: store large values out of line, so that updates leave
/// unchanged-TOAST placeholders in the replication stream.
pub fn toast_sql(t: &Table) -> Vec<String> {
    t.cols.iter().filter(|c| c.ty == Ty::Big).map(|c| format!("ALTER TABLE {} ALTER COLUMN {} SET STORAGE EXTERNAL", t.name, c.name)).collect()
}

pub fn insert_sql(rng: &mut Rng, tables: &mut [Table], ti: usize) -> String {
    let id = tables[ti].next_id;
    tables[ti].next_id += 1;
    insert_with_id(rng, tables, ti, id, "")
}

fn insert_with_id(rng: &mut Rng, tables: &[Table], ti: usize, id: i64, suffix: &str) -> String {
    let t = &tables[ti];
    let vals: Vec<String> = t.cols.iter().map(|c| if c.name == "id" { id.to_string() } else { value(rng, c, tables) }).collect();
    format!("INSERT INTO {} VALUES ({}){suffix};", t.name, vals.join(", "))
}

/// The statements of one random upstream transaction. `local`: only what the
/// engine accepts on its own tables (no TRUNCATE, no ON CONFLICT DO UPDATE).
pub fn transaction(rng: &mut Rng, tables: &mut [Table], local: bool) -> Vec<String> {
    let mut out = Vec::new();
    for _ in 0..(1 + rng.below(8)) {
        let ti = rng.below(tables.len() as u64) as usize;
        let max = tables[ti].next_id.max(2);
        match rng.below(20) {
            0..=7 => out.push(insert_sql(rng, tables, ti)),
            8..=12 => {
                let t = &tables[ti];
                let c = rng.pick(&t.cols[1..]).clone();
                let v = value(rng, &c, tables);
                out.push(format!("UPDATE {} SET {} = {v} WHERE id % {} = {};", t.name, c.name, 2 + rng.below(5), rng.below(3)));
            }
            13 => out.push(format!("UPDATE {} SET id = id + 100000 WHERE id = {};", tables[ti].name, rng.range(1, max))),
            14..=15 => out.push(format!("DELETE FROM {} WHERE id % {} = {};", tables[ti].name, 5 + rng.below(7), rng.below(5))),
            16 => {
                // the same row inserted, changed and maybe deleted again
                let id = tables[ti].next_id;
                tables[ti].next_id += 1;
                out.push(insert_with_id(rng, tables, ti, id, ""));
                let t = &tables[ti];
                let c = rng.pick(&t.cols[1..]).clone();
                out.push(format!("UPDATE {} SET {} = {} WHERE id = {id};", t.name, c.name, value(rng, &c, tables)));
                if rng.chance(50) {
                    out.push(format!("DELETE FROM {} WHERE id = {id};", t.name));
                }
            }
            17 => {
                // a row deleted and inserted again under the same key
                let id = rng.range(1, max);
                out.push(format!("DELETE FROM {} WHERE id = {id};", tables[ti].name));
                out.push(insert_with_id(rng, tables, ti, id, ""));
            }
            18 => {
                // many primary keys change at once
                out.push(format!("UPDATE {} SET id = id + 1000000 WHERE id % 5 = {};", tables[ti].name, rng.below(5)));
            }
            _ => {
                let id = rng.range(1, max);
                if local {
                    out.push(insert_with_id(rng, tables, ti, id, " ON CONFLICT DO NOTHING"));
                } else if rng.chance(10) {
                    out.push(format!("TRUNCATE {};", tables[ti].name));
                } else {
                    let t = &tables[ti];
                    let c = rng.pick(&t.cols[1..]).clone();
                    let suffix = format!(" ON CONFLICT (id) DO UPDATE SET {0} = EXCLUDED.{0}", c.name);
                    out.push(insert_with_id(rng, tables, ti, id, &suffix));
                }
            }
        }
    }
    out
}

/// Columns of `reps`, a table for sinks (no generated view reads it) whose
/// values come in several representations of equal values, besides values
/// alike in text and the same value written differently: name, type, values.
pub const REPS: &[(&str, &str, &[&str])] = &[
    ("iv", "interval", &["1 mon", "30 days", "29 days 24:00:00", "1 day", "24:00:00"]),
    ("ivs", "interval[]", &["{1 mon,NULL}", "{30 days,NULL}", "{{1 mon},{1 day}}", "{{30 days},{24:00:00}}", "[0:0]={1 mon}"]),
    ("n", "numeric", &["1.5", "1.50", "0", "0.000", "NaN"]),
    ("ns", "numeric[]", &["{1.5,NULL}", "{1.50,NULL}", "{{0},{NaN}}"]),
    ("f4", "real", &["0", "-0", "NaN", "1.5"]),
    ("f8", "float8", &["0", "-0", "NaN", "0.1", "0.1000000000000001"]),
    ("f8s", "float8[]", &["{0,NaN}", "{-0,NaN}", "{NULL,-0}"]),
    ("jb", "jsonb", &[r#"{"a": 1}"#, r#"{"a": 1.0}"#, "[1.50]", "[1.5]"]),
    ("jbs", "jsonb[]", &[r#"{"{\"a\": 1}"}"#, r#"{"{\"a\": 1.0}"}"#]),
    ("j", "json", &[r#"{"a": 1}"#, r#"{"a":1}"#, r#"{"a": 1.0}"#]),
    ("js", "json[]", &[r#"{"{\"a\": 1}"}"#, r#"{"{\"a\":1}"}"#]),
    ("jp", "jsonpath", &["$.a", r#"$."a""#, "$.b"]),
    ("b", "bpchar", &["a", "a ", "a  "]),
    ("bs", "bpchar[]", &["{a}", r#"{"a "}"#, r#"{"a  ",NULL}"#]),
    ("c", "char(3)", &["a", "a  ", "ab"]),
    ("tx", "text", &["\u{e4}", "a\u{308}", "A", "a"]),
    ("txs", "text[]", &["{\u{e4}}", "{a\u{308}}", "{{A},{a}}"]),
    ("ts", "timestamp", &["2024-01-01 00:00:00", "2024-01-01 00:00:00.000001"]),
    ("tz", "timestamptz", &["2024-01-01 05:45:00+05:45", "2024-01-01 00:00:00+00", "2024-01-01 00:00:00+01"]),
];

/// Materialized views over `reps` and the key of their sinks (keys of
/// several representations: the group shows one of them, which changes).
pub const REP_VIEWS: &[(&str, &str, Option<&str>)] = &[
    ("reps_all", "SELECT iv, ivs, n, ns, f4, f8, f8s, jb, jbs, j, js, jp, b, bs, c, tx, txs, ts, tz FROM reps", None),
    ("reps_iv", "SELECT iv, count(*) AS cnt, sum(id) AS ids FROM reps WHERE iv IS NOT NULL GROUP BY iv", Some("iv")),
    ("reps_n", "SELECT n, f8, count(*) AS cnt FROM reps WHERE n IS NOT NULL AND f8 IS NOT NULL GROUP BY n, f8", Some("n, f8")),
];

pub fn reps_table_sql() -> String {
    let cols: Vec<String> = REPS.iter().map(|(c, ty, _)| format!("{c} {ty}")).collect();
    format!("CREATE TABLE reps (id int PRIMARY KEY, {})", cols.join(", "))
}

fn rep_value(rng: &mut Rng, col: usize) -> String {
    let (_, ty, vals) = REPS[col];
    format!("'{}'::{ty}", rng.pick(vals))
}

/// An INSERT of a row of `reps` with one value (rows of one column make
/// equal rows of different representations common), or values of `n` and
/// `f8` (the key of `reps_n`).
pub fn reps_insert(rng: &mut Rng, id: i64) -> String {
    let mut vals = vec!["NULL".to_string(); REPS.len()];
    let cols = if rng.chance(25) { vec![2, 5] } else { vec![rng.below(REPS.len() as u64) as usize] };
    for c in cols {
        vals[c] = rep_value(rng, c);
    }
    format!("INSERT INTO reps VALUES ({id}, {});", vals.join(", "))
}

/// Statements of a random transaction on `reps`: rows inserted, values
/// changed to another representation, rows deleted.
pub fn reps_transaction(rng: &mut Rng, next_id: &mut i64) -> Vec<String> {
    let mut out = Vec::new();
    for _ in 0..(1 + rng.below(5)) {
        match rng.below(4) {
            0 | 1 => {
                for _ in 0..(1 + rng.below(4)) {
                    *next_id += 1;
                    out.push(reps_insert(rng, *next_id));
                }
            }
            2 => {
                let c = rng.below(REPS.len() as u64) as usize;
                let (v, name) = (rep_value(rng, c), REPS[c].0);
                out.push(format!("UPDATE reps SET {name} = {v} WHERE {name} IS NOT NULL AND id % {} = {};", 2 + rng.below(3), rng.below(2)));
            }
            // (a few rows: as many as inserted, on average)
            _ => out.push(format!("DELETE FROM reps WHERE id % {} = {};", 8 + rng.below(8), rng.below(8))),
        }
    }
    out
}

/// Whether `q` deduplicates equal values, where an implementation may keep
/// any of their representations.
pub fn dedups(q: &str) -> bool {
    let upper = q.to_uppercase();
    ["GROUP BY", "DISTINCT", "UNION", "INTERSECT", "EXCEPT", "MIN(", "MAX(", "MODE(", "LIMIT", "FETCH", "PERCENTILE_DISC"]
        .iter()
        .any(|k| upper.contains(k))
}

/// A table in scope for query generation.
#[derive(Clone)]
pub struct Src {
    pub alias: String,
    pub table: usize,
}

pub struct Gen<'a> {
    pub rng: &'a mut Rng,
    pub tables: &'a [Table],
}

impl Gen<'_> {
    fn col_of(&mut self, srcs: &[Src], pred: impl Fn(&Col) -> bool) -> Option<(String, Ty)> {
        let mut cands = Vec::new();
        for s in srcs {
            for c in &self.tables[s.table].cols {
                if pred(c) {
                    cands.push((format!("{}.{}", s.alias, c.name), c.ty));
                }
            }
        }
        if cands.is_empty() { None } else { Some(self.rng.pick(&cands).clone()) }
    }

    /// The ids of all sources: a total order over joined rows.
    fn ids(srcs: &[Src]) -> String {
        srcs.iter().map(|s| format!("{}.id", s.alias)).collect::<Vec<_>>().join(", ")
    }

    fn expr(&mut self, srcs: &[Src], depth: u32) -> (String, Ty) {
        let (c, ty) = self.col_of(srcs, |_| true).unwrap();
        if depth > 1 || self.rng.chance(50) {
            return (c, ty);
        }
        if self.rng.chance(15) {
            // conditional expressions
            let p = self.pred(srcs, 1);
            return match self.rng.below(4) {
                0 => (format!("CASE WHEN {p} THEN {c} ELSE NULL END"), ty),
                1 if ty.is_num() => {
                    let (d, _) = self.col_of(srcs, |x| x.ty == ty).unwrap();
                    (format!("GREATEST({c}, {d})"), ty)
                }
                2 if ty.is_num() => (format!("NULLIF({c}, {})", literal(self.rng, ty)), ty),
                _ => (format!("CASE WHEN {p} THEN 'yes' WHEN {c} IS NULL THEN 'null' ELSE 'no' END"), Ty::Text),
            };
        }
        match ty {
            Ty::Int | Ty::BigInt => match self.rng.below(5) {
                0 => (format!("({c} + {})", self.rng.range(1, 9)), ty),
                1 => (format!("({c} * 2)"), ty),
                2 => (format!("abs({c})"), ty),
                3 => (format!("coalesce({c}, 0)"), ty),
                _ => (format!("({c} % 7)"), ty),
            },
            Ty::Numeric | Ty::Decimal => match self.rng.below(5) {
                0 => (format!("round({c}, 1)"), ty),
                1 => (format!("({c} * 3)"), ty),
                2 => (format!("({c} - 1.5)"), ty),
                3 => (format!("({c} / 7)"), ty),
                _ => (format!("coalesce({c}, 0)"), ty),
            },
            Ty::Float => match self.rng.below(3) {
                0 => (format!("({c} * 2)"), ty),
                1 => (format!("({c} / 4)"), ty),
                _ => (format!("floor({c})"), ty),
            },
            Ty::Text => match self.rng.below(5) {
                0 => (format!("lower({c})"), ty),
                1 => (format!("upper({c})"), ty),
                2 => (format!("({c} || 'x')"), ty),
                3 => (format!("length({c})"), Ty::Int),
                _ => (format!("substr({c}, 1, 2)"), ty),
            },
            Ty::Big => match self.rng.below(4) {
                0 => (format!("length({c})"), Ty::Int),
                1 => (format!("md5({c})"), Ty::Text),
                2 => (format!("left({c}, 6)"), Ty::Text),
                _ => (format!("substr({c}, 30, 5)"), Ty::Text),
            },
            Ty::Bool => (format!("(NOT {c})"), ty),
            Ty::Date => match self.rng.below(3) {
                0 => (format!("({c} + 3)"), ty),
                1 => (format!("extract(day from {c})"), Ty::Numeric),
                _ => (format!("date_trunc('month', {c})"), Ty::Timestamp),
            },
            Ty::Timestamp => match self.rng.below(2) {
                0 => (format!("date_trunc('day', {c})"), ty),
                _ => (format!("({c} + interval '1 hour')"), ty),
            },
            Ty::Interval => match self.rng.below(3) {
                0 => (format!("({c} + interval '1 day')"), ty),
                1 => (format!("({c} * 2)"), ty),
                _ => (format!("extract(epoch from {c})"), Ty::Numeric),
            },
            Ty::Jsonb => match self.rng.below(7) {
                0 => (format!("jsonb_path_query_array({c}, '$.items[*].p')"), Ty::Jsonb),
                1 => (format!("jsonb_path_query_first({c}, '$.items[*] ? (@.p > 2).p')"), Ty::Jsonb),
                2 => (format!(r#"({c} @? '$.t[*] ? (@ == "x")')"#), Ty::Bool),
                3 => (format!("jsonb_path_exists({c}, 'strict $.items[1]', silent => true)"), Ty::Bool),
                4 => (format!("jsonb_path_match({c}, '$.a > 2', silent => true)"), Ty::Bool),
                5 => (format!(r#"jsonb_path_query_array({c}, '$.** ? (@.type() == "number")')"#), Ty::Jsonb),
                _ => (format!("({c} -> 'a')"), Ty::Jsonb),
            },
            Ty::IntArr => match self.rng.below(7) {
                0 => (format!("cardinality({c})"), Ty::Int),
                1 => (format!("{c}[1]"), Ty::Int),
                2 => (format!("{c}[2][1]"), Ty::Int),
                3 => (format!("{c}[1:1]"), Ty::IntArr),
                4 => (format!("array_ndims({c})"), Ty::Int),
                5 => (format!("array_to_string({c}, ',')"), Ty::Text),
                _ => (format!("({c} || ARRAY[9])"), Ty::IntArr),
            },
        }
    }

    fn pred(&mut self, srcs: &[Src], depth: u32) -> String {
        if depth < 2 && self.rng.chance(20) {
            let a = self.pred(srcs, depth + 1);
            let b = self.pred(srcs, depth + 1);
            return if self.rng.chance(50) { format!("({a} AND {b})") } else { format!("({a} OR {b})") };
        }
        if depth < 2 && self.rng.chance(8) {
            return format!("NOT {}", self.pred(srcs, depth + 1));
        }
        let (c, ty) = self.col_of(srcs, |_| true).unwrap();
        match self.rng.below(10) {
            0 => format!("{c} IS NULL"),
            1 => format!("{c} IS NOT NULL"),
            2 if ty != Ty::Bool => {
                let a = literal(self.rng, ty);
                let b = literal(self.rng, ty);
                format!("{c} IN ({a}, {b})")
            }
            3 if ty.is_num() => format!("{c} BETWEEN {} AND {}", literal(self.rng, ty), literal(self.rng, ty)),
            4 if ty == Ty::Text => format!("{c} LIKE '{}%'", self.rng.pick(&["a", "b", "f", "B", ""])),
            4 if ty == Ty::Big => format!("{c} LIKE '{}%'", self.rng.pick(&["c", "e", "4", "a8"])),
            5 if srcs.len() > 1 => {
                // compare columns of two sources (NULLs on either side)
                match self.col_of(srcs, |x| x.ty == ty && ty.minmax() && ty != Ty::Big) {
                    Some((d, _)) if d != c => format!("{c} {} {d}", self.rng.pick(&["=", "<>", "<", ">=", "IS DISTINCT FROM", "IS NOT DISTINCT FROM"])),
                    _ => format!("{c} IS NOT NULL"),
                }
            }
            _ if ty == Ty::Bool => c,
            _ => {
                let op = *self.rng.pick(&["=", "<>", "<", "<=", ">", ">="]);
                format!("{c} {op} {}", literal(self.rng, ty))
            }
        }
    }

    /// FROM clause with joins; returns (sql, sources).
    fn from(&mut self, prefix: &str) -> (String, Vec<Src>) {
        let n = 1 + self.rng.below(3) as usize;
        let t0 = self.rng.below(self.tables.len() as u64) as usize;
        let mut srcs = vec![Src { alias: format!("{prefix}0"), table: t0 }];
        let mut sql = format!("{} {prefix}0", self.tables[t0].name);
        for k in 1..n {
            // join along a reference if possible
            let mut cands: Vec<(usize, String)> = Vec::new();
            for (ti, t) in self.tables.iter().enumerate() {
                for c in &t.cols {
                    if let Some(target) = c.refs {
                        for s in &srcs {
                            if s.table == target {
                                cands.push((ti, format!("{prefix}{k}.{} = {}.id", c.name, s.alias)));
                            }
                            if s.table == ti {
                                cands.push((target, format!("{}.{} = {prefix}{k}.id", s.alias, c.name)));
                            }
                        }
                    }
                }
            }
            let ti = self.rng.below(self.tables.len() as u64) as usize;
            let other = format!("{prefix}{k}");
            let (ti, on) = match self.rng.below(10) {
                // equal values of any (nullable) columns: NULL keys never match
                0 | 1 => {
                    let mine = self.tables[ti].cols.clone();
                    let theirs = self.col_of(&srcs, |c| c.name != "id" && c.ty.minmax() && c.ty != Ty::Big && mine.iter().any(|m| m.name != "id" && m.ty == c.ty));
                    match theirs {
                        Some((tc, tty)) => {
                            let cands: Vec<&Col> = mine.iter().filter(|m| m.name != "id" && m.ty == tty).collect();
                            let mc = &self.rng.pick(&cands).name;
                            let op = if self.rng.chance(20) { "IS NOT DISTINCT FROM" } else { "=" };
                            (ti, format!("{other}.{mc} {op} {tc}"))
                        }
                        None => (ti, format!("{other}.id = {}.id", srcs[0].alias)),
                    }
                }
                // an inequality join
                2 if self.rng.chance(40) => (ti, format!("{other}.id < {}.id + 2 AND {other}.id > {}.id - 2", srcs[0].alias, srcs[0].alias)),
                _ if !cands.is_empty() => self.rng.pick(&cands).clone(),
                _ => (ti, format!("{other}.id = {}.id", srcs[0].alias)),
            };
            let kind = *self.rng.pick(&["JOIN", "JOIN", "LEFT JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"]);
            sql.push_str(&format!(" {kind} {} {other} ON {on}", self.tables[ti].name));
            if self.rng.chance(15) {
                let extra = self.pred(&[srcs[0].clone()], 1);
                sql.push_str(&format!(" AND {extra}"));
            }
            srcs.push(Src { alias: other, table: ti });
        }
        (sql, srcs)
    }

    /// A correlation between subquery alias `sq` (table `t`) and the outer
    /// sources: along a reference, else on ids.
    fn correlation(&self, t: usize, srcs: &[Src]) -> String {
        let mut corr = format!("sq.id = {}.id", srcs[0].alias);
        for c in &self.tables[t].cols {
            if let Some(target) = c.refs
                && let Some(s) = srcs.iter().find(|s| s.table == target)
            {
                corr = format!("sq.{} = {}.id", c.name, s.alias);
            }
        }
        corr
    }

    fn subquery_pred(&mut self, srcs: &[Src]) -> String {
        let t = self.rng.below(self.tables.len() as u64) as usize;
        let inner = Src { alias: "sq".into(), table: t };
        let corr = self.correlation(t, srcs);
        let extra = if self.rng.chance(50) { format!(" AND {}", self.pred(std::slice::from_ref(&inner), 1)) } else { String::new() };
        let tn = self.tables[t].name.clone();
        let outer_int = |g: &mut Self| g.col_of(srcs, |c| c.ty == Ty::Int).unwrap_or((format!("{}.id", srcs[0].alias), Ty::Int)).0;
        // a nullable int column of the subquery table, if any
        let inner_nullable = self.tables[t].cols.iter().filter(|c| c.ty == Ty::Int && c.nullable).map(|c| c.name.clone()).next();
        match self.rng.below(13) {
            0 => format!("EXISTS (SELECT 1 FROM {tn} sq WHERE {corr}{extra})"),
            1 => format!("NOT EXISTS (SELECT 1 FROM {tn} sq WHERE {corr}{extra})"),
            2 => format!("{} IN (SELECT sq.id FROM {tn} sq WHERE true{extra})", outer_int(self)),
            3 => format!("{} NOT IN (SELECT sq.id FROM {tn} sq WHERE sq.id IS NOT NULL{extra})", outer_int(self)),
            5 => format!("{} = ANY (SELECT sq.id FROM {tn} sq WHERE true{extra})", outer_int(self)),
            // correlated and possibly empty: ALL over nothing is true
            6 => format!("{} > ALL (SELECT sq.id FROM {tn} sq WHERE {corr}{extra})", outer_int(self)),
            7 => {
                let a = &srcs[0].alias;
                let not = if self.rng.chance(50) { "NOT " } else { "" };
                format!("({a}.id, {a}.id % 3) {not}IN (SELECT sq.id, sq.id % 3 FROM {tn} sq WHERE true{extra})")
            }
            // NOT IN over a column with NULLs: never true once a NULL is in
            8 => match inner_nullable {
                Some(c) => format!("{} NOT IN (SELECT sq.{c} FROM {tn} sq WHERE true{extra})", outer_int(self)),
                None => format!("{} <> ALL (SELECT sq.id FROM {tn} sq WHERE {corr})", outer_int(self)),
            },
            // a nullable left side
            9 => match self.col_of(srcs, |c| c.ty == Ty::Int && c.nullable) {
                Some((c, _)) => format!("{c} {}IN (SELECT sq.id FROM {tn} sq WHERE true{extra})", if self.rng.chance(50) { "NOT " } else { "" }),
                None => format!("EXISTS (SELECT 1 FROM {tn} sq WHERE {corr})"),
            },
            // a correlated scalar aggregate (NULL when nothing matches)
            10 => match self.col_of(srcs, |c| c.ty.is_num() && c.ty != Ty::Float) {
                Some((c, cty)) => {
                    let inner_col = self.tables[t].cols.iter().find(|x| x.ty == cty).map(|x| x.name.clone()).unwrap_or_else(|| "id".into());
                    let agg = *self.rng.pick(&["max", "min", "sum"]);
                    format!("{c} >= (SELECT {agg}(sq.{inner_col}) FROM {tn} sq WHERE {corr})")
                }
                None => format!("EXISTS (SELECT 1 FROM {tn} sq WHERE {corr})"),
            },
            // correlated on an inequality
            11 => format!("EXISTS (SELECT 1 FROM {tn} sq WHERE sq.id < {}.id AND sq.id > {}.id - 3{extra})", srcs[0].alias, srcs[0].alias),
            12 => format!("{} <> ANY (SELECT sq.id FROM {tn} sq WHERE {corr})", outer_int(self)),
            _ => format!("(SELECT count(*) FROM {tn} sq WHERE {corr}{extra}) > {}", self.rng.below(3)),
        }
    }

    /// An aggregate call over the sources (with the ids for ordered ones).
    fn aggregate(&mut self, srcs: &[Src]) -> String {
        let (c, ty) = self.col_of(srcs, |_| true).unwrap();
        let ids = Self::ids(srcs);
        match self.rng.below(22) {
            0 => "count(*)".to_string(),
            1 => format!("count({c})"),
            2 if ty.is_num() => format!("sum({c})"),
            3 if ty.is_num() && ty != Ty::Float => format!("avg({c})"),
            4 if ty.minmax() => format!("min({c})"),
            5 if ty.minmax() => format!("max({c})"),
            6 => format!("count(DISTINCT {c})"),
            7 if ty == Ty::Bool => format!("bool_and({c})"),
            8 if ty == Ty::Text => format!("string_agg({c}, ',' ORDER BY {c})"),
            // arrays accumulate into one more dimension (equal shapes only)
            8 if ty == Ty::IntArr => format!(
                "array_agg({c} ORDER BY {c}, {ids}) FILTER (WHERE array_ndims({c}) = 1 AND array_lower({c}, 1) = 1 AND cardinality({c}) = 2)"
            ),
            9 if ty.minmax() && ty != Ty::Big => format!("mode() WITHIN GROUP (ORDER BY {c})"),
            10 if ty == Ty::Bool => format!("bool_or({c})"),
            10 if ty.is_num() && ty != Ty::Float => format!("sum(DISTINCT {c})"),
            11 if ty == Ty::Bool => format!("every({c})"),
            11 if ty.is_int() || ty == Ty::Numeric => format!("{}({c})", self.rng.pick(&["stddev_samp", "var_pop", "variance", "stddev"])),
            12 => {
                let p = self.pred(srcs, 1);
                format!("count(*) FILTER (WHERE {p})")
            }
            13 if ty.is_num() => {
                let p = self.pred(srcs, 1);
                format!("sum({c}) FILTER (WHERE {p})")
            }
            14 if ty.minmax() && ty != Ty::Big => format!("percentile_disc({}) WITHIN GROUP (ORDER BY {c})", self.rng.pick(&["0.5", "0.25", "1"])),
            15 if ty.is_num() => format!("percentile_cont({}) WITHIN GROUP (ORDER BY {c})", self.rng.pick(&["0.5", "0.1", "0"])),
            16 if ty.canonical() && ty != Ty::Big => format!("array_agg({c} ORDER BY {c} NULLS FIRST, {ids})"),
            17 if ty.canonical() && ty != Ty::Big => format!("jsonb_agg({c} ORDER BY {c}, {ids})"),
            18 if ty.canonical() && ty != Ty::Big => format!("json_agg({c} ORDER BY {c} DESC, {ids})"),
            19 if ty.is_num() && ty != Ty::Float => format!("avg(DISTINCT {c})"),
            20 if ty.minmax() => {
                let p = self.pred(srcs, 1);
                format!("max({c}) FILTER (WHERE {p})")
            }
            21 if ty == Ty::Text => format!("string_agg(DISTINCT {c}, '|' ORDER BY {c})"),
            _ => format!("count({c})"),
        }
    }

    pub fn query(&mut self) -> String {
        let (from, srcs) = self.from("a");
        let mut wh = Vec::new();
        for _ in 0..self.rng.below(3) {
            wh.push(self.pred(&srcs, 0));
        }
        if self.rng.chance(25) {
            wh.push(self.subquery_pred(&srcs));
        }
        let where_sql = if wh.is_empty() { String::new() } else { format!(" WHERE {}", wh.join(" AND ")) };
        let shape = self.rng.below(16);
        if shape < 4 {
            return self.aggregation(&from, &srcs, &where_sql);
        }
        match shape {
            10 => return self.distinct_on(),
            11 => {
                if let Some(q) = self.recursive() {
                    return q;
                }
            }
            12 => return self.table_functions(&srcs, &from, &where_sql),
            _ => {}
        }
        // projection
        let n = 1 + self.rng.below(4) as usize;
        let mut sel = Vec::new();
        for _ in 0..n {
            let (e, _) = self.expr(&srcs, 0);
            sel.push(e);
        }
        let mut window_clause = String::new();
        if shape == 4 {
            let (w, clause) = self.window(&srcs);
            sel.push(w);
            window_clause = clause;
        }
        if shape == 7 {
            self.select_list_subqueries(&srcs, &mut sel);
        }
        let sel_aliased: Vec<String> = sel.iter().enumerate().map(|(i, e)| format!("{e} AS x{i}")).collect();
        let distinct = if self.rng.chance(20) { "DISTINCT " } else { "" };
        let mut q = format!("SELECT {distinct}{} FROM {from}{where_sql}{window_clause}", sel_aliased.join(", "));
        if shape == 8 {
            // derived table / CTE wrapping an aggregate
            let inner = q.clone();
            let cols: Vec<String> = (0..sel.len()).map(|i| format!("x{i}")).collect();
            return if self.rng.chance(50) {
                format!("WITH w AS ({inner}) SELECT {}, count(*) AS n FROM w GROUP BY {}", cols.join(", "), cols.join(", "))
            } else {
                format!("SELECT d.*, 1 AS one FROM ({inner}) d WHERE d.x0 IS NOT NULL")
            };
        }
        if shape == 9 {
            // LATERAL: the next row (top-1), or an aggregate over related rows
            let t = self.rng.below(self.tables.len() as u64) as usize;
            let tn = self.tables[t].name.clone();
            let a0 = &srcs[0].alias;
            return if self.rng.chance(50) {
                format!(
                    "SELECT {}, l.id AS lid FROM {from} LEFT JOIN LATERAL (SELECT sq.id FROM {tn} sq WHERE sq.id > {a0}.id ORDER BY sq.id LIMIT 1) l ON true{where_sql}",
                    sel_aliased.join(", ")
                )
            } else {
                let corr = self.correlation(t, &srcs);
                format!(
                    "SELECT {}, l.n AS ln, l.m AS lm FROM {from} CROSS JOIN LATERAL (SELECT count(*) AS n, max(sq.id) AS m FROM {tn} sq WHERE {corr}) l{where_sql}",
                    sel_aliased.join(", ")
                )
            };
        }
        if shape == 5 && distinct.is_empty() && self.rng.chance(30) {
            // ties on the first column are all kept, so the result is determined
            q.push_str(&format!(" ORDER BY 1 NULLS FIRST FETCH FIRST {} ROWS WITH TIES", 1 + self.rng.below(6)));
        } else if shape == 5 && distinct.is_empty() {
            // top-k with a deterministic order over all output columns
            let ord: Vec<String> = (0..sel.len()).map(|i| format!("{} NULLS FIRST", i + 1)).collect();
            let offset = if self.rng.chance(30) { format!(" OFFSET {}", self.rng.below(4)) } else { String::new() };
            q.push_str(&format!(" ORDER BY {} LIMIT {}{offset}", ord.join(", "), 1 + self.rng.below(10)));
        } else if shape == 6 {
            q = self.set_operation(q, &sel, &srcs);
        }
        q
    }

    fn aggregation(&mut self, from: &str, srcs: &[Src], where_sql: &str) -> String {
        let mut group = Vec::new();
        for _ in 0..self.rng.below(3) {
            let (e, _) = self.expr(srcs, 1);
            if !group.contains(&e) {
                group.push(e);
            }
        }
        let mut sel: Vec<String> = group.clone();
        for _ in 0..(1 + self.rng.below(3)) {
            let a = self.aggregate(srcs);
            sel.push(a);
        }
        let group_sql = if group.is_empty() {
            String::new()
        } else if self.rng.chance(30) {
            // grouping sets, with grouping() telling the sets apart
            sel.push(format!("grouping({})", group.join(", ")));
            match self.rng.below(3) {
                0 => format!(" GROUP BY ROLLUP ({})", group.join(", ")),
                1 => format!(" GROUP BY CUBE ({})", group.join(", ")),
                _ => format!(" GROUP BY GROUPING SETS (({}), ())", group.join(", ")),
            }
        } else {
            format!(" GROUP BY {}", group.join(", "))
        };
        let sel_aliased: Vec<String> = sel.iter().enumerate().map(|(i, e)| format!("{e} AS x{i}")).collect();
        let having = if !group.is_empty() && self.rng.chance(30) {
            match self.rng.below(3) {
                0 => " HAVING count(*) > 1".to_string(),
                1 => format!(" HAVING {} IS NOT NULL", self.aggregate(srcs)),
                _ => format!(" HAVING count(DISTINCT {}.id) <= {}", srcs[0].alias, 1 + self.rng.below(4)),
            }
        } else {
            String::new()
        };
        format!("SELECT {} FROM {from}{where_sql}{group_sql}{having}", sel_aliased.join(", "))
    }

    /// A window function over the sources; returns it and a WINDOW clause
    /// if it uses a named window.
    fn window(&mut self, srcs: &[Src]) -> (String, String) {
        let (p, _) = self.col_of(srcs, |_| true).unwrap();
        let part = if self.rng.chance(15) { String::new() } else { format!("PARTITION BY {p} ") };
        let order = Self::ids(srcs);
        let (v, _) = self.col_of(srcs, |c| c.ty.is_num()).unwrap_or((format!("{}.id", srcs[0].alias), Ty::Int));
        if self.rng.chance(25)
            && let Some((oc, _)) = self.col_of(srcs, |c| matches!(c.ty, Ty::Int | Ty::BigInt | Ty::Numeric))
        {
            // peers (equal ORDER BY values) share their frame, so aggregates
            // over RANGE and GROUPS frames are deterministic
            let agg = *self.rng.pick(&["sum", "count", "max", "min"]);
            let (lo, hi) = (self.rng.below(5), self.rng.below(3));
            let frames = [
                format!("RANGE BETWEEN {lo} PRECEDING AND CURRENT ROW"),
                format!("RANGE BETWEEN {lo} PRECEDING AND {hi} FOLLOWING"),
                format!("RANGE BETWEEN CURRENT ROW AND {hi} FOLLOWING"),
                format!("GROUPS BETWEEN {lo} PRECEDING AND CURRENT ROW"),
                format!("GROUPS BETWEEN CURRENT ROW AND {hi} FOLLOWING"),
                "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW".to_string(),
            ];
            let frame = self.rng.pick(&frames).clone();
            let dir = if self.rng.chance(30) { " DESC" } else { "" };
            return (format!("{agg}({v}) OVER ({part}ORDER BY {oc}{dir} {frame})"), String::new());
        }
        if self.rng.chance(15)
            && let Some((oc, _)) = self.col_of(srcs, |c| c.ty.minmax() && c.ty != Ty::Big)
        {
            // functions of peer groups only
            let f = *self.rng.pick(&["rank()", "dense_rank()", "percent_rank()", "cume_dist()"]);
            return (format!("{f} OVER ({part}ORDER BY {oc} NULLS FIRST)"), String::new());
        }
        let whole = " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING";
        let (f, frame) = match self.rng.below(15) {
            0 => ("row_number()".to_string(), ""),
            1 => ("rank()".to_string(), ""),
            2 => ("dense_rank()".to_string(), ""),
            3 => ("count(*)".to_string(), ""),
            4 => (format!("sum({v})"), *self.rng.pick(&["", " ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING", whole])),
            5 => (format!("lag({v})"), ""),
            6 => (format!("lead({v}, 2)"), ""),
            7 => (format!("first_value({v})"), ""),
            8 => (format!("max({v})"), *self.rng.pick(&["", " ROWS BETWEEN 2 PRECEDING AND CURRENT ROW", whole])),
            9 => (format!("min({v})"), *self.rng.pick(&["", " ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING", whole])),
            10 => (format!("ntile({})", 2 + self.rng.below(3)), ""),
            11 => (format!("last_value({v})"), whole),
            12 => (format!("nth_value({v}, 2)"), whole),
            13 => (format!("lag({v}, 1, 0)"), ""),
            _ => ("percent_rank()".to_string(), ""),
        };
        if self.rng.chance(20) {
            return (format!("{f} OVER w"), format!(" WINDOW w AS ({part}ORDER BY {order}{frame})"));
        }
        (format!("{f} OVER ({part}ORDER BY {order}{frame})"), String::new())
    }

    fn select_list_subqueries(&mut self, srcs: &[Src], sel: &mut Vec<String>) {
        let t = self.rng.below(self.tables.len() as u64) as usize;
        let tn = self.tables[t].name.clone();
        let corr = self.correlation(t, srcs);
        let (agg_col, _) = {
            let inner = [Src { alias: "sq".into(), table: t }];
            self.col_of(&inner, |c| c.ty.is_num()).unwrap_or(("sq.id".into(), Ty::Int))
        };
        match self.rng.below(4) {
            0 => sel.push(format!("EXISTS (SELECT 1 FROM {tn} sq WHERE {corr})")),
            1 => sel.push(format!("ARRAY(SELECT sq.id FROM {tn} sq WHERE {corr} ORDER BY sq.id)")),
            _ => {
                let agg = *self.rng.pick(&["sum", "max", "count", "min"]);
                sel.push(format!("(SELECT {agg}({agg_col}) FROM {tn} sq WHERE {corr})"));
            }
        }
        if self.rng.chance(40) {
            // set-returning functions in the select list (several run in lockstep)
            let a = &srcs[0].alias;
            sel.push(format!("generate_series(1, ({a}.id % 3) + 1)"));
            if self.rng.chance(50) {
                if let Some((c, _)) = self.col_of(srcs, |c| c.ty == Ty::IntArr) {
                    sel.push(format!("unnest({c})"));
                } else if let Some((c, _)) = self.col_of(srcs, |c| c.ty == Ty::Jsonb) {
                    sel.push(format!("jsonb_path_query({c}, 'lax $.items[*].p')"));
                }
            }
        }
    }

    fn set_operation(&mut self, q: String, sel: &[String], srcs: &[Src]) -> String {
        let second = |g: &mut Self| {
            let (from2, srcs2) = g.from("b");
            let mut sel2 = Vec::new();
            for e in sel {
                // same types: re-pick a column of the same type from the other query
                let ty = expr_type_guess(e, srcs, g.tables);
                match g.col_of(&srcs2, |c| Some(c.ty) == ty) {
                    Some((c, _)) => sel2.push(c),
                    None => sel2.push(format!("NULL::{}", ty.map(|t| t.sql()).unwrap_or("int"))),
                }
            }
            let wh = if g.rng.chance(40) { format!(" WHERE {}", g.pred(&srcs2, 1)) } else { String::new() };
            format!("SELECT {} FROM {from2}{wh}", sel2.join(", "))
        };
        let op = *self.rng.pick(&["UNION", "UNION ALL", "EXCEPT", "INTERSECT", "EXCEPT ALL", "INTERSECT ALL"]);
        let b = second(self);
        if self.rng.chance(25) {
            // nested, and inside a derived table that aggregates
            let op2 = *self.rng.pick(&["UNION", "EXCEPT ALL", "INTERSECT"]);
            let c = second(self);
            return format!("SELECT x0, count(*) AS n FROM (({q} {op} {b}) {op2} {c}) s GROUP BY x0");
        }
        format!("{q} {op} {b}")
    }

    /// DISTINCT ON over one table, ordered by its keys and then by id so that
    /// the row each group keeps is determined (PostgreSQL wants the ORDER BY
    /// to start with the DISTINCT ON expressions). The keys are plain
    /// columns, which are also selected; ivm also accepts other expressions
    /// and ORDER BY columns that are not selected, which this generator
    /// does not produce.
    fn distinct_on(&mut self) -> String {
        let t = self.rng.below(self.tables.len() as u64) as usize;
        let srcs = [Src { alias: "a0".into(), table: t }];
        let cols: Vec<String> =
            self.tables[t].cols.iter().filter(|c| c.name != "id" && c.ty.minmax() && c.ty != Ty::Big).map(|c| c.name.clone()).collect();
        let mut keys: Vec<String> = Vec::new();
        for _ in 0..(1 + self.rng.below(2)) {
            if let Some(c) = (!cols.is_empty()).then(|| self.rng.pick(&cols).clone())
                && !keys.contains(&c)
            {
                keys.push(c);
            }
        }
        if keys.is_empty() {
            keys.push("id".into());
        }
        let mut sel = keys.clone();
        for i in 0..(1 + self.rng.below(2)) {
            sel.push(format!("{} AS x{}", self.expr(&srcs, 0).0, i as usize + keys.len()));
        }
        sel.push("id AS i0".into());
        let wh = if self.rng.chance(60) { format!(" WHERE {}", self.pred(&srcs, 0)) } else { String::new() };
        let dir = if self.rng.chance(30) { " DESC" } else { "" };
        let limit = if self.rng.chance(20) { format!(" LIMIT {}", 1 + self.rng.below(5)) } else { String::new() };
        format!(
            "SELECT DISTINCT ON ({}) {} FROM {} a0{wh} ORDER BY {}, id{dir}{limit}",
            keys.join(", "),
            sel.join(", "),
            self.tables[t].name,
            keys.iter().map(|k| format!("{k} NULLS FIRST")).collect::<Vec<_>>().join(", ")
        )
    }

    /// A recursive walk over a table referencing itself (None if no table does).
    fn recursive(&mut self) -> Option<String> {
        let cands: Vec<usize> = (0..self.tables.len()).filter(|&i| self.tables[i].self_ref(i).is_some()).collect();
        if cands.is_empty() {
            return None;
        }
        let t = *self.rng.pick(&cands);
        let tn = &self.tables[t].name;
        let pr = self.tables[t].self_ref(t).unwrap().name.clone();
        let start = if self.rng.chance(50) { format!("a.{pr} IS NULL") } else { format!("a.id % {} = 0", 2 + self.rng.below(3)) };
        let depth = 1 + self.rng.below(4);
        let body = format!(
            "WITH RECURSIVE r(id, root, depth) AS (SELECT a.id, a.id, 0 FROM {tn} a WHERE {start} \
             UNION SELECT c.id, r.root, r.depth + 1 FROM {tn} c JOIN r ON c.{pr} = r.id WHERE r.depth < {depth})"
        );
        Some(match self.rng.below(3) {
            0 => format!("{body} SELECT root AS x0, count(*) AS x1, max(depth) AS x2 FROM r GROUP BY root"),
            1 => format!("{body} SELECT DISTINCT id AS x0, depth AS x1 FROM r"),
            _ => format!("{body} SELECT r.id AS x0, r.depth AS x1, t.{pr} AS x2 FROM r JOIN {tn} t ON t.id = r.root"),
        })
    }

    /// Joins with VALUES lists and set-returning functions in FROM.
    fn table_functions(&mut self, srcs: &[Src], from: &str, where_sql: &str) -> String {
        let (e, _) = self.expr(srcs, 1);
        let a0 = &srcs[0].alias;
        match self.rng.below(4) {
            0 => {
                let (c, _) = self.col_of(srcs, |c| c.ty == Ty::Int).unwrap_or((format!("{a0}.id"), Ty::Int));
                format!(
                    "SELECT {e} AS x0, v.s AS x1 FROM {from} JOIN (VALUES (1, 'one'), (2, 'two'), (NULL::int, 'none'), (2, 'deux')) v(k, s) ON v.k = {c}{where_sql}"
                )
            }
            // (ivm rejects WITH ORDINALITY with an alias list after an explicit LATERAL)
            1 => format!("SELECT {e} AS x0, g.n AS x1, g.o AS x2 FROM {from}, generate_series(1, {a0}.id % 3) WITH ORDINALITY AS g(n, o){where_sql}"),
            2 => match self.col_of(srcs, |c| c.ty == Ty::IntArr) {
                Some((c, _)) if self.rng.chance(50) => format!("SELECT {e} AS x0, u.v AS x1, u.o AS x2 FROM {from}, unnest({c}) WITH ORDINALITY AS u(v, o){where_sql}"),
                Some((c, _)) => format!("SELECT {e} AS x0, u.v AS x1 FROM {from} LEFT JOIN LATERAL unnest({c}) AS u(v) ON true{where_sql}"),
                None => format!("SELECT {e} AS x0, g.n AS x1 FROM {from} CROSS JOIN generate_series(0, 1) AS g(n){where_sql}"),
            },
            _ => match self.col_of(srcs, |c| c.ty == Ty::Jsonb) {
                Some((c, _)) => format!("SELECT {e} AS x0, j.p AS x1 FROM {from} CROSS JOIN LATERAL jsonb_path_query({c}, 'lax $.items[*].p') AS j(p){where_sql}"),
                None => format!("SELECT {e} AS x0, v.k AS x1 FROM {from} CROSS JOIN (VALUES (1), (2)) v(k){where_sql}"),
            },
        }
    }
}

/// Best-effort type of a generated expression.
fn expr_type_guess(e: &str, srcs: &[Src], tables: &[Table]) -> Option<Ty> {
    if e.starts_with("length(") {
        return Some(Ty::Int);
    }
    if e.starts_with("extract(") {
        return Some(Ty::Numeric);
    }
    if e.starts_with("date_trunc('month'") {
        return Some(Ty::Timestamp);
    }
    if e.starts_with("md5(") || e.starts_with("left(") {
        return Some(Ty::Text);
    }
    for s in srcs {
        for c in &tables[s.table].cols {
            if e.contains(&format!("{}.{}", s.alias, c.name)) {
                return Some(if c.ty == Ty::Big { Ty::Text } else { c.ty });
            }
        }
    }
    None
}

/// A view over view `name` (defined by `q`): the query to compare it with
/// (with `q` inlined) and the definition reading the view.
pub fn view_over_view(rng: &mut Rng, name: &str, q: &str) -> (String, String) {
    let inlined = match rng.below(3) {
        0 => format!("SELECT x0, count(*) AS n FROM ({q}) s GROUP BY x0"),
        1 => format!("SELECT DISTINCT x0 FROM ({q}) s WHERE x0 IS NOT NULL"),
        _ => format!("SELECT s.*, s2.n FROM ({q}) s JOIN (SELECT x0, count(*) AS n FROM ({q}) z GROUP BY x0) s2 ON s2.x0 = s.x0"),
    };
    let reading = inlined.replace(&format!("({q})"), name);
    (inlined, reading)
}
