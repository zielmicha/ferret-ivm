//! Differential testing against a real PostgreSQL.
//!
//! Starts a private PostgreSQL cluster and an in-process engine, replicates
//! randomly generated tables through logical replication, creates random
//! materialized views, applies random update streams upstream, and after each
//! batch compares every view with PostgreSQL recomputing the same query.
//!
//! usage: ivm-difftest [--seed N] [--views N] [--rounds N] [--keep]

use ivm::coord::{self, Config};
use ivm::server::{self, ServerConfig};
use pgproto::testutil::{ClusterOptions, TempCluster};
use pgproto::{Client, Config as PgConfig};
use std::net::TcpListener;
use std::time::{Duration, Instant};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }
    fn chance(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }
    fn pick<'a, T>(&mut self, v: &'a [T]) -> &'a T {
        &v[self.below(v.len() as u64) as usize]
    }
    fn range(&mut self, lo: i64, hi: i64) -> i64 {
        lo + self.below((hi - lo + 1) as u64) as i64
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Ty {
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
}

impl Ty {
    fn sql(&self) -> &'static str {
        match self {
            Ty::Int => "int",
            Ty::BigInt => "bigint",
            Ty::Numeric => "numeric(10,2)",
            Ty::Decimal => "numeric",
            Ty::Text => "text",
            Ty::Bool => "boolean",
            Ty::Date => "date",
            Ty::Timestamp => "timestamp",
            Ty::Float => "float8",
            Ty::Jsonb => "jsonb",
            Ty::IntArr => "int[]",
        }
    }
    fn is_num(&self) -> bool {
        matches!(self, Ty::Int | Ty::BigInt | Ty::Numeric | Ty::Decimal | Ty::Float)
    }
    fn orderable(&self) -> bool {
        true
    }
    /// min() / max() exist
    fn minmax(&self) -> bool {
        *self != Ty::Jsonb
    }
}

#[derive(Clone, Debug)]
struct Col {
    name: String,
    ty: Ty,
    nullable: bool,
    /// int column referencing another table's id
    refs: Option<usize>,
}

#[derive(Clone, Debug)]
struct Table {
    name: String,
    cols: Vec<Col>,
    next_id: i64,
}

fn literal(rng: &mut Rng, ty: Ty) -> String {
    match ty {
        Ty::Int => rng.range(-5, 40).to_string(),
        Ty::BigInt => rng.range(-1000, 1000).to_string(),
        Ty::Numeric => format!("{}.{:02}", rng.range(-100, 100), rng.below(100)),
        Ty::Decimal => {
            let choices = ["1", "1.0", "1.50", "-2.125", "0.000", "3", "10.5", "-0.25", "2.00", "7.0001", "-4"];
            rng.pick(&choices).to_string()
        }
        Ty::Text => {
            let words = ["a", "b", "foo", "bar", "Baz", "", "ü", "a b", "zz", "A"];
            let w = rng.pick(&words);
            if rng.chance(30) { format!("'{}{}'", w, rng.below(5)) } else { format!("'{w}'") }
        }
        Ty::Bool => (if rng.chance(50) { "true" } else { "false" }).into(),
        Ty::Date => format!("date '2024-01-01' + {}", rng.below(60)),
        Ty::Timestamp => format!("timestamp '2024-01-01 00:00:00' + interval '{} minutes'", rng.below(100000)),
        Ty::Float => {
            let choices = ["0", "1.5", "-2.25", "3.75", "100", "0.1", "-0.5", "7"];
            format!("{}::float8", rng.pick(&choices))
        }
        Ty::Jsonb => {
            // integers only: equal jsonb numbers of different scale would make
            // PostgreSQL's choice of representation nondeterministic
            let (n, m) = (rng.range(0, 6), rng.range(0, 6));
            let tags = *rng.pick(&[r#"["x", "y"]"#, r#"["y"]"#, "[]", r#"["x", "z", "x"]"#]);
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
    }
}

fn value(rng: &mut Rng, c: &Col, tables: &[Table]) -> String {
    if c.nullable && rng.chance(12) {
        return "NULL".into();
    }
    if let Some(t) = c.refs {
        let max = tables[t].next_id.max(2);
        return rng.range(1, max + 2).to_string();
    }
    literal(rng, c.ty)
}

fn gen_schema(rng: &mut Rng) -> Vec<Table> {
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
            ];
            let ty = *rng.pick(&tys);
            cols.push(Col { name: format!("c{j}"), ty, nullable: rng.chance(60), refs: None });
        }
        // foreign-key-like columns to earlier tables
        if i > 0 {
            for k in 0..(1 + rng.below(2) as usize) {
                let target = rng.below(i as u64) as usize;
                cols.push(Col { name: format!("r{k}"), ty: Ty::Int, nullable: rng.chance(30), refs: Some(target) });
            }
        }
        tables.push(Table { name: format!("t{i}"), cols, next_id: 1 });
    }
    tables
}

/// A table in scope for query generation.
#[derive(Clone)]
struct Src {
    alias: String,
    table: usize,
}

struct Gen<'a> {
    rng: &'a mut Rng,
    tables: &'a [Table],
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
        if cands.is_empty() {
            None
        } else {
            Some(self.rng.pick(&cands).clone())
        }
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
                    let (d, dty) = self.col_of(srcs, |x| x.ty == ty).unwrap();
                    let _ = dty;
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
            let (ti, on) = if cands.is_empty() || self.rng.chance(10) {
                let ti = self.rng.below(self.tables.len() as u64) as usize;
                (ti, format!("{prefix}{k}.id = {}.id", srcs[0].alias))
            } else {
                self.rng.pick(&cands).clone()
            };
            let kind = *self.rng.pick(&["JOIN", "JOIN", "LEFT JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"]);
            sql.push_str(&format!(" {kind} {} {prefix}{k} ON {on}", self.tables[ti].name));
            if self.rng.chance(15) {
                let extra = self.pred(&[srcs[0].clone()], 1);
                sql.push_str(&format!(" AND {extra}"));
            }
            srcs.push(Src { alias: format!("{prefix}{k}"), table: ti });
        }
        (sql, srcs)
    }

    fn subquery_pred(&mut self, srcs: &[Src]) -> String {
        let t = self.rng.below(self.tables.len() as u64) as usize;
        let inner = Src { alias: "sq".into(), table: t };
        // correlate on a reference or on id
        let mut corr = format!("sq.id = {}.id", srcs[0].alias);
        for c in &self.tables[t].cols {
            if let Some(target) = c.refs
                && let Some(s) = srcs.iter().find(|s| s.table == target)
            {
                corr = format!("sq.{} = {}.id", c.name, s.alias);
            }
        }
        let extra = if self.rng.chance(50) { format!(" AND {}", self.pred(std::slice::from_ref(&inner), 1)) } else { String::new() };
        let tn = &self.tables[t].name;
        match self.rng.below(8) {
            5 => {
                let (oc, _) = self.col_of(srcs, |c| c.ty == Ty::Int).unwrap_or((format!("{}.id", srcs[0].alias), Ty::Int));
                format!("{oc} = ANY (SELECT sq.id FROM {tn} sq WHERE true{extra})")
            }
            6 => {
                // correlated and possibly empty: ALL over nothing is true
                let (oc, _) = self.col_of(srcs, |c| c.ty == Ty::Int).unwrap_or((format!("{}.id", srcs[0].alias), Ty::Int));
                format!("{oc} > ALL (SELECT sq.id FROM {tn} sq WHERE {corr}{extra})")
            }
            7 => {
                let a = &srcs[0].alias;
                let not = if self.rng.chance(50) { "NOT " } else { "" };
                format!("({a}.id, {a}.id % 3) {not}IN (SELECT sq.id, sq.id % 3 FROM {tn} sq WHERE true{extra})")
            }
            0 => format!("EXISTS (SELECT 1 FROM {tn} sq WHERE {corr}{extra})"),
            1 => format!("NOT EXISTS (SELECT 1 FROM {tn} sq WHERE {corr}{extra})"),
            2 => {
                let (oc, _) = self.col_of(srcs, |c| c.ty == Ty::Int).unwrap_or((format!("{}.id", srcs[0].alias), Ty::Int));
                format!("{oc} IN (SELECT sq.id FROM {tn} sq WHERE true{extra})")
            }
            3 => {
                let (oc, _) = self.col_of(srcs, |c| c.ty == Ty::Int).unwrap_or((format!("{}.id", srcs[0].alias), Ty::Int));
                format!("{oc} NOT IN (SELECT sq.id FROM {tn} sq WHERE sq.id IS NOT NULL{extra})")
            }
            _ => format!("(SELECT count(*) FROM {tn} sq WHERE {corr}{extra}) > {}", self.rng.below(3)),
        }
    }

    fn query(&mut self) -> String {
        let (from, srcs) = self.from("a");
        let mut wh = Vec::new();
        for _ in 0..self.rng.below(3) {
            wh.push(self.pred(&srcs, 0));
        }
        if self.rng.chance(25) {
            wh.push(self.subquery_pred(&srcs));
        }
        let where_sql = if wh.is_empty() { String::new() } else { format!(" WHERE {}", wh.join(" AND ")) };
        let shape = self.rng.below(10);
        if shape < 4 {
            // aggregation
            let ng = self.rng.below(3) as usize;
            let mut group = Vec::new();
            for _ in 0..ng {
                let (e, _) = self.expr(&srcs, 1);
                if !group.contains(&e) {
                    group.push(e);
                }
            }
            let mut sel: Vec<String> = group.clone();
            let naggs = 1 + self.rng.below(3);
            for _ in 0..naggs {
                let (c, ty) = self.col_of(&srcs, |_| true).unwrap();
                let agg = match self.rng.below(10) {
                    9 if ty.orderable() => format!("mode() WITHIN GROUP (ORDER BY {c})"),
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
                        "array_agg({c} ORDER BY {c}) FILTER (WHERE array_ndims({c}) = 1 AND array_lower({c}, 1) = 1 AND cardinality({c}) = 2)"
                    ),
                    _ => format!("count({c})"),
                };
                sel.push(agg);
            }
            let mut sel = sel;
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
            let having = if !group.is_empty() && self.rng.chance(25) { " HAVING count(*) > 1".to_string() } else { String::new() };
            return format!("SELECT {} FROM {from}{where_sql}{group_sql}{having}", sel_aliased.join(", "));
        }
        // projection
        let n = 1 + self.rng.below(4) as usize;
        let mut sel = Vec::new();
        for _ in 0..n {
            let (e, _) = self.expr(&srcs, 0);
            sel.push(e);
        }
        if shape == 4 {
            // window function, deterministic order
            let (p, _) = self.col_of(&srcs, |_| true).unwrap();
            let all: Vec<String> = srcs.iter().map(|s| format!("{}.id", s.alias)).collect();
            let order = all.join(", ");
            let (v, vty) = self.col_of(&srcs, |c| c.ty.is_num()).unwrap_or((format!("{}.id", srcs[0].alias), Ty::Int));
            let _ = vty;
            let f = match self.rng.below(10) {
                0 => "row_number()".to_string(),
                1 => "rank()".to_string(),
                2 => "dense_rank()".to_string(),
                3 => "count(*)".to_string(),
                4 => format!("sum({v})"),
                5 => format!("lag({v})"),
                6 => format!("lead({v}, 2)"),
                7 => format!("first_value({v})"),
                8 => format!("max({v})"),
                _ => format!("min({v})"),
            };
            let frame = if f.starts_with("sum") || f.starts_with("max") || f.starts_with("min") {
                *self.rng.pick(&["", " ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING", " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"])
            } else {
                ""
            };
            if self.rng.chance(25)
                && let Some((oc, _)) = self.col_of(&srcs, |c| matches!(c.ty, Ty::Int | Ty::BigInt | Ty::Numeric))
            {
                // RANGE frames include peers, so aggregates are deterministic
                let agg = *self.rng.pick(&["sum", "count", "max", "min"]);
                let (lo, hi) = (self.rng.below(5), self.rng.below(3));
                let frames = [
                    format!("RANGE BETWEEN {lo} PRECEDING AND CURRENT ROW"),
                    format!("RANGE BETWEEN {lo} PRECEDING AND {hi} FOLLOWING"),
                    format!("RANGE BETWEEN CURRENT ROW AND {hi} FOLLOWING"),
                ];
                let frame = self.rng.pick(&frames).clone();
                let dir = if self.rng.chance(30) { " DESC" } else { "" };
                sel.push(format!("{agg}({v}) OVER (PARTITION BY {p} ORDER BY {oc}{dir} {frame})"));
            } else {
                sel.push(format!("{f} OVER (PARTITION BY {p} ORDER BY {order}{frame})"));
            }
        }
        if shape == 7 {
            // scalar subquery in the select list
            let t = self.rng.below(self.tables.len() as u64) as usize;
            let tn = self.tables[t].name.clone();
            let (agg_col, _) = {
                let inner = [Src { alias: "sq".into(), table: t }];
                self.col_of(&inner, |c| c.ty.is_num()).unwrap_or(("sq.id".into(), Ty::Int))
            };
            let mut corr = format!("sq.id = {}.id", srcs[0].alias);
            for c in &self.tables[t].cols {
                if let Some(target) = c.refs
                    && let Some(s) = srcs.iter().find(|s| s.table == target)
                {
                    corr = format!("sq.{} = {}.id", c.name, s.alias);
                }
            }
            let agg = *self.rng.pick(&["sum", "max", "count", "min"]);
            sel.push(format!("(SELECT {agg}({agg_col}) FROM {tn} sq WHERE {corr})"));
        }
        if shape == 7 && self.rng.chance(40) {
            // set-returning functions in the select list (several run in lockstep)
            let a = &srcs[0].alias;
            sel.push(format!("generate_series(1, ({a}.id % 3) + 1)"));
            if self.rng.chance(50) {
                if let Some((c, _)) = self.col_of(&srcs, |c| c.ty == Ty::IntArr) {
                    sel.push(format!("unnest({c})"));
                } else if let Some((c, _)) = self.col_of(&srcs, |c| c.ty == Ty::Jsonb) {
                    sel.push(format!("jsonb_path_query({c}, 'lax $.items[*].p')"));
                }
            }
        }
        let sel_aliased: Vec<String> = sel.iter().enumerate().map(|(i, e)| format!("{e} AS x{i}")).collect();
        let distinct = if self.rng.chance(20) { "DISTINCT " } else { "" };
        let mut q = format!("SELECT {distinct}{} FROM {from}{where_sql}", sel_aliased.join(", "));
        if shape == 8 {
            // derived table / CTE wrapping an aggregate
            let inner = q.clone();
            let cnt = sel.len();
            let cols: Vec<String> = (0..cnt).map(|i| format!("x{i}")).collect();
            q = if self.rng.chance(50) {
                format!("WITH w AS ({inner}) SELECT {}, count(*) AS n FROM w GROUP BY {}", cols.join(", "), cols.join(", "))
            } else {
                format!("SELECT d.*, 1 AS one FROM ({inner}) d WHERE d.x0 IS NOT NULL")
            };
            return q;
        }
        if shape == 9 {
            // LATERAL top-1 per row
            let t = self.rng.below(self.tables.len() as u64) as usize;
            let tn = self.tables[t].name.clone();
            q = format!(
                "SELECT {}, l.id AS lid FROM {from} LEFT JOIN LATERAL (SELECT sq.id FROM {tn} sq WHERE sq.id > {}.id ORDER BY sq.id LIMIT 1) l ON true{where_sql}",
                sel_aliased.join(", "),
                srcs[0].alias
            );
            return q;
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
            // set operation with a second query of the same shape
            let (from2, srcs2) = self.from("b");
            let mut sel2 = Vec::new();
            for e in &sel {
                // same types: re-pick a column of the same type from the other query
                let ty = expr_type_guess(e, &srcs, self.tables);
                match self.col_of(&srcs2, |c| Some(c.ty) == ty) {
                    Some((c, _)) => sel2.push(c),
                    None => sel2.push(format!("NULL::{}", ty.map(|t| t.sql()).unwrap_or("int"))),
                }
            }
            let op = *self.rng.pick(&["UNION", "UNION ALL", "EXCEPT", "INTERSECT", "EXCEPT ALL", "INTERSECT ALL"]);
            q = format!("{q} {op} SELECT {} FROM {from2}", sel2.join(", "));
        }
        q
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
    for s in srcs {
        for c in &tables[s.table].cols {
            if e.contains(&format!("{}.{}", s.alias, c.name)) {
                return Some(c.ty);
            }
        }
    }
    None
}

fn connect(conninfo: &str) -> Client {
    let cfg = PgConfig::parse(conninfo).expect("conninfo");
    Client::connect(&cfg).expect("connect")
}

fn rows(c: &mut Client, sql: &str) -> Result<(Vec<u32>, Vec<Vec<Option<String>>>), String> {
    match c.query(sql) {
        Ok(r) => Ok((r.columns.iter().map(|c| c.type_oid).collect(), r.rows)),
        Err(e) => Err(e.message),
    }
}

/// Normalize a result for comparison (sorted; floats rounded).
fn normalize(types: &[u32], rows: Vec<Vec<Option<String>>>) -> Vec<String> {
    normalize_with(types, rows, false)
}

/// `trim_numeric`: compare numerics by value ("1.0" = "1"). Where SQL
/// deduplicates equal values (GROUP BY, DISTINCT, set operations, MIN/MAX),
/// PostgreSQL keeps whichever representation it met first, so only the
/// value is defined there.
fn normalize_with(types: &[u32], mut rows: Vec<Vec<Option<String>>>, trim_numeric: bool) -> Vec<String> {
    for r in rows.iter_mut() {
        for (i, v) in r.iter_mut().enumerate() {
            if let Some(s) = v
                && matches!(types.get(i), Some(700) | Some(701))
                && let Ok(f) = s.parse::<f64>()
            {
                *s = if f == 0.0 { "0".into() } else { format!("{:.9e}", f) };
            }
            if let Some(s) = v
                && trim_numeric
                && types.get(i) == Some(&1700)
                && s.contains('.')
            {
                let t = s.trim_end_matches('0').trim_end_matches('.');
                *s = if t == "-0" || t.is_empty() { "0".into() } else { t.to_string() };
            }
        }
    }
    let mut out: Vec<String> = rows
        .into_iter()
        .map(|r| r.into_iter().map(|v| v.unwrap_or_else(|| "∅".into())).collect::<Vec<_>>().join("|"))
        .collect();
    out.sort();
    out
}

fn main() {
    let mut seed = 42u64;
    let mut nviews = 25usize;
    let mut rounds = 20usize;
    let mut keep = false;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--seed" => seed = args.next().unwrap().parse().unwrap(),
            "--views" => nviews = args.next().unwrap().parse().unwrap(),
            "--rounds" => rounds = args.next().unwrap().parse().unwrap(),
            "--keep" => keep = true,
            _ => {
                eprintln!("usage: ivm-difftest [--seed N] [--views N] [--rounds N] [--keep]");
                std::process::exit(2);
            }
        }
    }
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();
    let mut rng = Rng(seed.wrapping_mul(0x9E3779B97F4A7C15) | 1);
    let pg = TempCluster::start(ClusterOptions { extra_conf: vec!["timezone = 'UTC'".into()], keep, ..Default::default() })
        .expect("start postgres");
    let pg_conn = pg.conninfo("postgres");
    let mut up = connect(&pg_conn);

    // engine + server on a free port
    let data = std::env::temp_dir().join(format!("ivm-difftest-{}-{seed}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data);
    let running = coord::start(Config { data_dir: data.clone(), sync: false, ..Default::default() }).expect("engine");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let h = running.handle.clone();
    std::thread::spawn(move || {
        let _ = server::serve_listener(listener, ServerConfig { listen: String::new(), password: None }, h);
    });
    let mut ivm = connect(&format!("host=127.0.0.1 port={port} user=ivm dbname=ivm"));

    // schema + initial data
    let mut tables = gen_schema(&mut rng);
    for t in &tables {
        let cols: Vec<String> = t
            .cols
            .iter()
            .map(|c| format!("{} {}{}", c.name, c.ty.sql(), if c.name == "id" { " PRIMARY KEY" } else { "" }))
            .collect();
        up.execute(&format!("CREATE TABLE {} ({})", t.name, cols.join(", "))).unwrap();
    }
    for ti in 0..tables.len() {
        let n = 30 + rng.below(80);
        for _ in 0..n {
            insert_row(&mut rng, &mut tables, ti, &mut up);
        }
    }
    up.execute("CREATE PUBLICATION difftest FOR ALL TABLES").unwrap();
    ivm.execute(&format!("CREATE SOURCE src FROM POSTGRES CONNECTION '{pg_conn}' PUBLICATION 'difftest'"))
        .expect("create source");

    // views
    let mut views: Vec<(String, String)> = Vec::new();
    let mut unsupported = 0;
    let mut attempts = 0;
    while views.len() < nviews && attempts < nviews * 20 {
        attempts += 1;
        let q = Gen { rng: &mut rng, tables: &tables }.query();
        if rows(&mut up, &q).is_err() {
            continue; // invalid in PostgreSQL too
        }
        let name = format!("v{}", views.len());
        match ivm.execute(&format!("CREATE MATERIALIZED VIEW {name} AS {q}")) {
            Ok(_) => {
                if std::env::var("DIFFTEST_VERBOSE").is_ok() {
                    println!("{name}: {q}");
                }
                views.push((name, q))
            }
            Err(e) => {
                unsupported += 1;
                println!("unsupported: {} :: {q}", e.message);
            }
        }
    }
    // views over views
    let base = views.len();
    for k in 0..(base / 4) {
        let (src_name, src_q) = views[rng.below(base as u64) as usize].clone();
        let q = match rng.below(3) {
            0 => format!("SELECT x0, count(*) AS n FROM ({src_q}) s GROUP BY x0"),
            1 => format!("SELECT DISTINCT x0 FROM ({src_q}) s WHERE x0 IS NOT NULL"),
            _ => format!("SELECT s.*, s2.n FROM ({src_q}) s JOIN (SELECT x0, count(*) AS n FROM ({src_q}) z GROUP BY x0) s2 ON s2.x0 = s.x0"),
        };
        if rows(&mut up, &q).is_err() {
            continue;
        }
        // in the engine, read from the materialized view instead of inlining
        let ivm_q = q.replace(&format!("({src_q})"), &src_name.to_string());
        let name = format!("vv{k}");
        match ivm.execute(&format!("CREATE MATERIALIZED VIEW {name} AS {ivm_q}")) {
            Ok(_) => views.push((name, q)),
            Err(e) => {
                unsupported += 1;
                println!("unsupported: {} :: {ivm_q}", e.message);
            }
        }
    }
    println!("created {} views ({unsupported} rejected)", views.len());

    let mut failures = 0;
    for round in 0..=rounds {
        if round > 0 {
            // a random transaction batch upstream
            let ntx = 1 + rng.below(4);
            for _ in 0..ntx {
                let mut sql = String::from("BEGIN;");
                for _ in 0..(1 + rng.below(8)) {
                    let ti = rng.below(tables.len() as u64) as usize;
                    match rng.below(10) {
                        0..=3 => sql.push_str(&insert_sql(&mut rng, &mut tables, ti)),
                        4..=6 => {
                            let t = &tables[ti];
                            let c = rng.pick(&t.cols[1..]).clone();
                            let v = value(&mut rng, &c, &tables);
                            sql.push_str(&format!(
                                "UPDATE {} SET {} = {v} WHERE id % {} = {};",
                                t.name,
                                c.name,
                                2 + rng.below(5),
                                rng.below(3)
                            ));
                        }
                        7 => {
                            let t = &tables[ti];
                            let max = t.next_id.max(2);
                            if rng.chance(5) && round % 7 == 3 {
                                sql.push_str(&format!("TRUNCATE {};", t.name));
                            } else {
                                sql.push_str(&format!("UPDATE {} SET id = id + 100000 WHERE id = {};", t.name, rng.range(1, max)));
                            }
                        }
                        _ => {
                            let t = &tables[ti];
                            sql.push_str(&format!("DELETE FROM {} WHERE id % {} = {};", t.name, 5 + rng.below(7), rng.below(5)));
                        }
                    }
                }
                sql.push_str("COMMIT;");
                if let Err(e) = up.simple_query(&sql) {
                    let _ = up.simple_query("ROLLBACK");
                    println!("upstream batch failed (ignored): {}", e.message);
                }
            }
        }
        let lsn = up.query("SELECT pg_current_wal_lsn()").unwrap().rows[0][0].clone().unwrap();
        let t0 = Instant::now();
        let mut total_rows = 0;
        let mut both_err = 0;
        let mut matched = 0;
        for (name, q) in &views {
            let expect = rows(&mut up, q);
            let got = rows(&mut ivm, &format!("SELECT * FROM {name} AS OF AT LEAST LSN '{lsn}'"));
            match (expect, got) {
                (Ok((et, er)), Ok((_, gr))) => {
                    let (a, b) = (normalize(&et, er.clone()), normalize(&et, gr.clone()));
                    total_rows += a.len();
                    let upper = q.to_uppercase();
                    let dedups = ["GROUP BY", "DISTINCT", "UNION", "INTERSECT", "EXCEPT", "MIN(", "MAX(", "MODE(", "LIMIT", "FETCH"]
                        .iter()
                        .any(|k| upper.contains(k));
                    let same_values = a != b && dedups && normalize_with(&et, er, true) == normalize_with(&et, gr, true);
                    if same_values && std::env::var("DIFFTEST_VERBOSE").is_ok() {
                        println!("note round {round} view {name}: equal values in other representations (PostgreSQL keeps the first it meets)");
                    }
                    if a == b || same_values {
                        matched += 1;
                    } else {
                        failures += 1;
                        println!("MISMATCH round {round} view {name}: {q}");
                        let only_pg: Vec<&String> = a.iter().filter(|x| !b.contains(x)).take(5).collect();
                        let only_ivm: Vec<&String> = b.iter().filter(|x| !a.contains(x)).take(5).collect();
                        println!("  postgres rows {} / ivm rows {}", a.len(), b.len());
                        println!("  only in postgres: {only_pg:?}");
                        println!("  only in ivm:      {only_ivm:?}");
                        if only_pg.is_empty() && only_ivm.is_empty() {
                            // same values, different multiplicities
                            println!("  postgres: {a:?}");
                            println!("  ivm:      {b:?}");
                        }
                    }
                }
                (Err(_), Err(_)) => both_err += 1,
                (Err(e), Ok(_)) => println!("round {round} {name}: postgres errors ({e}) but ivm returned rows: {q}"),
                (Ok(_), Err(e)) => {
                    failures += 1;
                    println!("ERROR round {round} {name}: ivm failed: {e} :: {q}");
                }
            }
        }
        println!(
            "round {round}: {matched}/{} views match ({total_rows} rows, {both_err} both-error) in {:?}",
            views.len(),
            t0.elapsed()
        );
        if failures > 0 && round > 0 {
            break;
        }
    }
    running.shutdown();
    if !keep {
        let _ = std::fs::remove_dir_all(&data);
    }
    drop(pg);
    if failures > 0 {
        println!("FAILED with {failures} mismatches (seed {seed})");
        std::process::exit(1);
    }
    println!("OK (seed {seed})");
    let _ = Duration::from_secs(0);
}

fn insert_sql(rng: &mut Rng, tables: &mut [Table], ti: usize) -> String {
    let id = tables[ti].next_id;
    tables[ti].next_id += 1;
    let t = tables[ti].clone();
    let vals: Vec<String> = t.cols.iter().map(|c| if c.name == "id" { id.to_string() } else { value(rng, c, tables) }).collect();
    format!("INSERT INTO {} VALUES ({});", t.name, vals.join(", "))
}

fn insert_row(rng: &mut Rng, tables: &mut [Table], ti: usize, up: &mut Client) {
    let sql = insert_sql(rng, tables, ti);
    up.simple_query(&sql).unwrap();
}
