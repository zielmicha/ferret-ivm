//! Engine self-differential test (no PostgreSQL needed): every materialized
//! view must equal its query run ad hoc, which reads the base tables and
//! evaluates the plan non-incrementally (`exec.rs`).
//!
//! A case is a random schema of local tables, random views from the
//! generator ivm-difftest uses (`src/bin/ivm-difftest/sqlgen.rs`: joins of
//! every kind, subqueries, aggregates, grouping sets, windows, top-k,
//! DISTINCT ON, set operations, recursive CTEs, ...), and a sequence of
//! actions: random DML (also primary key changes and rows changed several
//! times), checkpoints, clean restarts, crash restarts (no final checkpoint,
//! so recovery replays the log), views created and dropped on the way, and
//! reads AS OF earlier GSNs, which must return what the views held then. All
//! views are compared after every action.
//!
//! Cases come from proptest, so a failure shrinks to a minimal set of views
//! and actions, and is recorded under `proptest-regressions/`.
//! `PROPTEST_CASES=N` runs N cases (default 6), `PROPTEST_RNG_SEED=N` fixes
//! the seed; the long variant runs bigger cases:
//! `cargo test -p ivm --release --test selfdiff -- --ignored`.
//! `SELFDIFF_VERBOSE=1` prints the views and actions of each case.

#[path = "../src/bin/ivm-difftest/sqlgen.rs"]
mod sqlgen;
#[path = "../src/testkit.rs"]
mod testkit;

use ivm::coord::api::{ExecResult, SessionVars};
use ivm::coord::{self, EngineHandle};
use ivm::types::SqlType;
use ivm::types::text::format_datum;
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use testkit::{proptest_cases, Rng, TempDir};

#[derive(Clone, Debug)]
enum Action {
    /// One random transaction, its statements applied one by one.
    Dml(u64),
    Checkpoint,
    Restart,
    Crash,
    CreateView(u64),
    DropView(usize),
    /// Reads AS OF an earlier GSN must return what the views held then.
    AsOf(usize),
}

fn arb_action() -> impl Strategy<Value = Action> {
    prop_oneof![
        60 => any::<u64>().prop_map(Action::Dml),
        5 => Just(Action::Checkpoint),
        4 => Just(Action::Restart),
        5 => Just(Action::Crash),
        5 => any::<u64>().prop_map(Action::CreateView),
        2 => any::<usize>().prop_map(Action::DropView),
        6 => any::<usize>().prop_map(Action::AsOf),
    ]
}

type Rows = Result<Vec<String>, String>;

/// Whether reads AS OF a GSN before a restart are checked. Off while
/// recovery re-plans joins from current table sizes and re-hydrates views
/// whose plan changed, losing their history (engine test
/// `as_of_history_survives_restart_after_table_sizes_change`).
const AS_OF_ACROSS_RESTARTS: bool = false;

fn config(dir: &Path, cache_bytes: usize) -> coord::Config {
    coord::Config {
        data_dir: dir.to_path_buf(),
        sync: false,
        tick: None,
        // checkpoints only on request or under memory pressure: reproducible
        checkpoint_interval: Duration::from_secs(24 * 3600),
        cache_bytes,
        retention_gsns: 100_000,
    }
}

fn execute(h: &EngineHandle, sql: &str) -> Result<ExecResult, String> {
    let stmt = ivm::sql::parse_statement(sql).map_err(|e| e.message)?;
    h.execute(stmt, vec![], SessionVars::default(), String::new()).map_err(|e| format!("{} ({})", e.message, e.code))
}

/// Result rows as sorted text; floats rounded, and numerics trimmed where SQL
/// may keep any representation of equal values.
fn query(h: &EngineHandle, sql: &str, trim_numeric: bool) -> Rows {
    let ExecResult::Query { columns, rows } = execute(h, sql)? else { return Err(format!("{sql}: no rows")) };
    let mut out: Vec<String> = rows
        .iter()
        .map(|r| {
            r.iter()
                .zip(&columns)
                .map(|(d, c)| {
                    if d.is_null() {
                        return "∅".to_string();
                    }
                    let s = format_datum(d);
                    match c.ty {
                        SqlType::Float4 | SqlType::Float8 => match s.parse::<f64>() {
                            Ok(f) if f == 0.0 => "0".into(),
                            Ok(f) => format!("{f:.9e}"),
                            Err(_) => s,
                        },
                        SqlType::Numeric if trim_numeric && s.contains('.') => {
                            let t = s.trim_end_matches('0').trim_end_matches('.');
                            if t == "-0" || t.is_empty() { "0".into() } else { t.to_string() }
                        }
                        _ => s,
                    }
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect();
    out.sort();
    Ok(out)
}

struct View {
    name: String,
    /// The view it reads, if any.
    reads: Option<String>,
    /// What it must equal: its query, with views it reads inlined.
    query: String,
    trim: bool,
    created: u64,
}

struct Case {
    _tmp: TempDir,
    dir: PathBuf,
    cache_bytes: usize,
    running: Option<coord::Running>,
    tables: Vec<sqlgen::Table>,
    views: Vec<View>,
    next_view: usize,
    /// (gsn, view name -> rows) after each action
    history: Vec<(u64, BTreeMap<String, Rows>)>,
    log: Vec<String>,
}

impl Case {
    fn h(&self) -> EngineHandle {
        self.running.as_ref().unwrap().handle.clone()
    }

    fn note(&mut self, s: String) {
        if std::env::var_os("SELFDIFF_VERBOSE").is_some() {
            eprintln!("{s}");
        }
        self.log.push(s);
    }

    fn gsn(&self) -> u64 {
        query(&self.h(), "SHOW GSN", false).unwrap()[0].parse().unwrap()
    }

    fn create_view(&mut self, seed: u64) {
        let mut rng = Rng::new(seed);
        let h = self.h();
        let name = format!("v{}", self.next_view);
        // a view over an earlier view, or a fresh query
        let (q, def, reads) = if !self.views.is_empty() && rng.chance(20) {
            let src = &self.views[rng.index(self.views.len())];
            let (q, def) = sqlgen::view_over_view(&mut rng, &src.name, &src.query);
            (q, def, Some(src.name.clone()))
        } else {
            let q = sqlgen::Gen { rng: &mut rng, tables: &self.tables }.query();
            (q.clone(), q, None)
        };
        if execute(&h, &format!("CREATE MATERIALIZED VIEW {name} AS {def}")).is_ok() {
            self.next_view += 1;
            let created = self.gsn();
            self.note(format!("{name} (gsn {created}): {def}"));
            self.views.push(View { name, reads, trim: sqlgen::dedups(&q), query: q, created });
        }
    }

    fn restart(&mut self, crash: bool) {
        let r = self.running.take().unwrap();
        if crash { r.crash() } else { r.shutdown() }
        if !AS_OF_ACROSS_RESTARTS {
            self.history.clear();
        }
        self.running = Some(coord::start(config(&self.dir, self.cache_bytes)).expect("engine restarts"));
    }

    /// Compare every view with its query. Only rows are compared: whether
    /// an expression error is raised can depend on the order of evaluation,
    /// which SQL leaves open (and views reusing a failing shared node miss
    /// its errors: engine test `views_sharing_a_failing_node_report_its_errors`).
    fn check(&mut self, what: &str) {
        let h = self.h();
        let gsn = self.gsn();
        let mut state = BTreeMap::new();
        let mut notes = Vec::new();
        for v in &self.views {
            let got = query(&h, &format!("SELECT * FROM {}", v.name), v.trim);
            let want = query(&h, &v.query, v.trim);
            // a view over a failing view misses rows of the failure (engine
            // test views_hydrated_from_failing_nodes_report_their_errors)
            if let Some(src) = &v.reads
                && query(&h, &format!("SELECT * FROM {src}"), false).is_err()
            {
                notes.push(format!("-- {} not compared: it reads failing view {src}", v.name));
                state.insert(v.name.clone(), Err("reads a failing view".into()));
                continue;
            }
            match (&got, &want) {
                (Ok(_), Ok(_)) if got != want => {
                    self.fail(&format!("after {what}: view {} = {}\n  view:  {got:?}\n  query: {want:?}", v.name, v.query))
                }
                (Ok(_), Err(e)) | (Err(e), Ok(_)) => notes.push(format!("-- {} and its query differ in raising {e}", v.name)),
                _ => {}
            }
            state.insert(v.name.clone(), got);
        }
        for n in notes {
            self.note(n);
        }
        self.history.push((gsn, state));
        if self.history.len() > 30 {
            self.history.remove(rng_index(gsn, 30));
        }
    }

    fn check_as_of(&mut self, pick: usize) {
        if self.history.is_empty() {
            return;
        }
        let (gsn, state) = self.history[pick % self.history.len()].clone();
        let h = self.h();
        for v in &self.views {
            // (error records are not versioned: reads AS OF g check the
            // view's current ones)
            let Some(Ok(then)) = state.get(&v.name) else { continue };
            let then = &Ok(then.clone());
            if v.created > gsn {
                continue;
            }
            let got = query(&h, &format!("SELECT * FROM {} AS OF {gsn}", v.name), v.trim);
            if got.is_ok() && &got != then {
                self.fail(&format!("view {} AS OF {gsn}\n  now reads: {got:?}\n  held then: {then:?}", v.name));
            }
            let adhoc = query(&h, &format!("{} AS OF {gsn}", v.query), v.trim);
            if adhoc.is_ok() && &adhoc != then {
                self.fail(&format!("query of view {} AS OF {gsn} = {}\n  now reads: {adhoc:?}\n  held then: {then:?}", v.name, v.query));
            }
        }
    }

    fn fail(&self, msg: &str) -> ! {
        panic!("{msg}\ncase:\n  {}", self.log.join("\n  "));
    }
}

/// A deterministic index below `n` (which history entry to forget).
fn rng_index(seed: u64, n: usize) -> usize {
    Rng::new(seed).index(n)
}

fn run(schema_seed: u64, view_seeds: &[u64], actions: &[Action], name: &str) {
    let mut rng = Rng::new(schema_seed);
    let tmp = TempDir::new(name);
    let dir = tmp.path().join("data");
    let cache_bytes = if rng.chance(30) { 256 << 10 } else { 1 << 30 };
    let running = coord::start(config(&dir, cache_bytes)).expect("engine starts");
    let tables = sqlgen::gen_schema(&mut rng);
    let mut case = Case { _tmp: tmp, dir, cache_bytes, running: Some(running), tables, views: vec![], next_view: 0, history: vec![], log: vec![] };
    let h = case.h();
    for sql in case.tables.iter().map(sqlgen::create_table_sql).collect::<Vec<_>>() {
        execute(&h, &sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        case.note(format!("{sql};"));
    }
    // some views see the data arrive, others are hydrated from it
    let (early, late) = view_seeds.split_at(view_seeds.len() / 3);
    for s in early {
        case.create_view(*s);
    }
    for ti in 0..case.tables.len() {
        for _ in 0..(5 + rng.below(25)) {
            let sql = sqlgen::insert_sql(&mut rng, &mut case.tables, ti);
            execute(&h, &sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
            case.note(sql);
        }
    }
    for s in late {
        case.create_view(*s);
    }
    case.check("setup");
    for a in actions {
        match a {
            Action::Dml(seed) => {
                let mut rng = Rng::new(*seed);
                for sql in sqlgen::transaction(&mut rng, &mut case.tables, true) {
                    let r = execute(&case.h(), &sql);
                    case.note(match r {
                        Ok(_) => sql,
                        Err(e) => format!("{sql} -- {e}"),
                    });
                }
            }
            Action::Checkpoint => {
                execute(&case.h(), "CHECKPOINT").unwrap();
                case.note("CHECKPOINT".into());
            }
            Action::Restart => {
                case.note("-- restart".into());
                case.restart(false);
            }
            Action::Crash => {
                case.note("-- crash".into());
                case.restart(true);
            }
            Action::CreateView(seed) => case.create_view(*seed),
            Action::DropView(pick) => {
                if !case.views.is_empty() {
                    let i = pick % case.views.len();
                    let name = case.views[i].name.clone();
                    // (views reading it would have to go first)
                    let has_dependents = case.views.iter().any(|v| v.reads.as_deref() == Some(name.as_str()));
                    if !has_dependents && execute(&case.h(), &format!("DROP VIEW {name}")).is_ok() {
                        case.note(format!("DROP VIEW {name}"));
                        case.views.remove(i);
                    }
                }
            }
            Action::AsOf(pick) => {
                case.note(format!("-- as of history entry {pick}"));
                case.check_as_of(*pick);
            }
        }
        case.check(&format!("{a:?}"));
    }
    case.running.take().unwrap().shutdown();
}

fn proptest_config(cases: u32) -> ProptestConfig {
    ProptestConfig {
        cases,
        // shrinking reruns whole cases: bounded so that a failure reports in minutes
        max_shrink_iters: 1000,
        max_shrink_time: 900_000,
        // next to this file (the default looks for the crate root above it)
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource("proptest-regressions"))),
        ..ProptestConfig::default()
    }
}

proptest! {
    #![proptest_config(proptest_config(proptest_cases(6)))]

    #[test]
    fn views_match_ad_hoc_queries(
        schema_seed in any::<u64>(),
        views in prop::collection::vec(any::<u64>(), 1..25),
        actions in prop::collection::vec(arb_action(), 1..50),
    ) {
        run(schema_seed, &views, &actions, "selfdiff");
    }
}

proptest! {
    #![proptest_config(proptest_config(proptest_cases(50)))]

    #[test]
    #[ignore = "long-running; see the module docs"]
    fn views_match_ad_hoc_queries_long(
        schema_seed in any::<u64>(),
        views in prop::collection::vec(any::<u64>(), 1..40),
        actions in prop::collection::vec(arb_action(), 1..150),
    ) {
        run(schema_seed, &views, &actions, "selfdiff-long");
    }
}
