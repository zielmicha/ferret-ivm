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
//! so recovery replays the log; sometimes the log ends in a torn record),
//! views created and dropped on the way, and reads AS OF earlier GSNs, which
//! must return what the views held then. Every view is also subscribed to
//! (some views' queries too, which builds their own dataflow): the stream of
//! changes must add up to the view's (query's) contents. All views are
//! compared after every action.
//!
//! Cases come from proptest, so a failure shrinks to a minimal set of views
//! and actions, and is recorded in `selfdiff.proptest-regressions` next to
//! this file.
//! `PROPTEST_CASES=N` runs N cases (default 10), `PROPTEST_RNG_SEED=N` fixes
//! the seed; the long variant runs bigger cases:
//! `cargo test -p ivm --release --test selfdiff -- --ignored`.
//! `SELFDIFF_VERBOSE=1` prints the views and actions of each case.

#[path = "../src/bin/ivm-difftest/sqlgen.rs"]
mod sqlgen;
#[path = "../src/testkit.rs"]
mod testkit;

use ivm::coord::api::{ExecResult, ResultColumn, SessionVars, SubEvent};
use ivm::coord::{self, EngineHandle};
use ivm::types::{Datum, SqlType};
use ivm::types::text::format_datum;
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use testkit::{proptest_cases, Rng, TempDir};

#[derive(Clone, Debug)]
enum Action {
    /// One random transaction, its statements applied one by one.
    Dml(u64),
    Checkpoint,
    Restart,
    /// A crash; with `true`, the input log also ends in a torn record (a
    /// write the crash cut short, never acknowledged).
    Crash(bool),
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
        5 => any::<bool>().prop_map(Action::Crash),
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

/// A result row as text; floats rounded, and numerics trimmed where SQL may
/// keep any representation of equal values.
fn row_text(r: &[Datum], columns: &[ResultColumn], trim_numeric: bool) -> String {
    r.iter()
        .zip(columns)
        .map(|(d, c)| {
            if d.is_null() {
                return "∅".to_string();
            }
            let s = format_datum(d);
            match c.ty {
                SqlType::Float4 | SqlType::Float8 => match s.parse::<f64>() {
                    Ok(f) => if f == 0.0 { "0".into() } else { format!("{f:.9e}") },
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
}

/// Result rows as sorted text (see `row_text`).
fn query(h: &EngineHandle, sql: &str, trim_numeric: bool) -> Rows {
    let ExecResult::Query { columns, rows } = execute(h, sql)? else { return Err(format!("{sql}: no rows")) };
    let mut out: Vec<String> = rows.iter().map(|r| row_text(r, &columns, trim_numeric)).collect();
    out.sort();
    Ok(out)
}

/// A subscription to a view (or to its query), with the rows its stream of
/// changes adds up to.
struct Sub {
    view: String,
    /// Subscribed to the view's query, not to the view.
    to_query: bool,
    columns: Vec<ResultColumn>,
    rx: crossbeam_channel::Receiver<SubEvent>,
    cancel: Arc<AtomicBool>,
    rows: BTreeMap<String, i64>,
    failed: Option<String>,
}

impl Sub {
    /// Apply the changes delivered so far (every applied commit delivers
    /// before it returns).
    fn drain(&mut self, trim: bool) {
        while let Ok(ev) = self.rx.try_recv() {
            match ev {
                SubEvent::Data { rows, .. } => {
                    for (r, w) in rows {
                        let k = row_text(&r, &self.columns, trim);
                        let e = self.rows.entry(k.clone()).or_default();
                        *e += w;
                        if *e == 0 {
                            self.rows.remove(&k);
                        }
                    }
                }
                SubEvent::Progress { .. } => {}
                SubEvent::Error(e) => self.failed = Some(e.message),
            }
        }
    }

    fn contents(&self) -> Vec<String> {
        let mut v: Vec<String> = self.rows.iter().flat_map(|(r, w)| std::iter::repeat_n(r.clone(), (*w).max(0) as usize)).collect();
        v.sort();
        v
    }
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
    subs: Vec<Sub>,
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
            let to_query = rng.chance(30);
            self.views.push(View { name, reads, trim: sqlgen::dedups(&q), query: q, created });
            self.subscribe(self.views.len() - 1, to_query);
        }
    }

    /// Subscribe to view `i` (and, with `to_query`, to its query as well).
    fn subscribe(&mut self, i: usize, to_query: bool) {
        let h = self.h();
        let v = &self.views[i];
        let mut targets = vec![(false, v.name.clone())];
        if to_query {
            targets.push((true, format!("({})", v.query)));
        }
        for (q, target) in targets {
            match execute(&h, &format!("SUBSCRIBE TO {target}")) {
                Ok(ExecResult::Subscribe { columns, rx, cancel, .. }) => {
                    self.subs.push(Sub { view: v.name.clone(), to_query: q, columns, rx, cancel, rows: BTreeMap::new(), failed: None })
                }
                Ok(other) => panic!("SUBSCRIBE TO {target}: {other:?}"),
                // the query may not be accepted outside a view (e.g. ORDER BY)
                Err(e) => self.log.push(format!("-- SUBSCRIBE TO {target} -- {e}")),
            }
        }
    }

    fn restart(&mut self, crash: bool, torn_log: bool) {
        let r = self.running.take().unwrap();
        if crash { r.crash() } else { r.shutdown() }
        if torn_log {
            let mut segs: Vec<PathBuf> = std::fs::read_dir(self.dir.join("log")).unwrap().map(|e| e.unwrap().path()).collect();
            segs.sort();
            let last = segs.last().expect("a log segment");
            let mut data = std::fs::read(last).unwrap();
            // a frame header promising more than follows, then some bytes
            data.extend_from_slice(&[200, 0, 0, 0, 1, 2, 3, 4, 9, 9, 9]);
            std::fs::write(last, data).unwrap();
        }
        if !AS_OF_ACROSS_RESTARTS {
            self.history.clear();
        }
        // subscriptions end with the engine: subscribe again
        let resubscribe: Vec<(usize, bool)> =
            (0..self.views.len()).map(|i| (i, self.subs.iter().any(|s| s.to_query && s.view == self.views[i].name))).collect();
        self.subs.clear();
        self.running = Some(coord::start(config(&self.dir, self.cache_bytes)).expect("engine restarts"));
        for (i, q) in resubscribe {
            self.subscribe(i, q);
        }
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
            // what subscriptions to it (or to its query) add up to
            for sub in self.subs.iter_mut().filter(|s| s.view == v.name) {
                sub.drain(v.trim);
                let (expect, of) = if sub.to_query { (&want, "its query") } else { (&got, "the view") };
                if let (Ok(expect), None) = (expect, &sub.failed) {
                    let acc = sub.contents();
                    if &acc != expect {
                        let msg = format!("after {what}: subscription to {} of {} = {}\n  stream adds up to: {acc:?}\n  {of} holds:       {expect:?}", if sub.to_query { "the query" } else { "view" }, v.name, v.query);
                        panic!("{msg}\ncase:\n  {}", self.log.join("\n  "));
                    }
                }
                sub.failed = None;
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
    let mut case = Case { _tmp: tmp, dir, cache_bytes, running: Some(running), tables, views: vec![], next_view: 0, history: vec![], subs: vec![], log: vec![] };
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
                case.restart(false, false);
            }
            Action::Crash(torn) => {
                case.note(format!("-- crash{}", if *torn { " (torn log tail)" } else { "" }));
                case.restart(true, *torn);
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
                        for sub in case.subs.iter().filter(|s| s.view == name) {
                            sub.cancel.store(true, std::sync::atomic::Ordering::Release);
                        }
                        case.subs.retain(|s| s.view != name);
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
    #![proptest_config(proptest_config(proptest_cases(10)))]

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
