//! The work a step, a hydration or a DDL statement does must not grow with
//! what it cannot change: these tests count index entries visited and
//! dataflow nodes considered (`dataflow::visited`) rather than time, on an
//! engine without its threads, applying records directly.

use super::*;
use crate::dataflow::WORK;
use crate::types::{Datum, Row};

struct T {
    e: Engine,
    gsn: Gsn,
    _dir: crate::testkit::TempDir,
}

impl T {
    fn new(name: &str) -> T {
        let dir = crate::testkit::TempDir::new(name);
        let (env, _) = Env::open(&dir.path().join("store"), EnvOptions::default()).unwrap();
        let (seq, _) = unbounded();
        let mut e = Engine::new(Config { data_dir: dir.path().to_path_buf(), ..Default::default() }, env, seq, Arc::new(Shared::default()));
        e.replaying = false;
        T { e, gsn: 0, _dir: dir }
    }

    fn ddl(&mut self, sql: &str) {
        self.gsn += 1;
        self.e.apply_ddl(self.gsn, sql, "").unwrap_or_else(|e| panic!("{sql}: {e}"));
        self.e.processed = self.gsn;
    }

    fn id(&self, name: &str) -> ObjectId {
        self.e.catalog.by_name(name).unwrap().id
    }

    fn commit(&mut self, table: &str, changes: Vec<TableChange>) {
        let id = self.id(table);
        self.gsn += 1;
        let rec = LogRecord::Commit { source: 0, lsn: 0, changes: changes.into_iter().map(|c| (id, c)).collect() };
        self.e.apply_record(self.gsn, rec).unwrap();
    }

    fn upsert(&mut self, table: &str, rows: Vec<Row>) {
        self.commit(table, rows.into_iter().map(|row| TableChange::Upsert { row, unchanged: vec![] }).collect());
    }

    fn tick(&mut self, micros: i64) {
        self.gsn += 1;
        self.e.apply_record(self.gsn, LogRecord::Tick { micros }).unwrap();
    }

    /// Rows of a materialized view, sorted.
    fn rows(&mut self, view: &str) -> Vec<Row> {
        let node = self.e.rt.views[&self.id(view)].node;
        let mut rows: Vec<Row> = self.e.graph.snapshot(&mut self.e.env, node).unwrap().data.into_iter().map(|(r, _)| r).collect();
        rows.sort_by(|a, b| crate::types::datum::exact_row_cmp(a, b));
        rows
    }

    /// (index entries visited, nodes considered by steps) while running `f`.
    fn work(&mut self, f: impl FnOnce(&mut T)) -> (u64, u64) {
        WORK.with(|w| w.set((0, 0)));
        f(self);
        WORK.with(|w| w.get())
    }
}

fn int(v: i64) -> Datum {
    Datum::Int8(v)
}

/// A step considers the nodes that can produce something (the changed
/// table's consumers), not every node of the graph; idle clock ticks
/// consider none.
#[test]
fn steps_consider_only_nodes_that_can_change() {
    let mut t = T::new("work-steps");
    for i in 0..40 {
        t.ddl(&format!("CREATE TABLE t{i} (id bigint PRIMARY KEY, g bigint, v bigint)"));
        t.ddl(&format!("CREATE MATERIALIZED VIEW v{i} AS SELECT g, sum(v) AS s FROM t{i} WHERE v > 0 GROUP BY g"));
    }
    t.ddl("CREATE TABLE hot (id bigint PRIMARY KEY, v bigint)");
    let (_, nodes) = t.work(|t| t.upsert("hot", vec![vec![int(1), int(1)]]));
    assert_eq!(nodes, 1, "the table only (nothing reads it)");
    let (_, nodes) = t.work(|t| t.upsert("t7", vec![vec![int(1), int(2), int(3)]]));
    assert!(nodes <= 4, "{nodes} nodes for a change to one view's table");
    assert_eq!(t.rows("v7"), vec![vec![int(2), Datum::numeric(pgnumeric::Numeric::from_i64(3))]]);
    let (_, nodes) = t.work(|t| t.tick(1_767_225_600_000_000));
    assert_eq!(nodes, 0);
}

/// A subscription to a query frees the slots of its nodes and arrangements
/// (and their statistics) when it ends, and the next one reuses them.
#[test]
fn query_subscriptions_reuse_graph_slots() {
    let mut t = T::new("work-slots");
    t.ddl("CREATE TABLE s (id bigint PRIMARY KEY, g bigint, v bigint)");
    t.upsert("s", vec![vec![int(1), int(1), int(1)]]);
    let mut sizes = Vec::new();
    for _ in 0..20 {
        let text = "SELECT g, count(*) AS n FROM s WHERE v > 0 GROUP BY g";
        let q = crate::sql::parser::parse_single_query(text).unwrap();
        let ExecResult::Subscribe { cancel, .. } =
            t.e.subscribe(crate::sql::parser::SubscribeTarget::Query(Box::new(q), text.to_string()), false, false, vec![]).unwrap()
        else {
            panic!("not a subscription")
        };
        t.upsert("s", vec![vec![int(2), int(1), int(5)]]);
        cancel.store(true, Ordering::Release);
        t.e.prune_subscriptions();
        sizes.push((t.e.graph.nodes.len(), t.e.graph.arrs.len()));
    }
    assert!(sizes.windows(2).all(|w| w[0] == w[1]), "{sizes:?}");
    let live: std::collections::HashSet<NodeId> = t.e.graph.live_nodes().map(|n| n.id).collect();
    assert!(t.e.graph.stats.keys().all(|n| live.contains(n)), "statistics of freed nodes are kept");
}

/// Clock ticks release the events that became due without visiting those
/// released before (which stay behind as entries of weight zero), and a
/// row that is already expired when it arrives visits nothing either.
#[test]
fn temporal_filters_skip_released_events() {
    let mut t = T::new("work-temporal");
    let t0: i64 = 1_767_225_600_000_000; // 2026-01-01, Unix microseconds
    t.tick(t0);
    t.ddl("CREATE TABLE ev (id bigint PRIMARY KEY, ts timestamptz)");
    t.ddl("CREATE MATERIALIZED VIEW live AS SELECT id FROM ev WHERE ts > now() - interval '1 second'");
    let pg = |unix: i64| Datum::TimestampTz(unix - 946_684_800_000_000);
    let n = 2000;
    t.upsert("ev", (0..n).map(|i| vec![int(i), pg(t0 + i * 1000)]).collect());
    assert_eq!(t.rows("live").len(), n as usize);
    // everything expires, in steps
    let mut now = t0;
    for _ in 0..4 {
        now += 1_000_000;
        t.tick(now);
    }
    assert!(t.rows("live").is_empty());
    let (entries, _) = t.work(|t| {
        for i in 1..=100 {
            t.tick(now + i);
        }
    });
    assert_eq!(entries, 0, "idle ticks visited released events");
    let (entries, _) = t.work(|t| t.upsert("ev", vec![vec![int(n + 1), pg(now - 10_000_000)]]));
    assert_eq!(entries, 0);
    // events still pending are released when due
    t.upsert("ev", vec![vec![int(n + 2), pg(now + 1000)]]);
    assert_eq!(t.rows("live"), vec![vec![int(n + 2)]]);
    let (entries, _) = t.work(|t| t.tick(now + 2_000_000));
    assert_eq!(entries, 1);
    assert!(t.rows("live").is_empty());
}

/// A change after a top-k window (in a group with a large OFFSET) visits no
/// entries; one before it visits the few entries entering and leaving the
/// window, not the rows skipped by OFFSET.
#[test]
fn topk_reads_only_where_the_window_changes() {
    let mut t = T::new("work-topk");
    t.ddl("CREATE TABLE k (id bigint PRIMARY KEY, v bigint)");
    let n = 5000;
    t.upsert("k", (0..n).map(|i| vec![int(i), int(i * 10)]).collect());
    t.ddl("CREATE MATERIALIZED VIEW top AS SELECT id, v FROM k ORDER BY v LIMIT 10 OFFSET 2000");
    let window = |t: &mut T| t.rows("top").into_iter().map(|r| r[0].clone()).collect::<Vec<_>>();
    assert_eq!(window(&mut t), (2000..2010).map(int).collect::<Vec<_>>());
    let (entries, _) = t.work(|t| t.upsert("k", vec![vec![int(4000), int(40_001)]]));
    assert_eq!(entries, 0, "a change after the window");
    let (entries, _) = t.work(|t| t.upsert("k", vec![vec![int(10), int(-1)]]));
    assert_eq!(entries, 0, "a change before the window that moves nothing in or out");
    let (entries, _) = t.work(|t| t.upsert("k", vec![vec![int(10), int(40_005)]]));
    assert!(entries <= 6, "{entries} entries for a row leaving the rows before the window");
    assert_eq!(window(&mut t), (2001..2011).map(int).collect::<Vec<_>>());
    let (entries, _) = t.work(|t| t.upsert("k", vec![vec![int(2003), int(1)]]));
    assert!(entries <= 6, "{entries} entries for a row leaving the window");
    assert_eq!(window(&mut t), (2000..2011).filter(|i| *i != 2003).map(int).collect::<Vec<_>>());
}

/// Percentiles read the values at their ranks (no scan of the group), with
/// the group cache on.
#[test]
fn percentiles_do_not_scan_their_group() {
    let mut t = T::new("work-percentile");
    t.ddl("CREATE TABLE p (id bigint PRIMARY KEY, g bigint, v bigint)");
    t.upsert("p", (0..5000).map(|i| vec![int(i), int(i % 2), int(i)]).collect());
    t.ddl("CREATE MATERIALIZED VIEW pv AS SELECT g, percentile_disc(0.5) WITHIN GROUP (ORDER BY v) AS m, count(*) AS n FROM p GROUP BY g");
    for i in 0..3 {
        let (entries, _) = t.work(|t| t.upsert("p", vec![vec![int(i), int(i % 2), int(-1)]]));
        assert_eq!(entries, 0);
    }
    assert_eq!(t.rows("pv"), vec![vec![int(0), int(2498), int(2500)], vec![int(1), int(2499), int(2500)]]);
}

/// Hydrating a join reads the input it drives, not the other inputs to
/// count their expression errors: a filter that cannot fail has none, and
/// the errors of one that can are kept by its error tracker (computed when
/// the tracker is created, then read).
#[test]
fn join_hydration_reads_the_driven_input_only() {
    let mut t = T::new("work-join");
    t.ddl("CREATE TABLE small (id bigint PRIMARY KEY, hid bigint)");
    t.ddl("CREATE TABLE huge (id bigint PRIMARY KEY, status bigint, x bigint)");
    t.upsert("huge", (0..5000).map(|i| vec![int(i), int(i % 3), int(i % 7)]).collect());
    t.upsert("small", (1..=10).map(|i| vec![int(i), int(i * 7)]).collect());
    let (entries, _) = t.work(|t| t.ddl("CREATE MATERIALIZED VIEW j1 AS SELECT s.id, h.status FROM small s JOIN huge h ON h.id = s.hid WHERE h.status = 1"));
    assert!(entries < 100, "{entries} entries");
    assert_eq!(t.rows("j1").len(), 4);
    // (a division by zero for every row with x = 0)
    let q = "SELECT s.id, h.status FROM small s JOIN huge h ON h.id = s.hid WHERE 10 / h.x > 1";
    let (entries, _) = t.work(|t| t.ddl(&format!("CREATE MATERIALIZED VIEW j2 AS {q}")));
    assert!((5000..6000).contains(&entries), "{entries} entries: the tracker reads huge once");
    let (entries, _) = t.work(|t| t.ddl(&format!("CREATE MATERIALIZED VIEW j3 AS SELECT * FROM ({q}) j WHERE id > 0")));
    assert!(entries < 100, "{entries} entries");
    for v in ["j2", "j3"] {
        let node = t.e.rt.views[&t.id(v)].node;
        assert_eq!(t.e.graph.errors_of(node), vec![(("22012".to_string(), "division by zero".to_string()), 715)], "{v}");
    }
}

/// CREATE OR REPLACE of a materialized view with its definition reads
/// nothing (the new version is the old dataflow), and a different
/// definition reads the two versions only for subscribers, merging them.
#[test]
fn view_replacement_reads_only_for_subscribers() {
    let mut t = T::new("work-replace");
    t.ddl("CREATE TABLE r (id bigint PRIMARY KEY, v bigint)");
    t.upsert("r", (0..3000).map(|i| vec![int(i), int(i)]).collect());
    t.ddl("CREATE MATERIALIZED VIEW m AS SELECT id, v FROM r WHERE v >= 0");
    t.ddl("CREATE MATERIALIZED VIEW d AS SELECT count(*) AS n FROM m");
    let (entries, _) = t.work(|t| t.ddl("CREATE OR REPLACE MATERIALIZED VIEW m AS SELECT id, v FROM r WHERE v >= 0"));
    assert_eq!(entries, 0);
    let ExecResult::Subscribe { rx, .. } = t.e.subscribe(crate::sql::parser::SubscribeTarget::Name("m".into()), false, false, vec![]).unwrap() else {
        panic!("not a subscription")
    };
    let (entries, _) = t.work(|t| t.ddl("CREATE OR REPLACE MATERIALIZED VIEW m AS SELECT id, v FROM r WHERE v >= 10"));
    // hydrating the new version (it reads r) and merging the two versions
    assert!(entries <= 3 * 3000 + 100, "{entries} entries");
    let mut diff: Vec<(Row, i64)> = rx.try_iter().flat_map(|e| if let SubEvent::Data { rows, .. } = e { rows } else { vec![] }).collect();
    diff.sort_by(|a, b| crate::types::datum::exact_row_cmp(&a.0, &b.0));
    assert_eq!(diff, (0..10).map(|i| (vec![int(i), int(i)], -1)).collect::<Vec<_>>());
    assert_eq!(t.rows("d"), vec![vec![int(2990)]]);
}

/// A fresh read of `query` at `as_of`, parked or answered.
fn read(t: &mut T, h: &EngineHandle, query: &str, as_of: crate::sql::parser::AsOf) -> Receiver<Result<ExecResult>> {
    let (reply, rx) = bounded(1);
    let q = crate::sql::parser::parse_single_query(query).unwrap();
    let stmt = crate::sql::parser::Statement::Select { query: Box::new(q), as_of, text: query.to_string() };
    if let Some(r) = t.e.execute(stmt, vec![], SessionVars::default(), String::new(), reply.clone(), h).unwrap() {
        reply.send(Ok(r)).unwrap();
    }
    rx
}

/// A source whose upstream is never contacted (no threads here), with one
/// replicated table `st`.
fn source(t: &mut T) -> (EngineHandle, Receiver<EngineMsg>, ObjectId) {
    let table = crate::coord::ddl::DiscoveredTable {
        schema: "public".into(),
        name: "st".into(),
        columns: vec![crate::types::Column::new("id", crate::types::ColumnType::new(crate::types::SqlType::Int8))],
        pk: vec![0],
        replica_identity_full: false,
    };
    t.gsn += 1;
    let sql = "CREATE SOURCE src FROM POSTGRES CONNECTION 'host=/nonexistent' PUBLICATION 'p'";
    t.e.apply_ddl(t.gsn, sql, &serde_json::to_string(&vec![table]).unwrap()).unwrap();
    t.e.processed = t.gsn;
    let (tx, rx) = unbounded();
    let h = EngineHandle { tx, seq: t.e.seq.clone(), shared: t.e.shared.clone() };
    (h, rx, t.id("src"))
}

/// A read waiting for an LSN runs once every source's frontier passed it;
/// sources wake the engine when their frontier passes the smallest LSN
/// waited for, not at every advance.
#[test]
fn lsn_reads_are_woken_when_the_frontier_passes() {
    let mut t = T::new("work-lsn");
    let (h, wakes, src) = source(&mut t);
    let rx = read(&mut t, &h, "SELECT 1", crate::sql::parser::AsOf::AtLeastLsn("0/1000".into()));
    assert_eq!(t.e.parked.len(), 1);
    for lsn in (0x100..0x1000).step_by(0x100) {
        h.advance_frontier(src, lsn);
    }
    assert_eq!(wakes.try_iter().count(), 0, "advances below the LSN waited for");
    t.e.wake_parked(&h);
    assert_eq!((t.e.parked.len(), t.e.deferred.len()), (1, 0));
    h.advance_frontier(src, 0x1000);
    h.advance_frontier(src, 0x1100);
    assert_eq!(wakes.try_iter().count(), 1);
    t.e.wake_parked(&h);
    assert_eq!((t.e.parked.len(), t.e.deferred.len()), (0, 1));
    assert!(!t.e.shared.lsn_waiters.load(Ordering::Acquire));
    drop(rx);
}

/// A read waiting for the initial snapshot of a table is run again (and
/// planned again) when that table is loaded or the catalog changes, not at
/// every progress record of the source.
#[test]
fn snapshot_reads_wait_for_their_tables() {
    let mut t = T::new("work-snapshot");
    let (h, _wakes, src) = source(&mut t);
    let st = t.id("st");
    let state = |done: bool| {
        let mut s = crate::source::postgres::SourceState::default();
        s.tables.insert(st, crate::source::postgres::SnapState { done, ..Default::default() });
        serde_json::to_string(&s).unwrap()
    };
    t.e.source_states.insert(src, state(false));
    t.e.snap_epoch += 1;
    let rx = read(&mut t, &h, "SELECT count(*) FROM st", crate::sql::parser::AsOf::Fresh);
    assert_eq!(t.e.parked.len(), 1);
    for _ in 0..5 {
        t.gsn += 1;
        t.e.apply_record(t.gsn, LogRecord::Progress { source: src, lsn: 0, state: state(false) }).unwrap();
        t.e.wake_parked(&h);
        assert_eq!((t.e.parked.len(), t.e.deferred.len()), (1, 0), "re-run while its table loads");
    }
    t.gsn += 1;
    t.e.apply_record(t.gsn, LogRecord::Progress { source: src, lsn: 0, state: state(true) }).unwrap();
    t.e.wake_parked(&h);
    assert_eq!((t.e.parked.len(), t.e.deferred.len()), (0, 1));
    drop(rx);
}

/// A change to a partition recomputes the window values that can change,
/// reading the rows their frames reach (and summaries for the parts of
/// frames up to the partition's ends): an append or an update at the end
/// of a 3000-row partition reads a few rows for a running sum, a sliding
/// frame or row_number. Frames ending at the partition's end change for
/// every row before a change, which is then read once (not once per row).
#[test]
fn windows_read_only_what_a_change_reaches() {
    let mut t = T::new("work-window");
    let n = 3000;
    let cases = [
        ("running", "sum(v) OVER (PARTITION BY g ORDER BY id)"),
        ("sliding", "sum(v) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)"),
        ("minmax", "max(v) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)"),
        ("numbered", "row_number() OVER (PARTITION BY g ORDER BY id)"),
        ("ranked", "rank() OVER (PARTITION BY g ORDER BY v)"),
        ("lagged", "lag(v) OVER (PARTITION BY g ORDER BY id)"),
        ("suffix", "sum(v) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)"),
    ];
    for (name, f) in cases {
        t.ddl(&format!("CREATE TABLE {name} (id bigint PRIMARY KEY, g bigint, v bigint)"));
        t.upsert(name, (0..n).map(|i| vec![int(i), int(1), int(i)]).collect());
        t.ddl(&format!("CREATE MATERIALIZED VIEW {name}_v AS SELECT id, {f} AS x FROM {name}"));
        let last = |t: &mut T| t.rows(&format!("{name}_v")).last().unwrap()[1].clone();
        let (update, _) = t.work(|t| t.upsert(name, vec![vec![int(n - 1), int(1), int(n)]]));
        let (append, _) = t.work(|t| t.upsert(name, vec![vec![int(n), int(1), int(n + 1)]]));
        let reads = update.max(append);
        match name {
            "suffix" => assert!(reads <= n as u64 + 50, "{name}: {reads} entries"),
            _ => assert!(reads <= 20, "{name}: {update} and {append} entries"),
        }
        let want = match name {
            "running" => int((0..n - 1).sum::<i64>() + 2 * n + 1),
            "sliding" => int((n - 2) + n + (n + 1)),
            "minmax" => int(n + 1),
            "numbered" => int(n + 1),
            "ranked" => int(n + 1),
            "lagged" => int(n),
            _ => int(n + 1),
        };
        // (a numeric sum)
        let got = match last(&mut t) {
            Datum::Numeric(x) => int(x.to_string().parse().unwrap()),
            d => d,
        };
        assert_eq!(got, want, "{name}");
    }
}
