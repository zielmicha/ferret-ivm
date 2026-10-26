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

    /// Apply a DDL statement as its record would be (hydrating inline:
    /// the engine has no threads).
    fn ddl(&mut self, sql: &str) {
        self.gsn += 1;
        self.e.apply_ddl(self.gsn, sql, "").unwrap_or_else(|e| panic!("{sql}: {e}"));
        self.e.processed = self.gsn;
        let then = std::mem::take(&mut self.e.record_then);
        self.e.hydrate(self.gsn, then).unwrap();
    }

    /// Subscribe to `target` (started inline: the engine has no threads).
    fn subscribe(&mut self, target: crate::sql::parser::SubscribeTarget) -> ExecResult {
        let (reply, rx) = bounded(1);
        if let Some(r) = self.e.subscribe(target, false, false, vec![], reply).unwrap() {
            return r;
        }
        rx.try_recv().expect("a started subscription").unwrap()
    }

    /// Checkpoint, stop, and start again from the checkpoint (an engine
    /// without its threads, as `new`).
    fn restart(self) -> T {
        let T { mut e, gsn, _dir } = self;
        e.checkpoint().unwrap();
        drop(e);
        let (env, recovered) = Env::open(&_dir.path().join("store"), EnvOptions::default()).unwrap();
        let (seq, _) = unbounded();
        let mut e = Engine::new(Config { data_dir: _dir.path().to_path_buf(), ..Default::default() }, env, seq, Arc::new(Shared::default()));
        e.recover(recovered).unwrap();
        e.replaying = false;
        T { e, gsn, _dir }
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

/// A record's steps and delivery cost what it changes, not what consumes
/// the graph: with many subscriptions to other views, a commit to a table
/// nobody reads and a clock tick do no bookkeeping, and a change to one
/// subscribed view reaches its subscription only. (The set of nodes whose
/// rows steps return was collected from every subscription and sink for
/// every record: 3 µs per record with 1,000 subscriptions, on top of the
/// 0.05 µs of a tick.)
#[test]
fn records_cost_what_they_change_not_what_subscribes() {
    use crate::dataflow::BOOKKEEPING;
    let mut t = T::new("work-consumers");
    let mut subs = Vec::new();
    for i in 0..100 {
        t.ddl(&format!("CREATE TABLE t{i} (id bigint PRIMARY KEY, v bigint)"));
        t.ddl(&format!("CREATE MATERIALIZED VIEW v{i} AS SELECT id, v FROM t{i} WHERE v > 0"));
        let ExecResult::Subscribe { rx, .. } = t.subscribe(crate::sql::parser::SubscribeTarget::Name(format!("v{i}"))) else {
            panic!("not a subscription")
        };
        subs.push(rx);
    }
    t.ddl("CREATE TABLE hot (id bigint PRIMARY KEY, v bigint)");
    // (the index of consumers is built once after they changed)
    t.upsert("hot", vec![vec![int(0), int(0)]]);
    BOOKKEEPING.with(|b| b.set(0));
    let (_, nodes) = t.work(|t| {
        t.upsert("hot", vec![vec![int(1), int(1)]]);
        t.tick(1_767_225_600_000_000);
    });
    assert_eq!((BOOKKEEPING.with(|b| b.get()), nodes), (0, 1));
    let (_, nodes) = t.work(|t| t.upsert("t7", vec![vec![int(1), int(2)]]));
    assert_eq!((BOOKKEEPING.with(|b| b.get()), nodes), (0, 2));
    for (i, rx) in subs.iter().enumerate() {
        let rows: Vec<(Row, i64)> = rx.try_iter().flat_map(|e| if let SubEvent::Data { rows, .. } = e { rows.to_vec() } else { vec![] }).collect();
        assert_eq!(rows, if i == 7 { vec![(vec![int(1), int(2)], 1)] } else { vec![] }, "subscription {i}");
    }
}

/// The bookkeeping of a DDL statement visits what the statement changes,
/// not the graph or the catalog: the metadata tree's records (only the
/// changed objects and arrangements), the mirrored error state (only the
/// nodes whose arrangements changed), what is hydrated (only what is
/// pending), what lowering left unreferenced, relations looked up by name.
/// Creating a table and a view costs the same with 400 objects as with 40.
/// (Each DDL record rewrote every arrangement's record and checked every
/// node's error state, hydration visited every node, lookups by name
/// scanned the catalog, and a small view was hydrated on a fork of the
/// whole storage: creating 3,000 tables and views took 24 s, 0.8 s now.)
#[test]
fn ddl_work_does_not_grow_with_the_catalog() {
    use crate::dataflow::BOOKKEEPING;
    let mut t = T::new("work-ddl");
    let ddl = |t: &mut T, sql: String| {
        t.gsn += 1;
        t.e.apply_record(t.gsn, LogRecord::Ddl { req: 0, sql, extra: String::new() }).unwrap();
    };
    let cost = |t: &mut T, i: usize| {
        BOOKKEEPING.with(|b| b.set(0));
        ddl(t, format!("CREATE TABLE t{i} (id bigint PRIMARY KEY, g bigint, v bigint)"));
        ddl(t, format!("CREATE MATERIALIZED VIEW v{i} AS SELECT g, sum(v) AS s FROM t{i} WHERE v > 0 GROUP BY g"));
        BOOKKEEPING.with(|b| b.get())
    };
    let mut costs = Vec::new();
    for i in 0..200 {
        let c = cost(&mut t, i);
        if i == 20 || i == 199 {
            costs.push(c);
        }
    }
    assert!(costs[1] <= costs[0] + 4, "a table and a view cost {} with 40 objects, {} with 400", costs[0], costs[1]);
    assert!(costs[0] < 100, "{costs:?}");
    // the metadata tree holds every arrangement's record all the same, also
    // after views are dropped
    for i in 0..100 {
        ddl(&mut t, format!("DROP MATERIALIZED VIEW v{i}"));
    }
    let mut want: Vec<(Vec<u8>, TreeId)> = t.e.graph.arrangement_trees().into_iter().collect();
    let mut have = t.e.meta.arrangement_records();
    want.sort();
    have.sort();
    assert_eq!(have, want);
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
        let ExecResult::Subscribe { cancel, .. } = t.subscribe(crate::sql::parser::SubscribeTarget::Query(Box::new(q), text.to_string())) else {
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
    let ExecResult::Subscribe { rx, .. } = t.subscribe(crate::sql::parser::SubscribeTarget::Name("m".into())) else {
        panic!("not a subscription")
    };
    let (entries, _) = t.work(|t| t.ddl("CREATE OR REPLACE MATERIALIZED VIEW m AS SELECT id, v FROM r WHERE v >= 10"));
    // hydrating the new version (it reads r) and merging the two versions
    assert!(entries <= 3 * 3000 + 100, "{entries} entries");
    let mut diff: Vec<(Row, i64)> = rx.try_iter().flat_map(|e| if let SubEvent::Data { rows, .. } = e { rows.to_vec() } else { vec![] }).collect();
    diff.sort_by(|a, b| crate::types::datum::exact_row_cmp(&a.0, &b.0));
    assert_eq!(diff, (0..10).map(|i| (vec![int(i), int(i)], -1)).collect::<Vec<_>>());
    assert_eq!(t.rows("d"), vec![vec![int(2990)]]);
    // versions indexed differently (by all columns, by the group key) are
    // read a batch at a time and consolidated
    t.ddl("CREATE MATERIALIZED VIEW c AS SELECT v % 3 AS g, 7::bigint AS n FROM r WHERE v < 3");
    let ExecResult::Subscribe { rx, .. } = t.subscribe(crate::sql::parser::SubscribeTarget::Name("c".into())) else {
        panic!("not a subscription")
    };
    t.ddl("CREATE OR REPLACE MATERIALIZED VIEW c AS SELECT v % 3 AS g, count(*) AS n FROM r GROUP BY 1");
    let mut diff: Vec<(Row, i64)> = rx.try_iter().flat_map(|e| if let SubEvent::Data { rows, .. } = e { rows.to_vec() } else { vec![] }).collect();
    diff.sort_by(|a, b| crate::types::datum::exact_row_cmp(&a.0, &b.0));
    let want: Vec<(Row, i64)> = (0..3).flat_map(|g| [(vec![int(g), int(7)], -1), (vec![int(g), int(1000)], 1)]).collect();
    assert_eq!(diff, want);
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

/// A source's progress records change its state table by table
/// (`ProgressDelta`), and so do the metadata tree's records of it, one per
/// table: logging that one table of a thousand is copied writes one record
/// and keeps the loading tables without looking at the others, and the
/// whole state comes back after a restart. (The state was written whole,
/// as one JSON value, at every metadata write, and the loading tables were
/// recomputed from the catalog at the next read after any progress.)
#[test]
fn source_progress_costs_what_it_changes() {
    use crate::dataflow::BOOKKEEPING;
    let mut t = T::new("work-source-state");
    let n = 1000;
    let tables: Vec<crate::coord::ddl::DiscoveredTable> = (0..n)
        .map(|i| crate::coord::ddl::DiscoveredTable {
            schema: "public".into(),
            name: format!("s{i}"),
            columns: vec![crate::types::Column::new("id", crate::types::ColumnType::new(crate::types::SqlType::Int8))],
            pk: vec![0],
            replica_identity_full: false,
        })
        .collect();
    t.gsn += 1;
    let sql = "CREATE SOURCE src FROM POSTGRES CONNECTION 'host=/nonexistent' PUBLICATION 'p'".to_string();
    t.e.apply_record(t.gsn, LogRecord::Ddl { req: 0, sql, extra: serde_json::to_string(&tables).unwrap() }).unwrap();
    let src = t.id("src");
    assert_eq!(t.e.loading.len(), n);
    let ids: Vec<ObjectId> = (0..n).map(|i| t.id(&format!("s{i}"))).collect();
    let progress = |t: &mut T, table: ObjectId, done: bool| {
        let snap = crate::source::postgres::SnapState { done, ..Default::default() };
        let delta = serde_json::json!({ "tables": { table.to_string(): snap } }).to_string();
        t.gsn += 1;
        t.e.apply_record(t.gsn, LogRecord::ProgressDelta { source: src, lsn: 0, delta }).unwrap();
    };
    for (i, id) in ids.iter().enumerate() {
        progress(&mut t, *id, i % 2 == 0);
    }
    t.e.persist_meta(t.gsn).unwrap();
    let epoch = t.e.snap_epoch;
    BOOKKEEPING.with(|b| b.set(0));
    progress(&mut t, ids[1], true);
    t.e.persist_meta(t.gsn).unwrap();
    let cost = BOOKKEEPING.with(|b| b.get());
    assert!(cost < 20, "{cost} records for one table's progress");
    assert_eq!(t.e.loading.len(), n / 2 - 1);
    assert_eq!(t.e.snap_epoch, epoch + 1, "the loading tables changed once");
    // progress that finishes nothing does not move the epoch
    progress(&mut t, ids[3], false);
    assert_eq!(t.e.snap_epoch, epoch + 1);
    let state = t.e.source_states[&src].to_json();
    let t = t.restart();
    assert_eq!(t.e.source_states[&src].to_json(), state);
    assert_eq!(t.e.loading.len(), n / 2 - 1);
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
    t.e.source_states.insert(src, crate::source::postgres::SourceState::parse(&state(false)));
    t.e.source_changed(src, None);
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
        if name == "running" {
            // a window operator whose state is unknown (as recovered from an
            // older checkpoint) learns it when the graph is hydrated
            let (id, p) = t
                .e
                .graph
                .live_nodes()
                .find_map(|n| match &n.kind {
                    crate::dataflow::NodeKind::Window(p) => Some((n.id, p.clone())),
                    _ => None,
                })
                .unwrap();
            let errors = p.errors;
            t.e.graph.forget_window_errors(id);
            t.e.graph.hydrate(&mut t.e.env, t.gsn).unwrap();
            assert!(errors.lock().known());
        }
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

/// A window operator's partitions in error, and that they are known (also
/// without any in error), are kept in the metadata tree: recovery restores
/// them without reading the operator's input, and a change after it is
/// applied to part of a partition at once.
#[test]
fn window_error_state_is_recovered_without_reading_the_input() {
    let mut t = T::new("work-window-recovery");
    t.ddl("CREATE TABLE w (id bigint PRIMARY KEY, g bigint, v bigint)");
    t.upsert("w", (0..3000).map(|i| vec![int(i), int(i % 2), int(1 + i % 5)]).collect());
    let q = "SELECT id, ntile(v::int) OVER (PARTITION BY g ORDER BY id) AS nt, sum(v) OVER (PARTITION BY g ORDER BY id) AS s FROM w";
    t.ddl(&format!("CREATE MATERIALIZED VIEW wv AS {q}"));
    t.ddl("CREATE MATERIALIZED VIEW ok AS SELECT id, sum(v) OVER (PARTITION BY g ORDER BY id) AS s FROM w");
    // (partition 0 in error: ntile(0))
    t.upsert("w", vec![vec![int(10), int(0), int(0)]]);
    // (whether every window operator's state is known, partitions in error)
    let window_errors = |t: &T| -> (bool, usize) {
        let mut st = (true, 0);
        for n in t.e.graph.live_nodes() {
            if let crate::dataflow::NodeKind::Window(p) = &n.kind {
                let e = p.errors.lock();
                st = (st.0 && e.known(), st.1 + e.iter().count());
            }
        }
        st
    };
    assert_eq!(window_errors(&t), (true, 1));
    WORK.with(|w| w.set((0, 0)));
    let mut t = t.restart();
    let (entries, _) = WORK.with(|w| w.get());
    assert_eq!(entries, 0, "recovery read the windows' input");
    assert_eq!(window_errors(&t), (true, 1));
    // changed in part (not whole, as without the state known)
    let (entries, _) = t.work(|t| t.upsert("w", vec![vec![int(2999), int(1), int(7)]]));
    assert!(entries <= 40, "{entries} entries");
    t.upsert("w", vec![vec![int(10), int(0), int(3)]]);
    assert_eq!(window_errors(&t), (true, 0));
    // (as a view hydrated now)
    t.ddl(&format!("CREATE MATERIALIZED VIEW fresh AS {q}"));
    assert_eq!(t.rows("wv"), t.rows("fresh"));
}
