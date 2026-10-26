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
