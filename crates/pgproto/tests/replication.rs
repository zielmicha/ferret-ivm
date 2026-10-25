//! Logical replication (pgoutput) integration tests against PostgreSQL 17.

use std::time::{Duration, Instant};

use pgproto::client::SnapshotAction;
use pgproto::pgoutput::{LogicalMessage as LM, Relation, RelationColumn, TupleValue as TV};
use pgproto::testutil::{ClusterOptions, TempCluster};
use pgproto::{Client, Config, ErrorKind, Lsn, ReplicationEvent, ReplicationStream};

const OPTS: [(&str, &str); 3] = [
    ("proto_version", "1"),
    ("publication_names", "p"),
    ("messages", "true"),
];

fn cfg(pg: &TempCluster) -> Config {
    Config::parse(&pg.conninfo("postgres")).unwrap()
}

fn t(s: &str) -> TV {
    TV::Text(s.as_bytes().to_vec())
}

fn oid(c: &mut Client, rel: &str) -> u32 {
    c.query(&format!("select '{rel}'::regclass::oid"))
        .unwrap()
        .get(0, 0)
        .unwrap()
        .parse()
        .unwrap()
}

/// Receives XLogData messages until `done` returns true for one; replies to
/// keepalives that request it (without confirming anything).
fn collect_until(s: &mut ReplicationStream, mut done: impl FnMut(&LM) -> bool) -> Vec<(Lsn, LM)> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut out = Vec::new();
    loop {
        assert!(
            Instant::now() < deadline,
            "timed out; received so far: {out:#?}"
        );
        match s.recv(Some(Duration::from_millis(500))).unwrap() {
            None => {}
            Some(ReplicationEvent::Keepalive {
                reply_requested, ..
            }) => {
                if reply_requested {
                    s.send_standby_status(Lsn(0), Lsn(0), Lsn(0), false)
                        .unwrap();
                }
            }
            Some(ReplicationEvent::XLogData {
                wal_start,
                wal_end,
                message,
                ..
            }) => {
                assert!(wal_end >= wal_start);
                let d = done(&message);
                out.push((wal_start, message));
                if d {
                    return out;
                }
            }
        }
    }
}

fn is_message(m: &LM, content: &str) -> bool {
    matches!(m, LM::Message { content: c, .. } if c == content.as_bytes())
}

fn is_commit(m: &LM) -> bool {
    matches!(m, LM::Commit { .. })
}

/// Drops Relation messages; replaces Begin/Commit LSN/time/xid fields with
/// zeros so sequences can be compared structurally.
fn normalize(msgs: &[(Lsn, LM)]) -> Vec<LM> {
    msgs.iter()
        .filter(|(_, m)| !matches!(m, LM::Relation(_)))
        .map(|(_, m)| match m {
            LM::Begin { .. } => LM::Begin {
                final_lsn: Lsn(0),
                commit_time: 0,
                xid: 0,
            },
            LM::Commit { .. } => LM::Commit {
                flags: 0,
                commit_lsn: Lsn(0),
                end_lsn: Lsn(0),
                commit_time: 0,
            },
            LM::Message {
                transactional,
                prefix,
                content,
                ..
            } => LM::Message {
                transactional: *transactional,
                lsn: Lsn(0),
                prefix: prefix.clone(),
                content: content.clone(),
            },
            other => other.clone(),
        })
        .collect()
}

struct Txn {
    commit_lsn: Lsn,
    end_lsn: Lsn,
}

/// Checks Begin/Commit pairing and LSN ordering; returns the transactions.
fn check_transactions(msgs: &[(Lsn, LM)]) -> Vec<Txn> {
    let mut txns = Vec::new();
    let mut open: Option<(Lsn, u32)> = None;
    let mut last_end = Lsn(0);
    for (wal_start, m) in msgs {
        match m {
            LM::Begin {
                final_lsn,
                xid,
                commit_time,
            } => {
                assert!(open.is_none(), "nested Begin");
                assert!(*xid != 0 && *commit_time > 0);
                assert!(*final_lsn > last_end);
                open = Some((*final_lsn, *xid));
            }
            LM::Commit {
                flags,
                commit_lsn,
                end_lsn,
                commit_time,
            } => {
                let (final_lsn, _) = open.take().expect("Commit without Begin");
                assert_eq!(*flags, 0);
                assert_eq!(
                    final_lsn, *commit_lsn,
                    "Begin.final_lsn == Commit.commit_lsn"
                );
                assert!(end_lsn > commit_lsn);
                assert!(*commit_time > 0);
                // The XLogData header of a Commit carries the commit end LSN.
                assert_eq!(wal_start, end_lsn);
                last_end = *end_lsn;
                txns.push(Txn {
                    commit_lsn: *commit_lsn,
                    end_lsn: *end_lsn,
                });
            }
            LM::Message {
                transactional: false,
                ..
            } => {}
            _ => assert!(open.is_some(), "change outside a transaction: {m:?}"),
        }
    }
    assert!(open.is_none());
    txns
}

fn wait_confirmed_flush(sql: &mut Client, slot: &str, want: Lsn) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let r = sql
            .query(&format!(
                "select confirmed_flush_lsn from pg_replication_slots where slot_name = '{slot}'"
            ))
            .unwrap();
        let got: Lsn = r.get(0, 0).unwrap().parse().unwrap();
        if got == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "confirmed_flush_lsn is {got}, want {want}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn logical_replication_end_to_end() {
    let pg = TempCluster::start(ClusterOptions::default()).unwrap();
    let mut sql = Client::connect(&cfg(&pg)).unwrap();
    sql.simple_query(
        "create table t1 (id int primary key, v text, big text); \
         alter table t1 alter column big set storage external; \
         create table t2 (a int, b text); \
         alter table t2 replica identity full; \
         create table unpublished (x int); \
         create publication p for table t1, t2;",
    )
    .unwrap();
    let t1 = oid(&mut sql, "t1");
    let t2 = oid(&mut sql, "t2");

    let mut repl = Client::connect_replication(&cfg(&pg)).unwrap();
    // Replication connections default to unambiguous output settings.
    assert_eq!(repl.parameter("DateStyle"), Some("ISO, MDY"));
    assert_eq!(repl.parameter("IntervalStyle"), Some("postgres"));
    let sys = repl.identify_system().unwrap();
    assert_eq!(sys.timeline, 1);
    assert_eq!(sys.dbname.as_deref(), Some("postgres"));
    assert!(sys.xlogpos > Lsn(0));
    assert!(!sys.systemid.is_empty() && sys.systemid.bytes().all(|b| b.is_ascii_digit()));
    // SQL works on a replication=database connection too.
    assert_eq!(repl.query("select 1").unwrap().get(0, 0), Some("1"));

    let slot = repl.create_logical_slot("s1", "pgoutput", false).unwrap();
    assert_eq!(slot.slot_name, "s1");
    assert_eq!(slot.output_plugin.as_deref(), Some("pgoutput"));
    assert_eq!(slot.snapshot_name, None);
    assert!(slot.consistent_point >= sys.xlogpos);
    let r = sql
        .query("select plugin, slot_type, temporary, database from pg_replication_slots where slot_name = 's1'")
        .unwrap();
    assert_eq!(
        r.rows[0],
        ["pgoutput", "logical", "f", "postgres"].map(|s| Some(s.to_string()))
    );

    let mut stream = repl.start_logical_replication("s1", Lsn(0), &OPTS).unwrap();
    assert_eq!(
        sql.query("select active from pg_replication_slots where slot_name = 's1'")
            .unwrap()
            .get(0, 0),
        Some("t")
    );

    let big = "x".repeat(10000);
    sql.execute(&format!("insert into t1 values (1, 'a', '{big}')"))
        .unwrap(); // T1
    sql.execute("update t1 set v = 'b' where id = 1").unwrap(); // T2: big is unchanged TOAST
    sql.execute("update t1 set id = 2 where id = 1").unwrap(); // T3: PK change
    sql.execute("insert into unpublished values (1)").unwrap(); // not published: no output
    sql.execute("insert into t2 values (10, 'p'), (11, 'q')")
        .unwrap(); // T4
    sql.execute("update t2 set b = 'pp' where a = 10").unwrap(); // T5: FULL identity
    sql.execute("delete from t1 where id = 2").unwrap(); // T6
    sql.execute("delete from t2 where a = 11").unwrap(); // T7
    sql.execute("truncate t2").unwrap(); // T8
    sql.execute("select pg_logical_emit_message(true, 'ivm', 'hello')")
        .unwrap(); // T9
    sql.execute("select pg_logical_emit_message(false, 'ivm', 'nontx')")
        .unwrap(); // non-transactional
    sql.simple_query(
        "begin; insert into t1 values (5, 'five', null); insert into t1 values (6, 'six', 'small'); \
         select pg_logical_emit_message(true, 'ivm', 'last'); commit",
    )
    .unwrap(); // T10

    let msgs = collect_until(&mut stream, is_commit_after_last());
    let txns = check_transactions(&msgs);
    assert_eq!(txns.len(), 10, "{msgs:#?}");

    // Relation messages precede the first change of each relation.
    let rel_pos = |id: u32| {
        msgs.iter()
            .position(|(_, m)| matches!(m, LM::Relation(r) if r.id == id))
            .unwrap_or_else(|| panic!("no Relation for {id}"))
    };
    let first_change = |id: u32| {
        msgs.iter()
            .position(|(_, m)| matches!(m, LM::Insert { relation_id, .. } if *relation_id == id))
            .unwrap()
    };
    assert!(rel_pos(t1) < first_change(t1));
    assert!(rel_pos(t2) < first_change(t2));
    let LM::Relation(r1) = &msgs[rel_pos(t1)].1 else {
        unreachable!()
    };
    assert_eq!(
        r1,
        &Relation {
            id: t1,
            namespace: "public".into(),
            name: "t1".into(),
            replica_identity: b'd',
            columns: vec![
                RelationColumn {
                    is_key: true,
                    name: "id".into(),
                    type_oid: 23,
                    type_modifier: -1
                },
                RelationColumn {
                    is_key: false,
                    name: "v".into(),
                    type_oid: 25,
                    type_modifier: -1
                },
                RelationColumn {
                    is_key: false,
                    name: "big".into(),
                    type_oid: 25,
                    type_modifier: -1
                },
            ],
        }
    );
    let LM::Relation(r2) = &msgs[rel_pos(t2)].1 else {
        unreachable!()
    };
    assert_eq!(r2.replica_identity, b'f');
    assert_eq!(r2.name, "t2");
    assert!(
        r2.columns.iter().all(|c| c.is_key),
        "FULL identity: every column is key"
    );

    let begin = LM::Begin {
        final_lsn: Lsn(0),
        commit_time: 0,
        xid: 0,
    };
    let commit = LM::Commit {
        flags: 0,
        commit_lsn: Lsn(0),
        end_lsn: Lsn(0),
        commit_time: 0,
    };
    let msg = |tx: bool, c: &str| LM::Message {
        transactional: tx,
        lsn: Lsn(0),
        prefix: "ivm".into(),
        content: c.as_bytes().to_vec(),
    };
    let expected = vec![
        // T1
        begin.clone(),
        LM::Insert {
            relation_id: t1,
            new: vec![t("1"), t("a"), t(&big)],
        },
        commit.clone(),
        // T2: unchanged TOASTed column, no old tuple (key unchanged)
        begin.clone(),
        LM::Update {
            relation_id: t1,
            old: None,
            old_is_key: false,
            new: vec![t("1"), t("b"), TV::UnchangedToast],
        },
        commit.clone(),
        // T3: PK change -> old key ('K'), non-key columns null
        begin.clone(),
        LM::Update {
            relation_id: t1,
            old: Some(vec![t("1"), TV::Null, TV::Null]),
            old_is_key: true,
            new: vec![t("2"), t("b"), TV::UnchangedToast],
        },
        commit.clone(),
        // T4
        begin.clone(),
        LM::Insert {
            relation_id: t2,
            new: vec![t("10"), t("p")],
        },
        LM::Insert {
            relation_id: t2,
            new: vec![t("11"), t("q")],
        },
        commit.clone(),
        // T5: REPLICA IDENTITY FULL -> old row ('O')
        begin.clone(),
        LM::Update {
            relation_id: t2,
            old: Some(vec![t("10"), t("p")]),
            old_is_key: false,
            new: vec![t("10"), t("pp")],
        },
        commit.clone(),
        // T6
        begin.clone(),
        LM::Delete {
            relation_id: t1,
            old: vec![t("2"), TV::Null, TV::Null],
            old_is_key: true,
        },
        commit.clone(),
        // T7
        begin.clone(),
        LM::Delete {
            relation_id: t2,
            old: vec![t("11"), t("q")],
            old_is_key: false,
        },
        commit.clone(),
        // T8
        begin.clone(),
        LM::Truncate {
            options: 0,
            relation_ids: vec![t2],
        },
        commit.clone(),
        // T9
        begin.clone(),
        msg(true, "hello"),
        commit.clone(),
        // non-transactional message: no Begin/Commit
        msg(false, "nontx"),
        // T10
        begin.clone(),
        LM::Insert {
            relation_id: t1,
            new: vec![t("5"), t("five"), TV::Null],
        },
        LM::Insert {
            relation_id: t1,
            new: vec![t("6"), t("six"), t("small")],
        },
        msg(true, "last"),
        commit.clone(),
    ];
    assert_eq!(normalize(&msgs), expected);

    // Message LSNs are real positions within the transaction.
    for (_, m) in &msgs {
        if let LM::Message { lsn, .. } = m {
            assert!(*lsn > slot.consistent_point);
        }
    }

    // --- Restart from a later LSN (nothing confirmed yet) -----------------
    // Clean CopyDone handshake returns the connection to command mode...
    let mut repl = stream.stop().unwrap();
    assert_eq!(repl.query("select 2").unwrap().get(0, 0), Some("2"));
    // ...but PostgreSQL cannot restart logical streaming on it.
    let e = repl
        .start_logical_replication("s1", Lsn(0), &OPTS)
        .unwrap_err();
    assert_eq!(e.kind, ErrorKind::Protocol);
    let restart_at = txns[4].end_lsn; // after T5
    let repl = Client::connect_replication(&cfg(&pg)).unwrap();
    let mut stream = repl
        .start_logical_replication("s1", restart_at, &OPTS)
        .unwrap();
    let msgs2 = collect_until(&mut stream, is_commit_after_last());
    let txns2 = check_transactions(&msgs2);
    assert_eq!(txns2.len(), 5);
    assert_eq!(
        txns2[0].commit_lsn, txns[5].commit_lsn,
        "first replayed txn is T6"
    );
    let tail_start = expected
        .iter()
        .position(|m| matches!(m, LM::Delete { relation_id, .. } if *relation_id == t1))
        .unwrap()
        - 1;
    assert_eq!(normalize(&msgs2), expected[tail_start..].to_vec());

    // --- recv timeout: no data -> Ok(None), promptly ---------------------
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let start = Instant::now();
        match stream.recv(Some(Duration::from_millis(300))).unwrap() {
            None => {
                let el = start.elapsed();
                assert!(
                    el >= Duration::from_millis(290) && el < Duration::from_secs(3),
                    "{el:?}"
                );
                break;
            }
            Some(ReplicationEvent::Keepalive { .. }) => {}
            Some(other) => panic!("unexpected event {other:?}"),
        }
        assert!(Instant::now() < deadline);
    }
    // A zero timeout only polls.
    match stream.recv(Some(Duration::ZERO)).unwrap() {
        None | Some(ReplicationEvent::Keepalive { .. }) => {}
        Some(other) => panic!("unexpected event {other:?}"),
    }

    // --- Keepalive on request -------------------------------------------
    stream
        .send_standby_status(Lsn(0), Lsn(0), Lsn(0), true)
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match stream.recv(Some(Duration::from_secs(1))).unwrap() {
            Some(ReplicationEvent::Keepalive {
                wal_end,
                server_time,
                ..
            }) => {
                assert!(wal_end >= txns[9].end_lsn);
                assert!(server_time > 0);
                break;
            }
            Some(other) => panic!("unexpected event {other:?}"),
            None => assert!(Instant::now() < deadline, "no keepalive reply"),
        }
    }
    assert!(stream.last_wal_end() >= txns[9].end_lsn);

    // --- Standby status advances confirmed_flush_lsn ----------------------
    let last = txns[9].end_lsn;
    stream.send_standby_status(last, last, last, false).unwrap();
    wait_confirmed_flush(&mut sql, "s1", last);
    drop(stream); // sends Terminate

    // --- Resume from the slot's confirmed position ------------------------
    let deadline = Instant::now() + Duration::from_secs(10);
    while sql
        .query("select active from pg_replication_slots where slot_name = 's1'")
        .unwrap()
        .get(0, 0)
        != Some("f")
    {
        assert!(Instant::now() < deadline, "slot still active");
        std::thread::sleep(Duration::from_millis(20));
    }
    let repl = Client::connect_replication(&cfg(&pg)).unwrap();
    let mut stream = repl.start_logical_replication("s1", Lsn(0), &OPTS).unwrap();
    sql.execute("insert into t1 values (7, 'seven', null)")
        .unwrap();
    let msgs3 = collect_until(&mut stream, is_commit);
    let txns3 = check_transactions(&msgs3);
    assert_eq!(txns3.len(), 1);
    assert!(txns3[0].commit_lsn > last);
    assert_eq!(
        normalize(&msgs3),
        vec![
            begin,
            LM::Insert {
                relation_id: t1,
                new: vec![t("7"), t("seven"), TV::Null]
            },
            commit
        ]
    );
    // New session: the Relation message is sent again.
    assert!(matches!(&msgs3[1].1, LM::Relation(r) if r.id == t1));

    // --- Drop the slot on the same connection after stopping -------------
    let mut repl = stream.stop().unwrap();
    repl.drop_slot("s1", false).unwrap();
    assert_eq!(
        sql.query("select count(*) from pg_replication_slots")
            .unwrap()
            .get(0, 0),
        Some("0")
    );
    repl.close();
}

/// `done` predicate: the Commit following the transactional "last" message.
fn is_commit_after_last() -> impl FnMut(&LM) -> bool {
    let mut seen_last = false;
    move |m| {
        if is_message(m, "last") {
            seen_last = true;
        }
        seen_last && is_commit(m)
    }
}

#[test]
fn slots_errors_and_snapshots() {
    let pg = TempCluster::start(ClusterOptions::default()).unwrap();
    let mut sql = Client::connect(&cfg(&pg)).unwrap();
    sql.simple_query("create table t (id int primary key, n int8, f float8, ts timestamptz, iv interval, b bytea); create publication p for table t")
        .unwrap();
    let mut repl = Client::connect_replication(&cfg(&pg)).unwrap();

    // Errors from replication commands are Server errors; connection survives.
    let e = repl.drop_slot("nope", false).unwrap_err();
    assert_eq!(
        (e.kind, e.code()),
        (ErrorKind::Server, Some("42704")),
        "{e:?}"
    );
    repl.create_logical_slot("dup", "pgoutput", false).unwrap();
    let e = repl
        .create_logical_slot("dup", "pgoutput", false)
        .unwrap_err();
    assert_eq!(e.code(), Some("42710"), "{e:?}");
    let e = repl
        .create_logical_slot("bad", "no_such_plugin", false)
        .unwrap_err();
    assert_eq!(e.kind, ErrorKind::Server, "{e:?}");
    repl.drop_slot("dup", true).unwrap();

    // Exported snapshot usable from another session.
    let s = repl
        .create_logical_slot_with_snapshot("snap", "pgoutput", false, SnapshotAction::Export)
        .unwrap();
    let name = s.snapshot_name.clone().expect("snapshot name");
    sql.simple_query(&format!(
        "begin isolation level repeatable read; set transaction snapshot '{name}'; select count(*) from t; commit"
    ))
    .unwrap();
    // The snapshot is released by the next command on the replication conn.
    repl.query("select 1").unwrap();
    repl.drop_slot("snap", false).unwrap();

    // Temporary slot disappears when its session ends.
    let tmp = repl.create_logical_slot("tmp", "pgoutput", true).unwrap();
    assert_eq!(tmp.slot_name, "tmp");
    assert_eq!(
        sql.query("select temporary from pg_replication_slots where slot_name = 'tmp'")
            .unwrap()
            .get(0, 0),
        Some("t")
    );

    // START_REPLICATION on a missing slot fails with a Server error.
    let other = Client::connect_replication(&cfg(&pg)).unwrap();
    let e = other
        .start_logical_replication("missing", Lsn(0), &OPTS)
        .unwrap_err();
    assert_eq!(
        (e.kind, e.code()),
        (ErrorKind::Server, Some("42704")),
        "{e:?}"
    );

    // Binary tuple format, with the temporary slot, plus value formatting of
    // the default replication-session settings (DateStyle=ISO etc.).
    let mut stream = repl
        .start_logical_replication(
            "tmp",
            Lsn(0),
            &[
                ("proto_version", "1"),
                ("publication_names", "p"),
                ("binary", "true"),
            ],
        )
        .unwrap();
    sql.execute(
        "insert into t values (1, 2, 0.1, '2024-01-02 03:04:05+00', '1 day 2 hours', '\\x0102')",
    )
    .unwrap();
    let msgs = collect_until(&mut stream, is_commit);
    let ins = msgs
        .iter()
        .find_map(|(_, m)| match m {
            LM::Insert { new, .. } => Some(new.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!(ins[0], TV::Binary(1i32.to_be_bytes().to_vec()));
    assert_eq!(ins[1], TV::Binary(2i64.to_be_bytes().to_vec()));
    assert_eq!(ins[2], TV::Binary(0.1f64.to_be_bytes().to_vec()));
    assert_eq!(ins[5], TV::Binary(vec![1, 2]));
    drop(stream);
    let deadline = Instant::now() + Duration::from_secs(10);
    while sql
        .query("select count(*) from pg_replication_slots where slot_name = 'tmp'")
        .unwrap()
        .get(0, 0)
        != Some("0")
    {
        assert!(Instant::now() < deadline, "temporary slot not dropped");
        std::thread::sleep(Duration::from_millis(20));
    }

    // Text format honours the replication defaults (ISO dates, full float
    // precision); TimeZone can be set through Config::options.
    let mut c = cfg(&pg);
    c.options.push(("TimeZone".into(), "UTC".into()));
    let mut repl = Client::connect_replication(&c).unwrap();
    repl.create_logical_slot("txt", "pgoutput", true).unwrap();
    let mut stream = repl
        .start_logical_replication("txt", Lsn(0), &OPTS)
        .unwrap();
    sql.execute(
        "insert into t values (2, 3, 0.1, '2024-01-02 03:04:05+00', '1 day 2 hours', '\\x0102')",
    )
    .unwrap();
    let msgs = collect_until(&mut stream, is_commit);
    let ins = msgs
        .iter()
        .find_map(|(_, m)| match m {
            LM::Insert { new, .. } => Some(new.clone()),
            _ => None,
        })
        .unwrap();
    let txt: Vec<&str> = ins.iter().map(|v| v.as_str().unwrap()).collect();
    assert_eq!(
        txt,
        [
            "2",
            "3",
            "0.1",
            "2024-01-02 03:04:05+00",
            "1 day 02:00:00",
            "\\x0102"
        ]
    );

    // User settings in `options` are not overridden by the defaults.
    let mut c = cfg(&pg);
    c.options
        .push(("options".into(), "-c DateStyle=German".into()));
    let mut r = Client::connect_replication(&c).unwrap();
    assert_eq!(r.parameter("DateStyle"), Some("German, DMY"));
    assert_eq!(r.parameter("IntervalStyle"), Some("postgres"));
    r.query("select 1").unwrap();
    drop(r);

    // A missing publication surfaces as a Server error from recv().
    drop(stream);
    let repl = Client::connect_replication(&cfg(&pg)).unwrap();
    let mut r2 = Client::connect_replication(&cfg(&pg)).unwrap();
    r2.create_logical_slot("nopub", "pgoutput", false).unwrap();
    drop(r2);
    let mut stream = repl
        .start_logical_replication(
            "nopub",
            Lsn(0),
            &[("proto_version", "1"), ("publication_names", "nope")],
        )
        .unwrap();
    sql.execute("insert into t values (3, 0, 0, now(), '1s', '')")
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let err = loop {
        match stream.recv(Some(Duration::from_millis(500))) {
            Err(e) => break e,
            Ok(Some(ReplicationEvent::Keepalive {
                reply_requested: true,
                ..
            })) => stream
                .send_standby_status(Lsn(0), Lsn(0), Lsn(0), false)
                .unwrap(),
            Ok(_) => {}
        }
        assert!(
            Instant::now() < deadline,
            "no error for missing publication"
        );
    };
    assert_eq!(err.kind, ErrorKind::Server, "{err:?}");
    assert_eq!(err.code(), Some("42704"), "{err:?}");
    assert!(err.message.contains("nope"), "{err}");
    // The stream is unusable afterwards.
    assert_eq!(
        stream.recv(Some(Duration::ZERO)).unwrap_err().kind,
        ErrorKind::Closed
    );
}

#[test]
fn keepalives_with_short_sender_timeout() {
    let pg = TempCluster::start(ClusterOptions {
        extra_conf: vec!["wal_sender_timeout = '1s'".into()],
        ..Default::default()
    })
    .unwrap();
    let mut sql = Client::connect(&cfg(&pg)).unwrap();
    sql.simple_query("create table t (id int primary key); create publication p for table t")
        .unwrap();
    let mut repl = Client::connect_replication(&cfg(&pg)).unwrap();
    repl.create_logical_slot("k", "pgoutput", false).unwrap();
    let mut stream = repl.start_logical_replication("k", Lsn(0), &OPTS).unwrap();

    // While we answer keepalives the connection survives well past the
    // sender timeout.
    let start = Instant::now();
    let mut requested = 0;
    while start.elapsed() < Duration::from_secs(4) {
        if let Some(ReplicationEvent::Keepalive {
            reply_requested: true,
            wal_end,
            ..
        }) = stream.recv(Some(Duration::from_millis(200))).unwrap()
        {
            requested += 1;
            stream
                .send_standby_status(wal_end, wal_end, wal_end, false)
                .unwrap();
        }
    }
    assert!(
        requested >= 2,
        "expected keepalive requests, got {requested}"
    );
    sql.execute("insert into t values (1)").unwrap();
    let msgs = collect_until(&mut stream, is_commit);
    assert!(msgs.iter().any(|(_, m)| matches!(m, LM::Insert { .. })));

    // If we stop answering, the server drops us after ~1s.
    let start = Instant::now();
    let err = loop {
        match stream.recv(Some(Duration::from_millis(200))) {
            Ok(_) => assert!(
                start.elapsed() < Duration::from_secs(10),
                "not disconnected"
            ),
            Err(e) => break e,
        }
    };
    assert!(
        matches!(
            err.kind,
            ErrorKind::Closed | ErrorKind::Io | ErrorKind::Server
        ),
        "{err:?}"
    );
}
