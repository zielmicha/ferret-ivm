//! Client integration tests against a real PostgreSQL 17 (TempCluster).

use std::io::Read;
use std::net::TcpListener;
use std::time::{Duration, Instant};

use pgproto::testutil::{ClusterOptions, TempCluster};
use pgproto::{Client, Config, ErrorKind};

fn cluster() -> TempCluster {
    TempCluster::start(ClusterOptions::default()).expect("start cluster")
}

fn connect(conninfo: &str) -> Client {
    Client::connect(&Config::parse(conninfo).unwrap()).expect("connect")
}

#[test]
fn connect_unix_and_tcp() {
    let pg = cluster();
    let mut c = connect(&pg.conninfo("postgres"));
    let r = c
        .query("select pg_backend_pid(), current_user, inet_client_addr() is null")
        .unwrap();
    assert_eq!(r.get(0, 0).unwrap(), c.backend_pid().to_string());
    assert_eq!(r.get(0, 1), Some("postgres"));
    assert_eq!(
        r.get(0, 2),
        Some("t"),
        "unix socket connection has no client addr"
    );
    assert!(c.parameter("server_version").unwrap().starts_with("17"));
    assert_eq!(c.parameter("client_encoding"), Some("UTF8"));
    assert_eq!(c.parameter("integer_datetimes"), Some("on"));
    assert_eq!(c.transaction_status(), b'I');

    let mut t = connect(&pg.tcp_conninfo("postgres"));
    let r = t.query("select host(inet_client_addr())").unwrap();
    assert_eq!(r.get(0, 0), Some("127.0.0.1"));

    // URI form, application_name and extra startup options.
    let uri = format!(
        "postgresql://postgres@127.0.0.1:{}/postgres?application_name=pgproto%20test",
        pg.port
    );
    let mut cfg = Config::parse(&uri).unwrap();
    cfg.options.push(("TimeZone".into(), "Asia/Tokyo".into()));
    cfg.options
        .push(("options".into(), "-c search_path=foo,public".into()));
    let mut u = Client::connect(&cfg).unwrap();
    let r = u
        .query("select current_setting('application_name'), current_setting('TimeZone'), current_setting('search_path')")
        .unwrap();
    assert_eq!(
        r.rows[0],
        vec![
            Some("pgproto test".to_string()),
            Some("Asia/Tokyo".to_string()),
            Some("foo,public".to_string())
        ]
    );
    assert_eq!(u.parameter("TimeZone"), Some("Asia/Tokyo"));
    assert_eq!(u.parameter("application_name"), Some("pgproto test"));
    u.close();

    // Unix socket via URI with percent-encoded directory.
    let dir = pg.socket_dir.to_string_lossy().replace('/', "%2F");
    let mut v = connect(&format!("postgres://postgres@{dir}:{}/postgres", pg.port));
    assert_eq!(v.query("select 42").unwrap().get(0, 0), Some("42"));
}

#[test]
fn connect_errors() {
    let pg = cluster();
    // Nonexistent database: FATAL 3D000 after authentication.
    let e = Client::connect(&Config::parse(&pg.conninfo("nope")).unwrap()).unwrap_err();
    assert_eq!(e.kind, ErrorKind::Server, "{e:?}");
    assert_eq!(e.code(), Some("3D000"));
    assert!(e.message.contains("does not exist"), "{e}");
    assert!(e.to_string().contains("SQLSTATE 3D000"));

    // Nonexistent role: FATAL 28000 during authentication -> Auth.
    let mut cfg = Config::parse(&pg.conninfo("postgres")).unwrap();
    cfg.user = "nobody".into();
    let e = Client::connect(&cfg).unwrap_err();
    assert_eq!(e.kind, ErrorKind::Auth, "{e:?}");
    assert_eq!(e.code(), Some("28000"));

    // Nothing listening.
    let port = {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let e = Client::connect(&Config::parse(&format!("host=127.0.0.1 port={port}")).unwrap())
        .unwrap_err();
    assert_eq!(e.kind, ErrorKind::Io, "{e:?}");
    assert!(e.message.contains("127.0.0.1"), "{e}");
    let e =
        Client::connect(&Config::parse("host=/nonexistent/dir port=5432").unwrap()).unwrap_err();
    assert_eq!(e.kind, ErrorKind::Io, "{e:?}");
    assert!(e.message.contains("/nonexistent/dir/.s.PGSQL.5432"), "{e}");
}

#[test]
fn connect_timeout_during_handshake() {
    // A server that accepts but never answers the startup packet.
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let h = std::thread::spawn(move || {
        let (mut s, _) = l.accept().unwrap();
        let mut buf = [0u8; 1024];
        // Read until the client gives up and closes.
        while let Ok(n) = s.read(&mut buf) {
            if n == 0 {
                break;
            }
        }
    });
    let t = Instant::now();
    let e = Client::connect(
        &Config::parse(&format!(
            "host=127.0.0.1 port={port} user=u dbname=d connect_timeout=1"
        ))
        .unwrap(),
    )
    .unwrap_err();
    assert_eq!(e.kind, ErrorKind::Timeout, "{e:?}");
    let el = t.elapsed();
    assert!(
        el >= Duration::from_millis(900) && el < Duration::from_secs(5),
        "{el:?}"
    );
    h.join().unwrap();
}

#[test]
fn auth_methods() {
    let pg = TempCluster::start(ClusterOptions {
        password: Some("super secret".into()),
        extra_conf: vec!["log_connections = on".into()],
        ..Default::default()
    })
    .unwrap();
    pg.psql(
        "postgres",
        "SET password_encryption = 'md5'; CREATE ROLE md5user LOGIN PASSWORD 'md5pw'; \
         SET password_encryption = 'scram-sha-256'; CREATE ROLE scramuser LOGIN PASSWORD 'scr@m pw'; \
         CREATE ROLE clearuser LOGIN PASSWORD 'clearpw'; CREATE ROLE trustuser LOGIN; \
         CREATE ROLE repluser LOGIN REPLICATION PASSWORD 'replpw';",
    )
    .unwrap();
    assert!(
        pg.psql(
            "postgres",
            "select rolpassword from pg_authid where rolname = 'md5user'"
        )
        .unwrap()
        .starts_with("md5")
    );
    assert!(
        pg.psql(
            "postgres",
            "select rolpassword from pg_authid where rolname = 'scramuser'"
        )
        .unwrap()
        .starts_with("SCRAM-SHA-256$")
    );
    pg.set_hba(
        "local all all trust\n\
         host all trustuser 127.0.0.1/32 trust\n\
         host all clearuser 127.0.0.1/32 password\n\
         host all md5user 127.0.0.1/32 md5\n\
         host all all 127.0.0.1/32 scram-sha-256\n\
         host replication all 127.0.0.1/32 scram-sha-256\n",
    )
    .unwrap();

    let tcp = |user: &str, pw: Option<&str>| {
        let mut s = format!(
            "host=127.0.0.1 port={} dbname=postgres user={user}",
            pg.port
        );
        if let Some(pw) = pw {
            s.push_str(&format!(" password='{pw}'"));
        }
        Client::connect(&Config::parse(&s).unwrap())
    };
    let whoami = |c: &mut Client| {
        c.query("select current_user").unwrap().rows[0][0]
            .clone()
            .unwrap()
    };

    // trust
    assert_eq!(whoami(&mut tcp("trustuser", None).unwrap()), "trustuser");
    // cleartext
    assert_eq!(
        whoami(&mut tcp("clearuser", Some("clearpw")).unwrap()),
        "clearuser"
    );
    // md5
    assert_eq!(
        whoami(&mut tcp("md5user", Some("md5pw")).unwrap()),
        "md5user"
    );
    // scram (password with a space and '@')
    assert_eq!(
        whoami(&mut tcp("scramuser", Some("scr@m pw")).unwrap()),
        "scramuser"
    );
    // superuser via tcp_conninfo (scram, password set by initdb --pwfile)
    let mut su = Client::connect(&Config::parse(&pg.tcp_conninfo("postgres")).unwrap()).unwrap();
    assert_eq!(whoami(&mut su), "postgres");
    // replication connection with scram
    let mut r = Client::connect_replication(
        &Config::parse(&format!(
            "host=127.0.0.1 port={} dbname=postgres user=repluser password=replpw",
            pg.port
        ))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        r.identify_system().unwrap().dbname.as_deref(),
        Some("postgres")
    );

    // Wrong passwords -> Auth with SQLSTATE 28P01, for every method.
    for user in ["clearuser", "md5user", "scramuser", "postgres"] {
        let e = tcp(user, Some("wrong")).unwrap_err();
        assert_eq!(e.kind, ErrorKind::Auth, "{user}: {e:?}");
        assert_eq!(e.code(), Some("28P01"), "{user}: {e:?}");
        assert!(e.message.contains("password authentication failed"), "{e}");
    }
    // Missing password -> client-side Auth error.
    for user in ["clearuser", "md5user", "scramuser"] {
        let e = tcp(user, None).unwrap_err();
        assert_eq!(e.kind, ErrorKind::Auth, "{user}: {e:?}");
        assert!(e.sqlstate.is_none());
    }

    // The server log confirms which method each successful login used.
    let log = std::fs::read_to_string(pg.log_path()).unwrap();
    for (user, method) in [
        ("clearuser", "password"),
        ("md5user", "md5"),
        ("scramuser", "scram-sha-256"),
    ] {
        assert!(
            log.contains(&format!("identity=\"{user}\" method={method}")),
            "no {method} login for {user} in log:\n{log}"
        );
    }
}

#[test]
fn simple_query_semantics() {
    let pg = cluster();
    let mut c = connect(&pg.conninfo("postgres"));

    let rs = c
        .simple_query(
            "create table t (id int primary key, v text); \
             insert into t values (1, 'a'), (2, null), (3, 'c'); \
             select id, v from t order by id; \
             update t set v = 'z' where id >= 2; \
             delete from t where id = 3; \
             select count(*) as n from t",
        )
        .unwrap();
    let tags: Vec<&str> = rs.iter().map(|r| r.tag.as_str()).collect();
    assert_eq!(
        tags,
        [
            "CREATE TABLE",
            "INSERT 0 3",
            "SELECT 3",
            "UPDATE 2",
            "DELETE 1",
            "SELECT 1"
        ]
    );
    let sel = &rs[2];
    assert_eq!(sel.columns.len(), 2);
    assert_eq!(sel.columns[0].name, "id");
    assert_eq!(sel.columns[0].type_oid, 23);
    assert_eq!(sel.columns[1].type_oid, 25);
    assert!(sel.columns[0].table_oid != 0);
    assert_eq!(sel.columns[0].column_attr, 1);
    assert_eq!(sel.columns[1].column_attr, 2);
    assert_eq!(
        sel.rows,
        vec![
            vec![Some("1".to_string()), Some("a".to_string())],
            vec![Some("2".to_string()), None],
            vec![Some("3".to_string()), Some("c".to_string())],
        ]
    );
    assert_eq!(rs[3].rows_affected(), 2);
    assert!(rs[0].columns.is_empty() && rs[0].rows.is_empty());

    // query(): last result with a row description.
    let r = c.query("select 1 as x; set work_mem = '8MB'").unwrap();
    assert_eq!(r.tag, "SELECT 1");
    assert_eq!(r.column_index("x"), Some(0));
    let r = c.query("set work_mem = '4MB'").unwrap();
    assert_eq!(r.tag, "SET");
    // A SELECT returning zero rows still has columns.
    let r = c.query("select * from t where false").unwrap();
    assert_eq!(
        (r.columns.len(), r.rows.len(), r.tag.as_str()),
        (2, 0, "SELECT 0")
    );

    // execute(): rows affected.
    assert_eq!(
        c.execute("insert into t select g, 'x' from generate_series(10, 14) g")
            .unwrap(),
        5
    );
    assert_eq!(c.execute("update t set v = 'y' where id >= 10").unwrap(), 5);
    assert_eq!(c.execute("create index on t (v)").unwrap(), 0);
    assert_eq!(c.execute("select * from t").unwrap(), 7);

    // Empty query string.
    assert!(c.simple_query("").unwrap().is_empty());
    assert!(c.simple_query(" ; ").unwrap().is_empty());
    assert_eq!(c.query("").unwrap(), Default::default());

    // Unicode round trip.
    let r = c
        .query("select 'zażółć 😀' as u, length('zażółć 😀')")
        .unwrap();
    assert_eq!(r.get(0, 0), Some("zażółć 😀"));
    assert_eq!(r.get(0, 1), Some("8"));

    // NUL bytes are rejected client-side, connection stays usable.
    let e = c.simple_query("select '\0'").unwrap_err();
    assert_eq!(e.kind, ErrorKind::Protocol);
    assert_eq!(c.query("select 1").unwrap().get(0, 0), Some("1"));
}

#[test]
fn errors_keep_connection_usable() {
    let pg = cluster();
    let mut c = connect(&pg.conninfo("postgres"));

    let e = c.simple_query("select * from no_such_table").unwrap_err();
    assert_eq!(e.kind, ErrorKind::Server);
    assert_eq!(e.code(), Some("42P01"));
    assert_eq!(e.message, "relation \"no_such_table\" does not exist");
    assert!(c.is_usable());
    assert_eq!(c.query("select 1").unwrap().get(0, 0), Some("1"));

    // Error in the middle of a multi-statement string: earlier results are
    // discarded (implicit transaction rolled back), later ones not run.
    c.execute("create table e (id int primary key)").unwrap();
    let e = c
        .simple_query(
            "insert into e values (1); insert into e values (1); insert into e values (2)",
        )
        .unwrap_err();
    assert_eq!(e.code(), Some("23505"));
    assert_eq!(e.detail.as_deref(), Some("Key (id)=(1) already exists."));
    assert_eq!(
        c.query("select count(*) from e").unwrap().get(0, 0),
        Some("0")
    );

    // Syntax error; hint field.
    let e = c.simple_query("selec 1").unwrap_err();
    assert_eq!(e.code(), Some("42601"));
    let e = c.simple_query("select 'a' + 1").unwrap_err();
    assert_eq!(e.code(), Some("22P02"));
    let e = c.simple_query("select now() + 1").unwrap_err();
    assert_eq!(e.code(), Some("42883"));
    assert!(e.hint.is_some(), "{e:?}");

    // Explicit transaction: failed state and recovery.
    c.execute("begin").unwrap();
    assert_eq!(c.transaction_status(), b'T');
    assert!(c.simple_query("select 1/0").unwrap_err().code() == Some("22012"));
    assert_eq!(c.transaction_status(), b'E');
    let e = c.simple_query("select 1").unwrap_err();
    assert_eq!(e.code(), Some("25P02"));
    c.execute("rollback").unwrap();
    assert_eq!(c.transaction_status(), b'I');
    assert_eq!(c.query("select 2").unwrap().get(0, 0), Some("2"));

    // RAISE with custom SQLSTATE, detail and hint.
    let e = c
        .simple_query(
            "do $$ begin raise exception 'boom %', 42 using errcode = 'P0042', detail = 'the detail', hint = 'the hint'; end $$",
        )
        .unwrap_err();
    assert_eq!(e.code(), Some("P0042"));
    assert_eq!(e.message, "boom 42");
    assert_eq!(e.detail.as_deref(), Some("the detail"));
    assert_eq!(e.hint.as_deref(), Some("the hint"));
    assert!(c.is_usable());
}

#[test]
fn notices_and_parameter_status() {
    let pg = cluster();
    let mut c = connect(&pg.conninfo("postgres"));
    c.simple_query(
        "do $$ begin raise notice 'hello %', 1; raise warning 'careful' using hint = 'h'; end $$",
    )
    .unwrap();
    let n = c.take_notices();
    assert_eq!(n.len(), 2, "{n:?}");
    assert_eq!(
        (
            n[0].severity.as_str(),
            n[0].message.as_str(),
            n[0].sqlstate.as_str()
        ),
        ("NOTICE", "hello 1", "00000")
    );
    assert_eq!(
        (
            n[1].severity.as_str(),
            n[1].message.as_str(),
            n[1].sqlstate.as_str()
        ),
        ("WARNING", "careful", "01000")
    );
    assert_eq!(n[1].hint.as_deref(), Some("h"));
    assert!(c.take_notices().is_empty());

    // GUC_REPORT parameters update asynchronously.
    c.execute("set timezone = 'America/New_York'").unwrap();
    assert_eq!(c.parameter("TimeZone"), Some("America/New_York"));
    c.execute("set application_name = 'abc'").unwrap();
    assert_eq!(c.parameter("application_name"), Some("abc"));
    c.execute("set datestyle = 'German'").unwrap();
    assert_eq!(c.parameter("DateStyle"), Some("German, DMY"));
}

#[test]
fn fatal_error_marks_connection_unusable() {
    let pg = cluster();
    let mut victim = connect(&pg.conninfo("postgres"));
    let mut admin = connect(&pg.conninfo("postgres"));
    let pid = victim.backend_pid();
    let r = admin
        .query(&format!("select pg_terminate_backend({pid})"))
        .unwrap();
    assert_eq!(r.get(0, 0), Some("t"));
    std::thread::sleep(Duration::from_millis(100));
    let e = victim.simple_query("select 1").unwrap_err();
    // Depending on timing we see the FATAL message or just the closed socket.
    assert!(
        e.code() == Some("57P01") || matches!(e.kind, ErrorKind::Closed | ErrorKind::Io),
        "{e:?}"
    );
    assert!(!victim.is_usable());
    let e2 = victim.simple_query("select 1").unwrap_err();
    assert_eq!(e2.kind, ErrorKind::Closed);
    // Other connections unaffected.
    assert_eq!(admin.query("select 1").unwrap().get(0, 0), Some("1"));

    // A statement timeout is a plain ERROR.
    let mut c = connect(&pg.conninfo("postgres"));
    c.execute("set statement_timeout = '50ms'").unwrap();
    let e = c.simple_query("select pg_sleep(5)").unwrap_err();
    assert_eq!(e.code(), Some("57014"));
    assert!(c.is_usable());
}

#[test]
fn large_results_and_values() {
    let pg = cluster();
    let mut c = connect(&pg.conninfo("postgres"));
    let r = c
        .query("select g, md5(g::text) from generate_series(1, 200000) g")
        .unwrap();
    assert_eq!(r.rows.len(), 200000);
    assert_eq!(r.rows[199999][0].as_deref(), Some("200000"));
    assert_eq!(r.tag, "SELECT 200000");

    // A single 20 MB value (one DataRow far larger than the read buffer).
    let r = c.query("select repeat('ab', 10 * 1024 * 1024)").unwrap();
    let v = r.get(0, 0).unwrap();
    assert_eq!(v.len(), 20 * 1024 * 1024);
    assert!(v.starts_with("abab") && v.ends_with("abab"));
    // Large query text.
    let big = format!("select length('{}')", "x".repeat(5_000_000));
    assert_eq!(c.query(&big).unwrap().get(0, 0), Some("5000000"));
}

#[test]
fn copy_to_stdout_and_from_stdin() {
    let pg = cluster();
    let mut c = connect(&pg.conninfo("postgres"));
    let r = c
        .query("copy (select g, 'v' || g from generate_series(1, 3) g) to stdout")
        .unwrap();
    assert_eq!(r.tag, "COPY 3");
    let lines: Vec<&str> = r
        .rows
        .iter()
        .map(|row| row[0].as_deref().unwrap())
        .collect();
    assert_eq!(lines, ["1\tv1", "2\tv2", "3\tv3"]);
    // COPY FROM STDIN is refused but the connection survives.
    c.execute("create table cp (a int)").unwrap();
    let e = c.simple_query("copy cp from stdin").unwrap_err();
    assert_eq!(e.kind, ErrorKind::Server);
    assert_eq!(e.code(), Some("57014"), "{e:?}");
    assert_eq!(c.query("select 1").unwrap().get(0, 0), Some("1"));
}

#[test]
fn abstract_unix_socket() {
    let name = format!("pgproto-abstract-{}", std::process::id());
    let pg = TempCluster::start(ClusterOptions {
        extra_conf: vec![format!("unix_socket_directories = '@{name}'")],
        ..Default::default()
    })
    .unwrap();
    let mut c = connect(&format!(
        "host=@{name} port={} user=postgres dbname=postgres",
        pg.port
    ));
    assert_eq!(
        c.query("select inet_client_addr() is null")
            .unwrap()
            .get(0, 0),
        Some("t")
    );
}

#[test]
fn simple_query_rows_borrows_values() {
    let pg = cluster();
    let mut c = connect(&pg.conninfo("postgres"));
    let sql = "BEGIN; SELECT 1 AS a, NULL::text AS b, '' AS c; \
               SELECT g, repeat('x', g) FROM generate_series(1, 3000) g; COMMIT";
    let mut seen: Vec<(usize, Vec<Option<String>>)> = Vec::new();
    let tags = c
        .simple_query_rows(sql, &mut |cols, r| {
            assert_eq!(cols.len(), r.len());
            let row = (0..r.len()).map(|i| r.get(i).map(|b| String::from_utf8(b.to_vec()).unwrap())).collect();
            seen.push((cols.len(), row));
            Ok(())
        })
        .unwrap();
    assert_eq!(tags, vec!["BEGIN", "SELECT 1", "SELECT 3000", "COMMIT"]);
    let owned: Vec<(usize, Vec<Option<String>>)> = c
        .simple_query(sql)
        .unwrap()
        .into_iter()
        .flat_map(|res| res.rows.into_iter().map(move |r| (r.len(), r)))
        .collect();
    assert_eq!(seen, owned);
    assert!(seen[0].1[1].is_none() && seen[0].1[2].as_deref() == Some(""));
    // an error from the callback is returned after the responses are drained
    let mut n = 0;
    let err = c
        .simple_query_rows("SELECT g FROM generate_series(1, 10) g", &mut |_, _| {
            n += 1;
            if n == 3 { Err("stop".into()) } else { Ok(()) }
        })
        .unwrap_err();
    assert!(err.to_string().contains("stop"), "{err}");
    assert_eq!(n, 3);
    assert_eq!(c.query("select 42").unwrap().get(0, 0), Some("42"));
    // server errors behave as in simple_query
    let err = c.simple_query_rows("SELECT 1/0", &mut |_, _| Ok(())).unwrap_err();
    assert_eq!(err.code(), Some("22012"));
    assert_eq!(c.query("select 7").unwrap().get(0, 0), Some("7"));
}
