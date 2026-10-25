//! Server-side codec tests: frontend message decoding, BackendWriter output
//! decoded by our client, a fake walsender, and psql talking to a tiny server.

use std::io::{BufReader, Cursor, Write};
use std::net::{TcpListener, TcpStream};
use std::process::Command;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use pgproto::pgoutput::LogicalMessage;
use pgproto::server::{
    BackendWriter, FieldDescription, FrontendMessage as FM, StartupPacket, read_frontend_message,
    read_startup_packet,
};
use pgproto::{Client, Config, ErrorKind, Lsn, ReplicationEvent};

// ---------------------------------------------------------------------------
// Hand encoding helpers (independent of the library's encoders)
// ---------------------------------------------------------------------------

fn msg(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut v = vec![tag];
    v.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    v.extend_from_slice(body);
    v
}

fn cstr(s: &str) -> Vec<u8> {
    let mut v = s.as_bytes().to_vec();
    v.push(0);
    v
}

fn cat(parts: &[&[u8]]) -> Vec<u8> {
    parts.concat()
}

fn startup_bytes(code: u32, rest: &[u8]) -> Vec<u8> {
    let mut v = ((rest.len() + 8) as u32).to_be_bytes().to_vec();
    v.extend_from_slice(&code.to_be_bytes());
    v.extend_from_slice(rest);
    v
}

#[test]
fn frontend_messages_roundtrip() {
    let bind_body = cat(&[
        &cstr("portal1"),
        &cstr("stmt1"),
        &2i16.to_be_bytes(),
        &0i16.to_be_bytes(),
        &1i16.to_be_bytes(),
        &3i16.to_be_bytes(),
        &2i32.to_be_bytes(),
        b"42",
        &(-1i32).to_be_bytes(),
        &0i32.to_be_bytes(),
        &1i16.to_be_bytes(),
        &1i16.to_be_bytes(),
    ]);
    let parse_body = cat(&[
        &cstr("s1"),
        &cstr("select $1::int, $2"),
        &2i16.to_be_bytes(),
        &23u32.to_be_bytes(),
        &0u32.to_be_bytes(),
    ]);
    let stream = cat(&[
        &msg(b'Q', &cstr("select 'zażółć'")),
        &msg(b'P', &parse_body),
        &msg(b'B', &bind_body),
        &msg(b'D', &cat(&[b"S", &cstr("s1")])),
        &msg(b'D', &cat(&[b"P", &cstr("")])),
        &msg(b'E', &cat(&[&cstr("portal1"), &100i32.to_be_bytes()])),
        &msg(b'C', &cat(&[b"P", &cstr("portal1")])),
        &msg(b'S', &[]),
        &msg(b'H', &[]),
        &msg(b'p', b"secret\0"),
        &msg(
            b'p',
            &cat(&[&cstr("SCRAM-SHA-256"), &4i32.to_be_bytes(), b"n,,x"]),
        ),
        &msg(b'd', b"\x00\x01raw"),
        &msg(b'c', &[]),
        &msg(b'f', &cstr("client gave up")),
        &msg(b'F', &[0, 0, 0, 1, 0, 0]),
        &msg(b'X', &[]),
    ]);
    let mut r = Cursor::new(stream);
    let mut got = Vec::new();
    while let Some(m) = read_frontend_message(&mut r).unwrap() {
        got.push(m);
    }
    assert_eq!(
        got,
        vec![
            FM::Query("select 'zażółć'".into()),
            FM::Parse {
                name: "s1".into(),
                query: "select $1::int, $2".into(),
                param_types: vec![23, 0]
            },
            FM::Bind {
                portal: "portal1".into(),
                statement: "stmt1".into(),
                param_formats: vec![0, 1],
                params: vec![Some(b"42".to_vec()), None, Some(vec![])],
                result_formats: vec![1],
            },
            FM::Describe {
                kind: b'S',
                name: "s1".into()
            },
            FM::Describe {
                kind: b'P',
                name: "".into()
            },
            FM::Execute {
                portal: "portal1".into(),
                max_rows: 100
            },
            FM::Close {
                kind: b'P',
                name: "portal1".into()
            },
            FM::Sync,
            FM::Flush,
            FM::Password(b"secret\0".to_vec()),
            FM::Password(cat(&[&cstr("SCRAM-SHA-256"), &4i32.to_be_bytes(), b"n,,x"])),
            FM::CopyData(b"\x00\x01raw".to_vec()),
            FM::CopyDone,
            FM::CopyFail("client gave up".into()),
            FM::FunctionCall,
            FM::Terminate,
        ]
    );
}

#[test]
fn frontend_message_errors() {
    // Clean EOF.
    assert_eq!(
        read_frontend_message(&mut Cursor::new(Vec::<u8>::new())).unwrap(),
        None
    );
    // EOF inside the header / body.
    let e = read_frontend_message(&mut Cursor::new(vec![b'Q', 0, 0])).unwrap_err();
    assert_eq!(e.kind, ErrorKind::Closed);
    let mut m = msg(b'Q', &cstr("select 1"));
    m.truncate(m.len() - 2);
    assert_eq!(
        read_frontend_message(&mut Cursor::new(m)).unwrap_err().kind,
        ErrorKind::Closed
    );
    // Invalid length.
    let e = read_frontend_message(&mut Cursor::new(vec![b'Q', 0, 0, 0, 3])).unwrap_err();
    assert_eq!(e.kind, ErrorKind::Protocol);
    // Unknown message type.
    let e = read_frontend_message(&mut Cursor::new(msg(b'?', &[]))).unwrap_err();
    assert_eq!(e.kind, ErrorKind::Protocol);
    // Invalid UTF-8 -> SQLSTATE 22021, usable with BackendWriter::error.
    let e = read_frontend_message(&mut Cursor::new(msg(b'Q', b"select '\xff'\0"))).unwrap_err();
    assert_eq!(e.code(), Some("22021"));
    // Missing terminator / truncated fields.
    let e = read_frontend_message(&mut Cursor::new(msg(b'Q', b"select 1"))).unwrap_err();
    assert_eq!(e.kind, ErrorKind::Protocol);
    let e = read_frontend_message(&mut Cursor::new(msg(b'E', &cstr("p")))).unwrap_err();
    assert_eq!(e.kind, ErrorKind::Protocol);
    let e =
        read_frontend_message(&mut Cursor::new(msg(b'D', &cat(&[b"X", &cstr("")])))).unwrap_err();
    assert_eq!(e.kind, ErrorKind::Protocol);
    // Bind with an absurd parameter count.
    let body = cat(&[
        &cstr(""),
        &cstr(""),
        &0i16.to_be_bytes(),
        &1000i16.to_be_bytes(),
    ]);
    assert_eq!(
        read_frontend_message(&mut Cursor::new(msg(b'B', &body)))
            .unwrap_err()
            .kind,
        ErrorKind::Protocol
    );

    let mut w = BackendWriter::new();
    w.error(&e);
    assert!(!w.is_empty());
}

#[test]
fn startup_packets() {
    let rd = |b: Vec<u8>| read_startup_packet(&mut Cursor::new(b));
    assert_eq!(
        rd(startup_bytes(80877103, &[])).unwrap(),
        StartupPacket::SslRequest
    );
    assert_eq!(
        rd(startup_bytes(80877104, &[])).unwrap(),
        StartupPacket::GssEncRequest
    );
    assert_eq!(
        rd(startup_bytes(
            80877102,
            &cat(&[&1234u32.to_be_bytes(), &0xdead_beefu32.to_be_bytes()])
        ))
        .unwrap(),
        StartupPacket::Cancel {
            pid: 1234,
            secret: 0xdead_beef
        }
    );
    let p = rd(startup_bytes(
        196608,
        &cat(&[
            &cstr("user"),
            &cstr("alice"),
            &cstr("database"),
            &cstr("db"),
            &cstr("options"),
            &cstr("-c x=y"),
            b"\0",
        ]),
    ))
    .unwrap();
    assert_eq!(
        p,
        StartupPacket::Startup {
            version: 196608,
            params: vec![
                ("user".into(), "alice".into()),
                ("database".into(), "db".into()),
                ("options".into(), "-c x=y".into()),
            ]
        }
    );
    assert_eq!(p.param("database"), Some("db"));
    assert_eq!(p.param("nope"), None);
    // Errors.
    assert!(rd(vec![0, 0, 0, 4]).is_err());
    assert!(rd(startup_bytes(2 << 16, &[0])).is_err());
    assert!(rd(vec![0, 0, 0x30, 0, 0, 3, 0, 0]).is_err()); // too long
    assert_eq!(rd(vec![]).unwrap_err().kind, ErrorKind::Closed);
    assert!(rd(startup_bytes(196608, &cstr("user"))).is_err()); // unterminated
}

#[test]
fn backend_writer_encoding() {
    let mut w = BackendWriter::new();
    assert!(w.is_empty());
    w.auth_ok();
    w.parse_complete();
    w.bind_complete();
    w.close_complete();
    w.no_data();
    w.portal_suspended();
    w.empty_query_response();
    w.copy_done();
    w.ready_for_query(b'T');
    w.parameter_description(&[23, 25]);
    w.backend_key_data(7, 9);
    w.auth_md5_password([1, 2, 3, 4]);
    w.auth_cleartext_password();
    w.copy_out_response(0, &[0, 1]);
    w.data_row(&[Some(b"ab"), None, Some(b"")]);
    let expected = cat(&[
        &msg(b'R', &0i32.to_be_bytes()),
        &msg(b'1', &[]),
        &msg(b'2', &[]),
        &msg(b'3', &[]),
        &msg(b'n', &[]),
        &msg(b's', &[]),
        &msg(b'I', &[]),
        &msg(b'c', &[]),
        &msg(b'Z', b"T"),
        &msg(
            b't',
            &cat(&[
                &2i16.to_be_bytes(),
                &23u32.to_be_bytes(),
                &25u32.to_be_bytes(),
            ]),
        ),
        &msg(b'K', &cat(&[&7u32.to_be_bytes(), &9u32.to_be_bytes()])),
        &msg(b'R', &cat(&[&5i32.to_be_bytes(), &[1, 2, 3, 4]])),
        &msg(b'R', &3i32.to_be_bytes()),
        &msg(
            b'H',
            &cat(&[
                &[0u8],
                &2i16.to_be_bytes(),
                &0i16.to_be_bytes(),
                &1i16.to_be_bytes(),
            ]),
        ),
        &msg(
            b'D',
            &cat(&[
                &3i16.to_be_bytes(),
                &2i32.to_be_bytes(),
                b"ab",
                &(-1i32).to_be_bytes(),
                &0i32.to_be_bytes(),
            ]),
        ),
    ]);
    assert_eq!(w.as_bytes(), expected.as_slice());
    let n = w.len();
    let mut out = Vec::new();
    w.flush_to(&mut out).unwrap();
    assert_eq!(out.len(), n);
    assert!(w.is_empty());
    // Embedded NULs cannot break framing.
    w.command_complete("A\0B");
    assert_eq!(w.as_bytes(), msg(b'C', b"AB\0").as_slice());
}

// ---------------------------------------------------------------------------
// Fake servers
// ---------------------------------------------------------------------------

type Conn = (BufReader<TcpStream>, TcpStream);

fn serve_one(f: impl FnOnce(Conn) + Send + 'static) -> (u16, JoinHandle<()>) {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let h = thread::spawn(move || {
        let (s, _) = l.accept().unwrap();
        let r = BufReader::new(s.try_clone().unwrap());
        f((r, s));
    });
    (port, h)
}

/// Answers SSL/GSS requests with 'N' and returns the startup parameters.
fn accept_startup(c: &mut Conn) -> Vec<(String, String)> {
    loop {
        match read_startup_packet(&mut c.0).unwrap() {
            StartupPacket::SslRequest | StartupPacket::GssEncRequest => {
                c.1.write_all(b"N").unwrap()
            }
            StartupPacket::Startup { version, params } => {
                assert_eq!(version, 196608);
                return params;
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}

fn send_ready(c: &mut Conn, w: &mut BackendWriter) {
    w.auth_ok();
    w.parameter_status("server_version", "17.0");
    w.parameter_status("client_encoding", "UTF8");
    w.parameter_status("standard_conforming_strings", "on");
    w.parameter_status("DateStyle", "ISO, MDY");
    w.parameter_status("integer_datetimes", "on");
    w.backend_key_data(4242, 0xfeed);
    w.ready_for_query(b'I');
    w.flush_to(&mut c.1).unwrap();
}

fn field(name: &str, type_oid: u32, type_size: i16) -> FieldDescription {
    FieldDescription {
        name: name.into(),
        table_oid: 0,
        column_attr: 0,
        type_oid,
        type_size,
        type_modifier: -1,
        format: 0,
    }
}

#[test]
fn backend_writer_decoded_by_client() {
    let (port, h) = serve_one(|mut c| {
        let params = accept_startup(&mut c);
        let get = |k: &str| {
            params
                .iter()
                .find(|(pk, _)| pk == k)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(get("user").as_deref(), Some("alice"));
        assert_eq!(get("database").as_deref(), Some("db"));
        assert_eq!(get("client_encoding").as_deref(), Some("UTF8"));
        assert_eq!(get("application_name").as_deref(), Some("app"));
        assert_eq!(get("TimeZone").as_deref(), Some("UTC"));
        assert_eq!(get("replication"), None);

        let mut w = BackendWriter::new();
        // Cleartext, then MD5 (a real server asks once; the client must
        // handle whatever arrives).
        w.auth_cleartext_password();
        w.flush_to(&mut c.1).unwrap();
        assert_eq!(
            read_frontend_message(&mut c.0).unwrap(),
            Some(FM::Password(b"pw\0".to_vec()))
        );
        w.auth_md5_password(*b"abcd");
        w.flush_to(&mut c.1).unwrap();
        let Some(FM::Password(p)) = read_frontend_message(&mut c.0).unwrap() else {
            panic!()
        };
        // md5(md5("pw" + "alice") + "abcd"), see auth unit tests for the algorithm.
        assert_eq!(p.len(), 36);
        assert!(p.starts_with(b"md5") && p.ends_with(b"\0"));
        send_ready(&mut c, &mut w);

        loop {
            let Some(m) = read_frontend_message(&mut c.0).unwrap() else {
                return;
            };
            let FM::Query(q) = m else {
                assert_eq!(m, FM::Terminate);
                continue;
            };
            match q.as_str() {
                "select" => {
                    w.row_description(&[field("a", 23, 4), field("b", 25, -1)]);
                    w.data_row(&[Some(b"1"), None]);
                    w.data_row(&[Some(b"2"), Some("zażółć".as_bytes())]);
                    w.command_complete("SELECT 2");
                    w.ready_for_query(b'I');
                }
                "multi" => {
                    w.notice_response("NOTICE", "00000", "hi there");
                    w.parameter_status("TimeZone", "Europe/Warsaw");
                    w.command_complete("SET");
                    w.empty_query_response();
                    w.row_description(&[field("x", 25, -1)]);
                    w.command_complete("SELECT 0");
                    w.command_complete("INSERT 0 17");
                    w.ready_for_query(b'T');
                }
                "bad" => {
                    w.row_description(&[field("x", 25, -1)]);
                    w.data_row(&[Some(b"partial")]);
                    w.error_response(
                        "ERROR",
                        "42P01",
                        "no such table",
                        Some("the detail"),
                        Some("the hint"),
                        Some(7),
                    );
                    w.ready_for_query(b'E');
                }
                "copy" => {
                    w.copy_out_response(0, &[0, 0]);
                    w.copy_data(b"1\tx\n");
                    w.copy_data(b"2\ty\n");
                    w.copy_done();
                    w.command_complete("COPY 2");
                    w.ready_for_query(b'I');
                }
                "fatal" => {
                    w.error_response("FATAL", "57P01", "terminating connection", None, None, None);
                    w.flush_to(&mut c.1).unwrap();
                    return;
                }
                other => panic!("unexpected query {other}"),
            }
            w.flush_to(&mut c.1).unwrap();
        }
    });

    let mut cfg = Config::parse(&format!(
        "host=127.0.0.1 port={port} user=alice dbname=db password=pw application_name=app connect_timeout=10"
    ))
    .unwrap();
    cfg.options.push(("TimeZone".into(), "UTC".into()));
    let mut c = Client::connect(&cfg).unwrap();
    assert_eq!(c.backend_pid(), 4242);
    assert_eq!(c.backend_secret(), 0xfeed);
    assert_eq!(c.parameter("server_version"), Some("17.0"));

    let r = c.query("select").unwrap();
    assert_eq!(r.columns.len(), 2);
    assert_eq!(
        (
            r.columns[0].name.as_str(),
            r.columns[0].type_oid,
            r.columns[0].type_size
        ),
        ("a", 23, 4)
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Some("1".into()), None],
            vec![Some("2".into()), Some("zażółć".into())]
        ]
    );
    assert_eq!(r.tag, "SELECT 2");

    let rs = c.simple_query("multi").unwrap();
    assert_eq!(
        rs.iter().map(|r| r.tag.as_str()).collect::<Vec<_>>(),
        ["SET", "SELECT 0", "INSERT 0 17"]
    );
    assert_eq!(rs[1].columns.len(), 1);
    assert_eq!(c.transaction_status(), b'T');
    assert_eq!(c.parameter("TimeZone"), Some("Europe/Warsaw"));
    let n = c.take_notices();
    assert_eq!(n.len(), 1);
    assert_eq!(
        (
            n[0].severity.as_str(),
            n[0].sqlstate.as_str(),
            n[0].message.as_str()
        ),
        ("NOTICE", "00000", "hi there")
    );
    assert_eq!(c.execute("multi").unwrap(), 17);

    let e = c.simple_query("bad").unwrap_err();
    assert_eq!(e.kind, ErrorKind::Server);
    assert_eq!(e.code(), Some("42P01"));
    assert_eq!(e.message, "no such table");
    assert_eq!(e.detail.as_deref(), Some("the detail"));
    assert_eq!(e.hint.as_deref(), Some("the hint"));
    assert_eq!(c.transaction_status(), b'E');
    assert!(c.is_usable());

    let r = c.query("copy").unwrap();
    assert_eq!(r.tag, "COPY 2");
    assert_eq!(
        r.rows,
        vec![vec![Some("1\tx".into())], vec![Some("2\ty".into())]]
    );

    let e = c.simple_query("fatal").unwrap_err();
    assert_eq!((e.kind, e.code()), (ErrorKind::Server, Some("57P01")));
    assert!(!c.is_usable());
    assert_eq!(
        c.simple_query("select").unwrap_err().kind,
        ErrorKind::Closed
    );
    drop(c);
    h.join().unwrap();
}

/// A fake walsender that delivers messages in fragments, to verify that
/// `ReplicationStream::recv` timeouts never lose partially read data.
#[test]
fn fake_walsender_resumable_recv() {
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let (port, h) = serve_one(move |mut c| {
        let params = accept_startup(&mut c);
        assert!(params.contains(&("replication".into(), "database".into())));
        assert!(params.contains(&("DateStyle".into(), "ISO".into())));
        let mut w = BackendWriter::new();
        send_ready(&mut c, &mut w);
        let Some(FM::Query(q)) = read_frontend_message(&mut c.0).unwrap() else {
            panic!()
        };
        assert_eq!(
            q,
            "START_REPLICATION SLOT \"my\"\"slot\" LOGICAL 1/A0 (\"proto_version\" '1', \"publication_names\" 'it''s')"
        );
        w.copy_both_response(0, &[]);
        w.flush_to(&mut c.1).unwrap();

        // XLogData(Begin), then a keepalive, as one byte string.
        let mut begin = vec![b'B'];
        begin.extend_from_slice(&0x1_0000_0100u64.to_be_bytes());
        begin.extend_from_slice(&777i64.to_be_bytes());
        begin.extend_from_slice(&42u32.to_be_bytes());
        let mut x = vec![b'w'];
        x.extend_from_slice(&0x1_0000_00F0u64.to_be_bytes());
        x.extend_from_slice(&0x1_0000_0200u64.to_be_bytes());
        x.extend_from_slice(&123456i64.to_be_bytes());
        x.extend_from_slice(&begin);
        w.copy_data(&x);
        let mut k = vec![b'k'];
        k.extend_from_slice(&0x1_0000_0300u64.to_be_bytes());
        k.extend_from_slice(&654321i64.to_be_bytes());
        k.push(1);
        w.copy_data(&k);
        let bytes = w.as_bytes().to_vec();
        w.clear();

        // Fragment boundaries: inside the 5-byte header, inside the body,
        // and inside the second message.
        let cuts = [3, 20, bytes.len() - 10, bytes.len()];
        let mut from = 0;
        for &to in &cuts {
            c.1.write_all(&bytes[from..to]).unwrap();
            from = to;
            if to != bytes.len() {
                go_rx.recv().unwrap();
            }
        }

        // Standby status update from the client.
        let Some(FM::CopyData(d)) = read_frontend_message(&mut c.0).unwrap() else {
            panic!()
        };
        assert_eq!(d.len(), 34);
        assert_eq!(d[0], b'r');
        assert_eq!(&d[1..9], &0x10u64.to_be_bytes());
        assert_eq!(&d[9..17], &0x20u64.to_be_bytes());
        assert_eq!(&d[17..25], &0x30u64.to_be_bytes());
        let ts = i64::from_be_bytes(d[25..33].try_into().unwrap());
        assert!(
            ts > 700_000_000_000_000,
            "client clock in PG epoch microseconds: {ts}"
        );
        assert_eq!(d[33], 1);

        // Client stops: CopyDone handshake, back to command mode.
        assert_eq!(read_frontend_message(&mut c.0).unwrap(), Some(FM::CopyDone));
        w.copy_data(&x); // in-flight data is discarded by stop()
        w.copy_done();
        w.command_complete("COPY 0");
        w.ready_for_query(b'I');
        w.flush_to(&mut c.1).unwrap();
        let Some(FM::Query(q)) = read_frontend_message(&mut c.0).unwrap() else {
            panic!()
        };
        assert_eq!(q, "select 1");
        w.row_description(&[field("?column?", 23, 4)]);
        w.data_row(&[Some(b"1")]);
        w.command_complete("SELECT 1");
        w.ready_for_query(b'I');
        w.flush_to(&mut c.1).unwrap();
        assert_eq!(
            read_frontend_message(&mut c.0).unwrap(),
            Some(FM::Terminate)
        );
        assert_eq!(read_frontend_message(&mut c.0).unwrap(), None);
    });

    let cfg = Config::parse(&format!("host=127.0.0.1 port={port} user=u dbname=d")).unwrap();
    let c = Client::connect_replication(&cfg).unwrap();
    let mut s = c
        .start_logical_replication(
            "my\"slot",
            Lsn(0x1_0000_00A0),
            &[("proto_version", "1"), ("publication_names", "it's")],
        )
        .unwrap();

    // Fragment 1 (3 bytes): nothing complete yet.
    let t = Instant::now();
    assert_eq!(s.recv(Some(Duration::from_millis(100))).unwrap(), None);
    assert!(t.elapsed() >= Duration::from_millis(95));
    go_tx.send(()).unwrap();
    // Fragment 2 lands; still incomplete.
    thread::sleep(Duration::from_millis(50));
    assert_eq!(s.recv(Some(Duration::ZERO)).unwrap(), None);
    assert_eq!(s.recv(Some(Duration::from_millis(50))).unwrap(), None);
    go_tx.send(()).unwrap();
    // Fragment 3 completes the first message.
    let ev = s.recv(Some(Duration::from_secs(5))).unwrap().unwrap();
    assert_eq!(
        ev,
        ReplicationEvent::XLogData {
            wal_start: Lsn(0x1_0000_00F0),
            wal_end: Lsn(0x1_0000_0200),
            server_time: 123456,
            message: LogicalMessage::Begin {
                final_lsn: Lsn(0x1_0000_0100),
                commit_time: 777,
                xid: 42
            },
        }
    );
    // The keepalive is still incomplete.
    assert_eq!(s.recv(Some(Duration::from_millis(30))).unwrap(), None);
    go_tx.send(()).unwrap();
    let ev = s.recv(None).unwrap().unwrap();
    assert_eq!(
        ev,
        ReplicationEvent::Keepalive {
            wal_end: Lsn(0x1_0000_0300),
            server_time: 654321,
            reply_requested: true
        }
    );
    assert_eq!(s.last_wal_end(), Lsn(0x1_0000_0300));
    s.send_standby_status(Lsn(0x10), Lsn(0x20), Lsn(0x30), true)
        .unwrap();

    let mut c = s.stop().unwrap();
    assert_eq!(c.query("select 1").unwrap().get(0, 0), Some("1"));
    c.close();
    h.join().unwrap();
}

#[test]
fn psql_against_tiny_server() {
    let psql = std::env::var_os("PG_BIN")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| "/usr/lib/postgresql/17/bin".into())
        .join("psql");
    let (port, h) = serve_one(|mut c| {
        let params = accept_startup(&mut c);
        assert!(params.iter().any(|(k, v)| k == "user" && v == "someone"));
        assert!(params.iter().any(|(k, v)| k == "database" && v == "somedb"));
        let mut w = BackendWriter::new();
        send_ready(&mut c, &mut w);
        loop {
            match read_frontend_message(&mut c.0).unwrap() {
                None | Some(FM::Terminate) => return,
                Some(FM::Query(q)) => {
                    assert_eq!(q, "select 1");
                    w.row_description(&[field("?column?", 23, 4)]);
                    w.data_row(&[Some(b"1")]);
                    w.command_complete("SELECT 1");
                    w.ready_for_query(b'I');
                    w.flush_to(&mut c.1).unwrap();
                }
                Some(other) => panic!("unexpected {other:?}"),
            }
        }
    });
    let mut cmd = Command::new(psql);
    for (k, _) in std::env::vars_os() {
        if k.to_string_lossy().starts_with("PG") {
            cmd.env_remove(k);
        }
    }
    let out = cmd
        .args([
            "-X",
            "-At",
            "-h",
            "127.0.0.1",
            "-p",
            &port.to_string(),
            "-U",
            "someone",
            "-d",
            "somedb",
            "-c",
            "select 1",
        ])
        .env("PGCONNECT_TIMEOUT", "10")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "psql failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&out.stdout), "1\n");
    h.join().unwrap();
}

#[test]
fn huge_timeouts_do_not_overflow() {
    let (port, h) = serve_one(|mut c| {
        accept_startup(&mut c);
        let mut w = BackendWriter::new();
        send_ready(&mut c, &mut w);
        let Some(FM::Query(_)) = read_frontend_message(&mut c.0).unwrap() else {
            panic!()
        };
        w.copy_both_response(0, &[]);
        let mut k = vec![b'k'];
        k.extend_from_slice(&5u64.to_be_bytes());
        k.extend_from_slice(&6i64.to_be_bytes());
        k.push(0);
        w.copy_data(&k);
        w.flush_to(&mut c.1).unwrap();
        while let Ok(Some(_)) = read_frontend_message(&mut c.0) {}
    });
    let mut cfg = Config::parse(&format!("host=127.0.0.1 port={port} user=u dbname=d")).unwrap();
    cfg.connect_timeout = Some(Duration::MAX);
    let c = Client::connect_replication(&cfg).unwrap();
    let mut s = c.start_logical_replication("s", Lsn(0), &[]).unwrap();
    let ev = s.recv(Some(Duration::MAX)).unwrap().unwrap();
    assert!(matches!(
        ev,
        ReplicationEvent::Keepalive {
            wal_end: Lsn(5),
            ..
        }
    ));
    drop(s);
    h.join().unwrap();
}
