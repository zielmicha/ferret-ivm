//! Wire-protocol tests against an in-process server, with raw messages.

use ivm::coord;
use ivm::listen::Stream;
use ivm::server::{self, ServerConfig};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::net::UnixStream;
use std::time::Duration;

mod common;
use common::{Engine as Running, TempDir};

/// Start an engine and a server on an ephemeral port.
fn start(name: &str) -> (Running, u16) {
    start_with(name, |_| {})
}

fn start_with(name: &str, configure: impl FnOnce(&mut coord::Config)) -> (Running, u16) {
    let engine = Running::start_with(name, configure);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let h = engine.handle.clone();
    std::thread::spawn(move || {
        let _ = server::serve_listener(listener, ServerConfig::default(), h);
    });
    (engine, port)
}

struct Conn {
    s: Stream,
}

/// A backend message: (tag, body).
type Msg = (u8, Vec<u8>);

impl Conn {
    fn connect(port: u16) -> Conn {
        let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let mut c = Conn { s: Stream::Tcp(s) };
        let mut body = Vec::new();
        body.extend_from_slice(&196608u32.to_be_bytes());
        for (k, v) in [("user", "ivm"), ("database", "ivm")] {
            body.extend_from_slice(k.as_bytes());
            body.push(0);
            body.extend_from_slice(v.as_bytes());
            body.push(0);
        }
        body.push(0);
        let mut pkt = ((body.len() + 4) as u32).to_be_bytes().to_vec();
        pkt.extend_from_slice(&body);
        c.s.write_all(&pkt).unwrap();
        c.until_ready();
        c
    }

    fn send(&mut self, tag: u8, body: &[u8]) {
        let mut m = vec![tag];
        m.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        m.extend_from_slice(body);
        self.s.write_all(&m).unwrap();
    }

    fn recv(&mut self) -> Msg {
        let mut hdr = [0u8; 5];
        self.s.read_exact(&mut hdr).expect("backend message (timed out?)");
        let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
        let mut body = vec![0u8; len - 4];
        self.s.read_exact(&mut body).unwrap();
        (hdr[0], body)
    }

    /// Messages up to and including ReadyForQuery.
    fn until_ready(&mut self) -> Vec<Msg> {
        let mut v = Vec::new();
        loop {
            let m = self.recv();
            let done = m.0 == b'Z';
            v.push(m);
            if done {
                return v;
            }
        }
    }

    fn query(&mut self, sql: &str) -> Vec<Msg> {
        let mut b = sql.as_bytes().to_vec();
        b.push(0);
        self.send(b'Q', &b);
        self.until_ready()
    }
}

fn tags(msgs: &[Msg]) -> String {
    msgs.iter().map(|m| m.0 as char).collect()
}

/// The text of every DataRow's first column.
fn first_values(msgs: &[Msg]) -> Vec<String> {
    msgs.iter()
        .filter(|m| m.0 == b'D')
        .map(|(_, b)| {
            let len = i32::from_be_bytes([b[2], b[3], b[4], b[5]]);
            if len < 0 { "NULL".into() } else { String::from_utf8(b[6..6 + len as usize].to_vec()).unwrap() }
        })
        .collect()
}

#[test]
fn function_call_message_gets_error_and_ready() {
    let (running, port) = start("fcall");
    let mut c = Conn::connect(port);
    // FunctionCall: function oid, no argument formats, no arguments, text result
    let mut body = 1598u32.to_be_bytes().to_vec();
    body.extend_from_slice(&0i16.to_be_bytes());
    body.extend_from_slice(&0i16.to_be_bytes());
    body.extend_from_slice(&0i16.to_be_bytes());
    c.send(b'F', &body);
    // used to leave the error unflushed and never send ReadyForQuery
    let msgs = c.until_ready();
    assert_eq!(tags(&msgs), "EZ");
    // the session is still usable
    assert_eq!(first_values(&c.query("SELECT 41 + 1")), vec!["42"]);
    running.shutdown();
}

fn cstr(s: &str) -> Vec<u8> {
    let mut v = s.as_bytes().to_vec();
    v.push(0);
    v
}

impl Conn {
    fn parse(&mut self, name: &str, sql: &str, types: &[u32]) {
        let mut b = cstr(name);
        b.extend(cstr(sql));
        b.extend_from_slice(&(types.len() as i16).to_be_bytes());
        for t in types {
            b.extend_from_slice(&t.to_be_bytes());
        }
        self.send(b'P', &b);
    }

    /// Bind with all parameters in `format` (0 text, 1 binary), text results.
    fn bind(&mut self, portal: &str, stmt: &str, params: &[Option<&[u8]>], format: i16) {
        let mut b = cstr(portal);
        b.extend(cstr(stmt));
        b.extend_from_slice(&1i16.to_be_bytes());
        b.extend_from_slice(&format.to_be_bytes());
        b.extend_from_slice(&(params.len() as i16).to_be_bytes());
        for p in params {
            match p {
                None => b.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(v) => {
                    b.extend_from_slice(&(v.len() as i32).to_be_bytes());
                    b.extend_from_slice(v);
                }
            }
        }
        b.extend_from_slice(&0i16.to_be_bytes());
        self.send(b'B', &b);
    }

    fn describe_stmt(&mut self, name: &str) {
        let mut b = vec![b'S'];
        b.extend(cstr(name));
        self.send(b'D', &b);
    }

    fn execute(&mut self, portal: &str, max_rows: i32) {
        let mut b = cstr(portal);
        b.extend_from_slice(&max_rows.to_be_bytes());
        self.send(b'E', &b);
    }

    fn sync(&mut self) -> Vec<Msg> {
        self.send(b'S', &[]);
        self.until_ready()
    }
}

/// Type OIDs of the ParameterDescription in `msgs`.
fn param_oids(msgs: &[Msg]) -> Vec<u32> {
    let (_, b) = msgs.iter().find(|m| m.0 == b't').expect("ParameterDescription");
    let n = i16::from_be_bytes([b[0], b[1]]) as usize;
    (0..n).map(|i| u32::from_be_bytes(b[2 + 4 * i..6 + 4 * i].try_into().unwrap())).collect()
}

/// The message text of the first ErrorResponse.
fn error_message(msgs: &[Msg]) -> Option<String> {
    let (_, b) = msgs.iter().find(|m| m.0 == b'E')?;
    b.split(|c| *c == 0).find(|f| f.first() == Some(&b'M')).map(|f| String::from_utf8_lossy(&f[1..]).into_owned())
}

#[test]
fn describe_infers_parameter_types() {
    let (running, port) = start("paramtypes");
    let mut c = Conn::connect(port);
    c.query("CREATE TABLE t (id int PRIMARY KEY, a int, s text)");
    c.query("INSERT INTO t VALUES (1, 1, 'x'), (2, 2, 'y'), (3, 2, 'z')");
    // types follow from the context of the first use, as in PostgreSQL
    for (sql, want) in [
        ("SELECT id FROM t WHERE a = $1", vec![23]),
        ("SELECT $1::date + 1, $2 || 'x', $3 + 1, $4", vec![1082, 25, 23, 25]),
        ("SELECT id FROM t ORDER BY id LIMIT $1", vec![20]),
        ("SELECT id FROM t WHERE s LIKE $1 AND a IN ($2, 3)", vec![25, 23]),
        ("UPDATE t SET s = '$9' WHERE id = $1", vec![25]),
    ] {
        c.parse("s", sql, &[]);
        c.describe_stmt("s");
        let msgs = c.sync();
        assert_eq!(param_oids(&msgs), want, "{sql}: {:?}", tags(&msgs));
    }
    // binding after Describe used to bind the values as text: integer = text
    c.parse("q", "SELECT id FROM t WHERE a = $1 ORDER BY id", &[]);
    c.describe_stmt("q");
    c.bind("", "q", &[Some(b"2")], 0);
    c.execute("", 0);
    let msgs = c.sync();
    assert_eq!(first_values(&msgs), vec!["2", "3"], "{:?}", error_message(&msgs));
    c.describe_stmt("q");
    c.bind("", "q", &[Some(&2i32.to_be_bytes())], 1);
    c.execute("", 0);
    let msgs = c.sync();
    assert_eq!(first_values(&msgs), vec!["2", "3"], "{:?}", error_message(&msgs));
    // DML parameters are not inferred, but untyped values still bind
    c.parse("u", "UPDATE t SET a = $1 WHERE id = $2", &[]);
    c.describe_stmt("u");
    c.bind("", "u", &[Some(b"5"), Some(b"1")], 0);
    c.execute("", 0);
    let msgs = c.sync();
    assert_eq!(error_message(&msgs), None);
    assert_eq!(first_values(&c.query("SELECT a FROM t WHERE id = 1")), vec!["5"]);
    running.shutdown();
}

/// CommandComplete tags in `msgs`.
fn command_tags(msgs: &[Msg]) -> Vec<String> {
    msgs.iter().filter(|m| m.0 == b'C').map(|(_, b)| String::from_utf8_lossy(&b[..b.len() - 1]).into_owned()).collect()
}

#[test]
fn portals_follow_postgresql_execute_semantics() {
    let (running, port) = start("portals");
    let mut c = Conn::connect(port);
    c.query("CREATE TABLE t (id int PRIMARY KEY)");
    c.query("INSERT INTO t VALUES (1), (2), (3), (4), (5)");
    // batches of two: suspended after each full batch; the count is per Execute
    c.parse("", "SELECT id FROM t ORDER BY id", &[]);
    c.bind("", "", &[], 0);
    for _ in 0..3 {
        c.execute("", 2);
    }
    let msgs = c.sync();
    assert_eq!(tags(&msgs), "12DDsDDsDCZ");
    assert_eq!(command_tags(&msgs), vec!["SELECT 1"]);
    // exactly max_rows rows left: still suspended, then an empty completion
    c.parse("", "SELECT id FROM t WHERE id <= 2 ORDER BY id", &[]);
    c.bind("", "", &[], 0);
    c.execute("", 2);
    c.execute("", 2);
    let msgs = c.sync();
    assert_eq!(tags(&msgs), "12DDsCZ");
    assert_eq!(command_tags(&msgs), vec!["SELECT 0"]);
    // an exhausted portal can be executed again (used to fail: no such portal)
    c.parse("", "SELECT 1", &[]);
    c.bind("p", "", &[], 0);
    c.execute("p", 0);
    c.execute("p", 0);
    let msgs = c.sync();
    assert_eq!(command_tags(&msgs), vec!["SELECT 1", "SELECT 0"], "{:?}", error_message(&msgs));
    // portals end with the (implicit) transaction
    c.execute("p", 0);
    let msgs = c.sync();
    assert!(error_message(&msgs).unwrap_or_default().contains("does not exist"));
    running.shutdown();
}

#[test]
fn bind_checks_the_number_of_parameters() {
    let (running, port) = start("bindcount");
    let mut c = Conn::connect(port);
    c.parse("s", "SELECT $1::int + $2::int", &[]);
    c.bind("", "s", &[Some(b"1")], 0);
    c.execute("", 0);
    let msgs = c.sync();
    // used to run with $2 silently NULL
    assert_eq!(
        error_message(&msgs).as_deref(),
        Some("bind message supplies 1 parameters, but prepared statement \"s\" requires 2")
    );
    c.bind("", "s", &[Some(b"1"), Some(b"2"), Some(b"3")], 0);
    c.execute("", 0);
    assert!(error_message(&c.sync()).is_some());
    c.bind("", "s", &[Some(b"1"), Some(b"2")], 0);
    c.execute("", 0);
    assert_eq!(first_values(&c.sync()), vec!["3"]);
    running.shutdown();
}

#[test]
fn untyped_literal_results_are_described_as_text() {
    let (running, port) = start("unknowncols");
    let mut c = Conn::connect(port);
    let msgs = c.query("SELECT 'abc', NULL");
    let (_, b) = msgs.iter().find(|m| m.0 == b'T').unwrap();
    // two fields; the type OID follows the name, table oid and attribute number
    let mut pos = 2;
    let mut oids = Vec::new();
    for _ in 0..2 {
        pos += b[pos..].iter().position(|c| *c == 0).unwrap() + 1;
        oids.push(u32::from_be_bytes(b[pos + 6..pos + 10].try_into().unwrap()));
        pos += 18;
    }
    assert_eq!(oids, vec![25, 25]);
    running.shutdown();
}

#[test]
fn session_commands_clients_send_at_connect() {
    let (running, port) = start("sessioncmds");
    let mut c = Conn::connect(port);
    // used to fail: unsupported statement / unrecognized parameter / no function
    for sql in ["RESET ALL", "SHOW server_version_num", "SHOW integer_datetimes", "SELECT set_config('search_path', 'public', false)"] {
        let msgs = c.query(sql);
        assert_eq!(error_message(&msgs), None, "{sql}");
    }
    assert_eq!(first_values(&c.query("SHOW server_version_num")), vec!["170000"]);
    running.shutdown();
}

#[test]
fn deep_prepared_statements_do_not_crash_sessions() {
    let (running, port) = start("deepprep");
    let mut c = Conn::connect(port);
    c.query("CREATE TABLE t (id int PRIMARY KEY, a int)");
    c.query("INSERT INTO t VALUES (1, 1), (2, 5)");
    // prepared statements used to be cloned for every Bind/Describe/Execute,
    // which overflowed the session thread's stack for long chains
    let ors = (0..20_000).map(|i| format!("a = {}", i * 5 + 1)).collect::<Vec<_>>().join(" OR ");
    c.parse("s", &format!("SELECT id FROM t WHERE {ors} ORDER BY id"), &[]);
    c.describe_stmt("s");
    c.bind("", "s", &[], 0);
    c.execute("", 0);
    let msgs = c.sync();
    assert_eq!(first_values(&msgs), vec!["1"], "{:?}", error_message(&msgs));
    let plus = format!("SELECT {}", vec!["1"; 40_000].join("+"));
    c.parse("p", &plus, &[]);
    c.describe_stmt("p");
    let msgs = c.sync();
    assert!(error_message(&msgs).unwrap_or_default().contains("stack depth limit exceeded"));
    assert_eq!(first_values(&c.query("SELECT 2")), vec!["2"]);
    running.shutdown();
}

#[test]
fn comment_only_statements_are_empty() {
    let (running, port) = start("comments");
    let mut c = Conn::connect(port);
    // used to end with ERROR: empty statement
    assert_eq!(tags(&c.query("SELECT 1; -- done")), "TDCZ");
    assert_eq!(tags(&c.query("-- nothing")), "IZ");
    assert_eq!(tags(&c.query("/* nothing */ ;")), "IZ");
    running.shutdown();
}

/// The BackendKeyData (pid, secret) of a fresh connection, and the connection.
fn connect_with_key(port: u16) -> (Conn, u32, u32) {
    let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    start_with_key(Stream::Tcp(s))
}

/// Start a session on `s`: its BackendKeyData (pid, secret), and the connection.
fn start_with_key(s: Stream) -> (Conn, u32, u32) {
    let mut c = Conn { s };
    let mut body = 196608u32.to_be_bytes().to_vec();
    body.extend_from_slice(b"user\0ivm\0\0");
    let mut pkt = ((body.len() + 4) as u32).to_be_bytes().to_vec();
    pkt.extend_from_slice(&body);
    c.s.write_all(&pkt).unwrap();
    let msgs = c.until_ready();
    let (_, k) = msgs.iter().find(|m| m.0 == b'K').expect("BackendKeyData");
    let (pid, secret) = (u32::from_be_bytes(k[0..4].try_into().unwrap()), u32::from_be_bytes(k[4..8].try_into().unwrap()));
    (c, pid, secret)
}

fn cancel(port: u16, pid: u32, secret: u32) {
    let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let mut m = 16u32.to_be_bytes().to_vec();
    m.extend_from_slice(&80877102u32.to_be_bytes());
    m.extend_from_slice(&pid.to_be_bytes());
    m.extend_from_slice(&secret.to_be_bytes());
    s.write_all(&m).unwrap();
}

#[test]
fn subscribe_through_the_extended_protocol_is_described() {
    let (running, port) = start("subscribe-ext");
    let (mut c, pid, secret) = connect_with_key(port);
    c.query("CREATE TABLE t (id int PRIMARY KEY, s text)");
    c.query("INSERT INTO t VALUES (1, 'x')");
    // Describe used to answer NoData, and Execute then streamed DataRows
    c.parse("", "SUBSCRIBE TO t WITH (PROGRESS)", &[]);
    c.bind("", "", &[], 0);
    let mut b = vec![b'P'];
    b.extend(cstr(""));
    c.send(b'D', &b);
    c.execute("", 0);
    c.send(b'H', &[]);
    let mut seen = Vec::new();
    loop {
        let m = c.recv();
        seen.push(m.0);
        if m.0 == b'D' {
            break;
        }
    }
    assert_eq!(seen.iter().map(|t| *t as char).collect::<String>(), "12TD");
    cancel(port, pid, secret);
    let msgs = c.sync();
    assert!(error_message(&msgs).unwrap_or_default().contains("canceling statement"));
    // COPY (SUBSCRIBE ...) through Execute streams COPY data
    c.parse("", "COPY (SUBSCRIBE TO t) TO STDOUT", &[]);
    c.bind("", "", &[], 0);
    c.execute("", 0);
    c.send(b'H', &[]);
    let first = loop {
        let m = c.recv();
        if m.0 != b'1' && m.0 != b'2' {
            break m.0;
        }
    };
    assert_eq!(first as char, 'H', "CopyOutResponse");
    cancel(port, pid, secret);
    c.sync();
    running.shutdown();
}

/// The fields of a DataRow (None: NULL).
fn data_fields(body: &[u8]) -> Vec<Option<Vec<u8>>> {
    let n = i16::from_be_bytes([body[0], body[1]]) as usize;
    let mut pos = 2;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let len = i32::from_be_bytes(body[pos..pos + 4].try_into().unwrap());
        pos += 4;
        if len < 0 {
            out.push(None);
        } else {
            out.push(Some(body[pos..pos + len as usize].to_vec()));
            pos += len as usize;
        }
    }
    out
}

/// The format codes of a RowDescription's fields.
fn field_formats(body: &[u8]) -> Vec<i16> {
    let n = i16::from_be_bytes([body[0], body[1]]) as usize;
    let mut pos = 2;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        pos += body[pos..].iter().position(|c| *c == 0).unwrap() + 1;
        // table oid, attribute number, type oid, size, modifier, format
        out.push(i16::from_be_bytes([body[pos + 16], body[pos + 17]]));
        pos += 18;
    }
    out
}

/// SUBSCRIBE through the extended protocol sends its rows in the result
/// formats of the Bind, which the portal's row description announces. The
/// rows used to be text whatever the description said, so a client asking
/// for binary results misread them (psycopg: "unpack requires a buffer of 8
/// bytes").
#[test]
fn subscribe_rows_follow_the_result_formats() {
    let (running, port) = start("subscribe-binary");
    let (mut c, pid, secret) = connect_with_key(port);
    c.query("CREATE TABLE t (id int PRIMARY KEY, s text)");
    c.query("INSERT INTO t VALUES (1, 'x')");
    // all binary, then one format per column
    for formats in [vec![1i16], vec![0, 1, 1, 0, 1]] {
        c.parse("", "SUBSCRIBE TO t WITH (PROGRESS)", &[]);
        let mut b = cstr("");
        b.extend(cstr(""));
        b.extend_from_slice(&0i16.to_be_bytes()); // parameter formats
        b.extend_from_slice(&0i16.to_be_bytes()); // parameters
        b.extend_from_slice(&(formats.len() as i16).to_be_bytes());
        for f in &formats {
            b.extend_from_slice(&f.to_be_bytes());
        }
        c.send(b'B', &b);
        let mut d = vec![b'P'];
        d.extend(cstr(""));
        c.send(b'D', &d);
        c.execute("", 0);
        c.send(b'H', &[]);
        let mut rows = Vec::new();
        let mut description = None;
        while rows.len() < 2 {
            let (tag, body) = c.recv();
            match tag {
                b'T' => description = Some(field_formats(&body)),
                b'D' => rows.push(data_fields(&body)),
                b'1' | b'2' => {}
                other => panic!("unexpected message {}", other as char),
            }
        }
        let binary: Vec<bool> = (0..5).map(|i| if formats.len() == 1 { formats[0] == 1 } else { formats[i] == 1 }).collect();
        assert_eq!(description, Some(binary.iter().map(|b| *b as i16).collect()), "{formats:?}");
        // the snapshot row, then the progress row
        let gsn = |v: &Option<Vec<u8>>, bin: bool| -> i64 {
            let v = v.as_ref().unwrap();
            if bin { i64::from_be_bytes(v[..].try_into().expect("8 bytes")) } else { String::from_utf8(v.clone()).unwrap().parse().unwrap() }
        };
        let (data, prog) = (&rows[0], &rows[1]);
        assert_eq!(gsn(&data[0], binary[0]), gsn(&prog[0], binary[0]), "{formats:?}");
        let want_data: Vec<Option<Vec<u8>>> = vec![
            data[0].clone(),
            Some(if binary[1] { vec![0] } else { b"f".to_vec() }),
            Some(if binary[2] { 1i64.to_be_bytes().to_vec() } else { b"1".to_vec() }),
            Some(if binary[3] { 1i32.to_be_bytes().to_vec() } else { b"1".to_vec() }),
            Some(b"x".to_vec()),
        ];
        assert_eq!(data, &want_data, "{formats:?}");
        assert_eq!(prog[1], Some(if binary[1] { vec![1] } else { b"t".to_vec() }), "{formats:?}");
        assert_eq!(&prog[2..], &[None, None, None], "{formats:?}");
        cancel(port, pid, secret);
        let msgs = c.sync();
        assert!(error_message(&msgs).unwrap_or_default().contains("canceling statement"), "{msgs:?}");
    }
    running.shutdown();
}

/// RESET (also RESET ALL, and SET ... TO DEFAULT) undoes a SET, and
/// current_setting() reports the session's settings, so a virtual view
/// sees those of the session reading it. RESET used to change nothing, and
/// current_setting() reported the server's fixed settings only.
#[test]
fn reset_and_current_setting_follow_the_session() {
    let (running, port) = start("reset");
    let mut c = Conn::connect(port);
    let value = |c: &mut Conn, sql: &str| -> String {
        let msgs = c.query(sql);
        assert_eq!(error_message(&msgs), None, "{sql}");
        first_values(&msgs).remove(0)
    };
    for reset in ["RESET read_mode", "RESET ALL", "SET read_mode TO DEFAULT", "SET ivm.read_mode = DEFAULT"] {
        c.query("SET read_mode = latest");
        assert_eq!(value(&mut c, "SELECT current_setting('read_mode')"), "latest");
        c.query(reset);
        assert_eq!(value(&mut c, "SHOW read_mode"), "fresh", "{reset}");
        assert_eq!(value(&mut c, "SELECT current_setting('ivm.read_mode')"), "fresh", "{reset}");
    }
    // application_name: back to the startup value (none), reported to the client
    c.query("SET application_name = 'other'");
    assert_eq!(value(&mut c, "SELECT current_setting('application_name')"), "other");
    let msgs = c.query("RESET application_name");
    let status: Vec<&Vec<u8>> = msgs.iter().filter(|m| m.0 == b'S').map(|m| &m.1).collect();
    assert_eq!(status, [&b"application_name\0\0".to_vec()]);
    assert_eq!(value(&mut c, "SELECT current_setting('application_name')"), "");
    // custom settings, seen by a virtual view as the reading session has them
    c.query("SET myapp.user_id = '42'");
    c.query("SET search_path = public");
    c.query("CREATE VIEW who AS SELECT current_setting('myapp.user_id', true) AS u");
    assert_eq!(value(&mut c, "SELECT u FROM who"), "42");
    let mut other = Conn::connect(port);
    assert_eq!(value(&mut other, "SELECT u FROM who"), "NULL");
    c.query("RESET ALL");
    assert_eq!(value(&mut c, "SELECT u FROM who"), "NULL");
    assert_eq!(value(&mut c, "SHOW search_path"), "\"$user\", public");
    assert_eq!(value(&mut c, "SELECT current_setting('search_path')"), "\"$user\", public");
    // the server's settings, as before
    assert_eq!(value(&mut c, "SELECT current_setting('TimeZone')"), "UTC");
    assert!(error_message(&c.query("SELECT current_setting('nope')")).unwrap_or_default().contains("unrecognized configuration parameter"));
    running.shutdown();
}

#[test]
fn show_read_mode_before_set() {
    let (running, port) = start("readmode");
    let mut c = Conn::connect(port);
    // used to fail: unrecognized configuration parameter
    assert_eq!(first_values(&c.query("SHOW read_mode")), vec!["fresh"]);
    c.query("SET ivm.read_mode = latest");
    assert_eq!(first_values(&c.query("SHOW read_mode")), vec!["latest"]);
    running.shutdown();
}

#[test]
fn set_rejects_settings_the_server_cannot_honor() {
    let (running, port) = start("setvalues");
    let mut c = Conn::connect(port);
    // these were accepted and then silently ignored: '01/02/2024'::date
    // stayed January 2nd under DMY, bytea kept the hex format, ...
    for sql in [
        "SET DateStyle = 'ISO, DMY'",
        "SET datestyle TO ymd",
        "SET DateStyle = 'European'",
        "SET IntervalStyle = 'iso_8601'",
        "SET bytea_output = 'escape'",
        "SET standard_conforming_strings = off",
    ] {
        let msgs = c.query(sql);
        assert!(error_message(&msgs).is_some_and(|m| m.contains("not supported")), "{sql}: {msgs:?}");
    }
    // the values in effect, and other settings, are still accepted
    for sql in [
        "SET DateStyle = 'ISO, MDY'",
        "SET DateStyle = ISO",
        "SET datestyle TO mdy",
        "SET IntervalStyle = postgres",
        "SET bytea_output = hex",
        "SET standard_conforming_strings = on",
        "SET extra_float_digits = 3",
        "SET search_path = public",
    ] {
        assert_eq!(error_message(&c.query(sql)), None, "{sql}");
    }
    assert_eq!(first_values(&c.query("SELECT '01/02/2024'::date")), vec!["2024-01-02"]);
    running.shutdown();
}

#[test]
fn show_reports_fixed_settings() {
    let (running, port) = start("showfixed");
    let mut c = Conn::connect(port);
    // used to fail with "unrecognized configuration parameter"; tools run
    // SHOW search_path when they connect
    for (name, value) in [
        ("search_path", "\"$user\", public"),
        ("extra_float_digits", "1"),
        ("bytea_output", "hex"),
        ("is_superuser", "on"),
        ("default_transaction_isolation", "serializable"),
        ("transaction_read_only", "off"),
    ] {
        assert_eq!(first_values(&c.query(&format!("SHOW {name}"))), vec![value], "{name}");
    }
    c.query("SET search_path = public");
    assert_eq!(first_values(&c.query("SHOW search_path")), vec!["public"]);
    running.shutdown();
}

/// The SQLSTATE of the ErrorResponse in `msgs`.
fn error_code(msgs: &[Msg]) -> Option<String> {
    let (_, b) = msgs.iter().find(|m| m.0 == b'E')?;
    b.split(|c| *c == 0).find(|f| f.first() == Some(&b'C')).map(|f| String::from_utf8_lossy(&f[1..]).into_owned())
}

fn stats(running: &Running) -> ivm::coord::inspect::EngineStats {
    match running.handle.inspect_wait(ivm::coord::inspect::InspectQuery::Stats, Duration::from_secs(10)).unwrap() {
        ivm::coord::inspect::InspectReply::Stats(s) => s,
        other => panic!("unexpected reply {other:?}"),
    }
}

/// Reads waiting in the engine.
fn parked_reads(running: &Running) -> usize {
    stats(running).parked_reads
}

/// Wait until `n` reads wait in the engine.
fn until_parked(running: &Running, n: usize) {
    let t0 = std::time::Instant::now();
    while parked_reads(running) != n {
        assert!(t0.elapsed() < Duration::from_secs(10), "{n} reads do not wait in the engine");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// A read waiting in the engine (here for an LSN that no source reaches:
/// the only one connects nowhere) ends with 57014 on a CancelRequest, and
/// is dropped when its client disconnects; it used to wait forever (only
/// SUBSCRIBE was cancellable).
#[test]
fn waiting_reads_are_cancelled_and_dropped() {
    let (running, port) = start("cancel-parked");
    // (created through the engine, with the upstream tables it discovered
    // given: none)
    let sql = "CREATE SOURCE s FROM POSTGRES CONNECTION 'host=127.0.0.1 port=1 connect_timeout=1' PUBLICATION 'p'";
    let stmt = ivm::sql::parse_statement(sql).unwrap();
    running.handle.execute(stmt, vec![], ivm::coord::api::SessionVars::default(), "[]".into()).unwrap();
    let read = "SELECT 1 AS OF AT LEAST LSN '0/16B3748'";
    let (mut c, pid, secret) = connect_with_key(port);
    let mut b = read.as_bytes().to_vec();
    b.push(0);
    // simple query
    c.send(b'Q', &b);
    until_parked(&running, 1);
    cancel(port, pid, secret);
    let msgs = c.until_ready();
    assert_eq!((error_code(&msgs).as_deref(), error_message(&msgs).as_deref()), (Some("57014"), Some("canceling statement due to user request")));
    assert_eq!(parked_reads(&running), 0);
    // the session goes on; a cancel without a running statement does nothing
    cancel(port, pid, secret);
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(first_values(&c.query("SELECT 7")), ["7"]);
    // extended protocol
    c.parse("", read, &[]);
    c.bind("", "", &[], 0);
    c.execute("", 0);
    c.send(b'S', &[]);
    until_parked(&running, 1);
    cancel(port, pid, secret);
    let msgs = c.until_ready();
    assert_eq!(error_code(&msgs).as_deref(), Some("57014"));
    // a client that goes away
    let (mut c2, _, _) = connect_with_key(port);
    c2.send(b'Q', &b);
    until_parked(&running, 1);
    drop(c2);
    until_parked(&running, 0);
    running.shutdown();
}

/// Wait until `cond` holds for the engine's stats.
fn until_stats(running: &Running, what: &str, cond: impl Fn(&ivm::coord::inspect::EngineStats) -> bool) {
    let t0 = std::time::Instant::now();
    loop {
        let st = stats(running);
        if cond(&st) {
            return;
        }
        assert!(t0.elapsed() < Duration::from_secs(20), "{what}: {st:?}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Insert `n` rows of about 1 KB into `t` (id, s).
fn insert_kb_rows(c: &mut Conn, from: i64, n: i64) {
    let mut i = from;
    while i < from + n {
        let msgs = c.query(&format!("INSERT INTO t SELECT g, repeat('x', 1000) FROM generate_series({i}, {}) g", i + 999));
        assert_eq!(error_message(&msgs), None);
        i += 1000;
    }
}

/// A client that subscribes and then stops reading, keeping its connection:
/// the server's writes to it block, but the engine stops queueing changes
/// at the subscription's limit and ends it (53000), the session drops what
/// was queued while it waits for the client, and when the client reads
/// again it gets what was written, the error and ReadyForQuery. The session
/// used to block in its write for good while the engine queued every change
/// (+530 MB, never released).
#[test]
fn a_subscriber_that_stops_reading_is_ended_and_its_queue_released() {
    let (running, port) = start_with("stalled-subscriber", |c| c.memory_bytes = 16 << 20);
    let mut w = Conn::connect(port);
    w.query("CREATE TABLE t (id int PRIMARY KEY, s text)");
    let mut c = Conn::connect(port);
    c.send(b'Q', b"SUBSCRIBE TO t\0");
    until_stats(&running, "the subscription starts", |s| s.subscriptions == 1);
    // far more than the socket buffers and the 4 MiB limit take
    insert_kb_rows(&mut w, 0, 40_000);
    until_stats(&running, "the subscription is ended and its queue released", |s| s.subscriptions == 0 && s.output_queue_bytes == 0);
    let mut rows = 0;
    let error = loop {
        let m = c.recv();
        match m.0 {
            b'D' => rows += 1,
            b'E' => break error_code(&[m]),
            _ => {}
        }
    };
    assert_eq!(error.as_deref(), Some("53000"));
    assert!((1..40_000).contains(&rows), "{rows} rows before the error");
    assert_eq!(c.recv().0, b'Z');
    assert_eq!(first_values(&c.query("SELECT 7")), ["7"]);
    running.shutdown();
}

/// A cancel request reaches a subscription whose client does not read: the
/// session ends it while its write waits (dropping the queued changes) and
/// reports 57014 when the client reads again.
#[test]
fn a_subscription_blocked_on_its_client_can_be_cancelled() {
    let (running, port) = start_with("stalled-cancel", |c| c.memory_bytes = 256 << 20);
    let mut w = Conn::connect(port);
    w.query("CREATE TABLE t (id int PRIMARY KEY, s text)");
    let (mut c, pid, secret) = connect_with_key(port);
    c.send(b'Q', b"SUBSCRIBE TO t\0");
    until_stats(&running, "the subscription starts", |s| s.subscriptions == 1);
    // more than the socket buffers take, less than the 16 MiB limit
    insert_kb_rows(&mut w, 0, 15_000);
    until_stats(&running, "changes are queued", |s| s.output_queue_bytes > 0);
    cancel(port, pid, secret);
    until_stats(&running, "the subscription is ended and its queue released", |s| s.subscriptions == 0 && s.output_queue_bytes == 0);
    let msgs = c.until_ready();
    assert_eq!(error_code(&msgs).as_deref(), Some("57014"));
    assert_eq!(first_values(&c.query("SELECT 7")), ["7"]);
    running.shutdown();
}

/// A large result goes out as the engine finds it, in order; an error
/// after its first rows ends them with an ErrorResponse (as in PostgreSQL),
/// while a result of one chunk fails before any row, as before; and a
/// portal over a streamed result is fetched in batches across Executes.
#[test]
fn large_results_are_streamed() {
    let (running, port) = start("streamed");
    let mut c = Conn::connect(port);
    c.query("CREATE TABLE t (id int PRIMARY KEY, s text)");
    c.query("INSERT INTO t SELECT g, repeat('x', 100) FROM generate_series(1, 30000) g");
    let msgs = c.query("SELECT id, s FROM t ORDER BY id");
    let vals = first_values(&msgs);
    assert_eq!((vals.len(), vals[0].as_str(), vals[29999].as_str()), (30000, "1", "30000"));
    assert_eq!(command_tags(&msgs), vec!["SELECT 30000"]);
    // division by zero at the 20,000th row (in key order)
    let msgs = c.query("SELECT 1 / (id - 20000) FROM t");
    let t = tags(&msgs);
    assert!(t.starts_with("TD") && t.ends_with("DEZ"), "{t}");
    assert_eq!(error_code(&msgs).as_deref(), Some("22012"));
    let sent = first_values(&msgs).len();
    assert!((1024..20000).contains(&sent), "{sent} rows before the error");
    let msgs = c.query("SELECT 1 / (id - 2) FROM t ORDER BY id LIMIT 5");
    assert_eq!(tags(&msgs), "EZ");
    // batches across Executes
    c.parse("", "SELECT id FROM t ORDER BY id", &[]);
    c.bind("", "", &[], 0);
    c.execute("", 5000);
    c.execute("", 5000);
    c.execute("", 0);
    let msgs = c.sync();
    let vals = first_values(&msgs);
    assert_eq!((vals.len(), vals[10000].as_str()), (30000, "10001"));
    assert_eq!(msgs.iter().filter(|m| m.0 == b's').count(), 2);
    assert_eq!(command_tags(&msgs), vec!["SELECT 20000"]);
    assert_eq!(first_values(&c.query("SELECT count(*) FROM t")), vec!["30000"]);
    running.shutdown();
}

/// Sessions on a Unix-domain socket: a client library finds it from a
/// directory and a port; reads are cancelled (also through another
/// listener of the same server) and dropped when the client goes away.
#[test]
fn unix_domain_sockets() {
    let sockets = TempDir::new("proto-unix-sockets");
    let path = sockets.path().join(".s.PGSQL.6875");
    let running = Running::start("proto-unix");
    let (unix, file) = ivm::listen::bind(path.to_str().unwrap()).unwrap();
    let tcp = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = tcp.local_addr().unwrap().port();
    let srv = server::Server::new(ServerConfig::default(), running.handle.clone());
    for l in [unix, tcp.into()] {
        let srv = srv.clone();
        std::thread::spawn(move || srv.serve(l));
    }
    let cfg = pgproto::Config::parse(&format!("host={} port=6875 user=ivm", sockets.path().display())).unwrap();
    let mut client = pgproto::Client::connect(&cfg).unwrap();
    client.simple_query("CREATE TABLE t (id int PRIMARY KEY)").unwrap();
    client.simple_query("INSERT INTO t VALUES (1), (2)").unwrap();
    assert_eq!(client.query("SELECT count(*) FROM t").unwrap().rows, [[Some("2".to_string())]]);
    // a read that waits for an LSN of a source that never connects (see
    // waiting_reads_are_cancelled_and_dropped)
    let sql = "CREATE SOURCE s FROM POSTGRES CONNECTION 'host=127.0.0.1 port=1 connect_timeout=1' PUBLICATION 'p'";
    let stmt = ivm::sql::parse_statement(sql).unwrap();
    running.handle.execute(stmt, vec![], ivm::coord::api::SessionVars::default(), "[]".into()).unwrap();
    let read = b"SELECT 1 AS OF AT LEAST LSN '0/16B3748'\0";
    let s = UnixStream::connect(&path).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let (mut c, pid, secret) = start_with_key(Stream::Unix(s));
    c.send(b'Q', read);
    until_parked(&running, 1);
    cancel(port, pid, secret);
    assert_eq!(error_code(&c.until_ready()).as_deref(), Some("57014"));
    c.send(b'Q', read);
    until_parked(&running, 1);
    drop(c);
    until_parked(&running, 0);
    drop(client);
    running.shutdown();
    drop(file);
    assert!(!path.exists(), "the socket file is removed");
}
