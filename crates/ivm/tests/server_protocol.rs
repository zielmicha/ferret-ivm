//! Wire-protocol tests against an in-process server, with raw messages.

use ivm::coord;
use ivm::server::{self, ServerConfig};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::time::Duration;

fn tmpdir(name: &str) -> PathBuf {
    let base = std::env::var("IVM_TEST_TMP").unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().into_owned());
    let d = PathBuf::from(base).join(format!("ivm-proto-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

/// Start an engine and a server on an ephemeral port.
fn start(name: &str) -> (coord::Running, u16) {
    let cfg = coord::Config { data_dir: tmpdir(name), sync: false, tick: None, ..Default::default() };
    let running = coord::start(cfg).expect("engine starts");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let h = running.handle.clone();
    std::thread::spawn(move || {
        let _ = server::serve_listener(listener, ServerConfig { listen: String::new(), password: None }, h);
    });
    (running, port)
}

struct Conn {
    s: TcpStream,
}

/// A backend message: (tag, body).
type Msg = (u8, Vec<u8>);

impl Conn {
    fn connect(port: u16) -> Conn {
        let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let mut c = Conn { s };
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
