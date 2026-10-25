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
