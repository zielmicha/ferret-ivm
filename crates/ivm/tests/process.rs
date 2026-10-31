//! The server binary: exit statuses and shutdown.

mod common;
use common::temp_dir as tmpdir;

use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

/// A port nothing listens on (as far as we know).
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// The server, with its working directory (for the stop file) and data
/// directory in `dir`.
fn ivm(dir: &Path, port: u16) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_ivm"));
    c.current_dir(dir)
        .args(["--data-dir", "data", "--listen", &format!("127.0.0.1:{port}"), "--tick-ms", "0", "--checkpoint-secs", "3600", "--no-sync"])
        .env("RUST_LOG", "warn")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    c
}

fn wait_exit(child: &mut Child, secs: u64) -> ExitStatus {
    let t0 = Instant::now();
    loop {
        if let Some(s) = child.try_wait().unwrap() {
            return s;
        }
        if t0.elapsed() > Duration::from_secs(secs) {
            let _ = child.kill();
            panic!("the server did not exit within {secs}s");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn stderr_of(child: &mut Child) -> String {
    let mut s = String::new();
    child.stderr.take().unwrap().read_to_string(&mut s).unwrap();
    s
}

fn connect(port: u16) -> pgproto::Client {
    connect_to(&format!("host=127.0.0.1 port={port} user=ivm dbname=ivm"))
}

fn connect_to(conninfo: &str) -> pgproto::Client {
    let cfg = pgproto::Config::parse(conninfo).unwrap();
    let t0 = Instant::now();
    loop {
        match pgproto::Client::connect(&cfg) {
            Ok(c) => return c,
            Err(e) if t0.elapsed() > Duration::from_secs(30) => panic!("cannot connect: {e}"),
            Err(_) => std::thread::sleep(Duration::from_millis(50)),
        }
    }
}

/// Every logged record is covered by the last checkpoint (the final one).
fn checkpoint_covers_the_log(data: &Path) -> bool {
    let (_env, rec) = ivm::storage::Env::open(&data.join("store"), Default::default()).unwrap();
    let gsn = rec.expect("a checkpoint").gsn;
    ivm::log::read_from(&data.join("log"), gsn).unwrap().next().is_none()
}

/// A listen address that cannot be bound fails at once, with status 1 (it
/// used to exit with status 0, after recovery).
#[test]
fn start_up_failures_exit_with_status_1() {
    let dir = tmpdir("bind");
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut child = ivm(&dir, taken.local_addr().unwrap().port()).spawn().unwrap();
    let status = wait_exit(&mut child, 30);
    let err = stderr_of(&mut child);
    assert_eq!(status.code(), Some(1), "{err}");
    assert!(err.contains("cannot listen on"), "{err}");
    // a data directory in use by another process
    let port = free_port();
    let mut first = ivm(&dir, port).spawn().unwrap();
    connect(port);
    let mut second = ivm(&dir, free_port()).spawn().unwrap();
    let status = wait_exit(&mut second, 30);
    let err = stderr_of(&mut second);
    assert_eq!(status.code(), Some(1), "{err}");
    assert!(err.contains("in use by another ivm process"), "{err}");
    let _ = first.kill();
    let _ = first.wait();
    let _ = std::fs::remove_dir_all(&dir);
}

/// SIGTERM, SIGINT and the stop file shut the server down after a final
/// checkpoint, with status 0. (There were no signal handlers, and the stop
/// file exited without waiting for the checkpoint.)
#[test]
fn shutdown_requests_take_a_final_checkpoint() {
    let dir = tmpdir("shutdown");
    for (i, how) in ["TERM", "INT", "stop file"].into_iter().enumerate() {
        let port = free_port();
        let mut child = ivm(&dir, port).spawn().unwrap();
        let mut c = connect(port);
        if i == 0 {
            c.simple_query("CREATE TABLE t (id int PRIMARY KEY)").unwrap();
        }
        c.simple_query(&format!("INSERT INTO t VALUES ({i})")).unwrap();
        if how == "stop file" {
            std::fs::write(dir.join(".ivm-stop"), b"").unwrap();
        } else {
            let ok = Command::new("kill").args([&format!("-{how}"), &child.id().to_string()]).status().unwrap().success();
            assert!(ok, "kill -{how}");
        }
        let status = wait_exit(&mut child, 30);
        let err = stderr_of(&mut child);
        assert_eq!(status.code(), Some(0), "{how}: {err}");
        assert!(checkpoint_covers_the_log(&dir.join("data")), "{how}: no final checkpoint");
    }
    // everything is there
    let port = free_port();
    let mut child = ivm(&dir, port).spawn().unwrap();
    let mut c = connect(port);
    assert_eq!(c.query("SELECT count(*) FROM t").unwrap().rows, [[Some("3".to_string())]]);
    std::fs::write(dir.join(".ivm-stop"), b"").unwrap();
    assert_eq!(wait_exit(&mut child, 30).code(), Some(0));
    let _ = std::fs::remove_dir_all(&dir);
}

/// Unix-domain sockets for the PostgreSQL protocol and the web UI, next to
/// TCP: a stale socket file is replaced, a live one is not taken over, and
/// the files are removed when the server exits (also when it cannot start).
#[test]
fn unix_domain_sockets() {
    let dir = std::path::absolute(tmpdir("unix")).unwrap();
    let (pg, ui) = (dir.join(".s.PGSQL.6875"), dir.join("ui.sock"));
    // left behind by a server that was killed
    drop(UnixListener::bind(&pg).unwrap());
    let port = free_port();
    // (paths relative to the server's working directory)
    let mut child = ivm(&dir, port).args(["--listen", "./.s.PGSQL.6875", "--http-listen", "./ui.sock"]).spawn().unwrap();
    let mut c = connect_to(&format!("host={} port=6875 user=ivm", dir.display()));
    c.simple_query("CREATE TABLE t (id int PRIMARY KEY)").unwrap();
    connect(port).simple_query("INSERT INTO t VALUES (1)").unwrap();
    assert_eq!(c.query("SELECT count(*) FROM t").unwrap().rows, [[Some("1".to_string())]]);
    let mut s = UnixStream::connect(&ui).unwrap();
    s.write_all(b"GET /api/overview HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").unwrap();
    let mut resp = String::new();
    s.read_to_string(&mut resp).unwrap();
    assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
    // another server cannot take the socket over
    let other = std::path::absolute(tmpdir("unix-other")).unwrap();
    let mut second = ivm(&other, free_port()).args(["--listen", pg.to_str().unwrap()]).spawn().unwrap();
    let status = wait_exit(&mut second, 30);
    let err = stderr_of(&mut second);
    assert_eq!(status.code(), Some(1), "{err}");
    assert!(err.contains(&format!("cannot listen on {}: ", pg.display())), "{err}");
    assert_eq!(c.query("SELECT count(*) FROM t").unwrap().rows, [[Some("1".to_string())]]);
    // a server that cannot start removes the sockets it created
    let mut third = ivm(&other, free_port()).args(["--listen", "./pg.sock", "--http-listen", "./missing/ui.sock"]).spawn().unwrap();
    let status = wait_exit(&mut third, 30);
    let err = stderr_of(&mut third);
    assert_eq!(status.code(), Some(1), "{err}");
    assert!(err.contains("cannot start the web UI: ./missing/ui.sock: "), "{err}");
    assert!(!other.join("pg.sock").exists());
    // shutdown
    std::fs::write(dir.join(".ivm-stop"), b"").unwrap();
    let status = wait_exit(&mut child, 30);
    let err = stderr_of(&mut child);
    assert_eq!(status.code(), Some(0), "{err}");
    assert!(!pg.exists() && !ui.exists(), "the socket files are removed");
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&other);
}
