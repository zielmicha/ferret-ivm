//! Frontend (client) side of the PostgreSQL protocol.
//!
//! Blocking I/O over TCP or Unix-domain sockets. Supports trust, cleartext,
//! MD5 and SCRAM-SHA-256 authentication (no TLS, no channel binding).

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream, ToSocketAddrs};
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

use crate::auth::{self, ScramClient};
use crate::pgoutput;
use crate::wire::{self, Reader};
use crate::{Error, ErrorKind, Lsn, Result};

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct Config {
    /// Hostname, IP address, or absolute unix-socket directory (a leading
    /// `@` selects a Linux abstract-namespace socket, like libpq).
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub dbname: String,
    pub application_name: Option<String>,
    pub connect_timeout: Option<Duration>,
    /// Extra startup parameters, e.g. `("TimeZone", "UTC")`. The libpq
    /// `options` and `client_encoding` keywords end up here too.
    pub options: Vec<(String, String)>,
}

impl Default for Config {
    fn default() -> Config {
        let user = default_user();
        Config {
            host: "localhost".into(),
            port: 5432,
            dbname: user.clone(),
            user,
            password: None,
            application_name: None,
            connect_timeout: None,
            options: Vec::new(),
        }
    }
}

fn default_user() -> String {
    std::env::var("USER")
        .ok()
        .or_else(|| std::env::var("LOGNAME").ok())
        .filter(|u| !u.is_empty())
        .unwrap_or_else(|| "postgres".into())
}

impl Config {
    /// Parses a libpq connection string: either `key=value` pairs (values
    /// may be single-quoted, with `\'` and `\\` escapes) or a
    /// `postgres://` / `postgresql://` URI. Defaults: host=localhost,
    /// port=5432, user=$USER, dbname=user. PG* environment variables are not
    /// consulted.
    pub fn parse(conninfo: &str) -> Result<Config> {
        let s = conninfo.trim();
        let pairs = if s.starts_with("postgresql://") || s.starts_with("postgres://") {
            parse_uri(s)?
        } else {
            parse_kv(s)?
        };
        let mut host = None;
        let mut hostaddr = None;
        let mut port = None;
        let mut user = None;
        let mut dbname = None;
        let mut password = None;
        let mut application_name = None;
        let mut fallback_application_name = None;
        let mut connect_timeout = None;
        let mut options: Vec<(String, String)> = Vec::new();
        let mut set_option = |k: &str, v: String| {
            options.retain(|(ok, _)| ok != k);
            options.push((k.to_string(), v));
        };
        for (k, v) in pairs {
            match k.as_str() {
                "host" => host = Some(v),
                "hostaddr" => hostaddr = Some(v),
                "port" => port = Some(v),
                "user" => user = Some(v),
                "password" => password = Some(v),
                "dbname" => dbname = Some(v),
                "application_name" => application_name = Some(v),
                "fallback_application_name" => fallback_application_name = Some(v),
                "connect_timeout" => {
                    let secs: i64 = v.trim().parse().map_err(|_| {
                        Error::config(format!("invalid integer value \"{v}\" for connection option \"connect_timeout\""))
                    })?;
                    connect_timeout = (secs > 0).then(|| Duration::from_secs(secs as u64));
                }
                "options" | "client_encoding" | "replication" => set_option(&k, v),
                "sslmode" => match v.as_str() {
                    "disable" | "allow" | "prefer" => {}
                    "require" | "verify-ca" | "verify-full" => {
                        return Err(Error::config(format!(
                            "sslmode={v} is not supported (no TLS support)"
                        )));
                    }
                    _ => return Err(Error::config(format!("invalid sslmode value: \"{v}\""))),
                },
                "gssencmode" => {
                    if v == "require" {
                        return Err(Error::config("gssencmode=require is not supported"));
                    }
                }
                "channel_binding" => {
                    if v == "require" {
                        return Err(Error::config(
                            "channel_binding=require is not supported (no TLS support)",
                        ));
                    }
                }
                "ssl" => {
                    if v == "true" {
                        return Err(Error::config("ssl=true is not supported (no TLS support)"));
                    }
                }
                // Accepted and ignored.
                "target_session_attrs"
                | "load_balance_hosts"
                | "keepalives"
                | "keepalives_idle"
                | "keepalives_interval"
                | "keepalives_count"
                | "tcp_user_timeout"
                | "sslcert"
                | "sslkey"
                | "sslrootcert"
                | "sslcrl"
                | "sslcrldir"
                | "sslpassword"
                | "sslcompression"
                | "sslsni"
                | "ssl_min_protocol_version"
                | "ssl_max_protocol_version"
                | "sslnegotiation"
                | "sslcertmode"
                | "sslkeylogfile"
                | "requirepeer"
                | "krbsrvname"
                | "gsslib"
                | "gssdelegation"
                | "passfile"
                | "service"
                | "require_auth"
                | "min_protocol_version"
                | "max_protocol_version"
                | "scram_client_key"
                | "scram_server_key" => {}
                _ => return Err(Error::config(format!("invalid connection option \"{k}\""))),
            }
        }
        let host = hostaddr
            .filter(|h| !h.is_empty())
            .or(host.filter(|h| !h.is_empty()))
            .unwrap_or_else(|| "localhost".into());
        if host.contains(',') {
            return Err(Error::config("multiple hosts are not supported"));
        }
        let port = match port.filter(|p| !p.is_empty()) {
            None => 5432,
            Some(p) => p
                .trim()
                .parse::<u16>()
                .ok()
                .filter(|&p| p != 0)
                .ok_or_else(|| Error::config(format!("invalid port number: \"{p}\"")))?,
        };
        let user = user.filter(|u| !u.is_empty()).unwrap_or_else(default_user);
        let dbname = dbname
            .filter(|d| !d.is_empty())
            .unwrap_or_else(|| user.clone());
        Ok(Config {
            host,
            port,
            user,
            password,
            dbname,
            application_name: application_name.or(fallback_application_name),
            connect_timeout,
            options,
        })
    }
}

fn parse_kv(s: &str) -> Result<Vec<(String, String)>> {
    let b: Vec<char> = s.chars().collect();
    let ws = |c: char| c.is_ascii_whitespace();
    let mut i = 0;
    let mut out = Vec::new();
    loop {
        while i < b.len() && ws(b[i]) {
            i += 1;
        }
        if i >= b.len() {
            break;
        }
        let ks = i;
        while i < b.len() && b[i] != '=' && !ws(b[i]) {
            i += 1;
        }
        let key: String = b[ks..i].iter().collect();
        while i < b.len() && ws(b[i]) {
            i += 1;
        }
        if i >= b.len() || b[i] != '=' {
            return Err(Error::config(format!(
                "missing \"=\" after \"{key}\" in connection info string"
            )));
        }
        i += 1;
        while i < b.len() && ws(b[i]) {
            i += 1;
        }
        let mut val = String::new();
        if i < b.len() && b[i] == '\'' {
            i += 1;
            loop {
                if i >= b.len() {
                    return Err(Error::config(
                        "unterminated quoted string in connection info string",
                    ));
                }
                match b[i] {
                    '\\' => {
                        i += 1;
                        if i < b.len() {
                            val.push(b[i]);
                            i += 1;
                        }
                    }
                    '\'' => {
                        i += 1;
                        break;
                    }
                    c => {
                        val.push(c);
                        i += 1;
                    }
                }
            }
        } else {
            while i < b.len() && !ws(b[i]) {
                if b[i] == '\\' {
                    i += 1;
                    if i < b.len() {
                        val.push(b[i]);
                        i += 1;
                    }
                } else {
                    val.push(b[i]);
                    i += 1;
                }
            }
        }
        out.push((key, val));
    }
    Ok(out)
}

fn percent_decode(s: &str) -> Result<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            let hex = |c: u8| (c as char).to_digit(16);
            match (
                b.get(i + 1).copied().and_then(hex),
                b.get(i + 2).copied().and_then(hex),
            ) {
                (Some(h), Some(l)) => out.push((h * 16 + l) as u8),
                _ => {
                    return Err(Error::config(format!(
                        "invalid percent-encoded token: \"{s}\""
                    )));
                }
            }
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out)
        .map_err(|_| Error::config(format!("invalid UTF-8 in percent-encoded token: \"{s}\"")))
}

fn parse_uri(s: &str) -> Result<Vec<(String, String)>> {
    let mut rest = s
        .strip_prefix("postgresql://")
        .or_else(|| s.strip_prefix("postgres://"))
        .expect("caller checked the scheme");
    let mut out = Vec::new();

    // user[:password]@
    if let Some(i) = rest
        .find(['@', '/'])
        .filter(|&i| rest.as_bytes()[i] == b'@')
    {
        let userinfo = &rest[..i];
        rest = &rest[i + 1..];
        let (user, password) = match userinfo.split_once(':') {
            Some((u, p)) => (u, Some(p)),
            None => (userinfo, None),
        };
        if !user.is_empty() {
            out.push(("user".to_string(), percent_decode(user)?));
        }
        if let Some(p) = password.filter(|p| !p.is_empty()) {
            out.push(("password".to_string(), percent_decode(p)?));
        }
    }

    // host[:port]
    let end = rest.find(['/', '?']).unwrap_or(rest.len());
    let hostport = &rest[..end];
    rest = &rest[end..];
    if hostport.contains(',') {
        return Err(Error::config("multiple hosts are not supported"));
    }
    let (host, port) = if let Some(h) = hostport.strip_prefix('[') {
        let close = h.find(']').ok_or_else(|| {
            Error::config(format!(
                "missing \"]\" in IPv6 host address in URI: \"{s}\""
            ))
        })?;
        let after = &h[close + 1..];
        let port = match after.strip_prefix(':') {
            Some(p) => Some(p),
            None if after.is_empty() => None,
            None => {
                return Err(Error::config(format!(
                    "unexpected character after IPv6 host address in URI: \"{s}\""
                )));
            }
        };
        (&h[..close], port)
    } else {
        match hostport.split_once(':') {
            Some((h, p)) => (h, Some(p)),
            None => (hostport, None),
        }
    };
    if !host.is_empty() {
        out.push(("host".to_string(), percent_decode(host)?));
    }
    if let Some(p) = port.filter(|p| !p.is_empty()) {
        out.push(("port".to_string(), percent_decode(p)?));
    }

    // /dbname
    if let Some(r) = rest.strip_prefix('/') {
        let end = r.find('?').unwrap_or(r.len());
        if end > 0 {
            out.push(("dbname".to_string(), percent_decode(&r[..end])?));
        }
        rest = &r[end..];
    }

    // ?params
    if let Some(q) = rest.strip_prefix('?') {
        for kv in q.split('&').filter(|kv| !kv.is_empty()) {
            let (k, v) = kv.split_once('=').ok_or_else(|| {
                Error::config(format!(
                    "missing key/value separator \"=\" in URI query parameter: \"{kv}\""
                ))
            })?;
            out.push((percent_decode(k)?, percent_decode(v)?));
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub table_oid: u32,
    pub column_attr: i16,
    pub type_oid: u32,
    pub type_size: i16,
    pub type_modifier: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct SimpleQueryResult {
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<Option<String>>>,
    /// Command tag, e.g. "SELECT 3", "INSERT 0 1".
    pub tag: String,
}

impl SimpleQueryResult {
    /// Rows affected / returned, parsed from the command tag (0 if none).
    pub fn rows_affected(&self) -> u64 {
        tag_rows(&self.tag)
    }

    /// Index of the column with the given name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Value at (row, column) as `&str`; `None` for SQL NULL or out of range.
    pub fn get(&self, row: usize, col: usize) -> Option<&str> {
        self.rows.get(row)?.get(col)?.as_deref()
    }
}

/// The callback of [`Client::simple_query_rows`]: called with the columns of
/// the statement and each of its data rows; an error ends the calls.
pub type OnRow<'f> = dyn FnMut(&[Column], RowRef<'_>) -> std::result::Result<(), String> + 'f;

/// A data row borrowed from the connection's buffer (see
/// [`Client::simple_query_rows`]).
pub struct RowRef<'a> {
    body: &'a [u8],
    /// (offset, length) of each field; length -1 for NULL.
    fields: &'a [(u32, i32)],
}

impl<'a> RowRef<'a> {
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Raw value of field `i` (text format); `None` for SQL NULL or out of
    /// range.
    pub fn get(&self, i: usize) -> Option<&'a [u8]> {
        let &(off, len) = self.fields.get(i)?;
        if len < 0 {
            return None;
        }
        Some(&self.body[off as usize..off as usize + len as usize])
    }
}

fn tag_rows(tag: &str) -> u64 {
    tag.rsplit(' ')
        .next()
        .and_then(|t| t.parse().ok())
        .unwrap_or(0)
}

/// A NoticeResponse received from the server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Notice {
    pub severity: String,
    pub sqlstate: String,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdentifySystem {
    pub systemid: String,
    pub timeline: u32,
    pub xlogpos: Lsn,
    pub dbname: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlotInfo {
    pub slot_name: String,
    pub consistent_point: Lsn,
    pub snapshot_name: Option<String>,
    pub output_plugin: Option<String>,
}

/// What CREATE_REPLICATION_SLOT does with the slot's initial snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SnapshotAction {
    /// `NOEXPORT_SNAPSHOT`: just create the slot.
    #[default]
    NoExport,
    /// `EXPORT_SNAPSHOT`: export a snapshot (`SlotInfo::snapshot_name`)
    /// usable with `SET TRANSACTION SNAPSHOT` from other sessions until the
    /// next command is executed on this replication connection.
    Export,
    /// `USE_SNAPSHOT`: use the snapshot for the current transaction on this
    /// connection (must be inside `BEGIN ISOLATION LEVEL REPEATABLE READ`).
    Use,
}

// ---------------------------------------------------------------------------
// Connection internals
// ---------------------------------------------------------------------------

enum Stream {
    Tcp(TcpStream),
    Unix(UnixStream),
    /// Placeholder left behind after the connection was moved elsewhere.
    Detached,
}

fn detached_err() -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, "connection was moved")
}

impl Stream {
    fn set_read_timeout(&self, t: Option<Duration>) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.set_read_timeout(t),
            Stream::Unix(s) => s.set_read_timeout(t),
            Stream::Detached => Err(detached_err()),
        }
    }
    fn set_write_timeout(&self, t: Option<Duration>) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.set_write_timeout(t),
            Stream::Unix(s) => s.set_write_timeout(t),
            Stream::Detached => Err(detached_err()),
        }
    }
    fn set_nonblocking(&self, nb: bool) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.set_nonblocking(nb),
            Stream::Unix(s) => s.set_nonblocking(nb),
            Stream::Detached => Err(detached_err()),
        }
    }
    fn shutdown(&self) {
        let _ = match self {
            Stream::Tcp(s) => s.shutdown(Shutdown::Both),
            Stream::Unix(s) => s.shutdown(Shutdown::Both),
            Stream::Detached => Ok(()),
        };
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Stream::Tcp(s) => s.read(buf),
            Stream::Unix(s) => s.read(buf),
            Stream::Detached => Err(detached_err()),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Stream::Tcp(s) => s.write(buf),
            Stream::Unix(s) => s.write(buf),
            Stream::Detached => Err(detached_err()),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn io_err(e: io::Error, context: String) -> Error {
    let mut err = Error::from(e);
    err.message = format!("{context}: {}", err.message);
    err
}

fn open_stream(cfg: &Config, deadline: Option<Instant>) -> Result<Stream> {
    let host = cfg.host.as_str();
    if host.starts_with('/') {
        let path = format!("{}/.s.PGSQL.{}", host.trim_end_matches('/'), cfg.port);
        let s = UnixStream::connect(&path).map_err(|e| {
            io_err(
                e,
                format!("connection to server on socket \"{path}\" failed"),
            )
        })?;
        return Ok(Stream::Unix(s));
    }
    #[cfg(target_os = "linux")]
    if let Some(name) = host.strip_prefix('@') {
        use std::os::linux::net::SocketAddrExt;
        let name = format!("{name}/.s.PGSQL.{}", cfg.port);
        let addr = std::os::unix::net::SocketAddr::from_abstract_name(name.as_bytes())?;
        let s = UnixStream::connect_addr(&addr).map_err(|e| {
            io_err(
                e,
                format!("connection to server on socket \"@{name}\" failed"),
            )
        })?;
        return Ok(Stream::Unix(s));
    }
    let addrs: Vec<SocketAddr> = (host, cfg.port)
        .to_socket_addrs()
        .map_err(|e| {
            io_err(
                e,
                format!("could not translate host name \"{host}\" to address"),
            )
        })?
        .collect();
    let mut last = None;
    for addr in addrs {
        let r = match deadline {
            Some(d) => {
                let rem = d.saturating_duration_since(Instant::now());
                if rem.is_zero() {
                    return Err(Error::new(
                        ErrorKind::Timeout,
                        "timeout expired while connecting",
                    ));
                }
                TcpStream::connect_timeout(&addr, rem)
            }
            None => TcpStream::connect(addr),
        };
        match r {
            Ok(s) => {
                let _ = s.set_nodelay(true);
                return Ok(Stream::Tcp(s));
            }
            Err(e) => last = Some((addr, e)),
        }
    }
    match last {
        Some((addr, e)) => Err(io_err(
            e,
            format!(
                "connection to server at \"{host}\" ({}), port {} failed",
                addr.ip(),
                cfg.port
            ),
        )),
        None => Err(Error::new(
            ErrorKind::Io,
            format!("could not translate host name \"{host}\" to address"),
        )),
    }
}

const INITIAL_BUF: usize = 64 * 1024;
const MAX_BACKEND_MESSAGE: usize = i32::MAX as usize;
const MAX_NOTICES: usize = 1000;

/// A protocol message located in the connection's read buffer:
/// (type byte, body start, body end).
type Msg = (u8, usize, usize);

struct Conn {
    stream: Stream,
    rbuf: Vec<u8>,
    rpos: usize,
    rend: usize,
    wbuf: Vec<u8>,
    read_timeout: Option<Duration>,
    params: HashMap<String, String>,
    pid: u32,
    secret: u32,
    txn_status: u8,
    notices: VecDeque<Notice>,
    /// Set once the connection can no longer be used, with the reason.
    broken: Option<String>,
    terminated: bool,
    /// START_REPLICATION already ran on this connection.
    streamed: bool,
}

impl Conn {
    fn new(stream: Stream) -> Conn {
        Conn {
            stream,
            rbuf: vec![0; INITIAL_BUF],
            rpos: 0,
            rend: 0,
            wbuf: Vec::with_capacity(1024),
            read_timeout: None,
            params: HashMap::new(),
            pid: 0,
            secret: 0,
            txn_status: b'I',
            notices: VecDeque::new(),
            broken: None,
            terminated: false,
            streamed: false,
        }
    }

    fn check(&self) -> Result<()> {
        match &self.broken {
            Some(reason) => Err(Error::closed(format!("connection is not usable: {reason}"))),
            None => Ok(()),
        }
    }

    fn fail(&mut self, e: Error) -> Error {
        if self.broken.is_none() {
            self.broken = Some(e.message.clone());
        }
        e
    }

    fn body(&self, m: &Msg) -> &[u8] {
        &self.rbuf[m.1..m.2]
    }

    fn set_read_timeout(&mut self, t: Option<Duration>) -> Result<()> {
        if self.read_timeout != t {
            self.stream.set_read_timeout(t)?;
            self.read_timeout = t;
        }
        Ok(())
    }

    /// Extracts the next complete message from the read buffer, if any.
    fn take_buffered(&mut self) -> Result<Option<Msg>> {
        let avail = self.rend - self.rpos;
        if avail < 5 {
            return Ok(None);
        }
        let p = self.rpos;
        let tag = self.rbuf[p];
        let len = u32::from_be_bytes([
            self.rbuf[p + 1],
            self.rbuf[p + 2],
            self.rbuf[p + 3],
            self.rbuf[p + 4],
        ]) as usize;
        if !(4..=MAX_BACKEND_MESSAGE).contains(&len) {
            return Err(Error::protocol(format!(
                "invalid message length {len} (message type {:?})",
                tag as char
            )));
        }
        if avail < 1 + len {
            return Ok(None);
        }
        self.rpos = p + 1 + len;
        Ok(Some((tag, p + 5, p + 1 + len)))
    }

    /// Reads more data from the socket. Returns `Ok(false)` if the deadline
    /// passed without any data arriving. Buffered data is never discarded.
    fn fill(&mut self, deadline: Option<Instant>) -> Result<bool> {
        if self.rpos == self.rend {
            self.rpos = 0;
            self.rend = 0;
            if self.rbuf.len() > 16 * INITIAL_BUF {
                self.rbuf.truncate(INITIAL_BUF);
                self.rbuf.shrink_to_fit();
            }
        }
        let avail = self.rend - self.rpos;
        let want_total = if avail >= 5 {
            let p = self.rpos;
            1 + u32::from_be_bytes([
                self.rbuf[p + 1],
                self.rbuf[p + 2],
                self.rbuf[p + 3],
                self.rbuf[p + 4],
            ]) as usize
        } else {
            5
        };
        let want_free = want_total.saturating_sub(avail).clamp(8192, 1 << 20);
        if self.rbuf.len() - self.rend < want_free {
            if self.rpos > 0 {
                self.rbuf.copy_within(self.rpos..self.rend, 0);
                self.rend -= self.rpos;
                self.rpos = 0;
            }
            if self.rbuf.len() - self.rend < want_free {
                let new_len = (self.rend + want_free).max(self.rbuf.len() * 2);
                self.rbuf.resize(new_len, 0);
            }
        }
        loop {
            let nonblocking = match deadline {
                None => {
                    self.set_read_timeout(None)?;
                    false
                }
                Some(d) => {
                    let now = Instant::now();
                    if d <= now {
                        true
                    } else {
                        self.set_read_timeout(Some(d - now))?;
                        false
                    }
                }
            };
            if nonblocking {
                self.stream.set_nonblocking(true)?;
            }
            let r = self.stream.read(&mut self.rbuf[self.rend..]);
            if nonblocking {
                self.stream.set_nonblocking(false)?;
            }
            match r {
                Ok(0) => {
                    return Err(Error::closed("server closed the connection unexpectedly"));
                }
                Ok(n) => {
                    self.rend += n;
                    return Ok(true);
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ) =>
                {
                    match deadline {
                        Some(d) if !nonblocking && Instant::now() < d => continue,
                        Some(_) => return Ok(false),
                        None => return Err(e.into()),
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Reads the next message; `Ok(None)` if `deadline` passes first.
    /// Errors make the connection unusable.
    fn recv_until(&mut self, deadline: Option<Instant>) -> Result<Option<Msg>> {
        loop {
            match self.take_buffered() {
                Ok(Some(m)) => return Ok(Some(m)),
                Ok(None) => {}
                Err(e) => return Err(self.fail(e)),
            }
            match self.fill(deadline) {
                Ok(true) => {}
                Ok(false) => return Ok(None),
                Err(e) => return Err(self.fail(e)),
            }
        }
    }

    /// Blocking read of the next message.
    fn recv(&mut self) -> Result<Msg> {
        match self.recv_until(None)? {
            Some(m) => Ok(m),
            None => Err(self.fail(Error::new(ErrorKind::Timeout, "unexpected read timeout"))),
        }
    }

    fn send(&mut self) -> Result<()> {
        let r = self.stream.write_all(&self.wbuf);
        self.wbuf.clear();
        // (a large query must not keep its buffer)
        if self.wbuf.capacity() > 4 << 20 {
            self.wbuf = Vec::with_capacity(1024);
        }
        r.map_err(|e| {
            let e = Error::from(e);
            self.fail(e)
        })
    }

    /// Handles asynchronous messages (NoticeResponse, ParameterStatus,
    /// NotificationResponse). Returns true if the message was consumed.
    fn handle_async(&mut self, m: &Msg) -> bool {
        match m.0 {
            b'N' => {
                let f = wire::parse_error_fields(self.body(m), ErrorKind::Server);
                if self.notices.len() >= MAX_NOTICES {
                    self.notices.pop_front();
                }
                self.notices.push_back(Notice {
                    severity: f.severity.unwrap_or_default(),
                    sqlstate: f.error.sqlstate.unwrap_or_default(),
                    message: f.error.message,
                    detail: f.error.detail,
                    hint: f.error.hint,
                });
                true
            }
            b'S' => {
                let mut r = Reader::new(self.body(m));
                if let (Ok(k), Ok(v)) = (r.cstr(), r.cstr()) {
                    self.params.insert(k, v);
                }
                true
            }
            b'A' => true,
            _ => false,
        }
    }

    /// Parses an ErrorResponse; FATAL/PANIC errors make the connection
    /// unusable (the server closes it).
    fn server_error(&mut self, m: &Msg) -> Error {
        let f = wire::parse_error_fields(self.body(m), ErrorKind::Server);
        if matches!(f.severity.as_deref(), Some("FATAL") | Some("PANIC")) {
            let e = f.error;
            self.broken.get_or_insert_with(|| e.message.clone());
            return e;
        }
        f.error
    }

    fn unexpected(&mut self, m: &Msg, context: &str) -> Error {
        let e = Error::protocol(format!(
            "unexpected message type {:?} (0x{:02x}) {context}",
            m.0 as char, m.0
        ));
        self.fail(e)
    }

    fn put_query(&mut self, sql: &str) -> Result<()> {
        if sql.as_bytes().contains(&0) {
            return Err(Error::new(
                ErrorKind::Protocol,
                "query string contains a NUL byte",
            ));
        }
        let at = wire::begin(&mut self.wbuf, b'Q');
        self.wbuf.extend_from_slice(sql.as_bytes());
        self.wbuf.push(0);
        wire::finish(&mut self.wbuf, at);
        Ok(())
    }

    /// Best-effort Terminate + socket shutdown (never blocks for long).
    fn terminate(&mut self) {
        if self.terminated {
            return;
        }
        self.terminated = true;
        if self.broken.is_none() {
            let _ = self.stream.set_write_timeout(Some(Duration::from_secs(1)));
            let _ = self.stream.write_all(&[b'X', 0, 0, 0, 4]);
        }
        self.stream.shutdown();
        self.broken
            .get_or_insert_with(|| "connection closed".into());
    }

    /// Run a simple query through ReadyForQuery, preserving the first server
    /// or consumer error while draining its response. Decode failures poison
    /// the connection; ordinary query errors leave it reusable.
    fn query_response(
        &mut self,
        sql: &str,
        copy_out: bool,
        mut consume: impl FnMut(u8, &[u8], &mut Option<Error>) -> Result<()>,
    ) -> Result<()> {
        self.check()?;
        self.put_query(sql)?;
        self.send()?;
        let mut error = None;
        loop {
            let m = self.recv()?;
            match m.0 {
                b'E' => {
                    let e = self.server_error(&m);
                    if self.broken.is_some() {
                        return Err(e);
                    }
                    error.get_or_insert(e);
                }
                b'Z' => {
                    self.txn_status = self.body(&m).first().copied().unwrap_or(b'I');
                    return error.map_or(Ok(()), Err);
                }
                b'G' | b'H' if m.0 == b'G' || !copy_out => {
                    let message = if copy_out {
                        "COPY FROM STDIN is not supported by this client"
                    } else {
                        "COPY is not supported by simple_query_rows"
                    };
                    if m.0 == b'G' {
                        let at = wire::begin(&mut self.wbuf, b'f');
                        wire::put_cstr(&mut self.wbuf, message);
                        wire::finish(&mut self.wbuf, at);
                        self.send()?;
                    }
                    if !copy_out {
                        error.get_or_insert(Error::new(ErrorKind::Protocol, message));
                    }
                }
                b'T' | b'D' | b'C' | b'I' | b'H' | b'd' | b'c' => {
                    if let Err(e) = consume(m.0, self.body(&m), &mut error) {
                        return Err(self.fail(e));
                    }
                }
                _ if self.handle_async(&m) => {}
                _ => return Err(self.unexpected(&m, "in simple query response")),
            }
        }
    }

    fn simple_query_rows(
        &mut self,
        sql: &str,
        on_row: &mut OnRow<'_>,
    ) -> Result<Vec<String>> {
        let mut tags = Vec::new();
        let mut columns = Vec::new();
        let mut fields = Vec::new();
        self.query_response(sql, false, |tag, body, error| {
            match tag {
                b'T' => columns = parse_row_description(body)?,
                b'D' => {
                    fields.clear();
                    let mut r = Reader::new(body);
                    let n = r.u16()? as usize;
                    for _ in 0..n {
                        let len = r.i32()?;
                        let off = body.len() - r.remaining();
                        if len > 0 {
                            r.bytes(len as usize)?;
                        }
                        fields.push((off as u32, len));
                    }
                    if error.is_none()
                        && let Err(msg) = on_row(&columns, RowRef { body, fields: &fields })
                    {
                        *error = Some(Error::new(ErrorKind::Protocol, msg));
                    }
                }
                b'C' => {
                    tags.push(Reader::new(body).cstr().unwrap_or_default());
                    columns.clear();
                }
                b'I' => columns.clear(),
                _ => {}
            }
            Ok(())
        })?;
        Ok(tags)
    }

    fn simple_query(&mut self, sql: &str) -> Result<Vec<SimpleQueryResult>> {
        let mut results = Vec::new();
        let mut cur: Option<SimpleQueryResult> = None;
        self.query_response(sql, true, |tag, body, _| {
            match tag {
                b'T' => cur = Some(SimpleQueryResult {
                    columns: parse_row_description(body)?,
                    ..Default::default()
                }),
                b'D' => cur.get_or_insert_with(Default::default).rows.push(parse_data_row(body)?),
                b'C' => {
                    let mut res = cur.take().unwrap_or_default();
                    res.tag = Reader::new(body).cstr().unwrap_or_default();
                    results.push(res);
                }
                b'I' => cur = None,
                // COPY TO STDOUT: each CopyData becomes a one-column row.
                b'H' => cur = Some(SimpleQueryResult {
                    columns: vec![Column {
                        name: "COPY".into(),
                        table_oid: 0,
                        column_attr: 0,
                        type_oid: 25,
                        type_size: -1,
                        type_modifier: -1,
                    }],
                    ..Default::default()
                }),
                b'd' => {
                    let mut line = wire::lossy(body);
                    if line.ends_with('\n') {
                        line.pop();
                    }
                    cur.get_or_insert_with(Default::default).rows.push(vec![Some(line)]);
                }
                _ => {}
            }
            Ok(())
        })?;
        Ok(results)
    }

}

fn parse_row_description(body: &[u8]) -> Result<Vec<Column>> {
    let mut r = Reader::new(body);
    let n = r.u16()? as usize;
    let mut cols = Vec::with_capacity(n);
    for _ in 0..n {
        let name = r.cstr()?;
        let table_oid = r.u32()?;
        let column_attr = r.i16()?;
        let type_oid = r.u32()?;
        let type_size = r.i16()?;
        let type_modifier = r.i32()?;
        let _format = r.i16()?;
        cols.push(Column {
            name,
            table_oid,
            column_attr,
            type_oid,
            type_size,
            type_modifier,
        });
    }
    Ok(cols)
}

fn parse_data_row(body: &[u8]) -> Result<Vec<Option<String>>> {
    let mut r = Reader::new(body);
    let n = r.u16()? as usize;
    let mut row = Vec::with_capacity(n);
    for _ in 0..n {
        let len = r.i32()?;
        if len < 0 {
            row.push(None);
        } else {
            row.push(Some(wire::lossy(r.bytes(len as usize)?)));
        }
    }
    Ok(row)
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn startup(cfg: &Config, replication: bool) -> Result<Conn> {
    let deadline = cfg
        .connect_timeout
        .and_then(|d| Instant::now().checked_add(d));
    let stream = open_stream(cfg, deadline)?;
    let mut c = Conn::new(stream);
    let timeout_err = || {
        Error::new(
            ErrorKind::Timeout,
            "timeout expired during connection startup",
        )
    };
    if let Some(d) = deadline {
        let rem = d.saturating_duration_since(Instant::now());
        if rem.is_zero() {
            return Err(timeout_err());
        }
        c.stream.set_write_timeout(Some(rem))?;
    }

    // StartupMessage
    // A setting counts as user-provided if given as its own startup parameter
    // or mentioned in an `options` ("-c name=value") parameter: separate
    // startup GUCs are applied after `options`, so defaults must not be
    // added on top of it.
    let has = |k: &str| {
        let lk = k.to_ascii_lowercase();
        cfg.options.iter().any(|(ok, ov)| {
            ok.eq_ignore_ascii_case(k) || (ok == "options" && ov.to_ascii_lowercase().contains(&lk))
        })
    };
    let mut params: Vec<(&str, &str)> = vec![("user", &cfg.user), ("database", &cfg.dbname)];
    if replication {
        params.push(("replication", "database"));
    }
    if let Some(app) = &cfg.application_name {
        params.push(("application_name", app));
    }
    if !has("client_encoding") {
        params.push(("client_encoding", "UTF8"));
    }
    if replication {
        // Same settings PostgreSQL's own logical replication workers use, so
        // that pgoutput's text output is unambiguous and parseable.
        for (k, v) in [
            ("DateStyle", "ISO"),
            ("IntervalStyle", "postgres"),
            ("extra_float_digits", "3"),
        ] {
            if !has(k) {
                params.push((k, v));
            }
        }
    }
    for (k, v) in &cfg.options {
        if replication && k == "replication" {
            continue;
        }
        params.push((k, v));
    }
    let at = wire::begin_untagged(&mut c.wbuf);
    wire::put_u32(&mut c.wbuf, crate::server::PROTOCOL_VERSION_3_0);
    for (k, v) in params {
        wire::put_cstr(&mut c.wbuf, k);
        wire::put_cstr(&mut c.wbuf, v);
    }
    c.wbuf.push(0);
    wire::finish(&mut c.wbuf, at);
    c.send()?;

    let startup_error = |c: &mut Conn, m: &Msg| {
        let mut e = c.server_error(m);
        if e.sqlstate.as_deref().is_some_and(|s| s.starts_with("28")) {
            e.kind = ErrorKind::Auth;
        }
        c.fail(e)
    };

    // Authentication
    let mut scram: Option<ScramClient> = None;
    loop {
        let m = c.recv_until(deadline)?.ok_or_else(timeout_err)?;
        match m.0 {
            b'R' => {
                let body = c.body(&m).to_vec();
                let mut r = Reader::new(&body);
                let code = r.i32()?;
                let need_password = || {
                    cfg.password.as_deref().ok_or_else(|| {
                        Error::auth(format!(
                            "password authentication failed: server requested a password for user \"{}\" but none was provided",
                            cfg.user
                        ))
                    })
                };
                match code {
                    0 => break,
                    3 => {
                        let pw = need_password()?;
                        let at = wire::begin(&mut c.wbuf, b'p');
                        wire::put_cstr(&mut c.wbuf, pw);
                        wire::finish(&mut c.wbuf, at);
                        c.send()?;
                    }
                    5 => {
                        let salt: [u8; 4] = r.bytes(4)?.try_into().expect("4 bytes");
                        let pw = need_password()?;
                        let hashed = auth::md5_password(&cfg.user, pw, salt);
                        let at = wire::begin(&mut c.wbuf, b'p');
                        wire::put_cstr(&mut c.wbuf, &hashed);
                        wire::finish(&mut c.wbuf, at);
                        c.send()?;
                    }
                    10 => {
                        let mut mechs = Vec::new();
                        loop {
                            let m = r.cstr()?;
                            if m.is_empty() {
                                break;
                            }
                            mechs.push(m);
                        }
                        if !mechs.iter().any(|m| m == auth::SCRAM_SHA_256) {
                            return Err(c.fail(Error::auth(format!(
                                "none of the server's SASL authentication mechanisms are supported: {}",
                                mechs.join(", ")
                            ))));
                        }
                        let pw = need_password()?;
                        let sc = ScramClient::new(pw)?;
                        let first = sc.client_first();
                        let at = wire::begin(&mut c.wbuf, b'p');
                        wire::put_cstr(&mut c.wbuf, auth::SCRAM_SHA_256);
                        wire::put_i32(&mut c.wbuf, first.len() as i32);
                        c.wbuf.extend_from_slice(&first);
                        wire::finish(&mut c.wbuf, at);
                        c.send()?;
                        scram = Some(sc);
                    }
                    11 => {
                        let sc = scram
                            .as_mut()
                            .ok_or_else(|| Error::protocol("unexpected SASLContinue"))?;
                        let fin = sc.server_first(r.rest()).map_err(|e| c.fail(e))?;
                        let at = wire::begin(&mut c.wbuf, b'p');
                        c.wbuf.extend_from_slice(&fin);
                        wire::finish(&mut c.wbuf, at);
                        c.send()?;
                    }
                    12 => {
                        let sc = scram
                            .as_ref()
                            .ok_or_else(|| Error::protocol("unexpected SASLFinal"))?;
                        sc.server_final(r.rest()).map_err(|e| c.fail(e))?;
                    }
                    2 => {
                        return Err(
                            c.fail(Error::auth("Kerberos V5 authentication is not supported"))
                        );
                    }
                    7 | 8 => {
                        return Err(c.fail(Error::auth("GSSAPI authentication is not supported")));
                    }
                    9 => return Err(c.fail(Error::auth("SSPI authentication is not supported"))),
                    _ => {
                        return Err(c.fail(Error::auth(format!(
                            "authentication method {code} is not supported"
                        ))));
                    }
                }
            }
            b'E' => return Err(startup_error(&mut c, &m)),
            b'v' => {} // NegotiateProtocolVersion: we only use 3.0 features.
            _ if c.handle_async(&m) => {}
            _ => return Err(c.unexpected(&m, "during authentication")),
        }
    }

    // Wait for ReadyForQuery.
    loop {
        let m = c.recv_until(deadline)?.ok_or_else(timeout_err)?;
        match m.0 {
            b'K' => {
                let mut r = Reader::new(c.body(&m));
                let pid = r.u32()?;
                let secret = r.u32().unwrap_or(0);
                c.pid = pid;
                c.secret = secret;
            }
            b'Z' => {
                c.txn_status = c.body(&m).first().copied().unwrap_or(b'I');
                break;
            }
            b'E' => return Err(startup_error(&mut c, &m)),
            b'v' => {}
            _ if c.handle_async(&m) => {}
            _ => return Err(c.unexpected(&m, "during connection startup")),
        }
    }
    c.stream.set_write_timeout(None)?;
    Ok(c)
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

pub struct Client {
    conn: Conn,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("backend_pid", &self.conn.pid)
            .field("broken", &self.conn.broken)
            .finish_non_exhaustive()
    }
}

impl Client {
    /// Opens a normal session.
    pub fn connect(cfg: &Config) -> Result<Client> {
        Ok(Client {
            conn: startup(cfg, false)?,
        })
    }

    /// Opens a logical replication session (`replication=database`), which
    /// accepts both replication commands and SQL. Unless overridden in
    /// `cfg.options`, also sets DateStyle=ISO, IntervalStyle=postgres and
    /// extra_float_digits=3 (as PostgreSQL's own apply workers do).
    pub fn connect_replication(cfg: &Config) -> Result<Client> {
        Ok(Client {
            conn: startup(cfg, true)?,
        })
    }

    /// Runs one or more statements with the simple query protocol. Returns
    /// one result per statement that completed (EmptyQueryResponse yields
    /// none). On a server error, the remaining responses are drained, the
    /// connection stays usable, and the error is returned.
    pub fn simple_query(&mut self, sql: &str) -> Result<Vec<SimpleQueryResult>> {
        self.conn.simple_query(sql)
    }

    /// Like `simple_query`, but hands every data row to `on_row` with the
    /// columns of its statement, borrowed from the receive buffer instead
    /// of collected as owned strings; returns the command tags. An error
    /// returned by `on_row` ends the calls to it and is returned (as a
    /// protocol error) once the responses are drained.
    pub fn simple_query_rows(
        &mut self,
        sql: &str,
        on_row: &mut OnRow<'_>,
    ) -> Result<Vec<String>> {
        self.conn.simple_query_rows(sql, on_row)
    }

    /// Like `simple_query`, returning the last result that has a row
    /// description (or, if none does, the last result; or an empty result).
    pub fn query(&mut self, sql: &str) -> Result<SimpleQueryResult> {
        let mut results = self.simple_query(sql)?;
        let idx = results
            .iter()
            .rposition(|r| !r.columns.is_empty())
            .or_else(|| results.len().checked_sub(1));
        Ok(match idx {
            Some(i) => results.swap_remove(i),
            None => SimpleQueryResult::default(),
        })
    }

    /// Like `simple_query`, returning the row count of the last command tag
    /// (0 for tags without a count).
    pub fn execute(&mut self, sql: &str) -> Result<u64> {
        let results = self.simple_query(sql)?;
        Ok(results.last().map(|r| tag_rows(&r.tag)).unwrap_or(0))
    }

    /// Current value of a server parameter reported via ParameterStatus
    /// (server_version, TimeZone, integer_datetimes, ...).
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.conn.params.get(name).map(|s| s.as_str())
    }

    pub fn backend_pid(&self) -> u32 {
        self.conn.pid
    }

    /// Secret key from BackendKeyData (for CancelRequest).
    pub fn backend_secret(&self) -> u32 {
        self.conn.secret
    }

    /// Transaction status from the last ReadyForQuery: `b'I'` idle, `b'T'`
    /// in a transaction block, `b'E'` in a failed transaction block.
    pub fn transaction_status(&self) -> u8 {
        self.conn.txn_status
    }

    /// Whether the connection is still usable (no I/O / protocol failure,
    /// no FATAL error, not closed).
    pub fn is_usable(&self) -> bool {
        self.conn.broken.is_none()
    }

    /// Drains the NoticeResponses received so far (at most the last 1000
    /// are kept).
    pub fn take_notices(&mut self) -> Vec<Notice> {
        self.conn.notices.drain(..).collect()
    }

    /// `IDENTIFY_SYSTEM` (replication connections).
    pub fn identify_system(&mut self) -> Result<IdentifySystem> {
        let r = self.query("IDENTIFY_SYSTEM")?;
        let row = r
            .rows
            .first()
            .ok_or_else(|| Error::protocol("IDENTIFY_SYSTEM returned no rows"))?;
        if row.len() < 4 {
            return Err(Error::protocol("IDENTIFY_SYSTEM returned too few columns"));
        }
        let field = |i: usize| {
            row[i]
                .clone()
                .ok_or_else(|| Error::protocol(format!("IDENTIFY_SYSTEM: column {i} is NULL")))
        };
        Ok(IdentifySystem {
            systemid: field(0)?,
            timeline: field(1)?
                .parse()
                .map_err(|_| Error::protocol("IDENTIFY_SYSTEM: invalid timeline"))?,
            xlogpos: field(2)?.parse()?,
            dbname: row[3].clone(),
        })
    }

    /// `CREATE_REPLICATION_SLOT slot [TEMPORARY] LOGICAL plugin NOEXPORT_SNAPSHOT`.
    pub fn create_logical_slot(
        &mut self,
        slot: &str,
        plugin: &str,
        temporary: bool,
    ) -> Result<SlotInfo> {
        self.create_logical_slot_with_snapshot(slot, plugin, temporary, SnapshotAction::NoExport)
    }

    /// `CREATE_REPLICATION_SLOT` with an explicit snapshot action.
    pub fn create_logical_slot_with_snapshot(
        &mut self,
        slot: &str,
        plugin: &str,
        temporary: bool,
        snapshot: SnapshotAction,
    ) -> Result<SlotInfo> {
        let sql = format!(
            "CREATE_REPLICATION_SLOT {}{} LOGICAL {} {}",
            quote_ident(slot),
            if temporary { " TEMPORARY" } else { "" },
            quote_ident(plugin),
            match snapshot {
                SnapshotAction::NoExport => "NOEXPORT_SNAPSHOT",
                SnapshotAction::Export => "EXPORT_SNAPSHOT",
                SnapshotAction::Use => "USE_SNAPSHOT",
            }
        );
        let r = self.query(&sql)?;
        let row = r
            .rows
            .first()
            .ok_or_else(|| Error::protocol("CREATE_REPLICATION_SLOT returned no rows"))?;
        let get = |i: usize| row.get(i).cloned().flatten();
        Ok(SlotInfo {
            slot_name: get(0).unwrap_or_else(|| slot.to_string()),
            consistent_point: get(1)
                .ok_or_else(|| {
                    Error::protocol("CREATE_REPLICATION_SLOT: missing consistent_point")
                })?
                .parse()?,
            snapshot_name: get(2),
            output_plugin: get(3),
        })
    }

    /// `DROP_REPLICATION_SLOT slot [WAIT]`.
    pub fn drop_slot(&mut self, slot: &str, wait: bool) -> Result<()> {
        let sql = format!(
            "DROP_REPLICATION_SLOT {}{}",
            quote_ident(slot),
            if wait { " WAIT" } else { "" }
        );
        self.simple_query(&sql).map(|_| ())
    }

    /// `START_REPLICATION SLOT slot LOGICAL start (options)`; switches the
    /// connection into CopyBoth mode. `start = Lsn(0)` resumes from the slot's
    /// confirmed_flush_lsn; a start before confirmed_flush_lsn is silently
    /// forwarded to it by the server. Option names are quoted as
    /// identifiers and values as string literals, e.g.
    /// `[("proto_version", "1"), ("publication_names", "p"), ("messages", "true")]`.
    ///
    /// Logical streaming can only be started once per connection: after
    /// [`ReplicationStream::stop`], PostgreSQL's walsender ends any further
    /// logical START_REPLICATION immediately (its CopyDone state is never
    /// reset), so this returns an error; open a new replication connection.
    pub fn start_logical_replication(
        mut self,
        slot: &str,
        start: Lsn,
        options: &[(&str, &str)],
    ) -> Result<ReplicationStream> {
        self.conn.check()?;
        if self.conn.streamed {
            return Err(Error::new(
                ErrorKind::Protocol,
                "logical streaming cannot be restarted on a connection that already streamed \
                 (PostgreSQL walsender limitation); open a new replication connection",
            ));
        }
        let mut sql = format!(
            "START_REPLICATION SLOT {} LOGICAL {start}",
            quote_ident(slot)
        );
        if !options.is_empty() {
            let opts: Vec<String> = options
                .iter()
                .map(|(k, v)| format!("{} {}", quote_ident(k), quote_literal(v)))
                .collect();
            sql.push_str(&format!(" ({})", opts.join(", ")));
        }
        let c = &mut self.conn;
        c.put_query(&sql)?;
        c.send()?;
        let mut error = None;
        loop {
            let m = c.recv()?;
            match m.0 {
                b'W' => {
                    c.streamed = true;
                    break;
                }
                b'E' => {
                    let e = c.server_error(&m);
                    if c.broken.is_some() {
                        return Err(e);
                    }
                    error.get_or_insert(e);
                }
                b'Z' => {
                    return Err(error.unwrap_or_else(|| {
                        Error::protocol("START_REPLICATION finished without entering COPY mode")
                    }));
                }
                b'C' | b'T' | b'D' | b'I' => {}
                _ if c.handle_async(&m) => {}
                _ => return Err(c.unexpected(&m, "in response to START_REPLICATION")),
            }
        }
        Ok(ReplicationStream {
            conn: std::mem::replace(&mut self.conn, Conn::detached()),
            last_wal_end: Lsn(0),
        })
    }

    /// Sends Terminate and closes the socket.
    pub fn close(mut self) {
        self.conn.terminate();
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.conn.terminate();
    }
}

impl Conn {
    /// A placeholder left behind when a connection is moved out of a
    /// `Client` / `ReplicationStream` (whose Drop impls then do nothing).
    fn detached() -> Conn {
        let mut c = Conn::new(Stream::Detached);
        c.rbuf = Vec::new();
        c.terminated = true;
        c.broken = Some("connection was moved".into());
        c
    }
}

// ---------------------------------------------------------------------------
// Replication stream
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplicationEvent {
    XLogData {
        wal_start: Lsn,
        wal_end: Lsn,
        /// Server clock, microseconds since 2000-01-01 UTC.
        server_time: i64,
        message: pgoutput::LogicalMessage,
    },
    Keepalive {
        wal_end: Lsn,
        server_time: i64,
        /// The server will disconnect after `wal_sender_timeout` unless a
        /// standby status update is sent; reply promptly.
        reply_requested: bool,
    },
}

/// A logical replication connection in CopyBoth mode.
pub struct ReplicationStream {
    conn: Conn,
    last_wal_end: Lsn,
}

impl fmt::Debug for ReplicationStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplicationStream")
            .field("backend_pid", &self.conn.pid)
            .field("last_wal_end", &self.last_wal_end)
            .field("broken", &self.conn.broken)
            .finish_non_exhaustive()
    }
}

impl ReplicationStream {
    /// Waits for the next replication event. `timeout = None` blocks
    /// indefinitely; `Some(Duration::ZERO)` only polls. Returns `Ok(None)` on
    /// timeout without losing partially read data (messages may arrive in
    /// pieces across calls). NoticeResponse / ParameterStatus are handled
    /// internally.
    ///
    /// Errors: `Server` for an ErrorResponse (e.g. the walsender was
    /// terminated), `Closed` if the server ended the stream or closed the
    /// connection, `Protocol` for malformed data. After an error other than
    /// a pgoutput decoding error the stream is unusable.
    pub fn recv(&mut self, timeout: Option<Duration>) -> Result<Option<ReplicationEvent>> {
        self.conn.check()?;
        // A timeout too large to represent means "no deadline".
        let deadline = timeout.and_then(|t| Instant::now().checked_add(t));
        loop {
            let Some(m) = self.conn.recv_until(deadline)? else {
                return Ok(None);
            };
            match m.0 {
                b'd' => {
                    let ev = parse_copy_data(self.conn.body(&m))?;
                    match &ev {
                        ReplicationEvent::XLogData { wal_end, .. }
                        | ReplicationEvent::Keepalive { wal_end, .. } => {
                            self.last_wal_end = self.last_wal_end.max(*wal_end)
                        }
                    }
                    return Ok(Some(ev));
                }
                b'E' => {
                    let e = self.conn.server_error(&m);
                    return Err(self.conn.fail(e));
                }
                b'c' => {
                    return Err(self.conn.fail(Error::closed(
                        "replication stream ended by server (CopyDone)",
                    )));
                }
                b'C' => {
                    return Err(self.conn.fail(Error::closed(
                        "replication stream ended by server (CommandComplete)",
                    )));
                }
                _ if self.conn.handle_async(&m) => {}
                _ => return Err(self.conn.unexpected(&m, "in replication stream")),
            }
        }
    }

    /// Sends a Standby Status Update. `flush` is what the server records as
    /// the slot's confirmed_flush_lsn (WAL before it may be recycled).
    pub fn send_standby_status(
        &mut self,
        write: Lsn,
        flush: Lsn,
        apply: Lsn,
        reply_requested: bool,
    ) -> Result<()> {
        self.conn.check()?;
        let c = &mut self.conn;
        let at = wire::begin(&mut c.wbuf, b'd');
        c.wbuf.push(b'r');
        wire::put_u64(&mut c.wbuf, write.0);
        wire::put_u64(&mut c.wbuf, flush.0);
        wire::put_u64(&mut c.wbuf, apply.0);
        wire::put_i64(&mut c.wbuf, wire::pg_now_micros());
        c.wbuf.push(reply_requested as u8);
        wire::finish(&mut c.wbuf, at);
        c.send()
    }

    /// Highest `wal_end` seen in XLogData / keepalive messages so far.
    pub fn last_wal_end(&self) -> Lsn {
        self.last_wal_end
    }

    pub fn backend_pid(&self) -> u32 {
        self.conn.pid
    }

    /// Ends streaming cleanly (CopyDone handshake) and returns the
    /// connection to command mode, e.g. to drop the slot or run SQL. Events
    /// still in flight are discarded. Streaming cannot be started again on
    /// the returned connection (see [`Client::start_logical_replication`]).
    pub fn stop(mut self) -> Result<Client> {
        self.conn.check()?;
        let c = &mut self.conn;
        let at = wire::begin(&mut c.wbuf, b'c');
        wire::finish(&mut c.wbuf, at);
        c.send()?;
        let mut error = None;
        loop {
            let m = c.recv()?;
            match m.0 {
                b'd' | b'c' | b'C' => {}
                b'E' => {
                    let e = c.server_error(&m);
                    if c.broken.is_some() {
                        return Err(e);
                    }
                    error.get_or_insert(e);
                }
                b'Z' => {
                    c.txn_status = c.body(&m).first().copied().unwrap_or(b'I');
                    break;
                }
                _ if c.handle_async(&m) => {}
                _ => return Err(c.unexpected(&m, "while stopping replication")),
            }
        }
        if let Some(e) = error {
            return Err(e);
        }
        Ok(Client {
            conn: std::mem::replace(&mut self.conn, Conn::detached()),
        })
    }
}

impl Drop for ReplicationStream {
    fn drop(&mut self) {
        self.conn.terminate();
    }
}

fn parse_copy_data(body: &[u8]) -> Result<ReplicationEvent> {
    let mut r = Reader::new(body);
    match r.u8()? {
        b'w' => {
            let wal_start = Lsn(r.u64()?);
            let wal_end = Lsn(r.u64()?);
            let server_time = r.i64()?;
            let message = pgoutput::parse(r.rest())?;
            Ok(ReplicationEvent::XLogData {
                wal_start,
                wal_end,
                server_time,
                message,
            })
        }
        b'k' => Ok(ReplicationEvent::Keepalive {
            wal_end: Lsn(r.u64()?),
            server_time: r.i64()?,
            reply_requested: r.u8()? != 0,
        }),
        other => Err(Error::protocol(format!(
            "unknown replication message type {:?} (0x{other:02x})",
            other as char
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kv_parsing() {
        let c = Config::parse(
            "host=/tmp/sock port=5433 user=bob password='it''s' dbname = 'my db' application_name=x\\ y",
        );
        // '' is not an escape in libpq conninfo (only backslash escapes are),
        // so this is 'it' immediately followed by garbage.
        assert!(c.is_err());
        let c = Config::parse(
            r"host=/tmp/sock port=5433 user=bob password='it\'s \\ ok' dbname = 'my db' application_name=x\ y connect_timeout=7 options='-c search_path=s1' TimeZone_ignored_check=1",
        );
        assert!(c.is_err()); // unknown keyword
        let c = Config::parse(
            r"host=/tmp/sock port=5433 user=bob password='it\'s \\ ok' dbname = 'my db' application_name=x\ y connect_timeout=7 options='-c search_path=s1' sslmode=disable",
        )
        .unwrap();
        assert_eq!(c.host, "/tmp/sock");
        assert_eq!(c.port, 5433);
        assert_eq!(c.user, "bob");
        assert_eq!(c.password.as_deref(), Some(r"it's \ ok"));
        assert_eq!(c.dbname, "my db");
        assert_eq!(c.application_name.as_deref(), Some("x y"));
        assert_eq!(c.connect_timeout, Some(Duration::from_secs(7)));
        assert_eq!(
            c.options,
            vec![("options".to_string(), "-c search_path=s1".to_string())]
        );

        let c = Config::parse("").unwrap();
        assert_eq!(c.host, "localhost");
        assert_eq!(c.port, 5432);
        assert_eq!(c.dbname, c.user);
        let c = Config::parse("user=alice").unwrap();
        assert_eq!(c.dbname, "alice");
        assert!(Config::parse("host").is_err());
        assert!(Config::parse("host='abc").is_err());
        assert!(Config::parse("port=abc").is_err());
        assert!(Config::parse("sslmode=require").is_err());
        assert!(
            Config::parse("password= dbname=x")
                .unwrap()
                .password
                .as_deref()
                == Some("dbname=x")
        );
        assert_eq!(
            Config::parse("password='' dbname=x")
                .unwrap()
                .password
                .as_deref(),
            Some("")
        );
    }

    #[test]
    fn uri_parsing() {
        let c = Config::parse("postgresql://al%40ice:p%3Ass@db.example.com:6543/my%20db?application_name=app&connect_timeout=3&options=-c%20x%3Dy").unwrap();
        assert_eq!(c.user, "al@ice");
        assert_eq!(c.password.as_deref(), Some("p:ss"));
        assert_eq!(c.host, "db.example.com");
        assert_eq!(c.port, 6543);
        assert_eq!(c.dbname, "my db");
        assert_eq!(c.application_name.as_deref(), Some("app"));
        assert_eq!(c.connect_timeout, Some(Duration::from_secs(3)));
        assert_eq!(
            c.options,
            vec![("options".to_string(), "-c x=y".to_string())]
        );

        let c = Config::parse("postgres://%2Ftmp%2Fpg/db").unwrap();
        assert_eq!(c.host, "/tmp/pg");
        assert_eq!(c.dbname, "db");
        let c = Config::parse("postgres:///db?host=/var/run/postgresql&port=5499&user=u").unwrap();
        assert_eq!(c.host, "/var/run/postgresql");
        assert_eq!(c.port, 5499);
        assert_eq!(c.user, "u");
        let c = Config::parse("postgresql://[::1]:5433/x").unwrap();
        assert_eq!(c.host, "::1");
        assert_eq!(c.port, 5433);
        let c = Config::parse("postgresql://").unwrap();
        assert_eq!(c.host, "localhost");
        let c = Config::parse("postgresql://u@h").unwrap();
        assert_eq!(
            (c.user.as_str(), c.host.as_str(), c.dbname.as_str()),
            ("u", "h", "u")
        );
        assert!(Config::parse("postgresql://h1,h2/db").is_err());
        assert!(Config::parse("postgresql://h/db?bogus=1").is_err());
        assert!(Config::parse("postgresql://h/db?x").is_err());
        assert!(Config::parse("postgresql://h:99999/db").is_err());
    }

    #[test]
    fn tag_parsing() {
        assert_eq!(tag_rows("INSERT 0 5"), 5);
        assert_eq!(tag_rows("UPDATE 3"), 3);
        assert_eq!(tag_rows("SELECT 0"), 0);
        assert_eq!(tag_rows("CREATE TABLE"), 0);
        assert_eq!(tag_rows(""), 0);
    }
}
