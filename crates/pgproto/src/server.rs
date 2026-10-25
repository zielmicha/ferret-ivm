//! Backend (server) side message codec, for implementing a pgwire server.
//!
//! Reading is blocking: `read_startup_packet` / `read_frontend_message` read
//! exactly one message from the given reader (wrap sockets in a `BufReader`).
//! If the reader has a read timeout and it fires mid-message, the partially
//! read message is lost, so use timeouts only to abort connections.

use std::io::{self, Read, Write};

use crate::wire::{self, Reader};
use crate::{Error, ErrorKind, Result};

/// Protocol version 3.0 as sent in the StartupMessage.
pub const PROTOCOL_VERSION_3_0: u32 = 196608;
const SSL_REQUEST_CODE: u32 = 80877103;
const GSSENC_REQUEST_CODE: u32 = 80877104;
const CANCEL_REQUEST_CODE: u32 = 80877102;
/// Same limit as PostgreSQL's MAX_STARTUP_PACKET_LENGTH.
const MAX_STARTUP_PACKET_LENGTH: usize = 10000;
/// Largest frontend message accepted (PostgreSQL's PQ_LARGE_MESSAGE_LIMIT).
const MAX_FRONTEND_MESSAGE_LENGTH: usize = 0x3fff_ffff;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartupPacket {
    SslRequest,
    GssEncRequest,
    Cancel {
        pid: u32,
        secret: u32,
    },
    Startup {
        version: u32,
        params: Vec<(String, String)>,
    },
}

impl StartupPacket {
    /// Value of a startup parameter (e.g. "user", "database").
    pub fn param(&self, name: &str) -> Option<&str> {
        match self {
            StartupPacket::Startup { params, .. } => params
                .iter()
                .rev()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.as_str()),
            _ => None,
        }
    }
}

/// Fills `buf` completely. Returns `Ok(false)` if EOF occurs before the first
/// byte (and `allow_eof`), an error for EOF in the middle.
fn read_full<R: Read>(r: &mut R, buf: &mut [u8], allow_eof: bool) -> Result<bool> {
    let mut filled = 0;
    while filled < buf.len() {
        match r.read(&mut buf[filled..]) {
            Ok(0) => {
                if filled == 0 && allow_eof {
                    return Ok(false);
                }
                return Err(Error::closed("unexpected EOF within message"));
            }
            Ok(n) => filled += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(true)
}

/// Reads the first packet of a connection (StartupMessage, SSLRequest,
/// GSSENCRequest or CancelRequest).
pub fn read_startup_packet<R: Read>(r: &mut R) -> Result<StartupPacket> {
    let mut hdr = [0u8; 4];
    if !read_full(r, &mut hdr, true)? {
        return Err(Error::closed("connection closed before startup packet"));
    }
    let len = u32::from_be_bytes(hdr) as usize;
    if !(8..=MAX_STARTUP_PACKET_LENGTH).contains(&len) {
        return Err(Error::protocol(format!(
            "invalid startup packet length {len}"
        )));
    }
    let mut body = vec![0u8; len - 4];
    read_full(r, &mut body, false)?;
    let mut rd = Reader::new(&body);
    let code = rd.u32()?;
    match code {
        SSL_REQUEST_CODE => Ok(StartupPacket::SslRequest),
        GSSENC_REQUEST_CODE => Ok(StartupPacket::GssEncRequest),
        CANCEL_REQUEST_CODE => {
            let pid = rd.u32()?;
            let secret = rd.u32()?;
            Ok(StartupPacket::Cancel { pid, secret })
        }
        version => {
            if version >> 16 != 3 {
                return Err(Error::protocol(format!(
                    "unsupported frontend protocol {}.{}",
                    version >> 16,
                    version & 0xffff
                )));
            }
            let mut params = Vec::new();
            loop {
                let name = rd.cstr_strict()?;
                if name.is_empty() {
                    break;
                }
                let value = rd.cstr_strict()?;
                params.push((name, value));
            }
            Ok(StartupPacket::Startup { version, params })
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrontendMessage {
    Query(String),
    Parse {
        name: String,
        query: String,
        param_types: Vec<u32>,
    },
    Bind {
        portal: String,
        statement: String,
        param_formats: Vec<i16>,
        params: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    },
    Describe {
        /// `b'S'` or `b'P'`.
        kind: u8,
        name: String,
    },
    Execute {
        portal: String,
        max_rows: i32,
    },
    Close {
        kind: u8,
        name: String,
    },
    Sync,
    Flush,
    Terminate,
    /// 'p' message: password, SASLInitialResponse or SASLResponse payload.
    Password(Vec<u8>),
    CopyData(Vec<u8>),
    CopyDone,
    CopyFail(String),
    /// Unsupported; parsed and ignored.
    FunctionCall,
}

fn count(rd: &mut Reader<'_>, elem_size: usize) -> Result<usize> {
    let n = rd.i16()?;
    if n < 0 || (n as usize) * elem_size > rd.remaining() {
        return Err(Error::protocol(format!(
            "invalid array length {n} in message"
        )));
    }
    Ok(n as usize)
}

/// Reads one frontend message. Returns `Ok(None)` on clean EOF (before the
/// first byte of a message). Strings that are not valid UTF-8 produce an
/// error with SQLSTATE 22021.
pub fn read_frontend_message<R: Read>(r: &mut R) -> Result<Option<FrontendMessage>> {
    let mut hdr = [0u8; 5];
    if !read_full(r, &mut hdr, true)? {
        return Ok(None);
    }
    let tag = hdr[0];
    let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
    if !(4..=MAX_FRONTEND_MESSAGE_LENGTH).contains(&len) {
        return Err(Error::protocol(format!(
            "invalid message length {len} for message type {:?}",
            tag as char
        )));
    }
    let mut body = vec![0u8; len - 4];
    read_full(r, &mut body, false)?;
    let mut rd = Reader::new(&body);
    let msg = match tag {
        b'Q' => FrontendMessage::Query(rd.cstr_strict()?),
        b'P' => {
            let name = rd.cstr_strict()?;
            let query = rd.cstr_strict()?;
            let n = count(&mut rd, 4)?;
            let mut param_types = Vec::with_capacity(n);
            for _ in 0..n {
                param_types.push(rd.u32()?);
            }
            FrontendMessage::Parse {
                name,
                query,
                param_types,
            }
        }
        b'B' => {
            let portal = rd.cstr_strict()?;
            let statement = rd.cstr_strict()?;
            let n = count(&mut rd, 2)?;
            let mut param_formats = Vec::with_capacity(n);
            for _ in 0..n {
                param_formats.push(rd.i16()?);
            }
            let n = count(&mut rd, 4)?;
            let mut params = Vec::with_capacity(n);
            for _ in 0..n {
                let l = rd.i32()?;
                if l < 0 {
                    params.push(None);
                } else {
                    params.push(Some(rd.bytes(l as usize)?.to_vec()));
                }
            }
            let n = count(&mut rd, 2)?;
            let mut result_formats = Vec::with_capacity(n);
            for _ in 0..n {
                result_formats.push(rd.i16()?);
            }
            FrontendMessage::Bind {
                portal,
                statement,
                param_formats,
                params,
                result_formats,
            }
        }
        b'D' => {
            let kind = rd.u8()?;
            if kind != b'S' && kind != b'P' {
                return Err(Error::protocol(format!(
                    "invalid DESCRIBE message subtype {}",
                    kind
                )));
            }
            FrontendMessage::Describe {
                kind,
                name: rd.cstr_strict()?,
            }
        }
        b'E' => FrontendMessage::Execute {
            portal: rd.cstr_strict()?,
            max_rows: rd.i32()?,
        },
        b'C' => {
            let kind = rd.u8()?;
            if kind != b'S' && kind != b'P' {
                return Err(Error::protocol(format!(
                    "invalid CLOSE message subtype {}",
                    kind
                )));
            }
            FrontendMessage::Close {
                kind,
                name: rd.cstr_strict()?,
            }
        }
        b'S' => FrontendMessage::Sync,
        b'H' => FrontendMessage::Flush,
        b'X' => FrontendMessage::Terminate,
        b'p' => FrontendMessage::Password(body.clone()),
        b'd' => FrontendMessage::CopyData(body.clone()),
        b'c' => FrontendMessage::CopyDone,
        b'f' => FrontendMessage::CopyFail(wire::lossy(rd.cstr_bytes().unwrap_or(&body))),
        b'F' => FrontendMessage::FunctionCall,
        other => {
            return Err(Error::protocol(format!(
                "invalid frontend message type {} ({:?})",
                other, other as char
            )));
        }
    };
    Ok(Some(msg))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FieldDescription {
    pub name: String,
    pub table_oid: u32,
    pub column_attr: i16,
    pub type_oid: u32,
    pub type_size: i16,
    pub type_modifier: i32,
    /// 0 = text, 1 = binary.
    pub format: i16,
}

/// Buffers backend messages; call [`BackendWriter::flush_to`] to write them out.
#[derive(Debug, Default, Clone)]
pub struct BackendWriter {
    buf: Vec<u8>,
}

impl BackendWriter {
    pub fn new() -> Self {
        BackendWriter { buf: Vec::new() }
    }

    fn simple(&mut self, tag: u8) {
        self.buf.push(tag);
        self.buf.extend_from_slice(&4u32.to_be_bytes());
    }

    fn auth(&mut self, code: i32, extra: &[u8]) {
        let at = wire::begin(&mut self.buf, b'R');
        wire::put_i32(&mut self.buf, code);
        self.buf.extend_from_slice(extra);
        wire::finish(&mut self.buf, at);
    }

    pub fn auth_ok(&mut self) {
        self.auth(0, &[]);
    }
    pub fn auth_cleartext_password(&mut self) {
        self.auth(3, &[]);
    }
    pub fn auth_md5_password(&mut self, salt: [u8; 4]) {
        self.auth(5, &salt);
    }
    /// AuthenticationSASL with the given mechanism names.
    pub fn auth_sasl(&mut self, mechanisms: &[&str]) {
        let mut extra = Vec::new();
        for m in mechanisms {
            wire::put_cstr(&mut extra, m);
        }
        extra.push(0);
        self.auth(10, &extra);
    }
    /// AuthenticationSASLContinue.
    pub fn auth_sasl_continue(&mut self, data: &[u8]) {
        self.auth(11, data);
    }
    /// AuthenticationSASLFinal.
    pub fn auth_sasl_final(&mut self, data: &[u8]) {
        self.auth(12, data);
    }

    /// NegotiateProtocolVersion: the newest minor version supported (for
    /// protocol 3) and the `_pq_.` startup options that were not recognized.
    pub fn negotiate_protocol_version(&mut self, newest_minor: u32, unrecognized: &[&str]) {
        let at = wire::begin(&mut self.buf, b'v');
        wire::put_u32(&mut self.buf, newest_minor);
        wire::put_u32(&mut self.buf, unrecognized.len() as u32);
        for o in unrecognized {
            wire::put_cstr(&mut self.buf, o);
        }
        wire::finish(&mut self.buf, at);
    }

    pub fn parameter_status(&mut self, name: &str, value: &str) {
        let at = wire::begin(&mut self.buf, b'S');
        wire::put_cstr(&mut self.buf, name);
        wire::put_cstr(&mut self.buf, value);
        wire::finish(&mut self.buf, at);
    }

    pub fn backend_key_data(&mut self, pid: u32, secret: u32) {
        let at = wire::begin(&mut self.buf, b'K');
        wire::put_u32(&mut self.buf, pid);
        wire::put_u32(&mut self.buf, secret);
        wire::finish(&mut self.buf, at);
    }

    /// `status`: `b'I'` idle, `b'T'` in transaction, `b'E'` failed transaction.
    pub fn ready_for_query(&mut self, status: u8) {
        let at = wire::begin(&mut self.buf, b'Z');
        self.buf.push(status);
        wire::finish(&mut self.buf, at);
    }

    pub fn row_description(&mut self, fields: &[FieldDescription]) {
        let at = wire::begin(&mut self.buf, b'T');
        wire::put_i16(&mut self.buf, fields.len() as i16);
        for f in fields {
            wire::put_cstr(&mut self.buf, &f.name);
            wire::put_u32(&mut self.buf, f.table_oid);
            wire::put_i16(&mut self.buf, f.column_attr);
            wire::put_u32(&mut self.buf, f.type_oid);
            wire::put_i16(&mut self.buf, f.type_size);
            wire::put_i32(&mut self.buf, f.type_modifier);
            wire::put_i16(&mut self.buf, f.format);
        }
        wire::finish(&mut self.buf, at);
    }

    pub fn data_row(&mut self, values: &[Option<&[u8]>]) {
        let at = wire::begin(&mut self.buf, b'D');
        wire::put_i16(&mut self.buf, values.len() as i16);
        for v in values {
            match v {
                None => wire::put_i32(&mut self.buf, -1),
                Some(d) => {
                    wire::put_i32(&mut self.buf, d.len() as i32);
                    self.buf.extend_from_slice(d);
                }
            }
        }
        wire::finish(&mut self.buf, at);
    }

    pub fn command_complete(&mut self, tag: &str) {
        let at = wire::begin(&mut self.buf, b'C');
        wire::put_cstr(&mut self.buf, tag);
        wire::finish(&mut self.buf, at);
    }

    pub fn empty_query_response(&mut self) {
        self.simple(b'I');
    }

    #[allow(clippy::too_many_arguments)]
    fn error_or_notice(
        &mut self,
        tag: u8,
        severity: &str,
        sqlstate: &str,
        message: &str,
        detail: Option<&str>,
        hint: Option<&str>,
        position: Option<u32>,
    ) {
        let at = wire::begin(&mut self.buf, tag);
        let mut field = |code: u8, v: &str| {
            self.buf.push(code);
            wire::put_cstr(&mut self.buf, v);
        };
        field(b'S', severity);
        field(b'V', severity);
        field(b'C', sqlstate);
        field(b'M', message);
        if let Some(d) = detail {
            field(b'D', d);
        }
        if let Some(h) = hint {
            field(b'H', h);
        }
        if let Some(p) = position {
            field(b'P', &p.to_string());
        }
        self.buf.push(0);
        wire::finish(&mut self.buf, at);
    }

    /// `severity`: "ERROR", "FATAL" or "PANIC". `position` is a 1-based
    /// character index into the query string.
    pub fn error_response(
        &mut self,
        severity: &str,
        sqlstate: &str,
        message: &str,
        detail: Option<&str>,
        hint: Option<&str>,
        position: Option<u32>,
    ) {
        self.error_or_notice(b'E', severity, sqlstate, message, detail, hint, position);
    }

    /// `severity`: "WARNING", "NOTICE", "DEBUG", "INFO" or "LOG".
    pub fn notice_response(&mut self, severity: &str, sqlstate: &str, message: &str) {
        self.error_or_notice(b'N', severity, sqlstate, message, None, None, None);
    }

    pub fn parse_complete(&mut self) {
        self.simple(b'1');
    }
    pub fn bind_complete(&mut self) {
        self.simple(b'2');
    }
    pub fn close_complete(&mut self) {
        self.simple(b'3');
    }
    pub fn no_data(&mut self) {
        self.simple(b'n');
    }

    pub fn parameter_description(&mut self, oids: &[u32]) {
        let at = wire::begin(&mut self.buf, b't');
        wire::put_i16(&mut self.buf, oids.len() as i16);
        for &o in oids {
            wire::put_u32(&mut self.buf, o);
        }
        wire::finish(&mut self.buf, at);
    }

    pub fn portal_suspended(&mut self) {
        self.simple(b's');
    }

    /// `format`: 0 text, 1 binary (overall); one entry per column.
    pub fn copy_out_response(&mut self, format: i8, column_formats: &[i16]) {
        self.copy_response(b'H', format, column_formats);
    }

    fn copy_response(&mut self, tag: u8, format: i8, column_formats: &[i16]) {
        let at = wire::begin(&mut self.buf, tag);
        self.buf.push(format as u8);
        wire::put_i16(&mut self.buf, column_formats.len() as i16);
        for &f in column_formats {
            wire::put_i16(&mut self.buf, f);
        }
        wire::finish(&mut self.buf, at);
    }

    /// CopyInResponse (COPY FROM STDIN).
    pub fn copy_in_response(&mut self, format: i8, column_formats: &[i16]) {
        self.copy_response(b'G', format, column_formats);
    }

    /// CopyBothResponse (used by walsenders for START_REPLICATION).
    pub fn copy_both_response(&mut self, format: i8, column_formats: &[i16]) {
        self.copy_response(b'W', format, column_formats);
    }

    pub fn copy_data(&mut self, data: &[u8]) {
        let at = wire::begin(&mut self.buf, b'd');
        self.buf.extend_from_slice(data);
        wire::finish(&mut self.buf, at);
    }

    pub fn copy_done(&mut self) {
        self.simple(b'c');
    }

    /// NotificationResponse (LISTEN/NOTIFY).
    pub fn notification_response(&mut self, pid: u32, channel: &str, payload: &str) {
        let at = wire::begin(&mut self.buf, b'A');
        wire::put_u32(&mut self.buf, pid);
        wire::put_cstr(&mut self.buf, channel);
        wire::put_cstr(&mut self.buf, payload);
        wire::finish(&mut self.buf, at);
    }

    /// ErrorResponse for an [`Error`] (e.g. a decode error from
    /// [`read_frontend_message`]): uses its SQLSTATE, or 08P01
    /// (protocol_violation) for protocol errors, XX000 otherwise.
    pub fn error(&mut self, e: &Error) {
        let code = e.sqlstate.as_deref().unwrap_or(match e.kind {
            ErrorKind::Protocol => "08P01",
            _ => "XX000",
        });
        self.error_response(
            "ERROR",
            code,
            &e.message,
            e.detail.as_deref(),
            e.hint.as_deref(),
            None,
        );
    }

    /// Appends a single byte with no framing (e.g. `b'N'` to refuse an
    /// SSLRequest / GSSENCRequest).
    pub fn raw_byte(&mut self, b: u8) {
        self.buf.push(b);
    }

    /// The buffered, not yet flushed bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    pub fn clear(&mut self) {
        self.buf.clear();
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Writes all buffered bytes to `w` (then flushes `w`) and clears the
    /// buffer. After an error the connection should be considered broken.
    pub fn flush_to<W: Write>(&mut self, w: &mut W) -> io::Result<()> {
        w.write_all(&self.buf)?;
        w.flush()?;
        self.buf.clear();
        Ok(())
    }
}
