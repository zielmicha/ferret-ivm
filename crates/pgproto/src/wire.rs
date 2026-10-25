//! Low-level helpers for reading and writing protocol messages.

use crate::{Error, Result};

/// Bounds-checked big-endian reader over a message body.
pub(crate) struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub(crate) fn bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(Error::protocol(format!(
                "message truncated: need {n} bytes, have {}",
                self.remaining()
            )));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut a = [0u8; N];
        a.copy_from_slice(self.bytes(N)?);
        Ok(a)
    }

    pub(crate) fn u8(&mut self) -> Result<u8> {
        Ok(self.array::<1>()?[0])
    }
    pub(crate) fn i16(&mut self) -> Result<i16> {
        Ok(i16::from_be_bytes(self.array()?))
    }
    pub(crate) fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.array()?))
    }
    pub(crate) fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.array()?))
    }
    pub(crate) fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.array()?))
    }
    pub(crate) fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(self.array()?))
    }
    pub(crate) fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    /// A NUL-terminated string, as raw bytes (without the terminator).
    pub(crate) fn cstr_bytes(&mut self) -> Result<&'a [u8]> {
        let rest = &self.buf[self.pos..];
        match rest.iter().position(|&b| b == 0) {
            Some(i) => {
                self.pos += i + 1;
                Ok(&rest[..i])
            }
            None => Err(Error::protocol("unterminated string in message")),
        }
    }

    /// A NUL-terminated string, converted lossily to UTF-8.
    pub(crate) fn cstr(&mut self) -> Result<String> {
        Ok(lossy(self.cstr_bytes()?))
    }

    /// A NUL-terminated string that must be valid UTF-8.
    pub(crate) fn cstr_strict(&mut self) -> Result<String> {
        let b = self.cstr_bytes()?;
        String::from_utf8(b.to_vec()).map_err(|_| Error {
            sqlstate: Some("22021".into()),
            ..Error::protocol("invalid byte sequence for encoding \"UTF8\"")
        })
    }

    pub(crate) fn rest(&mut self) -> &'a [u8] {
        let s = &self.buf[self.pos..];
        self.pos = self.buf.len();
        s
    }
}

pub(crate) fn lossy(b: &[u8]) -> String {
    match std::str::from_utf8(b) {
        Ok(s) => s.to_owned(),
        Err(_) => String::from_utf8_lossy(b).into_owned(),
    }
}

/// Starts a tagged message; returns the offset of the length field.
pub(crate) fn begin(buf: &mut Vec<u8>, tag: u8) -> usize {
    buf.push(tag);
    begin_untagged(buf)
}

/// Starts an untagged (startup-style) message.
pub(crate) fn begin_untagged(buf: &mut Vec<u8>) -> usize {
    let at = buf.len();
    buf.extend_from_slice(&[0, 0, 0, 0]);
    at
}

/// Patches the length field written by [`begin`].
pub(crate) fn finish(buf: &mut [u8], at: usize) {
    let len = (buf.len() - at) as u32;
    buf[at..at + 4].copy_from_slice(&len.to_be_bytes());
}

/// Appends a NUL-terminated string. Embedded NUL bytes would corrupt the
/// message structure and are dropped.
pub(crate) fn put_cstr(buf: &mut Vec<u8>, s: &str) {
    if s.as_bytes().contains(&0) {
        buf.extend(s.bytes().filter(|&b| b != 0));
    } else {
        buf.extend_from_slice(s.as_bytes());
    }
    buf.push(0);
}

pub(crate) fn put_i16(buf: &mut Vec<u8>, v: i16) {
    buf.extend_from_slice(&v.to_be_bytes());
}
pub(crate) fn put_i32(buf: &mut Vec<u8>, v: i32) {
    buf.extend_from_slice(&v.to_be_bytes());
}
pub(crate) fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_be_bytes());
}
pub(crate) fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_be_bytes());
}
pub(crate) fn put_i64(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_be_bytes());
}

/// Parsed ErrorResponse / NoticeResponse.
pub(crate) struct ErrorFields {
    pub(crate) severity: Option<String>,
    pub(crate) error: Error,
}

pub(crate) fn parse_error_fields(body: &[u8], kind: crate::ErrorKind) -> ErrorFields {
    let mut r = Reader::new(body);
    let mut localized_severity = None;
    let mut severity = None;
    let mut err = Error {
        kind,
        sqlstate: None,
        message: String::new(),
        detail: None,
        hint: None,
    };
    while let Ok(code) = r.u8() {
        if code == 0 {
            break;
        }
        let Ok(value) = r.cstr() else { break };
        match code {
            b'S' => localized_severity = Some(value),
            b'V' => severity = Some(value),
            b'C' => err.sqlstate = Some(value),
            b'M' => err.message = value,
            b'D' => err.detail = Some(value),
            b'H' => err.hint = Some(value),
            _ => {}
        }
    }
    if err.message.is_empty() {
        err.message = "unknown server error (no message field)".into();
    }
    ErrorFields {
        severity: severity.or(localized_severity),
        error: err,
    }
}

/// Microseconds between the Unix epoch and the PostgreSQL epoch (2000-01-01).
pub(crate) const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

/// Current time as microseconds since 2000-01-01 00:00:00 UTC.
pub(crate) fn pg_now_micros() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0);
    now - PG_EPOCH_OFFSET_MICROS
}
