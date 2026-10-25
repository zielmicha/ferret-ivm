//! pgproto: blocking PostgreSQL wire-protocol client (including logical
//! replication with pgoutput decoding), a backend-side message codec for
//! implementing a pgwire server, and a temporary-cluster test utility.
//!
//! Everything is synchronous (std::net / std::os::unix::net with timeouts);
//! there is no TLS support.

use std::fmt;
use std::str::FromStr;

mod auth;
pub mod client;
pub mod pgoutput;
pub mod server;
pub mod testutil;
mod wire;

pub use client::*;

/// A PostgreSQL WAL location. Displayed / parsed as `"16/B374D848"`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Lsn(pub u64);

impl Lsn {
    /// `0/0`, PostgreSQL's InvalidXLogRecPtr.
    pub const INVALID: Lsn = Lsn(0);
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

impl FromStr for Lsn {
    type Err = Error;
    fn from_str(s: &str) -> Result<Lsn> {
        let bad = || Error::new(ErrorKind::Protocol, format!("invalid LSN: {s:?}"));
        let (hi, lo) = s.trim().split_once('/').ok_or_else(bad)?;
        let ok =
            |p: &str| !p.is_empty() && p.len() <= 8 && p.bytes().all(|b| b.is_ascii_hexdigit());
        if !ok(hi) || !ok(lo) {
            return Err(bad());
        }
        let hi = u32::from_str_radix(hi, 16).map_err(|_| bad())?;
        let lo = u32::from_str_radix(lo, 16).map_err(|_| bad())?;
        Ok(Lsn(((hi as u64) << 32) | lo as u64))
    }
}

impl From<u64> for Lsn {
    fn from(v: u64) -> Lsn {
        Lsn(v)
    }
}

impl From<Lsn> for u64 {
    fn from(v: Lsn) -> u64 {
        v.0
    }
}

/// Error category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Socket / OS level failure.
    Io,
    /// Malformed or unexpected protocol data.
    Protocol,
    /// Authentication failure (including server errors with SQLSTATE class
    /// 28 received during the startup phase) or unsupported auth method.
    Auth,
    /// ErrorResponse from the server.
    Server,
    /// A timeout expired (connect timeout).
    Timeout,
    /// The connection was closed (by the server, or earlier failure made it
    /// unusable).
    Closed,
    /// Invalid configuration / connection string.
    Config,
}

#[derive(Debug, Clone)]
pub struct Error {
    pub kind: ErrorKind,
    pub sqlstate: Option<String>,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Error {
        Error {
            kind,
            sqlstate: None,
            message: message.into(),
            detail: None,
            hint: None,
        }
    }

    pub(crate) fn protocol(message: impl Into<String>) -> Error {
        Error::new(ErrorKind::Protocol, message)
    }

    pub(crate) fn closed(message: impl Into<String>) -> Error {
        Error::new(ErrorKind::Closed, message)
    }

    pub(crate) fn config(message: impl Into<String>) -> Error {
        Error::new(ErrorKind::Config, message)
    }

    pub(crate) fn auth(message: impl Into<String>) -> Error {
        Error::new(ErrorKind::Auth, message)
    }

    /// The SQLSTATE code, if this error came from the server.
    pub fn code(&self) -> Option<&str> {
        self.sqlstate.as_deref()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)?;
        if let Some(s) = &self.sqlstate {
            write!(f, " (SQLSTATE {s})")?;
        }
        Ok(())
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        use std::io::ErrorKind as K;
        let kind = match e.kind() {
            K::UnexpectedEof | K::ConnectionReset | K::ConnectionAborted | K::BrokenPipe => {
                ErrorKind::Closed
            }
            K::TimedOut | K::WouldBlock => ErrorKind::Timeout,
            _ => ErrorKind::Io,
        };
        Error::new(kind, e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_roundtrip() {
        let l: Lsn = "16/B374D848".parse().unwrap();
        assert_eq!(l, Lsn(0x16_B374_D848));
        assert_eq!(l.to_string(), "16/B374D848");
        assert_eq!(Lsn(0).to_string(), "0/0");
        assert_eq!("0/0".parse::<Lsn>().unwrap(), Lsn(0));
        assert_eq!("ffffffff/FFFFFFFF".parse::<Lsn>().unwrap(), Lsn(u64::MAX));
        for bad in [
            "",
            "/",
            "1/",
            "/1",
            "1",
            "123456789/0",
            "g/0",
            "1/2/3",
            "-1/0",
            "+1/0",
        ] {
            assert!(bad.parse::<Lsn>().is_err(), "{bad}");
        }
    }
}
