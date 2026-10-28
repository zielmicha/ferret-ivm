//! Listening sockets of the PostgreSQL-protocol server and the web UI.
//!
//! A listen address is `host:port` (TCP) or, if it contains a `/`, the path
//! of a Unix-domain socket. Binding creates the socket file; the
//! [`SocketFile`] returned with it removes the file when dropped. A socket
//! file nothing accepts connections on (left behind by a process that did
//! not exit cleanly) is replaced; a live socket, or anything else at the
//! path, is an error.

use std::fmt;
use std::io::{self, Read, Write};
use std::mem::MaybeUninit;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};

/// Is `addr` the path of a Unix-domain socket (rather than `host:port`)?
pub fn is_unix(addr: &str) -> bool {
    addr.contains('/')
}

/// Bind a listen address; for a Unix-domain socket, also the
/// [`SocketFile`] that removes the socket file.
pub fn bind(addr: &str) -> io::Result<(Listener, Option<SocketFile>)> {
    if !is_unix(addr) {
        return Ok((Listener::Tcp(TcpListener::bind(addr)?), None));
    }
    let path = Path::new(addr);
    let listener = match UnixListener::bind(path) {
        Ok(l) => l,
        Err(e) if e.kind() == io::ErrorKind::AddrInUse && stale(path) => {
            std::fs::remove_file(path)?;
            UnixListener::bind(path)?
        }
        Err(e) => return Err(e),
    };
    let m = std::fs::symlink_metadata(path)?;
    Ok((Listener::Unix(listener), Some(SocketFile { path: path.to_path_buf(), dev: m.dev(), ino: m.ino() })))
}

/// A socket file that refuses connections: nothing listens on it any more.
fn stale(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_socket())
        && UnixStream::connect(path).is_err_and(|e| e.kind() == io::ErrorKind::ConnectionRefused)
}

/// The file of a bound Unix-domain socket, removed when dropped (unless
/// another file has replaced it since).
#[derive(Debug)]
pub struct SocketFile {
    path: PathBuf,
    dev: u64,
    ino: u64,
}

impl Drop for SocketFile {
    fn drop(&mut self) {
        if std::fs::symlink_metadata(&self.path).is_ok_and(|m| m.dev() == self.dev && m.ino() == self.ino) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

pub enum Listener {
    Tcp(TcpListener),
    Unix(UnixListener),
}

impl From<TcpListener> for Listener {
    fn from(l: TcpListener) -> Listener {
        Listener::Tcp(l)
    }
}

impl From<UnixListener> for Listener {
    fn from(l: UnixListener) -> Listener {
        Listener::Unix(l)
    }
}

impl Listener {
    pub fn accept(&self) -> io::Result<Stream> {
        Ok(match self {
            Listener::Tcp(l) => Stream::Tcp(l.accept()?.0),
            Listener::Unix(l) => Stream::Unix(l.accept()?.0),
        })
    }

    pub fn local_addr(&self) -> io::Result<LocalAddr> {
        Ok(match self {
            Listener::Tcp(l) => LocalAddr::Tcp(l.local_addr()?),
            Listener::Unix(l) => LocalAddr::Unix(l.local_addr()?.as_pathname().map(Path::to_path_buf).unwrap_or_default()),
        })
    }
}

/// The address a listener is bound to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LocalAddr {
    Tcp(SocketAddr),
    /// The socket file's path, as given to [`bind`].
    Unix(PathBuf),
}

impl fmt::Display for LocalAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LocalAddr::Tcp(a) => write!(f, "{a}"),
            LocalAddr::Unix(p) => write!(f, "{}", p.display()),
        }
    }
}

/// A connection accepted by a [`Listener`].
pub enum Stream {
    Tcp(TcpStream),
    Unix(UnixStream),
}

impl Stream {
    pub fn try_clone(&self) -> io::Result<Stream> {
        Ok(match self {
            Stream::Tcp(s) => Stream::Tcp(s.try_clone()?),
            Stream::Unix(s) => Stream::Unix(s.try_clone()?),
        })
    }

    /// TCP_NODELAY (nothing to do on a Unix-domain socket).
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.set_nodelay(nodelay),
            Stream::Unix(_) => Ok(()),
        }
    }

    pub fn set_write_timeout(&self, timeout: Option<std::time::Duration>) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.set_write_timeout(timeout),
            Stream::Unix(s) => s.set_write_timeout(timeout),
        }
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.set_nonblocking(nonblocking),
            Stream::Unix(s) => s.set_nonblocking(nonblocking),
        }
    }

    /// Peek at the next byte without consuming it: `Ok(0)` at the end of
    /// the stream. (`UnixStream::peek` is not stable.)
    pub fn peek_byte(&self) -> io::Result<usize> {
        match self {
            Stream::Tcp(s) => s.peek(&mut [0u8; 1]),
            Stream::Unix(s) => socket2::SockRef::from(s).peek(&mut [MaybeUninit::uninit(); 1]),
        }
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Stream::Tcp(s) => s.read(buf),
            Stream::Unix(s) => s.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Stream::Tcp(s) => s.write(buf),
            Stream::Unix(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.flush(),
            Stream::Unix(s) => s.flush(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::TempDir;

    #[test]
    fn addresses() {
        assert!(!is_unix("127.0.0.1:6875"));
        assert!(!is_unix("[::1]:6875"));
        assert!(!is_unix("localhost:6875"));
        assert!(is_unix("/tmp/.s.PGSQL.6875"));
        assert!(is_unix("./ivm.sock"));
    }

    #[test]
    fn socket_files() {
        let dir = TempDir::new("listen");
        let path = dir.path().join("s");
        let addr = path.to_str().unwrap();
        // bound, removed when dropped
        let (l, file) = bind(addr).unwrap();
        assert_eq!(l.local_addr().unwrap(), LocalAddr::Unix(path.clone()));
        let mut c = UnixStream::connect(&path).unwrap();
        let mut s = l.accept().unwrap();
        c.write_all(b"x").unwrap();
        assert_eq!(s.peek_byte().unwrap(), 1);
        let mut b = [0u8; 1];
        s.read_exact(&mut b).unwrap();
        drop(c);
        assert_eq!(s.peek_byte().unwrap(), 0);
        // a live socket is not taken over
        assert_eq!(bind(addr).err().map(|e| e.kind()), Some(io::ErrorKind::AddrInUse));
        assert!(path.exists());
        drop(file);
        assert!(!path.exists());
        drop(l);
        // a stale one is replaced
        drop(UnixListener::bind(&path).unwrap());
        assert!(path.exists());
        let (_l, file) = bind(addr).unwrap();
        UnixStream::connect(&path).unwrap();
        // a file that replaced the socket is left alone
        std::fs::remove_file(&path).unwrap();
        let _other = UnixListener::bind(&path).unwrap();
        drop(file);
        assert!(path.exists());
        std::fs::remove_file(&path).unwrap();
        // so is anything else at the path
        std::fs::write(&path, b"data").unwrap();
        assert!(bind(addr).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"data");
    }
}
