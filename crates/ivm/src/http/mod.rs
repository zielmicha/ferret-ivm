//! Web UI and JSON API for inspecting a running server (`--http-listen`).
//!
//! An axum application on a small tokio runtime of its own, on any number
//! of TCP and Unix-domain listeners; the engine stays thread-based and is
//! reached through [`EngineHandle`]: snapshot requests carry a callback
//! that completes a oneshot channel, so no tokio thread ever blocks on the
//! engine. Connections are served by hyper's HTTP/1.1 implementation with
//! a header read timeout and a bounded header buffer, a cap on concurrent
//! connections, request timeouts and a body size limit. All UI assets are
//! compiled into the binary.

mod api;
mod assets;
mod console;
mod metrics;

use crate::coord::EngineHandle;
use crate::listen::{self, Listener, LocalAddr, SocketFile};
use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use base64::Engine as _;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};
use tower_http::compression::CompressionLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::timeout::{RequestBodyTimeoutLayer, TimeoutLayer};

pub use metrics::Sample;

/// Concurrent connections served; further ones are closed right away.
const MAX_CONNECTIONS: usize = 256;
/// Snapshot requests queued at the engine at any time.
const MAX_ENGINE_REQUESTS: usize = 16;
/// Concurrent event streams.
const MAX_EVENT_STREAMS: usize = 64;
const HEADER_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_BODY: usize = 64 * 1024;
/// Request line + headers.
const MAX_HEAD: usize = 64 * 1024;

#[derive(Clone, Debug, Default)]
pub struct HttpConfig {
    /// Addresses to listen on: `host:port` (e.g. `127.0.0.1:6876`) or the
    /// path of a Unix-domain socket (see [`listen`]).
    pub listen: Vec<String>,
    /// If set, every request needs HTTP Basic auth with this password (any
    /// user name).
    pub password: Option<String>,
}

/// A running HTTP server; stops when [`HttpServer::shutdown`] is called or
/// the value is dropped.
pub struct HttpServer {
    /// The bound addresses, in the order of [`HttpConfig::listen`].
    pub local_addrs: Vec<LocalAddr>,
    stop: watch::Sender<bool>,
    thread: Option<std::thread::JoinHandle<()>>,
    /// Removed after the server has stopped (fields drop after `drop`).
    _socket_files: Vec<SocketFile>,
}

impl HttpServer {
    pub fn shutdown(mut self) {
        self.stop_and_join();
    }

    fn stop_and_join(&mut self) {
        let _ = self.stop.send(true);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

pub(crate) struct AppState {
    pub engine: EngineHandle,
    pub password: Option<String>,
    /// Host names accepted without a password (DNS rebinding guard): those
    /// of the TCP listen addresses.
    pub listen_hosts: Vec<String>,
    pub metrics: Arc<metrics::Metrics>,
    pub engine_slots: Arc<Semaphore>,
    pub stream_slots: Arc<Semaphore>,
    pub console_slots: Arc<Semaphore>,
    pub started: SystemTime,
    pub started_at: Instant,
}

/// Bind `cfg.listen` and serve the UI on a runtime thread of its own.
pub fn start(cfg: HttpConfig, engine: EngineHandle) -> std::io::Result<HttpServer> {
    let mut bound = Vec::new();
    let mut socket_files = Vec::new();
    for addr in &cfg.listen {
        let (l, file) = listen::bind(addr).map_err(|e| std::io::Error::new(e.kind(), format!("{addr}: {e}")))?;
        bound.push(l);
        socket_files.extend(file);
    }
    let local_addrs = bound.iter().map(Listener::local_addr).collect::<std::io::Result<Vec<_>>>()?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(8)
        .thread_name("ivm-http")
        .enable_all()
        .build()?;
    let listeners = {
        let _rt = rt.enter();
        bound.into_iter().map(HttpListener::from_std).collect::<std::io::Result<Vec<_>>>()?
    };
    let (stop, stop_rx) = watch::channel(false);
    let state = Arc::new(AppState {
        engine,
        password: cfg.password.clone().filter(|p| !p.is_empty()),
        listen_hosts: cfg.listen.iter().filter(|a| !listen::is_unix(a)).map(|a| host_part(a).to_ascii_lowercase()).collect(),
        metrics: Arc::new(metrics::Metrics::new()),
        engine_slots: Arc::new(Semaphore::new(MAX_ENGINE_REQUESTS)),
        stream_slots: Arc::new(Semaphore::new(MAX_EVENT_STREAMS)),
        console_slots: Arc::new(Semaphore::new(4)),
        started: SystemTime::now(),
        started_at: Instant::now(),
    });
    let thread = std::thread::Builder::new().name("ivm-http".into()).spawn(move || {
        rt.block_on(async move {
            tokio::spawn(metrics::run_sampler(state.clone(), stop_rx.clone()));
            let app = router(state);
            // one connection limit for all listeners
            let slots = Arc::new(Semaphore::new(MAX_CONNECTIONS));
            let servers: Vec<_> =
                listeners.into_iter().map(|l| tokio::spawn(serve(l, app.clone(), slots.clone(), stop_rx.clone()))).collect();
            for s in servers {
                let _ = s.await;
            }
        });
        // don't wait for connections still open (e.g. event streams)
        rt.shutdown_timeout(Duration::from_secs(1));
    })?;
    for a in &local_addrs {
        match a {
            LocalAddr::Tcp(a) => log::info!("web UI on http://{a}/"),
            LocalAddr::Unix(p) => log::info!("web UI on the Unix-domain socket {}", p.display()),
        }
    }
    Ok(HttpServer { local_addrs, stop, thread: Some(thread), _socket_files: socket_files })
}

enum HttpListener {
    Tcp(tokio::net::TcpListener),
    Unix(tokio::net::UnixListener),
}

enum HttpStream {
    Tcp(tokio::net::TcpStream),
    Unix(tokio::net::UnixStream),
}

impl HttpListener {
    /// (Called within the runtime: tokio registers the socket with it.)
    fn from_std(l: Listener) -> std::io::Result<HttpListener> {
        Ok(match l {
            Listener::Tcp(l) => {
                l.set_nonblocking(true)?;
                HttpListener::Tcp(tokio::net::TcpListener::from_std(l)?)
            }
            Listener::Unix(l) => {
                l.set_nonblocking(true)?;
                HttpListener::Unix(tokio::net::UnixListener::from_std(l)?)
            }
        })
    }

    async fn accept(&self) -> std::io::Result<HttpStream> {
        Ok(match self {
            HttpListener::Tcp(l) => HttpStream::Tcp(l.accept().await?.0),
            HttpListener::Unix(l) => HttpStream::Unix(l.accept().await?.0),
        })
    }
}

async fn serve(listener: HttpListener, app: Router, slots: Arc<Semaphore>, mut stop: watch::Receiver<bool>) {
    loop {
        let stream = tokio::select! {
            _ = stop.changed() => break,
            r = listener.accept() => match r {
                Ok(s) => s,
                Err(e) => {
                    // e.g. out of file descriptors: back off instead of spinning
                    log::warn!("http: accept failed: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
        };
        let Ok(permit) = slots.clone().try_acquire_owned() else {
            drop(stream);
            continue;
        };
        let service = TowerToHyperService::new(app.clone());
        let stop = stop.clone();
        match stream {
            HttpStream::Tcp(s) => {
                let _ = s.set_nodelay(true);
                tokio::spawn(connection(s, service, stop, permit));
            }
            HttpStream::Unix(s) => {
                tokio::spawn(connection(s, service, stop, permit));
            }
        }
    }
}

/// Serve one connection until it closes or the server stops.
async fn connection<S>(stream: S, service: TowerToHyperService<Router>, mut stop: watch::Receiver<bool>, permit: OwnedSemaphorePermit)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let mut builder = hyper::server::conn::http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(HEADER_TIMEOUT)
        .max_buf_size(MAX_HEAD)
        .keep_alive(true);
    let conn = builder.serve_connection(TokioIo::new(stream), service);
    tokio::pin!(conn);
    tokio::select! {
        _ = conn.as_mut() => {}
        _ = stop.changed() => {
            conn.as_mut().graceful_shutdown();
            let _ = tokio::time::timeout(Duration::from_secs(1), conn).await;
        }
    }
    drop(permit);
}

fn router(state: Arc<AppState>) -> Router {
    let csp = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; \
               connect-src 'self'; font-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'";
    Router::new()
        .route("/api/overview", get(api::overview))
        .route("/api/metrics", get(metrics::history))
        .route("/api/events", get(metrics::events))
        .route("/api/sources", get(api::sources))
        .route("/api/catalog", get(api::catalog))
        .route("/api/catalog/history", get(api::history))
        .route("/api/dataflow", get(api::dataflow))
        .route("/api/dataflow/node/{id}", get(api::node))
        .route("/api/objects/{name}", get(api::object))
        .route("/api/sessions", get(api::sessions))
        .route("/api/errors", get(api::errors))
        .route("/api/query", post(console::query))
        .route("/api/{*rest}", get(api::not_found).post(api::not_found))
        .route("/", get(assets::index))
        .route("/{*path}", get(assets::file))
        .layer(axum::middleware::from_fn_with_state(state.clone(), guard))
        .layer(CompressionLayer::new())
        .layer(TimeoutLayer::with_status_code(StatusCode::GATEWAY_TIMEOUT, REQUEST_TIMEOUT))
        .layer(RequestBodyTimeoutLayer::new(Duration::from_secs(10)))
        .layer(RequestBodyLimitLayer::new(MAX_BODY))
        .layer(SetResponseHeaderLayer::if_not_present(header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff")))
        .layer(SetResponseHeaderLayer::if_not_present(header::REFERRER_POLICY, HeaderValue::from_static("no-referrer")))
        .layer(SetResponseHeaderLayer::if_not_present(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY")))
        .layer(SetResponseHeaderLayer::if_not_present(header::CONTENT_SECURITY_POLICY, HeaderValue::from_static(csp)))
        .with_state(state)
}

/// Authentication (HTTP Basic, when a password is configured) and, without
/// one, a DNS-rebinding guard: only IP-literal, `localhost` and listen-address
/// host names are served (over Unix-domain sockets too: a reverse proxy on
/// a loopback port may forward to one).
async fn guard(State(st): State<Arc<AppState>>, req: Request, next: Next) -> Response {
    match &st.password {
        Some(pw) => {
            if !basic_auth_ok(req.headers().get(header::AUTHORIZATION), pw) {
                let mut r = api::error_response(StatusCode::UNAUTHORIZED, "authentication required", None);
                r.headers_mut()
                    .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Basic realm=\"ivm\", charset=\"UTF-8\""));
                return r;
            }
        }
        None => {
            let host = req.headers().get(header::HOST).and_then(|h| h.to_str().ok()).unwrap_or("");
            if !host_allowed(host, &st.listen_hosts) {
                return api::error_response(
                    StatusCode::FORBIDDEN,
                    "this host name is not accepted without a password: use an IP address or localhost, or start ivm with --password",
                    None,
                );
            }
        }
    }
    next.run(req).await
}

fn basic_auth_ok(value: Option<&HeaderValue>, password: &str) -> bool {
    let Some(v) = value.and_then(|v| v.to_str().ok()) else { return false };
    let Some((scheme, cred)) = v.trim().split_once(' ') else { return false };
    if !scheme.eq_ignore_ascii_case("basic") {
        return false;
    }
    let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(cred.trim()) else { return false };
    let Some(sep) = decoded.iter().position(|b| *b == b':') else { return false };
    constant_time_eq(&decoded[sep + 1..], password.as_bytes())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = (a.len() ^ b.len()) as u64;
    for i in 0..a.len().max(b.len()) {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= (x ^ y) as u64;
    }
    diff == 0
}

/// Host part of "host:port", "[v6]:port" or a bare host.
fn host_part(s: &str) -> &str {
    let s = s.trim();
    if let Some(rest) = s.strip_prefix('[') {
        return rest.split(']').next().unwrap_or("");
    }
    match s.rsplit_once(':') {
        Some((h, p)) if !h.contains(':') && p.chars().all(|c| c.is_ascii_digit()) => h,
        _ => s,
    }
}

fn host_allowed(host_header: &str, listen_hosts: &[String]) -> bool {
    let host = host_part(host_header).to_ascii_lowercase();
    if host.is_empty() {
        // HTTP/1.0 clients without Host: not a browser on a rebound name
        return true;
    }
    host == "localhost"
        || host.ends_with(".localhost")
        || host.parse::<std::net::IpAddr>().is_ok()
        || listen_hosts.iter().any(|h| !h.is_empty() && *h == host)
}

impl IntoResponse for api::ApiError {
    fn into_response(self) -> Response {
        api::error_response(self.status, &self.message, self.code.as_deref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_auth() {
        let enc = |s: &str| HeaderValue::from_str(&format!("Basic {}", base64::engine::general_purpose::STANDARD.encode(s))).unwrap();
        assert!(basic_auth_ok(Some(&enc("any:pw")), "pw"));
        assert!(basic_auth_ok(Some(&enc(":pw")), "pw"));
        assert!(basic_auth_ok(Some(&enc("u:p:w")), "p:w"));
        assert!(!basic_auth_ok(Some(&enc("u:pw2")), "pw"));
        assert!(!basic_auth_ok(Some(&enc("pw")), "pw"));
        assert!(!basic_auth_ok(Some(&HeaderValue::from_static("Bearer x")), "pw"));
        assert!(!basic_auth_ok(Some(&HeaderValue::from_static("Basic !!!")), "pw"));
        assert!(!basic_auth_ok(None, "pw"));
    }

    #[test]
    fn hosts() {
        assert_eq!(host_part("127.0.0.1:6876"), "127.0.0.1");
        assert_eq!(host_part("[::1]:80"), "::1");
        assert_eq!(host_part("example.com"), "example.com");
        let hosts = |h: &[&str]| h.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert!(host_allowed("127.0.0.1:6876", &hosts(&["127.0.0.1"])));
        assert!(host_allowed("localhost:1", &[]));
        assert!(host_allowed("[::1]:6876", &[]));
        assert!(host_allowed("myhost:6876", &hosts(&["myhost"])));
        assert!(host_allowed("myhost:6876", &hosts(&["127.0.0.1", "myhost"])));
        assert!(!host_allowed("evil.example:6876", &hosts(&["127.0.0.1"])));
        assert!(!host_allowed("evil.example", &hosts(&["0.0.0.0"])));
        assert!(!host_allowed("evil.example", &hosts(&[""])));
    }
}
