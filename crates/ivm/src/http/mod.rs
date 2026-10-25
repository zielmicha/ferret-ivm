//! Web UI and JSON API for inspecting a running server (`--http-listen`).
//!
//! An axum application on a small tokio runtime of its own; the engine
//! stays thread-based and is reached through [`EngineHandle`]: snapshot
//! requests carry a callback that completes a oneshot channel, so no tokio
//! thread ever blocks on the engine. Connections are served by hyper's
//! HTTP/1.1 implementation with a header read timeout and a bounded header
//! buffer, a cap on concurrent connections, request timeouts and a body size
//! limit. All UI assets are compiled into the binary.

mod api;
mod assets;
mod console;
mod metrics;

use crate::coord::EngineHandle;
use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use base64::Engine as _;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{watch, Semaphore};
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
    /// Address to listen on, e.g. `127.0.0.1:6876`.
    pub listen: String,
    /// If set, every request needs HTTP Basic auth with this password (any
    /// user name).
    pub password: Option<String>,
}

/// A running HTTP server; stops when [`HttpServer::shutdown`] is called or
/// the value is dropped.
pub struct HttpServer {
    pub local_addr: SocketAddr,
    stop: watch::Sender<bool>,
    thread: Option<std::thread::JoinHandle<()>>,
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
    /// Host names accepted without a password (DNS rebinding guard).
    pub listen_host: String,
    pub metrics: Arc<metrics::Metrics>,
    pub engine_slots: Arc<Semaphore>,
    pub stream_slots: Arc<Semaphore>,
    pub console_slots: Arc<Semaphore>,
    pub started: SystemTime,
    pub started_at: Instant,
}

/// Bind `cfg.listen` and serve the UI on a runtime thread of its own.
pub fn start(cfg: HttpConfig, engine: EngineHandle) -> std::io::Result<HttpServer> {
    let listener = std::net::TcpListener::bind(&cfg.listen)?;
    listener.set_nonblocking(true)?;
    let local_addr = listener.local_addr()?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(8)
        .thread_name("ivm-http")
        .enable_all()
        .build()?;
    let (stop, stop_rx) = watch::channel(false);
    let state = Arc::new(AppState {
        engine,
        password: cfg.password.clone().filter(|p| !p.is_empty()),
        listen_host: host_part(&cfg.listen).to_ascii_lowercase(),
        metrics: Arc::new(metrics::Metrics::new()),
        engine_slots: Arc::new(Semaphore::new(MAX_ENGINE_REQUESTS)),
        stream_slots: Arc::new(Semaphore::new(MAX_EVENT_STREAMS)),
        console_slots: Arc::new(Semaphore::new(4)),
        started: SystemTime::now(),
        started_at: Instant::now(),
    });
    let thread = std::thread::Builder::new().name("ivm-http".into()).spawn(move || {
        rt.block_on(async move {
            let listener = match tokio::net::TcpListener::from_std(listener) {
                Ok(l) => l,
                Err(e) => {
                    log::error!("http: cannot use listener: {e}");
                    return;
                }
            };
            tokio::spawn(metrics::run_sampler(state.clone(), stop_rx.clone()));
            serve(listener, router(state), stop_rx).await;
        });
        // don't wait for connections still open (e.g. event streams)
        rt.shutdown_timeout(Duration::from_secs(1));
    })?;
    log::info!("web UI on http://{local_addr}/");
    Ok(HttpServer { local_addr, stop, thread: Some(thread) })
}

async fn serve(listener: tokio::net::TcpListener, app: Router, mut stop: watch::Receiver<bool>) {
    let slots = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    loop {
        let (stream, _peer) = tokio::select! {
            _ = stop.changed() => break,
            r = listener.accept() => match r {
                Ok(c) => c,
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
        let _ = stream.set_nodelay(true);
        let service = TowerToHyperService::new(app.clone());
        let mut stop = stop.clone();
        tokio::spawn(async move {
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
        });
    }
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
/// host names are served.
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
            if !host_allowed(host, &st.listen_host) {
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

fn host_allowed(host_header: &str, listen_host: &str) -> bool {
    let host = host_part(host_header).to_ascii_lowercase();
    if host.is_empty() {
        // HTTP/1.0 clients without Host: not a browser on a rebound name
        return true;
    }
    host == "localhost"
        || host.ends_with(".localhost")
        || host.parse::<std::net::IpAddr>().is_ok()
        || (!listen_host.is_empty() && host == listen_host)
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
        assert!(host_allowed("127.0.0.1:6876", "127.0.0.1"));
        assert!(host_allowed("localhost:1", ""));
        assert!(host_allowed("[::1]:6876", ""));
        assert!(host_allowed("myhost:6876", "myhost"));
        assert!(!host_allowed("evil.example:6876", "127.0.0.1"));
        assert!(!host_allowed("evil.example", "0.0.0.0"));
    }
}
