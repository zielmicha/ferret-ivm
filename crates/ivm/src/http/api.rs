//! JSON endpoints: engine snapshots (see `coord::inspect`) plus a few
//! process-level facts gathered off the engine thread.

use super::AppState;
use crate::coord::inspect::{InspectQuery, InspectReply};
use crate::error::{codes, SqlError};
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// How long a request waits for the engine (it may be busy hydrating).
const ENGINE_TIMEOUT: Duration = Duration::from_secs(15);

pub(crate) struct ApiError {
    pub status: StatusCode,
    pub message: String,
    pub code: Option<String>,
}

impl ApiError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> ApiError {
        ApiError { status, message: message.into(), code: None }
    }

    pub fn bad_request(message: impl Into<String>) -> ApiError {
        ApiError::new(StatusCode::BAD_REQUEST, message)
    }

    pub fn from_sql(e: SqlError) -> ApiError {
        let status = match e.code.as_str() {
            codes::UNDEFINED_OBJECT | codes::UNDEFINED_TABLE => StatusCode::NOT_FOUND,
            codes::ADMIN_SHUTDOWN => StatusCode::SERVICE_UNAVAILABLE,
            codes::QUERY_CANCELED => StatusCode::GATEWAY_TIMEOUT,
            c if c.starts_with("XX") || c.starts_with("58") => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::BAD_REQUEST,
        };
        ApiError { status, message: e.message, code: Some(e.code) }
    }
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    error: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<&'a str>,
}

pub(crate) fn error_response(status: StatusCode, message: &str, code: Option<&str>) -> Response {
    let mut r = (status, axum::Json(ErrorBody { error: message, code })).into_response();
    r.headers_mut().insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

/// Serialize a value as an uncached JSON response.
pub(crate) fn json<T: Serialize>(v: &T) -> Response {
    match serde_json::to_vec(v) {
        Ok(body) => (
            [(header::CONTENT_TYPE, HeaderValue::from_static("application/json")), (header::CACHE_CONTROL, HeaderValue::from_static("no-store"))],
            body,
        )
            .into_response(),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &format!("cannot serialize response: {e}"), None),
    }
}

/// Run a snapshot on the engine thread. At most `MAX_ENGINE_REQUESTS` are
/// queued there at once: the permit is released by the engine's reply, not
/// by a timeout here, so a stalled engine cannot accumulate work.
pub(crate) async fn inspect(st: &AppState, query: InspectQuery) -> Result<InspectReply, ApiError> {
    let permit = match tokio::time::timeout(ENGINE_TIMEOUT, st.engine_slots.clone().acquire_owned()).await {
        Ok(Ok(p)) => p,
        _ => return Err(ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "the engine is busy; try again")),
    };
    let (tx, rx) = tokio::sync::oneshot::channel();
    st.engine
        .inspect(query, move |r| {
            drop(permit);
            let _ = tx.send(r);
        })
        .map_err(ApiError::from_sql)?;
    match tokio::time::timeout(ENGINE_TIMEOUT, rx).await {
        Ok(Ok(r)) => r.map_err(ApiError::from_sql),
        Ok(Err(_)) => Err(ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "the engine is shutting down")),
        Err(_) => Err(ApiError::new(StatusCode::GATEWAY_TIMEOUT, "the engine did not answer in time (it may be busy hydrating a view)")),
    }
}

async fn snapshot(st: &AppState, q: InspectQuery) -> Response {
    match inspect(st, q).await {
        Ok(r) => json(&r),
        Err(e) => e.into_response(),
    }
}

pub(crate) fn param<T: std::str::FromStr>(q: &HashMap<String, String>, name: &str, default: T) -> Result<T, ApiError> {
    match q.get(name) {
        None => Ok(default),
        Some(v) => v.parse().map_err(|_| ApiError::bad_request(format!("invalid value for parameter {name}: {v:?}"))),
    }
}

#[derive(Serialize)]
struct OverviewResponse {
    #[serde(flatten)]
    engine: InspectReply,
    process: super::metrics::ProcessInfo,
    server: ServerInfo,
}

#[derive(Serialize)]
struct ServerInfo {
    started_ms: f64,
    uptime_secs: f64,
    auth: bool,
    log_bytes: u64,
    store_bytes: u64,
}

fn dir_bytes(dir: &std::path::Path) -> u64 {
    std::fs::read_dir(dir)
        .map(|rd| rd.flatten().filter_map(|e| e.metadata().ok()).filter(|m| m.is_file()).map(|m| m.len()).sum())
        .unwrap_or(0)
}

pub(crate) async fn overview(State(st): State<Arc<AppState>>) -> Response {
    let engine = match inspect(&st, InspectQuery::Overview).await {
        Ok(r) => r,
        Err(e) => return e.into_response(),
    };
    let data_dir = match &engine {
        InspectReply::Overview(o) => std::path::PathBuf::from(&o.config.data_dir),
        _ => std::path::PathBuf::new(),
    };
    let (log_bytes, store_bytes) = tokio::task::spawn_blocking(move || (dir_bytes(&data_dir.join("log")), dir_bytes(&data_dir.join("store"))))
        .await
        .unwrap_or((0, 0));
    json(&OverviewResponse {
        engine,
        process: st.metrics.process(),
        server: ServerInfo {
            started_ms: st.started.duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs_f64() * 1000.0).unwrap_or(0.0),
            uptime_secs: st.started_at.elapsed().as_secs_f64(),
            auth: st.password.is_some(),
            log_bytes,
            store_bytes,
        },
    })
}

pub(crate) async fn sources(State(st): State<Arc<AppState>>) -> Response {
    snapshot(&st, InspectQuery::Sources).await
}

pub(crate) async fn catalog(State(st): State<Arc<AppState>>) -> Response {
    snapshot(&st, InspectQuery::Catalog).await
}

pub(crate) async fn history(State(st): State<Arc<AppState>>, Query(q): Query<HashMap<String, String>>) -> Response {
    match param(&q, "limit", 500usize) {
        Ok(limit) => snapshot(&st, InspectQuery::History { limit: limit.min(100_000) }).await,
        Err(e) => e.into_response(),
    }
}

pub(crate) async fn dataflow(State(st): State<Arc<AppState>>) -> Response {
    snapshot(&st, InspectQuery::Dataflow).await
}

pub(crate) async fn node(State(st): State<Arc<AppState>>, Path(id): Path<String>, Query(q): Query<HashMap<String, String>>) -> Response {
    let Ok(id) = id.parse::<usize>() else {
        return ApiError::bad_request(format!("invalid node id {id:?}")).into_response();
    };
    match param(&q, "sample", 20usize) {
        Ok(sample) => snapshot(&st, InspectQuery::Node { id, sample }).await,
        Err(e) => e.into_response(),
    }
}

pub(crate) async fn object(State(st): State<Arc<AppState>>, Path(name): Path<String>, Query(q): Query<HashMap<String, String>>) -> Response {
    if name.is_empty() || name.len() > 1024 {
        return ApiError::bad_request("invalid object name").into_response();
    }
    match param(&q, "sample", 50usize) {
        Ok(sample) => snapshot(&st, InspectQuery::Object { name, sample }).await,
        Err(e) => e.into_response(),
    }
}

pub(crate) async fn sessions(State(st): State<Arc<AppState>>) -> Response {
    snapshot(&st, InspectQuery::Sessions).await
}

pub(crate) async fn errors(State(st): State<Arc<AppState>>) -> Response {
    snapshot(&st, InspectQuery::Errors).await
}

pub(crate) async fn not_found() -> Response {
    error_response(StatusCode::NOT_FOUND, "no such endpoint", None)
}
