//! `POST /api/query`: a read-only SQL console (SELECT, SHOW, EXPLAIN) with a
//! row cap. Statements run exactly like a pgwire session's, through the
//! engine's request queue.

use super::api::{json, ApiError};
use super::AppState;
use crate::coord::api::{ExecResult, Request, SessionVars};
use crate::coord::EngineMsg;
use crate::sql::parser::{split_statements, Statement};
use crate::types::text::format_datum;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use sqlparser::ast;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const DEFAULT_ROWS: usize = 1000;
pub const MAX_ROWS: usize = 10_000;
/// Bytes of cell text returned per response; rows beyond it are cut.
const MAX_RESULT_BYTES: usize = 8 << 20;
const MAX_CELL: usize = 64 * 1024;
const QUERY_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Deserialize)]
struct QueryRequest {
    sql: String,
    /// "fresh" (default: wait for the ingestion frontier) or "latest".
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Serialize)]
struct Column {
    name: String,
    #[serde(rename = "type")]
    ty: String,
}

#[derive(Serialize, Default)]
struct QueryResponse {
    columns: Vec<Column>,
    rows: Vec<Vec<Option<String>>>,
    /// Rows the statement produced (before the cap).
    row_count: usize,
    truncated: bool,
    command: Option<String>,
    elapsed_ms: f64,
}

/// Statements that only read.
pub(crate) fn read_only(stmt: &Statement) -> bool {
    match stmt {
        Statement::Select { .. } | Statement::Show(_) | Statement::Explain { .. } => true,
        Statement::Sql(s) => matches!(**s, ast::Statement::Query(_) | ast::Statement::ShowVariable { .. } | ast::Statement::Explain { .. }),
        _ => false,
    }
}

fn is_json(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.split(';').next().unwrap_or("").trim().eq_ignore_ascii_case("application/json"))
}

pub(crate) async fn query(State(st): State<Arc<AppState>>, headers: HeaderMap, body: Bytes) -> Response {
    match run(&st, &headers, &body).await {
        Ok(r) => json(&r),
        Err(e) => e.into_response(),
    }
}

async fn run(st: &AppState, headers: &HeaderMap, body: &[u8]) -> Result<QueryResponse, ApiError> {
    // a JSON body cannot be posted cross-origin without a CORS preflight,
    // which this server never grants
    if !is_json(headers) {
        return Err(ApiError::new(StatusCode::UNSUPPORTED_MEDIA_TYPE, "expected an application/json body"));
    }
    let req: QueryRequest = serde_json::from_slice(body).map_err(|e| ApiError::bad_request(format!("invalid request: {e}")))?;
    let limit = req.limit.unwrap_or(DEFAULT_ROWS).min(MAX_ROWS);
    let read_mode = match req.mode.as_deref() {
        None | Some("fresh") => "fresh",
        Some("latest") => "latest",
        Some(m) => return Err(ApiError::bad_request(format!("unknown read mode {m:?} (use fresh or latest)"))),
    };
    let stmts: Vec<String> = split_statements(&req.sql).into_iter().filter(|s| !s.trim().trim_matches(';').trim().is_empty()).collect();
    if stmts.len() != 1 {
        return Err(ApiError::bad_request("enter exactly one statement"));
    }
    let stmt = crate::sql::parse_statement(&stmts[0]).map_err(statement_error)?;
    if !read_only(&stmt) {
        return Err(ApiError::new(StatusCode::FORBIDDEN, "the console runs only SELECT, SHOW and EXPLAIN statements"));
    }
    let Ok(_permit) = st.console_slots.clone().try_acquire_owned() else {
        return Err(ApiError::new(StatusCode::TOO_MANY_REQUESTS, "too many console queries running; try again"));
    };
    let engine = st.engine.clone();
    let started = Instant::now();
    let task = tokio::task::spawn_blocking(move || -> Result<QueryResponse, ApiError> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        let interrupts = std::sync::Arc::new(crate::coord::api::Interrupts::default());
        let msg = EngineMsg::Request(Request::Execute {
            stmt: Box::new(stmt),
            params: vec![],
            vars: SessionVars { read_mode: read_mode.into(), interrupts: Some(interrupts.clone()), ..Default::default() },
            extra: String::new(),
            reply: tx,
        });
        engine.tx.send(msg).map_err(|_| ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "the engine is shutting down"))?;
        let res = match rx.recv_timeout(QUERY_TIMEOUT) {
            Ok(r) => r.map_err(statement_error)?,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                // a read still waiting in the engine is dropped
                interrupts.gone.store(true, std::sync::atomic::Ordering::Release);
                let _ = engine.tx.send(EngineMsg::Wake);
                return Err(ApiError::new(
                    StatusCode::GATEWAY_TIMEOUT,
                    "no result within 30 s (a fresh read waits for initial snapshots; try read mode \"latest\")",
                ));
            }
            Err(_) => return Err(ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "the engine is shutting down")),
        };
        Ok(render(res, limit))
    });
    let mut resp = task.await.map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("query task failed: {e}")))??;
    resp.elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    Ok(resp)
}

/// A failing statement is the client's problem (400), whatever its SQLSTATE,
/// unless the engine is stopping or broken.
fn statement_error(e: crate::error::SqlError) -> ApiError {
    let mut a = ApiError::from_sql(e);
    if a.status == StatusCode::NOT_FOUND {
        a.status = StatusCode::BAD_REQUEST;
    }
    a
}

fn render(res: ExecResult, limit: usize) -> QueryResponse {
    match res {
        ExecResult::Query { columns, rows } => {
            let mut out = QueryResponse {
                columns: columns.iter().map(|c| Column { name: c.name.clone(), ty: crate::types::format_type(&c.ty, c.typmod) }).collect(),
                row_count: rows.len(),
                ..Default::default()
            };
            let mut bytes = 0usize;
            for r in rows.iter() {
                if out.rows.len() >= limit || bytes > MAX_RESULT_BYTES {
                    out.truncated = true;
                    break;
                }
                let vals: Vec<Option<String>> = r
                    .iter()
                    .map(|d| {
                        if d.is_null() {
                            return None;
                        }
                        let mut s = format_datum(d);
                        if s.len() > MAX_CELL {
                            let mut end = MAX_CELL;
                            while !s.is_char_boundary(end) {
                                end -= 1;
                            }
                            s.truncate(end);
                            s.push('…');
                        }
                        bytes += s.len();
                        Some(s)
                    })
                    .collect();
                out.rows.push(vals);
            }
            out
        }
        ExecResult::Command { tag } => QueryResponse { command: Some(tag), ..Default::default() },
        ExecResult::Subscribe { cancel, .. } => {
            cancel.store(true, std::sync::atomic::Ordering::Release);
            QueryResponse { command: Some("SUBSCRIBE is not supported here".into()), ..Default::default() }
        }
    }
}
