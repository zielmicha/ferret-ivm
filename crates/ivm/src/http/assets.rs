//! The UI's static files, compiled into the binary from `crates/ivm/web/`.
//! Responses carry an ETag and `Cache-Control: no-cache`, so browsers
//! revalidate and pick up a new binary's files immediately.

use axum::extract::Path;
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use std::sync::OnceLock;

pub(crate) struct Asset {
    pub path: &'static str,
    pub body: &'static [u8],
}

macro_rules! assets {
    ($($path:literal),* $(,)?) => {
        &[$(Asset { path: $path, body: include_bytes!(concat!("../../web/", $path)) }),*]
    };
}

/// Every file under `web/` (a test checks that none is missing).
pub(crate) static ASSETS: &[Asset] = assets![
    "index.html",
    "favicon.svg",
    "css/app.css",
    "js/main.js",
    "js/live.js",
    "js/api.js",
    "js/dom.js",
    "js/format.js",
    "js/charts.js",
    "js/graph.js",
    "js/views/overview.js",
    "js/views/sources.js",
    "js/views/catalog.js",
    "js/views/dataflow.js",
    "js/views/arrangements.js",
    "js/views/activity.js",
    "js/views/errors.js",
    "js/views/history.js",
    "js/views/console.js",
    "vendor/dagre.min.js",
    "vendor/dagre.LICENSE.txt",
    "vendor/uPlot.iife.min.js",
    "vendor/uPlot.min.css",
    "vendor/uPlot.LICENSE.txt",
];

pub(crate) fn content_type(path: &str) -> &'static str {
    match path.rsplit('.').next().unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "svg" => "image/svg+xml",
        "json" => "application/json",
        "txt" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

fn etags() -> &'static [String] {
    static ETAGS: OnceLock<Vec<String>> = OnceLock::new();
    ETAGS.get_or_init(|| ASSETS.iter().map(|a| format!("\"{:08x}-{:x}\"", crc32fast::hash(a.body), a.body.len())).collect())
}

fn serve(path: &str, headers: &HeaderMap) -> Response {
    let Some(i) = ASSETS.iter().position(|a| a.path == path) else {
        return (StatusCode::NOT_FOUND, [(header::CONTENT_TYPE, "text/plain; charset=utf-8")], "not found\n").into_response();
    };
    let etag = &etags()[i];
    let cache = (header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    let tag = (header::ETAG, HeaderValue::from_str(etag).expect("ascii etag"));
    let matches = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.split(',').any(|t| t.trim().trim_start_matches("W/") == etag || t.trim() == "*"));
    if matches {
        return (StatusCode::NOT_MODIFIED, [cache, tag]).into_response();
    }
    let ctype = (header::CONTENT_TYPE, HeaderValue::from_static(content_type(path)));
    ([ctype, cache, tag], ASSETS[i].body).into_response()
}

pub(crate) async fn index(headers: HeaderMap) -> Response {
    serve("index.html", &headers)
}

pub(crate) async fn file(Path(path): Path<String>, headers: HeaderMap) -> Response {
    serve(&path, &headers)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn walk(dir: &std::path::Path, base: &std::path::Path, out: &mut Vec<String>) {
        for e in std::fs::read_dir(dir).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, base, out);
            } else {
                out.push(p.strip_prefix(base).unwrap().to_string_lossy().replace('\\', "/"));
            }
        }
    }

    #[test]
    fn every_web_file_is_embedded() {
        let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("web");
        let mut files = Vec::new();
        walk(&base, &base, &mut files);
        for f in &files {
            assert!(ASSETS.iter().any(|a| a.path == f), "web/{f} is not listed in http/assets.rs");
        }
        for a in ASSETS {
            assert_ne!(content_type(a.path), "application/octet-stream", "{}", a.path);
        }
    }
}
