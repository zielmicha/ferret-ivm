//! Time series for the dashboard: a once-a-second sampler keeping a ring
//! buffer of recent samples and pushing each one to Server-Sent Events
//! subscribers.
//!
//! A sample combines atomics shared with the engine and sources (GSNs, LSN
//! frontiers, ingest counters), process statistics from /proc, and the
//! engine's own counters, fetched with one cheap snapshot request per tick.
//! The sampler never blocks on the engine: when the engine has not answered
//! within a tick, the sample says so and the request stays outstanding.

use super::api::{error_response, json, param};
use super::AppState;
use crate::coord::inspect::{EngineStats, InspectQuery, InspectReply};
use crate::error::Result as SqlResult;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use parking_lot::Mutex;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, oneshot, watch};

/// Samples kept (one per second).
const RING: usize = 1200;
const INTERVAL: Duration = Duration::from_secs(1);
/// How long a tick waits for the engine's counters.
const ENGINE_WAIT: Duration = Duration::from_millis(600);

#[derive(Serialize, Clone, Debug)]
pub struct Sample {
    /// Unix ms.
    pub t: f64,
    pub durable_gsn: u64,
    pub processed_gsn: u64,
    pub rss_bytes: Option<u64>,
    /// Process CPU over the last interval (100 = one core).
    pub cpu_percent: Option<f64>,
    /// Engine counters from this tick (None: the engine is busy).
    pub engine: Option<EngineStats>,
    /// Time the engine took to answer this tick's request.
    pub engine_latency_ms: Option<f64>,
    /// How long an unanswered engine request has been outstanding.
    pub engine_busy_ms: Option<f64>,
    pub sources: Vec<SourceSample>,
}

#[derive(Serialize, Clone, Debug)]
pub struct SourceSample {
    pub id: u64,
    pub frontier_lsn: u64,
    pub upstream_lsn: u64,
    pub commits: u64,
    pub changes: u64,
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct ProcessInfo {
    pub pid: u32,
    pub rss_bytes: Option<u64>,
    pub virtual_bytes: Option<u64>,
    pub threads: Option<u64>,
    pub open_fds: Option<u64>,
    pub cpu_seconds: Option<f64>,
    pub cpu_percent: Option<f64>,
}

#[derive(Clone)]
struct Pushed {
    event: &'static str,
    data: Arc<str>,
}

pub(crate) struct Metrics {
    ring: Mutex<VecDeque<Arc<Sample>>>,
    tx: broadcast::Sender<Pushed>,
    last_cpu: Mutex<Option<(f64, Instant)>>,
    cpu_percent: Mutex<Option<f64>>,
}

impl Metrics {
    pub fn new() -> Metrics {
        let (tx, _) = broadcast::channel(64);
        Metrics { ring: Mutex::new(VecDeque::with_capacity(RING)), tx, last_cpu: Mutex::new(None), cpu_percent: Mutex::new(None) }
    }

    fn push(&self, s: Sample) {
        let s = Arc::new(s);
        {
            let mut r = self.ring.lock();
            if r.len() >= RING {
                r.pop_front();
            }
            r.push_back(s.clone());
        }
        if self.tx.receiver_count() > 0
            && let Ok(data) = serde_json::to_string(&*s)
        {
            let _ = self.tx.send(Pushed { event: "sample", data: data.into() });
        }
    }

    fn announce(&self, event: &'static str, data: String) {
        let _ = self.tx.send(Pushed { event, data: data.into() });
    }

    /// CPU usage since the previous call (process-wide).
    fn sample_cpu(&self) -> Option<f64> {
        let secs = proc_cpu_seconds()?;
        let now = Instant::now();
        let mut last = self.last_cpu.lock();
        let pct = last.map(|(s, t)| {
            let wall = now.duration_since(t).as_secs_f64();
            if wall > 0.0 { ((secs - s) / wall * 100.0).max(0.0) } else { 0.0 }
        });
        *last = Some((secs, now));
        *self.cpu_percent.lock() = pct;
        pct
    }

    pub fn process(&self) -> ProcessInfo {
        let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
        let kb = |key: &str| -> Option<u64> {
            status.lines().find(|l| l.starts_with(key))?.split_whitespace().nth(1)?.parse::<u64>().ok()
        };
        ProcessInfo {
            pid: std::process::id(),
            rss_bytes: kb("VmRSS:").map(|k| k * 1024),
            virtual_bytes: kb("VmSize:").map(|k| k * 1024),
            threads: kb("Threads:"),
            open_fds: std::fs::read_dir("/proc/self/fd").ok().map(|d| d.count() as u64),
            cpu_seconds: proc_cpu_seconds(),
            cpu_percent: *self.cpu_percent.lock(),
        }
    }
}

/// User + system CPU seconds of this process (Linux; clock ticks of 1/100 s).
fn proc_cpu_seconds() -> Option<f64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    // fields after the parenthesized command name, which may contain spaces
    let rest = &stat[stat.rfind(')')? + 2..];
    let f: Vec<&str> = rest.split_whitespace().collect();
    let utime: f64 = f.get(11)?.parse().ok()?;
    let stime: f64 = f.get(12)?.parse().ok()?;
    Some((utime + stime) / 100.0)
}

fn proc_rss_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    Some(pages * 4096)
}

fn now_ms() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs_f64() * 1000.0).unwrap_or(0.0)
}

type StatsReply = (SqlResult<InspectReply>, Duration);

/// Sample once per interval until `stop` changes.
pub(crate) async fn run_sampler(st: Arc<AppState>, mut stop: watch::Receiver<bool>) {
    let mut tick = tokio::time::interval(INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut pending: Option<(Instant, oneshot::Receiver<StatsReply>)> = None;
    let mut topology: Option<[usize; 6]> = None;
    loop {
        tokio::select! {
            _ = stop.changed() => return,
            _ = tick.tick() => {}
        }
        if pending.is_none() {
            let (tx, rx) = oneshot::channel();
            let sent = Instant::now();
            let ok = st
                .engine
                .inspect(InspectQuery::Stats, move |r| {
                    let _ = tx.send((r, sent.elapsed()));
                })
                .is_ok();
            if ok {
                pending = Some((sent, rx));
            }
        }
        let (mut engine, mut latency, mut busy) = (None, None, None);
        if let Some((sent, rx)) = pending.as_mut() {
            match tokio::time::timeout(ENGINE_WAIT, rx).await {
                Ok(Ok((Ok(InspectReply::Stats(s)), took))) => {
                    engine = Some(s);
                    latency = Some(took.as_secs_f64() * 1000.0);
                    pending = None;
                }
                Ok(_) => pending = None,
                Err(_) => busy = Some(sent.elapsed().as_secs_f64() * 1000.0),
            }
        }
        if let Some(e) = &engine {
            let t = [e.catalog_version, e.nodes_created, e.arrangements_created, e.nodes, e.arrangements, e.subscriptions];
            if topology.is_some_and(|old| old != t) {
                st.metrics.announce("topology", serde_json::json!({ "catalog_version": e.catalog_version }).to_string());
            }
            topology = Some(t);
        }
        let shared = &st.engine.shared;
        let frontiers = shared.source_lsn.lock().clone();
        let gauges: Vec<(u64, Arc<crate::source::SourceGauges>)> =
            shared.source_gauges.lock().iter().map(|(k, v)| (*k, v.clone())).collect();
        let mut sources: Vec<SourceSample> = gauges
            .into_iter()
            .map(|(id, g)| SourceSample {
                id,
                frontier_lsn: frontiers.get(&id).copied().unwrap_or(0),
                upstream_lsn: g.wal_end.load(Ordering::Relaxed),
                commits: g.commits.load(Ordering::Relaxed),
                changes: g.changes.load(Ordering::Relaxed),
            })
            .collect();
        sources.sort_by_key(|s| s.id);
        let sample = Sample {
            t: now_ms(),
            durable_gsn: shared.durable(),
            processed_gsn: shared.processed(),
            rss_bytes: proc_rss_bytes(),
            cpu_percent: st.metrics.sample_cpu(),
            engine,
            engine_latency_ms: latency,
            engine_busy_ms: busy,
            sources,
        };
        st.metrics.push(sample);
    }
}

#[derive(Serialize)]
struct History<'a> {
    interval_ms: u64,
    samples: Vec<&'a Sample>,
}

/// `GET /api/metrics?since=<unix ms>`: buffered samples newer than `since`.
pub(crate) async fn history(State(st): State<Arc<AppState>>, Query(q): Query<HashMap<String, String>>) -> Response {
    let since = match param(&q, "since", 0f64) {
        Ok(s) => s,
        Err(e) => return e.into_response(),
    };
    let samples: Vec<Arc<Sample>> = st.metrics.ring.lock().iter().filter(|s| s.t > since).cloned().collect();
    json(&History { interval_ms: INTERVAL.as_millis() as u64, samples: samples.iter().map(|s| &**s).collect() })
}

/// `GET /api/events`: Server-Sent Events. `sample` carries each new sample,
/// `topology` announces catalog or dataflow changes.
pub(crate) async fn events(State(st): State<Arc<AppState>>) -> Response {
    let Ok(permit) = st.stream_slots.clone().try_acquire_owned() else {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "too many event streams", None);
    };
    let rx = st.metrics.tx.subscribe();
    let stream = futures_util::stream::unfold((rx, permit), |(mut rx, permit)| async move {
        loop {
            match rx.recv().await {
                Ok(p) => {
                    let ev = Event::default().event(p.event).data(&*p.data);
                    return Some((Ok::<Event, Infallible>(ev), (rx, permit)));
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    });
    let mut r = Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))).into_response();
    r.headers_mut().insert(axum::http::header::CACHE_CONTROL, axum::http::HeaderValue::from_static("no-store"));
    r
}
