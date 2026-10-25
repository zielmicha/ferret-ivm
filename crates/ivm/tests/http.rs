//! The web UI's HTTP server and JSON API, against a real engine (no
//! PostgreSQL needed). Requests go over raw TCP (or a Unix-domain socket)
//! so malformed ones can be sent too.

use ivm::coord::api::{ExecResult, SessionVars};
use ivm::coord::{self, EngineHandle};
use ivm::http::{HttpConfig, HttpServer};
use ivm::listen::LocalAddr;
use ivm::types::text::format_datum;
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

fn tmpdir(name: &str) -> PathBuf {
    let base = std::env::var("IVM_TEST_TMP").unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().into_owned());
    let d = PathBuf::from(base).join(format!("ivm-http-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

struct Fixture {
    running: Option<coord::Running>,
    server: Option<HttpServer>,
    h: EngineHandle,
    addr: SocketAddr,
    dir: PathBuf,
}

impl Fixture {
    fn new(name: &str, password: Option<&str>) -> Fixture {
        let dir = tmpdir(name);
        let cfg = coord::Config { data_dir: dir.clone(), sync: false, tick: None, ..Default::default() };
        let running = coord::start(cfg).expect("engine starts");
        let h = running.handle.clone();
        let server = ivm::http::start(HttpConfig { listen: vec!["127.0.0.1:0".into()], password: password.map(String::from) }, h.clone())
            .expect("http server starts");
        let LocalAddr::Tcp(addr) = server.local_addrs[0] else { unreachable!() };
        Fixture { running: Some(running), server: Some(server), h, addr, dir }
    }

    fn exec(&self, sql: &str) -> ExecResult {
        let stmt = ivm::sql::parse_statement(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        self.h.execute(stmt, vec![], SessionVars::default(), String::new()).unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    fn rows(&self, sql: &str) -> Vec<Vec<String>> {
        match self.exec(sql) {
            ExecResult::Query { rows, .. } => rows.iter().map(|r| r.iter().map(format_datum).collect()).collect(),
            other => panic!("{sql}: expected rows, got {other:?}"),
        }
    }

    fn get(&self, path: &str) -> Response {
        request(self.addr, "GET", path, &[], b"")
    }

    fn json(&self, path: &str) -> Value {
        let r = self.get(path);
        assert_eq!(r.status, 200, "GET {path}: {}", r.text());
        assert!(r.header("content-type").starts_with("application/json"), "GET {path}: {}", r.header("content-type"));
        serde_json::from_slice(&r.body).unwrap_or_else(|e| panic!("GET {path}: {e}"))
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(s) = self.server.take() {
            s.shutdown();
        }
        if let Some(r) = self.running.take() {
            r.shutdown();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

struct Response {
    status: u16,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

impl Response {
    fn header(&self, name: &str) -> &str {
        self.headers.get(name).map(|s| s.as_str()).unwrap_or("")
    }
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| panic!("not JSON ({e}): {}", self.text()))
    }
}

/// Send raw bytes and read the response until the server closes.
fn raw(addr: SocketAddr, bytes: &[u8]) -> Vec<u8> {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
    let _ = s.write_all(bytes);
    let mut out = Vec::new();
    let _ = s.read_to_end(&mut out);
    out
}

fn raw_unix(path: &Path, bytes: &[u8]) -> Vec<u8> {
    let mut s = UnixStream::connect(path).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
    let _ = s.write_all(bytes);
    let mut out = Vec::new();
    let _ = s.read_to_end(&mut out);
    out
}

fn parse(resp: &[u8]) -> Response {
    let end = resp.windows(4).position(|w| w == b"\r\n\r\n").unwrap_or_else(|| panic!("no header end in {:?}", String::from_utf8_lossy(resp)));
    let head = String::from_utf8_lossy(&resp[..end]).into_owned();
    let mut lines = head.split("\r\n");
    let status: u16 = lines.next().unwrap().split(' ').nth(1).unwrap().parse().unwrap();
    let headers: HashMap<String, String> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
        .collect();
    let mut body = resp[end + 4..].to_vec();
    if headers.get("transfer-encoding").is_some_and(|t| t.contains("chunked")) {
        let mut out = Vec::new();
        let mut rest = &body[..];
        while let Some(nl) = rest.windows(2).position(|w| w == b"\r\n") {
            let size = usize::from_str_radix(String::from_utf8_lossy(&rest[..nl]).trim(), 16).unwrap_or(0);
            if size == 0 {
                break;
            }
            out.extend_from_slice(&rest[nl + 2..nl + 2 + size]);
            rest = &rest[nl + 2 + size + 2..];
        }
        body = out;
    }
    Response { status, headers, body }
}

fn request(addr: SocketAddr, method: &str, path: &str, headers: &[(&str, &str)], body: &[u8]) -> Response {
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n");
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    if !body.is_empty() || method == "POST" {
        req.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    req.push_str("\r\n");
    let mut bytes = req.into_bytes();
    bytes.extend_from_slice(body);
    parse(&raw(addr, &bytes))
}

fn post_query(addr: SocketAddr, body: &str) -> Response {
    request(addr, "POST", "/api/query", &[("Content-Type", "application/json")], body.as_bytes())
}

fn basic(user: &str, pw: &str) -> String {
    use base64::Engine as _;
    format!("Basic {}", base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pw}")))
}

fn ids(v: &Value) -> BTreeSet<u64> {
    v.as_array().unwrap().iter().map(|x| x["id"].as_u64().unwrap()).collect()
}

/// The catalog's dependencies are the relations a view names, as the binder
/// resolves them (the rule DROP uses; relations read through another view
/// are that view's dependencies). They used to be found textually: a
/// column alias named like a table made an edge, a quoted name with
/// capitals made none.
#[test]
fn catalog_dependencies_are_the_relations_views_name() {
    let f = Fixture::new("deps", None);
    f.exec("CREATE TABLE orders (id int PRIMARY KEY, amount int)");
    f.exec("CREATE TABLE total (id int PRIMARY KEY)");
    f.exec("CREATE TABLE \"Mixed\" (id int PRIMARY KEY)");
    f.exec("CREATE VIEW v1 AS SELECT o.id AS total FROM orders o");
    f.exec("CREATE VIEW v2 AS SELECT * FROM \"Mixed\"");
    f.exec("CREATE VIEW v3 AS SELECT total FROM v1");
    f.exec("CREATE MATERIALIZED VIEW m AS SELECT count(*) AS n FROM v3 JOIN v2 ON v2.id = v3.total");
    f.exec("CREATE INDEX m_n ON m (n)");
    let cat = f.json("/api/catalog");
    let objects = cat["objects"].as_array().unwrap();
    let id_of: HashMap<&str, u64> = objects.iter().map(|o| (o["name"].as_str().unwrap(), o["id"].as_u64().unwrap())).collect();
    let name_of: HashMap<u64, &str> = id_of.iter().map(|(n, i)| (*i, *n)).collect();
    let names = |o: &Value, key: &str| -> BTreeSet<String> {
        o[key].as_array().unwrap().iter().map(|d| name_of[&d.as_u64().unwrap()].to_string()).collect()
    };
    let deps: HashMap<&str, (BTreeSet<String>, BTreeSet<String>)> =
        objects.iter().map(|o| (o["name"].as_str().unwrap(), (names(o, "depends_on"), names(o, "dependents")))).collect();
    let set = |v: &[&str]| -> BTreeSet<String> { v.iter().map(|s| s.to_string()).collect() };
    assert_eq!(deps["v1"].0, set(&["orders"]));
    assert_eq!(deps["v2"].0, set(&["Mixed"]));
    assert_eq!(deps["v3"].0, set(&["v1"]));
    assert_eq!(deps["m"].0, set(&["v2", "v3"]));
    assert_eq!(deps["m_n"].0, set(&["m"]));
    assert_eq!(deps["total"].1, set(&[]));
    assert_eq!(deps["orders"].1, set(&["v1"]));
    assert_eq!(deps["Mixed"].1, set(&["v2"]));
    assert_eq!(deps["m"].1, set(&["m_n"]));
    // the object page agrees
    let obj = f.json("/api/objects/v1");
    assert_eq!(names(&obj["object"], "depends_on"), set(&["orders"]));
}

#[test]
fn snapshots_match_the_engine_state() {
    let f = Fixture::new("state", None);
    f.exec("CREATE TABLE t (id int PRIMARY KEY, g int, v int)");
    f.exec("INSERT INTO t SELECT i, i % 7, i * 3 FROM generate_series(1, 100) i");
    f.exec("CREATE MATERIALIZED VIEW agg AS SELECT g, count(*) AS n, sum(v) AS s FROM t GROUP BY g");
    f.exec("CREATE MATERIALIZED VIEW pairs AS SELECT a.id, b.v FROM t a JOIN t b ON a.g = b.id WHERE a.id < 50");
    f.exec("CREATE MATERIALIZED VIEW top3 AS SELECT id, v FROM t ORDER BY v DESC LIMIT 3");
    f.exec("CREATE VIEW virt AS SELECT id FROM t WHERE v > 10");
    f.exec("CREATE INDEX t_by_g ON t (g)");

    // catalog: every object, with rows from the arrangements and dependencies
    let cat = f.json("/api/catalog");
    let objects = cat["objects"].as_array().unwrap();
    let by_name: HashMap<&str, &Value> = objects.iter().map(|o| (o["name"].as_str().unwrap(), o)).collect();
    for name in ["t", "agg", "pairs", "top3", "virt", "t_by_g"] {
        assert!(by_name.contains_key(name), "{name} missing from {cat}");
    }
    assert_eq!(by_name["agg"]["kind"], "materialized view");
    assert_eq!(by_name["virt"]["kind"], "view");
    assert_eq!(by_name["t"]["rows"], 100);
    assert_eq!(by_name["agg"]["rows"], 7);
    assert_eq!(by_name["top3"]["rows"], 3);
    assert_eq!(by_name["t_by_g"]["rows"], 100);
    let t_id = by_name["t"]["id"].as_u64().unwrap();
    assert!(by_name["agg"]["depends_on"].as_array().unwrap().iter().any(|d| d.as_u64() == Some(t_id)));
    let t_dependents: BTreeSet<u64> = by_name["t"]["dependents"].as_array().unwrap().iter().map(|d| d.as_u64().unwrap()).collect();
    for name in ["agg", "pairs", "top3", "virt", "t_by_g"] {
        assert!(t_dependents.contains(&by_name[name]["id"].as_u64().unwrap()), "{name} should depend on t");
    }
    assert_eq!(by_name["t"]["columns"].as_array().unwrap().len(), 3);
    assert_eq!(by_name["t"]["columns"][0]["key"], true);

    // dataflow: consistent ids, arrangements as SHOW ARRANGEMENTS reports them
    let df = f.json("/api/dataflow");
    let nodes = df["nodes"].as_array().unwrap();
    let node_ids = ids(&df["nodes"]);
    let arr_ids = ids(&df["arrangements"]);
    assert_eq!(nodes.len(), node_ids.len());
    for e in df["edges"].as_array().unwrap() {
        assert!(node_ids.contains(&e[0].as_u64().unwrap()) && node_ids.contains(&e[1].as_u64().unwrap()), "dangling edge {e}");
    }
    let edge_count: usize = nodes.iter().map(|n| n["inputs"].as_array().unwrap().len()).sum();
    assert_eq!(df["edges"].as_array().unwrap().len(), edge_count);
    let shown = f.rows("SHOW ARRANGEMENTS");
    assert_eq!(arr_ids.len(), shown.len());
    for row in &shown {
        let id: u64 = row[0].parse().unwrap();
        let a = df["arrangements"].as_array().unwrap().iter().find(|a| a["id"] == id).unwrap_or_else(|| panic!("arrangement {id} missing"));
        assert_eq!(a["node"].as_u64().unwrap().to_string(), row[1]);
        assert_eq!(a["records"].as_i64().unwrap().to_string(), row[4], "records of arrangement {id}");
        assert_eq!(a["refs"].as_u64().unwrap().to_string(), row[5]);
    }
    for n in nodes {
        for a in n["arrangements"].as_array().unwrap().iter().chain(n["reads"].as_array().unwrap()) {
            assert!(arr_ids.contains(&a.as_u64().unwrap()), "node {} names unknown arrangement {a}", n["id"]);
        }
    }
    // every catalog object with a dataflow points at a live node, and the
    // nodes of EXPLAIN DATAFLOW are all listed
    for o in objects {
        if let Some(n) = o["node"].as_u64() {
            assert!(node_ids.contains(&n), "{} -> node {n}", o["name"]);
        }
        if let Some(a) = o["arrangement"].as_u64() {
            assert!(arr_ids.contains(&a), "{} -> arrangement {a}", o["name"]);
        }
    }
    for line in f.rows("EXPLAIN DATAFLOW pairs") {
        let id: u64 = line[0].trim().trim_start_matches('#').split(' ').next().unwrap().parse().unwrap();
        assert!(node_ids.contains(&id), "EXPLAIN DATAFLOW node {id} missing");
    }
    // the table node is used by everything built on it; objects index the list
    let t_node = by_name["t"]["node"].as_u64().unwrap();
    let tn = nodes.iter().find(|n| n["id"] == t_node).unwrap();
    assert_eq!(tn["kind"], "Table");
    assert_eq!(tn["label"], "t");
    let dobjects = df["objects"].as_array().unwrap();
    let users: BTreeSet<&str> = tn["users"].as_array().unwrap().iter().map(|i| dobjects[i.as_u64().unwrap() as usize]["name"].as_str().unwrap()).collect();
    for name in ["t", "agg", "pairs", "top3", "t_by_g"] {
        assert!(users.contains(name), "t's node should be used by {name}: {users:?}");
    }
    let table_arr = by_name["t"]["arrangement"].as_u64().unwrap();
    let ta = df["arrangements"].as_array().unwrap().iter().find(|a| a["id"] == table_arr).unwrap();
    assert_eq!(ta["records"], 100);
    assert_eq!(ta["role"], "index");
    assert!(ta["resident_bytes"].as_u64().unwrap() > 0);

    // node details: operator parameters and a bounded, decoded sample
    let nd = f.json(&format!("/api/dataflow/node/{t_node}?sample=5"));
    assert_eq!(nd["node"]["id"], t_node);
    assert_eq!(nd["columns"], serde_json::json!(["id", "g", "v"]));
    assert!(nd["details"].as_array().unwrap().iter().any(|d| d["label"] == "primary key"));
    let primary = nd["arrangements"].as_array().unwrap().iter().find(|a| a["id"] == table_arr).unwrap();
    let sample = primary["sample"]["rows"].as_array().unwrap();
    assert_eq!(sample.len(), 5);
    assert_eq!(primary["sample"]["truncated"], true);
    assert_eq!(sample[0]["values"], serde_json::json!(["1", "1", "3"]));
    assert_eq!(sample[0]["weight"], 1);
    assert_eq!(primary["columns"][0]["name"], "id");
    assert_eq!(nd["users"].as_array().unwrap().len(), tn["users"].as_array().unwrap().len());

    // object details: plans, maintenance report, sample
    let od = f.json("/api/objects/agg?sample=100");
    assert!(od["plan"].as_array().unwrap().iter().any(|l| l.as_str().unwrap().contains("Aggregate")), "{}", od["plan"]);
    assert!(!od["dataflow"].as_array().unwrap().is_empty());
    assert_eq!(od["maintenance"][0]["source"], "t");
    assert_eq!(od["sample"]["rows"].as_array().unwrap().len(), 7);
    assert_eq!(od["sample"]["truncated"], false);
    assert_eq!(od["sample"]["columns"][1]["name"], "n");
    assert!(od["problems"].as_array().unwrap().is_empty(), "{}", od["problems"]);
    let ov = f.json("/api/objects/virt");
    assert!(ov["plan"].is_array() && ov["sample"].is_null() && ov["dataflow"].is_null());

    // history, overview, sessions
    let hist = f.json("/api/catalog/history");
    assert_eq!(hist["total"], 6);
    assert_eq!(hist["entries"][0]["action"], "CREATE INDEX");
    assert_eq!(f.json("/api/catalog/history?limit=2")["entries"].as_array().unwrap().len(), 2);
    let o = f.json("/api/overview");
    assert_eq!(o["counts"]["tables"], 1);
    assert_eq!(o["counts"]["materialized_views"], 3);
    assert_eq!(o["counts"]["views"], 1);
    assert_eq!(o["counts"]["indexes"], 1);
    assert_eq!(o["counts"]["nodes"].as_u64().unwrap() as usize, nodes.len());
    assert!(o["stats"]["processed_gsn"].as_u64().unwrap() >= 7);
    assert_eq!(o["stats"]["catalog_version"], 6);
    assert!(o["process"]["pid"].as_u64().unwrap() > 0);
    let s = f.json("/api/sessions");
    assert!(s["subscriptions"].as_array().unwrap().is_empty());
    assert_eq!(f.json("/api/sources")["sources"].as_array().unwrap().len(), 0);

    // counters move once per step, never per row
    let before = tn["counters"]["steps"].as_u64().unwrap();
    f.exec("INSERT INTO t VALUES (1000, 1, 1), (1001, 2, 2)");
    f.exec("DELETE FROM t WHERE id = 1");
    let df2 = f.json("/api/dataflow");
    let tn2 = df2["nodes"].as_array().unwrap().iter().find(|n| n["id"] == t_node).unwrap();
    assert_eq!(tn2["counters"]["steps"].as_u64().unwrap(), before + 2);
    assert_eq!(tn2["counters"]["in_rows"].as_u64().unwrap() - tn["counters"]["in_rows"].as_u64().unwrap(), 3);
    assert!(df2["totals"]["nanos"].as_u64().unwrap() > 0);

    // errors are reported per view
    assert!(f.json("/api/errors")["views"].as_array().unwrap().is_empty());
    f.exec("CREATE MATERIALIZED VIEW bad AS SELECT id, 10 / (g - 3) AS q FROM t");
    let errs = f.json("/api/errors");
    let v = &errs["views"][0];
    assert_eq!(v["name"], "bad");
    assert_eq!(v["code"], "22012");
    assert_eq!(v["count"], 14);
    assert_eq!(f.json("/api/objects/bad")["errors"][0]["count"], 14);
}

#[test]
fn auth_is_enforced_when_a_password_is_set() {
    let f = Fixture::new("auth", Some("s3cret"));
    for path in ["/", "/api/overview", "/js/main.js", "/api/nope"] {
        let r = f.get(path);
        assert_eq!(r.status, 401, "{path} without credentials");
        assert!(r.header("www-authenticate").starts_with("Basic "), "{path}");
        let r = request(f.addr, "GET", path, &[("Authorization", &basic("admin", "wrong"))], b"");
        assert_eq!(r.status, 401, "{path} with a wrong password");
    }
    // any user name, the right password
    for user in ["admin", "", "x"] {
        let r = request(f.addr, "GET", "/api/overview", &[("Authorization", &basic(user, "s3cret"))], b"");
        assert_eq!(r.status, 200, "user {user:?}");
    }
    let r = request(f.addr, "GET", "/", &[("Authorization", &basic("u", "s3cret"))], b"");
    assert_eq!(r.status, 200);
    let r = request(f.addr, "GET", "/", &[("Authorization", "Bearer s3cret")], b"");
    assert_eq!(r.status, 401);
    let r = request(f.addr, "POST", "/api/query", &[("Authorization", &basic("u", "nope")), ("Content-Type", "application/json")], br#"{"sql":"select 1"}"#);
    assert_eq!(r.status, 401);
    // with a password, any host name is fine (no rebinding guard needed)
    let r = parse(&raw(f.addr, format!("GET /api/overview HTTP/1.1\r\nHost: ivm.example\r\nAuthorization: {}\r\nConnection: close\r\n\r\n", basic("u", "s3cret")).as_bytes()));
    assert_eq!(r.status, 200);
}

#[test]
fn static_files_are_served_with_content_types() {
    let f = Fixture::new("static", None);
    for (path, ctype) in [
        ("/", "text/html"),
        ("/index.html", "text/html"),
        ("/js/main.js", "text/javascript"),
        ("/js/views/dataflow.js", "text/javascript"),
        ("/css/app.css", "text/css"),
        ("/vendor/dagre.min.js", "text/javascript"),
        ("/vendor/uPlot.iife.min.js", "text/javascript"),
        ("/vendor/uPlot.min.css", "text/css"),
        ("/vendor/dagre.LICENSE.txt", "text/plain"),
        ("/favicon.svg", "image/svg+xml"),
    ] {
        let r = f.get(path);
        assert_eq!(r.status, 200, "{path}");
        assert!(r.header("content-type").starts_with(ctype), "{path}: {}", r.header("content-type"));
        assert!(!r.body.is_empty(), "{path}");
        assert_eq!(r.header("x-content-type-options"), "nosniff");
        assert!(r.header("content-security-policy").contains("default-src 'self'"));
        // revalidation
        let etag = r.header("etag").to_string();
        assert!(!etag.is_empty(), "{path}");
        let r2 = request(f.addr, "GET", path, &[("If-None-Match", &etag)], b"");
        assert_eq!(r2.status, 304, "{path}");
        assert!(r2.body.is_empty());
    }
    let index = f.get("/").text();
    assert!(index.contains("js/main.js") && index.contains("vendor/dagre.min.js"));
    // the page loads nothing from elsewhere
    assert!(!index.contains("http://") && !index.contains("https://"), "external reference in index.html");
    assert_eq!(f.get("/nope.js").status, 404);
    assert_eq!(f.get("/js/../../Cargo.toml").status, 404);
    // compression for clients that ask for it
    let r = request(f.addr, "GET", "/vendor/dagre.min.js", &[("Accept-Encoding", "gzip")], b"");
    assert_eq!(r.status, 200);
    assert_eq!(r.header("content-encoding"), "gzip");
}

#[test]
fn malformed_requests_get_errors_not_panics() {
    let f = Fixture::new("malformed", None);
    let status = |bytes: &[u8]| -> Option<u16> {
        let out = raw(f.addr, bytes);
        if out.is_empty() {
            return None;
        }
        Some(parse(&out).status)
    };
    assert_eq!(status(b"GARBAGE\r\n\r\n"), Some(400));
    assert_eq!(status(b"\x16\x03\x01\x02\x00\x01\x00\x01\xfc\x03\x03"), Some(400));
    assert!(status(b"GET / HTTP/9.9\r\nHost: 127.0.0.1\r\n\r\n").unwrap_or(400) >= 400);
    let big_header = format!("GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nX-Big: {}\r\n\r\n", "a".repeat(300_000));
    assert!(matches!(status(big_header.as_bytes()), Some(431) | Some(400) | None));
    let big_body = format!(
        "POST /api/query HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: 200000\r\nConnection: close\r\n\r\n{}",
        "x".repeat(200_000)
    );
    assert_eq!(status(big_body.as_bytes()), Some(413));
    // API errors are JSON with a status that says what went wrong
    let cases: &[(&str, &str, u16)] = &[
        ("GET", "/api/nope", 404),
        ("GET", "/api/dataflow/node/abc", 400),
        ("GET", "/api/dataflow/node/99999", 404),
        ("GET", "/api/dataflow/node/-1", 400),
        ("GET", "/api/dataflow/node/1?sample=lots", 400),
        ("GET", "/api/objects/nope", 404),
        ("GET", "/api/catalog/history?limit=-5", 400),
        ("GET", "/api/metrics?since=yesterday", 400),
        ("DELETE", "/api/overview", 405),
        ("PUT", "/api/query", 405),
    ];
    for (method, path, want) in cases {
        let r = request(f.addr, method, path, &[], b"");
        assert_eq!(r.status, *want, "{method} {path}: {}", r.text());
        if *want != 405 {
            assert!(r.json()["error"].is_string(), "{method} {path}");
        }
    }
    assert_eq!(request(f.addr, "POST", "/api/query", &[("Content-Type", "text/plain")], b"select 1").status, 415);
    assert_eq!(post_query(f.addr, "{not json").status, 400);
    assert_eq!(post_query(f.addr, r#"{"sql": 5}"#).status, 400);
    assert_eq!(post_query(f.addr, r#"{"sql":"select 1","mode":"sideways"}"#).status, 400);
    // a rebound host name is refused without a password
    let r = parse(&raw(f.addr, b"GET /api/overview HTTP/1.1\r\nHost: attacker.example\r\nConnection: close\r\n\r\n"));
    assert_eq!(r.status, 403);
    let r = parse(&raw(f.addr, b"GET /api/overview HTTP/1.1\r\nHost: localhost:6876\r\nConnection: close\r\n\r\n"));
    assert_eq!(r.status, 200);
    // slow clients are cut off, then the server still answers
    let mut slow = TcpStream::connect(f.addr).unwrap();
    slow.write_all(b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n").unwrap();
    assert_eq!(f.get("/api/overview").status, 200);
    drop(slow);
    assert_eq!(f.get("/api/overview").status, 200);
}

#[test]
fn sql_console_runs_reads_only() {
    let f = Fixture::new("console", None);
    f.exec("CREATE TABLE t (id int PRIMARY KEY, name text)");
    f.exec("INSERT INTO t SELECT i, 'n' || i FROM generate_series(1, 50) i");
    let r = post_query(f.addr, r#"{"sql":"SELECT id, name FROM t WHERE id <= 3 ORDER BY id"}"#);
    assert_eq!(r.status, 200, "{}", r.text());
    let v = r.json();
    assert_eq!(v["columns"][0]["name"], "id");
    assert_eq!(v["columns"][0]["type"], "integer");
    assert_eq!(v["rows"], serde_json::json!([["1", "n1"], ["2", "n2"], ["3", "n3"]]));
    assert_eq!(v["truncated"], false);
    // the row cap
    let v = post_query(f.addr, r#"{"sql":"SELECT * FROM t","limit":10,"mode":"latest"}"#).json();
    assert_eq!(v["rows"].as_array().unwrap().len(), 10);
    assert_eq!(v["row_count"], 50);
    assert_eq!(v["truncated"], true);
    // NULLs, SHOW and EXPLAIN
    let v = post_query(f.addr, r#"{"sql":"SELECT NULL::int AS x"}"#).json();
    assert_eq!(v["rows"], serde_json::json!([[null]]));
    assert_eq!(post_query(f.addr, r#"{"sql":"SHOW TABLES"}"#).json()["rows"][0][0], "t");
    assert!(!post_query(f.addr, r#"{"sql":"EXPLAIN SELECT * FROM t"}"#).json()["rows"].as_array().unwrap().is_empty());
    // writes, DDL and several statements are refused
    for sql in ["INSERT INTO t VALUES (99, 'x')", "DELETE FROM t", "CREATE TABLE u (a int)", "DROP TABLE t", "CHECKPOINT", "SUBSCRIBE TO t"] {
        let r = post_query(f.addr, &serde_json::json!({ "sql": sql }).to_string());
        assert_eq!(r.status, 403, "{sql}: {}", r.text());
    }
    assert_eq!(post_query(f.addr, r#"{"sql":"select 1; select 2"}"#).status, 400);
    assert_eq!(post_query(f.addr, r#"{"sql":"   "}"#).status, 400);
    assert_eq!(f.rows("SELECT count(*) FROM t"), vec![vec!["50".to_string()]]);
    // SQL errors carry their SQLSTATE
    let r = post_query(f.addr, r#"{"sql":"SELECT * FROM missing"}"#);
    assert_eq!(r.status, 400);
    assert_eq!(r.json()["code"], "42P01");
    let r = post_query(f.addr, r#"{"sql":"SELECT 1/0"}"#);
    assert_eq!(r.status, 400);
    assert_eq!(r.json()["code"], "22012");
}

#[test]
fn metrics_and_event_stream() {
    let f = Fixture::new("events", None);
    f.exec("CREATE TABLE t (id int PRIMARY KEY)");
    // the sampler takes one sample per second
    let deadline = Instant::now() + Duration::from_secs(10);
    let samples = loop {
        let m = f.json("/api/metrics");
        let s = m["samples"].as_array().unwrap().clone();
        if s.len() >= 2 && s.iter().any(|x| x["engine"].is_object()) {
            break s;
        }
        assert!(Instant::now() < deadline, "no samples: {m}");
        std::thread::sleep(Duration::from_millis(200));
    };
    let last = samples.last().unwrap();
    assert!(last["processed_gsn"].as_u64().unwrap() >= 1);
    assert!(samples.iter().any(|s| s["engine"]["catalog_version"] == 1));
    let t = last["t"].as_f64().unwrap();
    let newer = f.json(&format!("/api/metrics?since={t}"));
    assert!(newer["samples"].as_array().unwrap().iter().all(|s| s["t"].as_f64().unwrap() > t));

    // Server-Sent Events: a sample arrives within a couple of seconds, and a
    // DDL statement announces a topology change
    let mut s = TcpStream::connect(f.addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    s.write_all(b"GET /api/events HTTP/1.1\r\nHost: 127.0.0.1\r\nAccept: text/event-stream\r\n\r\n").unwrap();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let mut ddl_done = false;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let n = s.read(&mut chunk).unwrap();
        assert!(n > 0, "event stream closed");
        buf.extend_from_slice(&chunk[..n]);
        let text = String::from_utf8_lossy(&buf);
        assert!(text.starts_with("HTTP/1.1 200"), "{text}");
        assert!(text.to_ascii_lowercase().contains("content-type: text/event-stream"));
        if text.contains("event: sample") && !ddl_done {
            f.exec("CREATE MATERIALIZED VIEW v AS SELECT count(*) FROM t");
            ddl_done = true;
        }
        if text.contains("event: topology") {
            break;
        }
        assert!(Instant::now() < deadline, "no topology event: {text}");
    }
}

/// The UI on a Unix-domain socket, next to a TCP listener; the socket file
/// is removed at shutdown.
#[test]
fn unix_domain_sockets() {
    let dir = tmpdir("unix");
    let cfg = coord::Config { data_dir: dir.clone(), sync: false, tick: None, ..Default::default() };
    let running = coord::start(cfg).expect("engine starts");
    let path = dir.join("ui.sock");
    let listen = vec![path.to_str().unwrap().to_string(), "127.0.0.1:0".into()];
    let server = ivm::http::start(HttpConfig { listen, password: None }, running.handle.clone()).expect("http server starts");
    assert_eq!(server.local_addrs[0], LocalAddr::Unix(path.clone()));
    let LocalAddr::Tcp(addr) = server.local_addrs[1] else { panic!("{:?}", server.local_addrs) };
    let get = |host: &str| parse(&raw_unix(&path, format!("GET /api/overview HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n").as_bytes()));
    let r = get("localhost");
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["counts"]["tables"], 0, "{}", r.text());
    // the DNS-rebinding guard holds here too (a reverse proxy on a loopback
    // port may forward to the socket)
    assert_eq!(get("evil.example").status, 403);
    assert_eq!(request(addr, "GET", "/api/overview", &[], b"").status, 200);
    server.shutdown();
    assert!(!path.exists(), "the socket file is removed");
    running.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
