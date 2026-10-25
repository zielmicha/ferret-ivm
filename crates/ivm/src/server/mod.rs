//! PostgreSQL wire protocol server: simple and extended query protocols,
//! streaming SUBSCRIBE (as rows or COPY), cancellation.

use crate::coord::api::{ExecResult, ResultColumn, SessionVars, SubEvent};
use crate::coord::EngineHandle;
use crate::error::{codes, Result, SqlError};
use crate::sql::parser::{parse_statement, split_statements, AsOf, ShowKind, Statement, SubscribeTarget};
use crate::sql::ParamSlot;
use crate::types::text::{format_datum, parse_datum, recv_datum, send_datum};
use crate::types::{Datum, Row, SqlType};
use parking_lot::Mutex;
use pgproto::server::{read_frontend_message, read_startup_packet, BackendWriter, FieldDescription, FrontendMessage, StartupPacket};
use std::collections::HashMap;
use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Default)]
pub struct ServerConfig {
    pub listen: String,
    /// If set, clients must authenticate with this password (cleartext).
    pub password: Option<String>,
}

type CancelMap = Arc<Mutex<HashMap<u32, (u32, Arc<AtomicBool>)>>>;

/// Stack size of session threads: syntax trees of long operator chains (up
/// to sql::parser::MAX_OPERATOR_CHAIN deep) are dropped recursively.
/// Reserved, not committed, memory.
const SESSION_STACK: usize = 16 << 20;

/// Accept connections forever.
pub fn serve(cfg: ServerConfig, engine: EngineHandle) -> Result<()> {
    let listener = TcpListener::bind(&cfg.listen)?;
    log::info!("listening on {}", cfg.listen);
    serve_listener(listener, cfg, engine)
}

pub fn serve_listener(listener: TcpListener, cfg: ServerConfig, engine: EngineHandle) -> Result<()> {
    let cancels: CancelMap = Arc::new(Mutex::new(HashMap::new()));
    let next_pid = Arc::new(AtomicU32::new(1));
    for conn in listener.incoming() {
        let Ok(stream) = conn else { continue };
        if engine.shared.shutdown.load(Ordering::Acquire) {
            break;
        }
        let engine = engine.clone();
        let cfg = cfg.clone();
        let cancels = cancels.clone();
        let pid = next_pid.fetch_add(1, Ordering::Relaxed);
        std::thread::Builder::new()
            .name(format!("ivm-session-{pid}"))
            // statements (and their syntax trees) may nest deeply
            .stack_size(SESSION_STACK)
            .spawn(move || {
                let _ = stream.set_nodelay(true);
                if let Err(e) = Session::run(stream, cfg, engine, cancels.clone(), pid) {
                    log::debug!("session {pid} ended: {e}");
                }
                cancels.lock().remove(&pid);
            })
            .ok();
    }
    Ok(())
}

/// Prepared statements and portals keep the statement's text and parse it
/// again when needed: syntax trees can be deep, and cloning one recurses
/// once per level (parsing does not).
struct Prepared {
    text: String,
    param_types: Vec<SqlType>,
    /// Number of parameters a Bind must supply.
    nparams: usize,
}

struct Portal {
    text: String,
    params: Vec<ParamSlot>,
    result_formats: Vec<i16>,
    /// Remaining rows of a partially fetched (or exhausted) result.
    pending: Option<(Vec<ResultColumn>, std::vec::IntoIter<Row>)>,
}

struct Session {
    engine: EngineHandle,
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
    out: BackendWriter,
    vars: SessionVars,
    prepared: HashMap<String, Prepared>,
    portals: HashMap<String, Portal>,
    in_txn: bool,
    cancel: Arc<AtomicBool>,
    settings: HashMap<String, String>,
    /// The statement being run is COPY (SUBSCRIBE ...) TO STDOUT.
    pending_copy: bool,
}

fn field(c: &ResultColumn, format: i16) -> FieldDescription {
    // untyped literals in a result are text (PostgreSQL resolves them so)
    let ty = if c.ty == SqlType::Unknown { &SqlType::Text } else { &c.ty };
    FieldDescription {
        name: c.name.clone(),
        table_oid: 0,
        column_attr: 0,
        type_oid: ty.oid(),
        type_size: ty.type_size(),
        type_modifier: c.typmod,
        format,
    }
}

fn encode_value(d: &Datum, ty: &SqlType, binary: bool) -> Option<Vec<u8>> {
    if d.is_null() {
        return None;
    }
    if binary { send_datum(d, ty) } else { Some(format_datum(d).into_bytes()) }
}

fn copy_escape(s: &str) -> String {
    let mut o = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => o.push_str("\\\\"),
            '\t' => o.push_str("\\t"),
            '\n' => o.push_str("\\n"),
            '\r' => o.push_str("\\r"),
            c => o.push(c),
        }
    }
    o
}

impl Session {
    fn run(stream: TcpStream, cfg: ServerConfig, engine: EngineHandle, cancels: CancelMap, pid: u32) -> Result<()> {
        let mut reader = BufReader::new(stream.try_clone()?);
        let mut writer = BufWriter::new(stream.try_clone()?);
        let mut out = BackendWriter::new();
        // startup (SSL / GSS negotiation refused)
        let params = loop {
            match read_startup_packet(&mut reader)? {
                StartupPacket::SslRequest | StartupPacket::GssEncRequest => {
                    out.raw_byte(b'N');
                    out.flush_to(&mut writer)?;
                    writer.flush()?;
                }
                StartupPacket::Cancel { pid, secret } => {
                    if let Some((s, flag)) = cancels.lock().get(&pid)
                        && *s == secret
                    {
                        flag.store(true, Ordering::Release);
                    }
                    return Ok(());
                }
                StartupPacket::Startup { params, .. } => break params,
            }
        };
        if let Some(pw) = &cfg.password {
            out.auth_cleartext_password();
            out.flush_to(&mut writer)?;
            writer.flush()?;
            match read_frontend_message(&mut reader)? {
                Some(FrontendMessage::Password(p)) => {
                    let given = String::from_utf8_lossy(&p).trim_end_matches('\0').to_string();
                    if &given != pw {
                        let user = params.iter().find(|(k, _)| k == "user").map(|(_, v)| v.clone()).unwrap_or_default();
                        out.error_response("FATAL", "28P01", &format!("password authentication failed for user \"{user}\""), None, None, None);
                        out.flush_to(&mut writer)?;
                        writer.flush()?;
                        return Ok(());
                    }
                }
                _ => return Ok(()),
            }
        }
        out.auth_ok();
        let secret: u32 = (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u32) ^ pid.wrapping_mul(2654435761);
        let cancel = Arc::new(AtomicBool::new(false));
        cancels.lock().insert(pid, (secret, cancel.clone()));
        let app = params.iter().find(|(k, _)| k == "application_name").map(|(_, v)| v.clone()).unwrap_or_default();
        for (k, v) in [
            ("server_version", "17.0"),
            ("server_encoding", "UTF8"),
            ("client_encoding", "UTF8"),
            ("DateStyle", "ISO, MDY"),
            ("IntervalStyle", "postgres"),
            ("TimeZone", "UTC"),
            ("integer_datetimes", "on"),
            ("standard_conforming_strings", "on"),
            ("is_superuser", "on"),
            ("session_authorization", "ivm"),
            ("application_name", app.as_str()),
        ] {
            out.parameter_status(k, v);
        }
        out.backend_key_data(pid, secret);
        out.ready_for_query(b'I');
        out.flush_to(&mut writer)?;
        writer.flush()?;
        let mut s = Session {
            engine,
            reader,
            writer,
            out,
            vars: SessionVars { read_mode: "fresh".into() },
            prepared: HashMap::new(),
            portals: HashMap::new(),
            in_txn: false,
            cancel,
            settings: HashMap::new(),
            pending_copy: false,
        };
        s.settings.insert("application_name".into(), app);
        // read-only settings clients SHOW (besides those reported above)
        for (k, v) in [("server_version_num", "170000"), ("integer_datetimes", "on"), ("max_identifier_length", "63")] {
            s.settings.insert(k.into(), v.into());
        }
        s.main_loop()
    }

    fn flush(&mut self) -> Result<()> {
        self.out.flush_to(&mut self.writer)?;
        self.writer.flush()?;
        Ok(())
    }

    fn error(&mut self, e: &SqlError) {
        self.out.error_response("ERROR", &e.code, &e.message, e.detail.as_deref(), e.hint.as_deref(), None);
    }

    fn txn_status(&self) -> u8 {
        if self.in_txn { b'T' } else { b'I' }
    }

    fn main_loop(&mut self) -> Result<()> {
        let mut skip_until_sync = false;
        loop {
            let Some(msg) = read_frontend_message(&mut self.reader)? else { return Ok(()) };
            if skip_until_sync && !matches!(msg, FrontendMessage::Sync) {
                continue;
            }
            match msg {
                FrontendMessage::Query(q) => {
                    self.cancel.store(false, Ordering::Release);
                    self.simple_query(&q)?;
                    let st = self.txn_status();
                    self.out.ready_for_query(st);
                    self.flush()?;
                }
                FrontendMessage::Parse { name, query, param_types } => match self.parse(&name, &query, &param_types) {
                    Ok(()) => self.out.parse_complete(),
                    Err(e) => {
                        self.error(&e);
                        skip_until_sync = true;
                    }
                },
                FrontendMessage::Bind { portal, statement, param_formats, params, result_formats } => {
                    match self.bind(&portal, &statement, &param_formats, &params, result_formats) {
                        Ok(()) => self.out.bind_complete(),
                        Err(e) => {
                            self.error(&e);
                            skip_until_sync = true;
                        }
                    }
                }
                FrontendMessage::Describe { kind, name } => {
                    if let Err(e) = self.describe(kind, &name) {
                        self.error(&e);
                        skip_until_sync = true;
                    }
                }
                FrontendMessage::Execute { portal, max_rows } => {
                    self.cancel.store(false, Ordering::Release);
                    if let Err(e) = self.execute_portal(&portal, max_rows) {
                        self.error(&e);
                        skip_until_sync = true;
                    }
                }
                FrontendMessage::Close { kind, name } => {
                    if kind == b'S' {
                        self.prepared.remove(&name);
                    } else {
                        self.portals.remove(&name);
                    }
                    self.out.close_complete();
                }
                FrontendMessage::Sync => {
                    skip_until_sync = false;
                    // portals end with the transaction (outside a transaction
                    // block, with the implicit one Sync closes)
                    if self.in_txn {
                        self.portals.retain(|k, _| !k.is_empty());
                    } else {
                        self.portals.clear();
                    }
                    let st = self.txn_status();
                    self.out.ready_for_query(st);
                    self.flush()?;
                }
                FrontendMessage::Flush => self.flush()?,
                FrontendMessage::Terminate => return Ok(()),
                FrontendMessage::CopyData(_) | FrontendMessage::CopyDone | FrontendMessage::CopyFail(_) => {}
                FrontendMessage::Password(_) => {}
                FrontendMessage::FunctionCall => {
                    // like a simple query: the error, then ReadyForQuery
                    self.error(&SqlError::unsupported("function call protocol is not supported"));
                    let st = self.txn_status();
                    self.out.ready_for_query(st);
                    self.flush()?;
                }
            }
        }
    }

    /// Handle statements the session answers itself (SET, session SHOW).
    fn session_statement(&mut self, stmt: &Statement) -> Option<Result<ExecResult>> {
        match stmt {
            Statement::Sql(s) => match &**s {
                sqlparser::ast::Statement::Set(set) => Some(self.handle_set(set)),
                sqlparser::ast::Statement::StartTransaction { .. } => {
                    self.in_txn = true;
                    Some(Ok(ExecResult::Command { tag: "BEGIN".into() }))
                }
                sqlparser::ast::Statement::Commit { .. } => {
                    self.in_txn = false;
                    self.portals.clear();
                    Some(Ok(ExecResult::Command { tag: "COMMIT".into() }))
                }
                sqlparser::ast::Statement::Rollback { .. } => {
                    self.in_txn = false;
                    self.portals.clear();
                    Some(Ok(ExecResult::Command { tag: "ROLLBACK".into() }))
                }
                // settings are fixed or kept per session: nothing to reset
                sqlparser::ast::Statement::Reset(_) => Some(Ok(ExecResult::Command { tag: "RESET".into() })),
                sqlparser::ast::Statement::ShowVariable { variable } => {
                    let name = variable.iter().map(|i| i.value.to_lowercase()).collect::<Vec<_>>().join("_");
                    self.settings.get(&name).map(|v| {
                        Ok(ExecResult::Query {
                            columns: vec![ResultColumn { name: name.clone(), ty: SqlType::Text, typmod: -1 }],
                            rows: vec![vec![Datum::text(v)]],
                        })
                    })
                }
                _ => None,
            },
            Statement::Show(ShowKind::Variable(v)) => self.settings.get(&v.replace(' ', "_")).map(|val| {
                Ok(ExecResult::Query {
                    columns: vec![ResultColumn { name: v.clone(), ty: SqlType::Text, typmod: -1 }],
                    rows: vec![vec![Datum::text(val)]],
                })
            }),
            _ => None,
        }
    }

    fn handle_set(&mut self, set: &sqlparser::ast::Set) -> Result<ExecResult> {
        use sqlparser::ast::Set as S;
        let (name, value) = match set {
            S::SingleAssignment { variable, values, .. } => {
                let v = values.iter().map(|e| e.to_string().trim_matches('\'').to_string()).collect::<Vec<_>>().join(",");
                (variable.to_string().to_lowercase(), v)
            }
            S::SetTimeZone { value, .. } => ("timezone".to_string(), value.to_string().trim_matches('\'').to_string()),
            S::SetNames { charset_name, .. } => ("client_encoding".to_string(), charset_name.value.clone()),
            S::SetTransaction { .. } => return Ok(ExecResult::Command { tag: "SET".into() }),
            _ => return Ok(ExecResult::Command { tag: "SET".into() }),
        };
        match name.as_str() {
            "read_mode" | "ivm.read_mode" => {
                let v = value.to_lowercase();
                if v != "fresh" && v != "latest" {
                    return Err(SqlError::new(codes::INVALID_PARAMETER_VALUE, format!("invalid value for read_mode: \"{value}\"")));
                }
                self.vars.read_mode = v;
            }
            "timezone" | "time zone" => {
                let v = value.to_lowercase();
                if !matches!(v.as_str(), "utc" | "'utc'" | "gmt" | "z" | "+00" | "0" | "etc/utc" | "default" | "local") {
                    return Err(SqlError::unsupported(format!("only the UTC time zone is supported (got \"{value}\")")));
                }
            }
            "client_encoding" | "names" => {
                let v = value.to_uppercase().replace('-', "");
                if v != "UTF8" && v != "UNICODE" && v != "DEFAULT" {
                    return Err(SqlError::unsupported(format!("client_encoding \"{value}\" is not supported; use UTF8")));
                }
            }
            "datestyle" => {
                if !value.to_uppercase().contains("ISO") {
                    return Err(SqlError::unsupported("only DateStyle ISO is supported"));
                }
            }
            _ => {}
        }
        if name == "application_name" {
            self.out.parameter_status("application_name", &value);
        }
        self.settings.insert(name.replace(' ', "_"), value);
        Ok(ExecResult::Command { tag: "SET".into() })
    }

    /// Run a statement: session-level first, then the engine.
    fn run_statement(&mut self, stmt: Statement, params: Vec<ParamSlot>) -> Result<ExecResult> {
        if let Some(r) = self.session_statement(&stmt) {
            return r;
        }
        let extra = match &stmt {
            Statement::CreateSource { conninfo, publication, tables, .. } => {
                let t = crate::source::discover(conninfo, publication, tables.as_deref())?;
                serde_json::to_string(&t).map_err(|e| SqlError::internal(e.to_string()))?
            }
            Statement::AlterSourceRefresh { name } => {
                // the session needs the source definition: ask the engine
                let def = self.source_def(name)?;
                let t = crate::source::discover(&def.0, &def.1, def.2.as_deref())?;
                serde_json::to_string(&t).map_err(|e| SqlError::internal(e.to_string()))?
            }
            _ => String::new(),
        };
        self.engine.execute(stmt, params, self.vars.clone(), extra)
    }

    fn source_def(&mut self, name: &str) -> Result<(String, String, Option<Vec<String>>)> {
        // read through SHOW SOURCES + catalog via a query against the engine
        let q = parse_statement(&format!("SHOW CREATE VIEW {name}"))?;
        let _ = q;
        let r = self.engine.execute(
            Statement::Show(ShowKind::CreateView(name.to_string())),
            vec![],
            self.vars.clone(),
            String::new(),
        )?;
        let ExecResult::Query { rows, .. } = r else { return Err(SqlError::internal("unexpected result")) };
        let sql = rows.first().and_then(|r| r.get(1)).and_then(|d| d.as_str()).unwrap_or("").to_string();
        match parse_statement(&sql)? {
            Statement::CreateSource { conninfo, publication, tables, .. } => Ok((conninfo, publication, tables)),
            _ => Err(SqlError::new(codes::UNDEFINED_OBJECT, format!("source \"{name}\" does not exist"))),
        }
    }

    fn simple_query(&mut self, q: &str) -> Result<()> {
        let stmts = split_statements(q);
        if stmts.is_empty() {
            self.out.empty_query_response();
            return Ok(());
        }
        for text in stmts {
            let stmt = match parse_statement(&text) {
                Ok(s) => s,
                Err(e) => {
                    self.error(&e);
                    return Ok(());
                }
            };
            if matches!(stmt, Statement::Empty) {
                self.out.empty_query_response();
                continue;
            }
            self.pending_copy = matches!(stmt, Statement::Subscribe { copy: true, .. });
            match self.run_statement(stmt, vec![]) {
                Ok(res) => {
                    if !self.send_result(res, &[], 0, true)? {
                        return Ok(());
                    }
                }
                Err(e) => {
                    self.error(&e);
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Send a result. Returns false if the statement ended with an error
    /// that aborts the rest of a multi-statement query.
    fn send_result(&mut self, res: ExecResult, formats: &[i16], _max_rows: i32, describe: bool) -> Result<bool> {
        match res {
            ExecResult::Command { tag } => {
                if tag.is_empty() {
                    self.out.empty_query_response();
                } else {
                    self.out.command_complete(&tag);
                }
                Ok(true)
            }
            ExecResult::Query { columns, rows } => {
                let fmt = |i: usize| -> i16 {
                    match formats.len() {
                        0 => 0,
                        1 => formats[0],
                        _ => formats.get(i).copied().unwrap_or(0),
                    }
                };
                if describe {
                    let fields: Vec<FieldDescription> = columns.iter().enumerate().map(|(i, c)| field(c, fmt(i))).collect();
                    self.out.row_description(&fields);
                }
                let n = rows.len();
                for r in rows {
                    let vals: Vec<Option<Vec<u8>>> = r.iter().enumerate().map(|(i, d)| encode_value(d, &columns[i].ty, fmt(i) == 1)).collect();
                    let refs: Vec<Option<&[u8]>> = vals.iter().map(|v| v.as_deref()).collect();
                    self.out.data_row(&refs);
                    if self.out.len() > 1 << 20 {
                        self.flush()?;
                    }
                }
                self.out.command_complete(&format!("SELECT {n}"));
                Ok(true)
            }
            ExecResult::Subscribe { columns, rx, cancel, progress } => {
                let r = self.stream_subscription(&columns, rx, &cancel, progress, describe);
                cancel.store(true, Ordering::Release);
                match r {
                    Ok(()) => Ok(true),
                    Err(e) if e.code == codes::IO_ERROR => Err(e),
                    Err(e) => {
                        self.error(&e);
                        Ok(false)
                    }
                }
            }
        }
    }

    fn stream_subscription(
        &mut self,
        columns: &[ResultColumn],
        rx: crossbeam_channel::Receiver<SubEvent>,
        _cancel: &Arc<AtomicBool>,
        progress: bool,
        describe: bool,
    ) -> Result<()> {
        let copy = self.pending_copy;
        let mut cols = vec![ResultColumn { name: "gsn".into(), ty: SqlType::Int8, typmod: -1 }];
        if progress {
            cols.push(ResultColumn { name: "progressed".into(), ty: SqlType::Bool, typmod: -1 });
        }
        cols.push(ResultColumn { name: "diff".into(), ty: SqlType::Int8, typmod: -1 });
        cols.extend(columns.iter().cloned());
        if copy {
            self.out.copy_out_response(0, &vec![0i16; cols.len()]);
        } else if describe {
            let fields: Vec<FieldDescription> = cols.iter().map(|c| field(c, 0)).collect();
            self.out.row_description(&fields);
        }
        self.flush()?;
        let ncols = columns.len();
        loop {
            if self.cancel.load(Ordering::Acquire) {
                if copy {
                    self.out.copy_done();
                }
                return Err(SqlError::new(codes::QUERY_CANCELED, "canceling statement due to user request"));
            }
            let ev = match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(e) => e,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    // probe the connection so disconnected clients end the subscription
                    if self.writer.get_ref().peer_addr().is_err() {
                        return Err(SqlError::new(codes::IO_ERROR, "client disconnected"));
                    }
                    continue;
                }
                Err(_) => {
                    if copy {
                        self.out.copy_done();
                    }
                    self.out.command_complete("SUBSCRIBE");
                    return Ok(());
                }
            };
            match ev {
                SubEvent::Data { gsn, rows } => {
                    for (r, diff) in rows {
                        let mut vals: Vec<Option<String>> = vec![Some(gsn.to_string())];
                        if progress {
                            vals.push(Some("f".into()));
                        }
                        vals.push(Some(diff.to_string()));
                        vals.extend(r.iter().map(|d| if d.is_null() { None } else { Some(format_datum(d)) }));
                        self.emit_sub_row(copy, &vals);
                    }
                }
                SubEvent::Progress { gsn } => {
                    if progress {
                        let mut vals: Vec<Option<String>> = vec![Some(gsn.to_string()), Some("t".into()), None];
                        vals.extend(std::iter::repeat_n(None, ncols));
                        self.emit_sub_row(copy, &vals);
                    }
                }
                SubEvent::Error(e) => {
                    if copy {
                        self.out.copy_done();
                    }
                    return Err(e);
                }
            }
            if let Err(e) = self.flush() {
                return Err(SqlError::new(codes::IO_ERROR, format!("client disconnected: {e}")));
            }
        }
    }

    fn emit_sub_row(&mut self, copy: bool, vals: &[Option<String>]) {
        if copy {
            let line = vals.iter().map(|v| match v {
                None => "\\N".to_string(),
                Some(s) => copy_escape(s),
            });
            let mut l = line.collect::<Vec<_>>().join("\t");
            l.push('\n');
            self.out.copy_data(l.as_bytes());
        } else {
            let refs: Vec<Option<&[u8]>> = vals.iter().map(|v| v.as_deref().map(|s| s.as_bytes())).collect();
            self.out.data_row(&refs);
        }
    }

    // ------------------------------------------------------------------ extended protocol

    fn parse(&mut self, name: &str, query: &str, param_types: &[u32]) -> Result<()> {
        let stmts = split_statements(query);
        if stmts.len() > 1 {
            return Err(SqlError::syntax("cannot insert multiple commands into a prepared statement"));
        }
        let text = stmts.into_iter().next().unwrap_or_default();
        parse_statement(&text)?;
        let types: Vec<SqlType> = param_types.iter().map(|o| SqlType::from_oid(*o).unwrap_or(SqlType::Unknown)).collect();
        let nparams = crate::sql::parser::max_param_index(&text).max(types.len());
        self.prepared.insert(name.to_string(), Prepared { text, param_types: types, nparams });
        Ok(())
    }

    fn bind(&mut self, portal: &str, stmt: &str, formats: &[i16], params: &[Option<Vec<u8>>], result_formats: Vec<i16>) -> Result<()> {
        let p = self
            .prepared
            .get(stmt)
            .ok_or_else(|| SqlError::new(codes::INVALID_SQL_STATEMENT_NAME, format!("prepared statement \"{stmt}\" does not exist")))?;
        if params.len() != p.nparams {
            return Err(SqlError::new(
                codes::PROTOCOL_VIOLATION,
                format!("bind message supplies {} parameters, but prepared statement \"{stmt}\" requires {}", params.len(), p.nparams),
            ));
        }
        let mut slots = Vec::with_capacity(params.len());
        for (i, v) in params.iter().enumerate() {
            let ty = p.param_types.get(i).cloned().unwrap_or(SqlType::Unknown);
            let fmt = match formats.len() {
                0 => 0,
                1 => formats[0],
                _ => formats.get(i).copied().unwrap_or(0),
            };
            let value = match v {
                None => Datum::Null,
                Some(b) if fmt == 1 && ty != SqlType::Unknown => recv_datum(b, &ty)?,
                Some(b) => {
                    let s = std::str::from_utf8(b).map_err(|_| SqlError::new("22021", "invalid byte sequence for encoding \"UTF8\""))?;
                    if ty == SqlType::Unknown { Datum::text(s) } else { parse_datum(s, &ty, -1)? }
                }
            };
            slots.push(ParamSlot { ty: ty.clone(), value: Some(value) });
        }
        let text = p.text.clone();
        self.portals.insert(portal.to_string(), Portal { text, params: slots, result_formats, pending: None });
        Ok(())
    }

    fn describe(&mut self, kind: u8, name: &str) -> Result<()> {
        let (stmt, params, formats, is_stmt) = if kind == b'S' {
            let p = self
                .prepared
                .get(name)
                .ok_or_else(|| SqlError::new(codes::INVALID_SQL_STATEMENT_NAME, format!("prepared statement \"{name}\" does not exist")))?;
            let params: Vec<ParamSlot> = p.param_types.iter().map(|t| ParamSlot { ty: t.clone(), value: None }).collect();
            (parse_statement(&p.text)?, params, vec![], true)
        } else {
            let p = self
                .portals
                .get(name)
                .ok_or_else(|| SqlError::new(codes::INVALID_CURSOR_NAME, format!("portal \"{name}\" does not exist")))?;
            (parse_statement(&p.text)?, p.params.clone(), p.result_formats.clone(), false)
        };
        let (cols, ptypes) = match &stmt {
            Statement::Show(_) | Statement::Explain { .. } => {
                // run eagerly to learn the columns
                match self.run_statement(stmt, params)? {
                    ExecResult::Query { columns, .. } => (columns, vec![]),
                    _ => (vec![], vec![]),
                }
            }
            // rows streamed by SUBSCRIBE: gsn [, progressed], diff, then the
            // target's columns (COPY (SUBSCRIBE ...) has no row description)
            Statement::Subscribe { target, progress, copy: false, .. } => {
                let text = match target {
                    SubscribeTarget::Name(n) => format!("SELECT * FROM \"{}\"", n.replace('"', "\"\"")),
                    SubscribeTarget::Query(_, text) => text.clone(),
                };
                let query = Box::new(crate::sql::parser::parse_single_query(&text)?);
                let (columns, ptypes) = self.engine.describe(Statement::Select { query, as_of: AsOf::Default, text }, params)?;
                let mut cols = vec![ResultColumn { name: "gsn".into(), ty: SqlType::Int8, typmod: -1 }];
                if *progress {
                    cols.push(ResultColumn { name: "progressed".into(), ty: SqlType::Bool, typmod: -1 });
                }
                cols.push(ResultColumn { name: "diff".into(), ty: SqlType::Int8, typmod: -1 });
                cols.extend(columns);
                (cols, ptypes)
            }
            _ => self.engine.describe(stmt, params)?,
        };
        if is_stmt {
            let mut oids: Vec<u32> = ptypes.iter().map(|t| t.oid()).collect();
            if let Some(p) = self.prepared.get_mut(name) {
                for (i, t) in ptypes.iter().enumerate() {
                    if p.param_types.get(i).is_none_or(|x| *x == SqlType::Unknown) {
                        if i < p.param_types.len() {
                            p.param_types[i] = t.clone();
                        } else {
                            p.param_types.push(t.clone());
                        }
                    }
                }
                oids = p.param_types.iter().map(|t| if *t == SqlType::Unknown { 25 } else { t.oid() }).collect();
            }
            self.out.parameter_description(&oids);
        }
        if cols.is_empty() {
            self.out.no_data();
        } else {
            let fields: Vec<FieldDescription> = cols
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    let f = match formats.len() {
                        0 => 0,
                        1 => formats[0],
                        _ => formats.get(i).copied().unwrap_or(0),
                    };
                    field(c, f)
                })
                .collect();
            self.out.row_description(&fields);
        }
        Ok(())
    }

    fn execute_portal(&mut self, name: &str, max_rows: i32) -> Result<()> {
        let Some(mut portal) = self.portals.remove(name) else {
            return Err(SqlError::new(codes::INVALID_CURSOR_NAME, format!("portal \"{name}\" does not exist")));
        };
        if portal.pending.is_none() {
            let stmt = parse_statement(&portal.text)?;
            self.pending_copy = matches!(stmt, Statement::Subscribe { copy: true, .. });
            let res = self.run_statement(stmt, portal.params.clone())?;
            match res {
                ExecResult::Query { columns, rows } => portal.pending = Some((columns, rows.into_iter())),
                other => {
                    self.send_result(other, &portal.result_formats, max_rows, false)?;
                    return Ok(());
                }
            }
        }
        let formats = portal.result_formats.clone();
        let (cols, iter) = portal.pending.as_mut().unwrap();
        let limit = if max_rows <= 0 { usize::MAX } else { max_rows as usize };
        let mut sent = 0;
        for r in iter.by_ref() {
            let vals: Vec<Option<Vec<u8>>> = r
                .iter()
                .enumerate()
                .map(|(i, d)| {
                    let f = match formats.len() {
                        0 => 0,
                        1 => formats[0],
                        _ => formats.get(i).copied().unwrap_or(0),
                    };
                    encode_value(d, &cols[i].ty, f == 1)
                })
                .collect();
            let refs: Vec<Option<&[u8]>> = vals.iter().map(|v| v.as_deref()).collect();
            self.out.data_row(&refs);
            sent += 1;
            if sent >= limit {
                break;
            }
        }
        // as in PostgreSQL: suspended whenever max_rows rows were sent, and
        // the count is that of this Execute; an exhausted portal stays (until
        // Close or the end of the transaction) and yields no more rows
        if max_rows > 0 && sent == limit {
            self.out.portal_suspended();
        } else {
            self.out.command_complete(&format!("SELECT {sent}"));
        }
        self.portals.insert(name.to_string(), portal);
        Ok(())
    }
}
