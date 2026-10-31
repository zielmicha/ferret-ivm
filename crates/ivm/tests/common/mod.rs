//! Integration-test engine lifecycle and SQL assertions.
#![allow(dead_code, unused_imports)]

#[path = "../../src/testkit.rs"]
mod testkit;
pub use testkit::{TempDir, temp_dir};

use ivm::coord::api::{ExecResult, SessionVars};
use ivm::coord::{self, Config, EngineHandle};
use ivm::error::{Result, SqlError};
use ivm::types::text::format_datum;
use std::path::Path;

pub fn config(dir: &Path) -> Config {
    Config { data_dir: dir.to_path_buf(), sync: false, tick: None, ..Default::default() }
}

pub fn start_in(dir: &Path) -> coord::Running {
    coord::start(config(dir)).expect("engine starts")
}

pub fn query(h: &EngineHandle, sql: &str) -> Result<ExecResult> {
    h.execute(ivm::sql::parse_statement(sql)?, vec![], SessionVars::default(), String::new())
}

pub fn exec(h: &EngineHandle, sql: &str) -> ExecResult {
    query(h, sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
}

pub fn rows(h: &EngineHandle, sql: &str) -> Vec<String> {
    match exec(h, sql) {
        ExecResult::Query { rows, .. } => rows.iter().map(|r| r.iter().map(format_datum).collect::<Vec<_>>().join("|")).collect(),
        other => panic!("{sql}: expected rows, got {other:?}"),
    }
}

/// Owns an engine and, for `start`, its temporary directory. Engines stop
/// before their directory is removed; failed tests retain it for inspection.
pub struct Engine {
    pub handle: EngineHandle,
    running: Option<coord::Running>,
    config: Config,
    _dir: Option<TempDir>,
}

impl Engine {
    pub fn start(name: &str) -> Engine {
        Self::start_with(name, |_| {})
    }

    pub fn start_with(name: &str, configure: impl FnOnce(&mut Config)) -> Engine {
        let dir = TempDir::new(name);
        let mut cfg = config(dir.path());
        configure(&mut cfg);
        Self::launch(cfg, Some(dir))
    }

    /// Open an externally owned directory, leaving it available after shutdown.
    pub fn open(dir: &Path) -> Engine {
        Self::launch(config(dir), None)
    }

    fn launch(config: Config, dir: Option<TempDir>) -> Engine {
        let running = coord::start(config.clone()).expect("engine starts");
        Engine { handle: running.handle.clone(), running: Some(running), config, _dir: dir }
    }

    pub fn restart(mut self) -> Engine {
        self.running.take().unwrap().shutdown();
        let running = coord::start(self.config.clone()).expect("engine restarts");
        self.handle = running.handle.clone();
        self.running = Some(running);
        self
    }

    pub fn shutdown(self) {
        drop(self);
    }

    pub fn crash(mut self) {
        self.running.take().unwrap().crash();
    }

    pub fn try_exec(&self, sql: &str) -> Result<ExecResult> {
        query(&self.handle, sql)
    }

    pub fn exec(&self, sql: &str) -> ExecResult {
        exec(&self.handle, sql)
    }

    pub fn rows(&self, sql: &str) -> Vec<String> {
        rows(&self.handle, sql)
    }

    pub fn err(&self, sql: &str) -> SqlError {
        self.try_exec(sql).expect_err(sql)
    }

    pub fn columns(&self, sql: &str) -> Vec<String> {
        match self.exec(sql) {
            ExecResult::Query { columns, .. } => columns.into_iter().map(|c| c.name).collect(),
            other => panic!("{sql}: expected rows, got {other:?}"),
        }
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        if let Some(running) = self.running.take() {
            if std::thread::panicking() {
                // A timeout failure must not wait for a stuck engine thread.
                running.handle.shutdown();
            } else {
                running.shutdown();
            }
        }
    }
}
