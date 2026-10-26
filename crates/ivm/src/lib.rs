//! ivm: an incremental view maintenance engine for PostgreSQL sources.
//!
//! One representation (weighted rows stamped with a GSN), one clock (the GSN
//! assigned to every source commit on arrival), determinism (state at GSN g is
//! a pure function of the input log up to g).

pub mod catalog;
pub mod coord;
pub mod dataflow;
pub mod error;
pub mod exec;
pub mod expr;
pub mod http;
pub mod log;
pub mod memory;
pub mod server;
pub mod sink;
pub mod source;
pub mod sql;
pub mod storage;
pub mod sysrel;
#[cfg(test)]
pub(crate) mod testkit;
pub mod types;

pub use error::{Result, SqlError};
