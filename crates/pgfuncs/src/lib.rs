//! pgfuncs: PostgreSQL 17 compatible scalar function semantics.
//!
//! Every function here reproduces PostgreSQL 17 behaviour exactly (results,
//! error messages, SQLSTATEs and details) for a database with encoding UTF8
//! and the builtin collation provider with locale `C.UTF-8` (code-point
//! ordering, Unicode 15.1 simple case mapping). Expected results were recorded
//! from a real PostgreSQL 17 server (see `tests/gen/` and `tests/data/`).
//!
//! Modules:
//! - [`text`]: string functions (length, case mapping, substring, trim,
//!   padding, `format()`, `split_part`, `unistr`, hashing, quoting, ...).
//! - [`pattern`]: `LIKE`/`ILIKE` (text and bytea), `SIMILAR TO`, and POSIX
//!   regular expressions via a port of PostgreSQL's Henry Spencer regex
//!   engine (ARE/ERE/BRE syntax, backreferences, lookaround, PostgreSQL's
//!   leftmost-longest/shortest match and submatch rules) with the `regexp_*`
//!   functions, `~`, `substring(... from ...)`.
//! - [`bytea`]: bytea input/output and `encode`/`decode`.
//! - [`uuid`]: uuid input/output.
//! - [`array`](mod@array): array text input/output (`array_in`/`array_out` parsing and
//!   quoting; element conversion is up to the caller).
//! - [`json`]: `jsonb` (values, ordering, operators and functions, an
//!   order-preserving key encoding), `json` text functions, and the SQL/JSON
//!   path language ([`json::path`]: `jsonpath` input/output and the
//!   `jsonb_path_*` functions and `@?` / `@@` operators, including datetime
//!   items).
//! - [`unicode`]: the Unicode properties and case mappings PostgreSQL uses.
//!
//! Conventions: SQL NULL inputs are handled by the caller unless a signature
//! has an `Option`. Integer parameters typed `i64` stand for SQL `int4`
//! arguments; behaviour is exact within the `int4` range. Functions that can
//! raise an error return [`Result`]; a few infallible signatures have
//! `*_checked`/`try_*` companions for PostgreSQL's rare resource-limit errors.
//!
//! Portions of this crate are ported from PostgreSQL source code
//! (src/backend/utils/adt, src/backend/regex, src/common), Copyright (c)
//! 1996-2024, PostgreSQL Global Development Group, Portions Copyright (c)
//! 1994, The Regents of the University of California, used under the
//! PostgreSQL License; the regex engine is Copyright (c) 1998, 1999 Henry
//! Spencer, used under his license as distributed with PostgreSQL.

pub mod array;
pub mod bytea;
pub mod json;
pub mod pattern;
pub mod text;
pub mod unicode;
pub mod uuid;

/// Error raised by a function, mirroring a PostgreSQL `ereport(ERROR, ...)`.
/// `message` and `detail` are PostgreSQL's primary message and DETAIL text;
/// HINT and CONTEXT are not represented.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub sqlstate: &'static str,
    pub message: String,
    pub detail: Option<String>,
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn new(sqlstate: &'static str, message: impl Into<String>) -> Error {
        Error { sqlstate, message: message.into(), detail: None }
    }

    pub fn with_detail(sqlstate: &'static str, message: impl Into<String>, detail: impl Into<String>) -> Error {
        Error { sqlstate, message: message.into(), detail: Some(detail.into()) }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

/// SQLSTATE codes (PostgreSQL's errcodes.txt names).
pub mod sqlstate {
    pub const INVALID_TEXT_REPRESENTATION: &str = "22P02";
    pub const INVALID_PARAMETER_VALUE: &str = "22023";
    pub const INVALID_REGULAR_EXPRESSION: &str = "2201B";
    pub const INVALID_ESCAPE_SEQUENCE: &str = "22025";
    pub const SUBSTRING_ERROR: &str = "22011";
    pub const PROGRAM_LIMIT_EXCEEDED: &str = "54000";
    pub const NUMERIC_VALUE_OUT_OF_RANGE: &str = "22003";
    pub const ARRAY_SUBSCRIPT_ERROR: &str = "2202E";
    pub const FEATURE_NOT_SUPPORTED: &str = "0A000";
    pub const UNTRANSLATABLE_CHARACTER: &str = "22P05";
    pub const CHARACTER_NOT_IN_REPERTOIRE: &str = "22021";
    pub const INVALID_BINARY_REPRESENTATION: &str = "22P03";
    pub const STRING_DATA_RIGHT_TRUNCATION: &str = "22001";
    pub const INVALID_ARGUMENT_FOR_NTH_VALUE: &str = "22016";
    pub const NULL_VALUE_NOT_ALLOWED: &str = "22004";
    pub const OBJECT_NOT_IN_PREREQUISITE_STATE: &str = "55000";
    /// Also used for "stack depth limit exceeded".
    pub const STATEMENT_TOO_COMPLEX: &str = "54001";
    pub const DATATYPE_MISMATCH: &str = "42804";
    pub const UNDEFINED_PARAMETER: &str = "42P02";
    pub const INVALID_ESCAPE_CHARACTER: &str = "22019";
    pub const INVALID_USE_OF_ESCAPE_CHARACTER: &str = "2200C";
    pub const UNTERMINATED_C_STRING: &str = "22024";
    pub const INVALID_JSON_TEXT: &str = "22032";
    pub const INVALID_INDICATOR_PARAMETER_VALUE: &str = "22010";
    pub const OUT_OF_MEMORY: &str = "53200";
    pub const DIVISION_BY_ZERO: &str = "22012";
    pub const SYNTAX_ERROR: &str = "42601";
    pub const UNDEFINED_OBJECT: &str = "42704";
    pub const INTERNAL_ERROR: &str = "XX000";
    pub const INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION: &str = "22031";
    pub const INVALID_SQL_JSON_SUBSCRIPT: &str = "22033";
    pub const NON_NUMERIC_SQL_JSON_ITEM: &str = "22036";
    pub const SINGLETON_SQL_JSON_ITEM_REQUIRED: &str = "22038";
    pub const SQL_JSON_ARRAY_NOT_FOUND: &str = "22039";
    pub const SQL_JSON_MEMBER_NOT_FOUND: &str = "2203A";
    pub const SQL_JSON_NUMBER_NOT_FOUND: &str = "2203B";
    pub const SQL_JSON_OBJECT_NOT_FOUND: &str = "2203C";
    pub const DATETIME_VALUE_OUT_OF_RANGE: &str = "22008";
    pub const INVALID_DATETIME_FORMAT: &str = "22007";
    pub const DATETIME_FIELD_OVERFLOW: &str = "22008";
    pub const INVALID_TIME_ZONE_DISPLACEMENT_VALUE: &str = "22009";
}
