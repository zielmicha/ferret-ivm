//! Engine error type. Mirrors a PostgreSQL ErrorResponse: SQLSTATE, message,
//! optional detail and hint.

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlError {
    pub code: String,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
}

pub type Result<T> = std::result::Result<T, SqlError>;

impl SqlError {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        SqlError { code: code.to_string(), message: message.into(), detail: None, hint: None }
    }
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }
    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(codes::INTERNAL_ERROR, message)
    }
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(codes::FEATURE_NOT_SUPPORTED, message)
    }
    pub fn syntax(message: impl Into<String>) -> Self {
        Self::new(codes::SYNTAX_ERROR, message)
    }
    pub fn io(e: std::io::Error) -> Self {
        Self::new(codes::IO_ERROR, format!("I/O error: {e}"))
    }
    pub fn corrupt(message: impl Into<String>) -> Self {
        Self::new(codes::DATA_CORRUPTED, message)
    }
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)?;
        if let Some(d) = &self.detail {
            write!(f, " (DETAIL: {d})")?;
        }
        Ok(())
    }
}

impl std::error::Error for SqlError {}

impl From<std::io::Error> for SqlError {
    fn from(e: std::io::Error) -> Self {
        SqlError::io(e)
    }
}

macro_rules! from_crate_error {
    ($t:ty) => {
        impl From<$t> for SqlError {
            fn from(e: $t) -> Self {
                SqlError { code: e.sqlstate.to_string(), message: e.message, detail: e.detail, hint: None }
            }
        }
    };
}
from_crate_error!(pgnumeric::Error);
from_crate_error!(pgdatetime::Error);
from_crate_error!(pgfuncs::Error);

impl From<pgproto::Error> for SqlError {
    fn from(e: pgproto::Error) -> Self {
        SqlError {
            code: e.sqlstate.clone().unwrap_or_else(|| codes::CONNECTION_FAILURE.to_string()),
            message: e.message.clone(),
            detail: e.detail.clone(),
            hint: e.hint.clone(),
        }
    }
}

#[macro_export]
macro_rules! sql_bail {
    ($code:expr, $($arg:tt)*) => {
        return Err($crate::error::SqlError::new($code, format!($($arg)*)))
    };
}

/// SQLSTATE codes used by the engine.
pub mod codes {
    pub const SUCCESSFUL: &str = "00000";
    pub const FEATURE_NOT_SUPPORTED: &str = "0A000";
    pub const CARDINALITY_VIOLATION: &str = "21000";
    pub const DATA_EXCEPTION: &str = "22000";
    pub const STRING_DATA_RIGHT_TRUNCATION: &str = "22001";
    pub const NUMERIC_VALUE_OUT_OF_RANGE: &str = "22003";
    pub const NULL_VALUE_NOT_ALLOWED: &str = "22004";
    pub const DATETIME_FIELD_OVERFLOW: &str = "22008";
    pub const DIVISION_BY_ZERO: &str = "22012";
    pub const INVALID_PARAMETER_VALUE: &str = "22023";
    pub const CHARACTER_NOT_IN_REPERTOIRE: &str = "22021";
    pub const INVALID_TEXT_REPRESENTATION: &str = "22P02";
    pub const INVALID_BINARY_REPRESENTATION: &str = "22P03";
    pub const ARRAY_SUBSCRIPT_ERROR: &str = "2202E";
    pub const NOT_NULL_VIOLATION: &str = "23502";
    pub const UNIQUE_VIOLATION: &str = "23505";
    pub const INVALID_TRANSACTION_STATE: &str = "25000";
    pub const INVALID_SQL_STATEMENT_NAME: &str = "26000";
    pub const INVALID_CURSOR_NAME: &str = "34000";
    pub const SYNTAX_ERROR: &str = "42601";
    pub const INSUFFICIENT_PRIVILEGE: &str = "42501";
    pub const UNDEFINED_COLUMN: &str = "42703";
    pub const UNDEFINED_TABLE: &str = "42P01";
    pub const UNDEFINED_FUNCTION: &str = "42883";
    pub const UNDEFINED_OBJECT: &str = "42704";
    pub const DUPLICATE_OBJECT: &str = "42710";
    pub const DUPLICATE_TABLE: &str = "42P07";
    pub const DUPLICATE_COLUMN: &str = "42701";
    pub const AMBIGUOUS_COLUMN: &str = "42702";
    pub const AMBIGUOUS_FUNCTION: &str = "42725";
    pub const DATATYPE_MISMATCH: &str = "42804";
    pub const GROUPING_ERROR: &str = "42803";
    pub const WRONG_OBJECT_TYPE: &str = "42809";
    pub const INVALID_COLUMN_REFERENCE: &str = "42P10";
    pub const INVALID_RECURSION: &str = "42P19";
    pub const WINDOWING_ERROR: &str = "42P20";
    pub const UNDEFINED_PARAMETER: &str = "42P02";
    pub const INDETERMINATE_DATATYPE: &str = "42P18";
    pub const DEPENDENT_OBJECTS_STILL_EXIST: &str = "2BP01";
    pub const OBJECT_NOT_IN_PREREQUISITE_STATE: &str = "55000";
    pub const OBJECT_IN_USE: &str = "55006";
    pub const QUERY_CANCELED: &str = "57014";
    pub const ADMIN_SHUTDOWN: &str = "57P01";
    pub const CONNECTION_FAILURE: &str = "08006";
    pub const PROTOCOL_VIOLATION: &str = "08P01";
    pub const IO_ERROR: &str = "58030";
    pub const DATA_CORRUPTED: &str = "XX001";
    pub const INTERNAL_ERROR: &str = "XX000";
    pub const PROGRAM_LIMIT_EXCEEDED: &str = "54000";
    pub const TOO_MANY_ARGUMENTS: &str = "54023";
    pub const STATEMENT_TOO_COMPLEX: &str = "54001";
}
