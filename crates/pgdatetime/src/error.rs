//! Error helpers mirroring PostgreSQL's ereport() calls used by the
//! date/time code.

use crate::Error;

pub(crate) const INVALID_DATETIME_FORMAT: &str = "22007";
pub(crate) const DATETIME_FIELD_OVERFLOW: &str = "22008";
/// ERRCODE_DATETIME_VALUE_OUT_OF_RANGE shares SQLSTATE 22008 with
/// ERRCODE_DATETIME_FIELD_OVERFLOW.
pub(crate) const DATETIME_VALUE_OUT_OF_RANGE: &str = "22008";
pub(crate) const INVALID_TIME_ZONE_DISPLACEMENT_VALUE: &str = "22009";
pub(crate) const DIVISION_BY_ZERO: &str = "22012";
pub(crate) const INTERVAL_FIELD_OVERFLOW: &str = "22015";
pub(crate) const INVALID_PARAMETER_VALUE: &str = "22023";
pub(crate) const INVALID_TEXT_REPRESENTATION: &str = "22P02";
pub(crate) const FEATURE_NOT_SUPPORTED: &str = "0A000";
pub(crate) const CONFIG_FILE_ERROR: &str = "F0000";
/// Used by the non-`_opt` API wrappers when PostgreSQL would return SQL NULL.
pub const SQLSTATE_NULL_RESULT: &str = "22004";

impl Error {
    pub fn new(sqlstate: &'static str, message: impl Into<String>) -> Error {
        Error {
            sqlstate,
            message: message.into(),
            detail: None,
        }
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Error {
        self.detail = Some(detail.into());
        self
    }

    /// True if this error stands for "PostgreSQL would have returned NULL"
    /// (only produced by API functions that have an `_opt` twin).
    pub fn is_null_result(&self) -> bool {
        self.sqlstate == SQLSTATE_NULL_RESULT
    }
}

pub(crate) fn null_result() -> Error {
    Error::new(SQLSTATE_NULL_RESULT, "result is NULL")
}

pub(crate) fn timestamp_out_of_range() -> Error {
    Error::new(DATETIME_VALUE_OUT_OF_RANGE, "timestamp out of range")
}

pub(crate) fn interval_out_of_range() -> Error {
    Error::new(DATETIME_VALUE_OUT_OF_RANGE, "interval out of range")
}

pub(crate) fn date_out_of_range() -> Error {
    Error::new(DATETIME_VALUE_OUT_OF_RANGE, "date out of range")
}

/// DTERR_* codes from datetime.h
pub(crate) const DTERR_BAD_FORMAT: i32 = -1;
pub(crate) const DTERR_FIELD_OVERFLOW: i32 = -2;
pub(crate) const DTERR_MD_FIELD_OVERFLOW: i32 = -3;
pub(crate) const DTERR_INTERVAL_OVERFLOW: i32 = -4;
pub(crate) const DTERR_TZDISP_OVERFLOW: i32 = -5;
pub(crate) const DTERR_BAD_TIMEZONE: i32 = -6;
pub(crate) const DTERR_BAD_ZONE_ABBREV: i32 = -7;

/// Extra information for DTERR_BAD_TIMEZONE / DTERR_BAD_ZONE_ABBREV.
#[derive(Default, Debug, Clone)]
pub(crate) struct DtErrExtra {
    pub timezone: String,
    pub abbrev: String,
}

/// Port of DateTimeParseError().
pub(crate) fn datetime_parse_error(
    dterr: i32,
    extra: &DtErrExtra,
    s: &str,
    datatype: &str,
) -> Error {
    match dterr {
        DTERR_FIELD_OVERFLOW | DTERR_MD_FIELD_OVERFLOW => Error::new(
            DATETIME_FIELD_OVERFLOW,
            format!("date/time field value out of range: \"{}\"", s),
        ),
        DTERR_INTERVAL_OVERFLOW => Error::new(
            INTERVAL_FIELD_OVERFLOW,
            format!("interval field value out of range: \"{}\"", s),
        ),
        DTERR_TZDISP_OVERFLOW => Error::new(
            INVALID_TIME_ZONE_DISPLACEMENT_VALUE,
            format!("time zone displacement out of range: \"{}\"", s),
        ),
        DTERR_BAD_TIMEZONE => Error::new(
            INVALID_PARAMETER_VALUE,
            format!("time zone \"{}\" not recognized", extra.timezone),
        ),
        DTERR_BAD_ZONE_ABBREV => Error::new(
            CONFIG_FILE_ERROR,
            format!("time zone \"{}\" not recognized", extra.timezone),
        )
        .with_detail(format!(
            "This time zone name appears in the configuration file for time zone abbreviation \"{}\".",
            extra.abbrev
        )),
        _ => Error::new(
            INVALID_DATETIME_FORMAT,
            format!("invalid input syntax for type {}: \"{}\"", datatype, s),
        ),
    }
}
