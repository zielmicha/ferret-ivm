//! SQL types, datums and their encodings.

pub mod codec;
pub mod datum;
pub mod encoding;
pub mod text;

pub use datum::{ArrayValue, Datum, Row};

use serde::{Deserialize, Serialize};
use std::fmt;

/// A SQL type without modifiers (PostgreSQL type OID equivalent).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SqlType {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Numeric,
    Text,
    Varchar,
    Bpchar,
    Name,
    Bytea,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Interval,
    Uuid,
    Json,
    Jsonb,
    /// SQL/JSON path; values are held as their canonical text
    JsonPath,
    Oid,
    Array(Box<SqlType>),
    /// Untyped string literal or NULL before resolution.
    Unknown,
}

/// A column type: SQL type plus PostgreSQL-style type modifier (-1 = none).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnType {
    pub ty: SqlType,
    pub typmod: i32,
    pub nullable: bool,
}

impl ColumnType {
    pub fn new(ty: SqlType) -> Self {
        ColumnType { ty, typmod: -1, nullable: true }
    }
    pub fn with_typmod(ty: SqlType, typmod: i32) -> Self {
        ColumnType { ty, typmod, nullable: true }
    }
    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }
    pub fn nullable(mut self, n: bool) -> Self {
        self.nullable = n;
        self
    }
    /// How `=` relates to the identity of the column's values: as for its
    /// type, except that `character(n)` values are all padded to n
    /// characters, so none differ in trailing spaces alone.
    pub fn equality(&self) -> Equality {
        match self.ty.equality() {
            Equality::TrailingSpaces if self.typmod >= 0 => Equality::Identity,
            e => e,
        }
    }
}

/// Named column in a relation schema.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub ty: ColumnType,
}

impl Column {
    pub fn new(name: impl Into<String>, ty: ColumnType) -> Self {
        Column { name: name.into(), ty }
    }
}

pub const VARHDRSZ: i32 = 4;

// Declare scalar and array OIDs together so encoding and lookup cannot drift.
macro_rules! type_metadata {
    ($($ty:ident => ($oid:literal, $array_oid:literal, $name:literal)),* $(,)?) => {
        impl SqlType {
            pub fn oid(&self) -> u32 {
                match self {
                    $(Self::$ty => $oid,)*
                    Self::Unknown => 705,
                    Self::Array(e) => match e.as_ref() {
                        $(Self::$ty => $array_oid,)*
                        _ => 1009,
                    },
                }
            }

            pub fn from_oid(oid: u32) -> Option<Self> {
                Some(match oid {
                    $($oid => Self::$ty, $array_oid => Self::$ty.array_of(),)*
                    18 => Self::Bpchar, // PostgreSQL's internal "char"
                    705 => Self::Unknown,
                    _ => return None,
                })
            }

            pub fn name(&self) -> String {
                match self {
                    $(Self::$ty => $name.into(),)*
                    Self::Unknown => "unknown".into(),
                    Self::Array(e) => format!("{}[]", e.name()),
                }
            }
        }
    };
}

type_metadata! {
    Bool => (16, 1000, "boolean"),
    Bytea => (17, 1001, "bytea"),
    Name => (19, 1003, "name"),
    Int8 => (20, 1016, "bigint"),
    Int2 => (21, 1005, "smallint"),
    Int4 => (23, 1007, "integer"),
    Text => (25, 1009, "text"),
    Oid => (26, 1028, "oid"),
    Json => (114, 199, "json"),
    Float4 => (700, 1021, "real"),
    Float8 => (701, 1022, "double precision"),
    Bpchar => (1042, 1014, "character"),
    Varchar => (1043, 1015, "character varying"),
    Date => (1082, 1182, "date"),
    Time => (1083, 1183, "time without time zone"),
    Timestamp => (1114, 1115, "timestamp without time zone"),
    TimestampTz => (1184, 1185, "timestamp with time zone"),
    Interval => (1186, 1187, "interval"),
    Numeric => (1700, 1231, "numeric"),
    Uuid => (2950, 2951, "uuid"),
    Jsonb => (3802, 3807, "jsonb"),
    JsonPath => (4072, 4073, "jsonpath"),
}

impl SqlType {
    /// Fixed size for RowDescription (-1 = variable).
    pub fn type_size(&self) -> i16 {
        use SqlType::*;
        match self {
            Bool => 1,
            Int2 => 2,
            Int4 | Float4 | Date | Oid => 4,
            Int8 | Float8 | Time | Timestamp | TimestampTz => 8,
            Interval | Uuid => 16,
            Name => 64,
            Unknown => -2,
            _ => -1,
        }
    }

    pub fn is_integer(&self) -> bool {
        matches!(self, SqlType::Int2 | SqlType::Int4 | SqlType::Int8)
    }
    pub fn is_float(&self) -> bool {
        matches!(self, SqlType::Float4 | SqlType::Float8)
    }
    pub fn is_numeric_family(&self) -> bool {
        matches!(
            self,
            SqlType::Int2 | SqlType::Int4 | SqlType::Int8 | SqlType::Float4 | SqlType::Float8 | SqlType::Numeric
        )
    }
    pub fn is_string(&self) -> bool {
        matches!(self, SqlType::Text | SqlType::Varchar | SqlType::Bpchar | SqlType::Name)
    }
    pub fn is_datetime(&self) -> bool {
        matches!(self, SqlType::Date | SqlType::Time | SqlType::Timestamp | SqlType::TimestampTz)
    }

    /// PostgreSQL type category (pg_type.typcategory).
    pub fn category(&self) -> char {
        use SqlType::*;
        match self {
            Bool => 'B',
            Int2 | Int4 | Int8 | Float4 | Float8 | Numeric | Oid => 'N',
            Text | Varchar | Bpchar | Name => 'S',
            Bytea => 'U',
            Date | Time | Timestamp | TimestampTz => 'D',
            Interval => 'T',
            Uuid | Json | Jsonb | JsonPath => 'U',
            Array(_) => 'A',
            Unknown => 'X',
        }
    }

    /// Whether this is the preferred type of its category.
    pub fn is_preferred(&self) -> bool {
        matches!(self, SqlType::Bool | SqlType::Float8 | SqlType::Text | SqlType::TimestampTz | SqlType::Interval | SqlType::Oid)
    }

    /// Element type of an array type.
    pub fn elem(&self) -> Option<&SqlType> {
        match self {
            SqlType::Array(e) => Some(e),
            _ => None,
        }
    }

    /// The array type over this type (an array type is its own: there are
    /// no arrays of arrays, only multidimensional arrays).
    pub fn array_of(self) -> SqlType {
        match self {
            SqlType::Array(_) => self,
            t => SqlType::Array(Box::new(t)),
        }
    }

    /// How `=` relates to the identity of this type's values (an array's
    /// is its elements'). Every type is listed: a new one must say.
    pub fn equality(&self) -> Equality {
        use SqlType::*;
        match self {
            Numeric | Interval | Float4 | Float8 | Jsonb => Equality::Representation,
            Bpchar => Equality::TrailingSpaces,
            Json | JsonPath => Equality::None,
            Array(e) => e.equality(),
            Bool | Int2 | Int4 | Int8 | Oid | Text | Varchar | Name | Bytea | Date | Time | Timestamp | TimestampTz | Uuid | Unknown => {
                Equality::Identity
            }
        }
    }

    /// Whether values of this type need a fidelity suffix in key encodings
    /// (equal-comparing values with distinct representations).
    pub fn needs_fidelity(&self) -> bool {
        self.equality() == Equality::Representation
    }
}

/// How SQL equality (`=`) relates to the identity of a type's values. Said
/// once (`SqlType::equality`) for everything that depends on it: key
/// encodings (fidelity suffixes), decorrelation, and sinks (which target
/// row a retraction deletes, which columns can key a target).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Equality {
    /// Equal values are identical.
    Identity,
    /// Equal values can differ in representation: numeric scale (`1.0 =
    /// 1.00`), the sign of zero (`-0 = 0`), interval fields (`'1 mon' =
    /// '30 days'`, which differ in date arithmetic), numbers in jsonb. Key
    /// encodings group them by equality, and keep the representation in a
    /// fidelity suffix.
    Representation,
    /// Equal values can differ in trailing spaces (`character`: `'a' =
    /// 'a  '`). Comparisons ignore them, while key encodings keep them, so
    /// grouping, DISTINCT and keys tell such values apart (unlike
    /// PostgreSQL). The values of a `character(n)` column are all padded
    /// to n characters (`ColumnType::equality`).
    TrailingSpaces,
    /// There is no equality operator (json, jsonpath).
    None,
}

impl fmt::Display for SqlType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name())
    }
}

/// Type modifier helpers (PostgreSQL conventions).
pub mod typmod {
    use super::VARHDRSZ;
    pub fn numeric(precision: i32, scale: i32) -> i32 {
        ((precision << 16) | (scale & 0x7ff)) + VARHDRSZ
    }
    /// (precision, scale) of a numeric typmod.
    pub fn numeric_parts(typmod: i32) -> Option<(i32, i32)> {
        if typmod < VARHDRSZ {
            return None;
        }
        let t = typmod - VARHDRSZ;
        let precision = (t >> 16) & 0xffff;
        let scale = ((t & 0x7ff) ^ 1024) - 1024;
        Some((precision, scale))
    }
    pub fn varchar(len: i32) -> i32 {
        len + VARHDRSZ
    }
    pub fn varchar_len(typmod: i32) -> Option<i32> {
        if typmod < VARHDRSZ { None } else { Some(typmod - VARHDRSZ) }
    }
    /// The fields of an interval typmod as PostgreSQL's intervaltypmodout
    /// writes them ("day to second"; None for all fields or no typmod).
    pub fn interval_fields(typmod: i32) -> Option<&'static str> {
        if typmod < 0 {
            return None;
        }
        // field bits as in PostgreSQL's datetime.h
        const MONTH: i32 = 1 << 1;
        const YEAR: i32 = 1 << 2;
        const DAY: i32 = 1 << 3;
        const HOUR: i32 = 1 << 10;
        const MINUTE: i32 = 1 << 11;
        const SECOND: i32 = 1 << 12;
        Some(match (typmod >> 16) & 0x7FFF {
            YEAR => "year",
            MONTH => "month",
            DAY => "day",
            HOUR => "hour",
            MINUTE => "minute",
            SECOND => "second",
            r if r == YEAR | MONTH => "year to month",
            r if r == DAY | HOUR => "day to hour",
            r if r == DAY | HOUR | MINUTE => "day to minute",
            r if r == DAY | HOUR | MINUTE | SECOND => "day to second",
            r if r == HOUR | MINUTE => "hour to minute",
            r if r == HOUR | MINUTE | SECOND => "hour to second",
            r if r == MINUTE | SECOND => "minute to second",
            _ => return None,
        })
    }
    /// The fractional-second precision of an interval typmod (None: the
    /// default).
    pub fn interval_precision(typmod: i32) -> Option<i32> {
        (typmod >= 0 && typmod & 0xFFFF != 0xFFFF).then_some(typmod & 0xFFFF)
    }
}

/// Render a type with its modifier the way format_type() does.
pub fn format_type(ty: &SqlType, typmod: i32) -> String {
    match ty {
        SqlType::Numeric => match typmod::numeric_parts(typmod) {
            Some((p, s)) => format!("numeric({p},{s})"),
            None => "numeric".into(),
        },
        SqlType::Varchar => match typmod::varchar_len(typmod) {
            Some(n) => format!("character varying({n})"),
            None => "character varying".into(),
        },
        SqlType::Bpchar => match typmod::varchar_len(typmod) {
            Some(n) => format!("character({n})"),
            None => "bpchar".into(),
        },
        SqlType::Timestamp | SqlType::TimestampTz | SqlType::Time if typmod >= 0 => {
            let base = match ty {
                SqlType::Timestamp => "timestamp",
                SqlType::TimestampTz => "timestamp",
                _ => "time",
            };
            let suffix = match ty {
                SqlType::TimestampTz => " with time zone",
                _ => " without time zone",
            };
            format!("{base}({typmod}){suffix}")
        }
        SqlType::Interval if typmod >= 0 => {
            let mut s = String::from("interval");
            if let Some(f) = typmod::interval_fields(typmod) {
                s.push(' ');
                s.push_str(f);
            }
            if let Some(p) = typmod::interval_precision(typmod) {
                s.push_str(&format!("({p})"));
            }
            s
        }
        // (the modifier is the elements': without one, a bpchar[] is not
        // the character(1)[] that `character[]` means)
        SqlType::Array(e) => format!("{}[]", format_type(e, typmod)),
        _ => ty.name(),
    }
}

#[cfg(test)]
mod tests;
