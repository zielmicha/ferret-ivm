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

impl SqlType {
    pub fn oid(&self) -> u32 {
        use SqlType::*;
        match self {
            Bool => 16,
            Bytea => 17,
            Name => 19,
            Int8 => 20,
            Int2 => 21,
            Int4 => 23,
            Text => 25,
            Oid => 26,
            Json => 114,
            Float4 => 700,
            Float8 => 701,
            Unknown => 705,
            Bpchar => 1042,
            Varchar => 1043,
            Date => 1082,
            Time => 1083,
            Timestamp => 1114,
            TimestampTz => 1184,
            Interval => 1186,
            Numeric => 1700,
            Uuid => 2950,
            Jsonb => 3802,
            JsonPath => 4072,
            Array(e) => match **e {
                Bool => 1000,
                Bytea => 1001,
                Name => 1003,
                Int2 => 1005,
                Int4 => 1007,
                Text => 1009,
                Bpchar => 1014,
                Varchar => 1015,
                Int8 => 1016,
                Float4 => 1021,
                Float8 => 1022,
                Oid => 1028,
                Timestamp => 1115,
                Date => 1182,
                Time => 1183,
                TimestampTz => 1185,
                Interval => 1187,
                Numeric => 1231,
                Uuid => 2951,
                Json => 199,
                Jsonb => 3807,
                JsonPath => 4073,
                _ => 1009,
            },
        }
    }

    pub fn from_oid(oid: u32) -> Option<SqlType> {
        use SqlType::*;
        Some(match oid {
            16 => Bool,
            17 => Bytea,
            18 => Bpchar, // "char"
            19 => Name,
            20 => Int8,
            21 => Int2,
            23 => Int4,
            25 => Text,
            26 => Oid,
            114 => Json,
            700 => Float4,
            701 => Float8,
            705 => Unknown,
            1042 => Bpchar,
            1043 => Varchar,
            1082 => Date,
            1083 => Time,
            1114 => Timestamp,
            1184 => TimestampTz,
            1186 => Interval,
            1700 => Numeric,
            2950 => Uuid,
            3802 => Jsonb,
            4072 => JsonPath,
            1000 => Array(Box::new(Bool)),
            1001 => Array(Box::new(Bytea)),
            1003 => Array(Box::new(Name)),
            1005 => Array(Box::new(Int2)),
            1007 => Array(Box::new(Int4)),
            1009 => Array(Box::new(Text)),
            1014 => Array(Box::new(Bpchar)),
            1015 => Array(Box::new(Varchar)),
            1016 => Array(Box::new(Int8)),
            1021 => Array(Box::new(Float4)),
            1022 => Array(Box::new(Float8)),
            1028 => Array(Box::new(Oid)),
            1115 => Array(Box::new(Timestamp)),
            1182 => Array(Box::new(Date)),
            1183 => Array(Box::new(Time)),
            1185 => Array(Box::new(TimestampTz)),
            1187 => Array(Box::new(Interval)),
            1231 => Array(Box::new(Numeric)),
            2951 => Array(Box::new(Uuid)),
            199 => Array(Box::new(Json)),
            3807 => Array(Box::new(Jsonb)),
            4073 => Array(Box::new(JsonPath)),
            _ => return None,
        })
    }

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

    pub fn name(&self) -> String {
        use SqlType::*;
        match self {
            Bool => "boolean".into(),
            Int2 => "smallint".into(),
            Int4 => "integer".into(),
            Int8 => "bigint".into(),
            Float4 => "real".into(),
            Float8 => "double precision".into(),
            Numeric => "numeric".into(),
            Text => "text".into(),
            Varchar => "character varying".into(),
            Bpchar => "character".into(),
            Name => "name".into(),
            Bytea => "bytea".into(),
            Date => "date".into(),
            Time => "time without time zone".into(),
            Timestamp => "timestamp without time zone".into(),
            TimestampTz => "timestamp with time zone".into(),
            Interval => "interval".into(),
            Uuid => "uuid".into(),
            Json => "json".into(),
            Jsonb => "jsonb".into(),
            JsonPath => "jsonpath".into(),
            Oid => "oid".into(),
            Array(e) => format!("{}[]", e.name()),
            Unknown => "unknown".into(),
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

    /// Whether values of this type need a fidelity suffix in key encodings
    /// (equal-comparing values with distinct representations).
    pub fn needs_fidelity(&self) -> bool {
        match self {
            SqlType::Numeric | SqlType::Interval | SqlType::Float4 | SqlType::Float8 | SqlType::Jsonb => true,
            SqlType::Array(e) => e.needs_fidelity(),
            _ => false,
        }
    }
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
        _ => ty.name(),
    }
}

#[cfg(test)]
mod tests;
