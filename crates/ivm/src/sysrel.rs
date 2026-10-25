//! System catalogs (`pg_catalog`, `information_schema`) as constant
//! relations derived from the engine's catalog when a query is bound.
//!
//! Client tools (psql's `\d` family, drivers, ORMs, BI tools) discover
//! schemas through these relations. Only what such tools commonly read is
//! provided; relations the engine has no counterpart for exist but are
//! empty, so their queries bind and return nothing.

use crate::catalog::{Catalog, ObjectKind};
use crate::types::{format_type, typmod, Column, ColumnType, Datum, Row, SqlType};

pub const NS_PG_CATALOG: u32 = 11;
pub const NS_PUBLIC: u32 = 2200;
pub const NS_INFORMATION_SCHEMA: u32 = 13000;
const OWNER: u32 = 10;
const AM_HEAP: u32 = 2;
const AM_BTREE: u32 = 403;
const COLL_DEFAULT: u32 = 100;

/// OID of a catalog object.
pub fn rel_oid(id: u64) -> u32 {
    16384 + id as u32
}

/// OID of the primary-key index of a table.
fn pkey_oid(id: u64) -> u32 {
    rel_oid(id) + 1_000_000
}

/// OID of the primary-key constraint of a table.
fn pkey_con_oid(id: u64) -> u32 {
    rel_oid(id) + 2_000_000
}

use SqlType as T;

struct Rel {
    cols: Vec<Column>,
    rows: Vec<Row>,
}

impl Rel {
    fn new(cols: &[(&str, SqlType)]) -> Rel {
        Rel { cols: cols.iter().map(|(n, t)| Column::new(n.to_string(), ColumnType::new(t.clone()))).collect(), rows: vec![] }
    }
    fn push(&mut self, row: Row) {
        debug_assert_eq!(row.len(), self.cols.len());
        self.rows.push(row);
    }
}

fn text(s: &str) -> Datum {
    Datum::text(s)
}
fn oid(v: u32) -> Datum {
    Datum::Oid(v)
}
fn i2(v: i16) -> Datum {
    Datum::Int2(v)
}
fn i4(v: i32) -> Datum {
    Datum::Int4(v)
}
fn b(v: bool) -> Datum {
    Datum::Bool(v)
}
const NULL: Datum = Datum::Null;

/// PostgreSQL's internal name, length and category of a type.
fn type_info(t: &SqlType) -> (&'static str, i16, &'static str) {
    match t {
        T::Bool => ("bool", 1, "B"),
        T::Int2 => ("int2", 2, "N"),
        T::Int4 => ("int4", 4, "N"),
        T::Int8 => ("int8", 8, "N"),
        T::Float4 => ("float4", 4, "N"),
        T::Float8 => ("float8", 8, "N"),
        T::Numeric => ("numeric", -1, "N"),
        T::Text => ("text", -1, "S"),
        T::Varchar => ("varchar", -1, "S"),
        T::Bpchar => ("bpchar", -1, "S"),
        T::Name => ("name", 64, "S"),
        T::Bytea => ("bytea", -1, "U"),
        T::Date => ("date", 4, "D"),
        T::Time => ("time", 8, "D"),
        T::Timestamp => ("timestamp", 8, "D"),
        T::TimestampTz => ("timestamptz", 8, "D"),
        T::Interval => ("interval", 16, "T"),
        T::Uuid => ("uuid", 16, "U"),
        T::Json => ("json", -1, "U"),
        T::Jsonb => ("jsonb", -1, "U"),
        T::JsonPath => ("jsonpath", -1, "U"),
        T::Oid => ("oid", 4, "N"),
        // arrays are varlena (callers name them after their element type)
        T::Array(_) => ("_array", -1, "A"),
        T::Unknown => ("unknown", -2, "X"),
    }
}

const BASE_TYPES: &[SqlType] = &[
    T::Bool,
    T::Int2,
    T::Int4,
    T::Int8,
    T::Float4,
    T::Float8,
    T::Numeric,
    T::Text,
    T::Varchar,
    T::Bpchar,
    T::Name,
    T::Bytea,
    T::Date,
    T::Time,
    T::Timestamp,
    T::TimestampTz,
    T::Interval,
    T::Uuid,
    T::Json,
    T::Jsonb,
    T::JsonPath,
    T::Oid,
];

fn collation_of(t: &SqlType) -> u32 {
    if t.is_string() { COLL_DEFAULT } else { 0 }
}

/// Relations of the catalog visible as tables, in id order:
/// (oid, name, relkind, columns, has primary key).
fn relations(cat: &Catalog) -> Vec<(u32, String, &'static str, Vec<Column>, bool)> {
    let mut out = Vec::new();
    for o in cat.objects.values() {
        match &o.kind {
            ObjectKind::Table(t) => out.push((rel_oid(o.id), o.name.clone(), "r", t.columns.clone(), !t.pk.is_empty())),
            ObjectKind::MaterializedView(v) => out.push((rel_oid(o.id), o.name.clone(), "m", v.columns.clone(), false)),
            ObjectKind::View(v) => out.push((rel_oid(o.id), o.name.clone(), "v", v.columns.clone(), false)),
            _ => {}
        }
    }
    out
}

/// A system relation by (schema, name), as columns and rows.
pub fn system_relation(cat: &Catalog, schema: Option<&str>, name: &str) -> Option<(Vec<Column>, Vec<Row>)> {
    let r = match schema {
        // pg_catalog is implicitly first on the search path
        Some("pg_catalog") => pg_catalog(cat, name)?,
        None if name.starts_with("pg_") => pg_catalog(cat, name)?,
        Some("information_schema") => information_schema(cat, name)?,
        Some("ivm") => ivm_schema(cat, name)?,
        _ => return None,
    };
    Some((r.cols, r.rows))
}

fn pg_catalog(cat: &Catalog, name: &str) -> Option<Rel> {
    Some(match name {
        "pg_namespace" => {
            let mut r = Rel::new(&[("oid", T::Oid), ("nspname", T::Name), ("nspowner", T::Oid), ("nspacl", T::Text)]);
            for (o, n) in [(NS_PG_CATALOG, "pg_catalog"), (99, "pg_toast"), (NS_PUBLIC, "public"), (NS_INFORMATION_SCHEMA, "information_schema")] {
                r.push(vec![oid(o), text(n), oid(OWNER), NULL]);
            }
            r
        }
        "pg_class" => {
            let mut r = Rel::new(&[
                ("oid", T::Oid),
                ("relname", T::Name),
                ("relnamespace", T::Oid),
                ("reltype", T::Oid),
                ("reloftype", T::Oid),
                ("relowner", T::Oid),
                ("relam", T::Oid),
                ("relfilenode", T::Oid),
                ("reltablespace", T::Oid),
                ("relpages", T::Int4),
                ("reltuples", T::Float4),
                ("relallvisible", T::Int4),
                ("reltoastrelid", T::Oid),
                ("relhasindex", T::Bool),
                ("relisshared", T::Bool),
                ("relpersistence", T::Text),
                ("relkind", T::Text),
                ("relnatts", T::Int2),
                ("relchecks", T::Int2),
                ("relhasrules", T::Bool),
                ("relhastriggers", T::Bool),
                ("relhassubclass", T::Bool),
                ("relrowsecurity", T::Bool),
                ("relforcerowsecurity", T::Bool),
                ("relispopulated", T::Bool),
                ("relreplident", T::Text),
                ("relispartition", T::Bool),
                ("relrewrite", T::Oid),
                ("relacl", T::Text),
                ("reloptions", T::Array(Box::new(T::Text))),
                ("relpartbound", T::Text),
            ]);
            let row = |o: u32, n: &str, kind: &str, natts: usize, am: u32, hasindex: bool| {
                vec![
                    oid(o),
                    text(n),
                    oid(NS_PUBLIC),
                    oid(0),
                    oid(0),
                    oid(OWNER),
                    oid(am),
                    oid(o),
                    oid(0),
                    i4(0),
                    Datum::Float4(-1.0),
                    i4(0),
                    oid(0),
                    b(hasindex),
                    b(false),
                    text("p"),
                    text(kind),
                    i2(natts as i16),
                    i2(0),
                    b(kind == "v"),
                    b(false),
                    b(false),
                    b(false),
                    b(false),
                    b(true),
                    text("d"),
                    b(false),
                    oid(0),
                    NULL,
                    NULL,
                    NULL,
                ]
            };
            let mut rows = Vec::new();
            for (o, n, kind, cols, pk) in relations(cat) {
                let am = if kind == "v" { 0 } else { AM_HEAP };
                rows.push(row(o, &n, kind, cols.len(), am, pk));
                if pk {
                    rows.push(row(o + 1_000_000, &format!("{n}_pkey"), "i", 0, AM_BTREE, false));
                }
            }
            for o in cat.objects.values() {
                if let ObjectKind::Index(_) = &o.kind {
                    rows.push(row(rel_oid(o.id), &o.name, "i", 0, AM_BTREE, false));
                }
            }
            for x in rows {
                r.push(x);
            }
            r
        }
        "pg_attribute" => {
            let mut r = Rel::new(&[
                ("attrelid", T::Oid),
                ("attname", T::Name),
                ("atttypid", T::Oid),
                ("attlen", T::Int2),
                ("attnum", T::Int2),
                ("attndims", T::Int4),
                ("atttypmod", T::Int4),
                ("attbyval", T::Bool),
                ("attalign", T::Text),
                ("attstorage", T::Text),
                ("attnotnull", T::Bool),
                ("atthasdef", T::Bool),
                ("atthasmissing", T::Bool),
                ("attidentity", T::Text),
                ("attgenerated", T::Text),
                ("attisdropped", T::Bool),
                ("attislocal", T::Bool),
                ("attinhcount", T::Int2),
                ("attstattarget", T::Int2),
                ("attcompression", T::Text),
                ("attcollation", T::Oid),
                ("attacl", T::Text),
                ("attoptions", T::Array(Box::new(T::Text))),
            ]);
            for (o, _, _, cols, _) in relations(cat) {
                for (i, c) in cols.iter().enumerate() {
                    let (_, len, _) = type_info(&c.ty.ty);
                    r.push(vec![
                        oid(o),
                        text(&c.name),
                        oid(c.ty.ty.oid()),
                        i2(len),
                        i2(i as i16 + 1),
                        i4(matches!(c.ty.ty, T::Array(_)) as i32),
                        i4(c.ty.typmod),
                        b(len > 0 && len <= 8),
                        text("i"),
                        text(if len < 0 { "x" } else { "p" }),
                        b(!c.ty.nullable),
                        b(false),
                        b(false),
                        text(""),
                        text(""),
                        b(false),
                        b(true),
                        i2(0),
                        NULL,
                        text(""),
                        oid(collation_of(&c.ty.ty)),
                        NULL,
                        NULL,
                    ]);
                }
            }
            r
        }
        "pg_type" => {
            let mut r = Rel::new(&[
                ("oid", T::Oid),
                ("typname", T::Name),
                ("typnamespace", T::Oid),
                ("typowner", T::Oid),
                ("typlen", T::Int2),
                ("typbyval", T::Bool),
                ("typtype", T::Text),
                ("typcategory", T::Text),
                ("typispreferred", T::Bool),
                ("typisdefined", T::Bool),
                ("typdelim", T::Text),
                ("typrelid", T::Oid),
                ("typelem", T::Oid),
                ("typarray", T::Oid),
                ("typinput", T::Text),
                ("typoutput", T::Text),
                ("typnotnull", T::Bool),
                ("typbasetype", T::Oid),
                ("typtypmod", T::Int4),
                ("typndims", T::Int4),
                ("typcollation", T::Oid),
                ("typdefault", T::Text),
            ]);
            for t in BASE_TYPES {
                let (name, len, cat) = type_info(t);
                let arr = T::Array(Box::new(t.clone()));
                let preferred = matches!(t, T::Bool | T::Float8 | T::Text | T::TimestampTz | T::Interval | T::Oid);
                r.push(vec![
                    oid(t.oid()),
                    text(name),
                    oid(NS_PG_CATALOG),
                    oid(OWNER),
                    i2(len),
                    b(len > 0 && len <= 8),
                    text("b"),
                    text(cat),
                    b(preferred),
                    b(true),
                    text(","),
                    oid(0),
                    oid(0),
                    oid(arr.oid()),
                    text(&format!("{name}in")),
                    text(&format!("{name}out")),
                    b(false),
                    oid(0),
                    i4(-1),
                    i4(0),
                    oid(collation_of(t)),
                    NULL,
                ]);
                r.push(vec![
                    oid(arr.oid()),
                    text(&format!("_{name}")),
                    oid(NS_PG_CATALOG),
                    oid(OWNER),
                    i2(-1),
                    b(false),
                    text("b"),
                    text("A"),
                    b(false),
                    b(true),
                    text(","),
                    oid(0),
                    oid(t.oid()),
                    oid(0),
                    text("array_in"),
                    text("array_out"),
                    b(false),
                    oid(0),
                    i4(-1),
                    i4(0),
                    oid(collation_of(t)),
                    NULL,
                ]);
            }
            r
        }
        "pg_am" => {
            let mut r = Rel::new(&[("oid", T::Oid), ("amname", T::Name), ("amhandler", T::Text), ("amtype", T::Text)]);
            r.push(vec![oid(AM_HEAP), text("heap"), text("heap_tableam_handler"), text("t")]);
            r.push(vec![oid(AM_BTREE), text("btree"), text("bthandler"), text("i")]);
            r
        }
        "pg_database" => {
            let mut r = Rel::new(&[
                ("oid", T::Oid),
                ("datname", T::Name),
                ("datdba", T::Oid),
                ("encoding", T::Int4),
                ("datcollate", T::Text),
                ("datctype", T::Text),
                ("datistemplate", T::Bool),
                ("datallowconn", T::Bool),
                ("datconnlimit", T::Int4),
                ("datlocprovider", T::Text),
                ("datlocale", T::Text),
                ("daticurules", T::Text),
                ("datacl", T::Array(Box::new(T::Text))),
            ]);
            r.push(vec![
                oid(16384),
                text("ivm"),
                oid(OWNER),
                i4(6),
                text("C"),
                text("C"),
                b(false),
                b(true),
                i4(-1),
                text("c"),
                NULL,
                NULL,
                NULL,
            ]);
            r
        }
        "pg_roles" | "pg_authid" => {
            let mut r = Rel::new(&[
                ("oid", T::Oid),
                ("rolname", T::Name),
                ("rolsuper", T::Bool),
                ("rolinherit", T::Bool),
                ("rolcreaterole", T::Bool),
                ("rolcreatedb", T::Bool),
                ("rolcanlogin", T::Bool),
                ("rolreplication", T::Bool),
                ("rolconnlimit", T::Int4),
                ("rolbypassrls", T::Bool),
                ("rolpassword", T::Text),
                ("rolvaliduntil", T::TimestampTz),
                ("rolconfig", T::Array(Box::new(T::Text))),
            ]);
            r.push(vec![oid(OWNER), text("ivm"), b(true), b(true), b(true), b(true), b(true), b(false), i4(-1), b(true), text("********"), NULL, NULL]);
            r
        }
        "pg_user" => {
            let mut r = Rel::new(&[
                ("usename", T::Name),
                ("usesysid", T::Oid),
                ("usecreatedb", T::Bool),
                ("usesuper", T::Bool),
                ("userepl", T::Bool),
                ("usebypassrls", T::Bool),
            ]);
            r.push(vec![text("ivm"), oid(OWNER), b(true), b(true), b(false), b(true)]);
            r
        }
        "pg_settings" => {
            let mut r = Rel::new(&[("name", T::Text), ("setting", T::Text), ("unit", T::Text), ("category", T::Text), ("short_desc", T::Text)]);
            for (n, v) in [
                ("server_version", "17.0"),
                ("server_version_num", "170000"),
                ("server_encoding", "UTF8"),
                ("client_encoding", "UTF8"),
                ("DateStyle", "ISO, MDY"),
                ("TimeZone", "UTC"),
                ("integer_datetimes", "on"),
                ("standard_conforming_strings", "on"),
                ("max_identifier_length", "63"),
            ] {
                r.push(vec![text(n), text(v), NULL, text("Preset Options"), NULL]);
            }
            r
        }
        "pg_tablespace" => {
            let mut r = Rel::new(&[("oid", T::Oid), ("spcname", T::Name), ("spcowner", T::Oid)]);
            r.push(vec![oid(1663), text("pg_default"), oid(OWNER)]);
            r.push(vec![oid(1664), text("pg_global"), oid(OWNER)]);
            r
        }
        "pg_collation" => {
            let mut r = Rel::new(&[
                ("oid", T::Oid),
                ("collname", T::Name),
                ("collnamespace", T::Oid),
                ("collowner", T::Oid),
                ("collprovider", T::Text),
                ("collisdeterministic", T::Bool),
                ("collencoding", T::Int4),
            ]);
            r.push(vec![oid(COLL_DEFAULT), text("default"), oid(NS_PG_CATALOG), oid(OWNER), text("d"), b(true), i4(-1)]);
            r.push(vec![oid(950), text("C"), oid(NS_PG_CATALOG), oid(OWNER), text("c"), b(true), i4(-1)]);
            r
        }
        "pg_index" => {
            let mut r = Rel::new(&[
                ("indexrelid", T::Oid),
                ("indrelid", T::Oid),
                ("indnatts", T::Int2),
                ("indnkeyatts", T::Int2),
                ("indisunique", T::Bool),
                ("indnullsnotdistinct", T::Bool),
                ("indisprimary", T::Bool),
                ("indisexclusion", T::Bool),
                ("indimmediate", T::Bool),
                ("indisclustered", T::Bool),
                ("indisvalid", T::Bool),
                ("indcheckxmin", T::Bool),
                ("indisready", T::Bool),
                ("indislive", T::Bool),
                ("indisreplident", T::Bool),
                ("indkey", T::Array(Box::new(T::Int2))),
                ("indexprs", T::Text),
                ("indpred", T::Text),
            ]);
            let keyvec = |cols: &[usize]| {
                Datum::array(crate::types::ArrayValue::new_1d(T::Int2, cols.iter().map(|c| i2(*c as i16 + 1)).collect()))
            };
            for o in cat.objects.values() {
                match &o.kind {
                    ObjectKind::Table(t) if !t.pk.is_empty() => {
                        let n = t.pk.len() as i16;
                        r.push(vec![
                            oid(pkey_oid(o.id)),
                            oid(rel_oid(o.id)),
                            i2(n),
                            i2(n),
                            b(true),
                            b(false),
                            b(true),
                            b(false),
                            b(true),
                            b(false),
                            b(true),
                            b(false),
                            b(true),
                            b(true),
                            b(false),
                            keyvec(&t.pk),
                            NULL,
                            NULL,
                        ]);
                    }
                    ObjectKind::Index(ix) => {
                        let n = ix.columns.len() as i16;
                        r.push(vec![
                            oid(rel_oid(o.id)),
                            oid(rel_oid(ix.on)),
                            i2(n),
                            i2(n),
                            b(false),
                            b(false),
                            b(false),
                            b(false),
                            b(true),
                            b(false),
                            b(true),
                            b(false),
                            b(true),
                            b(true),
                            b(false),
                            keyvec(&ix.columns),
                            NULL,
                            NULL,
                        ]);
                    }
                    _ => {}
                }
            }
            r
        }
        "pg_views" => {
            let mut r = Rel::new(&[("schemaname", T::Name), ("viewname", T::Name), ("viewowner", T::Name), ("definition", T::Text)]);
            for o in cat.objects.values() {
                if let ObjectKind::View(v) = &o.kind {
                    r.push(vec![text("public"), text(&o.name), text("ivm"), text(&v.sql)]);
                }
            }
            r
        }
        "pg_matviews" => {
            let mut r = Rel::new(&[
                ("schemaname", T::Name),
                ("matviewname", T::Name),
                ("matviewowner", T::Name),
                ("tablespace", T::Name),
                ("hasindexes", T::Bool),
                ("ispopulated", T::Bool),
                ("definition", T::Text),
            ]);
            for o in cat.objects.values() {
                if let ObjectKind::MaterializedView(v) = &o.kind {
                    r.push(vec![text("public"), text(&o.name), text("ivm"), NULL, b(false), b(true), text(&v.sql)]);
                }
            }
            r
        }
        "pg_tables" => {
            let mut r = Rel::new(&[
                ("schemaname", T::Name),
                ("tablename", T::Name),
                ("tableowner", T::Name),
                ("tablespace", T::Name),
                ("hasindexes", T::Bool),
                ("hasrules", T::Bool),
                ("hastriggers", T::Bool),
                ("rowsecurity", T::Bool),
            ]);
            for (_, n, kind, _, pk) in relations(cat) {
                if kind == "r" {
                    r.push(vec![text("public"), text(&n), text("ivm"), NULL, b(pk), b(false), b(false), b(false)]);
                }
            }
            r
        }
        "pg_indexes" => {
            let mut r = Rel::new(&[("schemaname", T::Name), ("tablename", T::Name), ("indexname", T::Name), ("tablespace", T::Name), ("indexdef", T::Text)]);
            for o in cat.objects.values() {
                if let ObjectKind::Table(t) = &o.kind
                    && !t.pk.is_empty()
                {
                    let cols: Vec<&str> = t.pk.iter().map(|c| t.columns[*c].name.as_str()).collect();
                    let def = format!("CREATE UNIQUE INDEX {0}_pkey ON public.{0} USING btree ({1})", o.name, cols.join(", "));
                    r.push(vec![text("public"), text(&o.name), text(&format!("{}_pkey", o.name)), NULL, text(&def)]);
                }
            }
            r
        }
        // catalogs with no counterpart in the engine: present but empty
        "pg_attrdef" => Rel::new(&[("oid", T::Oid), ("adrelid", T::Oid), ("adnum", T::Int2), ("adbin", T::Text)]),
        "pg_constraint" => {
            let mut r = Rel::new(&[
                ("oid", T::Oid),
                ("conname", T::Name),
                ("connamespace", T::Oid),
                ("contype", T::Text),
                ("condeferrable", T::Bool),
                ("condeferred", T::Bool),
                ("convalidated", T::Bool),
                ("conrelid", T::Oid),
                ("conindid", T::Oid),
                ("conparentid", T::Oid),
                ("confrelid", T::Oid),
                ("conkey", T::Array(Box::new(T::Int2))),
                ("confkey", T::Array(Box::new(T::Int2))),
                ("conislocal", T::Bool),
                ("coninhcount", T::Int2),
                ("connoinherit", T::Bool),
            ]);
            for o in cat.objects.values() {
                if let ObjectKind::Table(t) = &o.kind
                    && !t.pk.is_empty()
                {
                    let key = Datum::array(crate::types::ArrayValue::new_1d(T::Int2, t.pk.iter().map(|c| i2(*c as i16 + 1)).collect()));
                    r.push(vec![
                        oid(pkey_con_oid(o.id)),
                        text(&format!("{}_pkey", o.name)),
                        oid(NS_PUBLIC),
                        text("p"),
                        b(false),
                        b(false),
                        b(true),
                        oid(rel_oid(o.id)),
                        oid(pkey_oid(o.id)),
                        oid(0),
                        oid(0),
                        key,
                        NULL,
                        b(true),
                        i2(0),
                        b(true),
                    ]);
                }
            }
            r
        }
        "pg_description" => Rel::new(&[("objoid", T::Oid), ("classoid", T::Oid), ("objsubid", T::Int4), ("description", T::Text)]),
        "pg_shdescription" => Rel::new(&[("objoid", T::Oid), ("classoid", T::Oid), ("description", T::Text)]),
        "pg_policy" => Rel::new(&[
            ("oid", T::Oid),
            ("polname", T::Name),
            ("polrelid", T::Oid),
            ("polcmd", T::Text),
            ("polpermissive", T::Bool),
            ("polroles", T::Array(Box::new(T::Oid))),
            ("polqual", T::Text),
            ("polwithcheck", T::Text),
        ]),
        "pg_statistic_ext" => Rel::new(&[
            ("oid", T::Oid),
            ("stxrelid", T::Oid),
            ("stxname", T::Name),
            ("stxnamespace", T::Oid),
            ("stxowner", T::Oid),
            ("stxstattarget", T::Int2),
            ("stxkind", T::Array(Box::new(T::Text))),
        ]),
        "pg_publication" => Rel::new(&[
            ("oid", T::Oid),
            ("pubname", T::Name),
            ("pubowner", T::Oid),
            ("puballtables", T::Bool),
            ("pubinsert", T::Bool),
            ("pubupdate", T::Bool),
            ("pubdelete", T::Bool),
            ("pubtruncate", T::Bool),
            ("pubviaroot", T::Bool),
        ]),
        "pg_publication_rel" => {
            Rel::new(&[("oid", T::Oid), ("prpubid", T::Oid), ("prrelid", T::Oid), ("prqual", T::Text), ("prattrs", T::Array(Box::new(T::Int2)))])
        }
        "pg_publication_namespace" => Rel::new(&[("oid", T::Oid), ("pnpubid", T::Oid), ("pnnspid", T::Oid)]),
        "pg_inherits" => {
            Rel::new(&[("inhrelid", T::Oid), ("inhparent", T::Oid), ("inhseqno", T::Int4), ("inhdetachpending", T::Bool)])
        }
        "pg_trigger" => Rel::new(&[("oid", T::Oid), ("tgrelid", T::Oid), ("tgname", T::Name), ("tgenabled", T::Text), ("tgisinternal", T::Bool)]),
        "pg_rewrite" => Rel::new(&[("oid", T::Oid), ("rulename", T::Name), ("ev_class", T::Oid), ("ev_type", T::Text)]),
        "pg_proc" => Rel::new(&[
            ("oid", T::Oid),
            ("proname", T::Name),
            ("pronamespace", T::Oid),
            ("proowner", T::Oid),
            ("prokind", T::Text),
            ("prorettype", T::Oid),
        ]),
        "pg_extension" => Rel::new(&[("oid", T::Oid), ("extname", T::Name), ("extowner", T::Oid), ("extnamespace", T::Oid), ("extversion", T::Text)]),
        "pg_enum" => Rel::new(&[("oid", T::Oid), ("enumtypid", T::Oid), ("enumsortorder", T::Float4), ("enumlabel", T::Name)]),
        "pg_foreign_table" => Rel::new(&[("ftrelid", T::Oid), ("ftserver", T::Oid)]),
        "pg_partitioned_table" => Rel::new(&[("partrelid", T::Oid), ("partstrat", T::Text), ("partnatts", T::Int2)]),
        "pg_sequence" => Rel::new(&[("seqrelid", T::Oid), ("seqtypid", T::Oid), ("seqstart", T::Int8), ("seqincrement", T::Int8)]),
        "pg_depend" => Rel::new(&[("classid", T::Oid), ("objid", T::Oid), ("objsubid", T::Int4), ("refclassid", T::Oid), ("refobjid", T::Oid), ("deptype", T::Text)]),
        "pg_event_trigger" => Rel::new(&[("oid", T::Oid), ("evtname", T::Name), ("evtevent", T::Name)]),
        "pg_stat_activity" => Rel::new(&[("datname", T::Name), ("pid", T::Int4), ("usename", T::Name), ("application_name", T::Text), ("state", T::Text), ("query", T::Text)]),
        _ => return None,
    })
}

/// Engine-specific relations.
fn ivm_schema(cat: &Catalog, name: &str) -> Option<Rel> {
    Some(match name {
        // the catalog is versioned by GSN: "what was the plan at g" is a query
        "catalog_history" => {
            let mut r = Rel::new(&[("gsn", T::Int8), ("statement", T::Text)]);
            for (g, sql) in &cat.history {
                r.push(vec![Datum::Int8(*g as i64), text(sql)]);
            }
            r
        }
        "objects" => {
            let mut r = Rel::new(&[("oid", T::Oid), ("name", T::Text), ("kind", T::Text), ("created_gsn", T::Int8), ("definition", T::Text)]);
            for o in cat.objects.values() {
                r.push(vec![oid(rel_oid(o.id)), text(&o.name), text(o.kind.name()), Datum::Int8(o.created_gsn as i64), text(&o.create_sql)]);
            }
            r
        }
        _ => return None,
    })
}

fn information_schema(cat: &Catalog, name: &str) -> Option<Rel> {
    Some(match name {
        "schemata" => {
            let mut r = Rel::new(&[("catalog_name", T::Name), ("schema_name", T::Name), ("schema_owner", T::Name)]);
            for s in ["pg_catalog", "public", "information_schema"] {
                r.push(vec![text("ivm"), text(s), text("ivm")]);
            }
            r
        }
        "tables" => {
            let mut r = Rel::new(&[
                ("table_catalog", T::Name),
                ("table_schema", T::Name),
                ("table_name", T::Name),
                ("table_type", T::Text),
                ("is_insertable_into", T::Text),
                ("is_typed", T::Text),
            ]);
            for (_, n, kind, _, _) in relations(cat) {
                // materialized views are the engine's main objects: list them
                let ty = if kind == "r" { "BASE TABLE" } else { "VIEW" };
                r.push(vec![text("ivm"), text("public"), text(&n), text(ty), text(if kind == "r" { "YES" } else { "NO" }), text("NO")]);
            }
            r
        }
        "views" => {
            let mut r = Rel::new(&[
                ("table_catalog", T::Name),
                ("table_schema", T::Name),
                ("table_name", T::Name),
                ("view_definition", T::Text),
                ("check_option", T::Text),
                ("is_updatable", T::Text),
                ("is_insertable_into", T::Text),
            ]);
            for o in cat.objects.values() {
                if let ObjectKind::View(v) | ObjectKind::MaterializedView(v) = &o.kind {
                    r.push(vec![text("ivm"), text("public"), text(&o.name), text(&v.sql), text("NONE"), text("NO"), text("NO")]);
                }
            }
            r
        }
        "columns" => {
            let mut r = Rel::new(&[
                ("table_catalog", T::Name),
                ("table_schema", T::Name),
                ("table_name", T::Name),
                ("column_name", T::Name),
                ("ordinal_position", T::Int4),
                ("column_default", T::Text),
                ("is_nullable", T::Text),
                ("data_type", T::Text),
                ("character_maximum_length", T::Int4),
                ("numeric_precision", T::Int4),
                ("numeric_scale", T::Int4),
                ("datetime_precision", T::Int4),
                ("udt_catalog", T::Name),
                ("udt_schema", T::Name),
                ("udt_name", T::Name),
                ("is_identity", T::Text),
                ("is_generated", T::Text),
                ("is_updatable", T::Text),
            ]);
            for (_, n, kind, cols, _) in relations(cat) {
                for (i, c) in cols.iter().enumerate() {
                    let (udt, _, _) = type_info(&c.ty.ty);
                    let udt = match &c.ty.ty {
                        T::Array(e) => format!("_{}", type_info(e).0),
                        _ => udt.to_string(),
                    };
                    let data_type = match &c.ty.ty {
                        T::Array(_) => "ARRAY".to_string(),
                        t => t.name(),
                    };
                    let (prec, scale) = match &c.ty.ty {
                        T::Numeric => match typmod::numeric_parts(c.ty.typmod) {
                            Some((p, s)) => (i4(p), i4(s)),
                            None => (NULL, NULL),
                        },
                        T::Int2 => (i4(16), i4(0)),
                        T::Int4 => (i4(32), i4(0)),
                        T::Int8 => (i4(64), i4(0)),
                        T::Float4 => (i4(24), NULL),
                        T::Float8 => (i4(53), NULL),
                        _ => (NULL, NULL),
                    };
                    let char_len = match &c.ty.ty {
                        T::Varchar | T::Bpchar if c.ty.typmod >= 4 => i4(c.ty.typmod - 4),
                        _ => NULL,
                    };
                    let dt_prec = match &c.ty.ty {
                        T::Time | T::Timestamp | T::TimestampTz | T::Interval => i4(if c.ty.typmod >= 0 { c.ty.typmod } else { 6 }),
                        T::Date => i4(0),
                        _ => NULL,
                    };
                    r.push(vec![
                        text("ivm"),
                        text("public"),
                        text(&n),
                        text(&c.name),
                        i4(i as i32 + 1),
                        NULL,
                        text(if c.ty.nullable { "YES" } else { "NO" }),
                        text(&data_type),
                        char_len,
                        prec,
                        scale,
                        dt_prec,
                        text("ivm"),
                        text("pg_catalog"),
                        text(&udt),
                        text("NO"),
                        text("NEVER"),
                        text(if kind == "r" { "YES" } else { "NO" }),
                    ]);
                }
            }
            r
        }
        _ => return None,
    })
}

/// OID -> text mappings for catalog functions and reg* casts that the
/// binder inlines (expressions cannot consult the catalog when evaluated).
pub fn definitions(cat: &Catalog, kind: &str) -> Vec<(u32, String)> {
    let mut out = Vec::new();
    let cols = |t: &crate::catalog::TableDef, idx: &[usize]| idx.iter().map(|c| t.columns[*c].name.clone()).collect::<Vec<_>>().join(", ");
    match kind {
        "regclass" => {
            for o in cat.objects.values() {
                match &o.kind {
                    ObjectKind::Table(t) => {
                        out.push((rel_oid(o.id), o.name.clone()));
                        if !t.pk.is_empty() {
                            out.push((pkey_oid(o.id), format!("{}_pkey", o.name)));
                        }
                    }
                    ObjectKind::MaterializedView(_) | ObjectKind::View(_) | ObjectKind::Index(_) => out.push((rel_oid(o.id), o.name.clone())),
                    _ => {}
                }
            }
        }
        "regtype" => {
            for t in BASE_TYPES {
                out.push((t.oid(), t.name()));
                let a = T::Array(Box::new(t.clone()));
                out.push((a.oid(), a.name()));
            }
        }
        "regnamespace" => {
            for (o, n) in [(NS_PG_CATALOG, "pg_catalog"), (99, "pg_toast"), (NS_PUBLIC, "public"), (NS_INFORMATION_SCHEMA, "information_schema")] {
                out.push((o, n.to_string()));
            }
        }
        "indexdef" => {
            for o in cat.objects.values() {
                match &o.kind {
                    ObjectKind::Table(t) if !t.pk.is_empty() => out.push((
                        pkey_oid(o.id),
                        format!("CREATE UNIQUE INDEX {0}_pkey ON public.{0} USING btree ({1})", o.name, cols(t, &t.pk)),
                    )),
                    ObjectKind::Index(ix) => {
                        if let Some(on) = cat.objects.get(&ix.on) {
                            let names = match &on.kind {
                                ObjectKind::Table(t) => cols(t, &ix.columns),
                                ObjectKind::MaterializedView(v) => ix.columns.iter().map(|c| v.columns[*c].name.clone()).collect::<Vec<_>>().join(", "),
                                _ => String::new(),
                            };
                            out.push((rel_oid(o.id), format!("CREATE INDEX {} ON public.{} USING btree ({names})", o.name, on.name)));
                        }
                    }
                    _ => {}
                }
            }
        }
        "constraintdef" => {
            for o in cat.objects.values() {
                if let ObjectKind::Table(t) = &o.kind
                    && !t.pk.is_empty()
                {
                    out.push((pkey_con_oid(o.id), format!("PRIMARY KEY ({})", cols(t, &t.pk))));
                }
            }
        }
        "viewdef" => {
            for o in cat.objects.values() {
                if let ObjectKind::MaterializedView(v) | ObjectKind::View(v) = &o.kind {
                    out.push((rel_oid(o.id), format!(" {};", v.sql)));
                }
            }
        }
        _ => {}
    }
    out
}

/// The name of a type OID as format_type() prints it.
pub fn format_type_oid(type_oid: u32, typmod: i32) -> Option<String> {
    SqlType::from_oid(type_oid).map(|t| format_type(&t, typmod))
}
