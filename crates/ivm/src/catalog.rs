//! Catalog: definitions of tables, views, indexes, sources and sinks.
//!
//! Definitions are data (SQL text, column lists); dataflow plans are
//! re-derived from them, so the catalog plus the input log determines all
//! state. The catalog is persisted with every checkpoint.

use crate::sql::plan::ObjectId;
use crate::sql::{Catalog as BindCatalog, CatalogRelation};
use crate::storage::Gsn;
use crate::types::Column;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableDef {
    pub columns: Vec<Column>,
    /// Primary key columns (empty: multiset table keyed by all columns).
    pub pk: Vec<usize>,
    /// Source providing the data (None: local table written through SQL).
    pub source: Option<ObjectId>,
    /// Upstream (schema, table) for source tables.
    pub upstream: Option<(String, String)>,
    /// Bumped on every schema change.
    pub version: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ViewDef {
    pub sql: String,
    pub columns: Vec<Column>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexDef {
    pub on: ObjectId,
    /// Key columns of the indexed relation.
    pub columns: Vec<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceDef {
    pub conninfo: String,
    pub publication: String,
    pub slot: String,
    pub tables: Option<Vec<String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SinkDef {
    pub from: ObjectId,
    pub conninfo: String,
    pub table: String,
    pub key: Option<Vec<String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ObjectKind {
    Table(TableDef),
    MaterializedView(ViewDef),
    View(ViewDef),
    Index(IndexDef),
    Source(SourceDef),
    Sink(SinkDef),
}

impl ObjectKind {
    pub fn name(&self) -> &'static str {
        match self {
            ObjectKind::Table(_) => "table",
            ObjectKind::MaterializedView(_) => "materialized view",
            ObjectKind::View(_) => "view",
            ObjectKind::Index(_) => "index",
            ObjectKind::Source(_) => "source",
            ObjectKind::Sink(_) => "sink",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CatalogObject {
    pub id: ObjectId,
    pub name: String,
    pub kind: ObjectKind,
    pub created_gsn: Gsn,
    /// The original statement, for SHOW CREATE.
    pub create_sql: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Catalog {
    pub objects: BTreeMap<ObjectId, CatalogObject>,
    pub next_id: ObjectId,
    /// DDL history: (gsn, statement) — "what was the catalog at g" is a query.
    pub history: Vec<(Gsn, String)>,
    /// Random id of this data directory, part of default replication slot
    /// names (so that engines with similar catalogs do not share slots);
    /// chosen at the first CREATE SOURCE and logged with each.
    #[serde(default)]
    pub instance_id: Option<String>,
}

impl Catalog {
    pub fn new() -> Catalog {
        Catalog { objects: BTreeMap::new(), next_id: 1000, history: Vec::new(), instance_id: None }
    }

    pub fn alloc_id(&mut self) -> ObjectId {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    pub fn by_name(&self, name: &str) -> Option<&CatalogObject> {
        self.objects.values().find(|o| o.name == name)
    }

    pub fn get(&self, id: ObjectId) -> Option<&CatalogObject> {
        self.objects.get(&id)
    }

    pub fn insert(&mut self, obj: CatalogObject) {
        self.objects.insert(obj.id, obj);
    }

    pub fn remove(&mut self, id: ObjectId) -> Option<CatalogObject> {
        self.objects.remove(&id)
    }

    /// Relations a query reads as the binder resolves their names, including
    /// those of the views it expands; None when it does not bind.
    pub fn references(&self, sql: &str) -> Option<Vec<ObjectId>> {
        struct Recording<'a> {
            cat: &'a Catalog,
            seen: std::cell::RefCell<Vec<ObjectId>>,
        }
        impl BindCatalog for Recording<'_> {
            fn definitions(&self, kind: &str) -> Vec<(u32, String)> {
                self.cat.definitions(kind)
            }
            fn resolve_relation(&self, schema: Option<&str>, name: &str) -> Option<CatalogRelation> {
                let r = self.cat.resolve_relation(schema, name);
                if let Some(CatalogRelation::Table { id, .. } | CatalogRelation::View { id, .. }) = &r {
                    self.seen.borrow_mut().push(*id);
                }
                r
            }
        }
        let q = crate::sql::parser::parse_single_query(sql).ok()?;
        let rec = Recording { cat: self, seen: Default::default() };
        let mut b = crate::sql::Binder::new(&rec);
        // a stored definition: tolerate what earlier versions accepted
        b.stored_definition = true;
        b.bind_query(&q).ok()?;
        Some(rec.seen.into_inner())
    }

    /// Does the query `sql` read object `id` (by name, conservatively, if
    /// it does not bind)?
    pub fn query_reads(&self, sql: &str, id: ObjectId) -> bool {
        match self.references(sql) {
            Some(refs) => refs.contains(&id),
            None => self.get(id).is_some_and(|o| sql_mentions(sql, &o.name)),
        }
    }

    /// (Non-materialized) views `sql` reads: queries inline them, so their
    /// planned form no longer shows them.
    pub fn mentioned_views(&self, sql: &str) -> Vec<ObjectId> {
        let refs = self.references(sql);
        self.objects
            .values()
            .filter(|o| matches!(o.kind, ObjectKind::View(_)))
            .filter(|o| match &refs {
                Some(r) => r.contains(&o.id),
                None => sql_mentions(sql, &o.name),
            })
            .map(|o| o.id)
            .collect()
    }

    /// Objects that depend on `id` (views referencing it, indexes on it,
    /// sinks from it, tables of a source).
    pub fn dependents(&self, id: ObjectId) -> Vec<ObjectId> {
        if self.get(id).is_none() {
            return vec![];
        }
        let mut out = Vec::new();
        for o in self.objects.values() {
            let dep = match &o.kind {
                ObjectKind::Index(i) => i.on == id,
                ObjectKind::Sink(s) => s.from == id,
                ObjectKind::Table(t) => t.source == Some(id),
                // the relations it reads, not every identifier equal to the name
                ObjectKind::MaterializedView(v) | ObjectKind::View(v) => self.query_reads(&v.sql, id),
                ObjectKind::Source(_) => false,
            };
            if dep && o.id != id {
                out.push(o.id);
            }
        }
        out
    }
}

/// Conservative test whether a view's SQL references a relation name.
pub(crate) fn sql_mentions(sql: &str, name: &str) -> bool {
    let lower = sql.to_lowercase();
    lower.split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '"')).any(|w| w.trim_matches('"') == name)
}

impl BindCatalog for Catalog {
    fn definitions(&self, kind: &str) -> Vec<(u32, String)> {
        crate::sysrel::definitions(self, kind)
    }

    fn resolve_relation(&self, schema: Option<&str>, name: &str) -> Option<CatalogRelation> {
        if let Some((columns, rows)) = crate::sysrel::system_relation(self, schema, name) {
            return Some(CatalogRelation::Constant { name: name.to_string(), columns, rows });
        }
        if let Some(s) = schema
            && s != "public"
            && !self.objects.values().any(|o| matches!(&o.kind, ObjectKind::Table(t) if t.upstream.as_ref().is_some_and(|(us, _)| us == s)))
        {
            return None;
        }
        let o = self.objects.values().find(|o| {
            o.name == name
                || matches!(&o.kind, ObjectKind::Table(t) if schema.is_some() && t.upstream.as_ref().is_some_and(|(us, un)| Some(us.as_str()) == schema && un == name))
        })?;
        match &o.kind {
            ObjectKind::Table(t) => Some(CatalogRelation::Table {
                id: o.id,
                name: o.name.clone(),
                columns: t.columns.clone(),
                keys: if t.pk.is_empty() { vec![] } else { vec![t.pk.clone()] },
            }),
            ObjectKind::MaterializedView(v) => {
                Some(CatalogRelation::Table { id: o.id, name: o.name.clone(), columns: v.columns.clone(), keys: vec![] })
            }
            ObjectKind::View(v) => {
                Some(CatalogRelation::View { id: o.id, name: o.name.clone(), columns: v.columns.clone(), sql: v.sql.clone() })
            }
            _ => None,
        }
    }
}
