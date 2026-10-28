//! Catalog: definitions of tables, views, indexes, sources and sinks.
//!
//! Definitions are data (SQL text, column lists); dataflow plans are
//! re-derived from them, so the catalog plus the input log determines all
//! state. The catalog is kept in the metadata tree of the storage
//! environment (`coord::meta`), which is updated with each change.

use crate::sql::plan::ObjectId;
use crate::sql::{Catalog as BindCatalog, CatalogRelation};
use crate::storage::Gsn;
use crate::types::Column;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

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
    /// Change it with `insert` and `remove`, which record the change for
    /// the metadata tree.
    pub objects: BTreeMap<ObjectId, CatalogObject>,
    pub next_id: ObjectId,
    /// DDL history: (gsn, statement) — "what was the catalog at g" is a query.
    pub history: Vec<(Gsn, String)>,
    /// Random id of this data directory, part of default replication slot
    /// names (so that engines with similar catalogs do not share slots);
    /// chosen at the first CREATE SOURCE and logged with each.
    #[serde(default)]
    pub instance_id: Option<String>,
    /// Objects inserted or removed since the last `take_changed`.
    #[serde(skip)]
    changed: BTreeSet<ObjectId>,
    /// The objects by name, built when first needed and then kept by
    /// `insert` and `remove`: a lookup by name costs no scan of the catalog.
    #[serde(skip)]
    names: std::sync::OnceLock<NameIndex>,
}

/// The catalog's objects by name, and its replicated tables by upstream
/// name.
#[derive(Clone, Debug, Default)]
struct NameIndex {
    names: std::collections::HashMap<String, BTreeSet<ObjectId>>,
    upstream: std::collections::HashMap<(String, String), BTreeSet<ObjectId>>,
    /// Upstream schemas, with the number of tables from each.
    schemas: std::collections::HashMap<String, usize>,
}

impl NameIndex {
    fn add(&mut self, o: &CatalogObject) {
        self.names.entry(o.name.clone()).or_default().insert(o.id);
        if let ObjectKind::Table(TableDef { upstream: Some(u), .. }) = &o.kind {
            self.upstream.entry(u.clone()).or_default().insert(o.id);
            *self.schemas.entry(u.0.clone()).or_default() += 1;
        }
    }

    fn remove(&mut self, o: &CatalogObject) {
        fn drop_id<K: std::hash::Hash + Eq>(m: &mut std::collections::HashMap<K, BTreeSet<ObjectId>>, k: &K, id: ObjectId) {
            if let Some(ids) = m.get_mut(k) {
                ids.remove(&id);
                if ids.is_empty() {
                    m.remove(k);
                }
            }
        }
        drop_id(&mut self.names, &o.name, o.id);
        if let ObjectKind::Table(TableDef { upstream: Some(u), .. }) = &o.kind {
            drop_id(&mut self.upstream, u, o.id);
            if let Some(n) = self.schemas.get_mut(&u.0) {
                *n -= 1;
                if *n == 0 {
                    self.schemas.remove(&u.0);
                }
            }
        }
    }
}

impl Catalog {
    pub fn new() -> Catalog {
        Catalog { objects: BTreeMap::new(), next_id: 1000, history: Vec::new(), instance_id: None, changed: BTreeSet::new(), names: Default::default() }
    }

    pub fn alloc_id(&mut self) -> ObjectId {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn names(&self) -> &NameIndex {
        self.names.get_or_init(|| {
            crate::dataflow::bookkept(self.objects.len() as u64);
            let mut ix = NameIndex::default();
            for o in self.objects.values() {
                ix.add(o);
            }
            ix
        })
    }

    /// The object of a name (the oldest, should several have it).
    pub fn by_name(&self, name: &str) -> Option<&CatalogObject> {
        self.names().names.get(name).and_then(|ids| ids.first()).and_then(|id| self.objects.get(id))
    }

    /// Keep the name index (if built) as `id` leaves the catalog or changes.
    fn unname(&mut self, id: ObjectId) {
        if let (Some(ix), Some(o)) = (self.names.get_mut(), self.objects.get(&id)) {
            ix.remove(o);
        }
    }

    pub fn get(&self, id: ObjectId) -> Option<&CatalogObject> {
        self.objects.get(&id)
    }

    pub fn insert(&mut self, obj: CatalogObject) {
        self.changed.insert(obj.id);
        self.unname(obj.id);
        if let Some(ix) = self.names.get_mut() {
            ix.add(&obj);
        }
        self.objects.insert(obj.id, obj);
    }

    pub fn remove(&mut self, id: ObjectId) -> Option<CatalogObject> {
        self.changed.insert(id);
        self.unname(id);
        self.objects.remove(&id)
    }

    /// Ids of the objects inserted, replaced or removed since the last call.
    pub(crate) fn take_changed(&mut self) -> BTreeSet<ObjectId> {
        std::mem::take(&mut self.changed)
    }

    /// Relations a query reads as the binder resolves their names, including
    /// those of the views it expands; None when it does not bind.
    pub fn references(&self, sql: &str) -> Option<Vec<ObjectId>> {
        self.recorded_references(sql, true)
    }

    /// Relations a query names, as the binder resolves them: as
    /// `references`, but views are not expanded, so the relations read
    /// through them are left out (they are the views' own references).
    pub fn direct_references(&self, sql: &str) -> Option<Vec<ObjectId>> {
        self.recorded_references(sql, false)
    }

    fn recorded_references(&self, sql: &str, expand_views: bool) -> Option<Vec<ObjectId>> {
        struct Recording<'a> {
            cat: &'a Catalog,
            expand_views: bool,
            seen: std::cell::RefCell<Vec<ObjectId>>,
        }
        impl BindCatalog for Recording<'_> {
            fn definitions(&self, kind: &str) -> Vec<(u32, String)> {
                self.cat.definitions(kind)
            }
            fn resolve_relation(&self, schema: Option<&str>, name: &str) -> Option<CatalogRelation> {
                match self.cat.resolve_relation(schema, name) {
                    Some(CatalogRelation::View { id, name, columns, .. }) if !self.expand_views => {
                        // bound by its stored columns, without its query
                        self.seen.borrow_mut().push(id);
                        Some(CatalogRelation::Table { id, name, columns, keys: vec![] })
                    }
                    r => {
                        if let Some(CatalogRelation::Table { id, .. } | CatalogRelation::View { id, .. }) = &r {
                            self.seen.borrow_mut().push(*id);
                        }
                        r
                    }
                }
            }
        }
        let q = crate::sql::parser::parse_single_query(sql).ok()?;
        let rec = Recording { cat: self, expand_views, seen: Default::default() };
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
        let ix = self.names();
        if let Some(s) = schema
            && s != "public"
            && !ix.schemas.contains_key(s)
        {
            return None;
        }
        // the oldest object of the name, or replicated table of the
        // upstream name (when a schema is given)
        let named = ix.names.get(name).and_then(|ids| ids.first());
        let upstream = schema.and_then(|s| ix.upstream.get(&(s.to_string(), name.to_string()))).and_then(|ids| ids.first());
        let id = match (named, upstream) {
            (Some(a), Some(b)) => a.min(b),
            (a, b) => a.or(b)?,
        };
        let o = self.objects.get(id)?;
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
