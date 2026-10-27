//! Durable metadata: the catalog and its DDL history, the dataflow's error
//! state, source states and resume LSNs, the clock's last tick, the map
//! from arrangement fingerprints to trees, and pending slot drops.
//!
//! They live in a storage tree of their own, the *metadata tree*, as
//! entries of a weighted multiset like an arrangement's: a record is one
//! entry `tag ++ key ++ value` with weight 1 (an error record: weighted by
//! its count), and a change retracts the old entry and adds the new one at
//! the change's GSN. So a checkpoint writes the few leaves that changed,
//! and its manifest record carries a small header naming the tree
//! (`encode_header`) instead of the whole state.
//!
//! The in-memory structures (`Catalog`, `Engine::source_states`, the
//! graph's error records, ...) stay authoritative while the engine runs;
//! `Engine::persist_meta` writes what changed since its previous call: after
//! every DDL or schema record and at every checkpoint.
//! Recovery reads the tree back (`load`).
//!
//! The data directory's format version is in its `FORMAT` file
//! (`read_format`). Format 1, written before there was one, kept all of
//! this as JSON in every checkpoint record (`AppState`); such a directory
//! is migrated on its first start (`coord::start`).

use super::Engine;
use crate::catalog::{Catalog, CatalogObject};
use crate::dataflow::{ErrDelta, ErrorKey, ErrorState};
use crate::error::{codes, Result, SqlError};
use crate::source::SlotDrop;
use crate::sql::plan::ObjectId;
use crate::storage::{CountSummarizer, Env, Gsn, TreeId};
use crate::types::codec::{get_bytes, get_ivarint, get_str, get_u8, get_uvarint, put_bytes, put_ivarint, put_str, put_uvarint};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

/// The on-disk format this code writes (see docs/on-disk-format.md,
/// "Compatibility"). It reads every earlier one.
pub const FORMAT_VERSION: u32 = 2;

// Record tags of the metadata tree (the first byte of every entry).
/// `u64 BE id` → `CatalogObject` (JSON)
const T_OBJECT: u8 = 1;
/// `u8 field` → value: 1 next object id (`uvarint`), 2 instance id (`str`)
const T_CATALOG: u8 = 2;
/// `u64 BE gsn` → the statement (UTF-8)
const T_HISTORY: u8 = 3;
/// `u64 BE source` → its `SourceState` (JSON; written without the tables'
/// entries, which are tag 12's, but read with them too)
const T_SOURCE_STATE: u8 = 4;
/// `u64 BE source` → resume LSN (`uvarint`)
const T_SOURCE_LSN: u8 = 5;
/// `bytes fingerprint` → `uvarint tree`, `uvarint since` (written by earlier
/// versions; read, the `since` ignored, and replaced by tag 13)
const T_ARRANGEMENT_SINCE: u8 = 6;
/// `bytes fingerprint`, `str sqlstate`, `str message`, weighted by the count
const T_NODE_ERROR: u8 = 7;
/// `bytes fingerprint`, `bytes group key`, `str sqlstate`, `str message`
const T_GROUP_ERROR: u8 = 8;
/// `u64 BE gsn` → clock (`ivarint`): the last tick (earlier versions kept
/// the ticks of their retention window)
const T_TICK: u8 = 9;
/// `str conninfo`, `str slot`
const T_SLOT_DROP: u8 = 10;
/// `bytes fingerprint`: the operator's groups in error are complete
const T_GROUPS_KNOWN: u8 = 11;
/// `u64 BE source`, `u64 BE table` → the table's `SnapState` in the
/// source's state (JSON)
const T_SOURCE_TABLE: u8 = 12;
/// `bytes fingerprint` → `uvarint tree`
const T_ARRANGEMENT: u8 = 13;

const F_NEXT_ID: u8 = 1;
const F_INSTANCE_ID: u8 = 2;

/// An error record: (SQLSTATE, message).
type ErrRecord = (String, String);

/// The application state of a checkpoint record of format 1 (JSON; see
/// docs/on-disk-format.md), read to migrate it. Also the form in which
/// recovery receives the state read from the metadata tree.
#[derive(Serialize, Deserialize, Default)]
pub(crate) struct AppState {
    pub catalog: Catalog,
    pub trees: Vec<(Vec<u8>, TreeId)>,
    pub view_errors: Vec<(ObjectId, ErrDelta)>,
    pub source_states: Vec<(ObjectId, String)>,
    #[serde(default)]
    pub source_lsns: Vec<(ObjectId, u64)>,
    pub next_req: u64,
    /// Logical clock (timestamptz microseconds) at the checkpoint.
    #[serde(default)]
    pub clock: i64,
    /// Error records of arranged dataflow nodes (absent in older checkpoints).
    #[serde(default)]
    pub errors: ErrorState,
    /// Recent (gsn, clock) ticks, oldest first (absent in older
    /// checkpoints): the last one is the clock's.
    #[serde(default)]
    pub ticks: Vec<(Gsn, i64)>,
    /// Replication slots of dropped sources still to be dropped.
    #[serde(default)]
    pub slot_drops: Vec<SlotDrop>,
}

/// What the metadata tree holds, as far as the next `persist_meta` needs to
/// know it to write only changes.
#[derive(Default)]
pub(crate) struct Meta {
    /// The metadata tree (created by the first `persist_meta`).
    pub tree: Option<TreeId>,
    /// Every entry of the records kept whole (catalog scalars, source
    /// states and LSNs, arrangements, the last tick, slot drops), by key.
    keyed: BTreeMap<Vec<u8>, Vec<u8>>,
    /// GSNs of the first and last history entries (0: none).
    history: (Gsn, Gsn),
    /// The tree does not hold the catalog's objects yet: write them all.
    all_objects: bool,
    /// The arrangement and source-state records are to be brought up to
    /// date as a whole (a new tree, or the first write after recovery);
    /// afterwards only those that changed are visited.
    all_arrangements: bool,
    all_sources: bool,
}

fn u64_key(tag: u8, v: u64) -> Vec<u8> {
    let mut k = vec![tag];
    k.extend_from_slice(&v.to_be_bytes());
    k
}

/// The entry of a keyed record: its key followed by its value.
fn entry(mut key: Vec<u8>, value: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
    value(&mut key);
    key
}

fn error_entry(k: &ErrorKey) -> Vec<u8> {
    let mut e = Vec::new();
    match k {
        ErrorKey::Node(fp, (code, msg)) => {
            e.push(T_NODE_ERROR);
            put_bytes(&mut e, fp);
            put_str(&mut e, code);
            put_str(&mut e, msg);
        }
        ErrorKey::Group(fp, group, (code, msg)) => {
            e.push(T_GROUP_ERROR);
            put_bytes(&mut e, fp);
            put_bytes(&mut e, group);
            put_str(&mut e, code);
            put_str(&mut e, msg);
        }
        ErrorKey::Known(fp) => {
            e.push(T_GROUPS_KNOWN);
            put_bytes(&mut e, fp);
        }
    }
    e
}

/// Retract every entry of `tree` in `[lo, hi)`.
fn retract_range(env: &mut Env, tree: TreeId, lo: &[u8], hi: &[u8], ups: &mut Vec<(Vec<u8>, i64)>) -> Result<()> {
    env.for_each(tree, Bound::Included(lo), Bound::Excluded(hi), &mut |k, h| {
        let w = h.total();
        if w != 0 {
            ups.push((k.to_vec(), -w));
        }
        Ok(true)
    })
}

impl Meta {
    /// Bring the records of `tag` kept whole to `desired` (key → entry).
    fn sync_keyed(&mut self, tag: u8, desired: BTreeMap<Vec<u8>, Vec<u8>>, ups: &mut Vec<(Vec<u8>, i64)>) {
        self.sync_prefix(&[tag], desired, ups)
    }

    /// Bring the records kept whole whose keys start with `prefix` to
    /// `desired` (key → entry).
    fn sync_prefix(&mut self, prefix: &[u8], desired: BTreeMap<Vec<u8>, Vec<u8>>, ups: &mut Vec<(Vec<u8>, i64)>) {
        let stale: Vec<Vec<u8>> = self
            .keyed
            .range(prefix.to_vec()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .inspect(|_| crate::dataflow::bookkept(1))
            .filter(|(k, e)| desired.get(*k) != Some(*e))
            .map(|(k, _)| k.clone())
            .collect();
        crate::dataflow::bookkept(desired.len() as u64);
        for k in stale {
            ups.push((self.keyed.remove(&k).expect("stale entry"), -1));
        }
        for (k, e) in desired {
            if let std::collections::btree_map::Entry::Vacant(v) = self.keyed.entry(k) {
                ups.push((e.clone(), 1));
                v.insert(e);
            }
        }
    }

    /// Bring the record kept whole under `key` to `entry` (None: none).
    fn sync_one(&mut self, key: Vec<u8>, entry: Option<Vec<u8>>, ups: &mut Vec<(Vec<u8>, i64)>) {
        crate::dataflow::bookkept(1);
        if self.keyed.get(&key) == entry.as_ref() {
            return;
        }
        if let Some(old) = self.keyed.remove(&key) {
            ups.push((old, -1));
        }
        if let Some(e) = entry {
            ups.push((e.clone(), 1));
            self.keyed.insert(key, e);
        }
    }
}

#[cfg(test)]
impl Meta {
    /// The arrangement records the tree holds: (fingerprint, tree).
    pub(crate) fn arrangement_records(&self) -> Vec<(Vec<u8>, TreeId)> {
        self.keyed
            .iter()
            .filter(|(k, _)| k[0] == T_ARRANGEMENT)
            .map(|(k, e)| {
                let b = &mut &k[1..];
                let fp = get_bytes(b).unwrap().to_vec();
                (fp, get_uvarint(&mut &e[k.len()..]).unwrap())
            })
            .collect()
    }
}

/// The key of a source table's entry (tag 12).
fn source_table_key(source: ObjectId, table: ObjectId) -> Vec<u8> {
    let mut k = u64_key(T_SOURCE_TABLE, source);
    k.extend_from_slice(&table.to_be_bytes());
    k
}

/// The key of an arrangement's record (tag 13).
fn arrangement_key(fp: &[u8]) -> Vec<u8> {
    let mut k = vec![T_ARRANGEMENT];
    put_bytes(&mut k, fp);
    k
}

/// An arrangement's record: its tree.
fn arrangement_entry(fp: &[u8], tree: TreeId) -> Vec<u8> {
    entry(arrangement_key(fp), |e| put_uvarint(e, tree))
}

impl Engine {
    /// The metadata tree, created if there is none yet.
    fn meta_tree(&mut self) -> TreeId {
        match self.meta.tree {
            Some(t) => t,
            None => {
                let t = self.env.create_tree(Arc::new(CountSummarizer));
                self.meta = Meta { tree: Some(t), all_objects: true, all_arrangements: true, all_sources: true, ..Default::default() };
                t
            }
        }
    }

    /// Write the metadata that changed since the previous call into the
    /// metadata tree, at `gsn` (the last applied GSN).
    pub(crate) fn persist_meta(&mut self, gsn: Gsn) -> Result<()> {
        let tree = self.meta_tree();
        let mut ups: Vec<(Vec<u8>, i64)> = Vec::new();
        // catalog objects: the stored entry of each changed one (whatever
        // the code that wrote it encoded) is replaced
        let mut changed = self.catalog.take_changed();
        if std::mem::take(&mut self.meta.all_objects) {
            changed.extend(self.catalog.objects.keys().copied());
        }
        for id in changed {
            let key = u64_key(T_OBJECT, id);
            self.env.for_each_prefix(tree, &key, &mut |k, h| {
                let w = h.total();
                if w != 0 {
                    ups.push((k.to_vec(), -w));
                }
                Ok(true)
            })?;
            if let Some(o) = self.catalog.get(id) {
                let json = serde_json::to_vec(o).map_err(|e| SqlError::internal(e.to_string()))?;
                ups.push((entry(key, |e| e.extend_from_slice(&json)), 1));
            }
        }
        let mut scalars = BTreeMap::new();
        let k = vec![T_CATALOG, F_NEXT_ID];
        scalars.insert(k.clone(), entry(k, |e| put_uvarint(e, self.catalog.next_id)));
        if let Some(inst) = &self.catalog.instance_id {
            let k = vec![T_CATALOG, F_INSTANCE_ID];
            scalars.insert(k.clone(), entry(k, |e| put_str(e, inst)));
        }
        self.meta.sync_keyed(T_CATALOG, scalars, &mut ups);
        // the history: appended at its end, trimmed at its start
        let hist = &self.catalog.history;
        let (first, last) = self.meta.history;
        for (g, sql) in hist.iter().rev().take_while(|(g, _)| *g > last) {
            ups.push((entry(u64_key(T_HISTORY, *g), |e| e.extend_from_slice(sql.as_bytes())), 1));
        }
        let front = hist.first().map(|h| h.0).unwrap_or(Gsn::MAX);
        if first != 0 && first < front {
            retract_range(&mut self.env, tree, &u64_key(T_HISTORY, first), &u64_key(T_HISTORY, front), &mut ups)?;
        }
        self.meta.history = (hist.first().map(|h| h.0).unwrap_or(0), hist.last().map(|h| h.0).unwrap_or(0));
        // the clock: its last tick (none before the first)
        let mut ticks = BTreeMap::new();
        if self.clock_gsn != 0 {
            let k = u64_key(T_TICK, self.clock_gsn);
            ticks.insert(k.clone(), entry(k, |e| put_ivarint(e, self.clock)));
        }
        self.meta.sync_keyed(T_TICK, ticks, &mut ups);
        // sources: their states' heads, and the entries of their tables
        // that changed (`Engine::source_changed`)
        let keyed = |tag: u8, id: u64, value: &dyn Fn(&mut Vec<u8>)| {
            let k = u64_key(tag, id);
            (k.clone(), entry(k, value))
        };
        let changed = std::mem::take(&mut self.source_changes);
        if std::mem::take(&mut self.meta.all_sources) {
            let heads = self.source_states.iter().map(|(id, s)| keyed(T_SOURCE_STATE, *id, &|e| e.extend_from_slice(s.head().to_json().as_bytes()))).collect();
            self.meta.sync_keyed(T_SOURCE_STATE, heads, &mut ups);
            let mut tables = BTreeMap::new();
            for (src, st) in &self.source_states {
                for (t, snap) in &st.tables {
                    let k = source_table_key(*src, *t);
                    tables.insert(k.clone(), entry(k, |e| e.extend_from_slice(serde_json::to_string(snap).unwrap().as_bytes())));
                }
            }
            self.meta.sync_keyed(T_SOURCE_TABLE, tables, &mut ups);
        } else {
            for (src, tables) in changed {
                let st = self.source_states.get(&src);
                let head = st.map(|s| entry(u64_key(T_SOURCE_STATE, src), |e| e.extend_from_slice(s.head().to_json().as_bytes())));
                self.meta.sync_one(u64_key(T_SOURCE_STATE, src), head, &mut ups);
                let table_entry = |t: ObjectId| {
                    let snap = st.and_then(|s| s.tables.get(&t))?;
                    Some(entry(source_table_key(src, t), |e| e.extend_from_slice(serde_json::to_string(snap).unwrap().as_bytes())))
                };
                match tables {
                    Some(ts) => {
                        for t in ts {
                            let e = table_entry(t);
                            self.meta.sync_one(source_table_key(src, t), e, &mut ups);
                        }
                    }
                    None => {
                        let desired = st.into_iter().flat_map(|s| s.tables.keys()).map(|t| (source_table_key(src, *t), table_entry(*t).unwrap())).collect();
                        self.meta.sync_prefix(&u64_key(T_SOURCE_TABLE, src), desired, &mut ups);
                    }
                }
            }
        }
        let lsns = self.source_lsns.iter().map(|(id, l)| keyed(T_SOURCE_LSN, *id, &|e| put_uvarint(e, *l))).collect();
        self.meta.sync_keyed(T_SOURCE_LSN, lsns, &mut ups);
        // arrangements: those that changed (all of them at first, replacing
        // the records of earlier versions, which carried a `since`)
        let changes = self.graph.take_arrangement_changes();
        if std::mem::take(&mut self.meta.all_arrangements) {
            let arrs = self.graph.arrangement_trees().into_iter().map(|(fp, t)| (arrangement_key(&fp), arrangement_entry(&fp, t))).collect();
            self.meta.sync_keyed(T_ARRANGEMENT, arrs, &mut ups);
            self.meta.sync_keyed(T_ARRANGEMENT_SINCE, BTreeMap::new(), &mut ups);
        } else {
            for (fp, rec) in changes {
                let e = rec.map(|t| arrangement_entry(&fp, t));
                self.meta.sync_one(arrangement_key(&fp), e, &mut ups);
            }
        }
        // pending slot drops (also removed by their threads)
        let drops = self
            .shared
            .slot_drops
            .lock()
            .iter()
            .map(|d| {
                let mut k = vec![T_SLOT_DROP];
                put_str(&mut k, &d.conninfo);
                put_str(&mut k, &d.slot);
                (k.clone(), k)
            })
            .collect();
        self.meta.sync_keyed(T_SLOT_DROP, drops, &mut ups);
        // the error state, as changes
        for (k, w) in self.graph.take_error_changes() {
            ups.push((error_entry(&k), w));
        }
        self.env.apply(tree, &mut ups, gsn)
    }
}

/// The header that a checkpoint record of the current format carries as
/// its application state: the format, the metadata tree, the request
/// counter.
pub(crate) fn encode_header(meta: &Meta, next_req: u64) -> Vec<u8> {
    let mut out = Vec::new();
    put_uvarint(&mut out, FORMAT_VERSION as u64);
    put_uvarint(&mut out, meta.tree.expect("metadata tree"));
    put_uvarint(&mut out, next_req);
    out
}

/// The state a checkpoint record's application state describes: the JSON
/// state of format 1 (`Meta::default()`: the tree holds nothing yet), or
/// the header of the current format and what the metadata tree it names
/// holds (that tree is attached).
pub(crate) fn load(env: &mut Env, app: &[u8]) -> Result<(AppState, Meta)> {
    if app.first() == Some(&b'{') {
        let app: AppState =
            serde_json::from_slice(app).map_err(|e| SqlError::corrupt(format!("cannot decode checkpoint state: {e}")))?;
        return Ok((app, Meta::default()));
    }
    let b = &mut &app[..];
    let format = get_uvarint(b)?;
    if format > FORMAT_VERSION as u64 {
        return Err(newer_format(format));
    }
    if format != FORMAT_VERSION as u64 {
        return Err(SqlError::corrupt(format!("bad checkpoint header: format {format}")));
    }
    let tree = get_uvarint(b)?;
    let next_req = get_uvarint(b)?;
    env.attach_tree(tree, Arc::new(CountSummarizer))?;
    let mut entries = Vec::new();
    env.for_each(tree, Bound::Unbounded, Bound::Unbounded, &mut |k, h| {
        let w = h.total();
        if w != 0 {
            entries.push((k.to_vec(), w));
        }
        Ok(true)
    })?;
    let mut st = AppState { next_req, ..Default::default() };
    let mut meta = Meta { tree: Some(tree), ..Default::default() };
    let mut next_id = None;
    let mut node_errors: BTreeMap<Vec<u8>, ErrDelta> = BTreeMap::new();
    let mut group_errors: BTreeMap<Vec<u8>, Vec<(Vec<u8>, ErrRecord)>> = BTreeMap::new();
    let mut known = Vec::new();
    let mut sources: BTreeMap<ObjectId, crate::source::postgres::SourceState> = BTreeMap::new();
    for (e, w) in entries {
        let bad = |what: &str| SqlError::corrupt(format!("bad metadata entry ({what}): {e:02x?}"));
        let tag = e[0];
        let mut b = &e[1..];
        if tag != T_NODE_ERROR && w != 1 {
            return Err(bad(&format!("weight {w}")));
        }
        let be64 = |b: &mut &[u8]| -> Result<u64> {
            let (v, rest) = b.split_first_chunk::<8>().ok_or_else(|| bad("key"))?;
            *b = rest;
            Ok(u64::from_be_bytes(*v))
        };
        let key_len = match tag {
            T_OBJECT => {
                let id = be64(&mut b)?;
                let o: CatalogObject = serde_json::from_slice(b).map_err(|err| bad(&format!("catalog object {id}: {err}")))?;
                if o.id != id || st.catalog.objects.insert(id, o).is_some() {
                    return Err(bad("catalog object"));
                }
                b = &[];
                None
            }
            T_CATALOG => {
                let field = get_u8(&mut b)?;
                match field {
                    F_NEXT_ID => next_id = Some(get_uvarint(&mut b)?),
                    F_INSTANCE_ID => st.catalog.instance_id = Some(get_str(&mut b)?.to_string()),
                    _ => return Err(bad("catalog field")),
                }
                Some(2)
            }
            T_HISTORY => {
                let g = be64(&mut b)?;
                st.catalog.history.push((g, String::from_utf8(b.to_vec()).map_err(|_| bad("statement"))?));
                b = &[];
                None
            }
            T_SOURCE_STATE => {
                let id = be64(&mut b)?;
                let json = std::str::from_utf8(b).map_err(|_| bad("source state"))?;
                let state = sources.entry(id).or_default();
                let tables = std::mem::take(&mut state.tables);
                *state = crate::source::postgres::SourceState::parse(json);
                state.tables.extend(tables);
                b = &[];
                Some(9)
            }
            T_SOURCE_TABLE => {
                let (src, table) = (be64(&mut b)?, be64(&mut b)?);
                let snap: crate::source::postgres::SnapState = serde_json::from_slice(b).map_err(|err| bad(&format!("source table {table}: {err}")))?;
                sources.entry(src).or_default().tables.insert(table, snap);
                b = &[];
                Some(17)
            }
            T_SOURCE_LSN => {
                let id = be64(&mut b)?;
                st.source_lsns.push((id, get_uvarint(&mut b)?));
                Some(9)
            }
            T_ARRANGEMENT | T_ARRANGEMENT_SINCE => {
                let fp = get_bytes(&mut b)?.to_vec();
                let n = e.len() - b.len();
                st.trees.push((fp, get_uvarint(&mut b)?));
                if tag == T_ARRANGEMENT_SINCE {
                    // (the GSN the tree's history started at: none is kept)
                    get_uvarint(&mut b)?;
                }
                Some(n)
            }
            T_NODE_ERROR => {
                let fp = get_bytes(&mut b)?.to_vec();
                let rec = (get_str(&mut b)?.to_string(), get_str(&mut b)?.to_string());
                node_errors.entry(fp).or_default().push((rec, w));
                None
            }
            T_GROUP_ERROR => {
                let fp = get_bytes(&mut b)?.to_vec();
                let group = get_bytes(&mut b)?.to_vec();
                let rec = (get_str(&mut b)?.to_string(), get_str(&mut b)?.to_string());
                group_errors.entry(fp).or_default().push((group, rec));
                None
            }
            T_GROUPS_KNOWN => {
                known.push(get_bytes(&mut b)?.to_vec());
                None
            }
            T_TICK => {
                st.ticks.push((be64(&mut b)?, get_ivarint(&mut b)?));
                Some(9)
            }
            T_SLOT_DROP => {
                let conninfo = get_str(&mut b)?.to_string();
                let slot = get_str(&mut b)?.to_string();
                st.slot_drops.push(SlotDrop { conninfo, slot });
                Some(e.len())
            }
            _ => return Err(bad("tag")),
        };
        if !b.is_empty() {
            return Err(bad("trailing bytes"));
        }
        if let Some(n) = key_len
            && meta.keyed.insert(e[..n].to_vec(), e.clone()).is_some()
        {
            return Err(bad("duplicate key"));
        }
    }
    st.catalog.next_id = next_id.ok_or_else(|| SqlError::corrupt("the metadata tree has no next object id"))?;
    st.source_states = sources.into_iter().map(|(id, s)| (id, s.to_json())).collect();
    // (the first write after recovery brings these up to date as a whole)
    (meta.all_arrangements, meta.all_sources) = (true, true);
    // (the clock is the last tick's: earlier versions kept more)
    st.clock = st.ticks.last().map(|t| t.1).unwrap_or(0);
    st.errors = ErrorState { nodes: node_errors.into_iter().collect(), operators: group_errors.into_iter().collect(), known };
    meta.history = (st.catalog.history.first().map(|h| h.0).unwrap_or(0), st.catalog.history.last().map(|h| h.0).unwrap_or(0));
    Ok((st, meta))
}

fn newer_format(format: u64) -> SqlError {
    SqlError::new(
        codes::FEATURE_NOT_SUPPORTED,
        format!("the data directory has format {format}, which this version of ivm cannot read (it reads formats up to {FORMAT_VERSION})"),
    )
}

/// The format version recorded in the data directory's `FORMAT` file, or
/// None if there is none: a new directory, or one written before format
/// versions existed (format 1). A newer format than this code reads is
/// refused (nothing is modified).
pub fn read_format(dir: &Path) -> Result<Option<u32>> {
    let text = match std::fs::read_to_string(dir.join("FORMAT")) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let v: u32 = text
        .trim()
        .parse()
        .map_err(|_| SqlError::corrupt(format!("{} does not hold a format version: {text:?}", dir.join("FORMAT").display())))?;
    if v > FORMAT_VERSION {
        return Err(newer_format(v as u64));
    }
    Ok(Some(v))
}

/// Does the data directory hold an input log or a store (written by any
/// version)?
pub fn holds_data(dir: &Path) -> Result<bool> {
    for sub in ["log", "store"] {
        match std::fs::read_dir(dir.join(sub)) {
            Ok(mut it) => {
                if it.next().is_some() {
                    return Ok(true);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(false)
}

/// Record the current format version in the data directory (atomically:
/// written to `FORMAT.new`, synced, renamed, and the directory synced).
pub fn write_format(dir: &Path) -> Result<()> {
    let tmp = dir.join("FORMAT.new");
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(format!("{FORMAT_VERSION}\n").as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, dir.join("FORMAT"))?;
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

/// Change the catalog objects stored in a stopped engine's data directory
/// (for tests and repairs): `f` edits them, and the change is written as a
/// new checkpoint record at the same GSN.
pub fn edit_stored_catalog(data_dir: &Path, f: impl FnOnce(&mut BTreeMap<ObjectId, CatalogObject>)) -> Result<()> {
    let (mut env, rec) = Env::open(&data_dir.join("store"), Default::default())?;
    let rec = rec.ok_or_else(|| SqlError::internal("the store has no checkpoint"))?;
    let (st, meta) = load(&mut env, &rec.app)?;
    let tree = meta.tree.ok_or_else(|| SqlError::internal("the checkpoint is of format 1"))?;
    let mut objects = st.catalog.objects;
    f(&mut objects);
    let mut ups = Vec::new();
    retract_range(&mut env, tree, &[T_OBJECT], &[T_OBJECT + 1], &mut ups)?;
    for (id, o) in &objects {
        let json = serde_json::to_vec(o).map_err(|e| SqlError::internal(e.to_string()))?;
        ups.push((entry(u64_key(T_OBJECT, *id), |e| e.extend_from_slice(&json)), 1));
    }
    env.apply(tree, &mut ups, rec.gsn)?;
    env.checkpoint(rec.gsn, &encode_header(&meta, st.next_req))
}
