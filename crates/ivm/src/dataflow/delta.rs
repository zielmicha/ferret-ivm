//! Per-GSN deltas in bounded memory.
//!
//! A node's output delta at a GSN is held in memory while it is small and
//! spilled beyond that into temporary trees: one per arrangement of the
//! node, in the arrangement's encoding (one representation, again), or an
//! index by all columns for a node without arrangements. Equal rows add up
//! there, so each tree is the consolidated delta, sorted by the
//! arrangement's entry key: a key prefix of the arrangement is a range of
//! it. Consumers read the delta in batches, and can read it by key range in
//! the encoding of any of the node's arrangements (`for_each_entry`) until
//! they have all run. The spilled trees are merged into the arrangements in
//! key order. The output of a node that nobody reads goes to its
//! arrangements directly. Sets of affected keys (groups, partitions,
//! values) spill likewise.

use super::*;
use crate::memory::{heap_block, row_heap, Charge};
use crate::storage::{CountSummarizer, TreeBuilder};
use std::ops::Bound;

/// Memory of rows or keys a delta or key set holds before it spills (and of
/// a table's changes applied at once): a buffer of the working memory
/// (`Work::buffer`, within 1 and 256 MiB; it spills sooner while the
/// buffers together exceed their share, see `Work::full`), or
/// `IVM_DELTA_SPILL_BYTES` (0 spills everything; the tests use it to run
/// every operator on spilled input).
pub fn spill_bytes(env: &Env) -> usize {
    static V: std::sync::OnceLock<Option<usize>> = std::sync::OnceLock::new();
    V.get_or_init(|| std::env::var("IVM_DELTA_SPILL_BYTES").ok().and_then(|v| v.parse().ok()))
        .unwrap_or_else(|| env.work().buffer(1 << 20, 256 << 20))
}

/// Memory of a row in a delta: its place in the vector and what it owns.
pub fn row_mem(row: &Row) -> usize {
    std::mem::size_of::<(Row, i64)>() + row_heap(row)
}

/// Receives a delta's rows a batch at a time.
pub type BatchFn<'a> = dyn FnMut(&mut Env, &[(Row, i64)]) -> Result<()> + 'a;

/// Receives a delta's entries in some encoding (false: stop).
pub type EntryFn<'a> = dyn FnMut(&[u8], i64) -> Result<bool> + 'a;

/// An encoding a node's delta is kept in: an arrangement of the node (with
/// the tree to apply the delta to, unless that is done already), or an
/// index by all columns.
#[derive(Clone)]
pub struct Target {
    pub arr: Option<ArrId>,
    pub layout: Arc<Layout>,
    pub apply_to: Option<TreeId>,
}

/// The index by all columns that spilled rows of `schema` are kept in.
pub fn spill_layout(schema: &[ColumnType]) -> Layout {
    Layout::new((0..schema.len()).map(Expr::Column).collect(), vec![SortSpec::ASC; schema.len()], schema, vec![])
}

/// A consolidated delta.
pub enum Rows {
    Mem(Delta),
    /// Spilled: per target, a temporary tree of the delta in the target's
    /// encoding (`rows`: the number of rows spilled, at least the number of
    /// entries of each tree).
    Tree { trees: Vec<TreeId>, rows: usize },
    /// Applied to the arrangements of a node nobody reads (`rows` spilled).
    Applied { rows: usize },
}

impl Default for Rows {
    fn default() -> Self {
        Rows::Mem(Vec::new())
    }
}

impl Rows {
    pub fn len(&self) -> usize {
        match self {
            Rows::Mem(d) => d.len(),
            Rows::Tree { rows, .. } | Rows::Applied { rows } => *rows,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Entries in some encoding with their weights, sorted.
type Entries = Vec<(Vec<u8>, i64)>;

/// A node's output at a GSN: its consolidated delta (in the encodings of
/// `targets` when spilled) and error records.
#[derive(Default)]
pub struct NodeDelta {
    pub rows: Rows,
    pub errs: ErrDelta,
    pub targets: Vec<Target>,
    /// Per target, an in-memory delta's entries in its encoding, sorted
    /// (computed when first read by key).
    sorted: Vec<std::sync::OnceLock<Entries>>,
    /// The working memory an in-memory delta takes, until it is dropped.
    _charge: Charge,
}

impl NodeDelta {
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty() && self.errs.is_empty()
    }

    /// Hand the rows to `f` in batches (in memory: all at once).
    pub fn for_each_batch(&self, env: &mut Env, f: &mut BatchFn) -> Result<()> {
        let tree = match &self.rows {
            Rows::Mem(d) => return if d.is_empty() { Ok(()) } else { f(env, d) },
            Rows::Tree { trees, .. } => trees[0],
            Rows::Applied { .. } => return Err(SqlError::internal("reading a delta applied to its arrangements")),
        };
        let layout = &self.targets[0].layout;
        let mut from: Option<Vec<u8>> = None;
        loop {
            let lo = match &from {
                Some(k) => Bound::Excluded(k.as_slice()),
                None => Bound::Unbounded,
            };
            let mut batch = Vec::new();
            let mut bytes = 0;
            let mut last = None;
            env.for_each(tree, lo, Bound::Unbounded, &mut |k, h| {
                batch.push((layout.decode_row(k)?, h.total()));
                bytes += k.len();
                if batch.len() >= ops::BATCH_ROWS || bytes >= 1 << 20 {
                    last = Some(k.to_vec());
                    return Ok(false);
                }
                Ok(true)
            })?;
            if !batch.is_empty() {
                f(env, &batch)?;
            }
            match last {
                Some(k) => from = Some(k),
                None => return Ok(()),
            }
        }
    }

    /// The delta's entries in the encoding of target `i`, from `lo` while
    /// below `hi`, in key order, until `f` returns false: what a consumer
    /// overlays on that arrangement of the node to read it as it was
    /// before this GSN.
    pub fn for_each_entry(&self, env: &mut Env, i: usize, lo: Bound<&[u8]>, hi: Bound<&[u8]>, f: &mut EntryFn) -> Result<()> {
        match &self.rows {
            Rows::Mem(d) => {
                let sorted = match self.sorted[i].get() {
                    Some(s) => s,
                    None => {
                        let layout = &self.targets[i].layout;
                        let mut v = Vec::with_capacity(d.len());
                        for (row, w) in d {
                            v.push((layout.encode_row(row)?, *w));
                        }
                        v.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                        self.sorted[i].get_or_init(|| v)
                    }
                };
                let start = match lo {
                    Bound::Unbounded => 0,
                    Bound::Included(k) => sorted.partition_point(|e| e.0.as_slice() < k),
                    Bound::Excluded(k) => sorted.partition_point(|e| e.0.as_slice() <= k),
                };
                for (k, w) in &sorted[start..] {
                    let below = match hi {
                        Bound::Unbounded => true,
                        Bound::Included(h) => k.as_slice() <= h,
                        Bound::Excluded(h) => k.as_slice() < h,
                    };
                    if !below || !f(k, *w)? {
                        break;
                    }
                }
                Ok(())
            }
            Rows::Tree { trees, .. } => env.for_each(trees[i], lo, hi, &mut |k, h| f(k, h.total())),
            Rows::Applied { .. } => Err(SqlError::internal("reading a delta applied to its arrangements")),
        }
    }

    /// Apply the delta to the targets' trees (a spilled one by merging each
    /// of its trees in key order).
    pub fn apply(&self, env: &mut Env, gsn: Gsn) -> Result<()> {
        match &self.rows {
            Rows::Mem(d) => {
                if d.is_empty() {
                    return Ok(());
                }
                for t in &self.targets {
                    let Some(tree) = t.apply_to else { continue };
                    let mut ups = Vec::with_capacity(d.len());
                    for (row, w) in d {
                        ups.push((t.layout.encode_row(row)?, *w));
                    }
                    env.apply(tree, &mut ups, gsn)?;
                }
                Ok(())
            }
            Rows::Tree { trees, .. } => {
                for (t, from) in self.targets.iter().zip(trees) {
                    let Some(into) = t.apply_to else { continue };
                    merge_into(env, *from, into, gsn)?;
                }
                Ok(())
            }
            Rows::Applied { .. } => Ok(()),
        }
    }

    /// All rows, in memory.
    pub fn to_vec(&self, env: &mut Env) -> Result<Delta> {
        match &self.rows {
            Rows::Mem(d) => Ok(d.clone()),
            _ => {
                let mut out = Vec::with_capacity(self.rows.len());
                self.for_each_batch(env, &mut |_, b| {
                    out.extend_from_slice(b);
                    Ok(())
                })?;
                Ok(out)
            }
        }
    }

    /// Drop the temporary trees, if any.
    pub fn release(self, env: &mut Env) -> Result<()> {
        if let Rows::Tree { trees, .. } = self.rows {
            for t in trees {
                env.drop_tree(t)?;
            }
        }
        Ok(())
    }
}

/// Apply the entries of tree `from` to tree `into` in key order, in
/// batches of about 1 MiB (each covering the next key range).
fn merge_into(env: &mut Env, from: TreeId, into: TreeId, gsn: Gsn) -> Result<()> {
    let mut resume: Option<Vec<u8>> = None;
    loop {
        let lo = match &resume {
            Some(k) => Bound::Excluded(k.as_slice()),
            None => Bound::Unbounded,
        };
        let mut batch = Vec::new();
        let mut bytes = 0;
        let mut last = None;
        env.for_each(from, lo, Bound::Unbounded, &mut |k, h| {
            batch.push((k.to_vec(), h.total()));
            bytes += k.len() + 40;
            if bytes >= 1 << 20 {
                last = Some(k.to_vec());
                return Ok(false);
            }
            Ok(true)
        })?;
        env.apply(into, &mut batch, gsn)?;
        match last {
            Some(k) => resume = Some(k),
            None => return Ok(()),
        }
    }
}

/// Finish every builder (aborting the rest after a failure).
pub fn finish_all(env: &mut Env, builders: impl IntoIterator<Item = TreeBuilder>) -> Result<()> {
    let mut r = Ok(());
    for b in builders {
        if r.is_ok() {
            r = b.finish(env);
        } else {
            b.abort(env)?;
        }
    }
    r
}

/// A delta being produced: rows pushed in any order, consolidated by
/// `finish`. Past `spill_bytes` they go through a `TreeBuilder` per
/// target: into temporary trees, or, for a node nobody reads (`direct`),
/// into the targets' trees.
pub struct DeltaBuf {
    rows: Delta,
    /// The rows' memory, charged to the working memory once rows arrive.
    charge: Charge,
    gsn: Gsn,
    targets: Vec<Target>,
    direct: bool,
    /// Once spilled: per target, its builder and temporary tree.
    spill: Option<Vec<(TreeBuilder, Option<TreeId>)>>,
    spilled: usize,
    /// The rows in memory need no consolidation (pushed by one `extend` of
    /// a consolidated delta).
    consolidated: bool,
    extends: usize,
}

impl DeltaBuf {
    /// A delta kept, in memory or in temporary trees in the encodings of
    /// `targets`, until the consumers have read it.
    pub fn new(gsn: Gsn, targets: Vec<Target>) -> DeltaBuf {
        DeltaBuf { rows: Vec::new(), charge: Charge::none(), gsn, targets, direct: false, spill: None, spilled: 0, consolidated: false, extends: 0 }
    }

    /// The output of a node nobody reads: past the spill threshold, rows
    /// go to the targets' trees directly.
    pub fn direct(gsn: Gsn, targets: Vec<Target>) -> DeltaBuf {
        DeltaBuf { rows: Vec::new(), charge: Charge::none(), gsn, targets, direct: true, spill: None, spilled: 0, consolidated: false, extends: 0 }
    }

    pub fn push(&mut self, env: &mut Env, row: Row, w: i64) -> Result<()> {
        if let Some(spill) = &mut self.spill {
            self.spilled += 1;
            for (t, (b, _)) in self.targets.iter().zip(spill.iter_mut()) {
                b.push(env, t.layout.encode_row(&row)?, w)?;
            }
            return Ok(());
        }
        if self.rows.is_empty() {
            self.charge = env.work().charge();
        }
        self.charge.grow(row_mem(&row));
        self.rows.push((row, w));
        if env.work().full(self.charge.bytes(), spill_bytes(env)) {
            if self.direct {
                self.targets.retain(|t| t.apply_to.is_some());
            }
            let mut spill = Vec::with_capacity(self.targets.len());
            for t in &self.targets {
                match t.apply_to {
                    Some(tree) if self.direct => spill.push((TreeBuilder::new(env, tree, self.gsn), None)),
                    _ => {
                        let tree = env.create_temp_tree(Arc::new(CountSummarizer));
                        spill.push((TreeBuilder::new(env, tree, self.gsn), Some(tree)));
                    }
                }
            }
            self.spill = Some(spill);
            let held = std::mem::take(&mut self.charge);
            for (row, w) in std::mem::take(&mut self.rows) {
                self.push(env, row, w)?;
            }
            drop(held);
        }
        Ok(())
    }

    pub fn extend(&mut self, env: &mut Env, rows: Delta) -> Result<()> {
        for (r, w) in rows {
            self.push(env, r, w)?;
        }
        Ok(())
    }

    /// `extend` with a consolidated delta: if it is the only one, the rows
    /// need no consolidation (a table's delta, applied in one chunk).
    pub fn extend_consolidated(&mut self, env: &mut Env, rows: Delta) -> Result<()> {
        self.extends += 1;
        self.consolidated = self.extends == 1 && self.rows.is_empty();
        self.extend(env, rows)
    }

    /// The consolidated delta and its targets.
    fn finish(mut self, env: &mut Env) -> Result<(Rows, Vec<Target>)> {
        let Some(spill) = self.spill.take() else {
            if !self.consolidated {
                consolidate(&mut self.rows);
            }
            return Ok((Rows::Mem(std::mem::take(&mut self.rows)), std::mem::take(&mut self.targets)));
        };
        let (builders, trees): (Vec<TreeBuilder>, Vec<Option<TreeId>>) = spill.into_iter().unzip();
        let r = finish_all(env, builders);
        if self.direct {
            return r.map(|_| (Rows::Applied { rows: self.spilled }, self.targets));
        }
        let trees: Vec<TreeId> = trees.into_iter().flatten().collect();
        if r.is_err() || trees.iter().all(|t| env.tree_is_empty(*t)) {
            // (or everything cancelled out)
            for t in trees {
                env.drop_tree(t)?;
            }
            return r.map(|_| (Rows::Mem(Vec::new()), self.targets));
        }
        Ok((Rows::Tree { trees, rows: self.spilled }, self.targets))
    }

    /// Drop what was spilled into temporary trees (after a failure).
    pub fn abort(mut self, env: &mut Env) -> Result<()> {
        if let Some(spill) = self.spill.take() {
            for (b, tree) in spill {
                b.abort(env)?;
                if let Some(t) = tree {
                    env.drop_tree(t)?;
                }
            }
        }
        Ok(())
    }

    /// The node's output (without error records).
    pub fn into_delta(mut self, env: &mut Env) -> Result<NodeDelta> {
        let charge = std::mem::take(&mut self.charge);
        let (rows, targets) = self.finish(env)?;
        let sorted = targets.iter().map(|_| std::sync::OnceLock::new()).collect();
        // (an in-memory delta keeps its charge)
        let charge = if matches!(rows, Rows::Mem(_)) { charge } else { Charge::none() };
        Ok(NodeDelta { rows, errs: Vec::new(), targets, sorted, _charge: charge })
    }
}

/// A node's output being computed by its operator: rows (a `DeltaBuf`)
/// and error records.
pub struct StepOut {
    pub rows: DeltaBuf,
    pub errs: ErrDelta,
    errs_limit: usize,
}

impl StepOut {
    pub fn new(rows: DeltaBuf) -> StepOut {
        StepOut { rows, errs: Vec::new(), errs_limit: 1024 }
    }

    pub fn push(&mut self, env: &mut Env, row: Row, w: i64) -> Result<()> {
        self.rows.push(env, row, w)
    }

    /// Add an error record, consolidating the records as they accumulate
    /// (every row of a large delta may fail).
    pub fn err(&mut self, e: (String, String), w: i64) {
        self.errs.push((e, w));
        if self.errs.len() >= self.errs_limit {
            consolidate_errs(&mut self.errs);
            self.errs_limit = (2 * self.errs.len()).max(1024);
        }
    }

    /// The node's output: consolidated rows and error records.
    pub fn finish(self, env: &mut Env) -> Result<NodeDelta> {
        let mut d = self.rows.into_delta(env)?;
        d.errs = self.errs;
        consolidate_errs(&mut d.errs);
        Ok(d)
    }
}

/// Distinct keys collected in any order, visited in key order: sorted in
/// memory while small, else through a `TreeBuilder` into a temporary tree.
pub struct KeySet {
    keys: Vec<Vec<u8>>,
    /// The keys' memory, charged to the working memory once keys arrive.
    charge: Charge,
    gsn: Gsn,
    spill: Option<(TreeId, TreeBuilder)>,
}

/// The keys of a `KeySet`, in order.
pub enum Keys {
    Mem(Vec<Vec<u8>>),
    Tree(TreeId),
}

impl KeySet {
    pub fn new(gsn: Gsn) -> KeySet {
        KeySet { keys: Vec::new(), charge: Charge::none(), gsn, spill: None }
    }

    pub fn push(&mut self, env: &mut Env, key: Vec<u8>) -> Result<()> {
        if let Some((_, b)) = &mut self.spill {
            return b.push(env, key, 1);
        }
        if self.keys.is_empty() {
            self.charge = env.work().charge();
        }
        self.charge.grow(std::mem::size_of::<Vec<u8>>() + heap_block(key.len()));
        self.keys.push(key);
        if env.work().full(self.charge.bytes(), spill_bytes(env)) {
            let tree = env.create_temp_tree(Arc::new(CountSummarizer));
            let mut b = TreeBuilder::new(env, tree, self.gsn);
            let held = std::mem::take(&mut self.charge);
            for k in std::mem::take(&mut self.keys) {
                b.push(env, k, 1)?;
            }
            drop(held);
            self.spill = Some((tree, b));
        }
        Ok(())
    }

    /// Drop what was spilled (after a failure).
    pub fn abort(self, env: &mut Env) -> Result<()> {
        if let Some((tree, b)) = self.spill {
            b.abort(env)?;
            env.drop_tree(tree)?;
        }
        Ok(())
    }

    pub fn finish(mut self, env: &mut Env) -> Result<Keys> {
        match self.spill.take() {
            None => {
                self.keys.sort();
                self.keys.dedup();
                Ok(Keys::Mem(self.keys))
            }
            Some((tree, b)) => {
                if let Err(e) = b.finish(env) {
                    env.drop_tree(tree)?;
                    return Err(e);
                }
                Ok(Keys::Tree(tree))
            }
        }
    }
}

impl Keys {
    /// Visit the keys in order, then drop the temporary tree (also when `f`
    /// fails).
    pub fn for_each(self, env: &mut Env, f: &mut dyn FnMut(&mut Env, &[u8]) -> Result<()>) -> Result<()> {
        match self {
            Keys::Mem(keys) => {
                for k in &keys {
                    f(env, k)?;
                }
                Ok(())
            }
            Keys::Tree(tree) => {
                let mut c = env.cursor(tree, Bound::Unbounded, Bound::Unbounded);
                let r = (|| {
                    while let Some((k, _)) = env.cursor_next(&mut c)? {
                        f(env, &k)?;
                    }
                    Ok(())
                })();
                env.drop_tree(tree)?;
                r
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::EnvOptions;
    use crate::testkit::{Rng, TempDir};
    use crate::types::{Datum, SqlType};
    use std::collections::BTreeMap;

    /// A delta small enough for memory and one that spills read alike: in
    /// batches, and by key range in the encoding of an arrangement (the
    /// spilled one from its tree in that encoding), and they apply alike.
    #[test]
    fn spilled_and_in_memory_deltas_read_alike() {
        let tmp = TempDir::new("delta-spill");
        let (mut env, _) = Env::open(tmp.path(), EnvOptions { cache_bytes: 1 << 20, ..Default::default() }).unwrap();
        let schema = vec![ColumnType::new(SqlType::Int8), ColumnType::new(SqlType::Int8)];
        // an arrangement keyed by the second column
        let by_b = Arc::new(Layout::new(vec![Expr::Column(1)], vec![SortSpec::ASC], &schema, vec![]));
        let mut rng = Rng::new(7);
        for n in [100, 40_000] {
            let arr = env.create_tree(Arc::new(CountSummarizer));
            let targets = vec![Target { arr: Some(0), layout: by_b.clone(), apply_to: Some(arr) }];
            let mut buf = DeltaBuf::new(5, targets);
            let mut model: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
            for _ in 0..n {
                let a = rng.below(n / 2) as i64;
                let row = vec![Datum::Int8(a), Datum::Int8(a % 97)];
                let w = if rng.chance(30) { -1 } else { 1 };
                *model.entry(by_b.encode_row(&row).unwrap()).or_default() += w;
                buf.push(&mut env, row, w).unwrap();
            }
            model.retain(|_, w| *w != 0);
            let d = buf.into_delta(&mut env).unwrap();
            assert_eq!(matches!(d.rows, Rows::Tree { .. }), n > 100, "spilled with {n} rows");
            // in batches: every row once, consolidated
            let mut rows: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
            d.for_each_batch(&mut env, &mut |_, b| {
                for (row, w) in b {
                    assert!(rows.insert(by_b.encode_row(row).unwrap(), *w).is_none(), "row read twice");
                }
                Ok(())
            })
            .unwrap();
            assert!(rows == model, "rows of the delta of {n}");
            // by key prefix, in the arrangement's encoding
            for b in [0i64, 13, 96] {
                let prefix = by_b.encode_prefix(&[Datum::Int8(b)]);
                let hi = crate::types::encoding::prefix_successor(&prefix).unwrap();
                let mut got = Vec::new();
                d.for_each_entry(&mut env, 0, Bound::Included(&prefix), Bound::Excluded(&hi), &mut |k, w| {
                    got.push((k.to_vec(), w));
                    Ok(true)
                })
                .unwrap();
                let want: Vec<(Vec<u8>, i64)> = model.iter().filter(|(k, _)| k.starts_with(&prefix)).map(|(k, w)| (k.clone(), *w)).collect();
                assert_eq!(got, want, "entries of key {b} in the delta of {n}");
            }
            d.apply(&mut env, 5).unwrap();
            let applied: BTreeMap<Vec<u8>, i64> =
                env.scan(arr, Bound::Unbounded, Bound::Unbounded).unwrap().into_iter().map(|(k, h)| (k.to_vec(), h.total())).collect();
            assert!(applied == model, "applied delta of {n}");
            d.release(&mut env).unwrap();
            env.drop_tree(arr).unwrap();
            assert!(env.tree_ids().is_empty(), "temporary trees left behind");
        }
    }
}
