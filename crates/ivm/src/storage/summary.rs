//! Monoid summaries carried by every tree node over its subtree.
//!
//! COUNT, SUM, MIN, MAX per group become range queries over an index whose
//! nodes are augmented with these summaries. Summaries are maintained
//! incrementally: counts and sums are invertible (numeric sums count their
//! special values and display scales apart, so removal is exact), and
//! MIN/MAX fold insertions directly. Only when a MIN/MAX loses its extreme
//! (or a tie between different representations of equal values makes the
//! leftmost one ambiguous) is a node's summary recomputed from its contents.
//! An incrementally maintained summary is always identical to a recomputed
//! one.
//!
//! States stay small whatever the data: a float sum is a normalized dyadic
//! rational, usually inline, and a node summary represents a MIN/MAX value
//! too wide for it by a prefix of its key encoding ([`EXTREME_PREFIX`]),
//! which range queries resolve to the value from the entries.

use crate::error::{Result, SqlError};
use crate::types::codec::{self, get_ivarint, get_u8, get_uvarint, put_ivarint, put_uvarint};
use crate::types::Datum;
use crate::types::datum::exact_cmp;
use crate::types::encoding::{encode_key, SortSpec, TAG_VALUE};
use num_bigint::{BigInt, BigUint, Sign};
use num_traits::{Signed, ToPrimitive, Zero};
use pgnumeric::Numeric;
use smallvec::SmallVec;
use std::cell::RefCell;
use std::cmp::Ordering;

/// Bytes of a MIN/MAX value's key encoding that a node summary holds at
/// most. A value whose encoding is longer is represented there by the first
/// `EXTREME_PREFIX` bytes of it ([`WideExtreme`]): the key encoding orders
/// like the values and no encoding is a prefix of another, so such prefixes
/// order like the values too, except that equal prefixes may stand for
/// different values.
pub const EXTREME_PREFIX: usize = 128;

/// A MIN/MAX value too wide for a node summary, as the summary holds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WideExtreme {
    /// The first [`EXTREME_PREFIX`] bytes of the value's key encoding.
    pub prefix: Box<[u8]>,
    /// The SHA-256 of the value's exact (codec) encoding when every entry
    /// that the summary covers and whose value has `prefix` has that value,
    /// which is then found at the first such entry; `None` when they differ.
    pub same: Option<[u8; 32]>,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Summary {
    /// Sum of current weights of all entries.
    pub count: i64,
    pub aggs: Vec<AggState>,
}

impl Summary {
    pub fn empty(spec: &[AggKind]) -> Summary {
        Summary { count: 0, aggs: spec.iter().map(|k| k.empty()).collect() }
    }

    pub fn combine(&mut self, other: &Summary) {
        self.count += other.count;
        for (a, b) in self.aggs.iter_mut().zip(other.aggs.iter()) {
            a.combine(b);
        }
    }

    /// Update a node's summary for one child whose summary `old` was replaced
    /// by the summaries `new` of the nodes that took its place (in key
    /// order). False when a MIN/MAX cannot be updated in place and the
    /// summary must be recomputed from the children.
    pub fn replace_child(&mut self, old: &Summary, new: &[&Summary]) -> bool {
        self.count -= old.count;
        for n in new {
            self.count += n.count;
        }
        for i in 0..self.aggs.len() {
            match extreme_kind(&self.aggs[i]) {
                Some(less) => {
                    // the extreme of the replacement nodes together
                    let mut b = extreme_state(less, None);
                    for n in new {
                        b.combine(&n.aggs[i]);
                    }
                    if !replace_extreme(less, &mut self.aggs[i], old.aggs[i].ext(), b.ext()) {
                        return false;
                    }
                }
                None => {
                    let st = &mut self.aggs[i];
                    st.subtract(&old.aggs[i]);
                    for n in new {
                        st.combine(&n.aggs[i]);
                    }
                }
            }
        }
        true
    }

    /// Equality including the representation of MIN/MAX values and of
    /// numeric sums (as finalized), not just SQL equality.
    pub fn exact_eq(&self, other: &Summary) -> bool {
        self.count == other.count
            && self.aggs.len() == other.aggs.len()
            && self.aggs.iter().zip(&other.aggs).all(|(a, b)| match (a, b) {
                (AggState::Min(x), AggState::Min(y)) | (AggState::Max(x), AggState::Max(y)) => match (x, y) {
                    (Some(x), Some(y)) => exact_cmp(x, y) == Ordering::Equal,
                    (None, None) => true,
                    _ => false,
                },
                (AggState::SumNumeric(x), AggState::SumNumeric(y)) => x == y && x.value().to_string() == y.value().to_string(),
                _ => a == b,
            })
    }

    /// Replace every MIN/MAX value too wide for a node summary by its prefix
    /// (see [`EXTREME_PREFIX`]): the form in which node summaries hold them.
    /// The result claims that no other value covered by the summary shares
    /// the prefix (`WideExtreme::same`), so this must be applied whenever a
    /// value may have become the extreme: after each entry folded or
    /// updated, once an extreme is wide.
    pub fn bound_extremes(&mut self) {
        for a in &mut self.aggs {
            let wide = match a {
                AggState::Min(Some(v)) | AggState::Max(Some(v)) => {
                    wide_prefix(v).map(|prefix| WideExtreme { prefix, same: Some(value_hash(v)) })
                }
                _ => None,
            };
            if let Some(w) = wide {
                *a = prefix_state(matches!(a, AggState::Min(_)), w);
            }
        }
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        put_ivarint(out, self.count);
        put_uvarint(out, self.aggs.len() as u64);
        for a in &self.aggs {
            a.encode(out);
        }
    }

    /// Length of the encoding (for node size estimates).
    pub fn encoded_len(&self) -> usize {
        ivarint_len(self.count) + uvarint_len(self.aggs.len() as u64) + self.aggs.iter().map(AggState::encoded_len).sum::<usize>()
    }

    pub fn decode(buf: &mut &[u8]) -> Result<Summary> {
        let count = get_ivarint(buf)?;
        let n = get_uvarint(buf)? as usize;
        let mut aggs = Vec::with_capacity(n);
        for _ in 0..n {
            aggs.push(AggState::decode(buf)?);
        }
        Ok(Summary { count, aggs })
    }
}

/// Puts a summary folded from entries into the form node summaries hold
/// when some MIN/MAX value is too wide for it (`Summary::bound_extremes`
/// after each entry would do, but hashes every value tying a prefix): shown
/// the summary of each entry alone, it notes whether another value shares a
/// wide extreme's prefix.
pub struct WideExtremes {
    /// (agg index, prefix, another value has it)
    wide: Vec<(usize, Box<[u8]>, bool)>,
}

impl WideExtremes {
    /// None when no MIN/MAX value of `s` is too wide for a node summary.
    pub fn of(s: &Summary) -> Option<WideExtremes> {
        let wide: Vec<_> = (0..s.aggs.len())
            .filter_map(|i| match &s.aggs[i] {
                AggState::Min(Some(v)) | AggState::Max(Some(v)) => wide_prefix(v).map(|p| (i, p, false)),
                _ => None,
            })
            .collect();
        (!wide.is_empty()).then_some(WideExtremes { wide })
    }

    /// Account for an entry of `s`, whose summary alone is `entry`.
    pub fn note(&mut self, s: &Summary, entry: &Summary) {
        for (i, p, mixed) in &mut self.wide {
            if let (AggState::Min(Some(m)) | AggState::Max(Some(m)), AggState::Min(Some(v)) | AggState::Max(Some(v))) =
                (&s.aggs[*i], &entry.aggs[*i])
                && !*mixed
                && exact_cmp(v, m) != Ordering::Equal
                && key_starts_with(v, p)
            {
                *mixed = true;
            }
        }
    }

    pub fn apply(self, s: &mut Summary) {
        for (i, prefix, mixed) in self.wide {
            if let AggState::Min(Some(v)) | AggState::Max(Some(v)) = &s.aggs[i] {
                let w = WideExtreme { prefix, same: (!mixed).then(|| value_hash(v)) };
                s.aggs[i] = prefix_state(matches!(s.aggs[i], AggState::Min(_)), w);
            }
        }
    }
}

/// Kind of a per-node aggregate (the monoid).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum AggKind {
    /// Weighted count of entries whose argument is non-null.
    CountNonNull,
    /// Weighted count of entries whose (boolean) argument is true.
    CountTrue,
    /// Weighted count of entries whose (boolean) argument is false.
    CountFalse,
    SumInt,
    SumNumeric,
    SumFloat,
    /// Sum of squares (for variance/stddev).
    SumSqInt,
    SumSqNumeric,
    SumSqFloat,
    Min,
    Max,
}

impl AggKind {
    pub fn empty(&self) -> AggState {
        match self {
            AggKind::CountNonNull | AggKind::CountTrue | AggKind::CountFalse => AggState::Count(0),
            AggKind::SumInt | AggKind::SumSqInt => AggState::SumInt(0),
            AggKind::SumNumeric | AggKind::SumSqNumeric => AggState::SumNumeric(Box::default()),
            AggKind::SumFloat | AggKind::SumSqFloat => AggState::SumFloat(Box::default()),
            AggKind::Min => AggState::Min(None),
            AggKind::Max => AggState::Max(None),
        }
    }

    /// Fold one entry with argument value `v` and current weight `w`.
    pub fn add(&self, state: &mut AggState, v: &Datum, w: i64) -> Result<()> {
        if w == 0 || v.is_null() {
            return Ok(());
        }
        match (self, state) {
            (AggKind::CountNonNull, AggState::Count(c)) => *c += w,
            (AggKind::CountTrue, AggState::Count(c)) => {
                if v.as_bool() == Some(true) {
                    *c += w
                }
            }
            (AggKind::CountFalse, AggState::Count(c)) => {
                if v.as_bool() == Some(false) {
                    *c += w
                }
            }
            (AggKind::SumInt, AggState::SumInt(s)) => {
                let x = v.as_i64().ok_or_else(|| SqlError::internal("SumInt on non-integer"))? as i128;
                *s += x * w as i128;
            }
            (AggKind::SumSqInt, AggState::SumInt(s)) => {
                let x = v.as_i64().ok_or_else(|| SqlError::internal("SumSqInt on non-integer"))? as i128;
                *s += x * x * w as i128;
            }
            (AggKind::SumNumeric, AggState::SumNumeric(s)) => {
                let x = v.as_numeric().ok_or_else(|| SqlError::internal("SumNumeric on non-numeric"))?;
                s.add_term(x, w)?;
            }
            (AggKind::SumSqNumeric, AggState::SumNumeric(s)) => {
                let x = v.as_numeric().ok_or_else(|| SqlError::internal("SumSqNumeric on non-numeric"))?;
                s.add_term(&x.mul_unbounded(x), w)?;
            }
            (AggKind::SumFloat, AggState::SumFloat(s)) => {
                let x = v.as_f64().ok_or_else(|| SqlError::internal("SumFloat on non-float"))?;
                s.add(x, w);
            }
            (AggKind::SumSqFloat, AggState::SumFloat(s)) => {
                let x = v.as_f64().ok_or_else(|| SqlError::internal("SumSqFloat on non-float"))?;
                s.add_square(x, w);
            }
            (AggKind::Min | AggKind::Max, st) if extreme_kind(st) == Some(*self == AggKind::Min) => {
                let less = *self == AggKind::Min;
                if w > 0 {
                    match st.ext().map(|cur| ext_cmp(Ext::Val(v), cur)) {
                        Some(Ordering::Equal) => join_tie(st, Ext::Val(v)),
                        Some(o) if o != best(less) => {}
                        _ => *st = extreme_state(less, Some(Ext::Val(v))),
                    }
                }
            }
            (k, s) => return Err(SqlError::internal(format!("agg kind/state mismatch {k:?} {s:?}"))),
        }
        Ok(())
    }

    /// Account for an entry with argument `v` whose weight changed from
    /// `before` to `after`. False when the state must be recomputed (a
    /// MIN/MAX lost its extreme, or a new value ties with it in a different
    /// representation, where the leftmost one wins).
    pub fn update(&self, state: &mut AggState, v: &Datum, before: i64, after: i64) -> Result<bool> {
        if before == after || v.is_null() {
            return Ok(true);
        }
        let (AggKind::Min | AggKind::Max) = self else {
            self.add(state, v, after - before)?;
            return Ok(true);
        };
        let less = *self == AggKind::Min;
        if extreme_kind(state) != Some(less) {
            return Err(SqlError::internal("agg kind/state mismatch"));
        }
        let (was, is) = (before > 0, after > 0);
        if was == is {
            return Ok(true);
        }
        Ok(if is {
            replace_extreme(less, state, None, Some(Ext::Val(v)))
        } else {
            replace_extreme(less, state, Some(Ext::Val(v)), None)
        })
    }
}

/// The extreme of a MIN/MAX state: a value, or the key-encoding prefix that
/// stands for a wide value in a node summary.
#[derive(Clone, Copy, Debug)]
enum Ext<'a> {
    Val(&'a Datum),
    Prefix(&'a WideExtreme),
}

/// The order of the extremes `a` and `b`. `Equal` means SQL-equal values
/// (which may differ in representation) or, when a prefix is involved, the
/// same prefix (which may stand for different values). A value whose key
/// encoding fits in a prefix never ties with one.
fn ext_cmp(a: Ext, b: Ext) -> Ordering {
    match (a, b) {
        (Ext::Val(x), Ext::Val(y)) => x.sql_cmp(y),
        (Ext::Prefix(p), Ext::Prefix(q)) => p.prefix.cmp(&q.prefix),
        (Ext::Val(x), Ext::Prefix(q)) => key_prefix(x, q.prefix.len()).as_slice().cmp(&q.prefix),
        (Ext::Prefix(p), Ext::Val(y)) => (*p.prefix).cmp(key_prefix(y, p.prefix.len()).as_slice()),
    }
}

/// The order of a better value: smaller for MIN (`less`), larger for MAX.
fn best(less: bool) -> Ordering {
    if less { Ordering::Less } else { Ordering::Greater }
}

/// `Some(true)` for a MIN state, `Some(false)` for a MAX state.
fn extreme_kind(st: &AggState) -> Option<bool> {
    match st {
        AggState::Min(_) | AggState::MinPrefix(_) => Some(true),
        AggState::Max(_) | AggState::MaxPrefix(_) => Some(false),
        _ => None,
    }
}

fn extreme_state(less: bool, e: Option<Ext>) -> AggState {
    match (less, e) {
        (true, None) => AggState::Min(None),
        (false, None) => AggState::Max(None),
        (true, Some(Ext::Val(v))) => AggState::Min(Some(v.clone())),
        (false, Some(Ext::Val(v))) => AggState::Max(Some(v.clone())),
        (true, Some(Ext::Prefix(p))) => AggState::MinPrefix(Box::new(p.clone())),
        (false, Some(Ext::Prefix(p))) => AggState::MaxPrefix(Box::new(p.clone())),
    }
}

fn prefix_state(less: bool, w: WideExtreme) -> AggState {
    if less { AggState::MinPrefix(Box::new(w)) } else { AggState::MaxPrefix(Box::new(w)) }
}

/// `WideExtreme::same` of an extreme.
fn same_of(e: Ext) -> Option<[u8; 32]> {
    match e {
        Ext::Val(v) => Some(value_hash(v)),
        Ext::Prefix(w) => w.same,
    }
}

/// The hash identifying a value exactly (representation included).
fn value_hash(v: &Datum) -> [u8; 32] {
    let mut enc = Vec::new();
    codec::encode_datum(v, &mut enc);
    pgfuncs::text::sha256(&enc).try_into().unwrap()
}

/// Account in a prefix `st` for another extreme `e` with the same prefix
/// (of a part of the range it covers, which has no other such values); a
/// value keeps its place against an equal one, the leftmost.
fn join_tie(st: &mut AggState, e: Ext) {
    if let AggState::MinPrefix(w) | AggState::MaxPrefix(w) = st
        && w.same.is_some()
        && w.same != same_of(e)
    {
        w.same = None;
    }
}

/// The first `n` bytes of `v`'s (ascending) key encoding.
fn key_prefix(v: &Datum, n: usize) -> Vec<u8> {
    let mut k = Vec::new();
    match v {
        // strings encode byte by byte, as bytea does: n bytes suffice
        Datum::Text(s) | Datum::Json(s) if s.len() > n => encode_key(&Datum::Bytea(s.as_bytes()[..n].into()), SortSpec::ASC, &mut k),
        Datum::Bytea(b) if b.len() > n => encode_key(&Datum::Bytea(b[..n].into()), SortSpec::ASC, &mut k),
        _ => encode_key(v, SortSpec::ASC, &mut k),
    }
    k.truncate(n);
    k
}

/// Whether `v`'s key encoding starts with `p` (without encoding all of a
/// long string).
fn key_starts_with(v: &Datum, p: &[u8]) -> bool {
    let b: &[u8] = match v {
        Datum::Text(s) | Datum::Json(s) => s.as_bytes(),
        Datum::Bytea(b) => b,
        _ => return key_prefix(v, p.len()) == p,
    };
    // as `encode_key`: the tag, the bytes with zeros escaped, a terminator
    let enc = std::iter::once(TAG_VALUE)
        .chain(b.iter().flat_map(|&c| [c, 0xFF].into_iter().take(if c == 0 { 2 } else { 1 })))
        .chain([0, 0]);
    let mut want = p.iter();
    for x in enc {
        match want.next() {
            None => return true,
            Some(&y) if y != x => return false,
            _ => {}
        }
    }
    want.next().is_none()
}

/// The prefix standing for `v` in a node summary, if `v`'s key encoding is
/// longer than [`EXTREME_PREFIX`].
fn wide_prefix(v: &Datum) -> Option<Box<[u8]>> {
    let wide = match v {
        // a string encodes as its bytes (zeros escaped) plus three bytes
        Datum::Text(s) | Datum::Json(s) => encoded_str_len(s.as_bytes()) > EXTREME_PREFIX,
        Datum::Bytea(b) => encoded_str_len(b) > EXTREME_PREFIX,
        Datum::Numeric(_) | Datum::Jsonb(_) | Datum::Array(_) => {
            let mut k = Vec::new();
            encode_key(v, SortSpec::ASC, &mut k);
            k.len() > EXTREME_PREFIX
        }
        // fixed-size types are short
        _ => false,
    };
    wide.then(|| key_prefix(v, EXTREME_PREFIX).into_boxed_slice())
}

fn encoded_str_len(b: &[u8]) -> usize {
    // (zeros, escaped to two bytes, decide only for lengths in between)
    if b.len() > EXTREME_PREFIX || 2 * b.len() + 3 <= EXTREME_PREFIX {
        return 3 + b.len();
    }
    3 + b.len() + b.iter().filter(|&&c| c == 0).count()
}

/// Update the extreme `cur` (a MIN if `less`, else a MAX) of a range — the
/// leftmost of its best values, or the prefix standing for it — when a part
/// of the range whose extreme was `old` now has extreme `new`. False when
/// the result depends on positions or values not at hand.
fn replace_extreme(less: bool, cur: &mut AggState, old: Option<Ext>, new: Option<Ext>) -> bool {
    let Some(m) = cur.ext() else {
        // nothing else in the range has a value
        *cur = extreme_state(less, new);
        return true;
    };
    // An `old` equal to `m` in another representation proves that `m` comes
    // from a part further left, which this change does not touch.
    let left_of_part = matches!((old, m), (Some(Ext::Val(a)), Ext::Val(m))
        if a.sql_cmp(m) == Ordering::Equal && exact_cmp(a, m) != Ordering::Equal);
    let old_may_be = matches!(old, Some(a) if may_be(a, m));
    if let Some(b) = new {
        let o = ext_cmp(b, m);
        if o == best(less) {
            *cur = extreme_state(less, Some(b));
            return true;
        }
        if o == Ordering::Equal {
            return match (b, m) {
                (Ext::Val(b), Ext::Val(m)) => exact_cmp(b, m) == Ordering::Equal || left_of_part,
                // The extreme keeps the prefix. Whether the values with it
                // are all the same is known unless the part had some before
                // (and they were not all `b`'s).
                (b, Ext::Prefix(w)) if !old_may_be || (w.same.is_some() && w.same == same_of(b)) => {
                    join_tie(cur, b);
                    true
                }
                _ => false,
            };
        }
    }
    // `new` is worse (or absent): `m` survives unless `old` may have been it
    !old_may_be
}

/// The extreme `a` of a part may be the extreme `m` of the range: the same
/// value exactly, or a value with `m`'s prefix.
fn may_be(a: Ext, m: Ext) -> bool {
    match (a, m) {
        (Ext::Val(a), Ext::Val(m)) => exact_cmp(a, m) == Ordering::Equal,
        _ => ext_cmp(a, m) == Ordering::Equal,
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum AggState {
    Count(i64),
    SumInt(i128),
    SumNumeric(Box<NumSum>),
    SumFloat(Box<FloatSum>),
    Min(Option<Datum>),
    Max(Option<Datum>),
    /// In a node summary, a MIN whose value is too wide to be held there:
    /// the first [`EXTREME_PREFIX`] bytes of the value's key encoding.
    /// `Env::range_summary` resolves it to the value.
    MinPrefix(Box<WideExtreme>),
    /// The same for a MAX.
    MaxPrefix(Box<WideExtreme>),
}

impl PartialEq for Datum {
    fn eq(&self, other: &Self) -> bool {
        match (self.is_null(), other.is_null()) {
            (true, true) => true,
            (false, false) => {
                std::mem::discriminant(self) == std::mem::discriminant(other) && self.sql_eq(other)
            }
            _ => false,
        }
    }
}

thread_local! {
    /// Buffer for measuring encodings without allocating.
    static SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

impl AggState {
    pub fn combine(&mut self, other: &AggState) {
        if let Some(less) = extreme_kind(self) {
            let Some(b) = other.ext().filter(|_| extreme_kind(other) == Some(less)) else { return };
            match self.ext().map(|a| (ext_cmp(b, a), matches!(a, Ext::Val(_)))) {
                // a value tying a prefix may not be the extreme (nor the
                // only value with it): only the prefix is certain
                Some((Ordering::Equal, true)) => {
                    if let Ext::Prefix(w) = b {
                        *self = prefix_state(less, WideExtreme { prefix: w.prefix.clone(), same: None });
                    }
                }
                Some((Ordering::Equal, false)) => join_tie(self, b),
                Some((o, _)) if o != best(less) => {}
                _ => *self = other.clone(),
            }
            return;
        }
        match (self, other) {
            (AggState::Count(a), AggState::Count(b)) => *a += b,
            (AggState::SumInt(a), AggState::SumInt(b)) => *a += b,
            (AggState::SumNumeric(a), AggState::SumNumeric(b)) => a.combine(b, 1),
            (AggState::SumFloat(a), AggState::SumFloat(b)) => a.combine(b),
            _ => {}
        }
    }

    /// Remove `other` from an invertible state (counts and sums).
    pub fn subtract(&mut self, other: &AggState) {
        match (self, other) {
            (AggState::Count(a), AggState::Count(b)) => *a -= b,
            (AggState::SumInt(a), AggState::SumInt(b)) => *a -= b,
            (AggState::SumNumeric(a), AggState::SumNumeric(b)) => a.combine(b, -1),
            (AggState::SumFloat(a), AggState::SumFloat(b)) => a.subtract(b),
            _ => {}
        }
    }

    /// Whether this is a MIN/MAX held as a prefix (see `Summary::bound_extremes`).
    pub fn is_prefix(&self) -> bool {
        matches!(self, AggState::MinPrefix(_) | AggState::MaxPrefix(_))
    }

    /// For a prefix standing for the extreme of a range, whether a subtree
    /// whose summary has the state `sub` may hold an entry of the range with
    /// that prefix: a subtree inside the range (`inside`) only if its own
    /// extreme is that prefix, one extending beyond it only if its extreme
    /// is not worse.
    pub fn may_hold_extreme(&self, sub: &AggState, inside: bool) -> bool {
        let (Some(less), Some(p @ Ext::Prefix(_)), Some(s)) = (extreme_kind(self), self.ext(), sub.ext()) else {
            return false;
        };
        let o = ext_cmp(s, p);
        if inside { o == Ordering::Equal } else { o != best(less).reverse() }
    }

    /// For a prefix standing for the extreme of the subtrees inside a range,
    /// settle it against `edges`, the extreme of the range's other entries
    /// (a value), where that tells the range's extreme: the subtrees cannot
    /// hold it (the prefix is dropped), or their entries with the prefix are
    /// all `edges`' value (which replaces it). True when it must be resolved
    /// from the subtrees.
    pub fn settle_with(&mut self, edges: &AggState) -> bool {
        let (Some(less), Some(p @ Ext::Prefix(w)), Some(e @ Ext::Val(_))) = (extreme_kind(self), self.ext(), edges.ext()) else {
            return true;
        };
        match ext_cmp(e, p) {
            o if o == best(less) => *self = extreme_state(less, None),
            Ordering::Equal if w.same.is_some() && w.same == same_of(e) => *self = edges.clone(),
            _ => return true,
        }
        false
    }

    /// For a prefix standing for the extreme of a range whose entries with
    /// that prefix all have the same value (`WideExtreme::same`): whether
    /// `found`, the extreme of some entries of the range, is that value.
    pub fn found_in(&self, found: &AggState) -> bool {
        matches!((self.ext(), found.ext()), (Some(Ext::Prefix(w)), Some(Ext::Val(v)))
            if w.same.is_some() && key_starts_with(v, &w.prefix))
    }

    fn ext(&self) -> Option<Ext<'_>> {
        match self {
            AggState::Min(Some(v)) | AggState::Max(Some(v)) => Some(Ext::Val(v)),
            AggState::MinPrefix(p) | AggState::MaxPrefix(p) => Some(Ext::Prefix(p)),
            _ => None,
        }
    }

    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            AggState::Count(c) => {
                out.push(1);
                put_ivarint(out, *c);
            }
            AggState::SumInt(s) => {
                out.push(2);
                out.extend_from_slice(&s.to_le_bytes());
            }
            AggState::SumNumeric(s) => {
                out.push(4);
                s.encode(out);
            }
            AggState::SumFloat(f) => {
                out.push(8);
                f.encode(out);
            }
            AggState::Min(d) => {
                out.push(6);
                codec::encode_datum(d.as_ref().unwrap_or(&Datum::Null), out);
            }
            AggState::Max(d) => {
                out.push(7);
                codec::encode_datum(d.as_ref().unwrap_or(&Datum::Null), out);
            }
            AggState::MinPrefix(w) | AggState::MaxPrefix(w) => {
                out.push(if matches!(self, AggState::MinPrefix(_)) { 9 } else { 10 });
                codec::put_bytes(out, &w.prefix);
                match &w.same {
                    Some(h) => {
                        out.push(1);
                        out.extend_from_slice(h);
                    }
                    None => out.push(0),
                }
            }
        }
    }

    fn encoded_len(&self) -> usize {
        1 + match self {
            AggState::Count(c) => ivarint_len(*c),
            AggState::SumInt(_) => 16,
            AggState::SumFloat(f) => f.encoded_len(),
            AggState::Min(None) | AggState::Max(None) => 1,
            AggState::Min(Some(Datum::Text(s) | Datum::Json(s))) | AggState::Max(Some(Datum::Text(s) | Datum::Json(s))) => {
                1 + uvarint_len(s.len() as u64) + s.len()
            }
            AggState::MinPrefix(w) | AggState::MaxPrefix(w) => {
                uvarint_len(w.prefix.len() as u64) + w.prefix.len() + 1 + if w.same.is_some() { 32 } else { 0 }
            }
            _ => {
                return SCRATCH.with(|b| {
                    let mut b = b.borrow_mut();
                    b.clear();
                    self.encode(&mut b);
                    b.len()
                });
            }
        }
    }

    fn decode(buf: &mut &[u8]) -> Result<AggState> {
        // A MIN/MAX value too wide for a node summary, written before
        // prefixes replaced such values, becomes its prefix (with other
        // values possibly sharing it: `same` is unknown).
        let extreme = |less: bool, d: Datum| match wide_prefix(&d) {
            _ if d.is_null() => extreme_state(less, None),
            Some(prefix) => prefix_state(less, WideExtreme { prefix, same: None }),
            None => extreme_state(less, Some(Ext::Val(&d))),
        };
        Ok(match get_u8(buf)? {
            1 => AggState::Count(get_ivarint(buf)?),
            2 => {
                if buf.len() < 16 {
                    return Err(SqlError::corrupt("truncated summary"));
                }
                let v = i128::from_le_bytes(buf[..16].try_into().unwrap());
                *buf = &buf[16..];
                AggState::SumInt(v)
            }
            4 => AggState::SumNumeric(Box::new(NumSum::decode(buf)?)),
            5 => AggState::SumFloat(Box::new(FloatSum::decode_fixed(buf)?)),
            6 => extreme(true, codec::decode_datum(buf)?),
            7 => extreme(false, codec::decode_datum(buf)?),
            8 => AggState::SumFloat(Box::new(FloatSum::decode(buf)?)),
            t @ (9 | 10) => {
                let prefix: Box<[u8]> = codec::get_bytes(buf)?.into();
                if prefix.len() != EXTREME_PREFIX {
                    return Err(SqlError::corrupt("bad MIN/MAX prefix length"));
                }
                let same = match get_u8(buf)? {
                    0 => None,
                    _ => {
                        let h: [u8; 32] = buf.get(..32).ok_or_else(|| SqlError::corrupt("truncated summary"))?.try_into().unwrap();
                        *buf = &buf[32..];
                        Some(h)
                    }
                };
                prefix_state(t == 9, WideExtreme { prefix, same })
            }
            t => return Err(SqlError::corrupt(format!("bad agg state tag {t}"))),
        })
    }
}

pub(crate) fn uvarint_len(v: u64) -> usize {
    (64 - (v | 1).leading_zeros() as usize).div_ceil(7)
}

fn ivarint_len(v: i64) -> usize {
    uvarint_len(((v << 1) ^ (v >> 63)) as u64)
}

/// Exact, order-independent and invertible sum of weighted numerics.
///
/// Like PostgreSQL's numeric aggregate state, NaN and the infinities are
/// counted apart from the finite sum. The finite terms' display scales are
/// counted too: the result's scale is the largest scale present, which
/// therefore does not depend on terms that were added and later removed.
///
/// Also like PostgreSQL's state, the finite sum is not limited to numeric's
/// range (a sum over a subtree, or a sum of squares, may exceed it while
/// every value read from it fits): maintaining it never fails, and readers
/// check the range of the result (`Numeric::checked`).
#[derive(Clone, Debug, PartialEq)]
pub struct NumSum {
    pub finite: Numeric,
    /// (display scale, weighted count of finite terms), sorted, no zeros.
    pub scales: SmallVec<[(u32, i64); 2]>,
    pub nan: i64,
    pub pos_inf: i64,
    pub neg_inf: i64,
}

impl Default for NumSum {
    fn default() -> Self {
        NumSum { finite: Numeric::zero(), scales: SmallVec::new(), nan: 0, pos_inf: 0, neg_inf: 0 }
    }
}

impl NumSum {
    pub fn add_term(&mut self, x: &Numeric, w: i64) -> Result<()> {
        if w == 0 {
            return Ok(());
        }
        if x.is_nan() {
            self.nan += w;
        } else if x.is_infinite() {
            if x.is_negative() {
                self.neg_inf += w
            } else {
                self.pos_inf += w
            }
        } else {
            self.finite = match w {
                1 => self.finite.add_unbounded(x),
                -1 => self.finite.sub_unbounded(x),
                _ => self.finite.add_unbounded(&x.mul_unbounded(&Numeric::from_i64(w))),
            };
            self.add_scale(x.dscale(), w);
        }
        Ok(())
    }

    fn add_scale(&mut self, scale: u32, w: i64) {
        match self.scales.binary_search_by_key(&scale, |(s, _)| *s) {
            Ok(i) => {
                self.scales[i].1 += w;
                if self.scales[i].1 == 0 {
                    self.scales.remove(i);
                }
            }
            Err(i) => self.scales.insert(i, (scale, w)),
        }
    }

    /// Add `sign` (1 or -1) times `o`.
    pub fn combine(&mut self, o: &NumSum, sign: i64) {
        self.finite = if sign > 0 { self.finite.add_unbounded(&o.finite) } else { self.finite.sub_unbounded(&o.finite) };
        for (s, c) in &o.scales {
            self.add_scale(*s, sign * c);
        }
        self.nan += sign * o.nan;
        self.pos_inf += sign * o.pos_inf;
        self.neg_inf += sign * o.neg_inf;
    }

    /// The sum (PostgreSQL semantics for special values), at the largest
    /// display scale among the terms; possibly beyond numeric's range.
    pub fn value(&self) -> Numeric {
        if self.nan > 0 || (self.pos_inf > 0 && self.neg_inf > 0) {
            return Numeric::nan();
        }
        if self.pos_inf > 0 {
            return Numeric::infinity();
        }
        if self.neg_inf > 0 {
            return Numeric::neg_infinity();
        }
        let scale = self.scales.last().map(|(s, _)| *s).unwrap_or(0);
        if self.finite.dscale() == scale { self.finite.clone() } else { self.finite.with_dscale(scale) }
    }

    pub fn has_special(&self) -> bool {
        self.nan > 0 || self.pos_inf > 0 || self.neg_inf > 0
    }

    fn encode(&self, out: &mut Vec<u8>) {
        self.finite.encode_binary(out);
        put_uvarint(out, self.scales.len() as u64);
        for (s, c) in &self.scales {
            put_uvarint(out, *s as u64);
            put_ivarint(out, *c);
        }
        put_ivarint(out, self.nan);
        put_ivarint(out, self.pos_inf);
        put_ivarint(out, self.neg_inf);
    }

    fn decode(buf: &mut &[u8]) -> Result<NumSum> {
        // (the exact sum may be beyond numeric's range)
        let (finite, len) = Numeric::decode_binary_unbounded(buf)?;
        *buf = &buf[len..];
        let n = get_uvarint(buf)? as usize;
        let mut scales = SmallVec::with_capacity(n);
        for _ in 0..n {
            scales.push((get_uvarint(buf)? as u32, get_ivarint(buf)?));
        }
        Ok(NumSum { finite, scales, nan: get_ivarint(buf)?, pos_inf: get_ivarint(buf)?, neg_inf: get_ivarint(buf)? })
    }
}

/// Exact, order-independent and invertible sum of weighted doubles.
///
/// The exact sum of doubles is a dyadic rational, kept normalized
/// ([`Dyadic`]); the result is rounded once when read. This pins down
/// floating-point SUM/AVG so that the result is a pure function of the
/// multiset, independent of update order. Every finite sum is a multiple of
/// 2^-1074 (the smallest subnormal), the grid that squares are truncated to
/// (`add_square`) and that `div_value` and `variance` compute on.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct FloatSum {
    pub finite: Dyadic,
    pub nan: i64,
    pub pos_inf: i64,
    pub neg_inf: i64,
}

impl FloatSum {
    pub fn add(&mut self, x: f64, w: i64) {
        if x.is_nan() {
            self.nan += w;
        } else if x == f64::INFINITY {
            self.pos_inf += w;
        } else if x == f64::NEG_INFINITY {
            self.neg_inf += w;
        } else if x != 0.0 {
            let (m, e) = decompose(x);
            self.finite.add_mag((m < 0) != (w < 0), limbs_of_product(m.unsigned_abs() as u128, w.unsigned_abs()), e);
        }
    }

    /// Add w·x², where x² = m²·2^(2e) is truncated to a multiple of 2^-1074
    /// when it has bits below it.
    pub fn add_square(&mut self, x: f64, w: i64) {
        if !x.is_finite() {
            self.add(x * x, w);
            return;
        }
        if x == 0.0 {
            return;
        }
        let (m, e) = decompose(x);
        let m2 = m.unsigned_abs() as u128 * m.unsigned_abs() as u128; // < 2^106
        let (sq, exp) = if 2 * e >= -1074 {
            (m2, 2 * e)
        } else {
            let s = (-1074 - 2 * e) as u32;
            (m2.checked_shr(s).unwrap_or(0), -1074)
        };
        self.finite.add_mag(w < 0, limbs_of_product(sq, w.unsigned_abs()), exp);
    }

    pub fn combine(&mut self, o: &FloatSum) {
        self.finite.add(&o.finite);
        self.nan += o.nan;
        self.pos_inf += o.pos_inf;
        self.neg_inf += o.neg_inf;
    }

    pub fn subtract(&mut self, o: &FloatSum) {
        self.finite.sub(&o.finite);
        self.nan -= o.nan;
        self.pos_inf -= o.pos_inf;
        self.neg_inf -= o.neg_inf;
    }

    /// Correctly rounded result. Infinity on overflow of the finite part.
    pub fn value(&self) -> f64 {
        if self.nan > 0 || (self.pos_inf > 0 && self.neg_inf > 0) {
            return f64::NAN;
        }
        if self.pos_inf > 0 {
            return f64::INFINITY;
        }
        if self.neg_inf > 0 {
            return f64::NEG_INFINITY;
        }
        self.finite.to_f64()
    }

    /// The finite part divided by n (for AVG): the quotient on the 2^-1074
    /// grid with 128 more bits, truncated, then rounded to the nearest
    /// double (ties to even).
    pub fn div_value(&self, n: i64) -> f64 {
        let v = self.value();
        if !v.is_finite() || n == 0 {
            return v / n as f64;
        }
        self.finite.div_to_f64(n)
    }

    /// Variance of n values from their sum (`self`) and sum of squares:
    /// `(N·Q − S²) / (N·(N−1))` for a sample, else `/ N²`, computed exactly
    /// on the 2^-2148 grid of the squares with 128 more bits, truncated,
    /// then rounded to the nearest double. 0 when not positive. The caller
    /// handles special values.
    pub fn variance(&self, sumsq: &FloatSum, n: i64, sample: bool) -> f64 {
        let (s, q) = (self.finite.to_fixed(), sumsq.finite.to_fixed());
        let nb = BigInt::from(n);
        let num: BigInt = ((&nb * &q) << 1074u32) - &s * &s;
        if !num.is_positive() {
            return 0.0;
        }
        let denom = if sample { &nb * BigInt::from(n - 1) } else { &nb * &nb };
        bigint_scaled_to_f64(&((num << 128u32) / denom), -2148 - 128)
    }

    fn encode(&self, out: &mut Vec<u8>) {
        self.finite.encode(out);
        put_ivarint(out, self.nan);
        put_ivarint(out, self.pos_inf);
        put_ivarint(out, self.neg_inf);
    }

    fn encoded_len(&self) -> usize {
        self.finite.encoded_len() + ivarint_len(self.nan) + ivarint_len(self.pos_inf) + ivarint_len(self.neg_inf)
    }

    fn decode(buf: &mut &[u8]) -> Result<FloatSum> {
        let finite = Dyadic::decode(buf)?;
        Ok(FloatSum { finite, nan: get_ivarint(buf)?, pos_inf: get_ivarint(buf)?, neg_inf: get_ivarint(buf)? })
    }

    /// The encoding of agg state tag 5: the finite part as an integer
    /// multiple of 2^-1074 (sign byte, magnitude).
    fn decode_fixed(buf: &mut &[u8]) -> Result<FloatSum> {
        let sign = match get_u8(buf)? {
            2 => Sign::Minus,
            0 => Sign::NoSign,
            _ => Sign::Plus,
        };
        let bytes = codec::get_bytes(buf)?;
        Ok(FloatSum {
            finite: Dyadic::checked(Dyadic::from_bigint(&BigInt::from_bytes_le(sign, bytes), -1074))?,
            nan: get_ivarint(buf)?,
            pos_inf: get_ivarint(buf)?,
            neg_inf: get_ivarint(buf)?,
        })
    }
}

/// An exact dyadic rational `±mag·2^exp`, normalized so that each value has
/// one representation (and encoding): `mag` is odd, or 0 (no limbs) with a
/// positive sign and `exp` = 0. The magnitude is held inline up to 256 bits:
/// a sum of doubles needs about 53 bits plus the span of their exponents
/// plus the bits of their count (106 bits plus twice the span for squares),
/// so adding a double to it rarely allocates.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Dyadic {
    neg: bool,
    /// Little-endian 64-bit limbs, without high zero limbs.
    mag: Limbs,
    exp: i32,
}

type Limbs = SmallVec<[u64; 4]>;

impl Dyadic {
    /// `±mag·2^e` (`mag` of any parity), normalized.
    fn new(neg: bool, mut mag: Limbs, e: i32) -> Dyadic {
        trim(&mut mag);
        let mut d = Dyadic { neg, mag, exp: e };
        d.normalize();
        d
    }

    fn from_bigint(v: &BigInt, e: i32) -> Dyadic {
        let (sign, bytes) = v.to_bytes_le();
        Dyadic::new(sign == Sign::Minus, limbs_from_le(&bytes), e)
    }

    fn normalize(&mut self) {
        if self.mag.is_empty() {
            *self = Dyadic::default();
            return;
        }
        let i = self.mag.iter().position(|&x| x != 0).unwrap();
        let tz = 64 * i as u32 + self.mag[i].trailing_zeros();
        shr_limbs(&mut self.mag, tz);
        self.exp += tz as i32;
    }

    pub fn is_zero(&self) -> bool {
        self.mag.is_empty()
    }

    /// Bits of the magnitude.
    fn bits(&self) -> u64 {
        match self.mag.last() {
            None => 0,
            Some(&top) => 64 * self.mag.len() as u64 - top.leading_zeros() as u64,
        }
    }

    /// Add `±mag·2^e` (`mag` without high zero limbs).
    fn add_mag(&mut self, neg: bool, mut mag: Limbs, e: i32) {
        if mag.is_empty() {
            return;
        }
        if self.mag.is_empty() {
            *self = Dyadic::new(neg, mag, e);
            return;
        }
        let lo = self.exp.min(e);
        shl_limbs(&mut self.mag, (self.exp - lo) as u32);
        shl_limbs(&mut mag, (e - lo) as u32);
        self.exp = lo;
        if self.neg == neg {
            add_limbs(&mut self.mag, &mag);
        } else {
            match cmp_limbs(&self.mag, &mag) {
                Ordering::Equal => self.mag.clear(),
                Ordering::Greater => sub_limbs(&mut self.mag, &mag),
                Ordering::Less => {
                    sub_limbs(&mut mag, &self.mag);
                    self.mag = mag;
                    self.neg = neg;
                }
            }
        }
        self.normalize();
    }

    pub fn add(&mut self, o: &Dyadic) {
        self.add_mag(o.neg, o.mag.clone(), o.exp);
    }

    pub fn sub(&mut self, o: &Dyadic) {
        self.add_mag(!o.neg, o.mag.clone(), o.exp);
    }

    /// Rounded to the nearest double, ties to even (±infinity beyond the
    /// range); the same steps as `bigint_scaled_to_f64`.
    pub fn to_f64(&self) -> f64 {
        let bits = self.bits();
        if bits == 0 {
            return 0.0;
        }
        let r = if bits <= 53 {
            ldexp(self.mag[0] as f64, self.exp)
        } else {
            // keep 53 significant bits, round half to even using the rest
            let shift = bits - 53;
            let (q, b) = ((shift / 64) as usize, shift % 64);
            let mut top = self.mag[q] >> b;
            if b > 0 && q + 1 < self.mag.len() {
                top |= self.mag[q + 1] << (64 - b);
            }
            top &= (1 << 53) - 1;
            // the half-unit bit, and whether any bit below it is set
            let (hq, hb) = (((shift - 1) / 64) as usize, (shift - 1) % 64);
            let rest = if self.mag[hq] >> hb & 1 == 0 {
                Ordering::Less
            } else if self.mag[..hq].iter().any(|&x| x != 0) || self.mag[hq] & ((1 << hb) - 1) != 0 {
                Ordering::Greater
            } else {
                Ordering::Equal
            };
            round_top(top, rest, self.exp as i64 + shift as i64)
        };
        if self.neg { -r } else { r }
    }

    /// The value in units of 2^-1074: an integer for every sum of doubles
    /// (and of their truncated squares).
    fn to_fixed(&self) -> BigInt {
        let bytes: Vec<u8> = self.mag.iter().flat_map(|l| l.to_le_bytes()).collect();
        let sign = if self.neg { Sign::Minus } else { Sign::Plus };
        BigInt::from_biguint(sign, BigUint::from_bytes_le(&bytes)) << (self.exp + 1074) as u32
    }

    /// `self / n` (`n` nonzero) on the 2^-1074 grid with 128 more bits,
    /// truncated, then rounded to the nearest double.
    fn div_to_f64(&self, n: i64) -> f64 {
        if self.is_zero() {
            return 0.0;
        }
        // A sum of at least 2^115 grid units has a quotient of at least
        // 2^52 units (2^-1022, the smallest normal double), where that
        // computation is correctly rounded (the truncation cannot turn the
        // remainder into an exact half: that would need a remainder of the
        // division divisible by 2^128). So a correctly rounded quotient,
        // with a sticky bit for the remainder, gives the same result without
        // scaling to the grid. Smaller sums, and magnitudes beyond 127 bits,
        // take the long way.
        let bits = self.bits();
        if bits <= 127 && bits + (self.exp + 1074) as u64 > 115 {
            let mag = self.mag[0] as u128 | (self.mag.get(1).copied().unwrap_or(0) as u128) << 64;
            let s = mag.leading_zeros() - 1;
            let x = mag << s; // 127 bits: the quotient has at least 64
            let d = n.unsigned_abs() as u128;
            let q = (x / d) << 1 | !x.is_multiple_of(d) as u128;
            return u128_scaled_to_f64(self.neg != (n < 0), q, self.exp - s as i32 - 1);
        }
        bigint_scaled_to_f64(&((self.to_fixed() << 128u32) / BigInt::from(n)), -1074 - 128)
    }

    fn encode(&self, out: &mut Vec<u8>) {
        put_ivarint(out, self.exp as i64);
        let len = (self.bits() as usize).div_ceil(8);
        put_uvarint(out, (len as u64) << 1 | self.neg as u64);
        let start = out.len();
        out.extend(self.mag.iter().flat_map(|l| l.to_le_bytes()));
        out.truncate(start + len);
    }

    fn encoded_len(&self) -> usize {
        let len = (self.bits() as usize).div_ceil(8);
        ivarint_len(self.exp as i64) + uvarint_len((len as u64) << 1) + len
    }

    fn decode(buf: &mut &[u8]) -> Result<Dyadic> {
        let exp = get_ivarint(buf)?;
        let head = get_uvarint(buf)?;
        let len = (head >> 1) as usize;
        if buf.len() < len || !(-(1 << 20)..=1 << 20).contains(&exp) {
            return Err(SqlError::corrupt("bad float sum in summary"));
        }
        let (mag, rest) = buf.split_at(len);
        *buf = rest;
        Dyadic::checked(Dyadic::new(head & 1 == 1, limbs_from_le(mag), exp as i32))
    }

    /// Reject values no sum of doubles can have (from corrupt data), whose
    /// `to_fixed` would fail or be huge.
    fn checked(d: Dyadic) -> Result<Dyadic> {
        if d.is_zero() || (d.exp >= -1074 && d.exp as i64 + d.bits() as i64 <= 1 << 16) {
            Ok(d)
        } else {
            Err(SqlError::corrupt("bad float sum in summary"))
        }
    }
}

fn limbs_from_le(bytes: &[u8]) -> Limbs {
    bytes
        .chunks(8)
        .map(|c| {
            let mut b = [0u8; 8];
            b[..c.len()].copy_from_slice(c);
            u64::from_le_bytes(b)
        })
        .collect()
}

/// The limbs of `a·b`.
fn limbs_of_product(a: u128, b: u64) -> Limbs {
    let lo = (a as u64 as u128) * b as u128;
    let hi = (a >> 64) * b as u128 + (lo >> 64);
    let mut v: Limbs = smallvec::smallvec![lo as u64, hi as u64, (hi >> 64) as u64];
    trim(&mut v);
    v
}

fn trim(v: &mut Limbs) {
    while v.last() == Some(&0) {
        v.pop();
    }
}

fn shl_limbs(v: &mut Limbs, s: u32) {
    if s == 0 || v.is_empty() {
        return;
    }
    let (q, b) = ((s / 64) as usize, s % 64);
    if b > 0 {
        let mut carry = 0;
        for x in v.iter_mut() {
            let next = *x >> (64 - b);
            *x = *x << b | carry;
            carry = next;
        }
        if carry != 0 {
            v.push(carry);
        }
    }
    if q > 0 {
        v.insert_many(0, std::iter::repeat_n(0, q));
    }
}

/// Shift right by `s` bits, dropping them.
fn shr_limbs(v: &mut Limbs, s: u32) {
    let (q, b) = ((s / 64) as usize, s % 64);
    v.drain(..q.min(v.len()));
    if b > 0 {
        for i in 0..v.len() {
            let high = v.get(i + 1).map_or(0, |&x| x << (64 - b));
            v[i] = v[i] >> b | high;
        }
        trim(v);
    }
}

fn add_limbs(a: &mut Limbs, b: &[u64]) {
    if a.len() < b.len() {
        a.resize(b.len(), 0);
    }
    let mut carry = false;
    for (i, x) in a.iter_mut().enumerate() {
        let y = b.get(i).copied().unwrap_or(0);
        if i >= b.len() && !carry {
            break;
        }
        let (s, c1) = x.overflowing_add(y);
        let (s, c2) = s.overflowing_add(carry as u64);
        *x = s;
        carry = c1 || c2;
    }
    if carry {
        a.push(1);
    }
}

/// `a -= b`, for `a >= b`.
fn sub_limbs(a: &mut Limbs, b: &[u64]) {
    let mut borrow = false;
    for (i, x) in a.iter_mut().enumerate() {
        let y = b.get(i).copied().unwrap_or(0);
        if i >= b.len() && !borrow {
            break;
        }
        let (d, b1) = x.overflowing_sub(y);
        let (d, b2) = d.overflowing_sub(borrow as u64);
        *x = d;
        borrow = b1 || b2;
    }
    trim(a);
}

/// Order of magnitudes without high zero limbs.
fn cmp_limbs(a: &[u64], b: &[u64]) -> Ordering {
    a.len().cmp(&b.len()).then_with(|| a.iter().rev().cmp(b.iter().rev()))
}

/// x = m * 2^e with integer m (|m| < 2^53) and e >= -1074.
fn decompose(x: f64) -> (i64, i32) {
    let bits = x.to_bits();
    let sign = if bits >> 63 != 0 { -1 } else { 1 };
    let exp = ((bits >> 52) & 0x7FF) as i32;
    let frac = (bits & ((1u64 << 52) - 1)) as i64;
    if exp == 0 {
        (sign * frac, -1074)
    } else {
        (sign * (frac | (1 << 52)), exp - 1075)
    }
}

/// Round v * 2^scale to the nearest double (ties to even).
pub fn bigint_scaled_to_f64(v: &BigInt, scale: i32) -> f64 {
    if v.is_zero() {
        return 0.0;
    }
    let neg = v.is_negative();
    let mag = v.abs().to_biguint().unwrap();
    let bits = mag.bits() as i64;
    let result = if bits <= 53 {
        let m = mag.to_u64().unwrap() as f64;
        ldexp(m, scale)
    } else {
        // keep 53 significant bits, round half to even using the rest
        let shift = (bits - 53) as u64;
        let top = (&mag >> shift).to_u64().unwrap();
        let rem = &mag - (BigUint::from(top) << shift);
        let half = BigUint::from(1u8) << (shift - 1);
        round_top(top, rem.cmp(&half), scale as i64 + shift as i64)
    };
    if neg { -result } else { result }
}

/// `bigint_scaled_to_f64` of an inline magnitude (the same steps).
fn u128_scaled_to_f64(neg: bool, mag: u128, scale: i32) -> f64 {
    if mag == 0 {
        return 0.0;
    }
    let bits = 128 - mag.leading_zeros() as i64;
    let result = if bits <= 53 {
        ldexp(mag as f64, scale)
    } else {
        let shift = (bits - 53) as u32;
        let rem = mag & ((1u128 << shift) - 1);
        round_top((mag >> shift) as u64, rem.cmp(&(1u128 << (shift - 1))), scale as i64 + shift as i64)
    };
    if neg { -result } else { result }
}

/// `top·2^e` rounded up by one unit if the rest (compared to half a unit)
/// requires it, ties to even.
fn round_top(top: u64, rest: Ordering, e: i64) -> f64 {
    let mut m = top;
    match rest {
        Ordering::Greater => m += 1,
        Ordering::Equal => {
            if m & 1 == 1 {
                m += 1
            }
        }
        Ordering::Less => {}
    }
    // smallest normal check: if the result would be subnormal we already
    // have more bits than representable; ldexp performs final rounding
    // only in that rare case.
    ldexp(m as f64, e.clamp(i32::MIN as i64, i32::MAX as i64) as i32)
}

fn ldexp(x: f64, e: i32) -> f64 {
    // multiply in steps to avoid intermediate overflow/underflow
    let mut x = x;
    let mut e = e;
    while e > 1000 {
        x *= 2f64.powi(1000);
        e -= 1000;
        if x.is_infinite() {
            return x;
        }
    }
    while e < -1000 {
        x *= 2f64.powi(-1000);
        e += 1000;
        if x == 0.0 {
            return x;
        }
    }
    x * 2f64.powi(e)
}

#[cfg(test)]
mod tests;
