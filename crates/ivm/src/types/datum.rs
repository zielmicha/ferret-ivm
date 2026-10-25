//! Runtime SQL values.

use super::SqlType;
use crate::error::{codes, Result, SqlError};
use pgdatetime::Interval;
use pgfuncs::json::Jsonb;
use pgnumeric::Numeric;
use std::cmp::Ordering;
use std::sync::Arc;

/// A SQL value. Heavy payloads are reference counted so rows clone cheaply.
#[derive(Clone, Debug)]
pub enum Datum {
    Null,
    Bool(bool),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Float4(f32),
    Float8(f64),
    Numeric(Arc<Numeric>),
    /// text, varchar, bpchar, name
    Text(Arc<str>),
    /// json (text-preserving)
    Json(Arc<str>),
    Bytea(Arc<[u8]>),
    Date(i32),
    Time(i64),
    Timestamp(i64),
    TimestampTz(i64),
    Interval(Interval),
    Uuid([u8; 16]),
    Jsonb(Arc<Jsonb>),
    Array(Arc<ArrayValue>),
    Oid(u32),
}

/// A (possibly multi-dimensional) array value.
#[derive(Clone, Debug)]
pub struct ArrayValue {
    pub elem_type: SqlType,
    /// (lower bound, length) per dimension; empty for an empty array.
    pub dims: Vec<(i32, i32)>,
    pub elems: Vec<Datum>,
}

/// PostgreSQL's limit on array dimensions (MAXDIM).
pub const MAX_ARRAY_DIMS: usize = 6;

fn array_err(code: &'static str, msg: &str) -> SqlError {
    SqlError::new(code, msg.to_string())
}

fn too_many_dims(n: usize) -> SqlError {
    SqlError::new(
        codes::PROGRAM_LIMIT_EXCEEDED,
        format!("number of array dimensions ({n}) exceeds the maximum allowed ({MAX_ARRAY_DIMS})"),
    )
}

impl ArrayValue {
    pub fn new_1d(elem_type: SqlType, elems: Vec<Datum>) -> ArrayValue {
        let dims = if elems.is_empty() { vec![] } else { vec![(1, elems.len() as i32)] };
        ArrayValue { elem_type, dims, elems }
    }

    pub fn empty(elem_type: SqlType) -> ArrayValue {
        ArrayValue { elem_type, dims: vec![], elems: vec![] }
    }

    /// ARRAY[a, b, ...] over arrays: the sub-arrays stacked along a new first
    /// dimension. NULL and empty sub-arrays are allowed only if all are.
    pub fn stack(elem_type: SqlType, subs: &[Datum]) -> Result<ArrayValue> {
        let mismatch = || {
            array_err(codes::ARRAY_SUBSCRIPT_ERROR, "multidimensional arrays must have array expressions with matching dimensions")
        };
        let mut inner: Option<&[(i32, i32)]> = None;
        let mut have_empty = false;
        let mut elems = Vec::new();
        let mut n = 0;
        for d in subs {
            match d {
                Datum::Array(a) if !a.dims.is_empty() => {
                    match inner {
                        None => inner = Some(&a.dims),
                        Some(f) if f != a.dims.as_slice() => return Err(mismatch()),
                        Some(_) => {}
                    }
                    elems.extend(a.elems.iter().cloned());
                    n += 1;
                }
                _ => have_empty = true,
            }
        }
        let Some(inner) = inner else { return Ok(ArrayValue::empty(elem_type)) };
        if have_empty {
            return Err(mismatch());
        }
        if inner.len() + 1 > MAX_ARRAY_DIMS {
            return Err(too_many_dims(inner.len() + 1));
        }
        let mut dims = vec![(1, n)];
        dims.extend_from_slice(inner);
        Ok(ArrayValue { elem_type, dims, elems })
    }

    /// array_agg / ARRAY(subquery) over arrays: like `stack`, but NULL and
    /// empty inputs are errors.
    pub fn accumulate(elem_type: SqlType, subs: &[Datum]) -> Result<ArrayValue> {
        let mut inner: Option<&[(i32, i32)]> = None;
        for d in subs {
            match d {
                Datum::Null => return Err(array_err(codes::NULL_VALUE_NOT_ALLOWED, "cannot accumulate null arrays")),
                Datum::Array(a) if a.dims.is_empty() => {
                    return Err(array_err(codes::ARRAY_SUBSCRIPT_ERROR, "cannot accumulate empty arrays"));
                }
                Datum::Array(a) => match inner {
                    None => inner = Some(&a.dims),
                    Some(f) if f != a.dims.as_slice() => {
                        return Err(array_err(codes::ARRAY_SUBSCRIPT_ERROR, "cannot accumulate arrays of different dimensionality"));
                    }
                    Some(_) => {}
                },
                _ => return Err(SqlError::internal("array expected")),
            }
        }
        ArrayValue::stack(elem_type, subs)
    }

    /// Row-major offset of the element at `idx` (one subscript per
    /// dimension), None if out of range or the count differs.
    fn offset(&self, idx: &[i64]) -> Option<usize> {
        if idx.len() != self.dims.len() || idx.is_empty() {
            return None;
        }
        let mut off = 0usize;
        for (&(lb, len), &i) in self.dims.iter().zip(idx) {
            let k = i - lb as i64;
            if k < 0 || k >= len as i64 {
                return None;
            }
            off = off * len as usize + k as usize;
        }
        Some(off)
    }

    /// a[i][j]...: NULL unless there is one in-range subscript per dimension.
    pub fn get(&self, idx: &[i64]) -> Datum {
        self.offset(idx).map(|o| self.elems[o].clone()).unwrap_or(Datum::Null)
    }

    /// a[lo:hi][lo:hi]...: bounds are clamped to the array's; dimensions
    /// beyond the given ones are taken whole; the result is 1-based.
    pub fn slice(&self, bounds: &[(i64, i64)]) -> ArrayValue {
        let nd = self.dims.len();
        if nd == 0 || bounds.len() > nd {
            return ArrayValue::empty(self.elem_type.clone());
        }
        let mut ranges = Vec::with_capacity(nd);
        for (d, &(lb, len)) in self.dims.iter().enumerate() {
            let (lb, ub) = (lb as i64, lb as i64 + len as i64 - 1);
            let (lo, hi) = bounds.get(d).map(|&(lo, hi)| (lo.max(lb), hi.min(ub))).unwrap_or((lb, ub));
            if lo > hi {
                return ArrayValue::empty(self.elem_type.clone());
            }
            ranges.push(((lo - lb) as usize, (hi - lb) as usize));
        }
        let mut strides = vec![1usize; nd];
        for d in (0..nd - 1).rev() {
            strides[d] = strides[d + 1] * self.dims[d + 1].1 as usize;
        }
        let mut elems = Vec::new();
        let mut cur: Vec<usize> = ranges.iter().map(|r| r.0).collect();
        loop {
            elems.push(self.elems[cur.iter().zip(&strides).map(|(c, s)| c * s).sum::<usize>()].clone());
            // advance the last dimension first
            let mut d = nd;
            loop {
                if d == 0 {
                    let dims = ranges.iter().map(|r| (1, (r.1 - r.0 + 1) as i32)).collect();
                    return ArrayValue { elem_type: self.elem_type.clone(), dims, elems };
                }
                d -= 1;
                if cur[d] < ranges[d].1 {
                    cur[d] += 1;
                    break;
                }
                cur[d] = ranges[d].0;
            }
        }
    }

    /// array_append: empty or one-dimensional arrays; keeps the lower bound.
    pub fn append(&self, e: Datum) -> Result<ArrayValue> {
        self.insert_1d(e, false)
    }

    /// array_prepend: empty or one-dimensional arrays; keeps the lower bound.
    pub fn prepend(&self, e: Datum) -> Result<ArrayValue> {
        self.insert_1d(e, true)
    }

    fn insert_1d(&self, e: Datum, front: bool) -> Result<ArrayValue> {
        let mut a = self.clone();
        match a.dims.len() {
            0 => a.dims = vec![(1, 1)],
            1 => a.dims[0].1 += 1,
            _ => return Err(array_err(codes::DATA_EXCEPTION, "argument must be empty or one-dimensional array")),
        }
        if front {
            a.elems.insert(0, e);
        } else {
            a.elems.push(e);
        }
        Ok(a)
    }

    /// array_cat / || on arrays of equal dimensionality or differing by one.
    pub fn concat(a: &ArrayValue, b: &ArrayValue) -> Result<ArrayValue> {
        let (n1, n2) = (a.dims.len(), b.dims.len());
        if n1 == 0 {
            return Ok(b.clone());
        }
        if n2 == 0 {
            return Ok(a.clone());
        }
        let incompatible = |detail: String| {
            SqlError::new(codes::ARRAY_SUBSCRIPT_ERROR, "cannot concatenate incompatible arrays").with_detail(detail)
        };
        let dims = if n1 == n2 {
            if a.dims[1..] != b.dims[1..] {
                return Err(incompatible("Arrays with differing element dimensions are not compatible for concatenation.".into()));
            }
            let mut d = a.dims.clone();
            d[0].1 += b.dims[0].1;
            d
        } else if n1 + 1 == n2 {
            if a.dims[..] != b.dims[1..] {
                return Err(incompatible("Arrays with differing dimensions are not compatible for concatenation.".into()));
            }
            let mut d = b.dims.clone();
            d[0].1 += 1;
            d
        } else if n1 == n2 + 1 {
            if a.dims[1..] != b.dims[..] {
                return Err(incompatible("Arrays with differing dimensions are not compatible for concatenation.".into()));
            }
            let mut d = a.dims.clone();
            d[0].1 += 1;
            d
        } else {
            return Err(incompatible(format!("Arrays of {n1} and {n2} dimensions are not compatible for concatenation.")));
        };
        let mut elems = Vec::with_capacity(a.elems.len() + b.elems.len());
        elems.extend(a.elems.iter().cloned());
        elems.extend(b.elems.iter().cloned());
        Ok(ArrayValue { elem_type: a.elem_type.clone(), dims, elems })
    }
}

pub type Row = Vec<Datum>;

impl Datum {
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Datum::Null)
    }

    pub fn text(s: &str) -> Datum {
        Datum::Text(Arc::from(s))
    }
    pub fn text_owned(s: String) -> Datum {
        Datum::Text(Arc::from(s.into_boxed_str()))
    }
    pub fn numeric(n: Numeric) -> Datum {
        Datum::Numeric(Arc::new(n))
    }
    pub fn bytea(b: Vec<u8>) -> Datum {
        Datum::Bytea(Arc::from(b.into_boxed_slice()))
    }
    pub fn jsonb(j: Jsonb) -> Datum {
        Datum::Jsonb(Arc::new(j))
    }
    pub fn array(a: ArrayValue) -> Datum {
        Datum::Array(Arc::new(a))
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Datum::Bool(b) => Some(*b),
            _ => None,
        }
    }
    /// Integer value of an integer-typed datum.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Datum::Int2(v) => Some(*v as i64),
            Datum::Int4(v) => Some(*v as i64),
            Datum::Int8(v) => Some(*v),
            Datum::Oid(v) => Some(*v as i64),
            _ => None,
        }
    }
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Datum::Float4(v) => Some(*v as f64),
            Datum::Float8(v) => Some(*v),
            _ => None,
        }
    }
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Datum::Text(s) | Datum::Json(s) => Some(s),
            _ => None,
        }
    }
    pub fn as_numeric(&self) -> Option<&Numeric> {
        match self {
            Datum::Numeric(n) => Some(n),
            _ => None,
        }
    }
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Datum::Bytea(b) => Some(b),
            _ => None,
        }
    }
    pub fn as_interval(&self) -> Option<Interval> {
        match self {
            Datum::Interval(i) => Some(*i),
            _ => None,
        }
    }

    /// SQL comparison of two non-null values of the same type (PostgreSQL
    /// btree ordering, C.UTF-8 collation). NULLs sort last.
    pub fn sql_cmp(&self, other: &Datum) -> Ordering {
        use Datum::*;
        match (self, other) {
            (Null, Null) => Ordering::Equal,
            (Null, _) => Ordering::Greater,
            (_, Null) => Ordering::Less,
            (Bool(a), Bool(b)) => a.cmp(b),
            (Int2(a), Int2(b)) => a.cmp(b),
            (Int4(a), Int4(b)) => a.cmp(b),
            (Int8(a), Int8(b)) => a.cmp(b),
            (Float4(a), Float4(b)) => float_cmp(*a as f64, *b as f64),
            (Float8(a), Float8(b)) => float_cmp(*a, *b),
            (Numeric(a), Numeric(b)) => a.cmp(b),
            (Text(a), Text(b)) => a.as_bytes().cmp(b.as_bytes()),
            (Json(a), Json(b)) => a.as_bytes().cmp(b.as_bytes()),
            (Bytea(a), Bytea(b)) => a.cmp(b),
            (Date(a), Date(b)) => a.cmp(b),
            (Time(a), Time(b)) => a.cmp(b),
            (Timestamp(a), Timestamp(b)) => a.cmp(b),
            (TimestampTz(a), TimestampTz(b)) => a.cmp(b),
            (Interval(a), Interval(b)) => pgdatetime::interval_cmp(*a, *b),
            (Uuid(a), Uuid(b)) => a.cmp(b),
            (Jsonb(a), Jsonb(b)) => a.cmp(b),
            (Oid(a), Oid(b)) => a.cmp(b),
            (Array(a), Array(b)) => array_cmp(a, b),
            // Mixed integer widths can meet in constant folding; compare numerically.
            (a, b) => match (a.as_i64(), b.as_i64()) {
                (Some(x), Some(y)) => x.cmp(&y),
                _ => panic!("sql_cmp on mismatched datums: {a:?} vs {b:?}"),
            },
        }
    }

    /// SQL equality of two non-null values (`=` semantics).
    pub fn sql_eq(&self, other: &Datum) -> bool {
        self.sql_cmp(other) == Ordering::Equal
    }

    /// Approximate heap + inline size, for memory accounting.
    pub fn size_hint(&self) -> usize {
        24 + match self {
            Datum::Text(s) | Datum::Json(s) => s.len(),
            Datum::Bytea(b) => b.len(),
            Datum::Numeric(_) => 40,
            Datum::Jsonb(_) => 64,
            Datum::Array(a) => a.elems.iter().map(|e| e.size_hint()).sum(),
            _ => 0,
        }
    }
}

/// PostgreSQL float ordering: NaN equals NaN and sorts above everything; -0 == 0.
pub fn float_cmp(a: f64, b: f64) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        _ => a.partial_cmp(&b).unwrap(),
    }
}

fn array_cmp(a: &ArrayValue, b: &ArrayValue) -> Ordering {
    for (x, y) in a.elems.iter().zip(b.elems.iter()) {
        let c = x.sql_cmp(y);
        if c != Ordering::Equal {
            return c;
        }
    }
    // then (as PostgreSQL) element count, dimensionality, lengths, bounds
    a.elems
        .len()
        .cmp(&b.elems.len())
        .then_with(|| a.dims.len().cmp(&b.dims.len()))
        .then_with(|| a.dims.iter().map(|d| d.1).cmp(b.dims.iter().map(|d| d.1)))
        .then_with(|| a.dims.iter().map(|d| d.0).cmp(b.dims.iter().map(|d| d.0)))
}

/// Exact identity order of datums: unlike SQL comparison it distinguishes
/// representations (numeric scale, -0.0, interval fields, NaN payloads are
/// canonical). Used to consolidate deltas without encoding rows.
pub fn exact_cmp(a: &Datum, b: &Datum) -> Ordering {
    use Datum::*;
    fn tag(d: &Datum) -> u8 {
        match d {
            Null => 0,
            Bool(_) => 1,
            Int2(_) => 2,
            Int4(_) => 3,
            Int8(_) => 4,
            Float4(_) => 5,
            Float8(_) => 6,
            Numeric(_) => 7,
            Text(_) => 8,
            Json(_) => 9,
            Bytea(_) => 10,
            Date(_) => 11,
            Time(_) => 12,
            Timestamp(_) => 13,
            TimestampTz(_) => 14,
            Interval(_) => 15,
            Uuid(_) => 16,
            Jsonb(_) => 17,
            Array(_) => 18,
            Oid(_) => 19,
        }
    }
    match (a, b) {
        (Float4(x), Float4(y)) => x.to_bits().cmp(&y.to_bits()),
        (Float8(x), Float8(y)) => x.to_bits().cmp(&y.to_bits()),
        (Numeric(x), Numeric(y)) => x.cmp(y).then_with(|| x.dscale().cmp(&y.dscale())),
        (Interval(x), Interval(y)) => (x.months, x.days, x.micros).cmp(&(y.months, y.days, y.micros)),
        (Jsonb(x), Jsonb(y)) => x.cmp(y).then_with(|| x.to_string().cmp(&y.to_string())),
        (Array(x), Array(y)) => {
            for (p, q) in x.elems.iter().zip(&y.elems) {
                let c = exact_cmp(p, q);
                if c != Ordering::Equal {
                    return c;
                }
            }
            x.elems.len().cmp(&y.elems.len()).then_with(|| x.dims.cmp(&y.dims))
        }
        _ if tag(a) == tag(b) => a.sql_cmp(b),
        _ => tag(a).cmp(&tag(b)),
    }
}

pub fn exact_row_cmp(a: &[Datum], b: &[Datum]) -> Ordering {
    for (x, y) in a.iter().zip(b.iter()) {
        let c = exact_cmp(x, y);
        if c != Ordering::Equal {
            return c;
        }
    }
    a.len().cmp(&b.len())
}

/// Compare rows column-wise with SQL semantics (NULLs last).
pub fn row_cmp(a: &[Datum], b: &[Datum]) -> Ordering {
    for (x, y) in a.iter().zip(b.iter()) {
        let c = x.sql_cmp(y);
        if c != Ordering::Equal {
            return c;
        }
    }
    a.len().cmp(&b.len())
}
