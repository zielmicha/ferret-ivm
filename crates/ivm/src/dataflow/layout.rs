//! Arrangement layouts: how rows of a collection map to index entries.
//!
//! An entry is `memcmp(key values) ++ fidelity(key values) ++ codec(payload)`:
//! the key part orders and groups entries (key prefixes are lookup ranges),
//! the payload carries the remaining columns exactly.

use crate::error::{Result, SqlError};
use crate::expr::Expr;
use crate::storage::{AggKind, Summarizer, Summary};
use crate::types::codec;
use crate::types::encoding::{self, SortSpec};
use crate::types::{ColumnType, Datum, Row, SqlType};
use std::sync::Arc;

#[derive(Debug)]
pub struct Layout {
    /// Key expressions over the collection's rows.
    pub key: Vec<Expr>,
    pub key_types: Vec<SqlType>,
    pub specs: Vec<SortSpec>,
    /// For each row column: position of a plain `Column` key expression.
    pub row_from_key: Vec<Option<usize>>,
    /// Row columns stored in the payload (not recoverable from the key).
    pub payload: Vec<usize>,
    pub row_len: usize,
    /// Per-node summary aggregates over rows: (kind, argument expression).
    pub summaries: Vec<(AggKind, Expr)>,
    summary_kinds: Vec<AggKind>,
    /// What summaries need decoded from an entry (None: the whole row).
    summary_decode: Option<PartialDecode>,
}

/// Columns to decode from an entry, the rest being skipped without
/// materializing them.
#[derive(Debug, Clone)]
pub struct PartialDecode {
    /// Per key column: decode it (its value is a needed row column, or it
    /// must be decoded to find the end of its fidelity suffix).
    key: Vec<bool>,
    /// Per payload position: decode it.
    payload: Vec<bool>,
    /// Payload positions after the last needed one are not looked at.
    payload_len: usize,
    /// Needed row columns taken from key values: (row column, key column).
    from_key: Vec<(usize, usize)>,
}

impl Layout {
    pub fn new(key: Vec<Expr>, specs: Vec<SortSpec>, schema: &[ColumnType], summaries: Vec<(AggKind, Expr)>) -> Layout {
        let key_types: Vec<SqlType> = key.iter().map(|k| k.ty(schema)).collect();
        let mut row_from_key = vec![None; schema.len()];
        for (i, k) in key.iter().enumerate() {
            if let Expr::Column(c) = k
                && row_from_key[*c].is_none()
            {
                row_from_key[*c] = Some(i);
            }
        }
        let payload: Vec<usize> = (0..schema.len()).filter(|c| row_from_key[*c].is_none()).collect();
        let summary_kinds = summaries.iter().map(|(k, _)| *k).collect();
        let needed: Vec<usize> = summaries.iter().flat_map(|(_, e)| e.column_set()).collect();
        let mut layout = Layout {
            key,
            key_types,
            specs,
            row_from_key,
            payload,
            row_len: schema.len(),
            summaries,
            summary_kinds,
            summary_decode: None,
        };
        if !layout.summaries.is_empty() {
            layout.summary_decode = layout.decode_plan(&needed);
        }
        layout
    }

    /// How to decode just the row columns `needed` from entries (None when
    /// that is all of them).
    pub fn decode_plan(&self, needed: &[usize]) -> Option<PartialDecode> {
        let mut needed = needed.to_vec();
        needed.sort();
        needed.dedup();
        if needed.len() >= self.row_len {
            return None;
        }
        let mut kd: Vec<bool> = self.key_types.iter().map(|t| matches!(t, SqlType::Array(_)) && encoding::has_fidelity(t)).collect();
        let mut from_key = Vec::new();
        for &c in &needed {
            if let Some(k) = self.row_from_key[c] {
                kd[k] = true;
                from_key.push((c, k));
            }
        }
        let pd: Vec<bool> = self.payload.iter().map(|c| needed.contains(c)).collect();
        let payload_len = pd.iter().rposition(|x| *x).map(|i| i + 1).unwrap_or(0);
        Some(PartialDecode { key: kd, payload: pd, payload_len, from_key })
    }

    /// Decode an entry's row columns of a `decode_plan` (all of them for
    /// None); the other columns are NULL.
    pub fn decode_row_with(&self, buf: &[u8], plan: Option<&PartialDecode>) -> Result<Row> {
        match plan {
            None => self.decode_row(buf),
            Some(_) => {
                let mut row = vec![Datum::Null; self.row_len];
                self.decode_columns(buf, plan, &mut row)?;
                Ok(row)
            }
        }
    }

    /// Decode the row columns the summary expressions read into `row`
    /// (of length `row_len`); other columns are left untouched.
    fn decode_summary_columns(&self, buf: &[u8], row: &mut [Datum]) -> Result<()> {
        self.decode_columns(buf, self.summary_decode.as_ref(), row)
    }

    /// Decode the row columns of `plan` (all for None) into `row` (of
    /// length `row_len`); other columns are left untouched.
    fn decode_columns(&self, buf: &[u8], plan: Option<&PartialDecode>, row: &mut [Datum]) -> Result<()> {
        let Some(pd) = plan else {
            for (c, v) in self.decode_row(buf)?.into_iter().enumerate() {
                row[c] = v;
            }
            return Ok(());
        };
        let mut pos = 0;
        // key values that are decoded (in key order); None marks a skipped NULL-or-value
        let mut vals: smallvec::SmallVec<[Option<Datum>; 8]> = smallvec::SmallVec::new();
        let mut nulls: smallvec::SmallVec<[bool; 8]> = smallvec::SmallVec::new();
        for (k, (t, spec)) in self.key_types.iter().zip(&self.specs).enumerate() {
            let rest = buf.get(pos..).ok_or_else(|| SqlError::corrupt("truncated key"))?;
            if pd.key[k] {
                let (d, n) = encoding::decode_key(rest, t, *spec)?;
                nulls.push(d.is_null());
                vals.push(Some(d));
                pos += n;
            } else {
                nulls.push(matches!(rest.first(), Some(&encoding::TAG_NULL_FIRST) | Some(&encoding::TAG_NULL_LAST)));
                vals.push(None);
                pos += encoding::skip_key(rest, t, *spec)?;
            }
        }
        for (k, t) in self.key_types.iter().enumerate() {
            if nulls[k] || !encoding::has_fidelity(t) {
                continue;
            }
            let rest = buf.get(pos..).ok_or_else(|| SqlError::corrupt("truncated fidelity"))?;
            match &mut vals[k] {
                Some(v) => pos += encoding::apply_fidelity(v, rest)?,
                None => pos += encoding::skip_fidelity(rest, t)?,
            }
        }
        let mut rd = buf.get(pos..).ok_or_else(|| SqlError::corrupt("truncated entry"))?;
        for (i, c) in self.payload[..pd.payload_len].iter().enumerate() {
            if pd.payload[i] {
                row[*c] = codec::decode_datum(&mut rd)?;
            } else {
                codec::skip_datum(&mut rd)?;
            }
        }
        for &(c, k) in &pd.from_key {
            if let Some(v) = &vals[k] {
                row[c] = v.clone();
            }
        }
        Ok(())
    }

    /// Layout keyed by all columns (in order).
    pub fn full(schema: &[ColumnType]) -> Layout {
        let key = (0..schema.len()).map(Expr::Column).collect();
        Layout::new(key, vec![SortSpec::ASC; schema.len()], schema, vec![])
    }

    pub fn key_len(&self) -> usize {
        self.key.len()
    }

    /// Evaluate the key values of a row.
    pub fn key_values(&self, row: &[Datum]) -> Result<Vec<Datum>> {
        self.key.iter().map(|k| k.eval(row)).collect()
    }

    /// Encode a full entry for `row`.
    pub fn encode_row(&self, row: &[Datum]) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(32);
        let mut kv = Vec::with_capacity(self.key.len());
        for (k, spec) in self.key.iter().zip(&self.specs) {
            let v = match k {
                Expr::Column(c) => row[*c].clone(),
                e => e.eval(row)?,
            };
            encoding::encode_key(&v, *spec, &mut out);
            kv.push(v);
        }
        for (v, t) in kv.iter().zip(&self.key_types) {
            if !v.is_null() && encoding::has_fidelity(t) {
                encoding::encode_fidelity(v, &mut out);
            }
        }
        for c in &self.payload {
            codec::encode_datum(&row[*c], &mut out);
        }
        Ok(out)
    }

    /// Encode a key prefix (the first `vals.len()` key columns, canonical).
    pub fn encode_prefix(&self, vals: &[Datum]) -> Vec<u8> {
        let mut out = Vec::with_capacity(16);
        for (v, spec) in vals.iter().zip(&self.specs) {
            encoding::encode_key(v, *spec, &mut out);
        }
        out
    }

    /// Decode the key values (canonical, without fidelity) and the byte
    /// offset where the canonical key part ends.
    pub fn decode_key_values(&self, buf: &[u8]) -> Result<(Vec<Datum>, usize)> {
        let mut pos = 0;
        let mut vals = Vec::with_capacity(self.key.len());
        for (t, spec) in self.key_types.iter().zip(&self.specs) {
            let (d, n) = encoding::decode_key(&buf[pos..], t, *spec)?;
            pos += n;
            vals.push(d);
        }
        Ok((vals, pos))
    }

    /// Decode an entry back into a row.
    pub fn decode_row(&self, buf: &[u8]) -> Result<Row> {
        let (mut vals, mut pos) = self.decode_key_values(buf)?;
        for (v, t) in vals.iter_mut().zip(&self.key_types) {
            if !v.is_null() && encoding::has_fidelity(t) {
                pos += encoding::apply_fidelity(v, &buf[pos..])?;
            }
        }
        let mut row: Row = vec![Datum::Null; self.row_len];
        let mut rd = &buf[pos..];
        for c in &self.payload {
            row[*c] = codec::decode_datum(&mut rd)?;
        }
        for (c, k) in self.row_from_key.iter().enumerate() {
            if let Some(k) = k {
                row[c] = vals[*k].clone();
            }
        }
        if !rd.is_empty() {
            return Err(SqlError::corrupt("trailing bytes in arrangement entry"));
        }
        Ok(row)
    }

    /// Canonical key bytes of an entry (prefix up to the end of the key).
    pub fn key_bytes<'a>(&self, buf: &'a [u8]) -> Result<&'a [u8]> {
        let (_, pos) = self.decode_key_values(buf)?;
        Ok(&buf[..pos])
    }
}

/// Summarizer computing a layout's per-node aggregates.
pub struct LayoutSummarizer {
    pub layout: Arc<Layout>,
}

impl Summarizer for LayoutSummarizer {
    fn spec(&self) -> &[AggKind] {
        &self.layout.summary_kinds
    }

    fn add_entry(&self, acc: &mut Summary, key: &[u8], weight: i64) -> Result<()> {
        acc.count += weight;
        if weight == 0 || self.layout.summaries.is_empty() {
            return Ok(());
        }
        let mut row: SmallRow = smallvec::smallvec![Datum::Null; self.layout.row_len];
        self.layout.decode_summary_columns(key, &mut row)?;
        for (i, (kind, e)) in self.layout.summaries.iter().enumerate() {
            match e {
                Expr::Column(c) => kind.add(&mut acc.aggs[i], &row[*c], weight)?,
                e => kind.add(&mut acc.aggs[i], &e.eval(&row)?, weight)?,
            }
        }
        Ok(())
    }

    fn add_entries(&self, acc: &mut Summary, entries: &mut dyn Iterator<Item = (&[u8], i64)>) -> Result<()> {
        if self.layout.summaries.is_empty() {
            for (_, w) in entries {
                acc.count += w;
            }
            return Ok(());
        }
        // one row buffer for all entries: decoded columns are overwritten
        let mut row: SmallRow = smallvec::smallvec![Datum::Null; self.layout.row_len];
        for (key, weight) in entries {
            acc.count += weight;
            if weight == 0 {
                continue;
            }
            self.layout.decode_summary_columns(key, &mut row)?;
            for (i, (kind, e)) in self.layout.summaries.iter().enumerate() {
                match e {
                    Expr::Column(c) => kind.add(&mut acc.aggs[i], &row[*c], weight)?,
                    e => kind.add(&mut acc.aggs[i], &e.eval(&row)?, weight)?,
                }
            }
        }
        Ok(())
    }

    fn update_entry(&self, acc: &mut Summary, key: &[u8], before: i64, after: i64) -> Result<bool> {
        acc.count += after - before;
        if before == after || self.layout.summaries.is_empty() {
            return Ok(true);
        }
        let mut row: SmallRow = smallvec::smallvec![Datum::Null; self.layout.row_len];
        self.layout.decode_summary_columns(key, &mut row)?;
        for (i, (kind, e)) in self.layout.summaries.iter().enumerate() {
            let ok = match e {
                Expr::Column(c) => kind.update(&mut acc.aggs[i], &row[*c], before, after)?,
                e => kind.update(&mut acc.aggs[i], &e.eval(&row)?, before, after)?,
            };
            if !ok {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// A row buffer kept on the stack for narrow rows.
type SmallRow = smallvec::SmallVec<[Datum; 8]>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::datum::exact_cmp;
    use std::cmp::Ordering;

    /// Partial decoding for summaries yields exactly the columns a full
    /// decode does, for keys of every kind (skipped, decoded, with fidelity,
    /// descending, computed) and payload columns before and after.
    #[test]
    fn summary_columns_match_full_decode() {
        let ty = |t: SqlType| ColumnType::new(t);
        let schema = vec![
            ty(SqlType::Text),
            ty(SqlType::Numeric),
            ty(SqlType::Int8),
            ty(SqlType::Float8),
            ty(SqlType::Interval),
            ty(SqlType::Text),
            ty(SqlType::Numeric),
            ty(SqlType::Array(Box::new(SqlType::Numeric))),
        ];
        let num = |s: &str| Datum::numeric(s.parse().unwrap());
        let rows: Vec<Row> = vec![
            vec![
                Datum::text("a\0b"),
                num("1.50"),
                Datum::Int8(-7),
                Datum::Float8(-0.0),
                Datum::Interval(pgdatetime::Interval { months: 1, days: 2, micros: 3 }),
                Datum::text("x"),
                num("-2.000"),
                Datum::array(crate::types::ArrayValue::new_1d(SqlType::Numeric, vec![num("1.0"), Datum::Null])),
            ],
            vec![Datum::Null, Datum::Null, Datum::Null, Datum::Null, Datum::Null, Datum::Null, Datum::Null, Datum::Null],
            vec![
                Datum::text(""),
                num("NaN"),
                Datum::Int8(i64::MAX),
                Datum::Float8(f64::NAN),
                Datum::Interval(pgdatetime::Interval { months: 0, days: 30, micros: 0 }),
                Datum::Null,
                num("1e-20"),
                Datum::Null,
            ],
        ];
        // key: text, numeric (desc), an expression, float, interval, the array;
        // payload: columns 2, 5, 6
        let key = vec![Expr::Column(0), Expr::Column(1), Expr::Column(2), Expr::Column(3), Expr::Column(4), Expr::Column(7)];
        let mut specs = vec![SortSpec::ASC; key.len()];
        specs[1] = SortSpec::DESC;
        let mut key = key;
        key[2] = Expr::Coalesce(vec![Expr::Column(2), Expr::lit(Datum::Int8(0), SqlType::Int8)], SqlType::Int8);
        for needed in [vec![6], vec![1], vec![3, 5], vec![4], vec![2, 6], vec![0, 7], vec![5]] {
            let summaries: Vec<(AggKind, Expr)> = needed.iter().map(|c| (AggKind::Max, Expr::Column(*c))).collect();
            let layout = Layout::new(key.clone(), specs.clone(), &schema, summaries);
            for row in &rows {
                let enc = layout.encode_row(row).unwrap();
                let full = layout.decode_row(&enc).unwrap();
                let mut part = vec![Datum::Bool(true); schema.len()];
                layout.decode_summary_columns(&enc, &mut part).unwrap();
                for c in &needed {
                    assert_eq!(exact_cmp(&part[*c], &full[*c]), Ordering::Equal, "column {c} of {row:?}: {:?} vs {:?}", part[*c], full[*c]);
                }
            }
        }
    }
}
