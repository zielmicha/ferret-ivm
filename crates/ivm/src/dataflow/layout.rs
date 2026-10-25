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
        let payload = (0..schema.len()).filter(|c| row_from_key[*c].is_none()).collect();
        let summary_kinds = summaries.iter().map(|(k, _)| *k).collect();
        Layout { key, key_types, specs, row_from_key, payload, row_len: schema.len(), summaries, summary_kinds }
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
        let row = self.layout.decode_row(key)?;
        for (i, (kind, e)) in self.layout.summaries.iter().enumerate() {
            let v = e.eval(&row)?;
            kind.add(&mut acc.aggs[i], &v, weight)?;
        }
        Ok(())
    }

    fn update_entry(&self, acc: &mut Summary, key: &[u8], before: i64, after: i64) -> Result<bool> {
        acc.count += after - before;
        if before == after || self.layout.summaries.is_empty() {
            return Ok(true);
        }
        let row = self.layout.decode_row(key)?;
        for (i, (kind, e)) in self.layout.summaries.iter().enumerate() {
            let v = e.eval(&row)?;
            if !kind.update(&mut acc.aggs[i], &v, before, after)? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
