//! Batch execution of logical plans against arrangements (point-in-time
//! reads, including time travel via MVCC histories, and WITH RECURSIVE).

use crate::dataflow::agg::AggSpec;
use crate::dataflow::ops::RecursivePlan;
use crate::dataflow::{consolidate, Graph, NodeId};
use crate::error::{codes, Result, SqlError};
use crate::expr::func::func_def;
use crate::expr::Expr;
use crate::sql::plan::{JoinKind, Rel, SetOpKind, SortKey};
use crate::storage::{AggKind, Env, ReadAt, Summary};
use crate::types::encoding::{self, SortSpec};
use crate::types::{Datum, Row, SqlType};
use rustc_hash::FxHashMap;
use std::cmp::Ordering;

pub type Rows = Vec<(Row, i64)>;

pub struct ExecCtx<'a> {
    pub g: &'a Graph,
    pub env: &'a mut Env,
    pub at: ReadAt,
    pub objects: &'a dyn Fn(u64) -> Option<NodeId>,
    pub rec: FxHashMap<u64, Rows>,
}

const MAX_RECURSION: usize = 1_000_000;

fn key_bytes(vals: &[Datum]) -> Vec<u8> {
    let mut out = Vec::new();
    for v in vals {
        encoding::encode_key(v, SortSpec::ASC, &mut out);
    }
    out
}

fn row_key(row: &[Datum]) -> Vec<u8> {
    key_bytes(row)
}

/// Canonical row encoding with fidelity (exact identity).
fn row_identity(row: &[Datum]) -> Vec<u8> {
    let mut out = Vec::new();
    crate::types::codec::encode_row(row, &mut out);
    out
}

impl<'a> ExecCtx<'a> {
    pub fn execute(&mut self, rel: &Rel) -> Result<Rows> {
        match rel {
            Rel::Get { id, name, .. } => {
                let node = (self.objects)(*id)
                    .ok_or_else(|| SqlError::new(codes::UNDEFINED_TABLE, format!("relation \"{name}\" is not readable")))?;
                self.read_node(node)
            }
            Rel::Values { rows, .. } => {
                let mut out = Vec::with_capacity(rows.len());
                for r in rows {
                    let row: Row = r.iter().map(|e| e.eval(&[])).collect::<Result<_>>()?;
                    out.push((row, 1));
                }
                Ok(out)
            }
            Rel::Map { input, exprs } => {
                let rows = self.execute(input)?;
                let mut out = Vec::with_capacity(rows.len());
                for (mut r, w) in rows {
                    for e in exprs {
                        let v = e.eval(&r)?;
                        r.push(v);
                    }
                    out.push((r, w));
                }
                Ok(out)
            }
            Rel::Filter { input, preds } => {
                if let Some(rows) = self.indexed_filter(input, preds)? {
                    return Ok(rows);
                }
                let rows = self.execute(input)?;
                let mut out = Vec::with_capacity(rows.len());
                'rows: for (r, w) in rows {
                    for p in preds {
                        if !p.eval_pred(&r)? {
                            continue 'rows;
                        }
                    }
                    out.push((r, w));
                }
                Ok(out)
            }
            Rel::Project { input, cols } => {
                let rows = self.execute(input)?;
                Ok(rows.into_iter().map(|(r, w)| (cols.iter().map(|c| r[*c].clone()).collect(), w)).collect())
            }
            Rel::Join { left, right, kind, on } => self.join(left, right, *kind, on),
            Rel::Aggregate { input, group, aggs } => {
                let rows = self.execute(input)?;
                let in_schema = input.schema();
                aggregate(rows, group, aggs, &in_schema)
            }
            Rel::Distinct { input } => {
                let mut rows = self.execute(input)?;
                consolidate(&mut rows);
                let mut seen = FxHashMap::default();
                let mut out = Vec::new();
                for (r, w) in rows {
                    if w <= 0 {
                        continue;
                    }
                    let k = row_key(&r);
                    if seen.insert(k, ()).is_none() {
                        out.push((r, 1));
                    }
                }
                Ok(out)
            }
            Rel::SetOp { op, all, left, right } => {
                let mut l = self.execute(left)?;
                let mut r = self.execute(right)?;
                consolidate(&mut l);
                consolidate(&mut r);
                let mut counts: Vec<(Row, i64, i64)> = Vec::new();
                let mut idx: FxHashMap<Vec<u8>, usize> = FxHashMap::default();
                for (row, w) in l {
                    let k = row_key(&row);
                    match idx.get(&k) {
                        Some(&i) => counts[i].1 += w,
                        None => {
                            idx.insert(k, counts.len());
                            counts.push((row, w, 0));
                        }
                    }
                }
                for (row, w) in r {
                    let k = row_key(&row);
                    match idx.get(&k) {
                        Some(&i) => counts[i].2 += w,
                        None => {
                            idx.insert(k, counts.len());
                            counts.push((row, 0, w));
                        }
                    }
                }
                let mut out = Vec::new();
                for (row, a, b) in counts {
                    let m = match (op, all) {
                        (SetOpKind::Except, false) => (a > 0 && b <= 0) as i64,
                        (SetOpKind::Except, true) => (a - b.max(0)).max(0),
                        (SetOpKind::Intersect, false) => (a > 0 && b > 0) as i64,
                        (SetOpKind::Intersect, true) => a.min(b).max(0),
                        (SetOpKind::Union, false) => (a > 0 || b > 0) as i64,
                        (SetOpKind::Union, true) => a + b,
                    };
                    if m > 0 {
                        out.push((row, m));
                    }
                }
                Ok(out)
            }
            Rel::UnionAll { inputs } => {
                let mut out = Vec::new();
                for i in inputs {
                    out.extend(self.execute(i)?);
                }
                Ok(out)
            }
            Rel::TopK { input, group, order, limit, offset, with_ties } => {
                let rows = self.execute(input)?;
                Ok(topk(rows, group, order, *limit, *offset, *with_ties))
            }
            Rel::Window { input, funcs } => {
                let rows = self.execute(input)?;
                let schema = input.schema();
                window(rows, funcs, &schema)
            }
            Rel::FlatMap { input, funcs, ordinality, .. } => {
                let rows = self.execute(input)?;
                let mut out = Vec::new();
                for (r, w) in rows {
                    for extra in crate::dataflow::tablefunc::eval(funcs, &r, *ordinality)? {
                        let mut row = r.clone();
                        row.extend(extra);
                        out.push((row, w));
                    }
                }
                Ok(out)
            }
            Rel::Recursive { id, base, step, distinct, .. } => self.recursive(*id, base, step, *distinct),
            Rel::RecRef { id, .. } => Ok(self.rec.get(id).cloned().unwrap_or_default()),
            Rel::Negate { input } => Ok(self.execute(input)?.into_iter().map(|(r, w)| (r, -w)).collect()),
            Rel::Apply { .. } => Err(SqlError::internal("correlated subquery survived decorrelation")),
        }
    }

    fn read_node(&mut self, node: NodeId) -> Result<Rows> {
        match self.g.arrangement_at(node, self.at) {
            Some(a) => {
                let mut rows = self.g.scan_arr(self.env, a, self.at)?;
                rows.retain(|(_, w)| *w > 0);
                Ok(rows)
            }
            None => {
                if let (ReadAt::AsOf(t), Some(a)) = (self.at, self.g.any_arrangement(node)) {
                    // (re)built later: its history before then is not kept
                    return Err(SqlError::new(
                        codes::OBJECT_NOT_IN_PREREQUISITE_STATE,
                        format!("AS OF {t} is before the history of a relation it reads, which starts at {}", self.g.arr(a).since),
                    ));
                }
                if self.at != ReadAt::Latest {
                    return Err(SqlError::unsupported("time travel requires an arranged relation"));
                }
                let mut rows = self.g.snapshot(self.env, node)?.data;
                rows.retain(|(_, w)| *w > 0);
                Ok(rows)
            }
        }
    }

    /// Filter(Get) with equality predicates on an arrangement key prefix ->
    /// index lookup instead of a scan.
    fn indexed_filter(&mut self, input: &Rel, preds: &[Expr]) -> Result<Option<Rows>> {
        let Rel::Get { id, .. } = input else { return Ok(None) };
        let Some(node) = (self.objects)(*id) else { return Ok(None) };
        // column = constant
        let mut eqs: FxHashMap<usize, Datum> = FxHashMap::default();
        for p in preds {
            if let Expr::Call(c) = p
                && func_def(c.func).name == "="
                && c.args.len() == 2
            {
                match (&c.args[0], &c.args[1]) {
                    (Expr::Column(col), Expr::Literal(d, _)) | (Expr::Literal(d, _), Expr::Column(col)) if !d.is_null() => {
                        eqs.insert(*col, d.clone());
                    }
                    _ => {}
                }
            }
        }
        if eqs.is_empty() {
            return Ok(None);
        }
        let schema = &self.g.node(node).schema;
        let mut best: Option<(usize, Vec<Datum>)> = None;
        for &a in &self.g.node(node).arrangements {
            let arr = self.g.arr(a);
            // (an index built after the read's GSN lacks its history)
            if !arr.covers(self.at) {
                continue;
            }
            let mut vals = Vec::new();
            for (k, t) in arr.layout.key.iter().zip(&arr.layout.key_types) {
                match k {
                    Expr::Column(c) if eqs.contains_key(c) && schema[*c].ty == *t => {
                        let d = &eqs[c];
                        // literal type must match exactly for a key lookup
                        let lit_ok = match (d, t) {
                            (Datum::Int2(_), SqlType::Int2) | (Datum::Int4(_), SqlType::Int4) | (Datum::Int8(_), SqlType::Int8) => true,
                            (Datum::Text(_), SqlType::Text | SqlType::Varchar | SqlType::Bpchar | SqlType::Name) => true,
                            (Datum::Numeric(_), SqlType::Numeric) => true,
                            (Datum::Date(_), SqlType::Date) | (Datum::Timestamp(_), SqlType::Timestamp) | (Datum::TimestampTz(_), SqlType::TimestampTz) => true,
                            (Datum::Uuid(_), SqlType::Uuid) | (Datum::Bool(_), SqlType::Bool) => true,
                            _ => false,
                        };
                        if !lit_ok {
                            break;
                        }
                        vals.push(d.clone());
                    }
                    _ => break,
                }
            }
            if !vals.is_empty() && best.as_ref().is_none_or(|(_, v)| vals.len() > v.len()) {
                best = Some((a, vals));
            }
        }
        let Some((arr, vals)) = best else { return Ok(None) };
        let rows = self.g.lookup(self.env, arr, &vals, self.at)?;
        let mut out = Vec::new();
        'rows: for (r, w) in rows {
            if w <= 0 {
                continue;
            }
            for p in preds {
                if !p.eval_pred(&r)? {
                    continue 'rows;
                }
            }
            out.push((r, w));
        }
        Ok(Some(out))
    }

    fn join(&mut self, left: &Rel, right: &Rel, kind: JoinKind, on: &[Expr]) -> Result<Rows> {
        let nl = left.arity();
        let nr = right.arity();
        let l = self.execute(left)?;
        let r = self.execute(right)?;
        // equi conditions (left expr, right expr, null_safe)
        let mut equi: Vec<(Expr, Expr, bool)> = Vec::new();
        let mut residual: Vec<Expr> = Vec::new();
        for c in on {
            let pair = match c {
                Expr::Call(call) if func_def(call.func).name == "=" && call.args.len() == 2 => Some((&call.args[0], &call.args[1], false)),
                Expr::IsDistinct(a, b, true) => Some((&**a, &**b, true)),
                _ => None,
            };
            if let Some((a, b, ns)) = pair {
                let (ac, bc) = (a.column_set(), b.column_set());
                if !ac.is_empty() && !bc.is_empty() {
                    if ac.iter().all(|x| *x < nl) && bc.iter().all(|x| *x >= nl) {
                        equi.push((a.clone(), b.remapped(&|x| x - nl), ns));
                        continue;
                    }
                    if bc.iter().all(|x| *x < nl) && ac.iter().all(|x| *x >= nl) {
                        equi.push((b.clone(), a.remapped(&|x| x - nl), ns));
                        continue;
                    }
                }
            }
            residual.push(c.clone());
        }
        // hash the right side
        let mut table: FxHashMap<Vec<u8>, Vec<usize>> = FxHashMap::default();
        for (i, (row, _)) in r.iter().enumerate() {
            let mut kv = Vec::with_capacity(equi.len());
            let mut skip = false;
            for (_, re, ns) in &equi {
                let v = re.eval(row)?;
                if v.is_null() && !ns {
                    skip = true;
                    break;
                }
                kv.push(v);
            }
            if !skip {
                table.entry(key_bytes(&kv)).or_default().push(i);
            }
        }
        let mut out = Vec::new();
        let mut right_matched = vec![false; r.len()];
        for (lrow, lw) in &l {
            let mut kv = Vec::with_capacity(equi.len());
            let mut skip = false;
            for (le, _, ns) in &equi {
                let v = le.eval(lrow)?;
                if v.is_null() && !ns {
                    skip = true;
                    break;
                }
                kv.push(v);
            }
            let mut matched = false;
            if !skip && let Some(cands) = table.get(&key_bytes(&kv)) {
                for &i in cands {
                    let (rrow, rw) = &r[i];
                    let mut joined = lrow.clone();
                    joined.extend(rrow.iter().cloned());
                    let mut ok = true;
                    for p in &residual {
                        if !p.eval_pred(&joined)? {
                            ok = false;
                            break;
                        }
                    }
                    if ok {
                        matched = true;
                        right_matched[i] = true;
                        match kind {
                            JoinKind::Semi | JoinKind::Anti => break,
                            _ => out.push((joined, lw * rw)),
                        }
                    }
                }
            }
            match kind {
                JoinKind::Semi if matched => out.push((lrow.clone(), *lw)),
                JoinKind::Anti if !matched => out.push((lrow.clone(), *lw)),
                JoinKind::Left | JoinKind::Full if !matched => {
                    let mut joined = lrow.clone();
                    joined.extend(std::iter::repeat_n(Datum::Null, nr));
                    out.push((joined, *lw));
                }
                _ => {}
            }
        }
        if matches!(kind, JoinKind::Right | JoinKind::Full) {
            for (i, (rrow, rw)) in r.iter().enumerate() {
                if !right_matched[i] {
                    let mut joined: Row = std::iter::repeat_n(Datum::Null, nl).collect();
                    joined.extend(rrow.iter().cloned());
                    out.push((joined, *rw));
                }
            }
        }
        Ok(out)
    }

    fn recursive(&mut self, id: u64, base: &Rel, step: &Rel, distinct: bool) -> Result<Rows> {
        let mut base_rows = self.execute(base)?;
        consolidate(&mut base_rows);
        let mut seen: FxHashMap<Vec<u8>, ()> = FxHashMap::default();
        let mut result: Rows = Vec::new();
        let mut working: Rows = Vec::new();
        for (r, w) in base_rows {
            if w <= 0 {
                continue;
            }
            if distinct {
                if seen.insert(row_key(&r), ()).is_none() {
                    result.push((r.clone(), 1));
                    working.push((r, 1));
                }
            } else {
                result.push((r.clone(), w));
                working.push((r, w));
            }
        }
        let mut iters = 0;
        while !working.is_empty() {
            iters += 1;
            if iters > MAX_RECURSION {
                return Err(SqlError::new(codes::PROGRAM_LIMIT_EXCEEDED, "recursive query exceeded the iteration limit"));
            }
            self.rec.insert(id, std::mem::take(&mut working));
            let mut produced = self.execute(step)?;
            consolidate(&mut produced);
            for (r, w) in produced {
                if w <= 0 {
                    continue;
                }
                if distinct {
                    if seen.insert(row_key(&r), ()).is_none() {
                        result.push((r.clone(), 1));
                        working.push((r, 1));
                    }
                } else {
                    result.push((r.clone(), w));
                    working.push((r, w));
                }
            }
        }
        self.rec.remove(&id);
        Ok(result)
    }
}

/// Evaluate a dataflow Recursive node's definition against current state.
pub fn eval_recursive(g: &Graph, env: &mut Env, p: &RecursivePlan) -> Result<Rows> {
    let sources = p.sources.clone();
    let objects = move |oid: u64| sources.iter().find(|(o, _)| *o == oid).map(|(_, n)| *n);
    let mut ctx = ExecCtx { g, env, at: ReadAt::Latest, objects: &objects, rec: FxHashMap::default() };
    let mut rows = ctx.recursive(p.rec_id, &p.base, &p.step, p.distinct)?;
    consolidate(&mut rows);
    Ok(rows)
}

/// Compare rows by sort keys (with full-row tiebreak for determinism).
pub fn compare_rows(a: &Row, b: &Row, order: &[SortKey]) -> Ordering {
    for k in order {
        let (x, y) = (&a[k.col], &b[k.col]);
        let c = match (x.is_null(), y.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => if k.spec.nulls_first { Ordering::Less } else { Ordering::Greater },
            (false, true) => if k.spec.nulls_first { Ordering::Greater } else { Ordering::Less },
            _ => {
                let c = x.sql_cmp(y);
                if k.spec.desc { c.reverse() } else { c }
            }
        };
        if c != Ordering::Equal {
            return c;
        }
    }
    Ordering::Equal
}

pub fn sort_rows(rows: &mut [(Row, i64)], order: &[SortKey]) {
    rows.sort_by(|a, b| compare_rows(&a.0, &b.0, order).then_with(|| row_identity(&a.0).cmp(&row_identity(&b.0))));
}

fn topk(rows: Rows, group: &[usize], order: &[SortKey], limit: Option<u64>, offset: u64, with_ties: bool) -> Rows {
    let mut groups: FxHashMap<Vec<u8>, Rows> = FxHashMap::default();
    let mut keys: Vec<Vec<u8>> = Vec::new();
    for (r, w) in rows {
        if w <= 0 {
            continue;
        }
        let kv: Vec<Datum> = group.iter().map(|c| r[*c].clone()).collect();
        let k = key_bytes(&kv);
        if !groups.contains_key(&k) {
            keys.push(k.clone());
        }
        groups.entry(k).or_default().push((r, w));
    }
    let mut out = Vec::new();
    for k in keys {
        let mut g = groups.remove(&k).unwrap();
        consolidate(&mut g);
        sort_rows(&mut g, order);
        let mut skipped = 0u64;
        let mut taken = 0u64;
        let lim = limit.unwrap_or(u64::MAX);
        let mut last: Option<Row> = None;
        for (r, mut w) in g {
            if taken >= lim {
                // past the limit only rows tied with the last one remain
                match &last {
                    Some(l) if with_ties && order.iter().all(|k| l[k.col].sql_cmp(&r[k.col]) == std::cmp::Ordering::Equal) => {
                        out.push((r, w));
                        continue;
                    }
                    _ => break,
                }
            }
            if skipped < offset {
                let s = (offset - skipped).min(w as u64);
                skipped += s;
                w -= s as i64;
                if w == 0 {
                    continue;
                }
            }
            let t = if with_ties { w as u64 } else { (lim - taken).min(w as u64) };
            taken += t;
            if with_ties {
                last = Some(r.clone());
            }
            out.push((r, t as i64));
        }
    }
    out
}

/// Hash aggregation.
pub fn aggregate(rows: Rows, group: &[usize], aggs: &[crate::sql::plan::AggCall], in_schema: &[crate::types::ColumnType]) -> Result<Rows> {
    let specs: Vec<AggSpec> = aggs
        .iter()
        .map(|a| AggSpec {
            func: a.func,
            arg: a.args.first().copied(),
            arg2: a.args.get(1).copied(),
            filter: a.filter,
            arg_type: a.args.first().map(|c| in_schema[*c].ty.clone()).unwrap_or(SqlType::Int8),
            ret: a.ret.clone(),
            fraction: a.fraction,
        })
        .collect();
    let mut kinds: Vec<(AggKind, Expr)> = Vec::new();
    let mut slots = Vec::new();
    for s in &specs {
        let mut sl = Vec::new();
        for k in s.summaries() {
            sl.push(kinds.len());
            kinds.push(k);
        }
        slots.push(sl);
    }
    let spec_kinds: Vec<AggKind> = kinds.iter().map(|(k, _)| *k).collect();
    let any_ordered = specs.iter().any(|s| s.func.is_ordered());
    struct G {
        key_row: Row,
        summary: Summary,
        rows: Rows,
    }
    let mut groups: FxHashMap<Vec<u8>, G> = FxHashMap::default();
    let mut order_keys: Vec<Vec<u8>> = Vec::new();
    // distinct aggregates: dedupe (group, args) first
    let mut distinct_seen: Vec<FxHashMap<Vec<u8>, ()>> = specs.iter().map(|_| FxHashMap::default()).collect();
    for (r, w) in rows {
        if w == 0 {
            continue;
        }
        let kv: Vec<Datum> = group.iter().map(|c| r[*c].clone()).collect();
        let k = key_bytes(&kv);
        let g = match groups.get_mut(&k) {
            Some(g) => g,
            None => {
                order_keys.push(k.clone());
                groups.entry(k.clone()).or_insert(G { key_row: kv.clone(), summary: Summary::empty(&spec_kinds), rows: vec![] })
            }
        };
        g.summary.count += w;
        for (i, s) in specs.iter().enumerate() {
            let w_eff = if aggs[i].distinct {
                let args: Vec<Datum> = aggs[i].args.iter().map(|c| r[*c].clone()).collect();
                let mut dk = k.clone();
                dk.extend(key_bytes(&args));
                if distinct_seen[i].insert(dk, ()).is_some() {
                    continue;
                }
                1
            } else {
                w
            };
            for &slot in &slots[i] {
                let (kind, e) = &kinds[slot];
                let v = e.eval(&r)?;
                kind.add(&mut g.summary.aggs[slot], &v, w_eff)?;
            }
            let _ = s;
        }
        if any_ordered {
            g.rows.push((r, w));
        }
    }
    if group.is_empty() && groups.is_empty() {
        order_keys.push(vec![]);
        groups.insert(vec![], G { key_row: vec![], summary: Summary::empty(&spec_kinds), rows: vec![] });
    }
    let mut out = Vec::with_capacity(groups.len());
    for k in order_keys {
        let mut g = groups.remove(&k).unwrap();
        if !group.is_empty() && g.summary.count <= 0 {
            continue;
        }
        let mut row = g.key_row.clone();
        for (i, s) in specs.iter().enumerate() {
            let v = if s.func.is_ordered() {
                let mut rs = std::mem::take(&mut g.rows);
                if aggs[i].distinct {
                    let mut seen = FxHashMap::default();
                    let a = aggs[i].args.clone();
                    rs.retain(|(r, _)| seen.insert(key_bytes(&a.iter().map(|c| r[*c].clone()).collect::<Vec<_>>()), ()).is_none());
                    for x in rs.iter_mut() {
                        x.1 = 1;
                    }
                }
                sort_rows(&mut rs, &aggs[i].order);
                let v = s.finalize_ordered(&mut rs.iter().cloned())?;
                g.rows = rs;
                v
            } else {
                s.finalize(&g.summary, &slots[i])?
            };
            row.push(v);
        }
        out.push((row, 1));
    }
    Ok(out)
}

fn window(rows: Rows, funcs: &[crate::sql::plan::WindowCall], schema: &[crate::types::ColumnType]) -> Result<Rows> {
    use crate::dataflow::window::{compute_partition, WinSpec};
    let mut cur: Rows = rows;
    consolidate(&mut cur);
    cur.retain(|(_, w)| *w > 0);
    let mut cur_schema = schema.to_vec();
    let mut i = 0;
    while i < funcs.len() {
        let (part, order) = (&funcs[i].partition, &funcs[i].order);
        let mut j = i;
        while j < funcs.len() && funcs[j].partition == *part && funcs[j].order == *order {
            j += 1;
        }
        let specs: Vec<WinSpec> = funcs[i..j]
            .iter()
            .map(|f| WinSpec {
                func: f.func,
                args: f.args.clone(),
                frame: f.frame,
                arg_types: f.args.iter().map(|a| cur_schema[*a].ty.clone()).collect(),
                ret: f.ret.clone(),
                range: f.range.clone(),
            })
            .collect();
        let mut groups: FxHashMap<Vec<u8>, Rows> = FxHashMap::default();
        let mut keys = Vec::new();
        for (r, w) in cur {
            let kv: Vec<Datum> = part.iter().map(|c| r[*c].clone()).collect();
            let k = key_bytes(&kv);
            if !groups.contains_key(&k) {
                keys.push(k.clone());
            }
            groups.entry(k).or_default().push((r, w));
        }
        let mut next = Vec::new();
        for k in keys {
            let mut g = groups.remove(&k).unwrap();
            sort_rows(&mut g, order);
            for r in compute_partition(&g, order, &specs)? {
                next.push((r, 1));
            }
        }
        cur_schema.extend(funcs[i..j].iter().map(|f| f.ret.clone()));
        cur = next;
        i = j;
    }
    Ok(cur)
}
