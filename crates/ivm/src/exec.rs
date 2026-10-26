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
use std::ops::Bound;

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
                if let Some(b) = base_access(rel)
                    && let Some(rows) = self.scan_base(&b)?
                {
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
                if let Some(b) = base_access(rel)
                    && let Some(rows) = self.scan_base(&b)?
                {
                    return Ok(rows);
                }
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

    /// The arrangement and key range of `node` to read for rows passing
    /// `preds`: equalities with constants on a key prefix, optionally
    /// followed by a range on the next key column. None when no predicate
    /// narrows any arrangement.
    fn index_range(&self, node: NodeId, preds: &[Expr]) -> Option<(usize, Vec<u8>, Option<Vec<u8>>)> {
        let mut conj: Vec<&Expr> = Vec::new();
        fn flatten<'e>(e: &'e Expr, out: &mut Vec<&'e Expr>) {
            match e {
                Expr::And(v) => v.iter().for_each(|x| flatten(x, out)),
                e => out.push(e),
            }
        }
        preds.iter().for_each(|p| flatten(p, &mut conj));
        // column <op> constant, with the column on the left
        let mut cmps: Vec<(usize, Cmp, &Datum)> = Vec::new();
        for p in conj {
            if let Expr::Call(c) = p
                && c.args.len() == 2
                && let Some(op) = Cmp::of(func_def(c.func).name)
            {
                match (&c.args[0], &c.args[1]) {
                    (Expr::Column(col), Expr::Literal(d, _)) if !d.is_null() => cmps.push((*col, op, d)),
                    (Expr::Literal(d, _), Expr::Column(col)) if !d.is_null() => cmps.push((*col, op.flip(), d)),
                    _ => {}
                }
            }
        }
        if cmps.is_empty() {
            return None;
        }
        let schema = &self.g.node(node).schema;
        // per arrangement: equality values on a key prefix, then bounds on the next key column
        let mut best: Option<IndexRange<'_>> = None;
        for &a in &self.g.node(node).arrangements {
            let arr = self.g.arr(a);
            // (an index built after the read's GSN lacks its history)
            if !arr.covers(self.at) {
                continue;
            }
            let usable = |c: usize, t: &SqlType, d: &Datum| schema[c].ty == *t && literal_matches(d, t);
            let mut vals = Vec::new();
            let mut lo: Option<&Datum> = None;
            let mut hi: Option<(&Datum, bool)> = None;
            for (i, (k, t)) in arr.layout.key.iter().zip(&arr.layout.key_types).enumerate() {
                let Expr::Column(c) = k else { break };
                if let Some((_, _, d)) = cmps.iter().find(|(col, op, d)| col == c && *op == Cmp::Eq && usable(*c, t, d)) {
                    vals.push((*d).clone());
                    continue;
                }
                // a range on this column (ascending keys only), then stop
                if !arr.layout.specs[i].desc {
                    for (_, op, d) in cmps.iter().filter(|(col, op, d)| col == c && *op != Cmp::Eq && usable(*c, t, d)) {
                        match op {
                            Cmp::Gt | Cmp::Ge => {
                                if lo.is_none_or(|l| d.sql_cmp(l) == Ordering::Greater) {
                                    lo = Some(d);
                                }
                            }
                            _ => {
                                if hi.is_none_or(|(h, _)| d.sql_cmp(h) == Ordering::Less) {
                                    hi = Some((d, *op == Cmp::Le));
                                }
                            }
                        }
                    }
                }
                break;
            }
            let score = |v: &Vec<Datum>, l: &Option<&Datum>, h: &Option<(&Datum, bool)>| (v.len(), l.is_some() as usize + h.is_some() as usize);
            if (!vals.is_empty() || lo.is_some() || hi.is_some())
                && best.as_ref().is_none_or(|(_, v, l, h)| score(&vals, &lo, &hi) > score(v, l, h))
            {
                best = Some((a, vals, lo, hi));
            }
        }
        let (arr, vals, lo, hi) = best?;
        let layout = &self.g.arr(arr).layout;
        let prefix = layout.encode_prefix(&vals);
        let bound_key = |d: &Datum| {
            let mut v = vals.clone();
            v.push(d.clone());
            layout.encode_prefix(&v)
        };
        // the range covers every match (predicates are checked on each row)
        let lo_key = match lo {
            Some(d) => bound_key(d),
            None => prefix.clone(),
        };
        let hi_key = match hi {
            Some((d, true)) => encoding::prefix_successor(&bound_key(d)),
            Some((d, false)) => Some(bound_key(d)),
            None => encoding::prefix_successor(&prefix),
        };
        Some((arr, lo_key, hi_key))
    }

    /// Read a base relation (a table or materialized view) through its
    /// arrangements: an index range when the predicates allow one, and only
    /// the columns the projection and the predicates need. None when the
    /// relation has no arrangement to read.
    fn scan_base(&mut self, b: &BaseAccess) -> Result<Option<Rows>> {
        let Some(node) = (self.objects)(b.id) else { return Ok(None) };
        let (arr, lo_key, hi_key) = match self.index_range(node, &b.preds) {
            Some(r) => r,
            // (the general path reports reads before an arrangement's history)
            None => match self.g.arrangement_at(node, self.at) {
                Some(a) => (a, Vec::new(), None),
                None => return Ok(None),
            },
        };
        let a = self.g.arr(arr);
        let layout = &a.layout;
        let plan = b.cols.as_ref().and_then(|cols| {
            let needed: Vec<usize> = cols.iter().copied().chain(b.preds.iter().flat_map(|p| p.column_set())).collect();
            layout.decode_plan(&needed)
        });
        let hi = match &hi_key {
            Some(h) => Bound::Excluded(h.as_slice()),
            None => Bound::Unbounded,
        };
        let (at, preds, cols) = (self.at, &b.preds, &b.cols);
        let distinct = cols.as_ref().is_some_and(|c| c.iter().enumerate().all(|(i, x)| !c[..i].contains(x)));
        let mut out = Vec::new();
        self.env.for_each(a.tree, Bound::Included(&lo_key), hi, &mut |k, h| {
            let w = h.weight(at);
            if w <= 0 {
                return Ok(true);
            }
            let mut r = layout.decode_row_with(k, plan.as_ref())?;
            for p in preds {
                if !p.eval_pred(&r)? {
                    return Ok(true);
                }
            }
            out.push((
                match cols {
                    // distinct columns are moved out of the decoded row
                    Some(cols) if distinct => cols.iter().map(|c| std::mem::replace(&mut r[*c], Datum::Null)).collect(),
                    Some(cols) => cols.iter().map(|c| r[*c].clone()).collect(),
                    None => r,
                },
                w,
            ));
            Ok(true)
        })?;
        Ok(Some(out))
    }

    fn join(&mut self, left: &Rel, right: &Rel, kind: JoinKind, on: &[Expr]) -> Result<Rows> {
        let nl = left.arity();
        let nr = right.arity();
        let l = self.execute(left)?;
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
        // a few left rows probe an index of the right side instead of scanning it
        if matches!(kind, JoinKind::Inner | JoinKind::Left | JoinKind::Semi | JoinKind::Anti)
            && let Some(rows) = self.index_join(&l, left, right, kind, &equi, &residual)?
        {
            return Ok(rows);
        }
        let r = self.execute(right)?;
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

    /// Join `l` (the rows of `left`) with `right`, a base relation (possibly
    /// filtered), by looking up each left row's key in an arrangement of the
    /// right relation keyed by columns it is equated with. None when there is
    /// no such arrangement or the left side is not small compared to it.
    #[allow(clippy::too_many_arguments)]
    fn index_join(
        &mut self,
        l: &Rows,
        left: &Rel,
        right: &Rel,
        kind: JoinKind,
        equi: &[(Expr, Expr, bool)],
        residual: &[Expr],
    ) -> Result<Option<Rows>> {
        let Some(b) = base_access(right) else { return Ok(None) };
        if equi.is_empty() {
            return Ok(None);
        }
        let Some(node) = (self.objects)(b.id) else { return Ok(None) };
        let lschema = left.schema();
        let rschema = &self.g.node(node).schema;
        // the right side's equated expressions over the base relation's columns
        let base_col = |re: &Expr| -> Option<usize> {
            let c = re.as_column()?;
            Some(match &b.cols {
                Some(cols) => cols[c],
                None => c,
            })
        };
        // the arrangement keyed by the most equated columns (types must match
        // exactly so that key encodings compare like the values)
        let mut best: Option<(usize, Vec<usize>)> = None;
        for &a in &self.g.node(node).arrangements {
            let arr = self.g.arr(a);
            // (an index built after the read's GSN lacks its history)
            if !arr.covers(self.at) {
                continue;
            }
            let mut used = Vec::new();
            for (k, t) in arr.layout.key.iter().zip(&arr.layout.key_types) {
                let Expr::Column(c) = k else { break };
                match equi.iter().position(|(le, re, _)| base_col(re) == Some(*c) && rschema[*c].ty == *t && le.ty(&lschema) == *t) {
                    Some(i) if !used.contains(&i) => used.push(i),
                    _ => break,
                }
            }
            if !used.is_empty() && best.as_ref().is_none_or(|(_, u)| used.len() > u.len()) {
                best = Some((a, used));
            }
        }
        let Some((arr, used)) = best else { return Ok(None) };
        let a = self.g.arr(arr);
        if (l.len() as i64).saturating_mul(64) >= self.env.tree_count(a.tree) {
            return Ok(None);
        }
        let (layout, tree, at) = (a.layout.clone(), a.tree, self.at);
        let plan = b.cols.as_ref().and_then(|cols| {
            let needed: Vec<usize> = cols.iter().copied().chain(b.preds.iter().flat_map(|p| p.column_set())).collect();
            layout.decode_plan(&needed)
        });
        let nr = right.arity();
        let mut out = Vec::new();
        for (lrow, lw) in l {
            let mut kv = Vec::with_capacity(used.len());
            for &i in &used {
                let (le, _, ns) = &equi[i];
                let v = le.eval(lrow)?;
                if v.is_null() && !ns {
                    break;
                }
                kv.push(v);
            }
            let mut matched = false;
            if kv.len() == used.len() {
                let mut cands = Vec::new();
                self.env.for_each_prefix(tree, &layout.encode_prefix(&kv), &mut |k, h| {
                    let w = h.weight(at);
                    if w > 0 {
                        cands.push((layout.decode_row_with(k, plan.as_ref())?, w));
                    }
                    Ok(true)
                })?;
                'cands: for (base, rw) in cands {
                    // the right relation's own filter and projection
                    for p in &b.preds {
                        if !p.eval_pred(&base)? {
                            continue 'cands;
                        }
                    }
                    let rrow: Row = match &b.cols {
                        Some(cols) => cols.iter().map(|c| base[*c].clone()).collect(),
                        None => base,
                    };
                    // every equality holds as in the hash join (equal key
                    // encodings), then the rest of the condition
                    for (le, re, ns) in equi {
                        let (x, y) = (le.eval(lrow)?, re.eval(&rrow)?);
                        if (!ns && (x.is_null() || y.is_null())) || key_bytes(&[x]) != key_bytes(&[y]) {
                            continue 'cands;
                        }
                    }
                    let mut joined = lrow.clone();
                    joined.extend(rrow.iter().cloned());
                    for p in residual {
                        if !p.eval_pred(&joined)? {
                            continue 'cands;
                        }
                    }
                    matched = true;
                    match kind {
                        JoinKind::Semi | JoinKind::Anti => break,
                        _ => out.push((joined, lw * rw)),
                    }
                }
            }
            match kind {
                JoinKind::Semi if matched => out.push((lrow.clone(), *lw)),
                JoinKind::Anti if !matched => out.push((lrow.clone(), *lw)),
                JoinKind::Left if !matched => {
                    let mut joined = lrow.clone();
                    joined.extend(std::iter::repeat_n(Datum::Null, nr));
                    out.push((joined, *lw));
                }
                _ => {}
            }
        }
        Ok(Some(out))
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

/// A read of a base relation: the rows of object `id` passing `preds`
/// (over its columns), projected to `cols` (all columns for None).
struct BaseAccess {
    id: u64,
    cols: Option<Vec<usize>>,
    preds: Vec<Expr>,
}

/// The plan as a base relation read, if it is a table or view possibly
/// filtered and projected.
fn base_access(rel: &Rel) -> Option<BaseAccess> {
    match rel {
        Rel::Get { id, .. } => Some(BaseAccess { id: *id, cols: None, preds: vec![] }),
        Rel::Project { input, cols } => {
            let mut b = base_access(input)?;
            b.cols = Some(match &b.cols {
                None => cols.clone(),
                Some(inner) => cols.iter().map(|c| inner[*c]).collect(),
            });
            Some(b)
        }
        Rel::Filter { input, preds } => {
            let mut b = base_access(input)?;
            for p in preds {
                b.preds.push(match &b.cols {
                    None => p.clone(),
                    Some(cols) => p.remapped(&|c| cols[c]),
                });
            }
            Some(b)
        }
        _ => None,
    }
}

/// An arrangement with the values of the equalities on a prefix of its key,
/// and the lower and upper bound (inclusive?) on the next key column.
type IndexRange<'a> = (usize, Vec<Datum>, Option<&'a Datum>, Option<(&'a Datum, bool)>);

/// A comparison of a column with a constant.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Cmp {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
}

impl Cmp {
    fn of(op: &str) -> Option<Cmp> {
        Some(match op {
            "=" => Cmp::Eq,
            "<" => Cmp::Lt,
            "<=" => Cmp::Le,
            ">" => Cmp::Gt,
            ">=" => Cmp::Ge,
            _ => return None,
        })
    }

    /// The operator with its operands swapped.
    fn flip(self) -> Cmp {
        match self {
            Cmp::Eq => Cmp::Eq,
            Cmp::Lt => Cmp::Gt,
            Cmp::Le => Cmp::Ge,
            Cmp::Gt => Cmp::Lt,
            Cmp::Ge => Cmp::Le,
        }
    }
}

/// Is a literal exactly of a key column's type (so that its key encoding
/// orders and compares like the column's values)?
fn literal_matches(d: &Datum, t: &SqlType) -> bool {
    matches!(
        (d, t),
        (Datum::Int2(_), SqlType::Int2)
            | (Datum::Int4(_), SqlType::Int4)
            | (Datum::Int8(_), SqlType::Int8)
            | (Datum::Text(_), SqlType::Text | SqlType::Varchar | SqlType::Bpchar | SqlType::Name)
            | (Datum::Numeric(_), SqlType::Numeric)
            | (Datum::Date(_), SqlType::Date)
            | (Datum::Timestamp(_), SqlType::Timestamp)
            | (Datum::TimestampTz(_), SqlType::TimestampTz)
            | (Datum::Uuid(_), SqlType::Uuid)
            | (Datum::Bool(_), SqlType::Bool)
    )
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
    // the group key of each row is encoded into one reused buffer; only a
    // new group allocates
    let mut k: Vec<u8> = Vec::new();
    for (r, w) in rows {
        if w == 0 {
            continue;
        }
        k.clear();
        for c in group {
            encoding::encode_key(&r[*c], SortSpec::ASC, &mut k);
        }
        let g = match groups.get_mut(k.as_slice()) {
            Some(g) => g,
            None => {
                order_keys.push(k.clone());
                let key_row = group.iter().map(|c| r[*c].clone()).collect();
                groups.entry(k.clone()).or_insert(G { key_row, summary: Summary::empty(&spec_kinds), rows: vec![] })
            }
        };
        g.summary.count += w;
        for i in 0..specs.len() {
            let w_eff = if aggs[i].distinct {
                // the FILTER applies before duplicates are removed
                if let Some(f) = aggs[i].filter
                    && !matches!(r[f], Datum::Bool(true))
                {
                    continue;
                }
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
                match e {
                    Expr::Column(c) => kind.add(&mut g.summary.aggs[slot], &r[*c], w_eff)?,
                    e => kind.add(&mut g.summary.aggs[slot], &e.eval(&r)?, w_eff)?,
                }
            }
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
            let v = if s.func.is_ordered() && aggs[i].distinct {
                // the FILTER applies before duplicates are removed, and the
                // group's rows stay intact for the other aggregates
                let a = &aggs[i].args;
                let mut seen = FxHashMap::default();
                let mut rs: Rows = g
                    .rows
                    .iter()
                    .filter(|(r, _)| aggs[i].filter.is_none_or(|f| matches!(r[f], Datum::Bool(true))))
                    .filter(|(r, _)| seen.insert(key_bytes(&a.iter().map(|c| r[*c].clone()).collect::<Vec<_>>()), ()).is_none())
                    .map(|(r, _)| (r.clone(), 1))
                    .collect();
                sort_rows(&mut rs, &aggs[i].order);
                s.finalize_ordered(&mut rs.iter().map(|(r, w)| (r, *w)))?
            } else if s.func.is_ordered() {
                let mut rs = std::mem::take(&mut g.rows);
                sort_rows(&mut rs, &aggs[i].order);
                let v = s.finalize_ordered(&mut rs.iter().map(|(r, w)| (r, *w)))?;
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
            next.extend(compute_partition(&g, order, &specs)?);
        }
        cur_schema.extend(funcs[i..j].iter().map(|f| f.ret.clone()));
        cur = next;
        i = j;
    }
    Ok(cur)
}
