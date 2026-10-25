# Dataflow: Z-sets, operators and arrangements

Maintained views run as a dataflow graph of incremental operators. This
chapter describes the data model, the graph, how a GSN is processed, every
operator, and hydration. Code: `crates/ivm/src/dataflow/` (`mod.rs`,
`ops.rs`, `join.rs`, `reduce.rs`, `agg.rs`, `window.rs`, `temporal.rs`,
`tablefunc.rs`, `mfp.rs`, `layout.rs`, `lower.rs`).

## Collections and deltas

Every collection — a base table, an intermediate result, a view — is a
**Z-set**: a multiset of rows in which each row has an integer weight
(`i64`). A table row present once has weight 1; a multiset table can have
weight 3 for three identical rows; intermediate collections may contain
negative weights transiently (an anti-join is `input − semijoin`).

Operators exchange **deltas**: `Vec<(Row, i64)>`, the change of a collection
at one GSN. An update is a retraction (`-1` of the old row) plus an insertion
(`+1` of the new one).

Every operator **consolidates** its output delta before handing it on: rows
are sorted by *exact identity* (`exact_row_cmp`, which distinguishes
representations such as `1.0` and `1.00`, `-0` and `0`, `'1 mon'` and
`'30 days'`), equal rows are merged by adding their weights, and zero weights
are dropped. Consequently no consumer — operator, subscriber or sink — ever
sees a change that cancels within a GSN, and a GSN's changes appear
atomically.

Alongside the data, every operator outputs **error records**
(`ErrDelta = Vec<((sqlstate, message), i64)>`): expression errors with the
weight of the row that caused them. They are consolidated per (SQLSTATE,
message) and flow to the consumers (see [Errors](#errors)).

## The graph

`dataflow::Graph` holds:

- **nodes** (`Node`): an operator (`NodeKind`), its input node ids, its
  output schema, a structural fingerprint, a reference count, the ids of its
  arrangements, and a `hydrated` flag;
- **arrangements** (`Arrangement`): persistent indexes of a node's output —
  a storage tree plus a `Layout` (key expressions, sort order, per-node
  summaries) — with a fingerprint, a reference count and a `hydrated` flag;
- the logical clock `now`, per-node statistics (for `EXPLAIN MAINTENANCE`),
  and, during recovery, the map of recovered trees by fingerprint.

Node ids are assigned in creation order and a node is always created after
its inputs, so **id order is a topological order**.

### Hash-consing

Nodes and arrangements are identified by **structural fingerprints**, not by
their ids:

- a node's fingerprint is a 128-bit FNV-1a hash of its operator name, the
  fingerprints of its inputs, a `0xFF` separator, and the operator's
  parameters (expressions, column lists, the fingerprints of arrangements it
  reads, ...);
- an arrangement's fingerprint hashes the owning node's fingerprint, the key
  expressions with their sort direction and null placement, a `0xFE`
  separator, and the summary aggregates.

`Graph::add_node` and `Graph::add_arrangement` return the existing node or
arrangement when one with the same fingerprint exists. Two views with a
common sub-plan therefore share its operators and indexes, a join lookup on
`t(c)` shares the index created by `CREATE INDEX ... ON t (c)`, and — because
fingerprints do not depend on creation order — a restarted engine finds the
trees of its checkpointed arrangements again (see
[Recovery](recovery.md#matching-checkpointed-indexes)). The exact hash input
is specified in [On-disk format](../on-disk-format.md#structural-fingerprints).

### Reference counting

A new node starts with zero references. Creating a node adds a reference to
each input and to each arrangement its operator reads. External *pins* add
references too: every table pins its node (and its primary index through
it), every materialized view pins its root node and its read arrangement,
every index pins its arrangement, every `SUBSCRIBE (query)` pins its root.
`release_node` and `release_arr` decrement, and at zero free the node (and,
recursively, what it referenced) or drop the arrangement's storage tree.
After planning, `gc_since` frees nodes and arrangements that the planner
created but that nothing references.

`SHOW ARRANGEMENTS` shows every arrangement with its node, operator, key,
size and reference count; `EXPLAIN DATAFLOW` marks nodes with more than one
reference as `shared`.

## Processing a GSN

`Graph::step(env, gsn, changes)` runs for every `Commit` record (with the
changes grouped by table node) and every `Tick` record (with no changes):

1. Visit nodes in topological order, skipping nodes that are not hydrated.
2. A `Table` node runs if the commit changes its table; it turns the logged
   changes into a delta (below).
3. Any other node runs if one of its inputs produced output at this GSN — or,
   for a temporal filter, if the clock moved since the previous step.
4. The node's output delta is applied to **all of its arrangements at
   `gsn`** before any consumer runs.
5. Consumers read arrangements through their (GSN, weight) histories either
   *before* the current GSN (`ReadAt::Before(gsn)`) or *after* it
   (`ReadAt::Latest`), so an operator can compute "old" and "new" results of
   the same key and emit the difference.

The outputs of all nodes are returned to the engine, which merges the error
records of each view's root into the view's error counts and forwards the
data deltas of subscribed nodes to subscriptions and sinks.

## Arrangements and layouts

An arrangement stores a node's collection in a storage tree
([Storage](storage.md)). A `Layout` maps rows to tree entries:

```
entry = key encoding of the key values      (order-preserving, canonical)
      ++ fidelity suffix of the key values  (exact representation)
      ++ row codec of the payload columns   (columns that are not plain key columns)
```

The key part orders and groups entries — a key prefix is a contiguous range,
so lookups are prefix scans — while the payload carries the remaining
columns exactly. Each entry's value in the tree is its history of
(GSN, weight) changes. A layout may also declare per-node *summaries*
(aggregates over expressions of the row) that the tree maintains for every
subtree. Byte formats: [On-disk format](../on-disk-format.md#data-encodings).

Kinds of arrangements:

| Arrangement | Key | Summaries | Created by |
|---|---|---|---|
| primary index of a table | primary-key columns (all columns for a multiset table) | — | the table |
| join lookup index | the join key expressions of that input | — | delta joins |
| presence index | the semijoin key columns | — (count) | semijoins |
| aggregation input | group columns (+ `ORDER BY` columns of an ordered aggregate) | the aggregates' needs | `Reduce` |
| aggregation output | group columns | — | `Reduce` |
| full index | all columns | — | `Distinct`, `SetOp`, `Recursive`, a view's read index |
| top-k / window input | group/partition columns, then sort columns | — | `TopK`, `Window` |
| temporal events | (time, kind) | — | `TemporalFilter` (private state) |
| user index | `CREATE INDEX` columns | — | `CREATE INDEX` |

A materialized view is read through its *read arrangement*: the first
arrangement of its root node if there is one (for example a `Reduce`'s
output index), otherwise a full index added for the view.

## Operators

| `NodeKind` | Shown as | Inputs | State |
|---|---|---|---|
| `Table` | `Table` | — | primary index |
| `Constant` | `Constant` | — | — |
| `Mfp` | `Mfp` | 1 | — |
| `Union` | `Union` | n | — |
| `Negate` | `Negate` | 1 | — |
| `FlatMap` | `FlatMap` | 1 | — |
| `Join` | `DeltaJoin` | n | lookup/presence indexes of inputs |
| `Reduce` | `Reduce` | 1 | input index with summaries, output index |
| `Distinct` | `Distinct` | 1 | full index of the input |
| `SetOp` | `SetOp` | 2 | full indexes of both inputs |
| `TopK` | `TopK` | 1 | sorted input index |
| `Window` | `Window` | 1 | sorted input index |
| `Recursive` | `Recursive` | the source tables/views | index of its own output |
| `Temporal` | `TemporalFilter` | 1 | event index |

### Table

`ops::table_step` turns a commit's `TableChange`s for one table into a delta
against the stored rows. For a table with a primary key, every touched key
gets its stored row (a prefix lookup in the primary index, which does not
yet contain this GSN) and its final state after all changes of the commit:

- `Upsert(row, unchanged)` sets the key's row; columns listed in `unchanged`
  (unchanged TOASTed values) are copied from the previous row;
- `Delete(key)` removes it; `Insert`/`Remove` behave like upsert/delete;
- `Truncate` removes every stored row and everything touched before it in
  the commit.

The output is `−stored, +final` for each touched key, consolidated, so an
upsert that does not change the row produces nothing. Because before-images
come from the stored row, sources never need `REPLICA IDENTITY FULL` for
tables with a key. For a multiset table (no key), `Insert`/`Upsert` add one
instance, `Remove`/`Delete` remove one, and `Truncate` retracts the current
contents plus everything added in the commit.

### Mfp (map, filter, project)

A sequence of `Map(expr)` (append a computed column) and `Filter(pred)`
steps followed by a projection. Steps run in SQL order, so a `WHERE`
condition guards the select list against errors. During lowering, filters,
maps and projections are kept as a *pending* MFP and fused into the
consuming operator (a join input, an arrangement key, ...); an `Mfp` node is
only created where one is needed. An expression error on a row yields an
error record with the row's weight instead of an output row.

### Union, Negate, Constant, FlatMap

Linear operators: `Union` concatenates its inputs' deltas (some negated —
used for `input − semijoin`), `Negate` flips weights, `Constant` emits fixed
rows (only at hydration), and `FlatMap` evaluates set-returning functions
per row and appends their output columns (several functions zipped, with
`WITH ORDINALITY` numbering). Error records pass through `Union` unchanged —
they are never negated.

### DeltaJoin

An n-way inner join that keeps **no intermediate state**. Each input has an
MFP (applied to its rows) and a position in the joined row; a *presence*
input (used for semijoins) has rows that are just the distinct key values of
an underlying collection, read through count summaries.

For every input *i* the plan has a **delta path**: filters that can be
checked on *i*'s row alone, then a sequence of lookup steps, each binding one
more input *j* through an arrangement of *j* keyed by the join keys between
*j* and the inputs bound so far, followed by the residual conditions that
became checkable.

At a GSN, for every input *i* whose node changed, *i*'s delta (after its MFP,
consolidated — changes to columns the join does not use cancel here) is
extended along *i*'s path. Input *j* is read *after* the GSN if `j < i` and
*before* it if `j > i`. This is the standard delta-query decomposition of
`d(R0 ⋈ … ⋈ Rn)`: every combination of changes is counted exactly once.

- A lookup with a NULL key on a non-null-safe column matches nothing; keys
  compared with `IS NOT DISTINCT FROM` match NULLs.
- A presence input contributes its key if the key's count (a range summary
  over the key prefix) is positive. Its count *before* the GSN is the current
  count minus the presence delta of this GSN. When a presence input is the
  delta source, a key contributes `+1` or `−1` only when its presence flips.
- When all inputs are bound, the joined row goes to the output.
- For hydration, the join drives the path of its smallest regular input
  (by index size) over that input's snapshot.

Lookup order is chosen per path by the planner ([Planner](planner.md#delta-join-planning)):
the next input is the one with the most equality conditions to the bound
inputs, preferring inputs whose primary key is covered (at most one match),
then smaller ones.

Joins that the lowering produces from other constructs:

- **Semijoin** (`EXISTS`, `IN`): `l` delta-joined with a presence input over
  the distinct key values of `r`. If the condition is not a conjunction of
  equalities between the two sides, the matching left rows are computed by
  a full inner join, projected to the left columns, and made distinct.
- **Antijoin** (`NOT EXISTS`, `NOT IN`): `Union(l, −semijoin(l, r))`.
- **Outer joins**: `Union(inner join, antijoin padded with NULLs)` (one or
  two antijoins for `FULL`).

### Reduce

Grouped aggregation as range queries over an augmented index (`reduce.rs`,
`agg.rs`). The input is arranged by the group key (then by the `ORDER BY`
of an ordered aggregate), and the arrangement's layout declares the
**summaries** each aggregate needs; the storage tree maintains them for
every subtree:

| Aggregate | Summaries |
|---|---|
| `count(*)` | the entry count (with `FILTER`: count of true filter values) |
| `count(x)` | non-null count |
| `sum(x)`, `avg(x)` | sum (integer, numeric or float), non-null count |
| `min(x)`, `max(x)`, `any_value(x)` | min / max |
| `bool_and(x)` | count of false, non-null count |
| `bool_or(x)` | count of true, non-null count |
| `stddev*`, `var*` | sum, sum of squares, non-null count |
| `percentile_cont/disc` | non-null count (values located by rank) |
| `string_agg`, `array_agg`, `json(b)_agg`, `json(b)_object_agg`, `mode` | none (the group is scanned) |

`FILTER (WHERE f)` is folded into the argument (`CASE WHEN f THEN x END`).
The output is arranged by the group key; it supplies the previous output row
of a group.

At a GSN, the changed rows are grouped by group key. For each affected
group: the old output row comes from the output index (or the group cache);
the new one from one **range summary** over the group's key range in the
input index — O(log n) whatever the group size — with the group's key
values taken from its first live entry (so the representation is that of the
first entry in index order). Ordered aggregates additionally scan the group.
If old and new rows differ (by exact comparison), `−old, +new` is emitted.

- **Group cache**: the summaries of up to 2^18 recently touched groups per
  `Reduce` are kept in memory. A small change to a large group is folded into
  the cached summary instead of summarising the range again. When folding is
  not exact — a `min`/`max` loses its extreme, or the group key has types with
  several representations and a row is retracted or arrives in another
  representation — the group is summarised from the index. Disabled for
  ordered aggregates and with `IVM_NO_GROUP_CACHE`.
- **Errors**: if finalising a group fails (for example `sum(float8)`
  overflows), the group's row is retracted and an error record emitted; the
  group's error is remembered (in memory) so that it can be retracted when
  the group computes again.
- **Exact arithmetic**: integer sums accumulate in `i128`; `numeric` sums in
  a structure that counts NaN/±Infinity and display scales separately, so
  that removing a value restores the exact previous result; float sums as a
  big integer multiple of 2^-1074 (the smallest subnormal), so addition is
  exact and associative and the result is rounded once. Variance and
  standard deviation are computed from exact sums of squares. See
  [Storage](storage.md#summaries).

### Distinct

The input is arranged by all columns. For each changed value (canonical key
prefix) the operator finds the first live entry before and after the GSN and
the total weight; it emits `−representative` if the value was present before
and `+representative` if it is present after. Equal values in different
representations are one group; the representative is the first in index
order.

### SetOp

`UNION`, `INTERSECT` and `EXCEPT` (with or without `ALL`) arrange both
inputs by all columns. For each changed value, the weights on both sides
before and after the GSN give the output multiplicities (for example
`EXCEPT ALL`: `max(a − max(b, 0), 0)`), and the difference is emitted with a
representative row. (`UNION` without `ALL` is lowered as `Distinct(Union)`.)

### TopK

`ORDER BY ... LIMIT/OFFSET`, `FETCH ... WITH TIES` and `DISTINCT ON` (limit
1 per group). The input is arranged by (group columns, sort columns). For
each changed group the operator reads the group's first `offset + limit`
rows before and after the GSN (walking entries and multiplicities) and emits
the difference. `WITH TIES` keeps rows whose canonical sort key equals the
last kept row's.

### Window

The input is arranged by (partition columns, `ORDER BY` columns); window
functions with the same partitioning and order share one operator. For each
changed partition, all window values are computed for the partition before
and after the GSN (`compute_partition`: peer groups, frame bounds per row,
running aggregates for frames starting at `UNBOUNDED PRECEDING`), and the old
rows are retracted and the new ones inserted. Cost is proportional to the
partition size — intrinsic for functions like `row_number` where one insert
shifts every later row. An error in a partition (for example `ntile(0)`)
becomes an error record.

### Recursive

`WITH RECURSIVE` is evaluated by the batch executor's semi-naive iteration
(`exec::eval_recursive`) over the current state of its source relations. At
every GSN at which an input changes, the operator recomputes the fixpoint and
emits the difference to its previous output (which it keeps in an index of
its own output). Correct, but the cost of a change is the cost of the whole
recursive query; nested (iteration) time is not implemented.

### TemporalFilter

Rows pass while temporal predicates over the clock hold. For each row the
operator bisects its validity interval `[lo, hi)`, emits it if valid now,
and stores future `(time, kind, row)` events in a private arrangement keyed
by (time `bigint`, kind `smallint`) — kind 0 "becomes valid", kind 1
"expires". At each step it releases the events due at the current clock.
See [Temporal filters](../user-guide/temporal-filters-and-errors.md#how-temporal-filters-are-maintained).

## Hydration

`Graph::hydrate(env, gsn)` brings every node and arrangement that is not yet
hydrated up to date as of `gsn`, computing their contents **from existing
state, never from the sources**:

1. Visit nodes in topological order. For a node that is not hydrated,
   operators with private state initialise it (the temporal filter computes
   its pending events from its input's snapshot).
2. If the node has arrangements that are not hydrated, compute the node's
   *snapshot* — its full contents — and write it into each of them at `gsn`
   (in batches of 100,000 rows). A snapshot (`ops::snapshot_node`) is read
   from any hydrated arrangement of the node if there is one; otherwise it is
   computed from the inputs' snapshots: a table from its primary index, an
   `Mfp` by evaluating its input's snapshot, a join by driving one path, a
   `Reduce` group by group from its input index, and so on.
3. Error records found in snapshots are returned to the caller (the new
   view's initial error counts).
4. Snapshots are cached only while a node still to be hydrated will read
   them, so large intermediate results are freed early.

Hydration is the common path of `CREATE MATERIALIZED VIEW`, `CREATE INDEX`,
`SUBSCRIBE (query)`, `CREATE OR REPLACE MATERIALIZED VIEW`, upstream schema
changes, and recovery (for arrangements whose checkpointed tree could not be
matched). After hydration the new nodes are stepped like all others from the
next GSN on.

## Errors

Error records are produced by `Mfp` and `FlatMap` (per row), `Reduce` (per
group), `Window` (per partition), `TemporalFilter` (per row) and `DeltaJoin`
(errors in the MFP of the delta's own input; errors in lookups and join
conditions are dropped). They pass through every other operator. The engine
keeps, for each materialized view, a map from (SQLSTATE, message) to count,
updated from the root node's output at every step and saved in checkpoints;
reads fail while any count is positive
([Errors as data](../user-guide/temporal-filters-and-errors.md#errors-as-data)).
