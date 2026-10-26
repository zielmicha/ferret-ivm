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

Every operator's output delta is **consolidated** before it is handed on:
rows are sorted by *exact identity* (`exact_row_cmp`, which distinguishes
representations such as `1.0` and `1.00`, `-0` and `0`, `'1 mon'` and
`'30 days'`), equal rows are merged by adding their weights, and zero weights
are dropped (a keyed table's delta is consolidated by construction, see
[Table](#table)). Consequently no consumer — operator, subscriber or sink —
ever sees a change that cancels within a GSN, and a GSN's changes appear
atomically.

Alongside the data, every operator outputs **error records**
(`ErrDelta = Vec<((sqlstate, message), i64)>`): expression errors with the
weight of the row that caused them. They are consolidated per (SQLSTATE,
message) and flow to the consumers (see [Errors](#errors)).

## The graph

`dataflow::Graph` holds:

- **nodes** (`Node`): an operator (`NodeKind`), its input node ids and the
  ids of the nodes that read it (its consumers), its output schema, a
  structural fingerprint, a reference count, the ids of its arrangements, a
  creation sequence number, and a `hydrated` flag;
- **arrangements** (`Arrangement`): persistent indexes of a node's output —
  a storage tree plus a `Layout` (key expressions, sort order, per-node
  summaries) — with a fingerprint, a reference count, a `hydrated` flag and
  `since`, the GSN from which its history is complete;
- the **error records** in each node's contents (`node_errors`, read with
  `Graph::errors_of`; see [Errors](#errors));
- the logical clock `now`, per-node statistics (for `EXPLAIN MAINTENANCE`),
  per-node activity counters (for the web UI), and, during recovery, the
  maps of recovered trees and their `since` by fingerprint.

Nodes and arrangements live in slots indexed by their ids; the slot of one
that is freed is reused (a `SUBSCRIBE (query)` would otherwise leave its
slots, and the statistics of its nodes, behind for good). A node is always
created after its inputs, so **creation order (`Node::seq`) is a
topological order**.

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
changes grouped by table node) and every `Tick` record (with no changes).
Only the nodes that can produce something are considered, so the cost of a
step does not depend on the size of the graph:

1. Nodes run in topological (creation) order, taken from a queue that
   starts with the changed tables' nodes and, if the clock moved since the
   previous step, every temporal filter; a node that produces output queues
   its consumers. Nodes that are not hydrated are skipped.
2. A `Table` node turns the commit's changes to its table into a delta
   (below).
3. Any other node runs because one of its inputs produced output at this
   GSN — or, for a temporal filter, because the clock moved.
4. The node's output delta is applied to **all of its arrangements at
   `gsn`** before any consumer runs, and its error records are added to the
   node's accumulated ones.
5. Consumers read arrangements through their (GSN, weight) histories either
   *before* the current GSN (`ReadAt::Before(gsn)`) or *after* it
   (`ReadAt::Latest`), so an operator can compute "old" and "new" results of
   the same key and emit the difference.

The outputs of the nodes that produced something are returned to the
engine, which merges the error records of each view's root into the view's
error counts and forwards the data deltas of subscribed nodes to
subscriptions and sinks ([Architecture](architecture.md#the-engine-loop)).

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
| join lookup index | the input's columns equated with the other inputs (computed join keys are appended to the input as columns first, see [DeltaJoin](#deltajoin)) | — | delta joins |
| presence index | all columns of the presence input (the semijoin key values) | — (count) | semijoins |
| aggregation input | group columns (+ `ORDER BY` columns of an ordered aggregate) | the aggregates' needs | `Reduce` |
| aggregation output | group columns | — | `Reduce` |
| full index | all columns | — | `Distinct`, `SetOp`, `Recursive` (its output, and source relations that have no arrangement), a view's read index |
| top-k / window input | group/partition columns, then sort columns | — | `TopK`, `Window` |
| temporal events | (time, kind) | — | `TemporalFilter` (private state) |
| user index | `CREATE INDEX` columns | — | `CREATE INDEX` |

A materialized view is read through its *read arrangement*: the first
arrangement of its root node if there is one (for example a `Reduce`'s
output index), otherwise a full index added for the view.

An arrangement's history is complete only from the GSN it was hydrated at
(`Arrangement::since`; for a new table, its creation): its contents as of
earlier GSNs were never stored. `Arrangement::covers(at)` tells whether a
read can use it — `since <= g` for `AS OF g` — and reads in the past only use
arrangements that cover them ([Planner](planner.md#batch-execution)). The
value is saved in checkpoints with the tree (`AppState::arr_since`), so a
recovered arrangement keeps its history; one rebuilt by hydration starts a
new one.

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
  (unchanged TOASTed values) are copied from the key's previous row (stored,
  or set earlier in the commit). A key without a row takes them from the row
  that the change just before deleted: a source sends a primary-key change
  as a delete of the old key directly followed by the upsert of the new row.
  Without either they become NULL (with a warning in the log);
- `Delete(key)` removes it; `Insert`/`Remove` behave like upsert/delete;
- `Truncate` removes every stored row and everything touched before it in
  the commit.

The output is `−stored, +final` for each touched key whose row changed (by
exact comparison), so an upsert that does not change the row produces
nothing; rows of different keys always differ, so the delta needs no further
consolidation. Because before-images come from the stored row, sources never
need `REPLICA IDENTITY FULL` for tables with a key. For a multiset table (no
key), `Insert`/`Upsert` add one instance, `Remove`/`Delete` remove one, and
`Truncate` retracts the stored contents and discards the changes before it
in the commit (changes after it are applied as usual).

### Mfp (map, filter, project)

A sequence of `Map(expr)` (append a computed column) and `Filter(pred)`
steps followed by a projection. Steps run in SQL order, so a `WHERE`
condition guards the select list against errors. During lowering, filters,
maps and projections are kept as a *pending* MFP: a join input applies it to
the rows it receives and looks up; every other consumer gets an `Mfp` node
(`materialize`). An expression error on a row yields an error record with
the row's weight instead of an output row.

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
became checkable. Arrangement keys are always plain columns of the input's
node: an input whose join key is computed (by its MFP, as in
`JOIN (SELECT id, 100 / x AS q FROM b WHERE x <> 0) b2 ON a.v = b2.q`, or by
the condition, as in `ON a.v = b.x + 1`) is first materialized as an `Mfp`
node that applies its MFP, filters first, and appends the computed keys as
columns. A key is therefore evaluated only on rows that pass the input's
filters, and one that fails becomes an error record of that node instead of
failing the step.

At a GSN, for every input *i* whose node changed, *i*'s delta (after its MFP,
consolidated — changes to columns the join does not use cancel here) is
extended along *i*'s path. Input *j* is read *after* the GSN if `j < i` and
*before* it if `j > i`. This is the standard delta-query decomposition of
`d(R0 ⋈ … ⋈ Rn)`: every combination of changes is counted exactly once.

- A lookup with a NULL key on a non-null-safe column matches nothing; keys
  compared with `IS NOT DISTINCT FROM` match NULLs.
- Rows of the frontier whose lookup keys encode equally (exact lookups:
  whose key values are identical) share one lookup. Looked-up entries are
  decoded only in the columns the input's MFP (and an exact match) reads.
- A presence input contributes its key if the key's count (a range summary
  over the key prefix) is positive. Its count *before* the GSN is the current
  count minus the presence delta of this GSN. When a presence input is the
  delta source, a key contributes `+1` or `−1` only when its presence flips;
  the rows such a path produces then get the presence input's columns
  recomputed with the key expressions the other paths look it up with, so
  that they carry the exact values (numeric scale, `-0`, ...) the other
  paths derive and retract each other's rows correctly.
- An *exact* presence input matches the whole entry encoding (values with
  their fidelity suffix) instead of the canonical key, and lookups keyed by
  its values keep only rows whose key values are identical (see *Semijoin*
  below).
- When all inputs are bound, the joined row goes to the output.
- For hydration, the join drives the path of its smallest regular input
  (by index size) over that input's snapshot; the other inputs are only
  looked up.
- Expression errors of an input's MFP count once per input row, by the
  input's **error tracker**: an input whose MFP may fail (every step but
  columns, constants, comparisons of two values of one scalar type, null
  tests, `AND`/`OR`/`NOT`, `CASE` and `COALESCE` over those) gets an `Mfp`
  node applying the MFP to the input's rows and keeping none, with an
  arrangement that stays empty. It is an input of the join, so its error
  records reach the join's output and contents like any input's; being
  arranged, its error records are authoritative and checkpointed: a join
  hydrated later over the same input reads them instead of evaluating the
  MFP over the input again (only the tracker's own hydration does, once).
  Joins whose input MFPs cannot fail have no trackers, and their
  hydration reads no input but the one it drives.

Lookup order is chosen per path by the planner ([Planner](planner.md#delta-join-planning)):
the next input is the one with the most equality conditions to the bound
inputs, preferring inputs whose primary key is covered (at most one match),
then smaller ones.

Joins that the lowering produces from other constructs:

- **Semijoin** (`EXISTS`, `IN`): `l` delta-joined with a presence input over
  the distinct key values of `r` (conditions on one side only filter that
  side). If the condition is not a conjunction of equalities between the two
  sides, the matching left rows are computed by an inner join of `l` and `r`
  with the whole condition and projected to the left columns; `l` is then
  joined with an *exact* presence input over them on all its columns
  (`IS NOT DISTINCT FROM`), which keeps `l`'s duplicate rows. Matching
  exactly keeps left rows apart that are equal but differ in representation
  (`1.0` and `1.00`), which the condition may tell apart; presence only
  needs a positive count, so the matched rows need no `Distinct`.
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
| `stddev*`, `var*` | sum, sum of squares, non-null count (for `bigint`, `numeric` sums of the argument cast to `numeric`: squares of `bigint` values would overflow `i128` sums) |
| `percentile_cont/disc` | non-null count (values read at their ranks with `Env::nth`); with `FILTER`, none (the group is scanned) |
| `string_agg`, `array_agg`, `json(b)_agg`, `json(b)_object_agg`, `mode` | none (the group is scanned) |

`FILTER (WHERE f)` is folded into the argument (`CASE WHEN f THEN x END`).
The output is arranged by the group key; it supplies the previous output row
of a group.

At a GSN, the changed rows are grouped by group key. For each affected
group: the old output row comes from the output index (or the group cache);
the new one from one **range summary** over the group's key range in the
input index — O(log n) whatever the group size — with the group's key
values taken from its first live entry (so the representation is that of the
first entry in index order). Percentiles (without `FILTER`) read the values
at the required ranks with `Env::nth` — O(log n) each, whatever the group
size; when the `ORDER BY` column sorts NULLs first (`DESC`, `NULLS FIRST`),
they skip the group's NULL entries first. The other ordered aggregates
(`string_agg`, `array_agg`, `json(b)_agg`, the object aggregates, `mode`,
and percentiles with `FILTER`) additionally scan the group. If old and new
rows differ (by exact comparison), `−old, +new` is emitted.

- **Group cache**: the summaries of up to 2^18 recently touched groups per
  `Reduce` are kept in memory. A small change to a large group is folded into
  the cached summary instead of summarising the range again. When folding is
  not exact — a `min`/`max` loses its extreme, or the group key has types with
  several representations and a row is retracted or arrives in another
  representation — the group is summarised from the index. A group with a
  percentile takes its previous output row from the output index (the
  values at the ranks before the step are no longer in the input index)
  and reads the new values at their ranks. Disabled for aggregates that
  scan the group and with `IVM_NO_GROUP_CACHE`.
- **Errors**: if finalising a group fails (for example `sum(float8)`
  overflows, or a `numeric` sum leaves numeric's range), the group's row is
  retracted and an error record emitted; the group's error is remembered in
  the operator's error state (`ReducePlan::errors`, by group key prefix) so
  that it is retracted when the group computes again. Checkpoints save that
  state with the graph's error state, and hydration (which recomputes every
  group, the global group of an aggregate without `GROUP BY` included)
  rebuilds it.
- **Exact arithmetic**: integer sums accumulate in `i128`; `numeric` sums in
  a structure that counts NaN/±Infinity and display scales separately and is
  not limited to numeric's range (only the value read from it is checked),
  so that removing a value restores the exact previous result; float sums as
  a big integer multiple of 2^-1074 (the smallest subnormal), so addition is
  exact and associative and the result is rounded once. Variance and
  standard deviation are computed from exact sums of squares. Maintaining a
  summary therefore never fails on data. See [Storage](storage.md#summaries).

### Distinct

The input is arranged by all columns. For each changed value (canonical key
prefix) the operator finds the first live entry before and after the GSN and
the total weight; it emits `−representative` if the value was present before
and `+representative` if it is present after. Equal values in different
representations are one group; the representative is the first in index
order.

### SetOp

`INTERSECT` and `EXCEPT` (with or without `ALL`) arrange both inputs by all
columns. For each changed value, the weights on both sides before and after
the GSN give the output multiplicities (for example `EXCEPT ALL`:
`max(a − max(b, 0), 0)`), and the difference is emitted with a
representative row. (`UNION ALL` is a plain `Union`, and `UNION` is lowered
as `Distinct(Union)`.)

### TopK

`ORDER BY ... LIMIT/OFFSET`, `FETCH ... WITH TIES` and `DISTINCT ON` (limit
1 per group). The input is arranged by (group columns, sort columns). A
group's output is its *window*: the rows (counted with their
multiplicities, in index order) at positions `offset` to
`offset + limit − 1`; `WITH TIES` extends it with the rest of the last row's
entry and the entries whose canonical sort key equals that entry's.
Positions are found with the count summaries (`Env::nth`, `range_summary`),
never by walking the rows before them.

For each changed group the operator compares the window before the GSN
with the window after it, without reading either in full:

- the positions before the GSN are those after it, shifted by the weight
  changes of the changed entries before them (each changed entry's weights
  before and after the GSN come from its history), so the window before
  the GSN is located from the current counts as well;
- if the first changed entry comes after the window (its position is past
  `offset + limit`, and with ties it is not tied with the last row), nothing
  changes;
- otherwise only the entries where the two windows can differ are visited:
  between the two windows' first rows, between their last rows (or through
  the tie groups), and the changed entries inside either window. Every
  other entry has all or none of its rows in both windows. Each visited
  entry contributes the difference of its rows in the two windows.

The work is proportional to the number of changes and to how far the
window moves, not to `offset` or `limit` (when the window moves past its
own length, the two windows are read).

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
recursive query; nested (iteration) time is not implemented. An expression
error during the evaluation (or reaching the iteration limit) means the
query has no valid result: its rows are retracted and an error record is
emitted, remembered in the operator's error state (`RecursivePlan::errors`,
checkpointed like a `Reduce`'s) and retracted once an evaluation succeeds
again.

### TemporalFilter

Rows pass while temporal predicates over the clock hold. For each row the
operator computes its validity interval `[lo, hi)` (below), emits it if
valid now, and stores future `(time, kind, row)` events in a private
arrangement keyed by (time `bigint`, kind `smallint`) — kind 0 "becomes
valid", kind 1 "expires". At each step it releases the events that became
due since the previous step: those after the previous step's clock
(`Graph::released_through`), up to the current one. Events are only ever
scheduled after the clock, so none at or before the previous step's clock
is still pending; released events stay in the arrangement as entries of
weight zero until compaction removes them, and are never visited again. A
graph rebuilt by recovery starts from the checkpoint's clock
(`Graph::restore_clock`), up to which the checkpointed filters released
everything.

Each predicate is `clock_side op row_side`; its value changes where
`clock_side(t)` reaches the row side's value `x`. The operator first tries
the time where a shift of the clock reaches `x` — `t0 + (y − clock_side(t0))`
at `t0 = y`, `y` being `x` or the next value of its type for a strict
condition — and takes it if the predicate changes exactly there (two
evaluations); only otherwise does it bisect the predicate over the searched
range. Shifts of the clock by constant intervals therefore take three
evaluations of the clock side per predicate instead of about 55, and the
result is exact whatever the arithmetic (months, time zones, `date_trunc`),
because it is a verified change of the predicate itself.
See [Temporal filters](../user-guide/temporal-filters-and-errors.md#how-temporal-filters-are-maintained).

## Hydration

`Graph::hydrate(env, gsn)` brings every node and arrangement that is not yet
hydrated up to date as of `gsn`, computing their contents **from existing
state, never from the sources**:

1. Visit nodes in topological order. For a node that is not hydrated,
   operators with private state initialise it (the temporal filter computes
   its pending events from its input's snapshot).
2. If the node has arrangements that are not hydrated, compute the node's
   *snapshot* — its full contents, rows and error records — and write its
   rows into each of them at `gsn`: they are encoded as the arrangement's
   entries, sorted by key and applied in batches of 100,000 that each cover
   one key range (so a batch rewrites only the leaves of its range). The
   arrangement's `since` becomes `gsn`. A snapshot (`ops::snapshot_node`)
   is read from any hydrated arrangement of the node if there is one, with
   the node's accumulated error records; otherwise it is computed from the
   inputs' snapshots: a table from its primary index, an `Mfp` by evaluating
   its input's snapshot, a join by driving one path, a `Reduce` group by
   group from its input index (seeking from one group's key prefix to the
   next, and rebuilding the operator's error state), and so on. Every
   operator's snapshot includes its inputs' error records, as its steps
   pass them on. Snapshots of row-wise operators (`Mfp`, `Union`, `Negate`,
   `FlatMap`, `DeltaJoin`, `TemporalFilter`) are passed on unconsolidated:
   arrangements consolidate entries as they apply them, and
   `Graph::snapshot` consolidates its result.
3. The error records of each hydrated node's snapshot become the node's
   error records (`Graph::errors_of`). A new or replaced materialized view
   takes its root's: when the root already existed (an identical view, or
   a view over another materialized view), those are the records the node
   has accumulated, so the view starts with the errors already present.
4. Snapshots are cached only while a node still to be hydrated will read
   them, so large intermediate results are freed early.

Hydration is the common path of `CREATE MATERIALIZED VIEW`, `CREATE INDEX`,
`SUBSCRIBE (query)`, `CREATE OR REPLACE MATERIALIZED VIEW` (and the
re-planning of the views reading a replaced view), upstream schema changes,
and recovery (for arrangements whose checkpointed tree could not be
matched). After hydration the new nodes are stepped like all others from the
next GSN on.

## Errors

Error records are produced by `Mfp` and `FlatMap` (per row), `Reduce` (per
group), `Window` (per partition), `TemporalFilter` (per row) and `Recursive`
(per query). Errors in the MFPs of a `DeltaJoin`'s inputs are counted by
the inputs' error trackers (`Mfp` nodes, see [DeltaJoin](#deltajoin)); a
looked-up row whose MFP fails is skipped. They pass through every other
operator. Errors in a join's
residual conditions (those that are not equalities between two inputs) are
not records: the condition counts as false, so the joined row is missing
where PostgreSQL would fail.

Three kinds of error state are kept:

- per node, the error records in its contents (`Graph::node_errors`): the
  sum of its output error deltas, set from its snapshot when it is
  hydrated. For arranged nodes this is authoritative — their snapshots read
  the arrangement, which holds rows only — and checkpoints save it by node
  fingerprint (`AppState::errors.nodes`), so that views created over such a
  node after a restart see its errors too;
- per operator, the groups currently in error of `Reduce` and `Recursive`
  (`AppState::errors.operators`, by node fingerprint), so that a group's
  error is retracted when it recovers, also after a restart;
- per materialized view, a map from (SQLSTATE, message) to count, taken
  from its root when the view is created and updated from the root's output
  at every step (`AppState::view_errors`). Reads fail while any count is
  positive
  ([Errors as data](../user-guide/temporal-filters-and-errors.md#errors-as-data)).

Recovery restores the per-node and per-operator state by fingerprint after
rebuilding the dataflow and before hydrating what it could not match.

Arrangement keys are plain columns (a computed join key is materialized as
a column of its input, see [DeltaJoin](#deltajoin)), and summaries cannot
fail: numeric sums are kept exactly beyond numeric's range (`NumSum`), and
only the aggregate value read from them is checked (an error record of its
group). In aggregates, temporal filters and recursive queries, errors that
are not caused by data — internal (`XX...`) and I/O (`58...`) errors — are
not turned into records but fail the step, as storage errors do; the engine
thread stops on such an error. Any other error that still escaped
`Graph::step` would be a bug: the engine logs it, does not apply the record
again (it may be half-applied), and fails the statement that wrote it.
