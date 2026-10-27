# Dataflow: Z-sets, operators and arrangements

Maintained views run as a dataflow graph of incremental operators. This
chapter describes the data model, the graph, how a GSN is processed, every
operator, and hydration. Code: `crates/ivm/src/dataflow/` (`mod.rs`,
`ops.rs`, `join.rs`, `reduce.rs`, `agg.rs`, `window.rs`, `temporal.rs`,
`tablefunc.rs`, `mfp.rs`, `layout.rs`, `lower.rs`, `delta.rs`).

## Collections and deltas

Every collection — a base table, an intermediate result, a view — is a
**Z-set**: a multiset of rows in which each row has an integer weight
(`i64`). A table row present once has weight 1; a multiset table can have
weight 3 for three identical rows; intermediate collections may contain
negative weights transiently (an anti-join is `input − semijoin`).

Operators exchange **deltas**, the change of a collection at one GSN: rows
with weights. An update is a retraction (`-1` of the old row) plus an
insertion (`+1` of the new one).

Every operator's output delta is **consolidated** before it is handed on:
rows are sorted by *exact identity* (`exact_row_cmp`, which distinguishes
representations such as `1.0` and `1.00`, `-0` and `0`, `'1 mon'` and
`'30 days'`), equal rows are merged by adding their weights, and zero weights
are dropped. Consequently no consumer — operator, subscriber or sink — ever
sees a change that cancels within a GSN, and a GSN's changes appear
atomically.

A node's delta at a GSN (`delta::NodeDelta`) is held in memory
(`delta::Rows::Mem`, a `Vec<(Row, i64)>`) while its rows' memory stays
within the *spill threshold*: a buffer of the transient working memory,
the memory budget's share that every in-memory delta, key set and sort
buffer charges until it is freed (`memory::Work`: half the share, within 1
and 256 MiB, and less while the buffers together exceed the share, see
[Memory](architecture.md#memory); `IVM_DELTA_SPILL_BYTES` overrides it,
and the tests run with 0, which spills every delta). A larger one is
**spilled**
(`delta::Rows::Tree`) into temporary storage trees, one per *target*: each
hydrated arrangement of the node, in the arrangement's encoding, or, for a
node without arrangements, an index by all columns (the encoding of a full
index). Equal rows — and only exactly equal ones — encode equally there and
become one entry whose weights add up, so each tree is the consolidated
delta, sorted by the arrangement's entry key; the trees are built through
`TreeBuilder`s ([Storage](storage.md#loading-unordered-entries)) and merged
into the arrangements in key order. Operators produce their output into a
`DeltaBuf`, which consolidates it in memory or spills it as it grows.
Consumers read a delta in batches of 1,024 rows (`for_each_batch`), or by
key range in the encoding of any of the node's arrangements
(`for_each_entry`: binary search in the in-memory rows sorted by that
encoding, or a range of the spilled tree), until they have all run.

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

A GSN is processed for every `Commit` record (with the changes grouped by
table node) and every `Tick` record (with no changes), in two phases:
`Graph::step_begin(gsn, observed)`, then `step_feed` with each table's
changes (a table's changes may come in several batches, in their order),
then `step_finish`; `Graph::step_observed(env, gsn, changes, observed)` does
all three. `observed` names the nodes whose rows the caller needs (the
engine passes the nodes of subscriptions and sinks, a set it keeps, shared,
until one of them is added, removed or moved). Only the nodes that
can produce something are considered, so the cost of a step does not
depend on the size of the graph:

1. **Feeding** a table's changes: they are applied in chunks of about the
   spill threshold (by `row_bytes`). `ops::table_step` turns a chunk into a
   delta against the stored rows (below), which is applied to the table's
   primary index at once, so that the next chunk reads the table as this
   one left it; the deltas of the chunks add up to the commit's, which is
   accumulated in a `DeltaBuf` (spilled in the encodings of the table's
   arrangements, the primary index included) and applied to the table's
   other arrangements when the table node is propagated. If nothing will
   read the table's delta (no hydrated consumer, not observed), a large
   one goes to those arrangements directly, through `TreeBuilder`s.
2. **Propagating**: nodes run in topological (creation) order, taken from
   a queue that starts with the fed tables' nodes and, if the clock moved
   since the previous step, every temporal filter; a node that produces
   output queues its hydrated consumers (every node knows its consumers).
   Nodes that are not hydrated are skipped. A `Table` node completes its
   arrangements and hands on its accumulated delta. Any other node runs
   because one of its inputs produced output at this GSN — or, for a
   temporal filter, because the clock moved — reading its inputs' deltas
   in batches.
3. The node's output delta is applied to **all of its arrangements at
   `gsn`** before any consumer runs (a spilled one by merging its trees),
   and its error records are added to the node's accumulated ones. The
   output of a node that nothing reads goes to its arrangements directly
   once it exceeds the spill threshold (through `TreeBuilder`s), rather
   than into temporary trees first.
4. Consumers read arrangements *after* the current GSN (`ReadAt::Latest`)
   or *before* it, so an operator can compute "old" and "new" results of
   the same key and emit the difference. Before the GSN is read through the
   entries' (GSN, weight) histories (`ReadAt::Before(gsn)`), or — TopK and
   Window — by overlaying the input's delta on the input's arrangement
   (`delta::Overlay`): the arrangement was changed at this GSN by exactly
   that delta, so an entry's weight before it is its current weight minus
   its change. The delta is read by key range in the arrangement's
   encoding, from memory or from its temporary tree alike: group by group
   (`Overlay::groups`, a group's changes collected in memory up to a
   limit), or merged with the arrangement's entries in chunks
   (`Overlay::for_each_versioned`, which also visits entries the step left
   at weight zero or removed).
5. A node's output is freed as soon as all its consumers have run: handed
   to the caller as `Output` (its rows only if the node is observed — in
   memory as they are, or still spilled, `Output::spilled`, for the caller
   to read in batches and release — its error records always) and its
   temporary trees dropped otherwise. So only the outputs of nodes with
   consumers still to run are alive at a time, and each is in memory only
   while small.

The engine merges the error records of each view's root into the view's
error counts and forwards the data deltas of subscribed nodes to
subscriptions and sinks, a spilled one a batch at a time
([Architecture](architecture.md#the-engine-loop)).

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
| top-k / window input | group/partition columns, then sort columns | — (window: the summaries of aggregates whose frames reach a partition's end) | `TopK`, `Window` |
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
value is saved with the tree in the metadata tree's arrangement records, so
a recovered arrangement keeps its history; one rebuilt by hydration starts a
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

`ops::table_step` turns a chunk of a commit's `TableChange`s for one table
into a delta against the stored rows. For a table with a primary key, every
touched key gets its stored row (a prefix lookup in the primary index, which
holds the commit's earlier chunks but nothing else of this GSN) and its
final state after all changes of the chunk:

- `Upsert(row, unchanged)` sets the key's row; columns listed in `unchanged`
  (unchanged TOASTed values) are copied from the key's previous row (stored,
  or set earlier in the commit). A key without a row takes them from the row
  that the change just before deleted: a source sends a primary-key change
  as a delete of the old key directly followed by the upsert of the new row.
  Without either they become NULL (with a warning in the log);
- `Delete(key)` removes it; `Insert`/`Remove` behave like upsert/delete;
- `Truncate` removes every stored row and everything touched before it in
  the commit: the delta of the changes before it is emitted (applied to the
  primary index), and then every row the index holds is retracted, a batch
  at a time.

The output is `−stored, +final` for each touched key whose row changed (by
exact comparison), so an upsert that does not change the row produces
nothing; rows of different keys always differ, so a chunk's delta needs no
further consolidation (the chunks' deltas are consolidated together, which
cancels a key's intermediate rows). The row the last change of a chunk
deleted carries over to the next chunk. Because before-images come from the
stored row, sources never need `REPLICA IDENTITY FULL` for tables with a
key. For a multiset table (no key), `Insert`/`Upsert` add one instance,
`Remove`/`Delete` remove one, and `Truncate` retracts the stored contents,
which include the changes before it in the commit (changes after it are
applied as usual).

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
  (by index size) over that input's snapshot, in chunks of about 8 MiB of
  rows (estimated by `row_bytes`); the other inputs are only looked up.
- A path is extended depth first in chunks: a lookup step takes at most
  8 MiB of its frontier at a time, hands its output on in chunks of 8 MiB,
  and reads a key's matches in batches of 1,024 rows (or 1 MiB of entries),
  so the rows in flight stay bounded whatever the fan-out. Within a chunk,
  rows are looked up in key order (rows with equal keys share a lookup).
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
input index — O(log n) whatever the group size (a `min`/`max` value wider
than node summaries hold takes a second descent to its entry, see
[Storage](storage.md#wide-min-and-max-values)) — with the group's key
values taken from its first live entry (so the representation is that of the
first entry in index order). Percentiles (without `FILTER`) read the values
at the required ranks with `Env::nth` — O(log n) each, whatever the group
size; when the `ORDER BY` column sorts NULLs first (`DESC`, `NULLS FIRST`),
they skip the group's NULL entries first. The other ordered aggregates
(`string_agg`, `array_agg`, `json(b)_agg`, the object aggregates, `mode`,
and percentiles with `FILTER`) additionally scan the group. If old and new
rows differ (by exact comparison), `−old, +new` is emitted. (A spilled
input delta's groups are collected as their key prefixes, through a
temporary tree if they exceed the spill threshold, and each group is
summarised from the index.)

- **Group cache**: the summaries (and key values) of recently touched groups
  are kept in memory. A small change to a large group is folded into the
  cached summary instead of summarising the range again (not from a spilled
  delta). When folding is
  not exact — a `min`/`max` loses its extreme, or the group key has types with
  several representations and a row is retracted or arrives in another
  representation — the group is summarised from the index. A group with a
  percentile takes its previous output row from the output index (the
  values at the ranks before the step are no longer in the input index)
  and reads the new values at their ranks. Disabled for aggregates that
  scan the group and with `IVM_NO_GROUP_CACHE`. The caches of all
  `Reduce`s share one byte budget, an eighth of the memory budget
  (`Graph::group_caches`, see [Memory](architecture.md#memory)); an entry is
  counted at its size in memory (the map's slot, the key, the summary's
  values and the key values). A cache that would exceed the budget does not
  cache the group and marks the budget squeezed; after the record the engine
  shrinks every cache (`reduce::trim_caches`): it evicts the groups not used
  since the previous shrink (entries remember the shrink epoch they were
  last cached in), and a cache still holding more than half of what it held
  loses every other remaining group. So the caches of aggregations that no
  longer change give way to those in use, and a cache in use keeps its
  recently used groups.
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
  exact dyadic rationals, so addition is exact and associative and the
  result is rounded once. Variance and standard deviation are computed from
  exact sums of squares. Maintaining a summary therefore never fails on
  data. See [Storage](storage.md#summaries).

Distinct, SetOp, TopK and Window (below), like a `Reduce` with a spilled
input, work on the changed values, groups or partitions as a sorted set of
key prefixes (`delta::KeySet`), kept in memory or, beyond the spill
threshold, in a temporary tree, and visited in key order.

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
  changes of the changed entries before them, so the window before the GSN
  is located from the current counts as well. An entry's weight before the
  GSN is its current weight minus the step's change to it (the operator's
  input delta overlaid on its input index, read group by group), not read
  from its history;
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
own length, the two windows are read). Windows of at most 32 rows
(`offset` included) are instead read from the start of their group, before
and after the GSN, which costs less than locating them; so are the windows
of a group with more than 4,096 changed entries, whose changes are then not
collected in memory but read along with the group's entries (in chunks), so
that a statement changing a large group at once stays in bounded memory.

### Window

The input is arranged by (partition columns, `ORDER BY` columns), with
summaries for the aggregates whose frames reach a partition's start or end
(`UNBOUNDED PRECEDING`, `UNBOUNDED FOLLOWING`); window functions with the
same partitioning and order share one operator.

Values are computed over a *run* of consecutive rows of a partition
(`compute`): frames slide over it — rows entering a frame are added to and
rows leaving it subtracted from the invertible states (counts, sums),
`min`/`max` are kept in monotonic deques (the leftmost of equal values
first, as summaries keep it), and ordered aggregates read the frame's rows
in place — so a run costs time proportional to its length, whatever the
frames. The parts of frames up to the partition's ends that lie outside the
run come from range summaries of the input index, and the rows at
positions outside it (for `lag`, `lead`, `first_value`, ...) from
`Env::nth`. A row's multiplicity is expanded into positions, but
consecutive equal output rows are emitted once, with their count.

For each changed partition, the operator determines from the functions and
their frames which rows' values the change can change: rows whose frame
holds a changed row (frames ending after it reach back from it, frames
starting before it forward from it), the rows after it for `row_number` and
`rank` when the partition's size changes (for `dense_rank` always; every row
for `percent_rank`, `cume_dist` and `ntile`), the next row for `lag` (any
row with an explicit offset), and so on. It reads the run those rows'
values depend on — their frames, except the parts that summaries or row
lookups provide; whole peer groups; from the partition's start for
`dense_rank` — computes their values before and after the GSN, and emits
the difference. Positions outside the changed rows are the same in both
versions, so both are computed from one read of the run (an entry's weight
before the GSN being its current weight minus the step's change to it, the
input delta overlaid on the input index). The work is proportional to the rows
that change and the rows their frames reach: appending to a partition with
a running sum reads a few rows, while a change that changes every row (an
insert at the start with `row_number`, any change with frames reaching the
partition's end) reads the partition once. Partitions of at most 32 rows
are recomputed whole: finding what to read would cost more. So are
partitions with more than 4,096 changed entries, read before the GSN with
the overlay of the delta read along, without collecting their changes.

An error in a partition (for example `ntile(0)`) becomes an error record,
and none of its rows is output. The partitions in error are remembered
(`WindowPlan::errors`): a change to a partition in error, or one that puts
it in error, recomputes the whole partition before and after the GSN. The
state is computed by the operator's snapshot or restored from a checkpoint
(with the record that it is known: [Errors](#errors)); an operator for which
neither happened (a new one whose snapshot was not taken, or one recovered
from a checkpoint of an older version) computes it at the end of hydration.
A hydration on another thread hands the state it computed over with the
operator (`Graph::adopt_hydrated`).

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
operator computes its validity intervals `[lo, hi)` (below), emits it if
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

Each predicate is `clock_side op row_side`, its clock side a chain of steps
applied to the clock reading `t`: casts, `date_trunc`, constants added or
subtracted. All of them are non-decreasing except adding an interval with
months to a date or timestamp, which clamps the day to the end of a shorter
target month: March 29th to 31st minus a month are all February 28th, at
their time of day, so the sum goes back by up to a day at the midnights
starting those days, and a predicate can hold, stop holding and hold again.
The interval's days and time are added after its months
(`timestamp_pl_interval`), so they are a non-decreasing step of their own.

Without months, the clock side is non-decreasing and the predicate changes
value once at most, where `clock_side(t)` reaches the row side's value `x`.
The operator first tries the time where a shift of the clock reaches `x` —
`t0 + (y − clock_side(t0))` at `t0 = y`, `y` being `x` or the next value of
its type for a strict condition — and takes it if the predicate changes
exactly there (two evaluations); only otherwise does it bisect the predicate
over the searched range. Shifts of the clock by constant intervals
therefore take three evaluations of the clock side per predicate instead of
about 55, and the result is exact whatever the arithmetic (time zones,
`date_trunc`), because it is a verified change of the predicate itself.

With months (`Clock`, `MonthSum`), the clock side is split at its outermost
addition of months into `outer(inner + n months)`, `outer` being
non-decreasing (it includes the interval's days and time):

1. A condition that holds until some time is solved as its negation (`<`
   as `>=`), which holds from some time on. It holds from some month sum `c`
   on, found like a change above, over sums instead of clock readings
   (guessed at `x` shifted back through `outer`, else bisected over the
   sums the clock reaches in the searched range, with a year's margin).
2. The values `u` of `inner` whose sum reaches `c` follow from the calendar
   (`month_sum_at_least`): `u + n months` lies in the month `n` months after
   `u`'s, at `u`'s day (clamped to that month's last day) and time of day.
   So the values of all months after the source month (`n` months before
   `c`'s) reach `c`, those of earlier months do not, and those of the source
   month do from `c`'s day and time on (none if the source month is shorter
   than `c`'s day); and if `c` is on the last day of a target month shorter
   than the source month, each later day of the source month, clamped to
   it, reaches `c` again from `c`'s time of day on. That is one interval of
   values, or one per such day: up to four.
3. When `inner` takes those values is found the same way, as the times from
   which `inner` is at least each bound — recursively if `inner` adds months
   itself.

A row's validity is the intersection of its predicates' sets, and each of
its intervals gets its pair of events. The result is exact: every part is
a change of a non-decreasing function, taken where verified or bisected,
or calendar arithmetic. Its cost does not depend on how far the changes
are: only the source month of `c` is looked at, whatever the months in
between. For `now() - interval '1 month'` and the like, `outer` and
`inner` are the identity, the guesses hold, and a predicate takes six
evaluations of trivial expressions (three for the sum, three for the
time); a row valid over several intervals takes three more per further
bound.
See [Temporal filters](../user-guide/temporal-filters-and-errors.md#how-temporal-filters-are-maintained).

## Hydration

`Graph::hydrate(env, gsn)` brings every node and arrangement that is not yet
hydrated up to date as of `gsn`, computing their contents **from existing
state, never from the sources**, in memory bounded by the buffer pool and
a constant:

1. Visit nodes in topological order. For a node that is not hydrated,
   operators with private state initialise it (the temporal filter computes
   its pending events from its input's snapshot, loading them into its
   event index through a `TreeBuilder`).
2. If the node has arrangements that are not hydrated, stream the node's
   *snapshot* — its full contents, rows and error records — into them at
   `gsn`: each batch of rows is encoded as each arrangement's entries and
   pushed to a `TreeBuilder` per arrangement, which loads entries arriving
   in any order through sorted runs ([Storage](storage.md#loading-unordered-entries)),
   so that each leaf is written once and at most a buffer of entries is in
   memory. The arrangement's `since` becomes `gsn`.
3. A snapshot (`ops::snapshot_stream`) is produced in batches of at most
   1,024 rows: read from any hydrated arrangement of the node if there is
   one (`Graph::scan_stream`, batches also bounded by 1 MiB of entries),
   with the node's accumulated error records; otherwise computed from the
   inputs' snapshots, a batch at a time: a table from its primary index, an
   `Mfp` by evaluating its input's batches, a join by driving one path (see
   [DeltaJoin](#deltajoin)), a `Reduce` group by group from its input index
   (seeking from one group's key prefix to the next, and rebuilding the
   operator's error state), a `TopK` likewise, a `SetOp` by merging both
   input indexes in key order, and so on. Every operator's snapshot
   includes its inputs' error records, as its steps pass them on. (A window
   partition and a recursive query's result are computed whole, in
   memory.) Snapshots are not consolidated: arrangements consolidate
   entries as they apply them, and `Graph::snapshot` consolidates its
   result.
4. The error records of each hydrated node's snapshot become the node's
   error records (`Graph::errors_of`). A new or replaced materialized view
   takes its root's: when the root already existed (an identical view, or
   a view over another materialized view), those are the records the node
   has accumulated, so the view starts with the errors already present.

Nothing is cached between readers: a node with an arrangement is read from
it (nodes are hydrated before their readers), and a node without one is
recomputed from its inputs for each reader, a batch at a time.

Hydration is the common path of `CREATE MATERIALIZED VIEW`, `CREATE INDEX`,
`SUBSCRIBE (query)`, `CREATE OR REPLACE MATERIALIZED VIEW` (and the
re-planning of the views reading a replaced view), upstream schema changes,
and recovery (for arrangements whose checkpointed tree could not be
matched). After hydration the new nodes are stepped like all others from the
next GSN on.

### Hydration off the engine thread

`CREATE MATERIALIZED VIEW`, `CREATE INDEX`, `CREATE OR REPLACE` (with the
re-planning of the views reading the replaced view), upstream schema changes
and `SUBSCRIBE (query)` hydrate their new nodes and arrangements without
stopping the engine (`coord/hydrate.rs`). A DDL or schema record collects
what has to follow the hydration of what it built (`hydrate::Then`) and
starts the hydration once it has been applied. A small one — reading no
more than the read budget of 50,000 entries, as estimated from the
arrangements the pending fragment reads (`Graph::pending_input_entries`,
which counts every input whole) and, for a replacement with consumers, the
old version — is hydrated inline, as replay does: forking the storage and
the graph would cost more than reading what it reads. (The graph keeps
its pending nodes and arrangements as they change, so finding and
hydrating them visits nothing else.) A table rebuilt after a schema change
always goes off the engine thread. At the GSN *g* the statement is
applied at (its DDL record, or the last applied record for `SUBSCRIBE`), the
engine takes a fork of the storage — every tree as of *g*
([Storage](storage.md#forks)) — and a copy of the part of the graph the new
fragment needs (`Graph::fork_of`: the pending nodes, the nodes owning the
pending arrangements, and everything they read, transitively, with all of
those nodes' arrangements; operator error state — groups in error, and
whether they are known — copied, not shared; node and arrangement ids and
sequence numbers kept, so that its steps run the nodes in the same order,
each node's consumers and the temporal filters those within the copy, and
the clock the engine's, its temporal filters having released their events
up to the same time; after hydrating, the thread drops the nodes the
fragment does not read and the arrangements no operator of it reads, so
that its steps maintain only those), and hands both
to a thread of its own (`ivm-hydrate`). That thread runs `Graph::hydrate` as
of *g* there — the same computation as inline — and then applies every
`Commit` and `Tick` record after *g*: the engine forwards each one's table
changes (of the fork's tables) as it feeds them to its own dataflow, a
batch at a time (`Feed` messages; a commit read from its parts takes one
per part), and then the clock (`Step`), and the thread feeds and steps its
copy of the graph the same way (`Graph::step_feed`, `Graph::step_finish`),
which steps the new fragment and the existing nodes it reads. Once the
thread has hydrated the fragment, the engine makes it catch up: before
forwarding a record it waits until the thread is one record less behind
than at the previous record (so the engine goes on at about half the speed
of maintaining everything including the new fragment, which it will have to
do once it is live). When the thread is at most 100 records behind, the
engine waits for it to apply them and adopts the new trees
(`Env::adopt_trees`, which moves their nodes) and the new nodes' state —
hydration flags, error records and per-group error state
(`Graph::adopt_hydrated`; by fingerprint, as the engine may have freed a
node meanwhile and reused its slot); from the next record on the fragment is stepped
by the engine with the rest. Its contents are exactly those of an inline
hydration at *g* followed by the same steps. The version of the inputs it
reads is the fork, not the trees' history, and the catch-up comes from the
forwarded changes, not from the log.

Meanwhile:

- The engine applies records, maintaining the existing views, and answers
  requests. DDL records are applied as they come, except those that would
  hydrate inline or read a relation's contents at once — replacing an
  existing view (`CREATE OR REPLACE`, with the re-planning of the views
  reading it) and `CREATE SINK` — and `Schema` records: such a record, and
  the records after it, wait until no hydration is in progress
  (`must_wait_for_hydration`). The nodes and arrangements of a view or index
  created meanwhile (its statement's DDL record applied, possibly on top of
  the fragment being hydrated) are hydrated by the next hydration, which
  begins when this one is done, with everything pending then; their
  statements are answered when that one is.
- The statement's view or index is in the catalog from *g* on; its DDL
  reply is sent once the fragment is live (a `SUBSCRIBE` is answered then
  too: it starts at the GSN the engine is at then, with its snapshot as of
  that GSN, streamed like any other, and the changes after it).
- Requests that read a relation still being hydrated — reads of the view,
  DML reading it, `SUBSCRIBE` to it — are parked (`Wait::Hydration`) and run
  again once it is live (a fresh read then reads the latest state, which
  includes everything up to the GSN it waited for).
  A `SUBSCRIBE` to a query waits for the hydration in progress to be done
  before its own begins.
- A checkpoint saves the view or index in the catalog without its trees
  (they are not hydrated in the engine yet, so the metadata tree names no
  arrangement of it): recovery rebuilds it as of the checkpoint, and the
  view takes its root's error records then, as every view does
  ([Recovery](recovery.md#hydrations-in-progress)).
- The changes forwarded to the thread and not applied there yet are kept
  in memory, each `Feed` message charging its bytes to the memory budget's
  part for them (`MemBudget::forwarded`, an eighth, at least 4 MiB) until the
  thread has fed them to its dataflow; beyond it the engine waits for the
  thread (also while it is still hydrating), within a record too, so that a
  commit of any size is forwarded in bounded memory.
  The thread steps its graph observing only the nodes whose outputs it
  keeps for replaced views, so the outputs of the others stay spilled or
  are freed as the step goes, as in the engine; its group caches count in
  the engine's share.

What "the view exists as of *g*" means: from *g* on its name is taken, it
is listed, and DDL can build on it; its contents at every GSN after *g* are
those of an inline run, but they can be read only once it is live, at some
*p* > *g* that depends on timing: every read of it, at any read point,
waits until then. *p* is never observable in contents — reads wait,
subscribers and sinks get what an inline run gives them at the same GSNs,
and a checkpoint taken in between holds only the catalog entry, so recovery
(or replay, which hydrates inline at *g*) reaches the same state. A view or
index created while another hydration is in progress is hydrated as of the
GSN the next hydration begins at, not as of its own statement's: its
contents from then on are the same function of the log, and nothing could
read it before.

Replacements need more than the new contents:

- A replaced materialized view (`replace_view`) is switched to its new
  version at the record's GSN *g*; the old version is kept (and maintained)
  until the hydration is done. Its subscribers (same columns) and sinks are
  held — they get nothing — until then; if there are any, the thread
  computes the change from the old to the new contents as of *g* on the
  fork, right after hydrating, and keeps it, with the new version's
  outputs at the records it applies after *g*, in a temporary tree keyed
  by GSN and row (`delta::GsnDeltas`, loaded through a `TreeBuilder`); the
  engine adopts that tree with the fragment (`Env::adopt_temp_tree`) and
  delivers it a chunk at a time, in GSN order, before releasing the holds
  and the old version (`finish_replace`). A sink that asked for a snapshot
  while it was held gets one instead.
  Subscribers and sinks get exactly what an inline replacement at *g* would
  have given them, at the same GSNs, which sinks need: they are idempotent
  by GSN. Sinks of a view whose columns changed start over then (they were
  held too, so that their targets did not get the old version's later
  changes). The change (`Graph::diff_arrangements`) is computed only for
  such subscribers and sinks, and is empty without reading anything when
  the new version's read arrangement is the old one (the definition planned
  to the same dataflow). Two versions whose read arrangements encode rows
  alike (the same key and columns) are merged in key order, so that only
  the entries that differ are decoded; others are read a batch at a time,
  the kept tree adding equal rows up. Nothing of the change is held in
  memory beyond the builder's buffer.
- A table rebuilt after an upstream schema change (`apply_schema_change`)
  gets a new, empty Table node at *g*, to which the table's changes are
  routed from then on; the thread first fills its primary index with the
  old version's rows as of *g* converted to the new shape (`fill_rebuilt`)
  and then hydrates the re-planned views. Reads of the table wait meanwhile,
  its sinks are held and start over afterwards, and no checkpoint is taken
  until it is done: the table's rows are in neither version's index alone,
  and a table's rows exist nowhere else (a crash replays the schema record).
- These records must start their hydration as of their own GSN (the switch
  is at *g* in every run, so that the change delivered at *g* is the same):
  they wait while another hydration is in progress (above).

Replay and recovery hydrate inline, with the same steps (nothing waits for
them). A failure of a background hydration (an internal or I/O error) stops
the engine, as one inline would.

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
  the arrangement, which holds rows only — and it is kept durably by node
  fingerprint, so that views created over such a node after a restart see
  its errors too;
- per operator, the groups currently in error of `Reduce` and `Recursive`
  and the partitions in error of `Window` (`GroupErrors`, kept durably by
  node fingerprint), so that a group's error is retracted when it
  recovers, also after a restart; for `Window`, also whether they are
  known (`GroupErrors::known`, kept durably as a record of its own, so that
  it holds also without partitions in error): an operator whose state is
  not known computes it at the end of the next hydration;
- per materialized view, a map from (SQLSTATE, message) to count, taken
  from its root when the view is created and updated from the root's output
  at every step: always equal to its root node's records. Reads fail while
  any count is positive
  ([Errors as data](../user-guide/temporal-filters-and-errors.md#errors-as-data)).

The durable copy lives in the metadata tree
([On-disk format](../on-disk-format.md#the-metadata-tree)), as one entry per
error record weighted by its count (and one per group in error, and one
per operator whose groups in error are known), and is kept by changes: `Graph::take_error_changes` returns what changed since its
last call — the error deltas of arranged nodes and the groups' changes as
they happen, a node's whole records when it is first seen arranged (an
operator's when it is first seen), and their retraction when it loses its
last arrangement or leaves the graph — and the engine applies them to the
tree at checkpoints and after DDL. So a checkpoint's cost is that of the
error records that changed, not of all of them. Records stay keyed by
(SQLSTATE, message), the message including the offending value: that is
what reads report and what makes a record retract with exactly the rows
that raised it; a view over dirty data has one record per distinct
message, but writes only the records that change.

Recovery restores the per-node and per-operator state by fingerprint after
rebuilding the dataflow and before hydrating what it could not match (and
retracts from the tree the records of nodes that no longer exist); each
view then takes its root's records.

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
