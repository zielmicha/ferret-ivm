# Maintained views

Materialized views and `SUBSCRIBE (query)` are *maintained*: the engine
builds a dataflow for the query and updates its result at every GSN that
changes its inputs. This page explains which queries can be maintained, what
is rejected and why, and how each construct is maintained — which determines
what a change costs. The operators themselves are described in
[Dataflow](../internals/dataflow.md).

## Why maintained views must be deterministic

The engine's central invariant is that the state of every index at GSN *g*
is a pure function of the input log up to *g*. Several mechanisms rely on it:

- **Recovery** loads a checkpoint and replays the log after it; replay must
  produce exactly the state the engine had before the crash.
- **Hydration**: a new view is computed from existing indexes at its creation
  GSN and then maintained incrementally. Both paths must agree, now and after
  any number of changes.
- **Sharing**: a sub-plan identical to one of another view reuses that
  view's operators and indexes; it must compute the same thing.
- **Time travel and repeatable reads**: reading a view `AS OF g` must give
  the same answer every time.

A function whose result depends on something other than its arguments
breaks all of these. Such functions are therefore rejected, and constructs
whose result PostgreSQL leaves unspecified are pinned down.

## What is rejected

When a materialized view is created (and when a `SUBSCRIBE (query)` starts),
every expression of the optimised plan — including expressions inside
decorrelated subqueries — is checked (`check_deterministic` in
`coord/ddl.rs`):

1. **Non-immutable functions.** The registry marks functions as immutable,
   stable or volatile. Everything that is not immutable is rejected:

   | Function | Volatility |
   |---|---|
   | `random()`, `gen_random_uuid()`, `clock_timestamp()`, `set_config()` | volatile |
   | `now()`, `current_timestamp`, `transaction_timestamp()`, `statement_timestamp()`, `localtimestamp`, `current_date` | stable (the clock) |
   | `current_setting()` | stable (the session) |
   | `pg_backend_pid()` | stable (the process) |

   ```
   ERROR:  function random() is not allowed in a materialized view
   DETAIL:  Maintained views must be deterministic: their state is a pure function of the input log.
   ```

2. **The clock outside temporal filters.** The `now()` family is allowed
   only in conditions that the engine can maintain as time passes (a
   *temporal filter*, see
   [Temporal filters](../user-guide/temporal-filters-and-errors.md#accepted-forms)):
   the logged clock ticks make it an input like any other. Anywhere else:

   ```
   ERROR:  now() is only supported in temporal filters of maintained views
   HINT:  Use a top-level WHERE condition comparing a column expression with now() (optionally plus or minus a constant interval).
   ```

3. **Reads of the catalog.** System relations (`pg_catalog.*`,
   `information_schema.*`, `ivm.*`), casts to `regclass` (of names or OIDs)
   and `pg_get_viewdef()`, `pg_get_indexdef()` and `pg_get_constraintdef()`
   are bound as constants from the catalog when the query is planned: a
   maintained view would not follow the catalog's changes (no log record
   changes it), and planning it again — a restart does — would change its
   contents. They are rejected (`system relation "pg_class" is not allowed
   in a materialized view`, `a cast to regclass ...`, `function
   pg_get_viewdef() ...`). Casts to `regtype` and `regnamespace` read fixed
   OIDs and are allowed.

Virtual views and ad hoc queries are not checked; they may use any function
and read the catalog as it is when they run.
A materialized view that reads a virtual view inlines its query, so the
check covers it, and `CREATE OR REPLACE` of a view is rejected (SQLSTATE
`2BP01`, `cannot replace view "v": materialized view "m" would no longer be
valid: ...`) when a materialized view reading it would then no longer plan
or would fail the check.

All other functions are immutable — including those that are only stable in
PostgreSQL because they depend on session settings that are fixed here (the
time zone is UTC: `to_char(timestamptz, ...)`, `date_trunc(text,
timestamptz)`, `timestamp` ↔ `timestamptz` casts; `DateStyle` and
`extra_float_digits` for the text of values in `concat`, `format` or
`to_json`), and the catalog helpers for client tools, which return
constants here (`version()`, `current_user`, `has_table_privilege`,
`pg_relation_size`, `pg_is_in_recovery`, ...).

## What is pinned down instead

| PostgreSQL leaves unspecified | The engine does |
|---|---|
| input order of `string_agg`, `array_agg`, `json_agg`, `jsonb_agg`, `json(b)_object_agg` without `ORDER BY` | orders the input by the argument values |
| rounding of floating-point `sum`/`avg`/`variance`/`stddev` (depends on summation order) | sums exactly and rounds once: the result depends only on the multiset of inputs |
| which representation of equal values a group key, `DISTINCT`, `min`/`max` shows (`1.0` vs `1.00`, `-0` vs `0`, `'1 mon'` vs `'30 days'`) | the one that sorts first in the index (by its exact encoding); `numeric` sums use the largest display scale present |
| which rows `LIMIT`, `DISTINCT ON` or `FETCH ... WITH TIES` keep among ties | the first in index order (by sort key, then by the encoding of the rest of the row) |
| which of several errors a query reports | error records are kept per (SQLSTATE, message); reads report the smallest |

These rules make an incrementally maintained result identical to a
recomputation, whatever the order of past changes.

## What can be maintained

Every construct of the [query language](queries.md) can be used in a
maintained view, subject to the rules above. How each construct is
maintained:

| Construct | Operator | Work per changed input row | State |
|---|---|---|---|
| `WHERE`, select-list expressions, projections | `Mfp` (fused map/filter/project) | constant | none |
| inner joins (any number of inputs), `CROSS JOIN`, comma joins | `DeltaJoin` | one chain of index lookups into the other inputs, in an order chosen to keep intermediate results small | indexes of each input on its join keys; primary-key indexes are reused, identical indexes shared. A key computed by an expression (`ON a.x = b.y + 1`) is computed by an `Mfp` over that input, after its filters, and indexed as a column |
| join with no equality condition between two inputs | `DeltaJoin` lookup with an empty key | a scan of the whole other input | an index of the input |
| `LEFT` / `RIGHT` / `FULL` joins | inner join plus anti-joins, NULL-padded, in a `Union` | as the joins | as the joins |
| `EXISTS`, `IN`, `= ANY`, semi-joins | presence join: lookup of the key's count in a count-summarised index (when correlated by equalities). With other conditions an inner join finds the matching left rows, and the left rows are looked up in a presence index of those, by identity (equal values in different representations, `1.0` and `1.00`, stay apart) | one lookup | index of the subquery's key columns (or of the matching left rows) |
| `NOT EXISTS`, `NOT IN`, anti-joins | input minus its semi-join | as semi-join | as semi-join |
| correlated and scalar subqueries | decorrelated into joins and aggregations | as those | as those |
| `GROUP BY` with `count`, `sum`, `avg`, `min`, `max`, `bool_and/or`, `every`, `stddev*`, `var*`, `any_value` | `Reduce` | per changed group, one range summary over the group in an index whose nodes carry partial aggregates — O(log n) regardless of group size; recently touched groups are cached in memory. If the query also has an ordered aggregate (next row), all its aggregates share one `Reduce` that rescans each changed group | input index keyed by the group key, with per-node summaries; output index keyed by the group key |
| `string_agg`, `array_agg`, `json(b)_agg`, `json(b)_object_agg`, `mode`, `percentile_cont/disc` | `Reduce` | rescan of each changed group | as above (input index ordered by group key, then the aggregate's `ORDER BY`) |
| `DISTINCT` aggregates | aggregation over `DISTINCT (group, arguments [, FILTER condition])` | as `Distinct` + `Reduce` | as those |
| several ordered aggregates with different orders, several different `DISTINCT` argument sets or filters, or `DISTINCT` and other aggregates together | one `Reduce` per kind, joined on the group key | sum of the parts | sum of the parts |
| `GROUPING SETS`, `ROLLUP`, `CUBE` | one aggregation per grouping set, unioned | per set | per set |
| `SELECT DISTINCT`, `UNION` | `Distinct` | two key-prefix scans per changed value (before and after) | index of all columns |
| `UNION ALL` | `Union` | constant | none |
| `INTERSECT [ALL]`, `EXCEPT [ALL]` | `SetOp` | four key-prefix scans per changed value (each input, before and after) | index of all columns of each input |
| `ORDER BY ... LIMIT/OFFSET`, `FETCH ... [WITH TIES]`, `DISTINCT ON` | `TopK` | per changed group, the first `offset + limit` rows before and after the change | input index keyed by (group, sort keys) |
| window functions | `Window` | recomputation of each changed partition (before and after) | input index keyed by (partition, order) |
| set-returning functions | `FlatMap` | the function's output | none |
| `WITH RECURSIVE` | `Recursive` | recomputation of the whole fixpoint from the current inputs, diffed against the previous result | index of the result (and of the inputs) |
| conditions over `now()` | `TemporalFilter` | bisection of the row's validity interval; due events at each tick | index of pending (time, row) events |
| `VALUES` | `Constant` | — | none |

Output of every operator is *consolidated* per GSN: a row inserted and
deleted in the same GSN never appears, and an update is one retraction and
one insertion.

Operators and indexes are hash-consed by their structure: two views that
share a sub-plan (same tables, same joins, same filters) share its operators
and indexes, and so does a view with the indexes created by
`CREATE INDEX`. `EXPLAIN DATAFLOW` shows `shared xN` on shared nodes.

## Errors

Expression errors inside a maintained view do not fail the statement that
caused them; they become error records that make reads of the view fail
until the offending rows are gone
([Errors as data](../user-guide/temporal-filters-and-errors.md#errors-as-data)).
This includes aggregates whose result is out of range: a `float8` or
`real` sum that overflows, or a `numeric` sum beyond numeric's range (the
sum itself is kept exactly, so the group recovers when rows are removed),
replaces the group's row by an error record of the group. Which groups (and
recursive queries) are in error is part of the checkpointed state, so their
error records are retracted correctly after a restart too.
