# Planner: binder, decorrelation, optimizer, lowering

A query goes through four stages:

```
SQL text ──sqlparser──▶ AST ──binder──▶ Rel ──decorrelate──▶ Rel ──optimize──▶ Rel ─┬─lower──▶ dataflow (maintained views, SUBSCRIBE)
                                                                                   └─exec───▶ rows (ad hoc reads, DML)
```

`sql::plan_query` runs the first three (`crates/ivm/src/sql/mod.rs`);
materialized views and subscriptions are then lowered into dataflow
(`dataflow/lower.rs`), while ad hoc queries are evaluated by the batch
executor (`exec.rs`). This chapter follows the code in
`crates/ivm/src/sql/` and `crates/ivm/src/dataflow/lower.rs`.

Planning runs on the engine thread, where a panic would stop the engine for
every session. Planning only reads the catalog, so `plan_query` catches a
panic (a bug) and reports it as an internal error (`XX000`, `internal error
while planning: ...`). `sql::plan_stored_query` plans a *stored*
definition — a materialized view at recovery or when it is re-planned, the
views a query expands, dependency analysis — leniently: definitions
accepted by earlier versions may contain a parameter placeholder, which
binds as NULL there (new statements are checked with `plan_query` before
they are logged, which rejects it).

## Parsing

`sql::parser::parse_statement` recognises the engine's own statements
(`CREATE/ALTER/DROP SOURCE`, `CREATE/DROP SINK`, `SUBSCRIBE`,
`COPY (SUBSCRIBE ...)`, `SHOW`, `EXPLAIN`, `CHECKPOINT`, the `AS OF` suffix)
with a small tokenizer, and hands everything else to `sqlparser` 0.63 with
the PostgreSQL dialect. `split_statements` splits a query string at
top-level semicolons (respecting quotes, comments, dollar quoting and
parentheses) and drops statements that are only comments, which become an
empty statement when nothing else is left.

- The `AS OF` suffix is the last `AS OF` (two words separated by any
  whitespace and followed by more text) outside parentheses, string literals
  (`E''` escapes included), quoted identifiers, comments and dollar quotes,
  found on an ASCII-lowercased copy so that byte offsets stay valid. It is
  `FRESH` or `AT LEAST LSN '...'`; `AS OF <gsn>` and `AS OF LATEST`, time
  travel of earlier versions, are refused with `0A000`.
- Before parsing, the statement's text is checked for chains of more than
  50,000 operators at one nesting level (`MAX_OPERATOR_CHAIN`), which are
  refused with `54001 stack depth limit exceeded`: the parser builds such a
  chain into a tree as deep as the chain is long, and cloning or dropping
  it recurses once per level.
- The number of parameters of a statement is the highest `$n` in its text
  (`max_param_index`), which `Bind` checks.

## The logical plan

`sql::plan::Rel` is a relational algebra over bound expressions
(`expr::Expr`, columns referenced by position):

| `Rel` | Meaning |
|---|---|
| `Get { id, name, columns, keys }` | a table or materialized view (with its primary key, if any) |
| `Values { rows, columns }` | constant rows (also system catalogs, computed at bind time) |
| `Map { exprs }` | append computed columns |
| `Filter { preds }` | conjunction of predicates |
| `Project { cols }` | select/reorder columns |
| `Join { kind, on }` | `Inner`, `Left`, `Right`, `Full`, `Semi`, `Anti`; `on` over left ++ right columns |
| `Aggregate { group, aggs }` | output: group columns, then aggregates |
| `Distinct` | set semantics |
| `SetOp { op, all }` | `Union`, `Except`, `Intersect` |
| `UnionAll { inputs }` | multiset union |
| `TopK { group, order, limit, offset, with_ties }` | per-group top-k |
| `Window { funcs }` | append window function columns |
| `FlatMap { funcs, out, ordinality }` | set-returning functions per row |
| `Recursive { id, base, step, distinct }`, `RecRef { id }` | `WITH RECURSIVE` and its self-reference |
| `Apply { kind }` | correlated subquery application (removed by decorrelation) |
| `Negate` | negate multiplicities |

`EXPLAIN` prints this plan ([Introspection](../user-guide/introspection.md#explain-plan-the-logical-plan)).

## Binder

`sql::binder::Binder` (`binder.rs`, `binder_expr.rs`) turns the AST into a
`BoundQuery`: a `Rel`, the output column names, the number of visible
columns (the rest are sort helpers), and the top-level `ORDER BY`, `LIMIT`,
`OFFSET` and `WITH TIES`.

- **Name resolution** uses scopes (`sql::scope::Scope`): every column of the
  current `FROM` clause with its table alias; columns merged by `USING` are
  hidden (an alias of a `USING` join names only its output columns). A
  `GROUP BY` name is a `FROM` column of the query itself, else an output
  alias, and only then a column of an enclosing query. References to
  enclosing queries become `Expr::Outer(depth, column, type)`.
- **Relations**: tables and materialized views become `Get`; virtual views
  are parsed and bound again, inline (recursion depth limit 32), as stored
  definitions; CTEs are bound once and copied into every reference (a
  `WITH RECURSIVE` CTE counts as recursive if binding it with a placeholder
  for its own name shows that it reads itself); system catalogs become
  `Values` computed from the catalog. The untyped output columns of a query,
  subquery, CTE or view (string literals, `NULL`) are resolved to `text`
  once it is bound, except in the branches of a set operation, which are
  unified first (`BoundQuery::resolve_unknowns`).
- **Expressions** are typed: literals start as `unknown` and are resolved by
  context; function and operator calls are resolved by PostgreSQL-style
  overload resolution (`expr::func::resolve`) and coerced with implicit casts
  ([Types](../sql/types.md#type-resolution)). Special SQL syntax is mapped to
  functions (`EXTRACT`, `POSITION`, `SUBSTRING`, `OVERLAY`, `AT TIME ZONE`,
  `SIMILAR TO` → regex, `BETWEEN` → two comparisons, row comparisons →
  conjunctions/disjunctions). Chains of `AND` (`OR`) are bound iteratively
  into one n-ary node, so long boolean chains work at any length; any other
  expression nested more than 1,000 levels deep (`MAX_EXPR_DEPTH`) is
  refused with `54001`, which keeps every later recursive pass shallow.
- **Parameters** (`$n`) bind as literals of the values the client bound.
  While a statement is described (no values yet), an untyped `$n` binds as
  a marker whose first coercion (an operator or function argument, `IN`,
  `LIMIT`, `CAST`, ...) records its type. A `$n` beyond the parameters
  supplied fails with `42P02` (`there is no parameter $n`), except in stored
  definitions, where it binds as NULL.
- **Subqueries** become `Expr::Subquery` (`Exists`, `Scalar`, `In(lhs)`,
  `AnyAll`) holding the subquery's `Rel`.
- **Dependencies**: `Catalog::references` binds a stored query through a
  catalog wrapper that records every relation the binder resolves, also
  inside the views it expands; these are the objects a view depends on (for
  `DROP` and `CASCADE`, re-planning after a change, the recovery order, and
  the relations a subscription reads). `Catalog::direct_references`
  presents views to the binder as tables of their stored columns instead
  of expanding them, which gives only the relations a query names (the web
  UI's dependency graphs). Only a definition that no longer binds falls back
  to matching relation names in its text.
- **Aggregates, window functions and select-list set-returning functions**
  are collected while binding the select list, `HAVING` and `ORDER BY`, and
  replaced by placeholders (`AggRef`, `WindowRef`, `SrfRef`). The `SELECT`
  is then assembled as

  ```
  FROM/WHERE ─▶ Map(group keys, aggregate arguments, filters, orders)
             ─▶ Aggregate            (or UnionAll of one Aggregate per grouping set)
             ─▶ Filter(HAVING)
             ─▶ Map(window arguments) ─▶ Window
             ─▶ FlatMap(set-returning functions)
             ─▶ Map(select list, sort helpers) ─▶ Project
             ─▶ Distinct | TopK(DISTINCT ON)
  ```

  Expressions after the aggregation are rewritten to refer to the
  aggregation's output (grouping expressions are matched by fingerprint);
  ungrouped column references fail with SQLSTATE `42803`. Grouping sets are
  one aggregation per set, unioned, with NULLs in ungrouped columns and one
  "not grouped" flag per grouping column for `grouping()`. Columns
  functionally dependent on a grouped primary key are added to the group key
  when used; the analysis follows a table's columns through maps and
  projections (such as a merged `USING` column) and the left side of
  `LATERAL`. Identical aggregate calls are bound to one aggregate (by a
  fingerprint of the call, a percentile's fraction included). A window
  aggregate's arguments are bound in the query's aggregation context, so
  `sum(count(*)) OVER ()` works; an aggregate whose arguments reference only
  columns of enclosing queries (which PostgreSQL evaluates at the outer
  level) is rejected as not supported.
- Ordered aggregates without `ORDER BY` get their arguments as the order;
  `LIMIT`/`OFFSET` must be constants; `BoundQuery::into_rel` turns a
  `LIMIT` into a `TopK` when a query is used as a relation (subquery, view).

## Decorrelation

`sql::decorrelate` removes every subquery expression in two phases.

**Phase 1 — subqueries to `Apply`.** In a `Filter`, a top-level `EXISTS`,
`IN` (or their negations) becomes `Apply(Semi { lhs, negated, null_aware })`
(`null_aware` implements `NOT IN`). Any other subquery in a `Filter` or a
`Map` is lifted into an `Apply(Mark)` (appends the boolean result of
`EXISTS` / `IN` / `ANY` / `ALL` with three-valued logic) or `Apply(Scalar)`
whose new columns replace the subquery. `Apply(Scalar)` appends the
subquery's value and its number of rows, and the subquery is replaced at its
position by `CASE WHEN rows > 1 THEN <error 21000 more than one row returned
by a subquery used as an expression> ELSE value END`: the cardinality error
is raised only where the expression around the subquery evaluates it, as in
PostgreSQL (a `CASE` or `COALESCE` that does not reach it avoids it).
Subqueries in an inner join's condition move to a filter above the join;
subqueries in outer-join conditions, in `VALUES` and in other positions are
rejected. `LATERAL` items are bound directly as `Apply(Lateral)`.

**Phase 2 — `Apply` to joins**, bottom-up:

- An uncorrelated right side becomes an ordinary join: semi/anti join on the
  `IN` columns, a mark join, a cross join with the one row of the scalar
  subquery's value and row count (an aggregation of `any_value` and
  `count(*)`, or the subquery itself with a count of 1 if it is an aggregate
  without `GROUP BY`), or a cross/left join for `LATERAL`.
- Simple correlations become a direct join: a semi/anti join or an inner
  `LATERAL` whose subquery is an uncorrelated plan below correlated filter
  conjuncts (and maps and projections) — the correlated conjuncts become the
  join condition — and a correlated scalar aggregate
  `(SELECT agg(...) FROM u WHERE inner_expr = outer_expr ...)`, where every
  correlation is an equality between an expression over the subquery's rows
  and one over outer columns only: the aggregate is grouped by the inner
  expressions and left-joined with the same equalities, operators and casts
  as written (so `u.d = t.ts::date` keeps comparing dates); a `count` of a
  missing group becomes 0.
- Everything else uses the general **dependent join**: `D = DISTINCT` of the
  outer columns the subquery references is pushed down through the subquery
  plan (`push_dep`), so that the subquery is computed once for every value of
  `D`, and the result is joined with the outer side on `D` with
  `IS NOT DISTINCT FROM`. For correlated columns of types whose equal values
  can differ in representation (`numeric`, floats, `interval`, `jsonb` and
  arrays of them), `D` also carries their text, so that the subquery sees
  each outer value exactly (`1.00`, not an equal `1.0`). Mark joins compute
  their boolean per `D` value (`EXISTS`: a count; `IN`: counts of matches,
  rows and non-null rows); `ANY`/`ALL` (and row-valued `IN`) compute theirs
  per distinct pair of `D` and compared values (`bool_or` of
  true/unknown/false comparisons) and join it back on those, not on whole
  outer rows. Scalar subqueries yield per `D` value the `any_value` of the
  value and the row count, or the value and a count of 1 when the plan is
  provably unique per `D`.

If an outer reference survives, the query fails with
`unsupported correlated subquery`.

## Optimizer

`sql::optimize::optimize` applies four rewrites:

1. **Constant folding** of every expression (`Expr::fold`), like
   PostgreSQL's `eval_const_expressions`: constant subexpressions of
   immutable functions are evaluated (a constant error surfaces at plan
   time), but parts that a constant makes unreachable are dropped without
   being folded — `CASE` branches with a false or NULL constant condition
   and everything after a true one, `COALESCE` arguments after a non-NULL
   constant, `AND`/`OR` arguments after a constant false/true. A strict
   function with a constant NULL argument folds to NULL, and `NULLIF` with a
   constant NULL argument to its first argument.
2. **Predicate push-down** (`push`): filter conjuncts move below `Project`,
   `Distinct`, `UnionAll`, `SetOp`, `Negate`; below `Map` — a conjunct over
   computed columns with their expressions substituted, as PostgreSQL
   pushes conditions into subqueries, unless an expression may change from
   one evaluation to the next (a non-immutable function, `now()`, a
   subquery, an outer reference), so the other columns are only computed
   for rows that pass; below `Aggregate` if they only use group columns and
   there is a `GROUP BY` (without one the aggregate yields a row even for an
   empty input, so `HAVING false` stays above it); below `TopK` if they only
   use the group columns, below `Window` if they only use columns that
   partition every window function, below `FlatMap` if they only use its
   input's columns, and into join inputs. For joins, a predicate over one
   side moves into that side (respecting outer-join semantics), and outer
   joins are simplified: a `WHERE` predicate that rejects NULLs from the
   nullable side turns `LEFT`/`FULL` into `INNER`/`RIGHT` (and
   symmetrically). (`x op ANY/ALL (array)` is only taken to be NULL when its
   array is NULL: with an empty array, a NULL `x` gives false for `ANY` and
   true for `ALL`.)
3. **Column pruning** (`prune`): every operator computes only the columns
   its consumers demand, so that joins and arrangements carry fewer columns
   and changes to unused columns cancel before they reach a join.
4. **Projection merging**: stacked projections are combined and identity
   projections and empty filters removed.

There is no cost-based join reordering at this stage; the join order of
maintained views is chosen per delta path during lowering, and ad hoc
queries join in plan order (hash or index joins, see
[Batch execution](#batch-execution)).

## Lowering to dataflow

`dataflow::lower::Lowerer` turns a `Rel` into dataflow nodes, adding or
reusing (hash-consed) nodes and arrangements in the `Graph`. It returns a
`Lowered { node, mfp, schema }`: a node plus a *pending* map/filter/project
that is fused into the consumer; `materialize` creates an `Mfp` node only
where an operator needs its input as a node.

| `Rel` | Lowered to |
|---|---|
| `Get` | the table's node or the materialized view's root node |
| `Values` | a `Constant` node |
| `Map`, `Filter`, `Project` | added to the pending MFP; filter conjuncts over the clock are split off into a `TemporalFilter` node with a private event arrangement keyed by (time, kind) |
| inner `Join` (and filters directly above inner joins) | flattened into one n-way `DeltaJoin` with all conditions (below) |
| `Join` `Semi` / `Anti` | presence join (a `DeltaJoin` with a presence input over the key columns of the right side); for non-equality conditions, the matching left rows come from an inner join projected to the left columns, matched with the left side exactly (an *exact* presence input over all its columns); anti = `Union(l, −semi)` ([Dataflow](dataflow.md#deltajoin)) |
| `Join` `Left` / `Right` / `Full` | `Union` of the inner join and the NULL-padded antijoin(s) |
| `Aggregate` | `Reduce` (below) |
| `Distinct` | `Distinct` over an arrangement of all columns |
| `SetOp Union` | `Distinct(Union)` |
| `SetOp Except/Intersect` | `SetOp` over full arrangements of both inputs |
| `UnionAll` | `Union` |
| `TopK` | arrangement keyed by (group, sort columns), `TopK` node |
| `Window` | for each run of functions with the same partitioning and order: arrangement keyed by (partition, order), `Window` node; runs are chained |
| `FlatMap` | `FlatMap` node |
| `Negate` | `Negate` node |
| `Recursive` | `Recursive` node over its source tables/views, with an arrangement of its own output |

### Aggregations

`lower_aggregate`:

- `DISTINCT` aggregates: if all aggregates are `DISTINCT` over the same
  arguments and the same `FILTER`, the input becomes
  `Distinct(Project(group ++ arguments [++ filter]))` and the aggregates run
  over it, filtered on the flag column: a value counts once among the rows
  that pass the filter, and a group none of whose rows pass it still exists.
  Otherwise one `DISTINCT` aggregate after another is split off into an
  aggregation of its own, all joined on the group key
  (`IS NOT DISTINCT FROM`).
- Ordered aggregates with different `ORDER BY`s are split the same way.
- The input is projected to the columns the aggregates read (the input
  arrangement is state, so its width matters) and arranged by the group key
  (plus the ordered aggregate's sort columns) with the summaries every
  aggregate needs ([Dataflow](dataflow.md#reduce)); the `Reduce` node gets
  an output arrangement keyed by the group key.

### Delta join planning

`plan_join` receives the join inputs (each a node, an MFP and a width) and
the conditions over the concatenated row:

1. **Equality edges**: conditions `a = b` or `a IS NOT DISTINCT FROM b`
   where each side references exactly one input, and the inputs differ.
   Everything else is a residual condition.
2. **Sizes**: the exact entry count of each input's first arrangement (one
   million if it has none), at the time the view is planned.
3. **Computed keys**: an input whose side of some edge is not a plain column
   of its node (computed by its MFP or by the condition) is materialized: an
   `Mfp` node applies its MFP and appends the computed key expressions as
   columns, and the input's MFP projects them away again. Arrangement keys
   are thus plain columns, evaluated only on rows that pass the input's
   filters ([Dataflow](dataflow.md#deltajoin)).
4. For each input *i*, a **delta path** is built greedily: starting with *i*
   bound, repeatedly pick the unbound input with the highest
   `(number of edges to bound inputs, unique, −size)`, where *unique* means
   the edges cover the input's primary key (or the input is a presence
   input). The lookup key is the list of the input's edge expressions,
   rewritten over the input node's raw columns through its MFP (or the
   appended key columns); if it covers exactly the primary key, it is
   ordered like the primary key (so the table's primary index is reused),
   otherwise by expression fingerprint (deterministic). An arrangement of
   the input node with that key is added (or shared). A lookup keyed by the
   values of an exact presence input is marked exact. Residual conditions
   are attached to the first step at which all their inputs are bound.
5. A path that has no edge to the remaining inputs uses an empty key: the
   lookup scans the whole input.

The measured delta rates shown by `EXPLAIN MAINTENANCE` are not used by the
planner. Because sizes are read when a view is planned, re-planning the same
catalog later (at recovery) can order a path differently; the view's
arrangements then have other fingerprints and are rebuilt
([Recovery](recovery.md#matching-checkpointed-indexes)).

### Finishing a view

`Engine::create_view_rt` plans the view's stored definition
(`plan_stored_query`), fails if the plan reads a broken view, lowers it
(releasing what a failed lowering created), pins the lowered root node,
chooses the view's read arrangement (the root's first arrangement, or a new
full arrangement), and frees planning leftovers (`gc_since`). The caller
hydrates the new parts ([Dataflow](dataflow.md#hydration)) and gives the
view its root's error records.

## Batch execution

Ad hoc queries (and the evaluation of DML statements) do not build dataflow.
`exec::ExecCtx` evaluates the optimised `Rel` directly against the
arrangements as they are (their current state):

- `Get` scans the node's first hydrated arrangement (a node without one is
  computed from its inputs' snapshots);
- a base relation read — a `Get` under filters and projections — reads one
  arrangement: equalities with constants on a key prefix, optionally
  followed by a range (`<`, `<=`, `>`, `>=`, `BETWEEN`) on the next key
  column, select a key range of the arrangement they narrow most (ascending
  keys only; constants of exactly the column's type; only hydrated
  arrangements). Only the columns the projection and the predicates use
  are decoded, and every predicate is still checked on every row;
- joins are hash joins on the equality conditions (all join kinds), except
  that an inner, left, semi or anti join whose right side is a base relation
  read, and whose left side is small compared to an arrangement of that
  relation keyed by equated columns (less than 1/64 of its entries), looks
  each left row's key up in the arrangement instead;
- aggregation is hash aggregation using the same aggregate definitions and
  summary arithmetic as `Reduce`, window functions the same partition
  computation as `Window`, `WITH RECURSIVE` semi-naive iteration;
- a query's result is produced by `ExecCtx::run_query`, which streams:
  base relation reads, filters, projections, maps, set-returning functions,
  `UNION ALL` and the probe side of joins hand each row on as it is found
  (`ExecCtx::stream`, with a sink that returns false once it has what it
  needs), and aggregation consumes its input as it is read; the other
  operators compute their result first. Without `ORDER BY`, rows are
  handed on as they come and the scan stops once `OFFSET` + `LIMIT` rows
  are out. With `ORDER BY`, a read whose plan is a base relation read
  under maps, filters and projections, by an arrangement whose key order —
  after the key columns its equalities fix — is the `ORDER BY` (same
  columns, direction, null placement and key type; a computed copy of a
  column counts as the column), scans that arrangement in order and stops
  the same way (`ordered_read`; an equality that narrows another
  arrangement wins, and that one is read and sorted). Otherwise the rows go
  through `TopN`, which keeps the first `OFFSET` + `LIMIT` (and their
  ties), pruning whenever it holds twice that, and sorts at the end (all
  rows without `LIMIT`). A plan with `Negate` is consolidated before its
  rows are read. Multiplicities are expanded, `OFFSET`/`LIMIT`/`WITH TIES`
  applied and the hidden sort columns truncated as the rows go out
  (`Emitter`). They go to the session in chunks of 1,024 rows or about
  256 KB (`coord/read.rs`, `ChunkedReply`: a result of one chunk as
  `ExecResult::Query`, a larger one as `ExecResult::Rows`, whose later
  chunks follow on a stream, `coord::stream`; a closed stream ends the
  read).

Before a read runs, `now()` and its relatives are bound to the logical clock,
and the read fails if it touches a broken view
(`55000`) or a materialized view with error records. Expression errors fail
the query, as in PostgreSQL (after the chunks already sent, if any).

A read runs on the engine thread with a budget of 50,000 entries that its
scans may visit (`Env::set_read_budget`; DML is evaluated without one), and
of 50,000 rows or 8 MiB that it may return: it keeps its rows until it is
done (it sends nothing before, so that it can still go elsewhere), and
then hands them to the session at once, counted in the outputs' memory. A
read that exceeds either is abandoned there and run again on a reader thread
(`coord/reader.rs`, four of them, started when first needed) against a fork
of the storage taken at once — every tree as it is after the last applied
record ([Storage](storage.md#forks)) — and a copy of the part of the graph
it reads (`Graph::fork_of`): the same read point and plan, hence the same
result, while the engine goes on applying records and answering other
statements. The reader thread streams the rows to the session as they are
found, waiting for it to take them: at most two chunks are in flight
(`coord::stream`), so a large result reaches a slow client in bounded
memory; the reader stops when the session drops the stream (the client
went away, or a portal was closed) or the statement is cancelled while it
waits (the session then reports `57014`). A small read costs no fork; a
large one costs the engine thread its budget and the fork (one
reference-count increment per loaded node), and holds a reader thread
while its client reads. The snapshots that a `SUBSCRIBE ... WITH SNAPSHOT`
starts with and that a sink resynchronises its target from are read the
same way (`Engine::snapshot_stream`): within the budget on the engine
thread, else on a reader thread from a fork as of the GSN the subscription
or snapshot starts at, as the consumer takes them. The consumer gets the
snapshot as one event in its queue at that GSN (`SubEvent::Snapshot`,
`SinkMsg::Snapshot`), before the changes after it, which the engine
delivers as usual meanwhile: it keeps nothing for the snapshot.

Because the executor and the dataflow share the aggregate, window and
expression code, a maintained view and the same query run ad hoc compute
the same values; the self-differential test compares the two, and the
differential tester compares maintained views with PostgreSQL
([Testing](../testing.md)).
