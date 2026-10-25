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

## Parsing

`sql::parser::parse_statement` recognises the engine's own statements
(`CREATE/ALTER/DROP SOURCE`, `CREATE/DROP SINK`, `SUBSCRIBE`,
`COPY (SUBSCRIBE ...)`, `SHOW`, `EXPLAIN`, `CHECKPOINT`, the `AS OF` suffix)
with a small tokenizer, and hands everything else to `sqlparser` 0.63 with
the PostgreSQL dialect. `split_statements` splits a query string at
top-level semicolons (respecting quotes, comments, dollar quoting and
parentheses).

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
  hidden. References to enclosing queries become `Expr::Outer(depth, col)`.
- **Relations**: tables and materialized views become `Get`; virtual views
  are parsed and bound again, inline (recursion depth limit 32); CTEs are
  bound once and copied into every reference; system catalogs become
  `Values` computed from the catalog.
- **Expressions** are typed: literals start as `unknown` and are resolved by
  context; function and operator calls are resolved by PostgreSQL-style
  overload resolution (`expr::func::resolve`) and coerced with implicit casts
  ([Types](../sql/types.md#type-resolution)). Special SQL syntax is mapped to
  functions (`EXTRACT`, `POSITION`, `SUBSTRING`, `OVERLAY`, `AT TIME ZONE`,
  `SIMILAR TO` → regex, `BETWEEN` → two comparisons, row comparisons →
  conjunctions/disjunctions).
- **Subqueries** become `Expr::Subquery` (`Exists`, `Scalar`, `In(lhs)`,
  `AnyAll`) holding the subquery's `Rel`.
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
  when used.
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
(appends the scalar value) whose new column replaces the subquery. Subqueries
in an inner join's condition move to a filter above the join; subqueries in
outer-join conditions and in `VALUES` are rejected. `LATERAL` items are bound
directly as `Apply(Lateral)`.

**Phase 2 — `Apply` to joins**, bottom-up:

- An uncorrelated right side becomes an ordinary join: semi/anti join on the
  `IN` columns, a mark join, a cross join with a `SingleValue` aggregate for
  scalar subqueries (which fails if the subquery returns more than one row),
  or a cross/left join for `LATERAL`.
- Simple correlations — equality predicates between inner and outer columns
  in a filter, possibly below a scalar aggregate — become a direct join (for
  a correlated scalar aggregate: a left join with the aggregate grouped by the
  correlated columns).
- Everything else uses the general **dependent join**: `D = DISTINCT` of the
  outer columns the subquery references is pushed down through the subquery
  plan (`push_dep`), so that the subquery is computed once for every value of
  `D`, and the result is joined with the outer side on `D` with
  `IS NOT DISTINCT FROM`. Mark joins compute their boolean per `D` value
  (`EXISTS`: a count; `IN`: counts of matches, rows and non-null rows;
  `ANY`/`ALL`: `bool_or` of true/unknown/false comparisons), scalar subqueries
  add a `SingleValue` aggregate unless the plan is provably unique per `D`.

If an outer reference survives, the query fails with
`unsupported correlated subquery`.

## Optimizer

`sql::optimize::optimize` applies four rewrites:

1. **Constant folding** of every expression.
2. **Predicate push-down** (`push`): filter conjuncts move below `Map`,
   `Project`, `Distinct`, `UnionAll`, `SetOp`, `Negate`, below `Aggregate` if
   they only use group columns (a conjunct that uses no columns at all also
   qualifies, which is wrong for an aggregate without `GROUP BY`:
   `SELECT count(*) FROM t HAVING false` returns one row with 0 instead of
   none — a known problem), below `TopK` if they only use the group
   columns, below `Window` if they only use columns that partition every
   window function, and into join inputs. For joins, a predicate over one
   side moves into that side (respecting outer-join semantics), and outer
   joins are simplified: a `WHERE` predicate that rejects NULLs from the
   nullable side turns `LEFT`/`FULL` into `INNER`/`RIGHT` (and symmetrically).
3. **Column pruning** (`prune`): every operator computes only the columns
   its consumers demand, so that joins and arrangements carry fewer columns
   and changes to unused columns cancel before they reach a join.
4. **Projection merging**: stacked projections are combined and identity
   projections and empty filters removed.

There is no cost-based join reordering at this stage; the join order of
maintained views is chosen per delta path during lowering, and ad hoc
queries use hash joins in plan order.

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
| `Join` `Semi` / `Anti` | presence join (a `DeltaJoin` with a presence input over the key columns of the right side); for non-equality conditions, the matching left rows come from an inner join, projected and made distinct; anti = `Union(l, −semi)` |
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
  arguments and without `FILTER`, the input becomes
  `Distinct(Project(group ++ arguments))`; otherwise the aggregates are split
  into several aggregations joined on the group key (`IS NOT DISTINCT FROM`).
- Ordered aggregates with different `ORDER BY`s are split the same way.
- The input is projected to the columns the aggregates read (the input
  arrangement is state, so its width matters) and arranged by the group key
  (plus the ordered aggregate's sort columns) with the summaries every
  aggregate needs; the `Reduce` node gets an output arrangement keyed by the
  group key.

### Delta join planning

`plan_join` receives the join inputs (each a node, an MFP and a width) and
the conditions over the concatenated row:

1. **Equality edges**: conditions `a = b` or `a IS NOT DISTINCT FROM b`
   where each side references exactly one input, and the inputs differ.
   Everything else is a residual condition.
2. **Sizes**: the exact entry count of each input's first arrangement (one
   million if it has none).
3. For each input *i*, a **delta path** is built greedily: starting with *i*
   bound, repeatedly pick the unbound input with the highest
   `(number of edges to bound inputs, unique, −size)`, where *unique* means
   the edges cover the input's primary key (or the input is a presence
   input). The lookup key is the list of the input's edge expressions,
   rewritten over the input node's raw columns through its MFP; if it covers
   exactly the primary key, it is ordered like the primary key (so the
   table's primary index is reused), otherwise by expression fingerprint
   (deterministic). An arrangement of the input node with that key is added
   (or shared). Residual conditions are attached to the first step at which
   all their inputs are bound.
4. A path that has no edge to the remaining inputs uses an empty key: the
   lookup scans the whole input.

The measured delta rates shown by `EXPLAIN MAINTENANCE` are not used by the
planner.

### Finishing a view

`Engine::create_view_rt` pins the lowered root node, chooses the view's read
arrangement (the root's first arrangement, or a new full arrangement), frees
planning leftovers (`gc_since`), and the caller hydrates the new parts
([Dataflow](dataflow.md#hydration)).

## Batch execution

Ad hoc queries (and the evaluation of DML statements) do not build dataflow.
`exec::ExecCtx` evaluates the optimised `Rel` directly against the
arrangements at a given `ReadAt`:

- `Get` scans the node's first hydrated arrangement (any table or
  materialized view has one); `Filter(Get)` with `column = constant`
  predicates uses the arrangement with the longest matching key prefix for a
  prefix lookup;
- joins are hash joins on the equality conditions (all join kinds);
- aggregation is hash aggregation using the same aggregate definitions and
  summary arithmetic as `Reduce`, window functions the same partition
  computation as `Window`, `WITH RECURSIVE` semi-naive iteration;
- results are consolidated; the engine then sorts, applies
  `OFFSET`/`LIMIT`/`WITH TIES` and projects the visible columns.

Because the executor and the dataflow share the aggregate, window and
expression code, a maintained view and the same query run ad hoc compute
the same values; the self-differential test compares the two, and the
differential tester compares maintained views with PostgreSQL
([Testing](../testing.md)).
