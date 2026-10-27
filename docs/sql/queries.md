# Queries

The same query language is used for ad hoc `SELECT`s, virtual views,
materialized views and `SUBSCRIBE`. It follows PostgreSQL 17 syntax and
semantics; this page lists what is supported and where the engine differs.
Materialized views have additional restrictions
([Maintained views](maintained-views.md)). Code: `crates/ivm/src/sql/`
(`binder.rs`, `binder_expr.rs`, `decorrelate.rs`, `optimize.rs`).

## SELECT

### Select list

- Expressions with optional `AS alias`; `*` and `table.*`.
- Output column names follow PostgreSQL's rules (`FigureColname`): the
  column or function name, `?column?`, `case` (or the `ELSE` result's name
  when that is a column or function), the outermost cast's type when the
  argument has no better name (`1::text::int` → `int4`, `'{}'::int[]` →
  `int4`), the subscripted expression's name (`a[1]` → `a`), `substr`,
  `ltrim`/`rtrim`/`btrim`, and `regclass` (`regtype`, `regnamespace`) for
  `reg*` casts. Differences: `SELECT true` is named `bool` (PostgreSQL:
  `?column?`), an `OVERLAPS` expression `?column?` (PostgreSQL:
  `overlaps`), and the column of `jsonb_array_elements(...) t` and
  `jsonb_array_elements_text(...) t` is named `t` (PostgreSQL: `value`).
- Set-returning functions in the select list (`SELECT id, unnest(a) FROM t`)
  — see [Set-returning functions](#set-returning-functions).

### DISTINCT and DISTINCT ON

- `SELECT DISTINCT` removes duplicate rows. As in PostgreSQL, its
  `ORDER BY` expressions must appear in the select list.
- `SELECT DISTINCT ON (expr, ...)` keeps the first row of each group in
  `ORDER BY` order. The `DISTINCT ON` items may be output columns (by name or
  position) or any expressions over the input, and `ORDER BY` may use
  columns that are not selected. Unlike PostgreSQL, the `ORDER BY` need not
  start with the `DISTINCT ON` expressions (PostgreSQL: `SELECT DISTINCT ON
  expressions must match initial ORDER BY expressions`): each group keeps its
  first row in `ORDER BY` order.

### FROM

| Item | Notes |
|---|---|
| `table`, `materialized_view`, `view` `[ [AS] alias [ (column, ...) ] ]` | virtual views are expanded inline (up to 32 levels deep) |
| `schema.name` | see [table names](../user-guide/sources.md#table-names); `pg_catalog.*`, `information_schema.*`, `ivm.*` system relations |
| `( query ) [AS] alias [ (column, ...) ]` | derived table |
| `LATERAL ( query ) alias` | may reference items to its left |
| `[LATERAL] function(args) [WITH ORDINALITY] [ [AS] alias [ (column, ...) ] ]` | [set-returning functions](#set-returning-functions); arguments may reference items to the left (implicitly lateral) |
| `UNNEST(array [, ...]) [WITH ORDINALITY]` | several arrays give one column per array, zipped and padded with NULLs (as PostgreSQL's `ROWS FROM`) |
| `a [INNER] JOIN b ON ...`, `LEFT`, `RIGHT`, `FULL [OUTER] JOIN`, `CROSS JOIN`, `,` | all join kinds |
| `JOIN ... USING (columns)`, `NATURAL JOIN` | merged columns as in PostgreSQL (`COALESCE` for `FULL`) |
| `a JOIN LATERAL (...) l ON ...`, `LEFT JOIN LATERAL (...) l ON true` | only inner and left lateral joins; a left lateral join needs `ON true` |
| `( a JOIN b ... ) [AS alias]` | parenthesised joins; the alias names the join's output columns |

Not supported: `ROWS FROM (...)`, table versions (`FOR SYSTEM_TIME`; use
`AS OF`). `TABLESAMPLE` is accepted and **ignored** (all rows are returned).

### WHERE, GROUP BY, HAVING

- `WHERE` takes any boolean expression, including subqueries. As in
  PostgreSQL, a condition on the output columns of a derived table or view
  is applied inside it, before its other columns are computed, so their
  expressions are not evaluated (and cannot fail) for the rows the condition
  removes — unless the condition uses a column whose expression may change
  between evaluations (non-immutable functions, `now()`, subqueries).
- `GROUP BY` items may be expressions, output column positions (`GROUP BY 1`)
  or output column aliases. A bare name means a column of the query's own
  `FROM` clause if there is one, then an output alias (one name given to
  several identical output expressions is not ambiguous), and only then a
  column of an enclosing query, as in PostgreSQL.
- `GROUP BY ()` (the empty grouping set: one group even without aggregate
  functions), `GROUPING SETS ( ... )`, `ROLLUP ( ... )`, `CUBE ( ... )` and
  their combinations (cross product); `grouping(expr, ...)` (fewer than 32
  arguments). `CUBE` is limited to 12 elements, the expansion to 4096
  grouping sets.
- Functional dependencies: columns of a table whose primary key is grouped
  may be used without being grouped, also through `USING` joins, `LATERAL`
  items and derived tables (PostgreSQL does not look into derived tables, so
  the engine accepts more queries).
- `HAVING` without `GROUP BY` aggregates over all rows and returns one row,
  or none when the condition is false.
- Not supported: `GROUP BY ALL`, `GROUP BY` modifiers (`WITH ROLLUP`
  MySQL-style).

### Window functions

```sql
func(args) OVER ( [ window_name ] [ PARTITION BY expr, ... ] [ ORDER BY expr, ... ] [ frame ] )
func(args) OVER window_name
... WINDOW window_name AS ( window_spec ) [ , ... ]

frame: { ROWS | RANGE | GROUPS } { start | BETWEEN start AND end }
start/end: UNBOUNDED PRECEDING | n PRECEDING | CURRENT ROW | n FOLLOWING | UNBOUNDED FOLLOWING
```

- Functions: `row_number()`, `rank()`, `dense_rank()`, `percent_rank()`,
  `cume_dist()`, `ntile(n)`, `lag(x [, offset [, default]])`,
  `lead(x [, offset [, default]])`, `first_value(x)`, `last_value(x)`,
  `nth_value(x, n)` (NULL for a NULL `n`), and every
  [aggregate](#aggregate-functions) except with `DISTINCT` or `FILTER`.
- Default frame: `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.
- `ROWS` and `GROUPS` offsets are integer constants. `RANGE` offsets need
  exactly one `ORDER BY` column of a numeric type (offset of the same type)
  or of a date/time type (`interval` offset); `date` values are compared as
  timestamps. A `RANGE` bound that overflows the column's type lies beyond
  every value, as in PostgreSQL.
- Frames are validated like PostgreSQL's: negative offsets (`frame starting
  offset must not be negative`, SQLSTATE `22013`; for `RANGE`, `invalid
  preceding or following size in window function`), frames that start after
  they end by construction (`ROWS BETWEEN 1 FOLLOWING AND 1 PRECEDING`,
  `ROWS 1 FOLLOWING`), a start at `UNBOUNDED FOLLOWING` or an end at
  `UNBOUNDED PRECEDING`, and `GROUPS` without `ORDER BY` are rejected.
- Named windows follow PostgreSQL's copy rules (a referencing window may add
  `ORDER BY` and a frame, but not `PARTITION BY`, and cannot copy a window
  that has a frame).
- Window functions may take aggregates as arguments (`rank() OVER (ORDER BY
  sum(x))`), and so may window aggregates (`sum(count(*)) OVER ()`).
- Not supported: frame `EXCLUDE` (syntax error), `DISTINCT` and `FILTER` in
  window aggregates. `IGNORE NULLS` / `RESPECT NULLS` are accepted and
  ignored.

### Set operations

`UNION`, `INTERSECT` and `EXCEPT`, each with `ALL` or `DISTINCT` (default),
with PostgreSQL's multiset semantics (`INTERSECT ALL` keeps
`min(m, n)` copies, `EXCEPT ALL` keeps `m − n`). Column types are unified
like PostgreSQL's `UNION` type resolution ([common types](types.md#common-type-resolution)).
Duplicates are found by value equality, except that `character(n)` values
keep their padding: `'a'::char(3)` and `'a'::char(5)` are different rows
here (see [Comparison and ordering](types.md#comparison-and-ordering)).

### ORDER BY, LIMIT, OFFSET, FETCH

- `ORDER BY` items: output column names or positions, or expressions over
  the input (including columns not in the select list, except with plain
  `DISTINCT`). A name that matches several output columns is ambiguous only
  if they are different expressions (`SELECT a, a FROM t ORDER BY a` works).
  `ASC`/`DESC`, `NULLS FIRST`/`NULLS LAST`; the default is
  `NULLS LAST` for `ASC` and `NULLS FIRST` for `DESC`. Rows that are equal
  on all sort keys are ordered by a deterministic internal tie-break (their
  encoded bytes), so the output order is repeatable.
- `LIMIT n`, `LIMIT ALL`, `LIMIT NULL`, `OFFSET n [ROW|ROWS]`,
  `FETCH FIRST|NEXT [n] ROW|ROWS ONLY|WITH TIES` (`WITH TIES` needs
  `ORDER BY`), and `OFFSET n ROWS FETCH NEXT m ROWS ONLY`. Counts must be
  constant expressions (`LIMIT 1 + 1`, or a parameter, but not
  `LIMIT (SELECT 1)`) and must not be negative (SQLSTATE `2201W`). `LIMIT`
  and `FETCH` cannot be combined (`cannot use both LIMIT and FETCH`).
- `ORDER BY` in a subquery or view without `LIMIT`/`OFFSET` does not affect
  the result (relations have no order); with them it selects the rows
  (top-k).
- Not supported: `FETCH ... PERCENT`, `ORDER BY ALL`, `LIMIT BY`,
  `FOR UPDATE`/`FOR SHARE`.

### WITH (common table expressions)

- `WITH name [ (columns) ] AS [ [ NOT ] MATERIALIZED ] ( query ) [ , ... ]`
  — CTEs are inlined wherever they are referenced (`MATERIALIZED` is
  ignored); a CTE name shadows a catalog object.
- `WITH RECURSIVE name [ (columns) ] AS ( non_recursive_term UNION [ ALL ] recursive_term )`
  — evaluated by semi-naive iteration (at most 1,000,000 iterations). A CTE
  of a `WITH RECURSIVE` is recursive if its query reads its own name as a
  relation. Untyped literal columns of the non-recursive term are `text`.
  The fixpoint is always computed completely: a recursive query that relies
  on an outer `LIMIT` to stop fails with `recursive query exceeded the
  iteration limit` (PostgreSQL stops early). Not supported: correlated
  recursive queries, `SEARCH` and `CYCLE` clauses.

### VALUES and TABLE

- `VALUES (...), (...)` as a query or `FROM` item; columns are named
  `column1`, `column2`, ...; their types are unified across rows.
- `TABLE name` only works as the right operand of `UNION`, `INTERSECT` or
  `EXCEPT`; as a statement, in `FROM`, in a CTE or in a view it is a syntax
  error. Write `SELECT * FROM name`.

## Subqueries

All subquery forms are decorrelated into joins:

| Form | Notes |
|---|---|
| `(SELECT ...)` scalar | NULL if empty; more than one row fails with `more than one row returned by a subquery used as an expression` (SQLSTATE `21000`) — only for rows where the value is evaluated, as in PostgreSQL (a `CASE` branch or `COALESCE` argument that is not reached does not raise it) |
| `EXISTS (...)`, `NOT EXISTS (...)` | |
| `x IN (SELECT ...)`, `x NOT IN (SELECT ...)` | `NOT IN` with SQL's NULL semantics |
| `(a, b) IN (SELECT x, y ...)`, `(a, b) = ANY (SELECT ...)`, `(a, b) <> ALL (SELECT ...)` | row values |
| `x op ANY (SELECT ...)`, `x op ALL (SELECT ...)` | any comparison operator |
| `ARRAY(SELECT ...)` | in the subquery's `ORDER BY` order; empty array (not NULL) when there are no rows |

Subqueries may be correlated (reference columns of enclosing queries at any
depth) and may appear in the select list, `WHERE`, `HAVING` and inner join
conditions. Not supported: subqueries in outer-join conditions and in
`VALUES` rows, and aggregates whose arguments reference only columns of an
enclosing query (`SELECT (SELECT max(t1.a) FROM t2) FROM t1`: in PostgreSQL
the aggregate belongs to the outer query; here it fails with SQLSTATE
`0A000`).

## Aggregate functions

```sql
agg( [ DISTINCT ] args [ ORDER BY sort_expr, ... ] ) [ FILTER ( WHERE condition ) ]
agg( fraction ) WITHIN GROUP ( ORDER BY sort_expr )      -- ordered-set aggregates
```

| Function | Argument types | Result type |
|---|---|---|
| `count(*)`, `count(x)` | any | `bigint` |
| `sum(x)` | `smallint`, `integer` → `bigint`; `bigint`, `numeric` → `numeric`; `real` → `real`; `double precision` → `double precision` | |
| `avg(x)` | integers and `numeric` → `numeric`; floats → `double precision` | |
| `min(x)`, `max(x)` | any type with an ordering | same as argument |
| `bool_and(x)`, `every(x)`, `bool_or(x)` | `boolean` | `boolean` |
| `stddev(x)` = `stddev_samp`, `stddev_pop`, `variance(x)` = `var_samp`, `var_pop` | numeric types | `double precision` for floats, otherwise `numeric` |
| `string_agg(x, delimiter)` | `text` or `bytea` | same |
| `array_agg(x)` | any (arrays accumulate into one more dimension) | array |
| `json_agg(x)`, `jsonb_agg(x)` | any | `json`, `jsonb` |
| `json_object_agg(k, v)`, `jsonb_object_agg(k, v)` | key, any (NULL key fails) | `json`, `jsonb` |
| `percentile_cont(f) WITHIN GROUP (ORDER BY x)` | `x` cast to `double precision`; `f` a constant in [0, 1] | `double precision` |
| `percentile_disc(f) WITHIN GROUP (ORDER BY x)` | any orderable `x` | same as `x` |
| `mode() WITHIN GROUP (ORDER BY x)` | any orderable `x` | same as `x` |
| `any_value(x)` | any | same as argument |

- `DISTINCT`, `FILTER (WHERE ...)` and `ORDER BY` inside the call are
  supported for all aggregates. As in PostgreSQL, the arguments and the
  `ORDER BY` expressions are evaluated only for the rows that pass the
  `FILTER`, after it: `sum(10 / x) FILTER (WHERE x <> 0)` does not divide
  by zero, and an error of the filter itself is raised before any of its
  arguments'. With `DISTINCT`, the `ORDER BY` expressions
  must be arguments (`in an aggregate with DISTINCT, ORDER BY expressions
  must appear in argument list`, as in PostgreSQL). A materialized view with
  several different `DISTINCT` argument sets (or filters), `DISTINCT` and
  other aggregates together, or ordered aggregates with different orders is
  split into several aggregations joined on the group key.
- `string_agg`, `array_agg`, `json_agg`, `jsonb_agg`, `json_object_agg`,
  `jsonb_object_agg`, `mode` and the percentiles are *ordered* aggregates.
  Without an `ORDER BY`, the input is ordered by the argument values, so the
  result is deterministic (PostgreSQL uses an arbitrary input order).
- Floating-point `sum`, `avg`, `stddev*` and `var*` are computed exactly
  (the sum of the exact binary values) and rounded once at the end; the
  result is independent of the order of the inputs, and may differ from
  PostgreSQL's sequential summation in the last digits. A `sum` of
  `double precision` or `real` values that overflows its type fails with
  `value out of range: overflow` (infinite inputs give `Infinity`).
- `numeric` sums keep NaN and infinities apart from the finite sum (like
  PostgreSQL), use the largest display scale among the inputs, and are exact
  without numeric's range limit; only the result is checked. A `sum` beyond
  the range fails with `value overflows numeric format`, but an `avg` of
  such values succeeds when the average itself is in range (PostgreSQL
  fails). `var*` and `stddev*` of `bigint` are computed from exact `numeric`
  sums.
- Not supported: other aggregates (`bit_and`, `bit_or`, `regr_*`, `corr`,
  `covar_*`, `xmlagg`, `range_agg`, `json_objectagg` (SQL/JSON), ...), and
  user-defined aggregates.

## Set-returning functions

| Function | Output columns |
|---|---|
| `generate_series(start, stop [, step])` | `integer`, `bigint` or `numeric` (bounds and step take their common type: `generate_series(1, 2, 0.5)` is `numeric`); `timestamp` / `timestamptz` with an `interval` step (required); dates, and untyped bounds with an `interval` step, use the `timestamptz` variant (two untyped bounds without a step are ambiguous, as in PostgreSQL). Up to 100,000,000 rows. |
| `unnest(array)` | the element type; all elements of a multi-dimensional array. In `FROM`, `unnest(a, b, ...)` returns one column per array, zipped and padded with NULLs |
| `jsonb_array_elements(jsonb)`, `jsonb_array_elements_text(jsonb)` | `value` |
| `jsonb_each(jsonb)`, `jsonb_each_text(jsonb)` | `key`, `value` |
| `jsonb_object_keys(jsonb)` | `jsonb_object_keys` |
| `jsonb_path_query(target, path [, vars [, silent]])`, `jsonb_path_query_tz(...)` | `jsonb` (named arguments allowed) |
| `regexp_split_to_table(string, pattern [, flags])` | `text` |
| `regexp_matches(string, pattern [, flags])` | `text[]` (one row per match with flag `g`) |

In `FROM` they accept `WITH ORDINALITY` (an extra `bigint` column numbered
from 1) and column aliases. In the select list they must return a single
column; several set-returning functions in one select list run in lockstep,
the shorter results padded with NULLs (as in PostgreSQL 10+); they are not
allowed in `WHERE`, `GROUP BY` and other clauses. A NULL argument produces
no rows. Other set-returning functions (`json_each`,
`json_array_elements`, `generate_subscripts`, `jsonb_to_recordset`, ...) are
not available.

## Expressions

### Literals

| Syntax | Type |
|---|---|
| `42`, `-2147483648` | `integer`; `bigint` if it does not fit; `numeric` if it does not fit `bigint` |
| `1.5`, `1e3` | `numeric`; underscores are allowed in all numeric literals (`1_000` is an `integer`) |
| `'text'`, `E'tab\t'`, `U&'d\0061t'`, `$tag$...$tag$` | untyped string, resolved by context (like PostgreSQL's `unknown`) |
| `X'0a0b'` | `bytea`; a character that is not a hex digit fails (SQLSTATE `22P02`) |
| `TRUE`, `FALSE`, `NULL` | |
| `DATE '2024-01-01'`, `TIMESTAMP '...'`, `INTERVAL '1 day'`, `INTERVAL '1 day 2 hours' DAY`, `type 'string'` | typed constants; interval field qualifiers restrict the value (`1 day`), as in PostgreSQL |
| `$1`, `$2`, ... | parameters (extended protocol); see below |

Differences: `X'0a0b'` is a `bytea` constant (in PostgreSQL it is a bit
string); `0x1F` is parsed as a `bytea` hex string (`\x1f`), not as the
integer 31, and `0o17` / `0b101` parse as `0` with a column alias; bit-string
literals (`B'101'`) are not supported.

An untyped literal (`'...'`, `NULL`) keeps the type `unknown` while its
context can still give it a type; a column that is still untyped when a
query, subquery, CTE or view is complete becomes `text`, as in PostgreSQL
(`SELECT 'a'` is a `text` column; the branches of a set operation are unified
first).

Parameters: the types of `$n` are given by the client or inferred while the
statement is described (from the first context that coerces them: an
operator or function argument, an `IN` list, `LIMIT`, a cast). A parameter
whose type cannot be inferred is described as `text` and its value is bound
as an untyped literal (PostgreSQL fails with `could not determine data type
of parameter`); parameters of `INSERT`, `UPDATE` and `DELETE` are always
described as `text` (PostgreSQL infers the column types), and their values
are converted to the column types. A `$n` without a value — in a simple
query, or beyond the parameters of the prepared statement — fails with
`there is no parameter $n` (SQLSTATE `42P02`), and so does a parameter in a
`CREATE [MATERIALIZED] VIEW`. A `Bind` with the wrong number of values fails
with SQLSTATE `08P01`.

### Operators and predicates

- Arithmetic, comparison, string, pattern, JSON, array and date/time
  operators: see [Functions and operators](functions.md#operators).
- `AND`, `OR`, `NOT` with three-valued logic.
- `IS [NOT] NULL`, `IS [NOT] TRUE | FALSE | UNKNOWN`,
  `IS [NOT] DISTINCT FROM` (also between row constructors).
- `x [NOT] BETWEEN a AND b` (`BETWEEN SYMMETRIC` is not supported).
- `x [NOT] IN (list)`, `(a, b) [NOT] IN ((1, 2), ...)`.
- `[NOT] LIKE`, `[NOT] ILIKE` (with `ESCAPE`; also `~~`, `~~*`, `!~~`,
  `!~~*`), on `text` and `bytea`; `x [NOT] [I]LIKE ANY (array)`;
  `[NOT] SIMILAR TO` (constant pattern only); POSIX regular expressions `~`,
  `~*`, `!~`, `!~*`. `LIKE ALL`, `LIKE SOME` and the operator form
  `~~ ANY (...)` are not supported.
- Row comparisons `(a, b) = (c, d)`, `<>`, `<`, `<=`, `>`, `>=`
  (lexicographic). Standalone row values (`ROW(...)`, records) are not
  supported.
- `(start, end_or_length) OVERLAPS (start, end_or_length)`.
- `x op ANY (array)`, `x op ALL (array)` (`SOME` = `ANY`).

Expressions may be nested at most 1000 levels deep — each operator of a
chain such as `a + b + c ...` is one level, while `AND` and `OR` chains of
any length are flat — and an expression may have at most 50,000 operators
at one level of parentheses. Larger expressions fail with `stack depth limit
exceeded` (SQLSTATE `54001`), where PostgreSQL's limit depends on its
`max_stack_depth`.

### Conditional expressions

`CASE` (simple and searched), `COALESCE`, `NULLIF`, `GREATEST`, `LEAST`.
As in PostgreSQL, `CASE` evaluates only the branch it selects and `COALESCE`
stops at the first non-NULL argument. Constant folding follows PostgreSQL's
rules: branches that a constant condition makes unreachable are dropped
without being folded (`CASE WHEN true THEN 1 ELSE 1/0 END` and
`COALESCE(1, 1/0)` are 1), a strict function with a NULL constant argument
is NULL, and constant subexpressions that remain are evaluated at plan time
(so a reachable `1/0` fails even when no row reaches it, also in
PostgreSQL). Strict functions evaluate all their arguments before checking
for NULLs, so an error in any argument is raised. `NULLIF(a, b)` returns
`a` with `a`'s type (`NULLIF(date_col, timestamp_value)` is a `date`).

### Special syntax

| Syntax | Equivalent |
|---|---|
| `CAST(x AS type)`, `x::type`, `type 'literal'` | casts ([Types](types.md#casts)); `TRY_CAST` not supported |
| `x AT TIME ZONE zone` | `timezone(zone, x)` (a `date` is taken as `timestamptz`, midnight UTC, as in PostgreSQL) |
| `EXTRACT(field FROM x)` | `extract(text, x)` (returns `numeric`) |
| `SUBSTRING(s FROM a FOR b)`, `SUBSTRING(s FROM pattern)` | `substring(...)` |
| `POSITION(a IN b)` | `strpos(b, a)` |
| `TRIM([LEADING | TRAILING | BOTH] chars FROM s)`, `TRIM(s)` | `ltrim` / `rtrim` / `btrim` (with `FROM`, `chars` is required) |
| `OVERLAY(s PLACING r FROM a [FOR b])` | `overlay(...)` |
| `CEIL(x)`, `FLOOR(x)` | (field forms are not supported) |
| `x COLLATE "C"` | only `C`, `default`, `ucs_basic`, `pg_c_utf8`, `C.UTF-8` (all mean code-point order); `x::type COLLATE "C"` is a parse error, write `(x::type) COLLATE "C"` |
| `current_date`, `current_timestamp`, `localtimestamp` | the clock ([Temporal filters](../user-guide/temporal-filters-and-errors.md)) |
| `current_schema` | `public` |
| `current_user`, `session_user`, `user`, `current_role`, `current_catalog` | `ivm` |

Not supported, although PostgreSQL has them: `current_time`, `localtime`,
`current_timestamp(p)`; `TRIM(LEADING FROM s)` and `TRIM(FROM s)` without
characters; `SUBSTRING(s SIMILAR pattern ESCAPE e)` (and
`SUBSTRING(s FROM pattern FOR escape)` is taken as positions); `U&'...'`
with `UESCAPE`; string literals continued across lines (`'foo'` newline
`'bar'`, which PostgreSQL concatenates): in a select list the second literal
silently becomes a column alias (the result is `foo` in a column named
`bar`), elsewhere it is a syntax error.

### Arrays

- `ARRAY[1, 2]`, `ARRAY[[1, 2], [3, 4]]` (multi-dimensional, up to 6
  dimensions), `ARRAY(SELECT ...)`, `'{1,2}'::int[]`, `'[0:1]={1,2}'`
  (explicit lower bounds).
- Subscripts `a[i]`, `a[i][j]` (NULL when out of range or when the number of
  subscripts differs from the number of dimensions), slices `a[lo:hi]`,
  `a[:hi]`, `a[lo:]`, `a[i][lo:hi]` (in a slice, a plain subscript `n`
  means `1:n`). Slices with a stride are not supported.
- Arrays compare element by element, then by number of elements,
  dimensions, lengths and lower bounds (PostgreSQL's ordering).

### JSON

- `jsonb` subscripts `j['key']`, `j[0]` (read-only).
- Operators `->`, `->>`, `#>`, `#>>`, `@>`, `<@`, `?`, `?|`, `?&`, `||`,
  `-`, `#-`, `@?`, `@@`; functions in [Functions](functions.md#json-and-jsonb).

## Type resolution

Function and operator calls are resolved like PostgreSQL's: candidates of
the right name and arity are matched with implicit casts, untyped literals
are resolved from context (preferring `text`, then the preferred type of
their category), exact and preferred-type matches win. See
[Types](types.md#type-resolution).
