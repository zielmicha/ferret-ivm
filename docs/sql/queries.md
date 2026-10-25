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
- Output column names mostly follow PostgreSQL's rules (`count`,
  `?column?`, `case`, the function or column name, the type name for casts
  of literals). Differences: `SELECT true` is named `bool` (PostgreSQL:
  `?column?`); a nested cast is named after the innermost type
  (`2.5::float8::int` → `float8`, PostgreSQL: `int4`); a literal cast to an
  array type, subscripts (`a[1]`, `j['k']`), `OVERLAPS` and `COLLATE` give
  `?column?` (PostgreSQL: the type, column or operator name); `substr(...)` is
  named `substring` and `trim(LEADING|TRAILING ...)` `btrim`; and the column
  of `jsonb_array_elements_text(...) t` is named `t` (PostgreSQL: `value`).
- Set-returning functions in the select list (`SELECT id, unnest(a) FROM t`)
  — see [Set-returning functions](#set-returning-functions).

### DISTINCT and DISTINCT ON

- `SELECT DISTINCT` removes duplicate rows.
- `SELECT DISTINCT ON (expr, ...)` keeps the first row of each group in
  `ORDER BY` order. The `DISTINCT ON` expressions must be output columns
  (by name or position), and — unlike PostgreSQL — all `ORDER BY`
  expressions must appear in the select list.

### FROM

| Item | Notes |
|---|---|
| `table`, `materialized_view`, `view` `[ [AS] alias [ (column, ...) ] ]` | virtual views are expanded inline (up to 32 levels deep) |
| `schema.name` | see [table names](../user-guide/sources.md#table-names); `pg_catalog.*`, `information_schema.*`, `ivm.*` system relations |
| `( query ) [AS] alias [ (column, ...) ]` | derived table |
| `LATERAL ( query ) alias` | may reference items to its left |
| `function(args) [WITH ORDINALITY] [ [AS] alias [ (column, ...) ] ]` | [set-returning functions](#set-returning-functions); arguments may reference items to the left (implicitly lateral) |
| `UNNEST(array) [WITH ORDINALITY]` | one array only |
| `a [INNER] JOIN b ON ...`, `LEFT`, `RIGHT`, `FULL [OUTER] JOIN`, `CROSS JOIN`, `,` | all join kinds |
| `JOIN ... USING (columns)`, `NATURAL JOIN` | merged columns as in PostgreSQL (`COALESCE` for `FULL`) |
| `a JOIN LATERAL (...) l ON ...`, `LEFT JOIN LATERAL (...) l ON true` | only inner and left lateral joins; a left lateral join needs `ON true` |
| `( a JOIN b ... ) [AS alias]` | parenthesised joins |

Not supported: `ROWS FROM (...)`, `UNNEST` with several arrays, table
versions (`FOR SYSTEM_TIME`; use `AS OF`). `TABLESAMPLE` is accepted and
**ignored** (all rows are returned).

### WHERE, GROUP BY, HAVING

- `WHERE` takes any boolean expression, including subqueries.
- `GROUP BY` items may be expressions, output column positions (`GROUP BY 1`)
  or output column aliases (when no input column has that name).
- `GROUPING SETS ( ... )`, `ROLLUP ( ... )`, `CUBE ( ... )` and their
  combinations (cross product); `grouping(expr, ...)` (fewer than 32
  arguments). `CUBE` is limited to 12 elements, the expansion to 4096
  grouping sets.
- Functional dependencies: columns of a table whose primary key is grouped
  may be used without being grouped.
- `HAVING` without `GROUP BY` aggregates over all rows.
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
  `nth_value(x, n)`, and every [aggregate](#aggregate-functions) except with
  `DISTINCT` or `FILTER`.
- Default frame: `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.
- `ROWS` and `GROUPS` offsets are integer constants. `RANGE` offsets need
  exactly one `ORDER BY` column of a numeric type (offset of the same type)
  or of a date/time type (`interval` offset); `date` values are compared as
  timestamps. Negative integer and `numeric` offsets are not rejected (a
  known problem; PostgreSQL fails with SQLSTATE `22013`): a negative `ROWS`
  offset acts like `UNBOUNDED`, a negative `RANGE` offset gives wrong
  results, and a negative `GROUPS` offset crashes the engine thread.
- Named windows follow PostgreSQL's copy rules (a referencing window may add
  `ORDER BY` and a frame, but not `PARTITION BY`, and cannot copy a window
  that has a frame).
- Window functions may take aggregates as arguments (`rank() OVER (ORDER BY
  sum(x))`).
- Not supported: frame `EXCLUDE` (syntax error), `DISTINCT` and `FILTER` in
  window aggregates. `IGNORE NULLS` / `RESPECT NULLS` are accepted and
  ignored.

### Set operations

`UNION`, `INTERSECT` and `EXCEPT`, each with `ALL` or `DISTINCT` (default),
with PostgreSQL's multiset semantics (`INTERSECT ALL` keeps
`min(m, n)` copies, `EXCEPT ALL` keeps `m − n`). Column types are unified
like PostgreSQL's `UNION` type resolution ([common types](types.md#common-type-resolution)).

### ORDER BY, LIMIT, OFFSET, FETCH

- `ORDER BY` items: output column names or positions, or expressions over
  the input (including columns not in the select list, except with
  `DISTINCT`). `ASC`/`DESC`, `NULLS FIRST`/`NULLS LAST`; the default is
  `NULLS LAST` for `ASC` and `NULLS FIRST` for `DESC`. Rows that are equal
  on all sort keys are ordered by a deterministic internal tie-break (their
  encoded bytes), so the output order is repeatable.
- `LIMIT n`, `LIMIT ALL`, `LIMIT NULL`, `OFFSET n [ROWS]`,
  `FETCH FIRST|NEXT [n] ROW|ROWS ONLY|WITH TIES` (`WITH TIES` needs
  `ORDER BY`). Counts must be constants (`LIMIT` and `OFFSET` must not be
  negative). `OFFSET` cannot be combined with `FETCH`
  (`cannot use both LIMIT and FETCH`; PostgreSQL accepts it).
- `ORDER BY` in a subquery or view without `LIMIT`/`OFFSET` does not affect
  the result (relations have no order); with them it selects the rows
  (top-k).
- Not supported: `FETCH ... PERCENT`, `ORDER BY ALL`, `LIMIT BY`,
  `FOR UPDATE`/`FOR SHARE`.

### WITH (common table expressions)

- `WITH name [ (columns) ] AS ( query ) [ , ... ]` — CTEs are inlined
  wherever they are referenced; a CTE name shadows a catalog object.
- `WITH RECURSIVE name [ (columns) ] AS ( non_recursive_term UNION [ ALL ] recursive_term )`
  — evaluated by semi-naive iteration (at most 1,000,000 iterations).
  A CTE counts as recursive if its query mentions its own name. Correlated
  recursive queries are not supported.

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
| `(SELECT ...)` scalar | NULL if empty; more than one row fails with `more than one row returned by a subquery used as an expression` (SQLSTATE `21000`) |
| `EXISTS (...)`, `NOT EXISTS (...)` | |
| `x IN (SELECT ...)`, `x NOT IN (SELECT ...)` | `NOT IN` with SQL's NULL semantics |
| `(a, b) IN (SELECT x, y ...)` | row values |
| `x op ANY (SELECT ...)`, `x op ALL (SELECT ...)` | any comparison operator |
| `ARRAY(SELECT ...)` | in the subquery's `ORDER BY` order; empty array (not NULL) when there are no rows |

Subqueries may be correlated (reference columns of enclosing queries at any
depth) and may appear in the select list, `WHERE`, `HAVING` and inner join
conditions. Not supported: subqueries in outer-join conditions and in
`VALUES` rows.

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
  supported for all aggregates (a materialized view with several different
  `DISTINCT` argument sets or orders is split into several aggregations
  joined on the group key).
- `string_agg`, `array_agg`, `json_agg`, `jsonb_agg`, `json_object_agg`,
  `jsonb_object_agg`, `mode` and the percentiles are *ordered* aggregates.
  Without an `ORDER BY`, the input is ordered by the argument values, so the
  result is deterministic (PostgreSQL uses an arbitrary input order).
- Floating-point `sum`, `avg`, `stddev*` and `var*` are computed exactly
  (the sum of the exact binary values) and rounded once at the end; the
  result is independent of the order of the inputs, and may differ from
  PostgreSQL's sequential summation in the last digits. `sum` of
  `double precision` values that overflows fails with
  `value out of range: overflow`; `sum` of `real` values returns `Infinity`
  instead (PostgreSQL fails).
- `numeric` sums keep NaN and infinities apart from the finite sum (like
  PostgreSQL) and use the largest display scale among the inputs.
- Not supported: other aggregates (`bit_and`, `bit_or`, `regr_*`, `corr`,
  `covar_*`, `xmlagg`, `range_agg`, `json_objectagg` (SQL/JSON), ...), and
  user-defined aggregates.

## Set-returning functions

| Function | Output columns |
|---|---|
| `generate_series(start, stop [, step])` | `integer`, `bigint` or `numeric` (by argument type); `timestamp` / `timestamptz` with an `interval` step (required); dates use the `timestamptz` variant. Up to 100,000,000 rows. |
| `unnest(array)` | the element type; all elements of a multi-dimensional array |
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
| `X'0a0b'` | `bytea` |
| `TRUE`, `FALSE`, `NULL` | |
| `DATE '2024-01-01'`, `TIMESTAMP '...'`, `INTERVAL '1 day'`, `INTERVAL '5' DAY`, `type 'string'` | typed constants |
| `$1`, `$2`, ... | parameters (extended protocol); types are inferred from context or given by the client |

Differences: `X'0a0b'` is a `bytea` constant (in PostgreSQL it is a bit
string); `0x1F` is parsed as a `bytea` hex string (`\x1f`), not as the
integer 31, and `0o17` / `0b101` parse as `0` with a column alias; bit-string
literals (`B'101'`) are not supported. Untyped literals that nothing gives a
type keep the type `unknown` (OID 705) in results, subqueries and views,
where PostgreSQL resolves them to `text`: `SELECT 'a'` reports type
`unknown`, and `SELECT v = 1 FROM (SELECT '1' AS v) s` is true where
PostgreSQL fails with `operator does not exist: text = integer` (only
`VALUES` and set operations resolve them).

### Operators and predicates

- Arithmetic, comparison, string, pattern, JSON, array and date/time
  operators: see [Functions and operators](functions.md#operators).
- `AND`, `OR`, `NOT` with three-valued logic.
- `IS [NOT] NULL`, `IS [NOT] TRUE | FALSE | UNKNOWN`,
  `IS [NOT] DISTINCT FROM` (also between row constructors).
- `x [NOT] BETWEEN a AND b` (`BETWEEN SYMMETRIC` is not supported).
- `x [NOT] IN (list)`, `(a, b) [NOT] IN ((1, 2), ...)`.
- `[NOT] LIKE`, `[NOT] ILIKE` (with `ESCAPE`; also `~~`, `~~*`, `!~~`,
  `!~~*`), on `text` and `bytea`; `[NOT] SIMILAR TO` (constant pattern
  only); POSIX regular expressions `~`, `~*`, `!~`, `!~*`. `LIKE ANY` is not
  supported.
- Row comparisons `(a, b) = (c, d)`, `<>`, `<`, `<=`, `>`, `>=`
  (lexicographic). Standalone row values (`ROW(...)`, records) are not
  supported.
- `(start, end_or_length) OVERLAPS (start, end_or_length)`.
- `x op ANY (array)`, `x op ALL (array)`.

### Conditional expressions

`CASE` (simple and searched), `COALESCE`, `NULLIF`, `GREATEST`, `LEAST`.
As in PostgreSQL, `CASE` evaluates only the branch it selects and `COALESCE`
stops at the first non-NULL argument — at run time. Constant folding,
however, evaluates every constant subexpression first, so
`CASE WHEN true THEN 1 ELSE 1/0 END` and `COALESCE(1, 1/0)` fail with
`division by zero` where PostgreSQL returns 1.

### Special syntax

| Syntax | Equivalent |
|---|---|
| `CAST(x AS type)`, `x::type`, `type 'literal'` | casts ([Types](types.md#casts)); `TRY_CAST` not supported |
| `x AT TIME ZONE zone` | `timezone(zone, x)` |
| `EXTRACT(field FROM x)` | `extract(text, x)` (returns `numeric`) |
| `SUBSTRING(s FROM a FOR b)`, `SUBSTRING(s FROM pattern)` | `substring(...)` |
| `POSITION(a IN b)` | `strpos(b, a)` |
| `TRIM([LEADING | TRAILING | BOTH] chars FROM s)`, `TRIM(s)` | `ltrim` / `rtrim` / `btrim` (with `FROM`, `chars` is required) |
| `OVERLAY(s PLACING r FROM a [FOR b])` | `overlay(...)` |
| `CEIL(x)`, `FLOOR(x)` | (field forms are not supported) |
| `x COLLATE "C"` | only `C`, `default`, `ucs_basic`, `pg_c_utf8`, `C.UTF-8` (all mean code-point order); `x::type COLLATE "C"` is a parse error, write `(x::type) COLLATE "C"` |
| `current_date`, `current_timestamp`, `localtimestamp` | the clock ([Temporal filters](../user-guide/temporal-filters-and-errors.md)) |
| `current_schema`, `current_role` | `public`, `ivm` |

`current_user`, `session_user`, `user` and `current_catalog` currently fail
(`function current_user() does not exist` and so on).

Not supported, although PostgreSQL has them: `current_time`, `localtime`,
`current_timestamp(p)`; `TRIM(LEADING FROM s)` and `TRIM(FROM s)` without
characters; `SUBSTRING(s SIMILAR pattern ESCAPE e)` (and
`SUBSTRING(s FROM pattern FOR escape)` is taken as positions); `U&'...'`
with `UESCAPE`; string literals continued across lines.

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
are resolved from context (preferring `text`), exact and preferred-type
matches win. See [Types](types.md#type-resolution).
