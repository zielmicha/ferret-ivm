# Tables, views and indexes

The engine has six kinds of catalog objects — tables, views, materialized
views, indexes, sources and sinks — in **one namespace**: two objects of any
kinds cannot have the same name. There are no schemas; everything appears in
schema `public` to client tools.

Every DDL statement is written to the input log and applied when the log
record is durable; replay applies it again identically. `CREATE TABLE`,
`CREATE [MATERIALIZED] VIEW`, `CREATE SOURCE` and `ALTER SOURCE` are first
validated against the current catalog, so their usual errors are reported
before anything is logged; `DROP`, `CREATE INDEX` and `CREATE SINK` are only
checked when the logged record is applied (a failing one fails again,
harmlessly, on every replay). Code: `crates/ivm/src/coord/ddl.rs` (DDL),
`crates/ivm/src/coord/dml.rs` (DML), `crates/ivm/src/catalog.rs`.

## Tables

Two kinds of tables exist:

- **replicated tables**, created by a [source](sources.md) for every
  published upstream table; they are read-only in the engine;
- **local tables**, created with `CREATE TABLE` and written with `INSERT`,
  `UPDATE` and `DELETE`. Their changes go through the same input log and the
  same dataflow as replicated changes.

A table with a primary key stores one row per key. A table without a primary
key is a **multiset**: duplicate rows are allowed and counted (its index is
keyed by all columns).

### CREATE TABLE

```sql
CREATE TABLE [ IF NOT EXISTS ] name (
    column type [ column_constraint ... ] [ , ... ]
    [ , PRIMARY KEY ( column [ , ... ] ) ]
    [ , other table constraints ]
)
```

- Column types: see [Data types](../sql/types.md). `serial`, `serial4`,
  `bigserial` and `serial8` are accepted as aliases of `integer`/`bigint`, but
  **no sequence or default is created** and no `NOT NULL` is implied;
  `smallserial` is not accepted.
- `PRIMARY KEY` (on a column or as a table constraint, possibly multi-column)
  is enforced on `INSERT` and `UPDATE` and makes its columns `NOT NULL`.
  Only one `PRIMARY KEY` table constraint is allowed; several column-level
  `PRIMARY KEY` markers are merged into one composite key (PostgreSQL rejects
  this).
- `NOT NULL` is enforced.
- `UNIQUE`, `CHECK`, `DEFAULT`, `REFERENCES` / `FOREIGN KEY` and other table
  constraints are **accepted and ignored**: they are not enforced and
  defaults are not applied (an omitted column is NULL).
- `IF NOT EXISTS` succeeds without doing anything if *any* object with that
  name exists.
- `TEMP`/`TEMPORARY` is ignored (the table is an ordinary, durable table).
- Not supported: `CREATE TABLE ... AS`, `ALTER TABLE` (any form), and
  `TRUNCATE` (use `DELETE FROM t`).

The statement is stored in the catalog in the canonical form printed by the
SQL parser (for example `CREATE TABLE t (id INT PRIMARY KEY, v TEXT)`), which
is what `SHOW CREATE VIEW` and `ivm.catalog_history` show.

### INSERT

```sql
INSERT INTO table [ ( column [ , ... ] ) ]
    { VALUES ( expr | DEFAULT [ , ... ] ) [ , ... ] | query }
    [ ON CONFLICT [ conflict_target ] DO NOTHING ]
```

- Columns that are not listed, and the keyword `DEFAULT`, give NULL (column
  defaults are not stored). Without a column list, every row must supply all
  columns (`INSERT has more target columns than expressions` otherwise;
  PostgreSQL fills missing trailing columns with their defaults).
- `VALUES` expressions are evaluated once and converted to the column types:
  untyped literals are parsed as the column type; typed values are cast to
  it. Any cast that exists is used, including ones PostgreSQL only allows
  explicitly (for example a `text` expression into an `integer` column),
  so values that do not convert fail at run time
  (`invalid input syntax for type integer: "abc"`).
- `INSERT ... SELECT` evaluates the query against the current state.
- A duplicate primary key fails with SQLSTATE `23505`,
  `duplicate key value violates unique constraint "<table>_pkey"` and a
  `DETAIL` naming the key — unless `ON CONFLICT ... DO NOTHING` is given, in
  which case conflicting rows (with stored rows or earlier rows of the same
  statement) are skipped. The conflict target is not checked; every
  primary-key conflict counts.
- `ON CONFLICT ... DO UPDATE` and `RETURNING` are not supported.
- Command tag: `INSERT 0 <n>`.

### UPDATE

```sql
UPDATE table [ [ AS ] alias ] SET column = expr [ , ... ] [ WHERE condition ]
```

- `SET` expressions are coerced to the column types with assignment casts.
- The primary key may be changed; the new key must not collide with a stored
  row (unless that row's key is vacated by the same statement) or with
  another updated row.
- Not supported: `UPDATE ... FROM`, `RETURNING`, multi-column assignment
  `SET (a, b) = (...)`.
- Command tag: `UPDATE <n>`.

### DELETE

```sql
DELETE FROM table [ [ AS ] alias ] [ WHERE condition ]
```

`USING` and `RETURNING` are not supported. Command tag: `DELETE <n>`.

### Semantics of DML

- `WHERE` clauses and `SET` expressions may use any expression, including
  subqueries (they are decorrelated like queries).
- A statement is evaluated against the current state, and the resulting row
  changes — not the SQL — are logged as one commit with one GSN. The
  statement is therefore atomic, and replay does not re-evaluate it.
- `now()` and related functions in DML return the **wall-clock time when the
  statement runs**, not the logical clock used by reads; the computed values
  are logged.
- DML statements are serialized: one is in flight at a time, and the client
  gets its reply after the change has been applied, so a following read sees
  it.
- Replicated tables reject DML
  (`cannot insert into source table "t": it is replicated from its source`,
  SQLSTATE `42501`); views reject it as well (`cannot insert into view`).
- There are no transactions: `BEGIN` ... `ROLLBACK` does not undo statements
  (see [Reads](reads.md#transactions)).

## Views and materialized views

```sql
CREATE [ OR REPLACE ] [ MATERIALIZED ] VIEW [ IF NOT EXISTS ] name
    [ ( column [ , ... ] ) ] AS query
```

| | `CREATE VIEW` (virtual) | `CREATE MATERIALIZED VIEW` (maintained) |
|---|---|---|
| Stored | the query text | the query text, a dataflow and an index of the results |
| Work on change | none | incremental maintenance of the dataflow at every GSN that touches its inputs |
| Read | the query is expanded inline and evaluated against current (or historical) state | served from the view's index |
| `SUBSCRIBE` | yes (a private dataflow is built for the subscription) | yes |
| `CREATE INDEX` on it, sink from it | no | yes |
| `now()`, `random()`, other non-deterministic functions | allowed | only `now()` family in [temporal filters](temporal-filters-and-errors.md); see [Maintained views](../sql/maintained-views.md) |
| Expression errors | the query fails | become [error records](temporal-filters-and-errors.md#errors-as-data) that make reads fail |

Both kinds accept the same query language ([Queries](../sql/queries.md)).
Column names come from the select list or from the optional column list
(extra names in the column list are silently ignored; PostgreSQL rejects
them); column types are derived from the query.

### Creating a materialized view

`CREATE MATERIALIZED VIEW` plans the query, lowers it into dataflow
operators, reuses every operator and index that an existing view already has
(identical sub-plans are shared), and **hydrates** the new parts at the
statement's GSN by reading existing indexes — the upstream is never
contacted. Maintenance continues from the next GSN. The statement returns
after hydration, so a large view takes as long as one scan of its inputs.

Errors found while binding and planning the query (unknown columns,
unsupported constructs, non-deterministic functions) are reported before the
statement is logged. Problems found later, while lowering or hydrating, are
reported after it was logged; the statement then fails again, harmlessly, on
every replay — but a crash at that stage recurs on every start (see
[Known problems](troubleshooting.md#known-problems)).

A materialized view may read other materialized views; it then consumes the
other view's maintained output rather than re-planning its query.

### CREATE OR REPLACE

- Replacing a materialized view with a materialized view builds the new
  version (sharing unchanged fragments), hydrates it, and switches readers,
  subscribers and sinks to it at the statement's GSN. Subscribers and sinks
  receive the difference between the old and the new contents as one change
  at that GSN. Two kinds of dependents are **not** switched:
  - materialized views defined over the replaced view keep reading the old
    version until the next restart, when they are re-planned against the new
    one;
  - indexes on the replaced view keep indexing the old version; the next
    start rebuilds them by column position on the new version, and if it has
    fewer columns than an index refers to, the engine panics at start-up.

  Drop such views and indexes before replacing, and re-create them after.
- In every other combination (a virtual view replaced by a materialized one,
  or vice versa, or virtual by virtual) the old object is dropped — without
  checking or dropping dependent objects — and the new one is created under
  the same object id. Subscriptions to a replaced materialized view end with
  `materialized view "x" was dropped`. If a materialized view that has an
  index or a sink is replaced by a virtual view, the sink stops receiving
  changes (although `SHOW SINKS` still says `running`) and the next start
  fails with `index target without dataflow` or
  `sink source without dataflow`.
- `IF NOT EXISTS` succeeds without doing anything if any object with that
  name exists. Without `OR REPLACE` or `IF NOT EXISTS`, an existing name
  fails with `relation "x" already exists` (SQLSTATE `42P07`).

## Indexes

```sql
CREATE INDEX [ IF NOT EXISTS ] [ name ] ON relation ( column [ , ... ] )
```

An index is an additional *arrangement* (sorted, persistent copy) of a table
or materialized view keyed by the given columns. It is built by scanning the
relation's existing index at the statement's GSN and maintained from then on.

- `relation` must be a table or a materialized view.
- Only plain column names are allowed (`only column indexes are supported`
  otherwise). Sort direction, `NULLS FIRST/LAST`, operator classes,
  `UNIQUE`, `USING`, `INCLUDE` and `WHERE` are accepted and ignored: the key
  is always ascending, unique indexes are not enforced, partial indexes are
  full.
- The default name is `<relation>_<col1>_<col2>..._idx`.

What indexes are used for:

- **Point lookups in reads**: a query whose `WHERE` contains
  `column = constant` conditions on a table or materialized view reads only
  the matching key range of the index with the longest matching key prefix,
  instead of scanning (the constant must have exactly the column's type).
  The primary index of a table already serves primary-key lookups.
- **Sharing with maintained views**: arrangements are identified by their
  structure (the relation and the key). A materialized view whose join needs
  lookups on `t(c)` uses the existing index on `t(c)` instead of building
  another one, and vice versa.

Indexes do not make joins in maintained views possible or impossible — the
planner creates the arrangements it needs — they only avoid duplicates and
speed up reads.

## DROP

```sql
DROP { TABLE | VIEW | MATERIALIZED VIEW | INDEX } [ IF EXISTS ] name [ , ... ] [ CASCADE | RESTRICT ]
DROP SOURCE [ IF EXISTS ] name [ CASCADE ]   -- see Sources
DROP SINK [ IF EXISTS ] name                 -- see Sinks
```

- `DROP VIEW` also drops materialized views; `DROP MATERIALIZED VIEW` only
  materialized ones; the object kind must otherwise match
  (`"x" is not a table`).
- Replicated tables cannot be dropped (`drop the source instead`).
- Dependent objects — views and materialized views whose query mentions the
  object's name, sinks from it — make the drop fail with SQLSTATE `2BP01`
  unless `CASCADE` is given, which drops them (recursively) first. Indexes
  on a dropped table or materialized view are dropped with it, as in
  PostgreSQL.
- A statement naming several objects is not atomic: they are dropped one
  after another, and an error (for example `table "nosuch" does not exist`)
  leaves the earlier ones dropped. Such a statement is also missing from
  `ivm.catalog_history`.
- Subscriptions to a dropped materialized view end with
  `materialized view "x" was dropped`.
- Dropping releases the object's dataflow nodes and indexes unless other
  objects still share them; storage is reclaimed by
  [garbage collection](operations.md#checkpoints-and-garbage-collection).

Dependency detection is textual: an object depends on `x` when its stored
query, lowercased, contains the word `x`. For ordinary lowercase names this
errs on the safe side — but too far: a view with a column alias or a string
literal equal to the name also counts, and `CASCADE` drops it too. It
**misses quoted names with upper-case letters**: after
`CREATE TABLE "MyTable" ...` and a view over it, `DROP TABLE "MyTable"`
succeeds without `CASCADE`, and the engine then fails to start after the
next checkpoint (`relation "MyTable" does not exist`). Avoid mixed-case
quoted names, or drop dependent views first. See
[Troubleshooting](troubleshooting.md#known-problems).

## Object ids

Catalog objects get consecutive ids starting at 1000 (ids are never reused).
Client tools see them as OIDs `16384 + id` (`ivm.objects.oid`,
`pg_class.oid`). The id also appears in default slot names
([Sources](sources.md#slots)).
