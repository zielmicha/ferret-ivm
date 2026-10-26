# Tables, views and indexes

The engine has six kinds of catalog objects — tables, views, materialized
views, indexes, sources and sinks — in **one namespace**: two objects of any
kinds cannot have the same name. There are no schemas; everything appears in
schema `public` to client tools.

Every DDL statement is written to the input log and applied when the log
record is durable; replay applies it again identically. `CREATE TABLE`,
`CREATE [MATERIALIZED] VIEW`, `CREATE SOURCE`, `ALTER SOURCE ... REFRESH`
and `CREATE SINK` are first validated against the current catalog (for
views: binding, planning, the determinism check and the column list), so
their usual errors are reported before anything is logged. Other checks
happen only when the logged record is applied: those of `DROP` and
`CREATE INDEX`, the dependency checks of `CREATE OR REPLACE`, and the
lowering and hydration of a materialized view. A statement that fails there
stays in the log and fails again, harmlessly, on every replay; only
successful statements appear in `ivm.catalog_history`. Code:
`crates/ivm/src/coord/ddl.rs` (DDL), `crates/ivm/src/coord/dml.rs` (DML),
`crates/ivm/src/catalog.rs`.

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
  is enforced on `INSERT` and `UPDATE` and makes its columns `NOT NULL`. A
  table has at most one: a second one (two column-level `PRIMARY KEY`s, or
  one and a table constraint) fails with SQLSTATE `42P16`
  (`multiple primary keys for table "t" are not allowed`), and a column named
  twice in the key, like a column defined twice, with `42701`.
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
- Type modifiers are enforced as on assignment in PostgreSQL: a string too
  long for `varchar(n)` or `character(n)`, or a number that does not fit
  `numeric(p,s)`, fails (an explicit cast in the expression, such as
  `'abcdef'::varchar(3)`, truncates first); array columns apply the modifier
  to every element.
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
- DML statements from different sessions are logged together (sharing the
  log's appends and `fsync`s) unless one read rows another, logged before
  it, writes: it then waits for that one to be applied and is evaluated
  again. The outcome is that of running the statements one after another in
  the order they were logged. The client gets its reply after the change
  has been applied, so a following read sees it.
- A statement planned while a `DROP` of its table was already logged (but
  not yet applied) applies nothing and fails with SQLSTATE `42P01`
  (`the table of this statement was dropped before it could be applied`).
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
Column names come from the select list or from the optional column list,
which renames the leading columns; a column list longer than the select
list fails with SQLSTATE `42601` (`CREATE VIEW specifies more column names
than columns`, for materialized views `too many column names were
specified`). Duplicate column names are accepted (PostgreSQL rejects them).
Column types are derived from the query; an untyped literal column (such as
`SELECT 'abc' AS s`) is `text`.

### Creating a materialized view

`CREATE MATERIALIZED VIEW` plans the query, lowers it into dataflow
operators, reuses every operator and index that an existing view already has
(identical sub-plans are shared), and **hydrates** the new parts at the
statement's GSN by reading existing indexes — the upstream is never
contacted. Hydration runs on a thread of its own, on a version of the
indexes as of that GSN, and then catches up with the changes applied since;
meanwhile the engine goes on maintaining the other views, applying changes
and answering other statements. The statement returns once the view is
maintained like the others, so a large view takes about as long as one scan
of its inputs. Until then the view is listed (with no rows) and statements
reading it wait for it. Views and indexes created meanwhile are hydrated
after it (their statements return then); `CREATE OR REPLACE` of an
existing view and `CREATE SINK` logged meanwhile wait for it, and so do the
changes logged after them. `CREATE INDEX` works the same way.

Errors found while binding and planning the query (unknown columns,
unsupported constructs, non-deterministic functions, a column list that is
too long, a [broken](operations.md#recovery) materialized view it reads) are
reported before the statement is logged. Problems found only while lowering
or hydrating are reported after it was logged; the statement then fails
again, harmlessly, on every replay.

A materialized view may read other materialized views; it then consumes the
other view's maintained output rather than re-planning its query.

### CREATE OR REPLACE

`CREATE OR REPLACE [MATERIALIZED] VIEW` replaces a view or materialized view
of that name (any other kind of object fails with `"x" is not a view`,
SQLSTATE `42809`); the object keeps its id. When the logged statement is
applied, the replacement is first checked against what depends on the view,
and rejected with SQLSTATE `2BP01` (`cannot replace view "x": ...`),
changing nothing, if

- an index on the view would lose a key column: indexes follow their
  columns by name, so the new definition must still have them (a virtual
  view has none to index);
- a sink reads the view and the replacement changes its columns (names,
  types or type modifiers) or is a virtual view: the sink's target table has
  the view's columns;
- another materialized view would no longer plan over the new definition,
  or would no longer be deterministic (for example it uses a column the
  replacement drops, or reads it through a virtual view that now calls
  `random()`).

Otherwise:

- **Materialized by materialized**: the new version is built (sharing
  unchanged fragments with the old one and with other views), hydrated, and
  swapped in at the statement's GSN; the view's indexes move to it, re-keyed
  to the positions of their columns. If the columns (names, types, type
  modifiers) are unchanged, subscriptions to the view and sinks from it
  receive the difference between the old and the new contents as one change
  at that GSN; otherwise subscriptions to the view end with
  `materialized view "x" changed its columns` (SQLSTATE `55000`).
- **Every other combination** (virtual by materialized, materialized by
  virtual, virtual by virtual): the old object is dropped and the new one is
  created under the same id. Subscriptions reading the old object end as for
  a [`DROP`](#drop) (`view "x" was dropped`, `materialized view "x" was
  dropped`).
- In both cases the materialized views that read the replaced view —
  directly, through other materialized views, or through virtual views they
  expand — are re-planned at the same GSN, in dependency order, so that they
  read the new definition, as a restart would plan them.
- A subscription to a query (`SUBSCRIBE (query)`) that reads the replaced
  materialized view — directly, through virtual views or through other
  materialized views — has a dataflow built over the old version: it ends
  with `materialized view "x" was replaced` (SQLSTATE `55000`); subscribe
  again to read the new one.
- Replacing a [broken](operations.md#recovery) materialized view with a
  definition that plans repairs it.

`IF NOT EXISTS` succeeds without doing anything if any object with that name
exists. Without `OR REPLACE` or `IF NOT EXISTS`, an existing name fails with
`relation "x" already exists` (SQLSTATE `42P07`).

## Indexes

```sql
CREATE INDEX [ IF NOT EXISTS ] [ name ] ON relation ( column [ , ... ] )
```

An index is an additional *arrangement* (sorted, persistent copy) of a table
or materialized view keyed by the given columns. It is built by scanning the
relation's existing index at the statement's GSN and maintained from then on.

- `relation` must be a table or a materialized view (`"x" is not a table or
  materialized view` otherwise), and not a [broken](operations.md#recovery)
  one.
- Only plain column names are allowed (`only column indexes are supported`
  otherwise). Sort direction, `NULLS FIRST/LAST`, operator classes,
  `UNIQUE`, `USING`, `INCLUDE` and `WHERE` are accepted and ignored: the key
  is always ascending, unique indexes are not enforced, partial indexes are
  full.
- The default name is `<relation>_<col1>_<col2>..._idx`.
- When the relation changes shape — an upstream schema change of a
  replicated table, a replacement or re-planning of a materialized view —
  its indexes follow their key columns by name (`SHOW INDEXES` shows the new
  positions). An index whose column is gone is dropped (logged); a
  `CREATE OR REPLACE` that would do that is rejected instead (see above).

What indexes are used for:

- **Lookups and ranges in reads**: a query whose `WHERE` contains
  `column = constant` conditions on a table or materialized view, optionally
  followed by a range (`<`, `<=`, `>`, `>=`, `BETWEEN`) on the next key
  column, reads only the matching key range of the index those conditions
  narrow most, instead of scanning (the constant must have exactly the
  column's type). The primary index of a table already serves primary-key
  lookups and ranges.
- **Index joins in reads**: when the left side of a join (inner, left, or
  the semi- and anti-joins of `EXISTS`/`IN` subqueries) has few rows and the
  right side is a table or materialized view (possibly filtered) with an
  index whose leading columns are equated with the left side, each left row
  is looked up in that index instead of scanning and hashing the right side.
- **Sharing with maintained views**: arrangements are identified by their
  structure (the relation and the key). A materialized view whose join needs
  lookups on `t(c)` uses the existing index on `t(c)` instead of building
  another one, and vice versa.

An `AS OF` read uses only indexes whose history covers its GSN
([Time travel](reads.md#time-travel-as-of-gsn)): an index created after
that GSN is not used for it. Indexes do not make joins in maintained views
possible or impossible — the planner creates the arrangements it needs —
they only avoid duplicates and speed up reads.

## DROP

```sql
DROP { TABLE | VIEW | MATERIALIZED VIEW | INDEX } [ IF EXISTS ] name [ , ... ] [ CASCADE | RESTRICT ]
DROP SOURCE [ IF EXISTS ] name [ CASCADE ]   -- see Sources
DROP SINK [ IF EXISTS ] name                 -- see Sinks
```

- `DROP VIEW` also drops materialized views; `DROP MATERIALIZED VIEW` only
  materialized ones; the object kind must otherwise match
  (`"x" is not a table`, SQLSTATE `42809`). A missing object fails with
  `42P01` (`table "x" does not exist`) unless `IF EXISTS` is given.
- Replicated tables cannot be dropped
  (`cannot drop source table "t"; drop the source instead`).
- The objects that depend on a relation are the views and materialized views
  that read it, the sinks from it and the indexes on it. A view reads the
  relations its query refers to as the binder resolves them, including
  through the virtual views it expands (only for a stored query that no
  longer binds does every occurrence of the name as a word count). Views and
  sinks make the drop fail with SQLSTATE `2BP01`
  (`cannot drop table t because other objects depend on it`, the first
  dependent in the detail) unless `CASCADE` is given, which drops them,
  recursively, first. Indexes never hold a drop back: they are dropped with
  their table or materialized view, as in PostgreSQL. `RESTRICT` is the
  default.
- A statement naming several objects is atomic: every name is checked
  first, and an error (for example `table "nosuch" does not exist`) drops
  nothing. Objects named in the same statement do not hold each other back
  (`DROP VIEW v, view_over_v` needs no `CASCADE`).
- Subscriptions reading a dropped object — subscribed to it, or to a view or
  query that reads it — end with `table "x" was dropped`
  (`view "x" ...`, `materialized view "x" ...`; SQLSTATE `42P01`).
- Dropping releases the object's dataflow nodes and indexes unless other
  objects still share them; storage is reclaimed by
  [garbage collection](operations.md#checkpoints-and-garbage-collection).

## Object ids

Catalog objects get consecutive ids starting at 1000 (ids are never reused).
Client tools see them as OIDs `16384 + id` (`ivm.objects.oid`,
`pg_class.oid`). The id also appears in default slot names
([Sources](sources.md#slots)).
