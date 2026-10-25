# Introspection: EXPLAIN, SHOW and system catalogs

Code: `crates/ivm/src/coord/read.rs` (`explain`, `show`),
`crates/ivm/src/sysrel.rs` (system relations), `crates/ivm/src/sql/plan.rs`
(`Rel::explain`).

## EXPLAIN

```sql
EXPLAIN [ PLAN ] [ FOR ] query
EXPLAIN [ PLAN ] [ FOR ] [ MATERIALIZED ] [ VIEW ] name
EXPLAIN DATAFLOW [ FOR ] [ MATERIALIZED ] [ VIEW ] name
EXPLAIN MAINTENANCE [ FOR ] [ MATERIALIZED ] [ VIEW ] name
```

`query` is anything starting with `SELECT`, `WITH`, `VALUES` or `(`.
`DATAFLOW` and `MAINTENANCE` need the name of a materialized view; for a
[broken](operations.md#recovery) one they fail with SQLSTATE `55000`
(`materialized view "v" is broken: <reason>`). `EXPLAIN` of DML or DDL is
not supported (anything else after `EXPLAIN` is taken as a view name).

### EXPLAIN [PLAN]: the logical plan

Prints the logical plan after binding, decorrelation and optimisation, one
operator per line, children indented below their parent. For a view name,
the view's query is planned. Column references are written `#n`: the n-th
column (0-based) of the operator's input; for a join, the left input's
columns come first.

```
 Aggregate group=[4] aggs=[count(*)([]), sum([5])]
   Map #3, #1
     Join Inner on (#2 = #0)
       Project [1, 2]
         Get orders
       Get customers
```

| Line | Operator |
|---|---|
| `Get name` | read a table or materialized view |
| `Values (n rows)` | constant rows (also system catalogs) |
| `Map e1, e2, ...` | append computed columns |
| `Filter p1 AND p2 ...` | keep rows where all predicates are true |
| `Project [c, ...]` | keep and reorder columns |
| `Join <kind> on ...` | join; kinds `Inner`, `Left`, `Right`, `Full`, `Semi`, `Anti` |
| `Aggregate group=[...] aggs=[...]` | grouped aggregation (output: group columns, then aggregates) |
| `Distinct` | remove duplicates |
| `Union`, `Except`, `Intersect` (`ALL`), `UnionAll` | set operations |
| `TopK group=[...] order=[...] limit=... offset=...` | per-group top-k (`LIMIT`/`OFFSET`, `DISTINCT ON`, `FETCH ... WITH TIES`) |
| `Window [...]` | window functions |
| `FlatMap [...]` | set-returning functions |
| `Recursive #id distinct=...`, `RecRef #id` | `WITH RECURSIVE` and its self-reference |
| `Negate` | multiplicities negated (internal) |

### EXPLAIN DATAFLOW: the maintained operators

Prints the dataflow of a materialized view from its root down to the base
tables:

```
 #4 Reduce group by [0], 2 aggregates arr#4[#0]
   #3 Mfp  arr#3[#0]
     #2 DeltaJoin 2 inputs, 2 delta paths
       #0 Table orders shared x2 arr#0[#0] arr#2[#1]
       #1 Table customers shared x2 arr#1[#0]
```

(Here `arr#2` is the join's lookup index of `orders` by `customer`, `arr#3`
the aggregation's input, arranged by the group key with per-node summaries,
and `arr#4` its output.)

Each line is `#<node id> <operator> <detail>`, followed by `shared x<n>` when
the node has more than one reference (consumers and pins: a table's own pin
counts, so a table read by one view shows `shared x2`; otherwise the node is
shared with other views), and by the node's arrangements as
`arr#<id>[<key expressions>]`. A node that appears again further down is
printed once more with `(see above)`.

| Operator | Detail | Meaning |
|---|---|---|
| `Table` | table name | a base table; its first arrangement is the primary index |
| `Constant` | | constant rows |
| `Mfp` | | fused map/filter/project |
| `Union`, `Negate` | | union of inputs (some negated) |
| `FlatMap` | | set-returning functions per row |
| `DeltaJoin` | `<n> inputs, <n> delta paths` | n-way join without intermediate state |
| `Reduce` | `group by [cols], <n> aggregates` | grouped aggregation over summaries |
| `Distinct` | | set semantics |
| `SetOp` | | `UNION`/`EXCEPT`/`INTERSECT [ALL]` |
| `TopK` | `limit Some(<n>) offset <m>` (`limit None` without a limit) | per-group top-k |
| `Window` | | window functions, per partition |
| `Recursive` | | `WITH RECURSIVE` (recomputed per change) |
| `TemporalFilter` | | conditions over `now()` |

The operators are described in [Dataflow](../internals/dataflow.md).

### EXPLAIN MAINTENANCE: work per change

One row per base table that the view depends on:

| Column | Meaning |
|---|---|
| `source` | the base table |
| `delta rows/tick` | measured average number of delta rows (an update is two: the old and the new row) per step in which the table changed, since the engine started (not persisted) |
| `expected work per delta` | heuristic estimate of the work one changed row causes on its way to the view |
| `path` | the operators from the table to the view's root |

```
  source   | delta rows/tick | expected work per delta |                              path
-----------+-----------------+-------------------------+----------------------------------------------------------------
 orders    | 4.00            | 4.0                     | orders -> join(1.0 lookups) -> mfp -> reduce(range summary)
 customers | 2.00            | 4.0                     | customers -> join(1.0 lookups) -> mfp -> reduce(range summary)
```

The estimate starts at 1 and follows the first consumer at each step: a
delta join multiplies it by `1 + lookups`, where `lookups` sums the expected
number of index probes along the table's delta path (each probe's fan-out is
the average number of entries per key in a sample of up to 2000 entries of
the probed index); a `Reduce` adds 2 (two range summaries); a `TopK` adds
its limit (100 without one); a `Window` multiplies by 10. It is a rough
guide, not a cost model — the planner does not use it.

## SHOW

```sql
SHOW SOURCES | TABLES | VIEWS | MATERIALIZED VIEWS | INDEXES | SINKS
SHOW ARRANGEMENTS | FRONTIERS | GSN
SHOW COLUMNS { FROM | IN } name
SHOW CREATE [ MATERIALIZED ] VIEW name
SHOW ALL
SHOW parameter
```

All results are text columns, except `SHOW GSN`.

| Command | Columns |
|---|---|
| `SHOW SOURCES` | `name`, `publication`, `slot`, `tables`, `lsn`, `status` ([Sources](sources.md#monitoring)) |
| `SHOW TABLES` | `name`, `source` (source name or `(local)`), `rows` (sum of multiplicities in the primary index) |
| `SHOW VIEWS` | `name`, `kind` (`materialized` or `virtual`), `rows` (materialized views: rows in the view's index), `errors` (number of distinct error records currently present), `status` (materialized views: `ok`, or `broken: <reason>` for a view that is not maintained, see [Operations](operations.md#recovery)) |
| `SHOW MATERIALIZED VIEWS` | same as `SHOW VIEWS`, materialized views only |
| `SHOW INDEXES` | `name`, `on` (relation), `columns` (0-based column positions, e.g. `[2]`) |
| `SHOW SINKS` | `name`, `from`, `table`, `status` ([Sinks](sinks.md#monitoring-and-drop-sink)); a sink of a broken view shows `error: broken: <reason>` |
| `SHOW ARRANGEMENTS` | `arrangement` (id), `node` (dataflow node id), `operator` (the node's operator), `key` (key expressions, `#n` = column n), `rows` (sum of multiplicities), `refs` (references: operators and pins using it) |
| `SHOW FRONTIERS` | rows of `frontier`, `value`: `durable_gsn` (last GSN made durable by the sequencer), `processed_gsn` (last GSN applied), `checkpoint_gsn` (GSN of the last checkpoint), `retention_frontier` (oldest GSN readable with `AS OF`), `cache_bytes` (bytes of tree nodes in memory), `segments` (number of storage segments), and one `source <name> lsn` row per source that has reported a frontier |
| `SHOW GSN` | `gsn` (`bigint`): last applied GSN |
| `SHOW COLUMNS FROM t` | `name`, `type` (as `format_type` prints it), `nullable` (`true`/`false`), `key` (`true` for primary-key columns of a table) |
| `SHOW CREATE VIEW name` | `name`, `create_sql`: the stored statement of **any** object named `name` (table, view, index, source, sink); for a replicated table a comment `-- table <schema>.<table> replicated by source <id>` |
| `SHOW ALL` | `name`, `setting` for `server_version`, `timezone`, `datestyle` |
| `SHOW parameter` | [Session settings](reads.md#session-settings) |

Statements are stored as the canonical text printed by the SQL parser, so
`SHOW CREATE VIEW` may differ in formatting from what was typed. The stored
`CREATE SOURCE` and `CREATE SINK` statements include the connection string,
including any password in it.

## The ivm schema

Two engine relations can be queried like tables:

```sql
SELECT * FROM ivm.catalog_history;
SELECT * FROM ivm.objects;
```

| Relation | Columns | Contents |
|---|---|---|
| `ivm.catalog_history` | `gsn bigint`, `statement text` | every DDL statement that was applied successfully, with its GSN, in order — including statements of objects that have since been dropped; the most recent 10,000 (up to 11,000) are kept. A statement that fails (for example a multi-name `DROP` with one unknown name) changes nothing and is not listed. "What did the catalog look like at GSN g?" is a query over it, with one gap: upstream schema changes rewrite table definitions without an entry. |
| `ivm.objects` | `oid oid`, `name text`, `kind text`, `created_gsn bigint`, `definition text` | the current catalog objects: OID (`16384 + id`), name, kind (`table`, `view`, `materialized view`, `index`, `source`, `sink`), the GSN of the statement that created (or last replaced) the object, and its stored statement |

Both are computed from the catalog when a query is planned; they are not
versioned, so `AS OF` returns their current contents.

## PostgreSQL system catalogs

Client tools discover schemas through `pg_catalog` and
`information_schema`. The engine provides the relations those tools commonly
read, computed from its catalog when a query is planned (`sysrel.rs`). Names
starting with `pg_` resolve to `pg_catalog` without a schema prefix. Every
object appears in schema `public`, owned by role `ivm`, in database `ivm`.

| Relation | Contents |
|---|---|
| `pg_namespace` | `pg_catalog` (11), `pg_toast` (99), `public` (2200), `information_schema` (13000) |
| `pg_class` | tables (`relkind` `r`), materialized views (`m`), views (`v`), indexes (`i`), plus one `<table>_pkey` index per table with a primary key (OID `16384 + id + 1000000`); `relhasindex` is true for tables with a primary key or an index and for materialized views with an index |
| `pg_attribute` | the columns of tables, views and materialized views (indexes have none) |
| `pg_type` | the base types and their array types |
| `pg_index` | primary-key indexes and `CREATE INDEX` indexes (`indkey` is 1-based) |
| `pg_constraint` | one primary-key constraint per table with a primary key |
| `pg_indexes` | primary-key indexes only |
| `pg_tables`, `pg_views`, `pg_matviews` | tables, views, materialized views (`hasindexes` as `relhasindex`) |
| `pg_am` | `heap`, `btree` |
| `pg_database` | `ivm` |
| `pg_roles`, `pg_authid`, `pg_user` | role `ivm` (superuser) |
| `pg_settings` | a few preset settings |
| `pg_tablespace`, `pg_collation` | defaults |
| `pg_attrdef`, `pg_description`, `pg_shdescription`, `pg_policy`, `pg_statistic_ext`, `pg_publication`, `pg_publication_rel`, `pg_publication_namespace`, `pg_inherits`, `pg_trigger`, `pg_rewrite`, `pg_proc`, `pg_extension`, `pg_enum`, `pg_foreign_table`, `pg_partitioned_table`, `pg_sequence`, `pg_depend`, `pg_event_trigger`, `pg_stat_activity` | present with their commonly used columns, but empty |
| `information_schema.schemata` | `pg_catalog`, `public`, `information_schema` |
| `information_schema.tables` | tables (`BASE TABLE`), views and materialized views (`VIEW`) |
| `information_schema.views` | views and materialized views |
| `information_schema.columns` | columns with types, precision, nullability |

Only the listed columns exist; a query that selects another column of these
relations fails with `column ... does not exist`. Other catalog relations
(`pg_cursors`, `pg_locks`, statistics views, ...) do not exist.

Catalog functions for client tools: `pg_table_is_visible`,
`pg_type_is_visible`, `pg_function_is_visible` (always true),
`pg_get_userbyid` (`ivm`), `format_type`, `pg_get_indexdef`,
`pg_get_constraintdef`, `pg_get_viewdef` (answered from the catalog),
`obj_description`, `col_description`, `shobj_description`, `pg_get_expr`,
`pg_get_ruledef` and the other `pg_get_*def` / `pg_get_function_*`
functions (NULL), `pg_relation_is_publishable` (false), `pg_table_size`,
`pg_relation_size`, `pg_total_relation_size`, `pg_indexes_size` (0),
`pg_size_pretty`, `current_schemas`, `has_*_privilege` (always true),
`pg_encoding_to_char` (`UTF8`), `pg_backend_pid`, `pg_is_in_recovery`
(false), `pg_typeof`, `version()` (`PostgreSQL 17.0 (ivm)`),
`current_database()` (`ivm`), `current_schema` (`public`), `current_user`
and the other SQL value functions (`ivm`), `current_setting` and
`set_config` ([Functions](../sql/functions.md#other-functions)).

Casts to `regclass`, `regtype` and `regnamespace` resolve names as in
PostgreSQL: `'orders'::regclass` (also schema-qualified or quoted; the
system catalog tables have PostgreSQL 17's fixed OIDs, so
`classoid = 'pg_class'::regclass` works), `'varchar(3)[]'::regtype` (any
type spelling), `'public'::regnamespace`, with PostgreSQL's errors for
unknown names (`relation "x" does not exist`, `type "x" does not exist`,
`schema "x" does not exist`). The result is an `oid`, so it compares with
OID columns (`WHERE attrelid = 'orders'::regclass`); as a select-list item
it prints the name, in a column named after the type (`regclass`). OIDs cast
to these types print names (or the number, for an OID that names nothing),
as `text`: compare OIDs, not reg* values (`c.oid::regclass =
'orders'::regclass` fails with `operator does not exist: text = oid`).
`regproc`, `regrole`, `regprocedure` and `regoper` are plain aliases of
`oid`: OIDs print as numbers and names are not accepted.

### psql

With psql 17, the `\d` family works:

| Command | Works |
|---|---|
| `\d`, `\dt`, `\dv`, `\dm`, `\di`, `\dn`, `\l`, `\du`, `\dg`, `\df`, `\dx` (both empty) | yes |
| `\d name`, `\d+ name` for tables and materialized views | yes, with the primary key and the indexes |
| `\d view`, `\d+ view`, `\sv view` | yes (`\d+` and `\sv` show the stored definition) |
| `\d index` | the index's table and kind, but no columns |
| `\dt+`, `\di+` | yes, sizes are `0 bytes` |
| `\dp`, `\db`, `\dd`, `\dD`, `\dC`, `\do`, `\dconfig` | no: they read catalog columns, functions or relations that do not exist |

The system catalogs are not rows of `pg_class` themselves, so `\dS` lists
only the engine's objects and `\d pg_class` finds nothing.
