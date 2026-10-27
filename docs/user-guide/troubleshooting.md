# Troubleshooting and limitations

## Troubleshooting

### A query hangs

- **A table is being copied.** Fresh reads of a replicated table (and of
  views over it) wait until the table is completely copied: during its
  initial snapshot, and during a re-snapshot after an upstream schema or
  key change, a lost replication slot or `ALTER SOURCE ... REFRESH`.
  `SHOW SOURCES` shows `snapshotting <schema>.<table>`. Use
  `SET read_mode = latest` to read the partial state.
- **A table that cannot be copied** (`error: cannot copy table ...` in
  `SHOW SOURCES`): fresh reads of it wait until it can be read; reads of
  the other tables do not.
- **`AS OF AT LEAST LSN`.** The read waits until every source's frontier
  passes the LSN: as long as any source is copying a table (or cannot copy
  one), disconnected (`error: ...`) or behind. Without any source it fails
  at once.
- **The engine thread is busy.** All statements run on one thread: while it
  applies a large upstream transaction, every other statement waits. (New
  and replaced views, indexes, tables rebuilt after upstream schema changes
  and subscriptions are hydrated, checkpoints written and large reads run
  on threads of their own.)
- **A view is being hydrated.** Statements that read a view that is still
  being hydrated wait until it is maintained; a new view or index created
  meanwhile is hydrated after it; `CREATE OR REPLACE` of an existing view or
  `CREATE SINK` logged meanwhile waits for it, and so do the changes logged
  after it.
- **DML** that reads rows a statement logged before it writes (the same
  primary key, or any row of a table it scans) waits for that statement to
  be applied.
- **`SUBSCRIBE` in psql** never prints anything: psql waits for the end of
  the result. Use `COPY (SUBSCRIBE ...) TO STDOUT`.

A waiting read can be cancelled (Ctrl-C in psql, SQLSTATE `57014`) and is
dropped when its client disconnects. DML queued behind another statement
can be cancelled too (it still runs if its client just disconnects).

### CREATE SOURCE fails

| Error | Fix |
|---|---|
| `publication "x" does not exist` | create the publication, or fix the name (it is case-sensitive) |
| `table "s.t" has no primary key and REPLICA IDENTITY is not FULL` | add a primary key, `ALTER TABLE s.t REPLICA IDENTITY USING INDEX <unique index>` or `... REPLICA IDENTITY FULL`, or exclude the table with `FOR TABLES (...)` |
| `relation "t" already exists` | another object (possibly a table of another source) has the same name; rename it or use separate publications |
| `upstream table "s.t" has the same name as source "t"` | choose another source name |
| `the upstream database's encoding is SQL_ASCII ...` | replicate from a database with a real encoding |
| `source connections must use client_encoding UTF8, not ...` | remove `client_encoding` from the connection string |
| connection errors, `sslmode=require is not supported (no TLS support)` | the engine connects without TLS ([connection strings](sources.md#connection-strings)) |

`CREATE SOURCE` does not create the replication slot itself; problems with
the replication connection (missing `REPLICATION` privilege, `wal_level`
not `logical`, no free slots or walsenders) appear afterwards as
`error: <message>` in `SHOW SOURCES` and as warnings in the server log, and
the source keeps retrying.

### A source shows `error: ...`

The source reconnects with backoff (up to 10 s) by itself. Persistent
errors:

- `replication slot "ivm_..." does not exist (dropped upstream); re-creating
  it ...` or `... was invalidated upstream ...`: the slot was dropped, or
  invalidated by `max_slot_wal_keep_size`. The source re-creates it and
  copies all its tables again by itself
  ([Slots](sources.md#slots)).
- authentication or permission errors: fix the role; the source picks the
  change up on its next attempt.
- `cannot copy table s.t: relation "s.t" does not exist`,
  `... permission denied for table t` or the error of a value that does not
  parse (after a streamed change with such a value, the table is copied
  again): the table still has to be copied but cannot be read. The source
  retries it (up to every 60 s) and streams the other tables meanwhile; fix
  the privilege or the value, or, if the table was dropped, drop and
  re-create the source
  ([Sources](sources.md#tables-added-to-or-removed-from-the-publication)).

The reconnect delay doubles up to 10 s while sessions keep failing, and is
reset to 200 ms by a session that replicated an upstream transaction or
stayed connected for 10 s.

### Reads fail with an error

- **The error of an expression** (`division by zero`, ...): the view has
  [error records](temporal-filters-and-errors.md#errors-as-data), some input
  row makes an expression fail. `SHOW VIEWS` shows the number of distinct
  errors. Fix or delete the offending rows; the error records go with them.
- **`materialized view "v" is broken: <reason>`** (SQLSTATE `55000`): the
  view, or a view it reads, could not be rebuilt at the last start. Drop it
  or repair it with `CREATE OR REPLACE MATERIALIZED VIEW`
  ([Recovery](operations.md#recovery)).
- **`AS OF <gsn> is not supported`, `AS OF LATEST is not supported`**
  (`0A000`): there is no time travel; reads see the latest state. Use
  `SET read_mode = latest` for what `AS OF LATEST` did.

### A statement is rejected

- `function f() is not allowed in a materialized view` — non-deterministic
  functions (`random()`, `clock_timestamp()`, `gen_random_uuid()`,
  `current_setting()`, `pg_backend_pid()`); see
  [Maintained views](../sql/maintained-views.md).
- `now() is only supported in temporal filters of maintained views` — see
  [Temporal filters](temporal-filters-and-errors.md#accepted-forms).
- `system relation "pg_class" is not allowed in a materialized view` (also
  casts to `regclass` and `pg_get_viewdef()`) — maintained views cannot
  read the catalog; use a virtual view
  ([Maintained views](../sql/maintained-views.md)).
- `... is not supported` (SQLSTATE `0A000`) — a construct the engine does
  not implement; see [Queries](../sql/queries.md).
- `cannot replace view "v": index "i" depends on its columns`,
  `... sink "s" reads its columns` or
  `... materialized view "w" would no longer be valid: <reason>`
  (SQLSTATE `2BP01`) — a `CREATE OR REPLACE` would break a dependent object:
  an index needs its columns (by name) in a materialized view, a sink needs
  the view's columns unchanged, and every materialized view reading the
  view must still plan and stay deterministic. Drop the dependent object
  first (and re-create it afterwards), or keep what it needs
  ([CREATE OR REPLACE](tables-and-views.md#create-or-replace)).
- `cannot drop table t because other objects depend on it` (`2BP01`) —
  views or sinks read it; use `CASCADE` or drop them first (indexes go with
  their relation).

### The engine does not start

See the [startup failure table](operations.md#recovery). A materialized
view or index that can no longer be built does not stop the start: it is
marked broken ([Recovery](operations.md#recovery)). What still keeps a data
directory from starting: a missing or damaged manifest or segment file, a
damaged or deleted input-log segment holding records after the last
checkpoint, a data directory or checkpoint written by an incompatible (or
newer) version ([Compatibility](../on-disk-format.md#compatibility)), and a
log record other than DDL that fails to apply again during replay (a bug:
such a record was skipped while the engine ran, and fails again on replay
unless a checkpoint was taken after it).

Either restore a copy of the data directory taken before the problem, or
start over with an empty data directory (and drop the old replication slots
upstream).

### A sink shows `error: ...` or stops advancing

The sink reconnects by itself and keeps the changes it has not written
yet, retrying after 100 ms, then with a delay doubling up to 10 s while its
attempts keep failing; the status shows the error until a write succeeds.
Check the role's privileges on the target schema and tables, and that an
existing target table matches the relation's columns
([requirements](sinks.md#requirements-on-the-target)):

- `error: column "c" of relation "t" does not exist` after an upstream
  schema change (or a re-planned view) gave the relation new columns: the
  sink never alters its target; add the columns to the target table (or
  drop the table, which the sink then creates again)
  ([Sinks](sinks.md#schema-changes-and-replacements)).
- an error of the target's own (`duplicate key value violates unique
  constraint`, `ON CONFLICT DO UPDATE command cannot affect row a second
  time`, `violates check constraint`, a value that does not convert): the
  target rejects a batch, and keeps doing so until the target is fixed —
  its constraints or column types, or duplicate `KEY` values (the KEY
  columns must be unique in the relation; a batch already pending keeps its
  duplicates, so re-create the sink after fixing the data, see below).
- `error: broken: <reason>`: the sink reads a broken view
  ([Recovery](operations.md#recovery)).

To force a resynchronisation, `DROP SINK` and `CREATE SINK` again: a new
sink's snapshot is newer than any committed progress, so it rewrites the
target from the snapshot. (A restart does not: a target that is up to date
resumes.)

### Maintenance is slow

Use `EXPLAIN MAINTENANCE` and `EXPLAIN DATAFLOW`. Typical causes:

- **Joins without an equality condition** (or with conditions that are not
  equalities between the two sides) scan the whole other input for every
  changed row.
- **Window functions** recompute every partition that changes, `WITH
  RECURSIVE` recomputes the whole fixpoint on every change, and ordered
  aggregates (`string_agg`, `array_agg`, `json_agg`, `jsonb_agg`,
  `json(b)_object_agg`, `mode`, `percentile_cont`, `percentile_disc`)
  rescan each changed group. Large partitions and groups make these expensive.
- **Top-k** (`ORDER BY ... LIMIT` in a view, `DISTINCT ON`) re-reads the
  first rows of each changed group.
- **Hot keys**: a change to a row that joins with many rows produces all of
  their outputs.
- Upstream bulk loads arrive as single large transactions, which are applied
  as one step.

Grouped `count`, `sum`, `avg`, `min`, `max`, `bool_and/or`, `stddev` and
`variance` are cheap regardless of group size (range summaries) — unless the
same query also has an ordered aggregate: then all of its aggregates are
computed by one operator that rescans each changed group.

When the engine cannot keep up, the backlog stays bounded: once the records
waiting to be applied take an eighth of `--memory-mb` (in bytes of memory),
sources stop reading the replication stream until it catches up (`SHOW
FRONTIERS`: `durable_gsn` − `processed_gsn`; `/api/overview`'s
`input_queue_bytes`, see [Memory](operations.md#memory)).

### Disk usage grows

- The input log keeps the segments holding records after the last checkpoint
  (and always the current 64 MiB segment). An engine that applies nothing
  but clock ticks checkpoints only every 3,600 ticks, so its log segment
  fills slowly (25 bytes per tick) and is deleted once it is sealed and
  covered by a checkpoint.
- Dead index nodes are reclaimed only in sealed segments that are less than
  half live: the maintenance pass before each checkpoint relocates their
  live nodes, within its budget
  ([Operations](operations.md#checkpoints-and-garbage-collection)); up to
  half of any other sealed segment can stay garbage, and the active segment
  (up to 256 MiB) is not collected until it is full.

### Upstream WAL grows

A slot retains WAL while its engine is stopped, cannot start, or a source is
stuck in an error loop, and while the engine is too far behind to take more
changes (see above). The slot of a dropped source is dropped in the
background, retried until the upstream answers. Check
`pg_replication_slots`; drop slots of engines that are gone
([cleanup](sources.md#cleaning-up-upstream-slots)).

### Clients cannot connect or fail right after connecting

- TLS required by the client (`sslmode=require`): not supported.
- The client sets `TimeZone` to something other than UTC, `client_encoding`
  other than UTF-8, or `DateStyle`, `IntervalStyle`, `bytea_output` or
  `standard_conforming_strings` to another value than the one in effect:
  the `SET` fails ([Session settings](reads.md#session-settings));
  configure the client accordingly.
- The client uses `DECLARE ... CURSOR` (named/server-side cursors), `COPY`,
  or other unsupported statements.

## Limitations and differences from PostgreSQL

### Engine

- Single node with a single dataflow thread; no replicas, no online backup,
  no shipping of the log to object storage, no Kafka sinks.
- No transactions: `BEGIN`/`COMMIT`/`ROLLBACK` are accepted but each
  statement is applied immediately and `ROLLBACK` undoes nothing.
- No roles, privileges or schemas; one optional cleartext password for all
  clients; no TLS. Connection strings, passwords included, are stored in
  clear text and shown with the stored statements (`SHOW CREATE VIEW`,
  `ivm.objects`, `ivm.catalog_history`, the web UI's catalog and history).
- Session settings are fixed: time zone UTC, `DateStyle` ISO, `IntervalStyle`
  postgres, encoding UTF-8; string comparison is by code point (the
  `C.UTF-8` collation); `COLLATE` only accepts `C`, `default`, `ucs_basic`,
  `pg_c_utf8` and `C.UTF-8`.
- There is no time travel: reads see the latest state (the indexes keep
  the current weights of their rows only).
- Changes queued for a subscription or a sink are bounded in memory: a
  subscription whose client does not keep up ends with `53000`, a sink that
  does not keep up resynchronises its target from a snapshot (see
  [Operations](operations.md#memory)).
- Upstream: PostgreSQL 14+, not `SQL_ASCII`; tables need a primary key,
  `REPLICA IDENTITY USING INDEX` or `REPLICA IDENTITY FULL`; publication row
  filters and column lists are not supported; unknown types replicate as
  text.

### DDL and DML

- No `ALTER TABLE`, `TRUNCATE` (local tables), `CREATE TABLE AS`, sequences,
  or column defaults (`DEFAULT` is accepted and ignored, `serial` has no
  sequence).
- Constraints other than `PRIMARY KEY` and `NOT NULL` are not enforced.
- No `RETURNING`, `ON CONFLICT DO UPDATE`, `UPDATE ... FROM`,
  `DELETE ... USING`, multi-column `SET (a, b) = ...`.
- `INSERT` converts values with any existing cast, including casts
  PostgreSQL would only apply explicitly.
- Views accept duplicate column names (`CREATE VIEW v AS SELECT 1 AS a,
  2 AS a`; PostgreSQL rejects them).
- Index options (`DESC`, `UNIQUE`, `WHERE`, expressions) are ignored or
  rejected.
- No cursors, `COPY` (other than `COPY (SUBSCRIBE ...) TO STDOUT`),
  `PREPARE`/`EXECUTE` statements (the protocol-level extended query
  protocol is supported), `LISTEN`/`NOTIFY`, user-defined functions or
  aggregates.

### Queries

See [Queries](../sql/queries.md) for the full list. Notable:

- Types: no `time with time zone`, `money`, network types, enums, ranges,
  composite/record types, geometric types, bit strings, `xml`, full text
  search types.
- `string_agg`, `array_agg`, `json_agg`, `jsonb_agg` and the object
  aggregates without `ORDER BY` order their input by the argument values (to
  be deterministic), not in an arbitrary scan order.
- Floating-point `sum`, `avg`, `variance` and `stddev` are computed exactly
  and rounded once, so results can differ from PostgreSQL's sequential
  summation in the last bits.
- When equal values have different representations (`1.0` and `1.00` in a
  `numeric` group key, `DISTINCT`, `min`/`max`), materialized views and
  `DISTINCT` return the one that sorts first in the index, while ad hoc
  `GROUP BY` and `min`/`max` return the first one in scan order. Either may
  differ from PostgreSQL's choice, and an ad hoc query can disagree with the
  same query as a materialized view (`1.000` versus `1`).
- `character` values that differ only in trailing spaces are told apart in
  grouping, `DISTINCT`, set operations and keys, and `real` compared with
  integers or `numeric` is compared in `real` (see
  [Known problems](#known-problems)).
- `numeric` sums are exact beyond `numeric`'s range: an `avg` whose sum
  overflows succeeds when the average fits (PostgreSQL fails).
- `WITH RECURSIVE` always computes the whole fixpoint: a recursive query
  that relies on an outer `LIMIT` to stop fails with the iteration limit
  (1,000,000).
- Expressions nested more than 1000 levels deep (for example a chain of more
  than 1000 `+`) fail with `stack depth limit exceeded`; `AND`/`OR` chains
  of any length work.
- Aggregates whose arguments reference only columns of an enclosing query
  are not supported.
- Parameters (`$n`) of `INSERT`, `UPDATE` and `DELETE`, and parameters
  whose type cannot be inferred, are described to drivers as `text`
  (PostgreSQL infers the column types, or fails).
- In maintained views, expression errors become error records instead of
  failing a statement.

## Known problems

Problems of the current code, each reproduced on a running engine or by a
test, with workarounds.

### Wrong or missing results

| Problem | Effect | Workaround |
|---|---|---|
| `character` (`bpchar`) values that differ only in trailing spaces — a `bpchar` without length, or `char(n)` columns of different lengths mixed — are distinct in primary keys, `GROUP BY`, `DISTINCT` and set operations (comparisons, joins and `IN` ignore the spaces, as in PostgreSQL) | `'x'` and `'x '` are two primary keys (PostgreSQL: a duplicate key) and two groups; `char(3) UNION char(5)` keeps `'ab '` and `'ab   '` apart | use `char(n)` of one length, or `rtrim()` / cast to `text` before grouping |
| Errors in join conditions of maintained views — a non-equality condition, the non-equality part of a join condition (`ON a.id = b.id AND a.x / b.y > 1`), `WHERE` conditions the optimizer moves into a join, and such conditions of `EXISTS`/`IN` subqueries — count as "no match" | the joined row is missing (in an outer join: NULL-padded) and no error record is created, where PostgreSQL fails; the same query ad hoc fails | guard the expression (`CASE WHEN b.y <> 0 THEN ... END`, `NULLIF`) |
| `real` compared with an integer or `numeric` value is compared in `real` (there are no cross-type comparison operators; arithmetic is done in `double precision`, as in PostgreSQL) | `0.1::real = 0.1` is true (PostgreSQL: false) | cast to `double precision` |
| A string literal continued on the next line (`'foo'` newline `'bar'`) | in a select list the second literal becomes a column alias (`foo` in a column `bar`; PostgreSQL returns `foobar`), elsewhere a syntax error | use `\|\|` |

### SQL and protocol

| Problem | Effect | Workaround |
|---|---|---|
| `set_config()` changes nothing (it returns its value) | `SELECT set_config('read_mode', 'latest', false)` leaves the session's settings as they were | use `SET` |

### Replication and sinks

| Problem | Effect | Workaround |
|---|---|---|
| Primary-key changes of a `REPLICA IDENTITY FULL` or `NOTHING` table are not detected (their Relation messages do not show the key) | the table stays keyed by the old key; if those columns stop being unique, rows with the same old key are merged | re-create the source after such a change |
| An upstream whose WAL positions went backwards (restored from a backup, or replaced by another cluster under the same connection string) | transactions up to the source's last logged LSN are skipped as already replicated | drop and re-create the source |
| A sink never alters its target table | after the relation gained columns (an upstream schema change, a re-planned view), the sink fails until the target has them | add the columns to the target, or drop the target table |

### Resources

| Problem | Effect | Workaround |
|---|---|---|
| The engine never waits for a client or a sink target: changes queue in memory up to each consumer's limit (a sixteenth of `--memory-mb`) | a subscriber that reads slowly ends with `53000`; a sink whose target is unreachable for long (or whose batches keep failing) stops receiving changes and rewrites the whole target once it can write again | subscribe again; give the server more memory if consumers are merely bursty |
