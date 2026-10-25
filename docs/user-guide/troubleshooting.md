# Troubleshooting and limitations

## Troubleshooting

### A query hangs

- **Initial snapshot in progress.** Fresh reads of a replicated table (and
  of views over it) wait until its initial snapshot is complete (and a
  re-snapshot after an upstream schema change, once its first chunk has been
  logged). `SHOW SOURCES` shows `snapshotting <schema>.<table>`. Use
  `SET read_mode = latest` or `AS OF LATEST` to read the partial state.
- **`AS OF AT LEAST LSN`.** The read waits until every source's frontier
  passes the LSN. It waits forever if there is no source at all, and as long
  as any source is snapshotting, disconnected (`error: ...`) or behind.
- **`AS OF <gsn>` in the future** waits until the engine reaches that GSN.
- **`SUBSCRIBE` in psql** never prints anything: psql waits for the end of
  the result. Use `COPY (SUBSCRIBE ...) TO STDOUT`.
- **Every DML statement hangs**, and the server log shows
  `failed to apply gsn ...`: a log record could not be processed (an error
  inside `WITH RECURSIVE` maintenance or in an index key expression), and
  the engine waits forever for its reply; see the last deviation in
  [Errors as data](temporal-filters-and-errors.md#errors-as-data).
- **`DROP SINK`, or a `DROP` that drops a sink, hangs**: the sink is
  retrying its connection ([Sinks](sinks.md#monitoring-and-drop-sink)).

Waiting statements cannot be cancelled (only `SUBSCRIBE` can); disconnect
the client.

### CREATE SOURCE fails

| Error | Fix |
|---|---|
| `publication "x" does not exist` | create the publication, or fix the name (it is case-sensitive) |
| `table "s.t" has no primary key and REPLICA IDENTITY is not FULL` | add a primary key, `ALTER TABLE s.t REPLICA IDENTITY USING INDEX <unique index>` or `... REPLICA IDENTITY FULL`, or exclude the table with `FOR TABLES (...)` |
| `relation "t" already exists` | another object (possibly a table of another source) has the same name; rename it or use separate publications |
| `upstream table "s.t" has the same name as source "t"` | choose another source name |
| connection errors, `sslmode=require is not supported` | the engine connects without TLS ([connection strings](sources.md#connection-strings)) |

`CREATE SOURCE` does not create the replication slot itself; problems with
the replication connection (missing `REPLICATION` privilege, `wal_level`
not `logical`, no free slots or walsenders) appear afterwards as
`error: <message>` in `SHOW SOURCES` and as warnings in the server log, and
the source keeps retrying.

### A source shows `error: ...`

The source reconnects with backoff (up to 10 s) by itself. Persistent
errors:

- `replication slot "ivm_..." does not exist`: the slot was dropped
  upstream. The source never re-creates a slot it created before; drop and
  re-create the source (which takes a new initial snapshot).
- authentication or permission errors: fix the role; the source picks the
  change up on its next attempt.
- `relation "s.t" does not exist` or `permission denied for table t` while a
  table still has to be copied: the source cannot continue at all until the
  table is readable again; if it was removed from the publication or
  dropped, drop and re-create the source
  ([Sources](sources.md#tables-added-to-or-removed-from-the-publication)).

The reconnect delay doubles up to 10 s while sessions keep failing, and is
reset to 200 ms by a session that replicated an upstream transaction or
stayed connected for 10 s.

### Reads of a view fail with an error

The view has [error records](temporal-filters-and-errors.md#errors-as-data):
some input row makes an expression fail. `SHOW VIEWS` shows the number of
distinct errors. Fix or delete the offending rows. If the view keeps failing
after the data is fixed and the engine was restarted in between, drop and
re-create the view (see [Known problems](#known-problems)).

### CREATE MATERIALIZED VIEW is rejected

- `function f() is not allowed in a materialized view` — non-deterministic
  functions (`random()`, `clock_timestamp()`, `gen_random_uuid()`); see
  [Maintained views](../sql/maintained-views.md).
- `now() is only supported in temporal filters of maintained views` — see
  [Temporal filters](temporal-filters-and-errors.md#accepted-forms).
- `... is not supported` (SQLSTATE `0A000`) — a construct the engine does
  not implement; see [Queries](../sql/queries.md).

### The engine does not start

See the [startup failure table](operations.md#recovery). Several problems
leave the data directory unusable without restoring a backup, because the
checkpointed catalog or the log contains something that can no longer be
planned or applied:

- a table with a quoted mixed-case name was dropped while views used it;
- an upstream schema change removed or renamed a column a materialized view
  uses, or a column an index on the replicated table uses;
- `CREATE OR REPLACE` replaced a materialized view that has an index with a
  version with fewer columns, replaced a materialized view that has an index
  or a sink with a virtual view, or made a view read a relation created after
  it (`relation "b" has no dataflow`);
- a materialized view with a `DISTINCT` aggregate and `FILTER`, or with a
  negative `GROUPS` frame offset, crashed the engine (the statement is
  replayed and crashes again on every start);
- a log record whose processing failed (see above) is replayed and fails
  again, if no checkpoint was taken after it.

Either restore a copy of the data directory taken before the problem, or
start over with an empty data directory (and drop the old replication slots
upstream). See [Known problems](#known-problems) for how to avoid these.

### A sink shows `error: ...`

The sink reconnects by itself. Check the role's privileges on the target
schema and tables, and that an existing target table matches the relation's
columns ([requirements](sinks.md#requirements-on-the-target)). After an
outage, compare the target with the view: changes that arrived while the sink
was reconnecting may be missing ([known problems](sinks.md#known-problems)).
To force a resynchronisation, restart the engine or `DROP SINK` and
`CREATE SINK` again: a new sink's snapshot is newer than any committed
progress, so it rewrites the target from the snapshot.

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

### Disk usage grows

- The input log keeps segments back to `checkpoint GSN − --retention-gsns`
  (and always the current 64 MiB segment).
- Dead index nodes are reclaimed only in sealed segments that are less than
  half live, one segment at a time after each checkpoint (at most 1000 nodes
  relocated per pass); up to half of any other sealed segment can stay
  garbage, and the active segment (up to 256 MiB) is not collected until it
  is full.
- Large `--retention-gsns` keeps more versions in the indexes.

### Upstream WAL grows

A slot retains WAL while its engine is stopped, cannot start, or a source is
stuck in an error loop. Check `pg_replication_slots`; drop slots of engines
that are gone ([cleanup](sources.md#cleaning-up-upstream-slots)).

### Clients cannot connect or fail right after connecting

- TLS required by the client (`sslmode=require`): not supported.
- The client sets `TimeZone` to something other than UTC, or
  `client_encoding` other than UTF-8: the `SET` fails; configure the client
  for UTC/UTF-8.
- The client uses `DECLARE ... CURSOR` (named/server-side cursors), `COPY`,
  or other unsupported statements.

## Limitations and differences from PostgreSQL

### Engine

- Single node with a single dataflow thread; no replicas, no online backup,
  no shipping of the log to object storage, no Kafka sinks.
- No transactions: `BEGIN`/`COMMIT`/`ROLLBACK` are accepted but each
  statement is applied immediately and `ROLLBACK` undoes nothing.
- No roles, privileges or schemas; one optional cleartext password for all
  clients; no TLS.
- Session settings are fixed: time zone UTC, `DateStyle` ISO, `IntervalStyle`
  postgres, encoding UTF-8; string comparison is by code point (the
  `C.UTF-8` collation); `COLLATE` only accepts `C`, `default`, `ucs_basic`,
  `pg_c_utf8` and `C.UTF-8`.
- `AS OF` reads use the current catalog.
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
- In maintained views, expression errors become error records instead of
  failing a statement.
- `current_user`, `session_user`, `user` and `current_catalog` fail with
  `function current_user() does not exist` (and so on); `current_role`,
  `current_schema` and `current_database()` work.

## Known problems

Problems found while writing and checking this documentation (at commit
`d80c7f2`), with workarounds. Most were reproduced on a running engine.

### Crashes and data directories that cannot start

| Problem | Effect | Workaround |
|---|---|---|
| A `DISTINCT` aggregate with `FILTER` (`count(DISTINCT v) FILTER (WHERE ...)`) in a materialized view or `SUBSCRIBE (query)` | the engine overflows its stack; for a materialized view the logged statement crashes every start | rewrite with a subquery or `CASE` |
| A negative `GROUPS` frame offset (`GROUPS BETWEEN -1 PRECEDING ...`) | the engine thread panics (every statement then fails with `engine is shutting down`); in a materialized view it is replayed on every start. Negative `ROWS`/`RANGE` offsets give wrong results instead of an error | do not use negative offsets |
| Indexes on a replicated table survive an upstream schema change pointing at the old table | `SHOW ARRANGEMENTS` crashes the engine thread; if the indexed column was dropped, start-up panics after the next checkpoint | drop indexes on replicated tables before changing their upstream schema |
| An upstream schema change that breaks a materialized view (dropped or renamed column) | the view silently stops updating; the engine fails to start after the next checkpoint | drop affected views before such changes |
| Dependency detection is textual (`catalog.rs`, `sql_mentions`) | dropping a quoted mixed-case table or view succeeds while views use it, and the engine fails to start after the next checkpoint; conversely a view that merely contains the name (a column alias, a string literal) blocks a `DROP`, and `CASCADE` drops it | avoid mixed-case quoted names; drop dependent views explicitly |
| `CREATE OR REPLACE MATERIALIZED VIEW` does not switch dependents | materialized views over it read the old version until the next restart; its indexes keep the old version and make start-up panic if the new version has fewer columns | drop dependent views and indexes first |
| `CREATE OR REPLACE` of a materialized view that has an index or a sink by a virtual view | the sink stops receiving changes; the next start fails (`index target without dataflow`, `sink source without dataflow`) | drop indexes and sinks first |
| `CREATE OR REPLACE` that makes a view read a relation created after the view | the next start fails (`relation "b" has no dataflow`), because recovery rebuilds objects in creation order | drop and re-create the view instead of replacing it |
| An error that escapes the dataflow (inside `WITH RECURSIVE` maintenance, or computing an index key such as a join on `a.x + 1`) | the change is half-applied, the statement never returns, every later DML statement hangs, and replaying the record fails at the next start unless a checkpoint followed it | avoid expressions that can fail in join keys and recursive queries |
| `var_pop`/`var_samp`/`stddev*` over very large `bigint` values | the sum of squares overflows: the debug build panics the engine thread, a release build returns a wrong result | cast to `numeric` |

### Lost or wrong data

| Problem | Effect | Workaround |
|---|---|---|
| Primary-key changes of a `REPLICA IDENTITY FULL` or `NOTHING` table are not detected | the table stays keyed by the old key; if those columns stop being unique, rows with the same old key are merged | re-create the source after such a change |
| A sink drops queued changes while reconnecting | the target misses changes after an outage, or after concurrent `CREATE SINK`s race on creating `ivm_sink_progress` | create `ivm_sink_progress` in advance; create sinks one at a time; resynchronise after outages |
| Time-travel reads use indexes built after the read GSN | `AS OF` reads of a replaced (or re-indexed) materialized view, and point lookups through a newer index, return too few rows or none | read without the `WHERE column = constant` condition, or within the index's lifetime |
| Error records: not retracted after a restart (aggregate groups) or when they existed at creation (aggregates without `GROUP BY`); errors below an index or of shared operators not counted at creation; not versioned for `AS OF`; errors in join conditions treated as no match | views fail although the data is fixed, or succeed although PostgreSQL would fail ([details](temporal-filters-and-errors.md#errors-as-data)) | drop and re-create the view |
| `HAVING` with a constant condition over an aggregate without `GROUP BY` (`SELECT count(*) FROM t HAVING false`) | returns one row instead of none | use `WHERE` on a subquery |
| Mixed `real` (`float4`) and `numeric`/`integer` arithmetic and comparisons use `real` | different results from PostgreSQL, which uses `double precision` (`0.1::float4 = 0.1` is true) | cast to `double precision` |
| Ticks can be postponed under continuous input | `now()` and temporal filters lag | none |
| `--no-sync` and operating-system crashes | lost records; GSN reuse with hanging clients, stale reads and lost upstream transactions | do not use `--no-sync` for real data |

### Hangs and other problems

| Problem | Effect | Workaround |
|---|---|---|
| Only `SUBSCRIBE` can be cancelled | reads waiting for a snapshot, an LSN or a GSN wait forever if the condition never holds | disconnect |
| `AS OF AT LEAST LSN` without sources | waits forever | do not use it without sources |
| `DROP SINK` of a sink that is retrying its connection | the engine thread waits for the sink, possibly for a long time | stop and restart the engine; the drop is replayed |
| A table that cannot be read while its snapshot is incomplete (removed from the publication, dropped, no `SELECT`) | the whole source stops streaming and the slot retains WAL | re-create the source |
| A dropped slot is never re-created | source stuck in `error: replication slot ... does not exist` | drop and re-create the source |
| `SUBSCRIBE` ends when an error record is retracted; a subscription to a dropped table never ends | spurious errors; a subscription that never finishes | re-subscribe; end it from the client |
| A multi-object `DROP` that fails part-way | the earlier objects are dropped anyway, and the statement is missing from `ivm.catalog_history` | drop objects one at a time |
| `CREATE SINK` does not check `KEY` columns | the sink fails every connection attempt (`sink key column "x" does not exist`) | check the names; drop and re-create the sink |
| A listen address that cannot be bound | the server exits with status 0 after recovery | check the log; do not rely on the exit status |
| `SHOW read_mode` before `SET read_mode` | `unrecognized configuration parameter` | `SET read_mode = fresh` first |
| `current_user`, `session_user`, `user`, `current_catalog` | `function ...() does not exist` | use a literal |
| Stop file | no final checkpoint; the log is replayed on the next start (harmless) | none needed |
