# Troubleshooting and limitations

## Troubleshooting

### A query hangs

- **Initial snapshot in progress.** Fresh reads of a replicated table (and
  of views over it) wait until its initial snapshot — or the re-snapshot
  after an upstream schema change — is complete. `SHOW SOURCES` shows
  `snapshotting <schema>.<table>`. Use `SET read_mode = latest` or
  `AS OF LATEST` to read the partial state.
- **`AS OF AT LEAST LSN`.** The read waits until every source's frontier
  passes the LSN. It waits forever if there is no source at all, and as long
  as any source is snapshotting, disconnected (`error: ...`) or behind.
- **`AS OF <gsn>` in the future** waits until the engine reaches that GSN.
- **`SUBSCRIBE` in psql** never prints anything: psql waits for the end of
  the result. Use `COPY (SUBSCRIBE ...) TO STDOUT`.

### CREATE SOURCE fails

| Error | Fix |
|---|---|
| `publication "x" does not exist` | create the publication, or fix the name (it is case-sensitive) |
| `table "s.t" has no primary key and REPLICA IDENTITY is not FULL` | add a primary key, `ALTER TABLE s.t REPLICA IDENTITY FULL`, or exclude the table with `FOR TABLES (...)` |
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

See the [startup failure table](operations.md#recovery). Two failures leave
the data directory unusable without restoring a backup, because the
checkpointed catalog contains a materialized view that can no longer be
planned:

- a table with a quoted mixed-case name was dropped while views used it;
- an upstream schema change removed or renamed a column a materialized view
  uses.

Either restore a copy of the data directory taken before the problem, or
start over with an empty data directory (and drop the old replication slots
upstream). See [Known problems](#known-problems) for how to avoid both.

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
`variance` are cheap regardless of group size (range summaries).

### Disk usage grows

- The input log keeps segments back to `checkpoint GSN − --retention-gsns`
  (and always the current 64 MiB segment).
- Dead index nodes are reclaimed only in sealed segments; the active segment
  (up to 256 MiB) is not collected until it is full.
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
- Upstream: PostgreSQL 14+, tables need a primary key or
  `REPLICA IDENTITY FULL`; publication row filters and column lists are not
  supported; unknown types replicate as text.

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
  `numeric` group key, `DISTINCT`, `min`/`max`), the engine returns the one
  that sorts first in its index, which may differ from the one PostgreSQL
  happens to return.
- In maintained views, expression errors become error records instead of
  failing a statement.
- `current_user`, `session_user`, `user` and `current_catalog` fail with
  `function current_user() does not exist` (and so on); `current_role`,
  `current_schema` and `current_database()` work.

## Known problems

Problems found while writing this documentation (at commit `d80c7f2`), with
workarounds:

| Problem | Effect | Workaround |
|---|---|---|
| Dependency detection lowercases view text (`catalog.rs`, `sql_mentions`) | Dropping a table/view with a quoted mixed-case name succeeds while views use it; the engine fails to start after the next checkpoint | avoid mixed-case quoted names; drop dependent views first |
| Upstream schema change that breaks a materialized view | The view silently stops updating; the engine fails to start after the next checkpoint | drop affected views before dropping/renaming upstream columns |
| Deletes during a re-snapshot after a schema change are skipped | Rows deleted upstream right after a schema change stay in the replica | re-create the source after the change if it matters |
| A restart before a re-snapshot's first chunk completes forgets the re-snapshot | Existing rows keep NULL in columns added upstream | re-create the source |
| A sink drops queued changes while reconnecting | Sink target misses changes after an outage, or after concurrent `CREATE SINK`s race on `ivm_sink_progress` | create `ivm_sink_progress` in advance; create sinks one at a time; resynchronise (see above) after outages |
| Aggregate-group errors are not retracted after a restart | The view keeps failing although the data is fixed | drop and re-create the view |
| Errors in join conditions and probed join inputs are dropped | Rows silently missing instead of an error | avoid expressions that can fail in join conditions |
| A dropped slot is never re-created | Source stuck in `error: replication slot ... does not exist` | drop and re-create the source |
| `AS OF AT LEAST LSN` without sources | waits forever | do not use it without sources |
| `SHOW read_mode` before `SET read_mode` | `unrecognized configuration parameter` | `SET read_mode = fresh` first |
| `current_user`, `session_user`, `user`, `current_catalog` | `function ...() does not exist` | use a literal |
| Ticks can be postponed under continuous input | `now()` and temporal filters lag | none |
| Stop file / signals | no final checkpoint; replay on next start (harmless) | none needed |
| `--no-sync` and OS crashes | lost records, GSN reuse | do not use `--no-sync` for real data |
