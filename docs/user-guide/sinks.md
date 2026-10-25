# Sinks: writing results back to PostgreSQL

A *sink* keeps a table in a PostgreSQL database equal to a table or
materialized view of the engine. It writes every change in transactions that
also record how far it got, so that a sink that dies mid-batch neither loses
nor duplicates changes. Code: `crates/ivm/src/sink/postgres.rs`,
`crates/ivm/src/sink/mod.rs`, `crates/ivm/src/coord/ddl.rs`.

## CREATE SINK

```sql
CREATE SINK name FROM relation INTO POSTGRES [ ( ] option [ [ , ] option ... ] [ ) ]

option:
    CONNECTION [ = ] 'conninfo'        -- required
  | TABLE [ = ] 'target_table'         -- required
  | KEY [ = ] ( column [ , ... ] )
```

Examples:

```sql
CREATE SINK revenue_out FROM revenue INTO POSTGRES (
  CONNECTION 'host=reporting dbname=reports user=writer password=...',
  TABLE 'public.revenue_copy',
  KEY (region)
);

CREATE SINK events_out FROM events INTO POSTGRES
  CONNECTION 'postgresql://writer@reporting/reports' TABLE 'events_copy';
```

- `relation` must be a table (local or replicated) or a materialized view
  (`sink source "v" must be a table or materialized view`, SQLSTATE
  `42809`); a [broken](operations.md#recovery) materialized view is refused
  (`55000`).
- `CONNECTION` uses the same connection-string syntax as sources
  ([Sources](sources.md#connection-strings)); no replication privileges are
  needed.
- `TABLE` names the target table. A dotted name (`schema.table`) is split at
  the dots and each part is quoted as an identifier, so names are
  case-sensitive and not lowercased.
- `KEY` lists columns of `relation` that are unique in it. With a key, rows
  are upserted and deleted by key; without one, the target is maintained as a
  multiset. The key is checked before the statement is logged (and again
  when it is applied): an unknown column fails with SQLSTATE `42703`
  (`sink key column "x" does not exist in "v"`), a repeated one with `42701`,
  and a column of a type that cannot be part of a primary key (`json`,
  `jsonpath`, or arrays of them) with `0A000`. Whether the key columns are
  unique is not checked (see
  [Requirements on the target](#requirements-on-the-target)).

The sink starts as soon as the statement is applied, and again after every
engine start. A sink whose relation is a broken materialized view is not
started (its status says why).

## How a sink works

### Start-up and resynchronisation

1. The engine starts a sink thread, which receives every change of the
   relation after the last applied GSN (the *start GSN*). At an engine
   start this happens before the log is replayed: the start GSN is the
   checkpoint's, and the replayed records reach the sink like live ones.
   No snapshot of the relation is taken unless the target needs one
   (below).
2. The thread connects and runs, every time it (re)connects:

   ```sql
   CREATE TABLE IF NOT EXISTS ivm_sink_progress (sink text PRIMARY KEY, gsn bigint NOT NULL);
   CREATE TABLE IF NOT EXISTS <target> (<column> <type>, ... [, PRIMARY KEY (<key columns>)]);
   ```

   The target's column names and types are those of the relation (types as
   `format_type` prints them, for example `numeric(10,2)`,
   `character varying(20)`, `timestamp with time zone`, `integer[]`); the
   primary key is added when `KEY` is given. `ivm_sink_progress` is created
   unqualified, i.e. in the first schema of the role's `search_path`.
3. It reads its progress row (`SELECT gsn FROM ivm_sink_progress WHERE sink = '<name>'`):
   - if the recorded GSN is at or after the start GSN (or the snapshot GSN
     of the sink's last resynchronisation), delivery resumes after it;
   - otherwise — no row yet, or the target is behind the changes the sink
     has — the target is **resynchronised**. This happens only at the
     thread's first synchronisation (and after dropped changes, see
     [Delivering changes](#delivering-changes)); if on a later reconnect the
     row is missing or behind, the sink reports
     `error: sink progress row went backwards` and keeps retrying.

To resynchronise, the sink thread asks the engine for a snapshot. The
engine reads the relation at its last applied GSN (the *snapshot GSN*),
between two records, and sends it to the thread in chunks of 10,000 rows,
after the changes it sent before (the snapshot includes them; the thread
drops them). The thread writes it in one transaction, opened at the first
chunk: `DELETE FROM <target>`, `INSERT`s of the snapshot rows (each row
repeated by its multiplicity, up to 500 rows per statement), and the
progress row set to the snapshot GSN. `SHOW SINKS` shows
`resynchronizing to gsn <n>` meanwhile. If the transaction fails, it is
rolled back, the rest of the snapshot is dropped, and once the thread is
connected again it asks for a new snapshot. The snapshot is not kept after
it is written.

Every transaction a sink writes is sent in as many messages as it needs,
each at most 8 MiB of SQL text (a single statement can be longer; an
`INSERT` holds fewer than 500 rows when they would exceed it): PostgreSQL
rejects messages over 1 GB, which a resynchronisation of a few million rows
in one message used to exceed.

### Delivering changes

For every GSN at which the relation changes, the engine hands the
consolidated change (rows with positive and negative multiplicities) to the
sink thread. The thread batches changes and flushes when more than 1000 rows
are pending or 100 ms have passed since the last flush. A flush is one
transaction of whole GSNs, about 50,000 rows (at least one GSN): a backlog
is written in several transactions, each of which leaves the target as of
a GSN, without waiting in between:

```sql
BEGIN;
SELECT gsn FROM ivm_sink_progress WHERE sink = '<name>' FOR UPDATE;
-- (checked by the sink before it goes on, see below)
-- for each GSN in order:
--   one DELETE per removed row instance, then
--   INSERTs of the added rows (500 rows per statement)
INSERT INTO ivm_sink_progress (sink, gsn) VALUES ('<name>', <last gsn delivered>)
  ON CONFLICT (sink) DO UPDATE SET gsn = GREATEST(ivm_sink_progress.gsn, EXCLUDED.gsn);
COMMIT;
```

The changes a sink has received but not written are bounded: when they
exceed 256 MiB (estimated from their values) and more than one GSN — the
target is unreachable for long, or keeps rejecting a batch — they are
dropped (logged as `more than 256 MiB of changes are waiting to be written;
dropping them and resynchronizing the target from a snapshot`), further
changes are ignored, and once the thread is connected again the target is
resynchronised from a fresh snapshot. (A single GSN's change is kept
whatever its size.)

Every write — a flush, a progress update, a resynchronisation — first locks
the sink's progress row, so that the writers of one sink take turns, and
checks it: a flush or progress update goes on only if the row still holds
the progress this thread last committed (or read), a resynchronisation only
if the row is not after its snapshot GSN. Otherwise another writer has
moved it — a newer sink of the same name, while this thread was stopping
(see [DROP SINK](#monitoring-and-drop-sink)) — and the transaction is rolled
back (`error: sink progress moved to gsn <n> by another writer`). The
progress row itself never moves backwards.

With `KEY`:

- a removed row is `DELETE FROM <target> WHERE k1 = <v1> AND ...`;
- added rows are `INSERT ... ON CONFLICT (<key>) DO UPDATE SET <other
  columns> = EXCLUDED.<column>` (`DO NOTHING` if every column is a key
  column). An update of a view row is a removal followed by an addition with
  the same key.

Without `KEY`:

- a removed row deletes one matching row:
  `DELETE FROM <target> WHERE ctid = (SELECT ctid FROM <target> WHERE c1 IS NOT DISTINCT FROM <v1> AND ... LIMIT 1)`.
  Columns of type `json`, `jsonb`, `jsonpath`, `numeric`, `real` or
  `double precision` (and arrays of them) are matched by their text
  (`c::text IS NOT DISTINCT FROM (<v>)::text`): PostgreSQL has no equality
  for `json` and `jsonpath`, and for the others equality would also match
  another representation of an equal value (`1.5` and `1.50`, `-0` and
  `0`), deleting a row the view still has;
- added rows are plain `INSERT`s.

No index can answer such a condition, so every removed row costs a scan of
the target table: a keyless sink of a large relation that changes often is
slow. Give the sink a `KEY` when the relation has unique columns.

Values are written as typed literals, `'<text output>'::<type>`, or `NULL`.

### Exactly-once delivery

Each flush stores the data and the sink's progress GSN in the same
transaction. If the transaction fails or its outcome is unknown, the thread
waits, reconnects and reads the progress row again: batches at or below the
committed GSN are discarded, the rest are applied again. Changes that arrive
meanwhile are kept (up to the bound above). A batch is therefore applied exactly once while the
engine keeps running. Failed connection attempts and failed writes are
retried after 100 ms, then with a delay doubling up to 10 s while they keep
failing (a batch the target rejects keeps failing until the target is
fixed); a write that succeeds resets the delay.

The progress row records the last GSN the engine delivered to the sink —
a change, or a GSN at which the relation did not change (clock ticks,
other tables) — once everything before it is written: with every flush,
every 5 s while nothing is pending, and when the sink stops. So it follows
the engine's GSN even while the relation does not change.

Across an **engine restart**, sinks start from the checkpoint before the
log is replayed. A target whose progress is at or after the checkpoint's GSN
— after a clean shutdown it is exactly that GSN, as sinks record the last
applied GSN when they stop, before the final checkpoint — resumes after
its progress and receives the replayed changes; it is not rewritten. Only a
target behind the checkpoint (the sink could not write for a while before a
crash, or it was created by an older version) is resynchronised from a
snapshot in one transaction. Changes that the sink received but had not
written when the engine stopped are written when it stops.

## Requirements on the target

- The sink owns the target table's contents: a resynchronisation deletes
  every row, and deletes assume that the table contains exactly the rows the
  sink wrote. Do not write to it from elsewhere.
- If the table already exists, it must have a column for every column of the
  relation, with the same names and compatible types (inserts name all
  columns); additional columns must have defaults or allow NULL. With `KEY`,
  there must be a unique index or constraint on exactly the key columns
  (`ON CONFLICT (<key>)` needs one); the table created by the sink has a
  primary key.
- The key columns must be unique in the relation. Duplicate keys make the
  upsert fail (`ON CONFLICT DO UPDATE command cannot affect row a second
  time`), which stops delivery (the batch is retried until it succeeds),
  or silently merge rows.
- Sink names must be unique among all engines writing into the same database:
  the progress row is keyed by the sink name only.
- The role needs `CREATE` on the schema (the `CREATE TABLE IF NOT EXISTS`
  statements run on every connect), and `SELECT`, `INSERT`, `UPDATE` and
  `DELETE` on the target table and on `ivm_sink_progress`.

## What is delivered

Only data changes. [Error records](temporal-filters-and-errors.md#errors-as-data)
of a materialized view are not delivered; a group that turns into an error is
removed from the view, so the sink deletes its row.

## Schema changes and replacements

- Replacing the relation (a materialized view) with
  `CREATE OR REPLACE MATERIALIZED VIEW` keeps the sink if the new definition
  has the same columns (names, types and type modifiers): the sink receives
  the difference between the old and the new contents at the replacement's
  GSN. A replacement that changes the columns, or makes the view a virtual
  one, is rejected while a sink reads it
  (`cannot replace view "v": sink "s" reads its columns`, SQLSTATE `2BP01`).
- When the relation's columns change anyway — an upstream schema change of
  a replicated table, or a materialized view re-planned after one
  ([Sources](sources.md#upstream-schema-changes)) — the sink restarts with
  the new columns, like a new sink: with a resynchronisation from a fresh
  snapshot (its progress is behind the GSN it starts at). The target
  table is never altered: until it has every new column, with a type the
  values convert to, the resynchronisation fails (for example
  `error: column "c" of relation "t" does not exist`) and is retried. Add
  the columns to the target (or drop the target table, which the sink then
  creates again). Columns that the relation lost stay in the target and are
  left NULL (or at their default) by the inserts; if a `KEY` column is
  gone, the sink cannot start again (`sink key column "k" does not
  exist`): drop it.
- A materialized view that an upstream change invalidates is dropped
  together with its sinks.

## Monitoring and DROP SINK

`SHOW SINKS` shows `name`, `from` (the relation), `table` (the target as
given), and `status`: `starting`, `running (gsn <n>)` (the GSN up to which
the target is current, as committed in the progress row),
`resynchronizing to gsn <n>` (a snapshot is being written),
`error: <message>`
while it cannot connect or its writes fail (the error of the last attempt,
shown until a write succeeds: reconnecting alone does not clear the error of
a batch the target rejects), `error: broken: <reason>` (the relation is a
broken materialized view; the sink is not started), or `stopped`.

```sql
DROP SINK [ IF EXISTS ] name
```

stops the sink thread. The target table and the progress row are left in
place. A sink is a dependent object of its relation: `DROP TABLE` /
`DROP MATERIALIZED VIEW` of the relation fails unless `CASCADE` is given,
which drops the sink too, and `DROP SOURCE` of a replicated table's source
drops the sink.

The engine does not wait for the sink thread: the thread stops by itself as
soon as it sees the request (a sink retrying its connection, or stuck in a
connection attempt or a long write, can take a while). A sink thread that
outlives its sink cannot undo the work of a new sink of the same name: a
write it has in progress holds the progress row's lock, so the new sink's
resynchronisation waits for it and then rewrites the target, and a write it
starts after the new sink wrote fails the progress check. At shutdown the
engine waits up to 5 s for its sinks to stop.

## Known problems

See also [Troubleshooting](troubleshooting.md#known-problems).

- The queue of changes from the engine to each sink is unbounded: the sink
  thread takes changes from it between its transactions (and between the
  chunks of a snapshot it writes), so what waits there is bounded only by
  what arrives during one transaction. A snapshot is read on the engine
  thread in one go, and its chunks wait in that queue until the sink has
  written them.
