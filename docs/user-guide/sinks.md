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

1. The engine takes a snapshot of the relation's current contents at the last
   applied GSN (the *snapshot GSN*) and starts a sink thread. At an engine
   start this happens before the log is replayed, from the checkpoint's
   state: the snapshot GSN is the checkpoint's, and the replayed records
   reach the sink like live ones.
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
   - if the recorded GSN is at or after the snapshot GSN, delivery resumes
     after it;
   - otherwise — no row yet, or the target is behind the snapshot — the
     target is **resynchronised** in one transaction: `DELETE FROM <target>`,
     `INSERT` of the snapshot rows (each row repeated by its multiplicity, 500
     rows per statement), and the progress row set to the snapshot GSN. This
     happens only at the thread's first synchronisation; if on a later
     reconnect the row is missing or behind the snapshot GSN, the sink
     reports `error: sink progress row went backwards` and keeps retrying.

### Delivering changes

For every GSN at which the relation changes, the engine hands the
consolidated change (rows with positive and negative multiplicities) to the
sink thread. The thread batches changes and flushes when more than 1000 rows
are pending or 100 ms have passed since the last flush. A flush is one
transaction:

```sql
BEGIN;
-- for each GSN in order:
--   one DELETE per removed row instance, then
--   INSERTs of the added rows (500 rows per statement)
INSERT INTO ivm_sink_progress (sink, gsn) VALUES ('<name>', <last gsn delivered>)
  ON CONFLICT (sink) DO UPDATE SET gsn = EXCLUDED.gsn;
COMMIT;
```

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

Values are written as typed literals, `'<text output>'::<type>`, or `NULL`.

### Exactly-once delivery

Each flush stores the data and the sink's progress GSN in the same
transaction. If the transaction fails or its outcome is unknown, the thread
waits, reconnects and reads the progress row again: batches at or below the
committed GSN are discarded, the rest are applied again. Changes that arrive
meanwhile are kept. A batch is therefore applied exactly once while the
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
crash, or it was created by an older version) is resynchronised from the
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
  the new columns, like a new sink: from a fresh snapshot, with a
  resynchronisation (its progress is behind that snapshot). The target
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
the target is current, as committed in the progress row), `error: <message>`
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
connection attempt, can take a while). A sink thread that outlives its sink
cannot undo the work of a new sink of the same name: it resumes from the
progress row, which only moves forward. At shutdown the engine waits up to
5 s for its sinks to stop.

## Known problems

See also [Troubleshooting](troubleshooting.md#known-problems).

- The queue of changes from the engine to each sink is unbounded: while a
  sink cannot write (its target is down, or a batch keeps failing), the
  changes it has not written accumulate in memory.
