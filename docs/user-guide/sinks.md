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

- `relation` must be a table (local or replicated) or a materialized view.
- `CONNECTION` uses the same connection-string syntax as sources
  ([Sources](sources.md#connection-strings)); no replication privileges are
  needed.
- `TABLE` names the target table. A dotted name (`schema.table`) is split at
  the dots and each part is quoted as an identifier, so names are
  case-sensitive and not lowercased.
- `KEY` lists columns of `relation` that are unique in it. With a key, rows
  are upserted and deleted by key; without one, the target is maintained as a
  multiset.

The sink starts as soon as the statement is applied, and again after every
engine start.

## How a sink works

### Start-up and resynchronisation

1. The engine takes a snapshot of the relation's current contents at the last
   applied GSN (the *snapshot GSN*) and starts a sink thread.
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
     rows per statement), and the progress row set to the snapshot GSN.

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
INSERT INTO ivm_sink_progress (sink, gsn) VALUES ('<name>', <last gsn>)
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
  `DELETE FROM <target> WHERE ctid = (SELECT ctid FROM <target> WHERE c1 IS NOT DISTINCT FROM <v1> AND ... LIMIT 1)`;
- added rows are plain `INSERT`s.

Values are written as typed literals, `'<text output>'::<type>`, or `NULL`.

### Exactly-once delivery

Each flush stores the data and the sink's progress GSN in the same
transaction. If the connection fails or the commit's outcome is unknown, the
thread reconnects (backoff from 100 ms, doubling, at most 10 s) and reads the
progress row again: batches at or below the committed GSN are discarded,
the rest are applied again. A batch is therefore applied exactly once while
the engine keeps running.

Across an **engine restart** the sink does not replay the individual changes
between its committed GSN and the restart: the restarted sink takes a new
snapshot, and if the target is behind that snapshot it is resynchronised
from the snapshot in one transaction. This is the usual case: the progress
row records the last GSN at which the relation changed, while the snapshot
GSN is the last applied GSN, which includes later inputs such as clock
ticks. The target is consistent at every committed transaction, but a
restart rewrites the whole table.

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
  time`) or silently merge rows.
- Sink names must be unique among all engines writing into the same database:
  the progress row is keyed by the sink name only.
- The role needs `CREATE` on the schema (the `CREATE TABLE IF NOT EXISTS`
  statements run on every connect), and `SELECT`, `INSERT`, `UPDATE` and
  `DELETE` on the target table and on `ivm_sink_progress`.

## What is delivered

Only data changes. [Error records](temporal-filters-and-errors.md#errors-as-data)
of a materialized view are not delivered; a group that turns into an error is
removed from the view, so the sink deletes its row. When the relation is a
materialized view that is replaced with `CREATE OR REPLACE`, the sink
receives the difference between the old and the new contents at the
replacement's GSN.

## Monitoring and DROP SINK

`SHOW SINKS` shows `name`, `from` (the relation), `table` (the target as
given), and `status`: `starting`, `running (gsn <n>)` (the last committed
GSN), `error: <message>` while reconnecting, or `stopped`.

```sql
DROP SINK [ IF EXISTS ] name
```

stops the sink thread. The target table and the progress row are left in
place. A sink is a dependent object of its relation: `DROP TABLE` /
`DROP MATERIALIZED VIEW` of the relation fails unless `CASCADE` is given,
which drops the sink too, and `DROP SOURCE` of a replicated table's source
drops the sink.

## Known problems

- While a sink is reconnecting, each retry takes one queued message off its
  channel to check for a stop request and discards it if it is a change, so
  changes that arrive during a connection outage can be **lost** until the
  affected rows change again. Concurrent sinks that create
  `ivm_sink_progress` at the same moment can trigger this: PostgreSQL may
  fail one of the concurrent `CREATE TABLE IF NOT EXISTS` statements with
  `duplicate key value violates unique constraint "pg_type_typname_nsp_index"`.
  Create the progress table beforehand, or create sinks one at a time. See
  [Troubleshooting](troubleshooting.md#known-problems).
- Every engine restart resynchronises every sink (see above).
