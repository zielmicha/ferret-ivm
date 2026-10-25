# Sources: replicating PostgreSQL

A *source* replicates the tables of one PostgreSQL publication into the
engine through logical replication (the `pgoutput` plugin). The engine is a
replica, not a stream consumer: it keeps the current contents of every
replicated table in a primary index, turns every upstream change into an
upsert or delete by primary key, and logs it with a GSN before anything is
derived from it.

Code: `crates/ivm/src/source/postgres.rs` (replication, snapshots),
`crates/ivm/src/coord/ddl.rs` (catalog side), `crates/pgproto` (wire
protocol and replication client).

## Upstream requirements

### Server version and configuration

- **PostgreSQL 14 or newer.** The source requests the `pgoutput` option
  `messages` (PostgreSQL 14+) and calls `pg_current_snapshot()`
  (PostgreSQL 13+). Development and all tests use PostgreSQL 17.
- **`wal_level = logical`**, which needs a server restart.
- **`max_replication_slots`** and **`max_wal_senders`**: each source uses one
  logical slot and one walsender connection while running.
- Each source additionally opens ordinary connections: one while
  `CREATE SOURCE` / `ALTER SOURCE ... REFRESH` discovers tables (from the
  session that runs the statement) and one for snapshot queries.
- Consider **`max_slot_wal_keep_size`**: if the engine is stopped (or cannot
  start), its slot retains WAL without bound.

### Publications

The source reads the tables of a publication from `pg_publication_tables`,
so publications created with `FOR TABLE`, `FOR TABLES IN SCHEMA` and
`FOR ALL TABLES` all work. Changes of the kinds the publication does not
publish (`publish = 'insert'` etc.) never arrive.

Not supported: publication column lists and row filters (discovery reads all
columns of a table from `pg_attribute`, and the initial snapshot reads all
rows), and partitioned tables (untested).

### Primary keys and replica identity

Every replicated table needs either

- a **primary key** — the replica identity can stay `DEFAULT`; before-images
  for updates and deletes come from the engine's stored copy of the row, not
  from the WAL; or
- **`REPLICA IDENTITY FULL`** — the table is then replicated as a multiset
  (duplicates allowed), and updates/deletes are matched on all columns.

Anything else (no primary key, replica identity `DEFAULT`, `NOTHING` or
`USING INDEX`) is refused when the source is created:

```
ERROR:  table "public.nokey" has no primary key and REPLICA IDENTITY is not FULL
```

The check covers every table of the publication (after the `FOR TABLES`
filter), so one unsuitable table makes `CREATE SOURCE` fail.

### Privileges

The role in the connection string needs:

- the `REPLICATION` attribute (or superuser), for the replication connection
  and `CREATE_REPLICATION_SLOT`;
- `SELECT` on every published table (initial snapshots);
- read access to `pg_catalog` (default) for discovery, and `EXECUTE` on
  `pg_logical_emit_message` and `pg_current_snapshot` (granted to `PUBLIC`
  by default).

A `LOGIN REPLICATION` role with `SELECT` on the tables is sufficient.

### Connection strings

`CONNECTION` takes a libpq-style string, either `key=value` pairs (values may
be single-quoted, with `\'` and `\\` escapes) or a `postgres://` /
`postgresql://` URI. Implemented in `crates/pgproto/src/client.rs`:

- Keywords used: `host` (a hostname, an IP address, a Unix-socket directory
  starting with `/`, or `@name` for a Linux abstract socket), `hostaddr`,
  `port` (default 5432), `user` (default `$USER`), `password`, `dbname`
  (default: the user name), `application_name`,
  `fallback_application_name`, `connect_timeout`, `options`,
  `client_encoding`.
- Authentication: trust, cleartext password, MD5, SCRAM-SHA-256 (without
  channel binding).
- No TLS: `sslmode=require`/`verify-*`, `ssl=true` and
  `channel_binding=require` are rejected; `sslmode=disable|allow|prefer`
  connect without TLS. Many other libpq keywords are accepted and ignored;
  unknown keywords are an error. Multiple hosts are not supported.
- `PG*` environment variables and `~/.pgpass` are **not** consulted.
- The source adds `TimeZone=UTC` and `application_name=ivm` unless the
  connection string sets them.

The connection string is stored in the catalog in clear text (and is visible
through `SHOW CREATE VIEW`, `ivm.objects` and `ivm.catalog_history`).

## CREATE SOURCE

```sql
CREATE SOURCE name FROM POSTGRES [ ( ] option [ [ , ] option ... ] [ ) ]

option:
    CONNECTION [ = ] 'conninfo'          -- required
  | PUBLICATION [ = ] 'publication'      -- required
  | SLOT [ = ] 'slot_name'
  | FOR ALL TABLES                       -- default
  | FOR TABLES ( table [ , ... ] )
```

Examples:

```sql
CREATE SOURCE pg FROM POSTGRES
  CONNECTION 'host=db port=5432 dbname=app user=repl password=secret'
  PUBLICATION 'app_pub';

CREATE SOURCE crm FROM POSTGRES (
  CONNECTION 'postgresql://repl@db/crm',
  PUBLICATION 'crm_pub',
  SLOT 'crm_ivm',
  FOR TABLES (accounts, public.contacts)
);
```

Options may be separated by commas and wrapped in parentheses; both are
optional. `name` is an identifier (unquoted names are lowercased, quoted
names keep their case).

What happens:

1. **Discovery** (in the session, before anything is logged): the session
   connects to the upstream, checks that the publication exists
   (`publication "x" does not exist` otherwise), and reads for every table of
   the publication its columns (name, type OID, typmod, `NOT NULL`), its
   primary key columns in key order, and its replica identity.
   `FOR TABLES (...)` keeps only tables whose name matches an entry, either
   as `table` or as `schema.table`; entries that match nothing are silently
   ignored.
2. **Validation**: no other object may be called like the source; every
   table needs a primary key or `REPLICA IDENTITY FULL`; table names must not
   collide with existing objects or with each other (the engine has one
   namespace, see below).
3. **Logging**: the statement and the discovered schemas (as JSON) are
   written to the input log as one DDL record. Replay never contacts the
   upstream again, so it reconstructs the same catalog.
4. **Apply**: the source object and one table object per upstream table are
   created (empty), and the source thread starts. `CREATE SOURCE` returns at
   this point; the initial snapshot runs in the background.

### Table names

Replicated tables get their upstream **table name** without the schema
(`public.orders` becomes `orders`). Sources, tables, views, materialized
views, indexes and sinks share one namespace, so `CREATE SOURCE` fails with
`relation "orders" already exists` if any object (including a table of
another source) already has that name, or if two published tables in
different schemas have the same name.

In queries, a schema-qualified name `s.t` is accepted when `s` is `public`
or the upstream schema of some replicated table; it resolves to the object
named `t` (or to the replicated table whose upstream name is `s.t`). Other
schemas are unknown (`relation "nope.t" does not exist`), except the system
schemas `pg_catalog`, `information_schema` and `ivm`
([Introspection](introspection.md)).

### Type mapping

Column types are mapped by upstream type OID (`map_type` in
`source/postgres.rs`):

| Upstream type | Engine type |
|---|---|
| `boolean`, `smallint`, `integer`, `bigint`, `real`, `double precision`, `numeric(p,s)`, `text`, `varchar(n)`, `char(n)`, `name`, `bytea`, `date`, `time`, `timestamp`, `timestamptz`, `interval`, `uuid`, `json`, `jsonb`, `jsonpath`, `oid` | the same type, with its typmod |
| `"char"` | `character` |
| the array types of the above (`int[]`, `text[]`, ..., except `"char"[]`) | the same array type |
| anything else (`inet`, `money`, `timetz`, enums, domains, composite types, `xml`, ranges, ...) | `text`, holding the upstream text output |

Values arrive in text format and are parsed with the engine's input
functions (which follow PostgreSQL 17). Nullability comes from `attnotnull`.

### Slots

Each source owns one logical replication slot using `pgoutput`:

- The default name is `ivm_<name>_<id>`: the source name lowercased, every
  character other than `a-z0-9` replaced by `_`, truncated so that the whole
  name fits PostgreSQL's 63-byte limit, and suffixed with the source's
  catalog object id. For example source `pg` with id 1000 gets
  `ivm_pg_1000`; source `"Second-Src"` gets `ivm_second_src_1011`. Because
  the id is unique, a re-created source never reuses a slot that is still
  being dropped.
- `SLOT 'name'` chooses the name explicitly.
- The slot is created when the source first starts streaming
  (`CREATE_REPLICATION_SLOT ... LOGICAL pgoutput NOEXPORT_SNAPSHOT`). If a
  slot with that name already exists, it is reused as is. The fact that the
  slot was created is recorded in the source's logged state.
- Streaming uses `START_REPLICATION SLOT ... LOGICAL <lsn>` with options
  `proto_version '1'`, `publication_names '"<publication>"'`,
  `messages 'true'`.

### Acknowledgements and WAL retention

The source acknowledges the slot (standby status update; write, flush and
apply positions all set to the same LSN) with the end LSN of the last
upstream transaction whose changes are **durable in the engine's input
log** — not after a checkpoint. While nothing is in flight it acknowledges
the server's current WAL end from keepalives. Status updates are sent at
least once per second, and immediately when a read waits for an LSN
(`AS OF AT LEAST LSN`).

Consequently WAL is retained upstream only for the ingestion lag (usually
milliseconds) while the engine runs. While the engine is stopped, or after it
fails to start, the slot holds back WAL removal indefinitely.

## Initial snapshot

After the slot is created, the source streams changes and, in between,
copies each table that has not been snapshotted yet, one table at a time
(lowest catalog object id first, which is discovery order: by schema, then
table name):

- **Tables with a primary key** are read in chunks of 10,000 rows in primary
  key order:
  `SELECT <columns> FROM <schema>.<table> [WHERE (pk) > (<last key>)] ORDER BY <pk> LIMIT 10000`
  inside a `REPEATABLE READ` transaction (string key columns compare with
  `COLLATE "C"`). Chunks are ordered against the change stream with DBLog
  watermarks (`pg_logical_emit_message` low and high markers), so the result
  equals a consistent copy. Every chunk becomes one logged commit; progress
  (the last copied key) is logged too, so a restart resumes with the next
  chunk. The algorithm is described in
  [Recovery and snapshots](../internals/recovery.md#the-dblog-snapshot-algorithm).
- **Tables with `REPLICA IDENTITY FULL`** (no key) are read in one query.

While a table's snapshot is incomplete:

- **Fresh reads** (the default) of the table — directly or through views —
  wait until the snapshot is complete: a partially loaded table is no state
  the upstream ever had.
- **`AS OF AT LEAST LSN`** reads wait as well: the source's LSN frontier does
  not advance until all of its tables are complete.
- **`read_mode = latest`** and **`AS OF <gsn>`** reads do not wait and see the
  partial contents.
- Materialized views over the table are maintained throughout; they show the
  partial contents too.

`SHOW SOURCES` reports the status `snapshotting <schema>.<table>` during a
snapshot and `streaming` afterwards.

## Streaming changes

For every upstream transaction the source logs one *commit* record holding
the transaction's changes (empty transactions are not logged). Mapping of
`pgoutput` messages:

| Message | Table with primary key | `REPLICA IDENTITY FULL` table |
|---|---|---|
| Insert | upsert of the new row | insert of one row |
| Update | upsert of the new row; if the key changed, a delete of the old key first | remove one instance of the old row, insert the new row |
| Delete | delete by key | remove one instance of the row |
| Truncate | truncate (all rows of the table go away) | truncate |
| Relation | schema check (see below) | schema check |
| Message with prefix `ivm` | snapshot watermark | snapshot watermark |

Unchanged TOASTed values (`pgoutput` sends a placeholder for large values
that an update did not touch) are filled from the stored row; if a row is
not present in the replica, they become NULL (with a warning in the log).

Upstream transactions are applied atomically: all changes of a transaction
get one GSN and are consolidated before any view sees them. Different
sources have no relative order other than their arrival order, which is
fixed once logged.

### Restarts and outages

- **Upstream restart or network failure**: the source thread logs a warning,
  shows `error: <message>` in `SHOW SOURCES`, and reconnects with exponential
  backoff (200 ms, doubling, at most 10 s). It resumes streaming from the end
  LSN of the last commit that is durable in the input log; transactions at
  or below that LSN that the server sends again are skipped. An interrupted
  snapshot chunk is discarded and redone.
- **Engine restart**: sources are restarted after recovery from the logged
  per-source state (slot created, snapshot progress) and the LSN of the last
  logged commit.

### TRUNCATE

`TRUNCATE` of a published table (published by default) removes all rows of
the replicated table at the GSN of the truncating transaction; views see one
retraction of every row. If a snapshot chunk of that table is in progress,
its rows are discarded.

### Upstream schema changes

`pgoutput` describes a table with a Relation message before the first change
that follows a schema change. When a Relation message differs from the known
table — a different number of columns, or a different column name or mapped
type at some position — the source:

1. logs a *schema* record with the new column list (names, types, typmods;
   nullability is kept for columns that existed before, new columns are
   nullable) and the new key (the Relation message's key columns, or the
   previous primary key matched by name when the table has
   `REPLICA IDENTITY FULL`);
2. the engine rebuilds the table's primary index in the new shape: columns
   are matched **by name**; existing values are cast to the new type (a
   failing cast gives NULL); new columns are NULL. The table's version is
   incremented;
3. dependent materialized views are re-planned against the new definition
   and swapped at that GSN;
4. the source re-snapshots the table from the beginning, which fills the new
   columns of existing rows. Fresh reads wait for this snapshot.

Schema changes that do not change the column names or mapped types are
invisible to the engine: new indexes, defaults, constraints, and typmod-only
changes such as `varchar(10)` to `varchar(20)`. A renamed column is a
dropped column plus a new one: its values are NULL until the re-snapshot
fills them, and materialized views that use the old name cannot be
re-planned (see below).

Known problems (see [Troubleshooting](troubleshooting.md#known-problems)):

- Rows deleted upstream between the schema change and the moment the
  re-snapshot reaches them stay in the replica: the re-snapshot only
  upserts the rows it reads.
- A materialized view that cannot be re-planned (for example it uses a
  dropped column) keeps its old dataflow and **stops being updated**; the
  failure is only logged (`view ... could not be re-planned after a schema
  change`). After the next checkpoint the engine **fails to start** because
  the view cannot be planned during recovery. Drop such views before making
  breaking upstream changes.

### Tables added to or removed from the publication

Tables added to the publication later are not picked up automatically; use
`ALTER SOURCE ... REFRESH`. Tables removed from the publication, or dropped
upstream, simply stop receiving changes; the replicated table and its
contents stay until the source is dropped.

## ALTER SOURCE ... REFRESH

```sql
ALTER SOURCE name REFRESH
```

Re-runs discovery with the source's connection string, publication and
`FOR TABLES` filter, and adds every table that the source does not replicate
yet (matched by upstream schema and name). New tables are validated like in
`CREATE SOURCE`, created empty and snapshotted in the background. The command
tag is `ALTER SOURCE <n>`, where `<n>` is the number of tables added.
Existing tables are not touched; their schema changes arrive through the
stream.

## DROP SOURCE

```sql
DROP SOURCE [ IF EXISTS ] name [ CASCADE ]
```

Dropping a source **always cascades**: the source's tables and every object
that depends on them — views, materialized views, indexes and sinks,
transitively — are dropped with it. `CASCADE` is accepted and has no effect.

The source thread is stopped and the replication slot is dropped in the
background (`DROP_REPLICATION_SLOT`, up to 20 attempts 250 ms apart; a slot
that no longer exists counts as dropped). If the upstream is unreachable
during those five seconds, or the engine stops before the drop succeeds, the
slot remains and must be dropped by hand (see below). Replaying a
`DROP SOURCE` from the log does not try to drop the slot again.

## Monitoring

`SHOW SOURCES` lists every source:

| Column | Meaning |
|---|---|
| `name` | source name |
| `publication` | publication name |
| `slot` | replication slot name |
| `tables` | number of replicated tables |
| `lsn` | the source's LSN frontier (`X/Y`): every upstream commit at or below it has been logged. It stays `0/0` until all of the source's tables have completed their initial snapshot, and does not advance during a re-snapshot. |
| `status` | `starting`, `streaming`, `snapshotting <schema>.<table>`, `error: <message>` (while reconnecting), or `stopped` (the source thread could not be started) |

`SHOW FRONTIERS` repeats the frontier per source; `SHOW TABLES` shows each
table's source and row count. Source warnings and errors are written to the
server log (`RUST_LOG=ivm::source=debug` for more).

## Cleaning up upstream slots

An engine that is stopped for good, a deleted data directory, or a failed
background drop leave a slot behind that retains WAL. List and drop them in
PostgreSQL:

```sql
SELECT slot_name, active, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots WHERE slot_name LIKE 'ivm\_%';

SELECT pg_drop_replication_slot('ivm_pg_1000');
```

A slot can only be dropped while no walsender uses it (`active = false`);
stop the engine (or drop the source) first.
