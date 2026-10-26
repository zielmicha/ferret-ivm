# Sources: replicating PostgreSQL

A *source* replicates the tables of one PostgreSQL publication into the
engine through logical replication (the `pgoutput` plugin). The engine is a
replica, not a stream consumer: it keeps the current contents of every
replicated table in a primary index, turns every upstream change into an
upsert or delete by key (see
[Primary keys and replica identity](#primary-keys-and-replica-identity)),
and logs it with a GSN before anything is derived from it.

Code: `crates/ivm/src/source/postgres.rs` (replication, snapshots),
`crates/ivm/src/coord/ddl.rs` (catalog side), `crates/pgproto` (wire
protocol and replication client).

## Upstream requirements

### Server version and configuration

- **PostgreSQL 14 or newer.** The source requests the `pgoutput` option
  `messages` (PostgreSQL 14+) and calls `pg_current_snapshot()`
  (PostgreSQL 13+). Development and all tests use PostgreSQL 17.
- **`wal_level = logical`**, which needs a server restart.
- **A database encoding other than `SQL_ASCII`.** Values are replicated as
  UTF-8 text; a `SQL_ASCII` database does not validate text and could hold
  bytes that are not UTF-8, so `CREATE SOURCE` (and every connection of the
  source) fails with `the upstream database's encoding is SQL_ASCII ...`.
  Other server encodings are converted to UTF-8 by PostgreSQL.
- **`max_replication_slots`** and **`max_wal_senders`**: each source uses one
  logical slot and one walsender connection while running.
- Each source additionally opens ordinary connections: one while
  `CREATE SOURCE` / `ALTER SOURCE ... REFRESH` discovers tables (from the
  session that runs the statement) and one for snapshot queries, opened by
  the source's copy thread when the source has a table to copy and then
  kept open (idle between chunks) while the source runs.
- Consider **`max_slot_wal_keep_size`**: if the engine is stopped (or cannot
  start), its slot retains WAL without bound. With the limit, PostgreSQL
  invalidates a slot that falls too far behind; the source then re-creates
  it and copies all its tables again (see [Slots](#slots)).

### Publications

The source reads the tables of a publication from `pg_publication_tables`,
so publications created with `FOR TABLE`, `FOR TABLES IN SCHEMA` and
`FOR ALL TABLES` all work. Changes of the kinds the publication does not
publish (`publish = 'insert'` etc.) never arrive.

Not supported: publication column lists and row filters (discovery reads all
columns of a table from `pg_attribute`, and the initial snapshot reads all
rows), and partitioned tables (untested). With a column list, the table's
first streamed change looks like a schema change that drops the unpublished
columns; with a row filter, snapshots and re-snapshots copy rows the filter
excludes.

### Primary keys and replica identity

Every replicated table needs a key by which the WAL identifies the old row
of updates and deletes:

- a **primary key**, with the replica identity `DEFAULT` (`FULL` and
  `NOTHING` also work); before-images for updates and deletes come from the
  engine's stored copy of the row, not from the WAL, and the old key the WAL
  carries for deletes and key changes is the primary key; or
- **`REPLICA IDENTITY USING INDEX`** — the table (with or without a primary
  key) is keyed by the index's columns, because they are all that deletes
  and key changes carry of the old row (PostgreSQL requires the index to be
  unique, not partial and on `NOT NULL` columns); or
- **`REPLICA IDENTITY FULL`** without a primary key — the table is then
  replicated as a multiset (duplicates allowed), and updates/deletes are
  matched on all columns. (A `FULL` table that has a primary key is keyed by
  the primary key.)

A table with none of these is refused when the source is created:

```
ERROR:  table "public.nokey" has no primary key and REPLICA IDENTITY is not FULL
```

The check covers every table of the publication (after the `FOR TABLES`
filter), so one unsuitable table makes `CREATE SOURCE` fail. (With
`NOTHING`, PostgreSQL itself rejects updates and deletes of a table that is
published for them.) Changes of the replica identity or of the primary key
after the source was created are handled as described in
[Upstream schema changes](#upstream-schema-changes).

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
  starting with `/`, or `@name` for a Linux abstract socket; default
  `localhost` over TCP), `hostaddr`, `port` (default 5432), `user` (default
  `$USER`, else `$LOGNAME`, else `postgres`), `password`, `dbname`
  (default: the user name), `application_name`,
  `fallback_application_name`, `connect_timeout`, `options`,
  `client_encoding` (sources require `UTF8`, the default).
- Authentication: trust, cleartext password, MD5, SCRAM-SHA-256 (without
  channel binding).
- No TLS: `sslmode=require`/`verify-*`, `ssl=true`, `gssencmode=require`
  and `channel_binding=require` are rejected; `sslmode=disable|allow|prefer`
  connect without TLS. Many other libpq keywords are accepted and ignored;
  unknown keywords are an error. Multiple hosts are not supported.
- `PG*` environment variables and `~/.pgpass` are **not** consulted.
- The source's sessions always use the time zone UTC (a `-c TimeZone=...`
  in `options` does not change it), and `application_name` is `ivm` unless
  the connection string sets it.

The connection string is stored in the catalog in clear text (and is visible
through `SHOW CREATE VIEW`, `ivm.objects` and `ivm.catalog_history`, and in
the stored statements the web UI shows; only its sources page masks
passwords).

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
   key columns in key order (those of the replica identity index, else of
   the primary key), and its replica identity.
   `FOR TABLES (...)` keeps only tables whose name matches an entry, either
   as `table` or as `schema.table`; entries that match nothing are silently
   ignored.
2. **Validation**: no other object may be called like the source
   (`source "x" already exists`), and no upstream table either; every
   table needs a key or `REPLICA IDENTITY FULL`; table names must not
   collide with existing objects or with each other (the engine has one
   namespace, see below).
3. **Logging**: the statement and the discovered schemas (as JSON, with the
   data directory's instance id for the default slot name) are written to
   the input log as one DDL record. Replay never contacts the upstream
   again, so it reconstructs the same catalog.
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
A streamed change with a value that cannot be converted fails its table,
not the source: the table's changes in that transaction are dropped and it
is copied again from an empty table (logged as `a change to table s.t
cannot be applied: <message>; copying the table again, streaming the other
tables`); while the value stays upstream, the copy fails too and is retried
like that of an [unreadable table](#tables-added-to-or-removed-from-the-publication).

### Slots

Each source owns one logical replication slot using `pgoutput`:

- The default name is `ivm_<name>_<instance>_<id>`: the source name
  lowercased, every character other than `a-z0-9` replaced by `_`, truncated
  so that the whole name fits PostgreSQL's 63-byte limit, then the data
  directory's instance id (8 hex digits, chosen randomly at its first
  `CREATE SOURCE`, stored in the catalog and logged with every
  `CREATE SOURCE`) and the source's catalog object id. For example source
  `pg` with id 1000 gets `ivm_pg_3f9a1c2e_1000`; source `"Second-Src"` gets
  `ivm_second_src_3f9a1c2e_1011`. Because the object id is unique, a
  re-created source never reuses a slot that is still being dropped; because
  of the instance id, engines whose catalogs are alike (same source names and
  ids) do not share slots when they replicate the same database. Sources
  created by versions without the instance id keep their names
  (`ivm_<name>_<id>`).
- `SLOT 'name'` chooses the name explicitly.
- The slot is created when the source first starts streaming
  (`CREATE_REPLICATION_SLOT ... LOGICAL pgoutput NOEXPORT_SNAPSHOT`). If a
  slot with that name already exists, it is reused as is. The fact that the
  slot was created is recorded in the source's logged state.
- If the slot later turns out to be gone — dropped upstream (`replication
  slot "..." does not exist`) or invalidated because it retained more WAL
  than `max_slot_wal_keep_size` allows (`can no longer get changes from
  replication slot`) — upstream changes since the last acknowledged position
  may be lost. The source logs an error, resets the snapshot state of all its
  tables (logged before the slot exists again, so a restart in between still
  copies them), drops an invalidated slot, creates the slot again and copies
  every table again, each starting from an empty table so that rows deleted
  in the meantime go away too. Until a table is complete, fresh reads of it
  wait, as during the initial snapshot.
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
milliseconds) while the engine runs and the source is streaming. While the
engine is stopped, after it fails to start, or while the source cannot
stream (it keeps reconnecting with `error: ...`), the slot holds back WAL
removal indefinitely. (A table the source cannot copy does not stop the
stream, see [below](#tables-added-to-or-removed-from-the-publication).)
When the records waiting to be applied fill their part of the memory budget
(an eighth of `--memory-mb`, in bytes of memory), sources stop reading the
replication stream until the engine catches up (also each one with 10,000
of its records waiting to be durable), and the upstream keeps the WAL
meanwhile. Their sessions stay up: they go on sending status updates.

## Initial snapshot

After the slot is created, the source streams changes and, at the same
time, copies each table that has not been snapshotted yet, one table at a
time (lowest catalog object id first, which is discovery order: by schema,
then table name). The copy runs on a thread of its own (`ivm-copy-<source
id>`), on the snapshot connection, while the replication connection keeps
streaming:

- **Tables with a key** are read in chunks of 10,000 rows in the order of
  their key index (the primary key's, or the replica identity index's):
  `SELECT <columns> FROM <schema>.<table> [WHERE (<key>) > ('<last key value>', ...)] ORDER BY <key> LIMIT 10000`
  inside a `REPEATABLE READ` transaction. The key columns are compared as
  they are, under their own collation, so the index serves every chunk as
  a range scan, also for text keys under a linguistic collation and for
  types replicated as text (enums, `inet`, ...). (Chunks used to be ordered
  by `col::text COLLATE "C"`, which no index serves unless the collation is
  C: every chunk scanned and sorted the whole table.) Chunks are ordered
  against the change stream with DBLog watermarks
  (`pg_logical_emit_message` low and high markers), so the result equals a
  consistent copy; a chunk is only read once its snapshot contains every
  transaction the stream has already decoded (otherwise it is retried
  100 ms later). Every chunk becomes one logged commit; progress (the last
  copied key) is logged too, so a restart resumes with the next chunk. The
  algorithm is described in
  [Recovery and snapshots](../internals/recovery.md#the-dblog-snapshot-algorithm).
- **Tables with `REPLICA IDENTITY FULL`** (no key) are read in one
  `REPEATABLE READ` transaction through a cursor, in pieces of 10,000 rows.
  The copy replaces the table's contents: a commit truncating the table,
  then a commit per piece as it arrives. Changes the copy's snapshot does
  not contain (made while it is read) apply after its last piece.

A chunk that takes long, or waits upstream (for a lock, say), holds up
neither the other tables' changes nor the replication session: the source
goes on streaming and sending status updates. If the snapshot connection
fails, no chunk is read for 1 s, doubling up to 60 s while it keeps
failing; the stream goes on, and `SHOW SOURCES` shows `error: cannot read
snapshot chunks: <message>; retrying every <n>s, streaming`.

While a table's snapshot is incomplete:

- **Fresh reads** (the default) of the table — directly or through views —
  wait until the snapshot is complete: a partially loaded table is no state
  the upstream ever had.
- **`AS OF AT LEAST LSN`** reads wait as well: the source's LSN frontier does
  not advance until all of its tables are complete.
- **`read_mode = latest`** and **`AS OF <gsn>`** reads do not wait and see the
  partial contents: the rows copied so far, and the rows the stream
  changed since the table's copy started.
- Materialized views over the table are maintained throughout; they show the
  partial contents too.

`SHOW SOURCES` reports the status `snapshotting <schema>.<table>` while a
chunk of that table is in flight and `streaming` otherwise.

## Streaming changes

For every upstream transaction the source logs one *commit* record holding
the transaction's changes (empty transactions are not logged). Mapping of
`pgoutput` messages:

| Message | Table with a key | `REPLICA IDENTITY FULL` table |
|---|---|---|
| Insert | upsert of the new row | insert of one row |
| Update | upsert of the new row; if the key changed, a delete of the old key first | remove one instance of the old row, insert the new row |
| Delete | delete by key | remove one instance of the row |
| Truncate | truncate (all rows of the table go away) | truncate |
| Relation | schema check (see below) | schema check |
| Message with prefix `ivm` | snapshot watermark | snapshot watermark |

Unchanged TOASTed values (`pgoutput` sends a placeholder for large values
that an update did not touch) are filled from the stored row — for an
update that changes the key, from the row stored under the old key; while
a snapshot chunk holding the row is in flight, from the chunk's copy. If
there is no such row, they become NULL (with a warning in the log); while
the table is being copied, such an update of a row that has not been
copied yet changes nothing instead, as the row's copy brings all of it (a
key change whose new key may have been copied already has that key read
again before the table counts as copied).

Upstream transactions are applied atomically: all changes of a transaction
get one GSN and are consolidated before any view sees them. Different
sources have no relative order other than their arrival order, which is
fixed once logged.

### Restarts and outages

- **Upstream restart or network failure**: the source thread logs a warning,
  shows `error: <message>` in `SHOW SOURCES`, and reconnects with exponential
  backoff: starting at 200 ms and doubling up to 10 s while sessions keep
  failing; a session that replicated an upstream transaction, or stayed
  connected for 10 s, resets the delay to 200 ms. It resumes streaming from the end
  LSN of the last commit that is durable in the input log; transactions at
  or below that LSN that the server sends again are skipped. An interrupted
  snapshot chunk is discarded and redone.
- **Engine restart**: sources are restarted once recovery has replayed the
  log, from the logged per-source state (slot created, snapshot progress)
  and the LSN of the last logged commit.

### TRUNCATE

`TRUNCATE` of a published table (published by default) removes all rows of
the replicated table at the GSN of the truncating transaction; views see one
retraction of every row. If a snapshot chunk of that table is in flight,
the rows it read before the truncation are discarded (a chunk read after
it keeps its rows).

### Upstream schema changes

`pgoutput` describes a table with a Relation message before the first change
that follows a schema change (and at the start of every replication session).
When a Relation message differs from the known table — a different number of
columns, a different column name or mapped type at some position, or a
different key — the source:

1. resets the table's snapshot state and logs it (logged first, so that a
   restart in between copies the table in its old shape and detects the
   change again);
2. logs a *schema* record with the new column list (names, types, typmods;
   nullability is kept for columns that existed before, new columns are
   nullable, key columns are `NOT NULL`) and the new key (see below);
3. the engine rebuilds the table's primary index in the new shape: columns
   are matched **by name**; existing values are cast to the new type (a
   failing cast gives NULL); new columns are NULL; a row whose new key
   would contain a NULL (a new key column) is left to the re-snapshot. The
   table's version is incremented. The rebuild, and the hydration of the
   re-planned views, run off the engine thread: other tables' changes go on
   being applied, reads of the table and of the re-planned views wait for
   it, and no checkpoint is taken until it is done (the table's rows would
   be in neither version alone);
4. at the same GSN: indexes on the table move to its new version, following
   their columns by name (an index whose column is gone is dropped, one that
   cannot be built is marked broken; both are logged); subscriptions that
   read the table — `SUBSCRIBE TO` the table, a virtual view over it, or a
   query reading it directly or through views (also through materialized
   views, whose new versions their dataflows would not follow) — end with
   `table "t" changed its columns upstream` (SQLSTATE `55000`); sinks from
   the table restart with the new columns
   ([Sinks](sinks.md#schema-changes-and-replacements)); and the materialized
   views reading the table are re-planned (below);
5. the source re-snapshots the table from the beginning, which fills the new
   columns of existing rows. Fresh reads of the table wait for it from the
   schema record on.

Every materialized view that reads the table, directly or through other
views, is planned again from its `CREATE` statement, in dependency order,
and swapped at the schema record's GSN, so that it reads the new version of
the table (as a restart would plan it). Its indexes follow their columns by
name. If its columns stay the same, its subscriptions and sinks continue
and receive the difference; if they change (a `SELECT *` view, say), its
subscriptions end with `materialized view "v" changed its columns` and its
sinks restart with the new columns. A view that no longer plans — it uses
a dropped or renamed column, or an expression no longer fits a new type —
is **dropped**, together with its indexes, its sinks, the virtual views
reading it (directly or through other virtual views, as `DROP ... CASCADE`
would; logged as `view w is dropped: it reads v, which is no longer valid`)
and the materialized views reading it (logged as `materialized view v is
dropped: it is no longer valid after a change to object <id>: <reason>`).
A view that is already [broken](operations.md#recovery) is built again if
it plans now, and stays broken otherwise. Replaying the log repeats all of
this identically. Other virtual views are not re-planned: they stay in the
catalog, are expanded when read, and fail then if they no longer plan (one
that uses a dropped column fails with `column "c" does not exist`); drop or
replace them.

The key comes from the replica identity the Relation message describes: with
`DEFAULT` the primary key's columns (the table becomes a keyless multiset if
it no longer has a primary key: PostgreSQL then refuses to publish its
updates and deletes), with `USING INDEX` the index's columns. `FULL` (which
marks every column) and `NOTHING` keep the known key, as long as its columns
still exist. So `ALTER TABLE ... REPLICA IDENTITY ...`, and adding, dropping
or replacing the primary key under `DEFAULT`, re-key the table. After a key
change the re-snapshot starts by truncating the table: deletes decoded with
the old key could not be applied. Not detected: primary-key changes of a
`FULL` or `NOTHING` table, whose Relation messages do not show the key (with
`FULL`, old rows are complete, so this only matters if the old key columns
stop being unique).

Schema changes that change neither the column names, the mapped types nor
the key are invisible to the engine: new indexes, defaults, constraints, and
typmod-only changes such as `varchar(10)` to `varchar(20)`. A renamed column
is a dropped column plus a new one: its values are NULL until the
re-snapshot fills them, and materialized views that use the old name are
dropped (see above). To keep such a view with its indexes and sinks,
replace it before the upstream change by a definition that does not need
the column.

### Tables added to or removed from the publication

Tables added to the publication later are not picked up automatically; use
`ALTER SOURCE ... REFRESH`. Tables removed from the publication, or dropped
upstream, simply stop receiving changes once their snapshot (or re-snapshot)
is complete; the replicated table and its contents stay until the source is
dropped.

If the source has to copy a table whose chunk query fails — the table was
dropped upstream, the role lacks `SELECT` on it, a value does not parse (a
streamed change with such a value makes the table be copied again, see
[type mapping](#type-mapping)), or any other error that leaves the connection
usable — that table alone is
retried, after 1 s, doubling up to 60 s, while the source copies its other
tables and keeps streaming. `SHOW SOURCES` shows
`error: cannot copy table <schema>.<table>: <message>; retrying every <n>s,
streaming the other tables`, and the web UI marks the table. Its changes are
skipped until it is copied (as for any table not copied yet), fresh reads of
it wait, and, as the source's LSN frontier does not advance while one of its
tables is incomplete, so do `AS OF AT LEAST LSN` reads. Once it can be read
(after a `GRANT`, say), it is copied and the status clears. A dropped table
keeps failing; drop and re-create the source to get rid of it (a single
table cannot be removed from a source).

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

The source thread is asked to stop (the engine does not wait for it: it may
be in a connection attempt or a snapshot query), and the replication slot is
dropped in the background (`DROP_REPLICATION_SLOT`; a slot that no longer
exists counts as dropped). Failed attempts — the upstream is unreachable,
the slot still in use, also by the stopping source's own session — are
retried with a delay doubling from 250 ms up to 10 s, with a warning in the
log at attempts 1, 2, 4, 8, ..., until the drop succeeds.
Pending drops are part of the checkpointed state (and re-derived from a
`DROP SOURCE` replayed from the log), so a drop that has not succeeded when
the engine stops is retried after the next start. Creating a source whose
slot has the name of a slot still to be dropped (an explicit `SLOT`)
cancels that drop.

## Monitoring

`SHOW SOURCES` lists every source:

| Column | Meaning |
|---|---|
| `name` | source name |
| `publication` | publication name |
| `slot` | replication slot name |
| `tables` | number of replicated tables |
| `lsn` | the source's LSN frontier (`X/Y`): every upstream commit at or below it has been logged. It stays `0/0` until all of the source's tables have completed their initial snapshot (and, after a restart, until the source has reported it again), and does not advance during a re-snapshot. |
| `status` | `starting`, `streaming`, `snapshotting <schema>.<table>`, `error: <message>` (while reconnecting), `error: cannot copy table <schema>.<table>: ...` (a table that cannot be read, retried while the rest streams; further failing tables are counted as `(and <n> more tables)`), `error: cannot read snapshot chunks: ...` (the snapshot connection failed; retried while the source streams), or `stopped` (the source thread could not be started) |

`SHOW FRONTIERS` repeats the frontier per source; `SHOW TABLES` shows each
table's source and row count. Source warnings and errors are written to the
server log (`RUST_LOG=ivm::source=debug` for more).

## Cleaning up upstream slots

An engine that is stopped for good, or a deleted data directory, leave
slots behind that retain WAL (as does, until it succeeds, the background
drop of a dropped source's slot whose upstream is unreachable). List and
drop them in PostgreSQL:

```sql
SELECT slot_name, active, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots WHERE slot_name LIKE 'ivm\_%';

SELECT pg_drop_replication_slot('ivm_pg_3f9a1c2e_1000');
```

A slot can only be dropped while no walsender uses it (`active = false`);
stop the engine (or drop the source) first.
