# Recovery and snapshots

Fault tolerance, view creation, consistent reads and time travel all use one
mechanism: *read the state as of GSN g, replay the log from g + 1*. This
chapter describes the recovery protocol, how checkpointed indexes are found
again, how sources resume, and the DBLog snapshot algorithm as implemented.
Code: `crates/ivm/src/coord/mod.rs` (`start`, `recover`, `checkpoint`),
`crates/ivm/src/storage/mod.rs` (`Env::open`, `Env::checkpoint`),
`crates/ivm/src/log.rs`, `crates/ivm/src/source/postgres.rs`.

## What is durable

| Store | Written | Contents |
|---|---|---|
| input log (`log/`) | by the sequencer, before anything is applied or acknowledged | every input with its GSN |
| checkpoint (`store/`) | by the engine every few seconds | all index trees as of the checkpoint GSN, plus the catalog and engine state (`AppState`) |

The input log is the source of truth. A checkpoint is an optimisation: it
bounds replay. Checkpoints are *atomic*: all trees and the engine state are
committed by one manifest append at one GSN (the design in
[doc.md](../../doc.md) describes per-arrangement durable frontiers; the
implementation checkpoints everything together).

## The recovery protocol

The server binary binds its listen address before recovery (a failure exits
with status 1 at once) and accepts connections only once recovery is done.
`coord::start`:

1. **Lock** `LOCK` (exclusive `flock`, retried for up to 2 s: a process
   forked while the lock was held keeps a copy of it until it execs); fail
   (`55006`) if another process holds it.
2. **Open the log** (`LogWriter::open`): read the newest segment, parse frames
   until the first one that is incomplete, fails its CRC or does not decode,
   truncate the file there (`fsync`), and take the last GSN (from the
   previous segment if the newest one is empty).
3. **Open the store** (`Env::open`): read `MANIFEST`, keep the last record
   whose CRC is valid, and truncate a torn tail. Re-open the active segment
   truncated to the recorded length, delete segment files the record does
   not list, and register every tree root as *recovered but not attached*.
   A manifest that has contents but no valid record, or a missing manifest
   while segment files hold data, stops the start before anything is
   modified: the checkpoint may have existed, and the log may already be
   truncated after it.
4. **Restore the engine state** (`Engine::recover`) from the record's
   application state (JSON `AppState`): catalog, the map from arrangement
   fingerprint to tree id and to the GSN its history starts at
   (`arr_since`), source states and resume LSNs, the request counter, the
   logical clock and its recent ticks, and the pending slot drops.
5. **Rebuild the dataflow** from the catalog: tables (`table_rt`), then
   materialized views (planned and lowered again from their SQL text, as
   stored definitions) and indexes, each after the views it reads
   (`dependency_order`: ids are not a dependency order, as
   `CREATE OR REPLACE` keeps the object's id). Every arrangement the planner
   asks for looks up its fingerprint in the recovered map (see below); found
   trees are attached, count as hydrated and keep their `since`. A view or
   index that cannot be built (its definition no longer plans, it reads a
   view that cannot be built, or an index's columns are gone) is logged and
   recorded in `Runtime::broken` with the reason instead of stopping the
   start (I/O errors still stop it); it stays in the catalog without a
   dataflow, and its recovered trees are dropped with the other unattached
   ones. Then the views' error counts and, by node fingerprint, the error
   records of arranged nodes and the per-group error state of operators are
   restored ([Dataflow](dataflow.md#errors)).
6. **Hydrate** everything that was not matched, from the recovered state at
   the checkpoint GSN (`Graph::hydrate`; the rebuilt arrangements' history
   starts there), then **drop** recovered trees that nothing attached
   (arrangements that no longer exist) and set the retention frontier.
7. **Start the sinks** from the recovered state (each takes a snapshot at
   the checkpoint GSN; a target at least that far resumes, one behind it is
   resynchronised, see
   [Sinks](../user-guide/sinks.md#start-up-and-resynchronisation)). A sink
   that cannot start (it reads a broken view) is marked broken.
8. **Replay** every log record after the checkpoint GSN through
   `apply_record`, with `replaying = true`: DDL is re-applied from its logged
   text and `extra`, no source is started, and nobody is waiting for
   replies; the sinks receive the replayed changes (a sink created by a
   replayed `CREATE SINK` starts then), and slot drops of replayed
   `DROP SOURCE`s are recorded. Reading the log (`log::read_from`) refuses
   invalid data anywhere but at the end of the newest segment, and a gap in
   the GSNs after the checkpoint, as corruption: replaying around them would
   silently skip records. A DDL or schema record that fails is only logged,
   but any other record that fails to apply stops the start-up with its
   error.
9. If the log ends before the checkpoint's GSN (an unsynced tail lost with
   `--no-sync`), GSN numbering continues after the checkpoint
   (`LogWriter::skip_to`). **Start** the sources (with their logged state
   and resume LSN), the pending slot drops, the sequencer (whose next GSN is
   the log's last GSN + 1), and the engine thread; then the server starts
   accepting connections.

A data directory without a checkpoint replays the whole log from GSN 1 (the
log is only truncated after checkpoints).

### Matching checkpointed indexes

A checkpoint records, for every hydrated arrangement, its **structural
fingerprint** and its tree id (`AppState::trees`). Because fingerprints are
computed from the operator structure — the operator kind, its parameters, the
fingerprints of its inputs and of the arrangements it reads, and the
arrangement's key and summaries — and never from node ids or creation order,
re-planning the same catalog produces the same fingerprints, even though
nodes get different ids after objects were dropped and re-created
([Dataflow](dataflow.md#hash-consing),
[On-disk format](../on-disk-format.md#structural-fingerprints)).

When `Graph::add_arrangement` creates an arrangement whose fingerprint is in
the recovered map and whose tree exists, it attaches that tree (with the new
layout's summarizer) instead of creating an empty one, marks the arrangement
hydrated, and gives it its checkpointed `since`, so that `AS OF` reads of
times before the restart can still use it. A node all of whose arrangements
were recovered needs no computation during hydration. The error state of
nodes and operators is restored by fingerprint as well; only the group
caches of aggregations, an accelerator, start empty.

An arrangement that is not matched is rebuilt, and its history starts at
the checkpoint: reads as of earlier GSNs that would need it fail with
`AS OF g is before the history of a relation it reads`. Besides code
changes ([Compatibility](../on-disk-format.md#compatibility)), this happens
when re-planning picks another join order: delta paths are ordered by the
current sizes of their inputs, so a view whose input tables changed size
considerably since it was planned can get a different join plan, and then
the join's node and everything downstream of it get new fingerprints (see
[Troubleshooting](../user-guide/troubleshooting.md#known-problems)).

**Tables are special**: their rows exist nowhere else (the log before the
checkpoint may have been truncated). If a table's primary index cannot be
matched, recovery stops with
`the checkpoint has no index for table <id> (version <v>); refusing to start without its rows`.
Views and indexes, by contrast, can always be recomputed from tables, so a
fingerprint mismatch only costs a rebuild.

## Checkpoints

`Engine::checkpoint` (every `--checkpoint-secs` if anything changed, when
dirty memory exceeds half the cache, on `CHECKPOINT`, and on shutdown):

1. build `AppState`: the catalog (with its DDL history), the fingerprint →
   tree id and fingerprint → `since` maps of all hydrated arrangements, each
   materialized view's error counts, the error records of arranged nodes and
   the per-group error state of operators, each source's opaque state (JSON)
   and last logged LSN, the request counter, the logical clock and its ticks
   in the retention window (at most the last 100,000), and the pending slot
   drops; serialise it as JSON
   ([On-disk format](../on-disk-format.md#checkpoint-application-state-appstate));
2. `Env::checkpoint(processed_gsn, json)`: write dirty nodes, `fsync`, append
   the manifest record (the commit point), delete dead segments
   ([Storage](storage.md#segments-and-checkpoints));
3. `Env::gc(1000)`: relocate live nodes of the oldest sparse segment;
4. send `Truncate(processed − retention)` to the sequencer, which deletes
   log segments whose records all lie at or before that GSN.

The engine thread does nothing else while a checkpoint runs.

## Source resumption

Each source logs, through the input log, everything it needs to resume:

- every upstream transaction as a `Commit` record carrying its end LSN; the
  engine keeps the highest per source (`source_lsns`, saved in checkpoints)
  as the **resume LSN**;
- its *state*: whether the slot has been created, and per table whether its
  initial snapshot is done, the last copied key (`boundary`, in the order
  of the upstream key index), and whether the next copy starts by
  truncating the table (`truncate`, see [Consequences](#consequences)). The
  source logs what changed, as a `ProgressDelta` record listing the changed
  tables' entries; the engine applies it to the state it keeps per source
  (`source_states`, saved in checkpoints as JSON; `Progress` records of
  earlier versions, which carry the whole state, replace it). Logging the
  whole state every time made a snapshot of `T` tables log `O(T²)` bytes.
  Applying a `Schema` record also marks the table's snapshot to be done
  again in the stored state (keeping its `truncate` flag), so that a
  restart before the re-snapshot logs progress of its own still redoes it,
  from an empty table after a key change.

On start, the source thread streams from `max(resume LSN, durable LSN)`
(`START_REPLICATION ... LOGICAL <lsn>`; the server starts at the slot's
confirmed position if that is later) and skips any transaction whose end LSN
is not greater than the resume LSN. Because the slot is only acknowledged up
to durable commits, every transaction after the resume LSN is still
available upstream. A snapshot chunk that was in flight is redone from the
logged boundary. (A boundary logged by an earlier version, in the engine's
key order, is kept only for keys without string columns, whose order it
is the same; otherwise the table's copy starts again from its first key.)

## The DBLog snapshot algorithm

Initial snapshots, and the copies a table needs again (after an upstream
schema or key change, a lost replication slot, or `ALTER SOURCE ...
REFRESH` for new tables), copy tables in chunks *while the change stream
keeps flowing*, without locking tables and without a replication-slot
snapshot, following the DBLog watermark idea: the chunk's reads are ordered
against the stream by writing *watermark* messages into the WAL around them.

Chunks are read by the source's **copy thread** (`ivm-copy-<source id>`,
started with the first chunk; `copy_thread`), on a connection of its own,
so that the replication thread goes on reading the stream and sending
status updates meanwhile: a chunk that takes long (or waits for a lock)
neither holds up the other tables' changes nor lets the session run into
`wal_sender_timeout`. The source thread asks for a chunk (`CopyRequest`)
and handles what the copy thread reports (`CopyEvent`) between stream
messages.

### Per-chunk protocol

`PgSource::maybe_start_chunk` runs whenever no upstream transaction is open
in the stream and no chunk is in flight:

1. Pick the table with the lowest id whose snapshot is not done (skipping
   tables whose chunk query failed recently: they are retried after a
   delay growing from 1 s to 60 s, while the other tables proceed).
2. If the table's state says `truncate`, log a commit that truncates the
   table (here, between upstream transactions and before the chunk's low
   watermark) and log the state without the flag: from then on the table
   fills exactly as in an initial snapshot.
3. Send the copy thread the chunk's request: the table, its boundary (or
   the keys to read again, if the table has any, see below), the watermark
   tags and the transactions the chunk's snapshot must see (see
   [Filtering stream changes](#filtering-stream-changes)).

The copy thread then:

1. emits the **low watermark**:
   `SELECT pg_logical_emit_message(true, 'ivm', 'L:<source>:<table>:<run>:<seq>')`
   (a transactional message: it appears in the stream at its commit).
   `<run>` is a random id chosen each time the source thread starts (16 hex
   digits) and `<seq>` counts its chunks, so the tag is unique: after a
   restart the stream resumes before watermarks of the previous run, and
   logical messages reach every slot of the database, including those of
   other engines;
2. opens the chunk's transaction and takes its snapshot:
   `BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT pg_current_snapshot()::text`.
   If the snapshot does not contain every transaction of the request, the
   transaction is rolled back and the source retries the chunk 100 ms later
   (its low watermark is then ignored: no chunk carries its tag);
3. reads the rows. For a table with a key:
   ```sql
   SELECT <columns> FROM <schema>.<table>
     [WHERE (<key>) > ('<boundary value>', ...)] ORDER BY <key> LIMIT 10000; COMMIT
   ```
   The key columns are named as they are, so the order and the comparison
   with the boundary are those of the table's key index (its primary key's,
   or its replica identity's), which serves both: each chunk is an index
   range scan. The boundary's values are untyped literals, which take the
   columns' types (also of types the engine replicates as text, such as
   enums). The engine never compares keys in this order (see below), so it
   need not be the engine's. (An identity index whose columns are sorted
   `DESC` or under another collation than the columns' still works, but its
   chunks are sorted rather than read from the index.) The rows (parsed,
   with their encoded keys), the text of the last key, whether this is the
   table's last chunk (fewer than 10,000 rows) and the snapshot go to the
   source in one event. For a table without a key, see
   [Tables without a primary key](#tables-without-a-primary-key);
4. emits the **high watermark** `'H:<source>:<table>:<run>:<seq>'`.

A failure that leaves the connection usable (no `SELECT` privilege, the
table dropped upstream, a value that does not parse) fails the table: the
transaction is rolled back and only that table is retried later
(`PgSource::failing`). Any other failure is one of the copy connection: it
is dropped (the next chunk connects again), and no chunk starts for a delay
growing from 1 s to 60 s; the status shows `error: cannot read snapshot
chunks: ...`. Neither ends the replication session. The source abandons a
chunk on these failures, when the session ends, and when the table is reset
(a schema change, a value that cannot be converted, a lost slot); the copy
thread drops a chunk the source no longer waits for (`Copier::wanted`), and
events of abandoned chunks are ignored.

The chunk starts in phase *before-low*. While the stream is processed:

- the low watermark switches it to phase *in-window*;
- the high watermark marks the transaction carrying it; at that
  transaction's commit the chunk's rows have arrived (the copy thread sent
  them before writing the watermark), its **remaining rows are added as
  upserts to that commit**, the table's boundary becomes the chunk's last
  key (`scanned` if this was the last range; a chunk of keys read again
  takes them off the list instead), the table is done once it is
  `scanned` with no keys left to read again, the commit is logged, and
  then a `ProgressDelta` record with the table's new state.

### Filtering stream changes

A streamed change always applies (it is logged with its transaction), with
one exception, and decides what happens to the chunk in flight
(`snapshot_filter`). For an upsert (insert or update) of key `k` in table
`T`:

| Situation | Action |
|---|---|
| no chunk of `T` is in flight, or the chunk's rows do not include `k` | apply |
| the chunk has a row of `k`, phase *in-window* | apply, and remove `k` from the chunk (the stream's value is at least as new) |
| same, phase *before-low*, the transaction is **not** visible in the chunk's snapshot | apply, and remove `k` from the chunk |
| same, phase *before-low*, the transaction **is** visible in the chunk's snapshot | skip (the chunk's row already contains its effect, or a later one) |

Keys are only looked up in the chunk's rows, never compared in an order, so
the stream needs no notion of which keys have been copied. A change to a
key that an earlier chunk copied applies to the engine's row; one to a key
that a later chunk reads applies too, and that chunk's row, read with a
later snapshot, overwrites it (with the same or a newer value); one to a
key no chunk reads (deleted before its chunk) is undone by the stream's
delete. Deletes, the delete of the old key of a primary-key change and
`TRUNCATE` always apply as well: the filter only decides whether the
chunk's row of that key is removed (for a `TRUNCATE`, all of them, unless
the chunk is in phase *before-low* and its snapshot saw the truncating
transaction: then the chunk already holds the table as it is after it).

The rows arrive while the stream goes on. A change to `T` decoded before
they do applies, and is recorded with its phase and transaction
(`Chunk::touched`); when the rows arrive, the recorded changes take away
the rows they are newer than, by the rules above (a change visible in the
snapshot and decoded before the low watermark leaves the chunk's row, which
then overwrites it with a value at least as new).

Unchanged TOASTed values of an applied upsert are filled from a complete
old row (`REPLICA IDENTITY FULL`), else, when the stream wins over the
chunk, from the chunk's row of the key; for a key change from the chunk's
row of the old key, if the snapshot does not contain the change (if it
does, a row of the old key in it was added after the change, and the new
key's row has the values). Otherwise the engine fills them from its stored
row ([Dataflow](dataflow.md#table)). While `T` is being copied, the engine
has no row of a key that has not been copied yet, so such an upsert is
logged as `UpsertIfPresent`, which changes nothing without a row of its key
(and, unlike `Upsert`, takes nothing from a row deleted just before: that
delete need not be of its old key): the key's chunk brings the whole row.
An upsert with unchanged TOASTed values decoded before the chunk's rows
arrive is logged that way too, and recorded with its values: when the rows
arrive and the stream wins over the chunk, the chunk's row of its key (of
the old key of a key change, by the rule above) takes the upsert's values,
keeping its own for the unchanged columns, and is logged with the chunk;
later changes of the key are held against that row. So the stream never
waits for a chunk.

A key change with unchanged TOASTed values and a key-only old tuple
(`REPLICA IDENTITY DEFAULT` or `USING INDEX`) whose values the source cannot
supply from the chunk leaves the new key without a row if the engine has
none of the old key either (it has not been copied yet); should the new
key's range be copied already, no chunk would bring it. So the source adds
the new key to the table's `reread` list (`PgSource::add_reread`), logged
in a `ProgressDelta` before the transaction's commit (a restart in between
streams the transaction again): before the next key range, a chunk reads
those keys again (`WHERE (<key>) IN (('<value>', ...), ...)`, up to 1,000
keys, same protocol), and the table is done once every range has been read
(`scanned`) and the list is empty.

Why this yields a consistent copy: the chunk's snapshot is taken after the
low watermark committed, so every transaction that commits after the high
watermark is newer than the chunk and is applied by the stream after the
chunk's rows. Transactions between the watermarks are applied and override
the chunk's possibly older values. Transactions before the low watermark
are normally contained in the chunk's snapshot: their changes to the
chunk's keys are skipped or overwritten by the chunk's rows; the snapshot
check catches the race in which such a transaction's commit record precedes
the low watermark in the WAL but the transaction was not yet visible when
the snapshot was taken. Each chunk's rows and the boundary move are logged
in the same commit. Once the last chunk is logged, every key holds the
upstream value as of the GSN.

"The chunk's row overwrites it with the same or a newer value" holds only
if the chunk's snapshot contains every transaction the stream applied
before the chunk started, and the same race applies: a transaction is
decoded once its commit record is flushed but becomes visible only when it
ends, which a synchronous standby can delay for as long as it takes to
confirm the commit. So the source keeps the ids of the transactions it
decodes while some table is being copied (`PgSource::unseen`; when a table
starts to be copied again, also the last 1,024 decoded before, whose
changes were applied to rows the re-snapshot overwrites), and a chunk is
read only once its snapshot sees all of them (the copy thread checks the
snapshot against the ids in the request); transactions a snapshot sees are
forgotten, as later snapshots see them too. If many accumulate without a
chunk being read (a table that cannot be read), they are checked against a
fresh `pg_current_snapshot()` (taken by the copy thread) at most once a
second. Without this check, a chunk read while such a transaction is
invisible would keep the row's old value for good (the replication test
`chunks_see_transactions_decoded_before_them` reproduces that with an
unreachable synchronous standby).

The 32-bit transaction ids of the stream are compared with the 64-bit
snapshot by choosing the epoch closest to the snapshot (`xid_visible`).

### Tables without a primary key

Tables with `REPLICA IDENTITY FULL` and no key are read whole, in one
snapshot: the copy thread opens the chunk's transaction as above, then
reads the table through a cursor
(`DECLARE ivm_copy NO SCROLL CURSOR FOR SELECT <columns> FROM <table>`,
`FETCH 10000 FROM ivm_copy` until fewer rows come), and commits. The copy
replaces what the engine stores (`push_multiset`):

- before a chunk of the table is in flight, its changes (including
  `TRUNCATE`) are skipped: the copy will read their effect;
- when the snapshot is known, a commit truncating the table is logged (for
  an initial snapshot a no-op; for a copy after a schema change it removes
  the rows kept from before), and then each piece of 10,000 rows as a
  commit of its own, as it arrives, between upstream transactions (the copy
  thread waits while two pieces are queued, so the source holds at most
  those in memory);
- a change to the table while the copy is in flight is kept if the copy's
  snapshot does not contain its transaction (committed after the snapshot
  was taken, or in the visibility race above) and dropped if it does; one
  decoded before the snapshot is known waits for it. The kept changes are
  applied after all of the copy's rows, in the commit carrying the high
  watermark (applying a removal before the piece holding the row would make
  its multiplicity negative), so they are held in memory until then;
- after the high watermark's commit the table is done.

### Consequences

- The source's LSN frontier does not advance while any of its tables is
  incomplete, so `AS OF AT LEAST LSN` reads never see a partial table; fresh
  reads wait for the snapshot explicitly (per table: a table that cannot be
  read keeps the fresh reads of it waiting, not those of the others).
- While a table is being copied, `read_mode = latest` and `AS OF <gsn>`
  reads (and materialized views) see the rows copied so far and the rows
  the stream changed since its copy started.
- A restart in the middle of a snapshot resumes after the last logged
  boundary.
- `ALTER SOURCE ... REFRESH` adds tables that are snapshotted the same way.
- After an upstream schema change the table's state is reset and the whole
  table is copied again. The reset is logged before the `Schema` record, so
  that a restart in between copies the table in its old shape and detects
  the change again. The changes of the current upstream transaction decoded
  before the change are rewritten for the new columns. A re-snapshot only
  upserts the rows it reads, so a row deleted upstream goes away through
  the stream's delete, which always applies. When the
  stored rows cannot be trusted — after a key change, whose deletes by the
  old key were lost — the state also says `truncate`: before the first
  chunk's low watermark, between upstream transactions, the source logs a
  commit truncating the table, which then fills exactly as in an initial
  snapshot (see
  [Sources](../user-guide/sources.md#upstream-schema-changes)).
- When the replication slot turns out to be gone (dropped, or invalidated by
  `max_slot_wal_keep_size`), every table's state is reset — with `truncate`
  for keyed tables — and logged before the slot is created again; an
  invalidated slot is dropped first. The new slot's stream starts after the
  old resume LSN, and the tables are copied again from empty tables, so
  rows deleted while changes were lost go away too.
