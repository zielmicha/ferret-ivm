# Recovery and snapshots

Fault tolerance, view creation, consistent reads and time travel all use one
mechanism: *read the state as of GSN g, replay the log from g + 1*. This
chapter describes the recovery protocol, how checkpointed indexes are found
again, how sources resume, and the DBLog snapshot algorithm as implemented.
Code: `crates/ivm/src/coord/mod.rs` (`start`, `recover`, `checkpoint`),
`crates/ivm/src/coord/meta.rs` (the metadata tree, `FORMAT`),
`crates/ivm/src/storage/mod.rs` (`Env::open`, `Env::checkpoint`),
`crates/ivm/src/log.rs`, `crates/ivm/src/source/postgres.rs`.

## What is durable

| Store | Written | Contents |
|---|---|---|
| input log (`log/`) | by the sequencer, before anything is applied or acknowledged | every input with its GSN |
| checkpoint (`store/`) | by the engine every few seconds | all index trees as of the checkpoint GSN, among them the *metadata tree*, which holds the catalog with its DDL history and the engine state |

The input log is the source of truth. A checkpoint is an optimisation: it
bounds replay. Checkpoints are *atomic*: all trees, the metadata tree
included, are committed by one manifest append at one GSN (the design in
[doc.md](../../doc.md) describes per-arrangement durable frontiers; the
implementation checkpoints everything together).

The catalog and the engine state are stored like the indexes, as a weighted
multiset of entries with GSN-stamped histories in a tree of their own
([On-disk format](../on-disk-format.md#the-metadata-tree)): a change
retracts the old entry and adds the new one, so a checkpoint writes only
the nodes that changed, and its manifest record names the trees and carries
a small header. The engine keeps the same state in memory (the catalog,
source states, the graph's error records, ...) and writes what changed
since the last time after every DDL or schema record, at its GSN (so the
tree holds the catalog as of every GSN of the retention window), and at
every checkpoint.

## The recovery protocol

The server binary binds its listen address before recovery (a failure exits
with status 1 at once) and accepts connections only once recovery is done.
`coord::start`:

1. **Lock** `LOCK` (exclusive `flock`, retried for up to 2 s: a process
   forked while the lock was held keeps a copy of it until it execs); fail
   (`55006`) if another process holds it. Then read the **format version**
   (`FORMAT`, before anything else is opened): a newer format than the code
   reads is refused (`0A000`), a new directory gets `FORMAT` at once, and a
   directory with data but without `FORMAT` (format 1) is migrated in step 9.
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
4. **Restore the engine state** (`Engine::recover`): the record's
   application state is a header naming the metadata tree (`meta::load`
   attaches it and reads all of it), or, for format 1, the JSON `AppState`:
   catalog with its DDL history, the map from arrangement fingerprint to
   tree id and to the GSN its history starts at (`arr_since`), the error
   state, source states and resume LSNs, the request counter, the logical
   clock and its recent ticks, and the pending slot drops.
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
   ones. Then, by node fingerprint, the error records of arranged nodes and
   the per-group error state of operators are restored
   ([Dataflow](dataflow.md#errors)); records of nodes that no longer exist
   are retracted from the metadata tree.
6. **Hydrate** everything that was not matched, from the recovered state at
   the checkpoint GSN (`Graph::hydrate`; the rebuilt arrangements' history
   starts there), give each materialized view the error records of its root
   node, then **drop** recovered trees that nothing attached (arrangements
   that no longer exist) and set the retention frontier.
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
   (`LogWriter::skip_to`). A format-1 directory is **migrated** here: a
   checkpoint writes the whole state into a new metadata tree, and only then
   is `FORMAT` written; a crash in between finds a format-1 record (before
   the checkpoint's commit) or a current one (after it) at the next start,
   and migrates again or only writes `FORMAT`
   ([Compatibility](../on-disk-format.md#migrating-format-1)). **Start** the sources (with their logged state
   and resume LSN), the pending slot drops, the sequencer (whose next GSN is
   the log's last GSN + 1), and the engine thread; then the server starts
   accepting connections.

A data directory without a checkpoint replays the whole log from GSN 1 (the
log is only truncated after checkpoints).

### Matching checkpointed indexes

A checkpoint records, for every hydrated arrangement, its **structural
fingerprint** and its tree id (in the metadata tree). Because fingerprints are
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

1. `persist_meta(processed GSN)`: write what changed since the last time
   into the metadata tree — catalog objects changed by DDL since (normally
   none: DDL records write them when applied), new and trimmed history
   entries, new ticks and ticks behind the retention frontier, changed
   source states and resume LSNs, arrangements hydrated or dropped, pending
   slot drops, and the changes of the error state
   ([On-disk format](../on-disk-format.md#the-metadata-tree));
2. `Env::checkpoint(processed_gsn, header)`: write dirty nodes (of the
   metadata tree too), `fsync`, append the manifest record — the trees, the
   segments and the header naming the metadata tree, tens of bytes per tree —
   (the commit point), delete dead segments
   ([Storage](storage.md#segments-and-checkpoints));
3. `Env::gc(1000)`: relocate live nodes of the oldest sparse segment;
4. send `Truncate(processed − retention)` to the sequencer, which deletes
   log segments whose records all lie at or before that GSN.

The engine thread does nothing else while a checkpoint runs. Its cost is
that of the nodes that changed since the previous one; it does not depend
on the size of what did not change (the catalog, its history of up to
11,000 statements, the error records).

## Source resumption

Each source logs, through the input log, everything it needs to resume:

- every upstream transaction as a `Commit` record carrying its end LSN; the
  engine keeps the highest per source (`source_lsns`, saved in checkpoints)
  as the **resume LSN**;
- its *state* as a `Progress` record: whether the slot has been created, and
  per table whether its initial snapshot is done, the last copied primary
  key (`boundary`), and whether the next copy starts by truncating the table
  (`truncate`, see [Consequences](#consequences)), as JSON. Applying a
  `Schema` record also marks the table's snapshot to be done again in the
  stored state (keeping its `truncate` flag), so that a restart before the
  re-snapshot logs progress of its own still redoes it, from an empty table
  after a key change.

On start, the source thread streams from `max(resume LSN, durable LSN)`
(`START_REPLICATION ... LOGICAL <lsn>`; the server starts at the slot's
confirmed position if that is later) and skips any transaction whose end LSN
is not greater than the resume LSN. Because the slot is only acknowledged up
to durable commits, every transaction after the resume LSN is still
available upstream. A snapshot chunk that was in flight is redone from the
logged boundary.

## The DBLog snapshot algorithm

Initial snapshots, and the copies a table needs again (after an upstream
schema or key change, a lost replication slot, or `ALTER SOURCE ...
REFRESH` for new tables), copy tables in chunks *while the change stream
keeps flowing*, without locking tables and without a replication-slot
snapshot, following the DBLog watermark idea: the chunk's reads are ordered
against the stream by writing *watermark* messages into the WAL around them.

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
3. On a normal connection, emit the **low watermark**:
   `SELECT pg_logical_emit_message(true, 'ivm', 'L:<source>:<table>:<run>:<seq>')`
   (a transactional message: it appears in the stream at its commit).
   `<run>` is a random id chosen each time the source thread starts (16 hex
   digits) and `<seq>` counts its chunks, so the tag is unique: after a
   restart the stream resumes before watermarks of the previous run, and
   logical messages reach every slot of the database, including those of
   other engines.
4. Read the chunk in one transaction, in two round trips:
   ```sql
   BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT pg_current_snapshot()::text;
   -- checked: does it see every transaction decoded so far? (below)
   SELECT <columns> FROM <schema>.<table>
     [WHERE (<pk>) > (<boundary>)] ORDER BY <pk> LIMIT 10000; COMMIT
   ```
   If the snapshot does not contain every transaction the stream has
   decoded while tables are being copied (see
   [Filtering stream changes](#filtering-stream-changes)), the transaction is
   rolled back and the chunk retried 100 ms later (its low watermark is
   then ignored: no chunk carries its tag).
   String key columns are compared as `col::text COLLATE "C"` (and the
   boundary as `'<value>'::<type>::text COLLATE "C"`): the byte order of
   their text form, which is how the engine orders these keys — also for
   upstream types it replicates as text (enums, `inet`, ...), whose own
   order differs and which take no collation. The rows are kept in memory
   keyed by their encoded primary key; the chunk records its last key
   (`max_key`), whether it is the last chunk (fewer than 10,000 rows), and
   the snapshot's `(xmin, xmax, xip)`.
   If the chunk query fails with an error that leaves the connection usable
   (no `SELECT` privilege, the table dropped upstream, a value that does not
   parse), the transaction is rolled back and only that table is retried
   later (`PgSource::failing`); other errors end the replication session.
5. Emit the **high watermark** `'H:<source>:<table>:<run>:<seq>'`.

The chunk starts in phase *before-low*. While the stream is processed:

- the low watermark switches it to phase *in-window*;
- the high watermark marks the transaction carrying it; at that
  transaction's commit, the chunk's **remaining rows are added as upserts to
  that commit**, the table's boundary becomes the chunk's last key (and the
  table is done if this was the last chunk), the commit is logged, and then a
  `Progress` record with the new state.

### Filtering stream changes

For every streamed upsert (insert or update) of key `k` in table `T`
(`snapshot_filter`):

| Situation | Action |
|---|---|
| `T`'s snapshot is done | apply |
| `k <= boundary` (already copied) | apply |
| a chunk of `T` is in flight and `k` is in its range (`k <= max_key`, or it is the last chunk): phase *in-window* | apply, and remove `k` from the chunk (the stream's value is at least as new) |
| same, phase *before-low*, the transaction is **not** visible in the chunk's snapshot | apply, and remove `k` from the chunk |
| same, phase *before-low*, the transaction **is** visible in the chunk's snapshot | skip (the chunk already contains its effect) |
| otherwise (`k` beyond everything copied so far) | skip: a later chunk will read the row's current value |

Deletes (and the delete of the old key of a primary-key change) always
apply: the filter only decides whether the chunk's row of that key is
removed. Deleting a key the engine does not hold changes nothing, but a
table that is copied again still holds its old rows, which no chunk would
remove (chunks only upsert the rows that exist). A `TRUNCATE` always applies
too, and clears the rows of a chunk of that table in flight, unless the
chunk is in phase *before-low* and its snapshot saw the truncating
transaction (then the chunk already holds the table as it is after the
truncate). Unchanged TOASTed values of an applied upsert are filled from the
chunk's row of the key when the stream wins over the chunk (for a key
change, from the chunk's row of the old key), else from a complete old row
(`REPLICA IDENTITY FULL`); otherwise the engine fills them from its stored
row ([Dataflow](dataflow.md#table)).

Why this yields a consistent copy: the chunk's snapshot is taken after the
low watermark committed, so every transaction that commits after the high
watermark is newer than the chunk and is applied by the stream (its key is
now at or below the boundary). Transactions between the watermarks are
applied and override the chunk's possibly older values. Transactions before
the low watermark are normally contained in the chunk's snapshot and are
skipped; the snapshot check catches the race in which such a transaction's
commit record precedes the low watermark in the WAL but the transaction was
not yet visible when the snapshot was taken. Keys beyond the chunk are not
touched until a later chunk copies them, with a later snapshot. Each chunk's
rows and the boundary move are logged in the same commit, so the engine's
copy is always a prefix of the key space that agrees with the upstream as of
the GSN.

"A later chunk reads the row's current value" holds only if that chunk's
snapshot contains the skipped transaction, and the same race applies: a
transaction is decoded once its commit record is flushed but becomes visible
only when it ends, which a synchronous standby can delay for as long as it
takes to confirm the commit. So the source keeps the ids of the transactions
it decodes while some table is being copied (`PgSource::unseen`; when a
table starts to be copied again, also the last 1,024 decoded before, whose
changes were applied to rows the re-snapshot overwrites), and reads a chunk
only once its snapshot sees all of them; transactions a snapshot sees are
forgotten, as later snapshots see them too. If many accumulate without a
chunk being read (a table that cannot be read), they are checked against a
fresh `pg_current_snapshot()` at most once a second. Without this check, a
chunk read while such a transaction is invisible would keep the row's old
value for good (the replication test
`chunks_see_transactions_decoded_before_them` reproduces that with an
unreachable synchronous standby).

The 32-bit transaction ids of the stream are compared with the 64-bit
snapshot by choosing the epoch closest to the snapshot (`xid_visible`).

### Tables without a primary key

Tables with `REPLICA IDENTITY FULL` and no key are read in a single chunk
without `WHERE`/`ORDER BY`/`LIMIT`; the chunk is the whole table and
replaces what the engine stores (`push_multiset`):

- before a chunk of the table is in flight, its changes (including
  `TRUNCATE`) are skipped: the chunk will read their effect;
- at the low watermark the chunk is emitted as a truncate of the table
  followed by its rows (for an initial snapshot the truncate is a no-op;
  for a copy after a schema change it removes the rows kept from before);
- changes decoded during phase *before-low* from transactions the chunk's
  snapshot does not see (their commit preceded the watermark in the WAL but
  was not yet visible) are kept and applied right after the chunk's rows;
- in the window, changes from transactions visible in the chunk's snapshot
  are skipped and the others applied;
- after the high watermark's commit the table is done.

### Consequences

- The source's LSN frontier does not advance while any of its tables is
  incomplete, so `AS OF AT LEAST LSN` reads never see a partial table; fresh
  reads wait for the snapshot explicitly (per table: a table that cannot be
  read keeps the fresh reads of it waiting, not those of the others).
- A restart in the middle of a snapshot resumes after the last logged
  boundary.
- `ALTER SOURCE ... REFRESH` adds tables that are snapshotted the same way.
- After an upstream schema change the table's state is reset and the whole
  table is copied again. The reset is logged before the `Schema` record, so
  that a restart in between copies the table in its old shape and detects
  the change again. The changes of the current upstream transaction decoded
  before the change are rewritten for the new columns. A re-snapshot only
  upserts the rows it reads, so stream deletes of keys beyond the boundary
  are applied rather than skipped (the row may still be stored). When the
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
