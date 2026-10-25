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

`coord::start`:

1. **Lock** `LOCK` (exclusive `flock`); fail if another process holds it.
2. **Open the log** (`LogWriter::open`): read the newest segment, parse frames
   until the first one that is incomplete, fails its CRC or does not decode,
   truncate the file there (`fsync`), and take the last GSN (from the
   previous segment if the newest one is empty).
3. **Open the store** (`Env::open`): read `MANIFEST`, keep the last record
   whose CRC is valid, and truncate a torn tail. Re-open the active segment
   truncated to the recorded length, delete segment files the record does
   not list, and register every tree root as *recovered but not attached*.
4. **Restore the engine state** (`Engine::recover`) from the record's
   application state (JSON `AppState`): catalog, the map from arrangement
   fingerprint to tree id, view error counts, source states and resume LSNs,
   the request counter and the logical clock.
5. **Rebuild the dataflow** from the catalog, object by object in id order:
   tables (`table_rt`), materialized views (planned and lowered again from
   their SQL text), indexes. Every arrangement the planner asks for looks up
   its fingerprint in the recovered map (see below); found trees are attached
   and count as hydrated. Because `CREATE OR REPLACE` keeps the object's id,
   a view replaced to read a relation created after it is rebuilt before
   that relation exists, and recovery fails (`relation "b" has no dataflow`;
   a known problem).
6. **Hydrate** everything that was not matched, from the recovered state at
   the checkpoint GSN (`Graph::hydrate`), then **drop** recovered trees that
   nothing attached (arrangements that no longer exist) and set the retention
   frontier.
7. **Replay** every log record after the checkpoint GSN through
   `apply_record`, with `replaying = true`: DDL is re-applied from its logged
   text and `extra`, no source or sink is started, no slot is dropped, and
   nobody is waiting for replies. A DDL record that fails is only logged, but
   any other record that fails to apply stops the start-up with its error.
8. **Start** the sources (with their logged state and resume LSN), the sinks
   (each takes a snapshot and resynchronises, see
   [Sinks](../user-guide/sinks.md#start-up-and-resynchronisation)), the
   sequencer (whose next GSN is the log's last GSN + 1), and the engine
   thread; then the server starts accepting connections.

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
layout's summarizer) instead of creating an empty one, and marks the
arrangement hydrated. A node all of whose arrangements were recovered needs
no computation during hydration. Operator state kept only in memory — the
group caches of aggregations and the per-group error map of `Reduce` — starts
empty (which causes the stale-error problem described in
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
   tree id map of all hydrated arrangements, each materialized view's error
   counts, each source's opaque state (JSON) and last logged LSN, the request
   counter, and the logical clock; serialise it as JSON;
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
- its *state* as a `Progress` record: whether the slot has been created, and
  per table whether its initial snapshot is done and the last copied primary
  key (`boundary`), as JSON.

On start, the source thread streams from `max(resume LSN, durable LSN)`
(`START_REPLICATION ... LOGICAL <lsn>`; the server starts at the slot's
confirmed position if that is later) and skips any transaction whose end LSN
is not greater than the resume LSN. Because the slot is only acknowledged up
to durable commits, every transaction after the resume LSN is still
available upstream. A snapshot chunk that was in flight is redone from the
logged boundary.

## The DBLog snapshot algorithm

Initial snapshots (and re-snapshots after schema changes) copy tables in
chunks *while the change stream keeps flowing*, without locking tables and
without a replication-slot snapshot, following the DBLog watermark idea: the
chunk's reads are ordered against the stream by writing *watermark* messages
into the WAL around them.

### Per-chunk protocol

`PgSource::maybe_start_chunk` runs whenever no upstream transaction is open
in the stream and no chunk is in flight:

1. Pick the table with the lowest id whose snapshot is not done.
2. On a normal connection, emit the **low watermark**:
   `SELECT pg_logical_emit_message(true, 'ivm', 'L:<source>:<table>:<run>:<seq>')`
   (a transactional message: it appears in the stream at its commit).
   `<run>` is a random id chosen each time the source thread starts and
   `<seq>` counts its chunks, so the tag is unique: after a restart the
   stream resumes before watermarks of the previous run, and logical
   messages reach every slot of the database, including those of other
   engines.
3. Read the chunk in one transaction:
   ```sql
   BEGIN ISOLATION LEVEL REPEATABLE READ;
   SELECT pg_current_snapshot()::text;
   SELECT <columns> FROM <schema>.<table>
     [WHERE (<pk>) > (<boundary>)] ORDER BY <pk> LIMIT 10000;
   COMMIT
   ```
   String key columns use `COLLATE "C"`, so the upstream order matches the
   engine's key order. The rows are kept in memory keyed by their encoded
   primary key; the chunk records its last key (`max_key`), whether it is the
   last chunk (fewer than 10,000 rows), and the snapshot's
   `(xmin, xmax, xip)`.
4. Emit the **high watermark** `'H:<source>:<table>:<run>:<seq>'`.

The chunk starts in phase *before-low*. While the stream is processed:

- the low watermark switches it to phase *in-window*;
- the high watermark marks the transaction carrying it; at that
  transaction's commit, the chunk's **remaining rows are added as upserts to
  that commit**, the table's boundary becomes the chunk's last key (and the
  table is done if this was the last chunk), the commit is logged, and then a
  `Progress` record with the new state.

### Filtering stream changes

For every streamed change of key `k` in table `T` (`snapshot_filter`):

| Situation | Action |
|---|---|
| `T`'s snapshot is done | apply |
| `k <= boundary` (already copied) | apply |
| a chunk of `T` is in flight and `k` is in its range (`k <= max_key`, or it is the last chunk): phase *in-window* | apply, and remove `k` from the chunk (the stream's value is at least as new) |
| same, phase *before-low*, the transaction is **not** visible in the chunk's snapshot | apply, and remove `k` from the chunk |
| same, phase *before-low*, the transaction **is** visible in the chunk's snapshot | skip (the chunk already contains its effect) |
| otherwise (`k` beyond everything copied so far) | skip: a later chunk will read the row's current value |
| `TRUNCATE` | apply always, and clear the chunk's rows of that table |

Why this yields a consistent copy (with one exception, below): the chunk's snapshot is taken after the
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

The exception, found by reading the code (not reproduced): a `TRUNCATE` that
committed before the low watermark but is processed while the chunk is in
flight clears the chunk's rows, although the chunk's snapshot already
reflects the truncation. Rows inserted after the `TRUNCATE` and visible in
the snapshot are then skipped by the stream (phase *before-low*, visible)
and missing from the chunk, so they are never copied.

The 32-bit transaction ids of the stream are compared with the 64-bit
snapshot by choosing the epoch closest to the snapshot (`xid_visible`).

### Tables without a primary key

Tables with `REPLICA IDENTITY FULL` are read in a single chunk without
`WHERE`/`ORDER BY`/`LIMIT`. Their changes are skipped while the table has not
been copied yet, including during phase *before-low*; at the low watermark
the chunk's rows are emitted as inserts; in the window, changes from
transactions visible in the chunk's snapshot are skipped and others applied;
after the high watermark's commit the table is done. Unlike the keyed path,
a transaction that commits just before the low watermark but is not visible
in the snapshot is not caught.

### Consequences

- The source's LSN frontier does not advance while any of its tables is
  incomplete, so `AS OF AT LEAST LSN` reads never see a partial table; fresh
  reads wait for the snapshot explicitly.
- A restart in the middle of a snapshot resumes after the last logged
  boundary.
- `ALTER SOURCE ... REFRESH` adds tables that are snapshotted the same way.
- After an upstream schema change the table's state is reset and the whole
  table is copied again; because a re-snapshot only upserts, rows deleted
  upstream while their key is beyond the boundary are not removed (see
  [Sources](../user-guide/sources.md#upstream-schema-changes)).
