# Operations

Running `ivm` in practice: configuration and tuning, the data directory,
recovery, checkpoints, retention, durability, and cleaning up. The byte-level
formats are in [On-disk format](../on-disk-format.md); the mechanisms in
[Recovery and snapshots](../internals/recovery.md).

## Configuration and tuning

All configuration is on the command line ([flags](getting-started.md#command-line-flags)).

| Knob | Default | Trade-off |
|---|---|---|
| `--memory-mb` | 2048 | The memory budget: the buffer pool of index nodes, the aggregation group caches, the records waiting to be applied and the changes queued for subscriptions and sinks together ([Memory](#memory)). The buffer pool gets what the others leave; clean index nodes beyond its share are evicted (CLOCK) and re-read from segment files on demand. Working sets that fit run from memory; larger ones degrade gradually. `--cache-mb` is an older name of this flag (it used to size the buffer pool alone, counted in serialized bytes: about a quarter of the memory it took). |
| `--checkpoint-secs` | 10 | Longer intervals mean less checkpoint I/O but longer log replay after a crash, and more dirty memory. The engine thread is busy during a checkpoint, so reads and maintenance pause while it runs. |
| `--retention-gsns` | 1000 | History kept for `AS OF`, in GSNs (every logged input, including each clock tick, is one GSN). More retention keeps more versions in the indexes, more input-log segments on disk, and more clock ticks in each checkpoint. `0` keeps no history beyond the previous GSN. |
| `--tick-ms` | 1000 | Granularity of `now()` for temporal filters. Each tick is one log record and one GSN (about 25 bytes of log per tick). `0` disables ticks and freezes `now()`. |
| `--no-sync` | off | Skips `fsync` of the input log. Only for tests and benchmarks; see [Durability](#durability-and---no-sync). |

Fixed internal parameters (not configurable):

| Parameter | Value |
|---|---|
| target size of index leaf and internal nodes | 16 KiB |
| storage segment size (roll-over threshold) | 256 MiB |
| input-log segment size (roll-over threshold) | 64 MiB |
| maximum records per sequencer batch (group commit) | 4096 |
| records waiting to be applied, before sources wait | an eighth of `--memory-mb` (at least 4 MiB), in bytes of memory |
| changes queued per subscription or sink | a sixteenth of `--memory-mb` (at least 4 MiB), in bytes of memory |
| aggregation group caches, all together | an eighth of `--memory-mb` |
| buffer pool | what the above leave of `--memory-mb`, at least a quarter of it |
| unacknowledged commits per source | 10,000 |
| initial-snapshot chunk | 10,000 rows |
| nodes relocated per garbage-collection pass | 1000 |
| manifest rewrite threshold | 8 MiB |
| DDL statements kept in the catalog history | 10,000 (trimmed back to 10,000 once there are more than 11,000) |
| clock ticks saved in a checkpoint | the retention window, at most 100,000 |
| source reconnect delay | 200 ms, doubling up to 10 s |
| retry of a table that cannot be copied | 1 s, doubling up to 60 s |
| sink reconnect delay | 100 ms, doubling up to 10 s |
| sink flush | > 1000 pending rows or 100 ms; progress also every 5 s while idle |
| retry of a dropped source's slot drop | 250 ms, doubling up to 10 s |
| wait for source and sink threads at shutdown | 5 s |

Things to keep in mind when sizing:

- **One dataflow thread.** All maintenance and all reads run on the engine
  thread, so the engine uses about one core for its work (plus the
  sequencer, one thread per source, sink and client session, and three
  threads for the web UI when it is enabled).
- **Memory**: see [below](#memory). Outside the budget: hydration of a
  new view holds snapshots of intermediate results, an ad-hoc query's
  result is built in memory, and each session, source and sink thread has
  its own buffers.
- **Shared work**: identical sub-plans and identical indexes are shared
  between views; `SHOW ARRANGEMENTS` shows the reference counts.

### Memory

`--memory-mb` is one budget for the memory the server keeps data in. Every
part is counted in bytes of memory — the sizes of the structures and of the
heap blocks they own — not in serialized bytes or numbers of records, so
the limits mean the same for narrow and wide rows:

| Part | Limit | When it is reached |
|---|---|---|
| records logged (or submitted to the log) that the engine has not applied yet | an eighth (at least 4 MiB) | sources wait before logging more, so they stop reading the replication stream for a while (the upstream keeps the WAL); the engine never waits, so it always catches up. A single record larger than the limit is let in once the queue is below it |
| changes queued for one subscription | a sixteenth (at least 4 MiB) | the subscription ends with `53000` and its queue is dropped ([Reads](reads.md#ending-a-subscription)) |
| changes queued for one sink and not written yet | a sixteenth (at least 4 MiB) | the sink is sent nothing more, and once it has written what it holds it is restarted from a snapshot, rewriting its target ([Sinks](sinks.md#known-problems)) |
| aggregation group caches (all of them) | an eighth | the group is not cached; after the record every cache drops the groups not used since the last time this happened, and at least half of its size, so idle aggregations give way |
| buffer pool (index nodes) | the rest, at least a quarter | clean nodes are evicted (also between the leaves of a scan); dirty nodes stay until the next checkpoint, which is forced when they exceed half of the pool's share |

Queued changes are shared: a change delivered to several subscriptions and
sinks is held once (each consumer's queue still counts it). The web UI's
`/api/overview` reports each part (`cache_used_bytes` against
`cache_capacity_bytes`, `group_cache_bytes`, `input_queue_bytes`,
`output_queue_bytes`; [Web UI](../web-ui.md#get-apioverview)).

Subscriptions and sinks can queue a sixteenth each, so many slow consumers
can together exceed the budget; the buffer pool then shrinks to its
quarter.

## The data directory

```
<data-dir>/
├── LOCK                              exclusive lock while a process uses the directory
├── log/
│   └── log-<first GSN, 20 digits>.ivl   input-log segments
└── store/
    ├── MANIFEST                      checkpoint records
    ├── MANIFEST.new                  only during a manifest rewrite
    └── seg-<id, 8 digits>.dat        index node segments
```

- **Input log** (`log/`): every logged input — upstream transactions, local
  DML, DDL, source progress, schema changes, clock ticks — with its GSN and a
  checksum. It is the source of truth: everything else can be recomputed
  from it plus the last checkpoint.
- **Store** (`store/`): the persistent indexes (copy-on-write B+trees) in
  append-only segment files, and the manifest, whose last valid record names
  the tree roots of the last checkpoint together with the catalog and the
  engine's state.

The stop file `.ivm-stop` lives in the process's working directory, not in
the data directory ([Stopping](getting-started.md#stopping-the-server)).

## The lock file

On start the engine creates `LOCK` (if missing) and takes an exclusive
advisory lock on it (`flock` through `File::try_lock`), held until the
process exits — also when it crashes. A lock that is taken is retried for
up to two seconds; then the process prints
`ivm: failed to start: data directory <dir> is in use by another ivm process`
and exits with status 1. The file is never deleted and has no contents.

Do not delete `LOCK` while a process runs: a new process would create a new
file, lock it, and run on the same directory concurrently.

## Recovery

Every start is a recovery (the listen address is bound before it, so
clients that connect meanwhile wait):

1. Lock the data directory.
2. Open the input log: scan the last segment, truncate a torn tail (a frame
   whose length or checksum does not match), and determine the last GSN.
3. Open the store: read the manifest, take its last valid record as the
   checkpoint, truncate the active segment to the length recorded in it, and
   delete segment files the checkpoint does not reference (written after
   it). A store whose manifest is missing (while segment files hold data)
   or has no valid record stops the start, untouched.
4. Restore the catalog and engine state from the checkpoint, rebuild the
   dataflow from the catalog (tables first, then materialized views and
   indexes, each after what it reads), and attach every index whose
   structure matches one in the checkpoint. A table whose primary index is
   not in the checkpoint stops the start (its rows exist nowhere else).
   Indexes that do not match are rebuilt from the recovered ones. A
   materialized view or index whose stored definition can no longer be
   built (code that became stricter, or a catalog that drifted) does not
   stop the start: it is logged and kept in the catalog as **broken**,
   together with the indexes on it, the views reading it and the sinks
   reading it (which are not started).
5. Restart sinks from the checkpoint's state (a target that is up to date
   resumes; one that is behind is resynchronised), then replay the
   input-log records after the checkpoint's GSN, which also reach the sinks.
   Damage before the last segment, or a gap in the GSNs, stops the start
   (`input log segment ... is corrupt`, `input log is missing records`)
   instead of being replayed around. If the log ends before the
   checkpoint's GSN (a lost unsynced tail, see
   [Durability](#durability-and---no-sync)), numbering continues after the
   checkpoint.
6. Restart sources (from their logged progress) and the retries of pending
   slot drops, start the sequencer and engine threads, and accept
   connections.

Recovery time is dominated by log replay (bounded by the checkpoint
interval) and by rebuilding indexes that could not be matched. Sources only
resume after replay; meanwhile the upstream retains WAL in the slot.

Startup failures and what they mean:

| Message | Cause |
|---|---|
| `data directory ... is in use by another ivm process` | another process holds `LOCK` |
| `cannot decode checkpoint state: ...` | the checkpoint was written by an incompatible version ([Compatibility](../on-disk-format.md#compatibility)) |
| `the checkpoint has no index for table <id> (version <v>); refusing to start without its rows` | the table's primary index cannot be found by its structural fingerprint — usually an incompatible version |
| `node checksum mismatch at ...`, `cannot open segment ...` | damaged or missing store files |
| `input log segment ... is corrupt at byte ...`, `input log is missing records: expected gsn ..., found ...` | a damaged or deleted input-log segment holding records after the checkpoint; those records exist nowhere else. Restore the data directory from a backup |
| `.../MANIFEST has no valid checkpoint record ...`, `.../MANIFEST is missing but segment files hold data ...` | the manifest is damaged or was deleted; nothing was modified. Restore it (or the data directory) from a backup. If the engine crashed during its very first checkpoint, removing `store/` is safe: the store is rebuilt from the log (start-up refuses if the log no longer starts at GSN 1) |
| a log record other than DDL that fails to apply during replay (`failed to start: ...` with the record's error; a failing DDL record is only logged, `DDL at gsn ... failed on replay`) | a bug: a record that could be applied when it was logged must apply again |
| a panic (`thread 'main' panicked at ...`, `has overflowed its stack`) | a bug triggered by the catalog or the log |

A broken object is not maintained. Reading it, or anything reading it,
fails with SQLSTATE `55000` and the reason (`materialized view "v" is broken:
column "b" does not exist`, with the hint to drop or replace it), and so do
`SUBSCRIBE`, `EXPLAIN DATAFLOW` and `EXPLAIN MAINTENANCE` of it, `CREATE
INDEX` on it, `CREATE SINK` from it and new views over it. `SHOW VIEWS`
(`status`), `SHOW SINKS` and the web UI (catalog and errors pages) show the
reason. `DROP` removes it (with `CASCADE`, what depends on it), and
`CREATE OR REPLACE MATERIALIZED VIEW` with a definition that plans repairs
it: the view is built and hydrated again, the views reading it with it, its
indexes are rebuilt and its sinks start. Every start tries to build broken
objects again, and a change that re-plans a broken view rebuilds it if it
plans by then (a broken view never blocks the replacement of another
view).

## Checkpoints and garbage collection

A checkpoint is taken every `--checkpoint-secs` seconds if anything was
applied since the last one, when the memory of dirty index nodes exceeds
half of the buffer pool's share of `--memory-mb` (also only if something
was applied), on the `CHECKPOINT`
statement, and as the final step of a requested shutdown. It:

1. serialises the engine state (the catalog and its DDL history, index
   fingerprints → tree ids and the GSN each index's history starts at,
   view error counts and the error records of operators, source states and
   LSNs, the logical clock and the recent clock ticks, pending slot drops)
   as JSON;
2. writes all dirty index nodes bottom-up to the active segment and `fsync`s
   the segment files;
3. appends a checkpoint record (tree roots, segment usage, the JSON state) to
   the manifest and `fsync`s it — this is the atomic commit point;
4. deletes sealed segment files with no live nodes;
5. runs one garbage-collection pass;
6. asks the sequencer to delete input-log segments whose records all lie at
   or before `checkpoint GSN − retention`.

Space reclamation: a node rewritten after a checkpoint makes its old copy
dead. Garbage collection picks the oldest sealed segment that is less than
half live and rewrites up to 1000 of its live nodes (they are written again
at the next checkpoint); a segment with no live bytes is deleted at the next
checkpoint. The active segment is never collected, so up to one segment
(256 MiB) of dead space can accumulate before it is sealed.

`SHOW FRONTIERS` shows `checkpoint_gsn` and the number of segments.

## Retention and time travel

Each index entry keeps a history of (GSN, weight) changes. Changes at or
before the *retention frontier* (`last applied GSN − --retention-gsns`) are
merged into one when an index leaf is next rewritten, and `AS OF` reads
before the frontier are refused. The same bound decides which input-log
segments a checkpoint may delete. See [Reads](reads.md#time-travel-as-of-gsn).

## Durability and --no-sync

By default the sequencer `fsync`s (`fdatasync`) the input log after writing
each batch of records and only then hands the records to the engine and
acknowledges them: a client's DML reply, a source's slot acknowledgement and
a DDL reply all imply that the record is on disk.

With `--no-sync` records reach the operating system but not necessarily the
disk. A crash of the `ivm` process alone loses nothing, but an operating
system crash or power loss can lose records that were already acknowledged —
including upstream transactions whose WAL the source has already released,
so they are not replicated again. Checkpoints are always `fsync`ed, so after
such a crash the last checkpoint can be newer than the end of the surviving
log; the next start then continues numbering after the checkpoint (logged as
`the input log ends at gsn ... before the checkpoint at ...`), keeping the
checkpointed state. Records after the checkpoint that were lost stay lost,
and a sink may already have written their changes and recorded progress
past the checkpoint, so that it skips the new records with those GSNs
(delete its row from `ivm_sink_progress` to resynchronise it). Use
`--no-sync` only for disposable data.

## Stopping and restarting

See [Stopping the server](getting-started.md#stopping-the-server): `SIGINT`,
`SIGTERM` and the stop file take a final checkpoint, so the next start has
next to nothing to replay (the clock ticks the sequencer logged while
stopping). Because recovery replays the log, any other way of stopping — a
second signal, `SIGKILL`, a crash — loses no acknowledged change either.
Upstream WAL is retained in the slots while the engine is down.

## What is safe to delete

While the engine runs: nothing in the data directory.

While it is stopped:

| File | Safe to delete? |
|---|---|
| `LOCK` | yes (it is re-created) |
| `store/MANIFEST.new` | yes — a leftover of an interrupted manifest rewrite; it is never read |
| `store/seg-*.dat` not referenced by the last checkpoint | they are deleted automatically on the next start |
| other `store/` files | **no** — the checkpoint refers to them |
| `log/*.ivl` | **no** — the engine deletes old segments itself. Removing a segment that holds records after the last checkpoint loses them: a gap in the GSNs stops the next start (`input log is missing records`), and removing the newest segments loses their records without an error |
| the whole data directory | yes, to start from scratch — then drop the upstream slots and, if needed, the sink targets |

## Backups

There is no online backup. Copy the whole data directory while the engine is
stopped. (Sealed segment files never change, and the manifest and log are
append-only, which makes incremental copies cheap, but a consistent copy of a
running engine is not supported.) A restored copy resumes its sources from
the slot positions it had logged; if the upstream slot has moved on in the
meantime, changes in between are missing (a slot that no longer exists is
re-created and the tables copied again). Sinks resume after the progress
their targets recorded: a target that is ahead of the restored copy skips
its changes up to that GSN. Before starting a restored copy, delete the
sinks' rows from `ivm_sink_progress` in the targets' databases, which makes
them resynchronise their targets.

## Cleaning up upstream slots

See [Sources](sources.md#cleaning-up-upstream-slots). Every source holds one
slot; a slot whose engine is gone retains WAL indefinitely.

## Monitoring checklist

- `SHOW FRONTIERS`: `durable_gsn` − `processed_gsn` is the apply backlog;
  `checkpoint_gsn` should advance every `--checkpoint-secs`.
- `SHOW SOURCES`: `status` (`streaming`, `snapshotting ...`, `error: ...`,
  `error: cannot copy table ...`) and `lsn`; upstream,
  `pg_replication_slots.confirmed_flush_lsn` should track
  `pg_current_wal_lsn()`.
- `SHOW SINKS`: `running (gsn n)` should follow `SHOW GSN`.
- `SHOW VIEWS`: the `errors` and `status` columns (error records, broken
  views).
- The server log (standard error), e.g. `RUST_LOG=info`.
- The [web UI](../web-ui.md) shows all of these, with history.
