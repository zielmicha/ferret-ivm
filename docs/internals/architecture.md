# Architecture: threads, messages and the clock

This chapter describes how the parts of the engine fit together: the
threads, the messages between them, the GSN clock and the frontiers derived
from it, and group commit. It describes the code in `crates/ivm/src/coord/`
(`mod.rs`, `seq.rs`, `api.rs`, `feed.rs`, `read.rs`), `source/`, `sink/` and
`server/`.

The design follows [doc.md](../../doc.md): one representation (rows with an
integer weight, stamped with a GSN), one clock (the GSN assigned to every
input on arrival), and determinism (the state at GSN *g* is a pure function
of the input log up to *g*).

## Design map

Where the ideas of the design live in the code (paths relative to
`crates/ivm/src/` unless noted):

| Design (doc.md) | Code |
|---|---|
| One representation: rows with a weight, stamped with a GSN | `dataflow` (Z-set deltas, consolidated per GSN), `storage` (each entry's current weight), `log.rs` |
| One clock: a GSN for every input, in arrival order | `coord/seq.rs` (sequencer, group commit); every upstream commit is logged with its LSN, from which the per-source LSN frontiers follow (`coord/mod.rs`) |
| Determinism; the wall clock as a logged tick stream | `LogRecord::Tick`, `dataflow/temporal.rs` (temporal filters), `coord/ddl.rs` (`check_deterministic`) |
| CDC edge: primary-key upserts, before-images and unchanged TOAST values from stored rows, DBLog chunked snapshots, slot acknowledged on log durability, schema in the log | `source/postgres.rs`, `dataflow/ops.rs` (`table_step`) |
| Ordered index keyed by (key, value), buffer pool (without the histories: below) | `storage` (copy-on-write B+trees of weighted entries, segments, manifest, CLOCK eviction, the maintenance pass: segment garbage collection, merging) |
| Augmented summaries; aggregates as range queries | `storage/summary.rs`, `dataflow/reduce.rs`, `Env::nth` (percentiles) |
| Shared, reference-counted arrangements; the primary index as a table's first arrangement | `dataflow/mod.rs` (hash-consed nodes and arrangements) |
| Delta joins; anti-joins for outer joins and `NOT EXISTS` | `dataflow/join.rs`, `dataflow/lower.rs` |
| Changing views: shared fragments, hydration from arrangements, catalog in storage | `dataflow/mod.rs` (`Graph::hydrate`), `coord/ddl.rs`; the catalog and its DDL history in a storage tree, versioned by GSN (`coord/meta.rs`) |
| Outputs: reads (fresh, latest, at least an LSN), `SUBSCRIBE` with progress, idempotent sinks | `coord/read.rs`, `coord/feed.rs`, `server/`, `sink/postgres.rs` |
| `EXPLAIN MAINTENANCE` | `coord/read.rs` |
| Expression semantics exactly PostgreSQL's | `expr/`, and the ports `crates/pgnumeric`, `crates/pgdatetime`, `crates/pgfuncs` |

Where the implementation differs from the design:

- **One dataflow thread.** Sources, the sequencer and sessions are separate
  threads, but the dataflow is not partitioned across cores.
- **Atomic checkpoints** of all indexes at one GSN instead of per-arrangement
  durable frontiers ([Recovery](recovery.md)).
- **No LSN→GSN mapping.** Each commit record carries its LSN, and
  `AS OF AT LEAST LSN` waits for the per-source LSN frontiers.
- **No time travel.** The design keeps each index entry's history of
  (GSN, weight) changes behind a retention frontier, so that reads,
  hydration and recovery could all read the state as of a GSN. The indexes
  keep each entry's current weight only. Nothing but `AS OF <gsn>` reads
  needed an earlier state, and they are not supported: hydration reads the
  current state (off the engine thread from a fork of the storage, then
  catching up from the records it is forwarded), recovery replays the log
  after a checkpoint, and the one earlier state the dataflow needs, an
  arrangement as it was before the GSN being applied, is its current state
  with that GSN's delta overlaid ([Dataflow](dataflow.md#arrangements-and-layouts)).
  Histories had a cost that grew with the retention: a retracted entry
  stayed in its leaf until the retention frontier passed it and a
  maintenance pass compacted it, a lookup of a hot row walked every version
  kept, and an entry took 56 bytes besides its key in memory (24 now), with
  compaction machinery (per-subtree compaction states, budgets) to bound
  the rest. Without them a deleted row leaves its index at once, and the
  maintenance pass only reclaims dead space in segment files.
- **Recursion by recomputation.** `WITH RECURSIVE` recomputes the fixpoint
  instead of using nested time ([Dataflow](dataflow.md#recursive)).
- **Planning.** Join orders use equality edges, unique keys and exact index
  sizes; the delta rates that `EXPLAIN MAINTENANCE` measures are not used.
  Whether a view is materialized is the user's choice.
- **Not implemented:** shipping the log or segments to object storage,
  replicas, multiple nodes, Kafka sinks, user-defined aggregates.

## Threads

| Thread | Owns | Does |
|---|---|---|
| main | | parses flags, starts the signal and stop-file threads, binds the listening sockets (TCP or Unix-domain, `listen.rs`), runs recovery (`coord::start`), starts the web UI (with `--http-listen`) and the listeners, then waits for a shutdown request (or for the engine to stop by itself) and shuts down after a final checkpoint, removing the files of Unix-domain sockets (`main.rs`; exit status 0, or 1 when a thread or the final checkpoint failed) |
| `ivm-listener` (one per `--listen` address) | a listening socket | accepts connections (`server::Server::serve`) and starts a session thread for each; the listeners share process IDs and cancel keys |
| `ivm-signals` | | turns the first `SIGINT`/`SIGTERM` into a shutdown request; a second one exits the process at once (status 1) |
| `ivm-sequencer` | the input-log writer | assigns GSNs, appends and `fsync`s records in batches, generates clock ticks, acknowledges submitters, forwards durable records to the engine |
| `ivm-engine` (64 MiB stack) | storage (`Env`), dataflow (`Graph`), catalog, runtime state | applies durable records in GSN order, answers queries, parks waiting reads, delivers changes to subscriptions and sinks, begins and finishes checkpoints, builds the web UI's snapshots |
| `ivm-reader-<n>` (four, started by the first large read; 64 MiB stack) | forks of the storage, copies of parts of the graph | run reads that exceeded their budget on the engine thread ([Planner](planner.md#batch-execution)), streaming their rows to their sessions as these take them; read large snapshots for subscriptions and for sinks resynchronising their targets, likewise |
| `ivm-checkpoint` (while a checkpoint is written) | a frozen version of the dirty nodes and the manifest (the append side is shared) | writes the nodes, `fsync`s and appends the manifest record with the header naming the metadata tree ([Recovery](recovery.md#checkpoints)) |
| `ivm-session-<n>` (one per client, 16 MiB stack) | the client connection | wire protocol, session settings, source discovery; forwards statements to the engine and streams results |
| `ivm-source-<name>` (one per source) | the replication connection | turns `pgoutput` messages and snapshot chunks into log records, acknowledges the slot; never blocks on a snapshot query or on the log |
| `ivm-copy-<source id>` (one per source, from its first snapshot chunk on) | the snapshot connection | reads snapshot chunks and writes their watermarks for the source thread ([Recovery](recovery.md#the-dblog-snapshot-algorithm)) |
| `ivm-sink-<name>` (one per sink) | the target connection | writes changes to PostgreSQL |
| `ivm-hydrate` (while a view, index or subscription is hydrated; 64 MiB stack) | a fork of the storage and of part of the graph | hydrates the new dataflow fragment as of its statement's GSN, then applies the records the engine forwards until it has caught up ([Dataflow](dataflow.md#hydration-off-the-engine-thread)) |
| `ivm-stop-file` | | polls for `.ivm-stop` every 500 ms; when it appears, removes it and requests a shutdown |
| `ivm-http` (with `--http-listen`: a driver thread and the two worker threads of its own tokio runtime) | the HTTP listeners | the web UI's server, its once-a-second metrics sampler and the SQL console ([Web UI](../web-ui.md)); engine snapshots are requested with a callback, so no async thread blocks on the engine |
| `ivm-slot-drop-<slot>` | | drop the replication slot of a dropped source, retrying with a delay doubling from 250 ms to 10 s (also while the stopping source's session still holds it) until it is dropped or gone; pending drops are part of the checkpointed state and started again after recovery |

There is exactly one engine thread: all dataflow maintenance, all index
reads and writes, and all query execution happen on it, except the
hydration of new views, indexes and subscriptions and large reads, which
run on forks of the storage, and writing checkpoints. Everything that
talks to the network (sessions, sources, sinks) runs on other threads, so
the engine never blocks on a remote server: stopping a source or a sink
(`DROP SOURCE`, `DROP SINK`, a cascading drop, a sink restarted after a
replacement) only sends the thread a stop request, and at shutdown the
engine waits at most 5 s for them.

Communication uses `crossbeam` channels (unbounded, except one-shot reply
channels of capacity 1) and a small block of shared state
(`coord::api::Shared`). What the channels hold is bounded in bytes of
memory instead ([Memory](#memory)): records submitted to the log and not
yet applied by sources that stop reading while they fill their part, the
queues to subscriptions and
sinks by their consumers' limits, the rows a reader thread streams to a
session or sink by the few chunks in flight (`coord::stream`). The web UI
keeps at most 16 of its
snapshot requests queued at the engine.

| Shared field | Meaning |
|---|---|
| `durable_gsn` | last GSN made durable by the sequencer |
| `processed_gsn` | last GSN applied by the engine |
| `source_lsn` | per-source LSN frontier (mutex-protected map) |
| `lsn_waiters` | some read is waiting for an LSN frontier |
| `lsn_wanted` | the smallest LSN such a read waits for: a source wakes the engine when its frontier passes it |
| `want_keepalive` | ask sources to request a keepalive (to move their frontier) |
| `source_gauges` | per-source upstream position and ingest counters (web UI) |
| `slot_drops` | replication slots of dropped sources still to be dropped (checkpointed) |
| `shutdown` | stop flag |
| `failed` | the engine or sequencer thread stopped with an error (also a failed final checkpoint or a panic); the process then exits with status 1 |
| `crash` | stop without the final checkpoint (a simulated crash, for tests) |
| `mem` | the memory budget, with the memory of the records waiting to be applied and of the changes queued for subscriptions and sinks ([Memory](#memory)) |

## Message flow

```
  upstream PostgreSQL                            clients
        │ pgoutput, snapshot queries               │ pgwire
        ▼                                          ▼
  ┌──────────────┐  Append(rec, ack)        ┌──────────────┐
  │ source thread│ ───────────────┐         │session thread│
  └──────────────┘                │         └──────┬───────┘
        ▲  ack(GSN) = durable     │                │ Request::Execute / Describe
        │                         ▼                ▼
        │                  ┌─────────────┐  Durable(batch)  ┌──────────────┐
        └───────────────── │  sequencer  │ ───────────────▶ │ engine thread│
                           │ GSN, append,│                  │ apply, read, │
          Append(rec) ───▶ │ fsync, ticks│ ◀─── Append ──── │ checkpoint   │
     (DML, DDL from engine)└──────┬──────┘   Truncate        └──────┬───────┘
                                  │                                │
                           log/*.ivl (input log)        store/ (indexes, manifest)
                                                                   │
                                   ChangeEvent (change feeds) ◀────┘
                                     ├─ subscriptions (SubEvent alias)
                                     └─ sink threads
```

### Upstream transactions

1. The source thread collects the changes of one upstream transaction and,
   at its commit, sends `SeqMsg::Append { rec: Commit, ack: Some(tx) }` to the
   sequencer without waiting (`log_now`: neither for the record to be
   durable nor for room, see [Group commit](#group-commit)). Several
   commits can be in flight.
   It holds at most `log::PART_BYTES` (1 MiB) of a transaction's changes:
   beyond that it writes them, as they arrive, to a spool file in the log
   directory (`log::Spool`), and at the commit sends the finished spool
   instead (`SeqMsg::AppendSpooled`, `log_spooled`), which the sequencer
   renames to the parts file of the commit's GSN and logs as a
   `CommitParts` record ([On-disk format](../on-disk-format.md#parts-files)).
   With protocol version 1, `pgoutput` sends a transaction only once it has
   committed, so a spool is never aborted; a session that ends in the middle
   of one deletes it (the upstream sends the transaction again).
2. The sequencer assigns the next GSN, appends the record, and at the end of
   the batch `fsync`s the log, publishes `durable_gsn`, sends the GSN on each
   `ack` channel, and forwards the batch to the engine as
   `EngineMsg::Durable`.
3. The source thread sees the ack, advances its *durable LSN* to the
   transaction's end LSN (acks are consumed in order) and reports it to the
   upstream as the slot's confirmed position. It also advances the source's
   LSN frontier in `Shared::source_lsn` (only while every one of its tables
   is completely copied: not during an initial snapshot or a re-snapshot),
   which wakes reads waiting for an LSN.
4. The engine applies the record: the changes go through the dataflow at
   that GSN (`Graph::step_observed`, which returns the rows of the nodes
   subscriptions and sinks read), and the outputs are delivered to
   subscriptions and sinks. A `CommitParts` record's changes are read from
   its parts file and fed to the dataflow one part at a time
   (`apply_commit`, `Graph::step_feed`), which applies them to the tables
   in bounded chunks; the GSN is then propagated (`Graph::step_finish`). A
   commit is never held in memory whole, however large.

Source progress (snapshot bookkeeping, as `ProgressDelta` records) and
schema records are sent the same way and count as in flight, at the LSN of
the commit before them, until durable. A source with 10,000 records in
flight, or while the records waiting to be applied fill their part of the
memory budget, stops reading the stream until some are durable or
applied, but goes on sending status updates, so its session stays up.

### Local DML

The session sends the statement to the engine. The engine evaluates it
against its current state, converts it into row changes, and submits one
`Commit { source: 0, lsn: <request id>, ... }` record to the sequencer (a
statement that changes nothing is answered at once, without a record). The
reply is sent when the record comes back durable and has been applied, so
the client's next fresh read sees it. DDL logged before the record applies
first: if it dropped the statement's table, the record changes nothing and
the statement fails (`the table of this statement was dropped before it
could be applied`), on replay too.

Several DML statements are in flight at once (`coord/dml.rs`): a statement
is evaluated as soon as it arrives and its record submitted, without waiting
for the records of earlier statements to be applied, unless it read rows
that one of those records writes. Its result could then differ from the one
it gets after that record; it is queued (`dml_queue`, in arrival order,
behind the first statement waiting) and evaluated again whenever a pending
DML record has been applied. The rows a statement reads are tracked while it
is evaluated (`Touched`): the primary keys it looks up (duplicate-key checks
of `INSERT`, new keys of `UPDATE`); for each table the plan of
`INSERT ... SELECT`, `UPDATE` or `DELETE` reads, the row with the key that
its predicates equate every key column with constants, or else every row;
for a materialized view, every row of every table. The rows a record writes
are its keys (every row of a table without a primary key). Local tables
change only through these records, and a statement that read nothing a
pending record writes sees what it would see after them: the results are
those of evaluating the statements one after another in log order, and
replay applies the logged changes, never re-evaluating.

### DDL

The session first does whatever needs the network (for `CREATE SOURCE` and
`ALTER SOURCE ... REFRESH`, discovering the upstream tables) and sends the
statement with the result (`extra`, JSON) to the engine. Some statements are
first validated against the current catalog (`submit_ddl`): `CREATE TABLE`
(the name, column types and primary key), `CREATE [MATERIALIZED] VIEW` (the
name, and as a dry run binding, planning, the determinism check and the
column list — not lowering or hydration), `CREATE SOURCE` and `ALTER SOURCE
... REFRESH` (the discovered tables; `CREATE SOURCE` also writes the data
directory's instance id into `extra`), and `CREATE SINK` (its relation and
`KEY` columns). The engine then submits a `Ddl { req, sql, extra }` record
and applies it when it comes back durable; the reply is sent then (for a
statement that built dataflow — `CREATE [OR REPLACE] MATERIALIZED VIEW`,
`CREATE INDEX` — once it has been hydrated off the engine thread, see
[Dataflow](dataflow.md#hydration-off-the-engine-thread)). Other
checks happen only while applying: `DROP` (which resolves and checks every
name before dropping anything), `CREATE INDEX`, the rules of
`CREATE OR REPLACE`, and lowering and hydration. A statement that fails there
leaves its record in the log; it fails again on replay, where the failure is
only logged. Replay applies the same record with the same `extra`, never
contacting the upstream.

### Clock ticks

When `--tick-ms` is non-zero, the sequencer waits for the first message of
a batch at most until the next tick deadline. Whenever the deadline has
passed — whether or not a message arrived first — it adds `Tick { micros }`
to the batch, with the current Unix time in microseconds (at least one more
than the previous tick), and sets the next deadline one interval later; so
ticks keep their schedule under a steady stream of appends. Ticks go through
the same path as any other record: they get a GSN, are made durable, and are
applied by the engine, which advances its logical clock and steps the
dataflow so that temporal filters release due events.

## Group commit

The sequencer processes messages in batches: it blocks for the first message
(or the tick deadline), adds a tick if one is due, then drains more messages
without blocking, up to 4096 per batch. All `Append`s of a batch are encoded
into one buffer, written with one `write` (or several: the buffer is
written out whenever it reaches 8 MiB), and made durable with one
`fdatasync` (an `AppendAll` — the records the engine submitted in one round
of messages, see below — counts as its records). A commit with more than
`log::PART_BYTES` of changes (local DML) is written to its parts file, made
durable first, and logged as a small `CommitParts` record, so no frame holds
more than about 1 MiB of changes. Only then are acknowledgements sent
and the records handed to the engine — no record is applied, acknowledged to
a source, or answered to a client before it is durable. Under load, batches
grow and the cost of `fsync` is shared; when idle, a batch is a single
record. `--no-sync` skips the `fsync` but keeps the order.

`SeqMsg::Truncate(gsn)` (sent after each checkpoint with its GSN) deletes
the log segments the checkpoint covers; `SeqMsg::Shutdown` stops the
sequencer after the current batch.

Backpressure is applied where records enter, in bytes of memory: while
the records submitted and not yet applied take their part of the memory
budget (`MemBudget::input_full`: an eighth, at least 4 MiB), a source
stops reading its replication stream (`PgSource::backlogged`; it goes on
sending status updates, so its session stays up and the upstream keeps
the WAL), and logs what it has already decoded without waiting
(`EngineHandle::log_now`): the queue exceeds its limit by at most what one
replication message produces. Other submitters wait for room
(`EngineHandle::log_async`, also behind the blocking `log`: polling every
millisecond, a record larger than the limit getting in once the queue is
below it, giving up with `57P01` when the server shuts down). Each record
is counted at its size in memory (`memory::record_bytes`: its rows as
decoded, not as logged, and its place in the queues) from its submission
until the engine has applied it — a `CommitParts` record, whose changes
are in its parts file, at its own small size; the engine's own appends
(DML, DDL) and the sequencer's ticks are counted without waiting. So the
sequencer's queue and the engine's queue of durable batches both stay
bounded, whatever the size of the records, and the sequencer itself never
waits. The engine never waits for the sequencer either (its appends are
not acknowledged synchronously), so it always catches up. (Sources also
stop once 10,000 of their records are unacknowledged, which bounds their
acknowledgement bookkeeping.)

## The engine loop

The engine thread waits for an `EngineMsg` with a 200 ms timeout and then
handles it and the messages already waiting behind it (up to 256 in a
round):

| Message | Action |
|---|---|
| `Durable(batch)` | apply each record in GSN order (`apply_record`), publish `processed_gsn`, send every sink the last applied GSN as its progress, re-check parked reads. While a hydration is in progress, a `Schema` record or a `Ddl` record replacing a view or creating a sink, and the records after it, are held until no hydration is; `Commit` and `Tick` records are forwarded to it before they are applied. A record that fails to apply is logged (`failed to apply gsn ...`); an internal (`XX...`) or I/O (`58...`) error stops the engine thread (`Shared::failed`, and the process ends with status 1); any other error (data errors are error records, so this is a bug) leaves the record possibly half-applied and not applied again: `processed` moves past it and its DML statement fails, so that neither the client, the DML queued behind it nor fresh reads wait forever |
| `Request(Execute)` | run a statement: DDL and DML are validated and submitted; reads are executed within the read budget (their rows sent to the session in chunks) or handed to a reader thread, or parked; `SUBSCRIBE` is registered (its snapshot a stream of chunks, read off the engine thread when large); `SHOW`, `EXPLAIN`, `CHECKPOINT` answered directly |
| `Request(Describe)` | result columns and parameter types of a prepared statement |
| `Request(Checkpoint)` | ask for a checkpoint: answered with its GSN once one begun after the request is committed |
| `Request(SinkSnapshot)` | a sink thread resynchronises its target: send it its relation at the last applied GSN as a stream of chunks of 10,000 rows (read within the read budget, or off the engine thread as the sink writes them), after the changes sent so far, and the changes after it from then on — also to a sink that had fallen behind; a sink held for a replacement still being hydrated is answered once it follows the new version ([Sinks](../user-guide/sinks.md#start-up-and-resynchronisation)) |
| `Request(Inspect)` | build a snapshot for the web UI (`coord/inspect.rs`) between two records, so that it is consistent at one GSN, and hand it to the requester's callback |
| `Wake` | a source frontier moved, or a statement was cancelled or its client went away: re-check parked reads |
| `Shutdown` | ask sources to stop, send every sink the last applied GSN as its progress (except one that fell behind, or one held for a replacement still being hydrated, which was not sent the records since) and ask it to stop, stop the sequencer, take a final checkpoint after the one being written, if any (not after a simulated crash, which only waits for that one to be written), wait up to 5 s for sources and sinks, exit |

After every round it runs deferred requests (queued DML, woken reads),
sends the records submitted in the round to the sequencer as one
`AppendAll` (so that the statements evaluated together share one log append
and `fdatasync`; `log_batches` in `Shared` counts the sequencer's batches),
finishes the hydration in progress if it has caught up (then applies the
records held behind it), finishes the checkpoint being written if it is
done, gives the buffer pool its share of the memory budget,
begins a checkpoint if one is due or asked for
([Recovery](recovery.md#checkpoints): clock ticks alone rarely make one
due), and removes cancelled subscriptions. The hydration thread sends
`Wake` when it has hydrated its fragment and whenever it has applied every
record forwarded so far; the checkpoint thread when it has written its
checkpoint.

`apply_record` dispatches on the record kind:

| Record | Effect |
|---|---|
| `Commit`, `CommitParts` | feed the changes to the dataflow by table (`Graph::step_feed`; changes to tables that no longer exist are skipped; a `CommitParts` record's are read from its parts file and fed one part at a time), then step the GSN (`Graph::step_finish`) and deliver outputs; for local DML reply to the client; for sources that still exist update the per-source last LSN |
| `Ddl` | apply the statement to the catalog and dataflow; on success append it to the catalog history (which keeps the most recent 10,000 statements, trimmed back to 10,000 once it exceeds 11,000); write the catalog's changes into the metadata tree at the record's GSN (`persist_meta`); reply |
| `Progress` | store the source's state (snapshot progress), if the source still exists |
| `ProgressDelta` | apply the change to the source's stored state (the listed tables' entries replace theirs), if the source still exists; the tables whose initial snapshot is loading, and the metadata tree's records of the state (at the next `persist_meta`), follow the listed tables only (`source_changed`) |
| `Schema` | rebuild the table in its new shape and re-plan dependent views, both hydrated off the engine thread ([Sources](../user-guide/sources.md#upstream-schema-changes), [Dataflow](dataflow.md#hydration-off-the-engine-thread)); mark the table's snapshot to be done again in the source's stored state, so that a restart before the re-snapshot logs progress still redoes it. A failure is only logged. Then, as for `Ddl`, `persist_meta` |
| `Tick` | advance the logical clock, step the dataflow (temporal filters), deliver outputs |

Subscriptions and sinks register in one change-feed registry
(`coord/feed.rs`). Each registration owns its node, generation, queue,
cancellation flag and delivery state; subscription metadata owns query
pins and relation dependencies, while sink handles own the external writer
thread and status. A cached node index lets `deliver` visit only feeds of
nodes that produced something, and supplies the nodes observed by steps.
Live delivery and replay after hydration use the same error, chunk and
progress operations.

`ChangeEvent` carries snapshots, deltas and progress (`SubEvent` is its
public subscription alias). Snapshot chunks are consumed before later
events. Deltas are shared (`SharedRows`) among feeds, with spilled outputs
read and released a chunk at a time. Subscriptions with `PROGRESS` receive
a mark after each complete GSN; sinks receive batch progress, including
at the end of replacement replay, and write only complete GSNs. Data and
progress both respect the queue limit ([Memory](#memory)). A new positive
error record ends a subscription; retracted errors do not. Sink feeds
continue following their relation when error records change.

Replacement holds a feed until the new version is hydrated. Its state
retains whether it had fallen behind and whether a snapshot was requested
meanwhile. A held or behind feed receives neither deltas nor progress;
a deferred snapshot comes from the new version, before subsequent changes.
Removing a registration invalidates its generation, so a replaced sink's
old thread cannot request a snapshot for the new one. PostgreSQL retries
and the idempotent progress-row writer remain in `sink/postgres.rs`.

Records at or below the last applied GSN are ignored, which makes replay
idempotent.

## Memory

One budget, `Config::memory_bytes` (`--memory-mb`), covers the memory the
engine keeps data in (`crate::memory`). Every part is measured in bytes of
memory — struct sizes plus the heap blocks they own, a block of *n* bytes
counted as *n* + 8 rounded up to 16 (`memory::heap_block`) — so that limits
do not depend on how wide rows are or how they serialize:

| Part | Meter | Limit | At the limit |
|---|---|---|---|
| records submitted to the log and not yet applied | `MemBudget::input` | an eighth, at least 4 MiB | sources stop reading their streams ([Group commit](#group-commit)) |
| changes and progress queued for a change feed, including unwritten sink changes and snapshot chunks in flight | `Feed::queue` (a child of `MemBudget::outputs`) | a sixteenth, at least 4 MiB per feed | terminate a subscription with `53000`; mark a sink behind until it resynchronises |
| query results in flight to sessions | `MemBudget::outputs` | a few chunks per read on a reader thread (it waits for its session); what a read on the engine thread returns, within its budget | the read goes on off the engine thread |
| aggregation group caches | `Graph::group_caches` | an eighth | the group is not cached, and all caches shrink after the record ([Dataflow](dataflow.md#reduce)) |
| transient working memory: in-memory deltas and key sets of a GSN, sort buffers of `TreeBuilder`s (the engine's and its forks') | `MemBudget::work` (`memory::Work`, which the engine gives its `Env`, `Env::set_work`, and forks share) | an eighth, set aside from the pool's share | a buffer spills to temporary trees (or applies to its index) beyond half the share (`Work::buffer`, within per-use bounds), or beyond a sixteenth of that while the buffers together exceed the share (`Work::full`) |
| the buffer pool of a fork of the storage (a hydration, a large read or snapshot) | `MemBudget::forks` (a charge each fork holds while it lives, `Env::hold`) | a sixteenth each (at least 1 MiB) | the fork writes and evicts its own nodes; those it shares with the engine are not counted |
| table changes forwarded to a hydration, not applied there yet | `MemBudget::forwarded` (a charge per forwarded batch of changes) | an eighth, at least 4 MiB | the engine waits for the hydration |
| buffer pool | `Env`'s own accounting ([Storage](storage.md#the-node-arena-buffer-pool)) | the rest, at least a quarter | clean nodes are evicted, dirty ones written and evicted |

After every applied record and every message, the engine sets the buffer
pool's target to what the other parts leave over (`rebalance_memory`,
`MemBudget::pool_target`), so that memory used by queues and caches is
taken from the pool, the one part that can always give memory back; it
also shrinks the group caches when one of them could not take a group. The
working memory's share is set aside rather than measured: its buffers live
within a record's processing, so between records it is mostly unused. The
pool writes dirty nodes early to keep them within half of its target (a
checkpoint is forced if they still exceed it after a message), and the
maintenance pass before a checkpoint rewrites no more nodes than the pool
has room for besides the dirty ones
([Storage](storage.md#space-reclamation)).

Queued changes release their memory wherever they are dropped: a charge
(`memory::Charge`) travels with each event and gives its bytes back when the
event is dropped — consumed, discarded with its channel, or (for a sink)
once written. Consumers never make the engine wait:

- **Subscriptions.** When an event would take a subscription's queue over
  its limit, the engine sends it an error (`53000`, `subscription ended: its
  client does not read changes as fast as they come ...`) instead and marks
  it cancelled. The session writes to its client with a write timeout of
  200 ms (`server::Conn`), retrying a write that made no progress; between
  attempts it checks whether the client closed the connection, and, while
  streaming a subscription, whether the statement was cancelled or the
  engine ended the subscription: it then marks the subscription cancelled,
  drops the queued events (releasing them) and reports the error once the
  client has taken what was written. Large changes are written as they are
  encoded (every 1 MiB), so a session buffers little of its own.
- **Sinks.** When an event would take a sink's queue over its limit, the
  sink falls behind: it is told so (`ChangeEvent::Behind`) and sent nothing
  more, not even progress marks (a progress row past changes the target
  did not get would let a restarted sink resume without resynchronising).
  Its thread drops the changes it held (releasing them) and, once it can
  write, asks for a snapshot (`Request::SinkSnapshot`), which it writes
  over its target; the engine sends it the changes after the snapshot's
  GSN from then on ([Sinks](../user-guide/sinks.md#delivering-changes)).
  This one bound covers what a sink keeps while it cannot write as well.
- **Snapshots and large results** are streams of chunks
  (`coord::stream`): a reader thread reads them from a fork as the
  consumer takes them, a couple of chunks ahead, and stops when the
  consumer goes away (or the statement or subscription is cancelled while
  it waits). A slow client holds its reader thread, not memory.

Not covered: the nodes a fork still shares with the engine once the
engine has evicted or replaced its own copies, the rows a hydration or a
join has in flight (a few batches, sized from the working memory), and the
buffers of session, source and sink threads.

## The GSN clock and frontiers

The **GSN** (global sequence number, `u64`) is assigned by the sequencer in
arrival order, contiguously from `last logged GSN + 1` (1 for an empty log).
If the log ends before the last checkpoint's GSN (with `--no-sync`, an
operating-system crash can lose an unsynced tail that a checkpoint already
covers), numbering continues after the checkpoint instead
(`LogWriter::skip_to`), so that no new record reuses a GSN the checkpoint
reflects. The GSN totally orders every input: transactions of all sources,
local DML, DDL, source progress, schema changes and clock ticks. Different
sources have no order between them other than their arrival order, which is
fixed once logged.

Frontiers derived from it:

| Frontier | Where | Meaning |
|---|---|---|
| durable GSN | `Shared::durable_gsn`, `SHOW FRONTIERS` | every record ≤ it is on disk |
| processed GSN | `Shared::processed_gsn`, `Engine::processed` | every record ≤ it has been applied; reads at "latest" use it |
| checkpoint GSN | manifest, `SHOW FRONTIERS` | the state ≤ it is in the last checkpoint; recovery replays from the next GSN |
| source LSN frontier | `Shared::source_lsn`, `SHOW SOURCES` | every upstream commit with an end LSN ≤ it has been logged |

Upstream LSNs are mapped to the GSN clock through the log: every source
`Commit` record carries the upstream transaction's end LSN. The engine keeps
the last logged LSN per source (checkpointed as the source's resume point).

Reads wait for these frontiers and then read the latest state
([Reads](../user-guide/reads.md#read-points)): a fresh read takes the
durable GSN at arrival and waits for the processed GSN to reach it (parked,
it is run again as `AsOf::AtLeastGsn`); an `AS OF AT LEAST LSN` read waits
until every source of the catalog has reported a frontier at or past the LSN
and then reads fresh (without any source it fails with `55000`).

### Parked reads

A read that has to wait is stored in `Engine::parked` with its reply channel
and a wait condition:

| Condition | Woken when |
|---|---|
| `Gsn(g)` | the engine has applied `g` (checked after every batch) |
| `Lsn(l)` | every source of the catalog has a frontier ≥ `l` (the frontier all sources have reached is computed once per check, for every waiting read); checked on `Wake` messages, which a source sends when its frontier passes `lsn_wanted` (not at every advance), after every batch and on the 200 ms timeout. When the last source is dropped, the read is re-run and fails |
| `Snapshot` | the tables it reads, directly or through views, whose initial snapshot was loading have all finished it, or the catalog changed (DDL or an upstream schema change: what the read reads may have changed); the read is re-run (planned again) and parks again if a table it reads is still loading. The loading tables are kept as source states and the catalog change, table by table (`Engine::loading`, `table_loading_changed`); source progress that finishes none of them does not re-check the reads |
| `Hydration` | the hydration in progress is done; a statement (read, DML or `SUBSCRIBE`) that read a view still being hydrated is re-run |

While an LSN read waits, `want_keepalive` makes the sources request a
keepalive from the upstream, so that the frontier advances to the server's
current WAL position even without traffic.

Each request carries its session's `Interrupts` (in `SessionVars`): a
`CancelRequest` sets `cancel` (and sends `Wake`), and the session sets
`gone` when its client disconnects while it waits (`execute_while` checks
the socket every 200 ms). `wake_parked` answers a parked read of a
cancelled statement with `57014` and drops one whose client is gone; queued
DML (`dml_queue`) is cancelled the same way, but still runs when only its
client is gone.

## The logical clock

The engine's logical clock (`Engine::clock`, PostgreSQL epoch microseconds)
is the maximum of the tick times applied so far; reads evaluate `now()` as
it. The engine keeps the GSN of the tick that set it (`Engine::clock_gsn`),
and checkpoints save that tick (the metadata tree's tick record), so the
clock is the same after a restart. The dataflow sees the clock as
`Graph::now`; temporal-filter operators run whenever it moves.
