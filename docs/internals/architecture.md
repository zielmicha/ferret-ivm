# Architecture: threads, messages and the clock

This chapter describes how the parts of the engine fit together: the
threads, the messages between them, the GSN clock and the frontiers derived
from it, and group commit. It describes the code in `crates/ivm/src/coord/`
(`mod.rs`, `seq.rs`, `api.rs`, `read.rs`), `source/`, `sink/` and
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
| One representation: rows with a weight, stamped with a GSN | `dataflow` (Z-set deltas, consolidated per GSN), `storage` (per-entry (GSN, weight) histories), `log.rs` |
| One clock: a GSN for every input, in arrival order | `coord/seq.rs` (sequencer, group commit); every upstream commit is logged with its LSN, from which the per-source LSN frontiers follow (`coord/mod.rs`) |
| Determinism; the wall clock as a logged tick stream | `LogRecord::Tick`, `dataflow/temporal.rs` (temporal filters), `coord/ddl.rs` (`check_deterministic`) |
| CDC edge: primary-key upserts, before-images and unchanged TOAST values from stored rows, DBLog chunked snapshots, slot acknowledged on log durability, schema in the log | `source/postgres.rs`, `dataflow/ops.rs` (`table_step`) |
| Ordered MVCC index, (GSN, weight) histories, retention frontier, buffer pool | `storage` (copy-on-write B+trees, segments, manifest, CLOCK eviction, garbage collection) |
| Augmented summaries; aggregates as range queries | `storage/summary.rs`, `dataflow/reduce.rs`, `Env::nth` (percentiles) |
| Shared, reference-counted arrangements; the primary index as a table's first arrangement | `dataflow/mod.rs` (hash-consed nodes and arrangements) |
| Delta joins; anti-joins for outer joins and `NOT EXISTS` | `dataflow/join.rs`, `dataflow/lower.rs` |
| Changing views: shared fragments, hydration from arrangements, catalog in storage | `dataflow/mod.rs` (`Graph::hydrate`), `coord/ddl.rs`, the catalog in every checkpoint |
| Outputs: `AS OF`, `SUBSCRIBE` with progress, idempotent sinks, read modes | `coord/read.rs`, `server/`, `sink/postgres.rs` |
| `EXPLAIN MAINTENANCE` | `coord/read.rs` |
| Expression semantics exactly PostgreSQL's | `expr/`, and the ports `crates/pgnumeric`, `crates/pgdatetime`, `crates/pgfuncs` |

Where the implementation differs from the design:

- **One dataflow thread.** Sources, the sequencer and sessions are separate
  threads, but the dataflow is not partitioned across cores.
- **Atomic checkpoints** of all indexes at one GSN instead of per-arrangement
  durable frontiers ([Recovery](recovery.md)).
- **No LSN→GSN mapping.** Each commit record carries its LSN, and
  `AS OF AT LEAST LSN` waits for the per-source LSN frontiers.
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
| main | | parses flags, starts the signal and stop-file threads, binds the listening socket, runs recovery (`coord::start`), starts the web UI (with `--http-listen`) and the listener, then waits for a shutdown request (or for the engine to stop by itself) and shuts down after a final checkpoint (`main.rs`; exit status 0, or 1 when a thread or the final checkpoint failed) |
| `ivm-listener` | the listening socket | accepts connections (`server::serve_listener`) and starts a session thread for each |
| `ivm-signals` | | turns the first `SIGINT`/`SIGTERM` into a shutdown request; a second one exits the process at once (status 1) |
| `ivm-sequencer` | the input-log writer | assigns GSNs, appends and `fsync`s records in batches, generates clock ticks, acknowledges submitters, forwards durable records to the engine |
| `ivm-engine` (64 MiB stack) | storage (`Env`), dataflow (`Graph`), catalog, runtime state | applies durable records in GSN order, answers queries, parks waiting reads, delivers changes to subscriptions and sinks, begins and finishes checkpoints, builds the web UI's snapshots |
| `ivm-reader-<n>` (four, started by the first large read; 64 MiB stack) | forks of the storage, copies of parts of the graph | run reads that exceeded their budget on the engine thread ([Planner](planner.md#batch-execution)) and answer their clients |
| `ivm-checkpoint` (while a checkpoint is written) | a frozen version of the dirty nodes, the store's append side and manifest | serializes the application state, writes the nodes, `fsync`s and appends the manifest record ([Recovery](recovery.md#checkpoints)) |
| `ivm-session-<n>` (one per client, 16 MiB stack) | the client connection | wire protocol, session settings, source discovery; forwards statements to the engine and streams results |
| `ivm-source-<name>` (one per source) | replication and snapshot connections | turns `pgoutput` messages and snapshot chunks into log records, acknowledges the slot |
| `ivm-sink-<name>` (one per sink) | the target connection | writes changes to PostgreSQL |
| `ivm-hydrate` (while a view, index or subscription is hydrated; 64 MiB stack) | a fork of the storage and of part of the graph | hydrates the new dataflow fragment as of its statement's GSN, then applies the records the engine forwards until it has caught up ([Dataflow](dataflow.md#hydration-off-the-engine-thread)) |
| `ivm-stop-file` | | polls for `.ivm-stop` every 500 ms; when it appears, removes it and requests a shutdown |
| `ivm-http` (with `--http-listen`: a driver thread and the two worker threads of its own tokio runtime) | the HTTP listener | the web UI's server, its once-a-second metrics sampler and the SQL console ([Web UI](../web-ui.md)); engine snapshots are requested with a callback, so no async thread blocks on the engine |
| `ivm-slot-drop-<slot>` | | drop the replication slot of a dropped source, retrying with a delay doubling from 250 ms to 10 s (also while the stopping source's session still holds it) until it is dropped or gone; pending drops are saved in checkpoints and started again after recovery |

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
(`coord::api::Shared`). The engine's queue of durable batches is bounded by
the sequencer's backpressure ([below](#group-commit)); the queues to
subscriptions and sinks are not. The web UI keeps at most 16 of its
snapshot requests queued at the engine.

| Shared field | Meaning |
|---|---|
| `durable_gsn` | last GSN made durable by the sequencer |
| `processed_gsn` | last GSN applied by the engine |
| `source_lsn` | per-source LSN frontier (mutex-protected map) |
| `lsn_waiters` | some read is waiting for an LSN frontier |
| `want_keepalive` | ask sources to request a keepalive (to move their frontier) |
| `source_gauges` | per-source upstream position and ingest counters (web UI) |
| `slot_drops` | replication slots of dropped sources still to be dropped (saved in checkpoints) |
| `shutdown` | stop flag |
| `failed` | the engine or sequencer thread stopped with an error (also a failed final checkpoint or a panic); the process then exits with status 1 |
| `crash` | stop without the final checkpoint (a simulated crash, for tests) |

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
                                     SubEvent (subscriptions) ◀────┤
                                     SinkMsg  (sink threads)  ◀────┘
```

### Upstream transactions

1. The source thread collects the changes of one upstream transaction and,
   at its commit, sends `SeqMsg::Append { rec: Commit, ack: Some(tx) }` to the
   sequencer without waiting (`log_async`). Several commits can be in flight.
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
   that GSN (`Graph::step`), and the outputs are delivered to subscriptions
   and sinks.

Source progress (snapshot bookkeeping) and schema records are logged with
the blocking `EngineHandle::log`, which returns once the record is durable.

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
and applies it when it comes back durable; the reply is sent then (for
`CREATE MATERIALIZED VIEW` and `CREATE INDEX`, once their dataflow has been
hydrated off the engine thread, see
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
into one buffer, written with one `write`, and made durable with one
`fdatasync` (an `AppendAll` — the records the engine submitted in one round
of messages, see below — counts as its records). Only then are acknowledgements sent
and the records handed to the engine — no record is applied, acknowledged to
a source, or answered to a client before it is durable. Under load, batches
grow and the cost of `fsync` is shared; when idle, a batch is a single
record. `--no-sync` skips the `fsync` but keeps the order.

`SeqMsg::Truncate(gsn)` (sent after each checkpoint) deletes old log
segments; `SeqMsg::Shutdown` stops the sequencer after the current batch.

Backpressure: before each batch the sequencer waits while the engine has
`MAX_ENGINE_LAG` (10,000) or more durable records still to apply
(`durable_gsn - processed_gsn`; not while shutting down). Acknowledgements
then stop, sources block once 10,000 of their commits are unacknowledged,
and the engine's queue of durable batches stays bounded. The engine never
waits for the sequencer (its appends are not acknowledged synchronously),
so it always catches up.

## The engine loop

The engine thread waits for an `EngineMsg` with a 200 ms timeout and then
handles it and the messages already waiting behind it (up to 256 in a
round):

| Message | Action |
|---|---|
| `Durable(batch)` | apply each record in GSN order (`apply_record`), publish `processed_gsn`, re-check parked reads. While a hydration is in progress, a `Ddl` or `Schema` record and the records after it are held until it is done; `Commit` and `Tick` records are forwarded to it before they are applied. A record that fails to apply is logged (`failed to apply gsn ...`); an internal (`XX...`) or I/O (`58...`) error stops the engine thread (`Shared::failed`, and the process ends with status 1); any other error (data errors are error records, so this is a bug) leaves the record possibly half-applied and not applied again: `processed` moves past it and its DML statement fails, so that neither the client, the DML queued behind it nor fresh reads wait forever |
| `Request(Execute)` | run a statement: DDL and DML are validated and submitted; reads are executed or parked; `SUBSCRIBE` is registered; `SHOW`, `EXPLAIN`, `CHECKPOINT` answered directly |
| `Request(Describe)` | result columns and parameter types of a prepared statement |
| `Request(Checkpoint)` | ask for a checkpoint: answered with its GSN once one begun after the request is committed |
| `Request(Inspect)` | build a snapshot for the web UI (`coord/inspect.rs`) between two records, so that it is consistent at one GSN, and hand it to the requester's callback |
| `Wake` | a source frontier moved, or a statement was cancelled or its client went away: re-check parked reads |
| `Shutdown` | ask sources to stop, send every sink the last applied GSN as its progress and ask it to stop, stop the sequencer, take a final checkpoint after the one being written, if any (not after a simulated crash, which only waits for that one to be written), wait up to 5 s for sources and sinks, exit |

After every round it runs deferred requests (queued DML, woken reads),
sends the records submitted in the round to the sequencer as one
`AppendAll` (so that the statements evaluated together share one log append
and `fdatasync`; `log_batches` in `Shared` counts the sequencer's batches),
finishes the hydration in progress if it has caught up (then applies the
records held behind it), finishes the checkpoint being written if it is
done, begins one if one is due or asked for, and removes cancelled
subscriptions. The hydration thread sends `Wake` when it has
hydrated its fragment and whenever it has applied every record forwarded so
far; the checkpoint thread when it has written its checkpoint.

`apply_record` dispatches on the record kind:

| Record | Effect |
|---|---|
| `Commit` | group the changes by table (changes to tables that no longer exist are skipped), `Graph::step` at the GSN, deliver outputs; for local DML reply to the client; for sources that still exist update the per-source last LSN |
| `Ddl` | apply the statement to the catalog and dataflow; on success append it to the catalog history (which keeps the most recent 10,000 statements, trimmed back to 10,000 once it exceeds 11,000); reply |
| `Progress` | store the source's opaque state (snapshot progress), if the source still exists |
| `Schema` | rebuild the table in its new shape and re-plan dependent views ([Sources](../user-guide/sources.md#upstream-schema-changes)); mark the table's snapshot to be done again in the source's stored state, so that a restart before the re-snapshot logs progress still redoes it. A failure is only logged |
| `Tick` | advance the logical clock, step the dataflow (temporal filters), deliver outputs |

Delivering outputs (`deliver`): the error records of each materialized
view's root are merged into the view's error counts; each subscription gets
the data changes of its node, ends at the first error record that is added
(retracted ones do not end it), and gets a progress mark; each sink gets the
changes of its node, or the GSN alone as progress when its relation did not
change.

Records at or below the last applied GSN are ignored, which makes replay
idempotent. After each record the retention frontier is moved to
`processed − max(retention, 1)` (recovery sets it to `processed − retention`).

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
| retention frontier | `Env::frontier` | history at or before it is consolidated; `AS OF` must not be older |
| source LSN frontier | `Shared::source_lsn`, `SHOW SOURCES` | every upstream commit with an end LSN ≤ it has been logged |

Upstream LSNs are mapped to the GSN clock through the log: every source
`Commit` record carries the upstream transaction's end LSN. The engine keeps
the last logged LSN per source (checkpointed as the source's resume point).

Reads choose a GSN from these frontiers
([Reads](../user-guide/reads.md#read-points)): a fresh read takes the
durable GSN at arrival and waits for the processed GSN to reach it; an
`AS OF AT LEAST LSN` read waits until every source of the catalog has
reported a frontier at or past the LSN and then reads fresh (without any
source it fails with `55000`).

### Parked reads

A read that has to wait is stored in `Engine::parked` with its reply channel
and a wait condition:

| Condition | Woken when |
|---|---|
| `Gsn(g)` | the engine has applied `g` (checked after every batch) |
| `Lsn` | every source of the catalog has a frontier ≥ the read's LSN (`lsn_reached`; checked on `Wake` messages, which sources send when a frontier moves while `lsn_waiters` is set, after every batch and on the 200 ms timeout). When the last source is dropped, the read is re-run and fails |
| `Snapshot` | something changed that could finish a snapshot (source progress, a schema change or DDL); the read is re-run and parks again if a table it reads, directly or through views, is still loading |
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
is the maximum of the tick times applied so far. A deque of recent
(GSN, clock) pairs lets a read at GSN *g* evaluate `now()` as the clock at
*g* (`clock_at`); pairs older than the retention frontier are dropped.
Checkpoints save the deque (at most its last 100,000 pairs; `AppState::ticks`),
so reads as of earlier GSNs see the same clock after a restart. The dataflow sees
the clock as `Graph::now`; temporal-filter operators run whenever it moves.
