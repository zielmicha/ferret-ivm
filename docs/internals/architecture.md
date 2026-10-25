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
| main | the listening socket | parses flags, runs recovery (`coord::start`), then accepts connections forever (`server::serve`) |
| `ivm-sequencer` | the input-log writer | assigns GSNs, appends and `fsync`s records in batches, generates clock ticks, acknowledges submitters, forwards durable records to the engine |
| `ivm-engine` | storage (`Env`), dataflow (`Graph`), catalog, runtime state | applies durable records in GSN order, answers queries, parks waiting reads, delivers changes to subscriptions and sinks, takes checkpoints |
| `ivm-session-<n>` (one per client) | the client connection | wire protocol, session settings, source discovery; forwards statements to the engine and streams results |
| `ivm-source-<name>` (one per source) | replication and snapshot connections | turns `pgoutput` messages and snapshot chunks into log records, acknowledges the slot |
| `ivm-sink-<name>` (one per sink) | the target connection | writes changes to PostgreSQL |
| stop-file watcher | | polls for `.ivm-stop` every 500 ms; when it appears, requests a shutdown and exits the process at once, without waiting for the final checkpoint |
| slot-drop helpers | | short-lived; drop a replication slot after `DROP SOURCE`, once the source thread has stopped |

There is exactly one engine thread: all dataflow maintenance, all index
reads and writes, and all query execution happen on it. Everything that
talks to the network (sessions, sources, sinks) runs on other threads, so
the engine never blocks on a remote server.

Communication uses `crossbeam` channels (unbounded, except one-shot reply
channels of capacity 1) and a small block of shared state
(`coord::api::Shared`):

| Shared field | Meaning |
|---|---|
| `durable_gsn` | last GSN made durable by the sequencer |
| `processed_gsn` | last GSN applied by the engine |
| `source_lsn` | per-source LSN frontier (mutex-protected map) |
| `lsn_waiters` | some read is waiting for an LSN frontier |
| `want_keepalive` | ask sources to request a keepalive (to move their frontier) |
| `shutdown` | stop flag |

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
   LSN frontier in `Shared::source_lsn` (once all of its tables have
   finished their initial snapshot), which wakes reads waiting for an LSN.
4. The engine applies the record: the changes go through the dataflow at
   that GSN (`Graph::step`), and the outputs are delivered to subscriptions
   and sinks.

Source progress (snapshot bookkeeping) and schema records are logged with
the blocking `EngineHandle::log`, which returns once the record is durable.

### Local DML

The session sends the statement to the engine. The engine evaluates it
against its current state, converts it into row changes, and submits one
`Commit { source: 0, lsn: <request id>, ... }` record to the sequencer. The
reply is sent when the record comes back durable and has been applied, so
the client's next fresh read sees it. Only one DML statement is in flight at
a time; others queue in the engine (`dml_queue`) and run in order.

### DDL

The session first does whatever needs the network (for `CREATE SOURCE` and
`ALTER SOURCE ... REFRESH`, discovering the upstream tables) and sends the
statement with the result (`extra`, JSON) to the engine. For `CREATE TABLE`,
`CREATE [MATERIALIZED] VIEW`, `CREATE SOURCE` and `ALTER SOURCE` the engine
first validates the statement against the current catalog (for views: binding,
planning and the determinism check, as a dry run — not lowering or
hydration). It then submits a `Ddl { req, sql, extra }` record and applies it
when it comes back durable; the reply is sent then. Errors found only while
applying (in lowering, hydration, or for statements without a dry run) leave
the failed record in the log; it fails again on replay, where the failure is
only logged. Replay applies the same record with the same `extra`, never
contacting the upstream.

### Clock ticks

When `--tick-ms` is non-zero, the sequencer waits for messages with a timeout
that ends at the next tick deadline; if no message arrives before it, it
appends `Tick { micros }` with the current Unix time in microseconds (at
least one more than the previous tick). Ticks go through the same path as any
other record: they get a GSN, are made durable, and are applied by the
engine, which advances its logical clock and steps the dataflow so that
temporal filters release due events.

## Group commit

The sequencer processes messages in batches: it blocks for the first message
(or the tick timeout), then drains up to 4095 more without blocking. All
`Append`s of a batch are encoded into one buffer, written with one `write`,
and made durable with one `fdatasync`. Only then are acknowledgements sent
and the records handed to the engine — no record is applied, acknowledged to
a source, or answered to a client before it is durable. Under load, batches
grow and the cost of `fsync` is shared; when idle, a batch is a single
record. `--no-sync` skips the `fsync` but keeps the order.

`SeqMsg::Truncate(gsn)` (sent after each checkpoint) deletes old log
segments; `SeqMsg::Shutdown` stops the sequencer after the current batch.

## The engine loop

The engine thread waits for an `EngineMsg` with a 200 ms timeout and
handles:

| Message | Action |
|---|---|
| `Durable(batch)` | apply each record in GSN order (`apply_record`), publish `processed_gsn`, re-check parked reads. A record that fails to apply is logged (`failed to apply gsn ...`); an internal (`XX...`) or I/O (`58...`) error stops the engine thread; any other error (data errors are error records, so this is a bug) leaves the record possibly half-applied and not applied again: `processed` moves past it and its DML statement fails |
| `Request(Execute)` | run a statement: DDL and DML are validated and submitted; reads are executed or parked; `SHOW`, `EXPLAIN`, `CHECKPOINT` answered directly |
| `Request(Describe)` | result columns and parameter types of a prepared statement |
| `Request(Checkpoint)` | take a checkpoint |
| `Wake` | a source frontier moved: re-check reads waiting for an LSN |
| `Shutdown` | stop sources and sinks, stop the sequencer, take a final checkpoint, exit (the stop-file path exits the process before this completes) |

After every message it runs deferred requests (queued DML, woken reads),
takes a checkpoint if one is due, and removes cancelled subscriptions.

`apply_record` dispatches on the record kind:

| Record | Effect |
|---|---|
| `Commit` | group the changes by table, `Graph::step` at the GSN, deliver outputs; for local DML reply to the client; for sources update the per-source last LSN |
| `Ddl` | apply the statement to the catalog and dataflow; on success append it to the catalog history; reply |
| `Progress` | store the source's opaque state (snapshot progress) |
| `Schema` | rebuild the table in its new shape and re-plan dependent views ([Sources](../user-guide/sources.md#upstream-schema-changes)) |
| `Tick` | advance the logical clock, step the dataflow (temporal filters), deliver outputs |

Records at or below the last applied GSN are ignored, which makes replay
idempotent. After each record the retention frontier is moved to
`processed − max(retention, 1)` (recovery sets it to `processed − retention`).

## The GSN clock and frontiers

The **GSN** (global sequence number, `u64`) is assigned by the sequencer in
arrival order, contiguously from `last logged GSN + 1` (1 for an empty log).
It totally orders every input: transactions of all sources, local DML, DDL,
source progress, schema changes and clock ticks. Different sources have no
order between them other than their arrival order, which is fixed once
logged.

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
`AS OF AT LEAST LSN` read waits for every source LSN frontier to reach the
LSN and then reads fresh.

### Parked reads

A read that has to wait is stored in `Engine::parked` with its reply channel
and a wait condition:

| Condition | Woken when |
|---|---|
| `Gsn(g)` | the engine has applied `g` (checked after every batch) |
| `Lsn` | every source frontier ≥ the read's LSN (checked on `Wake` messages, which sources send when a frontier moves while `lsn_waiters` is set, and on the 200 ms timeout) |
| `Snapshot` | something changed that could finish a snapshot (source progress or DDL); the read is re-run and parks again if a table is still loading |

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
*g* (`clock_at`); pairs older than the retention frontier are dropped, and
after a restart the deque starts at the checkpoint's clock. The dataflow sees
the clock as `Graph::now`; temporal-filter operators run whenever it moves.
