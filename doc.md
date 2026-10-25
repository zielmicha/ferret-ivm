The incremental algebra is the small part. Z-set operators with integer weights, DBSP-style incrementalization, nested time for recursion: a few thousand lines, and you've written them before. The design lives in three commitments. Made consistently, they turn the three failure modes in that summary (state, the CDC edge, view change) into non-problems rather than solved problems.

## Three commitments

**One representation end to end.** Every base table, view, index, operator state, inter-node message and output is the same thing: rows with an integer weight, stamped with a time. Ingestion produces it, storage stores it, operators transform it, clients read it. There is no separate snapshot format, changelog envelope, checkpoint format or sink format.

**One clock.** Each source commit gets a global sequence number (GSN) on arrival, the mapping LSN→GSN is persisted, and everything downstream is timestamped in GSN. Total order, not partial: with one ordered log, progress is a watermark per channel, and Timely's progress protocol and Flink's barriers are both unnecessary. Nested time stays internal to recursive scopes.

**Determinism.** The state of any arrangement at GSN *g* is a pure function of the input log up to *g*. Wall clock is a source (a logged tick stream), external lookups are sources with logged responses, and non-deterministic SQL (`random()`, `string_agg` without ORDER BY) is rejected or pinned down.

From those three, fault tolerance, replication, view hydration, consistent reads and time travel are literally one mechanism: *read state as of g, replay the log from g+1*.

## The CDC edge

The engine is a replica of the source tables, not a stream consumer. Every table with a primary key is ingested as an upsert stream keyed by PK, and the engine maintains the current-value index per table. That single decision removes a cluster of problems: REPLICA IDENTITY FULL isn't needed because the before-image for a retraction is the stored value; TOAST placeholders are filled from the stored value; a snapshot chunk and a streamed change are the same event (upsert K at *g*), so the initial snapshot is chunked upserts interleaved with the stream with DBLog-style watermarks fixing the order, and adding a table months later uses the same code path, resumable and parallel. Tables without a PK get REPLICA IDENTITY FULL or are refused.

The slot is acknowledged the moment the engine's own input log is durable, not after a checkpoint. PG WAL retention is then bounded by ingestion lag (milliseconds), instead of by checkpoint cadence times tolerated failures. The input log is a sequential append on local NVMe, shipped continuously to object storage.

Schema is versioned in the log: a relation message with a new shape is a timestamped event, so replay is deterministic and any view's plan can be re-derived against the schema as of any GSN. Multiple sources each have their own slot and snapshot and are merged into one GSN order on arrival; the sources have no cross-source atomicity anyway, so arrival order is as good as any, provided it's recorded once and never changes.

## State

One storage structure: an ordered MVCC index keyed by (key, value), with (GSN, weight) history consolidated behind a retention frontier. Memory is a buffer pool over it. There is no in-memory mode that later spills, so a working set that fits in RAM runs at RAM speed and one that doesn't degrades along the cache curve rather than off a cliff. This is the direct answer to Materialize's three attempts: disk was never retrofitted because it was never absent.

Two choices make this index do most of the engine's work:

- Arrangements are shared and refcounted. Each base table's PK index is its first arrangement and is retained for the life of the table. Joins on PK need no extra state. A new index on (table, col) is built by scanning a local arrangement, never by going back to Postgres.
- The index is augmented: each node carries a monoid summary over its subtree (count, sum, min, max, or a user-supplied monoid). COUNT, SUM, MIN, MAX per group, ROW_NUMBER, top-k and percentiles become range queries over the index, not operator state. The non-invertible aggregates, the usual pain in IVM, stop being special. This is also the reason to pick a write-optimized tree over an LSM: subtree summaries are natural in a tree and awkward across sorted runs. Otherwise your "RocksDB keyed by join key" is the right picture.

Local NVMe is the hot path and the source of truth on its node. Durability beyond the node is continuous incremental backup of immutable segments plus the shipped input log; availability is replicas, which determinism makes trivially consistent because they run the same log. Nothing in the hot path touches object storage. Hummock, ForSt and persist exist because cloud NVMe is ephemeral; replicas plus backup are the cheaper answer to the same fact.

Checkpointing isn't a mechanism. Each arrangement has a durable frontier (its flushed GSN). Recovery on a node: take the minimum durable frontier across its arrangements, replay the node's inbound logs from there, and each arrangement drops deltas at or before its own frontier. No coordinator, no barriers, no alignment. Across nodes this is your persistent-inbox scheme, generalised: every edge that crosses a durability domain is a GSN-stamped log on the receiving side, so recovery is node-local and a sender never recomputes for anyone else's sake. Within a node the input log is the only inbox needed, because recompute from it is deterministic.

## Compute

A small operator set, incrementalized mechanically: linear ops, bilinear join, distinct, aggregate, with outer joins and NOT EXISTS built from antijoin. Weights are i64, multiset by default, `distinct` where SQL needs sets. Output is consolidated per GSN before it leaves an operator, so no reader ever sees an intermediate retract/insert pair or a half-applied transaction.

Multi-way joins default to delta joins: an input delta becomes a chain of lookups into base arrangements, ordered worst-case-optimally per delta, so the only join state is the shared base indexes and there are no intermediate arrangements. It trades lookups for state, which is the right trade on NVMe with a warm buffer pool.

The planner optimises maintenance cost, not scan cost: expected work per source delta, from the delta rates the engine measures and exact cardinalities from the augmented indexes. Source constraints (PK, FK, unique, NOT NULL) are known and used; a PK join is 1:1 with no distinct, an FK join is a semijoin.

No per-view compile step. Operators are generic over a compact memcomparable row encoding; expressions compile at plan time in milliseconds, vectorized over batches of (key, value, GSN, weight). A view goes live in seconds, bounded only by hydration.

Expression semantics are exactly Postgres's, enforced by continuous differential testing against a real Postgres: random schemas, random update streams, compare every view with a recompute. Correctness of IVM means equivalence with recompute and nothing else, and this is the test every existing engine runs too little of.

## Views change

The catalog is a DAG of versioned plan fragments. Views compile into fragments; identical sub-plans are shared by normalized plan hash. Creating a view hydrates its new fragments from existing arrangements at a fixed *g* (a snapshot read of their inputs, then attach to live deltas from *g+1*), which is the same code path as recovery. Running views are never stalled, and Postgres is never touched. Altering a view is: plan the new version, keep shared fragments, hydrate the new ones, swap the view's output atomically at some *g*, refcount the old fragments to zero. Base-table schema changes flow through the same machinery. The catalog itself lives in the engine's storage, versioned by GSN, so "what was the plan at *g*" is a query.

Because arrangements are queryable, the engine is a database: a view can be left virtual (computed from arrangements on demand) or materialized, and the planner can choose.

## Output

Three forms of the same data: point-in-time SQL at a GSN; `SUBSCRIBE` yielding (GSN, diffs) with progress markers; sinks (Postgres write-back, Kafka) idempotent on GSN, so a sink that dies mid-batch replays safely. Reads are "as of *g*", "fresh" (wait for the ingestion frontier), or "at least LSN *L*" via the persisted mapping, which gives a client read-your-writes against its own Postgres commit.

Per-client live queries, the Zero use case, are parameterized views: a view joined against a parameters table that clients insert into, with a subscription filtered to their row. Thousands of clients share one dataflow and each pays only the final join. Analytics views and app sync become the same engine with one extra table, which is the argument for not specialising the design for either.

## Scale

Single node first and done properly: partition by key hash across cores, with per-worker storage partitions so nothing locks. A 2 TB NVMe box covers most people. Scale-out has two modes. The default is per-view isolation: each node subscribes to the input log for the base tables it needs and owns whole views, so there is no distributed dataflow, failures are isolated, and the cost is duplicated base indexes. The rare view too large for one node uses sharded dataflow, where inter-node edges are persistent inboxes and rescaling is a key-range split of an ordered index plus log redirection.

Operationally: one static binary, embedded storage, no JVM, no meta-store beyond a small Raft group for membership when multi-node. Backpressure is the input log; compute lag never stalls Postgres, it shows up as a GSN gap. `EXPLAIN MAINTENANCE` shows expected work per source delta, per view.

## What stays hard

With the practical layer done, what's left is intrinsic: fan-out (a delta on a hot key still produces every output it must; delta joins limit intermediate blow-up but can't shrink the answer), user-defined aggregates without an inverse (per-group recompute, bounded by group size), retention versus time travel (the retention frontier is the real cost knob), and the planner's cost model, which is only as good as its delta-rate estimates. Everything else in that summary, swap stalls, barrier alignment, state loss on ALTER, WAL growth after failover, minute-long compiles, Debezium in a JVM, is downstream of a choice this design doesn't make.
