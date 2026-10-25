# ivm documentation

ivm replicates PostgreSQL tables through logical replication, maintains SQL
views over them incrementally, and serves the results over the PostgreSQL
wire protocol. These documents describe the code as of commit `d80c7f2`
(with the test-tooling changes of `1275115`); the design it implements is in
[doc.md](../doc.md).

New users: read [Getting started](user-guide/getting-started.md),
[Sources](user-guide/sources.md) and [Reads](user-guide/reads.md) first, and
check [Troubleshooting and limitations](user-guide/troubleshooting.md) before
relying on a feature.

## User guide

| Document | Contents |
|---|---|
| [Getting started](user-guide/getting-started.md) | building and installing, command-line flags, stopping the server, connecting with `psql` and drivers, a first session |
| [Sources](user-guide/sources.md) | replicating PostgreSQL: upstream requirements (`wal_level`, publications, replica identity, privileges, WAL retention), `CREATE`/`ALTER ... REFRESH`/`DROP SOURCE`, table discovery, type mapping, slots, the initial snapshot, upstream restarts, `TRUNCATE` and schema changes |
| [Tables and views](user-guide/tables-and-views.md) | local tables and DML, views versus materialized views, `CREATE OR REPLACE`, indexes, `DROP` and dependencies |
| [Reads and subscriptions](user-guide/reads.md) | fresh and latest reads, `AS OF`, `AS OF AT LEAST LSN`, `read_mode` and other session settings, `SUBSCRIBE` (options, output format, `COPY` form) |
| [Sinks](user-guide/sinks.md) | writing views back to PostgreSQL: `CREATE SINK`, exactly-once delivery, target-table requirements |
| [Introspection](user-guide/introspection.md) | `EXPLAIN PLAN`/`DATAFLOW`/`MAINTENANCE`, `SHOW`, the `ivm.*` relations, `pg_catalog` and `information_schema`, `psql` `\d` support |
| [Temporal filters and errors](user-guide/temporal-filters-and-errors.md) | `now()` in maintained views and the logical clock; expression errors as data |
| [Operations](user-guide/operations.md) | tuning, the data directory and `LOCK`, recovery, checkpoints, retention, durability, what is safe to delete, backups, replication-slot cleanup, monitoring |
| [Troubleshooting and limitations](user-guide/troubleshooting.md) | common problems and their causes, limitations and differences from PostgreSQL, known problems with workarounds |

## SQL reference

| Document | Contents |
|---|---|
| [Statements](sql/statements.md) | every statement with its synopsis, unsupported statements, error codes |
| [Queries](sql/queries.md) | `SELECT` and its clauses, joins, subqueries, aggregates, window functions, set operations, CTEs, `VALUES`, set-returning functions, expressions and literals |
| [Data types](sql/types.md) | supported types, text and binary formats, casts, type resolution, comparison semantics |
| [Functions and operators](sql/functions.md) | operators and functions by category, and which crate implements them |
| [Maintained views](sql/maintained-views.md) | what materialized views reject (nondeterminism, `now()` outside temporal filters) and why, behaviours pinned down for determinism, how each construct is maintained and what it costs |

## Internals

| Document | Contents |
|---|---|
| [Architecture](internals/architecture.md) | design map, threads and message flow, group commit and acknowledgements, the GSN clock and frontiers, the logical clock |
| [Dataflow](internals/dataflow.md) | Z-sets and consolidation, the operator graph, hash-consing and reference counting, arrangements, every operator, hydration, errors |
| [Storage](internals/storage.md) | copy-on-write MVCC B+trees, (GSN, weight) histories, summaries and range aggregates, the buffer pool, checkpoints, garbage collection |
| [Planner](internals/planner.md) | parsing, binding, decorrelation, optimization, lowering to dataflow, delta-join planning, batch execution |
| [Recovery and snapshots](internals/recovery.md) | the recovery protocol, matching checkpointed indexes by structural fingerprint, source resumption, the DBLog snapshot algorithm as implemented |

## Reference

| Document | Contents |
|---|---|
| [On-disk format](on-disk-format.md) | byte-level layout of the data directory: input log, segments and nodes, manifest, checkpoints, key and row encodings, fingerprints, compatibility |
| [Testing](testing.md) | the test suites, how the golden data was generated, `ivm-difftest` |
