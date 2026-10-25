# ivm — incremental view maintenance over PostgreSQL

A single-node implementation of the design in [doc.md](doc.md): an engine that
replicates PostgreSQL tables through logical replication, maintains SQL views
over them incrementally, and serves the results over the PostgreSQL wire
protocol (point-in-time reads, `SUBSCRIBE`, write-back sinks).

```
            pgoutput (slot acked when the input log is durable)
PostgreSQL ───────────────────────────────▶ source ──▶ sequencer ──▶ input log (GSN-stamped, fsync, group commit)
                                                           │
             psql / drivers ◀── pgwire server ◀── engine ◀─┘  apply records in GSN order:
                                   │                │        dataflow step → arrangements (MVCC B-tree)
             sink (PG write-back) ◀┘                └─ checkpoint: catalog + arrangement roots
```

## Building and running

```sh
cargo build --release
./target/release/ivm --data-dir ./ivm-data --listen 127.0.0.1:6875
psql -h 127.0.0.1 -p 6875 -U ivm
```

Options: `--password PW` (cleartext auth), `--cache-mb N` (buffer pool),
`--checkpoint-secs N`, `--retention-gsns N` (time-travel window),
`--tick-ms N` (logged wall-clock ticks; 0 disables), `--no-sync`.
One process per data directory (enforced by a lock file).

```sql
-- replicate a publication (tables with a primary key, or REPLICA IDENTITY FULL)
CREATE SOURCE pg FROM POSTGRES CONNECTION 'host=... dbname=app user=...' PUBLICATION 'app_pub';
-- maintained views
CREATE MATERIALIZED VIEW revenue AS
  SELECT c.region, date_trunc('day', o.created) AS day, count(*), sum(o.amount)
  FROM orders o JOIN customers c ON c.id = o.customer_id GROUP BY 1, 2;
CREATE VIEW recent AS SELECT * FROM orders WHERE created > now() - interval '1 hour';  -- virtual
CREATE INDEX ON revenue (day);
-- reads
SELECT * FROM revenue;                                   -- fresh (waits for the log and initial snapshots)
SELECT * FROM revenue AS OF 123456;                      -- at a GSN (within the retention window)
SELECT * FROM revenue AS OF AT LEAST LSN '0/16B3748';    -- read-your-writes against PostgreSQL
SET read_mode = latest;                                  -- don't wait
SUBSCRIBE TO revenue WITH (PROGRESS);                    -- or COPY (SUBSCRIBE ...) TO STDOUT
CREATE SINK out FROM revenue INTO POSTGRES (CONNECTION '...', TABLE 'revenue_copy', KEY (region, day));
EXPLAIN MAINTENANCE revenue;  -- expected work per source delta
EXPLAIN DATAFLOW revenue;     -- operator graph and arrangements
SHOW SOURCES | TABLES | VIEWS | INDEXES | SINKS | ARRANGEMENTS | FRONTIERS | GSN | COLUMNS FROM t;
ALTER SOURCE pg REFRESH;      -- pick up tables added to the publication
```

Local tables (`CREATE TABLE`, `INSERT/UPDATE/DELETE`, `ON CONFLICT DO NOTHING`)
work too; their changes go through the same log as replicated ones.

## How the design maps to the code

| doc.md | Where |
|---|---|
| One representation: (row, weight, GSN) | `dataflow` (Z-set deltas, consolidated per GSN), `storage` (histories), `log` |
| One clock: GSN per source commit, LSN→GSN persisted | `coord/seq.rs` (sequencer, group commit), `coord` (`lsn_map`, frontiers) |
| Determinism; wall clock as a logged tick stream | `LogRecord::Tick`, `dataflow/temporal.rs` (temporal filters over `now()`), `coord/ddl.rs` (`check_deterministic`) |
| CDC edge: PK upserts, before-images/TOAST from stored rows, DBLog chunked snapshots, slot acked on log durability, schema in the log | `source/postgres.rs`, `dataflow/ops.rs` (`table_step`) |
| Ordered MVCC index, (GSN, weight) histories, retention frontier, buffer pool | `storage` (copy-on-write B+tree, segments, manifest, CLOCK eviction, GC) |
| Augmented monoid summaries; aggregates as range queries | `storage/summary.rs` (count, sums, min/max, maintained incrementally), `dataflow/reduce.rs`, `Env::nth` (percentiles) |
| Shared, refcounted arrangements; PK index as first arrangement | `dataflow/mod.rs` (hash-consed nodes and arrangements) |
| Delta joins, antijoins for outer joins / NOT EXISTS | `dataflow/join.rs`, `dataflow/lower.rs` |
| Views change: shared fragments, hydration from arrangements, catalog in storage | `dataflow` (`hydrate`), `coord/ddl.rs` (`CREATE OR REPLACE`, schema changes), checkpointed catalog |
| Outputs: AS OF, SUBSCRIBE with progress, idempotent sinks, read modes | `coord/read.rs`, `server`, `sink/postgres.rs` |
| EXPLAIN MAINTENANCE | `coord/read.rs` |
| Expression semantics exactly PostgreSQL's | `crates/pgnumeric`, `pgdatetime`, `pgfuncs` (ports with golden tests), `expr` |

Recovery is "read state as of g, replay the log from g+1": a checkpoint
stores the catalog and the roots of all arrangement trees; on start the
catalog is re-planned, arrangements are matched by structural fingerprint,
anything new is hydrated, and the input log is replayed.

## SQL

PostgreSQL syntax (via sqlparser) with PostgreSQL's type resolution and
semantics. Supported in maintained views and queries alike:

- joins of every kind (incl. `LATERAL`, `USING`, `NATURAL`), correlated and
  uncorrelated subqueries (`EXISTS`, `IN`/`NOT IN` incl. row values,
  `ANY`/`ALL`, scalar), CTEs incl. `WITH RECURSIVE`
- aggregates: count/sum/avg/min/max/bool_and/bool_or/every/stddev/variance,
  string_agg/array_agg/json(b)_agg/json(b)_object_agg (with ORDER BY),
  `DISTINCT`, `FILTER`, percentile_cont/disc, mode; `GROUPING SETS`, `ROLLUP`,
  `CUBE`, `grouping()`
- window functions (ranking, offsets, value functions, aggregates) with
  `ROWS`/`RANGE` (incl. value offsets)/`GROUPS` frames and named windows
- `DISTINCT ON`, `ORDER BY ... LIMIT/OFFSET`, `FETCH ... WITH TIES`, set
  operations with `ALL`, `VALUES`, set-returning functions in `FROM` (with
  `ORDINALITY`) and in the select list (several run in lockstep, as in
  PostgreSQL)
- types: bool, int2/4/8, numeric, float4/8, text/varchar/char, bytea, date,
  time, timestamp(tz), interval, uuid, json, jsonb, jsonpath, arrays
  (multidimensional, with lower bounds); other upstream types replicate as
  text
- a large function library (string, regex, date/time, numeric incl.
  to_char/to_number, json/jsonb, arrays) with PostgreSQL's results and errors
- SQL/JSON path: the `jsonpath` type, `jsonb_path_exists/match/query/
  query_array/query_first` (and `_tz`; named `vars`/`silent` arguments),
  `@?` and `@@`, including `.datetime()` and `like_regex`
- temporal filters: `WHERE ts > now() - interval '1 hour'` and similar
  conditions monotone in `now()` are maintained as time advances
- system catalogs for client tools: `pg_catalog` (pg_class, pg_attribute,
  pg_type, pg_namespace, pg_index, ...) and `information_schema`
  (tables, columns, views, schemata), enough for psql's `\d` family
- engine relations: `ivm.catalog_history` (every DDL statement with its GSN,
  so "what was the catalog at g" is a query) and `ivm.objects`

Errors are data: an expression error in a view becomes an error record that
makes reads of the view fail until the offending rows are gone.

## Testing

- `cargo test --workspace` (about a minute once built; tests that need
  PostgreSQL start private PostgreSQL 17 clusters from `PG_BIN`, default
  `/usr/lib/postgresql/17/bin`):
  - the golden suites of the PostgreSQL ports (tens of thousands of cases
    recorded from PostgreSQL 17);
  - property tests (proptest) of the storage engine against a model (random
    operations, time travel, incremental summaries, GC, checkpoints, crashes
    with torn files) and of the key and row encodings (key order is SQL
    order, exact round trips);
  - the self-differential test (`tests/selfdiff.rs`): random views over
    random tables must equal their queries run ad hoc, through restarts,
    crashes, subscriptions and views created, replaced and dropped;
  - engine, dataflow, HTTP and replication tests (`crates/ivm/tests`),
    mostly regression tests for specific bugs.

  Property tests run `PROPTEST_CASES` cases (`PROPTEST_RNG_SEED` fixes the
  seed); a failure shrinks to a minimal case, saved in a
  `*.proptest-regressions` file and re-run first from then on. Longer
  variants are `#[ignore]`d, as are tests of open bugs: see the module docs
  of `crates/ivm/src/storage/tests.rs` and `crates/ivm/tests/selfdiff.rs`.
- `ivm-difftest`: differential testing against a real PostgreSQL. It creates a
  random schema upstream, replicates it, defines random views (joins,
  subqueries, aggregates, grouping sets, windows, top-k, set operations,
  LATERAL, ...) and after each of 20 rounds of random DML compares every view,
  and every replicated table, with PostgreSQL (`ivm-difftest --seed N --views
  40`). `--restarts N` restarts or crashes the engine on the way (`--kill`:
  SIGKILLs it as a child process), `--sinks N` writes views back to
  PostgreSQL and checks the copies, `--schema-changes N` adds columns
  upstream. It starts a private PostgreSQL 17 cluster from `PG_BIN`;
  `DIFFTEST_VERBOSE=1` prints the views and details of any mismatch (more in
  its module docs).

## Deviations from the design and limitations

- **Single dataflow thread.** The source, sequencer and engine are separate
  threads, but the dataflow is not partitioned across cores.
- **Recursion by recomputation.** `WITH RECURSIVE` is correct (semi-naive
  iteration per change) but recomputes the fixpoint instead of using nested
  time.
- **Atomic checkpoints** of all arrangements instead of per-arrangement
  durable frontiers; recovery replays from the checkpoint GSN.
- **Planner:** join order uses unique keys and exact sizes from the indexes;
  measured delta rates are reported by `EXPLAIN MAINTENANCE` but not used.
- **Not implemented:** Kafka sinks, object-storage backup, replicas,
  multi-node operation, record/composite types, user-defined aggregates,
  `BETWEEN SYMMETRIC` and frame `EXCLUDE` (parser).
- Session time zone is UTC; `now()` in queries is the logical clock (the
  latest logged tick as of the read), so reads are repeatable.
- `AS OF AT LEAST LSN` compares against every source's frontier, so it is
  meant for sources from one PostgreSQL cluster.
- A fresh read of a table whose initial snapshot is still loading waits for
  it (a partial snapshot is no state the upstream ever had); `read_mode =
  latest` and `AS OF` reads do not wait.
