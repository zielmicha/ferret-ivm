# Testing

The project is tested at four levels: unit and model tests of the engine's
components, engine-level tests through its API, golden tests of the
PostgreSQL ports against results recorded from PostgreSQL 17, and
differential testing of maintained views against a real PostgreSQL
(`ivm-difftest`).

## Running the tests

```sh
cargo test                     # every crate
cargo test -p ivm              # the engine only
cargo test -p pgfuncs --test jsonpath_golden
```

Requirements and environment:

| Variable | Used by | Meaning |
|---|---|---|
| `PG_BIN` | `pgproto` integration tests, the engine's replication tests, `ivm-difftest`, `ivm-bench` | directory with PostgreSQL 17's `initdb`, `pg_ctl`, `postgres` and `psql` (default `/usr/lib/postgresql/17/bin`) |
| `PGPROTO_CLUSTER_DIR` | `pgproto::testutil::TempCluster` | parent directory of temporary clusters (default `$TMPDIR/pgproto-clusters`) |
| `TMPDIR` | everything | temporary files; also the fallback directory for a cluster's Unix socket when the cluster directory is too deep for a socket path (107 bytes) |
| `IVM_TEST_TMP` | the `ivm` crate's tests | parent of their data directories (default: the temporary directory); a failing test keeps its directory for inspection |
| `PROPTEST_CASES`, `PROPTEST_RNG_SEED` | property tests (proptest) | number of cases per property (each test has its own default), and a fixed seed for the random cases |
| `SELFDIFF_VERBOSE` | `tests/selfdiff.rs` | print each case's views and actions as it runs |
| `IVM_DELTA_SPILL_BYTES` | the engine | the size beyond which a per-GSN delta or key set is spilled into a temporary tree (default: half the working memory's share of the memory budget, within 1 and 256 MiB); `0` spills every one, which runs every operator on spilled input (see below) |

Only the `pgproto` integration tests (`client`, `replication`,
`server_codec`, `testutil`), the engine's replication tests
(`crates/ivm/tests/replication.rs`), `ivm-difftest` and `ivm-bench` need
PostgreSQL binaries; all other tests are self-contained (the process tests,
`crates/ivm/tests/process.rs`, run the `ivm` binary, which `cargo test`
builds). Golden tests read recorded data and need no server.

`TempCluster` (`crates/pgproto/src/testutil.rs`) creates a throwaway cluster
in `<base>/<pid>-<n>/`: `initdb` with user `postgres`, encoding UTF-8 and the
builtin `C.UTF-8` locale, trust authentication (or, with a password, trust
for Unix sockets and SCRAM for TCP); configuration `wal_level = logical`,
20 replication slots and walsenders, `fsync = off`, `listen_addresses =
'127.0.0.1'` on a free port, and the cluster directory as the socket
directory; then `pg_ctl start`. It is stopped and deleted when dropped. The
owning process holds a lock on `owner.lock` in each cluster directory; the
first cluster started by a process removes directories whose lock is free
(stopping postmasters left behind by killed test processes), except those
created with `keep: true`.

## Test suites

### `crates/ivm`

| Suite | What it covers |
|---|---|
| unit tests (`cargo test -p ivm --lib`) | input-log round trip, torn-tail truncation and corruption before the tail, large commits written as bounded parts, a multi-part commit torn before its record discarded whole, spools becoming commits, replay holding one frame of a backlog, oversized frames refused (`log.rs`); exact float sums, bit for bit against the earlier fixed-point representation on edge values, with canonical encodings; `min`/`max` prefixes ordering like their values and maintained like recomputed ones; summary sizes as encoded (`storage/summary.rs`); the sequencer's ticks under load (`coord/seq.rs`); memory meters and charges, the buffer pool's share (with working memory, forks' pools and forwarded changes taken from it), working buffers spilling at their cap or together over their share, and input admission (`memory.rs`); on an engine without its threads (`coord/mod.rs`): a subscription that fails to lower releasing what it built, ad hoc reads stopping their scans at their LIMIT (entries visited), sources waiting for room by the bytes of the records still to be applied (not their number), the group caches of several aggregations staying within one byte budget with an idle one giving way, clock ticks making a checkpoint due only when they change a view or after 3,600 of them, checkpoints truncating the log to their GSN, and maintenance passes compacting the metadata tree's retracted error records; ORDER BY with LIMIT holding only the rows that can be in the result (`exec.rs`); default slot names and the formats of logged `CREATE SOURCE` records (`coord/ddl.rs`); statement splitting, `AS OF` detection, `EXPLAIN` targets, operator-chain limits, parameter counts and the engine's own statements (`sql/parser.rs`); planning panics turned into errors, stored definitions with parameter placeholders (`sql`); the source against a fake engine: truncates and keyless changes around a snapshot chunk, schema changes in the middle of a transaction and their logged re-snapshot, keys implied by Relation messages and re-snapshots after a key change, watermark tags unique per run, values that are not UTF-8 failing their table (copied again) and not the session, chunks waiting for transactions decoded before them, transaction-id visibility across epochs, large transactions spooled in bounded memory and spooled changes reshaped or dropped later in their transaction (`source/postgres.rs`); pending slot drops recorded and cancelled (`source/mod.rs`); a sink's transactions packed into bounded messages (`sink/postgres.rs`); summaries decoding only the columns they read (`dataflow/layout.rs`); per-GSN deltas in memory and spilled read alike, in batches and by key range, and overlaid on the arrangement they changed give every entry's weight before the step, also by group and for entries the step removed (`dataflow/delta.rs`); validity intervals of temporal filters found by a verified guess equal to those found by bisection, in three evaluations for shifts of the clock (`dataflow/temporal.rs`); window frames that slide equal to frames summed from scratch, also for `min`/`max` over equal values in several representations (`dataflow/window.rs`); the work of steps, hydrations and DDL counted (index entries visited, nodes considered) on an engine without its threads: steps consider only the changed tables' consumers, graph slots are reused by query subscriptions, temporal filters skip released events, top-k windows, percentiles and window functions read only where a change reaches, join hydration reads only the input it drives, view replacements read their versions only for subscribers (merging versions indexed alike, consolidating others exactly), window operators recover their partitions in error without reading their input, and parked reads wake when the frontier passes their LSN or their tables are loaded (`coord/work_tests.rs`); connection-string masking and other helpers of the introspection snapshots (`coord/inspect.rs`); the web UI's Basic auth, host check and embedded assets (`http`); skipping key columns and fidelity suffixes without decoding them, on edge values (`types/encoding.rs`) |
| type property tests (`types/tests.rs`, proptest) | over random values of every column type (numerics of any scale, floats with NaN payloads and signed zeros, intervals, json, arrays with bounds and NULLs, ...): key order equals SQL order, for single values and composite keys; keys decode to the canonical value and, with the fidelity suffix, to the exact one; values and rows round-trip through the row codec, text I/O and binary send/recv; prefix successors bound every extension; skipping a key column, a fidelity suffix or a codec value consumes exactly what decoding it does. Plus key order on a table of edge values per type (`types/encoding.rs`) |
| storage model test (`storage/tests.rs`, proptest) | random operation sequences on several trees (counts, and numeric, float, integer and text summaries over values embedded in the keys, `min`/`max` values among them wider than node summaries hold) against a model of every key's (GSN, weight) history: weighted batches, retention frontier advances, tree creation and drops (which must read nothing), checkpoints, maintenance passes with random budgets (compaction, merges, segment GC), early writes of dirty nodes (`write_back`), changes of the buffer pool's target, clean reopens and crashes (no final checkpoint; after early writes, perhaps torn; garbage after the durable end of the manifest or the active segment; stray segment files; a crash in the middle of a checkpoint; a torn manifest record before any checkpoint was committed, which must refuse to open with every file left as it was, after which the store is removed and rebuilt from the operations, as the documented remedy does); checkpoints written in the background (begun, written and finished with updates, drops, maintenance passes, early writes and forks in between, or interrupted by a crash before they were written or after they were committed but before they were finished); forks checked against the model as of when they were taken, after the environment and the fork itself changed (writing early in a small pool), segment accounting included; trees filled in a fork, written there early, and adopted. Node and segment sizes, retention, the buffer pool (down to a few KB) and whether the case starts with the formats of earlier versions (kind-2 internal nodes, checkpoint records without tree space; the current ones from the first reopen) are part of the case. Checked: point lookups after every step; scans now and AS OF every retained GSN, range and prefix scans, range summaries and `nth`; the tree structure (key ranges, parent links, `min_seg`, `due` exact for clean nodes and never late for dirty ones, clean nodes matching their frames), each loaded node's byte size and memory, and the segment accounting, per segment and per tree; after a pass without limits, no leaf whose compaction is due and no sparse segment left; after a crash, the state of the last checkpoint, and replaying the operations since then must reproduce the state before the crash. Also: entries retracted behind the frontier are consolidated away; `leaf_compaction` against brute force; regression tests of space reclamation with bounds on counts (a queue keeps only the retention window, a 99% delete is compacted and merged, a pass short of budget reporting the work it left, a pass rewriting no more than the buffer pool has room for, appends fill leaves to 90%, a hot row's versions per lookup, space amplification under hot and cold updates, a drop that reads nothing, a store in the earlier formats upgraded); internal nodes stay within their size with 4 KiB `min`/`max` values (and a range summary loads one path to an extreme held as a prefix); merges of leaves and internal nodes whose summaries hold `min`/`max` prefixes, with and without their hash, keep the summaries exact; a batch of dirty data forty times the cache leaves the buffer pool within the cache, lookups after it sweep a slot or so each, and a crash recovers the checkpoint; a `TreeBuilder` loads entries in random order through several levels of sorted runs into the right tree, within the cache and without leaving temporary trees; a checkpoint taken while a temporary tree exists records nothing of it; a fork reads a tree dropped after it was taken and whose segments a checkpoint deleted since, and the segments a fork writes to survive the environment's checkpoints until its tree is adopted; and a full-history model test with AS OF reads across crashes, checkpoints and maintenance passes that ends with GC keeping the store within a small factor of its live data |
| engine tests (`tests/engine.rs`) | through the public API, without PostgreSQL: temporal filters following the logged clock, across a restart, and with the clock never going backwards; rejection of invalid temporal filters; temporal views (joins, aggregates, outer joins, `date_trunc`) equal to a recomputation as random changes and ticks interleave; recovery after drops (checkpointed indexes are found again although node ids changed; a multiset table keeps its rows); jsonpath views; multi-dimensional arrays and lockstep set-returning functions; ad hoc reads through index key ranges, partial decoding and index joins equal to the same queries written so that they cannot use them (`id + 0`), also `AS OF` an earlier GSN and across an index and a view rebuilt after it; reads stopping at their LIMIT (in index order, or keeping the top rows) equal to sorting the whole result; subscription snapshots sent in chunks; regression tests (DISTINCT ON over infinite intervals, ordered aggregates after a DISTINCT one, percentiles differing only in their fraction, `WITH ORDINALITY` after `LATERAL`, predicates on derived columns filtering before the other columns are computed, quantified subqueries (`ANY`/`ALL`) as decorrelated, exact representations of values derived from presence changes, subscriptions to a view sharing its dataflow with a dropped or replaced view, subscriptions ending when a replacement changes the view's columns) and an ignored test of an open bug |
| stall tests (`tests/stall.rs`) | long operations do not stop the engine, in counts rather than time: commits and reads are applied and answered while a large view hydrates (the statement creating it still running after them), its fork holding its share of the memory budget and the changes forwarded to it staying within theirs, both released once it is live; reads, DML and subscriptions reading a view being hydrated wait for it and see all of it, a commit applied meanwhile included; tables and views created meanwhile, and commits after them, do not wait for it (the views are hydrated next), a replacement does; replacing a view does not hold up commits, and its subscription gets the change to the new contents and then its changes (also when the new version exists already and a view reading it is re-planned); an upstream schema change (a logged schema record) rebuilds the table while commits go on, holds checkpoints back until it is done, and a crash in the middle recovers the table; a crash in the middle of a hydration (with commits and a checkpoint meanwhile) recovers to the state of an uninterrupted run, error records included; a subscription to a query hydrated while commits go on adds up to the query's result; a view with a temporal filter, window functions and a top-k window hydrated while clock ticks and commits go on equals its query afterwards, as the engine goes on from the fork's state, and after a restart; commits with GSNs after a large checkpoint's are applied and answered while it is written, none waiting for a good part of it; concurrent single-row inserts share log appends (at least two statements per append), concurrent updates of a few hot rows lose no update, and concurrent inserts of one key let exactly one succeed; commits are applied and answered while six large reads run, none waiting for a good part of them, each read seeing the table as of one GSN; commits are applied and answered while the snapshot of a large view is taken for a subscription, which with the changes after it adds up to the view; and DDL (views, also over views, replacements, drops, indexes), DML from four sessions, large and small reads, a subscription and frequent checkpoints all at once, after which (and after a crash and a restart) every view equals its query and the subscription adds up to its view |
| self-differential test (`tests/selfdiff.rs`, proptest) | random schemas of local tables and random views from `ivm-difftest`'s generator, then random actions: DML (also primary key changes and rows changed several times), checkpoints, clean restarts, crashes (sometimes with a torn record at the end of the log), views created, replaced and dropped, and reads AS OF earlier GSNs. After every action each view must equal its query run ad hoc (non-incrementally), reads AS OF a GSN must return what the views held then, and every view, and the queries of some, are subscribed to: the changes streamed must add up to their contents. A failure shrinks to a minimal case |
| dataflow tests (`tests/dataflow.rs`) | operator regression tests: maintained views equal a recompute (an ad hoc query or a freshly hydrated copy) after every change. Window frames past the partition end and `RANGE` offsets that overflow, `DISTINCT` aggregates with `FILTER` (maintained and ad hoc), percentiles over `NULLS FIRST` orders, top-k windows (`LIMIT`, `OFFSET`, `WITH TIES`, per group) and window functions of every kind and frame (`ROWS`, `RANGE` with offsets, `GROUPS`) following random changes with multiplicities, ties, NULLs and errors, and statements changing thousands of rows of one group at once; window frames reaching a partition's end over `min`/`max` values sharing long prefixes and float sums whose terms cancel, general semijoins keeping representations of equal values apart, joins on computed keys, `bigint` variance and `numeric` sums beyond `numeric`'s range; error records of shared nodes, of aggregate groups across recovery, of recursive queries, and of join inputs at hydration |
| recovery tests (`tests/recovery.rs`) | recovery building views after the views they read (a replaced view may read a newer one); a commit larger than a log frame replayed from its parts after a crash; views and indexes following a replaced view's new definition, and replacements rejected when they would invalidate a dependent view or make it non-deterministic; a log tail lost behind a checkpoint (no reused GSNs); `AS OF` reads only through arrangements whose history covers the GSN, also after a restart; views sharing a dataflow sharing its error records; dependencies as the relations a view reads; DML racing a `DROP` of its table; `bigint` variance; checkpoints writing what changed (the manifest record and the bytes written by a checkpoint after one `INSERT` do not grow with 2,000 more DDL statements in the history and 2,000 error records of a view), an idle engine taking no checkpoints, and the error state (node error records and groups in error, also of views dropped and created again) surviving crashes and restarts exactly |
| DDL tests (`tests/ddl.rs`) | validation of definitions (at most one primary key, view column lists, `CREATE SINK` keys); `DROP` taking a relation's indexes along; atomic multi-name `DROP`; `SUBSCRIBE` to a view naming its columns like reads of it; stored definitions with parameter placeholders of earlier versions; the bounded catalog history |
| lifecycle tests (`tests/lifecycle.rs`) | `AS OF AT LEAST LSN` without sources (also when the last source is dropped while a read waits); subscriptions ending when a relation they read is dropped, and not when an error record is retracted; query subscriptions ending when a materialized view they read (also through other views) is replaced; `DROP SINK`, `DROP SOURCE` and shutdown not blocked by a sink or source stuck connecting to a server that never answers; a subscription that is not read ending at its memory limit, and a sink that cannot write no longer being sent changes at its limit |
| memory tests (`tests/memory.rs`) | with a counting allocator (per thread, and for the whole process while one test holds the binary to itself): the buffer pool's accounting against the allocations it makes, for narrow entries, long histories, wide entries with wide summaries (held as prefixes) and exact float sums whose magnitudes spill out of their inline limbs; a full scan of a tree larger than the pool staying within the pool, and leaving the nodes of another tree in it; hydrating views over a table ten times the buffer pool, and views larger still (a filter, joins with fan-out, aggregates over a table and over a join), peaks within the pool plus 40 MiB; a commit ten times the pool into a table with views (a filter, a join, an aggregate) costs at most the pool plus 40 MiB more than the same commit into a table without views |
| restart tests (`tests/restart.rs`) | data directories the current code no longer accepts as they are: a view whose stored definition no longer plans comes up broken (reads fail with `55000`, `SHOW VIEWS` and the web UI show it, `DROP` and `CREATE OR REPLACE` repair it); indexes following their columns by name after an upstream schema change; `now()` in reads `AS OF` earlier GSNs after a restart; a store without its manifest refused with every file left as it was; a materialized view that the current rules reject still maintained, and holding back only the replacements of views it reads; the format-1 data directory `tests/data/format1` (written by the last format-1 version, with error records, groups in error, a source, a pending slot drop and clock ticks) migrated at its first start with everything it held, also across a crash before and after the migration's checkpoint; a newer format refused before anything is touched |
| process tests (`tests/process.rs`) | the `ivm` binary: exit status 1 when the listen address cannot be bound or the data directory is in use; `SIGTERM`, `SIGINT` and the stop file shut down after a final checkpoint with status 0 |
| SQL and protocol tests (`tests/sql_semantics.rs`, `tests/server_protocol.rs`) | results checked against PostgreSQL 17 and inputs that must fail instead of crashing the engine; the wire protocol with raw messages against an in-process server (portals and `Bind`, parameter inference, `SUBSCRIBE` through the extended protocol, session commands and settings, cancellation and disconnects of waiting reads, a subscriber that stops reading ended and its queue released, and cancelled while the session's write waits, large results streamed, with an error after the first rows and through portals) |
| HTTP tests (`tests/http.rs`) | the web UI's HTTP server and JSON API against a real engine (also: catalog dependencies are the relations views name, as the binder resolves them) |
| replication tests (`tests/replication.rs`) | against a private PostgreSQL: snapshots and streaming; an upstream transaction larger than the source's memory spooled and logged with its parts (also replayed after a crash); upstream schema changes (views over views, views a change invalidates and the virtual views over them, `SELECT *` views, DDL and DML in one transaction); `REPLICA IDENTITY USING INDEX` keys and detection of key and replica-identity changes; re-snapshots (deletes during them, keyless tables, a crash before they log progress, a restart before the truncate a key change needs, key changes of TOASTed rows in a chunk's window, rows added after a truncate the chunk saw); chunks waiting for transactions decoded before them (with an unreachable synchronous standby); a table that cannot be read not stopping the others; dropped and invalidated slots re-created; slots of dropped sources dropped after an outage and across restarts; default slot names differing between engines; `SQL_ASCII` upstreams rejected; reconnect backoff reset after a working session; TOASTed values across key changes, keyless tables truncated within a transaction, tables keyed by text-replicated types, `ALTER SOURCE REFRESH`; query subscriptions over a materialized view ending when a table under it changes upstream; sinks: exactly once across restarts, transactions in bounded messages and a backlog beyond the bound replaced by one resynchronising transaction, rejected batches retried with a growing delay while the status keeps the error, a sink thread outliving its sink not undoing the new one, no resynchronisation on restarts, keyless targets with json and with equal values of different representations, view replacements, unreachable targets, a sink whose writes block falling behind and restarted from a snapshot, a sink over a relation too large to snapshot on the engine thread starting from a snapshot taken off it without losing the commits made meanwhile |

The in-process engine tests start the engine with `sync: false` and
`tick: None` and, where time matters, log clock ticks themselves
(`LogRecord::Tick`), so time is fully controlled (the replication tests keep
the default clock tick).

The property tests' default case counts keep `cargo test` fast (10 cases
for the storage model and self-differential tests, 4,096 for the type
properties; `PROPTEST_CASES` overrides them). A failing case is shrunk to a minimal one,
and its seed is saved in a `*.proptest-regressions` file next to the test
(`proptest-regressions/` for unit tests) and re-run first from then on;
these files are committed. Longer variants are ignored by default:

```sh
cargo test -p ivm --lib storage::tests -- --ignored     # 100 cases of up to 2,500 operations (minutes)
cargo test -p ivm --release --test selfdiff -- --ignored  # 50 bigger cases (minutes)
```

Large deltas (beyond the spill threshold) take other code paths than
small ones: spilled into temporary trees, read in batches, their affected
keys collected in temporary trees. Run the dataflow suites with every delta
spilled too:

```sh
IVM_DELTA_SPILL_BYTES=0 cargo test -p ivm --test dataflow --test engine --test recovery --test selfdiff --test ddl --test lifecycle
```

Tests of bugs that are still open are ignored too, with the bug described
in the `#[ignore]` reason (`cargo test -p ivm --test engine -- --ignored`).

### `crates/pgnumeric`

| Suite | What it covers |
|---|---|
| `golden` | `tests/golden/*.tsv`: input (with and without typmod), arithmetic, unary and math functions, precision, casts, typmods, binary send/recv |
| `tochar_golden` | `tests/golden/tochar.jsonl`: `to_char` for `numeric`, `int8`, `int4`, `int2`, `float8`, `float4`, and `to_number` |
| `props` | key encoding order, scale insensitivity, round trips and rejection of garbage; binary encoding round trips; consistency of equality, hashing and ordering; float formatting round trips; helpers |
| unit tests | normalized results, encoding lengths without decoding, exact arithmetic beyond `numeric`'s range (for aggregate states), powers of ten in float formatting |

### `crates/pgdatetime`

| Suite | What it covers |
|---|---|
| `golden` | `tests/golden/*.tsv`: text I/O, time-zone I/O, session time zones, all zone names and abbreviations, typmods, casts, arithmetic, comparison, fields (`extract`, `date_part`), truncation, `to_char`, `to_date`, fuzzed `to_char`/`to_timestamp`/input cases, edge cases, randomised cases |
| `api` | behaviour not expressible as one SQL expression (for example the inverse transition of interval aggregates) |
| `fuzz` | no public function panics on arbitrary input, including out-of-range raw values (`PGDT_FUZZ_SEED`, `PGDT_FUZZ_ITERS`; 60,000 iterations by default) |
| unit tests | the fast paths for ISO dates and timestamps agree with the general parser on random inputs; POSIX time-zone rules; the `chrono-tz` fallback (for zones missing from the embedded database) agrees with the embedded data |
| `bench` (ignored) | rough timings: `cargo test --release -p pgdatetime --test bench -- --ignored --nocapture` |

### `crates/pgfuncs`

| Suite | What it covers |
|---|---|
| `text_golden`, `pattern_golden`, `bytea_golden`, `uuid_golden`, `array_golden`, `json_golden`, `jsonpath_golden` | `tests/data/*.jsonl`: string functions; `LIKE`, `SIMILAR TO` and regular expressions (plus a committed subset of the regex fuzz cases, `regex_fuzz.jsonl`); `bytea` I/O and `encode`/`decode`; `uuid`; array text I/O; json/jsonb functions and operators; SQL/JSON path |
| `array_props`, `json_props` | array format/parse round trips and parser robustness; jsonb key encoding versus ordering, round trips, hash consistency, total-order laws, stack safety for deeply nested values |
| unit tests | Unicode case mapping and character properties, JSON number normalization and ordering, jsonpath internals |

Ignored tests and extra data:

- `PGFUNCS_FUZZ_FILE=<file> cargo test -p pgfuncs --test pattern_golden -- --ignored`
  checks a large regex differential file produced by `tests/gen/fuzz_regex.py`.
- `JSONPATH_GOLDEN_FILE=<file>` makes `jsonpath_golden` check another data
  file (for example from `tests/gen/fuzz_jsonpath.py`);
  `JSONPATH_GOLDEN_DUMP=<file>` writes all failures to a file.

### `crates/pgproto`

| Suite | What it covers |
|---|---|
| unit tests | connection strings (key/value and URI), command tags, LSN text form, MD5 and SCRAM authentication (RFC 7677 vector), `pgoutput` message decoding |
| `client` | the client against PostgreSQL 17: Unix sockets (including abstract ones) and TCP, connection errors and timeouts, authentication methods (trust, cleartext, MD5, SCRAM), simple-query semantics (also rows handed to a callback as values borrowed from the receive buffer, `simple_query_rows`), errors, notices and parameter status, fatal errors, large results, `COPY TO STDOUT` and `COPY FROM STDIN` |
| `replication` | logical replication end to end: slots, `pgoutput` decoding (unchanged TOAST values, `REPLICA IDENTITY FULL`, `TRUNCATE`, logical messages), standby status updates, replication-command errors, keepalives with a short sender timeout |
| `server_codec` | the server-side protocol code: frontend message decoding and errors, startup packets, `BackendWriter` output decoded by the client, a fake walsender, and `psql` talking to a tiny server |
| `testutil` | `TempCluster` lifecycle (stop, restart, `psql`) and stale-cluster cleanup |

## Golden tests

The PostgreSQL ports are verified against expected results recorded from a
real PostgreSQL 17 server. About 375,000 recorded cases are committed:

| Crate | Data | Generator |
|---|---|---|
| `pgnumeric` | `tests/golden/*.tsv` (~27,600 lines), `tests/golden/tochar.jsonl` (~22,800) | `tests/gen_golden.py`, `tests/gen_tochar.py` |
| `pgdatetime` | `tests/golden/*.tsv` (~176,800 lines) | `tools/gen_golden.py` (with `tools/cases_io.py`, `cases_ops.py`, `cases_rand.py`) |
| `pgfuncs` | `tests/data/*.jsonl` (~147,900 lines) | `tests/gen/gen_text.py`, `gen_pattern.py`, `gen_bytea.py`, `gen_uuid.py`, `gen_array.py`, `gen_json.py`, `gen_jsonpath.py`, `fuzz_regex.py`, `fuzz_jsonpath.py` (shared helper `pgeval.py`) |

How the data was produced:

- Each generator evaluates every case on the server and records either the
  result as text (or NULL) or the error's SQLSTATE and message (and detail).
  The `pgnumeric` and `pgdatetime` generators evaluate each expression inside
  a PL/pgSQL helper that catches errors; the `pgfuncs` generators run each
  case as a statement in autocommit mode and record the error the server
  returns.
- Cases come from fixed lists plus random generators with fixed seeds, so a
  generator re-run against the same server reproduces its output.
- The server must be PostgreSQL 17 with database encoding UTF-8. The
  `pgfuncs` generators also need the builtin locale provider with locale
  `C.UTF-8`, and check it:
  `initdb -U postgres --auth=trust --locale-provider=builtin --builtin-locale=C.UTF-8 --locale=C -E UTF8`.
- Each generator sets the session settings its port implements:
  `extra_float_digits = 1` (`pgnumeric`, `pgfuncs`); `lc_numeric` and
  `lc_monetary` `C` (`to_char`); `DateStyle 'ISO, MDY'`,
  `IntervalStyle 'postgres'` and a per-case `TimeZone` (`pgdatetime`);
  `bytea_output 'hex'` (`pgfuncs`, with some cases under `'escape'`); a
  per-case `TimeZone` for jsonpath.
- Formats: `pgnumeric` TSV lines are `op arg1 arg2 arg3 ok result` or
  `op arg1 arg2 arg3 err sqlstate message detail` (COPY-style escapes);
  `pgdatetime` TSV lines are `func session_tz arg1 ... => result` with result
  `OK <text>`, `ERR <sqlstate> <message>` or `NULL`; JSON-lines files hold
  `{"fn": ..., "args": [...], "result": ...}` or
  `{"fn": ..., "args": [...], "error": {"sqlstate", "message", "detail"}}`
  (jsonpath cases may add the session time zone and current date).

Regenerating (each script documents its usage in its header):

```sh
# pgnumeric (psycopg2; libpq-style variables, PGUSER defaults to postgres)
PGHOST=/path/to/socketdir PGPORT=5432 python3 crates/pgnumeric/tests/gen_golden.py
PGHOST=/path/to/socketdir PGPORT=5432 python3 crates/pgnumeric/tests/gen_tochar.py [--out PATH] [--seed N] [--scale K]

# pgdatetime (runs the psql command in PGDT_PSQL, which is required;
# PGDT_GOLDEN_OUT overrides the output directory)
PGDT_PSQL="psql -h /path/to/socketdir -p 5432 -U postgres" python3 crates/pgdatetime/tools/gen_golden.py

# pgfuncs (one script per data file; PGHOST/PGPORT are deliberately ignored)
PGFUNCS_PGHOST=/path/to/socketdir PGFUNCS_PGPORT=5432 python3 crates/pgfuncs/tests/gen/gen_json.py
```

`PGDT_PSQL`, `PGFUNCS_PGHOST`/`PGFUNCS_PGPORT` and, for `gen_tochar.py`,
`PGHOST`/`PGPORT` are required (the scripts exit without them); the
`pgnumeric` scripts also read `PGUSER` and `PGDATABASE` (default `postgres`
for both), and its `gen_golden.py` falls back to libpq's default host and
port 5432.

Larger randomised runs can be checked without committing them:
`PGNUMERIC_EXTRA_GOLDEN=a.tsv:b.tsv cargo test -p pgnumeric --test golden`,
`PGNUMERIC_EXTRA_TOCHAR=big.jsonl cargo test -p pgnumeric --test tochar_golden`,
and `PGDT_GOLDEN_DIR=<dir> cargo test -p pgdatetime --test golden -- --ignored`
(checks every `*.tsv` in the directory; for such runs, `PGDT_SEED` and
`PGDT_SCALE` change the generator's seed and multiply the size of the
`pgdatetime` random cases).

Other generated files: `crates/pgdatetime/src/tz/tzdata.bin` is built by
`tools/gen_tzdata.py` from a compiled IANA zoneinfo directory (the files
PostgreSQL reads with system tzdata); `crates/pgfuncs/src/unicode_tables.rs`
by `tests/gen/gen_unicode_tables.py` from PostgreSQL 17's Unicode headers
(Unicode 15.1); `crates/pgfuncs/src/json/path/tables.rs` by
`tests/gen/gen_jsonpath_tables.py` from PostgreSQL 17's jsonpath grammar and
scanner (bison 3.8.2, flex 2.6.4).

## ivm-difftest

`ivm-difftest` (`crates/ivm/src/bin/ivm-difftest/main.rs`, with the
generator in `sqlgen.rs`, which the self-differential test shares) checks
the central correctness property — a maintained view equals a
recomputation — against PostgreSQL itself.

```sh
cargo build --release
target/release/ivm-difftest [--seed N] [--views N] [--rounds N] [--restarts N] [--kill] [--sinks N] [--schema-changes N] [--keep]
```

| Option | Default | Meaning |
|---|---|---|
| `--seed N` | 42 | seed of the random generator; a run is reproducible from its seed (up to the timing of replication, restarts and kills) |
| `--views N` | 25 | number of random views to create (plus about a quarter more views over views) |
| `--rounds N` | 20 | rounds of random upstream changes |
| `--restarts N` | 0 | restart the engine N times in random rounds, before or right after the round's changes: a clean shutdown, or (mostly) a crash that skips the final checkpoint, so that recovery replays the log |
| `--kill` | off | run the engine as a child process (the `ivm` binary next to `ivm-difftest`) and SIGKILL it for the restarts (3 unless `--restarts` says otherwise): while it applies changes, during a checkpoint, or during the initial snapshot |
| `--sinks N` | 3 | write the first N views that can be read back to PostgreSQL (database `sinks`) with sinks |
| `--schema-changes N` | 1 | add N columns to replicated tables upstream in random rounds |
| `--keep` | off | keep the PostgreSQL cluster and the engine's data directory |

Any other argument prints the usage and exits with status 2.

Environment: `PG_BIN` (PostgreSQL 17 binaries); `PGPROTO_CLUSTER_DIR` and
`TMPDIR` (where the cluster and the engine's data directory
`ivm-difftest-<pid>-<seed>` are created); `DIFFTEST_VERBOSE` (if set, print
every view's query, the upstream transactions, and notes on views not
compared or differing only in the representation of equal `numeric`
values); `DIFFTEST_DUMP_DIR` (write the upstream tables as `INSERT`
statements before every round, `<dir>/round-<n>.sql`, to replay a failing
round); `DIFFTEST_EXTRA_VIEWS` (`q1;;q2;;...`: extra views `e0`, `e1`, ...
compared like the generated ones without changing the seed's random
choices, to narrow a mismatch down with variants of a query); `RUST_LOG`
(default `warn`).

What a run does:

1. Starts a private PostgreSQL 17 cluster (`TempCluster`, with
   `timezone = 'UTC'`) and the engine: in process (without `fsync`, with the
   default clock tick) serving the wire protocol on a free port, or with
   `--kill` as a child process (`--no-sync --tick-ms 0 --checkpoint-secs 1`).
2. Generates a random schema upstream: 3–4 tables `t0`, `t1`, ... with an
   `id integer` primary key, 2–5 columns of random types (`int`, `bigint`,
   `numeric(10,2)`, unconstrained `numeric` with values of varying scale,
   `text`, `boolean`, `date`, `timestamp`, `float8`, `jsonb`, `interval`, and
   `int[]` with one- and two-dimensional values and explicit lower bounds),
   most of them nullable; sometimes a `big` text column of several KB
   stored out of line (TOAST); 1–2 foreign-key-like integer columns
   referencing earlier tables, and sometimes a `pr` column referencing the
   table itself (trees, for recursive queries). Inserts 30–109 rows per
   table.
3. Creates `PUBLICATION difftest FOR ALL TABLES` upstream and a source `src`
   over it in the engine (with `--kill`, the engine is sometimes killed
   during the initial snapshot).
4. Generates random queries — joins of 1–3 tables of every kind (also
   non-equi joins), `LATERAL` subqueries, subqueries in filters (`EXISTS`,
   `NOT EXISTS`, `IN`, `NOT IN`, `ANY`/`ALL`, row-valued `IN`), scalar
   subqueries, aggregates (`count`, `count(DISTINCT)`, `sum`, `avg`, `min`,
   `max`, `bool_and`, ordered `string_agg`, `array_agg`, `json_agg` and
   `jsonb_agg`, `percentile_cont`/`percentile_disc`, `mode()`, with
   `FILTER`), `HAVING`, `ROLLUP`, `CUBE` and `GROUPING SETS` with
   `grouping()`, window functions with `ROWS` and `RANGE` frames,
   `DISTINCT` and `DISTINCT ON`, top-k with `LIMIT`/`OFFSET` and `FETCH
   FIRST ... WITH TIES`, set operations with and without `ALL`, derived
   tables, CTEs and `WITH RECURSIVE`, `VALUES` and set-returning functions
   (`WITH ORDINALITY`), jsonpath and array expressions. Every order that
   decides which rows are returned is total, so any correct implementation
   returns the same rows. It keeps the queries PostgreSQL accepts and
   creates each as a materialized view, printing `unsupported: <error> ::
   <query>` for those the engine rejects (at most 20 × `--views` attempts),
   then adds views over views (grouping, `DISTINCT`, or a join of an
   existing view with an aggregate of itself), which read the materialized
   view in the engine and inline its query in PostgreSQL. Then the sinks.
5. Round 0 compares the initial state. Each later round first applies the
   round's schema changes, if any (`ALTER TABLE ... ADD COLUMN`, sometimes
   with a default, which existing rows get without a replicated change, so
   the source must snapshot the table again; then an update of some rows,
   which brings the new shape into the replication stream; later changes
   use the new column), then runs 1–4 random upstream transactions of 1–8
   statements: inserts, updates of a random column, primary-key changes
   (also of many rows at once), deletes, rows inserted and changed (and
   deleted) again or deleted and inserted again within the transaction,
   upserts (`ON CONFLICT DO UPDATE`), and occasionally `TRUNCATE` (a failing
   transaction is rolled back and reported as ignored); sometimes a
   `CHECKPOINT`; and the round's restart, if any.
6. It then reads `pg_current_wal_lsn()` upstream and compares every view,
   read with `AS OF AT LEAST LSN '<lsn>'`, with the same query run by
   PostgreSQL, and every replicated table with its upstream contents.
   Results are compared as sorted lists of rows in text form, with
   floating-point values rounded to 10 significant digits. For queries that
   deduplicate (`GROUP BY`, `DISTINCT`, a set operation, `min`, `max`,
   `mode`, `percentile_disc`, `LIMIT` or `FETCH`), rows that differ only in
   the representation of equal `numeric` values (`1.0` and `1.00`) also
   match, because PostgreSQL keeps whichever representation it meets
   first. A view over a view that fails in the engine is not compared (it
   fails with its source, while PostgreSQL need not compute the failing
   column). Each sink's table must converge to its view's contents within
   20 seconds.
7. Prints `round <n>: <m>/<k> views match (<rows> rows, <e> both-error), <s> sinks (<r> rows), in <time>`
   (and each schema change and restart as it happens).
   For a mismatch it prints the view and its query, both row counts, up to
   five rows found only in PostgreSQL and only in the engine (all rows if
   only the multiplicities differ), and whether the same query run ad hoc
   in the engine agrees with PostgreSQL (a maintenance bug) or with the view
   (a query semantics bug); likewise for tables (`TABLE MISMATCH`) and sinks
   (`SINK MISMATCH`, with the sink's status). A view that fails in both
   systems counts as both-error; a view that fails only in the engine is a
   failure; one that fails only in PostgreSQL is reported but not counted.
   After any round but round 0 that had failures, the run stops.

Exit status: 0 with `OK (seed N)`, 1 with
`FAILED with <n> mismatches (seed N)`. Example:

```
$ ivm-difftest --seed 7 --views 6 --rounds 2
created 7 views (0 rejected)
round 0: 7/7 views match (146 rows, 0 both-error), 3 sinks (105 rows), in 160.445808ms
round 1: ALTER TABLE t2 ADD COLUMN s6 text DEFAULT 'ü'; UPDATE t2 SET id = id WHERE id % 3 = 0 OR id = (SELECT min(id) FROM t2);
round 1: 7/7 views match (146 rows, 0 both-error), 3 sinks (105 rows), in 8.111596ms
round 2: 7/7 views match (146 rows, 0 both-error), 3 sinks (105 rows), in 11.88653ms
OK (seed 7)
```

To investigate a failure, re-run with the same `--seed` and
`DIFFTEST_VERBOSE=1` (with restarts or kills, the timing can make a run
pass or fail), and add `--keep` to inspect the cluster and the engine's
data directory afterwards. Campaigns run many seeds in parallel, e.g.
`seq 1 32 | xargs -P 4 -I{} sh -c 'target/release/ivm-difftest --seed {} --views 50 --schema-changes 3 > dt-{}.log 2>&1 || echo "seed {} failed"'`.

## ivm-bench

`ivm-bench` (`crates/ivm/src/bin/ivm-bench.rs`) is a reproducible
performance benchmark. It generates a deterministic data set in PostgreSQL,
replicates it into an in-process engine and drives the engine's pgwire
server like a client, measuring one phase after another.

```sh
cargo build --release
target/release/ivm-bench [--accounts N] [--txns N] [--small N] [--large N]
    [--point-reads N] [--sub-txns N] [--upstream CONNINFO] [--data-dir DIR]
    [--memory-mb N] [--no-sync] [--skip PHASE,...] [--json FILE] [--keep-data]
    [--repeat N]
```

| Option | Default | Meaning |
|---|---|---|
| `--accounts N`, `--txns N` | 100,000, 2,000,000 | rows of the upstream tables `accounts` and `txns` |
| `--small N` | 20,000 | single-statement transactions per small-transaction phase |
| `--large N` | 500,000 | rows of the large `INSERT` and `UPDATE` transactions |
| `--point-reads N` | 2,000 | queries per point-read phase |
| `--sub-txns N` | 5,000 | upstream transactions delivered to a `SUBSCRIBE` |
| `--upstream CONNINFO` | private cluster | use an existing cluster (`wal_level = logical`) |
| `--data-dir DIR` | `TMPDIR` | parent of the engine's data directory |
| `--memory-mb N` | 2048 | the engine's memory budget (`--memory-mb`; `--cache-mb` is accepted too) |
| `--no-sync` | off | run the engine without `fsync` |
| `--skip PHASE,...` | none | skip `small`, `recovery`, `large`, `reads` or `subscribe` |
| `--json FILE` | none | also write the results to `FILE` as a JSON array |
| `--keep-data` | off | keep the engine's data directory |
| `--repeat N` | 1 | run hydration, the small-transaction phases and the large transactions N times (every run is recorded) |

Without `--upstream` a private cluster is started (`TempCluster`, with
`PG_BIN`) and the data generated in it. With `--upstream`, the data set is
generated once into a template database `ivmbench_v2_<accounts>_<txns>` and
every run works on a copy of it (`ivmbench_run_<pid>`, with a replication
slot of its own), so runs start from identical data; run databases and
slots left behind by crashed runs are dropped.

The data: `accounts (id, region, name, balance)` with 20 regions, and
`txns (id, account_id, kind, amount, created)` whose account, kind and
amount are hashes of the id. The phases:

| Phase | What is measured |
|---|---|
| `load` | initial snapshot of both tables (source parsing, table updates) |
| `checkpoint_tables`, `checkpoint_views` | an explicit checkpoint; store size |
| `hydrate_by_region`, `hydrate_acct_totals` | `CREATE MATERIALIZED VIEW` of a join aggregate (`by_region`) and of a `LEFT JOIN` aggregate per account (`acct_totals`) over the loaded data |
| `small_live` | small transactions (inserts, updates and deletes of `txns`, updates of `accounts`) applied while the engine runs |
| `shutdown`, `recovery_start` | wall time of the engine's shutdown and restart in between |
| `small_catchup` | as many committed while the engine was down, caught up after the restart (cold cache) |
| `large_insert`, `large_update` | one transaction inserting, then one updating, `--large` rows, live |
| `read_*`, `adhoc_*` | point, range and scan reads of the views and tables; ad hoc aggregates and joins over the tables |
| `subscribe_snapshot`, `subscribe_live` | a `SUBSCRIBE`'s snapshot and its delivery of live changes |
| `recovery_shutdown`, `recovery` | a final shutdown, then a restart from its checkpoint until the first read |

Each phase prints a line and a `RESULT {...}` JSON record with its wall
time; CPU time per thread group, from `/proc/self/task/*/schedstat`
(`engine_cpu_s`: the thread that owns storage and the dataflow;
`source_cpu_s`: snapshot and replication decoding; `sequencer_cpu_s`;
`session_cpu_s`: pgwire), the engine thread's run-queue wait; the
upstream cluster's CPU time (when it runs on the same machine); the peak
RSS during the phase (`VmHWM`, reset per phase); and phase-specific rates
and latencies. The binary's global allocator counts allocations per
thread (`engine_allocs`, `source_allocs`): unlike CPU time on a busy
machine these are deterministic, a noise-free measure of allocation churn.
Each phase's time window is printed as `WINDOW <phase> <start> <end>`
(nanoseconds since the process started), to select it in a profile, e.g.
`gprofng display text -filters 'TSTAMP>=<start> && TSTAMP<=<end>' -functions x.er`
for an experiment recorded with `gprofng collect app -o x.er target/release/ivm-bench ...`.

`bench/compare.py` summarizes `--json` files (minimum and median of every
metric per phase) and compares two sets of runs:

```sh
bench/compare.py base-1.json base-2.json -- new-1.json new-2.json
```

CPU times on a shared machine vary by tens of percent between runs:
interleave the runs of the variants being compared (A B A B ...), compare
medians and minimums, and check allocation counts.
