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
| `PG_BIN` | `pgproto` integration tests, `ivm-difftest` | directory with PostgreSQL 17's `initdb`, `pg_ctl`, `postgres` and `psql` (default `/usr/lib/postgresql/17/bin`) |
| `PGPROTO_CLUSTER_DIR` | `pgproto::testutil::TempCluster` | parent directory of temporary clusters (default `$TMPDIR/pgproto-clusters`) |
| `TMPDIR` | everything | temporary files; also the fallback directory for a cluster's Unix socket when the cluster directory is too deep for a socket path (107 bytes) |
| `IVM_TEST_TMP` | `ivm` engine and storage tests | parent of their data directories (default: the temporary directory); a failing test keeps its directory for inspection |
| `PROPTEST_CASES`, `PROPTEST_RNG_SEED` | property tests (proptest) | number of cases per property (each test has its own default), and a fixed seed for the random cases |
| `SELFDIFF_VERBOSE` | `tests/selfdiff.rs` | print each case's views and actions as it runs |

Only the `pgproto` integration tests (`client`, `replication`,
`server_codec`, `testutil`), the engine's replication tests
(`crates/ivm/tests/replication.rs`) and `ivm-difftest` need PostgreSQL
binaries; all other tests are self-contained. Golden tests read recorded data and need no
server.

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
| unit tests (`cargo test -p ivm --lib`) | input-log round trip, torn-tail truncation and corruption before the tail (`log.rs`); exact float sums (`storage/summary.rs`); the sequencer's ticks under load (`coord/seq.rs`); statement splitting, the engine's own statement parser, planning panics turned into errors (`sql`); the source's snapshot chunks against a fake engine: truncates and keyless changes around a chunk, schema changes in the middle of a transaction, transaction-id visibility across epochs (`source/postgres.rs`); introspection and the web UI's assets (`coord/inspect.rs`, `http`) |
| type property tests (`types/tests.rs`, proptest) | over random values of every column type (numerics of any scale, floats with NaN payloads and signed zeros, intervals, json, arrays with bounds and NULLs, ...): key order equals SQL order, for single values and composite keys; keys decode to the canonical value and, with the fidelity suffix, to the exact one; values and rows round-trip through the row codec, text I/O and binary send/recv; prefix successors bound every extension. Plus key order on a table of edge values per type (`types/encoding.rs`) |
| storage model test (`storage/tests.rs`, proptest) | random operation sequences on several trees (counts, and numeric, float and integer summaries over values embedded in the keys) against a model of every key's (GSN, weight) history: weighted batches, retention frontier advances, tree creation and drops, checkpoints, GC, clean reopens and crashes (no final checkpoint; garbage after the durable end of the manifest or the active segment; stray segment files; a crash in the middle of a checkpoint). Node and segment sizes, retention and the buffer pool (down to a few KB) are part of the case. Checked: point lookups after every step; scans now and AS OF every retained GSN, range and prefix scans, range summaries and `nth`; the tree structure (key ranges, parent links, `min_seg`) and the segment accounting; after a crash, the state of the last checkpoint, and replaying the operations since then must reproduce the state before the crash. Also: entries retracted behind the frontier are consolidated away, and a full-history model test with AS OF reads across crashes, checkpoints and GC that ends with GC keeping the store within a small factor of its live data |
| engine tests (`tests/engine.rs`) | through the public API, without PostgreSQL: temporal filters following the logged clock, across a restart, and with the clock never going backwards; rejection of invalid temporal filters; temporal views (joins, aggregates, outer joins, `date_trunc`) equal to a recomputation as random changes and ticks interleave; recovery after drops (checkpointed indexes are found again although node ids changed; a multiset table keeps its rows); jsonpath views; multi-dimensional arrays and lockstep set-returning functions; regression tests (DISTINCT ON over infinite intervals, ordered aggregates after a DISTINCT one, percentiles differing only in their fraction, `WITH ORDINALITY` after `LATERAL`, exact representations of values derived from presence changes, subscriptions to a view sharing its dataflow with a dropped or replaced view, subscriptions ending when a replacement changes the view's columns) and an ignored test of an open bug |
| self-differential test (`tests/selfdiff.rs`, proptest) | random schemas of local tables and random views from `ivm-difftest`'s generator, then random actions: DML (also primary key changes and rows changed several times), checkpoints, clean restarts, crashes (sometimes with a torn record at the end of the log), views created, replaced and dropped, and reads AS OF earlier GSNs. After every action each view must equal its query run ad hoc (non-incrementally), reads AS OF a GSN must return what the views held then, and every view, and the queries of some, are subscribed to: the changes streamed must add up to their contents. A failure shrinks to a minimal case |
| dataflow tests (`tests/dataflow.rs`) | operator regression tests: maintained views equal a recompute (an ad hoc query or a freshly hydrated copy) after every change; error records of shared nodes, across recovery and in recursive queries |
| SQL and protocol tests (`tests/sql_semantics.rs`, `tests/server_protocol.rs`) | results checked against PostgreSQL 17 and inputs that must fail instead of crashing the engine; the wire protocol with raw messages against an in-process server |
| HTTP tests (`tests/http.rs`) | the web UI's HTTP server and JSON API against a real engine |
| replication tests (`tests/replication.rs`) | against a private PostgreSQL: snapshots and streaming, upstream schema changes (views over views, views a change invalidates, `SELECT *` views, DDL and DML in one transaction), re-snapshots (deletes during them, keyless tables, a crash before they log progress, key changes of TOASTed rows in a chunk's window, rows added after a truncate the chunk saw), TOASTed values across key changes, keyless tables truncated within a transaction, tables keyed by text-replicated types, `ALTER SOURCE REFRESH`; sinks: exactly once across restarts, keyless targets with json and with equal values of different representations, view replacements, unreachable targets |

The engine tests start an engine with `sync: false` and `tick: None` and log
clock ticks themselves (`LogRecord::Tick`), so time is fully controlled.

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

Tests of bugs that are still open are ignored too, with the bug described
in the `#[ignore]` reason (`cargo test -p ivm --test engine -- --ignored`).

### `crates/pgnumeric`

| Suite | What it covers |
|---|---|
| `golden` | `tests/golden/*.tsv`: input (with and without typmod), arithmetic, unary and math functions, precision, casts, typmods, binary send/recv |
| `tochar_golden` | `tests/golden/tochar.jsonl`: `to_char` for `numeric`, `int8`, `int4`, `int2`, `float8`, `float4`, and `to_number` |
| `props` | key encoding order, scale insensitivity, round trips and rejection of garbage; binary encoding round trips; consistency of equality, hashing and ordering; float formatting round trips; helpers |

### `crates/pgdatetime`

| Suite | What it covers |
|---|---|
| `golden` | `tests/golden/*.tsv`: text I/O, time-zone I/O, session time zones, all zone names and abbreviations, typmods, casts, arithmetic, comparison, fields (`extract`, `date_part`), truncation, `to_char`, `to_date`, fuzzed `to_char`/`to_timestamp`/input cases, edge cases, randomised cases |
| `api` | behaviour not expressible as one SQL expression (for example the inverse transition of interval aggregates) |
| `fuzz` | no public function panics on arbitrary input, including out-of-range raw values (`PGDT_FUZZ_SEED`, `PGDT_FUZZ_ITERS`; 60,000 iterations by default) |
| `bench` (ignored) | rough timings: `cargo test --release -p pgdatetime --test bench -- --ignored --nocapture` |

### `crates/pgfuncs`

| Suite | What it covers |
|---|---|
| `text_golden`, `pattern_golden`, `bytea_golden`, `uuid_golden`, `array_golden`, `json_golden`, `jsonpath_golden` | `tests/data/*.jsonl`: string functions; `LIKE`, `SIMILAR TO` and regular expressions (plus a committed subset of the regex fuzz cases, `regex_fuzz.jsonl`); `bytea` I/O and `encode`/`decode`; `uuid`; array text I/O; json/jsonb functions and operators; SQL/JSON path |
| `array_props`, `json_props` | array format/parse round trips and parser robustness; jsonb key encoding versus ordering, round trips, hash consistency, total-order laws, stack safety for deeply nested values |

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
| `client` | the client against PostgreSQL 17: Unix sockets (including abstract ones) and TCP, connection errors and timeouts, authentication methods (trust, cleartext, MD5, SCRAM), simple-query semantics, errors, notices and parameter status, fatal errors, large results, `COPY TO STDOUT` and `COPY FROM STDIN` |
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

# pgdatetime (runs psql; PGDT_GOLDEN_OUT overrides the output directory)
PGDT_PSQL="psql -h /path/to/socketdir -p 5432 -U postgres" python3 crates/pgdatetime/tools/gen_golden.py

# pgfuncs (one script per data file; PGHOST/PGPORT are deliberately ignored)
PGFUNCS_PGHOST=/path/to/socketdir PGFUNCS_PGPORT=5432 python3 crates/pgfuncs/tests/gen/gen_json.py
```

Always set `PGDT_PSQL`: its built-in default refers to a server of the
original development setup.

Larger randomised runs can be checked without committing them:
`PGNUMERIC_EXTRA_GOLDEN=a.tsv:b.tsv cargo test -p pgnumeric --test golden`,
`PGNUMERIC_EXTRA_TOCHAR=big.jsonl cargo test -p pgnumeric --test tochar_golden`,
and `PGDT_GOLDEN_DIR=<dir> cargo test -p pgdatetime --test golden -- --ignored`
(checks every `*.tsv` in the directory; `PGDT_SEED` and `PGDT_SCALE` change
the seed and multiply the size of the `pgdatetime` random cases).

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
   then adds views over views (grouping, `DISTINCT` and a self-join of an
   existing view), which read the materialized view in the engine and
   inline its query in PostgreSQL. Then the sinks.
5. Round 0 compares the initial state. Each later round first applies the
   round's schema changes, if any (`ALTER TABLE ... ADD COLUMN`, sometimes
   with a default, which existing rows get without a replicated change, so
   the source must snapshot the table again; then an update of some rows,
   which brings the new shape into the replication stream; later changes
   use the new column), then runs 1–4 random upstream transactions of 1–8
   statements: inserts, updates of a random column, primary-key changes,
   deletes, and occasionally `TRUNCATE` (a failing transaction is rolled
   back and reported as ignored); sometimes a `CHECKPOINT`; and the
   round's restart, if any.
6. It then reads `pg_current_wal_lsn()` upstream and compares every view,
   read with `AS OF AT LEAST LSN '<lsn>'`, with the same query run by
   PostgreSQL, and every replicated table with its upstream contents.
   Results are compared as sorted lists of rows in text form, with
   floating-point values rounded to 10 significant digits. For queries that
   deduplicate (`GROUP BY`, `DISTINCT`, a set operation, `min`, `max`,
   `mode`, `LIMIT` or `FETCH`), rows that differ only in the representation
   of equal `numeric` values (`1.0` and `1.00`) also match, because
   PostgreSQL keeps whichever representation it meets first. A view over a
   view that fails in the engine is not compared (it fails with its source,
   while PostgreSQL need not compute the failing column). Each sink's table
   must converge to its view's contents within 20 seconds.
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
