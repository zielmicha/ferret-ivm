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
| `IVM_TEST_TMP` | `ivm` engine and storage tests | parent of their data directories (default: the temporary directory); some of these tests leave their directories behind |

Only the `pgproto` integration tests (`client`, `replication`,
`server_codec`, `testutil`) and `ivm-difftest` need PostgreSQL binaries; all
other tests are self-contained. Golden tests read recorded data and need no
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
| unit tests (`cargo test -p ivm --lib`) | input-log round trip and torn-tail truncation (`log.rs`); history compaction (`storage/history.rs`); exact float sums and min/max combination (`storage/summary.rs`); key encoding: order, prefix-freedom, NULL placement, round trips, successors (`types/encoding.rs`); row codec and varints (`types/codec.rs`); statement splitting and the engine's own statement parser (`sql/parser.rs`); transaction-id visibility across epochs (`source/postgres.rs`) |
| storage model tests (`storage/tests.rs`) | random batches on a tree compared with a `BTreeMap` model — scans, point lookups, range summaries and `nth` — with checkpoints, garbage collection and re-opens in between, on tiny nodes, segments and cache; large batches; 100 KB keys with long shared prefixes; time-travel reads and consolidation behind the frontier; segment reclamation after dropping a tree; incrementally maintained summaries (`numeric` sum, min, max, count, sum of squares over equal values in several representations, NaN, infinities and NULLs) checked against recomputation (`Env::check_summaries`) and a fold over the model, with small and large nodes |
| engine tests (`tests/engine.rs`) | through the public API, without PostgreSQL: temporal filters following the logged clock, across a restart, and with the clock never going backwards; rejection of invalid temporal filters; aggregates equal to a recomputation after random changes with mixed `numeric` representations; temporal views (joins, aggregates, outer joins, `date_trunc`) equal to a recomputation as random changes and ticks interleave; recovery after drops (checkpointed indexes are found again although node ids changed; a multiset table keeps its rows); jsonpath views; multi-dimensional arrays and lockstep set-returning functions |

The engine tests start an engine with `sync: false` and `tick: None` and log
clock ticks themselves (`LogRecord::Tick`), so time is fully controlled.

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
| `api_surface` | compile-time check of the public API's signatures |

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

`ivm-difftest` (`crates/ivm/src/bin/ivm-difftest.rs`) checks the central
correctness property — a maintained view equals a recomputation — against
PostgreSQL itself.

```sh
cargo build --release
target/release/ivm-difftest [--seed N] [--views N] [--rounds N] [--keep]
```

| Option | Default | Meaning |
|---|---|---|
| `--seed N` | 42 | seed of the random generator; a run is reproducible from its seed |
| `--views N` | 25 | number of random views to create (plus about a quarter more views over views) |
| `--rounds N` | 20 | rounds of random upstream changes |
| `--keep` | off | keep the PostgreSQL cluster and the engine's data directory |

Any other argument prints the usage and exits with status 2.

Environment: `PG_BIN` (PostgreSQL 17 binaries); `PGPROTO_CLUSTER_DIR` and
`TMPDIR` (where the cluster and the engine's data directory
`ivm-difftest-<pid>-<seed>` are created); `DIFFTEST_VERBOSE` (if set, print
every view's query and a note whenever rows differ only in the
representation of equal `numeric` values); `RUST_LOG` (default `warn`).

What a run does:

1. Starts a private PostgreSQL 17 cluster (`TempCluster`, with
   `timezone = 'UTC'`) and an in-process engine (without `fsync`, with the
   default clock tick) serving the wire protocol on a free port.
2. Generates a random schema upstream: 3–4 tables `t0`, `t1`, ... with an
   `id integer` primary key, 2–5 columns of random types (`int`, `bigint`,
   `numeric(10,2)`, unconstrained `numeric` with values of varying scale,
   `text`, `boolean`, `date`, `timestamp`, `float8`, `jsonb`, and `int[]` with
   one- and two-dimensional values and explicit lower bounds), most of them
   nullable, plus 1–2 foreign-key-like integer columns referencing earlier
   tables. Inserts 30–109 rows per table.
3. Creates `PUBLICATION difftest FOR ALL TABLES` upstream and a source `src`
   over it in the engine.
4. Generates random queries — joins of 1–3 tables (inner, left, right, full,
   along the reference columns where possible), `LEFT JOIN LATERAL` top-1
   subqueries, filters with subqueries (`EXISTS`, `NOT EXISTS`, `IN`,
   `NOT IN`, `= ANY`, `> ALL`, row-valued `IN`, scalar `count`), aggregates
   (`count`, `count(DISTINCT)`, `sum`, `avg`, `min`, `max`, `bool_and`,
   ordered `string_agg` and `array_agg` with `FILTER`, `mode()`), `HAVING`,
   `ROLLUP`, `CUBE` and `GROUPING SETS` with `grouping()`, window functions
   with `ROWS` and `RANGE` frames, scalar subqueries in the select list,
   set-returning functions, `DISTINCT`, top-k with `LIMIT`/`OFFSET` and
   `FETCH FIRST ... WITH TIES`, `UNION`/`INTERSECT`/`EXCEPT` with and without
   `ALL`, derived tables and CTEs, jsonpath and array expressions. It keeps
   the queries PostgreSQL accepts and creates each as a materialized view,
   printing `unsupported: <error> :: <query>` for those the engine rejects
   (at most 20 × `--views` attempts). It then adds views over views
   (grouping, `DISTINCT` and a self-join of an existing view), which read the
   materialized view in the engine and inline its query in PostgreSQL.
5. Round 0 compares the initial state. Each later round first runs 1–4
   random upstream transactions of 1–8 statements: inserts, updates of a
   random column, primary-key changes, deletes, and occasionally `TRUNCATE`
   (a failing transaction is rolled back and reported as ignored).
6. It then reads `pg_current_wal_lsn()` upstream and compares every view,
   read with `AS OF AT LEAST LSN '<lsn>'`, with the same query run by
   PostgreSQL. Results are compared as sorted lists of rows in text form,
   with floating-point values rounded to 10 significant digits. For queries
   containing `GROUP BY`, `DISTINCT`, a set operation, `min`, `max`, `mode`,
   `LIMIT` or `FETCH`, rows that differ only in the representation of equal
   `numeric` values (`1.0` and `1.00`) also match, because PostgreSQL keeps
   whichever representation it meets first.
7. Prints `round <n>: <m>/<k> views match (<rows> rows, <e> both-error) in <time>`.
   For a mismatch it prints the view and its query, both row counts, and up
   to five rows found only in PostgreSQL and only in the engine (all rows if
   only the multiplicities differ). A view that fails in both systems counts
   as both-error; a view that fails only in the engine is a failure; one that
   fails only in PostgreSQL is reported but not counted. After any round but
   round 0 that had failures, the run stops.

Exit status: 0 with `OK (seed N)`, 1 with
`FAILED with <n> mismatches (seed N)`. Example:

```
$ ivm-difftest --seed 7 --views 6 --rounds 2
created 7 views (0 rejected)
round 0: 7/7 views match (99 rows, 0 both-error) in 2.770501ms
round 1: 7/7 views match (104 rows, 0 both-error) in 59.942671ms
round 2: 7/7 views match (91 rows, 0 both-error) in 106.878531ms
OK (seed 7)
```

To investigate a failure, re-run with the same `--seed` and
`DIFFTEST_VERBOSE=1`, and add `--keep` to inspect the cluster and the
engine's data directory afterwards.
