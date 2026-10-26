# Getting started

This chapter covers building `ivm`, running the server binary, connecting to
it, and a first session that replicates a PostgreSQL table and maintains a
view over it.

## Building and installing

`ivm` is a Cargo workspace (`Cargo.toml` at the repository root) with five
crates: the engine (`crates/ivm`) and four supporting libraries
(`pgnumeric`, `pgdatetime`, `pgfuncs`, `pgproto`). The workspace crates have
no build scripts, but two dependencies compile C or assembly code with the
`cc` crate — `libmimalloc-sys` (the mimalloc allocator the binaries use) and
`psm` (pulled in by `sqlparser` through `recursive` and `stacker`) — so a C
toolchain (`cc`) must be installed.

Requirements:

- A recent stable Rust toolchain. `rust-toolchain.toml` selects the `stable`
  channel (with `clippy` and `rustfmt`); the code uses Rust 2024 edition
  features and recent standard library APIs such as `File::try_lock`
  (Rust 1.89).
- For the test suites that talk to a real server, and for `ivm-difftest`:
  PostgreSQL 17 server binaries (`initdb`, `pg_ctl`, `postgres`), by default
  in `/usr/lib/postgresql/17/bin` (see [Testing](../testing.md)).

```sh
cargo build --release
```

This produces three binaries:

| Binary | Purpose |
|---|---|
| `target/release/ivm` | the server |
| `target/release/ivm-difftest` | differential tester against a real PostgreSQL ([Testing](../testing.md#ivm-difftest)) |
| `target/release/ivm-bench` | benchmark harness ([Testing](../testing.md#ivm-bench)) |

Build profiles (from the workspace `Cargo.toml`): `dev` builds the workspace
crates with `opt-level = 1` and all dependencies with `opt-level = 2`, with
debug info; `release` keeps line tables (`debug = 1`). A debug build
(`cargo build`) is therefore usable for experiments, with
`target/debug/ivm`.

To install the binaries into `~/.cargo/bin`:

```sh
cargo install --path crates/ivm            # installs ivm, ivm-difftest and ivm-bench
cargo install --path crates/ivm --bin ivm  # just the server
```

The server is a single binary with embedded storage; it needs nothing at
runtime except a writable data directory and, for replication, network
access to the upstream PostgreSQL.

## Running the server

```sh
ivm --data-dir ./ivm-data --listen 127.0.0.1:6875
```

On start the server binds the listen address, opens (or creates) the data
directory, recovers the last checkpoint, restarts the sinks, replays the
input log written after the checkpoint, restarts the sources, and then
accepts connections (a client that connects during recovery waits until
then). Log output goes to standard error. See
[Recovery](operations.md#recovery).

### Command-line flags

Every flag is parsed in `crates/ivm/src/main.rs`; defaults come from
`coord::Config::default()` and `ServerConfig`.

| Flag | Default | Meaning |
|---|---|---|
| `--data-dir DIR` | `ivm-data` (relative to the working directory) | Data directory: input log, storage segments, manifest, format version, lock file. Created if missing. See [Operations](operations.md#the-data-directory). |
| `--listen ADDR` | `127.0.0.1:6875` | TCP address of the PostgreSQL-protocol server (`host:port`, passed to `TcpListener::bind`). There is no Unix-socket listener. |
| `--password PW` | none | If given, every client must send this password (cleartext password authentication; the user name is not checked). Without it, any user name and password are accepted. |
| `--memory-mb N` | `2048` | Memory budget in MiB: the buffer pool of index nodes, aggregation caches and queued records and changes together (`--cache-mb` is an older name). See [Memory](operations.md#memory). |
| `--checkpoint-secs N` | `10` | Seconds between checkpoints (a checkpoint is only taken if something other than clock ticks was applied since the last one; ticks alone make one due after 3,600 of them). See [Checkpoints](operations.md#checkpoints-and-garbage-collection). |
| `--retention-gsns N` | `1000` | Number of GSNs of history kept for `AS OF` reads. Older versions are consolidated. |
| `--tick-ms N` | `1000` | Interval of logged wall-clock ticks, which define `now()`. `0` disables ticks; `now()` then stays at the last logged tick (2000-01-01 00:00:00 UTC on a data directory that never had one). |
| `--no-sync` | off (fsync on) | Do not `fsync` the input log after each commit batch. Faster, but an operating-system crash or power loss can lose changes that were already acknowledged to sources and clients; see [Operations](operations.md#durability-and---no-sync). |
| `--http-listen ADDR` | off | Serve the [web UI](../web-ui.md) and its JSON API on `ADDR` (`host:port`, e.g. `127.0.0.1:6876`). With `--password`, every HTTP request needs Basic authentication with that password (any user name). |
| `-h`, `--help` | | Print usage and exit with status 2. |

Any unknown flag, a missing value or an unparsable number also prints the
usage text and exits with status 2.

Exit statuses and start-up failures:

| Situation | Message on standard error | Status |
|---|---|---|
| bad command-line arguments, `--help` | the usage text | 2 |
| the listen address cannot be bound (it is bound before recovery) | `ivm: cannot listen on <addr>: <reason>` (for example `Address already in use`) | 1 |
| the engine cannot start (the data directory is locked, recovery fails, ...) | `ivm: failed to start: <reason>` | 1 |
| the web UI cannot start | `ivm: cannot start the web UI: <reason>` | 1 |
| the engine stops by itself after an error, or the listener fails | `ivm: the engine stopped` (the cause is logged before), `ivm: server error: <reason>` | 1 |
| a requested shutdown (below) whose final checkpoint succeeded | | 0 |
| a requested shutdown during which a thread failed or the final checkpoint failed | | 1 |
| a second `SIGINT`/`SIGTERM` | `ivm: second signal, exiting without a final checkpoint` | 1 |

There is no configuration file. Two environment variables affect the server:

| Variable | Effect |
|---|---|
| `RUST_LOG` | `env_logger` filter; default `info`. For example `RUST_LOG=debug` or `RUST_LOG=ivm::source=debug`. |
| `IVM_NO_GROUP_CACHE` | If set (to anything), disables the per-group summary cache of grouped aggregations (a debugging aid; results are the same). |

### Stopping the server

Three ways request a graceful shutdown: `SIGINT` (Ctrl-C), `SIGTERM`, and a
stop file — every 500 ms the process checks for a file named `.ivm-stop` in
its **current working directory** (not the data directory) and deletes it
when it appears. The server then asks sources and sinks to stop (sinks first
record everything applied as their progress), stops the sequencer, takes a
final checkpoint, waits up to 5 s for source and sink threads, and exits
with status 0 (status 1 if the checkpoint or another thread failed). A
request during recovery takes effect once recovery has finished.

A second signal exits at once (status 1), without the final checkpoint.
This is safe, as is `SIGKILL` or a crash: everything that was acknowledged
is in the input log, and the next start replays the log written after the
last checkpoint. See [Recovery](operations.md#recovery).

Only one process can use a data directory at a time; a second `ivm` on the
same directory fails with
`data directory ... is in use by another ivm process` (see
[the lock file](operations.md#the-lock-file)).

## Connecting

The server speaks the PostgreSQL frontend/backend protocol version 3, so
`psql` and PostgreSQL drivers connect to it directly:

```sh
psql -h 127.0.0.1 -p 6875 -U ivm
```

- **User and database names** are not checked; any values work. The session
  reports itself as user `ivm` in database `ivm` (`current_user`,
  `current_database()`).
- **Authentication**: none by default. With `--password`, the server requests
  a cleartext password (`AuthenticationCleartextPassword`); a wrong password
  fails with SQLSTATE `28P01`. MD5 and SCRAM are not offered by the server.
- **TLS** is not supported: the server answers `N` to an SSL or GSSAPI
  encryption request. Clients must use `sslmode=disable` or `prefer` (the
  libpq default); `sslmode=require` fails.
- The server announces `server_version` `17.0`, `server_encoding` and
  `client_encoding` `UTF8`, `DateStyle` `ISO, MDY`, `IntervalStyle`
  `postgres`, `TimeZone` `UTC`, `integer_datetimes` `on`,
  `standard_conforming_strings` `on`, `is_superuser` `on`,
  `session_authorization` `ivm`, and echoes `application_name`.

### Drivers

Supported protocol features:

- the simple query protocol, including several statements in one query
  string (execution stops at the first error);
- the extended protocol (Parse/Bind/Describe/Execute/Sync/Close/Flush),
  including `$n` parameters, text and binary parameter and result formats,
  and partial fetches with an `Execute` row limit (`PortalSuspended`);
- `COPY (SUBSCRIBE ...) TO STDOUT` (CopyOut, text format);
- query cancellation (`CancelRequest`) — it interrupts a running
  `SUBSCRIBE`, a read that waits (for a snapshot, an LSN or a GSN) and DML
  queued behind another statement (`57014`). A statement that is already
  executing on the engine thread, or DML already written to the log, runs
  to completion.

Not supported: `DECLARE`/`FETCH` cursors (so named "server-side cursors" in
psycopg and similar drivers fail; use the protocol-level row limit instead),
`COPY` in any other form, the function-call protocol (answered with an
error), `LISTEN`/`NOTIFY`, TLS.

Transactions are not supported. `BEGIN`, `COMMIT` and `ROLLBACK` are accepted
and only change the transaction status reported in `ReadyForQuery`; every
statement is applied on its own as soon as it runs, and **`ROLLBACK` does not
undo anything**. Drivers that wrap statements in transactions work, but
without transactional semantics. See [Reads](reads.md#transactions).

`SET` accepts any parameter name; the settings the engine fixes are
validated (see [Session settings](reads.md#session-settings)). Drivers that
set `TimeZone` to something other than UTC, `client_encoding` to something
other than UTF-8, or `DateStyle`, `IntervalStyle`, `bytea_output` or
`standard_conforming_strings` to a value other than the one in effect, get
an error.

## A first session

The following example assumes a PostgreSQL 14+ server with
`wal_level = logical` (see [Sources](sources.md#upstream-requirements)).
Upstream:

```sql
-- in PostgreSQL
CREATE TABLE orders (id int PRIMARY KEY, customer int, amount numeric(10,2), created timestamptz);
CREATE TABLE customers (id int PRIMARY KEY, region text);
CREATE PUBLICATION app_pub FOR TABLE orders, customers;
```

In `ivm`:

```sql
-- replicate the publication's tables
CREATE SOURCE pg FROM POSTGRES
  CONNECTION 'host=db.example.com port=5432 dbname=app user=replicator password=...'
  PUBLICATION 'app_pub';

SHOW SOURCES;   -- slot ivm_pg_<instance>_<id>, status "snapshotting public.orders", then "streaming"
SHOW TABLES;    -- orders, customers (source pg)

-- a maintained view
CREATE MATERIALIZED VIEW revenue AS
  SELECT c.region, count(*) AS n, sum(o.amount) AS total
  FROM orders o JOIN customers c ON c.id = o.customer
  GROUP BY c.region;

-- reads wait until the initial snapshot is complete and the log is applied
SELECT * FROM revenue ORDER BY region;

-- read your own upstream writes: wait until the source has passed an LSN
SELECT * FROM revenue AS OF AT LEAST LSN '0/16B3748';

-- stream changes (in psql use the COPY form; see the Reads chapter)
COPY (SUBSCRIBE TO revenue WITH (PROGRESS)) TO STDOUT;

-- where does the work go?
EXPLAIN MAINTENANCE revenue;
EXPLAIN DATAFLOW revenue;
```

Local tables work without any upstream:

```sql
CREATE TABLE notes (id int PRIMARY KEY, body text);
INSERT INTO notes VALUES (1, 'hello');
CREATE MATERIALIZED VIEW note_count AS SELECT count(*) FROM notes;
```

Next steps:

- [Sources](sources.md) — what the upstream must provide and how replication
  behaves.
- [Tables, views and indexes](tables-and-views.md).
- [Reads and subscriptions](reads.md).
- [SQL reference](../sql/statements.md).
