# Reads and subscriptions

Every read sees the engine's state as of one GSN — the global sequence number
the engine assigns to every logged input (source transaction, local DML
statement, DDL statement, clock tick). This chapter describes how a read
chooses its GSN, the session settings, and `SUBSCRIBE`. Code:
`crates/ivm/src/coord/read.rs`, `crates/ivm/src/server/mod.rs`,
`crates/ivm/src/exec.rs`.

## Read points

| Form | Reads the state at | Waits for |
|---|---|---|
| `SELECT ...` (session `read_mode = fresh`, the default) | the last durable GSN when the query arrives | the engine to apply that GSN; the snapshots of the replicated tables read |
| `SELECT ... AS OF FRESH` | same, regardless of `read_mode` | same |
| `SELECT ...` with `read_mode = latest`, or `SELECT ... AS OF LATEST` | the last applied GSN | nothing |
| `SELECT ... AS OF <gsn>` | `<gsn>` | the engine to apply `<gsn>` if it is in the future |
| `SELECT ... AS OF AT LEAST LSN '<X/Y>'` | the last durable GSN once every source's frontier has reached the LSN | every source frontier ≥ LSN; the snapshots of the replicated tables read |

All relations in one query are read at the same GSN, so a query never sees a
half-applied upstream transaction or a mix of two points in time.

### Syntax

`AS OF` is an engine extension and must be the **last** clause of the
statement — after `ORDER BY`, `LIMIT` and `OFFSET`:

```sql
SELECT * FROM revenue ORDER BY region AS OF 123456;
SELECT * FROM orders WHERE id = 7 AS OF AT LEAST LSN '0/16B3748';
SELECT count(*) FROM orders AS OF LATEST;
```

It is recognised as the last `AS OF` (case-insensitive, the two words
separated by whitespace, with whitespace before and after) at the top level
of the statement: outside parentheses, string literals, quoted identifiers,
comments and dollar-quoted strings. Anything after the clause is a syntax
error (`unexpected tokens after AS OF clause`). Sub-selects cannot have
their own `AS OF`; `FOR SYSTEM_TIME`-style table versions are rejected
(`table versions are not supported; use AS OF on the query`).

### Fresh reads (default)

A fresh read takes the GSN of the last input the sequencer has made durable
when the query arrives, and waits until the engine thread has applied it.
This includes every local DML statement whose reply the client has already
received, and every upstream transaction that has been logged. Fresh reads
also wait while a replicated table that the query reads — directly or through
views — is incomplete: a partially loaded table is no state the upstream ever
had. A table is incomplete during its initial snapshot, from an upstream
schema or key change until its re-snapshot has finished
([Sources](sources.md#upstream-schema-changes)), and after a lost
replication slot until it has been copied again ([Slots](sources.md#slots)).
The wait is per table: a table that cannot be copied (for example for lack
of `SELECT` upstream) holds back the fresh reads of that table only.

### Latest reads

`SET read_mode = latest` (or `AS OF LATEST`) reads whatever is applied right
now, without waiting. Use it when staleness of a few milliseconds does not
matter, or to inspect tables while a snapshot is loading.

### Time travel: AS OF gsn

`AS OF <gsn>` reads the state at a past GSN from the multi-version histories
of the indexes. The history kept is controlled by `--retention-gsns`
(default 1000): reads before the *retention frontier* (last applied GSN minus
the retention) fail with SQLSTATE `55000`:

```
ERROR:  AS OF 2 is before the retention frontier 3
```

A GSN in the future makes the read wait until the engine reaches it (the
wait can be cancelled, see below).
`SHOW GSN` returns the last applied GSN, and `SHOW FRONTIERS`
the retention frontier. Note that every clock tick (by default one per
second) consumes a GSN, so 1000 GSNs are at most about 17 minutes on an idle
engine.

Time-travel reads use the **current** catalog: a virtual view is expanded
with its current definition over the historical table contents.

An index keeps history only from the GSN at which it was built, and an
`AS OF` read uses only indexes whose history covers its GSN. A secondary
index created after that GSN is simply not used. When a relation the read
needs has no such index at all, the read fails with SQLSTATE `55000`:

```
ERROR:  AS OF 133 is before the history of a relation it reads, which starts at 134
```

This happens for a relation created after the GSN, and for one whose index
was rebuilt since: a materialized view replaced with `CREATE OR REPLACE` or
re-planned after a change to what it reads, a replicated table after an
upstream schema change, and a materialized view whose dataflow a restart
rebuilt (recovery plans every view again; when the plan comes out
different, for example because join orders follow the current table sizes,
the view's indexes are hydrated afresh; see
[Known problems](troubleshooting.md#known-problems)).

Error records are not versioned: a read of a materialized view as of any GSN
fails if the view has error records *now*, and succeeds if it has none now,
whatever its state at that GSN.

### Read-your-writes against PostgreSQL: AS OF AT LEAST LSN

After committing a transaction upstream, a client can read its effects by
passing the commit's LSN (for example from `pg_current_wal_lsn()` or
`pg_current_wal_insert_lsn()` after the commit):

```sql
SELECT * FROM revenue AS OF AT LEAST LSN '0/16B3748';
```

The read waits until the LSN frontier of **every** source has reached the
LSN — the frontier of a source is the position up to which every upstream
commit has been logged — and then performs a fresh read. While reads are
waiting, sources ask the upstream for a keepalive so that the frontier
advances even when no data flows. Because all sources are compared, this
form is meant for sources that replicate from the same PostgreSQL cluster.

A source's frontier does not advance while any of its tables is incomplete
(being copied, or failing to be copied; see above), nor while the source is
stuck in an error; a source that has not reported a frontier yet holds
reads back too. A dropped source no longer holds reads back. If the engine
has **no sources at all**, the read fails at once with SQLSTATE `55000`
(`AS OF AT LEAST LSN '...' cannot be satisfied: there is no source`), also a
waiting read when the last source is dropped: the LSN is a position in an
upstream's WAL, so a read without sources could not show the commit it
waits for.

A read that waits — for freshness, an LSN, a future GSN or an initial
snapshot — ends with SQLSTATE `57014` (`canceling statement due to user
request`) when the client cancels it (a `CancelRequest`, Ctrl-C in psql);
DML queued behind another statement is cancelled the same way. The engine
also drops a waiting read when its client disconnects (the session checks
the connection every 200 ms while it waits), and the web UI console drops
its read when it stops waiting (after 30 s).

### now() in reads

`now()`, `current_timestamp`, `current_date`, `localtimestamp`,
`transaction_timestamp()` and `statement_timestamp()` return the engine's
**logical clock as of the read's GSN**: the value of the latest logged clock
tick at or before that GSN. Reads are therefore repeatable — the same query
`AS OF g` returns the same result every time, also after a restart
(checkpoints keep the clock ticks). `clock_timestamp()` is the real wall
clock. See [Temporal filters](temporal-filters-and-errors.md#the-logical-clock).

### Views with errors

A read that references a materialized view that currently has
[error records](temporal-filters-and-errors.md#errors-as-data) fails with
that error (the error's SQLSTATE and message), regardless of the read point.
A read that references a [broken](operations.md#recovery) materialized view
(directly or through views) fails with SQLSTATE `55000`
(`materialized view "v" is broken: <reason>`).

### How reads are executed

Ad hoc queries do not create dataflow: they are planned like views and then
evaluated once by a batch executor over the indexes (key lookups and
ranges, index or hash joins, hash aggregation, sorting), at the chosen GSN.
Reading a materialized view is a scan of its index (or a key range of it,
see [Indexes](tables-and-views.md#indexes)); reading a virtual view
evaluates its query. Expression errors make the query fail, as in
PostgreSQL.

Rows are produced as the indexes are scanned and sent to the client as
they come, in chunks of 1,024 rows (or about 256 KB):

- without `ORDER BY`, a read stops scanning once it has `OFFSET` + `LIMIT`
  rows (`SELECT * FROM t LIMIT 1` reads one entry), and its rows come in
  the order of the index it scans, which is unspecified;
- with an `ORDER BY` that is the order of an index of the relation read —
  its leading key columns in the same direction and `NULLS` placement,
  possibly after columns fixed by equalities, as in
  `WHERE region = 'eu' ORDER BY id` with an index on `(region, id)` — the
  index is scanned in order and the read stops at its `LIMIT` too; this
  works through computed result columns and filters, not through joins or
  aggregates;
- any other `ORDER BY` with `LIMIT` keeps only the rows that can still be
  among the first `OFFSET` + `LIMIT` (and their ties with `WITH TIES`)
  while it reads; `ORDER BY` without `LIMIT` sorts the whole result;
- aggregates consume their input as it is read; the build side of a hash
  join, `DISTINCT`, set operations, window functions and `WITH RECURSIVE`
  hold their inputs.

Rows tied in the `ORDER BY` can come in any order, and which of them a
`LIMIT` keeps depends on the index read. An error in the middle of a large
result (a division by zero in row 20,000, say) ends it after the rows
already sent, as in PostgreSQL; a result of one chunk fails before any row.
A client that disconnects ends the read at its next chunk, and a cancel
ends it while it waits for the client. A small read (one that visits up to
50,000 index entries and returns up to 50,000 rows) runs on the engine
thread and hands its rows over at once; a larger one runs on one of four
reader threads ([Planner](../internals/planner.md#batch-execution)), which
reads ahead of the client by at most two chunks: a client that reads
slowly does not make the server's memory grow, but holds a reader thread
(four large reads whose clients do not read make other large reads wait).

## Transactions

There are no multi-statement transactions. `BEGIN` / `START TRANSACTION`,
`COMMIT`, `ROLLBACK` and `SET TRANSACTION ...` are accepted so that drivers
work; they only change the transaction status reported to the client
(`T` between `BEGIN` and `COMMIT`/`ROLLBACK`). Each statement runs on its
own: DML is applied immediately and **is not undone by `ROLLBACK`**, and each
read chooses its own GSN. `SHOW transaction_isolation` reports
`serializable`, because every single read is a consistent snapshot.

## Session settings

`SET name = value` (or `SET name TO value`) is handled by the session:

| Setting | Values | Effect |
|---|---|---|
| `read_mode` (alias `ivm.read_mode`) | `fresh` (default), `latest` | default read point of `SELECT` (see above); other values fail with SQLSTATE `22023` |
| `TimeZone` / `SET TIME ZONE` | `UTC`, `GMT`, `Z`, `+00`, `0`, `Etc/UTC`, `DEFAULT`, `LOCAL` (case-insensitive) | accepted; the session time zone is always UTC |
| `client_encoding`, `SET NAMES` | `UTF8`, `UTF-8`, `UNICODE`, `DEFAULT` | accepted; the encoding is always UTF-8 |
| `DateStyle` | `ISO`, `MDY` (or `US`, `NonEuropean`), `DEFAULT`, and combinations such as `ISO, MDY` | accepted; output is always `ISO, MDY` and input is read month first |
| `IntervalStyle` | `postgres`, `DEFAULT` | accepted |
| `bytea_output` | `hex`, `DEFAULT` | accepted |
| `standard_conforming_strings` | `on` (`true`, `yes`, `1`, `DEFAULT`) | accepted; backslashes in string literals are never escapes |
| `application_name` | any | stored and reported back with a `ParameterStatus` message |
| anything else | any | stored for `SHOW`, no effect |

For the fixed settings, any other value (another time zone or encoding,
`DateStyle = 'ISO, DMY'` or `German`, `IntervalStyle = iso_8601`,
`bytea_output = escape`, `standard_conforming_strings = off`) fails with
SQLSTATE `0A000` (`... is not supported`) rather than being accepted without
effect. `RESET name` (and `SET name TO DEFAULT`) restores the value the
session started with — the `application_name` of the startup message,
`read_mode = fresh`, the values `SHOW` reports below — or removes the
session's value; `RESET ALL` does so for every setting. `SET TRANSACTION
...`, `DISCARD` and `DEALLOCATE` are accepted and change nothing.

`SHOW name` returns the value set in the session if there is one; otherwise
the session or the engine answers for:

| Name | Value |
|---|---|
| `server_version` | `17.0` |
| `server_version_num` | `170000` |
| `server_encoding`, `client_encoding` | `UTF8` |
| `TimeZone` (`time zone`) | `UTC` |
| `DateStyle` | `ISO, MDY` |
| `IntervalStyle` | `postgres` |
| `transaction_isolation` (`transaction isolation level`), `default_transaction_isolation` | `serializable` |
| `transaction_read_only` | `off` |
| `standard_conforming_strings`, `integer_datetimes`, `is_superuser` | `on` |
| `search_path` | `"$user", public` |
| `extra_float_digits` | `1` |
| `bytea_output` | `hex` |
| `max_identifier_length` | `63` |
| `application_name` | the name the client sent at startup |
| `read_mode`, `ivm.read_mode` | the session's read mode |
| `gsn` | the last applied GSN |

Any other name fails with `unrecognized configuration parameter` (SQLSTATE
`42704`). `SHOW ALL` lists only `server_version`, `timezone` and
`datestyle`. `current_setting(name [, missing_ok])` returns the session's
value — any name set with `SET` (custom names such as `myapp.user_id`
included), `read_mode` and the other values the session starts with — and
otherwise the server's fixed settings above; a virtual view calling it sees
the settings of the session that reads it. `set_config(name, value,
is_local)` returns `value` without changing or checking anything.

## SUBSCRIBE

`SUBSCRIBE` streams the changes of a relation or query as they are applied.

```sql
SUBSCRIBE [ TO ] name    [ WITH ( option [ , ... ] ) ] [ AS OF gsn ]
SUBSCRIBE [ TO ] ( query ) [ WITH ( option [ , ... ] ) ] [ AS OF gsn ]
COPY ( SUBSCRIBE ... ) TO STDOUT

option:
    SNAPSHOT [ [ = ] value ]   -- default true
  | PROGRESS [ [ = ] value ]   -- default false
```

An option without a value means `true`. After `=`, the values `true`, `on`
and `1` mean true and anything else false; without `=`, only the words
`true` and `false` are recognised as a value.

- `name` may be a materialized view, a table, or a virtual view (which is
  subscribed as the query `SELECT * FROM name`, so the columns are named as
  in reads of the view, by its column list if it has one).
- `( query )` may be any query that a materialized view could have (the same
  [determinism rules](../sql/maintained-views.md) apply). A private dataflow
  is built for it — sharing every operator and index that existing views
  already have — hydrated, and released when the subscription ends.
- A [broken](operations.md#recovery) materialized view, or a query reading
  one, cannot be subscribed to (SQLSTATE `55000`).
- `AS OF gsn` is accepted but ignored: a subscription always starts at the
  last applied GSN.

### Output

The result columns are:

| Column | Type | Meaning |
|---|---|---|
| `gsn` | `bigint` | the GSN of the change (or of the progress mark) |
| `progressed` | `boolean` | only with `PROGRESS`: `t` on progress rows, `f` on data rows |
| `diff` | `bigint` | change in multiplicity: `+n` rows were added, `-n` removed; NULL on progress rows |
| *the relation's columns* | | the row; all NULL on progress rows |

The stream consists of:

1. with `SNAPSHOT` (default): the current contents, as rows with
   `gsn` = the last applied GSN and positive `diff` (the multiplicity),
   read from the relation's index in chunks of 10,000 rows as the session
   writes them out (every 1 MiB) — a large snapshot on a reader thread,
   which reads ahead of the client by two chunks, so that a snapshot of any
   size takes bounded memory; the changes after it are queued meanwhile,
   within the subscription's limit;
2. a progress row at that GSN (only with `PROGRESS`);
3. for every later GSN at which the relation changes: its changes,
   consolidated — no row appears with both signs within a GSN, so an update
   of a row is `-1` for the old version and `+1` for the new one, and a
   change that cancels within the GSN does not appear at all. The order of
   rows within a GSN is unspecified;
4. with `PROGRESS`: after every applied commit or clock tick, a progress row
   with that GSN, meaning "all changes up to and including this GSN have been
   delivered". DDL and source bookkeeping records do not produce progress
   rows, and with `--tick-ms 0` an idle engine produces none.

Example (`COPY` form, `PROGRESS`):

```
112	f	1	eu	2	16.75
112	f	1	us	2	120.00
112	t	\N	\N	\N	\N
113	t	\N	\N	\N	\N
114	f	-1	eu	2	16.75
114	f	1	eu	3	23.75
114	t	\N	\N	\N	\N
```

Values are sent in text format (the PostgreSQL output format of each
type) unless the client asks for binary results: with the extended
protocol, the rows follow the result formats of the `Bind` message (one
for every column, or one per column, `gsn` and `diff` counting as `bigint`
and `progressed` as `boolean`), as the portal's row description announces
them ([binary format](../sql/types.md#binary-format)).

### COPY form

`COPY (SUBSCRIBE ...) TO STDOUT` sends the same rows inside a `CopyOut`
response in text format: one line per row, columns separated by a tab, NULL
as `\N`, and backslash, tab, newline and carriage return escaped as `\\`,
`\t`, `\n`, `\r`. Anything after `TO STDOUT` is ignored; other `COPY` forms
are not supported.

Use the `COPY` form from `psql`: psql collects the rows of an ordinary query
until it completes, so a plain `SUBSCRIBE` shows nothing, while `COPY` output
is printed as it arrives. Programmatic clients can use either form, with the
simple or the extended protocol: `Describe` of a `SUBSCRIBE` reports the
streamed columns (`gsn`, `progressed` with `PROGRESS`, `diff`, then the
relation's), and `Execute` of `COPY (SUBSCRIBE ...) TO STDOUT` streams in
`COPY` format.

### Ending a subscription

A subscription runs until

- the client cancels the query (for example Ctrl-C in psql), which ends it
  with `canceling statement due to user request` (SQLSTATE `57014`);
- the client disconnects (the session checks the connection every 200 ms);
- a relation it reads — the subscribed table, view or materialized view,
  a relation a subscribed view or query reads, directly or through views —
  is dropped, or replaced by another kind of view (`table "x" was dropped`,
  `view "x" was dropped`, `materialized view "x" was dropped`, SQLSTATE
  `42P01`);
- the subscribed materialized view gets other columns (names, types or type
  modifiers), by `CREATE OR REPLACE` or when an upstream schema change
  re-plans it: `materialized view "x" changed its columns` (SQLSTATE
  `55000`);
- a replicated table it reads changes its columns upstream — the
  subscribed table, or a table that a subscribed virtual view or query
  reads, directly or through views, materialized ones included (a
  subscription to a materialized view itself follows its re-planning
  instead, see below): `table "x" changed its columns upstream` (SQLSTATE
  `55000`);
- a materialized view that its query reads (directly or through other
  views) is replaced with `CREATE OR REPLACE` or re-planned:
  `materialized view "x" was replaced` (SQLSTATE `55000`);
- the subscribed relation or query gets a new error record: the
  subscription ends with the error (the retraction of an error record does
  not end it). The initial snapshot of a subscription leaves rows that fail
  out instead of failing;
- its client does not read changes as fast as they come and their queue
  reaches its memory limit (SQLSTATE `53000`, see below);
- the engine shuts down.

If the subscribed materialized view is replaced with `CREATE OR REPLACE` by
a materialized view with the same columns, or re-planned after an upstream
schema change without changing its columns, the subscription continues: it
receives the difference between the old and the new contents at the
replacement's GSN. A subscription to a query has a dataflow of its own,
built over the current versions of the materialized views it reads, so it
ends instead (see above); subscribe again after such changes.

Changes are queued for each subscription in memory, up to a sixteenth of
the memory budget (`--memory-mb`, at least 4 MiB) per subscription. A client
that reads more slowly than changes arrive, or stops reading while keeping
its connection, fills its queue; the subscription then ends with an error
(SQLSTATE `53000`, `subscription ended: its client does not read changes as
fast as they come ...`) and its queued changes are dropped. The client
receives what was already written to its connection, then the error.
While the server waits for such a client to take what it wrote, a cancel
request ends the subscription as well (`57014`, reported when the client
reads again), and a client that closed its connection ends the session.
