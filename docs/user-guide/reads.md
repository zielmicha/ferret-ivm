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
| `SELECT ...` (session `read_mode = fresh`, the default) | the last durable GSN when the query arrives | the engine to apply that GSN; initial snapshots of the tables read |
| `SELECT ... AS OF FRESH` | same, regardless of `read_mode` | same |
| `SELECT ...` with `read_mode = latest`, or `SELECT ... AS OF LATEST` | the last applied GSN | nothing |
| `SELECT ... AS OF <gsn>` | `<gsn>` | the engine to apply `<gsn>` if it is in the future |
| `SELECT ... AS OF AT LEAST LSN '<X/Y>'` | the last durable GSN once every source's frontier has reached the LSN | every source frontier ≥ LSN; initial snapshots of the tables read |

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

It is recognised as the last occurrence of ` as of ` (case-insensitive,
surrounded by spaces) outside parentheses and quotes; anything after the
clause is a syntax error (`unexpected tokens after AS OF clause`).
Sub-selects cannot have their own `AS OF`; `FOR SYSTEM_TIME`-style table
versions are rejected.

### Fresh reads (default)

A fresh read takes the GSN of the last input the sequencer has made durable
when the query arrives, and waits until the engine thread has applied it.
This includes every local DML statement whose reply the client has already
received, and every upstream transaction that has been logged. Fresh reads
also wait while a replicated table that the query reads — directly or through
views — is still loading its initial snapshot: a partially loaded table is no
state the upstream ever had. A re-snapshot after an upstream schema change is
only waited for once its first chunk has been logged; until then fresh reads
return the table at once, with NULL in the new columns
([Sources](sources.md#upstream-schema-changes)).

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

A GSN in the future makes the read wait until the engine reaches it (such a
wait cannot be cancelled).
`SHOW GSN` returns the last applied GSN, and `SHOW FRONTIERS`
the retention frontier. Note that every clock tick (by default one per
second) consumes a GSN, so 1000 GSNs are at most about 17 minutes on an idle
engine.

Time-travel reads use the **current** catalog: a relation created after the
GSN reads as empty (its index has no versions before its creation), and a
virtual view is expanded with its current definition over the historical
table contents.

More generally, an index only has versions from the GSN at which it was
built, and reads as of an earlier GSN silently return too few rows or none
(a known problem):

- a materialized view read as of a GSN before its current index was built —
  before it was last replaced with `CREATE OR REPLACE`, or before a restart
  that rebuilt the index because its structure no longer matched — returns
  nothing;
- point lookups (`WHERE column = constant`) use any index on the relation,
  including a secondary index created after the read GSN: with an index on
  `t(c)` created at GSN 95, `SELECT * FROM t WHERE c = 10 AS OF 92` returns
  nothing, while a read of `t` without the condition (a scan of the primary
  index) returns the rows.

Error records are not versioned either: a read of a materialized view as of
any GSN fails if the view has error records *now*, and succeeds if it has
none now, whatever its state at that GSN.

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

A source's frontier does not advance while any of its tables is loading an
initial snapshot, nor while the source is stuck in an error; a source that
has not reported a frontier yet holds reads back too. A dropped source no
longer holds reads back. If the engine has **no sources at all**, the read
fails at once with SQLSTATE `55000` (`AS OF AT LEAST LSN '...' cannot be
satisfied: there is no source`), also a waiting read when the last source
is dropped: the LSN is a position in an upstream's WAL, so a read without
sources could not show the commit it waits for. Waiting reads
cannot be cancelled (only `SUBSCRIBE` can); a client that disconnects is
freed, but the read stays parked in the engine until its condition holds.

### now() in reads

`now()`, `current_timestamp`, `current_date`, `localtimestamp`,
`transaction_timestamp()` and `statement_timestamp()` return the engine's
**logical clock as of the read's GSN**: the value of the latest logged clock
tick at or before that GSN. Reads are therefore repeatable — the same query
`AS OF g` returns the same result every time. `clock_timestamp()` is the
real wall clock. See [Temporal filters](temporal-filters-and-errors.md).

### Views with errors

A read that references a materialized view that currently has
[error records](temporal-filters-and-errors.md#errors-as-data) fails with
that error (the error's SQLSTATE and message), regardless of the read point.

### How reads are executed

Ad hoc queries do not create dataflow: they are planned like views and then
evaluated once by a batch executor over the indexes (key lookups and
ranges, index or hash joins, hash aggregation, sorting), at the chosen GSN. Reading a materialized view is a
scan of its index (or a key range of it, see
[Indexes](tables-and-views.md#indexes)); reading a virtual view evaluates its
query. Expression errors make the query fail, as in PostgreSQL.

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
| `TimeZone` / `SET TIME ZONE` | `UTC`, `GMT`, `Z`, `+00`, `0`, `Etc/UTC`, `DEFAULT`, `LOCAL` (case-insensitive) | accepted; anything else fails: the session time zone is always UTC |
| `client_encoding`, `SET NAMES` | `UTF8`, `UTF-8`, `UNICODE`, `DEFAULT` | anything else fails |
| `DateStyle` | any value containing `ISO` | output is always `ISO, MDY`; other values fail |
| `application_name` | any | stored and reported back with a `ParameterStatus` message |
| anything else | any | stored for `SHOW`, no effect |

`SHOW name` returns the value set in the session if there is one; otherwise
the engine answers for:

| Name | Value |
|---|---|
| `server_version` | `17.0` |
| `server_encoding`, `client_encoding` | `UTF8` |
| `TimeZone` (`time zone`) | `UTC` |
| `DateStyle` | `ISO, MDY` |
| `IntervalStyle` | `postgres` |
| `transaction_isolation` (`transaction isolation level`) | `serializable` |
| `standard_conforming_strings` | `on` |
| `gsn` | the last applied GSN |

Any other name fails with `unrecognized configuration parameter` (SQLSTATE
`42704`) — including `read_mode` until it has been `SET` in the session.
`SHOW ALL` lists `server_version`, `timezone` and `datestyle`. `DISCARD` and
`DEALLOCATE` are accepted and do nothing.

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
  subscribed as its query; a column list given in `CREATE VIEW` is not
  applied, so the columns have the query's names).
- `( query )` may be any query that a materialized view could have (the same
  [determinism rules](../sql/maintained-views.md) apply). A private dataflow
  is built for it — sharing every operator and index that existing views
  already have — hydrated, and released when the subscription ends.
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
   `gsn` = the last applied GSN and positive `diff` (the multiplicity);
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

Values are always sent in text format (the PostgreSQL output format of each
type), even when a client asks for binary results.

### COPY form

`COPY (SUBSCRIBE ...) TO STDOUT` sends the same rows inside a `CopyOut`
response in text format: one line per row, columns separated by a tab, NULL
as `\N`, and backslash, tab, newline and carriage return escaped as `\\`,
`\t`, `\n`, `\r`. Anything after `TO STDOUT` is ignored; other `COPY` forms
are not supported.

Use the `COPY` form from `psql`: psql collects the rows of an ordinary query
until it completes, so a plain `SUBSCRIBE` shows nothing, while `COPY` output
is printed as it arrives. Programmatic clients can use either form; with the
extended protocol, `Describe` reports no columns for `SUBSCRIBE`, so the
simple query protocol (or `COPY`) is preferable.

### Ending a subscription

A subscription runs until

- the client cancels the query (for example Ctrl-C in psql), which ends it
  with `canceling statement due to user request` (SQLSTATE `57014`);
- the client disconnects — noticed when the server next sends something
  (a change, or a progress row with `PROGRESS`); until then the
  subscription, and the private dataflow of a query subscription, stay
  alive;
- the subscribed materialized view is dropped
  (`materialized view "x" was dropped`); a subscription to a dropped *table*
  does not end, and keeps sending progress rows;
- the subscribed relation's error records change: the subscription ends with
  the error — also when an error record is *retracted*, that is when the
  relation has just become readable again (a known problem). The initial
  snapshot of a subscription leaves rows that fail out instead of failing;
- the engine shuts down.

If the subscribed materialized view is replaced with `CREATE OR REPLACE` by
another materialized view, the subscription continues: it receives the
difference between the old and the new contents at the replacement's GSN.
Replacing it with a virtual view ends the subscription
(`materialized view "x" was dropped`).

Changes are queued for each subscription without a bound; a client that
reads more slowly than changes arrive makes the server's memory grow.
