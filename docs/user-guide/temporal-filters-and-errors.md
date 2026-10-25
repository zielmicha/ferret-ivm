# Temporal filters and errors as data

Two features make maintained views behave like a query that is re-run
continuously: conditions over `now()` that are maintained as time passes,
and expression errors that are stored as data instead of aborting
maintenance. Code: `crates/ivm/src/dataflow/temporal.rs`,
`crates/ivm/src/coord/seq.rs` (ticks), `crates/ivm/src/coord/ddl.rs`
(`check_deterministic`), `crates/ivm/src/dataflow/mod.rs` (error records).

## The logical clock

The state of a maintained view must be a pure function of the input log, so
the wall clock is an input like any other: the sequencer writes a *tick*
record holding the current wall-clock time (microseconds since the Unix
epoch) to the input log every `--tick-ms` milliseconds (default 1000), in
the same batch as whatever else is being logged at that moment. Each tick
gets a GSN.

- The engine's **logical clock** is the time of the latest applied tick. It
  never goes backwards: a tick older than the current clock (wall clock
  stepped back, or a restart) leaves it unchanged.
- The clock is saved with every checkpoint and rebuilt from the log on
  recovery, so it survives restarts.
- With `--tick-ms 0` no ticks are written: the clock stays at the last logged
  tick, which is 2000-01-01 00:00:00+00 on a data directory that never had
  one.
- Ticks keep their schedule under sustained input. They pause only while
  the sequencer holds back all input because the engine is 10,000 or more
  records behind ([Operations](operations.md#configuration-and-tuning)); the
  next tick then carries the current time.

Where the clock is used:

| Context | `now()` (and `current_timestamp`, `transaction_timestamp()`, `statement_timestamp()`, `localtimestamp`, `current_date`) |
|---|---|
| materialized views, `SUBSCRIBE (query)` | only in temporal filters (below), evaluated against the logical clock |
| `SELECT` (ad hoc, virtual views) | the logical clock as of the read's GSN — repeatable for a given GSN, also across restarts (the checkpoint keeps the last 100,000 clock ticks of the retention window; a read as of an older GSN after a restart sees the oldest kept tick's clock) |
| `INSERT`/`UPDATE`/`DELETE` | the wall clock when the statement runs (the resulting values are logged) |

`clock_timestamp()` always returns the wall clock and is rejected in
materialized views.

## Temporal filters

A *temporal filter* is a condition that compares a row with the clock, such
as "created within the last hour" or "due". In a materialized view it is
maintained: rows enter the view when the condition becomes true and leave it
when it becomes false, as ticks arrive.

```sql
CREATE MATERIALIZED VIEW recent AS
  SELECT * FROM orders WHERE created > now() - interval '1 hour';

CREATE MATERIALIZED VIEW due AS
  SELECT * FROM reminders WHERE remind_at <= now();

CREATE MATERIALIZED VIEW today AS
  SELECT id FROM events WHERE ts::date = current_date;

CREATE MATERIALIZED VIEW live_sessions AS
  SELECT id FROM sessions
  WHERE started <= now() AND started + ttl * interval '1 second' > now();

CREATE MATERIALIZED VIEW recent_counts AS
  SELECT k, count(*) FROM ev
  WHERE ts BETWEEN now() - interval '1 minute' AND now()
  GROUP BY k;
```

### Accepted forms

Each conjunct of a filter (a `WHERE` or `HAVING` condition, or a join
condition that the optimizer turns into a filter) that mentions the clock
must have the form

```
clock_expression  op  row_expression
row_expression    op  clock_expression
```

where `op` is `<`, `<=`, `>`, `>=` or `=` (equality holds during an
interval), and

- `clock_expression` is a non-decreasing function of the clock alone:
  `now()`, `current_timestamp`, `transaction_timestamp()`,
  `statement_timestamp()`, `localtimestamp` or `current_date`, optionally
  cast to `timestamp`, `timestamptz` or `date`, plus or minus a constant
  (`clock - c`, `clock + c`, `c + clock`), or `date_trunc(c, clock)`, nested
  arbitrarily. A *constant* uses no columns and only immutable functions
  (`interval '1 hour'`, `2 * interval '1 day'`);
- `row_expression` uses no clock function (it may be constant). It may use
  any deterministic expression of the row.

`BETWEEN now() - x AND now()` works because it is two such conjuncts.
Inner-join `ON` conditions on one input are accepted when the optimizer
pushes them into a filter on that input, and so are conditions in the `ON`
clause of a `LEFT`/`RIGHT JOIN` that only use the nullable input.

Rejected, with
`now() is only supported in temporal filters of maintained views`
(SQLSTATE `0A000`):

- the clock in the select list, `GROUP BY`, `ORDER BY`, a window, a
  function argument other than the forms above, or an outer-join condition
  that cannot be pushed into the nullable input (a condition on the
  preserved side, or any condition of a `FULL JOIN`);
- conditions combining the clock with `OR`, `NOT` or `<>`;
- clock expressions that are not monotone (`extract(hour from now()) = 3`)
  or that mix the clock with columns (`now() - ts < interval '1 minute'`; write
  `ts > now() - interval '1 minute'` instead).

Virtual views and ad hoc queries may use `now()` anywhere.

### How temporal filters are maintained

For each row, the operator computes the interval of logical time
`[lo, hi)` during which every temporal conjunct holds, by bisecting the
condition over clock values between 1900-01-01 and 2200-01-01: a row whose
validity starts before 1900 is valid from the beginning, and one whose
validity ends after 2200 never expires, but a row that would only become
valid after 2200, or that expired before 1900, is never valid. Because the clock side is
non-decreasing, each conjunct changes value at most once, and bisection keeps
calendar arithmetic (months, time zones, `date_trunc`) exact.

- A row valid now is emitted at once.
- A row that becomes valid in the future, or expires, is scheduled: the
  operator stores `(time, kind, row)` events in an index ordered by time.
- At every step in which the clock has moved, the events that are due are
  released: rows enter (`kind 0`) or leave (`kind 1`) the output.
- Changes to the input are filtered at the current clock: a deleted row
  retracts both its output and its pending events.

Pending events are part of the checkpointed state, so expirations are not
lost across restarts. `EXPLAIN DATAFLOW` shows the operator as
`TemporalFilter`. An error while evaluating the row side becomes an
[error record](#errors-as-data).

## Errors as data

In PostgreSQL, a query that hits a division by zero fails. A maintained view
cannot "fail" — it keeps being maintained — so an expression error on some
row becomes an **error record**: the pair (SQLSTATE, message), with the
row's multiplicity, stored alongside the view's rows instead of an output
row.

```sql
CREATE TABLE t (id int PRIMARY KEY, x int);
INSERT INTO t VALUES (1, 1), (2, 2);
CREATE MATERIALIZED VIEW e AS SELECT id, 10 / (x - 1) AS q FROM t;

SELECT * FROM e;          -- ERROR:  division by zero
SHOW VIEWS;               -- e | materialized | 1 | 1 | ok   (one error record)
DELETE FROM t WHERE id = 1;
SELECT * FROM e;          -- 2 | 10
```

Rules:

- Error records flow through every downstream operator and are counted per
  view. When the offending input row is deleted or updated, its error record
  is retracted.
- Every operator keeps the error records currently in its output, so a view
  created over existing operators — an identical query, a view over another
  materialized view, any shared sub-plan — starts with the error records
  already there, and errors below an index the plan reads count like any
  other.
- While a materialized view has an error record with a positive count, every
  read that references the view fails with that error (the smallest by
  SQLSTATE and message, if there are several), and `SHOW VIEWS` shows the
  number of distinct errors. A `SUBSCRIBE` ends with an error as soon as a
  change of the subscribed relation adds an error record (a retraction does
  not end it); a subscription started while the view already has errors is
  not affected until then.
- Grouped aggregates: if an aggregate of a group fails (for example
  `sum(float8)` overflowing to `value out of range: overflow`, or a `numeric`
  sum or average beyond `numeric`'s range: `value overflows numeric format`),
  the group's row is removed and one error record is kept for the group
  until the group computes again. This also holds for an aggregate without
  `GROUP BY`. (The sums themselves are kept exactly, so maintaining them
  never fails.)
- Window functions: if computing a partition fails (for example
  `ntile(0)`), the partition contributes an error record instead of rows.
- `WITH RECURSIVE`: an error while evaluating the query (including
  `recursive query exceeded the iteration limit`) retracts the view's rows
  and leaves one error record until the query evaluates without error.
- A scalar subquery that returns more than one row raises its error only
  where its value is used, as in PostgreSQL: a `CASE` branch that is not
  taken does not raise it. In a maintained view the error record exists
  while some row actually uses such a value.
- Errors are part of the checkpointed state — the views' error records, the
  operators', and which aggregate groups and recursive queries are in error
  — so they are retracted correctly after a restart. They are not delivered
  to sinks.
- Ad hoc queries do not use error records: they fail like in PostgreSQL.

In aggregates, temporal filters and recursive queries, errors that are not
caused by data — internal errors (`XX...` SQLSTATEs) and I/O errors
(`58...`) — are not turned into records. Such an error while applying a log
record stops the engine thread, and the process exits with status 1; the
next start replays the record. Any other error that escapes the dataflow (a
bug) is logged, the record is not applied again (it may be half-applied),
and the statement that wrote it fails; should the engine stop before its
next checkpoint, the replay of that record fails again and stops the start.

Known deviations (see [Troubleshooting](troubleshooting.md#known-problems)):

- Errors in **join conditions** are treated as "no match": the joined row is
  silently missing and no error record is created, where PostgreSQL (and the
  same query run ad hoc) fails. This applies to conditions that compare
  columns of several joined inputs other than the equalities the join looks
  rows up by: further `ON` conditions, `WHERE` conditions the optimizer
  moves into the join, and such conditions of `EXISTS`/`IN` subqueries — for
  example `ON a.id = b.id AND a.x / b.y > 1` with `b.y = 0`. Errors in
  expressions over a single input (including computed join keys such as
  `ON a.v = 100 / b.x`) are error records as usual.
- Error records are **not versioned**: an `AS OF` read checks the view's
  current error records, whatever they were at the read's GSN.
