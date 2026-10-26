# Web UI

`ivm` can serve a web console for inspecting a running server: live
throughput and lag, replication and snapshot progress, the catalog with its
lineage, the active dataflow as an interactive graph, arrangement sizes,
subscriptions, errors, the DDL history and a read-only SQL console.

The UI is served by the `ivm` binary itself. All HTML, JavaScript and CSS,
including the two third-party libraries it uses, are compiled into the
binary; the page loads nothing from the network (no CDNs, fonts or
analytics), so it works offline and air-gapped.

- [Enabling it](#enabling-it)
- [Authentication and security](#authentication-and-security)
- [Screens](#screens)
- [API reference](#api-reference)
- [What the engine measures](#what-the-engine-measures)
- [Implementation notes](#implementation-notes)
- [Third-party components](#third-party-components)
- [Limitations](#limitations)

## Enabling it

The HTTP server is off unless `--http-listen` is given:

```sh
ivm --data-dir ./ivm-data --listen 127.0.0.1:6875 --http-listen 127.0.0.1:6876
```

Then open <http://127.0.0.1:6876/>. The flag takes any `host:port`;
`127.0.0.1:0` picks a free port (the log line `web UI on http://…` shows
which). The server shares the process with the engine: it starts once
recovery has finished (an address that cannot be bound ends the process
with status 1, `ivm: cannot start the web UI: …`) and stops first at
shutdown.

To reach a server on another machine without exposing the port, prefer an
SSH tunnel (`ssh -L 6876:127.0.0.1:6876 host`) over listening on a public
interface. The server speaks plain HTTP; put a TLS-terminating reverse proxy
in front of it if it must be reachable over a network.

## Authentication and security

- **Password.** When ivm runs with `--password PW`, every HTTP request
  (pages, assets and API alike) requires HTTP Basic authentication with that
  password; the user name is ignored. Browsers prompt once and then send the
  credentials with every request, including the event stream. Wrong or
  missing credentials get `401` with
  `WWW-Authenticate: Basic realm="ivm", charset="UTF-8"`.
  The comparison is constant-time. Basic auth sends the password in clear
  text, so use it over loopback, an SSH tunnel or TLS.
- **Without a password,** the server only answers requests whose `Host`
  header is an IP address, `localhost` (or a `*.localhost` name) or the host
  of `--http-listen`. This blocks DNS-rebinding attacks, where a web page on
  another site points its own domain at your loopback address to read the
  API. Other host names get `403`; start ivm with `--password` to use them.
- **Read-only.** The API cannot change anything. The SQL console accepts
  exactly one `SELECT`, `SHOW` or `EXPLAIN` statement; everything else is
  refused with `403`. It only accepts `application/json` bodies (`415`
  otherwise), which a page on another origin cannot send without a CORS
  preflight that this server never grants.
- **Secrets.** The `conninfo` of a source (the Sources screen,
  `/api/sources`) is shown with passwords masked (`password=***`,
  `postgres://user:***@host`). The stored `CREATE SOURCE` and `CREATE SINK`
  statements are not: the catalog (`sql`), the history and the console
  (`ivm.objects`, `ivm.catalog_history`, `SHOW CREATE VIEW`) show their
  connection strings, passwords included, as a pgwire client sees them.
  Protect the UI like the SQL port.
- **Headers.** Responses carry a strict `Content-Security-Policy` (scripts
  only from the server itself, no inline scripts, no framing),
  `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY` and
  `Referrer-Policy: no-referrer`. API responses are `Cache-Control: no-store`.
- **Limits.** At most 256 concurrent connections (more are closed at once),
  request heads limited by a 64 KiB read buffer (`431` for a head that does
  not fit; the check is made per read, so heads somewhat above it can get
  through), bodies up to 64 KiB (`413`), 10 s to send the request head,
  10 s for a body, 60 s per request (`504`), 64 concurrent event streams
  and 4 concurrent console queries (`503` and `429` beyond). At most 16
  snapshot requests are queued at the engine at any time, however many
  clients poll; a request waits at most 15 s for a place in that queue
  (`503`) and 15 s for the engine's answer (`504`). Malformed requests get
  `400`; they never take the server down.

## Screens

The navigation on the left (a menu button on phones) leads to nine screens.
A dot in the header shows the state of the live connection (green: live,
yellow: the engine has not answered the last status request for 2 s, red:
reconnecting) next to the latest processed GSN. The moon button switches
between light and dark mode; the choice is remembered, otherwise the
operating system's preference applies. Every view keeps its state in the URL
hash (`#dataflow/node/12`, `#catalog/orders/lineage`), so views can be
bookmarked and shared.

### Overview

Tiles with the processed and durable GSN, the apply lag (GSNs logged but not
yet applied), throughput (log records per second), ingest (row changes per
second over all sources), dataflow output rows per second and how busy the
operators keep the engine thread, the last checkpoint and records since,
buffer-pool use against its capacity, process memory and CPU, the logical
clock and the retention frontier, and disk use of the store and the log.

Six charts cover the last 1, 5 or 20 minutes, one measure each:
throughput (GSN/s), ingest per source, apply lag, replication lag per
source (upstream WAL position minus the source frontier), engine time
(operator time and process CPU, 100% = one core) and memory (buffer pool,
its dirty part, process RSS). Hovering a chart shows the values at that
moment. The history is kept by the server (20 minutes at one sample per
second), so charts are filled as soon as the page opens. Below the charts,
cards summarize the catalog and dataflow, sources and sinks, and the engine
configuration.

### Sources

One card per source: status (`streaming`, `snapshotting …`, `error: …`),
publication and slot, the frontier LSN (every upstream commit at or below it
is in the input log), the last logged commit, the upstream WAL position the
walsender last reported and the lag between them, the time of the last
replication message, commits and row changes logged since start, the current
ingest rate with a two-minute sparkline, and the masked connection string.

The initial snapshot section shows how many tables are loaded and, per
table, its upstream name, key (the primary key, or the replica identity
index; none for a multiset table), row count, state (`done`, `loading`,
`pending`, or `cannot read; retrying` with the error as tooltip) and the
snapshot boundary (the key of the last row loaded; the next chunk starts
after it). Tables load one at a time in key-ordered chunks (a table without
a key in one piece); the upstream row count is not known, so the table
being loaded shows an animated bar and its growing row count. A table that
has to be copied again (after an upstream schema or key change, or a lost
replication slot) is `pending` or `loading` again until it is complete.
While a snapshot is loading, the frontier LSN does not advance (a read
waiting for an LSN must not see a partially loaded table), so the lag grows
until the snapshot completes.

### Catalog

A filterable list of all objects grouped by kind (sources, tables,
materialized views, views, indexes, sinks) with row counts and error badges.
Without a selection, the right side shows the dependency graph of the whole
catalog (sources feed tables, tables feed views and so on). Selecting an
object opens:

- **Overview:** the defining SQL, facts (id, creation GSN, rows, status, its
  dataflow node and arrangement), what it depends on and what uses it, and
  its columns with types, nullability and keys.
- **Lineage:** a graph of everything the object depends on and everything
  built on it; click a node to move there.
- **Plan:** the logical plan (`EXPLAIN`) of a view.
- **Dataflow:** `EXPLAIN DATAFLOW`, and a link that opens the object in the
  dataflow graph.
- **Maintenance:** `EXPLAIN MAINTENANCE`, the expected work per source delta.
- **Rows:** the first rows of a table or materialized view in index order
  (up to 100), with a link to query it in the console.
- **Errors:** the view's error records with SQLSTATE, message and the number
  of rows producing each.

Dependencies follow the rule `DROP` uses: a view depends on the relations
its query names, as the binder resolves them (`Catalog::direct_references`),
an index on its relation, a sink on the relation it reads and a replicated
table on its source. The relations a view reads through a virtual view are
that view's dependencies, so the graphs reach them through it. Only a view
whose definition no longer binds counts the relations whose names occur as
words in its SQL.

### Dataflow

The operators of the running dataflow as a layered graph, laid out left to
right or top down (by default whichever fits the view better). Each operator
shows its kind and id, the table or view it is the root of, its parameters
(filter, group key, join paths, limit, …), the records held in its
arrangements and, while it is processing changes, its output rate. Colors
follow the operator family (source, map/filter, join, aggregate,
order/limit, set operations, time/recursion; the legend is in the corner).
Edges of operators that produced output in the last interval are animated,
and busy operators glow. Operators not yet hydrated have a dashed outline.

Choose an object (table, materialized view, index, sink or subscription) in
the selector to highlight the operators it depends on; the rest are dimmed.
"Only its operators" hides everything else, which is the way to look at one
view in a large dataflow. Operators used by more than one object carry a
`×n` badge with the number of users: hash-consing shares identical
sub-plans, so a base table is typically used by everything built on it and
two views with the same sub-query share its operators. The search box finds
an operator by id, kind or name.

Drag or scroll to pan, pinch or Ctrl + scroll to zoom; with the graph
focused, `+`, `-`, `0` and the arrow keys work too, and operators can be
reached with Tab and opened with Enter. "Fit" shows the whole graph; a large
graph too small to read when fitted opens fitted to its width instead, and in
a dataflow of more than 80 operators choosing an object turns on "only its
operators" (untick it to see them in context). Graphs of more than 1,200
operators are only drawn per object.

Clicking an operator opens the inspector: its full parameters (for a join,
every input with its projection and each delta path with its lookups and
filters; for an aggregate, the group key and aggregates), output columns
with names and types, activity counters (steps, rows in and out, total and
average time per step, current output rate and share of the engine's time),
the objects using it, its inputs and consumers, and each arrangement it owns
with key, role (an index of the output, or operator state), records,
resident memory, references, readers and a sample of its rows.

### Arrangements

Every arrangement in a sortable, filterable table: owner operator, role,
key, records (rows counting multiplicity), bytes and pages resident in the
buffer pool, references, the operators reading it, the objects using it and
whether it is hydrated. Totals sit above the table. Clicking a row opens the
owning operator in the dataflow graph.

### Activity

`SUBSCRIBE` sessions (the operator they follow, the table or materialized
view subscribed to, or an ad hoc query, which includes a subscription to a
virtual view; whether progress messages are on), reads waiting for a GSN, an
LSN (`AS OF AT LEAST LSN`), a table's snapshot (initial, or a table copied
again) or a view's hydration, with their statements, requests waiting for their log record, the
DML queue, and sinks with status, committed GSN and how far behind the
engine they are.

### Errors

Error records held by materialized views (errors are data: an expression
error becomes an error record, and reads of the view fail until the
offending rows are gone), sources or sinks in an error state (including a
sink that cannot start, `error: broken: …`), and broken materialized views
and indexes (not maintained: they could not be built at a start, typically
because their definition no longer plans, or an index could not be rebuilt
after its relation changed shape; see
[Operations](user-guide/operations.md#recovery)). The number of rows
producing view errors (the sum of the error records' counts) also appears
as a badge in the navigation.

### History

Every DDL statement with the GSN it took effect at, newest first,
filterable by text and by kind (create, drop, alter). The screen shows the
latest 1,000 of the statements the catalog keeps (the most recent 10,000 to
11,000); the whole history is queryable as `ivm.catalog_history`.

### SQL console

Runs one read-only statement (`SELECT`, `SHOW`, `EXPLAIN`) through the same
path as a pgwire session, so results equal what psql would show. Choose the
read mode (`fresh` waits for the ingestion frontier and for tables still
being copied, `latest` reads what is processed now) and a row cap (100,
1,000 or 10,000 rows; the result says how many rows the statement
produced). `Ctrl+Enter` runs the statement. Recent statements are kept in
the browser. A statement without a result after 30 s is given up, and a
read still waiting in the engine (for freshness or a snapshot) is dropped
there.

## API reference

All endpoints are `GET` unless noted and return JSON (errors as
`{"error": "..."}` with a 4xx/5xx status, plus `"code": "SQLSTATE"` when
the error comes from SQL). Paths are relative to the server root. Every
engine snapshot is taken on the engine thread between two log records, so
it is consistent at one GSN, reported as `gsn`. Sizes are capped: samples
hold at most 500 rows and examine at most 20,000 entries, sample cells are
cut at 2 KiB and SQL texts at 64 KiB, the dataflow lists at most 20,000
operators and arrangements.

| Endpoint | Returns |
|---|---|
| `/api/overview` | Engine counters, configuration, storage, object counts, source and sink status, process and server facts |
| `/api/metrics?since=MS` | Buffered samples newer than `since` (Unix ms) |
| `/api/events` | Server-Sent Events: `sample`, `topology` |
| `/api/sources` | Sources with positions, lag, counters and per-table snapshot progress |
| `/api/catalog` | All catalog objects |
| `/api/catalog/history?limit=N` | DDL statements, newest first (default 500) |
| `/api/dataflow` | Operators, arrangements, edges and the objects using them |
| `/api/dataflow/node/ID?sample=N` | One operator in full, with samples of its arrangements (default 20 rows) |
| `/api/objects/NAME?sample=N` | One catalog object with plans, errors and a row sample (default 50 rows) |
| `/api/sessions` | Subscriptions, waiting reads, request queues, sinks |
| `/api/errors` | View error records, failing sources and sinks, broken views and indexes |
| `POST /api/query` | Read-only SQL console |

Static files are served from `/` (`index.html`, `js/…`, `css/…`,
`vendor/…`) with an `ETag` and `Cache-Control: no-cache`.

### `GET /api/overview`

```json
{
  "version": "0.1.0",
  "stats": {
    "processed_gsn": 1453, "durable_gsn": 1453, "retention_frontier": 453,
    "checkpoint_gsn": 1431, "records_since_checkpoint": 22,
    "last_checkpoint_ms": 1792931627308.5, "clock_ms": 1792931628328.0,
    "cache_used_bytes": 32505856, "cache_capacity_bytes": 268435456, "dirty_bytes": 11953766,
    "group_cache_bytes": 1048576, "input_queue_bytes": 0, "output_queue_bytes": 20480,
    "nodes": 27, "arrangements": 26, "nodes_created": 27, "arrangements_created": 26,
    "catalog_version": 15,
    "steps": {"steps": 604, "in_rows": 274155, "out_rows": 267461, "nanos": 437892939},
    "subscriptions": 1, "parked_reads": 0, "view_errors": 30
  },
  "config": {"data_dir": "ivm-data", "memory_bytes": 2147483648, "checkpoint_interval_secs": 10.0,
             "retention_gsns": 1000, "sync": true, "tick_ms": 1000},
  "storage": {"trees": 26, "loaded_nodes": 1520, "dirty_nodes": 410, "segments": 1,
              "segment_bytes": 136314880, "live_bytes": 120586240},
  "counts": {"tables": 4, "views": 1, "materialized_views": 8, "indexes": 2, "sources": 1,
             "sinks": 1, "nodes": 27, "arrangements": 26, "records": 1612345},
  "sources": [{"id": 1000, "name": "shop", "status": "streaming"}],
  "sinks": [{"id": 1017, "name": "region_out", "status": "running (gsn 1450)"}],
  "process": {"pid": 4242, "rss_bytes": 214000000, "virtual_bytes": 1900000000, "threads": 12,
              "open_fds": 40, "cpu_seconds": 21.5, "cpu_percent": 6.0},
  "server": {"started_ms": 1792931627000.0, "uptime_secs": 273.4, "auth": false,
             "log_bytes": 6700000, "store_bytes": 136300000}
}
```

The `*_bytes` fields of `stats` are bytes of memory: the buffer pool's
nodes (`cache_used_bytes`, of which `dirty_bytes` are dirty) against the
pool's current share of the memory budget (`cache_capacity_bytes`, which
moves as the other parts grow and shrink), the aggregation group caches,
the records waiting to be applied and the changes queued for subscriptions
and sinks; `config.memory_bytes` is the budget (`--memory-mb`, see
[Memory](user-guide/operations.md#memory)).
`stats.steps` sums the per-operator counters since start (monotonic);
`catalog_version` is the length of the catalog history, so it changes with
every applied DDL statement (the history is trimmed back to 10,000 entries
once it exceeds 11,000); `last_checkpoint_ms` is the engine start until the
first checkpoint; `records_since_checkpoint` counts every applied record,
clock ticks included (ticks alone make a checkpoint due only after 3,600 of
them, so on an idle engine it climbs to 3,600); `clock_ms` is null before
the first clock tick and `config.tick_ms` null with `--tick-ms 0`. The `process` fields come from
`/proc` and are null where it is not available (`cpu_percent` assumes 100
clock ticks per second, the Linux norm).

### `GET /api/metrics?since=MS` and `GET /api/events`

The server samples once per second and keeps 1,200 samples (20 minutes).
`/api/metrics` returns `{"interval_ms": 1000, "samples": [...]}`, oldest
first, optionally only those newer than `since`. Each sample:

```json
{
  "t": 1792931660317.4,
  "durable_gsn": 38, "processed_gsn": 38,
  "rss_bytes": 19173376, "cpu_percent": 1.0,
  "engine": { "...": "the same fields as overview.stats" },
  "engine_latency_ms": 0.03, "engine_busy_ms": null,
  "sources": [{"id": 1000, "frontier_lsn": 67599568, "upstream_lsn": 67616768,
               "commits": 47, "changes": 247711}]
}
```

`engine` is null when the engine did not answer within 600 ms (it may be
hydrating a large view); `engine_busy_ms` then says how long the request has
been outstanding. Counters are cumulative: rates are differences between
samples.

`/api/events` is an event stream (`text/event-stream`) with two events:
`sample` carries each new sample as above, and `topology` (data
`{"catalog_version": N}`) announces that the catalog or the dataflow changed,
so clients can reload them. A comment is sent every 15 seconds to keep the
connection alive. After a reconnect, fetch `/api/metrics?since=<last t>` to
fill the gap.

### `GET /api/sources`

```json
{
  "gsn": 65,
  "sources": [{
    "id": 1000, "name": "shop", "publication": "shop_pub", "slot": "ivm_shop_3f9a1c2e_1000",
    "conninfo": "host=127.0.0.1 port=5432 user=postgres dbname=shop password=***",
    "status": "streaming", "created_gsn": 3,
    "frontier_lsn": 67095592, "frontier": "0/3FFCC28",
    "last_commit_lsn": 67095592, "last_commit": "0/3FFCC28",
    "upstream_lsn": 67095592, "upstream": "0/3FFCC28", "lag_bytes": 0,
    "last_message_ms": 1792932981612.1, "server_time_ms": 1792932981610.8,
    "commits": 26, "changes": 245800, "slot_created": true,
    "tables": [{"id": 1001, "name": "customers", "upstream": "public.customers", "pk": ["id"],
                "snapshot": "done", "error": null, "boundary": ["5000"], "rows": 5000}],
    "tables_done": 4
  }]
}
```

`upstream_lsn` is the position the upstream walsender last reported (in
keepalives and with commits), i.e. how far it has decoded, so `lag_bytes`
measures what was decoded upstream but is not yet durable here. `commits`
and `changes` count what the source handed to the input log since the
process started (snapshot rows included). `pk` lists the table's key
columns (the replica identity index's, else the primary key's; empty for a
multiset table). `snapshot` is `done`, `loading` (the first table not done
whose chunks can be read, which the source copies next), `error` (its chunk
query failed; `error` holds the message, and the source retries it) or
`pending`; `boundary` is the key of the last row copied.

### `GET /api/catalog` and `GET /api/catalog/history`

```json
{
  "gsn": 1911, "version": 15,
  "objects": [{
    "id": 1005, "name": "revenue_by_region", "kind": "materialized view", "created_gsn": 83,
    "sql": "CREATE MATERIALIZED VIEW revenue_by_region AS SELECT ...",
    "columns": [{"name": "region", "type": "text", "nullable": false, "key": false}],
    "depends_on": [1001, 1002, 1003, 1004], "dependents": [1014, 1017],
    "rows": 4, "errors": 0, "node": 6, "arrangement": 8, "status": null, "detail": null
  }]
}
```

`kind` is `table`, `materialized view`, `view`, `index`, `source` or `sink`.
`rows` is the record count of the backing arrangement (tables, materialized
views, indexes), `errors` the number of distinct error records (materialized
views), `status` the state of sources and sinks, and `error: broken:
<reason>` for a broken materialized view or index (null otherwise),
`detail` a one-line description (replication origin of a table, key of an
index, target of a sink, publication and slot of a source). `depends_on` and
`dependents` follow the rule described under [Catalog](#catalog).

`/api/catalog/history?limit=N` returns `{"gsn", "total", "entries": [{"gsn",
"action", "sql"}]}`, newest first; `action` is the statement's leading
keywords, such as `CREATE MATERIALIZED VIEW` or `DROP TABLE`.

### `GET /api/dataflow`

```json
{
  "gsn": 1976,
  "nodes": [{
    "id": 4, "kind": "DeltaJoin", "label": null,
    "summary": "4 inputs, 4 delta paths, 12 lookups",
    "inputs": [2, 0, 1, 3], "schema": ["bigint", "integer", "text", "..."],
    "refs": 1, "hydrated": true, "arrangements": [], "reads": [0, 3, 4, 6],
    "records": 0, "users": [5, 13, 14],
    "counters": {"steps": 114, "in_rows": 11600, "out_rows": 95300, "nanos": 410000000}
  }],
  "arrangements": [{
    "id": 6, "node": 1, "role": "index", "key": ["#2"], "summaries": [], "refs": 3,
    "records": 272760, "resident_bytes": 6291456, "resident_nodes": 700,
    "hydrated": true, "readers": [4, 7, 25]
  }],
  "edges": [[2, 4], [0, 4]],
  "objects": [{"id": 1005, "name": "revenue_by_region", "kind": "materialized view",
               "node": 6, "arrangement": 8}],
  "totals": {"steps": 604, "in_rows": 274155, "out_rows": 267461, "nanos": 437892939},
  "truncated": false
}
```

`kind` is the operator (`Table`, `Constant`, `Mfp` (map/filter/project),
`Union`, `Negate`, `FlatMap`, `DeltaJoin`, `Reduce`, `Distinct`, `SetOp`,
`TopK`, `Window`, `Recursive`, `TemporalFilter`). `label` names the table, or
the views the operator is the root of. Column references in summaries and
details are positions (`#2` is the third input column). `arrangements` are
those the operator owns, `reads` those it reads (often its inputs'),
`records` the sum over the owned ones. `users` index into `objects`: the
tables, materialized views, indexes, sinks and subscriptions whose results
depend on the operator. An edge `[a, b]` means `a` is an input of `b` (one
per input; a self-join lists its input twice). `role` is `index` for an index
of the operator's output and `state` for operator-private state. `records`
is the weighted entry count (rows counting multiplicity) from the tree's
summary; `resident_*` is what the buffer pool holds in memory.

### `GET /api/dataflow/node/ID?sample=N`

```json
{
  "gsn": 2811,
  "node": { "...": "as in /api/dataflow" },
  "details": [{"label": "delta path 0",
               "value": "Δ input 0 → lookup input 1 in arr#0 by (#1) → lookup input 2 in arr#4 by (#0)"}],
  "consumers": [5],
  "columns": ["region", "orders", "revenue"],
  "arrangements": [{
    "id": 8, "node": 6, "role": "index", "key": ["#0"], "records": 4, "...": "as in /api/dataflow",
    "columns": [{"name": "region", "type": "text"}],
    "sample": {"rows": [{"values": ["amer", "45000", "34177500.00"], "weight": 1}],
               "truncated": false, "scanned": 4}
  }],
  "users": [{"id": 1005, "name": "revenue_by_region", "kind": "materialized view", "node": 6, "arrangement": 8}]
}
```

`details` lists the operator's parameters in full as labelled lines.
`columns` are the output column names when the operator is a table or view
root (empty otherwise). Samples are the first rows in key order with their
current weights (NULL is `null`); `truncated` says more rows exist, `scanned`
how many entries were examined. Unknown ids get `404`, malformed ones `400`.

### `GET /api/objects/NAME?sample=N`

`{"gsn", "object", "plan", "dataflow", "maintenance", "errors", "sample",
"source", "nodes", "problems"}`: `object` as in `/api/catalog`; `plan` the
`EXPLAIN` lines of a view; `dataflow` the `EXPLAIN DATAFLOW` lines and
`maintenance` the `EXPLAIN MAINTENANCE` rows (`source`,
`delta_rows_per_tick`, `work_per_delta`, `path`) of a materialized view;
`errors` its error records (`code`, `message`, `count`); `sample` the first
rows of a table or materialized view (`columns`, `rows`, `truncated`,
`scanned`); `source` the entry from `/api/sources` for a source; `nodes` the
dataflow operators the object depends on; `problems` the reason a broken
object is broken (`broken: <reason>`) and parts that could not be produced.
`plan`, `dataflow`, `maintenance`, `sample` and `source` are `null` where
they do not apply. Unknown names get `404`.

### `GET /api/sessions` and `GET /api/errors`

```json
{
  "gsn": 2011,
  "subscriptions": [{"node": 15, "target": "status_counts", "progress": true, "cancelled": false}],
  "parked": [{"wait": "snapshot", "gsn": null, "sql": "SELECT count(*) FROM orders"}],
  "pending_requests": 0, "dml_queued": 0, "dml_inflight": false,
  "sinks": [{"id": 1017, "name": "region_out", "from": "revenue_by_region", "table": "region_revenue",
             "status": "running (gsn 2011)", "committed_gsn": 2011, "lag_gsns": 0}]
}
```

`target` names the table or materialized view whose dataflow node the
subscription follows (one of them when identical views share the node), and
is null for a subscription to an ad hoc query or to a virtual view (which is
subscribed as its query); `wait` is `gsn`, `lsn`, `snapshot` or `hydration` (a statement reading a view still being hydrated).

`/api/errors` returns `{"gsn", "views": [{"id", "name", "code", "message",
"count"}], "sources": [{"id", "name", "status"}], "sinks": [...], "broken":
[...]}`, listing sources and sinks whose status starts with `error` (a sink
that cannot start has the status `error: broken: <reason>`), and broken
materialized views and indexes (status `error: broken: <reason>`, also the
`status` of such objects in `/api/catalog`).

### `POST /api/query`

Request (`Content-Type: application/json`):

```json
{"sql": "SELECT region, revenue FROM revenue_by_region", "mode": "fresh", "limit": 1000}
```

`mode` is `fresh` (default) or `latest` (anything else gets `400`);
`limit` defaults to 1,000 and is capped at 10,000. Response:

```json
{"columns": [{"name": "region", "type": "text"}, {"name": "revenue", "type": "numeric"}],
 "rows": [["amer", "34177500.00"]], "row_count": 4, "truncated": false,
 "command": null, "elapsed_ms": 0.6}
```

Values are PostgreSQL's text representations (NULL is `null`; cells longer
than 64 KiB are cut, and rows stop after about 8 MiB of cell text, with
`truncated` set). A body that is not `application/json` gets `415`, a
statement that is not `SELECT`, `SHOW` or `EXPLAIN` `403`, several
statements `400`, a failing statement `400` with its SQLSTATE in `code`
(`503` while the engine shuts down, `500` for internal errors), no result
within 30 s `504` (a fresh read waits for tables still being copied; try
`latest`; the waiting read is dropped in the engine), too many concurrent
queries `429`.

## What the engine measures

The UI reads state the engine keeps anyway (catalog, arrangement summaries,
frontiers, buffer-pool bookkeeping) plus three cheap counters added for it:

- **Per operator:** steps (GSNs in which the operator had input changes),
  input and output delta rows, and the time spent in the operator including
  the updates of its arrangements. They are updated once per operator per
  step, never per row, and the clock is read only for operators that run.
  Hydration (building a new view's state) is not counted.
- **Per source:** the upstream WAL position and server time from the
  replication stream, the arrival time of the last message, and commits and
  row changes handed to the input log, as relaxed atomic stores in the
  source thread; and the tables whose snapshot chunk cannot be read, with
  the error.
- **Per tree:** bytes and nodes resident in the buffer pool, computed on
  demand in one pass over the pool.

Counters live in memory and restart from zero with the process; so do the
sampled time series.

## Implementation notes

- `crates/ivm/src/coord/inspect.rs` builds the snapshots on the engine
  thread (`Request::Inspect`, `EngineHandle::inspect`). They are plain
  structs serialized by the HTTP thread.
- `crates/ivm/src/http/` is the server: `mod.rs` (startup, connection
  handling, router, auth), `api.rs` (JSON endpoints), `metrics.rs` (sampler,
  ring buffer, event stream, `/proc` statistics), `console.rs` (SQL console)
  and `assets.rs` (embedded files). It runs on a tokio runtime with two
  worker threads of its own (`ivm-http`); requests reach the engine through
  a callback that completes a oneshot channel, so no async thread blocks on
  the engine.
- `crates/ivm/web/` holds the UI: `index.html`, `favicon.svg`,
  `css/app.css`, and ES modules under `js/` (`main.js` router and shell,
  `live.js` samples and event stream, `api.js`, `dom.js`, `format.js`,
  `charts.js` (uPlot wrapper), `graph.js` (dagre layout, SVG rendering, pan
  and zoom) and one module per screen in `js/views/`). There is no build
  step: edit the files and rebuild the binary. A new file must be listed in
  `http/assets.rs`; a unit test fails otherwise.
- `crates/ivm/tests/http.rs` tests the server against a real engine:
  snapshot consistency with `SHOW ARRANGEMENTS` and `EXPLAIN DATAFLOW`,
  record counts, counters, error records, authentication, static files,
  malformed requests, the console and the event stream.

## Third-party components

Rust crates added for the server (all permissively licensed):

| Crate | Version | License |
|---|---|---|
| axum | 0.8.9 | MIT |
| tokio | 1.54.1 | MIT |
| hyper | 1.12.0 | MIT |
| hyper-util | 0.1.21 | MIT |
| tower | 0.5.3 | MIT |
| tower-http | 0.6.11 | MIT |
| futures-util | 0.3.34 | MIT OR Apache-2.0 |
| base64 | 0.22.1 | MIT OR Apache-2.0 |

JavaScript libraries, vendored unmodified from npm into
`crates/ivm/web/vendor/` with their licenses and served at `vendor/`:

| Library | Version | License | Use | Files (SHA-256) |
|---|---|---|---|---|
| [@dagrejs/dagre](https://github.com/dagrejs/dagre) | 3.1.3 | MIT | layered graph layout | `dagre.min.js` `54a503e3…b578` |
| [uPlot](https://github.com/leeoniya/uPlot) | 1.6.32 | MIT | time-series charts | `uPlot.iife.min.js` `19c8d4c6…8f1f`, `uPlot.min.css` `df630c6a…fa04` |

To update one, download the package (`npm pack @dagrejs/dagre@X` or
`npm pack uplot@X`), copy `dist/dagre.min.js` or `dist/uPlot.iife.min.js`
and `dist/uPlot.min.css`, and the `LICENSE`, then update this table.

## Limitations

- Counters, rates and the 20-minute metrics history are in memory only;
  they restart with the process.
- The snapshot progress of a table being loaded cannot show a percentage:
  the upstream row count is not known to the engine.
- `upstream_lsn` is the walsender's decoding position, not the upstream
  server's current WAL insert position, so a walsender that falls behind
  upstream does not show as lag.
- Operator time does not include hydration, and time spent outside
  operators (log apply bookkeeping, checkpoints) shows in process CPU only.
- Graphs of more than 1,200 operators are only drawn per object.
- The SQL console's row cap limits what is sent to the browser; the engine
  still computes the full result, as it would for psql.
- Plain HTTP only; use an SSH tunnel or a TLS-terminating proxy for remote
  access.
