# ivm demo

A self-contained demo: an upstream PostgreSQL with a small shop schema, an ivm
engine replicating it with the web UI enabled, a dozen maintained views, a
sink, a load generator streaming changes, and two `SUBSCRIBE` clients.

```sh
cargo build --release          # start.sh builds it if missing
examples/demo/start.sh         # prints the URLs
examples/demo/stop.sh          # keeps the data; --clean deletes it
```

Then open the web UI (default <http://127.0.0.1:6876/>) or connect with
`psql -h 127.0.0.1 -p 6875 -U ivm`.

What runs:

- `upstream.sql`: schema and seed data of database `shop` (regions,
  customers, products, orders, order items, events) and its publication.
- `views.sql`: the source and the views. They show joins with aggregates,
  maintained top-k, a view over a view, temporal filters over `now()`, outer
  joins, ordered-set aggregates, window functions, SQL/JSON path, a
  set-returning function, a virtual view, an index and a sink (into database
  `sinkdb`). `units_per_100` demonstrates errors as data: see below.
- `loadgen.py` (needs `psycopg` 3): about 20 small transactions per second
  (new orders, status changes, cancellations, restocks, events, customer
  changes), a bulk import of 3000 orders every 2 minutes, and every 3
  minutes a bad price update that sets a product's price to 0 for 20
  seconds — while it lasts, reads of `units_per_100` fail with "division by
  zero" and the Errors screen shows the error record.
- `psql` clients running `COPY (SUBSCRIBE TO top_products WITH (PROGRESS))`
  and the same for `order_status_counts`; their output goes to
  `subscribe-*.log`.

Settings (environment variables): `DEMO_DIR` (default `.scratch/demo` in the
repository), `PG_BIN` (default `/usr/lib/postgresql/17/bin`), `PG_PORT`
(54600), `IVM` (path of the binary), `IVM_LISTEN` (127.0.0.1:6875),
`HTTP_LISTEN` (127.0.0.1:6876), `RATE` (transactions per second, 20) and
`LOADGEN_ARGS` (extra `loadgen.py` options, e.g. `--glitch-every 60`).
