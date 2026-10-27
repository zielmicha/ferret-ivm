# ivm — incremental view maintenance over PostgreSQL

Human note: This project is fully vibe-coded based on careful initial design. 
It seems reasonable, but isn't yet really tested or production ready.

## READMe

ivm replicates PostgreSQL tables through logical replication, maintains SQL
views over them incrementally, and serves the results over the PostgreSQL
wire protocol: point-in-time and read-your-writes reads, `SUBSCRIBE` change
streams, and exactly-once write-back sinks. It is a single-node
implementation of the design in [doc.md](doc.md).

```
            pgoutput (slot acked when the input log is durable)
PostgreSQL ───────────────────────────────▶ source ──▶ sequencer ──▶ input log (GSN-stamped, fsync, group commit)
                                                           │
             psql / drivers ◀── pgwire server ◀── engine ◀─┘  apply records in GSN order:
                                   │                │        dataflow step → arrangements (MVCC B-tree)
             sink (PG write-back) ◀┘                └─ checkpoint: catalog + arrangement roots
```

## Quick start

```sh
cargo build --release
./target/release/ivm --data-dir ./ivm-data --listen 127.0.0.1:6875 --http-listen 127.0.0.1:6876
psql -h 127.0.0.1 -p 6875 -U ivm      # SQL
# web UI for inspecting the server: http://127.0.0.1:6876/
```

To see everything working at once, `examples/demo/start.sh` starts an
upstream PostgreSQL with a small shop schema, an engine with the web UI,
a dozen maintained views, a sink, streaming changes and live subscriptions
([examples/demo](examples/demo/README.md)).

The upstream (PostgreSQL 14 or newer) needs `wal_level = logical`, a
publication, and a primary key, `REPLICA IDENTITY USING INDEX` or
`REPLICA IDENTITY FULL` on every published table
([Sources](docs/user-guide/sources.md)).

```sql
CREATE SOURCE pg FROM POSTGRES CONNECTION 'host=... dbname=app user=...' PUBLICATION 'app_pub';
CREATE MATERIALIZED VIEW revenue AS
  SELECT c.region, count(*) AS orders, sum(o.amount) AS total
  FROM orders o JOIN customers c ON c.id = o.customer_id
  GROUP BY c.region;
SELECT * FROM revenue;                                  -- fresh: waits for everything logged so far
SELECT * FROM revenue AS OF AT LEAST LSN '0/16B3748';   -- read-your-writes against PostgreSQL
COPY (SUBSCRIBE TO revenue WITH (PROGRESS)) TO STDOUT;  -- stream changes
EXPLAIN MAINTENANCE revenue;                            -- expected work per source change
```

## Documentation

The [documentation](docs/README.md) covers:

- the **user guide**: installation, sources, tables and views, reads and
  subscriptions, sinks, introspection, temporal filters, operations,
  troubleshooting and limitations;
- the **SQL reference**: statements, queries, types, functions, and what
  maintained views accept;
- the **internals**: architecture, dataflow, storage, planner, recovery;
- the **web UI** for inspecting a running server;
- the **on-disk format** and the **testing** guide.

## Repository layout

| Crate | Contents |
|---|---|
| `crates/ivm` | the engine, the server binary `ivm` (with the web UI in `crates/ivm/web`), the differential tester `ivm-difftest` and the benchmark `ivm-bench` |
| `crates/pgproto` | PostgreSQL wire protocol: client with logical replication, server-side codec, temporary test clusters |
| `crates/pgnumeric` | PostgreSQL-compatible `numeric` and float text conversions |
| `crates/pgdatetime` | PostgreSQL-compatible date/time types and functions |
| `crates/pgfuncs` | PostgreSQL-compatible string, pattern, `bytea`, `uuid`, array, JSON and jsonpath functions |
