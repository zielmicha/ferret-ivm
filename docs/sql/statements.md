# SQL statements

The engine accepts PostgreSQL syntax: engine-specific statements are parsed
by a small hand-written parser (`crates/ivm/src/sql/parser.rs`), everything
else by `sqlparser` (PostgreSQL dialect) and then bound with PostgreSQL's
name-resolution and type rules. A query string may contain several
statements separated by `;` (simple query protocol); they run one after the
other and execution stops at the first error.

Identifiers follow PostgreSQL: unquoted names are folded to lower case,
double-quoted names keep their case. String literals use single quotes.

## Summary

| Statement | Reference |
|---|---|
| `SELECT`, `WITH`, `VALUES`, `TABLE` (+ `AS OF ...`) | [Queries](queries.md), [Reads](../user-guide/reads.md) |
| `CREATE TABLE` | [Tables](../user-guide/tables-and-views.md#create-table) |
| `INSERT`, `UPDATE`, `DELETE` | [DML](../user-guide/tables-and-views.md#insert) |
| `CREATE [OR REPLACE] [MATERIALIZED] VIEW` | [Views](../user-guide/tables-and-views.md#views-and-materialized-views), [Maintained views](maintained-views.md) |
| `CREATE INDEX` | [Indexes](../user-guide/tables-and-views.md#indexes) |
| `DROP TABLE / VIEW / MATERIALIZED VIEW / INDEX` | [DROP](../user-guide/tables-and-views.md#drop) |
| `CREATE SOURCE`, `ALTER SOURCE ... REFRESH`, `DROP SOURCE` | [Sources](../user-guide/sources.md) |
| `CREATE SINK`, `DROP SINK` | [Sinks](../user-guide/sinks.md) |
| `SUBSCRIBE`, `COPY (SUBSCRIBE ...) TO STDOUT` | [Subscriptions](../user-guide/reads.md#subscribe) |
| `EXPLAIN [PLAN | DATAFLOW | MAINTENANCE]` | [EXPLAIN](../user-guide/introspection.md#explain) |
| `SHOW ...` | [SHOW](../user-guide/introspection.md#show), [settings](../user-guide/reads.md#session-settings) |
| `SET`, `SET TIME ZONE`, `SET NAMES` | [Session settings](../user-guide/reads.md#session-settings) |
| `BEGIN`, `START TRANSACTION`, `COMMIT`, `ROLLBACK`, `SET TRANSACTION` | accepted, no effect ([Transactions](../user-guide/reads.md#transactions)) |
| `DISCARD`, `DEALLOCATE` | accepted, no effect |
| `CHECKPOINT` | forces a checkpoint ([Operations](../user-guide/operations.md#checkpoints-and-garbage-collection)) |

Everything else — `ALTER TABLE`, `TRUNCATE`, `CREATE TABLE AS`,
`CREATE SEQUENCE`, `CREATE FUNCTION`, `GRANT`, `COPY` (other forms),
`DECLARE`, `PREPARE`, `EXECUTE`, `LISTEN`, `VACUUM`, ... — fails with
`unsupported statement: ...` (SQLSTATE `0A000`).

DDL statements (`CREATE`, `DROP`, `ALTER SOURCE`) are logged and get a GSN;
their canonical text is visible in `ivm.catalog_history`. DML statements are
logged as row changes. Reads, `SHOW`, `EXPLAIN` and `SET` are not logged.

## Synopses

### Queries

```sql
[ WITH [ RECURSIVE ] cte [ , ... ] ]
SELECT [ ALL | DISTINCT | DISTINCT ON ( expr [ , ... ] ) ] select_list
  [ FROM from_item [ , ... ] ]
  [ WHERE condition ]
  [ GROUP BY grouping_element [ , ... ] ]
  [ HAVING condition ]
  [ WINDOW name AS ( window_spec ) [ , ... ] ]
  [ { UNION | INTERSECT | EXCEPT } [ ALL | DISTINCT ] select ]
  [ ORDER BY expr [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [ , ... ] ]
  [ LIMIT { count | ALL } ] [ OFFSET start [ ROW | ROWS ] ]
  [ FETCH { FIRST | NEXT } [ count ] { ROW | ROWS } { ONLY | WITH TIES } ]
  [ AS OF { gsn | FRESH | LATEST | AT LEAST LSN 'X/Y' } ]

VALUES ( expr [ , ... ] ) [ , ... ]
TABLE name
```

Details: [Queries](queries.md). `AS OF` only at the very end of a top-level
statement ([Reads](../user-guide/reads.md#syntax)).

### Tables and DML

```sql
CREATE TABLE [ IF NOT EXISTS ] name ( column type [ constraint ... ] [ , ... ] [ , PRIMARY KEY ( column [ , ... ] ) ] )

INSERT INTO table [ ( column [ , ... ] ) ]
    { VALUES ( { expr | DEFAULT } [ , ... ] ) [ , ... ] | query }
    [ ON CONFLICT [ ( column [ , ... ] ) ] DO NOTHING ]

UPDATE table [ [ AS ] alias ] SET column = expr [ , ... ] [ WHERE condition ]

DELETE FROM table [ [ AS ] alias ] [ WHERE condition ]
```

### Views and indexes

```sql
CREATE [ OR REPLACE ] [ MATERIALIZED ] VIEW [ IF NOT EXISTS ] name [ ( column [ , ... ] ) ] AS query

CREATE INDEX [ IF NOT EXISTS ] [ name ] ON relation ( column [ , ... ] )

DROP { TABLE | VIEW | MATERIALIZED VIEW | INDEX } [ IF EXISTS ] name [ , ... ] [ CASCADE | RESTRICT ]
```

### Sources and sinks

```sql
CREATE SOURCE name FROM POSTGRES [ ( ]
    CONNECTION [ = ] 'conninfo' [ , ] PUBLICATION [ = ] 'publication'
    [ [ , ] SLOT [ = ] 'slot' ]
    [ [ , ] FOR ALL TABLES | [ , ] FOR TABLES ( table [ , ... ] ) ]
[ ) ]

ALTER SOURCE name REFRESH

DROP SOURCE [ IF EXISTS ] name [ CASCADE ]

CREATE SINK name FROM relation INTO POSTGRES [ ( ]
    CONNECTION [ = ] 'conninfo' [ , ] TABLE [ = ] 'table'
    [ [ , ] KEY [ = ] ( column [ , ... ] ) ]
[ ) ]

DROP SINK [ IF EXISTS ] name
```

The options of `CREATE SOURCE` and `CREATE SINK` may appear in any order.
Tokens after the source or sink name in `DROP SOURCE` / `DROP SINK` other
than `CASCADE` are ignored.

### Subscriptions

```sql
SUBSCRIBE [ TO ] { name | ( query ) }
    [ WITH ( { SNAPSHOT | PROGRESS } [ [ = ] value ] [ , ... ] ) ]
    [ AS OF gsn ]

COPY ( SUBSCRIBE ... ) TO STDOUT
```

### Introspection and control

```sql
EXPLAIN [ PLAN | DATAFLOW | MAINTENANCE ] [ FOR ] { query | [ MATERIALIZED ] [ VIEW ] name }

SHOW { SOURCES | TABLES | VIEWS | MATERIALIZED VIEWS | INDEXES | SINKS
     | ARRANGEMENTS | FRONTIERS | GSN | ALL }
SHOW COLUMNS { FROM | IN } name
SHOW CREATE [ MATERIALIZED ] VIEW name
SHOW parameter

SET parameter { = | TO } value
SET TIME ZONE value

CHECKPOINT
```

## Errors

Errors are reported like PostgreSQL's: SQLSTATE, message, and optionally
detail and hint. The engine's own errors use these SQLSTATEs most often:

| SQLSTATE | Meaning |
|---|---|
| `0A000` | feature not supported |
| `42601` | syntax error |
| `42P01` | undefined table (`relation "x" does not exist`) |
| `42703` | undefined column |
| `42883` | undefined function / operator |
| `42804` | datatype mismatch |
| `42P07`, `42710` | duplicate table / object |
| `42809` | wrong object type |
| `2BP01` | dependent objects still exist (use `CASCADE`) |
| `23505`, `23502` | unique (primary key) / not-null violation |
| `55000` | object not in prerequisite state (e.g. `AS OF` before the retention frontier, table without primary key in a source) |
| `55006` | data directory in use |
| `57014` | query canceled (`SUBSCRIBE` cancelled) |
| `57P01` | engine shutting down |
| `08006` | connection failure (upstream) |
| `XX000`, `XX001` | internal error, data corrupted |

Errors of SQL functions and operators carry PostgreSQL's SQLSTATEs and
messages (for example `22012 division by zero`), because their
implementations are ports of PostgreSQL's ([Functions](functions.md)).
