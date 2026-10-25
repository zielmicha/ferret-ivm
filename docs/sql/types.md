# Data types

The engine implements a subset of PostgreSQL's types with PostgreSQL 17's
semantics — input and output formats, arithmetic, comparison, rounding and
error messages. Code: `crates/ivm/src/types/` (`mod.rs`, `datum.rs`,
`text.rs`), `crates/ivm/src/expr/cast.rs`, and the ports in
`crates/pgnumeric`, `crates/pgdatetime`, `crates/pgfuncs`.

## Supported types

| Type | Accepted names | OID | Notes |
|---|---|---|---|
| `boolean` | `bool`, `boolean` | 16 | input `t`/`true`/`yes`/`on`/`1`..., output `t`/`f`; a cast to `text` also gives `t`/`f` (PostgreSQL: `true`/`false`) |
| `smallint` | `smallint`, `int2` | 21 | |
| `integer` | `integer`, `int`, `int4`, `serial`, `serial4` | 23 | `serial` creates no sequence |
| `bigint` | `bigint`, `int8`, `bigserial`, `serial8` | 20 | |
| `real` | `real`, `float4`, `float(1..24)` | 700 | |
| `double precision` | `double precision`, `float8`, `float`, `float(25..53)` | 701 | |
| `numeric(p,s)` | `numeric`, `decimal`, `dec` with optional precision and scale | 1700 | arbitrary precision, NaN and ±Infinity, PostgreSQL's display scale rules (`crates/pgnumeric`) |
| `text` | `text` | 25 | |
| `character varying(n)` | `varchar(n)`, `character varying(n)` | 1043 | length checked on assignment, truncated by explicit casts of typed values (an untyped literal fails instead: `'abc'::varchar(2)` gives `value too long for type character varying(2)`, PostgreSQL returns `ab`) |
| `character(n)` | `char(n)`, `character(n)`, `bpchar` | 1042 | blank-padded; `char` without length is `char(1)` |
| `name` | `name` | 19 | |
| `bytea` | `bytea` | 17 | hex output (`\x...`), escape input |
| `date` | `date` | 1082 | including `infinity`, BC dates |
| `time(p)` | `time [(p)] [without time zone]` | 1083 | `time with time zone` is rejected |
| `timestamp(p)` | `timestamp [(p)] [without time zone]` | 1114 | |
| `timestamptz(p)` | `timestamptz`, `timestamp [(p)] with time zone` | 1184 | session time zone is always UTC |
| `interval` | `interval [(p)]` | 1186 | months, days and microseconds kept apart; `'1 mon' = '30 days'` compares equal |
| `uuid` | `uuid` | 2950 | |
| `json` | `json` | 114 | stored as validated text |
| `jsonb` | `jsonb` | 3802 | |
| `jsonpath` | `jsonpath` | 4072 | SQL/JSON path, stored as its canonical text |
| `oid` | `oid`, `regclass`, `regtype`, `regproc`, `regnamespace`, `regrole`, `regprocedure`, `regoper` | 26 | the `reg*` names are aliases of `oid`; casts of OIDs to `pg_catalog.regclass`, `regtype` and `regnamespace` print names (an unqualified `::regclass` prints the number), and names cannot be cast to them (`'integer'::regtype` fails with `invalid input syntax for type oid`) |
| arrays | `type[]`, `type[n]` | | of every type above; multi-dimensional (up to 6), with lower bounds |

Type modifiers (`numeric(10,2)`, `varchar(20)`, `timestamp(3)`,
`interval(2)`) are kept on columns and applied on input and on casts.
Interval field restrictions (`INTERVAL DAY TO SECOND`) are not applied.

Not supported: `time with time zone`, `money`, `bit`/`varbit`, `inet`,
`cidr`, `macaddr`, enums, domains, composite and record types, ranges and
multiranges, geometric types, `xml`, `tsvector`/`tsquery`, `hstore` and other
extension types. Upstream columns of such types are replicated as `text`
([type mapping](../user-guide/sources.md#type-mapping)); `CREATE TABLE`
with them fails with `type "x" does not exist` (`bit(n)` and
`time with time zone` with SQLSTATE `0A000`, `... is not supported`).

## Text format

Values are printed exactly like PostgreSQL 17 with the fixed session
settings `DateStyle = 'ISO, MDY'`, `IntervalStyle = 'postgres'`,
`TimeZone = 'UTC'`, `extra_float_digits = 1` (shortest round-trip float
output) and `bytea_output = 'hex'`. Text input accepts everything PostgreSQL
accepts for these types under the same settings, with the same errors
(`invalid input syntax for type integer: "abc"`, SQLSTATE `22P02`).

## Binary format

With the extended protocol, parameters and results may use binary format.
The encodings are PostgreSQL's `send`/`recv` formats: big-endian integers and
floats, `numeric` as PostgreSQL's base-10000 format, date/time types as
days or microseconds since 2000-01-01, `interval` as microseconds, days and
months, `uuid` as 16 bytes, `jsonb` and `jsonpath` as a version byte `1`
followed by text, arrays with PostgreSQL's array header, and text types as
UTF-8. `SUBSCRIBE` rows are always text.

## Casts

`CAST(x AS t)`, `x::t` and `t 'literal'` convert values. Each pair of types
has a context in which the conversion is applied automatically
(`expr/cast.rs`):

| From | To | Context |
|---|---|---|
| `smallint` | `integer`, `bigint`, `numeric`, `real`, `double precision`, `oid` | implicit |
| `integer` | `bigint`, `numeric`, `real`, `double precision`, `oid` | implicit |
| `bigint` | `numeric`, `real`, `double precision`, `oid` | implicit |
| `numeric` | `real`, `double precision` | implicit |
| `real` | `double precision` | implicit |
| `integer` | `smallint` | assignment |
| `bigint` | `smallint`, `integer` | assignment |
| `numeric` | `smallint`, `integer`, `bigint` | assignment |
| `real`, `double precision` | `smallint`, `integer`, `bigint`, `numeric` | assignment |
| `double precision` | `real` | assignment |
| `oid` | `integer`, `bigint` | assignment |
| `integer` ↔ `boolean` | | explicit |
| `varchar`, `char`, `name` | `text` | implicit |
| `text`, `char`, `name` | `varchar` | implicit |
| `text`, `varchar`, `char` | `name` | implicit |
| `text`, `varchar` | `char` | assignment |
| `date` | `timestamp`, `timestamptz` | implicit |
| `timestamp` | `timestamptz` | implicit |
| `timestamp`, `timestamptz` | `date`, `time` | assignment |
| `timestamptz` | `timestamp` | assignment |
| `time` | `interval` | implicit |
| `interval` | `time` | assignment |
| `json` ↔ `jsonb` | | assignment |
| `jsonb` | `boolean`, `numeric`, integers, floats | explicit (scalar values only) |
| any type | `text`, `varchar`, `char`, `name` | assignment (output function) |
| `text`, `varchar`, `char`, `name` | any type | explicit (input function) |
| `T[]` | `U[]` | element-wise, context of `T` → `U` |

- *implicit* casts are applied in expressions, function and operator
  arguments;
- *assignment* casts also in `UPDATE ... SET`;
- *explicit* casts only with `CAST` / `::`. (`INSERT` applies any cast,
  including explicit ones.)

Untyped literals (`'...'`, `NULL`) convert to any type. Numeric conversions
round like PostgreSQL (half away from zero for `numeric`, to nearest even for
floats → integers) and check ranges (`integer out of range`).

## Type resolution

Function and operator calls are resolved as in PostgreSQL
(`expr/func.rs`):

1. Candidates with the right name and number of arguments (polymorphic
   `anyelement`, `anyarray`, `anynonarray` and `any` parameters included).
2. For a binary operator with one untyped side, the candidate `(T, T)` with
   the other side's type `T` wins if it exists.
3. Otherwise each candidate gets a cost: 0 for an exact match, small for an
   untyped literal (lowest when the parameter is `text`), and for an
   implicit cast a base cost (lower for the target category's preferred type
   — `boolean`, `double precision`, `text`, `timestamptz`, `interval`,
   `oid`) plus the distance in the numeric (`smallint` < `integer` <
   `bigint` < `numeric` < `real` < `double precision`) or date/time
   (`date` < `timestamp` < `timestamptz`) hierarchy. The cheapest candidate
   wins; ties prefer resolving untyped literals as `text`.
4. No candidate: `function f(...) does not exist` or
   `operator does not exist: a op b` (SQLSTATE `42883`), with PostgreSQL's
   hint.

### Common type resolution

`UNION`/`INTERSECT`/`EXCEPT` columns, `CASE` results, `COALESCE`,
`GREATEST`/`LEAST`, `IN` lists, `ARRAY[...]` and `VALUES` columns use one
common type:

- untyped values take the type of the others (all untyped → `text`);
- numeric types → the highest in the numeric hierarchy;
- string types → `text` (PostgreSQL keeps `varchar` or `name` in some
  combinations, for example `COALESCE(varchar, char)` is `varchar` there);
- `date`/`timestamp`/`timestamptz` → the highest of them;
- arrays → array of the common element type;
- anything else must be identical, otherwise
  `<context> types X and Y cannot be matched` (SQLSTATE `42804`).

## Comparison and ordering

- Strings compare by Unicode code point (byte order of UTF-8), like the
  `C.UTF-8` / `C` collation.
- Floats: NaN equals NaN and sorts above all other values; `-0` equals `0`.
- `numeric`: `1.0 = 1.00`; NaN sorts above all numbers; infinities at the
  ends.
- `interval`: compared by total span with 30-day months and 24-hour days.
- `jsonb`: PostgreSQL's jsonb ordering.
- Arrays: element by element (NULL elements last), then by number of
  elements, dimensions, lengths and lower bounds.
- NULLs sort last in ascending order unless `NULLS FIRST` is given.
