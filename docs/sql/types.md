# Data types

The engine implements a subset of PostgreSQL's types with PostgreSQL 17's
semantics — input and output formats, arithmetic, comparison, rounding and
error messages. Code: `crates/ivm/src/types/` (`mod.rs`, `datum.rs`,
`text.rs`), `crates/ivm/src/expr/cast.rs`, and the ports in
`crates/pgnumeric`, `crates/pgdatetime`, `crates/pgfuncs`.

## Supported types

| Type | Accepted names | OID | Notes |
|---|---|---|---|
| `boolean` | `bool`, `boolean` | 16 | input `t`/`true`/`yes`/`on`/`1`..., output `t`/`f`; a cast to `text`, `varchar` or `char` gives `true`/`false`, as in PostgreSQL |
| `smallint` | `smallint`, `int2` | 21 | |
| `integer` | `integer`, `int`, `int4`, `serial`, `serial4` | 23 | `serial` creates no sequence |
| `bigint` | `bigint`, `int8`, `bigserial`, `serial8` | 20 | |
| `real` | `real`, `float4`, `float(1..24)` | 700 | |
| `double precision` | `double precision`, `float8`, `float`, `float(25..53)` | 701 | |
| `numeric(p,s)` | `numeric`, `decimal`, `dec` with optional precision and scale | 1700 | arbitrary precision, NaN and ±Infinity, PostgreSQL's display scale rules (`crates/pgnumeric`) |
| `text` | `text` | 25 | |
| `character varying(n)` | `varchar(n)`, `character varying(n)` | 1043 | length checked on assignment (`value too long for type character varying(2)`), truncated by explicit casts (`'abc'::varchar(2)` is `ab`) |
| `character(n)` | `char(n)`, `character(n)`, `bpchar` | 1042 | blank-padded; `char` without length is `char(1)`; trailing spaces are ignored by comparisons and dropped by casts to `text`, `varchar` and `name` (see [Comparison and ordering](#comparison-and-ordering)) |
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
| `oid` | `oid`, `regclass`, `regtype`, `regproc`, `regnamespace`, `regrole`, `regprocedure`, `regoper` | 26 | the `reg*` names are aliases of `oid`, see below |
| arrays | `type[]`, `type[n]` | | of every type above; multi-dimensional (up to 6), with lower bounds |

Type modifiers (`numeric(10,2)`, `varchar(20)`, `timestamp(3)`,
`interval(2)`, `interval day to second`) are kept on columns and applied on
input and on casts, including interval field restrictions (`INTERVAL '1 day
2 hours' DAY` and `'1 day 2 hours'::interval day` are `1 day`). The modifier
of an array type applies to every element (`'{abc}'::varchar(2)[]` is
`{ab}`, and inserts into a `numeric(5,2)[]` column check every element).

`reg*` casts: `regclass`, `regtype` and `regnamespace` read names, like
PostgreSQL's input functions: `'users'::regclass` (possibly
schema-qualified or quoted), `'integer'::regtype` (any type spelling) and
`'pg_catalog'::regnamespace` are the object's OID, as an `oid` value that
compares with `oid` columns; unknown names fail with PostgreSQL's errors
(`relation "x" does not exist`). The system catalogs have PostgreSQL 17's
fixed OIDs (`'pg_class'::regclass` is 1259). An OID cast to one of these
types gives the object's name as `text` (an OID that names nothing gives the
number), and a select-list item that is a `reg*` cast prints the name and is
named after the type, so `SELECT 'users'::regclass` shows `users`. Because
such a cast of an OID is `text`, `oid_col::regclass = 'users'::regclass`
fails (`operator does not exist: text = oid`); compare `oid_col =
'users'::regclass` instead. `regproc`, `regrole`, `regprocedure` and
`regoper` are plain `oid`s: names cannot be cast to them.

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
| `text`, `char` | `varchar` | implicit |
| `text`, `varchar` | `char` | implicit |
| `text`, `varchar`, `char` | `name` | implicit |
| `name` | `varchar`, `char` | assignment |
| `date` | `timestamp`, `timestamptz` | implicit |
| `timestamp` | `timestamptz` | implicit |
| `timestamp`, `timestamptz` | `date`, `time` | assignment |
| `timestamptz` | `timestamp` | assignment |
| `time` | `interval` | implicit |
| `interval` | `time` | assignment |
| `json` ↔ `jsonb` | | assignment |
| `jsonb` | `boolean`, `numeric`, integers, floats | explicit (scalar values only) |
| any type | `text`, `varchar`, `char`, `name` | assignment (output function; `boolean` gives `true`/`false`) |
| `text`, `varchar`, `char`, `name` | any type | explicit (input function) |
| `T[]` | `U[]` | element-wise, context of `T` → `U` |

- *implicit* casts are applied in expressions, function and operator
  arguments;
- *assignment* casts also in `UPDATE ... SET`;
- *explicit* casts only with `CAST` / `::`. (`INSERT` applies any cast,
  including explicit ones.)

Untyped literals (`'...'`, `NULL`) convert to any type. Numeric conversions
round like PostgreSQL (half away from zero for `numeric`, to nearest even for
floats → integers) and check ranges (`integer out of range`). Casts from
`character(n)` to `text`, `varchar` and `name` drop the trailing spaces (as
PostgreSQL's `text(bpchar)`), and so do the functions taking `text` and the
`||` operator; `LIKE`, `SIMILAR TO` and the regular-expression operators see
the padded value, and `octet_length` counts the padding, as in PostgreSQL.
An explicit cast truncates a value that is too long for `varchar(n)` or
`char(n)`; an assignment (`INSERT`, `UPDATE`) fails instead.

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
   wins; ties prefer resolving untyped literals as `text`, then as the
   preferred type of their category (`abs('-1.5')` is
   `abs(double precision)`).
4. No candidate: `function f(...) does not exist` or
   `operator does not exist: a op b` (SQLSTATE `42883`), with PostgreSQL's
   hint.

As in PostgreSQL, `+`, `-`, `*` and `/` exist for (`real`,
`double precision`) and (`double precision`, `real`), so `real` combined
with an integer or `numeric` is computed in `double precision`
(`0.1::real + 1` is `1.1000000014901161`). There are no cross-type
comparison operators (equi-join keys rely on `=` comparing one type), so a
comparison of `real` with an integer or `numeric` value is done in `real`;
see [Comparison and ordering](#comparison-and-ordering).

### Common type resolution

`UNION`/`INTERSECT`/`EXCEPT` columns, `CASE` results, `COALESCE`,
`GREATEST`/`LEAST`, `IN` lists, `ARRAY[...]` and `VALUES` columns use one
common type, chosen like PostgreSQL's `select_common_type`:

- untyped values are ignored (all untyped → `text`);
- every other input must be of the same type category as the first one
  (boolean; numeric, including `oid`; string; date/time; `interval`;
  arrays; other), otherwise `<context> types X and Y cannot be matched`
  (SQLSTATE `42804`);
- the first type is the candidate; a later type replaces it when the
  candidate is not the category's preferred type (`boolean`,
  `double precision`, `text`, `timestamptz`, `interval`, `oid`) and converts
  to the later type implicitly but not back;
- the inputs are then converted to the result with implicit casts; an
  input without one fails (`expression is of type jsonb but json is
  required`);
- `CASE` considers its `ELSE` result first.

So numeric types give the widest of them (`integer` and `numeric` →
`numeric`, `numeric` and `real` → `real`), dates and timestamps the latest
(`date` and `timestamp` → `timestamp`), `varchar` with `text` stays
`varchar` but `text` with `varchar` is `text`, `COALESCE(char_col,
text_col)` is `character` while `CASE WHEN ... THEN char_col ELSE text_col
END` is `text`, and arrays take the common element type.

## Comparison and ordering

- Strings compare by Unicode code point (byte order of UTF-8), like the
  `C.UTF-8` / `C` collation.
- `character(n)`: comparisons (`=`, `<`, ..., `IN`, `IS DISTINCT FROM`,
  `NULLIF`, joins) ignore trailing spaces, as in PostgreSQL. Grouping,
  `DISTINCT`, set operations and `ORDER BY` use the stored, padded value,
  which differs from PostgreSQL only when values of different padding are
  mixed (columns of different lengths, or untyped `character`):
  `SELECT k FROM c3 UNION SELECT k FROM c5` over `char(3)` and `char(5)`
  columns returns `'a'` twice.
- `real` compared with an integer or `numeric` value is compared as `real`:
  `0.1::real = 0.1` is true here and false in PostgreSQL, which compares in
  `double precision` (`real` with `double precision` is compared as
  `double precision`, as in PostgreSQL).
- Floats: NaN equals NaN and sorts above all other values; `-0` equals `0`.
- `numeric`: `1.0 = 1.00`; NaN sorts above all numbers; infinities at the
  ends.
- `interval`: compared by total span with 30-day months and 24-hour days.
- `jsonb`: PostgreSQL's jsonb ordering.
- Arrays: element by element (NULL elements last), then by number of
  elements, dimensions, lengths and lower bounds.
- NULLs sort last in ascending order unless `NULLS FIRST` is given.
