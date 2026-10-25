# Functions and operators

This page lists every built-in scalar function and operator, by category.
The list is the engine's function registry (`crates/ivm/src/expr/ops.rs`
and `builtins.rs`); aggregates, window functions and set-returning functions
are described in [Queries](queries.md). There are no user-defined functions.

Semantics — results, rounding, formats, error messages and SQLSTATEs — are
PostgreSQL 17's. Most functions are thin wrappers around ports of
PostgreSQL's C code, verified against results recorded from a real
PostgreSQL 17 server ([Testing](../testing.md#golden-tests)):

| Crate | Covers |
|---|---|
| `crates/pgnumeric` | `numeric` arithmetic and functions (port of `numeric.c`), float input/output, `to_char`/`to_number` for numbers |
| `crates/pgdatetime` | date/time input/output, arithmetic, `extract`, `date_trunc`, `date_bin`, `age`, `make_*`, `justify_*`, `to_char`/`to_timestamp`/`to_date`, time zones (embedded IANA tz database, as compiled into PostgreSQL's zoneinfo) |
| `crates/pgfuncs` | string functions (`text`), `LIKE`/`SIMILAR TO`/POSIX regular expressions (port of PostgreSQL's regex engine), `bytea`, `uuid`, array text I/O, `json`/`jsonb` operators and functions, SQL/JSON path (`jsonpath`) |

All functions are *strict* (a NULL argument gives NULL) unless noted
(`concat`, `concat_ws`, `format`, `num_nulls`, `num_nonnulls`,
`format_type`, `coalesce`, ...). Functions whose results depend on the clock
or are random are marked; they cannot be used in materialized views
([Maintained views](maintained-views.md)). Because the session time zone is
always UTC, functions that are only *stable* in PostgreSQL because of
`TimeZone` (`to_char(timestamptz, ...)`, `date_trunc(text, timestamptz)`,
casts between `timestamp` and `timestamptz`, ...) are immutable here.

## Operators

| Operator | Operand types | Result |
|---|---|---|
| `+` `-` `*` `/` | `smallint`, `integer`, `bigint`, `real`, `double precision`, `numeric` (same type both sides; mixed types are promoted — but `real` mixed with an integer or `numeric` is computed in `real`, where PostgreSQL uses `double precision`) | same type (integer division truncates) |
| `%` | integers, `numeric` | remainder |
| `^` | `double precision`, `numeric` | power |
| unary `-`, `+`, `@` (absolute value) | numeric types | |
| `\|/`, `\|\|/` | numeric types | square root, cube root (rewritten to `sqrt`, `cbrt`, so a `numeric` operand gives `numeric`; PostgreSQL's operators return `double precision`) |
| `&` `\|` `#` `~` `<<` `>>` | integers | bitwise and, or, xor, not, shifts |
| `=` `<>` `<` `<=` `>` `>=` | `boolean`, numeric types, `text`, `character`, `bytea`, `date`, `time`, `timestamp`, `timestamptz`, `interval`, `uuid`, `jsonb`, `oid`, arrays | `boolean` |
| `\|\|` | `text`, `text` / `text`, any non-array / any non-array, `text` | `text` |
| `\|\|` | `bytea` | `bytea` |
| `\|\|` | `jsonb` | `jsonb` (concatenation) |
| `\|\|` | arrays: array‖array, array‖element, element‖array | array |
| `~` `~*` `!~` `!~*` | `text` | POSIX regex match (case-insensitive with `*`, negated with `!`) |
| `LIKE` `ILIKE` `SIMILAR TO` (and `~~`, `~~*`, `!~~`, `!~~*`) | `text`; `LIKE` also `bytea` | `boolean` |
| `->` `->>` | `jsonb`/`json` with `text` key or `integer` index | `jsonb`/`json`, `text` |
| `#>` `#>>` | `jsonb`, `text[]` path | `jsonb`, `text` |
| `@>` `<@` | `jsonb`; arrays | containment |
| `?` `?\|` `?&` | `jsonb`, `text` / `text[]` | key existence (any / all) |
| `-` | `jsonb` with `text`, `integer` or `text[]` | delete key / element / keys |
| `#-` | `jsonb`, `text[]` | delete path |
| `@?` `@@` | `jsonb`, `jsonpath` | path exists / path predicate |
| `&&` | arrays | overlap |
| `+` `-` | `date` ± `integer` → `date`; `date` − `date` → `integer`; `date` ± `interval` → `timestamp`; `date` + `time` → `timestamp`; `timestamp[tz]` ± `interval`; `timestamp[tz]` − `timestamp[tz]` → `interval`; `time` ± `interval`; `time` − `time` → `interval`; `interval` ± `interval`; unary `-interval` | |
| `*` `/` | `interval` × / ÷ `double precision` | `interval` |

## Mathematical functions

| Function | Notes |
|---|---|
| `abs(x)` | all numeric types |
| `ceil(x)`, `ceiling(x)`, `floor(x)` | `double precision`, `numeric` |
| `round(x)`, `round(numeric, integer)` | floats round half to even, `numeric` half away from zero |
| `trunc(x)`, `trunc(numeric, integer)` | |
| `sign(x)`, `sqrt(x)`, `cbrt(double precision)` | |
| `exp(x)`, `ln(x)`, `log(x)` (base 10), `log10(x)`, `log(b numeric, x numeric)` | |
| `power(a, b)`, `pow(a, b)` | `double precision`, `numeric` |
| `mod(a, b)`, `div(numeric, numeric)` | |
| `gcd(a, b)`, `lcm(a, b)` | `integer`, `bigint`, `numeric` |
| `factorial(bigint)` | → `numeric` |
| `pi()`, `degrees(x)`, `radians(x)` | |
| `sin`, `cos`, `tan`, `cot`, `asin`, `acos`, `atan`, `atan2(y, x)`, `sinh`, `cosh`, `tanh`, `asinh`, `acosh`, `atanh` | `double precision` |
| `scale(numeric)`, `min_scale(numeric)`, `trim_scale(numeric)` | |
| `width_bucket(x, lo, hi, count)` | `double precision` |
| `random()` | volatile |

## String functions

| Function | Notes |
|---|---|
| `length(s)`, `char_length(s)`, `character_length(s)`, `octet_length(s)`, `bit_length(s)` | also `bytea` for `length`, `octet_length`, `bit_length` |
| `lower(s)`, `upper(s)`, `initcap(s)` | Unicode simple case mapping (PostgreSQL's tables, Unicode 15.1) |
| `substr(s, from [, count])`, `substring(s, from [, count])`, `substring(s, pattern)` | also `bytea`; the pattern form is a POSIX regex |
| `strpos(s, sub)`, `POSITION(sub IN s)` | |
| `overlay(s, r, from [, count])` | |
| `btrim(s [, chars])`, `ltrim(...)`, `rtrim(...)`, `TRIM(...)` | |
| `replace(s, from, to)`, `translate(s, from, to)`, `reverse(s)`, `repeat(s, n)` | |
| `split_part(s, delimiter, n)` | negative `n` counts from the end |
| `left(s, n)`, `right(s, n)`, `lpad(s, n [, fill])`, `rpad(s, n [, fill])` | |
| `ascii(s)`, `chr(n)` | |
| `starts_with(s, prefix)` | |
| `concat(...)`, `concat_ws(sep, ...)` | not strict; NULLs are skipped |
| `format(fmt, ...)` | `%s`, `%I`, `%L`, `%%`, positions and widths as in PostgreSQL |
| `quote_ident(s)`, `quote_literal(x)`, `quote_nullable(x)` | |
| `to_hex(integer \| bigint)` | |
| `md5(text \| bytea)` | → `text` |
| `sha224`, `sha256`, `sha384`, `sha512` (`bytea`) | → `bytea` |
| `encode(bytea, format)`, `decode(text, format)` | `hex`, `base64`, `escape` |
| `unistr(s)` | Unicode escapes |
| `string_to_array(s, delimiter [, null_string])`, `array_to_string(a, delimiter [, null_string])` | |
| `num_nulls(...)`, `num_nonnulls(...)` | not strict |

### Pattern matching

| Function | Notes |
|---|---|
| `LIKE`, `ILIKE`, `NOT LIKE`, ... with optional `ESCAPE` | the default escape is `\`; an empty escape disables it |
| `SIMILAR TO` | constant patterns only (translated to a regex) |
| `~`, `~*`, `!~`, `!~*` | POSIX regular expressions (advanced, extended and basic syntax, back-references, lookaround, PostgreSQL's leftmost-longest rules) |
| `regexp_replace(s, pattern, replacement [, flags])` | |
| `regexp_match(s, pattern [, flags])` | → `text[]` |
| `regexp_like(s, pattern [, flags])` | |
| `regexp_count(s, pattern [, start [, flags]])` | |
| `regexp_substr(s, pattern [, start [, n]])` | |
| `regexp_split_to_array(s, pattern [, flags])` | |
| `regexp_split_to_table`, `regexp_matches` | set-returning ([Queries](queries.md#set-returning-functions)) |

## Date and time functions

| Function | Notes |
|---|---|
| `now()`, `current_timestamp`, `transaction_timestamp()`, `statement_timestamp()`, `localtimestamp`, `current_date` | the logical clock in reads, the wall clock in DML; only in temporal filters in materialized views ([Temporal filters](../user-guide/temporal-filters-and-errors.md)) |
| `clock_timestamp()` | wall clock; volatile |
| `extract(field FROM x)`, `date_part(field, x)` | `date`, `time`, `timestamp`, `timestamptz`, `interval`; `extract` returns `numeric`, `date_part` `double precision` |
| `date_trunc(field, x)` | `timestamp`, `timestamptz`, `interval` |
| `date_bin(stride, source, origin)` | `timestamp`, `timestamptz` |
| `age(a, b)` | two-argument form only (`timestamp` or `timestamptz`) |
| `make_date(y, m, d)`, `make_time(h, m, s)`, `make_timestamp(...)`, `make_timestamptz(... [, zone])` | |
| `make_interval(years, months, weeks, days, hours, mins, secs)` | positional or named arguments (`make_interval(days => 3)`) |
| `to_timestamp(double precision)`, `to_timestamp(text, format)`, `to_date(text, format)` | |
| `to_char(timestamp \| timestamptz \| interval, format)` | |
| `justify_days(i)`, `justify_hours(i)`, `justify_interval(i)` | |
| `isfinite(x)` | `date`, `timestamp`, `timestamptz`, `interval` |
| `timezone(zone, x)`, `x AT TIME ZONE zone` | IANA names, abbreviations, POSIX offsets |
| `(a, b) OVERLAPS (c, d)` | `timestamp`, `timestamptz`, `time` (dates as timestamps) |
| `generate_series(start, stop, step)` | set-returning, `interval` step |

## Numeric formatting

| Function | Notes |
|---|---|
| `to_char(numeric \| bigint \| integer \| double precision \| real, format)` | PostgreSQL's number templates (`9`, `0`, `.`, `,`, `FM`, `PR`, `S`, `MI`, `RN`, `EEEE`, ...), C locale |
| `to_number(text, format)` | |

## JSON and jsonb

| Function | Notes |
|---|---|
| `to_json(x)`, `to_jsonb(x)` | any type; arrays become JSON arrays |
| `json_build_object(...)`, `jsonb_build_object(...)`, `json_build_array(...)`, `jsonb_build_array(...)` | variadic, also with no arguments |
| `json_typeof(json)`, `jsonb_typeof(jsonb)` | |
| `json_array_length(json)`, `jsonb_array_length(jsonb)` | |
| `jsonb_extract_path(j, VARIADIC path)`, `jsonb_extract_path_text(...)` | |
| `jsonb_set(j, path, value [, create_missing])`, `jsonb_insert(j, path, value [, insert_after])` | |
| `jsonb_strip_nulls(j)`, `jsonb_pretty(j)` | |
| `array_to_json(a [, pretty])` | |
| `jsonb_array_elements`, `jsonb_array_elements_text`, `jsonb_each`, `jsonb_each_text`, `jsonb_object_keys` | set-returning |
| `json_agg`, `jsonb_agg`, `json_object_agg`, `jsonb_object_agg` | aggregates |

The `json` type keeps its input text; only `->`, `->>`, `json_typeof`,
`json_array_length`, the `json_build_*` functions and casts work on `json`
directly — cast to `jsonb` for the rest.

### SQL/JSON path

| Function | Notes |
|---|---|
| `jsonb_path_exists(target, path [, vars [, silent]])` | |
| `jsonb_path_match(target, path [, vars [, silent]])` | |
| `jsonb_path_query_array(target, path [, vars [, silent]])` | |
| `jsonb_path_query_first(target, path [, vars [, silent]])` | |
| `jsonb_path_query(...)` | set-returning |
| the `_tz` variants of all of the above | time-zone-aware `.datetime()` comparisons (session zone UTC) |
| `target @? path`, `target @@ path` | |

Arguments may be passed by name (`target =>`, `path =>`, `vars =>`,
`silent =>`); `vars` defaults to `'{}'` and `silent` to `false`. The path
language is PostgreSQL's complete `jsonpath` (lax/strict modes, filters,
arithmetic, `like_regex`, `starts with`, `.datetime()` with templates, item
methods), with PostgreSQL's parser tables ported so that syntax errors match.

## Array functions

| Function | Notes |
|---|---|
| `array_length(a, dim)`, `array_upper(a, dim)`, `array_lower(a, dim)`, `cardinality(a)`, `array_ndims(a)`, `array_dims(a)` | |
| `array_append(a, x)`, `array_prepend(x, a)`, `array_cat(a, b)` | |
| `array_position(a, x)`, `array_remove(a, x)`, `array_replace(a, x, y)` | |
| `array_to_string(a, delimiter [, null_string])`, `string_to_array(...)` | |
| `array_to_json(a [, pretty])` | |
| `unnest(a)` | set-returning |
| `array_agg(x)` | aggregate |

## Other functions

| Function | Notes |
|---|---|
| `coalesce(...)`, `nullif(a, b)`, `greatest(...)`, `least(...)` | conditional expressions |
| `gen_random_uuid()` | volatile |
| `pg_typeof(x)` | the type name as `text` |
| `version()` | `PostgreSQL 17.0 (ivm)` |
| `current_database()`, `current_schema()`, `current_schemas(bool)` | `ivm`, `public` |
| `grouping(...)` | with `GROUP BY` |
| catalog helpers (`pg_table_is_visible`, `format_type`, `pg_get_indexdef`, `has_table_privilege`, ...) | for client tools ([Introspection](../user-guide/introspection.md#postgresql-system-catalogs)) |

## Not available

Functions not listed here do not exist (`function f(...) does not exist`),
for example: `age(timestamp)` (one argument), `to_tsvector` and the other
text-search functions, `regexp_instr`, `sum(interval)`, `avg(interval)`,
`percentile_cont` over `interval` or with an array of fractions,
`date_trunc(field, timestamptz, zone)`, `array_position(a, x, start)`,
`jsonb_to_record[set]`, `jsonb_populate_record`, `json_each` and the other
`json` (non-`jsonb`) processing functions, `generate_subscripts`,
`array_fill`, `encode`/`decode` formats other than those listed, and the
sequence and administration functions (`nextval`, `pg_sleep`,
`pg_cancel_backend`, ...).
