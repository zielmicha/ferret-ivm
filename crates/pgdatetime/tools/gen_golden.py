#!/usr/bin/env python3
"""Generate golden test data for pgdatetime from a real PostgreSQL 17.

Each output line (tests/golden/<category>.tsv) is tab separated:

    func <TAB> session_tz <TAB> arg1 <TAB> ... <TAB> => <TAB> result

where result is "OK <text>", "ERR <sqlstate> <message>" or "NULL".

Requires a running server with the pgdt_eval() helper (created by this
script) reachable via the psql command given in $PGDT_PSQL, e.g.
    PGDT_PSQL="psql -h @pgdt54402 -p 54402 -U postgres"
"""
import itertools
import os
import shlex
import subprocess
import sys

sys.dont_write_bytecode = True

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.environ.get("PGDT_GOLDEN_OUT", os.path.join(HERE, "..", "tests", "golden"))
PSQL = shlex.split(os.environ.get("PGDT_PSQL", "psql -h @pgdt54402 -p 54402 -U postgres"))

EVAL_FN = r"""
CREATE OR REPLACE FUNCTION pgdt_eval(tz text, expr text) RETURNS text LANGUAGE plpgsql AS $f$
DECLARE r text;
BEGIN
  PERFORM set_config('TimeZone', tz, false);
  PERFORM set_config('DateStyle', 'ISO, MDY', false);
  PERFORM set_config('IntervalStyle', 'postgres', false);
  EXECUTE 'SELECT (' || expr || ')::text' INTO r;
  IF r IS NULL THEN RETURN 'NULL'; END IF;
  RETURN 'OK ' || r;
EXCEPTION WHEN OTHERS THEN
  RETURN 'ERR ' || SQLSTATE || ' ' || SQLERRM;
END $f$;
"""


def q(s):
    return "'" + str(s).replace("'", "''") + "'"


def cmp_sql(a, b):
    return f"(CASE WHEN {a} < {b} THEN -1 WHEN {a} > {b} THEN 1 WHEN {a} = {b} THEN 0 END)"


def ov(x):
    return "NULL::timestamp" if x == "NULL" else f"{q(x)}::timestamp"


# SQL templates: func -> f(args) -> SQL expression
SQL = {
    # text I/O
    "date_in": lambda a: f"{q(a[0])}::date",
    "time_in": lambda a: f"{q(a[0])}::time",
    "timestamp_in": lambda a: f"{q(a[0])}::timestamp",
    "timestamptz_in": lambda a: f"{q(a[0])}::timestamptz",
    "interval_in": lambda a: f"{q(a[0])}::interval",
    "interval_out_style": lambda a: f"(SELECT set_config('IntervalStyle', {q(a[0])}, false) || {q(a[1])}::interval::text)",
    # typmod
    "time_p": lambda a: f"{q(a[0])}::time({a[1]})",
    "timestamp_p": lambda a: f"{q(a[0])}::timestamp({a[1]})",
    "timestamptz_p": lambda a: f"{q(a[0])}::timestamptz({a[1]})",
    "interval_p": lambda a: f"{q(a[0])}::interval({a[1]})",
    "interval_typmod": lambda a: f"{q(a[0])}::interval {a[1]}",
    # casts
    "date_to_timestamp": lambda a: f"{q(a[0])}::date::timestamp",
    "timestamp_to_date": lambda a: f"{q(a[0])}::timestamp::date",
    "date_to_timestamptz": lambda a: f"{q(a[0])}::date::timestamptz",
    "timestamptz_to_date": lambda a: f"{q(a[0])}::timestamptz::date",
    "timestamp_to_timestamptz": lambda a: f"{q(a[0])}::timestamp::timestamptz",
    "timestamptz_to_timestamp": lambda a: f"{q(a[0])}::timestamptz::timestamp",
    "timestamp_to_time": lambda a: f"{q(a[0])}::timestamp::time",
    "timestamptz_to_time": lambda a: f"{q(a[0])}::timestamptz::time",
    "time_to_interval": lambda a: f"{q(a[0])}::time::interval",
    "interval_to_time": lambda a: f"{q(a[0])}::interval::time",
    # arithmetic
    "date_pl_days": lambda a: f"{q(a[0])}::date + ({a[1]})::int4",
    "date_mi_days": lambda a: f"{q(a[0])}::date - ({a[1]})::int4",
    "date_mi_date": lambda a: f"{q(a[0])}::date - {q(a[1])}::date",
    "date_pl_interval": lambda a: f"{q(a[0])}::date + {q(a[1])}::interval",
    "date_mi_interval": lambda a: f"{q(a[0])}::date - {q(a[1])}::interval",
    "date_pl_time": lambda a: f"{q(a[0])}::date + {q(a[1])}::time",
    "timestamp_pl_interval": lambda a: f"{q(a[0])}::timestamp + {q(a[1])}::interval",
    "timestamp_mi_interval": lambda a: f"{q(a[0])}::timestamp - {q(a[1])}::interval",
    "timestamptz_pl_interval": lambda a: f"{q(a[0])}::timestamptz + {q(a[1])}::interval",
    "timestamptz_mi_interval": lambda a: f"{q(a[0])}::timestamptz - {q(a[1])}::interval",
    "timestamp_mi": lambda a: f"{q(a[0])}::timestamp - {q(a[1])}::timestamp",
    "timestamptz_mi": lambda a: f"{q(a[0])}::timestamptz - {q(a[1])}::timestamptz",
    "time_pl_interval": lambda a: f"{q(a[0])}::time + {q(a[1])}::interval",
    "time_mi_interval": lambda a: f"{q(a[0])}::time - {q(a[1])}::interval",
    "time_mi_time": lambda a: f"{q(a[0])}::time - {q(a[1])}::time",
    "interval_pl": lambda a: f"{q(a[0])}::interval + {q(a[1])}::interval",
    "interval_mi": lambda a: f"{q(a[0])}::interval - {q(a[1])}::interval",
    "interval_um": lambda a: f"- {q(a[0])}::interval",
    "interval_mul": lambda a: f"{q(a[0])}::interval * ({a[1]})::float8",
    "interval_div": lambda a: f"{q(a[0])}::interval / ({a[1]})::float8",
    "interval_cmp": lambda a: f"interval_cmp({q(a[0])}::interval, {q(a[1])}::interval)",
    "justify_hours": lambda a: f"justify_hours({q(a[0])}::interval)",
    "justify_days": lambda a: f"justify_days({q(a[0])}::interval)",
    "justify_interval": lambda a: f"justify_interval({q(a[0])}::interval)",
    # fields
    "extract_date": lambda a: f"extract({q(a[0])} from {q(a[1])}::date)",
    "extract_time": lambda a: f"extract({q(a[0])} from {q(a[1])}::time)",
    "extract_timestamp": lambda a: f"extract({q(a[0])} from {q(a[1])}::timestamp)",
    "extract_timestamptz": lambda a: f"extract({q(a[0])} from {q(a[1])}::timestamptz)",
    "extract_interval": lambda a: f"extract({q(a[0])} from {q(a[1])}::interval)",
    "date_part_date": lambda a: f"date_part({q(a[0])}, {q(a[1])}::date)",
    "date_part_time": lambda a: f"date_part({q(a[0])}, {q(a[1])}::time)",
    "date_part_timestamp": lambda a: f"date_part({q(a[0])}, {q(a[1])}::timestamp)",
    "date_part_timestamptz": lambda a: f"date_part({q(a[0])}, {q(a[1])}::timestamptz)",
    "date_part_interval": lambda a: f"date_part({q(a[0])}, {q(a[1])}::interval)",
    "date_trunc_timestamp": lambda a: f"date_trunc({q(a[0])}, {q(a[1])}::timestamp)",
    "date_trunc_timestamptz": lambda a: f"date_trunc({q(a[0])}, {q(a[1])}::timestamptz)",
    "date_trunc_timestamptz_at_zone": lambda a: f"date_trunc({q(a[0])}, {q(a[1])}::timestamptz, {q(a[2])})",
    "date_trunc_interval": lambda a: f"date_trunc({q(a[0])}, {q(a[1])}::interval)",
    "date_bin": lambda a: f"date_bin({q(a[0])}::interval, {q(a[1])}::timestamp, {q(a[2])}::timestamp)",
    "age": lambda a: f"age({q(a[0])}::timestamp, {q(a[1])}::timestamp)",
    "age_tz": lambda a: f"age({q(a[0])}::timestamptz, {q(a[1])}::timestamptz)",
    "make_date": lambda a: f"make_date({a[0]}, {a[1]}, {a[2]})",
    "make_time": lambda a: f"make_time({a[0]}, {a[1]}, {a[2]})",
    "make_timestamp": lambda a: f"make_timestamp({a[0]}, {a[1]}, {a[2]}, {a[3]}, {a[4]}, {a[5]})",
    "make_timestamptz": lambda a: f"make_timestamptz({a[0]}, {a[1]}, {a[2]}, {a[3]}, {a[4]}, {a[5]})",
    "make_timestamptz_at_zone": lambda a: f"make_timestamptz({a[0]}, {a[1]}, {a[2]}, {a[3]}, {a[4]}, {a[5]}, {q(a[6])})",
    "make_interval": lambda a: f"make_interval({a[0]}, {a[1]}, {a[2]}, {a[3]}, {a[4]}, {a[5]}, {a[6]})",
    "to_timestamp_epoch": lambda a: f"to_timestamp(({a[0]})::float8)",
    "timezone_tstz": lambda a: f"{q(a[1])}::timestamptz AT TIME ZONE {q(a[0])}",
    "timezone_ts": lambda a: f"{q(a[1])}::timestamp AT TIME ZONE {q(a[0])}",
    "timezone_ts_interval": lambda a: f"{q(a[1])}::timestamp AT TIME ZONE {q(a[0])}::interval",
    "timezone_tstz_interval": lambda a: f"{q(a[1])}::timestamptz AT TIME ZONE {q(a[0])}::interval",
    "isfinite_date": lambda a: f"isfinite({q(a[0])}::date)",
    "isfinite_timestamp": lambda a: f"isfinite({q(a[0])}::timestamp)",
    "isfinite_interval": lambda a: f"isfinite({q(a[0])}::interval)",
    # formatting
    "to_char_timestamp": lambda a: f"to_char({q(a[0])}::timestamp, {q(a[1])})",
    "to_char_timestamptz": lambda a: f"to_char({q(a[0])}::timestamptz, {q(a[1])})",
    "to_char_interval": lambda a: f"to_char({q(a[0])}::interval, {q(a[1])})",
    "to_date": lambda a: f"to_date({q(a[0])}, {q(a[1])})",
    "to_timestamp": lambda a: f"to_timestamp({q(a[0])}, {q(a[1])})",
    "to_timestamp_roundtrip": lambda a: f"to_timestamp(to_char({q(a[0])}::timestamptz, {q(a[1])}), {q(a[1])})",
    "to_date_roundtrip": lambda a: f"to_date(to_char({q(a[0])}::timestamp, {q(a[1])}), {q(a[1])})",
    # cross-type comparisons, OVERLAPS, aggregates
    "date_cmp_timestamp": lambda a: cmp_sql(f"{q(a[0])}::date", f"{q(a[1])}::timestamp"),
    "date_cmp_timestamptz": lambda a: cmp_sql(f"{q(a[0])}::date", f"{q(a[1])}::timestamptz"),
    "timestamp_cmp_timestamptz": lambda a: cmp_sql(f"{q(a[0])}::timestamp", f"{q(a[1])}::timestamptz"),
    "overlaps_ts": lambda a: "(" + ", ".join(ov(x) for x in a[:2]) + ") OVERLAPS (" + ", ".join(ov(x) for x in a[2:]) + ")",
    "interval_sum": lambda a: f"(SELECT sum(x) FROM unnest(string_to_array({q(a[0])}, '|')::interval[]) x)",
    "interval_avg": lambda a: f"(SELECT avg(x) FROM unnest(string_to_array({q(a[0])}, '|')::interval[]) x)",
    # session time zone parsing (SET TIME ZONE): render a fixed instant
    "session_tz": lambda a: f"'2020-07-01 12:00:00+00'::timestamptz",
}


def run(cases):
    """cases: list of (func, tz, args) -> list of results"""
    if not cases:
        return []
    values = []
    for i, (func, tz, args) in enumerate(cases):
        expr = SQL[func](args)
        values.append(f"({i}, {q(tz)}, {q(expr)})")
    results = [None] * len(cases)
    chunk = 2000
    for start in range(0, len(values), chunk):
        sql = (
            "SELECT id, replace(replace(pgdt_eval(tz, expr), E'\\n', '\\n'), E'\\t', '\\t') FROM (VALUES "
            + ",\n".join(values[start:start + chunk])
            + ") AS t(id, tz, expr) ORDER BY id;"
        )
        p = subprocess.run(PSQL + ["-At", "-X", "-F", "\x1f", "-f", "-"], input=sql, capture_output=True, text=True)
        if p.returncode != 0:
            print(p.stderr, file=sys.stderr)
            raise SystemExit(1)
        for line in p.stdout.splitlines():
            if not line:
                continue
            idx, res = line.split("\x1f", 1)
            results[int(idx)] = res
    return results


SPECIALS = ("now", "today", "tomorrow", "yesterday")
INPUT_FUNCS = ("date_in", "time_in", "timestamp_in", "timestamptz_in")


def fix_specials(func, args, r):
    """Non-deterministic special values must be rejected by pgdatetime."""
    import re
    if func not in INPUT_FUNCS or not r.startswith("OK"):
        return r
    for tok in re.findall(r"[a-z]+", str(args[0]).lower()):
        if tok in SPECIALS:
            return f'ERR 0A000 special value "{tok}" is not supported'
    return r


def write(category, cases):
    res = [fix_specials(f, a, r) for (f, _, a), r in zip(cases, run(cases))]
    os.makedirs(OUT, exist_ok=True)
    path = os.path.join(OUT, category + ".tsv")
    with open(path, "w") as f:
        for (func, tz, args), r in zip(cases, res):
            for a in args:
                assert "\t" not in str(a) and "\n" not in str(a), a
            f.write("\t".join([func, tz] + [str(a) for a in args] + ["=>", r]) + "\n")
    print(f"{category}: {len(cases)} cases", file=sys.stderr)


ZONES = [
    "UTC", "Europe/Warsaw", "America/New_York", "Asia/Kolkata", "Australia/Sydney", "America/St_Johns",
    "Pacific/Chatham", "Asia/Kathmandu", "Europe/London", "America/Sao_Paulo", "Europe/Moscow",
    "Africa/Casablanca", "Pacific/Apia", "America/Los_Angeles", "Asia/Tokyo",
]


def main():
    subprocess.run(PSQL + ["-X", "-q", "-c", EVAL_FN], check=True)
    cats = sys.argv[1:] or [name[4:] for name in globals() if name.startswith("cat_")]
    for c in cats:
        globals()["cat_" + c]()


import cases_io  # noqa: E402
import cases_ops  # noqa: E402
import cases_rand  # noqa: E402

for mod in (cases_io, cases_ops, cases_rand):
    for name in dir(mod):
        if name.startswith("cat_"):
            f = getattr(mod, name)
            globals()[name] = (lambda f: (lambda: f(write)))(f)

if __name__ == "__main__":
    main()
