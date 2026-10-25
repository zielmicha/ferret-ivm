"""Shared helper for generating golden test data from a real PostgreSQL 17.

The database must use the builtin locale provider with locale C.UTF-8 and
encoding UTF8, e.g.:

  initdb -D $D -U postgres --auth=trust --locale-provider=builtin \
         --builtin-locale=C.UTF-8 --locale=C -E UTF8

Connection parameters come from PGFUNCS_PGHOST (socket directory or host)
and PGFUNCS_PGPORT, which must be set. PGHOST/PGPORT are deliberately not
used so that a developer's default server is never picked up by accident.

Each case is a SQL statement with %s placeholders plus parameters. The
statement must return exactly one row with one column (cast it to text in
SQL). The recorded result is either {"result": <text or null>} or
{"error": {"sqlstate": ..., "message": ..., "detail": ...}}.

Output format: JSON lines, one object per case:
  {"fn": <name>, "args": [...], "result": ...}  or  {..., "error": {...}}
"""

import json
import os
import sys

import psycopg2

def connect():
    host = os.environ.get("PGFUNCS_PGHOST")
    port = os.environ.get("PGFUNCS_PGPORT")
    if not host or not port:
        sys.exit("set PGFUNCS_PGHOST and PGFUNCS_PGPORT to a PostgreSQL 17 server (see the module docstring)")
    conn = psycopg2.connect(
        host=host,
        port=int(port),
        user="postgres",
        dbname="postgres",
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SET bytea_output = 'hex'")
    cur.execute("SET standard_conforming_strings = on")
    cur.execute("SET extra_float_digits = 1")
    cur.execute("SHOW server_version")
    ver = cur.fetchone()[0]
    if not ver.startswith("17"):
        sys.exit(f"expected PostgreSQL 17, got {ver}")
    cur.execute("SELECT upper('é')")
    if cur.fetchone()[0] != "É":
        sys.exit("database is not using a Unicode-aware locale (builtin C.UTF-8 expected)")
    return conn


class Recorder:
    def __init__(self, path):
        self.conn = connect()
        self.cur = self.conn.cursor()
        self.path = path
        self.records = []

    def eval(self, sql, params=()):
        """Evaluate one statement; return ("ok", value) or ("err", dict).

        Placeholders followed by a cast ("%s::int4") are parenthesized so that
        negative numbers bind correctly ("-2147483648::int4" would otherwise
        parse as -(2147483648::int4)).
        """
        sql = sql.replace("%s::", "(%s)::")
        try:
            self.cur.execute(sql, params)
            rows = self.cur.fetchall()
            if len(rows) != 1 or len(rows[0]) != 1:
                raise RuntimeError(f"statement must return one row/column: {sql} -> {rows}")
            v = rows[0][0]
            if v is not None and not isinstance(v, str):
                raise RuntimeError(f"cast the result to text: {sql} -> {v!r}")
            return ("ok", v)
        except psycopg2.Error as e:
            d = e.diag
            return ("err", {"sqlstate": e.pgcode, "message": d.message_primary, "detail": d.message_detail})

    def case(self, fn, sql, args, extra=None):
        """Record a case. `args` are passed as query parameters and stored verbatim."""
        kind, v = self.eval(sql, tuple(args))
        rec = {"fn": fn, "args": list(args)}
        if extra:
            rec.update(extra)
        if kind == "ok":
            rec["result"] = v
        else:
            rec["error"] = v
        self.records.append(rec)
        return rec

    def write(self):
        with open(self.path, "w", encoding="utf-8") as f:
            for r in self.records:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        print(f"wrote {len(self.records)} cases to {self.path}")
