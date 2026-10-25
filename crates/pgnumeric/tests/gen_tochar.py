#!/usr/bin/env python3
"""Generate golden test data for pgnumeric's to_char/to_number from a real
PostgreSQL 17 server.

Usage:
    PGHOST=/path/to/socketdir PGPORT=54660 PGUSER=postgres python3 gen_tochar.py
        [--out PATH] [--seed N] [--scale K]

Writes tests/golden/tochar.jsonl (or PATH), one JSON object per line:

    {"fn": F, "args": [value, format], "result": text-or-null}
    {"fn": F, "args": [value, format],
     "error": {"sqlstate": S, "message": M, "detail": D-or-null}}

F names the variant: "numeric", "int8", "int4", "int2", "float8" or
"float4" for to_char(value::F, format) (the value is the text of the
argument), or "to_number" for to_number(value, format). Note that
to_char(smallint, text) resolves to the double precision variant.

The session runs with lc_numeric and lc_monetary set to C. The random
generator is seeded, so re-running against the same server produces
identical data. A larger --scale and a different --seed give bigger
randomized runs that can be checked without committing them, e.g.
    PGNUMERIC_EXTRA_TOCHAR=/tmp/big.jsonl cargo test -p pgnumeric --test tochar_golden
"""
import argparse
import json
import os
import random
import sys

import psycopg2

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT = os.path.join(HERE, "golden", "tochar.jsonl")

EVAL_FN = r"""
CREATE OR REPLACE FUNCTION pg_temp.tc_eval(kind text, v text, f text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE r text; st text; msg text; det text;
BEGIN
  IF kind = 'numeric' THEN r := to_char(v::numeric, f);
  ELSIF kind = 'int8' THEN r := to_char(v::int8, f);
  ELSIF kind = 'int4' THEN r := to_char(v::int4, f);
  ELSIF kind = 'int2' THEN r := to_char(v::int2, f);
  ELSIF kind = 'float8' THEN r := to_char(v::float8, f);
  ELSIF kind = 'float4' THEN r := to_char(v::float4, f);
  ELSIF kind = 'to_number' THEN r := to_number(v, f)::text;
  ELSE RAISE EXCEPTION 'bad kind %', kind;
  END IF;
  RETURN json_build_object('result', r)::text;
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS st = RETURNED_SQLSTATE, msg = MESSAGE_TEXT,
                          det = PG_EXCEPTION_DETAIL;
  RETURN json_build_object('error', json_build_object(
      'sqlstate', st, 'message', msg, 'detail', nullif(det, '')))::text;
END $$;
"""

TO_CHAR_KINDS = ["numeric", "int8", "int4", "int2", "float8", "float4"]

rnd = random.Random()


def connect():
    host = os.environ.get("PGHOST")
    port = os.environ.get("PGPORT")
    if not host or not port:
        sys.exit("set PGHOST and PGPORT to a PostgreSQL 17 server")
    conn = psycopg2.connect(
        host=host,
        port=int(port),
        user=os.environ.get("PGUSER", "postgres"),
        dbname=os.environ.get("PGDATABASE", "postgres"),
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SHOW server_version")
    ver = cur.fetchone()[0]
    if not ver.startswith("17"):
        sys.exit(f"expected PostgreSQL 17, got {ver}")
    cur.execute("SHOW server_encoding")
    if cur.fetchone()[0] != "UTF8":
        sys.exit("the database must use UTF8")
    cur.execute("SET client_encoding = 'UTF8'")
    cur.execute("SET lc_numeric = 'C'")
    cur.execute("SET lc_monetary = 'C'")
    cur.execute(EVAL_FN)
    return cur


def evaluate(cur, cases):
    out = []
    for i in range(0, len(cases), 2000):
        chunk = cases[i : i + 2000]
        cur.execute(
            "SELECT pg_temp.tc_eval(k, v, f) FROM unnest(%s::text[], %s::text[], %s::text[])"
            " WITH ORDINALITY AS t(k, v, f, i) ORDER BY i",
            ([c[0] for c in chunk], [c[1] for c in chunk], [c[2] for c in chunk]),
        )
        out.extend(json.loads(r[0]) for r in cur.fetchall())
    return out


# ----------------------------------------------------------------------
# Values
# ----------------------------------------------------------------------

def rand_digits(n):
    return "".join(rnd.choice("0123456789") for _ in range(n))


NUMERIC_FIXED = [
    "0", "0.00", "1", "-1", "5", "-5", "12", "13", "21", "-22", "103", "111", "1234",
    "-1234", "0.5", "-0.5", "0.05", "0.001", "1.5", "2.5", "-2.5", "9.99", "9.995",
    "99.995", "-0.9999", "123.456", "-123.456", "4.31", "-34338492.215397047",
    "7799461.4119", "16397.038491", "93901.57763026", "-83028485", "74881",
    "-24926804.045047420", "12345678901234567890", "-12345678901234567890.12345",
    "1e20", "1e-20", "1e100", "1.23456789e-10", "3999", "4000", "3999.5", "0.4",
    "2147483647", "2147483648", "-2147483649", "NaN", "Infinity", "-Infinity",
]

INT8_FIXED = [
    "0", "1", "-1", "5", "-5", "12", "13", "111", "1234", "-1234", "3999", "4000",
    "123456789", "-123456789", "4567890123456789", "-4567890123456789",
    "9223372036854775807", "-9223372036854775808", "2147483647", "2147483648",
    "-2147483648", "-2147483649", "1000000000000000000", "10", "100", "999", "1000", "25",
]

INT4_FIXED = [
    "0", "1", "-1", "5", "-5", "12", "13", "21", "111", "1234", "-1234", "3999", "4000",
    "123456", "-123456", "123456789", "2147483647", "-2147483648", "10", "100", "1000",
    "999", "25", "35", "-25",
]

INT2_FIXED = ["0", "1", "-1", "12", "1234", "-1234", "32767", "-32768", "3999", "4000", "25"]

FLOAT8_FIXED = [
    "0", "-0", "1", "-1", "0.5", "-0.5", "1.5", "2.5", "-2.5", "0.125", "0.1", "0.2",
    "0.3", "1.005", "2.675", "12.34", "-12.34", "123.456", "1234.5678", "0.000123", "1e-5",
    "1e-10", "1e15", "1e16", "1e20", "1e22", "1e23", "1.5e100", "-1.5e100", "1e300",
    "1.7976931348623157e308", "5e-324", "2.2250738585072014e-308", "123456789012345",
    "1234567890123456", "12345678901234567", "999999999999999", "9999999999999999",
    "0.999999999999999", "3999", "4000", "3999.5", "3998.5", "0.4", "0.6", "25", "35",
    "NaN", "Infinity", "-Infinity", "2147483648", "-2147483649",
]

FLOAT4_FIXED = [
    "0", "-0", "1", "-1", "0.5", "1.5", "2.5", "-2.5", "0.1", "0.2", "1.005", "12.34",
    "-12.34", "123.456", "1234.5678", "123456", "1234567", "12345678", "0.000123", "1e-5",
    "1e10", "1e20", "3.4028235e38", "1.4e-45", "1.17549435e-38", "3999", "4000",
    "3999.5", "25", "35", "NaN", "Infinity", "-Infinity", "16777216", "16777217",
]


def rand_numeric():
    r = rnd.random()
    if r < 0.04:
        return rnd.choice(["NaN", "Infinity", "-Infinity"])
    sign = "-" if rnd.random() < 0.4 else ""
    if r < 0.12:
        mant = rand_digits(rnd.randint(1, 14)).lstrip("0") or "1"
        e = rnd.randint(-40, 40)
        return f"{sign}{mant[0]}.{mant[1:] or '0'}e{e}"
    nint = rnd.choice([0, 0, 1, 1, 2, 3, 4, 4, 5, 6, 7, 8, 9, 10, 12, 15, 16, 18, 20, 25, 30])
    nfrac = rnd.choice([0, 0, 0, 1, 2, 2, 3, 3, 4, 5, 6, 8, 10, 12, 15, 20, 30])
    ip = rand_digits(nint).lstrip("0") or "0"
    fp = rand_digits(nfrac)
    k = rnd.random()
    if k < 0.08:
        # rounding edges: all nines
        ip = "9" * nint or "0"
        fp = "9" * nfrac
    elif k < 0.16 and fp:
        # exact halves
        fp = fp[:-1] + "5"
    elif k < 0.24 and nfrac:
        # leading fractional zeros
        fp = "0" * rnd.randint(1, 8) + fp
    s = sign + ip
    if fp:
        s += "." + fp
    return s


def rand_int(bits):
    lo, hi = -(1 << (bits - 1)), (1 << (bits - 1)) - 1
    r = rnd.random()
    if r < 0.1:
        v = rnd.choice([0, 1, -1, lo, hi, lo + 1, hi - 1])
    elif r < 0.3:
        v = rnd.randint(-100, 4100)
    elif r < 0.45:
        # digit-count edges: 10^k - 1, 10^k
        k = rnd.randint(1, 19)
        v = 10 ** k - rnd.choice([0, 1])
        if rnd.random() < 0.4:
            v = -v
    else:
        v = rnd.randint(0, 1 << rnd.randint(0, bits - 1))
        if rnd.random() < 0.4:
            v = -v
    return str(max(lo, min(hi, v)))


def rand_float(f4=False):
    r = rnd.random()
    max_e = 38 if f4 else 308
    if r < 0.06:
        return rnd.choice(["NaN", "Infinity", "-Infinity", "-0", "0"])
    sign = "-" if rnd.random() < 0.4 else ""
    if r < 0.4:
        # short decimals, the usual use
        nint = rnd.choice([0, 1, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 16, 18])
        nfrac = rnd.choice([0, 0, 1, 2, 2, 3, 4, 5, 6, 8, 10])
        if f4:
            nint = min(nint, 9)
        ip = rand_digits(nint).lstrip("0") or "0"
        fp = rand_digits(nfrac)
        if fp and rnd.random() < 0.2:
            fp = fp[:-1] + "5"
        return sign + ip + ("." + fp if fp else "")
    if r < 0.8:
        # random magnitude, many significant digits
        nd = rnd.randint(1, 9 if f4 else 17)
        mant = rand_digits(nd).lstrip("0") or "1"
        e = rnd.randint(-12, 25) if rnd.random() < 0.7 else rnd.randint(-max_e + 3, max_e - 3)
        return f"{sign}{mant[0]}.{mant[1:] or '0'}e{e}"
    if r < 0.9:
        # integers, incl. Roman-numeral range and exact halves
        v = rnd.randint(0, 4100) + rnd.choice([0, 0, 0.5, 0.25])
        return sign + repr(v)
    if f4:
        return rnd.choice(["3.4e38", "1e-40", "1.5e-45", "1e-38", "8388608.5", "16777215"])
    return rnd.choice(["1e308", "1e-310", "4.9e-324", "1e-300", "9007199254740993",
                       "4503599627370495.5", "1e21", "123456789012345678901"])


def fixed_values(kind):
    return {
        "numeric": NUMERIC_FIXED,
        "int8": INT8_FIXED,
        "int4": INT4_FIXED,
        "int2": INT2_FIXED,
        "float8": FLOAT8_FIXED,
        "float4": FLOAT4_FIXED,
    }[kind]


def rand_value(kind):
    if kind == "numeric":
        return rand_numeric()
    if kind == "int8":
        return rand_int(64)
    if kind == "int4":
        return rand_int(32)
    if kind == "int2":
        return rand_int(16)
    if kind == "float8":
        return rand_float()
    return rand_float(f4=True)


def pick_value(kind):
    if rnd.random() < 0.35:
        return rnd.choice(fixed_values(kind))
    return rand_value(kind)


# ----------------------------------------------------------------------
# Format pictures
# ----------------------------------------------------------------------

CURATED = [
    # digits
    "9", "99", "999", "9999", "99999", "9999999999", "999999999999999999999",
    "0", "00", "000", "0000", "09", "099", "0999", "90", "990", "9990", "909", "0990",
    "9.9", "9.99", "99.99", "999.999", "9999.9999", ".9", ".99", ".999", "0.0", "0.00",
    "00.00", "0.99", "9.00", "99.0", "9.90", "9.09", "9.", "99.", "0.", "FM9.", "FM999.",
    "FM999.9", "FM999.99", "FM990.99", "FM0.00", "FM9.09", "FM99.00", "FM9999.990", "FM.99",
    "FM.00", ".00", "FM0", "FM9", "FM99", "FM9999.9999", "FM0000.0000", "FM9.999999999",
    "9999999999999999.999999999999999", "0999999999999999.999999999999999",
    "FM9999999999999999.999999999999999", "FM0999999999999999.999909999999999",
    "FM9999999990999999.099999999999999", "FM9999999999990999.990999999999999",
    "FM9999999999999999.099999999999999", "FM9999999999999999.99999999999999",
    "FM9999999999D9999900000000000000000", "0999999999999999", "FM0999999999999999",
    "FM9999999999999999.000", "FM9999999999999999.999",
    # separators
    "9,999", "9,999.99", "99,999,999.99", "9G999", "9G999D99", "9G999G999G999G999G999",
    "9G999G999G999G999G999D999G999G999G999G999", "9,999,999,999,999,999.999,999",
    "FM9,999,999.99", "FM9G999D99", "99D99", "D99", "9D", "9d99", "9g999", ",999", "9,,9",
    ",", "G", "9,", "9G", "FM,999", "0,000.00", "FM0,000.00", "9,9.9,9",
    # signs
    "S9999", "9999S", "S9.99", "9.99S", "S0000", "FMS9999", "FMS9.99", "S 9 9 9", "9S99",
    "99.9S9", "SG9999", "9999SG", "SG99.99", "99SG99", "999999SG9999999999",
    "S9999999999999999.999999999999999", "9999999999999999.999999999999999S",
    "S0999999999999999.999999999999999", "FMS9999999999999999.999999999999999",
    "S 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9",
    "FMS 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9",
    "MI9999", "9999MI", "MI99.99", "99.99MI", "FMMI9999", "FM9999MI", "9MI9",
    "MI9999999999.99", "PL9999", "9999PL", "PL99.99", "FMPL9999", "FM9999PL", "PL9999MI",
    "MI9999PL", "PLMI9999", "9999PLMI", "9999PR", "9.99PR", "99.99PR", "FM9999PR",
    "FM9.99PR", "0000PR", "FM9999999999999999THPR", "FM9999999999999999.999999999999999THPR",
    "9999999999999999.999PR", "9999999999999999.999999999999999PR", "s9999", "9999s",
    "sg9999", "mi9999", "pl9999", "9999pr", "SG9999999999999999.999999999999999th",
    "SG9999999999999999th", "FMSG9999.99", "FMMI9999.99", "FMPL9999.99", "FM9999.99MI",
    "FM9999.99S", "FMS0000.0000", "FM9999.00PR", "FM9,999.00",
    # currency
    "L9999", "L9,999.99", "9999L", "FML9999", "L9999999999999999.000",
    "L9999999999999999.099999999999999", "LL9", "l9", "9l", "L", "FML",
    # ordinals
    "9999TH", "9999th", "FM9999TH", "FM9999th", "99.99TH", "TH", "th", "9TH9", "FM99thth",
    "999TH", "THth", "0TH", "FM0TH", "9 TH", "S9TH", "9THS", "9THPR", "MI9TH",
    # V
    "9V9", "9V99", "99V999", "9V9999999999", "999V99", "FM9V99", "S9V99", "V99", "V", "9V",
    "0V99", "9V99S", "PL9V9", "9V9PR", "9V99999999999999999999", "99999V9999", "9V0",
    "9V9V9", "9V999999999", "9V" + "9" * 18, "9V" + "9" * 19, "9V" + "9" * 23,
    "9V" + "9" * 40, "9V" + "9" * 209, "9V" + "9" * 210, "9V" + "9" * 309,
    # EEEE
    "9EEEE", "9.9EEEE", "9.999EEEE", "99.99EEEE", ".99EEEE", "EEEE", "9.99eeee",
    "0.00EEEE", "9.9999999999999999999EEEE",
    "9.999999999999999999999999999999999999999EEEE", "9,9.9EEEE", "L9.9EEEE",
    "9.9THEEEE", '"x"9.9EEEE', '9.9EEEE"x"', "9D99EEEE", "000.000EEEE", "9.EEEE",
    "99999EEEE", "9.9EEEE ", "9.9EEEEx", "9." + "9" * 400 + "EEEE", "9." + "9" * 30 + "EEEE",
    # Roman numerals
    "RN", "rn", "FMRN", "FMrn", "9RN", "RN9", "RNTH", "RN.99", "SGRN", "RNSG", "RNMI",
    "RNPL", "RN,", "RNL", "RNRN", "RN rn", '"r:"RN', 'FM"["RN"]"', "RNG", "MIRN", "PLRN",
    "RNPR", "RN9.9", "RNth", "FMRNSG", "RNV9", "RNG9", "FMRN,G", "RNS",
    # keywords that print nothing, letters that are not keywords
    "B9999", "9999B", "C9999", "SP9999", "9999SP", "c9", "b9", "9sp", "9 abc", "E9", "9E",
    "P9", "9P", "M9", "R9", "T9", "F9", "9EE", "9EEE", "9EEEEE", "9Sp", "9sP", "9eE",
    "9 FM", "9FM", "B", "C", "SP", "9B9", "B.99",
    # quoting
    '"text"9999', '9999" text"',
    r'99999 "text" 9999 "9999" 999 "\"text between quote marks\"" 9999',
    r'f\"oo999', r'f\\"oo999', r'f\\oo999', r'f\oo999', "foo999", r'f"\\ool"999',
    r'f"\ool"999', 'f"ool"999', r'f"ool\"999', r'f"ool\\"999', '"unterminated', '"', '""',
    '"\\"', '9"9"9', '"9"', '"FM"9', "\\", "\\\\9", '9"', '"\\', '"é"9', '\\"9"',
    # other literal characters
    "9 9 9", "9-9-9", "9/9", "9:9", "abc9", "9xyz", "é999", "9€9", "99%", "#999", "~9",
    "9~", "日本9", "9\t9", "(999)", "[9.99]", "$999.99", "9.9%", " 9", "9 ", "  999  ",
    "9\n9", "FMFM999", "9FM99", "9.9.", "+9", "9+", "-9", "9-",
    # long pictures (beyond the format cache entry size)
    "9" * 50, "9" * 40 + "." + "9" * 40, "FM" + "9" * 30 + "." + "9" * 30, "0" * 60,
    ("9," * 20) + "9", '"' + "x" * 150 + '"9', "9" * 120 + "S",
]

ERROR_PICTURES = [
    "9.9.9", "9..9", "9D.9", "D9D", "9.9D",
    "9V9.9", "9V.99", "9.9V9", "V.9", ".9V",
    "S9S", "SS9", "MI9S", "PL9S", "SG9S", "9PRS", "9PR S",
    "S9MI", "S9PL", "S9SG", "S9PR", "MI9PR", "PL9PR", "SG9PR",
    "9PR9", "9PR0", "PR9", "PR0",
    "9.9EEEE9", "9EEEEEEEE", "9EEEE.9", "9EEEES", "9EEEEPR", "9.9EEEEMI", "9EEEEFM",
    "9EEEERN", "9EEEETH", "9EEEE,", "9EEEEL",
    "FM9EEEE", "S9EEEE", "MI9EEEE", "PL9EEEE", "SG9EEEE", "9PREEEE", "RNEEEE", "9V9EEEE",
    "B9EEEE", "EEEEEEEE", "eeeeEEEE", "9.9S EEEE",
]

# Tokens for random pictures, with relative weights.
TOKENS = [
    ("9", 30), ("0", 10), (".", 6), ("D", 2), ("d", 1), (",", 5), ("G", 2), ("g", 1),
    ("S", 3), ("s", 1), ("MI", 2), ("mi", 1), ("PL", 2), ("pl", 1), ("SG", 2), ("sg", 1),
    ("PR", 2), ("pr", 1), ("L", 2), ("l", 1), ("TH", 2), ("th", 2), ("V", 2), ("v", 1),
    ("EEEE", 2), ("eeee", 1), ("RN", 1), ("rn", 1), ("FM", 3), ("fm", 1), ("B", 1),
    ("C", 1), ("SP", 1), (" ", 3), ('"lit"', 1), ('"\\""', 1), ("x", 1), ("E", 1),
    ("€", 1), ("-", 1), ("/", 1), ("\\", 1),
]


def rand_token():
    total = sum(w for _, w in TOKENS)
    x = rnd.uniform(0, total)
    for t, w in TOKENS:
        x -= w
        if x <= 0:
            return t
    return "9"


def rand_int_part(n):
    style = rnd.random()
    if style < 0.3:
        s = "9" * n
    elif style < 0.45:
        s = "0" * n
    elif style < 0.6:
        k = rnd.randint(0, n)
        s = "9" * k + "0" * (n - k)
    elif style < 0.8:
        k = rnd.randint(0, n)
        s = "0" * k + "9" * (n - k)
    else:
        s = "".join("0" if rnd.random() < 0.3 else "9" for _ in range(n))
    if n > 3 and rnd.random() < 0.35:
        sep = rnd.choice([",", ",", "G", "g"])
        out = []
        for i, c in enumerate(s):
            if i > 0 and (n - i) % 3 == 0:
                out.append(sep)
            out.append(c)
        s = "".join(out)
    return s


def rand_frac_part(n):
    style = rnd.random()
    if style < 0.4:
        return "9" * n
    if style < 0.6:
        return "0" * n
    if style < 0.8:
        k = rnd.randint(0, n)
        return "0" * k + "9" * (n - k)
    return "".join("0" if rnd.random() < 0.3 else "9" for _ in range(n))


LITERALS = ['"abc"', '" "', "x", " ", "$", "€", '"\\"q\\""', "#", "%", "-", "é"]


def rand_regular_picture():
    parts = []
    if rnd.random() < 0.3:
        parts.append(rnd.choice(["FM", "FM", "fm"]))
    if rnd.random() < 0.08:
        parts.append(rnd.choice(["L", "l"]))
    lead = rnd.choice(["", "", "", "", "", "S", "s", "SG", "MI", "PL", "PL"])
    trail = ""
    if not lead:
        trail = rnd.choice(["", "", "", "", "S", "SG", "MI", "PL", "PR", "pr", "PR", "MIPL"])
    parts.append(lead)
    if rnd.random() < 0.08:
        parts.append(rnd.choice(LITERALS))
    nint = rnd.choice([0, 1, 1, 2, 3, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 16, 20, 25])
    parts.append(rand_int_part(nint))
    if rnd.random() < 0.6:
        parts.append(rnd.choice([".", ".", ".", "D", "d"]))
        nfrac = rnd.choice([0, 1, 1, 2, 2, 2, 3, 4, 5, 6, 8, 10, 15, 16, 20])
        parts.append(rand_frac_part(nfrac))
    if rnd.random() < 0.08:
        parts.append(rnd.choice(["TH", "th"]))
    if rnd.random() < 0.08:
        parts.append(rnd.choice(["L", "l"]))
    parts.append(trail)
    if rnd.random() < 0.08:
        parts.append(rnd.choice(LITERALS))
    if rnd.random() < 0.05:
        # a sign token in the middle of the digits
        s = "".join(parts)
        i = rnd.randint(0, len(s))
        return s[:i] + rnd.choice(["S", "MI", "PL", "SG", " "]) + s[i:]
    return "".join(parts)


def rand_eeee_picture():
    parts = []
    if rnd.random() < 0.1:
        parts.append(rnd.choice(["FM", "S", "MI", "PL", "SG", "B", "L", "RN", "9V9", "TH"]))
    parts.append("9" * rnd.choice([0, 1, 1, 1, 2, 3]))
    if rnd.random() < 0.8:
        parts.append(rnd.choice([".", ".", "D"]))
        parts.append(rand_frac_part(rnd.choice([0, 1, 2, 3, 4, 6, 10, 15, 17, 20, 25, 40])))
    parts.append(rnd.choice(["EEEE", "EEEE", "eeee"]))
    if rnd.random() < 0.15:
        parts.append(rnd.choice(["9", "S", "PR", "MI", '"x"', " ", "EEEE", "TH", "x"]))
    return "".join(parts)


def rand_v_picture():
    parts = []
    if rnd.random() < 0.2:
        parts.append(rnd.choice(["FM", "S", "MI", "PL", "SG", "L"]))
    parts.append(rand_int_part(rnd.choice([0, 1, 2, 3, 4, 6, 8, 10])))
    parts.append(rnd.choice(["V", "V", "v"]))
    parts.append("9" * rnd.choice([0, 1, 1, 2, 3, 4, 5, 8, 9, 10, 12, 15, 18, 19, 20]))
    if rnd.random() < 0.2:
        parts.append(rnd.choice(["S", "MI", "PL", "PR", "TH", ".", "9", "0", "V9", " "]))
    return "".join(parts)


def rand_roman_picture():
    parts = []
    if rnd.random() < 0.4:
        parts.append(rnd.choice(["FM", "fm"]))
    for _ in range(rnd.randint(0, 2)):
        parts.append(rnd.choice(["9", ".", ",", "G", "L", "MI", "PL", "SG", "TH", " ", '"<"', "S"]))
    parts.append(rnd.choice(["RN", "rn"]))
    for _ in range(rnd.randint(0, 2)):
        parts.append(rnd.choice(["9", ".", ",", "G", "L", "MI", "PL", "SG", "TH", " ", '">"', "PR"]))
    return "".join(parts)


def rand_soup_picture():
    return "".join(rand_token() for _ in range(rnd.randint(1, 9)))


def rand_picture():
    r = rnd.random()
    if r < 0.05:
        return rand_roman_picture()
    if r < 0.13:
        return rand_eeee_picture()
    if r < 0.2:
        return rand_v_picture()
    if r < 0.28:
        return rand_soup_picture()
    return rand_regular_picture()


# ----------------------------------------------------------------------
# to_number inputs
# ----------------------------------------------------------------------

TO_NUMBER_FIXED = [
    # from PostgreSQL's regression tests
    ("-34,338,492", "99G999G999"), ("-34,338,492.654,878", "99G999G999D999G999"),
    ("<564646.654564>", "999999.999999PR"), ("0.00001-", "9.999999S"),
    ("5.01-", "FM9.999999S"), ("5.01-", "FM9.999999MI"),
    ("5 4 4 4 4 8 . 7 8", "9 9 9 9 9 9 . 9 9"), (".01", "FM9.99"),
    (".0", "99999999.99999999"), ("0", "99.99"), (".-01", "S99.99"), (".01-", "99.99S"),
    (" . 0 1-", " 9 9 . 9 9 S"), ("34,50", "999,99"), ("123,000", "999G"),
    ("123456", "999G999"), ("$1234.56", "L9,999.99"), ("$1234.56", "L99,999.99"),
    ("$1,234.56", "L99,999.99"), ("1234.56", "L99,999.99"), ("1,234.56", "L99,999.99"),
    ("42nd", "99th"),
    # more
    ("12345", "99.999"), ("abc", "999"), ("", "999"), ("12", ""), ("", ""), ("-", "9"),
    ("+", "9"), ("-.", "9.9"), (".", "9.9"), ("1.5", "9"), ("1.5", "9.9"), ("1.55", "9.9"),
    ("-1.5", "S9.9"), ("1.5-", "9.9S"), ("1.5+", "9.9S"), ("+1.5", "S9.9"),
    ("<1.5>", "9.9PR"), ("1.5", "9.9PR"), ("<1.5", "9.9PR"), ("1e5", "9999"),
    ("12th", "99TH"), ("12 th", "99TH"), ("1,234", "9G999"), ("1,234", "FM9G999"),
    ("1 234", "9G999"), ("1.234,5", "9G999D9"), ("12", "9V9"), ("123", "9V99"),
    ("123", "99V9"), ("1.5", "9.9EEEE"), ("V", "RN"), ("XII", "rn"), ("12-", "99MI"),
    ("-12", "MI99"), ("12+", "99PL"), ("+12", "PL99"), ("12-", "99SG"), ("-12", "SG99"),
    ("$12", "L99"), ("€12", "L99"), ("é12", "999"), ("12é", "99"), ("日本12", "L99"),
    ("1 2", "99"), ("1  2", "9 9"), ("  12", "99"), ("12  ", "99"), ("0012", "9999"),
    ("12.345", "99.99"), ("12.3", "99.999"), (".5", ".9"), ("5.", "9."), ("5.", "9.9"),
    ("1,2,3", "9,9,9"), ("1,2,3", "999"), ("123", "9,9,9"), ("-0", "9"), ("-0.0", "9.9"),
    ("99999999999999999999", "99999999999999999999"), ("1" * 40, "9" * 40),
    ("123456789.123456789", "999999999.999999999"), ("1-2", "9MI9"), ("12-", "9MI9"),
    ("12", "FM99"), ("12", "FM9"), ("1,000.5", "FM9,999.99"), ("1000.5", "FM9,999.99"),
    ("<12>", "99PR"), ("<12", "99PR"), ("12>", "99PR"), ("(12)", "99PR"), ("-12", "99PR"),
    ("12", "99S"), ("12-", "S99"), ("-12", "99S"), ("+-12", "S99"), ("12", "0099"),
    ("1.2.3", "9.9.9"), ("12", "9.9.9"), ("12", "9V9.9"), ("12", "S9S"),
    ("1st", "9th"), ("2nd", "9TH"), ("11", "9TH9"), ("1x2", "9x9"), ("1€2", "9€9"),
    ("1é2", "9 9"), ("12", '"ab"99'), ("ab12", '"ab"99'), ("abc12", '"ab"99'),
    ("1.5", "9D9"), ("1,5", "9D9"), ("1.5", "9G9"), ("1,5", "9G9"), ("12", "L99"),
    ("$ 12", "L99"), ("$$12", "LL99"), ("1 000", "9 999"), ("12.5", "99V9"),
    ("125", "9V99"), ("-125", "S9V99"), ("1.25", "9V99"), ("0.00", "9.99"),
    ("1e-5", "9.99999"), ("NaN", "999"), ("Infinity", "99999999"), ("12", "B99"),
    ("12", "C99"), ("12", "99SP"), ("12", "99EEEE"), ("12", "RN"), ("-.5", "S.9"),
    ("5-", "9MI"), ("5", "9MI"), ("- 5", "MI9"), ("5 -", "9 MI"), ("1-", "9SG"),
    ("1+", "9SG"), ("+1", "SG9"),
]

DATA_CHARS = "0123456789.,+-<> "
NOISE_CHARS = "0123456789.,+-<> $abcé€thsndr()"


def mutate(s):
    if not s:
        return rnd.choice(NOISE_CHARS)
    i = rnd.randint(0, len(s) - 1)
    op = rnd.random()
    if op < 0.3:
        return s[:i] + s[i + 1 :]
    if op < 0.6:
        return s[:i] + rnd.choice(NOISE_CHARS) + s[i:]
    if op < 0.8:
        return s[:i] + rnd.choice(NOISE_CHARS) + s[i + 1 :]
    return s.strip() if rnd.random() < 0.5 else " " + s + " "


def rand_number_text():
    sign = rnd.choice(["", "", "-", "+"])
    ip = rand_digits(rnd.choice([0, 1, 2, 3, 4, 6, 8, 12]))
    s = sign + ip
    if rnd.random() < 0.5:
        s += "." + rand_digits(rnd.choice([0, 1, 2, 3, 5, 8]))
    if rnd.random() < 0.15:
        s += rnd.choice(["-", "+", "S", " ", "th", ">"])
    if rnd.random() < 0.1:
        s = rnd.choice(["<", "$", " ", "(", "x"]) + s
    return s


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def build_cases(scale):
    cases = []
    seen = set()

    def add(kind, v, f):
        key = (kind, v, f)
        if key not in seen:
            seen.add(key)
            cases.append(key)

    # empty picture and pure literals
    for kind in TO_CHAR_KINDS:
        add(kind, "12", "")
        add(kind, "-12", '"abc"')
        add(kind, "12", "abc")

    # curated pictures: a sample of the fixed values of every type plus a
    # random value (fewer for the very long pictures)
    per_picture = {"numeric": 7, "int8": 3, "int4": 3, "int2": 2, "float8": 7, "float4": 3}
    for f in CURATED:
        for kind in TO_CHAR_KINDS:
            vals = fixed_values(kind)
            n = per_picture[kind] * scale if len(f) <= 60 else scale
            for v in rnd.sample(vals, min(n, len(vals))):
                add(kind, v, f)
            for _ in range(scale):
                add(kind, rand_value(kind), f)

    # invalid pictures
    for f in ERROR_PICTURES:
        for kind in TO_CHAR_KINDS:
            add(kind, pick_value(kind), f)
        add("to_number", "12", f)

    # type-specific errors and edge cases
    for v in ["NaN", "Infinity", "-Infinity", "1e10", "-3999.4", "3999.4", "3999.5", "0.5",
              "0.49", "-0.5", "2147483647.4", "2147483647.5", "-2147483648.5", "1e131071"]:
        for f in ["RN", "FMrn", "999TH", "999th", "FM999th", "999.9TH", "9EEEE", "9.99EEEE"]:
            add("numeric", v, f)
    for v in ["2147483648", "-2147483649", "9223372036854775807", "3000", "0", "-1"]:
        for f in ["RN", "FMRN", "rn"]:
            add("int8", v, f)
    for v in ["1", "-1", "9", "10", "922337203685477580", "-922337203685477580",
              "922337203685477581", "1000000000", "9223372036854775807"]:
        for f in ["9V9", "9V99", "9V999999999", "9V9999999999", "9V" + "9" * 18,
                  "9V" + "9" * 19, "99V9", "FM9V9"]:
            add("int8", v, f)
            add("int4", str(max(-(1 << 31), min((1 << 31) - 1, int(v)))), f)
    for v in ["NaN", "Infinity", "-Infinity", "-0", "4000", "3999.5", "3998.5", "0.5",
              "1.5", "2147483648", "1e300", "5e-324"]:
        for f in ["RN", "FMRN", "999TH", "FM9999th", "9.99EEEE", "9EEEE", "9V9", "999.99",
                  "FM999.99", "9999999999", "99999999999.999"]:
            add("float8", v, f)
            add("float4", v if v not in ("1e300", "5e-324") else "1e30", f)
    for v in ["0", "1", "-1", "5", "1e-300", "1e300", "NaN"]:
        for f in ["9V" + "9" * k for k in (22, 23, 24, 37, 38, 39, 45, 209, 210, 211, 308, 309, 310)]:
            add("float8", v, f)
            add("float4", v.replace("300", "30"), f)

    # random pictures
    for _ in range(600 * scale):
        f = rand_picture()
        for kind, n in (("numeric", 3), ("int8", 1), ("int4", 1), ("int2", 1),
                        ("float8", 2), ("float4", 1)):
            for _ in range(n):
                add(kind, pick_value(kind), f)

    # to_number: fixed cases, random texts against random pictures
    for v, f in TO_NUMBER_FIXED:
        add("to_number", v, f)
    for f in CURATED:
        for _ in range(2 * scale):
            add("to_number", rand_number_text(), f)
    for _ in range(800 * scale):
        add("to_number", rand_number_text(), rand_picture())
    return cases


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--seed", type=int, default=20261025)
    ap.add_argument("--scale", type=int, default=1)
    args = ap.parse_args()
    rnd.seed(args.seed)

    cur = connect()
    cases = build_cases(args.scale)
    results = evaluate(cur, cases)

    # to_number round trips of to_char output, plus mutated variants
    extra = []
    for (kind, v, f), r in zip(cases, results):
        if kind == "to_number" or "result" not in r or r["result"] is None:
            continue
        if rnd.random() < 0.2:
            extra.append(("to_number", r["result"], f))
            if rnd.random() < 0.5:
                extra.append(("to_number", mutate(r["result"]), f))
    seen = set(cases)
    extra = [c for c in dict.fromkeys(extra) if c not in seen]
    cases += extra
    results += evaluate(cur, extra)

    bad_cast = ("invalid input syntax for type bigint", "invalid input syntax for type integer",
                "invalid input syntax for type smallint", "invalid input syntax for type real",
                "invalid input syntax for type double precision", "is out of range for type")
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    counts = {}
    with open(args.out, "w", encoding="utf-8") as fh:
        for (kind, v, f), r in zip(cases, results):
            if "error" in r and kind != "to_number" and any(b in r["error"]["message"] for b in bad_cast):
                sys.exit(f"argument cast failed, fix the generator: {kind} {v!r}: {r}")
            rec = {"fn": kind, "args": [v, f]}
            rec.update(r)
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            counts[kind] = counts.get(kind, 0) + 1
    print(f"wrote {len(cases)} cases to {args.out}: {counts}", file=sys.stderr)


if __name__ == "__main__":
    main()
