#!/usr/bin/env python3
"""Generate golden test data for pgnumeric from a real PostgreSQL 17 server.

Usage:
    PGHOST=/path/to/socketdir PGPORT=54401 PGUSER=postgres python3 gen_golden.py

Writes tests/golden/*.tsv. Each line is tab separated (with COPY-style
backslash escapes for \\, tab, newline and carriage return):

    op  arg1  arg2  arg3  ok   result
    op  arg1  arg2  arg3  err  sqlstate  message  detail

The random generator is seeded, so re-running against the same server
produces identical files.
"""
import io
import math
import re
import os
import random
import struct
import sys

import psycopg2

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "golden")

TRY_EVAL = r"""
CREATE OR REPLACE FUNCTION pg_temp.try_eval(expr text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE r text; st text; msg text; det text;
BEGIN
  EXECUTE 'SELECT (' || expr || ')::text' INTO r;
  RETURN 'ok' || chr(31) || coalesce(r, '<NULL>');
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS st = RETURNED_SQLSTATE, msg = MESSAGE_TEXT, det = PG_EXCEPTION_DETAIL;
  RETURN 'err' || chr(31) || st || chr(31) || msg || chr(31) || coalesce(det, '');
END $$;
"""

conn = psycopg2.connect(
    host=os.environ.get("PGHOST"),
    port=int(os.environ.get("PGPORT", "5432")),
    user=os.environ.get("PGUSER", "postgres"),
    dbname=os.environ.get("PGDATABASE", "postgres"),
)
conn.autocommit = True
cur = conn.cursor()
cur.execute("SET extra_float_digits = 1")
cur.execute("SET client_encoding = 'UTF8'")
cur.execute(TRY_EVAL)

rnd = random.Random(20261024)


REPEAT = re.compile(r"<(.)\*(\d+)>")


def expand(s):
    """Expand the compact repeat notation <c*n> (n copies of character c)."""
    return REPEAT.sub(lambda m: m.group(1) * int(m.group(2)), s)


def lit(s):
    s = expand(s)
    return "'" + s.replace("'", "''") + "'"


def num(s):
    return lit(s) + "::numeric"


def esc(s):
    return (
        s.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def evaluate(exprs):
    res = []
    for i in range(0, len(exprs), 1000):
        chunk = exprs[i : i + 1000]
        cur.execute(
            "SELECT pg_temp.try_eval(e) FROM unnest(%s::text[]) WITH ORDINALITY AS t(e, i) ORDER BY i",
            (chunk,),
        )
        res.extend(r[0] for r in cur.fetchall())
    return res


def write(name, cases):
    """cases: list of (op, [args], sql_expr)"""
    results = evaluate([c[2] for c in cases])
    path = os.path.join(OUT, name + ".tsv")
    with open(path, "w", encoding="utf-8") as f:
        for (op, args, _), r in zip(cases, results):
            args = list(args) + [""] * (3 - len(args))
            parts = r.split("\x1f")
            f.write("\t".join(esc(x) for x in [op] + args + parts) + "\n")
    print(f"{name}: {len(cases)} cases", file=sys.stderr)


# ----------------------------------------------------------------------
# Value generators
# ----------------------------------------------------------------------

def rand_digits(n):
    return "".join(rnd.choice("0123456789") for _ in range(n))


def rand_num(maxint=30, maxfrac=30, allow_sci=True):
    sign = "-" if rnd.random() < 0.4 else ""
    r = rnd.random()
    if allow_sci and r < 0.12:
        # scientific notation with wide exponent range
        mant = rand_digits(rnd.randint(1, 20)).lstrip("0") or "1"
        if len(mant) > 1:
            mant = mant[0] + "." + mant[1:]
        e = rnd.randint(-60, 60)
        return f"{sign}{mant}e{e}"
    nint = rnd.choice([0, 0, 1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 15, 18, 20, 25, maxint])
    nfrac = rnd.choice([0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 16, 20, maxfrac])
    nint = min(nint, maxint)
    nfrac = min(nfrac, maxfrac)
    ip = rand_digits(nint).lstrip("0") or "0"
    fp = rand_digits(nfrac)
    if nfrac and rnd.random() < 0.3:
        # leading zeros after the point
        z = rnd.randint(1, 12)
        fp = "0" * z + fp
    if nfrac and rnd.random() < 0.2:
        fp = fp + "0" * rnd.randint(1, 5)
    s = sign + ip
    if fp:
        s += "." + fp
    return s


SPECIAL_VALUES = [
    "0", "0.0", "0.000", "-0", "1", "-1", "2", "-2", "0.5", "-0.5", "1.5", "2.5",
    "9", "10", "99", "100", "9999", "10000", "10001", "99999999", "100000000",
    "0.0001", "0.00001", "0.1", "0.9", "1.1", "0.99999999", "1.00000001",
    "3.14159265358979323846264338327950288", "-2.71828182845904523536",
    "123456789012345678901234567890", "0.000000000000000000000000000001",
    "9999999999999999999", "9223372036854775807", "-9223372036854775808",
    "9223372036854775808", "-9223372036854775809", "2147483647", "-2147483648",
    "2147483648", "32767", "-32768", "32768", "1e100", "-1e-100", "1.5e-20",
    "NaN", "Infinity", "-Infinity",
]


def mixed_values(n, **kw):
    vals = list(SPECIAL_VALUES)
    while len(vals) < n:
        vals.append(rand_num(**kw))
    return vals


# ----------------------------------------------------------------------
# numeric_in / numeric_out
# ----------------------------------------------------------------------

def gen_in():
    inputs = [
        "0", "-0", "+0", "0.0", "-0.00", "00012.3400", ".5", "5.", "-.5", "+.5", "-5.",
        "1e3", "1E-3", "1.5e+2", "1.5e-2", "-1.5E2", ".5e1", "5.e1", "1e0", "1e-0",
        "  12  ", "\t\n12\r\n", " \x0b\x0c12\x0b\x0c ", "1_000", "1_000.000_1", "1_2_3",
        "0x1F", "0X1f", "-0x80", "+0xff", "0o17", "0O17", "-0o17", "0b101", "0B101", "-0b0",
        "0x_1", "0x", "0x_", "0x1_", "0x1__2", "0xg", "0o8", "0b2", "0b", "0o", "0x1.5",
        "0x7FFFFFFFFFFFFFFF", "0x8000000000000000", "-0x8000000000000000",
        "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", "0o" + "7" * 60, "0b" + "1" * 130,
        "0x" + "f" * 300, "0x 1", " 0x10 ", "00x10", "0x10e2", "0x<f*108853>", "0b1<0*435412>", "0o1<0*145206>",
        "1__0", "_1", "1_", "1._5", "1_.5", "1e1_0", "1e_10", "1e1__0", "1_e10", "1.5_5", "1.5__5",
        "1e", "1e+", "1e-", "e1", ".", "-", "+", "", " ", "abc", "1.2.3", "1-", "--1",
        "+-1", "-+1", "1..2", "1 2", "1,5", "1.5x", "x1.5", "١٢", " 12",
        "NaN", "nan", "nAn", "  NaN  ", "-NaN", "+NaN", "NaNx", "NaN 1", "na",
        "Infinity", "-Infinity", "+Infinity", "inf", "-inf", "+INF", "infinity ", " -INFINITY ",
        "Infinit", "infinityx", "infx", "-in", "Inf1",
        "1e131072", "-1e131072", "0.1e131073", "<9*131073>", "1e-16384", "0e-16383",
        "0e-16384", "0.0e-16383", "0e1000000000", "1e-16383", "<0*131072>1", "0.<0*16384>",
        "1e1073741823", "1e1073741824", "1e-1073741824", "0e1073741823", "1e2147483647",
        "1e-1073741823",
        "123456789012345678901234567890123456789012345678901234567890.12345678901234567890",
        "0." + "0" * 100 + "1", "1" + "0" * 200, "-" + "9" * 120 + "." + "9" * 120,
        "0.0000000000000000000000000001000", "1000000000000000000000000000.0000",
        "3.", "3.0", "3.00000", "-0.0000", "+.000", "12345678.12345678",
        "1e-4", "1e-5", "1.23456789e-7", "9.99e2", "99.99e-2", "1.e3", "1.0e3",
    ]
    for _ in range(300):
        inputs.append(rand_num(maxint=40, maxfrac=40))
    cases = [("in", [s], num(s)) for s in inputs]
    write("in", cases)


# ----------------------------------------------------------------------
# Arithmetic
# ----------------------------------------------------------------------

def gen_arith():
    specials = ["NaN", "Infinity", "-Infinity", "0", "0.00", "1", "-1", "2.5", "-3.75"]
    pairs = [(a, b) for a in specials for b in specials]
    for _ in range(500):
        pairs.append((rand_num(), rand_num()))
    # same magnitude, different scales
    for _ in range(60):
        a = rand_num(maxint=10, maxfrac=10, allow_sci=False)
        pairs.append((a, a + "000"))
        pairs.append((a, "-" + a.lstrip("-")))
    # select_div_scale specific cases
    div_pairs = [
        ("1", "3"), ("2", "3"), ("10", "3"), ("1", "7"), ("1", "9999"), ("1", "10000"),
        ("9999", "1"), ("9999", "9999"), ("1", "1"), ("10000", "9999"), ("9999", "10000"),
        ("1.000000", "3"), ("1", "3.000000"), ("123456789", "0.0001"), ("0.0001", "123456789"),
        ("1", "0"), ("0", "0"), ("0.00", "1"), ("-1", "0"), ("1e-20", "3"), ("1e20", "3"),
        ("1e-100", "7e100"), ("7e100", "1e-100"), ("1", "123456789012345678901234567890"),
        ("123456789012345678901234567890", "987654321"), ("5", "2"), ("-5", "2"), ("5", "-2"),
        ("-5", "-2"), ("0.5", "2"), ("1", "0.5"), ("2", "0.5"), ("1", "0.0003"),
        ("12345.6789", "0.000012345"), ("1" + "0" * 40, "3"), ("1", "3" + "0" * 40),
        ("2", "1.5"), ("1.5", "2"), ("100", "1.5"), ("4999", "5000"), ("5001", "5000"),
        ("1", "1.00000000000000000001"), ("99999999999999999999", "100000000000000000000"),
        ("1", "9999999999999999"), ("1", "99999999.99999999"), ("12345678901234567890", "1.1"),
        ("1", "1e-20"), ("1e-1000", "3"), ("1", "1e1000"),
        ("0.1", "0.3"), ("1.0", "3.0"), ("10.0", "4.00"),
    ]
    pairs += div_pairs
    cases = []
    for a, b in pairs:
        cases.append(("add", [a, b], f"{num(a)} + {num(b)}"))
        cases.append(("sub", [a, b], f"{num(a)} - {num(b)}"))
        cases.append(("mul", [a, b], f"{num(a)} * {num(b)}"))
        cases.append(("div", [a, b], f"{num(a)} / {num(b)}"))
        cases.append(("div_trunc", [a, b], f"div({num(a)}, {num(b)})"))
        cases.append(("mod", [a, b], f"mod({num(a)}, {num(b)})"))
        cases.append(("cmp", [a, b], f"numeric_cmp({num(a)}, {num(b)})"))
    # big multiplications and dscale overflow in mul
    big = [
        ("mul", "0.<0*4000>1", "0.<0*4000>1"),
        ("mul", "0.<0*9000>3", "0.<0*9000>7"),
        ("mul", "1<0*70000>", "1<0*70000>"),
        ("mul", "<9*66000>", "<9*66000>"),
        ("mul", "1e131071", "10"),
        ("mul", "9.9e131071", "-9.9e131071"),
        ("mul", "1e-16383", "1e-16383"),
        ("mul", "1e-16383", "0.5"),
        ("mul", "<9*65536>", "<9*65537>"),
        ("add", "<9*131072>", "1"),
        ("sub", "-<9*131072>", "1"),
        ("add", "<9*131072>", "-<9*131072>"),
        ("div", "1e131071", "0.1"),
        ("div", "1e131071", "1e-10"),
        ("div", "1e-16383", "3"),
        ("div", "1", "1e131071"),
        ("div", "1", "0.<0*16380>1"),
        ("mod", "1e131071", "7"),
        ("mod", "<9*131072>", "1234567"),
        ("div_trunc", "1e131071", "<9*131071>"),
    ]
    for op, a, b in big:
        e = {"add": "{} + {}", "sub": "{} - {}", "mul": "{} * {}", "div": "{} / {}",
             "mod": "mod({}, {})", "div_trunc": "div({}, {})"}[op]
        cases.append((op, [a, b], e.format(num(a), num(b))))
    write("arith", cases)


# ----------------------------------------------------------------------
# Unary functions, rounding, scale
# ----------------------------------------------------------------------

def gen_unary():
    vals = mixed_values(400)
    vals += [
        "0.5", "-0.5", "1.5", "-1.5", "2.5", "0.05", "0.049999", "9999.5", "-9999.5",
        "99999999.995", "0.9999999999", "-0.99999", "4.5e-10", "123.456789", "9.9999",
        "99.995", "0.0000000001", "1e-16383", "9" * 40 + ".9", "0." + "9" * 30,
    ]
    cases = []
    for v in vals:
        for op, e in [
            ("ceil", f"ceil({num(v)})"),
            ("floor", f"floor({num(v)})"),
            ("abs", f"abs({num(v)})"),
            ("neg", f"-({num(v)})"),
            ("sign", f"sign({num(v)})"),
            ("scale", f"scale({num(v)})"),
            ("min_scale", f"min_scale({num(v)})"),
            ("trim_scale", f"trim_scale({num(v)})"),
        ]:
            cases.append((op, [v], e))
        scales = sorted(set([0, 1, 2, -1, -2, rnd.randint(-8, 25), rnd.randint(-8, 25)]))
        for s in scales:
            cases.append(("round", [v, str(s)], f"round({num(v)}, {s})"))
            cases.append(("trunc", [v, str(s)], f"trunc({num(v)}, {s})"))
    extreme = ["123.456", "-987654321.123", "0", "9" * 50, "0.00001"]
    for v in extreme:
        for s in [-131073, -131072, -131071, -100000, -2147483648, 1000, 1001, -40, -50,
                  -51, 100]:
            cases.append(("round", [v, str(s)], f"round({num(v)}, {s})"))
            cases.append(("trunc", [v, str(s)], f"trunc({num(v)}, {s})"))
    cases.append(("round", ["-1.5", "16384"], f"round({num('-1.5')}, 16384)"))
    cases.append(("trunc", ["-1.5", "2147483647"], f"trunc({num('-1.5')}, 2147483647)"))
    # rounding that overflows
    for v, s in [("<9*131072>", -1), ("<9*131072>.5", 0), ("-<9*131072>.5", 0),
                 ("<9*131071>", -131072), ("<9*131072>", -131072), ("<9*131072>", -131073),
                 ("5<0*131071>", -131072), ("4<9*131071>", -131072), ("5<0*131071>", -131073)]:
        cases.append(("round", [v, str(s)], f"round({num(v)}, {s})"))
    cases.append(("ceil", ["<9*131072>.5"], f"ceil({num('<9*131072>.5')})"))
    cases.append(("floor", ["-<9*131072>.5"], f"floor({num('-<9*131072>.5')})"))
    write("unary", cases)


# ----------------------------------------------------------------------
# Transcendental functions
# ----------------------------------------------------------------------

def pos_values(n):
    vals = []
    while len(vals) < n:
        v = rand_num(maxint=20, maxfrac=20).lstrip("-")
        try:
            if float(v) == 0.0:
                continue
        except ValueError:
            pass
        vals.append(v)
    return vals


def gen_math():
    cases = []
    sqrt_vals = [
        "0", "0.000", "1", "2", "3", "4", "0.25", "0.5", "2.0000000000", "10", "100",
        "1e100", "1e-100", "1e101", "1e-101", "9999", "10000", "99999999", "0.0001",
        "0.00001", "1.21", "152415787532388367501905199875019052100", "1e-16383", "1e2001",
        "2." + "0" * 2000, "0." + "0" * 3000 + "2", "123456789012345678901234567890.123",
        "-1", "-0.0001", "NaN", "Infinity", "-Infinity", "1.5e-7", "6.25", "0.0625",
        "1" + "0" * 99, "1" + "0" * 100, "2" + "0" * 39, "7e-30",
    ] + pos_values(150)
    for v in sqrt_vals:
        cases.append(("sqrt", [v], f"sqrt({num(v)})"))

    exp_vals = [
        "0", "1", "-1", "0.5", "2", "10", "-10", "0.01", "-0.01", "0.0100001", "0.001",
        "1e-20", "-1e-20", "100", "-100", "123.456", "-123.456", "700", "709.78", "1000",
        "1000.5", "-1000", "2302.5", "-2302.5", "2303", "5999", "-5999", "6000", "-6000",
        "5999.999", "1e10", "-1e10", "1.5", "1.50", "1.500000", "0.693147180559945309417232",
        "NaN", "Infinity", "-Infinity", "0.0000", "3.0000000000000000000000", "-0.5",
        "230.25850929940457", "-230.25850929940457", "8", "16", "32", "64", "1.0e-1000",
        "4605.17", "-4605.17", "-4700", "4700", "301.5", "-301.5",
    ] + [rand_num(maxint=3, maxfrac=12, allow_sci=False) for _ in range(120)]
    for v in exp_vals:
        cases.append(("exp", [v], f"exp({num(v)})"))

    ln_vals = [
        "1", "2", "10", "0.5", "0.1", "0.9", "1.1", "0.95", "1.05", "0.89999", "1.10001",
        "1.00000000001", "0.99999999999", "1.000000000000000000000000000001", "100",
        "1e100", "1e-100", "1e1000", "1e-1000", "1e-16383", "9.99e131071", "0", "-1",
        "-0.5", "NaN", "Infinity", "-Infinity", "2.718281828459045235360287471352662497757",
        "0.0001", "12345.6789", "2.0000000000000000000000000000", "3.000", "1e10",
        "7.389056098930650227230427460575", "1." + "0" * 50 + "1", "0." + "9" * 40,
        "0.9" + "0" * 30, "1.1" + "0" * 30,
    ] + pos_values(150)
    for v in ln_vals:
        cases.append(("ln", [v], f"ln({num(v)})"))
        cases.append(("log10", [v], f"log({num(v)})"))

    log_pairs = [
        ("2", "1024"), ("10", "0.001"), ("0.5", "8"), ("1", "10"), ("10", "1"),
        ("2.5", "6.25"), ("10", "1e100"), ("1e-100", "1e100"), ("2", "3"), ("3", "2"),
        ("10", "10"), ("1.1", "1.21"), ("0.9", "0.81"), ("100", "1e-200"),
        ("2", "0"), ("0", "2"), ("-2", "4"), ("2", "-4"), ("NaN", "2"), ("2", "NaN"),
        ("Infinity", "2"), ("2", "Infinity"), ("Infinity", "Infinity"),
        ("-Infinity", "2"), ("2", "-Infinity"), ("0", "Infinity"), ("Infinity", "0"),
        ("1.000001", "1.000002"), ("2.000000000", "8.0000"), ("1e-16383", "2"),
        ("1.00000000000000000000001", "2"), ("2", "1.00000000000000000000001"),
    ]
    vals = pos_values(80)
    for i in range(0, len(vals) - 1, 2):
        log_pairs.append((vals[i], vals[i + 1]))
    for b, x in log_pairs:
        cases.append(("log", [b, x], f"log({num(b)}, {num(x)})"))

    pow_pairs = [
        ("2", "10"), ("2", "-10"), ("-2", "3"), ("-2", "-3"), ("-2", "2"), ("0", "0"),
        ("0", "5"), ("0", "-1"), ("0", "-1.5"), ("0", "1.5"), ("0.0", "0"), ("1.5", "2"),
        ("1.0001", "100000"), ("10", "131072"), ("10", "131072.5"), ("10", "-16383"),
        ("9.99", "132000"), ("-10", "131073"), ("0.99", "-31000000"), ("10", "-131072"),
        ("10", "-16384"), ("0.1", "1000"), ("0.1", "1001"), ("2", "0.5"), ("-2", "0.5"),
        ("2", "1e10"), ("0.5", "1e10"), ("1", "1e100"), ("-1", "1e100"), ("-1", "1e100"),
        ("2", "3.5"), ("1e10", "0.1"), ("4", "0.5"), ("8", "0.333333333333333333"),
        ("2", "2.0"), ("2.00", "2"), ("2", "1"), ("2.5", "1"), ("2.555", "1.0"),
        ("3", "-1"), ("3.0", "-1"), ("7", "-2"), ("1.5", "-3"), ("-1.5", "-3"),
        ("-1", "2147483647"), ("-1", "2147483648"), ("-1", "-2147483648"), ("1", "-2147483649"),
        ("2", "2147483648"), ("0.5", "2147483648"), ("1.000000001", "2147483647"),
        ("0.999999999", "2147483647"), ("9.99", "100"), ("12345.6789", "7"),
        ("1.1", "1000"), ("1.1", "-1000"), ("2", "-1074"), ("2", "1023"), ("10", "-1000"),
        ("10", "-1001"), ("-10", "-1000"), ("10", "1000.5"), ("10", "-1000.5"),
        ("1e-100", "0.5"), ("1e100", "-0.5"), ("123.456", "-0.123"), ("2", "0.0000001"),
        ("NaN", "0"), ("NaN", "1"), ("1", "NaN"), ("2", "NaN"), ("NaN", "NaN"),
        ("Infinity", "0"), ("Infinity", "2"), ("Infinity", "-2"), ("Infinity", "0.5"),
        ("-Infinity", "3"), ("-Infinity", "2"), ("-Infinity", "-3"), ("-Infinity", "-2"),
        ("-Infinity", "2.5"), ("-Infinity", "-2.5"), ("2", "Infinity"), ("2", "-Infinity"),
        ("0.5", "Infinity"), ("0.5", "-Infinity"), ("-0.5", "Infinity"), ("-2", "Infinity"),
        ("-2", "-Infinity"), ("1", "Infinity"), ("-1", "Infinity"), ("-1", "-Infinity"),
        ("0", "Infinity"), ("0", "-Infinity"), ("Infinity", "Infinity"),
        ("Infinity", "-Infinity"), ("-Infinity", "Infinity"), ("-Infinity", "-Infinity"),
        ("1.0", "Infinity"), ("-3", "Infinity"),
        ("2", "-0.5"), ("0.5", "0.5"), ("100", "0.25"), ("1.5", "1.5"), ("10", "-0.1"),
        ("3.7", "4.2"), ("0.0001", "0.0001"), ("5", "2.5"), ("2", "100.5"), ("2", "-100.5"),
        ("10", "5000"), ("10", "-5000"), ("2", "16000"), ("2", "-16000"),
        ("1.000000000000001", "1000000000000000"), ("0.9999", "-1000000"),
        ("2", "131071.5"), ("1.5", "300000"), ("1.5", "-300000"),
    ]
    vals = pos_values(60)
    for i in range(0, len(vals) - 1, 2):
        e = rand_num(maxint=2, maxfrac=4, allow_sci=False)
        pow_pairs.append((vals[i], e))
    for _ in range(40):
        b = rand_num(maxint=3, maxfrac=6, allow_sci=False)
        e = str(rnd.randint(-30, 30))
        pow_pairs.append((b, e))
    for b, e in pow_pairs:
        cases.append(("pow", [b, e], f"power({num(b)}, {num(e)})"))

    gcd_pairs = [
        ("12", "18"), ("-12", "18"), ("12", "-18"), ("0", "5"), ("5", "0"), ("0", "0"),
        ("0.5", "0.25"), ("1.5", "2.25"), ("12.0", "18.000"), ("NaN", "1"), ("1", "Infinity"),
        ("123456789012345678901234567890", "987654321098765432109876543210"),
        ("7", "13"), ("2.5", "-0.75"), ("1e20", "1e10"), ("-0", "-7"),
        ("330", "-231"), ("0.0001", "0.000001"),
    ]
    for _ in range(30):
        gcd_pairs.append((rand_num(maxint=12, maxfrac=3, allow_sci=False),
                          rand_num(maxint=12, maxfrac=3, allow_sci=False)))
    for a, b in gcd_pairs:
        cases.append(("gcd", [a, b], f"gcd({num(a)}, {num(b)})"))
        cases.append(("lcm", [a, b], f"lcm({num(a)}, {num(b)})"))

    for n in [-5, -1, 0, 1, 2, 3, 5, 10, 20, 21, 25, 30, 50, 100, 170, 171, 1000, 32178,
              100000, -9223372036854775808]:
        cases.append(("fac", [str(n)], f"factorial({lit(str(n))}::int8)"))
    write("math", cases)


# ----------------------------------------------------------------------
# Casts
# ----------------------------------------------------------------------

def rand_double():
    r = rnd.random()
    if r < 0.6:
        bits = rnd.getrandbits(64)
        x = struct.unpack("<d", struct.pack("<Q", bits))[0]
    elif r < 0.75:
        # subnormals
        bits = rnd.getrandbits(52) | (rnd.getrandbits(1) << 63)
        x = struct.unpack("<d", struct.pack("<Q", bits))[0]
    elif r < 0.9:
        x = rnd.uniform(-1e6, 1e6)
        if rnd.random() < 0.5:
            x = round(x, rnd.randint(0, 8))
    else:
        x = float(rnd.randint(-10**17, 10**17))
    return x


def rand_float():
    r = rnd.random()
    if r < 0.6:
        bits = rnd.getrandbits(32)
    elif r < 0.75:
        bits = rnd.getrandbits(23) | (rnd.getrandbits(1) << 31)
    else:
        x = struct.unpack("<f", struct.pack("<f", rnd.uniform(-1e6, 1e6)))[0]
        return x
    return struct.unpack("<f", struct.pack("<I", bits))[0]


def f32_repr(x):
    """Shortest string that round-trips through float32 (via decimal digits)."""
    if math.isnan(x):
        return "NaN"
    if math.isinf(x):
        return "Infinity" if x > 0 else "-Infinity"
    for p in range(1, 18):
        s = "%.*e" % (p - 1, x)
        if struct.unpack("<f", struct.pack("<f", float(s)))[0] == x:
            # make sure float(s)->float32 is the same as direct strtof; for
            # 1..9 significant digits double rounding cannot happen
            return s
    return repr(x)


def gen_casts():
    cases = []
    int_vals = mixed_values(250, maxint=22, maxfrac=6)
    int_vals += [
        "9223372036854775807.4", "9223372036854775807.5", "-9223372036854775808.4",
        "-9223372036854775808.5", "2147483647.4999", "2147483647.5", "-2147483648.5",
        "-2147483648.4999", "32767.5", "32767.4999", "-32768.5", "-32768.49", "0.5", "-0.5",
        "0.4999999", "1e19", "-1e19", "1e-20", "123456789012345678901234567890",
    ]
    for v in int_vals:
        cases.append(("int8", [v], f"{num(v)}::int8"))
        cases.append(("int4", [v], f"{num(v)}::int4"))
        cases.append(("int2", [v], f"{num(v)}::int2"))

    f_vals = mixed_values(250, maxint=30, maxfrac=30)
    f_vals += [
        "1e308", "1.7976931348623157e308", "1.7976931348623158e308", "1.7976931348623159e308",
        "1e309", "-1e309", "1e-307", "2.2250738585072014e-308", "4.9e-324", "5e-324",
        "2.4703282292062327e-324", "2.4703282292062328e-324", "1e-324", "1e-400", "1e400",
        "3.4028235e38", "3.4028236e38", "3.40282356779733661637539395458142568448e38",
        "3.40282357e38", "1e39", "1e-45", "1.4e-45", "7e-46", "7.1e-46", "1e-46",
        "1.17549435e-38", "0.1", "0.2", "0.3", "123456789.123456789",
        "0." + "0" * 340 + "1", "9" * 400,
    ]
    for v in f_vals:
        cases.append(("float8", [v], f"{num(v)}::float8"))
        cases.append(("float4", [v], f"{num(v)}::float4"))

    # float8 text in/out, and float8 -> numeric
    dvals = [
        "0", "-0", "1", "-1", "0.1", "0.5", "1e15", "1e16", "1e-4", "1e-5", "123456789012345",
        "1234567890123456", "999999999999999", "9999999999999998", "0.0001", "0.00001",
        "0.00012345", "1.2345e-05", "9.999999999999999e14", "1e22", "1e23", "5e-324",
        "2.2250738585072014e-308", "2.225073858507201e-308", "1.7976931348623157e308",
        "1e100", "1.5e-07", "1e+20", "123.456", "-123.456e-10", "3.141592653589793",
        "2.718281828459045", "0.30000000000000004", "1.0000000000000002", "4.35",
        "100", "1000000", "1e6", "12345678", "0.000001", "1e-7", "9007199254740993",
        "9007199254740992", "18014398509481984", "295147905179352825856",
        "1000000000000005", "1000000000000015", "1000000000000025", "0.15", "0.25", "0.35",
        "2.5", "3.5", "1.1e-16", "4.9406564584124654e-324", "1.5e300", "-1.5e300",
        "Infinity", "-Infinity", "NaN", "inf", "-inf", "+inf", "infinity", "+Infinity",
        "nan", "-nan", "NAN", "nan(123)", "nan()", "nan(abc_1)", "nan(", "nan(1", "  nan  ",
        "1e400", "-1e400", "1e-400", "-1e-400", "abc", "", " ", "1e", "1e+", "0x1p3", "0x10",
        "0x1.8p1", "-0x1p-1074", "0x1p-1075", "0x1.0000000000001p-1075", "0x1p1024",
        "0x1.fffffffffffff8p1023", "0x1.fffffffffffff7p1023", "0x", "0xg", "0x.8", "0x1p",
        "0x1p+", "0X1P3", "0x1.8", "0x123456789abcdef123", "0x0", "0x0p100", "-0x0",
        "infinit", "1e308", "1.7976931348623158e308", "1.7976931348623159e308",
        "2.4703282292062327e-324", "2.4703282292062328e-324", "1_000", ".5", "5.", ".",
        "-.5e3", "+1.5", "++1", "1.5.5", " 1.5 ", "\t1.5\n", "1.5x", "1e5x", "1e400x",
        "1e-400x", "  1e400  ", "0.000000000000000000000000000000001", "1" + "0" * 400,
        "0." + "0" * 400 + "1", "1" * 30, "4.4501477170144023e-308", "1e-320",
        "123e-325", "9.88131291682493088353e-324", "1e99999999999999999999",
        "1e-99999999999999999999", "0e99999999999999999999", ".1e-2147483649",
        "-0e-99999999999999999999", "1e+0000000000000000000000308", "0x1p99999999999999999999",
        "0x1p-99999999999999999999", "0x0p99999999999999999999", "1.e5", "1.E5", "1.5e5.5",
        "\u00a01", "1\u00a0", "Infinity1", "-Infinityx", "nanx", "infinity()",
    ]
    for _ in range(1500):
        dvals.append(repr(rand_double()))
    for v in dvals:
        cases.append(("f8in", [v], f"{lit(v)}::float8"))
        cases.append(("f8num", [v], f"{lit(v)}::float8::numeric"))

    fvals = [
        "0", "-0", "1", "0.1", "1e6", "999999", "123456", "1234567", "1e-4", "1e-5", "1e7",
        "3.4028235e38", "3.4028236e38", "3.4028234e38", "1e39", "-1e39", "1.4e-45", "1e-45",
        "7e-46", "7.1e-46", "1e-46", "1.17549435e-38", "1.1754942e-38", "16777216",
        "16777217", "0.3", "2.5", "3.5", "100", "1.5e-7", "Infinity", "-Infinity", "NaN",
        "inf", "nan", "abc", "", "1e400", "1e-400", "0x1p3", "0x1p128", "0x1.fffffep127",
        "0x1.ffffffp127", "0x1p-149", "0x1p-150", "0x1.000001p-150", "7.038531e-26",
        "1e99999999999999999999", "1e-99999999999999999999", "3.40282357e38", "1.40129846e-45",
        "7.00649232e-46", "7.0064924e-46",
        "33554431", "0.000123", "12345.678", "8388608.5", "1e+20", "0.00001234",
    ]
    for _ in range(1500):
        fvals.append(f32_repr(rand_float()))
    for v in fvals:
        cases.append(("f4in", [v], f"{lit(v)}::float4"))
        cases.append(("f4num", [v], f"{lit(v)}::float4::numeric"))
    write("casts", cases)


# ----------------------------------------------------------------------
# typmod coercion, send
# ----------------------------------------------------------------------

def gen_typmod():
    cases = []
    vals = mixed_values(150, maxint=12, maxfrac=12)
    typmods = [(1, 0), (2, 1), (3, 0), (4, 2), (5, 5), (10, 2), (10, 0), (10, -2), (5, -3),
               (2, 5), (3, 7), (1, 1), (1, -1), (38, 10), (1000, 0), (1000, 1000), (1000, -1000),
               (4, 4), (6, 3), (15, 15), (20, 10), (3, -5), (1, 3)]
    for v in vals:
        tms = rnd.sample(typmods, 4)
        for p, s in tms:
            cases.append(("typmod", [v, str(p), str(s)], f"{num(v)}::numeric({p},{s})"))
    extra = [
        ("123.456", 5, 2), ("999.995", 5, 2), ("999.994", 5, 2), ("-999.995", 5, 2),
        ("0.0001", 1, 0), ("0.5", 1, 0), ("9.5", 1, 0), ("99", 1, 0), ("0.001", 2, 3),
        ("0.0099", 2, 3), ("0.0995", 2, 3), ("0.09949", 2, 3), ("12345", 5, -2),
        ("12349999", 5, -3), ("12350000", 5, -3), ("99999999", 5, -3), ("150", 1, -2),
        ("149.99", 1, -2), ("950", 1, -2), ("NaN", 5, 2), ("Infinity", 5, 2),
        ("-Infinity", 1000, 0), ("0", 1, 0), ("0.000000", 3, 2), ("1e-20", 5, 5),
        ("123", 0, 0), ("123", 1001, 0), ("123", 10, 1001), ("123", 10, -1001),
        ("123", -1, 0), ("1" + "0" * 999, 1000, 0), ("1" + "0" * 1000, 1000, 0),
        ("0.00001", 3, 7), ("1e-5", 1, 5), ("5e-6", 1, 5), ("4e-6", 1, 5),
    ]
    for v, p, s in extra:
        cases.append(("typmod", [v, str(p), str(s)], f"{num(v)}::numeric({p},{s})"))
    write("typmod", cases)

    sends = mixed_values(150) + ["0.000", "-0.0", "1e-16383", "9.99e131071", "1e131071",
                                  "0.0001", "10000", "123456789.000000001"]
    cases = [("send", [v], f"numeric_send({num(v)})") for v in sends]
    write("send", cases)


# ----------------------------------------------------------------------
# High precision operands
# ----------------------------------------------------------------------

def long_num(min_digits=20, max_digits=120, positive=False, max_int=60):
    nint = rnd.randint(0, max_int)
    nfrac = rnd.randint(0, max_digits)
    if nint + nfrac < min_digits:
        nfrac = min_digits - nint
    ip = rand_digits(nint).lstrip("0") or "0"
    fp = rand_digits(nfrac)
    s = ip + ("." + fp if fp else "")
    if s.strip("0.") == "":
        s = "1" + s
    if not positive and rnd.random() < 0.4:
        s = "-" + s
    return s


def gen_precision():
    cases = []
    for _ in range(250):
        a, b = long_num(), long_num()
        cases.append(("mul", [a, b], f"{num(a)} * {num(b)}"))
        cases.append(("div", [a, b], f"{num(a)} / {num(b)}"))
        cases.append(("mod", [a, b], f"mod({num(a)}, {num(b)})"))
        cases.append(("div_trunc", [a, b], f"div({num(a)}, {num(b)})"))
    for _ in range(150):
        a = long_num(positive=True)
        cases.append(("sqrt", [a], f"sqrt({num(a)})"))
        cases.append(("ln", [a], f"ln({num(a)})"))
        cases.append(("log10", [a], f"log({num(a)})"))
    for _ in range(150):
        a = long_num(max_int=3, max_digits=80)
        cases.append(("exp", [a], f"exp({num(a)})"))
    for _ in range(100):
        b = long_num(positive=True, max_int=4, max_digits=60)
        e = long_num(max_int=2, max_digits=40)
        cases.append(("pow", [b, e], f"power({num(b)}, {num(e)})"))
        x = long_num(positive=True, max_int=10, max_digits=50)
        cases.append(("log", [b, x], f"log({num(b)}, {num(x)})"))
    for _ in range(60):
        b = long_num(max_int=3, max_digits=40)
        e = str(rnd.randint(-200, 200))
        cases.append(("pow", [b, e], f"power({num(b)}, {num(e)})"))
    # large display scales
    for ds in [50, 300, 999, 1000, 1001]:
        for v in ["2", "0.5", "1.1", "123.456", "0.0001"]:
            vv = v + ("." if "." not in v else "") + "0" * ds
            vv = vv[: vv.index(".") + 1 + ds]
            cases.append(("sqrt", [vv], f"sqrt({num(vv)})"))
            cases.append(("ln", [vv], f"ln({num(vv)})"))
            cases.append(("exp", [vv], f"exp({num(vv)})"))
            cases.append(("log10", [vv], f"log({num(vv)})"))
            cases.append(("pow", [vv, "1.5"], f"power({num(vv)}, 1.5)"))
            cases.append(("pow", [vv, "3"], f"power({num(vv)}, 3)"))
            cases.append(("div", ["1", vv], f"1 / {num(vv)}"))
    write("precision", cases)


# ----------------------------------------------------------------------
# numeric_recv via binary COPY
# ----------------------------------------------------------------------

def gen_recv():
    cur.execute("CREATE TEMP TABLE IF NOT EXISTS recv_t (v numeric)")

    def payload(ndigits, weight, sign, dscale, digits):
        b = struct.pack(">HhHH", ndigits, weight, sign, dscale)
        for d in digits:
            b += struct.pack(">h", d)
        return b

    inputs = [
        payload(1, 0, 0, 0, [1]),
        payload(2, 0, 0x4000, 2, [1, 5000]),
        payload(0, 0, 0, 5, []),
        payload(0, 0, 0x4000, 3, []),
        payload(0, 7, 0, 0, []),
        payload(0, 0, 0xC000, 0, []),
        payload(0, 0, 0xD000, 32, []),
        payload(0, 0, 0xF000, 0, []),
        payload(1, 0, 0xC000, 9, [5]),
        payload(3, 1, 0, 4, [0, 12, 3456]),
        payload(3, 0, 0, 8, [12, 3456, 0]),
        payload(2, -1, 0, 2, [1234, 5678]),
        payload(2, -1, 0, 6, [1234, 5678]),
        payload(2, -1, 0, 0, [1234, 5678]),
        payload(1, 2, 0, 0, [7]),
        payload(1, -3, 0x4000, 12, [9999]),
        payload(1, -3, 0x4000, 11, [9999]),
        payload(1, 100, 0, 0, [1]),
        payload(1, -32768, 0, 16383, [1]),
        payload(1, 0, 0, 16384, [1]),
        payload(1, 0, 0x1000, 0, [1]),
        payload(1, 0, 0x8000, 0, [1]),
        payload(1, 0, 0, 0, [10000]),
        payload(1, 0, 0, 0, [-1]),
        payload(2, 30, 0, 0, [0, 1]),
        payload(2, 0, 0, 2, [5, 9999]),
        payload(4, 3, 0x4000, 0, [1, 2, 3, 4]),
        payload(2, 1, 0, 0, [1]),
        payload(1, 0, 0, 0, [1]) + b"\x00",
        b"\x00\x01\x00",
        payload(1, 0, 0, 0x3FFF, [1]),
        payload(1, 0, 0, 0xFFFF, [1]),
    ]
    lines = []
    for p in inputs:
        hexp = p.hex()
        copy = b"PGCOPY\n\xff\r\n\x00" + struct.pack(">ii", 0, 0)
        copy += struct.pack(">h", 1) + struct.pack(">i", len(p)) + p + struct.pack(">h", -1)
        cur.execute("TRUNCATE recv_t")
        try:
            cur.copy_expert("COPY recv_t FROM STDIN WITH (FORMAT binary)", io.BytesIO(copy))
            cur.execute("SELECT v::text FROM recv_t")
            r = "ok\x1f" + cur.fetchone()[0]
        except psycopg2.Error as e:
            d = e.diag
            r = "err\x1f%s\x1f%s\x1f%s" % (e.pgcode, d.message_primary, d.message_detail or "")
        lines.append(("recv", [hexp], r))
    path = os.path.join(OUT, "recv.tsv")
    with open(path, "w", encoding="utf-8") as f:
        for op, args, r in lines:
            args = list(args) + [""] * (3 - len(args))
            f.write("\t".join(esc(x) for x in [op] + args + r.split("\x1f")) + "\n")
    print(f"recv: {len(lines)} cases", file=sys.stderr)


# ----------------------------------------------------------------------
# numeric_in / numeric_recv with a typmod (via COPY into numeric(p,s))
# ----------------------------------------------------------------------

def copy_one(table, data, binary):
    cur.execute(f"TRUNCATE {table}")
    try:
        if binary:
            cur.copy_expert(f"COPY {table} FROM STDIN WITH (FORMAT binary)", io.BytesIO(data))
        else:
            cur.copy_expert(f"COPY {table} FROM STDIN", io.StringIO(data))
        cur.execute(f"SELECT v::text FROM {table}")
        return "ok\x1f" + cur.fetchone()[0]
    except psycopg2.Error as e:
        d = e.diag
        return "err\x1f%s\x1f%s\x1f%s" % (e.pgcode, d.message_primary, d.message_detail or "")


def gen_typmod_input():
    typmods = [(5, 2), (3, 0), (1, 0), (10, -2), (3, 5), (38, 10), (1000, 1000), (6, 6), (2, -3)]
    inputs = [
        "1e-20000", "1e-16384", "-1e-20000", "0e-20000", "1e131072", "123.456", "12.345",
        "99.995", "999.995", "0.000001", "0.0000005", "NaN", "Infinity", "-inf", " 12.5 ",
        "12.5x", "abc", "", "0x10", "-0b11", "1_000", "1e2", "9.9e2", "0.00999", "5e-6",
        "4.9e-6", "-0.0", "  +.5  ", "1e1073741824", "<9*131073>", "0.<0*20000>1",
    ]
    for _ in range(80):
        inputs.append(rand_num(maxint=12, maxfrac=12))
    lines = []
    for p, sc in typmods:
        t = f"tm_{p}_{abs(sc)}{'n' if sc < 0 else ''}"
        cur.execute(f"CREATE TEMP TABLE IF NOT EXISTS {t} (v numeric({p},{sc}))")
        for v in inputs:
            data = esc(expand(v)) + "\n"
            if expand(v) == "\\N":
                continue
            r = copy_one(t, data, False)
            lines.append(("in_typmod", [v, str(p), str(sc)], r))
        # binary input with typmod
        def payload(ndigits, weight, sign, dscale, digits):
            b = struct.pack(">HhHH", ndigits, weight, sign, dscale)
            for d in digits:
                b += struct.pack(">h", d)
            return b
        for pl in [payload(2, 0, 0, 4, [12, 3456]), payload(2, 1, 0x4000, 2, [1, 2345]),
                   payload(0, 0, 0xD000, 0, []), payload(0, 0, 0xC000, 0, []),
                   payload(1, -1, 0, 4, [5]), payload(1, 2, 0, 0, [7]),
                   payload(3, 0, 0, 8, [99, 9999, 5000])]:
            copy = b"PGCOPY\n\xff\r\n\x00" + struct.pack(">ii", 0, 0)
            copy += struct.pack(">h", 1) + struct.pack(">i", len(pl)) + pl + struct.pack(">h", -1)
            r = copy_one(t, copy, True)
            lines.append(("recv_typmod", [pl.hex(), str(p), str(sc)], r))
    path = os.path.join(OUT, "in_typmod.tsv")
    with open(path, "w", encoding="utf-8") as f:
        for op, args, r in lines:
            f.write("\t".join(esc(x) for x in [op] + args + r.split("\x1f")) + "\n")
    print(f"in_typmod: {len(lines)} cases", file=sys.stderr)


def main():
    os.makedirs(OUT, exist_ok=True)
    gen_in()
    gen_arith()
    gen_unary()
    gen_math()
    gen_casts()
    gen_typmod()
    gen_precision()
    gen_recv()
    gen_typmod_input()


if __name__ == "__main__":
    main()
