#!/usr/bin/env python3
"""Generate golden data for pgfuncs::array from a real PostgreSQL 17.

Usage: python3 gen_array.py   (writes ../data/array_golden.jsonl)

Cases:
  {"fn": "array_in", "args": [input, delim, sqltype], "result": <json text>}
     result = {"lbs": [...], "lens": [...], "elems": [...], "out": "<array_out>"}
     (lbs/lens/elems are null for an empty array)
  {"fn": "array_out", "args": [nested list of elements], "result": "<array_out>"}
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pgeval import Recorder  # noqa: E402

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "array_golden.jsonl")

ARRAY_IN_SQL = """
SELECT json_build_object(
  'lbs', (SELECT json_agg(array_lower(a, d) ORDER BY d) FROM generate_series(1, array_ndims(a)) d),
  'lens', (SELECT json_agg(array_length(a, d) ORDER BY d) FROM generate_series(1, array_ndims(a)) d),
  'elems', (SELECT json_agg(e::text ORDER BY o) FROM unnest(a) WITH ORDINALITY u(e, o)),
  'out', a::text)::text
FROM (SELECT %s::{typ} AS a) s
"""

TEXT_CASES = [
    # empty arrays
    "{}", " {} ", "{ }", "\t{\n}\r", "{{}}", "{{},{}}", "{{{}}}", "{ { } , { } }",
    "{{{{{{}}}}}}", "{{{{{{{}}}}}}}", "{{},{{}}}", "{{{}},{}}",
    # simple
    "{a}", "{a,b,c}", "{ a , b , c }", "{a b}", "{ a  b }", "{\"a\",\"b\"}", "{\"a b\", c d }",
    "{abc,def}", "{ab  ,  cd  }", "{a\t}", "{a\n,b}", "{a\x0b}", "{a\x0c}", "{\x0ba\x0c}",
    "{1,2,3}", "{-1,+2,3.5,1e10}",
    # NULL handling
    "{NULL}", "{null}", "{NuLl}", "{\"NULL\"}", "{NULL ,  null}", "{\\NULL}", "{NULLx}",
    "{ NULL }", "{N\\ULL}", "{\"null\",null}", "{NUL}", "{NULL NULL}", "{xNULL}", "{NULL\\ }",
    "{\"\"}", "{\"\",\"\"}", "{\"\" , \"\"}", "{a,\"\",NULL}",
    # escapes
    "{a\\,b}", "{\"a\\\"b\"}", "{\"a\\\\b\"}", "{\\{}", "{\\}}", "{a\\ }", "{a \\ b}", "{\\\\}",
    "{\"\\a\\b\\c\"}", "{\"a,b\"}", "{\"{a}\"}", "{\"a\\\\\"}", "{\\\"}", "{\\\"a\\\"}",
    "{\" \"}", "{\"\t\"}", "{\"  a  \"}", "{a\\\\b}", "{\\ a}", "{\\  }",
    # multi-dim
    "{{1,2},{3,4}}", "{{1,2},{3}}", "{{1},2}", "{1,{2}}", "{{{1}}}", "{{a,b},{c,d}}",
    " { { 1 } , { 2 } } ", "{{a},{b},{c}}", "{{{a,b},{c,d}},{{e,f},{g,h}}}", "{{a,b},{c,d},{e}}",
    "{{1},{}}", "{{},{1}}", "{{1,2},{3,4},{5,6}}", "{{\"a\",NULL},{NULL,\"b\"}}",
    "{{{{{{a}}}}}}", "{{{{{{{a}}}}}}}", "{{a},}", "{{a},{b},}", "{{a}{b}}", "{{a} {b}}",
    "{\"a\",{b}}", "{{\"a\"} , {\"b\"}}", "{{a,b}, {c,d}}",
    # explicit dimensions
    "[1:1]={a}", "[0:2]={a,b,c}", "[-2:-1]={a,b}", "[2]={a,b}", "[1:2][3:4]={{a,b},{c,d}}",
    " [1:2] = {a,b} ", "[1:2] [1:1]={{a},{b}}", "[1:2]{a,b}", "[1:2]= x", "[1:2]=x{a,b}",
    "[1:3]={a,b}", "[1:2]={{a},{b}}", "[1:1][1:1]={a}", "[1:0]={}", "[2:1]={a}", "[]={a}",
    "[a]={a}", "[1:]={a}", "[1:2={a,b}", "[1:2 ]={a,b}", "[ 1:2]={a,b}", "[+1:+2]={a,b}",
    "[-1]={}", "[0]={}", "[1:2147483647]={a}", "[-2147483648:2147483646]={a}",
    "[1:99999999999]={a}", "[99999999999999999999999]={a}", "[1][1][1][1][1][1][1]={a}",
    "[1][1][1][1][1][1]={{{{{{a}}}}}}", "[2147483647]={a}", "[-2147483649:1]={a}",
    "[1:2][1:2]={{a,b},{c,d}}", "[1:1]={a} junk", "[0:0]={NULL}", "[0:1]={\"a b\",\"\\\"\"}",
    "[1:2]={}", "[1:1]={}", "[1:1]={{}}", "[-]={a}", "[+]={a}", "[1:-]={a}", "[1:+5]={a,b,c,d,e}",
    "[1:2]", "[1:2]=", "[1:2]={a,b", "[0:0][0:0]={{x}}", "[1:1]=  \t{a}", "[3:3]={\"\"}",
    "[1:2]={a,b}x", "[10:11]={{a},{b}}", "[1:2][1:1]={a,b}", "[2147483646:2147483646]={a}",
    "[-2147483648:-2147483648]={a}", "[1:2]=={a,b}", "[1:2]:{a,b}", "[1:2)={a,b}", "[1:2:3]={a}",
    "[007:008]={a,b}", "[0x1]={a}", "[1:1]x={a}",
    # junk / errors
    "", "a", "{", "{a", "{a,", "{,a}", "{a,,b}", "{a b", "{\"a\"b}", "{a\"b\"}", "{\"a\" \"b\"}",
    "{a{b}}", "{a}{b}", "{}}", "{\"a\"}x", "{a,b}}", "{a} b", "{a} ", "{a}\t\n", "{a}}",
    "{\"a", "{\"a\\", "{\\", "{a\\", "{\\}", "{\"\\}", " ", "  {a}", "x{a}", "{a}x", "{,}", "{a,}",
    "{\"a\",}", "{\"a\" ,}", "{\"a\"  b}", "{\"a\"\"b\"}", "{\"}", "{\"\"\"}", "}", "{{}", "{{a}",
    "{}{}", "{a,{}}", "{{},a}", "{ {}, {} }x", "{\"a\"{}}", "{a\"}", "{\"a\"\n,\n\"b\"\n}",
    "{ \"a\" }", "{\"a\" , \"b\" }",
    # unicode
    "{zażółć,gęślą,jaźń}", "{\"日本語\",\"中文\"}", "{ ✓ , ☃ }", "{😀,\"😀 x\"}", "{\\é}",
    "{\u00a0a\u00a0}", "{\"\u2028\"}", "{\u3000x\u3000}", "{é é ,ü}", "{\"é\\\"ü\"}",
    "{{α,β},{γ,δ}}", "[0:1]={ж,з}", "{Ā\\Ą}", "{😀}", "{\"😀\",NULL,😀}",
]

BOX_CASES = [
    "{(1,1),(0,0);(2,2),(1,1)}", "{(1,1),(0,0)}", "{ (1,1),(0,0) ; (3,3),(2,2) }",
    "{(1,1),(0,0);}", "{;(1,1),(0,0)}", "{{(1,1),(0,0)};{(2,2),(1,1)}}",
    "[0:1]={(1,1),(0,0);(2,2),(1,1)}", "{\"(1,1),(0,0)\";NULL}", "{(1,1),(0,0),(2,2),(1,1)}",
    "{(1,1),(0,0);;(2,2),(1,1)}", "{}", "{NULL;NULL}", "{{(1,1),(0,0)},{(2,2),(1,1)}}",
    "{(1,1),(0,0);(2,2),(1,1)} x",
]

INT_CASES = [
    "{1,2,3}", "{ 1 , 2 }", "{1,NULL}", "[0:1]={1,2}", "{{1,2},{3,4}}", "{}", "{ -1 , +2 }",
]

ARRAY_OUT_CASES = [
    [],
    ["a"],
    ["a", "b", None],
    [""],
    ["NULL"],
    ["null"],
    ["Null"],
    ["NULLs"],
    [" "],
    ["a b"],
    [" a"],
    ["a "],
    ["\t"],
    ["\n"],
    ["\r"],
    ["\x0b"],
    ["\x0c"],
    ["a,b"],
    ["a;b"],
    ["{"],
    ["}"],
    ["{}"],
    ["\""],
    ["\\"],
    ["a\"b\\c"],
    ["\\\\"],
    ["\"\""],
    ["é"],
    ["zażółć gęślą"],
    ["日本語"],
    ["😀"],
    ["\u00a0"],
    ["\u2028"],
    ["\u3000"],
    ["a'b"],
    ["(1,2)"],
    ["[1:2]"],
    ["="],
    [None, None],
    ["x", "", None, "NULL", "\\\"", " y "],
    [["a", "b"], ["c", None]],
    [["a b", "\""], ["", "NULL"]],
    [[["a"], ["b"]], [["c"], ["d"]]],
    [["1", "2", "3"]],
    [["1"], ["2"], ["3"]],
]


SETUP_SQL = """
CREATE SCHEMA IF NOT EXISTS pgfuncs_test;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
                 WHERE n.nspname = 'pgfuncs_test' AND t.typname = 'semitext') THEN
    CREATE TYPE pgfuncs_test.semitext;
    CREATE FUNCTION pgfuncs_test.semitext_in(cstring) RETURNS pgfuncs_test.semitext
      AS 'textin' LANGUAGE internal IMMUTABLE STRICT;
    CREATE FUNCTION pgfuncs_test.semitext_out(pgfuncs_test.semitext) RETURNS cstring
      AS 'textout' LANGUAGE internal IMMUTABLE STRICT;
    CREATE TYPE pgfuncs_test.semitext (INPUT = pgfuncs_test.semitext_in,
      OUTPUT = pgfuncs_test.semitext_out, INTERNALLENGTH = VARIABLE, STORAGE = extended,
      DELIMITER = ';', CATEGORY = 'S');
  END IF;
END $$;
"""

SEMI_CASES = [
    "{a;b}", "{a,b;c}", "{ a , b ; c }", "{\"a;b\";c}", "{a\\;b}", "{a;;b}", "{;a}", "{a;}",
    "{{a;b};{c;d}}", "{{a,b},{c,d}}", "[0:1]={x;y}", "{NULL;null;\"NULL\"}", "{a;b,}", "{\"a\",b}",
    "{\"a\";b}", "{\"a\" ; b}", "{ ; }", "{,}", "{a,}",
]

# Alphabet for random literals: structural characters, whitespace, NULL letters,
# dimension syntax and a few multibyte characters.
RAND_ALPHABET = list("{}{}{},,;\"\"\\  \taNULnul[]:=1-x") + ["é", "日", "\n", "\x0b"]


def random_literal(rng):
    n = rng.randint(0, 14)
    s = "".join(rng.choice(RAND_ALPHABET) for _ in range(n))
    if rng.random() < 0.7:
        s = "{" + s
    if rng.random() < 0.5:
        s = s + "}"
    return s


ELEM_POOL = ["a", "b", "xyz", "", "NULL", "null", " ", "a b", "\"", "\\", "{", "}", ",", ";",
             "é", "日本", "😀", "\t", "x y z", "N", "NUL", "NULLL", "1", "-2", "[1]", "="]


def quote_elem(e, delim, rng):
    if e is None:
        return rng.choice(["NULL", "null", "NuLL"])
    special = (e == "" or e.lower() == "null" or any(c in e for c in '{}"\\ \t\n\r\x0b\x0c' + delim))
    if special or rng.random() < 0.3:
        return '"' + e.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return e


def random_valid_literal(rng, delim):
    ndim = rng.randint(1, 3)
    lens = [rng.randint(1, 3) for _ in range(ndim)]
    ws = lambda: rng.choice(["", "", "", " ", "  ", "\t", "\n"])

    def build(level):
        if level == ndim:
            e = None if rng.random() < 0.15 else rng.choice(ELEM_POOL)
            return ws() + quote_elem(e, delim, rng) + ws()
        parts = [build(level + 1) for _ in range(lens[level])]
        return ws() + "{" + delim.join(parts) + "}" + ws()

    body = build(0)
    if rng.random() < 0.3:
        dims = ""
        for n in lens:
            lb = rng.randint(-3, 3)
            dims += "[%d:%d]" % (lb, lb + n - 1) if rng.random() < 0.8 else "[%d]" % n
        body = dims + ws() + "=" + body
    return body


def mutate(s, rng):
    s = list(s)
    for _ in range(rng.randint(1, 2)):
        op = rng.randint(0, 2)
        pos = rng.randint(0, len(s))
        if op == 0 and s:
            del s[min(pos, len(s) - 1)]
        elif op == 1:
            s.insert(pos, rng.choice(RAND_ALPHABET))
        elif s:
            s[min(pos, len(s) - 1)] = rng.choice(RAND_ALPHABET)
    return "".join(s)


def main():
    import random
    rec = Recorder(OUT)
    rec.cur.execute(SETUP_SQL)
    for s in TEXT_CASES:
        rec.case("array_in", ARRAY_IN_SQL.format(typ="text[]"), [s], extra={"delim": ",", "type": "text[]"})
    for s in BOX_CASES:
        rec.case("array_in", ARRAY_IN_SQL.format(typ="box[]"), [s], extra={"delim": ";", "type": "box[]"})
    for s in INT_CASES:
        rec.case("array_in", ARRAY_IN_SQL.format(typ="int4[]"), [s], extra={"delim": ",", "type": "int4[]"})
    semi = "pgfuncs_test.semitext[]"
    for s in SEMI_CASES:
        rec.case("array_in", ARRAY_IN_SQL.format(typ=semi), [s], extra={"delim": ";", "type": "semitext[]"})
    rng = random.Random(20241024)
    seen = set()
    for i in range(6000):
        delim, typ, typname = (",", "text[]", "text[]") if i % 3 else (";", semi, "semitext[]")
        r = rng.random()
        if r < 0.35:
            lit = random_literal(rng)
        elif r < 0.7:
            lit = random_valid_literal(rng, delim)
        else:
            lit = mutate(random_valid_literal(rng, delim), rng)
        if (lit, delim) in seen:
            continue
        seen.add((lit, delim))
        rec.case("array_in", ARRAY_IN_SQL.format(typ=typ), [lit], extra={"delim": delim, "type": typname, "random": True})
    for elems in ARRAY_OUT_CASES:
        rec.case("array_out", "SELECT (%s::text[])::text", [elems])
    rec.write()


if __name__ == "__main__":
    main()
