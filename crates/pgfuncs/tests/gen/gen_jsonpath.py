"""Generate golden data for the jsonpath implementation (pgfuncs::json::path)
from a real PostgreSQL 17.

Usage:
  PGFUNCS_PGHOST=<socket dir> PGFUNCS_PGPORT=<port> \
      python3 crates/pgfuncs/tests/gen/gen_jsonpath.py [<regress-sql-dir>]

Writes crates/pgfuncs/tests/data/jsonpath_golden.jsonl. If given, the
directory must contain PostgreSQL 17's src/test/regress/sql/jsonpath.sql and
jsonb_jsonpath.sql; their jsonpath literals and jsonb_path_* calls are
extracted as additional cases.

Record kinds ("fn"):
  in            [path]                       path::jsonpath::text
  valid         [path]                       jsonpath input only (no output)
  in_rep        [pre, rep, n, mid, post]     path = pre + rep*n + mid + post*n; md5(output):length
  valid_rep     [pre, rep, n, mid, post]     same, input only
  exec_rep      [fn, docspec, pathspec, n]   fn in query_array/exists/match on documents and paths
                                             built as pre + rep*n + mid + post*n + tail
  query         [doc, path, vars, silent]    rows of jsonb_path_query, as a jsonb array
  query_array   [doc, path, vars, silent]    jsonb_path_query_array
  query_first   [doc, path, vars, silent]    jsonb_path_query_first
  exists        [doc, path, vars, silent]    jsonb_path_exists
  match         [doc, path, vars, silent]    jsonb_path_match
  exists_op     [doc, path]                  doc @? path
  match_op      [doc, path]                  doc @@ path
  query_tz, query_array_tz, query_first_tz, exists_tz, match_tz: the *_tz variants

Records may carry "tz" (the session TimeZone, default UTC) and "today" (the
session's current date as days since 2000-01-01, used where PostgreSQL casts
time to timetz).
"""

import json
import os
import random
import re
import sys

sys.path.insert(0, os.path.dirname(__file__))
from pgeval import Recorder  # noqa: E402

OUT = os.path.join(os.path.dirname(__file__), "..", "data", "jsonpath_golden.jsonl")

BS = "\\"

SQL = {
    "in": "SELECT %s::jsonpath::text",
    "valid": "SELECT (%s::jsonpath IS NOT NULL)::text",
    # path = pre || repeat(rep, n) || mid || repeat(post, n); output as md5:length
    "in_rep": "SELECT md5(x) || ':' || length(x) FROM (SELECT ((%s || repeat(%s, %s) || %s || repeat(%s, %s))"
              "::jsonpath)::text x) t",
    "valid_rep": "SELECT ((%s || repeat(%s, %s) || %s || repeat(%s, %s))::jsonpath IS NOT NULL)::text",
    "query": "SELECT coalesce(jsonb_agg(v ORDER BY o), '[]')::text "
             "FROM jsonb_path_query(%s::jsonb, %s::jsonpath, %s::jsonb, %s) WITH ORDINALITY t(v, o)",
    "query_array": "SELECT jsonb_path_query_array(%s::jsonb, %s::jsonpath, %s::jsonb, %s)::text",
    "query_first": "SELECT jsonb_path_query_first(%s::jsonb, %s::jsonpath, %s::jsonb, %s)::text",
    "exists": "SELECT jsonb_path_exists(%s::jsonb, %s::jsonpath, %s::jsonb, %s)::text",
    "match": "SELECT jsonb_path_match(%s::jsonb, %s::jsonpath, %s::jsonb, %s)::text",
    "exists_op": "SELECT (%s::jsonb @? %s::jsonpath)::text",
    "match_op": "SELECT (%s::jsonb @@ %s::jsonpath)::text",
}
for _f in ["query_array", "query_first", "exists", "match"]:
    SQL[_f + "_tz"] = SQL[_f].replace("jsonb_path_" + _f + "(", "jsonb_path_" + _f + "_tz(")
SQL["query_tz"] = SQL["query"].replace("jsonb_path_query(", "jsonb_path_query_tz(")

QUERY_FNS = ["query", "query_array", "query_first", "exists", "match"]


class Gen:
    def __init__(self):
        self.r = Recorder(OUT)
        self.seen = set()
        self.tz = None
        self.set_tz("UTC")

    def set_tz(self, tz):
        self.r.cur.execute("SET TimeZone = %s", (tz,))
        self.tz = tz

    def today(self):
        self.r.cur.execute("SELECT current_date - date '2000-01-01'")
        return self.r.cur.fetchone()[0]

    def case(self, fn, args):
        key = json.dumps([fn, args, self.tz])
        if key in self.seen:
            return
        self.seen.add(key)
        rec = self.r.case(fn, SQL[fn], args)
        if self.tz != "UTC":
            rec["tz"] = self.tz
        if self.tz != "UTC" or fn.endswith("_tz") or "time" in json.dumps(args):
            rec["today"] = self.today()

    def parse(self, path):
        self.case("in", [path])

    def query(self, fn, doc, path, vars_="{}", silent=False):
        if fn in ("exists_op", "match_op"):
            self.case(fn, [doc, path])
        else:
            self.case(fn, [doc, path, vars_, silent])


# ------------------------------------------------------------ regression

def sql_statements(text):
    """Split SQL text into statements (quotes and comments aware)."""
    out, cur, i, n = [], [], 0, len(text)
    while i < n:
        c = text[i]
        if c == "-" and text.startswith("--", i):
            j = text.find("\n", i)
            i = n if j < 0 else j
            continue
        if c in "'\"":
            q = c
            j = i + 1
            while j < n:
                if text[j] == q:
                    if j + 1 < n and text[j + 1] == q:
                        j += 2
                        continue
                    break
                j += 1
            cur.append(text[i:j + 1])
            i = j + 1
            continue
        if c == ";":
            out.append("".join(cur).strip())
            cur = []
            i += 1
            continue
        cur.append(c)
        i += 1
    if "".join(cur).strip():
        out.append("".join(cur).strip())
    return out


TOKEN_RE = re.compile(r"""\s*(?:(?P<str>[Ee]?'(?:[^']|'')*')|(?P<op>=>|::|@\?|@@|[(),])|(?P<word>[A-Za-z_][A-Za-z_0-9]*))""")


def tokens(stmt):
    pos, out = 0, []
    while pos < len(stmt):
        if stmt[pos:].strip() == "":
            break
        m = TOKEN_RE.match(stmt, pos)
        if not m:
            return None
        pos = m.end()
        if m.group("str") is not None:
            s = m.group("str")
            if s[0] in "Ee":
                return None  # escape strings are not used for interesting cases
            out.append(("str", s[1:-1].replace("''", "'")))
        elif m.group("op") is not None:
            out.append(("op", m.group("op")))
        else:
            out.append(("word", m.group("word").lower()))
    return out


def parse_literal(toks, i, typ):
    """[typ] 'lit' [::typ] -> (value, next index) or None."""
    if i < len(toks) and toks[i] == ("word", typ):
        i += 1
    if i >= len(toks) or toks[i][0] != "str":
        return None
    v = toks[i][1]
    i += 1
    if i + 1 < len(toks) and toks[i] == ("op", "::") and toks[i + 1][0] == "word":
        if toks[i + 1][1] != typ:
            return None
        i += 2
    return v, i


def extract_regress(g, d):
    for fname in ["jsonpath.sql", "jsonb_jsonpath.sql"]:
        path = os.path.join(d, fname)
        if not os.path.exists(path):
            continue
        for st in sql_statements(open(path, encoding="utf-8").read()):
            toks = tokens(st)
            if not toks or toks[0] != ("word", "select"):
                continue
            t = toks[1:]
            # select 'lit'::jsonpath
            if len(t) == 3 and t[0][0] == "str" and t[1] == ("op", "::") and t[2] == ("word", "jsonpath"):
                g.parse(t[0][1])
                continue
            # select jsonb 'doc' @? 'path'
            r = parse_literal(t, 0, "jsonb")
            if r and r[1] < len(t) and t[r[1]][0] == "op" and t[r[1]][1] in ("@?", "@@"):
                doc, i = r
                op = t[i][1]
                r2 = parse_literal(t, i + 1, "jsonpath")
                if r2 and r2[1] == len(t):
                    g.query("exists_op" if op == "@?" else "match_op", doc, r2[0])
                continue
            # select jsonb_path_xxx(doc, path [, vars] [, silent] [, vars => ..] [, silent => ..])
            if len(t) >= 2 and t[0][0] == "word" and t[0][1].startswith("jsonb_path_") and t[1] == ("op", "("):
                fn = t[0][1][len("jsonb_path_"):]
                fn = fn if fn in SQL and fn not in ("in", "valid") else None
                if not fn:
                    continue
                i = 2
                args = []
                named = {}
                ok = True
                while i < len(t) and t[i] != ("op", ")"):
                    if t[i][0] == "word" and i + 1 < len(t) and t[i + 1] == ("op", "=>"):
                        name = t[i][1]
                        i += 2
                        if name == "silent" and t[i][0] == "word" and t[i][1] in ("true", "false"):
                            named[name] = t[i][1] == "true"
                            i += 1
                        else:
                            r = parse_literal(t, i, "jsonb")
                            if not r:
                                ok = False
                                break
                            named[name], i = r
                    elif t[i][0] == "word" and t[i][1] in ("true", "false"):
                        args.append(t[i][1] == "true")
                        i += 1
                    else:
                        typ = "jsonpath" if len(args) == 1 else "jsonb"
                        r = parse_literal(t, i, typ)
                        if not r:
                            ok = False
                            break
                        v, i = r
                        args.append(v)
                    if i < len(t) and t[i] == ("op", ","):
                        i += 1
                if not ok or i != len(t) - 1 or len(args) < 2:
                    continue
                doc, path = args[0], args[1]
                vars_ = args[2] if len(args) > 2 else named.get("vars", "{}")
                silent = args[3] if len(args) > 3 else named.get("silent", False)
                if not isinstance(vars_, str) or not isinstance(silent, bool):
                    continue
                g.query(fn, doc, path, vars_, silent)


# ------------------------------------------------------------ parse cases

PARSE_CASES = [
    "", " ", "$", "strict $", "lax $", "STRICT $", "Lax $", "strict", "lax", "strict lax $", "$ strict",
    "@", "last", "$.a", "$.a.b.c", "$.*", "$[*]", "$.**", "$.a.**.b", "$.**{2}", "$.**{2 to 5}",
    "$.**{last}", "$.**{last to 2}", "$.**{2 to last}", "$.**{0}", "$.**{ 1 to 2 }", "$.**{1.5}", "$.**{-1}",
    "$.**{0x10}", "$.**{1_0}", "$.**{2147483647}", "$.**{2147483648}", "$.**{4294967295}", "$.**{a}",
    "$.**{}", "$.**{1 to}", "$.**{to 1}", "$.**{1, 2}", "$**", "$.** .a", "$.**.**",
    "$[0]", "$[1,2]", "$[1 to 3]", "$[last]", "$[last - 1]", "$[1 to last]", "$[$.a]", "$[@]",
    "$[1, 2 to 3, last]", "$[*, 1]", "$[]", "$[1,]", "$[,1]", "$[1 to 2 to 3]", "$[-1]", "$[1.5]", "$[\"a\"]",
    "$[0][1][2]", "$[*][*]", "$.a[*].b[0 to 2].c", "$[ 1 ]", "$[\t1\n]",
    "$.a ? (@ > 1)", "$ ? (@.a == 1 && @.b != 2 || !(@.c < 3))", "$ ? (exists(@.a))", "$ ? (@ starts with \"a\")",
    "$ ? (@ starts with $x)", "$ ? (@ starts with 1)", "$ ? (@ like_regex \"^a\")",
    "$ ? (@ like_regex \"^a\" flag \"i\")", "$ ? (@ like_regex \"^a\" flag \"ismq\")",
    "$ ? ((@ > 1) is unknown)", "$ ? (@ > 1 is unknown)", "$ ? (!(@ > 1))", "$ ? (!@ > 1)", "$ ? (@)",
    "$ ? ($)", "$ ? (1)", "$ ? (true)", "$ ? (@ == true)", "$ ? (@ ? (@ > 1))", "$ ? (@.a ? (@ > 1) > 2)",
    "$ ? (exists(@ ? (@ > 1)))", "$ ? (@ == @)", "$ ? (@[*] > 2)", "$ ? (@.** == 1)", "$ ?(@>1)", "$?(@>1)",
    "$ ? (@ > 1) ? (@ < 5)", "$ ? (@ > 1).a", "$.a ? (@.b ? (@.c == 1) != null)",
    "$ == 1", "$ != 1", "$ <> 1", "$ < 1", "$ <= 1", "$ > 1", "$ >= 1", "$ = 1", "$ === 1", "$ !== 1",
    "$ < > 1", "$ =< 1", "$ => 1", "$ ! = 1", "1 == 1 == 1", "$ == 1 && $ == 2", "$ == 1 || $ == 2",
    "$ == 1 && $ == 2 || $ == 3", "$ == 1 || $ == 2 && $ == 3", "($ == 1 || $ == 2) && $ == 3",
    "!($ == 1)", "!$ == 1", "!!($ == 1)", "!(!($ == 1))", "exists($)", "exists ($.a)", "EXISTS($)",
    "exists($ ? (@ > 1))", "exists", "exists()", "(exists($))", "exists($).a", "(exists($)).a",
    "($ == 1).type()", "($ == 1) is unknown", "$ == 1 is unknown", "(1).a", "(1).a.b", "(\"a\").b",
    "1 + 2", "1 - 2", "1 * 2", "1 / 2", "1 % 2", "1 + 2 * 3", "(1 + 2) * 3", "1 * (2 + 3)", "1 - (2 - 3)",
    "1 - 2 - 3", "1 / 2 / 3", "-1", "+1", "--1", "-+1", "+-1", "- 1", "-(1)", "-(-1)", "-$", "+$", "-$.a",
    "-(1).a", "-(1 + 2)", "+(1 + 2)", "-(1 + 2).a", "1 + -2", "1 - -2", "1--2", "1+-+-2", "$-1", "$+1",
    "$.a+1", "$.a-1", "$.a*1", "$.a/1", "$.a%1", "$ - - - 1", "2 * -3", "2*-$.a", "-$.a.b",
    "$a", "$a.b", "$\"a\"", "$\"a b\"", "$\"\"", "$\"a\".b", "$a$b", "$ a", "$1", "$_a", "$a_b",
    "$\"" + BS + "u0041\"", "$a" + BS + "u0041", "$" + BS + "u0041",
    "null", "true", "false", "\"a\"", "\"\"", "1", "1.5", "-1.5", "1e3", "1E3", "1e+3", "1e-3", ".5", "5.",
    "0", "00", "007", "0.0", "0.000", "1.50", "1_000", "1__000", "_1", "1_", "1._5", "1_.5", "0x1F", "0X1f",
    "0x", "0x_1", "0xG", "0o17", "0o8", "0b101", "0b2", "1e", "1e+", "1e-", "1.e", "1.2e", "1a", "1.2a",
    "1e3a", "1.e3", "1..2", "1.2.3", ".e3", "1.a", "1.type()", "1.2.type()", "(1.2).type()", "1.e3.e",
    "(1.e3).e", "1?(2>3)", "0.0010e-1", "123456789012345678901234567890", "1e1000", "1e1000000", "1e-1000000",
    "1e131072", "0e-20000", "1_000.000_005", "1_000.5e0_1", "0b_10", "0o_7", "0x_ff", "0xffffffffffffffffffff",
    "null.type()", "true.type()", "\"a\".type()", "$.null", "$.true", "$.false", "$.NULL", "$.True",
    "NULL", "TRUE", "False", "Null.a", "$.to", "$.is", "$.lax", "$.strict", "$.last", "$.exists", "$.flag",
    "$.with", "$.starts", "$.like_regex", "$.unknown", "$.datetime", "$.keyvalue", "$.type", "$.size",
    "$.abs", "$.floor", "$.ceiling", "$.double", "$.bigint", "$.boolean", "$.date", "$.decimal", "$.integer",
    "$.number", "$.string", "$.time", "$.time_tz", "$.timestamp", "$.timestamp_tz", "$.TYPE", "$.Size",
    "$.type()", "$.TYPE()", "$.size()", "$.abs()", "$.floor()", "$.ceiling()", "$.double()", "$.keyvalue()",
    "$.bigint()", "$.boolean()", "$.date()", "$.integer()", "$.number()", "$.string()", "$.decimal()",
    "$.decimal(1)", "$.decimal(4,2)", "$.decimal(4, 2)", "$.decimal(+4, -2)", "$.decimal(-1)", "$.decimal(1,2,3)",
    "$.decimal(1.5)", "$.decimal(a)", "$.decimal(1,)", "$.decimal(,1)", "$.decimal(0x10)", "$.decimal(99999999999)",
    "$.datetime()", "$.datetime(\"yyyy\")", "$.datetime(1)", "$.datetime(\"a\", \"b\")", "$.time()", "$.time(1)",
    "$.time(-1)", "$.time(1.5)", "$.time_tz(3)", "$.timestamp(6)", "$.timestamp_tz(0)", "$.timestamp(99999999999)",
    "$.date(1)", "$.type(1)", "$.size(", "$.size", "$.unknown()", "$.foo()", "$.a()", "$.\"type\"()",
    "$.keyvalue().key", "$.keyvalue().value.a", "$.a.keyvalue()[*]", "$.double().floor().ceiling().abs()",
    "$.\"a\"", "$.\"a b\"", "$.\"\"", "$.\"a\\\"b\"", "$.'a'", "$.a.\"b\".c", "$.\"" + BS + "u0041\"",
    "$.a b", "$.a  b", "$ .a", "$. a", "$.a .b", "$ . a", "$ ? ( @ . a > 1 )", "$.a/**/.b", "$.a /**/ .b",
    "$/* comment */.a", "$ /* c */ ? (@ > 1)", "/* only */", "$ /* unterminated", "$ */", "$ /**/", "/**/$",
    "$.a/**/", "$.\"a\"/**/.b", "$.a/*", "$.a/", "$.a/b", "$.a * b", "$.a.*b", "$.*.a", "$.**b",
    "$.a\tb", "$.a\nb", "$\n.a", "$\r\n.a\f", "\t$\t",
    "$.é", "$.日本", "$.\"日本\"", "$.a😀", "\"😀\"", "1é", "1.5é", "1e5é", "$.aé b", "$é",
    "\"" + BS + "b" + BS + "f" + BS + "n" + BS + "r" + BS + "t" + BS + "v" + BS + "\"" + BS + "/" + BS + BS + "\"",
    "\"" + BS + "x41\"", "\"" + BS + "x4\"", "\"" + BS + "x\"", "\"" + BS + "xGG\"", "\"" + BS + "x00\"",
    "\"" + BS + "xff\"", "\"" + BS + "u0041\"", "\"" + BS + "u00e9\"", "\"" + BS + "u004\"", "\"" + BS + "u\"",
    "\"" + BS + "u{41}\"", "\"" + BS + "u{1F600}\"", "\"" + BS + "u{}\"", "\"" + BS + "u{1234567}\"",
    "\"" + BS + "u{110000}\"", "\"" + BS + "u{10FFFF}\"", "\"" + BS + "u0000\"", "\"" + BS + "u{0}\"",
    "\"" + BS + "ud83d" + BS + "ude00\"", "\"" + BS + "ud83d\"", "\"" + BS + "ude00\"",
    "\"" + BS + "ud83d" + BS + "ud83d\"", "\"" + BS + "ud83d" + BS + "u0041\"", "\"" + BS + "ud83d x\"",
    "\"" + BS + "u{d83d}" + BS + "u{de00}\"", "\"" + BS + "u0041" + BS + "\"", "\"" + BS + "u0041" + BS + "n\"",
    "\"" + BS + "z\"", "\"" + BS + "é\"", "\"" + BS + "\n\"", "\"" + BS + "\"", "\"" + BS, "\"abc", "\"",
    "$.a" + BS + "u0042", "$." + BS + "u0061", "$." + BS + "x61b", BS + "u0061", BS + "x24",
    "$.a" + BS, "$." + BS + "n", BS, "$\"" + BS + "u004", "$\"abc",
    "$ ? (@ like_regex \"(\")", "$ ? (@ like_regex \"a\" flag \"x\")", "$ ? (@ like_regex \"a\" flag \"qx\")",
    "$ ? (@ like_regex \"a\" flag \"z\")", "$ ? (@ like_regex \"a\" flag \"é\")", "$ ? (@ like_regex \"a\" flag \"\")",
    "$ ? (@ like_regex \"a\" flag \"iiii\")", "$ ? (@ like_regex \"a\" flag \"msi\")", "$ ? (@ like_regex 1)",
    "$ ? (@ like_regex \"a\" flag i)", "$ ? (@ like_regex $x)", "$ like_regex \"a\"", "$.a like_regex \"a\" flag \"s\"",
    "$ ? (@ like_regex \"a\") like_regex \"b\"", "$ ? (@ like_regex \"[\")", "$ ? (@ like_regex \"a{2,1}\")",
    "$ ? (@ like_regex \"\\\\\")", "$ ? (@ like_regex \"(a)\\\\1\")", "$ ? (@ like_regex \"(?i)a\")",
    "$ starts with \"a\"", "$ starts with $x", "$ starts with", "$ starts \"a\"", "$ starts with \"a\" starts with \"b\"",
    "$ ? (@ starts with \"a\" && @ starts with \"b\")",
    "@ + 1", "$ ? (@ > last)", "$[last ? (@ > 1)]", "$[$[last]]", "$[$ ? (last > 1)]", "$ ? (last > 0)",
    "$[1 to (last)]", "$[last].a", "($[last])", "$[@[last]]", "$ ? (@[last] > 1)", "$[exists(last)]",
    "$ ? (@ ? (@ == $))", "$.a ? (@.b == $.c)", "$ ? (@.a[*] ? (@ > 1) > 1)",
    "(($))", "((($ + 1)).a + ((2)).b ? ((((@ > 1)) || (exists(@.c)))))", "($).a", "($.a.b).c.d",
    "($.a.b + -$.x.y).c.d", "(-+$.a.b).c.d", "1 + ($.a.b + 2).c.d", "1 + ($.a.b > 2).c.d", "($ < 1) || $.a.b <= $x",
    "(", ")", "()", "(()", "($", "$)", "($))", "[", "]", "$[", "$]", "{", "}", "$.{", "$.}", "#", "$#", "$ & $",
    "$ | $", "$ && ", "&& $", "$ ||", "$ ? ", "$ ? (", "$ ? ()", "$ ? (@ > 1", "$ ? @ > 1", "$ ? (@ > 1))",
    "$,", "$, $", ":", "$:", "$.:", "!", "$!", "$ !", "<", "$ <", "> $", "$ == ", "== $", "?", "@?",
    "$ $", "$ 1", "1 $", "1 2", "\"a\" \"b\"", "$.a $.b", "$ . . a", "$..a", "$.", "$.*.", "$[*].",
    "$ ? (@ == 1) ? ", "$ is unknown", "$ == 1 is unknown", "(1) is unknown", "(@) is unknown", "(true) is unknown",
    "1.e", "1..e", "1..e3", "(1.).e", "(1.).e3", "1.2.e", "(1.2).e", "1.2e3", "1.2e3a", "1.2.e3", "(1.2).e3",
    "0b", "1b", "0b0x", "0o", "1o", "0o0x", "1x", "0x0y", "100__000", "_1_000.5", "1_000_.5", "1_000._5",
    "1_000.5_", "1_000.5e_1", "0755", "1_2_3", "0x1EEE_FFFF", "0o2_73", "0b10_0101", "1_000.", ".000_005",
    "$ ? (@.a < .1e1)", "$ ? (@.a < -.1e-1)", "$ ? (@.a < +10.1e+1)", "$ ? (@ == -0)", "$ ? (@ == -0.0)",
    "$.a[1,2, 3 to 16]", "$.a[$a + 1, ($b[*]) to -($[0] * 2)]", "$.a[$.a.size() - 3]",
    "$.g ? (+@.x >= +-(+@.a + 2))", "$.g ? (@.x >= @[*]?(@.a > \"abc\"))", "1 * 2 + 4 % -3 != false",
    "$--+1", "$.a/+-1", "$ ? (@.a < 1) ? (@.b > 2)",
]


def parse_cases(g):
    for p in PARSE_CASES:
        g.parse(p)
    # deep nesting and long chains (bison stack, recursion limits)
    def rep(pre, r, n, mid="", post="", kind="in_rep"):
        g.r.case(kind, SQL[kind], [pre, r, n, mid, post, n])
        g.r.records[-1]["args"] = [pre, r, n, mid, post]

    for n in [1, 10, 100, 1000, 4000]:
        rep("$", ".a", n)
        rep("", "(", n, "$", ")")
        rep("", "-", n, "$")
        rep("$", "[0]", n)
        rep("", "!(", min(n, 1000), "$ == 1", ")")
        rep("1", " + 1", n)
    # bison's stack limit (YYMAXDEPTH) is exact
    for n in [9990, 9995, 9996, 9997, 10000]:
        rep("", "(", n, "1", ")")
        rep("", "-", n, "$")
    for n in [4996, 4997, 4998]:
        rep("", "!(", n, "$ == 1", ")")
    # check_stack_depth() limits depend on the caller's stack usage, so only
    # test depths clearly below / above them
    for n in [11000, 11700, 12100, 13000]:
        rep("$", ".a", n)
    for n in [14000, 14300, 14800, 20000]:
        rep("$", ".a", n, kind="valid_rep")
        rep("1", " + 1", n, kind="valid_rep")


# ------------------------------------------------------------ documents

DOCS = [
    "null", "true", "false", "0", "1", "-1.5", "1.50", "12345678901234567890.123", "1e-5", '""', '"a"', '"abc"',
    '"Hello World"', '"é日本"', "[]", "{}", "[1]", "[1, 2, 3]", "[1, \"2\", true, null]", "[[1, 2], [3, 4]]",
    "[[1, [2, [3]]]]", '{"a": 1}', '{"a": 1, "b": 2}', '{"a": {"b": {"c": 1}}}', '{"a": [1, 2, 3]}',
    '{"a": [{"b": 1}, {"b": 2}, {"c": 3}]}', '[{"a": 1}, {"a": 2}, {"b": 3}, 4, "s", null, [5]]',
    '{"a": null, "b": true, "c": "x", "d": 1.5, "e": [], "f": {}}', '{"": 1, "a b": 2, "é": 3}',
    '{"x": [1, [2, [3, [4]]]], "y": {"z": {"w": [5, {"v": 6}]}}}', '[1, 2.5, -3, 0, "4", true, null, [6], {"n": 7}]',
    '["abc", "abd", "b", "ab", "", "aBc", "a\\nb", "ABC"]', '{"a": "foo", "b": "foobar", "c": "bar"}',
    '[1.0, 1, 1.00, 2]', '{"num": "123", "flt": "1.5e3", "bad": "abc", "bool": "yes", "int": " 42 "}',
    '{"k": {"a": 1, "b": [1, 2]}, "l": [{"m": {"n": 1}}]}', '[[], {}, [[]], [{}], {"a": []}]',
    '{"a": [1, 2], "b": {"c": [3, {"d": 4}]}, "e": "f"}',
]

PATHS = [
    "$", "strict $", "$.a", "strict $.a", "$.b", "strict $.b", "$.a.b", "strict $.a.b", "$.a.b.c", "$.*", "strict $.*",
    "$[*]", "strict $[*]", "$[0]", "strict $[0]", "$[1]", "$[last]", "strict $[last]", "$[-1]", "strict $[-1]",
    "$[0 to 1]", "$[1 to 0]", "strict $[1 to 0]", "$[0 to last]", "$[last - 1]", "$[1.9]", "$[-0.5]", "$[100]",
    "strict $[100]", "$[0, 0]", "$[*][*]", "strict $[*][*]", "$[*].a", "strict $[*].a", "$.a[*]", "strict $.a[*]",
    "$.**", "strict $.**", "$.**{0}", "$.**{1}", "$.**{2}", "$.**{1 to 2}", "$.**{last}", "$.**{2 to last}",
    "$.**.a", "strict $.**.a", "$.**{1}.a", "lax $.**.b ? (@ > 1)", "$.a.**", "$.*.*", "$.*[*]", "$[*].*",
    "$.size()", "strict $.size()", "$.a.size()", "$[*].size()", "$.type()", "$[*].type()", "$.a.type()",
    "$.*.type()", "$.abs()", "$[*].abs()", "strict $[*].abs()", "$.floor()", "$[*].floor()", "$.ceiling()",
    "$[*].ceiling()", "$.double()", "$[*].double()", "$.*.double()", "$.bigint()", "$[*].bigint()", "$.*.bigint()",
    "$.integer()", "$[*].integer()", "$.*.integer()", "$.number()", "$[*].number()", "$.*.number()",
    "$.decimal()", "$[*].decimal()", "$.*.decimal(4,1)", "$[*].decimal(3)", "$[*].decimal(2,-1)",
    "$.boolean()", "$[*].boolean()", "$.*.boolean()", "$.string()", "$[*].string()", "$.*.string()",
    "$.keyvalue()", "strict $.keyvalue()", "$.*.keyvalue()", "$[*].keyvalue()", "$.a.keyvalue()",
    "$.keyvalue().key", "$.keyvalue().value", "$.keyvalue().keyvalue()", "$.keyvalue().value.keyvalue()",
    "$.**.keyvalue()", "$.a ? (@ > 1)", "$[*] ? (@ > 1)", "strict $[*] ? (@ > 1)", "$ ? (@ > 1)", "$ ? (@ == 1)",
    "$[*] ? (@ == \"a\")", "$[*] ? (@ < \"b\")", "$[*] ? (@ >= \"ab\")", "$[*] ? (@ != null)", "$[*] ? (@ == null)",
    "$[*] ? (@ == true)", "$[*] ? (@ > false)", "$[*] ? (@ starts with \"ab\")", "$[*] ? (@ like_regex \"^ab\")",
    "$[*] ? (@ like_regex \"^AB\" flag \"i\")", "$[*] ? (@ like_regex \"a.b\" flag \"s\")",
    "$[*] ? (@ like_regex \"^b$\" flag \"m\")", "$[*] ? (@ like_regex \"a.c\" flag \"q\")", "$[*] ? (@ like_regex \".\")",
    "$[*] ? (@.a == 1)", "strict $[*] ? (@.a == 1)", "$[*] ? (@.a > 0 || @.b > 0)", "$[*] ? (@.a > 0 && @.b > 0)",
    "$[*] ? (!(@.a > 0))", "$[*] ? ((@.a > 0) is unknown)", "$[*] ? (exists(@.a))", "strict $[*] ? (exists(@.a))",
    "$[*] ? (@.type() == \"number\")", "$[*] ? (@.size() > 1)", "$.** ? (@.type() == \"number\")",
    "$.** ? (@ > 1)", "$.** ? (@.a == 1)", "$.a[*] ? (@ > $.a[0])", "$ ? (@.a[*] > 2)", "strict $ ? (@.a[*] > 2)",
    "$ ? (@.a == @.b)", "$ ? ($.a == 1)", "$.a + 1", "$.a - 1", "$.a * 2", "$.a / 2", "$.a % 2", "$.a + $.b",
    "$[0] + $[1]", "$[*] + 1", "strict $[*] + 1", "1 + $[*]", "-$", "-$[*]", "-$.a", "+$.a", "-$.*", "strict -$[*]",
    "$.a / 0", "$.a % 0", "1 / 3", "2 / 3 * 3", "1.5 * 2.25", "10 % 3", "-10 % 3", "10.5 % 3", "$.a + \"x\"",
    "$.d * $.d", "$ == 1", "$ != 1", "$.a == 1", "$.a > 0", "$[*] > 2", "strict $[*] > 2", "$.a == $.b",
    "$.x == 1", "strict $.x == 1", "$ == null", "$.a == null", "exists($.a)", "exists($.x)", "strict exists($.x)",
    "!exists($.a)", "$.a starts with \"f\"", "$.a like_regex \"o\"", "($.a > 0) is unknown", "$.a ? (@ > 0) == 1",
    "$.a[$.b]", "$[$.size() - 1]", "$[$[0]]", "$[last ? (@ > 1)]", "$[0 to $.size() - 1]", "$[\"a\"]",
    "$[null]", "$[1 to \"a\"]", "strict $[2147483648]", "$[2147483648]", "$[-2147483649]",
    "$.datetime()", "$[*].datetime()", "$.keyvalue() ? (@.value > 1)", "$.keyvalue().id",
    "$x", "$x.a", "$x[*]", "$ ? (@ == $x)", "$[*] ? (@ > $x)", "$y", "$x + $y", "$x.keyvalue()", "$x.**",
    "$[*] ? (@ == $\"a b\")", "$ ? (@.a starts with $x)", "$.a ? (@ like_regex \"a\") ? (@ != 1)",
    "$.a.b.c.d", "strict $.a.b.c.d", "$..a", "$.**.**", "$.**[*]", "$[*].**", "$.a[*].b", "strict $.a[*].b",
    "$.a[*].c", "strict $.a[*].c", "$.a[1].b", "$.a[*] ? (@.b > 1).b", "$.e[*]", "strict $.e[*]", "$.f.*",
    "strict $.f.*", "$.c.size()", "$.k.keyvalue()", "$.l[0].m.keyvalue()", "$.y.z.w[1].keyvalue()",
    "1", "\"s\"", "null", "true", "1.5", "-2", "$.a.double() + 1", "\"1.5\".double()", "\"1.5\".number()",
    "\"abc\".number()", "\"1e400\".double()", "\"NaN\".double()", "\"inf\".number()", "\"NaN\".decimal()",
    "\"12.345\".decimal(4,2)", "\"12.345\".decimal(3,2)", "\"1\".bigint()", "\"1.5\".bigint()", "\"0x10\".integer()",
    "\" 7 \".integer()", "\"99999999999\".integer()", "\"9223372036854775808\".bigint()", "\"tr\".boolean()",
    "\"of\".boolean()", "\"o\".boolean()", "1.5.integer()", "(1.5).integer()", "(2.5).bigint()", "(-2.5).integer()",
    "(0).boolean()", "(1.0).boolean()", "(10).boolean()", "(1e10).boolean()", "true.string()", "(1.50).string()",
    "null.string()", "$.string().string()", "(12.5).decimal(2)", "(12.5).decimal(3)", "(12.5).decimal(1001)",
    "(12.5).decimal(0)", "(12.5).decimal(5, 1001)", "(12.5).decimal(5, -1001)", "(1234.5).decimal(5, -2)",
    "(1).decimal(2147483648)", "(1).decimal(5, 2147483648)", "1.size()", "(1).size()", "strict (1).size()",
    "(1).type()", "null.type()", "\"x\".type()", "(true).type()", "[1].type()", "$.a.abs().floor()",
    "(-1.5).abs()", "(-1.5).floor()", "(-1.5).ceiling()", "(1.5).floor()", "(1.5).ceiling()", "(-0.0).abs()",
    "(1.234).double()", "(1e300).double() * 10", "(\"1.23456789012345678\").double()",
]

VARS = ['{}', '{"x": 1}', '{"x": "a", "y": 2}', '{"x": [1, 2], "y": {"z": 3}}', '{"x": {"a": 1, "b": [2]}}',
        '{"a b": "abc"}', '[]', '1', 'null']


def systematic(g):
    # every path on a rotating subset of the documents
    nd = len(DOCS)
    for pi, path in enumerate(PATHS):
        for k in range(8):
            g.query("query", DOCS[(pi * 7 + k * 4) % nd], path)
        for k in range(4):
            doc = DOCS[(pi * 5 + k * 9 + 1) % nd]
            g.query("query", doc, path, silent=True)
            g.query("exists", doc, path)
        for k in range(3):
            doc = DOCS[(pi * 3 + k * 11 + 2) % nd]
            g.query("exists_op", doc, path)
            g.query("match_op", doc, path)
    # all functions on a smaller grid
    for doc in DOCS[::5]:
        for path in PATHS[::4]:
            for fn in QUERY_FNS:
                for silent in (False, True):
                    g.query(fn, doc, path, silent=silent)
    # variables
    for doc in ['{"a": 1, "b": [1, 2, 3]}', '[1, 2, 3]', '"abc"']:
        for path in [p for p in PATHS if "$x" in p or "$y" in p or "$\"" in p] + ["$", "$.a"]:
            for vars_ in VARS:
                for fn in ["query", "exists", "match"]:
                    for silent in (False, True):
                        g.query(fn, doc, path, vars_, silent)


# ------------------------------------------------------------ random

class RandomPaths:
    """Grammar-based random jsonpath generator (mostly valid paths)."""

    KEYS = ["a", "b", "c", "x", "key", "value", "id", "", "a b", "é", "1", "type", "last", "size", "true",
            "strict", "aa", "A"]

    def __init__(self, rnd):
        self.rnd = rnd

    def ws(self):
        r = self.rnd.random()
        if r < 0.7:
            return " "
        if r < 0.85:
            return ""
        return self.rnd.choice(["  ", "\n", "\t", " /* c */ "])

    def key(self):
        k = self.rnd.choice(self.KEYS)
        if k and re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", k) and self.rnd.random() < 0.7:
            return "." + k
        return "." + json.dumps(k, ensure_ascii=False)

    def number(self):
        return self.rnd.choice(["0", "1", "2", "3", "-1", "1.5", "0.5", "-2.25", "10", "100", "1e2", "2.50",
                                ".5", "1_000", "0x10", "7", "1e-3"])

    def literal(self):
        r = self.rnd.random()
        if r < 0.45:
            return self.number()
        if r < 0.75:
            return json.dumps(self.rnd.choice(["a", "b", "abc", "ab", "", "x", "1", "1.5", "true", "é"]),
                              ensure_ascii=False)
        return self.rnd.choice(["true", "false", "null"])

    def primary(self, depth, in_filter, in_sub):
        r = self.rnd.random()
        if in_filter and r < 0.35:
            p = "@"
        elif r < 0.6:
            p = "$"
        elif r < 0.67:
            p = self.rnd.choice(["$x", "$y", "$\"x\""])
        elif in_sub and r < 0.75:
            p = "last"
        else:
            p = self.literal()
            if self.rnd.random() < 0.3:
                p = "(" + p + ")"
        n = self.rnd.choice([0, 1, 1, 2, 2, 3, 4])
        for _ in range(n):
            p += self.accessor(depth, in_filter, in_sub, p)
        return p

    def accessor(self, depth, in_filter, in_sub, prev):
        r = self.rnd.random()
        if r < 0.25:
            return self.key()
        if r < 0.32:
            return ".*"
        if r < 0.42:
            return "[*]"
        if r < 0.52 and depth < 3:
            subs = []
            for _ in range(self.rnd.choice([1, 1, 1, 2, 3])):
                f = self.subscript(depth + 1, in_filter)
                if self.rnd.random() < 0.3:
                    f += " to " + self.subscript(depth + 1, in_filter)
                subs.append(f)
            return "[" + ", ".join(subs) + "]"
        if r < 0.58:
            return self.rnd.choice([".**", ".**{1}", ".**{0}", ".**{2}", ".**{1 to 2}", ".**{last}",
                                    ".**{2 to last}", ".**{0 to 1}"])
        if r < 0.78:
            m = self.rnd.choice(["type", "size", "abs", "floor", "ceiling", "double", "keyvalue", "bigint",
                                 "boolean", "integer", "number", "string", "decimal", "keyvalue", "size"])
            if m == "decimal" and self.rnd.random() < 0.5:
                return ".decimal(" + self.rnd.choice(["2", "3", "4, 1", "5, -1", "1, 0", "6,2"]) + ")"
            return "." + m + "()"
        if depth < 3:
            return self.ws() + "?" + self.ws() + "(" + self.pred(depth + 1, True, in_sub) + ")"
        return self.key()

    def subscript(self, depth, in_filter):
        r = self.rnd.random()
        if r < 0.5:
            return self.rnd.choice(["0", "1", "2", "-1", "last", "last - 1", "1.5", "100", "0"])
        return self.expr(depth, in_filter, True)

    def expr(self, depth, in_filter, in_sub=False):
        r = self.rnd.random()
        if depth >= 3 or r < 0.55:
            return self.primary(depth, in_filter, in_sub)
        if r < 0.8:
            op = self.rnd.choice(["+", "-", "*", "/", "%"])
            return self.expr(depth + 1, in_filter, in_sub) + self.ws() + op + self.ws() + \
                self.expr(depth + 1, in_filter, in_sub)
        if r < 0.9:
            return self.rnd.choice(["-", "+"]) + self.expr(depth + 1, in_filter, in_sub)
        return "(" + self.expr(depth + 1, in_filter, in_sub) + ")"

    def pred(self, depth, in_filter, in_sub=False):
        r = self.rnd.random()
        if depth >= 4 or r < 0.5:
            op = self.rnd.choice(["==", "!=", "<", "<=", ">", ">=", "<>"])
            return self.expr(depth + 1, in_filter, in_sub) + self.ws() + op + self.ws() + \
                self.expr(depth + 1, in_filter, in_sub)
        if r < 0.62:
            return self.pred(depth + 1, in_filter, in_sub) + self.ws() + self.rnd.choice(["&&", "||"]) + \
                self.ws() + self.pred(depth + 1, in_filter, in_sub)
        if r < 0.68:
            return "!(" + self.pred(depth + 1, in_filter, in_sub) + ")"
        if r < 0.76:
            return "exists" + self.ws() + "(" + self.expr(depth + 1, in_filter, in_sub) + ")"
        if r < 0.82:
            return "(" + self.pred(depth + 1, in_filter, in_sub) + ") is unknown"
        if r < 0.9:
            return self.expr(depth + 1, in_filter, in_sub) + " starts with " + \
                self.rnd.choice(['"a"', '"ab"', '""', "$x", '"é"'])
        pat = self.rnd.choice(["a", "^a", "b$", "^.*$", "a.c", "[0-9]+", "(a|b)", "^$", "é", "A"])
        flags = self.rnd.choice(["", "", ' flag "i"', ' flag "s"', ' flag "m"', ' flag "q"', ' flag "iq"',
                                 ' flag "sm"'])
        return self.expr(depth + 1, in_filter, in_sub) + " like_regex " + json.dumps(pat, ensure_ascii=False) + \
            flags

    def path(self):
        mode = self.rnd.choice(["", "", "", "lax ", "strict ", "strict "])
        if self.rnd.random() < 0.25:
            return mode + self.pred(1, False)
        return mode + self.expr(1, False)


def mutate(rnd, s):
    """Small random edit, to produce syntax errors and odd tokens."""
    pieces = ["(", ")", "[", "]", "$", "@", ".", "?", "*", "**", "\"", "1", "a", " ", "==", "&&", "!", ",",
              "{", "}", "last", "to", "-", "+", "/*", "*/", BS, "e", "_", "0x", "é", "#", ":", "'"]
    if not s:
        return rnd.choice(pieces)
    i = rnd.randrange(len(s) + 1)
    r = rnd.random()
    if r < 0.4:
        return s[:i] + rnd.choice(pieces) + s[i:]
    if r < 0.8 and i < len(s):
        j = min(len(s), i + rnd.randint(1, 3))
        return s[:i] + s[j:]
    return s[:i] + rnd.choice(pieces) + s[i + 1:]


def rnd_doc(rnd, depth=0):
    x = rnd.random()
    if depth > 2 or x < 0.4:
        return rnd.choice([
            "null", "true", "false", "0", "1", "2", "-1", "1.5", "2.50", "10", "-0.5", "100", "1e2",
            '"a"', '"b"', '"abc"', '""', '"1"', '"1.5"', '"x"', '"true"', '"ab"', '"é"', '"A"',
        ])
    if x < 0.7:
        return "[" + ", ".join(rnd_doc(rnd, depth + 1) for _ in range(rnd.randint(0, 4))) + "]"
    keys = [rnd.choice(["a", "b", "c", "x", "key", "value", "id", "", "a b", "é", "1", "type", "aa", "A"])
            for _ in range(rnd.randint(0, 4))]
    return "{" + ", ".join(json.dumps(k, ensure_ascii=False) + ": " + rnd_doc(rnd, depth + 1) for k in keys) + "}"


RANDOM_VARS = ['{}', '{"x": 1}', '{"x": "a", "y": [1, 2]}', '{"x": {"a": 1}, "y": 2.5}', '{"y": null}']


def random_cases(g, n_parse, n_exec):
    rnd = random.Random(20261025)
    rp = RandomPaths(rnd)
    for _ in range(n_parse):
        p = rp.path()
        if rnd.random() < 0.3:
            p = mutate(rnd, p)
        g.parse(p)
    for _ in range(n_exec):
        p = rp.path()
        doc = rnd_doc(rnd)
        vars_ = rnd.choice(RANDOM_VARS)
        r = rnd.random()
        fn = "query" if r < 0.55 else rnd.choice(["query_first", "exists", "match", "exists_op", "match_op",
                                                  "query_array"])
        g.query(fn, doc, p, vars_, rnd.random() < 0.4)


# ------------------------------------------------------------ targeted cases

KV_DOCS = [
    '{"a": 1}', '{"a": {"b": 1}}', '{"x": 1, "a": {"b": [1, {"c": 2}]}}', '[{"a": 1}, {"b": 2}, {"c": {"d": 3}}]',
    '{"n1": 1.5, "n2": 12345678901234567890, "n3": 0.0000001, "s": "abc", "o": {"k": "v"}}',
    '{"aaa": "é日本", "b": {"c": {"d": {"e": 1}}}, "z": [[], {}, [{"q": 1}]]}',
    '{"long": "' + "x" * 100 + '", "after": {"k": 1}}',
    '[1, 1.5, 100000000, 0.0001, "s", true, null, {"a": 1}, [{"b": 2}]]',
    '{"num": 1e100, "obj": {"x": 1}}', '{"num": 1.000000000000000000000000000000000000000000000000000000000000000001, "obj": {"x": 1}}',
    '{"num": 1e-70, "obj": {"x": 1}}', '{"num": 123456789e300, "obj": {"x": 1}}',
    '{"a": {}, "b": {"c": {}}}', '{"": {"": {"": 1}}}',
]

KV_PATHS = [
    "$.keyvalue()", "$.*.keyvalue()", "$.**.keyvalue()", "$[*].keyvalue()", "$.*.*.keyvalue()",
    "$.keyvalue().value.keyvalue()", "$.keyvalue().keyvalue()", "$.keyvalue().keyvalue().keyvalue()",
    "$.keyvalue() ? (@.value.type() == \"object\").value.keyvalue()", "$.**.keyvalue().id",
    "$.keyvalue().value.**.keyvalue()", "strict $.**.keyvalue()", "$.keyvalue().value[*].keyvalue()",
    "$.keyvalue() ? (@.id > 0)", "$.keyvalue().value.keyvalue().value.keyvalue()",
]

EXTRA = [
    # (doc, path, vars, silent)
    ('{"a": 1}', "$x.keyvalue()", '{"x": {"a": 1, "b": {"c": 2}}}', False),
    ('{"a": 1}', "$x.*.keyvalue()", '{"y": 1, "x": {"a": 1, "b": {"c": 2}}}', False),
    ('{"a": 1}', "$x.**.keyvalue()", '{"zz": [1, 2, 3], "x": {"a": 1.5, "b": {"c": 2}}}', False),
    ('{"a": {"b": 1}}', "$.a.keyvalue() ? ($x.keyvalue().id != 0)", '{"x": {"k": 1}}', False),
    ('{"a": 1}', "$x", '[]', False), ('{"a": 1}', "$", '1', False), ('{"a": 1}', "$", 'null', True),
    ('{"a": 1}', "$x", '{}', True), ('{"a": 1}', "$ ? (@.a == $x)", '{}', True),
    ('[1, 2]', "$[*] ? (@ == $x)", '{"x": 2}', False), ('[1, 2]', "$[$x]", '{"x": 1}', False),
    ('[1, 2]', "$[$x]", '{"x": 1.7}', False), ('[1, 2]', "$[$x]", '{"x": "1"}', False),
    ('[1, 2]', "$[$x]", '{"x": [1]}', False), ('[1, 2]', "$[$x to last]", '{"x": 0}', False),
    ('{"a": 1}', "$x + $y", '{"x": 1, "y": 2}', False), ('{"a": 1}', "$x + $y", '{"x": 1, "y": "2"}', False),
    ('{"a": 1}', "$x starts with $y", '{"x": "abc", "y": "ab"}', False),
    ('{"a": 1}', "$x like_regex \"b\"", '{"x": "abc"}', False),
    ('{"a": 1}', '$"x y"', '{"x y": 7}', False), ('{"a": 1}', '$"x\\"y"', '{"x\\"y": 7}', False),
]

NUM_PATHS = [
    "$ + 1", "$ - 1", "$ * 2", "$ / 3", "$ % 3", "$ / 0", "$ % 0", "-$", "+$", "$.abs()", "$.floor()",
    "$.ceiling()", "$.double()", "$.integer()", "$.bigint()", "$.number()", "$.decimal()", "$.decimal(3)",
    "$.decimal(5,2)", "$.decimal(1,0)", "$.decimal(2,-1)", "$.decimal(1000, 999)", "$.boolean()", "$.string()",
    "$ * $", "$ / 7 * 7", "1 / $", "$ % -7", "$.type()", "$.size()", "$ == 1", "$ < 0", "$ ? (@ > 0)",
    "$.double() * 2", "$.integer() + 1", "$ + 0.0000000001", "$ * 1e-10", "$ / 1e10",
]

NUM_DOCS = [
    "0", "1", "-1", "0.5", "1.5", "2.5", "-2.5", "0.1", "1e2", "1.50", "100.000", "123456789",
    "2147483647", "2147483648", "-2147483648", "-2147483649", "9223372036854775807", "9223372036854775808",
    "-9223372036854775808", "-9223372036854775809", "12345678901234567890123456789", "1e-20", "1e20",
    "0.000000000000000000000000000001", "1.7976931348623157e308", "1e308", "1e309", "1e-310", "1e-400",
    "3.141592653589793238462643383279", "-0.0", "1e1000", "99999999999999999999999999.999999999999",
    "0.333333333333333333333333333333333333333333333333333333333333333333333333333333333333",
]

STR_DOCS = [
    '"1"', '"1.5"', '" 1.5 "', '"-0"', '"1e3"', '"1e400"', '"1e-400"', '"0x1F"', '"0x1p3"', '"1_000"',
    '"NaN"', '"nan"', '"Infinity"', '"-inf"', '"+inf"', '"inf"', '"abc"', '""', '" "', '"1.23456789012345678"',
    '"0.1"', '"123456789012345678"', '"9223372036854775807"', '"9223372036854775808"', '"-9223372036854775808"',
    '"2147483647"', '"2147483648"', '" 42 "', '"4 2"', '"t"', '"TRUE"', '"y"', '"yes"', '"n"', '"no"', '"on"',
    '"off"', '"o"', '"1"', '"0"', '"00"', '"truex"', '"0b101"', '"0o17"', '"1.5e2"', '"5."', '".5"',
    '"1.99999999999999999999"', '"4.35"', '"1e15"', '"1e16"', '"12345678.9"', '"-1.5"', '"0.000001234"',
]

STR_PATHS = ["$.double()", "$.number()", "$.decimal()", "$.decimal(4,1)", "$.integer()", "$.bigint()",
             "$.boolean()", "$.string()", "$.double().string()", "$.number() + 1", "$.size()", "$.type()",
             "$ starts with \"1\"", "$ like_regex \"^[0-9]+$\"", "$.datetime()"]

REGEX_CASES = [
    ('["abc", "ABC", "aBc", "é", "É", "x\\ny", "a.c", "a+c"]', p) for p in [
        '$[*] ? (@ like_regex "abc")', '$[*] ? (@ like_regex "abc" flag "i")', '$[*] ? (@ like_regex "é" flag "i")',
        '$[*] ? (@ like_regex "É" flag "i")', '$[*] ? (@ like_regex "^y" flag "m")', '$[*] ? (@ like_regex "^y")',
        '$[*] ? (@ like_regex "x.y")', '$[*] ? (@ like_regex "x.y" flag "s")', '$[*] ? (@ like_regex "a.c" flag "q")',
        '$[*] ? (@ like_regex "A.C" flag "qi")', '$[*] ? (@ like_regex "a+c" flag "q")', '$[*] ? (@ like_regex "x$" flag "m")',
        '$[*] ? (@ like_regex "x$")', '$[*] ? (@ like_regex "x.y" flag "ms")', '$[*] ? (@ like_regex "(?i)abc")',
        '$[*] ? (@ like_regex "\\\\w+")', '$[*] ? (@ like_regex "[[:upper:]]")', '$[*] ? (@ like_regex "^.$")',
        '$[*] ? (@ like_regex "a.c" flag "x")', 'strict $[*] ? (@ like_regex "a")', '$ ? (@ like_regex "a")',
        '$[*] like_regex "abc"', 'strict $[*] like_regex "abc"', '$ like_regex "abc"',
    ]
] + [
    ('[1, "abc", null, {"a": "abc"}]', p) for p in [
        '$[*] ? (@ like_regex "abc")', 'strict $[*] ? (@ like_regex "abc")', '$[*] like_regex "abc"',
        'strict $[*] like_regex "abc"', '$[*] starts with "a"', 'strict $[*] starts with "a"',
        '$[*] ? (@ starts with "ab")', '$[*] ? (@ starts with $x)',
    ]
]


def targeted(g):
    for doc in KV_DOCS:
        for path in KV_PATHS:
            g.query("query", doc, path)
            g.query("query", doc, path, silent=True)
    for doc, path, vars_, silent in EXTRA:
        for fn in ["query", "exists", "match", "query_first"]:
            g.query(fn, doc, path, vars_, silent)
    for doc in NUM_DOCS:
        for path in NUM_PATHS:
            g.query("query", doc, path)
            g.query("exists", doc, path)
    for doc in NUM_DOCS[::4]:
        for path in NUM_PATHS:
            g.query("query", doc, path, silent=True)
            g.query("query", doc, "strict " + path)
    # numeric overflow at the edge of the numeric range
    for path in ["$ * 10", "$ * $", "$ + $ * 9", "$ / 0.1", "-$ * 10", "$.floor() * 10", "$ % 7",
                 "$ > 1", "$.type()"]:
        g.query("query", "1e131071", path)
        g.query("query", "1e131071", path, silent=True)
    g.query("query", "0.000001", "$ / 1e16383")
    g.query("query", "1e-16383", "$ * 0.1")
    g.query("query", "1e-16383", "$ / 10")
    for doc in STR_DOCS:
        for path in STR_PATHS:
            g.query("query", doc, path)
            g.query("query", doc, path, silent=True)
    for doc, path in REGEX_CASES:
        for fn in ["query", "exists_op", "match_op", "match"]:
            g.query(fn, doc, path, '{"x": "a"}')
    # operators with variables (vars are NULL there) and non-boolean match
    for doc in ['{"a": 1}', '[1, 2]', '1']:
        for path in ["$x", "$ ? (@ == $x)", "exists($x)", "$.a == $x", "$", "$.a", "$ == 1", "$[*] > 1",
                     "$.a + 1", "$.a / 0", "strict $.b", "$.b", "null", "$.a == 1 && $.b == 2"]:
            for fn in ["exists_op", "match_op", "match", "exists"]:
                g.query(fn, doc, path)
                if fn in ("match", "exists"):
                    g.query(fn, doc, path, silent=True)



# ------------------------------------------------------------ execution depth

EXEC_DEPTH = [
    # (fn, docspec, pathspec, [n below the limit, n above])
    ("query_array", ['', '{"a":', '1', '}', ''], ['$', '.a', '', '', ''], [2500, 3100]),
    ("exists", ['', '{"a":', '1', '}', ''], ['strict $', '.a', '', '', ''], [2500, 3100]),
    ("query_array", ['', '[', '1', ']', ''], ['$', '[*]', '', '', ''], [2000, 2500]),
    ("query_array", ['', '[', '1', ']', ''], ['$.** ? (@ == 2)', '', '', '', ''], [9000, 11000]),
    ("query_array", ['', '[{"a":', '1', '}]', ''], ['$', '.a', '', '', ''], [1800, 2250]),
    ("query_array", ['1', '', '', '', ''], ['1', ' + 1', '', '', ''], [6500, 8000]),
    ("match", ['1', '', '', '', ''], ['$ == 1', ' && $ == 1', '', '', ''], [12000, 14000]),
    ("query_array", ['1', '', '', '', ''], ['$ ? (', '@ ? (', '@ == 1', ') == 1', ')'], [1500, 1900]),
    ("query_array", ['', '{"value":', '1', '}', ''], ['$', '.keyvalue().value', '', '', ''], [1350, 1650]),
    ("query_array", ['-1', '', '', '', ''], ['$', '.abs()', '', '', ''], [7800, 9600]),
    ("query_array", ['[0]', '', '', '', ''], ['', '$[', '0', ']', ''], [1250, 1600]),
]


def build(spec, n):
    return "%s || repeat(%s, %s) || %s || repeat(%s, %s) || %s", [spec[0], spec[1], n, spec[2], spec[3], n, spec[4]]


def exec_depth(g):
    for fn, dspec, pspec, ns in EXEC_DEPTH:
        for n in ns:
            dsql, dargs = build(dspec, n)
            psql, pargs = build(pspec, n)
            if fn == "query_array":
                sql = f"SELECT jsonb_path_query_array(({dsql})::jsonb, ({psql})::jsonpath)::text"
            elif fn == "exists":
                sql = f"SELECT jsonb_path_exists(({dsql})::jsonb, ({psql})::jsonpath, '{{}}', false)::text"
            else:
                sql = f"SELECT jsonb_path_match(({dsql})::jsonb, ({psql})::jsonpath, '{{}}', false)::text"
            g.r.case("exec_rep", sql, dargs + pargs)
            g.r.records[-1]["args"] = [fn, dspec, pspec, n]


# ------------------------------------------------------------ datetime

DT_STRINGS = [
    # dates
    "2017-03-10", "2016-02-29", "2017-02-29", "0001-01-01", "9999-12-31", "10000-01-01", "2017-3-10",
    "2017-03-10 ", " 2017-03-10", "17-03-10", "2017/03/10", "20170310", "2017-03-10x", "2017-13-01",
    "2017-00-10", "2017-03-32", "5874897-12-31", "5874898-01-01", "0000-01-01",
    # times
    "12:34:56", "12:34:56.789", "12:34:56.789123", "12:34:56.7891234", "23:59:59.999999", "24:00:00",
    "12:34", "12:60:00", "12:34:60", "1:2:3", "12:34:56+05:30", "12:34:56-08", "12:34:56 +05:30",
    "12:34:56+05:30:15", "12:34:56+16", "12:34:56+15:59", "12:34:56+15:60", "12:34:56 PST", "12:34:56PST",
    "12:34:56 MSK", "12:34:56 XYZ", "12:34:56.123+02", "12:34:56Z", "12:34:56 Z", "00:00:00", "12:34:56+5",
    # timestamps
    "2017-03-10 12:34:56", "2017-03-10T12:34:56", "2017-03-10 12:34:56.789", "2017-03-10t12:34:56",
    "2017-03-10 12:34:56+03", "2017-03-10T12:34:56+03:00", "2017-03-10 12:34:56 +03:00",
    "2017-03-10 12:34:56-08:30", "2017-03-10 12:34:56 PST", "2017-03-10 12:34:56 CET", "2017-03-10 12:34:56 MSK",
    "2017-03-10T12:34:56.123456-05:00", "2017-03-26 02:30:00", "2017-10-29 02:30:00", "2017-03-10 25:00:00",
    "0001-01-01 00:00:00", "2017-03-10 12:34:56.1234567", "294276-12-31 23:59:59", "294277-01-01 00:00:00",
    "2017-03-10 12:34:56+15:59", "2017-03-10  12:34:56", "2017-03-10 12:34:56 ", "2017-06-15 12:00:00",
    "2017-12-31 23:59:59.999999+00", "1999-12-31 24:00:00",
    # garbage
    "", " ", "abc", "12", "2017", "2017-03", "T12:34:56", "2017-03-10T", "2017-03-10 12:34:56.", "1e5",
]

DT_METHODS = [
    ".datetime()", ".date()", ".time()", ".time_tz()", ".timestamp()", ".timestamp_tz()", ".time(0)",
    ".time(3)", ".time(9)", ".time_tz(2)", ".timestamp(1)", ".timestamp(6)", ".timestamp_tz(4)",
    ".time(2147483648)", ".datetime().type()", ".datetime().string()", ".date().type()",
    ".time_tz().type()", ".timestamp_tz().string()", ".timestamp_tz().type()",
]

DT_TEMPLATES = [
    ("yyyy-mm-dd", ["2017-03-10", "2017-3-1", "17-03-10", "2017-03-10 12:00", "2017/03/10", "2017-03"]),
    ("dd.mm.yyyy", ["10.03.2017", "1.3.2017", "10.03.17", "10-03-2017", "32.03.2017"]),
    ("mm/dd/yyyy", ["03/10/2017", "13/10/2017"]),
    ("yyyy-mm-dd HH24:MI:SS", ["2017-03-10 12:34:56", "2017-03-10 25:34:56", "2017-03-10 12:34"]),
    ("yyyy-mm-dd HH24:MI:SS TZH", ["2017-03-10 12:34:56 +03", "2017-03-10 12:34:56 -05", "2017-03-10 12:34:56 03",
                                    "2017-03-10 12:34:56 +16", "2017-03-10 12:34:56"]),
    ("yyyy-mm-dd HH24:MI:SS TZH:TZM", ["2017-03-10 12:34:56 +03:30", "2017-03-10 12:34:56 -03:45",
                                        "2017-03-10 12:34:56 +03:60"]),
    ("yyyy-mm-dd HH24:MI:SS TZM", ["2017-03-10 12:34:56 30"]),
    ("yyyy-mm-dd HH24:MI:SS TZ", ["2017-03-10 12:34:56 PST", "2017-03-10 12:34:56 +03", "2017-03-10 12:34:56 MSK",
                                  "2017-03-10 12:34:56 FOO", "2017-03-10 12:34:56 utc", "2017-03-10 12:34:56 -03:30"]),
    ("yyyy-mm-dd HH24:MI:SS OF", ["2017-03-10 12:34:56 +03", "2017-03-10 12:34:56 -03:30", "2017-03-10 12:34:56 PST"]),
    ("HH24:MI", ["12:34", "12:3", "1:2", "24:00"]),
    ("HH24:MI TZH", ["12:34 +02", "12:34"]),
    ("HH12:MI AM", ["12:34 AM", "12:34 PM", "01:34 pm", "13:34 PM", "00:34 AM", "12:34 XM"]),
    ("HH:MI:SS P.M.", ["11:59:59 P.M.", "11:59:59 a.m."]),
    ("DD Mon YYYY", ["10 Mar 2017", "10 mar 2017", "10 MAR 2017", "10 March 2017", "10 Xyz 2017"]),
    ("DD Month YYYY", ["10 March 2017", "10 Mar 2017", "10 marchy 2017"]),
    ("Day, DD Mon YYYY", ["Friday, 10 Mar 2017", "Monday, 10 Mar 2017", "Frid, 10 Mar 2017"]),
    ("Dy DD Mon YYYY", ["Fri 10 Mar 2017"]),
    ("YYYY-DDD", ["2017-069", "2017-366", "2016-366", "2017-0"]),
    ("IYYY-IW-ID", ["2017-10-5", "2017-53-1", "2017-10-8"]),
    ("IYYY-IW", ["2017-10"]),
    ("J", ["2457823", "0", "-1"]),
    ("yyyy-mm-dd\"T\"HH24:MI:SS.FF3", ["2017-03-10T12:34:56.123", "2017-03-10T12:34:56.12", "2017-03-10T12:34:56.1234"]),
    ("HH24:MI:SS.FF6", ["12:34:56.123456", "12:34:56.1"]),
    ("HH24:MI:SS.FF1", ["12:34:56.1", "12:34:56.12"]),
    ("HH24:MI:SS.US", ["12:34:56.123456", "12:34:56.1234567"]),
    ("HH24:MI:SS.MS", ["12:34:56.123", "12:34:56.1"]),
    ("SSSSS", ["45296", "86399", "86400"]),
    ("SSSS", ["45296"]),
    ("YY-MM-DD", ["17-03-10", "69-01-01", "70-01-01", "2017-03-10"]),
    ("YYY-MM-DD", ["017-03-10", "519-03-10", "520-03-10"]),
    ("Y-MM-DD", ["7-03-10"]),
    ("Y,YYY-MM-DD", ["2,017-03-10", "2017-03-10"]),
    ("CC YY-MM-DD", ["21 17-03-10", "21 00-03-10"]),
    ("YYYY-MM-DD BC", ["2017-03-10 BC", "2017-03-10 AD", "0001-01-01 BC"]),
    ("YYYY-RM-DD", ["2017-III-10", "2017-iii-10", "2017-XIII-10"]),
    ("Q YYYY-MM-DD", ["1 2017-03-10"]),
    ("yyyy-mm-dd FX", ["2017-03-10"]),
    ("yyyy mm dd", ["2017 03 10", "2017-03-10"]),
    ("yyyy-mm-dd HH24:MI:SS.FF3TZH", ["2017-03-10 12:34:56.123+02"]),
    ("FMDD.FMMM.YYYY", ["1.3.2017"]),
    ("DDth Mon YYYY", ["10th Mar 2017"]),
    ("yyyy|mm|dd", ["2017|03|10"]),
    ("yyyy%mm", ["2017%03"]),
    ("yyyy-mm-dd" + BS + "x", ["2017-03-10"]),
    ("\"year\" yyyy", ["year 2017", "yeat 2017"]),
    ("yyyy\"-\"mm", ["2017-03"]),
    ("HH24:MI:SS TZ", ["12:34:56 PST", "12:34:56 +02:30"]),
    ("HH24:MI:SS TZH:TZM", ["12:34:56 +02:30", "12:34:56 -15:59"]),
    ("", ["2017-03-10", ""]),
    ("MI", ["30"]),
    ("TZH", ["+03"]),
    ("yyyy-mm-dd TZH", ["2017-03-10 +03"]),
    ("HH24", ["12"]),
    ("yyyy-mm-dd HH24:MI:SS.FF9", ["2017-03-10 12:34:56.123"]),
    ("YYYY-MM-DD IW", ["2017-03-10 10"]),
    ("yyyy-mm-dd hh24:mi:ss", ["2017-03-10 12:34:56"]),
    ("YYYYMMDD", ["20170310", "2017031"]),
    ("YYYYMMDDHH24MISS", ["20170310123456"]),
    ("HH24MI", ["1234"]),
    # IW is not a date field for the result type (DCH_datetime_type)
    ("IW", ["10"]),
    ("IW HH24:MI", ["10 12:34"]),
    ("SSSSS IW", ["45296 10"]),
    ("IW-ID", ["10-5"]),
]

TEMPLATE_KEYWORDS = [
    "YYYY", "YYY", "YY", "Y", "Y,YYY", "IYYY", "IYY", "IY", "MM", "Mon", "MON", "mon", "Month", "MONTH", "DD", "DDD",
    "D", "ID", "IW", "WW", "W", "J", "CC", "Q", "RM", "rm", "HH24", "HH12", "HH", "MI", "SS", "MS", "US", "FF1", "FF2",
    "FF3", "FF4", "FF5", "FF6", "SSSSS", "SSSS", "AM", "PM", "am", "pm", "A.M.", "P.M.", "AD", "BC", "ad", "A.D.",
    "TZH", "TZM", "OF", "Day", "DAY", "Dy", "DY", "FMDD", "FMMM", "FMHH24", "DDTH", "\"T\"", "\"at\"", "TZ",
]
TEMPLATE_SEPS = ["-", ".", "/", ",", ":", ";", " ", "'"]


def template_fuzz(g, n):
    """Random templates with matching inputs rendered by to_char(). to_char()
    runs on a separate session: PostgreSQL's DCH format cache does not reset
    an entry's standard-mode flag when it recycles the entry, so mixing
    to_char() and .datetime() with the same template in one session can make
    .datetime() use a template parsed in non-standard mode."""
    import pgeval
    cur = pgeval.connect().cursor()
    rnd = random.Random(4242)
    stamps = ["2017-03-10 12:34:56.123456+00", "1999-12-31 23:59:59.5+05:30", "2024-02-29 00:00:00-08",
              "0001-01-01 00:00:00+00", "2005-01-02 09:08:07.006+03", "1970-07-15 13:14:15.16171-03:30"]
    for _ in range(n):
        parts = []
        for _ in range(rnd.randint(1, 6)):
            parts.append(rnd.choice(TEMPLATE_KEYWORDS))
            parts.append(rnd.choice(TEMPLATE_SEPS) if rnd.random() < 0.8 else "")
        fmt = "".join(parts).rstrip()
        try:
            cur.execute("SELECT to_char(%s::timestamptz, %s)", (rnd.choice(stamps), fmt))
            st = cur.fetchone()[0]
        except Exception:
            continue
        if st is None:
            continue
        doc = json.dumps(st)
        path = "$.datetime(" + json.dumps(fmt) + ")"
        g.query("query", doc, path)
        if rnd.random() < 0.3:
            g.query("query_tz", doc, path + rnd.choice([".timestamp_tz()", ".date()", ".time()", ".type()"]))

DT_TIMEZONES = ["UTC", "Europe/Warsaw", "America/New_York", "Asia/Kolkata", "+05", "UTC+3",
                "Australia/Lord_Howe", "America/St_Johns"]

DT_CMP_DOCS = [
    '["2017-03-10", "2017-03-11", "2017-03-09", "12:34:56", "01:02:03+04", "2017-03-10 00:00:00",'
    ' "2017-03-10 12:34:56", "2017-03-10 01:02:03+04", "2017-03-10 03:00:00+03"]',
]

DT_CMP_PATHS = [
    '$[*].datetime() ? (@ == "10.03.2017".datetime("dd.mm.yyyy"))',
    '$[*].datetime() ? (@ >= "10.03.2017".datetime("dd.mm.yyyy"))',
    '$[*].datetime() ? (@ < "10.03.2017".datetime("dd.mm.yyyy"))',
    '$[*].datetime() ? (@ == "12:35".datetime("HH24:MI"))',
    '$[*].datetime() ? (@ >= "12:35".datetime("HH24:MI"))',
    '$[*].datetime() ? (@ == "12:35 +1".datetime("HH24:MI TZH"))',
    '$[*].datetime() ? (@ < "12:35 +1".datetime("HH24:MI TZH"))',
    '$[*].datetime() ? (@ == "10.03.2017 12:35".datetime("dd.mm.yyyy HH24:MI"))',
    '$[*].datetime() ? (@ >= "10.03.2017 12:35".datetime("dd.mm.yyyy HH24:MI"))',
    '$[*].datetime() ? (@ == "10.03.2017 12:35 +1".datetime("dd.mm.yyyy HH24:MI TZH"))',
    '$[*].datetime() ? (@ > "10.03.2017 12:35 +1".datetime("dd.mm.yyyy HH24:MI TZH"))',
    '$[*].datetime() ? (@ == "2017-03-10".date())', '$[*].datetime() ? (@ < "2017-03-10".date())',
    '$[*].datetime() ? (@ == "12:34:56".time())', '$[*].datetime() ? (@ > "12:34:56+01".time_tz())',
    '$[*].datetime() ? (@ == "2017-03-10 12:34:56".timestamp())',
    '$[*].datetime() ? (@ <= "2017-03-10 12:34:56+01".timestamp_tz())',
    '$[*].datetime() ? (@ != "2017-03-10".datetime())', 'strict $[*].datetime() ? (@ == "2017-03-10".date())',
    '$[*].datetime() ? ((@ < "2017-03-10".date()) is unknown)', '$[*].datetime() == "2017-03-10".date()',
    '$[*].datetime().type()', '$[*].datetime().string()', '$[*].date()', '$[*].time()', '$[*].timestamp_tz()',
    '$[*].datetime() ? (@ == 1)', '$[*].datetime() ? (@ == "2017-03-10")', '$[*].datetime() ? (@ like_regex "2017")',
    '$[*].datetime() ? (@ starts with "2017")', '$[*].datetime().size()', '$[*].datetime().keyvalue()',
    '$[*].datetime() + 1', '$[*].datetime().datetime()', '$[*].datetime().abs()', '$[*].datetime().double()',
    '$[*].datetime().boolean()', '$[*].datetime().number()',
]


def datetime_cases(g):
    for tz in DT_TIMEZONES:
        g.set_tz(tz)
        full = tz in ("UTC", "Europe/Warsaw")
        for st in DT_STRINGS if full else DT_STRINGS[::3]:
            doc = json.dumps(st)
            for m in DT_METHODS if full else DT_METHODS[:6]:
                g.query("query", doc, "$" + m)
                g.query("query_tz", doc, "$" + m)
                if tz == "UTC":
                    g.query("query", doc, "$" + m, silent=True)
        for doc in DT_CMP_DOCS:
            for path in DT_CMP_PATHS:
                g.query("query", doc, path)
                g.query("query_tz", doc, path)
                if full:
                    g.query("query", doc, path, silent=True)
                    g.query("query_tz", doc, path, silent=True)
                    g.query("exists", doc, path)
                    g.query("exists_tz", doc, path)
        # time -> timetz conversions use the current date
        for st in ["12:34:56", "00:30:00", "23:59:59.5"]:
            doc = json.dumps(st)
            for path in ["$.time_tz()", "$.time_tz().string()", "$.datetime() ? (@ == \"12:34:56+00\".time_tz())",
                         "$.datetime() ? (@ < \"12:34:56+00\".time_tz())"]:
                g.query("query_tz", doc, path)
                g.query("query", doc, path)
    g.set_tz("UTC")
    for fmt, inputs in DT_TEMPLATES:
        for st in inputs:
            doc = json.dumps(st)
            path = "$.datetime(" + json.dumps(fmt) + ")"
            g.query("query", doc, path)
            g.query("query", doc, path, silent=True)
            g.query("query", doc, path + ".type()")
            g.query("query_tz", doc, path + ".timestamp_tz()")
    g.set_tz("Europe/Warsaw")
    for fmt, inputs in DT_TEMPLATES[:12]:
        for st in inputs:
            doc = json.dumps(st)
            path = "$.datetime(" + json.dumps(fmt) + ")"
            g.query("query_tz", doc, path + ".timestamp_tz()")
            g.query("query_tz", doc, path + ".timestamp()")
            g.query("query", doc, path + ".time_tz()")
    # historical offsets (LMT, with seconds), BC dates, extreme offsets
    for tz in ["Europe/Warsaw", "America/New_York", "America/St_Johns", "Asia/Kolkata"]:
        g.set_tz(tz)
        for st in ["1800-01-01", "1800-01-01 12:00:00", "1900-06-01 00:00:00", "0001-01-01 00:00:00",
                   "1883-11-18 12:00:00", "1970-01-01 00:00:00"]:
            doc = json.dumps(st)
            for path in ["$.timestamp_tz()", "$.timestamp_tz().string()", "$.datetime().timestamp_tz()",
                         "$.timestamp_tz().time_tz()", "$.timestamp_tz().timestamp()", "$.timestamp_tz().date()",
                         "$.timestamp_tz() ? (@ == \"" + st + "\".timestamp())"]:
                g.query("query_tz", doc, path)
        for st in ["1800-01-01 12:00:00+00", "0001-01-01 00:00:00+00"]:
            g.query("query", json.dumps(st), "$.time_tz()")
            g.query("query_tz", json.dumps(st), "$.time()")
            g.query("query_tz", json.dumps(st), "$.date()")
    g.set_tz("UTC")
    for st, fmt in [("0001-01-01 BC 12:00:00 +15:59", "YYYY-MM-DD BC HH24:MI:SS TZH:TZM"),
                    ("4713-11-24 BC 00:00:00 +00", "YYYY-MM-DD BC HH24:MI:SS TZH"),
                    ("4714-11-24 BC 00:00:00 +00", "YYYY-MM-DD BC HH24:MI:SS TZH"),
                    ("4713-11-24 BC 00:00:00 -15", "YYYY-MM-DD BC HH24:MI:SS TZH"),
                    ("4714-11-24 BC", "YYYY-MM-DD BC"), ("4714-11-23 BC", "YYYY-MM-DD BC"),
                    ("5874897-12-31", "YYYY-MM-DD"), ("294276-12-31 23:59:59 -15", "YYYY-MM-DD HH24:MI:SS TZH"),
                    ("294276-12-31 23:59:59 +15", "YYYY-MM-DD HH24:MI:SS TZH"),
                    ("12:00:00 +15:59", "HH24:MI:SS TZH:TZM"), ("12:00:00 -15:59", "HH24:MI:SS TZH:TZM")]:
        doc = json.dumps(st)
        path = "$.datetime(" + json.dumps(fmt) + ")"
        for p2 in [path, path + ".string()", path + ".timestamp_tz()", path + ".date()"]:
            g.query("query", doc, p2)
            g.query("query_tz", doc, p2)
    # other functions
    for fn in ["query_array", "query_first", "exists", "match", "exists_op", "match_op"]:
        for doc, path in [('"2017-03-10"', "$.datetime()"), ('"2017-03-10"', "$.datetime() == \"2017-03-10\".date()"),
                          ('"12:34"', "$.datetime(\"HH24:MI\")"), ('"x"', "$.datetime()"),
                          ('["2017-03-10", "12:00:00"]', "$[*].datetime() < \"2017-03-10 12:00\".timestamp()")]:
            g.query(fn, doc, path)
            if fn not in ("exists_op", "match_op"):
                g.query(fn + "_tz", doc, path)
                g.query(fn, doc, path, silent=True)


def main():
    g = Gen()
    if len(sys.argv) > 1:
        extract_regress(g, sys.argv[1])
    parse_cases(g)
    systematic(g)
    targeted(g)
    exec_depth(g)
    datetime_cases(g)
    template_fuzz(g, 2500)
    random_cases(g, 4000, 9000)
    g.r.write()


if __name__ == "__main__":
    main()
