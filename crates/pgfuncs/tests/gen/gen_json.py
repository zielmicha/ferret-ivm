"""Generate golden data for the json/jsonb functions from a real PostgreSQL 17.

Usage: python3 crates/pgfuncs/tests/gen/gen_json.py
Writes crates/pgfuncs/tests/data/json_golden.jsonl
"""

import itertools
import json
import os
import random
import sys

sys.path.insert(0, os.path.dirname(__file__))
from pgeval import Recorder  # noqa: E402

OUT = os.path.join(os.path.dirname(__file__), "..", "data", "json_golden.jsonl")

# ---------------------------------------------------------------- inputs

PARSE_INPUTS = [
    # scalars
    "null", "true", "false", "0", "-0", "1", "-1", "1.0", "1.50", "-1.50", "0.0", "-0.0", "0.000",
    "1e2", "1E2", "1e+2", "1E-2", "1.5e-3", "-1.5e-3", "1.5e3", "1.55e1", "12.34e-10", "0e5", "0e-5",
    "-0e-3", "123456789012345678901234567890", "-123456789012345678901234567890.123456789",
    "0.1e1", "100e-2", "1e1000", "1e-1000", "123.456e-300", "1e131072", "1e-16383",
    "1e-16384", "0e-16383", "0e-16384", "1e2147483647", "1e99999999999", "1.5e1073741823", "1e1073741824",
    "1e-1073741824", "0.00001e-16378",
    '""', '"a"', '"abc"', '" spaced "', '"é"', '"日本語"', '"😀"', '"\\u00e9"', '"\\u00E9"',
    '"\\ud83d\\ude00"', '"\\uD83D\\uDE00"', '"\\n\\t\\r\\b\\f\\"\\\\\\/"', '"a\\u0001b"', '"\\u001f"',
    '"\\u007f"', '"\u007f"', '"tab\\tnl\\n"', '"\\u2028"',
    # containers
    "[]", "{}", "[1,2,3]", "[1, [2, [3, []]], {}]", '{"a":1}', '{"a":1,"b":2}', '{"b":2,"a":1}',
    '{"bb":1,"a":2,"aaa":3,"c":4}', '{"a":1,"a":2}', '{"a":1,"b":2,"a":3}', '{"a":{"b":{"c":null}}}',
    '{"":1}', '{"": {"": []}}', '[null, true, false, "x", 1.10, {"k": [1, {"z": "y", "a": "b"}]}]',
    ' { "a" : [ 1 , 2 ] , "b" : { } } ', "\t[\n1\r\n]\n", '{"key with spaces": "v", "é": 1, "日本": 2}',
    '{"aa": 1, "b": 2, "ab": 3, "ba": 4, "a": 5}', '[[[[[[[[[[1]]]]]]]]]]',
    '{"a": [1, 2, {"b": [3, 4, {"c": 5}]}]}', '[1e2, 1.0e2, 100, 100.00]', '{"x": "\\u0041\\u0042"}',
    '[{"b": 1, "a": 2}, {"a": 2, "b": 1}]', '{"z": 1, "y": {"b": 1, "a": [3, 2, 1]}, "x": []}',
    # errors
    "", " ", "{", "}", "[", "]", "[1,", "[1", "[1 2]", "[1,]", "[,1]", "[,]", '{"a" 1}', '{"a":}',
    '{"a":1,}', '{"a":1 "b":2}', "{1:2}", "{a:1}", '{"a"}', '{"a":1}}', "[1]]", "[1] x", "1 2",
    "tru", "nul", "fals", "truex", "True", "NULL", "01", "00", "-01", "1.", ".1", "-", "+1", "1e",
    "1e+", "1.e5", "1.5x", "0x10", "-.5", "1..2", "Infinity", "NaN", "-Infinity", '"abc', '"',
    '"\\x"', '"\\u12"', '"\\u12G4"', '"\\uZZZZ"', '"\\', '"a\u0001b"', '"a\nb"', '"\\ud800"',
    '"\\udc00"', '"\\ud800\\ud800"', '"\\ud800x"', '"\\ud800\\u0041"', '"\\ud800\\n"', '"\\u0000"',
    '"a\\u0000b"', '{"\\u0000": 1}', '["\\ud800"]', "[1e1000000]", "[1e99999999999, x]",
    "[1e99999999999 x]", "@", "[@]", '{"a":@}', "é", "[é]", '"\\é"', "\f1", "1\f", "[\"\x7f\"]",
    '{"a":1, "b"', '{"a":1, "b":', '{"a":1, "b":2', '{"a":1, "b":[', "[[[]", "[{]", "[{}",
    "abc def", "[truefalse]", "[true false]", "{\"a\":1}x", "  [ 1 , 2 ] garbage",
    # more
    '{"a\\"b": 1, "c\\\\d": 2, "\\n": 3, "\\u00e9": 4, "\\t\\u0001": 5}', '{"a": [], "b": {}}',
    "[[], {}]", "[[[]]]", '{"a": {"b": {}}, "c": [[]]}', "-1.5E+3", "1E-0", "0.0e10", "-0e+0", "-0.0e-2",
    "1234567890123456789012345678901234567890", "0.000000000000000000000000000001", "-a", "-1a", "1e5x",
    "1.5.5", '"\\u"', '"\\u1"', '"\\u123"', '"\\uD800\\uDBFF"', '"\\uDFFF"', '"x\\uD83D"',
    '"\\uD83D\\uDE00\\uD83D"', '[1, {"a": [1, 2,]}]', '[1, ółż]', "truexxxxxxxxxxxxxxxxxxxxxxxx",
    '{"a":1,"b":2,"c":3,"d":4,"e":5,"f":6,"g":7,"h":8,"i":9,"j":10,"k":11}', " 1 ", "\n\n\"x\"\n",
    '{"x":1,"x":[1,2],"x":{"y":3}}', '["\\/", "\\b\\f"]', '"\\u00"', '{"a":"b"', '{"a" : ', "[1,2",
]

NUMERIC_INPUTS = [
    "0", "-0", "+0", "1", "+1.5", "  12  ", "\t3\n", "1_000", "1_000.000_1", "1__0", "_1", "1_", "1._5",
    "1_.5", ".5", "5.", "-.5", "+.5", ".", "-", "", " ", "1e5", "1E-5", "1e+5", "1e", "1e_5", "1e5_0",
    "1.2.3", "0x1F", "0X1f", "-0x1F", "0o17", "0O17", "0b101", "0B101", "0x", "0x_1", "0x1_F", "0x1__F",
    "0xG", "0b102", "0o8", "0x1F ", "0x1F x", "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", "007", "-000.0100",
    "1e-2", "123.456e2", "123.456e-2", "1e1000", "1e131072", "1e-16384", "0e-16384", "1.5e1073741823",
    "1e1073741824", "abc", "12abc", "1 2", "1e2.5", "١٢",
]

FLOAT_INPUTS = [
    "0", "-0", "1", "-1", "0.1", "0.2", "0.30000000000000004", "1e20", "1e21", "1e-7", "1.5e-7",
    "123.456", "1e15", "1e16", "1e300", "1.7976931348623157e308", "5e-324", "2.2250738585072014e-308",
    "3.141592653589793", "-2.5", "100", "1234567.125", "0.000001", "1e-5", "9007199254740993",
    "4.35", "0.1e-310", "123456789012345680000",
]

DOCS = [
    "null", "true", "false", "1", "-2.50", '"a"', '"b"', '""', '"foo"', "[]", "{}", "[1]", '["a"]',
    '["a", "b", "a"]', "[1, 2, 3]", '[1, "1", true, null, [1], {"a": 1}]',
    '{"a": 1}', '{"a": 1, "b": 2}', '{"a": null, "b": [1, null, {"c": null, "d": 1}]}',
    '{"a": {"b": {"c": 1}}, "d": [1, 2]}', '{"a": [1, 2, 3], "b": "x"}',
    '[{"a": 1, "b": 2}, {"c": 3}, [1, [2, 3]], "s", 4.0]', '{"aa": 1, "b": 2, "": 3}',
    '{"x": {"y": [10, 20, {"z": "deep"}]}}', '[[1, 2], [3, [4, 5]]]', '{"a": [{"b": 1}, {"b": 2}]}',
    '{"é": "日本", "k": "😀"}',
]

KEYS = ["a", "b", "c", "x", "", "aa", "1", "0", "foo", "é", "z"]
INDEXES = [0, 1, 2, -1, -2, -3, 3, 5, -6, 2147483647, -2147483648]
PATHS = [
    [], ["a"], ["b"], ["a", "b"], ["a", "b", "c"], ["0"], ["1"], ["-1"], ["5"], ["-5"], ["a", "0"],
    ["a", "1"], ["a", "-1"], ["x", "y", "2", "z"], ["x", "y", "-1"], ["d", "1"], ["0", "a"],
    ["2", "1", "0"], [" 1"], ["1 "], ["+1"], ["1a"], [""], ["a", ""], ["99999999999"], ["-0"],
    ["2147483648"], ["-2147483648"], ["a", "b", "c", "d"], ["é"], ["k"],
]
VALUES = ['"new"', "42", "null", '{"n": 1}', "[1, 2]"]


def main():
    r = Recorder(OUT)

    for s in PARSE_INPUTS:
        r.case("jsonb_in", "SELECT %s::jsonb::text", [s])
        r.case("json_in", "SELECT %s::json::text", [s])
    for s in PARSE_INPUTS:
        r.case("jsonb_pretty", "SELECT jsonb_pretty(%s::jsonb)", [s])
    # deeper / bigger documents
    for n in [1, 2, 50, 200]:
        deep = "[" * n + "]" * n
        r.case("jsonb_in", "SELECT %s::jsonb::text", [deep])
        deepo = '{"a":' * n + "1" + "}" * n
        r.case("jsonb_in", "SELECT %s::jsonb::text", [deepo])
        if n <= 50:
            r.case("jsonb_pretty", "SELECT jsonb_pretty(%s::jsonb)", [deepo])
    # nesting limits (PostgreSQL: check_stack_depth with max_stack_depth = 2MB)
    shapes = {
        "arr": ("repeat('[', %s) || repeat(']', %s)", "[", "]", ""),
        "obj": ("repeat('{\"a\":', %s) || '1' || repeat('}', %s)", '{"a":', "}", "1"),
        "mixed": ("repeat('[{\"a\":', %s) || '1' || repeat('}]', %s)", '[{"a":', "}]", "1"),
    }
    for typ, shape, n in [
        # the exact cut-off depends on the caller's stack usage, so only test
        # depths clearly below / above PostgreSQL's limit
        ("jsonb", "arr", 16000), ("jsonb", "arr", 16700), ("jsonb", "obj", 14200), ("jsonb", "obj", 14900),
        ("jsonb", "mixed", 7500), ("jsonb", "mixed", 7900), ("json", "arr", 16000), ("json", "arr", 16700),
        ("json", "obj", 14200), ("json", "obj", 14900), ("json", "mixed", 7500), ("json", "mixed", 7900),
    ]:
        expr = shapes[shape][0]
        kind, v = r.eval("SELECT length((" + expr + ")::" + typ + "::text)::text", (n, n))
        rec = {"fn": "depth", "args": [typ, shape, n]}
        rec["result" if kind == "ok" else "error"] = v
        r.records.append(rec)

    # recursion limits of jsonb_set (setPath) and @> (JsonbDeepContains)
    for shape, n in [("obj", 6500), ("obj", 7300), ("arr", 6500), ("arr", 7300)]:
        doc = ("repeat('{\"a\":', %s) || '1' || repeat('}', %s)" if shape == "obj"
               else "repeat('[', %s) || repeat(']', %s)")
        elem = "a" if shape == "obj" else "0"
        kind, v = r.eval("SELECT length(jsonb_set((" + doc + ")::jsonb, array_fill(%s::text, array[%s]), "
                         "'2')::text)::text", (n, n, elem, n))
        rec = {"fn": "depth_set", "args": [shape, n]}
        rec["result" if kind == "ok" else "error"] = v
        r.records.append(rec)
    for shape, n in [("obj", 7800), ("obj", 8600), ("arr", 7800), ("arr", 8600)]:
        doc = ("repeat('{\"a\":', %s) || '1' || repeat('}', %s)" if shape == "obj"
               else "repeat('[', %s) || repeat(']', %s)")
        kind, v = r.eval("SELECT ((" + doc + ")::jsonb @> (" + doc + ")::jsonb)::text", (n, n, n, n))
        rec = {"fn": "depth_contains", "args": [shape, n]}
        rec["result" if kind == "ok" else "error"] = v
        r.records.append(rec)

    for s in NUMERIC_INPUTS:
        r.case("numeric_in", "SELECT %s::numeric::text", [s])
    for s in FLOAT_INPUTS:
        r.case("float8_to_jsonb", "SELECT to_jsonb(%s::float8)::text", [s])

    for d in DOCS:
        r.case("typeof", "SELECT jsonb_typeof(%s::jsonb)", [d])
        r.case("array_length", "SELECT jsonb_array_length(%s::jsonb)::text", [d])
        r.case("object_keys",
               "SELECT coalesce(jsonb_agg(k ORDER BY o), '[]')::text "
               "FROM jsonb_object_keys(%s::jsonb) WITH ORDINALITY t(k, o)", [d])
        r.case("array_elements",
               "SELECT coalesce(jsonb_agg(e ORDER BY o), '[]')::text "
               "FROM jsonb_array_elements(%s::jsonb) WITH ORDINALITY t(e, o)", [d])
        r.case("array_elements_text",
               "SELECT coalesce(jsonb_agg(e ORDER BY o), '[]')::text "
               "FROM jsonb_array_elements_text(%s::jsonb) WITH ORDINALITY t(e, o)", [d])
        r.case("object_entries",
               "SELECT coalesce(jsonb_agg(jsonb_build_array(k, v) ORDER BY o), '[]')::text "
               "FROM jsonb_each(%s::jsonb) WITH ORDINALITY t(k, v, o)", [d])
        r.case("object_entries_text",
               "SELECT coalesce(jsonb_agg(jsonb_build_array(k, v) ORDER BY o), '[]')::text "
               "FROM jsonb_each_text(%s::jsonb) WITH ORDINALITY t(k, v, o)", [d])
        r.case("strip_nulls", "SELECT jsonb_strip_nulls(%s::jsonb)::text", [d])
        r.case("pretty", "SELECT jsonb_pretty(%s::jsonb)", [d])
        r.case("as_text_root", "SELECT %s::jsonb #>> '{}'", [d])
        for k in KEYS:
            r.case("get_key", "SELECT (%s::jsonb -> %s::text)::text", [d, k])
            r.case("get_key_text", "SELECT %s::jsonb ->> %s::text", [d, k])
            r.case("exists", "SELECT (%s::jsonb ? %s::text)::text", [d, k])
            r.case("delete_key", "SELECT (%s::jsonb - %s::text)::text", [d, k])
        for i in INDEXES:
            r.case("get_index", "SELECT (%s::jsonb -> %s::int)::text", [d, i])
            r.case("get_index_text", "SELECT %s::jsonb ->> %s::int", [d, i])
            r.case("delete_index", "SELECT (%s::jsonb - %s::int)::text", [d, i])
        for p in PATHS:
            r.case("get_path", "SELECT (%s::jsonb #> %s::text[])::text", [d, p])
            r.case("get_path_text", "SELECT %s::jsonb #>> %s::text[]", [d, p])
            r.case("delete_path", "SELECT (%s::jsonb #- %s::text[])::text", [d, p])
        for p, v in itertools.product(PATHS, VALUES[:1]):
            for create in (True, False):
                r.case("set", "SELECT jsonb_set(%s::jsonb, %s::text[], %s::jsonb, %s)::text",
                       [d, p, v, create])
            for after in (True, False):
                r.case("insert", "SELECT jsonb_insert(%s::jsonb, %s::text[], %s::jsonb, %s)::text",
                       [d, p, v, after])
        for keys in [[], ["a"], ["a", "b"], ["x", "y"], ["a", "zz"], ["", "1"], ["foo", "a"]]:
            r.case("exists_any", "SELECT (%s::jsonb ?| %s::text[])::text", [d, keys])
            r.case("exists_all", "SELECT (%s::jsonb ?& %s::text[])::text", [d, keys])
        for v in VALUES + ["[]", "{}", '{"a": 2}', '{"c": 9}']:
            r.case("set", "SELECT jsonb_set(%s::jsonb, %s::text[], %s::jsonb, %s)::text",
                   [d, ["a"], v, True])

    # set/insert/delete with null path elements (lazy null checks)
    for d in DOCS:
        for p in [[None], ["a", None], [None, "a"], ["x", None, "z"], ["0", None], ["a", "b", None]]:
            r.case("set_opt", "SELECT jsonb_set(%s::jsonb, %s::text[], '1'::jsonb, true)::text", [d, p])
            r.case("insert_opt", "SELECT jsonb_insert(%s::jsonb, %s::text[], '1'::jsonb, false)::text", [d, p])
            r.case("delete_path_opt", "SELECT (%s::jsonb #- %s::text[])::text", [d, p])

    contain_docs = DOCS + [
        '[1, [1, 2]]', '[[1, 2]]', '[[1], [2]]', '{"a": [1, 2, 3]}', '{"a": [1]}', '{"a": []}',
        '{"a": {}}', '{"a": {"b": 1}}', '{"a": 1, "c": 2}', '[{"a": 1}]', '[{"a": 1, "b": 2}, 3]',
        '["foo", "bar"]', '[1.0]', '[1.00, 2]', '{"a": 1.0}', '[[]]', '[{}]', '[null]',
    ]
    for a in contain_docs:
        for b in contain_docs:
            r.case("contains", "SELECT (%s::jsonb @> %s::jsonb)::text", [a, b])
    for a in DOCS:
        for b in DOCS:
            r.case("concat", "SELECT (%s::jsonb || %s::jsonb)::text", [a, b])

    # comparisons
    cmp_values = DOCS + [
        "[[]]", "[null]", '[""]', "[0]", "[[[]]]", '{"a": []}', '{"b": 1}', '{"a": 2}', '{"aa": 1}',
        '{"a": 1, "c": 1}', '{"b": 1, "c": 1}', '{"aa": 1, "b": 1}', '{"a": null}', '{"a": [1]}',
        '{"a": {"b": 1}}', "0", "0.0", "-0", "1.0", "1.00", "10", "9.99", "-10", "1e-5", '"A"', '"Z"',
        '"ab"', '"abc"', '"é"', '"z"', '"\\u0001"', "[true]", "[false]", "[1, 2]", "[1, 3]", "[2, 1]",
        '[1, "1"]', '["1", 1]', "[{}]", "[[1]]", "[1, [2]]", "[[2], 1]", '[{"a": 1}]',
    ]
    for a in cmp_values:
        for b in cmp_values:
            kind, v = r.eval(
                "SELECT (CASE WHEN a < b THEN -1 WHEN a = b THEN 0 ELSE 1 END)::text "
                "FROM (SELECT %s::jsonb a, %s::jsonb b) t", (a, b))
            rec = {"fn": "cmp", "args": [a, b]}
            rec["result" if kind == "ok" else "error"] = v
            r.records.append(rec)
    # one sort-order record over all comparison values
    kind, v = r.eval(
        "SELECT json_agg(i ORDER BY v, i)::text FROM "
        "(SELECT (ordinality - 1)::int i, x::jsonb v FROM unnest(%s::text[]) WITH ORDINALITY x) t",
        (cmp_values,))
    r.records.append({"fn": "sort", "args": [cmp_values], "result": v})

    # random documents: ordering and containment
    rnd = random.Random(20241025)

    def rnd_num():
        s = rnd.choice(["", "-"]) + rnd.choice(["0", "1", "2", "10", "123"])
        if rnd.random() < 0.4:
            s += "." + rnd.choice(["0", "5", "50", "25", "001"])
        if rnd.random() < 0.2:
            s += "e" + str(rnd.randint(-3, 3))
        return s

    def rnd_doc(depth=0):
        x = rnd.random()
        if depth > 2 or x < 0.45:
            return rnd.choice([
                "null", "true", "false", rnd_num(), rnd_num(),
                json.dumps(rnd.choice(["", "a", "b", "ab", "ba", "é", "a b", "A"])),
            ])
        if x < 0.72:
            return "[" + ", ".join(rnd_doc(depth + 1) for _ in range(rnd.randint(0, 3))) + "]"
        keys = [rnd.choice(["a", "b", "c", "aa", "ab", "", "é"]) for _ in range(rnd.randint(0, 3))]
        return "{" + ", ".join(json.dumps(k) + ": " + rnd_doc(depth + 1) for k in keys) + "}"

    rdocs = [rnd_doc() for _ in range(400)]
    kind, v = r.eval(
        "SELECT json_agg(i ORDER BY v, i)::text FROM "
        "(SELECT (ordinality - 1)::int i, x::jsonb v FROM unnest(%s::text[]) WITH ORDINALITY x) t",
        (rdocs,))
    r.records.append({"fn": "sort", "args": [rdocs], "result": v})
    # equality classes: for each doc, index of the first equal doc
    kind, v = r.eval(
        "SELECT json_agg((SELECT min(j) FROM (SELECT (ordinality - 1)::int j, y::jsonb w "
        "FROM unnest(%s::text[]) WITH ORDINALITY y) u WHERE u.w = t.v) ORDER BY i)::text FROM "
        "(SELECT (ordinality - 1)::int i, x::jsonb v FROM unnest(%s::text[]) WITH ORDINALITY x) t",
        (rdocs, rdocs))
    r.records.append({"fn": "eq_classes", "args": [rdocs], "result": v})
    for _ in range(3000):
        a, b = rnd_doc(), rnd_doc()
        if rnd.random() < 0.5:
            b = rnd.choice([b, a])
        r.case("contains", "SELECT (%s::jsonb @> %s::jsonb)::text", [a, b])

    # jsonb_build_object
    build_cases = [
        [], [("a", "1")], [("b", "1"), ("a", "2")], [("a", "1"), ("a", "2")], [("a", "1"), ("b", "2"), ("a", "3")],
        [("bb", "null"), ("a", '"x"'), ("ccc", "[1]"), ("", "{}")], [("é", "1"), ("e", "2")],
    ]
    for pairs in build_cases:
        args = []
        sql_args = []
        for k, v in pairs:
            args += [k, v]
            sql_args.append("%s::text, %s::jsonb")
        sql = "SELECT jsonb_build_object(" + ", ".join(sql_args) + ")::text"
        r.case("build_object", sql, args)

    # json (text) functions
    json_docs = [
        '{"a": 1, "b": [1, 2], "c": {"d": "x"}}', '{"a":1,"a":2}', '{"a" : "\\u00e9\\n" , "b":null}',
        '[1, "two", {"three": 3}, [4], null, true]', '[]', '{}', '"str"', '42', 'null', ' [ 1 , 2 ] ',
        '{"a": [ 1,\n 2 ] }', '{"a":"\\u0000"}', '["\\ud800"]', '{"x": {"a": 1}, "a": 2}',
        '[[1,2],[3]]', '{"k": "v\\"q"}', '{"a": "\\ud83d\\ude00"}',
        '{"a": {"a": {"a": 1}}, "b": [ {"a": 2} ]}', '{ "c" : "x" , "a" : [ ] , "b" : { } }',
        '{"a": "first", "b": 1, "a": "second"}', '[ "a" , [ "b" ] , { "c" : null } ]', '{"a":1}  ',
        '  {"a":1}', '{"a":1e5,"b":-0,"c":1.50}', '{"\\u0061": 1}', '{"a": "\\u0000"}',
    ]
    for d in json_docs:
        for k in ["a", "b", "c", "x", "k", "zz", ""]:
            r.case("json_get_key", "SELECT (%s::json -> %s::text)::text", [d, k])
            r.case("json_get_key_text", "SELECT %s::json ->> %s::text", [d, k])
        for i in [0, 1, 2, 5, -1, -2, -6, -7, -2147483648]:
            r.case("json_get_index", "SELECT (%s::json -> %s::int)::text", [d, i])
            r.case("json_get_index_text", "SELECT %s::json ->> %s::int", [d, i])
        r.case("json_as_text_root", "SELECT %s::json #>> '{}'", [d])
    for s in ["", "abc", 'q"uote', "back\\slash", "\n\r\t\b\f", "\u0001\u001f\u007f", "é日本😀", "/",
              " "]:
        r.case("escape", "SELECT to_json(%s::text)::text", [s])

    r.write()


if __name__ == "__main__":
    main()
