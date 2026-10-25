"""Generate tests/data/pattern_cases.jsonl (LIKE, SIMILAR TO, regex) from PostgreSQL 17."""
import os
import random
import sys

sys.path.insert(0, os.path.dirname(__file__))
from pgeval import Recorder  # noqa: E402

OUT = os.path.join(os.path.dirname(__file__), "..", "data", "pattern_cases.jsonl")
r = Recorder(OUT)

# ---------------------------------------------------------------- LIKE
LIKE_SUBJ = ["", "a", "abc", "ABC", "aBc", "a%c", "a_c", "a\\c", "abcabc", "héllo", "HÉLLO", "日本語", "x", "xx",
             "ǅ", "ǆ", "Ǆ", "İ", "i", "ß", "ẞ", "%", "_", "\\", "ab\\", "a\\b"]
LIKE_PAT = ["", "%", "_", "a%", "%c", "%b%", "a_c", "a\\%c", "a\\_c", "a\\\\c", "abc", "ABC", "%%", "_%", "%_",
            "__", "___", "a%c%", "%a%b%c%", "h_llo", "h%o", "日%", "%語", "_本_", "\\", "a\\", "%\\", "x\\", "\\a",
            "ǅ", "ǆ", "%ß", "%ẞ", "İ", "i", "a!%c", "a!!c", "a!", "!%", "a#%c", "%#", "#", "ab\\\\"]
for pat in LIKE_PAT:
    for s in LIKE_SUBJ:
        r.case("like", "SELECT (%s::text LIKE %s::text)::text", [s, pat])
        r.case("ilike", "SELECT (%s::text ILIKE %s::text)::text", [s, pat])
for pat in ["a!%c", "a!!c", "a!", "!%", "a!_c", "!!", "%!%%", "a!b"]:
    for s in ["a%c", "abc", "a!c", "a!", "%", "a_c", "!", "x%y", "ab", "a"]:
        r.case("like_esc", "SELECT (%s::text LIKE %s::text ESCAPE %s::text)::text", [s, pat, "!"])
        r.case("ilike_esc", "SELECT (%s::text ILIKE %s::text ESCAPE %s::text)::text", [s, pat, "!"])
for pat in ["a\\c", "a\\%", "\\\\", "%\\%", "a%", "\\"]:
    for s in ["a\\c", "a\\x", "\\\\", "a\\", "\\", "abc"]:
        r.case("like_esc", "SELECT (%s::text LIKE %s::text ESCAPE %s::text)::text", [s, pat, ""])
for pat in ["aéc", "aé%", "éé", "%é", "é"]:
    for s in ["a%c", "aéc", "é", "a%", "%"]:
        r.case("like_esc", "SELECT (%s::text LIKE %s::text ESCAPE %s::text)::text", [s, pat, "é"])
for esc in ["ab", "éé"]:
    r.case("like_esc", "SELECT (%s::text LIKE %s::text ESCAPE %s::text)::text", ["a", "a", esc])

# bytea LIKE
for pat in ["", "%", "_", "a%", "\\x00", "a_c", "%\\x00%"]:
    for s in ["", "a", "abc", "a\\x00c"]:
        r.case("like_bytea", "SELECT (convert_to(%s, 'UTF8') LIKE convert_to(%s, 'UTF8'))::text", [s, pat])
for s, pat in [("\\xff00", "\\377%"), ("\\x0102", "\\001_"), ("\\xc3a9", "_"), ("\\xc3a9", "__"),
               ("\\x5c", "\\\\"), ("\\x5c41", "\\\\A"), ("\\x00", "%"), ("ab", "a\\\\")]:
    r.case("like_bytea_lit", "SELECT (%s::bytea LIKE %s::bytea)::text", [s, pat])

# ---------------------------------------------------------------- SIMILAR TO
SIM_PAT = ["abc", "a%", "%b%", "a_c", "(a|b)%", "[ab]+c", "a*", "a+", "a?b", "a{2}", "%(b|c)", "\\%", "a.c",
           "^a", "a$", "[^a]%", "(abc|def)", "%[[:digit:]]%", "x\\_y", "a\"b\"c", "%\"b%\"", "[]a]",
           "[^]a]", "[a]b]", "\\d+", "a||b", "(a)", "[\\d]", ""]
SIM_SUBJ = ["abc", "a", "", "abcabc", "aab", "b", "a.c", "abd", "x_y", "xay", "a%", "123", "zz9", "]", "def"]
for pat in SIM_PAT:
    r.case("similar_escape", "SELECT similar_to_escape(%s::text)::text", [pat])
    for s in SIM_SUBJ:
        r.case("similar", "SELECT (%s::text SIMILAR TO %s::text)::text", [s, pat])
for pat, esc in [("a#%b", "#"), ("#\"x#\"", "#"), ("a\\b", ""), ("aé%", "é"), ("a#\"b#\"c#\"d#\"", "#"),
                 ("[#]]", "#"), ("[é]]", "é"), ("[ü]]", "é"), ("x\\_", "\\")]:
    r.case("similar_escape2", "SELECT similar_to_escape(%s::text, %s::text)::text", [pat, esc])
r.case("similar_escape2", "SELECT similar_to_escape(%s::text, %s::text)::text", ["a", "ab"])
for s, pat, esc in [("abcdef", "a#\"bc#\"%", "#"), ("abcdef", "%#\"c%e#\"%", "#"), ("xyz", "x#\"%#\"", "#"),
                    ("aaa", "a#\"%#\"a", "#"), ("abc", "#\"%#\"", "#"), ("abc", "q#\"%#\"", "#")]:
    r.case("similar_substring", "SELECT substring(%s::text similar %s::text escape %s::text)::text", [s, pat, esc])

# ---------------------------------------------------------------- regex
PATTERNS = [
    "a", "abc", "^abc", "abc$", "^$", "", ".", "a.c", "a*", "a+", "a?", "a{2}", "a{2,}", "a{2,3}", "a{,2}",
    "a{0}", "a{0,0}b", "(a){0}b", "(a)", "(a)(b)", "(a|b)", "a|ab", "ab|a", "(a|ab)(c|bcd)(d*)", "(a*)(a*)",
    "(a*?)(a*)", "(a*)(a*?)", "a*?", "a+?", "a??", "a{2,3}?", "a{2}?", "(a+|b+)*", "(a|b)*c", "(a|b)*?c",
    "[abc]", "[^abc]", "[a-z]+", "[[:alpha:]]+", "[[:digit:]]+", "[[:alnum:]_]+", "[[:upper:]]", "[[:lower:]]",
    "[[:space:]]", "[[:punct:]]+", "[[:xdigit:]]+", "[[:word:]]+", "[[:print:]]+", "[[:graph:]]+",
    "[[:cntrl:]]", "[[:blank:]]", "[[:ascii:]]+", "\\d+", "\\w+", "\\s", "\\D+", "\\W+", "\\S+", "[\\d]+",
    "[^\\d]+", "[\\w-]+", "[\\s\\S]", "[^\\W]+", "x*y*", "(x)?y", "(x)|y", "\\bx", "\\mfoo\\M", "\\yfoo\\y",
    "o\\Y", "\\Yo", "(?i)abc", "(?x) a b c # comment", "***=a.b", "***:a.b", "***?", "***x",
    "(?=a)a", "(?!a).", "(?<=a)b", "(?<!a)b", "a(?=b)", "a(?!b)", "(?<=^)a", "(?<=a|bc)d", "(?=(a))", "(a)\\1",
    "(a*)\\1", "(\\w+) \\1", "([a-z])\\1+", "(a)(b)\\2\\1", "(a|b)\\1", "((a)|b)\\2", "(a*)b\\1", "(a)\\1{2}",
    "(a)\\1*", "(a)\\1?", "\\1", "(a)\\2", "(", ")", "a)", "[", "a{3,2}", "*a", "a**", "a+*", "\\x41",
    "\\u0041", "\\U00000041", "\\101", "\\0", "\\cA", "\\e", "\\a", "\\Bx", "\\t", "\\v", "\\f", "\\r", "\\n",
    "[[.hyphen.]]", "[[.a.]]", "[[.space.]]", "[[.foo.]]", "[[=a=]]", "[[:foo:]]", "[a-]", "[]a]", "[^]a]",
    "[z-a]", "[a-a]", "\\", "a\\", "x{256}", "x{255}", "(?b)a\\{2\\}", "(?b)\\(a\\)\\1", "(?b)a*", "(?b)*a",
    "(?b)^*", "(?e)a+", "(?e)a\\d", "(?q).*", "(?m)^a$", "(?n)^b", "(?s)a.b", "(?p)a.b", "(?w)^b",
    "(?p)a[^x]b", "(?n)a[^x]b", "Y*([0-9]{1,3})", "Y*?([0-9]{1,3})", "(.*?)(\\d+)(.*)", "(\\w+)@(\\w+)\\.com",
    "^(\\d{3})-(\\d{4})$", "((a)|b)+", "(a)|b", "(?:(a)|b)*", "(?:a|(b))*", "é+", "[à-ÿ]+", "\\w+$",
    "^\\s*$", "(foo|foobar)", "(foobar|foo)", "(a(b)?)+", "(a|b|c|d)+", "(.)(.)(.)", "(..)*", "(.)*",
    "((.)(.))*", "a{1,2}?b", "(a{1,2}?)(a*)", "(ab|a)(bc|c)", "x(?:a|ab)(c|bcd)", "(?:a|b)*?b", "^(a+)+$",
    "(?#comment)a", "a(?#x)b", "(?z)a", "(?i", "a{1", "a{1,", "a{x}", "a{1}{2}", "a\\{", "[a-\\d]", "[\\d-a]",
    "[[:alpha:]-z]", "a|", "|a", "()", "(|a)", "(a|)", "a||b", "\\A", "\\Z", "\\Aa", "a\\Z", "(?<=(a))b",
    "(?<!\\1)b", "[\\b]", "[\\]]", "[\\a]", "\\q", "\\H", "\\%", "\\.", "\\*", "[.]", "[*]", "[[]", "[[a]",
    "\\d{2,}?\\d", "(?i)[a-c]+", "(?i)é", "(?i)[é]", "(?i)ǅ", "(?i)[ǅ]", "(?i)[ǅ-ǅ]", "(?i)\\w", "(?i)[[:upper:]]+",
    "(?i)[[:lower:]]+", "(?i)k", "(?i)[^a]", "(?i)a\\1", "(?i)(a)\\1",
]
SUBJECTS = ["", "a", "abc", "aaa", "abcd", "xyz", "XY1234Z", "foo bar foo", "abab", "aab", "a\nb", "Hello World",
            "héllo wörld", "test@example.com", "555-1234", "aabb", "ABC", "éÉ", "ǅǆǄ", "KkK", "a.b*c",
            "foobar", "b", "ba", "  ", "x\ty", "a-b_c", "\u0001", "aaaa", "abcabc"]

for pat in PATTERNS:
    for s in SUBJECTS:
        r.case("match", "SELECT to_json(regexp_match(%s::text, %s::text))::text", [s, pat])
for pat in PATTERNS[:120]:
    for s in SUBJECTS[:12]:
        r.case("match_i", "SELECT to_json(regexp_match(%s::text, %s::text, 'i'))::text", [s, pat])
        r.case("is_match", "SELECT (%s::text ~ %s::text)::text", [s, pat])
        r.case("is_match_i", "SELECT (%s::text ~* %s::text)::text", [s, pat])

for pat in PATTERNS[:200]:
    for s in ["abcabc", "aaa", "foo bar foo", "a\nb\nc", "héllo wörld", "", "xaxbx"]:
        r.case("matches_g", "SELECT coalesce(json_agg(m), '[]')::text FROM regexp_matches(%s::text, %s::text, 'g') m",
               [s, pat])
        r.case("split", "SELECT to_json(regexp_split_to_array(%s::text, %s::text))::text", [s, pat])
        r.case("count", "SELECT regexp_count(%s::text, %s::text)::text", [s, pat])
        r.case("replace", "SELECT regexp_replace(%s::text, %s::text, %s::text)::text", [s, pat, "<\\&>"])
        r.case("replace_g", "SELECT regexp_replace(%s::text, %s::text, %s::text, 'g')::text", [s, pat, "[\\1|\\2]"])
        r.case("substring", "SELECT substring(%s::text from %s::text)::text", [s, pat])

# flags
for flags in ["", "i", "c", "n", "m", "s", "p", "w", "x", "q", "b", "e", "t", "ix", "g", "z", "é", "in", "ci", "ic",
              "1", "nw", "pw", "wp"]:
    for pat, s in [("^b$", "a\nb\nc"), ("a.c", "a\nc"), ("A B", "AB"), ("a.c", "a.c"), ("[^x]", "\n"), ("^a", "Abc"),
                   ("a\\d", "a1"), ("a+", "aaa"), ("(a)\\1", "aa"), ("a{2}", "aa"), ("\\(a\\)", "(a)")]:
        r.case("match_flags", "SELECT to_json(regexp_match(%s::text, %s::text, %s::text))::text", [s, pat, flags])

# replace variants
REPL = ["X", "\\&", "\\1", "\\\\", "\\", "a\\", "\\0", "\\9", "<\\1-\\2>", "\\x", "é\\&é"]
for pat, s in [("(b)(c)?", "abcabcb"), ("", "abc"), ("x*", "abc"), ("é", "héllo"), ("(a|b)", "abba"),
               ("(?=b)", "abab"), ("$", "abc"), ("^", "abc"), ("b+", "abbbcbb")]:
    for rep in REPL:
        r.case("replace", "SELECT regexp_replace(%s::text, %s::text, %s::text)::text", [s, pat, rep])
        r.case("replace_g", "SELECT regexp_replace(%s::text, %s::text, %s::text, 'g')::text", [s, pat, rep])
    for start in [-1, 0, 1, 2, 3, 5, 8, 100]:
        for n in [-1, 0, 1, 2, 3]:
            r.case("replace_nth", "SELECT regexp_replace(%s::text, %s::text, %s::text, %s::int4, %s::int4)::text",
                   [s, pat, "X", start, n])

# count / instr / substr
for pat, s in [("a", "banana"), ("an", "banana"), ("(a)(n)?", "banana"), ("", "abc"), ("x", "abc"), ("é", "éaé"),
               ("[[:alpha:]]+", "hello big world"), ("(b)|(c)", "abcb")]:
    for start in [-1, 0, 1, 2, 3, 4, 7, 100]:
        r.case("count_start", "SELECT regexp_count(%s::text, %s::text, %s::int4)::text", [s, pat, start])
        for n in [0, 1, 2, 3]:
            for endopt in [0, 1, 2]:
                for sub in [-1, 0, 1, 2, 3]:
                    r.case("instr",
                           "SELECT regexp_instr(%s::text, %s::text, %s::int4, %s::int4, %s::int4, '', %s::int4)::text",
                           [s, pat, start, n, endopt, sub])
            for sub in [-1, 0, 1, 2, 3]:
                r.case("rsubstr", "SELECT regexp_substr(%s::text, %s::text, %s::int4, %s::int4, '', %s::int4)::text",
                       [s, pat, start, n, sub])

# ---------------------------------------------------------------- random differential cases
rng = random.Random(12345)


def rand_atom(depth, ngroups):
    k = rng.random()
    if depth > 3 or k < 0.45:
        return rng.choice(["a", "b", "c", ".", "[ab]", "[^a]", "a", "b"])
    if k < 0.65:
        ngroups[0] += 1
        return "(" + rand_re(depth + 1, ngroups) + ")"
    if k < 0.75:
        return "(?:" + rand_re(depth + 1, ngroups) + ")"
    if k < 0.80 and ngroups[0] > 0:
        return "\\" + str(rng.randint(1, ngroups[0]))
    if k < 0.84:
        return rng.choice(["^", "$", "\\m", "\\M", "\\y", "\\Y"])
    if k < 0.88:
        return rng.choice(["(?=", "(?!", "(?<=", "(?<!"]) + rng.choice(["a", "b", "ab", "[bc]", "a|b"]) + ")"
    return rng.choice(["a", "b", "c"])


def rand_quant():
    k = rng.random()
    if k < 0.55:
        return ""
    return rng.choice(["*", "+", "?", "*?", "+?", "??", "{2}", "{1,2}", "{0,1}", "{1,}", "{1,2}?", "{0,}?", "{2,3}"])


def rand_re(depth, ngroups):
    nbranch = 1 if rng.random() < 0.7 else rng.randint(2, 3)
    branches = []
    for _ in range(nbranch):
        n = rng.randint(1, 4)
        parts = []
        for _ in range(n):
            a = rand_atom(depth, ngroups)
            if a in ("^", "$", "\\m", "\\M", "\\y", "\\Y") or a.startswith("(?=") or a.startswith("(?!") \
                    or a.startswith("(?<"):
                parts.append(a)
            else:
                parts.append(a + rand_quant())
        branches.append("".join(parts))
    return "|".join(branches)


def rand_subject():
    n = rng.randint(0, 7)
    return "".join(rng.choice("abcab") for _ in range(n))


for i in range(2500):
    pat = rand_re(0, [0])
    subs = [rand_subject() for _ in range(3)]
    for s in subs:
        r.case("rmatch", "SELECT to_json(regexp_match(%s::text, %s::text))::text", [s, pat])
    s = rand_subject() + rand_subject()
    r.case("rmatches", "SELECT coalesce(json_agg(m), '[]')::text FROM regexp_matches(%s::text, %s::text, 'g') m",
           [s, pat])
    r.case("rreplace", "SELECT regexp_replace(%s::text, %s::text, %s::text, 'g')::text", [s, pat, "<\\&:\\1>"])

# ---------------------------------------------------------------- supplementary-plane characters
SUPP_SUBJ = ["😀", "a😀b", "😀😁😂", "𐐀𐐨", "x😀y😁", "日本😀語", ""]
SUPP_PAT = [".", "..", "^.$", "^..$", "[😀-😂]+", "\\U0001F600", "\\U0001F601+", "(.)(.)", "a.b", "[^a]+",
            "\\w+", "\\W+", "[[:alpha:]]+", "\U00010400", "(?i)\U00010428", "(?i)[\U00010400]", "😀|😁",
            "(😀)\\1", "(?<=😀)b", "😀(?=b)", "\\y", "[\\U0001F600-\\U0001F602]"]
for pat in SUPP_PAT:
    for s in SUPP_SUBJ:
        r.case("match", "SELECT to_json(regexp_match(%s::text, %s::text))::text", [s, pat])
        r.case("replace_g", "SELECT regexp_replace(%s::text, %s::text, %s::text, 'g')::text", [s, pat, "<\\&>"])
        r.case("is_match_i", "SELECT (%s::text ~* %s::text)::text", [s, pat])
for pat in ["_", "__", "%😀%", "a_b", "😀_", "%\U00010428", "_\U00010400"]:
    for s in SUPP_SUBJ:
        r.case("like", "SELECT (%s::text LIKE %s::text)::text", [s, pat])
        r.case("ilike", "SELECT (%s::text ILIKE %s::text)::text", [s, pat])

r.write()
