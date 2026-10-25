"""Generate tests/data/text_cases.jsonl from a real PostgreSQL 17 (builtin C.UTF-8)."""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from pgeval import Recorder  # noqa: E402

OUT = os.path.join(os.path.dirname(__file__), "..", "data", "text_cases.jsonl")
r = Recorder(OUT)

STRS = [
    "", "a", "abc", "Hello World", "hello world", "HELLO WORLD", "  padded  ", "x y\tz\n",
    "héllo wörld", "ÉCOLE élève", "straße", "STRASSE", "İstanbul", "ıi", "ǆemal ǅ Ǆ", "ǳ ǲ Ǳ",
    "ΣΑΣ σας ς", "日本語テキスト", "👍🏽 emoji 🎉", "é combining", "ﬃ ﬁne ﬂ", "Ꭰ ꭰ Cherokee",
    "𐐀𐐨 deseret", "ა ბ Ა Ბ", "o'neil", "123abc", "abc123def", "foo-bar_baz", "a.b,c;d", "٣abc ٣٤",
    "x²y", "Ⅻ ⅻ roman", "ⓐⓑ Ⓐ", "µ ÿ Ÿ", "ǅungla", "MacDonald", "the QUICK brown fOX", "ŉ", "ΐ",
    "ᾀᾈ", "ꞵ Ʇ", "ǰ", "ﬀ", "Ꞔ", "ᲀ", "ẞ", "Å Å", "K k", "ͅ", "_x_", "a-b c'd",
]

for s in STRS:
    for fn in ["char_length", "octet_length", "bit_length", "lower", "upper", "initcap", "reverse", "ascii",
               "md5", "quote_ident", "quote_literal"]:
        r.case(fn, f"SELECT {fn}(%s::text)::text", [s])

# quote_ident specifics
for s in ["select", "Select", "user", "abc", "_abc", "1abc", "a b", 'a"b', "", "abc$", "zone", "name",
          "json", "between", "xmltable", "merge_action", "abort", "é", "a_1", "ABC", "table1", "do", "is",
          "authorization", "current_user", "values", "\"", "a\"\"b"]:
    r.case("quote_ident", "SELECT quote_ident(%s::text)::text", [s])
for s in ["it's", "back\\slash", "both ' and \\", "", "plain", "\\", "''", "日本'語"]:
    r.case("quote_literal", "SELECT quote_literal(%s::text)::text", [s])

# substr
SUB = ["", "abc", "héllo wörld", "日本語テキスト", "👍🏽🎉x"]
for s in SUB:
    for start in [-5, -1, 0, 1, 2, 3, 5, 7, 11, 12, 100, 2147483647, -2147483648]:
        r.case("substr", "SELECT substr(%s::text, %s::int4)::text", [s, start])
        for ln in [-1, 0, 1, 2, 3, 6, 100, 2147483647, -2147483648]:
            r.case("substr", "SELECT substr(%s::text, %s::int4, %s::int4)::text", [s, start, ln])

# substr bytea
for hx in ["", "00", "0102030405", "deadbeef00ff"]:
    for start in [-3, 0, 1, 2, 4, 6, 7, 2147483647]:
        r.case("substr_bytes", "SELECT substr(decode(%s, 'hex'), %s::int4)::text", [hx, start])
        for ln in [-1, 0, 1, 2, 3, 10, 2147483647]:
            r.case("substr_bytes", "SELECT substr(decode(%s, 'hex'), %s::int4, %s::int4)::text", [hx, start, ln])

# strpos
for s, n in [("abc", "b"), ("abc", ""), ("", ""), ("", "a"), ("héllo wörld", "wö"), ("héllo wörld", "ö"),
             ("日本語テキスト", "テキ"), ("aaa", "aa"), ("abcabc", "cab"), ("abc", "abcd"), ("abc", "c"),
             ("👍🏽🎉x", "x"), ("👍🏽🎉x", "🎉"), ("abc", "B"), ("ab", "ab")]:
    r.case("strpos", "SELECT strpos(%s::text, %s::text)::text", [s, n])

# overlay
for s in ["Txxxxas", "héllo", "", "日本語"]:
    for repl in ["hom", "", "ÄÖ"]:
        for frm in [-1, 0, 1, 2, 4, 6, 10, 2147483647]:
            r.case("overlay", "SELECT overlay(%s::text placing %s::text from %s::int4)::text", [s, repl, frm])
            for ln in [-3, -1, 0, 1, 2, 4, 100, 2147483647, -2147483648]:
                r.case("overlay", "SELECT overlay(%s::text placing %s::text from %s::int4 for %s::int4)::text",
                       [s, repl, frm, ln])

# trims
for s in ["  abc  ", "xxabcxx", "abc", "", "xyxyx", "ééaéé", "日本日本語日", " \t abc\t "]:
    for chars in [" ", "x", "xy", "", "é", "日", " \t", "abcxyz"]:
        for fn in ["btrim", "ltrim", "rtrim"]:
            r.case(fn, f"SELECT {fn}(%s::text, %s::text)::text", [s, chars])

# replace
for s, f, t in [("abcabc", "b", "X"), ("abc", "", "X"), ("", "a", "b"), ("aaaa", "aa", "b"), ("héllo", "é", "e"),
                ("日本語日本", "日本", "にほん"), ("abc", "abc", ""), ("abc", "d", "e"), ("aaa", "a", "aa"),
                ("ababab", "aba", "X")]:
    r.case("replace", "SELECT replace(%s::text, %s::text, %s::text)::text", [s, f, t])

# split_part
for s in ["a,b,c", "", "abc", ",a,,b,", "a~~b~~c", "日本,語,テ", "aaaa"]:
    for d in [",", "", "~~", "x", "aa", "語"]:
        for n in [-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5]:
            r.case("split_part", "SELECT split_part(%s::text, %s::text, %s::int4)::text", [s, d, n])

# left / right
for s in ["", "abc", "héllo wörld", "日本語"]:
    for n in [-2147483648, -100, -4, -3, -2, -1, 0, 1, 2, 3, 4, 100, 2147483647]:
        r.case("left", "SELECT left(%s::text, %s::int4)::text", [s, n])
        r.case("right", "SELECT right(%s::text, %s::int4)::text", [s, n])

# lpad / rpad
for s in ["", "hi", "héllo", "日本語"]:
    for ln in [-1, 0, 1, 2, 3, 5, 10, 268435454, 268435455, 2147483647]:
        for fill in ["", " ", "xy", "éß", "日"]:
            if ln >= 268435454 and fill != "" and ln < 300000000:
                # Would actually allocate ~1GB in PostgreSQL; skip the success path.
                if ln == 268435454:
                    continue
            r.case("lpad", "SELECT lpad(%s::text, %s::int4, %s::text)::text", [s, ln, fill])
            r.case("rpad", "SELECT rpad(%s::text, %s::int4, %s::text)::text", [s, ln, fill])

# repeat
for s in ["", "ab", "é", "日本"]:
    for n in [-1, 0, 1, 3, 268435454, 536870911, 1073741819, 2147483647]:
        if n >= 268435454 and s != "":
            if (n * len(s.encode())) + 4 <= 0x3fffffff:
                continue  # would allocate a huge string
        r.case("repeat", "SELECT repeat(%s::text, %s::int4)::text", [s, n])

# translate
for s, f, t in [("12345", "143", "ax"), ("abc", "", "x"), ("", "a", "b"), ("héllo", "éo", "EO"), ("aaa", "a", ""),
                ("abcabc", "cba", "xyz"), ("日本語", "本", "ほ"), ("abc", "aa", "xy"), ("abc", "abc", "x")]:
    r.case("translate", "SELECT translate(%s::text, %s::text, %s::text)::text", [s, f, t])

# chr
for c in [-1, 0, 1, 65, 127, 128, 233, 0x7ff, 0x800, 0xd7ff, 0xd800, 0xdfff, 0xe000, 0xffff, 0x10000, 0x10ffff,
          0x110000, 2147483647, -2147483648]:
    r.case("chr", "SELECT chr(%s::int4)::text", [c])

# starts_with
for s, p in [("abc", "a"), ("abc", ""), ("", ""), ("", "a"), ("héllo", "hé"), ("héllo", "he"), ("abc", "abcd")]:
    r.case("starts_with", "SELECT starts_with(%s::text, %s::text)::text", [s, p])

# sha
for s in ["", "abc", "héllo", "The quick brown fox jumps over the lazy dog"]:
    for fn in ["sha224", "sha256", "sha384", "sha512"]:
        r.case(fn, f"SELECT encode({fn}(convert_to(%s::text, 'UTF8')), 'hex')", [s])

# to_hex / to_bin / to_oct
for v in [0, 1, -1, 255, 256, -256, 2147483647, -2147483648, 9223372036854775807, -9223372036854775808, 123456789]:
    for fn in ["to_hex", "to_bin", "to_oct"]:
        r.case(fn, f"SELECT {fn}(%s::int8)::text", [v])
        if -2147483648 <= v <= 2147483647:
            r.case(fn + "32", f"SELECT {fn}(%s::int4)::text", [v])

# format
FMT = [
    ("Hello %s", ["World"]),
    ("%s %s %s", ["a", None, "c"]),
    ("%I", ["select"]), ("%I", ["Foo bar"]), ("%I", [None]), ("%L", [None]), ("%L", ["O'Reilly"]),
    ("%L", ["back\\slash"]), ("%%", []), ("100%%", []), ("%", []), ("abc%", []), ("%s", []),
    ("%1$s %1$s", ["x"]), ("%2$s %1$s", ["a", "b"]), ("%0$s", ["a"]), ("%3$s", ["a"]),
    ("%10s|", ["abc"]), ("%-10s|", ["abc"]), ("%*s|", ["10", "abc"]), ("%-*s|", ["10", "abc"]),
    ("%*s|", ["-10", "abc"]), ("%*s|", [None, "abc"]), ("%*s|", ["x", "abc"]), ("%*s|", ["99999999999", "abc"]),
    ("%1$*2$s|", ["abc", "8"]), ("%*1$s|", ["5"]), ("%*1s", ["5"]), ("%x", ["a"]), ("%é", ["a"]),
    ("%5", ["a"]), ("%5$", ["a"]), ("%-", ["a"]), ("%*", ["a"]), ("%1$", ["a"]),
    ("%3s|", ["日本語テキスト"]), ("%8s|", ["日本語"]), ("%-8L|", ["日本"]), ("%s", ["a", "b", "c"]),
    ("%s %s", ["a"]), ("%1$s %s", ["a", "b"]), ("%2$s %s", ["a", "b"]), ("%2147483648s", ["a"]),
    ("%2147483647$s", ["a"]), ("%*s|", ["-2147483648", "abc"]), ("%*s|", ["0x10", "ab"]),
    ("%*s|", [" 12 ", "ab"]), ("%*s|", ["1_0", "ab"]), ("%--5s|", ["ab"]), ("%-5-s|", ["ab"]),
    ("%5-s|", ["ab"]), ("%I %L %s", ["a b", "c'd", None]), ("日本%s語", ["テ"]), ("%s", [""]),
    ("%I", [""]), ("%L", [""]), ("%*2$s|", ["ab", "4"]), ("%*3$s|", ["ab", "4"]),
]
for fmt, args in FMT:
    r.case("format", "SELECT format(%s::text, VARIADIC %s::text[])::text", [fmt, args])

# string_to_array
for s in ["a,b,c", "", "abc", ",a,,b,", "a,NULL,b", "日本,語", "aXbXc", "a~~b~~"]:
    for d in [",", "", None, "X", "~~"]:
        for n in [None, "", "NULL", "a", "b"]:
            r.case("string_to_array", "SELECT to_json(string_to_array(%s::text, %s::text, %s::text))::text", [s, d, n])

# unistr
for s in ["d\\0061t\\+000061", "\\u00e9", "\\U0001F600", "\\\\", "abc", "\\", "\\x", "\\00", "\\0000",
          "\\D800\\DC00", "\\D800", "\\DC00", "\\D800x", "\\D800\\\\", "\\+110000", "\\U00110000",
          "\\uD83D\\uDE00", "\\u00", "a\\u0062c", "\\+00006", "\\U0000006", "日本\\u8a9e", "\\uzzzz",
          "\\D83D\\u0041", "\\FFFF", "\\U00000000", "\\+000000"]:
    r.case("unistr", "SELECT unistr(%s::text)::text", [s])

r.write()
