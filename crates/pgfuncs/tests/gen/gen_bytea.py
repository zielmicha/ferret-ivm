#!/usr/bin/env python3
"""Generate golden data for pgfuncs::bytea from a real PostgreSQL 17.

Usage: python3 gen_bytea.py   (writes ../data/bytea_golden.jsonl)

Cases:
  {"fn": "bytea_in",  "args": [text],              "result": "<hex output>"}
  {"fn": "bytea_out_escape", "args": [hex input],  "result": "<escape output>"}
  {"fn": "encode",    "args": [hex input, format], "result": "<text>"}
  {"fn": "decode",    "args": [text, format],      "result": "<hex output>"}
"""

import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pgeval import Recorder  # noqa: E402

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "bytea_golden.jsonl")


def hexs(b):
    return "\\x" + b.hex()


BYTEA_IN = [
    # hex format
    "\\x", "\\x00", "\\xDEADbeef", "\\x de ad\tbe\nef\r", "\\x 0 1", "\\x0", "\\x0g", "\\xg0",
    "\\x é", "\\x0é", "\\x0日", "\\x00 ", " \\x00", "\\x\\x", "\\x00\\", "\\x000", "\\x0000",
    "\\xff\x0b", "\\x\x0c00", "\\x0\n0", "\\x12 34 56", "\\x  ", "\\x😀", "\\xA", "\\xAbCdEf",
    "\\x" + "".join("%02x" % i for i in range(256)),
    # escape format
    "", "abc", "a\\\\b", "\\001\\002", "\\377", "\\400", "\\39", "\\", "abc\\", "\\\\\\", "\\\\",
    "zażółć", "a\\0b", "\\0000", "\\000", "\\00", "\\0", "\\1234", "\\x".upper(), "\\X00", "x\\x00",
    "\\777", "\\378", "\\a", "\\n", "\\\\x00", "日本\\101", "\\101\\102\\103", "tab\there",
    "\\\\\\\\", "a\\", "\\'", "'", "\"", " ", "\\ 01", "\\01 ", "\\012\\",
]

ENCODE_DATA = [
    b"", b"a", b"ab", b"abc", b"abcd", b"\x00", b"\x00\xff", b"\\", b"a\\b", b"\x7f\x80\x9f\xa0\xfe\xff",
    bytes(range(32)), bytes(range(256)), "zażółć".encode(), b"a" * 56, b"a" * 57, b"a" * 58,
    b"a" * 113, b"a" * 114, b"a" * 115, b"a" * 171, b"x" * 200, b"\n\r\t", b"'\"", b" ",
]

FORMATS = ["hex", "HEX", "Hex", "base64", "BASE64", "Base64", "escape", "ESCAPE", "eScApE"]
BAD_FORMATS = ["foo", "", "base64 ", " hex", "héx", "base32", "base64url", "esc", "hex\t", "ＨＥＸ"]

DECODE_CASES = [
    # hex
    ("DEADbeef", "hex"), (" de ad ", "hex"), ("abc", "hex"), ("zz", "hex"), ("0é", "hex"),
    ("é0", "hex"), ("", "hex"), ("  ", "hex"), ("0 0", "hex"), ("00\n11\r22\t33", "hex"),
    ("\\x00", "hex"), ("0x00", "hex"), ("0\x0b0", "hex"), ("日本", "hex"), ("00 1", "hex"),
    ("FF", "HEX"),
    # base64
    ("YWJj", "base64"), ("YWI=", "base64"), ("YQ==", "base64"), ("YQ=", "base64"), ("YQ", "base64"),
    ("Y===", "base64"), ("=", "base64"), ("YW Jj\n", "base64"), ("YWJj\r\n", "base64"),
    ("AB==CDEF", "base64"), ("AB=C", "base64"), ("ABC=DEF=", "base64"), ("Y!==", "base64"),
    ("Yé==", "base64"), ("Y\x7f==", "base64"), ("YWJ\tj", "base64"), ("====", "base64"),
    ("AA==AA==", "base64"), ("A===", "base64"), ("AA=A", "base64"), ("AAA=A", "base64"),
    ("AAA=AAA=", "base64"), ("", "base64"), ("   ", "base64"), ("Y", "base64"), ("YW", "base64"),
    ("YWJ", "base64"), ("YWJjZA", "base64"), ("YWJjZA==", "base64"), ("YWJjZGU=", "base64"),
    ("+/+/", "base64"), ("-_-_", "base64"), ("YW\x0bJj", "base64"), ("YWJj=", "base64"),
    ("YWJj==", "base64"), ("YWJj===", "base64"), ("YWJj====", "base64"), ("Y W J j", "base64"),
    ("AB=", "base64"), ("AB= =", "base64"), ("AB=\n=", "base64"), ("😀", "base64"),
    ("YWJj", "Base64"),
    # escape
    ("abc", "escape"), ("a\\\\b", "escape"), ("\\001", "escape"), ("\\", "escape"), ("a\\", "escape"),
    ("\\400", "escape"), ("\\12", "escape"), ("\\1234", "escape"), ("zażółć", "escape"),
    ("\\377\\000", "escape"), ("\\\\\\", "escape"), ("", "escape"), ("\\x41", "escape"),
    ("\\0", "escape"), ("\\00", "escape"), ("\\000", "escape"), ("\\39", "escape"), ("\\a", "escape"),
    ("tab\there", "escape"), ("\\101BC", "ESCAPE"),
    # bad format names
    ("00", "foo"), ("00", ""), ("00", "Hex "), ("00", "héx"),
]


def main():
    rec = Recorder(OUT)
    for s in BYTEA_IN:
        rec.case("bytea_in", "SELECT %s::bytea::text", [s])

    rng = random.Random(4242)
    # random escape-format / hex-format inputs
    alphabet = list("\\\\\\\\0123478xX abfg\t\n") + ["é", "日"]
    seen = set(BYTEA_IN)
    for _ in range(1500):
        n = rng.randint(0, 10)
        s = "".join(rng.choice(alphabet) for _ in range(n))
        if rng.random() < 0.4:
            s = "\\x" + s
        if s in seen:
            continue
        seen.add(s)
        rec.case("bytea_in", "SELECT %s::bytea::text", [s], extra={"random": True})

    datas = ENCODE_DATA + [bytes(rng.randrange(256) for _ in range(rng.randint(0, 40))) for _ in range(40)]
    rec.cur.execute("SET bytea_output = 'escape'")
    for d in datas:
        rec.case("bytea_out_escape", "SELECT (%s::bytea)::text", [hexs(d)])
    rec.cur.execute("SET bytea_output = 'hex'")
    for d in datas:
        for f in FORMATS:
            rec.case("encode", "SELECT encode(%s::bytea, %s)", [hexs(d), f])
    for f in BAD_FORMATS:
        rec.case("encode", "SELECT encode(%s::bytea, %s)", ["\\x00", f])
        rec.case("encode", "SELECT encode(%s::bytea, %s)", ["\\x", f])

    for s, f in DECODE_CASES:
        rec.case("decode", "SELECT decode(%s, %s)::text", [s, f])
    # roundtrips of encode output through decode
    for d in ENCODE_DATA:
        for f in ["hex", "base64", "escape"]:
            kind, enc = rec.eval("SELECT encode(%s::bytea, %s)", (hexs(d), f))
            assert kind == "ok"
            rec.case("decode", "SELECT decode(%s, %s)::text", [enc, f])
    # random base64 / escape / hex decode inputs
    b64_alphabet = list("ABCxyz019+/=  \n\t") + ["!", "é", "-", "_"]
    esc_alphabet = list("\\\\\\0123478ab \t") + ["é"]
    hex_alphabet = list("0123456789abcdefABCDEFgx \n\t\r") + ["é"]
    for alphabet, f in [(b64_alphabet, "base64"), (esc_alphabet, "escape"), (hex_alphabet, "hex")]:
        seen = set()
        for _ in range(700):
            n = rng.randint(0, 12)
            s = "".join(rng.choice(alphabet) for _ in range(n))
            if s in seen:
                continue
            seen.add(s)
            rec.case("decode", "SELECT decode(%s, %s)::text", [s, f], extra={"random": True})
    rec.write()


if __name__ == "__main__":
    main()
