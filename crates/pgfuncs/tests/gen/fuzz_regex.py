"""Randomized differential test data for the regex engine.

Usage: python3 fuzz_regex.py <out.jsonl> <n_patterns> <seed>

Writes cases in the same format as pgeval.Recorder. tests/data/regex_fuzz.jsonl
is a committed subset (seed 777, 4000 patterns); larger runs can be checked with
PGFUNCS_FUZZ_FILE=<out.jsonl> cargo test -p pgfuncs --test pattern_golden -- --ignored
"""
import os
import random
import sys

sys.path.insert(0, os.path.dirname(__file__))
from pgeval import Recorder  # noqa: E402

out, npat, seed = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
rng = random.Random(seed)
r = Recorder(out)

CHARS = ["a", "b", "c", "A", "B", "é", "É", " ", "1", "x"]
CLASSES = ["[ab]", "[^a]", "[a-c]", "[[:alpha:]]", "[[:upper:]]", "[[:lower:]]", "[[:digit:]]", "[[:space:]]",
           "\\d", "\\w", "\\s", "\\D", "\\W", "\\S", "[\\w ]", "[^\\s]", "[éa]", "[[:alnum:]]", "[A-Za-z]", ".",
           "\\n", "[^ab\\n]", "[[:punct:]]"]
ANCHORS = ["^", "$", "\\A", "\\Z", "\\m", "\\M", "\\y", "\\Y"]
QUANTS = ["*", "+", "?", "*?", "+?", "??", "{2}", "{1,2}", "{0,1}", "{1,}", "{1,2}?", "{0,}?", "{2,3}", "{0,2}",
          "{3}", "{2,}?"]


class Ctx:
    def __init__(self):
        self.ngroups = 0
        self.closed = []


def atom(depth, ctx, in_lacon):
    k = rng.random()
    if depth > 3 or k < 0.35:
        return rng.choice(CHARS), True
    if k < 0.50:
        return rng.choice(CLASSES), True
    if k < 0.64:
        if in_lacon:
            return "(" + regex(depth + 1, ctx, in_lacon) + ")", True
        ctx.ngroups += 1
        n = ctx.ngroups
        inner = regex(depth + 1, ctx, in_lacon)
        ctx.closed.append(n)
        return "(" + inner + ")", True
    if k < 0.72:
        return "(?:" + regex(depth + 1, ctx, in_lacon) + ")", True
    if k < 0.78 and ctx.closed and not in_lacon:
        return "\\" + str(rng.choice(ctx.closed)), True
    if k < 0.84:
        return rng.choice(ANCHORS), False
    if k < 0.90 and not in_lacon:
        kind = rng.choice(["(?=", "(?!", "(?<=", "(?<!"])
        return kind + regex(depth + 2, ctx, True) + ")", False
    return rng.choice(CHARS), True


def branch(depth, ctx, in_lacon):
    n = rng.randint(1, 4)
    parts = []
    for _ in range(n):
        a, quantifiable = atom(depth, ctx, in_lacon)
        if quantifiable and rng.random() < 0.4:
            a += rng.choice(QUANTS)
        parts.append(a)
    return "".join(parts)


def regex(depth, ctx, in_lacon=False):
    nb = 1 if rng.random() < 0.75 else rng.randint(2, 3)
    return "|".join(branch(depth, ctx, in_lacon) for _ in range(nb))


def subject():
    n = rng.randint(0, 9)
    alphabet = "aabbcABé É1x\n"
    return "".join(rng.choice(alphabet) for _ in range(n))


FLAGS = ["", "", "", "i", "n", "p", "w", "in", "x"]

for i in range(npat):
    pat = regex(0, Ctx())
    flags = rng.choice(FLAGS)
    if "x" in flags:
        pat = pat.replace(" ", "\\ ")
    for _ in range(2):
        s = subject()
        r.case("fmatch", "SELECT to_json(regexp_match(%s::text, %s::text, %s::text))::text", [s, pat, flags])
    s = subject() + subject()
    r.case("fmatches", "SELECT coalesce(json_agg(m), '[]')::text FROM regexp_matches(%s::text, %s::text, %s::text) m",
           [s, pat, flags + "g"])
    r.case("freplace", "SELECT regexp_replace(%s::text, %s::text, %s::text, %s::text)::text",
           [s, pat, "<\\&|\\1|\\2>", flags + "g"])
    if rng.random() < 0.3:
        r.case("fsplit", "SELECT to_json(regexp_split_to_array(%s::text, %s::text, %s::text))::text", [s, pat, flags])
        r.case("fcount", "SELECT regexp_count(%s::text, %s::text, %s::int4, %s::text)::text",
               [s, pat, rng.randint(1, 4), flags])
        r.case("fis_match", "SELECT regexp_like(%s::text, %s::text, %s::text)::text", [s, pat, flags])

r.write()
