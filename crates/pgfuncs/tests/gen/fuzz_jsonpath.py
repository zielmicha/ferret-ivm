"""Randomized differential test data for jsonpath (larger than the committed
golden set).

Usage: PGFUNCS_PGHOST=... PGFUNCS_PGPORT=... python3 fuzz_jsonpath.py <out.jsonl> <n> <seed>

Writes cases in the format of tests/data/jsonpath_golden.jsonl; check them with
JSONPATH_GOLDEN_FILE=<out.jsonl> cargo test -p pgfuncs --test jsonpath_golden
"""
import json
import os
import random
import sys

sys.path.insert(0, os.path.dirname(__file__))
import gen_jsonpath as gj  # noqa: E402

out, n, seed = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
gj.OUT = out
rnd = random.Random(seed)
rp = gj.RandomPaths(rnd)

DCH = ["YYYY", "YYY", "YY", "Y", "MM", "DD", "DDD", "HH24", "HH12", "HH", "MI", "SS", "MS", "US", "FF1", "FF3",
       "FF6", "TZH", "TZM", "TZ", "OF", "AM", "PM", "Mon", "Month", "Day", "Dy", "J", "IYYY", "IW", "ID", "CC",
       "BC", "AD", "SSSSS", "Q", "RM", "FM", "TH", "W", "WW", "D", "\"T\"", "\"x\""]
SEPS = ["-", ".", "/", ",", "'", ":", ";", " ", ""]


def rnd_template():
    parts = []
    for _ in range(rnd.randint(1, 6)):
        parts.append(rnd.choice(DCH))
        parts.append(rnd.choice(SEPS))
    t = "".join(parts)
    if rnd.random() < 0.1:
        t = gj.mutate(rnd, t)
    return t


def rnd_dt_string():
    y = rnd.choice(["2017", "17", "0001", "9999", "2016", "1999", "20170", "2"])
    m = rnd.choice(["03", "3", "12", "13", "00", "02"])
    d = rnd.choice(["10", "1", "29", "31", "32"])
    hh = rnd.choice(["12", "00", "23", "24", "1", "11"])
    mi = rnd.choice(["34", "00", "59", "60", "5"])
    ss = rnd.choice(["56", "00", "59", "60"])
    frac = rnd.choice(["", ".1", ".123", ".123456", ".1234567"])
    tz = rnd.choice(["", "+03", "-05:30", " +03:00", "Z", " PST", " MSK", "+16", "+15:59", " UTC"])
    shape = rnd.random()
    if shape < 0.25:
        s = f"{y}-{m}-{d}"
    elif shape < 0.45:
        s = f"{hh}:{mi}:{ss}{frac}{tz}"
    elif shape < 0.85:
        s = f"{y}-{m}-{d}{rnd.choice([' ', 'T', 't', '  '])}{hh}:{mi}:{ss}{frac}{tz}"
    else:
        s = rnd.choice(["10.03.2017", "03/10/2017", "10 Mar 2017", "Friday, 10 Mar 2017", "2017-069", "2457823",
                        "12:34 PM", "11:59:59 P.M.", "2,017-03-10", "2017-III-10", "45296"])
    if rnd.random() < 0.1:
        s = gj.mutate(rnd, s)
    return s


DT_METHODS = [".datetime()", ".date()", ".time()", ".time_tz()", ".timestamp()", ".timestamp_tz()", ".time(2)",
              ".time_tz(0)", ".timestamp(3)", ".timestamp_tz(1)"]
TZS = ["UTC", "Europe/Warsaw", "America/New_York", "Asia/Kolkata", "+05", "UTC+3", "Australia/Lord_Howe"]


def dt_path():
    r = rnd.random()
    if r < 0.4:
        p = "$" + rnd.choice(DT_METHODS)
    elif r < 0.7:
        p = "$.datetime(" + json.dumps(rnd_template()) + ")"
    else:
        a = "$" + rnd.choice(DT_METHODS)
        b = json.dumps(rnd_dt_string()) + rnd.choice(DT_METHODS)
        p = "$ ? (@" + rnd.choice(DT_METHODS) + " " + rnd.choice(["==", "<", ">=", "!="]) + " " + b + ")"
        if rnd.random() < 0.3:
            p = a + " " + rnd.choice(["==", "<"]) + " " + b
    if rnd.random() < 0.3:
        p += rnd.choice([".type()", ".string()", ".size()", ".datetime()"])
    if rnd.random() < 0.2:
        p = "strict " + p
    return p


def main():
    g = gj.Gen()
    for i in range(n):
        if i % 1000 == 0:
            g.set_tz(rnd.choice(TZS))
        r = rnd.random()
        if r < 0.15:
            p = rp.path()
            if rnd.random() < 0.4:
                p = gj.mutate(rnd, p)
            g.parse(p)
        elif r < 0.75:
            fn = rnd.choice(["query", "query", "query", "query_first", "exists", "match", "exists_op", "match_op",
                             "query_array"])
            g.query(fn, gj.rnd_doc(rnd), rp.path(), rnd.choice(gj.RANDOM_VARS), rnd.random() < 0.4)
        else:
            doc = json.dumps(rnd_dt_string()) if rnd.random() < 0.8 else \
                json.dumps([rnd_dt_string() for _ in range(rnd.randint(1, 3))])
            fn = rnd.choice(["query", "query_tz", "query_tz", "exists_tz", "match", "query_first_tz"])
            g.query(fn, doc, dt_path() if not doc.startswith("[") else "$[*]" + rnd.choice(DT_METHODS),
                    "{}", rnd.random() < 0.3)
    g.r.write()


if __name__ == "__main__":
    main()
