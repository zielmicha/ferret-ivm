#!/usr/bin/env python3
"""Generate golden data for pgfuncs::uuid from a real PostgreSQL 17.

Usage: python3 gen_uuid.py   (writes ../data/uuid_golden.jsonl)

Cases: {"fn": "uuid_in", "args": [text], "result": "<uuid_out>"} or "error".
"""

import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pgeval import Recorder  # noqa: E402

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "uuid_golden.jsonl")

U = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
H = U.replace("-", "")

CASES = [
    U, U.upper(), "{" + U + "}", "{" + U.upper() + "}", H, "{" + H + "}", H.upper(),
    "a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11", "{a0eebc99-9c0b4ef8-bb6d6bb9-bd380a11}",
    "a0eebc999c0b4ef8bb6d6bb9-bd380a11", "a0ee-bc999c0b4ef8bb6d6bb9bd380a11",
    "00000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff",
    "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF", "A0EEbc99-9C0B-4ef8-BB6D-6bb9BD380A11",
    # errors
    "", "{}", "{", "}", " " + U, U + " ", "{" + U, U + "}", "{{" + U + "}}", U + "-",
    "a0eebc99--9c0b-4ef8-bb6d-6bb9bd380a11", "a0eebc9-99c0b-4ef8-bb6d-6bb9bd380a11",
    "a0-eebc999c0b4ef8bb6d6bb9bd380a11", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1",
    "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a111", "g0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
    "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1g", "-" + U, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a-11",
    "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1-1", "é0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
    "a0eebc99 9c0b 4ef8 bb6d 6bb9bd380a11", "a0eebc99_9c0b_4ef8_bb6d_6bb9bd380a11",
    "0x" + H, H + "0", H[:-1], "{" + H, H + "}", "(" + U + ")", "[" + U + "]", "{" + U + "} ",
    "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n", "\t" + U, "a0eebc99-9c0b-4ef8-bb6d-6bb9-bd380a11",
    "a0eebc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11", "a0eebc99-9c0b-4ef8-bb6d-6bb9-bd38-0a-11",
    "{-" + H + "}", "{" + H + "-}", "日本", "😀" * 16,
]


def main():
    rec = Recorder(OUT)
    for s in CASES:
        rec.case("uuid_in", "SELECT %s::uuid::text", [s])
    rng = random.Random(777)
    hexd = "0123456789abcdefABCDEF"
    seen = set(CASES)
    for _ in range(1500):
        digits = [rng.choice(hexd) for _ in range(rng.choice([31, 32, 32, 32, 32, 32, 33]))]
        out = []
        for i, d in enumerate(digits):
            out.append(d)
            r = rng.random()
            if (i + 1) % 4 == 0 and r < 0.4:
                out.append("-")
            elif r < 0.008:
                out.append("-")
            elif r < 0.012:
                out.append(rng.choice(["--", "g", " ", "{", "}", "é"]))
        s = "".join(out)
        r = rng.random()
        if r < 0.15:
            s = "{" + s + "}"
        elif r < 0.18:
            s = "{" + s
        elif r < 0.21:
            s = s + "}"
        if s in seen:
            continue
        seen.add(s)
        rec.case("uuid_in", "SELECT %s::uuid::text", [s], extra={"random": True})
    rec.write()


if __name__ == "__main__":
    main()
