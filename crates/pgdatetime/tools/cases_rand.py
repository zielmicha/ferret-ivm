"""Golden cases: randomized (fixed seed) differential cases and exhaustive
time zone coverage."""

import os
import random
import struct
import subprocess

POSTGRES_EPOCH_JDATE = 2451545


def seeded(default):
    """RNG; $PGDT_SEED overrides the seed for one-off larger sweeps."""
    s = os.environ.get("PGDT_SEED")
    return random.Random(default if s is None else int(s) * 1000003 + default)


def scaled(n):
    """Iteration count; $PGDT_SCALE multiplies it for one-off larger sweeps."""
    return n * int(os.environ.get("PGDT_SCALE", "1"))


def j2date(jd):
    julian = jd + 32044
    quad = julian // 146097
    extra = (julian - quad * 146097) * 4 + 3
    julian += 60 + quad * 3 + extra // 146097
    quad = julian // 1461
    julian -= quad * 1461
    y = julian * 4 // 1461
    julian = ((julian + 305) % 365 if y != 0 else (julian + 306) % 366) + 123
    y += quad * 4
    year = y - 4800
    quad = julian * 2141 // 65536
    day = julian - 7834 * quad // 256
    month = (quad + 10) % 12 + 1
    return year, month, day


def fmt_ts(jd, usec):
    y, m, d = j2date(jd)
    bc = y <= 0
    yy = -(y - 1) if bc else y
    h, rem = divmod(usec, 3600_000_000)
    mi, rem = divmod(rem, 60_000_000)
    s, us = divmod(rem, 1_000_000)
    out = f"{yy:04d}-{m:02d}-{d:02d} {h:02d}:{mi:02d}:{s:02d}"
    if us:
        out += f".{us:06d}"
    if bc:
        out += " BC"
    return out


def fmt_date(jd):
    y, m, d = j2date(jd)
    bc = y <= 0
    yy = -(y - 1) if bc else y
    return f"{yy:04d}-{m:02d}-{d:02d}" + (" BC" if bc else "")


def rand_jd(rng):
    r = rng.random()
    if r < 0.6:
        return rng.randint(2415021, 2488070)  # 1900..2100
    if r < 0.8:
        return rng.randint(1721426, 2816788)  # 1..3000
    if r < 0.9:
        return rng.randint(0, 1721425)  # BC
    return rng.randint(0, 109203527)  # full timestamp range


def rand_ts(rng):
    usec = rng.choice([0, rng.randint(0, 86399) * 1_000_000, rng.randint(0, 86_399_999_999)])
    return fmt_ts(rand_jd(rng), usec)


def rand_interval(rng):
    parts = []
    r = rng.random()
    if r < 0.3:
        parts.append(f"{rng.randint(-50, 50)} years")
    if rng.random() < 0.5:
        parts.append(f"{rng.randint(-40, 40)} mons")
    if rng.random() < 0.5:
        parts.append(f"{rng.randint(-400, 400)} days")
    if rng.random() < 0.6:
        h = rng.randint(-100, 100)
        parts.append(f"{h} hours {rng.randint(0, 59)} min {rng.randint(0, 59)}.{rng.randint(0, 999999):06d} sec")
    if rng.random() < 0.05:
        parts.append(f"{rng.randint(-100000, 100000)} years")
    if not parts:
        parts.append(f"{rng.randint(-1000000, 1000000)} seconds")
    return " ".join(parts)


RZONES = ["UTC", "Europe/Warsaw", "America/New_York", "Australia/Sydney", "Asia/Kolkata", "America/St_Johns",
          "Pacific/Chatham", "Asia/Kathmandu", "Europe/London", "America/Sao_Paulo", "Europe/Moscow",
          "Africa/Casablanca", "Pacific/Apia", "America/Los_Angeles", "Asia/Tokyo", "Europe/Dublin",
          "America/Santiago", "Asia/Tehran", "Australia/Lord_Howe", "Pacific/Kiritimati", "America/Havana",
          "Asia/Gaza", "Africa/Cairo", "America/Asuncion", "Europe/Lisbon", "Antarctica/Troll", "-7", "+11"]

FIELDS = ["microseconds", "milliseconds", "second", "minute", "hour", "day", "week", "month", "quarter", "year",
          "decade", "century", "millennium", "julian", "isoyear", "dow", "isodow", "doy", "epoch", "timezone",
          "timezone_hour", "timezone_minute"]
TRUNC = ["microseconds", "milliseconds", "second", "minute", "hour", "day", "week", "month", "quarter", "year",
         "decade", "century", "millennium"]
FORMATS = ["YYYY-MM-DD HH24:MI:SS.US TZ OF", "Day DD Month YYYY HH12:MI:SS AM", "IYYY-IW-ID DDD IDDD J Q WW W",
           "Y,YYY YYY YY Y CC BC FMDDth FMMMth", "TZH:TZM SSSS MS FF3"]


def cat_random(write):
    rng = seeded(12345)
    cases = []
    for _ in range(scaled(1200)):
        t = rand_ts(rng)
        iv = rand_interval(rng)
        z = rng.choice(RZONES)
        cases.append(("timestamp_pl_interval", "UTC", [t, iv]))
        cases.append(("timestamptz_pl_interval", z, [t, iv]))
        cases.append(("timestamptz_mi_interval", z, [t, iv]))
        cases.append(("timestamp_mi", "UTC", [t, rand_ts(rng)]))
        cases.append(("age", "UTC", [t, rand_ts(rng)]))
        cases.append(("age_tz", z, [t, rand_ts(rng)]))
        cases.append(("timestamptz_in", z, [t]))
        cases.append(("timestamp_to_timestamptz", z, [t]))
        cases.append(("extract_timestamptz", z, [rng.choice(FIELDS), t]))
        cases.append(("date_part_timestamptz", z, [rng.choice(FIELDS), t]))
        cases.append(("extract_timestamp", "UTC", [rng.choice(FIELDS), t]))
        cases.append(("date_trunc_timestamptz", z, [rng.choice(TRUNC), t]))
        cases.append(("date_trunc_timestamp", "UTC", [rng.choice(TRUNC), t]))
        cases.append(("to_char_timestamptz", z, [t, rng.choice(FORMATS)]))
        cases.append(("timezone_tstz", "UTC", [rng.choice(RZONES[:-2]), t + "+00"]))
        cases.append(("timezone_ts", z, [rng.choice(RZONES[:-2]), t]))
        cases.append(("interval_in", "UTC", [iv]))
        cases.append(("interval_mul", "UTC", [iv, repr(rng.uniform(-10, 10))]))
        cases.append(("interval_div", "UTC", [iv, repr(rng.uniform(-10, 10))]))
        cases.append(("justify_interval", "UTC", [iv]))
        cases.append(("extract_interval", "UTC", [rng.choice(FIELDS[:13] + ["epoch"]), iv]))
        cases.append(("date_part_interval", "UTC", [rng.choice(FIELDS[:13] + ["epoch"]), iv]))
        cases.append(("date_trunc_interval", "UTC", [rng.choice(TRUNC), iv]))
        cases.append(("to_char_interval", "UTC", [iv, "YYYY MM DD HH24:MI:SS.US"]))
        jd = rand_jd(rng)
        cases.append(("date_pl_interval", "UTC", [fmt_date(jd), iv]))
        cases.append(("extract_date", "UTC", [rng.choice(FIELDS), fmt_date(jd)]))
        cases.append(("date_bin", "UTC", [rng.choice(["15 min", "1 hour", "1 day", "7 days", "17 seconds"]),
                                          t, rand_ts(rng)]))
    write("random", cases)


def tzif_transitions(path):
    """Transition times (v2 64-bit section) of a TZif file."""
    data = open(path, "rb").read()

    def hdr(off):
        return struct.unpack(">6l", data[off + 20:off + 44]), data[off + 4:off + 5]

    (isut, isstd, leap, timecnt, typecnt, charcnt), ver = hdr(0)
    if ver == b"\0":
        return []
    off = 44 + timecnt * 5 + typecnt * 6 + charcnt + leap * 8 + isstd + isut
    (isut, isstd, leap, timecnt, typecnt, charcnt), _ = hdr(off)
    p = off + 44
    return [struct.unpack(">q", data[p + 8 * i:p + 8 * i + 8])[0] for i in range(timecnt)]


def all_zone_names(psql):
    p = subprocess.run(psql + ["-At", "-X", "-c", "SELECT name FROM pg_timezone_names ORDER BY name"],
                       capture_output=True, text=True, check=True)
    return [l for l in p.stdout.splitlines() if l and not l.startswith("posix/") and l != "localtime"]


def unix_to_pg_text(t):
    days, secs = divmod(t, 86400)
    return fmt_ts(days + 2440588, secs * 1_000_000)


def cat_zones(write):
    import gen_golden
    zones = all_zone_names(gen_golden.PSQL)
    rng = seeded(777)
    cases = []
    for z in zones:
        path = os.path.join("/usr/share/zoneinfo", z)
        trans = tzif_transitions(path) if os.path.exists(path) else []
        # transitions between 1950 and 2037 (sample) plus fixed sample instants
        sel = [t for t in trans if -631152000 <= t <= 2145916800]
        if len(sel) > scaled(4):
            sel = sorted(rng.sample(sel, scaled(4)))
        for t in sel:
            for d in (-3600, -1, 0, 1, 1800, 3600):
                cases.append(("timestamptz_in", z, [unix_to_pg_text(t + d) + "+00"]))
            for d in (-7200, -3600, -1800, 0, 1800, 3600, 7200):
                # local wall-clock times around the transition
                cases.append(("timestamptz_in", z, [unix_to_pg_text(t + d)]))
        for y in (1850, 1900, 1970, 2000, 2024, 2038, 2050, 2100, 2500):
            for md in ("01-15", "07-15"):
                cases.append(("timestamptz_in", z, [f"{y}-{md} 12:00:00"]))
                cases.append(("timestamptz_in", z, [f"{y}-{md} 12:00:00+00"]))
        cases.append(("timezone_tstz", "UTC", [z, "2020-07-01 12:00:00+00"]))
        cases.append(("timezone_tstz", "UTC", [z.upper(), "2020-01-01 12:00:00+00"]))
        cases.append(("timezone_ts", "UTC", [z.lower(), "2020-07-01 12:00:00"]))
        if z != "Factory":  # not representable as chrono_tz::Tz (abbreviation "-00")
            cases.append(("to_char_timestamptz", z, ["2020-07-01 12:00:00+00", "TZ tz OF TZH:TZM"]))
            cases.append(("to_char_timestamptz", z, ["2020-01-01 12:00:00+00", "TZ tz OF TZH:TZM"]))
        cases.append(("session_tz", z, []))
    write("zones", cases)


def cat_input_fuzz(write):
    """Random recombinations of date/time/zone input tokens."""
    rng = seeded(4242)
    dates = ["2020-01-15", "1/15/2020", "15-Jan-2020", "Jan 15 2020", "2020.015", "20200115", "January 15, 2020",
             "2020-Jan-15", "15 January 2020", "Wed Jan 15 2020", "J2458864", "01/15/20", "2020/01/15", "15.01.2020",
             "2020-01-15 BC", "0099-01-15", "99-01-15", "2020-01", "2020", "15/01/2020", "Jan-15-20", "20-Jan-15"]
    times = ["", "12:34", "12:34:56", "12:34:56.789", "123456", "1234", "T12:34:56", "12:34 PM", "00:00:00",
             "24:00", "12:34:56.123456789", "T123456", "12:34:56 AM", "allballs", "12h", "12:34:60"]
    zones = ["", "+02", "-05:30", "PST", "Europe/Warsaw", "Z", "UTC", "+0530", "EST5EDT", "GMT+3", "MSK", "PDT",
             "America/New_York", "-8", "zulu", "+14", "-12", "CET DST", "UTC+3", "Asia/Kolkata"]
    cases = []
    for _ in range(scaled(4000)):
        parts = [rng.choice(dates), rng.choice(times), rng.choice(zones)]
        if rng.random() < 0.15:
            rng.shuffle(parts)
        s = " ".join(p for p in parts if p)
        if rng.random() < 0.1:
            s = s.upper()
        func = rng.choice(["timestamptz_in", "timestamp_in", "date_in", "time_in"])
        z = rng.choice(["UTC", "Europe/Warsaw", "America/New_York"])
        cases.append((func, z, [s]))
    units = ["year", "years", "mon", "month", "months", "day", "days", "hour", "hours", "minute", "min", "second",
             "sec", "millisecond", "ms", "microsecond", "us", "week", "weeks", "decade", "century", "millennium",
             "y", "m", "d", "h", "s", "w"]
    for _ in range(scaled(3000)):
        n = rng.randint(1, 4)
        toks = []
        for _ in range(n):
            r = rng.random()
            if r < 0.6:
                num = rng.choice([str(rng.randint(-100, 100)), f"{rng.uniform(-100, 100):.3f}", "0", "1", "-1",
                                  str(rng.randint(-10**9, 10**9))])
                toks.append(f"{num} {rng.choice(units)}")
            elif r < 0.8:
                toks.append(rng.choice(["1:02", "-1:02:03", "+04:05:06.7", "12:00", "100:00:00", "1-2", "-1-2",
                                        "05:06.789", "1 2:03:04"]))
            else:
                toks.append(rng.choice(["@", "ago", "1", "2.5"]))
        s = " ".join(toks)
        if rng.random() < 0.1:
            s = "@ " + s
        if rng.random() < 0.1:
            s += " ago"
        cases.append(("interval_in", "UTC", [s]))
    for _ in range(1000):
        s = "P"
        for u in "YMWD":
            if rng.random() < 0.4:
                s += f"{rng.choice([rng.randint(-50, 50), round(rng.uniform(-10, 10), 2)])}{u}"
        if rng.random() < 0.7:
            s += "T"
            for u in "HMS":
                if rng.random() < 0.5:
                    s += f"{rng.choice([rng.randint(-100, 100), round(rng.uniform(-100, 100), 3)])}{u}"
        cases.append(("interval_in", "UTC", [s]))
    write("input_fuzz", cases)


def cat_to_char_fuzz(write):
    rng = seeded(99)
    kws = ["YYYY", "YYY", "YY", "Y", "Y,YYY", "IYYY", "IYY", "IY", "I", "MM", "MON", "Mon", "mon", "MONTH", "Month",
           "month", "DD", "DDD", "IDDD", "D", "ID", "DAY", "Day", "day", "DY", "Dy", "dy", "WW", "W", "IW", "CC", "J",
           "Q", "RM", "rm", "HH", "HH12", "HH24", "MI", "SS", "SSSS", "SSSSS", "MS", "US", "FF1", "FF2", "FF3", "FF4",
           "FF5", "FF6", "AM", "am", "PM", "pm", "A.M.", "a.m.", "P.M.", "p.m.", "AD", "ad", "BC", "bc", "A.D.",
           "a.d.", "B.C.", "b.c.", "TZ", "tz", "TZH", "TZM", "OF"]
    pre = ["", "", "", "FM", "TM", "FMTM"]
    post = ["", "", "", "TH", "th", "SP"]
    seps = [" ", "-", "/", ":", ".", ",", "", "  ", "\"x\"", "T", "\\\"", "|"]
    cases = []
    for _ in range(scaled(3000)):
        n = rng.randint(1, 5)
        f = "".join(rng.choice(pre) + rng.choice(kws) + rng.choice(post) + rng.choice(seps) for _ in range(n))
        t = rand_ts(rng)
        z = rng.choice(RZONES)
        cases.append(("to_char_timestamptz", z, [t, f]))
        cases.append(("to_char_interval", "UTC", [rand_interval(rng), f]))
    write("to_char_fuzz", cases)


RT_FORMATS = [
    "YYYY-MM-DD HH24:MI:SS.US", "YYYY-MM-DD HH24:MI:SS.MS TZH:TZM", "DD Mon YYYY HH12:MI:SS AM", "Month DD, YYYY",
    "YYYYMMDDHH24MISS", "IYYY-IW-ID", "YYYY-DDD", "IYYY-IDDD", "J", "Day DD Month YYYY", "DD/MM/YY",
    "YY-MM-DD HH:MI PM", "Y,YYY-MM-DD", "CC YY-MM-DD", "YYYY-MM-DD BC", "YYYY-MM-DD SSSS", "FMDD.FMMM.YYYY",
    "DDth Mon YYYY", "YYYY-MM-DD HH24:MI:SS.FF3", "YYYY-MM-DD HH24:MI:SS.FF6 OF", "RM YYYY DD", "Q YYYY-MM-DD",
    "WW YYYY", "W MM YYYY", "YYYY-MM-DD HH24:MI:SS TZ", "MON-DD-YYYY", "dy, DD mon YYYY", "YYYY-MM-DD\"T\"HH24:MI",
    "FXYYYY-MM-DD", "YYYY MM DD HH24 MI SS US", "IY-IW-ID", "I-IW-ID", "YYY-MM-DD", "Y-MM-DD",
]


def cat_to_timestamp_fuzz(write):
    rng = seeded(31337)
    cases = []
    for _ in range(scaled(2500)):
        t = rand_ts(rng)
        f = rng.choice(RT_FORMATS)
        z = rng.choice(["UTC", "Europe/Warsaw", "America/New_York", "Asia/Kolkata", "Australia/Sydney"])
        cases.append(("to_timestamp_roundtrip", z, [t, f]))
        cases.append(("to_date_roundtrip", "UTC", [t, f]))
    # random garbage-ish inputs
    pieces = ["2020", "1999", "05", "5", "12", "31", "-", "/", " ", ":", ".", "Jan", "JANUARY", "Mon", "monday", "pm",
              "AM", "BC", "+05", "-03:30", "PST", "MSK", "T", "000123", "2,020", "XIV", "iv", "1st", "23rd", "\"",
              "x", "  ", "123456789", "99"]
    for _ in range(scaled(2500)):
        s = "".join(rng.choice(pieces) for _ in range(rng.randint(1, 7)))
        f = rng.choice(RT_FORMATS)
        cases.append(("to_timestamp", "UTC", [s, f]))
        cases.append(("to_date", "UTC", [s, f]))
    write("to_timestamp_fuzz", cases)


def cat_abbrevs(write):
    import gen_golden
    p = subprocess.run(gen_golden.PSQL + ["-At", "-X", "-c", "SELECT abbrev FROM pg_timezone_abbrevs ORDER BY 1"],
                       capture_output=True, text=True, check=True)
    abbrevs = [l for l in p.stdout.splitlines() if l]
    cases = []
    for ab in abbrevs:
        for inst in ["1990-07-01 12:00", "2010-07-01 12:00", "2012-01-01 12:00", "2020-07-01 12:00", "1950-01-01 12:00"]:
            cases.append(("timestamptz_in", "UTC", [f"{inst} {ab}"]))
            cases.append(("timestamptz_in", "Europe/Warsaw", [f"{inst} {ab.lower()}"]))
            cases.append(("timezone_tstz", "UTC", [ab, inst + "+00"]))
            cases.append(("timezone_ts", "UTC", [ab.lower(), inst]))
            cases.append(("to_timestamp", "UTC", [f"{inst} {ab}", "YYYY-MM-DD HH24:MI TZ"]))
        cases.append(("time_in", "UTC", [f"12:00 {ab}"]))
        cases.append(("date_trunc_timestamptz_at_zone", "UTC", ["day", "2010-07-01 12:00+00", ab]))
        cases.append(("make_timestamptz_at_zone", "UTC", [2010, 7, 1, 12, 0, "0", ab]))
    write("abbrevs", cases)


def cat_edge(write):
    cases = []
    long_inputs = [
        "2020-01-01" + " " * 300 + "12:00",
        "2020-01-01 " + "1" * 120,
        "2020-01-01 " + "1" * 140,
        "a" * 130,
        "2020-01-01 12:00:00." + "1" * 100,
        "2020-01-01 12:00:00." + "1" * 140,
        " ".join(["1"] * 24),
        " ".join(["1"] * 25),
        " ".join(["1"] * 26),
        "2020-01-01 " + " ".join(["on"] * 30),
        "2020-01-01 " + "Europe/Warsaw" * 20,
    ]
    exotic = [
        "20011225T040506.789-07", "20011225T040506-07", "20011225 040506.789-07", "20011225T040506.789",
        "2001-12-25T04:05:06.789-07:00", "J2451545-08", "J2451545+05", "J2451545 12:00-08", "julian 2451545",
        "jd 2451545.75", "j 2451545 t 12:00", "2001-12-25 t 040506", "2001-12-25 t040506", "2001-12-25t04:05:06",
        "1225 2001", "Dec 25 040506 2001", "Dec 25 040506-07 2001", "Dec 25 04:05:06 PST 2001", "25 Dec 2001 04:05",
        "2001 Dec 25 04:05 BC", "Tue Dec 25 04:05:06 2001 BC", "12/25/2001 04:05:06 PM -07", "2001-12-25 4am",
        "2001-12-25 4 am", "2001-12-25 12am", "2001-12-25 12:30am", "2001-12-25 00:30 am", "2001-12-25 04:05:06 z",
        "2001-12-25 04:05:06 zulu", "2001-12-25 04:05:06 +0", "2001-12-25 04:05:06 -00:30", "2001-12-25 04:05:06 +1:2",
        "2001-12-25 04:05:06 +1:2:3", "2001-12-25 04:05:06 +01:02:03.5", "2001-12-25 04:05:06 +12345",
        "2001.359", "2001.360", "2001.359 04:05", "1999.366", "2001-359", "01.359", "120 2001 Dec", "Dec 120 2001",
        "2001 12 25", "25 12 2001", "12 25 2001", "2001-12-25 1:2", "2001-12-25 01:02:03.000000500",
        "2001-12-25 040506.5", "2001-12-25 0405", "2001-12-25 04", "2001-12-25 4", "2001-12-25 040506 BC",
        "040506 2001-12-25", "04:05:06 2001-12-25", "04:05:06 Dec 25 2001", "Dec 25 2001 04:05:06 America/New_York",
        "2001-12-25 America/New_York 04:05:06", "America/New_York 2001-12-25 04:05:06", "2001-12-25 04:05 EST DST",
        "2001-06-25 04:05 Europe/Warsaw", "2001-12-25 04:05:06.123456789012", "2001-12-25 04:05:06.",
        "2001-12-25 04:05:06.5.5", "2001-12-25 04:05:06 . 5", "2001-12-25 04:05:06 +5 PST", "2001-12-25 PST 04:05",
        "PST 2001-12-25 04:05", "2001-12-25 04:05 PST PST", "Sun 2001-12-25", "2001-12-25 Sun", "Sunday Dec 25 2001",
        "2001-12-25 ad", "ad 2001-12-25 bc", "2001-12-25 04:05:06 allballs", "allballs 2001-12-25",
        "epoch 2001-12-25", "2001-12-25 infinity", "-infinity 12:00", "+infinity", "+ infinity", "- 5",
        "2001-12-25 -05:00:00", "2001-12-25 04:05:06-05:00:00", "2001-12-25 04:05:06 GMT+5", "2001-12-25 04:05:06 GMT-5",
        "2001-12-25 04:05:06 UTC+05:30", "2001-12-25 04:05:06 <+05>-5", "2001-12-25 04:05:06 XYZ3ABC,M3.2.0,M11.1.0",
    ]
    for s in exotic:
        for f in ["date_in", "time_in", "timestamp_in"]:
            cases.append((f, "UTC", [s]))
        for z in ["UTC", "America/New_York", "Europe/Warsaw"]:
            cases.append(("timestamptz_in", z, [s]))
    for s in long_inputs:
        for f in ["date_in", "time_in", "timestamp_in", "timestamptz_in", "interval_in"]:
            cases.append((f, "UTC", [s]))
    intervals = [" ".join(["1 day"] * 12), " ".join(["1 day"] * 13), " ".join(["1"] * 30),
                 "1 day " + "1" * 300, "1." + "1" * 250 + " seconds", "1." + "1" * 230 + " seconds",
                 "P" + "1" * 30 + "D", "PT" + "0" * 400 + "1S", "P1D" * 3]
    for s in intervals:
        cases.append(("interval_in", "UTC", [s]))
    for s, f in [("2020-01-15 ü", "YYYY-MM-DD"), ("ü2020-01-15", "ÜYYYY-MM-DD"), ("2020ü01ü15", "YYYYüMMüDD"),
                 ("2020-01-15", "YYYY MM DD"), ("2020 01 15", "YYYY MM DD"),
                 ("Ünd 2020", "Mon YYYY"), ("2020-01-15 12:00 ÜTC", "YYYY-MM-DD HH24:MI TZ")]:
        cases.append(("to_timestamp", "UTC", [s, f]))
        cases.append(("to_date", "UTC", [s, f]))
    write("edge", cases)
