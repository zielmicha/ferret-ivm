"""Golden cases: casts, arithmetic, fields, constructors, time zones."""

import itertools

TS = [
    "2020-01-31 12:34:56.789", "2020-02-29 00:00:00", "2019-02-28 23:59:59.999999", "2020-03-31 10:00:00",
    "2000-01-01 00:00:00", "1999-12-31 23:59:59", "1970-01-01 00:00:00", "1969-12-31 23:59:59.5",
    "0001-01-01 00:00:00", "0001-12-31 23:59:59 BC", "0044-03-15 12:00:00 BC", "4714-11-24 00:00:00 BC",
    "294276-12-31 23:59:59.999999", "2020-12-31 23:59:59", "2021-01-01 00:00:00", "2020-06-15 15:30:00",
    "1900-02-28 12:00:00", "2100-02-28 12:00:00", "infinity", "-infinity", "2024-02-29 12:00:00",
    "2020-01-01 00:00:00.000001", "1582-10-04 12:00:00", "2020-10-25 02:30:00", "2020-03-29 02:30:00",
    "2020-03-08 02:30:00", "2020-11-01 01:30:00",
]

TSTZ = [
    "2020-01-31 12:34:56.789+00", "2020-03-29 00:30:00+00", "2020-03-29 01:30:00+00", "2020-10-25 00:30:00+00",
    "2020-10-25 01:30:00+00", "2020-03-08 06:30:00+00", "2020-03-08 07:30:00+00", "2020-11-01 05:30:00+00",
    "2020-11-01 06:30:00+00", "2020-02-29 12:00:00+00", "2000-01-01 00:00:00+00", "1970-01-01 00:00:00+00",
    "0001-01-01 00:00:00+00", "0044-03-15 12:00:00+00 BC", "4714-11-24 00:00:00+00 BC", "294276-12-31 23:59:59+00",
    "infinity", "-infinity", "2050-06-30 22:00:00+00", "2300-06-30 22:00:00+00", "1880-01-01 00:00:00+00",
    "2020-06-15 12:00:00+00", "2020-12-31 23:30:00+00", "2014-10-25 22:30:00+00", "2011-03-27 00:00:00+00",
]

INTERVALS = [
    "1 month", "-1 month", "1 year", "1 day", "-1 day", "24 hours", "1 mon 1 day", "1 day 1 second", "-1 second",
    "13 months", "-13 months", "2 years 3 months 4 days 05:06:07.89", "0", "1 week", "0.5 days", "100 years",
    "-100 years", "300000 years", "-300000 years", "infinity", "-infinity", "1 mon -1 day", "-1 mon +1 day",
    "12 hours", "1 day -12 hours", "29 days", "2147483647 days", "-2147483647 days", "00:00:00.000001",
    "1000000 days", "-1000000 days", "1 year -1 microsecond",
]

DATES = [
    "2020-01-31", "2020-02-29", "2019-02-28", "2000-01-01", "1970-01-01", "0001-01-01", "0001-12-31 BC",
    "4714-11-24 BC", "5874897-12-31", "294276-12-31", "294277-01-01", "infinity", "-infinity", "2020-12-31",
    "2024-03-31", "1900-03-01",
]

TIMES = ["00:00:00", "12:34:56.789", "23:59:59.999999", "24:00:00", "06:00:00", "18:30:00"]

TZS = ["UTC", "Europe/Warsaw", "America/New_York", "Australia/Sydney", "Asia/Kolkata", "Pacific/Chatham",
       "Europe/Moscow", "-5"]


def cat_casts(write):
    cases = []
    for d in DATES:
        cases.append(("date_to_timestamp", "UTC", [d]))
        for z in TZS:
            cases.append(("date_to_timestamptz", z, [d]))
    for t in TS:
        cases.append(("timestamp_to_date", "UTC", [t]))
        cases.append(("timestamp_to_time", "UTC", [t]))
        for z in TZS:
            cases.append(("timestamp_to_timestamptz", z, [t]))
    for t in TSTZ:
        for z in TZS:
            cases.append(("timestamptz_to_date", z, [t]))
            cases.append(("timestamptz_to_timestamp", z, [t]))
            cases.append(("timestamptz_to_time", z, [t]))
    for t in TIMES:
        cases.append(("time_to_interval", "UTC", [t]))
    for i in INTERVALS + ["-2 hours", "25 hours", "-25 hours", "1 day 01:00", "-00:00:00.000001"]:
        cases.append(("interval_to_time", "UTC", [i]))
    for v in ["2020-01-01", "infinity", "-infinity"]:
        cases.append(("isfinite_date", "UTC", [v]))
        cases.append(("isfinite_timestamp", "UTC", [v]))
    for v in ["1 day", "infinity", "-infinity"]:
        cases.append(("isfinite_interval", "UTC", [v]))
    write("casts", cases)


def cat_arith(write):
    cases = []
    for d in DATES:
        for n in [0, 1, -1, 31, 365, -730000, 2147483647, -2147483648, 5000000, 1000000000]:
            cases.append(("date_pl_days", "UTC", [d, n]))
            cases.append(("date_mi_days", "UTC", [d, n]))
    for a, b in itertools.product(DATES, DATES[:8] + ["infinity"]):
        cases.append(("date_mi_date", "UTC", [a, b]))
    for d in DATES:
        for i in INTERVALS:
            cases.append(("date_pl_interval", "UTC", [d, i]))
            cases.append(("date_mi_interval", "UTC", [d, i]))
        for t in TIMES:
            cases.append(("date_pl_time", "UTC", [d, t]))
    for t in TS:
        for i in INTERVALS:
            cases.append(("timestamp_pl_interval", "UTC", [t, i]))
            cases.append(("timestamp_mi_interval", "UTC", [t, i]))
    for z in ["UTC", "Europe/Warsaw", "America/New_York", "Australia/Sydney", "Pacific/Chatham", "-5"]:
        for t in TSTZ:
            for i in INTERVALS:
                cases.append(("timestamptz_pl_interval", z, [t, i]))
                cases.append(("timestamptz_mi_interval", z, [t, i]))
    for a, b in itertools.product(TS, TS[::3]):
        cases.append(("timestamp_mi", "UTC", [a, b]))
    for a, b in itertools.product(TSTZ, TSTZ[::4]):
        cases.append(("timestamptz_mi", "UTC", [a, b]))
    for t in TIMES:
        for i in INTERVALS[:12] + ["25 hours", "-25 hours", "100000 hours"]:
            cases.append(("time_pl_interval", "UTC", [t, i]))
            cases.append(("time_mi_interval", "UTC", [t, i]))
        for t2 in TIMES:
            cases.append(("time_mi_time", "UTC", [t, t2]))
    ivs = INTERVALS + ["178000000 years", "-178000000 years", "2147483647 months", "-2147483647 months",
                       "9223372036854775807 microseconds", "-9223372036854775807 microseconds"]
    for a, b in itertools.product(ivs[::2], ivs[::3]):
        cases.append(("interval_pl", "UTC", [a, b]))
        cases.append(("interval_mi", "UTC", [a, b]))
        cases.append(("interval_cmp", "UTC", [a, b]))
    for a in ivs:
        cases.append(("interval_um", "UTC", [a]))
        cases.append(("justify_hours", "UTC", [a]))
        cases.append(("justify_days", "UTC", [a]))
        cases.append(("justify_interval", "UTC", [a]))
        for f in ["2", "-2", "0.5", "1.5", "0", "1e-7", "3.14159", "-0.25", "1e10", "'Infinity'", "'-Infinity'",
                  "'NaN'", "1.0/3", "7", "0.1", "100"]:
            cases.append(("interval_mul", "UTC", [a, f]))
            cases.append(("interval_div", "UTC", [a, f]))
    for a in ["1 mon 25 days 01:00", "-1 mon -25 days", "1 mon -1 day", "-1 mon 1 day", "29 days 25:00",
              "30 days", "-30 days", "1 day -25 hours", "-1 day 25 hours", "35 days 49:00", "1 year -1 day 1 hour",
              "-2147483648 days -24 hours", "2147483647 days 24 hours", "2147483647 months 30 days"]:
        cases.append(("justify_hours", "UTC", [a]))
        cases.append(("justify_days", "UTC", [a]))
        cases.append(("justify_interval", "UTC", [a]))
    write("arith", cases)


DATE_FIELDS = ["microseconds", "milliseconds", "second", "minute", "hour", "day", "week", "month", "quarter",
               "year", "decade", "century", "millennium", "julian", "isoyear", "dow", "isodow", "doy", "epoch",
               "timezone", "timezone_hour", "timezone_minute", "foo", "days", "mon", "y", "yrs", "usec", "msec",
               "millennia", "c", "h", "m", "s", "us", "ms", "MONTH", "Year", "infinity", "now", "jd", "j"]


def cat_fields(write):
    cases = []
    tsvals = TS + ["2020-01-05 12:00", "2021-01-03 12:00", "2026-12-31 12:00", "2027-01-01 12:00",
                   "2020-12-28 12:00", "0002-01-01 BC", "0010-12-31 BC", "0011-01-01 BC", "1001-01-01",
                   "2001-01-01", "2000-12-31", "2008-12-29", "2010-01-03", "294276-01-01 12:00",
                   "2020-01-01 00:00:00.000000", "2020-01-01 00:00:00.000001", "2020-01-01 00:00:01",
                   "2020-01-01 00:00:10", "2020-01-01 00:01:00", "2020-01-01 06:00:00", "2020-01-01 23:59:59.999999",
                   "2020-01-01 12:34:56.123456"]
    for f in DATE_FIELDS:
        for t in tsvals:
            cases.append(("extract_timestamp", "UTC", [f, t]))
            cases.append(("date_part_timestamp", "UTC", [f, t]))
        for t in TSTZ:
            for z in ["UTC", "Europe/Warsaw", "America/St_Johns", "Asia/Kolkata", "Pacific/Chatham", "-5"]:
                cases.append(("extract_timestamptz", z, [f, t]))
                cases.append(("date_part_timestamptz", z, [f, t]))
        for d in DATES + ["2020-01-05", "2021-01-03", "0002-01-01 BC", "0011-01-01 BC", "2008-12-29"]:
            cases.append(("extract_date", "UTC", [f, d]))
            cases.append(("date_part_date", "UTC", [f, d]))
        for t in TIMES + ["00:00:00.000001", "13:01:02.5"]:
            cases.append(("extract_time", "UTC", [f, t]))
            cases.append(("date_part_time", "UTC", [f, t]))
        for i in INTERVALS + ["-1 year -2 mons -3 days -04:05:06.789", "1234 years 5 months", "-1234 years",
                              "-1 days -00:00:00.5", "123456789 hours", "11 months", "-11 months",
                              "2147483647 months 2147483647 days 2562047788 hours"]:
            cases.append(("extract_interval", "UTC", [f, i]))
            cases.append(("date_part_interval", "UTC", [f, i]))
    write("fields", cases)


TRUNC_FIELDS = ["microseconds", "milliseconds", "second", "minute", "hour", "day", "week", "month", "quarter",
                "year", "decade", "century", "millennium", "dow", "doy", "epoch", "timezone", "foo", "MONTH",
                "Week", "julian", "isoyear", "days", "y"]


def cat_trunc(write):
    cases = []
    tsvals = TS + ["2020-01-01 12:00", "2021-01-01 12:00", "2021-01-03 12:00", "2026-12-31 12:00", "2008-12-29 12:00",
                   "0002-06-15 12:00 BC", "0010-06-15 BC", "0011-06-15 BC", "0100-06-15 BC", "0101-06-15 BC",
                   "1000-06-15 BC", "1001-06-15 BC", "2000-06-15", "2001-06-15", "1999-06-15", "0100-06-15",
                   "0101-06-15", "1000-06-15", "1001-06-15"]
    for f in TRUNC_FIELDS:
        for t in tsvals:
            cases.append(("date_trunc_timestamp", "UTC", [f, t]))
        for t in TSTZ:
            for z in ["UTC", "Europe/Warsaw", "America/New_York", "Australia/Sydney", "Asia/Kathmandu", "-5"]:
                cases.append(("date_trunc_timestamptz", z, [f, t]))
            for zone in ["Europe/Warsaw", "America/New_York", "PST", "MSK", "+05", "UTC", "Foo/Bar"]:
                cases.append(("date_trunc_timestamptz_at_zone", "UTC", [f, t, zone]))
        for i in INTERVALS + ["-1 year -2 mons -3 days -04:05:06.789", "1234 years 5 months 6 days", "-1234 years",
                              "5 months", "-5 months", "1 day 12:34:56.789456", "-12:34:56.789456"]:
            cases.append(("date_trunc_interval", "UTC", [f, i]))
    write("trunc", cases)


def cat_misc(write):
    cases = []
    # date_bin
    strides = ["15 minutes", "1 hour", "1 day", "7 days", "1 microsecond", "0", "-1 hour", "1 month", "1 year",
               "1 day 1 hour", "infinity", "2147483647 days", "00:00:00.001"]
    srcs = ["2020-02-11 15:44:17.71393", "2001-01-01 00:00:00", "1999-12-31 23:59:59", "1969-12-31 23:59:59.999999",
            "infinity", "-infinity", "0001-01-01 00:00:00 BC", "294276-12-31 23:59:59", "4714-11-24 00:00:00 BC"]
    origins = ["2001-01-01", "2020-02-11 15:44:17.71393", "1970-01-01", "infinity", "4714-11-24 00:00:00 BC",
               "294276-12-31 23:59:59"]
    for s, src, o in itertools.product(strides, srcs, origins):
        cases.append(("date_bin", "UTC", [s, src, o]))
    # age
    ages = ["2020-01-31 12:00", "2020-02-29 00:00", "2020-03-31 23:59:59.5", "2019-12-31 00:00", "2000-02-29",
            "1957-06-13", "2001-04-10", "infinity", "-infinity", "0001-01-01 BC", "2020-01-01 12:00:00.000001"]
    for a, b in itertools.product(ages, ages):
        cases.append(("age", "UTC", [a, b]))
    for z in ["UTC", "Europe/Warsaw", "America/New_York"]:
        for a, b in itertools.product(TSTZ[:12], TSTZ[:12:3]):
            cases.append(("age_tz", z, [a, b]))
    # constructors
    for y, m, d in [(2020, 1, 1), (2020, 2, 29), (2019, 2, 29), (-44, 3, 15), (0, 1, 1), (-1, 1, 1), (-1, 2, 29),
                    (-5, 2, 29), (2020, 13, 1), (2020, 0, 1), (2020, 1, 0), (2020, 1, 32), (5874897, 12, 31),
                    (5874898, 1, 1), (-4713, 11, 24), (-4714, 11, 24), (-4714, 11, 23), (294277, 1, 1),
                    (2147483647, 1, 1), (-2147483647, 1, 1), (1, 1, 1), (10000, 1, 1)]:
        cases.append(("make_date", "UTC", [y, m, d]))
        cases.append(("make_timestamp", "UTC", [y, m, d, 12, 30, "45.5"]))
        cases.append(("make_timestamptz", "Europe/Warsaw", [y, m, d, 12, 30, "45.5"]))
    for h, mi, s in [(0, 0, "0"), (12, 34, "56.789"), (23, 59, "59.9999995"), (24, 0, "0"), (24, 0, "0.0000004"),
                     (24, 0, "0.000001"), (25, 0, "0"), (-1, 0, "0"), (12, 60, "0"), (12, 59, "60"), (12, 59, "60.5"),
                     (12, 0, "-1"), (12, 0, "'NaN'"), (12, 0, "'Infinity'"), (12, 0, "1e10"), (12, 0, "5"),
                     (12, 0, "5.25"), (12, 0, "0.000001"), (12, 0, "59.9999999")]:
        cases.append(("make_time", "UTC", [h, mi, s]))
        cases.append(("make_timestamp", "UTC", [2020, 6, 15, h, mi, s]))
    for args in [(2020, 3, 29, 2, 30, "0"), (2020, 10, 25, 2, 30, "0"), (2020, 7, 1, 12, 0, "0"),
                 (2300, 7, 1, 12, 0, "0"), (294276, 12, 31, 23, 59, "59"), (-4713, 11, 24, 0, 0, "0")]:
        for z in ["UTC", "Europe/Warsaw", "America/New_York", "Asia/Kolkata", "-5"]:
            cases.append(("make_timestamptz", z, list(args)))
        for zone in ["Europe/Warsaw", "America/New_York", "PST", "MSK", "+05", "-05:30", "05", "UTC", "+16",
                     "Foo/Bar", "Z", "EST5EDT", "+5:", "abc", "-1234"]:
            cases.append(("make_timestamptz_at_zone", "UTC", list(args) + [zone]))
    for args in [(0, 0, 0, 0, 0, 0, "0"), (1, 2, 3, 4, 5, 6, "7.5"), (-1, -2, -3, -4, -5, -6, "-7.5"),
                 (2147483647, 0, 0, 0, 0, 0, "0"), (178956970, 7, 0, 0, 0, 0, "0"), (178956970, 8, 0, 0, 0, 0, "0"),
                 (0, 0, 306783378, 1, 0, 0, "0"), (0, 0, 306783378, 2, 0, 0, "0"), (0, 0, 0, 0, 2147483647, 0, "0"),
                 (0, 0, 0, 0, 0, 2147483647, "0"), (0, 0, 0, 0, 0, 0, "1e15"), (0, 0, 0, 0, 0, 0, "1e16"),
                 (0, 0, 0, 0, 0, 0, "'NaN'"), (0, 0, 0, 0, 0, 0, "'Infinity'"), (0, 0, 0, 0, 0, 0, "0.0000005"),
                 (0, 0, 0, 0, 0, 0, "0.0000015"), (0, 0, 0, 0, 0, 0, "9223372036854.775807"),
                 (0, 0, 0, 0, 2147483647, 0, "1e20"), (0, 0, 0, 2147483647, 2147483647, 2147483647, "0")]:
        cases.append(("make_interval", "UTC", list(args)))
    for s in ["0", "1", "-1", "1.5", "1234567890.123456", "1e10", "-1e10", "'NaN'", "'Infinity'", "'-Infinity'",
              "-210866803200", "-210866803200.0000001", "-210866803201", "9224318016000", "9224318015999.999999",
              "9224318016000.000001", "1.0000005", "0.0000005", "-0.0000005", "1e-7", "946684800", "253402300799",
              "1e300"]:
        for z in ["UTC", "Europe/Warsaw"]:
            cases.append(("to_timestamp_epoch", z, [s]))
    # AT TIME ZONE
    zones = ["UTC", "utc", "Europe/Warsaw", "europe/warsaw", "America/New_York", "PST", "pst", "PDT", "EST", "CET",
             "CEST", "MSK", "IST", "+05", "-05", "+05:30", "-5", "5", "EST5EDT", "UTC+3", "<+03>-3", "Foo/Bar",
             "Asia/Kolkata", "Australia/Lord_Howe", "GMT", "Z", "zulu", "", "Etc/GMT+5", "Asia/Calcutta",
             "XYZ3ABC,M3.2.0,M11.1.0", "MET", "ART", "VET", "LMT", "NZDT"]
    tstzs = ["2020-01-01 12:00:00+00", "2020-07-01 12:00:00+00", "2010-07-01 12:00:00+00", "2020-03-29 01:30:00+00",
             "infinity", "-infinity", "4714-11-24 00:00:00+00 BC", "294276-12-31 23:59:59+00", "2000-07-01 12:00+00"]
    tss = ["2020-01-01 12:00:00", "2020-07-01 12:00:00", "2010-07-01 12:00:00", "2020-03-29 02:30:00",
           "2020-10-25 02:30:00", "infinity", "-infinity", "4714-11-24 00:00:00 BC", "294276-12-31 23:59:59",
           "2000-07-01 12:00"]
    for z in zones:
        for t in tstzs:
            cases.append(("timezone_tstz", "UTC", [z, t]))
        for t in tss:
            cases.append(("timezone_ts", "Europe/Warsaw", [z, t]))
    for zi in ["5 hours", "-05:30", "1 day", "1 month", "infinity", "-infinity", "00:00:00.5", "-12:00", "16 hours"]:
        for t in tstzs[:4]:
            cases.append(("timezone_tstz_interval", "UTC", [zi, t]))
        for t in tss[:4]:
            cases.append(("timezone_ts_interval", "UTC", [zi, t]))
    write("misc", cases)


TO_CHAR_FORMATS = [
    "YYYY-MM-DD HH24:MI:SS", "YYYY-MM-DD HH24:MI:SS.US", "YYYY-MM-DD HH24:MI:SS.MS", "HH HH12 HH24 MI SS",
    "HH:MI:SS AM", "HH:MI:SS PM", "HH:MI:SS am", "HH:MI:SS a.m.", "HH:MI:SS P.M.", "Mon Month MON MONTH mon month",
    "Dy Day DY DAY dy day", "FMMonth FMDay", "FMMONTH FMDAY FMmonth FMday", "TMMonth TMDay TMMon TMDy",
    "TMMONTH TMmonth TMDAY TMday", "DDD IDDD", "IW IYYY IYY IY I", "Q", "WW W", "J", "CC", "Y,YYY YYYY YYY YY Y",
    "FMY,YYY FMYYYY FMYYY FMYY", "BC AD B.C. A.D. bc ad b.c. a.d.", "TZ tz", "TZH:TZM", "OF", "FMOF", "SSSS SSSSS",
    "FF1 FF2 FF3 FF4 FF5 FF6", "MSTH USth", "DDTH DDth FMDDTH FMDDth", "MMTH", "HH24TH", "YYYYTH", "DTH", "IDth",
    "RM rm FMRM", "\"Year:\" YYYY", "\"\\\"quoted\\\"\" YYYY", "YYYY\\\"MM", "YYYY\"MM", "FMHH24:FMMI",
    "FXYYYY", "D ID", "Day, DD Month YYYY", "YYYYMMDDHH24MISS", "yyyy-mm-dd hh24:mi:ss", "Mon DD, YYYY HH12:MI AM",
    "", "HHTH HH12th", "SPDD", "TM", "FM", "FMTM", "TMFMMonth", "IYYY-IW-ID", "YYYY-\"W\"IW", "dd.mm.yyyy",
    "YYYY-MM-DD\"T\"HH24:MI:SS.MSOF", "Ü YYYY ü", "MI:SS.US", "CCth", "Jth", "WWth", "QTH",
]

TO_CHAR_TS = ["2020-01-01 00:00:00", "2020-07-04 13:05:09.123456", "2021-01-03 23:59:59.999999", "1999-12-31 12:00:00",
              "0001-01-01 00:00:00", "0044-03-15 12:00:00 BC", "0001-12-31 06:00 BC", "4714-11-24 00:00:00 BC",
              "294276-12-31 23:59:59.999999", "2020-02-29 00:00:01", "infinity", "-infinity", "2001-11-11 11:11:11",
              "2012-12-12 12:12:12.012", "2003-03-03 03:03:03.000003", "1000-01-01 00:00", "0100-06-15", "0099-06-15",
              "0010-01-01 BC", "0101-01-01 BC", "12345-06-15 10:00", "2008-12-29 12:00", "2010-01-03 12:00"]

TO_CHAR_INTERVALS = ["1 year 2 months 3 days 04:05:06.789", "-1 year -2 months -3 days -04:05:06.789", "0",
                     "15 hours", "-15 hours", "100 hours", "1 day", "13 months", "-13 months", "12 months",
                     "-12 months", "1234 years", "-1234 years", "0.5 seconds", "-0.5 seconds", "infinity",
                     "-infinity", "3 days 12:00", "-1 day 01:00", "11 months", "-11 months", "25 years 6 months"]

INTERVAL_FORMATS = ["HH24:MI:SS", "HH:MI:SS", "HH12 AM", "YYYY-MM-DD", "Y,YYY YYY YY Y", "DDD", "WW W", "Q",
                    "CC", "MM", "RM rm FMRM", "SSSS", "MS US FF1 FF6", "J", "IYYY IW IDDD", "Mon", "Day", "D",
                    "TZ", "OF", "BC", "FMHH24:FMMI", "HH24TH", "DDth", "YYYYth", "", "FMYYYY", "FMDD FMMM"]


def cat_to_char(write):
    cases = []
    for f in TO_CHAR_FORMATS:
        for t in TO_CHAR_TS:
            cases.append(("to_char_timestamp", "UTC", [t, f]))
        for t in TSTZ[:8] + ["2020-07-01 12:00+00", "2020-01-01 12:00+00", "1880-01-01 12:00+00", "infinity"]:
            for z in ["UTC", "Europe/Warsaw", "America/St_Johns", "Asia/Kolkata", "-5", "5.5", "Asia/Dubai",
                      "America/Sao_Paulo"]:
                cases.append(("to_char_timestamptz", z, [t, f]))
    for f in INTERVAL_FORMATS + TO_CHAR_FORMATS[:12]:
        for i in TO_CHAR_INTERVALS:
            cases.append(("to_char_interval", "UTC", [i, f]))
    write("to_char", cases)


TO_DATE = [
    ("2020-01-15", "YYYY-MM-DD"), ("15/01/2020", "DD/MM/YYYY"), ("01152020", "MMDDYYYY"), ("2020-01-15", "YYYYMMDD"),
    ("2020 Jan 15", "YYYY Mon DD"), ("15 January 2020", "DD Month YYYY"), ("15 JANUARY 2020", "DD MONTH YYYY"),
    ("15 january 2020", "DD month YYYY"), ("Wed 15 Jan 2020", "Dy DD Mon YYYY"), ("2020-366", "YYYY-DDD"),
    ("2021-366", "YYYY-DDD"), ("2020-02-30", "YYYY-MM-DD"), ("2020-13-01", "YYYY-MM-DD"), ("2020-1-5", "YYYY-MM-DD"),
    ("20-1-5", "YY-MM-DD"), ("69-1-5", "YY-MM-DD"), ("70-1-5", "YY-MM-DD"), ("520-1-5", "YYY-MM-DD"),
    ("519-1-5", "YYY-MM-DD"), ("5-1-5", "Y-MM-DD"), ("2020 015", "YYYY DDD"), ("2020-W03-3", "IYYY-\"W\"IW-ID"),
    ("2020-03", "IYYY-IW"), ("2020 1", "YYYY Q"), ("2459000", "J"), ("21 20", "CC YY"), ("21", "CC"), ("20 00", "CC YY"),
    ("-21 20", "CC YY"), ("0001-01-01 BC", "YYYY-MM-DD BC"), ("0044-03-15 BC", "YYYY-MM-DD AD"),
    ("0044-03-15 B.C.", "YYYY-MM-DD B.C."), ("2,020-01-15", "Y,YYY-MM-DD"), ("2020-01-15", "FXYYYY-MM-DD"),
    ("2020  -01-15", "FXYYYY-MM-DD"), ("2020  01  15", "YYYY MM DD"), ("  2020-01-15", "YYYY-MM-DD"),
    ("2020-01-15 junk", "YYYY-MM-DD"), ("2020", "YYYY-MM-DD"), ("", "YYYY-MM-DD"), ("2020-01-15", ""),
    ("2020-xx-15", "YYYY-MM-DD"), ("20200115", "YYYYMMDD"), ("2020115", "YYYYMMDD"), ("2020-1-15", "YYYY-FMMM-DD"),
    ("15th January 2020", "DDth Month YYYY"), ("1st Jan 2020", "DDth Mon YYYY"), ("XII 2020", "RM YYYY"),
    ("iv 2020", "rm YYYY"), ("2020-01-15", "YYYY-MM-DD HH24:MI:SS"), ("99999999-01-01", "YYYY-MM-DD"),
    ("5874897-12-31", "YYYY-MM-DD"), ("5874898-01-01", "YYYY-MM-DD"), ("4714-11-24 BC", "YYYY-MM-DD BC"),
    ("4714-11-23 BC", "YYYY-MM-DD BC"), ("2020-01-15", "YYYY-DD-MM"), ("2020 Mon 15", "YYYY Dy DD"),
    ("2020 Monday", "YYYY Day"), ("1 2020", "W YYYY"), ("3 2020", "WW YYYY"), ("2020-01-15", "YYYY-MM-DD-YYYY"),
    ("2020-01-15 2021", "YYYY-MM-DD YYYY"), ("2020 01 15", "YYYY MM DDD"), ("2147483648-01-01", "YYYY-MM-DD"),
    ("12345", "YYYY"), ("2020-01-15", "YYYY-MON-DD"), ("2020-Feb-15", "YYYY-MON-DD"), ("2020-FEBRUARY-15", "YYYY-MONTH-DD"),
    ("2020-Febr-15", "YYYY-Mon-DD"), ("2020-01-15", "IYYY-MM-DD"), ("2020-366", "IYYY-IDDD"),
    ("2020 53 7", "IYYY IW ID"), ("2020-01-15 10", "YYYY-MM-DD HH"), ("08 2020", "MM YYYY"), ("0 2020", "MM YYYY"),
    ("2020-01-15", "YYYY-MM-DDTH"), ("2020-1st-15", "YYYY-MMth-DD"),
]

TO_TS = [
    ("2020-01-15 12:34:56", "YYYY-MM-DD HH24:MI:SS"), ("2020-01-15 12:34:56.789", "YYYY-MM-DD HH24:MI:SS.MS"),
    ("2020-01-15 12:34:56.789123", "YYYY-MM-DD HH24:MI:SS.US"), ("2020-01-15 12:34:56.7", "YYYY-MM-DD HH24:MI:SS.MS"),
    ("2020-01-15 12:34:56.78", "YYYY-MM-DD HH24:MI:SS.US"), ("2020-01-15 12:34:56.123456", "YYYY-MM-DD HH24:MI:SS.FF3"),
    ("2020-01-15 12:34:56.123456", "YYYY-MM-DD HH24:MI:SS.FF6"), ("2020-01-15 12:34:56.123", "YYYY-MM-DD HH24:MI:SS.FF2"),
    ("2020-01-15 12:34:56.15", "YYYY-MM-DD HH24:MI:SS.FF1"), ("2020-01-15 12:34:56.99", "YYYY-MM-DD HH24:MI:SS.FF1"),
    ("2020-01-15 12:34:56 PM", "YYYY-MM-DD HH:MI:SS PM"), ("2020-01-15 12:34:56 AM", "YYYY-MM-DD HH:MI:SS AM"),
    ("2020-01-15 01:34:56 p.m.", "YYYY-MM-DD HH12:MI:SS a.m."), ("2020-01-15 13:34:56 PM", "YYYY-MM-DD HH:MI:SS PM"),
    ("2020-01-15 00:34:56 AM", "YYYY-MM-DD HH:MI:SS AM"), ("2020-01-15 24:00:00", "YYYY-MM-DD HH24:MI:SS"),
    ("2020-01-15 23:60:00", "YYYY-MM-DD HH24:MI:SS"), ("2020-01-15 23:59:60", "YYYY-MM-DD HH24:MI:SS"),
    ("2020-01-15 12:34:56 +05", "YYYY-MM-DD HH24:MI:SS TZH"), ("2020-01-15 12:34:56 -05", "YYYY-MM-DD HH24:MI:SS TZH"),
    ("2020-01-15 12:34:56 +05:30", "YYYY-MM-DD HH24:MI:SS TZH:TZM"), ("2020-01-15 12:34:56-05:30", "YYYY-MM-DD HH24:MI:SSTZH:TZM"),
    ("2020-01-15 12:34:56 -0530", "YYYY-MM-DD HH24:MI:SS TZHTZM"), ("2020-01-15 12:34:56 +16", "YYYY-MM-DD HH24:MI:SS TZH"),
    ("2020-01-15 12:34:56 PST", "YYYY-MM-DD HH24:MI:SS TZ"), ("2020-01-15 12:34:56 pst", "YYYY-MM-DD HH24:MI:SS TZ"),
    ("2020-01-15 12:34:56 CEST", "YYYY-MM-DD HH24:MI:SS TZ"), ("2010-07-15 12:34:56 MSK", "YYYY-MM-DD HH24:MI:SS TZ"),
    ("2020-01-15 12:34:56 XYZ", "YYYY-MM-DD HH24:MI:SS TZ"), ("2020-01-15 12:34:56 +03", "YYYY-MM-DD HH24:MI:SS TZ"),
    ("2020-01-15 12:34:56 +03:30", "YYYY-MM-DD HH24:MI:SS OF"), ("2020-01-15 12:34:56 -3", "YYYY-MM-DD HH24:MI:SS OF"),
    ("2020-01-15 12:34:56 ESTX", "YYYY-MM-DD HH24:MI:SS TZ"), ("2020-01-15 12:34:56", "YYYY-MM-DD HH24:MI:SS TZ"),
    ("12345", "SSSS"), ("86399", "SSSSS"), ("86400", "SSSS"), ("2020-01-15 3600", "YYYY-MM-DD SSSS"),
    ("2020-03-29 02:30:00", "YYYY-MM-DD HH24:MI:SS"), ("2020-10-25 02:30:00", "YYYY-MM-DD HH24:MI:SS"),
    ("2020-01-15T12:34:56", "YYYY-MM-DD\"T\"HH24:MI:SS"), ("2020-01-15T12:34:56", "YYYY-MM-DDTHH24:MI:SS"),
    ("15/01/2020 12h34", "DD/MM/YYYY HH24\"h\"MI"), ("20200115123456", "YYYYMMDDHH24MISS"),
    ("2020-01-15 1:2:3", "YYYY-MM-DD HH24:MI:SS"), ("2020-01-15 1:2:3", "YYYY-MM-DD FMHH24:FMMI:FMSS"),
    ("2020-01-15 12:34", "YYYY-MM-DD HH24:MI:SS"), ("12:34:56", "HH24:MI:SS"), ("2020-01-15 12:34:56.1234567", "YYYY-MM-DD HH24:MI:SS.US"),
    ("2020-01-15 12:34:56.1234567", "YYYY-MM-DD HH24:MI:SS.FF6"), ("2020-01-15 12:34:56 1234", "YYYY-MM-DD HH24:MI:SS MS"),
    ("2020-01-15 0:0:0", "YYYY-MM-DD HH12:MI:SS"), ("2020 1 15 2 PM", "YYYY MM DD HH PM"),
    ("1999-12-31 23:59:59.999999", "YYYY-MM-DD HH24:MI:SS.US"), ("294276-12-31 23:59:59", "YYYY-MM-DD HH24:MI:SS"),
    ("294277-01-01 00:00:00", "YYYY-MM-DD HH24:MI:SS"), ("0001-01-01 00:00:00 BC", "YYYY-MM-DD HH24:MI:SS BC"),
    ("2020-01-15 12:34:56", "YYYY-MM-DD HH24:MI:SS.FF3"), ("2020-01-15 12:34:56.5", "YYYY-MM-DD HH24:MI:SS.FF1"),
    ("Jan 15 2020 12:34:56", "Mon DD YYYY HH24:MI:SS"), ("2020-01-15 12:34:56 -1", "YYYY-MM-DD HH24:MI:SS TZH"),
    ("2020-01-15 12:34:56 GMT", "YYYY-MM-DD HH24:MI:SS TZ"), ("2020-01-15 12:34:56 UTC", "YYYY-MM-DD HH24:MI:SS TZ"),
    ("2020-01-15 12:34:56 Z", "YYYY-MM-DD HH24:MI:SS TZ"), ("2020-01-15 12:34:56 ZULU", "YYYY-MM-DD HH24:MI:SS TZ"),
    ("2020-01-15 12:34:56 +1", "YYYY-MM-DD HH24:MI:SS TZH"), ("2020-01-15 12:34:56 0", "YYYY-MM-DD HH24:MI:SS TZH"),
    ("2020-01-15 12:34:56-05", "YYYY-MM-DD HH24:MI:SS-TZH"), ("2020-01-15 12:34:56 05", "YYYY-MM-DD HH24:MI:SS TZM"),
]


def cat_to_date(write):
    cases = []
    for s, f in TO_DATE:
        cases.append(("to_date", "UTC", [s, f]))
    for s, f in TO_TS + TO_DATE:
        for z in ["UTC", "Europe/Warsaw", "America/New_York"]:
            cases.append(("to_timestamp", z, [s, f]))
    write("to_date", cases)


def cat_compare(write):
    cases = []
    dates = DATES + ["294276-12-31", "294277-01-01", "4714-11-24 BC", "2020-03-29", "2020-10-25"]
    tss = ["2020-01-31 00:00:00", "2020-01-30 23:00:00", "2020-01-31 00:00:01", "294276-12-31 23:59:59.999999",
           "4714-11-24 00:00:00 BC", "infinity", "-infinity", "2020-03-29 00:00", "2020-03-28 23:00",
           "2020-10-25 00:00", "1999-12-31 23:00", "2000-01-01 00:00"]
    tstzs = [t + ("+00" if t[0].isdigit() and "BC" not in t else "") for t in tss]
    for z in ["UTC", "Europe/Warsaw", "America/New_York", "Asia/Tokyo", "-5"]:
        for d in dates:
            for t in tss:
                cases.append(("date_cmp_timestamp", z, [d, t]))
            for t in tstzs:
                cases.append(("date_cmp_timestamptz", z, [d, t]))
        for t1 in tss:
            for t2 in tstzs:
                cases.append(("timestamp_cmp_timestamptz", z, [t1, t2]))
    pts = ["2020-01-01", "2020-01-05", "2020-01-10", "2020-01-15", "NULL"]
    for a in pts:
        for b in pts:
            for c in pts:
                for d in pts:
                    cases.append(("overlaps_ts", "UTC", [a, b, c, d]))
    sums = ["1 day|2 days|3 hours", "infinity|1 day", "-infinity|1 day", "infinity|-infinity", "infinity|infinity",
            "1 mon|-1 mon", "178000000 years|178000000 years", "1 day", "", "1.5 days|2 hours 30 min|-1 sec",
            "1 mon 1 day|2 mon 3 days|4 days 05:00", "infinity|178000000 years|178000000 years",
            "2147483647 days|1 day", "1 year|2 years|3 years|4 years", "-1 day|-2 hours|00:00:00.000001"]
    for s in sums:
        cases.append(("interval_sum", "UTC", [s]))
        cases.append(("interval_avg", "UTC", [s]))
    write("compare", cases)
