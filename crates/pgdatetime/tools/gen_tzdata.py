#!/usr/bin/env python3
"""Generate src/tz/tzdata.bin from a compiled IANA tz database directory.

PostgreSQL (as packaged on Debian, --with-system-tzdata) reads TZif files from
/usr/share/zoneinfo.  To reproduce its behaviour deterministically we embed the
version-2+ (64-bit) section and the POSIX footer of every TZif file, keyed by
every zone name (including links) that PostgreSQL would accept.

The tz database is in the public domain.

Usage: tools/gen_tzdata.py [/usr/share/zoneinfo] > src/tz/tzdata.bin
"""
import os
import struct
import sys

ZONEINFO = sys.argv[1] if len(sys.argv) > 1 else "/usr/share/zoneinfo"
SKIP_DIRS = {"right", "posix"}
SKIP_NAMES = {"localtime"}  # machine-specific symlink to /etc/localtime


def v2_section(data):
    """Return the v2+ header+data+footer of a TZif file (or v1 if old)."""
    def hdr(off):
        magic, ver = data[off:off + 4], data[off + 4:off + 5]
        counts = struct.unpack(">6l", data[off + 20:off + 44])
        return magic, ver, counts

    magic, ver, (isut, isstd, leap, timecnt, typecnt, charcnt) = hdr(0)
    assert magic == b"TZif"
    if ver == b"\0":
        return data
    v1len = 44 + timecnt * 5 + typecnt * 6 + charcnt + leap * 8 + isstd + isut
    return data[v1len:]


def main():
    version = "unknown"
    try:
        with open(os.path.join(ZONEINFO, "tzdata.zi"), "r") as f:
            first = f.readline()
            if first.startswith("# version "):
                version = first[len("# version "):].strip()
    except OSError:
        pass

    names = {}
    blobs = []
    blob_index = {}
    for root, dirs, files in os.walk(ZONEINFO):
        rel_root = os.path.relpath(root, ZONEINFO)
        if rel_root.split(os.sep)[0] in SKIP_DIRS:
            continue
        for fn in files:
            rel = os.path.normpath(os.path.join(rel_root, fn))
            if rel in SKIP_NAMES:
                continue
            path = os.path.join(root, fn)
            try:
                with open(path, "rb") as f:
                    data = f.read()
            except OSError:
                continue
            if data[:4] != b"TZif":
                continue
            sec = v2_section(data)
            if sec not in blob_index:
                blob_index[sec] = len(blobs)
                blobs.append(sec)
            names[rel.replace(os.sep, "/")] = blob_index[sec]

    out = bytearray()
    out += b"PGTZDATA"
    v = version.encode()
    out += struct.pack("<B", len(v)) + v
    out += struct.pack("<I", len(names))
    for name in sorted(names):
        nb = name.encode()
        out += struct.pack("<B", len(nb)) + nb + struct.pack("<H", names[name])
    out += struct.pack("<I", len(blobs))
    for b in blobs:
        out += struct.pack("<I", len(b)) + b
    sys.stdout.buffer.write(bytes(out))
    print(f"tzdata {version}: {len(names)} names, {len(blobs)} blobs, "
          f"{len(out)} bytes", file=sys.stderr)


if __name__ == "__main__":
    main()
