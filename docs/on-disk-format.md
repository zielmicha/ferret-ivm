# On-disk format

> **Scope.** This document describes the formats the current code writes and
> reads: format version 2, and format 1 as far as it is read to migrate it
> (see [Compatibility](#compatibility) for what changes the version). A
> change to any of these files changes a format described here and must be
> reflected in this document (paths relative to `crates/ivm/src/` unless
> noted): `log.rs`, `storage/`, `types/codec.rs`, `types/encoding.rs`,
> `dataflow/layout.rs`, `dataflow/mod.rs` and the operators' `fingerprint`
> methods (structural fingerprints), `expr/mod.rs` (expression
> fingerprints), the function registry order (`expr/ops.rs`,
> `expr/builtins.rs`), `catalog.rs`, `coord/meta.rs` (the metadata tree, the
> checkpoint header, `FORMAT`, format 1's `AppState`), `coord/ddl.rs`
> (`DiscoveredTable`, the `CREATE SOURCE` record's `extra`), `source/`
> (`SourceState`, `SlotDrop`), `crates/pgnumeric/src/encoding.rs` and
> `crates/pgfuncs/src/json/keyenc.rs`.

Every statement below was checked against the code and, where indicated, by
decoding files written by a running engine.

## Contents

1. [Conventions](#conventions)
2. [Data directory layout](#data-directory-layout)
3. [The LOCK file](#the-lock-file)
4. [The FORMAT file](#the-format-file)
5. [Input log](#input-log)
6. [Storage segments and nodes](#storage-segments-and-nodes)
7. [Manifest and checkpoints](#manifest-and-checkpoints)
8. [The metadata tree](#the-metadata-tree)
9. [Format 1 application state (AppState)](#format-1-application-state-appstate)
10. [Data encodings](#data-encodings)
11. [Structural fingerprints](#structural-fingerprints)
12. [Garbage collection and retention](#garbage-collection-and-retention)
13. [Compatibility](#compatibility)

## Conventions

| Notation | Meaning |
|---|---|
| `u8`, `u32 LE`, `u64 LE`, `i128 LE` | fixed-width integers, little-endian (`BE`: big-endian) |
| `uvarint` | unsigned LEB128: 7 bits per byte, least significant group first, high bit set on every byte but the last; at most 10 bytes for a `u64` (`codec::put_uvarint`). Decoding fails on truncation or when the shift reaches 64 |
| `ivarint` | signed: zigzag `(v << 1) ^ (v >> 63)`, then `uvarint` |
| `bytes` | `uvarint` length, then that many bytes |
| `str` | `bytes` holding UTF-8 (decoding validates UTF-8) |
| CRC-32 | the IEEE 802.3 / zlib CRC-32 (`crc32fast`) |
| GSN | global sequence number, `u64`, first GSN is 1 |
| `ObjectId` | catalog object id, `u64`, first id is 1000 |
| PostgreSQL epoch | 2000-01-01 00:00:00 UTC; date/time values count days or microseconds from it |

## Data directory layout

```
<data-dir>/
├── FORMAT
├── FORMAT.new                  (transient)
├── LOCK
├── log/
│   └── log-<first GSN, %020u>.ivl ...
└── store/
    ├── MANIFEST
    ├── MANIFEST.new            (transient)
    └── seg-<segment id, %08u>.dat ...
```

| Path | Written by | Contents |
|---|---|---|
| `FORMAT` | `coord::start` | the format version, ASCII decimal and a newline |
| `FORMAT.new` | `coord::start` | the new `FORMAT`, until renamed |
| `LOCK` | `coord::start` | empty; exclusive lock |
| `log/log-NNNNNNNNNNNNNNNNNNNN.ivl` | sequencer (`log::LogWriter`) | input-log segment; the number is the GSN of its first record, zero-padded to 20 digits |
| `store/MANIFEST` | checkpoints (`storage::manifest`) | sequence of checkpoint records |
| `store/MANIFEST.new` | manifest rewrite | the single record of a rewritten manifest, until renamed |
| `store/seg-NNNNNNNN.dat` | checkpoints (`storage::segment`) | B+tree node frames; the number is the segment id (`u32`, from 1), zero-padded to 8 digits |

Directories are created on demand; creating `store/` (and `log/`, unless
`--no-sync`) is followed by an `fsync` of the data directory, so that their
entries survive a crash. Files in `log/` and `store/` whose names do not
match these patterns are ignored. Example of a fresh directory after one
second: `FORMAT` (2 bytes, `2\n`), `LOCK` (0 bytes),
`log/log-00000000000000000001.ivl` (one tick frame per second, 25 bytes
each), `store/MANIFEST` (0 bytes until the first checkpoint),
`store/seg-00000001.dat` (0 bytes).

## The LOCK file

`LOCK` is opened (created if missing, never truncated or written) and locked
with `File::try_lock` (an exclusive, non-blocking `flock`). The handle is kept
open for the life of the process (`coord::Running::_lock`), so the lock is
released by the kernel when the process exits for any reason. A lock that is
taken is retried every 50 ms for up to 2 seconds (a process forked while the
lock was held keeps a copy of it until it execs); if it is still taken,
start-up fails with SQLSTATE `55006`
(`data directory <dir> is in use by another ivm process`). The file is never
deleted and its contents are never read.

## The FORMAT file

Code: `coord::meta` (`read_format`, `write_format`), `coord::start`.

`FORMAT` holds the data directory's format version: the version in ASCII
decimal followed by a newline (`2\n`). It is read right after the lock is
taken, before anything else is opened (opening the log truncates a torn
tail, opening the store deletes unreferenced segment files):

| `FORMAT` | Meaning | Start-up |
|---|---|---|
| missing, `log/` and `store/` empty or absent | a new directory | `FORMAT` is written first |
| missing, `log/` or `store/` holds a file | format 1, written before versions were recorded | recovered, then migrated: see [Compatibility](#compatibility) |
| `2` | the current format | normal |
| a larger number | written by a newer engine | refused with SQLSTATE `0A000` (`the data directory has format <n>, which this version of ivm cannot read (it reads formats up to 2)`), nothing is modified |
| anything else | damaged | refused with `XX001` (`.../FORMAT does not hold a format version: ...`) |

`FORMAT` is written atomically: to `FORMAT.new`, which is `fsync`ed and
renamed over `FORMAT`, followed by an `fsync` of the data directory. A
leftover `FORMAT.new` is never read.

## Input log

Code: `crates/ivm/src/log.rs`.

### Segments and rotation

- A new log starts with `log-00000000000000000001.ivl`.
- Records are appended by the sequencer in batches: each record is encoded
  into an in-memory buffer as a frame, the whole batch is written with one
  `write_all`, and then the file is `fdatasync`ed (`sync_data`) unless
  `--no-sync` is set.
- **Rotation**: when a record is appended while the buffer is empty (i.e. the
  first record of a batch) and the current segment holds at least 64 MiB
  (67,108,864 bytes), the current segment is `fsync`ed, a new segment named
  after that record's GSN is created, and the directory is `fsync`ed (also
  with `--no-sync`). A segment can therefore exceed 64 MiB by up to one
  batch, and a segment boundary always falls between batches. The first
  segment of a new log is followed by an `fsync` of `log/` unless
  `--no-sync` is set.
- GSNs are contiguous: each frame's GSN is the previous frame's plus one,
  across segment boundaries. The one exception: when the log ends before the
  checkpoint's GSN at start-up (an operating-system crash can lose the
  unsynced tail of a `--no-sync` log, while checkpoints are always synced),
  numbering continues after the checkpoint (`LogWriter::skip_to`), leaving a
  gap below the checkpoint that replay never reads.

### Frame

```
offset  size  field
0       4     len      u32 LE   payload length
4       4     crc      u32 LE   CRC-32 over (gsn as 8 bytes LE) ++ payload
8       8     gsn      u64 LE
16      len   payload  LogRecord
```

The CRC covers the GSN, so a frame moved to another position fails its
check. A tick frame from a running engine:

```
09 00 00 00                 len = 9
4f 00 25 55                 crc
01 00 00 00 00 00 00 00     gsn = 1
05                          tag 5 = Tick
98 84 cc d7 96 aa af 06     ivarint 1792928552419596 (Unix µs, 2026-10-25 11:42:32.419596)
```

### LogRecord

The first payload byte is the record tag (`LogRecord::encode` / `decode`):

| Tag | Record | Fields after the tag |
|---|---|---|
| 1 | `Commit` | `uvarint source`, `uvarint lsn`, `uvarint n`, then `n` changes |
| 2 | `Ddl` | `uvarint req`, `str sql`, `str extra` |
| 3 | `Progress` | `uvarint source`, `uvarint lsn`, `str state` |
| 4 | `Schema` | `uvarint source`, `uvarint table`, `bytes columns_json`, `uvarint k`, `k × uvarint pk_column` |
| 5 | `Tick` | `ivarint micros` |

Any other tag is a decoding error (`bad log record tag`).

**Commit** — one upstream transaction (a snapshot chunk's rows are added to
the commit that carries its watermark), one local DML statement, or the
truncation a source logs before re-copying a table.

- `source`: the source's `ObjectId`, or `0` for local tables written with SQL.
- `lsn`: for a source, the end LSN of the upstream transaction as a `u64`
  (`X/Y` is `(X << 32) | Y`); for local DML, the engine's request id (unrelated
  to LSNs; used to route the reply).
- each change: `uvarint table` (the table's `ObjectId`), `u8 kind`, payload:

| Kind | Change | Payload | Meaning |
|---|---|---|---|
| 1 | `Upsert` | `row`, `uvarint k`, `k × uvarint column` | set the row of its key; the listed columns (unchanged TOASTed values) are taken from the key's current row or, if the key has none, from the row deleted by the change directly before (a key change is a `Delete` of the old key followed by the `Upsert` of the new row), else NULL |
| 2 | `Delete` | `row` (key values in key order) | delete by key |
| 3 | `Insert` | `row` | add one instance (multiset tables) |
| 4 | `Remove` | `row` | remove one instance (multiset tables) |
| 5 | `Truncate` | — | remove all rows, including those the commit changed before it |

Changes apply in order. Tables with a key receive `Upsert`, `Delete` and
`Truncate` (an `Insert` or `Remove` there sets or deletes the row's key);
multiset tables receive `Insert`, `Remove` and `Truncate` (an `Upsert` or
`Delete` there adds or removes one instance of the row). The DBLog snapshot
of a multiset table is logged as `Truncate` followed by an `Insert` per row.

`row` is a row in the [row codec](#row-codec). Example (local
`INSERT INTO customers VALUES (1, 'eu'), (2, 'us')`, table id 1001):

```
1a 00 00 00  2f 09 98 ae  32 00 00 00 00 00 00 00     len 26, crc, gsn 50
01                                                     Commit
00                                                     source 0 (local)
04                                                     lsn = request id 4
02                                                     2 changes
e9 07  01  02  04 02  09 02 65 75  00                  table 1001, Upsert, row(int4 1, text 'eu'), 0 unchanged
e9 07  01  02  04 04  09 02 75 73  00                  table 1001, Upsert, row(int4 2, text 'us'), 0 unchanged
```

and `DELETE FROM orders WHERE id = 4` (table 1000):
`01 00 0c 01 e8 07 02 01 04 08` — Commit, source 0, request 12, one change,
table 1000, `Delete`, key row with one value, int4 4.

**Ddl** — a catalog change.

- `req`: the request id of the submitting session (meaningless on replay).
- `sql`: the canonical statement text. Standard statements are printed by
  `sqlparser` (`CREATE TABLE t (id INT PRIMARY KEY, v TEXT)`); the engine's own
  statements use the forms produced by `coord::ddl::statement_sql`:
  `CREATE SOURCE <name> FROM POSTGRES CONNECTION '<conninfo>' PUBLICATION '<pub>' [SLOT '<slot>'] [FOR TABLES (<t>, ...)]`,
  `ALTER SOURCE <name> REFRESH`, `DROP SOURCE [IF EXISTS] <name>`,
  `CREATE SINK <name> FROM <relation> INTO POSTGRES CONNECTION '<conninfo>' TABLE '<table>' [KEY (<c>, ...)]`,
  `DROP SINK [IF EXISTS] <name>` (names double-quoted unless they consist of
  lowercase letters, digits and `_`; single quotes doubled).
- `extra`: empty, except for the statements that discover upstream tables.
  A discovered table is
  `{"schema": str, "name": str, "columns": [Column], "pk": [column index, ...], "replica_identity_full": bool}`
  (`coord::ddl::DiscoveredTable`; `Column` as in [catalog objects](#catalog-objects);
  `pk` is the key: the columns of the replica identity index under
  `REPLICA IDENTITY USING INDEX`, else the primary key's, in key order).
  `ALTER SOURCE ... REFRESH` logs a JSON array of all discovered tables (the
  engine adds the new ones). `CREATE SOURCE` logs an object
  `{"tables": [DiscoveredTable, ...], "instance": str}` whose `instance` is
  the data directory's instance id (`catalog.instance_id`), so that replay
  gives a source without `SLOT` the same default slot name
  `ivm_<name>_<instance>_<id>`; applying the record stores the id in the
  catalog if it has none. A plain array in a `CREATE SOURCE` record (written
  by versions without instance ids) means a default name without it,
  `ivm_<name>_<id>`.

**Progress** — a source's bookkeeping without data.

- `lsn`: the end LSN of the commit after which it was logged (informational;
  not used on replay).
- `state`: JSON `SourceState` (`source::postgres`):
  `{"slot_created": bool, "tables": {"<table id>": {"done": bool, "boundary": null | [str, ...], "truncate": bool}}}`
  where `boundary` holds the text values of the last key copied by the
  table's snapshot, and `truncate` (absent: `false`) says that the table's
  next snapshot chunk starts by logging a commit that truncates it (set for
  tables with a key after a key change upstream or a lost replication
  slot). The whole state replaces the previous one; a state that does not
  parse is taken as empty (the slot counts as not created and every table
  is copied again). Progress records of a source that is no longer in the
  catalog are ignored.

**Schema** — an upstream table changed shape or key: the source `ObjectId`,
the table `ObjectId`, the new columns as a JSON array of `Column` (key
columns `NOT NULL`), and the new key as column indexes (the replica
identity's columns, as for `pk` above). Applying it also resets the table's
entry in the source's stored state to `{"done": false, "boundary": null}`,
keeping its `truncate` flag (which the source's `Progress` record logged just
before sets after a key change), so that the table is copied again, from an
empty table after a key change, even if the engine restarts before the
copy starts.

**Tick** — `micros` is the wall clock in microseconds since the Unix epoch.
The engine converts it to the PostgreSQL epoch (subtracting
946,684,800,000,000) and keeps the maximum.

### Opening, replay and truncation

- **Open** (`LogWriter::open`): only the newest segment is read. Frames are
  parsed from offset 0 while a full 16-byte header is present, the payload is
  complete, the CRC matches and the payload decodes. If any bytes follow the
  last good frame, the file is truncated there and `fsync`ed (logged as
  `truncating torn log tail`). The last GSN is the last good frame's; if the
  newest segment has none, it is the last frame of the previous segment, or
  the segment's first GSN − 1 if there is no previous segment. The next GSN is
  the last GSN + 1 (or the checkpoint's GSN + 1 if that is later, see above),
  and appends continue in the newest segment.
- **Replay** (`read_from(after)`, `after` = the checkpoint's GSN, 0 without
  a checkpoint): segments
  are visited in GSN order; a segment is skipped if the next segment starts
  at or before `after + 1`. Each remaining segment is parsed with the same
  rules and its records with `gsn > after` are returned. Only the newest
  segment may end in a torn write (already truncated by the open): a frame
  that fails to parse in any earlier segment stops the start with a
  data-corrupted error (`input log segment <path> is corrupt at byte <n>`),
  and so do returned GSNs that are not consecutive from `after + 1`
  (`input log is missing records: expected gsn <g>, found <h> in <path>`),
  rather than replaying around the damage.
- **Truncation** (`truncate_before(g)`, after every checkpoint with
  `g` = the checkpoint's GSN): every segment except the current one whose
  successor starts at or before `g + 1` (so all its records are ≤ `g`) is
  deleted. Recovery replays the log after the checkpoint, and nothing else
  reads the log, so the records a checkpoint covers are not kept (`AS OF`
  reads use the histories in the trees, not the log).
- The engine ignores records whose GSN is at or below the last GSN it has
  applied.

## Storage segments and nodes

Code: `crates/ivm/src/storage/segment.rs`, `node.rs`, `summary.rs`.

### Segment files

A segment file is a concatenation of node frames:

```
offset  size  field
0       4     len      u32 LE   payload length
4       4     crc      u32 LE   CRC-32 of the payload
8       len   payload  encoded node
```

A node's location (`DiskLoc`) is `(seg, off, len)`: the segment id, the
offset of the frame header, and the frame length **including** the 8-byte
header. Reading a node reads exactly `len` bytes at `off` and checks that
`frame.len + 8 == len` and the CRC (`node frame length mismatch`,
`node checksum mismatch`, SQLSTATE `XX001`).

- Segments are written only by checkpoints, append-only, through a buffer
  that is written out when it exceeds 4 MiB and at the checkpoint's sync.
- Before each append, if the active segment holds at least 256 MiB
  (268,435,456 bytes), it is sealed and segment `id + 1` is created (an
  existing file of that name is truncated) and the store directory
  `fsync`ed. Sealed segments are never modified; they are `fsync`ed at the
  next sync and deleted whole once no live node remains in them.
- On open, the active segment named by the last checkpoint (segment 1 for a
  new store) is opened, created if missing (then the store directory is
  `fsync`ed), and resized to the checkpoint's `active_len` (0 for a new
  store) — discarding nodes written by an interrupted checkpoint. Every
  other `seg-*.dat` that the checkpoint's segment table does not list is
  deleted.

### Leaf node

```
u8       kind = 1
uvarint  n                         number of entries
n × entry:
  uvarint  shared                  length of the prefix shared with the previous entry's key (0 for the first)
  bytes    suffix                  the rest of the key
  uvarint  h                       number of history elements
  h × (uvarint gsn_delta, ivarint weight)
                                   gsn = previous element's gsn + gsn_delta (the first delta is from 0)
```

Keys are unique, in ascending `memcmp` order; history elements are in
ascending GSN order with non-zero weights. Example — the four entries of a
table `customers (id int PRIMARY KEY, region text)` (decoded from a running
engine; key = `02` + int4 key + payload, see
[Arrangement entries](#arrangement-entries)):

```
2f 00 00 00  e0 4d a2 e2           frame: len 47, crc
01  04                             leaf, 4 entries
00  09 02 80 00 00 01 09 02 45 55  01 73 02
                                   key (1, 'EU'); history [(115, +1)]
07  02 65 75  02 32 02 41 01       shares 7 bytes; key (1, 'eu'); history [(50, +1), (115, −1)]
04  05 02 09 02 75 73  01 32 02    shares 4 bytes; key (2, 'us'); [(50, +1)]
04  07 03 09 04 61 70 61 63  01 4c 02
                                   shares 4 bytes; key (3, 'apac'); [(76, +1)]
```

(The update of row 1 from `'eu'` to `'EU'` at GSN 115 left two entries:
retention had not yet consolidated the old version.)

### Internal node

```
u8       kind = 3
uvarint  n                         number of children (at least 1)
n × child:
  bytes    separator               lower bound of the child's key range (stored but ignored for the first child)
  uvarint  seg, uvarint off, uvarint len
                                   DiskLoc of the child's frame
  uvarint  min_seg                 oldest segment id holding any node of the child's subtree (4294967295 = none)
  uvarint  due + 1                 compaction state of the child's subtree (below); 0 = none
  summary                          the child subtree's summary (below)
```

A key belongs to the last child whose separator is `<=` the key. Separators
are shortened at leaf splits: the separator of a new leaf is the shortest
prefix of its first key that is greater than the previous leaf's last key.

`due` is the smallest retention frontier at which compacting some leaf of
the subtree is worth rewriting it, the minimum over its leaves. A leaf's
`due` (`node::leaf_due`) is the smallest frontier *f* at which compacting
every entry's history at *f* (as described in
[Garbage collection and retention](#garbage-collection-and-retention))
removes at least a quarter of the leaf's estimated size: `8 + Σ (key length
+ 4 + 6 × history elements)` over its entries, an entry that compaction
empties counting all of its size. "None" (`due + 1 = 0`, the value
2^64 − 1 wrapped) means that no frontier would. The value of each child is
computed from the child's contents when the node is written.

Internal nodes of kind 2, written by earlier versions, are still read: the
same layout without the `due` field. Their children's `due` reads as 0, so
that the next maintenance pass visits every subtree under them once and
writes the nodes on the way again as kind 3.

### Summary

```
ivarint  count                     sum of the current (total) weights of all entries in the subtree
uvarint  m                         number of aggregate states
m × aggstate
```

| Tag | `AggState` | Payload |
|---|---|---|
| 1 | `Count` | `ivarint` |
| 2 | `SumInt` | 16 bytes, `i128 LE` (two's complement) |
| 4 | `SumNumeric` | finite sum as a [compact numeric](#compact-numeric-encoding) (not limited to numeric's range, see below); `uvarint s`, `s × (uvarint display_scale, ivarint count)`; `ivarint nan`; `ivarint pos_inf`; `ivarint neg_inf` |
| 5 | `SumFloat` | `u8 sign` (0 = zero, 1 = positive, 2 = negative), `bytes magnitude` (little-endian), `ivarint nan`, `ivarint pos_inf`, `ivarint neg_inf`; finite value = sign × magnitude × 2^−1074 |
| 6 | `Min` | one [codec datum](#row-codec); NULL (tag 0) means "no value" |
| 7 | `Max` | same |

Tag 3 is unused. The aggregate states are positional: their meaning (which
`AggKind` and which argument expression) comes from the arrangement's layout,
which is derived from the catalog when the dataflow is planned; it is not
stored in the tree.

The finite part of a `SumNumeric` state is the exact sum of its subtree's
terms (the arguments, or for `SumSqNumeric` their squares, times their
weights), kept like PostgreSQL's numeric aggregate state without numeric's
range limit: a sum over a subtree, or of squares of large values, may exceed
the range of `numeric` (131,072 digits before the decimal point, a
base-10000 weight of at most 32,767) even when every value read from it
fits. The compact encoding represents such values unchanged; summary states
are decoded without the range check (`Numeric::decode_binary_unbounded`,
which still requires |weight| ≤ 2^30), and only the aggregate value computed
from a state is range-checked (an out-of-range group becomes an error record
of that group).

## Manifest and checkpoints

Code: `crates/ivm/src/storage/manifest.rs`, `Env::checkpoint`.

### Framing

`store/MANIFEST` is a sequence of frames:

```
offset  size  field
0       4     len      u32 LE
4       4     crc      u32 LE   CRC-32 of the payload
8       len   payload  checkpoint record
```

### Checkpoint record

```
uvarint  seq                     checkpoint sequence number (1, 2, ...; continued across restarts)
uvarint  gsn                     every log record with GSN <= gsn is reflected
uvarint  nt                      number of trees
nt × tree:
  uvarint  id                    tree id
  u8       has_root              0 = empty tree; any other value = a root follows
  [ uvarint seg, uvarint off, uvarint len     root DiskLoc
    summary                                    root summary
    uvarint  min_seg ]
uvarint  ns                      number of segments
ns × (uvarint id, uvarint total_bytes, uvarint live_bytes)
uvarint  active_seg              id of the active segment
uvarint  active_len              length of the active segment belonging to this checkpoint
uvarint  next_tree_id            next tree id to allocate
bytes    app                     application state: the header below (format 1: JSON, see AppState below)
[ uvarint  nt                    tree space: the number of trees again, then for each
                                 tree, in the order of the trees above:
  nt × (uvarint due + 1          the root's due (as in an internal node; 0 = none)
        uvarint ns
        ns × (uvarint seg, uvarint live_bytes)) ]
                                 bytes of the tree's node frames still live, per segment
                                 (segments with none are left out)
```

The application state of the current format is a header naming the
[metadata tree](#the-metadata-tree), which holds the catalog and the rest of
the engine's state:

```
uvarint  format                  2 (format 1's JSON starts with '{', 0x7B)
uvarint  meta_tree               id of the metadata tree (one of the trees above)
uvarint  next_req                request counter
```

The tree space section follows `app` to the end of the record. Records of
format 1 end after `app`; reading one, the engine counts each tree's live
bytes per segment from its internal nodes (the leaves' frame lengths are in
their parents' pointers, so no leaf is read; every leaf is at the depth of
the leftmost one) and takes every root's `due` as 0.

Every record is complete: it lists every tree (the metadata tree included),
every segment and every tree's live bytes per segment, so only the last
record matters. The first record of a new data directory, ten seconds after
`CREATE TABLE customers (id int PRIMARY KEY, region text)`, `INSERT INTO
customers VALUES (1, 'eu'), (2, 'us'), (3, 'x')` and `CREATE MATERIALIZED
VIEW c AS SELECT id, region::int AS r FROM customers` (three error records,
no rows):

```
30 00 00 00  f6 fe 2c ac     len 48, crc
01                           seq 1
0e                           gsn 14
03                           three trees
01  01  01 00 2b  06 00  01  tree 1 (customers): root at segment 1, offset 0, 43 bytes; count 3, no aggregates; min_seg 1
02  01  01 2b b5 09  2a 00  01
                             tree 2 (the metadata tree): root at offset 43, 1205 bytes; 21 entries
03  00                       tree 3 (c): empty
01  01 e0 09 e0 09           one segment: id 1, 1248 bytes written, 1248 live
01                           active segment 1
e0 09                        active length 1248
04                           next tree id 4
03  02 02 04                 app: format 2, metadata tree 2, next request 4
03                           tree space of three trees:
00  01  01 2b                  tree 1: due none; 43 bytes live in segment 1
00  01  01 b5 09               tree 2: due none; 1205 bytes live in segment 1
00  00                         tree 3: due none; no live bytes
```

A checkpoint after changes to some rows of one tree has the same size; the
record grows only with the number of trees and segments (and of the
segments each tree has nodes in).

### Reading and updating

- **Open**: frames are parsed from the start; parsing stops at the first frame
  that is incomplete or fails its CRC, and the file is truncated to the end of
  the last valid frame (without `fsync`). The last valid record is the
  recovered checkpoint; a record that passes its CRC but does not decode
  aborts start-up. A non-empty manifest without any valid record, and a
  missing manifest while segment files hold data, also abort start-up,
  modifying nothing (opening would otherwise delete the segments). A new
  manifest is created empty and the directory `fsync`ed.
- **Append** (the checkpoint's commit point): the frame is written at the end
  of the file with a positioned write and the file is `fdatasync`ed. Before
  that, all nodes the record refers to have been written and `fsync`ed.
- **Rewrite**: if the append would grow the file beyond 8 MiB
  (8,388,608 bytes), the new frame is written alone to `MANIFEST.new`, which
  is `fsync`ed and renamed over `MANIFEST`; then the directory is `fsync`ed.
  Readers either see the old file or the new one. (With records of tens of
  bytes per tree this takes many thousands of checkpoints; format 1's
  records carried the whole engine state and could reach it every few.)

### Checkpoint procedure

0. The engine writes the metadata changed since the previous checkpoint into
   the [metadata tree](#the-metadata-tree) (`Engine::persist_meta`), then
   runs a maintenance pass, which marks the nodes it rewrites dirty (see
   [Garbage collection and retention](#garbage-collection-and-retention)).
1. Every tree with a dirty root is written depth-first, children before their
   parent, so that each internal node records the final locations (and
   `min_seg` and `due`) of its children; each written frame adds its length
   to its segment's `total_bytes` and `live_bytes`, and to its tree's live
   bytes in that segment.
2. The active segment and any segments sealed since the last checkpoint are
   `fsync`ed.
3. The checkpoint record is appended to the manifest.
4. Sealed segments whose `live_bytes` is 0 are deleted.

Creating a segment file (the first one, and each new active segment) is
followed by an `fsync` of the store directory, so that a checkpoint never
refers to a file whose directory entry a crash could lose.

## The metadata tree

Code: `crates/ivm/src/coord/meta.rs`.

The catalog, its DDL history and the engine's state live in one storage
tree, the *metadata tree*, named by the [checkpoint header](#checkpoint-record).
It is stored like an arrangement's tree (the same nodes; its summaries hold
the count only), and each record is one entry: a byte string whose first
byte is the record's tag, with a (GSN, weight) history. A record kept by
key — every record but the error records — has weight 1; changing it
retracts the old entry (weight −1) and adds the new one (+1) at the GSN of
the change, so the tree holds the metadata as of every GSN of the retention
window, as an arrangement holds its rows (versions at or before the
retention frontier are merged when a leaf is rewritten). An error record's
weight is its count.

| Tag | Record | Entry after the tag | Weight |
|---|---|---|---|
| 1 | catalog object | `u64 BE id`, then the [`CatalogObject`](#catalog-objects) as JSON (to the end of the entry) | 1 |
| 2 | catalog scalar | `u8 field`, then its value: field 1, the next object id (`uvarint`); field 2, the instance id (`str`; no record while there is none) | 1 |
| 3 | DDL history | `u64 BE gsn`, then the statement (UTF-8, to the end of the entry) | 1 |
| 4 | source state | `u64 BE source`, then its `SourceState` JSON (to the end of the entry), as in [`Progress` records](#logrecord) | 1 |
| 5 | source resume LSN | `u64 BE source`, `uvarint lsn` | 1 |
| 6 | arrangement | `bytes fingerprint`, `uvarint tree`, `uvarint since` | 1 |
| 7 | node error record | `bytes node fingerprint`, `str sqlstate`, `str message` | its count |
| 8 | group error | `bytes node fingerprint`, `bytes group key`, `str sqlstate`, `str message` | 1 |
| 9 | clock tick | `u64 BE gsn`, `ivarint clock` | 1 |
| 10 | pending slot drop | `str conninfo`, `str slot` | 1 |

The *key* of a record kept by key is its entry up to its value: tag and
id for tags 1, 3, 4, 5 and 9, tag and field for 2, tag and fingerprint for
6, the whole entry for 10. Fixed-width big-endian ids and length-prefixed
fields make keys prefix-free, so the entries of one key are found by a
prefix scan.

| Record | Contents |
|---|---|
| catalog objects | every object of the catalog, by id |
| DDL history | `ivm.catalog_history`: the successfully applied DDL statements, by GSN, the most recent 10,000; when a statement makes the history longer than 11,000, the oldest are retracted down to 10,000. Trimming happens when a statement is applied, so replay trims identically |
| catalog scalars | the next object id; the instance id: 8 hexadecimal digits, the data directory's random id in default slot names, chosen when the first `CREATE SOURCE` is submitted |
| source states, resume LSNs | per source: its `SourceState`; the end LSN of its last logged commit, from which it resumes streaming |
| arrangements | every hydrated arrangement (also private state arrangements, such as a temporal filter's events): its [structural fingerprint](#structural-fingerprints), its tree, and the GSN from which its history is complete (when it was hydrated, or created with its table): `AS OF` reads of earlier GSNs do not use it |
| node error records | the error records in the contents of every arranged node, by node fingerprint, so that views created later over the node start with them (an arrangement holds rows only); a materialized view's error records are those of its root node |
| group errors | for every `Reduce` and `Recursive` node, the groups in error with their error record, the group key being the canonical [key encoding](#order-preserving-key-encoding) of the group's key values (empty for an aggregate without `GROUP BY` and for a recursive query), so that the error is retracted when the group recovers |
| clock ticks | `(gsn, clock)` of the clock ticks of the retention window, for `now()` in reads as of earlier GSNs: the last tick at or before the retention frontier and every later one. `clock` is microseconds since the PostgreSQL epoch; the logical clock is the last tick's (0 without ticks) |
| pending slot drops | replication slots of dropped sources not dropped upstream yet, retried after the start |

**Writing** (`Engine::persist_meta`): after every applied DDL or `Schema`
record, at its GSN (catalog objects and history entries are thus stamped
with the GSN of their change), and at every checkpoint, at the checkpoint's
GSN, before its nodes are written. Each time only what changed since the
previous time is written: the catalog objects inserted, replaced or removed
(the stored entries of each are retracted, whatever their bytes); history
entries and ticks after the last ones written, and those trimmed or behind
the frontier (retracted by a range scan); the records kept by key whose
entry differs from the one the tree holds; and the changes to the error
state (`Graph::take_error_changes`: a node's error records are added when
it is first seen with an arrangement and retracted when it loses its last
one or leaves the graph; an operator's group errors when it is first seen
and when it leaves).

**Reading** (recovery, `meta::load`): every entry with a non-zero weight.
An entry with an unknown tag or trailing bytes, a record kept by key whose
weight is not 1 or whose key appears twice, a catalog object whose key is
not its `id`, and a tree without the next object id are refused
(`bad metadata entry (...)` with the entry's bytes, SQLSTATE `XX001`).

Entries of the data directory of the [example record](#checkpoint-record),
decoded:

```
01 00 00 00 00 00 00 03 e8  7b 22 69 64 22 3a 31 30 30 30 ...
                                   catalog object 1000: {"id":1000,"name":"customers","kind":{"Table":...
02 01  ea 07                       next object id 1002 (history [(5, +1)]; the entry 02 01 e9 07,
                                   1001, has [(3, +1), (5, −1)] until the frontier passes GSN 5)
03 00 00 00 00 00 00 00 03  43 52 45 41 54 45 ...
                                   history: GSN 3, CREATE TABLE customers (id INT PRIMARY KEY, region TEXT)
06 10 b7 47 23 59 ... 1f 56  01  03
                                   arrangement b7472359...ae511f56 (customers' primary index): tree 1, since GSN 3
07 10 c3 bf 44 d5 ... 2d ef 75  05 32 32 50 30 32  2a 69 6e 76 61 6c 69 64 ...
                                   node c3bf44d5...2def75: 22P02, invalid input syntax for type integer: "x"; weight 1
09 00 00 00 00 00 00 00 0e  94 d9 f7 d4 ad ec 80 03
                                   tick at GSN 14: 846286484862538 (2026-10-25 23:34:44.862538 UTC)
```

### Catalog objects

`CatalogObject`: `id`, `name`, `kind`, `created_gsn`, `create_sql` (the
logged statement; for replicated tables the text
`-- table <schema>.<table> replicated by source <id>`). `kind` is one of:

| Variant | Fields |
|---|---|
| `Table` | `columns: [Column]`, `pk: [column index]` (the key in key order: for a replicated table the replica identity index's columns, else the primary key's; empty: multiset), `source: id or null`, `upstream: [schema, table] or null`, `version: u32` (incremented by upstream schema changes) |
| `MaterializedView`, `View` | `sql` (the query text), `columns: [Column]` |
| `Index` | `on: id`, `columns: [column index]` |
| `Source` | `conninfo`, `publication`, `slot`, `tables: [name] or null` |
| `Sink` | `from: id`, `conninfo`, `table`, `key: [name] or null` |

`Column` is `{"name": str, "ty": {"ty": SqlType, "typmod": i32, "nullable": bool}}`.
`SqlType` is serialized by variant name: `"Bool"`, `"Int2"`, `"Int4"`,
`"Int8"`, `"Float4"`, `"Float8"`, `"Numeric"`, `"Text"`, `"Varchar"`,
`"Bpchar"`, `"Name"`, `"Bytea"`, `"Date"`, `"Time"`, `"Timestamp"`,
`"TimestampTz"`, `"Interval"`, `"Uuid"`, `"Json"`, `"Jsonb"`, `"JsonPath"`,
`"Oid"`, `"Unknown"`, and `{"Array": SqlType}`. Type modifiers follow
PostgreSQL (`numeric(p,s)`: `((p << 16) | s) + 4`; `varchar(n)`, `char(n)`:
`n + 4`; `time(p)`, `timestamp(p)`, `timestamptz(p)`: `p`; `interval`:
`(field range << 16) | precision`, with range `0x7FFF` for all fields and
precision `0xFFFF` for none; `-1`: none). An array column carries its
element type's modifier.

## Format 1 application state (AppState)

Data directories of format 1 (written before `FORMAT` existed) carry the
whole engine state in every checkpoint record's `app` field, as a JSON
object (`coord::meta::AppState`, `serde_json`). The current code reads it
to migrate the directory ([Compatibility](#compatibility)); it never writes
it.

```json
{
  "catalog": {
    "objects": {
      "1000": {
        "id": 1000,
        "name": "orders",
        "kind": {"Table": {"columns": [...], "pk": [0], "source": null, "upstream": null, "version": 0}},
        "created_gsn": 47,
        "create_sql": "CREATE TABLE orders (id INT PRIMARY KEY, ...)"
      }
    },
    "next_id": 1009,
    "history": [[47, "CREATE TABLE orders (...)"], ...],
    "instance_id": "3f9a1c2e"
  },
  "trees": [[[24, 205, 206, 130, 207, 117, 128, 118, 215, 229, 14, 19, 128, 103, 100, 230], 9], ...],
  "view_errors": [[1008, [[["22003", "value out of range: overflow"], 1]]], [1003, []]],
  "source_states": [[1000, "{\"slot_created\":true,\"tables\":{\"1001\":{\"done\":true,\"boundary\":null,\"truncate\":false}}}"]],
  "source_lsns": [[1000, 32216824]],
  "next_req": 24,
  "clock": 846243883442476,
  "errors": {
    "nodes": [[[211, 58, 133, 107, 251, 236, 109, 17, 110, 184, 217, 41, 100, 16, 214, 3], [[["22003", "value out of range: overflow"], 1]]], ...],
    "operators": [[[211, 58, 133, 107, 251, 236, 109, 17, 110, 184, 217, 41, 100, 16, 214, 3], [[[2, 97, 0, 0], ["22003", "value out of range: overflow"]]]]]
  },
  "arr_since": [[[24, 205, 206, 130, 207, 117, 128, 118, 215, 229, 14, 19, 128, 103, 100, 230], 47], ...],
  "ticks": [[1452, 846243881442476], [1453, 846243882442476], [1455, 846243883442476]],
  "slot_drops": [{"conninfo": "host=db dbname=app user=repl", "slot": "ivm_old_3f9a1c2e_1004"}]
}
```

| Field | Contents |
|---|---|
| `catalog.objects` | map from object id (as a JSON string key) to `CatalogObject` |
| `catalog.next_id` | next object id |
| `catalog.history` | `[gsn, statement]` of the DDL history, oldest first (as in the metadata tree) |
| `catalog.instance_id` | 8 hexadecimal digits, the data directory's random id in default slot names, chosen when the first `CREATE SOURCE` is submitted; `null` before that (and if absent) |
| `trees` | `[fingerprint, tree id]` for every hydrated arrangement; the fingerprint is 16 bytes as a JSON array of numbers ([Structural fingerprints](#structural-fingerprints)) |
| `view_errors` | per materialized view with a dataflow: its error records `[[sqlstate, message], count]` |
| `source_states` | per source: its `SourceState` JSON as a string (as in `Progress` records) |
| `source_lsns` | per source: the end LSN of its last logged commit (resume point); `[]` if absent |
| `next_req` | request counter |
| `clock` | logical clock, microseconds since the PostgreSQL epoch; `0` if absent |
| `errors` | error state of the dataflow, by node fingerprint. `nodes`: for every node that has an arrangement and error records, the error records in its contents (`[[sqlstate, message], count]`), so that views created later over the node start with them (an arrangement holds rows only). `operators`: for every `Reduce` and `Recursive` node with groups in error, `[group key, [sqlstate, message]]`, the group key being the canonical [key encoding](#order-preserving-key-encoding) of the group's key values (empty for an aggregate without `GROUP BY` and for a recursive query), so that the error is retracted when the group recovers. Restored by fingerprint before hydration; `{"nodes": [], "operators": []}` if absent (`operators` may also be absent alone) |
| `arr_since` | `[fingerprint, gsn]` for every hydrated arrangement: the GSN from which its history is complete (when it was hydrated, or created with its table). `AS OF` reads of earlier GSNs do not use the arrangement. `[]` if absent: recovered arrangements then count as complete from GSN 0 |
| `ticks` | `[gsn, clock]` of recent clock ticks, for `now()` in reads as of earlier GSNs: the last tick at or before the retention frontier and every later one, at most the last 100,000; `[]` if absent (recovery then starts from `[[checkpoint gsn, clock]]`) |
| `slot_drops` | replication slots of dropped sources not dropped upstream yet (`conninfo`, `slot`), retried after the start; `[]` if absent |

`view_errors` duplicates the error records of each view's root node in
`errors.nodes`; recovery gives a root node without records of its own
(checkpoints from before `errors` existed) its view's.

## Data encodings

### Order-preserving key encoding

Code: `crates/ivm/src/types/encoding.rs`. Used for the key part of every
arrangement entry. `memcmp` of two encodings orders like SQL comparison of
the values; every encoding is prefix-free, so the encodings of several key
columns concatenate into an order-preserving composite key; and equal values
encode identically (the encoding is *canonical*: `1.0` and `1.00`, `-0` and
`0`, `'1 mon'` and `'30 days'` all encode the same).

Each key column is encoded according to its sort specification
(`SortSpec { desc, nulls_first }`; the default is ascending with NULLs last,
`DESC` defaults to NULLs first):

| Value | Bytes |
|---|---|
| NULL | `0x01` if NULLs sort first, `0x03` if last (never inverted) |
| non-NULL | `0x02`, then the value bytes; for `DESC` every value byte is bitwise inverted (the `0x02` tag is not) |

Value bytes by type:

| Type | Encoding |
|---|---|
| `boolean` | 1 byte: `0x00` false, `0x01` true |
| `smallint` | 2 bytes BE of `(v as u16) ^ 0x8000` |
| `integer` | 4 bytes BE of `(v as u32) ^ 0x8000_0000` |
| `bigint` | 8 bytes BE of `(v as u64) ^ 0x8000_0000_0000_0000` |
| `oid` | 4 bytes BE (unsigned) |
| `real`, `double precision` | normalise `-0` to `+0` and every NaN to the canonical NaN; take the IEEE bits `b`; if the sign bit is set write `!b`, else `b` with the sign bit set; 4 / 8 bytes BE (NaN sorts above +Infinity) |
| `numeric` | [numeric key encoding](#numeric-key-encoding) |
| `text`, `varchar`, `char`, `name`, `json`, `jsonpath` | the UTF-8 bytes with every `0x00` written as `0x00 0xFF`, terminated by `0x00 0x00` |
| `bytea` | same escaping and terminator |
| `date` | 4 bytes BE of `(days as u32) ^ 0x8000_0000` |
| `time`, `timestamp`, `timestamptz` | 8 bytes BE of `(µs as u64) ^ 0x8000_0000_0000_0000` (time: µs since midnight; timestamps: µs since the PostgreSQL epoch, UTC) |
| `interval` | 16 bytes BE of `(span as u128) ^ (1 << 127)` with `span = (months × 30 + days) × 86,400,000,000 + µs` (PostgreSQL's comparison value) |
| `uuid` | the 16 bytes |
| `jsonb` | [jsonb key encoding](#jsonb-key-encoding) |
| arrays | for each element in row-major order: `0x03` for NULL, or `0x02` followed by the element's value bytes; then `0x00`; then `u8 ndims`; then `ndims × u32 BE` lengths; then `ndims × u32 BE` of `(lower bound as u32) ^ 0x8000_0000`. For `DESC` the whole value (elements, terminator, dimensions) is inverted together |

The decoder needs the column's type (from the arrangement's layout). An
`interval` decodes canonically from its span: with total days =
⌊span ÷ 86,400,000,000⌋, months = total days ÷ 30 (truncated) and days = the
remainder, both clamped to 32 bits, and µs = the rest of the span, so that
spans beyond what months and days can carry (the infinities, extreme field
values) keep exactly the encoded span and re-encode to the same bytes. The
exact fields come back from the fidelity suffix.

Example: `02 80 00 00 01` is the integer 1 ascending; `02 04 81 1f 96` is the
`numeric` 15.75 ascending.

#### Numeric key encoding

Code: `crates/pgnumeric/src/encoding.rs` (`encode_key`).

| Tag byte | Value |
|---|---|
| `0x01` | −Infinity |
| `0x02` | negative (followed by the inverted magnitude) |
| `0x03` | zero (any scale) |
| `0x04` | positive (followed by the magnitude) |
| `0x05` | +Infinity |
| `0x06` | NaN |

Magnitude, for value = 0.c₁c₂…cₙ × 100^E with base-100 digits cᵢ, c₁ ≠ 0 and
trailing zero digits removed (cₙ ≠ 0):

- exponent: if −64 ≤ E ≤ 63, one byte `0x40 + (E + 64)`; if E ≥ 64, `0xC0`
  followed by E as 3 bytes BE; if E ≤ −65, `0x3F` followed by `E + 2^23` as 3
  bytes BE;
- digits: every cᵢ as one byte `2cᵢ + 1`, except the last as `2cₙ` (the low
  bit marks continuation, which makes the encoding prefix-free).

For negative values every magnitude byte is inverted. Example: 15.75 =
0.1575 × 100¹ → `04` `81` `1f` (15·2+1) `96` (75·2).

#### jsonb key encoding

Code: `crates/pgfuncs/src/json/keyenc.rs`. Orders like PostgreSQL's jsonb
comparison (`compareJsonbContainers`):

```
top    := 0x01                          empty array (sorts first)
        | 0x02 scalar                   raw scalar
        | 0x03 count elem*              non-empty array
        | 0x04 count (string elem)*     object (keys in jsonb order)
elem   := scalar
        | 0x15 count elem*              nested array
        | 0x16 count (string elem)*     nested object
scalar := 0x10                          null
        | 0x11 string
        | 0x12 number
        | 0x13                          false
        | 0x14                          true
count  := u32 BE
string := UTF-8 bytes, 0x00 escaped as 0x00 0xFF, terminated by 0x00 0x01
number := 0x01 !mag (negative) | 0x02 (zero) | 0x03 mag (positive)
mag    := (exponent as u32 ^ 0x8000_0000, BE)
          (1 + 10·d1 + d2 for each pair of significant decimal digits)* 0x00
```

where value = 0.d₁d₂d₃… × 10^exponent with d₁ ≠ 0 and `!mag` inverts every
byte of `mag`.

### Fidelity suffix

The canonical key loses representation details that SQL equality ignores.
They are kept in a *fidelity suffix* written after **all** key columns of an
entry, so that key prefixes still group equal values together. For each key
column in order whose value is not NULL and whose type has fidelity
(`numeric`, `real`, `double precision`, `interval`, `jsonb`, and arrays of
those):

| Type | Fidelity bytes |
|---|---|
| `numeric` | `uvarint` display scale |
| `real`, `double precision` | 1 byte: `1` if the value is −0.0, else `0` |
| `interval` | `months i32 LE`, `days i32 LE`, `µs i64 LE` (16 bytes) |
| `jsonb` | `uvarint` length + the value's canonical text |
| arrays | the fidelity bytes of every non-NULL element, in order |

Lookups by key value (`Layout::encode_prefix`) encode only the canonical
part, so every representation of a value is found under one prefix. NaN
payloads are not preserved.

### Row codec

Code: `crates/ivm/src/types/codec.rs`. A compact, self-describing, exact
encoding (it preserves `numeric` scale, −0.0 and `interval` fields). Used for
log rows, arrangement payloads, `min`/`max` summary states, and constant rows
and literals in fingerprints.

```
row   := uvarint n, n × datum
datum := u8 tag, payload
```

| Tag | Type | Payload |
|---|---|---|
| 0 | NULL | — |
| 1 | `boolean` false | — |
| 2 | `boolean` true | — |
| 3 | `smallint` | `ivarint` |
| 4 | `integer` | `ivarint` |
| 5 | `bigint` | `ivarint` |
| 6 | `real` | 4 bytes LE (IEEE bits) |
| 7 | `double precision` | 8 bytes LE (IEEE bits) |
| 8 | `numeric` | [compact numeric](#compact-numeric-encoding) |
| 9 | `text`, `varchar`, `char`, `name`, `jsonpath` | `str` |
| 10 | `json` | `str` (the original text) |
| 11 | `bytea` | `bytes` |
| 12 | `date` | `ivarint` days since the PostgreSQL epoch |
| 13 | `time` | `ivarint` µs since midnight |
| 14 | `timestamp` | `ivarint` µs since the PostgreSQL epoch |
| 15 | `timestamptz` | `ivarint` µs since the PostgreSQL epoch (UTC) |
| 16 | `interval` | `ivarint months`, `ivarint days`, `ivarint µs` |
| 17 | `uuid` | 16 bytes |
| 18 | `jsonb` | `str` (canonical jsonb text; re-parsed on decode) |
| 19 | array | `type`, `uvarint ndims`, `ndims × (ivarint lower_bound, ivarint length)`, `uvarint n`, `n × datum` |
| 20 | `oid` | `uvarint` |

`type` (an array's element type) is `0xFF type` for an array type, otherwise
the type's OID as `uvarint` (OIDs as in PostgreSQL: 16 `bool`, 17 `bytea`,
19 `name`, 20 `int8`, 21 `int2`, 23 `int4`, 25 `text`, 26 `oid`, 114 `json`,
700 `float4`, 701 `float8`, 1042 `bpchar`, 1043 `varchar`, 1082 `date`,
1083 `time`, 1114 `timestamp`, 1184 `timestamptz`, 1186 `interval`,
1700 `numeric`, 2950 `uuid`, 3802 `jsonb`, 4072 `jsonpath`).

### Compact numeric encoding

Code: `crates/pgnumeric/src/encoding.rs` (`encode_binary`). Exact, preserves
the display scale:

```
u8       header      0xF0 NaN, 0xF1 +Infinity, 0xF2 −Infinity; otherwise
                     bit 6 (0x40) = negative, bits 0–5 = number of base-10000 digits nd
                     (63: the count follows as uvarint)
[uvarint nd]         if bits 0–5 are 63
uvarint  dscale      display scale
if nd > 0:
  ivarint weight     position of the first digit (value = Σ dᵢ × 10000^(weight − i))
  nd × u16 LE        base-10000 digits (each < 10000)
```

Example: `10.50` → `02 02 00 0a 00 88 13` (2 digits, scale 2, weight 0,
digits 10 and 5000); `0.5` with scale 1 → `01 01 01 88 13` (1 digit, scale 1,
weight −1, digit 5000).

### Arrangement entries

Code: `crates/ivm/src/dataflow/layout.rs`. An arrangement has a layout: key
expressions with sort specifications, the row schema, and summaries. The
tree key of a row is

```
entry := key(k₁) ++ … ++ key(kₘ)        canonical key encoding of each key expression's value
      ++ fidelity(k₁ … kₘ)              fidelity suffix of the non-NULL key values
      ++ datum(p₁) ++ … ++ datum(pⱼ)    codec datums of the payload columns
```

The payload columns are all row columns that are not a plain `Column` key
expression, in column order (without a count prefix). Decoding reads the key
values, applies the fidelity suffix, decodes the payload, and copies plain
key columns into the row; trailing bytes are an error. The tree value of an
entry is its history.

Examples (decoded from a running engine):

- primary index of `orders (id int PRIMARY KEY, customer int, amount numeric(10,2), created timestamptz)`:
  `02 80 00 00 01 | 04 02 | 08 02 02 00 0a 00 88 13 | 0f 80 a0 d9 b8 c1 e9 80 03`
  — key `id = 1`; payload `customer = 1`, `amount = 10.50`, `created`.
- multiset table `notes (k int, v text)`, keyed by all columns, no payload:
  `02 80 00 00 01 02 61 00 00` — (1, 'a'); `02 80 00 00 02 03` — (2, NULL).
- index on a `numeric` column of a materialized view (`CREATE INDEX ON revenue (total)`):
  `02 04 81 1f 96 | 02 | 09 02 65 75 | 05 04` — key 15.75, fidelity scale 2,
  payload `region = 'eu'`, `n = 2`.

Arrangements used by the engine:

| Arrangement | Key | Payload |
|---|---|---|
| table primary index | the table's key columns (`TableDef.pk`) in key order, ascending (all columns for a multiset table) | the other columns |
| `CREATE INDEX` | the listed columns, ascending | the other columns |
| join lookup index | the input node's join key columns (a key computed by the input's map is first materialized as an extra column of an `Mfp` node) | the input's columns that are not plain key columns |
| aggregation input | group columns, then the `ORDER BY` of an ordered aggregate | the other projected input columns; summaries per the aggregates |
| aggregation output, view read index, `Distinct`/`SetOp`/`Recursive` | group columns, or all columns | the rest |
| top-k and window input | group (partition) columns ascending, then the `ORDER BY` columns with their direction and NULL placement | the rest |
| temporal events | `(time bigint, kind smallint)` | the input row |

### Catalog serialisation

The catalog is stored in the [metadata tree](#the-metadata-tree): each object
as JSON, the history as statement text by GSN; DDL statements are
additionally logged as text in `Ddl` records.

## Structural fingerprints

Code: `crates/ivm/src/dataflow/mod.rs` (`hash128`, `node_fp`,
`add_arrangement`), the operators' `fingerprint` methods, `Mfp::fingerprint`,
`Expr::fingerprint`. Checkpoints identify arrangement trees by fingerprint
(the metadata tree's arrangement records); recovery finds them again by
re-planning the catalog and computing the same fingerprints.

**Hash**: 128-bit FNV-1a — state initialised to
`0x6c62272e07bb014262b821756295c58d`; for each byte, `state ^= byte;
state = state × 0x0000000001000000000000000000013B mod 2^128`; the result is
the state as 16 bytes little-endian.

**Node fingerprint** = hash of:

```
operator name (ASCII: Table, Constant, Mfp, Union, Negate, FlatMap, DeltaJoin,
               Reduce, Distinct, SetOp, TopK, Window, Recursive, TemporalFilter)
++ fingerprint of each input node (16 bytes each)
++ 0xFF
++ operator parameters
```

| Operator | Parameters |
|---|---|
| `Table` | object id `u64 LE`, table version `u32 LE`, Rust `Debug` text of the key column list `TableDef.pk` (e.g. `[0]`, `[0, 2]`, `[]`) |
| `Constant` | each row in the row codec |
| `Mfp` | input arity `u32 LE`; per step `0x01` + expression (map) or `0x02` + expression (filter); `0x03`; each projected column `u32 LE` |
| `Union` | one byte per input: 1 if negated |
| `Negate` | — |
| `FlatMap` | per function: `Debug` text `"<TableFunc>/<arity>"`, its argument expressions, `0xFD`; then the ordinality flag byte |
| `DeltaJoin` | per input: node fingerprint, MFP fingerprint, then for a presence input its arrangement fingerprint followed by `0xE0` if it matches exactly (by representation), otherwise `0x00`; per delta path: `0xA0`, source input `u32 LE`, source filter expressions; per step: `0xA1`, input `u32 LE`, arrangement fingerprint, key expressions, null-safe flag bytes, filter expressions, and `0xE1` if the lookup matches keys exactly |
| `Reduce` | `Debug` of the group-key column list; per aggregate `Debug` of (function, argument, second argument, filter, argument type, fraction) concatenated; input arrangement fingerprint |
| `Distinct` | arrangement fingerprint |
| `SetOp` | `Debug` text `"<op><all>"` (e.g. `Exceptfalse`), left and right arrangement fingerprints |
| `TopK` | `Debug` text of group key, limit, offset and with-ties concatenated; input arrangement fingerprint |
| `Window` | `Debug` of partition and order; input arrangement fingerprint; per function `Debug` of function, arguments, frame, range offsets |
| `Recursive` | `Debug` text of the base and step plans (`Rel`), distinct flag byte, per source object: id `u64 LE` + node fingerprint |
| `TemporalFilter` | per predicate: clock expression, operator byte (`<` 0, `<=` 1, `>` 2, `>=` 3), row expression |

**Expression fingerprint** (pre-order; each node writes a tag byte and a
`u32 LE` value, plus extras, then its children's fingerprints):

| Expression | Tag | Value / extras |
|---|---|---|
| column | 1 | column index |
| literal | 2 | 0; then the type (codec `type` encoding) and the value (codec datum) |
| function call | 3 | function id (`FuncId`, the index in the registry); then tag 0 with the argument count |
| cast | 4 | typmod (as `u32`); explicit flag byte; target type |
| `NOT` | 5 | 0 |
| `AND`, `OR` | 6, 7 | number of operands |
| `IS NULL`, `IS NOT NULL` | 8, 9 | 0 |
| `IS [NOT] TRUE/FALSE/UNKNOWN` | 10 | test kind (0–5) |
| `IS [NOT] DISTINCT FROM` | 11 | 1 for `NOT DISTINCT` |
| `CASE` | 12 | number of `WHEN`s |
| `COALESCE` | 13 | number of operands |
| `NULLIF` | 14 | 0 |
| `GREATEST`, `LEAST` | 15, 16 | number of operands |
| `IN (list)` | 17 | list length; negated flag byte |
| `LIKE` | 18 | escape character's code point (0 if none); case-insensitive, negated, bytea flag bytes |
| `ARRAY[...]` | 19 | number of elements; element type |
| subscript | 20 | 0 |
| `op ANY/ALL (array)` | 21 | function id; `ALL` flag byte |
| error | 22 | 0; SQLSTATE and message bytes |

(Tags 23–27 are binder placeholders that never reach a dataflow plan.)

**Arrangement fingerprint** = hash of

```
owning node's fingerprint
++ for each key expression: expression fingerprint, desc byte, nulls_first byte
++ 0xFE
++ for each summary: Debug name of the AggKind (e.g. "SumNumeric") ++ argument expression fingerprint
```

A private state arrangement (the temporal filter's events) uses the node
fingerprint ++ `"state"` ++ the key expressions with their sort bytes.

Example: the primary index of table 1000 (version 0, key `[0]`) has
node fingerprint `hash("Table" ++ FF ++ e8 03 00 00 00 00 00 00 ++ 00 00 00 00 ++ "[0]")`
and arrangement fingerprint
`hash(node_fp ++ 01 00 00 00 00 ++ 00 00 ++ FE)` = `b747235986039468879eec77ae511f56`
(recomputed independently from the description above and matching the
checkpoint of a running engine).

## Garbage collection and retention

- **Segment liveness**: the manifest's segment table records, per segment,
  the bytes written and the bytes still referenced by some tree, and its tree
  space section the same live bytes per tree. A node frame stops being live
  when its node is modified (dirty), dropped, or released with its tree.
- **Deletion**: after each checkpoint, sealed segments with zero live bytes
  are deleted. The active segment is never deleted or collected.
- **Compaction**: compacting a history at frontier *f* merges its elements
  at or before *f* into one stamped with the latest of their GSNs, dropped
  if their weights add up to 0; an entry left without elements is removed.
  An update compacts the entries it touches, and every entry of a leaf it
  rebuilds, at the current retention frontier (`processed GSN −
  --retention-gsns`).
- **Maintenance pass** (`Env::maintain`, before each checkpoint, which then
  writes what it rewrote): it rewrites, compacting them, the leaves whose
  `due` has passed the frontier (found through the subtrees' `due`) and
  compacts the leaves modified since the last checkpoint. It merges a node
  left smaller than a quarter of its target size (16 KiB) with a neighbour
  when both fit in three quarters of it. And it relocates the live nodes of
  the evacuated segments (chosen below): it marks them dirty (compacting
  leaves among them), found through the per-subtree `min_seg` values, the
  per-tree live bytes and the parents' locations of leaves (which it does
  not read unless they are rewritten). Reading and rewriting clean nodes is
  budgeted: for compaction and for relocation each, 8 MiB plus the bytes the
  checkpoints since the previous pass wrote for the engine's own updates.
- **Evacuated segments**: sealed segments with `live_bytes × 2 <
  total_bytes`, in decreasing order of `(1 − u) × age / (1 + u)` (`u` =
  live / total, `age` = active segment id − segment id), while their live
  bytes fit in the relocation budget (the first always does).
- **Dropping a tree** reads nothing: its loaded nodes are freed and its live
  bytes per segment subtracted from the segment table.
- **Unreferenced trees**: trees present in the checkpoint but not attached by
  any arrangement after recovery (arrangements that no longer exist, whose
  fingerprint changed, or of views and indexes that could not be built and
  are kept as broken) are dropped during start-up, after hydration.
- **Retention**: history elements at or before the retention frontier
  (`processed GSN − --retention-gsns`) are merged when a leaf is next
  modified or by a maintenance pass (in the metadata tree too, which merges
  away replaced records). Input-log segments whose records all lie at or
  before the checkpoint's GSN are deleted after the checkpoint, whatever the
  retention.

## Compatibility

### Format versions

A data directory records the format it is written in, in
[`FORMAT`](#the-format-file). Format 1 is everything written before the
version was recorded (no `FORMAT` file): the catalog and the engine state
as JSON in every checkpoint record. Format 2 keeps them in the
[metadata tree](#the-metadata-tree), with a header in the checkpoint record.

- **The version changes when existing data would not be read as it is**:
  something already written changes its meaning or layout (an existing tag
  or field reinterpreted, a structure replaced by another). Such a change
  raises `coord::meta::FORMAT_VERSION`, keeps reading the previous format,
  and migrates a directory of that format at its first start, as format 1
  is migrated (below).
- **Additive changes keep the version**: a new log record tag, change kind,
  datum tag, node kind, summary state tag or metadata record tag, a new
  JSON field with a default, data that existing code never reads. Code with
  the change reads everything written before it unchanged, because no
  existing tag or field changes its meaning. Data written after it may
  contain the new encodings, which code without the change does not know:
  it stops with a decoding error when it meets one (`bad log record tag`,
  `bad metadata entry (tag)`, `bad agg state tag`, ...), never misreading
  it — except that in the newest log segment a record it cannot decode is
  taken for a torn write and truncated with everything after it (see
  [Opening, replay and truncation](#opening-replay-and-truncation)). So an
  engine of a format version reads every directory of that version and of
  every earlier one, but running an older engine on a directory a newer one
  has written is not supported, within a format version as across.
- A directory of a newer format than the engine reads is refused before
  anything in it is opened.

### Migrating format 1

A data directory without `FORMAT` whose `log/` or `store/` holds a file is
of format 1. The engine starts it as usual — it recovers the last
checkpoint from its JSON state, rebuilds the dataflow and replays the log
after the checkpoint — then takes a checkpoint, which writes every record of
a new metadata tree, and then writes `FORMAT`. Until that checkpoint's
manifest append commits it, the format-1 record is the last valid one, so a
crash before it migrates again at the next start (the nodes it wrote are
beyond the recorded length of the active segment, and truncated); after it,
the next start finds a record of the current format — the application
state tells the formats apart: JSON starts with `{` (0x7B), the header with
its format number — and only writes `FORMAT`. The migration keeps
everything format 1 kept: the catalog and its history, the trees with
their history starts, the error records of nodes and the groups in error,
source states and resume LSNs, the request counter, the clock ticks and the
pending slot drops (a view's error records, which format 1 also kept with
the view, are its root node's).

### Checks

The engine notices incompatible data through these checks:

| Check | Error | Effect |
|---|---|---|
| `FORMAT` | `the data directory has format <n>, which this version of ivm cannot read ...`, `... does not hold a format version` | refused at start-up before anything is opened |
| frame CRC and length (log, segments, manifest) | `node checksum mismatch`, `input log segment ... is corrupt at byte <n>`, torn-tail truncation | log: in the newest segment, a bad frame and everything after it are truncated at start-up as a torn write; anywhere else, and for a gap in the GSNs after the checkpoint (`input log is missing records ...`), start-up fails; segments: fatal on access; manifest: a torn tail after the last valid record is dropped, and a manifest with contents but no valid record (or a missing one while segment files hold data) stops the start, modifying nothing |
| record decoding | `bad log record tag`, `bad change tag`, `bad datum tag`, `bad agg state tag`, ... | fatal; except that in the newest log segment a frame that does not decode counts as a torn write and is truncated with everything after it |
| checkpoint header and metadata tree | `the data directory has format <n> ...` (header), `bad metadata entry (...)`, `the metadata tree has no next object id` | fatal at start-up |
| format 1 `AppState` JSON decoding | `cannot decode checkpoint state: ...` | fatal at start-up |
| every table's primary index found by fingerprint | `the checkpoint has no index for table <id> (version <v>); refusing to start without its rows` | fatal at start-up |

Consequences of code changes for an existing data directory:

| Change | Consequence |
|---|---|
| frame layouts, `LogRecord` tags or field order, row codec tags, compact numeric encoding | decoding errors, or silently wrong values; incompatible. Code that does not know a record tag truncates the newest log segment at the first such record (see above), losing it and everything after it |
| `CatalogObject`, `ObjectKind`, `TableDef`, `ViewDef`, `Column`, `ColumnType`, `SqlType` serde layout (stored as JSON in the metadata tree and in format 1's `AppState`: renamed or removed fields or variants, new fields without `#[serde(default)]`) | `bad metadata entry (catalog object ...)`, or `cannot decode checkpoint state` for format 1; incompatible: a new format version with a migration. A new field with a default is additive |
| metadata record layouts (tags 1–10), the checkpoint header | incompatible: a new format version with a migration. New record tags are additive (unknown to older code: `bad metadata entry (tag)`) |
| format 1 `AppState` serde layout | read only to migrate: must keep decoding what format 1 wrote. (`source_lsns`, `clock`, `errors` and its `operators`, `arr_since`, `ticks`, `slot_drops`, `catalog.instance_id`, and `truncate` in `SourceState` were added to format 1 with `#[serde(default)]`; the defaults and what they imply are given in the [AppState](#format-1-application-state-appstate) table. The object form of the `CREATE SOURCE` record's `extra` is read next to the older plain array) |
| key encoding of any type, numeric or jsonb key encodings, fidelity suffix, entry layout | **not detected**: existing entries decode wrongly or sort in the wrong place (lookups miss rows); incompatible |
| summary state encodings | as for the row codec. `SumNumeric` states may hold sums beyond numeric's range in the unchanged encoding; code that decodes summary states with numeric's range check fails on them |
| internal node kind 3, the checkpoint record's tree space section | format 1's kind-2 nodes and records without the section are read (see [Internal node](#internal-node) and [Checkpoint record](#checkpoint-record)): the first start reads every tree's internal nodes once, and maintenance passes then visit every subtree once |
| inputs of a **table's** primary-index fingerprint: the operator name `Table`, the object id/version/key-column text, the column expression fingerprint (tag 1), the sort bytes, the `0xFE` separator, the hash function | the engine refuses to start (missing table index) |
| inputs of other fingerprints: operator names and parameter encodings, expression tags, function ids (**the order of function registration** in `expr/ops.rs` and `expr/builtins.rs`: adding, removing or reordering a function renumbers the following ones), `Debug` formatting of plan types (`AggKind`, `AggFunc`, `SqlType`, `SortKey`, `WindowFunc`, `Frame`, `RangeOffsets`, `Rel`, `TableFunc`, `SetOpKind`, `FuncId`) | the affected arrangements of views and indexes are not matched; they are rebuilt by hydration at start-up and the old trees dropped. Correct, but start-up takes as long as creating the views again, and the rebuilt arrangements' history (`arr_since`) starts at the checkpoint the engine restarted from: `AS OF` reads that need them at earlier GSNs fail |
| semantics of SQL functions, operators or aggregates | not detected: maintained results keep values computed with the old code until the rows change; drop and re-create affected views |
| planner or lowering changes that produce different plans for the same SQL | as for fingerprint changes: the views are rebuilt |
| catalog SQL that no longer binds (removed syntax or functions, stricter checks) | the view (and what reads it) is marked broken at start-up: not maintained, reads of it fail with the reason; drop it or repair it with `CREATE OR REPLACE` ([Operations](user-guide/operations.md#recovery)). Stored view definitions are planned leniently: a parameter placeholder (`$1`) in a definition logged by an older version binds as NULL instead of failing |
| stricter rules for [maintained views](sql/maintained-views.md#what-is-rejected) (a function that is no longer immutable, such as `pg_backend_pid()`; reads of the catalog) | not checked at start-up: a materialized view that uses such a construct is built and maintained as before. Re-planning it applies the rules: after an upstream schema change of a table it reads it is dropped as no longer valid, and a `CREATE OR REPLACE` of a view it reads is refused. Replace it with a definition that passes the rules |

Data directories written by earlier versions of the engine are compatible:
format 1 only gained fields with defaults (and a node kind that replaces
one still read), and is migrated to format 2 at the first start. New
functions (registered among the existing ones, which renumbers the later
ones) and planner fixes did change the fingerprints of many views'
arrangements (for example joins on computed keys, whose keys are
materialized in an `Mfp` node, semijoins whose presence inputs match
exactly, and the summaries of `var*`/`stddev*` over `bigint`), so such a
data directory keeps its tables and rebuilds those arrangements once at its
first start. `crates/ivm/tests/data/format1` is a format-1 directory that
the restart tests migrate.

A change in any of the "incompatible" rows needs a new format version whose
migration the engine runs at the first start (as for format 1), or starting
from an empty data directory (and re-creating sources, which re-snapshot the
upstream tables).
