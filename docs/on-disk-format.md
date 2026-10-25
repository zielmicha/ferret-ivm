# On-disk format

> **Version note.** This document describes the formats implemented at commit
> `d80c7f2` (the initial implementation). Commit `1275115`, also merged into
> this branch, only changes test tooling and does not affect any format. The
> engine has no format version number (see
> [Compatibility](#compatibility)); reconcile this document with later
> changes to `log.rs`, `storage/`, `types/codec.rs`, `types/encoding.rs`,
> `dataflow/layout.rs`, `dataflow/mod.rs` (fingerprints), `expr/mod.rs`
> (expression fingerprints), the function registry order, `catalog.rs`,
> `coord/mod.rs` (`AppState`), `crates/pgnumeric/src/encoding.rs` and
> `crates/pgfuncs/src/json/keyenc.rs`.

Every statement below was checked against the code and, where indicated, by
decoding files written by a running engine.

## Contents

1. [Conventions](#conventions)
2. [Data directory layout](#data-directory-layout)
3. [The LOCK file](#the-lock-file)
4. [Input log](#input-log)
5. [Storage segments and nodes](#storage-segments-and-nodes)
6. [Manifest and checkpoints](#manifest-and-checkpoints)
7. [Checkpoint application state](#checkpoint-application-state-appstate)
8. [Data encodings](#data-encodings)
9. [Structural fingerprints](#structural-fingerprints)
10. [Garbage collection and retention](#garbage-collection-and-retention)
11. [Compatibility](#compatibility)

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
| `LOCK` | `coord::start` | empty; exclusive lock |
| `log/log-NNNNNNNNNNNNNNNNNNNN.ivl` | sequencer (`log::LogWriter`) | input-log segment; the number is the GSN of its first record, zero-padded to 20 digits |
| `store/MANIFEST` | checkpoints (`storage::manifest`) | sequence of checkpoint records |
| `store/MANIFEST.new` | manifest rewrite | the single record of a rewritten manifest, until renamed |
| `store/seg-NNNNNNNN.dat` | checkpoints (`storage::segment`) | B+tree node frames; the number is the segment id (`u32`, from 1), zero-padded to 8 digits |

Directories are created on demand. Files in `log/` and `store/` whose names do
not match these patterns are ignored. Example of a fresh directory after one
second: `LOCK` (0 bytes), `log/log-00000000000000000001.ivl` (one tick frame
per second, 25 bytes each), `store/MANIFEST` (0 bytes until the first
checkpoint), `store/seg-00000001.dat` (0 bytes).

## The LOCK file

`LOCK` is opened (created if missing, never truncated or written) and locked
with `File::try_lock` (an exclusive, non-blocking `flock`). The handle is kept
open for the life of the process (`coord::Running::_lock`), so the lock is
released by the kernel when the process exits for any reason. A failed lock
attempt aborts start-up with SQLSTATE `55006`
(`data directory <dir> is in use by another ivm process`). The file is never
deleted and its contents are never read.

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
  after that record's GSN is created, and the directory is `fsync`ed. A
  segment can therefore exceed 64 MiB by up to one batch, and a segment
  boundary always falls between batches.
- GSNs are contiguous: each frame's GSN is the previous frame's plus one,
  across segment boundaries.

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

**Commit** — one upstream transaction, or one local DML statement.

- `source`: the source's `ObjectId`, or `0` for local tables written with SQL.
- `lsn`: for a source, the end LSN of the upstream transaction as a `u64`
  (`X/Y` is `(X << 32) | Y`); for local DML, the engine's request id (unrelated
  to LSNs; used to route the reply).
- each change: `uvarint table` (the table's `ObjectId`), `u8 kind`, payload:

| Kind | Change | Payload | Meaning |
|---|---|---|---|
| 1 | `Upsert` | `row`, `uvarint k`, `k × uvarint column` | set the row of its primary key; listed columns (unchanged TOASTed values) are taken from the stored row |
| 2 | `Delete` | `row` (primary-key values in key order) | delete by key |
| 3 | `Insert` | `row` | add one instance (multiset tables) |
| 4 | `Remove` | `row` | remove one instance (multiset tables) |
| 5 | `Truncate` | — | remove all rows |

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
- `extra`: empty, or for `CREATE SOURCE` and `ALTER SOURCE ... REFRESH` the
  discovered upstream tables as a JSON array of
  `{"schema": str, "name": str, "columns": [Column], "pk": [column index, ...], "replica_identity_full": bool}`
  (`coord::ddl::DiscoveredTable`; `Column` as in [AppState](#checkpoint-application-state-appstate)).
  For `REFRESH` the array lists all discovered tables; the engine adds the new
  ones.

**Progress** — a source's bookkeeping without data.

- `lsn`: the end LSN of the commit after which it was logged (informational;
  not used on replay).
- `state`: JSON `SourceState`:
  `{"slot_created": bool, "tables": {"<table id>": {"done": bool, "boundary": null | [str, ...]}}}`
  where `boundary` holds the text values of the last primary key copied by the
  initial snapshot.

**Schema** — an upstream table changed shape: the source `ObjectId`, the table
`ObjectId`, the new columns as a JSON array of `Column`, and the new primary
key as column indexes.

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
  the last GSN + 1, and appends continue in the newest segment.
- **Replay** (`read_from(after)`): segments are visited in GSN order; a segment
  is skipped if the next segment starts at or before `after + 1`. Each
  remaining segment is parsed with the same rules and its records with
  `gsn > after` are returned. Parsing of a segment stops at its first bad
  frame without reporting an error, and replay continues with the next
  segment.
- **Truncation** (`truncate_before(g)`, after every checkpoint with
  `g = checkpoint GSN − retention`): every segment except the current one
  whose successor starts at or before `g + 1` (so all its records are ≤ `g`)
  is deleted.
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
  existing file of that name is truncated). Sealed segments are never
  modified; they are `fsync`ed at the next sync and deleted whole once no live
  node remains in them.
- On open, the active segment named by the last checkpoint (segment 1 for a
  new store) is opened, created if missing, and resized to the checkpoint's
  `active_len` (0 for a new store) — discarding nodes written by an
  interrupted checkpoint. Every other `seg-*.dat` that the checkpoint's
  segment table does not list is deleted.

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
u8       kind = 2
uvarint  n                         number of children (at least 1)
n × child:
  bytes    separator               lower bound of the child's key range (stored but ignored for the first child)
  uvarint  seg, uvarint off, uvarint len
                                   DiskLoc of the child's frame
  uvarint  min_seg                 oldest segment id holding any node of the child's subtree (4294967295 = none)
  summary                          the child subtree's summary (below)
```

A key belongs to the last child whose separator is `<=` the key. Separators
are shortened at leaf splits: the separator of a new leaf is the shortest
prefix of its first key that is greater than the previous leaf's last key.

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
| 4 | `SumNumeric` | finite sum as a [compact numeric](#compact-numeric-encoding); `uvarint s`, `s × (uvarint display_scale, ivarint count)`; `ivarint nan`; `ivarint pos_inf`; `ivarint neg_inf` |
| 5 | `SumFloat` | `u8 sign` (0 = zero, 1 = positive, 2 = negative), `bytes magnitude` (little-endian), `ivarint nan`, `ivarint pos_inf`, `ivarint neg_inf`; finite value = sign × magnitude × 2^−1074 |
| 6 | `Min` | one [codec datum](#row-codec); NULL (tag 0) means "no value" |
| 7 | `Max` | same |

Tag 3 is unused. The aggregate states are positional: their meaning (which
`AggKind` and which argument expression) comes from the arrangement's layout,
which is derived from the catalog when the dataflow is planned; it is not
stored in the tree.

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
bytes    app                     application state (JSON, see below)
```

Every record is complete: it lists every tree, every segment and the whole
application state, so only the last record matters. The very first record of
a new data directory (after about ten seconds of ticks):

```
a7 00 00 00  78 16 b4 e1     len 167, crc
01                           seq 1
0b                           gsn 11
00                           no trees
01  01 00 00                 one segment: id 1, 0 bytes written, 0 live
01                           active segment 1
00                           active length 0
01                           next tree id 1
9b 01  7b 22 63 61 ...       app: 155 bytes of JSON {"catalog":{"objects":{},"next_id":1000,...
```

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
  Readers either see the old file or the new one.

### Checkpoint procedure

1. Every tree with a dirty root is written depth-first, children before their
   parent, so that each internal node records the final locations of its
   children; each written frame adds its length to its segment's
   `total_bytes` and `live_bytes`.
2. The active segment and any segments sealed since the last checkpoint are
   `fsync`ed.
3. The checkpoint record is appended to the manifest.
4. Sealed segments whose `live_bytes` is 0 are deleted.

Creating a segment file (the first one, and each new active segment) is
followed by an `fsync` of the store directory, so that a checkpoint never
refers to a file whose directory entry a crash could lose.

## Checkpoint application state (AppState)

The record's `app` field is a JSON object (`coord::AppState`, serialized
with `serde_json`):

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
    "history": [[47, "CREATE TABLE orders (...)"], ...]
  },
  "trees": [[[24, 205, 206, 130, 207, 117, 128, 118, 215, 229, 14, 19, 128, 103, 100, 230], 9], ...],
  "view_errors": [[1008, [[["22003", "value out of range: overflow"], 1]]], [1003, []]],
  "source_states": [[1000, "{\"slot_created\":true,\"tables\":{...}}"]],
  "source_lsns": [[1000, 32216824]],
  "next_req": 24,
  "clock": 846243883442476
}
```

| Field | Contents |
|---|---|
| `catalog.objects` | map from object id (as a JSON string key) to `CatalogObject` |
| `catalog.next_id` | next object id |
| `catalog.history` | `[gsn, statement]` of every successfully applied DDL statement (`ivm.catalog_history`) |
| `trees` | `[fingerprint, tree id]` for every hydrated arrangement; the fingerprint is 16 bytes as a JSON array of numbers ([Structural fingerprints](#structural-fingerprints)) |
| `view_errors` | per materialized view: its error records `[[sqlstate, message], count]` |
| `source_states` | per source: its `SourceState` JSON as a string (as in `Progress` records) |
| `source_lsns` | per source: the end LSN of its last logged commit (resume point); `[]` if absent |
| `next_req` | request counter |
| `clock` | logical clock, microseconds since the PostgreSQL epoch; `0` if absent |
| `ticks` | `[gsn, clock]` of the clock ticks in the retention window (at most the last 100,000), for `now()` in reads as of earlier GSNs; `[]` if absent (then `[[checkpoint gsn, clock]]`) |

`CatalogObject`: `id`, `name`, `kind`, `created_gsn`, `create_sql` (the
logged statement; for replicated tables the text
`-- table <schema>.<table> replicated by source <id>`). `kind` is one of:

| Variant | Fields |
|---|---|
| `Table` | `columns: [Column]`, `pk: [column index]` (empty: multiset), `source: id or null`, `upstream: [schema, table] or null`, `version: u32` (incremented by upstream schema changes) |
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
`n + 4`; `-1`: none).

The application state is the only place where the catalog is stored besides
the log; dataflow plans are re-derived from it on every start.

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
`interval` decodes canonically to (months = days ÷ 30, days mod 30, µs) and
gets its exact fields back from the fidelity suffix.

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
| table primary index | primary-key columns in key order, ascending (all columns for a multiset table) | the other columns |
| `CREATE INDEX` | the listed columns, ascending | the other columns |
| join lookup index | the join key expressions (may be computed expressions) | the input's columns that are not plain key columns |
| aggregation input | group columns, then the `ORDER BY` of an ordered aggregate | the other projected input columns; summaries per the aggregates |
| aggregation output, view read index, `Distinct`/`SetOp`/`Recursive` | group columns, or all columns | the rest |
| temporal events | `(time bigint, kind smallint)` | the input row |

### Catalog serialisation

The catalog is serialised only as part of the [application state](#checkpoint-application-state-appstate)
(JSON); DDL statements are additionally logged as text in `Ddl` records.

## Structural fingerprints

Code: `crates/ivm/src/dataflow/mod.rs` (`hash128`, `node_fp`,
`add_arrangement`), the operators' `fingerprint` methods, `Mfp::fingerprint`,
`Expr::fingerprint`. Checkpoints identify arrangement trees by fingerprint
(`AppState.trees`); recovery finds them again by re-planning the catalog and
computing the same fingerprints.

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
| `Table` | object id `u64 LE`, table version `u32 LE`, Rust `Debug` text of the primary-key column list (e.g. `[0]`, `[0, 2]`, `[]`) |
| `Constant` | each row in the row codec |
| `Mfp` | input arity `u32 LE`; per step `0x01` + expression (map) or `0x02` + expression (filter); `0x03`; each projected column `u32 LE` |
| `Union` | one byte per input: 1 if negated |
| `Negate` | — |
| `FlatMap` | per function: `Debug` text `"<TableFunc>/<arity>"`, its argument expressions, `0xFD`; then the ordinality flag byte |
| `DeltaJoin` | per input: node fingerprint, MFP fingerprint, presence arrangement fingerprint or `0x00`; per delta path: `0xA0`, source input `u32 LE`, source filter expressions; per step: `0xA1`, input `u32 LE`, arrangement fingerprint, key expressions, null-safe flag bytes, filter expressions |
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
| `LIKE` | 18 | escape character (0 if none); case-insensitive, negated, bytea flag bytes |
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

Example: the primary index of table 1000 (version 0, primary key `[0]`) has
node fingerprint `hash("Table" ++ FF ++ e8 03 00 00 00 00 00 00 ++ 00 00 00 00 ++ "[0]")`
and arrangement fingerprint
`hash(node_fp ++ 01 00 00 00 00 ++ 00 00 ++ FE)` = `b747235986039468879eec77ae511f56`
(recomputed independently from the description above and matching the
checkpoint of a running engine).

## Garbage collection and retention

- **Segment liveness**: the manifest's segment table records, per segment,
  the bytes written and the bytes still referenced by some tree. A node frame
  stops being live when its node is modified (dirty), dropped, or released
  with its tree.
- **Deletion**: after each checkpoint, sealed segments with zero live bytes
  are deleted. The active segment is never deleted or collected.
- **Relocation** (`Env::gc`, after each checkpoint, budget 1000 nodes): the
  oldest sealed segment with `live_bytes × 2 < total_bytes` is the victim;
  nodes stored in it (or in older segments) are marked dirty, found through
  the per-subtree `min_seg` values, and rewritten by the next checkpoint.
- **Unreferenced trees**: trees present in the checkpoint but not attached by
  any arrangement after recovery are dropped during start-up.
- **Retention**: history elements at or before the retention frontier
  (`processed GSN − --retention-gsns`) are merged when a leaf is next
  modified; input-log segments whose records all lie at or before
  `checkpoint GSN − retention` are deleted after the checkpoint.

## Compatibility

The engine writes no format version. It notices incompatible data only
through these checks:

| Check | Error | Effect |
|---|---|---|
| frame CRC and length (log, segments, manifest) | `node checksum mismatch`, torn-tail truncation | log: the bad frame and everything after it in that segment are ignored; segments: fatal on access |
| record decoding | `bad log record tag`, `bad change tag`, `bad datum tag`, `bad agg state tag`, ... | fatal |
| `AppState` JSON decoding | `cannot decode checkpoint state: ...` | fatal at start-up |
| every table's primary index found by fingerprint | `the checkpoint has no index for table <id> (version <v>); refusing to start without its rows` | fatal at start-up |

Consequences of code changes for an existing data directory:

| Change | Consequence |
|---|---|
| frame layouts, `LogRecord` tags or field order, row codec tags, compact numeric encoding | decoding errors, or silently wrong values; incompatible |
| `AppState`, `Catalog`, `CatalogObject`, `ObjectKind`, `TableDef`, `ViewDef`, `Column`, `ColumnType`, `SqlType` serde layout (renamed or removed fields or variants, new fields without `#[serde(default)]`) | `cannot decode checkpoint state`; incompatible. (`source_lsns`, `clock` and `ticks` were added with `#[serde(default)]`.) |
| key encoding of any type, numeric or jsonb key encodings, fidelity suffix, entry layout | **not detected**: existing entries decode wrongly or sort in the wrong place (lookups miss rows); incompatible |
| inputs of a **table's** primary-index fingerprint: the operator name `Table`, the object id/version/primary-key text, the column expression fingerprint (tag 1), the sort bytes, the `0xFE` separator, the hash function | the engine refuses to start (missing table index) |
| inputs of other fingerprints: operator names and parameter encodings, expression tags, function ids (**the order of function registration** in `expr/ops.rs` and `expr/builtins.rs`: adding, removing or reordering a function renumbers the following ones), `Debug` formatting of plan types (`AggKind`, `AggFunc`, `SqlType`, `WindowFunc`, `Frame`, `RangeOffsets`, `Rel`, `TableFunc`, `SetOpKind`, `FuncId`) | the affected arrangements of views and indexes are not matched; they are rebuilt by hydration at start-up and the old trees dropped. Correct, but start-up takes as long as creating the views again |
| semantics of SQL functions, operators or aggregates | not detected: maintained results keep values computed with the old code until the rows change; drop and re-create affected views |
| planner or lowering changes that produce different plans for the same SQL | as for fingerprint changes: the views are rebuilt |
| catalog SQL that no longer binds (removed syntax or functions) | the view (and what reads it) is marked broken at start-up: not maintained, reads of it fail with the reason; drop it or repair it with `CREATE OR REPLACE` ([Operations](user-guide/operations.md#recovery)) |

An upgrade that changes any of the "incompatible" rows requires starting
from an empty data directory (and re-creating sources, which re-snapshot the
upstream tables) or a migration tool, which does not exist.
