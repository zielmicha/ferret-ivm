# Storage: MVCC B+trees, summaries and the buffer pool

All persistent state except the input log lives in one storage environment
(`storage::Env`, `crates/ivm/src/storage/`): a set of copy-on-write B+trees
whose keys are byte strings and whose values are (GSN, weight) histories,
augmented with aggregate summaries per subtree, cached in a node arena that
acts as the buffer pool. This chapter describes the structures and
algorithms; byte formats are in [On-disk format](../on-disk-format.md).

Files: `mod.rs` (trees, arena, reads, writes, eviction, checkpoint, GC),
`node.rs` (node encoding), `history.rs`, `summary.rs`, `segment.rs`,
`manifest.rs`, `inspect.rs` (buffer-pool residency per tree, for the web UI),
`tests.rs` (model tests).

## Trees

A tree (`TreeId`, a `u64` from a persistent counter) is an ordered map from
**entry** (byte string) to **history**. The dataflow uses one tree per
arrangement; an entry is the encoding of one row (key part, fidelity suffix,
payload — see [Dataflow](dataflow.md#arrangements-and-layouts)), so the tree
is keyed by (key, value) and distinct rows with the same key are distinct
entries. Each tree has a *summarizer* that defines its per-node summaries.

Each tree's state: its root pointer, the summary of the whole tree, the
oldest segment holding any of its nodes (`min_seg`), its summarizer, and
whether it has been attached after recovery.

### Nodes

| Node | Contents |
|---|---|
| leaf | sorted entries, each `(key, history)` |
| internal | sorted children, each `(separator, pointer, summary of the child's subtree, min_seg of the child's subtree)` |

A child's separator is the lower bound of its key range (ignored for the
first child); a key belongs to the last child whose separator is `<=` it.
Nodes are split by size: the target is 16 KiB of estimated serialized size
for leaves and for internal nodes. A node whose contents after an update
exceed the target is split into chunks of balanced size (internal nodes keep
at least two children per chunk); when they exceed twice the target (a bulk
load, hydration), the chunks are filled to 90% of it, so that the next
inserts do not split every node right away. When a leaf splits, the
separator of each new chunk is the shortest prefix of its first key that is
greater than the previous chunk's last key (suffix truncation); a new
internal node takes its first child's separator. The first chunk keeps the
node's place (and separator) in its parent.

Pointers are either `Disk(DiskLoc)` — segment id, offset and length of the
node's frame in a segment file — or `Mem(slot)` — the node is loaded in the
arena.

## The node arena (buffer pool)

Loaded nodes live in `slots`. A slot holds the node, its disk location (if it
has an up-to-date copy on disk), a dirty flag, its parent slot, its tree, the
number of loaded children, a CLOCK reference bit, its estimated size, and
(for leaves) the number of in-place edits since it was last rebuilt.
Accessing a child through a `Disk` pointer reads and decodes the frame,
allocates a slot and replaces the pointer by `Mem(slot)`.

- **Copy-on-write at checkpoint granularity.** Modifying a node marks it
  dirty; a dirty node has no disk location (its previous on-disk copy becomes
  garbage). Dirty nodes stay in memory until the next checkpoint writes them,
  children before parents, to the end of the active segment. On-disk nodes
  are never overwritten, so the last checkpoint stays intact until the next
  one is committed.
- **Eviction.** When the arena's bytes exceed `--cache-mb`, a CLOCK sweep
  (at most two passes over all slots) evicts clean nodes that have no loaded
  children, giving a second chance to nodes whose reference bit is set;
  the parent's pointer reverts to `Disk`. Dirty nodes are never evicted; a
  checkpoint is forced when they exceed half of the cache.

There is no separate in-memory mode: a tree that fits in the cache is served
from memory, a larger one degrades gradually as nodes are re-read from the
segment files (through the operating system's page cache).

## Histories

A history (`storage::History`) is a sorted list of `(GSN, weight)` changes of
one entry. Reads choose a version:

| `ReadAt` | Weight |
|---|---|
| `Latest` | sum of all changes |
| `AsOf(t)` | sum of changes with GSN ≤ t |
| `Before(t)` | sum of changes with GSN < t |

`add(g, w, frontier)` adds a change (merging with an existing change at `g`)
and then compacts: all changes at or before the **retention frontier** are
merged into one change stamped with the latest of their GSNs (dropped if the
sum is zero), and zero-weight changes are removed. An entry whose history
becomes empty is deleted from its leaf. Reads at or after the frontier are
exact; the engine refuses `AS OF` reads before it. (A tree's history also
only starts when the dataflow filled it: an arrangement records the GSN it
was hydrated at, its `since`, and `AS OF` reads of earlier GSNs do not use
it; see [Dataflow](dataflow.md).)

Compaction is lazy: it happens to an entry when the entry is updated, and to
all entries of a leaf when the leaf is rebuilt (see below). Old versions of
entries in leaves that are not modified stay until then.

## Writes

`Env::apply(tree, updates, gsn)` applies a batch of `(entry, weight)`
changes at one GSN:

1. Sort by entry, add up equal entries, drop zero weights.
2. Descend from the root, partitioning the sorted updates among the children
   by binary search on the separators; only children that receive updates
   are visited (and loaded).
3. At a leaf, either edit in place — when the leaf is already dirty, has had
   fewer than 16 in-place edits since it was last rebuilt, and the updates
   are at most an eighth of its entries — or rebuild it by merging the
   updates into its entries, compacting every untouched entry on the way.
   (So the first change to a leaf after a checkpoint always rebuilds and
   compacts it.)
4. Split oversized nodes into balanced chunks; a root that splits gets a new
   parent; a root left with a single child is replaced by it; empty nodes are
   removed.
5. Update summaries on the way back up (incrementally where possible, see
   below), and the tree's total summary.

## Reads

| Operation | Algorithm |
|---|---|
| `get(key)` | descend to the leaf, binary search |
| `for_each(lo, hi, f)` / `for_each_prefix(p, f)` | call `f` with each entry of the range in key order, where it lies in its leaf (nothing is copied), until `f` returns false. One leaf at a time: after a leaf it descends again from the root with the next separator as its new lower bound, so it never pins nodes and tolerates eviction between leaves. A key prefix `p` is the range `[p, successor(p))`. The dataflow's lookups and scans use these |
| `scan` / `scan_prefix` | `for_each` collecting copies of the entries |
| `cursor(lo, hi)` | a stateless range cursor with the same descent that buffers one leaf's entries at a time (for callers that interleave other work between entries) |
| `range_summary(lo, hi)` | combine the stored summaries of children that lie entirely inside the range and recurse into the (at most two per level) partially covered children, folding leaf entries at the edges: O(log n) nodes. The whole-tree summary is stored |
| `nth(lo, hi, n)` | find the entry holding the n-th unit of weight in the range by skipping fully covered children by their counts |
| `tree_count(tree)` | the stored total count |

Summaries reflect the **current** weights only; historical reads use the
histories. (Operators that need a previous aggregate value read it from
their output arrangement.)

## Summaries

Every child pointer of an internal node carries the summary of the child's
subtree (`storage::Summary`): the sum of the current weights of its entries
(`count`) and one aggregate state per entry of the tree's summarizer spec.

| `AggKind` | State | Meaning |
|---|---|---|
| `CountNonNull` | `Count(i64)` | weighted count of entries whose argument is not NULL |
| `CountTrue`, `CountFalse` | `Count(i64)` | weighted count of entries whose boolean argument is true / false |
| `SumInt`, `SumSqInt` | `SumInt(i128)` | weighted sum (of squares) of an integer argument |
| `SumNumeric`, `SumSqNumeric` | `SumNumeric(NumSum)` | exact `numeric` sum (not limited to numeric's range) |
| `SumFloat`, `SumSqFloat` | `SumFloat(FloatSum)` | exact float sum |
| `Min`, `Max` | `Min(Option<Datum>)`, `Max(Option<Datum>)` | extreme value among entries with positive weight |

The summarizer of an arrangement (`LayoutSummarizer`) decodes the columns of
an entry that the aggregate argument expressions read (the others are
skipped without being materialized) and evaluates the expressions; trees
without aggregates (`CountSummarizer` and layouts with no summaries)
maintain only the count.

### Incremental maintenance

When a leaf entry's total weight changes from `before` to `after`, or a
child's summary is replaced by the summaries of the nodes that took its
place, the parent's summary is updated without looking at the other
children. (A rebuilt leaf records its entries' weight changes and folds them
into its old summary when it does not split and fewer of its entries
changed than it holds; otherwise, or when folding is not exact as described
below, its summary is computed from its entries once.)

- counts and sums are invertible: subtract the old contribution, add the new;
- `min`/`max`: a better new value replaces the extreme; a removal or a worse
  value keeps it — unless the removed value could have been the extreme, or a
  new value ties with it in a different representation (where the leftmost
  one must win). In those cases the node's summary is recomputed from its
  children (or its leaf entries).

An incrementally maintained summary is always identical — including the
representation of `min`/`max` values and the scale of `numeric` sums — to
one recomputed from scratch; `Env::check_summaries` verifies this in the
model tests. Ties between equal `min`/`max` candidates resolve to the
leftmost entry in key order, so the result is a function of the contents
only.

### Exact sums

- `SumInt` accumulates in `i128` without overflow checks. It holds sums of
  integer arguments and the sums of squares of `smallint` and `integer`
  arguments (each square below 2^62), which stay far within its range.
  Variance and standard deviation of `bigint` arguments, whose squares
  approach 2^126, sum the argument cast to `numeric` instead
  (`SumNumeric` and `SumSqNumeric`).
- `NumSum` (numeric) keeps the finite sum as a `Numeric`, the weighted
  counts of NaN, +Infinity and -Infinity terms apart (as PostgreSQL's
  aggregate state does), and a multiset of the finite terms' display scales.
  The value is NaN if any NaN (or both infinities) is present, else ±Infinity
  if present, else the finite sum at the largest display scale present.
  Removing a term exactly restores the previous state, scale included. Like
  PostgreSQL's numeric aggregate state, the finite sum is not limited to
  numeric's range (unbounded `add_unbounded` / `sub_unbounded` /
  `mul_unbounded` arithmetic): a sum over a subtree, or a sum of squares,
  may exceed it, so maintaining a summary never fails. Only the aggregate
  value read from it is range-checked (`Numeric::checked`); a group whose
  sum lies beyond the range becomes an error record of that group, which is
  retracted when the group is back in range.
- `FloatSum` keeps the finite part as an integer multiple of 2^-1074 (the
  smallest subnormal double) in a big integer: each `x = m·2^e` adds
  `m·2^(e+1074)`. Addition is exact, associative and invertible; the value is
  rounded to the nearest double (ties to even) once, when read. Infinities
  and NaN are counted apart. Squares are added as `m²·2^(2e+1074)`
  (truncated below the grid). `avg` divides the exact sum with 128 extra
  bits before rounding; variance is computed from the exact sums as
  `(N·Q − S²) / (N·(N−1))` (or `N²`). A finite sum whose rounded value
  overflows (for `sum(real)`, overflows `real`) fails with
  `value out of range: overflow`.

### How aggregates use summaries

A `Reduce` arranges its input by the group key with the summaries its
aggregates need; a group is a key prefix, so its aggregates are one
`range_summary` over that prefix
([Dataflow](dataflow.md#reduce)). Percentiles use `nth` to locate the rows
at the required ranks. Semijoin presence tests use the count of a key
prefix. `EXPLAIN MAINTENANCE` and `SHOW` use tree counts.

## Segments and checkpoints

Nodes are written only by checkpoints, to the **active segment**
(`store/seg-<id>.dat`), an append-only file that is rolled over to the next
id once it reaches 256 MiB. A checkpoint (`Env::checkpoint(gsn, app)`):

1. writes every dirty node of every tree, depth-first so that each parent is
   written after its children and records their new locations; updates each
   tree's `min_seg`;
2. `fsync`s the active segment and any segments rolled over since the last
   sync;
3. appends a checkpoint record — sequence number, GSN, every tree's root
   location, summary and `min_seg`, every segment's (total bytes, live
   bytes), the active segment and its length, the next tree id, and the
   application state — to `store/MANIFEST` and `fsync`s it. This append is
   the commit point;
4. deletes sealed segments without live bytes.

On open, the last valid manifest record defines the state: the active
segment is truncated to the recorded length, and segment files the record
does not mention are deleted. Without a valid record, a store that may have
had a checkpoint — a non-empty manifest, or no manifest but segment files
with data — is refused instead, and nothing is modified (an empty manifest,
left by a crash during the very first checkpoint, opens as a new store).

Creating the store directory, the manifest or a segment file is followed by
an `fsync` of the directory that holds it, so that a crash cannot lose the
directory entry of a file a checkpoint refers to; the manifest rewrite
renames `MANIFEST.new` over `MANIFEST` and `fsync`s the directory too.

## Garbage collection

The environment tracks, for every segment, the bytes written and the bytes
still referenced (`seg_live`). A node that becomes dirty, is dropped, or is
released with its tree subtracts its frame length from its segment's live
bytes.

`Env::gc(max_nodes)` (called after every checkpoint with a budget of 1000
nodes) picks the oldest sealed segment that is less than half live. Every
child pointer records the oldest segment holding any node of its subtree
(`min_seg`), so the pass only descends into subtrees that contain nodes of
the victim (or older) segments; it marks those nodes and their ancestors
dirty, so that the next checkpoint rewrites them into the active segment.
Once a segment has no live bytes it is deleted at the end of a checkpoint.
The active segment itself is never a victim.

Dropping a tree (`drop_tree`) releases all of its nodes; it reads and
decodes every node that is not in memory, leaves included, so dropping a
large tree that is mostly on disk reads all of it.
