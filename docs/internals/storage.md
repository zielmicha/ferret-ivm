# Storage: B+trees of weighted entries, summaries and the buffer pool

All persistent state except the input log lives in one storage environment
(`storage::Env`, `crates/ivm/src/storage/`): a set of copy-on-write B+trees
whose keys are byte strings and whose values are weights, augmented with
aggregate summaries per subtree, cached in a node arena that acts as the
buffer pool. This chapter describes the structures and algorithms; byte
formats are in [On-disk format](../on-disk-format.md).

Files: `mod.rs` (trees, arena, reads, writes, eviction, checkpoint, the
maintenance pass), `node.rs` (node encoding), `summary.rs`, `segment.rs`,
`manifest.rs`, `sort.rs` (loading unordered entries through sorted runs),
`inspect.rs` (buffer-pool residency per tree, for the web UI), `tests.rs`
(model tests).

## Trees

A tree (`TreeId`, a `u64` from a persistent counter) is an ordered map from
**entry** (byte string) to **weight** (`i64`, never 0: a change that makes
an entry's weight 0 removes the entry). The trees hold the current state
only: there are no versions, and nothing to read the state at an earlier
GSN from (the dataflow reads an arrangement as it was before the GSN being
applied by overlaying that GSN's delta on it, see
[Dataflow](dataflow.md#processing-a-gsn)). The dataflow uses one tree per
arrangement; an entry is the encoding of one row (key part, fidelity suffix,
payload — see [Dataflow](dataflow.md#arrangements-and-layouts)), so the tree
is keyed by (key, value) and distinct rows with the same key are distinct
entries. One more tree, the *metadata tree*, holds the catalog and the
engine's state as entries of the same kind
([On-disk format](../on-disk-format.md#the-metadata-tree)). Each tree has a
*summarizer* that defines its per-node summaries.

Each tree's state: its root pointer, the summary of the whole tree, the
oldest segment holding any of its nodes (`min_seg`), the live bytes of its
nodes per segment, its summarizer, and whether it has been attached after
recovery.

### Nodes

| Node | Contents |
|---|---|
| leaf | sorted entries, each `(key, weight)` |
| internal | sorted children, each `(separator, pointer, summary of the child's subtree, min_seg of the child's subtree)` |

A child's separator is the lower bound of its key range (ignored for the
first child); a key belongs to the last child whose separator is `<=` it.
Nodes are split by size: the target is 16 KiB of estimated serialized size
for leaves and for internal nodes (`node::entry_size`, `node::child_size`: a
leaf entry counts its key and 6 bytes; a child its separator and its
summary as encoded and 24 bytes). Each node's estimate is kept exact as its
contents change, also when a merge combines two children's summaries. A
node whose contents after an update exceed the target is split into chunks
of balanced size (internal nodes keep at least two children per chunk);
when they exceed twice the target (a bulk
load, hydration), the chunks are filled to 90% of it, so that the next
inserts do not split every node right away. After an append — a leaf
whose updates all lie past its last key, or an internal node whose updates
all went to its last child — every chunk but the last is filled to 90%
instead: the appends that follow go to the last chunk, so balanced chunks
would stay half full (sequential keys used to leave leaves at about 8 KiB).
When a leaf splits, the separator of each new chunk is the shortest prefix
of its first key that is greater than the previous chunk's last key (suffix
truncation); a new internal node takes its first child's separator. The
first chunk keeps the node's place (and separator) in its parent.

A node that an update leaves smaller than a quarter of its target (a
delete removes its entry at once), or that the maintenance pass relocates
while it is that small, is merged with its right neighbour, or else its left one,
when the two fit in three quarters of the target (so that the result does
not split again soon); the result is merged again while it is still that
small. Merging internal nodes moves the right node's children, its first
child taking the right node's separator. Merging never changes the parent's
summary (the same entries), and the merged node's summary is the two
summaries combined, which is exactly the summary of the merged entries:
sums add exactly, and a `min`/`max` held as a prefix keeps its hash only
when both sides had the same one (see [Summaries](#summaries)).

Pointers are either `Disk(DiskLoc)` — segment id, offset and length of the
node's frame in a segment file — or `Mem(slot)` — the node is loaded in the
arena.

## The node arena (buffer pool)

Loaded nodes live in `slots`. A slot holds the node, its disk location (if it
has an up-to-date copy on disk), a dirty flag, its parent slot, its tree, the
number of loaded children, a CLOCK reference bit, its estimated serialized
size (for splits), and its size in memory. Accessing a child through a `Disk`
pointer reads and decodes the frame, allocates a slot and replaces the
pointer by `Mem(slot)`.

The pool is accounted in bytes of memory, not in serialized bytes: a node's
size in memory (`Node::mem_size`, plus its slot) is its vector of entries or
children by capacity — 24 bytes per leaf entry (its key's pointer and
length, and its weight), whatever the entry's serialized size — plus the
heap blocks they own: each key or separator, and the values of summaries (`min`/`max`
datums or the boxes of their prefixes, exact sums with a float sum's
magnitude once it outgrows its four inline limbs). A heap block of *n*
bytes is counted as *n* + 8 rounded up to 16 (`memory::heap_block`, close
to what glibc and mimalloc take). In-place leaf edits maintain the size
incrementally, as they do the serialized size, and so do merges and the
maintenance pass for the children they change. An entry with an 8-byte key
takes 40 bytes in memory (14 by the serialized-size estimate, less on disk
with prefix compression). (Counting serialized bytes
undercounted narrow entries about fourfold; entries with histories took 56
bytes besides their key, and a heap block for a history of more than two
changes.)

- **Copy-on-write.** Modifying a node marks it dirty; a dirty node has no
  disk location (its previous on-disk copy becomes garbage). A dirty node is
  written to the end of the active segment once its loaded children are all
  clean (so that its frame records their locations): by the next
  checkpoint, or earlier by the buffer pool (below). A written node is clean
  until it is modified again, when its frame becomes garbage like any
  replaced node's. On-disk nodes are never overwritten, so the last
  checkpoint stays intact until the next one is committed; a frame written
  since is referred to by nothing durable, and a crash discards it (see
  [Segments and checkpoints](#segments-and-checkpoints)). Writing a node
  early therefore needs no manifest record: the unit of atomicity (the
  checkpoint) is not the unit of memory.
- **Shared nodes.** A slot holds its node behind a reference count (`Arc`),
  shared with forks of the environment and checkpoint jobs (below). A node
  that is shared is copied before it is modified — its contents, and also
  the child pointers that loading and eviction rewrite — so a fork or a
  checkpoint job keeps the version it was taken with; the copy's memory is
  counted anew (its blocks may have other capacities). An unshared node is
  rewritten in its allocation. Each slot carries a
  modification stamp, renewed whenever the node's contents may change or it
  becomes dirty.
- **Eviction.** A CLOCK sweep (at most two passes over all slots) runs after
  every read and write while the arena's memory exceeds its target — the
  buffer pool's share of the memory budget, which the engine adjusts
  (`Env::set_cache_bytes`, see [Memory](architecture.md#memory)) — or the
  memory of dirty nodes exceeds half of it. Its candidates are nodes without
  loaded children; a candidate whose reference bit is set gets a second
  chance. While the arena is over its target, a cold candidate is evicted —
  a dirty one after it is written — and the parent's pointer reverts to
  `Disk`: dirty and clean nodes compete for the pool alike. Within the
  target, cold dirty candidates are only written, while the dirty memory is
  over half of it, which bounds what a checkpoint has left to write. Every
  loaded subtree has candidates at its leaves, so a sweep finds what it
  needs unless dirty internal nodes hold their clean children in memory; a
  sweep that ends over a limit records a stall, and sweeps resume only once
  the arena or the dirty memory has grown by a sixteenth of the target
  (plus a leaf) beyond it, so sweeping costs nothing when there is nothing
  to evict. Two kinds of nodes are not candidates: dirty nodes frozen in a
  checkpoint job in progress, which the job writes (below), and in a fork
  the nodes it still shares with the environment it was forked from
  ([Forks](#forks)). A large scan — one that has read more than a quarter
  of the target from disk — also evicts, while the pool is over its
  target, the nodes it read as soon as it is done with them
  (`evict_scanned`, after each leaf): it recycles its own nodes, at
  O(depth) per leaf, instead of loading the whole tree or pushing the
  pool's working set out (what a smaller scan reads is cached like any
  other node, a sweep running after every 1/64 of the target it reads).
  `Env::write_back(target)` writes dirty nodes, children before parents,
  until at most `target` bytes of dirty memory remain: the hook for callers
  that manage memory or checkpoints themselves.

There is no separate in-memory mode: a tree that fits in the cache is served
from memory, a larger one degrades gradually as nodes are re-read from the
segment files (through the operating system's page cache). The same holds
for data that is not yet durable: a batch, a commit or a hydration much
larger than the cache is written out as it goes.

## Weights

An entry's weight is the sum of the changes applied to it; an entry whose
weight a change makes 0 is removed from its leaf at once (a leaf left
without entries goes with it). So a tree holds exactly the entries of
non-zero weight: a deleted row, a released temporal event or a retracted
error record leaves no trace in the tree, and a scan, a lookup or a range
summary visits live entries only (a hot row updated at every GSN, whose
versions sort one after the other, is one entry).

Earlier versions kept each entry's history of (GSN, weight) changes, for
reads as of earlier GSNs, consolidated behind a retention frontier by the
maintenance pass. Their leaves (kind 1,
[On-disk format](../on-disk-format.md#leaf-node)) are still read: an
entry's weight is its history's sum, and an entry whose history adds up to
0 is dropped as the leaf is decoded; the leaf is written in the current
format when it is next rewritten.

## Writes

`Env::apply(tree, updates)` applies a batch of `(entry, weight)` changes:

1. Sort by entry, add up equal entries, drop zero weights. A large batch is
   then applied in runs of consecutive entries whose cost — a leaf's size
   plus the entry's length per update, as if each dirtied a leaf of its
   own — stays within a quarter of the cache (at least four leaves), with
   an eviction sweep after each run: a batch never needs its dirty nodes in
   memory at once. Each run is applied as follows.
2. Descend from the root, partitioning the sorted updates among the children
   by binary search on the separators; only children that receive updates
   are visited (and loaded).
3. At a leaf, add each update's weight to its entry's (inserting the entry,
   or removing it when its weight becomes 0): in place when the updates are
   at most an eighth of the leaf's entries (each insert or removal shifts
   the entries after it), otherwise by merging the updates into a new
   vector of entries.
4. Split oversized nodes into chunks (see above); a root that splits gets a
   new parent; a root left with a single child is replaced by it; empty
   nodes are removed; children the update made smaller than a quarter of
   their target are merged with a neighbour (loading it if needed): a leaf
   that deletes empty goes, and leaves that deletes leave nearly empty are
   combined, without waiting for a maintenance pass.
5. Update summaries on the way back up (incrementally where possible, see
   below), and the tree's total summary. The sizes and `min_seg` of the
   internal nodes on the path follow the children that changed: a node's
   `min_seg` is the minimum of its children's, computed from all of its
   children only when a child that may have held the minimum got a higher
   one (the first update of a clean leaf after a checkpoint, which makes it
   dirty), children were merged, or the node splits. (So it never exceeds
   the minimum: between the scans and the writes that make it exact, it is
   only lowered.)

## Reads

| Operation | Algorithm |
|---|---|
| `get(key)` | descend to the leaf, binary search; the entry's weight |
| `for_each(lo, hi, f)` / `for_each_prefix(p, f)` | call `f` with each entry of the range and its weight in key order, where it lies in its leaf (nothing is copied), until `f` returns false. One leaf at a time: after a leaf it descends again from the root with the next separator as its new lower bound, so it never pins nodes; a large scan evicts the nodes it read once it is done with them, while the pool is over its target (see [eviction](#the-node-arena-buffer-pool)), so a scan of a tree larger than the pool holds about the pool's target plus one path. A key prefix `p` is the range `[p, successor(p))`. The dataflow's lookups and scans use these |
| `scan` / `scan_prefix` | `for_each` collecting copies of the entries |
| `cursor(lo, hi)` | a stateless range cursor with the same descent that buffers one leaf's entries at a time (for callers that interleave other work between entries) |
| `range_summary(lo, hi)` | combine the stored summaries of children that lie entirely inside the range and recurse into the (at most two per level) partially covered children, folding leaf entries at the edges: O(log n) nodes. The whole-tree summary is stored. A `min`/`max` that the result holds as a prefix is then resolved to the value ([below](#wide-min-and-max-values)) |
| `nth(lo, hi, n)` | find the entry holding the n-th unit of weight in the range (and its weight, and the unit's offset in it) by skipping fully covered children by their counts |
| `tree_count(tree)` | the stored total count |

`set_read_budget(Some(n))` limits the entries the scans (`for_each`,
`for_each_prefix`, cursors, and everything built on them) may still visit:
past it they fail with the internal code `IVMRB`, which the engine uses to
move a large read off its thread ([Planner](planner.md#batch-execution)).
A scan the budget stops is done with what it read: a large one evicts it
first, as at its end.

Summaries reflect the current weights, which are all a tree holds.
(Operators that need a previous aggregate value read it from their output
arrangement.)

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
| `SumFloat`, `SumSqFloat` | `SumFloat(FloatSum)` | exact float sum (a normalized dyadic rational) |
| `Min`, `Max` | `Min(Option<Datum>)`, `Max(Option<Datum>)`; in node summaries also `MinPrefix`, `MaxPrefix` | extreme value among entries with positive weight; a node summary holds a value whose key encoding is longer than 128 bytes as the first 128 bytes of that encoding |

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
  children (or its leaf entries). For an extreme held as a prefix (see
  below), a value with the same prefix that is added keeps the prefix
  (either way the extreme has it), and one that is removed could have been
  the extreme.

An incrementally maintained summary is always identical — including the
representation of `min`/`max` values and the scale of `numeric` sums — to
one recomputed from scratch; `Env::check_summaries` verifies this in the
model tests. Ties between equal `min`/`max` candidates resolve to the
leftmost entry in key order, so the result is a function of the contents
only.

### Wide `min` and `max` values

Every state of a node summary has a size bounded independently of the data
(counts; sums, see below), except a `min`/`max` value, which is as wide as
the widest value of its column. A node summary therefore holds a value
whose key encoding ([On-disk format](../on-disk-format.md#order-preserving-key-encoding),
ascending) is longer than 128 bytes (`summary::EXTREME_PREFIX`) as a
`WideExtreme` (states `MinPrefix`, `MaxPrefix`): the first 128 bytes of that
encoding and, when every entry of the subtree whose value has this prefix
has the same value, the SHA-256 of that value's exact (codec) encoding
(`same`). The key encoding orders like the values and no encoding is a
prefix of another, so prefixes order like the values, except that equal
prefixes may stand for different values; a value that fits never ties with
a prefix. Equal values have one key encoding, so they are all held as
values or all as the same prefix, and the leftmost representation only
matters between values.

Combining and maintaining summaries works on this form exactly, and the
result is a function of the subtree's contents like the value: the prefix
of the extreme is the extreme of the prefixes, and `same` survives
combining subtrees with the same prefix when they have the same hash. A
leaf summary with a wide extreme takes a second pass over the leaf's
entries, comparing those whose value has the prefix with the extreme
(`summary::WideExtremes`); an entry update that makes a wide value the
extreme bounds the summary at once (`Summary::bound_extremes`), one that
adds a value with the prefix hashes it, and one that removes such a value
recomputes the summary.

`range_summary` folds the entries in the leaves at the range's edges
plainly, apart from the stored summaries of the subtrees inside the range,
which it combines. A prefix in the latter needs resolving only where it can
be the range's extreme: an edge value that is better excludes it, and an
edge value that ties it while the subtrees' `same` is its hash is it.
Otherwise `Env::resolve_extremes` descends into the subtrees inside the
range whose stored extreme has the prefix and folds the leaves it reaches:
the extreme is the leftmost best of those entries. With `same`, the first
entry with the prefix has the value, so one path is visited and a range
summary stays O(log n); without (values sharing their first 128 encoded
bytes with the extreme), every subtree whose extreme has the prefix is
visited. The group cache of a `Reduce` keeps resolved values, so changes it
can fold in do not resolve again. (The shortcut takes values with the same
prefix and SHA-256 to be equal; everything else compares values.)

A wide value read from a node written before prefixes existed (states 6
and 7, see [On-disk format](../on-disk-format.md#summary)) becomes a prefix
without `same`, which is always safe (only the shortcut is lost) until the
node's summary is next recomputed.

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
- `FloatSum` keeps the exact finite sum as a dyadic rational `±m·2^e`
  (`Dyadic`), normalized so that every value has one representation and
  encoding: `m` odd, or 0 with `e` = 0. Each `x = m·2^e` adds `m·2^e`;
  addition aligns the exponents and is exact, associative and invertible.
  The magnitude is held in 64-bit limbs, inline up to 256 bits: a sum needs
  about 53 bits plus the span of its terms' exponents plus the bits of their
  count (106 bits plus twice the span for squares), so adding a value does
  not allocate, and a typical sum encodes in 10 to 20 bytes, a sum of
  squares in up to about 30. The value is rounded to the nearest double
  (ties to even) once, when read. Infinities and NaN are counted apart.
  Squares are added as `m²·2^(2e)`, truncated to a multiple of 2^-1074 (the
  smallest subnormal double), which every sum therefore is. `avg` divides
  the sum, as a number of 2^-1074 units, with 128 extra bits, truncates and
  rounds; for sums of at least 2^115 units, where that is correctly
  rounded, a correctly rounded division of the limbs gives the same result.
  Variance is computed from the exact sums as `(N·Q − S²) / (N·(N−1))` (or
  `N²`) on the 2^-2148 grid of the squares, with 128 extra bits, truncated
  and rounded. (These results are
  those of the earlier representation, an integer multiple of 2^-1074 in a
  big integer, bit for bit: maintained views hold values computed with it.)
  A finite sum whose rounded value overflows (for `sum(real)`, overflows
  `real`) fails with `value out of range: overflow`.

## Loading unordered entries

Applying batches of entries in arbitrary key order to a large tree would
touch, and rewrite, every leaf with every batch. A `TreeBuilder`
(`sort.rs`) loads entries in bounded memory with an external merge sort;
the dataflow uses it to build arrangements during hydration, to spill large
deltas and key sets into temporary trees, and to apply large deltas of
nodes nobody reads to their arrangements:

- entries are buffered up to a buffer of the environment's working memory
  (`Env::work`, `memory::Work`: half its share, at least 1 MiB, at most
  64 MiB, or less while the buffers together exceed the share), counting
  each entry's place in the buffer and its key's heap block, which the
  buffer charges to the working memory while it holds them;
- a full buffer is sorted; if it lies wholly after everything applied to
  the target so far (the first buffer, or input arriving in key order or
  nearly so), it is applied to the target directly, at its right edge;
  otherwise it is stored as a *run* in a temporary tree, whose nodes the
  buffer pool writes out and evicts like any other;
- once there are 128 runs, the oldest 64 are merged into one;
- `finish` merges the remaining runs (at most 64; first merging the oldest
  64 while there are more) into the target in key order, adding up equal
  entries, in batches of a quarter of the buffer, and drops the runs.

Temporary trees (`Env::create_temp_tree`: sort runs, spilled deltas) take
their ids from the top of the id range downwards (down to 2^63,
`TEMP_TREES`), so the ids of persistent trees do not depend on them; they
are not meant to outlive the operation that creates them. A checkpoint does
not record them, nor count their nodes as live in its segment table, and
maintenance passes leave them alone.
`Env::tree_is_empty` tells a tree without entries (its count, the sum of
the current weights, can be zero while entries remain).

### How aggregates use summaries

A `Reduce` arranges its input by the group key with the summaries its
aggregates need; a group is a key prefix, so its aggregates are one
`range_summary` over that prefix
([Dataflow](dataflow.md#reduce)). Percentiles use `nth` to locate the rows
at the required ranks. Semijoin presence tests use the count of a key
prefix. `EXPLAIN MAINTENANCE` and `SHOW` use tree counts.

## Forks

`Env::fork` returns an independent environment holding every tree as it is
at that moment, for another thread: it shares the loaded nodes (each side
copies a shared node before modifying it, see above) and holds the segment
files with live nodes open, so that it can still read nodes whose segments a
later checkpoint deletes. It can be modified, and it shares the store's
append side, so its buffer pool works like any other: it writes dirty nodes
early and evicts them, and a large hydration in a fork stays within its
pool. Its pool counts only the fork's own nodes against its target — those
it loads, creates, copies or modifies, and the ones it inherited that
nobody else holds any more (the environment modified or evicted its
version): the nodes still shared with the environment cost the fork
nothing, and it neither writes nor evicts them (it would only write the
environment's dirty nodes again). The bytes it appends are added to the
environment's segment totals (`count_foreign`, before a checkpoint, a
maintenance pass or an adoption), and every segment it appends to is
pinned — not deleted, also once dead — until it is dropped. Its target is
set by its user (the engine gives each fork a share of the memory budget
while it lives, `Env::hold`). It cannot take checkpoints. Taking a fork copies the slot table (one reference count
increment per loaded node), not the nodes.

A tree created empty in the environment before the fork and filled in the
fork can be moved back with `Env::adopt_trees`: its loaded nodes, dirty or
written, are moved into the environment's slots, not copied, and those on
disk stay where the fork wrote them, now counting as live in the
environment (their segments are pinned until then). Hydration off the
engine thread is built on this
([Dataflow](dataflow.md#hydration-off-the-engine-thread)).

## Segments and checkpoints

Nodes are written to the **active segment** (`store/seg-<id>.dat`), an
append-only file that is rolled over to the next id once it reaches
256 MiB, by checkpoints and by the buffer pool's early writes, which are
the same operation (`write_node`, for a dirty node whose loaded children
are clean): the frame counts as live bytes of its segment and of its tree,
and towards the next maintenance pass's budget unless that pass dirtied
the node; the child entry above the node gets its exact `min_seg`, and the
entries above that (and the tree's) get a `min_seg` lowered to it where
they were higher. A checkpoint has three parts, which can run on
different threads:

1. `Env::begin_checkpoint(gsn)` freezes a version of the dirty nodes: a
   checkpoint job holds them (shared, with their modification stamps), the
   clean nodes they and the tree roots point to, every tree's root, the
   segment accounting, and the manifest, which the environment lacks until
   the job is finished (a second checkpoint cannot begin meanwhile). The
   append side is shared: the environment goes on, and nodes it modifies
   are copied from the frozen ones; its pool goes on writing dirty nodes
   early, to the same active segment, except the frozen ones (stamped
   before the job began), which the job is writing (were both to write one,
   a frozen parent marked clean by the job would refer to the job's frame
   of a child the environment counts at another).
2. `CheckpointJob::run(app)` writes every frozen dirty node of every tree
   (temporary trees aside, below), depth-first so that each parent is
   written after its children and records their locations and `min_seg`,
   and updates each tree's `min_seg`; `fsync`s the active
   segment and any segments rolled over since the last sync (outside the
   append side's lock: the record names the length that sync covered, and
   frames others append meanwhile lie beyond it); appends a checkpoint
   record — sequence number, GSN, every tree's root location, summary and
   `min_seg`, every segment's (total bytes, its length; live bytes), the
   active segment and its length, the next tree id, the application state
   (a header of a few bytes naming the metadata tree), and every tree's
   live bytes per segment — to `store/MANIFEST` and `fsync`s it
   (this append is the commit point). It returns the sealed segments
   without live bytes in the committed state.
3. `Env::finish_checkpoint` takes the append side and manifest back, and
   marks each written node clean, with its new location, if its slot still
   holds it unchanged (same stamp): it is then accounted as if written
   here (its frame live in its segment and its tree's, its exact `min_seg`
   in its parent's child entry, `min_seg` lowered up the ancestors), and the bytes the job wrote for the engine's updates count
   towards the next maintenance pass. A written node that was modified (or
   freed) since the job began stays dirty (or gone): its image is garbage
   from the start, counted in its segment's total but not as live. Then the
   segments dead in the committed state are deleted if they are dead here
   too — the environment may have written live frames to them, or adopted a
   fork's tree with nodes in them, since the job began — and no fork pins
   them.

`Env::checkpoint(gsn, app)` does the three in a row.

On open, the last valid manifest record defines the state: the active
segment is truncated to the recorded length, and segment files the record
does not mention are deleted. This discards every frame written after the
record — by an interrupted checkpoint, or early by the buffer pool, in the
active segment beyond its recorded length or in segments started since —
and nothing durable refers to those frames. Without a valid record, a
store that may have had a checkpoint — a non-empty manifest, or no
manifest but segment files with data — is refused instead, and nothing is
modified (an empty manifest, left by a crash during the very first
checkpoint, opens as a new store).

Creating the store directory, the manifest or a segment file is followed by
an `fsync` of the directory that holds it, so that a crash cannot lose the
directory entry of a file a checkpoint refers to; the manifest rewrite
renames `MANIFEST.new` over `MANIFEST` and `fsync`s the directory too.

## Space reclamation

The environment tracks, for every segment, the bytes written and the bytes
still referenced (`seg_live`), and for every tree its live bytes per
segment. A node that becomes dirty, is dropped, or is released with its
tree subtracts its frame length from both — also a frame the buffer pool
wrote early, whose node was modified again before a checkpoint.

Garbage arises from updates in one place only: copy-on-write leaves the
previous image of every node a checkpoint rewrites in its segment (the
leaves themselves hold live entries only: a retraction removes its entry,
and merges combine the leaves deletes leave underfull, as the updates
happen). The updates do not reclaim the dead frames, so a pass finds them
by need and removes them.

`Env::maintain()` runs before every checkpoint (the checkpoint then writes
what it rewrote, and deletes the segments it emptied). It visits a tree only
if it has live bytes in a segment being evacuated, and descends only into
children that may hold such nodes (a node of an evacuated segment in the
subtree, found through `min_seg` and the child's own location). A node in
an evacuated segment is made dirty, so that the checkpoint writes it to the
active segment, and the path to it with it; a relocated node smaller than
a quarter of its target is merged with a neighbour, as after an update (a
leaf of an earlier version is smaller once its retracted entries are
dropped, and one that decodes empty is removed). None of this changes a
summary: relocating and merging keep the entries. Rewriting a clean node
marks it and its ancestors dirty.

The segments to evacuate are the sealed segments less than half live, in
decreasing order of the cost-benefit of cleaning them: free space gained per
byte moved, `(1 − u) / (1 + u)` with `u` the live fraction, weighted by age
(the active segment id minus the segment's), since data that survived long
tends to stay where it is moved; the pass takes them while their live bytes
fit in its budget (the first one always). It does not read a leaf to learn
whether it lies in such a segment: the parent's pointer says, and every
leaf of a tree is at the depth of the first one the pass reaches.

Reading and rewriting clean nodes is budgeted (`MaintBudget`; rewriting the
path to a rewritten node and anything dirty is not counted): for evacuation
and the merges it brings, 8 MiB plus the bytes written for the engine's own
updates since the previous pass (by checkpoints and early), which is the
rate at which garbage arises. The nodes it rewrites are dirty
until the checkpoint that follows the pass writes them, so their memory is
budgeted too: no more than the pool's target leaves besides the dirty
nodes already in it (the buffer pool keeps those within half of the
target, writing the rest early). What does not fit waits for the next pass:
the pass reports that it left work (`MaintStats::unfinished`), which makes
the engine's next checkpoint due even if nothing else changes.
`maintain_with` takes an explicit budget (a zero budget does nothing but
report the work it found). Once a segment has no live bytes it is deleted at the end of a
checkpoint. The active segment itself is never evacuated.

Dropping a tree (`drop_tree`) reads nothing: it frees the tree's loaded
nodes and subtracts its live bytes per segment from the segments'.
`Env::segment_reads` counts the node frames an environment has read.
