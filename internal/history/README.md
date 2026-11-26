# Disk-backed v1 history

`Catalog.Replace` opens the authoritative `bog.checkpoint` in a pinned
directory. The caller coordinates with the checkpoint publisher and installs
only after the manifest rename and directory sync succeed. An uncertain
publication requires the publisher's reopen and recovery procedure before
replacement. History uses the manifest and block formats in
[checkpoint/FORMAT.md](../checkpoint/FORMAT.md) and
[checkpoint/BLOCK_FORMAT.md](../checkpoint/BLOCK_FORMAT.md).

The reader verifies the exact manifest payload hash, every referenced file's
size and SHA-256, the block version and both CRC32C checksums, row boundaries,
UTF-8, exact inclusive time bounds, and strict order by UTC instant and append
ordinal. Across the segment set it checks that every ordinal in
`[0, NextOrdinal)` occurs once. Validation takes place before the catalog makes
a generation available. Unreferenced files have no effect. After activation,
segments must remain immutable and available while any generation refers to
them. An I/O error terminates the scan. A caller building a query page should
publish it only after its scan succeeds.

## WAL boundary and backup

`Generation.ID` is the SHA-256 of the exact manifest payload. `Boundary` carries
the separate SHA-256 of the WAL bytes in `[0, CoveredOffset)`, including the
eight-byte `BOGWAL01` magic, and the first append ordinal after that prefix.
The prefix digest identifies the bytes covered by the checkpoint. In v1 it
does not uniquely identify a WAL file or provide a writer epoch.

Checkpoint publication binds that offset to the end of a complete legacy
frame under the directory lock, or to byte 8 for an empty checkpoint. The
history reader consumes the published boundary with the validated segment
set. Recovery or backup must independently establish the matching WAL identity,
verify the committed frame boundaries and fix a committed end before replay
or export. The uncovered interval is `[CoveredOffset, committedEnd)`. Start
that suffix at `NextOrdinal` and retain complete frames in append order.
Generation acquisition pins the segment snapshot for this procedure. It does
not capture the committed end of a live WAL.

The interoperability fixture in `TestPublisherCompatibilityAndWALOracle`
uses the existing WAL writer and v1 checkpoint publisher, with a maximum-size
frame, duplicate rows, backfill, timestamp extrema and an uncommitted suffix.

## Query and lifetime

Acquire captures one generation. A reader can scan it after a swap, including
when its first scan begins after the swap. A request for an ID other than the
current ID returns `*ExpiredError`. Readers and in-flight scans hold separate
references. `Generation.Drained` closes once the generation has been retired
and all of those references are gone. Retention may use that signal together
with the authoritative manifest and other generations' references to decide
when files are safe to remove.

The merge uses at most one cursor per overlapping segment. Its heap key is
`(timestamp, append ordinal)`, preserving duplicate lines and backfill.
A query skips disjoint segment time bounds and binary-searches the sparse
points in each candidate. The interval is half-open, the optional continuation
position is exclusive, and matching uses the exact case-sensitive substring.
The caller owns cursor authentication and the lifetime of output it retains.

## Memory bounds

Let `S ≤ 1024` be the number of segments in a generation and
`B ≤ checkpoint.MaxBlockSize` the largest segment's encoded size.

- The manifest input is capped at 256 KiB. Each generation retains file
  metadata and at most 128 sparse seek points per segment. It retains no
  decoded row bodies. Each replacement validates one candidate at a time with
  one encoded buffer of at most `B` bytes. The fast path uses a reusable
  bitmap of roughly `B / (24 × 8)` bytes to prove dense ordinal ranges.
- For valid v1 producers that shuffle ordinal membership across runs, exact
  coverage validation uses an additional bitmap of at most 2 MiB. It revisits
  sparse runs that intersect each ordinal window. Ordinary legacy conversion
  produces contiguous ordinal intervals, even with timestamp backfill, so it
  takes the fast path and reads each segment once.
- Each scan holds a heap and cursor metadata of size `O(S)`, at most one 4 KiB
  page per overlapping run, and one reusable overflow line buffer capped by
  `B`. A matched line passed to the visitor is a separate copy of at most
  `B` bytes. The search literal is bounded by the v1 maximum line length.
  Page reads open one segment at a time relative to the pinned directory.
  Simultaneous scans each have their own bounded working state.
- Total retained metadata includes each still-pinned generation. Legacy
  conversion and WAL-tail replay remain separate operations. Their existing
  frame scanner holds at most one `store.MaxBatch` payload and its decoded
  rows. A caller combining the operations must add its tail indexing and
  query output buffers to the memory budget.
