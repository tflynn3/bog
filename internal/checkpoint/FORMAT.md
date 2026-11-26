# Offline checkpoint contract

The legacy scanner reads `BOGWAL01` and `BOGCMT01`; WAL integers are
little-endian. Each committed frame has a 16-byte header containing payload
length, its complement, CRC32C of the payload, and CRC32C of the preceding
header bytes. The payload uses the historical store decoder. The marker's offset
must land exactly after a complete frame. Bytes beyond it are uncommitted and
ignored. Row ordinals start at zero and increase in append order across frames,
including identical rows. A complete frame is visible as a batch.

## Manifest version 1

All integers are unsigned little-endian. The entire record is checksummed with
CRC32C (Castagnoli), excluding the final checksum itself:

| Offset | Size | Meaning |
| --- | --- | --- |
| 0 | 8 | ASCII `BOGMAN01` |
| 8 | 4 | Version, 1 |
| 12 | 16 | Nonzero opaque WAL generation |
| 28 | 8 | Covered committed frame-end offset, 8 through 2^63-1 |
| 36 | 8 | Next zero-based append ordinal |
| 44 | 4 | Segment count, at most 4096 |
| 48 | 16 × count | Distinct nonzero opaque segment identities |
| 48 + 16 × count | 4 | CRC32C of all preceding bytes |

The exact size is 52 + 16 × count bytes, at most 65588 bytes. No trailing
bytes are allowed. At offset 8 the ordinal and segment count must be zero. A
magic-only WAL therefore has a valid empty segment set, but still requires a
nonzero generation. Each identity names one immutable `BOGQSG01` segment; the
publisher and opener must agree on a safe identity-to-filename mapping. IDs are
not checksums. The codec does not prove that the offset is a frame boundary, the
ordinal matches the covered rows, or the segment set represents them. Those are
publisher and opener obligations. File readers must bound file size before
allocating or reading the record. Unknown versions, invalid fields, duplicate
identities, truncation, checksum failures and trailing data are errors.

Legacy WALs have no generation field; the handoff must establish a durable
unique generation without confusing a replacement WAL with the original. On
reopen validate the generation against the pinned WAL, validate the covered
boundary, and resume at that boundary with the recorded next ordinal. Replay
only the uncovered committed suffix, preserving each row exactly once. Missing
referenced files or corrupt committed data fail closed.

`BOGQSG01` is reserved for queryable segments; `BOGSEG01` belongs to the archive
format. Segment metadata and blocks must be versioned, checksummed, little-endian,
and include sparse time bounds. Validate lengths and offsets against file size
and configured limits before allocation or reads. Timestamp encoding must cover
the entire supported range; preserve timestamp, append ordinal and row identity,
including duplicates and backfill, across queries and snapshot pagination. Exact
literal matching and half-open time ranges remain unchanged.

The scanner proposal required eager verification of every referenced block
before any segment data became visible. This contract changes that rule: validate
segment metadata on open and verify each block before returning any of its rows.
Full verification is reserved for export and scrub; any encountered corruption
fails closed. The opener must not return rows from a corrupt block.

Under the pinned directory lock, the publisher must sync staged segments, install
their names and sync their directories, then sync and atomically publish the
manifest and sync its directory before switching readers or allowing reopen to
skip the covered WAL prefix. Retain old inputs until publication is durable and
existing readers release them. Uncertain publication stops progress; on reopen
the authoritative manifest determines the segment set. Recovery must be
idempotent and verify orphan status before cleanup. Publication, segment readers,
bounded tail replay, memory proofs, and Linux crash and performance qualification
are outside this codec.
