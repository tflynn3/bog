# ADR 0003 — WAL capture and restored-image startup

**Status** — Proposed extension to [ADR 0001](0001-single-writer-retained-history.md).
The WAL owner reviews the startup dispatch, linearization point, and pin
lifetime before implementation. Agree on the consumer seam with the query
owner after #52 and wire the storage adapter after bounded pins land.

## Restored-image startup

The [v1 backup engine](../../internal/backup/FORMAT.md) restores an image
with `bog.backup`, a checkpoint manifest and its segments, and
`bog.wal.tail`. The suffix begins at byte zero of the tail file and starts
at the checkpoint's covered offset and next ordinal in the logical WAL.
The covered WAL prefix is omitted. Today `OpenRestored` can validate and
read this layout offline.

Startup must classify the layout under the exclusive lock on the pinned
data directory, before opening or bootstrapping the legacy WAL, truncating
files, cleaning up staging files, or opening the HTTP listener. The presence
of `bog.backup` or `bog.wal.tail` requires restored-image handling. Missing
image components, malformed metadata, unsafe files, or coexistence with
legacy authoritative files (`bog.wal` or `bog.commit`) must fail closed. A
startup path that cannot safely open a restored image must reject it with an
operator-facing error and preserve the image. A directory eligible for
legacy bootstrapping must satisfy the legacy layout and bootstrap checks.
Until the startup guard ships, use the offline reader on restored images.

The checkpoint-aware path must validate the image metadata, manifest and
segment integrity and ordinal coverage, and the exact committed suffix,
including its length, digest and complete frame boundaries. Rebuild the tail
from the checkpoint boundary and next ordinal through the captured committed
end. Publish the recovered query view before listening. Keep the directory
lock for the lifetime of the serving process. Any path enabling writes must
first establish a durable, recoverable append state for that same view under
the same owner.

## Capture and lifetime

The WAL owner linearizes capture against both durable append publication
and generation activation. At the linearization point it obtains a pin on
the active validated generation and fixes the WAL identity or epoch, the
checkpoint's covered frame boundary and next ordinal, and the matching
durable committed end. Capture sees a whole group only once its commit is
durable and its tail is query-visible. The covered offset must be a complete
frame boundary at or below the captured end. The generation must cover that
WAL prefix. The prefix digest validates the covered bytes and the owner binds
the identity of the pinned WAL separately. A poisoned owner rejects new
captures. Later appends and generation swaps leave the captured view fixed.

The caller gets an opaque handle with bounded lifetime and budgeted pin
ownership. Paging and export hold operation references while reading. Export
uses the captured generation and exact committed suffix `[coveredOffset,
committedEnd)`, with metadata derived from the captured bounds. Release is
idempotent. Expiry or release prevents new uses and lets in-flight references
drain. Cancellation or failure during acquisition releases any acquired
references, and every operation releases its reference on exit. Reclamation
and shutdown keep the pinned generation and WAL readable until the last
reference drains.

## Consumer paging contract

HTTP owns admission, cursor authentication, and response framing. Its
consumer-owned storage seam accepts an opaque capture, a filter, an
exclusive resume position and bounded row and work limits. It returns
ordered rows with an opaque resume position for each row and indicates
whether another match exists. The positions represent `(UTC timestamp,
append ordinal)` order within the capture. This lets HTTP apply its byte
cap using the position of the last row it actually emits, without rescanning
storage. When the byte cap excludes every row and a match remains, return a
size error without advancing. A completed scan can signal exhaustion. Work
exhaustion, cancellation and read or validation errors must preserve the
caller's prior position. HTTP authenticates and binds continuations to the
capture and canonical filter and reports missing or expired captures
explicitly. The composition root wires the adapter and keeps HTTP-specific
types at the consumer boundary.

## Acceptance criteria

- Exercise capture during concurrent appends and generation activation.
  Verify that paging and export observe the same complete committed groups
  and that later appends and generation swaps preserve the captured view.
  Exercise cancellation, expiry, repeated release, reclamation and shutdown
  with outstanding pins and in-flight operations.
- Walk the captured view through the production HTTP path and compare it
  to an independent append-log model. Export the same capture, verify and
  restore it into an isolated directory, then start the shipped command on
  the image and repeat the walk with fresh cursors. Compare ordered rows and
  termination across page sizes and byte caps, including a cap that trims a
  storage page. Cover overlapping checkpoint and tail ranges, equal
  timestamps, duplicates, backfill, sparse and absent literals, and a write
  after capture that a fresh live walk can see.
- Verify that unsupported image startup, damaged or incomplete images and
  ambiguous layouts fail before listening and leave files intact. Exercise
  corrupt segments, missing files, invalid suffix boundaries and checksums,
  and interrupted publication. A verified image must survive startup and
  restart with the same captured rows. An interrupted restore must leave a
  recoverable publication state that can be checked before retry.
- Run the #3 Linux durability verifier and #32 capacity fixture on the same
  revision-pinned release candidate when qualifying the cutover. Retain the
  legacy capacity baseline for comparison.
