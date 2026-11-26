# ADR 0001 — Single writer and retained-history snapshots

**Status** — Accepted design for the retained-history cutover. The current
runtime is recorded in [SYSTEM.md](../architecture/SYSTEM.md).

## Context

The shipped server recovers the committed WAL into memory and queries that
store. The staged checkpoint publisher and history catalog already validate
immutable segments and manage generation references. A history catalog
acquisition pins a generation by itself. A live query or backup also needs
the exact committed end of the matching WAL. Reading these independently
allows a publication, append, or reclamation to interleave and produce a
mismatched snapshot.

## Decision

Keep one Linux server and one WAL owner for each data directory. The WAL
owner holds the directory lock and controls append order, durable committed
offsets, and the coordinated storage view. The composition root wires a
production retained-history adapter to that owner and the history catalog.
Checkpoint code owns format validation and durable publication. History
owns immutable segment reads, generation references, and safe reclamation.
Store owns row decoding and ordered tail data. HTTP owns admission,
authentication of cursor tokens, and response framing. Neither the query
handler nor the backup caller opens a second writer or infers storage state
from filenames.

Keep the paging interface at the consumer boundary and wire its storage
implementation in `cmd/bog`. WAL, checkpoint, history, and store have no
dependencies on HTTP or readiness. The adapter uses the WAL owner's
capture operation and the catalog's generation operations without making
the storage packages depend on the adapter.

### Capture and lifetime

The adapter exposes the following logical operations. Names describe the
contract and leave the Go API to the cutover.

| Operation | Contract |
| --- | --- |
| Capture | Return a handle for one coherent view. Atomically select and pin the active validated generation and the matching durable committed WAL end under the owner's synchronization. Bind the handle to the WAL identity, covered frame boundary, next ordinal, and an expiry. |
| Page | On that handle, return a bounded, ordered page and an exclusive continuation position when another match exists. Resume against the same capture. |
| Backup export | From that same kind of handle, export the pinned manifest and referenced segments, and the WAL bytes and commit metadata needed to reconstruct the exact committed view. |
| Release | Idempotently release the caller's ownership. In-flight operations keep their own references until completion. Retire expired captures and free pins when their last references drain. |

Capture linearizes against durable commit and generation activation. It fixes
which batches are query-visible at the capture point, so the WAL end and the
published tail agree. It validates that the checkpoint covers a complete
frame boundary at or below that end and matches the WAL prefix. The v1 WAL
prefix digest identifies the covered bytes. The owner also binds the actual
WAL identity or epoch so the digest cannot substitute for ownership of the
live file. The tail includes only complete frames in
`[coveredOffset, committedEnd)`, with its first row at `nextOrdinal`. Later
appends and generation swaps leave captured bounds unchanged.
If capture fails or is cancelled before returning a handle, release any
references acquired during the attempt.

A finite deadline and bounded count of outstanding captures limit how long
cursor walks can pin storage. An authenticated continuation carries or
references the capture identity, the canonical query parameters, and the
last returned key. Missing or expired captures produce an explicit expiry
result. Invalid or mismatched tokens produce validation errors. Neither case
implicitly starts a fresh walk. A pinned old generation remains readable
through a swap while the capture is valid. Completion, cancellation, and
errors release operation references. A cursor that can still be resumed
retains its capture until the walk releases it or its deadline expires.

### Paging

Merge the checkpoint runs and tail by `(UTC timestamp, append ordinal)`.
Use a half-open time range and exact, case-sensitive substring matching.
Keep duplicates distinct, including rows with equal timestamps and rows
written as backfill. Honor an exclusive continuation key and keep page
rows, encoded response bytes, scan work, and concurrent scans within
configured bounds. A next cursor advances only past rows returned in a
successfully prepared page. Exhausted work, cancellation, and corrupt or
unreadable data must leave the caller able to identify the failure without
advancing the cursor. Only a completed scan that can establish there are no
further matches may terminate a walk without a continuation.

### Publication, reclamation, and backup

Activate a generation after the referenced immutable files and the
authoritative manifest are validated and durably published. Keep the prior
recoverable view through uncertain publication and revalidate the
authoritative namespace before removal. Reclaim only files owned by this
protocol whose identity is confirmed and which are unreachable from the
authoritative view and every live query or backup pin. Drain readers before
reusing or deleting files and preserve unknown files.

Backup uses the same production capture as paging, with pins held through
the export. Export the manifest, its exact immutable segments, the matching
WAL data through the captured committed end (including the prefix if the
format requires it), and a marker or equivalent metadata for that exact
end. Do not copy a moving live marker or include writes beyond the captured
end. Verify file sizes and digests, the manifest/WAL binding, and complete
frame coverage before accepting a backup or restoring it. Restore into an
isolated destination, then open it through the shipped server and query it
through HTTP.

## Acceptance criteria

- The shipped command uses the retained-history adapter for live paging,
  recovery, and the backup capture for #6. Architecture fitness tests
  reflect the packages actually reached from that command.
- Concurrent append, checkpoint activation, query, and backup tests prove
  capture consistency. An acquired view survives a generation swap.
  Expiry, cancellation, and repeated release drain pins safely, and
  reclamation leaves all referenced files available.
- Compare an entire production cursor walk with a reference append-log
  model and with the same walk after restart and after #6 backup restore.
  Include an overlapping checkpoint and tail time range, equal timestamps,
  duplicates, backfill, sparse or absent literals, and varied page sizes.
  Use the same captured cutoff and compare ordered rows and page
  termination. A fresh query may see writes committed after the cutoff.
- Exercise missing or corrupt files, an invalid WAL boundary, interrupted
  publication, and an uncommitted tail. A damaged committed view fails
  closed. An interrupted operation must preserve a recoverable view and
  must keep acknowledged rows recoverable.
- Record bounded retained memory and tail replay under the #32 capacity
  load and durability and recovery under the #3 Linux verifier. Use the
  same revision-pinned Linux candidate binary for both gates and the
  production adapter for restore parity.
