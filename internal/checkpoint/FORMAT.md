# Offline checkpoint version 1

The legacy scanner reads `BOGWAL01` and `BOGCMT01` under the pinned directory
lock, checks every committed frame, and leaves both files untouched. Row ordinals
are zero-based append positions, including duplicates. Concurrent scans have
independent decode state. Close stops accepting operations and waits for active
operations and visitors to finish before releasing the lock. Visitors must
return before calling Close on the same handle.

A published `bog.checkpoint` is a JSON envelope containing a version 1 JSON
payload and the SHA-256 of its exact payload bytes. It records the SHA-256 of the
WAL bytes through the covered frame boundary, that boundary, the next append
ordinal, and up to 1024 immutable segment names, sizes, and SHA-256 digests. The
prefix digest binds this offline checkpoint to those bytes; it is not a unique
WAL generation identifier. Each segment is one version 1 checksummed block as
described in BLOCK_FORMAT.md. Its inclusive minimum and maximum timestamps are
sparse time bounds. Rows within each block are sorted by timestamp then ordinal;
blocks may overlap. The reader merges them in that order.

Each segment is staged under an exclusive temporary name, set to mode 0600,
synced, and linked to its final name. Once the staging links are removed, the
directory is synced. The manifest is staged with the same mode, synced, renamed
over the authoritative name, and followed by a second directory sync. An error
from the rename attempt or final directory sync makes publication uncertain and
requires reopening. Old files and any unreferenced outputs from interrupted
publications are retained. The reader validates the authoritative manifest, WAL
prefix and committed frames, and every referenced segment. It syncs the directory
before exposing rows. Missing or corrupt referenced files fail closed. Uncovered
replay starts from the validated frame boundary and its next append ordinal.

The builder retains at most one decoded legacy frame, one bounded block of sorted row references,
and one encoded block, plus at most 1024 metadata entries. The reader retains at
most 1024 decoded blocks plus row/string overhead and one temporary block buffer;
manifest input is capped at 256 KiB. Ordinal validation also uses one bit per
row, bounded before allocation by the sum of declared segment payload sizes divided
by the minimum row size (at most 1024 times (MaxBlockSize - 44) / 24 bits).
These are fixed limits, but may be large.
Checkpoints exceeding the segment limit fail; external merge/compaction and live
recovery remain future work.

# Retained checkpoint version 2

A v2 generation uses the same envelope, immutable blocks, and
`covered_offset`, `next_ordinal`, and `segments` fields as v1. The
payload sets `version` to 2 and carries an `advance` object in place of
`source_sha256`:

| Field | Binding |
| --- | --- |
| `generation` | SHA-256 of the exact predecessor manifest payload |
| `covered_offset` | Predecessor's logical covered byte offset |
| `next_ordinal` | Predecessor's first uncovered append ordinal |
| `consumed_sha256` | SHA-256 of the exact whole frames consumed by this advance |

Digests in the JSON records are lowercase hexadecimal. The covered offset
and next ordinal strictly advance. Logical offsets always include the
original eight-byte WAL magic. The manifest digest covers the exact encoded
payload and gives the installed file its name,
`bog-checkpoint-<64 lowercase hex digits>`. A backup carries the same
envelope under `bog.checkpoint`. Verification of a selected generation
uses its own manifest and blocks. The predecessor and consumed frames can
be reclaimed after the corresponding pins drain.

## Owner handoff

`Publisher.PublishRecoveryView` borrows the caller's pin and consumes a
sequential reader starting at the predecessor's covered offset. The
publisher checks the entire bounded captured suffix, including the
uncovered remainder, before offering the successor to the selector. The
publisher uses the configured `ReplayTailBytes` ceiling for the captured
range and charges new output through `StagingBytes`. The owner continues
accounting for the captured backing data.

The new blocks and manifest are synced and installed under the existing
publication lock. The intent records the candidate manifest's identity
alongside the identities of new blocks. The selector callback receives
the generation and the captured committed bounds and suffix digest at
the new cutoff. The owner checks that the predecessor remains selected,
reconciles commits since capture under append synchronization, and
durably selects the manifest and matching writable tail together. The
owner keeps required files and pins through that decision. A selector
error leaves the attempt pending and requires recovery before dependent
operations resume.

`Publisher.RecoverRecoveryView` uses the owner's selected view under
the publication lock. The owner validates the durable selector and tail
identity, syncs the selected record and tail as needed, and supplies
their pinned readers. Checkpoint validates the selected manifest, segment
coverage, and committed suffix before syncing the directory and cleaning
up attempt-owned files. Only a durably selected generation can be activated
or supplied to new captures. The owner manages retirement of previously
selected generations and tails after their pins drain.

Release Verification can reproduce the two fixture cycles with
`go test ./internal/backup -run TestPinnedTailRecoveryTwoCycles` and
exercise both selection outcomes and interrupted cleanup with
`go test ./internal/checkpoint -run TestRecoveryViewInterruptedCleanup`.
The fixtures supply the owner-side selector and synced appends. See
[ADR 0035](../../docs/adr/0035-tail-only-retained-recovery-contract.md)
for the composed runtime acceptance criteria.
