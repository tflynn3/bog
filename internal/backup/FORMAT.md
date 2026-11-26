# Backup bundle version 1

The recovery point identifies one v1 checkpoint manifest by the SHA-256 of
its exact JSON payload and a committed end in one pinned legacy WAL. Offsets
are absolute from the start of that WAL, including the eight-byte
`BOGWAL01` magic. Export checks every covered frame, the next ordinal at
the covered boundary, and the covered prefix digest from the manifest. The
caller must capture the manifest identity, boundary and committed end together
and keep the readers valid until export returns. The v1 WAL prefix digest
identifies the covered bytes. It is not a writer epoch.

The bundle is a directory with exactly these regular files.

| Name | Contents |
| --- | --- |
| `bog.backup` | Fixed 128-byte bundle metadata |
| `bog.checkpoint` | Original [v1 checkpoint manifest envelope](../checkpoint/FORMAT.md) |
| Each segment named in the manifest | Original [v1 checkpoint block](../checkpoint/BLOCK_FORMAT.md) |
| `bog.wal.tail` | Exact legacy frames in `[CoveredOffset, CommittedEnd)`, starting at file offset zero |

The metadata layout is fixed. Integers are unsigned little-endian on disk.
Both offsets must fit in signed 64-bit positions.

| Byte range | Field |
| --- | --- |
| `[0,8)` | ASCII `BOGBAK01` (format version 1) |
| `[8,40)` | Manifest payload SHA-256 |
| `[40,48)` | Checkpoint covered offset |
| `[48,56)` | Checkpoint next append ordinal |
| `[56,64)` | Captured committed end |
| `[64,96)` | Tail SHA-256, including SHA-256 of the empty string for an empty tail |
| `[96,128)` | SHA-256 of the preceding 96 metadata bytes |

A tail frame contains a 16-byte header (payload size, its bitwise complement,
payload CRC32C, CRC32C of the first 12 header bytes) followed by one legacy
ingest payload. Verification checks the exact tail size and digest, complete
frames, frame checksums and ingest decoding. It checks every segment's size,
SHA-256, block CRC32Cs, timestamp bounds and row order, plus exact coverage of
`[0, NextOrdinal)` across all segments. The manifest carries the digest of
the omitted covered prefix. That digest is checked against the source WAL
during export. Bundle checksums detect corruption and do not authenticate a
bundle from an untrusted source.

## Recovery and publication

Export and restore use the same layout. A restore is self-contained and can
be opened with `OpenRestored`. This is an engine image whose tail requires
checkpoint-aware replay. Starting the legacy WAL writer on the image is not
supported.

The destination parent must exist on a trusted local filesystem that supports
advisory locks, atomic rename, and directory sync. Export and Restore share an
exclusive publication protocol scoped to the opened parent and exact
destination basename. The caller controls the pathname against other
filesystem writers. A waiting retry honors context cancellation.

New staging entries are private siblings in the reserved `.bog-backup-v1-`
namespace. The persistent `.bog-backup-v1-lock` file coordinates publishers
in the same parent and allows the WAL owner to hold its directory lock
concurrently. A retry reclaims an abandoned copy only after validating its
claim and directory identity and acquiring the stage directory's lock.
Prefix-only entries from older releases and entries for other destinations
are preserved. The lock file and staging metadata live outside the v1 image.

Export and Restore sync the staged files and directory before publication.
Restore also verifies the staged copy. On Linux one directory-relative rename
installs the image over an absent destination or an empty placeholder, and
the publisher syncs the parent before returning success. A populated
destination is preserved. An uncertain rename or a sync failure after rename
returns `ErrPublicationUncertain`. Reopen and verify the destination before
retrying. A later attempt can clean up abandoned staging metadata beside a
completed image without modifying the image.
[ADR 0009](../../docs/adr/0009-backup-staging-recovery.md)

## Working memory

Let `S ≤ 1024` be the number of segments and `B ≤ 4 MiB + 68 bytes` the
largest encoded block. Verification, export and restore each use at most
one block buffer, bounded by `B`. Ordinal coverage uses one bit per covered
row, at most `ceil(S × floor((B − 44) / 24) / 8)` bytes (about 21.34 MiB
at the v1 limits). This bound holds even for shuffled ordinal membership
across segments. Manifest input is capped at 256 KiB with `O(S)` parsed
metadata. Copying uses one streaming buffer. WAL validation and replay use
one payload buffer up to 4 MiB and the decoding state and rows of one frame.
These operations retain no full checkpoint or WAL history in memory.

The restored query reader retains the segment metadata and sparse time bounds.
Each page holds at most one 4 KiB buffer and one cursor per overlapping run,
with a heap for their ordered merge and one transient checkpoint line up to
the block limit. It scans the bounded WAL suffix once per page and keeps at
most `limit + 1` matching tail keys and frame locations in a heap. The merge
rereads frames only for tail rows that enter the result, caching the most
recently decoded frame. Both passes use bounded single-frame decoding. The
result holds at most `limit` row bodies, and tail lookahead uses the matching
key alone. The tail scan and cursor walk repeat for each continuation.
Whole-frame replay can also be used independently of queries.
