# Offline checkpoint version 1

The legacy scanner reads `BOGWAL01` and `BOGCMT01` under the pinned directory
lock, checks every committed frame, and leaves both files untouched. Row ordinals
are zero-based append positions, including duplicates.

A published `bog.checkpoint` is a JSON envelope containing a version 1 JSON
payload and the SHA-256 of its exact payload bytes. It records the SHA-256 of the
WAL bytes through the covered frame boundary, that boundary, the next append
ordinal, and up to 1024 immutable segment names, sizes, and SHA-256 digests. The
prefix digest binds this offline checkpoint to those bytes; it is not a unique
WAL generation identifier. Each segment is one version 1 checksummed block as
described in BLOCK_FORMAT.md. Its inclusive minimum and maximum timestamps are
sparse time bounds. Rows within each block are sorted by timestamp then ordinal;
blocks may overlap. The reader merges them in that order.

Files are created exclusively with random names and synced, then the directory is
synced before the manifest is written and synced. The manifest is renamed over
the authoritative name and the directory synced again. An error after rename is
an uncertain publication and requires reopening. Old files are retained; orphan
cleanup is not implemented. Reopening validates the manifest, source prefix,
frame boundary and committed WAL, and every referenced segment before exposing
rows. Missing or corrupt referenced files fail closed. Unreferenced files have no
meaning and are ignored. This offline API does not integrate suffix replay or
reader switching with the live store.

The builder retains at most one decoded legacy frame, its sorted row references,
and one encoded block, plus at most 1024 metadata entries. The reader retains at
most 1024 decoded blocks plus row/string overhead and one temporary block buffer;
manifest input is capped at 256 KiB. These are fixed limits, but may be large.
Checkpoints exceeding the segment limit fail; external merge/compaction and live
recovery remain future work.
