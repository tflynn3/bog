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
