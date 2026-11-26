# Offline checkpoint prototype

The builder holds the same pinned directory and WAL locks as the legacy writer.
It reads only the committed marker boundary, never changes the WAL or marker,
and validates a complete frame before admitting any of its rows. Append sequence
is a zero-based row ordinal; it breaks timestamp ties and retains duplicates.

Version 1 stores one sorted segment. Its magic is `BOGSEG01`. Each record is a
little-endian uint32 body length followed by signed Unix seconds (int64),
nanoseconds (uint32), ordinal (uint64), line byte length (uint32), line bytes,
and CRC32C of the body (uint32). Seconds plus nanoseconds cover the full UTC
range accepted by the legacy decoder. Lengths are checked before allocation.
The manifest is a size-capped JSON envelope containing a SHA-256 checksum of its
JSON data. Version 1 records the segment name, committed WAL offset, next ordinal,
WAL device/inode identity, and SHA-256 of up to 64 bytes ending at the boundary.
The identity is tied to this stopped local copy; copying or replacing the WAL
requires rebuilding the checkpoint. The boundary fingerprint is not a checksum
of the historical WAL; the segment is the authoritative historical representation.

Runs are sorted in chunks and merged two at a time. Segment data is synced before
its final name is installed; the directory is synced before the synced manifest
is renamed over the old manifest, then synced again. A failed manifest rename or
final directory sync is uncertain and requires stopping and reopening. Old
segments and source files are retained; there is no orphan cleanup. Readers hold
the lock for their lifetime and validate the entire referenced segment on open.
They cap the committed tail by caller-supplied WAL bytes and decode no earlier
frames. Query streams a merge of the segment and sorted tail in half-open time
ranges, using literal substring matching.

Memory is independent of historical byte count: an 8 MiB run target plus one
maximum 4 MiB frame and its decoded rows, merge fan-in of two records (each at
most one frame), three 64 KiB I/O buffers, at most 64 run names, and tail state
bounded by the supplied tail byte limit plus decoded-row and sort overhead. Go
allocation overhead and frame row density mean this is not a hard RSS limit.
The prototype has no sparse time index, paged metadata, bounded-RSS admission,
production integration, or automatic cleanup. Full segment validation makes
restart time scale with checkpoint history even though WAL replay does not.
