# Offline checkpoint prototype

This package operates on a stopped legacy data directory. It takes the same
exclusive directory and WAL locks as the service. Neither input is rewritten.
The checkpoint is independent of service startup and does not reclaim WAL data.

Version 1 uses little-endian integers. `BOGSEG01` is followed by records: a
32-bit body length, signed Unix seconds encoded in 64 bits, nanoseconds (32
bits), append ordinal (64 bits), line length (32 bits), line bytes, and CRC32C
of the body. Lengths are checked before allocation. Seconds plus nanoseconds
represent the full legacy timestamp range. Records are ordered by timestamp,
then ordinal; duplicate lines remain separate. There is no sparse time index in
this prototype, so a query scans the entire segment.

The JSON manifest is wrapped in an envelope with a SHA-256 of its exact JSON
payload. Version 1 records the segment name, size and SHA-256, WAL device and
inode, a digest of the last up to 64 bytes at the covered boundary, the exact
committed byte offset, and the next append ordinal. The WAL has no generation
identifier, so device/inode and boundary digest are used as a local identity;
this is not a portable backup identifier. Open validates the segment digest and
replays only complete committed frames after the offset. A missing or corrupt
referenced segment or committed tail is an error. Query uses half-open bounds
and literal substring matching. Callers must retain the ordinal for pagination
across equal timestamps.

A run is sorted in chunks of approximately 8 MiB plus a decoded WAL frame;
merge fan-in is two with 64 KiB I/O buffers and at most two decoded records.
The run-level table is fixed at 64 entries. Open verifies the segment with a
64 KiB buffer, then holds the decoded tail in memory, capped by the caller's
WAL-byte limit. Decoded rows and Go allocation overhead can exceed encoded
input size; this is not a hard RSS limit. The largest accepted frame is 4 MiB
plus its decoded rows. Callback output is caller-owned and not included.

Each temporary run is synced before use. The final name is installed and the
directory synced before a synced manifest is renamed over the authoritative
manifest and the directory synced again. Failure during manifest rename or the
following directory sync returns ErrUncertain; stop and reopen. Old segments
and WAL files are deliberately retained. Orphans are not cleaned up. The
reader holds the locks until closed. This version has no automatic cleanup,
crash injection harness, metadata paging, or measured Linux resource guarantee.
