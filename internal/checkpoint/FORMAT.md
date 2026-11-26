# Offline checkpoint prototype

The exclusive lock pins the legacy directory and WAL while building or reading.
The WAL and commit marker are never modified. The authoritative manifest is
`checkpoint.manifest`; files not named by it are retained, including old
segments and interrupted-build files. There is no automatic orphan cleanup.

Manifest version 1 is a JSON envelope containing a JSON data object and its
SHA-256. It identifies one immutable segment, its byte length and SHA-256,
the WAL device and inode, exact committed frame boundary, SHA-256 of the last
up to 64 bytes at that boundary, and the next append ordinal. The identity and
boundary fingerprint detect ordinary replacement and truncation; they are not a
cryptographic generation identifier against deliberate in-place WAL rewriting.

Segment version 1 starts with `BOGSEG01`. Each record has a little-endian
32-bit body length, signed Unix seconds in two's complement, 32-bit nanoseconds,
64-bit append ordinal, 32-bit line length and line bytes, followed by CRC32C of
the body. Records are sorted by timestamp and ordinal. Lengths are bounded
before allocation. The manifest checksum covers metadata and the segment digest
covers the entire file, including its header and truncation. Timestamp seconds
and nanoseconds represent the complete legacy year 1 through 9999 range.

Build forms approximately 8 MiB sorted runs and merges two at a time, keeping
at most 64 run names. A legacy frame is capped at 4 MiB, but decoded rows and
Go allocation overhead add to the working set. Reopen hashes the segment with
a 64 KiB buffer and holds the caller-capped WAL tail decoded and sorted in
memory. Query uses one segment record and the tail; callers control output
buffering. This prototype has no sparse time index, paginated snapshot API,
measured RSS guarantee, or automatic reclamation. Tail byte caps are not a
precise heap cap because decoded rows and slice overhead expand the input.

Publication syncs the segment file, renames it, syncs the directory, then syncs
and renames a staged manifest and syncs the directory again. A rename or final
directory-sync error is reported as uncertain and callers must stop and reopen.
Existing inputs and prior segment generations remain available throughout.
