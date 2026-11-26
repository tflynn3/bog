# Offline checkpoint prototype, version 1

This package is independent of service startup. It holds exclusive locks on the
pinned data directory and the WAL while building or scanning. It does not modify
or reclaim the legacy WAL or commit marker. A failed build before publication
leaves the previous manifest authoritative. Following a manifest rename, a
directory sync failure is reported as `ErrUncertain`; stop and reopen rather
than retrying on the assumption that either version won. Old segments are not
removed, including possible orphans.

All integers are little endian. The manifest begins with `BOGMAN01`, a uint32
JSON length and a CRC32C of the JSON. Its bounded JSON describes version 1, a
single immutable segment name, byte size and SHA-256, the WAL device and inode,
the exact committed byte offset, and the next append ordinal. The segment starts
with `BOGSEG01`; each record has a uint32 length, CRC32C of its payload, and a
payload of signed Unix seconds (64 bits), nanoseconds (32 bits), append ordinal
(64 bits), line length (32 bits), and line bytes. Seconds and nanoseconds retain
the supported years 1 through 9999 without Unix-nanosecond overflow. Records
sort by timestamp then append ordinal; duplicates remain separate. Segment
lengths and checksums are checked before use. The whole segment digest is checked
before scanning. WAL frames use the legacy format and are validated in full.

Sorted runs are made from chunks and merged two at a time, so merge fan-in is
two and run-name metadata grows logarithmically with run count. The chunk size
is a sort target, not an RSS limit: peak also includes one decoded maximum
4 MiB WAL frame and its rows, Go allocation overhead, two current merge records,
64 KiB buffers, and logarithmic run metadata. Scan retains the decoded committed
WAL tail, bounded by the caller's byte limit but potentially larger in memory
than its encoded size, plus one maximum frame, segment buffers and records. It
reads only the WAL suffix after the stored offset, but hashes the entire segment
at each scan. This format has no sparse time index or page cache; scans are
linear. It is not a production recovery implementation or a proven RSS bound.
It assumes the locked legacy WAL inode is append-only and does not detect a
same-inode rewrite of already checkpointed bytes. No orphan cleanup is attempted.
