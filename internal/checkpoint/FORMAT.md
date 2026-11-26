# Offline checkpoint contract (proposed)

This scanner reads the existing `BOGWAL01` WAL and `BOGCMT01` commit marker;
it does not publish checkpoints. WAL integers are little-endian. Each committed
frame has a 16-byte header containing payload length, its complement, CRC32C of
the payload, and CRC32C of the preceding header bytes. The payload is decoded by
the historical store decoder. The marker's offset must land exactly after a
complete frame. Bytes beyond it are uncommitted and ignored. Row ordinals start
at zero and increase in append order across frames, including identical rows.

A future versioned, checksummed manifest must identify the WAL generation, the
exact committed frame boundary covered, the next append ordinal, and every
referenced immutable segment by identity. Legacy WALs have no generation field;
the handoff must establish a durable unique generation without confusing a
replacement WAL with the original. Reserve `BOGQSG01` for queryable segments;
`BOGSEG01` belongs to the archive format. Future segment metadata and blocks must
be versioned, checksummed, little-endian, and include sparse time bounds. Lengths
and offsets must be validated against file size and configured limits before
allocation or reads. Timestamp encoding must cover the store's entire supported
range, and ordering must retain timestamp, append ordinal and row identity.

Under the pinned directory lock, a future publisher must sync staged segments,
install their names and sync their directories, then sync and atomically publish
the manifest and sync its directory before switching readers or allowing reopen
to skip the covered WAL prefix. Retain old inputs until publication is durable
and existing readers release them. An uncertain publication stops progress; on
reopen the authoritative manifest determines the segment set and only the
uncovered committed WAL suffix is replayed. Missing referenced files or corrupt
committed data fail closed. Verify orphan status before cleanup. Segment metadata
and all referenced block checksums must be verified before any data from the
segment becomes visible; a future lazy verification design must preserve that
fail-closed guarantee. Publication, segment readers, and memory and Linux crash
qualification are outside this scanner.
