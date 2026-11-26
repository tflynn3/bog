# Segment v1 (experimental)

This codec is a building block, not a tiered server. There is no manifest,
legacy importer, cold backend, reclamation, or recovery integration yet. Do
not use these files as durable storage until those pieces are implemented.

All integers are little-endian. The 16-byte header contains `BOGSEG01`, a
32-bit version (1), and CRC32C of its first 12 bytes. Each 56-byte block header
contains `BLK1`, a 32-bit payload length, a 64-bit append sequence, inclusive
minimum and maximum timestamps (signed 64-bit Unix seconds and unsigned 32-bit
nanoseconds each), the payload CRC32C, eight reserved zero bytes, and CRC32C of
the first 52 header bytes. The payload is one complete Bog NDJSON batch in its
original byte order. A 24-byte footer contains `BOGEND01`, the 64-bit block
count, four reserved zero bytes, and CRC32C of the first 20 bytes. Sequences
are positive and contiguous within a segment. Per-block time bounds permit
pruning but never replace row-level filtering.

A reader checks remaining length and the 4 MiB batch limit before allocation,
verifies checksums and decodes each batch, and checks bounds against decoded
rows. The callback can see frames before the footer and later blocks have been
validated; callers must stage results and only publish them after successful
completion. The raw decode buffer is at most one batch; decoded row memory and
callback state are additional and depend on the batch and caller. This does not
establish a bound for full recovery or metadata growth.

A future catalog must identify the WAL generation and exact covered committed
frame boundary. Under the pinned directory lock it must sync and install all
segment names and sync their directories before syncing and atomically replacing
the manifest and syncing its directory. Readers may switch only after that
point; old inputs must remain until readers release them. An uncertain rename
or directory sync must stop writes, and recovery must reject missing or corrupt
referenced data and verify orphan status before deleting anything. A cold
migration must verify the uploaded object and durably publish its location
before local reclamation. These rules are not implemented by this codec.
