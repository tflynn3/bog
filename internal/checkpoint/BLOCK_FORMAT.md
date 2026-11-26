# Queryable checkpoint block, version 1

This standalone codec is intended for future `BOGQSG01` segments. It does not
assemble or publish segments. All integers are little-endian; signed seconds are
64-bit two's-complement Unix seconds. Times are UTC instants represented by seconds
and a 32-bit nanosecond fraction in [0, 1,000,000,000). Rows cover the store's
range from year 0001 through the nanosecond before 9999-12-31T23:59:59.999999999Z;
the latter instant is reserved as an exclusive query end.

The 44-byte header contains, in order: version u32 (1), row count u32, payload
length u32, minimum timestamp (i64 seconds, u32 nanoseconds), maximum timestamp
(same), CRC32C of the payload u32, and CRC32C of the preceding 40 header bytes
u32. Bounds are inclusive and are the exact extrema, regardless of row order.
Each payload row contains timestamp (12 bytes), zero-based append ordinal u64,
UTF-8 line length u32, and that many line bytes. Rows remain in supplied order;
duplicates and ordinals are retained. The maximum block size is 4 MiB plus 68
bytes, including the header, sufficient for any row decoded from a legacy frame.
Blocks are nonempty.

Before returning any rows, the decoder validates the version, header and payload
checksums (CRC32C/Castagnoli), total size, count, lengths, timestamps, exact bounds,
UTF-8 and exact payload consumption. It checks lengths before allocation or slicing.
Future segment readers must also validate segment metadata and every referenced
block before making any segment data visible; a valid individual block alone is
not authorization to expose a partially verified segment.
