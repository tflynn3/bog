# Offline segment prototype, version 1

This package is an offline conversion building block, not a tiered server or a
published catalog. Do not remove the WAL or marker after conversion. The
caller must stop Bog and hold the pinned data-directory lock during conversion;
`OpenLegacy` does not acquire or validate that lock. Staged output must be
discarded on any error.

All integers are little endian. A segment starts with a 24-byte header: eight
bytes `BOGSEG01`, the uint64 committed WAL end offset, a CRC32C of the first
16 bytes, and four reserved zero bytes. The remainder consists of the exact
committed WAL frames after the `BOGWAL01` magic. Each frame has the legacy
16-byte header (payload length, its bitwise complement, payload CRC32C, header
CRC32C), followed by its original NDJSON batch. The end offset is relative to
the source WAL and must equal eight plus the length of all segment frames.
Frame order and row order are unchanged. Retaining original timestamps avoids
nanosecond-since-epoch overflow across the supported year range.

The decoder checks file length, frame lengths and checksums before reading or
allocating payloads. It reuses a buffer capped at the 4 MiB maximum batch size;
`store.Decode` additionally allocates decoded rows and strings for one batch.
The callback must not retain its payload without copying, and must discard all
results if any subsequent frame fails validation. No time index, generation
identifier, manifest, crash-safe publication, streaming merge, or cold storage
is implemented here. Recovery memory and end-to-end durability have not been
established for a tiered system.
