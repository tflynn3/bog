# Checkpoint block layout benchmark

Run on Linux from this standalone Go module. Choose a data directory on the
filesystem or device under test. The runner creates and cleans up its own
temporary subdirectory.

```sh
cd bench/segment-layout
go run . -smoke
go run . -dir /path/to/data -fixture mixed -runs 21 -revision "$(git rev-parse HEAD)"
go run . -dir /path/to/data -fixture entropy -runs 21 -revision "$(git rev-parse HEAD)"
```

Each run emits one JSON report. Pass the revision of the measured source when
building without Git metadata. By default the executable uses Go's embedded
revision and dirty status. Smoke mode fixes the workload at 64 ordinary
batches and one sample per query. Its rows, block counts, and byte counts are
deterministic for the selected profile and dependency versions. Timings vary.

The mixed fixture cycles ordinary batch sizes of 1, 7, 64, and 256, using
mostly repeated log templates with some seeded random IDs. The entropy profile
adds 256 seeded random bits per ordinary line as hex. Both include the timestamp
extrema, a maximum-frame-sized singleton, and late backfill with duplicate
ties. The query suite covers dense, rare, absent, short UTF-8 literals of one
through three bytes, the oversized line's tail, and half-open time windows.
Each measured result is compared row-for-row with a stable timestamp sort of a
literal scan over the original append input. Both timestamps and append
ordinals participate in the equality check.

All six layouts pack the same append rows at a 64 KiB target and sort rows
within blocks by timestamp and append ordinal. Oversized rows get their own
blocks. Payloads use Bog's version 1 checkpoint codec directly. The benchmark
container stores a versioned, CRC32C-checked header and an index in front of the
payloads. Index entries have inclusive time bounds, raw and stored lengths, an
optional 1 KiB four-byte Bloom filter, and a CRC32C. LZ4 and single-threaded
Zstd compress each block independently, storing the raw block if compression
would expand it. The filters only skip blocks. Every returned line is matched
using the original literal.

- **Retained bytes** include the complete initial file, including index and
  summary overhead. **Rewrite bytes** count the full output of rebuilding
  after appending one late duplicate, with both generations present. The
  rewritten file is also compared with the oracle on the all-rows query.
  These are file bytes and exclude filesystem or device write amplification.
- **Logical blocks read** counts payload reads. Index entries are read for every
  query. Linux receives per-file `FADV_RANDOM` and `FADV_DONTNEED` advice before
  timing. The timed interval includes metadata and payload I/O, decoding,
  filtering, and sorting. Advice is best effort and operates on the Linux
  page cache. Storage-controller or host caches may still affect samples.
- **Peak decode bytes** is the largest accounted block working set in the
  query. It includes the stored payload, a separate expansion buffer when
  needed, decoded row structs, and copied line bytes. It excludes index bytes,
  codec-library internals, allocation overhead, and accumulated query results.
  It measures block decode buffers independently of process RSS.

The standalone container and rewrite model are for comparing checkpoint
layouts. Bog's production checkpoint publication and recovery are outside the
benchmark. Run `go vet ./...` and `go test -race ./...` from this directory
for the portable summary and format tests. On Linux the same commands also
cover the end-to-end fixture with the production codec.
