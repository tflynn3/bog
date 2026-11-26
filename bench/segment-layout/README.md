# Checkpoint block layout benchmark

Run with Go 1.27 on Linux from the repository root. Use a clean checkout and a
scratch directory on the filesystem you want to measure.

```sh
go -C bench/segment-layout run -buildvcs=true . -smoke
go -C bench/segment-layout run -buildvcs=true . -dir /path/to/scratch -trials 21 > layout.json
go -C bench/segment-layout vet ./...
go -C bench/segment-layout test -race ./...
```

The JSON records Go build and VCS metadata, kernel, architecture, CPU count,
filesystem magic, cache policy, fixture, and sample count. Smoke mode fixes the
seed and uses 6,005 rows and one trial per query by default. Its fixture, row
counts, block counts, and byte counts are deterministic. The full fixture has
120,005 rows and a maximum-size accepted batch containing a single oversized line.
Both alternate batch sizes and include duplicate rows, timestamp ties, backfill,
repetitive and high-entropy text, and the supported timestamp endpoints.

All variants use the same timestamp/append-ordinal sorted, 256 KiB-target
partition. Oversized rows occupy their own block. A separate append-order
reference scan checks every row, including the ordinal and exact line bytes,
before timing. Raw, LZ4 block, and default-level Zstd each run with and without
a per-block 2 KiB Bloom filter of overlapping four-byte line substrings. Blocks
with ineffective compression fall back to raw. Short literals bypass pruning,
and every candidate is checked with the exact literal and half-open time range.

The benchmark envelope uses little-endian fields and starts with the versioned
magic, block count, codec, summary flag, and CRC32C. Each record holds inclusive
time bounds copied from the checkpoint block, raw and stored lengths, payload
CRC32C, metadata CRC32C covering the optional summary, and the stored block.
A query verifies metadata before pruning and checks payloads that it reads.

Retained bytes include every envelope and summary. Rewrite bytes count actual
bytes written and synced for one complete repack of the same history. Logical
blocks read count payload reads. Cold p50 and p95 use nearest rank and time the
full scan after per-file `POSIX_FADV_DONTNEED` on clean pages. The advice is
best effort, and OS readahead and device caches can still affect latency.
Peak decode bytes report the maximum observed Go heap allocation during a
single-block decode in the untimed pass, including its input, output, rows and
fresh decoder. This estimates decode working memory from allocation traffic.
Already pooled buffers, stacks, filesystem caches and fixture memory are outside
this metric.
