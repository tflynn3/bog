# Checkpoint layout benchmark

Run this standalone module on native Linux amd64 and arm64. Coordinate
separate runner time with Linux Release after the composed release gate's
required runs. Use the same clean full commit and Go toolchain on both
architectures. Choose an existing directory on the local disk under test,
and save the binary and reports outside the checkout. Record the runner
and local device or filesystem with the results.

```sh
set -eu
test "$(uname -s)" = Linux
case "$(uname -m):$(go env GOHOSTARCH)" in
  x86_64:amd64|aarch64:arm64) ;;
  *) exit 1 ;;
esac
test -z "$(git status --porcelain)"
revision=$(git rev-parse HEAD)
arch=$(go env GOHOSTARCH)
data=/path/to/local/disk
evidence=/path/to/evidence
mkdir -p "$evidence"
findmnt -T "$data" > "$evidence/storage-$arch.txt"
cd bench/segment-layout
GOWORK=off GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go build -buildvcs=true \
  -o "$evidence/segment-layout-$arch" .
"$evidence/segment-layout-$arch" -dir "$data" -smoke > "$evidence/smoke-$arch.json"
for profile in mixed entropy; do
  for scale in small medium large; do
    "$evidence/segment-layout-$arch" -dir "$data" -fixture "$profile" \
      -scale "$scale" -runs 21 -revision "$revision" \
      > "$evidence/$arch-$profile-$scale.json"
  done
done
```

Each invocation emits one schema v3 JSON report. It records the reported
revision, embedded build revision and binary SHA-256 alongside the fixture
seed, profile and scale, sample count, Go runtime, kernel, filesystem and
codec settings. For qualification, require both revisions to equal the
pinned commit and match each phase's `oracle_sha256` across all layouts
and both architectures. This fingerprint covers the independent oracle's
ordered rows for every query in that generation. The benchmark records
filesystem information and the kernel machine name. The runner operator
confirms native execution and local storage.

| Scale | Ordinary batches | Initial retained rows |
| --- | ---: | ---: |
| `small` | 64 | 5,253 |
| `medium` (default) | 2,048 | 167,941 |
| `large` | 8,192 | 671,749 |

Use `-batches` for a custom size of 1–10,000 ordinary batches in place of
`-scale`. Smoke mode fixes the size at 64 batches and uses one sample per
query. Fixture rows, block counts and file byte counts are reproducible for
the same profile, size and dependency versions. Timings vary.

## Fixture and queries

The mixed profile cycles ordinary batch sizes of 1, 7, 64 and 256 with
repeated log templates and seeded random IDs. The entropy profile adds 256
seeded random bits per ordinary line as hex. Both include timestamp extrema,
a maximum-frame-sized singleton, exact duplicate rows and tied timestamps.
The full-range selectivity queries include absent, rare (two initial
matches), sparse (the first row of every 31st batch), medium (every eighth
row within a batch), dense (four out of every five rows within a batch) and
all rows. The report gives the observed match count and its fraction of all
retained rows in that generation. For time-bounded queries, the denominator
also includes rows outside the window.

Additional probes cover case sensitivity, one-, two- and three-byte UTF-8
literals (`x`, `é`, `€`), the oversized row's tail, and half-open time
windows. The false-positive probe combines sparse and dense terms that
co-occur with an intervening term. Their overlapping four-byte windows
admit Bloom candidates, and exact matching rejects them all.

The controller builds the fixture, packs all six layouts, and runs an
independent exact byte-substring scan over the append input for each query.
A stable timestamp sort preserves append-ordinal order for ties. It writes
those expected rows once per query and generation. Each fresh Linux worker
measures one layout across the three prepared generations, with workers
running serially. After every timed sample, the worker streams the shared
expected rows and compares timestamps, ordinals and complete lines,
including duplicates. Successful queries set `ordered_parity` to true.
Any mismatch or worker failure aborts the matrix. On completion or
cancellation the controller reaps the active worker and removes its
private workspace before returning.

## Layouts and backfill

Every layout uses the same input and query workload for a given profile,
scale and generation. The runner packs by append order with a 64 KiB target
and sorts each block by timestamp and append ordinal. Oversized rows get
their own block. Payloads use the production v1 checkpoint block codec.
The benchmark's versioned container uses little-endian lengths and CRC32C
for its header and index entries. LZ4 and single-threaded Zstd compress
blocks independently and fall back to raw storage when compression would
expand a block. Each codec has an unfiltered layout and a layout with a 1 KiB
Bloom filter over overlapping four-byte windows per block. Both use the
same time bounds. Filters can reject a payload read. A positive filter or a
literal shorter than four bytes still requires exact matching after decode.

Each backfill starts from the initial append input and appends one batch of
four exact duplicate rows with new ordinals. The clustered batch duplicates
rows at the first ordinary timestamp. The scattered batch duplicates rows
from the early and middle ordinary timestamps and near the end of ordinary
history, in descending timestamp order. At small custom batch counts those
anchors can coincide. Both generations are packed using the same rules as
the initial generation. All three generations and their shared oracle
files stay in the workspace through worker execution. Allow space for
both rewrites, the initial files and the oracle streams.

## Metrics

- **Retained bytes** count the entire file for the reported generation,
  including header, index and payload. **Bloom metadata bytes** count the
  added 1 KiB per block in the filtered layouts. The ratio divides Bloom
  metadata bytes by the full retained size for that layout and generation.
- **Payload reads avoided** subtracts the logical block payload reads from
  the corresponding unfiltered layout with the same codec, generation,
  literal and time window. The report includes both read counts and the
  analogous difference in stored payload bytes. These counters count
  successful logical reads through the container and exclude index reads.
  **Decoded bytes** sum the uncompressed block sizes passed to the
  checkpoint decoder per sample, including blocks admitted by a Bloom
  false positive. Each query also reports its same-codec baseline.
- **Cold p95** uses nearest rank over the recorded per-query nanosecond
  samples. The runner uses Linux `FADV_RANDOM` on each query's file
  descriptor and requests `FADV_DONTNEED` on the entire file before each
  timed sample. Timing covers index and payload I/O, decode, exact filtering
  and result sorting. The Linux kernel can retain pages despite the advice,
  and storage-controller or host caches can affect samples. Use local disk
  for measurements intended to characterize storage latency.
- **Rewrite bytes** count every byte of the new, synced container written
  by the full rebuild. They include unchanged contents written into the new
  file and exclude reads, the old file's bytes, WAL traffic, filesystem
  metadata, and device or filesystem write amplification. The top-level
  per-layout `rewrite_bytes` is the clustered scenario's value. Each entry
  in `backfills` includes its own rewrite count, timestamp bounds and
  full query metrics. The model measures whole-file checkpoint layout
  rewrites in the benchmark container. `rewrite_ns` is elapsed time for
  per-layout block sorting, encoding, optional compression and Bloom
  construction, file writes, sync and close. Shared partitioning and
  oracle preparation happen outside that interval.
- **Peak worker RSS** (`peak_worker_rss_bytes`) is read from Linux
  `/proc/self/status` (`VmHWM`, converted from KiB) after a layout's final
  query. It includes the worker runtime, codec internals, retained results,
  and streamed oracle verification. It covers the whole layout process
  through measurement and excludes controller fixture construction.
- **Peak decode bytes** is the largest accounted single-block working set
  observed across samples for a query. It counts the stored payload, any
  separate expansion buffer, decoded row structs and copied line bytes. It
  excludes codec-library internals, allocator overhead, the index and
  accumulated query results. A query with zero payload reads reports zero.

From this module run `go vet .` and `go test -race .`. On macOS the
tests use a portable worker harness with a no-op cache policy and a
synthetic RSS reading. On Linux they also exercise real cache advice,
RSS collection, and the full report. Both exercise the production
checkpoint block codec.

## Production format decision

Native amd64 and arm64 local-disk measurements are pending. This revision
does not yet have evidence to justify a production format proposal.
Compare all six layouts on each runner with matching fixture fingerprints,
recording ordered parity, sparse cold p95, decoded bytes, retained bytes,
clustered and scattered rewrite bytes and time, peak worker RSS, and peak
decode working set. Evaluate storage and sparse-query gains alongside
Bloom metadata, backfill cost, and worker memory across both profiles and
architectures before proposing a production format.
