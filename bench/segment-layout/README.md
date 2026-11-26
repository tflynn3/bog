# Checkpoint layout benchmark

Run this standalone Go module on Linux. Choose an existing directory on the
filesystem or device under test. The runner creates and cleans up a private
subdirectory there. Use `go run . -smoke` from this module for a quick parity
check.

## Native measurements

On each native Linux amd64 and arm64 runner, use the same clean commit and Go
version. Build once per runner and use that binary for all six profile and
scale combinations. Keep the binary and reports outside the checkout. Set
`output` to an existing external directory and `data_dir` to an existing
directory on the filesystem under test.

```sh
set -eu
cd bench/segment-layout
test -z "$(git status --porcelain)"
revision=$(git rev-parse HEAD)
arch=$(go env GOHOSTARCH)
test "$(go env GOHOSTOS)/$(go env GOOS)/$(go env GOARCH)" = "linux/linux/$arch"
mkdir -p "$output/$arch"
binary="$output/$arch/segment-layout"
CGO_ENABLED=0 go build -trimpath -buildvcs=true -o "$binary" .
sha256sum "$binary" > "$output/$arch/segment-layout.sha256"

for profile in mixed entropy; do
  for scale in small medium large; do
    "$binary" -dir "$data_dir" -fixture "$profile" -scale "$scale" -runs 21 \
      -revision "$revision" > "$output/$arch/$profile-$scale.json"
  done
done

sha256sum -c "$output/$arch/segment-layout.sha256"
```

Each invocation emits one schema v3 JSON report. It records the source and
embedded build revisions, binary SHA-256, fixture seed, profile and scale,
sample count, Go runtime, kernel machine, filesystem and codec settings.
Each generation has a SHA-256 of its append input, including timestamp,
ordinal, and exact line bytes. The default revision uses Go's embedded
revision and dirty status. An explicit `-revision` can label an exploratory
build without embedded Git metadata.

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
windows. The earliest row contains `Q7mR` and `7mR2` separately, so its
block's Bloom filter admits `Q7mR2` even though the literal is absent from
every row. The probe must decode at least one block and yield zero matches
on every layout and generation. Every timed sample is
compared after timing to an independent exact byte-substring scan over the
append input. A stable timestamp sort of that input preserves append-ordinal
order for ties. Verification compares timestamps, ordinals and complete
lines, including duplicates. Any mismatch fails the run.

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
the initial generation. The initial generation stays on disk through both
scenarios. A completed rewrite is removed before the next scenario.

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
- **Decoded bytes** sum the original checkpoint block sizes, including
  their headers, for every payload decoded. Raw blocks and raw fallbacks
  contribute their full size once. Time- and Bloom-pruned blocks contribute
  zero. The report includes the same-codec unfiltered count and the difference.
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
  rewrites in the benchmark container.
- **Peak decode bytes** is the largest accounted single-block working set
  observed across samples for a query. It counts the stored payload, any
  separate expansion buffer, decoded row structs and copied line bytes. It
  excludes codec-library internals, allocator overhead, the index and
  accumulated query results. A query with zero payload reads reports zero.

## Aggregate reports and choose a layout

Collect both architecture directories on one Linux runner and use either
runner's benchmark binary on its matching architecture.

```sh
"$output/amd64/segment-layout" aggregate "$output"/*/*.json > "$output/summary.json"
```

The aggregate report keeps cold p95, decoded bytes, peak decode bytes,
retained bytes, and rewrite bytes grouped by architecture, profile, scale,
generation, and layout. It checks fixture fingerprints and matching logical
query work across architectures, recomputes p95 from samples, and checks
the read, decode, and rewrite accounting against the reported baselines.
Each architecture must use one binary and identical recorded measurement
settings throughout its matrix. Run on native hardware. The recorded
kernel machine and Go target are provenance checks.

The current choice is **raw without Bloom** pending a complete native
matrix. The selection rule requires at least 21 samples per query in
each of the 12 architecture, profile and scale combinations for one clean
commit and separate architecture binaries. In every initial and backfilled
generation, a candidate must improve both `sparse` and `bounded-sparse`
p95 by at least 10%, keep the `all` p95 within 10% of raw, and stay within
raw's retained and whole-file rewrite byte counts. Among qualifying
candidates, the lowest worst-case sparse p95 ratio wins, with ties resolved
in report layout order. Incomplete evidence or a matrix without a qualifying
candidate retains raw. The latency thresholds are decision margins for
this experiment.

From this module run `go vet ./...`, `go test ./...` and
`go test -race ./...`. On macOS they exercise the portable fixture, layouts,
measurement and aggregation with the production checkpoint codec. Linux
also exercises the file cache advice and CLI cleanup.
