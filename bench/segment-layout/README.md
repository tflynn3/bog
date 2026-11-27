# Checkpoint layout benchmark

Run this standalone Go module on Linux. Choose an existing directory on the
filesystem or device under test. The runner creates and cleans up a private
subdirectory there.

```sh
cd bench/segment-layout
go run . -smoke -seed 44
for profile in mixed entropy; do
  for scale in small medium large; do
    go run . -dir /path/to/data -fixture "$profile" -scale "$scale" -seed 44 -runs 21 \
      -revision "$(git rev-parse HEAD)" > "$profile-$scale.json"
  done
done
```

Each invocation emits one schema v3 JSON report. The report records the source
revision, fixture seed, profile and scale, sample count, Go runtime, kernel,
filesystem, Go runtime environment and codec settings. Pass `-revision` when building without Git
metadata. The default uses Go's embedded revision and dirty status.

| Scale | Ordinary batches | Initial retained rows |
| --- | ---: | ---: |
| `small` | 64 | 5,253 |
| `medium` (default) | 2,048 | 167,941 |
| `large` | 8,192 | 671,749 |

Use `-batches` for a custom size of 1–10,000 ordinary batches in place of
`-scale`. Smoke mode fixes the size at 64 batches and uses one sample per
query. Use `-seed` to select the PCG seed (default 44) and `-timeout`
to bound each query sample or compaction scenario (default 5m). Fixture rows, block counts and file byte counts are reproducible for
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
windows. Each timed sample runs in a fresh child process and streams its results
after timing. The parent compares the stream to an independent exact
byte-substring scan over the append input. A stable timestamp sort of that input preserves append-ordinal
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
- **Cold p95** uses nearest rank over the recorded per-query nanosecond
  samples. The runner uses Linux `FADV_RANDOM` on each query's file
  descriptor and requests `FADV_DONTNEED` on the entire file before each
  timed sample. Timing covers index and payload I/O, decode, exact filtering
  and result sorting. The Linux kernel can retain pages despite the advice,
  and storage-controller or host caches can affect samples. Use local disk
  for measurements intended to characterize storage latency.
- **Peak RSS** samples Linux `VmHWM` for the whole executed query process
  at the end of the timed query, before streaming results. The report
  keeps each sample and the maximum per query. This includes the runtime,
  index, codec allocations and accumulated rows. It excludes parent
  fixture generation and the oracle. Component RSS covers the entire
  child through seeding, publication, and validation.
- **Rewrite bytes** count all logical bytes in the new synced container,
  including unchanged contents written into the new file. They exclude
  the previous file, WAL traffic, filesystem metadata and device I/O.
  The top-level layout count repeats the clustered scenario. Each
  backfill includes its own amplification ratio and query measurements.
- **Peak decode bytes** is the largest accounted single-block working set
  observed across samples for a query. It counts the stored payload, any
  separate expansion buffer, decoded row structs and copied line bytes. It
  excludes codec-library internals, allocator overhead, the index and
  accumulated query results. A query with zero payload reads reports zero.

## Compaction measurements

Each trial starts with a published v1 checkpoint over the same seeded
ordinary WAL. It appends the four backfill rows as one batch and publishes
through the exact new ordinal boundary. The common denominator for that
scenario is the durable WAL growth from the backfill, including the
16-byte frame header and encoded NDJSON. The three numerators have
different measurement boundaries.

| Result | Numerator and interval |
| --- | --- |
| Experimental layout rewrite | Logical bytes of the full synced experimental container. |
| Checkpoint publication component | Isolated child `/proc/self/io` `wchar` delta during `checkpoint.Publisher.PublishCaptured`. It includes staged writes even when later removed, plus the publication intent and manifest. Initial seeding, WAL appends, prefix hashing by the caller, and post-publication validation are outside the interval. Runtime diagnostics written during the interval also contribute. |
| Live server compaction | Pinned server `/proc/PID/io` `write_bytes` delta immediately before POST start through observed completion. This is Linux process-attributed storage accounting. It includes filesystem allocation effects and any other writes attributed to the process during that window. It excludes initial seeding and the acknowledged backfill. It does not subtract `cancelled_write_bytes`. |

Ratios divide each numerator by the same scenario's appended WAL bytes.
Compare the component and experimental results as logical write counts.
Use the live ratio to characterize process-attributed filesystem writes
under a recorded filesystem and kernel. None of the counters measures
physical media writes. The component exercises the production WAL capture
and pinned publisher. The runner reopens the durable checkpoint and checks
its timestamps, append ordinals and complete contents against the oracle.

To add live results, supply a clean native Linux `bog/cmd/bog` binary
built at the full #53 revision. The runner copies it privately and records
the revision and file SHA-256. It starts the binary on IPv4 loopback with
`BOG_CAPACITY_CONTROL=1` against an independently seeded copy of the same
fixture, then gets a 204 for the backfill, waits for a newer generation
covering the exact cutoff, and validates the persisted checkpoint after
shutdown.

```sh
go run . -smoke -bog-bin /path/to/pinned/bog -bog-revision "$candidate_revision"
```

Without a pinned binary, or when the startup control probe returns HTTP
404 or 501, the live report records an unavailable reason. Invalid binary
identity, startup or protocol failures, and timeouts fail the run.

## Observed trade-offs

The [mixed](results/linux-arm64-medium-mixed.json) and
[entropy](results/linux-arm64-medium-entropy.json) reports cover all six
layouts, the initial generation, and both clustered and scattered backfills.
Each profile used seed 44, 2,048 ordinary batches, 167,941 initial rows,
167,945 rows after each backfill, and 21 independent samples per query.
Each sample and published checkpoint passed the exact row oracle.

Both runs used the same arm64 executable built with Go 1.27.1 and cgo
disabled from base `0cbb7a51b72cc73946df61bbb7e125165660562e` plus the
benchmark changes. The SHA-256 of the Git diff from that base for
`bench/segment-layout`, excluding this README and `results/`, was
`776259dd720df1a50ac2ca362a28766d78beefa42da912f4c06ebb7f48e4b8d2`.
The executable SHA-256 was
`53aa3a0827ab4839532f4d7cd4fc2638ed116714045249f1730ec2256ba64a30`.
The samples ran sequentially on 2026-11-27 UTC in a disposable pod on the
shared Slipway Linux arm64 runner, kernel 6.8.0-117-generic, with an ext4
scratch volume at `/work`. Cache advice, CPU count and Go runtime
settings are in each report. Timing and RSS describe this runner.

```sh
# Run from the benchmark module on Linux arm64 with an existing ext4 work directory.
CGO_ENABLED=0 go build -o segment-layout .
for profile in mixed entropy; do
  ./segment-layout -dir /work -fixture "$profile" -scale medium -seed 44 -runs 21 \
    -revision 0cbb7a51b72cc73946df61bbb7e125165660562e-dirty > "linux-arm64-medium-$profile.json"
done
```

### Query results

The initial full-range sparse query matched 67 rows in each profile.
Retained bytes include all metadata. Peak RSS is the maximum of the 21
whole-process sample peaks.

| Profile | Layout | Retained bytes | Sparse blocks read | Sparse payload bytes | Sparse p95 (ms) | Sparse peak RSS (MiB) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| mixed | `lz4` | 1,429,807 | 249 | 1,420,825 | 20.34 | 20.38 |
| mixed | `lz4+summary` | 1,684,783 | 67 | 377,618 | 8.83 | 12.29 |
| mixed | `raw` | 20,404,776 | 249 | 20,395,794 | 21.90 | 20.18 |
| mixed | `raw+summary` | 20,659,752 | 67 | 4,345,967 | 8.14 | 12.34 |
| mixed | `zstd` | 902,263 | 249 | 893,281 | 38.32 | 20.62 |
| mixed | `zstd+summary` | 1,157,239 | 67 | 239,672 | 7.78 | 12.56 |
| entropy | `lz4` | 12,750,860 | 437 | 12,735,110 | 41.31 | 20.62 |
| entropy | `lz4+summary` | 13,198,348 | 434 | 12,660,085 | 38.83 | 12.79 |
| entropy | `raw` | 32,679,144 | 437 | 32,663,394 | 40.25 | 20.12 |
| entropy | `raw+summary` | 33,126,632 | 434 | 28,338,206 | 45.64 | 12.56 |
| entropy | `zstd` | 7,269,947 | 437 | 7,254,197 | 51.70 | 20.61 |
| entropy | `zstd+summary` | 7,717,435 | 434 | 7,220,528 | 49.34 | 12.95 |

Zstd retained the fewest bytes in both profiles. Bloom metadata added
254,976 bytes per layout in mixed and 447,488 bytes in entropy. The filter
cut mixed sparse reads from 249 blocks to 67 and lowered measured p95 for
all three codecs. In entropy it pruned three of 437 blocks and sparse p95
moved in both directions across codec pairs. The dense query matched
135,168 rows in each profile and pruned just one block with the filter
enabled. Across the six layouts, dense p95 ranged from 24.69 to 63.97 ms
in mixed and from 52.22 to 370.56 ms in entropy on this shared runner.

### Publication and rewrites

Each row compares one measured checkpoint publication with the range
across the six experimental rewrites. Every ratio uses the actual encoded
WAL growth for the same four-row backfill, including the frame header.

| Profile | Backfill | Appended WAL bytes | Checkpoint publication bytes | Checkpoint amplification | Experimental rewrite amplification range |
| --- | --- | ---: | ---: | ---: | ---: |
| mixed | clustered | 482 | 1,850 | 3.84× | 1,872–42,863× |
| mixed | scattered | 482 | 1,850 | 3.84× | 1,872–42,863× |
| entropy | clustered | 701 | 2,657 | 3.79× | 10,371–47,257× |
| entropy | scattered | 701 | 2,657 | 3.79× | 10,371–47,257× |

The checkpoint publisher starts at the previous cutoff and carries forward
the existing segment references. The experimental layout rewrites all
retained rows for each backfill. The live control was unavailable because
the run had no pinned #53 binary.

From this module, run `go vet .` and `go test -race .`. On macOS
these commands cover the portable codec, metrics, subprocess lifecycle,
and HTTP protocol tests. Run them on Linux for the complete fixture,
query-worker and checkpoint publication tests as well.
