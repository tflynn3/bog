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

The portable `TestFilterReadTradeoff` probe on Darwin arm64 with Go 1.27.1
used one 2,048-byte synthetic block of repetitive text. These are
container byte counts from the probe.

| Codec | Retained without filter | Retained with filter | Absent payload bytes without / with filter | Present payload bytes with filter |
| --- | ---: | ---: | ---: | ---: |
| Raw | 2,102 | 3,126 | 2,048 / 0 | 2,048 |
| LZ4 | 112 | 1,136 | 58 / 0 | 58 |
| Zstd | 103 | 1,127 | 49 / 0 | 49 |

The filter removed the absent payload read at a cost of 1,024 retained bytes
per block. The present and short literals still fetched the payload for
every codec. The filter cost exceeded the compressed payload size in this
probe. The incompressible block test verified that both codecs fall back to
raw storage when compression would expand the block.

Use the Linux report's paired `sparse`, `bounded-sparse`, and `dense`
entries to compare p95, RSS and payload reads against the retained byte
cost at each scale. Compare clustered and scattered ratios under the same
measurement boundary. The Darwin probe gives no Linux latency, memory,
or compaction measurements. A format proposal needs representative Linux
reports for both profiles and backfill shapes.

From this module, run `go vet .` and `go test -race .`. On macOS
these commands cover the portable codec, metrics, subprocess lifecycle,
and HTTP protocol tests. Run them on Linux for the complete fixture,
query-worker and checkpoint publication tests as well.
