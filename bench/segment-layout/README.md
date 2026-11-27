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

## Cold incremental comparison

Storage owns this nested module. Run the schema v7 cold matrix on native
Linux with a disk-backed work directory. From the repository root, set
`work` to an existing scratch directory and `artifacts` to an existing
evidence directory. Use the build, validate, and archive steps from the
segment-layout workflow for each native runner.

```sh
revision=$(git rev-parse HEAD)
cd bench/segment-layout
CGO_ENABLED=0 go build -o "$artifacts/segment-layout" .
"$artifacts/segment-layout" -cold -smoke -seed 44 -revision "$revision" \
  -dir "$work" > "$artifacts/cold-smoke.json"
"$artifacts/segment-layout" -validate-report "$artifacts/cold-smoke.json"
"$artifacts/segment-layout" -cold -scale medium -seed 44 -runs 21 \
  -revision "$revision" -dir "$work" > "$artifacts/cold-medium.json"
"$artifacts/segment-layout" -validate-report "$artifacts/cold-medium.json"
tar -C "$artifacts" -czf "$artifacts/segment-layout.tar.gz" segment-layout
```

Keep the validated JSON and matching executable archive together under the
runner architecture. The existing workflow runs the cold matrix regression
on native amd64 and arm64 and uploads its incremental smoke report. The
commands above produce the cold artifacts when run on those architectures.

A single `-cold` invocation runs both `mixed` and `entropy` with the same
PCG seed, batch schedule, scale, query schedule, and filesystem. Use
`-fixture entropy` to run entropy first. The report records profile order,
the batch pattern, seed, exact query windows, repetitions, and the cache
procedure. Mixed combines repetitive text and variable warnings. Entropy
adds four PCG-generated 64-bit values encoded as hex to each ordinary row.
Both include tied timestamps, duplicate rows, edge timestamps, and a
maximum-size singleton. Each profile contains the production v1 storage
component and all six experimental layouts. Each experiment uses a 64 KiB
target and one immutable file per segment. Raw, LZ4, and Zstd each run with
Bloom disabled and enabled. Four-row clustered and scattered backfills
independently extend the same initial fixture. The v1 baseline publishes
each fixture with the production checkpoint publisher and scans it with
the production retained-history reader.

Each sample starts a fresh Linux worker, opens the same published generation
for that layout and phase, loads the manifest, and initializes the reader.
For v1 this includes production manifest and segment validation and seek
index construction. Immediately before sampling, the worker requests
`FADV_DONTNEED` over every referenced segment, including reused segments.
The kernel may retain pages after this advice. The run leaves filesystem
metadata, the manifest cache, default readahead, and host and device caches
in place. The timer covers the ordered scan and result construction. It
excludes worker startup, generation validation, cache advice, metric
collection, and transfer to the oracle. This measures cold segment reads
against an initialized reader.

The runner rotates layout order across matched samples. `-runs` applies
to the full-range sparse, bounded sparse, medium, and dense probes. Sparse
selects the first row of every 31st batch plus matching backfill rows.
Bounded sparse uses the first 37 seconds from the fixture epoch. Medium
selects every eighth ordinary row and dense selects four out of five, with
matching backfill duplicates in both. Each query records the population,
time-window, and matching row counts as exact selectivity denominators.
Each phase and layout also runs one absent, one-byte, two-byte, three-byte, and all-rows probe.
The parent verifies every returned timestamp, append ordinal, and line byte
against the independent append-log oracle before accepting the sample.
The all-rows probe verifies the complete generation. The short-literal
probes require exact byte matching and check that enabling Bloom leaves
their payload counts unchanged.

### Paired codec results

Each profile's `pairing.comparisons` contains 72 comparisons. Each of the
three codec pairs (raw/LZ4, raw/Zstd, LZ4/Zstd) is measured in both Bloom
settings, for all four repeated queries, on the initial snapshot and after
each independent backfill.
All round deltas are candidate minus baseline, so a negative delta favors
the candidate on that metric. Join a round to its endpoint samples using
`pair_id` and the layout names. The ID binds the execution, fixture, exact query
window and literal, phase, cache protocol, and one-based round. Every sample
also records a digest of the ordered timestamps, append ordinals, and
complete line bytes it matched. Report validation rebuilds the fixture and
oracle and recomputes all comparisons from their endpoint samples.

| Paired field | Interpretation |
| --- | --- |
| `physical_read_bytes` | Delta in Linux `read_bytes` charged to each query worker, including readahead. `storage_read_observed_rounds` counts rounds with a positive reading on either endpoint. |
| `peak_worker_rss_bytes` | Delta in the two workers' whole-process high-water RSS for that round. |
| `rewrite_amplification` | Backfill-only delta in logical synced segment and manifest bytes and in bytes written per appended WAL byte. The report repeats the phase's single publication comparison for each repeated query. |
| `tradeoffs` | Candidate minus baseline for cold p95, process and physical read p95, payload and stored bytes, segment fan-out, and peak block decode bytes. The p95 deltas subtract endpoint percentiles. |

The read and memory summaries show nearest-rank p50 of the signed round
deltas and the number of lower, equal, and higher rounds. Use
`candidate_lower` or `baseline_lower` to find comparisons where one layout
has a favorable delta and all other reported tradeoff deltas, rewrite
bytes, and paired physical-read and RSS rounds are favorable or equal. `tradeoff_or_variable` means the direction changed across
metrics or rounds. `equal` means all observed deltas were zero. Check
`storage_read_observed_rounds` before drawing storage conclusions because
successful cache advice can still yield cached reads. Compare results across
both fixture profiles and backfill phases before choosing a layout.

The validator accepts historical schema v5 and v6 cold reports. Schema v7
requires both matched profiles and independently reconstructs their query
selectivities, segment packing, decoded fan-out, and decode ceilings.

### Individual worker measurements

| Report field | Measurement |
| --- | --- |
| `cold_p95_ns` | Nearest-rank p95 of the query timer. Single-sample probes are parity controls. |
| `process_read_bytes_p95` | Linux `rchar` delta, less the observer's own first counter read, including index and payload reads served from cache. |
| `storage_read_bytes_p95` | Linux `read_bytes` delta charged to the worker, including readahead. A cache hit can yield zero. |
| `experimental_payload` | Logical compressed or raw payload blocks and bytes, excluding index and Bloom metadata. Compare the matched on/off samples for each codec. |
| `stored_segment_and_manifest_bytes` | Logical bytes in the generation's retained segment files and current manifest, including codec and Bloom metadata. |
| `experimental_scan` | Segment indexes visited, their logical index bytes, segments whose payloads were decoded, and the largest decoded raw block. Time and Bloom pruning can reduce decoded fan-out. |
| `experimental_scan.decode` | Ordinary and oversized peak working bytes with ceilings. The ordinary ceiling follows the 64 KiB target. A larger block must contain one row and uses a size-specific ceiling capped by the existing block format. |
| `peak_decode_bytes` | Experimental maximum live single-block payload buffers, decoded rows, and line bytes. Codec internals and accumulated results are outside this count. |
| `peak_decode_go_heap_upper_bound_bytes` | Maximum query-window Go allocation delta, an upper bound on Go heap allocated for decoding in that window. It also covers other query allocations, such as results, and observer overhead. The v1 reader exposes this bound and worker RSS for memory comparison. |
| `peak_worker_rss_bytes` | Worker high-water RSS through the scan, including startup and reader initialization. |
| `rewrite_amplification` | For experiments, new synced segment and snapshot manifest bytes. For the v1 component, the publisher's Linux `wchar` delta including transient staging writes. The denominator in both is the same actual committed WAL suffix, including the frame header. |

After #53 ships loopback compaction, rerun with a clean pinned native Linux
binary from the same shipped revision on each target architecture.

```sh
go run . -cold -dir /path/to/data -fixture mixed -scale medium -seed 44 \
  -runs 21 -revision "$(git rev-parse HEAD)" \
  -bog-bin /path/to/pinned/bog -bog-revision "$candidate_revision" > cold-live-medium.json
go run . -validate-report cold-live-medium.json
```

The live section measures process-attributed filesystem writes around
the compaction request and completion. It checks the published cutoff,
ordered loopback query results for a bounded sparse query and all three
short literals, and the full persisted checkpoint after shutdown. A
missing candidate or unavailable control endpoint is recorded in the live
section. The live comparison is pending #53. The historical measurements
below describe the earlier full-rewrite experiment. Use schema v7 evidence
to assess the incremental format, including compressed size, Bloom metadata
cost, cold query latency and I/O, and backfill amplification.

## Successive publications

Run the incremental experiment on native Linux.

```sh
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
go build -o "$work/segment-layout" .
"$work/segment-layout" -incremental -smoke -seed 44 \
  -revision "$(git rev-parse HEAD)" -dir "$work" > report.json
"$work/segment-layout" -validate-report report.json
"$work/segment-layout" -incremental -dir /path/to/data -fixture entropy \
  -scale medium -seed 44 -runs 21 -revision "$(git rev-parse HEAD)" > entropy-medium.json
```

This mode emits schema v4 with `mode: "incremental"`. The default invocation
emits the schema v3 full-rewrite experiment described below. Incremental mode
uses the same scales and profiles, with an initial publication followed by
three cumulative four-row appends. The forward append shares the timestamp just
after the final ordinary batch. The clustered and scattered appends use exact
duplicates from the initial history with new ordinals. All nine codec and size
combinations and the `baseline` entry named `production-v1` see the same
WAL frames, append ordinals, and checkpoint cutoffs. The baseline calls the
production `checkpoint.Publisher.PublishCaptured` and queries the resulting
generation with the production retained `history.Reader`. The comparison
runs at the storage boundary.

The layouts use raw, LZ4, and Zstd with 16, 64, and 256 KiB uncompressed segment
targets. Each segment is an immutable file containing one v1 checkpoint block
in the benchmark container. The existing maximum-frame row gets a singleton
segment above the target. Each publication writes and syncs the changed
segment files and a new manifest. It carries forward sealed segments and
repacks the previous tail only when the first appended row fits there.
The benchmark keeps the predecessor readable while the successor is built.
Every selected publication is reopened and checked against the full independent
append-order oracle, including exact timestamps, ordinals, and line bytes.

- **Reuse and writes.** `reused_bytes` counts complete segment files referenced
  by both consecutive manifests. `rewritten_bytes` counts complete new segment
  files, including newly appended segments and any replacement tail.
  Their sum is `retained_bytes`. `retired_bytes` counts the predecessor's
  segment files dropped from the new manifest. The initial publication reports
  zero reuse and retirement. `manifest_bytes` gives the additional logical
  bytes written for publication metadata. These counts exclude filesystem
  metadata, WAL bytes, and device write amplification.
- **Decode allocations.** A fresh Linux worker loads the manifest, then scans
  its segments one at a time without retaining results. `allocations` and
  `allocation_bytes` are Go runtime counter deltas across the scan, including
  segment opens, validation, codec setup and decode. `peak_rss_bytes` is the
  worker's whole-process Linux high-water mark, including runtime and codec
  overhead.
- **Decode working set.** `peak_working_bytes` accounts for the largest live
  block's stored payload, a separate expansion buffer if needed, decoded row
  structs, and copied lines. Its bound allows two payload buffers up to the
  raw block limit plus the maximum decoded row and line footprint at that
  limit. The bound scales with the configured target and the oversized
  singleton, independent of the number of segments. Ordinary and oversized
  peaks are reported separately so the singleton cannot mask the effect of
  segment size. The working-set accounting excludes metadata, codec internals,
  allocator overhead, and accumulated query results.

  Schema v4 requires each experimental publication to report positive
  `allocations`, `allocation_bytes`, `peak_rss_bytes`, and ordinary and
  oversized working-set peaks, with `oversized_segments` equal to one.
  `peak_working_bytes` equals the larger of `peak_ordinary_working_bytes` and
  `peak_oversized_working_bytes`. `ordinary_working_bound_bytes` is computed
  from the layout target and must cover the ordinary peak.
  `working_bound_bytes` must cover both the ordinary bound and the overall
  peak. The `production-v1` baseline may omit the decode sample.
- **Query and publication latency.** Each run uses a fresh Linux process and
  private data directory. Sparse and dense scans run in separate trials.
  Initial queries run against the verified initial generation at idle. Each
  append times a full-range scan of the immutable predecessor while the writer
  builds and durably publishes the successor. Timed scans include file reads,
  decoding or row traversal, filtering, and producing ordered results. WAL
  writes and prefix hashing precede the timed publication. Successor
  activation, oracle checks and cleanup occur outside both timers. The
  filesystem page cache is left in place. Each phase records both timings
  and their actual intersection, plus nearest-rank p95 for each query and
  publication group. A trial contributes samples when every append scan
  overlaps publication. Each query gets up to five times the requested run
  count in attempts. The existing `query_during_compaction` field also
  summarizes sparse append scans.
- **Physical write amplification.** Each sample divides the Linux
  `/proc/self/io` `write_bytes` delta around publication and its paired
  read-only scan by the actual committed WAL suffix length, including frame
  headers. The initial denominator excludes the WAL magic. Use a disk-backed
  directory that supplies positive process write accounting. The numerator
  includes staging and publication writes charged to the worker by Linux.
  Device-level I/O and filesystem writes charged to other processes are
  outside this counter. Logical segment and manifest sizes are reported
  separately.
- **Peak RSS.** Each sample records the worker's Linux `VmHWM` through the
  timed publication and query. This is a whole-process high-water mark;
  fixture loading, WAL setup, and earlier work in the same trial contribute.
  The decode-only RSS remains a separate measurement collected in an
  isolated worker for the experimental layouts.

The native Linux smoke workflow runs the benchmark on amd64 and arm64 using
runner scratch space. It validates the schema v4 report, including the production
baseline, every codec/size combination, the exact phase and query matrix,
matched denominators, and the requested number of samples. Each job uploads
the report and an archive containing its native executable.

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
The historical sparse samples also show why it matters which blocks the
filter prunes. The unfiltered layouts reached roughly 8 MiB in peak
decode working bytes. The filtered layouts skipped the oversized singleton
and peaked at 147,320–153,053 bytes in mixed and 140,300–169,616 bytes in
entropy, even though the entropy filter pruned only three blocks.

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
