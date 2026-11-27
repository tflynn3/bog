# Retained-history capacity runner

Run from the repository root on Linux with Go 1.27 and cgroup v2. Use a
disposable local disk-backed filesystem with sufficient free space for the WAL,
checkpoint generations and the server's readiness floor. Supply an empty
delegated cgroup parent with the memory and I/O controllers enabled for its
children. The runner must be able to create leaves, set their limits and move
its child into them. It defaults to its current cgroup when a parent is omitted.

```sh
# Build in a clean checkout of the revision under test.
revision=$(git rev-parse HEAD)
go build -trimpath -buildvcs=true -o /absolute/path/to/bog ./cmd/bog
go run ./bench/retained-history \
  -binary /absolute/path/to/bog -revision "$revision" \
  -fixture linux-release-v1 -report /absolute/path/to/legacy.json \
  -cgroup-parent /sys/fs/cgroup/path/to/delegation \
  -work-dir /absolute/path/on/local/disk -mode legacy
```

The runner copies the executable into a private directory, hashes that copy and
checks its Go build metadata for the requested clean Git revision and native Linux
architecture. Each size gets a fresh store and cgroup. The same store and cgroup
are used for the two process lifetimes within a size. The runner reaps the
processes and removes the stores and measurement cgroups.

`-work-dir` may be relative to the runner's working directory. When omitted, it
defaults to the system temporary directory. The runner uses absolute paths
for the private executable and each store so the child can open them after
changing into the store.

## Fixture and measurements

| Mode | Retained seed rows | Mixed-load tail rows | Line bytes | Hard memory cap |
| --- | --- | --- | --- | --- |
| Legacy | 8,192 and 32,768 | 2,048 at each size | 1,024 | 128 MiB |
| Live | 196,608 and 393,216 | 2,048 at each size | 1,024 | 128 MiB |

The fixture submits ordered history, then reverse backfills, then mixed-load
backfills in 64-row durable batches. Timestamp ties include exact duplicates
and distinct lines. A case-sensitive literal containing `.*` and a half-open time
range select the broad query. Its first page captures the acknowledged seed
cutoff. Ingest and cursor continuations then run concurrently. A single ingest
worker fixes the append order for exact tie comparisons. The restart walk has
a new cursor and compares every acknowledged row in the full fixture range.

The report records startup and restart-to-ready time, nearest-rank ingest and
broad continuation p99 with sample counts, kernel cgroup memory peak, maximum
process RSS from Linux wait accounting, peak sampled descriptors, per-device
cgroup I/O counters and minimum free bytes on the fixture filesystem. Disk and
descriptor sampling runs every 10 ms, so short peaks may fall between samples.
Restart uses the existing filesystem cache. Zero read bytes can be a valid
cgroup count in that case. Times are in nanoseconds. Failed phases and timeouts
are retained in the report, with unavailable measurements represented by null
and an explanation. A required measurement failure returns a nonzero exit.

Each live fixture requires `cgroup_memory_limit_bytes` and
`cgroup_swap_max_bytes` read back from the kernel at 128 MiB and zero,
respectively. `cgroup_peak_memory_bytes` and `cgroup_oom_kill_count` come
from the same fixture cgroup after both lifetimes drain, before the cgroup
is removed. The peak must be positive and within the cap, and
`memory.events` must contain an explicit zero `oom_kill` counter. Cleanup
records reaping, subtree emptiness and removal, and fixture store removal.

## Slow-response qualification

Run the benchmark with the reviewed #108 binary on native Linux at both
fixture sizes. Build the binary in its clean candidate checkout and run the
benchmark from the checkout containing this workload.

```sh
go run ./bench/retained-history \
  -mode live -require-slow-response-overlap \
  -binary /absolute/path/to/bog -revision "$candidate_revision" \
  -fixture linux-release-v1 -report /absolute/path/to/slow-response.json \
  -cgroup-parent /sys/fs/cgroup/path/to/delegation \
  -work-dir /absolute/path/on/local/disk
```

This mode uses two query slots. The scattered trial holds a full-range cursor
and opens a selective cursor while a continuation is blocked on a loopback
socket. It observes a nonempty candidate send queue with a TCP zero-window
probe, then keeps the body unread across acknowledged ingest and publication.
After publication, a second paused continuation and a clean query-capacity
rejection confirm both slots are still occupied. Resetting the first socket
must let that same continuation return the original NDJSON bytes and next
cursor while the other write stays paused.

After checking both held snapshots, the runner retires the selective capture
and starts one more paused broad continuation. Explicit retirement bars new
resumes with HTTP 410 while the admitted write still pins the predecessor.
Cancellation must then allow the pin count to reach zero and the predecessor
manifest to disappear while the successor manifest retains its identity.
Full-range and selective walks compare ordered rows and duplicate multiplicity
against the acknowledged fixture ledger before and after restart.

The schema-v1 report adds `diagnostics.slow_responses` for this workload.
It records ordered observations, sampled `memory.current` peaks at phase
boundaries and every 20 ms during each phase, and the phase of any failure.
The lifetime kernel `memory.peak` and zero-swap, zero-OOM, and cleanup
requirements still apply. The query diagnostics record HTTP rejection reasons
and distinguish received headers from completed bodies.

## Live qualification

Live mode is the default used by the release gate. The legacy case records a
baseline and always leaves the gate's `retained_history_capacity` case skipped.
The checked-in [legacy report](evidence/legacy-linux-arm64.json) was collected
using the clean base revision on an arm64 Ubuntu Lima VM with Linux 6.8, an ext4
fixture filesystem and Go 1.27.0. Compare results on the same host and storage
configuration when measuring changes.

The live binary exposes an opt-in loopback path, default
`/_capacity/compaction`, with these operations:

- `POST ?action=start&cutoff=N` starts asynchronous compaction of exactly the
  committed prefix of N rows.
- `GET ?action=status&cutoff=N` reports the operation state. Both operations
  return JSON with `state` (`running` or `complete`), `generation`,
  `covered_rows` and `committed_rows`. Coverage and generation describe the
  currently published checkpoint. A completed operation must advance the
  generation and cover exactly the requested cutoff.
- `POST ?action=retire&cursor=TOKEN` retires the query capture identified by
  the authenticated continuation token. It returns an empty HTTP 204 on
  success. The runner uses the input token from the terminal page after
  retrying that same request and verifying its rows and terminal framing.

Live seeding advances the checkpoint at most every 8,192 rows at both sizes.
The clustered trial uses the last two seed batches, then seeding finishes at
the exact seed cutoff. The scattered trial starts with the same 64-row
uncovered prefix at each size. The runner verifies
that compaction stays active through an acknowledged batch and a continuation.
It then verifies publication, the expected uncovered tail and further pages
from the original cursor. A candidate lacking the control path fails in the
`live_capability` phase.

Live starts use a fixed budget of 2,147,483,648 query work units to certify
complete walks over the larger history. Fresh and restarted full walks use
up to 1,000 rows per page. The admission producer exercises the release's
fixed-budget thresholds separately.

Both native runners validate the additive capacity proof from the gate
checkout with `-validate-cutover REPORT`. The workflow also sets
`BOG_RELEASE_REQUIRE_CUTOVER=1` for the release validator owned by #80.
Schema-v1 archive readers can continue to read reports collected before
these proof fields were available. Strict native qualification requires them
at both live scales and independently checks removal of the fixture parent.

## Cutover diagnostics

Each fixture's additive `diagnostics` object records the starting and
restarting candidate separately. It includes exit code or signal, whether
the candidate had already exited when cleanup began, and the last 32 KiB
of combined output. Cgroup memory events include the kernel's OOM kill
counter for the two lifetimes together. Start-to-ready and restart-to-ready
include process startup and the successful HTTP readiness probe. The
replay-only measurement is null because the candidate does not expose
that phase separately.

Query timings are measured from request start. Each phase retains counts
by client-observed admission, maximum time to headers and to full body
completion, the latest attempt, and the first failed attempt if present.
An NDJSON response identifies admission once the client sees its headers.
A complete 503 with the matching refusal text and no cursor distinguishes
capacity from work-budget rejection. Check the failure phase, elapsed
time, time budget, and partial byte count when a response is interrupted.

The live runner verifies one uniquely identifiable acknowledged backfill and
then compares an exact fresh walk after the original cursor crosses
publication and drains. The clustered walk covers the backfill window and the
scattered and restarted walks each cover the entire fixture. Compare those
phases with the release saturation sequence before attributing a fresh query
timeout to admission or response delivery. The compaction-running ingest p99 and
sample count cover only acknowledgements after the start response and
before a status request that observed the same operation still running.
The existing mixed-load p99 includes every mixed-load acknowledgement.
[Release diagnosis sequence](../../verify/linux/README.md#cutover-diagnostics)

Every 100 ms in live mode, the runner samples the apparent bytes in
`.bog-checkpoint-stage` and counts published generation pin records in
the fixture directory. Pin age uses record modification time. Records
can outlive their locks, so these observations describe records on disk.
The samples can miss short-lived peaks. Zero represents an empty observed
sample. A null measurement or a query phase that never ran has an entry
in `diagnostics.unavailable`. Optional diagnostic gaps do not change the
release verdict.

## Architecture integration

`bog/bench/retained-history` is a verifier command with no Bog package
imports. It observes the pinned candidate over HTTP, through the process
lifecycle, and through Linux cgroup and filesystem measurements.

### Publication and pin-drain sample contract

Each live fixture reports two independent timelines under
`diagnostics.backfill_timelines`. Clustered backfill uses the last two
seed batches in a narrow timestamp band. Scattered backfill spreads the
mixed tail's tied groups through the broad query range. Each timeline
identifies the held cutoff, replacement cutoff, published generation,
and acknowledged rows, batch count, timing and timestamp range. The existing
top-level compaction
proof continues to describe the scattered operation.

Samples use monotonic nanoseconds relative to the start of their phase.
The timeline keeps its early samples and most recent samples and counts
any omitted intermediate samples. Phase boundaries include the held
capture, compaction start, overlap, exact publication, held exhaustion,
terminal retry, explicit retirement, drain and fresh parity. The report also
carries the phase failure when work stops early. The start request to first validated
completion is a client-observed upper bound on publication lag. The send
time of the last validated running request gives a conservative lower
bound. These bounds include observation delay.

Each timeline requires at least two `cgroup_memory_current` reads,
including one at the overlap with acknowledged backfill. A running status
reply must precede each read, and the following request must still observe
the same running compaction. Both replies must precede the publication
observation. The `live_memory_current` summary reports the sample count,
the elapsed nanoseconds from the first read's start to the last read's
end, and the minimum and maximum bytes read. All readings must be
positive and at most the final `memory.peak` collected from the same
fixture cgroup, which stays in place across candidate restart.

The existing loopback control may add a `metrics` object to either reply.
Each value is a nonnegative JSON integer. Missing, null or malformed
fields carry individual unavailable reasons. A supplied zero is valid.

| Candidate field | Meaning at the status snapshot | Runtime handoff |
| --- | --- | --- |
| `publication_lag_ns` | Start acceptance to durable publication and installation of the requested cutoff, available on completion | #53 |
| `staging_reserved_bytes` | Outstanding staging reservations | #71 via #53 control |
| `staging_used_bytes` | Bytes charged to live staging | #71 via #53 control |
| `oldest_pin_age_ns` | Age of the oldest live generation pin, zero if there are no live pins | #53 |
| `reclaimable_bytes` | Bytes eligible for reclaim under the authority and pin rules | #53 |
| `retired_generation_pins` | Live pins for the predecessor retired by this operation, available after publication | #53 |

Filesystem samples report apparent checkpoint staging bytes, published pin
record count and oldest record modification age separately. They cannot
establish reservations, pin liveness or reclaim eligibility. The runtime
must report a positive predecessor count after terminal replay while the
capture is held, followed by an observed zero after explicit retirement.
The existing `retired_pins_after_exhaustion` field records that post-retirement
sample. Drain polling has a bounded budget within the fixture deadline.
The workload fails if the held sample is missing or zero, or if the drain
budget ends without a zero. Other runtime metrics remain diagnostic.
