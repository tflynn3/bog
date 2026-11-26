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

## Live qualification

Live mode is the default used by the release gate. The legacy case records a
baseline and always leaves the gate's `retained_history_capacity` case skipped.
The checked-in [legacy report](evidence/legacy-linux-arm64.json) was collected
using the clean base revision on an arm64 Ubuntu Lima VM with Linux 6.8, an ext4
fixture filesystem and Go 1.27.0. Compare results on the same host and storage
configuration when measuring changes.

The runner's live control protocol needs to be wired to the live binary's
checkpoint control and observed state when those become available. It expects
a loopback path, default `/_capacity/compaction`, with these operations:

- `POST ?action=start&cutoff=N` starts asynchronous compaction of exactly the
  committed prefix of N rows.
- `GET ?action=status&cutoff=N` reports the operation state. Both operations
  return JSON with `state` (`running` or `complete`), `generation`,
  `covered_rows` and `committed_rows`. Coverage and generation describe the
  currently published checkpoint. A completed operation must advance the
  generation and cover exactly the requested cutoff.

Live seeding advances the checkpoint every 8,192 rows at both sizes. Mixed load
starts with the same 64-row uncovered prefix at each size. The runner verifies
that compaction stays active through an acknowledged batch and a continuation.
It then verifies publication, the expected uncovered tail and further pages
from the original cursor. A candidate lacking the control path fails in the
`live_capability` phase.

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

The live runner verifies one uniquely identifiable acknowledged backfill in
a fresh query after the original cursor crosses publication and drains.
Compare those phases with the release saturation sequence before attributing
a fresh query timeout to admission or response delivery. The restart walk
still compares the entire fixture. The compaction-running ingest p99 and
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

Register `bog/bench/retained-history` as a verifier command with no Bog package
imports in the root architecture registry, and add it to the system package
table. The existing fitness failure also needs a staged `bog/internal/backup`
entry allowing its direct import of `bog/internal/store`. Those changes belong
with the architecture owner. The release job needs a delegated cgroup v2
parent on its Linux worker.
