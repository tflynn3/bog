# Linux release verification

After #53, #70, #68, and the #60 readiness handoff land, dispatch the
**Linux release verification** workflow with the full composed commit SHA.
Use a workflow revision that includes this gate. The workflow
builds the same clean revision once on native Ubuntu 24.04 amd64 and once on
native Ubuntu 24.04 arm64 with Go 1.27.0. Each runner checks its runner
identity, kernel architecture, and Go host and target before building. All four
producers use that runner's pinned binary.

Download `linux-release-evidence-amd64` and `linux-release-evidence-arm64`
for the binaries, SHA-256 checksums, build metadata, fixture and runner
configuration, reports, producer exit statuses, and logs. The final
**Qualify both Linux architectures** job requires both native jobs to succeed.
It independently checks each uploaded binary's ELF machine and Go build
metadata, recomputes its digest, and validates all four reports and producer
exits against that architecture's binary and the requested revision.

The admission, durability, and capacity producers take
`-binary PATH -revision SHA -fixture ID -report PATH`.
The release fixture ID is `linux-release-v1`. The workflow runs
`go run ./verify/release` from the workflow checkout and
`go run ./verify/linux` (#3) and `go run ./bench/retained-history` (#32) from the
candidate checkout. The sibling commands need to provision their Linux fixtures
and use the supplied binary. The gate fails if any command or report is missing,
including on revisions where only earlier harness entry points exist. The
capacity producer runs as the runner user in a systemd service with delegated
cgroup v2 memory and I/O controllers. The launcher clears any inherited
`BOG_CAPACITY_CONTROL` and sets `BOG_CAPACITY_CONTROL=1` for both loopback
candidate starts in live mode. Legacy mode leaves it unset.
[ADR 0004](../../docs/adr/0004-retained-history-cutover.md#writable-activation-and-capacity-control)
specifies the server opt-in.

Reports have integer `schema_version` 1, string `revision`, lowercase hexadecimal
`binary_sha256`, string `fixture`, and a `cases` object. Each case has a string
`status` (`pass`, `fail`, or `skip`) and may include a string `detail` and
additional evidence. The following cases must pass.

| Producer | Required cases |
| --- | --- |
| Admission | `one_slot_saturation`, `ingest_during_saturation`, `cursor_resume`, `work_budget` |
| Durability | `sigkill`, `recovery`, `enospc` |
| Capacity | `retained_history_capacity` |
| Restored image | `restored_parity` |

The durability report validator requires these fields in the `enospc` case's
`evidence` object.

```json
{
  "errno": "ENOSPC",
  "disposable_local_filesystem": true,
  "observed_enospc": true,
  "recovered_after_space_freed": true
}
```

Native qualification requires explicit evidence in both retarget directions.
In `healthy_to_full`, readiness stays `ready`, ingest returns HTTP 204, and
the batch replays on the original healthy directory. In `full_to_healthy`,
readiness stays `low_disk_headroom` and ingest returns HTTP 503, followed by
`post_ingest_reason: "wal_rollback"` and `post_ingest_category: "enospc"`.
The durability producer propagates failures to stop its candidates, unmount
the disposable filesystem, or remove its fixture through its exit status.

## Live capacity evidence

The capacity case requires `mode: "live"` and exactly one passing record
each for 196,608 and 393,216 seed rows. Each fixture uses 1,024-byte
lines, 64-row batches, 2,048 tail rows, and a 128 MiB cgroup memory cap
with swap disabled. The compaction record must report a completed
generation covering the seed plus the first tail batch. The committed
and recovered row counts must both equal seed plus tail. The validator
also checks startup and restart timings, mixed-load p99s and sample
counts, memory peak, RSS, descriptor count, disk headroom, and per-device
I/O counters.

The live capacity producer carries a held cursor across generation
replacement while ingest acknowledges writes. It compares the held walk
against the captured cutoff and fresh walks against the committed ledger.

Each record carries `workload_status` and a separate `cleanup` object. An
overall pass requires both to pass. Cleanup records `candidate_reaped`,
`subtree_empty`, and `subtree_removed`. The runner gives the candidate
time to stop, reads hierarchical `cgroup.events`, and uses
`cgroup.kill` when processes remain. It waits for `populated 0` before
removing cgroups bottom-up. The cleanup deadline is independent of the
workload deadline. A workload failure can carry successful cleanup
evidence, which still fails the capacity case.

Set `BOG_TEST_CGROUP_PARENT` to a delegated cgroup v2 parent with memory
and I/O enabled for its children to run the kernel regression tests.
Run the test process from a separate child of that delegation, as the
release workflow does for the benchmark.

## Admission fixture

The probe ingests 320 timestamped rows one second apart from
2020-01-01T00:00:00Z in 32-row batches. Each line contains an ordinal and
32 KiB of padding. With `-query-concurrency=1`, it captures a one-row snapshot
page and starts a large continuation with the opaque cursor. The slow TCP
reader uses a 4096-byte receive buffer and stops reading after parsing the
response headers. The probe checks the Linux TCP persist timer and queued
bytes before and after a competing continuation and an ingest attempt.
The competing response must complete within two seconds and have HTTP 503,
zero NDJSON rows, and no next cursor. Ingest must return HTTP 204.

After disconnecting the slow reader, the probe replays the original cursor
and checks every remaining row, in order, through the terminal page. A
fresh query confirms that the acknowledged concurrent backfill, timestamped
inside the original snapshot range, is visible. A separate server with
`-query-work-budget=1` receives 32 fixture rows and must return a clean HTTP
503 on a scan for an absent string. All work-budget rejections must carry
the complete `query work budget exhausted\n` body with explicit HTTP
framing and no cursor header or trailer.

The fixed-budget probe has a one-minute deadline for both searches and walks.
The tied-row fixture uses three distinct rows with the same timestamp and
queries with `limit=1`. It starts a fresh candidate for each budget in order
from 1 through 2048. Each budget below the first cursor must return only the
work-budget error (HTTP 503 with no NDJSON or cursor). The candidate that issues
the first cursor must serve all three rows in append order through a terminal
page at that budget. The production Go client's `FetchPage` validates the HTTP
body framing, NDJSON records, and continuation header before the probe checks
fixture rows and cursor progress.

The sparse fixture filters four matches among 42 interior and trailing nonmatches.
The first request asks for four rows under a 96-byte response cap, which fits
exactly two of the matching records. Continuations lower the row limit to one
and must return the omitted match and the late match in order, with no cursor
on the terminal page. The probe bisects budgets 1 through 2048, assuming
monotone first-page work metering on the fixed snapshot. Each trial that
issues a cursor must reach exhaustion at its own budget. When a continuation
breaks, the case fails and the sparse search can still narrow the issuing
budget using the valid first page. An invalid first page stops the search.

The `work_budget` case keeps the budget-1 rejection evidence. Its
`fixed_budget_walk` object keeps `first_cursor_budget` as the lowest observed
issuing budget, with `pages` and `rows` counting verified progress at that
budget. `rejected_budgets` counts clean first-page rejections among the sampled
budgets. Its nested `sparse_walk` includes the same fields plus
`response_byte_cap`, `first_limit`, `first_rows`, and `resume_limit`.

Both walks include `probes` in execution order. Each probe records a budget,
verified page and row counts, and an outcome of `clean_rejection`,
`issued_cursor`, `exhausted`, or `indeterminate`. The report includes
`exact_threshold_budget` when the issuing budget is 1 or the search observed
a clean rejection at its immediate predecessor. `first_failure` and
`first_broken_continuation` retain the first observed failures in execution
order, with the budget, page, requested limit, and HTTP status where available.

## Restored parity fixture

The Linux test `TestReleaseRestoredParity` publishes a checkpoint and appends
a WAL tail with overlapping times, ties, duplicates, and backfill. It exports,
verifies, and restores that capture before starting the pinned candidate.
Fresh walks use `FetchPage` and the independent append-log oracle across page
sizes and literal filters with a 256-byte response cap. The test holds a cursor
from before the append, requires a 204 for the new row, and checks the
augmented view. It starts opt-in loopback compaction and requires publication
of a newer generation at the requested cutoff. The held walk must exhaust at
its original cutoff. Fresh walks after publication and after restart must
exhaust at the augmented cutoff.

On restart, a wire probe using a cursor from the old process must receive a
framed HTTP 400 with the exact `invalid cursor\n` body and no continuation
header or trailer. The proof records each verified row count, generation
replacement, stale cursor rejection, and fixture removal. Walk failures retain
the first failing stage, filter, page, limit, verified offset, and HTTP status
when available. The producer writes the report after process and fixture
cleanup and exits nonzero on failure.

The native jobs pass `-arch` to `go run ./verify/release validate` and require
all four reports. The original `validate` invocation without `-arch` and the
producer CLI continue to accept schema v1. To revalidate downloaded bundles,
keep their artifact names as subdirectories of one evidence directory.

```sh
go run ./verify/release qualify -evidence /path/to/bundles \
  -revision "$REVISION" -fixture linux-release-v1
```

## Architecture handoff for #61

The Linux production graph gains the direct import
`bog/verify/release` → `bog/internal/client`. Register that edge in
`internal/archtest` and the system architecture diagram in the composed PR.
