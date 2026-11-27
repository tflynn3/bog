# Linux release verification

Dispatch the **Linux release verification** workflow with the full commit SHA.
Both native jobs must pass. The amd64 job uses `ubuntu-24.04` with
`GOAMD64=v1`, and the arm64 job uses `ubuntu-24.04-arm` with
`GOARM64=v8.0`. Each job builds the candidate once with Go 1.27.0 and
uses the resulting absolute binary path for every verifier. Download
`linux-release-evidence-amd64` and `linux-release-evidence-arm64` for
the revision, SHA-256, build metadata, fixture, runner and filesystem
configuration, reports, producer exit statuses, and logs.

The admission, durability, and capacity producers take
`-binary PATH -revision SHA -fixture ID -report PATH`.
The release fixture ID is `linux-release-v1`. The workflow runs
`go run ./verify/release`, restored parity, the immediate post-restore crash
and fixed-budget backfill tests, and strict report validation from the
workflow checkout (`gate`). It runs
`go run ./verify/linux` (#3) and `go run ./bench/retained-history` (#32) from the
candidate checkout. The query-model race suite runs from `gate`, which
supplies the oracle and backfill test. The sibling commands provision their
Linux fixtures and use the supplied binary. The gate fails if any required
evidence is missing, including on revisions with earlier harness entry points.
The capacity producer runs as the runner user in a systemd service with delegated
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
| Recovery parity | `restored_parity` when supplied, plus `ordinary_wal_reopen` in cutover mode |
| Immediate restored-write crash | `post_restore_ack_crash` in the native workflow |

Admission also records the full saturation fixture verdict as
`saturation_fixture`. Validation accepts archived admission reports with
the original cases. When the fixture verdict is present, it must pass.
Failures include startup, candidate exit and process-group teardown, and
fixture-root removal. The primary probe error appears first when cleanup
also fails. Restored parity includes the same lifecycle and fixture cleanup
checks in its case.

Before sending a health request, the Linux verifier matches the reserved
loopback address and port to listening sockets in the candidate's own file
descriptors. Any matching foreign listener fails the fixture. The verifier
checks ownership again after health and after the workload. It observes
exit independently of probing and reserves the leader PID through
process-group teardown. Routine teardown requests SIGHUP and requires a
matching wait status. Unexpected exits and teardown timeouts fail the
fixture, even if escalation to SIGKILL succeeds.

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

The durability producer also requires both retarget checks to pass before
marking `enospc` as passing. In `healthy_to_full`, readiness stays `ready`
and ingest returns HTTP 204. In `full_to_healthy`, readiness stays
`low_disk_headroom` and ingest returns HTTP 503. The latter also records
`post_ingest_reason: "wal_rollback"` and `post_ingest_category: "enospc"`.

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

For cutover qualification, pass `validate -require-cutover` with the
usual report arguments or set `BOG_RELEASE_REQUIRE_CUTOVER=1`. Both live
records must carry passing clustered and scattered backfill timelines under
`diagnostics.backfill_timelines`. The validator checks acknowledged
backfill, held-cursor exhaustion, fresh visibility, publication at the
expected cutoff, and publication lag bounds against the sampled events.
Both strict validators require at least two timed `memory.current` reads
bracketed by running status for that compaction before publication. They
recompute the reported observation span and byte range and check each read
against the final `memory.peak` for the fixture cgroup. They also require
a positive retired pin count while the cursor is held and an observed zero
after explicit retirement. Standard validation reads archived schema-v1
capacity reports without those timelines.

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

## Fixed-budget backfill fixture

On each native runner, `TestReleaseFixedBudgetBackfill` launches the
pinned candidate in a fresh data directory for each budget attempt. The
tied and duplicate cases start at 11 work units. The sparse case starts
at 314 and requests four rows on the opening page. The 96-byte response
cap fits exactly two of those rows. All three cases resume with limit one.

A clean, framed opening work-budget refusal can advance the search to
twice the previous budget, up to 2,048 units. Once the opening page issues
a cursor, every resume uses the same process and configured budget. The
candidate must acknowledge a backfill with HTTP 204 before the first
resume and again before the terminal resume. The independent append-log
oracle compares each complete page to the original capture in exact order,
including duplicate multiplicity. Any continuation error fails the case.

The workflow requires an explicit passing Go test event on both jobs.
Each artifact retains `fixed-budget-backfill-events.jsonl`,
`fixed_budget_backfill.log`, and the producer's entry in `producers.txt`
with the gate checkout, candidate revision, and exit status. The test
logs the candidate SHA-256, fixture, attempted budgets, acknowledgements,
and accepted walk counts. This evidence accompanies the schema-v1
admission, durability, capacity, and parity reports.

## Restored parity fixture

The Linux test `TestReleaseRestoredParity` publishes a checkpoint and appends
a WAL tail with overlapping times, ties, duplicates, and backfill. It exports,
verifies, and restores that capture before starting the pinned candidate.
Fresh walks use `FetchPage` and must reproduce the exact append-order model
across page sizes and literal filters with a 256-byte response cap. After the
candidate returns HTTP 204 for a backfill on the restored image, the held
cursor must finish at the original capture and fresh walks must match the
augmented ledger. The verifier then checks listener ownership, intentionally
sends SIGKILL, and requires a matching wait status, process-group cleanup,
and reaping within the teardown deadline before restarting the same pinned
binary on the same directory. Fresh walks after restart must match the
augmented ledger in exact order through exhaustion. A raw request using the
pre-crash cursor must receive a framed HTTP 400 with the exact
`invalid cursor\n` body and no continuation header or trailer.
The `restored_parity` evidence records their row counts in `post_append_rows`
and `restart_rows`. The report is bound to the same candidate identity.

The same native test seeds a separate ordinary WAL before launching the
candidate. It checks the reopened rows, requires HTTP 204 for a backfill
batch, and compares the exact augmented ledger before shutdown and after
restart. The `ordinary_wal_reopen` case records the row counts and append
status.

The native workflow uses `-require-cutover -parity parity.json` and runs the
verifier tests with the race detector on Linux. Strict validation requires
both recovery cases to pass and checks these exact evidence fields for
`linux-release-v1`.

| Case | Required evidence |
| --- | --- |
| `ordinary_wal_reopen` | `reopened_rows` = 3, `append_status` = 204, `restart_rows` = 6 |
| `restored_parity` | `captured_rows` = 6, `held_capture_rows` = 6, `post_restore_204` = true, `post_append_rows` = 7, `restart_rows` = 7 |

Historical validation without `-require-cutover` accepts an omitted parity
report or a schema-v1 report with a passing `restored_parity` outcome.

## Immediate post-restore acknowledgment crash

`TestReleasePostRestoreAckCrash` starts the pinned candidate on a fresh
verified checkpoint-and-tail restore. It confirms listener ownership and
posts a two-row batch with timestamp ties and a duplicate. On HTTP 204 it
sends SIGKILL immediately, before reading or closing the response body or
issuing another probe. The verifier requires the matching wait status and
process-group cleanup, then restarts the same binary on the same directory.
A fresh paginated walk must match all eight rows in the independent
append-log oracle in timestamp and append order through exhaustion.

On each architecture the gate requires an explicit passing test event and
a nonempty `post-restore-ack-crash.json`. An absent, skipped, or failing test
fails that native job. In the artifact, look for the top-level test `pass`
event in `post-restore-ack-crash-events.jsonl` and the `post_restore_ack_crash`
entry with `exit=0` and `checkout=gate` in `producers.txt`. The corresponding
`post_restore_ack_crash.log` contains the producer output.

Compare the report's revision, `binary_sha256`, and fixture with `run.txt`
and `binary.sha256`. `run.txt` also identifies the gate revision and native
runner. The report's `post_restore_ack_crash` case must have `status: "pass"`,
the verified restore point, `append_status: 204`, `observed_signal: 9`,
`replay_matched: true`, and `replay_rows: 8`. It also records true values for
`crash_candidate_reaped`, `restart_candidate_reaped`, and `fixture_removed`.

The **Require both native Linux qualifications** job is the combined result
for the dispatched revision. For #53, require it to pass with both native
artifacts for the same candidate and gate revisions. Each native job must
pass immediate crash replay, strict restored parity, ENOSPC retargeting,
fixed-budget backfill, and both live capacity scales under 128 MiB with swap
disabled. [Immediate crash evidence contract](../../docs/adr/0027-immediate-post-restore-crash-evidence.md)

## Architecture handoff for #61

The Linux production graph gains the direct import
`bog/verify/release` → `bog/internal/client`. Register that edge in
`internal/archtest` and the system architecture diagram in the composed PR.
