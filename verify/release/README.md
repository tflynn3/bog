# Linux release verification

Dispatch the **Linux release verification** workflow with the full commit SHA.
The workflow builds the candidate once with Go 1.27.0 on Ubuntu 24.04 amd64 and
uses the resulting absolute binary path for every verifier. Download the
`linux-release-evidence` artifact for the revision, SHA-256, build metadata,
fixture, runner and filesystem configuration, reports, producer exit statuses,
and logs.

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

The durability report's `enospc` case requires the following `evidence` object.

```json
{
  "errno": "ENOSPC",
  "disposable_local_filesystem": true,
  "observed_enospc": true,
  "recovered_after_space_freed": true
}
```

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
503 on a scan for an absent string.

## Restored parity fixture

The Linux test `TestReleaseRestoredParity` publishes a checkpoint and appends
a WAL tail with overlapping times, ties, duplicates, and backfill. It exports,
verifies, and restores that capture before starting the pinned candidate.
Fresh HTTP cursors must reproduce the exact append-order model across page
sizes and literal filters with a 256-byte response cap. The candidate must
acknowledge an ingest on the restored image, then return the augmented model
after restart. The test writes a report bound to the same candidate identity.

The workflow passes `-parity parity.json` when validating the collected reports
and runs the verifier tests with the race detector on Linux.
