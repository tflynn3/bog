# ADR 0048 — Release gate evidence checks

**Status** — Evidence checks implemented for Cycle 189. The native
retained-shutdown producer is wired into the release gate. Qualification of
the composed #108 revision remains pending on native Linux amd64 and arm64.

## Decision

Release Verification owns the evidence acceptance rules in
`verify/release/check_gate_evidence.py`. The workflow invokes the checker
from the checkout pinned to the workflow SHA. It supplies the expectations
and artifact paths for each native Linux job.

| Check | Inputs | Required evidence |
| --- | --- | --- |
| `producers` | Manifest path, candidate revision, architecture, gate SHA, and newline-separated producer and checkout pairs | Exactly one record for each configured producer, with exit status zero and the expected revision, architecture, checkout, and checkout SHA. |
| `test-events` | Go JSONL event path, Go package, parent test, and zero or more relative subtest names | Exactly one pass each for the package, parent test, and every named subtest. A failure or skip of the package, parent, or any descendant fails the check. |

The workflow owns producer assignment and orchestration, including the
nonempty parity report requirement and artifact publication. It runs the
candidate storage race check in its existing scope and invokes both strict
wire validators from the gate checkout. Each architecture still qualifies
the same reviewed candidate revision using that runner's SHA-256-pinned
binary.
[Linux release workflow](../../.github/workflows/linux-release.yml),
[Release evidence process boundary](0013-release-evidence-process-boundary.md),
[Candidate runtime race qualification](0042-candidate-runtime-race-qualification.md)

## Retained shutdown

Each native job requires the `retained_shutdown` producer from the gate
checkout pinned to the workflow SHA. It runs
`go test -race -json -count=1 -timeout=90s -run '^TestReleaseRetainedShutdown$' ./verify/release`
with `BOG_RELEASE_RETAINED_SHUTDOWN=1` against the runner's pinned binary
for the reviewed candidate revision and the `linux-release-v1` fixture.
The workflow verifies `binary.sha256` before and after the test run. The
test also checks binary identity before launch, before restart, and at
the end of the scenario.

The workflow saves the Go events to `retained-shutdown-events.jsonl`.
Producer success requires exactly one explicit pass for
`TestReleaseRetainedShutdown` and one for `bog/verify/release`. The
checker rejects failures, skips, and missing or duplicate required
passes. The manifest check requires one successful `retained_shutdown`
record in `producers.txt` with the candidate revision, native
architecture, and gate checkout SHA. Both files are published in each
native job's evidence artifact.

The test captures a predecessor with a committed suffix, then holds
its admitted continuation under TCP backpressure. It acknowledges
backfill and publishes the successor at the exact cutoff while the
continuation still occupies the sole query slot and exactly one retired
predecessor pin remains. After SIGTERM, the candidate's listener refuses
new connections while the process remains alive, the response is still
backpressured, and a separate open of the directory cannot acquire the
exclusive lock. The verifier drains the same connection and checks the
complete terminal NDJSON framing, exact row order, and multiplicity.
It requires zero exit status and reaps the original process before
restarting the same pinned binary on the same directory. The restarted
walk must exactly match the independent ledger of all acknowledged rows
through exhaustion, including backfill, timestamp ties, and duplicates.

ADR 0039 also requires Storage's command-level evidence for resource
closure ordering and lock retention on failed drain.
[Retained-shutdown test](../../verify/release/retained_shutdown_linux_test.go),
[Query delivery and shutdown](0039-query-delivery-and-retained-cutover.md#native-retained-shutdown)

## Trade-offs and fitness

A standard-library entry point makes acceptance rules testable with local
fixtures and keeps them tied to the reviewed gate revision. The workflow
and checker share an argument contract, so changes to the qualification
matrix still need review at the call sites. Native preflight runs the CLI
fixtures, which cover accepted evidence, checkout identity mismatches,
missing or duplicate records and passes, and failed or skipped tests.
The fixtures exercise evidence acceptance. Native jobs exercise the
pinned binary and publish the evidence.
[Checker fixtures](../../verify/release/check_gate_evidence_test.py)

## Storage handoff

Link this ADR from the Release evidence section of Storage's leased
`SYSTEM.md` update. Record that the workflow SHA owns producer-manifest
and required Go test-event acceptance while the workflow supplies the
expectations for each native job.

For the running `SYSTEM.md` promotion, use this Release evidence table row.

| Producer | Report | Required evidence |
| --- | --- | --- |
| `verify/release` / `TestReleaseRetainedShutdown` (gate checkout) | `retained-shutdown-events.jsonl`, `retained_shutdown` in `producers.txt` | Require explicit test and package passes under race against each runner's pinned candidate, with SHA-256 verified before and after. Observe a retired predecessor pin and backpressured continuation across replacement, then listener close and directory lock retention during SIGTERM drain. Verify exact terminal NDJSON, successful exit and reaping, and exact replay of acknowledged rows after restart on the same directory. |

Record the composed candidate's native qualification as pending on both
architectures until both jobs pass for the same reviewed revision. The
earlier arm64 QA at `5423535` failed writable restore and loopback
compaction setup. Native amd64 evidence and fresh native runs of the
composed candidate are pending.
[System architecture](../architecture/SYSTEM.md#release-evidence)
