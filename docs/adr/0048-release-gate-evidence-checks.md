# ADR 0048 — Release gate evidence checks

**Status** — Implemented for Cycle 189. Native qualification of the composed
#108 revision is pending.

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
[System architecture](../architecture/SYSTEM.md#release-evidence)
