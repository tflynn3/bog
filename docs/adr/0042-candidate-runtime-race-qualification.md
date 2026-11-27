# ADR 0042 — Candidate runtime race qualification

**Status** — Workflow change for Cycle 182. Native qualification awaits the
reviewed #108 composition.

## Decision

On native Linux amd64 and arm64, use the candidate checkout's verified SHA
to run `go test -race -count=1 ./cmd/bog ./internal/history ./internal/readiness`.
These suites exercise the candidate's command lifecycle, generation pinning and
reclamation, and readiness probe lifecycle. Running from the candidate checkout
compiles the implementation and tests from the reviewed revision together,
including its Linux-only code and any lifecycle tests introduced with #108.
The existing race runner bounds the invocation and records the checkout SHA,
architecture, package list, test output, and exit status in `verifier-race.log`
in each `linux-release-evidence` artifact. A failed runtime run fails its native
job and blocks the combined qualification. Keep the separate three-repetition
checks for queued WAL shutdown and the checkpoint segment ceiling.
[Linux release workflow](../../.github/workflows/linux-release.yml)

The gate checkout owns the independent query oracle and both strict validators.
It supplies the expected results and acceptance rules while the black-box probes
exercise each runner's checksum-pinned release binary. This preserves independent
checks of candidate identity, listener ownership, and process teardown. Both the
runtime package tests and the black-box evidence are required.
[Release verifier composition](0008-release-verifier-composition.md),
[Release evidence process boundary](0013-release-evidence-process-boundary.md),
[Candidate listener ownership](0017-release-candidate-listener-ownership.md)

## Qualification handoff

Land the workflow independently of Storage's #108 integration. Once the
composed revision is reviewed, dispatch the updated gate with its full SHA.
Require both native jobs and both strict validators on each job to pass for
196,608 and 393,216 seed rows under the 128 MiB memory cap with swap disabled
and zero OOM kills. Historical schema-v1 reports remain readable under standard
validation.
[Capacity evidence validation](0018-capacity-evidence-validation.md),
[Runtime constructor boundary](0040-retained-runtime-constructor-boundary.md)
