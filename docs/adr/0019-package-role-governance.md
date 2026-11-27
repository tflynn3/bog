# ADR 0019 — Package role parity

**Status** — Proposed for Cycle 103 review.

## Context

The Go fitness registry records package roles and exact production imports.
SYSTEM.md documents those roles for reviewers. A check limited to staged
rows can miss an incorrect wired role, a missing verifier, or duplicate
rows elsewhere in the table. During #53, a stale role can hide whether a
retained library has entered the server's production dependency tree.
[Package registry](../architecture/SYSTEM.md#package-registry),
[Retained-history runtime cutover](0015-retained-history-runtime-cutover.md)

## Decision

Require exactly one row with the matching role for every Go package in the
root-module fitness registry, including test-only packages. Use the audited
SYSTEM.md labels below.

| Fitness registry role | SYSTEM.md role |
| --- | --- |
| Server command | Server command |
| Runtime library | Wired |
| Staged | Staged |
| Standalone | Standalone |
| Test-only | Test-only |
| Verifier command for `cmd/bog-verify` | Operator command |
| Verifier command for an evidence producer | Verifier command |
| Verifier support | Verifier support |

The package table also permits `bench/segment-layout` as `Nested-module benchmark`
and `.github/workflows/linux-release.yml` as `Release workflow`. Those rows
are outside the root-module Go inventory and must use their assigned labels
and appear at most once. Reject other unregistered entries. The role check
reads the package registry table. The Linux inventory checks continue to
verify exact imports, cycles, package kinds, and server reachability. Each
staged package still requires an owner and activation criteria.
[Role parity check](../../internal/archtest/system_registry_test.go),
[Linux architecture registry](../../internal/archtest/architecture_test.go),
[Staging declarations](../../internal/archtest/staged_packages_test.go)

## #53 promotion handoff

Architecture owns the fitness registry and SYSTEM.md. `bog-storage` owns
#53 composition and hands off the composed revision to Architecture for
review. Recheck the applicable write leases at dispatch and handoff.

On the composed revision, measure direct root-module production imports
and transitive reachability from `cmd/bog` on Linux amd64 (`GOAMD64=v1`)
and arm64 (`GOARM64=v8.0`). Use the architecture suite's `go list -deps`
settings with `CGO_ENABLED=0`, `GOENV=off`, empty `GOFLAGS`,
`GO111MODULE=on`, and `GOWORK=off`. Review both inventories against the
same registered graph before changing roles.

Promote checkpoint, history, and backup individually when the measured
server graph reaches each library on both targets and its activation
criteria are met. Operator or release verifier use alone leaves a library
staged. Keep idempotency staged until keyed-ingest activation passes,
including atomic WAL commit and recovery of retry records with their batches.
In the wiring PR, update the registered edges and roles, update the measured
revision, diagram, and package roles in SYSTEM.md, and remove the staging
declaration for each promoted library. Review those edits with the runtime
wiring, then run `go test -count=1 ./internal/archtest` and
`GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./...` on the composed tree.
Native release qualification still requires both Linux jobs to pass on the
same clean composed revision.
[Fitness ownership](../architecture/SYSTEM.md#fitness-and-registry-ownership),
[Cutover integration and qualification](0015-retained-history-runtime-cutover.md#integration-and-qualification)
