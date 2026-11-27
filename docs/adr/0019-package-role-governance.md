# ADR 0019 — Package role parity

**Status** — Enforced by the architecture fitness suite. Cycle 158 extends
the check to Mermaid subgraph membership.

## Context

The Go fitness registry records package roles and exact production imports.
SYSTEM.md documents those roles for reviewers. A check limited to staged
rows can miss an incorrect wired role, a missing verifier, or duplicate
rows elsewhere in the table. A diagram with exact nodes and imports can
still misstate whether a retained library has entered the server's
production dependency tree.
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
| Verifier command for `cmd/bog-verify` or `cmd/bog-export` | Operator command |
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

Require the Mermaid Runtime subgraph to contain exactly `cmd/bog` and its
transitive root-module production dependencies in the registry. Require
Staged to contain exactly the packages registered as staged. Check
membership alongside every production node and direct import edge. Declare
edges between groups at the top level so each grouped edge has both
endpoints declared in that group.
[Diagram fitness check](../../internal/archtest/system_diagram_test.go)

## #108 promotion handoff

Architecture in the bog AgentSet owns the fitness registry and SYSTEM.md.
`bog-storage` owns #108 recovery and publication and supplies the reviewed
Storage SHA. Storage owns runtime composition after the command lease
clears. `bog-query` owns the retained HTTP paging contract, and Release
Verification owns the native release gate. Recheck the applicable read
and write leases at dispatch and handoff.

At baseline `a720df0d818b19ba3359aa4226b8b844c8f5e063`, the Linux server
closure is `cmd/bog`, `internal/httpapi`, `internal/readiness`,
`internal/store`, and `internal/wal`. Checkpoint, history, backup, and
idempotency remain staged. Preserve the `cmd/bog-verify` import of backup
for read-only image verification.

Compare the Storage handoff and composed revision by measuring direct
root-module production imports and transitive reachability from
`cmd/bog` on Linux amd64 (`GOAMD64=v1`)
and arm64 (`GOARM64=v8.0`). Use the architecture suite's `go list -deps`
settings with `CGO_ENABLED=0`, `GOENV=off`, empty `GOFLAGS`,
`GO111MODULE=on`, and `GOWORK=off`. Review both inventories against the
same registered graph before changing roles.

Promote checkpoint, history, and backup individually when the measured
server graph reaches each library on both targets and its activation
criteria are met. Operator or release verifier use alone leaves a library
staged. Keep idempotency staged until keyed-ingest activation passes,
including atomic WAL commit and recovery of retry records with their batches.
In the command wiring PR, update the exact imports and roles in
`internal/archtest/architecture_test.go`, remove each promoted library's
declaration from `internal/archtest/staged_packages_test.go`, and update the
measured revision, diagram, and role table in `docs/architecture/SYSTEM.md`.
Review those edits with the runtime wiring and preserve the operator and
evidence registrations. On the composed tree, run
`go vet ./internal/archtest`, `go test -race -count=1 ./internal/archtest`,
and `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./...`.
Native qualification remains pending. Both native Linux jobs must qualify
the same clean composed revision, using one SHA-256-pinned binary per
architecture for the evidence producers and strict validation.
[Fitness ownership](../architecture/SYSTEM.md#fitness-and-registry-ownership),
[Cutover integration and qualification](0015-retained-history-runtime-cutover.md#integration-and-qualification)
