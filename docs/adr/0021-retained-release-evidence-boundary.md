# ADR 0021 — Retained storage boundary for release evidence

**Status** — Proposed for Cycle 107 review.

## Context

#53 brings retained checkpoint publication, history scans, and writable
restore into the shipped server. #87 validates capacity and writable-restore
evidence for that candidate. Calling the retained libraries from a producer
could bypass the candidate's recovery, publication, or query behavior and
give the probe and the implementation the same failure mode.
[Retained-history runtime cutover](0015-retained-history-runtime-cutover.md),
[Shared capacity evidence](0020-shared-capacity-evidence.md)

## Decision

Extend ADR 0013 for `verify/release`, `verify/linux`, and
`bench/retained-history`. Their root-module Linux production imports must
have no direct or transitive path to `internal/checkpoint`,
`internal/history`, or `internal/backup`, including through a registered
intermediary. The restriction applies by package path after promotion to
wired runtime libraries. The existing exclusions for `internal/wal`,
`internal/httpapi`, and `internal/readiness` still apply.
[Release evidence process boundary](0013-release-evidence-process-boundary.md)

Producers observe the separate pinned server through HTTP, process
lifecycle, and disposable filesystem fixtures. They may use the production
HTTP client and shared row types. Release tests may import storage packages
to construct fixtures. The restored-parity verdict comes from exercising
the pinned candidate and comparing its results with the independent
append-log oracle. `cmd/bog-verify` retains its read-only backup APIs for
offline verification and operator parity.
[Release verifier composition](0008-release-verifier-composition.md),
[Read-only operator verification](0011-operator-verification-boundary.md)

## Trade-offs and fitness

Process-level probes exercise recovery and publication through the shipped
binary. They cost startup time and disposable resources, and failures must
be diagnosed from observable state and logs. Sharing client transport and
row types avoids duplicating those contracts while keeping expected query
results independent of server storage code.

The architecture suite checks exact imports against Linux amd64 and arm64
production inventories. The release evidence rule follows registered edges
from each producer. Fully registered fixtures cover direct imports and
imports through verifier support after the retained libraries are wired.
The native release jobs check candidate identity, behavior, and cleanup.
[Release evidence fitness test](../../internal/archtest/release_evidence_boundary_test.go)

## SYSTEM.md handoff

Architecture and Release Verification own the boundary. Coordinate the
SYSTEM.md update with Storage during the leased #53 architecture refresh.
Recheck leases at dispatch and handoff. In Release evidence, record the
three additional exclusions, their direct and transitive scope, and the
allowances for the production client, shared row types, and test fixtures.
Link this ADR and the existing release evidence fitness rule. Keep the
operator backup reader in the Operator verification boundary. Review the
measured graph and role promotions in the composed #53 change under ADR
0019. Run `go test -count=1 ./internal/archtest` and
`GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./...` on the composed tree
before the native release gate.
[System architecture](../architecture/SYSTEM.md#release-evidence),
[Package role promotion handoff](0019-package-role-governance.md#53-promotion-handoff)
