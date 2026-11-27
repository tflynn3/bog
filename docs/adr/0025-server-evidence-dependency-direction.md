# ADR 0025 — Server-to-evidence dependency direction

**Status** — Proposed for Cycle 112 review.

## Context

The #53 cutover composes retained history and writable restore in `cmd/bog`.
Release qualification exercises the pinned server binary through HTTP,
process lifecycle, and disposable filesystem fixtures. Keeping evidence
producers and their support code outside the server dependency graph keeps
probe behavior out of the shipped runtime and lets the query oracle derive
expected results independently. ADR 0013 and ADR 0021 govern the direction
from evidence producers to server packages.
[Retained-history runtime cutover](0015-retained-history-runtime-cutover.md),
[Release evidence process boundary](0013-release-evidence-process-boundary.md),
[Retained storage boundary for release evidence](0021-retained-release-evidence-boundary.md)

## Decision

Extend the `cmd/bog` rule in ADR 0012. The server's registered root-module
Linux production imports must have no direct or transitive path to a
package in `verify` or `bench`, including packages at those roots and at
any depth below them. The rule matches package paths regardless of registry
role, including when the destination is registered as a runtime library or
reached through a registered intermediary. The `internal/client` exclusion
still applies. The Linux amd64 and arm64 inventories validate the registered
production edges. Test imports and nested modules are outside that registry.
[Command root boundaries](0012-command-root-boundaries.md),
[Server boundary fitness test](../../internal/archtest/command_roots_test.go),
[Linux architecture registry](../../internal/archtest/architecture_test.go)

## Shared packages and trade-off

Both the server and evidence producers can depend on neutral packages such
as `internal/store` for shared row types, subject to their existing boundary
rules. The operator command also uses read-only backup APIs. If the server
and a producer need the same new primitive, place the narrow contract in an
appropriate shared package outside the evidence trees and keep probe logic
and expected-result calculation with the producer. Extracting a shared
contract costs design and review effort, and shared code can give runtime
and evidence a common failure mode. Keep the independent query oracle in
`verify/query-model`.
[Read-only operator verification](0011-operator-verification-boundary.md),
[Release verifier composition](0008-release-verifier-composition.md)

## Integration and review

Architecture within Core Engineering owns the fitness rule. Review it with
Storage on the composed #53 and #70 revision. Run the portable architecture
suite against that revision to check the registered Linux graphs and the
fixtures for legitimate Storage composition and reclassified evidence
dependencies. The native release gate qualifies the same composed revision
on Linux amd64 and arm64.
[Fitness and registry ownership](../architecture/SYSTEM.md#fitness-and-registry-ownership),
[Native release evidence](../architecture/SYSTEM.md#release-evidence)
