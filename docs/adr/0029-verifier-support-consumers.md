# ADR 0029 — Consumer ownership for verifier support

**Status** — Proposed for Cycle 122 review.

## Context

The architecture registry records verifier support packages and their exact
Linux production imports. The independence rules protect the query model and
stream-policy runner from implementation dependencies. A verifier support
package can still satisfy those rules after its last consumer disappears.
[Release verifier composition](0008-release-verifier-composition.md),
[Release evidence process boundary](0013-release-evidence-process-boundary.md)

## Decision

Every package registered as verifier support needs a production path from
a registered evidence command or a named test consumer. Follow direct and
transitive production imports from the evidence command roots in the
existing registry. The server and the offline operator command are outside
this evidence root set. An import from an unrooted library supplies no
production ownership. Keep the existing independence and exact-import rules.
[Linux architecture registry](../../internal/archtest/architecture_test.go),
[Release evidence boundary](../../internal/archtest/release_evidence_boundary_test.go),
[Standalone consumer decision](0014-standalone-package-consumers.md)

| Verifier support package | Consumer |
| --- | --- |
| `verify/capacityevidence` | Production imports from `verify/release` and `bench/retained-history` |
| `verify/listener` | Production imports from `verify/release` and `bench/retained-history` |
| `verify/query-model` | Direct test import from `verify/release` |
| `verify/stream-policy` | In-package `TestReferenceScenario` |

Keep test consumer declarations beside the fitness check in a separate
table. For an importing consumer, check the named package's direct
`TestImports` and `XTestImports`. For an in-package consumer, inspect the
selected `TestGoFiles` for the named top-level test with a Go test signature.
Use Go's Linux amd64 and arm64 build selection with cgo disabled for both
checks. A declaration becomes stale if the support package is removed,
changes role, gains an evidence command production path, or loses its
declared test consumer on either target. Update the declaration with the
consumer change so review can verify the new owner.
[Verifier consumer fitness test](../../internal/archtest/verifier_consumers_test.go)

## Integration and review

Architecture within Core Engineering owns the ADR and fitness check.
Release Verification reviews the consumer declarations and checks the
continued independence of the evidence producers. Run
`go test -count=1 ./internal/archtest` on the composed #53 revision. The
fitness suite checks build selection and ownership. The native Linux jobs
qualify that same clean revision on amd64 and arm64.

For the owner of the leased #53 SYSTEM.md update, link this ADR from the
package registry and link `TestVerifierSupportConsumers` from the fitness
section. Record which support packages are reachable from evidence commands,
which tests own the other packages, and that both Linux target inventories
validate the test declarations separately from production imports.
[Package registry](../architecture/SYSTEM.md#package-registry),
[Fitness and registry ownership](../architecture/SYSTEM.md#fitness-and-registry-ownership)
