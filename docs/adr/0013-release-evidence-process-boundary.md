# ADR 0013 — Release evidence process boundary

**Status** — Accepted.

## Context

Release qualification measures the behavior of the shipped server. Each
native Linux runner qualifies the same clean revision with its own
SHA-256-pinned `cmd/bog` binary. In-process calls into server packages could
tell us whether an implementation method works while bypassing the startup,
HTTP, recovery, or readiness behavior users actually depend on.
[Release verifier composition](0008-release-verifier-composition.md)

## Decision

`verify/release`, `verify/linux`, and `bench/retained-history` run
separately from the server. They exercise the pinned process through its
HTTP interfaces, process lifecycle, and disposable filesystem fixtures.
The server owns writable recovery, request handling, and readiness. Each
producer must be registered as a verifier command and have no direct or
transitive root-module Linux production import path to `internal/wal`,
`internal/httpapi`, or `internal/readiness`. The rule applies even when
every edge through an intermediary is registered.
[Command root boundaries](0012-command-root-boundaries.md)

The production HTTP client and shared row types remain valid dependencies.
The query oracle retains its independence under ADR 0008. Test-only imports
remain outside the production graph. For example, the release tests may use
`internal/httpapi` to construct an admission fixture and `internal/wal` to
construct a restored-parity fixture. Restored-parity evidence comes from
starting the pinned binary on that fixture, observing an acknowledged
append, and comparing the exact rows after restart.
[Release admission test](../../verify/release/admission_linux_test.go),
[Restored parity test](../../verify/release/parity_linux_test.go)

## Consequences and fitness

Running probes across the process boundary costs startup time, requires
disposable resources and cleanup, and limits diagnosis to observable
behavior and logs. It makes release evidence exercise the actual startup,
request handling, durability, and shutdown of the process being qualified.

The architecture suite validates exact production imports for Linux amd64
and arm64. The release evidence check follows the registered production
edges from each evidence root and fails when it reaches an excluded
package. Its fixtures cover client and row sharing, direct violations, and
violations through a helper whose edges pass the exact-import check.
This fitness rule covers dependency direction. The release workflow checks
binary identity, behavior, and cleanup on the native runners.
[Linux architecture registry](../../internal/archtest/architecture_test.go),
[Release evidence fitness test](../../internal/archtest/release_evidence_boundary_test.go)

## SYSTEM.md handoff for #53

Link ADR 0013 from the Release evidence section during #53's leased system
architecture refresh. Record that all three producers exercise a separate
pinned server process and have no direct or transitive root-module
production path to the WAL owner, server HTTP, or readiness. Link the
release evidence fitness test from the fitness section. Recheck the
applicable leases before dispatch and before the #60 handoff. On the
composed tree, run the portable architecture suite and Linux cross-build
before dispatching the native release gate.
[System architecture](../architecture/SYSTEM.md#release-evidence)
