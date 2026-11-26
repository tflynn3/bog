# ADR 0008 — Release verifier composition

**Status** — Cycle 52 decision. Production-client verification and
retained runtime wiring are pending at
`d67892082f0ea3e9a951e55076d5eb80127bf031`.

## Release gate

#53 remains the retained-history release gate. Compose its runtime wiring
with #59 and qualify the Linux candidate built from the full composed SHA.
Admission, Linux durability and active-filesystem readiness, live capacity,
and restored parity all exercise the same binary. The gate binds every
required report to that revision, the binary SHA-256, and the
`linux-release-v1` fixture. Admission and restored parity run from the
workflow checkout. Dispatch from a workflow revision that includes the
production-client probes when qualifying that integration.
[Release verifier contract](../../verify/release/README.md)

ADR 0006 defines fixed-budget continuation, framing, and expiry. ADR 0007
defines the command-owned adapter and its handoff to the WAL owner. ADR 0005
governs activation, coordinated capture, publication, and drain. Keep one
Linux process, one listener, one WAL owner, and one pinned local data directory.
[ADR 0006](0006-fixed-budget-query-continuations.md)
[ADR 0007](0007-runtime-storage-composition.md)
[ADR 0005](0005-pinned-writable-recovery-authority.md)

## Framing and semantic evidence

Use the production Go client in the release verifier to exercise the same
page delivery contract callers use. `FetchPage` exposes a complete,
validated NDJSON page and its opaque cursor together. Use it for probes
that change limits between requests. `Walk` and `WalkFrom` check cursor
progress across successive pages and preserve the failed request's cursor
for retry. Keep each cursor on the issuing process and start fresh walks
after restart. Keep raw HTTP and socket probes for evidence that depends
on the wire, including the slow-reader admission probe and clean rejection
with no NDJSON rows or cursor.

Derive expected rows from fixture inputs and the verifier's ledger of
acknowledged appends. The append-log oracle in `verify/query-model` computes
membership from the captured prefix using half-open time bounds and a
case-sensitive literal, then orders matches by UTC timestamp and append
ordinal. At the verifier boundary, compare decoded rows field by field
and check terminal exhaustion. Use the original cutoff for a held
cursor across generation replacement and the augmented ledger for fresh
walks after acknowledged backfill or post-restore ingest and restart.
Fixed-budget probes also check tied rows and sparse byte-trimmed pages
with a smaller resume limit against their fixture expectations.

The verifier may depend on both the production client and the oracle.
Adapt their types at the verifier boundary. Oracle production imports
must have no direct or transitive path to the server command, its wired
or staged implementation libraries, or `internal/client`. A registered
intermediary still falls under this rule. Test-only adapters may import
implementation packages to exercise them against the independent model.
Use the existing architecture registry for package roles, exact direct
imports, orphan governance, and acyclicity.

## Graph updates and handoff

Core engineering owns #59, client release integration, and architecture
fitness. `bog-storage` owns #53 and the recovery and publication lifecycle.
Review the ADR 0006 and 0007 interfaces together before composition,
including commit and capture ordering, deterministic replay, expiry, and
reference ownership.

Record actual Linux production imports with the changes that introduce or
retire them. Core engineering owns the verifier's production client edge
and any production import of the oracle. For #53 composition, core
engineering and `bog-storage` coordinate the measured revision, diagram,
and role registry under the wiring PR's leases. Measure the graph after
composing #53 and #59, promote the staged libraries reached by the server,
and remove stale entries. Run the architecture suite and Linux build on
the composed tree before dispatching the release gate.
[System architecture](../architecture/SYSTEM.md#fitness-and-registry-ownership)
