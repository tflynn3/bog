# ADR 0008 — Release verifier composition

**Status** — Accepted composition decision. At
`9bf89aa4c5a173833bd8fb70d088ac0a257e20a8` the retained runtime wiring and
production-client verifier edge await their domain commits.

## Release gate

#53 remains the retained-history cutover gate. Compose its runtime wiring
with #59 and #61. Qualify the Linux candidate built from the full composed
SHA after the #60 active-filesystem readiness handoff.
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

The reviewed #61 candidate (`820dc5c988a0e79bb4a83227144998ea44cf32d5`)
uses `FetchPage` for fixed-budget walks. A transport observer retains
response metadata for media type, cursor trailers, and clean initial
503 checks. Its added production dependency is on `internal/client`.
The restored-parity test's oracle import stays test-only.

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
Use the architecture registry for package roles, exact direct imports,
orphan governance, and acyclicity. Check oracle independence on the
observed Linux production graph, including transitive paths through
external dependencies.

## Graph updates and handoff

`bog` owns #59, #61 client release integration, and architecture fitness.
`bog-storage` owns #53 and the recovery and publication lifecycle.
Review the ADR 0006 and 0007 interfaces together before composition,
including commit and capture ordering, deterministic replay, expiry, and
reference ownership.

Record the measured revision, diagram, and role registry with the actual
Linux imports on the tree composed from #53, #59, and #61. Promote staged
libraries reached by the server and remove retired edges. Run architecture
fitness and the Linux cross-build on that tree. Slipway runs the Linux
integration verifier. Full qualification stays pending through #60.
[System architecture](../architecture/SYSTEM.md#fitness-and-registry-ownership)
