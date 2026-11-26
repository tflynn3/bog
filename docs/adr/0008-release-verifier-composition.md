# ADR 0008 — Release verifier composition

**Status** — Accepted composition decision. #61 landed on main at
`3a28f26d322a777a423692f241ac962adf757e8d`. The Linux inventory with #59 and
#61 applied to the pinned base confirms the production verifier edge.
Full runtime composition awaits #53, and qualification follows #60.

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

#61 uses `FetchPage` for fixed-budget walks and restored parity. A transport
observer retains response metadata for media type, cursor trailers, and
clean initial 503 checks. The only measured root-module production import
change is `verify/release → internal/client`. Restored parity filters the
fixture's append ledger and sorts it stably by timestamp to produce its
expected rows. The independent oracle stays outside the verifier's import
graph in this revision.

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

Register the measured client edge with the #61 source in the integration
tree. Once #53 supplies the runtime wiring, measure the combined Linux
imports and run architecture fitness and the Linux cross-build on that tree.
Slipway runs the Linux integration verifier. Full qualification stays
pending through #60.
[System architecture](../architecture/SYSTEM.md#fitness-and-registry-ownership)
