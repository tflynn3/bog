# ADR 0008 — Release verifier composition

**Status** — Accepted. Native qualification is pending for the final
composed candidate.

## Release gate

#53 remains the retained-history release gate. Qualify one clean candidate
revision composing #53, #59 paging, and #60 descriptor-backed readiness.
Dispatch the workflow from a revision containing the #70 fixed-budget
probes, with the full 40-character lowercase SHA of the final candidate.
The workflow records its own revision and runs admission, restored parity,
and report validation from that checkout. Durability and capacity run from
the candidate checkout.
[Release verifier contract](../../verify/release/README.md)

Both Ubuntu 24.04 runners must pass. The amd64 job uses `ubuntu-24.04`
with `GOAMD64=v1`, and the arm64 job uses `ubuntu-24.04-arm` with
`GOARM64=v8.0`. Each job checks the Linux runner architecture, the kernel
machine, and Go host and target architectures. It confirms a clean
checkout at the requested SHA, builds `cmd/bog` once natively with Go
1.27.0 and cgo disabled, then checks the embedded build and VCS metadata.
Admission, durability, live capacity, and restored parity use that job's
single binary. The validator binds all reports to the candidate SHA, the
job's binary SHA-256, and the `linux-release-v1` fixture. The job rechecks
the digest after running the producers.

The gate requires both capacity fixtures, with 196,608 and 393,216 seed
rows, and both ENOSPC retarget directions on each runner. The durability
producer uses a dedicated absolute `TMPDIR` in a bounded systemd service
with a private mount namespace. The producer owns child-process and mount
cleanup. After it exits, the workflow removes the fixture parent with
`rmdir` to independently require that the producer removed its fixture
root. A leftover directory fails qualification and its path is logged.
Schema-v1 reports remain readable without retarget fields for historical
validation. The native workflow sets `BOG_RELEASE_REQUIRE_RETARGET=1` to
require both directions, including acknowledged ingest and replay on the
healthy filesystem and ENOSPC rollback on the full filesystem.

The jobs upload `linux-release-evidence-amd64` and
`linux-release-evidence-arm64` independently, including on failure. The
artifacts record runner and build identity, reports, producer exit
statuses, and logs. Unavailable native runners, missing mount privileges
or delegated cgroup v2 memory and I/O controllers, and missing reports
leave qualification incomplete. When a runner cannot start, the workflow
job status records the missing run. Both jobs and their validation and
cleanup checks must pass on the final candidate after #60.
[Linux release workflow](../../.github/workflows/linux-release.yml)

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
