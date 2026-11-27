# ADR 0026 — Command-owned retained storage composition

**Status** — Proposed for Cycle 114 review.

## Decision

`cmd/bog` composes the WAL owner with checkpoint, history, and backup for
retained queries, generation replacement, and writable restore. The WAL
owner holds the pinned directory lock and serializes durable append
publication, capture, and activation. Command-local adapters use that
authority to supply readers, bounds, and pins to the retained libraries and
inject the retained page source through `httpapi.NewWithPageSource`.
[Runtime storage composition](0007-runtime-storage-composition.md),
[Retained-history runtime cutover](0015-retained-history-runtime-cutover.md)

`internal/wal` must have no direct or transitive root-module Linux production
import path to `internal/checkpoint`, `internal/history`, or
`internal/backup`, including through registered intermediaries. Each of
those libraries and `internal/idempotency` must also have no production
import path back to WAL. The fitness rule follows registered production
imports, which the architecture suite compares with the Linux amd64 and
arm64 inventories. The WAL rule applies independently of whether HTTP and
readiness import WAL. Their own retained storage exclusions still apply.
[Storage authority fitness test](../../internal/archtest/storage_authority_test.go),
[Keyed-ingest storage authority](0022-keyed-ingest-storage-authority.md)

## Adapter trade-off

Command-local adapters add code at startup and at the capture and
publication seams. The command must translate types, track reference
ownership, and release pins on error and during shutdown. In exchange,
WAL keeps its commit and synchronization contracts independent of retained
formats and query types. The command owns the ordering of calls across
those contracts, including establishing writable authority before opening
the listener and capturing a generation with its matching durable tail.

## Integration and qualification

Within Core Engineering, Architecture owns this rule and ADR, Storage owns
#53 command wiring, Query owns the HTTP paging contract, and Release
Verification owns the native Linux evidence. Recheck write leases at
dispatch and handoff. Run `go test -count=1 ./internal/archtest` on the
composed #53 and #70 revision. Qualify that same revision on native Linux
amd64 and arm64 for bounded capacity, exact query order, fixed-budget
cursor progress, and durable replay through writable restore.
[System architecture and release evidence](../architecture/SYSTEM.md#release-evidence)
