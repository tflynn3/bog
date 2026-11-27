# ADR 0026 — Command-owned retained storage composition

**Status** — Cycle 135 integration contract. The package boundaries are
enforced on the measured base. #53 command composition awaits a Storage
handoff and native qualification.

## Decision

`cmd/bog` composes the WAL owner with checkpoint, history, and backup for
retained queries, generation replacement, and writable restore. The WAL
owner holds the pinned directory lock and serializes durable append
publication, capture, and activation. Command-local adapters use that
authority to supply readers, bounds, and pins to the retained libraries and
inject the retained page source through `httpapi.NewWithPageSource`.
[Runtime storage composition](0007-runtime-storage-composition.md),
[Retained-history runtime cutover](0015-retained-history-runtime-cutover.md)

At startup the command borrows the locked root for
`backup.VerifyPinnedRestore`, admits replay before suffix I/O, and owns
the verified absolute-offset reader through replay. The command installs
durable writable authority before opening the listener. For queries, it
pairs a generation pin with a captured WAL end under the owner's
synchronization and passes HTTP's work meter through retained and tail
scans. The loopback retirement control delegates to the API that issued
the cursor. The API stops new resumes and releases the capture after
admitted requests drain. The command drains HTTP and closes the API before
closing storage.
[Storage interfaces](0015-retained-history-runtime-cutover.md#runtime-interfaces),
[Cursor retirement](0031-retained-cursor-retirement.md)

`internal/wal` must have no direct or transitive root-module Linux production
import path to `internal/checkpoint`, `internal/history`, or
`internal/backup`, including through registered intermediaries. These
libraries have the same exclusion in the reverse direction. The WAL and
`internal/idempotency` are also mutually excluded. Idempotency remains
staged while keyed-ingest activation awaits its own durable retry proof.

`cmd/bog` is the only production package whose import graph may reach both
`internal/wal` and any of checkpoint, history, or backup. The fitness rule
reports the package combining those dependencies and the retained packages
it reaches, even when either path runs through a registered relay. It follows
registered root-module production imports, which the architecture suite
compares with the Linux amd64 and arm64 inventories.

Readiness can observe the WAL outcome independently. HTTP uses the injected
page source. The suite checks HTTP and readiness against the measured
inventories for direct or transitive reachability to retained storage. The
read-only operator graph may reach backup without WAL. Release evidence
producers exercise a separate pinned server and exclude retained libraries
from their production graphs.
[Storage authority fitness test](../../internal/archtest/storage_authority_test.go),
[Keyed-ingest storage authority](0022-keyed-ingest-storage-authority.md),
[Release evidence boundary](0021-retained-release-evidence-boundary.md)

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
#53 command wiring, Query owns paging and cursor retirement, and Release
Verification owns the native Linux evidence. Recheck write leases at
dispatch and handoff. Measure both Linux production inventories and run
the architecture suite and Linux amd64 cross-build on the immutable
composed revision. Review the registry and wiring together. Qualify the
same clean revision on native Linux amd64 and arm64 for exact query order,
fixed-budget cursor progress, durable replay through writable restore,
and the 196,608- and 393,216-row workloads under 128 MiB with swap disabled.
[System architecture and release evidence](../architecture/SYSTEM.md#release-evidence)
