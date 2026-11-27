# ADR 0037 — Independent recovery-view consumers

**Status** — Accepted. Runtime wiring and native qualification are pending.

## Decision

`internal/backup` validates the generation and suffix for export, image
verification, and offline reads. `internal/checkpoint` validates the
generation and suffix for advancement and publication. `internal/history`
may use checkpoint validation for generation scans and pins. Backup and
checkpoint/history must have no direct or transitive Linux production
import path to each other, including through local or external relays.
They may share row types from `internal/store`. `cmd/bog` can compose
both consumers with the WAL owner. `cmd/bog-verify` can consume backup's
read-only APIs without acquiring live storage authority.
[Command-owned storage composition](0026-command-owned-retained-storage-composition.md),
[Operator verification boundary](0011-operator-verification-boundary.md)

## Pinned view and reader ownership

The WAL owner captures one validated generation and its exact committed
suffix under the same synchronization that orders durable append
publication and generation activation. The view binds the manifest
identity to the backing WAL identity or tail epoch, logical byte and
first-free ordinal bounds, and the exact suffix digest. Readers expose the
captured manifest and its named segments, and `OpenSuffix` starts at the
covered offset and ends at the captured committed end. Later appends and
replacements leave that view stable. A full WAL and a rebased tail use
the same logical coordinates.

The caller holds the operation pin until the consumer returns, including
error and cancellation cleanup. Each successful open returns an independent
reader at its initial position. Concurrent opens on a shared view must be
safe. The consumer closes its reader and reports close errors. A failed
open cleans up partial acquisition. Release or expiry stops new operations
while admitted work keeps its references. Reclamation waits for durable
selection of the successor and final reference drain.
[Tail-only recovery contract](0035-tail-only-retained-recovery-contract.md),
[Generation pins](0002-bounded-history-generation-pins.md)

## Adapter cost and enforcement

The command supplies separate adapters for the package-local `RecoveryView`
interfaces. Their `Point` methods return distinct Go types with the same
fields, so each adapter translates the captured metadata and forwards reader
opens to the pinned view. The command owns reference transfer and cleanup.
Independent validation keeps the offline image reader and checkpoint
publication independently usable. It costs adapter code and requires
coordinating changes to the common reader semantics and evidence for both
validation paths.

Architecture enforces the boundary through the storage authority fitness
rule on the registry and the measured Linux amd64 and arm64 production
graphs. The Storage wiring PR records exact import changes and promotes
staged roles after the two inventories and activation evidence agree with
the composed revision. Release Verification qualifies that revision on
both native runners using the existing candidate-bound parity report.
[Recovery-view fitness fixtures](../../internal/archtest/recovery_view_boundary_test.go),
[Release evidence](../architecture/SYSTEM.md#release-evidence)
