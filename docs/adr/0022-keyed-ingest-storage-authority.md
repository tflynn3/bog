# ADR 0022 — Keyed-ingest storage authority

**Status** — Proposed for Cycle 110 review. Architecture within Core
Engineering owns this decision and its fitness test, with Storage review.
`internal/idempotency` remains staged under `bog-storage`.

## Decision

`internal/idempotency` owns keyed retry coordination and the `Backend`
interface. It must have no direct or transitive root-module Linux
production import path to `internal/wal`, including through registered
intermediaries. At activation, `cmd/bog` supplies a WAL-backed implementation
to `idempotency.New` from outside the coordinator. The coordinator serializes
overlapping submissions for a scoped key within one process and consults the
backend for every outcome. The WAL owner holds the pinned directory lock and
owns the authoritative decision across coordinators and restarts.
[Runtime storage composition](0007-runtime-storage-composition.md),
[Backend contract](../../internal/idempotency/backend.go)

The WAL owner serializes the active-record check with the append decision.
A fresh keyed commit puts the complete batch and its retry record under one
durable commit boundary. The record carries the scoped key, attempt ID,
fingerprint, applicable policy version, commit time, and absolute expiry.
Publish the whole batch before returning a confirmed success. On startup,
recover both the rows and the active retry index from the same committed
state under the owner lock before serving traffic. Before the commit boundary
neither part is recoverable as committed. After it both are recoverable even
if the response was lost. Keep the original expiry through restart and
retained generation replacement. Resolve uncertain outcomes from committed
authority or fence the affected key while the outcome is unknown.
[Ingest retry contract](../ingest-retries.md#wal-transaction-and-recovery)

Requests without `Idempotency-Key` continue through the existing unkeyed
ingest path with its existing validation, append, and response behavior.
Legacy WAL entries replay without retry records. Intentional duplicate rows
remain part of each committed batch.

## Fitness and activation evidence

The storage authority fitness rule follows registered production imports.
Fixtures reject both a direct WAL import and a path through a registered
relay. The permitted fixture models command-local injection with the command
importing both the coordinator and the WAL. The architecture suite checks
the registry against the Linux amd64 and arm64 production inventories.
[Storage authority fitness test](../../internal/archtest/storage_authority_test.go),
[Architecture suite](../architecture/SYSTEM.md#fitness-and-registry-ownership)

Before promoting the staged package, exercise the production WAL through
HTTP. Concurrent same-key, same-body submissions must publish exactly one
whole batch. Reuse of the active key with a different body must return 409
without an append. Drop the response after a durable commit, kill the
server, and reopen the same data directory in a new process. Within the
original retry window, the exact retry must return 204 and a conflicting
body must still return 409. Query the recovered rows and verify that neither
retry adds rows and that the original batch retained its intentional
duplicates. Also verify legacy WAL replay and unkeyed ingest across restart.
[Production acceptance cases](../ingest-retries.md#production-acceptance),
[Staging declaration](../../internal/archtest/staged_packages_test.go)
