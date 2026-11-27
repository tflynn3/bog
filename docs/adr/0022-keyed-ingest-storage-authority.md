# ADR 0022 — Keyed-ingest storage authority

**Status** — Proposed for Cycle 110 review. Architecture within Core
Engineering owns this decision and its fitness test, with Storage review.
`internal/idempotency` remains staged under `bog-storage`.

## Decision

`internal/idempotency` owns keyed retry coordination and the `Backend`
interface. `internal/idempotency` and `internal/wal` must have no Linux
production import path to each other, directly or through registered
root-module intermediaries. `cmd/bog` is the only production package whose
import graph may reach both. At activation, a command-local adapter
implements `Backend` and supplies it to `idempotency.New`. The coordinator
serializes overlapping submissions for a scoped key within one process and
consults the backend for every outcome. The WAL owner holds the pinned
directory lock and owns the authoritative decision across coordinators and
restarts.
[Runtime storage composition](0007-runtime-storage-composition.md),
[Backend contract](../../internal/idempotency/backend.go)

The command translates `CommitRequest` into the prepared batch and WAL-native
retry metadata. Preserve the operation namespace, canonical tenant, stream,
and principal IDs, client key, attempt ID, fingerprint, and policy version.
Keep the scoped key components distinct or encode them unambiguously. The
adapter supplies the request's `Now` and `RecordAt` semantics at the WAL
owner's commit decision, after queuing and preparation. The owner uses the
same time sample to check the current record's expiry and to set the commit
time and absolute expiry for a fresh record. WAL owns its durable encoding
and retry index using its own types.

The adapter maps authoritative WAL decisions to `BackendResult`. For
`BackendStored` and `BackendExisting`, it returns the authoritative record
with its original timestamps and a nil error. `Resolve` translates the
scoped key and exact attempt ID into a WAL lookup without appending. Map to
`BackendRolledBack` only when WAL proves the attempt never published and
cannot commit later. An absent or expired record alone leaves the outcome
`BackendUnknown`. `DeleteExpired` forwards the cutoff and candidate limit to
the WAL owner, which checks the current record and expiry atomically with
deletion. The adapter returns the actual deletion count, including partial
progress on error. Committed rows survive retry metadata cleanup.

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
Fixtures reject direct and relayed dependencies in either direction between
WAL and idempotency. They also reject any other production package that
directly or transitively reaches both, including the relay when it combines
both dependencies. The permitted fixture models command-local injection
with the command importing both the coordinator and WAL. The architecture
suite checks the registry against the Linux amd64 and arm64 production
inventories. Registry promotion belongs to the #53 runtime wiring change.
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
