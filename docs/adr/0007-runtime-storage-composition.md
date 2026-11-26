# ADR 0007 — Runtime storage composition

**Status** — Cycle 51 decision for the #53 wiring merge. At base
`5782ab3a08341cfb159a39bbf982f61482f4177c`, the command serves the ordinary
WAL and an in-memory query view. Checkpoint, history, and backup are staged.

This decision specifies the command composition for the
[retained-history cutover](0004-retained-history-cutover.md),
[pinned writable recovery authority](0005-pinned-writable-recovery-authority.md),
and [fixed-budget query continuations](0006-fixed-budget-query-continuations.md).

## Composition and startup

`cmd/bog` composes one WAL owner, one pinned local data directory, and one
HTTP listener in one Linux process. The owner holds the exclusive directory
lock from before layout classification through storage drain. It selects the
recovery authority and serializes durable append publication, capture, and
generation activation. The retained `PageSource` implementation lives in
`cmd/bog` and is injected via `httpapi.NewWithPageSource`. This keeps HTTP's
request, result, and cursor types at the consumer boundary and avoids adding
a production adapter package. The command supplies the storage libraries
with readers, bounds, and pins. Checkpoint, history, and backup must have no
direct or transitive production dependency on `internal/wal`.

| Directory state under the owner lock | Recovery input |
| --- | --- |
| Ordinary WAL or eligible empty directory | Validate the legacy bootstrap or `bog.wal` and `bog.commit`. With no checkpoint, replay the full committed prefix. |
| WAL with an authoritative retained generation | Resolve any interrupted publication, validate the manifest and segments against the pinned WAL identity and exact covered prefix, then replay the committed suffix. |
| Verified restore awaiting writable activation | Validate `bog.backup`, `bog.wal.tail`, and the referenced checkpoint and segments, including ordinal coverage, exact tail size and digest, and complete frames. Reject a mixture with legacy authority. Stage and sync writable files, install their names and sync the directory, then atomically publish the authority and sync the directory again. |
| Activated restore or interrupted activation | Resolve protocol-owned activation state and validate the selected authority and its referenced files. Recover from its current committed end. The verified input remains recoverable through activation. |

Use the same pinned directory for classification, validation, recovery, and
publication. The selected state binds the generation, covered logical WAL
offset and first uncovered ordinal to the WAL identity or epoch and durable
end. Tail-only restore offsets are logical even when its suffix starts at
physical byte zero. Replay complete frames from the checkpoint boundary to
the committed end to recover tail rows and the next global append ordinal.
Validate committed data before truncating uncommitted bytes. Unresolved,
ambiguous, or corrupt authority fails startup before listening. Once
activated, a restore recovers from the service directory alone. `bog.backup`
describes the fixed restore point. The writable authority carries later
commits. Install the recovered view before admitting HTTP traffic.

## Commit, capture, and replacement

1. Validate and append complete frames, sync the WAL, sync and atomically
   install the commit marker or equivalent writable authority, then sync the
   directory. Publish the whole durable group to the query-visible tail
   before acknowledging it with HTTP 204. A confirmed rollback publishes
   no rows. An uncertain commit or rollback fences the owner and requires
   recovery before it serves writes or grants new captures.
2. At capture, synchronize with both append publication and generation
   activation. Pin the validated generation and matching WAL identity or
   epoch together. Fix the covered frame boundary, first uncovered ordinal,
   and exact durable, query-visible tail end. If acquisition fails or is
   cancelled, release every acquired reference. Paging merges checkpoint
   and captured tail by `(UTC timestamp, append ordinal)`. Export consumes
   the same kind of capture and derives its bounds from that view.
3. For replacement, pin a committed whole-frame cutoff and stage and sync
   immutable segments. Publish and sync the new authoritative manifest,
   validate the successor, then install the catalog generation and trim the
   covered rows from the live tail under owner synchronization. Later
   committed rows remain in the live tail. Existing captures retain their
   original generation and tail. If publication is uncertain, preserve both
   recoverable views and fence dependent mutations until recovery validates
   and syncs the selected authority under the owner and publication locks.
   Reclaim only confirmed protocol-owned files unreachable from durable
   authority and all live pins.

Mount the capacity control only when `BOG_CAPACITY_CONTROL=1` and the
listener is loopback. It uses the same owner and activation path. At
`/_capacity/compaction`, POST starts
asynchronous compaction at the requested committed row cutoff. GET reports
progress. A `running` result describes the currently published generation.
Report `complete` after the newer generation is durable, installed, and
covers exactly the requested cutoff.
[Capacity protocol](../../bench/retained-history/README.md)

The v1 manifest digest binds the exact covered WAL bytes, so full-WAL
compaction and export retain access to that prefix. On a tail-only restore,
retain the verified checkpoint boundary, enforce bounded tail admission, and
reject v1 export or checkpoint advancement that requires missing prefix
bytes. [ADR 0005 format constraints](0005-pinned-writable-recovery-authority.md#keep-the-v1-prefix-digest-meaningful)

## Reference lifetime

Bound the number and lifetime of retained captures. HTTP applies the earlier
of its cursor deadline and the source expiry and certifies the walk through
exhaustion at the fixed work and byte budgets before issuing a cursor. Repeat
pages against the same capture yield the same rows, positions, and exhaustion
decision within the originally metered work. Failed acquisitions clean up in
the source. HTTP releases an acquired capture if it issues no cursor. An
issued cursor remains retryable through terminal pages until expiry or
explicit release. Expiry or idempotent release bars new operations, while
admitted readers keep their own references until they return. Authenticated
requests for an expired capture receive 410.

Hold the generation and WAL references through `backup.Export`, including
cancellation and error cleanup. Export closes the readers it opens. At
shutdown, stop admission and maintenance scheduling, retire captures, and
drain HTTP, appends, compaction, and exports before closing the catalog, WAL,
and pinned directory. Surface cleanup failures. If the drain deadline passes,
leave the owner locked until process exit so active users cannot overlap a
second writer.

## Ownership and release gate

The bog architecture maintainer owns this decision and reviews the command
seam with `bog-storage` and the #59 owner before composition. `bog-storage`
owns #53 recovery, capture, and live compaction. `bog-query` owns #59 and
the HTTP paging contract.

After #61 releases its read lease on `internal/archtest/architecture_test.go`,
acquire the registry write lease for the #53 wiring merge. Derive actual
Linux production roles and direct edges from the tree with #53 and #59
composed. Update the observed diagram and registry together, promoting only
reachable packages and removing retired edges. Run
`go test -count=1 ./internal/archtest` and
`GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./...` before merge.
[System architecture](../architecture/SYSTEM.md#fitness-and-registry-ownership)

#53 remains the release gate. Qualify one binary from the full composed
commit SHA. Require exact fixed-budget walks, a held cursor across live
generation replacement with acknowledged backfill, and fresh walks after
restart. Start that binary on a verified restore, append through HTTP, and
verify the exact augmented history after restart. Require the remaining
admission, durability, readiness, and capacity evidence for the same binary.
[Linux release verifier](../../verify/release/README.md)
