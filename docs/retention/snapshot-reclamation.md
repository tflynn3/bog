# Retained-history snapshots and durable reclamation

Status: proposed integration contract for #4, #6, #19 and #26. The existing
checkpoint scanner and block codec do not yet implement publication, leases,
retention, or export. This document sets their required behavior; it does not
claim those features are live.

## Identity and visibility

A manifest generation is a unique, monotonically increasing publication ID,
not a WAL generation. A versioned, checksummed manifest names the immutable
segment identities and the WAL generation, exact byte offset immediately after
a committed frame, and next append ordinal covered by that segment set. It also
names a monotonic retention floor and the active WAL generation and its
generation-bound commit marker. The covered offset
is relative to that WAL generation, including its header. The empty boundary
is after the WAL header. The next ordinal is the exclusive upper bound on rows
in the covered prefix; zero-based ordinals advance once per row, including
identical rows and rows within the same batch. A batch is wholly visible or
wholly absent. A unique WAL generation must be durably established for legacy
WALs before a manifest can cover them; never infer identity from a reused path.

A snapshot captures, atomically with respect to commits, retention and
publication, the manifest generation and segment identities, WAL generation
and exact committed frame boundary, and durable next append ordinal at that
boundary. The manifest's covered boundary can be earlier than the snapshot's
boundary: the snapshot also pins the committed suffix between them. Capture
only acknowledged durable commits, never a partial frame or uncommitted tail.
Its ordinal bound is exclusive, including when the snapshot is empty. Capture
also the effective retention cutoff, query bounds and literal filter. The
snapshot is an immutable logical view even if its files are later compacted.

Rows are ordered by (timestamp, append ordinal), ascending. The ordinal is a
durable row identity and tie breaker, not a segment-local position. Compaction,
legacy conversion and export preserve it, including for duplicate lines and
identical timestamps. Matching is exact literal containment and time ranges are
half-open [start, end). Use the checkpoint block's seconds and nanoseconds
encoding across the supported timestamp range, not a narrower Unix nanosecond
integer. See [checkpoint format](../../internal/checkpoint/FORMAT.md) and
[block format](../../internal/checkpoint/BLOCK_FORMAT.md).

## Cutoff and backfill

The configured retention policy computes a cutoff for each new snapshot and
clamps its requested start to the effective cutoff. A row at the cutoff is eligible;
one before it is ineligible for a new snapshot. The effective cutoff is the
later of the policy cutoff and the durable retention floor. Before discarding
any rows, publish and durably sync a nondecreasing floor at least as high as
the discard cutoff. Clock rollback or a policy extension cannot lower that floor
or make visibility depend on whether compaction ran. An end at or before the
effective start yields an empty result. Policy changes and wall-clock advancement
affect new snapshots only. A pinned snapshot keeps its captured cutoff and rows
even when the current cutoff advances; retaining those rows is a lease obligation,
not a promise of indefinite history to new clients.

Late backfill is committed with fresh ordinals in normal batch order. It is
eligible for a new snapshot only if its timestamp meets that snapshot's cutoff
and range. A backfill committed after capture is absent from every page of the
old snapshot, even if its timestamp sorts before the cursor. Backfill older
than the current cutoff may be accepted durably but is not exposed to new
snapshots; reclamation may discard it once no snapshot can need it. The
implementation must not resurrect expired rows in a later snapshot merely
because an old segment or export still contains them.

## Cursor and reference lifetime

A first `page=1` request acquires a bounded snapshot lease before returning a
continuation. An authenticated, versioned cursor binds the lease ID, expiry,
snapshot identity and ordinal bound, captured cutoff, normalized time bounds,
filter, and last emitted (timestamp, ordinal). It is exclusive of that position.
The server validates authentication and all bindings before using the lease;
changing page size is allowed. A continuation must read the same logical view,
including the pinned WAL suffix, regardless of intervening publication. A
cursor is not itself proof that its files still exist: the lease registry owns
references and enforces a configured maximum lifetime and resource bound.

At expiry reject continuation with HTTP 410 Gone and a stable
`cursor_expired` error, no NDJSON body and no next cursor. The client starts a
fresh `page=1` walk; it must discard or reconcile the partial old walk because
the new cutoff and committed rows can differ. Invalid, tampered, mismatched or
unsupported cursors are HTTP 400, not expiry. This is a proposed API distinction
for integration, not a description of the current process-local cursor key.
Never silently resume an expired cursor against a new snapshot. If admission or
work limits reject a page, return the appropriate #19 error without advancing or
releasing an otherwise valid lease; retry uses the same cursor.

A terminal page releases the cursor lease after its response is determined;
a failed or cancelled request releases its transient reader reference, but
keeps an unexpired continuation lease available for retry. An explicit close,
expiry, or server shutdown releases the lease. A reader holds independent file
references until it finishes or is cancelled, including during response writes;
expiry cannot invalidate a reader already using those files. Ordinary queries
hold references for their request lifetime. Lease acquisition, expiry and file
reference counts must be serialized with reclamation, preventing a reference
from being acquired after its input is declared reclaimable. Enforce limits on
lease count and age so abandoned cursors cannot pin storage without bound.

After restart, in-memory cursor leases do not survive: an otherwise valid token
for a lost lease returns 410. Token authentication must allow that distinction
across restart (for example, a durable server key); a malformed token is 400.
No old cursor is transferable to an export or a different restored instance.

An export (#6) captures the same tuple and cutoff at one durable boundary,
then holds independent references through copying, verification and durable
completion or failure/cancellation. It includes every file and committed suffix
needed to reconstruct that view, with ordinal identity and a self-consistent
manifest and boundary. Export must not rely on a lease surviving the export
operation; completion releases source references only after the exported copy is
durable. Its restore validates identities, checksums and boundaries before
serving. A fresh walk on the restored instance uses the exported cutoff and
boundary as its initial retained view; later local commits use higher ordinals.

## Publication and reclamation

Under the pinned directory lock, #26 syncs staged immutable segment files,
installs their names and syncs their directories; then it syncs the staged
manifest, atomically publishes it and syncs the manifest directory. Only after
that durable point may readers switch or recovery skip the covered WAL prefix.
The manifest and segments use explicit versions, little-endian fields,
checksummed metadata and blocks, sparse time bounds, and checked lengths and
offsets before allocation or reads. Validate all segment metadata and every
referenced block before exposing any segment rows. The exact file formats and
limits must agree with the checkpoint contracts; archive `BOGSEG01` is distinct
from queryable `BOGQSG01`.

Keep old inputs through durable publication and until every cursor, reader and
export reference is gone. Before deleting an old segment, under the same
serialization as pin acquisition, establish that the durable authoritative
manifest and every still-live snapshot/export do not reference it. Verify its
identity, not just its filename; do not follow symlinks. A failed unlink or
directory sync is an incomplete cleanup, not evidence of successful durable
reclamation. Sync affected directories after deletion. Compaction output must
preserve all rows needed by any retained snapshot; it cannot substitute a
filtered new view for a pinned old view.

Covered WAL bytes can be reclaimed only after the durable manifest covers their
exact complete committed frame boundary, the named segment set is verified,
and no reader, cursor, export, recovery or other live reference needs those
bytes. Preserve the identity and offsets of any pinned generation. In
particular, do not truncate or rewrite a live WAL prefix in place: use a
versioned generation handoff. Fence commits before capturing the final suffix;
keep them fenced through publication and the switch to the new WAL and marker.
Copy every complete uncovered committed frame with its original row ordinals,
including commits up to the fence, and record the exact boundary mapping. Sync
the new WAL and its generation-bound commit marker, install their names and sync
their directories. Publish a manifest selecting that WAL and marker together
with the segment coverage and mapping, using the publication order above. Only
after its directory sync succeeds may writers resume on the new pair. No commit
may be acknowledged on the old pair after the final capture. Retain the old
pair until publication is durable and all references are gone. On any uncertain
step keep the fence and stop; until this protocol is implemented, retain the
whole old WAL. Logical coverage alone does not authorize byte deletion.

If publication, rename or directory sync has an uncertain result, stop writes,
reader switching and reclamation; do not guess which manifest won. On reopen,
under the directory lock, identify and validate the visible authoritative
manifest and referenced files. Sync the manifest directory successfully before
serving, switching writers, or reclaiming anything: a rename visible after a
process restart may not yet be durable across a subsequent power loss. If that
sync fails or the outcome remains uncertain, stop and retain both old and new
inputs. After durability is established, replay only the selected generation’s
uncovered committed WAL suffix through its generation-bound durable commit
marker, preserving each row once. Missing references, bad checksums,
invalid committed frame boundaries or committed corruption fail closed, not
fall back to an older view or silently skip rows. Uncommitted tail handling
remains the legacy WAL contract. Reopen after interrupted deletion repeats
verification and reference/orphan checks before retrying cleanup; an absent
unreferenced file is harmless, but an absent referenced file is fatal. Staged
or abandoned files are deleted only after proving they are unreferenced by the
authoritative state and all live pins. Cleanup and reopen are idempotent.

## Work accounting and examples

#19's per-request work budget applies to ordered and merged scans, including
metadata and Bloom checks, candidate rows and bytes, and cancellation. Charge
physical block reads when issued, including verification and cache misses;
charge decoded bytes whenever decoded, including cache hits that require decode.
Charge lookahead used to determine a continuation, even if it emits no row or
finds no further match. An empty result is not free: all blocks read, bytes
decoded and candidates examined to establish emptiness count. A cached block
is not another physical read, but examining or decoding it still incurs the
corresponding work. Shared verification work must be bounded and attributed by
the #19 integration rather than bypassing admission. Exhaustion or cancellation
must not return a misleading terminal page or advance a cursor past unseen rows.
A byte-limited page includes a cursor only when lookahead proves more matches;
a first row too large for the response remains HTTP 422 with no cursor.

For example, capture boundary B and ordinal bound 4 with rows (T,0,"x"),
(T,1,"x"), (T,2,"y"), (U,3,"z"), where T < U. A page of two emits both
identical x rows and saves (T,1). Compaction publishes a new manifest while a
later backfill (T,4,"x") commits and the cutoff advances past T. The old lease
still pins its original view: its next pages return (T,2) and (U,3) exactly
once, excluding ordinal 4. A new walk uses the new cutoff and boundary; it
excludes T rows if T is now before cutoff. No timestamp-only continuation can
correctly distinguish the two x rows.

An export captured at B retains ordinals 0 through 3 and its captured cutoff.
After restore, a fresh walk with the same bounds and page size returns the two
x rows, y, then z in that order, independent of the source's later compaction,
cutoff and backfill. It starts a new local lease and never reuses the source
cursor. With a later exported boundary that includes ordinal 4 and a cutoff
that admits T, a fresh walk orders the three x/y ties by ordinal, with all
identical rows present. Export must carry sufficient cutoff and ordinal state
to make both cases reproducible.

## Qualification gates

Compare each recovered and restored paginated walk with the existing WAL-path
oracle, including duplicate rows, ties, shuffled input, late backfill, empty
results, time extremes and byte-limited lookahead. Inject crashes and I/O
failures at each file sync, install, directory sync, manifest rename, reference
release, unlink, WAL handoff and reopen; test uncertain publication and missing
or corrupt committed references fail closed. Exercise lease expiry, restart,
concurrent compaction/export and idempotent cleanup.

On Linux, measure restart-to-health, peak RSS, durable ingest throughput and
p99, query p95 and write amplification, stating workload, data size, cache and
hardware. Bound peak recovery memory as capped decode buffers plus bounded
cache, paged metadata, merge fan-in and WAL-tail working state; include legacy
conversion, shuffled input, metadata growth and the largest accepted frame.
Demonstrate that the bound holds for retained history larger than RAM before
claiming qualification. These are integration gates, not results of this
design document.
