# ADR 0005 — Durable authority for writable restores

**Status** — Cycle 42 design decision for #53. The pinned base uses the
restored-image startup guard. Writable activation requires the cutover
wiring and Linux qualification.

## Decision

Treat a verified v1 backup image as input to activation. Its
`bog.backup` metadata describes a fixed recovery point. It cannot
describe appends made after the restore. The writable service uses one
durable authority under the same exclusive data directory lock as commit,
recovery, and generation activation.

The active on-disk state must identify the validated checkpoint generation
and its covered frame boundary and next append ordinal. It must also bind
that checkpoint to the writable WAL identity or epoch, its physical offset
mapping, and an authoritative committed frame end. Store the advancing end
in the active commit marker or equivalent durable state bound to that WAL
identity. Recovery must be able to resolve these fields together without
guessing from file names, modification times, or file lengths. #53 owns the
concrete encoding and records it with the implementation.

In a v1 restore, byte zero of `bog.wal.tail` corresponds to
`CoveredOffset` in the original WAL, with the first row at `NextOrdinal`.
Validate the exact size and digest of the captured suffix and its complete
frames, and verify the manifest, segment integrity, and coverage of
`[0, NextOrdinal)`. The original committed end is a logical WAL offset.
Recovery of the writable layout must use the active mapping between logical
offsets and physical file positions, including when the captured tail is
empty or the active file has its own header. Continue assigning global
ordinals after the last verified row. The omitted prefix is represented by
the validated checkpoint and remains unnecessary for replay. The prefix
digest in the manifest identifies the source bytes checked at export. The
active authority binds the retained view to a specific writable WAL epoch.
See the [v1 backup format](../../internal/backup/FORMAT.md).

## Activation and restart

1. Acquire the exclusive lock on the pinned data directory. Classify and
   validate the layout before legacy bootstrap, tail truncation, staging
   cleanup, or listening. Either `bog.backup` or `bog.wal.tail` selects
   restored-image handling in the absence of a valid active authority.
   Check ownership, permissions, file type and identity using the same
   trusted-directory assumptions as legacy recovery.
2. Validate the complete restored image and prepare the writable tail and
   its initial commit point. Keep the verified image recoverable while
   preparing the new state. If recovery is to resume a partial activation,
   make its intent durable before installing names that would otherwise
   make the image ambiguous. Bind the intent to the image and the files
   owned by that attempt.
3. Sync the prepared files and their installed names before publishing
   the authority that selects them. Sync the authority file, atomically
   install it, and sync the directory. Publish the recovered query view and
   open the listener only after durable activation. Capture, readiness,
   and ingest then refer to the same owner and view.
4. Append and sync whole frames, publish the durable commit end, then make
   the batch query-visible before acknowledging it. On restart, recover
   the active checkpoint and every complete frame through the active
   committed end, including batches acknowledged after activation.
   Validate the committed range before discarding any uncommitted suffix.
   A generation replacement must preserve a recoverable authority for
   all acknowledged data across the swap.

| Startup evidence under the directory lock | Recovery action |
| --- | --- |
| Valid legacy layout | Use legacy recovery and its existing bootstrap rules. |
| Complete verified image with no activation state | Prepare and publish writable authority from that image. |
| Valid unfinished activation with no published authority | Validate the image and the attempt's file identities. Resume or discard only files proven to belong to the attempt, leaving the image available. |
| Valid active authority, including after an interrupted cleanup | Recover the active view and commit end. Leftover verified-image metadata is subordinate to the active state. Resume cleanup only for identified, unreferenced files. |
| Missing image components, unsafe entries, malformed or inconsistent authority, or legacy files mixed with an unactivated image without a validated intent | Fail before opening the listener. Preserve the directory for diagnosis. |

If authority publication returns an error after rename or during directory
sync, stop startup or further writes and preserve both recoverable states.
On restart re-open and validate the authority under the lock before resuming.
Only a valid, durably established state may serve. A present but invalid
authority or committed corruption fails closed. Falling back to the
original image in that case could lose acknowledged batches. Keep old
files through uncertain publication and until neither the durable authority
nor any query or export pin refers to them. The service directory must
recover independently of the source bundle after activation.

## Qualification

Exercise interruption before authority publication and after rename but
before directory sync, plus restart after activation and with interrupted
cleanup. Check empty and nonempty captured tails, corrupt or missing files,
mixed layouts, and committed corruption after a new acknowledged batch.
Compare exact HTTP walks with fresh cursors before and after writing and
restarting the restored directory. Include a generation swap with a live
capture, and run the restored-parity and durability probes on the same
pinned Linux binary. See [ADR 0004](0004-retained-history-cutover.md).
