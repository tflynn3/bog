# Live retained-history limits (#63 handoff to #53)

Share a process-local `checkpoint.NewBudget(checkpoint.Limits{...})` between
the WAL owner, `checkpoint.NewPublisher(root, dir, budget)`, and
`history.NewCatalog(budget)`. Configure `StagingBytes`, `ReplayTailBytes`,
`PinnedGenerations`, and `LiveCaptures` explicitly. Limits are inclusive.
`Budget.Usage()` provides a consistent configured, current, and peak sample.
Use `errors.As` with `*checkpoint.ExhaustedError` for the rejected resource,
limit, usage, and request size. Retrying after resources drain can succeed.
Exhaustion leaves the prior authority or captured view intact. If an error
also reports `checkpoint.ErrPublicationUncertain`, settle recovery before retry.

## WAL and runtime wiring

- Reserve logical encoded WAL tail bytes, including the frame headers, before
  reading startup replay or admitting new WAL appends. Use
  `budget.ReserveReplayTail(ctx, start, end)` at checked whole-frame boundaries.
  For startup, `budget.ReplayCapturedTail` can admit the entire suffix and
  return validated batches with individually releasable reservations.
  Charge every retained frame exactly once, or use a whole group when it shares
  backing storage and drains together. Concurrent pending writes and the live
  replay suffix share the same budget. On commit, transfer the reservation to
  the visible tail with the rows. Following confirmed rollback, release it once
  the allocation is discarded. Keep it with recoverable data on an uncertain
  outcome. At startup, block listener activation when the selected committed
  suffix exceeds the limit.
- Under the WAL owner's capture boundary, acquire a history reader and retain
  the matching tail reservations. Use `Reservation.Retain()` for shared
  immutable backing storage and a separate charge for independent copies.
  Dropping the live tail's reference after compaction releases bytes only when
  the last capture or operation drops its reference. The byte limit measures
  encoded framed bytes, so budget decoded storage and bounded frame scratch
  space separately. Take a `LiveCaptures` reservation directly for a view with
  no catalog generation, and carry it through operation drain.
- `Catalog.AcquireCurrentUntil(ctx, deadline)` binds the deadline to the
  reader. The command still owns cursor routing and should retire idle expired
  captures with `Reader.Close()` and release their WAL references. Admitted
  scans keep their history references; the command must similarly keep the
  WAL references for admitted pages and exports. Use HTTP's earlier deadline
  when it applies. A capture acquired before a generation swap still reads its
  original generation until its own retirement or deadline.

## Publication and recovery

1. Take `Catalog.ReserveReplacement(ctx)` before publication. It charges one
   candidate slot alongside the current generation and any retired generations
   with live references. Reserve at least two slots to support replacement.
   Release an unused ticket with `Replacement.Close()` on definitive failure.
2. Call `Publisher.PublishCaptured` with the same arguments as the existing
   function, except directory handles are bound to the publisher. Staging
   charges the exact encoded bytes requested for new segments, the candidate
   manifest, and the intent before writing each file. One inode with both a
   staging and final name is charged once. Directory metadata and filesystem
   allocation rounding belong to the active-filesystem headroom policy.
3. Once publication is durable, call `Replacement.ReplacePinned(ctx, dir)`
   to validate and activate the new manifest from the same directory handle.
   Use the reserved ticket under the owner's activation boundary, with an
   owner-controlled context that can finish activation even if the initiating
   request was cancelled. Trim the live WAL suffix only after the catalog swap.
   A successful installation can return a cleanup error with a populated
   generation. If validation fails, fence the owner until it can install the
   authoritative generation. A failed validation leaves the ticket reusable.
4. On `ErrPublicationUncertain`, keep the ticket and fence dependent
   mutations. Use the same publisher's `RecoverCaptured` under the owner lock
   to settle staging and its charges. The validator can call
   `history.ValidatePinnedAuthority(ctx, dir)` under the publication lock
   and must independently validate the WAL boundary. At startup perform this
   recovery before new admissions. If the candidate won, install it with the
   reserved ticket. If the old authority won, close the unused ticket and
   retry the publication as needed. Reclaim via `Catalog.ReclaimPinned` under
   the same pinned directory once references drain.
