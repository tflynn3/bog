# ADR 0005 — Pinned writable recovery authority

**Status** — Cycle 46 integration decision. The pinned base still rejects
restored images at server startup. Runtime implementation and qualification
belong to the composed [#53] and [#59] candidate.

This decision extends the [retained-history cutover](0004-retained-history-cutover.md).
One Linux process, one listener, one WAL owner, and one pinned local data
directory remain the deployment and consistency unit. The same owner decides
which bytes support recovery, which complete batches are visible, and which
generation a new reader sees. Readiness observes that decision. HTTP and
offline tools do not select the live authority.

## Recover and activate under the directory lock

Acquire the exclusive WAL lock and pin the directory before classification.
Use that directory for classification, validation, activation, replay, and
subsequent publication. Bind any handles supplied by path-taking storage APIs
to the same pinned directory identity. A substituted path or file identity
fails validation.

| Observed layout | Startup decision |
| --- | --- |
| Legacy WAL and commit marker, or a directory eligible for legacy bootstrap | Use legacy recovery and its existing bootstrap checks. |
| `bog.backup` or `bog.wal.tail` with no writable authority | Require an entire unambiguous v1 restore. Validate the metadata, manifest and every referenced segment, exact checkpoint ordinal coverage, tail size and digest, and every committed tail frame. Reject a mix with legacy authority files. |
| Writable restore or interrupted activation | Resolve only through state owned by the activation protocol. Check its version, integrity, and references, then validate the selected checkpoint and WAL. Reject a damaged or ambiguous authority before listening. |

The restore image provides the initial verified point. Publish a durable
writable authority that binds the checkpoint generation and its covered
logical WAL offset and first uncovered ordinal to an owned writable WAL
identity or epoch and its committed end. Keep the logical offsets explicit
when the suffix starts at physical byte zero. Replay complete committed frames
starting at the first uncovered ordinal to find the first free global append
ordinal. A restart on the activated directory uses the current writable
authority and committed end. The original bundle metadata describes the
initial restore point and its fixed tail length. It cannot act as a moving
commit marker.

Stage and sync the required data before installing authority. Sync the
directory after installing names and after atomically publishing the authority.
Keep the verified input recoverable through activation. Enable writes and
open the listener only after the selected authority is durable and the
recovered query view is installed. A subsequent append follows the WAL
group-commit contract. Return HTTP 204 only after its complete batch is
durable and query-visible. Recover it on restart from the service directory
alone, without opening the source bundle. Validate the committed suffix
before removing bytes beyond its end.

## Capture and activation handoff

The base `(*wal.Log).Capture` pins a WAL inode and its published
`CommittedEnd` and `NextOrdinal`. Here `NextOrdinal` is the first ordinal
after that end. History's `Catalog.AcquireCurrent` pins the active generation.
The manifest's `NextOrdinal` is the first ordinal after the checkpoint's
covered prefix. bog-storage must coordinate both acquisitions with append
publication and generation activation so the pinned generation, covered
boundary, fixed tail, and published committed end describe one recoverable
view. The v1 digest identifies bytes. Bind the pinned file identity or epoch
independently. Release acquired references if capture fails or is cancelled.

`cmd/bog` injects the adapter using `httpapi.NewWithPageSource`. [#59]'s
`PageSource` keeps the `Page(ctx, request, work)` and `Release(capture)`
contract from ADR 0004 and adds an optional source `ExpiresAt` on the first
result. Use the earlier of the source deadline and the HTTP lifetime for the
cursor. Repeat a page at the same limit and position with the same rows,
positions, and exhaustion decision within its original work cost, including
after generation replacement. HTTP certifies a walk at the fixed work
budget before issuing its first cursor, including for smaller resume limits.
Insufficient budget fails before response commitment. Every emitted row has
its own exclusive position, so trimming by encoded bytes preserves the
correct resume point. An expired
capture returns 410. HTTP releases an unpublished capture and retires a
published capture after expiry and admitted requests drain. A still-live
cursor remains retryable through the terminal page.

For export, keep the generation and WAL pins through the whole `backup.Export`
call. The captured point fixes the frame range and excludes later appends.
Both backup readers and paging operations retain their own operation references
until they finish. Retired segments and WAL views remain readable until all
their references drain.

## Publication, retry, and shutdown

On legacy-backed history, `checkpoint.PublishCaptured` takes a pinned
absolute-offset reader, a whole-frame end, the prior manifest, the next
ordinal, and the exact v1 prefix digest. The WAL owner serializes activation.
Sync new segments before making the manifest authoritative, validate the
durable successor, then install the catalog generation and trim the live
tail view at the covered boundary. Rows appended after the cutoff stay in
the tail. Report compaction complete only after the successor is durable
and installed.

Before authority publication, a failed attempt leaves the old authority
available. After a rename or directory-sync outcome becomes uncertain,
preserve both recoverable views and fence mutation that depends on choosing
one. Retry by inspecting the pinned namespace under the owner and
publication locks. `checkpoint.RecoverCaptured` takes an authority
validator under the publication lock. The validator checks the manifest,
referenced segments, and matching WAL boundary. Only absence of the
manifest itself can represent a first publication. Sync the selected
authority before retrying or deleting any losing output. If the requested
generation already won, validate and install it without republishing the
same cutoff. A catalog replacement can report a cleanup error after the
swap. Inspect the installed generation before deciding whether to retry.
If recovery cannot resolve authority, fail closed before serving again.
Only reclaim protocol-owned objects whose identity is confirmed and that
are unreachable from both the durable authority and all live pins.

Shutdown closes admission for new HTTP and storage work, stops readiness and
compaction scheduling, and retires outstanding captures. Drain in-flight
HTTP, append, compaction, and export references before closing the catalog,
WAL, and pinned directory. Keep the owner lock until storage users are done.
Cancel outstanding work at the shutdown deadline and surface drain,
publication, sync, and close errors. When an unresolved authority or timed-out
drain requires process termination, leave durable state for restart recovery
and let process exit release the locks.

## Keep the v1 prefix digest meaningful

The v1 manifest's `SourceSHA256` is SHA-256 over the **exact WAL bytes**
from the magic through the covered boundary. The v1 exporter reads and
checks that prefix. The backup stores the digest but omits those bytes.
Verification of the restored checkpoint and suffix is sufficient to
activate writable recovery at the verified point. A final SHA-256 digest
does not provide a continuation state for extending the prefix to a new
boundary. Re-encoding rows, filling missing bytes, or hashing only the
suffix changes the meaning of the field.

For v1 online publication on a full WAL, keep the covered prefix readable
and validate it when creating or exporting a checkpoint. This preserves
format compatibility at the cost of disk space and prefix hashing I/O.
On a tail-only restore, keep the verified checkpoint boundary while the
owner appends and recovers the bounded writable suffix. Refuse a v1 export
that requires unreadable prefix bytes or checkpoint advancement that would
need an unverifiable digest. Enforce the tail admission bound even on this
path. Advancing and exporting beyond that point without the original prefix
requires a separately specified recovery format or a verifiable digest
continuation representation.

## Integration evidence

- Start the shipped Linux binary in a verified isolated restore. Walk the
  restored rows, append a batch and observe 204, then restart on the same
  directory and walk the exact augmented history. Compare order, ties,
  duplicates, and terminal pages against the append-log oracle.
- Hold a cursor across a confirmed live generation replacement. Observe a
  continuation and acknowledged backfill while compaction reports
  `running` with the previous generation. Confirm a newer durable generation
  covers the requested cutoff, then finish the held walk against its original view.
  Compare a fresh walk to the view with the acknowledged writes and repeat
  after restart.
- Exercise publication interruption, failed validation, and shutdown with
  outstanding pins. Run the uncached Linux architecture inventory and Linux
  cross-build on the composed tree. Use the same revision-pinned Linux
  binary for the release evidence in [SYSTEM.md](../architecture/SYSTEM.md).

[#53]: https://github.com/tflynn3/bog/issues/53
[#59]: https://github.com/tflynn3/bog/issues/59
