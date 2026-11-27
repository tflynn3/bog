# ADR 0035 — Tail-only retained recovery contract

**Status** — Proposed for Cycle 145. Core Engineering Architecture owns this
contract. Storage owns checkpoint staging recovery (#83) and repeatable
tail-only checkpoint and backup recovery (#81). #53 owns runtime composition,
SYSTEM.md, and the architecture fitness registry. Release Verification
qualifies the same clean composed #53 revision on native Linux amd64 and arm64.
[Command-owned composition](0026-command-owned-retained-storage-composition.md),
[Capacity control qualification](0034-capacity-control-release-qualification.md)

## Decision

Checkpoint advancement and backup export consume the same kind of pinned
recovery view. The command supplies the active validated generation and its
matching committed suffix under the WAL owner's synchronization. The owner
holds the exclusive lock on the pinned data directory through recovery,
serving, and drain. Capture linearizes against durable append publication
and generation activation. It sees a group only after it is both durable and
query-visible. The command binds the opened WAL identity or epoch to the
generation and bounds when it takes the pin. Checkpoint and backup receive
read access through their consumer seams. Writable authority and activation
stay with the command and WAL owner. [WAL capture](0003-wal-capture-and-restored-image-startup.md),
[Pinned writable authority](0005-pinned-writable-recovery-authority.md)

## Recovery view

The Go shape below defines the values and read capabilities. The command
adapts them to the consuming packages. A common implementation package is
unnecessary. Checkpoint can use its existing validated manifest and segment
pin internally, and backup can reuse its manifest and segment opens.

```go
type RecoveryPoint struct {
    Generation       [32]byte
    CoveredOffset    int64
    NextOrdinal      uint64
    CommittedEnd     int64
    CommittedOrdinal uint64
    SuffixSHA256     [32]byte
}

type RecoveryView interface {
    Point() RecoveryPoint
    OpenManifest() (io.ReadCloser, error)
    OpenSegment(name string) (io.ReadCloser, error)
    OpenSuffix() (io.ReadCloser, error)
}
```

- `Generation` identifies the exact manifest payload by SHA-256. Manifest
  and segment readers provide the original encoded bytes of that generation.
  Segment names must occur in the validated manifest. The generation digest
  and WAL identity serve independent purposes.
- Offsets are signed 64-bit logical byte positions in the original WAL
  coordinate system, including its eight-byte magic. Require
  `8 <= CoveredOffset <= CommittedEnd`. The suffix contains exact whole
  frames in `[CoveredOffset, CommittedEnd)`. Byte zero of each suffix
  reader corresponds to `CoveredOffset`. The adapter maps a backing file
  offset by subtracting that file's logical base from the logical position.
  The base is zero for a full WAL and may be positive for a tail file. A
  consumer can scan or copy the bounded reader from zero and can reopen it
  for another pass. Later appends, the magic, and discarded prefix bytes
  are outside the reader's range. A short read before the expected length
  is a validation error.
- `SuffixSHA256` binds the exact bytes in the captured range. Use the
  trusted digest for those bounds when present in authority or image
  metadata. For a v1 source without such a digest, derive it while
  validating the fixed, pinned range. Validation can run after capture
  releases the owner mutex. Consumers check length, digest, frame checksums
  and decoding before publishing their outputs. Stream replay within the
  configured suffix and frame limits.
- Ordinals are global zero-based append positions. The checkpoint covers
  exactly `[0, NextOrdinal)`. Suffix replay starts at `NextOrdinal` and
  must finish at `CommittedOrdinal`, the first free ordinal at
  `CommittedEnd`. Count all decoded rows, including duplicates and
  backfill. Reject overflow, ordinal gaps or overlap, and byte bounds that
  split a frame. An empty suffix requires equal byte bounds and equal
  ordinal bounds. Query order remains UTC timestamp then append ordinal.
- Checkpoint may choose a whole-frame cutoff within the captured suffix.
  Derive the cutoff ordinal by replay and match it to the requested
  ordinal before publishing. Advancing to that cutoff preserves the global
  byte and ordinal coordinates. Backup exports the entire captured suffix
  and uses the captured generation even if the live generation advances
  before export completes.
- The caller owns the operation pin until the consumer returns, including
  error and cancellation cleanup. Each successful open transfers an
  independent reader to the consumer, which closes it and reports close
  errors. A failed open cleans up its partial acquisition. Opens on a view
  shared by concurrent operations must be safe and return independent read
  positions. A reader close leaves the caller's pin intact. Expiry or
  idempotent release stops new operations, and admitted operations retain
  their references until they finish. Reclamation, including tail
  retirement, waits for durable authority to move and the last relevant
  reference to drain. Shutdown follows the same ownership order.
  [Generation pins](0002-bounded-history-generation-pins.md),
  [Runtime composition](0007-runtime-storage-composition.md)

## Versioned integrity and writable recovery

Keep reading valid v1 checkpoints and verified v1 bundles with their exact
existing meanings. The v1 manifest's `SourceSHA256` covers the original
WAL bytes from byte zero through the covered boundary. A full-WAL v1 writer
or exporter still checks those bytes and their frame and ordinal bounds.
Opening a verified v1 restore validates the manifest envelope, every segment,
exact ordinal coverage, the image metadata, and its exact tail length,
digest, and frame sequence. Its end ordinal comes from replay. A v1
restore can be activated at that point with bounded writes. A v1 write
path that requires missing prefix bytes must refuse the operation. A final
SHA-256 digest cannot extend that prefix, and re-encoding rows cannot
recreate its original byte representation.
[v1 checkpoint format](../../internal/checkpoint/FORMAT.md),
[v1 backup format](../../internal/backup/FORMAT.md)

Use explicit v2 records for prefix-free advancement. The contract specifies
the fields and what they bind. Storage defines the byte encodings with #81.

| Record | Required integrity bindings |
| --- | --- |
| Checkpoint manifest | Version, covered logical offset, next ordinal, and the names, exact sizes, and SHA-256 digests of the immutable segments. The SHA-256 of the exact manifest payload identifies the generation and is checked by the envelope. For an advance, record the predecessor generation and its covered offset and next ordinal, with SHA-256 of the exact consumed frames from the predecessor boundary to the successor boundary. |
| Writable authority | Version and integrity checksum over the complete record, generation digest and checkpoint boundary tuple, owned tail file reference and identity or epoch, logical base of the tail file, committed logical end and first free ordinal, and SHA-256 of the exact committed suffix beginning at the checkpoint boundary. Validate the file identity in the pinned directory separately from its content digest. |
| Backup metadata | Version and integrity checksum over the complete payload, generation digest, covered offset and first uncovered ordinal, committed end and first free ordinal, and SHA-256 of the exact bundled suffix. Tail length is the difference of the two offsets. The bundle maps its tail to the covered offset and carries no source inode or writable credentials. |

Validate a v2 advance against the pinned predecessor and the exact frames
consumed before publishing it. The consumed-range digest records the input
to that transition. Once the bytes are reclaimed, a standalone reader
checks the selected manifest, segment bytes and exact ordinal coverage,
and committed suffix against the selected authority or bundle metadata.
A prior manifest or prior tail is unnecessary to verify, restore, or
export that generation. Digests detect corruption in the trusted storage
model. They provide no authentication for images supplied by an untrusted
writer. Check supported versions and required fields explicitly, including
the generation and boundary match between records. Reject unknown versions,
mixed authoritative layouts, and inconsistent fields. A v2 bundle may
carry a validated v1 manifest when the source is still at its verified v1
checkpoint boundary. Its metadata still uses v2 suffix semantics. Use
SHA-256 of the empty string for an empty suffix.

Under the owner lock, validate and sync the staged successor generation
and any successor tail before switching authority. Preserve commits after
the advancement cutoff. Serialize the final selection with append
publication so the successor includes all commits through the selected
durable end. If the tail is rebased, copy the exact remaining committed
frames, validate the new logical-to-physical mapping, and sync the new
file and its name before selection. The writable recovery record is the
single durable selector for its manifest and tail together. Install it
atomically and sync its directory before activating the successor or
exposing reclaimed capacity. A rename or sync with an uncertain outcome
fences dependent mutation and new captures. Recover under the owner and
publication locks by validating and syncing the selected authority before
cleaning up an alternative. Keep the old authority's files until that
decision is durable and their pins drain. If recovery finds an already
selected successor at the requested cutoff, validate and install it
without another advance. `bog.backup` records the immutable restore point.
After writable activation, restart uses the writable authority and checks
every committed frame before truncating any uncommitted trailing bytes.
[Retained resource admission](0010-retained-resource-admission.md)

## #83 staged checkpoint replacement recovery

A staged block rewrite is part of one unpublished checkpoint attempt.
Keep ADR 0016's stable slot and single rewrite per slot. Reserve the full
encoded successor size before allocation or write and retain the original
charge during the overlap. With the publication lock held, write the
successor under a fresh exclusive name in the private stage. Validate its
identity, exact size, digest, and block invariants. Sync and close it and
sync its staging directory entry before unlinking the old staged file.
Sync staging again to make that unlink durable before releasing the old
reservation. Finish the slot transition before the packer reuses the slot
map or writes the publication intent. Published originals remain immutable.
[Bounded staging reuse](0016-bounded-checkpoint-staging-reuse.md)

| Interruption | Recovery |
| --- | --- |
| Before successor sync and close complete | Stop the attempt. The stage can contain the old file and an incomplete successor. Keep both charges until durable cleanup. |
| After successor sync, before old-file retirement | The private stage can contain both complete files. Discard the unpublished attempt and reclaim both via bounded staging cleanup. |
| During old-file unlink or its directory sync | Either one or both names can survive. Keep the pending charges. Recover against the actual pinned stage and durably remove its owned entries. |
| After retirement sync, before or after old-charge release | The old name is durably absent. Release its charge idempotently and settle any remaining charge only once. An interrupted attempt still discards the unpublished successor. |

Before durable intent, use the existing bounded private-stage recovery to
abort and retry the attempt from the validated current generation. The
enumeration bound must allow one overlapping block at the segment ceiling.
Validate ownership, entry types, link counts, and names under the pinned
publication lock. Fail closed on unsafe staging, and never infer ownership
of a final name from a staging prefix. In-process retries retain unresolved
charges. After process death, recover the bounded stage before admitting
work against a fresh budget. Check write, close, unlink, and sync results.
An incomplete cleanup leaves the attempt pending and bars reuse. Retry
cleanup idempotently and release the remaining charges once removal is
durable.

The existing durable intent remains the boundary for installing final
names. It records only the live staged outputs and is synced before those
names can appear. The staging rewrite adds no publication record of its
own. After intent, recover by validating and syncing the selected
authority and removing only identity-checked losing outputs unreachable
from that authority and live pins. Retain the intent and pending charges
across uncertain publication or incomplete cleanup. The authoritative
rename and directory sync still determine which generation won. A cleanup
error after durable selection can accompany a valid successor.
[Checkpoint publication recovery](../../internal/checkpoint/publication_recovery.go)

## Acceptance

- **#83.** Inject failure and interrupt the process before and after
  successor file sync and close, stage sync, old unlink, retirement sync,
  and reservation release. Repeat recovery with the stage at its segment
  limit. Assert the previous authority and pinned reads are intact, no
  final name was installed before intent, overlap bytes were charged,
  and retry restores capacity exactly once. Also exercise a durable
  intent followed by interruption during final installation and around
  authoritative rename and directory sync. Verify both possible
  authority outcomes and preserve any unrelated file or pinned output.
- **#81, cycle one.** Restore a verified v1 bundle with a nonempty
  checkpoint and suffix into an isolated directory without the covered
  WAL prefix. Start writable recovery, hold a capture, append and
  acknowledge complete batches that include ties, duplicates, and
  backfill, then advance to an exact whole-frame cutoff with committed
  rows remaining above it. Confirm durable successor selection, exact held
  and fresh walks, and tail rebasing. Show that the old tail remains
  charged while pinned and that reclamation after drain restores append
  headroom under the same configured limit.
  Export from the advanced view, verify the v2 bundle, and restore it
  into a fresh directory. Restart the live directory and start writable
  recovery on the fresh restore after taking the source bundle out of reach.
  Both must yield the same terminal walk as an independent append-log
  oracle at the exported point.
- **#81, cycle two.** Use that fresh v2 restore as the writable input.
  Append and acknowledge another batch, advance at a second exact
  cutoff, then export, verify, restore into another fresh directory, and
  restart independently. Use different logical bases and nonempty
  carried suffixes across the two advances. Compare the observed
  generation digests, covered and committed byte and ordinal bounds,
  and exact tail sizes and digests with the fixture. Check every
  terminal walk against the oracle, including the rows remaining after
  each cutoff. Check that the next append after restart uses the
  recovered first free ordinal. Keep the earlier captures and bundles
  stable. Replay through a crash immediately after an acknowledged
  append and exercise interruption around successor selection.
- **Failure cases.** Exercise missing or substituted files, wrong
  generations or tail epochs, unsupported versions, bad metadata or
  segment or suffix digests, wrong sizes, truncated or partial committed
  frames, inconsistent ordinals, and arithmetic overflow. Verify empty
  suffixes, cap and overlap admission refusal, canceled acquisition and
  reads, pin expiry with an admitted reader, and uncertain publication
  retry. Invalid authority fails startup before listening and is
  preserved for diagnosis. A clean refusal preserves the previous
  authority and committed bounds. Export and restore use the existing
  bounded staging protocol and preserve a populated destination.
  [Backup staging](0009-backup-staging-recovery.md),
  [Immediate post-restore crash evidence](0027-immediate-post-restore-crash-evidence.md)
