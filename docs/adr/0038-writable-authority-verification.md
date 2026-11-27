# ADR 0038 — Writable authority verification

**Status** — Proposed, reviewed against the Cycle 171 source baseline.
Storage library and Release Verification changes establish the handoffs
below. Confirmation of the command-side writer under [#108] and native
qualification of the composed revision are pending.

## Ownership

One Linux process, one listener, one WAL owner, and one pinned data
directory form the consistency unit. Under the exclusive directory
lease, Storage owns recovery, append publication, and generation
activation. The command adapts the owner's selected generation and
committed suffix to the independent checkpoint and backup consumers.
Release Verification opens the selector read-only through its pinned
directory after the candidate has been reaped and decodes it independently
of the writer. The probe neither repairs nor selects authority.
[Tail-only recovery contract](0035-tail-only-retained-recovery-contract.md),
[Pinned writable authority](0005-pinned-writable-recovery-authority.md)

## Authority precedence and failure outcomes

| State under the directory lease | Recovery decision |
| --- | --- |
| Initial restore with confirmed selector absence and no unresolved activation | Verify the complete v1 or v2 image in the pinned directory. Check the backup metadata, exact manifest and segment coverage, and exact tail length, digest, and frame sequence. Derive the committed ordinal by replay for v1. Reject a mixed legacy and restore layout. Keep the verified input recoverable until the first writable selection is durable. |
| Interrupted initial activation or generation replacement | Resolve the protocol-owned intent and the actual selector under the owner and publication locks. If the predecessor is still selected, recover it. At initial activation, an absent selector permits retry from the verified image only when the protocol establishes that the install did not take effect. If the successor is selected, validate and sync that selection, then install it without repeating the same advance. Staged files and intent provide recovery context and carry no committed bounds of their own. |
| Restart with a writable selector | Recover the generation and tail named by `bog.authority` at its current committed end. `bog.backup` remains the immutable initial restore point. Validate the selected files and replay every committed frame before truncating any trailing uncommitted bytes. Recovery uses the service directory alone. |
| No retained or restore authority | Ordinary WAL recovery and bootstrap rules apply. |

A present selector takes precedence over restore metadata. An unreadable,
unsupported, corrupt, or inconsistent selector, a missing or substituted
selected file, or ambiguity in the activation state fails startup before
listening. Preserve the evidence for diagnosis. Only a positively
established absence can enter initial restore or ordinary recovery.

Stage and sync the selected generation, tail contents, and their names
before atomically installing the selector. Reconcile commits after the
cutoff under the same synchronization that orders append publication.
Sync the selector's directory before installing the view or opening the
listener on initial activation. A subsequent append syncs the whole batch
and durably updates the selected committed end, first free ordinal, and
suffix digest. Return HTTP 204 after the batch is durable and query-visible.

A confirmed failure before selection preserves the predecessor. An append
with a confirmed rollback leaves the previous committed bounds and query
view intact and returns 503. An uncertain selector rename or directory
sync, or an uncertain append commit or rollback, fences dependent
mutation and new captures. Preserve both recoverable views until the
owner resolves and syncs the selected authority under the locks. An
uncertain append outcome poisons the owner and requires process recovery.
A cleanup error after a durable switch can accompany a valid successor.
Check the installed selection before retrying. Reclaim only
identity-checked, protocol-owned files unreachable from durable authority
and all live pins.
[Publication and retry](0005-pinned-writable-recovery-authority.md#publication-retry-and-shutdown)

## Proposed selector encoding

The verifier reads the regular file `bog.authority` relative to the pinned
directory, with a 4 KiB cap on the full record. The JSON envelope requires
`payload` and `sha256`. The checksum is lowercase hexadecimal SHA-256 over
the exact JSON value bytes of `payload`, including whitespace inside the
value. The verifier checks this checksum before interpreting the payload.

| Payload fields | Binding |
| --- | --- |
| `version` | Integer version. The proposed verifier accepts version 2 and rejects unsupported versions, including version 1. A verified v1 backup remains a valid input to the candidate's writable activation. |
| `generation` | Lowercase hexadecimal SHA-256 of the exact manifest payload. It names `bog-checkpoint-<digest>`. The export adapter verifies the selected manifest's identity, covered boundary, and predecessor advance. |
| `covered_offset`, `committed_end` | Signed 64-bit logical WAL byte offsets in the original coordinates, including the eight-byte WAL magic. Require `8 <= covered_offset <= committed_end`. The committed suffix is the half-open range between them and contains exact whole frames. |
| `next_ordinal`, `committed_ordinal` | Nonnegative, zero-based global append ordinals. The checkpoint covers `[0, next_ordinal)`. Replay counts every decoded row in the suffix, including ties, duplicates and backfill, and ends at `committed_ordinal`, the first free ordinal. Empty byte and ordinal ranges agree. Reject overflow or a replay that disagrees with either bound. |
| `tail`, `tail_base` | A single `bog.wal.tail-*` basename in the pinned directory and the signed 64-bit logical offset represented by its physical byte zero. |
| `tail_device`, `tail_inode` | Unsigned 64-bit device and inode numbers, compared with the stat of the opened regular file. |
| `suffix_sha256` | Lowercase hexadecimal SHA-256 of the exact committed suffix bytes. An empty suffix uses the SHA-256 of the empty string. |

Required fields must be present with their exact names and non-null values.
Malformed JSON and duplicate keys fail decoding. Additional fields are
allowed for the recognized version and remain covered by the payload checksum.
An unreadable or invalid selector fails the evidence case.

Require `0 <= tail_base <= covered_offset`. Subtract `tail_base`
from the logical bounds to obtain the physical suffix range. Validate
that the opened file contains that entire range and verify its identity
separately from the suffix digest. On a rebase, copy
the exact remaining committed frames to the successor tail, set
`tail_base` to the new `covered_offset`, and sync the successor before
selection. The logical committed end and global ordinals survive the
physical relocation. Advances bind the predecessor generation and
boundary and the digest of the exact consumed frames. The selected
generation can be recovered once predecessor files have been reclaimed.

## Tail-only parity observation

In both cycles the fixture requires a fully rebased tail.
`tail_base` equals `covered_offset`, and the exact physical length is
`committed_end - tail_base`, bounded by the 4 KiB fixture limit. The
verifier compares the byte and ordinal bounds with the fixture and reads
the named tail through its descriptor. It checks the descriptor's identity
independently of the exact frame bytes and their SHA-256. It follows the
selector even when another tail contains identical bytes. The exact file
length is a parity assertion after clean exit. Restart recovery can
encounter uncommitted trailing bytes after interruption.

The successful observation transfers ownership of the open tail descriptor
to its caller. The export adapter validates the selected manifest identity,
boundary, and predecessor advance against the fixture, including the
consumed-frame digest. It supplies the bounded suffix snapshot through
backup's `RecoveryView`. Cycle two uses the verified v2 export of cycle
one. Each cycle checks the original directory after restart and an
independent restore with the source bundle out of reach. The caller
holds the original descriptor across restart to keep the inode from being
reused. After a restarted append is acknowledged and the child is reaped,
the verifier observes the selector again. It requires the same physical
file, the same checkpoint boundary, and the exact extended bytes, digest,
committed end, and first free ordinal. Held, fresh, restarted, and
separately restored HTTP walks use the independent append-log oracle
through exhaustion.
[Independent recovery-view consumers](0037-independent-recovery-view-consumers.md)

## Review evidence and qualification

| Owner | Scope confirmed in review | Reviewed revision and record |
| --- | --- | --- |
| Storage | The consumer-side review confirmed both interrupted-publication outcomes and repeatable cleanup after interrupted removal. Recovery follows the owner-supplied selection, including when it remains the predecessor. The fixtures exercised two restore and re-restore cycles with exact ordered walks and recovered ordinals. The owner handoff requires selection to be resolved before cleanup can proceed. | Slipway review `ebf53753-80a3-400b-9426-d02051b60c45` approved the [Storage candidate diff](https://github.com/tflynn3/bog/commit/db8815f477903d2de8e58bbbdab82f9825b955eb). The same patch landed at [Storage revision](https://github.com/tflynn3/bog/commit/a6677980921e55b1bded478d6893b1723eafdd6f). [#81 landing and review record](https://github.com/tflynn3/bog/issues/81#issuecomment-7216781854), [checkpoint owner handoff](../../internal/checkpoint/FORMAT.md#owner-handoff) |
| Release Verification | The parity review confirmed independent Unix selector decoding and descriptor ownership. The probe follows the selected inode even with a byte-identical alternative. A bad checksum or identity mismatch fails the observation, and restart must extend the same physical tail. These failures block parity evidence. | Slipway review `73ff3755-21d5-4c1e-9745-4bef1bc8a637` approved the [verifier candidate diff](https://github.com/tflynn3/bog/commit/9814df571e1d271ab226421bd310975ce20c9659). The same patch landed at [verifier revision](https://github.com/tflynn3/bog/commit/702b9f8483a48797ccc42adf5fe28e90b967b024). [Selector and restart tests](../../verify/release/tail_parity_authority_test.go) |

The two Slipway review records cover the library and verifier diffs.
GitHub records no associated PR reviews for those revisions. Storage
confirmation of the [#108] command and WAL writer still needs to cover
initial restore precedence, interrupted activation, restart, and fencing
when authority or append durability is uncertain. Release Verification
must confirm its independent probe against that exact writer revision.
Exercise interruption before selector installation, around rename and
directory sync, and after durable selection before in-memory activation.
Verify that recovery preserves every acknowledged batch and that a damaged
selector fences startup even when the original restore remains readable.
Keep this ADR Proposed pending the linked writer reviews and native
qualification. [#108] remains the release blocker. [#117] and [#118]
proceed in parallel.

Qualify one clean composed revision on native Linux amd64 and arm64.
Each runner builds once and uses its own SHA-256-pinned binary across the
release producers. Both must pass the existing strict cutover gate,
including all four `parity.json` outcomes under `linux-release-v1`.
Link both native runs and their reports with the full candidate revision
and the binary SHA-256 for each runner.
[System release evidence](../architecture/SYSTEM.md#release-evidence),
[Native release verification](../../verify/release/README.md)

[#108]: https://github.com/tflynn3/bog/issues/108
[#117]: https://github.com/tflynn3/bog/issues/117
[#118]: https://github.com/tflynn3/bog/issues/118
