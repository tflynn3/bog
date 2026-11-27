# ADR 0038 — Writable authority verification

**Status** — Proposed for Cycle 166. The encoding below records the
expectations in the native parity producer at the review baseline.
Storage confirmation under [#108](https://github.com/tflynn3/bog/issues/108)
is pending. #108 remains the release blocker.

## Ownership

Under its directory lease, Storage owns the writable selector and the
ordering of recovery, append publication, and generation activation.
Release Verification observes the candidate's durable selection through
the pinned data directory after the candidate has been reaped.
[Tail-only recovery contract](0035-tail-only-retained-recovery-contract.md),
[Pinned writable authority](0005-pinned-writable-recovery-authority.md)

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
| `covered_offset`, `committed_end` | Signed 64-bit logical WAL byte offsets including the eight-byte WAL magic in the original coordinates. ADR 0035 requires `8 <= covered_offset <= committed_end`. The committed suffix is the half-open range between them. |
| `next_ordinal`, `committed_ordinal` | First uncovered and first free global append ordinals, respectively. The verifier compares these with the independently computed fixture positions. |
| `tail`, `tail_base` | A single `bog.wal.tail-*` basename in the pinned directory and the signed 64-bit logical offset represented by its physical byte zero. |
| `tail_device`, `tail_inode` | Unsigned 64-bit device and inode numbers, compared with the stat of the opened regular file. |
| `suffix_sha256` | Lowercase hexadecimal SHA-256 of the exact committed suffix bytes. An empty suffix uses the SHA-256 of the empty string. |

Required fields must be present with their exact names and non-null values.
Malformed JSON and duplicate keys fail decoding. Additional fields are
allowed for the recognized version and remain covered by the payload checksum.
An unreadable or invalid selector fails the evidence case.

## Tail-only parity observation

In both cycles the fixture requires a fully rebased tail.
`tail_base` equals `covered_offset`, and the exact physical length is
`committed_end - tail_base`, bounded by the 4 KiB fixture limit. The
verifier compares the byte and ordinal bounds with the fixture and reads
the named tail through its descriptor. It checks the descriptor's identity
independently of the exact frame bytes and their SHA-256. It follows the
selector even when another tail contains identical bytes.

The successful observation transfers ownership of the open tail descriptor
to its caller. The export adapter validates the selected manifest and uses
the bounded suffix snapshot through backup's `RecoveryView`. The caller
holds the original descriptor across restart to keep the inode from being
reused. After a restarted append is acknowledged and the child is reaped,
the verifier observes the selector again. It requires the same physical
file, the same checkpoint boundary, and the exact extended bytes, digest,
committed end, and first free ordinal. HTTP walks continue to use the
independent append-log oracle.
[Independent recovery-view consumers](0037-independent-recovery-view-consumers.md)

Release Verification will confirm the encoding with the #108 owner
before changing the verifier's expectations. Qualification requires the
existing strict parity gate against the same reviewed candidate revision
on native Linux amd64 and arm64.
[Native release verification](../../verify/release/README.md)
