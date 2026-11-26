# ADR 0004 — Retained-history runtime cutover

**Status** — Proposed for Cycle 38. Agree on the storage and HTTP seam in
this decision before those tracks fan out. Record the shipped graph and
release evidence on the composed revision.

The legacy server replays and retains the full committed WAL in memory. The
cutover serves immutable checkpoint history with a bounded committed tail,
including on a writable verified restore. The deployment and consistency unit
remains one Linux process, one listener, one WAL owner, and one pinned local
data directory. `cmd/bog` composes the runtime and holds the owner through
recovery, serving, and shutdown. The WAL owner holds the exclusive directory
lock and decides when a complete batch is durable and visible. Checkpoint
publication, generation activation, and reclamation coordinate with that
owner.

This decision specifies the live cutover for
[ADR 0001](0001-single-writer-retained-history.md),
[ADR 0002](0002-bounded-history-generation-pins.md), and
[ADR 0003](0003-wal-capture-and-restored-image-startup.md).
[The system architecture](../architecture/SYSTEM.md) records the base and
intended import graphs.

## Storage and HTTP seam

The HTTP package owns the consumer interface. The names below describe
operations and results. The storage and HTTP owners agree on the Go types
before starting independent implementation. `cmd/bog` wires the adapter.
Storage packages depend only on storage primitives. HTTP owns admission,
filter canonicalization, cursor authentication, and NDJSON framing.

| Operation | Contract |
| --- | --- |
| Capture | Under the WAL owner's commit and activation synchronization, pin the validated generation and matching WAL identity or epoch. Fix the covered frame boundary, next ordinal, and durable query-visible committed end. Allocate an opaque capture handle with a finite expiry and a bounded pin reservation. |
| Page | Accept a context, capture handle, half-open UTC range, exact case-sensitive literal, optional exclusive position, row limit, and a chargeable request work budget. Return ordered rows with an opaque position for each row, and a definitive indication of whether another matching row exists. |
| Export | Use the same storage capture primitive. Keep the pinned manifest, segments, and exact committed suffix readable for the export. Derive export metadata from the capture's bounds. |
| Release | Release capture ownership idempotently. Expiry or release bars new operations. In-flight scans and exports keep their own references through completion or failure. |

Page positions follow the total order of UTC timestamp and global append
ordinal. Merge the checkpoint and captured tail in that order, preserving
duplicates, equal timestamps, and backfill. The captured view includes whole
durable groups through its fixed end. Later commits and generation
replacement leave membership and positions fixed. Reclaim retired files only
after authoritative state and file identity have been validated and every pin
has drained. A failed or cancelled capture releases any acquired
references. Shutdown stops new operations and drains users before closing
the owner. Set finite budgets for live captures, concurrent readers, tail
memory and replay. Account for validation and the largest admitted frame
separately from metadata and the scan working set.

Release a first-page capture that issued no continuation as soon as its
operation ends. Keep captures with issued cursors retryable until expiry
or explicit release, including after preparing a terminal page. The server
cannot infer successful delivery from a completed response write. Admission
rejects a new capture when the pin budget is full.

## Paging and delivery

- Start a walk with `page=1`. The continuation binds the capture, canonical
  time bounds, literal, and exclusive last emitted position. Equivalent time
  zones are allowed and clients may vary `limit`. Reject bad or mismatched
  tokens with HTTP 400 `invalid cursor`. Report unavailable or expired captures explicitly,
  including after restart, and require a fresh first-page request to start
  another walk.
- Use one bounded storage page call for each request, including the first
  page. Charge filtering, seek and merge work, and enough lookahead to
  establish whether a match remains. HTTP uses the same request budget for
  cursor checks, encoding, and delivery reservation. Finish these steps
  before committing the success headers, including the exact `Content-Length`
  and any `Bog-Next-Cursor`. Hold the query admission slot
  through body delivery. Ingest has independent admission.
- Emit the longest ordered prefix that fits the row limit and encoded byte
  cap. If the byte cap trims the storage result, sign the position already
  attached to the last emitted row. An omitted fetched match establishes
  that another page exists. This avoids a second scan to recover an anchor.
  A successful page with a continuation contains at least one row. A
  terminal page requires established exhaustion. If the first match exceeds
  the byte cap, return 422.
- When the work budget cannot establish and prepare the response, return
  503 `query work budget exhausted` with zero NDJSON rows and no next cursor.
  This covers exhaustion while looking for the first match, while proving
  terminality, and after finding rows but before completing preparation.
  Cancellation and storage errors before response commitment also return no
  partial page or continuation. Failed continuations preserve the request
  cursor for retry while the capture is live. A smaller `limit` may help a
  dense walk. Sparse or absent matches may still require more work to prove
  exhaustion.
- The Go client validates framing, rows, and continuation before callback
  delivery. Reject an empty page carrying a continuation and any next
  cursor already seen in the current walk, including the starting cursor of
  `WalkFrom`. This catches both self-loops and cycles such as A → B → A.
  Return a protocol error and the cursor used to request the rejected page.
  Advance only after a successful callback. Cycle tracking is scoped to one
  walk invocation, with one entry per accepted continuation. A separate
  `WalkFrom` starts with its supplied cursor.

## Writable activation and capacity control

Classify the layout under the directory lock before legacy bootstrap or any
recovery mutation. Recognize a restore if either `bog.backup` or
`bog.wal.tail` is present. Validate the complete v1 image, including
manifest and segment integrity, ordinal coverage, and suffix size, digest,
and whole-frame coverage. Reject unsafe, damaged, incomplete, or ambiguous
layouts. An unactivated image mixed with legacy authority files is ambiguous.
Recognize an interrupted activation only through the activation protocol's
own durable state and validate it before resuming.

The v1 image omits the covered WAL prefix. Activate a recoverable append
state for the verified checkpoint and suffix under the existing owner.
Publish the durable authority for that state before listening or accepting
writes. Recovery resumes at the next global append ordinal after the
verified suffix. A restart after activation recovers the checkpoint, its
committed suffix, and every subsequently acknowledged batch through the
same writable path. Failure before activation completes leaves a recoverable
image or a validated, resumable activation state. Keep the source bundle
separate from the service directory. Replay and serving must not depend on
the source bundle after activation.

The proposed server opt-in is `-enable-capacity-control`. It is off by
default and requires an explicitly loopback-bound listener when enabled.
Mount `/_capacity/compaction` on that listener for disposable qualification
stores. `POST ?action=start&cutoff=N` starts asynchronous compaction for
exactly the committed prefix of N rows. `GET ?action=status&cutoff=N`
observes progress. The JSON fields `state` (`running` or `complete`),
`generation`, `covered_rows`, and `committed_rows` report observed state.
Completion requires a new durably published generation covering the
requested cutoff. The live capacity producer enables the control explicitly
and exercises continuation and ingest during replacement. Its legacy
baseline leaves the control off.

## Ordered tracks and handoff

| Order | Track | Existing owner | Write ownership and handoff |
| --- | --- | --- | --- |
| 1 | Architecture governance (#54) | bog | Owns the architecture decision, operator documentation, and sole fitness-registry edits. Publishes the seam before tracks 2 and 3 fan out. Registers observed imports and promotions on the composed tree. |
| 2 | Storage and runtime integration | bog-storage | Owns WAL, checkpoint, history, backup, store, and `cmd/bog` composition. Delivers writable activation, a bounded capture and merge, and the capacity control wiring on the single listener. |
| 3 | HTTP query | bog | Owns `internal/httpapi`. Implements the consumer seam and the HTTP budget, framing, and continuation rules. Agrees on the interface with track 2 before implementation. |
| 4 | Go client | bog | Owns `internal/client`. Rejects cyclic and malformed pages before delivery and preserves the cursor on rejection. |
| 5 | Release verification | bog | Owns verifier and release-workflow changes. Runs admission, durability, and live capacity against the same integrated Linux amd64 binary. |

The table orders contract and integration handoffs. Tracks with agreed
interfaces and disjoint write scopes can implement in parallel. The sole
live lease is `council/planning/**`, outside these scopes. There are no
active backlogs in flight. Keyed ingest and readiness filesystem sampling
remain backlog work.

## Cutover criteria

1. Compare complete HTTP walks with the independent append-log model across
   a generation swap, including overlapping checkpoint and tail ranges,
   duplicates, equal timestamps, backfill, sparse and absent literals,
   varied row limits, and byte-trimmed pages. Exercise budgets that fail
   before a match, during lookahead, and during response preparation.
   Exercise expiration, cancellation, shutdown, reclamation, and cyclic
   client pages.
2. Export one captured view, verify and restore it into an isolated
   directory, and compare a fresh HTTP walk to the original cutoff. Commit
   a new batch, restart, and compare a fresh walk to the restored rows plus
   that batch. Exercise interrupted activation and damaged or mixed layouts
   before listener startup.
3. With the current target ref, review imports on both sides of the merge
   base. #54 registers the two existing verifier commands and updates the
   role and observed direct imports of each promoted package in the same
   composed change that wires it into the server. Run
   `go test -count=1 ./internal/archtest` against that exact tree.
4. Dispatch the [Linux release gate](../../verify/release/README.md) using
   the full composed commit SHA. Qualify one clean candidate built with Go
   1.27.0 on Ubuntu 24.04 amd64. The three required producer reports must
   pass for fixture `linux-release-v1`, with matching revision and binary
   SHA-256. The live capacity run uses delegated cgroup v2 and a local
   disk, with 196,608 and 393,216 retained rows under a 128 MiB memory cap
   at each size. Keep the legacy baseline for comparison. Publish the
   build metadata, reports, and logs with the release evidence.
