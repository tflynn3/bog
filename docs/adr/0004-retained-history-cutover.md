# ADR 0004 — Retained-history runtime cutover

**Status** — Cycle 42 integration contract. The pinned base serves the
legacy WAL layout. The HTTP handoff below is recorded from #13 candidate
`dd46a1e`. The target adds positioned tail pages in `64ed0cc`. #53 owns
the retained adapter and startup wiring. Composed Linux qualification is
pending.

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
[The system architecture](../architecture/SYSTEM.md) records the observed
Linux import graph and the registry update required at integration.

## Storage and HTTP seam

#13 owns admission, filter canonicalization, cursor authentication, NDJSON
framing, and the consumer interface in `internal/httpapi`. At cutover,
`cmd/bog` bridges the consumer types to the storage adapter in
`internal/retained` and injects that bridge via `httpapi.NewWithPageSource`.
The Linux HTTP import of WAL in #13 still classifies uncertain ingest
outcomes. The query path uses the consumer seam.

```go
type PageSource interface {
    Page(context.Context, PageRequest, *store.QueryWork) (PageResult, error)
    Release(capture string) error
}
```

| Type or operation | Contract |
| --- | --- |
| `httpapi.PageFilter` | `Start, End time.Time` and `Contains string`. Time bounds are canonicalized and half-open. The literal is case-sensitive. |
| `httpapi.PageRequest` | `Filter PageFilter`, `Limit int`, `Snapshot bool`, `Capture string`, and `After PagePosition`. With `Snapshot: true` and an empty ID and position, the source captures and scans in the same call. A continuation supplies both and resumes strictly after its position. An ordinary query needs no retained capture or lookahead. |
| `httpapi.PageResult` | `Capture string`, `Rows []PageRow`, and `More bool`. The first snapshot page returns a nonempty opaque UTF-8 capture ID. Each `PageRow` contains a `store.Row` and an opaque UTF-8 `PagePosition` (a string type). `More` establishes whether a further match exists for a snapshot. The ID and position must fit the signed cursor. |
| `*store.QueryWork` | The shared request budget charges scan, seek, merge, filter, and lookahead work. HTTP also charges cursor validation, response preparation, and delivery against it. |
| `Release(string) error` | Release capture ownership idempotently, including when racing with `Page`. Release or expiry bars new operations. Each admitted operation retains its own references until it returns. |

At capture, the WAL owner pins the validated history generation and the
matching WAL identity or epoch together. Capture fixes the covered frame
boundary, first uncovered ordinal, and durable query-visible end. The source
reserves a bounded slot and sets a finite expiry. The base WAL primitive
`(*wal.Log).Capture(context.Context) (*wal.Capture, error)` pins only the
published WAL prefix. Its `Identity`, `CommittedEnd`, and `NextOrdinal`
describe that prefix, with `NextOrdinal` identifying the first row after it.
The WAL rejects capture on cancellation, closure, or an uncertain commit.
`ReadAt` returns EOF on reads past the captured end. `Release() error` is
idempotent. Subsequent reads return `os.ErrClosed` and already admitted
reads can drain. #53 must coordinate this WAL boundary with the history
pin under the same commit and activation synchronization.

Page positions follow the total order of UTC timestamp and global append
ordinal. The target's `Store.QueryPositionedPage` exposes ordered tail rows
with global ordinals using the captured tail count and first ordinal.
Merge the checkpoint and captured tail in that order, preserving
duplicates, equal timestamps, and backfill. The captured view includes whole
durable groups through its fixed end. Later commits and generation
replacement leave membership and positions fixed. Reclaim retired files only
after authoritative state and file identity have been validated and every pin
has drained. A failed or cancelled capture releases any acquired
references. Shutdown stops new operations and drains users before closing
the owner. Set finite budgets for live captures, concurrent readers, tail
memory and replay. Account for validation and the largest admitted frame
separately from metadata and the scan working set.

HTTP calls `Release` after a successful first-page acquisition that
publishes no cursor, including when later response preparation fails. The
source cleans up a failed acquisition before returning. HTTP logs release
errors. Keep captures with issued cursors retryable through terminal pages
until expiry or explicit release. An interrupted body write can still expose
the cursor header.

## Storage export lifetime

`backup.Export(ctx context.Context, snapshot backup.Snapshot, destination string) error`
consumes an already-pinned capture of the same coordinated storage view.
`Snapshot.Point() backup.Point` gives the manifest generation digest,
`CoveredOffset int64`, `NextOrdinal uint64` for the first uncovered row,
and `CommittedEnd int64`. `OpenManifest()` and `OpenSegment(name string)`
return fresh `io.ReadCloser` values. `OpenWAL()` returns a fresh
`backup.WALReader` (`io.ReaderAt` and `io.Closer`) with absolute WAL
offsets. Export owns the opened readers and reports I/O and close failures.
The caller retains the generation and WAL pins until Export returns, on
success, cancellation, or error. Sharing a snapshot across exports requires
concurrent opens and a reference for each export.

The v1 exporter validates the captured legacy WAL prefix against the
manifest, then copies only complete frames in
`[CoveredOffset, CommittedEnd)`. It derives metadata from the captured
bounds. Later appends and manifest publication leave these inputs fixed.
An uncertain destination publication requires reopening and verifying the
destination before retry. The v1 input contract requires readable prefix
bytes. #53 owns any export adaptation for an active tail-only layout.

## Paging and delivery

- Start a walk with `page=1`. The continuation binds the capture, canonical
  time bounds, literal, and exclusive last emitted position. Equivalent time
  zones are allowed and clients may vary `limit`. A malformed or mismatched
  token returns 400 `invalid cursor`. An authenticated token with an expired
  deadline or a source error matching `httpapi.ErrCaptureExpired` returns
  410 `query capture expired`. #13 caps token lifetime at 15 minutes and
  generates a signing key at startup, so a prior issuer's token fails
  authentication after restart. Start a fresh walk with `page=1`.
- Read the requested page using the shared work budget. When #13 prepares a
  next cursor, it also meters an immediate continuation with a separate
  request-sized budget at the same limit, measuring its response without
  allocating a second body. Finish
  preparation before committing the exact `Content-Length` and any
  `Bog-Next-Cursor`. Hold the query admission slot through body delivery.
  Ingest has independent admission.
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
  Capture admission failure matching `httpapi.ErrQueryCapacityExhausted`
  returns 503. Context cancellation or deadline errors before commitment
  map to 408, and unexpected storage errors map to 500. These responses
  contain no partial NDJSON page or continuation. Failed continuations
  preserve the request cursor for retry while the capture is live. A
  smaller `limit` may help a dense walk. Sparse or absent matches may
  still require more work to prove exhaustion.
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
recovery mutation. A restored image becomes writable only when the
checkpoint identity, WAL epoch, and initial commit point form durable
recovery authority. [ADR 0005](0005-writable-restore-authority.md) defines the
validation, publication order, interrupted activation, and restart rules.

The capacity launcher clears inherited `BOG_CAPACITY_CONTROL`, then sets
`BOG_CAPACITY_CONTROL=1` for both candidate starts in live mode. Legacy mode
leaves it unset. The launcher reserves a loopback address and puts the child
in the measurement cgroup before allowing it to exec the candidate. #53
owns honoring the opt-in, requiring a loopback listener, and leaving the
control unmounted by default. The control uses the same listener and
storage owner as production ingest and query.

- `POST /_capacity/compaction?action=start&cutoff=N` starts asynchronous
  compaction of exactly the committed append prefix of N rows.
- `GET /_capacity/compaction?action=status&cutoff=N` reports progress.
  Both operations return JSON with `state` (`running` or `complete`),
  `generation`, `covered_rows`, and `committed_rows`. Coverage and
  generation describe the published checkpoint. Completion requires a new
  durably published generation covering exactly the requested cutoff.
- A missing or failing control fails live qualification. The fixture
  requires a continuation and an acknowledged batch while compaction is
  running, verifies the new generation and remaining tail, and compares
  complete walks including after restart.
- #57 owns bounded teardown on success, failure, and cancellation. It must
  reap the candidate and drain descendant processes, including when the
  parent cgroup has no direct members. Failure to drain or remove a
  disposable cgroup fails capacity qualification.

## Ordered tracks and handoff

| Track | Write ownership and handoff |
| --- | --- |
| Architecture | Owns this ADR, ADR 0005, the operator README, system graph, and sole fitness-registry edits. Lands role and edge changes with the wiring that requires them. |
| #53 storage and bootstrap | Sole writer of runtime storage and `cmd/bog` composition. Records the adapter's concrete types, imports, capture and export lifetime, activation failures, and capacity handler behavior for the composed tree. |
| #13 HTTP | Owns `internal/httpapi`. Supplies the consumer types and HTTP failure mapping above. |
| #55 checkpoint recovery | Owns recovery of an inaccessible empty checkpoint staging directory under the publication lock. Resolve interrupted checkpoint publication before using that generation. |
| #57 capacity and release evidence | Owns capacity teardown and release evidence. Uses the production launch contract to qualify the same composed binary at both sizes. |

#54 landed the verifier registrations and release lifecycle hardening in
`3afefb5`. Slipway recorded black-box verification and review for that
change. [#54 closure](https://github.com/tflynn3/bog/issues/54#issuecomment-7189446899)

The composed release verdict comes from the pinned Linux gate. #58 remains
queued.

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
3. Review both sides of the merge base with the current target ref.
   Derive Linux production imports from the composed #53 and #13 sources.
   Promote checkpoint, history, and backup when reachable from `cmd/bog`,
   register `internal/retained`, and remove retired edges and entries in the
   same integration change. Keep dependencies directed from the composition
   root toward HTTP and storage. Run `go test -count=1 ./internal/archtest` and
   `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./...` on that tree.
4. Dispatch the [Linux release gate](../../verify/release/README.md) using
   the full composed commit SHA. Qualify one clean candidate built with Go
   1.27.0 on Ubuntu 24.04 amd64. Admission, durability, live capacity, and
   restored-parity reports must pass for fixture `linux-release-v1` with
   matching revision and binary SHA-256. Live capacity uses delegated cgroup
   v2 and local disk, with 196,608 and 393,216 retained rows under a 128 MiB
   memory cap at each size. Require process and disposable-cgroup cleanup.
   Keep the legacy baseline for comparison and publish build metadata,
   reports, and logs in the release artifact.
