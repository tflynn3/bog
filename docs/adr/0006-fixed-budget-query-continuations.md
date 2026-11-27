# ADR 0006 — Fixed-budget query continuations

**Status** — Cycle 49 integration decision. The base at
`dd2b61184a115f72f77a89ae995dbfec67d46cb7` serves the legacy WAL and
in-memory query view. This contract applies to the composed #53 and #59
runtime. [ADR 0004](0004-retained-history-cutover.md#paging-and-delivery)
records the earlier immediate-continuation probe. This decision supersedes
that probe with certification through exhaustion or the first oversized match.

## Decision

A snapshot walk starts with `page=1`. Before committing a first page with
`Bog-Next-Cursor`, HTTP certifies that the rest of the walk can reach
exhaustion or the first oversized match under the same configured per-request
work budget and response byte cap. Certification covers retries and changes
to a valid `limit`, including smaller limits on resume. The first request pays for
capture, certification, and its own response within its work allowance.
The certification must also bound each later request within that same
allowance, including cursor authentication, source work, response
preparation, and delivery.

Certify bounded scans on the pinned view and retain a bounded replay plan.
Each retained-source replay uses the same source limit and
exclusive anchor as a certified scan. HTTP applies the requested row
limit and byte cap to the replayed rows. This lets a continuation split a
scan's rows or cross a scan boundary without introducing an uncertified
seek across a sparse range. Account for positioning, seek, merge, filter,
lookahead, encoding, and delivery, including the work to establish that
no further match exists. Bound live captures and plan metadata. An
ordinary query uses the same request limits without retaining a walk. The
legacy in-memory plan certifies replay using its captured ordered match offsets.

Each page emits the longest ordered prefix that fits the row and encoded
byte caps. If the byte cap trims the result, the continuation starts
strictly after the last row actually emitted. A page carrying a cursor
has at least one row and a remaining match. A terminal page establishes
exhaustion, including through trailing nonmatches. The captured
membership and order remain stable across appends, backfill, and
generation replacement. Ties use the global append ordinal.

When the available work or plan bounds cannot certify the walk, return
503 `query work budget exhausted` before committing any NDJSON or cursor.
If a matching record cannot fit by itself under the response cap, issue the
cursor alongside the preceding fitting rows. The request that reaches the
oversized match as its first row returns 422 with the same exact encoded size
on every retry while the capture is live. A walk that opens at an oversized
match returns 422 immediately. Query or capture saturation can still return
503 `query capacity exhausted`. Hold query admission
through body delivery. Failures before commitment preserve the request
cursor for retry while the capture is live. The prepared response has an
exact `Content-Length` so clients can detect an interrupted body.

## Capture lifetime and composition

HTTP owns the consumer-side `PageSource`, admission, cursor authentication,
and framing. `cmd/bog` injects the retained adapter with
`httpapi.NewWithPageSource`. The adapter captures the history generation
and exact durable, query-visible WAL tail under the storage owner's
synchronization described in [ADR 0005](0005-pinned-writable-recovery-authority.md#capture-and-activation-handoff).
Repeating a source page with the same capture, canonical filter, limit,
and exclusive position must yield the same ordered rows, positions, and
`More` decision within its originally metered work, including after
generation replacement.

The first source result may supply `ExpiresAt`. Use the earlier of that
deadline and HTTP's 15-minute maximum lifetime, with zero `ExpiresAt`
leaving the HTTP deadline in force. Resume preserves the original
deadline. A process-local signing key binds the cursor to the capture,
canonical half-open time bounds, case-sensitive literal, and last
emitted position. A malformed token, filter mismatch, or token from a
previous issuer returns 400 `invalid cursor`. An authenticated cursor
whose capture is expired or unavailable returns 410
`query capture expired`. Start a fresh walk with `page=1`.

The source cleans up failed acquisitions. HTTP owns successful acquisitions
and releases openings that prepare no cursor, fail before commitment, or
encounter a transport failure that confirms headers were never sent. A
`ResponseWriter` that can guarantee this outcome and prevent a later write
may wrap `httpapi.ErrQueryHeadersUnsent` in the opening header flush error.
A generic transport error leaves delivery uncertain. Once a cursor can
reach the client, keep the capture retryable even if the body is interrupted.
The input cursor of a terminal page replays its exact bytes until expiry or
explicit retirement.

Both constructors expose `RetireQueryCapture` and `Close` on the issuing
handler. Retirement authenticates either cursor format and is idempotent.
Expiry, retirement, and shutdown bar new resumes. Admitted requests keep
their references through delivery. After the final user drains, HTTP calls
`PageSource.Release` once and keeps the capture charged until release returns.
The command drains HTTP and calls `Close` with a deadline before closing
storage. [Cursor retirement](0031-retained-cursor-retirement.md)

## Ownership and qualification

The bog architecture specialty owns this decision, the system graph,
and the fitness registry. The bog query specialty (`bog-query`) owns
#59 and the HTTP certification and expiry contract. `bog-storage` owns
#53, including the adapter, coordinated capture, and runtime composition.
The query and storage owners review deterministic replay, deadline
propagation, and reference ownership at the `PageSource` handoff.

Qualify the composed revision with the pinned
[Linux release verifier](../../verify/release/README.md). The issuing
budget must complete both the tied-row walk and the sparse,
byte-trimmed walk with a smaller resume limit. Lower sampled budgets
must reject cleanly before issuing a cursor. Check that a held cursor
survives generation replacement and that fresh walks on a writable
restore match the append-log model after ingest and restart. The
[system architecture](../architecture/SYSTEM.md#fitness-and-registry-ownership)
records the registry updates and checks required in the wiring merge.
