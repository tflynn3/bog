# ADR 0039 — Query delivery and retained cutover

**Status** — Proposed for Cycle 169. Core Architecture owns this decision.
Storage, Query, and Release Verification review the runtime, HTTP, and
evidence handoffs respectively. Query's reviewed #116 change and Release
Verification's native gate are on main. Storage's #108 composition and
qualification of the combined runtime are pending.
[#108](https://github.com/tflynn3/bog/issues/108),
[#116 handoff](https://github.com/tflynn3/bog/commit/5c70815f3271ece893cfdc725b3a8a0b18c2e29d),
[Native gate handoff](https://github.com/tflynn3/bog/commit/8d62fb925c4b9b7b86856af99a6f4c1732e9fba7)

## Fixed-budget delivery

Before #116, ADR 0006 required 422 before issuing any cursor if any
matching record could not fit by itself under the response cap.
The #116 handoff updated ADR 0006 to supersede that whole-walk refusal
rule. Before issuing a cursor,
certify progress through exhaustion or the first individually oversized
match. The first request pays for capture, certification, and preparation
and delivery of its own response. Each continuation, including retries and
resumes with a different valid row limit, must fit the same per-request work
budget and encoded byte cap. Meter positioning, seek, merge, filtering,
lookahead through trailing nonmatches when proving exhaustion, cursor work,
encoding, and delivery. Bound live captures and replay metadata. If work
or plan capacity prevents certification, return 503
`query work budget exhausted` before sending NDJSON or a cursor. Query or
capture admission pressure returns 503 `query capacity exhausted`.
[ADR 0006](0006-fixed-budget-query-continuations.md)

Serve the longest ordered prefix of complete NDJSON records that fits both
the requested row limit and the encoded byte cap. Resume strictly after
the last emitted row, including after a byte cut. A page with a next cursor
contains at least one row and has another match ahead. At exhaustion,
return a terminal page without a next cursor. If the first remaining
match cannot fit in an empty page, return a complete plain-text 422 without
NDJSON or a next cursor. The diagnostic is
`query record exceeds response byte limit (cap N bytes, requires M bytes)`
with a trailing newline. Measure the exact JSON-escaped record, including its
newline. An opening at that match returns 422 immediately. An opening with a
fitting prefix can issue a cursor, and the continuation that starts at the
oversized match returns the same 422 and diagnostic on retry while the
capture is live. Rows beyond that match remain ordered and visible to a
fresh query whose filter excludes it.

Retained replay repeats the certified source limit and exclusive anchor
even when HTTP changes the client page size. The legacy handler uses its
captured ordered match offsets. Both preserve captured membership across
appends, backfill, and generation replacement, using UTC timestamp and
global append ordinal for order. A terminal continuation's input token
replays the exact NDJSON bytes on the same request while its capture is
live. Each successful response declares its exact `Content-Length` so
clients can detect an interrupted body. Ordinary queries use the same
request limits without retaining a walk.
[Exact terminal replay](0036-query-replay-wire-evidence.md)

## Capture ownership and the delivery boundary

Both `NewWithQueryLimits` and `NewWithPageSource` expose
`RetireQueryCapture` and `Close(context.Context)` on the issuing handler.
HTTP owns admission, cursor authentication, expiry, the bounded replay
plan, and response framing. The source cleans up failed acquisitions.
After a successful acquisition HTTP owns its release. Keep query
admission and the request's capture reference through body delivery and
error cleanup. Register the plan before the cursor can reach the client.

| Opening outcome | Capture action |
| --- | --- |
| Preparation or cancellation fails before commitment, or the response has no cursor | Retire the opening and release any acquired source capture. |
| The server can prove cursor headers were never sent and cannot be sent later | Retire the opening and reclaim its slot after release returns. A writer may signal this guarantee by wrapping `httpapi.ErrQueryHeadersUnsent` in its header-flush error. |
| Headers were sent or might have been sent | Keep the capture retryable, including when the body is truncated, a write is short, or a generic transport error leaves delivery uncertain. Client observation of zero bytes alone is insufficient evidence for reclamation. |

Failures on a resume preserve its input cursor while the capture is live.
The issuer authenticates the capture, canonical half-open time bounds,
case-sensitive literal, and last emitted position. A malformed, foreign,
or filter-mismatched token returns 400 `invalid cursor`. An authenticated
resume of an expired, retired, or unavailable capture returns 410
`query capture expired`. Use the earlier of the source's optional expiry
and HTTP's 15-minute deadline, with no renewal on resume.

Retirement authenticates either cursor format on the issuing API and is
idempotent, including after cleanup. Expiry, retirement, and close bar new
resumes. Admitted handlers keep their references through delivery. HTTP
calls the idempotent `PageSource.Release` once per successful retained
acquisition, after the final HTTP user drains, and keeps the slot charged
until the release call returns. Releases may run concurrently with other
captures and source calls. Idle retirement returns its release result.
With admitted users it returns after barring new resumes. HTTP logs
asynchronous release failures and reports release errors that occur during
the bounded API drain.
[Cursor retirement](0031-retained-cursor-retirement.md)

## Retained composition and shutdown

Storage supplies the command-local `PageSource` through
`httpapi.NewWithPageSource`. Under WAL owner synchronization it selects and
pins a validated generation with the matching WAL identity, covered
whole-frame and ordinal boundary, and exact durable, query-visible suffix.
The source meters acquisition and scans and reproduces rows, positions, and
`More` for the same captured request within its original scan allowance.
Fresh captures see later published whole batches. Replacement preserves
the held generation and suffix, and reclamation waits for durable
selection and the final storage reference. The opt-in loopback retirement
route delegates to the same API instance that issued the cursor.
[Runtime cutover](0015-retained-history-runtime-cutover.md),
[WAL capture and activation](0005-pinned-writable-recovery-authority.md#capture-and-activation-handoff)

1. The command stops admitting new work and scheduling maintenance,
   cancels readiness probes, and drains HTTP while the source and WAL
   owner are still available.
2. After HTTP drains, call the issuing API's `Close(ctx)` with a deadline.
   Close retires captures and waits for admitted capture users and source
   releases. A deadline bounds the wait and does not certify release.
3. Drain remaining admitted storage operations, including appends,
   publication, and exports. Close the readiness descriptor after its
   users drain. Close the WAL owner and release the pinned directory lock
   only after successful drains. Surface shutdown failures. On a failed
   drain keep the WAL and lock held until process exit.

Storage owns writable restore, append visibility, and activation under
the same pinned directory and incorporates #47 and #15 in its WAL and
startup work. The #108 wiring PR records the measured topology in
`SYSTEM.md` and updates executable architecture fitness checks.
[Command composition](0026-command-owned-retained-storage-composition.md),
[Writable authority verification](0038-writable-authority-verification.md)

## Release evidence

Run the handler fixtures from the candidate checkout with
`BOG_QUERY_VERIFY_104=1` and the race detector. These seven existing cases
exercise the candidate's HTTP implementation over real loopback TCP.

| Required case | Evidence |
| --- | --- |
| `TestQuery104CaptureLifecycle/legacy-slot-reuse` | With 63 live captures, a confirmed pre-header failure leaves the 64th slot reusable. The next opening after filling it gets a clean 503. |
| `TestQuery104CaptureLifecycle/retained-slot-reuse` | With the retained source, the 64th opening succeeds after the confirmed pre-header failure and the next opening gets the capacity 503. |
| `TestQuery104CaptureLifecycle/legacy-retirement` | At 64 captures, terminal replay survives a forged retirement attempt. Valid repeated retirement bars new resumes with 410. Two paused resumes finish on their original capture, and capacity returns after both drain. |
| `TestQuery104FittingPages/legacy/opening-limit-1` | Start with a row-limited page, then resume at limit 10 through the byte cut to the same 422 twice. |
| `TestQuery104FittingPages/legacy/opening-limit-10` | Hit the byte cut on the opening and replay the blocked continuation's 422. |
| `TestQuery104FittingPages/retained/opening-limit-1` | Preserve the row-limited held prefix through acknowledged backfill and replacement, then reach the repeatable 422 after a byte cut on resume. |
| `TestQuery104FittingPages/retained/opening-limit-10` | Preserve the held prefix through replacement with an opening byte cut and the same repeatable 422. |

The fitting oracle checks held and fresh prefixes, timestamp ties and
duplicates, complete framing, exact 422 text, and an exact filtered walk
including fitting rows beyond the oversized match. Require the companion
`TestQueryCaptureSurvivesFailedOpeningWrite` on both issuers for a received
cursor with a truncated body, the original suffix after backfill and
retained publication, and byte-exact terminal replay. Require
`TestRetainedCaptureRetirementDrainsAtCapacity` for the retained drain.
[Query model handoff](../../verify/query-model/README.md)

On `ubuntu-24.04` (native amd64, `GOAMD64=v1`) and `ubuntu-24.04-arm`
(native arm64, `GOARM64=v8.0`), build the same clean, reviewed #108 and
#116 revision once per runner and use that runner's SHA-256-pinned binary
for every producer. Check the digest before and after each query race run.
From the pinned gate checkout, require `TestPinnedQueryModelReplay`
in ordinary and retained modes and `TestPinnedQueryModel104` at opening
limits 1 and 10 in legacy (without compaction) and retained (with exact
cutoff publication) modes. Require `TestPinnedQueryCandidateStartup` and
`TestPinnedQueryCandidateRecoversFromTransientForeignListener` for listener
ownership and startup handling. The gate uses the external executable
for pinned walks and candidate-compiled handler fixtures for socket faults
and paused resumes.

Strict validation consumes `query-model-events.jsonl` and
`query-handler-events.jsonl` on each runner. Require one passing event for
each required package, parent, and subtest and a matching full revision and
binary digest from both pinned query test roots. A required skip, a failed
run, a missing or duplicate required pass, or an identity mismatch blocks
qualification. Keep the existing fixed-budget backfill gate at the issuing
budget, strict restored and restart parity, and the live capacity and
pin-drain checks. Require both native verdicts and verified cleanup.
[Native release gate](https://github.com/tflynn3/bog/blob/8d62fb925c4b9b7b86856af99a6f4c1732e9fba7/verify/release/README.md#query-lifecycle-qualification),
[Fixed-budget backfill evidence](0030-fixed-budget-replacement-evidence.md)

### Native retained shutdown

On each native runner, run the opt-in scenario from the verifier checkout
against that runner's pinned #108 binary. Set `BINARY` to its absolute
path, `REVISION` to the full composed commit, and `DIGEST` to the
SHA-256 recorded by the runner's build. Retain the events and require the
named test to pass on both runners at the same revision.

```sh
BOG_RELEASE_RETAINED_SHUTDOWN=1 BOG_RELEASE_BINARY="$BINARY" \
  BOG_RELEASE_REVISION="$REVISION" BOG_RELEASE_BINARY_SHA256="$DIGEST" \
  BOG_RELEASE_FIXTURE=linux-release-v1 \
  go test -race -json -count=1 -timeout=90s -run '^TestReleaseRetainedShutdown$' \
    ./verify/release > retained-shutdown-events.jsonl &&
  python3 verify/release/check_gate_evidence.py test-events \
    retained-shutdown-events.jsonl bog/verify/release TestReleaseRetainedShutdown
```

The test checks the digest before launch, before restart, and after the
scenario. It publishes a predecessor with a committed suffix, captures
one page, and admits the continuation on a socket with a small receive
window. It confirms TCP backpressure and an occupied query slot, then
acknowledges backfill and publishes the successor at the exact cutoff.
The predecessor pin and blocked continuation must still be observable.
After SIGTERM the listener must stop accepting connections while the
candidate remains alive and an independent open of the directory is
denied the exclusive lock. The verifier then reads the complete framed
predecessor response, requires exit status zero, and reaps the candidate.
It restarts on the same directory and compares the complete ordered
NDJSON walk with the acknowledged append ledger, including timestamp
ties and duplicates.

Storage's command-level test must observe the issuing API's
`Close(ctx)` after HTTP drain, completion of source releases and
admitted maintenance, and readiness descriptor closure before WAL
close releases the directory lock. It must also exercise a failed
drain and verify that the WAL and directory lock stay held until
process exit. The two native runs and Storage's command-level evidence
are required before closing qualification of the reviewed #108 composition.

## Costs and dispatch

Certification and bounded response preparation add latency before the first
cursor. Flushing cursor headers before the body adds a transport flush.
A capture retains replay metadata and, on the retained path, generation
and suffix references until expiry or retirement and the final drain.
Ambiguous delivery can occupy one of the 64 capture slots for the full
bounded lifetime. Slow writes and source releases extend the time slots
and storage remain charged and can delay reclamation or cause capacity
refusals.

Cycle 169 reserved one Architecture track within the conservative
three-slot budget. At dispatch the ordered repository lease was held and
merge-ready for this ADR alone, with no other active repository lease
covering the path. The base and upstream main had no ADR 0039. Use the
reviewed Query and Release Verification changes above and confirm the
composition and drain sequence against Storage's reviewed #108 handoff
before qualifying the combined revision. Core Architecture routes this
single-path change for Storage, Query, and Release Verification review.
