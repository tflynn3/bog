# ADR 0039 — Query delivery and retained cutover

**Status** — Cycle 174 cutover decision. Core Architecture owns the
decision and measured graph. Query's reviewed #116 change and Release
Verification's native gate are on main. The reviewed Storage SHA, the
atomic runtime and governance landing, and both native verdicts are pending.
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

Ship #108 as the default bounded retained-history runtime. Keep one Linux
process, one HTTP listener, one writable WAL owner, and one pinned local
data directory. Establish writable authority before listening. Validate
the selected generation and replay its exact bounded committed suffix
on restart. Gate capacity control behind explicit opt-in on the loopback
listener. [Retained-history runtime cutover](0015-retained-history-runtime-cutover.md),
[Capacity-control qualification](0034-capacity-control-release-qualification.md)

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

The command supplies `readiness.NewHandlerForDirectory` with an independently
opened, unlocked descriptor relative to the WAL owner's pinned directory.
Readiness probes keep observing the active filesystem after a path retarget.
[Constructor boundary](0040-retained-runtime-constructor-boundary.md)

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
the same pinned directory. #108 includes the WAL shutdown cleanup from
#47 and startup filesystem diagnostics from #15.
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
(native arm64, `GOARM64=v8.0`), build the same clean, composed revision
with #108 and #116 once per runner and use that runner's SHA-256-pinned
binary for every producer. Check the digest before and after each query race
run.
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
pin-drain checks. Exercise both 196,608- and 393,216-row capacity fixtures
under a 128 MiB cgroup cap with swap disabled. Mark release readiness only
after both native jobs pass strict validation and verify cleanup.
[Native release gate](https://github.com/tflynn3/bog/blob/8d62fb925c4b9b7b86856af99a6f4c1732e9fba7/verify/release/README.md#query-lifecycle-qualification),
[Fixed-budget backfill evidence](0030-fixed-budget-replacement-evidence.md)

## Costs and integration

Certification and bounded response preparation add latency before the first
cursor. Flushing cursor headers before the body adds a transport flush.
A capture retains replay metadata and, on the retained path, generation
and suffix references until expiry or retirement and the final drain.
Ambiguous delivery can occupy one of the 64 capture slots for the full
bounded lifetime. Slow writes and source releases extend the time slots
and storage remain charged and can delay reclamation or cause capacity
refusals.

#108 is the sole PM-selected item for Cycle 174. Run three ordered tracks
for Storage runtime wiring, Core Architecture governance, and Release
Verification integration. With the active constructor guard, that uses
four of ten delivery slots. Keep #119–#122 queued. The proposed write sets
are disjoint from one another and from both held leases in the supplied
scope, including the constructor guard's ordered reads. Recheck live
leases at dispatch and at integration handoff. The constructor guard and
its ADR remain under their existing ownership.

Storage hands off an immutable reviewed SHA containing the #108 runtime
wiring. Core Architecture records that SHA here and measures the exact
root-module production imports and roles for Linux amd64 and arm64.
Review dependency direction, acyclicity, staged
ownership, and operator and evidence isolation. Promote checkpoint,
history, and backup together with the server wiring once both inventories
and review confirm their activation criteria. Keep keyed ingest staged
under its own owner and criteria. Query reviews the `PageSource` and
capture lifecycle.

Run `go test -count=1 ./internal/archtest` on the composed revision.
Require both measured inventories to agree with the executable registry
and require the Mermaid graph and package table to match it. Land the
reviewed Storage and Architecture diffs in one PR or atomic merge.
Release Verification qualifies that composed revision and owns the
candidate evidence.

The Cycle 174 base `c3724de2bb26fa234c32f6ebf0e5d21f9e9fb679`
has identical Linux amd64 and arm64 inventories of 19 root-module
production packages and 23 direct root-module imports. In each,
`cmd/bog` imports HTTP, readiness, store, and WAL and uses ordinary WAL
recovery and in-memory paging. The baseline with the governance checks
passes the uncached architecture suite, architecture vet and race checks,
and the Linux amd64 cross-build. The reviewed Storage SHA is pending.
Composed inventory agreement, retained role promotion, and native
qualification remain pending.
[Measured graph and fitness ownership](../architecture/SYSTEM.md#fitness-and-registry-ownership)
