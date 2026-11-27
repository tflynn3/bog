# ADR 0031 — Retained cursor retirement

**Status** — Cycle 125 integration decision for #53.

## Ownership

Query owns `(*httpapi.API).RetireQueryCapture(token string) error` and the
certified HTTP cursor lifecycle. Storage owns the retained `PageSource`
adapter and the opt-in loopback capacity route in the #53 command
composition. The capacity route passes the continuation token to the same
API instance that issued it and returns an empty HTTP 204 on success.
Land the Query lifecycle change before mounting the control in `cmd/bog`.
[Fixed-budget continuations](0006-fixed-budget-query-continuations.md),
[Command-owned storage composition](0026-command-owned-retained-storage-composition.md)

## Replay and retirement

A terminal continuation sends no next cursor. Its input token still
replays the same terminal page while the capture is live, including
across generation replacement. The capacity runner retries that request
before using its input token to retire the capture. The existing deadline
remains the earlier of HTTP's deadline and the source's deadline, when
supplied.

HTTP authenticates the continuation with the issuing API's cursor codec
before consulting the registry. Malformed or foreign tokens return
`store.ErrInvalidQueryCursor`. Any authenticated continuation for the
capture can retire it. Repeating retirement succeeds, including after
expiry or cleanup. Later resumes of the retired capture return HTTP 410
with the original filter.

Retirement bars new resumes under the registry lock. Admitted resumes
keep their references through response delivery. HTTP calls
`PageSource.Release` once after the final reference drains and keeps the
capture charged against the registry limit until the release call
returns. Retirement of an idle capture returns the release result.
Retirement with admitted users returns after closing admission.
HTTP logs a release failure and `API.Close` reports failures from
releases that finish during its drain. The command drains HTTP and calls
`API.Close` before closing the Storage owner. Storage's idempotent release
contract governs reclamation after its own references drain.
[Retained resource admission](0010-retained-resource-admission.md)

Release Verification observes the retired-generation pin count before
retirement and waits for it to reach zero afterward on the composed
candidate. [Retained runtime observation](0023-retained-runtime-observation.md)
