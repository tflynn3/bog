# ADR 0036 — Exact terminal query replay in release evidence

**Status** — Cycle 152 integration decision. Candidate qualification uses the
existing native Linux release gate.

## Decision

Core Engineering Release Verification owns the pinned-binary check. Query
owns the replay contract in #110. Architecture reviews the evidence boundary.
The verifier exercises a separate candidate process via the production Go
HTTP client. The same test-side pager checks query bounds, NDJSON framing,
and exact Content-Length for fixed-budget backfill and restored parity.
The fixed-budget adapter also classifies clean work-budget refusals using
the original HTTP response metadata and the client's read and close results.
[Release evidence process boundary](0013-release-evidence-process-boundary.md),
[Fixed-budget continuations](0006-fixed-budget-query-continuations.md)

Require exact equality of the encoded terminal NDJSON body when the oracle
replays the terminal request at the issuing budget. The oracle checks decoded
rows against the original append-log prefix and verifies exhaustion and body
length. The release pager observes the bytes the client reads on that same
request. The client owns body validation, reading, and closing. The adapter
uses the copy only after successful client acceptance and verifies its length.
Field order and other valid JSON encoding differences can change the bytes
while preserving row values and encoded length.

The pinned base exposes body length through the oracle. The release adapter
checks byte equality while #110's `WirePage.EncodedBody` contract is pending.
When that contract lands, pass the observed body to the oracle and require
it in the fixed-budget release probe.

## Cost and qualification

The observer allocates at most one configured response byte cap per accepted
or attempted HTTP 200 page. It retains the original terminal copy through
replay, so at most two caps of payload are live for the comparison. The current
fixed-budget fixtures cap each page at 96 bytes. The observer performs no
additional requests. Ordinary and retained fixed-budget trials use the
existing `TestReleaseFixedBudgetBackfill` event in the native amd64 and arm64
gate. Existing test events and report formats are preserved.
[Fixed-budget replacement evidence](0030-fixed-budget-replacement-evidence.md),
[Native Linux release workflow](../../.github/workflows/linux-release.yml)
