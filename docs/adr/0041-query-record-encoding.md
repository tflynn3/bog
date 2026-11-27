# ADR 0041 — Shared NDJSON query record encoding

**Status** — Cycle 181 decision. Core Engineering owns HTTP query record
encoding. Storage owns the retained runtime composition in #108.

## Decision

`internal/httpapi` owns the wire format. The private
`(*queryResponse).appendMeasuredRow` takes a JSON-encoded timestamp, a raw
line, its exact framed record length, and an optional already-escaped line
payload. It assembles the timestamp and line in wire order, includes the
terminating newline, and allocates exactly the measured length. It adds
the record and its byte count to the response after successful assembly.
Escaping uses the same UTF-8-safe chunk boundaries and JSON options as
measurement, including the existing replacement of malformed UTF-8.

Callers still own measurement, fit checks, and work reservations. Ordinary
queries and retained opening and replay pages measure through `appendRow`
before assembly. Retained certification measures without assembling records.
Legacy openings measure once and immediately copy a complete single-chunk
payload from scratch when available. Legacy replay uses its certified
lengths and reserves line-byte work before encoding. Assembly checks
cancellation before each chunk it encodes. Copying borrowed scratch into
the record lets the next measurement reuse the buffer safely.

Exact sizing keeps rejected records out of the response allocation and
gives each completed page its exact `Content-Length`. It costs an escape
pass to measure ordinary and retained records and another to assemble
those that fit. Legacy opening scratch reuse avoids the second pass for
single-chunk lines, and legacy replay pays for its stored lengths in
capture metadata. The page writer still stops after a short write or
write error. The cursor delivery and capture lifetime rules govern retries,
including byte-exact replay of the terminal response.
[Query delivery contract](0039-query-delivery-and-retained-cutover.md),
[Exact terminal replay](0036-query-replay-wire-evidence.md)

## Caller inventory and retirement

The existing constructors and HTTP-owned `PageSource` contract are the
integration seams. Review the following inventory against the exact
candidate revision before retiring legacy paging. Record the disposition
of each production caller, direct or wrapped constructor reference,
fixture, and external verifier. Preserve coverage for ordinary WAL
startup and recovery through the retained runtime.

| Seam | Callers on the Cycle 181 base |
| --- | --- |
| `NewWithQueryLimits` | The serving command in `cmd/bog/ready.go`, the compatibility constructor chain, HTTP handler tests, and the legacy runtime fixture in `verify/query-model`. |
| `New` and `NewWithQueryMaxResponseBytes` | HTTP handler tests and Linux fixtures in `internal/readiness`, `verify/release`, and `internal/client`. Both delegate to `NewWithQueryLimits`. |
| `NewWithPageSource` | Retained HTTP handler and query-model fixtures, plus Linux fixtures in `cmd/bog-verify` and `cmd/bog-export`. This is the command seam for the #108 Storage adapter. |
| Issuer HTTP clients | `internal/client`, operator commands, and release and query-model probes exercise framing and opaque cursors through HTTP. Include their legacy-mode coverage and invocation paths in the inventory. |

Before removal, run the HTTP API race suite for exact JSON encoding and
allocation, byte cuts and oversized-record diagnostics, minimum-budget
legacy and retained continuation walks, cancellation, failed and short
delivery, and retirement through final drain. The minimum-budget
fixtures are `TestLegacyCursorIssuingBudgetOracle` and
`TestCertifiedQueryWalkAtMinimumBudget`.

Require Storage review of the serving adapter's pin, replay, release, and
shutdown behavior. Require the strict #108 gate to pass for the same
reviewed revision on native Linux amd64 and arm64. The evidence includes
exact held and fresh walks across acknowledged backfill and generation
replacement, exact restart and restore parity, fixed-budget progress with
changed resume limits and byte-trimmed pages, terminal body equality, and
capture release and pin drain. Review how each legacy fixture or gate case will
retain equivalent coverage before deleting it.
[Constructor and topology boundary](0040-retained-runtime-constructor-boundary.md),
[Native release evidence](../architecture/SYSTEM.md#release-evidence)

## Review and dispatch

Core Engineering routes the shared encoder change for Storage review at
the #108 handoff. At dispatch, recheck the live repository leases for the
two HTTP files and this ADR, and confirm that 0041 is available on the
current target branch. Storage retains the #108 command, WAL, history,
startup documentation, and topology promotion scope.
