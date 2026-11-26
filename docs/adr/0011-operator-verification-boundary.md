# ADR 0011 — Read-only operator verification

**Status** — Accepted and implemented. The command added in #67 is registered
as a verifier command in the Linux production graph.

## Context

Operators need to validate a published backup and compare its query results
with a server started from a restore of that backup. The server owns writable
recovery, publication, and readiness for its pinned local data directory.
The verification command runs separately and has no authority over that
directory. [System architecture](../architecture/SYSTEM.md)

## Decision

`cmd/bog-verify` uses the read-only verification and query APIs in
`internal/backup` for the expected recovery point and rows. The input image
must remain immutable through verification and the offline walk. A parity
check uses a separate server restored from that image and held at the same
recovery point for the comparison. Run the same half-open time range and
literal filter on the image and over HTTP. Use the production Go client's
query walker for complete NDJSON pages and opaque continuations. Compare
the ordered timestamp and line values, preserving duplicate rows and the
append order of equal timestamps. Confirm exhaustion on both sides, including
when the result is empty or page boundaries differ.
[Fixed-budget query continuations](0006-fixed-budget-query-continuations.md)

The command opens the image read-only and uses the server's query endpoint.
It must never acquire the WAL owner lock, activate recovery, export or
restore an image, settle publication, reclaim files, or invoke ingest or
maintenance endpoints. Cancellation, failed image verification, an
incomplete or malformed walk, and the first order or content mismatch must
surface through stable exit statuses and actionable diagnostics. Report
success only after verification and any requested comparison complete.

The command's root-module Linux production imports must have no direct or
transitive path to `internal/wal`, `internal/httpapi`, or
`internal/readiness`. The backup reader, the HTTP client, and their shared
row types are allowed. A registered intermediary cannot grant access to a
forbidden package. The import check protects this dependency direction.
Command tests must verify that the CLI uses only read-only operations from
the packages it imports. [Operator boundary fitness tests](../../internal/archtest/operator_boundary_test.go)

## Registration and release

Core engineering maintains the command's role and measured direct imports
in the Linux architecture registry. The operator boundary check requires
the registration and rejects direct and transitive paths to the excluded
packages. Its fixtures exercise shared read-side dependencies and
violations with fully registered edges that pass the existing exact-import,
cycle, and reachability checks.
[Linux architecture registry](../../internal/archtest/architecture_test.go),
[Command root boundaries](0012-command-root-boundaries.md)

#53 remains the release gate. Operator parity uses the verified image as
its reference. Release parity uses fixture inputs and the independent
append-log oracle to qualify the pinned server binary, including a live
append and an exact walk after restart. Operator results describe the
selected image and endpoint at the checked query scope.
[Release verifier composition](0008-release-verifier-composition.md)
