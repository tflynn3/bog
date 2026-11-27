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
`BoundedWalk` for complete NDJSON pages and opaque continuations. Compare the
ordered timestamp and line values in a single pass, holding up to one page
from each side. Preserve duplicate rows and the append order of equal
timestamps.

The shipped server emits at least one row on each page carrying a cursor.
The client transport also accepts fully framed empty pages with fresh valid
cursors. The operator follows these continuations and charges each accepted
page, including empty and terminal pages, against that stream's page budget.
Report equality only after consuming all rows through both terminal pages.
Cursor cycles, framing failures, and exhausted page, byte, or time limits
leave the comparison incomplete. Report the first mismatch by row position.
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

## Linux restore drill evidence

The CLI drill compares the restored server with the immutable bundle before
and after an operator-controlled restart, with writes paused for both phases.
The opt-in Linux test harness supplies the restart boundary and launches an
explicit candidate on a separate disposable restore. After both read-only
comparisons succeed, the harness submits one backfill batch with tied
timestamps through HTTP and requires a 204 response. It restarts the same
binary on the same data directory, verifies listener ownership, and compares
the complete ordered walk through the terminal page against the original
fixture and the batch in append order for equal timestamps. It then verifies
the reference bundle at its original recovery point. The harness owns fixture
setup, append submission, and process lifecycle. The server owns writable
recovery and publication. [Native Linux test invocation](../../cmd/bog-verify/README.md#restore-drill)

## Registration and release

Core engineering maintains the command's role and measured direct imports
in the Linux architecture registry. The operator boundary check requires
the registration and rejects direct and transitive paths to the excluded
packages. Its fixtures exercise shared read-side dependencies and
violations with fully registered edges that pass the existing exact-import,
cycle, and reachability checks.
[Linux architecture registry](../../internal/archtest/architecture_test.go),
[Command root boundaries](0012-command-root-boundaries.md)

`verify/release` remains the authority for release parity. It uses fixture
inputs and the independent append-log oracle to qualify the pinned server
binary, including a live append and an exact walk after restart. Operator
results describe the selected image and endpoint at the checked query scope.
[Release verifier composition](0008-release-verifier-composition.md)
