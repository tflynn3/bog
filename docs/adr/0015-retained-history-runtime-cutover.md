# ADR 0015 — Retained-history runtime cutover

**Status** — Proposed. The #53 integration review awaits Storage’s
composed revision. The baseline at
`63626c1bddc3664327d49ec70fb64e84e4943024` passes the portable architecture
suite for Linux amd64 and arm64, the race run, vet on `internal/archtest`,
and the Linux amd64 cross-build. Both native qualification verdicts are
pending the composed revision.
[#53](https://github.com/tflynn3/bog/issues/53)

## Runtime interfaces

The measured command calls `wal.Open` and `httpapi.NewWithQueryLimits`.
Startup rejects `bog.backup` and `bog.wal.tail`, and the live capacity
verifier cannot reach `/_capacity/compaction`. The paging constructor
and read-only image APIs are available for the command cutover.
[Command startup](../../cmd/bog/main.go), [HTTP construction](../../cmd/bog/ready.go)

Keep one Linux process, one HTTP listener, one WAL owner, and one pinned
local data directory. The owner takes the directory lock before
classifying recovery inputs and holds it until admitted storage work
drains. Startup validates and replays the ordinary WAL, or validates the
retained generation and replays its committed suffix. A verified restore
must establish durable writable authority before listening. Restart
recovers to the current committed end selected by that authority. The
original backup metadata continues to identify the verified restore point.
Fail startup on ambiguous authority or corrupt committed data.
[Storage composition](0007-runtime-storage-composition.md),
[Pinned writable recovery authority](0005-pinned-writable-recovery-authority.md)

| Boundary | Contract for #53 |
| --- | --- |
| HTTP and command adapter | Inject a command-local `httpapi.PageSource` with `httpapi.NewWithPageSource`. The source implements `Page(context.Context, PageRequest, *store.QueryWork) (PageResult, error)` and idempotent `Release(capture string) error`. Repeat the ordered rows, positions, and exhaustion decision for the same captured source request. HTTP owns query admission, the bounded replay plan, cursor authentication and expiry, and complete response framing. |
| WAL owner and retained view | Serialize durable append visibility, capture selection, and generation activation. Bind the generation and its pin to the matching WAL identity or epoch, covered whole-frame boundary, first uncovered ordinal, and exact durable query-visible end. Merge checkpoint and suffix by UTC timestamp and append ordinal. Publish whole committed batches before HTTP 204. Confirmed rollback publishes no rows. Uncertain outcomes fence the owner pending recovery. |
| Checkpoint, catalog, and backup | Supply pinned readers, bounds, and reservations through composition. Sync immutable checkpoint files and authority before validating and installing the successor and trimming the live tail. Existing captures and exports hold their generation and tail references through completion. Reclaim only protocol-owned files unreachable from durable authority and all live pins. |
| Capacity control | Mount `/_capacity/compaction` only with `BOG_CAPACITY_CONTROL=1` and a loopback listener. POST starts asynchronous replacement at the requested committed row cutoff. GET reports progress. Report `running` with the published generation and `complete` only after the newer generation is durable and installed at the exact cutoff. |
| Readiness and shutdown | `/ready` observes disk headroom and terminal write outcomes. #60 follows the command lease and supplies a separately owned descriptor to the active WAL directory through `readiness.NewHandlerForDirectory`. On shutdown stop admission and maintenance scheduling, retire captures, drain HTTP, append, publication, and export work, then close storage. A drain timeout leaves the lock held until process exit. |

HTTP certifies a snapshot walk through exhaustion under the configured
per-request work and response byte caps before issuing a cursor. The
same budgets apply when the client lowers its resume limit. The earlier
source or HTTP deadline bars new resumes, and an authenticated cursor
for an expired capture receives 410. Admitted operations retain their
references. [Fixed-budget continuation contract](0006-fixed-budget-query-continuations.md),
[Live capacity control and metrics](../../bench/retained-history/README.md)

## Trade-offs

Durable whole-batch acknowledgement and synchronized activation preserve
recoverable history at the cost of sync latency and write availability
on uncertainty. Certifying the walk spends work before the first cursor.
Pins preserve exact membership and order across appends and replacement
and delay reclamation. Bound replay and append reservations, live
captures, pinned generations, and staging. Capacity refusal protects
already admitted work and can defer maintenance or reject whole batches.
[Generation pins](0002-bounded-history-generation-pins.md),
[Resource admission](0010-retained-resource-admission.md)

A full v1 WAL keeps the exact prefix readable for checkpoint and export
digests. Validation, prefix hashing, and staging consume I/O and
temporary disk headroom. A tail-only v1 restore retains its verified
checkpoint boundary and can append only within its tail budget. Refuse
checkpoint advancement or v1 export when it requires unavailable prefix
bytes. [V1 prefix digest](0005-pinned-writable-recovery-authority.md#keep-the-v1-prefix-digest-meaningful),
[Backup staging recovery](0009-backup-staging-recovery.md)

## Staging and command boundaries

Promote each retained package only when both Linux production inventories
show that `cmd/bog` reaches it. Remove the corresponding staging
declaration in the same change. As measured, `cmd/bog-verify` directly
imports `internal/backup`, `internal/client`, and `internal/store`. Both
server inventories leave checkpoint, history, backup, and idempotency staged.

| Staged package | Owner | Activation criteria |
| --- | --- | --- |
| `internal/checkpoint` | `bog-storage` | #53 validates and publishes generations through the pinned WAL owner and proves durable replacement at the requested cutoff. |
| `internal/history` | `bog-storage` | #53 serves ordered checkpoint and tail rows through the command-owned page source and proves a held HTTP cursor survives replacement. |
| `internal/backup` | `bog-storage` | Server composition reaches backup and proves verified writable restore, an acknowledged append, and exact HTTP parity after restart. Read-only operator use already exists. |
| `internal/idempotency` | `bog-storage` | The production WAL atomically commits and recovers retry records with their batches. HTTP ingest covers concurrent same-key submissions, conflicting bodies, and retry after an ambiguous response and restart. |

A staging declaration must name an owner and state activation criteria.
The fitness check rejects missing or blank fields and declarations for
removed or reclassified packages. Review evaluates the criteria and
test evidence. Exact imports and reachability verify the role.
[Staging declaration check](../../internal/archtest/staged_packages_test.go),
[Command root boundaries](0012-command-root-boundaries.md),
[Standalone production consumers](0014-standalone-package-consumers.md)

HTTP and readiness stay outside the retained storage dependency tree.
Checkpoint, history, and backup cannot import the WAL owner, directly
or transitively. The operator and release producers cannot reach the WAL
owner, server HTTP, or readiness in production. The server cannot reach
the Go HTTP client or independent query oracle. The operator compares with
an immutable image. Native release parity compares with an independent
append ledger and exercises the separate pinned server process.
[Operator verification](0011-operator-verification-boundary.md),
[Release evidence process boundary](0013-release-evidence-process-boundary.md)

## Integration and qualification

Core engineering owns the registry and system record. Storage owns the
runtime draft and hands it off for read review. Recheck leases at dispatch
and handoff. Measure direct root-module production imports and transitive
server reachability on the composed tree with the architecture suite's
Linux amd64 and arm64 settings. Update measured edges, package roles,
and the system diagram together. Run `go test -count=1 ./internal/archtest`
and `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./...` on the
composed tree and land runtime and governance in one reviewed PR. The
companion owns `SYSTEM.md`, this ADR, `architecture_test.go`, and
`staged_packages_test.go`.
[System fitness settings](../architecture/SYSTEM.md#fitness-and-registry-ownership)

Both native verdicts are required for the same clean revision on amd64
and arm64. Each runner uses its own SHA-256-pinned binary for all
producers. Require exact HTTP order through verified restore, acknowledged
append, restart, and generation replacement with a held cursor and concurrent
backfill. Exhaust issued cursors at their original work budgets.
Run both 128 MiB capacity fixtures, capture exact publication and pin-drain
observations, and retain cleanup evidence. Verify readiness and ingest
against the active WAL filesystem after path retargets.
[System release evidence](../architecture/SYSTEM.md#release-evidence),
[Release composition](0008-release-verifier-composition.md)
