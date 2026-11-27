# ADR 0015 — Retained-history runtime cutover

**Status** — #108 is the Cycle 148 activation handoff for the runtime
contract developed under #53. At base
`0b940e742d589d3b2e4f3154803dd8c7e5c462e3`, the Linux amd64 and arm64
production inventories agree. `cmd/bog` directly imports HTTP,
readiness, store, and WAL and reaches those same four root-module
libraries. It serves ordinary WAL recovery and an in-memory query view.
Storage composition, package promotion, and native qualification are
pending.
[#108](https://github.com/tflynn3/bog/issues/108),
[#53](https://github.com/tflynn3/bog/issues/53)

## Runtime interfaces

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

| Boundary | Contract for #108 |
| --- | --- |
| HTTP and command adapter | Inject a command-local `httpapi.PageSource` with `httpapi.NewWithPageSource`. The source implements `Page(context.Context, PageRequest, *store.QueryWork) (PageResult, error)` and idempotent `Release(capture string) error`. Repeat the ordered rows, positions, and exhaustion decision for the same captured source request. HTTP owns query admission, the bounded replay plan, cursor authentication and expiry, and complete response framing. |
| WAL owner and retained view | Serialize durable append visibility, capture selection, and generation activation. Bind the generation and its pin to the matching WAL identity or epoch, covered whole-frame boundary, first uncovered ordinal, and exact durable query-visible end. Merge checkpoint and suffix by UTC timestamp and append ordinal. Publish whole committed batches before HTTP 204. Confirmed rollback publishes no rows. Uncertain outcomes fence the owner pending recovery. |
| Checkpoint, catalog, and backup | Supply pinned readers, bounds, and reservations through composition. Sync immutable checkpoint files and authority before validating and installing the successor and trimming the live tail. Existing captures and exports hold their generation and tail references through completion. Reclaim only protocol-owned files unreachable from durable authority and all live pins. |
| Capacity control | Mount `/_capacity/compaction` only with `BOG_CAPACITY_CONTROL=1` and a loopback listener. POST starts asynchronous replacement at the requested committed row cutoff. GET reports progress. Report `running` with the published generation and `complete` only after the newer generation is durable and installed at the exact cutoff. Retirement uses the HTTP API instance that issued the cursor. |
| Readiness and shutdown | `/ready` observes disk headroom and terminal write outcomes. Supply a separately owned descriptor to the active WAL directory through `readiness.NewHandlerForDirectory`. Stop admission and maintenance scheduling, drain HTTP, and close the HTTP API before closing the WAL owner. Keep the lock until admitted append, publication, and export work drains. A drain timeout leaves the lock held until process exit. |

HTTP certifies a snapshot walk through exhaustion under the configured
per-request work and response byte caps before issuing a cursor. The
same budgets apply when the client lowers its resume limit. The earlier
source or HTTP deadline bars new resumes. A terminal page's input token
replays that page while its capture is live. Explicit retirement bars
new resumes, as does expiry. Authenticated resumes then receive 410.
Admitted operations retain their references through completion.
[Fixed-budget continuation contract](0006-fixed-budget-query-continuations.md),
[Retained cursor retirement](0031-retained-cursor-retirement.md),
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
declaration in the same change. As measured, `cmd/bog-verify` reaches
`internal/backup`, `internal/client`, and `internal/store` directly.
The server reaches none of checkpoint, history, backup, or idempotency,
so their staging declarations remain active.

| Staged package | Owner | Activation criteria |
| --- | --- | --- |
| `internal/checkpoint` | `bog-storage` | #108 validates and publishes generations through the pinned WAL owner and proves durable replacement at the requested cutoff. |
| `internal/history` | `bog-storage` | #108 serves ordered checkpoint and tail rows through the command-owned page source and proves a held HTTP cursor survives replacement. |
| `internal/backup` | `bog-storage` | #108 wires backup into command composition and proves verified writable restore, an acknowledged append, and exact HTTP parity after restart. Read-only operator use already exists. |
| `internal/idempotency` | `bog-storage` | The production WAL atomically commits and recovers retry records with their batches. HTTP ingest covers concurrent same-key submissions, conflicting bodies, and retry after an ambiguous response and restart. |

A staging declaration must name an owner and state activation criteria.
The fitness check rejects missing or blank fields and declarations for
removed or reclassified packages. Review evaluates the criteria and
test evidence. Exact imports and reachability verify the role.
[Staging declaration check](../../internal/archtest/staged_packages_test.go),
[Command root boundaries](0012-command-root-boundaries.md),
[Standalone production consumers](0014-standalone-package-consumers.md)

HTTP and readiness stay outside the retained storage dependency tree.
Only `cmd/bog` can reach both WAL and any of checkpoint, history, or
backup in production. These libraries and WAL have no import path in
either direction, including through a relay. Production imports remain
acyclic. The operator and release producers cannot reach WAL, server
HTTP, or readiness. Release producers also cannot reach checkpoint,
history, or backup in production. The server cannot reach the Go HTTP
client or independent query oracle. The operator compares with an
immutable image. Native release parity compares with an independent
append ledger and exercises the separate pinned server process.
[Command-owned retained storage](0026-command-owned-retained-storage-composition.md),
[Operator verification](0011-operator-verification-boundary.md),
[Retained release evidence](0021-retained-release-evidence-boundary.md)

## Integration and qualification

Architecture owns the registry and system record. Storage owns the
runtime draft. Both are in the bog AgentSet and work under disjoint
write leases. Recheck leases at dispatch and at the immutable Storage
draft handoff. Measure direct root-module production imports and
transitive server reachability on the composed tree with the
architecture suite's Linux amd64 and arm64 settings. Update the measured
edges, package roles, staging declarations, and system diagram together.
Run `go test -count=1 ./internal/archtest` and
`GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./...` on the composed
tree and land runtime and governance in one reviewed PR.
[System fitness settings](../architecture/SYSTEM.md#fitness-and-registry-ownership)

The native matrix qualifies the same clean revision on amd64 and arm64.
Each runner uses its own SHA-256-pinned binary for all producers. Require
exact HTTP order through verified restore, acknowledged append, restart,
and generation replacement with a held cursor and concurrent backfill.
On each runner use both fixture scales (196,608 and 393,216 seed rows)
under a 128 MiB cgroup cap with swap disabled. Require zero cgroup OOM
kills, exact publication and pin-drain observations, and cleanup evidence.
Verify readiness and ingest against the active WAL filesystem after path
retargets. Require a passing capacity-control scope probe on both runners
and both strict native verdicts.
[System release evidence](../architecture/SYSTEM.md#release-evidence),
[Capacity control qualification](0034-capacity-control-release-qualification.md),
[Release composition](0008-release-verifier-composition.md)
