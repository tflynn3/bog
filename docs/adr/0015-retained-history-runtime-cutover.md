# ADR 0015 — Retained-history runtime cutover

**Status** — Cycle 150 baseline measured at
`54347c836018789e0e3a95f7d76a02293169dfc5`. #108 carries the activation
contract developed under #53. On both Linux targets the server uses
ordinary WAL recovery and the in-memory query view. The immutable
Storage handoff, package promotion, and native qualification are pending.
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
| Checkpoint, catalog, and backup | The command supplies a pinned generation and matching committed suffix under WAL synchronization, with exact byte and ordinal bounds and independent readers. Checkpoint and backup validate their inputs and own readers they open. Validate and sync the successor before durably selecting it. Serialize selection with append publication so all commits beyond the cutoff survive. Existing captures and exports retain references through completion. Reclaim protocol-owned files only after durable selection and the last relevant pin drains. |
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

The command maps the WAL's logical offsets to the pinned backing file
and supplies read access to the exact whole frames in the committed suffix.
It owns the operation pin through cancellation and error cleanup. ADR 0035
specifies repeatable prefix-free advancement with versioned authority and
suffix integrity. Review that contract separately from verified v1 writable
startup and its bounded tail.
[Command-owned composition](0026-command-owned-retained-storage-composition.md),
[Tail-only recovery contract](0035-tail-only-retained-recovery-contract.md)

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

Promote each retained package when both Linux production inventories
show that `cmd/bog` reaches it and review confirms its activation criteria.
Remove the corresponding staging declaration in the same change.

At the Cycle 150 base, `go list -deps` under the architecture suite settings
reported the same direct root-module production edges and transitive
server reachability for Linux amd64 and arm64 with cgo disabled.

| Package | Direct root-module production imports | Reachable from `cmd/bog` |
| --- | --- | --- |
| `cmd/bog` | HTTP, readiness, store, WAL | Server root |
| `internal/checkpoint` | Store | No |
| `internal/history` | Checkpoint, store | No |
| `internal/backup` | Store | No |
| `internal/idempotency` | None | No |

The server closure contains only the command and its four direct
root-module dependencies. `cmd/bog-verify` directly imports backup,
client, and store. That read-only consumer keeps backup in the
production inventory while its server activation criteria remain open.
[Exact production graph](../architecture/SYSTEM.md#declared-linux-production-import-graph)

| Staged package | Owner | Activation criteria |
| --- | --- | --- |
| `internal/checkpoint` | `bog-storage` | #108 validates and publishes generations through the pinned WAL owner and proves durable replacement at the requested cutoff. |
| `internal/history` | `bog-storage` | #108 serves ordered checkpoint and tail rows through the command-owned page source and proves a held HTTP cursor survives replacement. |
| `internal/backup` | `bog-storage` | #108 wires backup into command composition and proves verified writable restore, an acknowledged append, and exact HTTP parity after restart. Read-only operator use already exists. |
| `internal/idempotency` | `bog-storage` | The production WAL atomically commits and recovers retry records with their batches. HTTP ingest covers concurrent same-key submissions, conflicting bodies, and retry after an ambiguous response and restart. |

A staging declaration must name an owner and state activation criteria.
The fitness check rejects missing or blank fields and declarations for
removed or reclassified packages. Exact imports and reachability verify
the role. The Linux inventory also checks the paging and storage authority
seams through dependencies outside the root module.
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

Architecture in the bog AgentSet owns this ADR, SYSTEM.md,
`internal/archtest/architecture_test.go`, and
`internal/archtest/staged_packages_test.go`. Storage owns the runtime
draft and its write lease. At the immutable Storage handoff, check the
four Architecture paths against the five held leases. Use the reviewed
Storage SHA to measure the composed Linux amd64 and arm64 inventories.
Record the direct edges, transitive server closure, and activation evidence
for every promotion, and update the registry, staging declarations,
diagram, and role table together.

Run `go vet ./internal/archtest`,
`go test -race -count=1 ./internal/archtest`, and
`GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./...` on the composed
tree and land runtime and governance in one reviewed PR.
[System fitness settings](../architecture/SYSTEM.md#fitness-and-registry-ownership)

The baseline governance update passed architecture vet and race checks,
including both Linux inventory checks, and the Linux amd64 cross-build.
The activation evidence must come from the composed revision.

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
