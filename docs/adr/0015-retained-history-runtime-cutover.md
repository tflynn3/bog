# ADR 0015 — Retained-history runtime cutover

**Status** — Cycle 157 baseline measured at
`ddc0496f00812f17ab913ddb284587f7f216a231`. #108 carries the activation
contract developed under #53. The supplied revision uses ordinary WAL
recovery and the in-memory query view on both Linux targets. The reviewed
Storage SHA, retained package promotion, and native qualification of the
composed revision are pending.
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

## Source review at the measured revision

| Seam | Observed behavior |
| --- | --- |
| Lock and startup | `wal.Open` locks the pinned directory inode before checking restore names or replaying the ordinary WAL. It rejects `bog.backup` and `bog.wal.tail`. `cmd/bog` calls `net.Listen` after recovery succeeds. |
| Append and capture | The WAL syncs bytes and publishes the commit marker before publishing whole batches to the in-memory store. `Log.Capture` pins an opened WAL identity and the latest published byte and ordinal bounds. The command's paging path uses `httpapi.NewWithQueryLimits` over the store. |
| Replacement and restore | The checkpoint and history packages supply the retained primitives. Only the offline operator consumes backup in a production command. Live generation selection, a generation pin paired with the WAL capture, and writable restore startup await the Storage handoff. |
| Readiness and drain | The command uses `readiness.NewHandler` with the configured path. After stopping probes, it drains HTTP before calling `Log.Close`. WAL close waits for the active writer before closing the directory. On HTTP drain timeout the process exits with the lock still held. Wiring the descriptor sampler, retained API close, and maintenance drain is part of the handoff. |

Review the composed command against the runtime interfaces above using
the same directory and owner from classification through close. Inspect
the append and capture synchronization, the durable selector and treatment
of commits after its cutoff, and the drain order for readers, maintenance,
and probes. The import graph establishes dependency direction. Native
probes exercise the ordering and recovery guarantees.
[Command lifecycle](../../cmd/bog/main.go),
[Startup and directory lock](../../internal/wal/recovery.go),
[Capture implementation](../../internal/wal/capture_linux.go),
[Readiness constructors](../../internal/readiness/wal_linux.go)

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

The Cycle 157 baseline inventories agree on these direct root-module
production edges and transitive server reachability. Both use
`go list -deps` under the architecture suite settings.

| Package | Direct root-module production imports | Measured role |
| --- | --- | --- |
| `cmd/bog` | `internal/httpapi`, `internal/readiness`, `internal/store`, `internal/wal` | Server root |
| `internal/httpapi` | `internal/store`, `internal/wal` | Wired |
| `internal/readiness` | `internal/store`, `internal/wal` | Wired |
| `internal/wal` | `internal/store` | Wired |
| `internal/store` | None | Wired |
| `internal/checkpoint` | `internal/store` | Staged |
| `internal/history` | `internal/checkpoint`, `internal/store` | Staged |
| `internal/backup` | `internal/store` | Staged |
| `internal/idempotency` | None | Staged |
| `cmd/bog-verify` | `internal/backup`, `internal/client`, `internal/store` | Operator command |

The server closure contains the command and its four wired libraries.
The operator's read-only backup edge is outside that closure. The
observed server activation for checkpoint, history, and backup is
pending, so their roles and declarations remain staged.
[Exact production graph](../architecture/SYSTEM.md#declared-linux-production-import-graph)

| Staged package | Owner | Activation criteria | Evidence required from the composed revision |
| --- | --- | --- | --- |
| `internal/checkpoint` | `bog-storage` | Validate and publish generations under the pinned WAL owner's synchronization. | Show durable publication of a newer generation at the exact whole-batch cutoff and recovery of later acknowledged commits. Use the live capacity and capacity-control scope probes. |
| `internal/history` | `bog-storage` | Serve ordered checkpoint and tail rows through the injected command-owned page source. | Show an exact held walk across replacement and acknowledged backfill, exact fresh and restarted walks, and retirement and pin drain. Use fixed-budget backfill and live capacity evidence. |
| `internal/backup` | `bog-storage` | Validate a restored image in command composition and establish writable authority before listening. | Use restored parity to observe HTTP 204, verify the exact augmented history after restart, and recover an acknowledged batch after an immediate crash. |
| `internal/idempotency` | `bog-storage` | Atomically commit and recover retry records with their batches. | Exercise concurrent same-key submissions, conflicting bodies, and retry after an ambiguous response and restart. This activation remains staged beyond #108. |

A staging declaration must name an owner and state activation criteria.
The fitness check rejects missing or blank fields and declarations for
removed or reclassified packages. Exact imports and reachability verify
the role. The Linux inventory checks paging, storage authority, server
and operator verification, and release evidence and oracle independence
through production dependencies outside the root module as well.
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

Architecture in the existing bog AgentSet owns this ADR, SYSTEM.md,
`internal/archtest/architecture_test.go`, and
`internal/archtest/staged_packages_test.go`. Storage owns the runtime
draft and its write lease. Release Verification owns the native gate.
Keep #104 behind the HTTP read lease and retained paging handoff, and
#81 behind #108's writable recovery contract. Cycle 157 reserves the
ordered runtime and governance handoff plus an independent pinned-binary
query replay track. The listener work can proceed independently.

The supplied snapshot lists three held leases and omits the reported
#108 and pager scopes. PM reports five running tracks and the supplied
backlog export identifies #81 and #106. The four Architecture paths and
the listener write set are disjoint and clear the supplied snapshot.
Live lease scopes and a reviewed Storage SHA were unavailable for this
measurement. Preserve assignments and recheck the live scopes at dispatch
and again at the immutable Storage handoff. Measure the composed Linux
amd64 and arm64 inventories at the reviewed SHA.
Record the direct edges, transitive server closure, and activation evidence
for every promotion, and update the registry, staging declarations,
diagram, and role table together.

Run `go vet ./internal/archtest`,
`go test -race -count=1 ./internal/archtest`, and
`GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./...` on the composed
tree and land runtime and governance in one reviewed PR.
[System fitness settings](../architecture/SYSTEM.md#fitness-and-registry-ownership)

The supplied QA artifact at `79f0fe8adf1a3642c56990ad8791534fde1c426a`
records a harness failure. Native verdicts for the measured revision
are pending. The activation evidence requires the composed revision.

The native matrix qualifies the same clean revision on amd64 and arm64.
Each runner uses its own SHA-256-pinned binary for all producers. Require
exact HTTP order through verified restore, acknowledged append, restart,
and generation replacement with a held cursor and concurrent backfill.
On each runner use both fixture scales (196,608 and 393,216 seed rows)
under a 128 MiB cgroup cap with swap disabled. Require zero cgroup OOM
kills, exact publication and pin-drain observations, and cleanup evidence.
Verify readiness and ingest against the active WAL filesystem after path
retargets. Require a passing capacity-control scope probe on both runners
and both strict native verdicts. Require both seeded query replay
subtests to pass against each runner's pinned binary and record the
matching revision and binary digest.
[System release evidence](../architecture/SYSTEM.md#release-evidence),
[Capacity control qualification](0034-capacity-control-release-qualification.md),
[Release composition](0008-release-verifier-composition.md)
