# ADR 0049 — Retained runtime promotion

**Status** — Cycle 195 baseline measured on Linux amd64 and arm64.
The reviewed #108 Storage SHA and activation evidence are pending.
Checkpoint, history, and backup remain staged. #108 remains the release
gate until both native jobs pass strict cutover validation.

## Context

At `4972cc494302a03991789e79c5dc9035c8586780`, the Linux amd64 and arm64
inventories agree on 20 root-module production packages and 23 direct
imports. The server's four direct root-module imports, HTTP, readiness,
store, and WAL, are also its entire root-module dependency closure.
History imports checkpoint and store.
Checkpoint and backup each import store. The operator command already
imports backup for offline image verification. Checkpoint, history, and
backup remain staged at this revision. The newly landed
`internal/streampolicy` has zero direct root-module imports and has its
own staging declaration. `verify/stream-policy` remains independent.
[Measured system graph](../architecture/SYSTEM.md#declared-linux-production-import-graph),
[Stream-policy decision boundary](0050-stream-policy-decision-boundary.md)

## Storage handoff

Core Architecture owns the record and fitness changes. Storage owns
#108's recovery, publication, and command wiring, including #47's
shutdown cleanup and #15's startup filesystem diagnostics. Core Query
reviews the page source and capture lifecycle. Core Engineering names the
query execution and security integration owners within `bog`. Recheck
the active write leases at dispatch and at the Storage handoff. The Cycle
195 dispatch context puts the exclusive lease at `council/planning/**`
and records zero overlap with the architecture track's four paths.

Record the immutable reviewed Storage SHA and trace these paths before
changing roles.

| Boundary | Observed at the measured base | Evidence pending on the Storage SHA |
| --- | --- | --- |
| Query capture | `cmd/bog.run` opens the ordinary WAL into a `store.Store`. Its handler uses `httpapi.NewWithQueryLimits`. `wal.Log.Capture` pins the published prefix, exclusive byte end, and next ordinal under the WAL mutex. | Follow the serving page source to the synchronized acquisition of the generation and matching suffix. Verify capture release through the final admitted reader. |
| Readiness | `newReadyHandler` passes the configured path to `readiness.NewHandler`. | Follow the serving probe to an independent open relative to the pinned directory. Verify directory identity, probe cancellation before descriptor close, and readiness after path retargets in both directions. |
| Shutdown | The command cancels probes and drains HTTP before closing the WAL. A failed HTTP drain leaves the WAL locked until process exit. | Trace HTTP admission and drain through capture retirement and source release, then maintenance and export drain before WAL close. Verify startup error cleanup and lock retention on failed drain. |

[Command startup and shutdown](../../cmd/bog/main.go),
[Current paging and readiness call sites](../../cmd/bog/ready.go),
[WAL capture](../../internal/wal/capture_linux.go)

## Decision

Ship bounded retained history in the existing Linux process and pinned
directory. The WAL owner holds exclusive authority through startup,
acknowledged whole-batch publication, and shutdown. The command selects
and validates an ordinary WAL, a retained generation and its committed
suffix, or a verified restore before opening HTTP. Writable restore must
establish durable authority under the directory lock. Replacement durably
selects a validated successor before activation, fences writes on an
uncertain publication, and defers reclamation until the old references
drain.
[Runtime cutover](0015-retained-history-runtime-cutover.md),
[Pinned writable recovery](0005-pinned-writable-recovery-authority.md)

Keep composition in `cmd/bog`. The retained page source pins the selected
generation and exact durable, query-visible WAL suffix together under WAL
owner synchronization. HTTP keeps ownership of admission, paging,
delivery, and capture retirement and receives the source through
`httpapi.NewWithPageSource`. Readiness uses
`readiness.NewHandlerForDirectory` with a fresh, unlocked open relative to
the pinned WAL directory. Check directory identity at the serving call
site and confirm that readiness samples it after a configured-path
retarget. Cancel probes before closing their handle and keep a stalled
sample out of the drain path.

Stop admission and drain HTTP. Close the HTTP API to retire captures and
wait for admitted users and source releases. Drain admitted maintenance
and export work before closing storage and releasing the WAL lock.
Review startup error cleanup and verify that a failed drain retains the
lock until process exit.
[Constructor and ownership checks](0040-retained-runtime-constructor-boundary.md),
[Query lifecycle and shutdown](0039-query-delivery-and-retained-cutover.md)

Promote each library from staged to wired in the #108 wiring PR once
`cmd/bog` reaches it in both production inventories of the reviewed
Storage SHA and the corresponding activation evidence is available.

| Library | Activation evidence |
| --- | --- |
| `internal/checkpoint` | Validate and durably select the successor under pinned storage authority before live activation at the requested cutoff. |
| `internal/history` | Serve ordered checkpoint and suffix rows through the command-owned page source. Demonstrate a held cursor through replacement and acknowledged backfill, plus exact fresh and restarted walks and final pin drain. |
| `internal/backup` | Use backup APIs in server composition. Start writable on a verified restore, acknowledge a new append, and demonstrate exact HTTP parity after restart. Preserve `cmd/bog-verify`'s direct offline import. |

Keep idempotency and the stream-policy engine staged until their
separate production activation criteria are met.
Operator backup consumption leaves the backup library staged until
server reachability and its writable restore evidence are available.
The release producers continue to exercise a separate pinned process and
the query oracle stays independent. HTTP and readiness stay outside the
retained storage dependency tree. Checkpoint and backup consume their
own command-supplied recovery views, and history may use checkpoint
validation. Store remains a dependency of its consumers with no
root-module production dependencies of its own.
[Staging declarations](../../internal/archtest/staged_packages_test.go),
[Independent recovery views](0037-independent-recovery-view-consumers.md),
[Store dependency direction](0043-store-dependency-direction.md),
[Release evidence boundary](0021-retained-release-evidence-boundary.md)

## Costs

Validation and durable selection add startup and publication I/O.
Capturing and certifying exact snapshot walks adds work before cursor
issuance. Old generations and WAL suffixes stay charged while admitted
readers drain. Replay, append, capture, and staging reservations can defer
maintenance or refuse a whole batch when capacity is exhausted. Fencing
preserves recovery authority at the cost of write availability. The
command owns the sequencing and cleanup across package interfaces.

## Merge and qualification

Core Architecture and Storage review the exact direct edges and server
closure from both Linux inventories and update the fitness registry,
staging declarations, SYSTEM.md graph and table, and measured SHA in the
same wiring PR. Preserve the direct offline backup edge, the independent
verifier registrations, and the staged stream-policy engine. Before merge,
run `go vet ./internal/archtest`,
`go test -race -count=1 ./internal/archtest`, and
`GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./...`. The suite checks
production boundaries, cycles, orphan registrations, package reachability
and staging, documentation parity, and typed constructor references for
each Linux target. The constructor guard applies if either the registered
role or measured server reachability calls for it. Source review must
trace the serving adapter from the HTTP constructor to synchronized
generation and WAL suffix acquisition, and trace the probe descriptor
from the readiness constructor to the independent directory open.

Release Verification qualifies the same clean composed revision on
native Linux amd64 and arm64 using each job's SHA-256-pinned binary.
Require strict evidence at 196,608 and 393,216 rows under a 128 MiB cgroup
cap with swap disabled, including exact replacement and restart walks,
restored parity, readiness after directory retargets, and verified
cleanup. Reclamation evidence covers the held predecessor, admitted
readers after cursor retirement, final pin drain and artifact removal,
and intact successor storage. The release fixture exercises the
verifier's ability to detect a retained predecessor and a stalled reader.

The Linux inventories measure topology and the cross-build verifies
compilation. The baseline checks cover the measured graph and staging
update. At integration, hand the clean composed SHA to Release
Verification for the workflow's `revision` input. Record both native
verdicts against that SHA and each runner's binary checksum. The
composed SHA and both verdicts are pending. #108 remains the release gate.
[System fitness and registry ownership](../architecture/SYSTEM.md#fitness-and-registry-ownership),
[Native release evidence](../architecture/SYSTEM.md#release-evidence)
