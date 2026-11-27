# ADR 0049 — Retained runtime promotion

**Status** — Promotion criteria recorded for Cycle 191. The reviewed #108
Storage revision, package promotions, and native qualification are pending.

## Context

At `26535d86d556b9700bfb8a8b6c2c8379dcbfc094`, the Linux amd64 and arm64
inventories agree on 19 root-module production packages and 23 direct
imports. The server's direct imports are HTTP, readiness, store, and WAL.
It replays the ordinary WAL into memory and uses the legacy paging and
path-based readiness constructors. History imports checkpoint and store.
Checkpoint and backup each import store. The operator command already
imports backup for offline image verification. Checkpoint, history, and
backup remain staged at this revision.
[Measured system graph](../architecture/SYSTEM.md#declared-linux-production-import-graph)

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
the pinned WAL directory. The command cancels probes before closing the
descriptor and drains admitted HTTP and storage users before releasing
the WAL lock.
[Constructor and ownership checks](0040-retained-runtime-constructor-boundary.md),
[Query lifecycle and shutdown](0039-query-delivery-and-retained-cutover.md)

Promote each library from staged to wired in the #108 wiring PR once
`cmd/bog` reaches it in both production inventories of the reviewed
Storage SHA and the corresponding activation evidence is available.

| Library | Activation evidence |
| --- | --- |
| `internal/checkpoint` | Validate and publish the successor under pinned storage authority, with durable selection and live replacement at the requested cutoff. |
| `internal/history` | Serve ordered checkpoint and suffix rows through the command-owned page source. Demonstrate a held cursor through replacement and acknowledged backfill, plus exact fresh and restarted walks and final pin drain. |
| `internal/backup` | Use backup APIs in server composition. Start writable on a verified restore, acknowledge a new append, and demonstrate exact HTTP parity after restart. Preserve `cmd/bog-verify`'s direct offline import. |

Keep idempotency staged until its separate keyed-ingest criteria are met.
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
same PR. Run the architecture suite on the combined revision before
merge. It checks production boundaries, cycles, package reachability and
staging, documentation parity, and typed constructor references for each
Linux target. The constructor guard applies if either the registered role
or measured server reachability calls for it. Review the source and
descriptor passed at the serving call sites as part of the handoff.

Release Verification qualifies the same clean composed revision on
native Linux amd64 and arm64 using each job's SHA-256-pinned binary.
Require strict evidence at 196,608 and 393,216 rows under a 128 MiB cgroup
cap with swap disabled, including exact replacement and restart walks,
restored parity, readiness after directory retargets, and verified
cleanup. The Linux cross-build verifies compilation. The two native
verdicts establish qualification.
[System fitness and registry ownership](../architecture/SYSTEM.md#fitness-and-registry-ownership),
[Native release evidence](../architecture/SYSTEM.md#release-evidence)
