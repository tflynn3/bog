# Bog system architecture

One Linux process, one HTTP listener, one WAL owner, and one pinned local data
directory form Bog's deployment and consistency unit. `cmd/bog` acquires the
exclusive directory lock, recovers the committed state, and then opens the
listener. The owner holds the lock through HTTP drain. If drain fails, the
process exits with the WAL still open so handlers cannot race another owner.

The WAL owner controls append order and when a complete batch becomes durable
and query-visible. The service requires a trusted, service-owned local
filesystem with advisory locking, atomic rename, file and directory sync, and
powersafe overwrite. Keep each data directory under a single writer. The
listener exposes ingest and query without authentication. Deploy it on a
trusted network or behind an authenticated access layer.

## Observed Linux production imports

The diagram shows direct root-module production imports measured with
`GOOS=linux GOARCH=amd64 GOAMD64=v1 CGO_ENABLED=0`. Solid edges apply to
the pinned base `9bf89aa4c5a173833bd8fb70d088ac0a257e20a8`. The dotted
edge is the only change measured with the #59 and #61 sources overlaid
on that base. Arrows point from importer to dependency. The package
registry also includes Linux test-only packages.

```mermaid
flowchart TD
    subgraph Runtime["Wired Linux server (legacy WAL recovery)"]
        Cmd["cmd/bog"]
        HTTP["internal/httpapi"]
        Ready["internal/readiness"]
        WAL["internal/wal"]
        Store["internal/store"]
        Cmd --> HTTP
        Cmd --> Ready
        Cmd --> WAL
        Cmd --> Store
        HTTP --> WAL
        HTTP --> Store
        Ready --> WAL
        Ready --> Store
        WAL --> Store
    end
    subgraph Staged["Staged libraries"]
        Checkpoint["internal/checkpoint"]
        History["internal/history"]
        Backup["internal/backup"]
        Idempotency["internal/idempotency"]
        History --> Checkpoint
    end
    Checkpoint --> Store
    History --> Store
    Backup --> Store
    subgraph Evidence["Linux release evidence commands"]
        Admission["verify/release"]
        Durability["verify/linux"]
        Capacity["bench/retained-history"]
    end
    Client["internal/client (standalone)"]
    Model["verify/query-model (independent oracle)"]
    Client --> Store
    Admission -.-> Client
```

- **Ingest** validates complete NDJSON batches before appending. The WAL syncs
  frames, syncs and renames the temporary commit marker over `bog.commit`,
  syncs the directory, and publishes whole batches before HTTP returns 204.
  A confirmed rollback returns 503 and publishes no rows. An uncertain commit
  or rollback poisons the writer and terminates the process on the served
  path. Recovery validates the committed prefix and discards uncommitted bytes.
- **Queries** match a case-sensitive literal within a half-open time range.
  UTC timestamp and append ordinal determine order, including ties,
  duplicates, and backfill. The base serves a replayed in-memory store.
  Admission, scan work, and response bytes have independent limits.
- **Health** reports listener liveness at `GET /healthz` after recovery.
  `GET /ready` combines disk headroom sampled from the configured directory
  path and the terminal WAL group outcome. A rollback reports `wal_rollback`
  until a later durable outcome. An uncertain outcome reports
  `recovery_required` while the process serves. #60 plans to bind disk samples
  to the active WAL filesystem through the descriptor handoff in
  [ADR 0005](../adr/0005-pinned-writable-recovery-authority.md#readiness-descriptor-handoff-60).
- **Recovery** at this revision opens the legacy WAL layout. Under the
  directory lock, the command rejects a directory containing `bog.backup`
  or `bog.wal.tail`. The offline backup reader validates restored images.
  [ADR 0003](../adr/0003-wal-capture-and-restored-image-startup.md) records
  the startup guard and capture design.

## Integration status

| Domain change | Review and next integration boundary |
| --- | --- |
| #53 with #59 | The #59 candidate `b273daf97c719a7af33b9cfd747a26444ba90151` adds the consumer-owned `PageSource` and keeps the server's direct imports and reachable packages unchanged. The command still opens the legacy WAL and serves an in-memory view. #53 must supply the retained implementation, synchronized capture, and writable recovery. |
| #61 verifier composition | Landed on main at `3a28f26d322a777a423692f241ac962adf757e8d`. The measured edge `verify/release → internal/client` serves budget walks and restored-parity probes. Restored parity derives its expected rows directly from the fixture's append order. |
| Crash-safe backup staging | The storage candidate `9296e30409e01bab3d21de9a872618005a3010d5` adds a destination publication coordinator. Integration must wire it into export and restore and prove that retries reclaim interrupted stages while preserving durable destinations and unrelated entries. |

## Retained-history handoff

At cutover, the retained adapter lives in `cmd/bog` and implements HTTP's
consumer-owned `PageSource`. HTTP owns admission, cursor authentication and
lifetime, and NDJSON framing. The WAL owner selects and activates writable
recovery state for ordinary WAL directories, retained generations, and
verified restores. The adapter reads a coordinated capture from the owner and
history catalog. Checkpoint validates and durably publishes immutable
generations. History owns generation pins, segment scans, and reclamation.
Store owns decoded tail rows and their order. Backup consumes a pinned
storage view. Checkpoint, history, and backup receive readers, bounds, and
pins through composition and must have no production import path to the WAL
owner. The packages providing storage, including the staged retry library,
stay independent of HTTP, readiness, and the Go HTTP client.

The coordinated capture binds the validated generation and exact durable,
query-visible tail under the same owner. A cursor reads the same view across
generation replacement. A fresh walk sees later committed appends. Before
issuing its first cursor, HTTP certifies that the pinned walk can reach
exhaustion at the same per-request work budget and response byte cap,
including when a continuation lowers its row limit. The cursor uses the
earlier of HTTP's maximum lifetime and any source expiry. An authenticated
cursor whose capture has expired receives 410, and admitted operations
retain their references through completion.

Writable restore establishes checkpoint and tail recovery authority before
listening, and restart selects that writable authority. The opt-in loopback
capacity control drives replacement through the same owner.

ADRs 0006 and 0007 are the interface contract for `bog` and `bog-storage`.
The #59 candidate meters source work on each call and replays the original
source anchor and limit. The current history scanner exposes a matching-row
visitor, so the retained implementation needs a metering seam for seeks,
nonmatching rows, and exhaustion. ADR 0005 governs the lifecycle handoff.
The owner keeps the directory pinned through
activation, publication, and storage drain. `cmd/bog` owns the separate
readiness probe handle in #60, while readiness borrows it to sample disk
headroom and observes WAL outcomes. Capture, paging, and export references
drain before storage closes. A drain deadline leaves the owner locked until
process exit.

[ADR 0004](../adr/0004-retained-history-cutover.md)
[ADR 0005](../adr/0005-pinned-writable-recovery-authority.md)
[ADR 0006](../adr/0006-fixed-budget-query-continuations.md)
[ADR 0007](../adr/0007-runtime-storage-composition.md)

## Package registry

Wired libraries are reachable from `bog/cmd/bog` in the Linux production
graph. Staged libraries await runtime wiring. Standalone libraries have
independent consumers. Test-only packages have Linux test files and no Linux
production files. Verifier commands and support live outside the server
dependency tree.

| Package or entry point | Role | Responsibility |
| --- | --- | --- |
| `cmd/bog` | Server command | Recovery, listener, and lifecycle |
| `internal/httpapi` | Wired | HTTP validation, admission, cursors, and response framing |
| `internal/readiness` | Wired | Disk headroom and write outcome observation |
| `internal/wal` | Wired | Directory locking, commit, replay, and WAL capture |
| `internal/store` | Wired | Row decoding, publication, and ordered queries |
| `internal/checkpoint` | Staged | Legacy WAL scans, checkpoint format, and publication |
| `internal/history` | Staged | Generation validation, scans, pins, and reclamation |
| `internal/backup` | Staged | Pinned export, verification, restore, and offline reads |
| `internal/idempotency` | Intentionally staged | bog-storage owns the durable keyed-ingest backend. Activate with the HTTP ingest wiring once the production WAL commits retry records and their batches together and passes the restart retry contract. |
| `internal/client` | Standalone | Production HTTP client, complete-page framing, opaque cursor handling, and ingest outcomes |
| `internal/ingest` | Test-only | Reference decoder and fuzz tests |
| `internal/archtest` | Test-only | Linux dependency fitness suite |
| `verify/fuzz` | Test-only | Linux backup and history corruption harness with bounded inputs (#58) |
| `verify/release` | Verifier command | Admission and fixed-budget paging probes, report validation, and the restored-parity test entry point. #61 adds the production-client dependency on main. |
| `verify/linux` | Verifier command | Linux crash recovery, ENOSPC, and readiness across path retargets (#3) |
| `bench/retained-history` | Verifier command | Legacy baseline and live capacity producer (#32) |
| `verify/query-model` | Verifier support | Independent append-log oracle for ordered membership at a captured cutoff |
| `bench/segment-layout` | Nested-module benchmark | Checkpoint layout experiment outside the root-module graph |
| `.github/workflows/linux-release.yml` | Release workflow | Pins the candidate, runs the producers, and publishes evidence |

The activation criteria for keyed retries include concurrent same-key
submissions, conflicting bodies, and retries after an ambiguous response and
process restart. [Ingest retry contract](../ingest-retries.md)

## Ranked quality attributes

| Rank | Property | Trade-off |
| --- | --- | --- |
| 1 | Durability | Acknowledge whole batches after durable commit and query publication. At cutover, validate and sync new recovery authority before use. Retried backup publication must preserve complete destinations and reclaim only proven protocol-owned staging. |
| 2 | Exact queries | Preserve append-ordered ties, duplicates, and captured membership. #59 certifies fixed-budget walks before issuing a cursor. Cross-generation and post-restore parity require the #53 adapter and the fixture-derived release verdict. |
| 3 | Bounded resources | Limit concurrent scans, request work, and response bytes. At cutover, also bound retained captures and the writable tail. Pins defer reclamation through expiry and operation drain. V1 compatibility retains a readable WAL prefix. |
| 4 | Recovery time | The pinned base replays the committed WAL before listening. Cutover trades checkpoint validation before startup for bounded tail replay. |
| 5 | Throughput | Share syncs across admitted writes and prune provably irrelevant scan work. Hashing the v1 prefix for publication and export consumes disk bandwidth. |

## Release evidence

#53 gates retained-history cutover. Full qualification uses the #53 and
#59 runtime, #61 paging probes, and #60 readiness handoff. The workflow
builds one clean Go 1.27.0 candidate on Ubuntu 24.04 amd64 from the supplied
full SHA and supplies the same binary to four evidence sources. Every
report must match the revision, binary SHA-256, and `linux-release-v1`
fixture.
[Linux release workflow](../../.github/workflows/linux-release.yml)

| Source | Report | Required evidence |
| --- | --- | --- |
| `verify/release` | `admission.json` | Saturated query slot, concurrent ingest, cursor resume, and clean work-budget rejection. Issuing budgets must complete tied-row and sparse, byte-trimmed walks, including a smaller resume limit. |
| `verify/linux` | `durability.json` | SIGKILL recovery, committed-prefix checks, and observed ENOSPC on a disposable local filesystem. Retarget the configured path in both directions and require readiness and ingest to follow the active WAL filesystem. |
| `bench/retained-history` | `capacity.json` | Confirm a newer durable generation at the exact requested cutoff, resume a held cursor across replacement during acknowledged backfill, and verify fresh ordered history including after restart. Run at both fixture sizes with bounded cgroup and process cleanup. |
| `verify/release` / `TestReleaseRestoredParity` | `parity.json` | Start the shipped binary on a verified restore, compare fresh HTTP walks, observe 204 for a new append, and recover the exact augmented history after restart. |

The `linux-release-evidence` artifact contains build identity, fixture and
runner details, reports, exit statuses, and logs. The Linux verifier race
step also runs the bounded backup and history corruption corpus in
`verify/fuzz` and records it in `verifier-race.log`. The legacy capacity
run records a baseline.
[Release verifier contract](../../verify/release/README.md)

The #61 implementation uses `internal/client` for complete NDJSON pages and opaque
continuation handling in the fixed-budget probes. The verifier derives
expected membership, order, and snapshot cutoffs from the fixture and its
acknowledged appends. Low-level transport probes still supply evidence for
TCP backpressure and wire-level rejection. [ADR 0008](../adr/0008-release-verifier-composition.md)

## Fitness and registry ownership

`bog` owns the architecture record, fitness registry, #59 HTTP paging, and
#61 production-client verification. `bog-storage` owns #53 runtime wiring,
recovery, and backup publication. #60 follows the #53 command lease, #63
follows cutover, and #18 remains queued.

The pinned checkout passed architecture vet, the architecture race suite,
and the Linux cross-build. A Go overlay with the #59 and #61 sources also
passed the Linux cross-build and produced the dotted edge above. The
architecture race suite passed against that inventory with the dotted edge
added to the overlay registry. Register that exact edge when integrating
#61 into the checkout. The #53 runtime
commit is still required to measure the complete graph, revise the deployment
and consistency boundary, and promote any newly reachable storage packages.
[Fitness registry](../../internal/archtest/architecture_test.go)

The Architecture workflow runs `go test -count=1 ./internal/archtest`.
The suite lists packages with `GOOS=linux GOARCH=amd64 GOAMD64=v1
CGO_ENABLED=0` and `GOENV=off GOFLAGS= GO111MODULE=on GOWORK=off`. It checks
production imports within the root module and follows all listed production
dependencies for reachability and boundary checks. Host-only files, test
imports, and nested modules are outside the production import registry.
Package loading errors, cycles, unregistered packages, incorrect roles,
and forbidden, duplicate, or stale registered imports fail the suite.
`verify/fuzz` must contain Linux test files only. Storage cannot reach HTTP,
readiness, or the Go HTTP client. The paging check rejects any production
path from HTTP to checkpoint, history, or backup. Its fixtures exercise
command-owned adapter wiring and bypasses through local and external packages.
[Paging boundary check](../../internal/archtest/cutover_boundaries_test.go)

The storage authority check rejects direct and transitive production paths
from checkpoint, history, or backup to `internal/wal`, even through a
registered or external intermediary.
[Storage authority check](../../internal/archtest/storage_authority_test.go)

The oracle independence check follows production edges from
`verify/query-model` and rejects reachability to the production client,
the server command, or a wired or staged library.
Fixtures run the existing registry checks on the allowed verifier-to-client
and verifier-to-oracle edges and on fully registered paths that violate
oracle independence.
[Oracle independence check](../../internal/archtest/verifier_boundaries_test.go)

Run `go test -count=1 ./internal/archtest` and
`GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./...` on the composed tree.
Slipway runs the Linux integration verifier. Full release qualification stays
pending through #60, when the gate can exercise readiness on the active WAL
filesystem along with live compaction, writable restore, and fixed-budget
query parity on one SHA-pinned binary.
