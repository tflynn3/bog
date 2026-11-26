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

These are the direct imports between root-module production packages at
`ae0716c6adc383dbd6e23cce544391eff1815eff`, listed for Linux amd64 with
cgo disabled. Arrows point from importer to dependency. Test-only packages
are listed in the registry below. An isolated node has zero direct
root-module production imports.

```mermaid
flowchart TD
    subgraph Runtime["Server and reachable libraries"]
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
    subgraph Standalone["Standalone library"]
        Client["internal/client"]
    end
    Client --> Store
    subgraph Evidence["Verifier commands and support"]
        Admission["verify/release"]
        Durability["verify/linux"]
        Capacity["bench/retained-history"]
        Model["verify/query-model"]
    end
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

## Retained-history handoff

At cutover, `cmd/bog` injects the retained adapter into HTTP's consumer-owned
`PageSource`. HTTP owns admission, cursor authentication and lifetime, and
NDJSON framing. The adapter uses the WAL owner and history catalog to answer
queries from a coherent capture. Checkpoint validates and durably publishes
immutable generations. History owns generation pins, segment scans, and
reclamation. Store owns decoded tail rows and their order. Backup consumes a
pinned storage view. The packages providing storage, including the staged
retry library, stay independent of HTTP, readiness, and the Go HTTP client.

The coordinated capture binds the validated generation and exact durable,
query-visible tail under the same owner. A cursor reads the same view across
generation replacement. A fresh walk sees later committed appends. Writable
restore establishes checkpoint and tail recovery authority before listening,
and restart selects that writable authority. The opt-in loopback capacity
control drives replacement through the same owner.
[ADR 0004](../adr/0004-retained-history-cutover.md)
[ADR 0005](../adr/0005-pinned-writable-recovery-authority.md)

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
| `internal/client` | Standalone | HTTP client, page validation, and ingest outcomes |
| `internal/ingest` | Test-only | Reference decoder and fuzz tests |
| `internal/archtest` | Test-only | Linux dependency fitness suite |
| `verify/fuzz` | Test-only | Linux backup and history corruption harness with bounded inputs (#58) |
| `verify/release` | Verifier command | Admission and fixed-budget paging probes, report validation, and the restored-parity test entry point |
| `verify/linux` | Verifier command | Linux crash recovery, ENOSPC, and readiness across path retargets (#3) |
| `bench/retained-history` | Verifier command | Legacy baseline and live capacity producer (#32) |
| `verify/query-model` | Verifier support | Independent append-log oracle |
| `bench/segment-layout` | Nested-module benchmark | Checkpoint layout experiment outside the root-module graph |
| `.github/workflows/linux-release.yml` | Release workflow | Pins the candidate, runs the producers, and publishes evidence |

The activation criteria for keyed retries include concurrent same-key
submissions, conflicting bodies, and retries after an ambiguous response and
process restart. [Ingest retry contract](../ingest-retries.md)

## Ranked quality attributes

| Rank | Property | Trade-off |
| --- | --- | --- |
| 1 | Durability | Wait for durable whole-batch commit before acknowledging. Stop on uncertain writes or committed corruption, even when availability suffers. |
| 2 | Exact queries | Preserve ordered membership through restart and, at cutover, through compaction and verified restore. Certifying a fixed-budget walk uses work before issuing its first cursor. |
| 3 | Bounded resources | Limit captures, concurrent scans, request work, and response bytes. At cutover, bound the writable tail and retain pinned generations through cursor expiry and operation drain. Pins delay disk reclamation, and v1 format compatibility costs disk space for a readable WAL prefix. |
| 4 | Recovery time | The base replays the committed WAL before opening the listener. Cutover trades checkpoint validation before startup for bounded tail replay. |
| 5 | Throughput | Share syncs across admitted writes and prune provably irrelevant scan work. Hashing the v1 prefix for publication and export consumes disk bandwidth. |

## Release evidence

The [Linux release workflow](../../.github/workflows/linux-release.yml)
builds one clean Go 1.27.0 candidate on Ubuntu 24.04 amd64 from the supplied
full SHA and supplies the same binary to four evidence sources. Every report
must match the revision, binary SHA-256, and `linux-release-v1` fixture.

| Source | Report | Required evidence |
| --- | --- | --- |
| `verify/release` | `admission.json` | Saturated query slot, concurrent ingest, cursor resume, and clean work-budget rejection. Issuing budgets must complete tied-row and sparse, byte-trimmed walks, including a smaller resume limit. |
| `verify/linux` | `durability.json` | SIGKILL recovery, committed-prefix checks, and observed ENOSPC on a disposable local filesystem. Retarget the configured path in both directions and require readiness and ingest to follow the active WAL filesystem. |
| `bench/retained-history` | `capacity.json` | Confirm a newer durable generation at the exact requested cutoff, resume a held cursor across replacement during acknowledged backfill, and verify fresh ordered history including after restart. Run at both fixture sizes with bounded cgroup and process cleanup. |
| `verify/release` / `TestReleaseRestoredParity` | `parity.json` | Start the shipped binary on a verified restore, compare fresh HTTP walks, observe 204 for a new append, and recover the exact augmented history after restart. |

The `linux-release-evidence` artifact contains build identity, fixture and
runner details, reports, exit statuses, and logs. The legacy capacity run
records a baseline. See the [release verifier contract](../../verify/release/README.md).

## Fitness and registry ownership

The architecture track owns the fitness registry. It requires `verify/fuzz`
in the Linux inventory with test files only. Its disappearance or conversion
to production fails the suite. Checkpoint, history, and backup are staged on
the current tree.

At integration, review both sides of the current merge base and inspect the
actual Linux production graph with #53 and #59 composed. In the wiring merge,
the architecture owner promotes the packages newly reachable from `cmd/bog`,
registers any extracted adapter and its direct imports, and removes retired
entries and edges. The composed-cutover fixture exercises these rules with a
representative graph. [Fitness registry](../../internal/archtest/architecture_test.go)

The Architecture workflow runs `go test -count=1 ./internal/archtest` on the
host. The suite lists packages with `GOOS=linux GOARCH=amd64 GOAMD64=v1
CGO_ENABLED=0` and `GOENV=off GOFLAGS= GO111MODULE=on GOWORK=off`. It checks
production imports within the root module and follows all listed dependencies
for server reachability. Host-only files, test imports, and nested modules
are outside the production import registry. Package loading errors, cycles,
unregistered packages, incorrect roles, forbidden imports, and stale
registered imports fail the suite. A storage dependency on HTTP, readiness,
or the Go HTTP client fails even when it is transitive or all direct edges
are registered. Direct HTTP imports of checkpoint, history, or backup also
fail, keeping retained paging behind the consumer seam. Run
`go test -count=1 ./internal/archtest` and
`GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./...` on this PR. Repeat both
on the composed tree before dispatching the release gate with its full commit
SHA. Qualification requires the same Linux binary to pass verified restore,
fixed-budget pagination, readiness on the active WAL filesystem, and the other
checks in the existing gate.
