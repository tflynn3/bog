# Bog system architecture

## Deployment and consistency unit

One Linux process, one HTTP listener, one WAL owner, and one pinned local
data directory form Bog's deployment and consistency unit. `cmd/bog` opens
the listener after the owner acquires the exclusive directory lock and
recovers committed state. The owner holds the lock through HTTP drain. A
drain failure leaves the WAL open and locked until process exit.

The WAL owner orders appends and decides when a complete batch becomes
durable and query-visible. The service requires a trusted, service-owned
local filesystem with advisory locking, atomic rename, file and directory
sync, and powersafe overwrite. The ingest and query listener has no built-in
authentication. Deploy it on a trusted network or behind an authenticated
access layer. [Single-writer decision](../adr/0001-single-writer-retained-history.md)

The Linux amd64 and arm64 production inventories at
`a720df0d818b19ba3359aa4226b8b844c8f5e063` agree on the 19 root-module
production packages and 23 direct imports between them shown below.
`cmd/bog` reaches HTTP, readiness, store, and WAL and recovers an ordinary
WAL into an in-memory store. The command still uses path-based readiness
and the in-memory paging constructor. The WAL can capture a published
prefix, and the command has yet to compose that capability with a
retained generation. #108's writable restore, live replacement, and
retained paging await the reviewed Storage handoff.
[Retained-history runtime cutover](../adr/0015-retained-history-runtime-cutover.md)

## Declared Linux production import graph

The Mermaid graph declares the production packages and direct root-module
imports in the executable fitness registry. Arrows point from importer to
direct dependency. Nodes with no outgoing arrows have zero direct
root-module production imports. Test-only packages appear in the package
table below. Runtime contains the registered production closure rooted at
`cmd/bog`. Staged contains all libraries with the staged role, including
backup, which the operator command imports. The architecture suite checks
both memberships and the exact graph against the registry and checks the
registry against both Linux production inventories.

The declaration includes the fixture-backed stream policy verifier support
added in #86 and both #87 capacity evidence imports. The shared validator
and directory-anchored publisher are present in the repository. #87
qualification still requires running the strict evidence gate against the
composed #108 runtime on both native Linux runners.
[Shared capacity evidence boundary](../adr/0020-shared-capacity-evidence.md)

```mermaid
flowchart TD
    subgraph Runtime["Server production closure on Linux amd64 and arm64"]
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
    Operator["cmd/bog-verify"]
    Export["cmd/bog-export"]
    subgraph Staged["Libraries awaiting server activation"]
        Checkpoint["internal/checkpoint"]
        History["internal/history"]
        Backup["internal/backup"]
        Idempotency["internal/idempotency (staged keyed ingest)"]
        History --> Checkpoint
    end
    Client["internal/client (standalone)"]
    Checkpoint --> Store
    History --> Store
    Backup --> Store
    Operator --> Backup
    Operator --> Client
    Operator --> Store
    Export --> Client
    Client --> Store
    subgraph Evidence["Release evidence"]
        Admission["verify/release"]
        Durability["verify/linux"]
        Capacity["bench/retained-history"]
        CapacityEvidence["verify/capacityevidence"]
        Listener["verify/listener"]
        Model["verify/query-model (independent oracle)"]
        StreamPolicy["verify/stream-policy (fixture conformance)"]
        Admission --> Listener
        Capacity --> Listener
        Admission --> CapacityEvidence
        Capacity --> CapacityEvidence
    end
    Admission --> Client
```

- **Ingest.** The WAL syncs whole validated NDJSON batches, syncs and installs
  the commit marker over `bog.commit`, and syncs the directory. It publishes
  the durable rows before HTTP returns 204. A confirmed rollback returns 503
  with no rows published. An uncertain commit or rollback poisons the owner
  and terminates the served process.
- **Query.** Results match a case-sensitive literal within a half-open
  time range and are ordered by UTC timestamp and global append ordinal.
  That order preserves ties, duplicates, and backfill. The measured
  server supplies the replayed in-memory store through
  `httpapi.NewWithQueryLimits`. Query concurrency, scan work, and encoded
  response bytes have independent caps.
- **Recovery.** The measured server validates and replays the committed
  ordinary WAL before listening. Under the directory lock, it rejects
  restored layouts containing `bog.backup` or `bog.wal.tail`. The offline
  backup reader validates verified images. [Restored-image startup design](../adr/0003-wal-capture-and-restored-image-startup.md)
- **Readiness.** `GET /healthz` reports listener liveness after startup.
  `GET /ready` combines disk headroom and the terminal WAL group outcome.
  A confirmed rollback reports `wal_rollback` until a later durable
  outcome. An uncertain outcome reports `recovery_required` while the
  process serves. The measured command samples by configured path. The
  #108 handoff calls for `readiness.NewHandlerForDirectory` with a
  fresh, unlocked open relative to the WAL's pinned directory. The
  command owns that handle and cancels probes before closing it.
  [Readiness descriptor handoff](../adr/0005-pinned-writable-recovery-authority.md#readiness-descriptor-handoff-60)

## Retained-history handoff

#108 must compose the retained `PageSource` in `cmd/bog` and inject it through
`httpapi.NewWithPageSource`. HTTP owns admission, cursor authentication and
expiry, fixed-budget certification through exhaustion, and NDJSON framing.
The adapter pins the validated generation and the matching durable,
query-visible tail together under WAL owner synchronization. A held cursor
keeps its original view across replacement while fresh walks see later
commits. The earlier of the source expiry and HTTP deadline governs new
resumes. Admitted paging and export operations keep their references to
completion. [Runtime cutover](../adr/0015-retained-history-runtime-cutover.md),
[Fixed-budget continuations](../adr/0006-fixed-budget-query-continuations.md)

At cutover, `cmd/bog` must classify and validate ordinary WAL, retained
checkpoint plus committed suffix, and verified restore under the same
pinned directory. It establishes durable writable authority for restored
images before listening. Checkpoint validates and durably publishes
immutable generations. History owns generation scans, pins, and
reclamation. Store owns decoded rows and their order. The command uses
the WAL owner's synchronization to supply checkpoint and backup with a
pinned recovery view of the generation and its exact committed suffix.
The command and WAL owner coordinate append publication and activation.
[Runtime storage composition](../adr/0007-runtime-storage-composition.md),
[Pinned writable recovery](../adr/0005-pinned-writable-recovery-authority.md),
[Tail-only recovery view](../adr/0035-tail-only-retained-recovery-contract.md)

The cutover's opt-in loopback capacity control uses the same publication path.
Charge replay and whole-batch append reservations before reading or
writing the tail. Charge capture, pin, and staging capacity before doing
work. Credit reclaimed capacity after durable publication and activation
and once references to the covered data drain. Stop admitting new work
during shutdown, drain HTTP and storage work, then close storage and the
pinned directory. [Resource admission](../adr/0010-retained-resource-admission.md),
[Storage budget interfaces](../../internal/history/LIMITS.md)

## Package registry

Wired libraries are reachable from `cmd/bog` in the measured Linux
production graph. Staged libraries have a named owner and activation
criteria and await server wiring. Operator or evidence use alone leaves
that role unchanged. Standalone libraries sit outside the server tree and
must be reachable through production imports from a registered command.
Operator and evidence commands share the verifier role in the fitness
registry. Evidence commands establish production ownership of verifier
support packages. Test-only packages have Linux test files and no Linux
production files. [Command root boundaries](../adr/0012-command-root-boundaries.md),
[Standalone package consumers](../adr/0014-standalone-package-consumers.md)

| Package or entry point | Role | Responsibility |
| --- | --- | --- |
| `cmd/bog` | Server command | Recovery, listener, and lifecycle |
| `internal/httpapi` | Wired | HTTP validation, admission, cursors, and response framing |
| `internal/readiness` | Wired | Disk headroom and write outcome observation |
| `internal/wal` | Wired | Directory locking, commit, replay, and WAL capture |
| `internal/store` | Wired | Row decoding, publication, and ordered queries |
| `internal/checkpoint` | Staged | Checkpoint format, bounded WAL scans, and durable generation publication |
| `internal/history` | Staged | Generation validation, ordered scans, pins, and reclamation |
| `internal/backup` | Staged | Pinned export, verification, restore, offline reads, and staging recovery. Its production command consumer is `cmd/bog-verify`. |
| `internal/idempotency` | Staged | Keyed-ingest coordination and durable backend contract |
| `cmd/bog-verify` | Operator command | Read-only image verification and complete HTTP query parity |
| `cmd/bog-export` | Operator command | Stream ordered NDJSON from the server's HTTP query API |
| `internal/client` | Standalone | Production HTTP client for `cmd/bog-verify`, `cmd/bog-export`, and `verify/release`, including complete-page framing, opaque cursors, and ingest outcomes |
| `internal/ingest` | Test-only | Reference decoder and fuzz tests |
| `internal/archtest` | Test-only | Linux dependency fitness suite |
| `verify/fuzz` | Test-only | Bounded backup and history corruption harness |
| `verify/release` | Verifier command | Admission, fixed-budget paging, and report validation. Its test entry point produces restored-parity evidence. |
| `verify/linux` | Verifier command | Linux crash recovery, ENOSPC, and readiness across path retargets |
| `bench/retained-history` | Verifier command | Legacy baseline and live capacity evidence |
| `verify/capacityevidence` | Verifier support | Shared strict schema-v1 live capacity proof for release and capacity |
| `verify/listener` | Verifier support | Linux IPv4 loopback listener ownership for release and capacity |
| `verify/query-model` | Verifier support | Independent append-log oracle for expected query results |
| `verify/stream-policy` | Verifier support | Fixture-backed checks of stream isolation and pre-append redaction |
| `bench/segment-layout` | Nested-module benchmark | Checkpoint layout experiment outside the root module |
| `.github/workflows/linux-release.yml` | Release workflow | Pins the candidate, runs both native jobs, and publishes evidence |

`bog-storage` owns #108 activation for checkpoint, history, and backup.
The table and diagram describe the measured revision. Promote each
library in the command wiring PR once both Linux inventories confirm
server reachability and its activation criteria are met.
Idempotency remains staged under its own criteria. Keyed retries require
the production WAL to commit and recover retry records atomically with
their batches. Exercise concurrent same-key submissions, conflicting
bodies, and retries after an ambiguous response and process restart.
[Staging declarations](../../internal/archtest/staged_packages_test.go),
[Ingest retry contract](../ingest-retries.md)

## Ranked quality attributes

| Rank | Property | Measured server | #108 decision and cost |
| --- | --- | --- | --- |
| 1 | Durability | Acknowledge whole batches after durable commit and fence on uncertain WAL outcomes. | Sync the successor and durably select it before activation. Preserve whole-batch acknowledgments across selection and writable restore. Fencing during uncertain publication costs write availability. |
| 2 | Exact queries | Preserve timestamp and append ordinal order in memory and through ordinary WAL replay. | Pin a generation and its exact committed suffix together. Merge by UTC timestamp and append ordinal through compaction and restart. Certification spends work before cursor issuance, and live pins delay reclamation. |
| 3 | Bounded resources | Cap query work, response bytes, and concurrency. | Reserve replay, whole-batch ingest, capture, pin, and staging capacity before work. Capacity pressure can defer maintenance and refuse whole batches while admitted work drains. |
| 4 | Recovery time | Replay the full committed ordinary WAL before listening. | Validate the selected generation and writable authority, then replay its bounded committed suffix. Manifest, segment, and suffix validation adds startup I/O. |
| 5 | Throughput | Group syncs across admitted commits. | Prune provably irrelevant scans and serialize activation with appends. Keeping and hashing the readable v1 WAL prefix costs disk space and I/O. A verified tail-only v1 restore has finite writable tail capacity. |

## Operator verification boundary

`cmd/bog-verify` consumes an immutable verified image and, for parity, a
separately restored server held at the image's recovery point. It compares
timestamp and line values in exact order through both terminal pages
using the read-only backup APIs and the production Go client. Its
authority is limited to reading the immutable image and querying the
separate server. The server owns writable recovery and publication.
The operator command has no direct or transitive root-module production
path to `internal/wal`, `internal/httpapi`, or `internal/readiness`.
The server has no production path to the Go HTTP client or independent
query oracle. [Operator verification](../adr/0011-operator-verification-boundary.md),
[Command root boundaries](../adr/0012-command-root-boundaries.md)

## Operator export boundary

`cmd/bog-export` streams NDJSON from a snapshot query against the issuing
server. It follows opaque continuations with the original time range and
literal filter, preserving server order, timestamp ties, and duplicates.
Its only direct root-module production import is `internal/client`, which
uses `internal/store` for shared row types. Direct and transitive imports
to the server command, HTTP, readiness, WAL, backup, checkpoint, history,
idempotency, and packages at or under `verify` and `bench` are forbidden.
The walk uses one overall timeout and a finite page budget. The exporter
returns success after writing the terminal page. A walk or write failure
returns nonzero, and previously emitted complete rows form a partial export.
Release qualification uses the pinned candidate and its own evidence
producers. [Operator export boundary](../adr/0032-operator-export-boundary.md)

## Release evidence

Qualify the same clean composed revision on native Linux amd64
(`ubuntu-24.04`, `GOAMD64=v1`) and arm64 (`ubuntu-24.04-arm`,
`GOARM64=v8.0`). Each job verifies its runner and kernel architecture,
builds once with Go 1.27.0 and cgo disabled, and uses that job's pinned
binary for every producer. The workflow checks out the candidate and
the release gate at their respective pinned revisions. Reports bind to
the full candidate revision, the native binary's SHA-256, and the
`linux-release-v1` fixture. Both matrix jobs must pass strict report
validation and cleanup. Qualification of the composed #108 revision
is pending.
[Linux release workflow](../../.github/workflows/linux-release.yml)

| Producer | Report | Required evidence |
| --- | --- | --- |
| `verify/release` | `admission.json` | Query slot saturation, concurrent ingest, cursor replay, and clean work-budget rejection. Issuing budgets complete tied and sparse, byte-trimmed walks, including a smaller resume limit. |
| `verify/linux` | `durability.json` | SIGKILL recovery, committed-prefix checks, and real ENOSPC on a disposable local filesystem. Retarget the configured path in both directions and check readiness and ingest against the active WAL filesystem. |
| `bench/retained-history` | `capacity.json` | Both live scales (196,608 and 393,216 seed rows), each under a 128 MiB cgroup cap with swap disabled. Verify durable replacement at the exact cutoff, a held cursor across acknowledged backfill, exact fresh and restarted walks, and process and cgroup cleanup. Capture publication and retired pin drain evidence at the runtime observation points. |
| `verify/release` / `TestReleaseRestoredParity` | `parity.json` | Start the pinned binary on a verified restore, compare exact walks with the independent append-log oracle, observe 204 for new backfill, and check the held capture and exact augmented history after restart. Exercise immediate post-ack crash recovery on a separate restored fixture. |
| `verify/release` / `TestReleaseFixedBudgetBackfill` and `TestReleaseCapacityControlScope` | Passing test events | Exercise fixed-budget paging through backfill and replacement. Check that capacity routes are available only with explicit opt-in on the loopback listener. |
| `verify/query-model` / `TestPinnedQueryModelReplay` | `query-model-events.jsonl` | Require passing seeded ordinary and retained walks, exact terminal replay, and a candidate identity matching the revision and binary checksum on each runner. |

The baseline includes the native query replay startup checks for verified
bind collisions. Native qualification for this baseline is pending.
The two Linux inventories measure build topology, and the cross-build
checks compilation. Native verdicts require the pinned-binary producers.

The three producers exercise a separate pinned server process through HTTP,
process lifecycle, and disposable filesystem fixtures. Their Linux production
imports have no direct or transitive root-module path to `internal/wal`,
`internal/httpapi`, `internal/readiness`, `internal/checkpoint`,
`internal/history`, or `internal/backup`. The exclusions apply through
registered intermediaries and external dependencies and continue to apply
after retained libraries are wired into the server. Producers may use the
production HTTP client and shared row types. Test imports may use server
and storage packages to construct fixtures. Restored-parity evidence
comes from the pinned candidate and the independent append-log oracle.
Operator parity uses the verified image as its reference.
[Release verifier composition](../adr/0008-release-verifier-composition.md),
[Release evidence process boundary](../adr/0013-release-evidence-process-boundary.md),
[Retained storage boundary for release evidence](../adr/0021-retained-release-evidence-boundary.md),
[Release evidence fitness test](../../internal/archtest/release_evidence_boundary_test.go)

Release and capacity each verify listener ownership around the startup
HTTP probe and at workload completion. Capacity repeats the checks after
restart using the new child PID. The workflow records restored-parity
producer success only after an explicit passing test event and a
nonempty report. [Candidate listener ownership](../adr/0017-release-candidate-listener-ownership.md)

The benchmark's `-validate-cutover` and release's `validate -require-cutover`
use one strict schema-v1 capacity validator. The proof requires a numeric
zero in the raw cgroup OOM kill counter and validates the observed
backfill timeline in execution order. Standard release validation still
accepts historical schema-v1 live reports. Release validation matches
the pinned candidate identity and requires affirmative writable-restore
parity in strict mode. Release report publication anchors staging,
replacement, cleanup, and directory sync to the same opened parent.
[Capacity evidence validation](../adr/0018-capacity-evidence-validation.md),
[Shared capacity evidence boundary](../adr/0020-shared-capacity-evidence.md)

The workflow publishes `linux-release-evidence-amd64` and
`linux-release-evidence-arm64` with build identity, fixture and runner
details, reports, exit statuses, and logs. `verifier-race.log` includes the
bounded backup and history corruption corpus. The capacity sample
records publication at the requested cutoff. Runtime pin drain requires
a positive retired-generation pin count while the held cursor is live
and an observed zero after explicit retirement and drain. Missing runtime
metrics carry an unavailable reason. Record the configured resource limits and exercise
whole-batch and maintenance refusal, recovery when capacity returns, and
expiry with admitted operations still draining.
[Capacity evidence contract](../../bench/retained-history/README.md#publication-and-pin-drain-sample-contract),
[Resource qualification](../adr/0010-retained-resource-admission.md#qualification)

## Stream policy conformance

Security Verification owns the fixture-backed runner for the proposed
shared-deployment contract. It compares authorized query and cursor results,
retry outcomes, committed bytes and replayed records, and scoped export
evidence. Positive controls require exact retained values and durable byte
witnesses. Missing adapter evidence leaves conformance incomplete. This
milestone runs against deterministic synthetic adapters. Live server
qualification requires adapters that observe a shared-deployment implementation.
[Proposed stream authorization and durable redaction contract](../shared-deployment/stream-authorization-redaction.md),
[Runner and adapter contract](../../verify/stream-policy/README.md)

## Fitness and registry ownership

Architecture in the bog AgentSet owns the architecture record and
fitness registry, including this diagram check. `bog-storage` owns #108
recovery, publication, and command composition. `bog-query` owns the
HTTP paging contract. Release Verification owns the two native verdicts.
Cycle 158 advances #108 to a reviewed Storage handoff and wires the bounded
retained runtime when the command lease clears. #81 can start against the
frozen recovery-view contract. #104 remains reserved behind the reviewed
HTTP paging handoff. The backlog shows two running tracks and PM reports
three. Preserve capacity for the reported third track until dispatch
reconciles the counts. Keep the listener work, #106, and the exclusive
eval and council leases with their current owners. Recheck read and
write leases at dispatch and at the reviewed Storage SHA.
[Linux architecture registry](../../internal/archtest/architecture_test.go),
[Cutover handoff](../adr/0015-retained-history-runtime-cutover.md#integration-and-qualification),
[Tail-only recovery contract](../adr/0035-tail-only-retained-recovery-contract.md)

At the reviewed Storage SHA, compare both Linux inventories. The command
wiring PR updates `internal/archtest/architecture_test.go` with the exact
imports and roles, `internal/archtest/staged_packages_test.go` with the
remaining staging declarations, and `docs/architecture/SYSTEM.md` with the
measured revision, diagram, and role table. Preserve the operator backup
edge and the exporter and release evidence registrations.
[Package role governance](../adr/0019-package-role-governance.md),
[Exporter integration handoff](../adr/0032-operator-export-boundary.md#integration-and-qualification)

The Architecture workflow runs `go test -count=1 ./internal/archtest`.
The suite lists packages with `GOOS=linux`, `GOAMD64=v1`, `GOARM64=v8.0`,
`CGO_ENABLED=0`, `GOENV=off`, empty `GOFLAGS`, `GO111MODULE=on`, and
`GOWORK=off`, once for each target. Exact imports, cycles, package kinds,
and server reachability are checked against both inventories. The diagram
fitness test checks every declared production node and directed edge, the
Runtime closure, and Staged membership against the same registry. Test
imports and nested modules are outside the root-module production registry.
[Diagram fitness test](../../internal/archtest/system_diagram_test.go)

- HTTP owns the paging seam. It cannot reach checkpoint, history, or
  backup, even through a registered intermediary. Readiness can observe
  the WAL outcome and cannot reach retained storage packages.
  [Paging boundary](../../internal/archtest/cutover_boundaries_test.go)
- Checkpoint, history, and backup cannot reach the WAL. Storage cannot
  depend on HTTP, readiness, or the Go HTTP client. Only `cmd/bog` may
  reach both WAL and any retained storage library, including through a
  relay. The Linux inventory checks the paging and storage authority
  boundaries through external dependencies as well.
  [Storage authority](../../internal/archtest/storage_authority_test.go)
- Command roots enforce server, operator verification, and operator export
  exclusions. The measured Linux graph checks those exclusions and the
  oracle and three release producer boundaries through external production
  dependencies as well.
  [Server boundary](../../internal/archtest/command_roots_test.go),
  [Operator boundary](../../internal/archtest/operator_boundary_test.go),
  [Exporter boundary](../../internal/archtest/export_boundary_test.go),
  [Oracle boundary](../../internal/archtest/verifier_boundaries_test.go),
  [Release evidence boundary](../../internal/archtest/release_evidence_boundary_test.go)
- Stream policy conformance runs outside every registered production command
  dependency tree. Its own production imports cannot reach internal packages
  or command roots, including through external production dependencies.
  [Stream policy boundary](../../internal/archtest/stream_policy_boundary_test.go)
- The release and capacity producers are the only production consumers of
  the listener helper. The check follows indirect imports as well.
  [Candidate listener boundary](../../internal/archtest/listener_boundary_test.go)
- `verify/release` and `bench/retained-history` must both import
  `verify/capacityevidence` directly in production. The helper has no
  root-module production imports. The check rejects production reachability
  from any other registered package, including through an intermediary.
  [Capacity evidence boundary](../../internal/archtest/capacity_evidence_boundary_test.go)
- Standalone libraries require a production command consumer. Every staged
  package requires an owner and activation criteria. Declarations for
  removed or reclassified packages fail the suite.
  [Standalone consumer check](../../internal/archtest/standalone_consumers_test.go),
  [Staging declaration check](../../internal/archtest/staged_packages_test.go)
