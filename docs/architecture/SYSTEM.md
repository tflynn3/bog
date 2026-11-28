# Bog system architecture

## Deployment and consistency unit

One Linux process, one HTTP listener, one WAL owner, and one pinned local
data directory form Bog's deployment and consistency unit. `cmd/bog` opens
the listener after the owner acquires the exclusive directory lock and
recovers committed state. The owner holds the lock through HTTP, capture, and storage drain.
A drain failure leaves the WAL open and locked until process exit.

The WAL owner orders appends and decides when a complete batch becomes
durable and query-visible. The service requires a trusted, service-owned
local filesystem with advisory locking, atomic rename, file and directory
sync, and powersafe overwrite. The ingest and query listener has no built-in
authentication. Deploy it on a trusted network or behind an authenticated
access layer. [Single-writer decision](../adr/0001-single-writer-retained-history.md)

The #108 Linux amd64 and arm64 production inventories agree on the 21
root-module production packages and 30 direct imports shown below.
The server closure includes HTTP, readiness, store, WAL, checkpoint,
history, and backup. The command recovers and advances retained history
under the WAL directory lock.
[Retained-history runtime cutover](../adr/0015-retained-history-runtime-cutover.md),
[Retained runtime promotion](../adr/0049-retained-runtime-promotion.md)

## Declared Linux production import graph

The Mermaid graph declares the production packages and direct root-module
imports in the executable fitness registry. Arrows point from importer to
direct dependency. Nodes with no outgoing arrows have zero direct
root-module production imports. Test-only packages appear in the package
table below. Runtime contains the registered production closure rooted at
`cmd/bog`. Staged contains all libraries with the staged role. The architecture suite checks
both memberships and the exact graph against the registry and checks the
registry against both Linux production inventories.

The declaration includes the fixture-backed stream policy verifier support
added in #86, both #87 capacity evidence imports, and the ADR 0052
report publisher and its four direct imports. #87
qualification still requires running the strict evidence gate against the
composed #108 runtime on both native Linux runners.
[Shared capacity evidence boundary](../adr/0020-shared-capacity-evidence.md)

```mermaid
flowchart TD
    subgraph Runtime["Measured server production closure on Linux amd64 and arm64"]
        Cmd["cmd/bog"]
        HTTP["internal/httpapi"]
        Ready["internal/readiness"]
        WAL["internal/wal"]
        Store["internal/store"]
        Checkpoint["internal/checkpoint"]
        History["internal/history"]
        Backup["internal/backup"]
        Cmd --> Backup
        Cmd --> Checkpoint
        Cmd --> History
        History --> Checkpoint
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
        Idempotency["internal/idempotency (staged keyed ingest)"]
        StreamPolicyEngine["internal/streampolicy"]
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
        ReportPublication["verify/reportpublication"]
        Listener["verify/listener"]
        Model["verify/query-model (independent oracle)"]
        StreamPolicy["verify/stream-policy (fixture conformance)"]
        Admission --> Listener
        Capacity --> Listener
        Admission --> CapacityEvidence
        Capacity --> CapacityEvidence
        Admission --> ReportPublication
        Durability --> ReportPublication
        Capacity --> ReportPublication
        CapacityEvidence --> ReportPublication
    end
    Admission --> Client
```

- **Ingest.** The WAL syncs whole validated NDJSON batches and atomically
  installs the updated commit record before syncing the directory. The
  command publishes the durable rows before HTTP returns 204. A confirmed
  rollback returns 503 and keeps queries available. An uncertain WAL append
  poisons the owner and terminates the process for recovery.
- **Query.** The command pairs the selected generation with its exact
  committed suffix and merges rows by UTC timestamp and global append
  ordinal. The merge preserves ties, duplicates, and backfill. Queries
  match a case-sensitive literal within a half-open time range.
- **Recovery.** The command validates durable authority and replays the
  bounded suffix before listening. First startup can stream an ordinary
  WAL into its first checkpoint or activate a verified restore.
  [Restored-image startup design](../adr/0003-wal-capture-and-restored-image-startup.md)
- **Readiness.** `GET /healthz` reports listener liveness. `GET /ready`
  combines headroom on the pinned WAL filesystem and the latest terminal
  write outcome. A confirmed rollback reports `wal_rollback` until a
  later durable write.
  [Readiness descriptor handoff](../adr/0005-pinned-writable-recovery-authority.md#readiness-descriptor-handoff-60)

## Retained-history runtime

`cmd/bog` injects its page source through
`httpapi.NewWithPageSourceAndQueryResources`. Acquisition captures the
selected manifest and the durable, query-visible suffix together under the
command's append and activation mutex. The WAL capture records the pinned
inode and exact byte and ordinal bounds. HTTP owns admission, cursor
authentication and expiry, fixed-budget certification, NDJSON framing,
and final release. A held cursor keeps its original generation and suffix
across replacement and later acknowledgments. The page source meters
capture and scan work and returns deterministic results at the same
anchor.
[Runtime interfaces](../adr/0015-retained-history-runtime-cutover.md#runtime-interfaces),
[Resource-aware constructor](../adr/0055-retained-query-resource-construction.md)

Checkpoint stages and validates immutable successor files while the
predecessor stays active. At the requested whole-frame cutoff the WAL
owner reconciles later commits, copies the remaining frames with their
original logical offsets, syncs the successor tail, and installs and
syncs `bog.authority` before the command installs the new catalog view.
The old files remain reachable by admitted operations until their pins
drain. An uncertain selector fences writes and new captures until
recovery validates the durable choice. Checkpoint reads a command-owned
recovery view. Backup verifies restore input through its independent
pinned-directory API.
[Runtime storage composition](../adr/0007-runtime-storage-composition.md),
[Tail-only recovery view](../adr/0035-tail-only-retained-recovery-contract.md),
[Writable authority](../adr/0038-writable-authority-verification.md)

The default tail schedules advancement at 16 MiB of encoded frames or
8,192 frames. The live suffix is limited to 32 MiB and 16,384 frames. Live and
capture-held frames share 48 MiB of encoded-frame reservations. Storage
admits at most eight generations and uses a 1 GiB checkpoint staging
budget on disk. The issuing API has 64 capture slots, a 112 MiB aggregate
scan budget, a 32 MiB aggregate response budget, and an 8 MiB aggregate ingest body budget across 16 request slots. Each retained scan reserves 14 MiB and returns a bounded raw-line
chunk. The default per-query limits are 32 MiB encoded, eight query
slots, and 128 Mi work units. The command defaults Go's soft runtime memory
limit to 88 MiB when the environment supplies no override.
[Resource admission](../adr/0010-retained-resource-admission.md)

The opt-in loopback capacity control uses the same publication path and
can retire cursors through their issuing API. The command opens and
identity-checks an independent directory descriptor relative to the WAL
owner for `readiness.NewHandlerForDirectory`. On shutdown it stops
maintenance scheduling, cancels readiness probes, drains HTTP, closes the
issuing API, then drains storage before closing the WAL owner.
[Query delivery and shutdown](../adr/0039-query-delivery-and-retained-cutover.md)

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
| `cmd/bog` | Server command | Retained storage composition, listener, and lifecycle |
| `internal/httpapi` | Wired | HTTP validation, admission, cursors, and response framing |
| `internal/readiness` | Wired | Disk headroom and write outcome observation |
| `internal/wal` | Wired | Pinned directory locking, commit, replay, and published-prefix capture |
| `internal/store` | Wired | Row decoding, publication, and ordered queries |
| `internal/checkpoint` | Wired | Checkpoint format, bounded WAL scans, and durable generation publication |
| `internal/history` | Wired | Generation validation, ordered scans, pins, and reclamation |
| `internal/backup` | Wired | Verified writable restore input, pinned export, and offline verification |
| `internal/idempotency` | Staged | Keyed-ingest coordination and durable backend contract |
| `internal/streampolicy` | Staged | Atomic policy activation, scoped grant decisions, query masking, and whole-batch ingest redaction |
| `cmd/bog-verify` | Operator command | Read-only image verification and complete HTTP query parity |
| `cmd/bog-export` | Operator command | Stream ordered NDJSON from the server's HTTP query API |
| `internal/client` | Standalone | Production HTTP client for `cmd/bog-verify`, `cmd/bog-export`, and `verify/release`, including complete-page framing, opaque cursors, and ingest outcomes |
| `internal/ingest` | Test-only | Reference decoder and fuzz tests |
| `internal/archtest` | Test-only | Linux dependency fitness suite |
| `verify/fuzz` | Test-only | Bounded backup and history corruption harness |
| `verify/release` | Verifier command | Admission, fixed-budget paging, and report validation. Its test entry point produces restored-parity evidence. |
| `verify/linux` | Verifier command | Linux crash recovery, ENOSPC, and readiness across path retargets |
| `bench/retained-history` | Verifier command | Legacy baseline and live capacity evidence |
| `verify/capacityevidence` | Verifier support | Shared strict schema-v1 live capacity proof and nested benchmark publication compatibility |
| `verify/reportpublication` | Verifier support | Descriptor-relative staging, atomic commit, and durability for encoded reports |
| `verify/listener` | Verifier support | Linux IPv4 loopback listener ownership for release and capacity |
| `verify/query-model` | Verifier support | Independent append-log oracle for expected query results |
| `verify/stream-policy` | Verifier support | Fixture-backed checks of stream isolation and pre-append redaction |
| `bench/segment-layout` | Nested-module benchmark | Checkpoint layout experiment outside the root module |
| `.github/workflows/linux-release.yml` | Release workflow | Pins the candidate, runs both native jobs, and publishes evidence |

Both Linux inventories include checkpoint, history, and backup in the
server closure. The operator retains its separate direct backup import.
Idempotency remains staged under its own criteria. Keyed retries require
the production WAL to commit and recover retry records atomically with
their batches. Exercise concurrent same-key submissions, conflicting
bodies, and retries after an ambiguous response and process restart.
[Retained runtime promotion](../adr/0049-retained-runtime-promotion.md),
[Staging declarations](../../internal/archtest/staged_packages_test.go),
[Ingest retry contract](../ingest-retries.md)

Core Engineering owns the staged stream-policy engine. It has zero direct
root-module production imports and is outside the measured server closure
on both Linux targets. Activation requires measured server reachability
and the independent serving evidence specified by ADR 0050 on native
Linux amd64 and arm64.
[Stream-policy decision boundary](../adr/0050-stream-policy-decision-boundary.md)

## Ranked quality attributes

| Rank | Property | Runtime contract and cost |
| --- | --- | --- |
| 1 | Durability | Acknowledge whole batches after durable commit and visibility. Sync and select the successor before activation. Fence uncertain outcomes until recovery. |
| 2 | Exact queries | Pin the generation and committed suffix together. Preserve timestamp and append ordinal order through replacement, retirement, and restart. |
| 3 | Bounded resources | Reserve replay, ingest, scan, response, pin, and staging capacity before work. Held captures can defer reclamation and keep capacity charged. |
| 4 | Recovery time | Validate selected authority and immutable history, then replay the bounded suffix. Large ordinary WALs stream through bootstrap. |
| 5 | Throughput | Prune irrelevant segments and tail batches by time. Stage immutable files concurrently with ingests and serialize the final authority switch. |

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
validation and cleanup. Qualification of the composed #108 revision is
pending the reviewed Storage handoff and both native verdicts. Release
Verification owns the gate and the #130 retirement and #131 report
publication evidence tracks. The workflow SHA pins the producer-manifest
and Go test-event checker, and the workflow supplies the expectations for
each job.
[Linux release workflow](../../.github/workflows/linux-release.yml),
[Release gate evidence checks](../adr/0048-release-gate-evidence-checks.md)

| Producer | Report | Required evidence |
| --- | --- | --- |
| `verify/release` | `admission.json` | Query slot saturation, concurrent ingest, cursor replay, and clean work-budget rejection. Issuing budgets complete tied and sparse, byte-trimmed walks, including a smaller resume limit. |
| `verify/linux` | `durability.json` | SIGKILL recovery, committed-prefix checks, and real ENOSPC on a disposable local filesystem. Retarget the configured path in both directions and check readiness and ingest against the active WAL filesystem. |
| `bench/retained-history` | `capacity.json` | Both live scales (196,608 and 393,216 seed rows), each under a 128 MiB cgroup cap with swap disabled. Verify durable replacement at the exact cutoff, a held cursor across acknowledged backfill, exact fresh and restarted walks, and process and cgroup cleanup. Capture publication and retired pin drain evidence at the runtime observation points. |
| `verify/release` / `TestReleaseRestoredParity` | `parity.json` | Record ordinary WAL reopen, restored parity, immediate post-ack crash, and two-cycle tail-only parity as four outcomes in the existing candidate-bound report. |
| `verify/release` / `TestReleaseFixedBudgetBackfill` and `TestReleaseCapacityControlScope` | Passing test events | Exercise fixed-budget paging through backfill and replacement. Check that capacity routes are available only with explicit opt-in on the loopback listener. |
| `verify/release` / `TestReleaseRetainedShutdown` (gate checkout) | `retained-shutdown-events.jsonl`, `retained_shutdown` in `producers.txt` | Require explicit test and package passes under race against each runner's pinned candidate binary, with SHA-256 checks before and after. Across replacement, observe one retired predecessor pin and a backpressured continuation holding the sole query slot. Require listener close and lock retention during SIGTERM drain, exact terminal NDJSON, successful exit and reaping, and exact replay of acknowledged rows after restart on the same directory. |
| `verify/query-model` / `TestPinnedQueryModelReplay` | `query-model-events.jsonl` | Require passing seeded ordinary and retained walks, exact terminal replay, and a candidate identity matching the revision and binary checksum on each runner. |

Each native job requires one successful `retained_shutdown` manifest
record bound to the candidate revision, native architecture, and pinned
gate SHA. The test-event checker requires one pass each for
`TestReleaseRetainedShutdown` and `bog/verify/release` and rejects skips
and failures. Storage also supplies command-level evidence of resource
closure order and lock retention on failed drain.
[Retained shutdown gate](../adr/0048-release-gate-evidence-checks.md#retained-shutdown),
[Command-level shutdown evidence](../adr/0039-query-delivery-and-retained-cutover.md#native-retained-shutdown)

Strict cutover validation requires all four `parity.json` outcomes on each
native runner. Every recovery walk compares exact order and multiplicity
through exhaustion with the independent append-log oracle.

| Outcome | Required evidence |
| --- | --- |
| `ordinary_wal_reopen` | Reopen a seeded ordinary WAL, require HTTP 204 for a backfill batch, and verify the exact augmented walk before shutdown and after restart. |
| `restored_parity` | Start on a verified restore, require HTTP 204 for backfill, and verify both the held capture and fresh augmented walks. After crash and restart, check the exact augmented history and rejection of the pre-crash cursor. |
| `post_restore_ack_crash` | On a separate verified restore, send SIGKILL immediately on HTTP 204, before reading or closing the response body. Verify the signal, exact restart replay, both processes reaped, and fixture removal. |
| `tail_only_two_cycle_parity` | Start from a verified v1 restore with a nonempty tail and carry a nonempty suffix through each exact cutoff. Check held, fresh, restarted, and separately restored walks, v2 export and restore, generation and suffix digests, logical bounds, and physical tail rebasing. Verify the recovered next ordinal and use the first export as the second cycle's input. |

[Recovery parity contract](../../verify/release/README.md),
[Immediate post-restore crash evidence](../adr/0027-immediate-post-restore-crash-evidence.md)

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
the pinned candidate identity across evidence reports. Release, retained
history, and runner preflight publish through the shared transaction.
Standalone publication replaces an existing entry. The delegated supervisor
and producer use Linux atomic create-only commits in the parents opened by
the supervisor. The supervisor reads a bounded regular proof from the
inherited parent only after the producer exits successfully. Publication
records whether commit failed, committed with uncertain durability, or
completed both syncs.
[Shared report publication](../adr/0052-shared-report-publication.md)
[Capacity evidence validation](../adr/0018-capacity-evidence-validation.md),
[Shared capacity evidence boundary](../adr/0020-shared-capacity-evidence.md)

The workflow publishes `linux-release-evidence-amd64` and
`linux-release-evidence-arm64` with build identity, fixture and runner
details, reports, exit statuses, and logs. `verifier-race.log` includes the
bounded backup and history corruption corpus. The capacity sample
records publication at the requested cutoff. Runtime pin drain requires
a positive retired-generation pin count while the held cursor is live
and an observed zero after explicit retirement and drain. The
`verify/release` retirement fixture exercises the reclamation check with
two admitted readers, explicit cursor retirement, and a rejected new
resume. It checks that after the first reader drains the predecessor
artifact and candidate descriptor remain pinned, then checks that the
last reader's completion removes both while preserving successor
artifacts. The deliberate-pin and missing-reader cases exercise rejection
and bounded teardown. Native qualification still requires the composed
candidate's observed behavior. Missing runtime metrics carry an
unavailable reason. Record the configured resource limits and exercise
whole-batch and maintenance refusal, recovery when capacity returns, and
expiry with admitted operations still draining.
[Capacity evidence contract](../../bench/retained-history/README.md#publication-and-pin-drain-sample-contract),
[Retirement fixture](../../verify/release/retirement_linux_test.go),
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

Core Architecture in the bog AgentSet owns the architecture record and
fitness registry, including the diagram and constructor checks.
`bog-storage` owns #108 recovery, publication, and command composition.
Core Engineering owns the HTTP paging contract. Release Verification
owns the native Linux amd64 and arm64 verdicts for the same composed
revision. #108 remains the release gate.
[Linux architecture registry](../../internal/archtest/architecture_test.go),
[Cutover handoff](../adr/0015-retained-history-runtime-cutover.md#integration-and-qualification),
[Tail-only recovery contract](../adr/0035-tail-only-retained-recovery-contract.md)

ADR 0053 tightens the constructor fitness guard introduced by ADR 0040.
Storage carries #47 and #15 in #108 and coordinates role promotion with
Core Architecture in the reviewed command wiring PR. Recheck write leases
at dispatch and at the Storage handoff. At the reviewed Storage SHA,
compare both Linux inventories and rerun the architecture suite. Verify the
write leases cover the composition and architecture files before landing
the combined change. The wiring PR updates
`internal/archtest/architecture_test.go` with the exact
imports and roles, `internal/archtest/staged_packages_test.go` with the
remaining staging declarations, and `docs/architecture/SYSTEM.md` with the
measured revision, diagram, and role table. Preserve the operator backup
edge and the exporter and release evidence registrations.
Require the architecture suite to pass on the combined revision before
merging the wiring PR.
[Required constructor calls](../adr/0053-retained-serving-constructor-calls.md),
[Package role governance](../adr/0019-package-role-governance.md),
[Retained runtime promotion](../adr/0049-retained-runtime-promotion.md),
[Exporter integration handoff](../adr/0032-operator-export-boundary.md#integration-and-qualification)

At promotion, Storage traces the serving `PageSource` to the command-owned
adapter that pins the validated generation and exact durable, query-visible
WAL suffix under the owner's synchronization. Core Engineering reviews
paging and capture release after the final reference drains. Storage also
traces readiness to a fresh, unlocked open relative to the pinned WAL
directory and verifies that the command cancels probes before closing the
descriptor. Storage supplies the command-level startup and drain evidence
for the reviewed composition. On both native Linux targets, Release
Verification needs exact held, fresh, and restart walks across replacement
and acknowledged backfill, plus readiness and ingest against the active
WAL filesystem after path retargets in both directions.
[Promotion evidence](../adr/0040-retained-runtime-constructor-boundary.md#runtime-evidence),
[Native release evidence](#release-evidence)

The Architecture workflow runs `go test -count=1 ./internal/archtest`.
The suite lists packages with `GOOS=linux`, `GOAMD64=v1`, `GOARM64=v8.0`,
`CGO_ENABLED=0`, `GOENV=off`, empty `GOFLAGS`, `GO111MODULE=on`, and
`GOWORK=off`, once for each target. Exact imports, cycles, package kinds,
and server reachability are checked against both inventories. Each direct
production edge has one registry declaration. The diagram
fitness test checks every declared production node and directed edge, the
Runtime closure, and Staged membership against the same registry. Test
imports and nested modules are outside the root-module production registry.
Boundary checks walk declared imports and each measured Linux graph
independently. The measured walk also follows external production imports.
[Diagram fitness test](../../internal/archtest/system_diagram_test.go)

- When any of checkpoint, history, or backup has the runtime library role
  or appears in the measured server closure, `cmd/bog` must call
  `readiness.NewHandlerForDirectory` and exclude every reference to
  `readiness.NewHandler`.
  When history meets either condition, `cmd/bog` must
  also call `httpapi.NewWithPageSourceAndQueryResources` with a source
  that implements `PageSourceScanBound`. The typed check excludes
  compatibility constructors.
  The typed scan checks the selected production Go files independently
  for Linux amd64 and arm64. Direct calls count inside command-local
  wrappers and through aliased or dot imports. The fully staged baseline
  may use the compatibility constructors.
  [Constructor calls](../adr/0053-retained-serving-constructor-calls.md),
  [Constructor fitness test](../../internal/archtest/retained_runtime_wiring_test.go)
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
- Backup and checkpoint/history have no production import paths to each
  other in either Linux inventory, including through relays. History can
  import checkpoint, and the offline operator can import backup.
  [Recovery-view fitness fixtures](../../internal/archtest/recovery_view_boundary_test.go)
- Command roots enforce server, operator verification, and operator export
  exclusions. The measured Linux graph checks those exclusions and the
  oracle and three release producer boundaries through external production
  dependencies as well. Both the registry and measured Linux graphs require
  the operator's direct backup edge for offline verification.
  [Server boundary](../../internal/archtest/command_roots_test.go),
  [Operator boundary](../../internal/archtest/operator_boundary_test.go),
  [Exporter boundary](../../internal/archtest/export_boundary_test.go),
  [Oracle boundary](../../internal/archtest/verifier_boundaries_test.go),
  [Release evidence boundary](../../internal/archtest/release_evidence_boundary_test.go)
- Stream policy conformance runs outside every registered production command
  dependency tree. Its own production imports cannot reach internal packages
  or command roots, including through external production dependencies.
  The engine cannot reach other root-module packages, and operator and
  evidence packages must stay independent of it.
  [Conformance boundary](../../internal/archtest/stream_policy_boundary_test.go),
  [Engine boundary](../../internal/archtest/stream_policy_engine_boundary_test.go)
- The release and capacity producers are the only production consumers of
  the listener helper. The check follows indirect imports as well.
  [Candidate listener boundary](../../internal/archtest/listener_boundary_test.go)
- `verify/release` and `bench/retained-history` must both import
  `verify/capacityevidence` directly in production. The helper's one
  root-module import supports the nested benchmark's publication wrapper.
  The validator's reachability is limited to those two producers, including
  when the Linux runner publishes delegated evidence.
  [Capacity evidence boundary](../../internal/archtest/capacity_evidence_boundary_test.go)
- The release, Linux, and retained-history producers and the compatibility
  wrapper import `verify/reportpublication` directly. It has zero root-module
  production imports. Both Linux inventories check the registered role and
  direct consumers and reject reachability from other root-module
  production packages through registered or external intermediaries.
  [Report publication boundary](../../internal/archtest/report_publication_boundary_test.go)
- Standalone libraries require a production command consumer. Every staged
  package requires an owner and activation criteria. Declarations for
  removed or reclassified packages fail the suite.
  [Standalone consumer check](../../internal/archtest/standalone_consumers_test.go),
  [Staging declaration check](../../internal/archtest/staged_packages_test.go)
