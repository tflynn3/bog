# ADR 0055 — Retained query resource construction

**Status** — Proposed for Cycle 210. Core Architecture records the
contract and constructor fitness check. Coordinate its adoption with
`bog-storage` in [#108](https://github.com/tflynn3/bog/issues/108).
Runtime promotion and native Linux qualification are pending.

## Serving contract

Keep retained paging in the existing single-writer Linux process, HTTP
listener, and pinned data directory under the WAL owner's exclusive lock.
`cmd/bog` supplies a command-local adapter implementing both
`httpapi.PageSource` and `httpapi.PageSourceScanBound`. The adapter
captures the validated generation and the matching durable, query-visible
WAL suffix together under WAL owner synchronization. `Page` meters
acquisition and scanning with the supplied `store.QueryWork` and
reproduces rows, positions, and exhaustion for the same captured request.
The source cleans up failed acquisitions. HTTP takes ownership of
idempotent `Release` after successful acquisition and holds the capture
through the final admitted user's response delivery.
[Retained capture and delivery](0039-query-delivery-and-retained-cutover.md)

Build the issuing API with
`httpapi.NewWithPageSourceAndQueryResources(source, appender, maxBytes, concurrency, workBudget, limits)`.
The command supplies the per-query response byte cap, concurrency and
work limits, and explicit per-issuer `httpapi.QueryResourceLimits`.
Record the effective settings for the composed service. HTTP admits
against three independent aggregate limits.

| Limit | Charge |
| --- | --- |
| `ResponseBytes` | Encoded record backing across queries, including pages under construction and through response delivery |
| `ScanBytes` | Source scan reservations from before each `Page` call through consumption of its result |
| `Captures` | Capture slots from before acquisition through final source release, subject to the issuer's registry ceiling of 64 |

Each limit is inclusive. Zero disables admissions that need that
resource. HTTP returns 503 when resource admission fails during page
preparation, before sending NDJSON or a new cursor.

`PageScanBytes(PageRequest) uint64` supplies a conservative peak for a
single call before HTTP admits it. Computing the bound is concurrency-safe
and performs no scan or capture acquisition. It stays valid if the view
changes before the call starts. Include simultaneously live scratch and
result backing, including seek and merge structures, decode buffers,
row slice capacities, copied lines and positions, and borrowed buffers
held through result consumption. A zero bound promises no chargeable scan
buffers. Use `math.MaxUint64` to refuse admission when a safe bound cannot
be expressed. The explicit constructor also refuses scans if the source
lacks the bound interface.
[HTTP resource admission](../../internal/httpapi/query_resources.go)

Storage budgets shared retained data and capture-owned state, including
decoded tail backing and pinned generations and suffixes. Storage also
owns replay and whole-batch append reservations, compaction, export,
staging, and reclamation. Account for overlapping operations within the
same service envelope. Expiry and retirement bar new resumes while
admitted users hold their references. Reclaim covered data only after
durable selection and the last storage reference drains.
[Storage resource admission](0010-retained-resource-admission.md)

## Constructor fitness and review

This changes the paging row of ADR 0053. When `internal/history` is
registered as wired or measured reachable from `cmd/bog` on a target,
require a direct typed call to `httpapi.NewWithPageSourceAndQueryResources`
in that target's selected production command sources. Check Linux amd64
and arm64 independently. Reject every typed reference, including a
function value, to `httpapi.NewWithPageSource` and the store-backed
`httpapi.New`, `httpapi.NewWithQueryMaxResponseBytes`,
`httpapi.NewWithQueryLimits`, and `httpapi.NewWithQueryResources`.

The staged baseline permits compatibility constructors. When any of
checkpoint, history, or backup is wired or measured reachable, require a
direct typed call to `readiness.NewHandlerForDirectory` and reject typed
references to `readiness.NewHandler`. Checkpoint or backup activation
alone still permits compatibility paging. Readiness uses an independent,
unlocked open relative to the pinned WAL directory.
[Retained serving constructor calls](0053-retained-serving-constructor-calls.md),
[Constructor fitness check](../../internal/archtest/retained_runtime_wiring_test.go)

The guard establishes call presence. For #108, trace the installed HTTP
handler to the command adapter and verify both interfaces. Review the
scan bound against the adapter's scratch and result allocations and the
configured resource limits against overlapping serving and maintenance
work. Trace capture ownership through retirement, response delivery, and
shutdown. Verify the pinned readiness descriptor and startup cleanup,
and retain the WAL lock through successful HTTP, API, and storage drain
or until process exit if drain fails.

Queue the matching `SYSTEM.md` wording, topology, and role changes with
the #108 promotion after the current documentation lease clears. Promote
using both production inventories of the reviewed Storage revision and
the activation evidence in ADR 0049. Rerun architecture fitness on the
composed revision. Qualify that same clean revision on native Linux amd64
and arm64 using each runner's SHA-256-pinned binary, including the live
capacity and pin-drain evidence under the release memory cap.
[Retained runtime promotion](0049-retained-runtime-promotion.md)
