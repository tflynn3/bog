# ADR 0040 — Retained runtime constructor boundary

**Status** — The constructor fitness guard landed in
`b27b41ff9fcd46f3497725b052075e8353a0ede7`. Storage's #108 runtime wiring,
registry promotion, and native qualification are pending. Core Architecture
owns this decision.
[Landed constructor guard](https://github.com/tflynn3/bog/commit/b27b41ff9fcd46f3497725b052075e8353a0ede7)

## Decision

`cmd/bog` owns the retained storage composition. When history is server-wired,
it injects its retained `PageSource` through `httpapi.NewWithPageSource`.
The source captures the selected generation and its exact committed WAL
suffix under the owner's synchronization. Core Engineering owns the HTTP
paging contract, including admission, cursor lifecycle, and delivery.
[Runtime cutover](0015-retained-history-runtime-cutover.md),
[Command-owned storage composition](0026-command-owned-retained-storage-composition.md)

When any retained library is server-wired, the command gives
`readiness.NewHandlerForDirectory` an independently opened, unlocked
descriptor for the WAL owner's pinned directory. Open the descriptor relative
to the pinned directory so readiness keeps sampling the active filesystem
after a path retarget. A duplicate of the WAL descriptor
would share its lock. The command owns the probe descriptor and cancels the
probes before closing it.
[Readiness descriptor handoff](0005-pinned-writable-recovery-authority.md#readiness-descriptor-handoff-60)

## Fitness check

The package registry records server wiring through the runtime library
role. When all three retained libraries are staged, the command may use
the compatibility constructors. Apply each requirement as soon as its
registry condition holds.

| Runtime library in the registry | Required constructor | Excluded constructors |
| --- | --- | --- |
| Any of `internal/checkpoint`, `internal/history`, or `internal/backup` | `readiness.NewHandlerForDirectory` | `readiness.NewHandler` |
| `internal/history` | `httpapi.NewWithPageSource` | `httpapi.New`, `httpapi.NewWithQueryMaxResponseBytes`, `httpapi.NewWithQueryLimits` |

Checkpoint or backup can be promoted with compatibility paging while
history is staged. History promotion activates both requirements.
Evaluate each Linux target, amd64 and arm64, independently using the
architecture suite's build settings and selected production Go files in
`cmd/bog`. Both checks use typed references to package-level functions
identified by their defining import paths, `bog/internal/readiness` and
`bog/internal/httpapi`. Calls and function values count, including
references through aliased and dot imports. Report each missing
constructor with its target and each excluded reference with its file,
position, and constructor.

The existing fitness suites continue to enforce dependency direction,
cycles, package reachability, and staging declarations. Constructor
selection complements those checks because both implementations import the
same HTTP and readiness packages. The constructor rule checks source
references. Promotion also requires tracing the values passed to the
serving constructors and collecting the runtime evidence below.
[Constructor fitness check](../../internal/archtest/retained_runtime_wiring_test.go),
[System fitness settings](../architecture/SYSTEM.md#fitness-and-registry-ownership)

## Runtime evidence

- Trace the source passed to the serving HTTP API to the command-owned
  adapter. Check that it pins the validated generation and exact durable,
  query-visible WAL suffix together under WAL owner synchronization and
  releases the capture after its final user drains. On the pinned
  candidate, compare exact held and fresh walks through replacement and
  acknowledged backfill, then verify the exact fresh walk after restart.
- Trace the descriptor passed to readiness to a fresh, unlocked open
  relative to the WAL owner's pinned directory, and check that it
  identifies that directory. Check that the command cancels probes
  before closing its descriptor and that a stalled sample cannot delay
  HTTP or storage drain. The WAL owner holds its lock through drain.
  On the pinned candidate, retarget the configured path in both
  directions and verify readiness and ingest against the active WAL
  filesystem.

[Runtime interfaces and qualification](0015-retained-history-runtime-cutover.md),
[Native release evidence](../architecture/SYSTEM.md#release-evidence)

## Integration

Storage carries #47 and #15 in #108 and owns the runtime wiring. Recheck
write leases at dispatch and at the Storage handoff. Coordinate each role
promotion and its measured topology update with Core Architecture in the
reviewed command wiring PR. Promote a retained library when `cmd/bog`
reaches it in both Linux inventories and its activation criteria are met.
On each promotion and on the final composed #108 SHA, verify the registry
against both inventories and rerun the architecture suite. Storage reviews
source and descriptor ownership. Core Engineering reviews the `PageSource`
and paging lifecycle.
[Registry promotion handoff](../architecture/SYSTEM.md#fitness-and-registry-ownership)

#108 remains the release gate. Release Verification qualifies the same
composed revision on native Linux amd64 and arm64, including exact live
and restart walks at 196,608 and 393,216 rows under 128 MiB with swap
disabled.
[Storage cutover](https://github.com/tflynn3/bog/issues/108),
[Query delivery and release handoff](0039-query-delivery-and-retained-cutover.md)
