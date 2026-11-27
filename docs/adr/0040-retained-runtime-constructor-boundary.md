# ADR 0040 — Retained runtime constructor boundary

**Status** — Proposed for Cycle 176. Core Architecture owns this decision.

## Decision

`cmd/bog` owns the retained storage composition. It injects its retained
`PageSource` through `httpapi.NewWithPageSource`. The source captures the
selected generation and its exact committed WAL suffix under the owner's
synchronization. HTTP owns paging admission, cursor lifecycle, and delivery.
[Runtime cutover](0015-retained-history-runtime-cutover.md),
[Command-owned storage composition](0026-command-owned-retained-storage-composition.md)

The command gives `readiness.NewHandlerForDirectory` an independently
opened, unlocked descriptor for the WAL owner's pinned directory. Open the
descriptor relative to the pinned directory so readiness keeps sampling the
active filesystem after a path retarget. A duplicate of the WAL descriptor
would share its lock. The command owns the probe descriptor and cancels the
probes before closing it.
[Readiness descriptor handoff](0005-pinned-writable-recovery-authority.md#readiness-descriptor-handoff-60)

## Fitness check

The rule activates when the existing package registry marks all three of
`internal/checkpoint`, `internal/history`, and `internal/backup` as runtime
libraries. The staged baseline may still use the compatibility constructors.
After activation, check the production Go files selected for `cmd/bog`
on Linux amd64 and arm64 using the architecture suite's build settings.
For each target independently, require at least one typed reference to
the package-level function `httpapi.NewWithPageSource` from
`bog/internal/httpapi` and at least one to
`readiness.NewHandlerForDirectory` from `bog/internal/readiness`. Report
each missing constructor with its target.
Reject references to `httpapi.New`, `httpapi.NewWithQueryMaxResponseBytes`,
`httpapi.NewWithQueryLimits`, and `readiness.NewHandler`, and report
their file, position, and constructor. Both checks use the defining
import path and function identity. Calls and function values count,
including references through aliased and dot imports. Only selected
production files in `cmd/bog` contribute references.

The existing fitness suites continue to enforce dependency direction,
cycles, package reachability, and staging declarations. Constructor
selection complements those checks because both implementations import the
same HTTP and readiness packages. The constructor rule checks source
references.
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

Storage owns the #108 runtime wiring, role promotions, and measured
topology. Rerun the architecture suite on the composed #108 SHA after
the registry promotes all three libraries. Review this rule on that
revision with Storage checking source and descriptor ownership and
Query checking the `PageSource` and paging lifecycle. Release
Verification owns native amd64 and arm64 qualification of that
revision, including exact live and restart walks at 196,608 and
393,216 rows under 128 MiB with swap disabled.
[Storage cutover](https://github.com/tflynn3/bog/issues/108),
[Query delivery and release handoff](0039-query-delivery-and-retained-cutover.md)
