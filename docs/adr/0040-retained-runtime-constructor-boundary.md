# ADR 0040 — Retained runtime constructor boundary

**Status** — Proposed for Cycle 173. Core Architecture owns this decision.

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
Reject references to `httpapi.New`, `httpapi.NewWithQueryMaxResponseBytes`,
`httpapi.NewWithQueryLimits`, and `readiness.NewHandler`. Resolve each
reference to its defining import path, including aliased imports and
function values, and report its file, position, and constructor. Test
sources may exercise compatibility behavior independently.

The existing fitness suites continue to enforce dependency direction,
cycles, package reachability, and staging declarations. Constructor
selection complements those checks because both implementations import the
same HTTP and readiness packages. The constructor rule checks source
references. Runtime evidence establishes what the injected source serves
and which descriptor the command opens, uses, and closes.
[Constructor fitness check](../../internal/archtest/retained_runtime_wiring_test.go),
[System fitness settings](../architecture/SYSTEM.md#fitness-and-registry-ownership)

## Integration

Storage owns the #108 runtime wiring, role promotions, and measured
topology. Review this rule on the same composed revision with Storage
checking source and descriptor ownership and Query checking the
`PageSource` and paging lifecycle. Release Verification owns the native
amd64 and arm64 qualification of that revision, including exact live and
restart walks at 196,608 and 393,216 rows under 128 MiB with swap disabled.
[Storage cutover](https://github.com/tflynn3/bog/issues/108),
[Query delivery and release handoff](0039-query-delivery-and-retained-cutover.md)
