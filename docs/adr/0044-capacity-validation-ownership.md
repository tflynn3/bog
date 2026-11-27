# ADR 0044 — Capacity validation ownership

**Status** — Implemented for Cycle 184. Native qualification of the
composed #108 revision is pending.

## Decision

Release Verification owns the schema-v1 capacity wire types and verdicts
in `verify/capacityevidence`. Both existing verifier commands consume
that package. It checks the capacity fixture, scales, and evidence.
`verify/release` continues to match the report identity to the requested
revision, candidate binary SHA-256, and fixture, and to validate companion
reports and qualification events.

| Entry point | Required evidence |
| --- | --- |
| `ValidateRecord` and `ValidateReport` | The benchmark's live cutover proof for a single record and for both scales, respectively. Explicit readbacks, restart, cleanup, and ordered publication and cursor observations remain required. Correlation is validated when supplied and is mandatory with the correlated entry points. |
| `ValidateReleaseCutoverReport` | The benchmark report proof plus readiness and mixed-load timing, sample counts, RSS, descriptors, disk headroom, and per-device I/O at both scales. |
| `ValidateHistoricalReport` | The standard release contract for both live scales, including the release measurements, completed compaction, restart row count, and process and cgroup cleanup. Reports that predate the cutover diagnostics, cgroup readbacks, and fixture store removal verdict remain readable. |

[Capacity evidence validation](0018-capacity-evidence-validation.md),
[Shared capacity evidence](0020-shared-capacity-evidence.md),
[Strict retained capacity proof](0033-strict-retained-capacity-proof.md)

## Compatibility and trade-offs

Schema version 1 and the wire shape stay the same. Benchmark validation
leaves the release measurements opaque so producer and benchmark verdicts
remain stable. Standard release validation leaves cutover diagnostics
opaque so historical diagnostic extensions remain readable. Strict release
validation decodes the measurements after the benchmark proof succeeds.
This additional decoding preserves the benchmark contract and gives
proof failures precedence over measurement failures.

The shared package remains verifier support with no root-module
production dependencies. File access, candidate identity, companion
artifacts, and native runner orchestration belong to the entry points.
The cross-validator fixture checks the shared proof, and focused tests
cover release measurement failures and historical compatibility.
Native Linux amd64 and arm64 runs still qualify the composed #108
candidate with each runner's pinned binary.

[System architecture](../architecture/SYSTEM.md#release-evidence)
