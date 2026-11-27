# ADR 0054 — Delegated capacity proof outcome

**Status** — Proposed for Core Architecture review.

## Process contract

`verify/linux` delegates the live capacity workload to
`bench/retained-history` as a subprocess. The producer receives the pinned
proof parent on descriptor 3 and publishes with the create-only transaction
in ADR 0052. The process exit attests to this writer's publication.

| Producer exit | Supervisor behavior |
| --- | --- |
| 0 | Read the committed proof. Require a passing live case with both passing scales. |
| 20 | Read the committed diagnostic proof. Require a failed live case with at least one failed candidate workload record. The delegated run still fails. |
| Any other exit, a signal, or supervisor cancellation | Fail without accepting producer records. Retain live sampling and process diagnostics. |

Reserve exit 20 for a delegated candidate workload failure after successful
create-only commit, file and directory sync, and publication cleanup,
including closure of the inherited proof parent. Publication conflict,
uncertain durability, and publication cleanup failure must use a different
nonzero exit, even when a workload failed or a file exists at the proof
name. Standalone producer invocation keeps its existing exit behavior.
The supervisor trusts the exit attestation from the invoked producer in the
runner-owned directory. File presence alone cannot establish publication.
[Shared publication transaction](0052-shared-report-publication.md)

## Evidence acceptance

For either eligible exit, open the regular proof relative to the pinned
parent and apply the existing 4 MiB bound. Require schema version 1, live
mode, the expected case, and the candidate revision, binary SHA-256, and
fixture pinned by the supervisor. Accept each known scale at most once.
Check the whole handoff against the exit before merging any records.
Provisioning, cgroup placement, missing kernel accounting, and cleanup-only
failures do not by themselves substantiate exit 20. An explicit
`workload_status` must support the record's use as workload evidence.

Preserve per-scale phase, diagnostics, and cleanup observations with their
producer or live-sample attribution. Leave missing scales unobserved or
represented by live samples. The diagnostic proof keeps the release
qualification failed. The schema-v1 release validators
and the strict cutover observation requirements remain as defined in
ADR 0044. The supervisor and producer communicate through the subprocess
and proof file. Production import paths between them are forbidden in
both directions, including through intermediaries.
[Capacity validation ownership](0044-capacity-validation-ownership.md)

## Rollout

Core Architecture reviews the process and proof contract. Release
Verification lands supervisor support first and exercises successful
publication with the old producer contract. The existing #135 owner then
adds the producer exit and tests that conflicts, sync failures, and
publication cleanup errors cannot emit it. Native Linux amd64 and arm64
qualification of the same composed #108 revision remains part of the
release workflow.
