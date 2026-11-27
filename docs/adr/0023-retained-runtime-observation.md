# ADR 0023 — Retained runtime observation

**Status** — Proposed for the Cycle 111 integration review.

## Ownership and observation boundary

Release Verification in Core Engineering owns the capacity timeline and
its interpretation. Storage owns the producer in the #53 command composition.
Use the existing opt-in loopback `/_capacity/compaction` control and its
`metrics` object on start and status replies. Capture the metrics in the
same candidate snapshot as the compaction state, published generation, and
covered and committed row counts. The command retains one process, one
listener, one WAL authority, and one pinned data directory. The verifier
observes the candidate over its existing HTTP boundary. [Runtime cutover](0015-retained-history-runtime-cutover.md), [Release evidence boundary](0021-retained-release-evidence-boundary.md)

| Metric | Wire value and meaning |
| --- | --- |
| `pending_compactions` | An unsigned 64-bit JSON integer counting accepted unfinished compaction work, including queued and running work. Zero means the snapshot observed no pending work. |
| `oldest_pinned_generation` | A 64-character lowercase hexadecimal manifest SHA-256, the Storage generation ID. Select the earliest-published generation with a live pin, including retired generations that still have live references. JSON null means the snapshot observed an empty live pin set. |

The producer determines liveness and publication order. The manifest
digest identifies the selected generation. The existing numeric
`generation` field continues to order publications in the workload.
The producer can omit either metric when it cannot observe it. The
timeline records the fields under `candidate_status_metrics` and records
an individual `unavailable` reason for omitted or malformed fields.
A failed or skipped status probe marks both unavailable. In new reports,
a null oldest generation with no matching unavailable reason represents
an observed empty pin set. Older reports can omit the field entirely.
An explicit zero for pending work remains a numeric zero in the report.

## Compatibility and proof

Release Verification owns the shared runtime metric fields and their value
semantics in `verify/capacityevidence`. The retained-history benchmark uses
that contract when decoding live snapshots and supplies the diagnostic
reasons for missing or malformed fields and failed probes. The strict
report validator uses the same contract and decides whether an observation
is consistent with its timeline sample.

All runtime counters accept unsigned 64-bit JSON integers, including zero.
A missing or null counter supplies no measurement. A malformed live field
is recorded as unavailable while valid siblings are retained. A malformed
known field in a strict report fails validation. Both readers ignore
unknown metrics.

The fields are optional schema-v1 diagnostics. Legacy candidates and
historical reports remain compatible, at the cost of less information
when investigating an old run. A numeric observation or a generation digest
requires a successful status snapshot and cannot carry a matching
unavailable reason. The report reader distinguishes an omitted oldest
generation from an explicit null. A null with a matching reason or on a
skipped or failed snapshot supplies no pin-set observation. Sample order,
polling, deadlines, and memory read bracketing follow the existing capacity
proof. A positive `retired_generation_pins` count while the held capture is
live followed by zero after explicit retirement still establishes
predecessor drain.
The oldest live generation can still be present after the predecessor
drains. Filesystem staging bytes, pin record counts, and pin record
modification ages describe the fixture directory. Runtime pin evidence
comes from the candidate snapshot. [Capacity evidence validation](0018-capacity-evidence-validation.md), [Shared capacity evidence](0020-shared-capacity-evidence.md)

## Correlation with durability evidence

Within each native Linux job, match `revision`, `binary_sha256`, and
`fixture` across `capacity.json` and `durability.json`. Compare the
capacity failure phase and adjacent status samples with the cgroup
`memory.current` bracket, final `memory.peak`, and OOM events. Pending
work across samples can help locate a publication delay. A live pin on
an early publication can explain why some retired data remains protected.
Use the durability case's readiness and ingest observations to check
for disk headroom refusal, WAL rollback, or uncertain commit, and the
restart ledger to check what survived. Interpret elapsed times within
each report's own phase or case. The producers use separate processes
and fixtures, so matching build identity supports comparison across
scenarios without establishing event simultaneity or a causal link.
[Durability diagnostics](../../verify/linux/README.md#cutover-diagnostics), [System release evidence](../architecture/SYSTEM.md#release-evidence)

Recheck leases at dispatch and at the Storage handoff. Review the
producer and consumer contract on the composed #53 revision and qualify
that revision on native Linux amd64 and arm64 using the pinned binary
for each job. [#53 Storage handoff](https://github.com/tflynn3/bog/issues/53#issuecomment-7209993740)
