# ADR 0028 — Capacity observation validation

**Status** — Proposed for Cycle 121 review.

## Decision

Release Verification checks candidate runtime observations in the shared
schema-v1 capacity timeline. The producer's `ValidateRecord` and the
strict validators' `ValidateReport` apply the same rules to every sample
in recorded order, including running polls, phase boundaries, and drain
retries. [Shared capacity evidence](0020-shared-capacity-evidence.md),
[Retained runtime observation](0023-retained-runtime-observation.md)

| Reported field | Interpretation |
| --- | --- |
| Numeric runtime counters, including `pending_compactions` and `retired_generation_pins` | An unsigned 64-bit integer is an observation, including zero. A missing or null counter supplies no measurement. |
| `oldest_pinned_generation` | A lowercase hexadecimal manifest SHA-256 identifies the oldest live pinned generation. An explicit null in a successful snapshot without a matching unavailable reason records an empty live pin set. Omission, null with a matching reason, and legacy null placeholders on skipped probes supply no pin-set observation. |

An observed counter or generation ID requires a successful status
snapshot and cannot also carry a matching unavailable reason. A reason
for a different field can accompany a valid observation. Filesystem
records and runtime metrics describe different observation sources.

The strict proof still requires a positive `retired_generation_pins`
count after terminal replay while the capture is held, followed by a
numeric zero after explicit retirement and within the drain budget.
Pending work and the oldest live generation are diagnostic. Another
generation can remain pinned after the retired predecessor drains.
[Capacity evidence validation](0018-capacity-evidence-validation.md)

## Compatibility and trade-off

Keep schema version 1 and accept optional diagnostics omitted by
historical reports, including reports that otherwise satisfy the strict
cutover proof. Standard release validation continues to accept historical
live reports under its existing contract. Richer diagnostics can explain
publication delays and retained pins, and checking their consistency
keeps contradictory snapshots out of strict proof. Historical reports
with omitted diagnostics remain usable but provide less information for
investigating a run.
