# ADR 0046 — Live compaction observation

**Status** — Proposed for Core Architecture review.

## Decision

Release Verification owns one live reply decoder in
`verify/capacityevidence`. The release test adapter and retained-history
benchmark pass an already-bounded reply from `/_capacity/compaction`.
The decoder returns the candidate's status and the raw optional metrics
value. The callers own HTTP bounds, request cancellation, and the
workload checks that give each observation meaning.

A reply is one JSON object with no trailing value. The names `state`,
`generation`, `covered_rows`, and `committed_rows` match exactly and
their values must be present and non-null. State is `running` or
`complete`. Generation is an unsigned 64-bit integer. Row counts are
nonnegative integers within the verifier's integer range, and committed
rows must be at least the covered rows. Explicit zero is valid, including
the initial predecessor. Every envelope name can occur only once,
including `metrics` and unknown extension names. Name comparison follows
JSON string decoding, so an escaped spelling of the same name also
counts as a duplicate. Unique extensions are ignored.

The benchmark applies its existing metric diagnostics to the raw value.
An omitted, null, or unusable metrics value supplies no observations
for the affected metrics, and valid siblings of malformed or repeated
metric members still count. The release adapter requires a well-formed
object with an explicit unsigned `retired_generation_pins` count for pin
observations. Publication-only observations can omit metrics. An invalid
envelope supplies no candidate observation. The benchmark records that
failure alongside earlier timeline entries and independent measurements.

## Compatibility and qualification

The stricter live envelope rejects snapshots that previously could
supply a mis-cased field or resolve repeated fields by taking the last
value. Keeping metrics raw preserves diagnostics for individual fields
and permits additive producer extensions. Historical schema-v1 reports
retain their existing validation contract. Strict cutover proof still
requires ordered observations of publication at the requested cutoff,
held-cursor replay, explicit retirement, and pin drain.
[Runtime metric semantics](0023-retained-runtime-observation.md),
[Capacity observation validation](0028-capacity-observation-validation.md),
[Capacity validation ownership](0044-capacity-validation-ownership.md)

The shared decoder remains verifier support and uses the standard library.
Release Verification qualifies the composed #108 revision with the
pinned candidate binary on each native Linux runner, amd64 and arm64.
Storage owns runtime wiring and package promotion in the #108 PR.
[Release evidence boundary](0021-retained-release-evidence-boundary.md),
[System architecture](../architecture/SYSTEM.md#release-evidence)
