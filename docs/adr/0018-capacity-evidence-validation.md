# ADR 0018 — Capacity evidence validation

**Status** — Accepted. Native qualification of the composed candidate is pending.

## Context

The native release gate passes the same `capacity.json` to
`bench/retained-history -validate-cutover REPORT` and
`verify/release validate -require-cutover -capacity REPORT` alongside the
release command's other report and identity arguments. Both commands run
from the gate checkout and consume evidence produced with the pinned
candidate binary. Each native Linux runner qualifies the same revision
using its own binary. [Release verifier composition](0008-release-verifier-composition.md),
[Release evidence process boundary](0013-release-evidence-process-boundary.md)

## Common cutover payload

Strict capacity evidence uses schema version 1, fixture
`linux-release-v1`, and mode `live`. The passing
`retained_history_capacity` case contains exactly one record for each of
196,608 and 393,216 seed rows. Each fixture uses 2,048 tail rows,
1,024-byte lines, and 64-row batches. Each record must report a complete
phase and passing record and workload statuses, with the JSON boolean
`"timed_out": false`. Missing, null, and true timeout verdicts fail.

- Run live compaction in a delegated cgroup v2 subtree. Record the 128 MiB
  memory limit and zero swap limit readbacks, a positive final memory
  peak at or below the limit, and zero OOM kills in both the record and
  the cgroup memory events.
- Record clustered and scattered acknowledged backfill timelines. Prove
  publication of an advancing generation at each exact requested cutoff,
  a held walk against the original capture including terminal-page replay,
  and an exact fresh walk after retirement. The record's completed
  compaction matches scattered publication at seed plus the first tail
  batch. The scattered fresh and restarted walks cover all seed and tail
  rows.
- Include at least two `memory.current` reads per timeline, including the
  backfill overlap observation. Bracket each read with running statuses
  for the same operation before publication. Each read must be positive
  and at most the final peak. Derive the reported sample count,
  observation span, and byte range from the reads.
- Observe a positive retired-generation pin count after terminal replay
  while the capture is still held, then zero after explicit retirement
  within the drain budget. Record clean startup and restart lifecycles,
  candidate reaping, subtree emptiness and removal, and fixture store
  removal.

[Publication and pin-drain sample contract](../../bench/retained-history/README.md#publication-and-pin-drain-sample-contract)

## Release validation and compatibility

The release command additionally matches the report's full revision,
binary SHA-256, and fixture identity to the requested revision, the digest
of the supplied candidate binary, and the requested fixture. Its existing
capacity checks also cover readiness timings, mixed-load timings and
sample counts, RSS, descriptors, disk headroom, and per-device I/O.
Strict release validation requires the companion recovery parity report.
[Release report validation](../../verify/release/README.md#live-capacity-evidence)

Standard release validation retains support for historical schema-v1
live capacity reports that predate the additive cutover proof. The
benchmark's legacy mode continues to record a baseline. Native cutover
qualification requires the complete live proof at both scales on each
architecture.

Release Verification owns the cross-validator contract test. It writes
the benchmark's schema-v1 fixture, applies wire-level mutations, and
passes each resulting report file to both strict validators. The shared
cases cover explicit false, missing, null, and true timeout verdicts
alongside the resource, timeline, and cleanup evidence.
