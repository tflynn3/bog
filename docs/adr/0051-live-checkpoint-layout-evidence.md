# ADR 0051 — Live checkpoint layout evidence

**Status** — Proposed for Storage review.

## Decision

Release Verification supplies the reviewed #108 candidate SHA after the
retained runtime handoff. The segment layout workflow builds that clean
revision on native Linux amd64 and arm64 using the release build flags.
Each architecture qualifies the same candidate binary for the mixed and
entropy profiles and records its embedded Git revision and binary
SHA-256. The benchmark source is pinned separately at the workflow SHA.
The first full live run depends on the reviewed #108 candidate.

Each schema v8 report pairs the production v1 checkpoint component with
the experimental layouts on the same input. It adds live compaction
measurements through the candidate's opt-in IPv4 loopback control. Both
profiles must demonstrate exact cutoff advancement, ordered HTTP query
parity, and full persisted row parity for clustered and scattered
backfills. PR smoke and historical report validation remain available.

## Storage decision

Storage owns the layout decision after cutover. It will compare stored
segment and manifest bytes, paired cold scan p95, peak decode working
memory, and backfill write amplification on both architectures and
profiles. The component and experimental rewrite counters report logical
writes. The candidate's live ratio uses Linux process-attributed writes
over the compaction interval with the same committed WAL byte denominator.
Storage can use the live result to judge whether the component trade-offs
justify a format proposal. This evidence track adds no release gate or
format migration requirement to #108.

[Benchmark and handoff](../../bench/segment-layout/README.md),
[Retained runtime promotion](0049-retained-runtime-promotion.md),
[Live compaction observation](0046-live-compaction-observation.md)
