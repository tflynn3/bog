# ADR 0056 — Pinned query compaction observation

**Status** — Proposed for Core Engineering Query and Release Verification review.

## Private observation contract

Core Engineering Query owns the pinned query oracle's publication checks.
Release Verification owns `verify/capacityevidence.DecodeCompactionStatus`
and its wire contract in ADR 0046. The Linux test adapter passes only
bounded HTTP 200 replies to that decoder. The shared decoder validates
the envelope and returns raw optional metrics. The adapter retains the
8 KiB body limit, identity encoding, exact advertised length check, and
body close error handling. Only the fixed-budget capability probe treats
HTTP 404 as a legacy candidate without retained compaction control.
[Live compaction observation](0046-live-compaction-observation.md)

Both scenarios use one start-and-poll helper with a completed predecessor,
an advancing cutoff, and an expected committed row count. The cutoff may
equal the expected committed row count. Each running observation must
retain the predecessor generation and covered rows. Each completed
observation must name a newer generation and cover the requested cutoff.
Every reply must report the expected committed rows. Even an immediately
completed start requires a matching status observation of state,
generation, and row counts. Metrics can change between those replies.
The helper polls immediately and then every 20 milliseconds while running.
Each publication shares a five-second context with its requests, bounded
by the scenario deadline. The tail-only pre-start status request uses this
same context. Cancellation interrupts both HTTP and polling waits.

## Scenario evidence

The fixed-budget trace starts at an empty completed publication, then
uses the last confirmed generation as the predecessor for the next
publication. The acknowledged cutoff is also the expected committed row
count, so each publication fully covers the acknowledged history. The
trace still measures the adjacent rejection and issuing budgets and
checks held and fresh cursor walks through exact terminal replay.
[Fixed-budget replacement evidence](0030-fixed-budget-replacement-evidence.md),
[Exact terminal replay](0036-query-replay-wire-evidence.md)

The tail-only trace reads the predecessor after the cycle's appends and
requests a cutoff below its committed end. The append-log oracle checks
the completed counts against the acknowledged batches and verifies a
nonempty suffix on both replacements. It checks held cursor replay across
both replacements, the fresh walk, explicit cursor retirement and refusal,
and the retired pin count before and after drain. Pin samples use the same
status decoder and require an observable `retired_generation_pins` value
from the raw metrics. The on-disk fixture check still requires tail-only
authority.

The native query-model race suite runs the wire fixtures and the pinned
traces. Release Verification qualifies the composed #108 revision with
each runner's pinned candidate on native Linux amd64 and arm64. The live
capacity gate supplies the corresponding 128 MiB cgroup evidence with
swap disabled. Storage owns the system architecture and promotion updates
in its runtime wiring PR.
[Native Linux release workflow](../../.github/workflows/linux-release.yml),
[Retained runtime promotion](0049-retained-runtime-promotion.md)
