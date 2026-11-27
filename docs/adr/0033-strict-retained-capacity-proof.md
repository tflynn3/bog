# ADR 0033 — Strict retained capacity proof

**Status** — Cycle 139 integration decision for Release Verification.

## Decision

Both strict capacity entry points use the shared schema-v1 proof in
`verify/capacityevidence`. The benchmark checks each live record through
`ValidateRecord` before reporting a pass. The benchmark's
`-validate-cutover` and release's `validate -require-cutover` check both
live scales through `ValidateReport`. Standard release validation continues
to accept historical schema-v1 capacity reports. Candidate identity and
companion reports remain the responsibility of the release command.
[Shared capacity evidence](0020-shared-capacity-evidence.md)

At each scale require `diagnostics.restart` with status `pass`, phase
`complete`, and an empty or omitted failure. Require explicit numeric
device and inode observations for the original fixture directory, the
path before restart, the restarted candidate, and the path after the
restarted walk. All four device/inode pairs must match. Require affirmative
first-candidate reaping and an empty fixture cgroup before restart.

The restart publication snapshot must match the record's completed
scattered compaction. The recovered checkpoint must report the same
complete state and generation, cover exactly seed rows plus the first
64-row tail batch, and report all seed and tail rows committed. The
acknowledged ledger must pass and reach exhaustion with no failure and
with a positive number of complete pages. Its acknowledged and matched
row counts must both equal seed rows plus 2,048. Its acknowledged batch
count must equal that total divided by 64. Its expected and matched WAL
suffix counts must both equal the nonempty difference between the total
and the checkpoint cutoff, or 1,984 rows.

For each clustered and scattered held walk, keep the terminal NDJSON
body within the existing HTTP response bound. Replay the terminal
continuation with its original cursor, filter, and page limit before
retirement. Validate the replay's rows and terminal boundary, then compare
the response bodies byte for byte. Record `terminal_page_bytes_equal: true`
and the successful `terminal_retried` event only after the comparison
succeeds. The strict proof requires this affirmative observation and the
existing ordered exhaustion, replay, retirement, and pin-drain evidence.
The report carries the verdict without embedding response bodies.
[Retained cursor retirement](0031-retained-cursor-retirement.md)

## Ownership and qualification

Release Verification owns the producer, shared validator, and fixtures.
The runtime composition for #53 and the Architecture registry handoff
follow their existing tracks. The native Linux release workflow qualifies
the composed revision on amd64 and arm64 at both live scales.
[Linux release workflow](../../.github/workflows/linux-release.yml)
