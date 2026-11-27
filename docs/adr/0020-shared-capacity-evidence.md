# ADR 0020 — Shared capacity evidence boundary

**Status** — Proposed for the composed #87 review.

## Context

The release gate consumes the same `capacity.json` in two commands.
Two implementations of the strict proof can disagree about what the
candidate demonstrated. Report publication can also lose its directory
identity if a configured path is retargeted between staging and rename.
[#87](https://github.com/tflynn3/bog/issues/87),
[Capacity evidence validation](0018-capacity-evidence-validation.md)

## Decision

`verify/capacityevidence` is verifier support with zero direct
root-module production imports. Its interface takes schema-v1 report
data and returns the strict capacity proof verdict. The two direct
production consumers are `bench/retained-history -validate-cutover` and
`verify/release validate -require-cutover`. The commands own file
handling and candidate identity checks. Release validation also owns
the additional report checks and requires affirmative writable-restore
parity, including an acknowledged append on the restored image and exact
augmented results before shutdown and after restart.
[Release evidence process boundary](0013-release-evidence-process-boundary.md)

Keep schema version 1, fixture `linux-release-v1`, live mode, and the
existing report shape. Strict validation requires the complete ADR 0018
proof at both live scales. Each record needs explicit boolean
`timed_out: false` and a present, numeric zero for the raw cgroup
`oom_kill` counter, consistent with the record's zero OOM count. Use one
timeline-ordering rule for both commands. Check samples in recorded
execution order, from running compaction and overlapping memory reads
through publication and the final cutoff, then cursor exhaustion and
terminal replay, capture retirement and observed pin drain, and fresh
visibility. Shared fixtures exercise valid proof and wire-level
mutations in both strict entry points. Standard release validation
continues to read historical live schema-v1 reports that predate the
additive proof. The benchmark retains legacy baseline mode.

## Directory-anchored publication

The release publisher opens the destination parent once. Create a unique
staging file exclusively in that directory, write the whole report, sync
and close the file, rename it over the destination, and sync the opened
directory. Use the same directory handle for staging, rename, cleanup,
and sync so retargeting the input path cannot redirect any of these
operations. A pre-rename failure leaves the previous report intact and
attempts to remove the staging file. After rename, a directory sync
failure reports that replacement occurred with uncertain durability.
A directory close error after successful sync reports a synced
replacement with a close failure. Preserve operation and cleanup
errors. Concurrent writers stage independently and each reports the
outcome of its own replacement.

## Ranked quality trade-offs

| Rank | Quality | Decision and cost |
| --- | --- | --- |
| 1 | Evidence integrity | Require observed measurements and one timeline rule. Missing strict proof fails qualification and calls for a new evidence run. |
| 2 | Durable report replacement | Use directory-relative operations and sync the file and directory. The extra I/O and distinct failure states make publication more expensive. |
| 3 | Schema compatibility | Preserve historical schema-v1 validation alongside strict cutover. The two modes retain separate compatibility tests. |
| 4 | Dependency isolation | Keep the helper verifier-only and identity at the entry points. The two commands retain their own report handling. |

## Integration and qualification

Architecture owns the registry and Release Verification owns the
validator and publisher. Land both parts of #87 together. The fitness
suite checks the two direct imports and rejects production reachability
from any other registered package, including through an intermediary.
Run the portable suite on the composed tree with the existing Linux
amd64 and arm64 inventories. Keep the current server package roles
until the separate #53 wiring is measured. Recheck write leases at
dispatch and at the #53 handoff, and rebase the later registry change
onto the first landed PR. Qualify one clean composed revision on both
native runners using each runner's SHA-256-pinned binary.
[Package role handoff](0019-package-role-governance.md#53-promotion-handoff),
[System architecture](../architecture/SYSTEM.md#fitness-and-registry-ownership)
