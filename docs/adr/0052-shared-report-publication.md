# ADR 0052 — Shared verifier report publication

**Status** — Adopted for shared report publication and delegated Linux capacity.

## Context

Release and retained-history originally shared a private replacement
transaction with the nested `bench/segment-layout` module. The Linux
delegated runner opened both output parents before the workload and used
a separate transaction. Its early absence checks reject stale outputs.
A writer can still create either entry before commit, so delegated
outputs require atomic create-only semantics.
[Shared capacity evidence](0020-shared-capacity-evidence.md),
[Runner destination checks](../../verify/linux/runner_destinations.go)

## Transaction boundary

The shared `reportpublication.Publish` transaction takes a context, an
already-open pinned parent, a destination basename, encoded bytes, and
an explicit replacement or create-only mode. The basename is one
filename component, excluding an empty name, `.`, and `..`. The transaction
returns the publication state and an operation error. The caller owns the
parent and must keep it open for the whole call. The transaction owns each staging file
it successfully creates and its staging entry until commit. All operations
use names relative to the same parent. The transaction creates an exclusive
staging file, writes all bytes, syncs and closes the file, commits the
destination entry atomically, and syncs the parent. It checks cancellation
before staging and immediately before commit. Cancellation racing with
commit may arrive after the last check. A successful commit always leads
to a directory sync attempt, even if cancellation has arrived.

| Outcome | Meaning |
| --- | --- |
| Unpublished | This attempt did not install the destination. It attempts to remove its own staging entry if one was created and retains any removal error other than an already absent entry. |
| Published, durability uncertain | Commit installed the entry, but parent sync failed. The destination remains in place and the sync error is returned. |
| Durably published | Commit and both syncs succeeded. A subsequent close error carries this state. |

The states describe this writer's commit. Another writer can later change
the entry where replacement is allowed. Preserve the causal errors from
the operation, staging close, staging cleanup, and caller-owned directory
close.

`WriteReport` still encodes indented JSON with a trailing newline before
opening the parent. It splits the path without cleaning the parent spelling
so the filesystem resolves symlinks and dot segments in order. It uses
replacement semantics, mode `0600` at staging creation, and the existing
16-attempt collision budget. It closes the parent it opened and wraps
errors with the original path and actual publication state. Release and
retained-history encode their reports and call the shared replacement
entry point. The capacity evidence package keeps its compatibility API for
the nested benchmark.
[Shared transaction](../../verify/reportpublication/publication.go),
[Compatibility wrapper](../../verify/capacityevidence/report_publication.go)

## Delegated adoption

`verify/reportpublication` is verifier support. Each producer owns
encoding, schema, candidate identity, and evidence acceptance. Standalone
release, retained-history, segment-layout, and runner preflight publication
use replacement.

Delegated capacity requires create-only commits for both supervisor evidence
and the proof emitted by the capacity producer. Keep the early absence and
alias checks for diagnostics. The runner passes `-report-create-only` and its pinned proof parent on
descriptor 3. The producer uses that descriptor directly. Enforce absence
at the commit point with descriptor-relative Linux
`renameat2(RENAME_NOREPLACE)` in the pinned parent. An existing entry,
including a symlink, yields an unpublished outcome with an
`errors.Is(err, os.ErrExist)` cause. An unsupported operation fails
publication.

The supervisor retains ownership of each pinned parent until its last use,
through workload completion and proof reading for the proof parent, and
through final publication for the report parent. The producer owns the
handles it opens or receives in its own process. Each caller closes its
own handles once and joins close errors with the relevant operation error.
Destination identity and executable alias checks stay at the runner.
After a successful producer exit, the supervisor opens the expected regular
file relative to the pinned parent and reads at most 4 MiB of proof bytes,
with one additional byte to detect oversize input. Producer exit failure and
publication conflicts fail the delegated run before proof decoding.
[Runner transaction](../../verify/linux/runner_report.go),
[Proof descriptor handoff](../../verify/linux/capacity_runner_linux.go)

## Adoption fitness

Add a deterministic competing-writer fixture for each delegated output.
Pass the early absence check, pause the publisher after staging is synced
and closed, then install a distinct entry through the pinned parent before
resuming commit. Require an unpublished existence error, exact preservation
of the competing bytes, removal of the publisher's staging entry, and a
still-usable caller-owned directory. Include a proof conflict with a
schema-valid competing report and require the delegated verdict to fail.
Exercise successful create-only commit and directory sync failure, and
retain the standalone replacement, path-retarget, cancellation, and alias
fixtures.

The Linux amd64 and arm64 inventories register `verify/reportpublication`
as verifier support with zero root-module production imports. The direct
consumers are `verify/release`, `verify/linux`, `bench/retained-history`, and
the compatibility wrapper in `verify/capacityevidence`. The validator's
only production consumers remain release and retained-history. The nested
module is outside the root-module inventory.

Fitness tests check the publisher's role, exact imports, and permitted
reachability on both Linux graphs. Fixtures cover forbidden paths through
registered and external intermediaries from the server, offline
verification and export commands, and unrelated production packages.
[Architecture registry](../../internal/archtest/architecture_test.go),
[Capacity evidence fitness](../../internal/archtest/capacity_evidence_boundary_test.go),
[Verifier consumer fitness](../../internal/archtest/verifier_consumers_test.go),
[System architecture](../architecture/SYSTEM.md#fitness-and-registry-ownership)

Release Verification owns implementation and the delegated fixtures.
Core Architecture owns the registry, diagram, and fitness changes.
The registry, system diagram, and caller adoption land together.
