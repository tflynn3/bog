# ADR 0052 — Shared verifier report publication

**Status** — Private transaction extracted for Cycle 201. Shared package
and delegated runner adoption are proposed for a follow-up PR.

## Context

`verify/release` and `bench/retained-history` publish standalone reports
through `capacityevidence.ReportPublisher.WriteReport`. The nested
`bench/segment-layout` module also uses this API. The Linux delegated
runner has a separate transaction and opens its report and capacity proof
parents before starting the workload. Its early absence checks can reject
stale outputs, but another writer can create either entry before the final
rename. Ordinary replacement would overwrite that writer's evidence.
[Shared capacity evidence](0020-shared-capacity-evidence.md),
[Runner destination checks](../../verify/linux/runner_destinations.go)

## Transaction boundary

The private Go transaction takes a context, an already-open pinned parent,
a validated destination basename, and encoded bytes. The basename is one
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
errors with the original path and actual publication state. The release and
retained-history callers already reach the transaction through this API.
[Transaction and compatibility wrapper](../../verify/capacityevidence/report_publication.go)

## Delegated adoption

Move the transaction and directory-relative publication operations into
`verify/reportpublication` as verifier support independent of report
schemas and validation. The shared transaction takes encoded bytes. Each
producer owns encoding, schema, candidate identity, and evidence
acceptance. Keep standalone release, retained-history, segment-layout, and
runner preflight publication on explicit replacement. Migrate the nested
benchmark's publishing call or keep a compatibility wrapper backed by the
shared package.

Delegated capacity requires create-only commits for both supervisor evidence
and the proof emitted by the capacity producer. Keep the early absence and
alias checks for diagnostics. Pass the producer an explicit create-only
mode along with its inherited proof-parent descriptor. Enforce absence
at the commit point with descriptor-relative Linux
`renameat2(RENAME_NOREPLACE)` in the pinned parent. An existing entry,
including a symlink, yields an unpublished outcome with an
`errors.Is(err, os.ErrExist)` cause. An unsupported operation fails
publication.

The supervisor retains ownership of each pinned parent until its last use,
through workload completion and proof reading for the proof parent, and
through final publication for the report parent. The producer owns the
handles it opens or receives in its own process. Each caller closes its own handles once
and joins close errors with the relevant operation error. Keep the
destination identity and executable alias checks, and keep proof reading
bound to the pinned parent and expected regular file. A producer
publication conflict must fail the delegated run and the competing entry
must never count as proof of that run.
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

In the adoption PR, register `verify/reportpublication` as verifier
support with zero root-module production imports. Update the exact direct
imports for `verify/release`, `verify/linux`, and `bench/retained-history`
to include it. If a compatibility wrapper remains in
`verify/capacityevidence`, explicitly register and allow that one outgoing
edge in the capacity fitness rule. Preserve the validator's two direct
production consumers and exclude the Linux runner from its reachability.
Keep the nested module outside the root-module inventory.

Update the measured revision, package table, evidence diagram nodes and
edges, and release evidence and fitness descriptions in `SYSTEM.md`.
Check the shared publisher's registered role, exact imports, and
reachability in both measured Linux graphs. Only the declared evidence
producers and any explicitly registered compatibility wrapper may reach
it in root-module production. Include fixture violations through
registered and external intermediaries from the server, offline
verification and export commands, and unrelated production packages.
Preserve verifier consumer ownership, command boundaries, and oracle
independence.
[Architecture registry](../../internal/archtest/architecture_test.go),
[Capacity evidence fitness](../../internal/archtest/capacity_evidence_boundary_test.go),
[Verifier consumer fitness](../../internal/archtest/verifier_consumers_test.go),
[System architecture](../architecture/SYSTEM.md#fitness-and-registry-ownership)

Release Verification owns implementation and the delegated fixtures.
Core Architecture owns the registry, diagram, and fitness changes.
Coordinate leases with the #108 promotion before dispatching adoption.
This extraction introduces no production package or root-module import
edge and requires no change to the active promotion's architecture files.
