# ADR 0010 — Retained resource admission

**Status** — Accepted for the #53 cutover and #63 resource limits. Runtime
wiring and native Linux qualification are pending at the measured baseline.
[System architecture](../architecture/SYSTEM.md)

## Context

One WAL owner coordinates appends, captures, writable recovery, and
generation activation in one Linux process. A write can grow the WAL and
the decoded tail. A capture can keep an older generation and tail alive
after replacement. Compaction and export need extra space while the
currently published generation remains readable. These costs can overlap
even when each HTTP request stays within its work and response byte
budgets. [ADR 0007](0007-runtime-storage-composition.md)

## Decision

**#53 bounds the writable tail in both recovery layouts.** Account for
uncovered whole frames and rows beyond the active checkpoint boundary,
including in-flight append reservations. Check the projected tail under
the same owner synchronization that orders commit and generation
activation. Admit the entire batch or refuse it before mutating the WAL.
Consume a reservation when the batch becomes durable and visible, release
it on a confirmed rollback, and fence the owner on uncertainty. Credit
covered rows only after a successor is durable and installed. Recovery
reconstructs the tail accounting from the selected authority and its
committed suffix.

Full-WAL mode keeps the exact readable WAL prefix for v1 checkpoint and
export digests. A tail-only v1 restore keeps its verified checkpoint
boundary and shares the tail admission rule. Once that tail reaches the
limit, it can still serve existing history. Further writes require an
operator remedy that preserves recoverability. Advancing the boundary or
exporting an image that needs the missing prefix requires a separate
format decision. [ADR 0005](0005-pinned-writable-recovery-authority.md#keep-the-v1-prefix-digest-meaningful)

**#63 governs overlapping resource use.** Charge the cost of admitted work
before taking pins, staging output, or appending bytes. Enforce limits where
the relevant resources are owned, with the command supplying configuration
and handles. Keep these limits independent of request work and response
budgets.

| Resource | Admission and accounting |
| --- | --- |
| Writable tail | Bound committed uncovered WAL bytes and row count plus reservations for pending batches. Bound suffix replay by the same retained tail policy. |
| Captured views | Bound count and lifetime. Count preparations and expired views with active readers until their references drain. Account for generations and tail data that remain pinned across replacement. |
| Compaction and export | Bound concurrent work and reserve staging capacity for outputs that coexist with the live generation and pinned predecessors. Account for unpublished stages until durable publication or safe cleanup. Include the readable v1 prefix in the full-WAL disk footprint. Check headroom on the filesystem receiving the writes, including an export destination when separate. |
| Filesystem headroom | Preserve space for WAL and commit metadata on the pinned active filesystem. #60 samples that filesystem for readiness and observes terminal commit outcomes. Admission and actual I/O errors still determine whether an operation can safely proceed. |

Release reservations on failed admission, cancellation, or completion as
the underlying references and files are released. On uncertain publication,
preserve recoverable state and charge the files until recovery settles the
outcome. Reject new work that exceeds a bound before exposing a partial
result or issuing a new cursor. An ingest refused for capacity returns 503
without publishing any rows. Existing HTTP captures keep the lifetime and
fixed-budget continuation guarantees in ADR 0006. Never reclaim data that
durable authority or an admitted operation can still reach.
[ADR 0006](0006-fixed-budget-query-continuations.md)
[ADR 0009](0009-backup-staging-recovery.md)

## Trade-offs

A hard tail limit bounds memory and replay work at the cost of write
availability when compaction falls behind or a tail-only restore fills.
Keeping the v1 prefix uses disk space and makes hashing I/O part of
publication and export. Staging headroom and bounded concurrency reduce
peak disk demand and can defer maintenance. Maintaining pins through
expiry and drain delays reclamation and preserves exact snapshot walks.
Recovery and durable commit take precedence over admitting new traffic.

## Qualification

Qualify the same clean composed revision on native Linux amd64 and arm64.
On each architecture, use one candidate binary for fixed-budget HTTP walks
through exhaustion, live generation replacement with acknowledged
backfill and a held cursor, and writable v1 restore followed by an append
and exact restart parity. Bind evidence to the full revision, that
architecture's binary SHA-256, and the release fixture.

Record configured #63 limits and exercise admission at and beyond them.
Verify that overload refuses whole batches and new maintenance work,
accepted cursors can drain, and eligible work resumes after capacity is
released. Exercise tail saturation on a writable v1 restore and compare
acknowledged rows after restart. Include the live capacity fixture's two
scales under its 128 MiB cgroup cap and retain its cleanup evidence.
Exercise #60 by retargeting the configured directory across healthy and
full filesystems in both directions. Readiness and ingest must follow the
active WAL filesystem throughout. [Release evidence](../architecture/SYSTEM.md#release-evidence)
