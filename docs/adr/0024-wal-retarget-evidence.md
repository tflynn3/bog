# ADR 0024 — WAL evidence after a retarget readiness mismatch

**Status** — Implemented. Native qualification of the composed candidate is pending.

## Context

The Linux durability verifier fills a disposable filesystem and changes the
candidate's configured data path while the original WAL directory is open.
Readiness should continue to describe that original filesystem in both
retarget directions. A path-based sampler can report the new filesystem
while ingest and recovery still use the original. Returning immediately
on the readiness assertion loses the evidence needed to distinguish those
behaviors.
[Verifier composition](0008-release-verifier-composition.md),
[Readiness descriptor handoff](0005-pinned-writable-recovery-authority.md#readiness-descriptor-handoff-60)

## Decision

A complete, validated readiness response that differs from the expected
state fails the direction and records the actual status and reason.
While the candidate is still serving and the direction has time left,
the verifier submits the existing whole batch. With the original healthy
filesystem, it expects a durable acknowledgement. With the original full
filesystem, it expects a WAL refusal and a separate readiness observation
confirming ENOSPC rollback. The verifier uses those confirmed outcomes
to build the expected ledger.

After a confirmed outcome, the verifier checks the live query walk,
stops the candidate, restores the original launch path, and restarts the
same binary. It checks the stale cursor for the full-filesystem fixture
and reconciles a complete replay walk against the ledger, including order
within timestamp ties and absence of the rejected batch. Successful
reconciliation sets the existing replay flag. The direction retains the
original readiness assertion and reports any additional probe or cleanup
failure.

A readiness transport or response-validation failure ends the direction before
ingest. The observation window also ends the direction before ingest when it
expires or is canceled. Window termination is recorded as the terminal error.
An interrupted request whose only error comes from that window leaves the last
recorded sample intact. A completed, validated response remains attributable
to the candidate even if the window closes as the probe returns. Record any
state mismatch and any independent transport or response-validation failure
as sample evidence, including when it coincides with window termination.

An unconfirmed ingest outcome or failed process transition prevents a
successful replay claim. Candidate reaping, filler removal, and filesystem
detachment still run on failure.

## Trade-offs

The extra requests and restart spend time within the existing direction
budget. They show whether readiness, ingest, and replay agree at the
candidate's HTTP and process boundary. The observations support diagnosis
without changing the schema-v1 fields or the release pass criteria.
Release Verification owns these probes and their cleanup. The server owns
readiness, WAL writes, and recovery.
[Release evidence process boundary](0013-release-evidence-process-boundary.md),
[Retained storage evidence boundary](0021-retained-release-evidence-boundary.md)
