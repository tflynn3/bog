# ADR 0050 — Staged stream-policy decision boundary

**Status** — Accepted for staged registration in Cycle 199. Production
activation awaits serving integration and evidence.

## Context

#137 owns the decision engine in `internal/streampolicy`. The
shared-deployment contract requires current authorization for each
operation and redaction before ingest data becomes durable. The
retained-history cutover in #108 has its own promotion and
qualification criteria. A standalone engine can exercise policy semantics
while that work proceeds. Shared deployment requires later integration
across HTTP, storage, and export.
[#137 engine implementation](https://github.com/tflynn3/bog/issues/137),
[Shared-deployment contract](../shared-deployment/stream-authorization-redaction.md),
[Retained runtime promotion](0049-retained-runtime-promotion.md)

## Decision and redaction interface

`internal/streampolicy` owns immutable policy snapshots, atomic activation
of a validated revision, grant evaluation, and stream-local redaction and
masking rules. The proposed #137 interface uses the following semantic
contract. Agree the concrete Go API with #137 before extending it.

- Evaluate one operation using a trusted stable principal ID and canonical
  tenant and stream IDs from the caller. Ingest, query, and export grants
  are independent. A successful decision captures the scope, operation,
  and applicable rules and versions from one coherent snapshot. The
  authorization check at admission orders the request against policy
  activation. An acknowledged update governs subsequent admissions. A
  previously admitted operation may finish under its captured policy,
  subject to the final export publication check.
- Fail closed on unknown scope, absent grant, or unavailable authority.
  Return typed, non-sensitive outcomes for the caller to map to the public
  contract. HTTP owns credential authentication, selector resolution in
  trusted tenant context, uniform denials for unknown and inaccessible
  streams, and error precedence. Keep credentials, tokens, request values,
  and raw underlying errors outside decision diagnostics.
- For each query page, capture the current grant and effective masking
  rules and version together. Check authorization before examining a
  cursor. The authenticated cursor binds the principal, canonical scope,
  query, snapshot position, and masking version. HTTP owns token
  authentication, expiry, and invalid-cursor responses. An authorized page
  uses the same masking decision for cursor checks, matching, lookahead,
  byte limits, and encoding. Grant-only changes preserve the masking
  version. Each effective masking change gets a fresh stream-local version,
  including disabling masking or restoring earlier rules.
- An ingest admission captures one redaction policy and version. The batch
  redaction operation accepts the entire validated batch in memory and
  returns the sanitized lines in input order with the applied version.
  Failure on any row returns no sanitized batch and a non-sensitive error.
  The caller owns input validation and size limits, serialization and
  validation of the complete transformed batch, and deriving both durable
  bytes and published rows from that result. Append follows only after
  those checks pass. Missing or unavailable redaction fails closed.
- Authorize each ingest retry before lookup. Keyed ingest owns the scoped
  key, original-request fingerprint, recorded outcome, and atomic
  persistence with the sanitized batch. An authorized exact replay
  returns the original result under its recorded redaction version
  without redacting again or appending a second time.
  Export checks the current grant at admission, immediately before
  publication, and at every retrieval. The export caller coordinates
  the final check with publication and authenticates the artifact scope.
  A revoked publication creates no retrievable artifact.

Keep the engine free of direct and transitive dependencies on other
root-module packages. Callers supply trusted inputs and own transport,
retry lookup, persistence, and artifact I/O. Server-side callers may
consume the engine once the wiring meets activation criteria.
Operator commands, evidence producers, and verifier support stay
independent of the engine, including through intermediate packages.
`verify/stream-policy` supplies its own expected decisions and observes
the candidate across its public and durable boundaries. #137 can use
the same contract cases in engine unit tests. The new fitness test
checks production imports with Linux build selection on amd64 and arm64.
Its fixtures cover the absent-engine baseline, the staged and wired
shapes, and forbidden direct and indirect dependencies.
[Engine fitness test](../../internal/archtest/stream_policy_engine_boundary_test.go),
[Verifier independence test](../../internal/archtest/stream_policy_boundary_test.go),
[Verifier adapter contract](../../verify/stream-policy/README.md)

## Staged ownership and registration

The #137 implementation owner in Core Engineering owns the staged
library, its policy API, and unit and race evidence for revision
activation, revocation, cross-stream and cross-principal decisions,
masking versions, and all-or-nothing batch redaction. Security
Verification owns independent stream-policy conformance. Core
Architecture owns the package registry and SYSTEM.md.

The #137 engine landed before its registry entry. At
`74e56a7d2f46abd010eb0a86414b179fff06ecae`, the Linux amd64 and arm64
inventories show zero direct root-module production imports from
`internal/streampolicy` and no path to it from `cmd/bog`. Register the
engine as staged and record its ownership and activation criteria in the
staging declaration. Update the measured inventory, Staged subgraph,
and package role table in SYSTEM.md with the registry repair.

Core Architecture serializes the shared registry and SYSTEM.md edits
with #108 promotion. Recheck write leases at dispatch and confirm the
file handoff. If promotion holds the files, land the repair through its
ordered queue. Give the landed repair SHA to the retained promotion
owner for rebase and run the architecture suite on the composed revision
to verify both Linux graphs, staging declarations, and diagram parity.
[Package role parity](0019-package-role-governance.md),
[Fitness and registry ownership](../architecture/SYSTEM.md#fitness-and-registry-ownership)

## Production activation evidence

Move the engine to wired in the serving integration PR when the measured
`cmd/bog` closure reaches it on both Linux architectures and the
following evidence is available for that revision.

| Boundary | Evidence from the serving candidate |
| --- | --- |
| Admission and paging | Exercise unauthenticated and denied requests, independent grants, canonical scope isolation, and same-tenant, cross-tenant, and cross-principal attempts. Demonstrate revocation and regrant, masking revision changes including rollback to earlier rules, unaffected stream cursors, uniform denials, and rejection of pre-restart cursors. |
| Durable ingest and replay | Inspect the actual committed range and independently decode it. Verify exact sanitized rows and durable replacement witnesses, including escaped and Unicode input. Force a redaction failure in a multi-row batch and verify zero partial commit or publication. Repeat byte and row checks after restart. |
| Keyed retry | Prove current authorization before retry lookup, scoped keys and conflicts, replay of the recorded outcome across policy revisions, and atomic batch and disposition recovery after ambiguous responses and crashes wherever keyed ingest is enabled. |
| Export and restore | Prove scope and redaction in authentic artifacts, revocation at the publication barrier with no retrievable artifact, and uniform denial for revoked and absent-ID retrievals. For writable restore, prove target-policy compatibility or redaction before durable writes and exact scoped replay after restart. |

Security Verification runs the independent candidate and revision
scenarios with bounded adapters against the same clean serving revision
on native Linux amd64 and arm64. Record the pinned revision, each
binary checksum, the observed results, and cleanup. Synthetic fixtures
qualify the verifier; missing observations leave activation evidence
incomplete. Update the registry, staging declaration, SYSTEM.md diagram,
and role table with the measured serving wiring. Review storage format
and any explicit legacy migration under the shared-deployment contract
before enabling shared mode.
[Candidate and revision scenarios](../../verify/stream-policy/README.md),
[Retry production acceptance](../ingest-retries.md#production-acceptance)
