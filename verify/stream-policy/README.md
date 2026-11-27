# Stream policy conformance

`streampolicy.Run` checks the proposed shared-deployment contract on one
disposable target. Provision the canonical scopes, grants, and redactors from
`ReferenceFixture()`, then supply query, ingest, restart/replay, and export
observers backed by that same target. Give the run a context with a deadline.
The caller owns startup and cleanup. The unit tests use a deterministic
in-memory target with serialized committed frames.

| Check | Required observation |
| --- | --- |
| Authorization | A uniform 403 across missing grants and unknown selectors, including valid or invalid continuation tokens, keyed requests, and known or unknown artifact IDs. Compare error bodies and application headers and require an absence of page, cursor, or artifact evidence. |
| Cursor scope | Exact first and final pages for the issuer. After authorization, a changed tenant, stream, or principal yields the same 400 as a modified token. |
| Retry namespaces | The same key commits independently when any one scope component changes. An identical request returns the recorded response before and after reopening. A conflicting request returns 409, and exact query and committed-record comparisons catch duplicate or suppressed appends. |
| Pre-append redaction | Committed bytes, exact replayed rows, query results, and export bytes and rows preserve the expected retained line and replacement. The plain and JSON-escaped Unicode markers must be absent. A two-row batch with an unavailable redactor leaves neither row committed. |
| Scoped export | Authorized create and download have exact scoped records, including a positive control for a principal with only export permission. The observer verifies the artifact and authenticated manifest, then reports the source scope and policy versions. |

Observers preserve the complete observed application response for each call and
exclude transport-only metadata such as Date. The replay observer reads the
complete committed fixture bytes and independently replays those bytes, then
routes subsequent observations to the reopened target. The export observer
validates the complete artifact and returns its original bytes and decoded
records. Supply raw, inspectable fixture byte surfaces with positive retained
witnesses. An opaque encoding that cannot be inspected by this runner leaves
evidence incomplete. Each byte observation is bounded by `MaxEvidenceBytes`.

Ordinary application rejections are observations. Adapter errors, cancellation,
missing observations, and unsupported evidence fail with `ErrIncomplete`.
Mismatches fail with `ErrViolation`. The runner stops at the first failed check
and reports its phase without including fixture secrets, raw adapter errors, or
tokens. This milestone qualifies the fixture and evidence checks. Live policy
changes, restore, and release-binary qualification require separate scenarios.
