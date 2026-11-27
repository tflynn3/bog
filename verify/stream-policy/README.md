# Stream policy conformance

`bog-stream-policy` owns the runner and fixture coverage for Security
Verification within Core Engineering. `TestReferenceScenario` is the
in-package consumer. `streampolicy.Run` checks one disposable target
provisioned with the scopes, grants, and redactors from `ReferenceFixture()`.
Supply the query, ingest, restart/replay, and export observers backed by that
same target. Give the run a context with a deadline. The caller owns startup
and cleanup. The tests use independent synthetic targets with serialized
committed frames.

The fixture gives three principals different combinations of permissions over
three streams. Two streams share a tenant and two share a stream ID across
tenants. The runner exercises each principal against every known selector and
an unknown selector.

| Boundary | Evidence |
| --- | --- |
| Initial query | Every granted caller sees exact scoped rows, finds the retained replacement, and gets an empty page when searching for a forbidden marker. Ungranted callers receive equivalent 403s with no page or cursor. |
| Continuation | Each granted caller gets a first page and resumes its own cursor to the exact final page. Every issued and modified token is tried across all callers and selectors. A caller without query permission receives the same 403 as an initial query. An authorized caller using another issuer's cursor receives the same 400 as a modified token. |
| Keyed retry | The same key commits independently when the principal, tenant, or stream changes. Replay returns each recorded outcome, and changed bodies return 409. Ungranted callers receive equivalent 403s for exact bodies, changed bodies, and unused keys. The runner repeats the query and retry checks after reopening. |
| Export grant | Every granted caller creates an artifact, including callers with export permission alone. Ungranted creates yield equivalent 403s without an artifact ID. |
| Artifact download | Each artifact is fetched by every caller and selector. An export grant for the artifact's scope permits retrieval even when another principal created it. All other downloads and absent-ID controls yield equivalent 403s without bytes, records, manifest, or verification evidence. Successful downloads preserve exact scoped records and authenticate the source scope and policy version. |

Denial comparisons use the complete application response within each endpoint
and status class, including the body and application headers. Denials must
exclude fixture identities, retained rows, retry keys, and opaque references.
Opaque IDs are checked at token boundaries in bodies and headers, including
JSON and URI escapes. The ID `d` in `download denied` is ordinary prose.
The same ID in `/artifacts/d` or a standalone JSON value is a disclosure.

At each boundary the runner checks for raw redaction inputs, including the
JSON-escaped Unicode marker. Query results, committed bytes and replayed rows,
and export bytes and records must preserve the expected retained text and
replacement. A two-row batch with an unavailable redactor leaves neither row
committed. Final replay checks that the probes added no unexpected records.

Observers preserve the complete application response and exclude transport-only
metadata such as Date. The replay observer reads the complete committed fixture
bytes, independently replays them, and routes subsequent observations to the
reopened target. The export observer authenticates the artifact and returns its
original bytes and decoded records. Supply raw, inspectable fixture byte
surfaces with retained witnesses. `MaxEvidenceBytes` bounds each observation.

Ordinary application rejections are observations. Adapter errors, cancellation,
and missing or unsupported evidence fail with `ErrIncomplete`. Mismatches
fail with `ErrViolation`. Diagnostics identify the failed phase without
including fixture values or raw adapter errors. This fixture qualifies the
evidence checks. Live policy changes, restore, and release-binary qualification
require separate scenarios.
