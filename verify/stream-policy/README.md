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
including fixture values or raw adapter errors. This fixture qualifies the static evidence checks.

## Policy revision scenario

`streampolicy.RunPolicyRevisions` uses a fresh `ReferenceFixture()` target.
Supply the four base observers and three additional capabilities. Provide a
bounded context. The caller owns target startup and cleanup.

| Capability | Required observation |
| --- | --- |
| `PolicyObserver` | Apply each full snapshot through the target policy authority. Acknowledge the requested revision only once it governs new admissions. The mapping from fixture versions to native versions preserves stream-local masking versions across grant changes and uses a fresh version when earlier masking rules return. |
| `ExportPublicationObserver` | Pause an admitted export before its final grant check. Run the supplied callback to completion before releasing it. Observe the final application response and independently report every artifact ID made retrievable by that attempt. Cancel or release the barrier on every error or cancellation path. |
| `RetryRecoveryObserver` | Read the complete recovered persistence units in append order. Each unit provides the scoped key, original redaction version and disposition, and all rows from the same atomic commit boundary. The existing restart observer also inspects actual committed bytes and independently replays rows. |

The query probes hold cursors over acknowledged revocation and expansion,
compare complete denials for valid and modified tokens, and confirm that a
regrant restores an otherwise valid cursor. A masking revision invalidates
old tokens even after earlier masking rules return. Fresh pages match over
masked rows and continuations for an unaffected stream still work. Both decoded
rows and the actual NDJSON must agree with the expected scoped pages.

The export probes inspect exact authenticated artifacts for two scopes.
They revoke an export-only grant at the publication barrier, require no
artifact to become retrievable from the aborted attempt, and exercise
retrieval revocation against existing and absent IDs.

The retry probes change ingest redaction and revoke a previously successful
principal before restart. An authorized exact retry must reproduce the original
disposition. Revoked exact, changed-body, and unused-key requests must give
equivalent denials. A fresh append witnesses the new redactor. A second
restart verifies the exact committed rows and atomic batch-plus-disposition
units, detecting any additional durable append caused by a retry.

The in-package synthetic target exercises the evidence checks with serialized
commits. For an actual target, return an error or incomplete evidence when a
capability is unavailable. Runtime activation still requires production WAL
evidence for atomic batch and retry persistence and recovery, including
ambiguous response and crash cases in the
[ingest retry contract](../../docs/ingest-retries.md#production-acceptance).

## Candidate-driven acceptance

Use `streampolicy.RunCandidate` with a deadline and one disposable candidate
provisioned with the identities and canonical stream scopes from
`ReferenceFixture()`. The caller owns candidate startup and cleanup. The
HTTP binding maps the logical principal to its native credential and the
selector to the candidate's public protocol. An empty principal uses no
credentials. The adapter sends the fixture's original unkeyed NDJSON and
captures full responses without redirects or transport retries.

The policy binding applies revisions to the candidate's authoritative policy
service and acknowledges them when they govern new requests. The probes
exercise unauthenticated requests, independent grants, same- and cross-tenant
cursor and artifact attempts, revocation and regrant, and original-cursor
rejection after restart. Export creation must return its final outcome and
retrieval must expose the original artifact body. Verify its manifest and
payload independently using the candidate's native format.

Open the exact committed range before restart using authoritative commit
metadata. Decode that range with an independent parser and verify its framing
and integrity. The verifier compares all decoded records in order, checks the
actual bytes for plain and escaped forbidden values, and requires each
retained line and replacement to appear in the observed bytes. After restart
it checks reopened queries and exports, then reads the committed range again.
The HTTP and file fixture includes a target that serves redacted queries
while its committed file preserves a forbidden value.

Bindings should report unsupported capabilities as errors. Missing bindings,
incomplete observations, and HTTP 501 responses produce `ErrIncomplete`.
This entry point covers unkeyed ingest and grant checks. The existing revision
runner covers keyed retries and revocation at the export publication barrier.
