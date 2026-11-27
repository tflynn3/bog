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
including fixture values or raw adapter errors.

## Extended conformance and qualification

`RunConformance` runs the static matrix and then exercises the same
disposable target with acknowledged policy updates and controlled append
interruptions. Its reference model derives expectations from the fixture,
the test's append schedule, and acknowledged policy changes. It orders
queries by UTC timestamp and append order for ties, applies masking before
literal matching, and captures expected rows at cursor issuance. Each
request still selects one canonical stream from an interleaved dataset.

| Probe | Evidence |
| --- | --- |
| Held and fresh query walks | Preserve the held view across a backfill. A fresh walk includes the backfill and its intentional duplicates in exact order and excludes the upper time bound. Compare observed rows and raw NDJSON through the terminal page. |
| Policy changes between pages | Revoking a grant yields the same 403 for initial, valid-cursor, and modified-cursor requests. Regrant permits the original cursor. Enabling or disabling masking invalidates earlier revisions. Fresh queries match masked values, and revocation takes precedence over stale revisions. |
| Held and fresh artifacts | An export created before the backfill retains its original contents after the backfill and after revocation and regrant. New exports include only the requested stream's committed rows, including its row outside the query range, and authenticate all applicable redaction policy versions. |
| Interrupted append before commit | Recover neither the batch nor its retry outcome. Two concurrent same-key submissions then return success with exactly one copy of the whole batch. |
| Interrupted append after commit | Recover the complete redacted batch and its original retry outcome together after the response is lost. A revoked ingest grant blocks the retry. Once regranted, exact bytes return the committed result even under a new redaction policy. Changing the original bytes while preserving decoded rows still conflicts. A fresh key uses the new policy. |

`PolicyObserver` applies controls to the trusted policy authority and
acknowledges once new admissions use them. `CommitObserver` interrupts
one submission at the requested boundary and stops the target. The adapter
must observe that boundary independently of the HTTP response. `Reopen`
rebuilds service state from the same durable directory. `Inspect` reads the
running target's committed view. Supply complete committed bytes with
inspectable row, key, and policy witnesses, plus rows and scoped retry
outcomes independently decoded from that same boundary. Provision the
fixture inside the active retry window and preserve its policy authority
across restarts. The two same-key submissions may overlap. All other
observations are serial, and adapters must honor cancellation and bound
their observations. The caller owns startup, cleanup, and failure recovery.

`RunConformance` records synthetic conformance in `Report.SyntheticPassed`.
`QualifyShippedAdapter` separately records `Report.Qualified` for one full
revision and binary SHA-256. The release caller supplies the pinned identity
and an independent observer that checks the serving process at startup,
after each reopen, and at completion. Run that entry point with adapters
that drive the shipped process and inspect its durable authority. Failed or
unsupported observations leave both fields unset. The in-package tests use
synthetic fixtures to verify the checks and report handling. Shipped-adapter
qualification remains pending until a shared-deployment implementation
provides the required observers and passes the scenario.
