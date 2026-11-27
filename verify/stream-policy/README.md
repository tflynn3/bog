# Stream policy conformance

Security Verification within Core Engineering owns the stream policy
conformance runner and its fixture coverage.

Provision a fresh disposable target with the scopes, grants, and redactors
from `ReferenceFixture()`. Pass query, ingest, restart/replay, and export
observers backed by that target to `streampolicy.Run`. The caller owns startup
and cleanup and supplies a context with a deadline.

The runner derives one matrix from the fixture grants. It pairs every
principal with every configured selector and an unknown selector, then
runs those cells concurrently against the same target. Setup, replay, and
export inventories run after requests drain.

| Check | Required observation |
| --- | --- |
| Query authorization | Every ungranted initial query and continuation returns 403, including requests with valid or modified tokens. Authorized queries return exactly the stream's redacted rows in both NDJSON and decoded evidence. Filters for foreign rows yield empty pages. |
| Cursor scope | Every query grant issues a cursor with exact first and terminal pages. A different authorized principal or scope yields the same 400 as a modified token. The issuer successfully resumes after the denial probes. |
| Keyed retries | Each independent namespace commits and replays its own outcome, including after reopening. Changed retries return 409. Ungranted targets return 403 for exact retries, changed bodies, and a fresh key. |
| Pre-append redaction | Committed bytes, replayed rows, query results, and exports preserve the replacement and permitted text. Plain and JSON-escaped Unicode secrets stay absent. An unavailable redactor causes a whole two-row batch to fail. |
| Exports | An authorized creator in each scope produces a verified artifact, including an export-only control. Every same-scope export grant can download the original bytes. Missing grants and cross-scope downloads return 403. Each denied download of an existing ID is paired with an absent ID. |
| State | The complete committed bytes and replayed rows match before and after the matrix. The export state matches the authorized artifact IDs and fingerprints, with no new pending or published exports after the matrix. |

Denial comparisons require identical application response bodies and headers
within each operation. They check decoded observations and raw response bytes
for protected rows, cursors, retry outcomes, and artifact metadata. The runner
recognizes structured metadata and scans for observed artifact IDs and their
SHA-256 fingerprints in hexadecimal or standard base64 as whole tokens.
Ordinary prose with an ID's characters inside a word still passes.

Observers preserve the original application response and exclude transport
metadata such as Date. The query observer captures the original NDJSON.
The replay observer reads all committed bytes, independently replays them,
and routes subsequent observations to the reopened target. The export
observer validates artifact integrity and the authenticated manifest, then
reports the original bytes, decoded records, scope, and policy versions.

The export adapter also implements `ExportStateObserver`. It independently
enumerates all published artifacts and hashes their original bytes, including
authenticated metadata, and counts pending or staged exports. The runner
attests an empty inventory before authorized creation, checks the inventory
against the authorized downloads, and compares it again after the matrix.
Inventory evidence includes artifacts and pending work regardless of public
visibility. Adapters honor context, support concurrent requests, and keep
returned evidence stable through the run. Each raw byte observation is bounded
by `MaxEvidenceBytes`.

Application rejections are observations. Adapter errors, cancellation, missing
observations, and unsupported evidence fail with `ErrIncomplete`. Policy or
state mismatches fail with `ErrViolation`. Diagnostics identify the probe
phase and omit fixture secrets, raw adapter errors, cursor tokens, and artifact
IDs.

The Go unit and race tests qualify the fixture and its evidence checks against
a deterministic in-memory target. Live shared-deployment qualification requires
observers backed by that deployment and separate scenarios for policy changes,
restore, and release qualification.
