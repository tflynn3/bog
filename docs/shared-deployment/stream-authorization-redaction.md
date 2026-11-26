# Stream authorization and durable redaction contract

Status: proposed design for shared deployments; none of the authorization,
stream isolation, redaction, retry-key, or scoped export behavior below is
implemented at the base revision. This contract gates those features, including
#8 retry keys and #6 scoped export/restore. It is not a claim that the current
single-user service is safe to expose to mutually untrusted tenants.

## Current behavior and boundary

`POST /ingest` currently validates a whole NDJSON batch and passes both the
original request bytes and decoded rows to `wal.Append`. The WAL frames those
bytes, syncs them, and advances the durable commit marker before publishing the
rows. Recovery decodes the committed payload again. Consequently, masking only
query output or decoded rows would still leave the original values in the WAL
and resurrect them after restart. The current WAL has no stream identifier.

`GET /query` currently scans the shared store. Pagination captures a row count
and position; its HMAC-authenticated, process-lifetime cursor also binds the
time bounds and a digest of `contains`, but not an identity or stream. A cursor
is invalid after restart, and invalid or mismatched cursors return 400. There is
no authentication, per-stream filter, export route, or retry-key contract today.
The existing response byte limit and snapshot ordering still apply within an
authorized stream.

## Identity and permissions

A stream is identified internally by `(tenant_id, stream_id)`: two immutable,
server-issued opaque IDs. The tuple, not a display name, URL spelling, bearer
token, or caller-supplied label, is the canonical identity in authorization,
storage, cursors, retry records, and export manifests. IDs are never reused,
including after deletion. Display names may change without changing identity.
A trusted authentication layer maps credentials to a stable principal ID and
server-side grants. Unauthenticated requests fail closed. Stream selectors are
resolved under the authenticated tenant, with one unambiguous canonical tuple;
unknown, ambiguous, and inaccessible selectors must not reveal stream existence.
A caller cannot choose a tenant by supplying an untrusted header or batch field.

Each request operates on exactly one stream. There is no implicit wildcard or
cross-stream query, batch, cursor, or export. A future multi-stream operation
requires a separate contract. Grants are independent and least-privilege:

| Permission | Authority |
| --- | --- |
| `ingest` | Append a validated, redacted batch to that stream. |
| `query` | Read matching rows and continue a snapshot for that stream. |
| `export` | Create and retrieve a scoped durable export of that stream. |

`export` is independently granted and implies authority to read all retained
redacted data in that stream, even if `query` is absent. `ingest` grants neither
read nor export. Administrative grant management and restore are separate
privileged operations; none of these three grants confers them. The service
must authenticate and authorize server-side, never trust a client assertion of
grants, and deny on policy lookup failure. Authorization failures use a uniform
403 without rows, cursor, retry outcome, or stream-existence detail; invalid
credentials use 401. Requests with malformed syntax may receive 400.

## Check points and revocation

Resolve the canonical stream and check `ingest` before accepting a batch for
processing, `query` before every initial or ordinary query and every continuation,
and `export` before starting an export and before each artifact retrieval. A
retry is a new ingest request: check current `ingest` before consulting or
returning a recorded outcome. Token possession is never authorization.

Authorization has an explicit linearization point at the final grant check
immediately before admitting an operation. A grant update and admission must be
ordered by a coherent policy authority: an admission ordered after revocation
must fail, including across service instances. An operation admitted before
revocation may finish, including an already admitted durable append or a
response already being sent. Once a revocation is acknowledged, no new
admission under that grant may succeed. Long-running exports must recheck before
publishing an artifact and each download must be newly authorized. An export
revoked before publication is discarded and is not made available. The system
must not silently fall back to stale cached grants when the authority is down.

A revoked query continuation returns 403 with no page and no next cursor,
regardless of the cursor's validity; restoring the grant permits an otherwise
valid, unexpired cursor to be retried. Authorization is checked before cursor
validation so an unauthorized caller cannot use cursor errors as an oracle.
Already delivered pages cannot be recalled. Restart can independently invalidate
an otherwise authorized cursor, which then returns 400 as today.

## Cursors and retry keys

Version the cursor format and MAC domain for shared deployments. Authenticate
the canonical stream tuple and stable principal ID alongside the existing
normalized bounds, keyed filter digest, snapshot count and position. Derive
scope from authenticated server context, not from fields asserted by the token.
Verify MAC and compare scope before using the snapshot or scanning. A valid
cursor presented by a different principal or for a different stream returns
the same 400 invalid-cursor response as any other scope mismatch, provided the
caller is authorized for the requested stream. The cursor is not transferable,
even between two principals with identical grants. Limit may still change;
`contains` and equivalent time bounds retain their existing semantics. Preserve
header-size checks with the new maximum encoded token size. Reject old unscoped
cursors in shared mode; never interpret them as belonging to a default stream.

#8 must namespace a client retry key by canonical stream tuple and stable
principal ID, with a versioned operation domain. Do not key by credential bytes:
credential rotation must not create a different principal. Persist the key,
request fingerprint, redaction policy version, and outcome atomically with the
committed batch so recovery cannot publish one without the other. Fingerprint
the original validated request with a server-keyed digest, not raw secret
values; do not log it or expose it. A same-scope, same-key, same-request retry
returns the recorded result without another append, even after restart or a
policy change; a different request using that key returns 409 and writes
nothing. Define bounded retention and key-expiry behavior in #8; after expiry,
clients must use a new key and cannot assume deduplication. Across scopes the
same key is a distinct namespace: it never retrieves another scope's outcome or
suppresses its append. Checks for current authorization precede lookup, including
for a previously successful retry. Existing non-keyed ingests retain their
unknown-outcome behavior after a lost response.

## Redaction before durability

A stream has a server-controlled, versioned redaction policy. It must define
which portions of `line` are removed or replaced, and deterministic matching and
replacement rules; a missing, unavailable, or invalid policy is a failure, not
permission to store raw input. No client can weaken the policy. Policy changes
apply to newly admitted batches; they do not retroactively sanitize existing
WALs, exports, backups, or rows. A stronger policy requires a separate rewrite
and disposal plan before claiming historical data meets it.

For every ingest, after authentication and stream authorization:

1. Read within the existing 4 MiB request limit and validate the entire input
   under the current UTF-8, NDJSON, timestamp, field, and surrogate rules.
2. Apply the selected policy to every decoded row in memory. If any row cannot
   be redacted, fail the entire batch closed: no append, publication, retry
   success record, or partial rows. Return a generic error without input values;
   use 400 for invalid input and 503 for unavailable redaction infrastructure.
3. Serialize only the redacted rows into the new stream-aware durable format;
   validate the complete serialized representation and its size before append.
   Derive both WAL payload and published rows from that same validated result.
   Oversized transformed batches fail as a whole with 413. Any encoding or
   policy error fails before `wal.Append`; no raw fallback is permitted.
4. Sync the WAL, durably replace and directory-sync the commit marker, then
   publish the entire batch before acknowledging success. Queries see all or
   none of a batch, including during concurrent ingestion and recovery.
   A confirmed append failure returns 503 with no published batch; an uncertain
   commit retains today's fatal/unknown-outcome behavior. A retry key resolves
   an unknown outcome only under the #8 atomicity contract above.

The raw body, decoded raw rows, secrets, and failed redaction values must not be
written to WAL, temporary files, diagnostic logs, traces, metrics labels, error
responses, retry records, or export artifacts. Limit their in-memory lifetime;
this is not a promise of cryptographic memory erasure. Redaction is an ingest
boundary, not a substitute for transport security or access control. Tests must
exercise JSON escaping and Unicode as well as ordinary text so serialization
cannot reintroduce the original value.

Diagnostics must also exclude search literals, credentials, and cursor tokens,
including from request URLs and underlying error strings. Use bounded,
non-sensitive operation and failure categories instead; diagnostic delivery
must not delay the WAL rollback or commit path. Resolve and compile policy once
per admitted batch where possible, rather than performing policy lookups for
each row.

Storage exclusion guarantees only that values selected by the active policy
are absent from the sanitized durable representation and derived rows; it does
not conceal values the policy fails to match, previously stored data, or facts
inferable from the remaining data. Query filtering, pagination, and exports
operate on that sanitized representation. An optional output-masking policy is
weaker: unless matching and pagination also operate on masked content, callers
can infer hidden values by probing `contains`, observing page lengths, and
checking `Bog-Next-Cursor`. Shared mode must apply output masking before matching,
byte limits, lookahead, and cursor generation if those inferences are to be
prevented; otherwise the policy must explicitly disclose those channels and
must not claim storage exclusion. Export authorization does not bypass storage
exclusion, and any additional output masking promised for exports must be
applied before the artifact is made available.

## Storage, export, restore, and rollout

Shared-mode storage and indexes must preserve the canonical tuple durably on
every batch and enforce it during scans, recovery, and compaction; filtering
only after a global query or a global result limit is insufficient. Snapshot
positions and lookahead must be scoped so neither counts nor ordering leak rows
from another stream. The format needs explicit versioning and integrity checks;
legacy frames contain no tenant identity and cannot be safely guessed into a
shared stream. Do not enable shared mode on an existing WAL until an explicit,
offline operator migration assigns its entire legacy dataset to one chosen
stream, applies the target redaction policy, validates the result, and handles
old WALs and backups under a separate disposal plan. Fail startup on ambiguous
or mixed unsupported formats. Existing standalone mode remains a separate trust
boundary until migrated, and must retain the documented historical WAL decoding
behavior for explicitly supported legacy frames. Migration reads those frames
with their historical decoder before applying the target policy; it must not
silently reinterpret or discard them.

#6 exports must select one authorized canonical stream at a durable snapshot,
include only its already-redacted committed records and required scoped
metadata, and authenticate an integrity-protected, versioned manifest containing
the source tuple and redaction policy version(s). They must not copy the global
WAL, uncommitted tail, other streams, raw inputs, or unrelated retry records.
An artifact is sensitive even after redaction and must be access controlled.
Restore is an administrative operation, not implied by `export` or `ingest`.
It validates the complete manifest, integrity, format, stream mapping, and
records before publishing anything, and requires an explicit authorized target
stream mapping. Restore preserves redacted bytes/semantics and never invokes a
raw-data source or reverses masking. Fail closed on invalid or unsupported
artifacts; do not partially publish. Restoring old unredacted legacy backups
cannot satisfy this guarantee without the migration and redaction process above.

## Acceptance cases

- Alice has ingest/query/export on A; Bob has them only on B. Alice's attempt
  to select B for ingest, query, export, or artifact download returns 403 and
  creates or reveals nothing. A batch cannot nominate B inside its NDJSON.
- Alice pages A, then presents that cursor while authorized for B: 400, no rows
  and no cursor. Bob, even if granted query on A, cannot use Alice's cursor.
  A modified token also returns 400 after authorization.
- Alice writes A with retry key K. Repeating the same request and K in A
  returns its recorded success without duplicate rows, including after restart.
  Changing the body with K in A returns 409. K in B or under Bob is independent
  and cannot disclose or reuse Alice's outcome; an authorized fresh request may
  append once in that other scope.
- After Alice receives page one, revoke her query grant and acknowledge it.
  Her continuation returns 403 with no data or cursor; a token alone cannot
  restore access. An already admitted response may finish. After regrant, the
  original cursor works only if still valid; after restart it returns 400.
- Ingest a sentinel secret with a policy replacing it, then inspect committed
  WAL bytes and queries: only the replacement appears. Restart and query again,
  export A, inspect the artifact, restore to an authorized target, restart, and
  query the restored stream: the sentinel is absent throughout. A forced
  redactor failure on any row leaves no batch, retry success, or secret in WAL,
  logs, or export; other valid rows in that batch are not published.
- Export A while B contains a distinct sentinel: the artifact and restored
  stream contain no B records or metadata. Tampering with the manifest or
  payload, or omitting the target mapping, fails restore without publication.
