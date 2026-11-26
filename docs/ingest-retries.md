# Ingest retry contract

Issue #8 remains open until the production WAL passes the cases below across a
real process restart. This change provides the transaction coordinator and
backend contract in `internal/idempotency`. The HTTP and client behavior
described here is the contract for the WAL, HTTP, and client integration.

## Request scope and fingerprint

A keyed `POST /ingest` supplies one `Idempotency-Key` header value containing
1–128 bytes from `A–Z`, `a–z`, `0–9`, `-`, `_`, `.`, or `~`. An
explicitly empty, malformed, repeated, or comma-joined value is invalid and
returns HTTP 400. Keys are case-sensitive. Clients should generate a random
key for each logical batch and retain the key and body until the outcome is
known. Omission uses the existing unkeyed ingest path.

The namespace is the versioned ingest operation `bog-ingest-v1`, canonical
tenant ID, canonical stream ID, stable principal ID, and client key. All
three IDs come from trusted server context and use the same 1–128 byte
identifier syntax. Credential rotation and stream renames preserve the scope.
The same key under another tenant, stream, or principal is independent. A
standalone server uses a stable server-assigned scope for its single ingest
namespace. Scope and fingerprint derivation must stay consistent across
instances and restarts.

Follow the stream-authorization contract for issue #5. Authenticate, resolve
the canonical stream, and check its current ingest grant on every request
before consulting retry state.
[Stream authorization and durable redaction](shared-deployment/stream-authorization-redaction.md)

Check the original body against the 4 MiB limit and validate the full NDJSON
batch before fingerprinting. The fingerprint is HMAC-SHA-256 over the operation domain, a zero byte, and the exact original
body bytes. Whitespace, escaping, trailing newlines, and row order affect it.
Persist a securely generated 32-byte server secret separately from retry
records and use it consistently for still-active records, including through
restart. A future live secret rotation needs support for the fingerprint
version associated with each active record. Keep the digest and raw request
values out of logs, metrics, and responses. In shared mode the WAL receives
only the validated redacted batch, with the redaction policy version recorded
alongside the fingerprint. A policy change does not change the fingerprint or
permit a second append for an active key.

## Window and client handling

The default window is 24 hours measured from the successful commit boundary.
A new record stores its commit time and absolute expiry. Replay and conflict
leave the deadline unchanged. At exactly the stored expiry the key is
available for a new batch, even if cleanup has yet to run. Changing the
configured window affects future commits. Deployments offering a different
window must publish that guarantee to their clients.

| Observation | Result and client action |
| --- | --- |
| Fresh keyed batch commits | HTTP 204. The batch was committed once. |
| Same scoped key and exact bytes during the window | HTTP 204. The original commit satisfies the request. |
| Same scoped key with different bytes during the window | HTTP 409, `ingest retry key already used for a different batch`. No append from this attempt. Investigate the key collision. For a genuinely different logical batch, allocate a new key. |
| Validation fails or storage confirms rollback | Use the existing 400 or 413 for invalid input and the existing confirmed-uncommitted 503 for storage rollback. A confirmed rollback is safe to retry with the same key and bytes. |
| Response is dropped, truncated, or lacks a definitive outcome | Follow the unknown-outcome behavior from #7. While inside the guaranteed window, retry the same scoped key and exact bytes. An uncertain storage response must never be framed as a confirmed rollback. |
| The window may have elapsed after an unknown outcome | Stop automatic retries and reconcile the outcome before deliberately submitting a new logical batch with a new key. |

A client can measure a conservative retry deadline from its first dispatch.
A lost response can hide the server's actual commit time. A confirmed
rollback covers that attempt only. An earlier ambiguous attempt still needs
a replay or other authoritative resolution. Preserve the exact
body bytes across attempts. An unkeyed unknown outcome can still produce
duplicates if replayed. Each successful new batch preserves intentional
duplicate rows within it. Separate logical batches, including batches with
identical bytes, can each commit under distinct keys.

## WAL transaction and recovery

`Backend.Commit` owns the atomic decision against committed state. Serialize
the check of the current record and any new commit against other attempts for
that scoped key, including attempts from other coordinators. Compare expiry
at the commit boundary using the injected wall clock. Sample it after queuing
and preparatory work, when ordering the authoritative commit. Use a consistent
wall-time source across coordinators and recovery. If the current
record is active, return it with no batch write. Otherwise, include the scope,
key, attempt ID, fingerprint, redaction policy version, commit time, expiry,
and complete batch in the same durable transaction.

A later WAL adapter must put the record and batch under the same committed WAL
boundary and publish the whole batch after durability, before returning a
confirmed success. Recovery reconstructs the active retry index from the same
committed data used to recover rows. A crash before the boundary can leave
neither a visible batch nor a success record. A crash after it can leave both
even if the client saw a disconnect. Preserve existing unkeyed requests and
legacy WAL replay. Legacy entries have no retry record.

A backend can report rollback only with evidence the attempted transaction
has not published and can never commit later. For an uncertain attempt the
coordinator asks `Backend.Resolve` for the outcome of that exact attempt while
the request context permits. Recovery can report that commit or a proven
rollback. A missing index entry, transport error, or cancellation alone is
insufficient. While the outcome is unresolved, the backend fences that key
or reports storage unavailable. Before admitting a new write for that key it
must establish authoritative state. A subsequent request runs the same
transactional test. The in-process coordinator has no cache of outcomes.

Schedule bounded `Coordinator.Cleanup` passes over the backend's expiry index.
A pass examines at most its requested number of candidates, up to 1,024, with
one cutoff. Candidates qualify at or before that cutoff. Deletion must check
the current generation and expiry atomically with replacement so an earlier
pass cannot remove a renewed key. Cleanup never removes batch rows. Recovery
and compaction must preserve live records and their original expiry, and must
reclaim expired metadata without resurrecting removed records. Expiry bounds
retention by time. Capacity still depends on the keyed ingest rate and cleanup
throughput.

## Production acceptance

Exercise concurrent same-key submission, a dropped response after durable
commit, retry with changed bytes, isolation by each scope component, rollback
and retry, an unresolved commit, and retries on either side of the exact expiry
boundary. Verify one published batch for a replay even after killing and
reopening the service on the same WAL. Also verify legacy WAL replay, unchanged
unkeyed behavior, and preservation of intentional duplicate rows. The tests in
this change exercise the transaction contract with a reconstructible test
backend.
