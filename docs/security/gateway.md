# Read-only security gateway

`bog-gateway` is an opt-in query gateway. Deploy one private Bog service per stream, with HTTPS and a certificate trusted by the gateway. Restrict direct access to those services to the gateway and trusted ingestion infrastructure; an endpoint is a security boundary, not a tenant selector. The default stream explicitly owns the legacy database and WAL. Never point another stream at that data. There is no row-level migration or quarantine, and no gateway ingest endpoint: existing ingest remains a privileged internal operation. The gateway cannot enforce admission-time PII policy or secure direct access to the legacy service.

Build with `go build ./cmd/bog-gateway`. Run with `-config config.json -tls-cert server.crt -tls-key server.key`; TLS is mandatory. Protect configuration and keys with OS permissions and a secret manager. Use HTTPS directly; TLS termination at a proxy is not supported. Requests use `Authorization: Bearer <secret>` and `GET /query` with the existing parameters plus optional `stream` (default is `default_stream`). Only `query` is exposed. The server's TLS trust store must trust each upstream certificate; redirects are rejected. Restrict network access and set upstream transport policies outside this process.

Example configuration (replace every placeholder, including keys):

```json
{
  "default_stream": "legacy",
  "streams": {"legacy": {"url": "https://bog-private:8080", "policy": "standard"}},
  "principals": {"reader": {"token_sha256": "<64 hex characters>", "streams": ["legacy"]}},
  "policies": {"standard": {"version": "1", "rules": [{"pattern": "[a-z]+@example\\.com", "action": "mask"}]}},
  "cursor_key": "<64 hex characters>",
  "hash_key": "<at least 64 hex characters>",
  "audit_file": "/var/log/bog-gateway/audit.jsonl"
}
```

Store SHA-256 digests of high-entropy random bearer tokens, not passwords. Remove a digest and restart to revoke it; deploy multiple credentials under distinct principal names to rotate. Configuration is loaded at startup, so changes and revocations require a restart. Rules are ordered Go regular expressions over the entire line, including JSON text; `mask` replaces each match with `[REDACTED]`, while `hash` replaces it with an HMAC-SHA256 digest. Rules must cover every sensitive representation, including escaped or structured forms; there is no automatic field-aware detection. Rotate the hash key only with an explicit change to the policy version. Do not treat this configuration as a general PII detector.

Filtering is performed against the redacted line, never passed to the upstream. Every scan is capped at 1,000 upstream rows, 4 MiB output, 16 concurrent requests, and 15 seconds per upstream request. The response may contain fewer than the requested limit, including zero, while still providing `Bog-Next-Cursor`; follow it until absent. This applies even without `page=1`. Tokens are encrypted and authenticated, bind the principal, stream, normalized time bounds, filter and policy version, and expire 15 minutes after the first page. Changing `limit` is allowed. Rotate the cursor key to invalidate all tokens. Restart preserves gateway tokens if the key is retained, but an upstream restart invalidates its embedded cursor; restart the query from the first page. Each page is independently authorized. Backfill and duplicate ordering follow the upstream snapshot cursor.

Access decisions and outcomes are appended as JSON to a private regular audit file and synced before any response is released. Audit records contain an internally generated request ID, actor, stream, action, decision, timestamp and status; they omit credentials, filters, cursors and log lines. Audit writes are serialized; failure returns 503 without releasing query results. The operator must provision durable local storage, monitor capacity and ship/protect audit files independently. The gateway cannot guarantee an audit record when the audit storage fails, and a crash can leave a partial final record. An allowed decision means authorization succeeded, not that the query succeeded.

## Performance limitation

The gateway currently makes one upstream request per scanned row so that redacted filtering cannot skip rows in a continuation. It has not met or been validated against the requested less-than-5% full-response p95 overhead. Do not deploy against that performance requirement without a representative same-host Linux benchmark of both ordinary and paginated queries, including rare and absent matches. The work cap can require many continuation requests and does not bound the cost of each upstream query. The upstream cursor is process-local and is invalid after upstream restart.
