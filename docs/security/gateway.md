# Experimental read-only gateway

The gateway is opt-in and does not modify Bog or its WAL. Build with `go build ./cmd/bog-gateway` and run `bog-gateway -config gateway.json -tls-cert cert.pem -tls-key key.pem`. It terminates TLS itself; do not forward cleartext credentials. Keep the configuration and keys readable only by the service account. Backends must be private HTTPS origins with certificates trusted by the host; restrict network access so clients cannot bypass the gateway. Each catalog entry must point to a separate Bog instance. Map the legacy instance to `default_stream`; its existing unscoped rows all belong to that stream. Never map one backend to multiple security domains.

Example configuration (replace all placeholder keys and hashes):

```json
{
  "streams": {"legacy": {"endpoint": "https://bog.internal:8443"}},
  "default_stream": "legacy",
  "principals": [{"id": "reader", "token_sha256": "REPLACE_WITH_SHA256_HEX", "read": ["legacy"]}],
  "rules": [{"pattern": "[0-9]{3}-[0-9]{2}-[0-9]{4}", "replacement": "[redacted]"}],
  "policy_version": "1",
  "cursor_key": "REPLACE_WITH_32_BYTE_HEX_KEY",
  "audit_path": "/var/lib/bog-gateway/audit.jsonl"
}
```

Use a high-entropy bearer token and store its SHA-256 hex digest in the config. Send it as `Authorization: Bearer ...` and select a stream using `Bog-Stream`; absent that header the configured default is used. Configuration is loaded at startup. Rotate or revoke credentials by changing the configuration and restarting; existing requests can finish during replacement. Protect the config, TLS keys and audit directory from other users. Audit records are appended and synced before each response; audit failure returns 503 without releasing query data. Provision and monitor audit storage, and export it through a separately protected channel. Audit is local, synchronous and has no independent durable replication guarantee.

Rules apply in order to the line as regular expression replacements, before filtering. Rules must cover every sensitive representation (including structured JSON embedded in a line); this is not an automatic PII detector. This implementation supports masking, not keyed hashing. Sensitive values remain in the backend WAL and indexes: ingestion admission and redaction at rest are not implemented. Write access through this gateway is denied; ingest must not be exposed to untrusted clients.

Pagination uses encrypted authenticated gateway cursors, bound to the principal, stream, normalized bounds, filter and policy version. A cursor expires after 15 minutes. Keep the same key and policy version across gateway restarts to accept existing cursors; changing either invalidates them. Backend restart independently invalidates the wrapped backend cursor. Rotate keys by replacing the key and accepting that outstanding cursors fail; no key overlap is implemented. Reauthorization occurs on every request. A page can be empty and still have a cursor when the scan budget is reached. Continue until the cursor is absent. Page size may change; snapshot and duplicate ordering depend on the backend cursor contract.

The gateway bounds concurrent queries, request target, page scan count and output size. Backend work per query is not bounded by this gateway: the existing service can scan a large range to find a row, so deployment requires backend resource controls and trusted data sizes. Each scanned row requires a backend request; no latency objective has been demonstrated. Benchmark ordinary and paginated full responses on the agreed production workload before use; the required less-than-5% p95 overhead has not been established. This experimental implementation is not ready for production until admission-time PII handling, bounded backend query work, and the performance gate are addressed.
