# Opt-in query gateway

Build `go build ./cmd/bog-gateway` and run `bog-gateway -config gateway.json -tls-cert cert.pem -tls-key key.pem`. The configuration file and audit file must be private (mode 0600); protect the enclosing directories and TLS key. Example configuration (replace every secret):

```json
{
  "streams": {"legacy": "http://127.0.0.1:8080", "other": "http://127.0.0.1:8081"},
  "default_stream": "legacy",
  "principals": {"reader": {"token": "replace-with-a-unique-random-secret", "streams": ["legacy"]}},
  "rules": [{"pattern": "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+", "replacement": "[redacted]"}],
  "policy_version": "1", "cursor_key": "64-hex-digits", "hash_key": "64-hex-digits", "audit_path": "/private/bog-audit.jsonl"
}
```

Generate independent random 32-byte hex keys and high-entropy bearer tokens. TLS terminates at the gateway; clients send `Authorization: Bearer ...`. Each stream must point to a **different**, private Bog deployment and its own data directory. The gateway cannot verify aliases or enforce network isolation: block direct client access to Bog, restrict gateway-to-Bog traffic to a trusted private network, and use HTTPS for remote endpoints. Place existing unscoped data only in the configured default stream's deployment; migrate other streams into separate deployments before enabling access. Do not put data from different streams in one Bog instance. Bog ingest remains private and is not exposed by this gateway.

Queries use the existing `/query` parameters plus `stream`; omission selects the default. Regex rules apply in order to the entire decoded line; use patterns that cover all sensitive forms, including values in serialized structured data. `hash: true` replaces each match with an HMAC-SHA256 hex digest using `hash_key`; otherwise `replacement` is literal. Filters are applied to the resulting view. These rules do not modify stored data or enforce admission-time PII controls. Policy authors must test patterns against their actual data; changing a policy requires changing `policy_version`.

`page=1` and `cursor` preserve the upstream snapshot and duplicate order; page size may change. Tokens are encrypted and authenticated, bound to actor, stream, normalized bounds, filter, and policy, and expire 15 minutes after the first page. They remain valid across gateway restarts only while the key is unchanged and the upstream process has not restarted; upstream restart invalidates its snapshot cursors. Rotate the cursor key to invalidate all tokens; rotate hash keys and policy versions together. Restart the gateway after changing configuration to revoke or rotate credentials; requests are authenticated and authorized on each page. Existing requests use the prior configuration until they complete.

At most 16 requests execute concurrently; each scans at most 256 upstream rows and returns at most 4 MiB. Use pagination for larger or sparse searches: a page can be empty while still carrying a continuation. Unpaged requests that exhaust the work budget return 503 without data. Upstream errors fail closed. The gateway records structured access decisions and outcomes without filters, cursors, credentials, or log lines, and synchronously fsyncs the audit record before returning data. Audit failures return 503 without releasing data; writes are serialized, so slow audit storage applies backpressure. Provision and monitor a protected durable filesystem and independently manage audit retention; file creation durability depends on provisioning its parent directory and file before service startup.

This implementation uses an upstream request per examined row. It has not met or been benchmarked against the requested less-than-5% full-response p95 overhead, and should not be deployed where that latency requirement is mandatory without representative paired measurements and optimization. The base `cmd/bog` does not implement `-query-work-budget`; that prior verifier failure cannot be repaired within the authorized gateway/security write paths.
