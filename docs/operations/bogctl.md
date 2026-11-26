# bogctl and standalone operations

Build with `go build ./cmd/bogctl`. Options precede the command:

```
bogctl -server http://127.0.0.1:8080 health
bogctl -server http://127.0.0.1:8080 ingest < rows.ndjson
bogctl -start 2025-01-01T00:00:00Z -end 2025-01-02T00:00:00Z query
```

Query prints NDJSON without changing timestamps, duplicates, or ordering. The interval is half-open. Set `-timeout` for the entire operation. Point a query at a specific process, not a load-balanced service: cursors are process-local and become invalid on restart. Partial output may already have been written if a later page fails.

Ingest sends sequential atomic batches up to 4 MiB. Earlier batches may have succeeded when a later batch fails. HTTP 400, 413, 405 and the standalone server's 503 mean rejection; an interrupted connection or other response has an unknown outcome. Do not retry an unknown outcome: the server has no deduplication and may have committed it. Connect directly to the Bog server; intermediaries that synthesize HTTP responses can invalidate this distinction.

## Backup and restore

The server has no online snapshot API. Stop the server cleanly, ensure it has exited, then copy `bog.wal` and `bog.commit` together from its data directory to durable backup storage. Retain file ownership and restrictive permissions. Never copy these files independently while the server is running. Restore both files into a new, isolated, empty data directory on Linux storage meeting the durability requirements in the main README. Start an isolated Bog process against that directory, wait for health after replay, and compare application-specific queries and counts against independently recorded expectations before using the restore. Do not connect the restored copy to live writers while verifying it.

## Availability boundary

The current server has no peer protocol, quorum commit, leader fencing, deduplication, membership, or compatible online snapshot mechanism. Multiple replicas behind a service would be independent databases and cannot safely implement failover. Do not scale a standalone instance beyond one or claim automatic failover. A production operator and rolling, zero-outage HA upgrade require those server capabilities, protocol and WAL compatibility gates, and Linux failure testing first.
