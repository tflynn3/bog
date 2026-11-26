# bogctl operations

Build with `go build ./cmd/bogctl`. Flags precede the command:

```sh
bogctl -url http://127.0.0.1:8080 health
bogctl -url http://127.0.0.1:8080 ingest < rows.ndjson
bogctl -url http://127.0.0.1:8080 -start 2025-01-01T00:00:00Z -end 2025-01-02T00:00:00Z query > rows.ndjson
```

Ingest splits the input at line boundaries into requests of at most 4 MiB. Each request is atomic; earlier requests may already be committed when a later one fails. An unknown outcome must be resolved by the application, not blindly retried. A direct Bog server's 400, 413 and 503 responses indicate rejection; this classification assumes intermediaries preserve the server's admission semantics. The client makes no automatic write retries. Use `-timeout` to bound the whole operation.

Queries stream an NDJSON snapshot in server order, preserving duplicate rows and nanoseconds. Bounds are inclusive start and exclusive end. Use a direct process endpoint (for Kubernetes, a specific pod rather than a load-balanced Service) throughout the walk: cursors become invalid after restart or when a different process receives a continuation. A failed walk may have already written rows to stdout; discard partial output before restarting it.

## Backup and restore

Stop Bog cleanly and wait for exit before copying its data directory. Copy both `bog.wal` and `bog.commit` together to durable backup storage; do not copy a live, changing pair. Record the Bog version and verify backup integrity and restore procedure. Provision a trusted Linux filesystem satisfying the durability and permission requirements in the main README. With Bog stopped, restore the matching pair into an otherwise empty data directory, with the service user as owner and neither file group- or world-writable. Start the compatible Bog version and wait for health after WAL replay. Verify expected queries before accepting traffic. Never combine a WAL with a marker from another backup. These instructions apply to the base single-node server, not a replicated cluster.
