# bogctl and standalone operations

Build with `go build ./cmd/bogctl`. Global flags precede the command:

```
bogctl -server http://127.0.0.1:8080 health
bogctl -server http://127.0.0.1:8080 ingest < logs.ndjson
bogctl -server http://127.0.0.1:8080 query -start 2025-01-01T00:00:00Z -end 2025-01-02T00:00:00Z -limit 500
```

Ingest batches complete lines into requests of at most 4 MiB, sequentially. Each request is atomic; an earlier batch remains committed if a later batch fails. On failure, the error reports how many batches were acknowledged before the failed batch. Exit status 2 means the last batch may or may not have committed. Do not blindly retry it: Bog has no deduplication key. Validation rejections return status 1. A timeout covers the whole command. Queries stream NDJSON in server order, use half-open bounds and walk a server snapshot. Point the URL directly at one Bog process, not a load balancer: cursor state is process-specific and becomes invalid after restart. A failed query may already have written partial output.

Bog's current server is single-node. It has no replication protocol, fencing, leader routing, consistent online snapshot, or durable request deduplication. A Kubernetes StatefulSet with multiple replicas would create independent databases, not a highly available cluster. Do not use it to claim quorum commit or automatic failover.

For a consistent offline backup, stop the server cleanly and copy `bog.wal` and `bog.commit` together while it remains stopped. Preserve both files unchanged on durable storage. To verify a restore, copy them into a separate, trusted directory on a Linux filesystem meeting the durability requirements in the main README, start a separate Bog process on a different address, and query expected data before considering the backup usable. Never run two processes against the same directory. Restart the original only after the copy is complete. This procedure has downtime and is not an online or replicated snapshot.
