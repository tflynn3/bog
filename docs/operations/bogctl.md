# Bog operations

Build `go build ./cmd/bogctl` and target the **same process** throughout a paginated query. For example, use a port-forward to `bog-0`, not a load-balanced Service:

```sh
bogctl health -url http://127.0.0.1:8080
bogctl ingest -url http://127.0.0.1:8080 < batch.ndjson
bogctl query -start 2025-01-01T00:00:00Z -end 2025-01-02T00:00:00Z > rows.ndjson
```

Each ingest invocation is a single atomic batch of at most 4 MiB; the CLI does not split it. A 204 acknowledges the durable commit and publication. A confirmed rejection means the server did not admit the batch. A transport failure or unexpected response means the outcome is unknown: inspect application records before retrying, since Bog has no deduplication. Queries stream NDJSON in server order, including duplicates, with nanosecond timestamps and an exclusive end. A cursor becomes invalid on restart or failover; restart the entire query against the same process. Output may already contain partial results if a query fails.

`operator/statefulset.yaml` is a single-node deployment template, not a replication controller. Set the image and storage class after verifying compatibility. Its startup probe allows WAL replay to finish before liveness checks begin. The PVC must use Linux storage that supports exclusive advisory locks, atomic rename, file and directory sync, and powersafe overwrite, with trusted parent directories and the ownership and permissions described in the main README. Kubernetes access modes alone do not guarantee these properties. The sidecar is an optional single-upstream proxy, not replication or failover. Never scale this deployment above one: independent Bog processes do not share data, provide quorum, or fence leaders.

## Backup and restore

Stop writes and gracefully stop Bog before taking a consistent backup of the data directory. Preserve `bog.wal` and `bog.commit` together, their ownership and restrictive permissions, and verify the backup. Do not copy the two files independently while writes are active. Restore both files together to a stopped instance on suitable storage, with a durably established and trusted parent directory; then start Bog and wait for replay and health. Test restores regularly. Retain the original backup until verification is complete.

Before replacing the single pod, stop writes, back up, and verify the old and new binaries' client API and WAL compatibility. `OnDelete` keeps replacement manual. Restart invalidates cursors and causes downtime. Roll back only if the older binary can read the WAL written by the newer binary; otherwise restore a compatible backup, accepting any resulting loss of later writes. No peer protocol, replicated rollout, automatic scaling, or quorum-backed acknowledgement exists in this version.
