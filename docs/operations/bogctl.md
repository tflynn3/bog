# bogctl and deployment boundaries

Build with `go build ./cmd/bogctl`. The global `-addr` must identify a single Bog process, not a load-balanced service: cursor tokens are local to the process and expire on restart. `bogctl health`, `bogctl ingest < rows.ndjson`, and `bogctl query -start RFC3339 -end RFC3339` use the current standalone HTTP API. Query emits NDJSON in server order; the end is exclusive. The timeout applies to the whole operation. Query output may be partial if an error occurs; discard it before restarting a walk.

Ingest reads sequentially and sends batches of at most 4 MiB without splitting lines. Earlier batches may already have committed if a later batch fails. A rejection is confirmed for HTTP 4xx or the standalone server's 503; connection failures and unexpected responses have an unknown outcome. Do not replay an unknown batch or the entire input: this server does not deduplicate writes. An input line larger than 4 MiB cannot be ingested.

## Backup and restore

Stop Bog cleanly and wait for it to exit before copying both `bog.wal` and `bog.commit` together. Preserve ownership and restrictive permissions. Never copy a live WAL and marker independently: they may represent different commit boundaries. Restore both files into an isolated, trusted data directory on Linux storage satisfying the durability requirements in the main README. Start a separate Bog instance on that directory, wait for health after replay, and query expected data before considering the backup verified. Do not start two instances against the same directory or overwrite a running instance's files.

## Replication and upgrades

The current server has no peer replication protocol, durable quorum, fencing, leader discovery, deduplication, snapshot API, or compatibility negotiation. Multiple independent replicas cannot provide automatic failover or acknowledged-write durability. A Kubernetes controller must not scale this server as a replicated cluster, promote a follower, or claim zero-outage rolling upgrades. Single-node upgrades require a maintenance window and a verified offline backup; check client API and WAL compatibility before replacement, and retain a compatible rollback image. A readiness probe on `/healthz` only indicates that standalone replay completed, not quorum or leadership. Use persistent Linux storage with the locking, atomic rename, sync, and powersafe overwrite guarantees described in the README, keeping the WAL and marker on the same volume.
