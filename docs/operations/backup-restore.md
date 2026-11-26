# Backup and restore

Bog has no online snapshot API. Stop traffic and shut down Bog cleanly, then copy `bog.wal` and `bog.commit` together from the same data directory. Keep the pair consistent: do not copy them independently while Bog is running. Retain backups on durable storage and test recovery.

Restore both files into a new, existing directory with the permissions and ownership required by Bog. Start one Bog process against that directory on Linux storage meeting the locking, atomic rename, file and directory sync, and powersafe-overwrite requirements in the main README. Wait for replay to finish and `/healthz` to succeed before serving traffic. Never run two Bog processes against the same directory. A backup made before an acknowledged write necessarily omits it.

`bogctl -url http://host:8080 ingest < rows.ndjson` sends sequential batches of at most 4 MiB. Each batch is atomic, but the entire input is not: earlier batches can be committed if a later batch fails. A lost response is an unknown outcome; retrying can create duplicates. `bogctl -start ... -end ... query` streams snapshot pages to standard output. Use an address pinned to one Bog process; process restart, failover, or routing to another process invalidates its cursor. Query bounds are inclusive start and exclusive end.
