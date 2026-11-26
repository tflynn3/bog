# bogctl operations

Build with `go build -o bogctl ./cmd/bogctl`. Point `-endpoint` at a specific Bog process, not a load-balanced service; continuation cursors belong to that process and become invalid after restart. `-timeout` sets an overall deadline.

```sh
bogctl health -endpoint http://localhost:8080
bogctl ingest -endpoint http://localhost:8080 < records.ndjson
bogctl query -start 2025-01-01T00:00:00Z -end 2025-01-02T00:00:00Z > export.ndjson
```

Ingest preserves records and sends sequential atomic batches of at most 4 MiB. Earlier batches may have succeeded when a later batch fails. A confirmed rejection did not commit that batch; a lost response or unexpected server response has an unknown outcome. Do not retry an unknown outcome: the server has no deduplication and retries can create duplicates. Query bounds are inclusive start and exclusive end. Output streams in server order, including duplicates and nanoseconds; an interrupted query may have produced partial output.

## Backup and restore

A query export is useful for logical migration but is not a full fidelity backup unless the chosen interval covers all records; the final representable nanosecond cannot be included because end is exclusive. For a durable physical backup, stop ingestion and gracefully stop Bog, wait for process exit, then copy `bog.wal` and `bog.commit` together from the data directory to durable backup storage. Record the Bog version and verify the backup copy. Do not independently copy live files: they may represent different commits.

To restore, stop Bog, preserve the existing directory, and copy both files from the same backup into a new, empty, trusted data directory on compatible Linux storage. Preserve restrictive ownership and permissions described in the main README. Start a compatible Bog version against that directory, wait for `/healthz` after replay, and check representative queries. Restoring replaces all data after the backup; verify WAL compatibility before upgrade or rollback. Never attach one directory to multiple Bog processes.
