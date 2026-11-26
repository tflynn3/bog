# bogctl operations

Build with `go build -o bogctl ./cmd/bogctl`. Connect directly to a Bog process:

```sh
bogctl health -addr http://127.0.0.1:8080
bogctl ingest -addr http://127.0.0.1:8080 < rows.ndjson
bogctl query -addr http://127.0.0.1:8080 -start 2025-01-01T00:00:00Z -end 2025-01-02T00:00:00Z > rows.ndjson
```

Ingest splits input at line boundaries into atomic batches of at most 4 MiB. Earlier batches can succeed before a later error. A lost response has an unknown outcome: do not blindly retry, as Bog does not deduplicate writes. By default HTTP 503 is treated as an unknown outcome. `-retry-503` enables bounded retries only when connected directly to Bog, where 503 confirms non-admission; do not enable it through an intermediary that synthesizes 503. Query streams a snapshot in pages, with inclusive start and exclusive end. Address a single process throughout pagination: cursors become invalid after restart or when routed to another process. Partial output may already have been written if a query fails.

For a consistent physical backup, stop Bog cleanly and copy `bog.wal` and `bog.commit` together from its data directory to durable backup storage. Do not copy live files independently: they may represent different commit points. To restore, stop Bog, restore both files into a clean, trusted data directory with the required ownership and permissions, and start Bog against that directory. Verify health after replay. Preserve the originals until the restored service is verified. The destination Linux storage must meet the durability and locking requirements documented in the main README. A query export can be re-ingested as a logical copy but does not preserve batch boundaries and is not a substitute for the physical backup.
