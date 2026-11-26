# bogctl

Build with `go build ./cmd/bogctl`. Send a single atomic batch of UTF-8 NDJSON, at most 4 MiB, on standard input:

```sh
bogctl ingest -addr http://127.0.0.1:8080 < batch.ndjson
bogctl query -addr http://127.0.0.1:8080 -start 2025-01-01T00:00:00Z -end 2025-01-02T00:00:00Z -all
```

Query output is streamed as NDJSON. Bounds are inclusive at the start and exclusive at the end. `-all` walks a snapshot using continuation cursors. Connect directly to the same Bog process for the entire walk; a load-balanced endpoint can route a cursor to another process. Restart invalidates cursors. Without `-all`, `-cursor` continues a previously obtained cursor; the next cursor is printed to stderr. `-timeout` bounds the entire operation.

A 400, 413, or 503 ingest response confirms rejection by the base Bog server. A transport failure or unexpected response means the outcome is unknown; do not blindly retry because the batch may already be durable and retries can duplicate rows. The client does not retry automatically. Preserve `bog.wal` and `bog.commit` together on storage satisfying the durability requirements in the main README.

The included proxy is **not an HA implementation**. It rejects every ingest and reports itself unready: independent Bog instances have no common durable log, fencing, or deduplication protocol. Do not use it as a replicated service. Achieving quorum replication requires changes to the Bog server and its persistence protocol beyond the scope of this client and sidecar.
