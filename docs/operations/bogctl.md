# Operating a standalone Bog process

Build `bogctl` with `go build ./cmd/bogctl`. Select the process directly with
`--server http://host:8080`, before the command. `health` checks readiness;
`ingest` reads NDJSON from standard input; `query --start TIME --end TIME
[--contains TEXT] [--limit N]` writes NDJSON to standard output. Query bounds
are inclusive start and exclusive end. The limit is per page, not a total limit.
The timeout covers the whole operation and defaults to 30 seconds.

Ingest streams input in sequential requests of at most 4 MiB; each request is
atomic, but a stream spanning multiple requests is not. A failure may therefore
leave earlier batches committed. A rejected batch was not admitted; an unknown
outcome may have committed and must not be retried without accepting duplicates.
The client performs no automatic retries. It does not buffer an entire input or
query result. A line larger than 4 MiB cannot be sent. Input errors or oversized
lines can occur after earlier batches have committed.

Point pagination at a single process, not a load-balanced address. A restart
invalidates its cursors; the client surfaces a rejected continuation as a cursor
error. Output already streamed before an error is partial. Restart the complete
query if a consistent result is needed, and avoid appending partial output twice.

## Offline backup and isolated restore

Bog has no snapshot endpoint. Stop the process cleanly and wait for exit before
copying both `bog.wal` and `bog.commit` as one backup; never copy a live pair.
Keep the original data directory intact. Preserve the files together on durable
storage, and record the Bog binary version and checksum with the backup. Follow
the storage ownership, locking, sync and powersafe-overwrite requirements in
the main README. Do not use a shared live data directory.

To verify a restore, copy both files into a new, private data directory on a
compatible Linux filesystem and start the same Bog version on an isolated port.
Wait for `/healthz`, query known records, then stop the isolated instance. A
successful startup verifies WAL replay but does not prove that the backup contains
all records expected by an application. Test this procedure regularly.

## Replication and upgrades

The current server is single-node and has no quorum replication, leader fencing,
durable request deduplication, membership protocol, or compatibility negotiation.
Multiple replicas behind a Service would contain independent data and are not a
high-availability cluster. Do not claim automatic failover or zero-outage rolling
upgrades. A safe operator requires those server capabilities, persistent per-member
identities and volumes, serialized membership changes, and compatibility gates;
none can be supplied by a StatefulSet alone. Upgrade the standalone server only
with a verified offline backup and a planned interruption.
